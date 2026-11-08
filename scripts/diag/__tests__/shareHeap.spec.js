const { captureHeap, diagnosticOptions, runDiagnostic } = require('../shareHeap')

test('diagnostic mode is inert without its explicit flag and validates fixed repetition counts', async () => {
  expect(diagnosticOptions({})).toBeNull()
  await expect(runDiagnostic({ env: {} })).resolves.toBeNull()
  expect(diagnosticOptions({ SHARE_SOAK_HEAP_DIAG: '1' })).toMatchObject({ extraCycles: 0 })
  expect(() =>
    diagnosticOptions({ SHARE_SOAK_HEAP_DIAG: '1', SHARE_SOAK_HEAP_EXTRA_CYCLES: '-1' })
  ).toThrow(/extra cycles/)
})

test('captures twice-collected heap with original owner clock and records snapshot cost separately', () => {
  const calls = []
  let time = 50
  const result = captureHeap({
    directory: '/evidence',
    label: 'baseline',
    owner: { startedAt: 10, clock: { now: () => time } },
    gc: () => calls.push('gc'),
    memory: () => ({ heapUsed: 123 }),
    write: (file) => {
      calls.push(file)
      time += 7
      return file
    },
    stat: () => ({ size: 456 }),
  })
  expect(calls).toEqual(['gc', 'gc', '/evidence/heap-baseline.heapsnapshot'])
  expect(result).toMatchObject({
    file: 'heap-baseline.heapsnapshot',
    sizeBytes: 456,
    heapUsed: 123,
    ownerAtMs: 40,
    completedOwnerAtMs: 47,
    snapshotMs: 7,
    acceptanceEligible: false,
  })
})

test('captures outside actual owner stages and keeps extra blocks separate from acceptance work', async () => {
  const fs = require('node:fs')
  const os = require('node:os')
  const path = require('node:path')
  const v8 = require('node:v8')
  const soakModule = require('../../soak/soak')
  const evidence = require('../../../testHelpers/e2eEvidence')
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'share-heap-control-'))
  const originalGC = globalThis.gc
  const rows = []
  let now = 100
  let extraCalls = 0
  const driver = {
    prepare: jest.fn(async () => {}),
    final: jest.fn(async () => ({ originalFinal: true })),
    closePhase: jest.fn(async () => {}),
    openPhase: jest.fn(async () => {}),
    cycle: jest.fn(async (plan) => {
      expect(driver.final).toHaveBeenCalledTimes(1)
      expect(plan).toMatchObject({ cycle: 30 + extraCalls, phase: 'share-steady', reset: false })
      extraCalls++
      now += 100
      return { cycle: plan.cycle }
    }),
  }
  const owner = new soakModule.ShareSoak({
    driver,
    kafka: {},
    shares: { snapshot: async () => ({ sockets: { open: 0 }, heapUsed: 123 }), restore: jest.fn() },
    stack: { run: jest.fn(async () => {}) },
    short: true,
    evidence: {},
    gate: { assertClean: jest.fn(), taint: jest.fn() },
    clock: { now: () => now },
    pause: async (scope, ms) => {
      now += ms
    },
  })
  owner.runPhase = async () => {
    now += 320000
  }
  owner.verifyCycle = jest.fn(() => [])
  const originalSoak = owner.soak.bind(owner)
  owner.soak = () => {
    const soak = originalSoak()
    soak.judge = () => ({ ok: false, violations: ['original heap screen still failed'] })
    return soak
  }
  const mocks = [
    jest.spyOn(soakModule, 'buildShareSoak').mockReturnValue(owner),
    jest.spyOn(evidence, 'evidencePath').mockReturnValue(path.join(directory, 'diag.jsonl')),
    jest.spyOn(evidence, 'writeEvidence').mockImplementation((stem, row) => rows.push(row)),
    jest.spyOn(v8, 'writeHeapSnapshot').mockImplementation((file) => {
      // A long capture changes the owner's clock but must not spend the transition allocation.
      now += 500
      fs.writeFileSync(file, '{}')
      return file
    }),
  ]
  globalThis.gc = jest.fn()
  try {
    const result = await runDiagnostic({
      env: { SHARE_SOAK_HEAP_DIAG: '1', SHARE_SOAK_HEAP_EXTRA_CYCLES: '11' },
    })
    expect(result.resources.ok).toBe(false)
    expect(result.acceptanceEligible).toBe(false)
    expect(owner.spent.transition).toBe(0)
    expect(owner.spent.cleanup).toBe(0)
    expect(owner.verifyCycle).toHaveBeenCalledTimes(11)
    expect(globalThis.gc).toHaveBeenCalledTimes(12)
    const snapshots = rows.filter(({ event }) => event === 'diag-heap-snapshot')
    expect(snapshots.map(({ label }) => label)).toEqual([
      'baseline',
      'share-steady',
      'share-reset',
      'share-members',
      'repeat-010',
      'repeat-011',
    ])
    expect(snapshots.every(({ snapshotMs }) => snapshotMs === 500)).toBe(true)
    expect(
      rows.every(
        ({ diagnostic, acceptanceEligible, selector }) =>
          diagnostic && acceptanceEligible === false && selector === 'DIAG_SHARE_HEAP'
      )
    ).toBe(true)
    expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
    expect(owner.shares.restore).toHaveBeenCalledTimes(1)
  } finally {
    for (const mock of mocks) mock.mockRestore()
    if (originalGC === undefined) delete globalThis.gc
    else globalThis.gc = originalGC
    fs.rmSync(directory, { recursive: true, force: true })
  }
})
