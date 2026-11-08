const { captureHeap, diagnosticOptions, runDiagnostic } = require('../shareHeap')

test('diagnostic mode is inert without its explicit flag and validates fixed repetition counts', async () => {
  expect(diagnosticOptions({})).toBeNull()
  await expect(runDiagnostic({ env: {} })).resolves.toBeNull()
  expect(diagnosticOptions({ SHARE_SOAK_HEAP_DIAG: '1' })).toMatchObject({ extraCycles: 10 })
  expect(() =>
    diagnosticOptions({ SHARE_SOAK_HEAP_DIAG: '1', SHARE_SOAK_HEAP_EXTRA_CYCLES: '-1' })
  ).toThrow(/extra cycles/)
})

test.each([undefined, '0', String(Date.now() + 3600000)])(
  'refuses setup without enough measured step budget: %s',
  async (step) => {
    const originalGC = globalThis.gc
    globalThis.gc = () => {}
    try {
      await expect(
        runDiagnostic({ env: { SHARE_SOAK_HEAP_DIAG: '1', SHARE_SOAK_HEAP_STEP_STARTED_AT: step } })
      ).rejects.toThrow(/cannot fit/)
    } finally {
      if (originalGC === undefined) delete globalThis.gc
      else globalThis.gc = originalGC
    }
  }
)

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

test.each([false, true])(
  'captures outside owner stages and preserves repetition archival failures=%s',
  async (injectedFailure) => {
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
    const primary = new TypeError('extra row archival failed')
    const phaseCleanup = new Error('phase cleanup failed')
    const driver = {
      prepare: jest.fn(async () => {}),
      final: jest.fn(async () => ({ originalFinal: true })),
      closePhase: jest.fn(async () => {
        if (injectedFailure && extraCalls > 0) throw phaseCleanup
      }),
      openPhase: jest.fn(async () => {}),
      cycle: jest.fn(async (plan) => {
        expect(driver.final).toHaveBeenCalledTimes(1)
        expect(plan).toMatchObject({ cycle: 30 + extraCalls, phase: 'share-steady', reset: false })
        expect(typeof plan.first).toBe('bigint')
        extraCalls++
        now += 100
        return { cycle: plan.cycle }
      }),
    }
    const owner = new soakModule.ShareSoak({
      driver,
      kafka: {},
      shares: {
        snapshot: async () => ({ sockets: { open: 0 }, heapUsed: 123 }),
        restore: jest.fn(),
      },
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
      jest.spyOn(evidence, 'writeEvidence').mockImplementation((stem, row) => {
        if (injectedFailure && row.event === 'diag-extra-cycle') throw primary
        const line = JSON.stringify(row)
        rows.push(JSON.parse(line))
        fs.appendFileSync(path.join(directory, 'diag.jsonl'), `${line}\n`)
      }),
      jest.spyOn(v8, 'writeHeapSnapshot').mockImplementation((file) => {
        // A long capture changes the owner's clock but must not spend the transition allocation.
        now += 500
        fs.writeFileSync(file, '{}')
        return file
      }),
    ]
    globalThis.gc = jest.fn()
    try {
      const failure = await runDiagnostic({
        env: {
          SHARE_SOAK_HEAP_DIAG: '1',
          SHARE_SOAK_HEAP_EXTRA_CYCLES: '10',
          SHARE_SOAK_HEAP_STEP_STARTED_AT: String(Date.now()),
        },
      }).catch((error) => error)
      if (injectedFailure) {
        expect(failure).toBe(primary)
        expect(primary.cleanupErrors).toEqual([phaseCleanup])
        expect(rows.find(({ event }) => event === 'diag-cleanup-failed').error.message).toBe(
          phaseCleanup.message
        )
        expect(rows.find(({ event }) => event === 'diag-failed').error.message).toBe(
          primary.message
        )
        expect(rows.some(({ event }) => ['case-complete', 'diag-complete'].includes(event))).toBe(
          false
        )
        expect(owner.closed).toBe(true)
        return
      }
      expect(failure.message).toMatch(/Incomplete diagnostic evidence/)
      const extra = rows.filter(({ event }) => event === 'diag-extra-cycle')
      expect(extra.map(({ plan }) => plan.cycle)).toEqual([30, 31, 32, 33, 34, 35, 36, 37, 38, 39])
      expect(extra[0].plan.first).toBe('600')
      expect(extra[9].plan.end).toBe('800')
      expect(extra[0].plan.records).toHaveLength(60)
      expect(rows.find(({ kind }) => kind === 'resource-verdict').ok).toBe(false)
      expect(rows.some(({ event }) => event === 'diag-complete')).toBe(false)
      expect(owner.spent.transition).toBe(0)
      expect(owner.spent.cleanup).toBe(0)
      expect(owner.verifyCycle).toHaveBeenCalledTimes(10)
      expect(globalThis.gc).toHaveBeenCalledTimes(10)
      const snapshots = rows.filter(({ event }) => event === 'diag-heap-snapshot')
      expect(snapshots.map(({ label }) => label)).toEqual([
        'baseline',
        'share-steady',
        'share-reset',
        'share-members',
        'repeat-010',
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
  }
)

test.each(['workload', 'archival'])(
  'preserves primary %s error and archives joined cleanup failure separately',
  async (failure) => {
    const fs = require('node:fs')
    const os = require('node:os')
    const path = require('node:path')
    const soakModule = require('../../soak/soak')
    const evidence = require('../../../testHelpers/e2eEvidence')
    const { LiveRun } = require('../../../src/producer/__tests__/fixtures/bounded')
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'diag-failure-'))
    const primary = new Error(`primary ${failure}`)
    const cleanup = new Error('strict cleanup failed')
    const rows = []
    const gate = new LiveRun()
    let joined = false
    const owner = new soakModule.ShareSoak({
      driver: {
        prepare: async () => {
          if (failure === 'workload') throw primary
        },
      },
      kafka: {},
      shares: { restore: () => {} },
      short: true,
      gate,
      stack: {
        run: async () => {
          await Promise.resolve()
          joined = true
          throw cleanup
        },
      },
    })
    const mocks = [
      jest.spyOn(soakModule, 'buildShareSoak').mockReturnValue(owner),
      jest.spyOn(evidence, 'evidencePath').mockReturnValue(path.join(directory, 'diag.jsonl')),
      jest.spyOn(evidence, 'writeEvidence').mockImplementation((stem, row) => {
        if (failure === 'archival' && row.event === 'soak-schedule') throw primary
        rows.push(JSON.parse(JSON.stringify(row)))
      }),
    ]
    const originalGC = globalThis.gc
    globalThis.gc = () => {}
    try {
      await expect(
        runDiagnostic({
          env: { SHARE_SOAK_HEAP_DIAG: '1', SHARE_SOAK_HEAP_STEP_STARTED_AT: String(Date.now()) },
        })
      ).rejects.toBe(primary)
      expect(primary.cleanupErrors).toEqual([cleanup])
      expect(joined).toBe(true)
      expect(gate.tainted).toBe(true)
      expect(
        rows.filter(({ event }) => event === 'case-failed').map(({ error }) => error.message)
      ).toEqual([primary.message, cleanup.message])
      expect(rows.find(({ event }) => event === 'diag-cleanup-failed').error.message).toBe(
        cleanup.message
      )
      expect(rows.some(({ event }) => ['diag-complete', 'case-complete'].includes(event))).toBe(
        false
      )
    } finally {
      for (const mock of mocks) mock.mockRestore()
      if (originalGC === undefined) delete globalThis.gc
      else globalThis.gc = originalGC
      fs.rmSync(directory, { recursive: true, force: true })
    }
  }
)

test.each([false, true])(
  'measures original owner wrapper populations on exceptional cleanup=%s',
  async (exceptional) => {
    const { ShareSoak } = require('../../soak/soak')
    const { CleanupStack } = require('../../../src/producer/__tests__/fixtures/liveProbe')
    const { bounded, NEVER, LiveRun } = require('../../../src/producer/__tests__/fixtures/bounded')
    const {
      captureShareCleanupBaseline,
      assertShareCleanup,
    } = require('../../../testHelpers/shareFixtures')
    const { ShareTimerObserver } = require('../../soak/shareTimerObserver')
    const timers = new ShareTimerObserver()
    timers.install()
    const snapshots = []
    const probe = {
      snapshot: async () => {
        const activeResources = {}
        // eslint-disable-next-line n/no-unsupported-features/node-builtins -- diagnostic controls target Node 24
        for (const type of process.getActiveResourcesInfo())
          activeResources[type] = (activeResources[type] ?? 0) + 1
        const snapshot = {
          activeResources,
          timerOwners: timers.snapshot(),
          sockets: { created: 0, destroyed: 0, open: 0 },
          listeners: {},
          queueDepth: 0,
          ledgerSize: 0,
        }
        snapshots.push(snapshot)
        return snapshot
      },
    }
    const stack = new CleanupStack()
    const gate = new LiveRun()
    let baseline
    const owner = new ShareSoak({
      driver: {
        prepare: async (_, scope) => {
          baseline = await captureShareCleanupBaseline(probe, { signal: scope })
          stack.add('strict resource proof', (signal) =>
            assertShareCleanup(
              { listGroups: async () => ({ groups: [] }) },
              [],
              [],
              baseline,
              probe,
              { signal }
            )
          )
        },
      },
      kafka: {},
      shares: { restore: () => {} },
      stack,
      short: true,
      gate,
      evidence: { write: () => {} },
    })
    owner.startedAt = owner.clock.now()
    owner.workDue = owner.startedAt + 2000
    try {
      await bounded(NEVER, 2000, async (scope) => {
        owner.scope = scope
        await owner.prepare()
        if (!exceptional) await owner.stage('cleanup', () => owner.cleanupStack())
      })
      if (exceptional) {
        const error = await owner.close().catch((error) => error)
        expect(error).toBeInstanceOf(AggregateError)
        expect(error.errors[0].message).toMatch(/resources did not return to baseline/)
        expect(gate.tainted).toBe(true)
        expect(baseline.activeResources.Timeout - snapshots.at(-1).activeResources.Timeout).toBe(2)
      } else {
        await owner.close()
        expect(snapshots.at(-1).activeResources).toEqual(baseline.activeResources)
      }
      expect(baseline.timerOwners.dropped).toBe(0)
      expect(baseline.timerOwners.rows.filter(({ hasRef }) => hasRef)).toHaveLength(4)
      expect(snapshots.at(-1).timerOwners.rows.filter(({ hasRef }) => hasRef)).toHaveLength(
        exceptional ? 2 : 4
      )
    } finally {
      timers.restore()
    }
  }
)
