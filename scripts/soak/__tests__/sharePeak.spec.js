const {
  SharePeak,
  DisabledShareWorker,
  DisabledShareRegistry,
  installSharePeak,
} = require('../sharePeak')
const {
  ReferenceControl,
  TelemetryWorker,
  TelemetryWorkerProcess,
  declaredJournalBytes,
} = require('../telemetryChurn')
const { PROFILES, TELEMETRY } = require('../soak')
const { TelemetrySegments } = require('../../../testHelpers/telemetrySegments')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const crypto = require('node:crypto')
const { ShareAllocationProfile, topStacks, PROFILE_LIMITS } = require('../shareAllocationProfile')

/** @returns {object} deterministic kernel, engine and clock facts */
const fixture = () => {
  const state = { at: 0, vmrss: 1000, vmhwm: 1200, pending: 0 }
  const peak = new SharePeak({
    reserveBytes: 23192560,
    now: () => state.at,
    proc: () => ({ vmrss: state.vmrss, vmhwm: state.vmhwm }),
    usage: () => ({ rss: 1001, heapTotal: 800, heapUsed: 600, external: 200, arrayBuffers: 100 }),
    spaces: () => [
      { space_name: 'old_space', space_size: 800, space_used_size: 600, physical_space_size: 700 },
    ],
    pending: () => state.pending,
    capacity: 8,
  })
  return { peak, state }
}

describe('share peak attribution', () => {
  test('records the fixed grid and every observed high-water step with phase and raw byte facts', () => {
    const { peak, state } = fixture()
    peak.phase('setup')
    state.at = 20
    peak.poll()
    state.at = 40
    state.vmhwm = 1600
    state.pending = 2
    peak.phase('reopen', 6)
    state.at = 1000
    peak.poll()
    const report = peak.report()
    expect(report).toMatchObject({
      scope: 'shadow',
      acceptanceEligible: false,
      attributionComplete: false,
      complete: true,
      reserveBytes: 23192560,
      pollMs: 20,
      gridMs: 1000,
    })
    const step = report.readings.find((row) => row.VmHWM === 1600)
    expect(step).toMatchObject({
      phase: 'reopen',
      tick: 6,
      at: 40,
      previousPollAt: 20,
      hwmStep: true,
      VmRSS: 1000,
      pendingNativeJobs: 2,
      memoryUsage: { rss: 1001, heapTotal: 800, heapUsed: 600, external: 200, arrayBuffers: 100 },
      heapSpaces: [{ name: 'old_space', committedBytes: 800, usedBytes: 600, physicalBytes: 700 }],
    })
    expect(report.readings.filter((row) => row.gridAt !== null).map((row) => row.gridAt)).toEqual([
      0, 1000,
    ])
    expect(report.peak).toEqual(step)
  })

  test('records every VmHWM decrease as complete diagnostic evidence and retains the maximum peak', () => {
    const { peak, state } = fixture()
    peak.phase('setup')
    state.at = 20
    peak.poll()
    state.at = 40
    state.vmhwm = 1600
    peak.phase('reopen', 6)
    state.at = 60
    state.vmhwm = 1450
    state.vmrss = 1005
    peak.phase('retire', 7)
    state.at = 80
    state.vmhwm = 1400
    state.vmrss = 1004
    peak.poll()
    state.at = 100
    state.vmhwm = 1500
    peak.poll()
    state.at = 120
    state.vmhwm = 1300
    state.vmrss = 990
    peak.phase('cleanup')
    const report = JSON.parse(JSON.stringify(peak.report()))
    expect(report.complete).toBe(true)
    expect(report.errors).toEqual([])
    expect(report.hwmDecreases).toEqual([
      { index: 2, phase: 'retire', before: 1600, after: 1450, VmRSS: 1005 },
      { index: 3, phase: 'retire', before: 1450, after: 1400, VmRSS: 1004 },
      { index: 5, phase: 'cleanup', before: 1500, after: 1300, VmRSS: 990 },
    ])
    expect(report.readings.map((row) => row.VmHWM)).toEqual([1200, 1600, 1450, 1400, 1500, 1300])
    expect(report.peak).toMatchObject({ index: 1, phase: 'reopen', VmHWM: 1600 })
    expect(report.peak).toEqual(report.readings[1])
  })

  test('keeps missing sources incomplete without inventing a VmHWM decrease', () => {
    const { peak, state } = fixture()
    peak.poll()
    peak.proc = () => null
    state.at = 20
    peak.poll()
    state.at = 40
    peak.proc = () => ({ vmrss: 1000, vmhwm: 1100 })
    peak.poll()
    const report = peak.report()
    expect(report.complete).toBe(false)
    expect(report.readings[1].VmHWM).toBe(null)
    expect(report.errors).toEqual(['proc status unavailable'])
    expect(report.hwmDecreases).toEqual([])
    expect(report.peak.VmHWM).toBe(1200)
  })

  test('prefills bounded diagnostic storage without changing the original reserve and fails overflow', () => {
    const { peak, state } = fixture()
    const storage = peak.storage
    const bytes = storage.byteLength
    expect(new Uint8Array(storage.buffer).every((byte) => byte === 0xa5)).toBe(true)
    for (let i = 0; i < 8; i++) {
      state.at = i * 1000
      peak.poll()
    }
    state.at = 8000
    expect(() => peak.poll()).toThrow('share peak overflow')
    expect(peak.storage).toBe(storage)
    expect(peak.report()).toMatchObject({
      complete: false,
      diagnosticReserveBytes: bytes,
      reserveBytes: 23192560,
    })
    expect(declaredJournalBytes('shareConsumer')).toBe(23192560)
  })

  test('runs the disabled share foreground, registry and public reopen without telemetry calls', async () => {
    const client = {
      connect: jest.fn(),
      clientInstanceId: jest.fn(() => {
        throw new Error('telemetry disabled')
      }),
    }
    const segment = { id: 's0', observer: { checkpoint: () => 0 } }
    const cycle = {
      closed: Promise.resolve(),
      reopened: Promise.resolve(),
      from: segment,
      to: { id: 's1' },
    }
    const worker = new DisabledShareWorker({
      kind: 'shareConsumer',
      client,
      clock: { now: () => 10 },
      limits: { warmupMs: 60000, measureMs: 300000 },
      segments: {
        current: () => segment,
        close: jest.fn(),
        advance: jest.fn(),
        cycle: jest.fn(() => cycle),
        journal: () => [],
      },
      ordinary: { start: jest.fn(), pause: jest.fn(), resume: jest.fn(), core: jest.fn() },
      registry: { fill: jest.fn(), replace: jest.fn(() => 'r') },
    })
    worker.seal = jest.fn()
    worker.drain = jest.fn()
    await worker.prepare()
    await worker.begin()
    await worker.reopen({ tick: 6 })
    await worker.retire({ tick: 6 })
    worker.confirmation = { tick: 6 }
    await worker.confirm({ tick: 6 })
    expect(worker).toBeInstanceOf(TelemetryWorker)
    expect(client.connect).toHaveBeenCalledTimes(2)
    expect(client.clientInstanceId).not.toHaveBeenCalled()
    for (const method of ['start', 'pause', 'resume', 'core'])
      expect(worker.ordinary[method]).toHaveBeenCalledTimes(1)
    expect(worker.registry.fill).toHaveBeenCalledTimes(1)
    expect(worker.registry.replace).toHaveBeenCalledWith(6)
    expect(worker.segments.cycle).toHaveBeenCalledWith(client, { timeout: 3000 })
  })

  test('marks exported verdict inputs only while a share peak recorder is active', async () => {
    const snapshot = { journals: { byteLength: 4096, cursor: 0 } }
    const run = {
      baseline: snapshot,
      samples: [{ admittedPhaseMs: 60000, phaseMs: 60300, snapshot }],
      teardown: snapshot,
    }
    const ordinary = {
      clientCount: 1,
      baseline: snapshot,
      samples: [{ admittedAtMs: 60000, completedAtMs: 60300, atMs: 60300, snapshot }],
      teardown: snapshot,
      measurementMs: 300000,
      cleanupMs: 1000,
      reference: {},
      sampler: { skipped: [], readFailures: [] },
    }
    jest.spyOn(ReferenceControl.prototype, 'run').mockResolvedValue({ manifest: {} })
    jest.spyOn(TelemetrySegments, 'ordinary').mockReturnValue({})
    try {
      for (const active of [false, true]) {
        const messages = []
        const verdict = { ok: true, outcome: 'passed', violations: [] }
        const owner = new TelemetryWorkerProcess({
          kind: 'shareConsumer',
          soak: {
            TELEMETRY,
            PROFILES,
            logLevel: { NOTHING: 0 },
            instrument: () => ({ kafka: { shareConsumer: () => ({}) } }),
            sharePeak: () => (active ? fixture().peak : null),
            SharePeakWorker: class {
              constructor() {
                this.timing = { measurementMs: 300000, cleanupMs: 1000 }
                this.claims = { foreground: [] }
              }

              async prepare() {
                return 'uuid'
              }

              facts() {
                return {}
              }
            },
            TelemetryResourceVerdict: { evaluate: () => verdict },
            Soak: class {
              constructor({ judge }) {
                this.judge = judge
                this.skipped = []
                this.readFailures = []
              }

              async run() {
                return this.judge(run)
              }
            },
          },
          proc: {
            on() {},
            send(message, callback) {
              messages.push(JSON.parse(JSON.stringify(message)))
              callback?.()
            },
          },
          env: {},
        })
        owner.stamps = () => ({})
        owner.reference = () => ({})
        owner.inbox.push({ type: 'config', stem: 'peak', topic: 'topic', group: 'group' })
        owner.inbox.push({ type: 'start' })
        await expect(owner.run()).resolves.toEqual(verdict)
        const result = messages.find((message) => message.type === 'result')
        expect(JSON.stringify(result.input)).toBe(
          JSON.stringify({
            ...ordinary,
            ...(active && { diagnostic: true, acceptanceEligible: false }),
          })
        )
      }
    } finally {
      jest.restoreAllMocks()
    }
  })

  test('installs only in diagnostic share children and leaves the judged input contract unchanged', () => {
    const processOwner = { kind: 'shareConsumer', soak: {} }
    installSharePeak(processOwner, 'disabled')
    expect(processOwner.soak.SharePeakWorker).toBe(DisabledShareWorker)
    const kafka = { shareConsumer: jest.fn() }
    processOwner.create(kafka, { group: 'same-group' })
    expect(kafka.shareConsumer).toHaveBeenCalledWith({
      enableMetricsPush: false,
      groupId: 'same-group',
    })
    const registry = new DisabledShareRegistry(
      {
        registerMetricForSubscription: () => {
          throw new Error('disabled')
        },
      },
      'shareConsumer'
    )
    registry.fill()
    const before = registry.digest(null)
    registry.replace(6)
    registry.restore()
    expect(registry.registered).toHaveLength(128)
    expect(registry.digest(null)).toBe(before)
    expect(typeof processOwner.soak.sharePeak).toBe('function')
    const other = { kind: 'producer', soak: {} }
    installSharePeak(other, 'enabled')
    expect(other.soak).toEqual({})
    const send = jest.fn()
    const owner = new TelemetryWorkerProcess({
      kind: 'shareConsumer',
      soak: {},
      proc: { send },
      env: {},
    })
    const { peak } = fixture()
    owner.peak = peak
    const snapshot = Object.freeze({ memory: Object.freeze({ hwm: 1200 }) })
    const row = Object.freeze({ kind: 'baseline', snapshot })
    owner.progress(row)
    expect(send.mock.calls[0][0].row).toBe(row)
    expect(row.snapshot).toBe(snapshot)
    expect(row).not.toHaveProperty('shadow')
    expect(snapshot).not.toHaveProperty('sharePeak')
    expect(peak.report().readings[0].phase).toBe('M')
    expect(TelemetryWorkerProcess.prototype).not.toHaveProperty('sharePeak')
    expect(() => installSharePeak(processOwner, 'unknown')).toThrow('share peak arm')
  })
})

describe('share allocation sampling', () => {
  let directory
  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'share-allocation-'))
  })
  afterEach(() => fs.rmSync(directory, { recursive: true, force: true }))

  /** @returns {object} deterministic V8-shaped self-byte tree, including a collected allocation */
  const profile = () => ({
    head: {
      id: 1,
      callFrame: { functionName: '(root)', url: '', lineNumber: -1, columnNumber: -1 },
      selfSize: 0,
      children: [
        {
          id: 2,
          callFrame: {
            functionName: 'seal',
            url: 'file:///repo/scripts/soak/telemetryChurn.js',
            lineNumber: 2887,
            columnNumber: 2,
          },
          selfSize: 64,
          children: [
            {
              id: 3,
              callFrame: {
                functionName: 'readExports',
                url: 'file:///repo/testHelpers/telemetryFixtures.js',
                lineNumber: 74,
                columnNumber: 2,
              },
              selfSize: 8192,
              children: [],
            },
          ],
        },
      ],
    },
    samples: [{ size: 8192, nodeId: 3, ordinal: 1 }],
  })

  /**
   * @param {object} [options] a protocol failure or oversized profile
   * @returns {object} protocol seam and call log; work and artifact writing use real code
   */
  const recorder = ({ failStart = false, value = profile() } = {}) => {
    const calls = []
    const session = {
      connect: () => calls.push('connect'),
      disconnect: () => calls.push('disconnect'),
      post(method, params, callback) {
        calls.push([method, params])
        if (failStart && method === 'HeapProfiler.startSampling')
          callback(new Error('sampling unavailable'))
        else callback(null, method === 'HeapProfiler.stopSampling' ? { profile: value } : {})
      },
    }
    const allocation = new ShareAllocationProfile({ directory, session: () => session })
    return { allocation, calls }
  }

  test('ranks stacks by self bytes without counting parent allocations twice', () => {
    expect(topStacks(profile(), 1)).toEqual({
      totalSelfBytes: 8256,
      allocationNodes: 2,
      omittedNodes: 1,
      top: [
        {
          nodeId: 3,
          selfBytes: 8192,
          stack: [
            { functionName: '(root)', url: '', line: 0, column: 0 },
            {
              functionName: 'seal',
              url: 'file:///repo/scripts/soak/telemetryChurn.js',
              line: 2888,
              column: 3,
            },
            {
              functionName: 'readExports',
              url: 'file:///repo/testHelpers/telemetryFixtures.js',
              line: 75,
              column: 3,
            },
          ],
        },
      ],
    })
  })

  test('brackets work with collected-object sampling and preserves the complete bounded profile', async () => {
    const { allocation, calls } = recorder()
    const result = await allocation.capture('reopen', 6, async () => {
      calls.push('work')
      return 'original result'
    })
    expect(result).toBe('original result')
    expect(calls).toEqual([
      'connect',
      [
        'HeapProfiler.startSampling',
        {
          samplingInterval: PROFILE_LIMITS.samplingInterval,
          includeObjectsCollectedByMajorGC: true,
          includeObjectsCollectedByMinorGC: true,
        },
      ],
      'work',
      ['HeapProfiler.stopSampling', {}],
      'disconnect',
    ])
    const raw = fs.readFileSync(path.join(directory, '0-reopen-6.heapprofile'))
    expect(JSON.parse(raw)).toEqual(profile())
    const summary = JSON.parse(fs.readFileSync(path.join(directory, '0-reopen-6.json')))
    expect(summary).toMatchObject({
      diagnostic: true,
      acceptanceEligible: false,
      stage: 'reopen',
      tick: 6,
      workCompleted: true,
      totalSelfBytes: 8256,
      profile: {
        name: '0-reopen-6.heapprofile',
        bytes: raw.length,
        sha256: crypto.createHash('sha256').update(raw).digest('hex'),
      },
    })
    await allocation.capture('cleanup', null, async () => {})
    expect(allocation.report()).toMatchObject({
      complete: true,
      stages: [
        { stage: 'reopen', tick: 6 },
        { stage: 'cleanup', tick: null },
      ],
    })
  })

  test('stops and disconnects on workload failure while retaining its allocation evidence', async () => {
    const { allocation, calls } = recorder()
    const error = new Error('original close failed')
    await expect(
      allocation.capture('cleanup', null, async () => {
        throw error
      })
    ).rejects.toBe(error)
    expect(calls.at(-1)).toBe('disconnect')
    const summary = JSON.parse(fs.readFileSync(path.join(directory, '0-cleanup-none.json')))
    expect(summary).toMatchObject({ workCompleted: false, errors: ['original close failed'] })
    expect(summary.profile.bytes).toBeGreaterThan(0)
    expect(allocation.report().complete).toBe(false)
  })

  test('fails unavailable sampling and artifact bounds without truncating a profile or running unprofiled work', async () => {
    const unavailable = recorder({ failStart: true })
    let ran = false
    await expect(
      unavailable.allocation.capture('reopen', 6, async () => {
        ran = true
      })
    ).rejects.toThrow('sampling unavailable')
    expect(ran).toBe(false)
    expect(unavailable.calls.at(-1)).toBe('disconnect')
    const oversized = recorder({
      value: { ...profile(), padding: 'x'.repeat(PROFILE_LIMITS.profileBytes) },
    })
    await expect(oversized.allocation.capture('cleanup', null, async () => {})).rejects.toThrow(
      'profile byte bound'
    )
    expect(fs.readdirSync(directory).filter((name) => name.endsWith('.heapprofile'))).toEqual([])
    expect(oversized.allocation.report().complete).toBe(false)
    const largeStack = profile()
    largeStack.head.callFrame.url = 'x'.repeat(PROFILE_LIMITS.summaryBytes)
    const summaryOverflow = recorder({ value: largeStack })
    await expect(
      summaryOverflow.allocation.capture('cleanup', null, async () => {})
    ).rejects.toThrow('summary byte bound')
    expect(fs.readdirSync(directory).filter((name) => name.endsWith('.heapprofile'))).toEqual([])
    const capped = recorder().allocation
    capped.stages.length = PROFILE_LIMITS.stages
    await expect(capped.capture('cleanup', null, async () => {})).rejects.toThrow('stage bound')
  })

  test('installs allocation sampling only in the enabled diagnostic CI share worker', async () => {
    for (const [kind, arm, env, enabled] of [
      ['shareConsumer', 'enabled', { CI: 'true', SHARE_PEAK_DIAGNOSTIC: '1' }, true],
      ['shareConsumer', 'disabled', { CI: 'true', SHARE_PEAK_DIAGNOSTIC: '1' }, false],
      ['shareConsumer', 'enabled', {}, false],
      ['producer', 'enabled', { CI: 'true', SHARE_PEAK_DIAGNOSTIC: '1' }, false],
    ]) {
      const owner = { kind, soak: {} }
      installSharePeak(owner, arm, { ...env, SHARE_PEAK_ROOT: directory })
      const Worker = owner.soak.SharePeakWorker
      expect(!!Worker && Worker !== DisabledShareWorker).toBe(enabled)
      if (!enabled) continue
      expect(Worker.prototype).toBeInstanceOf(TelemetryWorker)
      const { allocation, calls } = recorder()
      const worker = Object.create(Worker.prototype)
      worker.peak = { allocations: allocation }
      let finishSeal
      const sealing = new Promise((resolve) => {
        finishSeal = resolve
      })
      const claim = { tick: 6, from: 's0', to: 's1' }
      const errors = []
      const failure = new Error('cleanup failure')
      jest.spyOn(TelemetryWorker.prototype, 'reopen').mockImplementation(function () {
        this.sealing = { promise: sealing }
        return Promise.resolve(claim)
      })
      jest.spyOn(TelemetryWorker.prototype, 'cleanup').mockImplementation(async (ledger) => {
        expect(ledger).toBe(errors)
        ledger.push(failure)
      })
      try {
        const opening = worker.reopen({ tick: 6 })
        await new Promise((resolve) => setImmediate(resolve))
        expect(calls.some((call) => call[0] === 'HeapProfiler.stopSampling')).toBe(false)
        finishSeal()
        await expect(opening).resolves.toBe(claim)
        await worker.cleanup(errors)
        expect(errors).toEqual([failure])
        expect(allocation.report()).toMatchObject({
          complete: true,
          stages: [
            { stage: 'reopen', tick: 6 },
            { stage: 'cleanup', tick: null },
          ],
        })
      } finally {
        finishSeal()
        jest.restoreAllMocks()
      }
    }
  })
})
