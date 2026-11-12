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

  test('preserves counter regressions and missing sources as incomplete diagnostic evidence', () => {
    const { peak, state } = fixture()
    peak.poll()
    state.at = 20
    state.vmhwm = 1100
    peak.poll()
    expect(peak.report()).toMatchObject({ complete: false })
    expect(peak.report().readings.at(-1).VmHWM).toBe(1100)
    peak.proc = () => null
    state.at = 40
    peak.poll()
    expect(peak.report().errors).toEqual(
      expect.arrayContaining(['VmHWM decreased', 'proc status unavailable'])
    )
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
