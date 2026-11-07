const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const zlib = require('node:zlib')
const {
  digest,
  decodePoints,
  decodeMetricsData,
  liveFixture,
  p99,
  exactSet,
  schedule,
  validateSchedule,
  collectSession,
  noise,
  validateRequests,
  validateDelivery,
  validatePayload,
  payloadWorkload,
  withinGates,
  validateEnvelope,
  samePathControl,
  Table,
  EvidenceWriter,
  readArtifact,
  readChunks,
  verifyDirectory,
  MemoryProbe,
  SliceMonitor,
  signedDelta,
  rssDelta,
  CodecUnavailableError,
  UsageError,
  parseWorkerArgs,
  validateManifest,
  childCount,
  minimumBudget,
  memoryPairs,
  evaluate,
  numericProjection,
  verifyWorker,
  requestEvidence,
  expectedSample,
  JOBS,
  PayloadFixture,
  validatePayloadSample,
  publicOptions,
  assertPublicOptions,
  TelemetryPushProbe,
  ExportArchive,
  telemetryEvidence,
  makeValue,
  valueHash,
  deliveryEvidence,
  deliveryResult,
  RequestWorkload,
  PayloadWorkload,
  DeliveryWorkload,
  TelemetrySession,
  main,
} = require('../telemetry')
const Metrics = require('../../../src/telemetry/metrics')
const Payload = require('../../../src/telemetry/payload')
const { encodeMetricsData } = require('../../../src/telemetry/otlp')
const { version: SCOPE_VERSION } = require('../../../package.json')

const ROOT = path.resolve(__dirname, '../../..')
const TUPLE = ['socket', 'generation', 'correlation', 'send']
const MIB = 1048576
jest.setTimeout(60000)

const dirs = []
afterEach(() => {
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true })
})

/** @returns {string} a new empty private directory removed after the test */
const tmp = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-bench-spec-'))
  dirs.push(dir)
  return dir
}

/** @param {string[]} [order] ledger receiving `gc` and `memory` in call order
 * @returns {{memory:Function,gc:Function}} deterministic injected readings; no host memory is consulted
 */
const memoryHooks = (order = []) => {
  let heap = 1000
  return {
    memory: () => {
      order.push('memory')
      heap += 10
      return { heapUsed: heap, external: 500, rss: 40000 }
    },
    gc: () => order.push('gc'),
  }
}

const IDENTITY = { runId: 'run-1', manifestSha256: 'b'.repeat(64) }

/** @param {object} fields extra declaration fields @returns {object} frozen run identity for a worker */
const declaration = (fields) => ({
  runId: IDENTITY.runId,
  tree: 'candidate',
  driverSha256: 'a'.repeat(64),
  manifestSha256: IDENTITY.manifestSha256,
  configSha256: 'c'.repeat(64),
  control: 0,
  ...fields,
})

/** @param {object} result workload result @param {EvidenceWriter} evidence writer @param {object} decl declaration
 * @returns {object} the envelope a worker would print, with a clean report
 */
const envelopeOf = (result, evidence, decl) => ({
  schemaVersion: 1,
  runId: decl.runId,
  tree: decl.tree,
  driverSha256: decl.driverSha256,
  manifestSha256: decl.manifestSha256,
  configSha256: decl.configSha256,
  ...result,
  mode: decl.mode,
  control: decl.control,
  artifacts: evidence.artifacts(),
  clean: { sockets: 0, timers: 0, children: 0, pending: 0, files: 0 },
})

/** Runs a real tiny requests worker and returns what a supervisor would verify.
 * @param {object} [options] telemetry arm and counts @returns {Promise<object>} directory, envelope, declaration
 */
const realRequestWorker = async ({
  telemetry = 'active',
  iterations = 4,
  warmup = 2,
  nodes = 2,
} = {}) => {
  const dir = tmp()
  const evidence = new EvidenceWriter(dir, { identity: IDENTITY })
  const config = { iterations, warmup, nodes, telemetry }
  const workload = new RequestWorkload({
    root: ROOT,
    config,
    control: 0,
    evidence,
    hooks: { memory: memoryHooks() },
  })
  let result
  try {
    result = await workload.run()
  } finally {
    await workload.close()
  }
  const decl = declaration({ mode: 'requests', config, root: ROOT })
  return { dir, evidence, config, decl, envelope: envelopeOf(result, evidence, decl) }
}

/** @returns {object} independently declared and observed minimal real-work oracle inputs */
const requestCase = () => ({
  result: {
    telemetry: 'active',
    completedPairs: 2,
    physicalConnections: 1,
    successfulSamples: 2,
    observerCalls: 2,
    aggregateSamples: 2,
    cpuMicros: 4,
    elapsedNs: 6000,
    cpuNsPerPair: 2000,
    elapsedNsPerPair: 3000,
  },
  evidence: {
    count: 2,
    expected: ['c1:q1', 'c1:q2'],
    writes: ['c1:q1', 'c1:q2'],
    responses: ['c1:q2', 'c1:q1'],
    returned: ['c1:q1', 'c1:q2'],
    connected: ['c1'],
    expectedConnections: ['c1'],
    sampled: 2,
    observations: 2,
    aggregateSamples: 2,
    connectionSum: 1,
  },
})

/** @returns {object} tiny untimed eachBatch oracle; final acceptance uses the fixed full denominators */
const deliveryCase = () => ({
  result: {
    handler: 'eachBatch',
    messages: 4,
    size: 200,
    deliveredMessages: 4,
    callbacks: 2,
    gapSamples: 1,
    deliveryGapP99Ms: 2,
    p99Ms: 4,
    messagesPerSecond: 4e9 / 6000,
    elapsedNs: 6000,
  },
  evidence: {
    expected: ['0', '1', '2', '3'],
    durable: ['3', '1', '2', '0'],
    delivered: ['0', '1', '2', '3'],
    measuredBytes: [200, 200, 200, 200],
    entries: [1, 3],
    gaps: [2],
    latencies: [1, 2, 3, 4],
    batchSizes: [2, 2],
    expectedCommits: { p0: '4' },
    finalCommits: { p0: '4' },
    minimumBatchGaps: 1,
    minimumMessageGaps: 1,
  },
})

/** @returns {object} minimal scalar envelope; its values are literals, never clock measurements */
const envelope = () => ({
  schemaVersion: 1,
  runId: 'run-1',
  tree: 'candidate',
  driverSha256: 'a'.repeat(64),
  manifestSha256: 'b'.repeat(64),
  configSha256: 'c'.repeat(64),
  mode: 'requests',
  control: 0,
  ...requestCase().result,
  artifacts: [{ name: 'requests.bin', sha256: 'd'.repeat(64), bytes: 12 }],
})

/** @param {object} options payload construction dimensions @returns {Promise<object>} actual untimed payload */
const payloadCase = async (options = {}) => {
  const workload = payloadWorkload(Metrics, {
    points: 257,
    attributes: 'poor',
    temporality: 'delta',
    ...options,
  })
  const payload = new Payload({})
  const result = await payload.buildPayload(workload.snapshot, {
    telemetryMaxBytes: 1048576,
    acceptedCompressionTypes: [],
    isCurrent: () => true,
  })
  const key = ({ name, attributes }) => JSON.stringify([name, attributes])
  return {
    workload,
    payload,
    result,
    evidence: {
      raw: result.metrics,
      transmitted: result.metrics,
      cap: 1048576,
      expected: workload.expected,
      included: workload.expected.map(key),
      omitted: [],
      expectedOmitted: [],
    },
  }
}

describe('telemetry benchmark controls', () => {
  test('three arms retain nine observations plus one discarded session warmup', () => {
    const declared = schedule(['previous', 'candidate'], 'previous', 9)
    const writes = []
    const rows = collectSession(
      declared,
      ['previous', 'candidate'].map((label) => ({
        label,
        run: () => ({ cpu: 10, latency: 20 }),
      })),
      (row) => writes.push(row)
    )
    expect(rows).toHaveLength(30)
    expect(writes).toEqual(rows)
    expect(rows.filter(({ warmup }) => warmup)).toHaveLength(3)
    expect(rows.filter(({ warmup }) => !warmup)).toHaveLength(27)
    expect(noise(declared, rows, ['cpu'], ['latency']).void).toBe(false)
  })

  test('five arms retain ten observations at equal positions', () => {
    const declared = schedule(['previous', 'off', 'empty', 'active'], 'previous', 10)
    const rows = collectSession(
      declared,
      declared.labels.slice(0, -1).map((label) => ({ label, run: () => ({ v: 1 }) })),
      () => {}
    )
    expect(rows).toHaveLength(55)
    for (const label of declared.labels)
      for (let position = 0; position < 5; position += 1) {
        expect(
          rows.filter((row) => row.label === label && row.position === position && !row.warmup)
        ).toHaveLength(2)
      }
  })

  test('seven runs for three arms are rejected before work', () => {
    expect(() => schedule(['previous', 'candidate'], 'previous', 7)).toThrow()
  })

  test('post hoc balanced round removal and position changes are rejected', () => {
    const declared = schedule(['previous', 'candidate'], 'previous', 9)
    expect(() =>
      validateSchedule(
        declared,
        declared.rows.filter(({ round }) => round < 7)
      )
    ).toThrow()
    const changed = declared.rows.map((row) => ({ ...row }))
    changed[4].position = 0
    expect(() => validateSchedule(declared, changed)).toThrow()
  })

  test('all measured observations enter the inherited median', () => {
    const declared = schedule(['previous', 'candidate'], 'previous', 9)
    const rows = declared.rows.map((row) => ({
      ...row,
      result: { cpu: row.warmup ? 999999 : row.round },
    }))
    expect(noise(declared, rows, ['cpu'], []).cost.arms.previous.cpu).toMatchObject({
      n: 9,
      median: 5,
    })
  })

  test('independent CPU and latency noise limits preserve void evidence', () => {
    const declared = schedule(['previous', 'candidate'], 'previous', 9)
    const make = (cpu, latency) =>
      declared.rows.map((row) => ({
        ...row,
        result: {
          cpu: row.label === 'previous~AA' ? cpu : 100,
          latency: row.label === 'previous~AA' ? latency : 100,
        },
      }))
    expect(noise(declared, make(103, 105), ['cpu'], ['latency']).void).toBe(false)
    expect(noise(declared, make(103.1, 100), ['cpu'], ['latency']).void).toBe(true)
    expect(noise(declared, make(100, 105.1), ['cpu'], ['latency']).void).toBe(true)
  })

  test('same path controls declare exact costs using a virtual clock', async () => {
    for (const [mode, amount] of [
      ['requests', 10000],
      ['payload', 100000000],
      ['delivery', 1000000],
    ]) {
      let time = 0n
      const now = () => (time += 10000n)
      const waits = []
      const wait = async (milliseconds) => {
        waits.push(milliseconds)
      }
      expect(await samePathControl(mode, 1, now, wait)).toBe(amount)
      expect(waits).toEqual(mode === 'delivery' ? [1] : [])
      expect(
        samePathControl(mode, 0, () => {
          throw new Error('disabled clock used')
        })
      ).toBe(0)
    }
  })

  test('schema accepts declared identities and rejects missing nonfinite and drifting fields', () => {
    const original = envelope()
    expect(() => validateEnvelope(original, original)).not.toThrow()
    for (const change of [
      { schemaVersion: 2 },
      { completedPairs: 1.5 },
      { elapsedNs: NaN },
      { cpuNsPerPair: undefined },
      { tree: 'other' },
      { control: 2 },
      { artifacts: [{ name: '../escape', sha256: 'd'.repeat(64), bytes: 12 }] },
    ]) {
      expect(() => validateEnvelope({ ...original, ...change }, original)).toThrow()
    }
  })

  test('full identities reject substitutions and duplicates with equal counts', () => {
    expect(() => exactSet(['a', 'b'], ['a', 'c'])).toThrow()
    expect(() => exactSet(['a', 'b'], ['a', 'a'])).toThrow()
    expect(() => exactSet(['a', 'b'], ['b', 'a'])).not.toThrow()
    expect(digest(['a', 'b'])).not.toBe(digest(['a', 'c']))
  })

  test('request evidence requires real connections and completed correlations', () => {
    const { result, evidence } = requestCase()
    expect(() => validateRequests(result, evidence)).not.toThrow()
    expect(() => validateRequests(result, { ...evidence, responses: ['c1:q1'] })).toThrow()
    expect(() => validateRequests(result, { ...evidence, connected: [] })).toThrow()
    expect(() => validateRequests({ ...result, completedPairs: 3 }, evidence)).toThrow()
  })

  test('collector bypass fails even if request and reported counts remain complete', () => {
    const { result, evidence } = requestCase()
    expect(() => validateRequests(result, { ...evidence, aggregateSamples: 0 })).toThrow()
    expect(() =>
      validateRequests({ ...result, observerCalls: 0 }, { ...evidence, observations: 0 })
    ).toThrow()
  })

  test('message counts remain separate from callbacks and batch sums', () => {
    const { result, evidence } = deliveryCase()
    expect(() => validateDelivery(result, evidence)).not.toThrow()
    expect(() => validateDelivery({ ...result, deliveredMessages: 2 }, evidence)).toThrow()
    expect(() => validateDelivery(result, { ...evidence, batchSizes: [1, 1] })).toThrow()
    expect(() => validateDelivery(result, { ...evidence, minimumBatchGaps: 1000 })).toThrow()
  })

  test('durable log handler corpus commits and actual payload sizes are independent', () => {
    const { result, evidence } = deliveryCase()
    for (const change of [
      { durable: ['0', '1', '2', '2'] },
      { delivered: ['0', '1', '2', 'other'] },
      { finalCommits: { p0: '3' } },
      { measuredBytes: [200, 200, 200, 199] },
      { gaps: [] },
    ]) {
      expect(() => validateDelivery(result, { ...evidence, ...change })).toThrow()
    }
  })

  test.each([1, 257])(
    'legal %i point workload is built by production validation and independently decoded',
    async (points) => {
      const { result, evidence, workload } = await payloadCase({ points })
      try {
        expect(workload.snapshot.points).toHaveLength(points)
        expect(() => validatePayload(result, evidence)).not.toThrow()
        if (points === 257) {
          expect(
            workload.snapshot.points.filter(({ name }) => !name.startsWith('org.apache.kafka.'))
          ).toHaveLength(128)
          const descriptor = {
            name: 'com.example.extra',
            type: 'sum',
            value: 1n,
          }
          expect(() => workload.metrics.register(descriptor)).toThrow()
        }
      } finally {
        workload.metrics.abandon(workload.snapshot)
      }
    }
  )

  test.each(['delta', 'cumulative'])(
    'legal %s snapshots preserve literal values for both attribute workloads',
    async (temporality) => {
      for (const attributes of ['compressible', 'poor']) {
        const { result, evidence, workload } = await payloadCase({
          temporality,
          attributes,
        })
        try {
          expect(() => validatePayload(result, evidence)).not.toThrow()
        } finally {
          workload.metrics.abandon(workload.snapshot)
        }
      }
    }
  )

  test('payload evidence rejects fabricated bytes missing points values and omitted sets', async () => {
    const { result, evidence, workload } = await payloadCase({ points: 1 })
    try {
      expect(() => validatePayload({ ...result, rawBytes: 748355 }, evidence)).toThrow()
      expect(() =>
        validatePayload({ ...result, transmittedBytes: result.transmittedBytes + 1 }, evidence)
      ).toThrow()
      expect(() => validatePayload(result, { ...evidence, included: [] })).toThrow()
      expect(() =>
        validatePayload(result, { ...evidence, expectedOmitted: ['invented'] })
      ).toThrow()
      expect(() =>
        validatePayload(result, {
          ...evidence,
          expected: [{ ...evidence.expected[0], value: '99' }],
        })
      ).toThrow()
      expect(() => validatePayload(result, { ...evidence, omitted: ['invented'] })).toThrow()
      expect(() => validatePayload({ ...result, fittingPasses: 9 }, evidence)).toThrow()
    } finally {
      workload.metrics.abandon(workload.snapshot)
    }
  })

  test('illegal descriptors cannot seed a sender snapshot', () => {
    const metrics = new Metrics({ kind: 'producer' })
    for (const descriptor of [
      { name: 'org.apache.kafka.fake', type: 'sum', value: 1n },
      { name: 'com.example.bad', type: 'gauge', value: NaN },
      { name: 'com.example.bad', type: 'sum', value: -1n },
      {
        name: 'com.example.bad',
        type: 'gauge',
        value: 1,
        attributes: { key: 'x'.repeat(257) },
      },
    ])
      expect(() => metrics.register(descriptor)).toThrow()
  })

  test('gate arithmetic keeps exact fixed ceilings on literal observations', () => {
    expect(
      withinGates('requests', { telemetry: 'off', cpuNsPerPair: 103 }, { cpuNsPerPair: 100 })
    ).toBe(true)
    expect(
      withinGates('requests', { telemetry: 'off', cpuNsPerPair: 103.01 }, { cpuNsPerPair: 100 })
    ).toBe(false)
    expect(
      withinGates('requests', { telemetry: 'active', cpuNsPerPair: 105.01 }, { cpuNsPerPair: 100 })
    ).toBe(false)
    const payload = {
      sliceP99Ms: 10,
      cpuP99Ms: 25,
      wallP99Ms: 100,
      fittingPasses: 8,
      heapExternalDeltaBytes: 8388608,
      rssDeltaBytes: 67108864,
    }
    expect(withinGates('payload', payload, {})).toBe(true)
    expect(withinGates('payload', { ...payload, wallP99Ms: 100.001 }, {})).toBe(false)
    expect(
      withinGates(
        'delivery',
        { messagesPerSecond: 97, p99Ms: 22 },
        { messagesPerSecond: 100, p99Ms: 20 }
      )
    ).toBe(true)
    expect(
      withinGates(
        'delivery',
        { messagesPerSecond: 96.999, p99Ms: 22 },
        { messagesPerSecond: 100, p99Ms: 20 }
      )
    ).toBe(false)
    expect(
      withinGates(
        'delivery',
        { messagesPerSecond: 100, p99Ms: 22.001 },
        { messagesPerSecond: 100, p99Ms: 20 }
      )
    ).toBe(false)
    expect(p99([100, 1, 2])).toBe(100)
  })
})

describe('telemetry request workload', () => {
  const tiny = (telemetry) => ({ iterations: 8, warmup: 2, nodes: 2, telemetry })

  /** @param {object} options workload options @returns {Promise<object>} result after awaited close */
  const execute = async (options) => {
    const evidence = new EvidenceWriter(tmp(), { identity: IDENTITY })
    const workload = new RequestWorkload({ root: ROOT, control: 0, evidence, ...options })
    try {
      return { result: await workload.run(), evidence }
    } finally {
      await workload.close()
    }
  }

  test('real request work crosses prepare queue socket and fulfill', async () => {
    const { result, evidence } = await execute({
      config: tiny('off'),
      hooks: { memory: memoryHooks() },
    })
    const read = (name, columns) => {
      const artifact = evidence.artifacts().find((a) => a.name === name)
      return Table.from(readArtifact(evidence.directory, artifact), columns)
    }
    const writes = read('requests-write.bin', TUPLE)
    const responses = read('requests-response.bin', TUPLE)
    const returned = read('requests-returned.bin', TUPLE)
    expect([writes.length, responses.length, returned.length]).toEqual([8, 8, 8])
    expect(writes.column('send')).toEqual([0, 1, 2, 3, 4, 5, 6, 7])
    expect(writes.column('socket')).toEqual([0, 1, 0, 1, 0, 1, 0, 1])
    expect(returned.column('correlation')).toEqual(writes.column('correlation'))
    expect(result.physicalConnections).toBe(2)
    expect(result.successfulSamples).toBe(8)
    expect(result.completedPairs).toBe(8)
  })

  test('a no op connect cannot replace physical work', async () => {
    class NoSocketConnection {
      constructor() {
        this.generation = 1
        this.requestQueue = { pending: [], inflight: new Map() }
      }

      setTelemetryObserver() {}

      async connect() {
        return true
      }

      async send() {
        return { brokers: [], topicMetadata: [] }
      }

      async disconnect() {}
    }
    await expect(
      execute({
        config: tiny('off'),
        hooks: { memory: memoryHooks(), Connection: NoSocketConnection },
      })
    ).rejects.toThrow(assert.AssertionError)
    const Real = require('../../../src/network/connection')
    let created = 0
    class Counting extends Real {
      constructor(options) {
        super({
          ...options,
          socketFactory: (factoryOptions) => {
            created += 1
            return options.socketFactory(factoryOptions)
          },
        })
      }
    }
    const { result } = await execute({
      config: tiny('off'),
      hooks: { memory: memoryHooks(), Connection: Counting },
    })
    expect(created).toBe(2)
    expect(result.physicalConnections).toBe(2)
  })

  test('off empty and active observe only their declared work', async () => {
    const expected = {
      off: { observerCalls: 0, aggregateSamples: 0 },
      empty: { observerCalls: 8, aggregateSamples: 0 },
      active: { observerCalls: 8, aggregateSamples: 8 },
    }
    for (const telemetry of ['off', 'empty', 'active']) {
      const order = []
      const hot = new Set(['wire', 'response', 'returned', 'observe'])
      const { result } = await execute({
        config: tiny(telemetry),
        hooks: {
          memory: memoryHooks(order),
          trace: (event) => {
            if (!hot.has(event)) order.push(event)
          },
          load: (file) => {
            if (telemetry === 'off' && /src[\\/]telemetry[\\/]/.test(file))
              throw new Error('telemetry module imported')
            return require(file)
          },
        },
      })
      expect(result).toMatchObject(expected[telemetry])
      expect(result.physicalConnections).toBe(2)
      expect(order).toEqual([
        'allocate',
        'gc',
        'memory',
        'setup',
        'window-start',
        'window-end',
        'gc',
        'memory',
        'after',
      ])
    }
  })

  test('busy observer is entered from the completed request hook', async () => {
    const order = []
    class SpyMetrics extends Metrics {
      observeRequest(slot, duration) {
        order.push('original')
        return super.observeRequest(slot, duration)
      }
    }
    const hooks = {
      memory: memoryHooks(),
      trace: (event) => order.push(event),
      control: (mode, enabled) => order.push(`control:${mode}:${enabled}`),
      load: (file) => (/telemetry[\\/]metrics$/.test(file) ? SpyMetrics : require(file)),
    }
    const { result } = await execute({
      config: { iterations: 2, warmup: 0, nodes: 1, telemetry: 'active' },
      control: 1,
      hooks,
    })
    const pair = ['wire', 'response', 'control:requests:1', 'observe', 'original', 'returned']
    expect(order.filter((event) => !/^(allocate|setup|window-.*|after)$/.test(event))).toEqual([
      ...pair,
      ...pair,
    ])
    expect(result.successfulSamples).toBe(2)
    const idle = await execute({
      config: { iterations: 2, warmup: 0, nodes: 1, telemetry: 'active' },
      hooks: {
        memory: memoryHooks(),
        control: () => {
          throw new Error('control clock read with control 0')
        },
      },
    })
    expect(idle.result.successfulSamples).toBe(2)
  })

  test('omitted completion and collector bypass cannot keep a reported pass', async () => {
    const config = { iterations: 2, warmup: 0, nodes: 1, telemetry: 'off' }
    const table = (...rows) => {
      const t = new Table(TUPLE, 2)
      for (const row of rows) t.add(...row)
      return t
    }
    const all = table([0, 1, 0, 0], [0, 1, 1, 1])
    const result = {
      telemetry: 'off',
      completedPairs: 2,
      physicalConnections: 1,
      successfulSamples: 2,
      observerCalls: 0,
      aggregateSamples: 0,
      cpuMicros: 4,
      elapsedNs: 6000,
      cpuNsPerPair: 2000,
      elapsedNsPerPair: 3000,
    }
    const meta = { physical: ['0.1'], successes: 2, connectionSum: 0, pairs: null }
    const make = (responses) =>
      requestEvidence(
        config,
        { writes: all, responses, returned: all, observed: new Table(['node', 'duration'], 2) },
        meta
      )
    expect(() => validateRequests(result, make(all))).not.toThrow()
    expect(() => validateRequests(result, make(table([0, 1, 0, 0])))).toThrow()
    await expect(
      execute({
        config: tiny('active'),
        hooks: { memory: memoryHooks(), bypassSink: true },
      })
    ).rejects.toThrow(assert.AssertionError)
    const { result: real } = await execute({
      config: tiny('active'),
      hooks: { memory: memoryHooks() },
    })
    expect(real.aggregateSamples).toBe(8)
  })
})

describe('telemetry payload workload', () => {
  const dims = { points: 257, attributes: 'poor', temporality: 'delta' }
  const config = (extra = {}) => ({
    snapshots: 2,
    warmup: 1,
    telemetryMaxBytes: 1048576,
    codec: 'none',
    ...dims,
    ...extra,
  })

  /** @param {object} options workload options @returns {Promise<object>} result, evidence and clean report */
  const execute = async ({ config: cfg, control = 0, hooks = {} }) => {
    const evidence = new EvidenceWriter(tmp(), { identity: IDENTITY, maxBytes: 128 * MIB })
    const workload = new PayloadWorkload({
      root: ROOT,
      config: cfg,
      control,
      evidence,
      hooks: { memory: memoryHooks(), resourceUsage: () => ({ maxRSS: 40 }), ...hooks },
    })
    try {
      return { result: await workload.run(), evidence }
    } finally {
      await workload.close()
    }
  }

  test('maximal sender input is admitted before serialization', () => {
    const fixture = new PayloadFixture(Metrics, dims)
    const snapshot = fixture.next(1)
    try {
      expect(snapshot.points).toHaveLength(257)
      const builtIn = snapshot.points.filter(({ name }) => name.startsWith('org.apache.kafka.'))
      expect(builtIn).toHaveLength(129)
      expect(snapshot.points.length - builtIn.length).toBe(128)
      expect(() =>
        fixture.metrics.register({ name: 'com.example.extra', type: 'sum', value: 1n })
      ).toThrow()
      for (const bad of [
        { name: 'org.apache.kafka.fake', type: 'sum', value: 1n },
        { name: 'com.example.bad', type: 'gauge', value: NaN },
        { name: 'com.example.bad', type: 'gauge', value: 1, attributes: { key: 'x'.repeat(257) } },
      ])
        expect(() => fixture.metrics.register(bad)).toThrow()
    } finally {
      fixture.metrics.abandon(snapshot)
    }
  })

  test('actual codec bytes and independently decoded identities determine evidence', async () => {
    const build = async (points, accepted, k = 1) => {
      const fixture = new PayloadFixture(Metrics, { ...dims, points })
      const snapshot = fixture.next(k)
      const built = await new Payload({}).buildPayload(snapshot, {
        telemetryMaxBytes: 1048576,
        acceptedCompressionTypes: accepted,
        isCurrent: () => fixture.metrics.isCurrent(snapshot),
      })
      return built
    }
    const cfg = (points, codec) => ({ ...dims, points, codec, telemetryMaxBytes: 1048576 })
    const judge = (built, c) =>
      validatePayloadSample(c, 1, SCOPE_VERSION, {
        ...built,
        includedKeys: built.includedIdentities.map(({ key }) => key),
        omittedKeys: [],
      })
    const gzip = await build(257, [1])
    expect(gzip.compressionType).toBe(1)
    expect(judge(gzip, cfg(257, 'gzip'))).toHaveLength(gzip.rawBytes)
    expect(zlib.gunzipSync(gzip.metrics)).toHaveLength(gzip.rawBytes)
    expect(gzip.transmittedBytes).toBeLessThan(gzip.rawBytes)
    const none = await build(257, [])
    expect(judge(none, cfg(257, 'none'))).toHaveLength(none.rawBytes)
    // eslint-disable-next-line n/no-unsupported-features/node-builtins -- feature detection
    if (typeof zlib.zstdCompressSync === 'function') {
      const zstd = await build(257, [4])
      expect(zstd.compressionType).toBe(4)
      expect(judge(zstd, cfg(257, 'zstd'))).toHaveLength(zstd.rawBytes)
    }
    const small = await build(1, [1])
    expect(small.compressionType).toBe(0)
    expect(small.rawBytes).toBeLessThanOrEqual(1000)
    expect(() => judge(small, cfg(1, 'gzip'))).not.toThrow()
    expect(() => judge({ ...gzip, rawBytes: gzip.rawBytes + 1 }, cfg(257, 'gzip'))).toThrow()
    expect(() =>
      judge({ ...gzip, transmittedBytes: gzip.transmittedBytes + 1 }, cfg(257, 'gzip'))
    ).toThrow()
    const corrupt = Buffer.from(gzip.metrics)
    corrupt[10] ^= 0xff
    expect(() => judge({ ...gzip, metrics: corrupt }, cfg(257, 'gzip'))).toThrow()
  })

  test('a smaller reachable cap retains the exact complete point prefix', async () => {
    const fixture = new PayloadFixture(Metrics, dims)
    const snapshot = fixture.next(1)
    const identity = (point) => point.identity.key
    const byName = (suffix) => (p) => p.name.endsWith(suffix)
    const connection = snapshot.points.filter(byName('.connection.creation.total'))
    const pairs = Array.from({ length: 64 }, (_, node) =>
      snapshot.points.filter(
        (p) =>
          /\.node\.request\.latency\.(avg|max)$/.test(p.name) &&
          p.attributes[0].value === String(node)
      )
    )
    const custom = snapshot.points
      .filter((p) => !p.name.startsWith('org.apache.kafka.'))
      .sort((a, b) => (identity(a) < identity(b) ? -1 : 1))
    const groups = [connection, ...pairs, ...custom.map((p) => [p])]
    const sizeOf = (count) =>
      encodeMetricsData(snapshot, { points: groups.slice(0, count).flat() }).length
    const keep = 100
    const cap = sizeOf(keep)
    expect(sizeOf(keep + 1)).toBeGreaterThan(cap)
    const built = await new Payload({}).buildPayload(snapshot, {
      telemetryMaxBytes: cap,
      acceptedCompressionTypes: [],
      isCurrent: () => fixture.metrics.isCurrent(snapshot),
    })
    const included = groups.slice(0, keep).flat().map(identity)
    const omitted = groups.slice(keep).flat().map(identity)
    expect(built.includedIdentities.map(({ key }) => key)).toEqual(expect.arrayContaining(included))
    expect(built.includedIdentities).toHaveLength(included.length)
    expect(built.fittingPasses).toBeLessThanOrEqual(8)
    const evidence = {
      raw: built.metrics,
      transmitted: built.metrics,
      cap,
      expected: expectedSample(dims, 1, SCOPE_VERSION).points,
      included,
      omitted,
      expectedOmitted: omitted,
    }
    expect(() =>
      validatePayload(
        {
          rawBytes: built.rawBytes,
          transmittedBytes: built.transmittedBytes,
          fittingPasses: built.fittingPasses,
        },
        evidence
      )
    ).not.toThrow()
    const swapped = [...omitted]
    swapped[0] = included[included.length - 1]
    expect(swapped).toHaveLength(omitted.length)
    expect(() =>
      validatePayload(
        {
          rawBytes: built.rawBytes,
          transmittedBytes: built.transmittedBytes,
          fittingPasses: built.fittingPasses,
        },
        { ...evidence, omitted: swapped }
      )
    ).toThrow()
    fixture.metrics.abandon(snapshot)
  })

  test('serializer delay and continuation slices belong to the measured build', async () => {
    let time = 0n
    const now = () => time
    const order = []
    class SpyPayload extends Payload {
      async buildPayload(snapshot, options) {
        order.push('build')
        return super.buildPayload(snapshot, options)
      }
    }
    const hooks = {
      now,
      control: (mode, enabled) => {
        order.push(`control:${mode}:${enabled}`)
        if (enabled) time += 100000000n
        return enabled ? 100000000 : 0
      },
      slice: { now, asyncHooks: { createHook: () => ({ enable() {}, disable() {} }) } },
      load: (file) => (/telemetry[\\/]payload$/.test(file) ? SpyPayload : require(file)),
    }
    const cfg = config({ points: 1 })
    const slow = await execute({ config: cfg, control: 1, hooks })
    expect(slow.result.sliceP99Ms).toBe(100)
    expect(slow.result.wallP99Ms).toBe(100)
    expect(order.slice(0, 2)).toEqual(['control:payload:1', 'build'])
    expect(order.filter((event) => event === 'build')).toHaveLength(3)
    order.length = 0
    const normal = await execute({ config: cfg, control: 0, hooks })
    expect(normal.result.sliceP99Ms).toBe(0)
    expect(order.slice(0, 2)).toEqual(['control:payload:0', 'build'])
    const callbacks = {}
    const clock = { value: 0n }
    const monitor = new SliceMonitor({
      now: () => clock.value,
      asyncHooks: {
        createHook: (c) => {
          Object.assign(callbacks, c)
          return {
            enable: () => (callbacks.enabled = true),
            disable: () => (callbacks.enabled = false),
          }
        },
      },
    })
    monitor.begin()
    expect(callbacks.enabled).toBe(true)
    monitor.call(() => {
      callbacks.init(10, 'PROMISE', 1)
      clock.value += 2000000n
    })
    callbacks.before(10)
    clock.value += 7000000n
    callbacks.after(10)
    callbacks.init(11, 'PROMISE', 99)
    callbacks.before(11)
    clock.value += 50000000n
    callbacks.after(11)
    const slices = monitor.end()
    expect(slices.slicesNs).toEqual([2000000n, 7000000n])
    expect(slices.maxSliceMs).toBe(7)
    expect(callbacks.enabled).toBe(false)
  })

  test('all snapshots retain their declared temporality and literal values', async () => {
    for (const temporality of ['delta', 'cumulative']) {
      for (const attributes of ['compressible', 'poor']) {
        const cfg = config({ temporality, attributes })
        const { result, evidence } = await execute({ config: cfg })
        const rows = Table.from(
          readArtifact(
            evidence.directory,
            evidence.artifacts().find((a) => a.name === 'payload-rows.bin')
          ),
          [
            'ordinal',
            'rawBytes',
            'transmittedBytes',
            'compressionType',
            'codecAttempts',
            'fittingPasses',
            'cpuMs',
            'wallMs',
            'sliceMs',
            'heapExternal',
          ]
        )
        expect(rows.column('ordinal')).toEqual([1, 2])
        const decl = declaration({ mode: 'payload', config: cfg, root: ROOT })
        const env = envelopeOf(result, evidence, decl)
        expect(() => verifyWorker(env, decl, evidence.directory)).not.toThrow()
        expect(() =>
          verifyWorker({ ...env, rawBytes: env.rawBytes + 1 }, decl, evidence.directory)
        ).toThrow()
      }
    }
    const fixture = new PayloadFixture(Metrics, { ...dims, points: 257 })
    const snapshot = fixture.next(1)
    const missing = encodeMetricsData(snapshot, { points: snapshot.points.slice(1) })
    expect(() =>
      validatePayloadSample(
        { ...dims, codec: 'none', telemetryMaxBytes: 1048576 },
        1,
        SCOPE_VERSION,
        {
          compressionType: 0,
          codecAttempts: 0,
          rawBytes: missing.length,
          transmittedBytes: missing.length,
          fittingPasses: 1,
          metrics: missing,
        }
      )
    ).toThrow()
    fixture.metrics.abandon(snapshot)
  })

  test('every payload invocation enforces memory ceilings and its actual RSS baseline', async () => {
    for (const field of ['heap', 'rss', 'baseline']) {
      const cfg = config({ points: 1 })
      const { result, evidence } = await execute({ config: cfg })
      const decl = declaration({ mode: 'payload', config: cfg, root: ROOT })
      const env = envelopeOf(result, evidence, decl)
      const file = path.join(evidence.directory, 'payload-memory.json')
      const memory = JSON.parse(fs.readFileSync(file, 'utf8'))
      if (field === 'heap') {
        memory.after.heapUsed = memory.before.heapUsed + 8388609
        memory.after.external = memory.before.external
        memory.signed = signedDelta(memory.before, memory.after)
        env.heapExternalDeltaBytes = 8388609
      } else {
        if (field === 'baseline') memory.rssBaselineBytes += 1024
        else memory.maxRssKiB = Math.ceil(memory.before.rss / 1024) + 65537
        memory.maxRssBytes = memory.maxRssKiB * 1024
        memory.rssDeltaBytes = rssDelta(memory.maxRssKiB, memory.rssBaselineBytes)
        env.rssDeltaBytes = memory.rssDeltaBytes
      }
      const bytes = Buffer.from(JSON.stringify(memory))
      fs.writeFileSync(file, bytes)
      Object.assign(
        env.artifacts.find((a) => a.name === 'payload-memory.json'),
        {
          bytes: bytes.length,
          sha256: require('node:crypto').createHash('sha256').update(bytes).digest('hex'),
        }
      )
      expect(() => verifyWorker(env, decl, evidence.directory)).toThrow()
    }
    for (const label of ['normal', 'slow', 'normal~AA']) {
      const { declared, rows } = syntheticRows('payload', ({ label }) =>
        METRICS.payload(label === 'slow')
      )
      rows.find((r) => !r.warmup && r.label === label).result.rssDeltaBytes = 67108865
      const verdict = evaluate('payload', declared, rows)
      expect(verdict.failures).toContainEqual(
        expect.objectContaining({ arm: label, fields: ['rssDeltaBytes'], expected: false })
      )
    }
  })

  test('codec availability and resource high water cannot silently become passing zeroes', async () => {
    await expect(
      execute({ config: config({ codec: 'zstd' }), hooks: { zstdSupported: false } })
    ).rejects.toThrow(CodecUnavailableError)
    class FallbackPayload extends Payload {
      buildPayload(snapshot, options) {
        return super.buildPayload(snapshot, { ...options, acceptedCompressionTypes: [] })
      }
    }
    await expect(
      execute({
        config: config({ codec: 'gzip' }),
        hooks: {
          load: (file) => (/telemetry[\\/]payload$/.test(file) ? FallbackPayload : require(file)),
        },
      })
    ).rejects.toThrow(assert.AssertionError)
    expect(() => samePathControl('payload', 2)).toThrow()
    expect(() => samePathControl('side', 1)).toThrow()
    expect(rssDelta(65536, 0)).toBe(67108864)
    expect(rssDelta(100, 204800)).toBe(0)
    expect(signedDelta({ heapUsed: 10, external: 5 }, { heapUsed: 7, external: 9 })).toEqual({
      heap: -3,
      external: 4,
      sum: 1,
    })
    expect(() =>
      signedDelta({ heapUsed: NaN, external: 5 }, { heapUsed: 7, external: 9 })
    ).toThrow()
    expect(() => rssDelta(NaN, 0)).toThrow()
    await expect(
      execute({
        config: config(),
        hooks: { memoryProbe: new MemoryProbe({ memory: memoryHooks().memory, gc: null }) },
      })
    ).rejects.toThrow(assert.AssertionError)
  })
})

const SEND = ['ordinal', 'sendMs', 'hash']
const HANDLER = ['ordinal', 'partition', 'offset', 'hash', 'entryMs', 'callback', 'bytes']
const CALLBACK = ['entryMs', 'size', 'firstOrdinal']
const DURABLE = ['ordinal', 'partition', 'offset', 'hash', 'tailOk']

/** Scripted public clients over one in-memory two-partition log. The main consumer is driven from every
 * send, the audit consumer replays the log, and `stale` makes the broker's group offsets lag.
 * @param {{partitions?:number,stale?:boolean,order?:string[]}} [options] cluster behaviour
 * @returns {{fixture:object,captured:object,commits:object,log:object[][]}} fixture plus observations
 */
const fakeCluster = ({ partitions = 2, stale = false, order = [] } = {}) => {
  const log = Array.from({ length: partitions }, () => [])
  const commits = {}
  const captured = { kafka: [], producer: [], consumer: [], run: [], fetchOffsets: [] }
  const cursor = Array(partitions).fill(0)
  let handlers = null
  let busy = false
  let clock = 0
  const kick = () => {
    if (busy || !handlers) return
    busy = true
    ;(async () => {
      let progressed = true
      while (progressed) {
        progressed = false
        for (let p = 0; p < partitions; p += 1) {
          const pending = log[p].slice(cursor[p])
          if (!pending.length) continue
          progressed = true
          const messages = pending.map((m, i) => ({
            key: Buffer.from(m.key),
            value: m.value,
            offset: String(cursor[p] + i),
          }))
          cursor[p] += pending.length
          if (handlers.eachBatch)
            await handlers.eachBatch({
              batch: { topic: 't', partition: p, messages },
              resolveOffset: () => {},
            })
          else
            for (const message of messages)
              await handlers.eachMessage({ topic: 't', partition: p, message })
        }
      }
      busy = false
    })()
  }
  const main = {
    connect: async () => {},
    subscribe: async () => {},
    stop: async () => {},
    disconnect: async () => {},
    commitOffsets: async (offsets) => {
      order.push('commit')
      for (const { partition, offset } of offsets) commits[partition] = offset
    },
    run: async (options) => {
      captured.run.push(options)
      handlers = options
    },
  }
  const audit = {
    connect: async () => {},
    subscribe: async () => {},
    stop: async () => {},
    disconnect: async () => {},
    run: async (options) => {
      setImmediate(async () => {
        for (let p = 0; p < partitions; p += 1)
          for (let i = 0; i < log[p].length; i += 1)
            await options.eachMessage({
              partition: p,
              message: {
                key: Buffer.from(log[p][i].key),
                value: log[p][i].value,
                offset: String(i),
              },
            })
      })
    },
  }
  const producer = {
    connect: async () => {},
    disconnect: async () => {},
    send: async ({ messages }) => {
      for (const m of messages) log[m.partition].push({ key: m.key, value: m.value })
      kick()
      return []
    },
  }
  const admin = {
    connect: async () => {},
    disconnect: async () => {},
    fetchTopicOffsets: async () =>
      log.map((entries, partition) => ({ partition, offset: String(entries.length) })),
    fetchOffsets: async (args) => {
      captured.fetchOffsets.push(args)
      const source = stale ? { 0: '1', 1: '1' } : commits
      return [
        {
          topic: 't',
          partitions: Object.entries(source).map(([partition, offset]) => ({
            partition: Number(partition),
            offset,
          })),
        },
      ]
    },
  }
  const fixture = {
    topic: 't',
    groupId: 'g',
    auditGroupId: 'a',
    clientId: 'c',
    supportsMetricsPush: true,
    now: () => {
      order.push('now')
      return (clock += 1)
    },
    wait: async () => {},
    provision: async (_admin, work) => work(),
    exports: async () => [],
    kafka: (options) => {
      captured.kafka.push(options)
      return {
        admin: () => admin,
        producer: (o) => {
          captured.producer.push(o)
          return producer
        },
        consumer: (o) => {
          captured.consumer.push(o)
          return o.groupId === 'a' ? audit : main
        },
      }
    },
  }
  return { fixture, captured, commits, log }
}

const deliveryConfig = (handler, extra = {}) => ({
  messages: 4,
  warmup: 2,
  size: 200,
  partitions: 2,
  handler,
  telemetry: 'off',
  interval: 1000,
  timeout: 5000,
  minimumGaps: 1,
  ...extra,
})

/** @param {object} cluster `fakeCluster` output @param {object} config delivery counts @param {object} [hooks] control
 * @returns {Promise<{result:object,evidence:EvidenceWriter}>} result after the awaited close
 */
const runDelivery = async (cluster, config, hooks = {}) => {
  const evidence = new EvidenceWriter(tmp(), { identity: IDENTITY })
  const workload = new DeliveryWorkload({
    root: ROOT,
    config,
    control: 0,
    evidence,
    fixture: cluster.fixture,
    hooks,
  })
  try {
    return { result: await workload.run(), evidence }
  } finally {
    await workload.close()
  }
}

/** @returns {{tables:object,meta:object,config:object}} four literal records over two partitions after two warmups */
const literalDelivery = () => {
  const tables = {
    send: new Table(SEND, 4),
    handler: new Table(HANDLER, 4),
    callbacks: new Table(CALLBACK, 2),
    durable: new Table(DURABLE, 4),
  }
  for (let i = 0; i < 4; i += 1) {
    const [partition, offset] = [i % 2, 1 + Math.floor(i / 2)]
    tables.send.add(i, 10 + i, 1000 + i)
    tables.handler.add(i, partition, offset, 1000 + i, 20 + i, Math.floor(i / 2), 200)
    tables.durable.add(i, partition, offset, 1000 + i, 1)
  }
  tables.callbacks.add(20, 2, 0)
  tables.callbacks.add(22, 2, 1)
  const meta = {
    baseOf: { 0: 0, 1: 0 },
    warmup: 2,
    partitions: 2,
    handler: 'eachBatch',
    ends: [
      { partition: 0, offset: '3' },
      { partition: 1, offset: '3' },
    ],
    elapsedNs: 1e6,
    expectedCommits: { p0: '3', p1: '3' },
    finalCommits: { p0: '3', p1: '3' },
  }
  return { tables, meta, config: deliveryConfig('eachBatch') }
}

describe('telemetry delivery workload', () => {
  test('message and batch handlers retain independent delivery and callback counts', async () => {
    for (const [handler, callbacks] of [
      ['eachBatch', 2],
      ['eachMessage', 4],
    ]) {
      const cluster = fakeCluster()
      const { result } = await runDelivery(cluster, deliveryConfig(handler))
      expect(result).toMatchObject({
        messages: 4,
        deliveredMessages: 4,
        callbacks,
        gapSamples: callbacks - 1,
        discardedWorkloadWarmups: 2,
      })
      expect(cluster.commits).toEqual({ 0: '3', 1: '3' })
      const expected = publicOptions({ handler, active: false, groupId: 'g', partitions: 2 })
      expect(cluster.captured.kafka).toEqual([
        { clientId: 'c-helper', enableMetricsPush: false },
        { clientId: 'c-measured', enableMetricsPush: false },
      ])
      expect(cluster.captured.producer[0]).toEqual(expected.producer)
      expect(cluster.captured.consumer[0]).toEqual(expected.consumer)
      expect(cluster.captured.consumer[0]).not.toHaveProperty('autoCommit')
      expect(Object.keys(cluster.captured.run[0]).sort()).toEqual(Object.keys(expected.run).sort())
      expect(cluster.captured.run[0]).toMatchObject({
        autoCommit: false,
        partitionsConsumedConcurrently: 2,
      })
      expect(typeof cluster.captured.run[0][handler]).toBe('function')
    }
  })

  test('handler control is inside the original callback before processing', async () => {
    for (const [handler, controls] of [
      ['eachBatch', 4],
      ['eachMessage', 6],
    ]) {
      const order = []
      const cluster = fakeCluster({ order })
      let failure = null
      const hooks = {
        control: async () => {
          order.push('control')
          if (failure) throw failure
          return 1000000
        },
      }
      await runDelivery(cluster, deliveryConfig(handler), hooks)
      const sequence = order.filter((event) => event === 'control' || event === 'commit')
      expect(sequence.filter((event) => event === 'control')).toHaveLength(controls)
      if (handler === 'eachBatch')
        sequence.forEach((event, i) => {
          if (event === 'control') expect(sequence[i + 1]).toBe('commit')
        })
      const sentinel = new Error('original rejection')
      failure = sentinel
      const callback = cluster.captured.run[0][handler]
      const input =
        handler === 'eachBatch'
          ? { batch: { partition: 0, messages: [] }, resolveOffset: () => {} }
          : {
              partition: 0,
              message: { key: Buffer.from('w-9'), value: Buffer.alloc(200), offset: '9' },
            }
      await expect(callback(input)).rejects.toBe(sentinel)
      expect(order.filter((event) => event === 'control')).toHaveLength(controls + 1)
    }
  })

  test('dropped duplicated and substituted records fail all outcome joins', () => {
    const judge = ({ tables, meta, config }) =>
      validateDelivery(deliveryResult(config, tables, meta), deliveryEvidence(config, tables, meta))
    expect(() => judge(literalDelivery())).not.toThrow()
    const wrongId = literalDelivery()
    wrongId.tables.handler.data[3 * 7 + 1] = 0
    expect(() => judge(wrongId)).toThrow()
    const duplicate = literalDelivery()
    duplicate.tables.durable.data.copyWithin(3 * 5, 2 * 5, 3 * 5)
    expect(() => judge(duplicate)).toThrow()
    const missing = literalDelivery()
    missing.tables.handler.length = 3
    expect(() => judge(missing)).toThrow()
    const damaged = literalDelivery()
    damaged.tables.durable.data[1 * 5 + 4] = 0
    expect(() => judge(damaged)).toThrow()
    const substituted = literalDelivery()
    substituted.tables.durable.data[1 * 5 + 3] = 7
    expect(() => judge(substituted)).toThrow()
  })

  test('raw broker commits cannot be replaced by local resolved offsets', async () => {
    const stale = fakeCluster({ stale: true })
    await expect(runDelivery(stale, deliveryConfig('eachBatch'))).rejects.toThrow(
      assert.AssertionError
    )
    expect(stale.captured.fetchOffsets[0]).toMatchObject({ groupId: 'g', resolveOffsets: false })
    expect(stale.commits).toEqual({ 0: '3', 1: '3' })
    const current = fakeCluster()
    const { result } = await runDelivery(current, deliveryConfig('eachBatch'))
    expect(result.deliveredMessages).toBe(4)
    expect(current.captured.fetchOffsets[0].resolveOffsets).toBe(false)
  })

  test('actual gaps exclude warmup and cannot be inferred from the corpus', async () => {
    await expect(
      runDelivery(fakeCluster(), deliveryConfig('eachBatch', { minimumGaps: undefined }))
    ).rejects.toThrow(assert.AssertionError)
    const { result, evidence } = await runDelivery(fakeCluster(), deliveryConfig('eachBatch'))
    expect(result.gapSamples).toBe(1)
    expect(result.callbacks).toBe(2)
    const literal = literalDelivery()
    expect(() =>
      validateDelivery(
        { ...deliveryResult(literal.config, literal.tables, literal.meta), messages: 1000000 },
        deliveryEvidence(literal.config, literal.tables, literal.meta)
      )
    ).toThrow()
    const config = deliveryConfig('eachBatch')
    const decl = declaration({ mode: 'delivery', config })
    const env = envelopeOf(result, evidence, decl)
    expect(() => verifyWorker(env, decl, evidence.directory)).not.toThrow()
    expect(() => verifyWorker({ ...env, gapSamples: 4 }, decl, evidence.directory)).toThrow()
  })

  test('telemetry frame evidence remains bounded and joins actual successful pushes', () => {
    const int16 = (v) => Buffer.from([(v >> 8) & 255, v & 255])
    const int32 = (v) => {
      const b = Buffer.alloc(4)
      b.writeInt32BE(v)
      return b
    }
    const frame = (...parts) => {
      const body = Buffer.concat(parts)
      return Buffer.concat([int32(body.length), body])
    }
    const raw = Buffer.from('0a14121212100a01612a0b0a09310100000000000000', 'hex')
    const uuid = (n) => Buffer.alloc(16, n)
    const header = (apiKey, correlation) =>
      Buffer.concat([
        int16(apiKey),
        int16(0),
        int32(correlation),
        int16(3),
        Buffer.from('abc'),
        Buffer.from([0]),
      ])
    const send = (tap, buffer) => {
      for (let i = 0; i < buffer.length; i += 5) tap.outbound(buffer.subarray(i, i + 5))
    }
    const subscribe = (tap, correlation, id, error = 0) => {
      send(tap, frame(header(71, correlation), uuid(id), Buffer.from([0])))
      return frame(
        int32(correlation),
        Buffer.from([0]),
        int32(0),
        int16(error),
        uuid(id),
        int32(7),
        Buffer.from([2, 1]),
        int32(1000),
        int32(1048576),
        Buffer.from([1, 1, 0])
      )
    }
    const push = (tap, correlation, id, error = 0) => {
      send(
        tap,
        frame(
          header(72, correlation),
          uuid(id),
          int32(7),
          Buffer.from([0, 0]),
          Buffer.concat([Buffer.from([raw.length + 1]), raw, Buffer.from([0])])
        )
      )
      return frame(int32(correlation), Buffer.from([0]), int32(0), int16(error), Buffer.from([0]))
    }
    const feed = (tap, buffer) => {
      for (let i = 0; i < buffer.length; i += 3) tap.inbound(buffer.subarray(i, i + 3))
    }
    const dir = tmp()
    const evidence = new EvidenceWriter(dir, { identity: IDENTITY })
    const probe = new TelemetryPushProbe({ evidence, now: () => 1000 })
    const tap = probe.open('1')
    for (const id of [1, 2]) {
      feed(tap, subscribe(tap, id * 10, id))
      feed(tap, push(tap, id * 10 + 1, id))
    }
    tap.outbound(frame(int16(0), int16(0), int32(5), int16(0), Buffer.alloc(10000)))
    feed(tap, frame(int32(5), Buffer.alloc(9000)))
    expect(tap.pending).toBe(0)
    const records = probe.finish()
    expect(records.map(({ kind, uuid: id }) => `${kind}:${id.slice(0, 2)}`)).toEqual([
      'subscription:01',
      'push:01',
      'subscription:02',
      'push:02',
    ])
    expect(records[0]).toMatchObject({ errorCode: 0, subscriptionId: 7, pushIntervalMs: 1000 })
    expect(records[1]).toMatchObject({ errorCode: 0, metricsBytes: raw.length, subscriptionId: 7 })
    const chunks = readChunks(dir, evidence.artifacts(), 'delivery-telemetry')
    expect(chunks.map((chunk) => chunk.toString().split('\n').filter(Boolean).length)).toEqual([4])
    const exports = records
      .filter(({ kind }) => kind === 'push')
      .map(({ uuid: id }, i) => ({
        clientInstanceId: id,
        pushIntervalMs: 1000,
        node: '1',
        seq: i + 1,
        dataBytes: raw.length,
        dataSha256: hashBytes(raw),
        terminating: false,
        parse: 'ok',
        v: 1,
      }))
    const window = { startMs: 999, endMs: 1001 }
    for (const change of [
      (r) => ({ ...r, rawSha256: '0'.repeat(64) }),
      (r) => ({ ...r, terminating: true }),
      (r) => ({ ...r, observedMs: 998 }),
      (r) => ({ ...r, errorCode: 1 }),
      (r) => ({ ...r, node: '2' }),
      (r) => ({ ...r, decoded: { points: [] } }),
    ]) {
      const changed = records.map((r) => (r.kind === 'push' ? change(r) : r))
      expect(() =>
        telemetryEvidence(changed, { active: true, intervalMs: 1000, exports, window })
      ).toThrow()
    }
    const warmSuccess = records
    const laterWindow = { startMs: 1001, endMs: 1003 }
    const failedWindow = records
      .filter((r) => r.kind === 'push')
      .map((r) => ({ ...r, observedMs: 1002, errorCode: 1 }))
    expect(() =>
      telemetryEvidence([...warmSuccess, ...failedWindow], {
        active: true,
        intervalMs: 1000,
        exports,
        window: laterWindow,
      })
    ).toThrow()
    const finalOnly = records.map((record) => {
      if (record.kind !== 'push') return record
      const body = Buffer.from(record.bodyHex, 'hex')
      body[20] = 1
      return {
        ...record,
        terminating: true,
        bodyHex: body.toString('hex'),
        bodySha256: hashBytes(body),
      }
    })
    expect(() =>
      telemetryEvidence(finalOnly, {
        active: true,
        intervalMs: 1000,
        exports: exports.map((e) => ({ ...e, terminating: true })),
        window,
      })
    ).toThrow()
    for (const field of ['node', 'dataSha256']) {
      const changed = exports.map((e) => ({
        ...e,
        [field]: field === 'node' ? '2' : '0'.repeat(64),
      }))
      expect(() =>
        telemetryEvidence(records, { active: true, intervalMs: 1000, exports: changed, window })
      ).toThrow()
    }
    expect(telemetryEvidence(records, { active: true, intervalMs: 1000, exports, window })).toEqual(
      {
        clients: 2,
        pushes: 2,
      }
    )
    expect(() =>
      telemetryEvidence([], { active: true, intervalMs: 1000, exports, window })
    ).toThrow()
    expect(() =>
      telemetryEvidence(records, { active: true, intervalMs: 1000, exports: [], window })
    ).toThrow()
    const failed = records.map((r) => (r.kind === 'push' ? { ...r, errorCode: 1 } : r))
    expect(() =>
      telemetryEvidence(failed, { active: true, intervalMs: 1000, exports, window })
    ).toThrow()
    expect(telemetryEvidence([], { active: false, intervalMs: 1000, exports: [] })).toEqual({
      clients: 0,
      pushes: 0,
    })
    expect(() => telemetryEvidence(records, { active: false, intervalMs: 1000, exports })).toThrow()
    const bounded = new TelemetryPushProbe({
      evidence: new EvidenceWriter(tmp()),
      maxRecords: 8,
      now: () => 1000,
    })
    const boundedTap = bounded.open('1')
    for (let i = 0; i < 20; i += 1) feed(boundedTap, subscribe(boundedTap, i, 1))
    expect(bounded.records.length).toBeLessThanOrEqual(8)
    bounded.finish()
    const overflow = new TelemetryPushProbe({ evidence: new EvidenceWriter(tmp()), maxPending: 1 })
    const other = overflow.open()
    other.outbound(frame(header(71, 1), uuid(1), Buffer.from([0])))
    other.outbound(frame(header(71, 2), uuid(1), Buffer.from([0])))
    expect(() => overflow.finish()).toThrow(assert.AssertionError)
    const crowded = new TelemetryPushProbe({ evidence: new EvidenceWriter(tmp()), maxSockets: 1 })
    crowded.open()
    crowded.open()
    expect(() => crowded.finish()).toThrow(assert.AssertionError)
    const writer = new EvidenceWriter(tmp(), { maxBytes: 10 })
    expect(() => writer.write('a', Buffer.alloc(11))).toThrow()
    expect(() =>
      new EvidenceWriter(tmp(), { maxArtifacts: 1 }).chunk('x', Buffer.alloc(2 * MIB + 1))
    ).toThrow()
    const expected = publicOptions({
      handler: 'eachMessage',
      active: true,
      groupId: 'g',
      partitions: 6,
    })
    const captured = {
      ...expected,
      run: { ...expected.run, eachMessage: () => {} },
    }
    expect(() => assertPublicOptions(captured, expected)).not.toThrow()
    expect(() =>
      assertPublicOptions(
        { ...captured, producer: { ...expected.producer, telemetry: true } },
        expected
      )
    ).toThrow()
    expect(() =>
      assertPublicOptions(
        { ...captured, consumer: { ...expected.consumer, autoCommit: false } },
        expected
      )
    ).toThrow()
    expect(() =>
      assertPublicOptions({ ...captured, kafka: { enableMetricsPush: false } }, expected)
    ).toThrow()
    const [a, b] = [makeValue(5, 1.5), makeValue(5, 2.5)]
    expect(a).toHaveLength(200)
    expect(valueHash(a)).not.toBe(valueHash(b))
  })
})

const hashBytes = (bytes) => require('node:crypto').createHash('sha256').update(bytes).digest('hex')
const asset = (file) => ({ path: file, sha256: hashBytes(fs.readFileSync(file)) })
const FROZEN = { path: path.join(ROOT, 'scripts/benchmarks/telemetry.js') }

/** @param {string} label source identity @returns {object} actual frozen source and receipt files */
const frozenSource = (label) => {
  const directory = tmp()
  const root = path.join(directory, 'source')
  fs.mkdirSync(root)
  fs.writeFileSync(path.join(root, 'index.js'), label)
  const consume = 'scripts/benchmarks/consumeThroughput.js'
  fs.mkdirSync(path.join(root, 'scripts/benchmarks'), { recursive: true })
  fs.copyFileSync(path.join(ROOT, consume), path.join(root, consume))
  const files = [
    { path: 'index.js', sha256: hashBytes(label) },
    { path: consume, sha256: asset(path.join(root, consume)).sha256 },
  ]
  const tree = digest(files)
  const tarball = path.join(directory, 'source.tgz')
  fs.writeFileSync(tarball, label)
  const source = path.join(directory, 'source.json')
  fs.writeFileSync(source, JSON.stringify({ tree, files, tarballSha256: asset(tarball).sha256 }))
  return { root, tree, source: asset(source), tarball: asset(tarball) }
}

/** @param {object} job declared job @param {string} label arm label @returns {string[]} valid worker argv */
const argvFor = (job, label) => {
  const control = String(job.controls.includes(label) ? 1 : 0)
  const base = ['--mode', job.mode, '--control', control]
  if (job.mode === 'payload')
    return [
      ...base,
      '--points',
      '257',
      '--temporality',
      'delta',
      '--codec',
      'gzip',
      '--attributes',
      'poor',
    ]
  if (job.mode === 'delivery')
    return [...base, '--handler', 'eachMessage', '--telemetry', job.telemetry[label]]
  return [...base, '--telemetry', job.telemetry[label]]
}

/** @param {string} jobName declared job @param {object} [extra] manifest overrides @returns {object} valid manifest */
const makeManifest = (jobName, extra = {}) => {
  const job = JOBS[jobName]
  const candidate = frozenSource('candidate')
  const previous = frozenSource('previous')
  const proof = path.join(tmp(), 'accepted.json')
  fs.writeFileSync(
    proof,
    JSON.stringify({
      milestone: '9E',
      status: 'accepted',
      tree: previous.tree,
      sourceSha256: previous.source.sha256,
      tarballSha256: previous.tarball.sha256,
    })
  )
  return {
    schemaVersion: 1,
    job: jobName,
    runId: 'sess',
    directory: '/unused',
    driver: asset(FROZEN.path),
    session: asset(path.join(ROOT, 'scripts/benchmarks/session.js')),
    consume: asset(path.join(ROOT, 'scripts/benchmarks/consumeThroughput.js')),
    node: {
      binary: process.execPath,
      sha256: asset(process.execPath).sha256,
      version: process.version,
      platform: process.platform,
      arch: process.arch,
    },
    codecs: { none: true, gzip: true, zstd: false },
    acceptedBaseline: asset(proof),
    arms: job.roles.map((label) => {
      const argv = argvFor(job, label)
      if (extra.optionalCodec === 'zstd' && job.mode === 'payload')
        argv[argv.indexOf('--codec') + 1] = 'zstd'
      return {
        label,
        ...(label === 'previous' ? previous : candidate),
        argv,
        configSha256: digest({ mode: job.mode, argv }),
      }
    }),
    reference: job.reference,
    runs: job.runs,
    budgetSeconds: minimumBudget(jobName),
    ...extra,
  }
}

const METRICS = {
  requests: (slow) => ({
    cpuNsPerPair: slow ? 200 : 100,
    elapsedNsPerPair: 50,
    heapExternalDeltaBytes: 0,
    completedPairs: 1000000,
  }),
  payload: (slow) => ({
    sliceP99Ms: slow ? 120 : 1,
    cpuP99Ms: 1,
    wallP99Ms: slow ? 130 : 1,
    fittingPasses: 3,
    heapExternalDeltaBytes: 100,
    rssDeltaBytes: 100,
    rawBytes: 10,
    transmittedBytes: 10,
  }),
  delivery: (slow) => ({
    messagesPerSecond: slow ? 500 : 1000,
    p99Ms: slow ? 30 : 10,
    deliveryGapP99Ms: 5,
    gapSamples: 5,
    elapsedNs: 1e9,
  }),
}

/** @param {object[]} calls ledger of invocations @param {object} [overrides] status/stdout/error replacements
 * @returns {Function} synchronous fake spawn: valid final JSON line, progress before it, no real process
 */
const fakeSpawn =
  (calls, overrides = {}) =>
  (command, args, options) => {
    calls.push({ command, args, options })
    if (overrides.spawn) return overrides.spawn({ command, args, options })
    const mode = args[args.indexOf('--mode') + 1]
    const slow = args[args.indexOf('--control') + 1] === '1'
    const body = JSON.stringify({ mode, ...METRICS[mode](slow) })
    return { status: 0, signal: null, stdout: `progress\n${body}\n`, stderr: '', ...overrides }
  }

const stubVerify = () => ({ signedHeapExternalBytes: 100 })

/** @param {string} jobName declared job @param {object} [extra] manifest overrides @param {object} [spawnOverrides]
 * @returns {object} manifest, directory, fake-process ledger and a session wired to them
 */
const sessionFixture = (jobName, extra = {}, spawnOverrides = {}) => {
  const calls = []
  const manifest = makeManifest(jobName, extra)
  const directory = path.join(tmp(), 'run')
  const io = { spawnSync: fakeSpawn(calls, spawnOverrides), verifyWorker: stubVerify }
  return {
    calls,
    manifest,
    directory,
    io,
    session: new TelemetrySession({ manifest, directory, io }),
  }
}

/** @param {string} jobName declared job @param {Function} resultOf row to numeric projection
 * @returns {{declared:object,rows:object[]}} a complete synthetic session */
const syntheticRows = (jobName, resultOf, derivedOf = () => ({ signedHeapExternalBytes: 100 })) => {
  const job = JOBS[jobName]
  const declared = schedule(job.roles, job.reference, job.runs)
  return {
    declared,
    rows: declared.rows.map((row) => ({ ...row, result: resultOf(row), derived: derivedOf(row) })),
  }
}

describe('telemetry benchmark session', () => {
  test('worker flags reject unknown repeated missing and cross mode inputs before side effects', async () => {
    const ok = ['--mode', 'requests', '--control', '0', '--telemetry', 'active']
    expect(parseWorkerArgs(ok)).toMatchObject({ mode: 'requests', control: 0 })
    expect(parseWorkerArgs([...ok.slice(0, 3), '1', ...ok.slice(4)])).toMatchObject({ control: 1 })
    expect(parseWorkerArgs(['--mode', 'requests', '--telemetry', 'off']).control).toBe(0)
    const payload = [
      '--mode',
      'payload',
      '--points',
      '257',
      '--temporality',
      'delta',
      '--codec',
      'zstd',
      '--attributes',
      'poor',
    ]
    expect(parseWorkerArgs(payload).config).toMatchObject({
      points: 257,
      snapshots: 100,
      warmup: 20,
    })
    const delivery = ['--mode', 'delivery', '--handler', 'eachBatch', '--telemetry', 'active']
    expect(parseWorkerArgs(delivery).config).toMatchObject({
      messages: 1000000,
      partitions: 6,
      timeout: 540000,
    })
    const bad = [
      ['--mode', 'requests', '--control', '--telemetry', 'off'],
      ['--mode', 'requests', '--control', '2', '--telemetry', 'off'],
      ['--mode', 'requests', '--telemetry', 'off', '--runs', '7'],
      ['--mode', 'requests', '--telemetry', 'off', '--telemetry', 'off'],
      ['--mode', 'requests', '--telemetry'],
      ['--mode', 'requests', '--telemetry', 'off', '--points', '1'],
      ['--mode', 'requests', '--telemetry', 'off', '--iterations', '1.5'],
      ['--mode', 'requests', '--telemetry', 'off', '--iterations', '5'],
      ['--mode', 'requests', '--control', '1', '--telemetry', 'off'],
      ['--mode', 'requests'],
      [
        '--mode',
        'payload',
        '--points',
        '258',
        '--temporality',
        'delta',
        '--codec',
        'none',
        '--attributes',
        'poor',
      ],
      ['--mode', 'delivery', '--handler', 'both', '--telemetry', 'off'],
      ['--mode', 'other'],
      ['--telemetry', 'off'],
    ]
    for (const argv of bad) expect(() => parseWorkerArgs(argv)).toThrow(UsageError)
    const spawn = jest.fn()
    const load = jest.fn()
    const spies = ['mkdirSync', 'writeFileSync', 'mkdtempSync'].map((name) => jest.spyOn(fs, name))
    try {
      for (const argv of bad) {
        const error = jest.fn()
        expect(await main(argv, { log: jest.fn(), error, spawnSync: spawn, load })).toBe(2)
        expect(error).toHaveBeenCalled()
      }
      expect(await main(['--session', 'relative.json'], { log: jest.fn(), error: jest.fn() })).toBe(
        2
      )
      expect(
        await main(['--session', '/a.json', '--mode', 'requests'], {
          log: jest.fn(),
          error: jest.fn(),
        })
      ).toBe(2)
      expect(spawn).not.toHaveBeenCalled()
      expect(load).not.toHaveBeenCalled()
      for (const spy of spies) expect(spy).not.toHaveBeenCalled()
    } finally {
      for (const spy of spies) spy.mockRestore()
    }
  })

  test('supervisor invokes unchanged scheduler and separate identical reference children', () => {
    for (const [jobName, children] of [
      ['requests-control', 30],
      ['requests-normal', 55],
      ['delivery-normal', 36],
    ]) {
      const { calls, session, directory } = sessionFixture(jobName)
      const verdict = session.run()
      expect(calls).toHaveLength(children)
      const raw = fs
        .readFileSync(path.join(directory, 'raw.jsonl'), 'utf8')
        .split('\n')
        .filter(Boolean)
        .map((line) => JSON.parse(line))
      expect(raw).toHaveLength(children)
      expect(raw.filter(({ warmup }) => warmup)).toHaveLength(JOBS[jobName].roles.length + 1)
      expect(new Set(raw.map(({ runId }) => runId)).size).toBe(children)
      const reference = makeManifest(jobName).arms.find(
        ({ label }) => label === JOBS[jobName].reference
      )
      const alike = makeManifest(jobName).arms.filter(
        ({ argv }) => argv.join(' ') === reference.argv.join(' ')
      ).length
      const same = calls.filter(({ args }) => args.slice(6).join(' ') === reference.argv.join(' '))
      expect(same).toHaveLength((alike + 1) * (JOBS[jobName].runs + 1))
      for (const { command, args, options } of calls) {
        expect(command).toBe('timeout')
        expect(args.slice(0, 6)).toEqual([
          '-k',
          '5s',
          '600s',
          process.execPath,
          '--expose-gc',
          FROZEN.path,
        ])
        expect(options.timeout).toBe(610000)
        expect(
          Object.keys(options.env)
            .filter((name) => name.startsWith('TELEMETRY_BENCH_'))
            .sort()
        ).toEqual([
          'TELEMETRY_BENCH_MANIFEST',
          'TELEMETRY_BENCH_OUTPUT',
          'TELEMETRY_BENCH_ROOT',
          'TELEMETRY_BENCH_RUN',
        ])
        expect(options.cwd).toBe(options.env.TELEMETRY_BENCH_ROOT)
      }
      if (jobName === 'requests-normal')
        expect(verdict).toMatchObject({ exit: 0, disposition: 'pass' })
    }
  })

  test('complete removed rounds cannot be re summarized as passing evidence', () => {
    const { session, directory } = sessionFixture('requests-control')
    session.run()
    expect(session.verify()).toMatchObject({ exit: 1, disposition: 'gate-failed' })
    const file = path.join(directory, 'raw.jsonl')
    const original = fs.readFileSync(file, 'utf8')
    const lines = original.split('\n').filter(Boolean)
    const rewrite = (rows) =>
      fs.writeFileSync(file, `${rows.map((r) => JSON.stringify(r)).join('\n')}\n`)
    const rows = lines.map((line) => JSON.parse(line))
    rewrite(rows.filter(({ round }) => round !== 4))
    expect(() => session.verify()).toThrow()
    rewrite(rows.map((row, i) => (i === 5 ? { ...row, position: (row.position + 1) % 3 } : row)))
    expect(() => session.verify()).toThrow()
    rewrite(rows.map((row, i) => (i === 3 ? { ...row, runId: rows[4].runId } : row)))
    expect(() => session.verify()).toThrow()
    fs.writeFileSync(file, original)
    expect(session.verify().exit).toBe(1)
  })

  test('worker status and immutable artifact bytes precede the numeric projection', async () => {
    const pristine = await realRequestWorker()
    expect(() => verifyWorker(pristine.envelope, pristine.decl, pristine.dir)).not.toThrow()
    const mutate = async (change, adjust = (e) => e) => {
      const worker = await realRequestWorker()
      change(worker)
      expect(() => verifyWorker(adjust(worker.envelope), worker.decl, worker.dir)).toThrow()
    }
    const file = (worker, name) => path.join(worker.dir, name)
    await mutate((w) => {
      const bytes = fs.readFileSync(file(w, 'requests-write.bin'))
      bytes[0] ^= 1
      fs.writeFileSync(file(w, 'requests-write.bin'), bytes)
    })
    await mutate((w) => fs.truncateSync(file(w, 'requests-response.bin'), 8))
    await mutate((w) => {
      fs.rmSync(file(w, 'requests-observe.bin'))
      fs.symlinkSync(file(w, 'requests-write.bin'), file(w, 'requests-observe.bin'))
    })
    await mutate(
      () => {},
      (e) => ({
        ...e,
        artifacts: [{ ...e.artifacts[0], name: '../escape' }, ...e.artifacts.slice(1)],
      })
    )
    await mutate((w) => fs.writeFileSync(file(w, 'extra.bin'), 'x'))
    await mutate((w) => fs.rmSync(file(w, 'request-meta.json')))
    await mutate(
      () => {},
      (e) => ({ ...e, completedPairs: e.completedPairs + 1 })
    )
    const bad = [
      { status: 1, stderr: 'boom' },
      { status: null, signal: 'SIGKILL' },
      { status: 0, stdout: '' },
      { status: 0, stdout: 'not json\n' },
      { status: 0, error: new Error('ETIMEDOUT') },
    ]
    for (const overrides of bad) {
      const { session } = sessionFixture('requests-control', {}, overrides)
      expect(() => session.run()).toThrow()
    }
    const dir = tmp()
    const evidence = new EvidenceWriter(dir)
    evidence.write('a.bin', 'abc')
    expect(() => verifyDirectory(dir, [{ name: 'a.bin' }])).not.toThrow()
    expect(() => verifyDirectory(dir, [])).toThrow()
    expect(numericProjection({ mode: 'requests', ...METRICS.requests(false) })).toEqual(
      METRICS.requests(false)
    )
    expect(() => numericProjection({ mode: 'requests', cpuNsPerPair: NaN })).toThrow()
  })

  test('void control regression and unavailable remain different outcomes', async () => {
    const control = (busy, aa, normal = 100) =>
      syntheticRows('requests-control', ({ label }) => ({
        cpuNsPerPair: label === 'busy' ? busy : label === 'normal~AA' ? aa : normal,
      }))
    const judge = ({ declared, rows }) => evaluate('requests-control', declared, rows)
    expect(judge(control(120, 103))).toMatchObject({
      exit: 1,
      disposition: 'gate-failed',
      failures: [{ arm: 'busy', fields: ['cpuNsPerPair'], expected: true }],
    })
    expect(judge(control(120, 103.1))).toMatchObject({ exit: 3, disposition: 'void' })
    expect(() => judge(control(100, 100))).toThrow(/ineffective/)
    expect(() => judge(control(120, 100, 0))).toThrow()
    const delivery = (aaP99) =>
      syntheticRows('delivery-normal', ({ label }) => ({
        messagesPerSecond: 1000,
        p99Ms: label === 'previous~AA' ? aaP99 : 100,
        deliveryGapP99Ms: 50,
      }))
    expect(evaluate('delivery-normal', ...Object.values(delivery(105)))).toMatchObject({ exit: 0 })
    expect(evaluate('delivery-normal', ...Object.values(delivery(105.1)))).toMatchObject({
      exit: 3,
    })
    const requests = (increment, derived) =>
      syntheticRows(
        'requests-normal',
        () => ({ cpuNsPerPair: 100 }),
        (row) =>
          derived?.(row) ?? {
            signedHeapExternalBytes: row.label === 'active' ? 1000 + increment : 1000,
          }
      )
    const normal = (set) => evaluate('requests-normal', set.declared, set.rows)
    expect(normal(requests(8388608))).toMatchObject({ exit: 0, disposition: 'pass' })
    const over = normal(requests(8388609))
    expect(over.exit).toBe(1)
    expect(over.failures).toHaveLength(10)
    expect(
      over.failures.every(({ fields }) => fields[0] === 'incrementalRequestHeapExternalBytes')
    ).toBe(true)
    expect(() => normal(requests(0, () => ({})))).toThrow()
    expect(() => normal(requests(0, () => ({ signedHeapExternalBytes: NaN })))).toThrow()
    const full = requests(0)
    const without = full.rows.filter((row) => !(row.label === 'active' && row.round === 3))
    expect(() => memoryPairs(without, full.declared.n)).toThrow()
    expect(memoryPairs(full.rows, full.declared.n)).toHaveLength(10)
    const worker = await realRequestWorker()
    const memoryFile = path.join(worker.dir, 'request-memory.json')
    const forged = JSON.parse(fs.readFileSync(memoryFile, 'utf8'))
    forged.signedDeltaBytes += 1
    const bytes = Buffer.from(JSON.stringify(forged))
    fs.writeFileSync(memoryFile, bytes)
    const entry = worker.envelope.artifacts.find(({ name }) => name === 'request-memory.json')
    entry.bytes = bytes.length
    entry.sha256 = require('node:crypto').createHash('sha256').update(bytes).digest('hex')
    expect(() => verifyWorker(worker.envelope, worker.decl, worker.dir)).toThrow(
      /signedDeltaBytes|deepEqual|equal/
    )
    await expect(
      new RequestWorkload({
        root: ROOT,
        config: { iterations: 2, warmup: 0, nodes: 1, telemetry: 'off' },
        control: 0,
        evidence: new EvidenceWriter(tmp(), { identity: IDENTITY }),
        hooks: { memoryProbe: new MemoryProbe({ memory: memoryHooks().memory, gc: null }) },
      }).run()
    ).rejects.toThrow(assert.AssertionError)
    const unavailable = sessionFixture(
      'payload',
      { optionalCodec: 'zstd' },
      { status: 4, stdout: '' }
    )
    expect(() => unavailable.session.run()).toThrow(CodecUnavailableError)
    const manifestFile = path.join(tmp(), 'manifest.json')
    fs.writeFileSync(
      manifestFile,
      JSON.stringify(
        makeManifest('payload', { optionalCodec: 'zstd', directory: path.join(tmp(), 'out') })
      )
    )
    const io = {
      log: jest.fn(),
      error: jest.fn(),
      spawnSync: fakeSpawn([], { status: 4, stdout: '' }),
      verifyWorker: stubVerify,
    }
    expect(await main(['--session', manifestFile], io)).toBe(4)
    const slow = sessionFixture('payload')
    expect(slow.session.run()).toMatchObject({ exit: 1, disposition: 'gate-failed' })
    const broken = sessionFixture('payload')
    const failing = new TelemetrySession({
      manifest: broken.manifest,
      directory: path.join(tmp(), 'run'),
      io: {
        spawnSync: fakeSpawn([]),
        verifyWorker: () => {
          throw new Error('identity mismatch')
        },
      },
    })
    expect(() => failing.run()).toThrow('identity mismatch')
  })

  test('process timeout and cleanup failure remain retained failures', async () => {
    const worker = await realRequestWorker()
    expect(() =>
      verifyWorker(
        { ...worker.envelope, clean: { ...worker.envelope.clean, sockets: 1 } },
        worker.decl,
        worker.dir
      )
    ).toThrow()
    const { clean, ...receiptless } = worker.envelope
    expect(clean).toBeDefined()
    expect(() => verifyWorker(receiptless, worker.decl, worker.dir)).toThrow()
    for (const overrides of [
      { status: null, signal: null },
      { error: Object.assign(new Error('spawnSync ETIMEDOUT'), { code: 'ETIMEDOUT' }) },
      { stdout: `${JSON.stringify({ mode: 'requests' })}\nlate progress\n` },
    ]) {
      const { session } = sessionFixture('requests-control', {}, overrides)
      expect(() => session.run()).toThrow()
    }
    const calls = []
    const manifest = makeManifest('requests-control')
    const directory = path.join(tmp(), 'run')
    const journal = path.join(directory, 'journal.jsonl')
    const io = {
      verifyWorker: stubVerify,
      spawnSync: (...args) => {
        calls.push(args)
        fs.rmSync(journal)
        fs.mkdirSync(journal)
        return { status: 1, signal: null, stdout: '', stderr: 'primary failure' }
      },
    }
    let thrown
    try {
      new TelemetrySession({ manifest, directory, io }).run()
    } catch (error) {
      thrown = error
    }
    expect(thrown).toBeInstanceOf(AggregateError)
    expect(thrown.errors).toHaveLength(2)
    expect(thrown.errors[0].message).toContain('primary failure')
    expect(calls).toHaveLength(1)
  })

  test('session budget includes every child verification grace and cleanup', () => {
    expect(childCount('requests-control')).toBe(30)
    expect(childCount('payload')).toBe(30)
    expect(childCount('requests-normal')).toBe(55)
    expect(childCount('delivery-normal')).toBe(36)
    expect(minimumBudget('requests-control')).toBe(18600)
    expect(minimumBudget('requests-normal')).toBe(33850)
    expect(minimumBudget('delivery-normal')).toBe(22260)
    expect(() =>
      validateManifest(makeManifest('requests-control', { budgetSeconds: 600 }))
    ).toThrow()
    expect(() =>
      validateManifest(makeManifest('requests-normal', { budgetSeconds: 33849 }))
    ).toThrow()
    expect(() => validateManifest(makeManifest('requests-normal'))).not.toThrow()
    expect(() => validateManifest(makeManifest('delivery-normal'))).not.toThrow()
  })

  test('saved manifest refuses an arm change or favorable observation subset', () => {
    const { session, directory, manifest, io } = sessionFixture('requests-control')
    session.run()
    expect(session.verify().exit).toBe(1)
    const variants = {
      runs: { runs: 7 },
      labels: { arms: [...manifest.arms].reverse() },
      reference: { reference: 'busy' },
    }
    for (const change of Object.values(variants))
      expect(
        () => new TelemetrySession({ manifest: { ...manifest, ...change }, directory, io })
      ).toThrow()
    const control = manifest.arms.map((arm) => {
      const argv = arm.argv.map((v, i, all) => (all[i - 1] === '--control' ? '0' : v))
      return { ...arm, argv, configSha256: digest({ mode: 'requests', argv }) }
    })
    expect(
      () => new TelemetrySession({ manifest: { ...manifest, arms: control }, directory, io })
    ).toThrow()
    const stale = manifest.arms.map((arm) => ({ ...arm, configSha256: 'f'.repeat(64) }))
    expect(
      () => new TelemetrySession({ manifest: { ...manifest, arms: stale }, directory, io })
    ).toThrow()
    const changed = [
      { driver: { ...FROZEN, sha256: 'd'.repeat(64) } },
      { arms: manifest.arms.map((arm) => ({ ...arm, tree: `${arm.tree}-other` })) },
      { arms: manifest.arms.map((arm) => ({ ...arm, root: `${arm.root}2` })) },
    ]
    for (const change of changed) {
      expect(() =>
        new TelemetrySession({ manifest: { ...manifest, ...change }, directory, io }).verify()
      ).toThrow()
    }
    const file = path.join(directory, 'raw.jsonl')
    const rows = fs
      .readFileSync(file, 'utf8')
      .split('\n')
      .filter(Boolean)
      .map((line) => JSON.parse(line))
    const reused = rows.map((row) =>
      row.label === 'normal~AA'
        ? { ...row, runId: rows.find((r) => r.label === 'normal').runId }
        : row
    )
    fs.writeFileSync(file, `${reused.map((r) => JSON.stringify(r)).join('\n')}\n`)
    expect(() => session.verify()).toThrow()
    fs.writeFileSync(file, `${rows.map((r) => JSON.stringify(r)).join('\n')}\n`)
    expect(session.verify().exit).toBe(1)
  })
})

describe('telemetry review regressions', () => {
  test('in-window Push capture forwards unchanged bytes before any independent decoding or hashing', () => {
    const i16 = (n) => {
      const b = Buffer.alloc(2)
      b.writeInt16BE(n)
      return b
    }
    const i32 = (n) => {
      const b = Buffer.alloc(4)
      b.writeInt32BE(n)
      return b
    }
    const frame = (...parts) => {
      const body = Buffer.concat(parts)
      return Buffer.concat([i32(body.length), body])
    }
    const header = (api, correlation) =>
      Buffer.concat([i16(api), i16(0), i32(correlation), i16(0), Buffer.from([0])])
    const uuid = Buffer.alloc(16, 1)
    const id = '01010101-0101-0101-0101-010101010101'
    const raw = Buffer.from('0a14121212100a01612a0b0a09310100000000000000', 'hex')
    const gzip = zlib.gzipSync(raw)
    const push = frame(
      header(72, 2),
      uuid,
      i32(7),
      Buffer.from([0, 1, gzip.length + 1]),
      gzip,
      Buffer.from([0])
    )
    const response = frame(i32(2), Buffer.from([0]), i32(0), i16(0), Buffer.from([0]))
    const exports = [
      {
        v: 1,
        node: '1',
        seq: 1,
        clientInstanceId: id,
        pushIntervalMs: 1000,
        dataBytes: raw.length,
        dataSha256: hashBytes(raw),
        terminating: false,
        parse: 'ok',
      },
    ]
    const evidence = new EvidenceWriter(tmp())
    const probe = new TelemetryPushProbe({ evidence, now: () => 1000 })
    const order = []
    const forwarded = {}
    const socket = new (require('node:events').EventEmitter)()
    const write = jest.fn(() => forwarded)
    socket.write = write
    socket.on('data', () => order.push('data'))
    const tapped = probe.wrap(
      () => socket,
      () => '1'
    )({})
    tapped.write(frame(header(71, 1), uuid, Buffer.from([0])))
    tapped.emit(
      'data',
      frame(
        i32(1),
        Buffer.from([0]),
        i32(0),
        i16(0),
        uuid,
        i32(7),
        Buffer.from([1]),
        i32(1000),
        i32(4096),
        Buffer.from([1, 1, 0])
      )
    )
    write.mockImplementation(() => {
      order.push('write')
      tapped.emit('data', response)
      return forwarded
    })
    order.length = 0
    const decode = zlib.gunzipSync
    const decoder = jest.spyOn(zlib, 'gunzipSync').mockImplementation((...args) => {
      order.push('decode')
      return decode(...args)
    })
    const hasher = jest.spyOn(require('node:crypto'), 'createHash')
    try {
      const callback = jest.fn()
      expect(tapped.write(push, callback)).toBe(forwarded)
      expect(write).toHaveBeenLastCalledWith(push, callback)
      expect(order).toEqual(['write', 'data'])
      expect(decoder).not.toHaveBeenCalled()
      expect(hasher).not.toHaveBeenCalled()
      const captured = probe.records.find((r) => r.kind === 'push')
      expect(Object.keys(captured).sort()).toEqual([
        'apiKey',
        'bodyHex',
        'correlation',
        'errorCode',
        'kind',
        'node',
        'observedMs',
        'socket',
      ])
      const records = probe.finish()
      expect(order.indexOf('decode')).toBeGreaterThan(order.indexOf('data'))
      expect(decoder).toHaveBeenCalled()
      expect(hasher).toHaveBeenCalled()
      const options = {
        active: true,
        intervalMs: 1000,
        clients: 1,
        window: { startMs: 999, endMs: 1001 },
        exports,
      }
      expect(telemetryEvidence(records, options)).toEqual({ clients: 1, pushes: 1 })
      const archived = readChunks(
        evidence.directory,
        evidence.artifacts(),
        'delivery-telemetry'
      ).flatMap((chunk) =>
        chunk
          .toString()
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line))
      )
      expect(telemetryEvidence(archived, options)).toEqual({ clients: 1, pushes: 1 })
      const corrupt = archived.map((record) => {
        if (record.kind !== 'push') return record
        const body = Buffer.from(record.bodyHex, 'hex')
        body[23] ^= 255
        return { ...record, bodyHex: body.toString('hex') }
      })
      expect(() => telemetryEvidence(corrupt, options)).toThrow()
      expect(() =>
        telemetryEvidence(archived, {
          ...options,
          exports: exports.map((e) => ({ ...e, dataSha256: '0'.repeat(64) })),
        })
      ).toThrow(/exporter/)

      const overflow = new TelemetryPushProbe({ evidence: new EvidenceWriter(tmp()), maxFrame: 16 })
      const failingSocket = new (require('node:events').EventEmitter)()
      const failedWrite = jest.fn(() => forwarded)
      failingSocket.write = failedWrite
      const failingTap = overflow.wrap(() => failingSocket)({})
      expect(failingTap.write(push, callback)).toBe(forwarded)
      expect(failedWrite).toHaveBeenCalledWith(push, callback)
      expect(() => overflow.finish()).toThrow(/latched/)
    } finally {
      decoder.mockRestore()
      hasher.mockRestore()
    }
  })

  test('captured chunks defer hashing until the telemetry series is sealed', () => {
    const evidence = new EvidenceWriter(tmp())
    const bytes = Buffer.from('captured wire bytes')
    const hasher = jest.spyOn(require('node:crypto'), 'createHash')
    try {
      evidence.chunk('delivery-telemetry', bytes, { deferHash: true })
      evidence.chunk('delivery-exports', bytes, { deferHash: true })
      expect(hasher).not.toHaveBeenCalled()
      expect(() => evidence.artifacts()).toThrow(/unsealed/)
      evidence.sealChunks('delivery-telemetry')
      expect(hasher).toHaveBeenCalled()
      expect(
        readChunks(
          evidence.directory,
          evidence.artifacts('delivery-telemetry'),
          'delivery-telemetry'
        )
      ).toEqual([bytes])
      expect(() => evidence.artifacts()).toThrow(/unsealed/)
      evidence.sealChunks('delivery-exports')
      expect(readChunks(evidence.directory, evidence.artifacts(), 'delivery-telemetry')).toEqual([
        bytes,
      ])
    } finally {
      hasher.mockRestore()
    }
  })

  test('export polling archives full rows with independent node cursors and preserves failures', async () => {
    const evidence = new EvidenceWriter(tmp())
    const requests = []
    let step = 0
    const read = async (options) => {
      requests.push(options)
      step += 1
      if (step === 1)
        return [
          { node: '1', seq: 1, dataSha256: 'a'.repeat(64) },
          { node: '2', seq: 1 },
        ]
      if (step === 2) return [{ node: '1', seq: 2 }]
      return []
    }
    const archive = new ExportArchive(evidence, read)
    await archive.start()
    await archive.poll()
    expect(await archive.finish()).toEqual([
      { node: '1', seq: 1, dataSha256: 'a'.repeat(64) },
      { node: '2', seq: 1 },
      { node: '1', seq: 2 },
    ])
    expect(requests).toEqual([{ after: {} }, { after: { 1: 1, 2: 1 } }, { after: { 1: 2, 2: 1 } }])
    const overflow = new ExportArchive(new EvidenceWriter(tmp()), async () => {
      throw new Error('sink overflow')
    })
    await expect(overflow.start()).rejects.toThrow('sink overflow')
    await overflow.stop()
    const gap = new ExportArchive(new EvidenceWriter(tmp()), async () => [{ node: '1', seq: 2 }])
    await expect(gap.start()).rejects.toThrow(/cursor/)
    await gap.stop()
  })

  test('active delivery retains wire bytes and offline in-window payload joins for both clients', async () => {
    const cluster = fakeCluster()
    const exports = []
    const sockets = []
    const original = cluster.fixture.kafka
    const raw = Buffer.from('0a14121212100a01612a0b0a09310100000000000000', 'hex')
    const i16 = (n) => {
      const b = Buffer.alloc(2)
      b.writeInt16BE(n)
      return b
    }
    const i32 = (n) => {
      const b = Buffer.alloc(4)
      b.writeInt32BE(n)
      return b
    }
    const frame = (...parts) => {
      const body = Buffer.concat(parts)
      return Buffer.concat([i32(body.length), body])
    }
    const header = (api, correlation) =>
      Buffer.concat([i16(api), i16(0), i32(correlation), i16(0), Buffer.from([0])])
    cluster.fixture.nodeFor = () => '1'
    cluster.fixture.socketFactory = () => () => {
      const socket = new (require('node:events').EventEmitter)()
      socket.write = () => true
      sockets.push(socket)
      return socket
    }
    cluster.fixture.exports = async ({ after }) =>
      exports.filter((e) => e.seq > (after[e.node] ?? 0))
    cluster.fixture.kafka = (options) => {
      const kafka = original(options)
      if (options.enableMetricsPush) {
        for (const kind of ['producer', 'consumer']) {
          const factory = kafka[kind]
          kafka[kind] = (config) => {
            const client = factory(config)
            client.connect = async () => {
              const id = sockets.length + 1
              const socket = options.socketFactory({ host: 'localhost', port: 19092 })
              socket.uuid = Buffer.alloc(16, id)
              socket.write(frame(header(71, id), socket.uuid, Buffer.from([0])))
              socket.emit(
                'data',
                frame(
                  i32(id),
                  Buffer.from([0]),
                  i32(0),
                  i16(0),
                  socket.uuid,
                  i32(7),
                  Buffer.from([1]),
                  i32(1000),
                  i32(1048576),
                  Buffer.from([1, 1, 0])
                )
              )
            }
            return client
          }
        }
      }
      return kafka
    }
    let callbacks = 0
    const hooks = {
      control: async () => {
        callbacks += 1
        if (callbacks !== 3) return
        for (let i = 0; i < sockets.length; i += 1) {
          const socket = sockets[i]
          const correlation = 10 + i
          socket.write(
            frame(
              header(72, correlation),
              socket.uuid,
              i32(7),
              Buffer.from([0, 0, raw.length + 1]),
              raw,
              Buffer.from([0])
            )
          )
          socket.emit(
            'data',
            frame(i32(correlation), Buffer.from([0]), i32(0), i16(0), Buffer.from([0]))
          )
          const hex = socket.uuid.toString('hex')
          const uuid = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
          exports.push({
            v: 1,
            node: '1',
            seq: exports.length + 1,
            clientInstanceId: uuid,
            pushIntervalMs: 1000,
            dataBytes: raw.length,
            dataSha256: hashBytes(raw),
            terminating: false,
            parse: 'ok',
          })
        }
      },
    }
    const config = deliveryConfig('eachMessage', { telemetry: 'active' })
    const { result, evidence } = await runDelivery(cluster, config, hooks)
    const decl = declaration({ mode: 'delivery', config })
    const env = envelopeOf(result, evidence, decl)
    expect(() => verifyWorker(env, decl, evidence.directory)).not.toThrow()
    const metaFile = path.join(evidence.directory, 'delivery-meta.json')
    const meta = JSON.parse(fs.readFileSync(metaFile, 'utf8'))
    expect(meta.window).toEqual({ startMs: expect.any(Number), endMs: expect.any(Number) })
    meta.window.endMs = meta.window.startMs
    const bytes = Buffer.from(JSON.stringify(meta))
    fs.writeFileSync(metaFile, bytes)
    Object.assign(
      env.artifacts.find((a) => a.name === 'delivery-meta.json'),
      { bytes: bytes.length, sha256: hashBytes(bytes) }
    )
    expect(() => verifyWorker(env, decl, evidence.directory)).toThrow()
  })

  test('normal manifests vary telemetry only in their declared roles', () => {
    for (const job of ['requests-normal', 'delivery-normal']) {
      const manifest = makeManifest(job)
      expect(() => validateManifest(manifest)).not.toThrow()
      manifest.arms[1].argv[manifest.arms[1].argv.indexOf('--telemetry') + 1] = 'active'
      expect(() => validateManifest(manifest)).toThrow()
    }
  })

  test('offline CLI opens the run directory and rejects corrupt evidence', async () => {
    const { session, directory, io } = sessionFixture('requests-normal')
    session.run()
    const output = { ...io, log: jest.fn(), error: jest.fn() }
    expect(await main(['--verify-session', directory], output)).toBe(0)
    expect(output.error).not.toHaveBeenCalled()
    const file = path.join(directory, 'raw.jsonl')
    fs.writeFileSync(file, fs.readFileSync(file, 'utf8').split('\n').slice(1).join('\n'))
    expect(await main(['--verify-session', directory], output)).toBe(1)
  })

  test('a consistently shortened rehashed delivery corpus cannot satisfy its declaration', async () => {
    const config = deliveryConfig('eachMessage')
    const { result, evidence } = await runDelivery(fakeCluster(), config)
    const decl = declaration({ mode: 'delivery', config: { ...config, messages: 1000000 } })
    const envelope = envelopeOf(result, evidence, decl)
    expect(() => verifyWorker(envelope, decl, evidence.directory)).toThrow()
    const literal = literalDelivery()
    for (const mutate of [
      (m) => {
        m.warmup = 0
      },
      (m) => {
        m.expectedCommits = { p0: '2', p1: '2' }
        m.finalCommits = m.expectedCommits
      },
      (m) => {
        m.ends = [
          { partition: 0, offset: '4' },
          { partition: 1, offset: '3' },
        ]
      },
    ]) {
      const meta = JSON.parse(JSON.stringify(literal.meta))
      mutate(meta)
      expect(() => deliveryEvidence(literal.config, literal.tables, meta)).toThrow()
    }
  })

  test('independent golden framing rejects wrong wire types duplicates and truncated fields', () => {
    const vectors = require('../../e2e/telemetry/fixtures/golden/vectors.json').vectors
    for (const name of ['hand-minimal', 'zero']) {
      const vector = vectors[name]
      const raw = Buffer.from(vector.hex, 'hex')
      expect(hashBytes(raw)).toBe(vector.sha256)
      const decoded = decodeMetricsData(raw)
      const points = decodePoints(raw)
      expect(points).toHaveLength(vector.semantic.metrics.length)
      vector.semantic.metrics.forEach((metric, i) => {
        expect(points[i]).toMatchObject({
          name: metric.name,
          unit: metric.unit,
          type: metric.kind === 'SUM' ? 'sum' : 'gauge',
          attributes: [],
          value: metric.points[0].asInt === '1' ? '1' : '0',
        })
        expect(decoded.points[i]).toMatchObject({
          start: '0',
          time: '0',
          temporality:
            metric.temporality === 'AGGREGATION_TEMPORALITY_DELTA' ? 1 : metric.temporality ? 2 : 0,
          monotonic: metric.monotonic ?? false,
        })
      })
    }
    // Literal nested lengths from the independent hand-minimal golden, with one timestamp.
    const wrap = (point) => {
      const nested = (tag, bytes) => Buffer.concat([Buffer.from([tag, bytes.length]), bytes])
      return nested(
        10,
        nested(
          18,
          nested(18, Buffer.concat([Buffer.from('0a0161', 'hex'), nested(42, nested(10, point))]))
        )
      )
    }
    const value = Buffer.from('310100000000000000', 'hex')
    const timestamp = Buffer.from('190100000000000000', 'hex')
    expect(decodeMetricsData(wrap(Buffer.concat([timestamp, value]))).points[0].time).toBe('1')
    for (const point of [
      Buffer.concat([Buffer.from('1a080100000000000000', 'hex'), value]),
      Buffer.from('32080100000000000000', 'hex'),
      Buffer.concat([timestamp, timestamp, value]),
      Buffer.concat([value, value]),
      Buffer.from('3101', 'hex'),
    ]) {
      expect(() => decodePoints(wrap(point))).toThrow()
      expect(() => decodeMetricsData(wrap(point))).toThrow()
    }
  })

  test('telemetry node joins use public cluster discovery instead of topic metadata', async () => {
    class Kafka {}
    const fixture = liveFixture(ROOT, { brokers: [], active: false }, 'topology', () => ({
      Kafka,
      logLevel: { NOTHING: 0 },
    }))
    const admin = {
      listTopics: async () => [],
      createTopics: async () => true,
      fetchTopicMetadata: async () => ({ topics: [{ partitions: Array(6).fill({}) }] }),
      describeCluster: async () => ({ brokers: [{ nodeId: 1, host: 'localhost', port: 19092 }] }),
      deleteTopics: async () => {},
    }
    await fixture.provision(admin, async () => {
      expect(fixture.nodeFor({ host: 'localhost', port: 19092 })).toBe('1')
      expect(() => fixture.nodeFor({ host: 'localhost', port: 19095 })).toThrow()
    })
  })

  test('topic setup preserves its primary error beside cleanup failures', async () => {
    const primary = new Error('ambiguous create')
    const cleanup = new Error('delete failed')
    const admin = {
      listTopics: async () => [],
      createTopics: async () => {
        throw primary
      },
      deleteTopics: async () => {
        throw cleanup
      },
    }
    class Kafka {}
    const fixture = liveFixture(ROOT, { brokers: [], active: false }, 'errors', () => ({
      Kafka,
      logLevel: { NOTHING: 0 },
    }))
    const work = jest.fn()
    const error = await fixture.provision(admin, work).catch((error) => error)
    expect(error).toBeInstanceOf(AggregateError)
    expect(error.errors).toEqual([primary, cleanup])
    expect(work).not.toHaveBeenCalled()
  })

  test('topic ownership covers ambiguous creation and failed metadata readback', async () => {
    for (const failure of ['create', 'readback', 'layout']) {
      const calls = []
      const primary = new Error(failure)
      const admin = {
        createTopics: async () => {
          calls.push('create')
          if (failure === 'create') throw primary
        },
        fetchTopicMetadata: async () => {
          calls.push('readback')
          if (failure === 'layout') return { topics: [{ partitions: [] }] }
          throw primary
        },
        deleteTopics: async () => {
          calls.push('delete')
        },
        listTopics: async () => {
          calls.push('absence')
          return []
        },
        connect: async () => {
          calls.push('connect')
        },
        disconnect: async () => {
          calls.push('disconnect')
        },
      }
      class Kafka {
        admin() {
          return admin
        }
      }
      const fixture = liveFixture(ROOT, { brokers: [], active: false }, 'cleanup', () => ({
        Kafka,
        logLevel: { NOTHING: 0 },
      }))
      const workload = new DeliveryWorkload({
        root: ROOT,
        config: deliveryConfig('eachMessage'),
        control: 0,
        evidence: new EvidenceWriter(tmp(), { identity: IDENTITY }),
        fixture,
      })
      if (failure === 'layout') await expect(workload.run()).rejects.toThrow(assert.AssertionError)
      else await expect(workload.run()).rejects.toBe(primary)
      await workload.close()
      expect(calls.slice(-3)).toEqual(['delete', 'absence', 'disconnect'])
    }
  })
})

describe('telemetry frozen provenance', () => {
  test('driver and session only overlay resolves consume bytes from the manifest and frozen roots', () => {
    const manifest = makeManifest('requests-normal')
    const overlay = tmp()
    for (const name of ['driver', 'session']) {
      const staged = path.join(overlay, path.basename(manifest[name].path))
      fs.copyFileSync(manifest[name].path, staged)
      manifest[name] = asset(staged)
    }
    const consume = path.join(manifest.arms[0].root, 'scripts/benchmarks/consumeThroughput.js')
    manifest.consume = asset(consume)
    expect(fs.readdirSync(overlay).sort()).toEqual(['session.js', 'telemetry.js'])
    const staged = require(manifest.driver.path)
    expect(() => staged.validateManifest(manifest)).not.toThrow()
    const changed = path.join(tmp(), 'consume.js')
    fs.writeFileSync(changed, 'different inherited consume bytes')
    expect(() => staged.validateManifest({ ...manifest, consume: asset(changed) })).toThrow(
      /consume driver/
    )
  })

  test('supervisor rejects unused assets runtime drift and modified frozen source before spawning', () => {
    for (const change of [
      (m) => {
        m.session = { ...m.session, path: path.join(tmp(), 'unused.js') }
      },
      (m) => {
        m.consume.sha256 = '0'.repeat(64)
      },
      (m) => {
        fs.writeFileSync(
          path.join(m.arms[0].root, 'scripts/benchmarks/consumeThroughput.js'),
          'changed consume'
        )
      },
      (m) => {
        m.driver = { ...m.session }
      },
      (m) => {
        m.node.version = 'v0.0.0'
      },
      (m) => {
        fs.writeFileSync(path.join(m.arms[0].root, 'index.js'), 'modified')
      },
      (m) => {
        fs.writeFileSync(path.join(m.arms[0].root, 'extra.js'), 'extra')
      },
      (m) => {
        m.arms[1] = { ...m.arms[1], ...frozenSource('other') }
      },
      (m) => {
        m.arms[0].source.sha256 = '0'.repeat(64)
      },
      (m) => {
        fs.writeFileSync(m.arms[0].tarball.path, 'wrong archive')
      },
    ]) {
      const manifest = makeManifest('requests-control')
      change(manifest)
      const spawnSync = jest.fn()
      expect(() =>
        new TelemetrySession({
          manifest,
          directory: path.join(tmp(), 'run'),
          io: { spawnSync },
        }).run()
      ).toThrow()
      expect(spawnSync).not.toHaveBeenCalled()
    }
  })

  test('previous source requires a matching accepted baseline receipt', () => {
    const manifest = makeManifest('requests-normal')
    expect(() => validateManifest(manifest)).not.toThrow()
    const proof = manifest.acceptedBaseline.path
    const receipt = JSON.parse(fs.readFileSync(proof, 'utf8'))
    receipt.status = 'pending'
    fs.writeFileSync(proof, JSON.stringify(receipt))
    manifest.acceptedBaseline = asset(proof)
    expect(() => validateManifest(manifest)).toThrow()
  })

  test('optional unavailability belongs only to a declared unavailable ZSTD payload cell', () => {
    for (const manifest of [
      makeManifest('payload', { optionalCodec: 'gzip' }),
      { ...makeManifest('payload'), optionalCodec: 'zstd' },
      makeManifest('requests-control', { optionalCodec: 'zstd' }),
      makeManifest('payload', {
        optionalCodec: 'zstd',
        codecs: { none: true, gzip: true, zstd: true },
      }),
    ])
      expect(() => validateManifest(manifest)).toThrow()
    const { session } = sessionFixture('payload', {}, { status: 4, stdout: '' })
    expect(() => session.run()).toThrow(/required codec/)
  })
})
