/**
 * Telemetry benchmark driver: one checked evidence core, three workload classes (requests, payload,
 * delivery), a strict worker CLI and a session supervisor. It consumes `session.js` and
 * `consumeThroughput.js` unchanged and adds nothing to the shipped library.
 *
 * Worker: node --expose-gc telemetry.js --mode requests|payload|delivery --control 0|1 <mode flags>
 * Controller: node --expose-gc telemetry.js --session <manifest> | --verify-session <directory>
 * Exit codes: 0 pass, 1 correctness/gate/child error, 2 usage, 3 void, 4 declared optional ZSTD unavailable.
 */
const assert = require('node:assert/strict')
const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const zlib = require('node:zlib')
const asyncHooks = require('node:async_hooks')
const { EventEmitter } = require('node:events')
const { spawnSync } = require('node:child_process')
const { runSession, summarize, median } = require('./session')

/** @param {unknown} value canonical JSON data @returns {string} content digest */
const digest = (value) => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex')

/** @param {Buffer|string} data bytes @returns {string} lowercase SHA-256 */
const sha256 = (data) => crypto.createHash('sha256').update(data).digest('hex')

const LOADED_SOURCES = Object.fromEntries(
  [
    ['driver', __filename],
    ['session', require.resolve('./session')],
  ].map(([name, file]) => [name, sha256(fs.readFileSync(file))])
)

/** @param {unknown} value integer candidate @param {number} min minimum @returns {boolean} valid count */
const integer = (value, min = 0) => Number.isSafeInteger(value) && value >= min

/** @param {number[]} values finite observations @returns {number} nearest-rank p99, without interpolation
 * @throws {AssertionError} for an absent, empty or nonfinite sample list
 */
const p99 = (values) => {
  assert(Array.isArray(values) && values.length > 0 && values.every(Number.isFinite))
  return [...values].sort((a, b) => a - b)[Math.ceil(values.length * 0.99) - 1]
}

/** Compares independently observed sets with a predeclared identity corpus; counts alone cannot pass.
 * @param {Array<string>} expected unique identities @param {Array<string>} actual observed identities
 * @returns {void} @throws {AssertionError} for duplicates, omission, substitution or extra work
 */
const exactSet = (expected, actual) => {
  assert(Array.isArray(expected) && Array.isArray(actual))
  assert.equal(new Set(expected).size, expected.length)
  assert.equal(new Set(actual).size, actual.length)
  assert.deepEqual([...actual].sort(), [...expected].sort())
}

/** Fixed-width numeric ledger: named float64 columns in one preallocated typed array, so a
 * million-row ledger costs no per-row object and its bytes are the sidecar.
 */
class Table {
  /** @param {string[]} columns column names @param {number} capacity maximum rows
   * @param {Float64Array} [data] existing contents for a decoded sidecar
   */
  constructor(columns, capacity, data = new Float64Array(columns.length * capacity)) {
    this.columns = columns
    this.capacity = capacity
    this.data = data
    this.length = 0
  }

  /** @param {Buffer} buffer sidecar bytes @param {string[]} columns column names
   * @returns {Table} decoded table @throws {AssertionError} for a truncated or misaligned buffer
   */
  static from(buffer, columns) {
    const width = columns.length * 8
    assert(buffer.length % width === 0)
    const data = new Float64Array(buffer.length / 8)
    for (let i = 0; i < data.length; i += 1) data[i] = buffer.readDoubleLE(i * 8)
    const table = new Table(columns, buffer.length / width, data)
    table.length = table.capacity
    return table
  }

  /** @param {...number} values one value per column @returns {void} @throws {AssertionError} when full */
  add(...values) {
    assert(this.length < this.capacity && values.length === this.columns.length)
    this.data.set(values, this.length * this.columns.length)
    this.length += 1
  }

  /** @returns {void} forgets recorded rows, keeping the storage */
  reset() {
    this.length = 0
  }

  /** @param {number} row row index @param {string} column column name @returns {number} cell */
  get(row, column) {
    return this.data[row * this.columns.length + this.columns.indexOf(column)]
  }

  /** @param {string} column column name @returns {number[]} every recorded cell of that column */
  column(column) {
    const index = this.columns.indexOf(column)
    const out = new Array(this.length)
    for (let row = 0; row < this.length; row += 1)
      out[row] = this.data[row * this.columns.length + index]
    return out
  }

  /** @returns {Buffer} the recorded rows as little-endian float64 bytes */
  buffer() {
    const out = Buffer.allocUnsafe(this.length * this.columns.length * 8)
    for (let i = 0; i < this.length * this.columns.length; i += 1)
      out.writeDoubleLE(this.data[i], i * 8)
    return out
  }
}

/** Validates the complete fixed schedule before running any worker.
 * @param {string[]} labels arms excluding the automatic reference A/A
 * @param {string} reference duplicated arm @param {number} runs measured observations per arm
 * @returns {object} immutable public schedule descriptor @throws {AssertionError} invalid schedule
 */
const schedule = (labels, reference, runs) => {
  assert(labels.length > 0 && new Set(labels).size === labels.length)
  assert(labels.every((label) => /^[a-z][a-z0-9-]*$/.test(label)))
  assert(labels.includes(reference))
  const m = labels.length + 1
  const n = m * Math.ceil(7 / m)
  assert.equal(runs, n)
  const all = [...labels, `${reference}~AA`]
  const rows = []
  for (let round = 0; round <= n; round += 1) {
    for (let position = 0; position < m; position += 1) {
      rows.push({
        round,
        position,
        label: all[(position + round) % m],
        warmup: round === 0,
      })
    }
  }
  return Object.freeze({
    labels: Object.freeze(all),
    reference,
    m,
    n,
    rows,
    hash: digest(rows),
  })
}

/** Rejects missing, rearranged, changed and extra observations, including balanced whole-round removal.
 * @param {object} declared precommitted schedule @param {Array<object>} observations raw rows
 * @returns {void} @throws {AssertionError} when the original schedule is not preserved exactly
 */
const validateSchedule = (declared, observations) => {
  assert.equal(observations.length, declared.rows.length)
  const rows = observations.map(({ round, position, label, warmup }) => ({
    round,
    position,
    label,
    warmup,
  }))
  assert.deepEqual(rows, declared.rows)
  assert.equal(digest(rows), declared.hash)
}

/** Runs the inherited scheduler without changing it; persists warmup and measured observations.
 * @param {object} declared schedule @param {Array<object>} arms inherited synchronous arm interface
 * @param {Function} append synchronous durable append, invoked once for every row
 * @returns {Array<object>} every row; warmups remain in raw evidence @throws {Error} worker/append failure
 */
const collectSession = (declared, arms, append) => {
  assert.deepEqual(
    arms.map(({ label }) => label),
    declared.labels.slice(0, -1)
  )
  const rows = runSession({
    arms,
    runs: declared.n,
    reference: declared.reference,
    onObservation: append,
  })
  validateSchedule(declared, rows)
  return rows
}

/** Applies distinct A/A noise limits to CPU/throughput and latency without rewriting session.js.
 * @param {object} declared schedule @param {Array<object>} rows complete numeric projections
 * @param {string[]} costKeys CPU/throughput keys, allowed A/A difference 3 percent
 * @param {string[]} latencyKeys p99 keys, allowed A/A difference 5 percent
 * @returns {{void:boolean,cost:object|null,latency:object|null}} all-observation summaries
 * @throws {Error} malformed rows, zero compared reference or missing metric
 */
const noise = (declared, rows, costKeys, latencyKeys) => {
  validateSchedule(declared, rows)
  const reduce = (keys, limit) =>
    keys.length
      ? summarize(rows, {
          metrics: keys,
          reference: declared.reference,
          voidOver: limit,
        })
      : null
  const cost = reduce(costKeys, 3)
  const latency = reduce(latencyKeys, 5)
  return { void: Boolean(cost?.void || latency?.void), cost, latency }
}

/** Checks integer counts and independently captured full request identities, never a reported denominator.
 * @param {object} result scalar worker result @param {object} evidence independently decoded trace projection
 * @returns {void} @throws {AssertionError} missing work, completion, physical connect or collector observation
 */
const validateRequests = (result, evidence) => {
  const { count, expected, writes, responses, returned, connected, sampled, observations } =
    evidence
  assert(integer(count, 1))
  assert.equal(expected.length, count)
  exactSet(expected, writes)
  exactSet(expected, responses)
  exactSet(expected, returned)
  assert.equal(result.completedPairs, count)
  assert.equal(result.physicalConnections, connected.length)
  exactSet(evidence.expectedConnections, connected)
  assert(connected.length > 0)
  assert.equal(result.successfulSamples, sampled)
  assert.equal(sampled, count)
  assert.equal(result.observerCalls, observations)
  assert.equal(observations, result.telemetry === 'off' ? 0 : count)
  assert.equal(result.aggregateSamples, result.telemetry === 'active' ? count : 0)
  assert.equal(evidence.aggregateSamples, result.aggregateSamples)
  assert.equal(evidence.connectionSum, result.telemetry === 'off' ? 0 : connected.length)
  assert.equal(result.cpuNsPerPair, (result.cpuMicros * 1000) / count)
  assert.equal(result.elapsedNsPerPair, result.elapsedNs / count)
}

/** Checks actual delivery entries, batch sums, durable identities, broker commits and real handler gaps.
 * @param {object} result scalar worker output @param {object} evidence independent topic/handler/commit reads
 * @returns {void} @throws {AssertionError} duplicate, fabricated count/gap, wrong value or commit
 */
const validateDelivery = (result, evidence) => {
  exactSet(evidence.expected, evidence.durable)
  exactSet(evidence.expected, evidence.delivered)
  assert.equal(
    evidence.measuredBytes.every((size) => size === result.size),
    true
  )
  assert.equal(evidence.measuredBytes.length, result.messages)
  assert.equal(result.messages, evidence.expected.length)
  assert.equal(result.deliveredMessages, evidence.delivered.length)
  assert.deepEqual(evidence.finalCommits, evidence.expectedCommits)
  assert(integer(result.callbacks, 1))
  assert.equal(result.callbacks, evidence.entries.length)
  assert.equal(result.gapSamples, Math.max(0, evidence.entries.length - 1))
  assert.equal(evidence.gaps.length, result.gapSamples)
  assert.equal(result.deliveryGapP99Ms, p99(evidence.gaps))
  assert.equal(result.p99Ms, p99(evidence.latencies))
  assert.equal(evidence.latencies.length, result.messages)
  assert.equal(result.messagesPerSecond, (result.deliveredMessages * 1e9) / result.elapsedNs)
  if (result.handler === 'eachBatch') {
    assert.equal(
      evidence.batchSizes.reduce((sum, size) => sum + size, 0),
      result.messages
    )
    assert.equal(evidence.batchSizes.length, result.callbacks)
    assert(result.gapSamples >= evidence.minimumBatchGaps)
  } else {
    assert.equal(result.callbacks, result.messages)
    assert(result.gapSamples >= evidence.minimumMessageGaps)
  }
}

/** Splits one protobuf message into raw fields; no schema, no production codec.
 * @param {Buffer} buffer message bytes @returns {Array<{number:number,wire:number,value:bigint|Buffer}>} fields
 * @throws {AssertionError} for invalid varints, lengths or field numbers
 */
const wireFields = (buffer) => {
  let offset = 0
  const readVarint = () => {
    let value = 0n
    for (let shift = 0n; shift < 70n; shift += 7n) {
      assert(offset < buffer.length)
      const byte = buffer[offset++]
      value |= BigInt(byte & 127) << shift
      if (!(byte & 128)) {
        assert(value <= 0xffffffffffffffffn, 'varint overflow')
        return value
      }
    }
    assert.fail('invalid varint')
  }
  const out = []
  while (offset < buffer.length) {
    const tag = readVarint()
    const number = Number(tag >> 3n)
    const wire = Number(tag & 7n)
    assert(number > 0 && number <= 0x1fffffff)
    let value
    if (wire === 0) value = readVarint()
    else {
      const length = wire === 1 ? 8 : wire === 5 ? 4 : wire === 2 ? Number(readVarint()) : -1
      assert(integer(length) && offset + length <= buffer.length)
      value = buffer.subarray(offset, offset + length)
      offset += length
    }
    out.push({ number, wire, value })
  }
  return out
}

/** Reads fields against the pinned OTLP wire map, preserving repeated messages only where declared.
 * @param {Buffer} bytes message bytes @param {object} schema field number to [wire, repeated]
 * @returns {Function} checked field accessor (missing scalar fields return undefined)
 * @throws {AssertionError} for unknown fields, wrong wires or duplicate scalar fields
 */
const otlpFields = (bytes, schema) => {
  const fields = new Map()
  for (const { number, wire, value } of wireFields(bytes)) {
    assert(schema[number], `unsupported OTLP field ${number}`)
    const [expected, repeated] = schema[number]
    assert.equal(wire, expected, `wrong OTLP wire type for ${number}`)
    assert(repeated || !fields.has(number), `duplicate OTLP field ${number}`)
    if (!fields.has(number)) fields.set(number, [])
    fields.get(number).push(value)
  }
  return (number) => (schema[number][1] ? (fields.get(number) ?? []) : fields.get(number)?.[0])
}

/** Independently reads the supported numeric OTLP subset; never uses the production encoder/decoder.
 * @param {Buffer} bytes actual MetricsData @returns {object[]} semantic points with decimal scalars
 * @throws {AssertionError} for invalid framing, cardinality, wire types or duplicate point identities
 */
const decodePoints = (bytes) =>
  decodeMetricsData(bytes).points.map(({ start, time, temporality, monotonic, ...point }) => point)

/** Reads one resource/scope of numeric MetricsData with the pinned field map and protobuf defaults.
 * @param {Buffer} bytes actual raw MetricsData @returns {object} full supported semantic data
 * @throws {AssertionError} for unsupported fields, malformed framing or ambiguous scalar values
 */
const decodeMetricsData = (bytes) => {
  const text = (value) => value?.toString('utf8') ?? ''
  const attrs = (buffers) => {
    const values = buffers
      .map((bytes) => {
        const field = otlpFields(bytes, { 1: [2], 2: [2] })
        assert(field(1) && field(2), 'incomplete attribute')
        const value = otlpFields(field(2), { 1: [2] })
        assert(value(1), 'attribute must be a string')
        return { key: text(field(1)), value: text(value(1)) }
      })
      .sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0))
    exactSet(
      values.map(({ key }) => key),
      values.map(({ key }) => key)
    )
    return values
  }
  const top = otlpFields(bytes, { 1: [2, true] })
  assert.equal(top(1).length, 1)
  const rm = otlpFields(top(1)[0], { 1: [2], 2: [2, true], 3: [2] })
  const resource = otlpFields(rm(1) ?? Buffer.alloc(0), { 1: [2, true], 2: [0] })
  assert.equal(rm(2).length, 1)
  const sm = otlpFields(rm(2)[0], { 1: [2], 2: [2, true], 3: [2] })
  const scope = otlpFields(sm(1) ?? Buffer.alloc(0), { 1: [2], 2: [2], 3: [2, true], 4: [0] })
  assert.equal(scope(3).length, 0, 'scope attributes are outside the supported subset')
  const points = []
  for (const metricBytes of sm(2)) {
    const metric = otlpFields(metricBytes, { 1: [2], 2: [2], 3: [2], 5: [2], 7: [2] })
    assert(metric(1)?.length, 'metric name is required')
    assert(Boolean(metric(5)) !== Boolean(metric(7)), 'exactly one metric type is required')
    const sum = Boolean(metric(7))
    const aggregate = otlpFields(
      metric(7) ?? metric(5),
      sum ? { 1: [2, true], 2: [0], 3: [0] } : { 1: [2, true] }
    )
    assert(aggregate(1).length > 0, 'empty metric')
    const temporality = sum ? Number(aggregate(2) ?? 0n) : 0
    const monotonic = sum ? (aggregate(3) ?? 0n) : 0n
    assert(!sum || [0, 1, 2].includes(temporality))
    assert(monotonic === 0n || monotonic === 1n)
    for (const pointBytes of aggregate(1)) {
      const point = otlpFields(pointBytes, {
        2: [1],
        3: [1],
        4: [1],
        6: [1],
        7: [2, true],
        8: [2, true],
        9: [0],
      })
      assert.equal(point(8).length, 0, 'exemplars are outside the supported subset')
      assert.equal(point(9) ?? 0n, 0n)
      assert(Boolean(point(6)) !== Boolean(point(4)), 'exactly one numeric value is required')
      const value = point(6)
        ? point(6).readBigInt64LE().toString()
        : point(4).readDoubleLE().toString()
      assert(Number.isFinite(Number(value)))
      points.push({
        name: text(metric(1)),
        unit: text(metric(3)),
        type: sum ? 'sum' : 'gauge',
        attributes: attrs(point(7)),
        value,
        start: (point(2)?.readBigUInt64LE() ?? 0n).toString(),
        time: (point(3)?.readBigUInt64LE() ?? 0n).toString(),
        temporality,
        monotonic: monotonic === 1n,
      })
    }
  }
  const identities = points.map(({ name, attributes }) => JSON.stringify([name, attributes]))
  exactSet(identities, identities)
  return {
    resource: attrs(resource(1)),
    dropped: String(resource(2) ?? 0n),
    scope: { name: text(scope(1)), version: text(scope(2)) },
    points,
  }
}

/** Validates actual raw and transmitted buffers against independently decoded expected point values.
 * @param {object} result encoded payload metadata @param {object} evidence raw/transmitted/expected/omissions
 * @returns {void} @throws {AssertionError} invented bytes, omissions, points, values or fit counts
 */
const validatePayload = (result, evidence) => {
  assert(Buffer.isBuffer(evidence.raw) && Buffer.isBuffer(evidence.transmitted))
  assert.equal(result.rawBytes, evidence.raw.length)
  assert.equal(result.transmittedBytes, evidence.transmitted.length)
  assert(result.rawBytes <= evidence.cap && result.transmittedBytes <= evidence.cap)
  assert(integer(result.fittingPasses, 1) && result.fittingPasses <= 8)
  const actual = decodePoints(evidence.raw)
  const key = ({ name, attributes }) => JSON.stringify([name, attributes])
  exactSet(evidence.expected.map(key), [...actual.map(key), ...evidence.omitted])
  exactSet(evidence.expectedOmitted, evidence.omitted)
  exactSet(evidence.included, actual.map(key))
  const byKey = new Map(evidence.expected.map((expected) => [key(expected), expected]))
  for (const point of actual) assert.deepEqual(point, byKey.get(key(point)))
}

/** @param {number} i custom series ordinal @param {'compressible'|'poor'} attributes value workload
 * @returns {{metric:object,expected:object}} registration descriptor (attributes object) and the
 * literal expected snapshot shape (sorted attribute array); values are `i + 1`
 */
const customSeries = (i, attributes) => {
  const labels = Array.from({ length: 8 }, (_, label) => ({
    key: `k${label}`.padEnd(63, 'k'),
    value:
      attributes === 'compressible'
        ? 'a'.repeat(256)
        : Array.from({ length: 8 }, (_, chunk) =>
            crypto.createHash('sha256').update(`${i}:${label}:${chunk}`).digest('hex').slice(0, 32)
          ).join(''),
  }))
  const metric = {
    name: `com.example.${i}.`.padEnd(200, 'n'),
    type: 'sum',
    unit: '1'.padEnd(16, 'u'),
    attributes: Object.fromEntries(labels.map(({ key, value }) => [key, value])),
    value: BigInt(i + 1),
  }
  return { metric, expected: { ...metric, attributes: labels, value: metric.value.toString() } }
}

/** Builds production-legal sender inputs; custom limits and built-in count are never raised.
 * @param {Function} Metrics candidate's approved collector @param {object} options point count/attrs/temporality
 * @returns {object} collector, literal expected values, accepted subscription and owned snapshot
 * @throws {Error} production validation failure; cannot substitute a preassembled fake snapshot
 */
const payloadWorkload = (Metrics, { points, attributes, temporality }) => {
  assert([1, 257].includes(points))
  assert(['compressible', 'poor'].includes(attributes))
  assert(['delta', 'cumulative'].includes(temporality))
  const metrics = new Metrics({ kind: 'producer' })
  const subscription = {
    revision: 1n,
    requestedMetrics: ['*'],
    deltaTemporality: temporality === 'delta',
  }
  const expected = []
  if (points === 257) {
    for (let i = 0; i < 128; i += 1) {
      const series = customSeries(i, attributes)
      metrics.register(series.metric)
      expected.push(series.expected)
    }
  }
  metrics.select(subscription)
  metrics.observeConnection({})
  expected.push({
    name: 'org.apache.kafka.producer.connection.creation.total',
    type: 'sum',
    unit: '1',
    attributes: [],
    value: '1',
  })
  if (points === 257) {
    for (let nodeId = 0; nodeId < 64; nodeId += 1) {
      const slot = metrics.bindNode({
        nodeId,
        endpoint: `node${nodeId}:9092`,
        endpointRevision: 1,
      })
      metrics.observeRequest(slot, nodeId + 1)
      for (const suffix of ['avg', 'max'])
        expected.push({
          name: `org.apache.kafka.producer.node.request.latency.${suffix}`,
          type: 'gauge',
          unit: 'ms',
          attributes: [{ key: 'node_id', value: `${nodeId}` }],
          value: `${nodeId + 1}`,
        })
    }
  }
  const snapshot = metrics.snapshot(subscription, {})
  assert.equal(snapshot.points.length, points)
  return { metrics, subscription, snapshot, expected }
}

/** Names the fixed absolute and relative ceilings a median breaches; the single gate arithmetic.
 * @param {string} mode requests/payload/delivery @param {object} value worker median metrics
 * @param {object} reference respective baseline medians
 * @returns {string[]} metric names outside their declared limit, empty when every gate passes
 */
const gateFailures = (mode, value, reference) => {
  if (mode === 'requests')
    return value.cpuNsPerPair <= reference.cpuNsPerPair * (value.telemetry === 'off' ? 1.03 : 1.05)
      ? []
      : ['cpuNsPerPair']
  if (mode === 'payload')
    return [
      ['sliceP99Ms', value.sliceP99Ms <= 10],
      ['cpuP99Ms', value.cpuP99Ms <= 25],
      ['wallP99Ms', value.wallP99Ms <= 100],
      ['fittingPasses', value.fittingPasses <= 8],
      ['heapExternalDeltaBytes', value.heapExternalDeltaBytes <= 8388608],
      ['rssDeltaBytes', value.rssDeltaBytes <= 67108864],
    ]
      .filter(([, passes]) => !passes)
      .map(([name]) => name)
  assert.equal(mode, 'delivery')
  return [
    ['messagesPerSecond', value.messagesPerSecond >= reference.messagesPerSecond * 0.97],
    ['p99Ms', value.p99Ms <= reference.p99Ms * 1.05 + 1],
  ]
    .filter(([, passes]) => !passes)
    .map(([name]) => name)
}

/** Applies predeclared numeric performance gates only to complete already-validated session medians.
 * @param {string} mode requests/payload/delivery @param {object} value worker median metrics
 * @param {object} reference respective baseline medians @returns {boolean} true when all gates pass
 */
const withinGates = (mode, value, reference) => gateFailures(mode, value, reference).length === 0

/** Validates the scalar envelope before any mode oracle; artifacts are externally re-read and hash-checked.
 * @param {object} result emitted data @param {object} declaration immutable expected run identity/config
 * @returns {void} @throws {AssertionError} unknown version, identity/config drift, missing scalar or nonfinite data
 */
const validateEnvelope = (result, declaration) => {
  assert.equal(result.schemaVersion, 1)
  for (const name of ['runId', 'tree', 'driverSha256', 'manifestSha256', 'mode', 'configSha256']) {
    assert.equal(result[name], declaration[name])
  }
  assert.equal(result.control, declaration.control)
  assert([0, 1].includes(result.control))
  const required = {
    requests: [
      'completedPairs',
      'physicalConnections',
      'successfulSamples',
      'observerCalls',
      'aggregateSamples',
      'cpuMicros',
      'elapsedNs',
      'cpuNsPerPair',
      'elapsedNsPerPair',
    ],
    payload: [
      'rawBytes',
      'transmittedBytes',
      'fittingPasses',
      'sliceP99Ms',
      'cpuP99Ms',
      'wallP99Ms',
      'heapExternalDeltaBytes',
      'rssDeltaBytes',
    ],
    delivery: [
      'messages',
      'size',
      'callbacks',
      'deliveredMessages',
      'gapSamples',
      'elapsedNs',
      'messagesPerSecond',
      'p99Ms',
      'deliveryGapP99Ms',
    ],
  }[result.mode]
  assert(required)
  for (const key of required) assert(Number.isFinite(result[key]) && result[key] >= 0, key)
  const counts = {
    requests: [
      'completedPairs',
      'physicalConnections',
      'successfulSamples',
      'observerCalls',
      'aggregateSamples',
    ],
    payload: ['rawBytes', 'transmittedBytes', 'fittingPasses'],
    delivery: ['messages', 'size', 'callbacks', 'deliveredMessages', 'gapSamples'],
  }[result.mode]
  for (const key of counts) assert(integer(result[key]), key)
  assert(Array.isArray(result.artifacts) && result.artifacts.length > 0)
  for (const artifact of result.artifacts) {
    assert(/^[a-zA-Z0-9][a-zA-Z0-9_.-]*$/.test(artifact.name))
    assert(/^[a-f0-9]{64}$/.test(artifact.sha256))
    assert(integer(artifact.bytes))
  }
  exactSet(
    result.artifacts.map(({ name }) => name),
    result.artifacts.map(({ name }) => name)
  )
}

/** Executes the selected positive control inside the observed callback or serializer entry.
 * @param {'requests'|'payload'|'delivery'} mode measured path
 * @param {number} enabled explicit 0 or 1 @param {Function} now monotonic bigint clock
 * @param {Function} wait awaited millisecond timer, replaceable with a virtual waiter in tests
 * @returns {number|Promise<number>} declared nanoseconds; virtual controls test placement without timing
 * @throws {AssertionError} invalid mode or flag
 */
const samePathControl = (
  mode,
  enabled,
  now = process.hrtime.bigint,
  wait = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds))
) => {
  const delay = { requests: 10000n, payload: 100000000n, delivery: 1000000n }[mode]
  assert(delay !== undefined && [0, 1].includes(enabled))
  if (!enabled) return 0
  if (mode === 'delivery') return wait(1).then(() => Number(delay))
  const end = now() + delay
  while (now() < end) {
    continue
  }
  return Number(delay)
}

/** @param {{heapUsed:number,external:number}} before reading before the work
 * @param {{heapUsed:number,external:number}} after reading after the work and a forced collection
 * @returns {{heap:number,external:number,sum:number}} signed deltas, never clamped
 * @throws {AssertionError} for nonfinite or negative raw readings
 */
const signedDelta = (before, after) => {
  for (const reading of [before, after])
    for (const key of ['heapUsed', 'external'])
      assert(Number.isFinite(reading[key]) && reading[key] >= 0, key)
  const heap = after.heapUsed - before.heapUsed
  const external = after.external - before.external
  return { heap, external, sum: heap + external }
}

/** @param {number} maxRssKiB `process.resourceUsage().maxRSS`, kibibytes on Linux
 * @param {number} baselineBytes fresh pre-work RSS @returns {number} max(0, high-water bytes - baseline)
 * @throws {AssertionError} for nonfinite or negative inputs
 */
const rssDelta = (maxRssKiB, baselineBytes) => {
  assert(Number.isFinite(maxRssKiB) && maxRssKiB >= 0)
  assert(Number.isFinite(baselineBytes) && baselineBytes >= 0)
  return Math.max(0, maxRssKiB * 1024 - baselineBytes)
}

/** Raised when a required codec is not available on this runtime; exit code 4 for an optional cell. */
class CodecUnavailableError extends Error {}

/** Raised for a command line the driver cannot act on; exit code 2. */
class UsageError extends Error {}

const ARTIFACT_NAME = /^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/
const MIB = 1048576
const NOFOLLOW = fs.constants.O_NOFOLLOW

/** @param {string} directory absolute path @returns {string} the path of a new private directory
 * @throws {Error} if the path is relative, exists, or its parent is not a real directory
 */
const makeExclusiveDirectory = (directory) => {
  assert(path.isAbsolute(directory), 'evidence directory must be absolute')
  const parent = fs.lstatSync(path.dirname(directory))
  assert(
    parent.isDirectory() && !parent.isSymbolicLink(),
    'evidence parent must be a real directory'
  )
  fs.mkdirSync(directory, { mode: 0o700 })
  return directory
}

/** Writes sealed, immutable evidence files into one private empty directory.
 * Files are created exclusively without following links and fsynced. Capture chunks can defer hashing
 * until their series is sealed outside the measured window.
 */
class EvidenceWriter {
  #directory
  #sealed = []
  #names = new Set()
  #bytes = 0
  #chunks = new Map()

  /** @param {string} directory existing empty real directory
   * @param {{identity?:object,maxBytes?:number,maxArtifacts?:number}} [options] run identity carried into
   * sidecars, total byte cap (default 256 MiB) and artifact cap (default 1024)
   * @throws {AssertionError} when the directory is a link, not empty or not absolute
   */
  constructor(directory, { identity = {}, maxBytes = 256 * MIB, maxArtifacts = 1024 } = {}) {
    assert(path.isAbsolute(directory))
    const stat = fs.lstatSync(directory)
    assert(stat.isDirectory() && !stat.isSymbolicLink(), 'evidence directory must be real')
    assert.equal(fs.readdirSync(directory).length, 0, 'evidence directory must be empty')
    this.#directory = directory
    this.identity = identity
    this.maxBytes = maxBytes
    this.maxArtifacts = maxArtifacts
  }

  /** @returns {string} the directory this writer owns */
  get directory() {
    return this.#directory
  }

  /** Opens one append-only artifact; it is immutable once sealed.
   * @param {string} name simple basename @param {{deferHash?:boolean}} [options] hash at series sealing
   * @returns {{append:(data:Buffer)=>void,seal:()=>object}} stream
   * @throws {AssertionError} for a bad or repeated name, or when a byte/artifact cap would be exceeded
   */
  stream(name, { deferHash = false } = {}) {
    assert(ARTIFACT_NAME.test(name) && !this.#names.has(name), `bad artifact name ${name}`)
    assert(this.#names.size < this.maxArtifacts, 'too many artifacts')
    this.#names.add(name)
    const fd = fs.openSync(
      path.join(this.#directory, name),
      fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | NOFOLLOW,
      0o600
    )
    const hash = deferHash ? null : crypto.createHash('sha256')
    let bytes = 0
    let open = true
    const close = () => {
      if (open) fs.closeSync(fd)
      open = false
    }
    return {
      append: (data) => {
        try {
          assert(open && this.#bytes + data.length <= this.maxBytes, 'evidence byte cap exceeded')
          for (let written = 0; written < data.length;)
            written += fs.writeSync(fd, data, written, data.length - written)
          hash?.update(data)
          bytes += data.length
          this.#bytes += data.length
        } catch (error) {
          close()
          throw error
        }
      },
      seal: () => {
        fs.fsyncSync(fd)
        close()
        const artifact = { name, bytes, sha256: hash?.digest('hex') ?? null }
        this.#sealed.push(artifact)
        return artifact
      },
    }
  }

  /** @param {string} name basename @param {Buffer|string} data complete contents
   * @param {{deferHash?:boolean}} [options] hash at series sealing
   * @returns {{name:string,bytes:number,sha256:string|null}} the artifact; deferred hashes await series sealing
   * @throws {AssertionError} caps
   */
  write(name, data, options) {
    const buffer = Buffer.isBuffer(data) ? data : Buffer.from(data)
    const stream = this.stream(name, options)
    stream.append(buffer)
    return stream.seal()
  }

  /** Seals one bounded numbered chunk of a series that is read back in order.
   * @param {string} prefix series name @param {Buffer} data at most 2 MiB; a series holds at most 32 MiB
   * @param {{deferHash?:boolean}} [options] defer capture hashing until `sealChunks`, outside timing
   * @returns {object} the sealed chunk @throws {AssertionError} when a chunk or series cap is exceeded
   */
  chunk(prefix, data, options) {
    const series = this.#chunks.get(prefix) ?? { count: 0, bytes: 0, deferred: [] }
    assert(data.length <= 2 * MIB && series.bytes + data.length <= 32 * MIB, 'chunk cap exceeded')
    this.#chunks.set(prefix, series)
    const artifact = this.write(`${prefix}.${String(series.count).padStart(6, '0')}`, data, options)
    if (artifact.sha256 === null) series.deferred.push(artifact)
    series.count += 1
    series.bytes += data.length
    return artifact
  }

  /** Hashes deferred chunks and writes the series index; callers keep this outside measured windows.
   * @param {string} prefix chunk series @returns {object} sealed index artifact recording its count
   * @throws {AssertionError|Error} if a captured chunk is no longer a regular file of its recorded size
   */
  sealChunks(prefix) {
    const series = this.#chunks.get(prefix)
    for (const artifact of series?.deferred ?? []) {
      const fd = fs.openSync(
        path.join(this.#directory, artifact.name),
        fs.constants.O_RDONLY | NOFOLLOW
      )
      try {
        const stat = fs.fstatSync(fd)
        assert(stat.isFile() && stat.size === artifact.bytes, 'captured chunk differs')
        artifact.sha256 = sha256(fs.readFileSync(fd))
      } finally {
        fs.closeSync(fd)
      }
    }
    if (series) series.deferred = []
    return this.write(`${prefix}.index`, JSON.stringify({ count: series?.count ?? 0 }))
  }

  /** @param {string} [prefix] restrict to one independently sealed chunk series
   * @returns {Array<{name:string,bytes:number,sha256:string}>} sealed artifacts, in creation order
   * @throws {AssertionError} while selected capture chunks still await their post-window hashes
   */
  artifacts(prefix) {
    const artifacts = this.#sealed.filter(({ name }) => !prefix || name.startsWith(`${prefix}.`))
    assert(
      artifacts.every((artifact) => artifact.sha256 !== null),
      'unsealed capture chunks'
    )
    return artifacts
  }
}

/** Rereads one declared artifact: regular non-link file, exact size and hash.
 * @param {string} directory run directory @param {{name:string,bytes:number,sha256:string}} artifact declaration
 * @param {number} [limit] maximum bytes @returns {Buffer} the verified bytes
 * @throws {AssertionError} for a path escape, link, wrong size, truncation or hash mismatch
 */
const readArtifact = (directory, { name, bytes, sha256: expected }, limit = 256 * MIB) => {
  assert(ARTIFACT_NAME.test(name), `bad artifact name ${name}`)
  const file = path.join(directory, name)
  assert.equal(path.dirname(path.resolve(file)), path.resolve(directory))
  const stat = fs.lstatSync(file)
  assert(stat.isFile() && !stat.isSymbolicLink(), `${name} must be a regular file`)
  assert(stat.size === bytes && bytes <= limit, `${name} has the wrong size`)
  const fd = fs.openSync(file, fs.constants.O_RDONLY | NOFOLLOW)
  try {
    const buffer = Buffer.alloc(bytes)
    for (let read = 0; read < bytes;) {
      const count = fs.readSync(fd, buffer, read, bytes - read, read)
      assert(count > 0, `${name} is truncated`)
      read += count
    }
    assert.equal(sha256(buffer), expected, `${name} hash mismatch`)
    return buffer
  } finally {
    fs.closeSync(fd)
  }
}

/** @param {string} directory run directory @param {Array<{name:string}>} artifacts declared files
 * @returns {void} @throws {AssertionError} unless the directory holds exactly the declared regular files
 */
const verifyDirectory = (directory, artifacts) => {
  const listed = fs.readdirSync(directory)
  for (const name of listed) {
    const stat = fs.lstatSync(path.join(directory, name))
    assert(stat.isFile() && !stat.isSymbolicLink(), `${name} must be a regular file`)
  }
  exactSet(
    artifacts.map(({ name }) => name),
    listed
  )
}

/** Reads a verified chunk series back in order.
 * @param {string} directory run directory @param {Array<object>} artifacts declared artifacts
 * @param {string} prefix series name @returns {Buffer[]} the chunks, ordinals 0..count-1
 * @throws {AssertionError} for a missing index, gap or extra chunk
 */
const readChunks = (directory, artifacts, prefix) => {
  const find = (name) => artifacts.find((artifact) => artifact.name === name)
  const index = find(`${prefix}.index`)
  assert(index, `${prefix} has no index`)
  const { count } = JSON.parse(readArtifact(directory, index, 8 * MIB).toString('utf8'))
  assert(integer(count))
  const extra = artifacts.filter(
    ({ name }) => name.startsWith(`${prefix}.`) && name !== `${prefix}.index`
  )
  assert.equal(extra.length, count)
  return Array.from({ length: count }, (_, ordinal) => {
    const artifact = find(`${prefix}.${String(ordinal).padStart(6, '0')}`)
    assert(artifact, `${prefix} is missing chunk ${ordinal}`)
    return readArtifact(directory, artifact, 2 * MIB)
  })
}

/** Takes memory readings through injectable functions so tests can assert their placement. */
class MemoryProbe {
  /** @param {{memory?:Function,gc?:Function}} [hooks] reading and collection functions */
  constructor({ memory = () => process.memoryUsage(), gc = globalThis.gc } = {}) {
    this.memory = memory
    this.gc = gc
  }

  /** @returns {void} @throws {AssertionError} unless garbage collection can be forced */
  requireGc() {
    assert.equal(
      typeof this.gc,
      'function',
      'run with --expose-gc: memory evidence needs global.gc'
    )
  }

  /** Forces a collection, then reads; callers place it outside every timed window.
   * @returns {{heapUsed:number,external:number,rss:number}} reading after collection
   * @throws {AssertionError} without garbage collection or on a nonfinite/negative reading
   */
  settled() {
    this.requireGc()
    this.gc()
    return this.raw()
  }

  /** @returns {{heapUsed:number,external:number,rss:number}} unforced reading
   * @throws {AssertionError} on a nonfinite or negative reading
   */
  raw() {
    const { heapUsed, external, rss } = this.memory()
    for (const value of [heapUsed, external, rss])
      assert(Number.isFinite(value) && value >= 0, 'unreliable memory reading')
    return { heapUsed, external, rss }
  }
}

/** Measures contiguous JavaScript occupancy of one serialization: the synchronous entry slice plus every
 * continuation slice of promises created beneath it, through a driver-local async hook.
 */
class SliceMonitor {
  #now
  #hooks
  #hook = null
  #tracked = new Set()
  #open = new Map()
  #inCall = false
  #slices = []

  /** @param {{now?:()=>bigint,asyncHooks?:object}} [options] monotonic nanosecond clock and a
   * `createHook` provider; tests supply virtual ones
   */
  constructor({ now = process.hrtime.bigint, asyncHooks: provider = asyncHooks } = {}) {
    this.#now = now
    this.#hooks = provider
  }

  /** @returns {void} installs and enables the hook for one sample */
  begin() {
    this.#tracked.clear()
    this.#open.clear()
    this.#slices = []
    this.#hook = this.#hooks.createHook({
      init: (id, type, trigger) => {
        if (this.#inCall || this.#tracked.has(trigger)) this.#tracked.add(id)
      },
      before: (id) => {
        if (this.#tracked.has(id)) this.#open.set(id, this.#now())
      },
      after: (id) => {
        if (!this.#open.has(id)) return
        this.#slices.push(this.#now() - this.#open.get(id))
        this.#open.delete(id)
      },
    })
    this.#hook.enable()
  }

  /** Brackets the synchronous entry of the serialization (control, fit and first await).
   * @param {Function} work starts the serialization @returns {*} what `work` returns
   */
  call(work) {
    const start = this.#now()
    this.#inCall = true
    try {
      return work()
    } finally {
      this.#inCall = false
      this.#slices.push(this.#now() - start)
    }
  }

  /** @returns {{slicesNs:bigint[],maxSliceMs:number,hookEnabled:boolean}} the sample's slices; the hook is disabled */
  end() {
    this.#hook.disable()
    this.#hook = null
    const maxNs = this.#slices.reduce((max, slice) => (slice > max ? slice : max), 0n)
    return { slicesNs: [...this.#slices], maxSliceMs: Number(maxNs) / 1e6, hookEnabled: true }
  }
}

/** Reports progress to stderr at a fixed interval so a long worker is observable without joining its timing. */
class Watchdog {
  #timer = null

  /** @param {{write?:Function,interval?:number,setTimer?:Function,clearTimer?:Function}} [options] */
  constructor({
    write = (line) => process.stderr.write(line),
    interval = 30000,
    setTimer = setInterval,
    clearTimer = clearInterval,
  } = {}) {
    this.write = write
    this.interval = interval
    this.setTimer = setTimer
    this.clearTimer = clearTimer
  }

  /** @param {string} label work name @param {()=>object} progress current counters @returns {void} */
  start(label, progress) {
    assert.equal(this.#timer, null)
    this.#timer = this.setTimer(
      () => this.write(`${JSON.stringify({ label, ...progress() })}\n`),
      this.interval
    )
  }

  /** @returns {void} cancels the timer; safe to repeat */
  stop() {
    if (this.#timer !== null) this.clearTimer(this.#timer)
    this.#timer = null
  }
}

/** @returns {{sockets:number,timers:number,children:number,pending:number,files:number}} a clean report */
const cleanReport = () => ({ sockets: 0, timers: 0, children: 0, pending: 0, files: 0 })

const WALL_MS = 1700000000000
const MONO0 = 1000n
const STEP_NS = 1000000000n
const PRODUCER = 'org.apache.kafka.producer.'
const seriesCache = new Map()

/** @param {number} i custom series ordinal @param {string} attributes workload
 * @returns {{metric:object,expected:object}} memoized legal series, used only by the fixture and its oracle
 */
const cachedSeries = (i, attributes) => {
  const key = `${attributes}:${i}`
  if (!seriesCache.has(key)) seriesCache.set(key, customSeries(i, attributes))
  return seriesCache.get(key)
}

/** Time source the fixture advances by hand, so every snapshot timestamp is a literal. */
class FixtureClock {
  /** @returns {void} starts at sample 0 */
  constructor() {
    this.mono = MONO0
  }

  /** @returns {number} fixed wall milliseconds */
  wallMs() {
    return WALL_MS
  }

  /** @returns {bigint} current monotonic nanoseconds */
  monotonicNs() {
    return this.mono
  }

  /** @param {number} k sample ordinal @returns {void} moves the clock to that sample's instant */
  set(k) {
    this.mono = MONO0 + BigInt(k) * STEP_NS
  }
}

/** @param {number} k sample ordinal @returns {bigint} the literal nanosecond timestamp of sample k */
const instant = (k) => BigInt(WALL_MS) * 1000000n + BigInt(k) * STEP_NS

/** @param {{points:number,attributes:string,temporality:string}} config workload dimensions
 * @param {number} k sample ordinal, from 1 @param {string} scopeVersion collector package version
 * @returns {{points:object[],facts:Map<string,object>,resource:object[],dropped:string,scope:object}}
 * what a correct sender must encode for sample k, derived from the workload literals alone
 */
const expectedSample = ({ points, attributes, temporality }, k, scopeVersion) => {
  const delta = temporality === 'delta'
  const list = []
  const facts = new Map()
  const add = (point, sum) => {
    const key = JSON.stringify([point.name, point.attributes])
    list.push(point)
    facts.set(key, {
      start: sum ? String(instant(delta ? k - 1 : 0)) : '0',
      time: String(instant(k)),
      temporality: sum ? (delta ? 1 : 2) : 0,
      monotonic: sum,
    })
  }
  if (points === 257) {
    for (let i = 0; i < 128; i += 1) {
      const { expected } = cachedSeries(i, attributes)
      add({ ...expected, value: String(delta ? i + 1 : (i + 1) * k) }, true)
    }
  }
  add(
    {
      name: `${PRODUCER}connection.creation.total`,
      type: 'sum',
      unit: '1',
      attributes: [],
      value: String(delta ? 1 : k),
    },
    true
  )
  if (points === 257) {
    for (let nodeId = 0; nodeId < 64; nodeId += 1)
      for (const suffix of ['avg', 'max'])
        add(
          {
            name: `${PRODUCER}node.request.latency.${suffix}`,
            type: 'gauge',
            unit: 'ms',
            attributes: [{ key: 'node_id', value: `${nodeId}` }],
            value: `${nodeId + 1}`,
          },
          false
        )
  }
  return {
    points: list,
    facts,
    resource: [],
    dropped: '0',
    scope: { name: 'countly-kafkajs', version: scopeVersion },
  }
}

/** Checks timestamps, temporality, monotonic flags, resource and scope of a fully decoded body.
 * @param {object} decoded `decodeMetricsData` output @param {object} expected `expectedSample` output
 * @returns {void} @throws {AssertionError} on any temporal, resource or scope difference
 */
const validateFacts = (decoded, expected) => {
  assert.deepEqual(decoded.resource, expected.resource)
  assert.equal(decoded.dropped, expected.dropped)
  assert.deepEqual(decoded.scope, expected.scope)
  assert.equal(decoded.points.length, expected.facts.size)
  for (const point of decoded.points) {
    const fact = expected.facts.get(JSON.stringify([point.name, point.attributes]))
    assert(fact, `unexpected point ${point.name}`)
    assert.equal(point.start, fact.start)
    assert.equal(point.time, fact.time)
    assert.equal(point.temporality, fact.temporality)
    assert.equal(point.monotonic, fact.monotonic)
  }
}

/** Decompresses an actual transmitted body with built-in zlib, capped at the 1 MiB protocol bound.
 * @param {number} type compression type 0 none, 1 gzip, 4 zstd @param {Buffer} body transmitted bytes
 * @returns {Buffer} raw bytes @throws {CodecUnavailableError|Error} for an unsupported type or corrupt body
 */
/* eslint-disable n/no-unsupported-features/node-builtins -- guarded by the typeof check below */
const inflate = (type, body) => {
  const options = { maxOutputLength: MIB }
  if (type === 0) return body
  if (type === 1) return zlib.gunzipSync(body, options)
  assert.equal(type, 4, 'unknown compression type')
  if (typeof zlib.zstdDecompressSync !== 'function')
    throw new CodecUnavailableError('zstd decompression is unavailable')
  return zlib.zstdDecompressSync(body, options)
}
/* eslint-enable n/no-unsupported-features/node-builtins */

/** Builds fresh production snapshots of one legal workload through accepted collector transitions. */
class PayloadFixture {
  /** @param {Function} Metrics candidate collector class
   * @param {{points:number,attributes:string,temporality:string}} config workload dimensions
   * @throws {Error} production validation failure for an illegal descriptor
   */
  constructor(Metrics, { points, attributes, temporality }) {
    assert([1, 257].includes(points))
    assert(['compressible', 'poor'].includes(attributes))
    assert(['delta', 'cumulative'].includes(temporality))
    this.config = { points, attributes, temporality }
    this.clock = new FixtureClock()
    this.metrics = new Metrics({ kind: 'producer', clock: this.clock })
    this.subscription = {
      revision: 1n,
      requestedMetrics: ['*'],
      deltaTemporality: temporality === 'delta',
    }
    this.series =
      points === 257 ? Array.from({ length: 128 }, (_, i) => cachedSeries(i, attributes)) : []
    for (const { metric } of this.series) this.metrics.register(metric)
    this.metrics.select(this.subscription)
    this.slots =
      points === 257
        ? Array.from({ length: 64 }, (_, nodeId) =>
            this.metrics.bindNode({ nodeId, endpoint: `node${nodeId}:9092`, endpointRevision: 1 })
          )
        : []
  }

  /** @param {number} k sample ordinal from 1, increasing by one
   * @returns {object} a fresh immutable collector snapshot with the literal step applied
   * @throws {Error} when the previous sample was neither written nor abandoned
   */
  next(k) {
    this.clock.set(k)
    this.metrics.observeConnection({})
    this.series.forEach(({ metric }, i) => {
      this.metrics.register({ ...metric, value: BigInt((i + 1) * k) })
    })
    this.slots.forEach((slot, nodeId) => this.metrics.observeRequest(slot, nodeId + 1))
    return this.metrics.snapshot(this.subscription, {})
  }
}

const TUPLE = ['socket', 'generation', 'correlation', 'send']
const REQUEST_SUBSCRIPTION = { revision: 1n, requestedMetrics: ['*'], deltaTemporality: true }

/** @param {Table} table four-column tuple ledger @returns {string[]} one identity string per row */
const tupleIds = (table) => {
  const width = table.columns.length
  const { data } = table
  const out = new Array(table.length)
  for (let row = 0; row < table.length; row += 1) {
    const at = row * width
    out[row] = `${data[at]}.${data[at + 1]}.${data[at + 2]}.${data[at + 3]}`
  }
  return out
}

/** The identity tuples a correct run must produce: one socket per node, generation 1, per-node
 * correlation counting on from the warmups, send ordinal equal to the measured index.
 * @param {{iterations:number,warmup:number,nodes:number}} config request counts
 * @returns {string[]} expected measured identities, derived from the configuration alone
 */
const expectedTuples = ({ iterations, warmup, nodes }) => {
  const base = (node) => Math.floor(warmup / nodes) + (node < warmup % nodes ? 1 : 0)
  return Array.from({ length: iterations }, (_, i) => {
    const node = i % nodes
    return `${node}.1.${base(node) + Math.floor(i / nodes)}.${i}`
  })
}

/** @param {Table} observed node/duration ledger @param {number} nodes node count
 * @returns {{counts:number[],sums:number[],maxes:number[]}} independent per-node reduction, in order
 */
const reduceObserved = (observed, nodes) => {
  const counts = new Array(nodes).fill(0)
  const sums = new Array(nodes).fill(0)
  const maxes = new Array(nodes).fill(0)
  for (let row = 0; row < observed.length; row += 1) {
    const node = observed.data[row * 2]
    const duration = observed.data[row * 2 + 1]
    counts[node] += 1
    sums[node] += duration
    if (duration > maxes[node]) maxes[node] = duration
  }
  return { counts, sums, maxes }
}

/** @param {{counts:number[],sums:number[],maxes:number[]}} reduced independent reduction
 * @param {Array<{avg:number,max:number}>|null} pairs collector's per-node pairs, null without a sample
 * @param {number} total observed input count @returns {number} `total` when every node's mean and max
 * equal the independent reduction, else 0 - a bypassed or wrong collector cannot report aggregation
 */
const matchedSamples = (reduced, pairs, total) => {
  if (!pairs || pairs.length !== reduced.counts.length) return 0
  const ok = pairs.every(
    (pair, node) =>
      reduced.counts[node] > 0 &&
      pair.avg === reduced.sums[node] / reduced.counts[node] &&
      pair.max === reduced.maxes[node]
  )
  return ok ? total : 0
}

/** Joins the persisted request ledgers into the evidence `validateRequests` judges.
 * @param {object} config request counts and telemetry arm
 * @param {{writes:Table,responses:Table,returned:Table,observed:Table}} ledgers recorded or decoded rows
 * @param {{physical:string[],successes:number,connectionSum:number,pairs:object[]|null}} meta collector facts
 * @returns {object} evidence for `validateRequests`
 */
const requestEvidence = (config, { writes, responses, returned, observed }, meta) => ({
  count: returned.length,
  expected: expectedTuples(config),
  writes: tupleIds(writes),
  responses: tupleIds(responses),
  returned: tupleIds(returned),
  connected: meta.physical,
  expectedConnections: Array.from({ length: config.nodes }, (_, node) => `${node}.1`),
  sampled: meta.successes,
  observations: observed.length,
  aggregateSamples:
    config.telemetry === 'active'
      ? matchedSamples(reduceObserved(observed, config.nodes), meta.pairs, observed.length)
      : 0,
  connectionSum: meta.connectionSum,
})

/** Transport only: the real Connection frames, queues and parses; this socket answers from a microtask. */
class BenchSocket extends EventEmitter {
  /** @param {number} ordinal issued socket ordinal @param {number} node owning node index
   * @param {(socket:BenchSocket, frame:Buffer)=>void} onWrite outbound frame sink
   */
  constructor(ordinal, node, onWrite) {
    super()
    this.ordinal = ordinal
    this.node = node
    this.destroyed = false
    this.onWrite = onWrite
    this.closed = new Promise((resolve) => this.once('close', resolve))
  }

  /** @param {Buffer} frame full outbound request frame @returns {boolean} accepted */
  write(frame) {
    this.onWrite(this, frame)
    return true
  }

  /** @returns {BenchSocket} this, destroyed */
  end() {
    return this.destroy()
  }

  /** @returns {BenchSocket} this; emits `close` naturally after the destroy */
  destroy() {
    if (!this.destroyed) {
      this.destroyed = true
      queueMicrotask(() => this.emit('close'))
    }
    return this
  }
}

/** Drives one million real Metadata v0 request/response pairs per arm through fake sockets, with the
 * telemetry observer wired exactly as the product wires it (or absent), and judges them by
 * independently recorded socket, connection and collector ledgers.
 */
class RequestWorkload {
  #root
  #config
  #control
  #evidence
  #hooks
  #connections = []
  #sockets = []
  #physical = []
  #closed = false
  #sendOrdinal = 0
  #writes
  #responses
  #returned
  #observed
  #failure = null
  #nodeSockets = []

  /** @param {{root:string,config:{iterations:number,warmup:number,nodes:number,telemetry:string},
   * control:number,evidence:EvidenceWriter,hooks?:object}} options frozen arm root, counts, control flag,
   * sidecar writer and private test hooks (`load`, `Connection`, `memory`, `trace`, `control`, `bypassSink`)
   */
  constructor({ root, config, control, evidence, hooks = {} }) {
    assert(['off', 'empty', 'active'].includes(config.telemetry))
    assert([0, 1].includes(control))
    assert(integer(config.iterations, 1) && integer(config.warmup) && integer(config.nodes, 1))
    assert(config.warmup <= config.iterations)
    assert(control === 0 || config.telemetry === 'active')
    this.#root = root
    this.#config = config
    this.#control = control
    this.#evidence = evidence
    this.#hooks = hooks
  }

  /** @param {BenchSocket} socket sending socket @param {Buffer} frame outbound request frame
   * @returns {void} records the write tuple and answers from a microtask
   * @throws {AssertionError} unless the frame is a Metadata v0 request with the empty topic list
   */
  #wire(socket, frame) {
    try {
      assert.equal(frame.readInt32BE(0), frame.length - 4)
      assert.equal(frame.readInt16BE(4), 3)
      assert.equal(frame.readInt16BE(6), 0)
      const body = frame.subarray(14 + frame.readInt16BE(12))
      assert(body.length === 4 && body.readInt32BE(0) === 0)
    } catch (error) {
      this.#failure = error
      throw error
    }
    const correlation = frame.readInt32BE(8)
    const generation = this.#connections[socket.node].generation
    const send = this.#sendOrdinal++
    this.#writes.add(socket.ordinal, generation, correlation, send)
    if (this.#hooks.trace) this.#hooks.trace('wire', send)
    queueMicrotask(() => {
      if (socket.destroyed) return
      this.#responses.add(socket.ordinal, generation, correlation, send)
      if (this.#hooks.trace) this.#hooks.trace('response', send)
      const reply = Buffer.alloc(16)
      reply.writeInt32BE(12, 0)
      reply.writeInt32BE(correlation, 4)
      socket.emit('data', reply)
    })
  }

  /** @param {number} node node index @param {{onConnect:Function}} options factory arguments
   * @returns {BenchSocket} a new fake socket whose connect is delivered asynchronously
   */
  #socket(node, { onConnect }) {
    const socket = new BenchSocket(this.#sockets.length, node, (s, frame) => this.#wire(s, frame))
    this.#sockets.push(socket)
    queueMicrotask(() => {
      onConnect()
      this.#physical.push(`${socket.ordinal}.${this.#connections[node].generation}`)
    })
    return socket
  }

  /** The private numeric sink: records the original slot and duration, applies the same-path control
   * immediately before the original collector method, and forwards with the original receiver.
   * @param {object} metrics approved collector @param {object[]} slots issued slots, by node
   * @returns {object} sink installed through the normal `setTelemetryObserver`
   */
  #sink(metrics, slots) {
    const index = new Map(slots.map((slot, node) => [slot, node]))
    const control = this.#hooks.control ?? samePathControl
    const enabled = this.#control
    const trace = this.#hooks.trace
    return {
      observeConnection: (token) => metrics.observeConnection(token),
      releaseNode: (slot) => metrics.releaseNode(slot),
      observeRequest: (slot, duration) => {
        this.#observed.add(index.get(slot) ?? -1, duration)
        if (enabled) control('requests', enabled)
        if (trace) trace('observe', duration)
        metrics.observeRequest(slot, duration)
      },
    }
  }

  /** @param {number} count requests @param {boolean} record whether ledgers and success count apply
   * @param {Int32Array} next predicted next correlation per node @param {object} call request, response
   * @returns {Promise<number>} successful parsed responses, in send order
   */
  async #pump(count, record, next, { request, response }) {
    const { nodes } = this.#config
    const trace = this.#hooks.trace
    let successes = 0
    for (let i = 0; i < count; i += 1) {
      const node = i % nodes
      const connection = this.#connections[node]
      const parsed = await connection.send({ request, response })
      const correlation = next[node]++
      if (record) {
        this.#returned.add(this.#nodeSockets[node], connection.generation, correlation, i)
        if (parsed.brokers.length === 0 && parsed.topicMetadata.length === 0) successes += 1
        if (trace) trace('returned', i)
      }
    }
    return successes
  }

  /** @returns {{pairs:object[],connection:number}} the collector's own per-node pairs and connection total
   * @param {object} metrics collector @param {string} telemetry arm label
   */
  #collect(metrics, telemetry) {
    if (telemetry === 'active') {
      const snapshot = metrics.snapshot(REQUEST_SUBSCRIPTION, {})
      const pairs = Array.from({ length: this.#config.nodes }, () => ({ avg: NaN, max: NaN }))
      let connection = 0
      for (const point of snapshot.points) {
        if (point.name.endsWith('.connection.creation.total')) connection = Number(point.value)
        else {
          const node = Number(point.attributes.find(({ key }) => key === 'node_id').value)
          pairs[node][point.name.endsWith('.avg') ? 'avg' : 'max'] = point.value
        }
      }
      metrics.abandon(snapshot)
      return { pairs, connection }
    }
    metrics.select({ revision: 2n, requestedMetrics: ['*'], deltaTemporality: false })
    const snapshot = metrics.snapshot({ revision: 2n }, {})
    const [point] = snapshot.points
    assert.equal(snapshot.points.length, 1)
    metrics.abandon(snapshot)
    return { pairs: null, connection: Number(point.value) }
  }

  /** Runs warmup, the measured window and every independent check.
   * @returns {Promise<object>} scalar result with sealed artifacts; originals parse results stay unchanged
   * @throws {Error} setup failure, an omitted completion, a collector bypass or any failed oracle
   */
  async run() {
    const { iterations, warmup, nodes, telemetry } = this.#config
    const hooks = this.#hooks
    const load = hooks.load ?? require
    const trace = hooks.trace ?? (() => {})
    const probe = hooks.memoryProbe ?? new MemoryProbe(hooks.memory)
    probe.requireGc()
    const join = (...parts) => path.join(this.#root, ...parts)
    const Connection = hooks.Connection ?? load(join('src/network/connection'))
    const { createLogger, LEVELS } = load(join('src/loggers'))
    const request = load(join('src/protocol/requests/metadata/v0/request'))({ topics: [] })
    const response = load(join('src/protocol/requests/metadata/v0/response'))
    this.#writes = new Table(TUPLE, iterations)
    this.#responses = new Table(TUPLE, iterations)
    this.#returned = new Table(TUPLE, iterations)
    this.#observed = new Table(['node', 'duration'], iterations)
    const next = new Int32Array(nodes)
    trace('allocate')
    const before = probe.settled()
    trace('setup')
    let metrics = null
    let slots = []
    let sink = null
    if (telemetry !== 'off') {
      const Metrics = load(join('src/telemetry/metrics'))
      metrics = new Metrics({ kind: 'producer' })
      metrics.select({
        ...REQUEST_SUBSCRIPTION,
        requestedMetrics: telemetry === 'active' ? ['*'] : [],
      })
      slots = Array.from({ length: nodes }, (_, nodeId) =>
        metrics.bindNode({ nodeId, endpoint: `node${nodeId}:9092`, endpointRevision: 1 })
      )
      if (!hooks.bypassSink) sink = this.#sink(metrics, slots)
    }
    const logger = createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} })
    for (let node = 0; node < nodes; node += 1) {
      const connection = new Connection({
        host: `node${node}`,
        port: 9092,
        logger,
        clientId: 'telemetry-benchmark',
        requestTimeout: 30000,
        connectionTimeout: 30000,
        connectionsMaxIdle: 0,
        maxInFlightRequests: 5,
        socketFactory: (options) => this.#socket(node, options),
      })
      if (sink) connection.setTelemetryObserver(sink, slots[node])
      this.#connections.push(connection)
    }
    for (const connection of this.#connections) await connection.connect()
    for (const socket of this.#sockets) this.#nodeSockets[socket.node] = socket.ordinal
    const call = { request, response }
    await this.#pump(warmup, false, next, call)
    this.#assertQuiet()
    if (metrics) {
      const snapshot = metrics.snapshot(REQUEST_SUBSCRIPTION, {})
      assert.equal(snapshot.points.length === 0, telemetry === 'empty')
      metrics.written(
        snapshot,
        snapshot.points.filter(({ type }) => type === 'gauge').map(({ identity }) => identity)
      )
    }
    for (const table of [this.#writes, this.#responses, this.#returned, this.#observed])
      table.reset()
    this.#sendOrdinal = 0
    trace('window-start')
    const cpuStart = process.cpuUsage()
    const start = process.hrtime.bigint()
    const successes = await this.#pump(iterations, true, next, call)
    const elapsedNs = Number(process.hrtime.bigint() - start)
    const cpu = process.cpuUsage(cpuStart)
    trace('window-end')
    this.#assertQuiet()
    const after = probe.settled()
    trace('after')
    const signed = signedDelta(before, after)
    const collected = metrics ? this.#collect(metrics, telemetry) : { pairs: null, connection: 0 }
    const meta = {
      physical: [...this.#physical],
      successes,
      connectionSum: collected.connection,
      pairs: collected.pairs,
    }
    const ledgers = {
      writes: this.#writes,
      responses: this.#responses,
      returned: this.#returned,
      observed: this.#observed,
    }
    const completed = this.#returned.length
    const cpuMicros = cpu.user + cpu.system
    const result = {
      mode: 'requests',
      telemetry,
      control: this.#control,
      completedPairs: completed,
      physicalConnections: meta.physical.length,
      successfulSamples: successes,
      observerCalls: this.#observed.length,
      aggregateSamples: 0,
      cpuMicros,
      elapsedNs,
      cpuNsPerPair: (cpuMicros * 1000) / completed,
      elapsedNsPerPair: elapsedNs / completed,
      heapExternalDeltaBytes: Math.max(0, signed.sum),
      discardedWorkloadWarmups: warmup,
      measuredObservations: completed,
    }
    const evidence = requestEvidence({ iterations, warmup, nodes, telemetry }, ledgers, meta)
    result.aggregateSamples = evidence.aggregateSamples
    validateRequests(result, evidence)
    this.#seal(result, before, after, signed, meta, ledgers)
    return result
  }

  /** @returns {void} @throws {AssertionError} unless every queue and in-flight map is empty */
  #assertQuiet() {
    assert.equal(this.#failure, null)
    for (const { requestQueue } of this.#connections) {
      assert.equal(requestQueue.pending.length, 0)
      assert.equal(requestQueue.inflight.size, 0)
    }
  }

  /** Persists the four fixed-width ledgers, node data and raw memory readings.
   * @param {object} result scalar result @param {object} before reading before the workload
   * @param {object} after reading after the window @param {object} signed signed deltas
   * @param {object} meta collector and connection facts @param {object} ledgers recorded tables
   * @returns {void} @throws {AssertionError} when a byte cap is exceeded
   */
  #seal(result, before, after, signed, meta, ledgers) {
    const { identity } = this.#evidence
    this.#evidence.write('requests-write.bin', ledgers.writes.buffer())
    this.#evidence.write('requests-response.bin', ledgers.responses.buffer())
    this.#evidence.write('requests-returned.bin', ledgers.returned.buffer())
    this.#evidence.write('requests-observe.bin', ledgers.observed.buffer())
    this.#evidence.write(
      'request-meta.json',
      JSON.stringify({ ...meta, telemetry: result.telemetry })
    )
    this.#evidence.write(
      'request-memory.json',
      JSON.stringify({
        schemaVersion: 1,
        runId: identity.runId,
        manifestSha256: identity.manifestSha256,
        before: { heapUsed: before.heapUsed, external: before.external },
        after: { heapUsed: after.heapUsed, external: after.external },
        signedDeltaBytes: signed.sum,
        pairs: result.completedPairs,
        connections: this.#config.nodes,
        collectorLive: this.#config.telemetry !== 'off',
      })
    )
    result.artifacts = this.#evidence.artifacts()
  }

  /** Disconnects every connection without drain, joins the real socket closes and checks for leaks.
   * @returns {Promise<object>} clean counters, all zero on success
   * @throws {Error} when any queue entry, timer or socket survives
   */
  async close() {
    if (this.#closed) return cleanReport()
    this.#closed = true
    await Promise.all(
      this.#connections.map((connection) => connection.disconnect({ drainTimeout: 0 }))
    )
    await Promise.all(this.#sockets.map(({ closed }) => closed))
    const clean = cleanReport()
    for (const connection of this.#connections) {
      clean.pending +=
        connection.requestQueue.pending.length + connection.requestQueue.inflight.size
      if (connection.idleTimer) clean.timers += 1
      if (connection.socket) clean.sockets += 1
    }
    clean.sockets += this.#sockets.filter(({ destroyed }) => !destroyed).length
    assert.deepEqual(clean, cleanReport(), 'request workload leaked resources')
    return clean
  }
}

const PAYLOAD_ROWS = [
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
const COMPRESSION = { none: [], gzip: [1], zstd: [4] }

/** Judges one actual sample against the workload literals: codec choice, framing, every point value,
 * timestamps, temporality, resource and scope. Used by the worker and, from sidecars, by verification.
 * @param {object} config payload dimensions @param {number} k sample ordinal from 1
 * @param {string} scopeVersion collector package version
 * @param {{compressionType:number,codecAttempts:number,rawBytes:number,transmittedBytes:number,
 * fittingPasses:number,metrics:Buffer,includedKeys?:string[],omittedKeys?:string[]}} sample encoded sample
 * @returns {Buffer} the independently inflated raw bytes
 * @throws {AssertionError|CodecUnavailableError} on any difference from the literal expectation
 */
const validatePayloadSample = (config, k, scopeVersion, sample) => {
  const raw = inflate(sample.compressionType, sample.metrics)
  const compressed = config.codec !== 'none' && sample.rawBytes > 1000
  assert.equal(sample.compressionType, compressed ? COMPRESSION[config.codec][0] : 0)
  assert.equal(sample.codecAttempts, compressed ? 1 : 0)
  const expected = expectedSample(config, k, scopeVersion)
  const key = ({ name, attributes }) => JSON.stringify([name, attributes])
  validatePayload(
    {
      rawBytes: sample.rawBytes,
      transmittedBytes: sample.transmittedBytes,
      fittingPasses: sample.fittingPasses,
    },
    {
      raw,
      transmitted: sample.metrics,
      cap: config.telemetryMaxBytes,
      expected: expected.points,
      included: sample.includedKeys ?? expected.points.map(key),
      omitted: sample.omittedKeys ?? [],
      expectedOmitted: [],
    }
  )
  validateFacts(decodeMetricsData(raw), expected)
  return raw
}

/** Serializes 100 fresh legal snapshots per arm through the production builder, with the same-path
 * control at the serializer entry, and checks every sample against independently decoded bytes.
 */
class PayloadWorkload {
  #root
  #config
  #control
  #evidence
  #hooks
  #fixture = null
  #builder = null
  #pending = null
  #streams = []
  #closed = false

  /** @param {{root:string,config:object,control:number,evidence:EvidenceWriter,hooks?:object}} options
   * frozen arm root, payload dimensions, control flag, sidecar writer and private test hooks (`load`,
   * `memory`, `now`, `slice`, `control`, `zstdSupported`, `scopeVersion`, `resourceUsage`)
   */
  constructor({ root, config, control, evidence, hooks = {} }) {
    assert([0, 1].includes(control))
    assert([1, 257].includes(config.points))
    assert(['none', 'gzip', 'zstd'].includes(config.codec))
    assert(integer(config.snapshots, 1) && integer(config.warmup))
    assert(integer(config.telemetryMaxBytes, 1) && config.telemetryMaxBytes <= MIB)
    this.#root = root
    this.#config = config
    this.#control = control
    this.#evidence = evidence
    this.#hooks = hooks
  }

  /** Builds, times and validates one sample; a failure abandons its snapshot exactly once.
   * @param {number} k sample ordinal @param {object} context collaborators and options
   * @returns {Promise<{built:object,cpuMs:number,wallMs:number,sliceMs:number}>} timed, validated sample
   * @throws {Error} serialization or validation failure
   */
  async #sample(k, { accepted, scopeVersion, control, now, monitor }) {
    const snapshot = this.#fixture.next(k)
    this.#pending = snapshot
    const options = {
      telemetryMaxBytes: this.#config.telemetryMaxBytes,
      acceptedCompressionTypes: accepted,
      isCurrent: () => this.#fixture.metrics.isCurrent(snapshot),
    }
    const cpuStart = process.cpuUsage()
    const start = now()
    let built
    let wallNs
    let cpu
    let slices
    monitor.begin()
    try {
      const promise = monitor.call(() => {
        control('payload', this.#control)
        return this.#builder.buildPayload(snapshot, options)
      })
      built = await promise
      wallNs = now() - start
      cpu = process.cpuUsage(cpuStart)
    } catch (error) {
      this.#fixture.metrics.abandon(snapshot)
      this.#pending = null
      throw error
    } finally {
      slices = monitor.end()
    }
    try {
      assert(built !== null, 'serializer returned no payload')
      assert.deepEqual(built.omittedIdentities, [])
      validatePayloadSample(this.#config, k, scopeVersion, {
        ...built,
        includedKeys: built.includedIdentities.map(({ key }) => key),
        omittedKeys: [],
      })
    } catch (error) {
      this.#fixture.metrics.abandon(snapshot)
      this.#pending = null
      throw error
    }
    this.#fixture.metrics.written(snapshot, built.includedIdentities)
    this.#pending = null
    return {
      built,
      cpuMs: (cpu.user + cpu.system) / 1000,
      wallMs: Number(wallNs) / 1e6,
      sliceMs: slices.maxSliceMs,
    }
  }

  /** Runs the warmups and measured samples and seals raw/transmitted buffers and per-sample rows.
   * @returns {Promise<object>} scalar result with sealed artifacts
   * @throws {CodecUnavailableError} when the requested codec is not supported by this runtime
   * @throws {Error} for a missing GC prerequisite, a silent codec fallback or any failed oracle
   */
  async run() {
    const { snapshots, warmup, points, temporality, attributes, codec } = this.#config
    const hooks = this.#hooks
    const load = hooks.load ?? require
    const probe = hooks.memoryProbe ?? new MemoryProbe(hooks.memory)
    probe.requireGc()
    const join = (...parts) => path.join(this.#root, ...parts)
    const Metrics = load(join('src/telemetry/metrics'))
    const Payload = load(join('src/telemetry/payload'))
    const zstdSupported =
      hooks.zstdSupported ?? load(join('src/protocol/message/compression/zstd')).isSupported
    if (codec === 'zstd' && !zstdSupported)
      throw new CodecUnavailableError('zstd is not supported by this runtime')
    const scopeVersion =
      hooks.scopeVersion ?? JSON.parse(fs.readFileSync(join('package.json'), 'utf8')).version
    const before = probe.settled()
    this.#fixture = new PayloadFixture(Metrics, { points, attributes, temporality })
    this.#builder = new Payload({})
    const context = {
      accepted: COMPRESSION[codec],
      scopeVersion,
      control: hooks.control ?? samePathControl,
      now: hooks.now ?? process.hrtime.bigint,
      monitor: new SliceMonitor(hooks.slice),
    }
    const raw = this.#evidence.stream('payload-raw.bin')
    const transmitted = this.#evidence.stream('payload-transmitted.bin')
    this.#streams = [raw, transmitted]
    const rows = new Table(PAYLOAD_ROWS, snapshots)
    let afterWarmup = null
    for (let k = 1; k <= warmup + snapshots; k += 1) {
      const { built, cpuMs, wallMs, sliceMs } = await this.#sample(k, context)
      if (k > warmup) {
        raw.append(inflate(built.compressionType, built.metrics))
        transmitted.append(built.metrics)
        const reading = probe.raw()
        rows.add(
          k - warmup,
          built.rawBytes,
          built.transmittedBytes,
          built.compressionType,
          built.codecAttempts,
          built.fittingPasses,
          cpuMs,
          wallMs,
          sliceMs,
          reading.heapUsed + reading.external
        )
      }
      if (k === warmup) afterWarmup = probe.settled()
    }
    this.#builder.retire()
    const after = probe.settled()
    this.#streams = []
    raw.seal()
    transmitted.seal()
    this.#evidence.write('payload-rows.bin', rows.buffer())
    const signed = signedDelta(before, after)
    const maxRssKiB = (hooks.resourceUsage ?? process.resourceUsage)().maxRSS
    const rssDeltaBytes = rssDelta(maxRssKiB, before.rss)
    const largest = (column) => rows.column(column).reduce((max, value) => Math.max(max, value), 0)
    const { identity } = this.#evidence
    this.#evidence.write(
      'payload-meta.json',
      JSON.stringify({
        schemaVersion: 1,
        runId: identity.runId,
        manifestSha256: identity.manifestSha256,
        scopeVersion,
        offeredCodec: codec,
        zstdSupported: Boolean(zstdSupported),
        hookEnabled: true,
      })
    )
    this.#evidence.write(
      'payload-memory.json',
      JSON.stringify({
        schemaVersion: 1,
        before,
        afterWarmup,
        after,
        signed,
        maxRssKiB,
        maxRssBytes: maxRssKiB * 1024,
        rssBaselineBytes: before.rss,
        rssDeltaBytes,
      })
    )
    this.#fixture = null
    return {
      mode: 'payload',
      control: this.#control,
      rawBytes: largest('rawBytes'),
      transmittedBytes: largest('transmittedBytes'),
      fittingPasses: largest('fittingPasses'),
      sliceP99Ms: p99(rows.column('sliceMs')),
      cpuP99Ms: p99(rows.column('cpuMs')),
      wallP99Ms: p99(rows.column('wallMs')),
      heapExternalDeltaBytes: Math.max(0, signed.sum),
      rssDeltaBytes,
      codecAvailable: { none: true, gzip: true, zstd: Boolean(zstdSupported) },
      discardedWorkloadWarmups: warmup,
      measuredObservations: snapshots,
      artifacts: this.#evidence.artifacts(),
    }
  }

  /** Retires the builder, abandons an unsent snapshot and seals any stream a failed run left open.
   * @returns {Promise<object>} clean counters, all zero
   */
  async close() {
    if (this.#closed) return cleanReport()
    this.#closed = true
    this.#builder?.retire()
    if (this.#pending) this.#fixture.metrics.abandon(this.#pending)
    this.#pending = null
    for (const stream of this.#streams) {
      try {
        stream.seal()
      } catch {}
    }
    this.#streams = []
    return cleanReport()
  }
}

const RECORD_BYTES = 200
const DELIVERY_SIZE = RECORD_BYTES
const CREDIT_SENDS = 5
const CREDIT_RECORDS = 5000
const SEND_BATCH = 1000
const COMMIT_EVERY = 1000
const MINIMUM_GAPS = { eachMessage: 200000, eachBatch: 1000 }
const SEND_COLUMNS = ['ordinal', 'sendMs', 'hash']
const HANDLER_COLUMNS = ['ordinal', 'partition', 'offset', 'hash', 'entryMs', 'callback', 'bytes']
const CALLBACK_COLUMNS = ['entryMs', 'size', 'firstOrdinal']
const DURABLE_COLUMNS = ['ordinal', 'partition', 'offset', 'hash', 'tailOk']

/** @param {number} ordinal record ordinal @param {number} at byte offset @returns {number} tail byte */
const tailByte = (ordinal, at) => (ordinal * 31 + at * 17) & 255

/** @param {number} ordinal record ordinal @param {number} sendMs producer send-entry monotonic milliseconds
 * @returns {Buffer} the 200 byte value: ordinal uint64, send time double, 184 derived bytes
 */
const makeValue = (ordinal, sendMs) => {
  const value = Buffer.alloc(RECORD_BYTES)
  value.writeBigUInt64LE(BigInt(ordinal), 0)
  value.writeDoubleLE(sendMs, 8)
  for (let at = 16; at < RECORD_BYTES; at += 1) value[at] = tailByte(ordinal, at)
  return value
}

/** @param {Buffer} value record value @param {number} ordinal expected ordinal
 * @returns {boolean} whether size, embedded ordinal and the derived tail are intact (send time is free)
 */
const tailIntact = (value, ordinal) => {
  if (value.length !== RECORD_BYTES || value.readBigUInt64LE(0) !== BigInt(ordinal)) return false
  for (let at = 16; at < RECORD_BYTES; at += 1)
    if (value[at] !== tailByte(ordinal, at)) return false
  return true
}

/** @param {Buffer} value bytes @returns {number} FNV-1a 32 bit hash of the whole timestamped payload */
const valueHash = (value) => {
  let hash = 0x811c9dc5
  for (let i = 0; i < value.length; i += 1) hash = Math.imul(hash ^ value[i], 16777619)
  return hash >>> 0
}

/** @param {string} namespace `w` warmup or `m` measured @param {number} ordinal record ordinal
 * @returns {string} the unique record key; warmup can never satisfy a measured identity
 */
const keyOf = (namespace, ordinal) => `${namespace}-${ordinal}`

/** @param {{handler:string,active:boolean,groupId:string,partitions:number,push:boolean}} options arm
 * @returns {{kafka:object,producer:object,consumer:object,run:object}} the only public options the
 * measured clients may receive; `enableMetricsPush` derives from the arm alone and `autoCommit` belongs to run
 */
const publicOptions = ({ handler, active, groupId, partitions, push = true }) => {
  const opt = push ? { enableMetricsPush: active } : {}
  const run = { autoCommit: false, partitionsConsumedConcurrently: partitions }
  return {
    kafka: { ...opt },
    producer: { idempotent: true, ...opt },
    consumer: {
      groupId,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
      maxWaitTimeInMs: 500,
      readUncommitted: false,
      maxBytesPerPartition: 65536,
      ...opt,
    },
    run:
      handler === 'eachMessage'
        ? { ...run, eachMessage: 'eachMessage' }
        : { ...run, eachBatchAutoResolve: false, eachBatch: 'eachBatch' },
  }
}

/** @param {object} captured options the clients actually received; handler functions appear by kind
 * @param {object} expected `publicOptions` output @returns {void}
 * @throws {AssertionError} for any extra, missing or different public option (e.g. a `telemetry` key)
 */
const assertPublicOptions = (captured, expected) => {
  const kind = (run) =>
    Object.fromEntries(
      Object.entries(run).map(([name, value]) => [name, typeof value === 'function' ? name : value])
    )
  assert.deepEqual(captured.producer, expected.producer)
  assert.deepEqual(captured.consumer, expected.consumer)
  assert.deepEqual(kind(captured.run), expected.run)
  assert.deepEqual(captured.kafka, expected.kafka)
}

/** @param {Buffer} buffer bytes @param {number} at start @returns {[number,number]} unsigned varint and next offset */
const readUVarInt = (buffer, at) => {
  let value = 0
  for (let shift = 0; shift < 35; shift += 7) {
    assert(at < buffer.length)
    const byte = buffer[at++]
    value += (byte & 127) * 2 ** shift
    if (!(byte & 128)) return [value, at]
  }
  return assert.fail('invalid unsigned varint')
}

/** @param {Buffer} uuid 16 bytes @returns {string} canonical lowercase UUID */
const uuidText = (uuid) => {
  const hex = uuid.toString('hex')
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}

/** Splits one direction of a byte stream into length-prefixed frames without buffering ordinary bodies:
 * only a frame whose head the `keeps` predicate accepts is retained (bounded by `maxFrame`).
 */
class FrameStream {
  #prefix = Buffer.alloc(4)
  #have = 0
  #length = -1
  #seen = 0
  #head = null
  #keep = null

  /** @param {{headBytes:number,maxFrame:number,keeps:(head:Buffer)=>boolean,onFrame:(body:Buffer)=>void}} o */
  constructor({ headBytes, maxFrame, keeps, onFrame }) {
    this.headBytes = headBytes
    this.maxFrame = maxFrame
    this.keeps = keeps
    this.onFrame = onFrame
  }

  /** @returns {void} decides retention once the head is complete */
  #decide() {
    if (!this.keeps(this.#head)) return
    assert(this.#length <= this.maxFrame, 'telemetry frame exceeds the bound')
    this.#keep = Buffer.alloc(this.#length)
    this.#head.copy(this.#keep)
  }

  /** @returns {void} hands a retained frame on and starts the next prefix */
  #finish() {
    const keep = this.#keep
    this.#length = -1
    this.#keep = null
    if (keep) this.onFrame(keep)
  }

  /** @param {Buffer} chunk any fragment of the stream @returns {void}
   * @throws {AssertionError} for a negative length or an oversized retained frame
   */
  push(chunk) {
    let at = 0
    while (at < chunk.length) {
      if (this.#length < 0) {
        const take = Math.min(4 - this.#have, chunk.length - at)
        chunk.copy(this.#prefix, this.#have, at, at + take)
        this.#have += take
        at += take
        if (this.#have < 4) return
        this.#have = 0
        this.#length = this.#prefix.readInt32BE(0)
        assert(this.#length >= 0, 'negative frame length')
        this.#seen = 0
        this.#head = Buffer.alloc(Math.min(this.#length, this.headBytes))
        if (this.#head.length === 0) {
          this.#decide()
          if (this.#length === 0) this.#finish()
        }
        continue
      }
      let rest = chunk.subarray(at, at + Math.min(this.#length - this.#seen, chunk.length - at))
      at += rest.length
      if (this.#seen < this.#head.length) {
        const taken = Math.min(this.#head.length - this.#seen, rest.length)
        rest.copy(this.#head, this.#seen, 0, taken)
        this.#seen += taken
        rest = rest.subarray(taken)
        if (this.#seen === this.#head.length) this.#decide()
      }
      if (rest.length) {
        if (this.#keep) rest.copy(this.#keep, this.#seen)
        this.#seen += rest.length
      }
      if (this.#seen === this.#length) this.#finish()
    }
  }
}

/** Independently parses the complete flexible PushTelemetry v0 body and decodes its numeric OTLP bytes.
 * Delivery uses the retained reporter's 4096-byte raw-payload domain; unknown codecs or tags fail closed.
 * @param {Buffer} body actual 72v0 body @returns {object} raw/transmitted hashes and full decoded values
 * @throws {AssertionError|Error} for framing, codec, size or OTLP errors
 */
const decodePush = (body) => {
  assert(body.length >= 24)
  assert(body[20] === 0 || body[20] === 1)
  const compressionType = body.readInt8(21)
  const [length, at] = readUVarInt(body, 22)
  const metricsBytes = length - 1
  assert(integer(metricsBytes) && metricsBytes <= 4096)
  assert.equal(at + metricsBytes + 1, body.length)
  assert.equal(body[body.length - 1], 0, 'unsupported Push tags')
  const transmitted = body.subarray(at, at + metricsBytes)
  let raw
  if (compressionType === 0) raw = transmitted
  else if (compressionType === 1) raw = zlib.gunzipSync(transmitted, { maxOutputLength: 4096 })
  else {
    assert.equal(compressionType, 4)
    // eslint-disable-next-line n/no-unsupported-features/node-builtins -- feature detection
    assert.equal(typeof zlib.zstdDecompressSync, 'function')
    // eslint-disable-next-line n/no-unsupported-features/node-builtins -- feature detection
    raw = zlib.zstdDecompressSync(transmitted, { maxOutputLength: 4096 })
  }
  assert(raw.length <= 4096)
  return {
    uuid: uuidText(body.subarray(0, 16)),
    subscriptionId: body.readInt32BE(16),
    terminating: body[20] === 1,
    compressionType,
    metricsBytes,
    bodyHex: body.toString('hex'),
    bodySha256: sha256(body),
    transmittedSha256: sha256(transmitted),
    rawHex: raw.toString('hex'),
    rawBytes: raw.length,
    rawSha256: sha256(raw),
    decoded: raw.length ? decodeMetricsData(raw) : null,
  }
}

/** Decodes a retained Push outside timing, checking any previously derived fields on re-verification.
 * Capture records contain only body bytes and socket facts; decoded fields are never needed to forward.
 * @param {object} record outbound Push or completed request/response tuple
 * @returns {object} independently decoded record
 * @throws {AssertionError|Error} for corrupt bytes, payload decoding or inconsistent derived fields
 */
const decodePushRecord = (record) => {
  if (record.kind !== 'push') return record
  assert(typeof record.bodyHex === 'string' && /^(?:[a-f0-9]{2})+$/.test(record.bodyHex))
  const actual = decodePush(Buffer.from(record.bodyHex, 'hex'))
  for (const key of Object.keys(actual))
    if (Object.hasOwn(record, key))
      assert.deepEqual(record[key], actual[key], `Push ${key} differs`)
  return { ...record, ...actual }
}

/** One socket's two directions: archives outbound Pushes before pairing telemetry requests (APIs 71
 * and 72) with their responses. Correlations retire when answered; ordinary frame bodies are discarded.
 */
class SocketTap {
  #pending = new Map()
  #out
  #in

  /** @param {{emit:(record:object)=>void,fail:(error:Error)=>void,socket:number,node:string,now:Function,maxPending:number,maxFrame:number}} o */
  constructor({ emit, fail, socket, node, now, maxPending, maxFrame }) {
    const guard = (work) => (frame) => {
      try {
        work(frame)
      } catch (error) {
        fail(error)
      }
    }
    this.#out = new FrameStream({
      headBytes: 2,
      maxFrame,
      keeps: (head) => head.length === 2 && [71, 72].includes(head.readInt16BE(0)),
      onFrame: guard((frame) => {
        assert(this.#pending.size < maxPending, 'too many pending telemetry correlations')
        assert.equal(frame.readInt16BE(2), 0)
        const apiKey = frame.readInt16BE(0)
        const correlation = frame.readInt32BE(4)
        const [tags, afterTags] = readUVarInt(frame, 10 + Math.max(0, frame.readInt16BE(8)))
        assert.equal(tags, 0, 'unsupported request header tags')
        const body = frame.subarray(afterTags)
        const request = {
          kind: apiKey === 71 ? 'subscription' : 'push',
          socket,
          node,
          apiKey,
          correlation,
        }
        if (apiKey === 72) request.bodyHex = body.toString('hex')
        else request.uuid = uuidText(body.subarray(0, 16))
        this.#pending.set(correlation, request)
        if (apiKey === 72) emit({ ...request, observedMs: now() })
      }),
    })
    this.#in = new FrameStream({
      headBytes: 4,
      maxFrame,
      keeps: (head) => head.length === 4 && this.#pending.has(head.readInt32BE(0)),
      onFrame: guard((frame) => {
        const correlation = frame.readInt32BE(0)
        const request = this.#pending.get(correlation)
        this.#pending.delete(correlation)
        assert.equal(frame[4], 0, 'unsupported response header tags')
        const body = frame.subarray(5)
        const record = { ...request, errorCode: body.readInt16BE(4), observedMs: now() }
        if (request.apiKey === 71 && record.errorCode === 0) {
          record.uuid = uuidText(body.subarray(6, 22))
          record.subscriptionId = body.readInt32BE(22)
          const [types, afterTypes] = readUVarInt(body, 26)
          const at = afterTypes + Math.max(0, types - 1)
          record.pushIntervalMs = body.readInt32BE(at)
          record.telemetryMaxBytes = body.readInt32BE(at + 4)
        }
        emit(record)
      }),
    })
  }

  /** @param {Buffer} chunk outbound bytes @returns {void} */
  outbound(chunk) {
    this.#out.push(chunk)
  }

  /** @param {Buffer} chunk inbound bytes @returns {void} */
  inbound(chunk) {
    this.#in.push(chunk)
  }

  /** @returns {number} correlations still waiting for their response */
  get pending() {
    return this.#pending.size
  }
}

/** Passive, bounded tap of the measured clients' sockets for GetTelemetrySubscriptions and PushTelemetry.
 * Errors latch locally and never alter the socket's own write or data behaviour.
 */
class TelemetryPushProbe {
  #evidence
  #maxSockets
  #maxPending
  #maxFrame
  #maxRecords
  #now
  #open = 0
  #sockets = 0
  #lines = []
  #bytes = 0
  #records = []
  #errors = []
  #finished = false

  /** @param {{evidence:EvidenceWriter,maxSockets?:number,maxPending?:number,maxFrame?:number,
   * maxRecords?:number,now?:Function}} options
   * bounds: 64 concurrent sockets, 128 pending correlations per socket, 1 MiB retained frame
   */
  constructor({
    evidence,
    maxSockets = 64,
    maxPending = 128,
    maxFrame = MIB,
    maxRecords = 256,
    now = () => performance.now(),
  }) {
    this.#evidence = evidence
    this.#maxSockets = maxSockets
    this.#maxPending = maxPending
    this.#maxFrame = maxFrame
    assert(integer(maxRecords, 1))
    this.#maxRecords = maxRecords
    this.#now = now
  }

  /** @param {Error} error problem to retain @returns {void} latched; the validation fails later */
  #fail(error) {
    if (this.#errors.length < 16) this.#errors.push(error.message)
  }

  /** @returns {void} appends the pending lines as one sealed chunk of at most 2 MiB */
  #flush() {
    if (!this.#lines.length) return
    try {
      this.#evidence.chunk('delivery-telemetry', Buffer.from(this.#lines.join('')), {
        deferHash: true,
      })
    } catch (error) {
      this.#fail(error)
    }
    this.#lines = []
    this.#bytes = 0
  }

  /** @param {object} record outbound Push or completed telemetry request/response tuple @returns {void} */
  #emit(record) {
    assert(!this.#finished, 'probe already sealed')
    if (this.#records.length === this.#maxRecords) this.#records.shift()
    this.#records.push(record)
    const line = `${JSON.stringify(record)}\n`
    if (this.#bytes + line.length > MIB) this.#flush()
    this.#lines.push(line)
    this.#bytes += line.length
  }

  /** @param {string} [node] independently discovered broker id @returns {SocketTap} a tap for one socket; exceeding the concurrent socket bound latches an error */
  open(node = 'unknown') {
    this.#open += 1
    if (this.#open > this.#maxSockets)
      this.#fail(new Error('too many concurrent telemetry sockets'))
    const tap = new SocketTap({
      emit: (record) => this.#emit(record),
      fail: (error) => this.#fail(error),
      socket: this.#sockets++,
      node: String(node),
      now: this.#now,
      maxPending: this.#maxPending,
      maxFrame: this.#maxFrame,
    })
    return Object.assign(tap, { release: () => (this.#open -= 1) })
  }

  /** @param {Function} factory a public `socketFactory` @param {Function} [nodeFor] endpoint-to-node lookup
   * @returns {Function} the same factory, tapped
   * (the socket's own `write` and `data` behaviour is preserved, probe errors latch)
   */
  wrap(factory, nodeFor = () => 'unknown') {
    return (options) => {
      const node = nodeFor(options)
      const socket = factory(options)
      const tap = this.open(node)
      const write = socket.write.bind(socket)
      socket.write = (chunk, ...rest) => {
        try {
          if (Buffer.isBuffer(chunk)) tap.outbound(chunk)
        } catch (error) {
          this.#fail(error)
        }
        return write(chunk, ...rest)
      }
      socket.on('data', (chunk) => {
        try {
          tap.inbound(chunk)
        } catch (error) {
          this.#fail(error)
        }
      })
      socket.on('close', () => tap.release())
      return socket
    }
  }

  /** @returns {object[]} bounded recent outbound Pushes and completed tuples; the full stream is on disk */
  get records() {
    return this.#records
  }

  /** @returns {string[]} latched probe errors */
  get errors() {
    return this.#errors
  }

  /** Seals capture and independently decodes all outbound and answered Push payloads after the measured
   * window, including requests unanswered when their socket closed. Later records are refused.
   * No decoder or payload hash runs on the forwarding path.
   * @returns {object[]} every decoded record @throws {AssertionError|Error} on latched or decoding errors
   */
  finish() {
    if (!this.#finished) {
      this.#finished = true
      this.#flush()
      this.#evidence.sealChunks('delivery-telemetry')
    }
    assert.deepEqual(this.#errors, [], 'telemetry probe latched an error')
    return readChunks(
      this.#evidence.directory,
      this.#evidence.artifacts('delivery-telemetry'),
      'delivery-telemetry'
    ).flatMap((chunk) =>
      chunk
        .toString('utf8')
        .split('\n')
        .filter(Boolean)
        .map((line) => decodePushRecord(JSON.parse(line)))
    )
  }
}

/** Decides whether the wire records and exporter records prove the arm's telemetry state.
 * Outbound-only Pushes are decoded but lack response success and cannot prove active clients.
 * @param {object[]} records outbound Pushes and completed 71/72 tuples
 * @param {{active:boolean,intervalMs:number,
 * exports:object[],clients?:number,window?:{startMs:number,endMs:number}}} options expected state; exporter records carry `clientInstanceId`
 * and `pushIntervalMs` @returns {{clients:number,pushes:number}} what was proven
 * @throws {AssertionError} for any telemetry traffic when off, or for an active arm without an accepted
 * subscription, a successful push response and a matching exporter record per measured client
 */
const telemetryEvidence = (records, { active, intervalMs, exports, clients = 2, window }) => {
  if (!active) {
    assert.equal(records.length, 0, 'opt-out client sent telemetry frames')
    return { clients: 0, pushes: 0 }
  }
  assert(
    Number.isFinite(window?.startMs) &&
      Number.isFinite(window?.endMs) &&
      window.endMs >= window.startMs,
    'missing measured Push window'
  )
  const cursors = {}
  for (const row of exports) {
    const node = String(row.node)
    assert(['1', '2', '3'].includes(node))
    assert.equal(row.seq, (cursors[node] ?? 0) + 1, 'archived exporter sequence has a gap')
    cursors[node] = row.seq
    assert(
      row.v === 1 &&
        row.parse === 'ok' &&
        !row.stub &&
        integer(row.dataBytes) &&
        /^[a-f0-9]{64}$/.test(row.dataSha256),
      'invalid archived exporter record'
    )
  }
  for (const record of records) {
    assert(Number.isFinite(record.observedMs) && integer(record.socket))
    assert(['1', '2', '3'].includes(record.node), 'missing Push broker-node identity')
  }
  const subscriptions = records.filter(
    (r) => r.kind === 'subscription' && r.errorCode === 0 && r.pushIntervalMs === intervalMs
  )
  const ids = new Set(subscriptions.map(({ uuid }) => uuid))
  assert.equal(ids.size, clients, 'measured clients did not obtain distinct accepted subscriptions')
  const consumed = new Set()
  const proven = new Set()
  let pushes = 0
  for (const captured of records.filter((r) => r.kind === 'push')) {
    const record = decodePushRecord(captured)
    if (record.errorCode !== 0) continue
    assert(
      subscriptions.some(
        (s) =>
          s.uuid === record.uuid &&
          s.node === record.node &&
          s.socket === record.socket &&
          s.subscriptionId === record.subscriptionId &&
          s.observedMs <= record.observedMs
      ),
      'Push lacks a matching accepted subscription'
    )
    if (record.rawBytes === 0) continue
    const matching = exports.find(
      (e) =>
        e.clientInstanceId === record.uuid &&
        e.pushIntervalMs === intervalMs &&
        String(e.node) === record.node &&
        e.dataSha256 === record.rawSha256 &&
        e.dataBytes === record.rawBytes &&
        e.terminating === record.terminating &&
        e.parse === 'ok' &&
        !e.stub &&
        integer(e.seq, 1) &&
        !consumed.has(`${e.node}:${e.seq}`)
    )
    assert(matching, 'no exporter record matches the actual Push payload/node')
    consumed.add(`${matching.node}:${matching.seq}`)
    if (
      !record.terminating &&
      record.observedMs >= window.startMs &&
      record.observedMs <= window.endMs
    ) {
      proven.add(record.uuid)
      pushes += 1
    }
  }
  exactSet([...ids], [...proven])
  return { clients: proven.size, pushes }
}

/** Polls the bounded sink with one cursor per broker and archives full rows as immutable chunks.
 * Polling never clears or rotates an active sink. Overflow, gaps and poll failures remain fatal.
 */
class ExportArchive {
  #evidence
  #read
  #timer = null
  #pending = null
  #error = null
  #after = {}
  #lines = ''
  #bytes = 0

  /** @param {EvidenceWriter} evidence owned writer @param {Function} read per-node cursor reader */
  constructor(evidence, read) {
    this.#evidence = evidence
    this.#read = read
  }

  /** @returns {Promise<void>} one serialized poll; repeated requests share the current read */
  poll() {
    if (this.#pending) return this.#pending
    this.#pending = this.#poll()
      .catch((error) => {
        this.#error ??= error
      })
      .finally(() => {
        this.#pending = null
      })
    return this.#pending
  }

  /** @returns {Promise<void>} validates node sequences before sealing the newly observed rows */
  async #poll() {
    const rows = await this.#read({ after: { ...this.#after } })
    for (const row of rows) {
      const node = String(row.node)
      assert(['1', '2', '3'].includes(node))
      assert.equal(row.seq, (this.#after[node] ?? 0) + 1, 'export cursor has a gap')
      this.#after[node] = row.seq
      const line = `${JSON.stringify(row)}\n`
      assert(Buffer.byteLength(line) <= 4096)
      if (this.#bytes + Buffer.byteLength(line) > 65536) this.#flush()
      this.#lines += line
      this.#bytes += Buffer.byteLength(line)
    }
  }

  /** @returns {void} seals at most 64 KiB of pending exporter rows */
  #flush() {
    if (!this.#lines) return
    this.#evidence.chunk('delivery-exports', Buffer.from(this.#lines), { deferHash: true })
    this.#lines = ''
    this.#bytes = 0
  }

  /** @returns {Promise<void>} initial poll before the first measured client connects */
  async start() {
    await this.poll()
    this.check()
    this.#timer = setInterval(() => {
      this.poll()
    }, 250)
  }

  /** @returns {void} throws the first retained poll failure @throws {Error} failed sink evidence */
  check() {
    if (this.#error) throw this.#error
  }

  /** @returns {Promise<void>} stops polling and joins the outstanding read on every exit path */
  async stop() {
    if (this.#timer) clearInterval(this.#timer)
    this.#timer = null
    if (this.#pending) await this.#pending
  }

  /** @returns {Promise<object[]>} final archived sink rows @throws {Error} any latched failure */
  async finish() {
    await this.stop()
    await this.poll()
    this.check()
    this.#flush()
    this.#evidence.sealChunks('delivery-exports')
    return readChunks(
      this.#evidence.directory,
      this.#evidence.artifacts('delivery-exports'),
      'delivery-exports'
    ).flatMap((chunk) =>
      chunk
        .toString('utf8')
        .split('\n')
        .filter(Boolean)
        .map((line) => JSON.parse(line))
    )
  }
}

/** Drives the public producer and an ordinary consumer through a prepared fixture and judges delivery by
 * three independent outcome sets: durable log, raw broker commits and handler invocations.
 */
class DeliveryWorkload {
  #config
  #control
  #evidence
  #hooks
  #fixture
  #clients = []
  #credit = null
  #closed = false
  #state
  #main = null
  #exports = null

  /** @param {{root:string,config:object,control:number,evidence:EvidenceWriter,fixture:object,
   * hooks?:object}} options frozen arm root, delivery counts (`minimumGaps` may lower the gap
   * requirement for class-level tests), control flag, sidecar writer, a prepared fixture (`kafka`,
   * `provision`, `topic`, `groupId`, `auditGroupId`, `clientId`, `now`, `wait`, `exports`) and test hooks
   */
  constructor({ config, control, evidence, fixture, hooks = {} }) {
    assert([0, 1].includes(control))
    assert(['eachMessage', 'eachBatch'].includes(config.handler))
    assert(['off', 'active'].includes(config.telemetry))
    assert(integer(config.messages, 1) && integer(config.warmup) && config.size === DELIVERY_SIZE)
    this.#config = config
    this.#control = control
    this.#evidence = evidence
    this.#fixture = fixture
    this.#hooks = hooks
  }

  /** Starts one completion phase: `state.finish` resolves it, `state.fail` rejects it with the handler's own
   * error. The rejection is observed immediately so an early failure cannot become unhandled.
   * @returns {Promise<void>} settles when the phase finishes or a handler fails
   */
  #phase() {
    const done = new Promise((resolve, reject) => {
      this.#state.finish = resolve
      this.#state.fail = reject
    })
    done.catch(() => {})
    return done
  }

  /** @param {Promise<void>} promise phase or read to wait for @returns {Promise<void>} its outcome
   * @throws {Error} the phase's own failure, or `delivery timed out` after the configured timeout; the
   * timer is always cleared
   */
  async #within(promise) {
    let timer
    const deadline = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error('delivery timed out')), this.#config.timeout)
    })
    try {
      await Promise.race([promise, deadline])
    } finally {
      clearTimeout(timer)
    }
  }

  /** @returns {void} releases the single waiting producer credit, if any */
  #wake() {
    const wake = this.#credit
    this.#credit = null
    if (wake) wake()
  }

  /** Sends `count` records of one namespace in batches of 1000 under the fixed credit rule: at most
   * 5 outstanding sends and 5000 unhandled records.
   * @param {Function} producerSend bound `producer.send` @param {string} namespace `w` or `m`
   * @param {number} count records @param {Table|null} ledger send ledger for measured records
   * @returns {Promise<void>} after every send settled @throws {Error} the first failed send
   */
  async #produce(producerSend, namespace, count, ledger) {
    const { partitions } = this.#config
    const { topic, now } = this.#fixture
    const sends = new Set()
    let failure = null
    const failed = () => failure !== null
    const handledAtStart = this.#state.handled
    let sent = 0
    for (let first = 0; first < count && !failed(); first += SEND_BATCH) {
      while (
        !failed() &&
        (sends.size >= CREDIT_SENDS ||
          sent - (this.#state.handled - handledAtStart) >= CREDIT_RECORDS)
      )
        await new Promise((resolve) => (this.#credit = resolve))
      const size = Math.min(SEND_BATCH, count - first)
      const sendMs = now()
      const messages = Array.from({ length: size }, (_, i) => {
        const ordinal = first + i
        const value = makeValue(ordinal, sendMs)
        if (ledger) ledger.add(ordinal, sendMs, valueHash(value))
        return { key: keyOf(namespace, ordinal), value, partition: ordinal % partitions }
      })
      sent += size
      const pending = producerSend({ topic, acks: -1, messages }).then(
        () => {},
        (error) => {
          failure = error
        }
      )
      sends.add(pending)
      pending.then(() => {
        sends.delete(pending)
        this.#wake()
      })
    }
    await Promise.all(sends)
    if (failure) throw failure
  }

  /** Builds the two handlers; entry time is read first, then the same-path control is awaited, then the
   * record is processed - the original handler path is otherwise identical in every arm.
   * @param {Function} commit `(partition, nextOffset) => Promise` public commit of one partition
   * @param {Function} commitAll `() => Promise` public commit of every accumulated processed frontier
   * @returns {{eachMessage:Function,eachBatch:Function}} handlers keyed by name
   */
  #handlers(commit, commitAll) {
    const state = this.#state
    const control = this.#hooks.control ?? samePathControl
    const { now } = this.#fixture
    const guard = (handler) => async (input) => {
      try {
        return await handler(input)
      } catch (error) {
        state.fail(error)
        throw error
      }
    }
    const record = (partition, message, entryMs, callback) => {
      const ordinal = Number(message.key.toString().slice(2))
      const offset = Number(message.offset)
      state.next[partition] = offset + 1
      state.handled += 1
      if (message.key.toString().startsWith('w')) return
      state.table.add(
        ordinal,
        partition,
        offset,
        valueHash(message.value),
        entryMs,
        callback,
        message.value.length
      )
      state.lastDone = now()
    }
    return {
      eachMessage: guard(async ({ partition, message }) => {
        const entryMs = now()
        await control('delivery', this.#control)
        const measured = message.key.toString().startsWith('m')
        if (measured) state.callbacks.add(entryMs, 1, Number(message.key.toString().slice(2)))
        record(partition, message, entryMs, state.callbacks.length - 1)
        if (measured && (state.table.length % COMMIT_EVERY === 0 || state.handled === state.total))
          await commitAll()
        this.#wake()
        if (state.handled === state.total) state.finish()
      }),
      eachBatch: guard(async ({ batch, resolveOffset }) => {
        const entryMs = now()
        await control('delivery', this.#control)
        const measured =
          batch.messages.length > 0 && batch.messages[0].key.toString().startsWith('m')
        if (measured)
          state.callbacks.add(
            entryMs,
            batch.messages.length,
            Number(batch.messages[0].key.toString().slice(2))
          )
        for (const message of batch.messages) {
          record(batch.partition, message, entryMs, state.callbacks.length - 1)
          resolveOffset(message.offset)
        }
        await commit(batch.partition, state.next[batch.partition])
        this.#wake()
        if (state.handled === state.total) state.finish()
      }),
    }
  }

  /** Reads the topic from the beginning with an ordinary audit consumer in a disjoint group and tallies
   * only the measured namespace; scans exactly the captured end offsets.
   * @param {object} kafka telemetry-disabled helper client @param {Array<{partition:number,offset:string}>} ends
   * @returns {Promise<Table>} durable ledger @throws {Error} on a failed read or a missing end
   */
  async #audit(kafka, ends) {
    const { topic, auditGroupId } = this.#fixture
    const consumer = kafka.consumer({ groupId: auditGroupId })
    this.#clients.push(consumer)
    const durable = new Table(DURABLE_COLUMNS, this.#config.messages)
    const remaining = new Map(
      ends.filter(({ offset }) => Number(offset) > 0).map((e) => [e.partition, Number(e.offset)])
    )
    let finish
    let fail
    const done = new Promise((resolve, reject) => {
      finish = resolve
      fail = reject
    })
    done.catch(() => {})
    if (remaining.size === 0) finish()
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ partition, message }) => {
        try {
          const key = message.key.toString()
          if (key.startsWith('m')) {
            const ordinal = Number(key.slice(2))
            durable.add(
              ordinal,
              partition,
              Number(message.offset),
              valueHash(message.value),
              tailIntact(message.value, ordinal) ? 1 : 0
            )
          }
          if (Number(message.offset) + 1 >= remaining.get(partition)) remaining.delete(partition)
          if (remaining.size === 0) finish()
        } catch (error) {
          fail(error)
          throw error
        }
      },
    })
    await this.#within(done)
    return durable
  }

  /** Runs setup, warmup, the measured window and every independent oracle.
   * @returns {Promise<object>} scalar result with sealed artifacts
   * @throws {Error|AggregateError} setup, work, audit or cleanup failure with the primary error kept first
   */
  async run() {
    const { partitions, handler, telemetry, interval = 1000 } = this.#config
    const fixture = this.#fixture
    const active = telemetry === 'active'
    const probe = new TelemetryPushProbe({ evidence: this.#evidence, now: () => fixture.now() })
    const expectedOptions = publicOptions({
      handler,
      active,
      groupId: fixture.groupId,
      partitions,
      push: fixture.supportsMetricsPush !== false,
    })
    const helper = fixture.kafka({
      clientId: `${fixture.clientId}-helper`,
      enableMetricsPush: false,
    })
    const measuredOptions = {
      clientId: `${fixture.clientId}-measured`,
      ...expectedOptions.kafka,
      ...(fixture.socketFactory
        ? { socketFactory: probe.wrap(fixture.socketFactory(), fixture.nodeFor) }
        : {}),
    }
    let result
    const admin = helper.admin()
    this.#clients.push(admin)
    await admin.connect()
    try {
      await fixture.provision(admin, async () => {
        if (active) {
          assert(fixture.exports, 'active fixture must provide the retained sink reader')
          this.#exports = new ExportArchive(this.#evidence, (options) => fixture.exports(options))
          await this.#exports.start()
        }
        result = await this.#measure({
          helper,
          admin,
          probe,
          expectedOptions,
          measuredOptions,
          interval,
        })
      })
    } finally {
      if (this.#exports) await this.#exports.stop()
    }
    return result
  }

  /** @param {object} context clients and options prepared by `run` @returns {Promise<object>} result */
  async #measure({ helper, admin, probe, expectedOptions, measuredOptions, interval }) {
    const { messages, warmup, partitions, handler, telemetry, size } = this.#config
    const fixture = this.#fixture
    const { topic, now } = fixture
    const active = telemetry === 'active'
    const base = await admin.fetchTopicOffsets(topic)
    const baseOf = Object.fromEntries(
      base.map(({ partition, offset }) => [partition, Number(offset)])
    )
    const kafka = fixture.kafka(measuredOptions)
    const producer = kafka.producer(expectedOptions.producer)
    const consumer = kafka.consumer(expectedOptions.consumer)
    this.#clients.push(consumer, producer)
    const total = warmup + messages
    this.#state = {
      handled: 0,
      total: warmup,
      next: new Array(partitions).fill(0),
      table: new Table(HANDLER_COLUMNS, messages),
      callbacks: new Table(CALLBACK_COLUMNS, messages),
      lastDone: 0,
    }
    const finished = this.#phase()
    const commit = (partition, offset) =>
      consumer.commitOffsets([{ topic, partition, offset: String(offset) }])
    const commitAll = () =>
      consumer.commitOffsets(
        this.#state.next
          .map((offset, partition) => ({ topic, partition, offset: String(offset) }))
          .filter(({ offset }) => offset !== '0')
      )
    const handlers = this.#handlers(commit, commitAll)
    this.#main = { consumer, producer, commitAll }
    const runOptions = { ...expectedOptions.run, [handler]: handlers[handler] }
    assertPublicOptions({ ...expectedOptions, run: runOptions }, expectedOptions)
    await producer.connect()
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })
    await consumer.run(runOptions)
    if (active) await this.#awaitSubscriptions(probe, interval)
    await this.#produce((m) => producer.send(m), 'w', warmup, null)
    if (warmup > 0) await this.#within(finished)
    const warmNext = [...this.#state.next]
    await Promise.all(
      warmNext.map((offset, partition) => (offset > 0 ? commit(partition, offset) : null))
    )
    this.#state.total = total
    const measuredDone = this.#phase()
    const sendLedger = new Table(SEND_COLUMNS, messages)
    const startMs = now()
    await this.#produce((m) => producer.send(m), 'm', messages, sendLedger)
    await this.#within(measuredDone)
    const elapsedNs = Math.round((this.#state.lastDone - startMs) * 1e6)
    const window = deliveryWindow(sendLedger, this.#state.table)
    if (this.#exports) this.#exports.check()
    return this.#judge({ admin, helper, probe, sendLedger, baseOf, elapsedNs, size, window })
  }

  /** Waits (bounded) until two accepted subscriptions at the declared interval were observed.
   * @param {TelemetryPushProbe} probe tap @param {number} interval declared push interval ms
   * @returns {Promise<void>} @throws {Error} when they did not arrive in 60 s of fixture time
   */
  async #awaitSubscriptions(probe, interval) {
    for (let waited = 0; waited < 60000; waited += 250) {
      const ok = new Set(
        probe.records
          .filter(
            (r) => r.kind === 'subscription' && r.errorCode === 0 && r.pushIntervalMs === interval
          )
          .map(({ uuid }) => uuid)
      )
      if (ok.size >= 2) return
      await this.#fixture.wait(250)
    }
    throw new Error('subscriptions were not accepted before the measured window')
  }

  /** Quiesces, commits the frontier, stops the clients and joins the three outcome sets.
   * @param {object} context ledgers and clients @returns {Promise<object>} scalar result
   * @throws {AssertionError} on any failed oracle
   */
  async #judge({ admin, helper, probe, sendLedger, baseOf, elapsedNs, size, window }) {
    const { warmup, partitions, handler, telemetry } = this.#config
    const fixture = this.#fixture
    const { topic, groupId } = fixture
    const state = this.#state
    const { consumer, producer, commitAll } = this.#main
    await commitAll()
    await consumer.stop()
    await consumer.disconnect()
    await producer.disconnect()
    const ends = await admin.fetchTopicOffsets(topic)
    const durable = await this.#audit(helper, ends)
    const commits = await admin.fetchOffsets({ groupId, topics: [topic], resolveOffsets: false })
    const finalCommits = {}
    for (const { partition, offset } of commits.flatMap((c) => c.partitions))
      finalCommits[`p${partition}`] = String(offset)
    const expectedCommits = Object.fromEntries(
      state.next.map((offset, p) => [`p${p}`, String(offset)]).filter(([, o]) => o !== '0')
    )
    const records = probe.finish()
    if (this.#exports) await fixture.wait(250)
    const exports = this.#exports ? await this.#exports.finish() : []
    telemetryEvidence(records, {
      active: telemetry === 'active',
      intervalMs: this.#config.interval ?? 1000,
      exports,
      window,
    })
    const tables = { send: sendLedger, handler: state.table, callbacks: state.callbacks, durable }
    const meta = {
      baseOf,
      warmup,
      partitions,
      elapsedNs,
      handler,
      finalCommits,
      expectedCommits,
      ends: ends.map(({ partition, offset }) => ({ partition, offset: String(offset) })),
      window,
    }
    const result = deliveryResult(this.#config, tables, meta)
    validateDelivery(result, deliveryEvidence(this.#config, tables, meta))
    this.#seal(result, tables, meta)
    return result
  }

  /** @param {object} result scalar result @param {object} tables ledgers @param {object} meta commit/offset facts
   * @returns {void} persists every ledger and the commit/offset facts; attaches the sealed artifacts
   */
  #seal(result, tables, meta) {
    const { identity } = this.#evidence
    this.#evidence.write('delivery-send.bin', tables.send.buffer())
    this.#evidence.write('delivery-handler.bin', tables.handler.buffer())
    this.#evidence.write('delivery-callbacks.bin', tables.callbacks.buffer())
    this.#evidence.write('delivery-durable.bin', tables.durable.buffer())
    this.#evidence.write(
      'delivery-meta.json',
      JSON.stringify({ ...meta, runId: identity.runId, manifestSha256: identity.manifestSha256 })
    )
    result.artifacts = this.#evidence.artifacts()
  }

  /** Stops and disconnects every public client in reverse creation order; errors are collected.
   * @returns {Promise<object>} clean counters @throws {AggregateError} when any cleanup step failed
   */
  async close() {
    if (this.#closed) return cleanReport()
    this.#closed = true
    const errors = []
    for (const client of [...this.#clients].reverse()) {
      try {
        if (typeof client.stop === 'function') await client.stop()
        await client.disconnect()
      } catch (error) {
        errors.push(error)
      }
    }
    this.#clients = []
    if (errors.length) throw new AggregateError(errors, 'delivery cleanup failed')
    return cleanReport()
  }
}

/** Derives the conservative measured Push window from actual send and callback-entry clocks.
 * The window excludes setup/warmup and the final callback's completion; recorded metadata cannot
 * widen it to borrow a warmup or shutdown Push during offline verification.
 * @param {Table} send measured send ledger @param {Table} handler measured handler ledger
 * @returns {{startMs:number,endMs:number}} independently reconstructible window
 * @throws {AssertionError} for missing or nonfinite observed times
 */
const deliveryWindow = (send, handler) => {
  const startMs = send.column('sendMs').reduce((min, value) => Math.min(min, value), Infinity)
  const endMs = handler.column('entryMs').reduce((max, value) => Math.max(max, value), -Infinity)
  assert(Number.isFinite(startMs) && Number.isFinite(endMs) && endMs >= startMs)
  return { startMs, endMs }
}

/** @param {object} config delivery counts @param {object} tables send/handler/callbacks/durable ledgers
 * @param {object} meta offsets, commits and elapsed time @returns {object} the scalar result those ledgers imply
 */
const deliveryResult = (config, { send, handler, callbacks }, meta) => {
  const sendMs = new Map(
    send.column('ordinal').map((ordinal, row) => [ordinal, send.get(row, 'sendMs')])
  )
  const latencies = handler
    .column('ordinal')
    .map((ordinal, row) => handler.get(row, 'entryMs') - sendMs.get(ordinal))
  const entries = callbacks.column('entryMs')
  const gaps = entries.slice(1).map((entry, i) => entry - entries[i])
  return {
    mode: 'delivery',
    handler: config.handler,
    telemetry: config.telemetry,
    messages: send.length,
    size: config.size,
    callbacks: callbacks.length,
    deliveredMessages: handler.length,
    gapSamples: Math.max(0, callbacks.length - 1),
    elapsedNs: meta.elapsedNs,
    messagesPerSecond: (handler.length * 1e9) / meta.elapsedNs,
    p99Ms: p99(latencies),
    deliveryGapP99Ms: p99(gaps),
    discardedWorkloadWarmups: config.warmup,
    measuredObservations: send.length,
  }
}

/** @param {object} config delivery counts @param {object} tables ledgers @param {object} meta offsets/commits
 * @returns {object} the independent evidence `validateDelivery` judges: the predeclared corpus, the durable
 * log, the handler invocations, raw broker commits and the real callback gaps
 * @throws {AssertionError} for an out-of-order send ledger or a damaged durable value
 */
const deliveryEvidence = (config, { send, handler, callbacks, durable }, meta) => {
  const { partitions } = config
  assert.equal(send.length, config.messages, 'delivery corpus is incomplete')
  assert.equal(handler.length, config.messages)
  assert.equal(durable.length, config.messages)
  assert.equal(meta.warmup, config.warmup)
  assert.equal(meta.partitions, partitions)
  assert.equal(meta.handler, config.handler)
  exactSet(
    Object.keys(meta.baseOf),
    Array.from({ length: partitions }, (_, p) => String(p))
  )
  assert(
    Object.values(meta.baseOf).every((offset) => offset === 0),
    'owned topic was not initially empty'
  )
  const countFor = (count, p) => Math.floor(count / partitions) + (p < count % partitions ? 1 : 0)
  const expectedCommits = Object.fromEntries(
    Array.from({ length: partitions }, (_, p) => [
      `p${p}`,
      String(meta.baseOf[p] + countFor(config.warmup, p) + countFor(config.messages, p)),
    ])
  )
  assert.deepEqual(meta.expectedCommits, expectedCommits)
  assert.deepEqual(meta.finalCommits, expectedCommits)
  assert.deepEqual(
    [...meta.ends].sort((a, b) => a.partition - b.partition),
    Array.from({ length: partitions }, (_, p) => ({
      partition: p,
      offset: expectedCommits[`p${p}`],
    }))
  )
  const warmCount = (p) =>
    Math.floor(meta.warmup / partitions) + (p < meta.warmup % partitions ? 1 : 0)
  const id = (ordinal, partition, offset, hash) => `m-${ordinal}|${partition}|${offset}|${hash}`
  const expected = Array.from({ length: config.messages }, (_, i) => {
    assert.equal(send.get(i, 'ordinal'), i, 'send ledger lost its order')
    const p = i % partitions
    const offset = (meta.baseOf[p] ?? 0) + warmCount(p) + Math.floor(i / partitions)
    return id(i, p, offset, send.get(i, 'hash'))
  })
  const rows = (table) =>
    Array.from({ length: table.length }, (_, r) =>
      id(
        table.get(r, 'ordinal'),
        table.get(r, 'partition'),
        table.get(r, 'offset'),
        table.get(r, 'hash')
      )
    )
  assert(
    durable.column('tailOk').every((ok) => ok === 1),
    'a durable value lost its derived bytes'
  )
  const sendMs = new Map(
    send.column('ordinal').map((ordinal, row) => [ordinal, send.get(row, 'sendMs')])
  )
  const entries = callbacks.column('entryMs')
  return {
    expected,
    durable: rows(durable),
    delivered: rows(handler),
    measuredBytes: handler.column('bytes'),
    entries,
    gaps: entries.slice(1).map((entry, i) => entry - entries[i]),
    latencies: handler
      .column('ordinal')
      .map((ordinal, row) => handler.get(row, 'entryMs') - sendMs.get(ordinal)),
    batchSizes: callbacks.column('size'),
    expectedCommits,
    finalCommits: meta.finalCommits,
    minimumBatchGaps: config.minimumGaps ?? MINIMUM_GAPS.eachBatch,
    minimumMessageGaps: config.minimumGaps ?? MINIMUM_GAPS.eachMessage,
  }
}

const COMMON_KEYS = [
  'schemaVersion',
  'runId',
  'tree',
  'driverSha256',
  'manifestSha256',
  'configSha256',
  'mode',
  'control',
  'artifacts',
  'clean',
  'discardedWorkloadWarmups',
  'measuredObservations',
]
const MODE_KEYS = {
  requests: [
    'telemetry',
    'completedPairs',
    'physicalConnections',
    'successfulSamples',
    'observerCalls',
    'aggregateSamples',
    'cpuMicros',
    'elapsedNs',
    'cpuNsPerPair',
    'elapsedNsPerPair',
    'heapExternalDeltaBytes',
  ],
  payload: [
    'codecAvailable',
    'rawBytes',
    'transmittedBytes',
    'fittingPasses',
    'sliceP99Ms',
    'cpuP99Ms',
    'wallP99Ms',
    'heapExternalDeltaBytes',
    'rssDeltaBytes',
  ],
  delivery: [
    'handler',
    'telemetry',
    'messages',
    'size',
    'callbacks',
    'deliveredMessages',
    'gapSamples',
    'elapsedNs',
    'messagesPerSecond',
    'p99Ms',
    'deliveryGapP99Ms',
  ],
}
const PROJECTION = {
  requests: ['cpuNsPerPair', 'elapsedNsPerPair', 'heapExternalDeltaBytes', 'completedPairs'],
  payload: [
    'sliceP99Ms',
    'cpuP99Ms',
    'wallP99Ms',
    'fittingPasses',
    'heapExternalDeltaBytes',
    'rssDeltaBytes',
    'rawBytes',
    'transmittedBytes',
  ],
  delivery: ['messagesPerSecond', 'p99Ms', 'deliveryGapP99Ms', 'gapSamples', 'elapsedNs'],
}

/** @param {object} result validated worker envelope @returns {Object<string,number>} only the finite named
 * numbers `session.js` summarizes; persist the full envelope before calling this
 * @throws {AssertionError} when a named field is not finite
 */
const numericProjection = (result) =>
  Object.fromEntries(
    PROJECTION[result.mode].map((key) => {
      assert(Number.isFinite(result[key]), `${key} must be finite`)
      return [key, result[key]]
    })
  )

/** @param {Function} read artifact reader @param {string} name JSON artifact
 * @param {object} declaration run identity @returns {object} parsed sidecar owned by this run
 */
const readOwned = (read, name, declaration) => {
  const data = JSON.parse(read(name, 8 * MIB).toString('utf8'))
  if (data.runId !== undefined) {
    assert.equal(data.runId, declaration.runId)
    assert.equal(data.manifestSha256, declaration.manifestSha256)
  }
  return data
}

/** @param {object} result worker envelope @param {object} declaration run identity and config
 * @param {Function} read artifact reader @returns {{signedHeapExternalBytes:number}} derived memory fact
 * @throws {AssertionError} for any ledger, identity, aggregate or memory difference
 */
const verifyRequests = (result, declaration, read) => {
  const { config } = declaration
  assert.equal(result.telemetry, config.telemetry)
  assert.equal(result.completedPairs, config.iterations)
  assert.equal(result.discardedWorkloadWarmups, config.warmup)
  const table = (name, columns) => Table.from(read(name), columns)
  const ledgers = {
    writes: table('requests-write.bin', TUPLE),
    responses: table('requests-response.bin', TUPLE),
    returned: table('requests-returned.bin', TUPLE),
    observed: table('requests-observe.bin', ['node', 'duration']),
  }
  const meta = readOwned(read, 'request-meta.json', declaration)
  assert.equal(meta.telemetry, config.telemetry)
  const memory = readOwned(read, 'request-memory.json', declaration)
  assert.equal(memory.schemaVersion, 1)
  const signed = signedDelta(memory.before, memory.after)
  assert.equal(memory.signedDeltaBytes, signed.sum)
  assert.equal(result.heapExternalDeltaBytes, Math.max(0, signed.sum))
  assert.equal(memory.pairs, result.completedPairs)
  assert.equal(memory.connections, config.nodes)
  assert.equal(memory.collectorLive, config.telemetry !== 'off')
  const nodes = ledgers.observed.column('node')
  assert.equal(nodes.length, config.telemetry === 'off' ? 0 : config.iterations)
  assert(
    nodes.every((node, i) => node === i % config.nodes),
    'observer slot identities differ'
  )
  validateRequests(result, requestEvidence(config, ledgers, meta))
  return { signedHeapExternalBytes: signed.sum }
}

/** @param {object} result worker envelope @param {object} declaration run identity and config
 * @param {Function} read artifact reader @returns {object} derived facts (none needed by the session)
 * @throws {AssertionError|CodecUnavailableError} for any sample, scalar or memory difference
 */
const verifyPayload = (result, declaration, read) => {
  const { config } = declaration
  const meta = readOwned(read, 'payload-meta.json', declaration)
  const memory = readOwned(read, 'payload-memory.json', declaration)
  assert.equal(meta.offeredCodec, config.codec)
  assert.equal(result.discardedWorkloadWarmups, config.warmup)
  assert.equal(result.measuredObservations, config.snapshots)
  assert.deepEqual(result.codecAvailable, { none: true, gzip: true, zstd: meta.zstdSupported })
  if (declaration.codecs) assert.deepEqual(result.codecAvailable, declaration.codecs)
  assert(config.codec !== 'zstd' || meta.zstdSupported, 'a required codec was unavailable')
  if (declaration.root)
    assert.equal(
      meta.scopeVersion,
      JSON.parse(fs.readFileSync(path.join(declaration.root, 'package.json'), 'utf8')).version
    )
  const rows = Table.from(read('payload-rows.bin'), PAYLOAD_ROWS)
  assert.equal(rows.length, config.snapshots)
  const raw = read('payload-raw.bin')
  const transmitted = read('payload-transmitted.bin')
  let rawAt = 0
  let sentAt = 0
  for (let row = 0; row < rows.length; row += 1) {
    const sample = {
      compressionType: rows.get(row, 'compressionType'),
      codecAttempts: rows.get(row, 'codecAttempts'),
      rawBytes: rows.get(row, 'rawBytes'),
      transmittedBytes: rows.get(row, 'transmittedBytes'),
      fittingPasses: rows.get(row, 'fittingPasses'),
    }
    sample.metrics = transmitted.subarray(sentAt, sentAt + sample.transmittedBytes)
    const inflated = validatePayloadSample(
      config,
      config.warmup + row + 1,
      meta.scopeVersion,
      sample
    )
    assert(inflated.equals(raw.subarray(rawAt, rawAt + sample.rawBytes)), 'raw buffer differs')
    rawAt += sample.rawBytes
    sentAt += sample.transmittedBytes
  }
  assert.equal(rawAt, raw.length)
  assert.equal(sentAt, transmitted.length)
  const largest = (column) => rows.column(column).reduce((max, value) => Math.max(max, value), 0)
  assert.equal(result.rawBytes, largest('rawBytes'))
  assert.equal(result.transmittedBytes, largest('transmittedBytes'))
  assert.equal(result.fittingPasses, largest('fittingPasses'))
  assert.equal(result.sliceP99Ms, p99(rows.column('sliceMs')))
  assert.equal(result.cpuP99Ms, p99(rows.column('cpuMs')))
  assert.equal(result.wallP99Ms, p99(rows.column('wallMs')))
  const signed = signedDelta(memory.before, memory.after)
  assert.deepEqual(memory.signed, signed)
  assert.equal(result.heapExternalDeltaBytes, Math.max(0, signed.sum))
  assert.equal(memory.rssBaselineBytes, memory.before.rss)
  assert.equal(memory.maxRssBytes, memory.maxRssKiB * 1024)
  assert.equal(memory.rssDeltaBytes, rssDelta(memory.maxRssKiB, memory.rssBaselineBytes))
  assert.equal(result.rssDeltaBytes, memory.rssDeltaBytes)
  assert(result.heapExternalDeltaBytes <= 8388608, 'payload heap/external ceiling exceeded')
  assert(result.rssDeltaBytes <= 67108864, 'payload RSS ceiling exceeded')
  return {}
}

/** @param {object} result worker envelope @param {object} declaration run identity and config
 * @param {Function} read artifact reader @param {Function} chunks verified chunk-series reader
 * @returns {object} derived facts (none needed by the session)
 * @throws {AssertionError} for any outcome-set, gap, commit, telemetry or scalar difference
 */
const verifyDelivery = (result, declaration, read, chunks) => {
  const { config } = declaration
  const tables = {
    send: Table.from(read('delivery-send.bin'), SEND_COLUMNS),
    handler: Table.from(read('delivery-handler.bin'), HANDLER_COLUMNS),
    callbacks: Table.from(read('delivery-callbacks.bin'), CALLBACK_COLUMNS),
    durable: Table.from(read('delivery-durable.bin'), DURABLE_COLUMNS),
  }
  const meta = readOwned(read, 'delivery-meta.json', declaration)
  assert.equal(result.handler, config.handler)
  assert.equal(result.telemetry, config.telemetry)
  assert.deepEqual(meta.window, deliveryWindow(tables.send, tables.handler))
  const derived = deliveryResult(config, tables, meta)
  for (const key of Object.keys(derived)) assert.deepEqual(result[key], derived[key], key)
  validateDelivery(derived, deliveryEvidence(config, tables, meta))
  const records = chunks('delivery-telemetry')
    .flatMap((chunk) => chunk.toString('utf8').split('\n'))
    .filter(Boolean)
    .map((line) => JSON.parse(line))
  telemetryEvidence(records, {
    active: config.telemetry === 'active',
    intervalMs: config.interval,
    exports:
      config.telemetry === 'active'
        ? chunks('delivery-exports').flatMap((chunk) =>
            chunk
              .toString('utf8')
              .split('\n')
              .filter(Boolean)
              .map((line) => JSON.parse(line))
          )
        : [],
    window: meta.window,
  })
  return {}
}

/** Rereads and independently judges one finished worker; no scalar-only acceptance path exists.
 * @param {object} result the worker's emitted envelope
 * @param {{runId:string,tree:string,driverSha256:string,manifestSha256:string,configSha256:string,mode:string,
 * control:number,config:object,root?:string}} declaration frozen identity from the manifest and the argv
 * @param {string} directory the worker's own evidence directory
 * @returns {object} facts derived from the sidecars (e.g. `signedHeapExternalBytes`)
 * @throws {AssertionError|CodecUnavailableError} for schema drift, a bad or extra artifact, nonzero clean
 * counters or any independently rederived difference
 */
const verifyWorker = (result, declaration, directory) => {
  validateEnvelope(result, declaration)
  const allowed = new Set([...COMMON_KEYS, ...MODE_KEYS[declaration.mode]])
  for (const key of Object.keys(result)) assert(allowed.has(key), `unexpected field ${key}`)
  assert.deepEqual(result.clean, cleanReport())
  assert(result.artifacts.length <= 1024)
  verifyDirectory(directory, result.artifacts)
  const find = (name) => result.artifacts.find((artifact) => artifact.name === name)
  const read = (name, limit) => {
    assert(find(name), `missing artifact ${name}`)
    return readArtifact(directory, find(name), limit)
  }
  const chunks = (prefix) => readChunks(directory, result.artifacts, prefix)
  const verify = { requests: verifyRequests, payload: verifyPayload, delivery: verifyDelivery }
  return verify[declaration.mode](result, declaration, read, chunks)
}

const DOMAINS = {
  requests: {
    fixed: { iterations: 1000000, warmup: 100000, nodes: 64 },
    choices: { telemetry: ['off', 'empty', 'active'] },
  },
  payload: {
    fixed: { snapshots: 100, warmup: 20, 'telemetry-max-bytes': 1048576 },
    choices: {
      points: ['1', '257'],
      temporality: ['delta', 'cumulative'],
      codec: ['none', 'gzip', 'zstd'],
      attributes: ['compressible', 'poor'],
    },
  },
  delivery: {
    fixed: {
      messages: 1000000,
      warmup: 100000,
      size: 200,
      partitions: 6,
      interval: 1000,
      timeout: 540000,
    },
    choices: { handler: ['eachMessage', 'eachBatch'], telemetry: ['off', 'active'] },
  },
}
const camel = (name) => name.replace(/-([a-z])/g, (_, letter) => letter.toUpperCase())

/** Parses one worker command line strictly, before any import, file or child exists.
 * @param {string[]} argv arguments after the script @returns {{mode:string,control:number,config:object}}
 * typed configuration; fixed quantities are the full-acceptance values, choices are mandatory
 * @throws {UsageError} for an unknown, repeated, valueless or cross-mode flag, a non-integer value, an
 * illegal domain, a bare or non-0/1 control, or a requests control without the active arm
 */
const parseWorkerArgs = (argv) => {
  const flags = new Map()
  for (let i = 0; i < argv.length; i += 2) {
    const [name, value] = [argv[i], argv[i + 1]]
    if (!/^--[a-z][a-z-]*$/.test(name)) throw new UsageError(`unexpected argument ${name}`)
    if (value === undefined || value.startsWith('--')) throw new UsageError(`${name} needs a value`)
    if (flags.has(name)) throw new UsageError(`${name} is repeated`)
    flags.set(name, value)
  }
  const mode = flags.get('--mode')
  const domain = DOMAINS[mode]
  if (!domain) throw new UsageError('--mode must be requests, payload or delivery')
  const allowed = new Set([
    '--mode',
    '--control',
    ...[...Object.keys(domain.fixed), ...Object.keys(domain.choices)].map((k) => `--${k}`),
  ])
  for (const name of flags.keys())
    if (!allowed.has(name)) throw new UsageError(`${name} is not a ${mode} flag`)
  const rawControl = flags.get('--control') ?? '0'
  if (!['0', '1'].includes(rawControl)) throw new UsageError('--control must be 0 or 1')
  const config = {}
  for (const [name, fixed] of Object.entries(domain.fixed)) {
    const value = flags.get(`--${name}`)
    if (value !== undefined && !(/^\d+$/.test(value) && Number(value) === fixed))
      throw new UsageError(`--${name} must be ${fixed}`)
    config[camel(name)] = fixed
  }
  for (const [name, choices] of Object.entries(domain.choices)) {
    const value = flags.get(`--${name}`)
    if (!choices.includes(value))
      throw new UsageError(`--${name} must be one of ${choices.join('|')}`)
    config[camel(name)] = name === 'points' ? Number(value) : value
  }
  const control = Number(rawControl)
  if (mode === 'requests' && control === 1 && config.telemetry !== 'active')
    throw new UsageError('the observer control needs --telemetry active')
  return { mode, control, config }
}

/** @param {string[]} argv process arguments after the script
 * @returns {{kind:'worker'|'session'|'verify',mode?:string,control?:number,config?:object,path?:string}}
 * @throws {UsageError} when controller flags are mixed with worker flags or the path is not absolute
 */
const parseArgs = (argv) => {
  const controller = ['--session', '--verify-session']
  if (controller.includes(argv[0])) {
    if (argv.length !== 2 || !path.isAbsolute(argv[1]))
      throw new UsageError(`${argv[0]} takes exactly one absolute path`)
    return { kind: argv[0] === '--session' ? 'session' : 'verify', path: argv[1] }
  }
  if (argv.some((arg) => controller.includes(arg)))
    throw new UsageError('controller flags exclude every worker flag')
  return { kind: 'worker', ...parseWorkerArgs(argv) }
}

const JOBS = {
  'requests-normal': {
    mode: 'requests',
    roles: ['previous', 'off', 'empty', 'active'],
    reference: 'previous',
    runs: 10,
    telemetry: { previous: 'off', off: 'off', empty: 'empty', active: 'active' },
    controls: [],
    cost: ['cpuNsPerPair'],
    latency: [],
    compare: [
      { arm: 'off', against: 'previous' },
      { arm: 'active', against: 'off' },
    ],
    memory: true,
  },
  'requests-control': {
    mode: 'requests',
    roles: ['normal', 'busy'],
    reference: 'normal',
    runs: 9,
    telemetry: { normal: 'active', busy: 'active' },
    controls: ['busy'],
    cost: ['cpuNsPerPair'],
    latency: [],
    compare: [{ arm: 'busy', against: 'normal', expect: ['cpuNsPerPair'] }],
  },
  payload: {
    mode: 'payload',
    roles: ['normal', 'slow'],
    reference: 'normal',
    runs: 9,
    telemetry: {},
    controls: ['slow'],
    cost: [],
    latency: ['sliceP99Ms', 'cpuP99Ms', 'wallP99Ms'],
    absolute: [{ arm: 'normal' }, { arm: 'slow', expect: ['sliceP99Ms', 'wallP99Ms'] }],
  },
  'delivery-normal': {
    mode: 'delivery',
    roles: ['previous', 'off', 'active'],
    reference: 'previous',
    runs: 8,
    telemetry: { previous: 'off', off: 'off', active: 'active' },
    controls: [],
    cost: ['messagesPerSecond'],
    latency: ['p99Ms', 'deliveryGapP99Ms'],
    compare: [
      { arm: 'off', against: 'previous' },
      { arm: 'active', against: 'off' },
    ],
  },
  'delivery-control': {
    mode: 'delivery',
    roles: ['normal', 'slow'],
    reference: 'normal',
    runs: 9,
    telemetry: { normal: 'active', slow: 'active' },
    controls: ['slow'],
    cost: ['messagesPerSecond'],
    latency: ['p99Ms', 'deliveryGapP99Ms'],
    compare: [{ arm: 'slow', against: 'normal', expect: ['messagesPerSecond', 'p99Ms'] }],
  },
}
const CHILD_SECONDS = 610
const RESERVE_SECONDS = 300

/** @param {string} jobName declared job @returns {number} children the fixed schedule executes: m*(n+1) */
const childCount = (jobName) => {
  const job = JOBS[jobName]
  return schedule(job.roles, job.reference, job.runs).rows.length
}

/** @param {string} jobName declared job @returns {number} minimum manifest budget in seconds */
const minimumBudget = (jobName) => childCount(jobName) * CHILD_SECONDS + RESERVE_SECONDS

/** Verifies a frozen input file without following a substituted symbolic link.
 * @param {{path:string,sha256:string}} asset manifest file identity @returns {Buffer} checked bytes
 * @throws {AssertionError|Error} for a missing file, link or digest mismatch
 */
const frozenAsset = (asset) => {
  assert(path.isAbsolute(asset.path) && /^[a-f0-9]{64}$/.test(asset.sha256))
  const fd = fs.openSync(asset.path, fs.constants.O_RDONLY | NOFOLLOW)
  try {
    assert(fs.fstatSync(fd).isFile())
    const bytes = fs.readFileSync(fd)
    assert.equal(sha256(bytes), asset.sha256, `frozen asset differs: ${asset.path}`)
    return bytes
  } finally {
    fs.closeSync(fd)
  }
}

/** Binds a frozen source snapshot to a complete sorted file catalogue and its archive receipt.
 * The source receipt is {tree, files:[{path,sha256}], tarballSha256}; tree is the catalogue digest.
 * Driver/session staging lives outside the source snapshot; .git and node_modules are excluded.
 * @param {object} arm root/tree/source/tarball declaration @param {string} consumeSha256 inherited driver digest
 * @returns {void}
 * @throws {AssertionError|Error} for changed bytes, unlisted source, links or archive identity drift
 */
const frozenSource = (arm, consumeSha256) => {
  const source = JSON.parse(frozenAsset(arm.source))
  frozenAsset(arm.tarball)
  assert.equal(source.tarballSha256, arm.tarball.sha256)
  assert.equal(source.tree, arm.tree)
  assert.equal(digest(source.files), arm.tree)
  const files = []
  const visit = (relative) => {
    const directory = path.join(arm.root, relative)
    assert(fs.lstatSync(directory).isDirectory() && !fs.lstatSync(directory).isSymbolicLink())
    for (const name of fs.readdirSync(directory).sort()) {
      if (name === '.git' || name === 'node_modules') continue
      const file = path.join(relative, name)
      const absolute = path.join(arm.root, file)
      const stat = fs.lstatSync(absolute)
      assert(!stat.isSymbolicLink(), 'frozen source cannot contain links')
      if (stat.isDirectory()) visit(file)
      else {
        assert(stat.isFile())
        files.push({ path: file, sha256: sha256(fs.readFileSync(absolute)) })
      }
    }
  }
  visit('')
  assert(files.length > 0)
  assert.deepEqual(files, source.files, 'frozen source catalogue differs')
  assert.equal(
    sha256(fs.readFileSync(path.join(arm.root, 'scripts/benchmarks/consumeThroughput.js'))),
    consumeSha256,
    'frozen root has a different inherited consume driver'
  )
}

/** Binds this loaded supervisor/session, unchanged consume driver, runtime and source roots before work.
 * Candidate arms share one snapshot. Previous requires a separately hashed accepted-9E receipt with
 * {milestone:'9E',status:'accepted',tree,sourceSha256,tarballSha256} matching its frozen inputs.
 * @param {object} manifest complete session declaration @returns {void}
 * @throws {AssertionError|Error} when any executable, runtime or frozen identity cannot be verified
 */
const verifyProvenance = (manifest) => {
  for (const name of ['driver', 'session']) {
    frozenAsset(manifest[name])
    assert.equal(manifest[name].sha256, LOADED_SOURCES[name], `${name} differs from loaded bytes`)
  }
  assert.equal(fs.realpathSync(manifest.driver.path), fs.realpathSync(__filename))
  assert.equal(
    fs.realpathSync(manifest.session.path),
    fs.realpathSync(require.resolve('./session'))
  )
  assert.equal(path.dirname(manifest.driver.path), path.dirname(manifest.session.path))
  frozenAsset(manifest.consume)
  frozenAsset({ path: manifest.node.binary, sha256: manifest.node.sha256 })
  assert.equal(fs.realpathSync(manifest.node.binary), fs.realpathSync(process.execPath))
  for (const [key, actual] of Object.entries({
    version: process.version,
    platform: process.platform,
    arch: process.arch,
  }))
    assert.equal(manifest.node[key], actual, `runtime ${key} differs`)
  const candidate = manifest.arms.filter(({ label }) => label !== 'previous')
  const identity = ({ root, tree, source, tarball }) => ({ root, tree, source, tarball })
  for (const arm of candidate) assert.deepEqual(identity(arm), identity(candidate[0]))
  const roots = new Map(manifest.arms.map((arm) => [arm.root, arm]))
  for (const arm of roots.values()) frozenSource(arm, manifest.consume.sha256)
  const previous = manifest.arms.find(({ label }) => label === 'previous')
  if (previous) {
    assert.notEqual(previous.root, candidate[0].root)
    assert.notEqual(previous.tree, candidate[0].tree)
    const accepted = JSON.parse(frozenAsset(manifest.acceptedBaseline))
    assert.equal(accepted.milestone, '9E')
    assert.equal(accepted.status, 'accepted')
    assert.equal(accepted.tree, previous.tree)
    assert.equal(accepted.sourceSha256, previous.source.sha256)
    assert.equal(accepted.tarballSha256, previous.tarball.sha256)
  }
}

/** Rejects any manifest that is not exactly a declared, balanced, complete session.
 * @param {object} manifest immutable session declaration @param {Function} [parse] argv parser (tests only)
 * @returns {void} @throws {AssertionError|UsageError} for a changed arm, label, reference, run count, control,
 * configuration hash, relative path or a budget below `children*610+300` seconds
 */
const validateManifest = (manifest, parse = parseWorkerArgs) => {
  assert.equal(manifest.schemaVersion, 1)
  const job = JOBS[manifest.job]
  assert(job, 'unknown job')
  assert.deepEqual(
    manifest.arms.map(({ label }) => label),
    job.roles
  )
  assert.equal(manifest.reference, job.reference)
  assert.equal(manifest.runs, job.runs)
  assert(
    manifest.budgetSeconds >= minimumBudget(manifest.job),
    'budget is below children*610+300 seconds'
  )
  for (const asset of ['driver', 'session', 'consume']) {
    assert(path.isAbsolute(manifest[asset].path))
    assert(/^[a-f0-9]{64}$/.test(manifest[asset].sha256))
  }
  const configs = []
  for (const arm of manifest.arms) {
    assert(path.isAbsolute(arm.root) && typeof arm.tree === 'string' && arm.tree)
    const parsed = parse(arm.argv.map(String))
    assert.equal(parsed.mode, job.mode)
    assert.equal(parsed.control, job.controls.includes(arm.label) ? 1 : 0)
    if (job.telemetry[arm.label] !== undefined)
      assert.equal(parsed.config.telemetry, job.telemetry[arm.label])
    assert.equal(arm.configSha256, digest({ mode: parsed.mode, argv: arm.argv }))
    const { telemetry, ...common } = parsed.config
    configs.push(common)
  }
  for (const config of configs.slice(1)) assert.deepEqual(config, configs[0])
  assert.equal(manifest.codecs?.none, true)
  assert.equal(manifest.codecs?.gzip, true)
  assert.equal(typeof manifest.codecs?.zstd, 'boolean')
  if (manifest.optionalCodec !== undefined) {
    assert.equal(manifest.optionalCodec, 'zstd')
    assert.equal(job.mode, 'payload')
    assert(configs.every((config) => config.codec === 'zstd'))
    assert.equal(manifest.codecs.zstd, false)
  }
  verifyProvenance(manifest)
}

/** @param {number} value finite median @returns {number} the same value @throws {AssertionError} unless positive */
const positive = (value) => {
  assert(
    Number.isFinite(value) && value > 0,
    'a compared reference median must be finite and positive'
  )
  return value
}

/** Pairs the candidate-active and candidate-off request-memory facts of each original measured round.
 * @param {Array<{round:number,label:string,warmup:boolean,derived?:{signedHeapExternalBytes:number}}>} rows
 * observations @param {number} rounds measured rounds the schedule declares
 * @returns {Array<{round:number,incrementBytes:number}>} `max(0, activeSigned - offSigned)` per round
 * @throws {AssertionError} for a missing sidecar fact, a nonfinite reading or a removed/mismatched pair
 */
const memoryPairs = (rows, rounds) => {
  const bucket = (label) => {
    const entries = rows.filter((row) => !row.warmup && row.label === label)
    for (const { derived } of entries)
      assert(
        Number.isFinite(derived?.signedHeapExternalBytes),
        'a request memory sidecar fact is missing'
      )
    return new Map(entries.map((row) => [row.round, row.derived.signedHeapExternalBytes]))
  }
  const [off, active] = [bucket('off'), bucket('active')]
  assert.equal(off.size, rounds)
  assert.equal(active.size, rounds)
  return [...active].map(([round, signed]) => {
    assert(off.has(round), 'an active/off memory pair is mismatched')
    return { round, incrementBytes: Math.max(0, signed - off.get(round)) }
  })
}

/** Evaluates a complete validated session: noise first (a void session says nothing about gates), then the
 * fixed comparisons, positive-control effectiveness and the request memory pairs.
 * @param {string} jobName declared job @param {object} declared schedule @param {Array<object>} rows
 * observations with numeric `result` projections and `derived` sidecar facts
 * @returns {{disposition:'pass'|'gate-failed'|'void',exit:number,failures:object[],noise:object}} verdict
 * @throws {AssertionError|Error} for a malformed session, a zero reference median or an ineffective control
 */
const evaluate = (jobName, declared, rows) => {
  const job = JOBS[jobName]
  const measured = noise(declared, rows, job.cost, job.latency)
  if (measured.void) return { disposition: 'void', exit: 3, failures: [], noise: measured }
  const medianOf = (label, key) =>
    median(
      rows.filter((row) => !row.warmup && row.label === label).map(({ result }) => result[key])
    )
  const medians = (label, keys) =>
    Object.fromEntries(keys.map((key) => [key, medianOf(label, key)]))
  const keys = [
    ...job.cost,
    ...job.latency,
    ...(job.mode === 'payload' ? ['fittingPasses', 'heapExternalDeltaBytes', 'rssDeltaBytes'] : []),
  ]
  const failures = []
  if (job.mode === 'payload')
    for (const row of rows) {
      const fields = ['heapExternalDeltaBytes', 'rssDeltaBytes'].filter(
        (key) =>
          !Number.isFinite(row.result[key]) ||
          row.result[key] < 0 ||
          row.result[key] > (key === 'rssDeltaBytes' ? 67108864 : 8388608)
      )
      if (fields.length)
        failures.push({ arm: row.label, round: row.round, fields, expected: false })
    }
  const judge = (arm, fields, expect) => {
    if (expect) {
      assert(
        expect.every((field) => fields.includes(field)),
        `the ${arm} positive control is ineffective`
      )
      failures.push({ arm, fields, expected: true })
    } else if (fields.length) failures.push({ arm, fields, expected: false })
  }
  for (const { arm, against, expect } of job.compare ?? []) {
    const reference = medians(against, keys)
    for (const key of [...job.cost, ...job.latency]) positive(reference[key])
    judge(
      arm,
      gateFailures(job.mode, { telemetry: job.telemetry[arm], ...medians(arm, keys) }, reference),
      expect
    )
  }
  for (const { arm, expect } of job.absolute ?? [])
    judge(arm, gateFailures('payload', medians(arm, keys), {}), expect)
  if (job.memory)
    for (const { round, incrementBytes } of memoryPairs(rows, declared.n))
      if (incrementBytes > 8388608)
        failures.push({
          arm: 'active',
          fields: ['incrementalRequestHeapExternalBytes'],
          round,
          expected: false,
        })
  return {
    disposition: failures.length ? 'gate-failed' : 'pass',
    exit: failures.length ? 1 : 0,
    failures,
    noise: measured,
  }
}

/** Appends one line durably: exclusive-or-append open without following links, fsync before return.
 * @param {string} file absolute path @param {string} line text without newline @returns {void}
 */
const appendDurable = (file, line) => {
  const fd = fs.openSync(
    file,
    fs.constants.O_WRONLY | fs.constants.O_APPEND | fs.constants.O_CREAT | NOFOLLOW,
    0o600
  )
  try {
    fs.writeSync(fd, `${line}\n`)
    fs.fsyncSync(fd)
  } finally {
    fs.closeSync(fd)
  }
}

/** Runs and re-verifies one balanced session of child workers under the inherited scheduler. */
class TelemetrySession {
  #manifest
  #directory
  #io
  #job
  #declared
  #meta = null
  #completed = 0

  /** @param {{manifest:object,directory:string,io?:object}} options immutable manifest, a not yet existing
   * absolute evidence directory and private test hooks (`spawnSync`, `verifyWorker`, `parse`, `now`)
   * @throws {AssertionError|UsageError} for an invalid manifest or an unavailable required capability
   */
  constructor({ manifest, directory, io = {} }) {
    this.#io = {
      spawnSync: io.spawnSync ?? spawnSync,
      verifyWorker: io.verifyWorker ?? verifyWorker,
      parse: io.parse ?? parseWorkerArgs,
      now: io.now ?? Date.now,
    }
    validateManifest(manifest, this.#io.parse)
    assert(path.isAbsolute(directory))
    this.#manifest = manifest
    this.#directory = directory
    this.#job = JOBS[manifest.job]
    this.#declared = schedule(this.#job.roles, this.#job.reference, this.#job.runs)
  }

  /** @param {string} name file under the evidence directory @returns {string} absolute path */
  #file(name) {
    return path.join(this.#directory, name)
  }

  /** @param {string} runId observation id @param {object} arm manifest arm
   * @param {string} manifestSha256 frozen manifest digest @returns {object} declaration
   */
  #declaration(runId, arm, manifestSha256) {
    const parsed = this.#io.parse(arm.argv.map(String))
    return {
      runId,
      tree: arm.tree,
      driverSha256: this.#manifest.driver.sha256,
      manifestSha256,
      configSha256: arm.configSha256,
      mode: parsed.mode,
      control: parsed.control,
      config: parsed.config,
      root: arm.root,
      codecs: this.#manifest.codecs,
    }
  }

  /** Spawns one worker with a hard bound, then requires closure, a final JSON line and verified sidecars.
   * @param {object} arm manifest arm @param {string} label observation label (A/A copy included)
   * @param {number} ordinal observation ordinal @param {string} manifestSha256 manifest hash
   * @returns {Object<string,number>} the numeric projection, after the full envelope was persisted
   * @throws {Error|CodecUnavailableError} with stderr for any non-clean child outcome
   */
  #child(arm, label, ordinal, manifestSha256) {
    const runId = `${this.#manifest.runId}-${String(ordinal).padStart(4, '0')}`
    const workerDirectory = makeExclusiveDirectory(this.#file(`workers/${runId}`))
    const env = { ...process.env }
    for (const name of [
      'NODE_OPTIONS',
      'KAFKAJS_LOG_LEVEL',
      'KAFKAJS_DEBUG_PROTOCOL_BUFFERS',
      'KAFKAJS_DEBUG_EXTENDED_PROTOCOL_BUFFERS',
    ])
      delete env[name]
    Object.assign(env, {
      TELEMETRY_BENCH_MANIFEST: this.#file('manifest.json'),
      TELEMETRY_BENCH_RUN: runId,
      TELEMETRY_BENCH_OUTPUT: workerDirectory,
      TELEMETRY_BENCH_ROOT: arm.root,
    })
    const child = this.#io.spawnSync(
      'timeout',
      [
        '-k',
        '5s',
        '600s',
        this.#manifest.node.binary,
        '--expose-gc',
        this.#manifest.driver.path,
        ...arm.argv.map(String),
      ],
      { cwd: arm.root, env, encoding: 'utf8', timeout: 610000, maxBuffer: 64 * MIB }
    )
    this.#meta = { runId, disposition: 'error' }
    fs.writeFileSync(this.#file(`stderr/${runId}.txt`), child.stderr ?? '', {
      flag: 'wx',
      mode: 0o600,
    })
    if (child.error) throw new Error(`${label} worker did not finish: ${child.error.message}`)
    if (child.status === 4) {
      if (this.#manifest.optionalCodec === 'zstd' && this.#manifest.codecs.zstd === false)
        throw new CodecUnavailableError(`${label} reported the declared unavailable ZSTD codec`)
      throw new Error(`${label} required codec prerequisite failed`)
    }
    if (child.status !== 0 || child.signal)
      throw new Error(`${label} failed (${child.status ?? child.signal}): ${child.stderr}`)
    const lines = child.stdout.trim().split('\n')
    const envelope = JSON.parse(lines[lines.length - 1])
    const bytes = Buffer.from(JSON.stringify(envelope))
    fs.writeFileSync(this.#file(`envelopes/${runId}.json`), bytes, { flag: 'wx', mode: 0o600 })
    const derived = this.#io.verifyWorker(
      envelope,
      this.#declaration(runId, arm, manifestSha256),
      workerDirectory
    )
    this.#meta = {
      runId,
      disposition: 'valid',
      envelopeSha256: sha256(bytes),
      derived: derived ?? {},
    }
    return numericProjection(envelope)
  }

  /** Executes exactly m*(n+1) children through the unchanged scheduler and seals the receipt.
   * @returns {{exit:number,disposition:string,failures:object[],receipt:object}} verdict; 0 pass, 1 gate or
   * control failure, 3 void
   * @throws {Error|AggregateError} child, verification or write failure; cleanup errors are kept beside it
   */
  run() {
    verifyProvenance(this.#manifest)
    makeExclusiveDirectory(this.#directory)
    for (const name of ['workers', 'envelopes', 'stderr']) makeExclusiveDirectory(this.#file(name))
    const manifestBytes = Buffer.from(JSON.stringify(this.#manifest))
    fs.writeFileSync(this.#file('manifest.json'), manifestBytes, { flag: 'wx', mode: 0o600 })
    const manifestSha256 = sha256(manifestBytes)
    const total = this.#declared.rows.length
    const arms = this.#manifest.arms.map((arm) => ({
      label: arm.label,
      run: (reportAs = arm.label) => {
        const ordinal = this.#completed
        appendDurable(
          this.#file('journal.jsonl'),
          JSON.stringify({
            job: this.#manifest.job,
            ordinal,
            label: reportAs,
            completed: this.#completed,
            total,
            deadline: this.#io.now() + 610000,
            disposition: 'started',
          })
        )
        return this.#child(arm, reportAs, ordinal, manifestSha256)
      },
    }))
    try {
      const rows = collectSession(this.#declared, arms, (row) => {
        this.#completed += 1
        const meta = this.#meta
        appendDurable(this.#file('raw.jsonl'), JSON.stringify({ ...row, ...meta }))
        appendDurable(
          this.#file('journal.jsonl'),
          JSON.stringify({
            job: this.#manifest.job,
            round: row.round,
            position: row.position,
            label: row.label,
            completed: this.#completed,
            total,
            deadline: null,
            disposition: meta.disposition,
          })
        )
      })
      const raw = this.#readRaw()
      return this.#conclude(
        rows.map((row, i) => ({ ...row, ...raw[i] })),
        manifestSha256
      )
    } catch (primary) {
      try {
        appendDurable(
          this.#file('journal.jsonl'),
          JSON.stringify({
            job: this.#manifest.job,
            completed: this.#completed,
            total,
            disposition: 'error',
            message: primary.message,
          })
        )
      } catch (cleanup) {
        throw new AggregateError(
          [primary, cleanup],
          'session failed and its failure record also failed'
        )
      }
      throw primary
    }
  }

  /** @returns {object[]} the durable raw rows, in order */
  #readRaw() {
    return fs
      .readFileSync(this.#file('raw.jsonl'), 'utf8')
      .split('\n')
      .filter(Boolean)
      .map((line) => JSON.parse(line))
  }

  /** @param {object[]} rows observations with envelope metadata @param {string} manifestSha256 manifest hash
   * @returns {object} verdict and the written receipt
   */
  #conclude(rows, manifestSha256) {
    let chain = ''
    for (const row of rows)
      chain = sha256(`${chain}|${row.envelopeSha256}|${row.round}/${row.position}/${row.label}`)
    const verdict = evaluate(this.#manifest.job, this.#declared, rows)
    const receipt = {
      schemaVersion: 1,
      manifestSha256,
      scheduleSha256: this.#declared.hash,
      count: rows.length,
      chain,
      disposition: verdict.disposition,
      exit: verdict.exit,
      failures: verdict.failures,
    }
    fs.writeFileSync(this.#file('receipt.json'), JSON.stringify(receipt), {
      flag: 'wx',
      mode: 0o600,
    })
    return {
      exit: verdict.exit,
      disposition: verdict.disposition,
      failures: verdict.failures,
      receipt,
    }
  }

  /** Rebuilds the schedule and rereads every envelope, hash and sidecar; starts no worker.
   * @param {{chain?:string,count?:number}} [frozen] terminal chain value and count the controller froze
   * @returns {{exit:number,disposition:string,failures:object[],receipt:object}} the recomputed verdict
   * @throws {AssertionError} for a removed, reordered, duplicated or altered observation, a changed
   * manifest, driver, configuration, tree or control, a reused worker, or a frozen value mismatch
   */
  verify(frozen = {}) {
    verifyProvenance(this.#manifest)
    const manifestBytes = fs.readFileSync(this.#file('manifest.json'))
    const manifestSha256 = sha256(manifestBytes)
    const saved = JSON.parse(manifestBytes.toString('utf8'))
    assert.deepEqual(saved, this.#manifest, 'the saved manifest differs from the declared one')
    const receipt = JSON.parse(fs.readFileSync(this.#file('receipt.json'), 'utf8'))
    assert.equal(receipt.manifestSha256, manifestSha256)
    const raw = this.#readRaw()
    validateSchedule(this.#declared, raw)
    assert.equal(new Set(raw.map(({ runId }) => runId)).size, raw.length, 'a worker run was reused')
    const rows = raw.map((row) => {
      const arm = this.#manifest.arms.find(({ label }) => label === row.label.replace(/~AA$/, ''))
      const envelopeFile = this.#file(`envelopes/${row.runId}.json`)
      const bytes = fs.readFileSync(envelopeFile)
      assert.equal(sha256(bytes), row.envelopeSha256)
      const envelope = JSON.parse(bytes.toString('utf8'))
      const derived = this.#io.verifyWorker(
        envelope,
        this.#declaration(row.runId, arm, manifestSha256),
        this.#file(`workers/${row.runId}`)
      )
      assert.deepEqual(numericProjection(envelope), row.result)
      return { ...row, derived: derived ?? {} }
    })
    let chain = ''
    for (const row of rows)
      chain = sha256(`${chain}|${row.envelopeSha256}|${row.round}/${row.position}/${row.label}`)
    assert.equal(chain, receipt.chain)
    assert.equal(rows.length, receipt.count)
    if (frozen.chain !== undefined) assert.equal(chain, frozen.chain)
    if (frozen.count !== undefined) assert.equal(rows.length, frozen.count)
    const verdict = evaluate(this.#manifest.job, this.#declared, rows)
    assert.equal(verdict.disposition, receipt.disposition)
    return {
      exit: verdict.exit,
      disposition: verdict.disposition,
      failures: verdict.failures,
      receipt,
    }
  }
}

/** Builds the live delivery fixture from the frozen candidate root: public clients only, a unique topic with
 * six partitions and RF3/minISR2, and (active arm) the exact client-metrics resource, all removed after.
 * @param {string} root frozen source root @param {{brokers:string[],active:boolean}} spec guard-provided brokers
 * @param {string} runId unique observation id @param {Function} [load] module loader (tests only)
 * @returns {object} fixture for `DeliveryWorkload`
 */
const liveFixture = (root, spec, runId, load = require) => {
  const { Kafka, logLevel } = load(path.join(root, 'index.js'))
  const clientId = `tbench-${runId}`
  const topic = `tbench-${runId}`
  const push = fs.existsSync(path.join(root, 'src/telemetry/metrics.js'))
  const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
  const nodes = new Map()
  return {
    topic,
    clientId,
    groupId: `${clientId}-group`,
    auditGroupId: `${clientId}-audit`,
    supportsMetricsPush: push,
    now: () => performance.now(),
    wait,
    kafka: (options) =>
      new Kafka({ brokers: spec.brokers, logLevel: logLevel.NOTHING, ...options }),
    socketFactory: () => load(path.join(root, 'src/network/socketFactory'))(),
    exports: (options) =>
      load(path.join(root, 'testHelpers/telemetryFixtures')).readExports(options),
    nodeFor: ({ host, port }) => {
      const node = nodes.get(`${host}:${port}`)
      assert(node, 'telemetry socket has no broker-node identity')
      return node
    },
    provision: async (admin, work) => {
      assert(!(await admin.listTopics()).includes(topic), 'owned topic must be initially absent')
      let primary = null
      try {
        await admin.createTopics({
          waitForLeaders: true,
          topics: [
            {
              topic,
              numPartitions: 6,
              replicationFactor: 3,
              configEntries: [{ name: 'min.insync.replicas', value: '2' }],
            },
          ],
        })
        const created = await admin.fetchTopicMetadata({ topics: [topic] })
        assert.equal(created.topics[0].partitions.length, 6)
        const topology = await admin.describeCluster()
        for (const broker of topology.brokers)
          nodes.set(`${broker.host}:${broker.port}`, String(broker.nodeId))
        if (spec.active) {
          const { withTelemetryResource } = load(path.join(root, 'testHelpers/telemetryFixtures'))
          const entries = {
            metrics: 'org.apache.kafka.',
            'interval.ms': '1000',
            match: `client_id=^(${clientId}-measured)$`,
          }
          await withTelemetryResource(admin, { name: `tbench-${runId}`, entries }, work)
        } else await work()
      } catch (error) {
        primary = error
      }
      try {
        await admin.deleteTopics({ topics: [topic] })
        const left = await admin.listTopics()
        assert(!left.includes(topic), 'owned topic deletion was not observed')
      } catch (cleanup) {
        if (primary)
          throw new AggregateError([primary, cleanup], 'delivery work and cleanup failed')
        throw cleanup
      }
      if (primary) throw primary
    },
  }
}

/** Runs one worker invocation: workload, awaited cleanup, then a single JSON envelope as the last stdout line.
 * @param {{mode:string,control:number,config:object}} parsed validated worker arguments
 * @param {string[]} argv the original worker arguments @param {{log:Function,error:Function}} io streams
 * @param {object} env process environment @returns {Promise<number>} exit code
 */
const runWorker = async (parsed, argv, io, env) => {
  const { mode, control, config } = parsed
  const manifestPath = env.TELEMETRY_BENCH_MANIFEST
  const manifest = manifestPath ? JSON.parse(fs.readFileSync(manifestPath, 'utf8')) : null
  const manifestSha256 = manifestPath ? sha256(fs.readFileSync(manifestPath)) : sha256('manual')
  if (manifest) validateManifest(manifest)
  const root = env.TELEMETRY_BENCH_ROOT ?? path.resolve(__dirname, '../..')
  const runId = env.TELEMETRY_BENCH_RUN ?? `manual-${process.pid}`
  const output =
    env.TELEMETRY_BENCH_OUTPUT ?? fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-bench-'))
  const tree = manifest ? manifest.arms.find((arm) => arm.root === root)?.tree : 'manual'
  assert(tree, 'the worker root is not an arm of the manifest')
  const evidence = new EvidenceWriter(output, {
    identity: { runId, manifestSha256 },
    maxBytes: (mode === 'payload' ? 128 : 256) * MIB,
  })
  const build = {
    requests: () => new RequestWorkload({ root, config, control, evidence }),
    payload: () => new PayloadWorkload({ root, config, control, evidence }),
    delivery: () => {
      if (!manifest?.fixture)
        throw new UsageError('delivery needs a session manifest with a fixture')
      const fixture = liveFixture(
        root,
        { ...manifest.fixture, active: config.telemetry === 'active' },
        runId
      )
      return new DeliveryWorkload({ root, config, control, evidence, fixture })
    },
  }
  const workload = build[mode]()
  const watchdog = new Watchdog()
  watchdog.start(mode, () => ({ runId }))
  let result
  const errors = []
  try {
    result = await workload.run()
  } catch (error) {
    errors.push(error)
  } finally {
    watchdog.stop()
  }
  let clean
  try {
    clean = await workload.close()
  } catch (error) {
    errors.push(error)
  }
  if (errors.length) {
    for (const error of errors) io.error(error.stack ?? String(error))
    return manifest?.optionalCodec === 'zstd' &&
      manifest.codecs.zstd === false &&
      mode === 'payload' &&
      config.codec === 'zstd' &&
      errors.some((error) => error instanceof CodecUnavailableError)
      ? 4
      : 1
  }
  io.log(
    JSON.stringify({
      schemaVersion: 1,
      runId,
      tree,
      driverSha256: sha256(fs.readFileSync(__filename)),
      manifestSha256,
      configSha256: digest({ mode, argv }),
      ...result,
      mode,
      control,
      artifacts: evidence.artifacts(),
      clean,
    })
  )
  return 0
}

/** The command line. 0 pass, 1 correctness/gate/child error, 2 usage, 3 void, 4 declared optional ZSTD unavailable.
 * It sets nothing global: the caller assigns `process.exitCode` after this settles.
 * @param {string[]} argv `process.argv.slice(2)`
 * @param {{log:Function,error:Function,env?:object}} [io] output functions (tests may inject `env` and more)
 * @returns {Promise<number>} the exit code
 */
async function main(argv, io = console) {
  let parsed
  try {
    parsed = parseArgs(argv)
  } catch (error) {
    if (!(error instanceof UsageError)) throw error
    io.error(error.message)
    return 2
  }
  try {
    if (parsed.kind === 'worker') return await runWorker(parsed, argv, io, io.env ?? process.env)
    const manifestPath =
      parsed.kind === 'session' ? parsed.path : path.join(parsed.path, 'manifest.json')
    const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'))
    const directory = parsed.kind === 'session' ? manifest.directory : parsed.path
    const session = new TelemetrySession({
      manifest,
      directory,
      io,
    })
    const verdict = parsed.kind === 'session' ? session.run() : session.verify()
    io.log(
      JSON.stringify({
        disposition: verdict.disposition,
        failures: verdict.failures,
        receipt: verdict.receipt,
      })
    )
    return verdict.exit
  } catch (error) {
    io.error(error.stack ?? String(error))
    if (error instanceof UsageError) return 2
    return error instanceof CodecUnavailableError ? 4 : 1
  }
}

module.exports = {
  JOBS,
  liveFixture,
  expectedSample,
  readChunks,
  deliveryEvidence,
  deliveryResult,
  cleanReport,
  digest,
  integer,
  p99,
  exactSet,
  schedule,
  validateSchedule,
  collectSession,
  noise,
  validateRequests,
  validateDelivery,
  decodePoints,
  decodeMetricsData,
  validatePayload,
  payloadWorkload,
  withinGates,
  gateFailures,
  validateEnvelope,
  samePathControl,
  median,
  Table,
  EvidenceWriter,
  readArtifact,
  verifyDirectory,
  makeExclusiveDirectory,
  MemoryProbe,
  SliceMonitor,
  Watchdog,
  signedDelta,
  rssDelta,
  CodecUnavailableError,
  UsageError,
  parseWorkerArgs,
  parseArgs,
  validateManifest,
  childCount,
  minimumBudget,
  memoryPairs,
  evaluate,
  numericProjection,
  verifyWorker,
  requestEvidence,
  expectedTuples,
  PayloadFixture,
  validatePayloadSample,
  publicOptions,
  assertPublicOptions,
  TelemetryPushProbe,
  ExportArchive,
  telemetryEvidence,
  makeValue,
  tailIntact,
  valueHash,
  RequestWorkload,
  PayloadWorkload,
  DeliveryWorkload,
  TelemetrySession,
  main,
}

if (require.main === module) {
  main(process.argv.slice(2)).then(
    (code) => {
      process.exitCode = code
    },
    (error) => {
      console.error(error)
      process.exitCode = 1
    }
  )
}
