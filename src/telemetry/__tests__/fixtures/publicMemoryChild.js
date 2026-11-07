'use strict'

/**
 * One single-client memory arm over an actual public client, run in its own `node --expose-gc`
 * process by `resources.spec.js`. It registers the maximum legal custom descriptors through the
 * public API, obtains the assigned UUID, closes and takes the matched disconnected reading, reopens
 * and lets the real native codecs push, then closes publicly once more and prints an explicit
 * disconnect receipt before its closed reading. Output is two JSON lines: the receipt, then the
 * readings. It never calls `process.exit`: a handle it leaves behind keeps the process alive, which
 * the parent fails.
 */
const path = require('node:path')
const Module = require('node:module')
const zlib = require('node:zlib')

const ROOT = path.join(__dirname, '..', '..', '..', '..')
const nativeResolve = Module._resolveFilename
// The helpers resolve this alias through jest's module map; a plain process needs the same one.
Module._resolveFilename = function (request, ...rest) {
  const target = request === 'testHelpers' ? path.join(ROOT, 'testHelpers', 'index.js') : request
  return nativeResolve.call(this, target, ...rest)
}

const { Kafka } = require(path.join(ROOT, 'index.js'))
const { createScriptedCluster } = require(path.join(ROOT, 'testHelpers', 'scriptedCluster.js'))
const { ResourceRoles } = require(path.join(ROOT, 'testHelpers', 'resourceProbe.js'))
const zstd = require('../../../protocol/message/compression/zstd')

// Opened before the observer starts, so the stdio pipes are never read as surviving handles.
if (process.stdout.fd < 0 || process.stderr.fd < 0) throw new Error('stdio is not open')

const ASSIGNED_UUID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const DESCRIPTORS = 128
const SAMPLE_MS = 20
const PHASE_PUSHES = 2
const WAIT_MS = 30000

/**
 * @param {() => boolean} predicate observed condition
 * @param {string} what names the condition in the failure
 * @returns {Promise<void>} resolves once the predicate holds
 * @throws {Error} when it does not hold within the bound
 */
const until = async (predicate, what) => {
  const deadline = Date.now() + WAIT_MS
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${what}`)
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
}

/**
 * Collects twice, a pause apart, and reads heap, external and RSS. `arrayBuffers` is part of
 * `external`, so `retained` does not add it again.
 * @returns {Promise<{heapUsed:number, external:number, arrayBuffers:number, retained:number,
 *   rss:number}>}
 */
const settled = async () => {
  globalThis.gc()
  await new Promise((resolve) => setTimeout(resolve, 100))
  globalThis.gc()
  const usage = process.memoryUsage()
  return {
    heapUsed: usage.heapUsed,
    external: usage.external,
    arrayBuffers: usage.arrayBuffers,
    retained: usage.heapUsed + usage.external,
    rss: usage.rss,
  }
}

/** @returns {number} the lifetime peak RSS of this process in bytes */
const lifetimePeak = () => process.resourceUsage().maxRSS * 1024

/** Reads the current RSS at least every 20 ms and keeps only the count and the maximum. */
class RssSampler {
  /** @returns {RssSampler} a stopped sampler */
  constructor() {
    this.count = 0
    this.peak = 0
    this.timer = null
  }

  /** @returns {void} takes a reading now and then every 20 ms */
  start() {
    this.take()
    this.timer = setInterval(() => this.take(), SAMPLE_MS)
  }

  /** @returns {void} takes one current reading */
  take() {
    this.count += 1
    this.peak = Math.max(this.peak, process.memoryUsage.rss())
  }

  /** @returns {void} takes a last reading and releases the timer */
  stop() {
    this.take()
    clearInterval(this.timer)
    this.timer = null
  }
}

/**
 * @param {number} index descriptor number
 * @returns {object} the largest descriptor the public fields allow: name 200, unit 16, eight
 *   63-byte keys and eight 256-byte values
 */
const descriptor = (index) => ({
  name: `com.example.resource.${index}`.padEnd(200, 'x'),
  unit: 'u'.repeat(16),
  type: 'sum',
  value: 9007199254740993n,
  attributes: Object.fromEntries(
    Array.from({ length: 8 }, (_, a) => [`a${a}`.padEnd(63, 'k'), 'x'.repeat(256)])
  ),
})

/**
 * Builds the actual public client of one kind over a scripted wire that answers the telemetry APIs.
 * @param {string} kind producer, consumer, admin or shareConsumer
 * @param {{codecs: number[], pushes: object[], first: Record<number, string>}} state what the
 *   wire answers and what it observed
 * @returns {{client: object, wire: object}} the public client and its wire
 */
const create = (kind, state) => {
  const advertise = { GetTelemetrySubscriptions: [0, 0], PushTelemetry: [0, 0] }
  const options = { enableMetricsPush: true }
  let wire
  let client
  if (kind === 'shareConsumer') {
    const { shareClient } = require(path.join(ROOT, 'testHelpers', 'shareHarness.js'))
    const harness = shareClient({
      advertise,
      kafka: options,
      share: { groupId: 'telemetry-g', maxWaitTimeInMs: 5 },
      profile: 'share',
    })
    wire = harness.cluster
    client = harness.consumer
  } else {
    wire = createScriptedCluster({
      nodes: [{ id: 1, host: 'a', port: 9092 }],
      profile: 'default',
      advertise,
    })
    const kafka = new Kafka(wire.kafkaOptions(options))
    client =
      kind === 'producer'
        ? kafka.producer({ idempotent: false })
        : kind === 'admin'
          ? kafka.admin()
          : kafka.consumer({ groupId: 'telemetry-g' })
  }
  wire.answer('GetTelemetrySubscriptions', (fields) => ({
    clientInstanceId: fields.clientInstanceId ?? ASSIGNED_UUID,
    subscriptionId: 7,
    acceptedCompressionTypes: state.codecs,
    pushIntervalMs: 100,
    telemetryMaxBytes: 1048576,
    deltaTemporality: true,
    requestedMetrics: ['*'],
  }))
  wire.answer('PushTelemetry', (fields) => {
    const type = fields.compressionType
    state.pushes.push({
      compressionType: type,
      terminating: fields.terminating,
      transmittedBytes: fields.metrics.length / 2,
    })
    if (type !== 0 && state.first[type] === undefined) state.first[type] = fields.metrics
    // The wire logs every request it decodes; a payload this large would otherwise pile up here.
    wire.log.length = 0
    return {}
  })
  return { client, wire }
}

/**
 * @param {string} kind public client kind
 * @param {object} client its public client
 * @param {number|undefined} timeout the public close budget; the admin client takes none
 * @returns {Promise<void>} the public close has resolved
 */
const closeClient = (kind, client, timeout) =>
  kind === 'admin' ? client.disconnect() : client.disconnect({ timeout })

/**
 * Decompresses the first push of a codec to learn its raw size, after every reading was taken.
 * @param {number} type compression type 1 or 4
 * @param {string} hex the transmitted bytes
 * @returns {Promise<number>} raw byte count
 */
const rawBytesOf = async (type, hex) => {
  const buffer = Buffer.from(hex, 'hex')
  return type === 4 ? (await zstd.decompress(buffer)).length : zlib.gunzipSync(buffer).length
}

/** @type {{kind: string, client: object}|null} the client a failed run still has to close */
let active = null

/**
 * Drives the client of one kind through registration, UUID, close, reopen, native pushes and a
 * final public close, reading the resource roles at the matched and the closed points.
 * @param {string} kind public client kind
 * @param {ResourceRoles} roles the started observer, already inside the target scope
 * @param {{codecs: number[], pushes: object[], first: Record<number, string>}} state wire state
 * @returns {Promise<object>} the readings that depend on the client
 */
const drive = async (kind, roles, state) => {
  const phases = [
    { name: 'gzip', type: 1 },
    ...(zstd.isSupported ? [{ name: 'zstd', type: 4 }] : []),
  ]
  const { client, wire } = create(kind, state)
  active = { kind, client }
  for (let index = 0; index < DESCRIPTORS; index++)
    client.registerMetricForSubscription(descriptor(index))
  await client.connect()
  const uuid = await client.clientInstanceId({ timeout: WAIT_MS })
  await closeClient(kind, client, 0)
  const matched = await settled()
  const matchedResources = roles.read()

  const sampler = new RssSampler()
  roles.run('fixture', () => sampler.start())
  try {
    for (const { type } of phases) {
      state.codecs = [type, 0]
      await client.connect()
      await until(
        () =>
          state.pushes.filter((push) => push.compressionType === type && !push.terminating)
            .length >= PHASE_PUSHES,
        `${type === 4 ? 'zstd' : 'gzip'} pushes`
      )
      if (type !== phases.at(-1).type) await closeClient(kind, client, 0)
    }
    const held = await settled()
    const uuidHeld = await client.clientInstanceId({ timeout: 0 })
    await closeClient(kind, client, 3000)
    process.stdout.write(`${JSON.stringify({ receipt: 'disconnect', kind, uuid })}\n`)
    sampler.stop()
    wire.log.length = 0
    const closed = await settled()
    const builds = []
    for (const { name, type } of phases) {
      const pushes = state.pushes.filter((push) => push.compressionType === type)
      builds.push({
        codec: name,
        compressionType: type,
        pushes: pushes.length,
        transmittedBytes: pushes[0].transmittedBytes,
        rawBytes: await rawBytesOf(type, state.first[type]),
      })
    }
    return {
      uuid,
      uuidHeld,
      matched,
      held,
      closed,
      builds,
      matchedResources,
      closedResources: roles.read(),
      ancestry: roles.ancestry(),
      rssSamples: sampler.count,
      rssPeakSample: sampler.peak,
    }
  } finally {
    if (sampler.timer !== null) sampler.stop()
  }
}

/**
 * Runs the whole arm for one kind.
 * @param {string} kind public client kind
 * @returns {Promise<object>} serializable readings
 */
const measure = async (kind) => {
  const roles = new ResourceRoles({ roles: ['target', 'fixture'] }).start()
  try {
    const baseline = await settled()
    const baselinePeakRss = lifetimePeak()
    const baselineResources = roles.read()
    const state = { codecs: [0], pushes: [], first: {} }
    const driven = await roles.run('target', () => drive(kind, roles, state))
    return {
      kind,
      mode: 'public',
      zstdSupported: zstd.isSupported,
      descriptors: DESCRIPTORS,
      baseline,
      baselinePeakRss,
      baselineResources,
      ...driven,
      peakRss: lifetimePeak(),
    }
  } finally {
    roles.stop()
  }
}

const [kind] = process.argv.slice(2)
measure(kind).then(
  (readings) => {
    process.stdout.write(`${JSON.stringify({ readings })}\n`)
  },
  (error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
    return active ? closeClient(active.kind, active.client, 0).catch(() => {}) : undefined
  }
)
