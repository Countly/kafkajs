'use strict'

/**
 * The fixed foreground workload of the single-client memory arms. The same public kind, ordinary
 * options, scripted core protocol and bounded input population run twice in one process: with
 * telemetry disabled as the control, and with it enabled as the arm under measurement. Both run
 * the same fixed catalogue of ordinary operations at every lifecycle, so any difference between
 * them is telemetry.
 *
 * Loading this module imports the application and the helpers an arm needs, including the
 * share-only helpers, so an arm can finish all loading before its control.
 */
const path = require('node:path')
const controlDiagnostics = require('./controlDiagnostics')

const ROOT = path.join(__dirname, '..', '..', '..', '..')
const { Kafka } = require(path.join(ROOT, 'index.js'))
const { createScriptedCluster, groupScript, producerScript } = require(
  path.join(ROOT, 'testHelpers', 'scriptedCluster.js')
)
const share = require(path.join(ROOT, 'testHelpers', 'shareHarness.js'))
const { TOPIC } = require('../../../consumer/__tests__/fixtures/fetchHarness')

const {
  OPERATION,
  OPS_PER_LIFECYCLE,
  RECORD_BYTES,
  foregroundCatalogue,
  maximumDescriptor,
  registryEntry,
  registryFingerprint,
} = require(path.join(ROOT, 'scripts', 'e2e', 'telemetry', 'resourceArtifact.js'))

/**
 * The finite protocol population this wire advertises, including both discovery attempts and
 * enabled-only APIs. Loading both directions does not build a request, collect a sample or run a
 * codec. Plain children do not use Jest's eager protocol imports.
 */
const MEMORY_PROTOCOLS = Object.freeze([
  ['apiVersions', 5],
  ['apiVersions', 4],
  ['metadata', 13],
  ['findCoordinator', 6],
  ['listOffsets', 9],
  ['fetch', 18],
  ['offsetForLeaderEpoch', 4],
  ['joinGroup', 8],
  ['syncGroup', 5],
  ['heartbeat', 4],
  ['leaveGroup', 5],
  ['offsetFetch', 9],
  ['offsetCommit', 9],
  ['initProducerId', 5],
  ['produce', 13],
  ['shareGroupHeartbeat', 1],
  ['shareGroupDescribe', 1],
  ['shareFetch', 2],
  ['shareAcknowledge', 2],
  ['describeShareGroupOffsets', 1],
  ['alterShareGroupOffsets', 0],
  ['deleteShareGroupOffsets', 0],
  ['getTelemetrySubscriptions', 0],
  ['pushTelemetry', 0],
])

/**
 * Loads the same finite core/telemetry wire and production modules before either mode constructs
 * a client. The discovery fallback's v0 response is a separate lazy production import.
 * @param {(file: string) => *} [load] module loader, replaceable for a catalogue unit test
 * @returns {void} all modules are loaded, without invoking protocol or native operations
 * @throws {Error} when a declared module cannot load; a missing fixture cannot certify a run
 */
const preloadProtocols = (load = require) => {
  for (const [api, version] of MEMORY_PROTOCOLS) {
    for (const direction of ['request', 'response'])
      load(path.join(ROOT, 'src', 'protocol', 'requests', api, `v${version}`, `${direction}.js`))
    load(path.join(ROOT, 'testHelpers', 'wire', `${api}V${version}.js`))
  }
  load(path.join(ROOT, 'src', 'protocol', 'requests', 'apiVersions', 'v0', 'response.js'))
}

preloadProtocols()

/** The UUID the scripted broker assigns, and the sizes of the maximum descriptor population. */
const MEMORY_UUID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const DESCRIPTORS = 128
const DESCRIPTOR_BYTES = 2768
const WAIT_MS = 30000

/** The live bounds of the scripted wire the arms run over. */
const WIRE_BOUNDS = Object.freeze({ maxLiveSockets: 8, maxLiveRequests: 64, maxHeldDials: 4 })

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
 * @param {Promise<*>} promise work that must settle
 * @param {string} what names the work in the failure
 * @returns {Promise<*>} its own outcome, or a rejection once the bound passes
 */
const within = (promise, what) => {
  let timer
  const limit = new Promise((resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`Timed out waiting for ${what}`)), WAIT_MS)
  })
  return Promise.race([promise, limit]).finally(() => clearTimeout(timer))
}

/**
 * @param {number} index descriptor number
 * @returns {object} the largest descriptor the public fields allow, with a Sum value above 2^53
 */
const descriptor = (index) => ({ ...maximumDescriptor(index), value: 9007199254740993n })

/**
 * The seeded controls: a deliberate leak through a product-reachable root, to prove the memory
 * gates see it. The bytes hang from the product's own exported `Kafka` class, so they outlive any
 * client and its release, exactly as a leak in a product singleton would.
 */
const SEED = Object.freeze({
  root: Symbol.for('resource.seed.root'),
  modes: Object.freeze(['close', 'close-warmup', 'warmup-only']),
  closeBytes: 131072,
  warmupBytes: 1048577,
  warmupAtClose: 2,
})

/**
 * Leaks on the public closes of one pass. `close` leaks `SEED.closeBytes` on every close in the
 * measured pass (and in the warm-up too for `close-warmup`); `warmup-only` leaks
 * `SEED.warmupBytes` once, at the first close after the matched position of the warm-up, and
 * nothing in the measured pass.
 */
class ProductSeed {
  /**
   * @param {'close'|'close-warmup'|'warmup-only'} mode which passes leak
   * @param {boolean} warm whether this is the warm-up pass
   * @returns {ProductSeed|null} the seed of this pass, or null when this pass does not leak
   * @throws {Error} for an unknown mode
   */
  static of(mode, warm) {
    if (!mode) return null
    if (!SEED.modes.includes(mode)) throw new Error(`Unknown seed mode ${mode}`)
    const leaks =
      mode === 'close-warmup' || (mode === 'close' && !warm) || (mode === 'warmup-only' && warm)
    return leaks ? new ProductSeed(mode) : null
  }

  /** @param {string} mode the seed mode @returns {ProductSeed} a seed that has leaked nothing */
  constructor(mode) {
    this.mode = mode
    this.closes = 0
    this.bytes = 0
    this.matchedBytes = null
  }

  /** @returns {void} leaks what this mode leaks at one public close */
  close() {
    this.closes += 1
    const bytes =
      this.mode === 'warmup-only'
        ? this.closes === SEED.warmupAtClose
          ? SEED.warmupBytes
          : 0
        : SEED.closeBytes
    if (bytes === 0) return
    Kafka[SEED.root] ??= []
    Kafka[SEED.root].push(Buffer.alloc(bytes, 0x5a))
    this.bytes += bytes
  }

  /** @returns {void} records how much leaked up to the matched position */
  matched() {
    this.matchedBytes = this.bytes
  }

  /**
   * @returns {{mode: string, closes: number, bytes: number, afterMatched: number}} what this pass
   *   leaked in all and after the matched position
   */
  summary() {
    const { mode, closes, bytes } = this
    return { mode, closes, bytes, afterMatched: bytes - (this.matchedBytes ?? 0) }
  }

  /** @returns {void} drops every leaked buffer from the product root */
  static release() {
    delete Kafka[SEED.root]
  }
}

/**
 * One public client of a kind over a bounded scripted wire that answers the core protocol and the
 * telemetry APIs. Telemetry is enabled or disabled by the constructor; the wire and the
 * operations are the same either way. It owns the client, its wire and the fixture timers, and
 * keeps only counts and the first accepted payload of each codec.
 */
class MemoryForeground {
  /**
   * @param {string} kind producer, consumer, admin or shareConsumer
   * @param {{enabled: boolean, seed?: ProductSeed|null, witnesses?: object|null}} options whether `enableMetricsPush` is
   *   set, and the seeded control's leak on every public close of this client, normally none
   * @param {object|null} [options.witnesses] diagnostic-only weak inventory of build inputs
   * @returns {MemoryForeground} an object with no client yet
   */
  constructor(kind, { enabled, seed = null, witnesses = null }) {
    this.kind = kind
    this.enabled = enabled
    this.seed = seed
    if (witnesses) this.witnesses = witnesses
    this.codecs = [0]
    this.timers = new Set()
    this.ops = []
    this.opened = 0
    this.joins = 0
    this.handled = null
    this.removers = []
    this.pushes = {}
    this.registry = []
    this.registered = []
    this.client = null
    this.wire = null
  }

  /**
   * Builds the client and the wire, and starts counting accepted ordinary Pushes by codec.
   * @returns {MemoryForeground} this object
   */
  build() {
    const advertise = { GetTelemetrySubscriptions: [0, 0], PushTelemetry: [0, 0] }
    const options = { enableMetricsPush: this.enabled }
    this.witnesses?.track('foreground:advertise', advertise)
    this.witnesses?.track('foreground:options', options)
    if (this.kind === 'shareConsumer') {
      this.harness = share.shareClient({
        advertise,
        kafka: options,
        share: { groupId: 'telemetry-g', maxWaitTimeInMs: 5 },
        profile: 'share',
      })
      this.wire = this.harness.cluster.bound(WIRE_BOUNDS)
      this.client = this.harness.consumer
    } else {
      this.wire = createScriptedCluster({
        nodes: [{ id: 1, host: 'a', port: 9092 }],
        profile: 'default',
        advertise,
        bounded: WIRE_BOUNDS,
      })
      const kafka = new Kafka(this.wire.kafkaOptions(options))
      this.witnesses?.track('foreground:kafka', kafka)
      if (this.kind === 'producer') this.client = kafka.producer({ idempotent: false })
      else if (this.kind === 'admin') this.client = kafka.admin()
      else
        this.client = kafka.consumer({
          groupId: 'telemetry-g',
          maxWaitTimeInMs: 20,
          heartbeatInterval: 100,
        })
      if (this.kind === 'consumer')
        groupScript(this.wire, { batches: 0, brokerAnswerTimers: this.timers })
      else producerScript(this.wire)
    }
    this.wire.answer('GetTelemetrySubscriptions', (fields) => ({
      clientInstanceId: fields.clientInstanceId ?? MEMORY_UUID,
      subscriptionId: 7,
      acceptedCompressionTypes: this.codecs,
      pushIntervalMs: 100,
      telemetryMaxBytes: 1048576,
      deltaTemporality: true,
      requestedMetrics: ['*'],
    }))
    this.wire.answer('PushTelemetry', () => ({}))
    this.removers.push(
      this.wire.watch((event, record) => {
        if (event !== 'answered' || record.api !== 'PushTelemetry' || record.fields.terminating)
          return
        const type = record.fields.compressionType
        const entry = (this.pushes[type] ??= { accepted: 0, transmittedBytes: 0, first: null })
        entry.accepted += 1
        if (entry.first === null && type !== 0) {
          entry.first = Buffer.from(record.fields.metrics, 'hex')
          entry.transmittedBytes = entry.first.length
        }
      })
    )
    if (this.kind === 'consumer')
      this.removers.push(this.client.on(this.client.events.GROUP_JOIN, () => (this.joins += 1)))
    this.witnesses?.track('foreground:built', this)
    return this
  }

  /**
   * Registers one descriptor through the public API and journals it only once that call returned.
   * @param {object} metric the descriptor
   * @returns {void}
   * @throws {Error} the public refusal, with nothing journaled
   */
  register(metric) {
    this.client.registerMetricForSubscription(metric)
    this.registry.push(registryEntry(metric))
    this.registered.push(metric)
  }

  /**
   * Fingerprints the population the production client holds, from its own answers. A new
   * identity must be refused at the registry limit; then every journaled descriptor is registered
   * again, and only those the client updated in place enter the fingerprint. At the limit, all of
   * them updating proves the client holds exactly the journaled population, so a descriptor
   * swapped for another of the same size leaves the fingerprint one short. Call it after a settled
   * reading, so the probe is not measured.
   * @param {string} uuid the assigned client instance id
   * @returns {{uuid: string, descriptors: number, charge: number, hash: string,
   *   overflow: string|null}} what the retained comparison must keep; `descriptors` counts the
   *   journaled descriptors the client updated in place
   */
  fingerprint(uuid) {
    const outcome = (metric) => {
      try {
        this.client.registerMetricForSubscription(metric)
        return 'updated'
      } catch (error) {
        return error.reason ?? 'refused'
      }
    }
    const overflow = outcome(descriptor(DESCRIPTORS))
    const present = this.registered.filter((metric) => outcome(metric) === 'updated')
    return {
      ...registryFingerprint(present.map(registryEntry), uuid),
      overflow: overflow === 'updated' ? null : overflow,
    }
  }

  /** @returns {number} accepted ordinary Pushes of a codec so far @param {number} type codec id */
  accepted(type) {
    return this.pushes[type]?.accepted ?? 0
  }

  /**
   * Connects the public client, starts what its kind needs and runs the fixed operations once.
   * @returns {Promise<void>} the lifecycle is open and has worked
   */
  async open() {
    await this.client.connect()
    await this.resume()
    await this.work()
  }

  /**
   * Starts the run a consumer or share consumer needs after a public connect; the producer and
   * the admin client have nothing to resume.
   * @returns {Promise<void>} the membership exists
   */
  async resume() {
    if (this.kind === 'consumer') {
      const before = this.joins
      await this.client.subscribe({ topic: TOPIC, fromBeginning: true })
      await this.client.run({ autoCommit: false, eachMessage: async () => {} })
      await until(() => this.joins > before, 'the group join')
    } else if (this.kind === 'shareConsumer') {
      await this.client.subscribe({ topics: ['topic-a'] })
      await this.client.run({
        eachMessage: async (payload) => {
          this.handled = payload.message.offset
        },
      })
    }
  }

  /**
   * Runs the fixed number of ordinary operations of this kind and records each outcome as scalars.
   * @returns {Promise<void>} every operation had its expected outcome
   * @throws {Error} when an operation fails or returns something other than its expected outcome
   */
  async work() {
    const lifecycle = this.opened++
    for (let index = 0; index < OPS_PER_LIFECYCLE; index++) {
      const ok = await this.operate(lifecycle * OPS_PER_LIFECYCLE + index)
      if (!ok) throw new Error(`The ${OPERATION[this.kind]} operation did not return as expected`)
      this.ops.push({ lifecycle, op: OPERATION[this.kind], bytes: RECORD_BYTES, ok })
    }
  }

  /**
   * @param {number} index operation number across the whole schedule
   * @returns {Promise<boolean>} whether the operation had its expected outcome
   */
  async operate(index) {
    if (this.kind === 'producer') {
      const result = await this.client.send({
        topic: TOPIC,
        messages: [{ value: Buffer.alloc(RECORD_BYTES, index) }],
      })
      return Array.isArray(result) && result.length === 1
    }
    if (this.kind === 'admin') {
      const result = await this.client.fetchTopicMetadata({ topics: [TOPIC] })
      return typeof result === 'object' && result !== null
    }
    if (this.kind === 'consumer') {
      await this.client.commitOffsets([{ topic: TOPIC, partition: 0, offset: String(index + 1) }])
      return true
    }
    const offset = 100 + index
    this.harness.script('ShareFetch', [
      share.fetchAnswer([
        share.part({
          topicId: share.A,
          partition: 0,
          acquire: [share.acquired(offset, offset)],
          records: share.recordsHex(offset, ['x'.repeat(RECORD_BYTES)]),
        }),
      ]),
    ])
    await until(() => this.handled === String(offset), 'the share handler')
    return true
  }

  /**
   * Starts the public close with the same budget for every kind, and returns its own promise, so
   * a caller can continue in its resolution; the scripted broker's answer timers are joined
   * separately by `drain`.
   * @param {number} timeout the public close budget, passed as `disconnect({ timeout })`
   * @returns {Promise<void>} the original public disconnect promise
   */
  close(timeout) {
    this.seed?.close()
    return this.client.disconnect({ timeout })
  }

  /**
   * @returns {Promise<void>} resolves once the scripted broker's answer timers have fired, so no
   *   fixture timer or unanswered request outlives a close
   */
  drain() {
    return until(() => this.timers.size === 0, 'the broker answer timers')
  }

  /**
   * @returns {{enabled:boolean, api71:number, api72:number, liveSockets:number,
   *   liveRequests:number, ops:object[]}} what the wire saw and the operations that ran; a
   *   disabled client must show no telemetry request
   */
  facts() {
    return {
      enabled: this.enabled,
      api71: this.wire.tally('GetTelemetrySubscriptions'),
      api72: this.wire.tally('PushTelemetry'),
      liveSockets: this.wire.sockets().length,
      liveRequests: this.wire.requests().length,
      ops: this.ops.map((op) => ({ ...op })),
    }
  }

  /**
   * Releases the listeners, the fixture timers and every reference to the client and its wire.
   * @returns {void}
   * @throws {Error} the first failure the scripted cluster itself recorded
   */
  release() {
    for (const remove of this.removers.splice(0)) remove()
    for (const timer of this.timers) clearTimeout(timer)
    this.timers.clear()
    const wire = this.wire
    this.client = null
    this.wire = null
    this.harness = null
    this.pushes = {}
    wire.assertClean()
  }
}

/**
 * Runs the disabled control of an arm: the same kind, wire and fixed operations as the enabled
 * client, over `lifecycles` public opens, with settled readings C0 after the first and C1 after
 * the last (both with the client connected and idle), then a public close, release of the client
 * and of everything it owned, and a settled reading once nothing of it remains. The control makes
 * no telemetry call; its facts say so.
 * @param {object} arm
 * @param {string} arm.kind public client kind
 * @param {number} arm.lifecycles how many times the client is opened and works
 * @param {import('../../../../testHelpers/processMemory').ProcessMemory} arm.memory the arm's meter
 * @param {import('../../../../testHelpers/resourceProbe').ResourceRoles} arm.roles the started roles
 * @param {import('../../../../testHelpers/processMemory').NativeJournal} arm.journal observing
 *   the native codecs, which must see no call
 * @param {MemoryForeground[]} arm.live the clients a failed run still has to close
 * @returns {Promise<{c0:object, c1:object, diagnostics:object, released:object, facts:object,
 *   releasedResources:object}>} the three settled readings, the control's facts and the roles
 *   still live after its release; diagnostics are separate post-reading counters, never verdict inputs
 */
const runControl = async ({ kind, lifecycles, memory, roles, journal, live }) => {
  const settled = () => roles.run('fixture', () => memory.settled())
  const turn = () => new Promise((resolve) => setImmediate(resolve))
  return roles.run('control', async () => {
    memory.mark('control:start')
    const client = new MemoryForeground(kind, { enabled: false }).build()
    live.push(client)
    await client.open()
    const c0 = await settled()
    const diagnostics = { c0: controlDiagnostics() }
    memory.mark('control:c0')
    for (let index = 1; index < lifecycles; index++) {
      await client.close(0)
      await client.drain()
      await client.open()
    }
    const c1 = await settled()
    diagnostics.c1 = controlDiagnostics()
    memory.mark('control:c1')
    const probe = await client.client.clientInstanceId({ timeout: 0 }).then(
      () => 'resolved',
      (error) => error.reason
    )
    await client.close(0)
    await client.drain()
    await turn()
    await turn()
    const facts = { ...client.facts(), nativeSubmissions: journal.rows.length, probe }
    client.release()
    live.splice(live.indexOf(client), 1)
    const released = await settled()
    memory.mark('control:released')
    return { c0, c1, diagnostics, released, facts, releasedResources: roles.read() }
  })
}

/**
 * Builds one current payload and one actually retired native completion in a separate async
 * scope. Only scalar proof leaves this scope: neither the successful Buffer nor the resolved
 * promises can remain in the caller's loop frame through its closed memory checkpoint.
 * @param {object} builder the persistent production payload builder
 * @param {object} snapshot the strict maximum collector sample
 * @param {{name:string,type:number}} codec the real codec to select
 * @param {() => void} retire advances the journal's lifecycle while the second job is pending
 * @param {object|null} [witnesses] diagnostic-only weak inventory of payload temporaries
 * @returns {Promise<object>} the byte/point/retirement facts, with no payload references
 * @throws {Error} preserves a failed native build
 */
const nativePair = async (builder, snapshot, codec, retire, witnesses = null) => {
  const options = {
    telemetryMaxBytes: 1048576,
    acceptedCompressionTypes: [codec.type, 0],
    isCurrent: () => true,
  }
  witnesses?.track('catalogue:native-options', options)
  const current = await builder.buildPayload(snapshot, options)
  witnesses?.track('catalogue:native-current', current)
  let valid = true
  const retiring = builder.buildPayload(snapshot, { ...options, isCurrent: () => valid })
  witnesses?.track('catalogue:native-retiring', retiring)
  retire()
  valid = false
  return {
    codec: codec.name,
    compressionType: current.compressionType,
    rawBytes: current.rawBytes,
    transmittedBytes: current.transmittedBytes,
    points: current.includedIdentities.length,
    retiredCompletion: (await retiring) === null,
  }
}

module.exports = {
  DESCRIPTORS,
  DESCRIPTOR_BYTES,
  MEMORY_UUID,
  MemoryForeground,
  ProductSeed,
  SEED,
  WAIT_MS,
  descriptor,
  foregroundCatalogue,
  runControl,
  nativePair,
  preloadProtocols,
  until,
  within,
}
