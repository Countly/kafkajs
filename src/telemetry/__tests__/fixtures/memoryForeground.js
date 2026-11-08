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

const ROOT = path.join(__dirname, '..', '..', '..', '..')
const { Kafka } = require(path.join(ROOT, 'index.js'))
const { createScriptedCluster, groupScript, producerScript } = require(
  path.join(ROOT, 'testHelpers', 'scriptedCluster.js')
)
const share = require(path.join(ROOT, 'testHelpers', 'shareHarness.js'))
const { TOPIC } = require('../../../consumer/__tests__/fixtures/fetchHarness')

const { OPERATION, OPS_PER_LIFECYCLE, RECORD_BYTES, foregroundCatalogue } = require(
  path.join(ROOT, 'scripts', 'e2e', 'telemetry', 'resourceArtifact.js')
)

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
 * One public client of a kind over a bounded scripted wire that answers the core protocol and the
 * telemetry APIs. Telemetry is enabled or disabled by the constructor; the wire and the
 * operations are the same either way. It owns the client, its wire and the fixture timers, and
 * keeps only counts and the first accepted payload of each codec.
 */
class MemoryForeground {
  /**
   * @param {string} kind producer, consumer, admin or shareConsumer
   * @param {{enabled: boolean}} options whether `enableMetricsPush` is set
   * @returns {MemoryForeground} an object with no client yet
   */
  constructor(kind, { enabled }) {
    this.kind = kind
    this.enabled = enabled
    this.codecs = [0]
    this.timers = new Set()
    this.ops = []
    this.opened = 0
    this.joins = 0
    this.handled = null
    this.removers = []
    this.pushes = {}
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
    return this
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
   * Closes the public client and waits for the scripted broker's own answer timers to fire, so no
   * fixture timer or unanswered request outlives the close.
   * @param {number|undefined} timeout the public close budget; the admin client takes none
   * @returns {Promise<void>} the public close has resolved and the broker side is quiet
   */
  async close(timeout) {
    if (this.kind === 'admin') await this.client.disconnect()
    else await this.client.disconnect({ timeout })
    await until(() => this.timers.size === 0, 'the broker answer timers')
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
 * @returns {Promise<{c0:object, c1:object, released:object, facts:object,
 *   releasedResources:object}>} the three settled readings, the control's facts and the roles
 *   still live after its release
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
    memory.mark('control:c0')
    for (let index = 1; index < lifecycles; index++) {
      await client.close(0)
      await client.open()
    }
    const c1 = await settled()
    memory.mark('control:c1')
    const probe = await client.client.clientInstanceId({ timeout: 0 }).then(
      () => 'resolved',
      (error) => error.reason
    )
    await client.close(0)
    await turn()
    await turn()
    const facts = { ...client.facts(), nativeSubmissions: journal.rows.length, probe }
    client.release()
    live.splice(live.indexOf(client), 1)
    const released = await settled()
    memory.mark('control:released')
    return { c0, c1, released, facts, releasedResources: roles.read() }
  })
}

module.exports = {
  DESCRIPTORS,
  DESCRIPTOR_BYTES,
  MEMORY_UUID,
  MemoryForeground,
  WAIT_MS,
  descriptor,
  foregroundCatalogue,
  runControl,
  until,
}
