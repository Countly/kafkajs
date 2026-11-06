const diagnosticsChannel = require('node:diagnostics_channel')
const { Kafka, logLevel } = require('../../../index')
const { createTopic, plainTextBrokers, secureRandom, writeEvidence } = require('testHelpers')
const { waitForCondition, withTimeout } = require('../../consumer/__tests__/fixtures/liveScenario')
const { runRecipe } = require('./fixtures/metricsRecipe')
const net = require('net')
const { isDeepStrictEqual } = require('util')
const { AlterConfigOpTypes, ConfigResourceTypes, ConfigSource } = require('../../../index')
const { wireVersionsOf } = require('testHelpers')
const {
  assertShareCleanup,
  setShareGroupConfigs,
  shareRecordIdentity,
  shareTopicName,
  verifierMember,
} = require('../../../testHelpers/shareFixtures')
const { readLog } = require('../../../testHelpers/rawLog')
const { createResourceProbe } = require('../../../testHelpers/resourceProbe')
const {
  NEVER,
  bounded,
  caseTimeout,
  liveCase,
  liveRun,
  pollTolerating,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')
const { CleanupStack, describeError } = require('../../producer/__tests__/fixtures/liveProbe')
const { FrameLog } = require('../../producer/__tests__/fixtures/liveWire')

// Gated: it needs a cluster. Run alone: `npm run test:e2e:diagnostics-channel`.
const describeChannelsLive = process.env.DIAGNOSTICS_CHANNEL_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'diagnostics-channel'
// Named so each wait says how long it is prepared to be wrong for.
const SCENARIO_BUDGET_MS = 60000
const DISCONNECT_BUDGET_MS = 15000
const WAIT_BUDGET = { attempts: 120, delay: 250 } // 30 s
const MESSAGES = 20
const CHANNELS = [
  'kafkajs.producer.network.request',
  'kafkajs.consumer.fetch',
  'kafkajs.consumer.end_batch_process',
]

/**
 * Live proof that the instrumentation events reach `node:diagnostics_channel` subscribers from real
 * clients through a produce and consume round trip, with the public event name as their type, and
 * that the documented prom-client recipe, run against a fake registry, counts the consumed
 * messages exactly.
 */
describeChannelsLive('Instrumentation > diagnostics channels (live)', () => {
  /** @type {{ channel: string, type: string, payload: object }[]} */
  const published = []
  const clients = []
  let recipe

  /** Records what a channel delivers, with the type as it is when it is published. */
  const record = (channel) => (message) =>
    published.push({ channel, type: message.type, payload: message.payload })
  const subscribers = CHANNELS.map((channel) => [channel, record(channel)])

  beforeAll(() => {
    recipe = runRecipe()
    for (const [channel, onMessage] of subscribers) {
      diagnosticsChannel.subscribe(channel, onMessage)
    }
  })

  afterAll(async () => {
    for (const [channel, onMessage] of subscribers) {
      diagnosticsChannel.unsubscribe(channel, onMessage)
    }
    recipe.stop()
    for (const client of clients.reverse()) {
      await withTimeout(
        client.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      )
    }
  }, SCENARIO_BUDGET_MS)

  test(
    'a round trip reaches the channels with public event names, and the recipe counts what was consumed',
    async () => {
      const topic = `e2e-p8-diagnostics-channel-${secureRandom()}`
      await createTopic({ topic, partitions: 3 })
      const kafka = new Kafka({
        clientId: `e2e-p8-diagnostics-channel-${secureRandom()}`,
        brokers: plainTextBrokers(),
        logLevel: logLevel.NOTHING,
      })
      const producer = kafka.producer()
      const consumer = kafka.consumer({ groupId: `e2e-p8-diagnostics-channel-${secureRandom()}` })
      clients.push(producer, consumer)
      let consumed = 0

      await producer.connect()
      await producer.send({
        topic,
        messages: Array.from({ length: MESSAGES }, (_, index) => ({ value: `message-${index}` })),
      })
      await consumer.connect()
      await consumer.subscribe({ topics: [topic], fromBeginning: true })
      await consumer.run({
        eachMessage: async () => {
          consumed += 1
        },
      })
      await waitForCondition(() => consumed >= MESSAGES, WAIT_BUDGET)
      const total = () =>
        recipe
          .metric('kafkajs_consumer_messages_total')
          .samples.reduce((sum, { value }) => sum + value, 0)
      await waitForCondition(() => total() >= MESSAGES, WAIT_BUDGET)

      const on = (channel) => published.filter((event) => event.channel === channel)
      writeEvidence(EVIDENCE, {
        scenario: 'round-trip',
        events: Object.fromEntries(CHANNELS.map((channel) => [channel, on(channel).length])),
        producerApis: [...new Set(on(CHANNELS[0]).map(({ payload }) => payload.apiName))],
        consumed,
        recipeMessages: total(),
      })

      for (const channel of CHANNELS) {
        expect(on(channel).length).toBeGreaterThan(0)
        expect(on(channel).every(({ type }) => type === channel.replace('kafkajs.', ''))).toBe(true)
      }
      expect(on(CHANNELS[0]).map(({ payload }) => payload.apiName)).toContain('Produce')
      expect(on(CHANNELS[1]).every(({ payload }) => typeof payload.duration === 'number')).toBe(
        true
      )
      expect(consumed).toBe(MESSAGES)
      expect(total()).toBe(MESSAGES)
      expect(
        recipe
          .metric('kafkajs_requests_total')
          .samples.some(({ labels }) => labels.kind === 'producer' && labels.api === 'Produce')
      ).toBe(true)
    },
    SCENARIO_BUDGET_MS
  )
})

// The share arm needs a broker line that can run share groups; the ordinary arm above runs on all.
const SHARE_LINES = ['4.2.2', '4.3.1', '4.4.0']
const describeShareChannelsLive =
  process.env.DIAGNOSTICS_CHANNEL_E2E === '1' && SHARE_LINES.includes(process.env.KAFKA_VERSION)
    ? describe
    : describe.skip

const SHARE_TITLE = 'publishes live share events to the eighteen named channels'
const SHARE_SCENARIO = 'share-round-trip'
const SHARE_BUDGET_MS = 50000
const SHARE_HOOK_MS = 10000
const SHARE_CLEANUP_MS = 10000
const SHARE_RECORDS = 20
const SHARE_PREFIX = 'kafkajs.share_consumer.'
const SHARE_API_KEYS = [76, 78, 79]
const RECORD_MARK = 'diag-record:'
const SHARE_CLIENT = {
  acquireMode: 'record_limit',
  maxRecords: SHARE_RECORDS,
  maxWaitTimeInMs: 250,
}
const SHARE_GROUP_CONFIG = { 'share.auto.offset.reset': 'earliest' }
const LOCK_CONFIG = 'share.record.lock.duration.ms'

const REQUEST_KEYS = [
  'apiKey',
  'apiName',
  'apiVersion',
  'broker',
  'clientId',
  'correlationId',
  'createdAt',
  'pendingDuration',
  'sentAt',
]

/** The public suffix of every share channel and the exact payload keys of its events, or none. */
const SHARE_PAYLOADS = {
  connect: null,
  disconnect: null,
  stop: null,
  heartbeat: ['groupId', 'memberId', 'memberEpoch', 'heartbeatIntervalMs'],
  group_join: ['groupId', 'memberId', 'memberEpoch'],
  assignment_changed: ['groupId', 'memberId', 'memberEpoch', 'assignment', 'added', 'removed'],
  fetch: [
    'nodeId',
    'sessionEpoch',
    'numberOfRecords',
    'numberOfGaps',
    'acknowledgementsPiggybacked',
    'duration',
  ],
  acknowledge: ['topic', 'partition', 'firstOffset', 'lastOffset', 'type', 'error'],
  'network.request': [...REQUEST_KEYS, 'duration', 'size'],
  'network.request_queue_size': ['broker', 'clientId', 'queueSize'],
  crash: ['error', 'groupId', 'restart'],
  session_reset: ['nodeId', 'reason', 'recordsDropped'],
  lock_expired: ['topic', 'partition', 'offsets', 'stage'],
  handler_error: ['topic', 'partition', 'offsets', 'error'],
  'network.request_timeout': REQUEST_KEYS,
  'network.authentication_failure': ['broker', 'mechanism', 'errorMessage'],
  'cluster.topic_recreated': ['topic', 'previousTopicId', 'topicId'],
  'cluster.rebootstrap': ['reason'],
}

/** The channels a normal round trip must publish on, and those it must never publish on. */
const SHARE_POSITIVE = [
  'connect',
  'disconnect',
  'stop',
  'heartbeat',
  'group_join',
  'assignment_changed',
  'fetch',
  'acknowledge',
  'network.request',
  'network.request_queue_size',
]
const SHARE_ABSENT = ['crash', 'handler_error']

const FIELD_TYPES = {
  groupId: 'string',
  memberId: 'string',
  memberEpoch: 'number',
  heartbeatIntervalMs: 'number',
  assignment: 'array',
  added: 'array',
  removed: 'array',
  nodeId: 'number',
  sessionEpoch: 'number',
  numberOfRecords: 'number',
  numberOfGaps: 'number',
  acknowledgementsPiggybacked: 'number',
  duration: 'number',
  topic: 'string',
  partition: 'number',
  firstOffset: 'string',
  lastOffset: 'string',
  type: 'string',
  apiKey: 'number',
  apiName: 'string',
  apiVersion: 'number',
  broker: 'string',
  clientId: 'string',
  correlationId: 'number',
  createdAt: 'number',
  pendingDuration: 'number',
  sentAt: 'number',
  size: 'number',
  queueSize: 'number',
  restart: 'boolean',
  reason: 'string',
  recordsDropped: 'number',
  offsets: 'array',
  stage: 'string',
  mechanism: 'string',
  errorMessage: 'string',
  previousTopicId: 'string',
  topicId: 'string',
}

/**
 * @param {unknown} value any payload field
 * @returns {string} `array`, `null` or the `typeof` of the value
 */
const kindOf = (value) => (Array.isArray(value) ? 'array' : value === null ? 'null' : typeof value)

/**
 * @param {string} suffix the public channel suffix
 * @param {unknown} payload one event's payload
 * @returns {boolean} whether it has exactly the keys and field types the event is documented with
 */
const payloadValid = (suffix, payload) => {
  const keys = SHARE_PAYLOADS[suffix]
  if (keys === null) return payload === undefined
  if (
    kindOf(payload) !== 'object' ||
    !isDeepStrictEqual(Object.keys(payload).sort(), [...keys].sort())
  ) {
    return false
  }
  return keys.every((key) =>
    key === 'error'
      ? payload.error === null || payload.error instanceof Error
      : kindOf(payload[key]) === FIELD_TYPES[key]
  )
}

/**
 * @param {unknown} value any part of an event payload
 * @param {number} [depth] how deep the walk is
 * @returns {boolean} whether it holds binary data or text of an application record
 */
const exposesRecord = (value, depth = 0) => {
  if (depth > 6 || value === null || value === undefined) return false
  if (typeof value === 'string') return value.includes(RECORD_MARK)
  if (ArrayBuffer.isView(value) || value instanceof ArrayBuffer) return true
  if (value instanceof Error) {
    return exposesRecord(value.message, depth + 1) || exposesRecord(value.cause, depth + 1)
  }
  return (
    typeof value === 'object' && Object.values(value).some((item) => exposesRecord(item, depth + 1))
  )
}

/**
 * @param {{ sockets: { open: number }, activeResources: Record<string, number>, listeners: Record<string, number>, queueDepth: number, ledgerSize: number }} snapshot a measurement
 * @returns {object} the evidence form of the measurement
 */
const resourceSummary = (snapshot) => ({
  openSockets: snapshot.sockets.open,
  timers: snapshot.activeResources.Timeout ?? 0,
  listeners: Object.values(snapshot.listeners).reduce((sum, n) => sum + n, 0),
  queueDepth: snapshot.queueDepth,
  ledgerSize: snapshot.ledgerSize,
})

/**
 * Logs every request frame the clients of one case write and counts their sockets and unanswered
 * requests from the bytes actually written and read. Every byte passes through untouched.
 */
class SocketLedger {
  /** Starts with no sockets. */
  constructor() {
    this.entries = []
    this.log = new FrameLog()
  }

  /** @returns {Function} a Kafka `socketFactory` whose sockets this ledger observes */
  factory() {
    return this.log.observe(({ host, port, onConnect }) => this.open(host, port, onConnect))
  }

  /**
   * @param {string} host the broker host
   * @param {number} port the broker port
   * @param {Function} onConnect the client's connect callback
   * @returns {import('net').Socket} a plaintext socket, counted
   */
  open(host, port, onConnect) {
    const socket = net.connect({ host, port }, onConnect)
    const entry = { socket, answered: 0, carry: Buffer.alloc(0) }
    socket.on('data', (chunk) => this.read(entry, chunk))
    this.entries.push(entry)
    return socket
  }

  /**
   * @param {{ answered: number, carry: Buffer }} entry the socket's bookkeeping
   * @param {Buffer} chunk the next bytes the broker sent on it
   * @returns {void} counts the response frames the chunk completed
   */
  read(entry, chunk) {
    let bytes = Buffer.concat([entry.carry, chunk])
    while (bytes.length >= 4 && bytes.length >= 4 + bytes.readInt32BE(0)) {
      bytes = bytes.subarray(4 + bytes.readInt32BE(0))
      entry.answered++
    }
    entry.carry = bytes
  }

  /** @returns {{ created: number, destroyed: number }} sockets opened and since destroyed */
  counts() {
    return {
      created: this.entries.length,
      destroyed: this.entries.filter(({ socket }) => socket.destroyed).length,
    }
  }

  /** @returns {number} requests written on live sockets that have no response yet */
  pending() {
    return this.entries.reduce(
      (total, { socket, answered }, index) =>
        socket.destroyed
          ? total
          : total +
            this.log.frames.filter(({ connection }) => connection === index + 1).length -
            answered,
      0
    )
  }

  /** @returns {object[]} every share request frame written, with its header facts */
  shareFrames() {
    return this.log.frames
      .filter(({ apiKey }) => SHARE_API_KEYS.includes(apiKey))
      .map(({ apiKey, apiVersion, correlationId, connection }) => ({
        apiKey,
        version: apiVersion,
        correlationId,
        socketId: connection,
      }))
  }
}

/**
 * One live run of the share diagnostics scenario: a share member reads twenty fixed records while
 * all eighteen share channels are subscribed by their public names and the same events are also
 * listened to through the client's public `on`. The events a normal round trip publishes must
 * arrive once, as the same object on both paths, with the documented type and payload.
 */
class ShareDiagnosticsCase {
  /** Builds a case with fresh names; nothing is created until `run`. */
  constructor() {
    this.nonce = secureRandom(6)
    this.topic = shareTopicName('diagnostics')
    this.group = `e2e-p9e-diagnostics-group-${this.nonce}`
    this.stack = new CleanupStack()
    this.ledger = new SocketLedger()
    this.openHandlers = 0
    this.probe = createResourceProbe({
      emitters: { process },
      sockets: () => this.ledger.counts(),
      queueDepth: () => this.ledger.pending(),
      ledgerSize: () => this.openHandlers,
      activeResources: true,
    })
    this.channels = Object.keys(SHARE_PAYLOADS).map((suffix) => ({
      suffix,
      name: `${SHARE_PREFIX}${suffix}`,
      publicName: `share_consumer.${suffix}`,
      channel: null,
      listener: null,
      identity: false,
      subscribed: false,
      names: [],
      viaChannel: [],
      viaListener: [],
      count: 0,
      payloadsValid: false,
    }))
    this.requests = []
    this.acks = []
    this.crashes = []
    this.entries = []
    this.expected = []
    this.unsubscribed = false
    this.baseline = null
    this.after = null
  }

  /**
   * @param {string} label the client label, also the start of its clientId
   * @returns {Kafka} a client whose sockets go through the ledger
   */
  client(label) {
    return new Kafka({
      clientId: `${label}-${this.nonce}`,
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      socketFactory: this.ledger.factory(),
    })
  }

  /**
   * Appends one evidence row.
   * @param {string} event the row kind
   * @param {object} [fields] the measured facts
   * @returns {void}
   */
  row(event, fields = {}) {
    writeEvidence(EVIDENCE, {
      event,
      case: SHARE_TITLE,
      scenario: SHARE_SCENARIO,
      nonce: this.nonce,
      profile: 'tv2',
      groups: { diagnostics: this.group },
      budgets: { caseMs: SHARE_BUDGET_MS, cleanupMs: SHARE_CLEANUP_MS },
      wireVersions: wireVersionsOf(this.requests),
      ...fields,
    })
  }

  /** @returns {object[]} the per-channel facts as evidence keeps them */
  channelFacts() {
    return this.channels.map(({ name, identity, subscribed, count, payloadsValid }) => ({
      name,
      identity,
      subscribed,
      count,
      payloadsValid,
    }))
  }

  /**
   * Runs the whole case: setup, delivery, observation, final verification and strict cleanup.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} the first assertion or fixture failure, with cleanup failures attached
   */
  async run(scope) {
    try {
      await this.stack.run(
        async () => {
          await this.setup(scope)
          await this.deliver(scope)
          await this.observe(scope)
          await this.finish(scope)
        },
        { budgetMs: SHARE_CLEANUP_MS }
      )
      await this.complete(scope)
    } catch (error) {
      this.row('case-failed', {
        error: describeError(error),
        channels: this.channelFacts(),
        crashes: this.crashes.map((crash) => describeError(crash)),
      })
      throw error
    }
  }

  /**
   * Subscribes every share channel by its public name, before any client exists, keeping the
   * channel object Node returned and the exact callback.
   * @returns {void}
   * @throws {Error} when a channel already has a subscriber
   */
  subscribeChannels() {
    this.stack.add('unsubscribe share channels', () => this.unsubscribeChannels())
    for (const entry of this.channels) {
      expect(diagnosticsChannel.hasSubscribers(entry.name)).toBe(false)
      entry.channel = diagnosticsChannel.channel(entry.name)
      entry.listener = (message, name) => {
        entry.names.push(name)
        entry.viaChannel.push(message)
      }
      diagnosticsChannel.subscribe(entry.name, entry.listener)
      entry.subscribed = diagnosticsChannel.hasSubscribers(entry.name)
      entry.identity = diagnosticsChannel.channel(entry.name) === entry.channel
    }
  }

  /**
   * Removes every exact callback and proves no share channel has a subscriber again. Repeating it
   * does nothing.
   * @returns {void}
   * @throws {Error} naming each channel that did not return to no subscribers
   */
  unsubscribeChannels() {
    if (this.unsubscribed) return
    this.unsubscribed = true
    const left = this.channels
      .filter(({ listener }) => listener !== null)
      .filter(
        ({ name, listener }) =>
          !diagnosticsChannel.unsubscribe(name, listener) || diagnosticsChannel.hasSubscribers(name)
      )
      .map(({ name }) => name)
    if (left.length) throw new Error(`Share channels still have subscribers: ${left.join(', ')}`)
  }

  /**
   * Subscribes the channels, then measures the baseline, creates the topic and group config,
   * reads the record lock back, connects the admin and the producer.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async setup(scope) {
    this.subscribeChannels()
    this.beforeAdmin = await this.probe.snapshot()
    this.admin = this.client('admin').admin()
    this.admin.on(this.admin.events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.stack.add('prove cleanup and disconnect admin', (signal) => this.cleanup(signal))
    this.stack.add('delete topic', (signal) => this.deleteTopic(signal))
    this.stack.add('delete group', (signal) => this.deleteGroup(signal))
    this.stack.add('reset group configs', (signal) => this.resetConfigs(signal))
    await bounded(scope, 15000, () => this.admin.connect())
    await bounded(scope, 30000, () =>
      createTopic({
        topic: this.topic,
        partitions: 1,
        config: [
          { name: 'retention.ms', value: '-1' },
          { name: 'cleanup.policy', value: 'delete' },
        ],
      })
    )
    await bounded(scope, 15000, () =>
      setShareGroupConfigs(this.admin, this.group, SHARE_GROUP_CONFIG)
    )
    const { resources } = await bounded(scope, 8000, () =>
      this.admin.describeConfigs({
        resources: [
          { type: ConfigResourceTypes.GROUP, name: this.group, configNames: [LOCK_CONFIG] },
        ],
      })
    )
    this.lock = resources[0].configEntries.find(({ configName }) => configName === LOCK_CONFIG)
    expect(this.lock?.configValue).toBe('30000')
    this.baseline = await bounded(scope, 60000, () =>
      bounded(scope, 10000, () => this.probe.snapshot())
    )
    this.row('config-readback', { group: SHARE_GROUP_CONFIG, lock: this.lock.configValue })
    this.row('resource-baseline', { snapshot: this.baseline })

    this.producer = this.client('producer').producer({ idempotent: true })
    this.producer.on(this.producer.events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.stack.add('disconnect producer', () => this.producer.disconnect())
    await bounded(scope, 15000, () => this.producer.connect())
  }

  /**
   * Starts the share member with a public listener on every event name, produces the fixed
   * records and waits until every one is delivered and accepted.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async deliver(scope) {
    const consumer = this.client('share').shareConsumer({
      groupId: this.group,
      acknowledgementMode: 'implicit',
      ...SHARE_CLIENT,
    })
    this.consumer = consumer
    expect(Object.values(consumer.events).sort()).toEqual(
      this.channels.map(({ publicName }) => publicName).sort()
    )
    const removers = this.channels.map((entry) =>
      consumer.on(entry.publicName, (event) => {
        entry.viaListener.push(event)
        if (entry.suffix === 'network.request') this.requests.push(event.payload)
        if (entry.suffix === 'acknowledge') this.acks.push(event.payload)
        if (entry.suffix === 'crash') this.crashes.push(event.payload.error)
      })
    )
    this.stack.add('remove share listeners', () => removers.forEach((remove) => remove()))
    this.stack.add('disconnect share member', () => consumer.disconnect({ timeout: 5000 }))
    await bounded(scope, 15000, () => consumer.connect())
    await bounded(scope, 15000, () => consumer.subscribe({ topics: [this.topic] }))
    await bounded(scope, 20000, () =>
      consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          this.openHandlers++
          try {
            this.entries.push(shareRecordIdentity({ ...message, topic, partition }))
          } finally {
            this.openHandlers--
          }
        },
      })
    )
    const messages = Array.from({ length: SHARE_RECORDS }, (_, index) => ({
      partition: 0,
      key: Buffer.from(`${RECORD_MARK}${String(index).padStart(4, '0')}`),
      value: Buffer.from(`${RECORD_MARK}value:${index}`),
      timestamp: '1700000000000',
    }))
    const [sent] = await bounded(scope, 15000, () =>
      this.producer.send({ topic: this.topic, acks: -1, messages })
    )
    expect(sent.errorCode).toBe(0)
    this.expected = messages.map((message, position) =>
      shareRecordIdentity({
        topic: this.topic,
        partition: 0,
        offset: String(BigInt(sent.baseOffset) + BigInt(position)),
        key: message.key,
        value: message.value,
      })
    )
    await pollUntil(scope, 20000, () => this.entries.length >= SHARE_RECORDS, {
      label: 'All deliveries',
    })
    await pollUntil(scope, 15000, () => this.acceptedOffsets() >= BigInt(SHARE_RECORDS), {
      label: 'All terminal accepts',
    })
    expect(consumer.acquisitionLockTimeoutMs()).toBe(30000)
  }

  /**
   * @returns {bigint} offsets the broker confirmed accepted
   * @throws {Error} on any terminal event that is not a confirmed accept
   */
  acceptedOffsets() {
    let total = 0n
    for (const { type, error, firstOffset, lastOffset } of this.acks) {
      if (type === 'renew') continue
      if (type !== 'accept' || error !== null) {
        throw new Error(`The member reported ${type} with ${error?.message ?? 'no error'}`)
      }
      total += BigInt(lastOffset) - BigInt(firstOffset) + 1n
    }
    return total
  }

  /**
   * Checks the three independent oracles: raw log, handler entries and broker position.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async observe(scope) {
    const log = await bounded(scope, 20000, () =>
      readLog({ brokers: plainTextBrokers(), topic: this.topic })
    )
    const { endOffset, records } = log.get(0)
    expect(endOffset).toBe(String(SHARE_RECORDS))
    expect(records.filter(({ batchContext }) => batchContext.isControlBatch)).toEqual([])
    this.raw = records.map(({ offset, key, value }) =>
      shareRecordIdentity({ topic: this.topic, partition: 0, offset: String(offset), key, value })
    )
    expect(this.raw).toEqual(this.expected)
    expect([...this.entries].sort()).toEqual([...this.expected].sort())
    this.broker = await this.awaitBrokerEnd(scope, endOffset)
  }

  /**
   * Waits until the share start offset of the partition equals the log end with known lag zero,
   * in both the named and the all-topics listing.
   * @param {AbortSignal} scope the case scope
   * @param {string} logEnd the independently read log end offset
   * @returns {Promise<object[]>} the terminal row of the partition
   */
  async awaitBrokerEnd(scope, logEnd) {
    let rows = []
    const pick = (listing) =>
      listing.find(({ topic }) => topic === this.topic)?.partitions.find((p) => p.partition === 0)
    await pollTolerating(
      scope,
      20000,
      async (poll) => {
        const named = await bounded(poll, 10000, () =>
          this.admin.listShareGroupOffsets({
            groupId: this.group,
            topics: [{ topic: this.topic, partitions: [0] }],
            apiTimeout: 8000,
          })
        )
        const all = await bounded(poll, 10000, () =>
          this.admin.listShareGroupOffsets({ groupId: this.group, apiTimeout: 8000 })
        )
        const [first, second] = [pick(named), pick(all)]
        rows = [
          {
            groupId: this.group,
            topic: this.topic,
            partition: 0,
            errorCode: first?.errorCode,
            startOffset: first?.startOffset,
            logEnd,
            lag: first?.lag,
          },
        ]
        const [row] = rows
        if (
          !(row.errorCode === 0 && row.startOffset === logEnd && row.lag === '0') ||
          !isDeepStrictEqual(first, second)
        ) {
          throw new Error(
            `Share position is not at the log end: ${JSON.stringify({ first, second })}`
          )
        }
        return true
      },
      { label: 'The broker share offset' }
    )
    return rows
  }

  /**
   * Stops and disconnects the member while every channel is still subscribed, checks what each
   * channel published, then lets a fresh member of the same group listen for five seconds.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async finish(scope) {
    await bounded(scope, 10000, () => this.consumer.stop())
    await bounded(scope, 10000, () => this.consumer.disconnect({ timeout: 5000 }))
    this.verifyChannels()
    const verified = await bounded(scope, 30000, (inner) =>
      verifierMember(this.group, this.topic, { kafka: this.client('verifier'), signal: inner })
    )
    expect(verified).toEqual([])
  }

  /**
   * Compares the two observation paths and checks every observed event: the channel subscriber
   * and the public listener saw the same event objects, of the public type, with the documented
   * payload and no application data.
   * @returns {void}
   * @throws {Error} on the first channel that does not hold
   */
  verifyChannels() {
    for (const entry of this.channels) {
      expect({ name: entry.name, identity: entry.identity, subscribed: entry.subscribed }).toEqual({
        name: entry.name,
        identity: true,
        subscribed: true,
      })
      expect(entry.names.every((name) => name === entry.name)).toBe(true)
      expect(entry.viaChannel).toHaveLength(entry.viaListener.length)
      entry.viaChannel.forEach((event, index) => expect(event).toBe(entry.viaListener[index]))
      entry.count = entry.viaChannel.length
      entry.payloadsValid = entry.viaChannel.every(
        ({ type, payload }) =>
          type === entry.publicName &&
          payloadValid(entry.suffix, payload) &&
          !exposesRecord(payload)
      )
      expect({ name: entry.name, payloadsValid: entry.payloadsValid }).toEqual({
        name: entry.name,
        payloadsValid: true,
      })
    }
    const counts = Object.fromEntries(this.channels.map(({ suffix, count }) => [suffix, count]))
    for (const suffix of SHARE_POSITIVE)
      expect({ suffix, count: counts[suffix] }).not.toEqual({ suffix, count: 0 })
    for (const suffix of SHARE_ABSENT)
      expect({ suffix, count: counts[suffix] }).toEqual({ suffix, count: 0 })
    expect(this.crashes).toEqual([])
    this.row('channels', { channels: this.channelFacts() })
  }

  /**
   * Removes the channel subscribers, then proves owned objects are gone and resources are back at
   * baseline, and disconnects the admin. It runs last.
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   * @throws {Error} the first failure of the removal, the proof or the disconnect
   */
  async cleanup(signal) {
    let failure = null
    try {
      this.unsubscribeChannels()
    } catch (error) {
      failure = error
    }
    try {
      if (this.baseline) {
        await pollUntil(
          signal,
          3000,
          () => {
            const { created, destroyed } = this.ledger.counts()
            return created - destroyed <= this.baseline.sockets.open
          },
          { delayMs: 50, label: 'The sockets closing' }
        )
        this.after = await assertShareCleanup(
          this.admin,
          [this.group],
          [this.topic],
          this.baseline,
          this.probe,
          { signal }
        )
      }
    } catch (error) {
      failure ??= error
    }
    try {
      await this.admin.disconnect()
    } catch (error) {
      failure ??= error
    }
    if (failure) throw failure
  }

  /**
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   * @throws {Error} when the topic is still listed after deletion
   */
  async deleteTopic(signal) {
    await bounded(signal, 10000, () =>
      this.admin.deleteTopics({ topics: [this.topic], timeout: 8000 })
    ).catch((error) => {
      if (error.type !== 'UNKNOWN_TOPIC_OR_PARTITION') throw error
    })
    await pollUntil(
      signal,
      10000,
      async () => !(await this.admin.listTopics()).includes(this.topic),
      { label: 'The topic disappearing' }
    )
  }

  /**
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   * @throws {Error} when the group is still listed after deletion
   */
  async deleteGroup(signal) {
    await pollUntil(
      signal,
      10000,
      async () => {
        try {
          await this.admin.deleteGroups([this.group])
          return true
        } catch (error) {
          const code = error.groups?.[0]?.error?.code
          if (code === 69) return true
          if (code === 68) return false
          throw error
        }
      },
      { label: 'The group deletion' }
    )
    await pollUntil(
      signal,
      10000,
      async () =>
        !(await this.admin.listGroups()).groups.some(({ groupId }) => groupId === this.group),
      { label: 'The group disappearing' }
    )
  }

  /**
   * Removes the owned group config overrides and proves none remain.
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   * @throws {Error} when the broker refuses or an override remains
   */
  async resetConfigs(signal) {
    const names = Object.keys(SHARE_GROUP_CONFIG)
    const { resources } = await bounded(signal, 8000, () =>
      this.admin.incrementalAlterConfigs({
        resources: [
          {
            type: ConfigResourceTypes.GROUP,
            name: this.group,
            configEntries: names.map((name) => ({ name, operation: AlterConfigOpTypes.DELETE })),
          },
        ],
      })
    )
    if (resources[0].errorCode !== 0) {
      throw new Error(`Config reset refused: ${resources[0].errorCode}`)
    }
    const read = await bounded(signal, 8000, () =>
      this.admin.describeConfigs({
        resources: [{ type: ConfigResourceTypes.GROUP, name: this.group, configNames: names }],
      })
    )
    const left = read.resources[0].configEntries.filter(
      ({ configSource }) => configSource === ConfigSource.GROUP_CONFIG
    )
    if (left.length > 0) {
      throw new Error(`Group config overrides remain: ${left.map((entry) => entry.configName)}`)
    }
  }

  /**
   * Checks the admin's own sockets are closed, then records the single completion row.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async complete(scope) {
    await pollUntil(
      scope,
      5000,
      async () => (await this.probe.snapshot()).sockets.open === this.beforeAdmin.sockets.open,
      { delayMs: 50, label: 'The admin sockets closing' }
    )
    this.row('cleanup', { snapshot: await bounded(scope, 10000, () => this.probe.snapshot()) })
    this.row('case-complete', {
      status: 'passed',
      tainted: false,
      oracles: {
        log: { expected: [...this.expected].sort(), actual: [...this.raw].sort() },
        handler: {
          expected: [...this.expected].sort().map((identity) => [identity, 1]),
          actual: [...this.entries].sort().map((identity) => [identity, 1]),
        },
        broker: this.broker,
      },
      channels: this.channelFacts(),
      resources: { before: resourceSummary(this.baseline), after: resourceSummary(this.after) },
      cleanup: {
        groupsAbsent: true,
        topicsAbsent: true,
        completedAt: Number(process.hrtime.bigint() / 1000000n),
      },
      config: { client: SHARE_CLIENT, group: SHARE_GROUP_CONFIG },
    })
  }
}

describeShareChannelsLive('Instrumentation > share diagnostics channels (live)', () => {
  beforeAll(
    () =>
      bounded(NEVER, SHARE_HOOK_MS - 2000, () => {
        liveRun.assertClean()
      }),
    SHARE_HOOK_MS
  )

  test(
    SHARE_TITLE,
    liveCase(SHARE_BUDGET_MS, (scope) => new ShareDiagnosticsCase().run(scope)),
    caseTimeout(SHARE_BUDGET_MS)
  )

  afterEach(() => {
    if (!liveRun.takeAdmission()) return
    liveRun.assertClean()
  })
})
