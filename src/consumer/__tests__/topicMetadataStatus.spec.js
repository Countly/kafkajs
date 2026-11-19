const { Kafka, logLevel } = require('../../../index')
const {
  createScriptedCluster,
  groupScript,
  consumerProtocolScript,
} = require('../../../testHelpers/scriptedCluster')
const Encoder = require('../../protocol/encoder')
const { RecordBatch } = require('../../protocol/recordBatch/v0')
const RecordEncoder = require('../../protocol/recordBatch/record/v0')
const { waitForCondition } = require('./fixtures/liveScenario')
const sleep = require('../../utils/sleep')

const HEALTHY = 'lifecycle-healthy'
const DOOMED = 'lifecycle-doomed'
const HEALTHY_ID = '0a0a0a0a-0000-4000-8000-000000000001'
const DOOMED_ID = '0a0a0a0a-0000-4000-8000-000000000002'
const RECREATED_ID = '0a0a0a0a-0000-4000-8000-000000000003'
const NULL_ID = '00000000-0000-0000-0000-000000000000'
const NODES = [{ id: 0, host: 'node0', port: 9092 }]
const RETRY = { retries: 2, initialRetryTime: 10, maxRetryTime: 20 }
const UNAVAILABLE_WARN =
  'Subscribed topic unavailable; continuing healthy topics and retrying metadata'

/** Polls until `predicate` holds, failing with `what` when it never does. */
const until = async (predicate, what, attempts = 2000) => {
  if (!(await waitForCondition(predicate, { attempts, delay: 5 }))) {
    throw new Error(`${what} did not happen`)
  }
}

/**
 * One record at `offset`, encoded as a broker would return it.
 * @param {string} offset
 * @returns {Promise<Buffer>}
 */
const recordAt = async (offset) =>
  Buffer.from(
    (
      await RecordBatch({
        firstOffset: offset,
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        records: [RecordEncoder({ key: Buffer.from('key'), value: Buffer.from('value') })],
      })
    ).buffer
  )

/**
 * A Fetch v18 response body over several topics, keyed by topic id.
 * @param {{ topicId: string, partitions: { partition: number, errorCode: number, records: Buffer|null }[] }[]} topics
 * @returns {Buffer}
 */
const fetchBody = (topics) =>
  new Encoder()
    .writeInt32(0)
    .writeInt16(0)
    .writeInt32(0)
    .writeUVarIntArray(
      topics.map(({ topicId, partitions }) =>
        new Encoder()
          .writeUuid(topicId)
          .writeUVarIntArray(
            partitions.map(({ partition, errorCode, records }) => {
              const entry = new Encoder()
                .writeInt32(partition)
                .writeInt16(errorCode)
                .writeInt64(1000)
                .writeInt64(1000)
                .writeInt64(0)
                .writeUVarIntArray([])
                .writeInt32(-1)
              if (records) entry.writeUVarInt(records.length + 1).writeBuffer(records)
              else entry.writeUVarInt(0)
              return entry.writeTaggedFields()
            })
          )
          .writeTaggedFields()
      )
    )
    .writeTaggedFields().buffer

/**
 * The scripted cluster's topics: each live one has a single partition on node 0 whose log holds a
 * record at every offset. A deleted name is answered with code 3, a refused one with its code.
 */
class World {
  constructor() {
    this.ids = new Map([
      [HEALTHY, HEALTHY_ID],
      [DOOMED, DOOMED_ID],
    ])
    this.refused = new Map()
    this.partitionErrors = new Map()
    this.epoch = 0
    this.assigned = null
    this.names = null
    this.regex = null
    /** @type {Map<string, number>} codes for live names on a lookup by name only */
    this.namedCodes = new Map()
    /** @type {Map<string, number>} codes for live topic ids on a lookup by id only */
    this.idCodes = new Map()
    /** @type {Set<string>} live names the coordinator does not assign */
    this.withheld = new Set()
    /** @type {Map<string, string>} the id the coordinator still assigns for a name */
    this.lagged = new Map()
    /** Whether the coordinator repeats an unchanged assignment instead of answering null. */
    this.repeat = false
  }

  /** @param {string} name @returns {boolean} whether the topic exists and can be described */
  live(name) {
    return this.ids.has(name) && !this.refused.has(name)
  }

  /** @param {string} name */
  remove(name) {
    this.ids.delete(name)
    this.refused.set(name, 3)
  }

  /** @param {string} name @param {string} topicId */
  recreate(name, topicId) {
    this.refused.delete(name)
    this.ids.set(name, topicId)
  }

  /** @param {string} topicId @returns {string|undefined} the live topic with that id */
  nameOf(topicId) {
    for (const [name, id] of this.ids) if (id === topicId && this.live(name)) return name
  }

  /**
   * @param {string} name
   * @param {boolean} [byId] whether the lookup was by topic id
   * @returns {object} one Metadata v13 topic entry
   */
  describe(name, byId = false) {
    if (!this.live(name) || (!byId && this.namedCodes.has(name))) {
      return {
        errorCode: (byId ? undefined : this.namedCodes.get(name)) ?? this.refused.get(name) ?? 3,
        name,
        topicId: NULL_ID,
        isInternal: false,
        topicAuthorizedOperations: -2147483648,
        partitions: [],
      }
    }
    return {
      errorCode: 0,
      name,
      topicId: this.ids.get(name),
      isInternal: false,
      topicAuthorizedOperations: -2147483648,
      partitions: [
        {
          errorCode: this.partitionErrors.get(name) ?? 0,
          partitionIndex: 0,
          leaderId: 0,
          leaderEpoch: 5,
          replicas: [0],
          isr: [0],
          offlineReplicas: [],
        },
      ],
    }
  }

  /** @param {{ name: string|null, topicId: string }[]|null} requested @returns {object} */
  metadata(requested) {
    const entries =
      requested === null
        ? [...this.ids.keys()]
            .filter((name) => this.live(name) && !this.idCodes.has(this.ids.get(name)))
            .map((name) => this.describe(name))
        : requested.map(({ name, topicId }) => {
            if (name != null) return this.describe(name)
            const known = this.nameOf(topicId)
            const errorCode = this.idCodes.get(topicId) ?? (known ? 0 : 100)
            return errorCode === 0
              ? this.describe(known, true)
              : { errorCode, name: null, topicId, isInternal: false, partitions: [] }
          })
    return {
      brokers: [{ nodeId: 0, host: 'node0', port: 9092, rack: null }],
      controllerId: 0,
      topics: entries,
    }
  }

  /** @param {{ topicId: string, partitions: { partition: number, fetchOffset: string }[] }[]} topics */
  async fetch(topics) {
    await sleep(5)
    const answered = []
    for (const { topicId, partitions } of topics) {
      const live = this.nameOf(topicId) !== undefined
      const entries = []
      for (const { partition, fetchOffset } of partitions) {
        entries.push(
          live
            ? { partition, errorCode: 0, records: await recordAt(fetchOffset) }
            : { partition, errorCode: 100, records: null }
        )
      }
      answered.push({ topicId, partitions: entries })
    }
    return fetchBody(answered)
  }

  /**
   * A coordinator of the consumer group protocol: it assigns partition 0 of every live topic the
   * member subscribes to, by name or by pattern, and a new epoch whenever that set changes.
   * @param {object} request the decoded heartbeat
   * @returns {object} the answer
   */
  heartbeat({ memberId, memberEpoch, subscribedTopicNames, subscribedTopicRegex }) {
    if (memberEpoch < 0) {
      return { memberId, memberEpoch, heartbeatIntervalMs: 50, assignment: null }
    }
    if (subscribedTopicNames) this.names = subscribedTopicNames
    if (subscribedTopicRegex) this.regex = new RegExp(subscribedTopicRegex)
    const wanted = [...this.ids.keys()]
      .filter((name) => this.live(name) && !this.withheld.has(name))
      .filter((name) => this.names?.includes(name) || this.regex?.test(name))
      .map((name) => this.lagged.get(name) ?? this.ids.get(name))
      .sort()
    const key = wanted.join()
    if (memberEpoch === 0 || key !== this.assigned || this.repeat) {
      if (memberEpoch === 0 || key !== this.assigned) this.epoch += 1
      this.assigned = key
      return {
        memberId,
        memberEpoch: this.epoch,
        heartbeatIntervalMs: 50,
        assignment: { topicPartitions: wanted.map((topicId) => ({ topicId, partitions: [0] })) },
      }
    }
    return { memberId, memberEpoch: this.epoch, heartbeatIntervalMs: 50, assignment: null }
  }
}

/**
 * A subscribed consumer survives one of its topics being deleted and picks it up again when it
 * is recreated: the scripted cluster plays the coordinator, the partition leader and the
 * metadata, and the consumer is the real one, built from `Kafka`.
 */
describe('Consumer > a subscribed topic deleted and recreated', () => {
  const consumers = []

  afterEach(async () => {
    for (const consumer of consumers.splice(0)) await consumer.disconnect().catch(() => {})
  })

  /**
   * @param {'classic'|'consumer'} protocol
   * @param {object} [options] consumer options over the scenario's
   * @param {object} [script] options of the group script, `committed` say
   * @returns {object} the world, the wire, the consumer and what it observed
   */
  const scenario = (protocol, options = {}, script = {}) => {
    const world = new World()
    let cluster = createScriptedCluster({
      nodes: NODES,
      profile: protocol === 'consumer' ? 'consumer' : 'default',
    })
    cluster =
      protocol === 'consumer'
        ? consumerProtocolScript(cluster, { topic: HEALTHY, ...script })
        : groupScript(cluster, { topic: HEALTHY, ...script })
    cluster.answer('Metadata', ({ topics }) => world.metadata(topics))
    cluster.answer('Fetch', ({ topics }) => world.fetch(topics))
    if (protocol === 'consumer') {
      cluster.answer('ConsumerGroupHeartbeat', (request) => world.heartbeat(request))
    } else {
      cluster.answer('SyncGroup', ({ memberId, groupAssignment }) => ({
        memberAssignment:
          groupAssignment.find((entry) => entry.memberId === memberId)?.memberAssignment ?? '',
      }))
    }

    const observed = { records: [], statuses: [], crashes: [], warnings: [], errors: [] }
    const kafka = new Kafka(
      cluster.kafkaOptions({
        retry: RETRY,
        logLevel: logLevel.WARN,
        logCreator:
          () =>
          ({ level, log }) => {
            if (level === logLevel.ERROR) observed.errors.push(log)
            if (level === logLevel.WARN && log.message === UNAVAILABLE_WARN) {
              observed.warnings.push(log)
              if (observed.loggerThrows) throw new Error('log sink down')
            }
          },
      })
    )
    const consumer = kafka.consumer({
      groupId: `lifecycle-${protocol}`,
      groupProtocol: protocol,
      allowAutoTopicCreation: false,
      metadataMaxAge: 100,
      maxWaitTimeInMs: 20,
      ...(protocol === 'classic' && { heartbeatInterval: 50 }),
      retry: { ...RETRY, restartOnFailure: async () => false },
      ...options,
    })
    consumers.push(consumer)
    consumer.on(consumer.events.TOPIC_METADATA_STATUS, ({ payload }) =>
      observed.statuses.push(payload)
    )
    consumer.on(consumer.events.CRASH, ({ payload }) => observed.crashes.push(payload))
    const run = () =>
      consumer.run({
        eachMessage: async ({ topic, message }) => {
          observed.records.push({ topic, offset: message.offset })
        },
      })
    return { world, cluster, consumer, observed, run }
  }

  /** @param {object} observed @param {string} topic @returns {string[]} the delivered offsets */
  const offsets = (observed, topic) =>
    observed.records.filter((record) => record.topic === topic).map(({ offset }) => offset)

  /**
   * Deletes `DOOMED` once both topics delivered, waits for healthy delivery to go on, recreates
   * it and waits for its first record from the new incarnation.
   * @param {object} wire the scenario
   * @returns {Promise<{ after: number }>} where the recreated topic's records begin
   */
  const deleteAndRecreate = async ({ world, observed }) => {
    await until(
      () => offsets(observed, HEALTHY).length >= 3 && offsets(observed, DOOMED).length >= 3,
      'records from both topics'
    )
    world.remove(DOOMED)
    await until(() => observed.statuses.length === 1, 'the unavailable status')
    const healthyAtDeletion = offsets(observed, HEALTHY).length
    await until(
      () => offsets(observed, HEALTHY).length >= healthyAtDeletion + 5,
      'healthy records after the deletion'
    )
    const after = observed.records.length
    world.recreate(DOOMED, RECREATED_ID)
    await until(
      () => observed.records.slice(after).some(({ topic }) => topic === DOOMED),
      'a record from the recreated topic'
    )
    await until(() => observed.statuses.length === 2, 'the available status')
    return { after }
  }

  /**
   * The assertions every kind shares: no crash, one warning, one event per transition, and the new
   * incarnation read from its beginning.
   * @param {object} wire the scenario
   * @param {number} after the record index the recreation began at
   * @param {string} groupId
   */
  const expectIsolated = ({ cluster, observed }, after, groupId) => {
    expect(observed.crashes).toEqual([])
    expect(observed.warnings).toEqual([
      expect.objectContaining({
        groupId,
        topic: DOOMED,
        type: 'UNKNOWN_TOPIC_OR_PARTITION',
        code: 3,
      }),
    ])
    expect(observed.statuses).toEqual([
      {
        groupId,
        topic: DOOMED,
        status: 'unavailable',
        type: 'UNKNOWN_TOPIC_OR_PARTITION',
        code: 3,
      },
      { groupId, topic: DOOMED, status: 'available', type: null, code: 0 },
    ])
    const recreated = observed.records.slice(after).filter(({ topic }) => topic === DOOMED)
    expect(recreated[0].offset).toBe('0')
    const firstAsked = cluster
      .requests()
      .filter(({ api }) => api === 'Fetch')
      .flatMap(({ fields }) => fields.topics)
      .find(({ topicId }) => topicId === RECREATED_ID)
    expect(firstAsked.partitions[0].fetchOffset).toBe('0')
  }

  test.each(['classic', 'consumer'])(
    '%s: a literal subscription keeps the healthy topic, reports the deleted one and reads its new incarnation from the start',
    async (protocol) => {
      const wire = scenario(protocol)
      await wire.consumer.connect()
      await wire.consumer.subscribe({ topics: [HEALTHY, DOOMED], fromBeginning: true })
      await wire.run()

      const { after } = await deleteAndRecreate(wire)

      expectIsolated(wire, after, `lifecycle-${protocol}`)
    },
    30000
  )

  test('manual: an assigned topic deleted and recreated is isolated the same way', async () => {
    const wire = scenario('classic', { groupId: 'lifecycle-manual' })
    await wire.consumer.connect()
    await wire.consumer.assign(
      [
        { topic: HEALTHY, partitions: [0] },
        { topic: DOOMED, partitions: [0] },
      ],
      { offsetReset: 'earliest' }
    )
    await wire.run()

    const { after } = await deleteAndRecreate(wire)

    expectIsolated(wire, after, 'lifecycle-manual')
    expect(
      wire.cluster.requests().filter(({ api }) => api === 'JoinGroup' || api === 'SyncGroup')
    ).toEqual([])
  }, 30000)

  test('consumer: a broker-side pattern keeps the healthy topic and reports the deleted match', async () => {
    const wire = scenario('consumer', { groupId: 'lifecycle-broker-regex' })
    await wire.consumer.connect()
    await wire.consumer.subscribe({ regex: '^lifecycle-.*', fromBeginning: true })
    await wire.run()

    const { after } = await deleteAndRecreate(wire)

    expectIsolated(wire, after, 'lifecycle-broker-regex')
  }, 30000)

  test('classic: a client-side pattern never sees the code, so it reports nothing and keeps the healthy topic', async () => {
    const wire = scenario('classic', { groupId: 'lifecycle-client-regex' })
    await wire.consumer.connect()
    await wire.consumer.subscribe({ topics: [/^lifecycle-/], fromBeginning: true })
    await wire.run()

    await until(
      () =>
        offsets(wire.observed, HEALTHY).length >= 3 && offsets(wire.observed, DOOMED).length >= 3,
      'records from both topics'
    )
    wire.world.remove(DOOMED)
    const healthyAtDeletion = offsets(wire.observed, HEALTHY).length
    await until(
      () => offsets(wire.observed, HEALTHY).length >= healthyAtDeletion + 5,
      'healthy records after the deletion'
    )

    expect(wire.observed.crashes).toEqual([])
    expect(wire.observed.statuses).toEqual([])
    expect(wire.observed.warnings).toEqual([])
  }, 30000)

  test('classic: a client-side pattern still never names a topic in Metadata and never sets the creation bit', async () => {
    const wire = scenario('classic', {
      groupId: 'lifecycle-client-regex-bit',
      allowAutoTopicCreation: true,
    })
    await wire.consumer.connect()
    await wire.consumer.subscribe({ topics: [/^lifecycle-/], fromBeginning: true })
    await wire.run()
    await until(() => offsets(wire.observed, DOOMED).length >= 3, 'records of a match')

    const metadata = wire.cluster.requests().filter(({ api }) => api === 'Metadata')
    expect(metadata.filter(({ fields }) => fields.topics !== null)).toEqual([])
    expect(new Set(metadata.map(({ fields }) => fields.allowAutoTopicCreation))).toEqual(
      new Set([false])
    )
  }, 30000)

  test.each([
    [17, 'INVALID_TOPIC_EXCEPTION'],
    [29, 'TOPIC_AUTHORIZATION_FAILED'],
  ])(
    'classic: code %i for a subscribed topic still stops the run with %s',
    async (code, type) => {
      const wire = scenario('classic')
      await wire.consumer.connect()
      await wire.consumer.subscribe({ topics: [HEALTHY, DOOMED], fromBeginning: true })
      await wire.run()
      await until(() => offsets(wire.observed, DOOMED).length >= 1, 'a record from the topic')

      wire.world.refused.set(DOOMED, code)
      await until(() => wire.observed.crashes.length === 1, 'the crash')

      const [{ error, restart }] = wire.observed.crashes
      let cause = error
      while (cause.cause && !cause.type) cause = cause.cause
      expect(cause.type).toBe(type)
      expect(restart).toBe(false)
      expect(wire.observed.statuses).toEqual([])
    },
    30000
  )

  test.each([
    [17, 'INVALID_TOPIC_EXCEPTION'],
    [29, 'TOPIC_AUTHORIZATION_FAILED'],
  ])(
    'consumer: a subscription answered code 3 before code %i rejects with %s, not as a topic still to come',
    async (code, type) => {
      const TYPO = 'lifecycle-typo'
      const wire = scenario('consumer')
      wire.world.remove(DOOMED)
      wire.world.refused.set(TYPO, code)
      const answered = []
      wire.cluster.answer('Metadata', ({ topics }) => {
        const answer = wire.world.metadata(topics)
        answer.topics.sort((a, b) => (b.errorCode === 3) - (a.errorCode === 3))
        answered.push(answer.topics.map(({ errorCode }) => errorCode))
        return answer
      })
      await wire.consumer.connect()

      await expect(
        wire.consumer.subscribe({ topics: [HEALTHY, DOOMED, TYPO], fromBeginning: true })
      ).rejects.toMatchObject({ type })
      expect(answered).toHaveLength(1)
      expect(answered[0][0]).toBe(3)
      expect([...answered[0]].sort()).toEqual([0, 3, code].sort())
      expect(wire.observed.statuses).toEqual([])
    },
    30000
  )

  test('unsubscribing drops the state of an unavailable topic and reports nothing for it', async () => {
    const wire = scenario('classic')
    await wire.consumer.connect()
    await wire.consumer.subscribe({ topics: [HEALTHY, DOOMED], fromBeginning: true })
    await wire.run()
    await until(() => offsets(wire.observed, DOOMED).length >= 1, 'a record from the topic')
    wire.world.remove(DOOMED)
    await until(() => wire.observed.statuses.length === 1, 'the unavailable status')

    await wire.consumer.unsubscribe()
    wire.world.recreate(DOOMED, RECREATED_ID)
    await wire.consumer.subscribe({ topics: [DOOMED], fromBeginning: true })

    expect(wire.observed.statuses).toEqual([
      expect.objectContaining({ topic: DOOMED, status: 'unavailable' }),
    ])
    expect(wire.observed.crashes).toEqual([])
  }, 30000)

  /** @param {object} observed @param {number} after @returns {boolean} whether the new incarnation began at 0 */
  const observedRecreatedFromStart = (observed, after) =>
    observed.records.slice(after).find(({ topic }) => topic === DOOMED)?.offset === '0'

  /** @param {Error} error a crash error @returns {Error} the first cause that carries a type */
  const typed = (error) => {
    let cause = error
    while (cause.cause && !cause.type) cause = cause.cause
    return cause
  }

  test('classic: a subscription whose validation failed is not established, so a later code 3 still stops the run', async () => {
    const TYPO = 'lifecycle-typo'
    const wire = scenario('classic')
    await wire.consumer.connect()
    wire.world.refused.set(TYPO, 42)
    await expect(
      wire.consumer.subscribe({ topics: [HEALTHY, TYPO], fromBeginning: true })
    ).rejects.toMatchObject({ type: 'INVALID_REQUEST' })

    wire.world.refused.set(TYPO, 3)
    await wire.run()
    await until(() => wire.observed.crashes.length === 1, 'the crash')

    expect(typed(wire.observed.crashes[0].error).type).toBe('UNKNOWN_TOPIC_OR_PARTITION')
    expect(wire.observed.statuses).toEqual([])
  }, 30000)

  test('manual: a topic deleted after assign() validated it and recreated before it was read starts its new incarnation at the beginning, not at the old committed offset', async () => {
    const wire = scenario(
      'classic',
      { groupId: 'lifecycle-manual-early' },
      { committed: { 0: '73' } }
    )
    await wire.consumer.connect()
    await wire.consumer.assign([
      { topic: HEALTHY, partitions: [0] },
      { topic: DOOMED, partitions: [0] },
    ])
    wire.world.remove(DOOMED)
    await wire.run()
    await until(() => wire.observed.statuses.length === 1, 'the unavailable status')
    await until(() => offsets(wire.observed, HEALTHY).length >= 3, 'healthy records')

    wire.world.recreate(DOOMED, RECREATED_ID)
    await until(
      () => offsets(wire.observed, DOOMED).length >= 1,
      'a record from the new incarnation'
    )

    expect(offsets(wire.observed, HEALTHY)[0]).toBe('73')
    expect(offsets(wire.observed, DOOMED)[0]).toBe('0')
    const firstAsked = wire.cluster
      .requests()
      .filter(({ api }) => api === 'Fetch')
      .flatMap(({ fields }) => fields.topics)
      .find(({ topicId }) => topicId === RECREATED_ID)
    expect(firstAsked.partitions[0].fetchOffset).toBe('0')
    expect(wire.observed.crashes).toEqual([])
  }, 30000)

  test.each([
    [17, 'INVALID_TOPIC_EXCEPTION'],
    [29, 'TOPIC_AUTHORIZATION_FAILED'],
  ])(
    'classic: code %i for one topic stops the run with %s even when a healthy topic answers a partition error',
    async (code, type) => {
      const wire = scenario('classic')
      await wire.consumer.connect()
      await wire.consumer.subscribe({ topics: [HEALTHY, DOOMED], fromBeginning: true })
      await wire.run()
      await until(() => offsets(wire.observed, DOOMED).length >= 1, 'a record from the topic')

      wire.world.partitionErrors.set(HEALTHY, 6)
      wire.world.refused.set(DOOMED, code)
      await until(() => wire.observed.crashes.length === 1, 'the crash')

      expect(typed(wire.observed.crashes[0].error).type).toBe(type)
      expect(wire.observed.statuses).toEqual([])
    },
    30000
  )

  test('a status listener that throws null and a log sink that throws cannot stop the run', async () => {
    const wire = scenario('classic')
    wire.observed.loggerThrows = true
    wire.consumer.on(wire.consumer.events.TOPIC_METADATA_STATUS, () => {
      throw null // eslint-disable-line no-throw-literal
    })
    await wire.consumer.connect()
    await wire.consumer.subscribe({ topics: [HEALTHY, DOOMED], fromBeginning: true })
    await wire.run()

    const { after } = await deleteAndRecreate(wire)

    expect(wire.observed.crashes).toEqual([])
    expect(wire.observed.statuses.map(({ status }) => status)).toEqual(['unavailable', 'available'])
    expect(observedRecreatedFromStart(wire.observed, after)).toBe(true)
  }, 30000)

  test('a listener whose promise rejects with null is logged, not left as an unhandled rejection', async () => {
    const wire = scenario('classic')
    const unhandled = []
    const onUnhandled = (reason) => unhandled.push(reason)
    process.on('unhandledRejection', onUnhandled)
    try {
      wire.consumer.on(wire.consumer.events.TOPIC_METADATA_STATUS, () => Promise.reject(null)) // eslint-disable-line prefer-promise-reject-errors
      await wire.consumer.connect()
      await wire.consumer.subscribe({ topics: [HEALTHY, DOOMED], fromBeginning: true })
      await wire.run()
      await until(() => offsets(wire.observed, DOOMED).length >= 1, 'a record from the topic')
      wire.world.remove(DOOMED)
      await until(() => wire.observed.statuses.length === 1, 'the unavailable status')
      await until(
        () =>
          wire.observed.errors.some(({ message }) =>
            message.startsWith('Failed to execute listener')
          ),
        'the listener failure log'
      )
      await sleep(20)
    } finally {
      process.off('unhandledRejection', onUnhandled)
    }

    expect(unhandled).toEqual([])
    expect(wire.observed.errors).toContainEqual(
      expect.objectContaining({ message: 'Failed to execute listener: undefined' })
    )
  }, 30000)

  test.each([
    ['classic', 'a literal subscription', { topics: [HEALTHY, DOOMED] }],
    ['consumer', 'a literal subscription', { topics: [HEALTHY, DOOMED] }],
    ['classic', 'a client-side pattern', { topics: [/^lifecycle-/] }],
    ['consumer', 'a broker-side pattern', { regex: '^lifecycle-.*' }],
  ])(
    '%s: %s reads a recreated topic from its start, never from the offset committed for the deleted one',
    async (protocol, kind, subscription) => {
      const wire = scenario(
        protocol,
        { groupId: `lifecycle-stale-${protocol}-${kind.split(' ')[1]}` },
        { committed: { 0: '73' } }
      )
      await wire.consumer.connect()
      await wire.consumer.subscribe(subscription)
      await wire.run()
      await until(() => offsets(wire.observed, DOOMED).length >= 3, 'records from the topic')
      expect(offsets(wire.observed, DOOMED)[0]).toBe('73')

      wire.world.remove(DOOMED)
      await until(
        () => offsets(wire.observed, HEALTHY).length >= 8,
        'healthy records after the deletion'
      )
      const after = wire.observed.records.length
      wire.world.recreate(DOOMED, RECREATED_ID)
      await until(
        () => wire.observed.records.slice(after).some(({ topic }) => topic === DOOMED),
        'a record from the new incarnation'
      )

      expect(observedRecreatedFromStart(wire.observed, after)).toBe(true)
      const firstAsked = wire.cluster
        .requests()
        .filter(({ api }) => api === 'Fetch')
        .flatMap(({ fields }) => fields.topics)
        .find(({ topicId }) => topicId === RECREATED_ID)
      expect(firstAsked.partitions[0].fetchOffset).toBe('0')
      expect(wire.observed.crashes).toEqual([])
    },
    30000
  )

  describe('consumer: a topic the coordinator assigns that a lookup by name does not know yet', () => {
    const NEW = 'lifecycle-new'
    const NEW_ID = '0a0a0a0a-0000-4000-8000-000000000004'
    const TYPO = 'lifecycle-typo'
    const TYPO_ID = '0a0a0a0a-0000-4000-8000-000000000005'

    /** @param {object} cluster @returns {object[]} the heartbeats sent, oldest first */
    const heartbeats = (cluster) =>
      cluster
        .requests()
        .filter(({ api }) => api === 'ConsumerGroupHeartbeat')
        .map(({ fields }) => fields)

    /** @param {object} cluster @returns {string[][]} the topic ids each acknowledgement carried */
    const acks = (cluster) =>
      heartbeats(cluster)
        .filter(({ memberEpoch, topicPartitions }) => memberEpoch > 0 && topicPartitions != null)
        .map(({ topicPartitions }) => topicPartitions.map(({ topicId }) => topicId).sort())

    /** @param {object} cluster @param {string} name @returns {object[]} lookups by name of it */
    const namedAsks = (cluster, name) =>
      cluster
        .requests()
        .filter(
          ({ api, fields }) =>
            api === 'Metadata' && (fields.topics || []).some((topic) => topic.name === name)
        )

    /** @param {object} cluster @returns {Set<string>} every topic id a Fetch asked for */
    const fetched = (cluster) =>
      new Set(
        cluster
          .requests()
          .filter(({ api }) => api === 'Fetch')
          .flatMap(({ fields }) => fields.topics.map(({ topicId }) => topicId))
      )

    /**
     * A consumer-protocol scenario that also records assignment events.
     * @param {object} [options] consumer options over the scenario's
     * @returns {object} the scenario with `assigned` and `revoked` event payloads
     */
    const assignedScenario = (options = {}) => {
      const wire = scenario('consumer', { groupId: `pending-${Math.random()}`, ...options })
      wire.world.ids.set(NEW, NEW_ID)
      wire.assigned = []
      wire.revoked = []
      wire.joins = 0
      wire.consumer.on(wire.consumer.events.PARTITIONS_ASSIGNED, ({ payload }) =>
        wire.assigned.push(payload)
      )
      wire.consumer.on(wire.consumer.events.PARTITIONS_REVOKED, ({ payload }) =>
        wire.revoked.push(payload)
      )
      wire.consumer.on(wire.consumer.events.GROUP_JOIN, () => wire.joins++)
      return wire
    }

    /** @param {object} payloads @param {string} name @returns {boolean} whether any names it */
    const names = (payloads, name) => JSON.stringify(payloads).includes(`"${name}"`)

    test('a broker-side pattern keeps the healthy topics, fetches a new healthy sibling at once and holds the unknown name out of the ACK, the Fetch and PARTITIONS_ASSIGNED, reporting it once', async () => {
      const wire = assignedScenario()
      wire.world.namedCodes.set(DOOMED, 3)
      await wire.consumer.connect()
      await wire.consumer.subscribe({ regex: '^lifecycle-.*', fromBeginning: true })
      await wire.run()

      await until(
        () =>
          offsets(wire.observed, HEALTHY).length >= 3 && offsets(wire.observed, NEW).length >= 3,
        'records from both healthy topics'
      )
      await until(() => namedAsks(wire.cluster, DOOMED).length >= 4, 'repeated lookups by name')

      expect(wire.observed.crashes).toEqual([])
      expect(fetched(wire.cluster).has(DOOMED_ID)).toBe(false)
      expect(acks(wire.cluster).flat()).not.toContain(DOOMED_ID)
      expect(acks(wire.cluster)).toContainEqual([HEALTHY_ID, NEW_ID].sort())
      expect(names(wire.assigned, DOOMED)).toBe(false)
      expect(wire.observed.statuses).toEqual([
        expect.objectContaining({ topic: DOOMED, status: 'unavailable', code: 3 }),
      ])
      expect(wire.observed.warnings).toHaveLength(1)
    }, 30000)

    test('an awaiting literal refused at its first add keeps its declaration and the healthy delivery, and is read from its start once it is known', async () => {
      const wire = assignedScenario()
      wire.world.remove(DOOMED)
      await wire.consumer.connect()
      await wire.consumer.subscribe({ topics: [HEALTHY, DOOMED], fromBeginning: true })
      await wire.run()
      await until(() => offsets(wire.observed, HEALTHY).length >= 3, 'healthy records')

      wire.world.namedCodes.set(DOOMED, 3)
      wire.world.recreate(DOOMED, DOOMED_ID)
      await until(() => namedAsks(wire.cluster, DOOMED).length >= 3, 'lookups by name')
      const healthy = offsets(wire.observed, HEALTHY).length
      await until(
        () => offsets(wire.observed, HEALTHY).length >= healthy + 3,
        'healthy records while the literal is refused'
      )
      expect(wire.observed.crashes).toEqual([])
      expect(fetched(wire.cluster).has(DOOMED_ID)).toBe(false)
      expect(heartbeats(wire.cluster).at(-1).subscribedTopicNames ?? wire.world.names).toContain(
        DOOMED
      )

      wire.world.namedCodes.delete(DOOMED)
      await until(() => offsets(wire.observed, DOOMED).length >= 1, 'a record from the topic')
      expect(offsets(wire.observed, DOOMED)[0]).toBe('0')
      expect(wire.observed.crashes).toEqual([])
    }, 30000)

    test.each([
      ['answers null', false, [HEALTHY, DOOMED]],
      ['repeats the assignment', true, [HEALTHY, DOOMED]],
      ['answers null to an all-pending target', false, [DOOMED]],
      ['repeats an all-pending target', true, [DOOMED]],
    ])(
      'a coordinator that %s sees one acknowledgement, regular heartbeats, no repeated applies and paced lookups while the name stays unknown, then one addition when it is known under the same id',
      async (_, repeat, topics) => {
        const wire = assignedScenario()
        wire.world.repeat = repeat
        wire.world.namedCodes.set(DOOMED, 3)
        await wire.consumer.connect()
        await wire.consumer.subscribe({ topics, fromBeginning: true })
        await wire.run()
        await until(() => wire.observed.statuses.length === 1, 'the unavailable status')
        await sleep(200)

        const ackCount = acks(wire.cluster).length
        const joins = wire.joins
        const sentAt = heartbeats(wire.cluster).length
        const askedAt = namedAsks(wire.cluster, DOOMED).length
        const started = Date.now()
        await sleep(600)
        const elapsed = Date.now() - started

        expect(acks(wire.cluster)).toHaveLength(ackCount)
        expect(ackCount).toBe(1)
        expect(acks(wire.cluster)[0]).toEqual(topics.includes(HEALTHY) ? [HEALTHY_ID] : [])
        expect(wire.joins).toBe(joins)
        expect(heartbeats(wire.cluster).length - sentAt).toBeLessThanOrEqual(elapsed / 50 + 2)
        expect(namedAsks(wire.cluster, DOOMED).length - askedAt).toBeLessThanOrEqual(
          elapsed / 100 + 2
        )
        expect(wire.observed.crashes).toEqual([])

        wire.world.namedCodes.delete(DOOMED)
        await until(() => offsets(wire.observed, DOOMED).length >= 1, 'a record from the topic')
        await sleep(200)
        expect(acks(wire.cluster).slice(ackCount)).toEqual([
          (topics.includes(HEALTHY) ? [HEALTHY_ID, DOOMED_ID] : [DOOMED_ID]).sort(),
        ])
        expect(wire.joins).toBe(joins + 1)
        expect(wire.observed.statuses.map(({ status }) => status)).toEqual([
          'unavailable',
          'available',
        ])
      },
      30000
    )

    /**
     * A broker-side pattern whose first add of `DOOMED` is held at the broker, with the
     * coordinator's one withdrawal of it held too. Every other lookup by name answers it code 3.
     * @param {boolean} heldKnows whether the held lookup answers it as a live topic
     * @returns {Promise<object>} the scenario and the releases of both holds
     */
    const heldWithdrawal = async (heldKnows) => {
      const wire = assignedScenario()
      const gates = {}
      const hold = (key) => new Promise((resolve) => (gates[key] = resolve))
      let metadataHeld = null
      let withdrawalHeld = null
      wire.cluster.answer('Metadata', async ({ topics }) => {
        if (!metadataHeld && topics?.some(({ name }) => name === DOOMED)) {
          metadataHeld = hold('metadata')
          await metadataHeld
          if (heldKnows) {
            wire.world.namedCodes.delete(DOOMED)
            const answer = wire.world.metadata(topics)
            wire.world.namedCodes.set(DOOMED, 3)
            return answer
          }
        }
        return wire.world.metadata(topics)
      })
      wire.cluster.answer('ConsumerGroupHeartbeat', async (request) => {
        const answer = wire.world.heartbeat(request)
        const ids = answer.assignment?.topicPartitions.map(({ topicId }) => topicId)
        if (!withdrawalHeld && wire.world.withheld.has(DOOMED) && ids && !ids.includes(DOOMED_ID)) {
          withdrawalHeld = hold('withdrawal')
          await withdrawalHeld
        }
        return answer
      })
      wire.world.ids.delete(NEW)
      wire.world.ids.delete(DOOMED)
      await wire.consumer.connect()
      await wire.consumer.subscribe({ regex: '^lifecycle-.*', fromBeginning: true })
      await wire.run()
      await until(() => offsets(wire.observed, HEALTHY).length >= 2, 'healthy records')

      wire.world.namedCodes.set(DOOMED, 3)
      wire.world.ids.set(NEW, NEW_ID)
      wire.world.ids.set(DOOMED, DOOMED_ID)
      await until(() => gates.metadata, 'the held first add')
      wire.world.withheld.add(DOOMED)
      await until(() => gates.withdrawal, 'the held withdrawal')
      return { wire, gates }
    }

    /** The withdrawal stuck: no later lookup, no revocation, healthy delivery goes on. */
    const expectWithdrawn = async (wire) => {
      await until(() => offsets(wire.observed, NEW).length >= 2, 'records from the new sibling')
      await sleep(300)
      const asked = namedAsks(wire.cluster, DOOMED).length
      const healthy = offsets(wire.observed, HEALTHY).length
      await sleep(400)
      expect(namedAsks(wire.cluster, DOOMED)).toHaveLength(asked)
      expect(offsets(wire.observed, HEALTHY).length).toBeGreaterThan(healthy)
      expect(fetched(wire.cluster).has(DOOMED_ID)).toBe(false)
      expect(acks(wire.cluster).at(-1)).toEqual([HEALTHY_ID, NEW_ID].sort())
      expect(names(wire.revoked, DOOMED)).toBe(false)
      expect(wire.observed.crashes).toEqual([])
    }

    test('a withdrawal held while the first add is answered code 3 and a sibling is applied still sticks', async () => {
      const { wire, gates } = await heldWithdrawal(false)
      gates.metadata()
      await until(() => names(wire.assigned, NEW), 'the sibling applied')
      gates.withdrawal()

      await expectWithdrawn(wire)
      expect(wire.observed.statuses).toEqual([
        expect.objectContaining({ topic: DOOMED, status: 'unavailable' }),
      ])
    }, 30000)

    test('a withdrawal released before a late successful first add still sticks and reports no recovery', async () => {
      const { wire, gates } = await heldWithdrawal(true)
      gates.withdrawal()
      await sleep(100)
      gates.metadata()

      await expectWithdrawn(wire)
      expect(wire.observed.statuses.map(({ status }) => status)).not.toContain('available')
    }, 30000)

    test.each([
      [17, 'the first add', 'INVALID_TOPIC_EXCEPTION'],
      [29, 'the first add', 'TOPIC_AUTHORIZATION_FAILED'],
      [17, 'a due retry', 'INVALID_TOPIC_EXCEPTION'],
      [29, 'a due retry', 'TOPIC_AUTHORIZATION_FAILED'],
    ])(
      'code 3 answered before code %i on %s stops the run with %s and acknowledges and reports nothing of that answer',
      async (code, when, type) => {
        const wire = assignedScenario()
        wire.cluster.answer('Metadata', ({ topics }) => {
          const answer = wire.world.metadata(topics)
          answer.topics.sort((a, b) => (b.errorCode === 3) - (a.errorCode === 3))
          return answer
        })
        wire.world.namedCodes.set(DOOMED, 3)
        if (when === 'the first add') wire.world.namedCodes.set(NEW, code)
        else wire.world.ids.delete(NEW)
        await wire.consumer.connect()
        await wire.consumer.subscribe({ regex: '^lifecycle-.*', fromBeginning: true })
        await wire.run()
        if (when === 'a due retry') {
          await until(() => wire.observed.statuses.length === 1, 'the unavailable status')
          await until(() => offsets(wire.observed, HEALTHY).length >= 1, 'healthy records')
          wire.world.namedCodes.set(HEALTHY, code)
        }
        await until(() => wire.observed.crashes.length === 1, 'the crash')

        expect(typed(wire.observed.crashes[0].error).type).toBe(type)
        expect(wire.observed.crashes[0].restart).toBe(false)
        if (when === 'the first add') {
          expect(acks(wire.cluster)).toEqual([])
          expect(wire.observed.statuses).toEqual([])
        } else {
          expect(wire.observed.statuses).toHaveLength(1)
        }
      },
      30000
    )

    test('code 29 for an assigned topic id stops the run with TOPIC_AUTHORIZATION_FAILED', async () => {
      const wire = assignedScenario()
      wire.world.ids.set(TYPO, TYPO_ID)
      wire.world.idCodes.set(TYPO_ID, 29)
      await wire.consumer.connect()
      await wire.consumer.subscribe({ regex: '^lifecycle-.*', fromBeginning: true })
      await wire.run().catch(() => {})
      await until(() => wire.observed.crashes.length === 1, 'the crash')

      expect(typed(wire.observed.crashes[0].error).type).toBe('TOPIC_AUTHORIZATION_FAILED')
      expect(fetched(wire.cluster).has(TYPO_ID)).toBe(false)
    }, 30000)

    test.each([false, true])(
      'a lookup by name that finds a new id while the coordinator still assigns the old one fetches neither until the coordinator assigns the new id, then reads it from the configured reset position (creation %s)',
      async (allowAutoTopicCreation) => {
        const wire = assignedScenario({ allowAutoTopicCreation })
        wire.world.remove(DOOMED)
        await wire.consumer.connect()
        await wire.consumer.subscribe({ topics: [HEALTHY, DOOMED], fromBeginning: true })
        await wire.run()
        await until(() => offsets(wire.observed, HEALTHY).length >= 2, 'healthy records')

        wire.world.recreate(DOOMED, DOOMED_ID)
        wire.world.namedCodes.set(DOOMED, 3)
        await until(() => wire.observed.statuses.length === 1, 'the unavailable status')
        wire.world.lagged.set(DOOMED, DOOMED_ID)
        wire.world.namedCodes.delete(DOOMED)
        wire.world.recreate(DOOMED, RECREATED_ID)
        await until(() => wire.observed.statuses.length === 2, 'the available status')
        await sleep(300)

        expect(fetched(wire.cluster).has(DOOMED_ID)).toBe(false)
        expect(fetched(wire.cluster).has(RECREATED_ID)).toBe(false)
        expect(acks(wire.cluster).flat()).not.toContain(DOOMED_ID)

        wire.world.lagged.delete(DOOMED)
        await until(() => offsets(wire.observed, DOOMED).length >= 1, 'a record from the topic')
        const firstAsked = wire.cluster
          .requests()
          .filter(({ api }) => api === 'Fetch')
          .flatMap(({ fields }) => fields.topics)
          .find(({ topicId }) => topicId === RECREATED_ID)
        expect(firstAsked.partitions[0].fetchOffset).toBe('0')
        expect(acks(wire.cluster).flat()).not.toContain(DOOMED_ID)
        const bits = namedAsks(wire.cluster, DOOMED).map(
          ({ fields }) => fields.allowAutoTopicCreation
        )
        expect(bits.length).toBeGreaterThan(1)
        expect(new Set(bits)).toEqual(new Set([allowAutoTopicCreation]))
        expect(wire.observed.crashes).toEqual([])
      },
      30000
    )

    test.each(['stop', 'unsubscribe'])(
      '%s while a due lookup of an unknown assigned name is held leaves no late report, Fetch, acknowledgement or lookup',
      async (operation) => {
        const wire = assignedScenario()
        wire.world.namedCodes.set(DOOMED, 3)
        await wire.consumer.connect()
        await wire.consumer.subscribe({ regex: '^lifecycle-.*', fromBeginning: true })
        await wire.run()
        await until(() => wire.observed.statuses.length === 1, 'the unavailable status')
        let release = null
        wire.cluster.answer('Metadata', async ({ topics }) => {
          if (!release && topics?.some(({ name }) => name === DOOMED)) {
            await new Promise((resolve) => (release = resolve))
          }
          return wire.world.metadata(topics)
        })
        await until(() => release, 'the held lookup')

        await wire.consumer[operation]()
        wire.world.namedCodes.delete(DOOMED)
        const requests = wire.cluster.requests().length
        release()
        await sleep(300)

        expect(wire.cluster.requests().slice(requests)).toEqual([])
        expect(wire.observed.statuses).toHaveLength(1)
        expect(fetched(wire.cluster).has(DOOMED_ID)).toBe(false)
        expect(wire.observed.crashes).toEqual([])

        if (operation === 'stop') {
          wire.world.withheld.add(DOOMED)
          const askedAtRestart = namedAsks(wire.cluster, DOOMED).length
          await wire.run()
          await until(() => offsets(wire.observed, NEW).length >= 2, 'records after the restart')
          await sleep(300)
          expect(namedAsks(wire.cluster, DOOMED)).toHaveLength(askedAtRestart)
          expect(fetched(wire.cluster).has(DOOMED_ID)).toBe(false)
          expect(wire.observed.statuses).toHaveLength(1)
        }
      },
      30000
    )

    test('a name known while a sibling apply is held gets one further apply and one acknowledgement with both', async () => {
      const wire = assignedScenario()
      wire.world.namedCodes.set(DOOMED, 3)
      wire.world.ids.delete(NEW)
      let releaseHook = null
      await wire.consumer.connect()
      await wire.consumer.subscribe({ regex: '^lifecycle-.*', fromBeginning: true })
      await wire.consumer.run({
        rebalanceListener: {
          onPartitionsAssigned: async ({ partitions }) => {
            if (JSON.stringify(partitions).includes(NEW)) {
              await new Promise((resolve) => (releaseHook = resolve))
            }
          },
        },
        eachMessage: async ({ topic, message }) => {
          wire.observed.records.push({ topic, offset: message.offset })
        },
      })
      await until(() => wire.observed.statuses.length === 1, 'the unavailable status')
      const joins = wire.joins

      wire.world.ids.set(NEW, NEW_ID)
      await until(() => releaseHook, 'the held sibling apply')
      wire.world.namedCodes.delete(DOOMED)
      await until(() => wire.observed.statuses.length === 2, 'the available status')
      releaseHook()
      await until(() => offsets(wire.observed, DOOMED).length >= 1, 'a record from the topic')
      await sleep(300)

      expect(wire.joins).toBe(joins + 2)
      expect(acks(wire.cluster).at(-1)).toEqual([HEALTHY_ID, NEW_ID, DOOMED_ID].sort())
      expect(acks(wire.cluster).filter((ids) => ids.includes(DOOMED_ID))).toHaveLength(1)
      expect(wire.observed.crashes).toEqual([])
    }, 30000)

    /** @param {object} cluster @returns {object[]} every Metadata request that names a topic */
    const lookupsByName = (cluster) =>
      cluster
        .requests()
        .filter(
          ({ api, fields }) => api === 'Metadata' && (fields.topics || []).some(({ name }) => name)
        )

    test.each([
      ['false, unchanged behaviour', false],
      ['true', true],
    ])(
      'a broker-side regex sends no creation bit on any lookup by name: the registration of an assigned name, the retry of a name unknown at its first add and the periodic refresh (creation %s)',
      async (_, allowAutoTopicCreation) => {
        const wire = assignedScenario({ allowAutoTopicCreation })
        wire.world.namedCodes.set(DOOMED, 3)
        await wire.consumer.connect()
        await wire.consumer.subscribe({ regex: '^lifecycle-.*', fromBeginning: true })
        await wire.run()
        await until(() => offsets(wire.observed, NEW).length >= 1, 'a record of the new name')
        await until(() => namedAsks(wire.cluster, DOOMED).length >= 3, 'retried lookups')
        await until(() => namedAsks(wire.cluster, HEALTHY).length >= 3, 'periodic refreshes')

        expect(namedAsks(wire.cluster, NEW).length).toBeGreaterThan(0)
        const bits = lookupsByName(wire.cluster).map(({ fields }) => fields.allowAutoTopicCreation)
        expect(new Set(bits)).toEqual(new Set([false]))
        expect(wire.observed.crashes).toEqual([])
      },
      30000
    )

    test('after unsubscribing a broker-side regex, a literal subscription sends the configured creation bit again', async () => {
      const wire = assignedScenario({ allowAutoTopicCreation: true })
      await wire.consumer.connect()
      await wire.consumer.subscribe({ regex: '^lifecycle-.*', fromBeginning: true })
      await wire.run()
      await until(() => namedAsks(wire.cluster, HEALTHY).length >= 2, 'lookups under the regex')
      await wire.consumer.unsubscribe()
      const before = lookupsByName(wire.cluster).length

      await wire.consumer.subscribe({ topics: [HEALTHY], fromBeginning: true })
      await wire.run()
      await until(
        () => lookupsByName(wire.cluster).length >= before + 3,
        'lookups under the literal'
      )

      const bits = (requests) =>
        new Set(requests.map(({ fields }) => fields.allowAutoTopicCreation))
      expect(bits(lookupsByName(wire.cluster).slice(0, before))).toEqual(new Set([false]))
      expect(bits(lookupsByName(wire.cluster).slice(before))).toEqual(new Set([true]))
      expect(wire.observed.crashes).toEqual([])
    }, 30000)
    /**
     * A coordinator with the revoke-before-assign rule of the Kafka group coordinator: when its
     * target drops `topicId`, a member that has not reported owned partitions without it keeps its
     * epoch and is sent the assignment without it, and gets the new target only once a heartbeat
     * reports owned partitions without it. A heartbeat that reports none still owns it.
     * @param {object} wire the scenario
     * @param {string} topicId the id the coordinator drops
     * @returns {{ freeze: (frozen: boolean) => void }} while frozen, the coordinator answers no change
     */
    const revokingCoordinator = (wire, topicId) => {
      let frozen = false
      let pending = false
      const unchanged = ({ memberId }, assignment = null) => ({
        memberId,
        memberEpoch: wire.world.epoch,
        heartbeatIntervalMs: 50,
        assignment,
      })
      wire.cluster.answer('ConsumerGroupHeartbeat', (request) => {
        if (frozen) return unchanged(request)
        const reported = request.topicPartitions?.map((topic) => topic.topicId)
        const owns = !reported || reported.includes(topicId)
        if (pending) {
          if (owns) return unchanged(request)
          pending = false
        } else if (owns && wire.world.assigned?.split(',').includes(topicId)) {
          const before = { epoch: wire.world.epoch, assigned: wire.world.assigned }
          const next = wire.world.heartbeat(request)
          const ids = next.assignment?.topicPartitions.map((topic) => topic.topicId)
          if (!ids || ids.includes(topicId)) return next
          Object.assign(wire.world, before)
          pending = true
          const kept = before.assigned.split(',').filter((id) => id !== topicId)
          return unchanged(request, {
            topicPartitions: kept.map((id) => ({ topicId: id, partitions: [0] })),
          })
        }
        return wire.world.heartbeat(request)
      })
      return { freeze: (value) => (frozen = value) }
    }

    test('an awaiting literal whose assigned id is deleted at its first add and recreated under a new id acknowledges the revocation of the old id and is read from its start', async () => {
      const wire = assignedScenario()
      wire.world.ids.delete(NEW)
      wire.world.remove(DOOMED)
      const coordinator = revokingCoordinator(wire, DOOMED_ID)
      let deleted = false
      wire.cluster.answer('Metadata', ({ topics }) => {
        const answer = wire.world.metadata(topics)
        if (!deleted && topics?.some(({ name, topicId }) => !name && topicId === DOOMED_ID)) {
          deleted = true
          wire.world.remove(DOOMED)
          coordinator.freeze(true)
        }
        return answer
      })
      await wire.consumer.connect()
      await wire.consumer.subscribe({ topics: [HEALTHY, DOOMED], fromBeginning: true })
      await wire.run()
      await until(() => offsets(wire.observed, HEALTHY).length >= 3, 'healthy records')

      wire.world.recreate(DOOMED, DOOMED_ID)
      await until(() => wire.observed.statuses.length === 1, 'the unavailable status')
      expect(deleted).toBe(true)
      wire.world.recreate(DOOMED, RECREATED_ID)
      await until(() => wire.observed.statuses.length === 2, 'the available status')
      coordinator.freeze(false)

      await until(() => offsets(wire.observed, DOOMED).length >= 1, 'a record from the topic')
      expect(offsets(wire.observed, DOOMED)[0]).toBe('0')
      expect(fetched(wire.cluster).has(DOOMED_ID)).toBe(false)
      expect(acks(wire.cluster)).toContainEqual([HEALTHY_ID])
      expect(acks(wire.cluster).at(-1)).toEqual([HEALTHY_ID, RECREATED_ID].sort())
      expect(wire.observed.crashes).toEqual([])
    }, 30000)
  })

  test('a classic consumer, a producer and an admin of one client, and a share consumer, still fail on a missing topic and report no status', async () => {
    const world = new World()
    world.remove(DOOMED)
    const cluster = createScriptedCluster({ nodes: NODES, profile: 'consumer' })
    cluster.answer('Metadata', ({ topics }) => world.metadata(topics))
    const kafka = new Kafka(cluster.kafkaOptions({ retry: RETRY, logLevel: logLevel.NOTHING }))
    const statuses = []
    const consumer = kafka.consumer({ groupId: 'strict-classic', allowAutoTopicCreation: false })
    consumer.on(consumer.events.TOPIC_METADATA_STATUS, ({ payload }) => statuses.push(payload))
    const producer = kafka.producer({ allowAutoTopicCreation: false })
    const admin = kafka.admin()
    const shareCluster = createScriptedCluster({ nodes: NODES, profile: 'share' })
    shareCluster.answer('Metadata', ({ topics }) => world.metadata(topics))
    const share = new Kafka(
      shareCluster.kafkaOptions({ retry: RETRY, logLevel: logLevel.NOTHING })
    ).shareConsumer({ groupId: 'strict-share' })
    consumers.push(consumer, producer, admin, share)
    const failure = async (promise) =>
      typed(
        await promise.then(
          () => null,
          (error) => error
        )
      )

    await consumer.connect()
    await producer.connect()
    await admin.connect()
    await share.connect()

    expect(
      await failure(consumer.subscribe({ topics: [HEALTHY, DOOMED], fromBeginning: true }))
    ).toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })
    expect(
      await failure(producer.send({ topic: DOOMED, messages: [{ value: 'x' }] }))
    ).toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })
    expect(await failure(admin.fetchTopicMetadata({ topics: [DOOMED] }))).toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
    })
    expect(await failure(share.subscribe({ topics: [DOOMED] }))).toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
    })
    expect(statuses).toEqual([])
  }, 30000)

  describe('with the default metadata age, as the live recreation checks run', () => {
    /**
     * Subscribes to the doomed topic alone, as the live recreation checks do, deletes it once its
     * first records were read and recreates it.
     * @param {object} [script] options of the group script
     * @param {object} [runOptions] options of `run`
     * @returns {Promise<object>} the scenario and where the new incarnation's records begin
     */
    const recreateAlone = async (script = {}, runOptions = {}) => {
      const wire = scenario('classic', { metadataMaxAge: undefined }, script)
      const recreated = []
      const joins = []
      wire.consumer.on(wire.consumer.events.TOPIC_RECREATED, ({ payload }) =>
        recreated.push(payload)
      )
      wire.consumer.on(wire.consumer.events.GROUP_JOIN, ({ payload }) => joins.push(payload))
      await wire.consumer.connect()
      await wire.consumer.subscribe({ topics: [DOOMED], fromBeginning: true })
      await wire.consumer.run({
        ...runOptions,
        eachMessage: async ({ topic, message }) => {
          wire.observed.records.push({ topic, offset: message.offset })
        },
      })
      await until(() => offsets(wire.observed, DOOMED).length >= 3, 'records from the topic')
      wire.world.remove(DOOMED)
      await until(() => wire.observed.statuses.length === 1, 'the unavailable status')
      const after = wire.observed.records.length
      wire.world.recreate(DOOMED, RECREATED_ID)
      await until(
        () => wire.observed.records.slice(after).some(({ topic }) => topic === DOOMED),
        'a record from the new incarnation',
        1000
      )
      return { wire, after, recreated, joins }
    }

    test('TOPIC_RECREATED fires and the consumer resumes from fromBeginning', async () => {
      const { wire, after, recreated } = await recreateAlone()

      expect(recreated).toEqual([
        expect.objectContaining({
          topic: DOOMED,
          previousTopicId: DOOMED_ID,
          topicId: RECREATED_ID,
        }),
      ])
      expect(observedRecreatedFromStart(wire.observed, after)).toBe(true)
      expect(wire.observed.crashes).toEqual([])
    }, 30000)

    test('a stale committed offset inside the recreated topic range is ignored', async () => {
      const { wire, after } = await recreateAlone({ committed: { 0: '73' } }, { autoCommit: false })

      expect(offsets(wire.observed, DOOMED)[0]).toBe('73')
      expect(observedRecreatedFromStart(wire.observed, after)).toBe(true)
    }, 30000)

    test('a recreation that leaves the partition count unchanged still resets the position, with no rejoin', async () => {
      const { wire, after, joins } = await recreateAlone({ committed: { 0: '73' } })

      expect(observedRecreatedFromStart(wire.observed, after)).toBe(true)
      expect(joins).toHaveLength(1)
    }, 30000)

    test('a recreation before the first confirmed commit resets again after stop and run', async () => {
      const { wire } = await recreateAlone({ committed: { 0: '73' } }, { autoCommit: false })

      await wire.consumer.stop()
      const restartedAt = wire.observed.records.length
      await wire.consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, message }) => {
          wire.observed.records.push({ topic, offset: message.offset })
        },
      })
      await until(
        () => wire.observed.records.slice(restartedAt).some(({ topic }) => topic === DOOMED),
        'a record after the restart'
      )

      expect(observedRecreatedFromStart(wire.observed, restartedAt)).toBe(true)
    }, 30000)
  })
})
