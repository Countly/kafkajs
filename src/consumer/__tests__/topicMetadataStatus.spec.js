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
    this.epoch = 0
    this.assigned = null
    this.names = null
    this.regex = null
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

  /** @param {string} name @returns {object} one Metadata v13 topic entry */
  describe(name) {
    if (!this.live(name)) {
      return {
        errorCode: this.refused.get(name) ?? 3,
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
          errorCode: 0,
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
        ? [...this.ids.keys()].filter((name) => this.live(name)).map((name) => this.describe(name))
        : requested.map(({ name, topicId }) => {
            if (name != null) return this.describe(name)
            const known = this.nameOf(topicId)
            return known
              ? this.describe(known)
              : { errorCode: 100, name: null, topicId, isInternal: false, partitions: [] }
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
      .filter((name) => this.live(name))
      .filter((name) => this.names?.includes(name) || this.regex?.test(name))
      .map((name) => this.ids.get(name))
      .sort()
    const key = wanted.join()
    if (memberEpoch === 0 || key !== this.assigned) {
      this.assigned = key
      this.epoch += 1
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
   * @returns {object} the world, the wire, the consumer and what it observed
   */
  const scenario = (protocol, options = {}) => {
    const world = new World()
    let cluster = createScriptedCluster({
      nodes: NODES,
      profile: protocol === 'consumer' ? 'consumer' : 'default',
    })
    cluster =
      protocol === 'consumer'
        ? consumerProtocolScript(cluster, { topic: HEALTHY })
        : groupScript(cluster, { topic: HEALTHY })
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

    const observed = { records: [], statuses: [], crashes: [], warnings: [] }
    const kafka = new Kafka(
      cluster.kafkaOptions({
        retry: RETRY,
        logLevel: logLevel.WARN,
        logCreator:
          () =>
          ({ level, log }) => {
            if (level === logLevel.WARN && log.message === UNAVAILABLE_WARN) {
              observed.warnings.push(log)
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
})
