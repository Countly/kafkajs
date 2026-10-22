const Broker = require('../index')
const { requests } = require('../../protocol/requests')
const apiKeys = require('../../protocol/requests/apiKeys')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const ISOLATION_LEVEL = require('../../protocol/isolationLevel')
const { Kafka, logLevel, PartitionAssigners } = require('../../../index')
const brokerRanges = require('../../../scripts/protocol/brokerRanges.json')
const { rangeOf } = require('../../../scripts/e2e/apiVersionsProbe')
const { readLog, endOffsets, committedRecords } = require('../../../testHelpers/rawLog')
const {
  connectionOpts,
  createConnectionPool,
  createTopic,
  newLogger,
  plainTextBrokers,
  secureRandom,
  writeEvidence,
  evidenceLines,
  wireVersionsOf,
} = require('testHelpers')
const { problemsOf } = require('../../../scripts/evidence/verify')
const { CALLS, LIMIT, overload } = require('../../network/__tests__/fixtures/inFlightOverload')
const {
  NAMES,
  GROUP: PROTOTYPE_GROUP,
  PrototypeCase,
  prototypeDescriptors,
} = require('../../consumer/__tests__/fixtures/prototypeCase')
const {
  sleep,
  waitForCondition,
  withTimeout,
} = require('../../consumer/__tests__/fixtures/liveScenario')

/**
 * The 4.0 floor core: the client's core paths against the oldest supported broker line, run with
 * `npm run test:floor`, whose `KAFKA_VERSION` names the selected floor image.
 *
 * Excluded by capability, never by a version string:
 * - the consumer-protocol case, when the broker's ApiVersions answer offers no
 *   ConsumerGroupHeartbeat (evidence: the offered table, recorded in `brokerRanges.json`);
 * - the transaction version 2 path, when the coordinator does not finalise `transaction.version`
 *   at level 2 (the version 1 path is asserted instead);
 * - share groups, outright: early access on the 4.0 line.
 *
 * Replay sets: durable log, none; committed offsets, back only to the last commit on a
 * rebalance; handler delivery, at least once after the last commit.
 */
const describeFloor = process.env.FLOOR_CORE_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'floor-core'
// The lines earlier runs left behind, so the last case judges only this run's.
const linesBefore = evidenceLines(EVIDENCE).length
/** The APIs of the scripted wire subset, plus the three admin and group APIs F01 adds. */
const NEGOTIATED = [
  'ApiVersions',
  'Metadata',
  'GroupCoordinator',
  'JoinGroup',
  'SyncGroup',
  'Heartbeat',
  'LeaveGroup',
  'OffsetFetch',
  'OffsetCommit',
  'ListOffsets',
  'Fetch',
  'OffsetForLeaderEpoch',
  'InitProducerId',
  'Produce',
  'AddPartitionsToTxn',
  'AddOffsetsToTxn',
  'TxnOffsetCommit',
  'EndTxn',
  'SaslHandshake',
  'SaslAuthenticate',
  'DescribeTopicPartitions',
  'ListPartitionReassignments',
  'ConsumerGroupHeartbeat',
  'CreateTopics',
  'DescribeConfigs',
  'DescribeGroups',
]
const RECORDS = 200
const PER_PARTITION = 10
const COMMITTED = '6'
const WAIT_BUDGET = { attempts: 240, delay: 250 } // 60 s
const QUIET_MS = 1500
const CASE_BUDGET_MS = 90000
const DISCONNECT_BUDGET_MS = 15000

/**
 * The version a negotiation should pick: the highest implemented version inside the broker's
 * offered range. Written here, apart from the client's own selection, so F01 checks it.
 * @param {Array<number|string>} implemented the codec's versions
 * @param {[number, number]|undefined} range what the broker offers
 * @returns {number|null} null when the two do not overlap
 */
const highestImplemented = (implemented, range) => {
  const usable = range
    ? implemented.map(Number).filter((version) => version >= range[0] && version <= range[1])
    : []
  return usable.length ? Math.max(...usable) : null
}

/**
 * The transaction version a coordinator's finalised features select (KIP-890): 2 when
 * `transaction.version` is finalised at level 2 or above with a valid epoch, else 1.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense
 * @param {{ [name: string]: { maxVersionLevel: number } }} features
 * @param {number} epoch
 * @returns {1|2}
 */
const transactionVersionOf = (features, epoch) =>
  epoch >= 0 && (features['transaction.version']?.maxVersionLevel ?? 0) >= 2 ? 2 : 1

/**
 * The wire versions a transaction is held to under each transaction version: the producer's
 * per-operation caps, which F01's uncapped query does not show.
 */
const TRANSACTION_CAPS = {
  1: { Produce: 11, EndTxn: 4, TxnOffsetCommit: 4 },
  2: { Produce: 12, EndTxn: 5, TxnOffsetCommit: 5 },
}

describe('Floor core expectations', () => {
  test('the expected version is the highest implemented one inside the offered range', () => {
    expect(highestImplemented([3, 4, 5, 13], [0, 12])).toBe(5)
    expect(highestImplemented(['0', '1', '2'], [0, 2])).toBe(2)
    expect(highestImplemented([4, 5], [6, 9])).toBeNull()
    expect(highestImplemented([4, 5], undefined)).toBeNull()
  })

  test('on the 4.0.2 floor the recorded ranges and the implemented codecs give these versions', () => {
    const expected = Object.fromEntries(
      NEGOTIATED.map((name) => [
        name,
        highestImplemented(requests[name].versions, rangeOf(brokerRanges['4.0.2'], apiKeys[name])),
      ])
    )

    expect(expected).toEqual({
      ApiVersions: 4,
      Metadata: 13,
      GroupCoordinator: 6,
      JoinGroup: 8,
      SyncGroup: 5,
      Heartbeat: 4,
      LeaveGroup: 5,
      OffsetFetch: 9,
      OffsetCommit: 9,
      ListOffsets: 9,
      Fetch: 17,
      OffsetForLeaderEpoch: 4,
      InitProducerId: 5,
      Produce: 12,
      AddPartitionsToTxn: 3,
      AddOffsetsToTxn: 4,
      TxnOffsetCommit: 5,
      EndTxn: 5,
      SaslHandshake: 1,
      SaslAuthenticate: 2,
      DescribeTopicPartitions: 0,
      ListPartitionReassignments: 0,
      ConsumerGroupHeartbeat: 1,
      CreateTopics: 7,
      DescribeConfigs: 4,
      DescribeGroups: 6,
    })
  })

  test('a coordinator selects transaction version 2 only at level 2 with a valid epoch', () => {
    const level = (maxVersionLevel) => ({ 'transaction.version': { maxVersionLevel } })

    expect(transactionVersionOf(level(2), 0)).toBe(2)
    expect(transactionVersionOf(level(2), -1)).toBe(1)
    expect(transactionVersionOf(level(1), 3)).toBe(1)
    expect(transactionVersionOf({}, 3)).toBe(1)
  })
})

describeFloor('Kafka floor core (live)', () => {
  const floor = process.env.KAFKA_VERSION
  const kafka = new Kafka({
    clientId: `floor-core-${secureRandom()}`,
    brokers: plainTextBrokers(),
    logLevel: logLevel.NOTHING,
  })
  const clients = []
  const topics = []
  const f08 = []
  let admin
  let offered
  let classicGroup

  /** @param {object} client @returns {Promise<object>} the client, connected and owned by the file */
  const connected = async (client) => {
    clients.push(client)
    await client.connect()
    return client
  }

  /** @param {string} name @param {object} [options] @returns {Promise<string>} a new topic */
  const freshTopic = async (name, { partitions = 1 } = {}) => {
    const topic = `e2e-p9a-floor-core-${name}-${secureRandom()}`
    await createTopic({ topic, partitions, replicas: 3 })
    topics.push(topic)
    return topic
  }

  /** @returns {Promise<Broker>} a raw broker on the first seed, connected */
  const rawBroker = async (address = {}) => {
    const broker = new Broker({
      connectionPool: createConnectionPool(address),
      logger: newLogger(),
    })
    clients.push(broker)
    await broker.connect()
    return broker
  }

  /**
   * An F08 member: a fault-free scenario that also runs on the fake lane, run here on the floor.
   * @param {string} name
   * @param {() => Promise<void>} scenario
   */
  const member = (name, scenario) => {
    f08.push(name)
    test(name, scenario, CASE_BUDGET_MS)
  }

  beforeAll(async () => {
    admin = await connected(kafka.admin())
  })

  afterAll(async () => {
    await admin.deleteTopics({ topics }).catch(() => {})
    for (const client of clients.reverse()) {
      await withTimeout(
        client.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      )
    }
  })

  test(
    'F01 negotiation: each API at the highest implemented version the floor broker offers',
    async () => {
      const broker = await rawBroker()
      const recorded = brokerRanges[floor]
      expect(recorded).toBeDefined()
      offered = Object.fromEntries(
        Object.entries(broker.versions).map(([apiKey, { minVersion, maxVersion }]) => [
          apiKey,
          [minVersion, maxVersion],
        ])
      )
      const rows = NEGOTIATED.map((name) => ({
        name,
        offered: offered[apiKeys[name]] ?? null,
        recorded: rangeOf(recorded, apiKeys[name]) ?? null,
        negotiated: broker.negotiatedVersion(apiKeys[name], requests[name]),
        expected: highestImplemented(requests[name].versions, offered[apiKeys[name]]),
      }))
      const unrecorded = Object.entries(offered)
        .filter(([apiKey, range]) => {
          const entry = rangeOf(recorded, Number(apiKey))
          return !entry || entry[0] !== range[0] || entry[1] !== range[1]
        })
        .map(([apiKey]) => Number(apiKey))
      writeEvidence(EVIDENCE, {
        case: 'F01',
        rows,
        unrecorded,
        wireVersions: Object.fromEntries(rows.map(({ name, negotiated }) => [name, negotiated])),
      })

      expect(unrecorded).toEqual([])
      expect(Object.keys(offered)).toHaveLength(Object.keys(recorded).length)
      for (const row of rows) {
        expect({ name: row.name, offered: row.offered }).toEqual({
          name: row.name,
          offered: row.recorded,
        })
        expect({ name: row.name, negotiated: row.negotiated }).toEqual({
          name: row.name,
          negotiated: row.expected,
        })
      }
    },
    CASE_BUDGET_MS
  )

  test(
    'F03 idempotent read-back: 200 records with acks -1 are in the log in order, each once',
    async () => {
      const topic = await freshTopic('idempotent')
      const producer = await connected(kafka.producer({ idempotent: true }))
      const requests = []
      producer.on(producer.events.REQUEST, ({ payload }) => requests.push(payload))
      const values = Array.from({ length: RECORDS }, (_, n) => `record-${n}`)
      for (let n = 0; n < RECORDS; n += 20) {
        await producer.send({
          topic,
          acks: -1,
          messages: values.slice(n, n + 20).map((value) => ({ value })),
        })
      }
      const [log] = (await readLog({ brokers: plainTextBrokers(), topic })).values()
      const logged = log.records.map(({ value }) => String(value))
      writeEvidence(EVIDENCE, {
        case: 'F03',
        logged: logged.length,
        wireVersions: wireVersionsOf(requests),
      })

      expect(logged).toEqual(values)
    },
    CASE_BUDGET_MS
  )

  test(
    'F04 transactions: an abort is invisible, a commit and its offsets are visible',
    async () => {
      const topic = await freshTopic('transactions')
      const groupId = `e2e-p9a-floor-core-transactions-${secureRandom()}`
      const transactionalId = `e2e-p9a-floor-core-${secureRandom()}`
      const producer = await connected(kafka.producer({ transactionalId, maxInFlightRequests: 1 }))
      const sent = []
      producer.on(producer.events.REQUEST, ({ payload }) => sent.push(payload))

      const aborted = await producer.transaction()
      await aborted.send({ topic, messages: [{ value: 'aborted-1' }, { value: 'aborted-2' }] })
      await aborted.abort()
      const committed = await producer.transaction()
      await committed.send({
        topic,
        messages: [{ value: 'committed-1' }, { value: 'committed-2' }],
      })
      await committed.sendOffsets({
        consumerGroupId: groupId,
        topics: [{ topic, partitions: [{ partition: 0, offset: '4' }] }],
      })
      await committed.commit()

      const seed = await rawBroker()
      const { coordinator } = await seed.findGroupCoordinator({
        groupId: transactionalId,
        coordinatorType: COORDINATOR_TYPES.TRANSACTION,
      })
      const coordinatorBroker = await rawBroker({ host: coordinator.host, port: coordinator.port })
      const level =
        coordinatorBroker.finalizedFeatures['transaction.version']?.maxVersionLevel ?? null
      const version = transactionVersionOf(
        coordinatorBroker.finalizedFeatures,
        coordinatorBroker.finalizedFeaturesEpoch
      )
      // EndTxn answers before the coordinator writes the markers: the commit is readable once the
      // last stable offset reaches the high watermark, and its offsets once the group holds them.
      await waitForCondition(async () => {
        const [high, stable, [{ partitions: held }]] = await Promise.all([
          endOffsets({ brokers: plainTextBrokers(), topic }),
          endOffsets({
            brokers: plainTextBrokers(),
            topic,
            isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
          }),
          admin.fetchOffsets({ groupId, topics: [topic] }),
        ])
        return stable.get(0) === high.get(0) && held[0]?.offset === '4'
      }, WAIT_BUDGET)
      const [log] = (
        await readLog({
          brokers: plainTextBrokers(),
          topic,
          isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
        })
      ).values()
      const visible = committedRecords(log).map(({ value }) => String(value))
      const [{ partitions }] = await admin.fetchOffsets({ groupId, topics: [topic] })
      const wire = wireVersionsOf(sent)
      writeEvidence(EVIDENCE, {
        case: 'F04',
        level,
        transactionVersion: version,
        visible,
        offsets: partitions,
        wireVersions: wire,
      })

      expect(visible).toEqual(['committed-1', 'committed-2'])
      expect(partitions).toEqual([expect.objectContaining({ partition: 0, offset: '4' })])
      expect(wire).toMatchObject(TRANSACTION_CAPS[version])
      if (version === 2) {
        expect(wire).not.toHaveProperty('AddPartitionsToTxn')
        expect(wire).not.toHaveProperty('AddOffsetsToTxn')
      } else {
        expect(wire).toMatchObject({ AddPartitionsToTxn: 3, AddOffsetsToTxn: 4 })
      }
    },
    CASE_BUDGET_MS
  )

  /**
   * Two members of one group over a two-partition topic: the first reads everything and commits
   * `COMMITTED` on both partitions, the second joins, and every partition a member takes on is
   * read again from the committed offset, never before it.
   * @param {string} name
   * @param {object} options the members' consumer options beyond the group id
   * @returns {Promise<{ groupId: string, resumed: object[] }>}
   */
  const twoMembers = async (name, options) => {
    const topic = await freshTopic(name, { partitions: 2 })
    const groupId = `e2e-p9a-floor-core-${name}-${secureRandom()}`
    const producer = await connected(kafka.producer())
    await producer.send({
      topic,
      messages: [0, 1].flatMap((partition) =>
        Array.from({ length: PER_PARTITION }, (_, n) => ({ partition, value: `${partition}-${n}` }))
      ),
    })
    const delivered = []
    const assigned = []
    const start = async (memberName) => {
      const consumer = await connected(kafka.consumer({ groupId, ...options }))
      consumer.on(consumer.events.PARTITIONS_ASSIGNED, ({ payload }) =>
        assigned.push({ member: memberName, at: delivered.length, partitions: payload.partitions })
      )
      await consumer.subscribe({ topic, fromBeginning: true })
      await consumer.run({
        autoCommit: false,
        eachMessage: async ({ partition, message }) =>
          delivered.push({ member: memberName, partition, offset: message.offset }),
      })
      return consumer
    }

    const first = await start('first')
    expect(await waitForCondition(() => delivered.length >= 2 * PER_PARTITION, WAIT_BUDGET)).toBe(
      true
    )
    await first.commitOffsets([0, 1].map((partition) => ({ topic, partition, offset: COMMITTED })))
    const joinedAt = delivered.length
    const second = await start('second')
    const balanced = await waitForCondition(
      () => assigned.some(({ member }) => member === 'second'),
      WAIT_BUDGET
    )
    expect(balanced).toBe(true)
    const taken = assigned
      .filter(({ at }) => at >= joinedAt)
      .flatMap(({ member, partitions }) =>
        partitions.flatMap(({ partitions: ids }) => ids.map((partition) => ({ member, partition })))
      )
    const resumedPartitions = [
      ...new Map(taken.map((entry) => [`${entry.member}|${entry.partition}`, entry])).values(),
    ]
    const reread = (PER_PARTITION - Number(COMMITTED)) * resumedPartitions.length
    await waitForCondition(() => delivered.length - joinedAt >= reread, WAIT_BUDGET)
    await sleep(QUIET_MS)
    const after = delivered.slice(joinedAt)
    const resumed = resumedPartitions.map(({ member, partition }) => ({
      member,
      partition,
      offsets: after
        .filter((d) => d.member === member && d.partition === partition)
        .map(({ offset }) => offset),
    }))
    for (const consumer of [first, second]) {
      const mine = after.filter(
        ({ member }) => member === (consumer === first ? 'first' : 'second')
      )
      const partitions = [...new Set(mine.map(({ partition }) => partition))]
      if (partitions.length) {
        await consumer.commitOffsets(
          partitions.map((partition) => ({ topic, partition, offset: String(PER_PARTITION) }))
        )
      }
    }
    const [{ partitions: committed }] = await admin.fetchOffsets({ groupId, topics: [topic] })
    writeEvidence(EVIDENCE, { case: name, resumed, committed })

    expect(resumed.length).toBeGreaterThan(0)
    for (const { offsets } of resumed) {
      expect(offsets).toEqual(['6', '7', '8', '9'])
    }
    expect(after.every(({ offset }) => Number(offset) >= Number(COMMITTED))).toBe(true)
    return { groupId, resumed, committed }
  }

  test(
    'F05 classic eager: two members rebalance, commit and resume at the committed offset',
    async () => {
      const { groupId } = await twoMembers('F05', {
        sessionTimeout: 30000,
        heartbeatInterval: 500,
        partitionAssigners: [PartitionAssigners.range],
      })
      classicGroup = groupId
    },
    CASE_BUDGET_MS
  )

  test(
    'F02 admin: topics, metadata, cluster and the groups the floor broker describes',
    async () => {
      const topic = `e2e-p9a-floor-core-admin-${secureRandom()}`
      topics.push(topic)
      const created = await admin.createTopics({
        topics: [{ topic, numPartitions: 3, replicationFactor: 3 }],
        waitForLeaders: true,
      })
      const metadata = await admin.fetchTopicMetadata({ topics: [topic] })
      const listed = await admin.listTopics()
      const cluster = await admin.describeCluster()
      const { groups } = await admin.describeGroups([classicGroup])
      writeEvidence(EVIDENCE, {
        case: 'F02',
        created,
        partitions: metadata.topics[0].partitions.length,
        brokers: cluster.brokers.length,
        groupState: groups[0]?.state,
        members: groups[0]?.members.length,
      })

      expect(created).toBe(true)
      expect(metadata.topics[0].partitions).toHaveLength(3)
      expect(listed).toContain(topic)
      expect(cluster.brokers).toHaveLength(3)
      expect(groups).toEqual([
        expect.objectContaining({
          groupId: classicGroup,
          state: 'Stable',
          protocolType: 'consumer',
        }),
      ])
      expect(groups[0].members).toHaveLength(2)
    },
    CASE_BUDGET_MS
  )

  test(
    'F06 classic cooperative: two members rebalance, commit and resume at the committed offset',
    async () => {
      await twoMembers('F06', {
        sessionTimeout: 30000,
        heartbeatInterval: 500,
        partitionAssigners: [PartitionAssigners.cooperativeSticky],
      })
    },
    CASE_BUDGET_MS
  )

  test(
    'F07 consumer protocol: join, assignment, commit, and a second member reconciled in',
    async () => {
      const capable = offered?.[apiKeys.ConsumerGroupHeartbeat] !== undefined
      writeEvidence(EVIDENCE, { case: 'F07-capability', consumerGroupHeartbeat: capable })
      if (!capable) return
      await twoMembers('F07', { groupProtocol: 'consumer' })
    },
    CASE_BUDGET_MS
  )

  describe('F08 fault-free scenarios on the floor', () => {
    member('a deleted subscribed topic stays absent when auto creation is disabled', async () => {
      const topic = await freshTopic('auto-create')
      const consumer = await connected(
        kafka.consumer({
          groupId: `e2e-p9a-floor-core-auto-create-${secureRandom()}`,
          allowAutoTopicCreation: false,
          metadataMaxAge: 0,
          // No retry and no restart: the unknown topic must surface as one crash, at once.
          retry: { retries: 0, restartOnFailure: async () => false },
        })
      )
      await consumer.subscribe({ topic })
      // Asked of every broker: each drops a deleted topic from its Metadata answers on its own
      // schedule, so one broker's absence still leaves the others listing the old topic.
      const seeds = await Promise.all(
        plainTextBrokers().map((address) => {
          const [host, port] = address.split(':')
          return rawBroker({ host, port: Number(port) })
        })
      )
      const listedAnywhere = async () =>
        (await Promise.all(seeds.map((broker) => broker.metadata([])))).some(({ topicMetadata }) =>
          topicMetadata.some(({ topic: name }) => name === topic)
        )
      await admin.deleteTopics({ topics: [topic] })
      // Deleted here, so the cleanup must not retry UNKNOWN_TOPIC_OR_PARTITION on it.
      topics.splice(topics.indexOf(topic), 1)
      expect(await waitForCondition(async () => !(await listedAnywhere()), WAIT_BUDGET)).toBe(true)
      const crashed = new Promise((resolve) => consumer.on(consumer.events.CRASH, resolve))
      await consumer.run({ eachMessage: async () => {} })
      const { payload } = await crashed
      let cause = payload.error
      while (cause.cause) cause = cause.cause
      const absent = []
      for (let attempt = 0; attempt < 20; attempt++) {
        absent.push(!(await listedAnywhere()))
        await sleep(50)
      }
      writeEvidence(EVIDENCE, {
        case: 'F08',
        member: 'auto-topic-creation',
        restart: payload.restart,
        cause: cause.type,
        recreated: absent.includes(false),
      })

      expect(payload.restart).toBe(false)
      expect(cause).toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })
      expect(absent.every(Boolean)).toBe(true)
    })

    member(
      `${CALLS} concurrent Metadata calls keep at most ${LIMIT} in flight and all resolve`,
      async () => {
        const topic = await freshTopic('max-in-flight')
        const { host, port, socketFactory } = connectionOpts()
        const outcome = await overload({ host, port, socketFactory, topics: [topic] })
        writeEvidence(EVIDENCE, {
          case: 'F08',
          member: 'max-in-flight',
          calls: CALLS,
          maxInFlightRequests: LIMIT,
          ...outcome,
        })

        expect(outcome.resolved).toBe(CALLS)
        expect(outcome.maxInFlight).toBeLessThanOrEqual(LIMIT)
        expect(outcome.maxInFlight).toBeGreaterThan(1)
      }
    )

    member(
      'topics named like Object.prototype members round-trip through a producer and a consumer',
      async () => {
        const before = prototypeDescriptors()
        const outcomes = []
        for (const name of NAMES) {
          const run = new PrototypeCase(name, 'docker')
          try {
            const consumer = await run.consume(await run.kafka())
            await consumer.commitOffsets([{ topic: name, partition: 0, offset: '1' }])
            const [offsets] = await run.admin.fetchOffsets({
              groupId: PROTOTYPE_GROUP,
              topics: [name],
            })
            outcomes.push({
              name,
              delivered: run.delivered.length,
              committed: offsets.partitions[0].offset,
            })
          } finally {
            await run.close()
          }
        }
        writeEvidence(EVIDENCE, { case: 'F08', member: 'prototype-topics', outcomes })

        expect(outcomes).toEqual(NAMES.map((name) => ({ name, delivered: 1, committed: '1' })))
        expect(prototypeDescriptors()).toEqual(before)
      }
    )
  })

  test('every evidence line of this run carries the stamps a verifier checks', () => {
    const lines = evidenceLines(EVIDENCE).slice(linesBefore)
    const verified = {
      tree: process.env.KAFKAJS_TREE || 'unfrozen',
      node: process.version,
      brokerVersion: floor,
    }

    expect(lines.map(({ case: name }) => name)).toEqual(
      expect.arrayContaining(['F01', 'F02', 'F03', 'F04', 'F05', 'F06', 'F07-capability'])
    )
    expect(lines.filter(({ case: name }) => name === 'F08')).toHaveLength(f08.length)
    for (const line of lines) expect(problemsOf(line, verified)).toEqual([])
  })
})
