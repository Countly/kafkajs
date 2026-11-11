const Broker = require('../index')
const { requests } = require('../../protocol/requests')
const apiKeys = require('../../protocol/requests/apiKeys')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const ISOLATION_LEVEL = require('../../protocol/isolationLevel')
const { Kafka, CompressionTypes, logLevel, PartitionAssigners } = require('../../../index')
const { lookupCodec } = require('../../protocol/message/compression')
const { KafkaJSNotImplemented, KafkaJSServerDoesNotSupportApiKey } = require('../../errors')
const {
  decodeField,
  recordsField,
} = require('../../protocol/message/compression/fixtures/compressedBatch')
const brokerRanges = require('../../../scripts/protocol/brokerRanges.json')
const { rangeOf } = require('../../../scripts/e2e/apiVersionsProbe')
const { readLog, endOffsets, committedRecords } = require('../../../testHelpers/rawLog')
const {
  connectionOpts,
  createConnectionPool,
  createTopic,
  waitForTopicPropagation,
  newLogger,
  plainTextBrokers,
  secureRandom,
  writeEvidence,
  evidenceLines,
  wireVersionsOf,
} = require('testHelpers')
const { problemsOf } = require('../../../scripts/evidence/verify')
const { lanes } = require('../../../testHelpers/lane')
const FloorCapabilities = require('../../../testHelpers/floorCapabilities')
const {
  HeldDial,
  SuiteBudget,
  SUITE_MS,
  TimerLedger,
  caseTimeout,
  deepest,
  differentLevel,
  markersOf,
  refusalOf,
  rejectionOf,
  runFloorCase,
} = require('./fixtures/floorAdmin')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')
const { WireTap, scanBatches } = require('../../producer/__tests__/fixtures/liveWire')
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
const net = require('net')
const fs = require('fs')
const path = require('path')
const { TelemetryWireObserver } = require('../../../testHelpers/telemetryFixtures')
const { KafkaJSShareGroupsNotSupportedError } = require('../../../index')
const { createResourceProbe } = require('../../../testHelpers/resourceProbe')
const { FrameLog } = require('../../producer/__tests__/fixtures/liveWire')
const { CleanupStack, describeError } = require('../../producer/__tests__/fixtures/liveProbe')
const live = require('../../producer/__tests__/fixtures/bounded')

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
// Fault-free cases also run on the fake lane, which a bare `jest` run is; the floor run is `docker`.
const describeFakeLane = lanes().includes('fake') ? describe : describe.skip

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
const DISCOVERY_WAIT = { attempts: 80, delay: 250 } // 20 s
const CASE_BUDGET_MS = 90000
const DISCONNECT_BUDGET_MS = 15000
const CODEC_CASE_BUDGET_MS = 180000 // five arms of setup, send, read and cleanup
const CODEC_DELIVERY_MS = 30000
const CODEC_READ = { maxWaitTime: 100, maxIdleFetches: 100 } // at most 10 s without progress
const CODEC_FAKE_BUDGET_MS = 30000

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

/**
 * The codecs of the round trip, by compression type. `None` is the control, run but not counted;
 * the other four are the cells the case must record.
 */
const CODECS = [
  { name: 'None', id: CompressionTypes.None, control: true },
  { name: 'GZIP', id: CompressionTypes.GZIP },
  { name: 'Snappy', id: CompressionTypes.Snappy },
  { name: 'LZ4', id: CompressionTypes.LZ4 },
  { name: 'ZSTD', id: CompressionTypes.ZSTD },
]

/**
 * What every arm sends to partition 0. Replay sets: durable log, each arm's two records exactly
 * once; committed offsets, none; handler delivery, none.
 */
const CODEC_RECORDS = [
  { key: 'one', value: 'a' },
  { key: 'two', value: 'b' },
]

/** @param {Buffer|null} bytes @returns {string|null} the bytes in hex, so an equal pair is byte for byte */
const hex = (bytes) => bytes?.toString('hex') ?? null

const EXPECTED_PAIRS = CODEC_RECORDS.map(({ key, value }) => [
  hex(Buffer.from(key)),
  hex(Buffer.from(value)),
])

/**
 * @param {number} id a compression type
 * @returns {boolean} whether the client can encode and decode it here
 * @throws {Error} whatever a codec lookup throws other than "this codec is not available"
 */
const isInstalled = (id) => {
  try {
    lookupCodec(id)
    return true
  } catch (error) {
    if (error instanceof KafkaJSNotImplemented) return false
    throw error
  }
}

/**
 * The compressed codecs this run cannot exercise. Each is an unmet cell of the four-codec case,
 * which fails on it instead of passing with fewer codecs.
 * @param {(id: number) => boolean} [installed] whether a codec is usable; replaced by a test
 * @returns {string[]} their names, in table order
 */
const unmetCodecs = (installed = isInstalled) =>
  CODECS.filter(({ control, id }) => !control && !installed(id)).map(({ name }) => name)

/**
 * One codec's result, the same in both lanes.
 * @param {{ name: string, id: number, control?: boolean }} codec
 * @param {Array<{ codec: number, producerId: string }>} batches the batches that hold the records,
 *   as the wire carried them
 * @param {Array<{ key: Buffer|null, value: Buffer|null }>} records what a fault-free read of
 *   partition 0 returned
 * @returns {{ codec: string, id: number, control: boolean, stored: number[], idempotent: boolean,
 *   pairs: Array<[string|null, string|null]> }}
 */
const armOf = (codec, batches, records) => ({
  codec: codec.name,
  id: codec.id,
  control: Boolean(codec.control),
  stored: batches.map(({ codec: id }) => id),
  idempotent: batches.length > 0 && batches.every(({ producerId }) => producerId !== '-1'),
  pairs: records.map(({ key, value }) => [hex(key), hex(value)]),
})

/**
 * Runs `arm` once for each installed codec, one after another, in table order.
 * @param {(codec: object) => Promise<object>} arm
 * @returns {Promise<{ arms: object[], unmet: string[] }>} each arm's result, and the unmet cells
 */
const roundTrip = async (arm) => {
  const unmet = unmetCodecs()
  const arms = []
  for (const codec of CODECS.filter(({ name }) => !unmet.includes(name))) {
    arms.push(await arm(codec))
  }
  return { arms, unmet }
}

/**
 * The case's verdict: no unmet cell, and in every arm the records are in one batch of the arm's
 * own codec, written by an idempotent producer, and read back once in order, byte for byte.
 * @param {object[]} arms
 * @param {string[]} unmet
 * @returns {void}
 * @throws {Error} the failed expectation
 */
const expectRoundTrip = (arms, unmet) => {
  expect(unmet).toEqual([])
  for (const arm of arms) {
    expect(arm).toMatchObject({ stored: [arm.id], idempotent: true, pairs: EXPECTED_PAIRS })
  }
}

/**
 * The fake lane's arm: a real idempotent producer sends to a scripted cluster, the batches it wrote
 * are found on the wire, and the fetch path's own decoder reads them back.
 * @param {{ name: string, id: number, control?: boolean }} codec
 * @returns {Promise<object>} the arm's result
 */
const fakeArm = async (codec) => {
  const cluster = producerScript(
    createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }] }),
    { topic: TOPIC }
  )
  const producer = new Kafka(cluster.kafkaOptions()).producer({ idempotent: true, linger: 0 })
  try {
    await producer.connect()
    await producer.send({
      topic: TOPIC,
      acks: -1,
      compression: codec.id,
      messages: CODEC_RECORDS.map((record) => ({ partition: 0, ...record })),
    })
  } finally {
    await producer.disconnect()
  }
  cluster.assertClean()
  const batches = scanBatches(Buffer.concat(cluster.sockets().flatMap(({ writes }) => writes)))
  const { messages } = await decodeField(recordsField(...batches.map(({ bytes }) => bytes)))
  return armOf(codec, batches, messages)
}

/**
 * A delivery reduced to what the corpus compares: where it came from and its value.
 * @param {{ topic: string, partition: number, message: { value: Buffer|null } }} delivery an `eachMessage` payload
 * @returns {{ topic: string, partition: number, value: string }}
 */
const identityOf = ({ topic, partition, message }) => ({
  topic,
  partition,
  value: String(message.value),
})

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
      ListOffsets: 10,
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

  test('a compressed codec that is not installed is an unmet cell, never a pass', () => {
    const gzip = {
      codec: 'GZIP',
      id: CompressionTypes.GZIP,
      stored: [CompressionTypes.GZIP],
      idempotent: true,
      pairs: EXPECTED_PAIRS,
    }

    expect(unmetCodecs(() => true)).toEqual([])
    expect(unmetCodecs((id) => id !== CompressionTypes.LZ4)).toEqual(['LZ4'])
    expect(unmetCodecs((id) => id === CompressionTypes.None)).toEqual([
      'GZIP',
      'Snappy',
      'LZ4',
      'ZSTD',
    ])
    expect(() => expectRoundTrip([gzip], [])).not.toThrow()
    expect(() => expectRoundTrip([gzip], ['LZ4'])).toThrow()
  })

  test('a codec arm fails on another batch codec, a lost idempotence or a changed pair', () => {
    const arm = {
      codec: 'ZSTD',
      id: CompressionTypes.ZSTD,
      stored: [CompressionTypes.ZSTD],
      idempotent: true,
      pairs: EXPECTED_PAIRS,
    }

    expect(() => expectRoundTrip([{ ...arm, stored: [CompressionTypes.None] }], [])).toThrow()
    expect(() => expectRoundTrip([{ ...arm, idempotent: false }], [])).toThrow()
    expect(() => expectRoundTrip([{ ...arm, pairs: [...EXPECTED_PAIRS].reverse() }], [])).toThrow()
    expect(() =>
      expectRoundTrip([{ ...arm, pairs: [...EXPECTED_PAIRS, ...EXPECTED_PAIRS] }], [])
    ).toThrow()
  })
})

describeFakeLane('Floor core codec round trip (fake lane)', () => {
  test(
    'round trips every installed built-in codec without faults',
    async () => {
      const { arms, unmet } = await roundTrip(fakeArm)

      expectRoundTrip(arms, unmet)
    },
    CODEC_FAKE_BUDGET_MS
  )
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
      // The leader wait asks one broker; the metadata read below may reach any of them.
      await waitForTopicPropagation(topic, 3)
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

  test(
    'round trips every installed built-in codec without faults',
    async () => {
      const sent = []
      const { arms, unmet } = await roundTrip(async (codec) => {
        const topic = await freshTopic(`codec-${codec.name.toLowerCase()}`, { partitions: 2 })
        const producer = await connected(
          kafka.producer({ idempotent: true, linger: 0, deliveryTimeout: CODEC_DELIVERY_MS })
        )
        producer.on(producer.events.REQUEST, ({ payload }) => sent.push(payload))
        await producer.send({
          topic,
          acks: -1,
          compression: codec.id,
          messages: CODEC_RECORDS.map((record) => ({ partition: 0, ...record })),
        })
        await withTimeout(
          producer.disconnect().catch(() => {}),
          DISCONNECT_BUDGET_MS
        )
        const tap = new WireTap()
        const log = await readLog({
          brokers: plainTextBrokers(),
          topic,
          open: tap.open,
          ...CODEC_READ,
        })
        return armOf(codec, tap.batches(), log.get(0).records)
      })
      writeEvidence(EVIDENCE, {
        case: 'F09',
        required: CODECS.filter(({ control }) => !control).map(({ name }) => name),
        unmet,
        arms,
        wireVersions: wireVersionsOf(sent),
      })

      expectRoundTrip(arms, unmet)
    },
    CODEC_CASE_BUDGET_MS
  )

  test.each([true, false])(
    'manual assignment round trips with group offsets enabled %s',
    async (withGroup) => {
      const topic = await freshTopic('manual-consumer')
      const groupId = `floor-manual-${secureRandom()}`
      const corpus = ['one', 'two'].map((value) => ({ topic, partition: 0, value }))
      const producer = await connected(kafka.producer())
      await producer.send({
        topic,
        messages: corpus.map(({ partition, value }) => ({ partition, value })),
      })
      const consumer = await connected(kafka.consumer(withGroup ? { groupId } : {}))
      const apiNames = []
      const remove = consumer.on(consumer.events.REQUEST, ({ payload }) =>
        apiNames.push(payload.apiName)
      )
      const delivered = []
      const evidence = { case: 'consumer-manual', withGroup, delivered, apiNames, committed: null }
      try {
        await consumer.assign([{ topic, partitions: [0] }], { offsetReset: 'earliest' })
        await consumer.run({
          autoCommit: false,
          eachMessage: async (payload) => delivered.push(identityOf(payload)),
        })
        expect(await waitForCondition(() => delivered.length === corpus.length, WAIT_BUDGET)).toBe(
          true
        )
        expect(consumer.assignment()).toEqual([{ topic, partitions: [0] }])
        if (withGroup) {
          await consumer.commitOffsets([{ topic, partition: 0, offset: '2' }])
          const [offsets] = await admin.fetchOffsets({ groupId, topics: [topic] })
          evidence.committed = offsets.partitions[0].offset
        }
        await consumer.stop()
        expect(await waitForCondition(() => apiNames.includes('Fetch'), WAIT_BUDGET)).toBe(true)
        const named = (names) => apiNames.filter((name) => names.includes(name))

        expect(delivered).toEqual(corpus)
        expect(evidence.committed).toBe(withGroup ? '2' : null)
        expect(
          named(['JoinGroup', 'SyncGroup', 'Heartbeat', 'LeaveGroup', 'ConsumerGroupHeartbeat'])
        ).toEqual([])
        if (!withGroup) expect(named(['OffsetFetch', 'OffsetCommit'])).toEqual([])
      } finally {
        writeEvidence(EVIDENCE, evidence)
        remove()
        await consumer.stop()
      }
    },
    CASE_BUDGET_MS
  )

  test(
    'an awaited assignment listener holds delivery until it completes',
    async () => {
      const topic = await freshTopic('listener-consumer')
      const corpus = [{ topic, partition: 0, value: 'one' }]
      const producer = await connected(kafka.producer())
      await producer.send({ topic, messages: [{ partition: 0, value: 'one' }] })
      const consumer = await connected(
        kafka.consumer({ groupId: `floor-listener-${secureRandom()}` })
      )
      let release
      const held = new Promise((resolve) => {
        release = resolve
      })
      const delivered = []
      const evidence = {
        case: 'consumer-listener',
        entered: 0,
        completed: 0,
        earlyDeliveries: 0,
        heldCompleted: null,
        heldDelivered: null,
        delivered,
      }
      let settled
      try {
        await consumer.subscribe({ topic, fromBeginning: true })
        settled = consumer
          .run({
            autoCommit: false,
            rebalanceListener: {
              onPartitionsAssigned: async () => {
                evidence.entered += 1
                await held
                evidence.completed += 1
              },
            },
            eachMessage: async (payload) => {
              if (evidence.completed === 0) evidence.earlyDeliveries += 1
              delivered.push(identityOf(payload))
            },
          })
          .then(
            () => null,
            (error) => error
          )
        expect(await waitForCondition(() => evidence.entered > 0, WAIT_BUDGET)).toBe(true)
        await sleep(QUIET_MS)
        evidence.heldCompleted = evidence.completed
        evidence.heldDelivered = [...delivered]
        release()
        expect(await settled).toBeNull()
        expect(await waitForCondition(() => delivered.length === corpus.length, WAIT_BUDGET)).toBe(
          true
        )
        await consumer.stop()

        expect(evidence.heldCompleted).toBe(0)
        expect(evidence.heldDelivered).toEqual([])
        expect(evidence.earlyDeliveries).toBe(0)
        expect(evidence.entered).toBe(1)
        expect(delivered).toEqual(corpus)
      } finally {
        writeEvidence(EVIDENCE, evidence)
        release()
        if (settled) await settled
        await consumer.stop()
      }
    },
    CASE_BUDGET_MS
  )

  test(
    'a pattern consumer discovers a matching topic created after delivery starts',
    async () => {
      const first = await freshTopic('regex-consumer')
      const second = `${first}-next`
      const corpus = [
        { topic: first, partition: 0, value: 'first' },
        { topic: second, partition: 0, value: 'second' },
      ]
      const producer = await connected(kafka.producer())
      const consumer = await connected(
        kafka.consumer({
          groupId: `floor-regex-${secureRandom()}`,
          metadataMaxAge: 1000,
          allowAutoTopicCreation: false,
        })
      )
      const delivered = []
      const evidence = { case: 'consumer-regex', delivered, discovered: false }
      try {
        await producer.send({ topic: first, messages: [{ partition: 0, value: 'first' }] })
        await consumer.subscribe({ topic: new RegExp(`^${first}(?:-next)?$`), fromBeginning: true })
        await consumer.run({
          autoCommit: false,
          eachMessage: async (payload) => delivered.push(identityOf(payload)),
        })
        expect(await waitForCondition(() => delivered.length === 1, WAIT_BUDGET)).toBe(true)
        // Finding the new topic rebalances the group, and a member that has committed nothing
        // restarts from the beginning, so the first record must be committed to stay delivered once.
        await consumer.commitOffsets([{ topic: first, partition: 0, offset: '1' }])
        await createTopic({ topic: second, partitions: 1, replicas: 3 })
        topics.push(second)
        await producer.send({ topic: second, messages: [{ partition: 0, value: 'second' }] })
        evidence.discovered = await waitForCondition(() => delivered.length === 2, DISCOVERY_WAIT)
        await consumer.stop()

        expect(evidence.discovered).toBe(true)
        expect(delivered).toEqual(corpus)
      } finally {
        writeEvidence(EVIDENCE, evidence)
        await consumer.stop()
      }
    },
    CASE_BUDGET_MS
  )

  test('every evidence line of this run carries the stamps a verifier checks', () => {
    const lines = evidenceLines(EVIDENCE).slice(linesBefore)
    const verified = {
      tree: process.env.KAFKAJS_TREE || 'unfrozen',
      node: process.version,
      brokerVersion: floor,
    }

    expect(lines.map(({ case: name }) => name)).toEqual(
      expect.arrayContaining([
        'F01',
        'F02',
        'F03',
        'F04',
        'F05',
        'F06',
        'F07-capability',
        'consumer-listener',
        'consumer-regex',
      ])
    )
    const ofCase = (name) => lines.filter(({ case: line }) => line === name)
    expect(
      ofCase('consumer-manual')
        .map(({ withGroup, delivered, committed }) => [withGroup, delivered.length, committed])
        .sort()
    ).toEqual([
      [false, 2, null],
      [true, 2, '2'],
    ])
    expect(ofCase('consumer-listener')).toEqual([
      expect.objectContaining({ heldCompleted: 0, heldDelivered: [], earlyDeliveries: 0 }),
    ])
    expect(ofCase('consumer-regex')).toEqual([expect.objectContaining({ discovered: true })])
    expect(lines.filter(({ case: name }) => name === 'F08')).toHaveLength(f08.length)
    for (const line of lines) expect(problemsOf(line, verified)).toEqual([])
  })
})

describe('Floor capability expectations', () => {
  const inputs = () => ({
    recorded: { ListTransactions: [0, 1] },
    observed: { ListTransactions: [0, 1] },
    implemented: { ListTransactions: [0, 1, 2] },
  })

  test('maps actual codec definitions and the retained probe without relying on a release label', () => {
    const names = ['ListOffsets', 'ListTransactions', 'ListConfigResources']
    const versions = Object.fromEntries(
      names.map((name) => {
        const [minVersion, maxVersion] = rangeOf(brokerRanges['4.0.2'], apiKeys[name])
        return [apiKeys[name], { minVersion, maxVersion }]
      })
    )
    const ranges = FloorCapabilities.fromBroker({ versions }, names, '4.0.2')
    expect(ranges.ListOffsets.selected).toBe(10)
    expect(ranges.ListTransactions.selected).toBe(1)
    expect(ranges.ListConfigResources.selected).toBe(0)
    delete versions[apiKeys.ListOffsets]
    expect(() => FloorCapabilities.fromBroker({ versions }, names, '4.0.2')).toThrow(
      'missing range'
    )
  })

  test('selects the highest overlap of independently observed and implemented ranges', () => {
    expect(new FloorCapabilities(inputs()).version('ListTransactions')).toBe(1)
  })

  test.each([undefined, [2, 1], [-1, 1], [0, 0]])(
    'refuses a missing, invalid or stale observed range %p',
    (range) => {
      const tables = inputs()
      tables.observed.ListTransactions = range
      expect(() => new FloorCapabilities(tables).version('ListTransactions')).toThrow()
    }
  )

  test('requires the option, needed version and both complete ranges in a typed refusal', () => {
    const oracle = new FloorCapabilities(inputs())
    const text =
      'ListTransactions transactionalIdPattern needs v2; client implements v0-v2; broker offers v0-v1'
    const error = new KafkaJSServerDoesNotSupportApiKey(text)
    expect(() =>
      oracle.unsupported(error, 'ListTransactions', 2, 'transactionalIdPattern')
    ).not.toThrow()
    for (const removed of ['ListTransactions', 'transactionalIdPattern', 'v2;', 'v0-v2', 'v0-v1']) {
      expect(() =>
        oracle.unsupported(
          new KafkaJSServerDoesNotSupportApiKey(text.replace(removed, '')),
          'ListTransactions',
          2,
          'transactionalIdPattern'
        )
      ).toThrow()
    }
    expect(() =>
      oracle.unsupported(new Error(text), 'ListTransactions', 2, 'transactionalIdPattern')
    ).toThrow('wrong refusal class')
  })

  test('correlates each SASL request with its negotiated version and refuses a missing or other one', () => {
    const ranges = { SaslHandshake: { selected: 1 }, SaslAuthenticate: { selected: 2 } }
    const sent = (apiName, ...versions) => versions.map((apiVersion) => ({ apiName, apiVersion }))
    const exchange = [
      { apiName: 'ApiVersions', apiVersion: 4 },
      ...sent('SaslHandshake', 1),
      ...sent('SaslAuthenticate', 2, 2),
    ]

    expect(() => FloorCapabilities.correlate(ranges, exchange)).not.toThrow()
    expect(() => FloorCapabilities.correlate(ranges, sent('SaslHandshake', 1))).toThrow(
      'SaslAuthenticate: no request observed'
    )
    expect(() =>
      FloorCapabilities.correlate(ranges, [
        ...sent('SaslHandshake', 0),
        ...sent('SaslAuthenticate', 2),
      ])
    ).toThrow('SaslHandshake: unnegotiated version sent')
    expect(() =>
      FloorCapabilities.correlate(ranges, [...exchange, ...sent('SaslAuthenticate', 1)])
    ).toThrow('SaslAuthenticate: unnegotiated version sent')
  })

  test('uses negotiated ranges even when an unrelated release label suggests a newer broker', () => {
    const tables = { ...inputs(), release: 'future' }
    expect(new FloorCapabilities(tables).version('ListTransactions')).toBe(1)
  })

  test('refuses a supported option represented as unsupported', () => {
    const tables = inputs()
    tables.observed.ListTransactions = [0, 2]
    tables.recorded.ListTransactions = [0, 2]
    expect(() =>
      new FloorCapabilities(tables).unsupported(
        new Error(),
        'ListTransactions',
        2,
        'transactionalIdPattern'
      )
    ).toThrow('option is supported')
  })
})

describe('Floor administration helpers', () => {
  test('a shared budget starts once, only shrinks and refuses a start with no cleanup reserve left', () => {
    const budget = new SuiteBudget({ totalMs: 60000, reserveMs: 100 })
    const first = budget.take()
    const second = budget.take()

    expect(first).toBeLessThanOrEqual(60000)
    expect(second).toBeLessThanOrEqual(first)
    expect(() => new SuiteBudget({ totalMs: 50, reserveMs: 100 }).take()).toThrow(
      'The shared administration budget is exhausted'
    )
  })

  test('a typed refusal is found through its cause chain, and a cycle or a non-error ends the walk', () => {
    const refusal = new KafkaJSServerDoesNotSupportApiKey('refused')
    const wrapped = new Error('outer', { cause: new Error('middle', { cause: refusal }) })
    const loop = new Error('a')
    loop.cause = new Error('b', { cause: loop })

    expect(refusalOf(wrapped)).toBe(refusal)
    expect(refusalOf(loop)).toBe(loop)
    expect(refusalOf(null)).toBeNull()
    expect(deepest(wrapped)).toBe(refusal)
    expect(deepest(loop)).toBe(loop.cause)
  })

  test('a rejection becomes the failure itself and a resolution becomes null', async () => {
    const failure = new Error('refused')

    expect(await rejectionOf(Promise.reject(failure))).toBe(failure)
    expect(await rejectionOf(Promise.resolve('fine'))).toBeNull()
  })

  test('a feature update that differs from the finalized level downgrades first, upgrades next and skips fixed or unsupported features', () => {
    const range = (minVersion, maxVersion) => ({ minVersion, maxVersion })
    const supportedFeatures = {
      'group.version': range(0, 1),
      'kraft.version': range(0, 1),
      'metadata.version': range(1, 26),
      'share.version': range(0, 1),
      'transaction.version': range(0, 2),
    }
    const finalized = (levels) => ({
      supportedFeatures,
      finalizedFeatures: Object.fromEntries(
        Object.entries(levels).map(([name, level]) => [
          name,
          { minVersionLevel: level, maxVersionLevel: level },
        ])
      ),
    })

    expect(
      differentLevel(
        finalized({
          'metadata.version': 20,
          'kraft.version': 1,
          'transaction.version': 2,
          'group.version': 1,
        })
      )
    ).toEqual({ name: 'group.version', level: 1, target: 0, upgradeType: 'safe-downgrade' })
    expect(differentLevel(finalized({ 'metadata.version': 20, 'share.version': 0 }))).toEqual({
      name: 'share.version',
      level: 0,
      target: 1,
      upgradeType: 'upgrade',
    })
    expect(
      differentLevel(finalized({ 'metadata.version': 20, 'kraft.version': 1 }))
    ).toBeUndefined()
    expect(differentLevel(finalized({ 'unsupported.version': 1 }))).toBeUndefined()
    expect(
      differentLevel({
        supportedFeatures: { 'transaction.version': range(2, 2) },
        finalizedFeatures: { 'transaction.version': { minVersionLevel: 2, maxVersionLevel: 2 } },
      })
    ).toBeUndefined()
  })

  test('a timer ledger reports the timers started since it began that are still pending, by identity', async () => {
    const earlier = setTimeout(() => {}, 60000)
    const ledger = new TimerLedger()
    ledger.start()
    const cleared = setTimeout(() => {}, 60000)
    const retained = setTimeout(() => {}, 60000)
    const fired = await new Promise((resolve) => {
      const timer = setTimeout(() => resolve(timer), 1)
    })
    clearTimeout(cleared)
    await new Promise((resolve) => setImmediate(resolve))
    const pending = ledger.stop()
    clearTimeout(earlier)
    clearTimeout(retained)

    expect(pending).toContain(retained)
    for (const gone of [earlier, cleared, fired]) expect(pending).not.toContain(gone)
  })

  test('a timer ledger records deep creation stacks for unref timers and restores the stack limit', () => {
    const ledger = new TimerLedger()
    const previousLimit = Error.stackTraceLimit
    const createOwnedTimer = () => setTimeout(() => {}, 60000).unref()
    const descend = (depth) => (depth ? descend(depth - 1) : createOwnedTimer())
    let retained
    let pending
    let diagnostics
    let restoredLimit
    try {
      Error.stackTraceLimit = 2
      ledger.start()
      retained = descend(20)
      restoredLimit = Error.stackTraceLimit
      diagnostics = ledger.read()
    } finally {
      pending = ledger.stop()
      clearTimeout(retained)
      Error.stackTraceLimit = previousLimit
    }

    expect(restoredLimit).toBe(2)
    expect(pending).toEqual([retained])
    expect(diagnostics).toMatchObject({ retained: 1, omitted: 0, missingStacks: 0 })
    expect(diagnostics.timers).toHaveLength(1)
    expect(diagnostics.timers[0]).toMatchObject({
      delayMs: 60000,
      referenced: false,
      destroyed: false,
      source: 'repo',
    })
    const { stack } = diagnostics.timers[0]
    expect(stack).toMatch(/createOwnedTimer.*src\/broker\/__tests__\/floorCoreLive\.spec\.js:\d+/)
    expect(stack).not.toContain('node:internal')
    expect(stack).not.toContain('node_modules')
    expect(stack.split('\n').length).toBeLessThanOrEqual(8)
    expect(stack.split('\n').every((frame) => frame.length <= 512)).toBe(true)
    expect(ledger.read()).toEqual({ retained: 0, omitted: 0, missingStacks: 0, timers: [] })
  })

  test('a timer ledger bounds diagnostics without dropping owned timers and releases destroyed stacks', async () => {
    const ledger = new TimerLedger({ capacity: 1 })
    const timers = []
    let overflow
    let reused
    let pending
    try {
      ledger.start()
      timers.push(
        setTimeout(() => {}, 60000),
        setTimeout(() => {}, 60000)
      )
      overflow = ledger.read()
      for (const timer of timers) clearTimeout(timer)
      expect(ledger.read()).toMatchObject({ retained: 2, omitted: 1, missingStacks: 1 })
      expect(ledger.read().timers[0].destroyed).toBe(true)
      await new Promise((resolve) => setImmediate(resolve))
      expect(ledger.read()).toEqual({ retained: 0, omitted: 0, missingStacks: 0, timers: [] })
      timers.push(setTimeout(() => {}, 60000))
      reused = ledger.read()
    } finally {
      pending = ledger.stop()
      for (const timer of timers) clearTimeout(timer)
    }

    expect(overflow).toMatchObject({ retained: 2, omitted: 1, missingStacks: 1 })
    expect(overflow.timers).toHaveLength(1)
    expect(overflow.timers[0].source).toBe('repo')
    expect(reused).toMatchObject({ retained: 1, omitted: 0, missingStacks: 0 })
    expect(reused.timers[0].source).toBe('repo')
    expect(pending).toEqual([timers[2]])
  })

  test('credential markers cover the encodings a log or a buffer dump can show', () => {
    expect(markersOf('ab')).toEqual(['ab', '6162', 'YWI=', '61 62', '97,98'])
    expect(markersOf(Buffer.from([0xff]))).toContain('ff')
  })

  test('a held dial withholds readiness, counts one attempt per destroyed socket and lets a free dial through', () => {
    const made = []
    const transport = (options) => {
      const socket = { destroyed: false, options, destroy: () => (socket.destroyed = true) }
      made.push(socket)
      return socket
    }
    const dial = new HeldDial(transport)
    const ready = []
    dial.holding = true
    const held = dial.factory({ onConnect: () => ready.push('held') })
    made[0].options.onConnect()
    held.destroy()
    held.destroy()
    dial.holding = false
    dial.factory({ onConnect: () => ready.push('free') })
    made[1].options.onConnect()

    expect(ready).toEqual(['free'])
    expect(dial.attempts).toHaveLength(1)
    expect(dial.attempts[0]).toBeGreaterThanOrEqual(0)
    expect(dial.sockets).toEqual(made)
  })
})

describeFloor('Administration and connection capabilities on the floor', () => {
  const budget = new SuiteBudget()
  const CASES = ['F10', 'F11', 'F12', 'F13', 'F14', 'F15', 'F16', 'F17']
  /** @param {string} title the exact case title @param {string} method the case it runs */
  const floorCase = (title, method) =>
    test(title, () => runFloorCase(budget, method), caseTimeout(SUITE_MS))

  floorCase('deletes offsets and fetches three groups through negotiated versions', 'offsets')
  floorCase('describes active transactions and refuses an unavailable pattern', 'diagnostics')
  floorCase('aborts a physical transaction and fences the original producer', 'abortFence')
  floorCase(
    'lists cluster resources and refuses filters absent from the negotiated version',
    'cluster'
  )
  floorCase(
    'reports replicas in their current directory without inventing a cordon flag',
    'logDirs'
  )
  floorCase('retains reassignment and offset-spec refusals on the negotiated floor', 'reassignment')
  floorCase('round trips quota and SCRAM administration without leaking credentials', 'security')
  floorCase('bounds setup, idle reopening and a stalled administrative call', 'connection')

  test('every administration and connection evidence line carries the stamps a verifier checks', () => {
    const lines = evidenceLines(EVIDENCE)
      .slice(linesBefore)
      .filter(({ case: name }) => CASES.includes(name))
    const verified = {
      tree: process.env.KAFKAJS_TREE || 'unfrozen',
      node: process.version,
      brokerVersion: process.env.KAFKA_VERSION,
    }

    expect(lines.map(({ case: name }) => name).sort()).toEqual(CASES)
    for (const line of lines) expect(problemsOf(line, verified)).toEqual([])
  })
})

const SHARE_FLOOR_TITLE = 'F23 share groups are refused on the broker floor'
const SHARE_FLOOR_BUDGET_MS = 30000
const SHARE_FLOOR_CLEANUP_MS = 8000
const SHARE_FLOOR_HOOK_MS = 10000
const SHARE_API_KEYS = [76, 78, 79]

/** What the public client must throw on a floor broker that does not advertise share groups. */
const SHARE_FLOOR_REFUSAL = {
  reason: 'api-not-advertised',
  apiKey: 76,
  apiName: 'ShareGroupHeartbeat',
  shareVersion: 0,
}

/**
 * Logs every request frame the clients of one case write and counts their sockets and unanswered
 * requests from the bytes actually written and read. Every byte passes through untouched.
 */
class FloorSocketLedger {
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
 * @param {{ sockets: { open: number }, activeResources: Record<string, number>, listeners: Record<string, number>, queueDepth: number, ledgerSize: number }} snapshot a measurement
 * @returns {object} the evidence form of the measurement
 */
const shareFloorResources = (snapshot) => ({
  openSockets: snapshot.sockets.open,
  timers: snapshot.activeResources.Timeout ?? 0,
  listeners: Object.values(snapshot.listeners).reduce((sum, n) => sum + n, 0),
  queueDepth: snapshot.queueDepth,
  ledgerSize: snapshot.ledgerSize,
})

/**
 * Compares exactly the five counts written into F23 evidence. Socket ownership comes from the
 * case ledger; unrelated handles in the process-wide inventory are not case resources. Every
 * Timeout still counts, so a retained product timer cannot be excluded as harness activity.
 * @param {object} baseline the measurement before the client exists
 * @param {object} after the measurement after disconnect
 * @returns {void}
 * @throws {Error} when the measured resources have not returned to baseline
 */
const assertShareFloorResources = (baseline, after) => {
  const beforeResources = shareFloorResources(baseline)
  const afterResources = shareFloorResources(after)
  const differing = Object.keys(beforeResources).filter(
    (key) => beforeResources[key] !== afterResources[key]
  )
  if (differing.length) {
    // Name the differences first: pollTolerating truncates its summary but retains this cause.
    throw new Error(
      `Resources differ (${differing.join(', ')}): ${JSON.stringify(beforeResources)} -> ${JSON.stringify(afterResources)}`
    )
  }
}

/**
 * The share refusal of the floor: a public share client connects once to the 4.0.2 broker, which
 * does not advertise the share APIs, and must be refused before it writes any share frame. It
 * creates no topic, group or record, and returns the five resource counts in its evidence to
 * their baseline.
 */
class FloorShareCase {
  /** Builds a case with fresh names; nothing is created until `run`. */
  constructor() {
    this.nonce = secureRandom(6)
    this.group = `floor-share-${this.nonce}`
    this.stack = new CleanupStack()
    this.ledger = new FloorSocketLedger()
    this.requests = []
    this.connects = 0
    this.openHandlers = 0
    this.baseline = null
    this.after = null
    this.probe = createResourceProbe({
      emitters: { process },
      sockets: () => this.ledger.counts(),
      queueDepth: () => this.ledger.pending(),
      ledgerSize: () => this.openHandlers,
      activeResources: true,
    })
  }

  /**
   * Makes the one public connect and records the refusal, then proves the resources are back.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} the first assertion or fixture failure, with cleanup failures attached
   */
  async run(scope) {
    try {
      expect(process.env.KAFKA_VERSION).toBe('4.0.2')
      this.baseline = await live.bounded(scope, 10000, () => this.probe.snapshot())
      let error
      await this.stack.run(
        async () => {
          error = await this.attempt(scope)
          this.expectRefusal(error)
        },
        { budgetMs: SHARE_FLOOR_CLEANUP_MS }
      )
      await this.settle(scope)
      this.record(error)
    } catch (failure) {
      writeEvidence(EVIDENCE, {
        event: 'case-failed',
        case: SHARE_FLOOR_TITLE,
        scenario: this.nonce,
        error: describeError(failure),
        shareFrames: this.ledger.shareFrames(),
      })
      throw failure
    }
  }

  /**
   * Builds the share consumer and connects it once, capturing what connect throws before anything
   * is asserted. Its listeners and disconnect are registered before the connect starts.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<Error|null>} what connect threw, or null when it resolved
   */
  async attempt(scope) {
    const consumer = new Kafka({
      clientId: `floor-share-${this.nonce}`,
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      socketFactory: this.ledger.factory(),
    }).shareConsumer({ groupId: this.group })
    const removers = [
      consumer.on(consumer.events.REQUEST, ({ payload }) => this.requests.push(payload)),
      consumer.on(consumer.events.CONNECT, () => this.connects++),
    ]
    this.stack.add('remove share listeners', () => removers.forEach((remove) => remove()))
    this.stack.add('disconnect share consumer', () => consumer.disconnect({ timeout: 2000 }))
    try {
      await live.bounded(scope, 10000, () => consumer.connect())
      return null
    } catch (error) {
      return error
    }
  }

  /**
   * @param {Error|null} error what connect threw
   * @returns {void}
   * @throws {Error} on the first property that is not the expected refusal
   */
  expectRefusal(error) {
    if (error === null) {
      throw new Error('connect resolved on a broker that does not advertise share groups')
    }
    expect(error).toBeInstanceOf(KafkaJSShareGroupsNotSupportedError)
    expect(error).toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
    expect(error).toMatchObject({ ...SHARE_FLOOR_REFUSAL, retriable: false })
    expect(error.message).toContain(SHARE_FLOOR_REFUSAL.reason)
    expect(this.ledger.shareFrames()).toEqual([])
    expect(this.connects).toBe(0)
  }

  /**
   * Waits for case sockets, timers, listeners and pending work to return to baseline.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} naming the last measurement when they are not back at the deadline
   */
  async settle(scope) {
    await live.pollTolerating(
      scope,
      8000,
      async () => {
        this.after = await this.probe.snapshot()
        assertShareFloorResources(this.baseline, this.after)
        return true
      },
      { delayMs: 50, label: 'The resources returning to baseline' }
    )
  }

  /**
   * Writes the observation row and the single completion row of the case.
   * @param {Error} error the refusal connect threw
   * @returns {void}
   */
  record(error) {
    const outcome = {
      kind: 'unsupported',
      className: error.name,
      reason: error.reason,
      apiKey: error.apiKey,
      apiName: error.apiName,
      shareVersion: error.shareVersion,
      retriable: error.retriable,
      message: error.message,
    }
    const wireVersions = wireVersionsOf(this.requests)
    writeEvidence(EVIDENCE, { case: 'F23', outcome, shareFrames: [], wireVersions })
    writeEvidence(EVIDENCE, {
      event: 'case-complete',
      case: SHARE_FLOOR_TITLE,
      profile: 'tv2',
      scenario: this.nonce,
      groups: { floor: this.group },
      budgets: { caseMs: SHARE_FLOOR_BUDGET_MS, cleanupMs: SHARE_FLOOR_CLEANUP_MS },
      status: 'passed',
      tainted: false,
      connectAttempted: true,
      outcome,
      shareFrames: [],
      oracles: {
        log: { expected: [], actual: [] },
        handler: { expected: [], actual: [] },
        broker: [],
      },
      resources: {
        before: shareFloorResources(this.baseline),
        after: shareFloorResources(this.after),
      },
      cleanup: {
        groupsAbsent: true,
        topicsAbsent: true,
        completedAt: Number(process.hrtime.bigint() / 1000000n),
      },
      wireVersions,
      config: { line: process.env.KAFKA_VERSION },
    })
  }
}

const { ordinaryOwnership } = require('../../../scripts/e2e/telemetry/semantics')
const { persistEvidence, verifyScenario } = require('../../../scripts/e2e/telemetry/evidence')

const digestFloor = (keys) =>
  require('crypto')
    .createHash('sha256')
    .update([...keys].sort().join('\n'))
    .digest('hex')

const F24_TITLE = 'telemetry remains quiet on an unsupported broker'
const F24_BUDGET_MS = 58000
const F24_RECORDS = 1000
const F24_PARTITIONS = 3

/**
 * F24: on the floor broker, which advertises neither telemetry API, telemetry-enabled producer,
 * ordinary consumer and admin keep every supported operation and send no 71/72 frame or extra
 * connection. A stable 1000-record workload has exact durable-log, raw-commit and handler sets,
 * each read independently of the client.
 */
class FloorTelemetryCase {
  /** Builds a case with fresh names; nothing is created until `run`. */
  constructor() {
    this.nonce = secureRandom(6)
    this.arm = `floor-telemetry-${this.nonce}`
    this.topic = `${this.arm}-topic`
    this.group = `${this.arm}-group`
    this.stack = new CleanupStack()
    this.expected1000Ids = Array.from(
      { length: F24_RECORDS },
      (_, i) => `${this.arm}-${String(i).padStart(4, '0')}`
    )
    this.ends = Object.fromEntries(
      Array.from({ length: F24_PARTITIONS }, (_, p) => [
        p,
        String(this.expected1000Ids.filter((_id, i) => i % F24_PARTITIONS === p).length),
      ])
    )
    this.handled = []
    this.processedNext = {}
    this.committedNext = {}
    this.observers = {}
  }

  /**
   * @param {number} index record number
   * @returns {Buffer} exactly 200 deterministic bytes that carry the identity
   */
  value(index) {
    return Buffer.from(`${this.expected1000Ids[index]}|`.padEnd(200, 'x'))
  }

  /**
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} the first assertion or fixture failure, with cleanup failures attached
   */
  async run(scope) {
    try {
      expect(process.env.KAFKA_VERSION).toBe('4.0.2')
      const directory = path.resolve('.tmp/e2e', this.arm)
      fs.mkdirSync(directory, { recursive: true })
      let evidence
      await this.stack.run(
        async () => {
          evidence = await this.work(scope, directory)
        },
        { budgetMs: 8000 }
      )
      const record = {
        status: 'passed',
        run: { stem: this.arm, line: process.env.KAFKA_VERSION },
        corpus: evidence.corpus,
        oracles: { corpus: evidence.corpus.inputs },
        outcomes: Object.fromEntries(
          ['producer', 'consumer', 'admin'].map((kind) => [kind, { status: 'connected' }])
        ),
      }
      record.sidecars = persistEvidence({
        name: 'telemetry-floor',
        record,
        directory,
        observers: this.observers,
      })
      verifyScenario(record, directory)
      writeEvidence(EVIDENCE, {
        case: 'F24',
        archiveDirectory: path.relative(path.resolve('.tmp/e2e'), directory),
        archive: record,
        ...evidence,
        wireVersions: {},
        config: { line: process.env.KAFKA_VERSION },
      })
    } catch (failure) {
      writeEvidence(EVIDENCE, {
        event: 'case-failed',
        case: F24_TITLE,
        scenario: this.nonce,
        error: describeError(failure),
      })
      throw failure
    }
  }

  /**
   * Builds one telemetry-enabled client on an observed base-fixture socket factory; it is
   * disconnected, its observer disposed and required clean when the case ends.
   * @param {string} kind producer, consumer or admin
   * @param {string} directory absolute observer directory
   * @returns {object} the public client
   */
  client(kind, directory) {
    const observer = new TelemetryWireObserver({
      directory,
      line: process.env.KAFKA_VERSION,
      clock: { now: () => performance.now() },
      mode: 'base',
      owner: kind,
    })
    this.observers[kind] = observer
    const kafka = new Kafka({
      clientId: `${this.arm}-${kind}`,
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      socketFactory: (options) => observer.socketFactory(options),
    })
    const client =
      kind === 'producer'
        ? kafka.producer({ enableMetricsPush: true, idempotent: true })
        : kind === 'admin'
          ? kafka.admin({ enableMetricsPush: true })
          : kafka.consumer({
              enableMetricsPush: true,
              groupId: this.group,
              sessionTimeout: 30000,
              heartbeatInterval: 3000,
              maxWaitTimeInMs: 500,
              readUncommitted: false,
            })
    this.stack.add(`require ${kind} observer clean`, () => observer.assertClean())
    this.stack.add(`dispose ${kind} observer`, () => observer.dispose())
    this.stack.add(`disconnect ${kind}`, () => observer.closeClient(client))
    return client
  }

  /**
   * @param {AbortSignal} scope the case scope
   * @param {string} directory absolute observer directory
   * @returns {Promise<object>} the evidence of the three independent oracles and the quiet path
   */
  async work(scope, directory) {
    const brokers = plainTextBrokers()
    await live.bounded(scope, 20000, () =>
      createTopic({
        topic: this.topic,
        partitions: F24_PARTITIONS,
        replicas: 3,
        config: [{ name: 'min.insync.replicas', value: '2' }],
      })
    )
    const producer = this.client('producer', directory)
    const consumer = this.client('consumer', directory)
    const admin = this.client('admin', directory)
    const raw = new Kafka({
      clientId: `${this.arm}-raw`,
      brokers,
      logLevel: logLevel.NOTHING,
    }).admin({ enableMetricsPush: false })
    this.stack.add('disconnect raw admin', () => raw.disconnect())
    await live.bounded(scope, 10000, () =>
      Promise.all([producer.connect(), consumer.connect(), admin.connect(), raw.connect()])
    )
    for (const observer of Object.values(this.observers)) observer.confirmForegroundBootstrap()
    await live.bounded(scope, 15000, () =>
      producer.send({
        topic: this.topic,
        acks: -1,
        messages: this.expected1000Ids.map((key, i) => ({
          key,
          value: this.value(i),
          partition: i % F24_PARTITIONS,
        })),
      })
    )
    await consumer.subscribe({ topic: this.topic, fromBeginning: true })
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ partition, message }) => {
        this.handled.push({
          partition,
          offset: message.offset,
          key: message.key.toString(),
          ordinal: this.handled.length + 1,
        })
        const next = String(BigInt(message.offset) + 1n)
        this.processedNext[partition] = next
        if (next === this.ends[partition] || this.handled.length % 100 === 0) {
          await consumer.commitOffsets([{ topic: this.topic, partition, offset: next }])
          this.committedNext[partition] = next
        }
      },
    })
    await live.pollUntil(
      scope,
      25000,
      () => Object.entries(this.ends).every(([p, end]) => this.committedNext[p] === end),
      { delayMs: 100, label: 'All floor records handled and committed' }
    )
    await consumer.stop()
    await live.bounded(scope, 10000, () => admin.describeCluster())

    const log = await live.bounded(scope, 20000, () =>
      readLog({ brokers: [brokers[0]], topic: this.topic })
    )
    const durableIds = []
    const rawEndByPartition = {}
    for (const [partition, { endOffset, records }] of log) {
      rawEndByPartition[partition] = endOffset
      for (const record of records) {
        const index = this.expected1000Ids.indexOf(record.key.toString())
        expect(record.value.equals(this.value(index))).toBe(true)
        durableIds.push(record.key.toString())
      }
    }
    const fetched = await live.bounded(scope, 10000, () =>
      raw.fetchOffsets({ groupId: this.group, topics: [this.topic], resolveOffsets: false })
    )
    const rows = fetched[0]?.partitions ?? fetched
    const rawCommittedNextByPartition = Object.fromEntries(
      rows.map((r) => [r.partition, String(r.offset)])
    )
    const processedNextByPartition = { ...this.processedNext }
    const handlerIds = this.handled.map((h) => h.key)
    const sortedDurableIds = [...durableIds].sort()
    const sortedHandlerIds = [...handlerIds].sort()
    const expected1000Ids = [...this.expected1000Ids].sort()

    const adverts = Object.values(this.observers).flatMap((observer) => observer.advertisements())
    const offered = adverts.flatMap((a) =>
      [71, 72].filter((apiKey) => a.ranges[apiKey] !== null).map((apiKey) => ({ apiKey }))
    )
    const observedTelemetryRequests = Object.values(this.observers).flatMap((observer) =>
      observer.readTranscript().filter((r) => r.apiKey === 71 || r.apiKey === 72)
    )
    const observedTelemetryConnections = Object.values(this.observers).flatMap((observer) =>
      observer.telemetrySockets()
    )
    expect(adverts.length).toBeGreaterThan(0)
    expect(offered.some(({ apiKey }) => apiKey === 71 || apiKey === 72)).toBe(false)
    expect(observedTelemetryRequests).toEqual([])
    expect(observedTelemetryConnections).toEqual([])
    for (const observer of Object.values(this.observers))
      ordinaryOwnership({ records: observer.readTranscript(), sockets: observer.socketOwnership() })
    expect(sortedDurableIds).toEqual(expected1000Ids)
    expect(sortedHandlerIds).toEqual(expected1000Ids)
    expect(new Set(handlerIds).size).toBe(F24_RECORDS)
    expect(rawCommittedNextByPartition).toEqual(processedNextByPartition)
    expect(rawCommittedNextByPartition).toEqual(rawEndByPartition)
    for (const kind of ['producer', 'consumer', 'admin']) {
      const client = { producer, consumer, admin }[kind]
      const error = await client.clientInstanceId({ timeout: 0 }).then(
        () => null,
        (e) => e
      )
      expect(error).toMatchObject({ name: 'KafkaJSTelemetryError', reason: 'unsupported' })
      this.observers[kind].assertHealthy()
    }
    return {
      corpus: {
        count: F24_RECORDS,
        expectedSha256: digestFloor(expected1000Ids),
        logSha256: digestFloor(durableIds),
        handlerSha256: digestFloor(handlerIds),
        handlerEntries: this.handled.length,
        rawCommitted: rawCommittedNextByPartition,
        processedNext: processedNextByPartition,
        rawEnd: rawEndByPartition,
        inputs: {
          expected: this.expected1000Ids.map((key, i) => ({
            key,
            partition: i % F24_PARTITIONS,
            valueHex: this.value(i).toString('hex'),
          })),
          log: [...log].flatMap(([partition, { records }]) =>
            records.map((r) => ({
              partition,
              offset: r.offset,
              key: r.key.toString(),
              valueHex: r.value.toString('hex'),
            }))
          ),
          handled: this.handled,
          commits: fetched,
          ends: rawEndByPartition,
        },
      },
      records: F24_RECORDS,
      handlerEntries: this.handled.length,
      rawCommittedNextByPartition,
      rawEndByPartition,
      offered: offered.length,
      telemetryRequests: observedTelemetryRequests.length,
      telemetryConnections: observedTelemetryConnections.length,
    }
  }
}

describeFakeLane('Floor share resource comparison', () => {
  /** @returns {object} the five measured resource counts, plus raw process/socket history */
  const measurement = () => ({
    sockets: { created: 0, destroyed: 0, open: 0 },
    activeResources: { PipeWrap: 2, Timeout: 7 },
    listeners: { process: 0 },
    queueDepth: 0,
    ledgerSize: 0,
  })

  test('accepts equal evidence projections despite unrelated process and socket history changes', () => {
    const before = measurement()
    const after = {
      ...measurement(),
      sockets: { created: 1, destroyed: 1, open: 0 },
      activeResources: { Timeout: 7, PipeWrap: 1 },
    }

    expect(shareFloorResources(after)).toEqual(shareFloorResources(before))
    expect(() => assertShareFloorResources(before, after)).not.toThrow()
  })

  test('treats an absent Timeout entry as the measured zero printed in evidence', () => {
    const before = { ...measurement(), activeResources: {} }
    const after = { ...measurement(), activeResources: { Timeout: 0 } }

    expect(() => assertShareFloorResources(before, after)).not.toThrow()
  })

  test.each([
    ['openSockets', { sockets: { created: 1, destroyed: 0, open: 1 } }],
    ['timers', { activeResources: { PipeWrap: 2, Timeout: 8 } }],
    ['listeners', { listeners: { process: 1 } }],
    ['queueDepth', { queueDepth: 1 }],
    ['ledgerSize', { ledgerSize: 1 }],
  ])('rejects a retained %s and names it before the full compared projections', (field, change) => {
    const before = measurement()
    const after = { ...measurement(), ...change }

    expect(() => assertShareFloorResources(before, after)).toThrow(
      `Resources differ (${field}): ${JSON.stringify(shareFloorResources(before))} -> ${JSON.stringify(shareFloorResources(after))}`
    )
  })

  test('still detects a real retained timer through the resource probe', async () => {
    const probe = createResourceProbe({
      sockets: () => ({ created: 0, destroyed: 0 }),
      activeResources: true,
      emitters: { process },
      queueDepth: () => 0,
      ledgerSize: () => 0,
    })
    const before = await probe.snapshot()
    const timer = setTimeout(() => {}, 60000)
    try {
      const after = await probe.snapshot()
      expect(shareFloorResources(after).timers).toBe(shareFloorResources(before).timers + 1)
      expect(() => assertShareFloorResources(before, after)).toThrow('Resources differ (timers)')
    } finally {
      clearTimeout(timer)
    }
    const cleared = await probe.snapshot()
    expect(() => assertShareFloorResources(before, cleared)).not.toThrow()
  })
})

describeFloor('Share groups on the broker floor', () => {
  beforeAll(
    () =>
      live.bounded(live.NEVER, SHARE_FLOOR_HOOK_MS - 2000, () => {
        live.liveRun.assertClean()
      }),
    SHARE_FLOOR_HOOK_MS
  )

  test(
    SHARE_FLOOR_TITLE,
    live.liveCase(SHARE_FLOOR_BUDGET_MS, (scope) => new FloorShareCase().run(scope)),
    caseTimeout(SHARE_FLOOR_BUDGET_MS)
  )

  test(
    F24_TITLE,
    live.liveCase(F24_BUDGET_MS, (scope) => new FloorTelemetryCase().run(scope)),
    caseTimeout(F24_BUDGET_MS)
  )

  afterEach(() => {
    if (!live.liveRun.takeAdmission()) return
    live.liveRun.assertClean()
  })

  test('the share refusal evidence of this run carries the stamps a verifier checks', () => {
    const lines = evidenceLines(EVIDENCE)
      .slice(linesBefore)
      .filter(({ case: name }) => name === 'F23' || name === SHARE_FLOOR_TITLE || name === 'F24')
    const verified = {
      tree: process.env.KAFKAJS_TREE || 'unfrozen',
      node: process.version,
      brokerVersion: process.env.KAFKA_VERSION,
    }

    expect(lines.map(({ case: name }) => name).sort()).toEqual(
      ['F23', SHARE_FLOOR_TITLE, 'F24'].sort()
    )
    for (const line of lines) expect(problemsOf(line, verified)).toEqual([])
  })
})
