const { Kafka, Partitioners, logLevel } = require('../../../index')
const {
  secureRandom,
  plainTextBrokers,
  createCluster,
  writeEvidence,
  wireVersionsOf,
} = require('testHelpers')
const { readLog } = require('../../../testHelpers/rawLog')
const { waitForCondition, withTimeout } = require('../../consumer/__tests__/fixtures/liveScenario')

// Gated: it creates and deletes a topic on the three-rack cluster. Run it alone through
// `npm run test:e2e:rack-aware-producer`; `KAFKA_VERSION` names the cluster under test.
const describeRackAwareProducer =
  process.env.RACK_AWARE_PRODUCER_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'rack-aware-producer'
// The scenario budget is the whole file, from the first hook to the last cleanup.
const SCENARIO_BUDGET_MS = 180000
const CASE_BUDGET_MS = 40000
const HOOK_BUDGET_MS = 40000
const DISCONNECT_BUDGET_MS = 10000
const PLACEMENT_BUDGET = { attempts: 100, delay: 250 } // 25 s

/** The brokers' racks in `docker-compose.racks.yml`. */
const BROKER_RACKS = { 1: 'rack-a', 2: 'rack-b', 3: 'rack-c' }
/** Six partitions of two replicas; the first replica of each is its preferred and initial leader. */
const REPLICAS = [
  [1, 2],
  [2, 3],
  [3, 1],
  [1, 3],
  [2, 1],
  [3, 2],
]
const LEADERS = { 0: 1, 1: 2, 2: 3, 3: 1, 4: 2, 5: 3 }
const OWN_RACK = 'rack-b'
/** Where the Java client's hash puts these keys on six partitions: on and off `OWN_RACK`. */
const KEYED = [
  ['alpha', 4],
  ['bravo', 5],
  ['charlie', 0],
  ['delta', 2],
  ['echo', 5],
  ['foxtrot', 3],
]
const EXPLICIT = [0, 2, 5]
const ALL_PARTITIONS = [0, 1, 2, 3, 4, 5]

/**
 * Live proof of KIP-1123's producer rack preference on three brokers, each in a rack of its own,
 * with a topic whose six partitions are led two to a rack. The placement is read from the
 * cluster's own Metadata before every case and again after it, and the case fails when it is not
 * the one asked for, so no assertion is relaxed after a topology change. Each record is sent on
 * its own, so its acknowledgement names its partition and offset; after each case the topic is
 * read straight from the partition leaders, with no consumer group, and holds exactly the records
 * every case so far was acknowledged for, each once, at the partition and offset it was
 * acknowledged at.
 *
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java
 */
describeRackAwareProducer('Producer > rack-aware partitioning (live)', () => {
  const runId = secureRandom()
  const topic = `e2e-rack-aware-producer-${runId}-topic`
  const brokers = plainTextBrokers()
  const kafka = new Kafka({
    clientId: 'rack-aware-producer-live',
    brokers,
    logLevel: logLevel.NOTHING,
  })
  const admin = kafka.admin()
  /** @type {object} reads Metadata for the placement; made by `beforeAll`, so a skipped run holds none */
  let cluster
  /** @type {{ value: string, partition: number, offset: string }[]} every acknowledged record */
  const ledger = []
  /** what cleanup could not undo; a topic already gone is no failure */
  const cleanupFailures = []
  let startedAt

  /**
   * @returns {Promise<{ racks: Record<number, string|null>, leaders: Record<number, number> }>} what
   *   the cluster's current Metadata says of the brokers' racks and the topic's partition leaders
   */
  const placementOf = async () => {
    await cluster.refreshMetadata()
    const { brokers: nodes } = cluster.brokerPool.metadata
    return {
      racks: Object.fromEntries(nodes.map(({ nodeId, rack }) => [nodeId, rack])),
      leaders: Object.fromEntries(
        cluster
          .findTopicPartitionMetadata(topic)
          .map(({ partitionId, leader }) => [partitionId, leader])
      ),
    }
  }

  /**
   * Waits for the placement the fixture asks for, then returns it.
   * @returns {Promise<{ racks: Record<number, string|null>, leaders: Record<number, number> }>}
   */
  const verifiedPlacement = async () => {
    const wanted = { racks: BROKER_RACKS, leaders: LEADERS }
    await waitForCondition(
      async () => JSON.stringify(await placementOf()) === JSON.stringify(wanted),
      PLACEMENT_BUDGET
    )
    const placement = await placementOf()
    expect(placement).toEqual(wanted)

    return placement
  }

  /**
   * @param {{ racks: Record<number, string|null>, leaders: Record<number, number> }} placement
   * @param {string} rack
   * @returns {number[]} the partitions whose leader the placement puts in `rack`, in order
   */
  const ledIn = ({ racks, leaders }, rack) =>
    ALL_PARTITIONS.filter((partition) => racks[leaders[partition]] === rack)

  /**
   * Sends `count` records of one corpus one at a time, each with its own acknowledgement.
   * @param {object} producer
   * @param {string} name names the corpus, so no value repeats across cases
   * @param {number} count
   * @param {(index: number) => object} [shape] the fields a record has besides its value
   * @returns {Promise<{ value: string, partition: number, offset: string }[]>} what was acknowledged
   */
  const sendEach = async (producer, name, count, shape = () => ({})) => {
    const acknowledged = []
    for (let index = 0; index < count; index += 1) {
      const value = `${runId}-${name}-${index}`
      const result = await producer.send({ topic, messages: [{ ...shape(index), value }] })
      expect(result).toHaveLength(1)
      acknowledged.push({ value, partition: result[0].partition, offset: result[0].baseOffset })
    }
    ledger.push(...acknowledged)

    return acknowledged
  }

  /** @returns {Promise<void>} once the topic's log holds exactly the ledger, each record once */
  const expectLogToEqualLedger = async () => {
    const log = await readLog({ brokers, topic })
    const physical = [...log].flatMap(([partition, { records }]) =>
      records.map(({ value, offset }) => ({ value: value.toString(), partition, offset }))
    )
    const byValue = (a, b) => (a.value < b.value ? -1 : 1)

    expect(physical.sort(byValue)).toEqual([...ledger].sort(byValue))
  }

  /** @returns {Record<number, number>} how many of `records` each partition was acknowledged for */
  const countsOf = (records) =>
    records.reduce((counts, { partition }) => {
      counts[partition] = (counts[partition] ?? 0) + 1
      return counts
    }, {})

  /**
   * Runs one case's sends with a producer that is always disconnected afterwards, between two
   * reads of the placement that must agree.
   * @param {object} options the producer's options
   * @param {(producer: object, placement: object, wireVersions: () => Record<string, number>) => Promise<void>} work
   *   given what the producer's requests negotiated, for the evidence line
   * @returns {Promise<void>}
   */
  const withProducer = async (options, work) => {
    const before = await verifiedPlacement()
    const producer = kafka.producer({ linger: 0, ...options })
    const requests = []
    producer.on(producer.events.REQUEST, ({ payload }) =>
      requests.push({ apiName: payload.apiName, apiVersion: payload.apiVersion })
    )
    try {
      await producer.connect()
      await work(producer, before, () => wireVersionsOf(requests))
    } finally {
      await withTimeout(producer.disconnect(), DISCONNECT_BUDGET_MS)
    }
    expect(await placementOf()).toEqual(before)
    await expectLogToEqualLedger()
  }

  beforeAll(async () => {
    startedAt = Date.now()
    await admin.connect()
    await admin.createTopics({
      waitForLeaders: true,
      timeout: 30000,
      topics: [
        {
          topic,
          replicaAssignment: REPLICAS.map((replicas, partition) => ({ partition, replicas })),
        },
      ],
    })
    cluster = createCluster({}, brokers)
    await cluster.connect()
    await cluster.addTargetTopic(topic)
    await verifiedPlacement()
  }, HOOK_BUDGET_MS)

  afterEach(() => {
    expect(Date.now() - startedAt).toBeLessThan(SCENARIO_BUDGET_MS)
  })

  afterAll(async () => {
    const cleanup = async (work) => {
      try {
        await work()
      } catch (error) {
        if (error.type !== 'UNKNOWN_TOPIC_OR_PARTITION') cleanupFailures.push(error)
      }
    }
    await cleanup(() => admin.deleteTopics({ topics: [topic], timeout: 10000 }))
    await cleanup(() => withTimeout(cluster?.disconnect(), DISCONNECT_BUDGET_MS))
    await cleanup(() => withTimeout(admin.disconnect(), DISCONNECT_BUDGET_MS))

    expect(cleanupFailures).toEqual([])
    expect(Date.now() - startedAt).toBeLessThan(SCENARIO_BUDGET_MS)
  }, HOOK_BUDGET_MS)

  test(
    'the default partitioner of a rack-aware producer sends keyless records to a partition led in its rack',
    async () => {
      await withProducer(
        { rackId: OWN_RACK, rackAware: true },
        async (producer, placement, wireVersions) => {
          const matching = ledIn(placement, OWN_RACK)
          expect(matching).toEqual([1, 4])

          const acknowledged = await sendEach(producer, 'default', 12)

          expect(acknowledged.filter(({ partition }) => !matching.includes(partition))).toEqual([])
          writeEvidence(EVIDENCE, {
            wireVersions: wireVersions(),
            step: 'default-partitioner',
            rackId: OWN_RACK,
            placement,
            counts: countsOf(acknowledged),
          })
        }
      )
    },
    CASE_BUDGET_MS
  )

  test(
    'DefaultPartitioner of a rack-aware producer alternates over exactly the partitions led in its rack',
    async () => {
      await withProducer(
        { createPartitioner: Partitioners.DefaultPartitioner, rackId: OWN_RACK, rackAware: true },
        async (producer, placement, wireVersions) => {
          const matching = ledIn(placement, OWN_RACK)

          const acknowledged = await sendEach(producer, 'round-robin', 40)

          expect(countsOf(acknowledged)).toEqual({ [matching[0]]: 20, [matching[1]]: 20 })
          writeEvidence(EVIDENCE, {
            wireVersions: wireVersions(),
            step: 'round-robin-in-rack',
            rackId: OWN_RACK,
            placement,
            counts: countsOf(acknowledged),
          })
        }
      )
    },
    CASE_BUDGET_MS
  )

  test(
    'a rack that no partition leader is in falls back to every partition',
    async () => {
      await withProducer(
        {
          createPartitioner: Partitioners.DefaultPartitioner,
          rackId: 'rack-z',
          rackAware: true,
        },
        async (producer, placement, wireVersions) => {
          expect(ledIn(placement, 'rack-z')).toEqual([])

          const acknowledged = await sendEach(producer, 'fallback', 36)

          expect(countsOf(acknowledged)).toEqual(
            Object.fromEntries(ALL_PARTITIONS.map((partition) => [partition, 6]))
          )
          writeEvidence(EVIDENCE, {
            wireVersions: wireVersions(),
            step: 'fallback',
            rackId: 'rack-z',
            placement,
            counts: countsOf(acknowledged),
          })
        }
      )
    },
    CASE_BUDGET_MS
  )

  test.each([
    ['a rackId alone', { rackId: OWN_RACK }],
    ['rackAware: false and a rackId', { rackId: OWN_RACK, rackAware: false }],
  ])(
    'a producer with %s spreads keyless records over every partition',
    async (name, options) => {
      await withProducer(
        { createPartitioner: Partitioners.DefaultPartitioner, ...options },
        async (producer, placement, wireVersions) => {
          const acknowledged = await sendEach(producer, name.replace(/ /g, '-'), 36)

          expect(countsOf(acknowledged)).toEqual(
            Object.fromEntries(ALL_PARTITIONS.map((partition) => [partition, 6]))
          )
          writeEvidence(EVIDENCE, {
            wireVersions: wireVersions(),
            step: 'opt-out',
            control: name,
            placement,
            counts: countsOf(acknowledged),
          })
        }
      )
    },
    CASE_BUDGET_MS
  )

  test(
    'explicit and keyed records of a rack-aware producer keep their partitions',
    async () => {
      await withProducer(
        { rackId: OWN_RACK, rackAware: true },
        async (producer, placement, wireVersions) => {
          const explicit = await sendEach(producer, 'explicit', EXPLICIT.length, (index) => ({
            partition: EXPLICIT[index],
          }))
          const keyed = await sendEach(producer, 'keyed', KEYED.length, (index) => ({
            key: KEYED[index][0],
          }))

          expect(explicit.map(({ partition }) => partition)).toEqual(EXPLICIT)
          expect(keyed.map(({ partition }) => partition)).toEqual(
            KEYED.map(([, partition]) => partition)
          )
          const matching = ledIn(placement, OWN_RACK)
          expect(keyed.filter(({ partition }) => !matching.includes(partition))).not.toEqual([])
          writeEvidence(EVIDENCE, {
            wireVersions: wireVersions(),
            step: 'explicit-and-keyed',
            rackId: OWN_RACK,
            placement,
            explicit: explicit.map(({ partition }) => partition),
            keyed: Object.fromEntries(keyed.map(({ partition }, i) => [KEYED[i][0], partition])),
          })
        }
      )
    },
    CASE_BUDGET_MS
  )

  test(
    'a custom partitioner keeps its choice of a partition led outside the rack of a rack-aware producer',
    async () => {
      const seen = []
      const createPartitioner = () => (args) => {
        seen.push(Object.keys(args).sort())
        return 2
      }
      await withProducer(
        { createPartitioner, rackId: OWN_RACK, rackAware: true },
        async (producer, placement, wireVersions) => {
          expect(ledIn(placement, OWN_RACK)).not.toContain(2)

          const acknowledged = await sendEach(producer, 'custom', 6)

          expect(acknowledged.map(({ partition }) => partition)).toEqual([2, 2, 2, 2, 2, 2])
          expect(seen).toEqual(
            Array.from({ length: 6 }, () => ['message', 'partitionMetadata', 'topic'])
          )
          writeEvidence(EVIDENCE, {
            wireVersions: wireVersions(),
            step: 'custom-partitioner',
            rackId: OWN_RACK,
            placement,
            counts: countsOf(acknowledged),
          })
        }
      )
    },
    CASE_BUDGET_MS
  )
})
