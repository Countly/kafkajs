const { isDeepStrictEqual } = require('node:util')
const defaultSocketFactory = require('../../network/socketFactory')
const frameRequest = require('../../protocol/request')
const Broker = require('../../broker')
const { requests } = require('../../protocol/requests')
const apiKeys = require('../../protocol/requests/apiKeys')
const { Kafka, Partitioners, logLevel } = require('../../../index')
const { KafkaJSDeleteGroupsError } = require('../../errors')
const { NO_TOPIC_ID } = require('../../protocol/requests/topicIds')
const waitFor = require('../../utils/waitFor')
const FloorCapabilities = require('../../../testHelpers/floorCapabilities')
const apiVersionsV4 = require('../../../testHelpers/wire/apiVersionsV4')
const apiVersionsV5 = require('../../../testHelpers/wire/apiVersionsV5')
const { readLog } = require('../../../testHelpers/rawLog')
const { withDeadline } = require('../../consumer/__tests__/fixtures/boundedCleanup')
const {
  createCluster,
  createConnectionPool,
  createTopic,
  newLogger,
  plainTextBrokers,
  readRequestHeader,
  secureRandom,
  waitForTopicPropagation,
  writeEvidence,
  wireVersionsOf,
} = require('testHelpers')

// Gated: it creates and deletes topics and groups on a live cluster of three brokers in three
// racks. Run it alone through `npm run test:floor:conditional`; `KAFKA_VERSION` names the floor.
const describeConditional = process.env.FLOOR_CONDITIONAL_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'floor-conditional'
const HOOK_BUDGET_MS = 60000
const STEP_MS = 10000
const TEARDOWN_BUDGET_MS = 8 * STEP_MS
const API_VERSIONS_CASE_MS = 60000
const OFFSETS_CASE_MS = 120000
const DELETE_GROUPS_CASE_MS = 90000
const RACKS_CASE_MS = 150000
const POLL = { delay: 250, maxWait: 30000 }
/** Every client retries, because the cluster's own retrier looks up the coordinators a cold broker is still creating. */
const RETRY = { retries: 10, initialRetryTime: 300, maxRetryTime: 5000 }

const NONE = 0
const UUID = /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/
const UNSUPPORTED_VERSION = 35
const GROUP_ID_NOT_FOUND = 69
const RECORDS = 5
/** The APIs the file asks the floor's range oracle about. */
const NAMES = ['ApiVersions', 'TxnOffsetCommit', 'DeleteGroups', 'Produce', 'Metadata']
/** The last TxnOffsetCommit version a transaction under transaction version 1 may send: v5 carries the second part of KIP-890. */
const TXN_OFFSET_COMMIT_V1_MAX = 4
/** The last DeleteGroups version whose answers carry no per-group message. */
const DELETE_GROUPS_NO_MESSAGE_MAX = 2
/**
 * DescribeGroups answers a missing group with GROUP_ID_NOT_FOUND from this version, and with no
 * error before it.
 * @see https://github.com/apache/kafka/blob/4.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java
 */
const DESCRIBE_GROUPS_DEAD_CODE_FROM = 6
/** The APIs a producer that never sends a transaction or an offset may use. */
const PRODUCER_APIS = ['ApiVersions', 'Metadata', 'Produce']

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
const ALL_PARTITIONS = [0, 1, 2, 3, 4, 5]

/** @param {Error} error @returns {boolean} whether the topic is already gone */
const topicGone = (error) => error.type === 'UNKNOWN_TOPIC_OR_PARTITION'

/** @param {Error} error @returns {boolean} whether the error says only that every group asked for is already gone */
const groupGone = (error) =>
  error instanceof KafkaJSDeleteGroupsError &&
  error.groups.every(({ errorCode }) => errorCode === GROUP_ID_NOT_FOUND)

/** @param {{ host: string, port: number }} address @returns {string} `host:port` */
const endpointOf = ({ host, port }) => `${host}:${port}`

/**
 * Polls `check` until it returns something truthy.
 * @template T
 * @param {() => Promise<T|false>} check one look at the cluster
 * @param {string} what names the awaited state in the failure
 * @returns {Promise<T>} what `check` last returned
 * @throws {Error} a timeout naming `what`, or whatever `check` throws
 */
const until = (check, what) =>
  waitFor(check, { ...POLL, timeoutMessage: `Timed out waiting for ${what}` })

/** @returns {{ promise: Promise<void>, resolve: () => void }} a promise and the way to settle it */
const deferred = () => {
  let resolve
  const promise = new Promise((done) => {
    resolve = done
  })
  return { promise, resolve }
}

/**
 * The range facts of the floor broker, one per name: what it offers, what was recorded for the
 * floor, what the client implements, and the highest version the client may choose.
 * @returns {Promise<Record<string, { recorded: number[], offered: number[], implemented: number[], selected: number }>>}
 * @throws {Error} when the broker differs from the recorded probe, or a name has no overlap
 */
const rangesOfFloor = async () => {
  const broker = new Broker({ connectionPool: createConnectionPool(), logger: newLogger() })
  try {
    await broker.connect()
    return FloorCapabilities.fromBroker(broker, NAMES, process.env.KAFKA_VERSION)
  } finally {
    await broker.disconnect()
  }
}

/**
 * Cleanup steps that run newest first, each under its own deadline, so that what a step needs (a
 * connected admin, a closed group member) is still there when it runs, and a step that hangs fails
 * the case instead of passing it.
 */
class Teardown {
  #steps = []

  /**
   * Registers a step. Call it before the action it undoes.
   * @param {() => unknown} step the undo
   * @param {(error: Error) => boolean} [tolerates] true for a failure that means "already undone"
   * @returns {void}
   */
  add(step, tolerates = () => false) {
    this.#steps.push({ step, tolerates })
  }

  /**
   * Runs every registered step, newest first, past failures, and forgets them.
   * @returns {Promise<void>}
   * @throws {AggregateError} every failure that was not tolerated, once all steps ran
   */
  async run() {
    const failures = []
    for (const { step, tolerates } of this.#steps.splice(0).reverse()) {
      try {
        await withDeadline(Promise.resolve().then(step), STEP_MS)
      } catch (error) {
        if (!tolerates(error)) failures.push(error)
      }
    }
    if (failures.length > 0) {
      throw new AggregateError(failures, `${failures.length} cleanup step(s) failed`)
    }
  }
}

/** The ApiVersions request readers, by the version they read. */
const API_VERSIONS_READERS = { 4: apiVersionsV4, 5: apiVersionsV5 }

/**
 * One connection a client opened, as the bytes on its socket show it.
 * @typedef {object} TappedConnection
 * @property {string} endpoint `host:port` the socket was opened to
 * @property {{ correlationId: number, version: number, fields: object }[]} asked the ApiVersions
 *   requests written on it, in order, with the fields their bodies name
 * @property {Map<number, number>} answers the error code of the answer to each of them
 * @property {Buffer} inbound the received bytes not yet part of a whole frame
 */

/**
 * Observes the ApiVersions exchanges of the sockets a client opens: each request's version and the
 * fields its body names, and the error code of its own answer. It reads the socket's bytes and
 * never the client's own state, and passes every byte through untouched.
 */
class ApiVersionsTap {
  /** Starts with no connection; each socket the `socketFactory` makes is recorded from then on. */
  constructor() {
    /** @type {TappedConnection[]} in the order the sockets were opened */
    this.connections = []
    const inner = defaultSocketFactory()
    /** The socket factory the client under observation dials through. */
    this.socketFactory = (options) => {
      const socket = inner(options)
      const connection = this.open(endpointOf(options))
      const write = socket.write.bind(socket)
      socket.write = (data, ...rest) => {
        this.sent(connection, Buffer.isBuffer(data) ? data : Buffer.from(data, rest[0]))
        return write(data, ...rest)
      }
      socket.on('data', (chunk) => this.received(connection, chunk))
      return socket
    }
  }

  /**
   * @param {string} endpoint `host:port` of a socket just opened
   * @returns {TappedConnection} its record
   */
  open(endpoint) {
    const connection = { endpoint, asked: [], answers: new Map(), inbound: Buffer.alloc(0) }
    this.connections.push(connection)
    return connection
  }

  /**
   * Records a request frame written on a connection when it is an ApiVersions request.
   * @param {TappedConnection} connection
   * @param {Buffer} frame one whole request frame, length prefix included
   * @returns {void}
   * @throws {Error} for an ApiVersions version with no reader, or a body its reader refuses
   */
  sent(connection, frame) {
    const header = readRequestHeader(frame)
    if (header.apiKey !== apiKeys.ApiVersions) return
    const reader = API_VERSIONS_READERS[header.apiVersion]
    if (!reader) throw new Error(`No reader for an ApiVersions v${header.apiVersion} request`)
    connection.asked.push({
      correlationId: header.correlationId,
      version: header.apiVersion,
      fields: reader.readRequest(frame.subarray(header.byteLength)),
    })
  }

  /**
   * Takes received bytes, split anywhere, into frames and keeps the error code of each answer to
   * an ApiVersions request. Every ApiVersions answer starts with it, whatever its version: its
   * header has no tagged fields.
   * @param {TappedConnection} connection
   * @param {Buffer} chunk the next bytes the socket delivered
   * @returns {void}
   */
  received(connection, chunk) {
    connection.inbound = Buffer.concat([connection.inbound, chunk])
    while (connection.inbound.length >= 4) {
      const size = connection.inbound.readInt32BE(0)
      if (connection.inbound.length < 4 + size) return
      if (size >= 6) {
        const correlationId = connection.inbound.readInt32BE(4)
        if (connection.asked.some((request) => request.correlationId === correlationId)) {
          connection.answers.set(correlationId, connection.inbound.readInt16BE(8))
        }
      }
      connection.inbound = connection.inbound.subarray(4 + size)
    }
  }

  /**
   * @param {TappedConnection} connection
   * @returns {{ version: number, fields: object, errorCode: number|undefined }[]} its ApiVersions
   *   exchanges in order; `errorCode` is undefined for a request no answer was seen to
   */
  exchanges(connection) {
    return connection.asked.map(({ correlationId, version, fields }) => ({
      version,
      fields,
      errorCode: connection.answers.get(correlationId),
    }))
  }
}

/**
 * The three-rack topic of `docker-compose.racks.yml`: six two-replica partitions led two to a
 * rack. Its placement is read from the cluster's own Metadata before and after every arm, and its
 * acknowledged records are the ledger the topic's log is judged against.
 */
class RackTopic {
  /**
   * @param {string} topic the topic's name
   * @param {object} admin a connected admin, which creates the topic
   */
  constructor(topic, admin) {
    this.topic = topic
    this.admin = admin
    /** @type {object|null} reads Metadata for the placement; set by `create` */
    this.cluster = null
    /** @type {{ value: string, partition: number, offset: string }[]} every acknowledged record */
    this.ledger = []
  }

  /**
   * Creates the topic, waits until every broker serves it, and opens the cluster that reads its
   * placement.
   * @param {Teardown} teardown given the undo of each step before the step runs
   * @returns {Promise<void>}
   */
  async create(teardown) {
    teardown.add(() => this.admin.deleteTopics({ topics: [this.topic], timeout: 10000 }), topicGone)
    await this.admin.createTopics({
      waitForLeaders: true,
      timeout: 30000,
      topics: [
        {
          topic: this.topic,
          replicaAssignment: REPLICAS.map((replicas, partition) => ({ partition, replicas })),
        },
      ],
    })
    await waitForTopicPropagation(this.topic, REPLICAS.length)
    this.cluster = createCluster({}, plainTextBrokers())
    teardown.add(() => this.cluster.disconnect())
    await this.cluster.connect()
    await this.cluster.addTargetTopic(this.topic)
  }

  /**
   * @returns {Promise<{ racks: Record<number, string|null>, leaders: Record<number, number> }>}
   *   what the cluster's current Metadata says of the brokers' racks and the partition leaders
   */
  async placement() {
    await this.cluster.refreshMetadata()
    const { brokers } = this.cluster.brokerPool.metadata
    return {
      racks: Object.fromEntries(brokers.map(({ nodeId, rack }) => [nodeId, rack])),
      leaders: Object.fromEntries(
        this.cluster
          .findTopicPartitionMetadata(this.topic)
          .map(({ partitionId, leader }) => [partitionId, leader])
      ),
    }
  }

  /**
   * Waits for the placement the fixture asks for. No assertion is relaxed after a topology change:
   * a placement that never arrives fails the case.
   * @returns {Promise<{ racks: Record<number, string|null>, leaders: Record<number, number> }>}
   * @throws {Error} a timeout when the cluster never shows that placement
   */
  async verified() {
    const wanted = { racks: BROKER_RACKS, leaders: LEADERS }
    await until(
      async () => isDeepStrictEqual(await this.placement(), wanted),
      'the placement the rack fixture asks for'
    )
    return this.placement()
  }

  /**
   * @param {{ racks: Record<number, string|null>, leaders: Record<number, number> }} placement
   * @param {string} rack
   * @returns {number[]} the partitions whose leader the placement puts in `rack`, in order
   */
  ledIn({ racks, leaders }, rack) {
    return ALL_PARTITIONS.filter((partition) => racks[leaders[partition]] === rack)
  }

  /**
   * Sends records one at a time, so that each acknowledgement names its own partition and offset.
   * @param {object} producer
   * @param {string} name names the corpus, so that no value repeats across arms
   * @param {number} count
   * @param {string} runId
   * @returns {Promise<{ value: string, partition: number, offset: string }[]>} what was acknowledged
   */
  async sendEach(producer, name, count, runId) {
    const acknowledged = []
    for (let index = 0; index < count; index += 1) {
      const value = `${runId}-${name}-${index}`
      const result = await producer.send({ topic: this.topic, messages: [{ value }] })
      expect(result).toHaveLength(1)
      acknowledged.push({ value, partition: result[0].partition, offset: result[0].baseOffset })
    }
    this.ledger.push(...acknowledged)
    return acknowledged
  }

  /**
   * Reads the log straight from the partition leaders and requires it to hold exactly what was
   * acknowledged, each record once, at the partition and offset it was acknowledged at.
   * @returns {Promise<void>}
   */
  async expectLogToEqualLedger() {
    const log = await readLog({ brokers: plainTextBrokers(), topic: this.topic })
    const physical = [...log].flatMap(([partition, { records }]) =>
      records.map(({ value, offset }) => ({ value: value.toString(), partition, offset }))
    )
    const byValue = (a, b) => (a.value < b.value ? -1 : 1)
    expect(physical.sort(byValue)).toEqual([...this.ledger].sort(byValue))
  }
}

/**
 * @param {{ partition: number }[]} records acknowledged records
 * @returns {Record<number, number>} how many of them each partition was acknowledged for
 */
const countsOf = (records) =>
  records.reduce((counts, { partition }) => {
    counts[partition] = (counts[partition] ?? 0) + 1
    return counts
  }, {})

/**
 * The 4.0 floor against the features Kafka added after it, run with `npm run test:floor:conditional`.
 * On the floor broker each is absent, so each case proves that the client chooses the old version by
 * the range the broker offers and that the public operation still works at it.
 *
 * - ApiVersions v5 (KIP-1242): the client asks v5, the broker refuses it, the client asks again at
 *   the highest version the range holds, and that request names no cluster and no node.
 * - TxnOffsetCommit v6 (KIP-1319): the client knows the topic's id and implements v6, and sends the
 *   highest version of the offered range; the group's offset is published only by the commit.
 * - DeleteGroups v3: the answer keeps the old shape, with no `errorMessage`, and the group and its
 *   commit are really gone.
 * - Rack-aware partitioning (KIP-1123): it is client-side and needs only Produce and Metadata, so
 *   it works unchanged: in-rack leaders first, every partition when no leader is in the rack, and a
 *   custom partitioner keeps its own choice.
 *
 * Excluded by capability, never by a version string: the expected versions come from the range
 * oracle, which compares the broker's offered ranges with the recorded probe and the codecs the
 * client implements, and the transaction version is read from the broker's finalized features.
 * The file runs on the racks cluster, whose brokers differ from the base cluster's only by their
 * racks, a replica selector and a JMX port, none of which the first three cases touch.
 *
 * Replay sets: durable log, the rack case's acknowledged records exactly once; committed offsets,
 * the one the transaction commits and the one the group's consumer commits; handler delivery, none
 * is judged.
 */
describeConditional('Conditional features on the negotiated floor', () => {
  const runId = secureRandom()
  const kafka = new Kafka({
    clientId: 'floor-conditional-live',
    brokers: plainTextBrokers(),
    logLevel: logLevel.NOTHING,
    retry: RETRY,
  })
  const admin = kafka.admin()
  const suite = new Teardown()
  /** @type {{ apiName: string, apiVersion: number }[]} what the shared admin sent */
  const adminRequests = []
  /** @type {Teardown} the running case's cleanup */
  let cleanup
  let ranges

  /**
   * @param {string} groupId
   * @param {string} topic
   * @returns {Promise<string>} the group's committed next offset on partition 0, as OffsetFetch says
   */
  const committedOffset = async (groupId, topic) => {
    const [{ partitions }] = await admin.fetchOffsets({ groupId, topics: [topic] })
    return partitions.find(({ partition }) => partition === 0).offset
  }

  /** @returns {Promise<string>} the group's committed offset once it equals `expected` */
  const committedOffsetReaching = (groupId, topic, expected) =>
    until(async () => {
      const offset = await committedOffset(groupId, topic)
      return offset === String(expected) && offset
    }, `the committed offset of ${groupId} to reach ${expected}`)

  /**
   * @param {string} groupId
   * @param {(row: object) => boolean} holds
   * @returns {Promise<object>} the group's DescribeGroups row once `holds` accepts it
   */
  const describedWhen = (groupId, holds) =>
    until(async () => {
      const [row] = (await admin.describeGroups([groupId])).groups
      return holds(row) && row
    }, `the description of ${groupId}`)

  /** @returns {Promise<string[]>} the ids ListGroups names */
  const listedGroups = async () => (await admin.listGroups()).groups.map(({ groupId }) => groupId)

  /** @returns {number[]} the DeleteGroups versions the shared admin sent, in order */
  const deleteGroupsVersions = () =>
    adminRequests
      .filter(({ apiName }) => apiName === 'DeleteGroups')
      .map(({ apiVersion }) => apiVersion)

  /**
   * Creates a topic of one partition that cleanup deletes, and fills it when asked to.
   * @param {string} name the topic's name
   * @param {number} [records] how many records to write to partition 0
   * @returns {Promise<string>} the topic
   */
  const freshTopic = async (name, records = 0) => {
    const topic = `e2e-floor-conditional-${runId}-${name}`
    cleanup.add(() => admin.deleteTopics({ topics: [topic] }), topicGone)
    await createTopic({ topic, partitions: 1 })
    if (records > 0) {
      const producer = kafka.producer()
      cleanup.add(() => producer.disconnect())
      await producer.connect()
      await producer.send({
        topic,
        messages: Array.from({ length: records }, (_, index) => ({
          partition: 0,
          value: `r-${index}`,
        })),
      })
    }
    return topic
  }

  beforeAll(async () => {
    admin.on(admin.events.REQUEST, ({ payload }) =>
      adminRequests.push({ apiName: payload.apiName, apiVersion: payload.apiVersion })
    )
    suite.add(() => admin.disconnect())
    await admin.connect()
    ranges = await rangesOfFloor()
  }, HOOK_BUDGET_MS)

  beforeEach(() => {
    cleanup = new Teardown()
  })

  afterEach(() => cleanup.run(), TEARDOWN_BUDGET_MS)

  afterAll(() => suite.run(), HOOK_BUDGET_MS)

  test(
    'ApiVersions retains the old identity-free path on the floor',
    async () => {
      const { ApiVersions: range } = ranges
      const highest = Math.max(...range.implemented)
      // The client implements a version the floor's range does not hold, so the refusal is the range's doing.
      expect(highest).toBeGreaterThan(range.offered[1])
      const tap = new ApiVersionsTap()
      const client = new Kafka({
        clientId: 'floor-conditional-api-versions',
        brokers: plainTextBrokers(),
        logLevel: logLevel.NOTHING,
        retry: RETRY,
        socketFactory: tap.socketFactory,
      })
      const probe = client.admin()
      cleanup.add(() => probe.disconnect())

      await probe.connect()
      const { clusterId, brokers } = await probe.describeCluster()
      const seed = tap.connections[0].endpoint
      const target = brokers.find((broker) => endpointOf(broker) !== seed)
      const features = await probe.describeFeatures({ brokerId: target.nodeId })

      expect(new Set(brokers.map(endpointOf))).toEqual(new Set(plainTextBrokers()))
      const { minVersionLevel, maxVersionLevel } = features.finalizedFeatures['metadata.version']
      expect(minVersionLevel).toBeGreaterThan(0)
      expect(maxVersionLevel).toBeGreaterThanOrEqual(minVersionLevel)
      // Every connection asks the highest version, is refused, and asks again at the range's version.
      const asked = tap.connections.filter(({ asked: exchanged }) => exchanged.length > 0)
      expect(asked.length).toBeGreaterThanOrEqual(2)
      const refusedThenAccepted = [
        [highest, UNSUPPORTED_VERSION],
        [range.selected, NONE],
      ]
      for (const connection of asked) {
        const exchanges = tap.exchanges(connection)
        expect(exchanges.length % 2).toBe(0)
        expect(exchanges.map(({ version, errorCode }) => [version, errorCode])).toEqual(
          Array.from({ length: exchanges.length / 2 }, () => refusedThenAccepted).flat()
        )
        for (const { version, fields } of exchanges) {
          if (version === range.selected) {
            expect(fields).toEqual({
              clientSoftwareName: expect.stringMatching(/\S/),
              clientSoftwareVersion: expect.stringMatching(/\S/),
            })
          }
        }
      }
      // The seed knew nothing yet, so its refused request named the defaults.
      expect(tap.exchanges(tap.connections[0])[0].fields).toMatchObject({
        clusterId: null,
        nodeId: -1,
      })
      // A known node's refused request did name its cluster and node, so the retry's lack of them is no accident.
      const known = tap.connections.filter(({ endpoint }) => endpoint === endpointOf(target))
      expect(known.length).toBeGreaterThanOrEqual(1)
      for (const connection of known) {
        for (const { version, fields } of tap.exchanges(connection)) {
          if (version === highest) {
            expect(fields).toMatchObject({ clusterId, nodeId: target.nodeId })
          }
        }
      }
      writeEvidence(EVIDENCE, {
        case: 'F20',
        ranges: { ApiVersions: range },
        wireVersions: { ApiVersions: range.selected },
        asked: [highest, range.selected],
        connections: asked.length,
      })
    },
    API_VERSIONS_CASE_MS
  )

  test(
    'Transactional offset commits retain the named-topic path on the floor',
    async () => {
      const { TxnOffsetCommit: range } = ranges
      const highest = Math.max(...range.implemented)
      expect(highest).toBeGreaterThan(range.offered[1])
      const topic = await freshTopic('offsets')
      const groupId = `e2e-floor-conditional-${runId}-offsets-group`
      const producer = kafka.producer({
        transactionalId: `e2e-floor-conditional-${runId}-offsets`,
        maxInFlightRequests: 1,
      })
      const sent = []
      const calls = []
      const gate = { hold: null, reached: false }
      producer.on(producer.events.REQUEST, ({ payload }) =>
        sent.push({ apiName: payload.apiName, apiVersion: payload.apiVersion })
      )
      cleanup.add(() => admin.deleteGroups([groupId]), groupGone)
      cleanup.add(() => producer.disconnect())
      cleanup.add(() => jest.restoreAllMocks())
      cleanup.add(() => gate.hold?.resolve())
      const { txnOffsetCommit, endTxn } = Broker.prototype
      jest.spyOn(Broker.prototype, 'txnOffsetCommit').mockImplementation(function (request) {
        calls.push({
          advertisedMax: this.versions?.[apiKeys.TxnOffsetCommit]?.maxVersion ?? null,
          transactionVersion: request.transactionVersion,
          topicIds: request.topicIds ? [...request.topicIds] : null,
        })
        return txnOffsetCommit.call(this, request)
      })
      jest.spyOn(Broker.prototype, 'endTxn').mockImplementation(async function (request) {
        if (gate.hold) {
          gate.reached = true
          await gate.hold.promise
        }
        return endTxn.call(this, request)
      })
      const { finalizedFeatures, finalizedFeaturesEpoch } = await admin.describeFeatures()
      const transactionVersion =
        finalizedFeaturesEpoch !== null &&
        (finalizedFeatures['transaction.version']?.maxVersionLevel ?? 0) >= 2
          ? 2
          : 1
      const expected =
        transactionVersion === 2
          ? range.selected
          : Math.min(range.selected, TXN_OFFSET_COMMIT_V1_MAX)

      await producer.connect()
      const baseline = await committedOffset(groupId, topic)
      const transaction = await producer.transaction()
      await transaction.send({
        topic,
        messages: Array.from({ length: RECORDS }, (_, index) => ({
          partition: 0,
          value: `commit-${index}`,
        })),
      })
      await transaction.sendOffsets({
        consumerGroupId: groupId,
        topics: [{ topic, partitions: [{ partition: 0, offset: String(RECORDS) }] }],
      })
      const staged = await committedOffset(groupId, topic)
      gate.hold = deferred()
      let settled = false
      const outcome = transaction.commit().then(
        () => {
          settled = true
          return null
        },
        (error) => {
          settled = true
          return error
        }
      )
      await until(async () => gate.reached, 'the commit to reach EndTxn')
      expect(settled).toBe(false)
      const held = await committedOffset(groupId, topic)
      gate.hold.resolve()
      expect(await outcome).toBeNull()
      const published = await committedOffsetReaching(groupId, topic, RECORDS)

      const wire = sent.filter(({ apiName }) => apiName === 'TxnOffsetCommit')
      expect(baseline).toBe('-1')
      expect(staged).toBe(baseline)
      expect(held).toBe(baseline)
      expect(published).toBe(String(RECORDS))
      expect(calls.length).toBeGreaterThanOrEqual(1)
      for (const call of calls) {
        expect(call.advertisedMax).toBe(range.offered[1])
        expect(call.transactionVersion).toBe(transactionVersion)
        // The producer knew the topic's id, so the id was never the reason for the lower version.
        expect(call.topicIds).toEqual([[topic, expect.stringMatching(UUID)]])
        expect(call.topicIds[0][1]).not.toBe(NO_TOPIC_ID)
      }
      const versions = wire.map(({ apiVersion }) => apiVersion)
      expect(versions.length).toBeGreaterThanOrEqual(1)
      expect(versions).not.toContain(highest)
      expect(new Set(versions)).toEqual(new Set([expected]))
      writeEvidence(EVIDENCE, {
        case: 'F21',
        ranges: { TxnOffsetCommit: range },
        wireVersions: wireVersionsOf(sent),
        transactionVersion,
        expectedVersion: expected,
        rawNextOffsets: { baseline, staged, held, published },
      })
    },
    OFFSETS_CASE_MS
  )

  test(
    'DeleteGroups retains the old wire contract on the floor',
    async () => {
      const { DeleteGroups: range } = ranges
      expect(Math.max(...range.implemented)).toBeGreaterThan(range.offered[1])
      expect(range.selected).toBeLessThanOrEqual(DELETE_GROUPS_NO_MESSAGE_MAX)
      const topic = await freshTopic('delete-groups', RECORDS)
      const groupId = `e2e-floor-conditional-${runId}-delete-groups-group`
      const consumer = kafka.consumer({ groupId, maxWaitTimeInMs: 500 })
      const read = []
      let joined = false
      cleanup.add(() => admin.deleteGroups([groupId]), groupGone)
      cleanup.add(() => consumer.disconnect())
      consumer.on(consumer.events.GROUP_JOIN, () => {
        joined = true
      })
      await consumer.connect()
      await consumer.subscribe({ topic, fromBeginning: true })
      await consumer.run({
        autoCommit: false,
        eachMessage: async ({ message }) => read.push(message.offset),
      })
      await until(async () => joined && read.length >= RECORDS, 'the member to join and read')
      await consumer.commitOffsets([{ topic, partition: 0, offset: String(RECORDS) }])
      expect(await committedOffset(groupId, topic)).toBe(String(RECORDS))
      await consumer.disconnect()
      await describedWhen(groupId, ({ state }) => state === 'Empty')
      await until(async () => (await listedGroups()).includes(groupId), 'the group to be listed')
      const before = deleteGroupsVersions().length

      const results = await admin.deleteGroups([groupId])

      expect(results).toEqual([{ groupId, errorCode: NONE }])
      expect(results[0]).not.toHaveProperty('errorMessage')
      const wire = deleteGroupsVersions().slice(before)
      expect(wire.length).toBeGreaterThanOrEqual(1)
      expect(new Set(wire)).toEqual(new Set([range.selected]))
      const dead = await describedWhen(groupId, ({ state }) => state === 'Dead')
      const describeVersion = adminRequests
        .filter(({ apiName }) => apiName === 'DescribeGroups')
        .at(-1).apiVersion
      expect(dead.errorCode).toBe(
        describeVersion >= DESCRIBE_GROUPS_DEAD_CODE_FROM ? GROUP_ID_NOT_FOUND : NONE
      )
      expect(await committedOffset(groupId, topic)).toBe('-1')
      await until(async () => !(await listedGroups()).includes(groupId), 'the group to vanish')
      writeEvidence(EVIDENCE, {
        case: 'F22',
        ranges: { DeleteGroups: range },
        wireVersions: { DeleteGroups: range.selected },
        result: results[0],
      })
    },
    DELETE_GROUPS_CASE_MS
  )

  test(
    'Built-in rack selection and custom authority work on the floor',
    async () => {
      const fixture = new RackTopic(`e2e-floor-conditional-${runId}-racks-topic`, admin)
      const sent = []
      cleanup.add(() => jest.restoreAllMocks())
      await fixture.create(cleanup)

      /**
       * Runs one arm with a producer that is always disconnected afterwards, between two reads of
       * the placement that must agree.
       * @param {object} options the producer's options
       * @param {(producer: object, placement: object) => Promise<object[]>} work the arm's sends
       * @returns {Promise<object[]>} what the arm's sends were acknowledged as
       */
      const arm = async (options, work) => {
        const before = await fixture.verified()
        const producer = kafka.producer({ linger: 0, ...options })
        producer.on(producer.events.REQUEST, ({ payload }) =>
          sent.push({ apiName: payload.apiName, apiVersion: payload.apiVersion })
        )
        let acknowledged
        try {
          await producer.connect()
          acknowledged = await work(producer, before)
        } finally {
          await withDeadline(producer.disconnect(), STEP_MS)
        }
        expect(await fixture.placement()).toEqual(before)
        return acknowledged
      }

      const inRack = await arm(
        { rackId: OWN_RACK, rackAware: true },
        async (producer, placement) => {
          expect(fixture.ledIn(placement, OWN_RACK)).toEqual([1, 4])
          // A draw of 0 makes a pick that ignores the rack choose partition 0, which is led in rack-a.
          const draw = jest.spyOn(Math, 'random').mockReturnValue(0)
          try {
            return await fixture.sendEach(producer, 'in-rack', 12, runId)
          } finally {
            draw.mockRestore()
          }
        }
      )
      const fallback = await arm(
        {
          createPartitioner: Partitioners.DefaultPartitioner,
          rackId: 'rack-z',
          rackAware: true,
        },
        async (producer, placement) => {
          expect(fixture.ledIn(placement, 'rack-z')).toEqual([])
          return fixture.sendEach(producer, 'fallback', 36, runId)
        }
      )
      const seen = []
      const custom = await arm(
        {
          createPartitioner: () => (args) => {
            seen.push(Object.keys(args).sort())
            return 2
          },
          rackId: OWN_RACK,
          rackAware: true,
        },
        async (producer, placement) => {
          expect(fixture.ledIn(placement, OWN_RACK)).not.toContain(2)
          return fixture.sendEach(producer, 'custom', 6, runId)
        }
      )

      expect(inRack.map(({ partition }) => partition)).toEqual(Array(12).fill(1))
      expect(countsOf(fallback)).toEqual(
        Object.fromEntries(ALL_PARTITIONS.map((partition) => [partition, 6]))
      )
      expect(custom.map(({ partition }) => partition)).toEqual(Array(6).fill(2))
      expect(seen).toEqual(
        Array.from({ length: 6 }, () => ['message', 'partitionMetadata', 'topic'])
      )
      await fixture.expectLogToEqualLedger()
      const wire = wireVersionsOf(sent)
      expect(Object.keys(wire)).toEqual(expect.arrayContaining(['Produce', 'Metadata']))
      expect(Object.keys(wire).filter((name) => !PRODUCER_APIS.includes(name))).toEqual([])
      for (const name of ['Produce', 'Metadata']) {
        expect(wire[name]).toBeGreaterThanOrEqual(ranges[name].offered[0])
        expect(wire[name]).toBeLessThanOrEqual(ranges[name].offered[1])
      }
      writeEvidence(EVIDENCE, {
        case: 'F23',
        ranges: { Produce: ranges.Produce, Metadata: ranges.Metadata },
        wireVersions: wire,
        placement: await fixture.placement(),
        counts: {
          inRack: countsOf(inRack),
          fallback: countsOf(fallback),
          custom: countsOf(custom),
        },
      })
    },
    RACKS_CASE_MS
  )
})

describe('Floor conditional expectations', () => {
  test('reads what each ApiVersions request names and the error code of its own answer', async () => {
    const tap = new ApiVersionsTap()
    const connection = tap.open('node:9092')
    /** @returns {Promise<Buffer>} the frame the client writes for an ApiVersions request of `version` */
    const frameOf = async (correlationId, version, identity) => {
      const { request } = requests.ApiVersions.protocol({ version })(identity)
      return (await frameRequest({ correlationId, clientId: 'tap', request })).buffer
    }
    /** @returns {Buffer} an answer frame: size, correlation id, error code, then `padding` bytes */
    const answerOf = (correlationId, errorCode, padding = 0) => {
      const bytes = Buffer.alloc(10 + padding)
      bytes.writeInt32BE(6 + padding, 0)
      bytes.writeInt32BE(correlationId, 4)
      bytes.writeInt16BE(errorCode, 8)
      return bytes
    }

    tap.sent(connection, await frameOf(1, 5, { clusterId: 'cluster', nodeId: 2 }))
    tap.sent(connection, await frameOf(2, 4, { clusterId: 'cluster', nodeId: 2 }))
    // An answer to a request that was never asked, and chunks that cut frames in the middle.
    const stream = Buffer.concat([
      answerOf(1, UNSUPPORTED_VERSION, 10),
      answerOf(3, 99),
      answerOf(2, NONE),
    ])
    for (const [from, to] of [
      [0, 7],
      [7, 15],
      [15, stream.length],
    ]) {
      tap.received(connection, stream.subarray(from, to))
    }

    expect(tap.exchanges(connection)).toEqual([
      {
        version: 5,
        errorCode: UNSUPPORTED_VERSION,
        fields: {
          clientSoftwareName: expect.any(String),
          clientSoftwareVersion: expect.any(String),
          clusterId: 'cluster',
          nodeId: 2,
        },
      },
      {
        version: 4,
        errorCode: NONE,
        fields: {
          clientSoftwareName: expect.any(String),
          clientSoftwareVersion: expect.any(String),
        },
      },
    ])
    expect(connection.answers.size).toBe(2)
    expect(connection.inbound).toHaveLength(0)
  })

  test('runs cleanup newest first, continues past a failure and reports only what it could not undo', async () => {
    const teardown = new Teardown()
    const order = []
    teardown.add(() => order.push('registered first'))
    teardown.add(() => {
      order.push('fails')
      throw new Error('could not undo')
    })
    teardown.add(
      () => {
        order.push('already undone')
        throw Object.assign(new Error('gone'), { type: 'GONE' })
      },
      ({ type }) => type === 'GONE'
    )

    const failure = await teardown.run().catch((error) => error)

    expect(order).toEqual(['already undone', 'fails', 'registered first'])
    expect(failure).toBeInstanceOf(AggregateError)
    expect(failure.errors.map(({ message }) => message)).toEqual(['could not undo'])
    await expect(teardown.run()).resolves.toBeUndefined()
  })
})
