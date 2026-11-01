const { Schedule } = require('./schedule')
const { createFakeSocketFactory, respondWithFrame, readRequestHeader } = require('./fakeSocket')
const { responseHeaderVersion } = require('../src/protocol/headers')
const apiKeys = require('../src/protocol/requests/apiKeys')
const { logLevel } = require('../index')
const { TOPIC, TOPIC_ID } = require('../src/consumer/__tests__/fixtures/fetchHarness')
const { MemberAssignment } = require('../src/consumer/assignerProtocol')

/**
 * The pairs the bootstrap and the classic group protocol use: the one version of each that has a
 * wire pair, advertised with `min = max` so the client's version selection lands on it.
 * @type {Array<[string, number]>}
 */
const CLASSIC = [
  ['ApiVersions', 4],
  ['Metadata', 13],
  ['FindCoordinator', 6],
  ['ListOffsets', 9],
  ['Fetch', 18],
  ['OffsetForLeaderEpoch', 4],
  ['JoinGroup', 8],
  ['SyncGroup', 5],
  ['Heartbeat', 4],
  ['LeaveGroup', 5],
  ['OffsetFetch', 9],
  ['OffsetCommit', 9],
]

/** The pairs an idempotent or non-transactional producer adds to the classic base. */
const PRODUCER = [
  ['InitProducerId', 5],
  ['Produce', 13],
]

const DEFAULT = [...CLASSIC, ...PRODUCER]

/**
 * What each scenario profile advertises. A pair whose wire file does not exist yet is still
 * advertised, and the request for it is refused by name. The two transaction profiles carry the
 * classic base beside their transaction pairs, and each advertises exactly the versions the
 * client's own caps allow under that transaction version: a transactional Produce, EndTxn and
 * TxnOffsetCommit at v11, v4 and v4 under transaction version 1, and at v12, v5 and v5 under 2,
 * which also sends no AddPartitionsToTxn and no AddOffsetsToTxn.
 * @type {Record<'default'|'sasl'|'consumer'|'tv1'|'tv2'|'transactions', Array<[string, number]>>}
 */
const PROFILES = {
  transactions: [
    ...DEFAULT,
    ['DescribeProducers', 0],
    ['DescribeTransactions', 0],
    ['ListTransactions', 2],
    ['WriteTxnMarkers', 2],
  ],
  default: DEFAULT,
  sasl: [...DEFAULT, ['SaslHandshake', 1], ['SaslAuthenticate', 2]],
  consumer: [
    ...DEFAULT.filter(([api]) => api !== 'OffsetFetch' && api !== 'OffsetCommit'),
    ['OffsetFetch', 10],
    ['OffsetCommit', 10],
    ['ConsumerGroupHeartbeat', 1],
  ],
  tv1: [
    ...CLASSIC,
    ['InitProducerId', 5],
    ['AddPartitionsToTxn', 3],
    ['AddOffsetsToTxn', 4],
    ['Produce', 11],
    ['TxnOffsetCommit', 4],
    ['EndTxn', 4],
  ],
  tv2: [...CLASSIC, ['InitProducerId', 5], ['Produce', 12], ['TxnOffsetCommit', 5], ['EndTxn', 5]],
}

/**
 * The finalized features a transaction coordinator on transaction version 2 reports: it finalizes
 * `transaction.version` at level 2, which a broker writes with equal minimum and maximum, beside
 * the `metadata.version` every answer carries.
 */
const TRANSACTION_V2_FEATURES = [
  { name: 'metadata.version', minVersionLevel: 1, maxVersionLevel: 25 },
  { name: 'transaction.version', minVersionLevel: 2, maxVersionLevel: 2 },
]

/**
 * One request the client wrote, as `requests()` logs it and a `Schedule` sees it.
 * @typedef {object} RequestRecord
 * @property {number} node the id of the node it was written to
 * @property {string} api the API name, e.g. `JoinGroup`
 * @property {number} apiKey
 * @property {number} version
 * @property {number} correlationId
 * @property {number} generation which connection of the node it came in on, counting from 1
 * @property {object|null} fields what the wire pair read from it; null when it could not be read
 * @property {number} order where it was written among the requests, answers and marks of the cluster
 * @property {number} [answeredOrder] where its answer was written, once it was
 */

/**
 * A module of `testHelpers/wire`: the response one version of an API gets, and the request it
 * reads.
 * @typedef {object} WirePair
 * @property {string} api
 * @property {number} version
 * @property {(fields?: object) => Buffer} buildResponse the response body, the given fields over
 *   the pair's defaults
 * @property {(body: Buffer) => object} readRequest the fields of a request body, after its header
 */

/**
 * The name the protocol gives an api key: this client's name for key 10 is `GroupCoordinator`.
 * @param {number} apiKey
 * @returns {string} the name
 */
const apiNameOf = (apiKey) => {
  const name = Object.keys(apiKeys).find((candidate) => apiKeys[candidate] === apiKey)
  return name === 'GroupCoordinator' ? 'FindCoordinator' : name
}

const apiKeyOf = (api) => apiKeys[api === 'FindCoordinator' ? 'GroupCoordinator' : api]

/**
 * @param {string} api the API name
 * @param {number} version
 * @returns {WirePair|null} the wire pair of the API at the version, or null while it has none
 * @throws {Error} when the pair's module exists and fails to load
 */
const pairOf = (api, version) => {
  try {
    return require(`./wire/${api[0].toLowerCase()}${api.slice(1)}V${version}`)
  } catch (error) {
    if (error.code === 'MODULE_NOT_FOUND') {
      return null
    }
    throw error
  }
}

/**
 * @template T
 * @param {string} what names what is read, for the message
 * @param {() => T} read
 * @returns {T} what `read` returns
 * @throws {Error} naming `what`, with the error of `read` as its cause
 */
const reading = (what, read) => {
  try {
    return read()
  } catch (error) {
    throw new Error(`ScriptedCluster: cannot read ${what}: ${error.message}`, {
      cause: error,
    })
  }
}

/**
 * @param {Buffer} frame one request, size prefix included
 * @returns {ReturnType<typeof readRequestHeader>} the header of the request
 * @throws {Error} when the size prefix is not the length of what follows it, or the header is cut
 *   short
 */
const headerOf = (frame) =>
  reading('the request frame', () => {
    if (frame.readInt32BE(0) !== frame.length - 4) {
      throw new Error(
        `its size prefix says ${frame.readInt32BE(0)} bytes, ${frame.length - 4} follow`
      )
    }

    return readRequestHeader(frame)
  })

/**
 * The topic and partition a selector can name, so it can say `{ topic: 't', partition: 0 }`. The
 * topic is promoted when the request names exactly one topic by name; the partition only when that
 * topic names exactly one partition. Any other request leaves them out: use `match`. A request
 * that names topics by id only (Fetch v18) is not promoted to the topic's name.
 * @param {object} fields what a pair's `readRequest` returned
 * @returns {{ topic?: string, partition?: number }}
 */
const targetOf = (fields) => {
  const topics = fields.topics?.filter((entry) => entry.topic ?? entry.name)
  if (topics?.length !== 1) {
    return {}
  }

  const [entry] = topics
  const topic = entry.topic ?? entry.name
  const partitions = entry.partitions ?? []
  return partitions.length === 1
    ? {
        topic,
        partition: partitions[0].partition ?? partitions[0].partitionIndex,
      }
    : { topic }
}

/**
 * A cluster that exists only as sockets: the real client connects to it through its public
 * `socketFactory` option, and every request it writes is answered in process.
 *
 * It owns the node endpoints, the ApiVersions table of its profile, routing of each request through
 * a `Schedule` (so a test can hold, answer or fail any of them), and the log of requests. It owns
 * no log, group, transaction, leadership, quota or ACL state: what an answer holds is whatever the
 * wire pair's vector holds, or what the test scripts through `answer` or a `Schedule`. A request is
 * refused by name, and fails the test, when it is not one whole frame, names a version the cluster
 * does not advertise or one that has no wire pair, or has a body its pair cannot read to the end;
 * it never hangs.
 *
 * A held call that is failed (`gate.fail(error)`) destroys the socket it came in on with that
 * error; a call released with an object answers with that object's fields over the defaults, one
 * released with a Buffer answers with those bytes as the response body, and one released with no
 * value answers as it would have had nothing held it.
 */
class ScriptedCluster {
  /**
   * @param {object} options
   * @param {Array<{ id: number, host: string, port: number }>} options.nodes the endpoints the
   *   client may dial; a dial to any other endpoint fails
   * @param {Schedule} [options.schedule] routes the calls; one is made when omitted
   * @param {'default'|'sasl'|'consumer'|'tv1'|'tv2'|'transactions'} [options.profile='default'] what the cluster advertises
   * @param {Record<string, [number, number]>} [options.advertise] `[version, version]` per API
   *   name, over the profile's: how a test makes the client negotiate another single version, the
   *   only one the cluster then accepts; a non-singleton range is rejected
   */
  constructor({ nodes, schedule = new Schedule(), profile = 'default', advertise = {} }) {
    if (!PROFILES[profile]) {
      throw new Error(`ScriptedCluster: no profile named ${profile}`)
    }

    this.nodes = nodes
    this.schedule = schedule
    this.profile = profile
    this.fake = createFakeSocketFactory()
    this.log = []
    this.marks = []
    this.clock = 0
    this.errors = []
    this.answerers = new Map()
    this.downNodes = new Set()
    this.blackholedNodes = new Set()
    this.advertised = new Map(PROFILES[profile].map(([api, version]) => [api, [version, version]]))
    for (const [api, range] of Object.entries(advertise)) {
      if (
        !Array.isArray(range) ||
        range.length !== 2 ||
        !Number.isInteger(range[0]) ||
        range[0] < 0 ||
        range[0] !== range[1]
      ) {
        throw new Error(`ScriptedCluster: advertise ${api} must select one version`)
      }
      this.advertised.set(api, [...range])
    }

    this.socketFactory = ({ host, port, onConnect }) => this.connect(host, port, onConnect)
  }

  /**
   * Sets what an API answers when nothing in the schedule picks the call, in place of the pair's
   * defaults.
   * @param {string} api the API name, e.g. `JoinGroup`
   * @param {(fields: object, record: object) => object} answerer called with the request's fields
   *   and its record; returns the response fields to lay over the pair's defaults
   * @returns {ScriptedCluster} this
   */
  answer(api, answerer) {
    this.answerers.set(api, answerer)
    return this
  }

  /**
   * Takes a node down: every dial to it is refused (`ECONNREFUSED`) until `up`. Connections it
   * already holds are left as they are.
   * @param {number} id the node's id
   * @returns {ScriptedCluster} this
   */
  down(id) {
    this.blackholedNodes.delete(id)
    this.downNodes.add(id)
    return this
  }

  /**
   * @param {number} id the id of a node taken down with `down`
   * @returns {ScriptedCluster} this
   */
  /** Holds future dials without connecting; up restores them, existing sockets stay unchanged.
   * @param {number} id node id
   * @returns {ScriptedCluster} this
   */
  blackhole(id) {
    this.downNodes.delete(id)
    this.blackholedNodes.add(id)
    return this
  }

  /** Restores future dials; a held old dial remains held. @param {number} id node id @returns {ScriptedCluster} this */
  up(id) {
    this.downNodes.delete(id)
    this.blackholedNodes.delete(id)
    return this
  }

  /**
   * @returns {Array<{ apiKey: number, minVersion: number, maxVersion: number }>} the table the
   *   cluster answers ApiVersions with: its profile's, over any `advertise` override
   */
  apiVersionsTable() {
    return [...this.advertised].map(([api, [minVersion, maxVersion]]) => ({
      apiKey: apiKeyOf(api),
      minVersion,
      maxVersion,
    }))
  }

  /**
   * @returns {{ apiKeys: Array<{ apiKey: number, minVersion: number, maxVersion: number }>,
   *   finalizedFeatures?: Array<{ name: string, minVersionLevel: number, maxVersionLevel: number }> }}
   *   the fields the ApiVersions answer is built from: the table, and under `tv2` the finalized
   *   `transaction.version` at level 2, which is what makes a client choose transaction version 2
   */
  apiVersionsAnswer() {
    return this.profile === 'tv2'
      ? {
          apiKeys: this.apiVersionsTable(),
          finalizedFeatures: TRANSACTION_V2_FEATURES,
        }
      : { apiKeys: this.apiVersionsTable() }
  }

  /** @returns {import('events').EventEmitter[]} every socket the client opened, oldest first */
  sockets() {
    return [...this.fake.state.sockets]
  }

  /** @returns {RequestRecord[]} every request the client wrote, oldest first */
  requests() {
    return [...this.log]
  }

  /**
   * Notes that something outside the wire happened now, so a test can place it among the requests
   * and answers (the shared offset cache being written, for one).
   * @param {string} label
   * @returns {void}
   */
  mark(label) {
    this.marks.push({ order: this.clock++, label })
  }

  /**
   * Every request written, every answer given and every mark, in the order they happened.
   * @returns {Array<{ order: number, kind: 'request'|'response'|'mark', api?: string, version?:
   *   number, node?: number, correlationId?: number, generation?: number, label?: string }>}
   */
  timeline() {
    const events = this.marks.map(({ order, label }) => ({
      order,
      kind: 'mark',
      label,
    }))
    for (const { order, answeredOrder, api, version, node, correlationId, generation } of this
      .log) {
      events.push({
        order,
        kind: 'request',
        api,
        version,
        node,
        correlationId,
        generation,
      })
      if (answeredOrder !== undefined) {
        events.push({
          order: answeredOrder,
          kind: 'response',
          api,
          version,
          node,
          correlationId,
          generation,
        })
      }
    }

    return events.sort((a, b) => a.order - b.order)
  }

  /** @returns {Error[]} what went wrong in the cluster itself, such as a request with no wire pair */
  failures() {
    return [...this.errors]
  }

  /**
   * @returns {void}
   * @throws {Error} the first failure of the cluster itself, so a test that ends with one fails
   */
  assertClean() {
    if (this.errors.length > 0) {
      throw this.errors[0]
    }
  }

  /**
   * The options for `new Kafka(...)` that connect it to this cluster.
   * @param {object} [overrides]
   * @returns {object}
   */
  kafkaOptions(overrides = {}) {
    return {
      clientId: 'scripted-cluster',
      brokers: this.nodes.map(({ host, port }) => `${host}:${port}`),
      socketFactory: this.socketFactory,
      logLevel: logLevel.NOTHING,
      connectionTimeout: 1000,
      requestTimeout: 5000,
      connectionsMaxIdle: 0,
      ...overrides,
    }
  }

  /**
   * @private
   * @param {string} host
   * @param {number} port
   * @param {() => void} onConnect called once the socket is connected
   * @returns {import('events').EventEmitter} a socket to the node at the endpoint, answering every
   *   request written to it, or one that fails with `ECONNREFUSED` while the node is down
   * @throws {Error} when no node has that endpoint
   */
  connect(host, port, onConnect) {
    const node = this.nodes.find((candidate) => candidate.host === host && candidate.port === port)
    if (!node) {
      throw new Error(`ScriptedCluster: no node at ${host}:${port}`)
    }

    if (this.downNodes.has(node.id)) {
      const refused = this.fake.factory({ host, port, onConnect: () => {} })
      setImmediate(() => {
        refused.emit(
          'error',
          Object.assign(new Error('connect ECONNREFUSED'), {
            code: 'ECONNREFUSED',
          })
        )
        refused.destroy()
      })
      return refused
    }

    if (this.blackholedNodes.has(node.id))
      return this.fake.factory({ host, port, onConnect: () => {} })

    const socket = this.fake.factory({ host, port, onConnect })
    const generation = this.fake.state.sockets.filter(
      (other) => other.endpoint.host === host && other.endpoint.port === port
    ).length
    const write = socket.write
    socket.write = (buffer, ...rest) => {
      const result = write(buffer, ...rest)
      this.handle(socket, node, generation, Buffer.from(buffer))
      return result
    }

    return socket
  }

  /**
   * Reads one request frame and logs it, before the request is answered or refused.
   * @private
   * @param {{ id: number }} node the node the frame was written to
   * @param {number} generation which connection of the node it came in on, counting from 1
   * @param {Buffer} frame one request, size prefix included
   * @returns {{ record: RequestRecord, pair: WirePair }}
   * @throws {Error} when the frame is not one whole request the cluster advertises and has a pair
   *   for; a request whose header was read is logged with `fields: null` first
   */
  accept(node, generation, frame) {
    const header = headerOf(frame)
    const api = apiNameOf(header.apiKey)
    const record = {
      node: node.id,
      api,
      apiKey: header.apiKey,
      version: header.apiVersion,
      correlationId: header.correlationId,
      generation,
      fields: null,
      order: this.clock++,
    }
    this.log.push(record)

    const pair = this.pairFor(api, header.apiVersion)
    record.fields = reading(`the ${api} v${header.apiVersion} request`, () =>
      pair.readRequest(frame.subarray(header.byteLength))
    )
    return { record, pair }
  }

  /**
   * @private
   * @param {string} api the API name
   * @param {number} version the version requested
   * @returns {WirePair}
   * @throws {Error} when the cluster advertises the API at another version or not at all, or has
   *   no wire pair for the version it advertises
   */
  pairFor(api, version) {
    const offered = this.advertised.get(api)?.[0]
    if (offered !== version) {
      throw new Error(
        `ScriptedCluster: ${api} v${version} was requested but the cluster advertises ${
          offered === undefined ? 'no version of it' : `v${offered}`
        }`
      )
    }

    const pair = pairOf(api, version)
    if (!pair) {
      throw new Error(`ScriptedCluster: no response builder for ${api} v${version}`)
    }

    return pair
  }

  /**
   * Answers one request frame, or refuses it. The answer goes out on a later turn of the event
   * loop, and not at all on a socket destroyed by then.
   * @private
   * @param {import('events').EventEmitter} socket the socket the frame came in on
   * @param {{ id: number }} node the node it was written to
   * @param {number} generation which connection of the node the socket is, counting from 1
   * @param {Buffer} frame exactly one request, size prefix included
   * @returns {void}
   */
  handle(socket, node, generation, frame) {
    let accepted
    try {
      accepted = this.accept(node, generation, frame)
    } catch (error) {
      this.refuse(socket, error)
      return
    }

    const { record, pair } = accepted
    this.schedule
      .run({ ...record, ...targetOf(record.fields) }, () => this.defaultAnswer(pair, record))
      .then((answer) => (answer === undefined ? this.defaultAnswer(pair, record) : answer))
      .then(
        (answer) =>
          // A macrotask, as the bytes of a real answer arrive: a client that loops on empty
          // answers must not be able to starve the timers behind a chain of microtasks.
          setImmediate(() => {
            if (!socket.destroyed) {
              const body = Buffer.isBuffer(answer) ? answer : pair.buildResponse(answer)
              record.answeredOrder = this.clock++
              respondWithFrame(
                { socket },
                record.correlationId,
                responseHeaderVersion(record.apiKey, record.version),
                body
              )
            }
          }),
        (error) => socket.destroy(error)
      )
  }

  /**
   * The fields laid over a pair's defaults when nothing in the schedule picks a call: the table
   * of the profile for ApiVersions, the nodes for Metadata, the first node for FindCoordinator,
   * else what a test scripted with `answer`, else nothing.
   * @private
   * @param {WirePair} pair
   * @param {RequestRecord} record
   * @returns {object|Promise<object>} the response fields to lay over the pair's defaults; a
   *   promise when the scripted answer is asynchronous
   */
  defaultAnswer(pair, record) {
    const scripted = this.answerers.get(pair.api)
    if (scripted) {
      return scripted(record.fields, record)
    }

    switch (pair.api) {
      case 'ApiVersions':
        return this.apiVersionsAnswer()
      case 'Metadata':
        return {
          brokers: this.nodes.map(({ id, host, port }) => ({
            nodeId: id,
            host,
            port,
            rack: null,
          })),
          controllerId: this.nodes[0].id,
          topics: [],
        }
      case 'FindCoordinator': {
        const [{ id, host, port }] = this.nodes
        return {
          coordinators: record.fields.coordinatorKeys.map((key) => ({
            key,
            nodeId: id,
            host,
            port,
            errorCode: 0,
            errorMessage: null,
          })),
        }
      }
      default:
        return {}
    }
  }

  /**
   * @private
   * @param {import('events').EventEmitter} socket
   * @param {Error} error destroys the socket with it and keeps it for `failures()`
   */
  refuse(socket, error) {
    this.errors.push(error)
    socket.destroy(error)
  }
}

/**
 * Answers Metadata with one topic led by the first node, the way every script of this file does.
 * @param {ScriptedCluster} cluster
 * @param {{ topic: string, partitions: number }} options
 * @returns {ScriptedCluster} the cluster
 */
const answerMetadata = (cluster, { topic, partitions }) => {
  const [leader] = cluster.nodes
  const partitionIds = Array.from({ length: partitions }, (_, partition) => partition)

  return cluster.answer('Metadata', () => ({
    brokers: cluster.nodes.map(({ id, host, port }) => ({
      nodeId: id,
      host,
      port,
      rack: null,
    })),
    controllerId: leader.id,
    topics: [
      {
        errorCode: 0,
        name: topic,
        topicId: TOPIC_ID,
        isInternal: false,
        topicAuthorizedOperations: -2147483648,
        partitions: partitionIds.map((partitionIndex) => ({
          errorCode: 0,
          partitionIndex,
          leaderId: leader.id,
          leaderEpoch: 5,
          replicas: [leader.id],
          isr: [leader.id],
          offlineReplicas: [],
        })),
      },
    ],
  }))
}

/**
 * Scripts the answers one classic consumer needs, all fixed: the cluster has no group logic, so a
 * test that needs another outcome overrides one answer with `answer` or the schedule.
 *
 * Metadata always lists the topic. The consumer is its group's only member and leader: JoinGroup answers with the member and
 * generation given and hands back the member's own metadata, SyncGroup assigns every partition of
 * the topic to it, OffsetFetch reports `committed` (`-1` for a partition without one), ListOffsets
 * answers `earliest` for every partition, the first `batches` Fetches carry one record batch per
 * partition asked for and later ones are empty after the wait the Fetch asked for (real timers: a
 * spec on fake timers fakes everything but `setImmediate`), and commits and leaves are accepted. The topic is
 * the fetch harness's one topic, which the Fetch and OffsetForLeaderEpoch wire pairs are built
 * around; `await prepareRecordBatches()` from the harness before a run that fetches records.
 * @param {ScriptedCluster} cluster
 * @param {object} [options]
 * @param {string} [options.topic] the harness's `test-topic`
 * @param {number} [options.partitions=1]
 * @param {string} [options.memberId='member-1']
 * @param {number} [options.generationId=1]
 * @param {Record<number, string>} [options.committed={}] committed offset by partition
 * @param {string} [options.earliest='0']
 * @param {number} [options.batches=1]
 * @param {number} [options.coordinator] the id of the node that is the group coordinator, the
 *   first node when omitted (the partitions are always led by the first)
 * @returns {ScriptedCluster} the cluster
 */
const groupScript = (
  cluster,
  {
    topic = TOPIC,
    partitions = 1,
    memberId = 'member-1',
    generationId = 1,
    committed = {},
    earliest = '0',
    batches = 1,
    coordinator,
  } = {}
) => {
  const [leader] = cluster.nodes
  const coordinatorNode = cluster.nodes.find(({ id }) => id === coordinator) ?? leader
  const partitionIds = Array.from({ length: partitions }, (_, partition) => partition)
  let fetches = 0

  return answerMetadata(cluster, { topic, partitions })
    .answer('FindCoordinator', ({ coordinatorKeys }) => ({
      coordinators: coordinatorKeys.map((key) => ({
        key,
        nodeId: coordinatorNode.id,
        host: coordinatorNode.host,
        port: coordinatorNode.port,
        errorCode: 0,
        errorMessage: null,
      })),
    }))
    .answer('JoinGroup', ({ protocolType, groupProtocols: [protocol] }) => ({
      protocolType,
      memberId,
      generationId,
      leaderId: memberId,
      groupProtocol: protocol.name,
      members: [{ memberId, groupInstanceId: null, memberMetadata: protocol.metadata }],
    }))
    .answer('SyncGroup', () => ({
      memberAssignment: MemberAssignment.encode({
        version: 1,
        assignment: { [topic]: partitionIds },
      }).toString('hex'),
    }))
    .answer('OffsetFetch', ({ groupId, topics }) => ({
      groupId,
      topics: topics.map((entry) => ({
        topic: entry.topic,
        partitions: entry.partitions.map(({ partition }) => ({
          partition,
          offset: committed[partition] ?? '-1',
          leaderEpoch: -1,
          metadata: null,
          errorCode: 0,
        })),
      })),
    }))
    .answer('ListOffsets', ({ topics }) => ({
      topics: topics.map((entry) => ({
        name: entry.name,
        partitions: entry.partitions.map(({ partitionIndex }) => ({
          partitionIndex,
          errorCode: 0,
          timestamp: '-1',
          offset: earliest,
          leaderEpoch: 5,
        })),
      })),
    }))
    .answer('Fetch', async ({ maxWaitMs, topics }) => {
      fetches += 1
      if (fetches > batches) {
        // Like a broker with nothing to return: hold the answer for the longest wait asked for.
        await new Promise((resolve) => setTimeout(resolve, maxWaitMs))
      }

      return {
        partitions: topics[0].partitions.map(({ partition }) => ({
          partition,
          errorCode: 0,
          records: fetches <= batches,
        })),
      }
    })
    .answer('OffsetCommit', ({ topics }) => ({
      topics: topics.map((entry) => ({
        topic: entry.topic,
        partitions: entry.partitions.map(({ partition }) => ({
          partition,
          errorCode: 0,
        })),
      })),
    }))
    .answer('LeaveGroup', ({ members }) => ({
      members: members.map((member) => ({ ...member, errorCode: 0 })),
    }))
}

/**
 * Scripts the answers one consumer of the consumer group protocol needs, over the `consumer`
 * profile: everything `groupScript` scripts (the metadata, the coordinator, the position, the
 * fetches), and then the heartbeat as the group coordinator answers it. A join (epoch 0) is
 * answered with epoch 1, the heartbeat interval and the assignment of every partition of the topic;
 * a later heartbeat with the same epoch and a leave (a negative epoch) are answered with the
 * epoch they carried and no assignment. OffsetFetch and OffsetCommit answer for the topic ids the
 * request names, and OffsetFetch for the group it names, `committed` (`-1` for none) by partition.
 * A test that needs another outcome overrides one answer with `answer` or the schedule.
 * @param {ScriptedCluster} cluster
 * @param {object} [options] those of `groupScript`, and
 * @param {number} [options.heartbeatIntervalMs=100] the interval the coordinator sets
 * @returns {ScriptedCluster} the cluster
 */
const consumerProtocolScript = (cluster, options = {}) => {
  const { partitions = 1, committed = {}, heartbeatIntervalMs = 100 } = options
  const partitionIds = Array.from({ length: partitions }, (_, partition) => partition)
  const positions = (entries) =>
    entries.map(({ topicId, partitions: asked }) => ({
      topicId,
      partitions: asked.map(({ partition }) => ({
        partition,
        offset: committed[partition] ?? '-1',
        leaderEpoch: -1,
        metadata: null,
        errorCode: 0,
      })),
    }))

  return groupScript(cluster, options)
    .answer('ConsumerGroupHeartbeat', ({ memberId, memberEpoch }) => ({
      memberId,
      memberEpoch: memberEpoch === 0 ? 1 : memberEpoch,
      heartbeatIntervalMs,
      assignment:
        memberEpoch === 0
          ? {
              topicPartitions: [{ topicId: TOPIC_ID, partitions: partitionIds }],
            }
          : null,
    }))
    .answer('OffsetFetch', ({ groupId, topics }) => ({
      groupId,
      topics: positions(
        topics ?? [
          {
            topicId: TOPIC_ID,
            partitions: partitionIds.map((partition) => ({ partition })),
          },
        ]
      ),
    }))
    .answer('OffsetCommit', ({ topics }) => ({
      topics: topics.map(({ topicId, partitions: sent }) => ({
        topicId,
        partitions: sent.map(({ partition }) => ({ partition, errorCode: 0 })),
      })),
    }))
}

/**
 * Scripts the answers one producer needs, all fixed, so that an idempotent producer (the `default`
 * profile) and a transactional one (`tv1`, `tv2`) can run their whole path: Metadata lists the
 * topic, a Produce, an AddPartitionsToTxn and a TxnOffsetCommit answer for exactly the partitions
 * the request names with no error, and InitProducerId, AddOffsetsToTxn and EndTxn answer as the
 * wire pair's defaults do. The Produce answers carry no log state: every base offset is `0`, and no
 * leader or node endpoint is sent, as a broker sends none while leadership has not moved.
 * A test that needs another outcome overrides one answer with `answer` or the schedule.
 * @param {ScriptedCluster} cluster
 * @param {object} [options]
 * @param {string} [options.topic] the fetch harness's `test-topic`
 * @param {number} [options.partitions=1]
 * @returns {ScriptedCluster} the cluster
 */
const producerScript = (cluster, { topic = TOPIC, partitions = 1 } = {}) =>
  answerMetadata(cluster, { topic, partitions })
    .answer('Produce', ({ topics }) => ({
      responses: topics.map(({ partitions: entries, ...key }) => ({
        ...key,
        partitions: entries.map(({ partition }) => ({
          partition,
          errorCode: 0,
          baseOffset: '0',
          logAppendTime: '-1',
          logStartOffset: '0',
          leader: null,
        })),
      })),
      nodeEndpoints: [],
    }))
    .answer('AddPartitionsToTxn', ({ topics }) => ({
      errors: topics.map((entry) => ({
        topic: entry.topic,
        partitionErrors: entry.partitions.map((partition) => ({
          partition,
          errorCode: 0,
        })),
      })),
    }))
    .answer('TxnOffsetCommit', ({ topics }) => ({
      topics: topics.map((entry) => ({
        topic: entry.topic,
        partitions: entry.partitions.map(({ partition }) => ({
          partition,
          errorCode: 0,
        })),
      })),
    }))

/**
 * @param {ConstructorParameters<typeof ScriptedCluster>[0]} options
 * @returns {ScriptedCluster}
 */
const createScriptedCluster = (options) => new ScriptedCluster(options)

module.exports = {
  createScriptedCluster,
  ScriptedCluster,
  groupScript,
  consumerProtocolScript,
  producerScript,
  PROFILES,
}
