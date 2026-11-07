const { Schedule } = require('./schedule')
const { createFakeSocketFactory, respondWithFrame, readRequestHeader } = require('./fakeSocket')
const { responseHeaderVersion } = require('../src/protocol/headers')
const apiKeys = require('../src/protocol/requests/apiKeys')
const { logLevel } = require('../index')
const { TOPIC, TOPIC_ID } = require('../src/consumer/__tests__/fixtures/fetchHarness')
const { MemberAssignment } = require('../src/consumer/assignerProtocol')
const { requests: clientRequests } = require('../src/protocol/requests')

/**
 * The ApiVersions version the client asks first: the highest it implements. A cluster that offers
 * a lower one answers it as a broker of that version does, with a v0-shaped UNSUPPORTED_VERSION.
 */
const DISCOVERY_VERSION = Math.max(...clientRequests.ApiVersions.versions.map(Number))

/**
 * The fake milliseconds a connect to a profile that offers ApiVersions v4 needs before its
 * negotiation is answered, for a test that advances fake timers: the refused v5 request and the
 * v4 request are each answered on a later turn, and a turn scheduled while a tick runs falls one
 * fake millisecond after the one before. The dial itself is not counted.
 */
const NEGOTIATION_MS = 2

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
 * which also sends no AddPartitionsToTxn and no AddOffsetsToTxn. The `share` profile advertises the
 * bootstrap pairs and the seven released share-group APIs at their one implemented version each;
 * every one of them has a wire file.
 * @type {Record<'default'|'sasl'|'consumer'|'tv1'|'tv2'|'offsets'|'transactions'|'storage'|'securityAdmin'|'clusterAdmin'|'share'|'telemetry', Array<[string, number]>>}
 */
const PROFILES = {
  share: [
    ['ApiVersions', 4],
    ['Metadata', 13],
    ['FindCoordinator', 6],
    ['ShareGroupHeartbeat', 1],
    ['ShareGroupDescribe', 1],
    ['ShareFetch', 2],
    ['ShareAcknowledge', 2],
    ['DescribeShareGroupOffsets', 1],
    ['AlterShareGroupOffsets', 0],
    ['DeleteShareGroupOffsets', 0],
  ],
  storage: [
    ...DEFAULT.filter(([api]) => api !== 'ListOffsets'),
    ['ListOffsets', 11],
    ['DescribeLogDirs', 5],
    ['AlterReplicaLogDirs', 2],
    ['AlterPartitionReassignments', 1],
    ['DeleteTopics', 6],
  ],
  offsets: [...DEFAULT, ['OffsetDelete', 0]],
  transactions: [
    ...DEFAULT,
    ['DescribeProducers', 0],
    ['DescribeTransactions', 0],
    ['ListTransactions', 2],
    ['WriteTxnMarkers', 2],
  ],
  default: DEFAULT,
  telemetry: [...DEFAULT, ['GetTelemetrySubscriptions', 0], ['PushTelemetry', 0]],
  securityAdmin: [
    ...DEFAULT,
    ['DescribeClientQuotas', 1],
    ['AlterClientQuotas', 1],
    ['DescribeUserScramCredentials', 0],
    ['AlterUserScramCredentials', 0],
    ['CreateDelegationToken', 3],
    ['RenewDelegationToken', 2],
    ['ExpireDelegationToken', 2],
    ['DescribeDelegationToken', 3],
  ],
  clusterAdmin: [
    ...DEFAULT,
    ['ElectLeaders', 2],
    ['UpdateFeatures', 2],
    ['DescribeCluster', 2],
    ['ListConfigResources', 1],
  ],
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
 * @property {true} [discovery] set on the refused ApiVersions request `requests()` leaves out
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
 * ApiVersions discovery: the client's ApiVersions request is at the highest version it implements.
 * When the profile offers a lower one, that request is read by the pair of the client's version
 * and answered at once with the v0-shaped UNSUPPORTED_VERSION body of a broker that offers
 * ApiVersions up to the profile's version; the client then asks again at that version, and that
 * request is the one logged, scheduled and answered as any other. `requests()`, `timeline()` and
 * the schedule leave out one such refusal per negotiation, so they see one ApiVersions request per
 * negotiation, as they do for a broker that offers the client's version: the first request of a
 * socket, and the one that negotiates it again after the client opened another socket to the node,
 * which is what it does when its pool's generation changes. Any other, a second negotiation on a
 * connection that has not changed, is answered the same way but `requests()` and `timeline()` list
 * it. Neither kind is scheduled, and `apiVersionTrace()` lists both.
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
   * @param {'default'|'sasl'|'consumer'|'tv1'|'tv2'|'offsets'|'transactions'|'storage'|'securityAdmin'|'clusterAdmin'|'share'|'telemetry'} [options.profile='default'] what the cluster advertises
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
    /** @type {Map<string, number>} sockets opened to a node when the socket last negotiated, by socket */
    this.negotiatedAt = new Map()
    this.marks = []
    this.clock = 0
    this.errors = []
    this.answerers = new Map()
    this.downNodes = new Set()
    this.blackholedNodes = new Set()
    /** @type {Array<{node: object, socket: object, onConnect: () => void, generation: number}>} */
    this.heldDials = []
    /** @type {Map<string, object>} fields laid over every object answer of an API during its outage */
    this.outages = new Map()
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
   * Holds every later dial to a node: the socket exists but never connects, so a connect stays
   * pending until `release`. Sockets the client already holds are unchanged.
   * @param {number} id the node's id
   * @returns {ScriptedCluster} this
   */
  blackhole(id) {
    this.downNodes.delete(id)
    this.blackholedNodes.add(id)
    return this
  }

  /**
   * Restores future dials of a node taken down or blackholed. A dial already held stays held until
   * `release`.
   * @param {number} id the node's id
   * @returns {ScriptedCluster} this
   */
  up(id) {
    this.downNodes.delete(id)
    this.blackholedNodes.delete(id)
    return this
  }

  /**
   * Connects every dial of a node that `blackhole` is holding, as if the network let it through
   * now: each live held socket reports its connection on a later turn and answers requests from
   * then on. A held socket the client already destroyed, for example on its connection timeout,
   * stays dead. Callable whether or not the node is still blackholed.
   * @param {number} id the node's id
   * @returns {ScriptedCluster} this
   */
  release(id) {
    const released = this.heldDials.filter(({ node }) => node.id === id)
    this.heldDials = this.heldDials.filter(({ node }) => node.id !== id)
    for (const { node, socket, onConnect, generation } of released) {
      if (socket.destroyed) continue
      this.answerOn(socket, node, generation)
      setImmediate(onConnect)
    }
    return this
  }

  /**
   * @param {number} id the node's id
   * @returns {number} how many dials to the node are held and still alive, so a test can wait for
   *   a dial to arrive before it releases it
   */
  heldDialsTo(id) {
    return this.heldDials.filter(({ node, socket }) => node.id === id && !socket.destroyed).length
  }

  /**
   * Starts an outage of one API: until `recover`, every later object answer of it carries `fields`
   * over what it would have answered, so the broker stays reachable while the API fails. For
   * Metadata, `{ errorCode: 129 }` is the top-level REBOOTSTRAP_REQUIRED answer. A Buffer answer
   * released by a test is sent as it is.
   * @param {string} api the API name, e.g. `Metadata`
   * @param {object} fields the response fields that make it fail
   * @returns {ScriptedCluster} this
   */
  outage(api, fields) {
    this.outages.set(api, fields)
    return this
  }

  /**
   * Ends the outage of an API; its later answers are the ones nothing overrides.
   * @param {string} api the API name
   * @returns {ScriptedCluster} this
   */
  recover(api) {
    this.outages.delete(api)
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
   *   finalizedFeaturesEpoch?: number,
   *   finalizedFeatures?: Array<{ name: string, minVersionLevel: number, maxVersionLevel: number }> }}
   *   the fields the ApiVersions answer is built from: the table, under `tv2` the finalized
   *   `transaction.version` at level 2, which is what makes a client choose transaction version 2,
   *   and under `share` the finalized `share.version` at level 1 with feature epoch 7
   */
  apiVersionsAnswer() {
    if (this.profile === 'share') {
      return {
        apiKeys: this.apiVersionsTable(),
        finalizedFeaturesEpoch: 7,
        finalizedFeatures: [{ name: 'share.version', minVersionLevel: 1, maxVersionLevel: 1 }],
      }
    }

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

  /**
   * @returns {RequestRecord[]} every request the client wrote, oldest first, without the one
   *   ApiVersions discovery request each negotiation is kept apart from (see `apiVersionTrace`)
   */
  requests() {
    return this.log.filter(({ discovery }) => !discovery)
  }

  /**
   * @returns {RequestRecord[]} every ApiVersions request the client wrote, oldest first, the
   *   discovery requests a lower-versioned profile refused with UNSUPPORTED_VERSION included:
   *   `[5, 4]` per negotiation against a profile that offers v4, `[5]` against one that offers v5.
   *   A refusal `requests()` does not list is marked `discovery: true`.
   */
  apiVersionTrace() {
    return this.log.filter(({ apiKey }) => apiKey === apiKeys.ApiVersions)
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
    for (const {
      order,
      answeredOrder,
      api,
      version,
      node,
      correlationId,
      generation,
    } of this.requests()) {
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
   * Setup defaults to two minutes to tolerate coverage runner delays; Jest still bounds a hung
   * test, and timeout tests can override this budget explicitly.
   * @param {object} [overrides]
   * @returns {object}
   */
  kafkaOptions(overrides = {}) {
    return {
      clientId: 'scripted-cluster',
      brokers: this.nodes.map(({ host, port }) => `${host}:${port}`),
      socketFactory: this.socketFactory,
      logLevel: logLevel.NOTHING,
      connectionTimeout: 120000,
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

    if (this.blackholedNodes.has(node.id)) {
      const held = this.fake.factory({ host, port, onConnect: () => {} })
      this.heldDials.push({ node, socket: held, onConnect, generation: this.socketsOpenedTo(node) })
      return held
    }

    const socket = this.fake.factory({ host, port, onConnect })
    this.answerOn(socket, node, this.socketsOpenedTo(node))

    return socket
  }

  /**
   * Makes a socket answer every request written to it.
   * @private
   * @param {import('events').EventEmitter} socket the socket of a connected dial
   * @param {{ id: number, host: string, port: number }} node the node it dialled
   * @param {number} generation which connection of the node it is, counting from 1
   * @returns {void}
   */
  answerOn(socket, node, generation) {
    const write = socket.write
    socket.write = (buffer, ...rest) => {
      const result = write(buffer, ...rest)
      this.handle(socket, node, generation, Buffer.from(buffer))
      return result
    }
  }

  /**
   * @private
   * @param {{ host: string, port: number }} node
   * @returns {number} how many sockets the client has opened to the node, refused dials included;
   *   the last one's number is its `generation`
   */
  socketsOpenedTo({ host, port }) {
    return this.fake.state.sockets.filter(
      (other) => other.endpoint.host === host && other.endpoint.port === port
    ).length
  }

  /**
   * Reads one request frame and logs it, before the request is answered or refused.
   * @private
   * @param {{ id: number, host: string, port: number }} node the node the frame was written to
   * @param {number} generation which connection of the node it came in on, counting from 1
   * @param {Buffer} frame one request, size prefix included
   * @returns {{ record: RequestRecord, pair: WirePair, refused: boolean }} `refused` when a broker
   *   of the profile answers it with UNSUPPORTED_VERSION
   * @throws {Error} when the frame is not one whole request the cluster advertises and has a pair
   *   for; a request whose header was read is logged with `fields: null` first
   */
  accept(node, generation, frame) {
    const header = headerOf(frame)
    const api = apiNameOf(header.apiKey)
    const refused = this.isRefusedApiVersions(header)
    const discovery = refused && this.opensNegotiation(node, generation)
    const record = {
      node: node.id,
      api,
      apiKey: header.apiKey,
      version: header.apiVersion,
      correlationId: header.correlationId,
      generation,
      fields: null,
      order: discovery ? this.clock : this.clock++,
      ...(discovery && { discovery: true }),
    }
    this.log.push(record)

    const pair = refused ? pairOf(api, header.apiVersion) : this.pairFor(api, header.apiVersion)
    if (!pair) {
      throw new Error(`ScriptedCluster: no response builder for ${api} v${header.apiVersion}`)
    }

    record.fields = reading(`the ${api} v${header.apiVersion} request`, () =>
      pair.readRequest(frame.subarray(header.byteLength))
    )
    return { record, pair, refused }
  }

  /**
   * @private
   * @param {{ apiKey: number, apiVersion: number }} header
   * @returns {boolean} whether the request is an ApiVersions request at the highest version the
   *   client implements, to a profile that offers a lower one
   */
  isRefusedApiVersions({ apiKey, apiVersion }) {
    const offered = this.advertised.get('ApiVersions')?.[0]
    return (
      apiKey === apiKeys.ApiVersions &&
      apiVersion === DISCOVERY_VERSION &&
      offered !== undefined &&
      offered < DISCOVERY_VERSION
    )
  }

  /**
   * Whether a refused ApiVersions request is the one a negotiation is kept apart from: the first
   * request of its socket, or the one that negotiates the socket again after another socket to the
   * node was opened. It remembers that the socket negotiated, so a second one with nothing opened
   * in between is not. Call it once per request, before the request is logged.
   * @private
   * @param {{ id: number, host: string, port: number }} node
   * @param {number} generation which connection of the node the socket is, counting from 1
   * @returns {boolean}
   */
  opensNegotiation(node, generation) {
    const socket = `${node.id}/${generation}`
    const opened = this.socketsOpenedTo(node)
    const negotiatedAt = this.negotiatedAt.get(socket)
    const opens =
      negotiatedAt === undefined
        ? !this.log.some((other) => other.node === node.id && other.generation === generation)
        : negotiatedAt < opened
    if (opens) this.negotiatedAt.set(socket, opened)
    return opens
  }

  /**
   * Answers a refused ApiVersions request as a broker that offers ApiVersions up to the profile's
   * version does: UNSUPPORTED_VERSION with a v0-shaped body naming that range, on a later turn of
   * the event loop and not at all on a socket destroyed by then. It is not scheduled and not
   * answered by a scripted answerer: the request the client asks next is. One that `requests()`
   * lists is marked answered, so `timeline()` shows its response.
   * @private
   * @param {import('events').EventEmitter} socket the socket the request came in on
   * @param {RequestRecord} record the logged request
   * @param {WirePair} pair the pair of the version it asked, which builds the body
   * @returns {void}
   */
  refuseApiVersions(socket, record, pair) {
    setImmediate(() => {
      if (socket.destroyed) return
      if (!record.discovery) record.answeredOrder = this.clock++
      respondWithFrame(
        { socket },
        record.correlationId,
        responseHeaderVersion(record.apiKey, record.version),
        pair.buildUnsupportedVersionResponse({
          apiKeys: [
            {
              apiKey: apiKeys.ApiVersions,
              minVersion: 0,
              maxVersion: this.advertised.get('ApiVersions')[0],
            },
          ],
        })
      )
    })
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
   * loop, and not at all on a socket destroyed by then. The share profile defaults object answers
   * to zero throttle instead of inheriting the wire vectors' quota examples; explicit throttles
   * and raw response buffers are preserved, including answers released through a schedule gate.
   * @private
   * @param {import('events').EventEmitter} socket the socket the frame came in on
   * @param {{ id: number, host: string, port: number }} node the node it was written to
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

    const { record, pair, refused } = accepted
    if (refused) {
      this.refuseApiVersions(socket, record, pair)
      return
    }

    this.schedule
      .run({ ...record, ...targetOf(record.fields) }, () => this.defaultAnswer(pair, record))
      .then((answer) => (answer === undefined ? this.defaultAnswer(pair, record) : answer))
      .then((answer) => this.underOutage(record.api, answer))
      .then(
        (answer) =>
          // A macrotask, as the bytes of a real answer arrive: a client that loops on empty
          // answers must not be able to starve the timers behind a chain of microtasks.
          setImmediate(() => {
            if (!socket.destroyed) {
              const body = Buffer.isBuffer(answer)
                ? answer
                : pair.buildResponse({
                    ...(this.profile === 'share' && { throttleTime: 0 }),
                    ...answer,
                  })
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
   * @private
   * @param {string} api the API name
   * @param {object|Buffer} answer what the call would be answered with
   * @returns {object|Buffer} the answer with the API's outage fields over it, or the answer itself
   *   when there is no outage or the answer is a Buffer
   */
  underOutage(api, answer) {
    const outage = this.outages.get(api)
    return outage && !Buffer.isBuffer(answer) ? { ...answer, ...outage } : answer
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
 * @param {Set<ReturnType<typeof setTimeout>>} [options.brokerAnswerTimers] outstanding Fetch-answer timers
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
    brokerAnswerTimers = new Set(),
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
        await new Promise((resolve) => {
          const timer = setTimeout(() => {
            brokerAnswerTimers.delete(timer)
            resolve()
          }, maxWaitMs)
          brokerAnswerTimers.add(timer)
        })
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
 * Installs literal share-group answers without simulating membership, sessions, a log or
 * ownership: each row's `response` is returned as it is and the wire pair supplies any field it
 * omits. A repeated API replaces its earlier answer, and the schedule stays the only place that
 * orders calls. Intended for the `share` profile.
 * @param {ScriptedCluster} cluster
 * @param {Array<{ api: string, response: object }>} rows literal answers, applied in order
 * @returns {ScriptedCluster} the cluster; no group or acknowledgement state is kept
 */
const shareScript = (cluster, rows) => {
  for (const { api, response } of rows) {
    cluster.answer(api, () => response)
  }

  return cluster
}

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
  shareScript,
  PROFILES,
  NEGOTIATION_MS,
}
