const Broker = require('../src/broker')
const Connection = require('../src/network/connection')
const apiKeys = require('../src/protocol/requests/apiKeys')
const { lookup } = require('../src/protocol/requests')
const { responseHeaderVersion } = require('../src/protocol/headers')
const {
  createLogger,
  LEVELS: { NOTHING },
} = require('../src/loggers')
const LoggerConsole = require('../src/loggers/console')
const { createFakeSocketFactory, respondWithFrame, readRequestHeader } = require('./fakeSocket')
const setupPool = require('./setupPool')
const joinGroupV8 = require('./wire/joinGroupV8')
const syncGroupV5 = require('./wire/syncGroupV5')
const heartbeatV4 = require('./wire/heartbeatV4')
const leaveGroupV5 = require('./wire/leaveGroupV5')

/** Kafka error codes the classic group handlers answer with. */
const CODES = Object.freeze({
  NONE: 0,
  NOT_COORDINATOR: 16,
  ILLEGAL_GENERATION: 22,
  INCONSISTENT_GROUP_PROTOCOL: 23,
  UNKNOWN_MEMBER_ID: 25,
  REBALANCE_IN_PROGRESS: 27,
  MEMBER_ID_REQUIRED: 79,
  FENCED_INSTANCE_ID: 82,
})

/** Classic group states, as Kafka names them. */
const STATES = Object.freeze({
  EMPTY: 'Empty',
  PREPARING: 'PreparingRebalance',
  COMPLETING: 'CompletingRebalance',
  STABLE: 'Stable',
})

/** The APIs the double serves, at the versions a Kafka 4.x coordinator and this client agree on. */
const WIRE = Object.freeze({
  [apiKeys.JoinGroup]: joinGroupV8,
  [apiKeys.SyncGroup]: syncGroupV5,
  [apiKeys.Heartbeat]: heartbeatV4,
  [apiKeys.LeaveGroup]: leaveGroupV5,
})

const EMPTY_ASSIGNMENT = Buffer.alloc(0)

/**
 * @param {object} fields
 * @returns {object} a JoinGroup answer with Kafka's field defaults under `fields`
 */
const joinAnswer = (fields) => ({
  errorCode: CODES.NONE,
  generationId: -1,
  protocolType: null,
  groupProtocol: null,
  leaderId: '',
  memberId: '',
  members: [],
  ...fields,
})

/**
 * @param {object} fields
 * @returns {object} a SyncGroup answer with Kafka's field defaults under `fields`
 */
const syncAnswer = (fields) => ({
  errorCode: CODES.NONE,
  protocolType: null,
  protocolName: null,
  memberAssignment: EMPTY_ASSIGNMENT,
  ...fields,
})

/** @returns {{ promise: Promise<*>, resolve: Function }} */
const deferred = () => {
  let resolve
  const promise = new Promise((settle) => (resolve = settle))
  return { promise, resolve }
}

/**
 * Reads a request body with the wire reader of its API, its byte fields as buffers.
 * @param {number} apiKey
 * @param {Buffer} body
 * @returns {object}
 */
const readRequest = (apiKey, body) => {
  const request = WIRE[apiKey].readRequest(body)
  if (apiKey === apiKeys.JoinGroup) {
    request.groupProtocols = request.groupProtocols.map(({ name, metadata }) => ({
      name,
      metadata: Buffer.from(metadata, 'hex'),
    }))
  }
  if (apiKey === apiKeys.SyncGroup) {
    request.groupAssignment = request.groupAssignment.map(({ memberId, memberAssignment }) => ({
      memberId,
      memberAssignment: Buffer.from(memberAssignment, 'hex'),
    }))
  }
  return request
}

/**
 * A Kafka 4.x classic group coordinator for one group, modelled on the classic handlers of the
 * group coordinator (`GroupMetadataManager` and `ClassicGroup`, the same at 4.0.0 and 4.4.0):
 *
 * - JoinGroup: MEMBER_ID_REQUIRED (79) for a new dynamic member, which joins as a pending member;
 *   INCONSISTENT_GROUP_PROTOCOL (23) when no offered protocol is supported by every member; the
 *   join barrier (every member, and no pending member, must have joined, or the rebalance timeout
 *   passes and the dynamic members that did not rejoin are removed); the initial rebalance delay;
 *   a leader rejoining a stable group or a member with changed metadata starts a rebalance; a
 *   follower rejoining unchanged gets the current generation; a static member joining with an
 *   empty member id takes a new id and fences the old one's parked JoinGroup and SyncGroup (82).
 * - SyncGroup: member and instance checks (25, 82), generation check (22), protocol check (23),
 *   REBALANCE_IN_PROGRESS (27) while preparing, parked while completing until the leader's
 *   SyncGroup stores its plan once for every member (missing members get zero bytes), the stored
 *   bytes while stable. A rebalance answers parked SyncGroups with 27.
 * - Heartbeat: 25, 82, 22, then 27 while preparing.
 * - LeaveGroup: decided by the identity each request member names: an empty member id with an
 *   instance id removes whoever holds the instance; a pending id is removed; otherwise the pair is
 *   validated (25, 82). A removed member's parked JoinGroup is answered 25.
 *
 * Not modelled: session expiry runs only when a test calls `expireSession`; pending-sync expiry,
 * the group size limit and failed metadata writes are absent; the protocol vote breaks ties by
 * the order members joined, and a removed leader is replaced by the earliest member left (Kafka
 * iterates a hash map). A request it cannot read, or for another group, closes its connection
 * with an error and is listed in `failures`.
 *
 * Each `connect()` gives a production `Broker` over its own connection and scripted socket, at
 * JoinGroup v8, SyncGroup v5, Heartbeat v4 and LeaveGroup v5. Every request goes through three
 * steps: the client writes it, the coordinator accepts it (its effect on the group happens here),
 * and the reply is released. A test can hold a step (`hold`), so a client that gives up after
 * acceptance never undoes what the coordinator did. As Kafka does, one connection is served one
 * request at a time: a parked JoinGroup or SyncGroup holds back what was written after it.
 * `faultNextReply` changes one delivered reply without touching what the group stored.
 * `unload()` answers everything parked with NOT_COORDINATOR (16), as Kafka does when the
 * coordinator moves; `close()` also closes every connection. A spec calls `close()` after each
 * test.
 * @see https://github.com/apache/kafka/blob/4.4.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java
 * @see https://github.com/apache/kafka/blob/4.4.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/classic/ClassicGroup.java
 */
class ClassicGroupCoordinator {
  /**
   * @param {object} [options]
   * @param {string} [options.groupId='classic-group']
   * @param {number} [options.initialRebalanceDelay=0] `group.initial.rebalance.delay.ms`; 0 ends
   *   the first rebalance of an empty group one event-loop turn after it starts
   */
  constructor({ groupId = 'classic-group', initialRebalanceDelay = 0 } = {}) {
    this.groupId = groupId
    this.initialRebalanceDelay = initialRebalanceDelay
    this.exists = false
    this.unloaded = false
    this.state = STATES.EMPTY
    this.previousState = STATES.EMPTY
    this.generationId = 0
    this.protocolType = null
    this.protocolName = null
    this.leaderId = null
    this.newMemberAdded = false
    /** @type {Map<string, object>} */
    this.members = new Map()
    /** @type {Map<string, string>} instance id to member id */
    this.staticMembers = new Map()
    this.pendingJoin = new Set()
    this.pendingSync = new Set()
    /** @type {Map<{ resolve: Function }, Function>} parked replies Kafka leaves unanswered, each
     * with the builder of its API's answer */
    this.orphans = new Map()
    /** @type {(() => void)|null} cancels the join phase's one timer */
    this.joinTimer = null
    this.nextId = 0
    /** @type {{ api: string, connection: number, request: object }[]} */
    this.requests = []
    /** @type {Error[]} */
    this.failures = []
    this.gates = []
    /** Gates a request is held at now. */
    this.held = new Set()
    this.faults = []
    this.connections = []
  }

  /**
   * Opens a client connection to this coordinator.
   * @param {object} [options]
   * @param {string} [options.clientId='classic-group-client']
   * @param {number} [options.requestTimeout=30000]
   * @param {boolean} [options.enforceRequestTimeout=false]
   * @returns {{ broker: Broker, id: number, close: () => Promise<void> }} `broker` is a production
   *   `Broker`; `id` names this connection in `requests`; `close` closes the connection at once,
   *   failing what is still in flight. Its SyncGroup calls carry `protocolType` and
   *   `protocolName`, as the consumer's do, so they go out at v5.
   */
  connect({
    clientId = 'classic-group-client',
    requestTimeout = 30000,
    enforceRequestTimeout = false,
  } = {}) {
    const id = this.connections.length
    const logger = createLogger({ level: NOTHING, logCreator: LoggerConsole })
    const { factory } = createFakeSocketFactory()
    const socketFactory = (options) => {
      const socket = factory(options)
      let served = Promise.resolve()
      const write = socket.write
      socket.write = (buffer) => {
        const frame = Buffer.from(buffer)
        served = served
          .then(() => this.serve(id, socket, frame))
          .catch((error) => {
            this.failures.push(error)
            socket.destroy(error)
          })
        return write(buffer)
      }
      return socket
    }
    const connection = new Connection({
      host: '127.0.0.1',
      port: 9092,
      logger,
      socketFactory,
      clientId,
      connectionTimeout: 10000,
      requestTimeout,
      enforceRequestTimeout,
    })
    const pool = {
      host: '127.0.0.1',
      port: 9092,
      sasl: null,
      connectionTimeout: 10000,
      generation: 0,
      isConnected: () => connection.isConnected(),
      isAuthenticated: () => true,
      getConnection: async () => {
        if (!connection.isConnected()) await connection.connect()
        return connection
      },
      getConnectionByRequest: async () => pool.getConnection(),
      send: async (protocolRequest, _connection, options) => {
        const established = await pool.getConnection()
        const guard = options?.guard
        return established.send(guard ? { ...protocolRequest, guard } : protocolRequest)
      },
      setVersions: () => {},
      destroy: async () => connection.disconnect({ drainTimeout: 0 }),
    }
    const versions = Object.fromEntries(
      Object.values(WIRE).map(({ apiKey, version }) => [
        apiKey,
        { minVersion: 0, maxVersion: version },
      ])
    )
    const broker = new Broker({ connectionPool: setupPool(pool), logger, nodeId: 0, versions })
    broker.lookupRequest = lookup(versions)
    broker.versionsGeneration = pool.generation
    const handle = { broker, id, close: () => pool.destroy() }
    this.connections.push(handle)
    return handle
  }

  /**
   * Holds the next request of `api` at `stage`: before the coordinator accepts it
   * (`'acceptance'`), or after it accepted it and before its reply is sent (`'reply'`).
   * @param {'JoinGroup'|'SyncGroup'|'Heartbeat'|'LeaveGroup'} api
   * @param {'acceptance'|'reply'} stage
   * @returns {{ reached: Promise<object>, release: () => void }} `reached` resolves with the
   *   decoded request once it is held
   */
  hold(api, stage) {
    const reached = deferred()
    const released = deferred()
    this.gates.push({ api, stage, reached, released })
    return { reached: reached.promise, release: () => released.resolve() }
  }

  /**
   * Changes the next reply of `api` delivered to the client, after the coordinator decided it. What
   * the group stored is unchanged, so a later request gets the stored answer.
   * @param {'JoinGroup'|'SyncGroup'|'Heartbeat'|'LeaveGroup'} api
   * @param {(answer: object, request: object) => object} transform
   * @param {{ memberId?: string }} [match] only a request of this member id
   * @returns {void}
   */
  faultNextReply(api, transform, { memberId } = {}) {
    this.faults.push({ api, memberId, transform })
  }

  /**
   * The member's session expires: Kafka's heartbeat expiry. A pending member is removed; a member
   * is removed unless it awaits a JoinGroup or SyncGroup answer and joined before the current
   * generation (Kafka's `hasSatisfiedHeartbeat`).
   * @param {string} memberId
   * @returns {boolean} whether the member was removed
   */
  expireSession(memberId) {
    if (this.pendingJoin.has(memberId)) {
      this.remove(memberId)
      if (this.state === STATES.PREPARING && this.allJoined()) this.completeJoin()
      return true
    }
    const member = this.members.get(memberId)
    if (!member || (!member.isNew && (member.join || member.sync))) return false
    this.removeCurrentMember(memberId)
    if (this.state === STATES.STABLE || this.state === STATES.COMPLETING) {
      this.maybePrepareRebalanceOrCompleteJoin()
    } else if (this.state === STATES.PREPARING && this.allJoined()) {
      this.completeJoin()
    }
    return true
  }

  /**
   * The coordinator moves away: parked JoinGroup and SyncGroup requests and held steps are
   * answered NOT_COORDINATOR (16), timers stop, and every later request gets 16 too.
   * @returns {void}
   */
  unload() {
    this.unloaded = true
    this.cancelJoinTimer()
    for (const member of this.members.values()) {
      this.deliverJoin(member, joinAnswer({ errorCode: CODES.NOT_COORDINATOR }))
      this.deliverSync(member, syncAnswer({ errorCode: CODES.NOT_COORDINATOR }))
    }
    for (const [reply, answer] of this.orphans) {
      reply.resolve(answer({ errorCode: CODES.NOT_COORDINATOR }))
    }
    this.orphans.clear()
    for (const gate of [...this.gates.splice(0), ...this.held]) gate.released.resolve()
  }

  /**
   * Unloads the coordinator and closes every connection. Specs call it after each test.
   * @returns {Promise<void>}
   */
  async close() {
    this.unload()
    await Promise.all(this.connections.map((connection) => connection.close()))
  }

  /**
   * @returns {{ state: string, generationId: number, leaderId: string|null,
   *   protocolName: string|null, members: { memberId: string, groupInstanceId: string|null,
   *   assignment: Buffer }[], pending: string[] }} the group as the coordinator holds it
   */
  describe() {
    return {
      state: this.state,
      generationId: this.generationId,
      leaderId: this.leaderId,
      protocolName: this.protocolName,
      members: Array.from(this.members.values(), ({ memberId, groupInstanceId, assignment }) => ({
        memberId,
        groupInstanceId,
        assignment,
      })),
      pending: Array.from(this.pendingJoin),
    }
  }

  /**
   * Serves one request frame of a connection, after the previous one of that connection.
   * @private
   * @param {number} connection
   * @param {import('events').EventEmitter} socket
   * @param {Buffer} frame
   * @returns {Promise<void>}
   */
  async serve(connection, socket, frame) {
    await new Promise(setImmediate)
    const header = readRequestHeader(frame)
    const wire = WIRE[header.apiKey]
    let request
    try {
      if (!wire || header.apiVersion !== wire.version) {
        throw new Error(`No ${header.apiKey} v${header.apiVersion} in the classic group double`)
      }
      request = readRequest(header.apiKey, frame.subarray(header.byteLength))
      if (request.groupId !== this.groupId) {
        throw new Error(`${wire.api} for group ${request.groupId}, not ${this.groupId}`)
      }
    } catch (error) {
      error.message = `Refused by the classic group double: ${error.message}`
      throw error
    }

    this.requests.push({ api: wire.api, connection, request })
    await this.passGate(wire.api, 'acceptance', request)
    if (socket.destroyed) return

    let answer = await this.accept(header.apiKey, request)
    await this.passGate(wire.api, 'reply', request)
    const fault = this.faults.findIndex(
      ({ api, memberId }) =>
        api === wire.api && (memberId === undefined || memberId === request.memberId)
    )
    if (fault !== -1) answer = this.faults.splice(fault, 1)[0].transform(answer, request)
    if (socket.destroyed) return

    respondWithFrame(
      { socket },
      header.correlationId,
      responseHeaderVersion(header.apiKey, header.apiVersion),
      wire.buildResponse({ throttleTime: 0, ...answer })
    )
  }

  /**
   * @private
   * @param {string} api
   * @param {'acceptance'|'reply'} stage
   * @param {object} request
   * @returns {Promise<void>}
   */
  async passGate(api, stage, request) {
    const at = this.gates.findIndex((gate) => gate.api === api && gate.stage === stage)
    if (at === -1) return
    const [gate] = this.gates.splice(at, 1)
    this.held.add(gate)
    gate.reached.resolve(request)
    await gate.released.promise
    this.held.delete(gate)
  }

  /**
   * The coordinator's handling of one request; the group changes before this returns.
   * @private
   * @param {number} apiKey
   * @param {object} request
   * @returns {Promise<object>} the answer, later for a parked request
   */
  accept(apiKey, request) {
    const unloaded = { errorCode: CODES.NOT_COORDINATOR }
    switch (apiKey) {
      case apiKeys.JoinGroup:
        return this.unloaded ? Promise.resolve(joinAnswer(unloaded)) : this.join(request)
      case apiKeys.SyncGroup:
        return this.unloaded ? Promise.resolve(syncAnswer(unloaded)) : this.sync(request)
      case apiKeys.Heartbeat:
        return Promise.resolve(this.unloaded ? unloaded : this.heartbeat(request))
      default:
        return Promise.resolve(this.unloaded ? { ...unloaded, members: [] } : this.leave(request))
    }
  }

  /**
   * @private
   * @param {object} request a JoinGroup v8 request
   * @returns {Promise<object>}
   */
  join(request) {
    const { memberId, groupInstanceId } = request
    const reply = deferred()
    const answer = (fields) => {
      reply.resolve(joinAnswer(fields))
      return reply.promise
    }

    if (memberId === '') this.exists = true
    if (!this.supportsProtocols(request)) {
      return answer({ memberId, errorCode: CODES.INCONSISTENT_GROUP_PROTOCOL })
    }

    if (memberId === '') {
      const newId = `${groupInstanceId ?? 'member'}-${++this.nextId}`
      if (groupInstanceId === null) {
        this.pendingJoin.add(newId)
        return answer({ memberId: newId, errorCode: CODES.MEMBER_ID_REQUIRED })
      }
      const existing = this.staticMembers.get(groupInstanceId)
      if (existing !== undefined) this.replaceStaticMember(request, existing, newId, reply)
      else this.addMember(request, newId, reply)
      return reply.promise
    }

    if (this.pendingJoin.has(memberId)) {
      this.addMember(request, memberId, reply)
      return reply.promise
    }

    const invalid = this.validateMember(memberId, groupInstanceId)
    if (invalid !== CODES.NONE) return answer({ memberId, errorCode: invalid })

    const member = this.members.get(memberId)
    const unchanged = matches(member.protocols, request.groupProtocols)
    if (
      this.state === STATES.PREPARING ||
      (this.state === STATES.COMPLETING && !unchanged) ||
      (this.state === STATES.STABLE && (memberId === this.leaderId || !unchanged))
    ) {
      this.updateMember(member, request, reply)
      this.maybePrepareRebalanceOrCompleteJoin()
      return reply.promise
    }
    if (this.state === STATES.COMPLETING || this.state === STATES.STABLE) {
      return answer(this.currentJoinAnswer(member, this.state === STATES.COMPLETING))
    }
    return answer({ memberId, errorCode: CODES.UNKNOWN_MEMBER_ID })
  }

  /**
   * @private
   * @param {object} request a SyncGroup v5 request
   * @returns {Promise<object>}
   */
  sync(request) {
    const { memberId, groupInstanceId, generationId } = request
    const answer = (fields) => Promise.resolve(syncAnswer(fields))

    if (this.state === STATES.EMPTY) return answer({ errorCode: CODES.UNKNOWN_MEMBER_ID })
    const invalid = this.validateMember(memberId, groupInstanceId)
    if (invalid !== CODES.NONE) return answer({ errorCode: invalid })
    if (generationId !== this.generationId) return answer({ errorCode: CODES.ILLEGAL_GENERATION })
    if (
      inconsistent(request.protocolType, this.protocolType) ||
      inconsistent(request.protocolName, this.protocolName)
    ) {
      return answer({ errorCode: CODES.INCONSISTENT_GROUP_PROTOCOL })
    }
    if (this.state === STATES.PREPARING) return answer({ errorCode: CODES.REBALANCE_IN_PROGRESS })

    const member = this.members.get(memberId)
    if (this.state === STATES.STABLE) {
      this.pendingSync.delete(memberId)
      return answer(this.storedSyncAnswer(member))
    }

    const reply = deferred()
    if (member.sync) this.orphans.set(member.sync, syncAnswer)
    member.sync = reply
    this.pendingSync.delete(memberId)
    if (memberId === this.leaderId) {
      const plan = new Map(
        request.groupAssignment.map(({ memberId: id, memberAssignment }) => [id, memberAssignment])
      )
      for (const each of this.members.values()) {
        each.assignment = plan.get(each.memberId) ?? EMPTY_ASSIGNMENT
      }
      this.transitionTo(STATES.STABLE)
      for (const each of this.members.values()) {
        this.deliverSync(each, this.storedSyncAnswer(each))
      }
    }
    return reply.promise
  }

  /**
   * @private
   * @param {object} request a Heartbeat v4 request
   * @returns {{ errorCode: number }}
   */
  heartbeat({ memberId, groupInstanceId, groupGenerationId }) {
    if (!this.exists) return { errorCode: CODES.UNKNOWN_MEMBER_ID }
    const invalid = this.validateMember(memberId, groupInstanceId)
    if (invalid !== CODES.NONE) return { errorCode: invalid }
    if (groupGenerationId !== this.generationId) return { errorCode: CODES.ILLEGAL_GENERATION }
    if (this.state === STATES.EMPTY) return { errorCode: CODES.UNKNOWN_MEMBER_ID }
    if (this.state === STATES.PREPARING) return { errorCode: CODES.REBALANCE_IN_PROGRESS }
    return { errorCode: CODES.NONE }
  }

  /**
   * @private
   * @param {object} request a LeaveGroup v5 request
   * @returns {{ errorCode: number, members: object[] }}
   */
  leave({ members }) {
    if (!this.exists) return { errorCode: CODES.UNKNOWN_MEMBER_ID, members: [] }

    const answers = members.map(({ memberId, groupInstanceId }) => {
      const answer = (errorCode) => ({ memberId, groupInstanceId, errorCode })
      if (memberId === '') {
        const holder = groupInstanceId === null ? null : this.staticMembers.get(groupInstanceId)
        if (holder == null) return answer(CODES.UNKNOWN_MEMBER_ID)
        this.removeCurrentMember(holder)
        return answer(CODES.NONE)
      }
      if (this.pendingJoin.has(memberId)) {
        this.remove(memberId)
        return answer(CODES.NONE)
      }
      const invalid = this.validateMember(memberId, groupInstanceId)
      if (invalid !== CODES.NONE) return answer(invalid)
      this.removeCurrentMember(memberId)
      return answer(CODES.NONE)
    })

    if (answers.some(({ errorCode }) => errorCode === CODES.NONE)) {
      if (this.state === STATES.STABLE || this.state === STATES.COMPLETING) {
        this.maybePrepareRebalanceOrCompleteJoin()
      } else if (this.state === STATES.PREPARING) {
        this.maybeCompleteJoinPhase()
      }
    }
    return { errorCode: CODES.NONE, members: answers }
  }

  /**
   * `ClassicGroup.validateMember`: the instance must be held by this member id, and the member
   * must be in the group.
   * @private
   * @param {string} memberId
   * @param {string|null} groupInstanceId
   * @returns {number} an error code, NONE when valid
   */
  validateMember(memberId, groupInstanceId) {
    if (groupInstanceId !== null) {
      const holder = this.staticMembers.get(groupInstanceId)
      if (holder === undefined) return CODES.UNKNOWN_MEMBER_ID
      if (holder !== memberId) return CODES.FENCED_INSTANCE_ID
    }
    return this.members.has(memberId) ? CODES.NONE : CODES.UNKNOWN_MEMBER_ID
  }

  /**
   * @private
   * @param {{ protocolType: string, groupProtocols: { name: string }[] }} request
   * @returns {boolean} whether one offered protocol is supported by every member
   */
  supportsProtocols({ protocolType, groupProtocols }) {
    if (this.state === STATES.EMPTY) return protocolType !== '' && groupProtocols.length > 0
    return (
      protocolType === this.protocolType &&
      groupProtocols.some(({ name }) => this.candidates().has(name))
    )
  }

  /**
   * @private
   * @returns {Set<string>} the protocol names every member supports
   */
  candidates() {
    const [first, ...rest] = this.members.values()
    if (!first) return new Set()
    return new Set(
      first.protocols
        .map(({ name }) => name)
        .filter((name) => rest.every(({ protocols }) => protocols.some((p) => p.name === name)))
    )
  }

  /**
   * Each member votes for its first supported candidate; the most votes win.
   * @private
   * @returns {string}
   */
  selectProtocol() {
    const candidates = this.candidates()
    const votes = new Map()
    for (const { protocols } of this.members.values()) {
      const vote = protocols.find(({ name }) => candidates.has(name)).name
      votes.set(vote, (votes.get(vote) ?? 0) + 1)
    }
    let selected = null
    for (const [name, count] of votes) {
      if (selected === null || count > votes.get(selected)) selected = name
    }
    return selected
  }

  /**
   * @private
   * @param {object} request
   * @param {string} memberId
   * @param {{ resolve: Function }} reply
   * @returns {void}
   */
  addMember(request, memberId, reply) {
    const member = {
      memberId,
      groupInstanceId: request.groupInstanceId,
      protocols: request.groupProtocols,
      rebalanceTimeout: request.rebalanceTimeout,
      assignment: EMPTY_ASSIGNMENT,
      isNew: true,
      join: reply,
      sync: null,
    }
    if (this.state === STATES.PREPARING && this.previousState === STATES.EMPTY) {
      this.newMemberAdded = true
    }
    if (member.groupInstanceId !== null) this.staticMembers.set(member.groupInstanceId, memberId)
    if (this.members.size === 0) this.protocolType = request.protocolType
    if (this.leaderId === null) this.leaderId = memberId
    this.members.set(memberId, member)
    this.pendingJoin.delete(memberId)
    this.maybePrepareRebalanceOrCompleteJoin()
  }

  /**
   * @private
   * @param {object} member
   * @param {object} request
   * @param {{ resolve: Function }} reply
   * @returns {void}
   */
  updateMember(member, request, reply) {
    member.protocols = request.groupProtocols
    member.rebalanceTimeout = request.rebalanceTimeout
    if (member.join) this.orphans.set(member.join, joinAnswer)
    member.join = reply
  }

  /**
   * A static member joined with an empty member id: its old id is fenced and replaced.
   * @private
   * @param {object} request
   * @param {string} oldId
   * @param {string} newId
   * @param {{ resolve: Function }} reply
   * @returns {void}
   */
  replaceStaticMember(request, oldId, newId, reply) {
    const currentLeader = this.leaderId
    const old = this.members.get(oldId)
    this.members.delete(oldId)
    this.deliverJoin(
      old,
      joinAnswer({ memberId: oldId, errorCode: CODES.FENCED_INSTANCE_ID, leaderId: '' })
    )
    this.deliverSync(old, syncAnswer({ errorCode: CODES.FENCED_INSTANCE_ID }))
    const member = { ...old, memberId: newId, join: null, sync: null }
    this.members.set(newId, member)
    if (this.leaderId === oldId) this.leaderId = newId
    this.staticMembers.set(request.groupInstanceId, newId)
    this.updateMember(member, request, reply)

    if (this.state === STATES.STABLE) {
      if (this.protocolName === this.selectProtocol()) {
        // JoinGroup v8 cannot skip the assignment, so the answer names the leader before the
        // replacement and lists no members.
        this.deliverJoin(member, {
          ...this.currentJoinAnswer(member, false),
          leaderId: currentLeader,
        })
      } else {
        this.maybePrepareRebalanceOrCompleteJoin()
      }
    } else if (this.state === STATES.COMPLETING) {
      this.prepareRebalance()
    } else {
      this.maybeCompleteJoinPhase()
    }
  }

  /**
   * Kafka's `removeCurrentMemberFromClassicGroup`: a parked JoinGroup is answered 25 first.
   * @private
   * @param {string} memberId
   * @returns {void}
   */
  removeCurrentMember(memberId) {
    const member = this.members.get(memberId)
    this.deliverJoin(member, joinAnswer({ errorCode: CODES.UNKNOWN_MEMBER_ID }))
    if (member.sync) this.orphans.set(member.sync, syncAnswer)
    this.remove(memberId)
  }

  /**
   * @private
   * @param {string} memberId
   * @returns {void}
   */
  remove(memberId) {
    const member = this.members.get(memberId)
    if (member) {
      this.members.delete(memberId)
      if (member.groupInstanceId !== null) this.staticMembers.delete(member.groupInstanceId)
    }
    if (this.leaderId === memberId) {
      const [next] = this.members.keys()
      this.leaderId = next ?? null
    }
    this.pendingJoin.delete(memberId)
    this.pendingSync.delete(memberId)
  }

  /** @private @returns {void} */
  maybePrepareRebalanceOrCompleteJoin() {
    if (
      this.state === STATES.STABLE ||
      this.state === STATES.COMPLETING ||
      this.state === STATES.EMPTY
    ) {
      this.prepareRebalance()
    } else {
      this.maybeCompleteJoinPhase()
    }
  }

  /** @private @returns {void} */
  maybeCompleteJoinPhase() {
    if (
      this.state === STATES.PREPARING &&
      this.previousState !== STATES.EMPTY &&
      this.allJoined()
    ) {
      this.completeJoin()
    }
  }

  /**
   * Starts a rebalance: parked SyncGroups of a completing group are answered 27.
   * @private
   * @returns {void}
   */
  prepareRebalance() {
    if (this.state === STATES.COMPLETING) {
      for (const member of this.members.values()) {
        member.assignment = EMPTY_ASSIGNMENT
        this.deliverSync(member, syncAnswer({ errorCode: CODES.REBALANCE_IN_PROGRESS }))
      }
    }
    this.pendingSync.clear()
    const initial = this.state === STATES.EMPTY
    this.transitionTo(STATES.PREPARING)
    if (initial) {
      this.scheduleInitialRebalance(this.initialRebalanceDelay, this.rebalanceTimeout())
    } else if (this.allJoined()) {
      this.completeJoin()
    } else {
      this.scheduleJoinTimeout()
    }
  }

  /**
   * @private
   * @param {number} delay
   * @param {number} remaining
   * @returns {void}
   */
  scheduleInitialRebalance(delay, remaining) {
    this.schedule(delay, () => {
      if (this.newMemberAdded && remaining > 0) {
        this.newMemberAdded = false
        this.scheduleInitialRebalance(
          Math.min(this.initialRebalanceDelay, remaining),
          Math.max(remaining - delay, 0)
        )
      } else {
        this.completeJoin()
      }
    })
  }

  /** @private @returns {void} */
  scheduleJoinTimeout() {
    this.schedule(this.rebalanceTimeout(), () => this.completeJoin())
  }

  /**
   * Sets the join phase's one timer, replacing the previous one, as Kafka's join key does. A delay
   * of 0 fires on the next event-loop turn, so it needs no fake clock.
   * @private
   * @param {number} delay
   * @param {() => void} task
   * @returns {void}
   */
  schedule(delay, task) {
    this.cancelJoinTimer()
    const fire = () => {
      this.joinTimer = null
      task()
    }
    if (delay > 0) {
      const timer = setTimeout(fire, delay)
      this.joinTimer = () => clearTimeout(timer)
    } else {
      const immediate = setImmediate(fire)
      this.joinTimer = () => clearImmediate(immediate)
    }
  }

  /** @private @returns {void} */
  cancelJoinTimer() {
    if (this.joinTimer === null) return
    this.joinTimer()
    this.joinTimer = null
  }

  /** @private @returns {number} the largest rebalance timeout of the members */
  rebalanceTimeout() {
    let timeout = 0
    for (const { rebalanceTimeout } of this.members.values()) {
      timeout = Math.max(timeout, rebalanceTimeout)
    }
    return timeout
  }

  /** @private @returns {boolean} every member awaits its JoinGroup answer and none is pending */
  allJoined() {
    for (const member of this.members.values()) if (!member.join) return false
    return this.pendingJoin.size === 0
  }

  /**
   * Ends the join phase: dynamic members that did not rejoin are removed, the generation moves on
   * and every parked JoinGroup is answered.
   * @private
   * @returns {void}
   */
  completeJoin() {
    this.cancelJoinTimer()
    for (const member of Array.from(this.members.values())) {
      if (!member.join && member.groupInstanceId === null) this.remove(member.memberId)
    }
    if (this.members.size > 0 && !this.electJoinedLeader()) {
      this.scheduleJoinTimeout()
      return
    }

    this.generationId += 1
    this.pendingSync.clear()
    if (this.members.size === 0) {
      this.protocolName = null
      this.transitionTo(STATES.EMPTY)
      return
    }
    this.protocolName = this.selectProtocol()
    this.transitionTo(STATES.COMPLETING)
    for (const member of this.members.values()) {
      this.deliverJoin(member, this.currentJoinAnswer(member, true))
      member.isNew = false
      this.pendingSync.add(member.memberId)
    }
  }

  /**
   * @private
   * @returns {boolean} whether the leader, or failing it another member, awaits its JoinGroup
   */
  electJoinedLeader() {
    if (this.leaderId === null) return false
    if (this.members.get(this.leaderId).join) return true
    for (const member of this.members.values()) {
      if (member.join) {
        this.leaderId = member.memberId
        return true
      }
    }
    return false
  }

  /**
   * @private
   * @param {object} member
   * @param {boolean} withMembers whether a leader gets the member list
   * @returns {object} a JoinGroup answer for the current generation
   */
  currentJoinAnswer(member, withMembers) {
    const leads = withMembers && member.memberId === this.leaderId
    return joinAnswer({
      memberId: member.memberId,
      generationId: this.generationId,
      protocolType: this.protocolType,
      groupProtocol: this.protocolName,
      leaderId: this.leaderId,
      members: leads
        ? Array.from(this.members.values(), ({ memberId, groupInstanceId, protocols }) => ({
            memberId,
            groupInstanceId,
            memberMetadata: protocols.find(({ name }) => name === this.protocolName).metadata,
          }))
        : [],
    })
  }

  /**
   * @private
   * @param {object} member
   * @returns {object} the SyncGroup answer of the stored assignment
   */
  storedSyncAnswer(member) {
    return syncAnswer({
      protocolType: this.protocolType,
      protocolName: this.protocolName,
      memberAssignment: member.assignment,
    })
  }

  /**
   * @private
   * @param {string} state
   * @returns {void}
   */
  transitionTo(state) {
    this.previousState = this.state
    this.state = state
  }

  /**
   * @private
   * @param {object} member
   * @param {object} answer
   * @returns {void}
   */
  deliverJoin(member, answer) {
    if (!member.join) return
    member.join.resolve(answer)
    member.join = null
  }

  /**
   * @private
   * @param {object} member
   * @param {object} answer
   * @returns {void}
   */
  deliverSync(member, answer) {
    if (!member.sync) return
    member.sync.resolve(answer)
    member.sync = null
  }
}

/**
 * @param {{ name: string, metadata: Buffer }[]} held
 * @param {{ name: string, metadata: Buffer }[]} offered
 * @returns {boolean} whether a JoinGroup offers the same protocols, in the same order
 */
const matches = (held, offered) =>
  held.length === offered.length &&
  held.every(
    ({ name, metadata }, at) => name === offered[at].name && metadata.equals(offered[at].metadata)
  )

/**
 * @param {string|null} sent
 * @param {string|null} held
 * @returns {boolean} Kafka's `isProtocolInconsistent`
 */
const inconsistent = (sent, held) => sent !== null && held !== null && sent !== held

module.exports = { ClassicGroupCoordinator, CODES, STATES }
