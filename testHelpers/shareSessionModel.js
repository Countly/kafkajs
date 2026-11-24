/** Error codes the model writes (`src/protocol/error.js`). */
const UNKNOWN_TOPIC_OR_PARTITION = 3
const INVALID_REQUEST = 42
const INVALID_RECORD_STATE = 121
const SHARE_SESSION_NOT_FOUND = 122
const INVALID_SHARE_SESSION_EPOCH = 123
const SHARE_SESSION_LIMIT_REACHED = 133
/** Top-level codes a script may only give where the broker would give them too. */
const SESSION_CODES = [
  SHARE_SESSION_NOT_FOUND,
  INVALID_SHARE_SESSION_EPOCH,
  SHARE_SESSION_LIMIT_REACHED,
]

const FINAL_EPOCH = -1
const INT32_MAX = 2147483647

/** What each terminal acknowledgement type leaves a record in; type 4 renews instead. */
const OUTCOMES = Object.freeze({
  0: ['ARCHIVED', 'gap'],
  1: ['ACKNOWLEDGED', 'accept'],
  2: ['AVAILABLE', 'release'],
  3: ['ARCHIVED', 'reject'],
})
const RENEW = 4

/**
 * @typedef {object} Stamp What the model keeps of a request.
 * @property {string} api
 * @property {number} node
 * @property {number} generation
 * @property {number} correlationId
 * @property {number} order where it was written
 * @property {number} executedOrder where it executed, on the same counter
 */

/**
 * @typedef {object} Change One change of an acquired record.
 * @property {'ACKNOWLEDGED'|'AVAILABLE'|'ARCHIVED'} state what the record became
 * @property {'accept'|'release'|'reject'|'gap'|'final'|'disconnect'|'expiry'} cause
 * @property {number} order where it happened, on the cluster's counter
 */

/**
 * @param {object} record a request record of the scripted cluster
 * @returns {Stamp}
 */
const stampOf = ({ api, node, generation, correlationId, order, executedOrder }) => ({
  api,
  node,
  generation,
  correlationId,
  order,
  executedOrder,
})

/**
 * @param {number} epoch
 * @returns {number} the epoch after it, wrapping as Kafka's `ShareRequestMetadata.nextEpoch`
 */
const nextEpoch = (epoch) => (epoch === INT32_MAX ? 1 : epoch + 1)

/**
 * @param {Array<{ topicId: string, partitions: Array<number|{ partitionIndex: number }> }>} topics
 * @returns {string[]} `topicId:partition` of every listed partition
 */
const partitionKeys = (topics = []) =>
  topics.flatMap(({ topicId, partitions }) =>
    partitions.map((entry) => `${topicId}:${entry.partitionIndex ?? entry}`)
  )

/**
 * @param {object} acquisition
 * @returns {'ACQUIRED'|Change['state']} what the acquired record is now
 */
const stateOf = ({ changes }) => (changes.length ? changes[changes.length - 1].state : 'ACQUIRED')

/**
 * Kafka's `KafkaApis#validateAcknowledgementBatches`.
 * @param {Array<{ firstOffset: string, lastOffset: string, acknowledgeTypes: number[] }>} batches
 * @param {number} maxType 4 at version 2, else 3
 * @param {boolean} isRenewAck
 * @returns {number} 42 when a batch breaks a rule, else 0
 */
const invalidBatches = (batches, maxType, isRenewAck) => {
  let previousLast = -1n
  for (const { firstOffset, lastOffset, acknowledgeTypes: types } of batches) {
    const first = BigInt(firstOffset)
    const last = BigInt(lastOffset)
    if (
      first > last ||
      first < previousLast ||
      !types?.length ||
      (types.length > 1 && last - first !== BigInt(types.length - 1)) ||
      types.some((type) => type < 0 || type > maxType) ||
      (types.includes(RENEW) && !isRenewAck)
    ) {
      return INVALID_REQUEST
    }
    previousLast = last
  }
  return 0
}

/**
 * A share-group broker's session, acquisition and membership state, kept as Apache Kafka 4.3.1
 * keeps it, behind a scripted cluster. It does not invent answers: the test still scripts what a
 * ShareFetch acquires. When a request executes, the model applies Kafka's rules to the scripted
 * answer and rewrites what the broker would refuse:
 *
 * - Sessions are keyed (node, member). A scripted top-level error other than 122, 123 and 133 is
 *   a refusal before admission: the answer passes unchanged and nothing changes. A scripted 122 or
 *   123 that differs from the model's own verdict, and a 133 anywhere but an admitted epoch-0
 *   ShareFetch, is a recorded failure. An epoch-0 ShareFetch carrying acknowledgements, and a
 *   renewing ShareFetch asking for records, are answered 42. An epoch-0 ShareFetch replaces the
 *   member's session and opens one at epoch 1 on its connection; answered 133 it removes the old
 *   session and opens nothing. ShareAcknowledge at epoch 0 is answered 123; any other request needs
 *   a session (else 122) at its epoch (else 123) and moves it to the next epoch, after 2^31-1
 *   to 1. A final (epoch -1) needs a session (else 122), applies its acknowledgements, then
 *   removes the member's session and releases the member's acquired records on the partitions
 *   that session caches. A broker-side close of a connection does the same for the session last
 *   opened on it, unless a removal since marked that connection stale.
 * - A ShareFetch fetches its effective set: the request's partitions at epoch 0, the session's
 *   cached partitions after the update otherwise, nothing for a final or a renewing fetch. An
 *   answer that acquires outside it, or acquires a record that is acquired, acknowledged or
 *   archived, is a recorded failure and acquires nothing there.
 * - Records are kept per node and partition, as each broker keeps its share partitions; a node has
 *   a share partition once a fetch there fetched it. Acknowledgements are checked partition by
 *   partition, all or nothing: 42 for a batch that breaks Kafka's batch rules or ends past the
 *   cached records, 121 for a batch that starts below every cached record or covers a cached
 *   record the sending member does not hold acquired, 3 when the node has no share partition.
 *   Renew (4) keeps the acquisition. Session identity and epoch are never compared, as on the
 *   broker. `expireLock(observationId)` is the scheduled lock expiry.
 * - Joins (member epoch 0) and leaves (-1) of answered heartbeats are logged with their orders.
 *
 * Not modelled: persistence, the log start offset and its archiving, delivery-count exhaustion,
 * renewal timers, the group-config renew switch, `onGroupEmpty`, partition leadership, Kafka's
 * batch-level checks on a cached batch only partly inside an acknowledged range, and the interleaving
 * of a final's acknowledgements with its release (one execution step here). A Buffer answer and a
 * request of another group pass through untouched. Install it with `installSessionModel`, before
 * the client writes anything.
 */
class ShareSessionModel {
  /**
   * @param {object} [options]
   * @param {string} [options.group='group'] the share group modelled; others pass through
   */
  constructor({ group = 'group' } = {}) {
    this.group = group
    this.cluster = null
    /** @type {Map<string, object>} live sessions by `node|member` */
    this.live = new Map()
    /** @type {Map<string, { key: string, stale: boolean }>} session last opened per `node/generation` */
    this.connections = new Map()
    /** @type {Array<{ session: object, by: Stamp|null }>} every removal; `by` is null for a close */
    this.removals = []
    /** @type {Map<string, Map<string, object>>} per `node|topicId:partition`, the last acquisition of each offset */
    this.partitions = new Map()
    /** @type {object[]} every acquisition, observation id - 1 */
    this.observations = []
    /** @type {Array<{ kind: 'join'|'leave', member: string } & Stamp>} */
    this.memberships = []
  }

  /**
   * Hooks the model into a scripted cluster's execution and socket closes.
   * @param {import('./scriptedCluster').ScriptedCluster} cluster
   * @returns {ShareSessionModel} this
   * @throws {Error} when the cluster already has hooks
   */
  install(cluster) {
    this.cluster = cluster
    cluster.hook({
      onExecute: (record, answer) => this.execute(record, answer),
      onSocketClosed: (closed) => this.socketClosed(closed),
    })
    return this
  }

  /**
   * Applies one executed request.
   * @param {object} record the cluster's request record, `executedOrder` stamped
   * @param {object|Buffer} answer what the cluster would send
   * @returns {object|Buffer} the answer the broker sends
   */
  execute(record, answer) {
    if (Buffer.isBuffer(answer) || record.fields?.groupId !== this.group) return answer
    if (record.api === 'ShareGroupHeartbeat') this.beat(record, answer)
    if (record.api === 'ShareFetch' || record.api === 'ShareAcknowledge') {
      return this.share(record, answer)
    }
    return answer
  }

  /**
   * Applies a broker-side close: removes the session last opened on that connection, unless the
   * connection was marked stale, and releases its member's records.
   * @param {{ node: number, generation: number, kind: 'client'|'broker' }} closed
   * @returns {void}
   */
  socketClosed({ node, generation, kind }) {
    if (kind !== 'broker') return
    const connection = `${node}/${generation}`
    const mapping = this.connections.get(connection)
    this.connections.delete(connection)
    if (!mapping || mapping.stale) return
    const session = this.live.get(mapping.key)
    if (!session) return
    const order = this.cluster.clock++
    this.remove(session, null)
    this.release(session, 'disconnect', order)
  }

  /**
   * The acquisition lock of one acquisition expires: the record becomes available again.
   * @param {number} observationId the acquisition, as `acquisitionsOf` lists it
   * @returns {void}
   * @throws {Error} for an unknown observation id, or one no longer acquired (a stale expiry)
   */
  expireLock(observationId) {
    const acquisition = this.observations[observationId - 1]
    if (!acquisition) throw new Error(`ShareSessionModel: no observation ${observationId}`)
    if (stateOf(acquisition) !== 'ACQUIRED') {
      throw new Error(`ShareSessionModel: observation ${observationId} is no longer acquired`)
    }
    this.settle(acquisition, 'AVAILABLE', 'expiry', this.cluster.clock++)
  }

  /**
   * @param {number} node
   * @param {string} member
   * @returns {{ node: number, member: string, epoch: number, generation: number, partitions: string[] }|null}
   *   the member's live session on the node: the epoch it expects next, the connection it was
   *   opened on and its `topicId:partition` keys
   */
  session(node, member) {
    const session = this.live.get(`${node}|${member}`)
    return session ? this.view(session) : null
  }

  /** @returns {Array<ReturnType<ShareSessionModel['session']>>} every live session */
  sessions() {
    return [...this.live.values()].map((session) => this.view(session))
  }

  /**
   * @returns {Array<{ session: object, by: Stamp }>} sessions removed by a request written before
   *   the session's opening request was written and executed after that request executed
   */
  lateRemovals() {
    return this.removals
      .filter(
        ({ session, by }) =>
          by && by.order < session.opened.order && by.executedOrder > session.opened.executedOrder
      )
      .map(({ session, by }) => ({ session: this.view(session), by }))
  }

  /**
   * @returns {Array<{ leave: Stamp, join: Stamp }>} leaves written before a join of the same member
   *   and executed after it
   */
  lateLeaves() {
    const joins = this.memberships.filter(({ kind }) => kind === 'join')
    return this.memberships
      .filter(({ kind }) => kind === 'leave')
      .flatMap((leave) =>
        joins
          .filter(
            (join) =>
              join.member === leave.member &&
              leave.order < join.order &&
              leave.executedOrder > join.executedOrder
          )
          .map((join) => ({ leave, join }))
      )
  }

  /**
   * @param {number} observationId
   * @returns {Change[]} what happened to that acquisition, oldest first
   * @throws {Error} for an unknown observation id
   */
  changesTo(observationId) {
    const acquisition = this.observations[observationId - 1]
    if (!acquisition) throw new Error(`ShareSessionModel: no observation ${observationId}`)
    return acquisition.changes.map((change) => ({ ...change }))
  }

  /**
   * @param {string} topicId
   * @param {number} partition
   * @param {string|number} offset
   * @returns {Array<{ observationId: number, member: string, node: number, state: string, renewals: number }>}
   *   every acquisition of the record on any node, oldest first; `state` is `ACQUIRED` or its last
   *   change
   */
  acquisitionsOf(topicId, partition, offset) {
    const key = `${topicId}:${partition}:${String(offset)}`
    return this.observations
      .filter((acquisition) => acquisition.key === key)
      .map((acquisition) => ({
        observationId: acquisition.observationId,
        member: acquisition.member,
        node: acquisition.node,
        state: stateOf(acquisition),
        renewals: acquisition.renewals,
      }))
  }

  /**
   * @param {{ executedOrder?: number }} a a request record
   * @param {{ executedOrder?: number }} b a request record
   * @returns {boolean} whether `a` executed after `b`
   * @throws {Error} when either has not executed
   */
  executedAfter(a, b) {
    if (a.executedOrder === undefined || b.executedOrder === undefined) {
      throw new Error('ShareSessionModel: both requests must have executed')
    }
    return a.executedOrder > b.executedOrder
  }

  /**
   * @private
   * @param {object} record a ShareGroupHeartbeat record
   * @param {object} answer
   * @returns {void}
   */
  beat(record, answer) {
    if (answer.errorCode) return
    const { memberId, memberEpoch } = record.fields
    const kind = memberEpoch === 0 ? 'join' : memberEpoch === -1 ? 'leave' : null
    if (kind) this.memberships.push({ kind, member: memberId, ...stampOf(record) })
  }

  /**
   * @private
   * @param {object} record a ShareFetch or ShareAcknowledge record
   * @param {object} answer
   * @returns {object} the answer, with the refusals the broker would write
   */
  share(record, answer) {
    const fetch = record.api === 'ShareFetch'
    const { memberId, shareSessionEpoch: epoch } = record.fields
    const key = `${record.node}|${memberId}`
    const scripted = answer.errorCode ?? 0
    if (scripted && !SESSION_CODES.includes(scripted)) return answer

    const verdict = this.admission(record, fetch, this.live.get(key))
    if (scripted === SHARE_SESSION_LIMIT_REACHED && fetch && epoch === 0 && !verdict) {
      this.drop(key, stampOf(record))
      return answer
    }
    if (scripted && scripted !== verdict) {
      this.fail(`${record.api} answered ${scripted} where the broker answers ${verdict}`)
      return answer
    }
    if (verdict) return { ...answer, errorCode: verdict, errorMessage: null, responses: [] }

    const fetchSet = this.admit(record, fetch, key)
    const answered = this.acknowledge(record, answer, fetch)
    if (fetch) this.acquire(record, answered, fetchSet)
    if (epoch === FINAL_EPOCH) this.closeSession(key, stampOf(record), record.executedOrder)
    return answered
  }

  /**
   * The broker's refusal of a request before any state changes, in Kafka's order.
   * @private
   * @param {object} record
   * @param {boolean} fetch
   * @param {object|undefined} session the member's session on the node
   * @returns {number} the top-level error code, or 0 when the broker admits it
   */
  admission({ version, fields }, fetch, session) {
    const { shareSessionEpoch: epoch } = fields
    if (fetch) {
      const { isRenewAck, maxBytes, minBytes, maxRecords, maxWaitMs } = fields
      if (version >= 2 && isRenewAck && (maxBytes || minBytes || maxRecords || maxWaitMs)) {
        return INVALID_REQUEST
      }
      if (epoch === 0) {
        const acknowledging = fields.topics.some(({ partitions }) =>
          partitions.some(({ acknowledgementBatches }) => acknowledgementBatches?.length)
        )
        return acknowledging ? INVALID_REQUEST : 0
      }
    } else if (epoch === 0) {
      return INVALID_SHARE_SESSION_EPOCH
    }
    if (!session) return SHARE_SESSION_NOT_FOUND
    return epoch === FINAL_EPOCH || epoch === session.epoch ? 0 : INVALID_SHARE_SESSION_EPOCH
  }

  /**
   * Opens or updates the member's session for an admitted request.
   * @private
   * @param {object} record
   * @param {boolean} fetch
   * @param {string} key `node|member`
   * @returns {Set<string>} the `topicId:partition` keys a ShareFetch fetches
   */
  admit(record, fetch, key) {
    const { node, generation, fields } = record
    if (fetch && fields.shareSessionEpoch === 0) {
      this.drop(key, stampOf(record))
      const session = {
        node,
        member: fields.memberId,
        epoch: 1,
        opened: stampOf(record),
        connection: `${node}/${generation}`,
        partitions: new Set(partitionKeys(fields.topics)),
      }
      this.live.set(key, session)
      this.connections.set(session.connection, { key, stale: false })
      return new Set(session.partitions)
    }
    if (fields.shareSessionEpoch === FINAL_EPOCH) return new Set()

    const session = this.live.get(key)
    session.epoch = nextEpoch(session.epoch)
    if (!fetch) return new Set()
    for (const partition of partitionKeys(fields.topics)) session.partitions.add(partition)
    for (const partition of partitionKeys(fields.forgottenTopicsData)) {
      session.partitions.delete(partition)
    }
    return record.version >= 2 && fields.isRenewAck ? new Set() : new Set(session.partitions)
  }

  /**
   * Applies the request's acknowledgements, partition by partition, all or nothing.
   * @private
   * @param {object} record
   * @param {object} answer
   * @param {boolean} fetch
   * @returns {object} the answer; a copy with the refusal code on each refused partition's row
   */
  acknowledge(record, answer, fetch) {
    const { memberId, isRenewAck, topics } = record.fields
    const maxType = record.version === 2 ? RENEW : 3
    const refused = []
    for (const { topicId, partitions } of topics) {
      for (const { partitionIndex, acknowledgementBatches: batches } of partitions) {
        if (!batches?.length) continue
        const row = answer.responses
          ?.find((topic) => topic.topicId === topicId)
          ?.partitions.find((partition) => partition.partitionIndex === partitionIndex)
        if (row && (fetch ? row.acknowledgeErrorCode : row.errorCode)) continue

        const partition = `${topicId}:${partitionIndex}`
        const { code, steps } = invalidBatches(batches, maxType, isRenewAck)
          ? { code: INVALID_REQUEST }
          : this.check(record.node, partition, memberId, batches)
        if (code) {
          refused.push({ topicId, partitionIndex, code })
          continue
        }
        for (const { acquisition, type } of steps) {
          if (type === RENEW) acquisition.renewals += 1
          else this.settle(acquisition, ...OUTCOMES[type], record.executedOrder)
        }
      }
    }
    return refused.length ? this.refuse(answer, refused, fetch) : answer
  }

  /**
   * Kafka's `SharePartition#acknowledge` checks over one node's share partition, before any change.
   * @private
   * @param {number} node
   * @param {string} partition `topicId:partition`
   * @param {string} member
   * @param {Array<{ firstOffset: string, lastOffset: string, acknowledgeTypes: number[] }>} batches
   * @returns {{ code: number, steps?: Array<{ acquisition: object, type: number }> }}
   */
  check(node, partition, member, batches) {
    const cached = this.partitions.get(`${node}|${partition}`)
    if (!cached) return { code: UNKNOWN_TOPIC_OR_PARTITION }
    const offsets = [...cached.keys()].map(BigInt).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))
    const steps = []
    const transitioning = new Set()
    for (const { firstOffset, lastOffset, acknowledgeTypes: types } of batches) {
      const first = BigInt(firstOffset)
      const last = BigInt(lastOffset)
      if (!offsets.some((offset) => offset <= first)) return { code: INVALID_RECORD_STATE }
      if (!cached.has(String(last))) return { code: INVALID_REQUEST }
      for (const offset of offsets.filter((candidate) => candidate >= first && candidate <= last)) {
        const acquisition = cached.get(String(offset))
        const type = types.length === 1 ? types[0] : types[Number(offset - first)]
        if (
          stateOf(acquisition) !== 'ACQUIRED' ||
          acquisition.member !== member ||
          transitioning.has(offset)
        ) {
          return { code: INVALID_RECORD_STATE }
        }
        if (type !== RENEW) transitioning.add(offset)
        steps.push({ acquisition, type })
      }
    }
    return { code: 0, steps }
  }

  /**
   * @private
   * @param {object} answer
   * @param {Array<{ topicId: string, partitionIndex: number, code: number }>} refused
   * @param {boolean} fetch whether the answer is a ShareFetch answer
   * @returns {object} a copy of the answer with each refused partition's code on its row
   */
  refuse(answer, refused, fetch) {
    const field = fetch ? 'acknowledgeErrorCode' : 'errorCode'
    const responses = (answer.responses ?? []).map((topic) => ({
      ...topic,
      partitions: topic.partitions.map((partition) => ({ ...partition })),
    }))
    for (const { topicId, partitionIndex, code } of refused) {
      let topic = responses.find((entry) => entry.topicId === topicId)
      if (!topic) responses.push((topic = { topicId, partitions: [] }))
      let row = topic.partitions.find((entry) => entry.partitionIndex === partitionIndex)
      if (!row) topic.partitions.push((row = this.quietRow(partitionIndex, fetch)))
      row[field] = code
    }
    return { ...answer, responses }
  }

  /**
   * @private
   * @param {number} partitionIndex
   * @param {boolean} fetch
   * @returns {object} a complete partition row with no error and no records
   */
  quietRow(partitionIndex, fetch) {
    const base = {
      partitionIndex,
      errorCode: 0,
      errorMessage: null,
      currentLeader: { leaderId: -1, leaderEpoch: -1 },
    }
    return fetch
      ? {
          ...base,
          acknowledgeErrorCode: 0,
          acknowledgeErrorMessage: null,
          acquiredRecords: [],
          records: '',
        }
      : base
  }

  /**
   * Records what a ShareFetch answer acquires for the member on the node, within the fetch set.
   * @private
   * @param {object} record
   * @param {object} answer
   * @param {Set<string>} fetchSet the `topicId:partition` keys the request fetches
   * @returns {void}
   */
  acquire(record, answer, fetchSet) {
    const { node, fields } = record
    const failed = new Set()
    for (const { topicId, partitions } of answer.responses ?? []) {
      for (const { partitionIndex, errorCode, acquiredRecords } of partitions) {
        const partition = `${topicId}:${partitionIndex}`
        if (errorCode) {
          failed.add(partition)
          continue
        }
        if (!acquiredRecords?.length) continue
        if (!fetchSet.has(partition)) {
          this.fail(
            `ShareFetch at epoch ${fields.shareSessionEpoch} acquired ${partition} outside its fetch set`
          )
          continue
        }
        const cached = this.cachedOf(node, partition)
        for (const { firstOffset, lastOffset } of acquiredRecords) {
          for (let offset = BigInt(firstOffset); offset <= BigInt(lastOffset); offset++) {
            const held = cached.get(String(offset))
            if (held && stateOf(held) !== 'AVAILABLE') {
              this.fail(`${node}|${partition}:${offset} acquired while ${stateOf(held)}`)
              continue
            }
            const acquisition = {
              observationId: this.observations.length + 1,
              key: `${partition}:${offset}`,
              member: fields.memberId,
              node,
              renewals: 0,
              changes: [],
            }
            this.observations.push(acquisition)
            cached.set(String(offset), acquisition)
          }
        }
      }
    }
    for (const partition of fetchSet) if (!failed.has(partition)) this.cachedOf(node, partition)
  }

  /**
   * @private
   * @param {number} node
   * @param {string} partition `topicId:partition`
   * @returns {Map<string, object>} the node's share partition, created when missing
   */
  cachedOf(node, partition) {
    const key = `${node}|${partition}`
    let cached = this.partitions.get(key)
    if (!cached) this.partitions.set(key, (cached = new Map()))
    return cached
  }

  /**
   * @private
   * @param {object} acquisition
   * @param {Change['state']} state
   * @param {Change['cause']} cause
   * @param {number} order
   * @returns {void}
   */
  settle(acquisition, state, cause, order) {
    acquisition.changes.push({ state, cause, order })
  }

  /**
   * Kafka's `releaseSession`: removes the session the key holds now and releases its member's
   * acquired records on the partitions it caches.
   * @private
   * @param {string} key `node|member`
   * @param {Stamp} by the final request
   * @param {number} order
   * @returns {void}
   */
  closeSession(key, by, order) {
    const session = this.live.get(key)
    if (!session) return
    this.remove(session, by)
    this.release(session, 'final', order)
  }

  /**
   * @private
   * @param {object} session a removed session
   * @param {'final'|'disconnect'} cause
   * @param {number} order
   * @returns {void}
   */
  release({ node, member, partitions }, cause, order) {
    for (const partition of partitions) {
      for (const acquisition of this.partitions.get(`${node}|${partition}`)?.values() ?? []) {
        if (acquisition.member === member && stateOf(acquisition) === 'ACQUIRED') {
          this.settle(acquisition, 'AVAILABLE', cause, order)
        }
      }
    }
  }

  /**
   * @private
   * @param {string} key `node|member`
   * @param {Stamp} by
   * @returns {void}
   */
  drop(key, by) {
    const session = this.live.get(key)
    if (session) this.remove(session, by)
  }

  /**
   * Removes a session and, as Kafka's `ShareSessionCache#remove`, marks its connection stale.
   * @private
   * @param {object} session
   * @param {Stamp|null} by the removing request, or null for a connection close
   * @returns {void}
   */
  remove(session, by) {
    this.live.delete(`${session.node}|${session.member}`)
    this.removals.push({ session, by })
    const mapping = this.connections.get(session.connection)
    if (mapping) mapping.stale = true
  }

  /**
   * @private
   * @param {string} message
   * @returns {void}
   */
  fail(message) {
    this.cluster.errors.push(new Error(`ShareSessionModel: ${message}`))
  }

  /**
   * @private
   * @param {object} session
   * @returns {{ node: number, member: string, epoch: number, generation: number, partitions: string[] }}
   */
  view({ node, member, epoch, opened, partitions }) {
    return { node, member, epoch, generation: opened.generation, partitions: [...partitions] }
  }
}

/**
 * Puts a `ShareSessionModel` behind a share harness's scripted cluster.
 * @param {{ cluster: import('./scriptedCluster').ScriptedCluster }} h a harness from `harness` or
 *   `shareClient`, before the client wrote anything
 * @param {object} [options] the model's options
 * @returns {ShareSessionModel} the installed model
 */
const installSessionModel = (h, options) => new ShareSessionModel(options).install(h.cluster)

module.exports = { ShareSessionModel, installSessionModel }
