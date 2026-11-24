/** Error codes the model writes (`src/protocol/error.js`). */
const INVALID_RECORD_STATE = 121
const SHARE_SESSION_NOT_FOUND = 122
const INVALID_SHARE_SESSION_EPOCH = 123
const SHARE_SESSION_LIMIT_REACHED = 133

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
 * @param {string|number} first
 * @param {string|number} last
 * @returns {string[]} every offset of the inclusive range, as int64 decimal strings
 */
const offsetsOf = (first, last) => {
  const offsets = []
  for (let offset = BigInt(first); offset <= BigInt(last); offset++) offsets.push(String(offset))
  return offsets
}

/**
 * @param {Array<{ topicId: string, partitions: Array<number|{ partitionIndex: number }> }>} topics
 * @returns {string[]} `topicId:partition` of every listed partition
 */
const partitionKeys = (topics = []) =>
  topics.flatMap(({ topicId, partitions }) =>
    partitions.map((entry) => `${topicId}:${entry.partitionIndex ?? entry}`)
  )

/**
 * A share-group broker's session, acquisition and membership state, kept as Kafka 4.2.0 keeps it,
 * behind a scripted cluster. It does not invent answers: the test still scripts what a ShareFetch
 * acquires. When a request executes, the model applies Kafka's rules to the scripted answer and
 * rewrites what the broker would refuse:
 *
 * - Sessions are keyed (node, member). An epoch-0 ShareFetch answered without an error replaces
 *   the member's session and opens one at epoch 1 on the request's connection; one answered 133
 *   removes the old session and opens nothing; any other error changes nothing. An ordinary
 *   request needs a session (else 122) at its epoch (else 123), and moves it to the next epoch. A
 *   final (epoch -1) needs some session (else 122), applies its acknowledgements, removes the
 *   session and releases the member's records on that node. A broker-side close of the connection
 *   a session was opened on does the same; a replaced session's old connection removes nothing.
 * - Records are keyed (topic id, partition, offset), owned by a member id; each acquisition has its
 *   own observation id. An acknowledgement applies only when every offset of the partition is
 *   acquired by the sending member, else the partition is answered 121 and nothing changes; renew
 *   (4) keeps the acquisition. Session identity and epoch are never compared, as on the broker.
 * - Joins (member epoch 0) and leaves (-1) of answered heartbeats are logged with their orders.
 *
 * A Buffer answer, a request of another group and an answer without `responses` change nothing.
 * Install it with `installSessionModel`, before the client writes anything.
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
    /** @type {Array<{ session: object, by: Stamp|null }>} every removal; `by` is null for a close */
    this.removals = []
    /** @type {Map<string, object>} the acquisition holding each `topicId:partition:offset` */
    this.current = new Map()
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
   * Applies a broker-side close: removes the sessions opened on that connection.
   * @param {{ node: number, generation: number, kind: 'client'|'broker' }} closed
   * @returns {void}
   */
  socketClosed({ node, generation, kind }) {
    if (kind !== 'broker') return
    const order = this.cluster.clock++
    for (const session of [...this.live.values()]) {
      if (session.node === node && session.opened.generation === generation) {
        this.remove(session, null)
        this.release(session.member, node, 'disconnect', order)
      }
    }
  }

  /**
   * The acquisition lock of one record expires: it becomes available again.
   * @param {string} topicId
   * @param {number} partition
   * @param {string|number} offset
   * @returns {void}
   * @throws {Error} when the record is not acquired
   */
  expireLock(topicId, partition, offset) {
    const key = `${topicId}:${partition}:${offset}`
    const acquisition = this.current.get(key)
    if (!acquisition) throw new Error(`ShareSessionModel: ${key} is not acquired`)
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
   *   every acquisition of the record, oldest first; `state` is `ACQUIRED` or its last change
   */
  acquisitionsOf(topicId, partition, offset) {
    const key = `${topicId}:${partition}:${String(offset)}`
    return this.observations
      .filter((acquisition) => acquisition.key === key)
      .map(({ observationId, member, node, changes, renewals }) => ({
        observationId,
        member,
        node,
        state: changes.length ? changes[changes.length - 1].state : 'ACQUIRED',
        renewals,
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
    const { memberId, shareSessionEpoch: epoch } = record.fields
    const fetch = record.api === 'ShareFetch'
    const key = `${record.node}|${memberId}`
    const session = this.live.get(key)
    const stamp = stampOf(record)

    if (fetch && epoch === 0) {
      if (answer.errorCode && answer.errorCode !== SHARE_SESSION_LIMIT_REACHED) return answer
      if (session) this.remove(session, stamp)
      if (answer.errorCode) return answer
      this.live.set(key, {
        node: record.node,
        member: memberId,
        epoch: 1,
        opened: stamp,
        partitions: new Set(partitionKeys(record.fields.topics)),
      })
      this.acquire(record, answer)
      return answer
    }

    const refusal =
      epoch === 0
        ? INVALID_SHARE_SESSION_EPOCH
        : !session
          ? SHARE_SESSION_NOT_FOUND
          : epoch !== -1 && epoch !== session.epoch
            ? INVALID_SHARE_SESSION_EPOCH
            : 0
    if (refusal) return { ...answer, errorCode: refusal, errorMessage: null, responses: [] }
    if (answer.errorCode) return answer

    const answered = this.acknowledge(record, answer)
    if (epoch === -1) {
      this.remove(session, stamp)
      this.release(memberId, record.node, 'final', record.executedOrder)
      return answered
    }
    session.epoch += 1
    if (fetch) {
      for (const partition of partitionKeys(record.fields.topics)) session.partitions.add(partition)
      for (const partition of partitionKeys(record.fields.forgottenTopicsData)) {
        session.partitions.delete(partition)
      }
      this.acquire(record, answered)
    }
    return answered
  }

  /**
   * Applies the request's acknowledgements, partition by partition, all or nothing.
   * @private
   * @param {object} record
   * @param {object} answer
   * @returns {object} the answer; a copy with a 121 row for each refused partition
   */
  acknowledge(record, answer) {
    const fetch = record.api === 'ShareFetch'
    const refused = []
    for (const { topicId, partitions } of record.fields.topics) {
      for (const { partitionIndex, acknowledgementBatches } of partitions) {
        if (!acknowledgementBatches?.length) continue
        const row = answer.responses
          ?.find((topic) => topic.topicId === topicId)
          ?.partitions.find((partition) => partition.partitionIndex === partitionIndex)
        if (row && (fetch ? row.acknowledgeErrorCode : row.errorCode)) continue

        const steps = acknowledgementBatches.flatMap(
          ({ firstOffset, lastOffset, acknowledgeTypes }) =>
            offsetsOf(firstOffset, lastOffset).map((offset, i) => ({
              acquisition: this.current.get(`${topicId}:${partitionIndex}:${offset}`),
              type: acknowledgeTypes.length === 1 ? acknowledgeTypes[0] : acknowledgeTypes[i],
            }))
        )
        if (steps.some(({ acquisition }) => acquisition?.member !== record.fields.memberId)) {
          refused.push({ topicId, partitionIndex })
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
   * @private
   * @param {object} answer
   * @param {Array<{ topicId: string, partitionIndex: number }>} refused
   * @param {boolean} fetch whether the answer is a ShareFetch answer
   * @returns {object} a copy of the answer with code 121 on each refused partition's row
   */
  refuse(answer, refused, fetch) {
    const field = fetch ? 'acknowledgeErrorCode' : 'errorCode'
    const responses = (answer.responses ?? []).map((topic) => ({
      ...topic,
      partitions: topic.partitions.map((partition) => ({ ...partition })),
    }))
    for (const { topicId, partitionIndex } of refused) {
      let topic = responses.find((entry) => entry.topicId === topicId)
      if (!topic) responses.push((topic = { topicId, partitions: [] }))
      let row = topic.partitions.find((entry) => entry.partitionIndex === partitionIndex)
      if (!row) topic.partitions.push((row = this.quietRow(partitionIndex, fetch)))
      row[field] = INVALID_RECORD_STATE
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
   * Records what a ShareFetch answer acquires for the member.
   * @private
   * @param {object} record
   * @param {object} answer
   * @returns {void}
   * @throws {Error} into the cluster's failures when an offset is acquired while it is held
   */
  acquire(record, answer) {
    for (const { topicId, partitions } of answer.responses ?? []) {
      for (const { partitionIndex, errorCode, acquiredRecords } of partitions) {
        if (errorCode) continue
        for (const { firstOffset, lastOffset, deliveryCount } of acquiredRecords ?? []) {
          for (const offset of offsetsOf(firstOffset, lastOffset)) {
            const key = `${topicId}:${partitionIndex}:${offset}`
            if (this.current.has(key)) {
              this.cluster.errors.push(
                new Error(`ShareSessionModel: ${key} acquired while it is still acquired`)
              )
              continue
            }
            const acquisition = {
              observationId: this.observations.length + 1,
              key,
              member: record.fields.memberId,
              node: record.node,
              deliveryCount,
              renewals: 0,
              changes: [],
            }
            this.observations.push(acquisition)
            this.current.set(key, acquisition)
          }
        }
      }
    }
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
    this.current.delete(acquisition.key)
  }

  /**
   * Makes every record the member holds through the node available again.
   * @private
   * @param {string} member
   * @param {number} node
   * @param {'final'|'disconnect'} cause
   * @param {number} order
   * @returns {void}
   */
  release(member, node, cause, order) {
    for (const acquisition of [...this.current.values()]) {
      if (acquisition.member === member && acquisition.node === node) {
        this.settle(acquisition, 'AVAILABLE', cause, order)
      }
    }
  }

  /**
   * @private
   * @param {object} session
   * @param {Stamp|null} by the removing request, or null for a connection close
   * @returns {void}
   */
  remove(session, by) {
    this.live.delete(`${session.node}|${session.member}`)
    this.removals.push({ session, by })
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
