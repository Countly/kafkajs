const { fetchResponse, TOPIC, TOPIC_ID } = require('./fetchHarness')

const NODE = { id: 0, host: 'node', port: 9092 }
const NEW_TOPIC_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'
const UNKNOWN_TOPIC_ID = 100
const SESSION_ID = 7
const SESSION_NOT_FOUND = 70

const INCARNATIONS = Object.freeze({
  [TOPIC_ID]: 'incarnation-a',
  [NEW_TOPIC_ID]: 'incarnation-b',
})

/**
 * The remote half of a schedule episode: a scripted broker's one topic with one record per
 * partition at offset 0, the group's committed offsets keyed by topic id, and every offset commit
 * it accepted, decoded from the frames the client wrote. The topic can be recreated under a new id
 * while keeping the same numeric offsets, and faults can be queued for the next heartbeat.
 */
class ScheduleBroker {
  /**
   * @param {object} options
   * @param {number} options.partitions how many partitions the topic has
   * @param {boolean} options.sessions whether Fetch answers carry a session id
   */
  constructor({ partitions, sessions }) {
    this.partitions = partitions
    this.sessions = sessions
    this.topicId = TOPIC_ID
    this.committed = new Map()
    /** @type {Array<{ incarnation: string, partition: number, offset: string, order: number }>} */
    this.commits = []
    this.fetches = []
    this.faults = []
    /** @type {number[]} the log order of each heartbeat a queued fault was served to */
    this.faultOrders = []
    this.metadataRequests = 0
    this.session = null
  }

  /** @returns {string} the label of the topic's current incarnation */
  incarnation() {
    return INCARNATIONS[this.topicId]
  }

  /**
   * Recreates the topic: a new id, nothing committed under it, the same numeric offsets.
   * @returns {void}
   */
  recreate() {
    this.topicId = NEW_TOPIC_ID
  }

  /**
   * Queues an error for the next heartbeat the client sends.
   * @param {number} errorCode
   * @returns {void}
   */
  failNextHeartbeat(errorCode) {
    this.faults.push(errorCode)
  }

  /**
   * @param {string} topicId
   * @param {number} partition
   * @returns {string} the committed offset the group holds, `-1` for none
   */
  committedOf(topicId, partition) {
    return this.committed.get(`${topicId}|${partition}`) ?? '-1'
  }

  /** @returns {object} the Metadata answer for the current topic id */
  metadata(nodes = [NODE]) {
    const partitions = Array.from({ length: this.partitions }, (_, partitionIndex) => ({
      errorCode: 0,
      partitionIndex,
      leaderId: nodes[0].id,
      leaderEpoch: 5,
      replicas: [nodes[0].id],
      isr: [nodes[0].id],
      offlineReplicas: [],
    }))
    return {
      brokers: nodes.map(({ id, host, port }) => ({ nodeId: id, host, port, rack: null })),
      controllerId: nodes[0].id,
      topics: [
        {
          errorCode: 0,
          name: TOPIC,
          topicId: this.topicId,
          isInternal: false,
          topicAuthorizedOperations: -2147483648,
          partitions,
        },
      ],
    }
  }

  /**
   * Overrides the answers of a group script, so the broker's state decides what is committed,
   * fetched and served.
   * @param {object} scripted a `ScriptedCluster`
   * @param {'classic'|'consumer'} kind the group protocol family
   * @returns {object} `scripted`
   */
  install(scripted, kind) {
    const consumer = kind === 'consumer'
    const ids = Array.from({ length: this.partitions }, (_, partition) => partition)
    const heartbeat = (api) => (_fields, record) => {
      const errorCode = this.faults.shift() ?? 0
      if (errorCode !== 0) this.faultOrders.push(record.order)
      return errorCode === 0 ? {} : { errorCode, errorMessage: `fault ${api}` }
    }
    scripted
      .answer('Metadata', () => {
        this.metadataRequests += 1
        return this.metadata(scripted.nodes)
      })
      .answer('OffsetFetch', ({ groupId, topics }) => ({
        groupId,
        topics: consumer
          ? (
              topics ?? [
                { topicId: this.topicId, partitions: ids.map((partition) => ({ partition })) },
              ]
            ).map(({ topicId, partitions }) => ({
              topicId,
              partitions: partitions.map(({ partition }) => ({
                partition,
                offset: this.committedOf(topicId, partition),
                leaderEpoch: -1,
                metadata: null,
                errorCode: 0,
              })),
            }))
          : topics.map((entry) => ({
              topic: entry.topic,
              partitions: entry.partitions.map(({ partition }) => ({
                partition,
                offset: this.committedOf(this.topicId, partition),
                leaderEpoch: -1,
                metadata: null,
                errorCode: 0,
              })),
            })),
      }))
      .answer('OffsetCommit', ({ topics }, record) => ({
        topics: topics.map((entry) => {
          const topicId = consumer ? entry.topicId : this.topicId
          return {
            ...(consumer ? { topicId } : { topic: entry.topic }),
            partitions: entry.partitions.map(({ partition, offset }) => {
              this.committed.set(`${topicId}|${partition}`, String(offset))
              this.commits.push({
                incarnation: INCARNATIONS[topicId] ?? 'unknown',
                partition,
                offset: String(offset),
                order: record.order,
              })
              return { partition, errorCode: 0 }
            }),
          }
        }),
      }))
      .answer('Fetch', async (fields, record) => {
        const { body, records } = this.fetchBody(fields, record)
        if (!records) await new Promise((resolve) => setTimeout(resolve, fields.maxWaitMs))
        return body
      })
      .answer('Heartbeat', heartbeat('Heartbeat'))
      .answer('ConsumerGroupHeartbeat', (request, record) => {
        const fault = this.faults.shift() ?? 0
        if (fault !== 0) this.faultOrders.push(record.order)
        const joining = request.memberEpoch === 0
        return {
          memberId: request.memberId,
          memberEpoch: fault === 0 ? (joining ? 1 : request.memberEpoch) : -1,
          heartbeatIntervalMs: 100,
          errorCode: fault,
          errorMessage: fault === 0 ? null : 'fault',
          assignment: joining
            ? { topicPartitions: [{ topicId: this.topicId, partitions: ids }] }
            : null,
        }
      })
    return scripted
  }

  /**
   * Builds the Fetch answer for the broker's state right now. A held call is answered from the
   * state it arrived in by building this when it arrives and releasing the result later. With
   * sessions on, the broker keeps one session: a full request replaces it, an incremental one
   * changes it, and the answer covers the session's partitions that have data.
   * @param {{ topics: object[], sessionId: number, sessionEpoch: number, forgottenTopics?: object[] }} fields
   *   the decoded request
   * @param {{ order: number }} record the request's log entry
   * @returns {{ body: Buffer, records: boolean }} the response body, and whether it carries data
   */
  fetchBody({ topics, sessionId, sessionEpoch, forgottenTopics = [] }, record) {
    const asked = topics.flatMap(({ topicId, partitions }) =>
      partitions.map(({ partition, fetchOffset }) => ({ topicId, partition, fetchOffset }))
    )
    this.fetches.push({
      order: record.order,
      topicId: topics[0]?.topicId,
      sessionId,
      sessionEpoch,
      partitions: asked.map(({ partition }) => partition),
    })
    let wanted = asked
    if (this.sessions) {
      const full = sessionId === 0 || sessionEpoch <= 0
      if (!full && (sessionId !== SESSION_ID || !this.session)) {
        return { body: this.#build([], { topLevel: SESSION_NOT_FOUND }), records: false }
      }
      if (full) this.session = new Map()
      for (const entry of asked) this.session.set(`${entry.topicId}|${entry.partition}`, entry)
      for (const { topicId, partitions } of forgottenTopics)
        for (const partition of partitions) this.session.delete(`${topicId}|${partition}`)
      const entries = [...this.session.values()]
      const live = entries.filter(
        ({ topicId, fetchOffset }) =>
          topicId === this.topicId && (full || this.#hasData(topicId, fetchOffset))
      )
      wanted = live.length > 0 ? live : entries.filter(({ topicId }) => topicId !== this.topicId)
    }
    const rows = wanted.map(({ partition, topicId, fetchOffset }) => {
      const stale = topicId !== undefined && topicId !== this.topicId
      return {
        partition,
        errorCode: stale ? UNKNOWN_TOPIC_ID : 0,
        records: !stale && this.#hasData(topicId, fetchOffset),
      }
    })
    const answeredId =
      wanted.find(({ topicId }) => topicId !== this.topicId)?.topicId ?? this.topicId
    return {
      body: this.#build(rows, { topicId: answeredId }),
      records: rows.some(({ records }) => records),
    }
  }

  #hasData(topicId, fetchOffset) {
    return (topicId === undefined || topicId === this.topicId) && BigInt(fetchOffset) <= 0n
  }

  #build(rows, { topLevel = 0, topicId = this.topicId } = {}) {
    const body = fetchResponse(rows, {
      topLevel,
      sessionId: this.sessions ? SESSION_ID : 0,
      includeTopic: rows.length > 0,
    })
    return this.withTopicId(body, topicId)
  }

  /**
   * Re-keys a Fetch body written for the harness's topic id to `topicId`, so the same bytes serve
   * a recreated topic, or an answer to a request that still carried the old id.
   * @param {Buffer} body
   * @param {string} [topicId] the id the answer names; the current one by default
   * @returns {Buffer}
   */
  withTopicId(body, topicId = this.topicId) {
    if (topicId === TOPIC_ID) return body
    const from = Buffer.from(TOPIC_ID.replaceAll('-', ''), 'hex')
    const at = body.indexOf(from)
    if (at < 0) return body
    const patched = Buffer.from(body)
    Buffer.from(topicId.replaceAll('-', ''), 'hex').copy(patched, at)
    return patched
  }
}

module.exports = { ScheduleBroker, NEW_TOPIC_ID, SESSION_ID }
