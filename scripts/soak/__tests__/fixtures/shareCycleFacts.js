const Encoder = require('../../../../src/protocol/encoder')
const { shareRecordIdentity } = require('../../../../testHelpers/shareFixtures')
const { ShareCycleOracle } = require('../../soak')

/**
 * Builds independent primitive cycle evidence for a separate corpus and member namespace.
 * @param {object} [options] Fixed topic, group, UUID and member prefix.
 * @returns {object} Pure evidence constructors; no sockets, timers or product workload.
 */
const createCycleFacts = ({
  topic: TOPIC_NAME = 'soak-share-spec',
  group = 'soak-group',
  uuid: SOAK_UUID = '00000000-0000-4000-8000-000000000001',
  memberPrefix = '',
} = {}) => {
  /** Every identity of one cycle, as the producer acknowledged it. */
  const cycleIdentities = (plan) => {
    const next = [0, 0, 0]
    return plan.records.map((record) => ({
      partition: record.partition,
      identity: shareRecordIdentity({
        topic: TOPIC_NAME,
        partition: record.partition,
        offset: String(plan.first + BigInt(next[record.partition]++)),
        key: Buffer.from(record.key),
        value: Buffer.from(record.value),
      }),
    }))
  }

  /** What a correct cycle observes, including the held set and replays of a reset cycle. */

  /** Primitive original request/response observations for one successful acquisition. */
  const exchangeFacts = (id, partition, acquiredRecords) => ({
    id,
    apiKey: 78,
    groupId: group,
    memberId: `${memberPrefix}member-A`,
    endpoint: 'node:1',
    open: 1,
    pool: 1,
    generation: 1,
    responseIdentity: id,
    tickets: [],
    request: {},
    frames: [
      {
        id,
        socket: 1,
        correlationId: id,
        apiKey: 78,
        apiVersion: 2,
        endpoint: 'node:1',
        bytes: 'request',
        responseBytes: 'response',
        request: {
          groupId: group,
          memberId: `${memberPrefix}member-A`,
          topics: [
            {
              topicId: SOAK_UUID,
              partitions: [{ partitionIndex: partition, acknowledgementBatches: [] }],
            },
          ],
        },
      },
    ],
    response: {
      errorCode: 0,
      responses: [
        {
          topicId: SOAK_UUID,
          partitions: [
            { partitionIndex: partition, errorCode: 0, acknowledgeErrorCode: 0, acquiredRecords },
          ],
        },
      ],
    },
  })

  /** Encodes independent primitive request/answer bytes after the scripted exchange is assembled. */
  const sealExchange = (exchange) => {
    const frame = exchange.frames[0]
    const request = frame.request
    const body = new Encoder()
      .writeUVarIntString(request.groupId)
      .writeUVarIntString(request.memberId)
      .writeInt32(request.shareSessionEpoch ?? 0)
      .writeInt32(250)
      .writeInt32(1)
      .writeInt32(1048576)
      .writeInt32(100)
      .writeInt32(0)
      .writeInt8(1)
      .writeBoolean(false)
      .writeUVarIntArray(
        request.topics.map((topic) =>
          new Encoder()
            .writeUuid(topic.topicId)
            .writeUVarIntArray(
              topic.partitions.map((partition) =>
                new Encoder()
                  .writeInt32(partition.partitionIndex)
                  .writeUVarIntArray(
                    partition.acknowledgementBatches.map((batch) =>
                      new Encoder()
                        .writeInt64(batch.firstOffset)
                        .writeInt64(batch.lastOffset)
                        .writeUVarIntArray(
                          batch.acknowledgeTypes.map((type) => new Encoder().writeInt8(type))
                        )
                        .writeTaggedFields([])
                    )
                  )
                  .writeTaggedFields([])
              )
            )
            .writeTaggedFields([])
        )
      )
      .writeUVarIntArray(
        (request.forgottenTopicsData ?? []).map(({ topicId, partitions }) =>
          new Encoder()
            .writeUuid(topicId)
            .writeUVarIntArray(partitions.map((partition) => new Encoder().writeInt32(partition)))
            .writeTaggedFields([])
        )
      )
      .writeTaggedFields([])
    const header = new Encoder()
      .writeInt16(78)
      .writeInt16(2)
      .writeInt32(frame.correlationId)
      .writeString('soak')
      .writeTaggedFields([])
    const bytes = Buffer.concat([header.buffer, body.buffer])
    frame.bytes = new Encoder()
      .writeInt32(bytes.length)
      .writeBuffer(bytes)
      .buffer.toString('base64')
    frame.request = require('../../../../testHelpers/wire/shareFetchV2').readRequest(body.buffer)
    const response = require('../../../../testHelpers/wire/shareFetchV2').buildResponse({
      errorCode: exchange.response.errorCode,
      responses: exchange.response.responses.map((topic) => ({
        ...topic,
        partitions: topic.partitions.map((partition) => ({
          ...partition,
          errorMessage: null,
          acknowledgeErrorMessage: null,
          currentLeader: { leaderId: -1, leaderEpoch: -1 },
          records: '',
        })),
      })),
      nodeEndpoints: [],
    })
    const inner = new Encoder()
      .writeInt32(frame.correlationId)
      .writeTaggedFields([])
      .writeBuffer(response)
    frame.responseBytes = new Encoder()
      .writeInt32(inner.buffer.length)
      .writeBuffer(inner.buffer)
      .buffer.toString('base64')
    return exchange
  }

  /**
   * Independent heartbeat request bytes, including the original socket correlation header.
   * @param {object} request Original group/member/epoch fields.
   * @param {number} correlationId Socket correlation.
   * @returns {Buffer} Complete request frame.
   */
  const heartbeatBytes = (request, correlationId) => {
    const bytes = new Encoder()
      .writeInt16(76)
      .writeInt16(1)
      .writeInt32(correlationId)
      .writeString('soak')
      .writeTaggedFields([])
      .writeUVarIntString(request.groupId)
      .writeUVarIntString(request.memberId)
      .writeInt32(request.memberEpoch)
      .writeUVarIntString(null)
      .writeUVarIntArray(null)
      .writeTaggedFields([])
    return new Encoder().writeInt32(bytes.size()).writeEncoder(bytes).buffer
  }

  /**
   * A matched original leave exchange for the pure reconstruction controls.
   * @param {string} memberId Original cycle member.
   * @param {number} at Original invocation/write/answer timestamp.
   * @param {number} socket Original socket identity and correlation.
   * @returns {object} Heartbeat invocation with matched request and response bytes.
   */
  const leaveFacts = (memberId, at, socket) => {
    const request = { groupId: group, memberId, memberEpoch: -1 }
    const response = { errorCode: 0, memberId, memberEpoch: -1, assignment: null }
    const body = require('../../../../testHelpers/wire/shareGroupHeartbeatV1').buildResponse(
      response
    )
    const answer = new Encoder().writeInt32(socket).writeTaggedFields([]).writeBuffer(body)
    return {
      ...request,
      endpoint: 'node:1',
      at,
      request,
      answeredAt: at,
      response,
      frames: [
        {
          id: socket,
          apiKey: 76,
          apiVersion: 1,
          correlationId: socket,
          socket,
          endpoint: 'node:1',
          at,
          answeredAt: at,
          request: { ...request, rackId: null, subscribedTopicNames: null },
          bytes: heartbeatBytes(request, socket).toString('base64'),
          responseBytes: new Encoder()
            .writeInt32(answer.size())
            .writeEncoder(answer)
            .buffer.toString('base64'),
        },
      ],
    }
  }

  /** Both actual listing forms and independent ends, kept separate from normalized rows. */
  const positionFacts = (plan) => {
    const partitions = [0, 1, 2].map((partition) => ({ partition, floor: '0', end: plan.end }))
    const listing = [
      {
        topic: TOPIC_NAME,
        topicId: SOAK_UUID,
        partitions: partitions.map(({ partition }) => ({
          partition,
          startOffset: '0',
          lag: '0',
          errorCode: 0,
          leaderEpoch: 1,
        })),
      },
    ]
    return {
      target: { groupId: group, topic: TOPIC_NAME, partitions },
      named: JSON.parse(JSON.stringify(listing)),
      all: JSON.parse(JSON.stringify(listing)),
      topicId: SOAK_UUID,
      ends: partitions.map(({ partition, end }) => ({ partition, high: end })),
    }
  }

  /** Measured assigned interval, with an actual in-window ShareFetch and joined leave. */
  const verifierFacts = (cycle = 0, base = cycle * 32000 + 1000, kind = 'cycle') => {
    const memberId = `${memberPrefix}verifier-${kind}-${cycle}`
    const exchanges = [0, 1, 2].map((partition) => {
      const exchange = exchangeFacts(9000 + partition, partition, [])
      exchange.memberId = memberId
      Object.assign(exchange.frames[0], { at: base + 100, answeredAt: base + 200 })
      exchange.frames[0].request.memberId = memberId
      return sealExchange(exchange)
    })
    return {
      active: true,
      cycle,
      kind,
      groupId: group,
      topic: TOPIC_NAME,
      topicId: SOAK_UUID,
      memberId,
      windowMs: 5000,
      openedAt: base - 100,
      closedAt: base + 5100,
      delivered: [],
      provenance: { exchanges },
      events: [
        { kind: 'GROUP_JOIN', at: base - 50, payload: { memberId, groupId: group } },
        {
          kind: 'window-start',
          at: base,
          wallAt: base + 10000,
          payload: [{ topic: TOPIC_NAME, partitions: [0, 1, 2] }],
        },
        ...exchanges.map(({ frames: [frame] }) => ({
          kind: 'REQUEST',
          at: base + 250,
          payload: {
            apiName: 'ShareFetch',
            sentAt: base + 10100,
            correlationId: frame.correlationId,
            broker: frame.endpoint,
          },
        })),
        {
          kind: 'window-end',
          at: base + 5000,
          wallAt: base + 15000,
          payload: [{ topic: TOPIC_NAME, partitions: [0, 1, 2] }],
        },
        { kind: 'DISCONNECT', at: base + 5100, payload: {} },
      ],
    }
  }

  const cycleFacts = (
    cycle,
    { startedAt = cycle * 32000, due = startedAt + 32000, kind = 'cycle' } = {}
  ) => {
    const plan = ShareCycleOracle.plan(cycle)
    const produced = cycleIdentities(plan)
    const identities = produced.map(({ identity }) => identity)
    const last = plan.first + BigInt(plan.perPartition) - 1n
    const held = plan.reset
      ? produced.filter(({ partition }) => partition === 0).map(({ identity }) => identity)
      : []
    const facts = {
      produced,
      log: [...identities],
      logs: Object.fromEntries(
        [0, 1, 2].map((partition) => [partition, { end: plan.end, count: Number(plan.end) }])
      ),
      handler: [
        ...identities.map((identity) => ({ identity, deliveryCount: 1 })),
        ...held.map((identity) => ({ identity, deliveryCount: 2 })),
      ],
      held,
      accepted: [0, 1, 2].map((partition) => ({
        topic: TOPIC_NAME,
        partition,
        firstOffset: String(plan.first),
        lastOffset: String(last),
      })),
      broker: [0, 1, 2].map((partition) => ({
        partition,
        startOffset: '0',
        logEnd: plan.end,
        lag: '0',
        errorCode: 0,
      })),
      members: {
        aJoined: true,
        aLeft: true,
        bAdded: true,
        bRemoved: true,
        bRemovedAt: startedAt + (kind === 'final' ? -200 : 100),
        aLeftAt: startedAt + (kind === 'final' ? -100 : 200),
        emptyAt: startedAt + 500,
      },
      timing: { cycle, kind, startedAt, completedAt: startedAt + 6300, due },
      reset: plan.reset
        ? {
            apiKey: 78,
            nodeId: 1,
            reason: 'connection',
            oldSocket: 3,
            oldEpoch: 4,
            nextSocket: 7,
            nextEpoch: 0,
          }
        : null,
      verifier: verifierFacts(cycle, startedAt + 1000, kind),
      clientsDisconnected: true,
      positions: [
        {
          ...positionFacts(plan),
          cycle,
          kind,
          startedAt: startedAt + 300,
          completedAt: startedAt + 400,
        },
        {
          ...positionFacts(plan),
          cycle,
          kind,
          startedAt: startedAt + 6200,
          completedAt: startedAt + 6300,
        },
      ],
    }
    facts.acknowledgedPrefix = Array.from({ length: cycle + 1 }, (_, i) =>
      cycleIdentities(ShareCycleOracle.plan(i))
    )
      .flat()
      .map(({ identity }) => identity)
    facts.prefix = [...facts.acknowledgedPrefix]
    const provenance = { exchanges: [], acquisitions: [], outcomes: [], retirements: [] }
    for (const [index, entry] of facts.handler.entries()) {
      const [, partition, offset] = JSON.parse(entry.identity)
      const acquisition = index + 1
      entry.acquisition = acquisition
      entry.dispatch = acquisition
      const range = { firstOffset: offset, lastOffset: offset, deliveryCount: entry.deliveryCount }
      const row = {
        id: acquisition,
        exchange: acquisition,
        topic: TOPIC_NAME,
        topicId: SOAK_UUID,
        partition,
        acquiredRecords: [range],
      }
      const exchange = exchangeFacts(acquisition, partition, [range])
      provenance.acquisitions.push(row)
      provenance.exchanges.push(exchange)
    }
    facts.accepted = []
    for (const [index, entry] of facts.handler.entries()) {
      if (facts.handler.slice(index + 1).some((other) => other.identity === entry.identity))
        continue
      const [, partition, offset] = JSON.parse(entry.identity)
      const acquisition = entry.acquisition
      const source = provenance.exchanges.find(({ id }) => id === acquisition)
      const batch = { firstOffset: offset, lastOffset: offset, acknowledgeTypes: [1] }
      const exchange = exchangeFacts(1000 + index, partition, [])
      exchange.open = source.open
      exchange.tickets = [{ acquisition, ticket: index, isRenewAck: false, batches: [batch] }]
      exchange.frames[0].request.topics[0].partitions[0].acknowledgementBatches = [batch]
      provenance.exchanges.push(exchange)
      const result = {
        acquisition,
        topic: TOPIC_NAME,
        partition,
        firstOffset: offset,
        lastOffset: offset,
        type: 'accept',
        error: null,
      }
      provenance.outcomes.push(result)
      facts.accepted.push({ topic: TOPIC_NAME, partition, firstOffset: offset, lastOffset: offset })
    }
    provenance.exchanges.forEach(sealExchange)
    provenance.heartbeats = [
      leaveFacts(`${memberPrefix}member-B`, facts.members.bRemovedAt, 101),
      leaveFacts(`${memberPrefix}member-A`, facts.members.aLeftAt, 102),
    ]
    facts.memberObservations = [
      { member: 'A', memberId: `${memberPrefix}member-A` },
      { member: 'B', memberId: `${memberPrefix}member-B` },
    ]
    facts.provenance = provenance
    return facts
  }

  return {
    TOPIC_NAME,
    SOAK_UUID,
    cycleIdentities,
    exchangeFacts,
    sealExchange,
    heartbeatBytes,
    leaveFacts,
    positionFacts,
    verifierFacts,
    cycleFacts,
  }
}
module.exports = { createCycleFacts, ...createCycleFacts() }
