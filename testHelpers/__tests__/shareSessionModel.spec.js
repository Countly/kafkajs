const Encoder = require('../../src/protocol/encoder')
const shareFetchRequest = require('../../src/protocol/requests/shareFetch/v2/request')
const shareAcknowledgeRequest = require('../../src/protocol/requests/shareAcknowledge/v2/request')
const heartbeatRequest = require('../../src/protocol/requests/shareGroupHeartbeat/v1/request')
const shareFetchWire = require('../wire/shareFetchV2')
const shareAcknowledgeWire = require('../wire/shareAcknowledgeV2')
const { createScriptedCluster } = require('../scriptedCluster')
const {
  A,
  acquired,
  part,
  fetchAnswer,
  topError,
  echo,
  flush,
  heartbeatAnswer,
  installSessionModel,
} = require('../shareHarness')

const NODES = [{ id: 0, host: 'node0', port: 9092 }]
const ACCEPT = 1
const RENEW = 4
const SESSION_EPOCH_FINAL = -1

let correlationId = 0
/**
 * @param {{ apiKey: number, apiVersion: number, encode: () => Promise<Encoder> }} request
 * @returns {Promise<{ id: number, frame: Buffer }>} the request framed as the client writes it
 */
const frameOf = async (request) => {
  const id = ++correlationId
  const header = new Encoder()
    .writeInt16(request.apiKey)
    .writeInt16(request.apiVersion)
    .writeInt32(id)
    .writeString('model')
    .writeTaggedFields()
  const framed = new Encoder().writeEncoder(header).writeEncoder(await request.encode())
  return { id, frame: new Encoder().writeInt32(framed.size()).writeEncoder(framed).buffer }
}

const fetch = (memberId, epoch, { topics = [], forgotten = [] } = {}) =>
  shareFetchRequest({
    groupId: 'group',
    memberId,
    shareSessionEpoch: epoch,
    maxWaitMs: 0,
    minBytes: 1,
    maxBytes: 1048576,
    maxRecords: 500,
    batchSize: 500,
    shareAcquireMode: 1,
    topics,
    forgottenTopicsData: forgotten,
  })
const acknowledge = (memberId, epoch, topics, isRenewAck = false) =>
  shareAcknowledgeRequest({
    groupId: 'group',
    memberId,
    shareSessionEpoch: epoch,
    isRenewAck,
    topics,
  })
const beat = (memberId, memberEpoch) =>
  heartbeatRequest({
    groupId: 'group',
    memberId,
    memberEpoch,
    subscribedTopicNames: memberEpoch === 0 ? ['topic-a'] : null,
  })
/** @returns {object[]} topic A with each `[partition, batches?]` given */
const topicA = (...partitions) => [
  {
    topicId: A,
    partitions: partitions.map(([partitionIndex, acknowledgementBatches = []]) => ({
      partitionIndex,
      acknowledgementBatches,
    })),
  },
]
const batch = (first, last, type) => ({
  firstOffset: String(first),
  lastOffset: String(last),
  acknowledgeTypes: [type],
})
/** @returns {object} a fetch answer that acquires offsets first..last of partition 0 of topic A */
const acquiring = (first, last) =>
  fetchAnswer([part({ partition: 0, acquire: [acquired(first, last)] })])

/**
 * A scripted share cluster with the model installed, and raw connections to node 0.
 * @returns {object} `cluster`, `model`, `answers` (queued ShareFetch and ShareAcknowledge
 *   answers, the quiet broker's after them), `connect`, `send`, `reply`
 */
const setup = () => {
  const cluster = createScriptedCluster({ nodes: NODES, profile: 'share' })
  const answers = { ShareFetch: [], ShareAcknowledge: [] }
  for (const api of Object.keys(answers)) {
    cluster.answer(api, (fields) => answers[api].shift() ?? echo(api, fields))
  }
  const context = { interval: 50, epochs: new Map(), topics: new Map() }
  cluster.answer('ShareGroupHeartbeat', (fields) => heartbeatAnswer(fields, context))
  const model = installSessionModel({ cluster })
  /** @returns {object} a new connection to node 0: `socket`, `replies`, `errors` */
  const connect = () => {
    const socket = cluster.socketFactory({ host: 'node0', port: 9092, onConnect: () => {} })
    const connection = { socket, replies: [], errors: [] }
    socket.on('data', (chunk) => connection.replies.push(chunk))
    socket.on('error', (error) => connection.errors.push(error))
    return connection
  }
  /** @returns {Promise<object>} the logged record of the request, once the cluster had its turns */
  const send = async (connection, request) => {
    const { id, frame } = await frameOf(request)
    connection.socket.write(frame)
    await flush()
    return cluster.requests().find((record) => record.correlationId === id)
  }
  /** @returns {object|null} the decoded answer the connection received to the request */
  const reply = (connection, record) => {
    const frame = connection.replies.find((chunk) => chunk.readInt32BE(4) === record.correlationId)
    if (!frame) return null
    const pair = record.api === 'ShareFetch' ? shareFetchWire : shareAcknowledgeWire
    return pair.readResponse(frame.subarray(9))
  }
  return { cluster, model, answers, connect, send, reply }
}

describe('ShareSessionModel', () => {
  test("an epoch-0 fetch opens a session that replaces the member's previous one", async () => {
    const { cluster, model, connect, send } = setup()

    await send(connect(), fetch('m1', 0, { topics: topicA([0]) }))
    expect(model.session(0, 'm1')).toEqual({
      node: 0,
      member: 'm1',
      epoch: 1,
      generation: 1,
      partitions: [`${A}:0`],
    })

    await send(connect(), fetch('m1', 0, { topics: topicA([1]) }))

    expect(model.sessions()).toEqual([
      { node: 0, member: 'm1', epoch: 1, generation: 2, partitions: [`${A}:1`] },
    ])
    expect(model.lateRemovals()).toEqual([])
    cluster.assertClean()
  })

  test('an epoch-0 fetch answered with a non-zero error opens no session', async () => {
    const { cluster, model, answers, connect, send } = setup()
    const connection = connect()

    answers.ShareFetch.push(topError(133))
    await send(connection, fetch('m2', 0, { topics: topicA([0]) }))
    answers.ShareFetch.push(topError(25))
    await send(connection, fetch('m3', 0, { topics: topicA([0]) }))
    expect(model.sessions()).toEqual([])

    await send(connection, fetch('m1', 0, { topics: topicA([0]) }))
    answers.ShareFetch.push(topError(25))
    await send(connection, fetch('m1', 0, { topics: topicA([1]) }))
    expect(model.session(0, 'm1')).toMatchObject({ epoch: 1, partitions: [`${A}:0`] })

    answers.ShareFetch.push(topError(133))
    await send(connection, fetch('m1', 0, { topics: topicA([1]) }))
    expect(model.session(0, 'm1')).toBeNull()
    cluster.assertClean()
  })

  test('an ordinary request with a wrong epoch is answered 123 and a missing session 122', async () => {
    const { cluster, model, connect, send, reply } = setup()
    const connection = connect()
    await send(connection, fetch('m1', 0, { topics: topicA([0]) }))

    const ahead = await send(connection, fetch('m1', 2))
    const zero = await send(connection, acknowledge('m1', 0, []))
    expect(reply(connection, ahead)).toMatchObject({ errorCode: 123, responses: [] })
    expect(reply(connection, zero)).toMatchObject({ errorCode: 123, responses: [] })
    expect(model.session(0, 'm1').epoch).toBe(1)

    const matching = await send(connection, acknowledge('m1', 1, []))
    expect(reply(connection, matching).errorCode).toBe(0)
    expect(model.session(0, 'm1').epoch).toBe(2)

    const missing = await send(connection, fetch('m2', 1))
    const missingAck = await send(connection, acknowledge('m2', 1, []))
    expect(reply(connection, missing)).toMatchObject({ errorCode: 122, responses: [] })
    expect(reply(connection, missingAck)).toMatchObject({ errorCode: 122, responses: [] })
    cluster.assertClean()
  })

  test('a final removes whatever session the member holds', async () => {
    const { cluster, model, answers, connect, send, reply } = setup()
    const first = connect()
    await send(first, fetch('m1', 0, { topics: topicA([0]) }))
    answers.ShareFetch.push(acquiring(0, 0))
    await send(connect(), fetch('m1', 0, { topics: topicA([0]) }))

    const final = await send(first, fetch('m1', SESSION_EPOCH_FINAL))

    expect(reply(first, final).errorCode).toBe(0)
    expect(model.sessions()).toEqual([])
    expect(model.acquisitionsOf(A, 0, 0)).toEqual([
      expect.objectContaining({ member: 'm1', state: 'AVAILABLE' }),
    ])
    expect(model.changesTo(1)).toEqual([
      { state: 'AVAILABLE', cause: 'final', order: final.executedOrder },
    ])
    const again = await send(first, fetch('m1', SESSION_EPOCH_FINAL))
    expect(reply(first, again).errorCode).toBe(122)
    cluster.assertClean()
  })

  test('a late final removes a newer session and is listed as a late removal', async () => {
    const { cluster, model, connect, send } = setup()
    const old = connect()
    await send(old, fetch('m1', 0, { topics: topicA([0]) }))
    const gate = cluster.schedule.hold({
      api: 'ShareFetch',
      match: (call) => call.fields.shareSessionEpoch === SESSION_EPOCH_FINAL,
    })
    const final = await send(old, fetch('m1', SESSION_EPOCH_FINAL))
    await gate.arrived()
    const open = await send(connect(), fetch('m1', 0, { topics: topicA([0]) }))
    expect(model.session(0, 'm1')).toMatchObject({ generation: 2, epoch: 1 })

    gate.release()
    await flush()

    expect(model.sessions()).toEqual([])
    expect(model.executedAfter(final, open)).toBe(true)
    expect(model.lateRemovals()).toEqual([
      {
        session: expect.objectContaining({ member: 'm1', generation: 2 }),
        by: expect.objectContaining({
          api: 'ShareFetch',
          order: final.order,
          executedOrder: final.executedOrder,
        }),
      },
    ])
    cluster.assertClean()
  })

  test('a final written after its own session opened is not a late removal', async () => {
    const { cluster, model, connect, send } = setup()
    const connection = connect()
    await send(connection, fetch('m1', 0, { topics: topicA([0]) }))
    expect(model.session(0, 'm1')).toMatchObject({ epoch: 1, generation: 1 })

    await send(connection, fetch('m1', SESSION_EPOCH_FINAL))

    expect(model.sessions()).toEqual([])
    expect(model.lateRemovals()).toEqual([])
    cluster.assertClean()
  })

  test('a broker-side disconnect of a stale generation removes nothing', async () => {
    const { cluster, model, answers, connect, send } = setup()
    answers.ShareFetch.push(acquiring(0, 0))
    await send(connect(), fetch('m1', 0, { topics: topicA([0]) }))
    await send(connect(), fetch('m1', 0, { topics: topicA([0]) }))

    cluster.brokerDisconnect(0, 1)
    await flush()

    expect(model.session(0, 'm1')).toMatchObject({ generation: 2 })
    expect(model.acquisitionsOf(A, 0, 0)).toEqual([
      expect.objectContaining({ member: 'm1', state: 'ACQUIRED' }),
    ])

    cluster.brokerDisconnect(0, 2)
    await flush()

    expect(model.sessions()).toEqual([])
    expect(model.changesTo(1)).toEqual([
      { state: 'AVAILABLE', cause: 'disconnect', order: expect.any(Number) },
    ])
    cluster.assertClean()
  })

  test('a client-only socket loss keeps the session until the broker-side disconnect', async () => {
    const { cluster, model, answers, connect, send } = setup()
    const connection = connect()
    answers.ShareFetch.push(acquiring(0, 0))
    await send(connection, fetch('m1', 0, { topics: topicA([0]) }))

    cluster.clientLoss(0, 1)
    await flush()

    expect(connection.socket.destroyed).toBe(true)
    expect(connection.errors).toEqual([expect.objectContaining({ code: 'ECONNRESET' })])
    expect(model.session(0, 'm1')).toMatchObject({ generation: 1 })
    expect(model.acquisitionsOf(A, 0, 0)[0].state).toBe('ACQUIRED')

    cluster.brokerDisconnect(0, 1)
    await flush()

    expect(model.sessions()).toEqual([])
    expect(model.acquisitionsOf(A, 0, 0)[0].state).toBe('AVAILABLE')
    expect(model.changesTo(1)).toEqual([
      expect.objectContaining({ state: 'AVAILABLE', cause: 'disconnect' }),
    ])
    cluster.assertClean()
  })

  test('a held request released with a custom answer still executes in the model', async () => {
    const { cluster, model, connect, send } = setup()
    const gate = cluster.schedule.hold({ api: 'ShareFetch' })
    const open = await send(connect(), fetch('m1', 0, { topics: topicA([0]) }))
    await gate.arrived()
    expect(open.executedOrder).toBeUndefined()
    expect(model.sessions()).toEqual([])

    gate.release(acquiring(0, 1))
    await flush()

    expect(open.executedOrder).toBeGreaterThan(open.order)
    expect(model.session(0, 'm1')).toMatchObject({ epoch: 1, generation: 1 })
    expect(model.acquisitionsOf(A, 0, 1)).toEqual([
      { observationId: 2, member: 'm1', node: 0, state: 'ACQUIRED', renewals: 0 },
    ])
    cluster.assertClean()
  })

  test('a request whose client socket closed still executes and is not delivered', async () => {
    const { cluster, model, answers, connect, send } = setup()
    const connection = connect()
    answers.ShareFetch.push(acquiring(0, 0))
    await send(connection, fetch('m1', 0, { topics: topicA([0]) }))
    const gate = cluster.schedule.hold({ api: 'ShareAcknowledge' })
    const accept = await send(connection, acknowledge('m1', 1, topicA([0, [batch(0, 0, ACCEPT)]])))
    await gate.arrived()
    cluster.clientLoss(0, 1)
    await flush()
    const replies = connection.replies.length

    gate.release()
    await flush()

    expect(accept.executedOrder).toBeGreaterThan(accept.order)
    expect(accept.answeredOrder).toBeUndefined()
    expect(cluster.tally('ShareAcknowledge', 'cancelled')).toBe(1)
    expect(connection.replies).toHaveLength(replies)
    expect(model.changesTo(1)).toEqual([
      { state: 'ACKNOWLEDGED', cause: 'accept', order: accept.executedOrder },
    ])
    expect(model.session(0, 'm1').epoch).toBe(2)
    cluster.assertClean()
  })

  test('a request whose reply is held executes at once and is delivered only when released', async () => {
    const { cluster, model, answers, connect, send, reply } = setup()
    const connection = connect()
    answers.ShareFetch.push(acquiring(0, 0))
    await send(connection, fetch('m1', 0, { topics: topicA([0]) }))
    const held = cluster.holdReply({ api: 'ShareAcknowledge' })

    const accept = await send(connection, acknowledge('m1', 1, topicA([0, [batch(0, 0, ACCEPT)]])))

    expect(await held.executed()).toBe(accept)
    expect(model.changesTo(1)).toEqual([
      { state: 'ACKNOWLEDGED', cause: 'accept', order: accept.executedOrder },
    ])
    expect(reply(connection, accept)).toBeNull()

    held.deliver()

    expect(reply(connection, accept).responses[0].partitions[0].errorCode).toBe(0)
    expect(accept.answeredOrder).toBeGreaterThan(accept.executedOrder)
    cluster.assertClean()
  })

  test("an incremental fetch with no topic rows keeps the session's partitions", async () => {
    const { cluster, model, connect, send } = setup()
    const connection = connect()
    await send(connection, fetch('m1', 0, { topics: topicA([0], [1]) }))

    await send(connection, fetch('m1', 1))

    expect(model.session(0, 'm1')).toMatchObject({ epoch: 2, partitions: [`${A}:0`, `${A}:1`] })

    await send(connection, fetch('m1', 2, { forgotten: [{ topicId: A, partitions: [1] }] }))

    expect(model.session(0, 'm1')).toMatchObject({ epoch: 3, partitions: [`${A}:0`] })
    cluster.assertClean()
  })

  test('a renewal checks ownership and keeps the acquisition', async () => {
    const { cluster, model, answers, connect, send, reply } = setup()
    const connection = connect()
    answers.ShareFetch.push(acquiring(0, 0))
    await send(connection, fetch('m1', 0, { topics: topicA([0]) }))
    await send(connection, fetch('m2', 0, { topics: topicA([0]) }))

    const own = await send(
      connection,
      acknowledge('m1', 1, topicA([0, [batch(0, 0, RENEW)]]), true)
    )
    const other = await send(
      connection,
      acknowledge('m2', 1, topicA([0, [batch(0, 0, RENEW)]]), true)
    )

    expect(reply(connection, own).responses[0].partitions[0].errorCode).toBe(0)
    expect(reply(connection, other).responses[0].partitions[0].errorCode).toBe(121)
    expect(model.acquisitionsOf(A, 0, 0)).toEqual([
      { observationId: 1, member: 'm1', node: 0, state: 'ACQUIRED', renewals: 1 },
    ])
    expect(model.changesTo(1)).toEqual([])
    cluster.assertClean()
  })

  test('an acknowledgement from a member that does not own the offset is answered 121 and changes nothing', async () => {
    const { cluster, model, answers, connect, send, reply } = setup()
    const connection = connect()
    answers.ShareFetch.push(acquiring(0, 1))
    await send(connection, fetch('m1', 0, { topics: topicA([0]) }))
    await send(connection, fetch('m2', 0, { topics: topicA([0]) }))

    const standalone = await send(
      connection,
      acknowledge('m2', 1, topicA([0, [batch(0, 1, ACCEPT)]]))
    )
    const piggybacked = await send(
      connection,
      fetch('m2', 2, { topics: topicA([0, [batch(0, 0, ACCEPT)]]) })
    )
    const partlyHeld = await send(
      connection,
      acknowledge('m1', 1, topicA([0, [batch(0, 1, ACCEPT), batch(5, 5, ACCEPT)]]))
    )

    expect(reply(connection, standalone).responses[0].partitions[0]).toMatchObject({
      partitionIndex: 0,
      errorCode: 121,
    })
    expect(reply(connection, piggybacked).responses[0].partitions[0]).toMatchObject({
      partitionIndex: 0,
      errorCode: 0,
      acknowledgeErrorCode: 121,
    })
    expect(reply(connection, partlyHeld).responses[0].partitions[0].errorCode).toBe(121)
    for (const offset of [0, 1]) {
      expect(model.acquisitionsOf(A, 0, offset)).toEqual([
        expect.objectContaining({ member: 'm1', state: 'ACQUIRED' }),
      ])
    }
    expect(model.changesTo(1)).toEqual([])
    expect(model.changesTo(2)).toEqual([])
    cluster.assertClean()
  })

  test('a late accept from the same member on a re-acquisition at a matching epoch changes that re-acquisition', async () => {
    const { cluster, model, answers, connect, send } = setup()
    const old = connect()
    answers.ShareFetch.push(acquiring(0, 0))
    await send(old, fetch('m1', 0, { topics: topicA([0]) }))
    const gate = cluster.schedule.hold({ api: 'ShareAcknowledge' })
    const accept = await send(old, acknowledge('m1', 1, topicA([0, [batch(0, 0, ACCEPT)]])))
    await gate.arrived()
    model.expireLock(A, 0, 0)
    answers.ShareFetch.push(acquiring(0, 0))
    const reacquire = await send(connect(), fetch('m1', 0, { topics: topicA([0]) }))
    expect(model.session(0, 'm1')).toMatchObject({ generation: 2, epoch: 1 })

    gate.release()
    await flush()

    expect(model.executedAfter(accept, reacquire)).toBe(true)
    const [first, second] = model.acquisitionsOf(A, 0, 0)
    expect(first).toMatchObject({ observationId: 1, state: 'AVAILABLE' })
    expect(second).toMatchObject({ observationId: 2, member: 'm1', state: 'ACKNOWLEDGED' })
    expect(model.changesTo(1)).toEqual([expect.objectContaining({ cause: 'expiry' })])
    expect(model.changesTo(2)).toEqual([
      { state: 'ACKNOWLEDGED', cause: 'accept', order: accept.executedOrder },
    ])
    cluster.assertClean()
  })

  test('a leave written before a replacement join and executed after it is a late leave; an ordinary leave is not', async () => {
    const { cluster, model, connect, send } = setup()
    const connection = connect()
    await send(connection, beat('m1', 0))
    await send(connection, beat('m1', -1))
    expect(model.lateLeaves()).toEqual([])

    const gate = cluster.schedule.hold({
      api: 'ShareGroupHeartbeat',
      match: (call) => call.fields.memberEpoch === -1,
    })
    const leave = await send(connection, beat('m1', -1))
    await gate.arrived()
    const join = await send(connection, beat('m1', 0))
    gate.release()
    await flush()

    expect(model.lateLeaves()).toEqual([
      {
        leave: expect.objectContaining({
          kind: 'leave',
          member: 'm1',
          order: leave.order,
          executedOrder: leave.executedOrder,
        }),
        join: expect.objectContaining({ kind: 'join', member: 'm1', order: join.order }),
      },
    ])
    cluster.assertClean()
  })
})
