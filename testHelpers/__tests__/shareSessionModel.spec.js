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

const ACCEPT = 1
const RENEW = 4
const SESSION_EPOCH_FINAL = -1
const INT32_MAX = 2147483647

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

/** A renewing fetch (`renew`) asks for no records unless `asking` says otherwise. */
const fetch = (
  memberId,
  epoch,
  { topics = [], forgotten = [], renew = false, asking = !renew } = {}
) =>
  shareFetchRequest({
    groupId: 'group',
    memberId,
    shareSessionEpoch: epoch,
    maxWaitMs: 0,
    minBytes: asking ? 1 : 0,
    maxBytes: asking ? 1048576 : 0,
    maxRecords: asking ? 500 : 0,
    batchSize: 500,
    shareAcquireMode: 1,
    isRenewAck: renew,
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
/** @returns {object} a fetch answer that acquires offset 0 of partitions 0 and 1 of topic A */
const acquiringBoth = () =>
  fetchAnswer([
    part({ partition: 0, acquire: [acquired(0, 0)] }),
    part({ partition: 1, acquire: [acquired(0, 0)] }),
  ])
/** @returns {string[]} the messages of the cluster's recorded failures */
const failed = (cluster) => cluster.failures().map(({ message }) => message)

/**
 * A scripted share cluster, by default with the model installed, and raw connections to its nodes.
 * @param {{ nodes?: number, model?: boolean }} [options]
 * @returns {object} `cluster`, `model`, `answers` (queued ShareFetch and ShareAcknowledge
 *   answers, the quiet broker's after them), `connect`, `send`, `reply`
 */
const setup = ({ nodes = 1, model: modelled = true } = {}) => {
  const cluster = createScriptedCluster({
    nodes: Array.from({ length: nodes }, (_, id) => ({ id, host: `node${id}`, port: 9092 })),
    profile: 'share',
  })
  const answers = { ShareFetch: [], ShareAcknowledge: [] }
  for (const api of Object.keys(answers)) {
    cluster.answer(api, (fields) => answers[api].shift() ?? echo(api, fields))
  }
  const context = { interval: 50, epochs: new Map(), topics: new Map() }
  cluster.answer('ShareGroupHeartbeat', (fields) => heartbeatAnswer(fields, context))
  const model = modelled ? installSessionModel({ cluster }) : null
  /** @returns {object} a new connection to the node: `socket`, `replies`, `errors` */
  const connect = (node = 0) => {
    const socket = cluster.socketFactory({ host: `node${node}`, port: 9092, onConnect: () => {} })
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

  test('an epoch-0 fetch carrying acknowledgements and a renewing fetch asking for records are answered 42 and change nothing', async () => {
    const { cluster, model, answers, connect, send, reply } = setup()
    const connection = connect()
    answers.ShareFetch.push(acquiring(0, 0))
    await send(connection, fetch('m1', 0, { topics: topicA([0]) }))

    const other = connect()
    const reopen = await send(other, fetch('m1', 0, { topics: topicA([0, [batch(0, 0, ACCEPT)]]) }))
    const renewing = await send(
      connection,
      fetch('m1', 1, { renew: true, asking: true, topics: topicA([0, [batch(0, 0, RENEW)]]) })
    )

    expect(reply(other, reopen)).toMatchObject({ errorCode: 42, responses: [] })
    expect(reply(connection, renewing)).toMatchObject({ errorCode: 42, responses: [] })
    expect(model.sessions()).toEqual([
      { node: 0, member: 'm1', epoch: 1, generation: 1, partitions: [`${A}:0`] },
    ])
    expect(model.acquisitionsOf(A, 0, 0)).toEqual([
      { observationId: 1, member: 'm1', node: 0, state: 'ACQUIRED', renewals: 0 },
    ])
    cluster.assertClean()
  })

  test('a scripted top-level error is a refusal before admission, and a scripted 122, 123 or 133 must agree with the session', async () => {
    const { cluster, model, answers, connect, send, reply } = setup()
    const connection = connect()
    await send(connection, fetch('m1', 0, { topics: topicA([0]) }))

    answers.ShareFetch.push(topError(30))
    const denied = await send(connection, fetch('m1', 1))
    expect(reply(connection, denied)).toMatchObject({ errorCode: 30, responses: [] })
    expect(model.session(0, 'm1').epoch).toBe(1)
    expect(failed(cluster)).toEqual([])

    answers.ShareFetch.push(topError(122), topError(133))
    await send(connection, fetch('m1', 1))
    await send(connection, fetch('m1', 1))

    expect(failed(cluster)).toEqual([
      expect.stringContaining('answered 122 where the broker answers 0'),
      expect.stringContaining('answered 133 where the broker answers 0'),
    ])
    expect(model.session(0, 'm1').epoch).toBe(1)
  })

  test('the session epoch after 2147483647 is 1', async () => {
    const { cluster, model, connect, send, reply } = setup()
    const connection = connect()
    await send(connection, fetch('m1', 0, { topics: topicA([0]) }))
    model.live.get('0|m1').epoch = INT32_MAX

    const last = await send(connection, acknowledge('m1', INT32_MAX, []))
    expect(reply(connection, last).errorCode).toBe(0)
    expect(model.session(0, 'm1').epoch).toBe(1)

    const wrapped = await send(connection, fetch('m1', 1))
    expect(reply(connection, wrapped).errorCode).toBe(0)
    expect(model.session(0, 'm1').epoch).toBe(2)
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

  test('a broker-side disconnect removes only the session last opened on that connection', async () => {
    const { cluster, model, answers, connect, send } = setup()
    const connection = connect()
    answers.ShareFetch.push(acquiring(0, 0))
    await send(connection, fetch('m1', 0, { topics: topicA([0]) }))
    answers.ShareFetch.push(acquiring(1, 1))
    await send(connection, fetch('m2', 0, { topics: topicA([0]) }))

    cluster.brokerDisconnect(0, 1)
    await flush()

    expect(model.sessions()).toEqual([
      { node: 0, member: 'm1', epoch: 1, generation: 1, partitions: [`${A}:0`] },
    ])
    expect(model.acquisitionsOf(A, 0, 0)).toEqual([
      { observationId: 1, member: 'm1', node: 0, state: 'ACQUIRED', renewals: 0 },
    ])
    expect(model.changesTo(2)).toEqual([
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
    const { cluster, model, answers, connect, send } = setup()
    const connection = connect()
    await send(connection, fetch('m1', 0, { topics: topicA([0], [1]) }))

    answers.ShareFetch.push(fetchAnswer([part({ partition: 1, acquire: [acquired(0, 0)] })]))
    await send(connection, fetch('m1', 1))

    expect(model.session(0, 'm1')).toMatchObject({ epoch: 2, partitions: [`${A}:0`, `${A}:1`] })
    expect(model.acquisitionsOf(A, 1, 0)).toEqual([
      { observationId: 1, member: 'm1', node: 0, state: 'ACQUIRED', renewals: 0 },
    ])

    await send(connection, fetch('m1', 2, { forgotten: [{ topicId: A, partitions: [1] }] }))

    expect(model.session(0, 'm1')).toMatchObject({ epoch: 3, partitions: [`${A}:0`] })
    cluster.assertClean()
  })

  test('a fetch that acquires outside its fetch set is a recorded failure and acquires nothing', async () => {
    const { cluster, model, answers, connect, send } = setup()
    const connection = connect()
    await send(connection, fetch('m1', 0, { topics: topicA([0], [1]) }))

    answers.ShareFetch.push(fetchAnswer([part({ partition: 1, acquire: [acquired(0, 0)] })]))
    await send(connection, fetch('m1', 1, { forgotten: [{ topicId: A, partitions: [1] }] }))
    answers.ShareFetch.push(acquiring(0, 0))
    await send(connection, fetch('m1', 2, { renew: true }))
    answers.ShareFetch.push(acquiring(0, 0))
    await send(connection, fetch('m1', SESSION_EPOCH_FINAL))

    expect(failed(cluster)).toEqual([
      expect.stringContaining(`${A}:1 outside its fetch set`),
      expect.stringContaining(`${A}:0 outside its fetch set`),
      expect.stringContaining(`${A}:0 outside its fetch set`),
    ])
    expect(model.acquisitionsOf(A, 1, 0)).toEqual([])
    expect(model.acquisitionsOf(A, 0, 0)).toEqual([])
  })

  test("a final releases the member's records only on the partitions its session caches", async () => {
    const { cluster, model, answers, connect, send, reply } = setup()
    const connection = connect()
    answers.ShareFetch.push(acquiringBoth())
    await send(connection, fetch('m1', 0, { topics: topicA([0], [1]) }))
    await send(connection, fetch('m1', 1, { forgotten: [{ topicId: A, partitions: [1] }] }))

    const final = await send(connection, fetch('m1', SESSION_EPOCH_FINAL))

    expect(reply(connection, final).errorCode).toBe(0)
    expect(model.sessions()).toEqual([])
    expect(model.changesTo(1)).toEqual([
      { state: 'AVAILABLE', cause: 'final', order: final.executedOrder },
    ])
    expect(model.acquisitionsOf(A, 1, 0)).toEqual([
      { observationId: 2, member: 'm1', node: 0, state: 'ACQUIRED', renewals: 0 },
    ])
    cluster.assertClean()
  })

  test("acknowledgements and a final on one node leave another node's records alone", async () => {
    const { cluster, model, answers, connect, send, reply } = setup({ nodes: 2 })
    const zero = connect(0)
    const one = connect(1)
    answers.ShareFetch.push(acquiring(0, 0))
    await send(zero, fetch('m1', 0, { topics: topicA([0]) }))
    await send(one, fetch('m1', 0, { topics: topicA([0]) }))

    const elsewhere = await send(one, acknowledge('m1', 1, topicA([0, [batch(0, 0, ACCEPT)]])))
    const final = await send(one, fetch('m1', SESSION_EPOCH_FINAL))

    expect(reply(one, elsewhere).responses[0].partitions[0]).toMatchObject({
      partitionIndex: 0,
      errorCode: 121,
    })
    expect(reply(one, final).errorCode).toBe(0)
    expect(model.sessions()).toEqual([
      { node: 0, member: 'm1', epoch: 1, generation: 1, partitions: [`${A}:0`] },
    ])
    expect(model.acquisitionsOf(A, 0, 0)).toEqual([
      { observationId: 1, member: 'm1', node: 0, state: 'ACQUIRED', renewals: 0 },
    ])

    const home = await send(zero, acknowledge('m1', 1, topicA([0, [batch(0, 0, ACCEPT)]])))

    expect(reply(zero, home).responses[0].partitions[0].errorCode).toBe(0)
    expect(model.changesTo(1)).toEqual([
      { state: 'ACKNOWLEDGED', cause: 'accept', order: home.executedOrder },
    ])
    cluster.assertClean()
  })

  test('a fetch that acquires an acknowledged offset again is a recorded failure', async () => {
    const { cluster, model, answers, connect, send } = setup()
    const connection = connect()
    answers.ShareFetch.push(acquiring(0, 0))
    await send(connection, fetch('m1', 0, { topics: topicA([0]) }))
    await send(connection, acknowledge('m1', 1, topicA([0, [batch(0, 0, ACCEPT)]])))

    answers.ShareFetch.push(acquiring(0, 0))
    await send(connection, fetch('m1', 2))

    expect(failed(cluster)).toEqual([
      expect.stringContaining(`${A}:0:0 acquired while ACKNOWLEDGED`),
    ])
    expect(model.acquisitionsOf(A, 0, 0)).toEqual([
      { observationId: 1, member: 'm1', node: 0, state: 'ACKNOWLEDGED', renewals: 0 },
    ])
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

    expect(reply(connection, standalone).responses[0].partitions[0]).toMatchObject({
      partitionIndex: 0,
      errorCode: 121,
    })
    expect(reply(connection, piggybacked).responses[0].partitions[0]).toMatchObject({
      partitionIndex: 0,
      errorCode: 0,
      acknowledgeErrorCode: 121,
    })
    for (const offset of [0, 1]) {
      expect(model.acquisitionsOf(A, 0, offset)).toEqual([
        expect.objectContaining({ member: 'm1', state: 'ACQUIRED' }),
      ])
    }
    expect(model.changesTo(1)).toEqual([])
    expect(model.changesTo(2)).toEqual([])
    cluster.assertClean()
  })

  test('an acknowledgement over an offset another member holds is answered 121 while a sibling partition applies', async () => {
    const { cluster, model, answers, connect, send, reply } = setup()
    const connection = connect()
    answers.ShareFetch.push(acquiringBoth())
    await send(connection, fetch('m1', 0, { topics: topicA([0], [1]) }))
    answers.ShareFetch.push(fetchAnswer([part({ partition: 0, acquire: [acquired(1, 1)] })]))
    await send(connect(), fetch('m2', 0, { topics: topicA([0]) }))

    const mixed = await send(
      connection,
      acknowledge('m1', 1, topicA([0, [batch(0, 1, ACCEPT)]], [1, [batch(0, 0, ACCEPT)]]))
    )

    expect(reply(connection, mixed).responses[0].partitions).toEqual([
      expect.objectContaining({ partitionIndex: 0, errorCode: 121 }),
      expect.objectContaining({ partitionIndex: 1, errorCode: 0 }),
    ])
    expect(model.acquisitionsOf(A, 0, 0)).toEqual([
      { observationId: 1, member: 'm1', node: 0, state: 'ACQUIRED', renewals: 0 },
    ])
    expect(model.acquisitionsOf(A, 0, 1)).toEqual([
      { observationId: 3, member: 'm2', node: 0, state: 'ACQUIRED', renewals: 0 },
    ])
    expect(model.changesTo(2)).toEqual([
      { state: 'ACKNOWLEDGED', cause: 'accept', order: mixed.executedOrder },
    ])
    cluster.assertClean()
  })

  test('an acknowledgement past the cached tail or breaking a batch rule is answered 42 and changes nothing', async () => {
    const { cluster, model, answers, connect, send, reply } = setup()
    const connection = connect()
    answers.ShareFetch.push(
      fetchAnswer([
        part({ partition: 0, acquire: [acquired(0, 1)] }),
        part({ partition: 1, acquire: [acquired(0, 0)] }),
      ])
    )
    await send(connection, fetch('m1', 0, { topics: topicA([0], [1]) }))

    const pastTail = await send(
      connection,
      acknowledge('m1', 1, topicA([0, [batch(0, 1, ACCEPT), batch(5, 5, ACCEPT)]]))
    )
    const malformed = await send(
      connection,
      acknowledge('m1', 2, topicA([0, [batch(1, 0, ACCEPT)]], [1, [batch(0, 0, RENEW)]]))
    )

    expect(reply(connection, pastTail).responses[0].partitions).toEqual([
      expect.objectContaining({ partitionIndex: 0, errorCode: 42 }),
    ])
    expect(reply(connection, malformed).responses[0].partitions).toEqual([
      expect.objectContaining({ partitionIndex: 0, errorCode: 42 }),
      expect.objectContaining({ partitionIndex: 1, errorCode: 42 }),
    ])
    for (const offset of [0, 1]) {
      expect(model.acquisitionsOf(A, 0, offset)).toEqual([
        expect.objectContaining({ member: 'm1', state: 'ACQUIRED' }),
      ])
    }
    expect(model.acquisitionsOf(A, 1, 0)).toEqual([
      { observationId: 3, member: 'm1', node: 0, state: 'ACQUIRED', renewals: 0 },
    ])
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
    model.expireLock(1)
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

describe('ScriptedCluster execution hooks', () => {
  test('a kept reply is cancelled once when its socket closes, and teardown settles a hold that never matched', async () => {
    const { cluster, answers, connect, send } = setup()
    const connection = connect()
    answers.ShareFetch.push(acquiring(0, 0))
    await send(connection, fetch('m1', 0, { topics: topicA([0]) }))
    const held = cluster.holdReply({ api: 'ShareAcknowledge' })
    const unmatched = cluster.holdReply({ api: 'ShareGroupHeartbeat' })
    const accept = await send(connection, acknowledge('m1', 1, topicA([0, [batch(0, 0, ACCEPT)]])))
    expect(await held.executed()).toBe(accept)

    cluster.clientLoss(0, 1)
    await flush()

    expect(cluster.tally('ShareAcknowledge', 'cancelled')).toBe(1)
    expect(() => held.deliver()).toThrow('ReplyHold: no kept reply to deliver')

    cluster.cancelReplyHolds()

    await expect(unmatched.executed()).resolves.toBeNull()
    expect(cluster.tally('ShareAcknowledge', 'cancelled')).toBe(1)
    expect(accept.answeredOrder).toBeUndefined()
    cluster.assertClean()
  })

  test('a throwing execution hook or reply selector is a recorded failure that cancels the request and closes its socket', async () => {
    const failure = new Error('hook failed')
    const hooked = setup({ model: false })
    hooked.cluster.hook({
      onExecute: () => {
        throw failure
      },
    })
    hooked.hold = hooked.cluster.holdReply({ api: 'ShareGroupHeartbeat' })
    const selected = setup({ model: false })
    selected.hold = selected.cluster.holdReply({
      api: 'ShareGroupHeartbeat',
      match: () => {
        throw failure
      },
    })

    for (const { cluster, connect, send, hold } of [hooked, selected]) {
      const connection = connect()
      await send(connection, beat('m1', 0))

      expect(cluster.failures()).toEqual([failure])
      expect(cluster.tally('ShareGroupHeartbeat', 'cancelled')).toBe(1)
      expect(connection.socket.destroyed).toBe(true)
      expect(connection.errors).toEqual([failure])
      expect(connection.replies).toEqual([])
      await expect(
        Promise.race([
          hold.executed(),
          flush().then(() => Promise.reject(new Error('the hold is still waiting'))),
        ])
      ).resolves.toBeNull()
    }
  })

  test('split socket loss is refused in bounded capture', () => {
    const cluster = createScriptedCluster({
      nodes: [{ id: 0, host: 'node0', port: 9092 }],
      profile: 'share',
      bounded: true,
    })
    cluster.socketFactory({ host: 'node0', port: 9092, onConnect: () => {} })

    expect(() => cluster.clientLoss(0, 1)).toThrow(
      'ScriptedCluster: clientLoss() needs the full history'
    )
  })
})
