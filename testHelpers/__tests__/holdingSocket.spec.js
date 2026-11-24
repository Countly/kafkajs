const Encoder = require('../../src/protocol/encoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const heartbeatRequest = require('../../src/protocol/requests/shareGroupHeartbeat/v1/request')
const heartbeatWire = require('../wire/shareGroupHeartbeatV1')
const { createScriptedCluster } = require('../scriptedCluster')
const { createFakeSocketFactory } = require('../fakeSocket')
const { createHoldingSocketFactory } = require('../holdingSocket')
const { flush, heartbeatAnswer } = require('../shareHarness')

const HEARTBEAT = { apiKey: apiKeys.ShareGroupHeartbeat }

let correlationId = 0
/**
 * @param {string} memberId
 * @returns {Promise<{ id: number, frame: Buffer }>} a ShareGroupHeartbeat v1 join, framed as the
 *   client writes it
 */
const joinFrame = async (memberId) => {
  const id = ++correlationId
  const header = new Encoder()
    .writeInt16(apiKeys.ShareGroupHeartbeat)
    .writeInt16(1)
    .writeInt32(id)
    .writeString('holding')
    .writeTaggedFields()
  const body = await heartbeatRequest({
    groupId: 'group',
    memberId,
    memberEpoch: 0,
    subscribedTopicNames: [],
  }).encode()
  const framed = new Encoder().writeEncoder(header).writeEncoder(body)
  return { id, frame: new Encoder().writeInt32(framed.size()).writeEncoder(framed).buffer }
}

/** @returns {object} a decoded ShareGroupHeartbeat v1 response frame */
const readAnswer = (frame) => heartbeatWire.readResponse(frame.subarray(9))

/**
 * A scripted share broker behind a holding socket factory, and one client-side socket to node 0
 * that records what the client would see.
 * @returns {object} `cluster`, `factory`, `socket`, `seen` (`data` frames and event names)
 */
const setup = () => {
  const cluster = createScriptedCluster({
    nodes: [{ id: 0, host: 'node0', port: 9092 }],
    profile: 'share',
  })
  const context = { interval: 50, epochs: new Map(), topics: new Map() }
  cluster.answer('ShareGroupHeartbeat', (fields) => heartbeatAnswer(fields, context))
  const factory = createHoldingSocketFactory(cluster.socketFactory)
  const socket = factory({ host: 'node0', port: 9092, onConnect: () => {} })
  const seen = { data: [], events: [] }
  socket.on('data', (frame) => seen.data.push(frame))
  for (const event of ['end', 'close', 'error']) socket.on(event, () => seen.events.push(event))
  return { cluster, factory, socket, seen }
}

/**
 * A holding socket over an in-memory broker side the test answers by emitting `data` chunks.
 * @returns {object} `factory`, `socket`, `upstream` (the fake broker-side socket, which records
 *   `writes`), `seen` (`data` frames and event names the client saw)
 */
const inMemory = () => {
  const factory = createHoldingSocketFactory(createFakeSocketFactory().factory)
  const socket = factory({ host: 'node0', port: 9092, onConnect: () => {} })
  const seen = { data: [], events: [] }
  socket.on('data', (frame) => seen.data.push(frame))
  for (const event of ['end', 'close', 'error']) socket.on(event, () => seen.events.push(event))
  return { factory, socket, upstream: socket.upstream, seen }
}

/**
 * @param {number} id the correlation id
 * @param {string} [body='']
 * @returns {Buffer} a response frame: size, correlation id, body
 */
const responseFrame = (id, body = '') => {
  const frame = Buffer.alloc(8 + body.length)
  frame.writeInt32BE(4 + body.length, 0)
  frame.writeInt32BE(id, 4)
  frame.write(body, 8)
  return frame
}

/**
 * @param {Promise} promise
 * @returns {Promise} the promise, or a rejection naming it still pending after the turns of `flush`
 */
const settled = (promise) =>
  Promise.race([
    promise,
    flush().then(() => Promise.reject(new Error('still pending after flush'))),
  ])

describe('HoldingSocket', () => {
  test('holds one frame, reports the client side closed and forwards the frame upstream later', async () => {
    const { cluster, factory, socket, seen } = setup()
    try {
      const hold = factory.holdRequest({ apiKey: apiKeys.ShareGroupHeartbeat })
      const { frame } = await joinFrame('held')
      socket.write(frame)
      expect(await hold.arrived()).toBe(hold)
      expect(hold.socket).toBe(socket)
      await flush()
      expect(cluster.requests()).toEqual([])

      socket.closeClientSide()
      await flush()
      expect(seen.events).toEqual(['end', 'close'])
      expect(socket.destroyed).toBe(true)
      expect(socket.upstream.destroyed).toBe(false)

      const answered = hold.forward()
      await flush()

      expect(cluster.requests()).toEqual([
        expect.objectContaining({ api: 'ShareGroupHeartbeat', fields: expect.anything() }),
      ])
      expect(cluster.requests()[0].fields.memberId).toBe('held')
      await expect(answered).resolves.toBeInstanceOf(Buffer)
      expect(seen.data).toEqual([])
      cluster.assertClean()
    } finally {
      factory.closeUpstreams()
    }
  })

  test('reports the upstream response to a forwarded frame', async () => {
    const { cluster, factory, socket, seen } = setup()
    try {
      const hold = factory.holdRequest({ apiKey: apiKeys.ShareGroupHeartbeat })
      const held = await joinFrame('held')
      const passing = await joinFrame('passing')
      socket.write(held.frame)
      socket.write(passing.frame)
      await hold.arrived()
      await flush()
      expect(seen.data.map((frame) => frame.readInt32BE(4))).toEqual([passing.id])

      const response = await Promise.race([
        hold.forward(),
        flush().then(() => Promise.reject(new Error('no response to the forwarded frame'))),
      ])

      expect(response.readInt32BE(4)).toBe(held.id)
      expect(readAnswer(response)).toMatchObject({ errorCode: 0, memberId: 'held', memberEpoch: 1 })
      expect(seen.data.map((frame) => frame.readInt32BE(4))).toEqual([passing.id])
      cluster.assertClean()
    } finally {
      factory.closeUpstreams()
    }
  })

  test('exposes no inner property on the client-facing socket', () => {
    const { cluster, factory, socket } = setup()
    try {
      expect('inner' in socket).toBe(false)
      expect(socket.inner).toBeUndefined()
      expect(socket.upstream).toBe(cluster.sockets()[0])
    } finally {
      factory.closeUpstreams()
    }
  })

  test('keeps the broker side open past a client close only while a held frame needs it', async () => {
    const holding = setup()
    const idle = setup()
    try {
      holding.factory.holdRequest({ apiKey: apiKeys.ShareGroupHeartbeat })
      holding.socket.write((await joinFrame('held')).frame)

      holding.socket.destroy()
      idle.socket.destroy()
      await flush()

      expect(holding.seen.events).toEqual(['close'])
      expect(holding.socket.upstream.destroyed).toBe(false)
      expect(idle.seen.events).toEqual(['close'])
      expect(idle.socket.upstream.destroyed).toBe(true)
    } finally {
      holding.factory.closeUpstreams()
      idle.factory.closeUpstreams()
    }
  })

  test('withholds one response from the client and reports it', async () => {
    const { cluster, factory, socket, seen } = setup()
    try {
      const withheld = factory.withholdResponse({ apiKey: apiKeys.ShareGroupHeartbeat })
      const first = await joinFrame('first')
      const second = await joinFrame('second')

      socket.write(first.frame)
      socket.write(second.frame)
      await flush()

      expect((await withheld.arrived()).readInt32BE(4)).toBe(first.id)
      expect(seen.data.map((frame) => frame.readInt32BE(4))).toEqual([second.id])
      expect(cluster.requests()).toHaveLength(2)
      cluster.assertClean()
    } finally {
      factory.closeUpstreams()
    }
  })

  test('closes the broker side once the last kept frame after a client close has its response', async () => {
    const { factory, socket, upstream } = inMemory()
    try {
      const hold = factory.holdRequest(HEARTBEAT)
      const { id, frame } = await joinFrame('held')
      socket.write(frame)
      await hold.arrived()
      socket.destroy()
      await flush()
      expect(upstream.destroyed).toBe(false)

      const answered = hold.forward()
      expect(upstream.writes).toEqual([frame])
      upstream.emit('data', responseFrame(id))

      expect(await settled(answered)).toEqual(responseFrame(id))
      expect(upstream.destroyed).toBe(true)
    } finally {
      factory.closeUpstreams()
    }
  })

  test('rejects a forward when the broker side ends before the response, and one after it closed', async () => {
    const { factory, socket, upstream } = inMemory()
    try {
      const first = factory.holdRequest(HEARTBEAT)
      const second = factory.holdRequest(HEARTBEAT)
      socket.write((await joinFrame('first')).frame)
      socket.write((await joinFrame('second')).frame)
      await second.arrived()

      const answered = first.forward()
      upstream.emit('end')

      await expect(settled(answered)).rejects.toThrow('HoldingSocket: the broker side closed')
      await expect(settled(second.forward())).rejects.toThrow(
        'HoldingSocket: the broker side closed'
      )
    } finally {
      factory.closeUpstreams()
    }
  })

  test('passes split and coalesced frames whole in both directions', async () => {
    const { factory, socket, upstream, seen } = inMemory()
    try {
      const [a, b, c] = await Promise.all(['a', 'b', 'c'].map((member) => joinFrame(member)))
      socket.write(a.frame.subarray(0, 7))
      socket.write(a.frame.subarray(7))
      socket.write(Buffer.concat([b.frame, c.frame]))

      expect(upstream.writes).toEqual([a.frame, b.frame, c.frame])

      const replies = [responseFrame(a.id, 'x'), responseFrame(b.id, 'yy'), responseFrame(c.id)]
      const stream = Buffer.concat(replies)
      upstream.emit('data', stream.subarray(0, 3))
      upstream.emit('data', stream.subarray(3, replies[0].length + 2))
      upstream.emit('data', stream.subarray(replies[0].length + 2))

      expect(seen.data).toEqual(replies)
    } finally {
      factory.closeUpstreams()
    }
  })

  test('withholds the response its predicate picks from the request frame', async () => {
    const { factory, socket, upstream, seen } = inMemory()
    try {
      const first = await joinFrame('first')
      const second = await joinFrame('second')
      const withheld = factory.withholdResponse({
        ...HEARTBEAT,
        match: (request) => request.readInt32BE(8) === second.id,
      })
      socket.write(first.frame)
      socket.write(second.frame)

      upstream.emit('data', responseFrame(first.id))
      upstream.emit('data', responseFrame(second.id))

      expect(await settled(withheld.arrived())).toEqual(responseFrame(second.id))
      expect(seen.data).toEqual([responseFrame(first.id)])
    } finally {
      factory.closeUpstreams()
    }
  })

  test('teardown settles unmatched holds, rejects a pending forward and closes every broker side', async () => {
    const { factory, socket, upstream } = inMemory()
    const kept = factory.holdRequest(HEARTBEAT)
    socket.write((await joinFrame('kept')).frame)
    await kept.arrived()
    const answered = kept.forward()
    const other = factory({ host: 'node1', port: 9092, onConnect: () => {} })
    const stranded = factory.holdRequest(HEARTBEAT)
    other.write((await joinFrame('stranded')).frame)
    await stranded.arrived()
    const request = factory.holdRequest(HEARTBEAT)
    const response = factory.withholdResponse(HEARTBEAT)

    factory.closeUpstreams()

    await expect(settled(answered)).rejects.toThrow('HoldingSocket: the broker side closed')
    await expect(settled(request.arrived())).resolves.toBeNull()
    await expect(settled(response.arrived())).resolves.toBeNull()
    expect(upstream.destroyed).toBe(true)
    expect(other.upstream.destroyed).toBe(true)
    expect(stranded.frame).toBeNull()
    await expect(settled(stranded.forward())).rejects.toThrow(
      'HoldingSocket: the broker side closed'
    )
    expect(other.upstream.writes).toEqual([])
  })

  test('rejects a forward made in the turn the broker side was destroyed, and writes nothing to it', async () => {
    const { factory, socket, upstream } = inMemory()
    try {
      const hold = factory.holdRequest(HEARTBEAT)
      socket.write((await joinFrame('held')).frame)
      await hold.arrived()

      upstream.destroy()
      const answered = hold.forward()

      expect(upstream.writes).toEqual([])
      await expect(settled(answered)).rejects.toThrow('HoldingSocket: the broker side closed')
    } finally {
      factory.closeUpstreams()
    }
  })
})
