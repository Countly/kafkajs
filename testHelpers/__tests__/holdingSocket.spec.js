const Encoder = require('../../src/protocol/encoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const heartbeatRequest = require('../../src/protocol/requests/shareGroupHeartbeat/v1/request')
const heartbeatWire = require('../wire/shareGroupHeartbeatV1')
const { createScriptedCluster } = require('../scriptedCluster')
const { createHoldingSocketFactory } = require('../holdingSocket')
const { flush, heartbeatAnswer } = require('../shareHarness')

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
})
