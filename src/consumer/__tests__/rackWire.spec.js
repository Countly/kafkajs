const { EventEmitter } = require('events')
const Encoder = require('../../protocol/encoder')
const createRequest = require('../../protocol/request')
const apiKeys = require('../../protocol/requests/apiKeys')
const { requests } = require('../../protocol/requests')
const { MemberAssignment } = require('../assignerProtocol')
const { WireTranscript, bindTopic } = require('./fixtures/rackWire')
const { fetchResponse } = require('./fixtures/fetchHarness')

const TOPIC = 'test-topic'
const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'

/**
 * @param {number} correlationId
 * @param {number[]} partitions partitions the Fetch asks for
 * @returns {Promise<Buffer>} one framed Fetch v18 request naming the harness topic by id
 */
const fetchRequest = async (correlationId, partitions) => {
  const { request } = requests.Fetch.protocol({ version: 18 })({
    maxWaitTime: 500,
    minBytes: 1,
    maxBytes: 1024,
    rackId: 'rack-a',
    topicIds: new Map([[TOPIC, TOPIC_ID]]),
    topics: [
      {
        topic: TOPIC,
        partitions: partitions.map((partition) => ({
          partition,
          fetchOffset: '0',
          maxBytes: 1024,
        })),
      },
    ],
  })
  return (await createRequest({ correlationId, clientId: 'rack-wire', request })).buffer
}

/**
 * @param {number} correlationId
 * @param {object[]} partitions what the harness response carries
 * @returns {Buffer} one framed Fetch v18 answer: size, correlation id, empty tag buffer, body
 */
const fetchAnswer = (correlationId, partitions) => {
  const body = fetchResponse(partitions, { apiVersion: 18 })
  return new Encoder()
    .writeInt32(4 + 1 + body.length)
    .writeInt32(correlationId)
    .writeInt8(0)
    .writeBuffer(body).buffer
}

/** @returns {EventEmitter} a socket whose writes are discarded */
const plainSocket = () => {
  const socket = new EventEmitter()
  socket.write = jest.fn()
  return socket
}

/**
 * @param {number} apiKey
 * @param {number} apiVersion a version with the plain request header
 * @param {number} correlationId
 * @returns {Buffer} a request frame that is only its header
 */
const headerOnly = (apiKey, apiVersion, correlationId) => {
  const header = new Encoder()
    .writeInt16(apiKey)
    .writeInt16(apiVersion)
    .writeInt32(correlationId)
    .writeString('rack-wire')
  return new Encoder().writeInt32(header.size()).writeEncoder(header).buffer
}

describe('Consumer > rack wire transcript', () => {
  it('pairs a Fetch with its answer across split chunks and names the hinted follower', async () => {
    const transcript = new WireTranscript(plainSocket)
    transcript.nodes.set('localhost:9095', 2)
    const socket = transcript.socketFactory({ host: 'localhost', port: 9095 })
    socket.write(await fetchRequest(7, [1, 2]))
    const answer = fetchAnswer(7, [
      { partition: 1, errorCode: 0, preferredReadReplica: 1 },
      { partition: 2, errorCode: 0, preferredReadReplica: -1 },
    ])
    socket.emit('data', answer.subarray(0, 9))
    socket.emit('data', answer.subarray(9))

    const hints = await transcript.fetchHints()
    expect(hints).toHaveLength(1)
    expect(hints[0]).toMatchObject({
      node: 2,
      topicId: TOPIC_ID,
      partition: 1,
      preferredReadReplica: 1,
    })
    expect(hints[0].exchange.fetch.rackId).toBe('rack-a')
  })

  it('finds the first Fetch written after the answer that asks for the partition', async () => {
    const transcript = new WireTranscript(plainSocket)
    const leader = transcript.socketFactory({ host: 'localhost', port: 9095 })
    const follower = transcript.socketFactory({ host: 'localhost', port: 9092 })
    leader.write(await fetchRequest(1, [1]))
    leader.emit('data', fetchAnswer(1, [{ partition: 1, errorCode: 0, preferredReadReplica: 1 }]))
    follower.write(await fetchRequest(1, [2]))
    follower.write(await fetchRequest(2, [1]))

    const [hint] = await transcript.fetchHints()
    expect(transcript.nextFetch(hint)).toMatchObject({
      endpoint: 'localhost:9092',
      correlationId: 2,
    })
    expect(transcript.nextFetch({ ...hint, partition: 3 })).toBeUndefined()
  })

  it('names the asked topic entry of a real Fetch v18 request, by id', async () => {
    const transcript = new WireTranscript(plainSocket)
    const socket = transcript.socketFactory({ host: 'localhost', port: 9092 })
    socket.write(await fetchRequest(1, [1, 4]))
    const [exchange] = transcript.exchanges

    expect(exchange.apiVersion).toBe(18)
    expect(transcript.askedTopic(exchange, { topicId: TOPIC_ID })).toMatchObject({
      topicId: TOPIC_ID,
      partitions: [{ partition: 1 }, { partition: 4 }],
    })
    expect(transcript.askedTopic(exchange, { topicId: 'other' })).toBeUndefined()
  })

  it('keeps a SyncGroup answer and drops the answers of other apis', () => {
    const transcript = new WireTranscript(plainSocket)
    const socket = transcript.socketFactory({ host: 'localhost', port: 9092 })
    socket.write(headerOnly(apiKeys.Heartbeat, 3, 3))
    socket.write(headerOnly(apiKeys.SyncGroup, 3, 4))
    const answer = (correlationId) =>
      new Encoder().writeInt32(6).writeInt32(correlationId).writeInt16(0).buffer
    socket.emit('data', Buffer.concat([answer(3), answer(4)]))
    expect(transcript.exchanges[0].answer).toBeUndefined()
    expect(transcript.lastAnswered(apiKeys.SyncGroup, 1).answer.frame).toEqual(answer(4))
  })

  it('binds the topic of a Java assignment vector without touching its other bytes', () => {
    const vector = '0003000000010003742d61000000020000000200000003ffffffff'
    const bound = bindTopic(vector, 'e2e-p9c-rack-aware-x')
    const decoded = MemberAssignment.decode(Buffer.from(bound, 'hex'))
    expect(decoded.assignment).toEqual({ 'e2e-p9c-rack-aware-x': [2, 3] })
    expect(bound.endsWith('000000020000000200000003ffffffff')).toBe(true)
  })
})
