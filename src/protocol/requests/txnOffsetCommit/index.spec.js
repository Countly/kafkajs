const { protocol, versions } = require('./index')
const { KafkaJSInvariantViolation } = require('../../../errors')

const response = (caseId) => require(`./fixtures/java/txnOffsetCommitResponse-v6-${caseId}.json`)
const request = (caseId) => require(`./fixtures/java/txnOffsetCommitRequest-v6-${caseId}.json`)

const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const OTHER_TOPIC_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'
const REPLACEMENT_ID = '31323334-3536-3738-393a-3b3c3d3e3f40'

/** What the Java vectors commit: two topics, three partitions. */
const TOPICS = [
  {
    topic: 'test-topic',
    partitions: [
      { partition: 0, offset: '1' },
      { partition: 1, offset: '2' },
    ],
  },
  { topic: 'other-topic', partitions: [{ partition: 0, offset: '3' }] },
]

/** A fresh name-to-id map, as the Broker hands one to the builder. */
const topicIds = () =>
  new Map([
    ['test-topic', TOPIC_ID],
    ['other-topic', OTHER_TOPIC_ID],
  ])

/** @returns {{ request: object, response: object }} a v6 protocol request over `map` */
const build = (map = topicIds(), topics = TOPICS) =>
  protocol({ version: 6 })({
    transactionalId: 'test-transactional-id',
    groupId: 'test-group',
    producerId: '1000',
    producerEpoch: 3,
    topics,
    topicIds: map,
  })

describe('Protocol > Requests > TxnOffsetCommit', () => {
  test('offers v0 to v6', () => {
    expect(versions).toEqual(['0', '1', '2', '3', '4', '5', '6'])
  })

  test('v6 takes the ids from the private map and the request bytes name no topic', async () => {
    const { buffer } = await build().request.encode()

    expect(buffer.includes(Buffer.from('test-topic'))).toBe(false)
    expect(buffer.includes(Buffer.from(TOPIC_ID.replace(/-/g, ''), 'hex'))).toBe(true)
  })

  test('v5 ignores the private map: the same arguments are keyed by name', async () => {
    const { request: built } = protocol({ version: 5 })({
      transactionalId: 'test-transactional-id',
      groupId: 'test-group',
      producerId: '1000',
      producerEpoch: 3,
      topics: TOPICS,
      topicIds: topicIds(),
    })
    const { buffer } = await built.encode()

    expect(built.apiVersion).toEqual(5)
    expect(buffer.includes(Buffer.from('test-topic'))).toBe(true)
    expect(buffer.includes(Buffer.from(TOPIC_ID.replace(/-/g, ''), 'hex'))).toBe(false)
  })

  test('v6 binds its response to the topics the request named, when it was built', async () => {
    const map = topicIds()
    const { response: bound } = build(map)
    // What a metadata refresh does while the request is in flight: the ids change under it.
    map.set('test-topic', REPLACEMENT_ID)
    map.set('other-topic', REPLACEMENT_ID)

    const data = await bound.decode(Buffer.from(response('ok').hex, 'hex'))

    expect(data.topics.map(({ topic }) => topic)).toEqual(['test-topic', 'other-topic'])
    await expect(bound.parse(data)).resolves.toBe(data)
  })

  test('v6 names nothing for an id its request did not carry, and refuses that answer', async () => {
    const { response: bound } = build(new Map([['test-topic', TOPIC_ID]]), TOPICS.slice(0, 1))
    const data = await bound.decode(Buffer.from(response('ok').hex, 'hex'))

    expect(data.topics.map(({ topic }) => topic)).toEqual(['test-topic', null])
    await expect(bound.parse(data)).rejects.toThrow(KafkaJSInvariantViolation)
  })

  test('v6 refuses an answer that does not correlate with the request before it reads an error code', async () => {
    const { response: bound } = build(new Map([['test-topic', TOPIC_ID]]), TOPICS.slice(0, 1))
    const data = await bound.decode(Buffer.from(response('staleMemberEpoch').hex, 'hex'))

    expect(data.topics[0].partitions[0].errorCode).toEqual(113)
    await expect(bound.parse(data)).rejects.toThrow(KafkaJSInvariantViolation)
  })

  test('v6 throws the typed protocol error of an exactly correlated answer', async () => {
    const { response: bound } = build()
    const data = await bound.decode(Buffer.from(response('groupIdNotFound').hex, 'hex'))

    await expect(bound.parse(data)).rejects.toMatchObject({
      type: 'GROUP_ID_NOT_FOUND',
      code: 69,
      retriable: false,
    })
  })

  test('v6 encodes the request vector it is built to, through the version map', async () => {
    const { hex, fields } = request('groupMetadata')
    const topics = [
      { topic: 'test-topic', partitions: fields.topics[0].partitions },
      { topic: 'other-topic', partitions: fields.topics[1].partitions },
    ]
    const { request: built } = protocol({ version: 6 })({ ...fields, topics, topicIds: topicIds() })

    expect((await built.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
