const { decode, parse } = require('./response')
const { decode: decodeV3 } = require('../v3/response')
const Encoder = require('../../../encoder')
const { KafkaJSPartialMessageError } = require('../../../../errors')

/**
 * @param {Encoder[]|null} topics encoded topic entries, or null for the null array
 * @returns {Buffer} a v4 response body with throttle time 0
 */
const answer = (topics) =>
  new Encoder().writeInt32(0).writeUVarIntArray(topics, 'object').writeTaggedFields().buffer

/**
 * @param {Encoder[]|null} partitions encoded partition entries, or null for the null array
 * @returns {Encoder} one topic entry named `test-topic`
 */
const topicEntry = (partitions) =>
  new Encoder()
    .writeUVarIntString('test-topic')
    .writeUVarIntArray(partitions, 'object')
    .writeTaggedFields()

describe('Protocol > Requests > OffsetForLeaderEpoch > v4', () => {
  test('decodes to the same shape v3 produces', async () => {
    const v4 = await decode(Buffer.from(require('../fixtures/v4_response.json')))
    const v3 = await decodeV3(Buffer.from(require('../fixtures/v3_response.json')))

    expect(v4).toEqual(v3)
    await expect(parse(v4)).resolves.toBeTruthy()
  })

  test('parse does not throw on a per-partition error code', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v4_response.json')))
    data.topics[0].partitions[0].errorCode = 29 // TOPIC_AUTHORIZATION_FAILED

    await expect(parse(data)).resolves.toBe(data)
  })

  test('decodes the vector Apache own codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v4_response_java.json')))

    expect(data.topics).toEqual([
      {
        topic: 'test-topic',
        partitions: [{ errorCode: 0, partition: 0, leaderEpoch: 5, endOffset: '120' }],
      },
    ])
  })

  test.each([
    ['Topics', () => answer(null)],
    ['Partitions', () => answer([topicEntry(null)])],
  ])('refuses a null %s array as a malformed answer', async (_, body) => {
    await expect(decode(body())).rejects.toBeInstanceOf(KafkaJSPartialMessageError)
  })

  test('decodes empty Topics and Partitions arrays as before', async () => {
    await expect(decode(answer([]))).resolves.toMatchObject({ topics: [] })
    await expect(decode(answer([topicEntry([])]))).resolves.toMatchObject({
      topics: [{ topic: 'test-topic', partitions: [] }],
    })
  })
})
