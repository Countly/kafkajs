const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')
const { protocol } = require('../index')
const { KafkaJSInvariantViolation } = require('../../../../errors')

const ok = require('../fixtures/java/offsetFetchResponse-v10-ok.json')
const unknownTopicId = require('../fixtures/java/offsetFetchResponse-v10-unknownTopicId.json')

const TOPIC_ID = ok.fields.topics[0].topicId
const TOPIC_NAMES = new Map([[TOPIC_ID, 'test-topic']])

/**
 * A v10 body built from the 4.3.1 definition with `Encoder`, for the cases no vector covers.
 * @param {object} options
 * @param {object[]} [options.groups] each `{ groupId, errorCode, topics: [{ topicId, partitions }] }`
 * @returns {Buffer}
 */
const body = ({ groups }) =>
  new Encoder()
    .writeInt32(25)
    .writeUVarIntArray(
      groups.map(({ groupId, errorCode = 0, topics = [] }) =>
        new Encoder()
          .writeUVarIntString(groupId)
          .writeUVarIntArray(
            topics.map(({ topicId, partitions }) =>
              new Encoder()
                .writeUuid(topicId)
                .writeUVarIntArray(
                  partitions.map(({ partition, offset, epoch, metadata, errorCode: code = 0 }) =>
                    new Encoder()
                      .writeInt32(partition)
                      .writeInt64(offset)
                      .writeInt32(epoch)
                      .writeUVarIntString(metadata)
                      .writeInt16(code)
                      .writeTaggedFields()
                  )
                )
                .writeTaggedFields()
            )
          )
          .writeInt16(errorCode)
          .writeTaggedFields()
      )
    )
    .writeTaggedFields().buffer

describe('Protocol > Requests > OffsetFetch > v10 response', () => {
  test('decodes the ok vector into the v8 shape, naming the topic from the request map', async () => {
    const data = await decode(Buffer.from(ok.hex, 'hex'), { topicNames: TOPIC_NAMES })

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: ok.fields.throttleTime,
      errorCode: 0,
      responses: [
        {
          topic: 'test-topic',
          partitions: [
            { partition: 0, offset: '10', committedLeaderEpoch: 3, metadata: null, errorCode: 0 },
          ],
        },
      ],
    })
    await expect(parse(data)).resolves.toBe(data)
  })

  test('keeps an id the request did not carry, with a null topic, as an all-topics answer has', async () => {
    const data = await decode(Buffer.from(ok.hex, 'hex'))

    expect(data.responses).toEqual([
      expect.objectContaining({ topic: null, topicId: TOPIC_ID, partitions: expect.any(Array) }),
    ])
  })

  test('hands UNKNOWN_TOPIC_ID to parse as a retriable protocol error', async () => {
    const data = await decode(Buffer.from(unknownTopicId.hex, 'hex'), { topicNames: TOPIC_NAMES })

    expect(data.responses[0].partitions[0]).toMatchObject({ offset: '-1', errorCode: 100 })
    await expect(parse(data)).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_ID',
      code: 100,
      retriable: true,
    })
  })

  test('hands a group-level error to parse, as v9 does for STALE_MEMBER_EPOCH', async () => {
    const data = await decode(body({ groups: [{ groupId: 'g', errorCode: 113 }] }))

    expect(data.errorCode).toEqual(113)
    await expect(parse(data)).rejects.toMatchObject({ type: 'STALE_MEMBER_EPOCH' })
  })

  test('keeps an empty metadata string empty and a long offset exact', async () => {
    const data = await decode(
      body({
        groups: [
          {
            groupId: 'g',
            topics: [
              {
                topicId: TOPIC_ID,
                partitions: [
                  { partition: 2, offset: '9007199254740993', epoch: 0, metadata: '' },
                  { partition: 3, offset: '1', epoch: -1, metadata: 'note' },
                ],
              },
            ],
          },
        ],
      }),
      { topicNames: TOPIC_NAMES }
    )

    expect(data.responses[0].partitions).toEqual([
      {
        partition: 2,
        offset: '9007199254740993',
        committedLeaderEpoch: 0,
        metadata: '',
        errorCode: 0,
      },
      { partition: 3, offset: '1', committedLeaderEpoch: -1, metadata: 'note', errorCode: 0 },
    ])
  })

  test.each([[0], [2]])('throws an invariant violation for %i groups', async (count) => {
    const groups = Array.from({ length: count }, (_, index) => ({ groupId: `g${index}` }))

    await expect(decode(body({ groups }))).rejects.toThrow(KafkaJSInvariantViolation)
  })

  test('fails with a typed error, never a native one, on every truncation of the vector', async () => {
    const buffer = Buffer.from(ok.hex, 'hex')

    for (let length = 0; length < buffer.length; length++) {
      await expect(decode(buffer.subarray(0, length), { topicNames: TOPIC_NAMES })).rejects.toThrow(
        expect.objectContaining({ name: expect.stringMatching(/^KafkaJS/) })
      )
    }
  })

  test('binds the request map through the version map, per request', async () => {
    const build = (name) =>
      protocol({ version: 10 })({
        groupId: 'g',
        topics: [{ topic: name, partitions: [{ partition: 0 }] }],
        topicIds: new Map([[name, TOPIC_ID]]),
      })

    const first = await build('first').response.decode(Buffer.from(ok.hex, 'hex'))
    const second = await build('second').response.decode(Buffer.from(ok.hex, 'hex'))

    expect([first.responses[0].topic, second.responses[0].topic]).toEqual(['first', 'second'])
  })
})
