const { decode, parse } = require('./response')
const { protocol } = require('../index')

const ok = require('../fixtures/java/offsetCommitResponse-v10-ok.json')
const unknownTopicId = require('../fixtures/java/offsetCommitResponse-v10-unknownTopicId.json')

const TOPIC_ID = ok.fields.topics[0].topicId
const TOPIC_NAMES = new Map([[TOPIC_ID, 'test-topic']])

describe('Protocol > Requests > OffsetCommit > v10 response', () => {
  test('decodes the ok vector into the v8 shape, naming the topic from the request map', async () => {
    const data = await decode(Buffer.from(ok.hex, 'hex'), { topicNames: TOPIC_NAMES })

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: ok.fields.throttleTime,
      responses: [{ topic: 'test-topic', partitions: [{ partition: 0, errorCode: 0 }] }],
    })
    await expect(parse(data)).resolves.toBe(data)
  })

  test('keeps an id the request did not carry, with a null topic, and never throws for it', async () => {
    const data = await decode(Buffer.from(ok.hex, 'hex'), { topicNames: new Map() })

    expect(data.responses).toEqual([
      { topic: null, topicId: TOPIC_ID, partitions: [{ partition: 0, errorCode: 0 }] },
    ])
  })

  test('decodes without any options, naming nothing', async () => {
    const data = await decode(Buffer.from(ok.hex, 'hex'))

    expect(data.responses[0]).toMatchObject({ topic: null, topicId: TOPIC_ID })
  })

  test('hands UNKNOWN_TOPIC_ID to parse as a retriable protocol error', async () => {
    const data = await decode(Buffer.from(unknownTopicId.hex, 'hex'), { topicNames: TOPIC_NAMES })

    expect(data.responses[0].partitions[0].errorCode).toEqual(100)
    await expect(parse(data)).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_ID',
      code: 100,
      retriable: true,
    })
  })

  test('fails with a typed error, never a native one, on every truncation of the vector', async () => {
    const body = Buffer.from(ok.hex, 'hex')

    for (let length = 0; length < body.length; length++) {
      await expect(decode(body.subarray(0, length), { topicNames: TOPIC_NAMES })).rejects.toThrow(
        expect.objectContaining({ name: expect.stringMatching(/^KafkaJS/) })
      )
    }
  })

  test('binds the request map through the version map, per request', async () => {
    const topics = [{ topic: 'test-topic', partitions: [{ partition: 0, offset: '10' }] }]
    const build = (name) =>
      protocol({ version: 10 })({
        groupId: 'g',
        groupGenerationId: 1,
        memberId: 'm',
        topics: [{ ...topics[0], topic: name }],
        topicIds: new Map([[name, TOPIC_ID]]),
      })

    const first = await build('first').response.decode(Buffer.from(ok.hex, 'hex'))
    const second = await build('second').response.decode(Buffer.from(ok.hex, 'hex'))

    expect([first.responses[0].topic, second.responses[0].topic]).toEqual(['first', 'second'])
  })
})
