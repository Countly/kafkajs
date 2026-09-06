const responseV13 = require('./response')

const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const topicNames = () => new Map([[TOPIC_ID, 'test-topic']])

describe('Protocol > Requests > Produce > v13', () => {
  test('restores the topic name during decode, before parse can read it', async () => {
    const data = await responseV13.decode(Buffer.from(require('../fixtures/v13_response.json')), {
      topicNames: topicNames(),
    })

    expect(data.topics[0].topicName).toEqual('test-topic')
    expect(data.topics[0].topicId).toEqual(TOPIC_ID)
    expect(data.topics[0].partitions[0]).toMatchObject({
      partition: 0,
      errorCode: 0,
      baseOffset: '100',
    })
    await expect(responseV13.parse(data)).resolves.toBeTruthy()
  })

  test('a failure carries the topic name, which is what keeps recordErrors attachable', async () => {
    const data = await responseV13.decode(
      Buffer.from(require('../fixtures/v13_response_error.json')),
      { topicNames: topicNames() }
    )

    await expect(responseV13.parse(data)).rejects.toMatchObject({
      type: 'NOT_LEADER_FOR_PARTITION',
      topic: 'test-topic',
      partition: 0,
    })
  })

  test('an id with no mapping leaves topicName null rather than throwing from a codec', async () => {
    const data = await responseV13.decode(Buffer.from(require('../fixtures/v13_response.json')), {
      topicNames: new Map(),
    })

    expect(data.topics[0].topicName).toBeNull()
  })
})
