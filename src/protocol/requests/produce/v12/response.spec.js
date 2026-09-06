const responseV12 = require('./response')
const responseV11 = require('../v11/response')

describe('Protocol > Requests > Produce > v12', () => {
  test('decodes v11 bytes identically and shares its parse', async () => {
    const raw = Buffer.from(require('../fixtures/v11_response.json'))

    expect(await responseV12.decode(raw)).toEqual(await responseV11.decode(raw))
    expect(responseV12.parse).toBe(responseV11.parse)
  })

  test('decodes the vector Apache own codec produced', async () => {
    const data = await responseV12.decode(
      Buffer.from(require('../fixtures/v12_response_java.json'))
    )

    expect(data.topics[0].topicName).toEqual('test-topic')
    expect(data.topics[0].partitions[0]).toEqual({
      partition: 0,
      errorCode: 0,
      baseOffset: '100',
      logAppendTime: '-1',
      logStartOffset: '0',
      recordErrors: [],
      errorMessage: null,
      currentLeader: { leaderId: 2, leaderEpoch: 7 },
    })
    expect(data.nodeEndpoints).toEqual([{ nodeId: 2, host: '127.0.0.1', port: 9092, rack: null }])
  })
})
