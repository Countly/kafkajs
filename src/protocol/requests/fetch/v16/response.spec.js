const { decode, parse } = require('./response')
const { decode: decodeV15 } = require('../v15/response')

describe('Protocol > Requests > Fetch > v16', () => {
  test('decodes NodeEndpoints from the response body tag section', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v16_response_node_endpoints.json')))

    expect(data.nodeEndpoints).toEqual([
      { nodeId: 2, host: 'kafka2', port: 9095, rack: null },
      { nodeId: 3, host: 'kafka3', port: 9098, rack: 'eu-1' },
    ])
    expect(data.responses[0].partitions[0].currentLeader).toEqual({ leaderId: 2, leaderEpoch: 7 })
    await expect(parse(data)).resolves.toBe(data)
  })

  test('a response with no endpoints decodes to an empty list, not undefined', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v13_response.json')))
    expect(data.nodeEndpoints).toEqual([])
  })

  test('v15 does not decode node endpoints at all', async () => {
    const data = await decodeV15(Buffer.from(require('../fixtures/v13_response.json')))
    expect(data.nodeEndpoints).toBeUndefined()
  })

  test('decodes the vector Apache own codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v16_response_java.json')))

    expect(data.responses[0].topicId).toEqual('01020304-0506-0708-090a-0b0c0d0e0f10')
    expect(data.responses[0].partitions[0].highWatermark).toEqual('100')
    expect(data.nodeEndpoints).toEqual([{ nodeId: 2, host: '127.0.0.1', port: 9092, rack: null }])
  })
})
