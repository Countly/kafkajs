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
})
