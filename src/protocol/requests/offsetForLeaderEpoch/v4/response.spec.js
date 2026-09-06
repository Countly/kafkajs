const { decode, parse } = require('./response')
const { decode: decodeV3 } = require('../v3/response')

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
})
