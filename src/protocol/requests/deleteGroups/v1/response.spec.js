const Encoder = require('../../../encoder')
const { decode } = require('./response')
const { decode: decodeV0 } = require('../v0/response')

/** A v0 and v1 response body: the throttle time, then the group ids with their error codes. */
const body = (throttleTime, results) =>
  new Encoder()
    .writeInt32(throttleTime)
    .writeArray(
      results.map(([groupId, errorCode]) =>
        new Encoder().writeString(groupId).writeInt16(errorCode)
      )
    ).buffer

describe('Protocol > Requests > DeleteGroups > v1 response', () => {
  test('reports the broker throttle time as clientSideThrottleTime, a number', async () => {
    const data = await decode(body(25, [['test-group', 0]]))

    expect(data).toEqual({
      throttleTimeMs: 25,
      throttleTime: 0,
      clientSideThrottleTime: 25,
      results: [{ groupId: 'test-group', errorCode: 0 }],
    })
  })

  test('reports a throttle time of 0 as 0, not as undefined', async () => {
    expect((await decode(body(0, []))).clientSideThrottleTime).toBe(0)
  })

  test('keeps the keys v0 decodes and the error of a failed group', async () => {
    const bytes = body(7, [['g', 69]])
    const v0 = await decodeV0(bytes)
    const v1 = await decode(bytes)

    expect(v1.throttleTimeMs).toEqual(v0.throttleTimeMs)
    expect(v1.results).toEqual(v0.results)
    expect(v1.results[0].error.type).toEqual('GROUP_ID_NOT_FOUND')
  })
})
