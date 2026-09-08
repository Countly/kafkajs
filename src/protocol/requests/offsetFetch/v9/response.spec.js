const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')
const responseV8 = require('../v8/response')

const vector = require('../fixtures/java/offsetFetchResponse-v9-basic.json')

/**
 * Builds a v9 response body; no committed vector carries a non-zero throttle or a failing code.
 * @param {object} options
 * @param {number} options.throttleTime
 * @param {number} options.errorCode group-level error code
 * @returns {Buffer}
 */
const buildResponse = ({ throttleTime = 0, errorCode = 0 } = {}) =>
  new Encoder()
    .writeInt32(throttleTime)
    .writeUVarIntArray([
      new Encoder()
        .writeUVarIntString('test-group')
        .writeUVarIntArray([
          new Encoder()
            .writeUVarIntString('test-topic')
            .writeUVarIntArray([
              new Encoder()
                .writeInt32(0)
                .writeInt64('10')
                .writeInt32(3)
                .writeUVarIntString(null)
                .writeInt16(0)
                .writeTaggedFields(),
            ])
            .writeTaggedFields(),
        ])
        .writeInt16(errorCode)
        .writeTaggedFields(),
    ])
    .writeTaggedFields().buffer

describe('Protocol > Requests > OffsetFetch > v9', () => {
  test('decodes the vector the Apache 4.3.1 codec produced into the v8 shape', async () => {
    const data = await decode(Buffer.from(vector.hex, 'hex'))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
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

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('is v8 decode and parse, unchanged: the body gained no field at v9', async () => {
    expect(decode).toBe(responseV8.decode)
    expect(parse).toBe(responseV8.parse)
  })

  test('reports a non-zero throttle as clientSideThrottleTime (KIP-219 split)', async () => {
    const data = await decode(buildResponse({ throttleTime: 4321 }))

    expect(data.throttleTime).toEqual(0)
    expect(data.clientSideThrottleTime).toEqual(4321)
  })

  test('throws on STALE_MEMBER_EPOCH, the code v9 exists to carry', async () => {
    const data = await decode(buildResponse({ errorCode: 113 }))

    expect(data.errorCode).toEqual(113)
    await expect(parse(data)).rejects.toMatchObject({
      type: 'STALE_MEMBER_EPOCH',
      retriable: false,
    })
  })

  test('keeps the frozen decoded key names the offset manager reads', async () => {
    const data = await decode(Buffer.from(vector.hex, 'hex'))

    expect(Object.keys(data).sort()).toEqual([
      'clientSideThrottleTime',
      'errorCode',
      'responses',
      'throttleTime',
    ])
    expect(Object.keys(data.responses[0].partitions[0])).toContain('committedLeaderEpoch')
  })
})
