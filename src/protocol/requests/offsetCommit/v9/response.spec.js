const { decode, parse } = require('./response')
const responseV8 = require('../v8/response')

const vector = require('../fixtures/java/offsetCommitResponse-v9-basic.json')

/** ThrottleTimeMs (INT32) is the body's first field. */
const THROTTLE_TIME_OFFSET = 0
/** The partition ErrorCode (INT16): ThrottleTimeMs 4, Topics 1, Name 1 + 10, Partitions 1, PartitionIndex 4. */
const PARTITION_ERROR_CODE_OFFSET = 21

/**
 * The Apache vector's own bytes with one field overwritten in place, so a frame the assertions
 * below read was written by Kafka's codec, never by this repo's encoder for its own decoder.
 * @param {(buffer: Buffer) => void} patch
 * @returns {Buffer}
 */
const vectorWith = (patch) => {
  const buffer = Buffer.from(vector.hex, 'hex')
  patch(buffer)
  return buffer
}

describe('Protocol > Requests > OffsetCommit > v9', () => {
  test('decodes the vector the Apache 4.3.1 codec produced into the v8 shape', async () => {
    const data = await decode(Buffer.from(vector.hex, 'hex'))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      responses: [{ topic: 'test-topic', partitions: [{ partition: 0, errorCode: 0 }] }],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('is v8 decode and parse, unchanged: the body gained no field at v9', async () => {
    expect(decode).toBe(responseV8.decode)
    expect(parse).toBe(responseV8.parse)
  })

  test('reports a non-zero throttle as clientSideThrottleTime (KIP-219 split)', async () => {
    const data = await decode(
      vectorWith((buffer) => buffer.writeInt32BE(4321, THROTTLE_TIME_OFFSET))
    )

    expect(data.throttleTime).toEqual(0)
    expect(data.clientSideThrottleTime).toEqual(4321)
  })

  test('throws on STALE_MEMBER_EPOCH, the code v9 exists to carry', async () => {
    const data = await decode(
      vectorWith((buffer) => buffer.writeInt16BE(113, PARTITION_ERROR_CODE_OFFSET))
    )

    expect(data.responses[0].partitions[0].errorCode).toEqual(113)
    await expect(parse(data)).rejects.toMatchObject({
      type: 'STALE_MEMBER_EPOCH',
      retriable: false,
    })
  })
})
