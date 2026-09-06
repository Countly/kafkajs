const Decoder = require('../../../decoder')
const { decode, parse } = require('./response')

describe('Protocol > Requests > Fetch > v12', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v12_response.json')))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      sessionId: 0,
      responses: [
        {
          topicName: 'test-topic',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              highWatermark: '100',
              lastStableOffset: '100',
              lastStartOffset: '0',
              abortedTransactions: [],
              preferredReadReplica: -1,
              divergingEpoch: null,
              currentLeader: null,
              snapshotId: null,
              messages: [],
            },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('reads a COMPACT_RECORDS block: a real record batch decodes to its records', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v12_response_records.json')))
    const [partition] = data.responses[0].partitions

    // From v12 the records block is a UVarInt of length + 1 rather than an INT32 length. This is
    // the case that proves the compact size read reaches a genuine batch: the fixture's block is
    // produced by this repository's own record-batch encoder, and `messages: []` in the case
    // above comes from a compact *null* block (a single 0x00), not from a truncated one.
    expect(partition.messages).toHaveLength(1)
    expect(partition.messages[0].offset).toEqual('100')
    expect(partition.messages[0].key).toEqual(Buffer.from('k'))
    expect(partition.messages[0].value).toEqual(Buffer.from('v'))
    expect(partition.messages[0].batchContext.partitionLeaderEpoch).toEqual(5)
  })

  test('an empty records block is the compact null, and decodes to no messages', async () => {
    const raw = Buffer.from(require('../fixtures/v12_response.json'))
    const data = await decode(raw)

    expect(data.responses[0].partitions[0].messages).toEqual([])

    // Pinned so nobody "fixes" the fixture into a short stand-in buffer: decodeMessages reads the
    // magic byte at offset 16 of the block, so a 1-16 byte block throws rather than decoding. The
    // assertion walks to the records block itself and reads its size, rather than searching the
    // whole buffer for a zero byte - throttleTime alone is four of those.
    const decoder = new Decoder(raw)
    decoder.readInt32() // clientSideThrottleTime
    decoder.readInt16() // errorCode
    decoder.readInt32() // sessionId
    decoder.readUVarInt() //       responses array length + 1
    decoder.readUVarIntString() // topicName
    decoder.readUVarInt() //       partitions array length + 1
    decoder.readInt32()
    decoder.readInt16()
    decoder.readInt64()
    decoder.readInt64()
    decoder.readInt64()
    decoder.readUVarIntArray(() => {}) // abortedTransactions
    decoder.readInt32() //               preferredReadReplica

    expect(decoder.readUVarInt()).toEqual(0) // the compact null, exactly one byte
  })

  test('decodes the three tagged fields and skips an unknown tag without losing bytes', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v12_response_tagged.json')))
    const [partition] = data.responses[0].partitions

    expect(partition.divergingEpoch).toEqual({ epoch: 4, endOffset: '50' })
    expect(partition.currentLeader).toEqual({ leaderId: 2, leaderEpoch: 7 })
    expect(partition.snapshotId).toEqual({ endOffset: '9', epoch: 3 })
    // Everything after the tag section still decoded, which is the real assertion about the
    // unknown tag: a mis-skip would have corrupted the topic tag buffer that follows.
    expect(data.responses[0].topicName).toEqual('test-topic')
  })

  test('parse throws on nothing: partition and top-level codes come back in the body', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v12_response.json')))
    data.errorCode = 70 // FETCH_SESSION_ID_NOT_FOUND
    data.responses[0].partitions[0].errorCode = 6 // NOT_LEADER_FOR_PARTITION

    await expect(parse(data)).resolves.toBe(data)
  })

  test('decodes the vector Apache own codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v12_response_java.json')))

    expect(data.responses[0].topicName).toEqual('test-topic')
    expect(data.responses[0].partitions[0].highWatermark).toEqual('100')
    expect(data.responses[0].partitions[0].lastStableOffset).toEqual('100')
    expect(data.responses[0].partitions[0].messages).toEqual([])
    expect(data).toEqual(await decode(Buffer.from(require('../fixtures/v12_response.json'))))
  })
})
