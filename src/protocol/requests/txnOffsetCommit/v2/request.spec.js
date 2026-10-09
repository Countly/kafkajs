const Decoder = require('../../../decoder')
const RequestV2Protocol = require('./request')
const { protocol } = require('../index')

const vector = (caseId) => require(`../fixtures/java/txnOffsetCommitRequest-v2-${caseId}.json`)

/**
 * Reads a v2 request body back into the builder's arguments: the other direction of the byte check.
 * @param {Buffer} buffer
 * @returns {object}
 */
const decodeRequest = (buffer) => {
  const decoder = new Decoder(buffer)
  const fields = {
    transactionalId: decoder.readString(),
    groupId: decoder.readString(),
    producerId: decoder.readInt64().toString(),
    producerEpoch: decoder.readInt16(),
    topics: decoder.readArray((topics) => ({
      topic: topics.readString(),
      partitions: topics.readArray((partitions) => ({
        partition: partitions.readInt32(),
        offset: partitions.readInt64().toString(),
        leaderEpoch: partitions.readInt32(),
        metadata: partitions.readString(),
      })),
    })),
  }
  expect(decoder.canReadBytes(1)).toBe(false)
  return fields
}

/** The fields v2 carries: the group metadata in the vectors exists from v3 and is not sent. */
const v2Fields = ({ transactionalId, groupId, producerId, producerEpoch, topics }) => ({
  transactionalId,
  groupId,
  producerId,
  producerEpoch,
  topics,
})

describe('Protocol > Requests > TxnOffsetCommit > v2', () => {
  test.each(['legacy', 'leaderEpoch'])(
    'encodes the Apache 4.3.1 %s vector byte for byte',
    async (caseId) => {
      const { hex, fields } = vector(caseId)
      const { buffer } = await RequestV2Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test.each(['legacy', 'leaderEpoch'])(
    'the %s vector decodes back to the values it was built from',
    (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(decodeRequest(Buffer.from(hex, 'hex'))).toEqual(v2Fields(fields))
    }
  )

  test('an absent leader epoch and absent metadata encode as -1 and null, the fields defaults', async () => {
    const { hex, fields } = vector('legacy')
    const [{ topic, partitions }] = fields.topics
    const { partition, offset } = partitions[0]
    const { buffer } = await RequestV2Protocol({
      ...fields,
      topics: [{ topic, partitions: [{ partition, offset }] }],
    }).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('a leader epoch of 0 is committed as 0, not replaced by the default', async () => {
    const { fields } = vector('leaderEpoch')
    const [{ topic, partitions }] = fields.topics
    const zeroEpoch = {
      ...fields,
      topics: [{ topic, partitions: [{ ...partitions[0], leaderEpoch: 0 }] }],
    }
    const { buffer } = await RequestV2Protocol(zeroEpoch).encode()

    expect(decodeRequest(buffer).topics[0].partitions[0].leaderEpoch).toBe(0)
  })

  test("the offsets a consumer's uncommittedOffsets() hands out encode as the numeric ones do", async () => {
    const { hex, fields } = vector('leaderEpoch')
    const uncommitted = {
      ...fields,
      topics: [
        { topic: 'test-topic', partitions: [{ partition: '0', offset: '10', leaderEpoch: 3 }] },
      ],
    }
    const numeric = {
      ...fields,
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0, offset: 10, leaderEpoch: 3 }] }],
    }

    expect((await RequestV2Protocol(uncommitted).encode()).buffer.toString('hex')).toEqual(hex)
    expect((await RequestV2Protocol(numeric).encode()).buffer.toString('hex')).toEqual(hex)
  })

  test('announces version 2 and is reachable through the version map', async () => {
    const { hex, fields } = vector('leaderEpoch')
    const { request } = protocol({ version: 2 })(fields)

    expect(request).toMatchObject({ apiName: 'TxnOffsetCommit', apiVersion: 2 })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
