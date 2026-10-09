const Decoder = require('../../../decoder')
const RequestV3Protocol = require('./request')
const { protocol } = require('../index')

const vector = (caseId) => require(`../fixtures/java/txnOffsetCommitRequest-v3-${caseId}.json`)

/**
 * Reads a v3 request body back into the builder's arguments: compact strings and arrays, a tag
 * buffer closing every structure.
 * @param {Buffer} buffer
 * @returns {object}
 */
const decodeRequest = (buffer) => {
  const decoder = new Decoder(buffer)
  const closed = (reader) => (structure) => {
    const value = reader(structure)
    expect(structure.readTaggedFields().size).toEqual(0)
    return value
  }
  const fields = {
    transactionalId: decoder.readUVarIntString(),
    groupId: decoder.readUVarIntString(),
    producerId: decoder.readInt64().toString(),
    producerEpoch: decoder.readInt16(),
    generationId: decoder.readInt32(),
    memberId: decoder.readUVarIntString(),
    groupInstanceId: decoder.readUVarIntString(),
    topics: decoder.readUVarIntArray(
      closed((topics) => ({
        topic: topics.readUVarIntString(),
        partitions: topics.readUVarIntArray(
          closed((partitions) => ({
            partition: partitions.readInt32(),
            offset: partitions.readInt64().toString(),
            leaderEpoch: partitions.readInt32(),
            metadata: partitions.readUVarIntString(),
          }))
        ),
      }))
    ),
  }
  expect(decoder.readTaggedFields().size).toEqual(0)
  expect(decoder.canReadBytes(1)).toBe(false)
  return fields
}

describe('Protocol > Requests > TxnOffsetCommit > v3', () => {
  test.each(['legacy', 'groupMetadata'])(
    'encodes the Apache 4.3.1 %s vector byte for byte',
    async (caseId) => {
      const { hex, fields } = vector(caseId)
      const { buffer } = await RequestV3Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test.each(['legacy', 'groupMetadata'])(
    'the %s vector decodes back to the values it was built from',
    (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(decodeRequest(Buffer.from(hex, 'hex'))).toEqual(fields)
    }
  )

  test('without group metadata it sends -1, "" and null: a commit the coordinator does not fence', async () => {
    const { hex, fields } = vector('legacy')
    const { generationId, memberId, groupInstanceId, ...withoutMetadata } = fields

    expect([generationId, memberId, groupInstanceId]).toEqual([-1, '', null])
    expect((await RequestV3Protocol(withoutMetadata).encode()).buffer.toString('hex')).toEqual(hex)
  })

  test("the offsets a consumer's uncommittedOffsets() hands out encode as the numeric ones do", async () => {
    const { hex, fields } = vector('legacy')
    const uncommitted = {
      ...fields,
      topics: [
        { topic: 'test-topic', partitions: [{ partition: '0', offset: '10', leaderEpoch: -1 }] },
      ],
    }
    const withEpoch = (partition, offset) => ({
      ...fields,
      topics: [{ topic: 'test-topic', partitions: [{ partition, offset, leaderEpoch: 3 }] }],
    })

    expect((await RequestV3Protocol(uncommitted).encode()).buffer.toString('hex')).toEqual(hex)
    expect((await RequestV3Protocol(withEpoch('0', '10')).encode()).buffer).toEqual(
      (await RequestV3Protocol(withEpoch(0, 10)).encode()).buffer
    )
  })

  test('a leader epoch of 0 is committed as 0, not replaced by the default', async () => {
    const { fields } = vector('groupMetadata')
    const [{ topic, partitions }] = fields.topics
    const zeroEpoch = {
      ...fields,
      topics: [{ topic, partitions: [{ ...partitions[0], leaderEpoch: 0 }] }],
    }
    const { buffer } = await RequestV3Protocol(zeroEpoch).encode()

    expect(decodeRequest(buffer).topics[0].partitions[0].leaderEpoch).toBe(0)
  })

  test('a null leader epoch is the -1 default, never 0', async () => {
    const { hex, fields } = vector('legacy')
    const nullEpoch = {
      ...fields,
      topics: [
        { topic: 'test-topic', partitions: [{ partition: 0, offset: '10', leaderEpoch: null }] },
      ],
    }

    expect((await RequestV3Protocol(nullEpoch).encode()).buffer.toString('hex')).toEqual(hex)
  })

  test('announces version 3 and is reachable through the version map', async () => {
    const { hex, fields } = vector('groupMetadata')
    const { request } = protocol({ version: 3 })(fields)

    expect(request).toMatchObject({ apiName: 'TxnOffsetCommit', apiVersion: 3 })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
