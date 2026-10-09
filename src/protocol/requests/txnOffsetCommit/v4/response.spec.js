const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/txnOffsetCommitResponse-v4-${caseId}.json`)

describe('Protocol > Requests > TxnOffsetCommit > v4 response', () => {
  test.each(['ok', 'error'])(
    'decodes the Apache 4.3.1 %s vector into the v1 shape',
    async (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
        throttleTime: 0,
        clientSideThrottleTime: fields.throttleTime,
        topics: fields.topics,
      })
    }
  )

  test('the ok values encode to the vector bytes', () => {
    const { hex, fields } = vector('ok')
    const [{ topic, partitions }] = fields.topics
    const [{ partition, errorCode }] = partitions
    const body = new Encoder()
      .writeInt32(fields.throttleTime)
      .writeUVarIntArray([
        new Encoder()
          .writeUVarIntString(topic)
          .writeUVarIntArray([
            new Encoder().writeInt32(partition).writeInt16(errorCode).writeTaggedFields(),
          ])
          .writeTaggedFields(),
      ])
      .writeTaggedFields().buffer

    expect(body.toString('hex')).toEqual(hex)
  })

  test("parse throws the failed partition's error, TRANSACTION_ABORTABLE included", async () => {
    const failed = await decode(Buffer.from(vector('error').hex, 'hex'))
    const abortable = {
      ...failed,
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0, errorCode: 120 }] }],
    }

    await expect(parse(failed)).rejects.toMatchObject({ type: 'ILLEGAL_GENERATION', code: 22 })
    await expect(parse(abortable)).rejects.toMatchObject({
      type: 'TRANSACTION_ABORTABLE',
      code: 120,
    })
  })
})
