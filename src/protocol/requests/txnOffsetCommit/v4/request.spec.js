const RequestV4Protocol = require('./request')
const { protocol } = require('../index')

const vector = (version, caseId) =>
  require(`../fixtures/java/txnOffsetCommitRequest-v${version}-${caseId}.json`)

describe('Protocol > Requests > TxnOffsetCommit > v4', () => {
  test.each(['legacy', 'groupMetadata'])(
    'encodes the Apache 4.3.1 %s vector byte for byte',
    async (caseId) => {
      const { hex, fields } = vector(4, caseId)
      const { buffer } = await RequestV4Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test('is the v3 body: v4 only lets the broker answer TRANSACTION_ABORTABLE', async () => {
    const { hex, fields } = vector(4, 'groupMetadata')
    const v3 = await require('../v3/request')(fields).encode()

    expect(v3.buffer.toString('hex')).toEqual(hex)
    expect(vector(3, 'groupMetadata').hex).toEqual(hex)
  })

  test("the offsets a consumer's uncommittedOffsets() hands out encode as the numeric ones do", async () => {
    const { hex, fields } = vector(4, 'legacy')
    const { generationId, memberId, groupInstanceId, ...withoutMetadata } = fields
    const uncommitted = {
      ...withoutMetadata,
      topics: [
        { topic: 'test-topic', partitions: [{ partition: '0', offset: '10', leaderEpoch: -1 }] },
      ],
    }

    expect((await RequestV4Protocol(uncommitted).encode()).buffer.toString('hex')).toEqual(hex)
  })

  test('announces version 4 and is reachable through the version map', async () => {
    const { hex, fields } = vector(4, 'groupMetadata')
    const { request } = protocol({ version: 4 })(fields)

    expect(request).toMatchObject({ apiName: 'TxnOffsetCommit', apiVersion: 4 })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
