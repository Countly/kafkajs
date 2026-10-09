const RequestV5Protocol = require('./request')
const { protocol } = require('../index')

const vector = (version, caseId) =>
  require(`../fixtures/java/txnOffsetCommitRequest-v${version}-${caseId}.json`)

describe('Protocol > Requests > TxnOffsetCommit > v5', () => {
  test.each(['legacy', 'groupMetadata'])(
    'encodes the Apache 4.3.1 %s vector byte for byte',
    async (caseId) => {
      const { hex, fields } = vector(5, caseId)
      const { buffer } = await RequestV5Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test('is the v3 body: under transaction version 2 the broker adds the offsets itself', async () => {
    const { hex, fields } = vector(5, 'groupMetadata')
    const v3 = await require('../v3/request')(fields).encode()

    expect(v3.buffer.toString('hex')).toEqual(hex)
    expect(vector(3, 'groupMetadata').hex).toEqual(hex)
  })

  test("the offsets a consumer's uncommittedOffsets() hands out encode as the numeric ones do", async () => {
    const { hex, fields } = vector(5, 'legacy')
    const { generationId, memberId, groupInstanceId, ...withoutMetadata } = fields
    const uncommitted = {
      ...withoutMetadata,
      topics: [
        { topic: 'test-topic', partitions: [{ partition: '0', offset: '10', leaderEpoch: -1 }] },
      ],
    }

    expect((await RequestV5Protocol(uncommitted).encode()).buffer.toString('hex')).toEqual(hex)
  })

  test('announces version 5 and is reachable through the version map', async () => {
    const { hex, fields } = vector(5, 'groupMetadata')
    const { request } = protocol({ version: 5 })(fields)

    expect(request).toMatchObject({ apiName: 'TxnOffsetCommit', apiVersion: 5 })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
