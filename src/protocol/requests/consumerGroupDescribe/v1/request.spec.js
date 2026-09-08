const RequestV1Protocol = require('./request')
const Decoder = require('../../../decoder')

const vector = (caseId) =>
  require(`../fixtures/java/consumerGroupDescribeRequest-v1-${caseId}.json`)

/**
 * Decodes a v1 request body with a hand-written reader, so the assertion is about the bytes and
 * not about this repo's own encoder agreeing with itself.
 * @param {Buffer} buffer
 * @returns {object} the decoded fields, in the fixture's own shape
 */
const decodeRequest = (buffer) => {
  const decoder = new Decoder(buffer)
  const fields = {
    groupIds: decoder.readUVarIntArray((d) => d.readUVarIntString()),
    includeAuthorizedOperations: decoder.readBoolean(),
  }

  decoder.readTaggedFields()
  expect(decoder.canReadBytes(1)).toBe(false)
  return fields
}

describe('Protocol > Requests > ConsumerGroupDescribe > v1', () => {
  test.each(['authorizedOperations'])(
    'encodes the %s vector byte-identically to the Apache 4.3.1 codec',
    async (caseId) => {
      const { hex, fields } = vector(caseId)
      const { buffer } = await RequestV1Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test.each(['authorizedOperations'])(
    'the %s vector decodes back to the values it was built from',
    (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(decodeRequest(Buffer.from(hex, 'hex'))).toEqual(fields)
    }
  )

  test('announces itself as version 1', async () => {
    const request = RequestV1Protocol({ groupIds: ['test-group'] })

    expect(request.apiVersion).toEqual(1)
    expect(request.apiName).toEqual('ConsumerGroupDescribe')
  })

  test('is byte-identical to v0 for the same values', async () => {
    // v1 changes only the RESPONSE (it adds MemberType). A v1 request that differs from a v0 one
    // would mean a field was invented here.
    const fields = { groupIds: ['test-group'], includeAuthorizedOperations: true }

    expect((await RequestV1Protocol(fields).encode()).buffer).toEqual(
      (await require('../v0/request')(fields).encode()).buffer
    )
  })

  test('defaults the authorized-operations flag to false and the ids to none', async () => {
    const { buffer } = await RequestV1Protocol({}).encode()

    expect(decodeRequest(buffer)).toEqual({ groupIds: [], includeAuthorizedOperations: false })
  })
})
