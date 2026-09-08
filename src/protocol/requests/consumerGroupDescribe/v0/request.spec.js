const RequestV0Protocol = require('./request')
const Decoder = require('../../../decoder')

const vector = (caseId) =>
  require(`../fixtures/java/consumerGroupDescribeRequest-v0-${caseId}.json`)

/**
 * Decodes a v0 request body with a hand-written reader, so the assertion is about the bytes and
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

describe('Protocol > Requests > ConsumerGroupDescribe > v0', () => {
  test.each(['twoGroups'])(
    'encodes the %s vector byte-identically to the Apache 4.3.1 codec',
    async (caseId) => {
      const { hex, fields } = vector(caseId)
      const { buffer } = await RequestV0Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test.each(['twoGroups'])(
    'the %s vector decodes back to the values it was built from',
    (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(decodeRequest(Buffer.from(hex, 'hex'))).toEqual(fields)
    }
  )

  test('defaults the authorized-operations flag to false and the ids to none', async () => {
    const { buffer } = await RequestV0Protocol({}).encode()

    expect(decodeRequest(buffer)).toEqual({ groupIds: [], includeAuthorizedOperations: false })
  })

  test('sends the flag as a single true byte when asked for', async () => {
    const { buffer } = await RequestV0Protocol({
      groupIds: ['test-group'],
      includeAuthorizedOperations: true,
    }).encode()

    expect(decodeRequest(buffer)).toEqual({
      groupIds: ['test-group'],
      includeAuthorizedOperations: true,
    })
  })
})
