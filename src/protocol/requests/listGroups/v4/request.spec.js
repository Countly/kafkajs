const RequestV4Protocol = require('./request')
const Decoder = require('../../../decoder')

const vector = (caseId) => require(`../fixtures/java/listGroupsRequest-v4-${caseId}.json`)

/**
 * Reads a v4 request body with a hand-written reader, so the assertion is about the bytes and not
 * about this repo's own encoder agreeing with itself.
 * @param {Buffer} buffer
 * @returns {{ states: string[] }} the decoded fields, in the fixture's own shape
 */
const decodeRequest = (buffer) => {
  const decoder = new Decoder(buffer)
  const fields = { states: decoder.readUVarIntArray((d) => d.readUVarIntString()) }

  decoder.readTaggedFields()
  expect(decoder.canReadBytes(1)).toBe(false)
  return fields
}

describe('Protocol > Requests > ListGroups > v4', () => {
  test.each(['noFilters', 'statesFilter'])(
    'encodes the %s vector byte-identically to the Apache 4.3.1 codec',
    async (caseId) => {
      const { hex, fields } = vector(caseId)
      const { buffer } = await RequestV4Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test.each(['noFilters', 'statesFilter'])(
    'the %s vector decodes back to the values it was built from',
    (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(decodeRequest(Buffer.from(hex, 'hex'))).toEqual(fields)
    }
  )

  test('encodes an empty filter as the empty compact array, never the null one', async () => {
    // "No filter" and "filter on nothing" would otherwise be the same bytes, which would make an
    // unfiltered list indistinguishable from a list of nothing. The field is not nullable.
    const { buffer } = await RequestV4Protocol({}).encode()

    expect(buffer[0]).toEqual(0x01)
    expect(decodeRequest(buffer)).toEqual({ states: [] })
  })

  test('is constructible with no options, both directly and through the version map', async () => {
    // The version map hands v0-v2 no options at all, so every version has to build from nothing.
    const direct = await RequestV4Protocol().encode()
    const { request } = require('../index').protocol({ version: 4 })()

    expect(direct.buffer.toString('hex')).toEqual(vector('noFilters').hex)
    expect((await request.encode()).buffer.toString('hex')).toEqual(vector('noFilters').hex)
  })

  test('announces itself as version 4', () => {
    const request = RequestV4Protocol({ states: ['Stable'] })

    expect(request.apiVersion).toEqual(4)
    expect(request.apiName).toEqual('ListGroups')
  })
})
