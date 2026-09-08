const RequestV5Protocol = require('./request')
const Decoder = require('../../../decoder')

const vector = (caseId) => require(`../fixtures/java/listGroupsRequest-v5-${caseId}.json`)

/**
 * Reads a v5 request body with a hand-written reader: two compact string arrays, then the
 * request's own tag buffer.
 * @param {Buffer} buffer
 * @returns {{ states: string[], types: string[] }} the decoded fields, in the fixture's own shape
 */
const decodeRequest = (buffer) => {
  const decoder = new Decoder(buffer)
  const fields = {
    states: decoder.readUVarIntArray((d) => d.readUVarIntString()),
    types: decoder.readUVarIntArray((d) => d.readUVarIntString()),
  }

  decoder.readTaggedFields()
  expect(decoder.canReadBytes(1)).toBe(false)
  return fields
}

describe('Protocol > Requests > ListGroups > v5', () => {
  test.each(['noFilters', 'statesAndTypes'])(
    'encodes the %s vector byte-identically to the Apache 4.3.1 codec',
    async (caseId) => {
      const { hex, fields } = vector(caseId)
      const { buffer } = await RequestV5Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test.each(['noFilters', 'statesAndTypes'])(
    'the %s vector decodes back to the values it was built from',
    (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(decodeRequest(Buffer.from(hex, 'hex'))).toEqual(fields)
    }
  )

  test('encodes empty filters as empty arrays at v5', async () => {
    // An empty filter is an empty COMPACT_ARRAY (0x01), never the null one (0x00): the fields are
    // not nullable, and "no filter" and "filter on nothing" would otherwise be the same bytes -
    // which would make an unfiltered list indistinguishable from a list of nothing.
    const { buffer } = await RequestV5Protocol({ states: [], types: [] }).encode()

    expect(buffer.toString('hex')).toEqual(vector('noFilters').hex)
    expect(buffer[0]).toEqual(0x01)
    expect(buffer[1]).toEqual(0x01)
    expect(decodeRequest(buffer)).toEqual({ states: [], types: [] })
  })

  test('defaults both filters to none', async () => {
    const { buffer } = await RequestV5Protocol({}).encode()

    expect(decodeRequest(buffer)).toEqual({ states: [], types: [] })
  })

  test('is constructible with no options, both directly and through the version map', async () => {
    // The version map hands v0-v2 no options at all, so every version has to build from nothing.
    const direct = await RequestV5Protocol().encode()
    const { request } = require('../index').protocol({ version: 5 })()

    expect(direct.buffer.toString('hex')).toEqual(vector('noFilters').hex)
    expect((await request.encode()).buffer.toString('hex')).toEqual(vector('noFilters').hex)
  })

  test('announces itself as version 5', () => {
    const request = RequestV5Protocol({ states: ['Stable'], types: ['Consumer'] })

    expect(request.apiVersion).toEqual(5)
    expect(request.apiName).toEqual('ListGroups')
  })
})
