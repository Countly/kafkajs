const RequestV3Protocol = require('./request')
const Decoder = require('../../../decoder')

const vector = (caseId) => require(`../fixtures/java/listGroupsRequest-v3-${caseId}.json`)

describe('Protocol > Requests > ListGroups > v3', () => {
  // v3's request body is a tag buffer and nothing else - the first flexible version of an API
  // that has never had a request field.
  test('encodes an empty body at v3', async () => {
    const { buffer } = await RequestV3Protocol().encode()

    expect(buffer.toString('hex')).toEqual(vector('noFilters').hex)
    expect(buffer.length).toEqual(1)
  })

  test('the noFilters vector decodes back to the values it was built from', () => {
    const decoder = new Decoder(Buffer.from(vector('noFilters').hex, 'hex'))

    decoder.readTaggedFields()

    expect(decoder.canReadBytes(1)).toBe(false)
    expect(vector('noFilters').fields).toEqual({})
  })

  test('announces itself as version 3 and ignores the filters the version map hands it', async () => {
    const request = RequestV3Protocol({ states: ['Stable'], types: ['Consumer'] })

    expect(request.apiVersion).toEqual(3)
    expect(request.apiName).toEqual('ListGroups')
    expect((await request.encode()).buffer.toString('hex')).toEqual(vector('noFilters').hex)
  })
})
