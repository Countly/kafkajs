const RequestV6Protocol = require('./request')
const RequestV5Protocol = require('../v5/request')

const vector = (version) =>
  require(`../fixtures/java/describeGroupsRequest-v${version}-oneGroup.json`)

describe('Protocol > Requests > DescribeGroups > v6', () => {
  test('encodes the oneGroup vector byte-identically to the Apache 4.3.1 codec', async () => {
    const { hex, fields } = vector(6)
    const { buffer } = await RequestV6Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('writes the same bytes as v5, because v6 changes only the response', async () => {
    // KIP-1043 added ErrorMessage and the GROUP_ID_NOT_FOUND answer to the RESPONSE; the request
    // is untouched, and the committed vectors say so.
    const v5 = await RequestV5Protocol(vector(5).fields).encode()
    const v6 = await RequestV6Protocol(vector(6).fields).encode()

    expect(v6.buffer).toEqual(v5.buffer)
    expect(vector(6).hex).toEqual(vector(5).hex)
  })

  test('announces itself as version 6', () => {
    const request = RequestV6Protocol(vector(6).fields)

    expect(request.apiVersion).toEqual(6)
    expect(request.apiName).toEqual('DescribeGroups')
  })

  test('is constructible with no options through the version map', async () => {
    const { request } = require('../index').protocol({ version: 6 })()
    const direct = await RequestV6Protocol().encode()

    expect((await request.encode()).buffer).toEqual(direct.buffer)
  })
})
