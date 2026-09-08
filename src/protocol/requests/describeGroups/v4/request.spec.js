const RequestV4Protocol = require('./request')
const RequestV3Protocol = require('../v3/request')

const vector = (version) =>
  require(`../fixtures/java/describeGroupsRequest-v${version}-oneGroup.json`)

describe('Protocol > Requests > DescribeGroups > v4', () => {
  test('encodes the oneGroup vector byte-identically to the Apache 4.3.1 codec', async () => {
    const { hex, fields } = vector(4)
    const { buffer } = await RequestV4Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('writes the same bytes as v3, because v4 changes only the response', async () => {
    // KIP-345 added GroupInstanceId to the members of the RESPONSE; the request is untouched, and
    // the committed vectors say so.
    const v3 = await RequestV3Protocol(vector(3).fields).encode()
    const v4 = await RequestV4Protocol(vector(4).fields).encode()

    expect(v4.buffer).toEqual(v3.buffer)
    expect(vector(4).hex).toEqual(vector(3).hex)
  })

  test('announces itself as version 4', () => {
    const request = RequestV4Protocol(vector(4).fields)

    expect(request.apiVersion).toEqual(4)
    expect(request.apiName).toEqual('DescribeGroups')
  })

  test('is constructible with no options, both directly and through the version map', async () => {
    const { request } = require('../index').protocol({ version: 4 })()
    const direct = await RequestV4Protocol().encode()

    expect(direct.buffer.toString('hex')).toEqual('0000000000')
    expect((await request.encode()).buffer).toEqual(direct.buffer)
  })
})
