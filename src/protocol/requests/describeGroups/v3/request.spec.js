const RequestV3Protocol = require('./request')
const Decoder = require('../../../decoder')

const vector = () => require('../fixtures/java/describeGroupsRequest-v3-oneGroup.json')

describe('Protocol > Requests > DescribeGroups > v3', () => {
  test('encodes the oneGroup vector byte-identically to the Apache 4.3.1 codec', async () => {
    const { hex, fields } = vector()
    const { buffer } = await RequestV3Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('sends IncludeAuthorizedOperations from v3', async () => {
    // The field arrives at v3 (KIP-430). v0-v2 have no such field, so the assertion is that it is
    // on the wire here and readable back as a boolean.
    const { hex, fields } = vector()
    const { buffer } = await RequestV3Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)

    const decoder = new Decoder(buffer)
    expect(decoder.readArray((d) => d.readString())).toEqual(['test-group'])
    expect(decoder.readBoolean()).toBe(true)
    expect(decoder.canReadBytes(1)).toBe(false)
  })

  test('defaults the flag to false, which is one byte on the wire, not an absent field', async () => {
    const { buffer } = await RequestV3Protocol({ groupIds: ['test-group'] }).encode()
    const decoder = new Decoder(buffer)

    decoder.readArray((d) => d.readString())
    expect(decoder.readBoolean()).toBe(false)
    expect(decoder.canReadBytes(1)).toBe(false)
  })

  test('is constructible with no options, both directly and through the version map', async () => {
    const direct = await RequestV3Protocol().encode()
    const { request } = require('../index').protocol({ version: 3 })()

    expect(direct.buffer.toString('hex')).toEqual('0000000000')
    expect((await request.encode()).buffer.toString('hex')).toEqual('0000000000')
  })

  test('announces itself as version 3', () => {
    const request = RequestV3Protocol(vector().fields)

    expect(request.apiVersion).toEqual(3)
    expect(request.apiName).toEqual('DescribeGroups')
  })
})
