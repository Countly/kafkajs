const RequestV5Protocol = require('./request')
const Decoder = require('../../../decoder')

const vector = () => require('../fixtures/java/describeGroupsRequest-v5-oneGroup.json')

/**
 * Reads a v5 request body with a hand-written reader: a compact string array, a boolean, then the
 * request's own tag buffer.
 * @param {Buffer} buffer
 * @returns {{ groupIds: string[], includeAuthorizedOperations: boolean }} the fixture's own shape
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

describe('Protocol > Requests > DescribeGroups > v5', () => {
  test('encodes the oneGroup vector byte-identically to the Apache 4.3.1 codec', async () => {
    const { hex, fields } = vector()
    const { buffer } = await RequestV5Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('the oneGroup vector decodes back to the values it was built from', () => {
    const { hex, fields } = vector()

    expect(decodeRequest(Buffer.from(hex, 'hex'))).toEqual(fields)
  })

  test('is the flexible framing: a compact array and a tag buffer, not v3s classic bytes', async () => {
    const v5 = await RequestV5Protocol(vector().fields).encode()
    const v3 = await require('../v3/request')(vector().fields).encode()

    expect(v5.buffer.length).toBeLessThan(v3.buffer.length)
    expect(v5.buffer[0]).toEqual(0x02)
    expect(v5.buffer[v5.buffer.length - 1]).toEqual(0x00)
  })

  test('is constructible with no options, both directly and through the version map', async () => {
    const direct = await RequestV5Protocol().encode()
    const { request } = require('../index').protocol({ version: 5 })()

    expect(decodeRequest(direct.buffer)).toEqual({
      groupIds: [],
      includeAuthorizedOperations: false,
    })
    expect((await request.encode()).buffer).toEqual(direct.buffer)
  })

  test('announces itself as version 5', () => {
    const request = RequestV5Protocol(vector().fields)

    expect(request.apiVersion).toEqual(5)
    expect(request.apiName).toEqual('DescribeGroups')
  })
})
