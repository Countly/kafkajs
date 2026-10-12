const RequestV2Protocol = require('./request')
const { protocol } = require('../index')

const vector = () => require('../fixtures/java/createAclsRequest-v2-basic.json')

describe('Protocol > Requests > CreateAcls > v2', () => {
  test('encodes the Apache 4.3.1 basic vector byte for byte', async () => {
    const { hex, fields } = vector()
    const { buffer } = await RequestV2Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('is reachable through the version map as v2 of CreateAcls', async () => {
    const { hex, fields } = vector()
    const { request } = protocol({ version: 2 })(fields)

    expect(request).toMatchObject({ apiKey: 30, apiVersion: 2, apiName: 'CreateAcls' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
