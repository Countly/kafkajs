const RequestV2Protocol = require('./request')
const { protocol, versions } = require('../index')

const vector = () => require('../fixtures/java/deleteGroupsRequest-v2-basic.json')

describe('Protocol > Requests > DeleteGroups > v2', () => {
  test('encodes the Apache 4.3.1 basic vector byte for byte', async () => {
    const { hex, fields } = vector()
    const { buffer } = await RequestV2Protocol(fields.groupIds).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('is reachable through the version map as v2 of DeleteGroups', async () => {
    const { hex, fields } = vector()
    const { request } = protocol({ version: 2 })(fields.groupIds)

    expect(versions).toEqual(['0', '1', '2'])
    expect(request).toMatchObject({ apiKey: 42, apiVersion: 2, apiName: 'DeleteGroups' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
