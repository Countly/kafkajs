const RequestV3Protocol = require('./request')
const { protocol, versions } = require('../index')

const vector = (caseId) => require(`../fixtures/java/deleteGroupsRequest-v3-${caseId}.json`)

describe('Protocol > Requests > DeleteGroups > v3', () => {
  test('encodes the Apache 4.4.0 basic vector byte for byte', async () => {
    const { hex, fields } = vector('basic')
    const { buffer } = await RequestV3Protocol(fields.groupIds).encode()

    expect(buffer.toString('hex')).toEqual(hex)
    expect(hex).toEqual('030b746573742d67726f75700c6f746865722d67726f757000')
  })

  test.each(['empty', 'nonAscii'])(
    'encodes the Apache 4.4.0 %s vector byte for byte',
    async (caseId) => {
      const { hex, fields } = vector(caseId)
      const { buffer } = await RequestV3Protocol(fields.groupIds).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test('is reachable through the version map as v3 of DeleteGroups', async () => {
    const { hex, fields } = vector('basic')
    const { request } = protocol({ version: 3 })(fields.groupIds)

    expect(versions).toEqual(['0', '1', '2', '3'])
    expect(request).toMatchObject({ apiKey: 42, apiVersion: 3, apiName: 'DeleteGroups' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
