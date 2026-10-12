const RequestV3Protocol = require('./request')
const { protocol, versions } = require('../index')

const vector = (caseId) => require(`../fixtures/java/deleteAclsRequest-v3-${caseId}.json`)

describe('Protocol > Requests > DeleteAcls > v3', () => {
  test.each(['basic', 'anyFilter', 'userResource'])(
    'encodes the Apache 4.3.1 %s vector byte for byte',
    async (caseId) => {
      const { hex, fields } = vector(caseId)
      const { buffer } = await RequestV3Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test('is reachable through the version map as v3 of DeleteAcls', async () => {
    const { hex, fields } = vector('userResource')
    const { request } = protocol({ version: 3 })(fields)

    expect(versions).toEqual(['1', '2', '3'])
    expect(request).toMatchObject({ apiKey: 31, apiVersion: 3, apiName: 'DeleteAcls' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
