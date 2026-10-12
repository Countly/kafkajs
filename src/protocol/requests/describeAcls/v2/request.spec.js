const RequestV2Protocol = require('./request')
const { protocol } = require('../index')

const vector = (caseId) => require(`../fixtures/java/describeAclsRequest-v2-${caseId}.json`)

describe('Protocol > Requests > DescribeAcls > v2', () => {
  test.each(['basic', 'anyFilter'])(
    'encodes the Apache 4.3.1 %s vector byte for byte',
    async (caseId) => {
      const { hex, fields } = vector(caseId)
      const { buffer } = await RequestV2Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test('writes a resource name, principal and host that are left out as the compact null', async () => {
    const { hex, fields } = vector('anyFilter')
    const { resourceType, resourcePatternType, operation, permissionType } = fields
    const { buffer } = await RequestV2Protocol({
      resourceType,
      resourcePatternType,
      operation,
      permissionType,
    }).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('is reachable through the version map as v2 of DescribeAcls', async () => {
    const { hex, fields } = vector('basic')
    const { request } = protocol({ version: 2 })(fields)

    expect(request).toMatchObject({ apiKey: 29, apiVersion: 2, apiName: 'DescribeAcls' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
