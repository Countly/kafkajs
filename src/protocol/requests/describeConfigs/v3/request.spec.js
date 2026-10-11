const RequestV3Protocol = require('./request')
const { protocol } = require('../index')

const CASES = ['keys', 'allKeys', 'includeDocumentation', 'noSynonyms']
const vector = (caseId) => require(`../fixtures/java/describeConfigsRequest-v3-${caseId}.json`)

describe('Protocol > Requests > DescribeConfigs > v3', () => {
  test.each(CASES)('encodes the Apache 4.3.1 %s vector byte for byte', async (caseId) => {
    const { hex, fields } = vector(caseId)
    const { buffer } = await RequestV3Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test.each([[[]], [undefined], [null]])(
    'writes configNames %j as the null list, the allKeys vector',
    async (configNames) => {
      const { hex, fields } = vector('allKeys')
      const [resource] = fields.resources
      const { buffer } = await RequestV3Protocol({
        ...fields,
        resources: [{ ...resource, configNames }],
      }).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test('sends no synonyms and no documentation unless asked for', async () => {
    const { hex, fields } = vector('noSynonyms')
    const { buffer } = await RequestV3Protocol({ resources: fields.resources }).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('is reachable through the version map as v3 of DescribeConfigs', async () => {
    const { hex, fields } = vector('includeDocumentation')
    const { request } = protocol({ version: 3 })(fields)

    expect(request).toMatchObject({ apiKey: 32, apiVersion: 3, apiName: 'DescribeConfigs' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
