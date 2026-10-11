const RequestV4Protocol = require('./request')
const { protocol, versions } = require('../index')

const CASES = ['keys', 'allKeys', 'includeDocumentation', 'noSynonyms']
const vector = (caseId) => require(`../fixtures/java/describeConfigsRequest-v4-${caseId}.json`)

describe('Protocol > Requests > DescribeConfigs > v4', () => {
  test.each(CASES)('encodes the Apache 4.3.1 %s vector byte for byte', async (caseId) => {
    const { hex, fields } = vector(caseId)
    const { buffer } = await RequestV4Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('writes each key name as a compact string, not as an empty list', async () => {
    const { buffer } = await RequestV4Protocol(vector('keys').fields).encode()

    expect(buffer.toString('hex')).toContain(Buffer.from('cleanup.policy').toString('hex'))
    expect(buffer.toString('hex')).toContain(Buffer.from('retention.ms').toString('hex'))
  })

  test.each([[[]], [undefined], [null]])(
    'writes configNames %j as the compact null, the allKeys vector',
    async (configNames) => {
      const { hex, fields } = vector('allKeys')
      const [resource] = fields.resources
      const { buffer } = await RequestV4Protocol({
        ...fields,
        resources: [{ ...resource, configNames }],
      }).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test('sends no synonyms and no documentation unless asked for', async () => {
    const { hex, fields } = vector('noSynonyms')
    const { buffer } = await RequestV4Protocol({ resources: fields.resources }).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('is reachable through the version map, and v2 ignores the flag', async () => {
    const { hex, fields } = vector('includeDocumentation')
    const { request } = protocol({ version: 4 })(fields)
    const resources = fields.resources.map((resource) => ({ ...resource, configNames: [] }))
    const asked = { resources, includeSynonyms: true, includeDocumentation: true }
    const v2 = await protocol({ version: 2 })(asked).request.encode()
    const v2Plain = await protocol({ version: 2 })({
      resources,
      includeSynonyms: true,
    }).request.encode()

    expect(versions).toEqual(['1', '2', '3', '4'])
    expect(request).toMatchObject({ apiKey: 32, apiVersion: 4, apiName: 'DescribeConfigs' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
    expect(v2.buffer).toEqual(v2Plain.buffer)
  })
})
