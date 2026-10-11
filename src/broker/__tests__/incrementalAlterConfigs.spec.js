const { createGroupApiBroker } = require('./fixtures/groupApiHarness')
const apiKeys = require('../../protocol/requests/apiKeys')
const { requests } = require('../../protocol/requests')

const DIR = '../../protocol/requests/'
const incremental = (kind, version, caseId) =>
  require(
    `${DIR}incrementalAlterConfigs/fixtures/java/incrementalAlterConfigs${kind}-v${version}-${caseId}.json`
  )
const describe_ = (kind, version, caseId) =>
  require(`${DIR}describeConfigs/fixtures/java/describeConfigs${kind}-v${version}-${caseId}.json`)
const alter = (kind, caseId) =>
  require(`${DIR}alterConfigs/fixtures/java/alterConfigs${kind}-v2-${caseId}.json`)

const build = (apiKey, maxVersion, respondHex) =>
  createGroupApiBroker({
    versions: { [apiKey]: { minVersion: 0, maxVersion } },
    respond: () => Buffer.from(respondHex, 'hex'),
  })

describe('Broker > configuration APIs', () => {
  let harness

  afterEach(async () => {
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  describe('incrementalAlterConfigs', () => {
    test('sends the v1 codec bytes and resolves the decoded response', async () => {
      const { fields } = incremental('Request', 1, 'set')
      harness = build(apiKeys.IncrementalAlterConfigs, 1, incremental('Response', 1, 'ok').hex)

      const response = await harness.broker.incrementalAlterConfigs(fields)

      expect(harness.requests[0]).toMatchObject({ apiKey: 44, apiVersion: 1 })
      expect(harness.requests[0].body.toString('hex')).toEqual(incremental('Request', 1, 'set').hex)
      expect(response).toEqual({
        throttleTime: 0,
        clientSideThrottleTime: 25,
        resources: incremental('Response', 1, 'ok').fields.resources,
      })
    })

    test('falls back to the v0 codec against a broker that offers nothing newer', async () => {
      const { fields } = incremental('Request', 0, 'twoResources')
      harness = build(apiKeys.IncrementalAlterConfigs, 0, incremental('Response', 0, 'ok').hex)

      await harness.broker.incrementalAlterConfigs(fields)

      expect(harness.requests[0]).toMatchObject({ apiKey: 44, apiVersion: 0 })
      expect(harness.requests[0].body.toString('hex')).toEqual(
        incremental('Request', 0, 'twoResources').hex
      )
    })

    test.each([
      ['by default', {}, 'set'],
      ['when asked to', { validateOnly: true }, 'validateOnly'],
    ])('validates only %s', async (_, options, caseId) => {
      const { fields } = incremental('Request', 1, 'set')
      harness = build(apiKeys.IncrementalAlterConfigs, 1, incremental('Response', 1, 'ok').hex)

      await harness.broker.incrementalAlterConfigs({ resources: fields.resources, ...options })

      expect(harness.requests[0].body.toString('hex')).toEqual(
        incremental('Request', 1, caseId).hex
      )
    })

    test('resolves a response that carries a failed resource instead of throwing', async () => {
      const { fields } = incremental('Request', 1, 'twoResources')
      harness = build(apiKeys.IncrementalAlterConfigs, 1, incremental('Response', 1, 'error').hex)

      const response = await harness.broker.incrementalAlterConfigs(fields)

      expect(response.resources.map(({ errorCode }) => errorCode)).toEqual([0, 40])
    })
  })

  describe('describeConfigs', () => {
    const entries = describe_('Response', 4, 'entries')

    test('asks for documentation at v4 and decodes the new entry fields', async () => {
      const { fields } = describe_('Request', 4, 'includeDocumentation')
      harness = build(apiKeys.DescribeConfigs, 4, entries.hex)

      const response = await harness.broker.describeConfigs(fields)

      expect(harness.requests[0]).toMatchObject({ apiKey: 32, apiVersion: 4 })
      expect(harness.requests[0].body.toString('hex')).toEqual(
        describe_('Request', 4, 'includeDocumentation').hex
      )
      expect(response.resources[0].configEntries[0]).toMatchObject({
        configName: 'retention.ms',
        configType: 5,
        documentation: 'The retention time',
      })
    })

    test('sends no synonyms and no documentation by default', async () => {
      const { fields } = describe_('Request', 4, 'noSynonyms')
      harness = build(apiKeys.DescribeConfigs, 4, entries.hex)

      await harness.broker.describeConfigs({ resources: fields.resources })

      expect(harness.requests[0].body.toString('hex')).toEqual(
        describe_('Request', 4, 'noSynonyms').hex
      )
    })

    test('sends the same v2 bytes as before against a broker that stops at v2', async () => {
      const resources = [{ type: 2, name: 'test-topic', configNames: ['cleanup.policy'] }]
      harness = build(apiKeys.DescribeConfigs, 2, '0000000000000000')

      await harness.broker.describeConfigs({
        resources,
        includeSynonyms: true,
        includeDocumentation: true,
      })

      const expected = await requests.DescribeConfigs.protocol({ version: 2 })({
        resources,
        includeSynonyms: true,
      }).request.encode()

      expect(harness.requests[0].apiVersion).toEqual(2)
      expect(harness.requests[0].body).toEqual(expected.buffer)
    })
  })

  describe('alterConfigs', () => {
    test('is sent at v2 and resolves the decoded response', async () => {
      const { fields } = alter('Request', 'basic')
      harness = build(apiKeys.AlterConfigs, 2, alter('Response', 'ok').hex)

      const response = await harness.broker.alterConfigs(fields)

      expect(harness.requests[0]).toMatchObject({ apiKey: 33, apiVersion: 2 })
      expect(harness.requests[0].body.toString('hex')).toEqual(alter('Request', 'basic').hex)
      expect(response.resources).toEqual(alter('Response', 'ok').fields.resources)
    })
  })
})
