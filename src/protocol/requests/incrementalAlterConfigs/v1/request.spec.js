const AlterConfigOpTypes = require('../../../alterConfigOpTypes')
const RequestV1Protocol = require('./request')
const { protocol, versions } = require('../index')
const { requests, selectVersion } = require('../../index')

const CASES = ['set', 'append', 'subtract', 'delete', 'twoResources', 'validateOnly']
const vector = (caseId) =>
  require(`../fixtures/java/incrementalAlterConfigsRequest-v1-${caseId}.json`)

/** One resource carrying one config entry. */
const withEntry = (entry) => ({
  resources: [{ type: 2, name: 'test-topic', configEntries: [entry] }],
})

describe('Protocol > Requests > IncrementalAlterConfigs > v1', () => {
  test.each(CASES)('encodes the Apache 4.3.1 %s vector byte for byte', async (caseId) => {
    const { hex, fields } = vector(caseId)
    const { buffer } = await RequestV1Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('writes a DELETE value as the compact null, whatever value the entry carries', async () => {
    const { hex, fields } = vector('delete')
    const [resource] = fields.resources
    const withValue = {
      ...fields,
      resources: [{ ...resource, configEntries: [{ ...resource.configEntries[0], value: 'x' }] }],
    }
    const { buffer } = await RequestV1Protocol(withValue).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test.each([[undefined], [null], ['1'], [1.5], [-1], [4]])(
    'refuses an entry whose operation is %j, before writing anything',
    async (operation) => {
      const entry = { name: 'retention.ms', operation, value: '1' }

      await expect(RequestV1Protocol(withEntry(entry)).encode()).rejects.toMatchObject({
        name: 'KafkaJSNonRetriableError',
        message: expect.stringContaining('Invalid operation'),
      })
    }
  )

  test.each(['SET', 'APPEND', 'SUBTRACT'])(
    'refuses a %s with a null or missing value, which the controller would apply as a removal',
    async (type) => {
      const operation = AlterConfigOpTypes[type]

      for (const entry of [
        { name: 'retention.ms', operation, value: null },
        { name: 'retention.ms', operation },
      ]) {
        await expect(RequestV1Protocol(withEntry(entry)).encode()).rejects.toMatchObject({
          name: 'KafkaJSNonRetriableError',
          message: expect.stringContaining('needs a value'),
        })
      }
    }
  )

  test('accepts a DELETE with no value at all', async () => {
    const { hex } = vector('delete')
    const { buffer } = await RequestV1Protocol(
      withEntry({ name: 'retention.ms', operation: AlterConfigOpTypes.DELETE })
    ).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('sends validateOnly as false when it is omitted', async () => {
    const { hex, fields } = vector('set')
    const { buffer } = await RequestV1Protocol({ resources: fields.resources }).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('is registered for key 44 and selected at v1 from a broker offering v0 to v1', async () => {
    const { hex, fields } = vector('validateOnly')
    const definition = requests.IncrementalAlterConfigs
    const version = selectVersion({ 44: { minVersion: 0, maxVersion: 1 } }, 44, definition)
    const { request } = protocol({ version })(fields)

    expect(versions).toEqual(['0', '1'])
    expect(definition.versions).toEqual(['0', '1'])
    expect(request).toMatchObject({ apiKey: 44, apiVersion: 1, apiName: 'IncrementalAlterConfigs' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
