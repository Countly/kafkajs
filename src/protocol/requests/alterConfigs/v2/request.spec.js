const RequestV2Protocol = require('./request')
const { protocol, versions } = require('../index')

const vector = (caseId) => require(`../fixtures/java/alterConfigsRequest-v2-${caseId}.json`)

describe('Protocol > Requests > AlterConfigs > v2', () => {
  test.each(['basic', 'validateOnly'])(
    'encodes the Apache 4.3.1 %s vector byte for byte',
    async (caseId) => {
      const { hex, fields } = vector(caseId)
      const { buffer } = await RequestV2Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test('writes a null value as the compact null', async () => {
    const { buffer } = await RequestV2Protocol({
      resources: [
        { type: 2, name: 'test-topic', configEntries: [{ name: 'retention.ms', value: null }] },
      ],
    }).encode()
    const text = (value) => Buffer.from(value).toString('hex')

    expect(buffer.toString('hex')).toEqual(
      // resources, type, name, configs, then name, null value, the tag buffers of config,
      // resource and request, and validateOnly
      `0202${'0b' + text('test-topic')}02${'0d' + text('retention.ms')}00` + '00000000'
    )
  })

  test('sends validateOnly as false when it is omitted', async () => {
    const { hex, fields } = vector('basic')
    const { buffer } = await RequestV2Protocol({ resources: fields.resources }).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('is reachable through the version map as v2 of AlterConfigs', async () => {
    const { hex, fields } = vector('validateOnly')
    const { request } = protocol({ version: 2 })(fields)

    expect(versions).toEqual(['0', '1', '2'])
    expect(request).toMatchObject({ apiKey: 33, apiVersion: 2, apiName: 'AlterConfigs' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
