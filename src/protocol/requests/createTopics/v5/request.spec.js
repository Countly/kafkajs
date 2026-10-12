const RequestV5Protocol = require('./request')
const { protocol } = require('../index')

const CASES = ['defaults', 'explicit', 'assigned']
const vector = (caseId) => require(`../fixtures/java/createTopicsRequest-v5-${caseId}.json`)

describe('Protocol > Requests > CreateTopics > v5', () => {
  test.each(CASES)('encodes the Apache 4.3.1 %s vector byte for byte', async (caseId) => {
    const { hex, fields } = vector(caseId)
    const { buffer } = await RequestV5Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('is reachable through the version map as v5 of CreateTopics', async () => {
    const { hex, fields } = vector('explicit')
    const { request } = protocol({ version: 5 })(fields)

    expect(request).toMatchObject({ apiKey: 19, apiVersion: 5, apiName: 'CreateTopics' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })

  test('sends -1 partitions, -1 replication factor and empty lists for a bare topic name', async () => {
    const { hex, fields } = vector('defaults')
    const { buffer } = await RequestV5Protocol({
      topics: [{ topic: fields.topics[0].topic }],
      timeout: fields.timeout,
    }).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('writes a null config value as the compact null', async () => {
    const { buffer } = await RequestV5Protocol({
      topics: [{ topic: 't', configEntries: [{ name: 'k', value: null }] }],
    }).encode()

    expect(buffer.toString('hex')).toEqual(
      '02' +
        '0274' +
        'ffffffff' +
        'ffff' +
        '01' +
        '02' +
        '026b' +
        '00' +
        '00' +
        '00' +
        '00001388' +
        '00' +
        '00'
    )
  })
})
