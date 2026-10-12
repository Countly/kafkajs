const RequestV7Protocol = require('./request')
const { protocol } = require('../index')

const CASES = ['defaults', 'explicit', 'assigned']
const vector = (caseId) => require(`../fixtures/java/createTopicsRequest-v7-${caseId}.json`)

describe('Protocol > Requests > CreateTopics > v7', () => {
  test.each(CASES)('encodes the Apache 4.3.1 %s vector byte for byte', async (caseId) => {
    const { hex, fields } = vector(caseId)
    const { buffer } = await RequestV7Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('is reachable through the version map as v7 of CreateTopics', async () => {
    const { hex, fields } = vector('explicit')
    const { request } = protocol({ version: 7 })(fields)

    expect(request).toMatchObject({ apiKey: 19, apiVersion: 7, apiName: 'CreateTopics' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
