const RequestV4Protocol = require('./request')
const { protocol } = require('../index')

const CASES = ['defaults', 'explicit', 'assigned']
const vector = (caseId) => require(`../fixtures/java/createTopicsRequest-v4-${caseId}.json`)

describe('Protocol > Requests > CreateTopics > v4', () => {
  test.each(CASES)('encodes the Apache 4.3.1 %s vector byte for byte', async (caseId) => {
    const { hex, fields } = vector(caseId)
    const { buffer } = await RequestV4Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('sends -1 partitions and -1 replication factor for a topic that names neither', async () => {
    const { hex, fields } = vector('defaults')
    const { buffer } = await RequestV4Protocol({
      topics: [{ topic: fields.topics[0].topic }],
      timeout: fields.timeout,
    }).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('is reachable through the version map as v4 of CreateTopics', async () => {
    const { hex, fields } = vector('assigned')
    const { request } = protocol({ version: 4 })(fields)

    expect(request).toMatchObject({ apiKey: 19, apiVersion: 4, apiName: 'CreateTopics' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
