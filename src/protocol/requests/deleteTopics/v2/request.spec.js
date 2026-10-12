const RequestV2Protocol = require('./request')
const { protocol } = require('../index')

const vector = () => require('../fixtures/java/deleteTopicsRequest-v2-names.json')

describe('Protocol > Requests > DeleteTopics > v2', () => {
  test('encodes the Apache 4.3.1 names vector byte for byte', async () => {
    const { hex, fields } = vector()
    const { buffer } = await RequestV2Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('is reachable through the version map as v2 of DeleteTopics', async () => {
    const { hex, fields } = vector()
    const { request } = protocol({ version: 2 })(fields)

    expect(request).toMatchObject({ apiKey: 20, apiVersion: 2, apiName: 'DeleteTopics' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
