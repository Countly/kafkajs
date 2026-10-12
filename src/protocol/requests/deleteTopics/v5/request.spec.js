const RequestV5Protocol = require('./request')
const { protocol } = require('../index')

const vector = () => require('../fixtures/java/deleteTopicsRequest-v5-names.json')

describe('Protocol > Requests > DeleteTopics > v5', () => {
  test('encodes the Apache 4.3.1 names vector byte for byte', async () => {
    const { hex, fields } = vector()
    const { buffer } = await RequestV5Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('is reachable through the version map as v5 of DeleteTopics', async () => {
    const { hex, fields } = vector()
    const { request } = protocol({ version: 5 })(fields)

    expect(request).toMatchObject({ apiKey: 20, apiVersion: 5, apiName: 'DeleteTopics' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
