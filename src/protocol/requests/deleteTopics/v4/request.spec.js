const RequestV4Protocol = require('./request')
const { protocol } = require('../index')

const vector = () => require('../fixtures/java/deleteTopicsRequest-v4-names.json')

describe('Protocol > Requests > DeleteTopics > v4', () => {
  test('encodes the Apache 4.3.1 names vector byte for byte', async () => {
    const { hex, fields } = vector()
    const { buffer } = await RequestV4Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('sends a timeout of 5000 ms when it is not given', async () => {
    const { hex, fields } = vector()
    const { buffer } = await RequestV4Protocol({ topics: fields.topics }).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('is reachable through the version map as v4 of DeleteTopics', async () => {
    const { hex, fields } = vector()
    const { request } = protocol({ version: 4 })(fields)

    expect(request).toMatchObject({ apiKey: 20, apiVersion: 4, apiName: 'DeleteTopics' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
