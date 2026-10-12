const RequestV6Protocol = require('./request')
const { protocol, versions } = require('../index')

const vector = () => require('../fixtures/java/deleteTopicsRequest-v6-byName.json')

describe('Protocol > Requests > DeleteTopics > v6', () => {
  test('encodes the Apache 4.3.1 byName vector byte for byte', async () => {
    const { hex, fields } = vector()
    const { buffer } = await RequestV6Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('takes names, as at every version, and sends each with the zero uuid', async () => {
    const { fields } = vector()
    const { buffer } = await RequestV6Protocol({ topics: ['t'], timeout: fields.timeout }).encode()

    expect(buffer.toString('hex')).toEqual(
      `02${'02' + '74' + '00'.repeat(16) + '00'}` + '00001388' + '00'
    )
  })

  test('sends a timeout of 5000 ms when it is not given', async () => {
    const { hex, fields } = vector()
    const { buffer } = await RequestV6Protocol({ topics: fields.topics }).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('is reachable through the version map as v6 of DeleteTopics', async () => {
    const { hex, fields } = vector()
    const { request } = protocol({ version: 6 })(fields)

    expect(versions).toEqual(['1', '2', '3', '4', '5', '6'])
    expect(request).toMatchObject({ apiKey: 20, apiVersion: 6, apiName: 'DeleteTopics' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
