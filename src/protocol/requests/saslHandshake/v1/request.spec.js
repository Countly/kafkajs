const RequestV1Protocol = require('./request')
const { protocol, versions } = require('../index')
const golden = require('../fixtures/v1_request.json')

describe('Protocol > Requests > SASLHandshake > v1 request', () => {
  test('encodes the hand-computed golden byte for byte', async () => {
    const { buffer } = await RequestV1Protocol({ mechanism: golden.fields.mechanism }).encode()

    expect(buffer.toString('hex')).toEqual(golden.hex)
  })

  test('announces version 1 and is reachable through the version map', () => {
    const { request } = protocol({ version: 1 })({ mechanism: 'PLAIN' })

    expect(request).toMatchObject({ apiName: 'SaslHandshake', apiVersion: 1 })
    expect(versions).toEqual(['0', '1'])
  })
})
