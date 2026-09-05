const RequestV4Protocol = require('./request')

describe('Protocol > Requests > ApiVersions > v4', () => {
  test('request', async () => {
    const { buffer } = await RequestV4Protocol({
      clientSoftwareName: 'countly-kafkajs',
      clientSoftwareVersion: '3.0.0-alpha.4',
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v4_request.json')))
    // v4 is wire-identical to v3 (KAFKA-17011); the two fixtures must never drift apart.
    expect(Buffer.from(require('../fixtures/v4_request.json'))).toEqual(
      Buffer.from(require('../fixtures/v3_request.json'))
    )
  })

  test('matches the vector produced by the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV4Protocol({
      clientSoftwareName: 'countly-kafkajs',
      clientSoftwareVersion: '3.0.0-alpha.4',
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v4_request_java.json')))
  })

  test('defaults to this package identity', async () => {
    const { CLIENT_SOFTWARE_NAME, clientSoftwareVersion } = require('../../../clientSoftware')
    const request = RequestV4Protocol({})
    const { buffer } = await request.encode()
    const Decoder = require('../../../decoder')
    const decoder = new Decoder(buffer)

    expect(decoder.readUVarIntString()).toEqual(CLIENT_SOFTWARE_NAME)
    expect(decoder.readUVarIntString()).toEqual(clientSoftwareVersion())
    expect(decoder.readTaggedFields().size).toEqual(0)
  })
})
