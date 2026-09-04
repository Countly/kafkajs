const RequestV3Protocol = require('./request')

describe('Protocol > Requests > ApiVersions > v3', () => {
  test('request', async () => {
    const { buffer } = await RequestV3Protocol({
      clientSoftwareName: 'countly-kafkajs',
      clientSoftwareVersion: '3.0.0-alpha.4',
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v3_request.json')))
  })

  test('defaults to this package identity', async () => {
    const { CLIENT_SOFTWARE_NAME, clientSoftwareVersion } = require('../../../clientSoftware')
    const request = RequestV3Protocol({})
    const { buffer } = await request.encode()
    const Decoder = require('../../../decoder')
    const decoder = new Decoder(buffer)

    expect(decoder.readUVarIntString()).toEqual(CLIENT_SOFTWARE_NAME)
    expect(decoder.readUVarIntString()).toEqual(clientSoftwareVersion())
    expect(decoder.readTaggedFields().size).toEqual(0)
  })
})
