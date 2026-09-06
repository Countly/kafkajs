const RequestV2Protocol = require('./request')
const RequestV0Protocol = require('../v0/request')
const Encoder = require('../../../encoder')
const Decoder = require('../../../decoder')

/** What every mechanism provider hands over: INT32-length-prefixed bytes. */
const prefixed = (payload) => new Encoder().writeBytes(payload).buffer

describe('Protocol > Requests > SaslAuthenticate > v2', () => {
  test.each([
    ['plain', Buffer.from(' user password')],
    ['scram-sha-256', Buffer.from('n,,n=user,r=clientnonce')],
    ['scram-sha-512', Buffer.from('c=biws,r=nonce,p=proof')],
    ['oauthbearer', Buffer.from('n,,auth=Bearer token')],
    ['aws', Buffer.from('{"version":"2020_10_22","host":"kafka"}')],
  ])(
    '%s: the compact payload is the v0 payload minus its INT32 prefix',
    async (_mechanism, payload) => {
      const authBytes = prefixed(payload)

      const v2 = await RequestV2Protocol({ authBytes }).encode()
      const v0 = await RequestV0Protocol({ authBytes }).encode()

      expect(v0.buffer).toEqual(authBytes)
      expect(RequestV2Protocol({ authBytes }).apiVersion).toEqual(2)

      const decoder = new Decoder(v2.buffer)
      expect(decoder.readUVarIntBytes()).toEqual(payload)
      expect(decoder.readTaggedFields().size).toEqual(0)
      expect(decoder.canReadBytes(1)).toBe(false)
    }
  )

  test('the legal null encoding becomes the compact-bytes null, a single 0x00', async () => {
    const { buffer } = await RequestV2Protocol({ authBytes: prefixed(null) }).encode()

    expect(buffer[0]).toEqual(0x00)
    expect(buffer.length).toEqual(2) // the null byte plus the body tag buffer
  })

  test('a buffer whose prefix disagrees with its length is a provider bug, not a wire frame', async () => {
    const malformed = Buffer.concat([new Encoder().writeInt32(99).buffer, Buffer.from('short')])

    await expect(
      RequestV2Protocol({ authBytes: malformed, mechanism: 'custom' }).encode()
    ).rejects.toMatchObject({ name: 'KafkaJSSASLAuthenticationError' })
    await expect(
      RequestV2Protocol({ authBytes: malformed, mechanism: 'custom' }).encode()
    ).rejects.toThrow(/custom/)
  })
})
