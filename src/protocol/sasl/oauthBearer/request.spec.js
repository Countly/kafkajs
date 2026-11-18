const request = require('./request')

// Expected bytes are written out by hand: an INT32 length prefix, then the RFC 7628 client
// response with US-ASCII SOH (\u0001) as the key/value separator.
const cases = [
  {
    name: 'without authorizationIdentity or extensions',
    sasl: {},
    token: { value: 'tok' },
    prefix: [0x00, 0x00, 0x00, 0x15],
    body: 'n,,\u0001auth=Bearer tok\u0001\u0001',
  },
  {
    name: 'with authorizationIdentity and no extensions',
    sasl: { authorizationIdentity: 'user' },
    token: { value: 'tok' },
    prefix: [0x00, 0x00, 0x00, 0x1b],
    body: 'n,a=user,\u0001auth=Bearer tok\u0001\u0001',
  },
  {
    name: 'with extensions and no authorizationIdentity',
    sasl: {},
    token: { value: 'tok', extensions: { traceId: 'abc', region: 'eu' } },
    prefix: [0x00, 0x00, 0x00, 0x2b],
    body: 'n,,\u0001auth=Bearer tok\u0001traceId=abc\u0001region=eu\u0001\u0001',
  },
  {
    name: 'with authorizationIdentity and extensions',
    sasl: { authorizationIdentity: 'user' },
    token: { value: 'tok', extensions: { traceId: 'abc' } },
    prefix: [0x00, 0x00, 0x00, 0x27],
    body: 'n,a=user,\u0001auth=Bearer tok\u0001traceId=abc\u0001\u0001',
  },
  {
    name: 'with an authorizationIdentity holding a comma and an equals sign',
    sasl: { authorizationIdentity: 'a,b=c' },
    token: { value: 'tok' },
    prefix: [0x00, 0x00, 0x00, 0x20],
    body: 'n,a=a=2Cb=3Dc,\u0001auth=Bearer tok\u0001\u0001',
  },
  {
    name: 'with an empty authorizationIdentity',
    sasl: { authorizationIdentity: '' },
    token: { value: 'tok' },
    prefix: [0x00, 0x00, 0x00, 0x15],
    body: 'n,,\u0001auth=Bearer tok\u0001\u0001',
  },
]

// Apache Kafka's OAuthBearerClientInitialResponse CLIENT_INITIAL_RESPONSE_PATTERN (the saslname
// of RFC 5801: ASCII except NUL, "=" and ",", or the escapes =2C and =3D).
const SASLNAME = '(?:[\\x01-\\x2B\\x2D-\\x3C\\x3E-\\x7F]|=2C|=3D)+'
const KVPAIRS = '(?:[A-Za-z]+=[\\x21-\\x7E \\t\\r\\n]+\\x01)*'
const KAFKA_CLIENT_RESPONSE = new RegExp(`^n,(a=${SASLNAME})?,\\x01${KVPAIRS}\\x01$`)

describe('Protocol > SASL OAUTHBEARER > initial client response', () => {
  test.each(cases)('is byte-exact $name', async ({ sasl, token, prefix, body }) => {
    const { encode } = await request(sasl, token)

    expect(await encode()).toEqual(
      Buffer.concat([Buffer.from(prefix), Buffer.from(body, 'latin1')])
    )
  })

  test.each(cases)('is accepted by the broker grammar $name', async ({ sasl, token }) => {
    const { encode } = await request(sasl, token)
    const bytes = await encode()

    expect(bytes.readInt32BE(0)).toBe(bytes.length - 4)
    expect(bytes.toString('latin1', 4)).toMatch(KAFKA_CLIENT_RESPONSE)
  })
})
