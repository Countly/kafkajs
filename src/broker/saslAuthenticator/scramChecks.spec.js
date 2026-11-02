const crypto = require('crypto')
const Encoder = require('../../protocol/encoder')
const Decoder = require('../../protocol/decoder')
const { SCRAM, DIGESTS } = require('./scram')
const { KafkaJSError, KafkaJSSASLAuthenticationError } = require('../../errors')
const { failureDetail } = require('./failure')

/**
 * Hand-built inert markers, one per secret class; never real credentials. A hostile server places
 * its own markers in the nonce, the salt and the error attribute.
 */
const PASSWORD = 'pw-marker-scram-7f3a'
const SERVER_NONCE = 'nonce-marker-91c2'
const SALT = Buffer.from('salt-marker-55d0')
const SERVER_ERROR = 'error-marker-08be'
const MARKERS = [PASSWORD, SERVER_NONCE, SALT.toString(), SALT.toString('base64'), SERVER_ERROR]

/** The INT32-prefixed bytes a mechanism response decoder reads; null frames a null payload. */
const framed = (text) => new Encoder().writeBytes(text).buffer

/**
 * The server-final message an honest server sends (RFC 5802 section 3), computed here with node:crypto
 * alone from the salted password the server stores.
 * @param {object} digest a `DIGESTS` entry
 * @param {Buffer} salted the server's salted password
 * @param {string} serverFirst the server-first message the client answered
 * @returns {(context: { clientFirstBare: string, message: string }) => string}
 */
const honestFinal =
  (digest, salted, serverFirst) =>
  ({ clientFirstBare, message }) => {
    const hmac = (key, data) => crypto.createHmac(digest.type, key).update(data).digest()
    const withoutProof = message.slice(0, message.indexOf(',p='))
    const authMessage = [clientFirstBare, serverFirst, withoutProof].join(',')
    return `v=${hmac(hmac(salted, 'Server Key'), authMessage).toString('base64')}`
  }

/**
 * Runs the real SCRAM client against scripted server messages, through the real request encoders
 * and the real server-message decoder and parser.
 * @param {object} options
 * @param {(clientNonce: string) => string|null} options.first the server-first message
 * @param {(context: { clientFirstBare: string, message: string }) => string} options.final
 * @param {object} [options.digest=DIGESTS.SHA256]
 * @returns {{ scram: SCRAM, logger: object, sent: string[], saslAuthenticate: jest.Mock }}
 */
const exchange = ({ first, final, digest = DIGESTS.SHA256 }) => {
  const logger = { debug: jest.fn(), error: jest.fn() }
  const sent = []
  const saslAuthenticate = jest.fn(async ({ request, response }) => {
    const message = new Decoder(await request.encode()).readBytes().toString()
    sent.push(message)
    const reply =
      sent.length === 1
        ? first(message.slice('n,,n=user,r='.length))
        : final({ clientFirstBare: sent[0].slice('n,,'.length), message })
    return response.parse(await response.decode(framed(reply)))
  })
  const scram = new SCRAM(
    { username: 'user', password: PASSWORD },
    'broker',
    9092,
    logger,
    saslAuthenticate,
    digest
  )
  return { scram, logger, sent, saslAuthenticate }
}

/** Every string a failure exposes: message, stack, cause and each logger argument. */
const exposed = (error, logger) =>
  [
    error.message,
    error.stack,
    String(error.cause ?? ''),
    JSON.stringify([...logger.debug.mock.calls, ...logger.error.mock.calls]),
  ].join('\n')

/** The salted password the derivation spy returns, so no derivation ever does real work. */
const saltedOf = (length) => Buffer.alloc(length, 7)

describe('Broker > SASL Authenticator > SCRAM server message checks', () => {
  let pbkdf2

  beforeEach(() => {
    pbkdf2 = jest
      .spyOn(crypto, 'pbkdf2')
      .mockImplementation((password, salt, iterations, length, digest, callback) =>
        callback(null, saltedOf(length))
      )
  })

  afterEach(() => {
    jest.restoreAllMocks()
  })

  test.each([
    ['SHA-256', DIGESTS.SHA256],
    ['SHA-512', DIGESTS.SHA512],
  ])(
    'authenticates a %s server that extends the nonce, comparing its signature in constant time',
    async (_, digest) => {
      const timingSafeEqual = jest.spyOn(crypto, 'timingSafeEqual')
      const salted = saltedOf(digest.length)
      let serverFirst
      const { scram } = exchange({
        digest,
        first: (clientNonce) => {
          serverFirst = `r=${clientNonce}${SERVER_NONCE},s=${SALT.toString('base64')},i=4096`
          return serverFirst
        },
        final: (context) => honestFinal(digest, salted, serverFirst)(context),
      })

      await expect(scram.authenticate()).resolves.toBeUndefined()
      expect(pbkdf2.mock.calls.map((call) => call.slice(0, 5))).toEqual([
        [PASSWORD, SALT, 4096, digest.length, digest.type],
        [PASSWORD, SALT, 4096, digest.length, digest.type],
      ])
      expect(timingSafeEqual).toHaveBeenCalledTimes(1)
      const [received, expected] = timingSafeEqual.mock.calls[0]
      expect([Buffer.isBuffer(received), Buffer.isBuffer(expected)]).toEqual([true, true])
      expect([received.length, expected.length]).toEqual([digest.length, digest.length])
    }
  )

  it('never derives a key from a hostile iteration count', async () => {
    const { scram } = exchange({
      first: (clientNonce) =>
        `r=${clientNonce}${SERVER_NONCE},s=${SALT.toString('base64')},i=1000000000`,
      final: () => 'v=AAAA',
    })

    await expect(scram.authenticate()).rejects.toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(pbkdf2.mock.calls.map((call) => call[2])).toEqual([])
  })

  test('names a failed key derivation by its class only', async () => {
    pbkdf2.mockImplementation((password, salt, iterations, length, digest, callback) =>
      callback(new Error(`cannot derive from ${PASSWORD}`))
    )
    const { scram, logger } = exchange({
      first: (clientNonce) => `r=${clientNonce}${SERVER_NONCE},s=${SALT.toString('base64')},i=4096`,
      final: () => 'v=AAAA',
    })

    const error = await scram.authenticate().catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.message).toBe('SASL SCRAM SHA256 authentication failed: Error')
    expect(exposed(error, logger)).not.toContain(PASSWORD)
  })

  test('rejects an invalid salt before invoking PBKDF2', async () => {
    const { scram } = exchange({
      first: (nonce) => `r=${nonce}x,s=!!!!,i=4096`,
      final: () => 'v=AAAA',
    })

    await expect(scram.authenticate()).rejects.toThrow(
      'SASL SCRAM SHA256 authentication failed: Invalid salt in the server first message'
    )
    expect(pbkdf2).not.toHaveBeenCalled()
  })

  test('rejects garbage appended to an otherwise correct verifier', async () => {
    let serverFirst
    const timingSafeEqual = jest.spyOn(crypto, 'timingSafeEqual')
    const { scram } = exchange({
      first: (nonce) => {
        serverFirst = `r=${nonce}x,s=${SALT.toString('base64')},i=4096`
        return serverFirst
      },
      final: (context) => `${honestFinal(DIGESTS.SHA256, saltedOf(32), serverFirst)(context)}!!!!`,
    })

    await expect(scram.authenticate()).rejects.toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(timingSafeEqual).not.toHaveBeenCalled()
  })

  test('accepts an iteration count of exactly 1048576', async () => {
    const salted = saltedOf(32)
    let serverFirst
    const { scram } = exchange({
      first: (clientNonce) => {
        serverFirst = `r=${clientNonce}${SERVER_NONCE},s=${SALT.toString('base64')},i=1048576`
        return serverFirst
      },
      final: (context) => honestFinal(DIGESTS.SHA256, salted, serverFirst)(context),
    })

    await expect(scram.authenticate()).resolves.toBeUndefined()
    expect(pbkdf2.mock.calls.map((call) => call[2])).toEqual([1048576, 1048576])
  })

  test.each([
    [
      'a non-numeric count',
      (n) => `r=${n}x,s=c2FsdA==,i=abc`,
      'Invalid iteration count in the server first message',
    ],
    [
      'a missing count',
      (n) => `r=${n}x,s=c2FsdA==`,
      'Invalid iteration count in the server first message',
    ],
    [
      'an exponent count',
      (n) => `r=${n}x,s=c2FsdA==,i=1e9`,
      'Invalid iteration count in the server first message',
    ],
    [
      'a negative count',
      (n) => `r=${n}x,s=c2FsdA==,i=-4096`,
      'Invalid iteration count in the server first message',
    ],
    [
      'a fractional count',
      (n) => `r=${n}x,s=c2FsdA==,i=4096.5`,
      'Invalid iteration count in the server first message',
    ],
    [
      'a count below the minimum',
      (n) => `r=${n}x,s=c2FsdA==,i=4095`,
      'Requested iterations 4095 is less than the minimum 4096',
    ],
    [
      'a count of a billion',
      (n) => `r=${n}x,s=c2FsdA==,i=1000000000`,
      'Requested iterations 1000000000 is more than the maximum 1048576',
    ],
    [
      'a count one above the ceiling',
      (n) => `r=${n}x,s=c2FsdA==,i=1048577`,
      'Requested iterations 1048577 is more than the maximum 1048576',
    ],
    [
      'a nonce that does not extend the client nonce',
      () => `r=${SERVER_NONCE},s=c2FsdA==,i=4096`,
      'Invalid server nonce, it does not start with the client nonce',
    ],
    [
      'a nonce equal to the client nonce',
      (n) => `r=${n},s=c2FsdA==,i=4096`,
      'Invalid server nonce, it adds no server part',
    ],
    ['a missing salt', (n) => `r=${n}x,i=4096`, 'Missing salt in the server first message'],
    [
      'an unknown attribute',
      (n) => `r=${n}x,s=c2FsdA==,i=4096,x=1`,
      'The SCRAM server message has an unknown or repeated attribute',
    ],
    [
      'a repeated attribute',
      (n) => `r=${n}x,r=${n}y,s=c2FsdA==,i=4096`,
      'The SCRAM server message has an unknown or repeated attribute',
    ],
    [
      'a mandatory extension',
      (n) => `m=ext,r=${n}x,s=c2FsdA==,i=4096`,
      'The SCRAM server message requires an unsupported mandatory extension',
    ],
    ['an unparsable message', () => 'not a scram message', 'Malformed SCRAM server message'],
    ['a null message', () => null, 'Malformed SCRAM server message'],
    ['a server error', () => `e=${SERVER_ERROR}`, 'The server first message reports an error'],
  ])('refuses a server-first message with %s before deriving a key', async (_, first, reason) => {
    const { scram, logger, saslAuthenticate } = exchange({ first, final: () => 'v=AAAA' })

    const error = await scram.authenticate().catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.message).toBe(`SASL SCRAM SHA256 authentication failed: ${reason}`)
    expect(pbkdf2).not.toHaveBeenCalled()
    expect(saslAuthenticate).toHaveBeenCalledTimes(1)
    for (const marker of MARKERS) expect(exposed(error, logger)).not.toContain(marker)
  })

  test.each([
    [
      'a wrong signature of the right length',
      () => `v=${Buffer.alloc(32, 1).toString('base64')}`,
      1,
      'Invalid server signature in server final message',
    ],
    [
      'a signature of the wrong length',
      () => 'v=AAAA',
      0,
      'Invalid server signature in server final message',
    ],
    [
      'a signature that is not base64',
      () => 'v=!!!!',
      0,
      'Invalid server signature in server final message',
    ],
    ['no signature', () => 'r=x', 0, 'Invalid server signature in server final message'],
    ['a server error', () => `e=${SERVER_ERROR}`, 0, 'The server final message reports an error'],
    [
      'an unknown attribute',
      () => 'x=1',
      0,
      'The SCRAM server message has an unknown or repeated attribute',
    ],
    [
      'a mandatory extension',
      () => 'm=1,v=AAAA',
      0,
      'The SCRAM server message requires an unsupported mandatory extension',
    ],
  ])('refuses a server-final message with %s', async (_, final, comparisons, reason) => {
    const timingSafeEqual = jest.spyOn(crypto, 'timingSafeEqual')
    const { scram, logger, sent } = exchange({
      first: (clientNonce) => `r=${clientNonce}${SERVER_NONCE},s=${SALT.toString('base64')},i=4096`,
      final,
    })

    const error = await scram.authenticate().catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.message).toBe(`SASL SCRAM SHA256 authentication failed: ${reason}`)
    expect(timingSafeEqual).toHaveBeenCalledTimes(comparisons)
    const proof = sent[1].slice(sent[1].indexOf(',p=') + ',p='.length)
    for (const marker of [...MARKERS, proof]) expect(exposed(error, logger)).not.toContain(marker)
  })
})

describe('SASL diagnostic provenance', () => {
  test.each(['kafkajs', 'name', 'getters'])('does not trust supplier %s errors', async (kind) => {
    const secret = 'supplier-secret-marker'
    const reads = jest.fn(() => {
      throw new Error(secret)
    })
    const error =
      kind === 'kafkajs'
        ? new KafkaJSError(secret)
        : kind === 'name'
          ? Object.assign(new Error(secret), { name: secret })
          : Object.defineProperties(
              {},
              Object.fromEntries(
                ['name', 'message', 'code', 'stack'].map((key) => [key, { get: reads }])
              )
            )
    const { scram, logger } = exchange({ first: () => '', final: () => '' })
    scram.saslAuthenticate.mockRejectedValue(error)
    const thrown = await scram.authenticate().catch((e) => e)
    expect(thrown.message).toBe('SASL SCRAM SHA256 authentication failed: Error')
    expect(exposed(thrown, logger)).not.toContain(secret)
    expect(reads).not.toHaveBeenCalled()
  })

  test('keeps the XOR invariant reason but property copies cannot transfer trust', async () => {
    const { scram } = exchange({
      first: (nonce) => `r=${nonce}x,s=c2FsdA==,i=4096`,
      final: () => '',
    })
    scram.clientKey = async () => Buffer.alloc(1)
    scram.clientSignature = () => Buffer.alloc(2)
    await expect(scram.authenticate()).rejects.toThrow(
      'SASL SCRAM SHA256 authentication failed: Buffers must be of the same length'
    )
    let trusted
    try {
      SCRAM.xor(Buffer.alloc(1), Buffer.alloc(2))
    } catch (error) {
      trusted = error
    }
    expect(failureDetail(trusted)).toBe('Buffers must be of the same length')
    const copy = Object.create(
      Object.getPrototypeOf(trusted),
      Object.getOwnPropertyDescriptors(trusted)
    )
    expect(failureDetail(copy)).toBe('Error')
    expect(failureDetail(Object.assign(new Error('supplier-secret'), trusted))).toBe('Error')
  })
})
