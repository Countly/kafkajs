const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { jwtBearer } = require('../jwtBearer')
const { signAssertion } = require('../signAssertion')
const { KafkaJSSASLAuthenticationError, KafkaJSInvalidConfigurationError } = require('../../errors')

const MARKER = 'inert-marker'
const NOW = 1792000000000
const deferred = () => {
  const result = {}
  result.promise = new Promise((resolve, reject) => Object.assign(result, { resolve, reject }))
  return result
}
const observe = (promise) => {
  const seen = {}
  promise.then(
    (value) => {
      seen.value = value
    },
    (error) => {
      seen.error = error
    }
  )
  return seen
}
const textOf = (value, seen = new Set()) => {
  if (value === null || value === undefined) return ''
  if (typeof value !== 'object' && typeof value !== 'function') return String(value)
  if (seen.has(value)) return ''
  seen.add(value)
  return Object.getOwnPropertyNames(value)
    .map((name) => textOf(value[name], seen))
    .concat(value instanceof Error ? [String(value.stack), textOf(value.cause, seen)] : [])
    .join('\n')
}
const json = (body, status = 200) => new Response(JSON.stringify(body), { status })
const decode = (part) => JSON.parse(Buffer.from(part, 'base64url').toString('utf8'))
const verifies = (assertion, publicKey, digest, ecdsa) => {
  const [header, claims, signature] = assertion.split('.')
  return crypto.verify(
    digest,
    Buffer.from(`${header}.${claims}`),
    ecdsa ? { key: publicKey, dsaEncoding: 'ieee-p1363' } : publicKey,
    Buffer.from(signature, 'base64url')
  )
}
const pem = (key, options = {}) => key.export({ type: 'pkcs8', format: 'pem', ...options })

describe('OAuth JWT bearer and signer', () => {
  const keys = {}
  const providers = []
  let dir

  beforeAll(() => {
    keys.rsa = crypto.generateKeyPairSync('rsa', { modulusLength: 2048 })
    keys.rsaOther = crypto.generateKeyPairSync('rsa', { modulusLength: 2048 })
    keys.rsaThird = crypto.generateKeyPairSync('rsa', { modulusLength: 2048 })
    keys.pss = crypto.generateKeyPairSync('rsa-pss', { modulusLength: 2048 })
    keys.rsaShort = crypto.generateKeyPairSync('rsa', { modulusLength: 1024 })
    keys.p256 = crypto.generateKeyPairSync('ec', { namedCurve: 'prime256v1' })
    keys.p384 = crypto.generateKeyPairSync('ec', { namedCurve: 'secp384r1' })
    keys.p521 = crypto.generateKeyPairSync('ec', { namedCurve: 'secp521r1' })
    keys.ed25519 = crypto.generateKeyPairSync('ed25519')
  })

  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'oauth-signer-'))
    jest.useFakeTimers({ now: NOW })
  })

  afterEach(() => {
    providers.splice(0).forEach((provider) => provider.close())
    const timers = jest.getTimerCount()
    jest.restoreAllMocks()
    jest.useRealTimers()
    fs.rmSync(dir, { recursive: true, force: true })
    expect(timers).toBe(0)
  })

  const provider = (options) => {
    const created = jwtBearer({ tokenEndpointUrl: 'https://issuer.example/token', ...options })
    providers.push(created)
    return created
  }
  const keyFile = (key, name = 'key.pem') => {
    const file = path.join(dir, name)
    fs.writeFileSync(file, pem(key))
    return file
  }
  const touch = (file, seconds) => fs.utimesSync(file, NOW / 1000 + seconds, NOW / 1000 + seconds)

  describe('the JWT bearer grant', () => {
    test('JWT bearer sends its grant without Basic authorization', async () => {
      const fetch = jest.fn(async () => json({ access_token: 'opaque-token' }))
      const created = provider({ assertion: async () => 'a+b.c/d.e=f', scope: 'read write', fetch })
      expect(await created()).toEqual({ value: 'opaque-token' })
      const [url, init] = fetch.mock.calls[0]
      expect(url).toBe('https://issuer.example/token')
      expect(init.body).toBe(
        'grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=a%2Bb.c%2Fd.e%3Df&scope=read+write'
      )
      expect(init.headers.Authorization).toBeUndefined()
      expect(init.body).not.toContain('client_assertion_type')
      expect(init.body).not.toContain('client_id')
    })

    test('a synchronous supplier and no scope send only the grant and assertion', async () => {
      const fetch = jest.fn(async () => json({ access_token: 'opaque-token' }))
      await provider({ assertion: () => 'header.payload.signature', fetch })()
      expect(fetch.mock.calls[0][1].body).toBe(
        'grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=header.payload.signature'
      )
    })

    test('a failing supplier is retried and reported without its text', async () => {
      const assertion = jest
        .fn()
        .mockRejectedValueOnce(new Error(MARKER))
        .mockImplementationOnce(() => {
          throw new Error(MARKER)
        })
        .mockResolvedValue('header.payload.signature')
      const fetch = jest.fn(async () => json({ access_token: 'opaque-token' }))
      const call = observe(provider({ assertion, fetch })())
      await jest.advanceTimersByTimeAsync(300)
      expect(assertion).toHaveBeenCalledTimes(3)
      expect(call.value).toEqual({ value: 'opaque-token' })
      const failing = observe(
        provider({ assertion: () => Promise.reject(new Error(MARKER)), fetch })()
      )
      await jest.advanceTimersByTimeAsync(8000)
      expect(failing.error.message).toBe(
        'SASL OAUTHBEARER helper: assertion: the assertion supplier failed'
      )
      expect(textOf(failing.error)).not.toContain(MARKER)
      expect(fetch).toHaveBeenCalledTimes(1)
    })

    test.each([[null], [undefined], [''], [5], [{ assertion: 'x' }]])(
      'a supplier returning %p fails terminally',
      async (value) => {
        const assertion = jest.fn(async () => value)
        const fetch = jest.fn()
        const error = await provider({ assertion, fetch })().catch((caught) => caught)
        expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
        expect(error.message).toBe(
          'SASL OAUTHBEARER helper: assertion: the assertion supplier failed'
        )
        expect(assertion).toHaveBeenCalledTimes(1)
        expect(fetch).not.toHaveBeenCalled()
      }
    )

    test.each([
      [{ assertion: 'header.payload.signature' }, 'assertion'],
      [{ assertion: undefined }, 'assertion'],
      [{ assertion: () => 'a', scope: 5 }, 'scope'],
      [
        { assertion: () => 'a', tokenEndpointUrl: 'http://issuer.example/token' },
        'tokenEndpointUrl',
      ],
      [{ assertion: () => 'a', maxRetryTime: 0 }, 'maxRetryTime'],
    ])('jwt bearer validates its options before work: %p', (options, name) => {
      expect(() => provider(options)).toThrow(`Invalid OAuth bearer helper option: ${name}`)
    })

    test('refuses options that are not an object', () => {
      expect(() => jwtBearer(undefined)).toThrow(KafkaJSInvalidConfigurationError)
    })
  })

  describe('the local signer', () => {
    test.each([
      ['RS256', 'rsa', 'sha256', 256],
      ['RS384', 'rsa', 'sha384', 256],
      ['RS512', 'rsa', 'sha512', 256],
      ['ES256', 'p256', 'sha256', 64],
      ['ES384', 'p384', 'sha384', 96],
      ['ES512', 'p521', 'sha512', 132],
    ])(
      '%s verifies independently with the required signature encoding',
      async (algorithm, name, digest, size) => {
        const { privateKey, publicKey } = keys[name]
        const supplier = signAssertion({
          privateKey,
          algorithm,
          keyId: 'key-one',
          claims: {
            iss: 'issuer',
            sub: 'client',
            aud: ['service', 'endpoint'],
            tenant: { region: 'eu' },
          },
        })
        const assertion = await supplier()
        const [header, claims, signature] = assertion.split('.')
        expect(decode(header)).toEqual({ alg: algorithm, typ: 'JWT', kid: 'key-one' })
        expect(decode(claims)).toEqual({
          iss: 'issuer',
          sub: 'client',
          aud: ['service', 'endpoint'],
          tenant: { region: 'eu' },
          iat: NOW / 1000,
          nbf: NOW / 1000 - 60,
          exp: NOW / 1000 + 300,
        })
        expect(assertion).not.toContain('=')
        expect(Buffer.from(signature, 'base64url')).toHaveLength(size)
        expect(verifies(assertion, publicKey, digest, name !== 'rsa')).toBe(true)
        const otherDigest = { sha256: 'sha384', sha384: 'sha512', sha512: 'sha256' }[digest]
        expect(verifies(assertion, publicKey, otherDigest, name !== 'rsa')).toBe(false)
      }
    )

    test('signer defaults use the declared time controls', async () => {
      jest.setSystemTime(NOW + 999)
      const assertion = await signAssertion({ privateKey: keys.rsa.privateKey })()
      const [header, claims] = assertion.split('.')
      expect(decode(header)).toEqual({ alg: 'RS256', typ: 'JWT' })
      expect(decode(claims)).toEqual({
        iat: NOW / 1000,
        nbf: NOW / 1000 - 60,
        exp: NOW / 1000 + 300,
      })
      expect(verifies(assertion, keys.rsa.publicKey, 'sha256')).toBe(true)
    })

    test.each([
      [
        { expiresIn: 1, notBefore: 0 },
        { nbf: 0, exp: 1 },
      ],
      [
        { expiresIn: 2147483647, notBefore: 2147483647 },
        { nbf: -2147483647, exp: 2147483647 },
      ],
    ])('signer options alter only their declared fields: %p', async (options, offsets) => {
      const claims = decode(
        (await signAssertion({ privateKey: keys.rsa.privateKey, ...options })()).split('.')[1]
      )
      expect(claims).toEqual({
        iat: NOW / 1000,
        nbf: NOW / 1000 + offsets.nbf,
        exp: NOW / 1000 + offsets.exp,
      })
    })

    test('jti false omits the identifier and an empty key id is kept', async () => {
      const assertion = await signAssertion({
        privateKey: keys.rsa.privateKey,
        jti: false,
        keyId: '',
      })()
      expect(decode(assertion.split('.')[0])).toEqual({ alg: 'RS256', typ: 'JWT', kid: '' })
      expect(decode(assertion.split('.')[1])).not.toHaveProperty('jti')
    })

    test('every assertion receives fresh time and identifier', async () => {
      const supplier = signAssertion({ privateKey: keys.rsa.privateKey, jti: true })
      const first = decode((await supplier()).split('.')[1])
      jest.setSystemTime(NOW + 1000)
      const second = decode((await supplier()).split('.')[1])
      expect(second.iat - first.iat).toBe(1)
      expect(second.nbf - first.nbf).toBe(1)
      expect(second.exp - first.exp).toBe(1)
      expect(first.jti).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
      )
      expect(second.jti).not.toBe(first.jti)
    })

    test('PEM Buffer KeyObject and encrypted PEM share the same contract', async () => {
      const { privateKey, publicKey } = keys.rsa
      const forms = [
        { privateKey: pem(privateKey) },
        { privateKey: Buffer.from(pem(privateKey)) },
        { privateKey },
        {
          privateKey: pem(privateKey, { cipher: 'aes-256-cbc', passphrase: 'phrase' }),
          passphrase: 'phrase',
        },
      ]
      for (const form of forms) {
        expect(verifies(await signAssertion(form)(), publicKey, 'sha256')).toBe(true)
      }
    })

    test.each([
      [
        'a wrong passphrase',
        () => ({
          privateKey: pem(keys.rsa.privateKey, { cipher: 'aes-256-cbc', passphrase: 'phrase' }),
          passphrase: MARKER,
        }),
      ],
      [
        'a missing passphrase',
        () => ({
          privateKey: pem(keys.rsa.privateKey, { cipher: 'aes-256-cbc', passphrase: 'phrase' }),
        }),
      ],
      ['a public key object', () => ({ privateKey: keys.rsa.publicKey })],
      [
        'a public PEM',
        () => ({ privateKey: keys.rsa.publicKey.export({ type: 'spki', format: 'pem' }) }),
      ],
      ['a secret key object', () => ({ privateKey: crypto.createSecretKey(Buffer.from(MARKER)) })],
      [
        'malformed PEM',
        () => ({ privateKey: `-----BEGIN PRIVATE KEY-----\n${MARKER}\n-----END PRIVATE KEY-----` }),
      ],
      ['an empty PEM', () => ({ privateKey: '' })],
      ['an RSA-PSS key', () => ({ privateKey: keys.pss.privateKey })],
      ['a 1024-bit RSA key', () => ({ privateKey: keys.rsaShort.privateKey })],
      ['an EdDSA key', () => ({ privateKey: keys.ed25519.privateKey })],
      ['an EC key for RS256', () => ({ privateKey: keys.p256.privateKey })],
      ['an RSA key for ES256', () => ({ privateKey: keys.rsa.privateKey, algorithm: 'ES256' })],
      ['P-384 for ES256', () => ({ privateKey: keys.p384.privateKey, algorithm: 'ES256' })],
      ['P-256 for ES384', () => ({ privateKey: keys.p256.privateKey, algorithm: 'ES384' })],
      ['P-384 for ES512', () => ({ privateKey: keys.p384.privateKey, algorithm: 'ES512' })],
      ['an unreadable key file', () => ({ privateKey: { file: path.join(dir, `${MARKER}.pem`) } })],
    ])('an initial key with %s is a configuration error', (_, options) => {
      let error
      try {
        signAssertion(options())
      } catch (caught) {
        error = caught
      }
      expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(error.message).toBe('Invalid OAuth bearer helper option: privateKey')
      expect(error.cause).toBeUndefined()
      expect(textOf(error)).not.toContain(MARKER)
    })

    test.each([
      [[]],
      [5],
      [null],
      [undefined],
      [{}],
      [{ file: 'key.pem', key: 'pem' }],
      [{ path: 'key.pem' }],
      [new Date(0)],
    ])('privateKey %p is refused', (privateKey) => {
      expect(() => signAssertion({ privateKey })).toThrow(
        'Invalid OAuth bearer helper option: privateKey'
      )
    })

    test.each([[''], [5], ['a\0b']])('a key file path %p is refused', (file) => {
      expect(() => signAssertion({ privateKey: { file } })).toThrow(
        'Invalid OAuth bearer helper option: privateKey.file'
      )
    })

    test.each([
      ['an array', ['RS256']],
      ['an object', { toString: () => 'RS256' }],
    ])('refuses %s algorithm before key-file access', (_, algorithm) => {
      const file = keyFile(keys.rsa.privateKey)
      const stat = jest.spyOn(fs, 'statSync')
      const read = jest.spyOn(fs, 'readFileSync')
      let error
      try {
        signAssertion({ privateKey: { file }, algorithm })
      } catch (caught) {
        error = caught
      }
      expect(stat).not.toHaveBeenCalled()
      expect(read).not.toHaveBeenCalled()
      expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(error.message).toBe('Invalid OAuth bearer helper option: algorithm')
    })

    test.each([['HS256'], ['none'], ['rs256'], ['RS257'], [null], [5], ['toString']])(
      'unsupported algorithm %p fails at construction',
      (algorithm) => {
        expect(() => signAssertion({ privateKey: keys.rsa.privateKey, algorithm })).toThrow(
          'Invalid OAuth bearer helper option: algorithm'
        )
      }
    )

    test.each([
      [{ expiresIn: 0 }, 'expiresIn'],
      [{ expiresIn: -1 }, 'expiresIn'],
      [{ expiresIn: 1.5 }, 'expiresIn'],
      [{ expiresIn: 2147483648 }, 'expiresIn'],
      [{ expiresIn: Infinity }, 'expiresIn'],
      [{ expiresIn: '300' }, 'expiresIn'],
      [{ expiresIn: null }, 'expiresIn'],
      [{ notBefore: -1 }, 'notBefore'],
      [{ notBefore: 0.5 }, 'notBefore'],
      [{ notBefore: 2147483648 }, 'notBefore'],
      [{ notBefore: NaN }, 'notBefore'],
      [{ notBefore: '60' }, 'notBefore'],
      [{ jti: 'true' }, 'jti'],
      [{ jti: 1 }, 'jti'],
      [{ jti: null }, 'jti'],
      [{ keyId: 5 }, 'keyId'],
      [{ keyId: null }, 'keyId'],
      [{ keyId: 'a\0b' }, 'keyId'],
      [{ passphrase: 5 }, 'passphrase'],
      [{ passphrase: null }, 'passphrase'],
    ])('signer option %p is validated before signing', (options, name) => {
      expect(() => signAssertion({ privateKey: keys.rsa.privateKey, ...options })).toThrow(
        `Invalid OAuth bearer helper option: ${name}`
      )
    })

    test.each([[undefined], [null], ['options'], [[]]])(
      'signer options %p are refused',
      (options) => {
        expect(() => signAssertion(options)).toThrow('Invalid OAuth bearer helper option: options')
      }
    )

    const accessor = Object.defineProperty({}, 'value', { enumerable: true, get: () => 'x' })
    // eslint-disable-next-line accessor-pairs -- A setter-only claim is the case under test.
    const setter = Object.defineProperty({}, 'value', { enumerable: true, set: () => {} })
    const cycle = {}
    cycle.self = cycle
    const sparse = []
    sparse[1] = 'x'
    const tagged = Object.assign(['x'], { extra: 'y' })
    test.each([
      ['iat', { iat: 1 }],
      ['nbf', { nbf: 1 }],
      ['exp', { exp: undefined }],
      ['jti', { jti: 'id' }],
      ['a cycle', { nested: cycle }],
      ['a BigInt', { value: BigInt(1) }],
      ['NaN', { value: NaN }],
      ['Infinity', { value: [Infinity] }],
      ['a function', { value: () => 'x' }],
      ['a toJSON function', { value: { toJSON: () => 'x' } }],
      ['a symbol', { value: Symbol('x') }],
      ['an undefined member', { value: undefined }],
      ['a getter', { nested: accessor }],
      ['a setter', { nested: setter }],
      ['a date', { value: new Date() }],
      ['a class instance', { value: new (class Claim {})() }],
      ['a sparse array', { value: sparse }],
      ['an array with extra members', { value: tagged }],
      ['a numeric issuer', { iss: 5 }],
      ['a null subject', { sub: null }],
      ['an empty audience list', { aud: [] }],
      ['a duplicate audience list', { aud: ['a', 'a'] }],
      ['a numeric audience', { aud: 5 }],
      ['a numeric audience member', { aud: ['a', 5] }],
      ['an array root', ['x']],
      ['a null root', null],
      ['a date root', new Date()],
    ])('claims with %s reject before work', (_, claims) => {
      expect(() =>
        signAssertion({ privateKey: { file: path.join(dir, 'absent.pem') }, claims })
      ).toThrow('Invalid OAuth bearer helper option: claims')
    })

    test('audiences preserve valid strings and repeated subobjects are accepted', async () => {
      const shared = { region: 'eu' }
      const audiences = ['service', 'endpoint']
      const claims = {
        aud: audiences,
        iss: '',
        sub: 'client',
        first: shared,
        second: shared,
        list: [shared, null, true, 1.5],
      }
      const supplier = signAssertion({ privateKey: keys.rsa.privateKey, claims })
      expect(decode((await supplier()).split('.')[1])).toMatchObject({
        aud: ['service', 'endpoint'],
        iss: '',
        first: { region: 'eu' },
        second: { region: 'eu' },
        list: [{ region: 'eu' }, null, true, 1.5],
      })
      expect(audiences).toEqual(['service', 'endpoint'])
      const single = signAssertion({ privateKey: keys.rsa.privateKey, claims: { aud: 'service' } })
      expect(decode((await single()).split('.')[1]).aud).toBe('service')
      const bare = signAssertion({ privateKey: keys.rsa.privateKey, claims: Object.create(null) })
      expect(decode((await bare()).split('.')[1])).toEqual({
        iat: NOW / 1000,
        nbf: NOW / 1000 - 60,
        exp: NOW / 1000 + 300,
      })
    })

    test('caller claim mutation does not alter later assertions', async () => {
      const claims = { aud: ['service'], nested: { region: 'eu' } }
      const supplier = signAssertion({ privateKey: keys.rsa.privateKey, claims })
      claims.aud.push('other')
      claims.nested.region = 'us'
      claims.added = 'x'
      expect(decode((await supplier()).split('.')[1])).toMatchObject({
        aud: ['service'],
        nested: { region: 'eu' },
      })
      expect(decode((await supplier()).split('.')[1])).not.toHaveProperty('added')
    })

    test('file key rotation changes the verifying key', async () => {
      const file = keyFile(keys.rsa.privateKey)
      touch(file, 0)
      const supplier = signAssertion({ privateKey: { file } })
      const first = await supplier()
      fs.writeFileSync(file, pem(keys.rsaOther.privateKey))
      touch(file, 10)
      const second = await supplier()
      expect(verifies(first, keys.rsa.publicKey, 'sha256')).toBe(true)
      expect(verifies(second, keys.rsaOther.publicKey, 'sha256')).toBe(true)
      expect(verifies(second, keys.rsa.publicKey, 'sha256')).toBe(false)
    })

    test('unchanged key file avoids rereading material', async () => {
      const file = keyFile(keys.rsa.privateKey)
      const stat = jest.spyOn(fs.promises, 'stat')
      const read = jest.spyOn(fs.promises, 'readFile')
      const supplier = signAssertion({ privateKey: { file }, jti: true })
      const assertions = [await supplier(), await supplier(), await supplier()]
      expect(stat).toHaveBeenCalledTimes(3)
      expect(read).not.toHaveBeenCalled()
      expect(new Set(assertions).size).toBe(3)
    })

    test('failed key rotation cannot continue using the old key', async () => {
      const file = keyFile(keys.rsa.privateKey)
      touch(file, 0)
      const supplier = signAssertion({ privateKey: { file }, algorithm: 'RS256' })
      await supplier()
      fs.writeFileSync(file, `not a key ${MARKER}`)
      touch(file, 10)
      const bad = await supplier().catch((caught) => caught)
      expect(bad).toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(bad.message).toBe(
        'SASL OAUTHBEARER helper: key: the signing key file could not be loaded'
      )
      expect(textOf(bad)).not.toContain(MARKER)
      expect(textOf(bad)).not.toContain(dir)
      await expect(supplier()).rejects.toThrow('key')
      fs.writeFileSync(file, pem(keys.p256.privateKey))
      touch(file, 20)
      await expect(supplier()).rejects.toThrow('key')
      fs.writeFileSync(file, pem(keys.rsaThird.privateKey))
      touch(file, 20)
      expect(verifies(await supplier(), keys.rsaThird.publicKey, 'sha256')).toBe(true)
    })

    test('a missing rotated key file fails without a path or cause', async () => {
      const file = keyFile(keys.rsa.privateKey)
      const supplier = signAssertion({ privateKey: { file } })
      fs.rmSync(file)
      const error = await supplier().catch((caught) => caught)
      expect(error.message).toContain('key')
      expect(error.cause).toBeUndefined()
      expect(textOf(error)).not.toContain(dir)
    })

    test('a change during a key read is seen at the next call', async () => {
      const file = keyFile(keys.rsa.privateKey)
      touch(file, 0)
      const supplier = signAssertion({ privateKey: { file } })
      fs.writeFileSync(file, pem(keys.rsaOther.privateKey))
      touch(file, 10)
      const original = fs.promises.readFile
      const read = jest.spyOn(fs.promises, 'readFile').mockImplementationOnce(async (...args) => {
        const material = await original(...args)
        fs.writeFileSync(file, pem(keys.rsaThird.privateKey))
        touch(file, 20)
        return material
      })
      expect(verifies(await supplier(), keys.rsaOther.publicKey, 'sha256')).toBe(true)
      expect(verifies(await supplier(), keys.rsaThird.publicKey, 'sha256')).toBe(true)
      await supplier()
      expect(read).toHaveBeenCalledTimes(2)
    })

    test('a hung rotated key cannot escape retrieval close or deadline', async () => {
      const file = keyFile(keys.rsa.privateKey)
      touch(file, 0)
      const assertion = signAssertion({ privateKey: { file } })
      fs.writeFileSync(file, pem(keys.rsaOther.privateKey))
      touch(file, 10)
      const held = deferred()
      let reads = 0
      const reading = deferred()
      jest.spyOn(fs.promises, 'readFile').mockImplementation(() => {
        if (++reads === 2) reading.resolve()
        return held.promise
      })
      const fetch = jest.fn(async () => json({ access_token: 'opaque-token' }))
      const closing = provider({ assertion, fetch })
      const closed = observe(closing())
      const expiring = observe(provider({ assertion, fetch })())
      await reading.promise
      closing.close()
      await jest.advanceTimersByTimeAsync(8000)
      expect(closed.error.message).toContain('closed')
      expect(expiring.error.message).toContain('deadline')
      held.resolve(Buffer.from(pem(keys.rsaOther.privateKey)))
      await jest.advanceTimersByTimeAsync(0)
      expect(fetch).not.toHaveBeenCalled()
    })

    test('a signed assertion composes with the JWT bearer grant', async () => {
      const fetch = jest.fn(async () => json({ access_token: 'opaque-token' }))
      const assertion = signAssertion({
        privateKey: keys.p256.privateKey,
        algorithm: 'ES256',
        claims: { iss: 'client' },
      })
      await provider({ assertion, fetch })()
      const sent = new URLSearchParams(fetch.mock.calls[0][1].body).get('assertion')
      expect(verifies(sent, keys.p256.publicKey, 'sha256', true)).toBe(true)
    })
  })
})
