const crypto = require('node:crypto')
const fs = require('node:fs')
const http = require('node:http')
const os = require('node:os')
const path = require('node:path')
const oauthBearer = require('..')
const { KafkaJSSASLAuthenticationError } = require('../../errors')

const MARKER = 'inert-marker'
const SECRET = 'inert-secret'
const RECIPES = [
  'authenticateWithHelper',
  'createSecretProvider',
  'createAssertionProvider',
  'createGrantProvider',
  'createFileProvider',
  'createLocalAssertion',
  'createRequestJson',
  'discoverProvider',
  'refreshToken',
  'exchangeToken',
  'mutualTlsToken',
  'createKmsProvider',
  'createTransportProvider',
]
const SECTIONS = [
  'OAuth bearer authentication',
  'Providers and lifecycle',
  'Options',
  'Client credentials',
  'Client assertion',
  'JWT bearer grant',
  'File tokens',
  'Local assertions',
  'Discovery',
  'Refresh tokens',
  'Token exchange',
  'Mutual TLS',
  'Remote signing',
  'Proxies and private certificate authorities',
  'Failures and budgets',
]
const segment = (value) => Buffer.from(JSON.stringify(value)).toString('base64url')
const TOKEN = `${segment({ alg: 'none' })}.${segment({ sub: 'docs', exp: Math.floor(Date.now() / 1000) + 3600 })}.`
const ASSERTION_TYPE = 'urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer'

/**
 * Compiles the reviewed `js` fences of the local guide. Each fence starts with one function
 * definition; the recipes test checks that every recipe is defined exactly once.
 */
const compileRecipes = (markdown) => {
  const fences = [...markdown.matchAll(/^```js\n([\s\S]*?)^```$/gm)].map((match) => match[1])
  const names = fences.map((fence) => /^(?:async )?function (\w+)\(/.exec(fence)?.[1])
  const defined = RECIPES.filter((name) => names.includes(name))
  // eslint-disable-next-line no-new-func -- Execute reviewed local documentation fences only.
  const recipes = new Function(`${fences.join('\n')}\nreturn { ${defined.join(', ')} }`)()
  return { names, recipes }
}

describe('OAuth documentation recipes', () => {
  let markdown
  let names
  let recipes
  let server
  let local
  let ledger
  let answer
  let dir
  const providers = []

  const tokenAnswer = (request, response) => {
    response.writeHead(200, { 'Content-Type': 'application/json' })
    response.end(
      JSON.stringify(
        request.method === 'GET'
          ? { issuer: 'https://issuer.example', token_endpoint: 'https://issuer.example/token' }
          : { access_token: TOKEN, expires_in: 3600, token_type: 'Bearer' }
      )
    )
  }
  const fetchLocal = jest.fn((url, init) => {
    const { pathname, search } = new URL(url)
    return fetch(`${local}${pathname}${search}`, init)
  })
  const retain = (provider) => {
    providers.push(provider)
    return provider
  }

  beforeAll(async () => {
    markdown = fs.readFileSync(path.join(__dirname, '../../../docs/OAuth.md'), 'utf8')
    const compiled = compileRecipes(markdown)
    names = compiled.names
    recipes = compiled.recipes
    server = http.createServer(async (request, response) => {
      const chunks = []
      for await (const chunk of request) chunks.push(chunk)
      ledger.push({
        method: request.method,
        url: request.url,
        headers: request.headers,
        body: Buffer.concat(chunks).toString(),
      })
      answer(request, response)
    })
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
    local = `http://127.0.0.1:${server.address().port}`
  })

  beforeEach(() => {
    ledger = []
    answer = tokenAnswer
    fetchLocal.mockClear()
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'oauth-docs-'))
  })

  afterEach(() => {
    providers.splice(0).forEach((provider) => provider.close())
    jest.useRealTimers()
    fs.rmSync(dir, { recursive: true, force: true })
  })

  afterAll(async () => {
    server.closeIdleConnections()
    await new Promise((resolve) => server.close(resolve))
  })

  test('each recipe is defined by exactly one local fence', () => {
    expect([...names].sort()).toEqual([...RECIPES].sort())
  })

  test('the guide has its sections in order and no machine-specific path or local-only file', () => {
    const headings = [...markdown.matchAll(/^## (.+)$/gm)].map((match) => match[1])
    expect(headings).toEqual(SECTIONS)
    expect(markdown).toMatch(/^---\nid: oauth\ntitle: OAuth Helpers and Recipes\n---\n/)
    expect(markdown).not.toMatch(
      /\/Users\/|\/home\/|\/var\/tmp|new_countly|~\/|C:\\|superpowers|upgrade-ref|CLAUDE\.md|AGENT\.md|EXECUTION\.md|\b(?:Phase|Task|Wave) \d/
    )
    expect(/\p{Cc}/u.test(markdown.replace(/[\t\n\r]/g, ''))).toBe(false)
  })

  test('the guide states the lifecycle, the budget and the failure contract', () => {
    for (const sentence of [
      'Closing a Kafka client does not close a shared provider.',
      'Reading an expiry is not token validation.',
      'The default helper retrieval budget is 8000 ms.',
      'A token callback slower than `authenticationTimeout` needs a larger value; the helpers refresh ahead of time.',
      'An arbitrary application promise cannot be physically canceled by the library.',
      'They omit credentials, assertions, access tokens, response bodies and raw causes.',
      'The library does not wrap that callback merely because helpers are available.',
    ]) {
      expect(markdown).toContain(sentence)
    }
  })

  describe('the JSON request adapter', () => {
    test('requests over HTTPS and returns the parsed object', async () => {
      const requestJson = recipes.createRequestJson({ transport: fetchLocal })
      expect(await requestJson('https://issuer.example/.well-known/openid-configuration')).toEqual({
        issuer: 'https://issuer.example',
        token_endpoint: 'https://issuer.example/token',
      })
      expect(fetchLocal.mock.calls[0][1]).toMatchObject({ redirect: 'manual' })
      expect(fetchLocal.mock.calls[0][1].signal).toBeInstanceOf(AbortSignal)
    })

    test.each([true, false])(
      'aborts owned transport after completion (success=%s)',
      async (success) => {
        let signal
        const requestJson = recipes.createRequestJson({
          transport: async (_url, init) => {
            signal = init.signal
            if (!success) throw new Error(MARKER)
            return new Response('{}', { status: 200 })
          },
        })
        if (success) await expect(requestJson('https://issuer.example/token')).resolves.toEqual({})
        else
          await expect(requestJson('https://issuer.example/token')).rejects.toThrow(
            /^Token request failed$/
          )
        expect(signal.aborted).toBe(true)
      }
    )

    test('refuses plain HTTP unless a test loopback endpoint is allowed', async () => {
      await expect(recipes.createRequestJson()(`${local}/token`)).rejects.toThrow(
        'Refused token endpoint'
      )
      await expect(recipes.createRequestJson()('http://issuer.example/token')).rejects.toThrow(
        'Refused token endpoint'
      )
      const testOnly = recipes.createRequestJson({ allowInsecure: true })
      await expect(testOnly('http://issuer.example/token')).rejects.toThrow(
        'Refused token endpoint'
      )
      expect(await testOnly(`${local}/token`, { method: 'POST' })).toMatchObject({
        access_token: TOKEN,
      })
      expect(ledger).toHaveLength(1)
    })

    test('refuses redirects without following them', async () => {
      answer = (_request, response) => {
        response.writeHead(302, { Location: `${local}/elsewhere` })
        response.end()
      }
      const requestJson = recipes.createRequestJson({ allowInsecure: true })
      await expect(requestJson(`${local}/token`)).rejects.toThrow(/^Token request failed$/)
      expect(ledger.map((request) => request.url)).toEqual(['/token'])
    })

    test('copies chunks before the transport reuses their backing buffer', async () => {
      const expected = { access_token: TOKEN, expires_in: 3600, token_type: 'Bearer' }
      const payload = Buffer.from(JSON.stringify(expected))
      const backing = Buffer.alloc(13)
      const requestJson = recipes.createRequestJson({
        transport: async () => ({
          status: 200,
          body: (async function* () {
            for (let offset = 0; offset < payload.length; offset += backing.length) {
              const size = Math.min(backing.length, payload.length - offset)
              payload.copy(backing, 0, offset, offset + size)
              yield backing.subarray(0, size)
            }
            backing.fill(0)
          })(),
        }),
      })
      await expect(requestJson('https://issuer.example/token')).resolves.toEqual(expected)
    })

    test('many empty chunks do not grow retained storage and still parse', async () => {
      const empty = new Uint8Array(0)
      const expected = { access_token: TOKEN, token_type: 'Bearer' }
      const payload = Buffer.from(JSON.stringify(expected))
      const requestJson = recipes.createRequestJson({
        transport: async () => ({
          status: 200,
          body: (async function* () {
            yield payload.subarray(0, 1)
            for (let index = 0; index < 4096; index++) yield empty
            yield payload.subarray(1)
          })(),
        }),
      })
      let retainedLength = 0
      const push = Array.prototype.push
      const storage = jest.spyOn(Array.prototype, 'push').mockImplementation(function (...items) {
        const length = push.apply(this, items)
        if (items.includes(empty)) retainedLength = Math.max(retainedLength, length)
        return length
      })
      let result
      try {
        result = await requestJson('https://issuer.example/token')
      } finally {
        storage.mockRestore()
      }
      expect(result).toEqual(expected)
      expect(retainedLength).toBeLessThanOrEqual(1)
    })

    test('bounds the body at one MiB and fails with fixed text', async () => {
      answer = (_request, response) => {
        response.writeHead(200, { 'Content-Type': 'application/json' })
        response.end(`"${'x'.repeat(1048576)}${MARKER}"`)
      }
      const error = await recipes
        .createRequestJson({ allowInsecure: true })(`${local}/token`)
        .catch((caught) => caught)
      expect(error.message).toBe('Token request failed')
      expect(error.cause).toBeUndefined()
    })

    test('caps headers and body with one timeout', async () => {
      answer = (_request, response) => {
        response.writeHead(200, { 'Content-Type': 'application/json' })
        response.write('{"access_token":')
      }
      const started = Date.now()
      const error = await recipes
        .createRequestJson({ allowInsecure: true, timeout: 100 })(`${local}/token`)
        .catch((caught) => caught)
      expect(error.message).toBe('Token request failed')
      expect(Date.now() - started).toBeLessThan(5000)
    })

    test.each([
      ['a refusal', 401, JSON.stringify({ error: MARKER })],
      ['text that is not JSON', 200, `not json ${MARKER}`],
      ['a JSON array', 200, JSON.stringify([MARKER])],
      ['a JSON null', 200, 'null'],
    ])('never repeats %s', async (_, status, body) => {
      answer = (_request, response) => {
        response.writeHead(status)
        response.end(body)
      }
      const error = await recipes
        .createRequestJson({ allowInsecure: true })(`${local}/token`)
        .catch((caught) => caught)
      expect(error.message).toBe('Token request failed')
    })
  })

  test('helper lifecycle example authenticates and closes', async () => {
    const events = []
    let created
    const shellBearer = {
      ...oauthBearer,
      clientCredentials: (options) => {
        created = oauthBearer.clientCredentials(options)
        const close = created.close
        created.close = () => {
          events.push('close')
          close()
        }
        return created
      },
    }
    const shell = (topics) =>
      class Kafka {
        constructor({ sasl }) {
          this.provider = sasl.oauthBearerProvider
        }

        admin() {
          return {
            connect: async () => {
              events.push(`connect ${(await this.provider()).value === TOKEN}`)
            },
            listTopics: async () => topics(),
            disconnect: async () => {
              events.push('disconnect')
            },
          }
        }
      }
    const options = {
      oauthBearer: shellBearer,
      brokers: ['broker:9092'],
      tokenEndpointUrl: 'https://issuer.example/token',
      clientId: 'client',
      clientSecret: SECRET,
      fetch: fetchLocal,
    }
    expect(
      await recipes.authenticateWithHelper({ ...options, Kafka: shell(() => ['topic-a']) })
    ).toEqual(['topic-a'])
    expect(events).toEqual(['connect true', 'disconnect', 'close'])
    expect(ledger).toHaveLength(1)
    await expect(created()).rejects.toThrow('closed')
    events.length = 0
    const failing = shell(() => {
      throw new Error('listing failed')
    })
    await expect(recipes.authenticateWithHelper({ ...options, Kafka: failing })).rejects.toThrow(
      'listing failed'
    )
    expect(events).toEqual(['connect true', 'disconnect', 'close'])
  })

  test('secret recipe sends the declared request', async () => {
    const provider = retain(
      recipes.createSecretProvider({
        oauthBearer,
        tokenEndpointUrl: `${local}/token`,
        clientId: 'client',
        clientSecret: SECRET,
        scope: 'read',
        allowInsecure: true,
      })
    )
    expect(await provider()).toEqual({ value: TOKEN })
    expect(ledger).toHaveLength(1)
    expect(ledger[0].headers.authorization).toBe(
      `Basic ${Buffer.from(`client:${SECRET}`).toString('base64')}`
    )
    expect(ledger[0].body).toBe('grant_type=client_credentials&scope=read')
  })

  test('assertion recipe sends the declared request', async () => {
    const clientAssertion = jest.fn(() => 'header.payload.signature')
    const provider = retain(
      recipes.createAssertionProvider({
        oauthBearer,
        tokenEndpointUrl: 'https://issuer.example/token',
        clientId: 'client',
        clientAssertion,
        fetch: fetchLocal,
      })
    )
    expect(await provider()).toEqual({ value: TOKEN })
    expect(ledger[0].headers.authorization).toBeUndefined()
    expect(ledger[0].body).toBe(
      `grant_type=client_credentials&client_assertion_type=${ASSERTION_TYPE}&client_assertion=header.payload.signature&client_id=client`
    )
    expect(clientAssertion).toHaveBeenCalledTimes(1)
  })

  test('grant recipe uses the JWT grant URI', async () => {
    const provider = retain(
      recipes.createGrantProvider({
        oauthBearer,
        tokenEndpointUrl: 'https://issuer.example/token',
        assertion: () => 'header.payload.signature',
        scope: 'read',
        fetch: fetchLocal,
      })
    )
    expect(await provider()).toEqual({ value: TOKEN })
    expect(ledger.map((request) => request.body)).toEqual([
      'grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=header.payload.signature&scope=read',
    ])
  })

  test('file recipe rereads rotation', async () => {
    const file = path.join(dir, 'access.jwt')
    const second = `${segment({ alg: 'none' })}.${segment({ sub: 'second', exp: Math.floor(Date.now() / 1000) + 3600 })}.`
    fs.writeFileSync(file, TOKEN)
    fs.utimesSync(file, 1, 1)
    const provider = retain(recipes.createFileProvider({ oauthBearer, path: file }))
    expect((await provider()).value).toBe(TOKEN)
    fs.writeFileSync(file, second)
    fs.utimesSync(file, 2, 2)
    expect((await provider()).value).toBe(second)
    expect(ledger).toHaveLength(0)
  })

  test('local assertion recipe verifies with the independent public key', async () => {
    const { privateKey, publicKey } = crypto.generateKeyPairSync('rsa', { modulusLength: 2048 })
    const supplier = recipes.createLocalAssertion({
      oauthBearer,
      privateKey,
      clientId: 'client',
      tokenEndpointUrl: 'https://issuer.example/token',
    })
    const [header, claims, signature] = (await supplier()).split('.')
    expect(
      crypto.verify(
        'sha256',
        Buffer.from(`${header}.${claims}`),
        publicKey,
        Buffer.from(signature, 'base64url')
      )
    ).toBe(true)
    const decoded = JSON.parse(Buffer.from(claims, 'base64url').toString('utf8'))
    expect(JSON.parse(Buffer.from(header, 'base64url').toString('utf8'))).toEqual({
      alg: 'RS256',
      typ: 'JWT',
    })
    expect(decoded).toMatchObject({
      iss: 'client',
      sub: 'client',
      aud: 'https://issuer.example/token',
    })
    expect(decoded.exp - decoded.iat).toBe(300)
    expect(decoded.iat - decoded.nbf).toBe(60)
    expect(decoded.jti).toMatch(/^[0-9a-f-]{36}$/)
    expect(ledger).toHaveLength(0)
  })

  test('discovery recipe validates the issuer before token retrieval', async () => {
    const requestJson = recipes.createRequestJson({ transport: fetchLocal })
    const discovered = {
      oauthBearer,
      requestJson,
      issuer: 'https://issuer.example',
      clientId: 'client',
      clientSecret: SECRET,
      scope: 'read',
      fetch: fetchLocal,
    }
    const provider = retain(await recipes.discoverProvider(discovered))
    expect(await provider()).toEqual({ value: TOKEN })
    expect(ledger.map(({ method, url }) => `${method} ${url}`)).toEqual([
      'GET /.well-known/openid-configuration',
      'POST /token',
    ])
    const tenant = jest.fn(async () => ({
      issuer: 'https://issuer.example/tenant/',
      token_endpoint: 'https://login.example/token',
    }))
    retain(
      await recipes.discoverProvider({
        ...discovered,
        requestJson: tenant,
        issuer: 'https://issuer.example/tenant/',
      })
    )
    expect(tenant.mock.calls[0][0]).toBe(
      'https://issuer.example/tenant/.well-known/openid-configuration'
    )
    for (const [metadata, message] of [
      [
        { issuer: 'https://other.example', token_endpoint: 'https://issuer.example/token' },
        'Invalid discovery metadata',
      ],
      [{ issuer: 'https://issuer.example' }, 'Invalid discovery metadata'],
      [
        { issuer: 'https://issuer.example', token_endpoint: 'http://issuer.example/token' },
        'Invalid discovered endpoint',
      ],
      [
        {
          issuer: 'https://issuer.example',
          token_endpoint: 'https://user:pw@issuer.example/token',
        },
        'Invalid discovered endpoint',
      ],
      [
        { issuer: 'https://issuer.example', token_endpoint: 'https://issuer.example/token#x' },
        'Invalid discovered endpoint',
      ],
    ]) {
      await expect(
        recipes.discoverProvider({ ...discovered, requestJson: async () => metadata })
      ).rejects.toThrow(message)
    }
    await expect(
      recipes.discoverProvider({ ...discovered, issuer: 'http://issuer.example' })
    ).rejects.toThrow('Invalid issuer')
    answer = (_request, response) => {
      response.writeHead(500)
      response.end(MARKER)
    }
    await expect(recipes.discoverProvider(discovered)).rejects.toThrow(/^Token request failed$/)
    expect(ledger.filter(({ method }) => method === 'POST')).toHaveLength(1)
  })

  test.each([
    [
      'refreshToken',
      { refreshToken: `${MARKER}-refresh` },
      `grant_type=refresh_token&refresh_token=${MARKER}-refresh`,
    ],
    [
      'exchangeToken',
      { subjectToken: `${MARKER}-subject`, audience: 'kafka' },
      `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange&subject_token=${MARKER}-subject&subject_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token&audience=kafka`,
    ],
  ])(
    '%s recipe delegates client authentication and sends its grant',
    async (name, inputs, body) => {
      const authenticateRequest = jest.fn((init) => ({
        ...init,
        headers: { ...init.headers, Authorization: 'Basic Y2xpZW50' },
      }))
      const requestJson = recipes.createRequestJson({ transport: fetchLocal })
      const options = {
        requestJson,
        tokenEndpointUrl: 'https://issuer.example/token',
        authenticateRequest,
        ...inputs,
      }
      expect(await recipes[name](options)).toEqual({ value: TOKEN })
      expect(authenticateRequest).toHaveBeenCalledTimes(1)
      expect(ledger[0].body).toBe(body)
      expect(ledger[0].headers.authorization).toBe('Basic Y2xpZW50')
      answer = (_request, response) => {
        response.writeHead(200, { 'Content-Type': 'application/json' })
        response.end(JSON.stringify({ error: MARKER }))
      }
      const error = await recipes[name](options).catch((caught) => caught)
      expect(error.message).toBe('Invalid token response')
    }
  )

  test.each([
    ['with', 'read', 'grant_type=client_credentials&client_id=client&scope=read'],
    ['without', undefined, 'grant_type=client_credentials&client_id=client'],
  ])('mutual TLS recipe uses the configured transport %s a scope', async (_, scope, body) => {
    const mutualTlsFetch = jest.fn(fetchLocal)
    const requestJson = recipes.createRequestJson()
    const result = await recipes.mutualTlsToken({
      requestJson,
      mutualTlsFetch,
      tokenEndpointUrl: 'https://issuer.example/token',
      clientId: 'client',
      scope,
    })
    expect(result).toEqual({ value: TOKEN })
    expect(mutualTlsFetch).toHaveBeenCalledTimes(1)
    expect(mutualTlsFetch.mock.calls[0][1].signal).toBeInstanceOf(AbortSignal)
    expect(ledger[0].body).toBe(body)
    expect(ledger[0].headers.authorization).toBeUndefined()
    answer = (_request, response) => {
      response.writeHead(200, { 'Content-Type': 'application/json' })
      response.end('{}')
    }
    await expect(
      recipes.mutualTlsToken({
        requestJson,
        mutualTlsFetch,
        tokenEndpointUrl: 'https://issuer.example/token',
        clientId: 'client',
      })
    ).rejects.toThrow('Invalid token response')
  })

  test('remote signing recipe stays inside the helper deadline', async () => {
    const { privateKey, publicKey } = crypto.generateKeyPairSync('ec', { namedCurve: 'prime256v1' })
    const signLocally = oauthBearer.signAssertion({ privateKey, algorithm: 'ES256' })
    const signRemotely = jest.fn(async (claims) => {
      expect(claims).toEqual({
        issuer: 'client',
        subject: 'client',
        audience: 'https://issuer.example/token',
      })
      return signLocally()
    })
    const provider = retain(
      recipes.createKmsProvider({
        oauthBearer,
        tokenEndpointUrl: 'https://issuer.example/token',
        clientId: 'client',
        signRemotely,
        fetch: fetchLocal,
      })
    )
    expect(await provider()).toEqual({ value: TOKEN })
    const assertion = new URLSearchParams(ledger[0].body).get('client_assertion')
    const [header, claims, signature] = assertion.split('.')
    expect(
      crypto.verify(
        'sha256',
        Buffer.from(`${header}.${claims}`),
        { key: publicKey, dsaEncoding: 'ieee-p1363' },
        Buffer.from(signature, 'base64url')
      )
    ).toBe(true)
    expect(ledger).toHaveLength(1)
    jest.useFakeTimers()
    let release
    const hung = retain(
      recipes.createKmsProvider({
        oauthBearer,
        tokenEndpointUrl: 'https://issuer.example/token',
        clientId: 'client',
        signRemotely: () =>
          new Promise((resolve) => {
            release = resolve
          }),
        fetch: fetchLocal,
      })
    )
    const settled = hung().catch((error) => error)
    await jest.advanceTimersByTimeAsync(7999)
    expect(fetchLocal).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(1)
    expect((await settled).message).toContain('deadline')
    release(assertion)
    await jest.advanceTimersByTimeAsync(0)
    expect(fetchLocal).toHaveBeenCalledTimes(1)
  })

  test('configured transport receives redirect and cancellation settings', async () => {
    const configuredFetch = jest.fn(fetchLocal)
    const options = {
      oauthBearer,
      configuredFetch,
      tokenEndpointUrl: 'https://issuer.example/token',
      clientId: 'client',
      clientSecret: SECRET,
    }
    const provider = retain(recipes.createTransportProvider(options))
    expect(await provider()).toEqual({ value: TOKEN })
    expect(configuredFetch).toHaveBeenCalledTimes(1)
    expect(configuredFetch.mock.calls[0][1]).toMatchObject({ redirect: 'manual', method: 'POST' })
    let signal
    const stalled = retain(
      recipes.createTransportProvider({
        ...options,
        configuredFetch: (_url, init) => {
          signal = init.signal
          return new Promise(() => {})
        },
      })
    )
    const call = stalled().catch((error) => error)
    await new Promise((resolve) => setImmediate(resolve))
    stalled.close()
    expect(signal.aborted).toBe(true)
    expect(await call).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    const failing = retain(
      recipes.createTransportProvider({
        ...options,
        configuredFetch: async () => Promise.reject(new Error(MARKER)),
        clientSecret: SECRET,
      })
    )
    jest.useFakeTimers()
    const rejected = failing().catch((error) => error)
    await jest.advanceTimersByTimeAsync(8000)
    const error = await rejected
    expect(error.message).toContain('transport')
    expect(error.message).not.toContain(MARKER)
  })
})
