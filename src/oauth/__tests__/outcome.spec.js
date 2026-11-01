const crypto = require('node:crypto')
const fs = require('node:fs')
const http = require('node:http')
const os = require('node:os')
const path = require('node:path')
const { setTimeout: delay } = require('node:timers/promises')
const { Kafka, logLevel, oauthBearer, KafkaJSSASLAuthenticationError } = require('../../..')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')

const MARKER = 'inert-marker'
const SECRET = 'inert-secret'
const segment = (value) => Buffer.from(JSON.stringify(value)).toString('base64url')
const unsecured = (sub, lifetimeSeconds = 3600) =>
  `${segment({ alg: 'none', typ: 'JWT' })}.${segment({ sub, exp: Math.floor(Date.now() / 1000) + lifetimeSeconds })}.`
const saslBytes = (token) => Buffer.from(`n,,\x01auth=Bearer ${token}\x01\x01`)
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

describe('OAuth authentication outcome', () => {
  let cluster
  let admin
  let provider
  let issuer
  let dir
  let logs

  /** An in-process token endpoint on 127.0.0.1 that answers from `issuer.answer`. */
  const startIssuer = async (answer) => {
    const started = { requests: [], answer, sockets: new Set() }
    started.server = http.createServer(async (request, response) => {
      const chunks = []
      for await (const chunk of request) chunks.push(chunk)
      started.requests.push({ headers: request.headers, body: Buffer.concat(chunks).toString() })
      started.answer(started.requests.at(-1), response)
    })
    started.server.on('connection', (socket) => {
      started.sockets.add(socket)
      socket.once('close', () => started.sockets.delete(socket))
    })
    await new Promise((resolve) => started.server.listen(0, '127.0.0.1', resolve))
    started.url = `http://127.0.0.1:${started.server.address().port}/token`
    issuer = started
    return started
  }
  const answerWith =
    (token, extra = {}) =>
    (_request, response) => {
      response.writeHead(200, { 'Content-Type': 'application/json' })
      response.end(JSON.stringify({ access_token: token, token_type: 'Bearer', ...extra }))
    }

  const connect = async (created, overrides = {}) => {
    provider = created
    cluster = createScriptedCluster({
      nodes: [{ id: 0, host: 'broker', port: 9092 }],
      profile: 'sasl',
    })
      .answer('SaslHandshake', () => ({ enabledMechanisms: ['OAUTHBEARER'] }))
      .answer('SaslAuthenticate', () => ({
        authBytes: Buffer.alloc(0),
        sessionLifetimeMs: '15000',
      }))
    admin = new Kafka(
      cluster.kafkaOptions({
        sasl: { mechanism: 'oauthbearer', oauthBearerProvider: created },
        retry: { retries: 0 },
        logLevel: logLevel.DEBUG,
        logCreator: () => (entry) => logs.push(entry),
        ...overrides,
      })
    ).admin()
    await admin.connect()
  }
  const sent = (api) => cluster.requests().filter((request) => request.api === api)

  beforeEach(() => {
    logs = []
  })

  afterEach(async () => {
    await admin?.disconnect()
    provider?.close?.()
    cluster?.assertClean()
    if (issuer) {
      const closed = [...issuer.sockets].map(
        (socket) => new Promise((resolve) => socket.once('close', resolve))
      )
      await new Promise((resolve) => issuer.server.close(resolve))
      await Promise.all(closed)
    }
    if (dir) fs.rmSync(dir, { recursive: true, force: true })
    cluster = admin = provider = issuer = dir = undefined
  })

  test('secret helper crosses the real SASL boundary', async () => {
    const token = unsecured('secret-form')
    const { url, requests } = await startIssuer(answerWith(token))
    await connect(
      oauthBearer.clientCredentials({
        tokenEndpointUrl: url,
        clientId: 'client',
        clientSecret: SECRET,
        allowInsecure: true,
      })
    )
    expect(sent('SaslHandshake').map((request) => request.fields)).toEqual([
      { mechanism: 'OAUTHBEARER' },
    ])
    expect(sent('SaslAuthenticate').map((request) => request.fields.authBytes)).toEqual([
      saslBytes(token),
    ])
    expect(requests).toHaveLength(1)
    expect(requests[0].headers.authorization).toBe(
      `Basic ${Buffer.from(`client:${SECRET}`).toString('base64')}`
    )
    expect(requests[0].body).toBe('grant_type=client_credentials')
  })

  test('assertion helper crosses the real SASL boundary', async () => {
    const token = unsecured('assertion-form')
    const { url, requests } = await startIssuer(answerWith(token))
    const clientAssertion = jest.fn(() => 'header.payload.signature')
    await connect(
      oauthBearer.clientCredentials({
        tokenEndpointUrl: url,
        clientId: 'client',
        clientAssertion,
        allowInsecure: true,
      })
    )
    expect(sent('SaslAuthenticate').map((request) => request.fields.authBytes)).toEqual([
      saslBytes(token),
    ])
    expect(requests).toHaveLength(1)
    expect(requests[0].headers.authorization).toBeUndefined()
    expect(requests[0].body).toBe(
      'grant_type=client_credentials&client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer&client_assertion=header.payload.signature&client_id=client'
    )
    expect(clientAssertion).toHaveBeenCalledTimes(1)
  })

  test('JWT bearer helper crosses the real SASL boundary', async () => {
    const token = unsecured('jwt-form')
    const { url, requests } = await startIssuer(answerWith(token))
    await connect(
      oauthBearer.jwtBearer({
        tokenEndpointUrl: url,
        assertion: () => 'header.payload.signature',
        allowInsecure: true,
      })
    )
    expect(sent('SaslAuthenticate').map((request) => request.fields.authBytes)).toEqual([
      saslBytes(token),
    ])
    expect(requests.map((request) => request.body)).toEqual([
      'grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=header.payload.signature',
    ])
  })

  test('file helper crosses the real SASL boundary', async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'oauth-outcome-'))
    const file = path.join(dir, 'access.jwt')
    const token = unsecured('file-form')
    fs.writeFileSync(file, `${token}\n`)
    const fetch = jest.fn()
    await connect(oauthBearer.fileToken({ path: file, fetch }))
    expect(sent('SaslAuthenticate').map((request) => request.fields.authBytes)).toEqual([
      saslBytes(token),
    ])
    expect(fetch).not.toHaveBeenCalled()
  })

  test.each(['clientAssertion', 'jwtBearer'])(
    'local signer supplies the assertion before SASL authentication: %s',
    async (form) => {
      const { privateKey, publicKey } = crypto.generateKeyPairSync('ec', {
        namedCurve: 'prime256v1',
      })
      const token = unsecured('signed-form')
      const verified = []
      const { url } = await startIssuer((request, response) => {
        const fields = new URLSearchParams(request.body)
        const assertion = fields.get(form === 'jwtBearer' ? 'assertion' : 'client_assertion')
        const [header, claims, signature] = assertion.split('.')
        verified.push(
          crypto.verify(
            'sha256',
            Buffer.from(`${header}.${claims}`),
            { key: publicKey, dsaEncoding: 'ieee-p1363' },
            Buffer.from(signature, 'base64url')
          )
        )
        answerWith(token)(request, response)
      })
      const assertion = oauthBearer.signAssertion({
        privateKey,
        algorithm: 'ES256',
        claims: { iss: 'client', sub: 'client', aud: url },
        jti: true,
      })
      const options = { tokenEndpointUrl: url, allowInsecure: true }
      await connect(
        form === 'jwtBearer'
          ? oauthBearer.jwtBearer({ ...options, assertion })
          : oauthBearer.clientCredentials({
              ...options,
              clientId: 'client',
              clientAssertion: assertion,
            })
      )
      expect(verified).toEqual([true])
      const [authenticate] = sent('SaslAuthenticate')
      expect(authenticate.fields.authBytes).toEqual(saslBytes(token))
      expect(authenticate.fields.authBytes.toString()).not.toContain('ES256')
    }
  )

  test('reauthentication reuses a fresh cached token without a token request', async () => {
    const token = unsecured('cached')
    const { url, requests } = await startIssuer(answerWith(token))
    await connect(
      oauthBearer.clientCredentials({
        tokenEndpointUrl: url,
        clientId: 'client',
        clientSecret: SECRET,
        allowInsecure: true,
      }),
      { reauthenticationThreshold: 14900 }
    )
    await delay(150)
    await admin.listTopics()
    expect(sent('SaslAuthenticate').map((request) => request.fields.authBytes)).toEqual([
      saslBytes(token),
      saslBytes(token),
    ])
    expect(requests).toHaveLength(1)
  })

  test('without a reauthentication threshold the session is not renewed', async () => {
    const { url, requests } = await startIssuer(answerWith(unsecured('cached')))
    await connect(
      oauthBearer.clientCredentials({
        tokenEndpointUrl: url,
        clientId: 'client',
        clientSecret: SECRET,
        allowInsecure: true,
      }),
      { reauthenticationThreshold: 0 }
    )
    await delay(150)
    await admin.listTopics()
    expect(sent('SaslAuthenticate')).toHaveLength(1)
    expect(requests).toHaveLength(1)
  })

  test('proactive replacement is reused by near threshold authentication', async () => {
    const replacement = 'replacement-token'
    let refreshed
    const second = new Promise((resolve) => {
      refreshed = resolve
    })
    const { url, requests } = await startIssuer((request, response) => {
      if (requests.length === 1)
        return answerWith('first-token', { expires_in: 1 })(request, response)
      answerWith(replacement, { expires_in: 3600 })(request, response)
      refreshed()
    })
    await connect(
      oauthBearer.clientCredentials({
        tokenEndpointUrl: url,
        clientId: 'client',
        clientSecret: SECRET,
        allowInsecure: true,
        minRefreshPeriod: 1,
        refreshBuffer: 0,
        clockSkew: 0,
        refreshWindow: 0.3,
        refreshJitter: 0,
      }),
      { reauthenticationThreshold: 14900 }
    )
    await second
    for (let attempt = 0; attempt < 100 && (await provider()).value !== replacement; attempt++) {
      await delay(10)
    }
    await admin.listTopics()
    expect(requests).toHaveLength(2)
    expect(sent('SaslAuthenticate').map((request) => request.fields.authBytes)).toEqual([
      saslBytes('first-token'),
      saslBytes(replacement),
    ])
  })

  test('expired tokens with a dead endpoint fail as sanitized SASL errors', async () => {
    const short = `short-${MARKER}`
    const { url, requests } = await startIssuer(answerWith(short, { expires_in: 1 }))
    await connect(
      oauthBearer.clientCredentials({
        tokenEndpointUrl: url,
        clientId: 'client',
        clientSecret: SECRET,
        allowInsecure: true,
        clockSkew: 900,
        maxRetryTime: 2000,
      }),
      { reauthenticationThreshold: 14900 }
    )
    issuer.answer = (_request, response) => {
      response.writeHead(503, { 'Content-Type': 'application/json' })
      response.end(JSON.stringify({ error: 'temporarily_unavailable', error_description: MARKER }))
    }
    await delay(150)
    const started = Date.now()
    const error = await admin.listTopics().catch((caught) => caught)
    expect(Date.now() - started).toBeLessThan(5000)
    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.message).toMatch(/^SASL OAUTHBEARER helper: (status|deadline): /)
    expect(sent('SaslAuthenticate')).toHaveLength(1)
    expect(requests.length).toBeGreaterThan(1)
    expect(textOf(error)).not.toMatch(new RegExp(`${MARKER}|${SECRET}`))
    expect(textOf(logs)).not.toMatch(new RegExp(`${MARKER}|${SECRET}`))
  })

  test('a bare callback preserves the exact rejection object', async () => {
    const original = new Error('application-owned error')
    await expect(
      connect(async () => {
        throw original
      })
    ).rejects.toBe(original)
  })

  test.each([
    [
      'a supplier error',
      () =>
        oauthBearer.jwtBearer({
          tokenEndpointUrl: 'https://issuer.example/token',
          assertion: () => Promise.reject(new Error(MARKER)),
          maxRetryTime: 1,
        }),
    ],
    [
      'a token endpoint body',
      () =>
        oauthBearer.clientCredentials({
          tokenEndpointUrl: 'https://issuer.example/token',
          clientId: 'client',
          clientSecret: SECRET,
          fetch: async () =>
            new Response(JSON.stringify({ error: MARKER, access_token: MARKER }), { status: 400 }),
        }),
    ],
    [
      'a token file',
      () => {
        dir = fs.mkdtempSync(path.join(os.tmpdir(), 'oauth-outcome-'))
        fs.writeFileSync(path.join(dir, 'access.jwt'), MARKER)
        return oauthBearer.fileToken({ path: path.join(dir, 'access.jwt') })
      },
    ],
    [
      'a signing key file',
      () => {
        dir = fs.mkdtempSync(path.join(os.tmpdir(), 'oauth-outcome-'))
        const file = path.join(dir, 'key.pem')
        const { privateKey } = crypto.generateKeyPairSync('rsa', { modulusLength: 2048 })
        fs.writeFileSync(file, privateKey.export({ type: 'pkcs8', format: 'pem' }))
        const assertion = oauthBearer.signAssertion({ privateKey: { file } })
        fs.writeFileSync(file, `${MARKER} key material`)
        fs.utimesSync(file, new Date(), new Date(Date.now() + 10000))
        return oauthBearer.jwtBearer({
          tokenEndpointUrl: 'https://issuer.example/token',
          assertion,
          maxRetryTime: 1,
        })
      },
    ],
  ])('helper errors contain no supplier body token or key material: %s', async (_, create) => {
    const error = await connect(create()).catch((caught) => caught)
    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.message).toMatch(/^SASL OAUTHBEARER helper: /)
    expect(textOf(error)).not.toMatch(new RegExp(`${MARKER}|${SECRET}`))
    expect(textOf(logs)).not.toMatch(new RegExp(`${MARKER}|${SECRET}`))
    expect(textOf(error)).not.toContain('oauth-outcome-')
  })

  test('extension bytes remain compatible', async () => {
    const token = unsecured('extensions')
    const { url } = await startIssuer(answerWith(token))
    await connect(
      oauthBearer.clientCredentials({
        tokenEndpointUrl: url,
        clientId: 'client',
        clientSecret: SECRET,
        allowInsecure: true,
        extensions: { trace: 'one' },
      })
    )
    expect(sent('SaslAuthenticate')[0].fields.authBytes).toEqual(
      Buffer.from(`n,,\x01auth=Bearer ${token}\x01trace=one\x01\x01`)
    )
  })
})
