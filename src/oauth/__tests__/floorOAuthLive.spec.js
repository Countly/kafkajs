const FloorCapabilities = require('../../../testHelpers/floorCapabilities')
const crypto = require('node:crypto')
const fs = require('node:fs')
const http = require('node:http')
const os = require('node:os')
const path = require('node:path')
const { Kafka, logLevel, oauthBearer } = require('../../..')
const { saslBrokers, sslConnectionOpts, writeEvidence, wireVersionsOf } = require('testHelpers')
const { waitForCondition, withTimeout } = require('../../consumer/__tests__/fixtures/liveScenario')
const { CleanupStack } = require('../../producer/__tests__/fixtures/liveProbe')

// Gated: it authenticates against the OAUTHBEARER compose (docker-compose.oauthbearer.yml). Run
// alone: `npm run test:floor:oauth`.
const describeLive = process.env.FLOOR_OAUTH_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'floor-oauth'
const SCENARIO_BUDGET_MS = 180000
const HOOK_BUDGET_MS = 30000
const DISCONNECT_BUDGET_MS = 15000
const LIFETIME_SECONDS = 120
const TOLERANCE_MS = 1000
const HTTP_FORMS = ['secret', 'assertion', 'jwt']
const VARIANTS = [...HTTP_FORMS, 'file', 'signer']
const segment = (value) => Buffer.from(JSON.stringify(value)).toString('base64url')

/**
 * Live proof that the four helper forms authenticate a producer and an admin against a real
 * OAUTHBEARER broker: one token request per acquisition shared by both clients, the proactive
 * refresh observed as a second request inside the seeded window, a rotated token file used at the
 * next authentication, and every acknowledged record read back from the log exactly once.
 *
 * The in-process issuer returns unsecured access JWTs, which the compose validator accepts; signed
 * assertions go to the issuer, which verifies them with the separately held public key. Expected
 * times are computed from the issuer's own `exp` and the observed receipt times, never by the
 * helpers' code.
 */
describeLive('OAuth helpers against a live OAUTHBEARER broker', () => {
  const runId = `${Date.now().toString(36)}-${crypto.randomUUID().slice(0, 8)}`
  const topic = `e2e-oauth-helpers-${runId}`
  /** @type {Record<string, Array<{ at: number, exp: number }>>} what the issuer issued per form */
  const issued = { secret: [], assertion: [], jwt: [] }
  const verified = []
  const signerCalls = { assertion: 0, jwt: 0 }
  const wire = []
  const acknowledged = []
  const fileSupplied = []
  const clients = []
  const cleanup = new CleanupStack()
  let issuer
  let issuerUrl
  let sockets
  let dir
  let tokenFile
  let providers
  let signingKeys

  const unsecured = (form, generation) => {
    const iat = Math.floor(Date.now() / 1000)
    return {
      exp: (iat + LIFETIME_SECONDS) * 1000,
      token: `${segment({ alg: 'none', typ: 'JWT' })}.${segment({ sub: 'test', iat, exp: iat + LIFETIME_SECONDS, form, generation })}.`,
    }
  }
  const verifiesAssertion = (assertion) => {
    const [header, claims, signature] = String(assertion).split('.')
    return crypto.verify(
      'sha256',
      Buffer.from(`${header}.${claims}`),
      signingKeys.publicKey,
      Buffer.from(signature || '', 'base64url')
    )
  }
  const formOf = (headers, fields) => {
    if (
      headers.authorization ===
      `Basic ${Buffer.from('live-secret-client:inert-live-secret').toString('base64')}`
    )
      return 'secret'
    if (
      fields.get('client_assertion_type') ===
      'urn:ietf:params:oauth:client-assertion-type:jwt-bearer'
    )
      return 'assertion'
    if (fields.get('grant_type') === 'urn:ietf:params:oauth:grant-type:jwt-bearer') return 'jwt'
    return null
  }
  const signer = (form) => {
    const sign = oauthBearer.signAssertion({
      privateKey: signingKeys.privateKey,
      claims: { iss: `live-${form}-client`, sub: `live-${form}-client`, aud: 'live-issuer' },
      jti: true,
    })
    return () => {
      signerCalls[form]++
      return sign()
    }
  }
  const kafkaFor = (clientId, oauthBearerProvider) =>
    new Kafka({
      clientId,
      brokers: saslBrokers(),
      ssl: sslConnectionOpts().ssl,
      sasl: { mechanism: 'oauthbearer', oauthBearerProvider },
      logLevel: logLevel.NOTHING,
    })
  const track = (client) => {
    client.on(client.events.REQUEST, ({ payload }) =>
      wire.push({ apiName: payload.apiName, apiVersion: payload.apiVersion })
    )
    clients.push(client)
    return client
  }
  const produce = async (producer, form, step) => {
    const id = `${form}-${step}-${runId}`
    const [result] = await producer.send({ topic, acks: -1, messages: [{ key: id, value: id }] })
    acknowledged.push({ id, partition: result.partition, offset: result.baseOffset })
  }

  /**
   * Disconnects every tracked client, each whatever the others did.
   * @returns {Promise<void>}
   * @throws {Error} the first disconnect failure, after all were attempted
   */
  const disconnectClients = async () => {
    const results = await Promise.allSettled(
      clients
        .splice(0)
        .reverse()
        .map((client) => withTimeout(client.disconnect(), DISCONNECT_BUDGET_MS))
    )
    const failed = results.find(({ status }) => status === 'rejected')
    if (failed) throw failed.reason
  }

  beforeAll(async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'oauth-helpers-live-'))
    cleanup.add('temporary directory removal', () =>
      fs.rmSync(dir, { recursive: true, force: true })
    )
    tokenFile = path.join(dir, 'access.jwt')
    fs.writeFileSync(tokenFile, unsecured('file', 1).token)
    signingKeys = crypto.generateKeyPairSync('rsa', { modulusLength: 2048 })
    sockets = new Set()
    issuer = http.createServer(async (request, response) => {
      const chunks = []
      for await (const chunk of request) chunks.push(chunk)
      const fields = new URLSearchParams(Buffer.concat(chunks).toString())
      const form = formOf(request.headers, fields)
      if (form === 'assertion') verified.push(verifiesAssertion(fields.get('client_assertion')))
      if (form === 'jwt') verified.push(verifiesAssertion(fields.get('assertion')))
      if (!form || verified.includes(false)) {
        response.writeHead(400, { 'Content-Type': 'application/json' })
        return response.end(JSON.stringify({ error: 'invalid_request' }))
      }
      const { token, exp } = unsecured(form, issued[form].length + 1)
      issued[form].push({ at: Date.now(), exp })
      response.writeHead(200, { 'Content-Type': 'application/json' })
      response.end(JSON.stringify({ access_token: token, token_type: 'Bearer' }))
    })
    issuer.on('connection', (socket) => {
      sockets.add(socket)
      socket.once('close', () => sockets.delete(socket))
    })
    await new Promise((resolve) => issuer.listen(0, '127.0.0.1', resolve))
    cleanup.add('issuer shutdown', async () => {
      const closed = [...sockets].map(
        (socket) => new Promise((resolve) => socket.once('close', resolve))
      )
      issuer.closeIdleConnections()
      await new Promise((resolve) => issuer.close(resolve))
      await Promise.all(closed)
    })
    cleanup.add('client disconnect', disconnectClients)
    issuerUrl = `http://127.0.0.1:${issuer.address().port}/token`
  }, HOOK_BUDGET_MS)

  afterAll(async () => {
    const failures = await cleanup.closeAll({ budgetMs: HOOK_BUDGET_MS - 5000 })
    if (failures.length) throw new AggregateError(failures, 'The OAuth floor cleanup failed')
  }, HOOK_BUDGET_MS)

  test(
    'authenticates every helper form and observes refresh and file rotation',
    async () => {
      const started = Date.now()
      const helperOptions = {
        tokenEndpointUrl: issuerUrl,
        allowInsecure: true,
        timeout: 5000,
        maxRetryTime: 8000,
        minRefreshPeriod: 1000,
        refreshBuffer: 0,
        clockSkew: 1000,
        refreshWindow: 0.8,
        refreshJitter: 0.05,
      }
      providers = {
        secret: oauthBearer.clientCredentials({
          ...helperOptions,
          clientId: 'live-secret-client',
          clientSecret: 'inert-live-secret',
        }),
        assertion: oauthBearer.clientCredentials({
          ...helperOptions,
          clientId: 'live-assertion-client',
          clientAssertion: signer('assertion'),
        }),
        jwt: oauthBearer.jwtBearer({ ...helperOptions, assertion: signer('jwt') }),
        file: oauthBearer.fileToken({ ...helperOptions, path: tokenFile }),
      }
      cleanup.add('provider close', () => Object.values(providers).forEach((p) => p.close()))
      const fileProvider = async () => {
        const response = await providers.file()
        fileSupplied.push(response.value)
        return response
      }

      // 1. One acquisition per HTTP form, with the refresh draw seeded at the window's centre.
      const receipt = {}
      const random = jest.spyOn(Math, 'random').mockReturnValue(0.5)
      try {
        for (const form of HTTP_FORMS) {
          const before = Date.now()
          await providers[form]()
          receipt[form] = { before, after: Date.now() }
        }
        await providers.file()
      } finally {
        random.mockRestore()
      }
      expect(HTTP_FORMS.map((form) => issued[form].length)).toEqual([1, 1, 1])
      expect(verified).toEqual([true, true])

      // 2. A producer and an admin per form share its provider. A separate admin on its own fresh
      // tokens stays open to delete the topic after every other client has gone.
      const janitor = kafkaFor(`oauth-helpers-janitor-${runId}`, async () => ({
        value: unsecured('janitor', 1).token,
      })).admin()
      cleanup.add('janitor disconnect', () => janitor.disconnect())
      cleanup.add('topic deletion', async () => {
        try {
          await janitor.deleteTopics({ topics: [topic] })
        } catch (error) {
          if (error.type !== 'UNKNOWN_TOPIC_OR_PARTITION') throw error
        }
      })
      await janitor.connect()
      const producers = {}
      for (const form of [...HTTP_FORMS, 'file']) {
        const kafka = kafkaFor(
          `oauth-helpers-${form}-${runId}`,
          form === 'file' ? fileProvider : providers[form]
        )
        const admin = track(kafka.admin())
        producers[form] = track(kafka.producer())
        await admin.connect()
        await producers[form].connect()
        if (form === 'secret') {
          await admin.createTopics({
            waitForLeaders: true,
            topics: [{ topic, numPartitions: 2, replicationFactor: 2 }],
          })
        }
        expect((await admin.fetchTopicMetadata({ topics: [topic] })).topics[0].name).toBe(topic)
        await produce(producers[form], form, 'first')
      }
      expect(HTTP_FORMS.map((form) => issued[form].length)).toEqual([1, 1, 1])
      expect(signerCalls).toEqual({ assertion: 1, jwt: 1 })
      FloorCapabilities.variants(VARIANTS, [
        ...new Set(acknowledged.map(({ id }) => id.split('-')[0])),
        ...(signerCalls.assertion > 0 && signerCalls.jwt > 0 ? ['signer'] : []),
      ])

      // 3. A rotated file is used at the next authentication, while the first token is valid.
      const rotated = unsecured('file', 2).token
      fs.writeFileSync(tokenFile, rotated)
      fs.utimesSync(tokenFile, new Date(), new Date(Date.now() + 1000))
      await producers.file.disconnect()
      await producers.file.connect()
      expect(fileSupplied.at(-1)).toBe(rotated)
      await produce(producers.file, 'file', 'rotated')

      // 4. Each HTTP form refreshes once, on its own timer, inside the seeded window.
      const windows = {}
      for (const form of HTTP_FORMS) {
        const { exp } = issued[form][0]
        const at = (fetched, factor) => fetched + factor * (exp - fetched)
        windows[form] = {
          seeded: [
            at(receipt[form].before, 0.8) - TOLERANCE_MS,
            at(receipt[form].after, 0.8) + TOLERANCE_MS,
          ],
          nominal: [at(receipt[form].before, 0.75), at(receipt[form].after, 0.85)],
        }
      }
      const latest = Math.max(...HTTP_FORMS.map((form) => windows[form].seeded[1]))
      const refreshed = await waitForCondition(
        () => HTTP_FORMS.every((form) => issued[form].length >= 2),
        {
          attempts: Math.ceil((latest - Date.now() + 5000) / 250),
          delay: 250,
        }
      )
      expect(refreshed).toBe(true)
      for (const form of HTTP_FORMS) {
        const second = issued[form][1].at
        expect(second).toBeGreaterThanOrEqual(windows[form].seeded[0])
        expect(second).toBeLessThanOrEqual(windows[form].seeded[1])
      }
      expect(signerCalls).toEqual({ assertion: 2, jwt: 2 })

      // 5. After the refresh the clients keep working with no further acquisition.
      for (const form of HTTP_FORMS) {
        await producers[form].disconnect()
        await producers[form].connect()
        await produce(producers[form], form, 'refreshed')
      }
      expect(HTTP_FORMS.map((form) => issued[form].length)).toEqual([2, 2, 2])

      // 6. The broker's log holds every acknowledged record exactly once.
      const reader = track(
        kafkaFor(`oauth-helpers-reader-${runId}`, providers.secret).consumer({
          groupId: `e2e-oauth-helpers-reader-${runId}`,
        })
      )
      const observed = []
      await reader.connect()
      await reader.subscribe({ topic, fromBeginning: true })
      await reader.run({
        autoCommit: false,
        eachMessage: async ({ partition, message }) => {
          observed.push({ id: message.value.toString(), partition, offset: message.offset })
        },
      })
      const complete = await waitForCondition(() => observed.length >= acknowledged.length, {
        attempts: 120,
        delay: 250,
      })
      await reader.stop()
      const key = ({ id, partition, offset }) => `${id}|${partition}|${offset}`
      expect(complete).toBe(true)
      expect(observed.map(key).sort()).toEqual(acknowledged.map(key).sort())

      writeEvidence(EVIDENCE, {
        case: 'F18',
        scenario: 'oauth-helpers',
        forms: [...HTTP_FORMS, 'file'],
        lifetimeMs: LIFETIME_SECONDS * 1000,
        config: {
          timeout: helperOptions.timeout,
          maxRetryTime: helperOptions.maxRetryTime,
          minRefreshPeriod: helperOptions.minRefreshPeriod,
          refreshBuffer: helperOptions.refreshBuffer,
          clockSkew: helperOptions.clockSkew,
          refreshWindow: helperOptions.refreshWindow,
          refreshJitter: helperOptions.refreshJitter,
        },
        requests: Object.fromEntries(
          HTTP_FORMS.map((form) => [form, issued[form].map(({ at }) => at - started)])
        ),
        windows: Object.fromEntries(
          HTTP_FORMS.map((form) => [
            form,
            {
              seeded: windows[form].seeded.map((at) => at - started),
              nominal: windows[form].nominal.map((at) => at - started),
            },
          ])
        ),
        signerCalls,
        rotationObserved: fileSupplied.at(-1) === rotated,
        acknowledged: acknowledged.map(({ id, partition, offset }) => ({ id, partition, offset })),
        observed: observed.length,
        wireVersions: wireVersionsOf(wire),
      })
      expect(Date.now() - started).toBeLessThan(SCENARIO_BUDGET_MS)
    },
    SCENARIO_BUDGET_MS
  )

  test(
    'closed providers reject later calls and the issuer keeps no sockets',
    async () => {
      await disconnectClients()
      Object.values(providers).forEach((provider) => provider.close())
      for (const provider of Object.values(providers))
        await expect(provider()).rejects.toThrow('closed')
      issuer.closeIdleConnections()
      const settled = await waitForCondition(() => sockets.size === 0, { attempts: 40, delay: 250 })
      expect(settled).toBe(true)
    },
    HOOK_BUDGET_MS
  )

  test(
    'reads the SASL capability facts from an OAUTHBEARER-authenticated connection',
    async () => {
      const provider = oauthBearer.clientCredentials({
        tokenEndpointUrl: issuerUrl,
        allowInsecure: true,
        timeout: 5000,
        maxRetryTime: 8000,
        clientId: 'live-secret-client',
        clientSecret: 'inert-live-secret',
      })
      cleanup.add('capability provider close', () => provider.close())
      const [host, port] = saslBrokers()[0].split(':')
      const ranges = await FloorCapabilities.authenticated(
        {
          ...sslConnectionOpts(),
          host,
          port: Number(port),
          sasl: { mechanism: 'oauthbearer', oauthBearerProvider: provider },
        },
        process.env.KAFKA_VERSION
      )

      writeEvidence(EVIDENCE, { case: 'F18-capability', authenticated: 'oauthbearer', ranges })
      expect(Object.keys(ranges)).toEqual(['SaslHandshake', 'SaslAuthenticate'])
    },
    HOOK_BUDGET_MS
  )
})

describe('Floor OAuth expectations', () => {
  test('requires every named variant exactly once', () => {
    expect(() => FloorCapabilities.variants(VARIANTS, VARIANTS)).not.toThrow()
  })

  test('refuses an omitted variant', () => {
    expect(() => FloorCapabilities.variants(VARIANTS, VARIANTS.slice(1))).toThrow(
      'Variant population differs'
    )
  })

  test('refuses duplicate or extra variants', () => {
    expect(() => FloorCapabilities.variants(VARIANTS, [...VARIANTS, VARIANTS[0]])).toThrow(
      'Duplicate observed variant'
    )
    expect(() => FloorCapabilities.variants(VARIANTS, [...VARIANTS, 'extra'])).toThrow(
      'Variant population differs'
    )
  })
})
