const { execFileSync } = require('child_process')
const { randomUUID } = require('crypto')
const fs = require('fs')
const net = require('net')
const path = require('path')
const Connection = require('../connection')
const defaultSocketFactory = require('../socketFactory')
const createRequest = require('../../protocol/request')
const apiVersions = require('../../protocol/requests/apiVersions')
const { createLogger, LEVELS } = require('../../loggers')
const { KafkaJSConnectionError, KafkaJSInvalidFrameError } = require('../../errors')
const { writeEvidence, evidenceLines } = require('testHelpers')

// Gated: needs the required-client-authentication cluster. Run only through the shared cluster guard.
const describeTls = process.env.TLS_NEGATIVE_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'tls-negative'
const linesBefore = evidenceLines(EVIDENCE).length
const ROOT = path.join(__dirname, '../../..')
const CA = [fs.readFileSync(path.join(ROOT, 'testHelpers/certs/cert-signed'))]
const ALERT = '15030300020250'
const PLAINTEXT_HINT = 'is this a TLS client on a plaintext port?'
const CASE_BUDGET_MS = 20000
const SCENARIO_BUDGET_MS = 120000

/**
 * TLS outcomes against real Kafka listeners: the base one-way SSL listener (9093), the dedicated
 * listener that requires a client certificate (9193) and the plaintext listener (9092). A positive
 * client-certificate control completes a Kafka request/response before any negative runs.
 *
 * Replay sets: durable log, committed offsets and handler delivery do not apply (no records).
 */
describeTls('TLS negatives against real SSL listeners (live)', () => {
  const started = Date.now()
  let dir, client, entries
  const connections = []

  /** A Connection to localhost over the default socket factory, logging into `entries`. */
  const connectionTo = (port, ssl = null) => {
    const connection = new Connection({
      host: 'localhost',
      port,
      logger: createLogger({
        level: LEVELS.DEBUG,
        logCreator: () => (entry) => entries.push(entry),
      }),
      socketFactory: defaultSocketFactory(),
      connectionTimeout: 5000,
      requestTimeout: 10000,
      ssl,
    })
    connections.push(connection)
    return connection
  }

  /** The first generation failure of a connection, whenever the transport reports it. */
  const firstFailure = (connection) =>
    new Promise((resolve) => {
      const failGeneration = connection.failGeneration.bind(connection)
      connection.failGeneration = (error, prefix) => {
        resolve(error)
        return failGeneration(error, prefix)
      }
    })

  const apiVersionsRequest = () => apiVersions.protocol({ version: 0 })()

  /**
   * The bytes a listener sends a plaintext ApiVersions request before it closes, read from a raw
   * socket so that no client framing interprets them.
   * @param {number} port
   * @returns {Promise<string>} hex of every byte received
   */
  const rawAnswer = async (port) => {
    const { buffer } = await createRequest({
      request: apiVersionsRequest().request,
      correlationId: 0,
      clientId: 'tls-negative',
    })
    return new Promise((resolve, reject) => {
      const chunks = []
      const socket = net.connect({ host: 'localhost', port }, () => socket.write(buffer))
      const timer = setTimeout(() => socket.destroy(new Error('no close within 5000 ms')), 5000)
      socket.on('data', (chunk) => chunks.push(chunk))
      socket.on('error', (error) => {
        clearTimeout(timer)
        if (error.code === 'ECONNRESET') resolve(Buffer.concat(chunks).toString('hex'))
        else reject(error)
      })
      socket.on('close', () => {
        clearTimeout(timer)
        resolve(Buffer.concat(chunks).toString('hex'))
      })
    })
  }

  beforeAll(() => {
    dir = path.join(ROOT, '.tmp', `tls-required-${randomUUID()}`)
    execFileSync(
      'bash',
      [path.join(ROOT, 'scripts/generateKafkaCertificates.sh'), '--client', dir],
      {
        timeout: 60000,
        stdio: 'ignore',
      }
    )
    client = {
      key: fs.readFileSync(path.join(dir, 'client.key')),
      cert: fs.readFileSync(path.join(dir, 'client.pem')),
    }
  })

  beforeEach(() => {
    entries = []
  })

  afterEach(async () => {
    await Promise.all(connections.splice(0).map((c) => c.disconnect({ drainTimeout: 0 })))
  })

  afterAll(() => {
    if (dir) fs.rmSync(dir, { recursive: true, force: true })
  })

  test(
    'a client certificate completes a Kafka request on the listener that requires one',
    async () => {
      const connection = connectionTo(9193, { ca: CA, key: client.key, cert: client.cert })
      await connection.connect()
      const answer = await connection.send(apiVersionsRequest())

      writeEvidence(EVIDENCE, {
        case: 'client-certificate',
        ok: answer.errorCode === 0 && answer.apiVersions.length > 0,
        wireVersions: { ApiVersions: 0 },
      })
      expect(answer.errorCode).toBe(0)
      expect(answer.apiVersions.length).toBeGreaterThan(0)
    },
    CASE_BUDGET_MS
  )

  test.each([
    [
      'a CA the client does not trust',
      true,
      /^(SELF_SIGNED_CERT_IN_CHAIN|UNABLE_TO_VERIFY_LEAF_SIGNATURE|UNABLE_TO_GET_ISSUER_CERT_LOCALLY)$/,
    ],
    [
      'a server name the certificate does not carry',
      { ca: CA, servername: 'wrong.example.test' },
      /^ERR_TLS_CERT_ALTNAME_INVALID$/,
    ],
  ])(
    'the one-way listener refuses %s, typed',
    async (name, ssl, code) => {
      const error = await connectionTo(9093, ssl)
        .connect()
        .catch((e) => e)

      writeEvidence(EVIDENCE, {
        case: `one-way ${name}`,
        ok: true,
        error: error?.name,
        code: error?.code,
      })
      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      expect(error.code).toMatch(code)
    },
    CASE_BUDGET_MS
  )

  test(
    'the listener that requires a client certificate refuses a client without one, typed',
    async () => {
      const connection = connectionTo(9193, { ca: CA })
      const failure = firstFailure(connection)
      const connected = await connection.connect().then(
        () => true,
        () => false
      )
      if (connected) connection.send(apiVersionsRequest()).catch(() => {})
      const error = await failure

      writeEvidence(EVIDENCE, {
        case: 'missing client certificate',
        ok: true,
        handshakeCompleted: connected,
        error: error.name,
        code: error.code,
      })
      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      expect(error.code).toMatch(/(CERTIFICATE_REQUIRED|HANDSHAKE_FAILURE)$/)
    },
    CASE_BUDGET_MS
  )

  test.each([9093, 9193])(
    'a plaintext client on SSL listener %i is refused by the frame cap before allocation',
    async (port) => {
      const bytes = await rawAnswer(port)
      const connection = connectionTo(port)
      await connection.connect()
      const error = await connection.send(apiVersionsRequest()).catch((e) => e)

      writeEvidence(EVIDENCE, {
        case: `plaintext on ${port}`,
        ok: true,
        bytes,
        error: error.name,
        reason: error.reason ?? null,
      })
      if (process.env.KAFKA_VERSION === '4.3.1') expect(bytes).toBe(ALERT)
      if (bytes.startsWith('1503')) {
        expect(error).toBeInstanceOf(KafkaJSInvalidFrameError)
        expect(error).toMatchObject({ reason: 'too-large', size: 0x15030300 })
        expect(error.message).toContain('TLS alert')
      } else {
        expect(bytes).toBe('')
        expect(error).toBeInstanceOf(KafkaJSConnectionError)
      }
    },
    CASE_BUDGET_MS
  )

  test(
    'a TLS client on the plaintext listener keeps its connection error and warns once',
    async () => {
      const error = await connectionTo(9092, { ca: CA })
        .connect()
        .catch((e) => e)
      const warnings = entries.filter(
        ({ label, log }) => label === 'WARN' && log.message.includes(PLAINTEXT_HINT)
      )

      writeEvidence(EVIDENCE, {
        case: 'TLS on plaintext',
        ok: true,
        error: error.name,
        message: error.message,
        warnings: warnings.length,
      })
      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      expect(['Connection closed while connecting', 'Connection error: read ECONNRESET']).toContain(
        error.message
      )
      expect(warnings).toHaveLength(1)
    },
    CASE_BUDGET_MS
  )

  test('records every case of this run within the scenario budget', () => {
    const cases = evidenceLines(EVIDENCE)
      .slice(linesBefore)
      .map((line) => line.case)

    expect(cases).toEqual([
      'client-certificate',
      'one-way a CA the client does not trust',
      'one-way a server name the certificate does not carry',
      'missing client certificate',
      'plaintext on 9093',
      'plaintext on 9193',
      'TLS on plaintext',
    ])
    expect(Date.now() - started).toBeLessThan(SCENARIO_BUDGET_MS)
  })
})
