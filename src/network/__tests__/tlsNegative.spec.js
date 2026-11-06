const fs = require('fs')
const net = require('net')
const path = require('path')
const tls = require('tls')
const Connection = require('../connection')
const defaultSocketFactory = require('../socketFactory')
const apiVersions = require('../../protocol/requests/apiVersions')
const { createLogger, LEVELS } = require('../../loggers')
const { KafkaJSConnectionError, KafkaJSInvalidFrameError } = require('../../errors')

/**
 * The tracked test CA (`testHelpers/certs`): a self-signed certificate, `CN=kafkajs-test-ca` with no
 * subject alternative name, and its key. Local servers present it; clients trust it through `ca`.
 */
const certs = path.join(__dirname, '../../../testHelpers/certs')
const key = fs.readFileSync(path.join(certs, 'ca-key'))
const cert = fs.readFileSync(path.join(certs, 'cert-signed'))

const PLAINTEXT_HINT = 'is this a TLS client on a plaintext port?'

describe('Network > TLS negatives through the ssl option and the default socket factory', () => {
  let servers, sockets, connections, entries

  /**
   * Starts a server on 127.0.0.1 and tracks it and its sockets for teardown.
   * @param {object} server a `net` or `tls` server whose connection listener is attached
   * @returns {Promise<number>} its port
   */
  const listen = async (server) => {
    servers.push(server)
    server.on(server instanceof tls.Server ? 'secureConnection' : 'connection', (socket) =>
      sockets.push(socket)
    )
    server.on('connection', (socket) => {
      sockets.push(socket)
      socket.on('error', () => {})
    })
    server.on('tlsClientError', () => {})
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
    return server.address().port
  }

  /** A Connection over the default socket factory, logging every entry into `entries`. */
  const connectionTo = (port, { host = '127.0.0.1', ssl = null } = {}) => {
    const connection = new Connection({
      host,
      port,
      logger: createLogger({
        level: LEVELS.DEBUG,
        logCreator: () => (entry) => entries.push(entry),
      }),
      socketFactory: defaultSocketFactory(),
      connectionTimeout: 3000,
      requestTimeout: 30000,
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

  const warnings = () =>
    entries.filter(({ label, log }) => label === 'WARN' && log.message.includes(PLAINTEXT_HINT))

  /** An ApiVersions v0 answer with no API: the smallest Kafka response the client decodes. */
  const answerApiVersions = (socket) =>
    socket.once('data', (frame) => {
      const correlationId = frame.readInt32BE(8)
      const answer = Buffer.alloc(14)
      answer.writeInt32BE(10, 0)
      answer.writeInt32BE(correlationId, 4)
      socket.write(answer)
    })

  beforeEach(() => {
    servers = []
    sockets = []
    connections = []
    entries = []
  })

  afterEach(async () => {
    await Promise.all(connections.map((connection) => connection.disconnect({ drainTimeout: 0 })))
    sockets.forEach((socket) => socket.destroy())
    await Promise.all(servers.map((server) => new Promise((resolve) => server.close(resolve))))
  })

  describe('server certificate verification is on by default', () => {
    let port

    beforeEach(async () => {
      port = await listen(tls.createServer({ key, cert }, answerApiVersions))
    })

    test('refuses a certificate from a CA the client does not trust', async () => {
      const error = await connectionTo(port, { ssl: true })
        .connect()
        .catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      expect(error.code).toBe('DEPTH_ZERO_SELF_SIGNED_CERT')
      expect(warnings()).toEqual([])
    })

    test('refuses an IP address that is not in the certificate', async () => {
      const error = await connectionTo(port, { ssl: { ca: [cert] } })
        .connect()
        .catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      expect(error.code).toBe('ERR_TLS_CERT_ALTNAME_INVALID')
      expect(error.message).toContain("IP: 127.0.0.1 is not in the cert's list")
    })

    test('checks the host name, sent as the server name, against the certificate', async () => {
      const error = await connectionTo(port, { host: 'localhost', ssl: { ca: [cert] } })
        .connect()
        .catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      expect(error.code).toBe('ERR_TLS_CERT_ALTNAME_INVALID')
      expect(error.message).toContain('Host: localhost.')
    })

    test('refuses a servername override the certificate does not name', async () => {
      const error = await connectionTo(port, {
        ssl: { ca: [cert], servername: 'wrong.example.test' },
      })
        .connect()
        .catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      expect(error.code).toBe('ERR_TLS_CERT_ALTNAME_INVALID')
    })

    test('connects and exchanges a request when the servername override names the certificate', async () => {
      const connection = connectionTo(port, { ssl: { ca: [cert], servername: 'kafkajs-test-ca' } })
      await connection.connect()

      await expect(connection.send(apiVersions.protocol({ version: 0 })())).resolves.toMatchObject({
        errorCode: 0,
        apiVersions: [],
      })
    })
  })

  describe('a listener that requires a client certificate', () => {
    const required = (options = {}) =>
      listen(
        tls.createServer(
          { key, cert, ca: [cert], requestCert: true, rejectUnauthorized: true, ...options },
          answerApiVersions
        )
      )
    const ssl = { ca: [cert], servername: 'kafkajs-test-ca' }

    test('answers a client that presents a trusted certificate', async () => {
      const connection = connectionTo(await required(), { ssl: { ...ssl, key, cert } })
      await connection.connect()

      await expect(connection.send(apiVersions.protocol({ version: 0 })())).resolves.toMatchObject({
        errorCode: 0,
      })
    })

    test('with TLS 1.3, fails a client without a certificate after the handshake, typed', async () => {
      const connection = connectionTo(await required({ minVersion: 'TLSv1.3' }), { ssl })
      const failure = firstFailure(connection)

      await connection.connect()
      const sent = connection.send(apiVersions.protocol({ version: 0 })()).catch((e) => e)

      const error = await failure
      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      expect(error.code).toMatch(/CERTIFICATE_REQUIRED$/)
      expect(await sent).toBeInstanceOf(KafkaJSConnectionError)
      expect(connection.isConnected()).toBe(false)
      expect(warnings()).toEqual([])
    })

    test('with TLS 1.2, fails a client without a certificate during the handshake, typed', async () => {
      const connection = connectionTo(await required({ maxVersion: 'TLSv1.2' }), { ssl })

      const error = await connection.connect().catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      expect(error.code).toMatch(/^ERR_SSL_.*HANDSHAKE_FAILURE$/)
      expect(warnings()).toEqual([])
    })
  })

  describe('a plaintext client answered with a TLS alert', () => {
    test.each([
      ['handshake_failure', [0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x46]],
      ['internal_error', [0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x50]],
    ])(
      'refuses the %s record as an oversized frame before allocating its body',
      async (_, alert) => {
        const port = await listen(
          net.createServer((socket) => socket.once('data', () => socket.end(Buffer.from(alert))))
        )
        const connection = connectionTo(port)
        await connection.connect()
        const allocations = []
        const allocUnsafe = jest.spyOn(Buffer, 'allocUnsafe')
        const alloc = jest.spyOn(Buffer, 'alloc')
        try {
          const error = await connection
            .send(apiVersions.protocol({ version: 0 })())
            .catch((e) => e)
          allocations.push(...allocUnsafe.mock.calls, ...alloc.mock.calls)

          expect(error).toBeInstanceOf(KafkaJSInvalidFrameError)
          expect(error).toMatchObject({ reason: 'too-large', size: 0x15030300 })
          expect(error.message).toBe(
            'A response frame declared 352518912 bytes, above maxResponseSize (104857600). The four size bytes look like a TLS alert: is this a plaintext connection to a TLS endpoint?'
          )
          expect(allocations.filter(([size]) => size >= 65536)).toEqual([])
          expect(connection.isConnected()).toBe(false)
        } finally {
          allocUnsafe.mockRestore()
          alloc.mockRestore()
        }
      }
    )
  })

  describe('a TLS client on a plaintext port', () => {
    test('keeps the connection error of a peer that closes, and warns once', async () => {
      const port = await listen(
        net.createServer((socket) => socket.once('data', () => socket.end()))
      )

      const error = await connectionTo(port, { ssl: { ca: [cert] } })
        .connect()
        .catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      expect(error.message).toBe('Connection closed while connecting')
      expect(warnings()).toHaveLength(1)
      expect(warnings()[0].log.broker).toBe(`127.0.0.1:${port}`)
    })

    test('keeps the connection error of a peer that resets, and warns once', async () => {
      const port = await listen(
        net.createServer((socket) => socket.once('data', () => socket.resetAndDestroy()))
      )

      const error = await connectionTo(port, { ssl: { ca: [cert] } })
        .connect()
        .catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      expect(error.code).toBe('ECONNRESET')
      expect(error.message).toBe('Connection error: read ECONNRESET')
      expect(warnings()).toHaveLength(1)
    })

    test('does not warn for a plaintext client whose peer closes', async () => {
      const port = await listen(net.createServer((socket) => socket.end()))
      const connection = connectionTo(port)
      const failure = firstFailure(connection)

      await connection.connect().catch(() => {})
      await failure

      expect(warnings()).toEqual([])
    })
  })
})
