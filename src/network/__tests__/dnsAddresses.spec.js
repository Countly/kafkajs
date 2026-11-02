const dns = require('dns')
const fs = require('fs')
const net = require('net')
const path = require('path')
const tls = require('tls')
const Connection = require('../connection')
const defaultSocketFactory = require('../socketFactory')
const { createLogger, LEVELS } = require('../../loggers')

const HOST = 'brokers.example.test'
const certs = path.join(__dirname, '../../../testHelpers/certs')
const key = fs.readFileSync(path.join(certs, 'ca-key'))
const cert = fs.readFileSync(path.join(certs, 'cert-signed'))
const logger = createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} })

describe('Network > DNS through the default socket factory', () => {
  let servers, sockets, connections, lookups

  /**
   * Answers the test host with two addresses: 127.0.0.2, where nothing listens, then 127.0.0.1.
   * Every other host goes to the real resolver.
   */
  beforeEach(() => {
    servers = []
    sockets = []
    connections = []
    lookups = []
    const lookup = dns.lookup
    jest.spyOn(dns, 'lookup').mockImplementation(function (host, options, callback) {
      if (host !== HOST) return lookup.apply(this, arguments)
      lookups.push(options)
      const addresses = [
        { address: '127.0.0.2', family: 4 },
        { address: '127.0.0.1', family: 4 },
      ]
      process.nextTick(() =>
        options.all ? callback(null, addresses) : callback(null, addresses[0].address, 4)
      )
    })
  })

  afterEach(async () => {
    jest.restoreAllMocks()
    await Promise.all(connections.map((connection) => connection.disconnect({ drainTimeout: 0 })))
    sockets.forEach((socket) => socket.destroy())
    await Promise.all(servers.map((server) => new Promise((resolve) => server.close(resolve))))
  })

  /** @param {object} server @returns {Promise<number>} its port on 127.0.0.1 */
  const listen = async (server) => {
    servers.push(server)
    server.on('connection', (socket) => sockets.push(socket))
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
    return server.address().port
  }

  test.each([
    ['plaintext', () => net.createServer(), null],
    ['TLS', () => tls.createServer({ key, cert }), { ca: [cert], servername: 'kafkajs-test-ca' }],
  ])(
    'a %s connection tries every resolved address and resolves again for the next connection',
    async (_, createServer, ssl) => {
      const port = await listen(createServer())
      const connection = new Connection({
        host: HOST,
        port,
        logger,
        socketFactory: defaultSocketFactory(),
        connectionTimeout: 3000,
        requestTimeout: 30000,
        ssl,
      })
      connections.push(connection)

      await connection.connect()
      expect(connection.socket.autoSelectFamilyAttemptedAddresses).toEqual([
        `127.0.0.2:${port}`,
        `127.0.0.1:${port}`,
      ])
      await connection.disconnect()
      await connection.connect()

      expect(lookups).toHaveLength(2)
      expect(lookups.every((options) => options.all === true)).toBe(true)
      expect(connection.socket.remoteAddress).toBe('127.0.0.1')
    }
  )

  test('the documented canonical-names recipe resolves each bootstrap address to its name', async () => {
    const markdown = fs.readFileSync(path.join(__dirname, '../../../docs/Configuration.md'), 'utf8')
    const [fence] = [...markdown.matchAll(/^```javascript\n([\s\S]*?)^```$/gm)]
      .map((match) => match[1])
      .filter((code) => code.includes('async function canonicalBrokers('))
    const stub = {
      promises: {
        lookup: jest.fn(async (host, options) => {
          expect(options).toEqual({ all: true })
          return host === 'kafka.example.com'
            ? [
                { address: '10.0.0.1', family: 4 },
                { address: '10.0.0.2', family: 4 },
              ]
            : []
        }),
        reverse: jest.fn(async (address) => {
          if (address === '10.0.0.2')
            throw Object.assign(new Error('no PTR'), { code: 'ENOTFOUND' })
          return ['kafka-1.internal.example.com']
        }),
      },
    }
    const configs = []
    // eslint-disable-next-line no-new-func -- Execute the reviewed documentation fence only.
    new Function('require', 'Kafka', fence)(
      (name) => (name === 'dns' ? stub : require(name)),
      function Kafka(config) {
        configs.push(config)
      }
    )

    expect(configs).toHaveLength(1)
    await expect(configs[0].brokers()).resolves.toEqual([
      'kafka-1.internal.example.com:9093',
      '10.0.0.2:9093',
    ])
  })
})
