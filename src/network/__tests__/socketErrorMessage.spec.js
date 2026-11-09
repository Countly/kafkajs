const Connection = require('../connection')
const Encoder = require('../../protocol/encoder')
const { createFakeSocketFactory } = require('../../../testHelpers/fakeSocket')
const { connectionOpts } = require('testHelpers')

describe('Network > socket error diagnostics', () => {
  test('an aggregate refusal while connecting reports its code and rejects the connect', async () => {
    const fake = createFakeSocketFactory()
    const connection = new Connection(
      connectionOpts({
        socketFactory: (options) => fake.factory({ ...options, onConnect: () => {} }),
      })
    )
    try {
      const pending = connection.connect().catch((error) => error)
      const cause = Object.assign(new AggregateError([]), { code: 'ECONNREFUSED' })
      fake.state.socket.emit('error', cause)

      await expect(pending).resolves.toMatchObject({
        name: 'KafkaJSConnectionError',
        message: 'Connection error: ECONNREFUSED',
        code: 'ECONNREFUSED',
        retriable: true,
      })
      expect(connection.isConnected()).toBe(false)
      expect(fake.state.socket.destroyed).toBe(true)
    } finally {
      await connection.disconnect({ drainTimeout: 0 })
    }
  })

  test.each([
    [
      'an aggregate refusal',
      () => Object.assign(new AggregateError([]), { code: 'ECONNREFUSED' }),
      'ECONNREFUSED',
    ],
    ['an empty Error', () => new Error(), 'Socket error'],
    [
      'a normal error',
      () => Object.assign(new Error('connect failed'), { code: 'ECONNRESET' }),
      'connect failed',
    ],
  ])(
    'describes %s while failing the pending request immediately',
    async (_label, createError, description) => {
      const fake = createFakeSocketFactory()
      const connection = new Connection(connectionOpts({ socketFactory: fake.factory }))
      const cause = createError()
      try {
        await connection.connect()
        const pending = connection
          .send({
            request: {
              apiKey: 18,
              apiVersion: 0,
              apiName: 'ApiVersions',
              encode: async () => new Encoder(),
              expectResponse: () => true,
            },
            response: { decode: async (buffer) => buffer, parse: async (buffer) => buffer },
          })
          .catch((error) => error)
        await new Promise((resolve) => setImmediate(resolve))
        expect(connection.requestQueue.inflight.size).toBe(1)

        fake.state.socket.emit('error', cause)
        const error = await pending

        expect(error).toMatchObject({
          name: 'KafkaJSConnectionError',
          message: `Connection error: ${description}`,
          code: cause.code,
          retriable: true,
          broker: connection.broker,
        })
        expect(fake.state.socket.destroyed).toBe(true)
        expect(connection.requestQueue.inflight.size).toBe(0)
      } finally {
        await connection.disconnect({ drainTimeout: 0 })
      }
    }
  )
})
