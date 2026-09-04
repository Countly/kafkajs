const zlib = require('zlib')
const createProducer = require('./index')
const createConsumer = require('../consumer')
const Compression = require('../protocol/message/compression')
const { KafkaJSNotImplemented } = require('../errors')

const {
  secureRandom,
  createTopic,
  createModPartitioner,
  connectionOpts,
  newLogger,
  createCluster,
  waitForMessages,
} = require('testHelpers')

describe('Producer', () => {
  let topicName, cluster, producer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    await createTopic({ topic: topicName })

    cluster = createCluster({ ...connectionOpts(), createPartitioner: createModPartitioner })
    producer = createProducer({ cluster, logger: newLogger() })
    await producer.connect()
  })

  afterEach(async () => {
    producer && (await producer.disconnect())
  })

  // eslint-disable-next-line n/no-unsupported-features/node-builtins -- feature detection, not a call
  const hasZstd = typeof zlib.zstdCompressSync === 'function'
  let hasLz4 = true
  try {
    require('lz4-napi')
  } catch (e) {
    hasLz4 = false
  }

  // Snappy has no built-in codec and still requires user registration.
  describe('snappy compression not configured', () => {
    it('throws an error', async () => {
      await expect(
        producer.send({
          topic: topicName,
          compression: Compression.Types.Snappy,
          messages: [{ key: secureRandom(), value: secureRandom() }],
        })
      ).rejects.toThrow(KafkaJSNotImplemented)
    })
  })

  // LZ4 and ZSTD are built in from Phase 2 — when available they must round-trip, and when the
  // platform cannot provide them the error must still be the documented KafkaJSNotImplemented.
  const builtInCodecs = [
    { name: 'lz4', codec: Compression.Types.LZ4, available: hasLz4 },
    { name: 'zstd', codec: Compression.Types.ZSTD, available: hasZstd },
  ]

  for (const { name, codec, available } of builtInCodecs) {
    describe(`${name} compression`, () => {
      if (!available) {
        it('throws KafkaJSNotImplemented when the platform cannot provide it', async () => {
          await expect(
            producer.send({
              topic: topicName,
              compression: codec,
              messages: [{ key: secureRandom(), value: secureRandom() }],
            })
          ).rejects.toThrow(KafkaJSNotImplemented)
        })
        return
      }

      it('produces and consumes a compressed batch through the cluster', async () => {
        const key = secureRandom()
        const value = secureRandom()

        await producer.send({ topic: topicName, compression: codec, messages: [{ key, value }] })

        const consumer = createConsumer({
          cluster: createCluster(),
          groupId: `consumer-group-id-${secureRandom()}`,
          logger: newLogger(),
          maxWaitTimeInMs: 100,
        })
        await consumer.connect()
        await consumer.subscribe({ topic: topicName, fromBeginning: true })

        const received = []
        await consumer.run({ eachMessage: async (event) => received.push(event) })

        // waitForMessages resolves once `number` messages have arrived; a broken codec never
        // delivers one, so the test fails via Jest's own timeout instead of hanging forever.
        const [{ message }] = await waitForMessages(received, { number: 1 })
        expect(message.key).toEqual(Buffer.from(key))
        expect(message.value).toEqual(Buffer.from(value))

        await consumer.disconnect()
      })
    })
  }
})
