const crypto = require('crypto')
const Connection = require('../../../network/connection')
const { createFakeSocketFactory } = require('../../../../testHelpers/fakeSocket')
const { createLogger, LEVELS } = require('../../../loggers')
const { Kafka } = require('../../../../index')
const { KafkaJSInvalidConfigurationError } = require('../../../errors')
const { mulberry32 } = require('../../../../testHelpers/schedule')
const { Types, Codecs } = require('../../message/compression')
const { protocol, versions } = require('./index')
const {
  RequestBudget,
  batchBytes,
  encodedBound,
  framing,
  minimumRequestSize,
  topicFraming,
} = require('../../../producer/state/recordSize')

const logger = createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} })
const TOPIC_ID = '00000000-0000-4000-8000-000000000001'

/** The refusal the producer's own guard returns for a frame above `maxRequestSize`. */
const capGuard = (maxRequestSize, seen) => (frameLength) => {
  if (frameLength === undefined) return null
  seen.push(frameLength)
  return frameLength > maxRequestSize ? new Error(`${frameLength} > ${maxRequestSize}`) : null
}

/**
 * Writes one Produce request through a real `Connection` over a capturing socket.
 * @returns {Promise<{ written: Buffer[], refusal: Error|null, open: boolean, inflight: number }>}
 */
const transmit = async ({ version, clientId, transactionalId, topicData, guard }) => {
  const fake = createFakeSocketFactory()
  const connection = new Connection({
    host: 'broker',
    port: 9092,
    clientId,
    logger,
    socketFactory: fake.factory,
    connectionTimeout: 1000,
    requestTimeout: 30000,
    maxInFlightRequests: 1,
    enforceRequestTimeout: false,
  })
  await connection.connect()
  const { request, response } = protocol({ version })({
    acks: 0,
    timeout: 30000,
    topicData,
    transactionalId,
    topicIds: new Map(topicData.map(({ topic }) => [topic, TOPIC_ID])),
  })
  const refusal = await connection.send({ request, response, guard }).then(
    () => null,
    (error) => error
  )
  const result = {
    written: [...fake.state.writes],
    refusal,
    open: connection.isConnected(),
    inflight: connection.requestQueue.inflight.size,
  }
  await connection.disconnect()
  return result
}

/** What the producer plans for one request: its framing, each topic's, each batch's. */
const planOf = (budget, clientId, transactionalId, topicData, compression = Types.None) => {
  let bytes = framing(clientId, transactionalId)
  for (const { topic, partitions } of topicData) {
    bytes += budget.topicBytes(topic)
    for (const { messages } of partitions) bytes += budget.batchBytes({ compression, messages })
  }
  return bytes
}

describe('Protocol > Requests > Produce > the request-size plan and the frame', () => {
  describe.each(versions.map(Number))('Produce v%i', (version) => {
    const clientId = 'framing-client'
    const transactionalId = version >= 3 ? 'framing-transaction' : null
    const topicData = [
      {
        topic: 'framing-topic',
        partitions: [
          { partition: 0, messages: [{ key: 'k', value: 'x'.repeat(5000), headers: { h: 'v' } }] },
        ],
      },
    ]

    test('the guard is handed the frame that is written, its length prefix and header included', async () => {
      const seen = []
      const { written, refusal } = await transmit({
        version,
        clientId,
        transactionalId,
        topicData,
        guard: capGuard(Infinity, seen),
      })

      expect(refusal).toBeNull()
      expect(written).toHaveLength(1)
      expect(seen).toEqual([written[0].length])
      expect(written[0].readInt32BE(0) + 4).toBe(written[0].length)
    })

    test('a frame at the cap is written, and one byte over it is refused unwritten with the connection open', async () => {
      const probe = await transmit({ version, clientId, transactionalId, topicData })
      const frame = probe.written[0].length

      const at = await transmit({
        version,
        clientId,
        transactionalId,
        topicData,
        guard: capGuard(frame, []),
      })
      const over = await transmit({
        version,
        clientId,
        transactionalId,
        topicData,
        guard: capGuard(frame - 1, []),
      })

      expect(at.written.map((buffer) => buffer.length)).toEqual([frame])
      expect(over.written).toEqual([])
      expect(over.refusal.message).toBe(`${frame} > ${frame - 1}`)
      expect(over.open).toBe(true)
      expect(over.inflight).toBe(0)
    })

    test('the plan of the request is at least its frame', async () => {
      const budget = new RequestBudget({ maxRequestSize: 1048576, clientId, transactionalId })
      const { written } = await transmit({ version, clientId, transactionalId, topicData })

      expect(planOf(budget, clientId, transactionalId, topicData)).toBeGreaterThanOrEqual(
        written[0].length
      )
    })
  })

  test('over 200 seeded shapes the plan is at least the frame and at most 32 + 26 per topic + 8 per partition + 21 per record above it', async () => {
    const outcomes = []
    for (let seed = 1; seed <= 200; seed += 1) {
      const random = mulberry32(seed)
      const below = (n) => Math.floor(random() * n)
      const version = Number(versions[seed % versions.length])
      const clientId = 'c'.repeat(200)
      const transactionalId = random() < 0.5 ? null : 't'.repeat(below(50))
      const topicData = []
      let partitions = 0
      let records = 0
      for (let t = 0, topics = 1 + below(5); t < topics; t += 1) {
        const entry = { topic: `shape-${seed}-${t}-${'x'.repeat(below(30))}`, partitions: [] }
        for (let p = 0, count = 1 + below(8); p < count; p += 1) {
          const messages = Array.from({ length: 1 + below(5) }, () => ({
            ...(random() < 0.5 ? { key: 'k'.repeat(below(40)) } : {}),
            value: random() < 0.1 ? null : 'v'.repeat(below(300)),
            ...(random() < 0.3 ? { headers: { h: 'x'.repeat(below(20)), list: ['a', 'bb'] } } : {}),
            ...(random() < 0.3 ? { timestamp: String(1700000000000 + below(1000000)) } : {}),
          }))
          entry.partitions.push({ partition: p, messages })
          partitions += 1
          records += messages.length
        }
        topicData.push(entry)
      }
      const budget = new RequestBudget({ maxRequestSize: 2147483647, clientId, transactionalId })
      const { written } = await transmit({ version, clientId, transactionalId, topicData })
      const slack = planOf(budget, clientId, transactionalId, topicData) - written[0].length
      const allowed = 32 + 26 * topicData.length + 8 * partitions + 21 * records
      outcomes.push(
        slack >= 0 && slack <= allowed ? 'within' : `seed ${seed}: ${slack} of ${allowed}`
      )
    }

    expect(outcomes.filter((outcome) => outcome !== 'within')).toEqual([])
  })

  test.each([
    ['gzip', Types.GZIP, [undefined, -1, 1, 9]],
    ['zstd', Types.ZSTD, [undefined, -131072, 1, 22]],
    ['lz4', Types.LZ4, [undefined]],
    ['snappy', Types.Snappy, [undefined]],
  ])(
    'the allowance of %s bounds what it writes for incompressible records',
    async (_, type, levels) => {
      const codec = Codecs[type]()
      const over = []
      for (const n of [0, 1, 1000, 32767, 32768, 32769, 65536, 65537, 131072, 1048576]) {
        const input = crypto.randomBytes(n)
        for (const level of levels) {
          const output = await codec.compress(
            { buffer: input },
            level === undefined ? {} : { level }
          )
          if (output.length > encodedBound(type, n)) over.push({ n, level, written: output.length })
        }
      }

      expect(over).toEqual([])
    }
  )

  test('the allowances are the codecs’ own worst cases, and nothing for no codec or a plugin', () => {
    const saved = Codecs[Types.GZIP]
    try {
      expect([0, 4096, 1048576].map((n) => encodedBound(Types.GZIP, n))).toEqual([
        25,
        4122,
        1048576 + 256 + 64 + 25,
      ])
      expect([0, 131072, 1048576].map((n) => encodedBound(Types.ZSTD, n))).toEqual([
        64,
        131072 + 512,
        1048576 + 4096,
      ])
      expect([0, 65536, 65537].map((n) => encodedBound(Types.LZ4, n))).toEqual([
        27,
        65536 + 8 + 27,
        65537 + 16 + 27,
      ])
      expect(encodedBound(Types.Snappy, 32768)).toBe(16 + 32768 + 12 + 1024)
      expect([encodedBound(Types.None, 1000), encodedBound(undefined, 1000)]).toEqual([1000, 1000])
      Codecs[Types.GZIP] = () => ({ compress: async ({ buffer }) => buffer })
      expect(encodedBound(Types.GZIP, 1000)).toBe(1000)
    } finally {
      Codecs[Types.GZIP] = saved
    }
  })

  test('framing counts the length prefix, the header and the identifiers, the connection’s default client id when absent', () => {
    expect(framing()).toBe(32 + 'kafkajs'.length)
    expect(framing(null, null)).toBe(32)
    expect(framing('é', 'tx')).toBe(32 + 2 + 2)
    expect(topicFraming('topic')).toBe(37)
    expect(batchBytes([{ value: null }, { key: 'k', value: 'v' }])).toBe(61 + 24 + 26)
    expect(minimumRequestSize('client', 'tx')).toBe(32 + 6 + 2 + 61 + 24)
  })

  test('the chunk capacity is the largest record payload whose request alone fits, -1 when none does, unlimited without a cap', () => {
    const budget = new RequestBudget({ maxRequestSize: 10000, clientId: 'c' })
    const fixed = framing('c') + topicFraming('t') + 16 + 61
    for (const type of [Types.None, Types.GZIP, Types.ZSTD, Types.LZ4, Types.Snappy]) {
      const capacity = budget.chunkCapacity('t', type)
      expect(fixed + encodedBound(type, capacity)).toBeLessThanOrEqual(10000)
      expect(fixed + encodedBound(type, capacity + 1)).toBeGreaterThan(10000)
      expect(budget.chunkCapacity('t', type)).toBe(capacity)
    }
    expect(budget.chunkCapacity('t', Types.None)).toBe(10000 - fixed)
    expect(budget.maxRequestBytes).toBe(10000 - framing('c'))
    expect(new RequestBudget({ maxRequestSize: fixed - 1, clientId: 'c' }).chunkCapacity('t')).toBe(
      -1
    )
    expect(new RequestBudget().chunkCapacity('t', Types.GZIP)).toBe(Infinity)
    expect(new RequestBudget().maxRequestBytes).toBe(Infinity)
  })

  describe('kafka.producer({ maxRequestSize })', () => {
    const kafka = (clientId) => new Kafka({ clientId, brokers: ['localhost:9092'], logLevel: 0 })

    test('takes an integer from the minimum for its identifiers to 2147483647, the default included', () => {
      const minimum = minimumRequestSize('sizes', 'tx')
      expect(() =>
        kafka('sizes').producer({ transactionalId: 'tx', maxRequestSize: minimum })
      ).not.toThrow()
      expect(() => kafka('sizes').producer({ maxRequestSize: 2147483647 })).not.toThrow()
      expect(() => kafka('sizes').producer({})).not.toThrow()
    })

    test.each([
      ['one byte below the minimum', minimumRequestSize('sizes', 'tx') - 1],
      ['above 2147483647', 2147483648],
      ['fractional', 1048576.5],
      ['not a number', '1048576'],
      ['NaN', NaN],
      ['Infinity', Infinity],
    ])('refuses a maxRequestSize %s, naming the minimum', (_, maxRequestSize) => {
      expect(() => kafka('sizes').producer({ transactionalId: 'tx', maxRequestSize })).toThrow(
        KafkaJSInvalidConfigurationError
      )
      expect(() => kafka('sizes').producer({ transactionalId: 'tx', maxRequestSize })).toThrow(
        `maxRequestSize must be an integer from ${minimumRequestSize('sizes', 'tx')} `
      )
    })

    test('counts a long client id and transactional id in the minimum', () => {
      const clientId = 'c'.repeat(600000)
      const transactionalId = 't'.repeat(600000)
      expect(() => kafka(clientId).producer({ transactionalId })).toThrow(
        `maxRequestSize must be an integer from ${minimumRequestSize(clientId, transactionalId)} `
      )
    })
  })
})
