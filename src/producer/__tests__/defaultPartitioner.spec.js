const { webcrypto } = require('crypto')
const { Kafka, Partitioners } = require('../../../index')
const { LEVELS } = require('../../loggers')
const { mulberry32 } = require('../../../testHelpers/schedule')
const { bufferedProducer, deferred, flushAll } = require('./fixtures/bufferHarness')
const { createFakeCluster } = require('./fixtures/fakeCluster')

const TOPIC = 'topic-a'

/** The partitions the fake broker was asked to write, in request order. */
const written = (cluster) =>
  cluster.broker.produce.mock.calls.flatMap(([{ topicData }]) =>
    topicData.flatMap(({ partitions }) => partitions.map(({ partition }) => partition))
  )

describe('Producer > the default partitioner', () => {
  afterEach(() => jest.restoreAllMocks())

  test('with no createPartitioner, keyless records stick to one partition until batchSize bytes have gone to it', async () => {
    jest.spyOn(Math, 'random').mockImplementation(mulberry32(3))
    const { producer, cluster } = bufferedProducer({ partitions: 3, batchSize: 4096 })
    await producer.connect()

    // 1025 wire bytes a record: the fourth reaches 4096 and the fifth moves on
    for (let i = 0; i < 8; i += 1) {
      await producer.send({ topic: TOPIC, messages: [{ value: 'x'.repeat(1000) }] })
    }

    const order = written(cluster)
    expect(new Set(order.slice(0, 4)).size).toBe(1)
    expect(new Set(order.slice(4)).size).toBe(1)
    expect(order[4]).not.toBe(order[0])
    await producer.disconnect()
  })

  test('DefaultPartitioner, and its alias JavaCompatiblePartitioner, keep the round robin of every keyless record', async () => {
    for (const createPartitioner of [
      Partitioners.DefaultPartitioner,
      Partitioners.JavaCompatiblePartitioner,
    ]) {
      const { producer, cluster } = bufferedProducer({ partitions: 3, createPartitioner })
      await producer.connect()

      for (let i = 0; i < 6; i += 1) {
        await producer.send({ topic: TOPIC, messages: [{ value: 'x' }] })
      }

      const order = written(cluster)
      expect(order.slice(1).map((partition, i) => (partition - order[i] + 3) % 3)).toEqual([
        1, 1, 1, 1, 1,
      ])
      await producer.disconnect()
    }
  })

  test('DefaultPartitioner is the round robin of the legacy partitioner: one counter per topic, over the partitions with a leader', () => {
    jest.spyOn(webcrypto, 'getRandomValues').mockImplementation((buffer) => buffer.fill(0))
    const partitioner = Partitioners.DefaultPartitioner()
    const send = (partitionMetadata) =>
      partitioner({ topic: TOPIC, partitionMetadata, message: { value: 'v' } })
    const all = [0, 1, 2, 3].map((partitionId) => ({ partitionId, leader: 0 }))
    const some = [0, 1, 2, 3].map((partitionId) => ({
      partitionId,
      leader: partitionId % 2 ? 0 : -1,
    }))

    // the counter starts at 0 and moves on before each pick
    expect([send(all), send(all), send(all), send(all)]).toEqual([1, 2, 3, 0])
    expect([send(some), send(some)]).toEqual([3, 1])
  })

  test('10 000 seeded keys over 1 to 64 partitions map the same under the new default and DefaultPartitioner', () => {
    const random = mulberry32(7)
    const sticky = Partitioners.UniformStickyPartitioner({ stickyBytes: 16384 })()
    const byDefault = Partitioners.DefaultPartitioner()
    const differing = []

    for (let i = 0; i < 10000; i += 1) {
      const count = 1 + (i % 64)
      const partitionMetadata = Array.from({ length: count }, (_, partitionId) => ({
        partitionId,
        leader: 0,
      }))
      const key =
        i % 2 === 0
          ? String(Math.floor(random() * 1e9))
          : Buffer.from(Array.from({ length: 1 + (i % 17) }, () => Math.floor(random() * 256)))
      const args = { topic: TOPIC, partitionMetadata, message: { key, value: 'v' } }
      if (sticky(args) !== byDefault(args)) differing.push(i)
    }

    expect(differing).toEqual([])
  })

  test("the producer's default partitioner is adaptive", async () => {
    const held = deferred()
    let calls = 0
    const cluster = createFakeCluster({
      partitions: 2,
      produce: async ({ topicData }) => {
        if ((calls += 1) === 1) await held.promise
        return {
          topics: topicData.map(({ topic, partitions }) => ({
            topicName: topic,
            partitions: partitions.map(({ partition }) => ({
              partition,
              errorCode: 0,
              baseOffset: '0',
              logAppendTime: '-1',
              logStartOffset: '0',
            })),
          })),
        }
      },
    })
    const { producer } = bufferedProducer({ cluster })
    await producer.connect()

    // partition 0's broker answers slowly: one batch waits for it, none for partition 1
    const sends = [producer.send({ topic: TOPIC, messages: [{ value: 'a', partition: 0 }] })]
    try {
      await flushAll()
      expect(written(cluster)).toEqual([0])

      // weights 1 and 2, a draw of 0.4 * 3 picks partition 1; a uniform pick would take 0
      jest.spyOn(Math, 'random').mockReturnValue(0.4)
      sends.push(producer.send({ topic: TOPIC, messages: [{ value: 'b' }] }))
      await flushAll()
      expect(written(cluster)).toEqual([0, 1])
    } finally {
      held.resolve()
      await Promise.allSettled(sends)
      await producer.disconnect()
    }
    await expect(Promise.all(sends)).resolves.toHaveLength(2)
  })

  test('an explicit partition goes where it says under the new default', async () => {
    const { producer, cluster } = bufferedProducer({ partitions: 3 })
    await producer.connect()

    await producer.send({
      topic: TOPIC,
      messages: [
        { value: 'a', partition: 2 },
        { value: 'b', partition: 0 },
      ],
    })

    expect(written(cluster).sort()).toEqual([0, 2])
    await producer.disconnect()
  })

  test.each([
    ['without KAFKAJS_NO_PARTITIONER_WARNING', undefined],
    ['with KAFKAJS_NO_PARTITIONER_WARNING', '1'],
  ])('a producer created without createPartitioner logs no partitioner warning, %s', (_, value) => {
    const saved = process.env.KAFKAJS_NO_PARTITIONER_WARNING
    const logs = []
    try {
      if (value === undefined) delete process.env.KAFKAJS_NO_PARTITIONER_WARNING
      else process.env.KAFKAJS_NO_PARTITIONER_WARNING = value
      const kafka = new Kafka({
        clientId: 'warnings',
        brokers: ['localhost:9092'],
        logLevel: LEVELS.DEBUG,
        logCreator: () => (entry) => logs.push(entry),
      })

      kafka.producer()
      kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })
    } finally {
      if (saved === undefined) delete process.env.KAFKAJS_NO_PARTITIONER_WARNING
      else process.env.KAFKAJS_NO_PARTITIONER_WARNING = saved
    }

    expect(logs.filter(({ log }) => /partitioner/i.test(log.message))).toEqual([])
  })
})
