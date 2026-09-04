const createProducer = require('../../producer')
const { secureRandom, createCluster, createTopic, newLogger } = require('testHelpers')

describe('Network > maxInFlightRequests > against a live broker', () => {
  it('produces correctly with the bounded default and with null', async () => {
    const topic = `test-topic-${secureRandom()}`
    await createTopic({ topic, partitions: 1 })

    for (const maxInFlightRequests of [undefined, null, 1]) {
      const producer = createProducer({
        cluster: createCluster({ maxInFlightRequests }),
        logger: newLogger(),
      })
      await producer.connect()

      const sends = Array.from({ length: 20 }, (_, i) =>
        producer.send({ topic, messages: [{ key: `k-${i}`, value: `v-${i}` }] })
      )
      const results = await Promise.all(sends)
      expect(results).toHaveLength(20)

      await producer.disconnect()
    }
  }, 60000)
})
