const createProducer = require('../../producer')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { lanes } = require('../../../testHelpers/lane')
const { CALLS, LIMIT, overload } = require('./fixtures/inFlightOverload')
const {
  connectionOpts,
  createCluster,
  createTopic,
  newLogger,
  secureRandom,
  writeEvidence,
} = require('testHelpers')

/** No records or commits; a held limit and every call answered. */
describe.each(lanes())('Network > maxInFlightRequests > an overload on %s', (lane) => {
  test(`${CALLS} concurrent Metadata calls keep at most ${LIMIT} in flight and all resolve`, async () => {
    let outcome
    if (lane === 'fake') {
      const cluster = createScriptedCluster({ nodes: [{ id: 0, host: 'node', port: 9092 }] })
      outcome = await overload({
        host: 'node',
        port: 9092,
        socketFactory: cluster.socketFactory,
        topics: [],
      })
      cluster.assertClean()
    } else {
      const topic = `e2e-p9a-max-in-flight-${secureRandom()}`
      await createTopic({ topic })
      outcome = await overload({
        host: connectionOpts().host,
        port: 9092,
        socketFactory: connectionOpts().socketFactory,
        topics: [topic],
      })
      writeEvidence('max-in-flight', { calls: CALLS, maxInFlightRequests: LIMIT, ...outcome })
    }

    expect(outcome.resolved).toBe(CALLS)
    expect(outcome.maxInFlight).toBeLessThanOrEqual(LIMIT)
    expect(outcome.maxInFlight).toBeGreaterThan(1)
    expect(outcome.written).toBeGreaterThanOrEqual(CALLS)
  }, 60000)

  const live = lane === 'docker' ? test : test.skip
  live(
    'produces correctly with the bounded default and with null',
    async () => {
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
    },
    60000
  )
})
