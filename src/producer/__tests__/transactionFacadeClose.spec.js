const { bufferedProducer } = require('./fixtures/bufferHarness')
const ProducerStateManager = require('../state')

/**
 * Runs `fn` once `count` promise continuations have passed: the producer's own awaits between
 * the begin call returning and `transaction()` resuming.
 * @param {number} count empty continuations to let pass first
 * @param {() => void} fn
 * @returns {Promise<void>}
 */
const later = (count, fn) => {
  let next = Promise.resolve()
  for (let i = 0; i < count; i += 1) next = next.then(() => {})
  return next.then(fn)
}

describe('transaction facade after a forced close', () => {
  test.each([1, 2])(
    'a close between the begin and the facade rejects the start, transaction version %i',
    async (version) => {
      const { producer, cluster } = bufferedProducer({ transactionalId: 'facade-close' })
      Object.assign(cluster.broker, {
        finalizedFeatures: { 'transaction.version': { maxVersionLevel: version } },
        finalizedFeaturesEpoch: 1,
        negotiatedVersion: () => 5,
      })
      await producer.connect()

      let state
      let stateAtClose
      let closing
      const transition = ProducerStateManager.prototype.transitionTo
      const observation = jest
        .spyOn(ProducerStateManager.prototype, 'transitionTo')
        .mockImplementation(function (...args) {
          const result = transition.apply(this, args)
          state = this.state()
          return result
        })

      // The second lookup is the begin's, after initialization; the close lands once the manager
      // is in the transaction but `transaction()` has not resumed yet.
      let lookups = 0
      cluster.findGroupCoordinator.mockImplementation(async () => {
        lookups += 1
        if (lookups === 2) {
          later(2, () => {
            stateAtClose = state
            closing = producer.disconnect({ timeout: 0 })
          })
        }
        return cluster.broker
      })

      try {
        const outcome = await producer.transaction().then(
          (transaction) => ({ transaction }),
          (error) => ({ error })
        )
        await closing
        expect(stateAtClose).toBe('IN_TRANSACTION')
        expect(outcome.error?.name).toBe('KafkaJSProducerClosedError')
      } finally {
        observation.mockRestore()
        await producer.disconnect({ timeout: 0 })
      }
    }
  )
})
