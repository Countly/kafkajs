const { Kafka } = require('../../../index')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { WireOrder } = require('../../../testHelpers/wireOrder')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')
/** Advances transport callbacks. @returns {Promise<void>} completion; @throws Never. */
const turn = () => new Promise((resolve) => setImmediate(resolve))
/** Pumps finite transport turns. @param {Function} ready predicate @returns {Promise<void>} completion @throws {Error} on a missing transition. */
const until = async (ready) => {
  for (let i = 0; i < 200 && !ready(); i++) {
    if (typeof setTimeout.clock === 'object') await jest.advanceTimersByTimeAsync(1)
    else await new Promise((resolve) => setTimeout(resolve, 1))
    await turn()
  }
  expect(ready()).toBe(true)
}
/** Watches settlement. @param {Promise<*>} promise operation @returns {object} state @throws Never. */
const watch = (promise) => {
  const result = { status: 'pending' }
  promise.then(
    (value) => Object.assign(result, { status: 'fulfilled', value }),
    (error) => Object.assign(result, { status: 'rejected', error })
  )
  return result
}

const ProducerStateManager = require('../state')
/**
 * Attempts every cleanup action and unconditionally restores global test state.
 * @param {Error|undefined} primary original test failure
 * @param {Function[]} actions independent resource cleanup attempts
 * @param {Function} [restore] final synchronous restoration, even after cleanup failures
 * @returns {Promise<void>} after all actions and restoration were attempted
 * @throws {Error|AggregateError} original with cleanupErrors, or cleanup failures alone
 */
const finish = async (primary, actions, restore = () => {}) => {
  const failures = []
  try {
    for (const action of actions) {
      try {
        await action()
      } catch (error) {
        failures.push(error)
      }
    }
  } finally {
    try {
      restore()
    } catch (error) {
      failures.push(error)
    }
  }
  if (primary) {
    if (failures.length) primary.cleanupErrors = failures
    throw primary
  }
  if (failures.length) throw new AggregateError(failures, 'Resource cleanup failed')
}

describe.each(['default', 'tv1', 'tv2'])('Cross-request responses under %s', (profile) => {
  it('keeps each partition acknowledgement and next sequence after reversed answers', async () => {
    const cluster = producerScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }], profile }),
      { partitions: 2 }
    )
    const producer = new Kafka(cluster.kafkaOptions({ retry: { retries: 1 } })).producer({
      idempotent: true,
      linger: 0,
      ...(profile === 'default' ? {} : { transactionalId: 'reorder' }),
    })
    const gates = [0, 1].map((partition) =>
      cluster.schedule.hold({
        api: 'Produce',
        match: (call) =>
          call.fields.topics.some((t) => t.partitions.some((p) => p.partition === partition)),
      })
    )
    let manager
    const enqueue = ProducerStateManager.prototype.enqueue
    const spy = jest.spyOn(ProducerStateManager.prototype, 'enqueue').mockImplementation(function (
      ...args
    ) {
      manager = this
      return enqueue.apply(this, args)
    })
    const pending = []
    const settlements = []
    let primary
    try {
      await producer.connect()
      const sender = profile === 'default' ? producer : await producer.transaction()
      const send = (partition) =>
        sender.send({
          topic: TOPIC,
          messages: [{ partition, value: String(partition), timestamp: 1000 }],
        })
      pending.push(send(0))
      const first = watch(pending[0])
      settlements.push(first)
      await until(() => gates[0].call !== null)
      pending.push(send(1))
      const second = watch(pending[1])
      settlements.push(second)
      await until(() => gates[1].call !== null)
      const answer = (partition, baseOffset) => ({
        responses: [
          {
            ...gates[partition].call.fields.topics[0],
            partitions: [
              {
                partition,
                errorCode: 0,
                baseOffset,
                logAppendTime: '-1',
                logStartOffset: '0',
                leader: null,
              },
            ],
          },
        ],
        nodeEndpoints: [],
      })
      gates[1].release(answer(1, '202'))
      await until(() => second.status !== 'pending')
      expect(first.status).toBe('pending')
      expect(second.error).toBeUndefined()
      expect(second).toMatchObject({
        status: 'fulfilled',
        value: [{ partition: 1, baseOffset: '202' }],
      })
      gates[0].release(answer(0, '101'))
      await Promise.all(pending)
      expect(first).toMatchObject({
        status: 'fulfilled',
        value: [{ partition: 0, baseOffset: '101' }],
      })
      expect([0, 1].map((p) => manager.sequencer.entry(TOPIC, p).lastAckedOffset)).toEqual([
        101, 202,
      ])
      await send(0)
      await send(1)
      const wire = WireOrder.of(cluster)
      expect(wire.produced().map(({ partition, sequence }) => [partition, sequence])).toEqual([
        [0, 0],
        [1, 0],
        [0, 1],
        [1, 1],
      ])
      wire.assertNoRewrite()
      wire.assertSequencesInOrder()
      wire.assertNoStaleGeneration()
      if (profile !== 'default') await sender.abort()
    } catch (error) {
      primary = error
    } finally {
      await finish(primary, [
        () => {
          cluster.schedule.rules.length = 0
        },
        ...gates.map((gate) => () => {
          if (gate.settle) gate.fail(new Error('Fixture closing'))
        }),
        async () => {
          const close = producer.disconnect()
          const closed = watch(close)
          await until(() => closed.status !== 'pending')
          await close
        },
        async () => {
          await until(() => settlements.every(({ status }) => status !== 'pending'))
          await Promise.allSettled(pending)
        },
        () => cluster.assertClean(),
        () => spy.mockRestore(),
      ])
    }
  })
})
