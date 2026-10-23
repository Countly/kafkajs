const { Kafka } = require('../../../index')
const { KafkaJSDeliveryTimeout } = require('../../errors')
const { Schedule } = require('../../../testHelpers/schedule')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { WireOrder } = require('../../../testHelpers/wireOrder')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')

describe('Non-idempotent producer delivery expiry', () => {
  let producer, cluster, gate, pending, observation

  beforeEach(async () => {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] })
    const schedule = new Schedule()
    gate = schedule.hold({ api: 'Produce' })
    cluster = producerScript(
      createScriptedCluster({
        nodes: [{ id: 0, host: 'node0', port: 9092 }],
        schedule,
      })
    )
    producer = new Kafka({
      ...cluster.kafkaOptions(),
      requestTimeout: 1000,
      enforceRequestTimeout: false,
    }).producer({ idempotent: false, linger: 0, deliveryTimeout: 1000 })
    await producer.connect()
    let state = 'pending'
    let error
    pending = producer
      .send({ topic: TOPIC, messages: [{ value: 'deadline', timestamp: 1000 }] })
      .then(
        () => {
          state = 'fulfilled'
        },
        (failure) => {
          if (!(failure instanceof KafkaJSDeliveryTimeout)) throw failure
          error = failure
          state = 'rejected'
        }
      )
    for (let i = 0; i < 100 && !gate.call; i += 1) {
      await jest.advanceTimersByTimeAsync(1)
      await new Promise((resolve) => setImmediate(resolve))
    }
    expect(gate.call).not.toBeNull()
    await gate.arrived()
    cluster.mark('answer held')
    await jest.advanceTimersByTimeAsync(1000)
    const atDeadline = state
    await jest.advanceTimersByTimeAsync(4000)
    observation = { atDeadline, atFiveSeconds: state, error: error?.name, outcome: error?.outcome }
    gate.release()
    await pending
    observation.afterLateSuccess = state
    await new Promise((resolve) => setImmediate(resolve))
    expect(cluster.requests().filter(({ api }) => api === 'Produce')).toHaveLength(1)
    WireOrder.of(cluster).assertSequencesInOrder()
  })

  afterEach(async () => {
    if (gate?.settle) gate.release()
    if (pending) await pending
    if (producer) await producer.disconnect()
    if (cluster) cluster.assertClean()
    jest.useRealTimers()
  })

  it.failing('settles an on-wire send at its delivery deadline and ignores a late success', () => {
    expect(observation).toEqual({
      atDeadline: 'rejected',
      atFiveSeconds: 'rejected',
      error: 'KafkaJSDeliveryTimeout',
      outcome: 'unknown',
      afterLateSuccess: 'rejected',
    })
  })

  it('runs a complete held-answer scenario through the public producer', () => {
    const produce = cluster.timeline().filter(({ api }) => api === 'Produce')
    expect(produce.map(({ kind }) => kind)).toEqual(['request', 'response'])
  })
})
