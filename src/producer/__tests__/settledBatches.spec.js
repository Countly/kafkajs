const v8 = require('v8')
const vm = require('vm')
const createProducer = require('../index')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { newLogger } = require('testHelpers')

const TOPIC = 'topic-a'

/**
 * Sends one record and returns a weak reference to its value. The value lives only in this call's
 * frame, so the test holds nothing that keeps it alive.
 * @param {ReturnType<typeof createProducer>} producer
 * @param {number} index
 * @returns {Promise<WeakRef<Buffer>>}
 */
const sendRecord = async (producer, index) => {
  const value = Buffer.from(`record-${index}`)
  await producer.send({ topic: TOPIC, messages: [{ value }] })
  return new WeakRef(value)
}

describe('Producer > the idempotent send path does not keep settled batches', () => {
  it('lets the records of acknowledged sends be collected while the producer lives on', async () => {
    v8.setFlagsFromString('--expose-gc')
    const gc = vm.runInNewContext('gc')
    const cluster = createFakeCluster()
    const producer = createProducer({ cluster, logger: newLogger() })
    await producer.connect()

    const refs = []
    for (let index = 0; index < 30; index += 1) {
      refs.push(await sendRecord(producer, index))
      // a jest.fn keeps every request it was called with, which would hold the records itself
      cluster.broker.produce.mockClear()
    }
    // the next send is what makes the send path forget the batches settled before it
    await producer.send({ topic: TOPIC, messages: [{ value: 'last' }] })
    await new Promise((resolve) => setImmediate(resolve))
    gc()

    expect(refs.filter((ref) => ref.deref() !== undefined)).toHaveLength(0)
    await producer.disconnect()
  })
})
