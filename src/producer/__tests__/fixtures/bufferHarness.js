const v8 = require('v8')
const vm = require('vm')
const createProducer = require('../../index')
const { Admission } = require('../../state/admission')
const { createFakeCluster } = require('./fakeCluster')
const { createLogger, LEVELS } = require('../../../loggers')

/**
 * Creates a promise whose owner can resolve or reject it explicitly.
 * @returns {{ promise: Promise<unknown>, resolve: (value: unknown) => void,
 *   reject: (error: unknown) => void }} the promise and its settlement callbacks
 */
const deferred = () => {
  const handle = {}
  handle.promise = new Promise((resolve, reject) => {
    handle.resolve = resolve
    handle.reject = reject
  })
  return handle
}

/**
 * Observes a promise without awaiting it; rejection is stored as a value, not rethrown.
 * @param {Promise<unknown>} promise the operation to observe
 * @returns {{ state: string, value: unknown }} mutable state, initially pending
 */
const watch = (promise) => {
  const watched = { state: 'pending', value: undefined }
  promise.then(
    (value) => Object.assign(watched, { state: 'fulfilled', value }),
    (value) => Object.assign(watched, { state: 'rejected', value })
  )
  return watched
}

/**
 * Lets every queued callback run, and under Jest's fake timers every timer due now: the send path
 * hands a batch on through a timer of no delay.
 * @returns {Promise<void>} resolves after the queued work and due fake timers run
 */
const flushAll = async () => {
  for (let i = 0; i < 5; i += 1) {
    await new Promise((resolve) => setImmediate(resolve))
    if (typeof setTimeout.clock === 'object') {
      await jest.advanceTimersByTimeAsync(0)
    }
  }
}

/**
 * Runs five full collections on separate event-loop turns so weak references can clear.
 * @returns {Promise<void>} resolves after the final collection
 */
const collect = async () => {
  v8.setFlagsFromString('--expose-gc')
  const gc = vm.runInNewContext('gc')
  for (let pass = 0; pass < 5; pass += 1) {
    await new Promise((resolve) => setImmediate(resolve))
    gc()
  }
}

/**
 * Captures the buffer's four accounting counters without changing them.
 * @param {Admission} admission the buffer to inspect
 * @returns {{ available: number, reserved: number, waitingBytes: number, debts: number }}
 */
const countersOf = (admission) => ({
  available: admission.available,
  reserved: admission.reserved,
  waitingBytes: admission.waitingBytes,
  debts: admission.debts,
})

/**
 * Builds the expected counters for a buffer with no outstanding owners or waiters.
 * @param {Admission} admission the buffer whose capacity is expected to be free
 * @returns {{ available: number, reserved: number, waitingBytes: number, debts: number }}
 */
const quiescent = (admission) => ({
  available: admission.capacity,
  reserved: 0,
  waitingBytes: 0,
  debts: 0,
})

/**
 * A producer over a fake cluster (`fixtures/fakeCluster.js`) with an `Admission` of its own, so a
 * spec reads the buffer's counters: `linger: 0` and a 1000 ms delivery deadline unless told
 * otherwise.
 * @param {object} [options]
 * @param {number} [options.capacity]
 * @param {number} [options.maxBlockMs]
 * @param {object} [options.cluster] a fake cluster; a new one with `partitions` when absent
 * @param {number} [options.partitions]
 * @returns {{ producer: object, cluster: object, admission: Admission }}
 */
const bufferedProducer = ({
  capacity = 1048576,
  maxBlockMs = 60000,
  partitions = 1,
  cluster = createFakeCluster({ partitions }),
  ...options
} = {}) => {
  const admission = new Admission({ capacity, maxBlockMs })
  const producer = createProducer({
    cluster,
    logger: createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} }),
    linger: 0,
    deliveryTimeout: 1000,
    admission,
    ...options,
  })
  return { producer, cluster, admission }
}

/**
 * The answer of the fake broker to a Produce request: success for every partition it carries.
 * @param {{ topicData: { topic: string, partitions: { partition: number }[] }[] }} request
 * @returns {object[]}
 */
const succeeded = ({ topicData }) =>
  topicData.flatMap(({ topic, partitions }) =>
    partitions.map(({ partition }) => ({
      topicName: topic,
      partition,
      errorCode: 0,
      error: null,
      baseOffset: '0',
      logAppendTime: '-1',
      logStartOffset: '0',
    }))
  )

/**
 * A transactional producer over a fake cluster, inside a transaction, whose Produce answers wait
 * until `release()` answers every request sent so far, on transaction version 2; InitProducerId
 * bumps the epoch, so an abort that has to bump it works.
 * @param {object} [options] as `bufferedProducer`
 * @returns {Promise<{ producer: object, cluster: object, admission: object, transaction: object,
 *   release: () => Promise<void> }>}
 */
const transactionalProducer = async (options = {}) => {
  const answers = []
  const fixture = bufferedProducer({
    transactionalId: 'tx-buffer',
    idempotent: true,
    deliveryTimeout: 60000,
    ...options,
  })
  const { broker } = fixture.cluster
  broker.initProducerId.mockImplementation(async ({ producerEpoch = -1 }) => ({
    producerId: '1000',
    producerEpoch: producerEpoch + 1,
  }))
  broker.produce.mockImplementation(async (request) => {
    const held = deferred()
    answers.push(held)
    await held.promise
    return succeeded(request)
  })
  broker.endTxn.mockImplementation(async () => ({ producerId: '-1', producerEpoch: -1 }))
  Object.assign(broker, {
    finalizedFeatures: { 'transaction.version': { minVersionLevel: 0, maxVersionLevel: 2 } },
    finalizedFeaturesEpoch: 1,
    negotiatedVersion: () => 5,
    addOffsetsToTxn: jest.fn(async () => ({})),
    txnOffsetCommit: jest.fn(async () => ({})),
  })
  await fixture.producer.connect()
  const transaction = await fixture.producer.transaction()
  const release = async () => {
    await flushAll()
    while (answers.length > 0) {
      answers.shift().resolve()
      await flushAll()
    }
  }
  return { ...fixture, transaction, release }
}

module.exports = {
  succeeded,
  transactionalProducer,
  deferred,
  watch,
  flushAll,
  collect,
  countersOf,
  quiescent,
  bufferedProducer,
}
