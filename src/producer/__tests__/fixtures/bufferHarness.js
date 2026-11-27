const v8 = require('v8')
const vm = require('vm')
const createProducer = require('../../index')
const { Admission } = require('../../state/admission')
const { createFakeCluster } = require('./fakeCluster')
const { createLogger, LEVELS } = require('../../../loggers')
const Decoder = require('../../../protocol/decoder')
const decodeRecordBatch = require('../../../protocol/recordBatch/v0/decoder')
const buildRecordBatch = require('../../../protocol/requests/produce/buildRecordBatch')
const { Types: Compression } = require('../../../protocol/message/compression')

/**
 * A batch's records, for a test that builds a batch itself, in the form batches hold them.
 * @param {object[]} messages the records as a caller passes them to `send()`
 * @returns {object[]} the records a batch's `messages` field holds
 */
const owned = (messages) => messages

/**
 * @typedef {{ topicData?: { topic: string, partitions: { partition: number,
 *   firstSequence?: number, messages: object[] }[] }[], batches?: { topic: string,
 *   partition: number, firstSequence?: number, messages: object[] }[] }} SentRequest a Produce
 *   request (`topicData`) or an accumulator hand-off (`batches`)
 */

/**
 * @typedef {{ topic: string, partition: number, firstSequence: number, records: {
 *   key: Buffer|null, value: Buffer|null, headers: object, timestamp: string, offset: string }[]
 *   }} ReadBatch a batch as a broker reads it; `offset` is the record's position in its batch
 */

/**
 * A batch's place and its own copy of its record list.
 * @param {string} topic
 * @param {{ partition: number, firstSequence?: number, messages: object[] }} batch
 * @returns {{ topic: string, partition: number, firstSequence: number|undefined,
 *   messages: object[] }}
 */
const placed = (topic, { partition, firstSequence, messages }) => {
  const copy = new Array(messages.length)
  for (let at = 0; at < messages.length; at++) copy[at] = messages[at]
  return { topic, partition, firstSequence, messages: copy }
}

/**
 * The batches a request carries, read synchronously, in request order, each with its own copy of
 * its record list. The one place a fixture reads a request's records, for a fake broker or probe
 * that decides or labels by them at the instant the request arrives. It is plain loops and
 * property reads, no built-in method, so a spec that spies on one sees only the producer's calls.
 * @param {SentRequest} request
 * @returns {{ topic: string, partition: number, firstSequence: number|undefined,
 *   messages: object[] }[]}
 */
const batchesOf = (request) => {
  const taken = []
  if (request.batches) {
    for (let at = 0; at < request.batches.length; at++) {
      taken[taken.length] = placed(request.batches[at].topic, request.batches[at])
    }
    return taken
  }
  for (let at = 0; at < request.topicData.length; at++) {
    const { topic, partitions } = request.topicData[at]
    for (let index = 0; index < partitions.length; index++) {
      taken[taken.length] = placed(topic, partitions[index])
    }
  }
  return taken
}

/**
 * Reads one record batch with the consumer's batch decoder, CRC included.
 * @param {Buffer} buffer the batch's bytes
 * @returns {Promise<{ firstSequence: number, records: ReadBatch['records'] }>}
 * @throws {import('../../../errors').KafkaJSCorruptRecordError} for bytes that are not a valid
 *   batch, such as a payload byte changed under its CRC
 */
const readBatch = async (buffer) => {
  const { firstSequence, records } = await decodeRecordBatch(new Decoder(buffer), {
    maxDecompressedBatchBytes: Number.MAX_SAFE_INTEGER,
  })
  return {
    firstSequence,
    records: records.map(({ key, value, headers, timestamp, offset }) => ({
      key,
      value,
      headers,
      timestamp,
      offset,
    })),
  }
}

/**
 * Takes what a request carries at this instant without waiting: each batch's place, the request's
 * first sequence, its record count and its own copy of the record list (`batchesOf`), and nothing
 * else. Writing and decoding run only in `read()`, so a fake broker or probe that takes a request
 * decides, records and answers at the same instant, and calls the same built-ins, as one that
 * reads nothing. A later change to a batch's record list is not seen; the records themselves are
 * the producer's own and are read when `read()` runs, so a test does not change them after the
 * call.
 *
 * What a read checks is the record content: keys, values, headers, timestamps, order and offsets,
 * written by the producer's batch writer and read through a CRC-checked decode, and that the bytes
 * hold `count` records. The batch is written uncompressed with the default producer id and epoch
 * and no transaction flag, so it says nothing of the request's envelope, compression, producer
 * identity or transaction attribute; tests check those on the request or the wire.
 * @param {SentRequest} request
 * @returns {{ batches: { topic: string, partition: number, firstSequence: number|undefined,
 *   count: number }[], read: () => Promise<ReadBatch[]> }} `read` settles once and rejects with
 *   what writing or decoding threw
 */
const takeSent = (request) => {
  const taken = batchesOf(request)
  const batches = new Array(taken.length)
  for (let at = 0; at < taken.length; at++) {
    const { topic, partition, firstSequence, messages } = taken[at]
    batches[at] = { topic, partition, firstSequence, count: messages.length }
  }
  let reading = null
  const read = () =>
    (reading ??= Promise.all(
      taken.map(async ({ messages, firstSequence }, at) => {
        const { topic, partition, count } = batches[at]
        const { buffer } = await buildRecordBatch({
          compression: Compression.None,
          messages,
          firstSequence,
        })
        const { firstSequence: written, records } = await readBatch(buffer)
        if (records.length !== count) {
          throw new Error(
            `${topic}/${partition} held ${count} records, its bytes ${records.length}`
          )
        }
        return { topic, partition, firstSequence: written, records }
      })
    ))
  return { batches, read }
}

/**
 * The records a broker reads from a request, taken now (`takeSent`) and decoded.
 * @param {SentRequest} request
 * @returns {Promise<ReadBatch[]>} one entry per batch, in request order
 */
const sentRecords = (request) => takeSent(request).read()

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
  owned,
  batchesOf,
  readBatch,
  takeSent,
  sentRecords,
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
