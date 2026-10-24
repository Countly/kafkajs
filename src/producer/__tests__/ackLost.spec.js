const { Kafka } = require('../../../index')
const {
  createScriptedCluster,
  groupScript,
  producerScript,
} = require('../../../testHelpers/scriptedCluster')
const { WireOrder } = require('../../../testHelpers/wireOrder')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')
const { createErrorFromCode } = require('../../protocol/error')
const { KafkaJSProtocolError, KafkaJSConnectionError } = require('../../errors')
const RecordBatch = require('../../protocol/recordBatch/v0')
const Record = require('../../protocol/recordBatch/record/v0')
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

describe.each([true, false])('Applied Produce with idempotence %s', (idempotent) => {
  it('retries the same payload and exposes the declared physical duplicate allowance', async () => {
    const cluster = producerScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }] })
    )
    const log = []
    const attempts = []
    cluster.answer('Produce', (fields) => {
      const batch = fields.topics[0].partitions[0].batches[0]
      attempts.push([batch.producerId, batch.producerEpoch, batch.baseSequence, batch.digest])
      if (!idempotent || attempts.length === 1) log.push(batch.digest)
      if (attempts.length === 1) throw new KafkaJSProtocolError(createErrorFromCode(7))
      return {
        responses: [
          {
            ...fields.topics[0],
            partitions: [
              {
                partition: 0,
                errorCode: idempotent ? 46 : 0,
                baseOffset: '0',
                logAppendTime: '-1',
                logStartOffset: '0',
                leader: null,
              },
            ],
          },
        ],
        nodeEndpoints: [],
      }
    })
    const producer = new Kafka(
      cluster.kafkaOptions({
        retry: { retries: 2, initialRetryTime: 1, maxRetryTime: 1, factor: 0 },
      })
    ).producer({ idempotent, linger: 0 })
    let primary
    try {
      await producer.connect()
      await expect(
        producer.send({ topic: TOPIC, messages: [{ value: 'applied', timestamp: 1000 }] })
      ).resolves.toHaveLength(1)
      expect(attempts).toHaveLength(2)
      expect(attempts[1]).toEqual(attempts[0])
      expect(log).toHaveLength(idempotent ? 1 : 2)
      if (idempotent) WireOrder.of(cluster).assertNoRewrite()
    } catch (error) {
      primary = error
    } finally {
      await finish(primary, [() => producer.disconnect(), () => cluster.assertClean()])
    }
  })
})

const nodes = [{ id: 0, host: 'node0', port: 9092 }]
const RETRY = { retries: 3, initialRetryTime: 1, maxRetryTime: 5, factor: 0 }
/** The fixed log of partition 0: offsets 0 to 3 hold these values. */
const LOG = ['a', 'b', 'c', 'd']
/** The records the first consumer can see; the restarted consumer also sees the last. */
const FIRST_VISIBLE = 3

/**
 * Encodes the fixed log once, one batch per offset, with fixed timestamps.
 * @returns {Promise<Buffer[]>} the batches, indexed by offset
 * @throws {Error} when a batch cannot be encoded
 */
const encodeLog = () =>
  Promise.all(
    LOG.map(async (value, offset) =>
      Buffer.from(
        (
          await RecordBatch.RecordBatch({
            firstOffset: String(offset),
            firstTimestamp: 1000,
            maxTimestamp: 1000,
            records: [Record({ value: Buffer.from(value) })],
          })
        ).buffer
      )
    )
  )

/**
 * A public consumer over a scripted broker that applies a request and then loses its answer, for
 * Fetch or OffsetCommit. The fixed log and the committed-offset ledger live in the fixture, so a
 * repeated request is answered from what the broker already applied.
 */
class AckLossCase {
  #api
  #records
  #consumers = []
  #visible = FIRST_VISIBLE
  #cluster = null

  /**
   * @param {{ api: 'Fetch'|'OffsetCommit', records: Buffer[] }} options the API whose first answer
   *   is lost, and the fixed log's batches indexed by offset
   */
  constructor({ api, records }) {
    this.#api = api
    this.#records = records
  }

  /**
   * Runs the scenario to completion: the first consumer processes offsets 0 to 2, for OffsetCommit
   * commits offset 3 and a second consumer of the same group then reads offset 3.
   * @returns {Promise<{ requests: string[], deliveries: string[][], committed: { requests:
   *   string[], broker: string|undefined }, restartedDeliveries: string[][] }>} the offsets the
   *   lost API asked for in request order, what each consumer's handler saw as `[offset, value]`,
   *   and the commit ledger (the requested offsets and what the broker applied)
   * @throws {Error} when the scenario does not progress; resources stay open for `close()`
   */
  async run() {
    const ledger = new Map()
    const commits = []
    const cluster = (this.#cluster = groupScript(createScriptedCluster({ nodes }), { batches: 0 }))
    cluster.answer('Fetch', async ({ maxWaitMs, topics }) => {
      const [{ fetchOffset }] = topics[0].partitions
      const records = this.#records.slice(Number(fetchOffset), this.#visible)
      if (records.length === 0) await new Promise((resolve) => setTimeout(resolve, maxWaitMs))
      return {
        partitions: [
          {
            partition: 0,
            errorCode: 0,
            highWatermark: String(this.#visible),
            records: records.length === 0 ? false : Buffer.concat(records),
          },
        ],
      }
    })
    cluster
      .answer('OffsetCommit', ({ groupId, topics }) => {
        const [{ offset }] = topics[0].partitions
        commits.push(offset)
        ledger.set(groupId, offset)
        if (this.#api === 'OffsetCommit' && commits.length === 1) {
          throw new KafkaJSConnectionError('answer lost')
        }
        return {
          topics: topics.map(({ topic, partitions }) => ({
            topic,
            partitions: partitions.map(({ partition }) => ({ partition, errorCode: 0 })),
          })),
        }
      })
      .answer('OffsetFetch', ({ groupId, topics }) => ({
        groupId,
        topics: topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({
            partition,
            offset: ledger.get(groupId) ?? '-1',
            leaderEpoch: -1,
            metadata: null,
            errorCode: 0,
          })),
        })),
      }))
    if (this.#api === 'Fetch') {
      cluster.schedule.once({ api: 'Fetch' }, () =>
        Promise.reject(new KafkaJSConnectionError('answer lost'))
      )
    }

    const first = await this.#consume(LOG.slice(0, FIRST_VISIBLE).length)
    if (this.#api === 'OffsetCommit') {
      await first.consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '3' }])
    }
    await first.consumer.stop()
    let restartedDeliveries = []
    if (this.#api === 'OffsetCommit') {
      this.#visible = LOG.length
      restartedDeliveries = (await this.#consume(1)).deliveries
    }

    const asked = cluster.requests().filter(({ api }) => api === this.#api)
    return {
      requests: asked.map(({ fields }) =>
        this.#api === 'Fetch'
          ? fields.topics[0].partitions[0].fetchOffset
          : fields.topics[0].partitions[0].offset
      ),
      deliveries: first.deliveries,
      committed: { requests: commits, broker: ledger.get('ack-lost') },
      restartedDeliveries,
    }
  }

  /**
   * Stops and disconnects every consumer, each independently, then checks the scripted cluster
   * saw no request it could not answer. Safe to call after a failed `run()` and more than once.
   * @returns {Promise<void>} after every cleanup was attempted
   * @throws {AggregateError} the cleanup failures, when any
   */
  async close() {
    const failures = []
    for (const consumer of this.#consumers.splice(0)) {
      for (const action of [() => consumer.stop(), () => consumer.disconnect()]) {
        try {
          await action()
        } catch (error) {
          failures.push(error)
        }
      }
    }
    try {
      this.#cluster?.assertClean()
    } catch (error) {
      failures.push(error)
    }
    if (failures.length) throw new AggregateError(failures, 'Resource cleanup failed')
  }

  /**
   * Starts one manual-commit consumer of the group and waits for its handler to see records.
   * @param {number} count how many records the handler must see
   * @returns {Promise<{ consumer: object, deliveries: string[][] }>} the consumer, still running
   * @throws {Error} when the handler does not see `count` records within a finite wait
   */
  async #consume(count) {
    const consumer = new Kafka(this.#cluster.kafkaOptions({ retry: RETRY })).consumer({
      groupId: 'ack-lost',
      retry: RETRY,
      maxWaitTimeInMs: 20,
    })
    this.#consumers.push(consumer)
    const deliveries = []
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ message }) => {
        deliveries.push([message.offset, message.value.toString()])
      },
    })
    for (let i = 0; i < 400 && deliveries.length < count; i++) {
      await new Promise((resolve) => setTimeout(resolve, 5))
    }
    expect(deliveries).toHaveLength(count)
    return { consumer, deliveries }
  }
}

describe.each(['Fetch', 'OffsetCommit'])('Lost %s answer', (api) => {
  let records
  beforeAll(async () => {
    records = await encodeLog()
  })

  it(
    api === 'Fetch'
      ? 'is fetched again from the same position and delivers every record once'
      : 'is repeated and leaves the broker at the processed position',
    async () => {
      const ackCase = new AckLossCase({ api, records })
      let primary
      try {
        const { requests, deliveries, committed, restartedDeliveries } = await ackCase.run()
        expect(deliveries).toEqual([
          ['0', 'a'],
          ['1', 'b'],
          ['2', 'c'],
        ])
        expect(new Set(deliveries.map(([offset]) => offset)).size).toBe(3)
        if (api === 'Fetch') {
          expect(requests.slice(0, 2)).toEqual(['0', '0'])
          expect(committed.requests).toEqual([])
        } else {
          expect(committed.requests).toEqual(['3', '3'])
          expect(committed.broker).toBe('3')
          expect(requests).toEqual(['3', '3'])
          expect(restartedDeliveries).toEqual([['3', 'd']])
          expect(committed.requests.every((offset) => BigInt(offset) >= 3n)).toBe(true)
        }
      } catch (error) {
        primary = error
      } finally {
        await finish(primary, [() => ackCase.close()])
      }
    }
  )
})
