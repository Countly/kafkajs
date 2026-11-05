const { KafkaJSCommitFailedError } = require('../../errors')
const { TOPIC, batchAt, gate, pause, until } = require('./fixtures/closeRig')
const { RecreationRig } = require('./fixtures/recreationRig')

const BOUND_MS = 300
const NEW = ['value-0', 'value-1', 'value-2']
const OLD = 'value-5'

/**
 * The old incarnation's records 5..7 as ONE fetched batch, so the later records are fetched
 * before the topic is recreated and only handled after it.
 * @returns {Promise<Record<string, Buffer>>} the raw batches served at fetch offset 5
 */
const oldBatchAt5 = async () => ({
  '0|5': Buffer.concat([await batchAt(5), await batchAt(6), await batchAt(7)]),
})

/**
 * A topic recreated while a handler still works through an old-incarnation batch.
 *
 * The handler of the batch's first record waits on a gate. `release()` lets it finish and then run
 * `afterGate`, which a row uses to make an explicit commit from inside the handler.
 */
class StaleBatchScenario {
  /**
   * @param {object} options
   * @param {'classic'|'consumer'} options.protocol
   * @param {'eachMessage'|'eachBatch'} options.mode which handler the consumer runs
   * @param {(handler: object) => Promise<void>} [options.afterGate] runs inside the handler once
   *   the gate opens, with `{ consumer, commitOffsetsIfNecessary }`
   * @returns {Promise<StaleBatchScenario>} a consumer is inside the old batch's first record
   */
  static async open({ protocol, mode, afterGate = async () => {} }) {
    const rec = await RecreationRig.open({ protocol, log: await oldBatchAt5() })
    const scenario = new StaleBatchScenario(rec)
    const handler = { consumer: null }
    const gated = async (values, extras) => {
      scenario.seen.push(...values)
      if (!values.includes(OLD)) return
      scenario.entered = true
      await scenario.gate.promise
      await afterGate({ ...handler, ...extras }).catch((error) => scenario.errors.push(error))
      scenario.finished = true
    }
    const run =
      mode === 'eachMessage'
        ? { eachMessage: ({ message }) => gated([message.value.toString()]) }
        : {
            eachMessage: undefined,
            eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary }) => {
              await gated(
                batch.messages.map(({ value }) => value.toString()),
                { commitOffsetsIfNecessary }
              )
              for (const { offset } of batch.messages) resolveOffset(offset)
            },
          }
    try {
      const { consumer } = await rec.start({ delivered: 0, run })
      handler.consumer = scenario.consumer = consumer
      await until(() => scenario.entered, 'the old batch handler')
    } catch (error) {
      await scenario.dispose()
      throw error
    }
    return scenario
  }

  /** @param {RecreationRig} rec */
  constructor(rec) {
    this.rec = rec
    this.gate = gate()
    /** @type {object|null} */
    this.consumer = null
    this.entered = false
    this.finished = false
    /** @type {string[]} */
    this.seen = []
    /** @type {Error[]} */
    this.errors = []
  }

  /** @returns {string[]} the offsets of every OffsetCommit frame, in order */
  committed() {
    return this.rec
      .sent('OffsetCommit')
      .flatMap(({ topics }) => topics.flatMap(({ partitions }) => partitions))
      .map(({ offset }) => String(offset))
  }

  /**
   * Lets the old handler finish and the consumer settle on whatever it does next.
   * @returns {Promise<void>}
   */
  async release() {
    this.gate.resolve()
    await until(() => this.finished, 'the old batch handler to finish')
    await pause(150)
  }

  /** @returns {Promise<void>} */
  dispose() {
    this.gate.resolve()
    return this.rec.dispose()
  }
}

describe.each(['classic', 'consumer'])('an old-incarnation batch, %s protocol', (protocol) => {
  describe.each(['eachMessage', 'eachBatch'])('%s', (mode) => {
    /** @type {StaleBatchScenario|undefined} */
    let scenario
    afterEach(async () => {
      await scenario?.dispose()
      scenario = undefined
    })
    const open = async (options) => {
      scenario = await StaleBatchScenario.open({ protocol, mode, ...options })
      return scenario
    }

    test('records fetched before the recreation commit nothing into the new incarnation', async () => {
      const { rec, consumer } = await open()
      const list = rec.rig.hold('ListOffsets')
      await rec.recreate()

      await scenario.release()
      expect(list.hits).toBe(1)
      await consumer.disconnect({ timeout: BOUND_MS })
      list.release()
      await pause(150)

      expect(scenario.committed()).toEqual([])
      expect(rec.offset).toBe('5')
    })

    test('the new incarnation commits only its own records', async () => {
      const { rec, consumer } = await open()
      await rec.recreate()

      await scenario.release()
      await until(() => NEW.every((value) => scenario.seen.includes(value)), 'the new records')
      await consumer.disconnect({ timeout: BOUND_MS })

      expect(scenario.committed()).toEqual(['3'])
      expect(rec.offset).toBe('3')
    })

    /**
     * A forced close need not send its final commit, so allow the fixture's polling budget.
     * @returns {Promise<void>} resolves after the commit is observed and the consumer closes
     * @throws {Error} if the commit is not observed within the budget or disconnect fails
     */
    test('without a recreation the old batch commits as before', async () => {
      const { rec, consumer } = await open()

      await scenario.release()
      await Promise.all([
        consumer.disconnect({ timeout: 10000 }),
        until(() => scenario.committed().length > 0, 'the old batch commit'),
      ])

      expect(scenario.committed()).toEqual(['8'])
      expect(rec.offset).toBe('8')
    })

    test('an explicit commit from the old handler is refused, and so is the commit at close', async () => {
      const offsets = [{ topic: TOPIC, partition: 0, offset: '8' }]
      const topics = [{ topic: TOPIC, partitions: [{ partition: 0, offset: '8' }] }]
      const { rec, consumer } = await open({
        afterGate: ({ consumer: owner, commitOffsetsIfNecessary }) =>
          mode === 'eachMessage'
            ? owner.commitOffsets(offsets)
            : commitOffsetsIfNecessary({ topics }),
      })
      const list = rec.rig.hold('ListOffsets')
      await rec.recreate()

      await scenario.release()
      expect(list.hits).toBe(1)
      await consumer.disconnect({ timeout: BOUND_MS })
      list.release()
      await pause(150)

      expect(scenario.errors).toHaveLength(1)
      expect(scenario.errors[0]).toBeInstanceOf(KafkaJSCommitFailedError)
      expect(scenario.committed()).toEqual([])
    })

    test('the same explicit commit without a recreation is written', async () => {
      const offsets = [{ topic: TOPIC, partition: 0, offset: '8' }]
      const topics = [{ topic: TOPIC, partitions: [{ partition: 0, offset: '8' }] }]
      const { rec } = await open({
        afterGate: ({ consumer: owner, commitOffsetsIfNecessary }) =>
          mode === 'eachMessage'
            ? owner.commitOffsets(offsets)
            : commitOffsetsIfNecessary({ topics }),
      })

      await scenario.release()

      expect(scenario.errors).toEqual([])
      expect(scenario.committed()).toEqual(['8'])
      expect(rec.offset).toBe('8')
    })
  })
})
