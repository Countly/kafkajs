const { CloseRig, TOPIC, gate, pause, until } = require('./fixtures/closeRig')
const ConsumerGroup = require('../consumerGroup')
const Runner = require('../runner')
const { KafkaJSError, KafkaJSNonRetriableError } = require('../../errors')

const COMMIT = [{ topic: TOPIC, partition: 0, offset: '1' }]
const NOT_RUNNING = 'Consumer is not running, wait for consumer#run to resolve first'

/**
 * @param {Promise<unknown>} promise
 * @returns {Promise<unknown>} 'committed', or the rejection
 */
const settle = (promise) =>
  promise.then(
    () => 'committed',
    (error) => error
  )

/** @param {object} ownership @returns {number} holders retained on the current membership */
const holders = (ownership) => ownership.states.get(ownership.token()).holders

describe.each(['classic', 'consumer'])(
  'Consumer > a commit that reaches a consumer that is not running, %s protocol',
  (protocol) => {
    /** @type {CloseRig} */
    let rig
    /** @type {{ resolve: Function }[]} */
    let gates
    beforeEach(() => {
      gates = []
    })
    afterEach(async () => {
      for (const held of gates) held.resolve()
      jest.restoreAllMocks()
      await rig?.dispose()
      rig = null
    })

    /**
     * Holds the next `ConsumerGroup#connect` calls, in order, as scripted.
     * @param {Array<'hold'|Error>} steps what each call does: wait for its gate, or reject
     * @returns {{ calls: object[], held: ReturnType<typeof gate>[] }} the groups that connected and the holds
     */
    const scriptConnect = (steps) => {
      const original = ConsumerGroup.prototype.connect
      const calls = []
      const held = []
      jest.spyOn(ConsumerGroup.prototype, 'connect').mockImplementation(async function (...args) {
        calls.push(this)
        const step = steps.shift()
        if (step instanceof Error) throw step
        if (step === 'hold') {
          const boundary = gate()
          held.push(boundary)
          gates.push(boundary)
          await boundary.promise
        }
        return original.apply(this, args)
      })
      return { calls, held }
    }

    /** @returns {Promise<object>} a connected, subscribed consumer that has not run */
    const subscribed = async () => {
      rig = await CloseRig.open({ protocol })
      const consumer = rig.consumer()
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      return consumer
    }

    test('a commit made while run() is still connecting rejects as not running and writes nothing', async () => {
      const connect = scriptConnect(['hold'])
      const consumer = await subscribed()
      const running = consumer.run({ autoCommit: false, eachMessage: async () => {} })
      await until(() => connect.held.length === 1, 'the held connect')
      const ownership = connect.calls[0].ownership

      const error = await settle(consumer.commitOffsets(COMMIT))

      expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
      expect(error.message).toBe(NOT_RUNNING)
      expect(holders(ownership)).toBe(0)
      connect.held[0].resolve()
      await running
      await pause(100)
      expect(rig.frames('OffsetCommit')).toHaveLength(0)
    })

    test('a commit made while the restart after a crash is connecting rejects as not running and writes nothing', async () => {
      const connect = scriptConnect([
        new KafkaJSError('The scripted connect failed', { retriable: true }),
        'hold',
      ])
      const consumer = await subscribed()
      await consumer.run({ autoCommit: false, eachMessage: async () => {} })
      await until(() => connect.held.length === 1, 'the restart held in its connect')

      const error = await settle(consumer.commitOffsets(COMMIT))

      expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
      expect(error.message).toBe(NOT_RUNNING)
      expect(holders(connect.calls[1].ownership)).toBe(0)
      connect.held[0].resolve()
      await pause(100)
      expect(rig.frames('OffsetCommit')).toHaveLength(0)
    })

    test('a commit made while a crashed run is cleaned up rejects with CONSUMER_STOPPING and writes nothing', async () => {
      scriptConnect([new KafkaJSNonRetriableError('The scripted connect failed')])
      const original = Runner.prototype.stop
      const cleanup = gate()
      gates.push(cleanup)
      const stopped = []
      jest.spyOn(Runner.prototype, 'stop').mockImplementation(async function (...args) {
        stopped.push(this)
        const stopping = original.apply(this, args)
        await cleanup.promise
        return stopping
      })
      const consumer = await subscribed()
      // run() settles only once the crash it started has been cleaned up.
      const running = consumer.run({ autoCommit: false, eachMessage: async () => {} })
      await until(() => stopped.length === 1, 'the crash cleanup stopping the run')

      const error = await settle(consumer.commitOffsets(COMMIT))

      expect(error.type).toBe('CONSUMER_STOPPING')
      expect(error.cause).toBeUndefined()
      expect(holders(stopped[0].consumerGroup.ownership)).toBe(0)
      cleanup.resolve()
      await running
      await pause(100)
      expect(rig.frames('OffsetCommit')).toHaveLength(0)
    })

    test('a commit made while a stop is in progress returns a promise that rejects with CONSUMER_STOPPING, never a synchronous throw', async () => {
      rig = await CloseRig.open({ protocol })
      const { consumer } = await rig.started({ run: { autoCommit: false } })
      const stopping = consumer.stop()

      let call
      expect(() => (call = consumer.commitOffsets(COMMIT))).not.toThrow()
      expect(call).toBeInstanceOf(Promise)
      const error = await settle(call)
      await stopping

      expect(error.type).toBe('CONSUMER_STOPPING')
      expect(rig.frames('OffsetCommit')).toHaveLength(0)
    })

    test('an empty commit made while a stop is in progress still resolves and writes nothing', async () => {
      rig = await CloseRig.open({ protocol })
      const { consumer } = await rig.started({ run: { autoCommit: false } })
      const stopping = consumer.stop()

      const outcome = await settle(consumer.commitOffsets([]))
      await stopping

      expect(outcome).toBe('committed')
      expect(rig.frames('OffsetCommit')).toHaveLength(0)
    })
  }
)
