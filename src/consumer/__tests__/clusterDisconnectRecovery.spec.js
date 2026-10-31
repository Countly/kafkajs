const Cluster = require('../../cluster')
const createConsumer = require('../index')
const { newLogger, waitForConsumerToJoinGroup } = require('testHelpers')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { prepareRecordBatches, TOPIC } = require('./fixtures/fetchHarness')
const { RecordBatch } = require('../../protocol/recordBatch/v0')
const RecordEncoder = require('../../protocol/recordBatch/record/v0')
const { waitForCondition, withTimeout } = require('./fixtures/liveScenario')
const sleep = require('../../utils/sleep')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { CLUSTER_TOPIC_RECREATED } = require('../../cluster/instrumentationEvents')

const NODES = [{ id: 0, host: 'node0', port: 9092 }]
const RETRY = { retries: 3, initialRetryTime: 10, maxRetryTime: 20 }
const LONG = 60000

/**
 * Makes Fetch answer empty, after a short wait, until `released()`, then one record at the offset
 * it asks for: an idle consumer whose message arrives later.
 */
const lateRecord = (scripted, released) =>
  scripted.answer('Fetch', async ({ topics }) => {
    await sleep(10)
    const partitions = []
    for (const { partition, fetchOffset } of topics[0].partitions) {
      if (!released()) {
        partitions.push({ partition, errorCode: 0, records: false })
        continue
      }
      const batch = await RecordBatch({
        firstOffset: fetchOffset,
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        records: [RecordEncoder({ key: Buffer.from('key'), value: Buffer.from('value') })],
      })
      partitions.push({ partition, errorCode: 0, records: Buffer.from(batch.buffer) })
    }
    return { partitions }
  })

/**
 * `cluster.disconnect()` called from outside a running consumer crashes the run (no broker is left
 * to fetch from) and the crash restarts it, as in kafkajs 2.2.4. A `disconnect()` made by a CRASH
 * listener, as `waitForConsumerToJoinGroup` leaves one, does not cancel that restart; one asked
 * after the restart is armed does, and so does a forced or expired close of the CRASH listener.
 * The restart waits for the close the listener started.
 */
describe('Consumer > external cluster.disconnect() under a running consumer', () => {
  let consumer
  let released
  let held = null

  beforeAll(prepareRecordBatches)

  afterEach(async () => {
    await withTimeout(Promise.resolve(consumer?.disconnect()), 5000)
    consumer = undefined
  })

  /**
   * Joins an idle consumer on a cluster the test can disconnect.
   * @param {{ onCrash?: Function }} [options] a CRASH listener to use instead of the helper's, which
   *   makes a plain `disconnect()`
   * @returns what the consumer recorded, and the cluster that carries it
   */
  const startConsumer = async ({ onCrash } = {}) => {
    released = false
    held = null
    const scripted = groupScript(createScriptedCluster({ nodes: NODES }), { batches: 0 })
    lateRecord(scripted, () => released)
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const cluster = new Cluster({
      ...scripted.kafkaOptions(),
      logger: newLogger(),
      retry: RETRY,
      instrumentationEmitter,
    })
    const realDisconnect = cluster.disconnect.bind(cluster)
    cluster.disconnect = async (...args) => {
      await realDisconnect(...args)
      await held?.promise
    }
    const records = []
    consumer = createConsumer({
      cluster,
      groupId: 'reconnect-group',
      maxWaitTimeInMs: 1,
      logger: newLogger(),
      retry: RETRY,
      instrumentationEmitter,
    })

    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    consumer.run({ eachMessage: async ({ message }) => records.push(message.offset) })
    if (onCrash) {
      const joined = new Promise((resolve) => consumer.on(consumer.events.GROUP_JOIN, resolve))
      consumer.on(consumer.events.CRASH, onCrash)
      await joined
    } else {
      await waitForConsumerToJoinGroup(consumer)
    }
    return {
      cluster,
      records,
      joins: () => scripted.requests().filter(({ api }) => api === 'JoinGroup').length,
      recreationListeners: () =>
        instrumentationEmitter.emitter.listenerCount(CLUSTER_TOPIC_RECREATED),
    }
  }

  /**
   * Spies on the timers. `restartTimers(mark)` returns the `LONG` timers armed since call index
   * `mark`, and `clears()` the handles passed to `clearTimeout`; each timer is cleared on restore.
   */
  const spyOnTimers = () => {
    const realClear = clearTimeout
    const timers = jest.spyOn(global, 'setTimeout')
    const cleared = jest.spyOn(global, 'clearTimeout')
    const restartTimers = (mark) =>
      timers.mock.results
        .slice(mark)
        .filter((_, index) => timers.mock.calls[mark + index][1] === LONG)
        .map(({ value }) => value)
    const restore = timers.mockRestore.bind(timers)
    timers.mockRestore = () => {
      const armed = restartTimers(0)
      cleared.mockRestore()
      restore()
      armed.forEach((timer) => realClear(timer))
    }
    return { timers, restartTimers, clears: () => cleared.mock.calls.map(([timer]) => timer) }
  }

  /** Holds the cluster disconnect of the close a CRASH listener starts until `release()`. */
  const holdClose = () => {
    let release
    held = { promise: new Promise((resolve) => (release = resolve)) }
    return release
  }

  /** @returns {Promise<object>} the CRASH payload, once the consumer crashed after `cluster.disconnect()` */
  const crashed = (cluster) => {
    const event = new Promise((resolve) => consumer.on(consumer.events.CRASH, resolve))
    cluster.disconnect()
    return event
  }

  it('keeps consuming after the cluster is disconnected', async () => {
    const { cluster, records } = await startConsumer()

    await cluster.disconnect()
    expect(cluster.isConnected()).toBe(false)
    released = true

    const resumed = await waitForCondition(() => records.length > 0, { attempts: 300, delay: 10 })
    expect(resumed).toBe(true)
    expect(cluster.isConnected()).toBe(true)
  }, 20000)

  it('does not restart a consumer disconnected after the restart was armed', async () => {
    const { cluster, records } = await startConsumer()
    consumer.on(consumer.events.CRASH, () => setImmediate(() => consumer.disconnect()))

    await cluster.disconnect()
    released = true
    await sleep(300)

    expect(records).toEqual([])
    expect(cluster.isConnected()).toBe(false)
  }, 20000)

  describe('the restart and a close the CRASH listener started', () => {
    it('does not restart after a forced close from the listener', async () => {
      const { cluster, records, joins } = await startConsumer({
        onCrash: () => consumer.disconnect({ timeout: 0 }),
      })
      const before = joins()

      const { payload } = await crashed(cluster)
      released = true
      await sleep(300)

      expect(payload.restart).toBe(true)
      expect(joins()).toBe(before)
      expect(records).toEqual([])
    }, 20000)

    it('does not restart after a close whose deadline expired before the restart', async () => {
      let release
      const { cluster, records, joins } = await startConsumer({
        onCrash: () => {
          release = holdClose()
          consumer.disconnect({ timeout: 50 })
        },
      })
      const before = joins()

      await crashed(cluster)
      await sleep(150)
      expect(joins()).toBe(before)
      release()
      released = true
      await sleep(300)

      expect(joins()).toBe(before)
      expect(records).toEqual([])
    }, 20000)

    it('starts no second runner before the listener close released its cleanup', async () => {
      let release
      const { cluster, records, joins } = await startConsumer({
        onCrash: () => {
          release = holdClose()
          consumer.disconnect()
        },
      })
      const before = joins()

      await crashed(cluster)
      released = true
      await sleep(200)
      expect(joins()).toBe(before)

      release()
      const restarted = await waitForCondition(() => records.length > 0, {
        attempts: 300,
        delay: 10,
      })

      expect(restarted).toBe(true)
      expect(joins()).toBe(before + 1)
    }, 20000)

    it('cancels the restart a disconnect asked while the close was held', async () => {
      let release
      const { cluster, records, joins } = await startConsumer({
        onCrash: () => {
          release = holdClose()
          consumer.disconnect()
        },
      })
      const before = joins()

      await crashed(cluster)
      await sleep(100)
      const joined = consumer.disconnect()
      release()
      await joined
      released = true
      await sleep(300)

      expect(joins()).toBe(before)
      expect(records).toEqual([])
    }, 20000)

    it('leaves no timer, listener or lifetime when a disconnect joins the close after arming', async () => {
      const { timers, clears, restartTimers } = spyOnTimers()
      let release
      let mark
      const { cluster, recreationListeners } = await startConsumer({
        onCrash: ({ payload }) => {
          payload.error.retryTime = LONG
          mark = timers.mock.calls.length
          release = holdClose()
          consumer.disconnect()
        },
      })

      try {
        await crashed(cluster)
        const handles = restartTimers(mark)
        expect(handles.length).toBeGreaterThan(0)

        const joined = consumer.disconnect()
        expect(handles.every((timer) => clears().includes(timer))).toBe(true)

        release()
        await joined
        expect(recreationListeners()).toBe(0)
      } finally {
        timers.mockRestore()
      }
    }, 20000)
  })

  describe('an explicit run and a pending automatic restart', () => {
    /** CRASH listener that makes the restart wait `LONG` ms, or `delay`, after a plain disconnect. */
    const delayedRestart =
      (delay, onCrash) =>
      ({ payload }) => {
        payload.error.retryTime = delay
        onCrash?.()
        consumer.disconnect()
      }

    it('keeps the explicit run and starts no second runner while the timer is armed', async () => {
      const {
        cluster,
        records: oldRecords,
        joins,
      } = await startConsumer({
        onCrash: delayedRestart(300),
      })
      const records = []
      const before = joins()

      await crashed(cluster)
      await sleep(50)
      await consumer.connect()
      await consumer.run({ eachMessage: async ({ message }) => records.push(message.offset) })
      expect(joins()).toBe(before + 1)

      await sleep(500)
      released = true
      const consumed = await waitForCondition(() => records.length > 0, {
        attempts: 300,
        delay: 10,
      })

      expect(consumed).toBe(true)
      expect(oldRecords).toEqual([])
      expect(joins()).toBe(before + 1)

      await consumer.disconnect()
      const after = joins()
      await sleep(100)
      expect(joins()).toBe(after)
    }, 20000)

    it('keeps the explicit run that waited on the same held close as the restart', async () => {
      let release
      const {
        cluster,
        records: oldRecords,
        joins,
      } = await startConsumer({
        onCrash: delayedRestart(10, () => (release = holdClose())),
      })
      const before = joins()

      await crashed(cluster)
      await sleep(100)
      const records = []
      const explicit = consumer.run({
        eachMessage: async ({ message }) => records.push(message.offset),
      })
      await sleep(50)
      release()
      await explicit
      await sleep(200)
      expect(joins()).toBe(before + 1)

      released = true
      const consumed = await waitForCondition(() => records.length > 0, {
        attempts: 300,
        delay: 10,
      })
      expect(consumed).toBe(true)
      expect(oldRecords).toEqual([])
      expect(joins()).toBe(before + 1)

      await consumer.disconnect()
      const after = joins()
      await sleep(100)
      expect(joins()).toBe(after)
    }, 20000)
  })

  describe('a forced close and the restart timer', () => {
    it('arms no restart timer for a close forced during CRASH delivery', async () => {
      const { timers, clears, restartTimers } = spyOnTimers()
      let mark
      const { cluster, joins } = await startConsumer({
        onCrash: ({ payload }) => {
          payload.error.retryTime = LONG
          mark = timers.mock.calls.length
          consumer.disconnect({ timeout: 0 })
        },
      })
      const before = joins()

      try {
        await crashed(cluster)
        await sleep(50)
        expect(restartTimers(mark).every((timer) => clears().includes(timer))).toBe(true)
        expect(joins()).toBe(before)
      } finally {
        timers.mockRestore()
      }
    }, 20000)

    it('clears the restart timer promptly when the close expires after arming', async () => {
      const { timers, clears, restartTimers } = spyOnTimers()
      let release
      let mark
      const { cluster, joins } = await startConsumer({
        onCrash: ({ payload }) => {
          payload.error.retryTime = LONG
          mark = timers.mock.calls.length
          release = holdClose()
          consumer.disconnect({ timeout: 50 })
        },
      })
      const before = joins()

      try {
        await crashed(cluster)
        const handles = restartTimers(mark)
        expect(handles.length).toBeGreaterThan(0)
        expect(handles.some((timer) => clears().includes(timer))).toBe(false)

        await sleep(150)
        expect(handles.every((timer) => clears().includes(timer))).toBe(true)
        release()
        await sleep(50)
        expect(joins()).toBe(before)
      } finally {
        timers.mockRestore()
      }
    }, 20000)
  })
})
