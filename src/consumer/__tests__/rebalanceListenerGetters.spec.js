const { Kafka, logLevel } = require('../../../index')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { prepareRecordBatches, TOPIC } = require('./fixtures/fetchHarness')
const { gate } = require('./fixtures/hookRound')

const NODE = { id: 0, host: 'node-0', port: 9092 }
const BUDGET_MS = 10000
const PAIRS = [0, 1].map((partition) => ({ topic: TOPIC, partition }))
const LATE_EARLIEST = {
  topics: [
    {
      name: TOPIC,
      partitions: [0, 1].map((partitionIndex) => ({
        partitionIndex,
        errorCode: 0,
        timestamp: '-1',
        offset: '7',
        leaderEpoch: 5,
      })),
    },
  ],
}

/** @param {() => boolean} predicate @param {string} label @returns {Promise<void>} resolves when it holds */
const until = async (predicate, label) => {
  const deadline = Date.now() + BUDGET_MS
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`Did not happen in time: ${label}`)
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

/** @param {object} cluster @param {string} api @returns {object[]} the frames of one API */
const frames = (cluster, api) => cluster.requests().filter((request) => request.api === api)

describe('position and committed inside awaited rebalance callbacks', () => {
  let cluster
  let consumer
  const errors = []

  beforeAll(prepareRecordBatches)

  /**
   * Builds a scripted two-partition group whose coordinator answers commits with metadata.
   * @param {object} [options]
   * @param {number} [options.batches=0] how many Fetches carry records
   * @param {Record<number, string>} [options.committed] commit by partition; absent means none
   * @param {number} [options.rebalanceTimeout] the round budget in milliseconds
   * @returns {object} the consumer
   */
  const start = ({ batches = 0, committed = { 0: '99', 1: '77' }, rebalanceTimeout } = {}) => {
    cluster = groupScript(createScriptedCluster({ nodes: [NODE] }), {
      partitions: 2,
      batches,
      committed,
    }).answer('OffsetFetch', ({ groupId, topics }) => ({
      groupId,
      topics: topics.map((entry) => ({
        topic: entry.topic,
        partitions: entry.partitions.map(({ partition }) => ({
          partition,
          offset: committed[partition] ?? '-1',
          leaderEpoch: -1,
          metadata: `meta-${partition}`,
          errorCode: 0,
        })),
      })),
    }))
    const kafka = new Kafka(
      cluster.kafkaOptions({
        logLevel: logLevel.ERROR,
        logCreator:
          () =>
          ({ level, log }) => {
            if (level === logLevel.ERROR) errors.push(log)
          },
        retry: { retries: 0 },
      })
    )
    consumer = kafka.consumer({ groupId: 'hook-getters', maxWaitTimeInMs: 20, rebalanceTimeout })
    return consumer
  }

  afterEach(async () => {
    errors.length = 0
    await consumer?.disconnect().catch(() => {})
    consumer = null
    cluster?.assertClean()
  })

  test('an assigned callback reads broker commits and metadata, and a seek is the position', async () => {
    start()
    const seen = {}
    const held = gate()
    let entered = false
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    const firstFetch = cluster.schedule.hold({ api: 'Fetch' })
    const running = consumer.run({
      autoCommit: false,
      eachMessage: async () => {},
      rebalanceListener: {
        onPartitionsAssigned: async () => {
          seen.committed = await consumer.committed(PAIRS)
          seen.untouched = await consumer.position(PAIRS[0])
          consumer.seek({ ...PAIRS[0], offset: '40' })
          seen.afterSeek = await consumer.position(PAIRS[0])
          seen.committedAfterSeek = await consumer.committed([PAIRS[0]])
          seen.afterRead = await consumer.position(PAIRS[0])
          entered = true
          await held.promise
        },
      },
    })
    await until(() => entered, 'assigned callback')
    expect(frames(cluster, 'Fetch')).toHaveLength(0)
    held.release()
    await running
    await firstFetch.arrived()

    expect(seen.committed).toEqual([
      { ...PAIRS[0], offset: '99', metadata: 'meta-0' },
      { ...PAIRS[1], offset: '77', metadata: 'meta-1' },
    ])
    expect(seen.untouched).toBe('99')
    expect(seen.afterSeek).toBe('40')
    expect(seen.committedAfterSeek).toEqual([{ ...PAIRS[0], offset: '99', metadata: 'meta-0' }])
    expect(seen.afterRead).toBe('40')
    const [first] = frames(cluster, 'Fetch')
    const fetched = first.fields.topics[0].partitions.find(({ partition }) => partition === 0)
    expect(fetched.fetchOffset).toBe('40')
    const stop = consumer.stop()
    firstFetch.release({
      partitions: [0, 1].map((partition) => ({ partition, errorCode: 0, records: false })),
    })
    await stop
  })

  test('a revoked callback at a graceful stop reads the quiescent next position', async () => {
    start({ batches: 1, committed: {} })
    const delivered = []
    const seen = {}
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ partition, message }) => {
        delivered.push({ partition, offset: Number(message.offset) })
      },
      rebalanceListener: {
        onPartitionsRevoked: async ({ partitions }) => {
          if (!partitions.length) return
          seen.position = await consumer.position(PAIRS[0])
          seen.committed = await consumer.committed([PAIRS[0]])
        },
      },
    })
    await until(() => delivered.some(({ partition }) => partition === 0), 'a record of partition 0')
    await consumer.stop()

    const last = Math.max(
      ...delivered.filter(({ partition }) => partition === 0).map(({ offset }) => offset)
    )
    expect(seen.position).toBe(String(last + 1))
    expect(seen.committed).toEqual([{ ...PAIRS[0], offset: null, metadata: null }])
  })

  test('a callback of a retired run cannot read through the getters, and sends nothing', async () => {
    start()
    const held = gate()
    const late = {}
    let entered = false
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    const first = consumer.run({
      autoCommit: false,
      eachMessage: async () => {},
      rebalanceListener: {
        onPartitionsAssigned: async () => {
          entered = true
          await held.promise
          for (const [name, call] of [
            ['position', () => consumer.position(PAIRS[0])],
            ['committed', () => consumer.committed(PAIRS)],
          ]) {
            late[name] = await call().then(
              (value) => value,
              (error) => error
            )
          }
        },
      },
    })
    await until(() => entered, 'assigned callback')
    await consumer.stop()
    await first.catch(() => {})
    await consumer.run({ autoCommit: false, eachMessage: async () => {}, rebalanceListener: {} })
    const offsetFetches = frames(cluster, 'OffsetFetch').length
    const listOffsets = frames(cluster, 'ListOffsets').length

    held.release()
    await until(() => late.committed !== undefined, 'late reads')

    expect(late.position).toBeInstanceOf(Error)
    expect(late.committed).toBeInstanceOf(Error)
    expect(late.committed.message).toBe('The consumer is stopping')
    expect(frames(cluster, 'OffsetFetch')).toHaveLength(offsetFetches)
    expect(frames(cluster, 'ListOffsets')).toHaveLength(listOffsets)
  })

  test('a close-time callback of the current run keeps its authority when a handler requested the stop', async () => {
    start({ batches: 1, committed: {} })
    const seen = {}
    let stopping
    let from
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ partition }) => {
        if (stopping === undefined) {
          from = { topic: TOPIC, partition }
          stopping = consumer.stop()
        }
      },
      rebalanceListener: {
        onPartitionsRevoked: async ({ partitions }) => {
          if (!partitions.length) return
          seen.position = await consumer.position(from).catch((error) => error)
          seen.committed = await consumer.committed([from]).catch((error) => error)
        },
      },
    })
    await until(() => stopping !== undefined, 'a handler stop request')
    await stopping

    expect(seen.position).toBe('1')
    expect(seen.committed).toEqual([{ ...from, offset: null, metadata: null }])
  })

  describe('a position read that outlives its callback', () => {
    /** Starts a run whose assigned callback begins a position read held at ListOffsets. */
    const runWithHeldPosition = async ({ afterArrival, rebalanceTimeout } = {}) => {
      start({ committed: {}, rebalanceTimeout })
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      const held = cluster.schedule.hold({ api: 'ListOffsets' })
      let late
      const running = consumer
        .run({
          autoCommit: false,
          eachMessage: async () => {},
          rebalanceListener: {
            onPartitionsAssigned: async () => {
              consumer.seek({ ...PAIRS[0], offset: '-2' })
              late = consumer.position(PAIRS[0]).then(
                (value) => ({ value }),
                (error) => ({ error })
              )
              await held.arrived()
              await afterArrival?.()
            },
          },
        })
        .catch((error) => error)
      return { held, running, read: () => late }
    }

    test('is refused once the callback settled, and its late answer is never published', async () => {
      const { held, running, read } = await runWithHeldPosition()
      await running
      held.release(LATE_EARLIEST)
      const result = await read()
      await until(() => frames(cluster, 'Fetch').length > 0, 'a fetch')
      const [first] = frames(cluster, 'Fetch')
      const fetched = first.fields.topics[0].partitions.find(({ partition }) => partition === 0)
      expect(fetched.fetchOffset).toBe('0')
      await consumer.stop()
      expect(result.error).toBeInstanceOf(Error)
      expect(result.value).toBeUndefined()
    })

    test('is refused once the run was retired while the callback still ran', async () => {
      const gateStop = gate()
      let stopping
      const { held, running, read } = await runWithHeldPosition({
        afterArrival: async () => {
          stopping = consumer.stop()
          await new Promise((resolve) => setTimeout(resolve, 20))
          await gateStop.promise
        },
      })
      await until(() => stopping !== undefined, 'a stop request')
      held.release()
      gateStop.release()
      await running
      await stopping
      const result = await read()
      expect(result.error).toBeInstanceOf(Error)
    })

    test('is refused once the callback expired', async () => {
      const { held, running, read } = await runWithHeldPosition({
        rebalanceTimeout: 100,
        afterArrival: () => new Promise((resolve) => setTimeout(resolve, 300)),
      })
      await running
      held.release()
      const result = await read()
      expect(result.error).toBeInstanceOf(Error)
    })

    test('a read answered inside its own callback resolves', async () => {
      start({ committed: {} })
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      const held = cluster.schedule.hold({ api: 'ListOffsets' })
      let value
      const running = consumer.run({
        autoCommit: false,
        eachMessage: async () => {},
        rebalanceListener: {
          onPartitionsAssigned: async () => {
            consumer.seek({ ...PAIRS[0], offset: '-2' })
            const read = consumer.position(PAIRS[0])
            await held.arrived()
            held.release()
            value = await read
          },
        },
      })
      await running
      expect(value).toMatch(/^\d+$/)
      await consumer.stop()
    })
  })
})
