const Cluster = require('../../cluster')
const createConsumer = require('../index')
const { newLogger } = require('testHelpers')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { prepareRecordBatches, TOPIC } = require('./fixtures/fetchHarness')
const { withTimeout } = require('./fixtures/liveScenario')
const { gate, until } = require('./fixtures/closeRig')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')

jest.setTimeout(20000)

const NODES = [{ id: 0, host: 'node0', port: 9092 }]
const RETRY = { retries: 3, initialRetryTime: 10, maxRetryTime: 20 }
const OFFSET = { topic: TOPIC, partition: 0, offset: '1' }
const idle = { eachMessage: async () => {} }

/** @returns {Promise<void>} resolves after the pending microtasks and one macrotask turn */
const flush = () => new Promise((resolve) => setImmediate(resolve))

/**
 * @param {string} groupId
 * @param {string} memberId
 * @returns {object[]} what `toEqual` matches against one OffsetCommit of offset 1 for partition 0
 */
const oneCommit = (groupId, memberId) => [
  expect.objectContaining({
    groupId,
    memberId,
    groupGenerationId: 1,
    topics: [
      expect.objectContaining({
        topic: TOPIC,
        partitions: [expect.objectContaining({ partition: 0, offset: '1' })],
      }),
    ],
  }),
]

/**
 * A consumer that calls a public method of another consumer from inside its own handler or
 * rebalance callback, or from async work started there, gets the result a call from outside gets:
 * the stores of the first consumer belong to that consumer only.
 */
describe('Consumer > calls from another consumer in the same process', () => {
  const members = []
  const releases = []

  beforeAll(prepareRecordBatches)

  afterEach(async () => {
    for (const release of releases.splice(0)) release()
    for (const { consumer } of members.splice(0))
      await withTimeout(
        consumer.disconnect().catch(() => {}),
        5000
      )
  })

  /**
   * A consumer over its own scripted cluster, the one member of its group, assigned every partition.
   * Its cluster counts its connects and can hold the cluster disconnect of a close.
   * @param {object} options
   * @param {string} options.groupId
   * @param {string} options.memberId the member id its JoinGroup answer gives
   * @param {number} [options.partitions=1]
   * @param {number} [options.batches=0] the first Fetches that carry one record per partition
   */
  const member = ({ groupId, memberId, partitions = 1, batches = 0 }) => {
    const scripted = groupScript(createScriptedCluster({ nodes: NODES }), {
      partitions,
      batches,
      memberId,
    })
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const cluster = new Cluster({
      ...scripted.kafkaOptions(),
      logger: newLogger(),
      retry: RETRY,
      instrumentationEmitter,
    })
    const close = { entered: false, held: null }
    const realDisconnect = cluster.disconnect.bind(cluster)
    cluster.disconnect = async (...args) => {
      await realDisconnect(...args)
      close.entered = true
      await close.held
    }
    let connects = 0
    const realConnect = cluster.connect.bind(cluster)
    cluster.connect = (...args) => {
      connects++
      return realConnect(...args)
    }
    const consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 1,
      logger: newLogger(),
      retry: RETRY,
      instrumentationEmitter,
    })
    const created = {
      consumer,
      scripted,
      close,
      connects: () => connects,
      joins: () => scripted.requests().filter(({ api }) => api === 'JoinGroup').length,
      commits: () =>
        scripted
          .requests()
          .filter(({ api }) => api === 'OffsetCommit')
          .map(({ fields }) => fields),
      /** Holds the cluster disconnect of the next close until the returned release is called. */
      holdClose: () => {
        const held = gate()
        close.held = held.promise
        releases.push(held.resolve)
        return held.resolve
      },
      /** Holds the next request of `api` on this member's cluster. */
      hold: (api) => {
        const held = scripted.schedule.hold({ api })
        releases.push(() => held.settle && held.release())
        return held
      },
    }
    members.push(created)
    return created
  }

  /** @returns {Promise<object>} the member, connected and subscribed from the beginning */
  const subscribed = async (created) => {
    await created.consumer.connect()
    await created.consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    return created
  }

  /** @returns {Promise<object>} the member, running without auto-commit and joined */
  const running = async (created, runOptions) => {
    await subscribed(created)
    const joined = new Promise((resolve) =>
      created.consumer.on(created.consumer.events.GROUP_JOIN, resolve)
    )
    await Promise.all([created.consumer.run({ autoCommit: false, ...runOptions }), joined])
    return created
  }

  const memberB = (options = {}) => member({ groupId: 'group-b', memberId: 'member-b', ...options })

  /** Consumer A: two partitions, each with one record, unless `options` says otherwise. */
  const startA = (runOptions, options = {}) =>
    running(
      member({ groupId: 'group-a', memberId: 'member-a', partitions: 2, batches: 1, ...options }),
      runOptions
    )

  it('B.commitOffsets inside an eachMessage of A commits the offset of B', async () => {
    const b = await running(memberB(), idle)
    let outcome
    await startA({
      eachMessage: async ({ partition }) => {
        if (partition === 1)
          outcome ??= b.consumer.commitOffsets([OFFSET]).then(
            () => 'committed',
            (error) => error
          )
      },
    })

    await until(() => outcome, 'the commit from the handler of A')
    expect(await outcome).toBe('committed')
    expect(b.commits()).toEqual(oneCommit('group-b', 'member-b'))
  })

  it('B.commitOffsets inside an eachBatch of A commits the offset of B', async () => {
    const b = await running(memberB(), idle)
    let outcome
    await startA({
      eachBatch: async ({ batch }) => {
        if (batch.partition === 1)
          outcome ??= b.consumer.commitOffsets([OFFSET]).then(
            () => 'committed',
            (error) => error
          )
      },
    })

    await until(() => outcome, 'the commit from the handler of A')
    expect(await outcome).toBe('committed')
    expect(b.commits()).toEqual(oneCommit('group-b', 'member-b'))
  })

  it('B.commitOffsets inside a handler of A commits when both use one group id', async () => {
    const b = await running(member({ groupId: 'shared', memberId: 'member-b' }), idle)
    let outcome
    await startA(
      {
        eachMessage: async ({ partition }) => {
          if (partition === 1)
            outcome ??= b.consumer.commitOffsets([OFFSET]).then(
              () => 'committed',
              (error) => error
            )
        },
      },
      { groupId: 'shared' }
    )

    await until(() => outcome, 'the commit from the handler of A')
    expect(await outcome).toBe('committed')
    expect(b.commits()).toEqual(oneCommit('shared', 'member-b'))
  })

  it('B.commitOffsets and B.pause from work a handler of A started still work after A stopped', async () => {
    const b = await running(memberB(), idle)
    const go = gate()
    let later
    const a = await startA({
      eachMessage: async ({ partition }) => {
        if (partition !== 1 || later) return
        later = go.promise
          .then(() => b.consumer.commitOffsets([OFFSET]))
          .then(() => b.consumer.pause([{ topic: TOPIC, partitions: [0] }]))
          .then(
            () => 'done',
            (error) => error
          )
      },
    })
    await until(() => later, 'the work the handler of A started')

    await a.consumer.stop()
    go.resolve()

    expect(await later).toBe('done')
    expect(b.commits()).toEqual(oneCommit('group-b', 'member-b'))
    expect(b.consumer.paused()).toEqual([{ topic: TOPIC, partitions: [0] }])
  })

  it('B.connect inside a handler of A waits for the close of B in progress', async () => {
    const b = await subscribed(memberB())
    const release = b.holdClose()
    const closing = b.consumer.disconnect()
    await until(() => b.close.entered, 'the held close of B')
    let call
    await startA({
      eachMessage: async ({ partition }) => {
        if (partition !== 1 || call) return
        const before = b.connects()
        const attempt = b.consumer.connect().then(
          () => 'connected',
          (error) => error
        )
        call = { before, during: b.connects(), attempt, settled: false }
        attempt.then(() => (call.settled = true))
      },
    })
    await until(() => call, 'the connect from the handler of A')

    expect(call.during).toBe(call.before)
    await flush()
    expect(call.settled).toBe(false)

    release()
    await closing
    expect(await call.attempt).toBe('connected')
    expect(b.connects()).toBe(call.before + 1)
  })

  it('B.run inside onPartitionsAssigned of A starts B', async () => {
    const b = await subscribed(memberB())
    let started
    await startA({
      ...idle,
      rebalanceListener: {
        onPartitionsAssigned: async () => {
          started ??= b.consumer.run({ autoCommit: false, ...idle }).then(
            () => 'started',
            (error) => error
          )
        },
      },
    })

    expect(await started).toBe('started')
    expect(b.joins()).toBe(1)
  })

  it('B.commitOffsets from work onPartitionsAssigned of A started commits after that callback returned', async () => {
    const b = await running(memberB(), idle)
    const go = gate()
    let late
    // Resolves after the GROUP_JOIN of A, which follows the return of its callback.
    await startA({
      ...idle,
      rebalanceListener: {
        onPartitionsAssigned: async () => {
          late ??= go.promise
            .then(() => b.consumer.commitOffsets([OFFSET]))
            .then(
              () => 'committed',
              (error) => error
            )
        },
      },
    })

    go.resolve()

    expect(await late).toBe('committed')
    expect(b.commits()).toEqual(oneCommit('group-b', 'member-b'))
  })

  it('B.stop inside onPartitionsRevoked of A settles after the leave of B', async () => {
    const b = await running(memberB(), idle)
    const leave = b.hold('LeaveGroup')
    let stopping
    const a = await startA({
      ...idle,
      rebalanceListener: {
        onPartitionsRevoked: async ({ partitions }) => {
          if (partitions.length === 0 || stopping) return
          stopping = { promise: b.consumer.stop(), settled: false }
          stopping.promise.then(() => (stopping.settled = true))
        },
      },
    })

    await a.consumer.stop()
    await leave.arrived()
    await flush()
    expect(stopping.settled).toBe(false)

    leave.release()
    await stopping.promise
    expect(stopping.settled).toBe(true)
  })

  it('B.connect inside onPartitionsAssigned of A waits for the close of B in progress', async () => {
    const b = await subscribed(memberB())
    const release = b.holdClose()
    const closing = b.consumer.disconnect()
    await until(() => b.close.entered, 'the held close of B')
    let call
    await startA({
      ...idle,
      rebalanceListener: {
        onPartitionsAssigned: async () => {
          if (call) return
          const before = b.connects()
          const attempt = b.consumer.connect().then(
            () => 'connected',
            (error) => error
          )
          call = { before, during: b.connects(), attempt, settled: false }
          attempt.then(() => (call.settled = true))
        },
      },
    })

    expect(call.during).toBe(call.before)
    await flush()
    expect(call.settled).toBe(false)

    release()
    await closing
    expect(await call.attempt).toBe('connected')
    expect(b.connects()).toBe(call.before + 1)
  })

  it('B.disconnect inside onPartitionsRevoked of A settles after the close of B', async () => {
    const b = await running(memberB(), idle)
    const release = b.holdClose()
    let closing
    const a = await startA({
      ...idle,
      rebalanceListener: {
        onPartitionsRevoked: async ({ partitions }) => {
          if (partitions.length === 0 || closing) return
          closing = { promise: b.consumer.disconnect(), settled: false }
          closing.promise.then(() => (closing.settled = true))
        },
      },
    })

    await a.consumer.stop()
    await until(() => b.close.entered, 'the held close of B')
    await flush()
    expect(closing.settled).toBe(false)

    release()
    await closing.promise
    expect(closing.settled).toBe(true)
  })

  it('a B started inside onPartitionsAssigned of A can call A from its own handler after that callback returned', async () => {
    // B is subscribed and idle, and its first Fetch carries a record once released.
    const b = await subscribed(memberB({ batches: 1 }))
    const fetch = b.hold('Fetch')
    let back
    let started
    const handlerOfB = async () => {
      if (back) return
      back = {}
      try {
        a.consumer.pause([{ topic: TOPIC, partitions: [0] }])
        back.pause = 'paused'
      } catch (error) {
        back.pause = error
      }
      back.commit = a.consumer.commitOffsets([OFFSET]).then(
        () => 'committed',
        (error) => error
      )
    }
    // Resolves after the GROUP_JOIN of A, which follows the return of its callback.
    const a = await startA(
      {
        ...idle,
        rebalanceListener: {
          onPartitionsAssigned: async () => {
            started ??= b.consumer.run({ autoCommit: false, eachMessage: handlerOfB }).then(
              () => 'started',
              (error) => error
            )
          },
        },
      },
      { partitions: 1, batches: 0 }
    )
    expect(await started).toBe('started')
    await fetch.arrived()

    fetch.release()
    await until(() => back?.commit, 'the calls from the handler of B')

    expect(back.pause).toBe('paused')
    expect(await back.commit).toBe('committed')
    expect(a.consumer.paused()).toEqual([{ topic: TOPIC, partitions: [0] }])
    expect(a.commits()).toEqual(oneCommit('group-a', 'member-a'))
  })

  it('a B started inside an eachMessage of A can call A from a fetch-loop listener after A was replaced', async () => {
    // B is subscribed and idle, and its first Fetch carries a record once released.
    const b = await subscribed(memberB({ batches: 1 }))
    const fetch = b.hold('Fetch')
    let heard
    b.consumer.on(b.consumer.events.START_BATCH_PROCESS, () => {
      if (heard !== undefined) return
      // A listener's throw is contained, so the outcome is recorded instead.
      try {
        a.consumer.pause([{ topic: TOPIC, partitions: [0] }])
        heard = 'paused'
      } catch (error) {
        heard = error
      }
    })
    let started
    const a = await startA(
      {
        eachMessage: async () => {
          started ??= b.consumer.run({ autoCommit: false, ...idle }).then(
            () => 'started',
            (error) => error
          )
        },
      },
      { partitions: 1 }
    )
    await until(() => started, 'the run of B from the handler of A')
    expect(await started).toBe('started')
    await fetch.arrived()

    await a.consumer.stop()
    await a.consumer.run({ autoCommit: false, ...idle })
    fetch.release()
    await until(() => heard !== undefined, 'the listener of B')

    expect(heard).toBe('paused')
    expect(a.consumer.paused()).toEqual([{ topic: TOPIC, partitions: [0] }])
  })

  it('a handler of a retired run of A still cannot stop the run of A that replaced it', async () => {
    const reached = gate()
    const go = gate()
    let outcome
    const a = await startA(
      {
        eachMessage: async () => {
          if (outcome !== undefined) return
          outcome = null
          reached.resolve()
          await go.promise
          outcome = a.consumer.stop().then(
            () => 'stopped',
            (error) => error
          )
        },
      },
      { partitions: 1 }
    )
    await reached.promise

    await a.consumer.disconnect({ timeout: 0 })
    await a.consumer.connect()
    await a.consumer.run({ autoCommit: false, ...idle })
    go.resolve()
    await until(() => outcome, 'the stop from the retired handler')

    expect(await outcome).toMatchObject({ type: 'CONSUMER_STOPPING' })
    expect(a.consumer.assignment()).toEqual([{ topic: TOPIC, partitions: [0] }])
  })

  it('B.position inside onPartitionsAssigned of A resolves the position of B', async () => {
    const b = await running(memberB(), idle)
    let position
    await startA({
      ...idle,
      rebalanceListener: {
        onPartitionsAssigned: async () => {
          if (position !== undefined) return
          position = null
          position = await b.consumer
            .position({ topic: TOPIC, partition: 0 })
            .catch((error) => error)
        },
      },
    })

    expect(position).toBe('0')
  })
})
