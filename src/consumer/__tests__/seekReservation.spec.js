const {
  TOPIC,
  createScheduledOffsetManager,
  groupOver,
} = require('../offsetManager/__tests__/fixtures/scheduledOffsetManager')
const { FetchPreparation } = require('../fetchPreparation')
const { ConsumerOperation } = require('../groupProtocol/interface')

const KEY = `${TOPIC}|0`
const CELL = { topic: TOPIC, partition: 0 }

const flush = async () => {
  for (let turn = 0; turn < 5; turn++) await new Promise(setImmediate)
}

/** @returns {{ promise: Promise<object>, resolve: Function, reject: Function }} */
const deferred = () => {
  let resolve, reject
  const promise = new Promise((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

/**
 * @param {number} at the acquisition clock value
 * @returns {import('../handlerContext').Acquisition}
 */
const acquisition = (at) => ({
  acquiredAt: at,
  changedAt: at,
  recreatedAt: 0,
  soughtAt: 0,
  retired: false,
})

/**
 * A real group, offset manager and ledger with auto-commit on, partitions 0 and 1 acquired, a
 * committed offset of 2 and progress resolved to 3 on partition 0 and 8 on partition 1. The
 * coordinator stores a commit when it arrives; `holdCommit()` makes the next one wait, unstored,
 * until the test answers it.
 * @param {object} [options]
 * @param {import('../offsetManager/offsetOperations')} [options.operations] a ledger shared with
 *   another consumer of the group
 * @returns {object} the fixture
 */
const open = ({ operations } = {}) => {
  const ctx = createScheduledOffsetManager({
    autoCommit: true,
    committed: '2',
    partitions: [0, 1],
    ...(operations && { operations }),
  })
  const group = groupOver(ctx, [{ topic: TOPIC, partitions: [0, 1] }])
  group.assignmentEpoch = 1
  group.acquireClock = 1
  group.acquired.set(KEY, acquisition(1))
  group.acquired.set(`${TOPIC}|1`, acquisition(1))
  ctx.manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '2' })
  ctx.manager.resolveOffset({ topic: TOPIC, partition: 1, offset: '7' })
  const wire = []
  const store = ctx.coordinator.offsetCommit.getMockImplementation()
  ctx.coordinator.offsetCommit.mockImplementation((payload, ...rest) => {
    wire.push(
      ...payload.topics.flatMap(({ partitions }) =>
        partitions.map(({ partition, offset }) => `${partition}:${offset}`)
      )
    )
    return store(payload, ...rest)
  })
  /**
   * Holds the next OffsetCommit unanswered and unstored.
   * @returns {{ arrived: Promise<void>, accept: () => void, fail: (error: Error) => void }}
   */
  const holdCommit = () => {
    const arrived = deferred()
    const answer = deferred()
    ctx.coordinator.offsetCommit.mockImplementationOnce((payload) => {
      payload.topics.forEach(({ partitions }) =>
        partitions.forEach(({ partition, offset }) => wire.push(`${partition}:${offset}`))
      )
      arrived.resolve()
      return answer.promise.then(() => {
        payload.topics.forEach(({ partitions }) =>
          partitions.forEach(({ partition, offset }) => {
            ctx.stored[partition] = offset
          })
        )
        return {}
      })
    })
    return {
      arrived: arrived.promise,
      accept: () => answer.resolve(),
      fail: (error) => answer.reject(error),
    }
  }
  return { ...ctx, group, wire, holdCommit }
}

/**
 * Ends a fixture's run and checks it left nothing behind: no ledger cell or reservation, no
 * position waiter and no release registration.
 * @param {...object} fixtures
 * @returns {void}
 */
const audit = (...fixtures) => {
  for (const { group } of fixtures) group.idleWait.dispose()
  for (const { group, manager, operations } of fixtures) {
    manager.clearPendingSeeks()
    expect(operations.reserved).toBe(0)
    expect(operations.size()).toBe(0)
    expect(group.idleWait.waiters.size).toBe(0)
    expect(operations.releaseWakes.size).toBe(0)
  }
}

const failureOf = (promise) =>
  promise.then(
    () => null,
    (error) => error
  )

describe('a seek reservation', () => {
  let fixtures
  beforeEach(() => {
    fixtures = []
  })
  afterEach(() => audit(...fixtures))
  const fixture = (options) => {
    const ctx = open(options)
    fixtures.push(ctx)
    return ctx
  }

  test.each(['position', 'fetch round'])(
    'a seek applied by a %s succeeds before a seek admitted while its commit is unanswered can send',
    async (path) => {
      const h = fixture()
      const first = h.holdCommit()
      h.group.seek({ ...CELL, offset: '5' })
      const parts = [{ topic: TOPIC, partitions: [0] }]
      const older =
        path === 'position'
          ? h.group.position(CELL)
          : h.group.seekOffsets(
              parts,
              new FetchPreparation(
                h.group,
                h.group.idleWait,
                parts,
                Infinity,
                h.group.ownership.token()
              )
            )
      await first.arrived
      expect(h.operations.reserved).toBe(1)
      expect(h.group.isSeekGated(TOPIC, 0)).toBe(true)

      h.group.seek({ ...CELL, offset: '6' })
      const second = h.holdCommit()
      const newer = failureOf(h.group.position(CELL))
      await flush()
      expect(h.wire).toEqual(['0:5'])

      first.accept()
      await second.arrived
      expect(h.wire).toEqual(['0:5', '0:6'])
      const rejected = new Error('the broker rejected the newer seek')
      second.fail(rejected)

      const outcomes = [await failureOf(older), await newer]
      expect(outcomes.filter((outcome) => outcome?.message === rejected.message)).toHaveLength(1)
      expect(h.operations.reserved).toBe(0)
      expect(h.stored[0]).toBe('5')
      expect(await h.group.position(CELL)).toBe('5')
      expect(h.wire).toEqual(['0:5', '0:6'])
    }
  )

  test('only the latest of two seeks admitted before either was popped is sent', async () => {
    const h = fixture()
    h.group.seek({ ...CELL, offset: '5' })
    h.group.seek({ ...CELL, offset: '7' })

    expect(await h.group.position(CELL)).toBe('7')
    expect(h.wire).toEqual(['0:7'])
    expect(h.stored[0]).toBe('7')
  })

  test.each([
    [
      'acquired again',
      (group) => {
        group.acquired.get(KEY).retired = true
        group.acquired.set(KEY, acquisition(++group.acquireClock))
        group.assignmentEpoch += 1
      },
    ],
    ['recreated', (group) => group.markRecreated(TOPIC, 0)],
    ['taken over by a new run', (group) => group.startIdleWait()],
  ])(
    'a seek admitted after the partition was %s is held back only from popping until the earlier seek commit settled',
    async (change, replace) => {
      const h = fixture()
      const first = h.holdCommit()
      h.group.seek({ ...CELL, offset: '5' })
      const older = failureOf(h.group.position(CELL))
      await first.arrived
      replace(h.group)

      expect(h.group.isSeekGated(TOPIC, 0)).toBe(false)
      expect(h.operations.reserved).toBe(1)
      h.group.seek({ ...CELL, offset: '7' })
      const newer = h.group.position(CELL)
      await flush()
      expect(h.wire).toEqual(['0:5'])

      first.accept()

      expect(await newer).toBe('7')
      await older
      expect(h.wire).toEqual(['0:5', '0:7'])
      expect(h.stored[0]).toBe('7')
    }
  )

  test('a reservation held by another consumer of the group blocks only the pop of a new seek', async () => {
    const other = fixture()
    const h = fixture({ operations: other.operations })
    const first = other.holdCommit()
    other.group.seek({ ...CELL, offset: '5' })
    const older = other.group.position(CELL)
    await first.arrived

    expect(h.group.isSeekGated(TOPIC, 0)).toBe(false)
    expect(h.group.hasSeekGates()).toBe(true)
    h.group.seek({ ...CELL, offset: '7' })
    expect(h.group.isSeekGated(TOPIC, 0)).toBe(true)
    const newer = h.group.position(CELL)
    await flush()
    expect(other.wire).toEqual(['0:5'])
    expect(h.wire).toEqual([])

    first.accept()

    await older
    expect(await newer).toBe('7')
    expect(other.wire).toEqual(['0:5'])
    expect(h.wire).toEqual(['0:7'])
  })

  test('a waiting position caller whose deadline passes leaves its seek queued and no waiter behind', async () => {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask'] })
    try {
      const h = fixture()
      const first = h.holdCommit()
      h.group.seek({ ...CELL, offset: '5' })
      const older = h.group.position(CELL)
      await first.arrived
      h.group.seek({ ...CELL, offset: '7' })
      const operation = new ConsumerOperation({ deadlineAt: performance.now() + 30 })
      const expired = failureOf(operation.wait(() => h.group.position(CELL, { operation })))
      await flush()
      expect(h.group.idleWait.waiters.size).toBe(1)

      await jest.advanceTimersByTimeAsync(31)

      expect(await expired).toBeInstanceOf(Error)
      expect(h.group.idleWait.waiters.size).toBe(0)
      expect(h.group.hasSeekOffset(CELL)).toBe(true)
      expect(h.wire).toEqual(['0:5'])
      first.accept()
      expect(await older).toBe('7')
      expect(h.wire).toEqual(['0:5', '0:7'])
    } finally {
      jest.useRealTimers()
    }
  })

  /**
   * Sends the fixture's next OffsetCommit through a real request queue, which writes it and then
   * owns its end: an answer, the queue's request timeout or the close of its connection.
   * @param {object} h the fixture
   * @param {{ enforceRequestTimeout: boolean }} options
   * @returns {{ queue: RequestQueue, written: Promise<void> }}
   */
  const sendThroughQueue = (h, { enforceRequestTimeout }) => {
    const RequestQueue = require('../../network/requestQueue')
    const queue = new RequestQueue({
      maxInFlightRequests: 1,
      requestTimeout: 1000,
      enforceRequestTimeout,
      clientId: 'test',
      broker: 'coordinator:9092',
      logger: h.group.logger,
    })
    queue.scheduleRequestTimeoutCheck()
    const written = deferred()
    h.coordinator.offsetCommit.mockImplementationOnce(
      (payload) =>
        new Promise((resolve, reject) => {
          queue.push({
            entry: {
              correlationId: 1,
              apiKey: 8,
              apiName: 'OffsetCommit',
              apiVersion: 9,
              resolve,
              reject,
            },
            expectResponse: true,
            sendRequest: () => {
              h.wire.push(`0:${payload.topics[0].partitions[0].offset}`)
              written.resolve()
            },
          })
        })
    )
    return { queue, written: written.promise }
  }

  test.each([
    ['times out', (queue) => jest.advanceTimersByTimeAsync(1200)],
    [
      'is closed with its connection',
      async (queue) => queue.rejectAll(new Error('Connection closed')),
    ],
  ])(
    'the reservation outlives the caller of its seek and ends only when the commit request %s',
    async (end, settle) => {
      jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask'] })
      try {
        const h = fixture()
        const { queue, written } = sendThroughQueue(h, { enforceRequestTimeout: true })
        h.group.seek({ ...CELL, offset: '5' })
        const operation = new ConsumerOperation({ deadlineAt: performance.now() + 30 })
        const expired = failureOf(operation.wait(() => h.group.position(CELL, { operation })))
        await written
        await jest.advanceTimersByTimeAsync(31)
        expect(await expired).toBeInstanceOf(Error)
        expect(h.operations.reserved).toBe(1)
        expect(queue.inflight.size).toBe(1)
        h.group.seek({ ...CELL, offset: '7' })
        const newer = h.group.position(CELL)
        await flush()
        expect(h.wire).toEqual(['0:5'])

        await settle(queue)

        expect(await newer).toBe('7')
        expect(h.wire).toEqual(['0:5', '0:7'])
        expect(h.stored[0]).toBe('7')
        expect(queue.inflight.size + queue.pending.length).toBe(0)
        queue.destroy()
      } finally {
        jest.useRealTimers()
      }
    }
  )

  test('with no request timeout, the reservation survives time and a stop until its connection closes', async () => {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask'] })
    try {
      const h = fixture()
      const { queue, written } = sendThroughQueue(h, { enforceRequestTimeout: false })
      h.group.seek({ ...CELL, offset: '5' })
      const older = failureOf(h.group.position(CELL))
      await written
      await jest.advanceTimersByTimeAsync(10 * 60 * 1000)
      h.group.idleWait.dispose()

      expect(h.operations.reserved).toBe(1)
      expect(queue.inflight.size).toBe(1)
      queue.rejectAll(new Error('Connection closed'))
      expect((await older).message).toBe('Connection closed')
      expect(h.operations.reserved).toBe(0)
      expect(queue.inflight.size + queue.pending.length).toBe(0)

      h.group.startIdleWait()
      h.group.seek({ ...CELL, offset: '7' })
      expect(await h.group.position(CELL)).toBe('7')
      expect(h.wire).toEqual(['0:5', '0:7'])
    } finally {
      jest.useRealTimers()
    }
  })

  test.each([
    ['accepted', '6'],
    ['rejected', null],
  ])(
    'a position read of a halted partition waits for a queued seek held back by a seek in flight, then reports what the newer seek did when it is %s',
    async (outcome, position) => {
      const h = fixture()
      const halt = new Error('the partition is out of range')
      h.group.haltedPartitions.set(KEY, { reason: 'offset-out-of-range', error: halt })
      const first = h.holdCommit()
      h.group.seek({ ...CELL, offset: '5' })
      const parts = [{ topic: TOPIC, partitions: [0] }]
      const round = failureOf(
        h.group.seekOffsets(
          parts,
          new FetchPreparation(
            h.group,
            h.group.idleWait,
            parts,
            Infinity,
            h.group.ownership.token()
          )
        )
      )
      await first.arrived
      h.group.seek({ ...CELL, offset: '6' })
      const second = h.holdCommit()
      const read = h.group.position(CELL).then(
        (value) => ({ value }),
        (error) => error
      )
      await flush()
      expect(await Promise.race([read, flush().then(() => 'waiting')])).toBe('waiting')
      expect(h.wire).toEqual(['0:5'])

      first.accept()
      await second.arrived
      expect(h.wire).toEqual(['0:5', '0:6'])
      const rejected = new Error('the broker rejected the newer seek')
      if (position) second.accept()
      else second.fail(rejected)

      const result = await read
      if (position) expect(result).toEqual({ value: position })
      else expect(result).toBe(rejected)
      expect(await round).toBeNull()
    }
  )

  test('a log creator that edits the logged seek cannot send it to another partition', async () => {
    const h = fixture()
    const { createLogger, LEVELS } = require('../../loggers')
    h.group.logger = createLogger({
      level: LEVELS.DEBUG,
      logCreator:
        () =>
        ({ log }) => {
          if (log.message === 'Seek offset') log.seek.partition = 1
        },
    }).namespace('ConsumerGroup')
    const first = h.holdCommit()
    h.group.seek({ topic: TOPIC, partition: 1, offset: '5' })
    const older = h.group.position({ topic: TOPIC, partition: 1 })
    await first.arrived
    h.group.seek({ ...CELL, offset: '7' })

    expect(await h.group.position(CELL)).toBe('7')
    expect(h.wire).toEqual(['1:5', '0:7'])
    first.accept()
    expect(await older).toBe('5')
  })

  test('a position read from a rebalance callback while the run stops applies and waits for no seek', async () => {
    const h = fixture()
    h.group.seek({ ...CELL, offset: '7' })
    h.group.isStopping = () => true
    const RebalanceHooks = require('../rebalanceHooks')
    const current = jest.spyOn(RebalanceHooks, 'current').mockReturnValue({})
    const refusal = jest.spyOn(RebalanceHooks, 'refusal').mockReturnValue(null)
    const fence = jest.spyOn(RebalanceHooks, 'fence').mockReturnValue(h.group.idleWait.fence)
    try {
      expect(await h.group.position(CELL)).toBe('3')
      expect(h.wire).toEqual([])
      expect(h.group.hasSeekOffset(CELL)).toBe(true)
      expect(h.operations.reserved).toBe(0)
    } finally {
      current.mockRestore()
      refusal.mockRestore()
      fence.mockRestore()
    }
  })
})
