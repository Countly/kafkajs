const createFetchManager = require('../fetchManager')
const Runner = require('../runner')
const { FetchOutage } = require('../outage')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { createLogger, LEVELS } = require('../../loggers')
const { KafkaJSConnectionError } = require('../../errors')
const { stampOwnership } = require('../rebalanceErrorContract')
const { newLogger } = require('testHelpers')

const WARNING = 'Lost a connection while fetching, trying again'

/**
 * Lets the continuations and the fetcher's `setImmediate` yield of each round run. The real
 * `setImmediate` is not faked, so this works under fake timers.
 * @returns {Promise<void>}
 */
const settle = async () => {
  for (let turn = 0; turn < 5; turn++) await new Promise(setImmediate)
}

/**
 * Moves the fake clock, then lets what it woke run.
 * @param {number} ms
 * @returns {Promise<void>}
 */
const advance = async (ms) => {
  await jest.advanceTimersByTimeAsync(ms)
  await settle()
}

/**
 * @param {Promise<unknown>} promise
 * @returns {{ done: boolean }} whether the promise has settled, either way
 */
const track = (promise) => {
  const state = { done: false }
  const finish = () => {
    state.done = true
  }
  promise.then(finish, finish)
  return state
}

/**
 * A fetch manager over a node set the test changes and a fetch the test answers one call at a
 * time, so a long poll stays open for exactly as long as the test wants.
 */
class Rig {
  /**
   * @param {number[]} nodeIds the node ids the fake cluster advertises first
   * @param {(error: Error) => number|undefined} retryDelay the manager's decision on a failed fetch
   */
  constructor(nodeIds, retryDelay) {
    this.nodeIds = nodeIds
    /** @type {number[]} the node of each fetch call, in call order */
    this.calls = []
    /** @type {Map<number, Array<{ resolve: Function, reject: Function }>>} */
    this.pending = new Map()
    /** @type {Map<number, number>} fetches open now, per node */
    this.open = new Map()
    /** @type {Map<number, number>} the most fetches ever open at once, per node */
    this.peak = new Map()
    this.running = null
    this.manager = createFetchManager({
      logger: newLogger(),
      getNodeIds: () => [...this.nodeIds],
      fetch: (nodeId) => this.fetch(nodeId),
      retryDelay,
      handler: async () => {},
      concurrency: 1,
    })
  }

  /**
   * @param {number} nodeId
   * @returns {Promise<object[]>} a fetch that stays open until `answer` or `fail`
   */
  fetch(nodeId) {
    this.calls.push(nodeId)
    const open = (this.open.get(nodeId) ?? 0) + 1
    this.open.set(nodeId, open)
    this.peak.set(nodeId, Math.max(this.peak.get(nodeId) ?? 0, open))
    const pending = new Promise((resolve, reject) => {
      const queue = this.pending.get(nodeId) ?? []
      queue.push({ resolve, reject })
      this.pending.set(nodeId, queue)
    })
    const close = () => this.open.set(nodeId, this.open.get(nodeId) - 1)
    pending.then(close, close)
    return pending
  }

  /** Starts the manager and lets its first round run. @returns {Promise<void>} */
  async start() {
    this.running = this.manager.start()
    this.running.catch(() => {})
    await settle()
  }

  /** Ends the oldest open fetch of a node with no batches. @param {number} nodeId @returns {void} */
  answer(nodeId) {
    this.pending.get(nodeId).shift().resolve([])
  }

  /**
   * Fails the oldest open fetch of a node, then lets the failure run.
   * @param {number} nodeId
   * @param {Error} error
   * @returns {Promise<void>}
   */
  async fail(nodeId, error) {
    this.pending.get(nodeId).shift().reject(error)
    await settle()
  }

  /** @param {number} nodeId @returns {number} how many fetches the node has made */
  count(nodeId) {
    return this.calls.filter((node) => node === nodeId).length
  }

  /** Stops the manager, answering whatever stays open. @returns {Promise<void>} */
  async finish() {
    const stopping = this.manager.stop()
    await settle()
    for (const queue of this.pending.values()) {
      for (const { resolve } of queue.splice(0)) resolve([])
    }
    await settle()
    await stopping
    await this.running.catch(() => {})
  }
}

describe('Fetch manager > a retriable fetch error stays on its fetcher', () => {
  beforeEach(() => jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick'] }))
  afterEach(() => jest.useRealTimers())

  it('lets the other nodes keep fetching while one node backs off, then retries that node after its delay', async () => {
    const lost = new KafkaJSConnectionError('lost')
    const retryDelay = jest.fn(() => 100)
    const rig = new Rig([1, 2, 3], retryDelay)
    await rig.start()

    await rig.fail(3, lost)
    rig.answer(1)
    rig.answer(2)
    await settle()

    expect([rig.count(1), rig.count(2), rig.count(3)]).toEqual([2, 2, 1])
    expect(retryDelay).toHaveBeenCalledTimes(1)
    expect(retryDelay).toHaveBeenCalledWith(lost, 3)
    await advance(99)
    expect(rig.count(3)).toBe(1)
    await advance(1)
    expect(rig.count(3)).toBe(2)
    expect(rig.peak.get(3)).toBe(1)
    await rig.finish()
  })

  it('surfaces an error the outage no longer absorbs to start() once, after every fetcher has stopped', async () => {
    const outage = new FetchOutage(100)
    const retryDelay = jest.fn((error, nodeId) =>
      outage.absorbs(error, performance.now(), nodeId) ? 100 : undefined
    )
    const rig = new Rig([1, 2, 3], retryDelay)
    await rig.start()
    await rig.fail(3, new KafkaJSConnectionError('first'))
    await advance(100)
    expect(rig.count(3)).toBe(2)

    const exhausted = new KafkaJSConnectionError('budget spent')
    await rig.fail(3, exhausted)
    const settled = track(rig.running)
    await settle()
    expect(settled.done).toBe(false)

    rig.answer(1)
    rig.answer(2)
    await settle()

    await expect(rig.running).rejects.toBe(exhausted)
    expect(retryDelay).toHaveBeenCalledTimes(2)
    expect([rig.count(1), rig.count(2), rig.count(3)]).toEqual([1, 1, 2])
    expect(jest.getTimerCount()).toBe(0)
  })

  it('stops every fetcher and rejects start() for an error the decision does not accept', async () => {
    const retryDelay = jest.fn(() => undefined)
    const rig = new Rig([1, 2, 3], retryDelay)
    await rig.start()
    const denied = new Error('denied')

    await rig.fail(2, denied)
    rig.answer(1)
    rig.answer(3)
    await settle()

    await expect(rig.running).rejects.toBe(denied)
    expect(retryDelay).toHaveBeenCalledWith(denied, 2)
    expect([rig.count(1), rig.count(2), rig.count(3)]).toEqual([1, 1, 1])
  })

  it('keeps the old behaviour when no decision is given: the first error stops every fetcher', async () => {
    const rig = new Rig([1, 2], undefined)
    await rig.start()
    const lost = new KafkaJSConnectionError('lost')

    await rig.fail(2, lost)
    rig.answer(1)
    await settle()

    await expect(rig.running).rejects.toBe(lost)
    expect(jest.getTimerCount()).toBe(0)
  })

  it('cancels the backoff of a node when stop() runs during it, and settles', async () => {
    const rig = new Rig([3], () => 100)
    await rig.start()
    await rig.fail(3, new KafkaJSConnectionError('lost'))
    expect(jest.getTimerCount()).toBe(1)

    await rig.manager.stop()
    await rig.running

    expect(jest.getTimerCount()).toBe(0)
    await advance(1000)
    expect(rig.count(3)).toBe(1)
  })

  it('makes stop() wait for the fetchers that still have a fetch open, not for the backing-off one', async () => {
    const rig = new Rig([1, 3], () => 100)
    await rig.start()
    await rig.fail(3, new KafkaJSConnectionError('lost'))

    const stopped = track(rig.manager.stop())
    await settle()
    expect(stopped.done).toBe(false)
    rig.answer(1)
    await settle()

    expect(stopped.done).toBe(true)
    expect(jest.getTimerCount()).toBe(0)
    expect(rig.count(3)).toBe(1)
    await rig.running
  })

  it('does not fetch again when the failure arrives after the fetcher was stopped', async () => {
    const retryDelay = jest.fn(() => 100)
    const rig = new Rig([3], retryDelay)
    await rig.start()

    const stopping = rig.manager.stop()
    await rig.fail(3, new KafkaJSConnectionError('lost'))
    await stopping
    await rig.running

    expect(jest.getTimerCount()).toBe(0)
    expect(rig.count(3)).toBe(1)
  })

  it('keeps one fetcher per node across a backoff, while a node that joined starts at once', async () => {
    const rig = new Rig([1, 2, 3], () => 100)
    await rig.start()
    await rig.fail(3, new KafkaJSConnectionError('lost'))

    rig.nodeIds = [1, 2, 3, 4]
    rig.answer(1)
    await settle()
    expect(rig.count(4)).toBe(1)

    await advance(100)

    expect(rig.count(3)).toBe(2)
    expect([rig.peak.get(3), rig.peak.get(4)]).toEqual([1, 1])
    await rig.finish()
  })

  it('retires the backing-off fetcher of a node that left, and starts a new one when it returns', async () => {
    const rig = new Rig([1, 2, 3], () => 100)
    await rig.start()
    await rig.fail(3, new KafkaJSConnectionError('lost'))
    expect(jest.getTimerCount()).toBe(1)

    rig.nodeIds = [1, 2]
    rig.answer(1)
    await settle()
    expect(jest.getTimerCount()).toBe(0)
    await advance(1000)
    expect(rig.count(3)).toBe(1)

    rig.nodeIds = [1, 2, 3]
    rig.answer(2)
    await settle()

    expect(rig.count(3)).toBe(2)
    expect(rig.peak.get(3)).toBe(1)
    await rig.finish()
  })
})

describe('Runner > which fetch failures stay on their fetcher', () => {
  /** @type {object[]} what the runner logged at warn level */
  let warnings

  /**
   * Builds a runner over a stand-in group; no broker or membership is involved.
   * @param {'classic'|'consumer'|'manual'} [kind]
   * @returns {Runner}
   */
  const createRunner = (kind = 'classic') => {
    warnings = []
    const logger = createLogger({
      level: LEVELS.WARN,
      logCreator:
        () =>
        ({ namespace, log }) =>
          warnings.push({ namespace, ...log }),
    })
    return new Runner({
      consumerGroup: { kind, groupId: 'g', memberId: 'm', attach: jest.fn(), getNodeIds: () => [] },
      onCrash: jest.fn(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger,
      eachBatch: jest.fn(),
      concurrency: 1,
      heartbeatInterval: 3000,
      maxPollInterval: 300000,
      sessionTimeout: 30000,
      retry: { retries: 0, initialRetryTime: 5 },
    })
  }

  /** @returns {Error} a lost connection raised by a Fetch */
  const lostFetch = (runner) =>
    runner.markOwnRequest(new KafkaJSConnectionError('connection lost'), 'fetch')

  it.each(['classic', 'consumer'])(
    'gives a %s run the backoff its fetch loop would use, and warns as it does',
    (kind) => {
      const runner = createRunner(kind)
      const error = lostFetch(runner)

      const delay = runner.fetchRetryDelay(error, 3)

      expect(typeof delay).toBe('number')
      expect(warnings).toHaveLength(1)
      expect(warnings[0]).toMatchObject({
        namespace: 'Runner',
        message: WARNING,
        groupId: 'g',
        memberId: 'm',
        error: 'connection lost',
        requestClass: 'fetch',
        delay,
      })
    }
  )

  it('leaves a manual run to the retrier, whose counted retries it never rode out', () => {
    const runner = createRunner('manual')

    expect(runner.fetchRetryDelay(lostFetch(runner), 3)).toBeUndefined()
    expect(warnings).toEqual([])
  })

  it('leaves an error the fetch loop would not ride out to the loop', () => {
    const runner = createRunner()

    expect(runner.fetchRetryDelay(new Error('not a connection'), 3)).toBeUndefined()
    expect(runner.fetchRetryDelay(new KafkaJSConnectionError('unmarked'), 3)).toBeUndefined()
    expect(warnings).toEqual([])
  })

  it.each(['heartbeat', 'handler'])(
    'leaves a lost connection of the %s class to the loop: it is not a broker fetch',
    (requestClass) => {
      const runner = createRunner()
      const error = runner.markOwnRequest(new KafkaJSConnectionError('lost'), requestClass)

      expect(runner.fetchRetryDelay(error, 3)).toBeUndefined()
      expect(warnings).toEqual([])
    }
  )

  it('stops accepting once the fetch outage has outlasted the session timeout, as the loop does', () => {
    const runner = createRunner()
    runner.outages.fetch.absorbs(lostFetch(runner), performance.now() - 60000, 3)

    expect(runner.fetchRetryDelay(lostFetch(runner), 3)).toBeUndefined()
    expect(runner.rideOutage(lostFetch(runner), 3)).toBeUndefined()
    expect(warnings).toEqual([])
  })

  it('leaves an error that classifies as a rebalance to the loop', () => {
    const runner = createRunner()
    jest.spyOn(runner.rebalanceCoordinator, 'classify').mockReturnValue({ kind: 'lost' })

    expect(runner.fetchRetryDelay(lostFetch(runner), 3)).toBeUndefined()
    expect(warnings).toEqual([])
  })

  it('leaves everything to the loop while a verdict waits in the slot, and leaves the slot alone', () => {
    const runner = createRunner()
    const verdict = { kind: 'revoked', reason: 'rebalance-in-progress', error: new Error('x') }
    runner.rebalanceCoordinator.verdict = verdict

    expect(runner.fetchRetryDelay(lostFetch(runner), 3)).toBeUndefined()
    expect(runner.rebalanceCoordinator.verdict).toBe(verdict)
  })

  it('leaves an answer for a membership already replaced to the loop', () => {
    const runner = createRunner()
    runner.consumerGroup.ownership = { token: () => 2 }
    const stale = stampOwnership(lostFetch(runner), 1)

    expect(runner.fetchRetryDelay(stale, 3)).toBeUndefined()
    expect(warnings).toEqual([])
  })

  it('counts the wait as a poll only while no handler runs', () => {
    const runner = createRunner()

    runner.pollTimer.lastPollAt = 0
    runner.inFlightHandlers = 1
    expect(runner.fetchRetryDelay(lostFetch(runner), 3)).toEqual(expect.any(Number))
    expect(runner.pollTimer.lastPollAt).toBe(0)

    runner.inFlightHandlers = 0
    expect(runner.fetchRetryDelay(lostFetch(runner), 3)).toEqual(expect.any(Number))
    expect(runner.pollTimer.lastPollAt).toBeGreaterThan(0)
  })
})
