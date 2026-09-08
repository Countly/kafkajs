const PollTimer = require('../pollTimer')
const Runner = require('../runner')
const Batch = require('../batch')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { newLogger } = require('testHelpers')

describe('Consumer > PollTimer', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => jest.useRealTimers())

  it('expires once the deadline has passed, and not before', () => {
    const timer = new PollTimer({ pollDeadline: 1000 })

    jest.advanceTimersByTime(999)
    expect(timer.expired()).toBe(false)

    jest.advanceTimersByTime(2)
    expect(timer.expired()).toBe(true)
    expect(timer.elapsed()).toBeGreaterThan(1000)
  })

  it('is monotonic: a wall-clock jump does not expire a healthy consumer', () => {
    const timer = new PollTimer({ pollDeadline: 1000 })

    // Modern fake timers move performance.now() with advanceTimersByTime but NOT with
    // setSystemTime - which is exactly the difference this timer relies on.
    jest.setSystemTime(new Date(Date.now() + 3600000))

    expect(timer.expired()).toBe(false)
  })

  it('poll() and reset() both restart the deadline', () => {
    const timer = new PollTimer({ pollDeadline: 1000 })

    jest.advanceTimersByTime(900)
    timer.poll()
    jest.advanceTimersByTime(900)
    expect(timer.expired()).toBe(false)

    jest.advanceTimersByTime(200)
    expect(timer.expired()).toBe(true)

    timer.reset()
    expect(timer.expired()).toBe(false)
  })

  it('stops reporting after stop() and reports again after reset()', () => {
    const timer = new PollTimer({ pollDeadline: 10 })

    jest.advanceTimersByTime(100)
    expect(timer.expired()).toBe(true)

    timer.stop()
    expect(timer.expired()).toBe(false)

    timer.reset()
    jest.advanceTimersByTime(100)
    expect(timer.expired()).toBe(true)
  })

  it('reports the configured deadline', () => {
    expect(new PollTimer({ pollDeadline: 300000 }).deadline()).toEqual(300000)
  })
})

describe('Consumer > PollTimer call sites', () => {
  const buildRunner = (options = {}) => {
    const consumerGroup = {
      groupId: 'poll-group',
      memberId: 'm',
      generationId: 1,
      leaderId: 'm',
      groupInstanceId: null,
      groupMembership: {},
      assignmentEpoch: 1,
      isLeader: () => true,
      rebalanceProtocol: () => 'eager',
      attach: jest.fn(),
      getNodeIds: () => [1],
      connect: jest.fn(),
      joinAndSync: jest.fn(async () => ({ assignment: {} })),
      applyRebalanceResult: jest.fn(async () => ({ assignment: [], added: [], revoked: [] })),
      assigned: () => [],
      fetch: jest.fn(async () => []),
      resolveOffset: jest.fn(),
      commitOffsets: jest.fn(),
      commitOffsetsIfNecessary: jest.fn(),
      uncommittedOffsets: jest.fn(),
      heartbeat: jest.fn(),
      isPaused: () => false,
      isAssigned: () => true,
      // `processEachMessage` calls this OUTSIDE its try, so a missing stub rejects the batch case
      // with a TypeError rather than failing an assertion.
      hasSeekOffset: () => false,
      discardUncommittedOffsets: jest.fn(),
      clearStaleAssignmentPending: jest.fn(),
      // The membership seam the runner drives: the real ConsumerGroup answers both.
      heartbeatInterval: () => 3000,
      onStopRequested: jest.fn(),
      forgetOwnedPartitions: jest.fn(),
      clearAssignerState: jest.fn(),
    }

    return new Runner({
      logger: newLogger(),
      consumerGroup,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      concurrency: 1,
      heartbeatInterval: 3000,
      maxPollInterval: 300000,
      eachBatch: async () => {},
      onCrash: jest.fn(),
      ...options,
    })
  }

  it('resets the deadline once per batch hand-off, before the handler, and not per message', async () => {
    const seen = []
    const runner = buildRunner({
      eachMessage: async () => seen.push(runner.pollTimer.lastPollAt),
      eachBatch: undefined,
    })
    runner.running = true
    const poll = jest.spyOn(runner.pollTimer, 'poll')

    await runner.handleBatch(
      new Batch('t', 0, {
        partition: 0,
        highWatermark: 10,
        messages: [
          { offset: '0', key: null, value: null },
          { offset: '1', key: null, value: null },
          { offset: '2', key: null, value: null },
        ],
      })
    )

    expect(poll).toHaveBeenCalledTimes(1)
    expect(new Set(seen).size).toEqual(1)
  })

  it('resets the deadline on a fetch round that produced nothing', async () => {
    const runner = buildRunner()
    runner.running = true
    const poll = jest.spyOn(runner.pollTimer, 'poll')

    await runner.fetch(1)

    expect(poll).toHaveBeenCalledTimes(1)
    // The existing heartbeat on the empty-round path is KEPT: it is what wakes an idle consumer.
    expect(runner.consumerGroup.heartbeat).toHaveBeenCalled()
  })

  it('resets the deadline when the fetch manager (re)starts', async () => {
    const runner = buildRunner()
    runner.running = true

    // `scheduleFetchManager()` is re-entered as soon as the retrier resolves, so an
    // instantly-resolving start() recurses in microtasks forever and a setTimeout-based wait
    // never fires. Block on a deferred instead, and stop the loop on the first pass.
    let release
    const blocked = new Promise((resolve) => (release = resolve))
    runner.fetchManager = {
      start: jest.fn(async () => {
        runner.stopRequested = true
        runner.running = false
        release()
        return blocked
      }),
      stop: jest.fn(async () => {}),
    }
    const reset = jest.spyOn(runner.pollTimer, 'reset')

    runner.scheduleFetchManager()
    await blocked

    expect(reset).toHaveBeenCalledTimes(1)
    expect(runner.fetchManager.start).toHaveBeenCalledTimes(1)
  })

  it('arms a timer after the initial round and leaves none after stop()', async () => {
    jest.useFakeTimers()
    const runner = buildRunner()
    runner.scheduleFetchManager = jest.fn()

    try {
      await runner.start()
      expect(runner.heartbeatManager.isRunning()).toBe(true)
      expect(jest.getTimerCount()).toEqual(1)

      await runner.stop()
      expect(jest.getTimerCount()).toEqual(0)
    } finally {
      runner.heartbeatManager.stop()
      jest.useRealTimers()
    }
  })
})
