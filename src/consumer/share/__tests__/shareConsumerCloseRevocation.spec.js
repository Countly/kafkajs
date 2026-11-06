const { ShareRun } = require('../shareRun')
const { ShareCloseEpisode } = require('../shareCloseEpisode')
const { ConsumerOperation } = require('../../groupProtocol/interface')
const { KafkaJSNonRetriableError } = require('../../../errors')
const {
  ShareClock,
  acquire,
  deferred,
  fetchAnswer,
  shareClient,
  until,
} = require('../../../../testHelpers/shareHarness')

/** Fake timers, a public share client on a scripted cluster and the observers these tests read. */
class Rig {
  /**
   * @param {object} [options] Rig options.
   * @param {number} [options.interval=600000] The `heartbeatIntervalMs` the coordinator answers.
   */
  constructor({ interval = 600000 } = {}) {
    this.clock = new ShareClock().install()
    this.runs = []
    this.calls = []
    this.acks = []
    this.crashes = []
    this.gate = deferred()
    const runs = this.runs
    const start = ShareRun.prototype.start
    jest.spyOn(ShareRun.prototype, 'start').mockImplementation(function () {
      runs.push(this)
      return start.call(this)
    })
    this.h = shareClient({
      interval,
      kafka: { requestTimeout: 1000, enforceRequestTimeout: false },
    })
    const { events, on } = this.h.consumer
    on(events.CRASH, ({ payload }) => this.crashes.push(payload))
    on(events.ACKNOWLEDGE, ({ payload }) =>
      this.acks.push([
        payload.partition,
        payload.firstOffset,
        payload.lastOffset,
        payload.type,
        payload.error ? payload.error.reason : null,
      ])
    )
  }

  /**
   * Subscribes and runs a handler that records its callbacks, optionally decides, then waits for
   * `gate`.
   * @param {object} [options] Start options.
   * @param {'accept'|'release'|'reject'} [options.decide] An explicit decision made first.
   * @param {Array<object|Function>} [options.fetches] Scripted ShareFetch answers.
   * @returns {Promise<void>} Resolves once the first handler started.
   */
  async begin({ decide, fetches } = {}) {
    await this.h.consumer.subscribe({ topics: ['topic-a'] })
    this.h.script('ShareFetch', fetches ?? [fetchAnswer([acquire(0, 0, ['a'])])])
    await this.h.consumer.run({
      eachMessage: async ({ message, acknowledge, renew, isRunning }) => {
        this.calls.push({ offset: message.offset, acknowledge, renew, isRunning })
        if (decide) acknowledge(decide)
        await this.gate.promise
      },
    })
    await until(() => this.calls.length > 0, 'a held handler')
  }

  /** @returns {object[]} Every final frame (ShareAcknowledge at epoch -1). */
  finals() {
    return this.h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1)
  }

  /** @returns {object[]} Every leave frame (ShareGroupHeartbeat at epoch -1). */
  leaves() {
    return this.h.frames(76).filter(({ fields }) => fields.memberEpoch === -1)
  }

  /** @returns {object[]} Every join frame (ShareGroupHeartbeat at epoch 0). */
  joins() {
    return this.h.frames(76).filter(({ fields }) => fields.memberEpoch === 0)
  }

  /**
   * @param {{node: number, generation: number}} record A recorded request.
   * @returns {object} The fake socket that request travelled on.
   */
  socketOf({ node, generation }) {
    return this.h.cluster.sockets().filter(({ endpoint }) => endpoint.host === `node${node}`)[
      generation - 1
    ]
  }

  /** @returns {Promise<void>} Releases every hold, restores timers and mocks, and closes the client. */
  async end() {
    this.gate.resolve()
    jest.restoreAllMocks()
    this.clock.restore()
    await this.h.close()
  }
}

/**
 * @param {Promise<*>} promise A promise the test does not await yet.
 * @returns {{settled: boolean, error: Error|null}} Its live state, so "still pending" is observable.
 */
const track = (promise) => {
  const state = { settled: false, error: null }
  promise.then(
    () => {
      state.settled = true
    },
    (error) => {
      state.settled = true
      state.error = error
    }
  )
  return state
}

describe('a zero timeout during a running close', () => {
  let rig
  afterEach(async () => {
    await rig?.end()
    rig = undefined
  })

  test.each([
    [
      'a disconnect with a deadline',
      async (r) => {
        const closing = track(r.h.consumer.disconnect({ timeout: 5000 }))
        await r.clock.advance(4000)
        return closing
      },
    ],
    [
      'a stop',
      async (r) => {
        const closing = track(r.h.consumer.stop())
        r.gate.resolve()
        return closing
      },
    ],
  ])(
    'ends a written, unanswered final at once and closes its socket when it follows %s',
    async (_name, startClose) => {
      rig = new Rig()
      await rig.begin({ decide: 'accept' })
      const final = rig.h.hold({
        api: 'ShareAcknowledge',
        match: ({ fields }) => fields.shareSessionEpoch === -1,
      })
      const closing = await startClose(rig)
      await final.arrived()
      expect(rig.finals()).toHaveLength(1)
      expect(closing.settled).toBe(false)
      const before = performance.now()

      const zero = track(rig.h.consumer.disconnect({ timeout: 0 }))
      await until(() => closing.settled && zero.settled, 'the revoked close')

      expect(performance.now()).toBe(before)
      expect(closing.error).toBeNull()
      expect(zero.error).toBeNull()
      expect(rig.acks).toEqual([[0, '0', '0', 'accept', 'session-lost']])
      expect(rig.leaves()).toHaveLength(0)
      expect(rig.finals()).toHaveLength(1)
      expect(rig.socketOf(rig.finals()[0]).destroyed).toBe(true)
    }
  )
})

describe('a fenced run that is waiting for its final', () => {
  let rig
  afterEach(async () => {
    await rig?.end()
    rig = undefined
  })

  test('sends no heartbeat or join frame however long the held final waits within its cap', async () => {
    rig = new Rig({ interval: 100 })
    let fence = false
    await rig.h.consumer.subscribe({ topics: ['topic-a'] })
    rig.h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
    rig.h.script(
      'ShareGroupHeartbeat',
      Array.from({ length: 100 }, () => () => {
        if (!fence) return undefined
        fence = false
        return { errorCode: 110 }
      })
    )
    const final = rig.h.hold({
      api: 'ShareAcknowledge',
      match: ({ fields }) => fields.shareSessionEpoch === -1,
    })
    const started = deferred()
    await rig.h.consumer.run({
      eachMessage: async ({ acknowledge }) => {
        acknowledge()
        started.resolve()
        await rig.gate.promise
      },
    })
    await started.promise
    fence = true
    await rig.clock.advance(100)
    await final.arrived()
    const [run] = rig.runs
    const beats = rig.h.frames(76).length
    expect(run.manager.isRunning()).toBe(false)
    expect(rig.joins()).toHaveLength(1)

    for (let step = 0; step < 3; step++) {
      await rig.clock.advance(300)
      expect(rig.h.frames(76)).toHaveLength(beats)
    }
    expect(run.manager.isRunning()).toBe(false)
    expect(rig.joins()).toHaveLength(1)

    final.release()
    for (let step = 0; step < 10 && rig.joins().length < 2; step++) await rig.clock.advance(100)
    expect(rig.joins()).toHaveLength(2)
    expect(run.manager.isRunning()).toBe(true)
    expect(rig.crashes).toEqual([])
  })
})

describe('a handler that outlives the loss of its data socket', () => {
  let rig
  afterEach(async () => {
    await rig?.end()
    rig = undefined
  })

  test('keeps the run going, settles its acquisition lost and reopens the session', async () => {
    rig = new Rig()
    await rig.begin({
      fetches: [fetchAnswer([acquire(0, 0, ['a'])]), fetchAnswer([acquire(0, 0, ['a'])])],
    })
    const [first] = rig.h.frames(78)
    rig.socketOf(first).destroy()
    rig.gate.resolve()
    for (let step = 0; step < 10 && rig.calls.length < 2; step++) await rig.clock.advance(60)

    expect(rig.crashes).toEqual([])
    expect(rig.calls.map(({ offset }) => offset)).toEqual(['0', '0'])
    expect(rig.acks[0]).toEqual([0, '0', '0', 'accept', 'session-lost'])
    const opens = rig.h.frames(78).filter(({ fields }) => fields.shareSessionEpoch === 0)
    expect(opens).toHaveLength(2)
    expect(opens[1].generation).toBeGreaterThan(opens[0].generation)
    expect(() => rig.calls[0].acknowledge('reject')).toThrow('Handler call is no longer current')
  })
})

describe('a close episode and its traffic operation', () => {
  /** @returns {ShareCloseEpisode} An episode on a clock that never moves. */
  const episodeAtZero = () =>
    new ShareCloseEpisode({ run: { identity: Symbol('run') }, requestTimeout: 1000, now: () => 0 })

  test('retires the attached operation, and wakes its waits, in the tick a zero timeout revokes', async () => {
    const operation = new ConsumerOperation()
    const episode = episodeAtZero()
    episode.attach(operation)
    const outcome = operation
      .wait(() => new Promise(() => {}))
      .then(
        () => null,
        (error) => error
      )
    expect(operation.error).toBeNull()

    episode.disconnect(0)

    expect(operation.error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(operation.error.message).toBe('Captured close is no longer current')
    expect(await outcome).toBe(operation.error)
  })

  test('retires an operation that is attached after the revocation', () => {
    const operation = new ConsumerOperation()
    const episode = episodeAtZero()
    episode.disconnect(0)

    episode.attach(operation)

    expect(operation.error).toBeInstanceOf(KafkaJSNonRetriableError)
  })

  test('leaves the operation running for a nonzero timeout', () => {
    const operation = new ConsumerOperation()
    const episode = episodeAtZero()
    episode.attach(operation)

    episode.disconnect(400)

    expect(episode.revoked).toBe(false)
    expect(operation.error).toBeNull()
    operation.finish()
  })

  test('forgets its operation when it finishes', () => {
    const operation = new ConsumerOperation()
    const episode = episodeAtZero()
    episode.attach(operation)

    episode.finish()

    expect(episode.operation).toBeNull()
    expect(operation.error).toBeNull()
    operation.finish()
  })
})
