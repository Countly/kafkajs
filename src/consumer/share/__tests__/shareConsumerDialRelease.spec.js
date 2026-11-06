const {
  ShareClock,
  acquire,
  deferred,
  fetchAnswer,
  shareClient,
  until,
} = require('../../../../testHelpers/shareHarness')

const STALE = 'Handler call is no longer current'

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

/**
 * A public share client under a controlled clock whose first handler holds a delivered record while
 * the node's data socket is lost and its cluster stops letting new dials through. When the handler
 * returns, the session settles lost and the node's next opening request waits on a held dial.
 */
class DialRig {
  constructor() {
    this.clock = new ShareClock().install()
    this.h = shareClient({
      interval: 100,
      kafka: { requestTimeout: 1000, enforceRequestTimeout: false },
    })
    this.gate = deferred()
    this.calls = []
    this.acks = []
    this.crashes = []
    this.joins = []
    this.trigger = null
    const { events, on } = this.h.consumer
    on(events.CRASH, ({ payload }) => this.crashes.push(payload))
    on(events.GROUP_JOIN, ({ payload }) => this.joins.push(payload))
    on(events.ACKNOWLEDGE, ({ payload }) =>
      this.acks.push([
        payload.firstOffset,
        payload.lastOffset,
        payload.type,
        payload.error ? payload.error.reason : null,
      ])
    )
  }

  /**
   * Starts a run whose first handler waits on `gate`, then loses the data socket, blackholes the
   * node, releases the handler and waits until the node's reopening dial is held.
   * @returns {Promise<void>} Resolves once exactly one dial to node 0 is held.
   */
  async holdReopeningDial() {
    const { h } = this
    await h.consumer.subscribe({ topics: ['topic-a'] })
    h.script('ShareFetch', [
      fetchAnswer([acquire(0, 0, ['a'])]),
      fetchAnswer([acquire(0, 0, ['a'])]),
    ])
    h.script(
      'ShareGroupHeartbeat',
      Array.from({ length: 100 }, () => () => {
        if (this.trigger === null) return undefined
        const errorCode = this.trigger
        this.trigger = null
        return { errorCode }
      })
    )
    await h.consumer.run(this.handler())
    await until(() => this.calls.length === 1, 'the first handler')
    h.cluster
      .sockets()
      .filter(({ endpoint }) => endpoint.host === 'node0')
      [h.frames(78)[0].generation - 1].destroy()
    h.cluster.blackhole(0)
    this.gate.resolve()
    await this.advanceUntil(() => h.cluster.heldDialsTo(0) === 1, 'the held data dial')
  }

  /** @returns {object} A `run` argument whose handlers record their callbacks; only the first one waits. */
  handler() {
    return {
      eachMessage: async ({ message, acknowledge, renew }) => {
        this.calls.push({ offset: message.offset, acknowledge, renew })
        if (this.calls.length === 1) await this.gate.promise
      },
    }
  }

  /**
   * Moves the controlled clock in small steps until a condition holds.
   * @param {() => boolean} condition The stage that is awaited.
   * @param {string} what What is awaited, named by the failure.
   * @returns {Promise<void>}
   * @throws {Error} When the condition does not hold after 30 steps of 60 fake milliseconds.
   */
  async advanceUntil(condition, what) {
    for (let step = 0; step < 30 && !condition(); step++) await this.clock.advance(60)
    if (!condition()) throw new Error(`${what} did not happen`)
  }

  /**
   * Makes the next heartbeat answer with a membership code, and delivers it.
   * @param {number} errorCode The top-level code of the answer.
   * @returns {Promise<void>}
   */
  async answerNextHeartbeat(errorCode) {
    this.trigger = errorCode
    await this.clock.advance(100)
  }

  /** @returns {Promise<void>} Lets the held dial through and delivers whatever it unblocks. */
  async releaseDial() {
    this.h.cluster.up(0)
    this.h.cluster.release(0)
    await this.clock.advance(60)
  }

  /** @returns {Promise<void>} Releases every hold, restores timers and mocks, and closes the client. */
  async end() {
    this.gate.resolve()
    jest.restoreAllMocks()
    this.clock.restore()
    await this.h.close()
  }
}

describe('a data dial held after the session was lost', () => {
  let rig
  afterEach(async () => {
    await rig?.end()
    rig = undefined
  })

  /**
   * Checks the state every case shares while the dial is held: the lost acquisition settled once,
   * nothing was written or dialled for it, and its old callbacks refuse.
   * @returns {void}
   */
  const expectHeld = () => {
    const { h } = rig
    expect(rig.acks).toEqual([['0', '0', 'accept', 'session-lost']])
    expect(h.frames(78)).toHaveLength(1)
    expect(h.frames(79)).toHaveLength(0)
    expect(() => rig.calls[0].acknowledge('reject')).toThrow(STALE)
    expect(() => rig.calls[0].renew()).toThrow(STALE)
    expect(h.cluster.heldDialsTo(0)).toBe(1)
    expect(h.frames(78)).toHaveLength(1)
    expect(h.frames(79)).toHaveLength(0)
  }

  test('refuses the old acknowledgement and renewal callbacks without a frame or a dial while the dial is held', async () => {
    rig = new DialRig()
    await rig.holdReopeningDial()

    expectHeld()
    expect(rig.crashes).toEqual([])
  })

  test('writes no old frame after a disconnect crossed its handler boundary, and a fresh run works', async () => {
    rig = new DialRig()
    await rig.holdReopeningDial()
    const closing = track(rig.h.consumer.disconnect({ timeout: 400 }))
    await rig.clock.advance(300)
    await rig.advanceUntil(() => closing.settled, 'the close')
    expect(closing.error).toBeNull()

    await rig.releaseDial()

    expect(rig.h.frames(78)).toHaveLength(1)
    expect(rig.h.frames(79)).toHaveLength(0)
    expect(rig.h.frames(76).filter(({ fields }) => fields.memberEpoch === -1)).toHaveLength(1)
    expect(rig.acks).toEqual([['0', '0', 'accept', 'session-lost']])
    await rig.h.consumer.run(rig.handler())
    await rig.advanceUntil(() => rig.calls.length === 2, 'the fresh run delivery')
    const opens = rig.h.frames(78).filter(({ fields }) => fields.shareSessionEpoch === 0)
    expect(opens).toHaveLength(2)
    expect(new Set(rig.joins.map(({ memberId }) => memberId)).size).toBe(2)
    expect(() => rig.calls[0].acknowledge('reject')).toThrow(STALE)
  })

  test('writes no old frame after a terminal heartbeat failure ended the run, and a fresh run works', async () => {
    rig = new DialRig()
    await rig.holdReopeningDial()
    await rig.answerNextHeartbeat(30)
    await rig.advanceUntil(() => rig.crashes.length === 1, 'the crash')
    expect(rig.crashes[0].restart).toBe(false)

    await rig.releaseDial()

    expect(rig.h.frames(78)).toHaveLength(1)
    expect(rig.h.frames(79)).toHaveLength(0)
    expect(rig.acks).toEqual([['0', '0', 'accept', 'session-lost']])
    await rig.h.consumer.run(rig.handler())
    await rig.advanceUntil(() => rig.calls.length === 2, 'the fresh run delivery')
    expect(rig.h.frames(78).filter(({ fields }) => fields.shareSessionEpoch === 0)).toHaveLength(2)
    expect(new Set(rig.joins.map(({ memberId }) => memberId)).size).toBe(2)
    expect(() => rig.calls[0].acknowledge('reject')).toThrow(STALE)
  })

  test.each([
    ['unknown member', 25],
    ['fenced member epoch', 110],
  ])(
    'lets only the replacement membership use the released dial after %s (code %s)',
    async (_name, code) => {
      rig = new DialRig()
      await rig.holdReopeningDial()
      await rig.answerNextHeartbeat(code)
      await rig.advanceUntil(() => rig.joins.length === 2, 'the rejoin')
      expect(rig.h.frames(78)).toHaveLength(1)

      await rig.releaseDial()
      await rig.advanceUntil(() => rig.calls.length === 2, 'the replacement delivery')

      const opens = rig.h.frames(78).filter(({ fields }) => fields.shareSessionEpoch === 0)
      expect(opens).toHaveLength(2)
      expect(rig.acks.filter(([, , , reason]) => reason === 'session-lost')).toEqual([
        ['0', '0', 'accept', 'session-lost'],
      ])
      expect(rig.crashes).toEqual([])
      expect(() => rig.calls[0].acknowledge('reject')).toThrow(STALE)
      expect(() => rig.calls[0].renew()).toThrow(STALE)
      expect(rig.h.frames(79)).toHaveLength(0)
    }
  )
})
