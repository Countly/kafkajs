const Cluster = require('../../../cluster')
const { ShareRun } = require('../shareRun')
const {
  A,
  ShareClock,
  acquire,
  deferred,
  fetchAnswer,
  flush,
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
  /**
   * @param {object} [options] Overrides for the scripted client, including multi-node fixtures.
   */
  constructor(options = {}) {
    this.clock = new ShareClock().install()
    this.h = shareClient({
      interval: 100,
      ...options,
      kafka: { requestTimeout: 1000, enforceRequestTimeout: false, ...options.kafka },
    })
    this.gate = deferred()
    this.calls = []
    this.handlers = []
    this.acks = []
    this.partitionAcks = []
    this.crashes = []
    this.joins = []
    this.trigger = null
    const { events, on } = this.h.consumer
    on(events.CRASH, ({ payload }) => this.crashes.push(payload))
    on(events.GROUP_JOIN, ({ payload }) => this.joins.push(payload))
    on(events.ACKNOWLEDGE, ({ payload }) => {
      this.partitionAcks.push([payload.partition, payload.type, payload.error?.reason ?? null])
      this.acks.push([
        payload.firstOffset,
        payload.lastOffset,
        payload.type,
        payload.error ? payload.error.reason : null,
      ])
    })
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
      [h.frames(78)[0].generation - 1].emit('end')
    h.cluster.blackhole(0)
    this.gate.resolve()
    await this.advanceUntil(() => h.cluster.heldDialsTo(0) === 1, 'the held data dial')
  }

  /** @returns {object} A `run` argument whose handlers record their callbacks; only the first one waits. */
  handler() {
    return {
      eachMessage: (payload) => {
        const handled = this.handle(payload)
        this.handlers.push(handled)
        return handled
      },
    }
  }

  /**
   * Records one invocation and holds only the first handler until the test releases it.
   * @param {object} payload The public message payload.
   * @returns {Promise<void>} The handler continuation, awaited during cleanup.
   */
  async handle({ message, acknowledge, renew }) {
    this.calls.push({ offset: message.offset, acknowledge, renew })
    if (this.calls.length === 1) await this.gate.promise
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
    for (const node of [0, 1]) this.h.cluster.up(node).release(node)
    try {
      await this.h.close()
    } finally {
      try {
        await Promise.all(this.handlers)
      } finally {
        jest.restoreAllMocks()
        this.clock.restore()
      }
    }
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

/** Two live leaders let poll expiry remain reachable while the other node's first dial waits. */
class InitialDialRig extends DialRig {
  /** Creates an initial slot-one hold without blocking coordinator or metadata discovery. */
  constructor() {
    super({
      nodes: 2,
      topics: {
        'topic-a': {
          topicId: A,
          partitions: [
            { leader: 0, epoch: 1 },
            { leader: 1, epoch: 1 },
          ],
        },
      },
      kafka: { connectionTimeout: 10000 },
      share: { maxPollInterval: 1000 },
    })
    this.runs = []
    const start = ShareRun.prototype.start
    const { runs, h } = this
    jest.spyOn(ShareRun.prototype, 'start').mockImplementation(function () {
      runs.push(this)
      return start.call(this)
    })
    const find = Cluster.prototype.findGroupCoordinator
    let held = false
    jest.spyOn(Cluster.prototype, 'findGroupCoordinator').mockImplementation(async function (
      ...args
    ) {
      const coordinator = await find.apply(this, args)
      if (!held) {
        await this.findBroker({ nodeId: 1 })
        h.cluster.blackhole(1)
        held = true
      }
      return coordinator
    })
  }

  /**
   * Records a delivered callback and holds its return until the current gate is released.
   * @param {object} payload The public handler payload.
   * @returns {Promise<void>} The continuation owned by the parent rig's cleanup.
   */
  async handle({ partition, message, acknowledge, renew, isRunning }) {
    this.calls.push({ partition, offset: message.offset, acknowledge, renew, isRunning })
    await this.gate.promise
  }

  /** @returns {Promise<void>} Joins with node 0 handling and node 1 waiting on its first data dial. */
  async begin() {
    await this.h.consumer.subscribe({ topics: ['topic-a'] })
    this.h.script(
      'ShareFetch',
      Array.from(
        { length: 20 },
        () =>
          (fields, { node }) =>
            fields.shareSessionEpoch === 0
              ? fetchAnswer([acquire(node, 0, [`v${node}`])])
              : undefined
      )
    )
    await this.h.consumer.run(this.handler())
    await until(
      () => this.calls.length === 1 && this.h.cluster.heldDialsTo(1) === 1,
      'the sibling handler and initial data dial'
    )
    expect(this.calls.map(({ partition, offset }) => [partition, offset])).toEqual([[0, '0']])
    expect(this.h.frames(78).map(({ node }) => node)).toEqual([0])
    expect(this.h.frames(79)).toHaveLength(0)
    expect(this.acks).toEqual([])
    const initial = this.runs[0].nodes.get(1)
    expect(initial.session.binding).toBeNull()
    expect(initial.operation.error).toBeNull()
    const opening = track(initial.drive)
    await flush()
    expect(opening.settled).toBe(false)
  }

  /** @returns {Promise<void>} Releases the initial dial without moving the application clock. */
  async releaseInitialDial() {
    this.h.cluster.up(1).release(1)
    await flush()
  }
}

describe('an initial data dial beside an active sibling node', () => {
  let rig
  afterEach(async () => {
    await rig?.end()
    rig = undefined
  })

  test('delivers and confirms work when a current initial dial is released', async () => {
    rig = new InitialDialRig()
    await rig.begin()
    await rig.releaseInitialDial()
    await until(() => rig.calls.length === 2, 'the current initial delivery')
    const current = rig.calls.find(({ partition }) => partition === 1)
    expect(current.offset).toBe('0')
    expect(current.isRunning()).toBe(true)
    await expect(current.renew()).resolves.toBe(15000)
    current.acknowledge('accept')
    const stopping = rig.h.consumer.stop()
    rig.gate.resolve()
    await stopping
    expect(rig.acks).toEqual([
      ['0', '0', 'renew', null],
      ['0', '0', 'accept', null],
      ['0', '0', 'accept', null],
    ])
    expect(rig.h.frames(78).map(({ node, fields }) => [node, fields.shareSessionEpoch])).toEqual([
      [0, 0],
      [1, 0],
    ])
    expect(rig.crashes).toEqual([])
    expect(rig.partitionAcks.filter(([partition]) => partition === 1)).toEqual([
      [1, 'renew', null],
      [1, 'accept', null],
    ])
  })

  test.each([
    'stop boundary',
    'crash',
    'unknown member',
    'fenced epoch',
    'poll expiry',
    'replacement run',
  ])(
    'rejects the old initial dial across %s and confirms the current membership',
    async (transition) => {
      rig = new InitialDialRig()
      await rig.begin()
      const [oldRun] = rig.runs
      const oldNode = oldRun.nodes.get(1)
      const [oldCall] = rig.calls
      const oldMember = rig.joins[0].memberId
      const oldToken = oldRun.authorityToken
      if (transition === 'unknown member' || transition === 'fenced epoch') {
        rig.h.script('ShareGroupHeartbeat', [
          { errorCode: transition === 'unknown member' ? 25 : 110 },
        ])
        await rig.advanceUntil(() => rig.joins.length === 2, 'the replacement join')
      } else if (transition === 'poll expiry') {
        await rig.advanceUntil(
          () => oldNode.operation.error !== null,
          'the active sibling poll expiry'
        )
        expect(rig.joins).toHaveLength(1)
        rig.gate.resolve()
        await Promise.all(rig.handlers)
        rig.gate = deferred()
        await rig.advanceUntil(() => rig.joins.length === 2, 'the drained sibling rejoin')
      } else {
        if (transition === 'crash') {
          const failure = new Error('terminal initial-dial failure')
          await oldRun.crash(failure)
          expect(rig.crashes).toHaveLength(1)
          expect(rig.crashes[0]).toMatchObject({ error: failure, restart: false })
        } else {
          const stopping = track(rig.h.consumer.stop())
          const disconnecting = track(rig.h.consumer.disconnect({ timeout: 400 }))
          await rig.clock.advance(300)
          await until(() => stopping.settled && disconnecting.settled, 'the stop boundary')
          expect([stopping.error, disconnecting.error]).toEqual([null, null])
        }
        if (transition !== 'replacement run') {
          await rig.releaseInitialDial()
          await oldNode.drive
          expect(rig.h.frames(78).map(({ node }) => node)).toEqual([0])
        }
        await rig.h.consumer.run(rig.handler())
      }
      expect(() => oldCall.acknowledge()).toThrow(STALE)
      expect(() => oldCall.renew()).toThrow(STALE)
      expect(rig.acks).toEqual([['0', '0', 'release', 'membership-lost']])
      await rig.releaseInitialDial()
      await oldNode.drive
      await rig.advanceUntil(
        () => rig.calls.some(({ partition }) => partition === 1),
        'the current node-one delivery'
      )
      const current = rig.calls.find(({ partition }) => partition === 1)
      const run = rig.runs.at(-1)
      expect(run.nodes.get(1)).not.toBe(oldNode)
      expect(run.authorityToken).not.toBe(oldToken)
      expect(current.isRunning()).toBe(true)
      const nodeOne = rig.h.frames(78).filter(({ node }) => node === 1)
      expect(nodeOne.map(({ fields }) => fields.shareSessionEpoch)).toEqual([0])
      if (['stop boundary', 'crash', 'replacement run'].includes(transition))
        expect(nodeOne[0].fields.memberId).not.toBe(oldMember)
      await expect(current.renew()).resolves.toBe(15000)
      current.acknowledge('accept')
      const stopping = rig.h.consumer.stop()
      rig.gate.resolve()
      await stopping
      expect(rig.acks.filter(([, , type]) => type === 'renew')).toEqual([['0', '0', 'renew', null]])
      expect(rig.acks.filter(([, , , reason]) => reason !== null)).toEqual([
        ['0', '0', 'release', 'membership-lost'],
      ])
      expect(rig.partitionAcks.filter(([partition]) => partition === 1)).toEqual([
        [1, 'renew', null],
        [1, 'accept', null],
      ])
      expect(() => oldCall.renew()).toThrow(STALE)
    }
  )
})
