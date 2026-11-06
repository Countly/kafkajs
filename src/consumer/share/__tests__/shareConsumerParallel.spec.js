const {
  A,
  acquire,
  deferred,
  echo,
  fetchAnswer,
  flush,
  part,
  shareClient,
  until,
} = require('../../../../testHelpers/shareHarness')

/** Every timer the client owns is faked; socket answers still travel on real macrotasks. */
const FAKE = { doNotFake: ['setImmediate', 'queueMicrotask', 'nextTick', 'hrtime'] }
const TWO_NODES = {
  'topic-a': {
    topicId: A,
    partitions: [
      { leader: 0, epoch: 1 },
      { leader: 1, epoch: 1 },
    ],
  },
}
const COORDINATOR_BUSY = { errorCode: 14 }

/**
 * @param {number} ms Fake milliseconds.
 * @returns {Promise<void>} Moves the fake clock and runs the client timers that fall due.
 */
const advance = (ms) => jest.advanceTimersByTimeAsync(ms)

describe('share handlers on parallel leader nodes', () => {
  let h
  /** Test-owned gates, released before the client closes so no held continuation outlives a test. */
  const holds = []
  beforeEach(async () => {
    jest.useFakeTimers(FAKE)
    await advance(1000)
  })
  afterEach(async () => {
    for (const hold of holds.splice(0)) hold.release()
    await h?.close()
    h = undefined
    jest.useRealTimers()
  })

  /**
   * @param {object} [options] `shareClient` options.
   * @returns {Promise<object>} A subscribed client whose public events are collected in `h.seen`.
   */
  const start = async (options) => {
    h = shareClient({ ...options })
    h.seen = { crashes: [], joins: [], stops: [] }
    const { events, on } = h.consumer
    on(events.CRASH, ({ payload }) => h.seen.crashes.push(payload))
    on(events.GROUP_JOIN, ({ payload }) => h.seen.joins.push(payload))
    on(events.STOP, () => h.seen.stops.push(true))
    await h.consumer.subscribe({ topics: ['topic-a'] })
    return h
  }
  /**
   * @param {object} config The `run` argument.
   * @returns {Promise<void>} Resolves once joined, with the heartbeat chain armed.
   */
  const run = async (config) => {
    await h.consumer.run(config)
    await flush()
  }
  /**
   * @param {number} count Heartbeat frames expected once the next tick's request was written.
   * @returns {Promise<void>}
   */
  const beat = async (count) => {
    await advance(50)
    await until(() => h.frames(76).length === count, `heartbeat frame ${count}`)
    await flush()
  }
  /**
   * Moves the clock `ms` in two steps, and proves the tick fires at the last millisecond and not
   * before: one heartbeat frame is written, at the end.
   * @param {number} ms Fake milliseconds to the tick.
   * @param {number} count Heartbeat frames expected once the tick's request was written.
   * @returns {Promise<void>}
   */
  const tickAt = async (ms, count) => {
    await advance(ms - 1)
    await flush()
    expect(h.frames(76)).toHaveLength(count - 1)
    await advance(1)
    await until(() => h.frames(76).length === count, `heartbeat frame ${count}`)
    await flush()
  }
  /**
   * Moves the fake clock in 50 ms steps until the condition holds.
   * @param {() => boolean} predicate The reached-stage condition.
   * @param {string} what What is awaited, named by the failure.
   * @returns {Promise<void>}
   * @throws {Error} When the condition does not hold after ten steps.
   */
  const advanceUntil = async (predicate, what) => {
    for (let step = 0; step < 10 && !predicate(); step++) {
      await advance(50)
      await flush()
    }
    if (!predicate()) throw new Error(`${what} did not happen`)
  }
  /**
   * Stops while node loops may sit in their 50 ms idle pacing, which a clean stop waits for: the
   * fake clock moves in 50 ms steps until the close ended.
   * @returns {Promise<void>}
   * @throws {Error} When the close did not end within ten steps.
   */
  const stopIdle = async () => {
    let done = false
    const stopping = h.consumer.stop().then(() => {
      done = true
    })
    for (let step = 0; step < 10; step++) {
      await flush()
      if (done) break
      await advance(50)
    }
    await flush()
    if (!done) throw new Error('the clean stop did not end')
    await stopping
  }
  /** @returns {object[]} The leave frames (ShareGroupHeartbeat at epoch -1). */
  const leaves = () => h.frames(76).filter(({ fields }) => fields.memberEpoch === -1)
  /** @returns {object[]} The join frames (ShareGroupHeartbeat at epoch 0). */
  const joins = () => h.frames(76).filter(({ fields }) => fields.memberEpoch === 0)

  /**
   * Node 0 hands partition 0 one record on its opening fetch; node 1 hands partition 1 one record
   * per fetch, `bRecords` times; every other fetch is quiet.
   * @param {number} bRecords How many records node 1 delivers.
   * @returns {void}
   */
  const serveNodes = (bRecords) => {
    let given = false
    let next = 0
    h.cluster.answer('ShareFetch', (fields, call) => {
      if (call.node === 0 && !given && fields.shareSessionEpoch === 0) {
        given = true
        return fetchAnswer([acquire(0, 0, ['a'])])
      }
      if (call.node === 1 && next < bRecords) return fetchAnswer([acquire(1, next++, ['b'])])
      return echo('ShareFetch', fields)
    })
  }
  /**
   * @returns {{gates: {a: object, b: object[]}, started: {a: number, b: number}, handler: Function}}
   *   A handler that holds partition 0's call on `gates.a` and partition 1's call of offset `n` on
   *   `gates.b[n]`, counting the calls it started.
   */
  const gatedHandler = () => {
    const gates = { a: deferred(), b: Array.from({ length: 8 }, () => deferred()) }
    const started = { a: 0, b: 0 }
    const handler = async ({ partition, message }) => {
      if (partition === 0) {
        started.a++
        await gates.a.promise
      } else {
        started.b++
        await gates.b[Number(message.offset)].promise
      }
    }
    return { gates, started, handler }
  }
  /**
   * @param {{gates: {a: object, b: object[]}}} held A `gatedHandler` result.
   * @returns {void} Lets every held handler return.
   */
  const releaseAll = ({ gates }) => {
    gates.a.resolve()
    for (const gate of gates.b) gate.resolve()
  }

  test('expires the oldest active node handler while another progresses', async () => {
    await start({ nodes: 2, topics: TWO_NODES, share: { maxPollInterval: 200 } })
    serveNodes(6)
    const held = gatedHandler()
    const { gates, started } = held
    await run({ eachMessage: held.handler })
    await until(() => started.a === 1 && started.b === 1, 'both handlers')
    for (let k = 0; k < 4; k++) {
      await beat(k + 2)
      gates.b[k].resolve()
      await until(() => started.b === k + 2, `progress at node 1 (${k + 2})`)
    }
    expect(h.frames(76)).toHaveLength(5)
    expect(leaves()).toHaveLength(0)
    expect(h.seen.crashes).toEqual([])
    gates.b[4].resolve()
    await until(() => started.b === 6, 'the last progress before the check')
    await advance(50)
    await until(() => leaves().length === 1, 'the poll-expiry leave')
    await flush()
    await advance(100)
    expect(leaves()).toHaveLength(1)
    expect(joins()).toHaveLength(1)
    expect(h.seen.crashes).toEqual([])
    releaseAll(held)
  })

  test('keeps an idle member healthy', async () => {
    await start({ share: { maxPollInterval: 100 } })
    let armed = false
    let given = false
    h.cluster.answer('ShareFetch', (fields) => {
      if (!armed || given) return echo('ShareFetch', fields)
      given = true
      return fetchAnswer([acquire(0, 0, ['a'])])
    })
    const handled = []
    await run({ eachMessage: async ({ message }) => handled.push(message.offset) })
    for (let count = 2; count <= 7; count++) await beat(count)
    await flush()
    expect(leaves()).toHaveLength(0)
    expect(joins()).toHaveLength(1)
    expect(h.seen.crashes).toEqual([])
    expect(handled).toEqual([])
    armed = true
    await advance(50)
    await until(() => handled.length === 1, 'a normal delivery after three poll intervals')
    expect(handled).toEqual(['0'])
    expect(leaves()).toHaveLength(0)
    expect(joins()).toHaveLength(1)
  })

  describe('expired handlers on both nodes', () => {
    /**
     * @returns {Promise<object>} A run whose two handlers start together and stay held, driven to
     *   the first tick that sees both of them older than the poll interval.
     */
    const expireBoth = async () => {
      await start({ nodes: 2, topics: TWO_NODES, share: { maxPollInterval: 200 } })
      serveNodes(1)
      const held = gatedHandler()
      await run({ eachMessage: held.handler })
      await until(() => held.started.a === 1 && held.started.b === 1, 'both handlers')
      for (let count = 2; count <= 5; count++) await beat(count)
      expect(leaves()).toHaveLength(0)
      await advance(50)
      await until(() => leaves().length === 1, 'the poll-expiry leave')
      await flush()
      return held
    }

    test('waits for every captured expired call', async () => {
      const held = await expireBoth()
      held.gates.a.resolve()
      await flush()
      await advance(100)
      expect(joins()).toHaveLength(1)
      held.gates.b[0].resolve()
      await flush()
      await advance(1)
      await until(() => joins().length === 2, 'the rejoin after the last expired call returned')
      expect(leaves()).toHaveLength(1)
      expect(new Set(joins().map(({ fields }) => fields.memberId)).size).toBe(1)
      expect(h.seen.crashes).toEqual([])
    })

    test('does not rejoin when a stop arrives while the last expired call is held', async () => {
      const held = await expireBoth()
      held.gates.a.resolve()
      await flush()
      await h.consumer.stop()
      held.gates.b[0].resolve()
      await flush()
      await advance(100)
      expect(joins()).toHaveLength(1)
      expect(leaves()).toHaveLength(1)
      expect(h.seen.stops).toEqual([true])
      expect(h.seen.crashes).toEqual([])
    })
  })

  test('uses one completion-rearmed heartbeat chain', async () => {
    await start()
    h.script('ShareGroupHeartbeat', [
      undefined,
      undefined,
      COORDINATOR_BUSY,
      COORDINATOR_BUSY,
      COORDINATOR_BUSY,
      undefined,
    ])
    await run({ eachMessage: async () => {} })
    const slow = h.hold({ api: 'ShareGroupHeartbeat' })
    await advance(49)
    await flush()
    expect(h.frames(76)).toHaveLength(1)
    await advance(1)
    await slow.arrived()
    expect(h.frames(76)).toHaveLength(2)
    await advance(500)
    await flush()
    expect(h.frames(76)).toHaveLength(2)
    slow.release()
    await flush()
    await tickAt(50, 3)
    await tickAt(50, 4)
    await tickAt(100, 5)
    await tickAt(100, 6)
    await tickAt(50, 7)
    expect(h.seen.crashes).toEqual([])
    expect(joins()).toHaveLength(1)
  })

  test('cannot rearm a terminal old run', async () => {
    await start()
    const { events, on } = h.consumer
    on(events.HEARTBEAT, () => {
      throw new Error('heartbeat observer')
    })
    on(events.STOP, () => {
      throw new Error('stop observer')
    })
    on(events.GROUP_JOIN, async () => {
      throw new Error('join observer')
    })
    let armed = false
    let given = false
    h.cluster.answer('ShareFetch', (fields) => {
      if (!armed || given) return echo('ShareFetch', fields)
      given = true
      return fetchAnswer([acquire(0, 0, ['a'])])
    })
    await run({ eachMessage: async () => {} })
    const old = h.hold({ api: 'ShareGroupHeartbeat' })
    await advance(50)
    await old.arrived()
    await stopIdle()
    expect(leaves()).toHaveLength(1)
    expect(h.seen.stops).toEqual([true])
    const sockets = h.cluster.sockets().length
    const fetches = h.frames(78).length
    const timers = jest.getTimerCount()
    old.release()
    await flush()
    await advance(1000)
    expect(jest.getTimerCount()).toBeLessThanOrEqual(timers)
    expect(h.cluster.sockets()).toHaveLength(sockets)
    expect(h.frames(78)).toHaveLength(fetches)
    expect(h.frames(76)).toHaveLength(3)
    const handled = []
    armed = true
    await run({ eachMessage: async ({ message }) => handled.push(message.offset) })
    await until(() => handled.length === 1, 'the replacement delivery')
    expect(joins()).toHaveLength(2)
    expect(new Set(joins().map(({ fields }) => fields.memberId)).size).toBe(2)
    await beat(5)
    expect(leaves()).toHaveLength(1)
  })

  test('cannot rearm a crashed old run when its held handler returns after the restart', async () => {
    const restartOnFailure = jest.fn(async () => true)
    await start({ nodes: 2, topics: TWO_NODES, share: { retry: { restartOnFailure } } })
    const openings = [0, 0]
    h.cluster.answer('ShareFetch', (fields, call) => {
      if (fields.shareSessionEpoch !== 0) return echo('ShareFetch', fields)
      const opened = openings[call.node]++
      if (call.node === 0) return fetchAnswer([acquire(0, 0, ['a'])])
      return opened === 0
        ? fetchAnswer([part({ partition: 1, code: 2 })])
        : echo('ShareFetch', fields)
    })
    const crashing = h.hold({ api: 'ShareFetch', node: 1 })
    const gate = deferred()
    holds.push({ release: () => gate.resolve() })
    const handled = []
    await run({
      eachMessage: async ({ message }) => {
        handled.push(message.offset)
        if (handled.length === 1) await gate.promise
      },
    })
    await until(() => handled.length === 1, 'the held handler')
    await crashing.arrived()
    crashing.release()
    await until(() => h.seen.crashes.length === 1, 'the crash event')
    expect(h.seen.crashes[0]).toMatchObject({ restart: true, groupId: 'group' })
    expect(h.seen.crashes[0].error.code).toBe(2)
    expect(handled).toEqual(['0'])
    await advanceUntil(() => h.seen.joins.length === 2 && handled.length === 2, 'the restarted run')
    await flush()
    const before = {
      beats: h.frames(76).length,
      fetches: h.frames(78).length,
      acknowledgements: h.frames(79).length,
      sockets: h.cluster.sockets().length,
      timers: jest.getTimerCount(),
    }
    gate.resolve()
    await flush()
    expect(h.frames(76)).toHaveLength(before.beats)
    expect(h.frames(78)).toHaveLength(before.fetches)
    expect(h.frames(79)).toHaveLength(before.acknowledgements)
    expect(h.cluster.sockets()).toHaveLength(before.sockets)
    expect(jest.getTimerCount()).toBeLessThanOrEqual(before.timers)
    expect(handled).toEqual(['0', '0'])
    expect(h.seen.crashes).toHaveLength(1)
    expect(h.seen.stops).toEqual([])
    const [first, second] = joins().map(({ fields }) => fields.memberId)
    expect(joins()).toHaveLength(2)
    expect(first).not.toBe(second)
    await advanceUntil(() => h.frames(76).length > before.beats, 'a beat of the restarted run')
    const later = h.frames(76).slice(before.beats)
    expect(
      later.every(({ fields }) => fields.memberId === second && fields.memberEpoch === 1)
    ).toBe(true)
    expect(leaves().every(({ fields }) => fields.memberId === first)).toBe(true)
  })
})
