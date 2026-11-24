const HeartbeatManager = require('../../heartbeatManager')
const { ShareRunner } = require('../shareRunner')
const { ShareFetchLoop } = require('../shareFetchLoop')
const {
  A,
  ackAnswer,
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
  let manager
  /** Test-owned gates, released before the client closes so no held continuation outlives a test. */
  const holds = []
  beforeEach(async () => {
    jest.useFakeTimers(FAKE)
    const schedule = HeartbeatManager.prototype.schedule
    jest.spyOn(HeartbeatManager.prototype, 'schedule').mockImplementation(function () {
      schedule.call(this)
      manager = this
    })
    await advance(1000)
  })
  afterEach(async () => {
    const released = holds.splice(0)
    for (const hold of released) hold.release()
    try {
      await h?.close()
    } finally {
      await Promise.all(released.flatMap((hold) => hold.completions ?? []))
      h = undefined
      manager = undefined
      jest.useRealTimers()
      jest.restoreAllMocks()
    }
  })

  /**
   * @param {object} [options] `shareClient` options.
   * @returns {Promise<object>} A subscribed client whose public events are collected in `h.seen`.
   */
  const start = async (options) => {
    h = shareClient({ ...options })
    h.seen = { acks: [], crashes: [], joins: [], stops: [] }
    const { events, on } = h.consumer
    on(events.ACKNOWLEDGE, ({ payload }) => h.seen.acks.push(payload))
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
    const previous = manager
    await h.consumer.run(config)
    await until(() => manager && manager !== previous, 'the new heartbeat chain')
    await heartbeatReady()
  }
  /**
   * Waits at a fixed clock until the current heartbeat answer has rearmed its timer.
   * @returns {Promise<void>}
   */
  const heartbeatReady = () =>
    until(
      () => manager && manager.handle !== null && !manager.inFlight && !manager.stopped,
      'the armed heartbeat timer'
    )
  /**
   * @param {number} count Heartbeat frames expected once the next tick's request was written.
   * @returns {Promise<void>}
   */
  const beat = async (count) => {
    await heartbeatReady()
    await advance(50)
    await until(() => h.frames(76).length === count, `heartbeat frame ${count}`)
    await heartbeatReady()
  }
  /**
   * Moves the clock `ms` in two steps, and proves the tick fires at the last millisecond and not
   * before: one heartbeat frame is written, at the end.
   * @param {number} ms Fake milliseconds to the tick.
   * @param {number} count Heartbeat frames expected once the tick's request was written.
   * @returns {Promise<void>}
   */
  const tickAt = async (ms, count) => {
    await heartbeatReady()
    await advance(ms - 1)
    await flush()
    expect(h.frames(76)).toHaveLength(count - 1)
    await advance(1)
    await until(() => h.frames(76).length === count, `heartbeat frame ${count}`)
    await heartbeatReady()
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
  /** @returns {object[]} The final acknowledgements (ShareAcknowledge at session epoch -1). */
  const finalFrames = () => h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1)
  /**
   * @param {number} partition Partition index.
   * @returns {Array<Array>} Range, type and error reason of each published acknowledgement of it.
   */
  const outcomesOf = (partition) =>
    h.seen.acks
      .filter((ack) => ack.partition === partition)
      .map(({ firstOffset, lastOffset, type, error }) => [
        firstOffset,
        lastOffset,
        type,
        error ? error.reason : null,
      ])

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
    const completions = []
    const handler = async ({ partition, message }) => {
      const completed = deferred()
      completions.push(completed.promise)
      try {
        if (partition === 0) {
          started.a++
          await gates.a.promise
        } else {
          started.b++
          await gates.b[Number(message.offset)].promise
        }
      } finally {
        completed.resolve()
      }
    }
    const held = { gates, started, handler }
    holds.push({ release: () => releaseAll(held), completions })
    return held
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
     * @param {() => void} [observe] Runs once the client exists, before it runs.
     * @returns {Promise<object>} A run whose two handlers start together and stay held, driven to
     *   the first tick that sees both of them older than the poll interval.
     */
    const expireBoth = async (observe = () => {}) => {
      await start({ nodes: 2, topics: TWO_NODES, share: { maxPollInterval: 200 } })
      observe()
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

    test('a node whose final capture fails does not keep the other node from its final or the leave', async () => {
      const capture = jest
        .spyOn(ShareFetchLoop.prototype, 'captureFinal')
        .mockImplementationOnce(() => {
          throw new Error('capture failed')
        })
      let logged
      await expireBoth(() => {
        logged = jest.spyOn(h.consumer.logger(), 'error')
      })
      expect(capture).toHaveBeenCalledTimes(2)
      const finals = finalFrames()
      expect(finals.map(({ node }) => node)).toEqual([1])
      expect(finals[0].order).toBeLessThan(leaves()[0].order)
      await until(() => outcomesOf(0).length === 1, 'the outcome of node 0')
      expect(outcomesOf(0)).toEqual([['0', '0', 'release', 'membership-lost']])
      const failures = logged.mock.calls.filter(
        ([message]) => message === 'Share final capture failed'
      )
      expect(failures).toEqual([
        ['Share final capture failed', { nodeId: 0, error: 'capture failed' }],
      ])
      expect(h.seen.crashes).toEqual([])
    })

    test('a zero-timeout disconnect requested from the capture failure log neither re-enters the close nor leaves the other node choice without an outcome', async () => {
      const capture = jest
        .spyOn(ShareFetchLoop.prototype, 'captureFinal')
        .mockImplementationOnce(() => {
          throw new Error('capture failed')
        })
      await start({ nodes: 2, topics: TWO_NODES, share: { maxPollInterval: 200 } })
      const gate = deferred()
      const completions = []
      holds.push({ release: () => gate.resolve(), completions })
      let requested = 0
      jest.spyOn(h.consumer.logger(), 'error').mockImplementation((message) => {
        if (message !== 'Share final capture failed' || requested++ > 0) return
        completions.push(h.consumer.disconnect({ timeout: 0 }).catch(() => {}))
      })
      serveNodes(1)
      const started = []
      await run({
        eachMessage: async ({ partition, acknowledge }) => {
          const completed = deferred()
          completions.push(completed.promise)
          try {
            started.push(partition)
            if (partition === 1) acknowledge('accept')
            await gate.promise
          } finally {
            completed.resolve()
          }
        },
      })
      await until(() => started.length === 2, 'both handlers')
      for (let count = 2; count <= 5; count++) await beat(count)
      expect(leaves()).toHaveLength(0)
      await advance(50)
      await until(() => outcomesOf(1).length === 1, 'the outcome of the choice captured on node 1')
      await flush()
      expect(requested).toBe(1)
      expect(capture).toHaveBeenCalledTimes(2)
      expect(new Set(capture.mock.contexts).size).toBe(2)
      expect(outcomesOf(1)).toEqual([['0', '0', 'accept', 'not-sent']])
      expect(outcomesOf(0)).toEqual([['0', '0', 'release', 'membership-lost']])
      expect(finalFrames()).toEqual([])
      expect(leaves()).toEqual([])
      expect(h.seen.crashes).toEqual([])
    })

    test('a zero-timeout disconnect from the outcome of a failed final frame build captures the other node once', async () => {
      const buildAcknowledge = ShareFetchLoop.prototype.buildAcknowledge
      let failed = false
      jest
        .spyOn(ShareFetchLoop.prototype, 'buildAcknowledge')
        .mockImplementation(function (prepared, final) {
          if (final && !failed) {
            failed = true
            throw new Error('frame build failed')
          }
          return buildAcknowledge.call(this, prepared, final)
        })
      const capture = jest.spyOn(ShareFetchLoop.prototype, 'captureFinal')
      await start({ nodes: 2, topics: TWO_NODES, share: { maxPollInterval: 200 } })
      const gate = deferred()
      const completions = []
      holds.push({ release: () => gate.resolve(), completions })
      let requested = 0
      h.consumer.on(h.consumer.events.ACKNOWLEDGE, () => {
        if (requested++ > 0) return
        completions.push(h.consumer.disconnect({ timeout: 0 }).catch(() => {}))
      })
      serveNodes(1)
      const started = []
      await run({
        eachMessage: async ({ partition, acknowledge }) => {
          const completed = deferred()
          completions.push(completed.promise)
          try {
            started.push(partition)
            acknowledge(partition === 0 ? 'reject' : 'accept')
            await gate.promise
          } finally {
            completed.resolve()
          }
        },
      })
      await until(() => started.length === 2, 'both handlers')
      for (let count = 2; count <= 5; count++) await beat(count)
      expect(leaves()).toHaveLength(0)
      await advance(50)
      await until(() => outcomesOf(1).length === 1, 'the outcome of the choice captured on node 1')
      await flush()
      expect(failed).toBe(true)
      expect(requested).toBeGreaterThan(0)
      expect(capture).toHaveBeenCalledTimes(2)
      expect(new Set(capture.mock.contexts).size).toBe(2)
      expect(outcomesOf(0)).toEqual([['0', '0', 'reject', 'not-sent']])
      expect(outcomesOf(1)).toEqual([['0', '0', 'accept', 'not-sent']])
      expect(finalFrames()).toEqual([])
      expect(leaves()).toEqual([])
      expect(h.seen.crashes).toEqual([])
    })

    test('a capture error whose message cannot be read still lets the captured node final before the leave', async () => {
      const unreadable = new Error('capture failed')
      Object.defineProperty(unreadable, 'message', {
        get() {
          throw new Error('message read')
        },
      })
      const captureFinal = ShareFetchLoop.prototype.captureFinal
      let captures = 0
      const capture = jest
        .spyOn(ShareFetchLoop.prototype, 'captureFinal')
        .mockImplementation(function () {
          if (++captures === 2) throw unreadable
          return captureFinal.call(this)
        })
      await start({ nodes: 2, topics: TWO_NODES, share: { maxPollInterval: 200 } })
      const logged = jest.spyOn(h.consumer.logger(), 'error')
      let listened = 0
      h.consumer.on(h.consumer.events.ACKNOWLEDGE, () => {
        if (listened++ === 0) throw unreadable
      })
      const gate = deferred()
      const completions = []
      holds.push({ release: () => gate.resolve(), completions })
      serveNodes(1)
      const started = []
      await run({
        eachMessage: async ({ partition, acknowledge }) => {
          const completed = deferred()
          completions.push(completed.promise)
          try {
            started.push(partition)
            if (partition === 0) acknowledge('accept')
            await gate.promise
          } finally {
            completed.resolve()
          }
        },
      })
      await until(() => started.length === 2, 'both handlers')
      for (let count = 2; count <= 5; count++) await beat(count)
      expect(leaves()).toHaveLength(0)
      await advance(50)
      await until(() => leaves().length === 1, 'the poll-expiry leave')
      await until(() => outcomesOf(0).length === 1, 'the outcome of the choice captured on node 0')
      await flush()
      expect(capture).toHaveBeenCalledTimes(2)
      expect(new Set(capture.mock.contexts).size).toBe(2)
      const finals = finalFrames()
      expect(finals.map(({ node }) => node)).toEqual([0])
      expect(finals[0].fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
        { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] },
      ])
      expect(finals[0].order).toBeLessThan(leaves()[0].order)
      expect(outcomesOf(0)).toEqual([['0', '0', 'accept', null]])
      expect(outcomesOf(1)).toEqual([['0', '0', 'release', 'membership-lost']])
      const messages = logged.mock.calls.map(([message]) => message)
      const closeFailures = new Set([
        'Share final capture failed',
        'Share node disposal failed',
        'Share node observer failed',
        'Share close failed',
      ])
      expect(logged.mock.calls.filter(([message]) => closeFailures.has(message))).toEqual([
        ['Share final capture failed', { nodeId: 1, error: '<unreadable thrown value>' }],
      ])
      expect(
        messages.filter((message) => message.startsWith('Failed to execute listener'))
      ).toEqual(['Failed to execute listener: <unreadable thrown value>'])
      gate.resolve()
      await flush()
      await advance(1)
      await until(() => joins().length === 2, 'the rejoin after the close ended')
      expect(h.seen.crashes).toEqual([])
    })
  })

  test('a poll expiry over a handler that changed its message offset after acknowledging finals both nodes and leaves', async () => {
    await start({ nodes: 2, topics: TWO_NODES, share: { maxPollInterval: 200 } })
    serveNodes(1)
    const gate = deferred()
    const completions = []
    holds.push({ release: () => gate.resolve(), completions })
    const started = []
    await run({
      eachMessage: async ({ partition, message, acknowledge }) => {
        const completed = deferred()
        completions.push(completed.promise)
        try {
          started.push(partition)
          acknowledge(partition === 0 ? 'reject' : 'accept')
          if (partition === 0) message.offset = '7'
          await gate.promise
        } finally {
          completed.resolve()
        }
      },
    })
    await until(() => started.length === 2, 'both handlers')
    for (let count = 2; count <= 5; count++) await beat(count)
    expect(leaves()).toHaveLength(0)
    await advance(50)
    await until(() => leaves().length === 1, 'the poll-expiry leave')
    await flush()
    const finals = h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1)
    expect(finals.map(({ node }) => node).sort()).toEqual([0, 1])
    const topicsOf = (node) => finals.find((final) => final.node === node).fields.topics
    expect(topicsOf(0)).toEqual([
      {
        topicId: A,
        partitions: [
          {
            partitionIndex: 0,
            acknowledgementBatches: [{ firstOffset: '0', lastOffset: '0', acknowledgeTypes: [3] }],
          },
        ],
      },
    ])
    expect(topicsOf(1)).toEqual([
      {
        topicId: A,
        partitions: [
          {
            partitionIndex: 1,
            acknowledgementBatches: [{ firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] }],
          },
        ],
      },
    ])
    for (const final of finals) expect(final.order).toBeLessThan(leaves()[0].order)
    expect(h.seen.crashes).toEqual([])
  })

  test('an eachMessage handler that changed its message offset to the next record acknowledges its own record, and the next record is released', async () => {
    await start({ share: { maxPollInterval: 200 } })
    let given = false
    h.cluster.answer('ShareFetch', (fields) => {
      if (given || fields.shareSessionEpoch !== 0) return echo('ShareFetch', fields)
      given = true
      return fetchAnswer([acquire(0, 0, ['a', 'b'])])
    })
    const gate = deferred()
    const completions = []
    holds.push({ release: () => gate.resolve(), completions })
    const started = []
    const errors = []
    await run({
      eachMessage: async ({ message, acknowledge }) => {
        const completed = deferred()
        completions.push(completed.promise)
        try {
          started.push(message.offset)
          acknowledge('reject')
          message.offset = '1'
          acknowledge('accept')
        } catch (error) {
          errors.push(error)
        }
        try {
          await gate.promise
        } finally {
          completed.resolve()
        }
      },
    })
    await until(() => started.length === 1, 'the first handler')
    for (let count = 2; count <= 5; count++) await beat(count)
    expect(leaves()).toHaveLength(0)
    await advance(50)
    await until(() => leaves().length === 1, 'the poll-expiry leave')
    await flush()
    expect(started).toEqual(['0'])
    expect(errors).toEqual([])
    const finals = finalFrames()
    expect(finals).toHaveLength(1)
    expect(finals[0].fields.topics).toEqual([
      {
        topicId: A,
        partitions: [
          {
            partitionIndex: 0,
            acknowledgementBatches: [
              { firstOffset: '0', lastOffset: '1', acknowledgeTypes: [1, 2] },
            ],
          },
        ],
      },
    ])
    expect(finals[0].order).toBeLessThan(leaves()[0].order)
    expect(h.seen.crashes).toEqual([])
  })

  test('a poll expiry over a handler whose renewal lost its session still finals the other node and leaves', async () => {
    await start({ nodes: 2, topics: TWO_NODES, share: { maxPollInterval: 200 } })
    const opened = [false, false]
    h.cluster.answer('ShareFetch', (fields, call) => {
      if (fields.shareSessionEpoch !== 0 || opened[call.node]) return echo('ShareFetch', fields)
      opened[call.node] = true
      return fetchAnswer([call.node === 0 ? acquire(0, 0, ['a']) : acquire(1, 0, ['b', 'c'])])
    })
    h.cluster.answer('ShareAcknowledge', (fields, call) =>
      call.node === 0 && fields.isRenewAck
        ? ackAnswer([], { errorCode: 122 })
        : echo('ShareAcknowledge', fields)
    )
    const gate = deferred()
    const completions = []
    holds.push({ release: () => gate.resolve(), completions })
    const started = []
    const renewals = []
    await run({
      eachMessage: async ({ partition, message, acknowledge, renew }) => {
        const completed = deferred()
        completions.push(completed.promise)
        try {
          started.push(`${partition}:${message.offset}`)
          acknowledge('accept')
          if (partition === 0) await renew().catch((error) => renewals.push(error))
          await gate.promise
        } finally {
          completed.resolve()
        }
      },
    })
    await until(
      () => started.length === 2 && renewals.length === 1,
      'both handlers and the renewal'
    )
    expect(renewals[0].reason).toBe('session-lost')
    for (let count = 2; count <= 5; count++) await beat(count)
    expect(leaves()).toHaveLength(0)
    await advance(50)
    await until(() => leaves().length === 1, 'the poll-expiry leave')
    await flush()
    const finals = h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1)
    expect(finals.map(({ node }) => node)).toEqual([1])
    expect(finals[0].fields.topics).toEqual([
      {
        topicId: A,
        partitions: [
          {
            partitionIndex: 1,
            acknowledgementBatches: [
              { firstOffset: '0', lastOffset: '1', acknowledgeTypes: [1, 2] },
            ],
          },
        ],
      },
    ])
    expect(finals[0].order).toBeLessThan(leaves()[0].order)
    const fromNode0 = h.frames(79).filter(({ node }) => node === 0)
    expect(fromNode0.map(({ fields }) => fields.isRenewAck)).toEqual([true])
    expect(started.sort()).toEqual(['0:0', '1:0'])
    expect(h.seen.crashes).toEqual([])
  })

  describe('eachBatch arrays at a forced close', () => {
    test('reversing batch.messages after rejecting one record keeps only that reject in the forced-close final', async () => {
      await start({ share: { maxPollInterval: 200 } })
      let given = false
      h.cluster.answer('ShareFetch', (fields) => {
        if (given || fields.shareSessionEpoch !== 0) return echo('ShareFetch', fields)
        given = true
        return fetchAnswer([acquire(0, 0, ['a', 'b'])])
      })
      const gate = deferred()
      const completions = []
      holds.push({ release: () => gate.resolve(), completions })
      const started = []
      await run({
        eachBatch: async ({ batch, acknowledge }) => {
          const completed = deferred()
          completions.push(completed.promise)
          try {
            started.push(batch.messages.map(({ offset }) => offset))
            acknowledge(batch.messages[0], 'reject')
            batch.messages.reverse()
            await gate.promise
          } finally {
            completed.resolve()
          }
        },
      })
      await until(() => started.length === 1, 'the batch handler')
      expect(started).toEqual([['0', '1']])
      for (let count = 2; count <= 5; count++) await beat(count)
      expect(leaves()).toHaveLength(0)
      await advance(50)
      await until(() => leaves().length === 1, 'the poll-expiry leave')
      await flush()
      const finals = finalFrames()
      expect(finals).toHaveLength(1)
      expect(finals[0].fields.topics).toEqual([
        {
          topicId: A,
          partitions: [
            {
              partitionIndex: 0,
              acknowledgementBatches: [
                { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [3] },
              ],
            },
          ],
        },
      ])
      expect(finals[0].order).toBeLessThan(leaves()[0].order)
      const lost = () => outcomesOf(0).filter(([, , , reason]) => reason === 'membership-lost')
      await until(() => lost().length === 1, 'the lost outcome')
      expect(lost()).toEqual([['1', '1', 'release', 'membership-lost']])
      expect(h.seen.crashes).toEqual([])
    })

    /**
     * Delivers `values` as one acquisition from offset 0 and runs `eachBatch` held after `decide`,
     * then drives the first poll-expiry close.
     * @param {string[]} values Record values.
     * @param {(payload: object) => void} decide Runs in the handler before it is held.
     * @returns {Promise<Error[]>} What `decide` threw.
     */
    const holdBatchThroughExpiry = async (values, decide) => {
      let given = false
      h.cluster.answer('ShareFetch', (fields) => {
        if (given || fields.shareSessionEpoch !== 0) return echo('ShareFetch', fields)
        given = true
        return fetchAnswer([acquire(0, 0, values)])
      })
      const gate = deferred()
      const completions = []
      holds.push({ release: () => gate.resolve(), completions })
      let started = 0
      const errors = []
      await run({
        eachBatch: async (payload) => {
          const completed = deferred()
          completions.push(completed.promise)
          started++
          try {
            decide(payload)
          } catch (error) {
            errors.push(error)
          }
          try {
            await gate.promise
          } finally {
            completed.resolve()
          }
        },
      })
      await until(() => started === 1, 'the batch handler')
      for (let count = 2; count <= 5; count++) await beat(count)
      expect(leaves()).toHaveLength(0)
      await advance(50)
      await until(() => leaves().length === 1, 'the poll-expiry leave')
      await flush()
      return errors
    }

    test('an offset-only selector after another message took its offset decides the record acquired at that offset', async () => {
      await start({ share: { maxPollInterval: 200 } })
      const errors = await holdBatchThroughExpiry(['a', 'b'], ({ batch, acknowledge }) => {
        const [first] = batch.messages
        acknowledge(first, 'reject')
        first.offset = '1'
        acknowledge({ offset: '1' }, 'accept')
      })
      expect(errors).toEqual([])
      const finals = finalFrames()
      expect(finals).toHaveLength(1)
      expect(finals[0].fields.topics).toEqual([
        {
          topicId: A,
          partitions: [
            {
              partitionIndex: 0,
              acknowledgementBatches: [
                { firstOffset: '0', lastOffset: '1', acknowledgeTypes: [3, 1] },
              ],
            },
          ],
        },
      ])
      expect(finals[0].order).toBeLessThan(leaves()[0].order)
      expect(outcomesOf(0).filter(([, , , reason]) => reason !== null)).toEqual([])
      expect(h.seen.crashes).toEqual([])
    })

    test('records lost at the close are counted by their acquired offsets after the handler changed one', async () => {
      await start({ share: { maxPollInterval: 200 } })
      const resets = []
      h.consumer.on(h.consumer.events.SESSION_RESET, ({ payload }) => resets.push(payload))
      const errors = await holdBatchThroughExpiry(['a', 'b', 'c'], ({ batch, acknowledge }) => {
        acknowledge(batch.messages[0], 'reject')
        batch.messages[1].offset = '7'
      })
      expect(errors).toEqual([])
      const lost = () => outcomesOf(0).filter(([, , , reason]) => reason === 'membership-lost')
      await until(() => lost().length === 1, 'the lost outcome')
      expect(lost()).toEqual([['1', '2', 'release', 'membership-lost']])
      expect(resets.filter(({ reason }) => reason === 'membership')).toEqual([
        { nodeId: 0, reason: 'membership', recordsDropped: 2 },
      ])
      expect(finalFrames()[0].fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
        { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [3] },
      ])
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
    const supervision = jest.spyOn(ShareRunner.prototype, 'superviseCrash')
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
    const completed = deferred()
    const completions = []
    holds.push({ release: () => gate.resolve(), completions })
    const handled = []
    await run({
      eachMessage: async ({ message }) => {
        handled.push(message.offset)
        if (handled.length === 1) {
          completions.push(completed.promise)
          try {
            await gate.promise
          } finally {
            completed.resolve()
          }
        }
      },
    })
    await until(() => handled.length === 1, 'the held handler')
    await crashing.arrived()
    crashing.release()
    await until(() => h.seen.crashes.length === 1, 'the crash event')
    expect(h.seen.crashes[0]).toMatchObject({ restart: true, groupId: 'group' })
    expect(h.seen.crashes[0].error.code).toBe(2)
    expect(handled).toEqual(['0'])
    await until(
      () => supervision.mock.contexts[0]?.restartOperation != null,
      'the armed restart delay'
    )
    await advance(50)
    await until(() => h.seen.joins.length === 2 && handled.length === 2, 'the restarted run')
    await heartbeatReady()
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
    await beat(before.beats + 1)
    const later = h.frames(76).slice(before.beats)
    expect(
      later.every(({ fields }) => fields.memberId === second && fields.memberEpoch === 1)
    ).toBe(true)
    expect(leaves().every(({ fields }) => fields.memberId === first)).toBe(true)
  })
})
