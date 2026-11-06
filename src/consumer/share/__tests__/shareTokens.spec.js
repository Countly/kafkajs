const {
  A,
  acquire,
  acquired,
  ackAnswer,
  deferred,
  echo,
  fetchAnswer,
  flush,
  part,
  recordsHex,
  shareClient,
  topError,
  until,
} = require('../../../../testHelpers/shareHarness')
const Cluster = require('../../../cluster')

/** Every timer the client owns is faked; socket answers still travel on real macrotasks. */
const FAKE = { doNotFake: ['setImmediate', 'queueMicrotask', 'nextTick', 'hrtime'] }
const U1 = '20112233-4455-6677-8899-aabbccddeeff'
const TWO_NODES = {
  'topic-a': {
    topicId: A,
    partitions: [
      { leader: 0, epoch: 1 },
      { leader: 1, epoch: 1 },
    ],
  },
}
const FENCE = { errorCode: 110 }

/**
 * @param {number} ms Fake milliseconds.
 * @returns {Promise<void>} Moves the fake clock and runs the client timers that fall due.
 */
const advance = (ms) => jest.advanceTimersByTimeAsync(ms)

/**
 * @param {object} frame A recorded ShareFetch or ShareAcknowledge frame.
 * @returns {object[]} The acknowledgement batches it carried, in wire order.
 */
const batchesOf = ({ fields }) =>
  fields.topics.flatMap((t) => t.partitions.flatMap((p) => p.acknowledgementBatches))

/**
 * @param {object} error A settlement error or null.
 * @returns {string|null} Its failure reason, or null for a confirmed outcome.
 */
const reasonOf = (error) => error?.reason ?? null

describe('public share token contract', () => {
  let h
  beforeEach(async () => {
    jest.useFakeTimers(FAKE)
    await advance(1000)
  })
  afterEach(async () => {
    await h?.close()
    h = undefined
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  /**
   * @param {object} [options] `shareClient` options.
   * @returns {Promise<object>} A subscribed client whose public events are collected in `h.seen`.
   */
  const start = async (options) => {
    h = shareClient({ ...options })
    h.seen = { acks: [], joins: [], beats: [], crashes: [], resets: [], stops: [], assigned: [] }
    const { events, on } = h.consumer
    on(events.ACKNOWLEDGE, ({ payload }) => h.seen.acks.push(payload))
    on(events.GROUP_JOIN, ({ payload }) => h.seen.joins.push(payload))
    on(events.HEARTBEAT, ({ payload }) => h.seen.beats.push(payload))
    on(events.CRASH, ({ payload }) => h.seen.crashes.push(payload))
    on(events.SESSION_RESET, ({ payload }) => h.seen.resets.push(payload))
    on(events.STOP, () => h.seen.stops.push(true))
    on(events.ASSIGNMENT_CHANGED, ({ payload }) => h.seen.assigned.push(payload))
    await h.consumer.subscribe({ topics: ['topic-a'] })
    return h
  }
  /**
   * Starts the run and lets the heartbeat chain arm, so the first fake tick is the next beat.
   * @param {object} config The `run` argument.
   * @returns {Promise<void>}
   */
  const run = async (config) => {
    await h.consumer.run(config)
    await flush()
  }
  /**
   * Answers every ShareFetch quietly except opening ones, which acquire `openings` in order. It
   * replaces the client's queue so the answer depends on the request, never on arrival order.
   * @param {object[]} openings One ShareFetch answer per opening request.
   * @returns {void}
   */
  const openWith = (openings) => {
    let opened = 0
    h.cluster.answer('ShareFetch', (fields) =>
      fields.shareSessionEpoch === 0 && opened < openings.length
        ? openings[opened++]
        : echo('ShareFetch', fields)
    )
  }
  /**
   * @param {number} ms Fake milliseconds to the tick.
   * @param {number} count Heartbeat frames expected once the tick's request was written.
   * @returns {Promise<void>}
   */
  const beat = async (ms, count) => {
    await advance(ms)
    await until(() => h.frames(76).length === count, `heartbeat frame ${count}`)
    await flush()
  }
  /** @returns {object[]} The leave frames (ShareGroupHeartbeat at epoch -1). */
  const leaves = () => h.frames(76).filter(({ fields }) => fields.memberEpoch === -1)
  /** @returns {object[]} The join frames (ShareGroupHeartbeat at epoch 0). */
  const joins = () => h.frames(76).filter(({ fields }) => fields.memberEpoch === 0)
  /** @returns {object[]} The final frames (ShareAcknowledge at epoch -1). */
  const finals = () => h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1)
  /** @returns {Array<Array<string|null>>} Every public acknowledgement as [type, first, last, reason]. */
  const outcomes = () =>
    h.seen.acks.map(({ type, firstOffset, lastOffset, error }) => [
      type,
      firstOffset,
      lastOffset,
      reasonOf(error),
    ])
  /**
   * @param {number} [count=1] Delivery count of the acquired record.
   * @returns {object} A ShareFetch answer acquiring offset 0 of partition 0 with one record.
   */
  const oneRecord = (count = 1) =>
    fetchAnswer([
      part({ partition: 0, acquire: [acquired(0, 0, count)], records: recordsHex(0, ['a']) }),
    ])
  /** @returns {object} The held ShareFetch carrying acknowledgements of an established session. */
  const holdAcknowledgingFetch = () =>
    h.hold({ api: 'ShareFetch', match: ({ fields }) => fields.shareSessionEpoch === 1 })

  test('an old callback throws while a replacement acquisition of the same offset works', async () => {
    await start()
    // The clock never moves, so both acquisitions carry the same send time, offset, count and epoch.
    h.script('ShareFetch', [oneRecord(), topError(122), oneRecord()])
    const calls = []
    await run({
      eachMessage: async ({ message, acknowledge, renew, isRunning }) => {
        const call = { acknowledge, renew, isRunning, count: message.deliveryCount, thrown: [] }
        calls.push(call)
        if (calls.length < 2) return
        for (const old of [() => calls[0].acknowledge('reject'), () => calls[0].renew()]) {
          try {
            old()
            call.thrown.push(null)
          } catch (error) {
            call.thrown.push(error)
          }
        }
        call.oldRunning = calls[0].isRunning()
        acknowledge('reject')
      },
    })
    await until(() => h.frames(78).length >= 4 && h.seen.acks.length === 2, 'both acquisitions')
    expect(calls.map(({ count }) => count)).toEqual([1, 1])
    expect(calls[1].thrown.map((error) => [error.name, error.message])).toEqual([
      ['KafkaJSNonRetriableError', 'Handler call is no longer current'],
      ['KafkaJSNonRetriableError', 'Handler call is no longer current'],
    ])
    expect(calls[1].oldRunning).toBe(true)
    expect(h.frames(78).map(({ fields }) => fields.shareSessionEpoch)).toEqual([0, 1, 0, 1])
    expect(h.frames(78).map(batchesOf)).toEqual([
      [],
      [{ firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] }],
      [],
      [{ firstOffset: '0', lastOffset: '0', acknowledgeTypes: [3] }],
    ])
    expect(outcomes()).toEqual([
      ['accept', '0', '0', 'session-lost'],
      ['reject', '0', '0', null],
    ])
    expect(h.frames(79)).toHaveLength(0)
  })

  test('keeps isRunning true for a handler that outlives a data session reset', async () => {
    await start()
    h.script('ShareFetch', [oneRecord(), oneRecord()])
    h.script('ShareAcknowledge', [ackAnswer([], { errorCode: 122 })])
    const calls = []
    const during = {}
    await run({
      eachMessage: async ({ acknowledge, renew, isRunning }) => {
        calls.push(true)
        if (calls.length > 1) return
        during.renewal = await renew().then(
          () => null,
          (error) => error
        )
        during.running = isRunning()
        try {
          acknowledge('reject')
        } catch (error) {
          during.late = error.message
        }
      },
    })
    await until(() => calls.length === 2 || h.seen.crashes.length > 0, 'the replacement delivery')
    expect(h.seen.crashes).toEqual([])
    expect(calls).toHaveLength(2)
    expect(during.renewal).toMatchObject({
      name: 'KafkaJSShareAcknowledgementError',
      reason: 'session-lost',
      ranges: [{ firstOffset: '0', lastOffset: '0' }],
    })
    expect(during.running).toBe(true)
    expect(during.late).toBe('Handler call is no longer current')
    expect(h.frames(78).map(({ fields }) => fields.shareSessionEpoch)).toEqual([0, 0])
  })

  test('sends no heartbeat from a coordinator lookup held across a stop', async () => {
    await start()
    const lookup = h.hold({ api: 'FindCoordinator' })
    const first = h.consumer.run({ eachMessage: async () => {} })
    await lookup.arrived()
    expect(h.frames(76)).toHaveLength(0)
    await h.consumer.stop()
    await first
    lookup.release()
    await flush()
    expect(h.frames(76)).toHaveLength(0)
    expect(h.seen.joins).toEqual([])
    expect(h.consumer.assignment()).toEqual([])
    await run({ eachMessage: async () => {} })
    expect(joins()).toHaveLength(1)
    expect(h.seen.joins).toHaveLength(1)
  })

  test('keeps a running handler out of the next membership after a fence', async () => {
    await start()
    openWith([oneRecord(), oneRecord()])
    h.script('ShareGroupHeartbeat', [undefined, { errorCode: 25 }])
    const gate = deferred()
    const returned = deferred()
    const saved = []
    const handled = []
    await run({
      eachMessage: async ({ acknowledge }) => {
        handled.push(true)
        if (handled.length > 1) return
        saved.push(acknowledge)
        await gate.promise
        returned.resolve()
      },
    })
    await until(() => saved.length === 1, 'the running handler')
    await beat(50, 2)
    await until(() => handled.length === 1 && h.seen.assigned.length === 2, 'the fence')
    await flush()
    await advance(50)
    await until(() => handled.length === 2 && h.seen.joins.length === 2, 'the replacement delivery')
    expect(() => saved[0]('reject')).toThrow('Handler call is no longer current')
    gate.resolve()
    await returned.promise
    await until(() => h.seen.acks.length === 2, 'both settlements')
    expect(outcomes()).toEqual([
      ['release', '0', '0', 'membership-lost'],
      ['accept', '0', '0', null],
    ])
    expect(
      h
        .frames(78)
        .flatMap(batchesOf)
        .filter(({ acknowledgeTypes }) => acknowledgeTypes.includes(3))
    ).toEqual([])
    expect(h.seen.crashes).toEqual([])
    expect(new Set(joins().map(({ fields }) => fields.memberId)).size).toBe(1)
    expect(joins()).toHaveLength(2)
  })

  test('settles a written request lost once when the membership changes and a replacement succeeds', async () => {
    await start()
    openWith([oneRecord(), oneRecord()])
    h.script('ShareGroupHeartbeat', [undefined, FENCE])
    const held = holdAcknowledgingFetch()
    const second = deferred()
    const counts = []
    await run({
      eachMessage: async ({ message, acknowledge }) => {
        counts.push(message.deliveryCount)
        if (counts.length < 2) return
        await second.promise
        acknowledge('reject')
      },
    })
    await held.arrived()
    expect(batchesOf(held.call)).toEqual([
      { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] },
    ])
    await beat(50, 2)
    await until(() => h.seen.acks.length === 1, 'the lost settlement')
    expect(h.seen.acks[0]).toMatchObject({
      topic: 'topic-a',
      partition: 0,
      firstOffset: '0',
      lastOffset: '0',
      type: 'accept',
      error: { reason: 'membership-lost', ranges: [{ firstOffset: '0', lastOffset: '0' }] },
    })
    await flush()
    await advance(50)
    await until(() => counts.length === 2, 'the replacement delivery')
    held.release()
    await flush()
    expect(h.seen.acks).toHaveLength(1)
    expect(h.frames(78)).toHaveLength(3)
    expect(h.frames(79)).toHaveLength(0)
    second.resolve()
    await until(() => h.seen.acks.length === 2, 'the replacement settlement')
    expect(counts).toEqual([1, 1])
    expect(outcomes()).toEqual([
      ['accept', '0', '0', 'membership-lost'],
      ['reject', '0', '0', null],
    ])
    expect(h.frames(78).map(batchesOf)).toEqual([
      [],
      [{ firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] }],
      [],
      [{ firstOffset: '0', lastOffset: '0', acknowledgeTypes: [3] }],
    ])
    expect(joins()).toHaveLength(2)
  })

  test('keeps a data acquisition when only the coordinator socket reconnects', async () => {
    await start()
    openWith([oneRecord()])
    const held = holdAcknowledgingFetch()
    await run({ eachMessage: async () => {} })
    await held.arrived()
    const [join] = h.frames(76)
    h.cluster.sockets()[join.generation - 1].destroy()
    await beat(50, 2)
    expect(h.frames(76)[1].generation).toBeGreaterThan(join.generation)
    held.release()
    await until(() => h.seen.acks.length === 1, 'the confirmed settlement')
    expect(outcomes()).toEqual([['accept', '0', '0', null]])
    await advance(50)
    await until(() => h.frames(78).length === 3, 'the next fetch')
    expect(h.frames(78).map(({ fields }) => fields.shareSessionEpoch)).toEqual([0, 1, 2])
    expect(new Set(h.frames(78).map(({ generation }) => generation)).size).toBe(1)
    expect(h.seen.resets).toEqual([])
    expect(h.seen.joins).toHaveLength(1)
    expect(h.seen.crashes).toEqual([])
  })

  test('settles old records session-lost once across a data socket generation change', async () => {
    await start()
    openWith([oneRecord(), oneRecord()])
    const held = holdAcknowledgingFetch()
    const second = deferred()
    const counts = []
    await run({
      eachMessage: async ({ message, acknowledge }) => {
        counts.push(message.deliveryCount)
        if (counts.length < 2) return
        await second.promise
        acknowledge('reject')
      },
    })
    await held.arrived()
    h.cluster.sockets()[held.call.generation - 1].destroy()
    await until(() => h.seen.acks.length === 1, 'the lost settlement')
    expect(outcomes()).toEqual([['accept', '0', '0', 'session-lost']])
    expect(h.seen.resets).toEqual([{ nodeId: 0, reason: 'connection', recordsDropped: 1 }])
    await flush()
    await advance(50)
    await until(() => counts.length === 2, 'the fresh opening delivery')
    held.release()
    await flush()
    expect(h.seen.acks).toHaveLength(1)
    const frames = h.frames(78)
    expect(frames.map(({ fields }) => fields.shareSessionEpoch)).toEqual([0, 1, 0])
    expect(batchesOf(frames[2])).toEqual([])
    expect(frames[2].generation).toBeGreaterThan(frames[1].generation)
    second.resolve()
    await until(() => h.seen.acks.length === 2, 'the replacement settlement')
    expect(batchesOf(h.frames(78)[3])).toEqual([
      { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [3] },
    ])
    expect(outcomes()).toEqual([
      ['accept', '0', '0', 'session-lost'],
      ['reject', '0', '0', null],
    ])
    expect(h.frames(79)).toHaveLength(0)
  })

  test('freezes only the explicit decisions of a running handler and loses its undecided records once', async () => {
    await start()
    openWith([
      fetchAnswer([
        acquire(
          0,
          0,
          Array.from({ length: 10 }, (_, i) => `v${i}`)
        ),
      ]),
    ])
    h.script('ShareGroupHeartbeat', [undefined, FENCE])
    const gate = deferred()
    const decided = deferred()
    await run({
      eachBatch: async ({ batch, acknowledge }) => {
        const [m0, m1, m2, m3] = batch.messages
        acknowledge(m0, 'accept')
        acknowledge(m1, 'reject')
        acknowledge(m2, 'release')
        acknowledge(m3, 'accept')
        decided.resolve()
        await gate.promise
      },
    })
    await decided.promise
    await beat(50, 2)
    await until(() => finals().length === 1 && h.seen.acks.length === 5, 'the captured final')
    expect(batchesOf(finals()[0])).toEqual([
      { firstOffset: '0', lastOffset: '3', acknowledgeTypes: [1, 3, 2, 1] },
    ])
    expect(outcomes()).toEqual([
      ['release', '4', '9', 'membership-lost'],
      ['accept', '0', '0', null],
      ['reject', '1', '1', null],
      ['release', '2', '2', null],
      ['accept', '3', '3', null],
    ])
    expect(h.frames(79)).toHaveLength(1)
    gate.resolve()
  })

  test('captures the unstarted records of a running eachMessage as one release', async () => {
    await start()
    openWith([fetchAnswer([acquire(0, 0, ['a', 'b', 'c', 'd', 'e', 'f', 'g'])])])
    h.script('ShareGroupHeartbeat', [undefined, FENCE])
    const gate = deferred()
    const decided = deferred()
    const started = []
    await run({
      eachMessage: async ({ message, acknowledge }) => {
        started.push(message.offset)
        acknowledge('accept')
        decided.resolve()
        await gate.promise
      },
    })
    await decided.promise
    await beat(50, 2)
    await until(() => finals().length === 1 && h.seen.acks.length === 2, 'the captured final')
    expect(started).toEqual(['0'])
    expect(batchesOf(finals()[0])).toEqual([
      { firstOffset: '0', lastOffset: '6', acknowledgeTypes: [1, 2, 2, 2, 2, 2, 2] },
    ])
    expect(outcomes()).toEqual([
      ['accept', '0', '0', null],
      ['release', '1', '6', null],
    ])
    gate.resolve()
  })

  test('separates two answers that repeat an offset and a send time', async () => {
    await start()
    // Same topic, partition, offset and open epoch; only the delivery count tells them apart.
    openWith([oneRecord(1), oneRecord(2)])
    const held = holdAcknowledgingFetch()
    const second = deferred()
    const counts = []
    await run({
      eachMessage: async ({ message, acknowledge }) => {
        counts.push(message.deliveryCount)
        if (counts.length < 2) return
        await second.promise
        acknowledge('reject')
      },
    })
    await held.arrived()
    held.release(topError(122))
    await until(() => counts.length === 2, 'the second acquisition')
    expect(counts).toEqual([1, 2])
    expect(outcomes()).toEqual([['accept', '0', '0', 'session-lost']])
    second.resolve()
    await until(() => h.seen.acks.length === 2, 'the second settlement')
    expect(outcomes()).toEqual([
      ['accept', '0', '0', 'session-lost'],
      ['reject', '0', '0', null],
    ])
    expect(h.frames(78).map(({ fields }) => fields.shareSessionEpoch)).toEqual([0, 1, 0, 1])
    expect(h.frames(78).map(batchesOf)).toEqual([
      [],
      [{ firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] }],
      [],
      [{ firstOffset: '0', lastOffset: '0', acknowledgeTypes: [3] }],
    ])
    await advance(50)
    await until(() => h.frames(78).length === 5, 'an acknowledgement-only round')
    await flush()
    expect(counts).toEqual([1, 2])
  })

  describe('poll expiry over two leader nodes', () => {
    /**
     * Node 0 delivers one record to a handler that stays held; node 1 delivers a record per fetch
     * to handlers the test releases one at a time.
     * @param {object} [share] `shareConsumer` option overrides.
     * @returns {Promise<{aGate: object, bGates: object[], started: object}>} The handler gates.
     */
    const twoNodes = async (share = {}) => {
      await start({ nodes: 2, topics: TWO_NODES, share: { maxPollInterval: 200, ...share } })
      let given = false
      let next = 0
      h.cluster.answer('ShareFetch', (fields, call) => {
        if (call.node === 0 && !given && fields.shareSessionEpoch === 0) {
          given = true
          return fetchAnswer([acquire(0, 0, ['a'])])
        }
        if (call.node === 1 && next < 6) return fetchAnswer([acquire(1, next++, ['b'])])
        return echo('ShareFetch', fields)
      })
      const aGate = deferred()
      const bGates = Array.from({ length: 6 }, () => deferred())
      const started = { a: 0, b: 0 }
      await run({
        eachMessage: async ({ partition, message }) => {
          if (partition === 0) {
            started.a++
            await aGate.promise
          } else {
            started.b++
            await bGates[Number(message.offset)].promise
          }
        },
      })
      return { aGate, bGates, started }
    }

    test('expires a held handler despite progress at another node and rejoins only after it returns', async () => {
      const { aGate, bGates, started } = await twoNodes()
      await until(() => started.a === 1 && started.b === 1, 'both handlers')
      for (let k = 0; k < 4; k++) {
        await beat(50, k + 2)
        bGates[k].resolve()
        await until(() => started.b === k + 2, `progress at node 1 (${k + 2})`)
      }
      expect(leaves()).toHaveLength(0)
      expect(h.seen.crashes).toEqual([])
      await advance(50)
      await until(() => leaves().length === 1, 'the poll-expiry leave')
      await flush()
      await advance(100)
      expect(leaves()).toHaveLength(1)
      expect(joins()).toHaveLength(1)
      aGate.resolve()
      await flush()
      await advance(1)
      await until(() => joins().length === 2, 'the rejoin after the held handler returned')
      expect(leaves()).toHaveLength(1)
      expect(new Set(joins().map(({ fields }) => fields.memberId)).size).toBe(1)
      expect(h.seen.crashes).toEqual([])
      for (const gate of bGates) gate.resolve()
    })
  })

  describe('crash supervision', () => {
    /**
     * @returns {Promise<{restartOnFailure: Function, gate: object}>} A client whose first fetch
     *   answers a fatal partition error and whose restart verdict waits for the test.
     */
    const crashWithHeldVerdict = async () => {
      const gate = deferred()
      const restartOnFailure = jest.fn(() => gate.promise.then(() => true))
      await start({ share: { retry: { restartOnFailure } } })
      h.script('ShareFetch', [fetchAnswer([part({ partition: 0, code: 2 })])])
      await run({ eachMessage: async () => {} })
      await until(() => restartOnFailure.mock.calls.length === 1, 'the restart verdict request')
      return { restartOnFailure, gate }
    }

    test('ignores an old restart verdict released after a stop', async () => {
      const { restartOnFailure, gate } = await crashWithHeldVerdict()
      await h.consumer.stop()
      const timers = jest.getTimerCount()
      gate.resolve()
      await until(() => h.seen.crashes.length === 1, 'the crash event')
      await flush()
      expect(h.seen.crashes[0]).toMatchObject({ restart: false, groupId: 'group' })
      expect(h.seen.crashes[0].error.code).toBe(2)
      expect(restartOnFailure).toHaveBeenCalledTimes(1)
      expect(jest.getTimerCount()).toBeLessThanOrEqual(timers)
      await advance(5000)
      expect(joins()).toHaveLength(1)
      await run({ eachMessage: async () => {} })
      expect(joins()).toHaveLength(2)
      expect(new Set(joins().map(({ fields }) => fields.memberId)).size).toBe(2)
      expect(h.seen.crashes).toHaveLength(1)
    })

    test('restarts under the same schedule when no stop arrives', async () => {
      const { gate } = await crashWithHeldVerdict()
      gate.resolve()
      await until(() => h.seen.crashes.length === 1, 'the crash event')
      expect(h.seen.crashes[0].restart).toBe(true)
      await flush()
      await advance(50)
      await until(() => joins().length === 2, 'the restarted run')
      expect(new Set(joins().map(({ fields }) => fields.memberId)).size).toBe(2)
    })
  })

  describe('held metadata flights', () => {
    /**
     * Holds every by-id metadata flight inside the cluster helper until the test releases it.
     * @returns {Array<{ids: string[], gate: object}>} The flights, oldest first.
     */
    const holdFlights = () => {
      const flights = []
      const original = Cluster.prototype.refreshMetadataForTopicIds
      jest
        .spyOn(Cluster.prototype, 'refreshMetadataForTopicIds')
        .mockImplementation(function (ids, scope) {
          const gate = deferred()
          flights.push({ ids, gate })
          return gate.promise.then(() => original.call(this, ids, scope))
        })
      return flights
    }
    /**
     * @param {...string} topicIds Topic ids of the assignment, partition 0 each.
     * @returns {object} A steady heartbeat answer carrying that assignment.
     */
    const target = (...topicIds) => ({
      memberEpoch: 1,
      heartbeatIntervalMs: 50,
      assignment: { topicPartitions: topicIds.map((topicId) => ({ topicId, partitions: [0] })) },
    })
    /** @returns {object[]} The by-id metadata requests that reached the wire for the unknown id. */
    const wireFlights = () =>
      h.cluster
        .requests()
        .filter(
          ({ api, fields }) => api === 'Metadata' && fields.topics?.some((t) => t.topicId === U1)
        )

    /** Lets the cluster name the unknown id, so a flight that was not discarded would install it. */
    const nameUnknownId = () =>
      h.table.set('topic-u1', { topicId: U1, partitions: [{ leader: 0, epoch: 1 }] })

    test('discards a held unresolved-id flight after the assignment names another id', async () => {
      const flights = holdFlights()
      await start()
      openWith([fetchAnswer([acquire(0, 0, ['a'])])])
      h.script('ShareGroupHeartbeat', [target(U1), target(A)])
      const handled = []
      await run({ eachMessage: async ({ message }) => handled.push(message.offset) })
      await until(() => flights.length === 1, 'the held flight')
      expect(flights[0].ids).toEqual([U1])
      expect(h.seen.assigned).toEqual([])
      await beat(50, 2)
      await until(() => handled.length === 1, 'the delivery of the current id')
      nameUnknownId()
      flights[0].gate.resolve()
      await flush()
      expect(wireFlights()).toEqual([])
      expect(handled).toEqual(['0'])
      expect(h.seen.assigned).toHaveLength(1)
      expect(h.seen.assigned[0]).toMatchObject({
        assignment: [{ topic: 'topic-a', partitions: [0] }],
        added: [{ topic: 'topic-a', partitions: [0] }],
        removed: [],
      })
      expect(h.consumer.assignment()).toEqual([{ topic: 'topic-a', partitions: [0] }])
    })

    test('publishes the assignment of a held flight when its target is still current', async () => {
      const flights = holdFlights()
      await start()
      h.script('ShareGroupHeartbeat', [target(U1)])
      await run({ eachMessage: async () => {} })
      await until(() => flights.length === 1, 'the held flight')
      nameUnknownId()
      flights[0].gate.resolve()
      await until(() => h.seen.assigned.length === 1, 'the resolved assignment')
      expect(wireFlights().length).toBeGreaterThanOrEqual(1)
      expect(h.seen.assigned[0].added).toEqual([{ topic: 'topic-u1', partitions: [0] }])
    })

    test('discards the held flight of a replaced target revision with identical eventual ids', async () => {
      const flights = holdFlights()
      await start()
      openWith([fetchAnswer([acquire(0, 0, ['a'])])])
      h.script('ShareGroupHeartbeat', [target(A, U1), target(A), target(A, U1)])
      const handled = []
      await run({ eachMessage: async ({ message }) => handled.push(message.offset) })
      await until(() => flights.length === 1 && handled.length === 1, 'the known id while U1 waits')
      expect(flights[0].ids).toEqual([U1])
      await beat(50, 2)
      await flush()
      await beat(50, 3)
      await until(() => flights.length === 2, 'the flight of the new revision')
      expect(flights[1].ids).toEqual([U1])
      nameUnknownId()
      flights[0].gate.resolve()
      await flush()
      expect(wireFlights()).toEqual([])
      flights[1].gate.resolve()
      await until(() => wireFlights().length >= 1, 'the current flight on the wire')
      expect(handled).toEqual(['0'])
    })
  })

  test('publishes no heartbeat for a stale or failed answer and one group join per membership', async () => {
    await start()
    const steady = (heartbeatIntervalMs) => ({
      memberEpoch: 1,
      heartbeatIntervalMs,
      assignment: null,
    })
    h.script('ShareGroupHeartbeat', [
      undefined,
      steady(70),
      { memberEpoch: 0, heartbeatIntervalMs: 999, assignment: null },
      steady(90),
      { errorCode: 14 },
      undefined,
    ])
    await run({ eachMessage: async () => {} })
    const [{ fields: first }] = h.frames(76)
    const beatOf = (heartbeatIntervalMs) => ({
      groupId: 'group',
      memberId: first.memberId,
      memberEpoch: 1,
      heartbeatIntervalMs,
    })
    expect(h.seen.beats).toEqual([beatOf(50)])
    await beat(50, 2)
    await until(() => h.seen.beats.length === 2, 'the first steady beat')
    expect(h.seen.beats).toEqual([beatOf(50), beatOf(70)])
    await beat(70, 3)
    await flush()
    expect(h.seen.beats).toEqual([beatOf(50), beatOf(70)])
    await beat(70, 4)
    await until(() => h.seen.beats.length === 3, 'the third answered beat')
    await beat(90, 5)
    await flush()
    expect(h.seen.beats).toHaveLength(3)
    await beat(50, 6)
    await until(() => h.seen.beats.length === 4, 'the beat after the failed one')
    expect(h.seen.beats).toEqual([beatOf(50), beatOf(70), beatOf(90), beatOf(50)])
    expect(h.seen.joins).toEqual([{ groupId: 'group', memberId: first.memberId, memberEpoch: 1 }])
    expect(h.seen.crashes).toEqual([])
  })

  test('a stop requested by the join observer cannot duplicate the join or start a handler', async () => {
    await start()
    let stopping
    h.consumer.on(h.consumer.events.GROUP_JOIN, () => {
      stopping = h.consumer.stop()
    })
    const joined = h.consumer.run({ eachMessage: async () => {} })
    await expect(joined).resolves.toBeUndefined()
    await stopping
    expect(h.seen.joins).toHaveLength(1)
    expect(h.seen.beats).toHaveLength(1)
    expect(h.seen.stops).toEqual([true])
    expect(h.frames(78)).toHaveLength(0)
    expect(leaves()).toHaveLength(1)
    expect(joins()).toHaveLength(1)
  })
})
