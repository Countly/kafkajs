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
const Broker = require('../../../broker')
const Cluster = require('../../../cluster')
const RequestQueue = require('../../../network/requestQueue')
const { requests } = require('../../../protocol/requests')
const { ShareFetchLoop } = require('../shareFetchLoop')
const { ShareRunner } = require('../shareRunner')

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

/** Holds the next ordinary ShareAcknowledge (any epoch but -1) at one stage of its write path. */
class OrdinaryAckHold {
  /**
   * @param {'encode'|'queue'} stage Where the request waits: inside its encoder, after the guard of
   *   the prepare step passed and before it enters the request queue.
   */
  constructor(stage) {
    this.gate = deferred()
    this.arrival = deferred()
    const { gate, arrival } = this
    let taken = false
    if (stage === 'encode') {
      const original = requests.ShareAcknowledge.protocol
      jest.spyOn(requests.ShareAcknowledge, 'protocol').mockImplementation((selection) => {
        const factory = original(selection)
        return (options) => {
          const built = factory(options)
          if (taken || options?.shareSessionEpoch === -1) return built
          taken = true
          const { encode } = built.request
          return {
            ...built,
            request: {
              ...built.request,
              encode: async () => {
                arrival.resolve()
                await gate.promise
                return encode()
              },
            },
          }
        }
      })
    } else {
      const original = RequestQueue.prototype.push
      jest.spyOn(RequestQueue.prototype, 'push').mockImplementation(function (pushed) {
        if (taken || pushed.entry.apiKey !== 79) return original.call(this, pushed)
        taken = true
        arrival.resolve()
        gate.promise.then(() => {
          try {
            original.call(this, pushed)
          } catch (error) {
            pushed.entry.reject(error)
          }
        })
      })
    }
  }

  /** @returns {Promise<void>} Resolves once the request reached the held stage. */
  arrived() {
    return this.arrival.promise
  }

  /** @returns {void} Lets the request continue from its stage. */
  release() {
    this.gate.resolve()
  }
}

describe('public share token contract', () => {
  let h
  /** Test-owned gates, released before the client closes so no held continuation outlives a test. */
  const holds = []
  const handlers = []
  const continuations = []
  /** @returns {object} A test gate released even when an assertion fails before its normal release. */
  const heldGate = () => {
    const gate = deferred()
    holds.push({ release: () => gate.resolve() })
    return gate
  }
  beforeEach(async () => {
    jest.useFakeTimers(FAKE)
    await advance(1000)
  })
  afterEach(async () => {
    let settled
    try {
      for (const hold of holds.splice(0)) hold.release()
      await h?.close()
    } finally {
      try {
        settled = await Promise.allSettled([...handlers.splice(0), ...continuations.splice(0)])
      } finally {
        h = undefined
        jest.useRealTimers()
        jest.restoreAllMocks()
      }
    }
    const failed = settled.find(({ status }) => status === 'rejected')
    if (failed) throw failed.reason
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
    const key = config.eachMessage ? 'eachMessage' : 'eachBatch'
    const handler = config[key]
    await h.consumer.run({
      ...config,
      [key]: (payload) => {
        const pending = handler(payload)
        handlers.push(pending)
        return pending
      },
    })
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
   * Stops while node loops may sit in their 50 ms idle pacing, which a clean stop waits for.
   * @returns {Promise<void>}
   * @throws {Error} When the close did not end within ten steps.
   */
  const stopIdle = async () => {
    const stopping = track(h.consumer.stop())
    await advanceUntil(() => stopping.settled, 'the clean stop')
    expect(stopping.error).toBeNull()
  }
  /** @returns {number[]} The member epoch of every heartbeat frame, in wire order. */
  const heartbeatEpochs = () => h.frames(76).map(({ fields }) => fields.memberEpoch)

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
    const replacement = heldGate()
    const returned = deferred()
    const calls = []
    const during = {}
    try {
      await run({
        eachMessage: async ({ acknowledge, renew, isRunning }) => {
          calls.push(true)
          if (calls.length > 1) {
            await replacement.promise
            returned.resolve()
            return
          }
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
    } finally {
      replacement.resolve()
      if (calls.length > 1) await returned.promise
    }
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

  test('sends no frame from the old lookup when a replacement run joins while it is held', async () => {
    await start()
    const lookup = h.hold({ api: 'FindCoordinator' })
    const first = h.consumer.run({ eachMessage: async () => {} })
    await lookup.arrived()
    await h.consumer.stop()
    await first
    await run({ eachMessage: async () => {} })
    const [{ fields: replacement }] = joins()
    expect(h.frames(76)).toHaveLength(1)
    lookup.release()
    await flush()
    expect(h.frames(76)).toHaveLength(1)
    expect(h.seen.joins).toEqual([
      { groupId: 'group', memberId: replacement.memberId, memberEpoch: 1 },
    ])
    expect(h.consumer.assignment()).toEqual([{ topic: 'topic-a', partitions: [0] }])
  })

  describe('a normal heartbeat held at the coordinator lookup', () => {
    /**
     * Joins, answers the second beat with code 14 so the coordinator is forgotten, and holds the
     * lookup the third beat then makes.
     * @param {boolean} [withData=false] Also holds the node's second ShareFetch, so a data answer
     *   can end the membership while the lookup waits.
     * @returns {Promise<{lookup: object, data: object|null}>} The held lookup and data fetch.
     */
    const holdRejoinLookup = async (withData = false) => {
      await start()
      h.script('ShareGroupHeartbeat', [undefined, { errorCode: 14 }])
      const data = withData ? holdAcknowledgingFetch() : null
      await run({ eachMessage: async () => {} })
      const lookup = h.hold({ api: 'FindCoordinator' })
      await beat(50, 2)
      await advance(50)
      await lookup.arrived()
      if (data) await data.arrived()
      expect(heartbeatEpochs()).toEqual([0, 1])
      return { lookup, data }
    }

    test('sends no frame from the held lookup of a stopped run and a fresh run joins', async () => {
      const { lookup } = await holdRejoinLookup()
      await stopIdle()
      expect(heartbeatEpochs()).toEqual([0, 1])
      lookup.release()
      await flush()
      expect(heartbeatEpochs()).toEqual([0, 1])
      await run({ eachMessage: async () => {} })
      expect(heartbeatEpochs()).toEqual([0, 1, 0])
      expect(new Set(joins().map(({ fields }) => fields.memberId)).size).toBe(2)
    })

    test('sends no frame from the held lookup of a fenced membership and the rejoin keeps its member', async () => {
      const { lookup, data } = await holdRejoinLookup(true)
      data.release(topError(25))
      await until(() => h.seen.assigned.length === 2, 'the fence')
      expect(heartbeatEpochs()).toEqual([0, 1])
      openWith([oneRecord()])
      await advanceUntil(
        () => h.seen.acks.length === 1 && joins().length === 2,
        'the rejoin and acquisition'
      )
      expect(heartbeatEpochs()).toEqual([0, 1, 0])
      expect(outcomes()).toEqual([['accept', '0', '0', null]])
      lookup.release()
      await flush()
      expect(heartbeatEpochs()).toEqual([0, 1, 0])
      expect(new Set(joins().map(({ fields }) => fields.memberId)).size).toBe(1)
      await beat(50, 4)
      expect(heartbeatEpochs()).toEqual([0, 1, 0, 1])
      expect(h.seen.crashes).toEqual([])
    })

    test('sends no frame from the held lookup of a crashed run and a fresh run joins', async () => {
      const { lookup, data } = await holdRejoinLookup(true)
      data.release(fetchAnswer([part({ partition: 0, code: 2 })]))
      await until(() => h.seen.crashes.length === 1, 'the crash event')
      await flush()
      expect(h.seen.crashes[0]).toMatchObject({ restart: false, groupId: 'group' })
      expect(h.seen.crashes[0].error.code).toBe(2)
      expect(heartbeatEpochs()).toEqual([0, 1])
      openWith([oneRecord()])
      await run({ eachMessage: async () => {} })
      await until(() => h.seen.acks.length === 1, 'the fresh acquisition settlement')
      expect(outcomes()).toEqual([['accept', '0', '0', null]])
      lookup.release()
      await flush()
      expect(heartbeatEpochs()).toEqual([0, 1, 0])
      expect(new Set(joins().map(({ fields }) => fields.memberId)).size).toBe(2)
      await beat(50, 4)
      expect(heartbeatEpochs()).toEqual([0, 1, 0, 1])
      expect(h.seen.crashes).toHaveLength(1)
    })
  })

  test('keeps a replacement run current when the stopped heartbeat lookup is released late', async () => {
    await start()
    h.script('ShareGroupHeartbeat', [undefined, { errorCode: 14 }])
    await run({ eachMessage: async () => {} })
    const lookup = h.hold({ api: 'FindCoordinator' })
    await beat(50, 2)
    await advance(50)
    await lookup.arrived()
    await stopIdle()
    openWith([oneRecord()])
    const delivered = []
    await run({ eachMessage: async ({ message }) => delivered.push(message.offset) })
    await until(() => h.seen.acks.length === 1, 'the replacement acquisition settlement')
    const replacementId = joins()[1].fields.memberId
    expect(heartbeatEpochs()).toEqual([0, 1, 0])
    expect(delivered).toEqual(['0'])
    expect(outcomes()).toEqual([['accept', '0', '0', null]])
    lookup.release()
    await flush()
    expect(heartbeatEpochs()).toEqual([0, 1, 0])
    expect(h.consumer.assignment()).toEqual([{ topic: 'topic-a', partitions: [0] }])
    await beat(50, 4)
    expect(heartbeatEpochs()).toEqual([0, 1, 0, 1])
    expect(h.frames(76)[3].fields.memberId).toBe(replacementId)
    expect(h.seen.joins).toHaveLength(2)
    expect(h.seen.crashes).toEqual([])
  })

  test('keeps a running handler out of the next membership after a fence', async () => {
    await start()
    openWith([oneRecord(), oneRecord()])
    h.script('ShareGroupHeartbeat', [undefined, { errorCode: 25 }])
    const gate = heldGate()
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
    const second = heldGate()
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
    h.cluster.sockets()[join.generation - 1].emit('end')
    await beat(50, 2)
    expect(h.frames(76)[1].generation).toBeGreaterThan(join.generation)
    held.release()
    await until(() => h.seen.acks.length === 1, 'the confirmed settlement')
    expect(outcomes()).toEqual([['accept', '0', '0', null]])
    // The reconnect tick already covered pacing; another tick can send a fourth fetch.
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
    const second = heldGate()
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
    h.cluster.sockets()[held.call.generation - 1].emit('end')
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
    const gate = heldGate()
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
    const gate = heldGate()
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
    const second = heldGate()
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

  test('ignores an old acquisition-bearing answer released after its replacement was delivered', async () => {
    jest.spyOn(performance, 'now').mockReturnValue(1000)
    const openings = []
    const newExchange = ShareFetchLoop.prototype.newExchange
    jest.spyOn(ShareFetchLoop.prototype, 'newExchange').mockImplementation(function (...args) {
      const exchange = newExchange.apply(this, args)
      if (exchange.kind === 'fetch' && this.session.epoch === 0) openings.push(exchange)
      return exchange
    })
    // A frozen monotonic clock also needs a zero membership backoff for the rejoin to become due.
    await start({ share: { retry: { initialRetryTime: 0, maxRetryTime: 0 } } })
    const stale = fetchAnswer(
      [part({ partition: 0, acquire: [acquired(0, 0, 7)], records: recordsHex(0, ['old']) })],
      { acquisitionLockTimeoutMs: 7777 }
    )
    openWith([stale, oneRecord(1)])
    h.script('ShareGroupHeartbeat', [undefined, FENCE])
    // Park the decoded answer so socket teardown cannot prevent its eventual delivery to the loop.
    const original = Broker.prototype.shareFetch
    const arrival = deferred()
    const gate = heldGate()
    const done = deferred()
    let taken = false
    jest.spyOn(Broker.prototype, 'shareFetch').mockImplementation(async function (...args) {
      const first = !taken
      taken = true
      if (!first) return original.apply(this, args)
      continuations.push(done.promise)
      try {
        const response = await original.apply(this, args)
        arrival.resolve()
        await gate.promise
        return response
      } finally {
        done.resolve()
      }
    })
    const second = heldGate()
    const delivered = []
    const config = {
      eachMessage: async ({ message, acknowledge }) => {
        delivered.push([message.offset, message.deliveryCount, message.value.toString()])
        await second.promise
        acknowledge('reject')
      },
    }
    await run(config)
    await arrival.promise
    expect(delivered).toEqual([])
    await advanceUntil(() => delivered.length === 1 && h.seen.joins.length === 2, 'the replacement')
    expect(openings).toHaveLength(2)
    expect(openings.map(({ sentAt }) => sentAt)).toEqual([1000, 1000])
    expect(openings[0].open).not.toBe(openings[1].open)
    expect(h.frames(78).map(({ fields }) => fields.shareSessionEpoch)).toEqual([0, 0])
    expect(delivered).toEqual([['0', 1, 'a']])
    expect(h.consumer.acquisitionLockTimeoutMs()).toBe(15000)
    gate.resolve()
    await done.promise
    await flush()
    expect(delivered).toEqual([['0', 1, 'a']])
    expect(h.consumer.acquisitionLockTimeoutMs()).toBe(15000)
    expect(outcomes()).toEqual([])
    second.resolve()
    await until(() => h.seen.acks.length === 1, 'the replacement settlement')
    expect(outcomes()).toEqual([['reject', '0', '0', null]])
    expect(h.frames(78).map(batchesOf)).toEqual([
      [],
      [],
      [{ firstOffset: '0', lastOffset: '0', acknowledgeTypes: [3] }],
    ])
    expect(h.seen.crashes).toEqual([])
  })

  test('an old callback of a returned handler throws while a redelivery of the same offset works', async () => {
    await start()
    // The clock never moves, so both acquisitions carry the same send time, offset, count and epoch.
    h.script('ShareFetch', [oneRecord(), oneRecord()])
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
    await until(() => h.frames(78).length >= 3 && h.seen.acks.length === 2, 'both acquisitions')
    expect(calls.map(({ count }) => count)).toEqual([1, 1])
    expect(calls[1].thrown.map((error) => [error.name, error.message])).toEqual([
      ['KafkaJSNonRetriableError', 'Handler call is no longer current'],
      ['KafkaJSNonRetriableError', 'Handler call is no longer current'],
    ])
    expect(calls[1].oldRunning).toBe(true)
    const rounds = h.frames(78).slice(0, 3)
    expect(rounds.map(({ fields }) => fields.shareSessionEpoch)).toEqual([0, 1, 2])
    expect(rounds.map(batchesOf)).toEqual([
      [],
      [{ firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] }],
      [{ firstOffset: '0', lastOffset: '0', acknowledgeTypes: [3] }],
    ])
    expect(outcomes()).toEqual([
      ['accept', '0', '0', null],
      ['reject', '0', '0', null],
    ])
    expect(h.frames(79)).toHaveLength(0)
  })

  describe('an ordinary renewal held before the wire', () => {
    test.each(['encode', 'queue'])(
      'refuses an ordinary renewal held at the %s stage once the membership is fenced',
      async (stage) => {
        await start()
        openWith([oneRecord(), oneRecord()])
        h.script('ShareGroupHeartbeat', [undefined, FENCE])
        let hold
        const results = []
        await run({
          eachMessage: async ({ renew }) => {
            if (results.length === 0) {
              hold = new OrdinaryAckHold(stage)
              holds.push(hold)
            }
            results.push(
              await renew().then(
                (duration) => duration,
                (error) => error
              )
            )
          },
        })
        await until(() => hold !== undefined, 'the renewing handler')
        await hold.arrived()
        expect(h.frames(79)).toHaveLength(0)
        await beat(50, 2)
        await until(() => results.length === 1, 'the refused renewal')
        expect(results[0]).toMatchObject({
          name: 'KafkaJSShareAcknowledgementError',
          reason: 'membership-lost',
          ranges: [{ firstOffset: '0', lastOffset: '0' }],
        })
        expect(outcomes()).toHaveLength(2)
        expect(outcomes()).toEqual(
          expect.arrayContaining([
            ['renew', '0', '0', 'membership-lost'],
            ['release', '0', '0', 'membership-lost'],
          ])
        )
        hold.release()
        await flush()
        expect(h.frames(79)).toHaveLength(0)
        await advanceUntil(() => results.length === 2, 'the replacement renewal')
        expect(results[1]).toBe(15000)
        expect(h.frames(79)).toHaveLength(1)
        const [renewal] = h.frames(79)
        expect(renewal.fields.isRenewAck).toBe(true)
        expect(renewal.fields.shareSessionEpoch).toBe(1)
        expect(batchesOf(renewal)).toEqual([
          { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [4] },
        ])
        expect(h.seen.crashes).toEqual([])
      }
    )
  })

  describe('a written renewal held at its response', () => {
    /**
     * Starts a run whose handler renews and records how the renewal ended.
     * @param {Array<object>} openings The ShareFetch answers of each opening.
     * @returns {Promise<{held: object, results: Array<number|Error>}>} The held renewal's gate
     *   once its frame was written, and the renewal results.
     */
    const startRenewing = async (openings) => {
      await start()
      openWith(openings)
      const held = h.hold({ api: 'ShareAcknowledge' })
      const results = []
      await run({
        eachMessage: async ({ renew }) => {
          results.push(
            await renew().then(
              (duration) => duration,
              (error) => error
            )
          )
        },
      })
      await held.arrived()
      expect(results).toEqual([])
      return { held, results }
    }

    test('settles a renewal lost once and ignores its late answer after the data session resets', async () => {
      const { held, results } = await startRenewing([oneRecord(), oneRecord()])
      h.cluster.sockets()[held.call.generation - 1].emit('end')
      await until(() => h.seen.acks.length === 2, 'the lost outcomes')
      expect(outcomes()).toEqual(
        expect.arrayContaining([
          ['renew', '0', '0', 'session-lost'],
          ['release', '0', '0', 'session-lost'],
        ])
      )
      expect(h.seen.resets).toEqual([{ nodeId: 0, reason: 'connection', recordsDropped: 1 }])
      await advanceUntil(() => results.length === 2, 'the replacement renewal')
      expect(results[0]).toMatchObject({
        name: 'KafkaJSShareAcknowledgementError',
        reason: 'session-lost',
        ranges: [{ firstOffset: '0', lastOffset: '0' }],
      })
      expect(results[1]).toBe(15000)
      held.release()
      await flush()
      expect(outcomes().filter(([, , , reason]) => reason === 'session-lost')).toHaveLength(2)
      const renewals = h.frames(79)
      expect(renewals).toHaveLength(2)
      expect(renewals.map(({ fields }) => fields.isRenewAck)).toEqual([true, true])
      expect(renewals[1].generation).toBeGreaterThan(renewals[0].generation)
      expect(h.seen.crashes).toEqual([])
    })

    test('retires a written renewal when a routing update moves its partition and ignores its late answer', async () => {
      let cluster
      const routing = Cluster.prototype.onRoutingChange
      jest.spyOn(Cluster.prototype, 'onRoutingChange').mockImplementation(function (listener) {
        cluster = this
        return routing.call(this, listener)
      })
      await start({ nodes: 2 })
      const opened = new Set()
      h.cluster.answer('ShareFetch', (fields, call) => {
        if (fields.shareSessionEpoch !== 0 || opened.has(call.node))
          return echo('ShareFetch', fields)
        opened.add(call.node)
        return oneRecord(call.node + 1)
      })
      const old = h.hold({ api: 'ShareAcknowledge', node: 0 })
      const results = []
      await run({
        eachMessage: async ({ renew, acknowledge }) => {
          const index = results.length
          const result = { pending: true }
          results.push(result)
          result.value = await renew().then(
            (duration) => duration,
            (error) => error
          )
          result.pending = false
          if (index === 1) acknowledge('reject')
        },
      })
      await old.arrived()
      expect(results).toEqual([{ pending: true }])
      expect(old.call.fields.isRenewAck).toBe(true)
      expect(batchesOf(old.call)).toEqual([
        { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [4] },
      ])
      await expect(
        cluster.applyLeaderHint({ topic: 'topic-a', partition: 0, leaderId: 1, leaderEpoch: 2 })
      ).resolves.toBe(true)
      expect(cluster.cachedLeader('topic-a', 0)).toBe(1)
      await until(
        () =>
          results.length === 2 &&
          !results[1].pending &&
          h.seen.acks.some(({ type }) => type === 'reject'),
        'the new leader renewal and terminal settlement'
      )
      expect(results[1]).toEqual({ pending: false, value: 15000 })
      const retiredBeforeAnswer = !results[0].pending
      old.release(ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 7777 }))
      await until(() => !results[0].pending, 'the old renewal continuation')
      await flush()
      expect(retiredBeforeAnswer).toBe(true)
      expect(results[0].value).toMatchObject({
        name: 'KafkaJSShareAcknowledgementError',
        reason: 'session-lost',
        ranges: [{ firstOffset: '0', lastOffset: '0' }],
      })
      expect(outcomes()).toEqual([
        ['renew', '0', '0', 'session-lost'],
        ['release', '0', '0', 'session-lost'],
        ['renew', '0', '0', null],
        ['reject', '0', '0', null],
      ])
      expect(h.consumer.acquisitionLockTimeoutMs()).toBe(15000)
      expect(outcomes()).toHaveLength(4)
      expect(
        h
          .frames(79)
          .filter(({ fields }) => fields.isRenewAck)
          .map(({ node }) => node)
      ).toEqual([0, 1])
      expect(h.seen.crashes).toEqual([])
    })

    test('settles a renewal lost once at the disconnect deadline and ignores its late answer', async () => {
      const { held, results } = await startRenewing([oneRecord()])
      const closing = track(h.consumer.disconnect({ timeout: 400 }))
      await advance(299)
      await flush()
      expect(results).toEqual([])
      expect(closing.settled).toBe(false)
      await advance(1)
      await until(() => closing.settled, 'the close')
      expect(closing.error).toBeNull()
      expect(results).toHaveLength(1)
      expect(results[0]).toMatchObject({
        name: 'KafkaJSShareAcknowledgementError',
        reason: 'membership-lost',
        ranges: [{ firstOffset: '0', lastOffset: '0' }],
      })
      const lost = [
        ['renew', '0', '0', 'membership-lost'],
        ['release', '0', '0', 'membership-lost'],
      ]
      expect(outcomes()).toHaveLength(2)
      expect(outcomes()).toEqual(expect.arrayContaining(lost))
      held.release()
      await flush()
      expect(outcomes()).toHaveLength(2)
      expect(h.frames(79)).toHaveLength(1)
    })
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
      const aGate = heldGate()
      const bGates = Array.from({ length: 6 }, () => heldGate())
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
      const gate = heldGate()
      const original = ShareRunner.prototype.superviseCrash
      jest.spyOn(ShareRunner.prototype, 'superviseCrash').mockImplementation(function (...args) {
        const pending = original.apply(this, args)
        continuations.push(pending)
        return pending
      })
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

    test('ignores an old restart verdict released after a disconnect', async () => {
      const { restartOnFailure, gate } = await crashWithHeldVerdict()
      await h.consumer.disconnect({ timeout: 0 })
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

  test('discards initial node discovery released after a replacement run is serving records', async () => {
    await start()
    const gate = heldGate()
    const arrived = deferred()
    const done = deferred()
    let taken = false
    const findBroker = Cluster.prototype.findBroker
    let cluster
    h.consumer.on(h.consumer.events.GROUP_JOIN, () => {
      if (taken) return
      taken = true
      let held = false
      jest.spyOn(Cluster.prototype, 'findBroker').mockImplementation(async function (options) {
        if (held) return findBroker.call(this, options)
        held = true
        cluster = this
        continuations.push(done.promise)
        try {
          const broker = await findBroker.call(this, options)
          arrived.resolve()
          await gate.promise
          return broker
        } finally {
          done.resolve()
        }
      })
    })
    await run({ eachMessage: async () => {} })
    await arrived.promise
    expect(h.frames(78)).toHaveLength(0)
    await h.consumer.disconnect({ timeout: 0 })
    openWith([oneRecord()])
    const handled = []
    await run({ eachMessage: async ({ message }) => handled.push(message.offset) })
    await until(() => h.seen.acks.length === 1, 'the replacement delivery and settlement')
    const replacementId = joins()[1].fields.memberId
    const [replacement] = h.frames(78)
    expect(handled).toEqual(['0'])
    expect(outcomes()).toEqual([['accept', '0', '0', null]])
    expect(h.frames(78).map(({ fields }) => fields.memberId)).toEqual([
      replacementId,
      replacementId,
    ])
    const socket = h.cluster.sockets()[replacement.generation - 1]
    expect(socket.destroyed).toBe(false)
    expect(cluster.metadataCache.routingListeners.size).toBe(1)
    const timers = jest.getTimerCount()
    gate.resolve()
    await done.promise
    await flush()
    expect(jest.getTimerCount()).toBe(timers)
    expect(cluster.metadataCache.routingListeners.size).toBe(1)
    expect(socket.destroyed).toBe(false)
    expect(handled).toEqual(['0'])
    expect(h.frames(78)).toHaveLength(2)
    expect(h.consumer.assignment()).toEqual([{ topic: 'topic-a', partitions: [0] }])
    await beat(50, 3)
    await until(() => h.frames(78).length === 3, 'a current fetch after the old discovery finished')
    expect(h.frames(78)[2].fields).toMatchObject({ memberId: replacementId, shareSessionEpoch: 2 })
    expect(h.frames(78)[2].generation).toBe(replacement.generation)
    expect(joins()).toHaveLength(2)
    expect(h.seen.crashes).toEqual([])
  })

  describe('held metadata flights', () => {
    /**
     * Holds every by-id metadata flight inside the cluster helper until the test releases it.
     * @returns {Array<{ids: string[], gate: object, done: object}>} The flights, oldest first;
     *   each `done` resolves after the released provider continuation settles.
     */
    const holdFlights = () => {
      const flights = []
      const original = Cluster.prototype.refreshMetadataForTopicIds
      jest
        .spyOn(Cluster.prototype, 'refreshMetadataForTopicIds')
        .mockImplementation(function (ids, scope) {
          const gate = heldGate()
          const done = deferred()
          continuations.push(done.promise)
          flights.push({ ids, gate, done })
          return (async () => {
            try {
              await gate.promise
              return await original.call(this, ids, scope)
            } finally {
              done.resolve()
            }
          })()
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

    test('discards the held flight of a stopped run once a replacement run is current', async () => {
      const flights = holdFlights()
      await start()
      h.script('ShareGroupHeartbeat', [target(U1)])
      await run({ eachMessage: async () => {} })
      await until(() => flights.length === 1, 'the held flight')
      await h.consumer.stop()
      await run({ eachMessage: async () => {} })
      expect(flights).toHaveLength(1)
      expect(h.seen.assigned).toHaveLength(1)
      expect(h.seen.assigned[0].added).toEqual([{ topic: 'topic-a', partitions: [0] }])
      nameUnknownId()
      flights[0].gate.resolve()
      await flush()
      expect(wireFlights()).toEqual([])
      expect(h.seen.assigned).toHaveLength(1)
      expect(h.consumer.assignment()).toEqual([{ topic: 'topic-a', partitions: [0] }])
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
