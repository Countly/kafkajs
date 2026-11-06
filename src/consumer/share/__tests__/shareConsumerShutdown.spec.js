const { ShareCloseEpisode } = require('../shareCloseEpisode')
const { ShareRun } = require('../shareRun')
const { KafkaJSNonRetriableError } = require('../../../errors')
const { A, flush } = require('../../../../testHelpers/shareHarness')

const TWO_NODES = {
  'topic-a': {
    topicId: A,
    partitions: [
      { leader: 0, epoch: 1 },
      { leader: 1, epoch: 1 },
    ],
  },
}

/** Fake timers over a zero-based monotonic clock; no timer runs until `advance` delivers it. */
class Clock {
  /** @returns {void} Installs fake timers and a `performance.now` that starts at zero. */
  install() {
    jest.useFakeTimers({
      now: 1000000,
      doNotFake: ['setImmediate', 'queueMicrotask', 'nextTick', 'hrtime', 'performance'],
    })
    const origin = Date.now()
    jest.spyOn(performance, 'now').mockImplementation(() => Date.now() - origin)
  }

  /**
   * @param {number} ms Milliseconds of fake time to deliver, timer by timer.
   * @returns {Promise<void>} Resolves once the scripted sockets answered what the timers started.
   */
  async advance(ms) {
    await jest.advanceTimersByTimeAsync(ms)
    await flush()
  }

  /**
   * @param {() => boolean} predicate The reached-stage condition.
   * @param {string} what What is awaited, named by the failure.
   * @returns {Promise<void>}
   * @throws {Error} When the condition does not hold after 40 steps of 100 fake milliseconds.
   */
  async advanceUntil(predicate, what) {
    for (let turn = 0; turn < 40 && !predicate(); turn++) await this.advance(100)
    if (!predicate()) throw new Error(`${what} did not happen`)
  }
}

/** @param {object} options Case overrides. @returns {object} Episode and controlled actual clock. */
const setup = ({ timeout, requestTimeout = 1000 } = {}) => {
  let now = 0
  let bound = true
  const run = { identity: Symbol('run') }
  const episode = new ShareCloseEpisode({ run, requestTimeout, now: () => now })
  const node = Object.freeze({ nodeId: 1, current: () => bound })
  if (timeout !== undefined) episode.disconnect(timeout)
  return {
    episode,
    node,
    advance: (value) => {
      now = value
    },
    replace: () => {
      bound = false
    },
  }
}

describe('Captured share shutdown authority', () => {
  test('reserves a quarter of a short disconnect deadline', () => {
    const c = setup({ timeout: 400 })
    expect(c.episode.handlerBoundary).toBe(300)
    c.advance(300)
    c.episode.freeze([c.node])
    expect(c.episode.finalDeadline).toBe(400)
  })
  test('caps the reserve at one second', () => {
    const c = setup({ timeout: 5000 })
    expect(c.episode.handlerBoundary).toBe(4000)
    c.advance(4000)
    c.episode.freeze([c.node])
    expect(c.episode.finalDeadline).toBe(5000)
  })
  test('does not reserve a millisecond below four', () => {
    const c = setup({ timeout: 3 })
    expect(c.episode.handlerBoundary).toBe(3)
  })
  test('has one finite final cap shared across all nodes and leave', () => {
    const c = setup()
    c.advance(250)
    c.episode.freeze([c.node])
    expect(c.episode.finalDeadline).toBe(1250)
    c.advance(1249)
    expect(c.episode.claimFinal(1)).toBe(c.node)
    c.advance(1250)
    expect(c.episode.claimLeave(c.node)).toBe(false)
  })
  test('a concurrent disconnect cannot renew the original whole-call deadline', () => {
    const c = setup({ timeout: 400 })
    c.advance(100)
    c.episode.disconnect(5000)
    expect(c.episode.deadlineAt).toBe(400)
    expect(c.episode.handlerBoundary).toBe(300)
  })
  test('natural disconnect remains natural when another call supplies a budget', () => {
    const c = setup()
    c.episode.disconnect()
    c.advance(100)
    c.episode.disconnect(400)
    expect(c.episode.deadlineAt).toBe(Infinity)
  })
  test('zero revokes unsent work synchronously even after another close began', () => {
    const c = setup({ timeout: 400 })
    c.episode.freeze([c.node])
    c.episode.disconnect(0)
    expect(c.episode.claimFinal(1)).toBeNull()
    expect(c.episode.claimLeave(c.node)).toBe(false)
  })
  test('checks actual time even when a deadline timer has not run', () => {
    const c = setup({ timeout: 400 })
    c.episode.freeze([c.node])
    c.advance(400)
    expect(c.episode.refusal(c.node)).toBeInstanceOf(Error)
    expect(c.episode.revoked).toBe(true)
  })
  test('authorizes each captured final and leave at most once', () => {
    const c = setup()
    c.episode.freeze([c.node])
    expect(c.episode.claimFinal(1)).toBe(c.node)
    expect(c.episode.claimFinal(1)).toBeNull()
    expect(c.episode.claimFinal(2)).toBeNull()
    expect(c.episode.claimLeave(c.node)).toBe(true)
    expect(c.episode.claimLeave(c.node)).toBe(false)
  })
  test('never recaptures a replacement session', () => {
    const c = setup()
    c.episode.freeze([c.node])
    c.replace()
    c.episode.freeze([{ nodeId: 2, current: () => true }])
    expect(c.episode.claimFinal(1)).toBeNull()
    expect(c.episode.claimFinal(2)).toBeNull()
  })
  test('releases captured references on completion', () => {
    const c = setup()
    c.episode.freeze([c.node])
    c.episode.finish()
    expect(c.episode.run).toBeNull()
    expect(c.episode.nodes.size).toBe(0)
    expect(c.episode.claimFinal(1)).toBeNull()
  })
  test('counts option parsing time from the original disconnect entry', () => {
    const c = setup()
    const enteredAt = 0
    c.advance(100)
    c.episode.disconnect(400, enteredAt)
    expect(c.episode.deadlineAt).toBe(400)
    expect(c.episode.handlerBoundary).toBe(300)
  })
})

describe('public share shutdown contract', () => {
  const {
    acquire,
    deferred,
    fetchAnswer,
    shareClient,
    until,
  } = require('../../../../testHelpers/shareHarness')
  let h
  const gates = []
  const settling = []
  /** @returns {{promise: Promise<void>, resolve: () => void}} A handler gate the cleanup releases. */
  const hold = () => {
    const gate = deferred()
    gates.push(gate)
    return gate
  }
  /**
   * @param {Function} handler An `eachMessage` handler that may wait on a held gate.
   * @returns {Function} The handler, whose promise the cleanup awaits once its gate is released.
   */
  const tracked = (handler) => (payload) => {
    const handled = handler(payload)
    settling.push(handled)
    return handled
  }
  afterEach(async () => {
    for (const gate of gates.splice(0)) gate.resolve()
    await Promise.allSettled(settling.splice(0))
    jest.restoreAllMocks()
    jest.useRealTimers()
    await h?.close()
    h = undefined
  })
  /** @param {object} [options] `shareClient` options. @returns {Promise<object>} A subscribed client. */
  const start = async (options) => {
    h = shareClient(options)
    h.order = []
    h.acks = []
    const { events, on } = h.consumer
    for (const name of ['STOP', 'DISCONNECT', 'CRASH'])
      on(events[name], () => h.order.push(name.toLowerCase()))
    on(events.ACKNOWLEDGE, ({ payload }) =>
      h.acks.push([
        payload.partition,
        payload.firstOffset,
        payload.lastOffset,
        payload.type,
        payload.error ? payload.error.reason : null,
      ])
    )
    await h.consumer.subscribe({ topics: ['topic-a'] })
    return h
  }
  /** @returns {object[]} The leave frames (ShareGroupHeartbeat at epoch -1). */
  const leaves = () => h.frames(76).filter(({ fields }) => fields.memberEpoch === -1)
  /** @returns {object[]} The final frames (ShareAcknowledge at epoch -1). */
  const finals = () => h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1)
  /**
   * @param {{node: number, generation: number}} record A recorded request.
   * @returns {object} The fake socket the request travelled on.
   */
  const socketOf = ({ node, generation }) =>
    h.cluster.sockets().filter(({ endpoint }) => endpoint.host === `node${node}`)[generation - 1]
  /**
   * @param {() => *} call A handler callback that must refuse.
   * @returns {void}
   */
  const expectStale = (call) => {
    let thrown = null
    try {
      call()
    } catch (error) {
      thrown = error
    }
    expect(thrown).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(thrown.message).toBe('Handler call is no longer current')
  }

  test('finishes every node handler before captured finals and one leave', async () => {
    await start({ nodes: 2, topics: TWO_NODES })
    const answer = (fields, call) => fetchAnswer([acquire(call.node, 0, [`v${call.node}`])])
    h.script('ShareFetch', [answer, answer])
    const held = [hold(), hold()]
    const started = new Set()
    await h.consumer.run({
      eachMessage: tracked(async ({ partition }) => {
        started.add(partition)
        await held[partition].promise
      }),
    })
    await until(() => started.size === 2, 'both handlers')
    const stopping = h.consumer.stop()
    const fetches = h.frames(78).length
    await flush()
    expect(finals()).toHaveLength(0)
    expect(leaves()).toHaveLength(0)
    held[0].resolve()
    await flush()
    expect(finals()).toHaveLength(0)
    expect(leaves()).toHaveLength(0)
    held[1].resolve()
    await stopping
    expect(
      finals()
        .map(({ node }) => node)
        .sort()
    ).toEqual([0, 1])
    for (const final of finals()) {
      expect(final.fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
        { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] },
      ])
    }
    expect(leaves()).toHaveLength(1)
    for (const final of finals()) expect(final.answeredOrder).toBeLessThan(leaves()[0].order)
    expect(h.order).toEqual(['stop'])
    expect(h.frames(78)).toHaveLength(fetches)
    expect(finals().map((final) => socketOf(final).destroyed)).toEqual([false, false])
    await h.consumer.disconnect()
    expect(h.order).toEqual(['stop', 'disconnect'])
    expect(finals().map((final) => socketOf(final).destroyed)).toEqual([true, true])
  })

  test('skips finals for unopened sessions and still leaves', async () => {
    const clock = new Clock()
    clock.install()
    await start()
    const gate = h.hold({ api: 'ShareFetch' })
    await h.consumer.run({ eachMessage: async () => {} })
    await gate.arrived()
    const disconnecting = h.consumer.disconnect({ timeout: 400 })
    await clock.advance(300)
    await until(() => leaves().length === 1, 'the leave with an unopened data session')
    await disconnecting
    gate.release()
    expect(h.frames(79)).toHaveLength(0)
    expect(leaves()).toHaveLength(1)
  })

  test('treats timeout zero as a synchronous traffic fence', async () => {
    await start()
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
    const saved = []
    const gate = hold()
    await h.consumer.run({
      eachMessage: tracked(async ({ acknowledge, renew }) => {
        saved.push({ acknowledge, renew })
        acknowledge()
        await gate.promise
      }),
    })
    await until(() => saved.length === 1, 'the held handler')
    const closing = h.consumer.disconnect({ timeout: 0 })
    expectStale(() => saved[0].acknowledge('reject'))
    expectStale(() => saved[0].renew())
    await closing
    expect(h.frames(79)).toHaveLength(0)
    expect(leaves()).toHaveLength(0)
    expect(h.consumer.assignment()).toEqual([])
    expect(h.acks).toEqual([[0, '0', '0', 'accept', 'not-sent']])
    expect(h.order).toEqual(['stop', 'disconnect'])
  })

  test('keeps invalid deadline calls from stopping the current handler', async () => {
    await start()
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
    const gate = hold()
    const checks = []
    await h.consumer.run({
      eachMessage: tracked(async ({ acknowledge, isRunning }) => {
        for (const timeout of [-1, 1.5, NaN, '5', null, 2147483648]) {
          await h.consumer.disconnect({ timeout }).catch((error) => checks.push(error.name))
        }
        acknowledge('reject')
        checks.push(isRunning())
        await gate.promise
      }),
    })
    await until(() => checks.length === 7, 'the invalid calls')
    expect(checks).toEqual([...Array(6).fill('KafkaJSInvalidConfigurationError'), true])
    gate.resolve()
    await h.consumer.stop()
    expect(finals()[0].fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
      { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [3] },
    ])
  })

  test('starts a fresh run after a clean stop', async () => {
    const runs = captureRuns()
    await start()
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
    const first = []
    await h.consumer.run({
      eachMessage: async ({ acknowledge }) => {
        first.push(acknowledge)
      },
    })
    await until(() => first.length === 1, 'the first delivery')
    await h.consumer.stop()
    expect(h.consumer.subscription()).toEqual(['topic-a'])
    expect(h.consumer.assignment()).toEqual([])
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
    const second = []
    await h.consumer.run({
      eachMessage: async ({ acknowledge }) => {
        second.push(acknowledge)
        acknowledge('reject')
      },
    })
    await until(() => second.length === 1, 'the second delivery')
    expect(() => first[0]('reject')).toThrow('no longer current')
    const joins = h.frames(76).filter(({ fields }) => fields.memberEpoch === 0)
    expect(new Set(joins.map(({ fields }) => fields.memberId)).size).toBe(2)
    expect(h.consumer.assignment()).toEqual([{ topic: 'topic-a', partitions: [0] }])
    expect(runs).toHaveLength(2)
    for (const owned of ['poll', 'manager', 'membership', 'operation'])
      expect(runs[1][owned]).not.toBe(runs[0][owned])
    await h.consumer.stop()
    expect(h.acks.at(-1)).toEqual([0, '0', '0', 'reject', null])
    expect(h.acks).toHaveLength(2)
    expect(h.consumer.assignment()).toEqual([])
  })
  /** @returns {object[]} Every run the client starts, oldest first: state a test cannot read publicly. */
  const captureRuns = () => {
    const runs = []
    const original = ShareRun.prototype.start
    jest.spyOn(ShareRun.prototype, 'start').mockImplementation(function () {
      runs.push(this)
      return original.call(this)
    })
    return runs
  }
  /**
   * Fences the running member, with its handler held and its captured final held at the wire.
   * @param {Clock} [clock] Controlled time; the member runs on real timers without it.
   * @returns {Promise<{final: object, gate: object}>} The held final request and the handler's gate.
   */
  const fenceWithHeldFinal = async (clock) => {
    await start()
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
    let fence = false
    h.script(
      'ShareGroupHeartbeat',
      Array.from({ length: 100 }, () => () => {
        if (!fence) return undefined
        fence = false
        return { errorCode: 110 }
      })
    )
    const final = h.hold({ api: 'ShareAcknowledge' })
    const gate = hold()
    const started = deferred()
    await h.consumer.run({
      eachMessage: tracked(async ({ acknowledge }) => {
        acknowledge()
        started.resolve()
        await gate.promise
      }),
    })
    await started.promise
    fence = true
    if (clock) {
      let arrived = false
      final.arrived().then(() => {
        arrived = true
      })
      await clock.advanceUntil(() => arrived, 'the held final')
    } else await final.arrived()
    return { final, gate }
  }

  test('starts no handler after a disconnect that arrives during an earlier one', async () => {
    await start()
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a', 'b', 'c'])])])
    const started = []
    let closing
    let running
    await h.consumer.run({
      eachMessage: async ({ message, isRunning }) => {
        started.push(message.offset)
        closing = h.consumer.disconnect()
        running = isRunning()
      },
    })
    await until(() => closing !== undefined, 'the disconnect')
    await closing
    expect(started).toEqual(['0'])
    expect(running).toBe(false)
    expect(finals()[0].fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
      { firstOffset: '0', lastOffset: '2', acknowledgeTypes: [1, 2, 2] },
    ])
  })

  test('ends the run when a stop arrives during a fence recovery close', async () => {
    const { final, gate } = await fenceWithHeldFinal()
    let stopped = false
    const stopping = h.consumer.stop().then(() => {
      stopped = true
    })
    await flush()
    expect(stopped).toBe(false)
    final.release()
    await stopping
    expect(h.order).toEqual(['stop'])
    gate.resolve()
    await expect(h.consumer.run({ eachMessage: async () => {} })).resolves.toBeUndefined()
    const joins = h.frames(76).filter(({ fields }) => fields.memberEpoch === 0)
    expect(new Set(joins.map(({ fields }) => fields.memberId)).size).toBe(2)
  })

  test('keeps the heartbeat chain suspended while a fenced run closes and resumes it after', async () => {
    const clock = new Clock()
    clock.install()
    const runs = captureRuns()
    const { final, gate } = await fenceWithHeldFinal(clock)
    const [run] = runs
    const joinFrames = () => h.frames(76).filter(({ fields }) => fields.memberEpoch === 0)
    expect(run.manager.isRunning()).toBe(false)
    expect(joinFrames()).toHaveLength(1)
    const beats = h.frames(76).length
    await clock.advance(4000)
    expect(h.frames(76)).toHaveLength(beats)
    expect(run.manager.isRunning()).toBe(false)
    let rejoined = false
    h.consumer.on(h.consumer.events.GROUP_JOIN, () => {
      rejoined = true
    })
    final.release()
    await clock.advanceUntil(() => rejoined, 'the rejoin')
    expect(run.manager.isRunning()).toBe(true)
    expect(joinFrames()).toHaveLength(2)
    gate.resolve()
  })

  test('freezes handler authority in the tick a crash cuts a waiting stop short', async () => {
    const runs = captureRuns()
    await start()
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
    const gate = hold()
    const started = deferred()
    const saved = {}
    await h.consumer.run({
      eachMessage: tracked(async ({ acknowledge, renew }) => {
        Object.assign(saved, { acknowledge, renew })
        started.resolve()
        await gate.promise
      }),
    })
    await started.promise
    const stopping = h.consumer.stop()
    await flush()
    expect(finals()).toHaveLength(0)
    const crashed = runs[0].crash(new Error('terminal'))
    expect(() => saved.acknowledge('reject')).toThrow('no longer current')
    expect(() => saved.renew()).toThrow('no longer current')
    gate.resolve()
    await Promise.all([crashed, stopping])
    expect(h.frames(79).filter(({ fields }) => fields.isRenewAck)).toHaveLength(0)
  })
})
