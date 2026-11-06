const { ShareCloseEpisode } = require('../shareCloseEpisode')

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
    flush,
    shareClient,
    until,
  } = require('../../../../testHelpers/shareHarness')
  let h
  afterEach(async () => {
    await h?.close()
    h = undefined
  })
  /** @param {object} [options] `shareClient` options. @returns {Promise<object>} A subscribed client. */
  const start = async (options) => {
    h = shareClient(options)
    h.order = []
    const { events, on } = h.consumer
    for (const name of ['STOP', 'DISCONNECT', 'CRASH'])
      on(events[name], () => h.order.push(name.toLowerCase()))
    await h.consumer.subscribe({ topics: ['topic-a'] })
    return h
  }
  /** @returns {object[]} The leave frames (ShareGroupHeartbeat at epoch -1). */
  const leaves = () => h.frames(76).filter(({ fields }) => fields.memberEpoch === -1)
  /** @returns {object[]} The final frames (ShareAcknowledge at epoch -1). */
  const finals = () => h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1)

  test('finishes handlers before captured finals and leave', async () => {
    await start()
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
    const gate = deferred()
    let started = false
    await h.consumer.run({
      eachMessage: async () => {
        started = true
        await gate.promise
      },
    })
    await until(() => started, 'the running handler')
    const stopping = h.consumer.stop()
    const fetches = h.frames(78).length
    await flush()
    expect(h.frames(78)).toHaveLength(fetches)
    expect(finals()).toHaveLength(0)
    expect(leaves()).toHaveLength(0)
    gate.resolve()
    await stopping
    expect(finals()).toHaveLength(1)
    expect(finals()[0].fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
      { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] },
    ])
    expect(leaves()).toHaveLength(1)
    expect(finals()[0].order).toBeLessThan(leaves()[0].order)
    expect(h.order).toEqual(['stop'])
    expect(h.frames(78)).toHaveLength(fetches)
  })

  test('skips finals for unopened sessions and still leaves', async () => {
    await start()
    const gate = h.hold({ api: 'ShareFetch' })
    await h.consumer.run({ eachMessage: async () => {} })
    await gate.arrived()
    await h.consumer.disconnect({ timeout: 400 })
    gate.release()
    expect(h.frames(79)).toHaveLength(0)
    expect(leaves()).toHaveLength(1)
  })

  test('treats timeout zero as a synchronous traffic fence', async () => {
    await start()
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
    await h.consumer.run({ eachMessage: async () => {} })
    await until(() => h.frames(78).length >= 2, 'an established session')
    await h.consumer.disconnect({ timeout: 0 })
    expect(finals()).toHaveLength(0)
    expect(leaves()).toHaveLength(0)
    expect(h.consumer.assignment()).toEqual([])
  })

  test('abandons a held handler at the reserved boundary', async () => {
    await start()
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
    const saved = []
    await h.consumer.run({
      eachMessage: async ({ acknowledge, isRunning }) => {
        saved.push({ acknowledge, isRunning })
        await new Promise(() => {})
      },
    })
    await until(() => saved.length === 1, 'the held handler')
    const began = performance.now()
    await h.consumer.disconnect({ timeout: 400 })
    const elapsed = performance.now() - began
    expect(elapsed).toBeGreaterThanOrEqual(299)
    expect(elapsed).toBeLessThan(3000)
    expect(() => saved[0].acknowledge('reject')).toThrow('no longer current')
    expect(saved[0].isRunning()).toBe(false)
    expect(h.order).toEqual(['stop', 'disconnect'])
  })

  test('keeps invalid deadline calls from stopping the current handler', async () => {
    await start()
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
    const gate = deferred()
    const checks = []
    await h.consumer.run({
      eachMessage: async ({ acknowledge, isRunning }) => {
        for (const timeout of [-1, 1.5, NaN, '5', null, 2147483648]) {
          await h.consumer.disconnect({ timeout }).catch((error) => checks.push(error.name))
        }
        acknowledge('reject')
        checks.push(isRunning())
        await gate.promise
      },
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
  })
})
