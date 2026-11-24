const { AsyncLocalStorage } = require('node:async_hooks')
const Cluster = require('../../../cluster')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { ShareRunner } = require('../shareRunner')
const { handlerRunIdentity } = require('../shareRun')
const { ShareFetchLoop } = require('../shareFetchLoop')
const { validateShareConsumerOptions } = require('..')
const { KafkaJSNonRetriableError } = require('../../../errors')
const { newLogger } = require('testHelpers')
const {
  ShareClock,
  acquire,
  deferred,
  fetchAnswer,
  flush,
  shareClient,
  until,
} = require('../../../../testHelpers/shareHarness')

/**
 * Holds every Cluster teardown until the test releases it, so a close stays between the end of
 * its run's episode and its `disconnect` event.
 * @returns {{release: () => void, reached: () => boolean, spy: jest.SpyInstance}} The hold.
 */
const holdTeardown = () => {
  const gate = deferred()
  const original = Cluster.prototype.disconnect
  let reached = false
  const spy = jest.spyOn(Cluster.prototype, 'disconnect').mockImplementation(async function (
    ...args
  ) {
    reached = true
    await gate.promise
    return original.apply(this, args)
  })
  return { release: gate.resolve, reached: () => reached, spy }
}

describe('share client admission while a close is in progress', () => {
  let clients = []
  const teardowns = []
  let clock = null

  /** @param {object} [options] Options of `shareClient`. @returns {object} A tracked client. */
  const open = (options) => {
    const h = shareClient(options)
    clients.push(h)
    return h
  }
  /** @returns {object} A tracked teardown hold. */
  const hold = () => {
    const teardown = holdTeardown()
    teardowns.push(teardown)
    return teardown
  }
  /** @param {object} h A client. @returns {string[]} Connect and disconnect events, in order. */
  const lifecycle = (h) => {
    const seen = []
    h.consumer.on(h.consumer.events.CONNECT, () => seen.push('connect'))
    h.consumer.on(h.consumer.events.DISCONNECT, () => seen.push('disconnect'))
    return seen
  }
  /** @param {object} h A client. @returns {Promise<void>} Resolves after the first join. */
  const run = (h) => h.consumer.run({ eachMessage: async () => {} })
  /** @param {object} h A client. @returns {object[]} Heartbeats that join with epoch zero. */
  const joins = (h) => h.frames(76).filter(({ fields }) => fields.memberEpoch === 0)
  /** @param {object} h A client. @returns {object[]} Heartbeats that leave with epoch -1. */
  const leaves = (h) => h.frames(76).filter(({ fields }) => fields.memberEpoch === -1)

  afterEach(async () => {
    for (const teardown of teardowns.splice(0)) teardown.release()
    jest.restoreAllMocks()
    clock?.restore()
    clock = null
    const closing = clients
    clients = []
    for (const h of closing) await h.close()
  })

  test('connect during close waits for DISCONNECT then emits a fresh CONNECT', async () => {
    const h = open()
    const seen = lifecycle(h)
    await h.consumer.connect()
    const teardown = hold()
    const closing = h.consumer.disconnect()
    await until(teardown.reached, 'the held teardown')
    const reconnecting = h.consumer.connect().then(() => seen.push('connect() resolved'))
    await flush()
    expect(seen).toEqual(['connect'])
    teardown.release()
    await closing
    await reconnecting
    expect(seen).toEqual(['connect', 'disconnect', 'connect', 'connect() resolved'])
    expect(teardown.spy).toHaveBeenCalledTimes(1)
  })

  test('run during close rejects and sends no heartbeat', async () => {
    clock = new ShareClock().install()
    const h = open()
    await h.consumer.subscribe({ topics: ['topic-a'] })
    await run(h)
    expect(joins(h)).toHaveLength(1)
    const teardown = hold()
    const closing = h.consumer.disconnect()
    await until(teardown.reached, 'the held teardown')
    expect(leaves(h)).toHaveLength(1)
    const beats = h.frames(76).length
    const refused = run(h).catch((error) => error)
    await flush()
    expect(h.frames(76)).toHaveLength(beats)
    teardown.release()
    await closing
    const error = await refused
    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error.message).toBe('run() cannot start while another run, close or subscribe is active')
    await clock.advance(1000)
    expect(h.frames(76)).toHaveLength(beats)
    expect(joins(h)).toHaveLength(1)
    expect(h.consumer.assignment()).toEqual([])
  })

  test('subscribe during close rejects', async () => {
    const h = open()
    await h.consumer.connect()
    const teardown = hold()
    const closing = h.consumer.disconnect()
    await until(teardown.reached, 'the held teardown')
    const refused = h.consumer.subscribe({ topics: ['topic-a'] }).catch((error) => error)
    await flush()
    teardown.release()
    await closing
    const error = await refused
    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error.message).toBe(
      'subscribe() cannot replace the subscription while a run, a close or another subscribe is active'
    )
    expect(h.consumer.subscription()).toEqual([])
    await h.consumer.subscribe({ topics: ['topic-a'] })
    expect(h.consumer.subscription()).toEqual(['topic-a'])
  })

  test('disconnect during a pending connect leaves no connection', async () => {
    const h = open()
    const seen = lifecycle(h)
    const gate = h.hold({ api: 'ApiVersions' })
    const pending = h.consumer.connect().catch((error) => error)
    await gate.arrived()
    const opened = h.cluster.sockets()
    expect(opened.length).toBeGreaterThan(0)
    const teardown = hold()
    const closing = h.consumer.disconnect().then(() => seen.push('closed'))
    const later = h.consumer.connect().then(() => seen.push('later connect() resolved'))
    gate.release()
    const error = await pending
    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error.message).toBe('Share client was disconnected')
    await until(teardown.reached, 'the held teardown')
    await flush()
    expect(seen).toEqual([])
    teardown.release()
    await closing
    await later
    expect(opened.every((socket) => socket.destroyed)).toBe(true)
    expect(seen).toEqual(['closed', 'connect', 'later connect() resolved'])
  })

  test('repeated disconnect calls share one close', async () => {
    const h = open()
    const seen = lifecycle(h)
    await h.consumer.subscribe({ topics: ['topic-a'] })
    await run(h)
    const teardown = hold()
    const settled = []
    const calls = [h.consumer.disconnect(), h.consumer.disconnect(), h.consumer.disconnect()].map(
      (call, index) => call.then(() => settled.push(index))
    )
    await until(teardown.reached, 'the held teardown')
    await flush()
    expect(settled).toEqual([])
    teardown.release()
    await Promise.all(calls)
    expect(settled.sort()).toEqual([0, 1, 2])
    expect(teardown.spy).toHaveBeenCalledTimes(1)
    expect(leaves(h)).toHaveLength(1)
    expect(seen).toEqual(['connect', 'disconnect'])
  })

  test('a disconnect from a listener inside a zero-timeout disconnect joins its close and tears the client down once', async () => {
    const h = open()
    const seen = lifecycle(h)
    const entered = deferred()
    const proceed = deferred()
    const outcomes = []
    const settled = []
    /**
     * @param {string} name Recorded once the call settles either way.
     * @param {Promise<void>} call A public disconnect.
     * @returns {Promise<string|Error>} `'resolved'` or the rejection.
     */
    const track = (name, call) =>
      call.then(
        () => {
          settled.push(name)
          return 'resolved'
        },
        (failure) => {
          settled.push(name)
          return failure
        }
      )
    let nested = null
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
    h.consumer.on(h.consumer.events.ACKNOWLEDGE, ({ payload }) => {
      const { firstOffset, lastOffset, type, error } = payload
      outcomes.push([firstOffset, lastOffset, type, error ? error.reason : null])
      nested ??= track('nested', h.consumer.disconnect({ timeout: 0 }))
    })
    await h.consumer.subscribe({ topics: ['topic-a'] })
    await h.consumer.run({
      eachMessage: async () => {
        entered.resolve()
        await proceed.promise
      },
    })
    try {
      await entered.promise
      const teardown = hold()
      const closing = track('closing', h.consumer.disconnect({ timeout: 0 }))
      await until(() => nested !== null, 'the listener disconnect')
      await until(teardown.reached, 'the held teardown')
      await flush()
      const waiter = h.consumer.connect().then(() => seen.push('connect() resolved'))
      await flush()
      expect(settled).toEqual([])
      expect(seen).toEqual(['connect'])
      expect(teardown.spy).toHaveBeenCalledTimes(1)
      teardown.release()
      expect(await closing).toBe('resolved')
      expect(await nested).toBe('resolved')
      expect(settled.sort()).toEqual(['closing', 'nested'])
      await waiter
      proceed.resolve()
      await flush()
      expect(teardown.spy).toHaveBeenCalledTimes(1)
      expect(seen).toEqual(['connect', 'disconnect', 'connect', 'connect() resolved'])
      expect(outcomes).toEqual([['0', '0', 'release', 'membership-lost']])
      expect(h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1)).toEqual([])
      expect(leaves(h)).toEqual([])
    } finally {
      proceed.resolve()
    }
  })

  test('a stopped waiter cannot restart membership', async () => {
    clock = new ShareClock().install()
    const h = open()
    const seen = lifecycle(h)
    await h.consumer.subscribe({ topics: ['topic-a'] })
    await run(h)
    const teardown = hold()
    const closing = h.consumer.disconnect()
    await until(teardown.reached, 'the held teardown')
    const waiter = h.consumer.connect().then(
      () => 'resolved',
      (error) => error
    )
    const joined = h.consumer.disconnect()
    teardown.release()
    await closing
    await joined
    const error = await waiter
    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error.message).toBe('Share client was disconnected')
    const beats = h.frames(76).length
    await clock.advance(1000)
    expect(h.frames(76)).toHaveLength(beats)
    expect(joins(h)).toHaveLength(1)
    expect(h.consumer.assignment()).toEqual([])
    expect(seen).toEqual(['connect', 'disconnect'])
    expect(teardown.spy).toHaveBeenCalledTimes(1)
  })

  test('telemetry resumes only on the new connection', async () => {
    const order = []
    const teardown = deferred()
    const telemetry = {
      resume: jest.fn(() => order.push('resume')),
      suspend: jest.fn(),
      close: jest.fn(() => {
        order.push('telemetry close')
        return null
      }),
      methods: () => ({}),
      setResourceReader: jest.fn(),
    }
    const runner = new ShareRunner({
      ...validateShareConsumerOptions({ groupId: 'g' }),
      logger: newLogger(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
      requestTimeout: 5000,
      cluster: {
        connect: jest.fn(async () => {}),
        findConnectedBroker: jest.fn(async () => ({})),
        disconnect: jest.fn(async () => {
          order.push('teardown')
          await teardown.promise
        }),
        onRoutingChange: jest.fn(() => jest.fn()),
      },
      telemetry,
    })
    runner.checkCapability = jest.fn()
    const consumer = runner.api()
    consumer.on(consumer.events.CONNECT, () => order.push('connect'))
    consumer.on(consumer.events.DISCONNECT, () => order.push('disconnect'))
    await consumer.connect()
    const closing = consumer.disconnect()
    const reconnecting = consumer.connect()
    await flush()
    expect(order).toEqual(['resume', 'connect', 'telemetry close', 'teardown'])
    teardown.resolve()
    await closing
    await reconnecting
    expect(order).toEqual([
      'resume',
      'connect',
      'telemetry close',
      'teardown',
      'disconnect',
      'resume',
      'connect',
    ])
    await consumer.disconnect()
  })

  test.each(['eachMessage', 'eachBatch'])(
    'an %s handler awaiting connect during a natural close is refused and the close completes',
    async (kind) => {
      const h = open()
      const seen = lifecycle(h)
      const entered = deferred()
      const proceed = deferred()
      let refusal = null
      h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
      await h.consumer.subscribe({ topics: ['topic-a'] })
      await h.consumer.run({
        [kind]: async () => {
          entered.resolve()
          await proceed.promise
          refusal = await h.consumer.connect().then(
            () => 'resolved',
            (error) => error
          )
        },
      })
      await entered.promise
      let closed = false
      const closing = h.consumer.disconnect().then(() => {
        closed = true
      })
      proceed.resolve()
      await until(() => closed, 'the natural close')
      await closing
      expect(refusal).toBeInstanceOf(KafkaJSNonRetriableError)
      expect(refusal.message).toBe(
        'connect() cannot wait for a close from a handler of the run being closed'
      )
      expect(leaves(h)).toHaveLength(1)
      expect(seen).toEqual(['connect', 'disconnect'])
      expect(h.cluster.sockets().every((socket) => socket.destroyed)).toBe(true)
    }
  )

  test('a handler awaiting connect during a bounded close is refused before its deadline', async () => {
    clock = new ShareClock().install()
    const h = open()
    const entered = deferred()
    const proceed = deferred()
    let refusal = null
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
    await h.consumer.subscribe({ topics: ['topic-a'] })
    await h.consumer.run({
      eachMessage: async () => {
        entered.resolve()
        await proceed.promise
        refusal = await h.consumer.connect().then(
          () => 'resolved',
          (error) => error
        )
      },
    })
    await entered.promise
    let closed = false
    const closing = h.consumer.disconnect({ timeout: 5000 }).then(() => {
      closed = true
    })
    proceed.resolve()
    await until(() => closed, 'the bounded close without advancing its deadline')
    await closing
    expect(refusal).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(refusal.message).toBe(
      'connect() cannot wait for a close from a handler of the run being closed'
    )
    expect(leaves(h)).toHaveLength(1)
  })

  test.each([
    ['a natural', undefined],
    ['a timeout zero', { timeout: 0 }],
  ])(
    '%s disconnect after the close cancels a connect queued on it and leaves no socket',
    async (_, options) => {
      const h = open()
      const seen = lifecycle(h)
      await h.consumer.connect()
      const first = h.consumer.disconnect()
      const waiter = h.consumer.connect().then(
        () => 'resolved',
        (error) => error
      )
      const second = first.then(() => {
        seen.push('second disconnect called')
        return h.consumer.disconnect(options)
      })
      const [error] = await Promise.all([waiter, second])
      await flush()
      expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
      expect(error.message).toBe('Share client was disconnected')
      expect(seen.filter((event) => event === 'connect')).toHaveLength(1)
      expect(h.cluster.sockets().every((socket) => socket.destroyed)).toBe(true)
    }
  )

  test('connects queued on one close share one fresh attempt and one CONNECT', async () => {
    const h = open()
    const seen = lifecycle(h)
    await h.consumer.connect()
    const teardown = hold()
    const closing = h.consumer.disconnect()
    await until(teardown.reached, 'the held teardown')
    const sockets = h.cluster.sockets().length
    const waiters = [h.consumer.connect(), h.consumer.connect(), h.consumer.connect()]
    teardown.release()
    await closing
    await Promise.all(waiters)
    expect(seen).toEqual(['connect', 'disconnect', 'connect'])
    expect(h.cluster.sockets().filter((socket) => !socket.destroyed)).toHaveLength(1)
    expect(h.cluster.sockets()).toHaveLength(sockets + 1)
  })

  test('an invalid disconnect call does not cancel a queued connect', async () => {
    const h = open()
    const seen = lifecycle(h)
    await h.consumer.connect()
    const teardown = hold()
    const closing = h.consumer.disconnect()
    await until(teardown.reached, 'the held teardown')
    const waiter = h.consumer.connect()
    await expect(h.consumer.disconnect({ timeout: -1 })).rejects.toThrow()
    teardown.release()
    await closing
    await waiter
    expect(seen).toEqual(['connect', 'disconnect', 'connect'])
  })

  test('an eachMessage batch enters the handler context once for all of its messages', async () => {
    const entries = jest.spyOn(AsyncLocalStorage.prototype, 'run').mockName('AsyncLocalStorage#run')
    const h = open()
    const handled = []
    const done = deferred()
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a', 'b', 'c', 'd'])])])
    await h.consumer.subscribe({ topics: ['topic-a'] })
    await h.consumer.run({
      eachMessage: async ({ message }) => {
        handled.push({ offset: message.offset, identity: handlerRunIdentity() })
        if (handled.length === 4) done.resolve()
      },
    })
    await done.promise
    const runIdentities = entries.mock.calls
      .map(([store]) => store)
      .filter((store) => typeof store === 'symbol' && store.description === 'share run')
    expect(handled.map(({ offset }) => offset)).toEqual(['0', '1', '2', '3'])
    expect(runIdentities).toHaveLength(1)
    expect(handled.every(({ identity }) => identity === runIdentities[0])).toBe(true)
  })

  test('a HANDLER_ERROR listener calling connect during a close waits for DISCONNECT and reconnects', async () => {
    const h = open()
    const seen = lifecycle(h)
    const entered = deferred()
    const proceed = deferred()
    let reconnect = null
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
    h.consumer.on(h.consumer.events.HANDLER_ERROR, () => {
      seen.push('handler error')
      reconnect = h.consumer.connect().then(
        () => 'resolved',
        (error) => error
      )
    })
    await h.consumer.subscribe({ topics: ['topic-a'] })
    await h.consumer.run({
      eachMessage: async () => {
        entered.resolve()
        await proceed.promise
        throw new Error('handler failure')
      },
    })
    await entered.promise
    const closing = h.consumer.disconnect()
    proceed.resolve()
    await closing
    await until(() => reconnect !== null, 'the HANDLER_ERROR listener')
    expect(await reconnect).toBe('resolved')
    expect(seen).toEqual(['connect', 'handler error', 'disconnect', 'connect'])
  })

  /** @returns {Array<{kind: string, identity: *}>} Each acknowledgement start and its context. */
  const recordAcknowledgements = () => {
    const original = ShareFetchLoop.prototype.acknowledge
    const started = []
    jest.spyOn(ShareFetchLoop.prototype, 'acknowledge').mockImplementation(function (request) {
      started.push({ kind: request.renew ? 'renew' : 'flush', identity: handlerRunIdentity() })
      return original.call(this, request)
    })
    return started
  }

  test('a renewal and a release flush started by a handler run outside the handler context', async () => {
    const started = recordAcknowledgements()
    const h = open()
    const failed = deferred()
    let inside = null
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
    h.consumer.on(h.consumer.events.HANDLER_ERROR, () => failed.resolve())
    await h.consumer.subscribe({ topics: ['topic-a'] })
    await h.consumer.run({
      eachMessage: async ({ renew }) => {
        inside = handlerRunIdentity()
        renew().catch(() => {})
        throw new Error('handler failure')
      },
    })
    await failed.promise
    await flush()
    expect(typeof inside).toBe('symbol')
    expect(started).toEqual([
      { kind: 'renew', identity: undefined },
      { kind: 'flush', identity: undefined },
    ])
  })

  test('a boundary flush between handlers runs outside the handler context', async () => {
    clock = new ShareClock().install()
    const started = recordAcknowledgements()
    const h = open()
    const entered = deferred()
    const proceed = deferred()
    const done = deferred()
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a', 'b'])])])
    await h.consumer.subscribe({ topics: ['topic-a'] })
    await h.consumer.run({
      eachMessage: async ({ message }) => {
        if (message.offset === '1') return done.resolve()
        entered.resolve()
        await proceed.promise
      },
    })
    await entered.promise
    expect(started).toEqual([])
    await clock.advance(8000)
    proceed.resolve()
    await done.promise
    expect(started).toEqual([{ kind: 'flush', identity: undefined }])
  })

  test('an omission flush in explicit mode runs outside the handler context', async () => {
    const started = recordAcknowledgements()
    const h = open({
      share: { acknowledgementMode: 'explicit', retry: { restartOnFailure: async () => false } },
    })
    const crashed = deferred()
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
    h.consumer.on(h.consumer.events.CRASH, () => crashed.resolve())
    await h.consumer.subscribe({ topics: ['topic-a'] })
    await h.consumer.run({ eachMessage: async () => {} })
    await crashed.promise
    expect(started).toEqual([{ kind: 'flush', identity: undefined }])
  })
})
