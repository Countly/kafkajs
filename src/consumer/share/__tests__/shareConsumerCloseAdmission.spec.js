const Cluster = require('../../../cluster')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { ShareRunner } = require('../shareRunner')
const { validateShareConsumerOptions } = require('..')
const { KafkaJSNonRetriableError } = require('../../../errors')
const { newLogger } = require('testHelpers')
const {
  ShareClock,
  deferred,
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
})
