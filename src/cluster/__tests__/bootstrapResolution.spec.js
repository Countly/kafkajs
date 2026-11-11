const { Kafka } = require('../../../index')
const connectionPoolBuilder = require('../connectionPoolBuilder')
const BootstrapResolution = require('../bootstrapResolution')
const {
  KafkaJSBootstrapResolutionError,
  KafkaJSInvalidConfigurationError,
  KafkaJSNonRetriableError,
} = require('../../errors')
const { newLogger } = require('testHelpers')
const { createFakeSocketFactory } = require('../../../testHelpers/fakeSocket')

const TIMEOUT = 1000
const RETRY = { initialRetryTime: 100, maxRetryTime: 100, factor: 0, multiplier: 1, retries: 5 }
const ADDRESSES = [{ address: '10.0.0.7', family: 4 }]
const notFound = (host) =>
  Object.assign(new Error(`getaddrinfo ENOTFOUND ${host}`), { code: 'ENOTFOUND' })

/** @returns {Promise<void>} a finite native-promise chain, untouched by fake timers */
const flush = async () => {
  for (let n = 0; n < 12; n++) await Promise.resolve()
}

/**
 * @param {Promise<unknown>} promise a wait to observe without consuming
 * @returns {{ done: boolean, error: Error|null }} its outcome as it happens
 */
const watch = (promise) => {
  const seen = { done: false, error: null }
  promise.then(
    () => (seen.done = true),
    (error) => {
      seen.done = true
      seen.error = error
    }
  )
  return seen
}

/**
 * A scripted `dns.lookup`: every call is recorded and answered as `plan(host, attempt)` says, or
 * held until the test settles it by hand.
 * @param {(host: string, attempt: number) => 'hold'|'fail'|'resolve'} [plan]
 * @returns {{ lookup: Function, calls: object[], hosts: () => string[] }}
 */
const createLookup = (plan = () => 'hold') => {
  const calls = []
  const attempts = new Map()
  const lookup = (host, options, callback) => {
    const attempt = attempts.get(host) ?? 0
    attempts.set(host, attempt + 1)
    calls.push({ host, options, callback, attempt })
    const verdict = plan(host, attempt)
    if (verdict === 'fail') Promise.resolve().then(() => callback(notFound(host)))
    if (verdict === 'resolve') Promise.resolve().then(() => callback(null, ADDRESSES))
  }
  return { lookup, calls, hosts: () => calls.map(({ host }) => host) }
}

/**
 * @param {object} [options]
 * @param {number} [options.timeout]
 * @param {string[]} [options.hosts] the list the gate resolves
 * @param {Function} [options.resolveHosts] replaces the list source
 * @param {Function} [options.plan] see `createLookup`
 * @returns {{ gate: BootstrapResolution, dns: ReturnType<typeof createLookup>, resolveHosts: jest.Mock }}
 */
const createGate = ({ timeout = TIMEOUT, hosts = ['a.test'], resolveHosts, plan } = {}) => {
  const dns = createLookup(plan)
  const source = jest.fn(resolveHosts ?? (async () => hosts))
  const gate = new BootstrapResolution({
    timeout,
    resolveHosts: source,
    lookup: dns.lookup,
    retry: RETRY,
    now: () => Date.now(),
  })
  return { gate, dns, resolveHosts: source }
}

describe('Bootstrap DNS resolution', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => {
    expect(jest.getTimerCount()).toBe(0)
    jest.useRealTimers()
  })

  test('zero performs no lookup and preserves the existing connect path', async () => {
    const provider = jest
      .fn()
      .mockResolvedValueOnce(['first.test:9092'])
      .mockResolvedValueOnce(['second.test:9093'])
    const builder = connectionPoolBuilder({
      brokers: provider,
      clientId: 'zero',
      logger: newLogger(),
      socketFactory: jest.fn(),
    })
    const { gate, dns, resolveHosts } = createGate({ timeout: 0 })

    await gate.wait()
    await gate.wait()

    expect(dns.calls).toHaveLength(0)
    expect(resolveHosts).not.toHaveBeenCalled()
    expect(provider).not.toHaveBeenCalled()
    const first = await builder.build()
    const second = await builder.build()
    expect([first.host, first.port]).toEqual(['first.test', 9092])
    expect([second.host, second.port]).toEqual(['second.test', 9093])
    expect(provider).toHaveBeenCalledTimes(2)
  })

  test('one good seed opens the gate while other seeds fail', async () => {
    const { gate, dns } = createGate({ hosts: ['a.test', 'b.test', 'c.test'] })
    const waiting = watch(gate.wait())
    await flush()

    expect(dns.hosts()).toEqual(['a.test', 'b.test', 'c.test'])
    expect(dns.calls.map(({ options }) => options)).toEqual([
      { all: true },
      { all: true },
      { all: true },
    ])
    dns.calls[0].callback(notFound('a.test'))
    dns.calls[2].callback(notFound('c.test'))
    await flush()
    expect(waiting.done).toBe(false)
    expect(jest.getTimerCount()).toBe(3)

    dns.calls[1].callback(null, ADDRESSES)
    await flush()

    expect(waiting).toEqual({ done: true, error: null })
    expect(jest.getTimerCount()).toBe(0)
  })

  test('all failed seeds reach the deadline and cache one terminal error', async () => {
    const { gate, dns } = createGate({ hosts: ['a.test', 'b.test'], plan: () => 'fail' })
    const waiting = watch(gate.wait())

    await jest.advanceTimersByTimeAsync(TIMEOUT)

    expect(waiting.error).toBeInstanceOf(KafkaJSBootstrapResolutionError)
    expect(waiting.error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(waiting.error.retriable).toBe(false)
    expect(waiting.error.cause.code).toBe('ENOTFOUND')
    expect(waiting.error.message).toContain('a.test')
    const lookups = dns.calls.length
    expect(lookups).toBeGreaterThan(2)

    await expect(gate.wait()).rejects.toBe(waiting.error)
    await expect(gate.wait()).rejects.toBe(waiting.error)
    await jest.advanceTimersByTimeAsync(5 * TIMEOUT)

    expect(dns.calls).toHaveLength(lookups)
  })

  test('concurrent connects share one pending lookup round', async () => {
    const { gate, dns, resolveHosts } = createGate({
      hosts: ['a.test', 'b.test', 'a.test', 'c.test'],
    })
    const first = watch(gate.wait())
    const second = watch(gate.wait())
    const third = watch(gate.wait())
    await flush()

    expect(resolveHosts).toHaveBeenCalledTimes(1)
    expect(dns.hosts()).toEqual(['a.test', 'b.test', 'c.test'])
    expect([first.done, second.done, third.done]).toEqual([false, false, false])

    dns.calls[2].callback(null, ADDRESSES)
    await flush()

    expect([first, second, third]).toEqual([
      { done: true, error: null },
      { done: true, error: null },
      { done: true, error: null },
    ])
    expect(dns.calls).toHaveLength(3)
  })

  test('reconnect after success never checks DNS again', async () => {
    const { gate, dns, resolveHosts } = createGate({ plan: () => 'resolve' })
    await Promise.all([gate.wait(), gate.wait()])
    expect(dns.calls).toHaveLength(1)

    gate.cancel(new Error('disconnect'))
    await gate.wait()
    gate.cancel(new Error('disconnect'))
    await gate.wait()

    expect(dns.calls).toHaveLength(1)
    expect(resolveHosts).toHaveBeenCalledTimes(1)
  })

  test('dynamic brokers use the first returned validated list', async () => {
    const provider = jest
      .fn()
      .mockResolvedValueOnce(['first-a.test:9092', 'first-b.test:9093'])
      .mockResolvedValue(['later.test:9094'])
    const builder = connectionPoolBuilder({
      brokers: provider,
      clientId: 'dynamic',
      logger: newLogger(),
      socketFactory: jest.fn(),
    })
    const dns = createLookup(() => 'resolve')
    const gate = new BootstrapResolution({
      timeout: TIMEOUT,
      resolveHosts: () => builder.bootstrapHosts(),
      lookup: dns.lookup,
      retry: RETRY,
      now: () => Date.now(),
    })

    await gate.wait()

    expect(provider).toHaveBeenCalledTimes(1)
    expect(dns.hosts()).toEqual(['first-a.test', 'first-b.test'])
    const firstDial = await builder.build()
    expect([firstDial.host, firstDial.port]).toEqual(['first-a.test', 9092])
    expect(provider).toHaveBeenCalledTimes(1)
    const laterDial = await builder.build()
    expect([laterDial.host, laterDial.port]).toEqual(['later.test', 9094])
    expect(provider).toHaveBeenCalledTimes(2)
    await gate.wait()
    expect(dns.calls).toHaveLength(2)
  })

  test('disconnect cancels pending work and the next connect checks again', async () => {
    const { gate, dns } = createGate({ hosts: ['a.test', 'b.test'] })
    const closed = new Error('client disconnected')
    const first = watch(gate.wait())
    const second = watch(gate.wait())
    await flush()
    expect(dns.calls).toHaveLength(2)

    gate.cancel(closed)
    await flush()

    expect(first.error).toBe(closed)
    expect(second.error).toBe(closed)
    expect(jest.getTimerCount()).toBe(0)
    await jest.advanceTimersByTimeAsync(5 * TIMEOUT)
    expect(dns.calls).toHaveLength(2)

    const again = watch(gate.wait())
    await flush()
    expect(dns.calls).toHaveLength(4)
    dns.calls[3].callback(null, ADDRESSES)
    await flush()
    expect(again).toEqual({ done: true, error: null })
  })

  test('late lookup callbacks cannot open a cancelled generation', async () => {
    const { gate, dns } = createGate({ hosts: ['a.test'] })
    const cancelled = watch(gate.wait())
    await flush()
    const [old] = dns.calls

    gate.cancel(new Error('client disconnected'))
    const fresh = watch(gate.wait())
    await flush()
    expect(dns.calls).toHaveLength(2)
    expect(cancelled.done).toBe(true)

    old.callback(null, ADDRESSES)
    old.callback(notFound('a.test'))
    await flush()
    expect(fresh.done).toBe(false)
    await jest.advanceTimersByTimeAsync(RETRY.initialRetryTime - 1)
    expect(dns.calls).toHaveLength(2)

    await jest.advanceTimersByTimeAsync(TIMEOUT)
    expect(fresh.error).toBeInstanceOf(KafkaJSBootstrapResolutionError)
    const terminal = fresh.error
    const lookups = dns.calls.length
    old.callback(null, ADDRESSES)
    dns.calls[1].callback(null, ADDRESSES)
    await flush()

    await expect(gate.wait()).rejects.toBe(terminal)
    expect(dns.calls).toHaveLength(lookups)
  })

  test('TLS and custom factories receive the original hostname', async () => {
    const ssl = { rejectUnauthorized: true, servername: 'sni.example.com' }
    const sockets = createFakeSocketFactory()
    const socketFactory = jest.fn((args) => sockets.factory(args))
    const builder = connectionPoolBuilder({
      brokers: ['broker.example.com:9093'],
      ssl,
      socketFactory,
      clientId: 'tls',
      connectionTimeout: 1000,
      logger: newLogger(),
    })
    const dns = createLookup(() => 'resolve')
    const gate = new BootstrapResolution({
      timeout: TIMEOUT,
      resolveHosts: () => builder.bootstrapHosts(),
      lookup: dns.lookup,
      retry: RETRY,
      now: () => Date.now(),
    })

    await gate.wait()
    const pool = await builder.build()
    const connecting = pool.pool[0].connect().catch(() => {})
    await flush()

    expect(dns.hosts()).toEqual(['broker.example.com'])
    expect(pool.host).toBe('broker.example.com')
    expect(pool.ssl).toBe(ssl)
    expect(pool.socketFactory).toBe(socketFactory)
    expect(socketFactory).toHaveBeenCalledTimes(1)
    const [dialled] = socketFactory.mock.calls[0]
    expect({ host: dialled.host, port: dialled.port }).toEqual({
      host: 'broker.example.com',
      port: 9093,
    })
    expect(dialled.ssl).toBe(ssl)
    expect(JSON.stringify(dialled.ssl)).not.toContain('10.0.0.7')

    await pool.destroy({ drainTimeout: 0 })
    await jest.advanceTimersByTimeAsync(1000)
    await connecting
  })

  test('unresolved DNS rounds follow the retry schedule without a microtask loop', async () => {
    const { gate, dns } = createGate({ hosts: ['a.test', 'b.test'], plan: () => 'fail' })
    const waiting = watch(gate.wait())

    await flush()
    expect(dns.calls).toHaveLength(2)
    await jest.advanceTimersByTimeAsync(RETRY.initialRetryTime - 1)
    expect(dns.calls).toHaveLength(2)

    for (let round = 2; round <= 10; round++) {
      await jest.advanceTimersByTimeAsync(round === 2 ? 1 : RETRY.initialRetryTime)
      expect(dns.calls).toHaveLength(2 * round)
    }
    expect(waiting.done).toBe(false)

    await jest.advanceTimersByTimeAsync(TIMEOUT - 9 * RETRY.initialRetryTime)

    expect(waiting.error).toBeInstanceOf(KafkaJSBootstrapResolutionError)
    expect(dns.calls).toHaveLength(2 * 10)
    expect(dns.calls.filter(({ host }) => host === 'a.test').map(({ attempt }) => attempt)).toEqual(
      [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    )
  })

  test('a hanging lookup and a hanging dynamic list cannot outlive the deadline', async () => {
    const hangingLookup = createGate({ hosts: ['a.test'] })
    const lookupWait = watch(hangingLookup.gate.wait())
    await jest.advanceTimersByTimeAsync(TIMEOUT)
    expect(lookupWait.error).toBeInstanceOf(KafkaJSBootstrapResolutionError)
    expect(jest.getTimerCount()).toBe(0)
    hangingLookup.dns.calls[0].callback(null, ADDRESSES)
    await flush()
    await expect(hangingLookup.gate.wait()).rejects.toBe(lookupWait.error)

    let supply
    const hangingList = createGate({
      resolveHosts: () => new Promise((resolve) => (supply = resolve)),
    })
    const listWait = watch(hangingList.gate.wait())
    await jest.advanceTimersByTimeAsync(TIMEOUT)
    expect(listWait.error).toBeInstanceOf(KafkaJSBootstrapResolutionError)
    expect(jest.getTimerCount()).toBe(0)
    supply(['a.test'])
    await flush()
    expect(hangingList.dns.calls).toHaveLength(0)
    await expect(hangingList.gate.wait()).rejects.toBe(listWait.error)
  })

  test('rejects invalid timeout configuration before network work', async () => {
    const resolveHosts = jest.fn()
    const lookup = jest.fn()
    for (const timeout of [-1, 1.5, NaN, Infinity, '1000', null, 2147483648]) {
      expect(
        () => new BootstrapResolution({ timeout, resolveHosts, lookup, retry: RETRY })
      ).toThrow(KafkaJSInvalidConfigurationError)
      expect(
        () => new Kafka({ brokers: ['a.test:9092'], bootstrapResolveTimeout: timeout })
      ).toThrow(KafkaJSInvalidConfigurationError)
    }
    for (const timeout of [0, 1, TIMEOUT, 2147483647]) {
      expect(
        () => new BootstrapResolution({ timeout, resolveHosts, lookup, retry: RETRY })
      ).not.toThrow()
      expect(
        () => new Kafka({ brokers: ['a.test:9092'], bootstrapResolveTimeout: timeout })
      ).not.toThrow()
    }

    expect(resolveHosts).not.toHaveBeenCalled()
    expect(lookup).not.toHaveBeenCalled()
  })

  test('literal IPv4 and bracketed IPv6 seeds preserve the parsed host', async () => {
    const provider = jest.fn().mockResolvedValue(['127.0.0.1:9092', '[::1]:9093'])
    const builder = connectionPoolBuilder({
      brokers: provider,
      clientId: 'literals',
      logger: newLogger(),
      socketFactory: jest.fn(),
    })
    const dns = createLookup(() => 'resolve')
    const gate = new BootstrapResolution({
      timeout: TIMEOUT,
      resolveHosts: () => builder.bootstrapHosts(),
      lookup: dns.lookup,
      retry: RETRY,
      now: () => Date.now(),
    })

    await gate.wait()

    expect(dns.hosts()).toEqual(['127.0.0.1', '::1'])
    const ipv4 = await builder.build()
    expect([ipv4.host, ipv4.port]).toEqual(['127.0.0.1', 9092])
    expect(provider).toHaveBeenCalledTimes(1)
    const ipv6 = await builder.build()
    expect([ipv6.host, ipv6.port]).toEqual(['::1', 9093])
    expect(provider).toHaveBeenCalledTimes(2)

    const idle = connectionPoolBuilder({
      brokers: provider,
      clientId: 'literals',
      logger: newLogger(),
      socketFactory: jest.fn(),
    })
    provider.mockClear()
    const zero = new BootstrapResolution({
      timeout: 0,
      resolveHosts: () => idle.bootstrapHosts(),
      lookup: dns.lookup,
      retry: RETRY,
    })
    await zero.wait()
    expect(provider).not.toHaveBeenCalled()
    expect(idle.bootstrapEndpoints.size).toBe(0)
  })
})
