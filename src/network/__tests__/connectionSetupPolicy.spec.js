const {
  ConnectionSetupPolicy: Policy,
  NodeSetupStates: States,
  SetupAttempt: Attempt,
} = require('../connectionSetup')
const {
  KafkaJSConnectionError,
  KafkaJSSASLAuthenticationError,
  KafkaJSRequestTimeoutError,
} = require('../../errors')

it.each([
  [0, [8000, 16000, 24000, 24000]],
  [0.5, [10000, 20000, 30000, 30000]],
  [1, [12000, 24000, 30000, 30000]],
])('samples jitter from the first attempt at random %p', (random, expected) => {
  const policy = Policy.resolve({}, () => random)
  expect([0, 1, 2, 3].map((n) => policy.timeoutFor(n))).toEqual(expected)
})

it.each([
  [{}, 10000, 30000, 50, 1000],
  [{ connectionTimeout: 1000 }, 1000, 1000, 50, 1000],
  [{ connectionTimeout: 3000 }, 3000, 3000, 50, 1000],
  [{ connectionTimeoutMax: 500 }, 500, 500, 50, 1000],
  [{ connectionTimeoutMax: 20000 }, 10000, 20000, 50, 1000],
  [{ connectionTimeout: 300, connectionTimeoutMax: 600 }, 300, 600, 50, 1000],
  [{ reconnectBackoff: 50 }, 10000, 30000, 50, 50],
  [{ reconnectBackoffMax: 20 }, 10000, 30000, 20, 20],
  [{ reconnectBackoff: 0 }, 10000, 30000, 0, 0],
  [{ reconnectBackoff: 0, reconnectBackoffMax: 1000 }, 10000, 30000, 0, 1000],
])(
  'resolves the explicit and omitted option combinations %p',
  (explicit, base, max, wait, waitMax) => {
    const p = Policy.resolve(explicit, () => 0.5)
    expect([p.timeoutFor(0), p.timeoutCeiling, p.waitFor(1), p.waitCeiling]).toEqual([
      base,
      max,
      wait,
      waitMax,
    ])
    expect(p.waitFor(0)).toBe(0)
  }
)

it('keeps backoff counted from failure and one node independent of another', () => {
  const p = Policy.resolve({}, () => 0.5)
  const s = new States(p)
  s.retain(['a', 'b'])
  const first = s.begin('a')
  expect(s.failed('a', 10, first)).toBe(true)
  expect(s.failed('a', 10, first)).toBe(false)
  expect(s.state('a', 40)).toEqual({ failures: 1, retryAt: 60, waitMs: 20 })
  expect(s.state('b', 40).failures).toBe(0)
  const second = s.begin('a')
  s.failed('a', 100, second)
  expect(s.state('a', 100).waitMs).toBe(100)
  s.ready('a')
  expect(s.entries.size).toBe(0)
  expect(s.state('a', 100).failures).toBe(0)
})

it('rejects late old failures after sibling ready and delete/recreate', () => {
  const s = new States(Policy.resolve({}, () => 0.5))
  s.retain(['a'])
  const old = s.begin('a')
  const sibling = s.begin('a')
  s.ready('a', sibling)
  const fresh = s.begin('a')
  expect(fresh.id).not.toBe(old.id)
  expect(s.failed('a', 1, old)).toBe(false)
  s.finish('a', old)
  expect(s.state('a', 1).failures).toBe(0)
  s.ready('a', fresh)
  expect(s.entries.size).toBe(0)
  expect(s.live.size).toBe(0)
})

it('bounds state by current endpoints plus live attempts across address churn', () => {
  const s = new States(Policy.resolve({}, () => 0.5))
  const live = s.begin('old')
  for (let n = 0; n < 1000; n++) {
    const address = String(n)
    s.retain([address])
    const token = s.begin(address)
    s.failed(address, n, token)
    expect(s.entries.size).toBeLessThanOrEqual(2)
  }
  s.retain([])
  expect(s.entries.size).toBe(1)
  s.failed('old', 1001, live)
  expect(s.entries.size).toBe(0)
  const pending = s.begin('a')
  s.clear()
  expect(s.failed('a', 1002, pending)).toBe(false)
  s.finish('a', pending)
  expect(s.entries.size).toBe(0)
})

describe('attempt settlement', () => {
  let states, policy
  beforeEach(() => {
    jest.useFakeTimers()
    policy = Policy.resolve({}, () => 0.5)
    states = new States(policy)
    states.retain(['a'])
  })
  afterEach(() => {
    expect(jest.getTimerCount()).toBe(0)
    jest.useRealTimers()
  })

  it.each([
    [() => new KafkaJSConnectionError('reset'), 1],
    [
      () =>
        new KafkaJSRequestTimeoutError({
          broker: 'a',
          correlationId: 0,
          createdAt: 0,
          sentAt: 0,
          pendingDuration: 0,
        }),
      1,
    ],
    [() => new KafkaJSSASLAuthenticationError('credentials'), 0],
    [() => Object.assign(new Error('credential'), { code: 58 }), 0],
    [() => new Error('application'), 0],
  ])('counts only physical failures %p', (error, count) => {
    const a = new Attempt({ states, address: 'a', policy })
    a.fail(error())
    a.fail(new KafkaJSConnectionError('duplicate'))
    expect(states.state('a', 0).failures).toBe(count)
  })

  it('records expiry once before teardown cancellation', async () => {
    const a = new Attempt({ states, address: 'a', policy })
    const close = jest.fn(() => a.cancel())
    a.arm(close)
    const result = a.guard(new Promise(() => {}), { phase: 'ApiVersions' }).catch((e) => e)
    await jest.advanceTimersByTimeAsync(10000)
    expect(await result).toBeInstanceOf(KafkaJSConnectionError)
    expect(states.state('a', 10000).failures).toBe(1)
    expect(close).toHaveBeenCalledTimes(1)
  })

  it('cancels the wait immediately and retains the prior failure count', async () => {
    states.failed('a', 0, states.begin('a'))
    const a = new Attempt({ states, address: 'a', policy })
    const wait = a.wait().catch((e) => e)
    expect(jest.getTimerCount()).toBe(1)
    a.cancel()
    expect(await wait).toBeInstanceOf(KafkaJSConnectionError)
    expect(states.state('a', 0).failures).toBe(1)
  })

  it('uses the smaller SASL or remaining setup budget with one timer', async () => {
    const a = new Attempt({ states, address: 'a', policy })
    a.arm(() => {})
    await jest.advanceTimersByTimeAsync(9800)
    const result = a.guard(new Promise(() => {}), { phase: 'SASL', cap: 400 }).catch((e) => e)
    expect(jest.getTimerCount()).toBe(1)
    await jest.advanceTimersByTimeAsync(200)
    expect(await result).toBeInstanceOf(KafkaJSConnectionError)
    expect(states.state('a', 10000).failures).toBe(1)
  })

  it('refuses readiness at the deadline even before the timer callback runs', () => {
    let time = 0
    const a = new Attempt({ states, address: 'a', policy, now: () => time })
    const close = jest.fn()
    a.arm(close)
    time = 10000
    expect(() => a.ready()).toThrow(KafkaJSConnectionError)
    expect(close).toHaveBeenCalledTimes(1)
    expect(states.state('a', time).failures).toBe(1)
  })

  it('keeps composite lock budgets above the timer integer limit', async () => {
    const Lock = require('../../utils/lock')
    const lock = new Lock({ timeout: 2147483647 + 1000 })
    await lock.acquire()
    let result
    const waiter = lock.acquire().then(
      () => {
        result = 'acquired'
      },
      (e) => {
        result = e
      }
    )
    await jest.advanceTimersByTimeAsync(2147483647)
    expect(result).toBeUndefined()
    await lock.release()
    await waiter
    expect(result).toBe('acquired')
    await lock.release()
  })

  it('keeps the largest setup budget pending through its last millisecond', async () => {
    const maximum = Policy.resolve({ connectionTimeout: 2147483647, reconnectBackoff: 0 })
    const a = new Attempt({ address: 'a', policy: maximum })
    a.arm(() => {})
    await jest.advanceTimersByTimeAsync(2147483646)
    expect(a.status).toBe('active')
    a.cancel()
  })
})

it('a long lock expires only at its full composite ceiling', async () => {
  jest.useFakeTimers()
  const Lock = require('../../utils/lock')
  const lock = new Lock({ timeout: 2147483647 + 1000 })
  await lock.acquire()
  let outcome
  const waiting = lock.acquire().catch((e) => {
    outcome = e
  })
  await jest.advanceTimersByTimeAsync(2147483647 + 999)
  expect(outcome).toBeUndefined()
  await jest.advanceTimersByTimeAsync(1)
  await waiting
  expect(outcome.name).toBe('KafkaJSLockTimeout')
  await lock.release()
  expect(jest.getTimerCount()).toBe(0)
  jest.useRealTimers()
})

it('a direct connection resolves every setup option before dialing', () => {
  const Connection = require('../connection')
  const { createLogger } = require('../../loggers')
  const connection = new Connection({
    host: 'a',
    port: 1,
    logger: createLogger({ level: 0, logCreator: () => () => {} }),
    connectionTimeout: 100,
    connectionTimeoutMax: 300,
    reconnectBackoff: 0,
    reconnectBackoffMax: 0,
  })
  expect(connection.setupPolicy.timeoutCeiling).toBe(300)
  expect(connection.setupPolicy.waitCeiling).toBe(0)
})
