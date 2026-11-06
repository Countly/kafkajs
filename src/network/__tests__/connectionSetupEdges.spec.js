const { ConnectionSetupPolicy: Policy, SetupAttempt: Attempt } = require('../connectionSetup')
const { KafkaJSConnectionError } = require('../../errors')

const policy = Policy.resolve({}, () => 0.5)

describe('connection setup edges', () => {
  afterEach(() => jest.useRealTimers())

  test('resolving with no options at all gives the documented defaults and a frozen policy', () => {
    const resolved = Policy.resolve()

    expect({
      timeoutBase: resolved.timeoutBase,
      timeoutCeiling: resolved.timeoutCeiling,
      waitBase: resolved.waitBase,
      waitCeiling: resolved.waitCeiling,
      authenticationTimeout: resolved.authenticationTimeout,
    }).toEqual({
      timeoutBase: 10000,
      timeoutCeiling: 30000,
      waitBase: 50,
      waitCeiling: 1000,
      authenticationTimeout: 10000,
    })
    expect(resolved.random).toBe(Math.random)
    expect(Object.isFrozen(resolved)).toBe(true)
  })

  test('wait returns at once, without a timer, when no backoff remains', async () => {
    jest.useFakeTimers()
    const attempt = new Attempt({ address: 'a', policy, now: () => 100 })

    await expect(attempt.wait()).resolves.toBeUndefined()

    expect(jest.getTimerCount()).toBe(0)
  })

  test('arming twice keeps the first budget and its expiry callback', () => {
    jest.useFakeTimers()
    let time = 0
    const attempt = new Attempt({ address: 'a', policy, timeoutMs: 1000, now: () => time })
    const first = jest.fn()
    const second = jest.fn()
    attempt.arm(first)
    attempt.arm(second)

    time = 1000
    const refusal = attempt.refusal()

    expect(refusal).toBeInstanceOf(KafkaJSConnectionError)
    expect(first).toHaveBeenCalledTimes(1)
    expect(first).toHaveBeenCalledWith(refusal)
    expect(second).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(0)
  })

  test('guard keeps the current phase unless the caller names one', async () => {
    const attempt = new Attempt({ address: 'a', policy })

    await expect(attempt.guard(Promise.resolve('first'))).resolves.toBe('first')
    expect(attempt.phase).toBe('TCP/TLS')
    await expect(attempt.guard(Promise.resolve('second'), { phase: 'SASL' })).resolves.toBe(
      'second'
    )
    expect(attempt.phase).toBe('SASL')
  })

  test('an attempt that became ready refuses further work with a generic setup-ended error', () => {
    const attempt = new Attempt({ address: 'a', policy })
    attempt.ready()

    const refusal = attempt.refusal()

    expect(refusal).toBeInstanceOf(KafkaJSConnectionError)
    expect(refusal.message).toContain('Connection setup ended')
  })
})
