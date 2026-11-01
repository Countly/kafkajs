const http = require('node:http')
const { ReadableStream } = require('node:stream/web')
const { setTimeout: realDelay } = require('node:timers/promises')
const { optionsOf, providerOf } = require('../tokenCache')
const { clientCredentials } = require('../clientCredentials')
const { KafkaJSSASLAuthenticationError, KafkaJSInvalidConfigurationError } = require('../../errors')

const MARKER = 'inert-marker'
const segment = (value) => Buffer.from(JSON.stringify(value)).toString('base64url')
const jwt = (exp, claims = {}) => `${segment({ alg: 'none' })}.${segment({ exp, ...claims })}.`
const deferred = () => {
  const result = {}
  result.promise = new Promise((resolve, reject) => Object.assign(result, { resolve, reject }))
  return result
}
const observe = (promise) => {
  const seen = {}
  promise.then(
    (value) => {
      seen.value = value
    },
    (error) => {
      seen.error = error
    }
  )
  return seen
}
const textOf = (value, seen = new Set()) => {
  if (value === null || value === undefined) return ''
  if (typeof value !== 'object' && typeof value !== 'function') return String(value)
  if (seen.has(value)) return ''
  seen.add(value)
  return Object.getOwnPropertyNames(value)
    .map((name) => textOf(value[name], seen))
    .concat(value instanceof Error ? [String(value.stack), textOf(value.cause, seen)] : [])
    .join('\n')
}
const monotonicMs = () => Number(process.hrtime.bigint()) / 1e6

describe('OAuth token cache', () => {
  const owned = []
  const make = (source, options = {}, revalidate) => {
    const provider = providerOf(source, optionsOf(options), revalidate)
    owned.push(provider)
    return provider
  }

  beforeEach(() => {
    jest.useFakeTimers({ now: 0 })
    jest.spyOn(Math, 'random').mockReturnValue(0.5)
  })

  afterEach(async () => {
    owned.splice(0).forEach((provider) => provider.close())
    await jest.advanceTimersByTimeAsync(0)
    const timers = jest.getTimerCount()
    jest.restoreAllMocks()
    jest.useRealTimers()
    expect(timers).toBe(0)
  })

  test('fifty callers share one flight', async () => {
    const held = deferred()
    const source = jest.fn(() => held.promise)
    const provider = make(source)
    const calls = Array.from({ length: 50 }, () => provider())
    await jest.advanceTimersByTimeAsync(0)
    expect(source).toHaveBeenCalledTimes(1)
    held.resolve({ value: jwt(3600) })
    const results = await Promise.all(calls)
    expect(results.every((result) => result.value === jwt(3600))).toBe(true)
    expect(new Set(results).size).toBe(50)
    expect(source).toHaveBeenCalledTimes(1)
  })

  test('failed callers share one sanitized error and a later call can recover', async () => {
    const held = deferred()
    const source = jest.fn(() => held.promise)
    const provider = make(source, { maxRetryTime: 100 })
    const calls = Array.from({ length: 50 }, () => observe(provider()))
    held.reject(Object.assign(new Error(MARKER), { cause: new Error(MARKER) }))
    await jest.advanceTimersByTimeAsync(0)
    const [first] = calls
    expect(first.error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(calls.every((call) => call.error === first.error)).toBe(true)
    expect(textOf(first.error)).not.toContain(MARKER)
    source.mockResolvedValueOnce({ value: jwt(3600) })
    expect((await provider()).value).toBe(jwt(3600))
    expect(source).toHaveBeenCalledTimes(2)
  })

  test('separate providers do not share credentials or token state', async () => {
    const one = jest.fn(async () => ({ value: jwt(3600, { sub: 'one' }) }))
    const two = jest.fn(async () => ({ value: jwt(3600, { sub: 'two' }) }))
    const first = make(one)
    const second = make(two)
    const results = await Promise.all(
      Array.from({ length: 50 }, (_, index) => (index % 2 ? first() : second()))
    )
    expect(one).toHaveBeenCalledTimes(1)
    expect(two).toHaveBeenCalledTimes(1)
    expect(results.filter((result) => result.value === jwt(3600, { sub: 'one' }))).toHaveLength(25)
    first.close()
    await expect(first()).rejects.toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect((await second()).value).toBe(jwt(3600, { sub: 'two' }))
    expect(two).toHaveBeenCalledTimes(1)
  })

  test('refresh starts at eighty percent with the buffer disabled', async () => {
    const timeouts = jest.spyOn(global, 'setTimeout')
    const source = jest.fn(async () => ({ value: jwt(3600) }))
    const provider = make(source, { refreshBuffer: 0 })
    await provider()
    const refresh = timeouts.mock.results.at(-1).value
    expect(refresh.hasRef()).toBe(false)
    await jest.advanceTimersByTimeAsync(2879999)
    expect(source).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(1)
    expect(source).toHaveBeenCalledTimes(2)
  })

  test.each([
    ['lower', 0, 2700000],
    ['upper', 1, 3060000],
  ])('refresh jitter includes the %s configured endpoint', async (_, random, due) => {
    Math.random.mockReturnValue(random)
    const source = jest.fn(async () => ({ value: jwt(3600) }))
    const provider = make(source, { refreshBuffer: 0 })
    await provider()
    await jest.advanceTimersByTimeAsync(due - 1)
    expect(source).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(1)
    expect(source).toHaveBeenCalledTimes(2)
  })

  test('long token lifetimes cannot overflow the refresh timer', async () => {
    const timeouts = jest.spyOn(global, 'setTimeout')
    const source = jest.fn(async () => ({ value: jwt(4000000) }))
    const provider = make(source, { refreshBuffer: 0 })
    await provider()
    expect(timeouts.mock.calls.at(-1)[1]).toBe(2147483647)
    await jest.advanceTimersByTimeAsync(2147483647)
    expect(source).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(1)
    await jest.advanceTimersByTimeAsync(3199999999 - 2147483647)
    expect(source).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(1)
    expect(source).toHaveBeenCalledTimes(2)
    expect(timeouts.mock.calls.every(([, delay]) => delay <= 2147483647)).toBe(true)
  })

  test('refresh buffer can precede the fractional window', async () => {
    const source = jest.fn(async () => ({ value: jwt(600) }))
    const provider = make(source)
    await provider()
    await jest.advanceTimersByTimeAsync(299999)
    expect(source).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(1)
    expect(source).toHaveBeenCalledTimes(2)
  })

  test('refresh never precedes the minimum period', async () => {
    const source = jest.fn(async () => ({ value: jwt(120) }))
    const provider = make(source)
    await provider()
    await jest.advanceTimersByTimeAsync(59999)
    expect(source).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(1)
    expect(source).toHaveBeenCalledTimes(2)
  })

  test('short lived tokens refresh on demand at the skew boundary', async () => {
    const replacement = jwt(120, { sub: 'replacement' })
    const source = jest
      .fn()
      .mockResolvedValueOnce({ value: jwt(45) })
      .mockResolvedValue({ value: replacement })
    const provider = make(source)
    const first = await provider()
    expect(jest.getTimerCount()).toBe(0)
    await jest.advanceTimersByTimeAsync(14999)
    expect(await provider()).toEqual(first)
    expect(source).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(1)
    const results = await Promise.all(Array.from({ length: 50 }, () => provider()))
    expect(source).toHaveBeenCalledTimes(2)
    expect(results.every((result) => result.value === replacement)).toBe(true)
  })

  test('the on demand fetch at the skew boundary has its own budget', async () => {
    const source = jest
      .fn()
      .mockResolvedValueOnce({ value: jwt(45) })
      .mockReturnValue(new Promise(() => {}))
    const provider = make(source)
    await provider()
    await jest.advanceTimersByTimeAsync(15000)
    const calls = Array.from({ length: 50 }, () => observe(provider()))
    await jest.advanceTimersByTimeAsync(7999)
    expect(source).toHaveBeenCalledTimes(2)
    expect(calls.some((call) => 'error' in call || 'value' in call)).toBe(false)
    await jest.advanceTimersByTimeAsync(1)
    expect(calls[0].error.message).toContain('deadline')
    expect(calls.every((call) => call.error === calls[0].error)).toBe(true)
  })

  test('failure at the short token boundary never reuses the expired token', async () => {
    const expiring = jwt(45)
    const source = jest
      .fn()
      .mockResolvedValueOnce({ value: expiring })
      .mockRejectedValue(new Error(MARKER))
    const provider = make(source)
    await provider()
    await jest.advanceTimersByTimeAsync(15000)
    const calls = Array.from({ length: 50 }, () => observe(provider()))
    await jest.advanceTimersByTimeAsync(8000)
    expect(calls.every((call) => call.error instanceof KafkaJSSASLAuthenticationError)).toBe(true)
    expect(calls.some((call) => 'value' in call)).toBe(false)
    const before = source.mock.calls.length
    const later = observe(provider())
    await jest.advanceTimersByTimeAsync(8000)
    expect(source.mock.calls.length).toBeGreaterThan(before)
    expect(later.error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
  })

  test.each([
    ['a token without lifetime', { value: 'opaque' }],
    ['a zero lifetime', { value: 'opaque', expiresIn: 0 }],
    ['a token already inside clock skew', { value: jwt(10) }],
  ])('%s does not schedule itself and is fetched again', async (_, result) => {
    const source = jest.fn(async () => result)
    const provider = make(source)
    const results = await Promise.all(Array.from({ length: 50 }, () => provider()))
    expect(results).toHaveLength(50)
    expect(results.every((response) => response.value === result.value)).toBe(true)
    expect(source).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
    await provider()
    expect(source).toHaveBeenCalledTimes(2)
  })

  test.each([
    ['at the current second', 0],
    ['in the past', -60],
  ])('an actually expired JWT is refused without a refresh loop: exp %s', async (_, exp) => {
    const source = jest.fn(async () => ({ value: jwt(exp) }))
    const provider = make(source)
    const error = await provider().catch((caught) => caught)
    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.message).toContain('expired')
    expect(source).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
  })

  test('JWT expiration takes precedence over expires_in', async () => {
    const source = jest.fn(async () => ({ value: jwt(45), expiresIn: 3600 }))
    const provider = make(source)
    await provider()
    await jest.advanceTimersByTimeAsync(14999)
    await provider()
    expect(source).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(1)
    await provider()
    expect(source).toHaveBeenCalledTimes(2)
  })

  test('opaque tokens use expires_in', async () => {
    const source = jest.fn(async () => ({ value: 'opaque', expiresIn: 60 }))
    const provider = make(source)
    await provider()
    expect(jest.getTimerCount()).toBe(0)
    await jest.advanceTimersByTimeAsync(29999)
    await provider()
    expect(source).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(1)
    await provider()
    expect(source).toHaveBeenCalledTimes(2)
  })

  test.each([
    ['a missing exp', `${segment({ alg: 'none' })}.${segment({ sub: 'x' })}.`],
    ['a string exp', `${segment({ alg: 'none' })}.${segment({ exp: '3600' })}.`],
    ['a null exp', `${segment({ alg: 'none' })}.${segment({ exp: null })}.`],
    [
      'an exp beyond finite milliseconds',
      `${segment({ alg: 'none' })}.${segment({ exp: 1e308 })}.`,
    ],
    ['a payload that is not JSON', `${segment({ alg: 'none' })}.bm90LWpzb24.`],
    ['a null payload', `${segment({ alg: 'none' })}.${segment(null)}.`],
    ['an opaque token', 'opaque'],
  ])('invalid JWT expiry falls back to expires_in without coercion: %s', async (_, value) => {
    const source = jest.fn(async () => ({ value, expiresIn: 60 }))
    const provider = make(source)
    await provider()
    await jest.advanceTimersByTimeAsync(29999)
    await provider()
    expect(source).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(1)
    await provider()
    expect(source).toHaveBeenCalledTimes(2)
  })

  test.each([['60'], [-1], [Infinity], [1e306], [null], [undefined]])(
    'an unusable expires_in %p is not a cached lifetime',
    async (expiresIn) => {
      const source = jest.fn(async () => ({ value: 'opaque', expiresIn }))
      const provider = make(source)
      await provider()
      await provider()
      expect(source).toHaveBeenCalledTimes(2)
      expect(jest.getTimerCount()).toBe(0)
    }
  )

  test.each([[null], [{}], [{ value: '' }], [{ value: 5 }], ['token']])(
    'a source result without a token fails terminally: %p',
    async (result) => {
      const source = jest.fn(async () => result)
      const provider = make(source)
      const error = await provider().catch((caught) => caught)
      expect(error.message).toBe(
        'SASL OAUTHBEARER helper: token: the token response has no access token'
      )
      expect(source).toHaveBeenCalledTimes(1)
    }
  )

  test('retrieval retries at one hundred two hundred and four hundred milliseconds', async () => {
    const starts = []
    const source = jest.fn(async () => {
      starts.push(monotonicMs())
      if (starts.length < 4) throw new Error(MARKER)
      return { value: jwt(3600) }
    })
    const provider = make(source)
    const call = observe(provider())
    await jest.advanceTimersByTimeAsync(700)
    expect(starts).toEqual([0, 100, 300, 700])
    expect(call.value.value).toBe(jwt(3600))
  })

  test('retry backoff stops growing at its configured maximum', async () => {
    const starts = []
    const source = jest.fn(async () => {
      starts.push(monotonicMs())
      if (starts.length < 5) throw new Error(MARKER)
      return { value: jwt(3600) }
    })
    const provider = make(source, { retry: { initial: 100, max: 250 } })
    const call = observe(provider())
    await jest.advanceTimersByTimeAsync(800)
    expect(starts).toEqual([0, 100, 300, 550, 800])
    expect(call.value.value).toBe(jwt(3600))
  })

  test('retrieval stops under the original deadline', async () => {
    const starts = []
    const source = jest.fn(async () => {
      starts.push(monotonicMs())
      throw new Error(MARKER)
    })
    const provider = make(source)
    const call = observe(provider())
    await jest.advanceTimersByTimeAsync(6299)
    expect(call.error).toBeUndefined()
    await jest.advanceTimersByTimeAsync(1)
    expect(call.error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(starts).toEqual([0, 100, 300, 700, 1500, 3100, 6300])
    await jest.advanceTimersByTimeAsync(10000)
    expect(source).toHaveBeenCalledTimes(7)
    expect(textOf(call.error)).not.toContain(MARKER)
  })

  test('a caller joining late shares the original deadline', async () => {
    const source = jest.fn(() => new Promise(() => {}))
    const provider = make(source)
    const first = observe(provider())
    await jest.advanceTimersByTimeAsync(7900)
    const late = observe(provider())
    await jest.advanceTimersByTimeAsync(99)
    expect(first.error).toBeUndefined()
    expect(late.error).toBeUndefined()
    await jest.advanceTimersByTimeAsync(1)
    expect(late.error).toBe(first.error)
    expect(first.error.message).toContain('deadline')
    expect(source).toHaveBeenCalledTimes(1)
  })

  test('a hung supplier has settled at the original budget boundary', async () => {
    const provider = make(() => new Promise(() => {}))
    const call = observe(provider())
    await jest.advanceTimersByTimeAsync(7999)
    expect(call.error).toBeUndefined()
    await jest.advanceTimersByTimeAsync(1)
    expect(call.error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
  })

  test('a late supplier cannot pass the deadline fence', async () => {
    const held = deferred()
    const sent = jest.fn()
    const provider = make(async (context) => {
      const assertion = await held.promise
      context.check()
      sent(assertion)
      return { value: jwt(3600) }
    })
    const call = observe(provider())
    await jest.advanceTimersByTimeAsync(8000)
    expect(call.error.message).toContain('deadline')
    held.resolve('assertion')
    await jest.advanceTimersByTimeAsync(1)
    expect(sent).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(0)
  })

  test('a late source result is not applied after the deadline', async () => {
    const held = deferred()
    const source = jest.fn().mockReturnValueOnce(held.promise)
    const provider = make(source)
    const call = observe(provider())
    await jest.advanceTimersByTimeAsync(8000)
    expect(call.error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    held.resolve({ value: jwt(3600) })
    await jest.advanceTimersByTimeAsync(0)
    expect(jest.getTimerCount()).toBe(0)
    source.mockResolvedValueOnce({ value: jwt(3600, { sub: 'next' }) })
    expect((await provider()).value).toBe(jwt(3600, { sub: 'next' }))
  })

  test('a withheld timer cannot permit work after actual monotonic expiry', async () => {
    let now = 0n
    jest.spyOn(process.hrtime, 'bigint').mockImplementation(() => now)
    const held = deferred()
    const sent = jest.fn()
    const provider = make(async (context) => {
      await held.promise
      context.check()
      sent()
      return { value: jwt(3600) }
    })
    const call = observe(provider())
    now = 8000000000n
    held.resolve()
    await jest.advanceTimersByTimeAsync(0)
    expect(call.error.message).toContain('deadline')
    expect(sent).not.toHaveBeenCalled()
  })

  test('a withheld deadline timer cannot let a retry start after the budget', async () => {
    let now = 0n
    jest.spyOn(process.hrtime, 'bigint').mockImplementation(() => now)
    const source = jest.fn(async () => {
      throw new Error(MARKER)
    })
    const provider = make(source)
    const call = observe(provider())
    await jest.advanceTimersByTimeAsync(0)
    expect(source).toHaveBeenCalledTimes(1)
    now = 9000000000n
    await jest.advanceTimersByTimeAsync(100)
    expect(call.error.message).toContain('deadline')
    expect(source).toHaveBeenCalledTimes(1)
  })

  test('wall time cannot extend the retrieval budget', async () => {
    const provider = make(() => new Promise(() => {}))
    const call = observe(provider())
    jest.setSystemTime(Date.now() + 3600000)
    await jest.advanceTimersByTimeAsync(7999)
    expect(call.error).toBeUndefined()
    await jest.advanceTimersByTimeAsync(1)
    expect(call.error.message).toContain('deadline')
  })

  test('a forward wall step expires a cached token before its timer wakes', async () => {
    const source = jest.fn(async () => ({ value: jwt(3600) }))
    const provider = make(source)
    await provider()
    jest.setSystemTime(3570000)
    await provider()
    expect(source).toHaveBeenCalledTimes(2)
  })

  test('a refresh wake after a forward wall step finds the token expired and waits', async () => {
    const source = jest.fn(async () => ({ value: jwt(3600) }))
    const provider = make(source, { refreshBuffer: 0 })
    await provider()
    jest.setSystemTime(3590000)
    await jest.advanceTimersByTimeAsync(2880000)
    expect(source).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
  })

  test('a refresh timer left from an expired token cannot start a second retrieval', async () => {
    const held = deferred()
    const source = jest
      .fn()
      .mockResolvedValueOnce({ value: jwt(3600) })
      .mockReturnValueOnce(held.promise)
    const provider = make(source, { refreshBuffer: 0 })
    await provider()
    await jest.advanceTimersByTimeAsync(2879000)
    jest.setSystemTime(3570000)
    const call = observe(provider())
    jest.setSystemTime(2879000)
    await jest.advanceTimersByTimeAsync(1000)
    expect(source).toHaveBeenCalledTimes(2)
    expect(call).toEqual({})
    held.resolve({ value: jwt(9000, { sub: 'next' }) })
    await jest.advanceTimersByTimeAsync(0)
    expect(call.value.value).toBe(jwt(9000, { sub: 'next' }))
    expect(source).toHaveBeenCalledTimes(2)
  })

  test('a backward wall clock does not renew an acquisition', async () => {
    jest.setSystemTime(10000000)
    const provider = make(() => new Promise(() => {}))
    const call = observe(provider())
    jest.setSystemTime(10000000 - 3600000)
    await jest.advanceTimersByTimeAsync(7999)
    expect(call.error).toBeUndefined()
    await jest.advanceTimersByTimeAsync(1)
    expect(call.error.message).toContain('deadline')
  })

  test('a refresh wake before its wall clock time only re-arms', async () => {
    const source = jest.fn(async () => ({ value: jwt(3600) }))
    const provider = make(source, { refreshBuffer: 0 })
    await provider()
    jest.setSystemTime(Date.now() - 1000)
    await jest.advanceTimersByTimeAsync(2880000)
    expect(source).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(1)
    await jest.advanceTimersByTimeAsync(1000)
    expect(source).toHaveBeenCalledTimes(2)
  })

  test('background failure retains the valid token', async () => {
    const first = jwt(3600)
    const starts = []
    const source = jest.fn(async () => {
      starts.push(Date.now())
      if (starts.length === 1) return { value: first }
      throw new Error(MARKER)
    })
    const provider = make(source, { refreshBuffer: 0 })
    await provider()
    await jest.advanceTimersByTimeAsync(2880000)
    expect(source).toHaveBeenCalledTimes(2)
    expect((await provider()).value).toBe(first)
    expect(source).toHaveBeenCalledTimes(2)
    await jest.advanceTimersByTimeAsync(300)
    expect(starts).toEqual([0, 2880000, 2880100, 2880300])
    expect((await provider()).value).toBe(first)
  })

  test('background retry stops at usable expiry', async () => {
    const replacement = jwt(120, { sub: 'replacement' })
    const starts = []
    const source = jest.fn(async () => {
      starts.push(Date.now())
      if (starts.length === 1) return { value: jwt(20) }
      if (Date.now() >= 19000) return { value: replacement }
      throw new Error(MARKER)
    })
    const provider = make(source, { clockSkew: 1000, minRefreshPeriod: 1000, refreshBuffer: 0 })
    await provider()
    await jest.advanceTimersByTimeAsync(17500)
    expect(starts).toEqual([0, 16000, 16100, 16300, 16700, 17500])
    expect(jest.getTimerCount()).toBe(0)
    await jest.advanceTimersByTimeAsync(1499)
    expect(source).toHaveBeenCalledTimes(6)
    expect((await provider()).value).toBe(jwt(20))
    await jest.advanceTimersByTimeAsync(1)
    expect((await provider()).value).toBe(replacement)
    expect(starts.at(-1)).toBe(19000)
  })

  test('background retries continue their backoff across bounded retrievals', async () => {
    const starts = []
    const source = jest.fn(async () => {
      starts.push(Date.now())
      if (starts.length === 1) return { value: jwt(100) }
      throw new Error(MARKER)
    })
    const provider = make(source, {
      clockSkew: 1000,
      minRefreshPeriod: 1000,
      refreshBuffer: 0,
      maxRetryTime: 1000,
    })
    await provider()
    await jest.advanceTimersByTimeAsync(99000)
    expect(starts).toEqual([0, 80000, 80100, 80300, 80700, 81500, 83100, 86300, 92700])
    expect(jest.getTimerCount()).toBe(0)
  })

  test('close during a background retrieval stops its retry chain', async () => {
    const held = deferred()
    const source = jest
      .fn()
      .mockResolvedValueOnce({ value: jwt(3600) })
      .mockReturnValueOnce(held.promise)
    const provider = make(source, { refreshBuffer: 0 })
    await provider()
    await jest.advanceTimersByTimeAsync(2880000)
    expect(source).toHaveBeenCalledTimes(2)
    provider.close()
    held.reject(new Error(MARKER))
    await jest.advanceTimersByTimeAsync(3600000)
    expect(source).toHaveBeenCalledTimes(2)
  })

  test('a successful background retry replaces the token', async () => {
    const values = [jwt(3600), jwt(7200, { sub: 'two' })]
    const starts = []
    const source = jest.fn(async () => {
      starts.push(Date.now())
      if (starts.length === 2) throw new Error(MARKER)
      return { value: values.shift() }
    })
    const provider = make(source, { refreshBuffer: 0 })
    await provider()
    await jest.advanceTimersByTimeAsync(2880100)
    expect(starts).toEqual([0, 2880000, 2880100])
    expect((await provider()).value).toBe(jwt(7200, { sub: 'two' }))
  })

  test('a successful background retry resets the backoff for the next refresh', async () => {
    const starts = []
    const source = jest.fn(async () => {
      starts.push(Date.now())
      if (starts.length === 1) return { value: jwt(100) }
      if (starts.length === 6) return { value: jwt(200, { sub: 'two' }) }
      throw new Error(MARKER)
    })
    const provider = make(source, {
      clockSkew: 1000,
      minRefreshPeriod: 1000,
      refreshBuffer: 0,
      maxRetryTime: 1000,
    })
    await provider()
    await jest.advanceTimersByTimeAsync(176400)
    expect(starts).toEqual([0, 80000, 80100, 80300, 80700, 81500, 176300, 176400])
    expect((await provider()).value).toBe(jwt(200, { sub: 'two' }))
  })

  test('close rejects every waiter and rejects later calls', async () => {
    const held = deferred()
    const source = jest.fn(() => held.promise)
    const provider = make(source)
    const calls = Array.from({ length: 50 }, () => observe(provider()))
    await jest.advanceTimersByTimeAsync(0)
    provider.close()
    provider.close()
    await jest.advanceTimersByTimeAsync(0)
    expect(calls[0].error.message).toContain('closed')
    expect(calls.every((call) => call.error === calls[0].error)).toBe(true)
    await expect(provider()).rejects.toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(source).toHaveBeenCalledTimes(1)
    held.resolve({ value: jwt(3600) })
    await jest.advanceTimersByTimeAsync(0)
    expect(jest.getTimerCount()).toBe(0)
    await expect(provider()).rejects.toThrow('closed')
  })

  test('close clears a pending retry without a later request', async () => {
    const source = jest.fn(async () => {
      throw new Error(MARKER)
    })
    const provider = make(source)
    const call = observe(provider())
    await jest.advanceTimersByTimeAsync(50)
    provider.close()
    await jest.advanceTimersByTimeAsync(10000)
    expect(source).toHaveBeenCalledTimes(1)
    expect(call.error.message).toContain('closed')
    expect(jest.getTimerCount()).toBe(0)
  })

  test('close clears an unref refresh timer', async () => {
    const source = jest.fn(async () => ({ value: jwt(3600) }))
    const provider = make(source)
    await provider()
    expect(jest.getTimerCount()).toBe(1)
    provider.close()
    expect(jest.getTimerCount()).toBe(0)
    await jest.advanceTimersByTimeAsync(3600000)
    expect(source).toHaveBeenCalledTimes(1)
  })

  test('an aborted completion cannot replace a closed cache record', async () => {
    const held = deferred()
    const signals = []
    const provider = make(async ({ signal }) => {
      signals.push(signal)
      await held.promise
      return { value: jwt(3600, { sub: 'late' }) }
    })
    const call = observe(provider())
    await jest.advanceTimersByTimeAsync(0)
    provider.close()
    expect(signals[0].aborted).toBe(true)
    held.resolve()
    await jest.advanceTimersByTimeAsync(0)
    expect(call.error.message).toContain('closed')
    expect(jest.getTimerCount()).toBe(0)
    await expect(provider()).rejects.toThrow('closed')
  })

  test('old cleanup cannot release a successor flight', async () => {
    const late = deferred()
    const next = deferred()
    const source = jest.fn().mockReturnValueOnce(late.promise).mockReturnValueOnce(next.promise)
    const provider = make(source)
    const first = observe(provider())
    await jest.advanceTimersByTimeAsync(8000)
    expect(first.error.message).toContain('deadline')
    const second = observe(provider())
    await jest.advanceTimersByTimeAsync(0)
    late.resolve({ value: jwt(3600, { sub: 'old' }) })
    await jest.advanceTimersByTimeAsync(0)
    expect(second).toEqual({})
    expect(jest.getTimerCount()).toBe(1)
    next.resolve({ value: jwt(3600, { sub: 'new' }) })
    await jest.advanceTimersByTimeAsync(0)
    expect(second.value.value).toBe(jwt(3600, { sub: 'new' }))
    expect((await provider()).value).toBe(jwt(3600, { sub: 'new' }))
  })

  test('extensions are copied for each returned response', async () => {
    const extensions = { trace: 'one' }
    const held = deferred()
    const source = jest.fn().mockReturnValueOnce(held.promise)
    const provider = make(source, { extensions })
    extensions.trace = 'changed'
    const calls = Array.from({ length: 50 }, () => provider())
    held.resolve({ value: jwt(3600) })
    const results = await Promise.all(calls)
    results[0].extensions.trace = 'mutated'
    expect(results.slice(1).every((result) => result.extensions.trace === 'one')).toBe(true)
    expect(new Set(results.map((result) => result.extensions)).size).toBe(50)
    expect(await provider()).toEqual({ value: jwt(3600), extensions: { trace: 'one' } })
  })

  test('a response without extensions carries only the token', async () => {
    const provider = make(async () => ({
      value: jwt(3600),
      expiresIn: 60,
      refreshToken: MARKER,
    }))
    expect(Object.keys(await provider())).toEqual(['value'])
  })

  test('supplier errors cannot borrow the helper error class to leak a secret', async () => {
    const borrowed = new KafkaJSSASLAuthenticationError(`${MARKER} message`, {
      cause: new Error(`${MARKER} cause`),
    })
    borrowed.stack = `${MARKER} stack`
    const provider = make(async () => {
      throw borrowed
    })
    const call = observe(provider())
    await jest.advanceTimersByTimeAsync(8000)
    expect(call.error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(call.error).not.toBe(borrowed)
    expect(textOf(call.error)).not.toContain(MARKER)
  })

  test('a revalidating cache asks its source on every call and keeps an unchanged record', async () => {
    const source = jest.fn(async ({ previous, forceRead }) =>
      previous && !forceRead ? previous : { value: jwt(3600), version: 'one' }
    )
    const provider = make(source, {}, true)
    await provider()
    await provider()
    await provider()
    expect(source).toHaveBeenCalledTimes(3)
    expect(source.mock.calls.map(([context]) => context.forceRead)).toEqual([true, false, false])
    expect(jest.getTimerCount()).toBe(0)
    await jest.advanceTimersByTimeAsync(2880000)
    await provider()
    expect(source.mock.calls.at(-1)[0].forceRead).toBe(true)
  })

  test('a revalidating cache offers its record again after a failed check', async () => {
    const source = jest
      .fn()
      .mockResolvedValueOnce({ value: jwt(3600), version: 'one' })
      .mockRejectedValueOnce(new Error(MARKER))
      .mockImplementation(async ({ previous }) => previous)
    const provider = make(source, { maxRetryTime: 100 }, true)
    const first = await provider()
    await expect(provider()).rejects.toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(await provider()).toEqual(first)
    expect(source.mock.calls.map(([context]) => context.previous?.version)).toEqual([
      undefined,
      'one',
      'one',
    ])
  })

  describe('options', () => {
    const refused = (options, name) => {
      expect(() => optionsOf(options)).toThrow(KafkaJSInvalidConfigurationError)
      expect(() => optionsOf(options)).toThrow(`Invalid OAuth bearer helper option: ${name}`)
    }

    test.each([[undefined], [null], [[]], [false], [1], ['x']])(
      'refuses non object options %p',
      (options) => refused(options, 'options')
    )

    test('defaults every common option', () => {
      expect(optionsOf({})).toEqual({
        timeout: 5000,
        maxRetryTime: 8000,
        retry: { initial: 100, max: 10000 },
        refreshWindow: 0.8,
        refreshJitter: 0.05,
        minRefreshPeriod: 60000,
        refreshBuffer: 300000,
        clockSkew: 30000,
        extensions: undefined,
        allowInsecure: false,
        fetch: globalThis.fetch,
      })
      expect(optionsOf({ ...{ timeout: undefined, retry: undefined } }).timeout).toBe(5000)
    })

    test.each(['timeout', 'maxRetryTime', 'minRefreshPeriod'])(
      'validates the positive integral option %s',
      (name) => {
        for (const value of [0, -1, 0.5, 2147483648, NaN, Infinity, null, '1', true, {}, []]) {
          refused({ [name]: value }, name)
        }
        for (const value of [1, 2147483647]) expect(optionsOf({ [name]: value })[name]).toBe(value)
      }
    )

    test.each(['refreshBuffer', 'clockSkew'])(
      'validates the nonnegative integral option %s',
      (name) => {
        for (const value of [-1, 0.5, 2147483648, NaN, Infinity, null, '0', true, {}]) {
          refused({ [name]: value }, name)
        }
        for (const value of [0, 2147483647]) expect(optionsOf({ [name]: value })[name]).toBe(value)
      }
    )

    test('validates the retry backoff', () => {
      for (const retry of [null, 1, 'x', []]) refused({ retry }, 'retry')
      for (const initial of [0, 0.5, '100', 2147483648, null]) {
        refused({ retry: { initial } }, 'retry.initial')
      }
      for (const max of [0, 2147483648, '100', null]) refused({ retry: { max } }, 'retry.max')
      refused({ retry: { initial: 200, max: 100 } }, 'retry.max')
      refused({ retry: { initial: 20000 } }, 'retry.max')
      expect(optionsOf({ retry: {} }).retry).toEqual({ initial: 100, max: 10000 })
      expect(optionsOf({ retry: { initial: 1, max: 1 } }).retry).toEqual({ initial: 1, max: 1 })
      expect(optionsOf({ retry: { initial: 2147483647, max: 2147483647 } }).retry.max).toBe(
        2147483647
      )
    })

    test('validates the refresh window and jitter', () => {
      for (const refreshWindow of [0, 1, -0.1, 1.1, NaN, Infinity, null, '0.5']) {
        refused({ refreshWindow }, 'refreshWindow')
      }
      for (const refreshJitter of [-0.01, NaN, Infinity, null, '0.05']) {
        refused({ refreshJitter }, 'refreshJitter')
      }
      refused({ refreshWindow: 0.8, refreshJitter: 0.2 }, 'refreshJitter')
      refused({ refreshWindow: 0.1, refreshJitter: 0.1 }, 'refreshJitter')
      expect(optionsOf({ refreshWindow: 0.06 }).refreshWindow).toBe(0.06)
      expect(optionsOf({ refreshJitter: 0 }).refreshJitter).toBe(0)
      expect(optionsOf({ refreshWindow: 0.8, refreshJitter: 0.19 }).refreshJitter).toBe(0.19)
    })

    test('validates allowInsecure and fetch', () => {
      for (const allowInsecure of [1, 'true', null]) refused({ allowInsecure }, 'allowInsecure')
      for (const fetch of [null, {}, [], 'x', true, 1]) refused({ fetch }, 'fetch')
      const fetch = () => {}
      expect(optionsOf({ allowInsecure: true, fetch })).toMatchObject({
        allowInsecure: true,
        fetch,
      })
    })

    test('validates and copies extensions', () => {
      for (const extensions of [
        [],
        'x',
        null,
        { '': 'x' },
        { 'a\0': 'x' },
        { a: 1 },
        { a: 'x\0' },
      ]) {
        refused({ extensions }, 'extensions')
      }
      const extensions = Object.assign(Object.create({ inherited: 'x' }), { a: '', b: 'two' })
      const copied = optionsOf({ extensions }).extensions
      extensions.a = 'changed'
      expect(copied).toEqual({ a: '', b: 'two' })
      expect(Object.getPrototypeOf(copied)).toBe(Object.prototype)
    })
  })

  describe('over a token endpoint', () => {
    let server
    let url
    let requests
    let sockets
    let answer

    beforeEach(async () => {
      requests = []
      sockets = new Set()
      answer = (_request, response) => {
        response.setHeader('Content-Type', 'application/json')
        response.end(JSON.stringify({ access_token: 'opaque-token', expires_in: 3600 }))
      }
      server = http.createServer(async (request, response) => {
        const chunks = []
        for await (const chunk of request) chunks.push(chunk)
        requests.push({ headers: request.headers, body: Buffer.concat(chunks).toString() })
        answer(request, response)
      })
      server.on('connection', (socket) => {
        socket.bytes = 0
        socket.on('data', (data) => {
          socket.bytes += data.length
        })
        sockets.add(socket)
        socket.once('close', () => sockets.delete(socket))
      })
      await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
      url = `http://127.0.0.1:${server.address().port}/token?query=${MARKER}`
    })

    afterEach(async () => {
      owned.splice(0).forEach((provider) => provider.close())
      const closed = [...sockets].map(
        (socket) => new Promise((resolve) => socket.once('close', resolve))
      )
      await new Promise((resolve) => server.close(resolve))
      await Promise.all(closed)
    })

    const helper = (options = {}) => {
      const provider = clientCredentials({
        tokenEndpointUrl: url,
        clientId: 'client',
        clientSecret: MARKER,
        allowInsecure: true,
        ...options,
      })
      owned.push(provider)
      return provider
    }

    test('fifty concurrent calls make one request', async () => {
      const provider = helper()
      const results = await Promise.all(Array.from({ length: 50 }, () => provider()))
      expect(requests).toHaveLength(1)
      expect(results.every((result) => result.value === 'opaque-token')).toBe(true)
    })

    test('close during a stalled request leaves no timers and no sockets', async () => {
      const arrived = deferred()
      answer = () => arrived.resolve()
      const provider = helper()
      const call = observe(provider())
      await arrived.promise
      expect(sockets.size).toBe(1)
      const [active] = sockets
      const socketClosed = new Promise((resolve) => active.once('close', resolve))
      provider.close()
      expect(jest.getTimerCount()).toBe(0)
      await socketClosed
      await realDelay(50)
      expect(sockets.has(active)).toBe(false)
      expect([...sockets].every((socket) => socket.bytes === 0)).toBe(true)
      expect(requests).toHaveLength(1)
      await jest.advanceTimersByTimeAsync(0)
      expect(call.error.message).toContain('closed')
    })

    test('close during a stalled body rejects callers and releases the socket', async () => {
      const arrived = deferred()
      answer = (_request, response) => {
        response.writeHead(200, { 'Content-Type': 'application/json' })
        response.write('{"access_token":"')
        arrived.resolve()
      }
      const provider = helper()
      const call = observe(provider())
      await arrived.promise
      const [active] = sockets
      const socketClosed = new Promise((resolve) => active.once('close', resolve))
      provider.close()
      await socketClosed
      await jest.advanceTimersByTimeAsync(0)
      expect(call.error.message).toContain('closed')
      expect(jest.getTimerCount()).toBe(0)
    })

    test('the error text never contains the secret or the token', async () => {
      answer = (_request, response) => {
        response.writeHead(400, { 'Content-Type': 'application/json' })
        response.end(
          JSON.stringify({
            error: 'invalid_client',
            error_description: MARKER,
            access_token: MARKER,
          })
        )
      }
      const error = await helper()().catch((caught) => caught)
      expect(error.message).toBe(
        `SASL OAUTHBEARER helper: status: the token endpoint refused the request (endpoint=${new URL(url).origin}/token status=400 error=invalid_client)`
      )
      expect(textOf(error)).not.toContain(MARKER)
      expect(requests).toHaveLength(1)
    })

    test('a never resolving assertion supplier settles at the budget and sends nothing later', async () => {
      const held = deferred()
      const provider = helper({ clientSecret: undefined, clientAssertion: () => held.promise })
      const call = observe(provider())
      await jest.advanceTimersByTimeAsync(8000)
      expect(call.error.message).toContain('deadline')
      held.resolve('late.assertion.value')
      await jest.advanceTimersByTimeAsync(0)
      await realDelay(50)
      expect(requests).toHaveLength(0)
      expect(sockets.size).toBe(0)
    })

    test('a retry near the deadline shares the remaining hundred milliseconds', async () => {
      const supplierStarts = []
      const attemptStarts = []
      const aborted = []
      let releaseBody
      const clientAssertion = () => {
        supplierStarts.push(monotonicMs())
        return supplierStarts.length === 1
          ? new Promise((_, reject) => setTimeout(() => reject(new Error(MARKER)), 7800))
          : 'header.payload.signature'
      }
      const fetch = jest.fn((_url, init) => {
        attemptStarts.push(monotonicMs())
        const body = new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode('{"access_token":"'))
            releaseBody = () => {
              controller.enqueue(new TextEncoder().encode('late-token"}'))
              controller.close()
            }
            init.signal.addEventListener('abort', () => aborted.push(monotonicMs()))
          },
        })
        return Promise.resolve(new Response(body, { status: 200 }))
      })
      const provider = helper({
        clientSecret: undefined,
        clientAssertion,
        fetch,
        tokenEndpointUrl: 'https://issuer.example/token',
      })
      const call = observe(provider())
      await jest.advanceTimersByTimeAsync(7900)
      expect(supplierStarts).toEqual([0, 7900])
      expect(attemptStarts).toEqual([7900])
      await jest.advanceTimersByTimeAsync(99)
      expect(call).toEqual({})
      await jest.advanceTimersByTimeAsync(1)
      expect(call.error.message).toContain('deadline')
      expect(aborted).toEqual([8000])
      try {
        releaseBody()
      } catch {
        // The canceled stream refuses more data.
      }
      await jest.advanceTimersByTimeAsync(100)
      expect(attemptStarts).toEqual([7900])
      expect(provider.close()).toBeUndefined()
      expect(jest.getTimerCount()).toBe(0)
    })
  })
})
