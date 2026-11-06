const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { setTimeout: realDelay } = require('node:timers/promises')
const { fileToken } = require('../fileToken')
const { KafkaJSSASLAuthenticationError, KafkaJSInvalidConfigurationError } = require('../../errors')

const MARKER = 'inert-marker'
const NOW = 1792000000000
const segment = (value) => Buffer.from(JSON.stringify(value)).toString('base64url')
const jwt = (expSeconds, claims = {}) =>
  `${segment({ alg: 'none', typ: 'JWT' })}.${segment({ exp: expSeconds, ...claims })}.`
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

describe('OAuth file token', () => {
  const providers = []
  let dir
  let file

  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'oauth-file-'))
    file = path.join(dir, 'access.jwt')
    jest.useFakeTimers({ now: NOW })
    jest.spyOn(Math, 'random').mockReturnValue(0.5)
  })

  afterEach(async () => {
    providers.splice(0).forEach((provider) => provider.close())
    await jest.advanceTimersByTimeAsync(0)
    const timers = jest.getTimerCount()
    jest.restoreAllMocks()
    jest.useRealTimers()
    fs.rmSync(dir, { recursive: true, force: true })
    expect(timers).toBe(0)
  })

  const make = (options = {}) => {
    const provider = fileToken({ path: file, maxRetryTime: 50, ...options })
    providers.push(provider)
    return provider
  }
  const write = (content, seconds) => {
    fs.writeFileSync(file, content)
    fs.utimesSync(file, NOW / 1000 + seconds, NOW / 1000 + seconds)
  }
  const spies = () => ({
    stat: jest.spyOn(fs.promises, 'stat'),
    read: jest.spyOn(fs.promises, 'readFile'),
  })

  test('unchanged file is read once and a changed file is used immediately', async () => {
    const first = jwt(NOW / 1000 + 3600, { sub: 'one' })
    const second = jwt(NOW / 1000 + 3600, { sub: 'two' })
    write(first, 0)
    const { stat, read } = spies()
    const provider = make()
    expect(await provider()).toEqual({ value: first })
    expect((await provider()).value).toBe(first)
    expect((await provider()).value).toBe(first)
    expect(stat).toHaveBeenCalledTimes(3)
    expect(read).toHaveBeenCalledTimes(1)
    write(second, 10)
    expect((await provider()).value).toBe(second)
    expect((await provider()).value).toBe(second)
    expect(read).toHaveBeenCalledTimes(2)
  })

  test('fifty file callers share one validation read', async () => {
    const token = jwt(NOW / 1000 + 3600)
    write(token, 0)
    const original = fs.promises.readFile
    const reading = deferred()
    const held = deferred()
    const read = jest.spyOn(fs.promises, 'readFile').mockImplementation(async (...args) => {
      reading.resolve()
      await held.promise
      return original(...args)
    })
    const provider = make()
    const calls = Array.from({ length: 50 }, () => provider())
    await reading.promise
    held.resolve()
    const results = await Promise.all(calls)
    expect(read).toHaveBeenCalledTimes(1)
    expect(results.every((result) => result.value === token)).toBe(true)
    expect(new Set(results).size).toBe(50)
  })

  test('source revalidation preserves the original expiry and refresh point', async () => {
    write(jwt(NOW / 1000 + 3600), 0)
    const { read } = spies()
    const provider = make()
    await provider()
    for (const step of [1000000, 1000000, 879999]) {
      await jest.advanceTimersByTimeAsync(step)
      await provider()
    }
    expect(read).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(1)
    await provider()
    expect(read).toHaveBeenCalledTimes(2)
    await jest.advanceTimersByTimeAsync(419999)
    await provider()
    expect(read).toHaveBeenCalledTimes(2)
    await jest.advanceTimersByTimeAsync(1)
    await provider()
    expect(read).toHaveBeenCalledTimes(3)
    expect(jest.getTimerCount()).toBe(0)
  })

  test('an unchanged file is read again once at its refresh point', async () => {
    const token = jwt(NOW / 1000 + 3600)
    write(token, 0)
    const { read } = spies()
    const provider = make()
    await provider()
    await jest.advanceTimersByTimeAsync(2880000)
    const results = await Promise.all(Array.from({ length: 50 }, () => provider()))
    expect(read).toHaveBeenCalledTimes(2)
    expect(results.every((result) => result.value === token)).toBe(true)
  })

  test('a later rewrite that restores the modification time is still detected', async () => {
    const first = jwt(NOW / 1000 + 3600, { sub: 'one' })
    const second = jwt(NOW / 1000 + 3600, { sub: 'two' })
    expect(second).toHaveLength(first.length)
    write(first, 0)
    const provider = make()
    await provider()
    await realDelay(50)
    write(second, 0)
    expect((await provider()).value).toBe(second)
  })

  test('atomic replacement is detected by source identity', async () => {
    const first = jwt(NOW / 1000 + 3600, { sub: 'one' })
    const second = jwt(NOW / 1000 + 3600, { sub: 'two' })
    write(first, 0)
    const provider = make()
    await provider()
    const staged = path.join(dir, 'staged.jwt')
    fs.writeFileSync(staged, second)
    fs.utimesSync(staged, NOW / 1000, NOW / 1000)
    fs.renameSync(staged, file)
    expect((await provider()).value).toBe(second)
  })

  test('an empty rotated file cannot fall back to the old usable token', async () => {
    write(jwt(NOW / 1000 + 3600, { sub: 'one' }), 0)
    const provider = make()
    await provider()
    write('', 10)
    const error = await provider().catch((caught) => caught)
    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.message).toBe(
      'SASL OAUTHBEARER helper: file-token: the token file does not hold one JWT'
    )
    const { read } = spies()
    await expect(provider()).rejects.toThrow('file-token')
    expect(read).toHaveBeenCalledTimes(1)
    const repaired = jwt(NOW / 1000 + 3600, { sub: 'three' })
    write(repaired, 20)
    expect((await provider()).value).toBe(repaired)
  })

  test('missing or unreadable files never become cache successes', async () => {
    const provider = make()
    const missing = await provider().catch((caught) => caught)
    expect(missing.message).toBe('SASL OAUTHBEARER helper: file: the token file could not be read')
    const token = jwt(NOW / 1000 + 3600)
    write(token, 0)
    const denial = new Error(`EACCES: permission denied, open '${file}' ${MARKER}`)
    jest
      .spyOn(fs.promises, 'readFile')
      .mockRejectedValueOnce(Object.assign(denial, { code: 'EACCES' }))
    const denied = await provider().catch((caught) => caught)
    expect(denied.message).toContain('file: the token file could not be read')
    for (const error of [missing, denied]) {
      expect(error.cause).toBeUndefined()
      expect(textOf(error)).not.toContain(dir)
      expect(textOf(error)).not.toContain(MARKER)
    }
    expect((await provider()).value).toBe(token)
  })

  test('a directory in place of the token file is a read failure', async () => {
    fs.mkdirSync(file)
    await expect(make()()).rejects.toThrow('file: the token file could not be read')
  })

  test.each([
    ['opaque text', MARKER],
    ['two segments', `${segment({ alg: 'none' })}.${segment({ exp: 1 })}`],
    ['four segments', `${jwt(NOW / 1000 + 3600)}.x`],
    ['an empty header', `.${segment({ exp: NOW / 1000 + 3600 })}.`],
    ['a header that is not JSON', `bm90LWpzb24.${segment({ exp: NOW / 1000 + 3600 })}.`],
    [
      'a header that is not an object',
      `${segment(MARKER)}.${segment({ exp: NOW / 1000 + 3600 })}.`,
    ],
    ['a payload that is not JSON', `${segment({ alg: 'none' })}.bm90LWpzb24.`],
    ['an array payload', `${segment({ alg: 'none' })}.${segment([MARKER])}.`],
    ['leading whitespace', ` ${jwt(NOW / 1000 + 3600)}`],
    ['trailing whitespace', `${jwt(NOW / 1000 + 3600)} `],
    [
      'an internal line break',
      `${segment({ alg: 'none' })}.\n${segment({ exp: NOW / 1000 + 3600 })}.`,
    ],
    ['two final newlines', `${jwt(NOW / 1000 + 3600)}\n\n`],
    ['a bare carriage return', `${jwt(NOW / 1000 + 3600)}\r`],
    ['a NUL byte', `${jwt(NOW / 1000 + 3600)}\0`],
    [
      'a character outside base64url',
      `${segment({ alg: 'none' })}+.${segment({ exp: NOW / 1000 + 3600 })}.`,
    ],
  ])('a token file with %s fails without echoing bytes', async (_, content) => {
    write(content, 0)
    const error = await make()().catch((caught) => caught)
    expect(error.message).toBe(
      'SASL OAUTHBEARER helper: file-token: the token file does not hold one JWT'
    )
    expect(textOf(error)).not.toContain(MARKER)
    expect(textOf(error)).not.toContain(content.slice(0, 12))
  })

  test.each([['\n'], ['\r\n']])('one final newline %p is accepted', async (end) => {
    const token = jwt(NOW / 1000 + 3600)
    write(`${token}${end}`, 0)
    expect((await make()()).value).toBe(token)
  })

  test('expiration is checked even when modification time is unchanged', async () => {
    write(jwt(NOW / 1000 + 60), 0)
    const { read } = spies()
    const provider = make()
    await provider()
    await jest.advanceTimersByTimeAsync(29999)
    await provider()
    expect(read).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(1)
    await provider()
    expect(read).toHaveBeenCalledTimes(2)
    await provider()
    expect(read).toHaveBeenCalledTimes(3)
    await jest.advanceTimersByTimeAsync(30000)
    await expect(provider()).rejects.toThrow('expired: the token has already expired')
    expect(jest.getTimerCount()).toBe(0)
  })

  test('a JWT without an expiry is read at every authentication', async () => {
    write(`${segment({ alg: 'none' })}.${segment({ sub: 'client' })}.`, 0)
    const { read } = spies()
    const provider = make()
    await provider()
    await provider()
    expect(read).toHaveBeenCalledTimes(2)
  })

  test('failed reads do not consume a modification stamp', async () => {
    const token = jwt(NOW / 1000 + 3600)
    write(token, 0)
    const read = jest.spyOn(fs.promises, 'readFile').mockRejectedValueOnce(new Error(MARKER))
    const provider = make()
    await expect(provider()).rejects.toThrow('file')
    expect((await provider()).value).toBe(token)
    expect(read).toHaveBeenCalledTimes(2)
  })

  test.each(['stat', 'readFile'])(
    'a late file completion cannot publish after close: %s held',
    async (stage) => {
      write(jwt(NOW / 1000 + 3600), 0)
      const original = fs.promises[stage]
      const held = deferred()
      const reached = deferred()
      jest.spyOn(fs.promises, stage).mockImplementation(async (...args) => {
        reached.resolve()
        await held.promise
        return original(...args)
      })
      const provider = make()
      const calls = Array.from({ length: 5 }, () => observe(provider()))
      await reached.promise
      provider.close()
      expect(jest.getTimerCount()).toBe(0)
      held.resolve()
      await jest.advanceTimersByTimeAsync(0)
      await new Promise((resolve) => fs.stat(file, resolve))
      await jest.advanceTimersByTimeAsync(0)
      expect(calls.every((call) => call.error?.message.includes('closed'))).toBe(true)
      expect(jest.getTimerCount()).toBe(0)
      await expect(provider()).rejects.toThrow('closed')
    }
  )

  test('a change during a read is seen at the next authentication', async () => {
    const first = jwt(NOW / 1000 + 3600, { sub: 'one' })
    const second = jwt(NOW / 1000 + 3600, { sub: 'two' })
    write(first, 0)
    const original = fs.promises.readFile
    jest.spyOn(fs.promises, 'readFile').mockImplementationOnce(async (...args) => {
      const content = await original(...args)
      write(second, 10)
      return content
    })
    const provider = make()
    expect((await provider()).value).toBe(first)
    expect((await provider()).value).toBe(second)
  })

  test('a token file larger than one MiB is refused unread', async () => {
    write(jwt(NOW / 1000 + 3600), 0)
    fs.truncateSync(file, 1048577)
    const { read } = spies()
    await expect(make()()).rejects.toThrow('file-token')
    expect(read).not.toHaveBeenCalled()
  })

  test('a token file of exactly one MiB is read', async () => {
    write(jwt(NOW / 1000 + 3600), 0)
    fs.truncateSync(file, 1048576)
    const { read } = spies()
    await expect(make()()).rejects.toThrow('file-token')
    expect(read).toHaveBeenCalledTimes(1)
  })

  test('an expired token file is refused', async () => {
    write(jwt(NOW / 1000 - 1), 0)
    await expect(make()()).rejects.toThrow('expired')
  })

  test('extensions and close follow the common provider contract', async () => {
    const token = jwt(NOW / 1000 + 3600)
    write(token, 0)
    const provider = make({ extensions: { trace: 'one' } })
    expect(await provider()).toEqual({ value: token, extensions: { trace: 'one' } })
    expect(provider.close()).toBeUndefined()
    provider.close()
    await expect(provider()).rejects.toThrow('closed')
  })

  test.each([
    [{ path: '' }, 'path'],
    [{ path: null }, 'path'],
    [{ path: 5 }, 'path'],
    [{ path: 'a\0b' }, 'path'],
    [{ path: undefined }, 'path'],
    [{ path: 'access.jwt', clockSkew: -1 }, 'clockSkew'],
  ])('file token validates its options before work: %p', (options, name) => {
    const stat = jest.spyOn(fs.promises, 'stat')
    expect(() => fileToken(options)).toThrow(KafkaJSInvalidConfigurationError)
    expect(() => fileToken(options)).toThrow(`Invalid OAuth bearer helper option: ${name}`)
    expect(stat).not.toHaveBeenCalled()
  })

  test.each([[null], [undefined], ['access.jwt']])('file token refuses options %p', (options) => {
    expect(() => fileToken(options)).toThrow('Invalid OAuth bearer helper option: options')
  })
})
