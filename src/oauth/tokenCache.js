const { KafkaJSInvalidConfigurationError, KafkaJSSASLAuthenticationError } = require('../errors')

/** The longest delay a Node timer accepts, and the upper bound of every integral option. */
const MAX_INTEGER = 2147483647

/** What each retrieval stage reports. A helper error carries only these local texts. */
const STAGES = {
  closed: 'the provider is closed',
  settled: 'the retrieval has already settled',
  deadline: 'the retrieval budget ran out',
  source: 'the token source failed',
  token: 'the token response has no access token',
  expired: 'the token has already expired',
  assertion: 'the assertion supplier failed',
  timeout: 'a token request timed out',
  transport: 'a token request could not be completed',
  redirect: 'the token endpoint answered with a redirect',
  status: 'the token endpoint refused the request',
  body: 'the token response body is not bytes',
  'body-size': 'the token response is larger than 1 MiB',
  json: 'the token response is not JSON',
  'token-type': 'the token type is not bearer',
  file: 'the token file could not be read',
  'file-token': 'the token file does not hold one JWT',
  key: 'the signing key file could not be loaded',
}

/** Errors this module built, so a supplier's error of the same class is never trusted. */
const trusted = new WeakSet()
/** Built errors that end an acquisition at once instead of being retried. */
const terminal = new WeakSet()

/**
 * Ignores a rejection that has already been reported another way.
 * @returns {void}
 */
const ignore = () => {}

/**
 * @template T
 * @param {T|undefined} value an option value, `undefined` meaning unset
 * @param {T} fallback the default
 * @returns {T} the value, or the default when unset
 */
const or = (value, fallback) => (value === undefined ? fallback : value)

/**
 * @param {string} name the option that is invalid
 * @returns {never}
 * @throws {KafkaJSInvalidConfigurationError} always, naming the option and never its value
 */
const invalid = (name) => {
  throw new KafkaJSInvalidConfigurationError(`Invalid OAuth bearer helper option: ${name}`)
}

/**
 * @param {*} value
 * @returns {boolean} whether the value is a non-null, non-array object
 */
const isRecord = (value) => value !== null && typeof value === 'object' && !Array.isArray(value)

/**
 * @param {*} value
 * @param {string} name the option, for the error
 * @param {boolean} [empty=false] whether the empty string is accepted
 * @returns {string} the value
 * @throws {KafkaJSInvalidConfigurationError} unless a string without NUL, nonempty unless allowed
 */
const text = (value, name, empty = false) => {
  if (typeof value !== 'string' || (!empty && value === '') || value.includes('\0')) invalid(name)
  return value
}

/**
 * @param {*} value
 * @param {string} name the option, for the error
 * @param {number} min the smallest accepted value, 0 or 1
 * @returns {number} the value
 * @throws {KafkaJSInvalidConfigurationError} unless an integer from `min` to 2147483647
 */
const integer = (value, name, min) => {
  if (!Number.isInteger(value) || value < min || value > MAX_INTEGER) invalid(name)
  return value
}

/**
 * @param {*} input the `extensions` option
 * @returns {Record<string, string>|undefined} a plain copy of its own string entries
 * @throws {KafkaJSInvalidConfigurationError} unless a record of nonempty keys and string values,
 *   neither holding NUL
 */
const extensionsOf = (input) => {
  if (input === undefined) return undefined
  if (!isRecord(input)) invalid('extensions')
  return Object.fromEntries(
    Object.keys(input).map((key) => [text(key, 'extensions'), text(input[key], 'extensions', true)])
  )
}

/**
 * The validated options every helper shares, copied so later changes to the caller's object have
 * no effect.
 * @typedef {object} HelperOptions
 * @property {number} timeout ms one HTTP attempt may take, headers and body included
 * @property {number} maxRetryTime ms one retrieval may take, every stage included
 * @property {{ initial: number, max: number }} retry the backoff between attempts, in ms
 * @property {number} refreshWindow the fraction of a lifetime after which to refresh
 * @property {number} refreshJitter the random spread around that fraction
 * @property {number} minRefreshPeriod ms after a fetch before a proactive refresh may start
 * @property {number} refreshBuffer ms before expiry by which to refresh where possible
 * @property {number} clockSkew ms before `exp` at which a token counts as expired
 * @property {Record<string, string>|undefined} extensions copied into every response
 * @property {boolean} allowInsecure whether a literal loopback `http:` endpoint is accepted
 * @property {typeof fetch} fetch the transport
 */

/**
 * Validates and copies the common helper options, before any work starts. `undefined` means unset.
 * @param {*} input the factory's options object
 * @returns {HelperOptions}
 * @throws {KafkaJSInvalidConfigurationError} naming the first invalid option
 */
const optionsOf = (input) => {
  if (!isRecord(input)) invalid('options')
  const refreshWindow = or(input.refreshWindow, 0.8)
  const refreshJitter = or(input.refreshJitter, 0.05)
  if (typeof refreshWindow !== 'number' || !(refreshWindow > 0 && refreshWindow < 1)) {
    invalid('refreshWindow')
  }
  if (
    typeof refreshJitter !== 'number' ||
    !(refreshJitter >= 0 && refreshWindow - refreshJitter > 0 && refreshWindow + refreshJitter < 1)
  ) {
    invalid('refreshJitter')
  }
  const retry = or(input.retry, {})
  if (!isRecord(retry)) invalid('retry')
  const initial = integer(or(retry.initial, 100), 'retry.initial', 1)
  const max = integer(or(retry.max, 10000), 'retry.max', 1)
  if (max < initial) invalid('retry.max')
  const allowInsecure = or(input.allowInsecure, false)
  if (typeof allowInsecure !== 'boolean') invalid('allowInsecure')
  const fetch = or(input.fetch, globalThis.fetch)
  if (typeof fetch !== 'function') invalid('fetch')
  return {
    timeout: integer(or(input.timeout, 5000), 'timeout', 1),
    maxRetryTime: integer(or(input.maxRetryTime, 8000), 'maxRetryTime', 1),
    retry: { initial, max },
    refreshWindow,
    refreshJitter,
    minRefreshPeriod: integer(or(input.minRefreshPeriod, 60000), 'minRefreshPeriod', 1),
    refreshBuffer: integer(or(input.refreshBuffer, 300000), 'refreshBuffer', 0),
    clockSkew: integer(or(input.clockSkew, 30000), 'clockSkew', 0),
    extensions: extensionsOf(input.extensions),
    allowInsecure,
    fetch,
  }
}

/**
 * Builds a helper error from local text only: an endpoint's origin and path, a numeric status and
 * an allowlisted OAuth error code at most. No caller data, body, credential or cause enters it.
 * @param {keyof typeof STAGES} stage where the retrieval failed
 * @param {{ endpoint?: string, status?: number, error?: string }} [context] safe details
 * @param {boolean} [final=false] whether the acquisition ends at once rather than retrying
 * @returns {KafkaJSSASLAuthenticationError}
 */
const failure = (stage, context = {}, final = false) => {
  const details = ['endpoint', 'status', 'error']
    .filter((name) => context[name] !== undefined)
    .map((name) => `${name}=${context[name]}`)
  const suffix = details.length ? ` (${details.join(' ')})` : ''
  const error = new KafkaJSSASLAuthenticationError(
    `SASL OAUTHBEARER helper: ${stage}: ${STAGES[stage]}${suffix}`
  )
  trusted.add(error)
  if (final) terminal.add(error)
  return error
}

/**
 * @param {*} error anything a stage threw
 * @param {keyof typeof STAGES} stage the stage reported when the error was not built here
 * @param {object} [context] safe details for that report
 * @returns {KafkaJSSASLAuthenticationError} the error itself when built here, else a new one
 */
const safe = (error, stage, context) => (trusted.has(error) ? error : failure(stage, context))

/**
 * @param {import('fs').BigIntStats} stat
 * @returns {string} the file's identity: device, inode, size and the nanosecond modification and
 *   change times, so an in-place rewrite and an atomic replacement both change it
 */
const fileVersion = (stat) => `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`

/** @returns {number} monotonic milliseconds; wall-clock changes never move it */
const monotonic = () => Number(process.hrtime.bigint()) / 1e6

/**
 * Reads the `exp` claim of a JWT-shaped token without verifying it; the broker verifies tokens.
 * @param {string} value the access token
 * @returns {number|undefined} the expiry in wall-clock ms, or undefined when there is none
 */
const expiryOf = (value) => {
  try {
    const { exp } = JSON.parse(Buffer.from(value.split('.')[1], 'base64url').toString('utf8'))
    return typeof exp === 'number' && Number.isFinite(exp * 1000) ? exp * 1000 : undefined
  } catch {
    return undefined
  }
}

/**
 * @param {*} expiresIn the `expires_in` of a token response, in seconds
 * @param {number} now the wall-clock ms the token arrived
 * @returns {number|undefined} the expiry in ms, when the lifetime is a finite nonnegative number
 */
const lifetimeOf = (expiresIn, now) =>
  typeof expiresIn === 'number' && expiresIn >= 0 && Number.isFinite(expiresIn * 1000)
    ? now + expiresIn * 1000
    : undefined

/**
 * What a source resolves with.
 * @typedef {object} TokenResult
 * @property {string} value the access token
 * @property {*} [expiresIn] the lifetime in seconds an opaque token was issued with
 * @property {string} [version] the identity of the file the token was read from
 */

/**
 * The cache's one record of a token.
 * @typedef {object} TokenRecord
 * @property {string} value
 * @property {number|undefined} exp wall-clock ms the token expires at
 * @property {number} fetched wall-clock ms the token was retrieved at
 * @property {number} refreshAt wall-clock ms of its proactive refresh
 * @property {string|undefined} version the file identity it was read under
 */

/**
 * What a source is called with, once per attempt.
 * @typedef {object} SourceContext
 * @property {AbortSignal} signal aborted when the retrieval ends: closed, out of budget or settled
 * @property {() => void} check throws the retrieval's failure once it has ended; call it after
 *   every await, before starting more work
 * @property {TokenRecord|null} previous the cache's current record
 * @property {boolean} forceRead whether the cache will not keep `previous` as it is: it is
 *   missing, expired or due for refresh
 */

/**
 * @callback TokenSource
 * @param {SourceContext} context
 * @returns {Promise<TokenResult>} the token; resolving with `context.previous` itself keeps the
 *   current record, its expiry and its refresh point unchanged
 */

/**
 * One retrieval: its absolute monotonic deadline, its abort signal, its backoff and its timers.
 * Every caller that joins it shares this one deadline.
 */
class Flight {
  /**
   * Starts the deadline timer.
   * @param {number} allowance ms the retrieval may take
   * @param {number} delay ms of the first backoff
   */
  constructor(allowance, delay) {
    this.controller = new AbortController()
    this.end = monotonic() + allowance
    this.delay = delay
    this.error = null
    this.pause = null
    this.stopped = new Promise((_, reject) => {
      this.reject = reject
    })
    this.timer = setTimeout(() => this.stop(failure('deadline')), allowance)
  }

  /**
   * @returns {void}
   * @throws {KafkaJSSASLAuthenticationError} once the retrieval has stopped or its deadline has
   *   passed, whether or not the deadline timer has run yet
   */
  check() {
    if (this.error) throw this.error
    if (monotonic() >= this.end) throw failure('deadline')
  }

  /** @returns {number} ms left before the deadline, never negative */
  remaining() {
    return Math.max(0, this.end - monotonic())
  }

  /**
   * @param {number} ms
   * @returns {Promise<void>} resolves after the backoff; never resolves once the retrieval stops
   */
  sleep(ms) {
    return new Promise((resolve) => {
      this.pause = setTimeout(resolve, ms)
    })
  }

  /**
   * Ends the retrieval: clears its timers, aborts its signal and settles its waiters with the
   * error. A later call only changes what detached work's next check throws.
   * @param {KafkaJSSASLAuthenticationError} error
   * @returns {void}
   */
  stop(error) {
    this.error = error
    clearTimeout(this.timer)
    clearTimeout(this.pause)
    this.controller.abort(error)
    this.reject(error)
  }
}

/** Marks a retrieval whose waiters have settled, so its detached work stops at the next check. */
const SETTLED = failure('settled')

/**
 * One provider's token cache: its single token record, its single retrieval in flight, and the
 * refresh and retry timer. A retrieval is single-flight: every concurrent caller joins it and
 * shares its result, its error object and its deadline.
 *
 * Expiry comes from a JWT `exp` when present, else from `expires_in`; a token without a usable
 * lifetime serves the callers of its retrieval and is not kept. A kept token is refreshed in the
 * background at the earlier of `fetched + lifetime × (refreshWindow ± refreshJitter)` and
 * `exp − refreshBuffer`, never before `fetched + minRefreshPeriod`; a failed background refresh
 * keeps the valid token and retries with backoff until its skew-adjusted expiry.
 */
class TokenCache {
  /**
   * @param {TokenSource} source
   * @param {HelperOptions} options
   * @param {boolean} revalidate whether every call asks the source again, for a source that can
   *   change before the token expires; a revalidating cache never refreshes in the background
   */
  constructor(source, options, revalidate) {
    this.source = source
    this.options = options
    this.revalidate = revalidate
    this.closed = false
    this.token = null
    this.flight = null
    this.timer = null
    this.delay = options.retry.initial
  }

  /**
   * @returns {Promise<{ value: string, extensions?: Record<string, string> }>} a new response
   *   object per call: from the cache while the token is usable, else from the shared retrieval
   * @throws {KafkaJSSASLAuthenticationError} (as a rejection) when closed or the retrieval fails
   */
  get() {
    if (this.closed) return Promise.reject(failure('closed'))
    if (!this.revalidate && this.usable()) return Promise.resolve(this.response(this.token))
    const flight = this.flight || this.start(false)
    return flight.promise.then((record) => this.response(record))
  }

  /**
   * Closes the cache: drops the token, clears the timers, aborts the retrieval in flight and
   * rejects its waiters. Synchronous and idempotent; later calls to `get` reject.
   * @returns {void}
   */
  close() {
    if (this.closed) return
    this.closed = true
    this.token = null
    clearTimeout(this.timer)
    this.flight?.stop(failure('closed'))
  }

  /** @returns {boolean} whether a token is kept and not yet `clockSkew` from its expiry */
  usable() {
    return this.token !== null && Date.now() < this.token.exp - this.options.clockSkew
  }

  /**
   * @param {TokenRecord} record
   * @returns {{ value: string, extensions?: Record<string, string> }} a fresh object, with a fresh
   *   copy of the extensions
   */
  response(record) {
    const { extensions } = this.options
    return extensions === undefined
      ? { value: record.value }
      : { value: record.value, extensions: { ...extensions } }
  }

  /**
   * Starts a retrieval and disarms the refresh timer, so only one retrieval can be in flight. A
   * background one is bounded by the token's usable expiry as well as by `maxRetryTime`, and
   * continues the backoff its predecessor reached.
   * @param {boolean} background whether a refresh timer started it while the token is usable
   * @returns {Flight}
   */
  start(background) {
    const { maxRetryTime, clockSkew, retry } = this.options
    clearTimeout(this.timer)
    const flight = new Flight(
      background ? Math.min(maxRetryTime, this.token.exp - clockSkew - Date.now()) : maxRetryTime,
      background ? this.delay : retry.initial
    )
    const context = {
      signal: flight.controller.signal,
      check: () => flight.check(),
      previous: this.token,
      forceRead: !this.usable() || Date.now() >= this.token.refreshAt,
    }
    this.flight = flight
    flight.promise = Promise.race([this.run(flight, context), flight.stopped]).finally(() => {
      this.flight = null
      flight.stop(SETTLED)
    })
    flight.promise.catch(() => {
      if (background) this.retryLater(flight.delay)
    })
    return flight
  }

  /**
   * Calls the source until it succeeds, it fails terminally, or the next backoff would not end
   * before the deadline. Every await is followed by the flight's check.
   * @param {Flight} flight
   * @param {SourceContext} context
   * @returns {Promise<TokenRecord>}
   */
  async run(flight, context) {
    for (;;) {
      try {
        const result = await this.source(context)
        flight.check()
        return this.apply(result)
      } catch (error) {
        flight.check()
        if (terminal.has(error) || flight.remaining() <= flight.delay) throw safe(error, 'source')
        await flight.sleep(flight.delay)
        flight.check()
        flight.delay = Math.min(flight.delay * 2, this.options.retry.max)
      }
    }
  }

  /**
   * Records a source's result. A JWT that has already expired is refused; a token whose lifetime
   * is missing, zero or inside `clockSkew` is returned without being kept.
   * @param {TokenResult} result
   * @returns {TokenRecord}
   * @throws {KafkaJSSASLAuthenticationError} when the result has no token or has expired
   */
  apply(result) {
    if (!isRecord(result) || typeof result.value !== 'string' || result.value === '') {
      throw failure('token', {}, true)
    }
    if (result === this.token) return result
    const now = Date.now()
    const claimed = expiryOf(result.value)
    if (claimed <= now) throw failure('expired', {}, true)
    const exp = claimed === undefined ? lifetimeOf(result.expiresIn, now) : claimed
    const record = { value: result.value, version: result.version, exp, fetched: now, refreshAt: 0 }
    this.delay = this.options.retry.initial
    this.token = exp - this.options.clockSkew > now ? record : null
    if (this.token) this.schedule(record)
    return record
  }

  /**
   * Records the refresh point of a new token and, unless revalidating, arms its timer when the
   * point comes before the token stops being usable.
   * @param {TokenRecord} record
   * @returns {void}
   */
  schedule(record) {
    const { refreshWindow, refreshJitter, refreshBuffer, minRefreshPeriod, clockSkew } =
      this.options
    const { fetched, exp } = record
    const factor = refreshWindow + (2 * Math.random() - 1) * refreshJitter
    record.refreshAt = Math.floor(
      Math.max(
        fetched + minRefreshPeriod,
        Math.min(fetched + (exp - fetched) * factor, exp - refreshBuffer)
      )
    )
    if (!this.revalidate && record.refreshAt < exp - clockSkew) this.arm(record.refreshAt)
  }

  /**
   * After a failed background retrieval, arms the next one after its backoff while the token
   * stays usable; otherwise the chain stops and the next call fetches on demand.
   * @param {number} delay ms the failed retrieval would have waited next
   * @returns {void}
   */
  retryLater(delay) {
    const due = Date.now() + delay
    if (this.usable() && due < this.token.exp - this.options.clockSkew) {
      this.delay = Math.min(delay * 2, this.options.retry.max)
      this.arm(due)
    }
  }

  /**
   * Arms the one unref'd timer for a wall-clock time. A delay longer than a Node timer allows is
   * armed in parts, each part only re-arming until the time is reached.
   * @param {number} due wall-clock ms
   * @returns {void}
   */
  arm(due) {
    clearTimeout(this.timer)
    this.timer = setTimeout(() => this.wake(due), Math.min(due - Date.now(), MAX_INTEGER))
    this.timer.unref()
  }

  /**
   * @param {number} due the wall-clock ms the timer was armed for
   * @returns {void}
   */
  wake(due) {
    if (Date.now() < due) return this.arm(due)
    if (this.usable()) this.start(true)
  }
}

/**
 * Builds the existing `oauthBearerProvider` callback over one new cache, with its `close()`.
 * @param {TokenSource} source
 * @param {HelperOptions} options
 * @param {boolean} [revalidate=false] see {@link TokenCache}
 * @returns {import('../../types').OAuthBearerTokenProvider} the callback, with `close()`
 */
const providerOf = (source, options, revalidate = false) => {
  const cache = new TokenCache(source, options, revalidate)
  const provider = () => cache.get()
  provider.close = () => cache.close()
  return provider
}

module.exports = {
  ignore,
  or,
  invalid,
  isRecord,
  text,
  integer,
  optionsOf,
  failure,
  safe,
  fileVersion,
  providerOf,
}
