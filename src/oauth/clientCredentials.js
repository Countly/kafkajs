const {
  ignore,
  invalid,
  isRecord,
  text,
  optionsOf,
  failure,
  safe,
  providerOf,
} = require('./tokenCache')

/** The hosts an `http:` endpoint may name, spelled exactly so, and only with `allowInsecure`. */
const LOOPBACK = new Set(['localhost', '127.0.0.1', '[::1]'])
/**
 * The OAuth `error` codes RFC 6749 defines (§5.2; §4.1.2.1 for the last two) that a helper error
 * may repeat.
 */
const OAUTH_ERRORS = new Set([
  'invalid_request',
  'invalid_client',
  'invalid_grant',
  'unauthorized_client',
  'unsupported_grant_type',
  'invalid_scope',
  'server_error',
  'temporarily_unavailable',
])
/** The largest token response read, in bytes. */
const BODY_LIMIT = 1048576
/** RFC 7523 §2.2: the client authenticates with a JWT assertion. */
const ASSERTION_TYPE = 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer'

/**
 * @param {string} value
 * @returns {string} the value decoded, or as given when it is not valid percent-encoding
 */
const decoded = (value) => {
  try {
    return decodeURIComponent(value)
  } catch {
    return value
  }
}

/**
 * @param {URL} url
 * @param {string|undefined} raw the endpoint as configured
 * @param {boolean} allowInsecure
 * @returns {boolean} whether the scheme is `https:`, or `http:` with the escape and a host spelled
 *   in the configured text as a literal loopback name, so an alias that URL parsing would turn into
 *   one (`127.1`, `0x7f000001`) is refused
 */
const permitted = (url, raw, allowInsecure) => {
  if (url.protocol === 'https:') return true
  const spelled = /^http:\/\/([^/?#]*)/i.exec(raw)?.[1].replace(/:\d*$/, '').toLowerCase()
  return url.protocol === 'http:' && allowInsecure && LOOPBACK.has(spelled)
}

/**
 * Validates the token endpoint and builds the label errors may name: origin and path, never the
 * query, and the path replaced when it holds the client secret.
 * @param {*} raw the `tokenEndpointUrl` option
 * @param {boolean} allowInsecure
 * @param {string|undefined} secret the client secret, if any
 * @returns {{ href: string, label: string }}
 * @throws {KafkaJSInvalidConfigurationError} on a relative or malformed URL, credentials in it, a
 *   fragment, port 0, or a scheme the rules above refuse
 */
const endpointOf = (raw, allowInsecure, secret) => {
  text(raw, 'tokenEndpointUrl')
  let url
  try {
    url = new URL(raw)
  } catch {
    invalid('tokenEndpointUrl')
  }
  if (
    url.username ||
    url.password ||
    raw.includes('#') ||
    url.port === '0' ||
    !permitted(url, raw, allowInsecure)
  ) {
    invalid('tokenEndpointUrl')
  }
  const { origin, pathname } = url
  const exposed = secret && (pathname.includes(secret) || decoded(pathname).includes(secret))
  return { href: url.href, label: `${origin}${exposed ? '/[redacted]' : pathname}` }
}

/**
 * @param {*} response what `fetch` resolved with
 * @returns {number} its HTTP status, or 0 when that is not a status code
 */
const statusOf = (response) => {
  const { status } = response
  return Number.isInteger(status) && status >= 100 && status <= 599 ? status : 0
}

/**
 * Reads a response body as UTF-8, counting bytes before keeping each chunk, and cancels the
 * reader on every path.
 * @param {ReadableStream<Uint8Array>|null|undefined} body
 * @param {Promise<never>} stopped rejects when the attempt ends
 * @param {() => void} check the retrieval's check
 * @param {object} context safe error details
 * @returns {Promise<string>} the text; empty for an absent body
 * @throws {KafkaJSSASLAuthenticationError} past 1 MiB, or on a chunk that is not bytes
 */
const readText = async (body, stopped, check, context) => {
  if (!body) return ''
  const reader = body.getReader()
  let ended = false
  let bytes = Buffer.allocUnsafe(BODY_LIMIT)
  const read = async () => {
    let size = 0
    for (;;) {
      const { done, value } = await reader.read()
      if (ended) return ''
      check()
      if (done) return bytes.subarray(0, size).toString('utf8')
      if (!(value instanceof Uint8Array)) throw failure('body', context, true)
      if (size + value.byteLength > BODY_LIMIT) throw failure('body-size', context, true)
      bytes.set(value, size)
      size += value.byteLength
    }
  }
  try {
    return await Promise.race([read(), stopped])
  } finally {
    ended = true
    bytes = null
    reader.cancel().catch(ignore)
  }
}

/**
 * @param {string} body a successful response's text
 * @param {object} context safe error details
 * @returns {{ value: string, expiresIn: * }} the access token and its `expires_in`
 * @throws {KafkaJSSASLAuthenticationError} on invalid JSON, a missing or empty `access_token`, or a
 *   `token_type` other than bearer (a missing one is accepted)
 */
const tokenOf = (body, context) => {
  let payload
  try {
    payload = JSON.parse(body)
  } catch {
    throw failure('json', context, true)
  }
  if (
    !isRecord(payload) ||
    typeof payload.access_token !== 'string' ||
    payload.access_token === ''
  ) {
    throw failure('token', context, true)
  }
  const type = payload.token_type
  if (type !== undefined && (typeof type !== 'string' || type.toLowerCase() !== 'bearer')) {
    throw failure('token-type', context, true)
  }
  return { value: payload.access_token, expiresIn: payload.expires_in }
}

/**
 * @param {number} status a refused response's status
 * @param {string} body its text, read under the same cap
 * @param {object} context safe error details
 * @returns {KafkaJSSASLAuthenticationError} a redirect is terminal; 408, 429 and 5xx are retried
 *   within the budget; the OAuth `error` code is repeated only when RFC 6749 defines it
 */
const refusalOf = (status, body, context) => {
  if (status >= 300 && status < 400) return failure('redirect', context, true)
  let code
  try {
    code = JSON.parse(body).error
  } catch {
    // A refusal that is not JSON carries no OAuth code.
  }
  const error = OAUTH_ERRORS.has(code) ? code : 'unknown'
  const retriable = status === 408 || status === 429 || status >= 500
  return failure('status', { ...context, error }, !retriable)
}

/**
 * Cancels the body of a response that arrived after its attempt ended.
 * @param {*} response
 * @returns {void}
 */
const discard = (response) => {
  try {
    response.body.cancel().catch(ignore)
  } catch {
    // A late answer without a body has nothing to release.
  }
}

/**
 * Sends one token request and reads its answer. Headers and body share one `timeout`, and the
 * retrieval's own signal ends the attempt at its deadline, so an attempt lasts at most
 * `min(timeout, remaining budget)`, also against a `fetch` that ignores its signal. The attempt's
 * signal is aborted on every path, and a late answer's body is canceled.
 * @param {{ href: string, label: string }} endpoint
 * @param {import('./tokenCache').HelperOptions} options
 * @param {{ body: string, headers: Record<string, string> }} request
 * @param {import('./tokenCache').SourceContext} context
 * @returns {Promise<{ value: string, expiresIn: * }>}
 * @throws {KafkaJSSASLAuthenticationError} sanitized: timeout and transport failures and 408, 429
 *   and 5xx are retriable, every other failure terminal
 */
const post = async (endpoint, options, request, { signal, check }) => {
  const controller = new AbortController()
  let fail
  const stopped = new Promise((_, reject) => {
    fail = reject
  })
  const end = (error) => {
    clearTimeout(timer)
    controller.abort()
    fail(error)
  }
  const timer = setTimeout(
    () => end(failure('timeout', { endpoint: endpoint.label })),
    options.timeout
  )
  const abort = () => end(signal.reason)
  signal.addEventListener('abort', abort)
  let settled = false
  const pending = (async () =>
    options.fetch(endpoint.href, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Accept: 'application/json',
        'Cache-Control': 'no-cache',
        ...request.headers,
      },
      body: request.body,
      redirect: 'manual',
      signal: controller.signal,
    }))()
  pending.then((late) => {
    if (settled) discard(late)
  }, ignore)
  try {
    const response = await Promise.race([pending, stopped])
    check()
    const status = statusOf(response)
    const context = { endpoint: endpoint.label, status }
    if (response.redirected === true) {
      discard(response)
      throw failure('redirect', context, true)
    }
    const body = await readText(response.body, stopped, check, context)
    if (status >= 200 && status < 300) return tokenOf(body, context)
    throw refusalOf(status, body, context)
  } catch (error) {
    throw safe(error, 'transport', { endpoint: endpoint.label })
  } finally {
    settled = true
    clearTimeout(timer)
    signal.removeEventListener('abort', abort)
    controller.abort()
  }
}

/**
 * Calls an application's assertion supplier inside the retrieval.
 * @param {() => string|Promise<string>} supplier
 * @returns {Promise<string>} the assertion
 * @throws {KafkaJSSASLAuthenticationError} when the supplier fails (retriable; its error is never
 *   kept) or returns anything but a nonempty string (terminal)
 */
const assertionOf = async (supplier) => {
  let value
  try {
    value = await supplier()
  } catch (error) {
    throw safe(error, 'assertion')
  }
  if (typeof value !== 'string' || value === '') throw failure('assertion', {}, true)
  return value
}

/**
 * Builds a provider that posts a form to the token endpoint, shared by both request helpers.
 * Everything is validated and copied now; nothing runs until the provider is called.
 * @param {object} input the factory's options
 * @param {import('./tokenCache').HelperOptions} options
 * @param {() => Promise<Record<string, string>>} fieldsOf the grant's fields, built inside the
 *   retrieval so that a supplier runs under its budget
 * @param {Record<string, string>} headers request headers beyond the common ones
 * @param {string|undefined} secret the client secret, kept out of error labels
 * @returns {import('../../types').OAuthBearerTokenProvider}
 * @throws {KafkaJSInvalidConfigurationError} on an invalid endpoint or scope
 */
const tokenRequest = (input, options, fieldsOf, headers, secret) => {
  const scope = input.scope === undefined ? undefined : text(input.scope, 'scope', true)
  const endpoint = endpointOf(input.tokenEndpointUrl, options.allowInsecure, secret)
  return providerOf(async (context) => {
    const fields = await fieldsOf()
    context.check()
    const body = new URLSearchParams(scope === undefined ? fields : { ...fields, scope })
    return post(endpoint, options, { body: body.toString(), headers }, context)
  }, options)
}

/**
 * @param {string} value
 * @returns {string} the value form-encoded, as RFC 6749 §2.3.1 requires before HTTP Basic
 */
const formEncoded = (value) => new URLSearchParams({ value }).toString().slice('value='.length)

/**
 * Builds the existing `oauthBearerProvider` callback for the client-credentials grant (RFC 6749
 * §4.4, KIP-768), over one single-flight token cache. With `clientSecret` the request carries HTTP
 * Basic authentication; with `clientAssertion` (RFC 7523 §2.2, KIP-1258) it carries the supplied
 * assertion and the client id instead, and the supplier is called once per retrieval attempt.
 * @param {object} input see `oauthBearer.clientCredentials` in the type declarations
 * @returns {import('../../types').OAuthBearerTokenProvider} the callback, with `close()`
 * @throws {KafkaJSInvalidConfigurationError} naming the first invalid option, before any work
 */
const clientCredentials = (input) => {
  const options = optionsOf(input)
  const clientId = text(input.clientId, 'clientId')
  const { clientSecret, clientAssertion } = input
  if ((clientSecret === undefined) === (clientAssertion === undefined)) {
    invalid('clientSecret or clientAssertion')
  }
  if (clientAssertion !== undefined) {
    if (typeof clientAssertion !== 'function') invalid('clientAssertion')
    return tokenRequest(
      input,
      options,
      async () => ({
        grant_type: 'client_credentials',
        client_assertion_type: ASSERTION_TYPE,
        client_assertion: await assertionOf(clientAssertion),
        client_id: clientId,
      }),
      {},
      undefined
    )
  }
  const credentials = `${formEncoded(clientId)}:${formEncoded(text(clientSecret, 'clientSecret'))}`
  return tokenRequest(
    input,
    options,
    async () => ({ grant_type: 'client_credentials' }),
    { Authorization: `Basic ${Buffer.from(credentials).toString('base64')}` },
    clientSecret
  )
}

module.exports = { clientCredentials, tokenRequest, assertionOf }
