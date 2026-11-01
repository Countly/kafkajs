const http = require('node:http')
const { ReadableStream } = require('node:stream/web')
const { setTimeout: realDelay } = require('node:timers/promises')
const { clientCredentials } = require('../clientCredentials')
const { KafkaJSSASLAuthenticationError, KafkaJSInvalidConfigurationError } = require('../../errors')

const MARKER = 'inert-marker'
const SECRET = 'inert-secret'
const ASSERTION_FORM =
  'grant_type=client_credentials&client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer&client_assertion=header.payload.signature&client_id=client'
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
const json = (body, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })
const streamOf = (chunks, { hold = false, onCancel = () => {} } = {}) =>
  new ReadableStream({
    start(controller) {
      chunks.forEach((chunk) => controller.enqueue(chunk))
      if (!hold) controller.close()
    },
    cancel: onCancel,
  })

describe('OAuth client credentials', () => {
  let servers
  const providers = []

  /** Starts an in-process token endpoint on 127.0.0.1 and records every request it reads. */
  const listen = async (answer) => {
    const endpoint = { requests: [], sockets: new Set(), answer }
    endpoint.server = http.createServer(async (request, response) => {
      const chunks = []
      for await (const chunk of request) chunks.push(chunk)
      endpoint.requests.push({
        method: request.method,
        url: request.url,
        headers: request.headers,
        body: Buffer.concat(chunks).toString(),
      })
      endpoint.answer(request, response)
    })
    endpoint.server.on('connection', (socket) => {
      endpoint.sockets.add(socket)
      socket.once('close', () => endpoint.sockets.delete(socket))
    })
    await new Promise((resolve) => endpoint.server.listen(0, '127.0.0.1', resolve))
    endpoint.url = `http://127.0.0.1:${endpoint.server.address().port}/token`
    servers.push(endpoint)
    return endpoint
  }
  const tokenAnswer = (_request, response) => {
    response.writeHead(200, { 'Content-Type': 'application/json' })
    response.end(
      JSON.stringify({ access_token: 'opaque-token', expires_in: 3600, token_type: 'Bearer' })
    )
  }
  const make = (options) => {
    const provider = clientCredentials({
      clientId: 'client',
      clientSecret: SECRET,
      allowInsecure: true,
      ...options,
    })
    providers.push(provider)
    return provider
  }

  beforeEach(() => {
    servers = []
    jest.useFakeTimers({ now: 0 })
    jest.spyOn(Math, 'random').mockReturnValue(0.5)
  })

  afterEach(async () => {
    providers.splice(0).forEach((provider) => provider.close())
    await jest.advanceTimersByTimeAsync(0)
    const timers = jest.getTimerCount()
    jest.restoreAllMocks()
    jest.useRealTimers()
    for (const { server, sockets } of servers) {
      const closed = [...sockets].map(
        (socket) => new Promise((resolve) => socket.once('close', resolve))
      )
      await new Promise((resolve) => server.close(resolve))
      await Promise.all(closed)
      expect(sockets.size).toBe(0)
    }
    expect(timers).toBe(0)
  })

  test('a transport that followed a redirect is refused and its body canceled', async () => {
    const response = json({ access_token: 'redirected-token' })
    Object.defineProperty(response, 'redirected', { value: true })
    const cancel = jest.spyOn(response.body, 'cancel')
    const provider = make({
      tokenEndpointUrl: 'https://issuer.example/token',
      fetch: async () => response,
    })
    await expect(provider()).rejects.toThrow(/redirect/)
    expect(cancel).toHaveBeenCalledTimes(1)
  })

  test('streamed bytes are copied before a transport reuses its backing buffer', async () => {
    const body = Buffer.from(JSON.stringify({ access_token: 'copied-token' }))
    const chunk = new Uint8Array(1)
    let index = 0
    const provider = make({
      tokenEndpointUrl: 'https://issuer.example/token',
      fetch: async () => ({
        status: 200,
        body: {
          getReader: () => ({
            read: async () => {
              if (index === body.length) return { done: true }
              chunk[0] = body[index++]
              return { done: false, value: chunk }
            },
            cancel: async () => {},
          }),
        },
      }),
    })
    await expect(provider()).resolves.toEqual({ value: 'copied-token' })
  })

  test('many body chunks do not accumulate reactions on the attempt stop promise', async () => {
    const counts = new Map()
    const then = Promise.prototype.then
    const spy = jest.spyOn(Promise.prototype, 'then').mockImplementation(function (...args) {
      counts.set(this, (counts.get(this) || 0) + 1)
      return then.apply(this, args)
    })
    const bytes = Buffer.from(JSON.stringify({ access_token: 'bounded-token' }))
    const provider = make({
      tokenEndpointUrl: 'https://issuer.example/token',
      fetch: async () => ({
        status: 200,
        body: streamOf([...Array.from({ length: 1000 }, () => new Uint8Array(0)), bytes]),
      }),
    })
    try {
      await expect(provider()).resolves.toEqual({ value: 'bounded-token' })
      expect(Math.max(...counts.values())).toBeLessThan(10)
    } finally {
      spy.mockRestore()
    }
  })

  test('fifty callers make one POST to the token endpoint', async () => {
    const endpoint = await listen(tokenAnswer)
    const provider = make({ tokenEndpointUrl: endpoint.url })
    const results = await Promise.all(Array.from({ length: 50 }, () => provider()))
    expect(endpoint.requests).toHaveLength(1)
    expect(endpoint.requests[0].method).toBe('POST')
    expect(results.every((result) => result.value === 'opaque-token')).toBe(true)
  })

  test('encodes credentials before Basic and scope in the form', async () => {
    const endpoint = await listen(tokenAnswer)
    const provider = make({
      tokenEndpointUrl: `${endpoint.url}?tenant=one`,
      clientId: 'a:b +é',
      clientSecret: 's&:/ +',
      scope: 'read write+é',
    })
    await provider()
    const [captured] = endpoint.requests
    expect(captured.url).toBe('/token?tenant=one')
    expect(captured.headers.authorization).toBe('Basic YSUzQWIrJTJCJUMzJUE5OnMlMjYlM0ElMkYrJTJC')
    expect(captured.headers['content-type']).toBe('application/x-www-form-urlencoded')
    expect(captured.headers.accept).toBe('application/json')
    expect(captured.headers['cache-control']).toBe('no-cache')
    expect(captured.body).toBe('grant_type=client_credentials&scope=read+write%2B%C3%A9')
    expect(captured.body).not.toContain('s%26')
  })

  test('the secret form without a scope sends only its grant type', async () => {
    const endpoint = await listen(tokenAnswer)
    await make({ tokenEndpointUrl: endpoint.url })()
    expect(endpoint.requests[0].body).toBe('grant_type=client_credentials')
    expect(endpoint.requests[0].headers.authorization).toBe(
      `Basic ${Buffer.from(`client:${SECRET}`).toString('base64')}`
    )
  })

  test.each([
    ['synchronous', () => 'header.payload.signature'],
    ['asynchronous', async () => 'header.payload.signature'],
  ])('assertion credentials omit the Basic header: %s supplier', async (_, supplier) => {
    const endpoint = await listen(tokenAnswer)
    const clientAssertion = jest.fn(supplier)
    const provider = make({
      tokenEndpointUrl: endpoint.url,
      clientSecret: undefined,
      clientAssertion,
      scope: 'read write',
    })
    await provider()
    const [captured] = endpoint.requests
    expect(captured.headers.authorization).toBeUndefined()
    expect(captured.body).toBe(`${ASSERTION_FORM}&scope=read+write`)
    expect(captured.body).not.toContain('client_secret')
    expect(clientAssertion).toHaveBeenCalledTimes(1)
  })

  test('a supplier is evaluated once per shared acquisition', async () => {
    const endpoint = await listen(tokenAnswer)
    const held = deferred()
    const clientAssertion = jest
      .fn()
      .mockReturnValueOnce(held.promise)
      .mockReturnValue('second.assertion.value')
    const provider = make({
      tokenEndpointUrl: endpoint.url,
      clientSecret: undefined,
      clientAssertion,
    })
    const calls = Array.from({ length: 50 }, () => provider())
    await jest.advanceTimersByTimeAsync(0)
    expect(clientAssertion).toHaveBeenCalledTimes(1)
    held.resolve('header.payload.signature')
    await Promise.all(calls)
    expect(endpoint.requests).toHaveLength(1)
    await provider()
    expect(clientAssertion).toHaveBeenCalledTimes(1)
    const refreshed = new Promise((resolve) => {
      endpoint.answer = (request, response) => {
        tokenAnswer(request, response)
        resolve()
      }
    })
    await jest.advanceTimersByTimeAsync(2880000)
    await refreshed
    expect(clientAssertion).toHaveBeenCalledTimes(2)
    expect(endpoint.requests[1].body).toContain('client_assertion=second.assertion.value')
  })

  test.each(['deadline', 'close'])(
    'late assertion completion cannot issue HTTP after the %s',
    async (ending) => {
      const endpoint = await listen(tokenAnswer)
      const held = deferred()
      const provider = make({
        tokenEndpointUrl: endpoint.url,
        clientSecret: undefined,
        clientAssertion: () => held.promise,
      })
      const call = observe(provider())
      if (ending === 'close') provider.close()
      else await jest.advanceTimersByTimeAsync(8000)
      await jest.advanceTimersByTimeAsync(0)
      expect(call.error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
      held.resolve('header.payload.signature')
      await jest.advanceTimersByTimeAsync(0)
      await realDelay(50)
      expect(endpoint.requests).toHaveLength(0)
      expect(endpoint.sockets.size).toBe(0)
    }
  )

  test.each([
    [400, true],
    [401, true],
    [403, true],
    [408, false],
    [429, false],
    [500, false],
    [503, false],
  ])(
    'status %s errors contain only the sanitized endpoint status and code',
    async (status, terminal) => {
      const starts = []
      const fetch = jest.fn(async () => {
        starts.push(monotonicMs())
        return new Response(`${SECRET} ${MARKER} body`, { status })
      })
      const provider = make({
        tokenEndpointUrl: `https://issuer.example/token?query=${MARKER}`,
        fetch,
      })
      const call = observe(provider())
      await jest.advanceTimersByTimeAsync(8000)
      expect(call.error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(call.error.message).toBe(
        `SASL OAUTHBEARER helper: status: the token endpoint refused the request (endpoint=https://issuer.example/token status=${status} error=unknown)`
      )
      expect(textOf(call.error)).not.toMatch(new RegExp(`${SECRET}|${MARKER}`))
      expect(call.error.cause).toBeUndefined()
      expect(starts).toEqual(terminal ? [0] : [0, 100, 300, 700, 1500, 3100, 6300])
    }
  )

  test('OAuth error codes are allowlisted before diagnostics', async () => {
    const answers = [
      json({ error: 'invalid_client', error_description: MARKER }, 401),
      json({ error: `${MARKER}_code` }, 400),
      json({ error: { nested: MARKER } }, 400),
      json(null, 400),
    ]
    const fetch = jest.fn(async () => answers.shift())
    const errors = []
    for (let index = 0; index < 4; index++) {
      errors.push(
        await make({ tokenEndpointUrl: 'https://issuer.example/token', fetch })().catch(
          (error) => error
        )
      )
    }
    expect(errors.map((error) => error.message.match(/error=(\w+)/)[1])).toEqual([
      'invalid_client',
      'unknown',
      'unknown',
      'unknown',
    ])
    expect(errors.every((error) => !textOf(error).includes(MARKER))).toBe(true)
  })

  test('non JSON responses do not expose their text', async () => {
    const answers = [
      new Response(`not json ${MARKER}`),
      new Response(`not json ${MARKER}`, { status: 401 }),
    ]
    const fetch = async () => answers.shift()
    const ok = await make({ tokenEndpointUrl: 'https://issuer.example/token', fetch })().catch(
      (error) => error
    )
    const refused = await make({ tokenEndpointUrl: 'https://issuer.example/token', fetch })().catch(
      (error) => error
    )
    expect(ok.message).toBe(
      'SASL OAUTHBEARER helper: json: the token response is not JSON (endpoint=https://issuer.example/token status=200)'
    )
    expect(refused.message).toContain('status=401 error=unknown')
    expect(textOf(ok) + textOf(refused)).not.toContain(MARKER)
  })

  test.each([
    ['an empty object', {}],
    ['an empty token', { access_token: '' }],
    ['a null token', { access_token: null }],
    ['a numeric token', { access_token: 5 }],
    ['an array token', { access_token: ['token'] }],
    ['a JSON null', null],
    ['a JSON array', ['opaque-token']],
  ])('missing and invalid access tokens fail typed: %s', async (_, body) => {
    const fetch = jest.fn(async () => json(body))
    const provider = make({ tokenEndpointUrl: 'https://issuer.example/token', fetch })
    const error = await provider().catch((caught) => caught)
    expect(error.message).toBe(
      'SASL OAUTHBEARER helper: token: the token response has no access token (endpoint=https://issuer.example/token status=200)'
    )
    expect(fetch).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
  })

  test.each([['Bearer'], ['bearer'], ['BEARER'], [undefined]])(
    'token type %p is accepted',
    async (tokenType) => {
      const fetch = async () => json({ access_token: 'opaque-token', token_type: tokenType })
      expect(await make({ tokenEndpointUrl: 'https://issuer.example/token', fetch })()).toEqual({
        value: 'opaque-token',
      })
    }
  )

  test.each([['mac'], [null], [5]])('token type %p is refused', async (tokenType) => {
    const fetch = async () => json({ access_token: 'opaque-token', token_type: tokenType })
    await expect(
      make({ tokenEndpointUrl: 'https://issuer.example/token', fetch })()
    ).rejects.toThrow('token-type')
  })

  test('unknown response members are not retained', async () => {
    const fetch = async () =>
      json({
        access_token: 'opaque-token',
        refresh_token: MARKER,
        id_token: MARKER,
        error_description: MARKER,
        nested: { value: MARKER },
      })
    const provider = make({
      tokenEndpointUrl: 'https://issuer.example/token',
      fetch,
      extensions: { trace: 'one' },
    })
    const response = await provider()
    expect(response).toEqual({ value: 'opaque-token', extensions: { trace: 'one' } })
    expect(textOf(response)).not.toContain(MARKER)
  })

  test.each([301, 302, 303, 307, 308])(
    'a %s redirect never forwards credentials',
    async (status) => {
      const target = await listen(tokenAnswer)
      const origin = await listen((_request, response) => {
        response.writeHead(status, { Location: target.url })
        response.end()
      })
      const error = await make({ tokenEndpointUrl: origin.url })().catch((caught) => caught)
      expect(error.message).toBe(
        `SASL OAUTHBEARER helper: redirect: the token endpoint answered with a redirect (endpoint=${new URL(origin.url).origin}/token status=${status})`
      )
      expect(origin.requests).toHaveLength(1)
      expect(target.requests).toHaveLength(0)
    }
  )

  test('an injected fetch is asked not to follow redirects', async () => {
    const fetch = jest.fn(async () => json({ access_token: 'opaque-token' }))
    await make({ tokenEndpointUrl: 'https://issuer.example/token', fetch })()
    expect(fetch.mock.calls[0][0]).toBe('https://issuer.example/token')
    expect(fetch.mock.calls[0][1]).toMatchObject({ method: 'POST', redirect: 'manual' })
  })

  test('a body larger than one MiB is aborted before buffering more', async () => {
    const endpoint = await listen((_request, response) => {
      response.writeHead(200, { 'Content-Type': 'application/json' })
      const chunk = Buffer.alloc(65536, 'x')
      for (let index = 0; index < 16; index++) response.write(chunk)
      response.write('x')
      response.end('"}')
    })
    const error = await make({ tokenEndpointUrl: endpoint.url })().catch((caught) => caught)
    expect(error.message).toContain('body-size: the token response is larger than 1 MiB')
    expect(error.message).toContain('status=200')
  })

  test.each([
    ['absent', undefined],
    ['falsely small', '10'],
  ])('a large body is bounded with a %s content length', async (_, length) => {
    const canceled = jest.fn()
    let signal
    const fetch = async (_url, init) => {
      signal = init.signal
      const body = streamOf([new Uint8Array(1048576), new Uint8Array(1)], {
        hold: true,
        onCancel: canceled,
      })
      return new Response(body, { headers: length ? { 'Content-Length': length } : {} })
    }
    const error = await make({ tokenEndpointUrl: 'https://issuer.example/token', fetch })().catch(
      (caught) => caught
    )
    expect(error.message).toContain('body-size')
    expect(canceled).toHaveBeenCalledTimes(1)
    expect(signal.aborted).toBe(true)
  })

  test('a valid body exactly one MiB long is accepted', async () => {
    const start = '{"access_token":"opaque-token","pad":"'
    const end = '"}'
    const body = start + 'x'.repeat(1048576 - start.length - end.length) + end
    expect(Buffer.byteLength(body)).toBe(1048576)
    const endpoint = await listen((_request, response) => {
      response.writeHead(200, { 'Content-Type': 'application/json' })
      response.end(body)
    })
    expect(await make({ tokenEndpointUrl: endpoint.url })()).toEqual({ value: 'opaque-token' })
  })

  test('multibyte response data consumes bytes rather than characters', async () => {
    const start = '{"access_token":"opaque-token","pad":"'
    const body = `${start}${'é'.repeat(524288)}"}`
    expect(body.length).toBeLessThan(1048576)
    expect(Buffer.byteLength(body)).toBeGreaterThan(1048576)
    const endpoint = await listen((_request, response) => {
      response.writeHead(200, { 'Content-Type': 'application/json' })
      response.end(body)
    })
    await expect(make({ tokenEndpointUrl: endpoint.url })()).rejects.toThrow('body-size')
  })

  test('a body chunk that is not bytes is refused', async () => {
    const fetch = async () => new Response(streamOf(['{"access_token":"opaque-token"}']))
    await expect(
      make({ tokenEndpointUrl: 'https://issuer.example/token', fetch })()
    ).rejects.toThrow('body: the token response body is not bytes')
  })

  test('an absent body is an empty body', async () => {
    const fetch = async () => new Response(null, { status: 204 })
    await expect(
      make({ tokenEndpointUrl: 'https://issuer.example/token', fetch })()
    ).rejects.toThrow(
      'json: the token response is not JSON (endpoint=https://issuer.example/token status=204)'
    )
  })

  test('a status outside the HTTP range is reported as zero', async () => {
    const fetch = async () => ({ status: `${MARKER}`, body: null })
    const error = await make({ tokenEndpointUrl: 'https://issuer.example/token', fetch })().catch(
      (caught) => caught
    )
    expect(error.message).toContain('status=0 error=unknown')
    expect(textOf(error)).not.toContain(MARKER)
  })

  test.each([99, 600, 200.5])('a status of %s is reported as zero', async (status) => {
    const fetch = async () => ({ status, body: null })
    const error = await make({ tokenEndpointUrl: 'https://issuer.example/token', fetch })().catch(
      (caught) => caught
    )
    expect(error.message).toContain('status=0 error=unknown')
  })

  test('a transport failure is retried and then reported without its text', async () => {
    const starts = []
    const fetch = jest.fn(async () => {
      starts.push(monotonicMs())
      throw new TypeError(`fetch failed ${MARKER}`)
    })
    const call = observe(make({ tokenEndpointUrl: 'https://issuer.example/token', fetch })())
    await jest.advanceTimersByTimeAsync(8000)
    expect(starts).toEqual([0, 100, 300, 700, 1500, 3100, 6300])
    expect(call.error.message).toBe(
      'SASL OAUTHBEARER helper: transport: a token request could not be completed (endpoint=https://issuer.example/token)'
    )
    expect(textOf(call.error)).not.toContain(MARKER)
  })

  test('a fetch that throws synchronously or answers nothing is a transport failure', async () => {
    const answers = [
      () => {
        throw new Error(MARKER)
      },
      () => null,
      () => json({ access_token: 'opaque-token' }),
    ]
    const fetch = jest.fn(() => answers.shift()())
    const call = observe(make({ tokenEndpointUrl: 'https://issuer.example/token', fetch })())
    await jest.advanceTimersByTimeAsync(300)
    expect(fetch).toHaveBeenCalledTimes(3)
    expect(call.value).toEqual({ value: 'opaque-token' })
  })

  test('a transport failure followed by success recovers inside the budget', async () => {
    const endpoint = await listen(tokenAnswer)
    let calls = 0
    const fetch = (url, init) =>
      ++calls === 1 ? Promise.reject(new TypeError(MARKER)) : globalThis.fetch(url, init)
    const call = observe(make({ tokenEndpointUrl: endpoint.url, fetch })())
    await jest.advanceTimersByTimeAsync(100)
    await realDelay(200)
    expect(call.value).toEqual({ value: 'opaque-token' })
    expect(endpoint.requests).toHaveLength(1)
  })

  test('headers and body share one attempt timeout', async () => {
    const starts = []
    const fetch = jest.fn(
      (_url, init) =>
        new Promise((resolve) => {
          starts.push(monotonicMs())
          const headers = setTimeout(
            () =>
              resolve(
                new Response(streamOf([new TextEncoder().encode('{"access')], { hold: true }))
              ),
            4900
          )
          init.signal.addEventListener('abort', () => {
            clearTimeout(headers)
            starts.push(`aborted ${monotonicMs()}`)
          })
        })
    )
    const call = observe(make({ tokenEndpointUrl: 'https://issuer.example/token', fetch })())
    await jest.advanceTimersByTimeAsync(5000)
    expect(starts).toEqual([0, 'aborted 5000'])
    await jest.advanceTimersByTimeAsync(2999)
    expect(starts).toEqual([0, 'aborted 5000', 5100])
    expect(call).toEqual({})
    await jest.advanceTimersByTimeAsync(1)
    expect(call.error.message).toContain('deadline')
    expect(starts).toEqual([0, 'aborted 5000', 5100, 'aborted 8000'])
  })

  test('custom fetch cannot extend the attempt deadline', async () => {
    const canceled = jest.fn()
    const late = []
    const fetch = jest.fn(
      () =>
        new Promise((resolve) => {
          setTimeout(() => {
            late.push(monotonicMs())
            resolve(
              new Response(
                streamOf([new TextEncoder().encode('{"access_token":"late"}')], {
                  onCancel: canceled,
                })
              )
            )
          }, 9000)
        })
    )
    const provider = make({
      tokenEndpointUrl: 'https://issuer.example/token',
      fetch,
      maxRetryTime: 5000,
    })
    const call = observe(provider())
    await jest.advanceTimersByTimeAsync(5000)
    expect(call.error.message).toContain('deadline')
    await jest.advanceTimersByTimeAsync(4000)
    expect(late).toEqual([9000])
    expect(canceled).toHaveBeenCalledTimes(1)
    expect(fetch).toHaveBeenCalledTimes(1)
  })

  test('a late answer without a body is released quietly', async () => {
    const fetch = jest.fn(() => new Promise((resolve) => setTimeout(() => resolve(null), 6000)))
    const call = observe(
      make({ tokenEndpointUrl: 'https://issuer.example/token', fetch, maxRetryTime: 5000 })()
    )
    await jest.advanceTimersByTimeAsync(6000)
    expect(call.error.message).toContain('deadline')
  })

  test.each(['the supplier', 'the headers', 'the body', 'the backoff'])(
    'close during %s prevents later HTTP work',
    async (stage) => {
      const held = deferred()
      const fetch = jest.fn(async (_url, init) => {
        if (stage === 'the backoff') throw new TypeError(MARKER)
        if (stage === 'the headers') return new Promise(() => {})
        return new Response(streamOf([new TextEncoder().encode('{"access_token"')], { hold: true }))
      })
      const provider = make({
        tokenEndpointUrl: 'https://issuer.example/token',
        clientSecret: undefined,
        clientAssertion: () =>
          stage === 'the supplier' ? held.promise : 'header.payload.signature',
        fetch,
      })
      const call = observe(provider())
      await jest.advanceTimersByTimeAsync(50)
      provider.close()
      expect(jest.getTimerCount()).toBe(0)
      held.resolve('header.payload.signature')
      await jest.advanceTimersByTimeAsync(10000)
      expect(call.error.message).toContain('closed')
      expect(fetch).toHaveBeenCalledTimes(stage === 'the supplier' ? 0 : 1)
    }
  )

  test('a stalled real request is aborted when its retrieval ends', async () => {
    const arrived = deferred()
    const endpoint = await listen(() => arrived.resolve())
    const call = observe(
      make({ tokenEndpointUrl: endpoint.url, timeout: 1000, maxRetryTime: 1000 })()
    )
    await arrived.promise
    const [active] = endpoint.sockets
    const closed = new Promise((resolve) => active.once('close', resolve))
    await jest.advanceTimersByTimeAsync(1000)
    await closed
    expect(call.error.message).toContain('deadline')
  })

  test.each([
    ['https://issuer.example/token', false],
    ['http://localhost/token', true],
    ['http://LOCALHOST:8080/token', true],
    ['http://127.0.0.1:1/token', true],
    ['http://[::1]:1/token', true],
  ])('accepts the endpoint %s', (tokenEndpointUrl, allowInsecure) => {
    const fetch = jest.fn()
    expect(typeof make({ tokenEndpointUrl, allowInsecure, fetch })).toBe('function')
    expect(fetch).not.toHaveBeenCalled()
  })

  test.each([
    ['http://example.org/token', true],
    ['http://localhost/token', false],
    ['http://127.0.0.2/token', true],
    ['http://127.1/token', true],
    ['http://0x7f000001/token', true],
    ['http://2130706433/token', true],
    ['http://0177.0.0.1/token', true],
    ['http://[0:0:0:0:0:0:0:1]/token', true],
    ['http://localhost.evil.example/token', true],
    ['http:localhost/token', true],
    ['http://user@localhost/token', true],
    ['https://user:password@issuer.example/token', false],
    ['https://:password@issuer.example/token', false],
    ['https://issuer.example/token#fragment', false],
    ['https://issuer.example/token#', false],
    ['https://issuer.example:0/token', false],
    ['https://issuer.example:99999/token', false],
    ['ftp://issuer.example/token', true],
    ['file:///token', true],
    ['/token', false],
    ['', false],
    ['https://issuer.example/a\0b', false],
    [5, false],
    [undefined, false],
  ])('refuses the endpoint %p before supplier or fetch', (tokenEndpointUrl, allowInsecure) => {
    const fetch = jest.fn()
    const clientAssertion = jest.fn()
    expect(() => make({ tokenEndpointUrl, allowInsecure, fetch })).toThrow(
      'Invalid OAuth bearer helper option: tokenEndpointUrl'
    )
    expect(() =>
      make({ tokenEndpointUrl, allowInsecure, fetch, clientSecret: undefined, clientAssertion })
    ).toThrow(KafkaJSInvalidConfigurationError)
    expect(fetch).not.toHaveBeenCalled()
    expect(clientAssertion).not.toHaveBeenCalled()
  })

  test.each([
    [{ clientId: '' }, 'clientId'],
    [{ clientId: null }, 'clientId'],
    [{ clientId: 5 }, 'clientId'],
    [{ clientId: 'a\0b' }, 'clientId'],
    [{ clientSecret: '' }, 'clientSecret'],
    [{ clientSecret: 5 }, 'clientSecret'],
    [{ clientSecret: null }, 'clientSecret'],
    [{ clientSecret: `${SECRET}\0` }, 'clientSecret'],
    [{ clientSecret: undefined }, 'clientSecret or clientAssertion'],
    [{ clientAssertion: () => 'assertion' }, 'clientSecret or clientAssertion'],
    [{ clientSecret: undefined, clientAssertion: 'assertion' }, 'clientAssertion'],
    [{ clientSecret: undefined, clientAssertion: null }, 'clientAssertion'],
    [{ scope: null }, 'scope'],
    [{ scope: 5 }, 'scope'],
    [{ scope: 'a\0b' }, 'scope'],
    [{ timeout: 0 }, 'timeout'],
    [{ extensions: { trace: 1 } }, 'extensions'],
  ])('credentials and common knobs validate before work: %p', (options, name) => {
    const fetch = jest.fn()
    let error
    try {
      make({ tokenEndpointUrl: 'https://issuer.example/token', fetch, ...options })
    } catch (caught) {
      error = caught
    }
    expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(error.message).toBe(`Invalid OAuth bearer helper option: ${name}`)
    expect(textOf(error)).not.toContain(SECRET)
    expect(fetch).not.toHaveBeenCalled()
  })

  test.each([[null], [undefined], ['options']])('refuses options %p', (options) => {
    expect(() => clientCredentials(options)).toThrow('Invalid OAuth bearer helper option: options')
  })

  test('an empty scope is sent as given', async () => {
    const fetch = jest.fn(async () => json({ access_token: 'opaque-token' }))
    await make({ tokenEndpointUrl: 'https://issuer.example/token', fetch, scope: '' })()
    expect(fetch.mock.calls[0][1].body).toBe('grant_type=client_credentials&scope=')
  })

  test('changing the original options cannot redirect later retrieval', async () => {
    const fetch = jest.fn(async () => json({ access_token: 'opaque-token', expires_in: 3600 }))
    const options = {
      tokenEndpointUrl: 'https://issuer.example/token',
      clientId: 'client',
      clientSecret: SECRET,
      scope: 'read',
      fetch,
      retry: { initial: 100, max: 10000 },
      extensions: { trace: 'one' },
    }
    const provider = clientCredentials(options)
    providers.push(provider)
    Object.assign(options, {
      tokenEndpointUrl: 'https://other.example/token',
      clientId: 'other',
      clientSecret: 'other-secret',
      scope: 'write',
      fetch: jest.fn(),
    })
    options.retry.initial = 1
    options.extensions.trace = 'two'
    expect(await provider()).toEqual({ value: 'opaque-token', extensions: { trace: 'one' } })
    expect(fetch.mock.calls[0][0]).toBe('https://issuer.example/token')
    expect(fetch.mock.calls[0][1].body).toBe('grant_type=client_credentials&scope=read')
    expect(fetch.mock.calls[0][1].headers.Authorization).toBe(
      `Basic ${Buffer.from(`client:${SECRET}`).toString('base64')}`
    )
  })

  test.each([
    ['literally', `https://issuer.example/${SECRET}/token`],
    ['percent encoded', 'https://issuer.example/inert%2Dsecret/token'],
    ['beside a malformed escape', `https://issuer.example/%zz/${SECRET}`],
  ])('an endpoint path holding the client secret %s is redacted', async (_, tokenEndpointUrl) => {
    const fetch = async () => json({ error: 'invalid_client' }, 401)
    const error = await make({ tokenEndpointUrl, fetch })().catch((caught) => caught)
    expect(error.message).toContain('endpoint=https://issuer.example/[redacted] status=401')
    expect(textOf(error)).not.toContain(SECRET)
  })

  test('a secret holding a percent escape is redacted from the literal path', async () => {
    const fetch = async () => json({ error: 'invalid_client' }, 401)
    const error = await make({
      tokenEndpointUrl: 'https://issuer.example/inert%41secret/token',
      clientSecret: 'inert%41secret',
      fetch,
    })().catch((caught) => caught)
    expect(error.message).toContain('endpoint=https://issuer.example/[redacted] status=401')
    expect(textOf(error)).not.toMatch(/inert%41secret|inertAsecret/)
  })

  test('an endpoint path with a malformed escape and no secret is named as encoded', async () => {
    const fetch = async () => json({}, 500)
    const call = observe(
      make({ tokenEndpointUrl: 'https://issuer.example/%zz/token', fetch, maxRetryTime: 1 })()
    )
    await jest.advanceTimersByTimeAsync(1)
    expect(call.error.message).toContain('endpoint=https://issuer.example/%zz/token status=500')
  })
})
