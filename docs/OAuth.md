---
id: oauth
title: OAuth Helpers and Recipes
---

## OAuth bearer authentication

The `oauthBearer` export builds the existing `oauthBearerProvider` callback. Pass the returned function as `sasl.oauthBearerProvider`. The provider belongs to your application: disconnect every client using it, then call `provider.close()` when you no longer need it. Closing a Kafka client does not close a shared provider. Closing the provider aborts owned retrieval work, clears its timers and rejects later calls. `signAssertion` returns an assertion supplier, rather than a Kafka token provider.

Access-token expiry is read from a JWT's `exp` claim without verifying its signature, or from `expires_in` for an opaque token. The broker validates the token. Reading an expiry is not token validation. A token with no usable lifetime is acquired again on the next authentication; concurrent acquisitions still share one retrieval.

```javascript
const { Kafka, oauthBearer } = require('@countly/kafkajs')
// or: import { Kafka, oauthBearer } from '@countly/kafkajs'
```

## Providers and lifecycle

Each helper returns one provider with one token cache. Concurrent authentications share one retrieval, and the token is refreshed in the background before it expires, so an authentication usually finds a fresh token. The example below is the whole lifecycle: create the provider, use it for every client that needs it, disconnect those clients, then close the provider.

```js
async function authenticateWithHelper({ Kafka, oauthBearer, brokers, tokenEndpointUrl, clientId, clientSecret, fetch: fetchImpl, allowInsecure = false }) {
  const provider = oauthBearer.clientCredentials({ tokenEndpointUrl, clientId, clientSecret, fetch: fetchImpl, allowInsecure })
  const kafka = new Kafka({ brokers, ssl: true, sasl: { mechanism: 'oauthbearer', oauthBearerProvider: provider } })
  const admin = kafka.admin()
  try {
    await admin.connect()
    return await admin.listTopics()
  } finally {
    try {
      await admin.disconnect()
    } finally {
      provider.close()
    }
  }
}
```

The function's parameters are deployment inputs: the broker list, the token endpoint and the client credentials come from your configuration, never from source code.

## Options

Every provider factory accepts these options. Durations are in milliseconds; the window and jitter are fractions.

| Option | Default and unit | Meaning |
|---|---|---|
| `timeout` | 5000 ms | One HTTP attempt, including headers and body; clamped to the remaining total. |
| `maxRetryTime` | 8000 ms | One shared on-demand retrieval, supplier/key/file work, all HTTP attempts, bodies, backoff and publication included. |
| `retry.initial` / `retry.max` | 100 / 10000 ms | Exponential retry delays. |
| `refreshWindow` / `refreshJitter` | .8 / .05, fractions | Successful token lifetime's refresh point ± jitter. |
| `minRefreshPeriod` | 60000 ms | Earliest proactive refresh after a successful fetch; short tokens refresh on demand. |
| `refreshBuffer` | 300000 ms | Refresh before expiration where possible, without violating the minimum period. |
| `clockSkew` | 30000 ms | A cached token counts as expired this far before exp. |
| `extensions` | absent | Copied string record passed through the existing SASL callback. |
| `allowInsecure` | false | Tests only: HTTP additionally requires a literal loopback host. Use HTTPS for deployments. |
| `fetch` | global fetch | A compatible fetch implementation for deployment-controlled transport, private CAs or proxies. |

Integral options accept 1 to 2147483647 (`refreshBuffer` and `clockSkew` also accept 0); `refreshWindow` lies strictly between 0 and 1, and the window minus and plus the jitter must also. Numeric strings are refused. An invalid option throws `KafkaJSInvalidConfigurationError` naming it, before any request.

## Client credentials

`clientCredentials` sends the OAuth 2.0 client-credentials grant (RFC 6749 §4.4). With `clientSecret`, the client id and secret are form-encoded and sent as HTTP Basic authentication; the body is `grant_type=client_credentials`, plus `scope` when given.

```js
function createSecretProvider({ oauthBearer, tokenEndpointUrl, clientId, clientSecret, scope, fetch: fetchImpl, allowInsecure = false }) {
  return oauthBearer.clientCredentials({ tokenEndpointUrl, clientId, clientSecret, scope, fetch: fetchImpl, allowInsecure })
}
```

## Client assertion

With `clientAssertion` in place of the secret, the client authenticates with a signed JWT (RFC 7523 §2.2): the body carries `client_assertion_type`, `client_assertion` and `client_id`, and there is no Authorization header. The supplier is called once for each token request; it may return the assertion or a promise of it.

```js
function createAssertionProvider({ oauthBearer, tokenEndpointUrl, clientId, clientAssertion, scope, fetch: fetchImpl, allowInsecure = false }) {
  return oauthBearer.clientCredentials({ tokenEndpointUrl, clientId, clientAssertion, scope, fetch: fetchImpl, allowInsecure })
}
```

## JWT bearer grant

`jwtBearer` sends the JWT-bearer grant (RFC 7523 §2.1): the assertion is the grant, as `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=…`, plus `scope` when given.

```js
function createGrantProvider({ oauthBearer, tokenEndpointUrl, assertion, scope, fetch: fetchImpl, allowInsecure = false }) {
  return oauthBearer.jwtBearer({ tokenEndpointUrl, assertion, scope, fetch: fetchImpl, allowInsecure })
}
```

## File tokens

`fileToken` reads a token that another process writes, for example a sidecar or a mounted secret. Every authentication checks the file's identity (its modification time, size and inode); the file is read again when that changes, or when the cached token is due for refresh or has expired. A rotated token is therefore used at the next authentication. The file must hold one JWT on one line, with at most one final newline. An unreadable, empty or malformed file is an error and is never cached; the previous token is not used after its file has gone bad.

```js
function createFileProvider({ oauthBearer, path }) {
  return oauthBearer.fileToken({ path })
}
```

## Local assertions

`signAssertion` signs JWT assertions with `node:crypto`, for `clientAssertion` or `assertion`. It supports RS256, RS384, RS512 (an RSA key of at least 2048 bits), and ES256, ES384 and ES512 (a P-256, P-384 or P-521 key respectively, with signatures in the JOSE `ieee-p1363` encoding). RS256 is the default. The key is a PEM string, a PEM `Buffer`, a private `KeyObject`, or `{ file }`; a key file is checked on every call and read again when its modification time changes, and a key that fails to load is never replaced by the old one. An encrypted PEM key needs its `passphrase`.

Every call signs a new assertion: `iat` is now, `nbf` is now minus `notBefore` (60 seconds by default), `exp` is now plus `expiresIn` (300 seconds by default), and `jti` is a fresh UUID when `jti: true`. Both are in seconds. The `claims` you give are copied when the signer is created; `iat`, `nbf`, `exp` and `jti` cannot be among them. Set `iss`, `sub` and `aud` to what your authorization server expects; the signer invents none of them. A bad algorithm or key is a configuration error at creation.

```js
function createLocalAssertion({ oauthBearer, privateKey, clientId, tokenEndpointUrl }) {
  return oauthBearer.signAssertion({
    privateKey,
    algorithm: 'RS256',
    claims: { iss: clientId, sub: clientId, aud: tokenEndpointUrl },
    expiresIn: 300,
    notBefore: 60,
    jti: true,
  })
}
```

A signed assertion and an access token are different values. The assertion goes to the token endpoint; only the access token the endpoint returns goes to Kafka.

## Discovery

The helpers take the token endpoint as configuration. To find it from an OpenID Connect issuer (RFC 8414), fetch the issuer's metadata first, check it, then build the helper. The metadata is untrusted input; the issuer you expect is your configuration. The recipes in this and the following sections use a small JSON request adapter, `requestJson`, which is application code: it accepts only HTTPS (or, in tests, a literal loopback HTTP host), refuses redirects, caps the request at `timeout` milliseconds including the body and the body at 1 MiB, and fails with fixed text that never repeats the response.

```js
function createRequestJson({ transport = fetch, allowInsecure = false, timeout = 5000 } = {}) {
  const loopback = ['localhost', '127.0.0.1', '[::1]']
  return async function requestJson(url, init = {}, fetchImpl = transport) {
    const target = new URL(url)
    const testOnly = allowInsecure && target.protocol === 'http:' && loopback.includes(target.hostname)
    if (target.protocol !== 'https:' && !testOnly) throw new Error('Refused token endpoint')
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), timeout)
    try {
      const response = await fetchImpl(target.href, { ...init, redirect: 'manual', signal: controller.signal })
      if (response.status < 200 || response.status > 299) throw new Error('Refused')
      const bytes = Buffer.allocUnsafe(1048576)
      let size = 0
      for await (const chunk of response.body) {
        if (!(chunk instanceof Uint8Array)) throw new Error('Invalid body')
        if (size + chunk.byteLength > bytes.length) throw new Error('Too large')
        bytes.set(chunk, size)
        size += chunk.byteLength
      }
      const value = JSON.parse(bytes.subarray(0, size).toString('utf8'))
      if (value === null || typeof value !== 'object' || Array.isArray(value)) throw new Error('Not an object')
      return value
    } catch {
      throw new Error('Token request failed')
    } finally {
      clearTimeout(timer)
      controller.abort()
    }
  }
}
```

```js
async function discoverProvider({ oauthBearer, requestJson, issuer, clientId, clientSecret, scope, fetch: fetchImpl }) {
  const expected = new URL(issuer)
  if (expected.protocol !== 'https:' || expected.username || expected.password || expected.hash) throw new Error('Invalid issuer')
  const metadata = await requestJson(new URL('.well-known/openid-configuration', `${expected.href.replace(/\/$/, '')}/`).href, { method: 'GET' })
  if (metadata.issuer !== issuer || typeof metadata.token_endpoint !== 'string') throw new Error('Invalid discovery metadata')
  const endpoint = new URL(metadata.token_endpoint)
  if (endpoint.protocol !== 'https:' || endpoint.username || endpoint.password || endpoint.hash) throw new Error('Invalid discovered endpoint')
  return oauthBearer.clientCredentials({ tokenEndpointUrl: endpoint.href, clientId, clientSecret, scope, fetch: fetchImpl })
}
```

Do not assume the token endpoint shares the issuer's origin; accept any HTTPS endpoint the checked metadata names. Discovery runs before the helper exists, so it is not inside a helper's retrieval budget; run it at startup.

## Refresh tokens

The helpers do not use refresh tokens: a client-credentials or JWT-bearer token is simply requested again. Where your issuer hands out refresh tokens for another grant, the refresh-token grant is a recipe. `authenticateRequest` adds your client authentication to the request.

```js
async function refreshToken({ requestJson, tokenEndpointUrl, refreshToken, authenticateRequest }) {
  const body = new URLSearchParams({ grant_type: 'refresh_token', refresh_token: refreshToken })
  const response = await requestJson(tokenEndpointUrl, authenticateRequest({ method: 'POST', body: body.toString(), headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }))
  if (typeof response.access_token !== 'string' || !response.access_token) throw new Error('Invalid token response')
  return { value: response.access_token }
}
```

## Token exchange

Token exchange (RFC 8693) trades a token you hold for one meant for Kafka. Whether your issuer accepts a given subject token, audience and client authentication is its configuration.

```js
async function exchangeToken({ requestJson, tokenEndpointUrl, subjectToken, audience, authenticateRequest }) {
  const body = new URLSearchParams({
    grant_type: 'urn:ietf:params:oauth:grant-type:token-exchange',
    subject_token: subjectToken,
    subject_token_type: 'urn:ietf:params:oauth:token-type:access_token',
    audience,
  })
  const response = await requestJson(tokenEndpointUrl, authenticateRequest({ method: 'POST', body: body.toString(), headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }))
  if (typeof response.access_token !== 'string' || !response.access_token) throw new Error('Invalid token response')
  return { value: response.access_token }
}
```

A recipe that returns a bare `{ value }` gets no helper cache. If you use one as `oauthBearerProvider`, your callback keeps its existing responsibility for caching, refresh, its time budget and cleanup. Prefer a helper for the grants the helpers support.

## Mutual TLS

With mutual-TLS client authentication (RFC 8705), the client certificate authenticates the client and the body carries only `client_id`. Configure `mutualTlsFetch` with your client certificate, key and trust settings. A fetch implementation must actually use that TLS configuration; adding `cert` or `key` to a standard fetch init object does not configure the global fetch. Where your issuer requires mutual TLS and a client assertion together, pass the configured fetch to the client-assertion helper instead.

```js
async function mutualTlsToken({ requestJson, mutualTlsFetch, tokenEndpointUrl, clientId, scope }) {
  const body = new URLSearchParams({ grant_type: 'client_credentials', client_id: clientId })
  if (scope !== undefined) body.set('scope', scope)
  const response = await requestJson(tokenEndpointUrl, {
    method: 'POST',
    body: body.toString(),
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
  }, mutualTlsFetch)
  if (typeof response.access_token !== 'string' || !response.access_token) throw new Error('Invalid token response')
  return { value: response.access_token }
}
```

## Remote signing

A key held in a KMS or HSM never reaches `node:crypto`, so `signAssertion` cannot use it. Pass a supplier that asks your KMS to sign instead: `signRemotely` returns a complete compact signed JWT, and your deployment owns its claims, credentials and transport. The supplier runs inside the helper's retrieval budget; a late answer cannot start a token request.

```js
function createKmsProvider({ oauthBearer, tokenEndpointUrl, clientId, signRemotely, fetch: fetchImpl }) {
  return oauthBearer.clientCredentials({
    tokenEndpointUrl,
    clientId,
    clientAssertion: () => signRemotely({ issuer: clientId, subject: clientId, audience: tokenEndpointUrl }),
    fetch: fetchImpl,
  })
}
```

Do not pass a KMS key id as `privateKey`.

## Proxies and private certificate authorities

The helpers add no HTTP dependency. To reach the token endpoint through a proxy or with a private certificate authority, pass a `fetch` implementation that applies that configuration. It must keep the `AbortSignal` the helper passes and must not follow redirects. Do not disable TLS validation.

```js
function createTransportProvider({ oauthBearer, configuredFetch, tokenEndpointUrl, clientId, clientSecret }) {
  return oauthBearer.clientCredentials({ tokenEndpointUrl, clientId, clientSecret, fetch: configuredFetch })
}
```

## Failures and budgets

The default helper retrieval budget is 8000 ms. Its default HTTP attempt cap is 5000 ms, including the response body. Retries spend the same budget. Keep that total below your `authenticationTimeout`; the default SASL timeout is 10000 ms. A token callback slower than `authenticationTimeout` needs a larger value; the helpers refresh ahead of time. A never-resolving application supplier can be detached when the budget ends; its late result cannot start an HTTP request or publish a token. An arbitrary application promise cannot be physically canceled by the library.

Refresh uses the earlier of the configured lifetime fraction and expiry minus the refresh buffer, but never begins before the minimum refresh period. Tokens shorter than that period are refreshed on demand. A failed background refresh preserves a still-valid HTTP token only until its skew-adjusted expiry. A modified or unreadable token file is revalidated on the next authentication.

Network failures, timeouts and the statuses 408, 429 and 5xx are retried with backoff inside the budget. Other statuses, redirects (never followed, so credentials never reach another host), responses over 1 MiB, invalid JSON, a missing `access_token` and a token type other than bearer end the retrieval at once.

Invalid helper options or an invalid initial signing key throw `KafkaJSInvalidConfigurationError`. Retrieval, deadline, close and source failures from the shipped helpers reject with `KafkaJSSASLAuthenticationError`. Diagnostics contain a stage, sanitized endpoint origin/path and status where available, and a known OAuth error code. They omit credentials, assertions, access tokens, response bodies and raw causes. A callback you wrote yourself retains its existing rejection behavior. The library does not wrap that callback merely because helpers are available.

Built-in PLAIN, SCRAM, OAUTHBEARER and AWS exchange-failure wrappers append only client-authored text: fixed local SCRAM validation reasons, the canonical protocol-code description for protocol failures, or `Error` otherwise. A bare application callback still rejects unchanged, as described above. The authentication-failure event and the reconnect blackout name the mechanism, the broker and the stage. The broker's own error message, a SCRAM server error and the authentication bytes are never copied into errors, causes or logs, whatever the buffer-diagnostics setting.
