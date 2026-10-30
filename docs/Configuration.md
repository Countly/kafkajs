---
id: configuration
title: Client Configuration
---

The client must be configured with at least one broker. The brokers on the list are considered seed brokers and are only used to bootstrap the client and load initial metadata.

```javascript
const { Kafka } = require('@countly/kafkajs')

// Create the client with the broker list
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092']
})
```

## Client Id
A logical identifier of an application. Can be used by brokers to apply quotas or trace requests to a specific application. Example: `booking-events-processor`.

The [kafka documentation](https://kafka.apache.org/documentation/#design_quotasgroups) describes the `clientId` as:

> Client-id is a logical grouping of clients with a meaningful name chosen by the client application. The tuple (user, client-id) defines a secure logical group of clients that share both user principal and client-id. Quotas can be applied to (user, client-id), user or client-id groups.

It [also says](https://kafka.apache.org/documentation/#producerconfigs_client.id):

> `client.id`
>
> An id string to pass to the server when making requests. The purpose of this is to be able to track the source of requests beyond just ip/port by allowing a logical application name to be included in server-side request logging.

Therefore the `clientId` should be shared across multiple instances in a cluster or horizontally scaled application, but distinct for each application.

## Broker discovery

Normally KafkaJS will notice and react to broker cluster topology changes automatically, but in some circumstances you may want to be able to dynamically
fetch the seed brokers instead of using a statically configured list. In that case, `brokers` can be set to an async function that resolves to
a broker array:

```javascript
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: async () => {
    // Example getting brokers from Confluent REST Proxy
    const clusterResponse = await fetch('https://kafka-rest:8082/v3/clusters', {
      headers: 'application/vnd.api+json',
    }).then(response => response.json())
    const clusterUrl = clusterResponse.data[0].links.self

    const brokersResponse = await fetch(`${clusterUrl}/brokers`, {
      headers: 'application/vnd.api+json',
    }).then(response => response.json())

    const brokers = brokersResponse.data.map(broker => {
      const { host, port } = broker.attributes
      return `${host}:${port}`
    })

    return brokers
  }
})
```

Note that this discovery mechanism is only used to get the initial set of brokers (i.e. the seed brokers). After successfully connecting to
a broker in this list, Kafka has its own mechanism for discovering the rest of the cluster.

## SSL

The `ssl` option can be used to configure the TLS sockets. The options are passed directly to [`tls.connect`](https://nodejs.org/api/tls.html#tls_tls_connect_options_callback) and used to create the TLS Secure Context, all options are accepted.

```javascript
const fs = require('fs')

new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  ssl: {
    rejectUnauthorized: false,
    ca: [fs.readFileSync('/my/custom/ca.crt', 'utf-8')],
    key: fs.readFileSync('/my/custom/client-key.pem', 'utf-8'),
    cert: fs.readFileSync('/my/custom/client-cert.pem', 'utf-8')
  },
})
```

Refer to [TLS create secure context](https://nodejs.org/dist/latest-v8.x/docs/api/tls.html#tls_tls_createsecurecontext_options) for more information. `NODE_EXTRA_CA_CERTS` can be used to add custom CAs. Use `ssl: true` if you don't have any extra configurations and want to enable SSL.

## SASL

Kafka has support for using SASL to authenticate clients. The `sasl` option can be used to configure the authentication mechanism. Currently, KafkaJS supports `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, and `AWS` mechanisms.

Note that the broker may be configured to reject your authentication attempt if you are not using TLS, even if the credentials themselves are valid. In particular, never authenticate without TLS when using `PLAIN` as your authentication mechanism, as that will transmit your credentials unencrypted in plain text. See [SSL](#ssl) for more information on how to enable TLS.

### Options

| option                    | description                                                                                                                                                                             | default |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| authenticationTimeout     | Timeout in ms for authentication requests                                                                                                                                               | `10000`  |
| reauthenticationThreshold | When periodic reauthentication (`connections.max.reauth.ms`) is configured on the broker side, reauthenticate when `reauthenticationThreshold` milliseconds remain of session lifetime. | `10000` |

### PLAIN/SCRAM Example

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  // authenticationTimeout: 10000,
  // reauthenticationThreshold: 10000,
  ssl: true,
  sasl: {
    mechanism: 'plain', // scram-sha-256 or scram-sha-512
    username: 'my-username',
    password: 'my-password'
  },
})
```

### OAUTHBEARER Example

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  // authenticationTimeout: 10000,
  // reauthenticationThreshold: 10000,
  ssl: true,
  sasl: {
    mechanism: 'oauthbearer',
    oauthBearerProvider: async () => {
      // Use an unsecured token...
      const token = jwt.sign({ sub: 'test' }, 'abc', { algorithm: 'none' })

      // ...or, more realistically, grab the token from some OAuth endpoint

      return {
        value: token
      }
    }
  },
})
```

The `sasl` object must include a property named `oauthBearerProvider`, an
async function that is used to return the OAuth bearer token.

The OAuth bearer token must be an object with properties value and
(optionally) extensions, that will be sent during the SASL/OAUTHBEARER
request.

The implementation of the oauthBearerProvider must take care that tokens are
reused and refreshed when appropriate. An example implementation using
[`simple-oauth2`](https://github.com/lelylan/simple-oauth2) would look something
like the following:

```ts
import { AccessToken, ClientCredentials } from 'simple-oauth2'
interface OauthBearerProviderOptions {
  clientId: string;
  clientSecret: string;
  host: string;
  path: string;
  refreshThresholdMs: number;
}

const oauthBearerProvider = (options: OauthBearerProviderOptions) => {
  const client = new ClientCredentials({
    client: {
      id: options.clientId,
      secret: options.clientSecret
    },
    auth: {
      tokenHost: options.host,
      tokenPath: options.path
    }
  });

  let tokenPromise: Promise<string>;
  let accessToken: AccessToken;

  async function refreshToken() {
    try {
      if (accessToken == null) {
        accessToken = await client.getToken({})
      }

      if (accessToken.expired(options.refreshThresholdMs / 1000)) {
        accessToken = await accessToken.refresh()
      }

      const nextRefresh = accessToken.token.expires_in * 1000 - options.refreshThresholdMs;
      setTimeout(() => {
        tokenPromise = refreshToken()
      }, nextRefresh);

      return accessToken.token.access_token;
    } catch (error) {
      accessToken = null;
      throw error;
    }
  }

  tokenPromise = refreshToken();

  return async function () {
    return {
      value: await tokenPromise
    }
  }
};

const kafka = new Kafka({
  // ... other required options
  sasl: {
    mechanism: 'oauthbearer',
    oauthBearerProvider: oauthBearerProvider({
      clientId: 'oauth-client-id',
      clientSecret: 'oauth-client-secret',
      host: 'https://my-oauth-server.com',
      path: '/oauth/token',
      // Refresh the token 15 seconds before it expires
      refreshThreshold: 15000,
    }),
  },
})
```

### AWS IAM Example

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  // authenticationTimeout: 10000,
  // reauthenticationThreshold: 10000,
  ssl: true,
  sasl: {
    mechanism: 'aws',
    authorizationIdentity: 'AIDAIOSFODNN7EXAMPLE', // UserId or RoleId
    accessKeyId: 'AKIAIOSFODNN7EXAMPLE',
    secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    sessionToken: 'WHArYt8i5vfQUrIU5ZbMLCbjcAiv/Eww6eL9tgQMJp6QFNEXAMPLETOKEN' // Optional
  },
})
```

For more information on the basics of IAM credentials and authentication, see the
[AWS Security Credentials - Access Keys](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html#access-keys-and-secret-access-keys) page.

Use of this functionality requires
[STACK's Kafka AWS IAM LoginModule](https://github.com/STACK-Fintech/kafka-auth-aws-iam), or a
compatible alternative to be installed on all of the target brokers.

In the above example, the `authorizationIdentity` must be the `aws:userid` of the AWS IAM
identity. Typically, you can retrieve this value using the `aws iam get-user` or `aws iam get-role`
commands of the [AWS CLI toolkit](https://aws.amazon.com/cli/). The `aws:userid` is usually listed
as the `UserId` or `RoleId` property of the response.

You can also programmatically retrieve the `aws:userid` for currently available credentials with the
[AWS SDK's Security Token Service](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/STS.html).

A complete breakdown can be found in the IAM User Guide's
[Reference on Policy Variables](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_variables.html#policy-vars-infotouse).

### Use Encrypted Protocols

It is **highly recommended** that you use SSL for encryption when using `PLAIN` or `AWS`,
otherwise credentials will be transmitted in cleartext!

### Custom Authentication Mechanisms

If an authentication mechanism is not supported out of the box in KafkaJS, a custom authentication
mechanism can be introduced as a plugin:

```js
{ 
  sasl: { 
      mechanism: <mechanism name>,
      authenticationProvider: ({ host, port, logger, saslAuthenticate }) => { authenticate: () => Promise<void> }
  }
}
```

See [Custom Authentication Mechanisms](CustomAuthenticationMechanism.md) for more information on how to implement your own
authentication mechanism.

## Consumer group identity

`groupId` is required for consumer subscription and group offset APIs. A consumer that only uses [manual assignment](Consuming.md#manual-assignment) may omit it or pass undefined; its offset state is private, automatic commits default to false and explicit autoCommit true rejects. Null, empty and non-string group IDs reject at construction. Configure this option on `kafka.consumer()`. Manual assignment uses no membership protocol, even when a group protocol option is configured.

## Connection Timeout

Time in milliseconds to wait for a successful connection. The default value is: `1000`.

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  connectionTimeout: 3000
})
```

## Request Timeout

Time in milliseconds to wait for a successful request. The default value is: `30000`.

Consumer group liveness uses separate options. The classic `sessionTimeout` defaults to 45000 ms and must be an integer from 1 to 2147483647; `heartbeatInterval` defaults to 3000 ms and must be a positive integer below the session. The consumer protocol keeps broker-controlled liveness and rejects explicit classic timeout options. See the [consumer options](Consuming.md#options) for the warning, broker bounds, kept defaults and the explicit 30000 ms compatibility setting.

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  requestTimeout: 25000
})
```

A producer's explicit `deliveryTimeout` must be at least its `linger` (5 ms unless the producer sets another, see the [accumulator](Producing.md#accumulator)) plus this value - 30005 ms with both defaults - see [Delivery timeout](Producing.md#delivery-timeout).

The request timeout can be disabled by setting `enforceRequestTimeout` to `false`.

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  enforceRequestTimeout: false
})
```

## Max In Flight Requests

`maxInFlightRequests`, available on `producer()` and `consumer()`, caps the number of
unacknowledged requests allowed on a broker connection before further requests are queued
client-side. The default is `5`, matching the Java client's
`max.in.flight.requests.per.connection`. Pass `null` to restore the previous unbounded behavior.

```javascript
kafka.producer({
  maxInFlightRequests: 1
})
```

An idempotent producer - the default - allows at most 5. A producer that did not ask for idempotence and passes more, or `null`, runs without it and logs one warning; with `idempotent: true` or a `transactionalId` the combination throws `KafkaJSInvalidConfigurationError`. See [Idempotence](Producing.md#idempotence).

## Max Response Size

`maxResponseSize` is the response-size limit passed to every connection, in bytes, not counting
the four-byte length prefix. It is an integer from 4 to 2147483647 and defaults to 104857600
(100 MiB). It is not the consumer's `maxBytes`.

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092'],
  maxResponseSize: 104857600,
})
```

A value outside that range, or one that is not an integer, makes the constructor throw
`KafkaJSInvalidConfigurationError`.

A response frame that is negative, shorter than a correlation id or larger than the limit closes
its connection with `KafkaJSInvalidFrameError` (retriable; its `reason` is `negative`, `too-short`
or `too-large`) and fails every request on that connection; the next request opens a new one. Raise
`maxResponseSize` only for deliberately larger responses, such as a consumer `maxBytes` near the
limit or a very large metadata response. A consumer whose `maxBytes` is above the limit logs one
warning. Each connection can buffer up to this much, so the limit is not a memory ceiling for the
process.

A Fetch frame is at most `maxResponseSize` bytes, but a compressed record batch inside it may
decompress to far more. The consumer's `maxDecompressedBatchBytes` (1 GiB by default) bounds one
batch after decompression, for every codec; see
[Decompression bound](Consuming.md#decompression-bound).

## Metadata recovery

`metadataRecoveryStrategy` and `metadataRecoveryRebootstrapTriggerMs`, available on the `Kafka`
constructor, control what the client does when it loses its view of the cluster (KIP-899/KIP-1102,
matching the Java client's `metadata.recovery.strategy` and
`metadata.recovery.rebootstrap.trigger.ms`).

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  metadataRecoveryStrategy: 'rebootstrap',
  metadataRecoveryRebootstrapTriggerMs: 300000,
})
```

- `metadataRecoveryStrategy`: `'rebootstrap'` (the default) or `'none'`. With `'rebootstrap'`, the
  client rebuilds its connections and metadata from the configured broker list when:
  1. metadata has been failing to refresh **continuously** for `metadataRecoveryRebootstrapTriggerMs`;
  2. a broker responds with `REBOOTSTRAP_REQUIRED` (protocol error code 129); or
  3. every broker the client currently knows about has become unreachable.

  With `'none'`, none of the above happen: a `REBOOTSTRAP_REQUIRED` response surfaces as the
  non-retriable `KafkaJSProtocolError` it already is, restoring the pre-3.0 behavior.
- `metadataRecoveryRebootstrapTriggerMs`: how long metadata must fail continuously before trigger
  1 above fires. Defaults to `300000` (5 minutes, matching the Java client). Must be an integer
  **≥ 1** — `0` is rejected rather than meaning "rebootstrap immediately": at `0` the trigger would
  fire on the very next failed attempt, and one unreachable cluster would produce a full teardown
  per metadata refresh instead of at most one per window.

"Continuously" means the client has not seen a single successful metadata response since the
first failure. The clock starts at the first metadata failure that follows a success and is
cleared by any success — so a client that has been idle for a while (nothing refreshes metadata
until it is stale) and then hits one transient failure, such as a rolling restart or a leader
election, does **not** rebootstrap; it only rebootstraps once the failures are back-to-back for
the whole window.

A rebootstrap waits for each connection's in-flight requests to drain, then closes every
connection the client holds. A request already in flight when it starts normally completes on the
old socket rather than being rejected; the connections themselves are gone by the time
`rebootstrap()` returns, so the *next* request the caller makes goes through the caller's normal
retry path (the producer's or consumer's own `retry` configuration) against the rebuilt cluster —
it is not itself a source of message loss, but a producer created with `idempotent: false` can
duplicate records the broker had already appended when a produce is retried. SASL authentication state
(the blackout applied after a rejected credential) is preserved across a rebootstrap, and a SASL
authentication failure is never a rebootstrap trigger — a rejected credential means the broker is
reachable and fine, only this client's credentials are bad, which no rebootstrap can fix.

## Default Retry

The `retry` option can be used to set the configuration of the retry mechanism, which is used to retry connections and API calls to Kafka (when using producers or consumers).

The retry mechanism uses a randomization function that grows exponentially.
[Detailed example](RetryDetailed.md)

If the max number of retries is exceeded the retrier will throw `KafkaJSNumberOfRetriesExceeded` and interrupt. Producers will bubble up the error to the user code; Consumers will wait the retry time attached to the exception (it will be based on the number of attempts) and perform a full restart.

__Available options:__

The defaults below apply to the general retrier, including transaction control requests,
admin and consumer calls. Producer delivery retries instead default to 2147483647 retries,
with a 100 ms initial backoff and a 1000 ms cap, bounded by the delivery deadline.
Explicit retry settings override these defaults key by key; see [Producer retry](Producing.md#retry).

| option              | description                                                                                                             | default             |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------- | ------------------- |
| maxRetryTime        | Maximum wait time for a retry in milliseconds                                                                           | `30000`             |
| initialRetryTime    | Initial value used to calculate the retry in milliseconds (This is still randomized following the randomization factor) | `300`               |
| factor              | Randomization factor                                                                                                    | `0.2`               |
| multiplier          | Exponential factor                                                                                                      | `2`                 |
| retries             | Max number of retries per call                                                                                          | `5`                 |
| maxTotalTime        | Deadline in ms for one call, from its first attempt: when the wait before the next attempt would end past it, the call rejects with `KafkaJSDeliveryTimeout`, whose `cause` is the last attempt's error. Running out of `retries` first still rejects with `KafkaJSNumberOfRetriesExceeded`. An integer from 1 to 2147483647. A producer's `deliveryTimeout` (120000 ms unless set) sets it for that producer's sends; the producer's own `maxTotalTime`, if you set one, is used when it sets no `deliveryTimeout`. Producer sends and transaction retries, `consumer.commitOffsets()`, the consumer group's join and sync, admin calls, and the client's connect, metadata and coordinator lookups honour it. The consumer's fetch loop (with the auto-commits made inside it) ignores it, because the loop runs as one operation for as long as `run()` lasts; so do the consumer-protocol heartbeat backoff and position validation, which uses its own fixed retry budget | none (no deadline) |
| restartOnFailure    | Only used in consumer. See [`restartOnFailure`](#restartonfailure)                                                      | none (restart on retriable errors only) |

Example:

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  retry: {
    initialRetryTime: 100,
    retries: 8
  }
})
```

### `restartOnFailure`

An async function that will be invoked after the consumer exhausts all retries, to decide whether or not to restart the consumer (essentially resetting `consumer.run`). This can be used to, for example, cleanly shut down resources before crashing, if that is preferred. The function will be passed the error, which allows it to decide based on the type of error whether or not to exit the application or allow it to restart.

A fenced static instance and a fatal membership error under the consumer protocol never restart, even if `restartOnFailure` returns `true`. A duplicate `groupInstanceId` must be removed, or the rejected membership configuration, authorization or broker capability must be corrected first. Other terminal errors retain the callback policy.

For a classic consumer, a lost broker connection or a moved group coordinator on its own requests is not a terminal death while it lasts less than `sessionTimeout`: the consumer retries it without crashing and without calling this function (see [Membership errors during rebalance](Consuming.md#membership-errors)). An error from your handler, and a consumer with `groupProtocol: 'consumer'`, still count against `retry.retries`.

The function has the following signature: `(error: Error) => Promise<boolean>`

* If the promise resolves to `true`: the consumer will restart
* If the promise resolves to `false`: the consumer will **not** restart
* If the promise rejects: the consumer will restart
* If `consumer.disconnect()` (or `stop()`) is called while the callback is still deciding: the consumer will **not** restart, whatever the callback returns
* If there is no `restartOnFailure` provided: the consumer restarts on retriable errors and stops on non-retriable ones (the previous default behavior, unchanged for callers who don't set the callback)

When `restartOnFailure` is provided, it is authoritative for **every** terminal death of the consumer, retriable or not — this is a deliberate change from earlier versions, where the callback was only consulted for retriable errors. If you need to tell retriable and non-retriable errors apart inside the callback, inspect the error passed to it (for example `error.retriable` or `error.name === 'KafkaJSNumberOfRetriesExceeded'`).

## Offset reset policy

Offset reset policy is configured per subscribe call, rather than in the Kafka constructor. See [fromBeginning](Consuming.md#from-beginning) for earliest, latest, none and by_duration, their explicit fromBeginning conflicts and runtime validation. The duration declaration requires TypeScript 4.1 or later.

## Logging

KafkaJS has a built-in `STDOUT` logger which outputs JSON. It also accepts a custom log creator which allows you to integrate your favorite logger library. There are 5 log levels available: `NOTHING`, `ERROR`, `WARN`, `INFO`, and `DEBUG`. `INFO` is configured by default.

##### Log level

```javascript
const { Kafka, logLevel } = require('@countly/kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  logLevel: logLevel.ERROR
})
```

To override the log level after instantiation, call `setLogLevel` on the individual logger.

```javascript
const { Kafka, logLevel } = require('@countly/kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  logLevel: logLevel.ERROR
})
kafka.logger().setLogLevel(logLevel.WARN)

const producer = kafka.producer(...)
producer.logger().setLogLevel(logLevel.INFO)

const consumer = kafka.consumer(...)
consumer.logger().setLogLevel(logLevel.DEBUG)

const admin = kafka.admin(...)
admin.logger().setLogLevel(logLevel.NOTHING)
```

The environment variable `KAFKAJS_LOG_LEVEL` can also be used and it has precedence over the configuration in code, example:

```sh
KAFKAJS_LOG_LEVEL=info node code.js
```

> Note: for more information on how to customize your logs, take a look at [Custom logging](CustomLogger.md)

## Custom socket factory

To allow for custom socket configurations, the client accepts an optional `socketFactory` property that will be used to construct
any socket.

`socketFactory` should be a function that returns an object compatible with [`net.Socket`](https://nodejs.org/api/net.html#net_class_net_socket) (see the [default implementation](../src/network/socketFactory.js)).

```javascript
const { Kafka } = require('@countly/kafkajs')

// Example socket factory setting a custom TTL
const net = require('net')
const tls = require('tls')

const myCustomSocketFactory = ({ host, port, ssl, onConnect }) => {
  const socket = ssl
    ? tls.connect(
        Object.assign({ host, port }, ssl),
        onConnect
      )
    : net.connect(
        { host, port },
        onConnect
      )

  socket.setKeepAlive(true, 30000)

  return socket
}

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  socketFactory: myCustomSocketFactory,
})
```

### Proxy support

Support for proxying traffic can be implemented with a custom socket factory. In the example
below we are using [proxy-chain](https://github.com/apify/proxy-chain) to integrate with the
proxy server, but any other proxy library can be used as long as the socket factory interface
is implemented.

```javascript
const tls = require('tls')
const net = require('net')
const { createTunnel, closeTunnel } = require('proxy-chain')

const socketFactory = ({ host, port, ssl, onConnect }) => {
  const socket = ssl ? new tls.TLSSocket() : new net.Socket()

  createTunnel(process.env.HTTP_PROXY, `${host}:${port}`)
    .then((tunnelAddress) => {
      const [tunnelHost, tunnelPort] = tunnelAddress.split(':')
      socket.setKeepAlive(true, 60000)
      socket.connect(
        Object.assign({ host: tunnelHost, port: tunnelPort, servername: host }, ssl),
        onConnect
      )

      socket.on('close', () => {
        closeTunnel(tunnelServer, true)
      })
    })
    .catch(error => socket.emit('error', error))

  return socket
}
```