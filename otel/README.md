# OpenTelemetry instrumentation for `@countly/kafkajs`

This package instruments [`@countly/kafkajs`](https://github.com/Countly/kafkajs), the KafkaJS
fork for Apache Kafka 4.0 and later. It is derived from
[`@opentelemetry/instrumentation-kafkajs`](https://www.npmjs.com/package/@opentelemetry/instrumentation-kafkajs)
0.32.0, tag `instrumentation-kafkajs-v0.32.0` of
[open-telemetry/opentelemetry-js-contrib](https://github.com/open-telemetry/opentelemetry-js-contrib/tree/instrumentation-kafkajs-v0.32.0/packages/instrumentation-kafkajs)
(commit `cc263ac4e94320402ac3afe50d50eb00905e1bb8`), by the OpenTelemetry Authors and Aspecto, and
is licensed under Apache-2.0 like it (see [LICENSE](LICENSE)).

The upstream package does not instrument this client: it hooks the package `kafkajs` at versions
below 3. Replace it with this one; spans and metrics keep their names.

## Installation

```bash
npm install @countly/opentelemetry-instrumentation-kafkajs @opentelemetry/api
```

Supported: `@countly/kafkajs` `>=3.0.0-0 <4` (the 3.0.0 prereleases included), on Node.js 22 or
later. The client is matched under either name it can be installed as: `@countly/kafkajs`, or
`kafkajs` when it is installed through the npm alias `"kafkajs": "npm:@countly/kafkajs@3"`. A
`kafkajs` 2.x install is left to `@opentelemetry/instrumentation-kafkajs`.

## Usage

```js
const { NodeSDK } = require('@opentelemetry/sdk-node')
const { KafkaJsInstrumentation } = require('@countly/opentelemetry-instrumentation-kafkajs')

const sdk = new NodeSDK({
  instrumentations: [
    new KafkaJsInstrumentation({
      // producerHook(span, { topic, message }) and consumerHook(span, { topic, message }) can add
      // attributes before a message is sent or processed.
    }),
  ],
})
sdk.start()

// Only after the instrumentation is enabled:
const { Kafka } = require('@countly/kafkajs')
```

The instrumentation must be enabled before the client is first required; a client required
earlier is left as it is.

`disable()` stops instrumenting clients created afterwards; clients created while enabled stay
instrumented. It restores `Kafka#producer` and `Kafka#consumer`, and the producers and consumers
those had already created keep their wrapped methods and their `REQUEST` listener, as with the
upstream package.

**ESM applications** need the loader hook, as with the upstream package: start Node.js with
`--experimental-loader=@opentelemetry/instrumentation/hook.mjs`. Without it an
`import { Kafka } from '@countly/kafkajs'` is not instrumented (and nothing fails). Node.js
discourages `--experimental-loader` and deprecates `module.register()` (DEP0205, a runtime
deprecation from Node.js 26) in favour of `module.registerHooks()`. The synchronous hooks of
`import-in-the-middle` 3.1.0 and later (`import-in-the-middle/register-hooks.mjs`, on Node.js
22.22.3, 24.11.1, 26.0.0 or later) instrument the same import when registered with `--import`;
this package's tests check that path, and this package will follow upstream when upstream
documents it.

## Spans, metrics and attributes

| Object | Action | Span kind | Span name | Operation type / name |
| --- | --- | --- | --- | --- |
| Consumer | `eachBatch` | Client | `poll <topic>` | `receive` / `poll` |
| Consumer | `eachBatch`, `eachMessage` | Consumer | `process <topic>` | `process` / `process` |
| Producer | `send`, `sendBatch` | Producer | `send <topic>` | `send` / `send` |
| Producer | `transaction` | Internal | `transaction` | — |

| Metric | Recorded for |
| --- | --- |
| `messaging.process.duration` | each consumed message (in `eachBatch`, the whole batch's duration) |
| `messaging.client.consumed.messages` | consumed messages |
| `messaging.client.operation.duration` | every request the client sends, from its `REQUEST` event |
| `messaging.client.sent.messages` | messages a producer attempted to send |

Attributes: `messaging.system`, `messaging.destination.name`, `messaging.destination.partition.id`,
`messaging.operation.type`, `messaging.operation.name`, `messaging.kafka.message.key`,
`messaging.kafka.message.tombstone`, `messaging.kafka.offset`, `messaging.batch.message_count`,
`error.type`, `server.address` and `server.port`. The producer writes the span context into each
message's `headers` (the caller's message object), and the consumer reads it back.

## Differences from `@opentelemetry/instrumentation-kafkajs` 0.32.0

- **No `messaging.kafka.cluster.id`.** Upstream reads it from the client's internal modules
  (`src/producer/index.js`, `src/consumer/index.js`); this package patches the public API only.
- **Module names and range:** `@countly/kafkajs` and `kafkajs`, both `>=3.0.0-0 <4`, instead of
  `kafkajs` `>=0.3.0 <3`.
- **Plain CommonJS** with JSDoc and a hand-written `src/index.d.ts`; no build step. The version and
  instrumentation name come from this package's `package.json`.
- **A transaction has one span, ended by the first `commit()` or `abort()` that settles**, as
  upstream's does. A `commit()` of this client that cannot learn the outcome rejects with a
  retriable error (the rejected error has `retriable: true`) and stays pending; the span ends
  `ERROR` and records, as upstream's does, only that error's message and its class name as
  `error.type`, not `retriable`. An `ERROR` `transaction` span after such a rejection means that
  the outcome of the commit is unknown, not that the transaction aborted, and the `commit()` that
  later completes it, yours or the one `producer.disconnect()` sends, is not recorded on the span,
  which has already ended. A `commit()` made while another is under way settles with it on the
  client, but the instrumented `commit()` returns its own promise to each caller, so the two
  promises are not `===`, although the client's are.
- **A handler that throws synchronously still ends its spans and records its failure metrics**,
  like one that rejects. A rejection reason of `null`, or an object with a `message` but no
  constructor, is passed on unchanged and recorded with `error.type` `_OTHER`. A broker address that
  is IPv6, such as `::1:9092`, gives `server.address` `::1` and `server.port` `9092`.
- **Telemetry is guarded on the send path it wraps.** A meter, span or span processor that throws
  while the send span is prepared or ended or the sent-message metrics are recorded, a frozen or
  sealed message or headers, and a missing `messages` array neither skip the produce nor change its
  result, though some messages may then be sent without trace context. Other telemetry calls are not
  guarded: for example, a throwing `REQUEST` duration metric escapes into the client's event emit,
  and a throwing transaction span replaces a transactional send's own error.

## Tests

The upstream suite (`test/kafkajs.test.ts`) is ported to `node:test` block for block, with the
same mocks. The 14 tests of its `cluster id` block are not ported, because the attribute is never
set:

1. sets messaging.kafka.cluster.id on producer spans when cluster ID is available
2. does not set messaging.kafka.cluster.id when cluster provides no ID
3. does not set messaging.kafka.cluster.id when cluster ID is null
4. does not set messaging.kafka.cluster.id when cluster ID is not a string
5. sets messaging.kafka.cluster.id on sendBatch spans when cluster ID is available
6. does not set messaging.kafka.cluster.id when cluster ID is empty string
7. sets messaging.kafka.cluster.id on consumer eachMessage span when cluster ID is available
8. reads cluster id fresh on every send without caching
9. does not call admin() to resolve cluster id
10. does not break message flow when cluster ID lookup throws
11. sets messaging.kafka.cluster.id on consumer eachBatch spans when cluster ID is available
12. does not add any own property or symbol to the Kafka instance while capturing the cluster
13. leaves a non-function module export untouched
14. captures params.cluster from the wrapped factory and surfaces it on a span for that client

Run them from the repository root with `npm run test:otel`, which installs the packed client into
this directory the way an application does.
