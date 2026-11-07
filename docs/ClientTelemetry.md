---
id: client-telemetry
title: Client Telemetry
---

## Protocol codecs

GetTelemetrySubscriptions (71) and PushTelemetry (72) implement flexible version 0,
including request header version 2 and response header version 1. Their structured
request and response vectors record independent bytes and literal fields.

Runtime use requires both telemetry APIs and a working broker exporter. A broker
version number alone does not establish telemetry support. The options and methods below enable the subscription loop and numeric registration API.
See [Testing](Testing.md#telemetry-wire-evidence) for the broker-free wire checks.

## Numeric metric model

Producer, ordinary consumer, admin and ShareConsumer each expose the methods below
and own independent numeric state; ShareConsumer uses the consumer names.

| Name | Type | Unit | Measurement |
|---|---|---|---|
| `org.apache.kafka.producer.connection.creation.total` | monotonic Sum | `1` | Successful normal physical socket establishments; no attributes. |
| `org.apache.kafka.producer.node.request.latency.avg` | Gauge | `ms` | Mean completed-response RTT since the previous written sample; node_id. |
| `org.apache.kafka.producer.node.request.latency.max` | Gauge | `ms` | Maximum over the same window; node_id. |
| `org.apache.kafka.consumer.connection.creation.total` | monotonic Sum | `1` | Successful normal physical socket establishments; no attributes. |
| `org.apache.kafka.consumer.node.request.latency.avg` | Gauge | `ms` | Mean completed-response RTT since the previous written sample; node_id. |
| `org.apache.kafka.consumer.node.request.latency.max` | Gauge | `ms` | Maximum over the same window; node_id. |
| `org.apache.kafka.admin.connection.creation.total` | monotonic Sum | `1` | Successful normal physical socket establishments; no attributes. |
| `org.apache.kafka.admin.node.request.latency.avg` | Gauge | `ms` | Mean completed-response RTT since the previous written sample; node_id. |
| `org.apache.kafka.admin.node.request.latency.max` | Gauge | `ms` | Maximum over the same window; node_id. |

The six producer and consumer templates are the required standard set. The three
admin names are explicit extensions. Node IDs are decimal broker IDs; unknown nodes
omit the latency points. No endpoint, topic, record, header, correlation ID or secret
is a metric label. A collector retains at most 64 current node slots (129 built-in
series) and 128 custom series, for 257 series in total. Node overflow retires a slot;
late responses cannot relabel a retired slot. A logical connect that reuses a socket
adds no connection. Dedicated telemetry traffic is excluded from observation.

A singleton `['*']` selects all metric names. `['']` also matches all names by prefix.
Other lists are case-sensitive literal prefixes: a star inside a mixed list has no
wildcard meaning. An empty selection pauses RTT collection while retaining normal
physical connection totals and registered descriptions.

DELTA Sum values cover the interval since the last actual included write. The first
sample may cover metric origin. CUMULATIVE Sum values retain the lifetime total.
Changing from cumulative to delta starts at the accepted subscription boundary;
changing back uses the original cumulative total. An omitted or unsent point does
not advance its baseline. A written point advances before the broker response;
response loss or rejection does not replay it. Latency snapshots swap windows so
later observations survive, and abandonment merges unsent windows back. Observed
zero gauges remain visible; a latency gauge without a sample is omitted.

Sum values are nonnegative BigInts through 9223372036854775807; timestamps use exact
unsigned 64-bit nanoseconds. Gauges use finite Numbers. No absolute nanosecond time
or Sum passes through Number. Resource attributes are limited to applicable nonempty
client_rack, group_id, group_instance_id, group_member_id and transactional_id.
ShareConsumer omits group_instance_id; admin omits group and transaction fields.
Strings over 512 UTF-8 bytes are omitted intact and counted as dropped attributes.
Client identity and software context supplied by the broker are not duplicated.

### Numeric snapshot registration

Registration stores an explicit value; it invokes no callback, Promise or OTel SDK.
A custom name is case-preserved ASCII, contains a dot and matches
`^[A-Za-z][A-Za-z0-9_.]{0,199}$`; `org.apache.kafka.` is reserved. A Gauge accepts a
finite Number, including negative, fractional and zero values. A Sum accepts a
nonnegative monotonic BigInt. Re-registering the same name and sorted string
attributes updates one series. Decreasing a Sum or changing a name's type/unit is
invalid until all series of that name are unregistered. Unregister before resetting
a Sum. A new registration after removal gets a fresh metric lifetime even when its
value and clock equal the old registration.

The default unit is `1`; explicit units contain 1–16 printable ASCII bytes. A custom
identity has at most eight own enumerable string attributes with 1–63 byte identifier
keys and 0–256 UTF-8 byte values. Identity/resource names are reserved, including
node_id, principal, client_id, client_instance_id and broker software/source names.
Metadata is copied during the synchronous validated call, at most 4096 bytes per
descriptor and 512 KiB total; invalid input leaves storage unchanged. Throwing getters
produce a fixed configuration error, and sampling never reads them again. At the
128-series limit an existing identity can update; a new identity is refused.
Unregistration of an absent valid identity is a no-op. Unregistration invalidates an
unsent snapshot; an already written sample cannot be retracted. Descriptions survive
explicit disconnect/reconnect while transient snapshots and latency windows clear.

All four returned client kinds expose numeric registration methods. Existing
instrumentation and the optional OTel package remain available for application
metrics; no callback equivalence or SDK bridge is claimed here.

### Optional standard templates excluded in this release

Each template below has its own release disposition. Required connection and latency
templates remain implemented.

| Standard metric suffix after `org.apache.kafka.` | Disposition and reason |
|---|---|
| `producer.connection.creation.rate` | excluded in this release: cumulative connection total lets the receiver calculate rate; a JS sampled rate would add a window convention. |
| `consumer.connection.creation.rate` | excluded in this release: cumulative connection total lets the receiver calculate rate; a JS sampled rate would add a window convention. |
| `producer.produce.throttle.time.avg` | excluded in this release: existing REQUEST payload has no throttle duration (`src/network/requestQueue/socketRequest.js:128–140`); no producer-response expansion for this optional metric. |
| `producer.produce.throttle.time.max` | excluded in this release: the same missing throttle measurement cannot honestly supply a maximum. |
| `producer.record.queue.time.avg` | excluded in this release: pending request duration is not record accumulator residence (`src/network/requestQueue/socketRequest.js:88–90`); no false equivalent. |
| `producer.record.queue.time.max` | excluded in this release: request pending time likewise cannot supply maximum record accumulator residence. |
| `consumer.poll.idle.ratio.avg` | excluded in this release: push callbacks have no Java application `poll()` boundary; callback gaps measure a different property. |
| `consumer.coordinator.commit.latency.avg` | excluded in this release: network RTT is not the full coordinator commit measurement; ordinary-consumer lifecycle owns cadence and hooks. |
| `consumer.coordinator.commit.latency.max` | excluded in this release: network RTT likewise does not establish maximum coordinator commit latency. |
| `consumer.coordinator.assigned.partitions` | excluded in this release: a new metric lifetime across classic/consumer/manual/share modes is unspecified; existing assignment surfaces remain available on the existing consumer APIs. |
| `consumer.coordinator.rebalance.latency.avg` | excluded in this release: ordinary GROUP_JOIN duration is not the share assignment/acknowledgement lifecycle. |
| `consumer.coordinator.rebalance.latency.max` | excluded in this release: ordinary GROUP_JOIN does not establish a common maximum for those lifecycles. |
| `consumer.coordinator.rebalance.latency.total` | excluded in this release: ordinary GROUP_JOIN does not establish a common lifetime total for those lifecycles. |
| `consumer.fetch.manager.fetch.latency.avg` | excluded in this release: full fetch handling/decode latency differs from request RTT; required per-node RTT already diagnoses transport latency. |
| `consumer.fetch.manager.fetch.latency.max` | excluded in this release: request RTT likewise is not maximum full fetch handling/decode latency. |

## OTLP payload bounds and compression

The internal writer emits only OTLP MetricsData numeric Gauge and Sum messages.
It has no runtime protobuf or OTel SDK dependency. A zero-valued point remains a
point; a selection with no points produces a zero-byte blob. Sum values remain exact
signed 64-bit integers; timestamps are unsigned 64-bit nanoseconds. Neither passes
through a JavaScript Number. Point order is deterministic and equal-name aggregates
share one Metric. Oversize resource values are omitted intact, with the resource
dropped-attribute count retained.

The default and hard local raw-body bound is 1048576 bytes (1 MiB). Both raw protobuf
and transmitted Metrics bytes must fit the smaller positive local, broker and
temporary size-penalty limits. Whole points are retained in this priority: connection
Sum, average/maximum pairs in numeric node order, then canonical custom identities.
Fitting never splits a point, an attribute or a node pair, and never creates another
push within the interval. A tiny cap may omit every point and send a zero-byte blob.
The RPC framing is separate from the broker's Metrics-byte bound.

Above 1000 raw bytes, available built-in ZSTD or GZIP is selected in broker preference
order. At 1000 bytes or below the payload uses NONE. Unknown, duplicate, Snappy and
LZ4 entries do not invoke custom codecs. NONE is always available. A failed built-in
is suppressed for the current transport; a new transport resets that suppression.
Compression that exceeds the transmitted limit falls back to the already fitting
raw body. Producer compression registrations are unchanged.

Encoding and fitting do not advance metric baselines. Only included identities at
an authorized socket write advance them; response loss or rejection does not replay
already written data. Omitted or unsent points retain their unsent baseline. Metrics
are best effort and a rejected written sample can therefore be absent from a later
delta. Omission counts are local diagnostics, with at most one sanitized warning per
subscription revision. The options below configure these bounds on each returned client.

## Subscription transport and recovery

The subscription engine uses one dedicated broker socket per enabled client. It
inherits TLS, SASL and the socket factory, authenticates separately, and keeps its
queue, throttling and failure state separate from foreground operations. It uses
only current connected broker advertisements, requires version 0 of both telemetry
APIs, then negotiates again on the dedicated socket and completes a successful Get.
A missing exporter leaves telemetry dormant. A successful Get with no matching
CLIENT_METRICS resource keeps its UUID and polls without sending Push requests.
Telemetry remains opt-in through the public factory options below.

Each accepted Get replaces filters, codecs, cap, interval and temporality together.
The first Get uses a zero UUID until a current successful response assigns one; all
later Gets retain that ID. Subscription IDs are opaque signed integers. An empty
requested list polls; a nonempty list with no matching points sends a zero-byte
Metrics blob using no compression. A single `*` selects all; mixed lists are literal
case-sensitive prefixes, including an empty prefix.

The first Push waits 0.5 to 1.5 intervals. Subsequent Pushes and empty polls wait
from completion and respect the broker throttle. Nonpositive intervals use 300000 ms;
positive intervals below 100 ms are clamped to 100 ms for local resource safety.
Large delays use stepped timers, and a delayed callback never causes catch-up Pushes.
Each setup, serialization and RPC attempt has a 30000 ms local cap. The dedicated
queue enforces a 5000 ms response timeout and one in-flight request even when
foreground request-timeout enforcement is disabled. Response frames are limited to
65536 bytes or the configured smaller frame limit. Metadata is bounded before
array allocation: 128 codecs, 256 prefixes, 256 UTF-8 bytes per prefix, and 16384
aggregate prefix bytes.

| Result | Isolated next action |
|---|---|
| Success | Install a complete Get replacement or schedule the next normal Push. |
| 117 or 76 from Push | One immediate Get subject to throttle; repeated recovery before a successful Push backs off. A rejected codec stays suppressed if the refreshed list repeats it. |
| 117 or 76 from Get | Back off without recursive Get. |
| 118 | Halve the effective payload budget to at least one byte; wait interval and throttle, then Get. A larger refreshed cap cannot clear the penalty before a successful Push. |
| 89 | Wait at least interval and throttle, then Get. |
| 35 | Refuse this endpoint advertisement revision; switch only to an already-known eligible candidate after backoff, otherwise become dormant. |
| Retriable transport or local deadline | Retire only the dedicated socket and back off from 1000 ms exponentially, with 0.8–1.2 jitter and a final 30000 ms cap, then Get with the retained UUID. |
| 42, 87, unknown code, malformed success or non-retriable authentication failure | Stop this object's telemetry and leave foreground operation outcomes unchanged. |

A public reconnect after a terminating attempt keeps the accepted UUID and uses a
new socket. Its stream remains provisional until a successful normal Push. If that
provisional Push receives 42, one corrective retirement, backoff, nonzero-ID Get
and additional normal Push is permitted. The allowance is consumed before this
work; successful Gets and codec refreshes cannot refill it. A second 42 stops
telemetry. Established streams have no such exception. Closing a provisional
stream skips another terminating Push.

Metric baselines advance only at the authorized socket-write boundary, for the
complete points actually included. A later rejection or lost response does not
rewind a written sample. Unsent work preserves eligible sums and windows. Omitted
points are counted locally, with one fixed warning per subscription revision.
Diagnostics contain fixed text and never expose raw broker messages, metric
values, credentials, payloads or application causes.

Authentication uses one persistent client-owned shell for the entire mechanism,
including built-in OAuth token retrieval and custom providers. Retiring a socket
cancels its local wait but does not free the shell until the underlying work
settles. A later ordinary request captures its own authority for renewal, including
all internal SASL writes and readiness publication. It cannot inherit an expired
opening request or adopt a newer request midway through authentication.

A terminating Push is established-only: it never dials or starts authentication.
If authentication is due at preparation or becomes due while the final waits in
the queue, the final is abandoned and only its captured transport is retired.
An already-authenticated final can write once within the original close budget.

## Configuration and public methods

Broker client metrics (KIP-714) are available with `enableMetricsPush: true`; the default remains false. Kafka-wide defaults can be overridden on producer, consumer, admin and share consumer.

| Option | Kafka default | Per-client override | Validation |
|---|---|---|---|
| `enableMetricsPush` | `false` | Explicit `true` or `false` wins; omitted/undefined inherits. | Boolean only; null and coercible values reject. |
| `telemetryMaxBytes` | `1048576` | Explicit byte limit wins; omitted/undefined inherits. | Integer 1..1048576, even when telemetry is disabled. |

Both options are copied and validated at construction/factory entry. Invalid values or throwing option accessors produce `KafkaJSInvalidConfigurationError` before core or telemetry objects are constructed. Changing the original options later cannot enable traffic or change the limits. `telemetryMaxBytes` is independent of consumer fetch limits and response/decompression caps.

Enabling broker telemetry can open one additional broker connection per client and send GetTelemetrySubscriptions/PushTelemetry traffic. Unsupported brokers quietly leave foreground operations usable.

Budget for that dedicated connection's TLS/SASL authentication and interval requests. A missing exporter differs from a valid empty subscription: the latter keeps polling without pushes. Explicit false disables the additional connection and all telemetry collection.

```js
const { Kafka, KafkaJSTelemetryError } = require('@countly/kafkajs')
const kafka = new Kafka({
  brokers: ['localhost:9092'],
  enableMetricsPush: true,
  telemetryMaxBytes: 1048576,
})
const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'orders', enableMetricsPush: false })
const admin = kafka.admin({ enableMetricsPush: true, telemetryMaxBytes: 4096 })
const share = kafka.shareConsumer({ groupId: 'workers', enableMetricsPush: true })
await producer.connect()
try {
  const id = await producer.clientInstanceId({ timeout: 60000 })
  console.log(id)
} catch (error) {
  if (!(error instanceof KafkaJSTelemetryError)) throw error
  console.log(error.reason)
}
```

Each client exposes an asynchronous stable `clientInstanceId({ timeout })`; disabled/unsupported/closed/unavailable calls reject with `KafkaJSTelemetryError`.

The getter is available on producer, consumer, admin and share consumer, and is absent from the Kafka factory and transaction handles. It returns a lowercase canonical UUID belonging to that returned object. Siblings have independent IDs even with the same client ID, group or transaction ID. It never enables telemetry, connects, starts a stopped loop or resets a lifecycle.

Omitted options or timeout use 60000 ms. Timeout is an integer 0..2147483647; 0 is cached-only. Invalid options, including null, arrays, signal strings and throwing accessors, return a rejected Promise with `KafkaJSInvalidConfigurationError`; the call never throws synchronously. Validation precedes state checks. After validation: disabled wins; then a cached ID resolves even while closed, suspended or stopped; otherwise closed rejects `closed`, suspended or not foreground connected rejects `not-connected`, established unsupported rejects `unsupported`, and a fatally stopped telemetry stream rejects `stopped`. Eligible uncached callers wait independently and can reject `timeout`; a 65th concurrent waiter rejects `limit`. There are at most 64 waiters and one earliest-deadline timer. Expiry detaches only that caller and does not cancel the shared Get.

`KafkaJSTelemetryError` extends `KafkaJSNonRetriableError`. Its readonly `reason` is one of `disabled`, `not-connected`, `closed`, `unsupported`, `timeout`, `stopped` or `limit`; `apiKey` is optional. It does not indicate a producer-fatal or abortable transaction. Retrying a getter after a timeout is allowed. Only the error class is a runtime export; import types and the error from the root entry point. CommonJS, ESM named/default imports and the supported package alias use the same error constructor within their module instance. Internal telemetry modules have no exported deep paths.

### Registration on returned clients

Applications may register/unregister finite numeric Gauge and monotonic BigInt Sum snapshots for broker subscriptions.

```js
producer.registerMetricForSubscription({ name: 'com.example.queue.depth', type: 'gauge', value: 3 })
producer.registerMetricForSubscription({ name: 'com.example.processed.total', type: 'sum', value: 5n })
producer.registerMetricForSubscription({ name: 'com.example.processed.total', type: 'sum', value: 8n })
producer.unregisterMetricFromSubscription({ name: 'com.example.queue.depth' })
```

The methods are synchronous and return void. They work on all four kinds. Registering while disabled throws telemetry `disabled`; unregistering an absent valid identity remains a no-op while disabled. Enabled registration stores bounded values before connect, after disconnect and while stopped; it opens no socket, restarts no machine, and survives explicit reconnect. Invalid descriptors throw `KafkaJSInvalidConfigurationError` before mutation. No callback or OTel SDK is sampled. Use existing instrumentation or optional OTel instead if you do not want to register broker metrics.

Telemetry buffers, metric identities, labels and timers have fixed bounds; `telemetryMaxBytes` may lower the 1 MiB default. Small limits may omit metrics or send an empty blob.

At most 64 node slots and 128 custom series are retained. The 65th ID waiter's refusal is independent of those series limits. Both raw and transmitted bytes respect the smaller local/broker maximum. Custom overflow explicitly rejects; there is no unbounded setting. Lost or rejected written samples may be absent from subsequent deltas. Intervals below 100 ms clamp locally to 100 ms. Lower `telemetryMaxBytes` or disable telemetry to reduce resource/traffic costs; existing response and decompression limits are unchanged.

### Resource disclosure and lifecycle

Telemetry includes only applicable rack/group/static-member/current-member/transaction identity attributes, library scope/version and per-node metric labels.

The whitelist is `client_rack`, `group_id`, `group_instance_id`, `group_member_id`, `transactional_id`. Consumer without a group omits all group fields; ShareConsumer has no ordinary static-member field; admin omits group and transaction fields. Current membership comes from the lifecycle owner, with issued revision checks. Values over 512 UTF-8 bytes are omitted intact and increase `dropped_attributes_count`. Scope name is `countly-kafkajs`, with the package version. Node IDs label the destination broker's RTT, separately from broker exporter context. Credentials, records, headers, arbitrary options, environment and principal are never collected. Treat group/transaction identities as information disclosed to the configured receiver. Disable telemetry to opt out of the standard attributes; no per-attribute opt-out is added.

Disconnect sends a best-effort terminating telemetry push on an active connected subscription, for at most 1000 ms within the existing close budget; timeout0 sends no final.

Telemetry cleanup starts concurrently with core draining, under the first valid inherited close deadline. It never reserves or subtracts core drain time or changes the original close result/error. Existing close overloads, signal callbacks and natural/unbounded core behavior remain as documented for each client; no new disconnect timeout option is introduced. Use `disconnect({ timeout: 0 })` only on a client whose inherited close supports it, or disable telemetry, to omit the final.

A final never reconnects, authenticates, gets a subscription or retries. Public reconnect retains UUID and registrations and uses nonzero-ID Get; the first successful normal Push establishes the reopened stream. Only its first provisional Push 42 permits one corrective retirement/backoff/Get/additional normal Push. Get cannot replenish this allowance; a second 42 or other fatal result stops only telemetry. No UUID rotation, terminating resend or foreground reconnect is authorized. A provisional close skips another final.

Internal consumer/share crash cleanup suspends telemetry without a final. Expired uncached waiters reject `timeout`, remaining pending waiters reject `not-connected`, and none transfer to a replacement lifecycle. Ordinary transport retry preserves waiter deadlines. `stop()` alone keeps connected telemetry active. Seek, rejoin, topic recreation and reassignment retain the UUID. Existing instrumentation events and diagnostics channels keep their names and payloads; broker telemetry is distinct from the optional OTel integration.
