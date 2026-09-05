# Migrating from kafkajs 2.2.4 to @countly/kafkajs 3.0.0

`@countly/kafkajs` is a drop-in replacement for `kafkajs` 2.2.4 targeting Apache Kafka 4.0+ and
Node.js 22/24/26: the public API, `require`/`import` entry points and event names are unchanged,
so most applications need no code changes. This guide lists every behavior change a caller might
actually notice, phase by phase, as the fork is developed. It grows with each release; the goal is that a
change not listed here did not change observable behavior.

### Phase 4 — flexible protocol foundation

- **CRC32C verification is on by default (`checkCrcs`)**. The consumer now checks every fetched
  record batch's CRC32C checksum before delivering it, instead of reading and discarding the
  checksum as kafkajs 2.2.4 did. The cost is proportional to batch size, not record count: an
  isolated decode-loop benchmark measured roughly +0.6µs on a 481B/10-record batch, +19µs on a
  ~21KB/100-record batch, and +225µs on a ~250KB/500-record batch (median of 3 runs). Against
  real end-to-end fetch latency — network round trip plus broker-side I/O — this is expected to
  be a much smaller fraction, though that end-to-end number has not been measured. In exchange,
  a corrupted batch is now caught instead of silently delivered: on a mismatch, no message from
  that batch reaches `eachMessage`/`eachBatch`, the consumer crashes with a non-retriable
  `KafkaJSPartialMessageError` (`crcMismatch: true`), the `CRASH` event's `restart` follows your
  `retry.restartOnFailure` as usual, and the offset is **not** advanced past the corrupt batch —
  a restart re-reads it rather than skipping the bad data. Pass `checkCrcs: false` on the
  consumer (or on `Broker#fetch`) to restore the 2.2.4 behavior of delivering corrupt records
  without checking them.
- **The client now identifies itself to brokers as `countly-kafkajs`** (KIP-511), carrying this
  package's version. Operators watching a broker's `clientSoftwareName`/`clientSoftwareVersion`
  metrics or request logs will see this name where kafkajs 2.2.4 previously showed `unknown`.
- **Negotiated protocol versions moved up**: ApiVersions from v2 to v4, and Metadata from v6 to
  v12. ApiVersions is now re-negotiated on every (re)connect to a broker rather than once per
  broker object, so each (re)connect costs one extra round trip per broker.
- **ApiVersions failures surface differently**. When a broker rejects every ApiVersions version
  the client can send, `connect()` now rejects with the broker's own `KafkaJSProtocolError`
  (`The version of API is not supported`) instead of a `KafkaJSNonRetriableError('API Versions
  not supported')`. A broker that rejects the client's software name or version
  (`INVALID_REQUEST`) closes that connection and rejects with a `KafkaJSNonRetriableError` that
  keeps the protocol error's `type`, `code` and `cause`. Code that matched the old message text
  needs updating; code that checks `retriable` behaves as before.
- **`cluster.metadata()` no longer returns `clusterAuthorizedOperations`**. That top-level field
  only exists up to Metadata v10 and was never requested (it was always `-2147483648`); at v11+
  it is simply absent. The new per-topic `topicId` / `topicAuthorizedOperations` and per-partition
  `leaderEpoch` keys are additive; every key kafkajs 2.2.4 returned is still there.
- **`Broker#isConnected()` also requires a completed version negotiation**. A `Broker` handed
  back by `Cluster#findBroker` and friends reports `true` only once its socket is up **and**
  ApiVersions has been negotiated, so a caller can no longer observe a connected-but-unusable
  broker. This can only turn a previously wrong `true` into a correct `false`.
