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
  v12. ApiVersions is now re-negotiated whenever a broker is (re)connected through `connect()`
  rather than once per broker object, so each such (re)connect costs one extra round trip per
  broker. A fetch socket that the connection pool re-establishes on its own, while the broker's
  other socket stays up, keeps the current version table until the next `connect()`.
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
- **Error codes 90–133 are now recognised**. Errors a Kafka 4.x broker returns with those codes
  used to surface as `type: 'KAFKAJS_UNKNOWN_ERROR_CODE'`; they now carry their real type (for
  example `PRODUCER_FENCED`, `TRANSACTION_ABORTABLE`, `REBOOTSTRAP_REQUIRED`) and a `retriable`
  flag derived from Kafka's own exception hierarchy, so codes 100, 103, 106, 122, 123 and 133 are
  retried where 2.2.4 failed fast. Types, messages and flags of codes 0–89 are unchanged.
- **Metadata results gain fields**. Partition objects returned by `admin.fetchTopicMetadata()`
  (and `cluster.metadata()`) now include `leaderEpoch`; topics include `topicId` and
  `topicAuthorizedOperations`. Existing keys are unchanged; deep-equality snapshots of these
  results need updating.

### Phase 5A — core APIs, part 1

- **Client rebootstrap is on by default.** Two new `Kafka` options control it:
  `metadataRecoveryStrategy`, which is `'rebootstrap'` by default and `'none'` to opt out, and
  `metadataRecoveryRebootstrapTriggerMs`, which defaults to `300000` and must be an integer of at
  least `1`. With the default strategy the client rebuilds its connections and its metadata from
  the broker list you configured when any of three things happen: metadata has been failing
  *continuously* for the trigger window, a broker answers `REBOOTSTRAP_REQUIRED` (error code 129),
  or every broker the client knows about has become unreachable. "Continuously" is measured from
  the first failure that follows a success, and any success clears it — so an idle client that hits
  one transient metadata failure and then succeeds never rebootstraps, however long it has been
  running. A rebootstrap waits for each connection's in-flight requests to drain and then closes
  every connection, so requests still outstanding fail with `KafkaJSConnectionClosedError` and go
  through the caller's normal retry path against the rebuilt cluster. Until the idempotent producer
  is on by default, that retry is not deduplicated: **a produce request that the broker had already
  appended, but whose response was lost when the connection closed, is sent again on the retry and
  can duplicate those records.** The SASL authentication blackout survives a rebootstrap, and an
  authentication failure is never a trigger — a rejected credential means the broker is reachable
  and healthy, which no rebootstrap can fix. Setting `metadataRecoveryStrategy: 'none'` disables
  all three triggers and restores the previous behavior exactly, with code 129 surfacing as the
  non-retriable protocol error it already was.
- **A recreated topic resets that topic's consumer offsets once.** Previously, when a topic was
  deleted and recreated under a running consumer, the consumer kept using the committed offset from
  the *previous* incarnation. Because the cleanup of `__consumer_offsets` is asynchronous, that
  stale offset was often still there, and if it happened to fall inside the new topic's offset
  range the broker accepted it: the consumer resumed in the middle of the new log and silently
  skipped every record before that point, with no `OFFSET_OUT_OF_RANGE` and nothing in the log to
  say so. Now a metadata refresh that sees a subscribed topic come back with a different topic id
  emits `TOPIC_RECREATED`, drops the client's cached state for that topic, and resolves that
  topic's next offsets from the boundary the subscription configured (`fromBeginning`) instead of
  from the coordinator's committed offset. The reset is **one-shot** — it applies at the first
  offset resolution after the recreation and then stops, so offsets committed against the *new*
  incarnation are never discarded — and it is per that subscription's `fromBeginning`, not a global
  reset. Partitions you paused stay paused across a recreation, because pausing is user intent
  rather than offset state. There is no opt-out. An application that relied on offsets surviving a
  topic recreation must stop relying on it; it was never a guarantee the broker made.
- **Produce errors carry per-record detail.** The error thrown by a failed `send()`/`sendBatch()`
  now carries `topic`, `partition`, the broker's own `errorMessage`, and, when the broker rejected
  individual records rather than the whole partition, `recordErrors`: an array of
  `{ batchIndex, record, errorMessage }`, where `record` is the message object you passed in, not a
  copy or an index into a flattened list. `type`, `code` and `retriable` are unchanged. Which
  failure is reported is also unchanged — the first failing partition in the response, exactly as
  before — and there is still no automatic re-batching: a batch containing one rejected record
  fails as a whole, as it always did.
- **The producer follows leader hints.** When a produce request fails with a leadership error, a
  Kafka 4.x broker also names the node that now leads the partition and, where the client may not
  know it, that node's address. The producer now applies both immediately, so its next retry
  attempt goes straight to the new leader instead of routing to the old one until the metadata
  refresh finishes. Retry timing, the retry unit (the whole per-broker request) and the idempotent
  producer's sequence bookkeeping are all unchanged. The one consequence to know about: if a hint
  replaces the connection to a broker that had *already succeeded* within the current `send()`,
  that broker's partitions are produced again on the next attempt. This slightly widens the
  duplicate-on-retry window that already existed, and the array `send()` resolves with can then
  contain two `RecordMetadata` entries for the same partition instead of one.
- **Negotiated protocol versions moved up.** Against a Kafka 4.1 or 4.3 broker the client now
  negotiates: Metadata **v13** (was v12), Produce **v11** (was v7), FindCoordinator **v6** (was
  v2), OffsetCommit **v8** (was v5), OffsetFetch **v8** (was v4), ListOffsets **v9** (was v3) and
  InitProducerId **v5** (was v1). What each means for a caller:
  - *Metadata v13* adds a top-level error code to the response, surfaced ahead of any topic- or
    partition-level error; it is how a broker asks a client to rebootstrap. Decoded metadata gains
    an additive `errorCode`.
  - *Produce v11* moves the hottest write path onto the compact (flexible) encoding and is what
    makes the per-record errors and leader hints above possible.
  - *FindCoordinator v6* takes a list of coordinator keys and answers with a list; this client asks
    for one and reads one, so the decoded result keeps the shape it always had.
  - *OffsetCommit v8* is what finally sends `groupInstanceId` on offset commits for consumers using
    static membership — previously it reached JoinGroup, SyncGroup, Heartbeat and LeaveGroup only.
    The per-partition committed leader epoch the protocol gained at v6 is sent as its default
    (`-1`): it means the leader epoch of the *consumed record*, which this release does not track.
  - *OffsetFetch v8* takes and returns a list of groups; again one is sent and one is read. Each
    partition additionally carries `committedLeaderEpoch`, which nothing in this release reads.
    `RequireStable` is sent `false`, so a transactional producer's in-flight offsets are returned
    exactly as before.
  - *ListOffsets v9* sends the leader epoch the client currently believes is in effect for each
    partition (`-1` when it does not know one), so a broker can reject a request built on metadata
    that has since moved; the response's per-partition leader epoch is decoded and returned
    additively. Because the request now carries an epoch, `FENCED_LEADER_EPOCH` and
    `UNKNOWN_LEADER_EPOCH` can now come back from the offset APIs, where they could not before —
    the client refreshes metadata and retries, so callers see a slower call rather than a failure.
  - *InitProducerId v5* sends the producer id and epoch fields added at v3 as their defaults
    (`-1`), since this release does not re-initialise a fenced producer under its previous identity.
    `PRODUCER_FENCED` and `TRANSACTION_ABORTABLE` can now reach an idempotent or transactional
    producer's initialisation, both as the non-retriable protocol errors they already are.

  Two versions are deliberately not implemented. **ListOffsets stops at v9**: v10 and v11 exist for
  tiered storage — asynchronous remote list-offsets and the earliest pending upload offset — which
  this client does not use, and both change the request's shape and semantics. **InitProducerId
  stops at v5**: v6 is the two-phase-commit protocol, which upstream still marks unstable, so
  brokers do not advertise it by default.
- **Six new instrumentation events.** `TOPIC_RECREATED` and `REBOOTSTRAP` are available on all
  three clients. Subscribe with `client.events.TOPIC_RECREATED` / `client.events.REBOOTSTRAP` as
  usual; the `type` string delivered on the event is `consumer.cluster.topic_recreated`,
  `producer.cluster.topic_recreated`, `admin.cluster.topic_recreated`,
  `consumer.cluster.rebootstrap`, `producer.cluster.rebootstrap` and
  `admin.cluster.rebootstrap` respectively. `TOPIC_RECREATED`'s payload is
  `{ topic, previousTopicId, topicId }` and fires when a topic name the client already had metadata
  for comes back with a different topic id; on a consumer it accompanies the one-shot offset reset
  described above, while on a producer and an admin client it is purely observational.
  `REBOOTSTRAP`'s payload is `{ reason }`, where `reason` is `'timeout'`, `'required'` or
  `'all-brokers-unavailable'` for the three triggers described above; it never fires under
  `metadataRecoveryStrategy: 'none'`.
- **The `COMMIT_OFFSETS` event's payload gains `groupInstanceId`.** It is `string | null` and is
  present for every consumer, not only those configured for static membership — the same additive
  change `HEARTBEAT`'s payload received in the previous release. Every key the payload already had
  is unchanged, so only code that deep-equality-compares the whole payload is affected.

### Phase 5B — core APIs, part 2

- **A fetch error on one partition no longer discards the whole response or rejoins the group.** A
  leader move, an unknown topic id, or an offset not yet available on a follower used to fail the
  entire fetch: every partition in that response was thrown away, and the consumer left and rejoined
  its group to recover. Now only the affected partition is skipped, and only for one round — the
  other partitions in the same response are delivered as usual, and the group is left alone. At most
  one metadata refresh is issued per fetch round however many partitions asked for one. A partition
  that keeps failing is not silent: after five consecutive failing rounds the consumer logs a
  warning, and another every twenty-five rounds after that, so a partition that is quietly making no
  progress is visible at the default log level. Note also that fetch requests now carry the leader
  epoch the client believes is in effect for each partition (`CurrentLeaderEpoch`, a field that has
  existed since Fetch v9 and that this client previously always sent as `-1`), so a broker can now
  reject a fetch built on metadata that has since moved — `FENCED_LEADER_EPOCH` and
  `UNKNOWN_LEADER_EPOCH` can reach a fetch where they could not before, and both are handled by this
  same per-partition path. A partition whose epoch the client has never seen still sends `-1`, which
  means "do not fence".
- **A fetch response's top-level error code is now acted on.** It was decoded and never read: a
  broker could report that the whole request was rejected and the client would treat the response as
  an ordinary empty one. From Fetch v7 — the first version that carries the field — the consumer
  reads it. The three fetch-session codes (`FETCH_SESSION_ID_NOT_FOUND`,
  `INVALID_FETCH_SESSION_EPOCH`, `FETCH_SESSION_TOPIC_ID_ERROR`) produce a warning, one metadata
  refresh and an empty round; any other top-level code follows the same recovery path a
  request-level fetch error already followed. Because this client sends only full (sessionless)
  fetches, the field is in practice reachable only through those three codes.
- **A leader move during consumption is handled by a hint instead of a rebalance.** When the broker
  tells the client which node now leads a partition (KIP-951), the consumer routes the next fetch
  there directly; consumption of the other partitions is not interrupted and no group rebalance
  happens. Where the broker also sends the new leader's address, the client uses it. If the hint
  names a broker the client has never heard of and carries no address for it, the client ignores the
  hint and refreshes metadata instead — a hint is never trusted far enough to invent a connection.
- **Log truncation is detected and acted on.** After a leader change the consumer now checks that
  its position still exists on the new leader, using the leader epoch of the last record it
  consumed. If the log diverged, the partition moves to the first offset known to diverge; if the
  divergence point cannot be determined, the partition resets per its `fromBeginning` setting —
  which, like the existing out-of-range reset, commits that offset to the coordinator even when
  `autoCommit: false`. Both cases log a warning and emit a new instrumentation event,
  `consumer.events.LOG_TRUNCATION`, whose payload is
  `{ topic, partition, offset, divergingOffset, leaderEpoch }`: `offset` is where the consumer was
  when the divergence was found, and `divergingOffset` is where it resumes — `null` in the reset
  case, where the resume point comes from `fromBeginning` rather than from the broker. This is the
  seventh instrumentation event new in 3.0.0, after the six the previous release added, and it
  exists on the consumer only. Previously the records past the divergence point were silently
  skipped or silently redelivered, with nothing in the log to say so. A paused partition can be
  repositioned this way and stays paused. The check is served by a protocol call the client did not
  make before, `OffsetForLeaderEpoch`, which also appears on the public `Broker` type as
  `broker.offsetForLeaderEpoch()`; against a broker that advertises no usable version of it the
  consumer asks nothing, logs one notice per broker per assignment, and fetches unvalidated exactly
  as it did before.
- **Committed offsets now carry the leader epoch.** `consumer.commitOffsets` and auto-commit send
  the epoch of the record whose offset is being committed (the `CommittedLeaderEpoch` field
  OffsetCommit gained at v6, which this client previously always sent as `-1`), and read it back on
  assignment. That epoch is what makes the truncation check above possible. Offsets committed by an
  older client have no epoch, so the first assignment after upgrading validates nothing; from the
  first commit onwards it does. An offset you commit yourself through
  `consumer.commitOffsets(offsets)` still carries no epoch — the API takes a topic, a partition
  and an offset, and the client will not guess which epoch you meant — and neither does an offset
  resolved to a position with no record behind it, such as one reached by a seek or by a
  log-compacted advance.
- **Produce and fetch requests are keyed by topic id.** Requests use topic ids (KIP-516) whenever
  the client knows them, and fall back to topic names for that one request otherwise. This is
  invisible in the API: the decoded fetch and produce responses, the `Batch` handed to `eachBatch`
  and the fields of a produce error are all unchanged, and topic ids never reach a caller. What it
  changes is which protocol version the client negotiates, and therefore what the compatibility
  matrix reports: against a Kafka 4.1 or 4.3 broker, Fetch moves from **v11** to **v18** (**v12**
  for a request in which any topic's id is not yet known) and Produce from **v11** to **v13**
  (**v11** for a request in which any topic's id is not yet known). A send inside a transaction is
  deliberately held at Produce **v11**: from v12, producing inside a transaction implicitly adds the
  partition to it, and that path is not implemented yet. `SaslAuthenticate` also moves to **v2** and
  `OffsetForLeaderEpoch` **v4** appears; custom SASL mechanisms are unaffected, since a provider
  still returns length-prefixed bytes from `request.encode()` and still receives length-prefixed
  bytes in `response.decode()`.
- **A broker whose API versions were learned on a superseded socket is treated as not connected.**
  The previous release made `broker.isConnected()` require a completed version negotiation; it now
  additionally requires that the negotiation happened on the *current* connection, so a socket the
  connection pool re-establishes on its own re-negotiates before it serves a request instead of
  reusing a version table learned on a socket that is gone. This can only turn a previously
  optimistic `true` into a correct `false`. It has two costs, both one-off and both self-correcting:
  one extra `ApiVersions` round trip the first time a broker's second socket is opened, and —
  because the group coordinator is re-resolved whenever its broker reports itself not connected —
  one extra `FindCoordinator` before the next commit after such a reconnect.
- **`broker.fetch()` no longer rejects on a fetch error — it returns the codes.** `Broker` is a
  public type and `cluster.findBroker()` hands one to application code. Previously a fetch
  response's partition errors rejected the promise (the first failing partition won) and its
  top-level error code was decoded but never read. From Fetch v12 neither rejects: both come back in
  the response body, so a direct `broker.fetch(...)` caller reads `errorCode` at the top level and
  `responses[].partitions[].errorCode` per partition instead of catching. If you call
  `broker.fetch(...)` yourself and rely on a rejection to notice a bad partition, that code needs
  updating. The consumer is the only in-tree caller and classifies both codes for you, as described
  in the first two changes above.
- **A leader change sends a rack-aware consumer back to the leader for one round.** When a
  partition's leader epoch changes, the consumer now drops that partition's preferred read replica
  along with its position validation, matching the Java client. A consumer configured with `rackId`
  that was reading from a nearby follower reads from the leader until the next fetch response offers
  a new `preferredReadReplica`; previously it kept using the old follower until that entry expired
  at `metadataMaxAge`. This can briefly move read traffic across availability zones after a leader
  election, and it also happens on the first fetch round of every new assignment, where no partition
  has been validated yet. On a deployment that pays for cross-zone traffic this is a real, if brief,
  cost; it is the price of not reading from a replica that may no longer be following the leader you
  think it is.
- **`UNKNOWN_LEADER_EPOCH` no longer loops on offset lookups.** When a broker reports that the
  client is ahead of it — `UNKNOWN_LEADER_EPOCH`, most visibly from `admin.fetchTopicOffsets` and
  from consumer offset resets — the client now forgets its cached leader epoch for the partitions
  involved instead of defending it. Previously the metadata refresh that followed returned a lower
  epoch, the client kept the higher one it already had, and every retry resent the same rejected
  epoch until the retry budget was exhausted and the call failed with
  `KafkaJSNumberOfRetriesExceeded`. The call now converges on the next attempt.
  `FENCED_LEADER_EPOCH` is unaffected: there the client is behind, and a metadata refresh already
  converged.
- **The `COMMIT_OFFSETS` event's payload gains `leaderEpoch`.** Each entry of
  `topics[].partitions[]` now carries `leaderEpoch` (a number, `-1` when the epoch is unknown)
  alongside `partition` and `offset`, and the entries returned by `consumer.uncommittedOffsets()`
  gain the same key. Every key those payloads already had is unchanged, so only code that
  deep-equality-compares a whole payload is affected — the same kind of additive change
  `groupInstanceId` was in the previous release.
- **A fetch round can pause briefly after a leader election while positions are checked.** The
  truncation check above is a gate rather than a background task: when a partition's leader epoch
  changes, that partition is held out of the next fetch request until the new leader has answered
  for it. The check has its own small budget — at most two retries, capped at five seconds of
  backoff — and is not governed by your `retry` options; if it has not resolved by then, the round
  fetches that partition unvalidated rather than stalling. Partitions whose epoch did not change are
  fetched throughout. In practice this is a latency change measured in milliseconds, visible only
  around leader elections and at the first fetch of a new assignment, and never a stall.
