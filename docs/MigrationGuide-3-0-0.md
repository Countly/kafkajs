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

### Phase 6A — classic-protocol consumer foundation

- **The rebalance timeout default changes from 60 s to 5 minutes.** The rebalance timeout sent to
  the coordinator is now the new `maxPollInterval` option, which defaults to 300000 ms and matches
  the Java client's `max.poll.interval.ms`. If you never set `rebalanceTimeout`, a member that stops
  responding now holds up a group rebalance for up to 5 minutes instead of 60 seconds. **To keep the
  old 60 s rebalance timing, set `rebalanceTimeout: 60000`** — the legacy option still goes on the
  wire unchanged, and it never becomes a handler deadline below 5 minutes (see the third item).
  Setting `maxPollInterval: 60000` instead would also shorten your handler deadline to 60 s, which
  is usually not what you want. One related timing does change if you had raised the rebalance
  timeout above 60 s: the client-side timeout on the SyncGroup request is now capped at 65 s, where
  it used to follow the rebalance timeout, so a `rebalanceTimeout: 120000` waits 65 s for the
  assignment instead of 125 s. That is what the Java client sends. If your group leader genuinely
  needs longer to compute an assignment, raise the client-wide `requestTimeout`, which still raises
  this one with it. At the default nothing changes: it was 65 s and it stays 65 s. The JoinGroup
  request goes the other way: its client-side timeout is `maxPollInterval + 5000`, so 305 s at the
  default where 2.2.4 gave 65 s. A JoinGroup is held open by the coordinator until every member has
  rejoined, so the client has to be willing to wait out the whole rebalance timeout — the Java
  client sizes it the same way — but a coordinator that accepts a join and then goes silent now
  stalls that consumer for about five minutes instead of about one, holding an in-flight request
  slot for that long. A lower client-wide `requestTimeout` does not shorten it; a lower
  `maxPollInterval` does.
- **A handler that runs longer than the poll deadline now loses its partitions.** There is now a
  poll-interval guard, as in the Java client. If your `eachMessage`/`eachBatch` handler takes longer
  than `maxPollInterval` (5 minutes by default) to return, the consumer logs a warning, emits
  `consumer.poll_interval_exceeded`, leaves the group (or, for a static member, resets its
  generation and keeps its slot), discards uncommitted offsets and rejoins. Two details are worth
  knowing. The deadline is measured **per batch handed to your handler**, not per message: with
  `eachMessage`, one KafkaJS batch is bounded by `maxBytesPerPartition` rather than by a record
  count, so a very large batch is measured as a whole — unlike the Java client, which resets the
  clock on every poll of at most `max.poll.records`. And the overrun is noticed on the next
  heartbeat tick after the deadline passes, and never while a rebalance round is in flight: the time
  a member spends blocked in its own JoinGroup belongs to the coordinator, not to your handler, and
  the round restarts the deadline when it completes. A handler that was already overrunning when a
  cooperative round began is therefore measured from the end of that round — at most one extra
  deadline before it is evicted. **The guard is
  weaker than it sounds whenever more than one fetch can make progress**: the deadline is reset on
  every batch hand-off *and* on every fetch round that comes back empty, and fetching is done by one
  fetcher per broker, so it fires only when every one of those fetchers is stalled behind the
  overrunning handler. A single stuck handler does not trip it while another broker's fetcher keeps
  polling or another partition keeps flowing — in practice that means a single-broker assignment
  with `partitionsConsumedConcurrently: 1`. The batch the overrunning handler finally returns is
  dropped, so that work is redelivered after the rejoin.
  In kafkajs 2.2.4 a long handler was never evicted as long as it called `heartbeat()`. If you have
  handlers that legitimately run for a long time, set `maxPollInterval` to cover them.
- **`rebalanceTimeout` is deprecated, and it does not become your eviction deadline.** It still
  works, it still goes on the wire when it is the only one of the two you set, and
  `rebalanceTimeout: 0` is unchanged — including its request timeouts, which still fall back to
  `sessionTimeout + 5000`. What it does **not** do is become the poll-interval guard: a legacy
  configuration's handler deadline is `max(rebalanceTimeout, 300000)`, so a 20 s or 60 s rebalance
  timeout never turns into a 20 s or 60 s handler deadline. Set `maxPollInterval` explicitly to make
  the two one number, as Java does. Setting `rebalanceTimeout` now logs a deprecation warning once
  per consumer you create.
- **Heartbeats now happen on a timer — with one limit.** Heartbeats are sent by a background timer
  at `heartbeatInterval`, so a handler that *awaits* no longer risks a session timeout while it
  works. A handler that blocks the event loop without awaiting still blocks them, because Node has
  one thread; that case is unchanged from 2.2.4, and such a handler also cannot trigger its own
  poll-interval eviction. The `heartbeat()` callback keeps working exactly as before: it and the
  timer share one in-flight request and one interval gate, so neither sends twice. Because the timer
  is a second source of heartbeats, a test that counts `consumer.heartbeat` events over a run will
  see more of them than it did in 2.2.4.
- **Cooperative rebalancing is available, opt-in, and needs two deploys.**
  `PartitionAssigners.cooperativeSticky` implements KIP-429, where a member keeps the partitions it
  is not losing instead of giving up everything at the start of each round. Turning it on takes two
  deploys, exactly as it does for the Java client:

  ```javascript
  // Deploy 1 — every member learns the new assigner. Still eager.
  partitionAssigners: [PartitionAssigners.roundRobin, PartitionAssigners.cooperativeSticky]

  // Deploy 2 — only once every member is running deploy 1. Now cooperative.
  partitionAssigners: [PartitionAssigners.cooperativeSticky]
  ```

  The protocol is chosen when you create the consumer, as the strongest one *every* configured
  assigner supports, so deploy 1 is still eager. During deploy 2 the group is briefly mixed —
  members on deploy 1 computing eagerly, restarted ones computing cooperatively, with the
  coordinator picking `cooperative-sticky` for everyone because it is the only name they share. That
  is safe by construction: an eager member's JoinGroup advertises no owned partitions — it has
  given everything up before it rejoins — and the cooperative algorithm never hands a partition to a
  member that still owns it. It also withholds a partition that two members claim in the same
  generation, which is what happens when one of them is working from a membership it has already
  lost: neither gets it for a round, and the follow-up round hands it to whichever member is still
  in the group. Going straight to deploy 2 splits the group with `INCONSISTENT_GROUP_PROTOCOL` until
  the rollout finishes.
- **Java-compatible assigners are available.** `PartitionAssigners.range`, `.sticky` and
  `.cooperativeSticky` implement the Java algorithms and advertise the Java names `range`, `sticky`
  and `cooperative-sticky`, so a group can mix this client with Java consumers. The export key and
  the name on the wire are different things: the default assigner keeps the wire name
  `RoundRobinAssigner`, which no Java client understands — renaming it would split every existing
  group — and the default assigner list is unchanged at `[PartitionAssigners.roundRobin]`, so a
  consumer you do not reconfigure behaves exactly as before. Rack-aware assignment is not
  implemented: `rackId` is sent to the leader and ignored when partitions are assigned. One
  small caveat if you run a mixed group: on a shape where more than one balanced sticky assignment
  is equally correct — an incumbent tie-break during a scale-up — a Java leader and a leader running
  this client can pick different ones, so a leader change between the two may move one partition
  more than Java alone would. The result is always balanced and always valid.
- **New instrumentation events, and two payloads gain a field.** `consumer.partitions_assigned`,
  `consumer.partitions_revoked`, `consumer.partitions_lost` and `consumer.poll_interval_exceeded`
  are new, and the first three fire on the default eager path as well as the cooperative one.
  `consumer.rebalancing` gains a `reason` and `consumer.group_join` a `rebalanceProtocol`;
  `group_join`'s `groupProtocol` still carries the assignor name and `memberAssignment` is still the
  full assignment. Every key those payloads already had is unchanged, so only code that
  deep-equality-compares a whole payload needs attention. One value does move: the
  `consumer.rebalancing` raised by an `UNKNOWN_MEMBER_ID` now reports `memberId: null` rather than
  the member id the coordinator had just rejected, because the id is dropped before the rejoin is
  reported — the payload names the id the round will actually join with. Code that reads
  `payload.memberId` from this event to correlate a rebalance should fall back to the id from the
  following `consumer.group_join`.
- **Two new log lines for configurations that changed nothing.** Every consumer now logs one INFO
  line at construction naming the rebalance protocol it selected and the assigners it selected it
  from, and a WARN if you set the deprecated `rebalanceTimeout` (or set it together with a
  `maxPollInterval` that disagrees, in which case `maxPollInterval` wins). A third WARN is new for a
  configuration that was already legal: a `heartbeatInterval` above a third of `sessionTimeout`. The
  existing hard failure when `heartbeatInterval` is not lower than `sessionTimeout` is unchanged.
  None of these change behavior; they are new output at the default log level.
- **On the cooperative path, `heartbeat()` no longer throws when a rebalance starts.** With a
  cooperative assigner configured, a `REBALANCE_IN_PROGRESS` no longer propagates out of the
  `heartbeat()` callback into your handler: the call resolves and the rejoin runs in the background
  while your handler keeps its partitions. `consumer.stop()` and `disconnect()` wait for that
  background round to finish before leaving the group, so shutdown is unaffected. On the default
  eager path the behavior is unchanged — the callback still throws and your handler is interrupted.
  If you catch and act on that error today, note that it will stop firing once you opt into
  cooperative rebalancing.
- **A custom assigner that declares a subscription version above 0 now emits that version's
  layout.** The subscription your assigner's `protocol()` builds with
  `AssignerProtocol.MemberMetadata.encode` is a positional format, and the `version` you declare
  decides which fields are written. Previously every declared version wrote the same three fields,
  so a `version: 1` subscription carried a v0 body under a v1 header — readable by another kafkajs
  member and unreadable by anything that decodes it at the version it claims, which is what the Java
  client does. An assigner declaring `version: 0` is byte-identical to before, including the
  built-in round-robin assigner and therefore the default configuration. An assigner declaring
  `version: 1` now emits four more bytes, an empty `OwnedPartitions` list, and higher versions
  likewise write the fields they define, each omitted value taking its Kafka default. This is what
  makes a custom assigner's subscription readable by a Java group leader, and it is what a mixed
  Java/JavaScript group needs. If you declared a version whose fields you never send, either drop to
  `version: 0` or start sending them; see the custom-assigner example in
  [Consuming.md](Consuming.md#custom-partition-assigner).
- **An eager rebalance now stops the world before it rejoins, and stale batches are dropped.** On
  the default eager path — and on any rebalance where the coordinator has invalidated this member's
  generation — the consumer stops fetching and waits for every running `eachMessage`/`eachBatch` to
  return before it commits or discards offsets and rejoins, which is what the Java client does by
  only rebalancing between polls. Two things follow. The rejoin is delayed by however long your
  handler takes to return from the batch it is on, bounded in practice by `maxPollInterval`. And a
  batch that was fetched under the previous assignment is dropped rather than handed to your
  handler: its records are fetched again after the group has rejoined, where before they could
  arrive twice — once from the queued batch and once from the new assignment re-reading the same
  offsets. A rebalance also issues exactly one auto-commit now; the heartbeat path and the fetch
  path each used to issue one, so a default eager rebalance sent two OffsetCommits. On the
  cooperative path nothing waits, which is the point of it.
- **`resolveOffset` and `commitOffsetsIfNecessary` are ignored for a partition you no longer hold.**
  This only comes up under cooperative rebalancing, where a round runs alongside your handler. A
  handler that is already running when its partition is revoked still runs to completion — it is
  never interrupted — but its position updates and commits for that partition are dropped, because
  another member owns it by then and committing would roll that member's position back. Batches
  fetched for a revoked partition are dropped before the handler for the same reason. When you pass
  your own offsets to `commitOffsetsIfNecessary(offsets)` from `eachBatch`, the payload is filtered
  per partition rather than rejected as a whole: the partitions you still hold are committed and the
  rest are dropped, so one revoked partition in a payload does not cost you the others.

### Phase 6B — KIP-848 consumer protocol, admin group visibility, fetch prefetch

- **A cooperative rebalance now commits before it releases partitions.** This applies to the
  classic cooperative path you may already be using (`cooperativeSticky`, or any assigner that
  selects the cooperative protocol) as well as to the new consumer protocol. With
  `autoCommit: true`, whatever your handlers resolved on the partitions a round is about to revoke
  is committed before those partitions are pruned. What changes for you: fewer redeliveries after a
  cooperative round, and one extra `OffsetCommit` per round that revokes something. A failure of
  that commit — including a membership that was lost meanwhile — is logged at warn and the round
  proceeds, so nothing new can fail your consumer. With `autoCommit: false` nothing changes: your
  own commits stay yours, and eager rounds already committed everything before rejoining.
- **A stale member epoch is handled per commit under `groupProtocol: 'consumer'`.** When the
  coordinator answers an `OffsetCommit` with `STALE_MEMBER_EPOCH` (113) — the epoch moved on while
  the commit was in flight — what happens depends on who asked for it:
  - An **interval or threshold auto-commit** is logged at error (`Auto-commit rejected with a stale
    member epoch; skipping this one`) and skipped. Nothing is raised, and the next interval commit
    goes out under the epoch the heartbeats have installed by then, so the offsets are committed one
    interval later rather than lost.
  - **`commitOffsetsIfNecessary()` called with no arguments from `eachBatch`** follows that same
    rule, because it is the auto-commit primitive: the call resolves, and the commit it skipped is
    picked up by the next one. If you rely on that call rejecting to detect a failed commit, note
    that this one case resolves instead.
  - **`consumer.commitOffsets(offsets)`**, and `commitOffsetsIfNecessary(offsets)` with your own
    payload, still fail: they reject with a non-retriable error whose message begins `OffsetCommit
    failed with stale member epoch.` and whose `type` is `STALE_MEMBER_EPOCH`. Handle it as you would
    any other commit failure — retrying it yourself under the same epoch cannot succeed.

  An `OffsetFetch` rejected the same way is re-sent under whatever epoch the heartbeats have
  installed by then, for up to the rebalance timeout, so a first fetch of committed offsets around a
  rebalance can take longer without failing. The retry is abandoned as soon as you call
  `stop()`/`disconnect()`, so a shutdown is not held open by it. Classic consumers see
  `ILLEGAL_GENERATION` for a stale generation, exactly as before.
- **A static member under `groupProtocol: 'consumer'` sends a leave on close and on poll expiry.**
  A static classic member (`groupInstanceId` set) sends nothing when it stops: the coordinator only
  notices at `sessionTimeout`. A static consumer-protocol member instead sends one heartbeat
  carrying member epoch −2, the protocol's "this static member is going away" signal, which tells
  the coordinator about the departure straight away while keeping the assignment reserved for that
  `groupInstanceId` — the protocol has no way for a static member to leave permanently. Two things
  follow for you. The coordinator learns about a shutdown immediately instead of waiting out the
  session timeout. And because a member that still holds a live epoch gives its partitions up
  properly, closing one emits `PARTITIONS_REVOKED` and, with `autoCommit: true`, commits the
  offsets its handlers resolved first — where a static classic member emits neither. The same
  −2 heartbeat is sent when the poll deadline expires, which is the case `POLL_INTERVAL_EXCEEDED`'s
  `isStatic: true` reports.
- **A fatal membership error never restarts under `groupProtocol: 'consumer'`.** These conditions
  will be answered the same way by every rejoin, so the consumer stops instead of looping: the group
  is at its configured size limit (`GROUP_MAX_SIZE_REACHED`, 81), the static instance id is still
  held by another member (`UNRELEASED_INSTANCE_ID`, 111), the server assignor you asked for does not
  exist (`UNSUPPORTED_ASSIGNOR`, 112), the subscription regex is one the broker will not accept
  (`INVALID_REGULAR_EXPRESSION`, 128), the caller is not authorized for the group
  (`GROUP_AUTHORIZATION_FAILED`, 30), the coordinator rejected the request as malformed
  (`INVALID_REQUEST`, 42), a duplicate `groupInstanceId` fenced this member (`FENCED_INSTANCE_ID`,
  82), or the cluster does not implement the consumer protocol at all. Each emits `CRASH` with
  `restart: false` **regardless of `retry.restartOnFailure`** — a callback returning `true` does not
  bring the consumer back. What to do: fix the ACL, the configuration, the assignor name, the
  pattern or the duplicate `groupInstanceId`, and create a new consumer. Classic consumers are
  unaffected — they keep restarting on 30 and 42 as they always have — so only
  `groupProtocol: 'consumer'` users see this.
- **Under `groupProtocol: 'consumer'`, a topic you are not authorized to read is logged, not
  raised.** When the coordinator answers `TOPIC_AUTHORIZATION_FAILED` for part of the subscription,
  the member stays in the group and keeps serving the partitions it can, and the failure is
  reported as an error-level log line (once per distinct message). The Java client raises it to the
  application; this client does not, so the log line is your signal that an ACL is missing.
- **Poll-overrun detection is less immediate under `groupProtocol: 'consumer'`.** Your deadline is
  still `maxPollInterval`, and an overrun still costs the member its partitions. But the expiry is
  noticed on a heartbeat tick, and never while a reconciliation is in flight, so how quickly it is
  noticed is bounded by the heartbeat interval the coordinator asked for rather than by a
  `heartbeatInterval` you configured. Expect the detection to lag the deadline by up to one
  server-driven heartbeat interval.
- **A regex subscription's resolved topic set only grows.** Under `subscribe({ regex })` the names
  the pattern has resolved to are remembered for the life of the membership: a topic the pattern
  stops matching stays in the subscription until the consumer rebuilds its membership (a restart or
  a crash recovery). It is bounded by the number of distinct topics the pattern ever matched.

- **A commit for a group that no longer exists rejoins instead of crashing, and the broker's code
  for it changed.** `OffsetCommit` is now negotiated up to v9 against a Kafka 4.x broker. From v9
  the coordinator answers `GROUP_ID_NOT_FOUND` (69) where v8 answered `ILLEGAL_GENERATION` (22) for
  a commit from a member whose group has been deleted (`admin.deleteGroups`, or expiry after
  `offsets.retention.minutes`). This client maps a 69 **on the commit path** to exactly the verdict
  22 has always taken — discard the uncommitted offsets, rejoin, keep running — so a classic
  consumer behaves as it did before. Nothing else about 69 changes: it is still a non-retriable
  "no such group" for `admin.fetchOffsets`, and still the signal that routes a describe to the
  classic API. If you match on the error code in a `CRASH` handler or a log filter, expect 69 rather
  than 22 for this case.
- **`consumer.describeGroup()` returns the unified description.** It is now routed through the same
  describer `admin.describeGroups` uses, so a `groupProtocol: 'consumer'` group describes itself
  correctly instead of coming back as a `Dead` row with `errorCode: 69`. Every field 2.2.4 returned
  is unchanged in name and meaning; the row additionally carries `groupType`, `groupEpoch`,
  `assignmentEpoch`, `assignorName` and `errorMessage`, and each member carries `groupInstanceId`,
  `rackId`, `memberEpoch`, `assignment`, `targetAssignment`, `memberType`, `subscribedTopics` and
  `subscribedTopicRegex`. One value changed: `authorizedOperations` is `null` rather than the raw
  `-2147483648` sentinel when authorized operations were not requested, which is what
  `admin.describeGroups` has always reported and what the `GroupDescription` type declares. A
  deep-equality assertion against the whole row needs updating; reading individual fields does not.
- **`admin.listGroups()` rejects with the retrier's error class.** Every per-broker call now runs
  inside the configured retrier, so what you catch is `KafkaJSNonRetriableError` (or
  `KafkaJSNumberOfRetriesExceeded` once the budget is spent) with the broker's error on `cause`,
  where 2.2.4 surfaced the raw `KafkaJSProtocolError`. `.type` and `.code` are still on the error
  you catch; `catch (e) { if (e.name === 'KafkaJSProtocolError') … }` is not. The one exception is a
  filter this broker's version cannot serve, which still rejects with the
  `KafkaJSServerDoesNotSupportApiKey` instance itself, `apiKey` and `apiName` intact.
- **`admin.listGroups()` rows always carry `groupState` and `groupType`.** Against a broker whose
  `ListGroups` version has neither field (v0–v2) they are decoded as `''` rather than being absent,
  so one row shape works at every version. A deep-equality assertion against a pre-v3 row needs the
  two keys.

### Phase 7 — Transactions TV2, the KIP-1050 taxonomy and producer performance

The producer becomes idempotent by default and transactions move to the transaction protocol Kafka 4.x brokers speak; each change below says what you may notice and what, if anything, to do.

### TypeScript: source-breaking type changes

The runtime is unchanged by these; they can fail `tsc` on an existing codebase.

- **`ConsumerGroupJoinEvent.payload.leaderId` is now `string | null`.** Under
  `groupProtocol: 'consumer'` there is no leader, and the event says so. Under `strictNullChecks`
  `const leader: string = event.payload.leaderId` is now `TS2322` and `event.payload.leaderId
  .slice(0, 8)` is `TS18047`, for every user — including one who never sets `groupProtocol`. Guard
  the value (`event.payload.leaderId ?? ''`) or narrow it before use.
- **`ConsumerGroupState` gained `'Assigning' | 'Reconciling' | 'NotReady'`**, the states
  `GroupState.java` adds for consumer groups. An exhaustive `switch` over the union without a
  `default` no longer compiles; add the three cases or a `default`.
- **`GroupOverview.groupState`, `GroupOverview.groupType` and `GroupDescription.errorCode` are
  required.** Code that *constructs* these types — test fixtures, admin mocks, a function typed
  `(): GroupOverview` — must supply them. Code that only reads them is unaffected.
  `GroupOverview.groupType` keeps its literals for autocomplete but accepts any string, because a
  group type a newer broker introduces is passed through lower-cased; an exhaustive `switch` over it
  needs a `default`.
- **`subscribe()`'s three shapes are now mutually exclusive.** `{ regex, topics }`,
  `{ regex, topic }` and `{ topic, topics }` are compile errors, matching the runtime, which has
  always thrown for them.

The rest of this section is additive — nothing here changes unless you opt in.

- **`groupProtocol` is opt-in and defaults to `'classic'`.** Setting `groupProtocol: 'consumer'`
  moves a consumer to the KIP-848 protocol, where the coordinator computes the assignment and the
  member reconciles it over its heartbeats. It requires Kafka 4.0 or newer.

  ```javascript
  const consumer = kafka.consumer({ groupId: 'my-group', groupProtocol: 'consumer' })
  ```

  Options the broker owns under that protocol are rejected at construction rather than ignored:
  setting `partitionAssigners`, `heartbeatInterval` or `sessionTimeout` throws
  `partitionAssigners, heartbeatInterval, sessionTimeout cannot be set when groupProtocol=consumer`
  (naming only the ones you actually set), and setting `groupRemoteAssignor` under the classic
  protocol throws `groupRemoteAssignor cannot be set when groupProtocol=classic`.
  `rebalanceTimeout` and `maxPollInterval` are legal under both. `groupRemoteAssignor` names the
  server-side assignor to ask for (`'uniform'` and `'range'` on a stock 4.x broker); left `null`,
  the coordinator picks. See [Consumer group protocols](Consuming.md#group-protocols).
- **`subscribe({ regex })` is a new subscription form**, available only under
  `groupProtocol: 'consumer'`. The pattern is **RE2, evaluated by the broker**, not a JavaScript
  `RegExp`, so topics created later join the subscription without a client-side metadata poll. A
  consumer subscribes to names or to a regex, never both. A JavaScript `RegExp` inside `topics`
  keeps its 2.2.4 behaviour under both protocols: matched client-side, once, at `subscribe()` time.
- **`admin.listGroups` takes filters, and `admin.describeGroups` reports errors per group id.**
  `admin.listGroups({ states, types })` filters server-side with lower-case values, and every row
  now carries `groupState` and `groupType`. `admin.describeGroups` describes classic and consumer
  groups in one shape; `errorCode` is retained on every row, as it always has been, and a group id
  that does not exist comes back as a `Dead` row carrying `errorCode: 69` instead of failing the
  call. Rows are not returned in the order the ids were passed. See
  [List groups](Admin.md#list-groups) and [Describe groups](Admin.md#describe-groups).
- **`maxPrefetchBatches` is accepted but inert.** `0` (the default) and `1` behave identically:
  the fetch loop is exactly what it was. The one-deep prefetch the option was to control measured
  +2.45 % throughput against the ≥ +10 % the design required of it, and was withdrawn on that
  number; the option is reserved so a later release can turn the prefetch on without a
  configuration change. A value of `2` or more is rejected at construction rather than clamped.
  See [Prefetching](Consuming.md#prefetching).

