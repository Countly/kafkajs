# Migrating from kafkajs 2.2.4 to @countly/kafkajs 3.0.0

`@countly/kafkajs` is a drop-in replacement for `kafkajs` 2.2.4 targeting Apache Kafka 4.0+ and
Node.js 22/24/26: the public API and the names of the existing events are the ones 2.2.4 had, apart
from what is listed below, so most applications need no code changes beyond the new package name
and a runtime that meets the floors. The package itself differs from 2.2.4's: it is renamed, only
its documented entry points can be imported, and `import` resolves its own ESM entry. This guide
lists every behavior change a caller might actually notice, grouped by the part of your application
it touches; each item says what changed and what, if anything, to do. The goal is that a change not
listed here did not change observable behavior. Start with **Install and runtime**, then read the
areas you use.

### Install and runtime

What to change to install the package and run it on a Kafka 4.x cluster.

- **The package is now `@countly/kafkajs`.** Install it in place of `kafkajs` and change every
  `require('kafkajs')` and `import … from 'kafkajs'` to `@countly/kafkajs`. The API behind the name
  is the one 2.2.4 had: everything 2.2.4 exported is still exported under the same name, and the
  additions and changes are listed below. A third-party package that requires
  `kafkajs` itself keeps loading the original package, which is a separate module and shares no
  state with this one. *What to do:* replace the dependency and every import; for a dependency that
  needs `kafkajs`, check whether it can be pointed at this package.

- **Only the documented entry points can be imported (`exports`).** The package's `exports` map
  lists `.` (the CommonJS entry, the ESM entry and the types) and `./package.json`; nothing under
  `src/` can be imported. kafkajs 2.2.4 had no `exports` map, so `require('kafkajs/src/…')` worked
  there; against this package it throws `ERR_PACKAGE_PATH_NOT_EXPORTED`. *What to do:* search your
  code and your test helpers for deep imports of the package (patches of its internals, direct
  uses of `src/` classes) and move them to the public API; if the API has no equivalent, ask for
  one rather than importing a path that is no longer exported.

- **`import` has its own ESM entry.** `import { Kafka } from '@countly/kafkajs'` resolves
  `index.mjs`, which re-exports every named export of the CommonJS entry and the CommonJS module
  itself as `default`; `require('@countly/kafkajs')` resolves `index.js`. Both give the same
  objects, so a class from one passes `instanceof` against the other. *What to do:* nothing for
  CommonJS; ESM code can import any export by name, or take the default. Under 2.2.4, Node's
  CommonJS interop could import only `Kafka`, `PartitionAssigners`, `AssignerProtocol`,
  `Partitioners`, `logLevel` and `CompressionTypes` by name; the error classes,
  `CompressionCodecs` and the other enums were reachable only through the default import, which
  keeps working.

- **Node.js 22 or newer is required.** The package declares `engines.node >=22.0.0` and
  `engines.npm >=10`, where 2.2.4 declared Node `>=14.0.0`, and targets Node 22, 24 and 26.
  *What to do:* run it on Node 22 or newer.

- **Apache Kafka 4.0 or newer is required.** Brokers before 4.0, including every ZooKeeper-mode
  broker, are not supported, and the client no longer implements the request versions Kafka 4.0
  removed ("Request versions below the Kafka 4.0 floors are gone."). *What to do:* upgrade the
  cluster to Kafka 4.0 or newer, which is KRaft-only, before you upgrade the client.

- **The client now identifies itself to brokers as `countly-kafkajs`** (KIP-511), carrying this
  package's version. Operators watching a broker's `clientSoftwareName`/`clientSoftwareVersion`
  metrics or request logs will see this name where kafkajs 2.2.4 previously showed `unknown`.

### Producer

Producers are idempotent by default and gather sends into batches; each item says what you may
notice and what, if anything, to do.

- **`maxInFlightRequests` defaults to 5, for producers and consumers.** It was unbounded. It is
  Java's `max.in.flight.requests.per.connection`: once five requests are outstanding on a broker
  connection, the next ones queue client-side. The admin client (`kafka.admin()`) gets the same
  default and has no option for it. No ordering guarantee is claimed by this change.
  *What to do:* nothing, unless you relied on unbounded pipelining; `maxInFlightRequests: null`
  restores it (and turns idempotence off for a producer — see the producer items).

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

- **`kafka.producer()` is idempotent by default.** Every send requires `acks: -1` and the
  producer allocates a producer id at `connect()`. The size of the retry budget is unchanged:
  every producer keeps its `retry.retries` (5 unless set), as in 2.2.4; what it counts is per
  partition now (the retry items below). `producer.isIdempotent()` reports
  `false` after a configuration turned idempotence off (the `maxInFlightRequests` and `retries`
  items below).
  *What to do:* producers are idempotent unless you pass `idempotent: false`. If you relied on
  `acks: 1` or `acks: 0`, either keep passing it — the producer then logs a warning once and runs
  non-idempotent — or pass `idempotent: false` explicitly. `idempotent: false` drops the producer
  id and the sequence numbers; it does not restore 2.2.4's send path, which every producer shares
  (the accumulator and per-partition retry items below). If you check `producer.isIdempotent()`,
  read it after `connect()`.

- **`idempotent: true` plus `acks` 0 or 1 still throws, with a new class.** The error is now
  `KafkaJSInvalidConfigurationError`, still a `KafkaJSNonRetriableError`, with the same message.
  *What to do:* catch blocks keying on `KafkaJSNonRetriableError` are unaffected; the new class is
  a subclass.

- **`maxInFlightRequests` above 5, or `null`, disables idempotence — or throws if you asked for
  it.** A producer that never mentioned `idempotent` keeps working and logs one warning; a
  producer created with `idempotent: true` and more than 5 in-flight requests now throws at
  construction. *What to do:* if you pass `maxInFlightRequests: null` or a value above 5 and want
  idempotence, lower it to 5 or less; otherwise nothing to do — the producer runs non-idempotent
  and says so once in the log. With `idempotent: true` the combination is
  rejected, as it is in the Java client.

- **`connect()` can now fail with an authorization error** on a cluster that grants the
  principal neither `IDEMPOTENT_WRITE` nor, from Kafka 2.8, `WRITE` on any topic, because an
  idempotent producer issues `InitProducerId` there. *What to do:* grant the ACL, or create the
  producer with `idempotent: false`. The error names both remedies. It is a
  `KafkaJSInvalidConfigurationError` (`category` `'invalidConfiguration'`) whose `cause` is the
  broker's `KafkaJSProtocolError`; code that matched `error.type === 'CLUSTER_AUTHORIZATION_FAILED'`
  reads `error.cause.type`, or `error.category`, instead.

- **`retry: { retries: 0 }` disables idempotence — or throws if you asked for it.** A producer
  that never mentioned `idempotent` keeps its fail-fast configuration and logs one warning;
  `idempotent: true` (or a `transactionalId`) with `retries: 0` throws at construction. That
  throw is a `KafkaJSInvalidConfigurationError` — a `KafkaJSNonRetriableError` subclass, where 2.2.4
  threw `KafkaJSNonRetriableError` itself — with the message
  `Idempotent producer must allow retries to protect against transient errors`. A producer that
  is still idempotent with `retries` between 1 and 4 logs the "limiting retries may invalidate EoS
  guarantees" warning — new for a producer that did not pass `idempotent: true`, since 2.2.4
  logged it only for one that did. *What to do:* nothing for a fail-fast producer: it still sends
  each partition once, without retrying, and says once in the log that idempotence is off. If
  you want idempotence with `retries: 0`, you cannot have it — raise `retries`.

- **A `transactionalId` implies `idempotent: true` explicitly.** A transactional producer never
  downgrades: an `acks`, `maxInFlightRequests` or `retries` conflict throws rather than silently
  turning idempotence off. *What to do:* if you create a transactional producer with
  `maxInFlightRequests: 1` (a common 2.2.4 work-around) nothing changes — it is below the cap.
  `maxInFlightRequests: null` on a transactional producer now throws; drop it. A transactional
  producer's own `send()` with `acks` 0 or 1 now throws: send with `acks: -1`, or use a separate
  non-transactional producer for those sends.

- **The producer's per-broker lock is gone.** An idempotent or transactional producer used to
  send one request at a time to each broker, behind a mutex that rejected with
  `KafkaJSLockTimeout` after 65.5 s and emitted `producer.events.LOCK_TIMEOUT` first. Batches of
  different partitions are now pipelined, up to 5 requests per broker (fewer when the
  connection's `maxInFlightRequests` is lower); within one partition a batch waits for the
  previous one's answer, which keeps the partition in order. A busy partition gathers the records
  of several sends into each batch while it waits (the accumulator, below). `LOCK_TIMEOUT` can
  no longer be emitted. The event stays declared and the error class stays exported: a `send()`
  registering its topics with the cluster, and connecting to a broker, can still raise
  `KafkaJSLockTimeout`.
  *What to do:* `LOCK_TIMEOUT` listeners on a producer will never be called and can be removed.
  Keep any `catch` that matches `KafkaJSLockTimeout` by name. `maxInFlightRequests: 1` on a
  transactional producer is no longer needed and now costs throughput.

- **A failed send no longer cancels the partitions that were still going, and `retry.retries` is
  now per partition batch** on an idempotent producer (the default). In 2.2.4 one non-retriable
  error bailed the whole `send()` and no further partition was written; now each topic-partition
  is retried and fails on its own, so a rejecting `send()` may have written the partitions whose
  batches succeeded, and a multi-partition send can take up to `retries` attempts per partition
  before it gives up. The promise still settles once — after every batch has settled — and still
  rejects with one error: the first one any of its batches failed with.
  *What to do:* if you depended on a rejected `send()` meaning "nothing after the failure was
  written", check which partitions it wrote: no setting restores the all-or-nothing bail, since an
  `idempotent: false` producer now settles each partition on its own too (below). If you tune
  `retry.retries` for worst-case latency, budget it per partition of a send rather than per call.

- **An `idempotent: false` producer settles and retries each partition on its own, too.** In
  2.2.4 a `send()` was retried as a whole call: a failed request was sent again with every
  partition it carried, written ones included, and an error that is not retriable bailed the call.
  Now each partition's batch settles on its own answer, as in the Java client: one written
  resolves at once and is never sent again, so a retriable error on one partition no longer writes
  the others twice; one refused with an error that is not retriable rejects alone; and only the
  partitions whose answer was retriable go out again, within one `retry.retries` budget shared by
  the partitions that went out together; a topic or broker that cannot be looked up holds back
  only its own partitions, and the slowest broker of an attempt paces the next one. Sends to
  different partitions that shared a request no longer share each other's failure, and a rejected
  `send()` may have written its other partitions. Records are assigned to partitions once, before they are sent, under a retry budget
  of their own, so waiting for a topic's metadata no longer spends the send's retries.
  *What to do:* nothing, unless you relied on a rejected `send()` writing nothing: check the
  partitions it wrote. `retry: { retries: 0 }` still sends each partition once.

- **`send()`/`sendBatch()` resolve their `RecordMetadata[]` in a different order** on every
  producer, idempotent or not: sorted by topic name, then partition ascending. 2.2.4's order was
  grouped by the broker that led each partition, in the order the call listed its topics — an
  artifact of how the request was split, not a documented contract — so a multi-broker send, or a
  send listing its topics out of name order, returns the same entries in a different sequence.
  Every entry still carries `topicName` and `partition`; sorting changes no request.
  *What to do:* match results by `topicName`/`partition` rather than by array index. A
  single-partition send is unaffected; so is any caller that already reads the fields.

- **Every producer gathers sends into batches, so a `send()` may wait up to 5 ms before its
  request leaves.** The accumulator is on for every producer, idempotent or not, with the Java
  client's defaults: `linger` 5 ms (Java's `linger.ms` moved from 0 to 5 in Kafka 4.0) and
  `batchSize` 16384 bytes of keys and values. In 2.2.4 every `send()` and `sendBatch()` was its own
  request; now the records of sends made within `linger` of each other to one topic-partition
  share a batch, a batch goes out once it reaches `batchSize`, and one `send()` or `sendBatch()`
  usually makes one Produce request per broker: its records that join a batch an earlier send
  began leave with that batch, so one call can reach a broker in two requests. On an idempotent
  producer a partition's next batch also keeps gathering sends while the batch before it waits for
  its answer.
  *What to do:* nothing for code that sends without awaiting each send. Code that awaits every
  `send()` before the next one waits up to `linger` each time: pass `linger: 0`, which sends at
  once, with no timer, and on an idempotent producer still merges the sends that queue behind a
  partition's batch in flight.
  No setting restores one request per `send()`; the Java client has none either.

- **Sends to one topic-partition can share a batch, and then succeed or fail together.** A batch
  is one unit on the wire: when the broker rejects it, every send whose records it carries rejects
  with that error, and when it is written, every one of them resolves — each with the
  `baseOffset` of its own first record.
  *What to do:* treat a rejected send as before. Keep `batchSize` below the topic's
  `max.message.bytes`, so a merged batch is never refused for its size; header bytes are not
  counted in `batchSize`.

- **A record that cannot be encoded rejects its own send with `KafkaJSInvalidConfigurationError`**
  (`category` `'invalidConfiguration'`), before it joins a batch, instead of a `TypeError` failing
  every send of its request. A key, value or header value must be a string, a `Buffer` or `null`
  (header values: not `null`), and a timestamp must hold an integer — a number, a numeric string,
  a `Date`, a `BigInt` or a `Long`. Inside a transaction such a send leaves the transaction as it
  was.
  *What to do:* code that caught a `TypeError` from `send()` catches
  `KafkaJSInvalidConfigurationError` instead, and fixes the record.

- **`producer.disconnect()` delivers before it disconnects.** It sends every waiting batch, its
  own and its transaction's, and waits for every send begun before it — retries included, up to
  `deliveryTimeout` or the retry budget — refusing new sends meanwhile. 2.2.4 disconnected at once,
  failing the sends still under way.
  *What to do:* nothing for a clean shutdown. A shutdown with a hard time limit, such as a
  container's grace period, can wait out the retry budget while the cluster is unreachable: set
  `deliveryTimeout` to bound each send, or race `disconnect()` against a timer.

- **`deliveryTimeout` counts the wait for `linger` and for the producer id.** A send's deadline
  starts when the send is made, as the Java client's does, so the `linger` wait - and on an
  idempotent producer the `InitProducerId` wait - count against it, and its floor is
  `linger + requestTimeout`: 30005 ms with both defaults.
  *What to do:* nothing unless you set `deliveryTimeout`, new in 3.0.0: give it at least
  `linger + requestTimeout`.

- **Three opt-ins, none of them on by default**: `Partitioners.UniformStickyPartitioner`;
  `deliveryTimeout`; and `leaderEpoch` on the partitions given to `sendOffsets`.
  *What to do:* nothing; leaving them unset changes nothing beyond what the other items describe.

### Transactions

Transactions move to the transaction protocol Kafka 4.x brokers speak, and a failed send now ends a
transaction in a way its error decides.

- **A consumer resolving its position waits for open transactions** (KIP-447). Every `OffsetFetch`
  a consumer sends to find its committed offsets now asks for stable offsets (`RequireStable`),
  under both group protocols and whatever the isolation level, as the Java consumer does. A
  partition whose committed offset is held by a transaction that is still open answers
  `UNSTABLE_OFFSET_COMMIT`, and the consumer asks again — after 100 ms, backing off to once a
  second — until the transaction commits or aborts, for at most its rebalance timeout
  (`maxPollInterval`, 300 000 ms by default). While it waits it still counts as polling, and it
  stops waiting within a second for a `stop()` or for a rebalance that pauses fetching. Position
  resolution can therefore take longer while a transaction is open. A transaction left open past
  that bound fails the lookup with `UNSTABLE_OFFSET_COMMIT` (retriable) — 2.2.4 never raised it
  here, because it read the unstable offset — and the consumer's retry policy retries the lookup.
  `admin.fetchOffsets` is unchanged.
  *What to do:* no action. If you run long transactions, note that a rebalancing consumer waits
  for them to resolve instead of reading a position that may be rolled back.

- **`producer.transaction()` can now reject when the transaction coordinator cannot be found.**
  It looks the coordinator up before it resolves, so a lookup failure rejects `transaction()`
  itself instead of the transaction's first `send()`, `sendOffsets()` or `commit()`; the producer
  stays ready and the next `transaction()` tries again. *What to do:* nothing if you already
  handle a rejected `transaction()` — it could reject in 2.2.4 too, for example on its first
  InitProducerId.

- **Transactions use the transaction protocol version 2 on Kafka 4.x clusters** (KIP-890). The
  producer reads the cluster's `transaction.version` feature from the transaction coordinator at
  the start of every transaction. Under version 2 the producer epoch changes on every commit and
  abort, `AddPartitionsToTxn` and `AddOffsetsToTxn` are no longer sent (Produce v12 and
  `TxnOffsetCommit` v5 add the partition and the offsets themselves), and a transaction in which
  nothing was produced and no offsets were sent still issues no `EndTxn` at all — as in 2.2.4 and
  as in the Java client. A cluster that moves from version 1 to 2 costs one extra `InitProducerId`
  before the next transaction.
  *What to do:* no application change. If you log `producerEpoch`, expect it to change per
  transaction; if you assert on the request sequence, it is now Produce v12 + `EndTxn` v5.

- **A send that fails inside a transaction ends that transaction — how depends on the error.** An
  `abortable` error, a retriable one that ran out of retries, and every `invalidConfiguration` code
  except `CLUSTER_AUTHORIZATION_FAILED`, `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` and
  `UNSUPPORTED_VERSION` make the transaction abort-only: `commit()` then rejects with a
  `KafkaJSTransactionAbortableError` without contacting the broker, and `abort()` works. An
  `applicationRecoverable` error and those three codes make the producer itself unusable: every
  later call rejects with a `KafkaJSApplicationRecoverableError` telling you to create a new
  producer. `producer.transaction()` called again from either state rejects with that error rather
  than "Cannot continue to use transaction once ended". A retriable error that ran out of retries
  inside a transaction reaches you as a `KafkaJSTransactionAbortableError` whose `cause` is the
  `KafkaJSNumberOfRetriesExceeded`. A batch dropped because the producer was disconnecting or
  disconnected ends the transaction the same way; a fault in the client's own send loop makes the
  producer unusable; and an error the client raised itself rather than the broker — a `TypeError`
  while encoding an offset, say — makes the transaction abort-only as a
  `KafkaJSTransactionAbortableError` whose `cause` is that error, leaving the producer usable. A
  batch that had been sent before it gave up may have been written all the same, so its partition's
  sequence stays unresolved: `commit()` and `abort()` check every partition's sequence before
  `EndTxn`, and a gap nothing else reported makes the transaction abort-only ("The client hasn't
  received acknowledgment for some previously sent messages and can no longer retry them. It is safe
  to abort the transaction and continue."), and no `EndTxn` is sent while a batch is still in flight
  — which only a send-loop fault, having already made the producer unusable, can leave behind.
  `commit()` and `abort()` wait for the transaction's own sends and `sendOffsets` to settle; an
  `abort()` called while a `commit()` is under way waits for it and ends the transaction only if the
  commit failed. `abort()` always ends the transaction: a failure inside it is logged unless it
  leaves the producer unusable. Under transaction version 1 an abort bumps the producer epoch
  (KIP-360) when the transaction was abort-only, as the Java client does, or when it cancelled a
  batch that had already been sent once; under either version it does so when its own `EndTxn`
  failed. A send refused before any record was queued — for its own arguments (an empty topic, a
  message without a value, a record that cannot be encoded) or by a custom partitioner that threw —
  leaves the transaction as it was.
  *What to do:* wrap the transaction in `try` and, on any error, `abort()` first. A send rejects
  with its own error — the broker's, such as a `KafkaJSProtocolError` with code 90 when another
  producer took over the `transactionalId` — and the wrapper classes come from `commit()`, `abort()`
  and every later call: `abort()` rejects only when the producer cannot continue, with a
  `KafkaJSApplicationRecoverableError`, and that is when to create a new producer; otherwise keep
  it, and for `invalidConfiguration` also fix the record, config or ACL. A send rejected inside a
  transaction after its records were queued always ends that transaction, so `commit()` after one
  will not silently publish a partial transaction.

- **`consumer.groupMetadata()` is new, and `sendOffsets` accepts `consumerGroupMetadata`**
  (KIP-447). Passed through, the consumer's generation, member id and group instance id go on the
  `TxnOffsetCommit`, so the group coordinator rejects a commit from a member that a rebalance
  replaced. The legacy `{ consumerGroupId }` shape still works and still commits without group
  fencing. Passing both, or neither, throws `KafkaJSNonRetriableError`.
  *What to do:* switch consume-transform-produce loops to
  `sendOffsets({ consumerGroupMetadata: consumer.groupMetadata(), topics })` and re-read the
  metadata after every rebalance. Unchanged from 2.2.4: `sendOffsets` marks the offsets committed in
  the client's own cluster cache as soon as they are sent, not after `commit()` succeeds, so an
  aborted transaction leaves that cache optimistic — the same behaviour, and the same work-around,
  as in 2.2.4.

- **`ILLEGAL_GENERATION`, `UNKNOWN_MEMBER_ID`, `FENCED_INSTANCE_ID` and `STALE_MEMBER_EPOCH` from a
  transactional offset commit are abortable**, not a bare failure: `sendOffsets` rejects with a
  `KafkaJSTransactionAbortableError` whose `cause` is the broker's error (`category: 'abortable'`),
  and the transaction must be aborted and the consumer rejoined. On every other request those
  codes keep the category they have.
  *What to do:* abort, let the consumer rejoin, reprocess from the committed offsets. A
  classic-protocol member sees code 22 and a `groupProtocol: 'consumer'` member sees 113 for the
  same condition.

### Consumer

Rebalancing, fetching, committing and group membership, for the classic protocol and, where an item
says so, `groupProtocol: 'consumer'`.

- **`retry.restartOnFailure` is consulted for every terminal consumer crash.** It used to be
  invoked only for errors KafkaJS considered retriable, so a callback that returned `true` or
  `false` for a non-retriable error was silently ignored. If you never set it, nothing changes: the
  default is still "restart on retriable errors, stop on non-retriable ones". If the consumer is
  stopped or disconnected while the callback is still deciding, no restart is scheduled. A fenced
  static member (`FENCED_INSTANCE_ID`, 82) and, under `groupProtocol: 'consumer'`, a fatal
  membership error never restart, whatever the callback returns (see "A fatal membership error
  never restarts under `groupProtocol: 'consumer'`"). *What to do:* check that a
  `restartOnFailure` you already pass returns what you want for non-retriable errors too.

- **`groupInstanceId` makes a static member that does not leave the group on
  `consumer.disconnect()`.**
  The option (static membership, KIP-345) now takes effect; in 2.2.4 it was not a consumer option
  and passing it did nothing. A consumer that passes it keeps its slot and its partitions when it
  disconnects, until `sessionTimeout` elapses, instead of triggering a rebalance for every replica
  of a rolling restart. A restart that rejoins under the same `groupInstanceId` before
  `sessionTimeout` takes its partitions over with no rebalance, and the old, superseded process is
  fenced (`FENCED_INSTANCE_ID`, 82) and crashes without restarting. *What to do:* a static member
  that disconnects and is not coming back holds its partitions until `sessionTimeout`; lower
  `sessionTimeout` for that consumer, or do not pass `groupInstanceId`.
  Under `groupProtocol: 'consumer'` a static member does send a leave (see "A static member under
  `groupProtocol: 'consumer'` sends a leave on close and on poll expiry").

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
  case, where the resume point comes from `fromBeginning` rather than from the broker. It is new in
  3.0.0 and exists on the consumer only. Previously the records past the divergence point were
  silently skipped or silently redelivered, with nothing in the log to say so. A paused partition
  can be repositioned this way and stays paused. The check is served by a protocol call the client
  did not make before, `OffsetForLeaderEpoch`, which also appears on the public `Broker` type as
  `broker.offsetForLeaderEpoch()`; against a broker that advertises no usable version of it the
  consumer asks nothing, logs one notice per broker per assignment, and fetches unvalidated exactly
  as it did before.

- **Committed offsets now carry the leader epoch.** `consumer.commitOffsets` and auto-commit send
  the epoch of the record whose offset is being committed (the `CommittedLeaderEpoch` field
  OffsetCommit gained at v6, which this client previously always sent as `-1`), and read it back on
  assignment. That epoch is what makes the truncation check ("Log truncation is detected and acted
  on.") possible. Offsets committed by an older client have no epoch, so the first assignment after
  upgrading validates nothing; from the first commit onwards it does. An offset you commit yourself
  through `consumer.commitOffsets(offsets)` still carries no epoch — the API takes a topic, a
  partition and an offset, and the client will not guess which epoch you meant — and neither does an
  offset resolved to a position with no record behind it, such as one reached by a seek or by a
  log-compacted advance.

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

- **A fetch round can pause briefly after a leader election while positions are checked.** The
  truncation check ("Log truncation is detected and acted on.") is a gate rather than a
  background task: when a partition's leader epoch
  changes, that partition is held out of the next fetch request until the new leader has answered
  for it. The check has its own small budget — at most two retries, capped at five seconds of
  backoff — and is not governed by your `retry` options; if it has not resolved by then, the round
  fetches that partition unvalidated rather than stalling. Partitions whose epoch did not change are
  fetched throughout. In practice this is a latency change measured in milliseconds, visible only
  around leader elections and at the first fetch of a new assignment, and never a stall.

- **The rebalance timeout default changes from 60 s to 5 minutes.** The rebalance timeout sent to
  the coordinator is now the new `maxPollInterval` option, which defaults to 300000 ms and matches
  the Java client's `max.poll.interval.ms`. If you never set `rebalanceTimeout`, a member that stops
  responding now holds up a group rebalance for up to 5 minutes instead of 60 seconds. **To keep the
  old 60 s rebalance timing, set `rebalanceTimeout: 60000`** — the legacy option still goes on the
  wire unchanged, and it never becomes a handler deadline below 5 minutes (see "`rebalanceTimeout`
  is deprecated, and it does not become your eviction deadline."). Setting `maxPollInterval: 60000`
  instead would also shorten your handler deadline to 60 s, which is usually not what you want. One
  related timing does change if you had raised the rebalance timeout above 60 s: the client-side
  timeout on the SyncGroup request is now capped at 65 s, where it used to follow the rebalance
  timeout, so a `rebalanceTimeout: 120000` waits 65 s for the assignment instead of 125 s. That is
  what the Java client sends. If your group leader genuinely needs longer to compute an assignment,
  raise the client-wide `requestTimeout`, which still raises this one with it. At the default
  nothing changes: it was 65 s and it stays 65 s. The JoinGroup request goes the other way: its
  client-side timeout is `maxPollInterval + 5000`, so 305 s at the default where 2.2.4 gave 65 s. A
  JoinGroup is held open by the coordinator until every member has rejoined, so the client has to be
  willing to wait out the whole rebalance timeout — the Java client sizes it the same way — but a
  coordinator that accepts a join and then goes silent now stalls that consumer for about five
  minutes instead of about one, holding an in-flight request slot for that long. A lower client-wide
  `requestTimeout` does not shorten it; a lower `maxPollInterval` does.

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

- **Two ordering details around a rebalance.** The auto-commit a revocation triggers is now always
  sent before `consumer.rebalancing` is emitted; which came first used to depend on which path
  noticed the rebalance. And a rejoin caused by a stale assignment — a fetch that finds the member
  no longer owns what it is fetching — now emits `consumer.rebalancing` and auto-commits like every
  other revocation; 2.2.4 rejoined silently and committed nothing. *What to do:* if you use
  `consumer.rebalancing` to flush your own state, expect it in the stale-assignment case too, after
  the commit.

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
- **On the consumer group protocol, offsets are committed and fetched by topic id.** A consumer with
  `groupProtocol: 'consumer'` against Kafka 4.2 or later sends OffsetCommit and OffsetFetch v10
  (topic ids, KIP-848) when it knows the id of every topic in the request; `UNKNOWN_TOPIC_ID`,
  which a deleted and recreated topic answers, is retried after a metadata refresh. Kafka 4.0 and
  4.1 do not advertise v10, so nothing changes there, and the classic protocol and the admin client
  keep sending topic names. *What to do:* nothing.

### Admin

Group visibility and the admin client.

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

- **`admin.describeGroups` and `consumer.describeGroup()` negotiate `DescribeGroups` up to v6, and a
  group that does not exist is reported differently.** Against a 4.x broker the request used to be
  capped at v2, where a missing group came back as a row in state `Dead` with `errorCode: 0`. At v6
  the broker answers `GROUP_ID_NOT_FOUND` (69) with a message. The row keeps its shape —
  `state: 'Dead'`, `members: []`, `protocol: ''`, `protocolType: ''` — and now also carries
  `errorCode: 69` and the broker's `errorMessage`; the call still resolves, is not retried (69 is
  not retriable) and returns one row per requested group id. Both calls ask the consumer-protocol
  API, `ConsumerGroupDescribe`, first, and an error that API reports for one group no longer
  fails the whole request: it is carried on that group's row as `errorCode`/`errorMessage`
  while the other groups come back normally. Every other group-level error that `DescribeGroups`
  itself reports still fails the call and is still retried exactly as before. A coordinator that
  is loading or has moved is retried on the configured budget as before, and a transport or
  coordinator error that outlives it still fails the call. Rows are not returned in the order the
  ids were passed, as before. Rows also gain `errorMessage` and `authorizedOperations`, and
  members gain `groupInstanceId` (static membership). *What to do:* code that tested
  `state === 'Dead'` keeps working; code that reads `errorCode: 0` as "the group exists", or
  compares a whole row for deep equality, needs updating.
- **`createTopics` with `waitForLeaders` no longer fails while a new topic propagates.** On KRaft
  the broker that took the request can need a few seconds to know the topic; the leader wait used
  to fail with `UNKNOWN_TOPIC_OR_PARTITION` (code 3) in that window and now keeps waiting, within
  `timeout` (10 s when it is not set); a code 3 that never clears ends as a `KafkaJSTimeout`.
  *What to do:* nothing; a spurious failure is gone.
- **`DeleteGroups` throttling is honoured.** Against a 4.x broker kafkajs 2.2.4 negotiated
  `DeleteGroups` v1, whose decoder reported `clientSideThrottleTime` as `undefined` whatever the
  broker sent, so a broker that throttled the request was never waited out on that connection. The
  client now negotiates v2; v1 and v2 report the number (`0` when the broker did not throttle; v0
  reports `throttleTimeMs` only) and the connection waits it out as it does for every other API
  (KIP-219). `admin.deleteGroups()` results are unchanged. *What to do:* nothing.
- **Cluster-default broker configs can be described and altered.** `admin.describeConfigs` and
  `admin.alterConfigs` threw `KafkaJSNonRetriableError` (invalid resource name) for a `BROKER`
  resource whose name is the empty string. That name now stands for the cluster-wide default broker
  config, and all three config methods send it to any broker. Every other resource type still needs
  a non-empty name. *What to do:* nothing.
- **`createTopics`, `deleteTopics` and `createPartitions` retry a quota refusal.** On a cluster with
  controller-mutation quotas (KIP-599) a broker now refuses with `THROTTLING_QUOTA_EXCEEDED` a
  request it used to slow down, because the client negotiates newer versions of these APIs. The
  client waits the throttle time the broker reports and sends only the refused topics again within
  the `retry` budget, so a call that succeeded under 2.2.4 still succeeds, a little later; one that
  outlives the budget rejects with `KafkaJSNumberOfRetriesExceeded`, whose `cause` is the quota
  error. *What to do:* nothing.
- **Admin and broker results gain keys.** Nothing was renamed or removed; code that compares whole
  objects sees more keys. `admin.fetchTopicMetadata()` topics gain `topicId` (`null` when the broker
  sends none) and `isInternal`, on the Metadata path and on the new `useDescribeTopicPartitions`
  path (the item "Metadata results gain fields", under *Protocol*, covers `cluster.metadata()`).
  `admin.describeConfigs()` entries gain `configType` and `documentation`. At broker level
  `createTopics` results gain `numPartitions`, `replicationFactor`, `configs`,
  `topicConfigErrorCode` and `topicId`, and `deleteTopics` results gain `topicId` and
  `errorMessage`. `admin.createTopics()` still resolves a boolean: read the id with
  `fetchTopicMetadata`. *What to do:* snapshot tests over these objects need the new keys.

### Errors and events

Error classes and categories, new instrumentation events and the payload keys they gain, and what is
now logged.

- **A version range that does not overlap fails with `KafkaJSServerDoesNotSupportApiKey`.**
  Version negotiation now rejects an API whose broker range and client range share no version,
  and the error message names both ranges, where 2.2.4 sent a version the broker rejected or chose
  one the client does not implement. *What to do:* none against a Kafka 4.x broker; if you see the
  error, the message says which API and which two ranges did not meet.

- **ApiVersions failures surface differently**. When a broker rejects every ApiVersions version
  the client can send, `connect()` now rejects with the broker's own `KafkaJSProtocolError`
  (`The version of API is not supported`) instead of a `KafkaJSNonRetriableError('API Versions
  not supported')`. A broker that rejects the client's software name or version
  (`INVALID_REQUEST`) closes that connection and rejects with a `KafkaJSNonRetriableError` that
  keeps the protocol error's `type`, `code` and `cause`. Code that matched the old message text
  needs updating; code that checks `retriable` behaves as before.

- **Error codes 90–133 are now recognised**. Errors a Kafka 4.x broker returns with those codes
  used to surface as `type: 'KAFKAJS_UNKNOWN_ERROR_CODE'`; they now carry their real type (for
  example `PRODUCER_FENCED`, `TRANSACTION_ABORTABLE`, `REBOOTSTRAP_REQUIRED`) and a `retriable`
  flag derived from Kafka's own exception hierarchy, so codes 100, 103, 106, 122, 123 and 133 are
  retried where 2.2.4 failed fast. Types, messages and flags of codes 0–89 are unchanged.

- **Six new instrumentation events.** `TOPIC_RECREATED` and `REBOOTSTRAP` are available on all
  three clients. Subscribe with `client.events.TOPIC_RECREATED` / `client.events.REBOOTSTRAP` as
  usual; the `type` string delivered on the event is `consumer.cluster.topic_recreated`,
  `producer.cluster.topic_recreated`, `admin.cluster.topic_recreated`,
  `consumer.cluster.rebootstrap`, `producer.cluster.rebootstrap` and
  `admin.cluster.rebootstrap` respectively. `TOPIC_RECREATED`'s payload is
  `{ topic, previousTopicId, topicId }` and fires when a topic name the client already had metadata
  for comes back with a different topic id; on a consumer it accompanies the one-shot offset reset
  ("A recreated topic resets that topic's consumer offsets once."), while on a producer and an admin
  client it is purely observational. `REBOOTSTRAP`'s payload is `{ reason }`, where `reason` is
  `'timeout'`, `'required'` or `'all-brokers-unavailable'` for the three triggers ("Client
  rebootstrap is on by default."); it never fires under `metadataRecoveryStrategy: 'none'`.

- **The `COMMIT_OFFSETS` event's payload gains `groupInstanceId`.** It is `string | null` and is
  present for every consumer, not only those configured for static membership — `HEARTBEAT`'s
  payload gained the same key. Every key the payload already had is unchanged, so only code that
  deep-equality-compares the whole payload is affected.

- **The `COMMIT_OFFSETS` event's payload gains `leaderEpoch`.** Each entry of
  `topics[].partitions[]` now carries `leaderEpoch` (a number, `-1` when the epoch is unknown)
  alongside `partition` and `offset`, and the entries returned by `consumer.uncommittedOffsets()`
  gain the same key. Every key those payloads already had is unchanged, so only code that
  deep-equality-compares a whole payload is affected — the same kind of additive change
  `groupInstanceId` was ("The `COMMIT_OFFSETS` event's payload gains `groupInstanceId`.").

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

- **Every error this client raises through a KafkaJS error class carries a `category`, and five
  error classes are added.** `error.category` is one of `retriable`, `refreshRetriable`,
  `abortable`, `applicationRecoverable` or `invalidConfiguration` (KIP-1050) and names the remedy:
  retry, refresh and retry, abort the transaction and keep the producer, create a new producer, or
  fix the configuration, record or ACL. Errors are **not** re-wrapped: `err.name`, `instanceof` and
  every field an error already carried are unchanged, and `category` is the one new key. The new
  classes are
  `KafkaJSTransactionAbortableError`, `KafkaJSApplicationRecoverableError`,
  `KafkaJSInvalidConfigurationError`, `KafkaJSDeliveryTimeout` (all `KafkaJSNonRetriableError`
  subclasses) and `KafkaJSRefreshRetriableError`. `TRANSACTION_ABORTABLE` (120) and
  `PRODUCER_FENCED` (90) are known codes rather than `KAFKAJS_UNKNOWN_ERROR_CODE`.
  *What to do:* prefer `error.category` over string-matching. `error.retriable` keeps its meaning;
  `KafkaJSRefreshRetriableError` is retriable, the other four new classes are not. Snapshot tests
  over serialized errors will see one added field — `category` is an own enumerable property, so
  it appears in `JSON.stringify(error)` and in structured logs. On the transaction APIs a fenced
  epoch now arrives as `PRODUCER_FENCED` (90) rather than `INVALID_PRODUCER_EPOCH` (47); code that
  matched 47 should match `error.category` or both codes. Outside a transaction,
  `KafkaJSNumberOfRetriesExceeded` means the send ran out of its retry budget; the producer remains
  usable, and retrying the send is safe on an idempotent producer.
- **Instrumentation events are also published on `node:diagnostics_channel`.** Each of the 42
  events is on the channel `kafkajs.<event name>`, with the same event object an `on()` listener
  receives; when an event has both, channel subscribers run first. Subscribers run synchronously and
  must not throw (Node turns a throw into an `'uncaughtException'`). *What to do:* nothing; to use
  it, subscribe with `diagnostics_channel.subscribe('kafkajs.consumer.fetch', fn)`.

### Protocol

Wire and connection behavior: negotiated versions, version floors, authentication and rebootstrap.

- **Request versions below the Kafka 4.0 floors are gone.** The client no longer implements Produce
  v0–2, Fetch v0–3, ListOffsets v0, OffsetCommit v0–1, CreateTopics v0–1, DeleteTopics v0,
  DescribeAcls/CreateAcls/DeleteAcls v0 and DescribeConfigs v0 (KIP-896, KIP-724). A Kafka 4.x
  broker never negotiates them. Reading old message-format (magic 0 and 1) records out of a Fetch
  response still works, because old segments can be served inside Fetch v4 and later. *What to
  do:* nothing against a Kafka 4.x cluster.

- **The raw SASL token path is gone.** The pre-KIP-152 exchange, used only when a broker lacked
  `SaslAuthenticate`, is removed; every mechanism goes through `SaslAuthenticate`, which every 4.x
  broker has. *What to do:* nothing against a Kafka 4.x cluster.

- **A broker that just rejected your SASL credentials is left alone for a while.** After a
  `SASL_AUTHENTICATION_FAILED` (58) the client waits `max(authenticationTimeout, 1000 ms)` — 10 s
  at the default `authenticationTimeout` — before it authenticates to that broker again, to avoid
  a reconnect storm against credentials that will keep failing. A refused attempt throws the same
  non-retriable `KafkaJSSASLAuthenticationError` class as the original failure, whose message
  contains the broker's own message, so a retrier stops at once instead of ending in
  `KafkaJSNumberOfRetriesExceeded`. A connection that is already authenticated is unaffected.
  *What to do:* after fixing credentials, expect the first reconnect to wait out the window.

- **ApiVersions and Metadata are negotiated at newer versions**: ApiVersions from v2 to v4, and
  Metadata from v6 to v13. ApiVersions is now re-negotiated whenever a broker is (re)connected
  through `connect()` rather than once per broker object, so each such (re)connect costs one extra
  round trip per broker. A fetch socket that the connection pool re-establishes on its own, while
  the broker's other socket stays up, keeps the current version table until the next `connect()`.

- **`cluster.metadata()` no longer returns `clusterAuthorizedOperations`**. That top-level field
  only exists up to Metadata v10 and was never requested (it was always `-2147483648`); at v11+
  it is simply absent. The new per-topic `topicId` / `topicAuthorizedOperations` and per-partition
  `leaderEpoch` keys are additive; every key kafkajs 2.2.4 returned is still there.

- **`Broker#isConnected()` also requires a completed version negotiation**. A `Broker` handed
  back by `Cluster#findBroker` and friends reports `true` only once its socket is up **and**
  ApiVersions has been negotiated, so a caller can no longer observe a connected-but-unusable
  broker. This can only turn a previously wrong `true` into a correct `false`.

- **Metadata results gain fields**. Partition objects returned by `admin.fetchTopicMetadata()`
  (and `cluster.metadata()`) now include `leaderEpoch`; the topics `cluster.metadata()`
  returns also include `topicId` and `topicAuthorizedOperations`. Existing keys are unchanged;
  deep-equality snapshots of these results need updating.

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
  through the caller's normal retry path against the rebuilt cluster. On an idempotent
  producer (the default) the retry carries the same producer id and sequence numbers, so the broker
  deduplicates it; **on a producer created with `idempotent: false`, or one whose configuration
  turned idempotence off, a produce request that the broker had already appended, but whose
  response was lost when the connection closed, is sent again on the retry and can duplicate those
  records.** The SASL authentication blackout survives a rebootstrap, and an authentication
  failure is never a trigger — a rejected credential means the broker is reachable and healthy,
  which no rebootstrap can fix. Setting `metadataRecoveryStrategy: 'none'` disables all three
  triggers and restores the previous behavior exactly, with code 129 surfacing as the non-retriable
  protocol error it already was.

- **Negotiated protocol versions moved up.** Against a Kafka 4.1 or 4.3 broker the client now
  negotiates: Metadata **v13** (was v6), Produce **v13** (was v7; **v11** when a topic's id is not
  yet known, **v11** or **v12** inside a transaction), FindCoordinator **v6** (was v2),
  OffsetCommit **v9** (was v5), OffsetFetch **v9** (was v4), ListOffsets **v9** (was v3) and
  InitProducerId **v5** (was v1). What each means for a caller:
  - *Metadata v13* adds a top-level error code to the response, surfaced ahead of any topic- or
    partition-level error; it is how a broker asks a client to rebootstrap. Decoded metadata gains
    an additive `errorCode`.
  - *Produce v11* moves the hottest write path onto the compact (flexible) encoding and is what
    makes the per-record errors and leader hints possible (see "Produce errors carry per-record
    detail." and "The producer follows leader hints.").
  - *FindCoordinator v6* takes a list of coordinator keys and answers with a list; this client asks
    for one and reads one, so the decoded result keeps the shape it always had.
  - *OffsetCommit v8* is what finally sends `groupInstanceId` on offset commits for consumers using
    static membership; `groupInstanceId` now also reaches JoinGroup, SyncGroup, Heartbeat and
    LeaveGroup ("`groupInstanceId` makes a static member that does not leave the group on
    `consumer.disconnect()`."). The per-partition committed leader epoch the protocol gained at v6
    is the leader epoch of the *consumed record*; "Committed offsets now carry the leader epoch."
    says when it is sent.
  - *OffsetFetch v8* takes and returns a list of groups; again one is sent and one is read. Each
    partition additionally carries `committedLeaderEpoch`, which the consumer reads back on
    assignment ("Committed offsets now carry the leader epoch."). `RequireStable` is sent `false`,
    so a transactional producer's in-flight offsets are returned exactly as before.
  - *Produce v13, OffsetCommit v9 and OffsetFetch v9* are the newest versions the client
    implements. Produce v13 keys each topic by its id ("Produce and fetch requests are keyed by
    topic id."). OffsetCommit v9 and OffsetFetch v9 carry the consumer protocol's member epoch; a
    classic member and an admin client send the defaults, so they mean what they meant at v8 ("A
    commit for a group that no longer exists rejoins instead of crashing, and the broker's code
    for it changed.").
  - *ListOffsets v9* sends the leader epoch the client currently believes is in effect for each
    partition (`-1` when it does not know one), so a broker can reject a request built on metadata
    that has since moved; the response's per-partition leader epoch is decoded and returned
    additively. Because the request now carries an epoch, `FENCED_LEADER_EPOCH` and
    `UNKNOWN_LEADER_EPOCH` can now come back from the offset APIs, where they could not before —
    the client refreshes metadata and retries, so callers see a slower call rather than a failure.
  - *InitProducerId v5* sends the producer id and epoch fields added at v3 as their defaults
    (`-1`) on a first initialization. A transactional producer that bumps its epoch (after an
    abort that needs one, or when the cluster's transaction version moves from 1 to 2) sends the
    pair it holds (KIP-360). An idempotent-only producer bumps its epoch locally, with no request,
    and takes a new producer id, again with the default pair, only when its epoch is exhausted.
    `PRODUCER_FENCED` and `TRANSACTION_ABORTABLE` can now reach an idempotent or transactional
    producer's initialisation, both as the non-retriable protocol errors they already are.

  Two versions are deliberately not implemented. **ListOffsets stops at v9**: v10 and v11 exist for
  tiered storage — asynchronous remote list-offsets and the earliest pending upload offset — which
  this client does not use, and both change the request's shape and semantics. **InitProducerId
  stops at v5**: v6 is the two-phase-commit protocol, which upstream still marks unstable, so
  brokers do not advertise it by default.

- **Produce and fetch requests are keyed by topic id.** Requests use topic ids (KIP-516) whenever
  the client knows them, and fall back to topic names for that one request otherwise. This is
  invisible in the API: the decoded fetch and produce responses, the `Batch` handed to `eachBatch`
  and the fields of a produce error are all unchanged, and topic ids never reach a caller. What it
  changes is which protocol version the client negotiates, and therefore what the compatibility
  matrix reports: against a Kafka 4.1 or 4.3 broker, Fetch moves from **v11** to **v18** (**v12**
  for a request in which any topic's id is not yet known) and Produce from **v11** to **v13**
  (**v11** for a request in which any topic's id is not yet known). A send inside a transaction is
  held at Produce **v11** under transaction version 1: from v12, producing inside a transaction
  implicitly adds the partition to it, which is what transaction version 2 does ("Transactions use
  the transaction protocol version 2 on Kafka 4.x clusters"). `SaslAuthenticate` also moves to
  **v2** and `OffsetForLeaderEpoch` **v4** appears; custom SASL mechanisms are unaffected, since a
  provider still returns length-prefixed bytes from `request.encode()` and still receives
  length-prefixed bytes in `response.decode()`.

- **A broker whose API versions were learned on a superseded socket is treated as not connected.**
  `broker.isConnected()` already required a completed version negotiation ("`Broker#isConnected()`
  also requires a completed version negotiation"); it now additionally requires that the negotiation
  happened on the *current* connection, so a socket the connection pool re-establishes on its own
  re-negotiates before it serves a request instead of reusing a version table learned on a socket
  that is gone. This can only turn a previously optimistic `true` into a correct `false`. It has two
  costs, both one-off and both self-correcting: one extra `ApiVersions` round trip the first time a
  broker's second socket is opened, and — because the group coordinator is re-resolved whenever its
  broker reports itself not connected — one extra `FindCoordinator` before the next commit after
  such a reconnect.

- **`broker.fetch()` no longer rejects on a fetch error — it returns the codes.** `Broker` is a
  public type and `cluster.findBroker()` hands one to application code. Previously a fetch
  response's partition errors rejected the promise (the first failing partition won) and its
  top-level error code was decoded but never read. From Fetch v12 neither rejects: both come back in
  the response body, so a direct `broker.fetch(...)` caller reads `errorCode` at the top level and
  `responses[].partitions[].errorCode` per partition instead of catching. If you call
  `broker.fetch(...)` yourself and rely on a rejection to notice a bad partition, that code needs
  updating. The consumer is the only in-tree caller and classifies both codes for you, as described
  in "A fetch error on one partition no longer discards the whole response or rejoins the group."
  and "A fetch response's top-level error code is now acted on."

### TypeScript

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
- **`Consumer`, `Broker`, `typeof Partitioners` and `KafkaJSError` gained required members**:
  `consumer.groupMetadata()`; the broker's `finalizedFeatures`, `finalizedFeaturesEpoch`,
  `initProducerId`, `endTxn` and `txnOffsetCommit`; `Partitioners.UniformStickyPartitioner`; and
  every error's `category`. Code that *constructs* these types by hand — test fixtures, mocks, an
  object typed `Consumer` — must supply them. Code that only reads or calls them is unaffected.

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
