---
id: consuming
title: Consuming Messages
---

Consumer groups allow a group of machines or processes to coordinate access to a list of topics, distributing the load among the consumers. When a consumer fails the load is automatically distributed to other members of the group. Consumer groups __must have__ unique group ids within the cluster, from a kafka broker perspective.

Creating the consumer:

```javascript
const consumer = kafka.consumer({ groupId: 'my-group' })
```

Subscribing to some topics:

```javascript
await consumer.connect()

await consumer.subscribe({ topics: ['topic-A'] })

// You can subscribe to multiple topics at once
await consumer.subscribe({ topics: ['topic-B', 'topic-C'] })

// It's possible to start from the beginning of the topic
await consumer.subscribe({ topics: ['topic-D'], fromBeginning: true })
```

Alternatively, you can subscribe to any topic that matches a regular expression:

```javascript
await consumer.connect()
await consumer.subscribe({ topics: [/topic-(eu|us)-.*/i] })
```

Client-side `RegExp` subscriptions are re-evaluated at metadata refresh opportunities. Topics created later can join and deleted matches leave after the corresponding subscription revision applies. Matching uses cloned expressions without `g` and `y`; other flags and partial matching are preserved, and the caller's expression is unchanged. Topics flagged internal by the broker are excluded from patterns; an explicit literal name can still subscribe to one. The last matching subscription call supplies the topic's reset policy.

The regular expression is matched against the metadata of every topic, which a partition without a leader does not fail: `subscribe()` resolves while a topic the expression does not match has a partition without a leader. The topics it does match are then asked for by name, and a partition of theirs without a leader is retried within the `retry` budget, after which `subscribe()` rejects.

KafkaJS offers you two ways to process your data: `eachMessage` and `eachBatch`

## <a name="each-message"></a> eachMessage

The `eachMessage` handler provides a convenient and easy to use API, feeding your function one message at a time. It is implemented on top of `eachBatch`, and it will automatically commit your offsets and heartbeat at the configured interval for you. If you are just looking to get started with Kafka consumers this a good place to start.

```javascript
await consumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
        console.log({
            key: message.key.toString(),
            value: message.value.toString(),
            headers: message.headers,
        })
    },
})
```

Yielding asynchronous work is kept alive by the background heartbeat while it waits, up to `maxPollInterval`. Increasing `sessionTimeout` or manually calling `heartbeat()` does not extend that processing deadline. An event-loop-blocking handler prevents background heartbeats and the poll guard from running, and can lose membership after `sessionTimeout`. The poll guard is global across all partitions: another partition making progress can refresh it, so it is not a separate deadline for each concurrently running handler.
The `pause` function is a convenience for `consumer.pause({ topic, partitions: [partition] })`. It will pause the current topic-partition and returns a function that allows you to resume consuming later.

## <a name="each-batch"></a> eachBatch

Some use cases require dealing with batches directly. This handler will feed your function batches and provide some utility functions to give your code more flexibility: `resolveOffset`, `heartbeat`, `commitOffsetsIfNecessary`, `uncommittedOffsets`, `isRunning`, `isStale`, and `pause`. All resolved offsets will be automatically committed after the function is executed.

> Note: Be aware that using `eachBatch` directly is considered a more advanced use case as compared to using `eachMessage`, since you will have to understand how session timeouts and heartbeats are connected.

```javascript
await consumer.run({
    eachBatchAutoResolve: true,
    eachBatch: async ({
        batch,
        resolveOffset,
        heartbeat,
        commitOffsetsIfNecessary,
        uncommittedOffsets,
        isRunning,
        isStale,
        pause,
    }) => {
        for (let message of batch.messages) {
            console.log({
                topic: batch.topic,
                partition: batch.partition,
                highWatermark: batch.highWatermark,
                message: {
                    offset: message.offset,
                    key: message.key.toString(),
                    value: message.value.toString(),
                    headers: message.headers,
                }
            })

            resolveOffset(message.offset)
            await heartbeat()
        }
    },
})
```

* `eachBatchAutoResolve` configures auto-resolve of batch processing. If set to true, KafkaJS will automatically commit the last offset of the batch if `eachBatch` doesn't throw an error. Default: true.
* `batch.highWatermark` is the last committed offset within the topic partition. It can be useful for calculating lag.
* `resolveOffset()` is used to mark a message in the batch as processed. On a handler error, the consumer attempts to commit resolved progress only while membership and the batch remain usable; a failed progress commit does not replace the handler's own error.
* `heartbeat(): Promise<void>` can be used to send heartbeat to the broker according to the set `heartbeatInterval` value in consumer [configuration](#options), which means if you invoke `heartbeat()` sooner than `heartbeatInterval` it will be ignored.
* commitOffsetsIfNecessary(offsets?): Promise<void> checks automatic commit eligibility at that call. Without explicit offsets, configured interval or threshold conditions apply. Batch completion also performs the applicable automatic check when autoCommit is enabled. See [autoCommit](#auto-commit) for the opportunity and idle-time behavior; explicit offsets retain the existing commit rules.
* `uncommittedOffsets()` returns all offsets by topic-partition which have not yet been committed.
* `isRunning()` returns true if consumer is in running state, else it returns false.
* `isStale()` returns whether the messages in the batch have been rendered stale through some other operation and should be discarded. For example, when calling [`consumer.seek`](#seek) the messages in the batch should be discarded, as they are not at the offset we seeked to.
* `pause()` can be used to pause the consumer for the current topic-partition. All offsets resolved up to that point will be committed (subject to `eachBatchAutoResolve` and [autoCommit](#auto-commit)). Throw an error to pause in the middle of the batch without resolving the current offset. Alternatively, disable `eachBatchAutoResolve`. The returned function can be used to resume processing of the topic-partition. See [Pause & Resume](#pause-resume) for more information about this feature.

### Example

```javascript
consumer.run({
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
        for (let message of batch.messages) {
            if (!isRunning() || isStale()) break
            await processMessage(message)
            resolveOffset(message.offset)
            await heartbeat()
        }
    }
})
```

In the example above, if the consumer is shutting down in the middle of the batch, the remaining messages won't be resolved and therefore not committed. This way, you can quickly shut down the consumer without losing/skipping any messages. If the batch goes stale for some other reason (like calling `consumer.seek`) none of the remaining messages are processed either.

## <a name="concurrent-processing"></a> Partition-aware concurrency

By default, [`eachMessage`](Consuming.md#each-message) is invoked sequentially for each message in each partition. In order to concurrently process several messages per once, you can increase the `partitionsConsumedConcurrently` option:

```javascript
consumer.run({
    partitionsConsumedConcurrently: 3, // Default: 1
    eachMessage: async ({ topic, partition, message }) => {
        // This will be called up to 3 times concurrently
    },
})
```

Messages in the same partition are still guaranteed to be processed in order, but messages from multiple partitions can be processed at the same time. If `eachMessage` consists of asynchronous work, such as network requests or other I/O, this can improve performance. If `eachMessage` is entirely synchronous, this will make no difference.

The same thing applies if you are using [`eachBatch`](Consuming.md#each-batch). Given `partitionsConsumedConcurrently > 1`, you will be able to process multiple batches concurrently.

A guideline for setting `partitionsConsumedConcurrently` would be that it should not be larger than the number of partitions consumed. Depending on whether or not your workload is CPU bound, it may also not benefit you to set it to a higher number than the number of logical CPU cores. A recommendation is to start with a low number and measure if increasing leads to higher throughput.

## <a name="auto-commit"></a> autoCommit

Resolved offsets are committed at processing/check opportunities. With neither autoCommitInterval nor autoCommitThreshold set, successful data batches retain per-batch automatic commits. With either knob set, a batch-end check commits when the interval has elapsed or the threshold has been reached; either condition is sufficient when both are set. There is no background commit timer. A below-interval batch followed by idle time does not commit solely because the interval later elapses; a later processing/check opportunity can commit it.

autoCommitInterval is the elapsed interval in milliseconds checked at an opportunity. autoCommitThreshold is the resolved-offset threshold checked at an opportunity. Both default to null. autoCommit:false disables automatic commits, including the final automatic attempt; explicit commits remain available. Fewer actual broker commits can increase redelivery after a crash. Classic graceful stop, disconnect and unsubscribe attempt eligible final progress even when the interval or threshold is not due, subject to ownership, halt bounds and any close deadline.

```javascript
consumer.run({
  autoCommitInterval: 5000,
  // ...
})
```

```javascript
consumer.run({
  autoCommitThreshold: 100,
  // ...
})
```

`autoCommit`: Advanced option to disable auto committing altogether. Instead, you can [manually commit offsets](#manual-commits). Default: `true`

## <a name="membership-errors"></a> Membership errors during rebalance

When the group coordinator rejects a heartbeat or a fetch, the consumer tells apart two kinds of error and reacts differently:

| Error | Code | Meaning | Consumer action |
|---|--:|---|---|
| `ILLEGAL_GENERATION` | 22 | member is **LOST** | discard uncommitted offsets, never autocommit, rejoin (the member id is kept — the generation is stale, not the member id) |
| `UNKNOWN_MEMBER_ID` | 25 | member is **LOST** | same, plus reset the member id |
| `FENCED_INSTANCE_ID` | 82 | member is **LOST** (fenced by a duplicate static member) | same, and do not rejoin |
| `REBALANCE_IN_PROGRESS` | 27 | partitions are being **REVOKED** | a final commit is allowed before rejoining |
| `NOT_COORDINATOR_FOR_GROUP` | 16 | the coordinator moved | look the coordinator up again and keep the membership: no rejoin, as for `COORDINATOR_NOT_AVAILABLE` (15) |
| `GROUP_LOAD_IN_PROGRESS` | 14 | the coordinator is still loading the group | retry the request: no rejoin, as for 15 and 16 |

A LOST member's generation, member id, or static instance id has already been invalidated by the coordinator, so any offsets committed under it would be rejected forever — that used to surface as a repeating "Specified group generation id is not valid" error. A REVOKED member is still a valid part of the group, so committing its already-resolved offsets before rejoining is safe — this covers every rebalance-class error above and beyond it, not just 27. On the fetch path, a LOST or REVOKED error emits the same public `REBALANCING` instrumentation event before the consumer rejoins. A `consumer.commitOffsets()` call answered with `REBALANCE_IN_PROGRESS` still rejects with that error, and it also asks the group to rejoin, as the same answer to a heartbeat does.

Two more things happen around a rebalance, both new in 3.0.0. A handler that has not returned within `maxPollInterval` (see [Options](#options)) is treated as a lost member: the consumer logs a warning, emits `POLL_INTERVAL_EXCEEDED`, gives up its partitions and rejoins, and the batch that handler eventually returns is dropped and redelivered afterwards. The deadline is reset on every batch handed to your handler *and* on every fetch round that comes back empty, and there is one fetcher per broker, so the guard fires only when every fetcher is stalled behind the overrunning handler — in practice a single-broker assignment with `partitionsConsumedConcurrently: 1`. And on the **cooperative** path a `REBALANCE_IN_PROGRESS` no longer comes out of the `heartbeat()` callback into your handler — the call resolves and the rejoin runs in the background while your handler keeps the partitions it is not losing. On the default eager path that callback still throws, as before.

Not every fetch-path error is a rebalance signal, though: a corrupt record batch — a CRC32C mismatch (`checkCrcs`), a malformed compressed body, a batch that decompresses past `maxDecompressedBatchBytes`, or an impossible record count (see [Options](#options) and [Decompression bound](#decompression-bound)) — is not a rebalance and does not crash the consumer. It halts only its own partition; the other partitions keep delivering.

## <a name="decompression-bound"></a> Decompression bound

`maxDecompressedBatchBytes` is the most one record batch may decompress to, in bytes, for every codec (GZIP, Snappy, LZ4 and ZSTD). The default is 1073741824 (1 GiB); it is an integer from 1 to 2147483647. The same value also sets a ceiling of `max(1, floor(maxDecompressedBatchBytes / 512))` records per batch, checked before any record is allocated, so a batch can stay under the byte limit and still be refused for its record count. Raising the option raises both.

A batch that breaks the bound, a compressed body that is malformed, an impossible record count and a CRC32C mismatch all halt the partition the same way: the records before the batch are delivered, `consumer.partition_halted` fires with the `reason` (`decompressed-size`, `decompression`, `count` or `checksum`), the `codec` of a compressed batch, the `limit` in force (bytes, or records for a `count` over the ceiling) and, when the batch boundary could be validated, the `nextOffset` after it, and the partition is not fetched again. To skip the batch, call `consumer.seek` for the partition with the event's `nextOffset` or any later offset. A `checksum` halt carries no `nextOffset`, so you choose the offset. A rebalance that revokes the partition, or a restart, clears the halt; a restart halts again at the same batch. Offsets the client derives itself never cross the batch, while an offset you commit yourself keeps its ordinary meaning.

The bound is a limit on the decompressed bytes of one batch, not on the memory of the process. Each concurrent fetch may hold a full batch, and there is one fetch per broker with assigned partitions, so size the option against `maxBytes`, the number of brokers and the heap: a batch under the byte limit still costs 208 to 321 bytes of heap per decoded record, so a full 1 GiB batch of small records takes several GiB. Lower it to what your largest legitimate batch needs. LZ4 is checked before the native decoder is called, gzip and ZSTD while they decompress, Snappy before its output is allocated, and a codec you register yourself after it has run (see [Compression](Producing.md#compression-other)).

For a classic member (the default `groupProtocol`), a lost connection is not a rebalance signal either. When a broker refuses or resets a connection, the connection closes, or a request on it times out, the consumer retries its own request after `retry.initialRetryTime` without spending `retry.retries`; so it does when the group coordinator moved (`NOT_COORDINATOR`), is unavailable, or is still loading the group (`GROUP_LOAD_IN_PROGRESS`). This covers the join, and the Fetch, heartbeat and automatic commit requests of the fetch loop, and the consumer looks the coordinator and the partition leaders up again. It keeps its membership and partitions, and fires no `consumer.crash`, until one kind of request has failed for `sessionTimeout` since its first failure with no success: the time the coordinator keeps a classic member it does not hear from. A longer outage crashes as before, and `restartOnFailure` decides; while some leader still answers, a lost connection to another leader is retried until its partitions move, as the Java client does. An error thrown by your own handler, a lost connection included, counts against `retry.retries` as before. There is one exception for a classic member: if `eachBatch` propagates an OffsetCommit `COORDINATOR_LOAD_IN_PROGRESS` (14) from `commitOffsetsIfNecessary`, it is retried as a commit outage until `sessionTimeout`, without spending `retry.retries`. The same goes for a `NOT_COORDINATOR` (16) that a handler propagates from its `heartbeat()` or `commitOffsetsIfNecessary()` callback: it is ridden out like the request that raised it, until `sessionTimeout`, without spending `retry.retries`. A member with `groupProtocol: 'consumer'` keeps its counted retries.

## <a name="manual-commits"></a> Manual committing

When disabling [`autoCommit`](#auto-commit) you can still manually commit message offsets, in a couple of different ways:

- Use the public consumer.commitOffsets method for an explicit commit. The no-offset automatic check does not commit when autoCommit is false; supplying explicit offsets through an existing supported handler commit call retains its explicit-offset and ownership rules.
- By [sending message offsets in a transaction](Transactions.md#offsets).
- By using the `commitOffsets` method of the consumer (see below).

The `consumer.commitOffsets` is the lowest-level option and will ignore all other auto commit settings, but in doing so allows the committed offset to be set to any offset and committing various offsets at once. This can be useful, for example, for building a processing reset tool. It can only be called after `consumer.run`. Committing offsets does not change what message we'll consume next once we've started consuming, but instead is only used to determine **from which place to start**. To immediately change from what offset you're consuming messages, you'll want to [seek](#seek), instead.

```javascript
consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
        // Process the message somehow
    },
})

consumer.commitOffsets([
  { topic: 'topic-A', partition: 0, offset: '1' },
  { topic: 'topic-A', partition: 1, offset: '3' },
  { topic: 'topic-B', partition: 0, offset: '2' }
])
```

A consumer cannot commit after it has learned that its membership was lost. `consumer.commitOffsets()` and `eachBatch`'s explicit `commitOffsetsIfNecessary(offsets)` reject with `KafkaJSCommitFailedError`. Its `reason` is `membership-lost`, `poll-interval-exceeded`, or `stale-assignment`. A broker-reported loss keeps its original `type`, `code` and `cause`; synthetic poll expiry has type `POLL_INTERVAL_EXCEEDED` and no broker code, while a stale assignment has type `STALE_ASSIGNMENT` and no broker code. Test the error class and `reason` to recognize every refusal: `isLostMember` continues to match only broker codes 22, 25, 82 and 110.

A commit called from a handler keeps that handler's batch identity across asynchronous work. Rejoining, or revoking and reacquiring the same partition, does not make an old handler's commit valid. A partition retained through a cooperative round can continue its work. A call from a context that never ran inside a handler, such as a queue created at startup, has no handler batch and is checked against membership alone. Automatic commits are skipped while ownership is lost. A handler's own error is preserved when its catch cannot commit progress.

A commit transmitted before the loss was learned cannot be recalled. It may have been applied before the member left or rejected by the coordinator; the client cannot tell which. Its late completion does not update the client's cache or last-commit time or start another retry. An automatic commit carries processed offsets; an explicit commit carries the caller's offsets, so this guarantee makes no claim about application progress. Let the consumer rejoin and reprocess from the coordinator's committed offsets. Transactional `sendOffsets` and the admin offset APIs are separate contracts: `sendOffsets` carries the supplied consumer group metadata when given, while admin `setOffsets` and `resetOffsets` act on a group id.

Note that you don't *have* to store consumed offsets in Kafka, but instead store it in a storage mechanism of your own choosing. That's an especially useful approach when the results of consuming a message are written to a datastore that allows atomically writing the consumed offset with it, like for example a SQL database. When possible it can make the consumption fully atomic and give "exactly once" semantics that are stronger than the default "at-least once" semantics you get with Kafka's offset commit functionality.

The usual usage pattern for offsets stored outside of Kafka is as follows:

- Run the consumer with `autoCommit` disabled.
- Store a message's `offset + 1` in the store together with the results of processing. `1` is added to prevent that same message from being consumed again.
- Use the externally stored offset on restart to [seek](#seek) the consumer to it.

## <a name="from-beginning"></a> fromBeginning

The consumer group will use the latest committed offset when starting to fetch messages. If the offset is invalid or not defined, `fromBeginning` defines the behavior of the consumer group. This can be configured when subscribing to a topic:

```javascript
await consumer.subscribe({ topics: ['test-topic'], fromBeginning: true })
await consumer.subscribe({ topics: ['other-topic'], fromBeginning: false })
```

When `fromBeginning` is `true`, the group will use the earliest offset. If set to `false`, it will use the latest offset. The default is `false`.

`subscribe` also accepts `offsetReset: 'earliest' | 'latest' | 'none' | 'by_duration:<ISO-8601 duration>'`, per topic. The last successful subscription naming a topic wins. Earliest and latest match fromBeginning true and false. When fromBeginning is explicit, only its matching earliest/latest policy is accepted. None halts a partition with no committed offset, an out-of-range offset after the existing leader retry, or unknown truncation divergence; it emits one partition_halted event and error log for the episode. Other partitions continue. Seek supplies an explicit position; revocation, recreation or restart clears the episode, and restart halts again when no usable position exists.

Durations use unsigned days and time components, such as P1DT2H or PT10M. Signs, weeks, years and months are rejected. At least one component is required; zero is allowed. Up to nine fractional-second digits are accepted and sub-millisecond precision is truncated after exact safe-range validation. The client computes max(0, now minus duration) whenever resolution runs. If no record exists at or after that timestamp, it uses the log end with one latest lookup. This differs from Java's awaiting-until-a-later-record behavior. With autoCommit false, duration resets and later seeks do not automatically commit; explicit commits use actual decimal offsets.

If the partition's leader changes and the new leader's log diverges from what this consumer had already consumed — the tail of the old leader's log was never replicated — the consumer detects it before its next fetch and repositions the partition. When the divergence point is known, the partition moves to the first offset known to diverge and resumes from there; When the divergence point is unknown, the partition follows its reset policy: none halts, by_duration first resolves an actual timestamp-based position and commits it only with autoCommit enabled, while the existing earliest/latest forms retain their reset-commit behavior. Either way the consumer logs a warning and emits [`LOG_TRUNCATION`](InstrumentationEvents.md#consumer). A paused partition can be repositioned this way and stays paused — pausing is your intent, not offset state.

## <a name="topic-recreation"></a> Topic recreation

If a subscribed topic is deleted and recreated, the new topic is a different entity from the broker's point of view even though it has the same name — it gets a new topic id and starts back at offset 0. A committed offset from the old topic is meaningless against it, and because cleanup of the internal `__consumer_offsets` topic is asynchronous, that stale offset can still be there — and can still look like a perfectly valid offset into the new topic's log.

When the consumer notices a subscribed topic has come back under a new id, it resets that topic's offsets, and keeps owing the reset until a confirmed commit for the new topic settles it: it ignores the coordinator's committed offset for it and resolves the next offset from the configured boundary instead, per that subscription's [`fromBeginning`](#from-beginning) setting. This happens automatically and needs no configuration. A pause belongs to one topic incarnation. A recreated topic starts unpaused; pause it again after consumer.cluster.topic_recreated if needed. Ordinary rebalances preserve pause state.

The reset stays owed for a partition until an offset for the new topic is durably committed for it and that commit is confirmed against the new incarnation: the reset's own resolution while [`autoCommit`](#auto-commit) is on, a [`seek`](#seek) while `autoCommit` is on, an explicit [`commitOffsets`](#manual-commits), or the offsets of a confirmed transaction, the last three also when another client of the same `Kafka` instance makes them. Resolving or seeking locally does not settle it: a resolution or a seek with `autoCommit` off, a sentinel seek (`'-2'` or `'-1'`) boundary and a `by_duration` position that was not committed leave it owed, and so do a `ListOffsets` request that fails, a commit that fails, is stale or is unconfirmed, an answer that predates the recreation and the consumer's automatic restart after a crash. While a partition owes the reset, a `stop()` followed by `run()`, a crash restart or an assignment rebuild resolves it again from the configured boundary, even over a position you set locally with a seek; to keep a position across such a rebuild, commit it. `consumer.disconnect()` drops the debt. Once a confirmed commit settles a partition, the reset no longer applies to it, so a later seek or commit for the new topic is never undone by it. A seek issued *before* the consumer noticed the recreation is dropped, since its target offset belonged to the topic's previous incarnation. Each member of a group resets on its own the first time it notices the recreation, unless another consumer of the group in the same `Kafka` instance settled the partition first.

Offset operations on a partition are ordered by when they began, not by when their answers arrive. Of two seeks, two commits, or a commit and a transaction's offsets, the one that began later wins, even when its offset is lower and whichever answer arrives first; the older one's late answer changes nothing in this client. A resolution of committed offsets fills only a partition that is still unresolved and that no seek, commit or reset touched since it began. A revoke, an assignment rebuild and a recreation fence every operation of their partitions that began before them. A commit already sent cannot be recalled: when a newer operation wins, the coordinator may still have stored the older commit.

The clients created from one `Kafka` instance share, for each consumer group, the cache of committed offsets and the record of these operations; two `Kafka` instances share neither. A transaction's offsets reach that cache only after the transaction's commit is confirmed, once, and a newer seek, reset, commit or recreation of a partition fences them there (see [Sending Offsets](Transactions.md#offsets)). A group's shared entries are released when its last consumer, producer and open operation are done.

The Java client (Apache Kafka 4.3.1) resets only a partition's last-seen leader epoch when a topic's id changes. This client is stricter on purpose: it resets the topic's offsets and keeps that debt, across a stop/run cycle, a crash restart and an assignment rebuild, until an offset for the new topic is durably committed and confirmed, so it never resumes from the old topic's committed offset in the new topic. With `autoCommit` off, commit a position with `commitOffsets` (or the reset stays owed and a rebuild resolves it again).

## <a name="topic-metadata"></a> Topic metadata

The consumer routes fetches and offset requests from the metadata it caches for the topics it uses. When a broker's answer to a produce or fetch request names a new leader for a partition (a leader hint, KIP-951), only that partition's `leader` and `leaderEpoch` are updated in place. The other fields of the cached partition, `replicas`, `isr` and `offlineReplicas`, stay as the last metadata response said and may lag until the next metadata refresh. Produce and fetch routing read `leader` and `leaderEpoch`; the only other field it reads is `offlineReplicas`, when a consumer with `rackId` chooses between a preferred read replica and the leader. A read that needs the current replica set, such as `admin.fetchTopicMetadata()`, asks a broker directly and returns that answer as it is, without writing it into the cache.

## <a name="options"></a> Options

```javascript
kafka.consumer({
  groupId: <String>,
  partitionAssigners: <Array>,
  sessionTimeout: <Number>,
  maxPollInterval: <Number>,
  heartbeatInterval: <Number>,
  metadataMaxAge: <Number>,
  allowAutoTopicCreation: <Boolean>,
  maxBytesPerPartition: <Number>,
  minBytes: <Number>,
  maxBytes: <Number>,
  maxWaitTimeInMs: <Number>,
  checkCrcs: <Boolean>,
  maxDecompressedBatchBytes: <Number>,
  retry: <Object>,
  maxInFlightRequests: <Number>,
  rackId: <String>,
  groupInstanceId: <String>,
  groupProtocol: <String>,
  groupRemoteAssignor: <String>,
  maxPrefetchBatches: <Number>
})
```

| option                 | description                                                                                                                                                                                                                                                                                                                                        | default                           |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- |
| partitionAssigners     | List of partition assigners                                                                                                                                                                                                                                                                                                                        | `[PartitionAssigners.roundRobin]` |
| sessionTimeout | Classic failure-detection timeout in milliseconds. Integer from 1 to 2147483647; heartbeatInterval must be lower. The broker also enforces its configured group.min.session.timeout.ms and group.max.session.timeout.ms. Rejected bounds produce an actionable non-retriable error. Under groupProtocol: 'consumer', the broker controls the session and this option cannot be set. | `45000` |
| maxPollInterval        | The maximum time, in milliseconds, your `eachMessage`/`eachBatch` handler may take to return, and the rebalance timeout sent to the coordinator. It is the Java client's `max.poll.interval.ms`. A handler that takes longer loses its partitions and the consumer rejoins the group; see [Membership errors during rebalance](#membership-errors) | `300000` (5 minutes)              |
| rebalanceTimeout       | **Deprecated — use `maxPollInterval`.** The maximum time that the coordinator will wait for each member to rejoin when rebalancing the group. Still sent on the wire when it is the only one of the two you set, and setting it logs a deprecation warning once per consumer you create. It never becomes your handler deadline: that stays at `max(rebalanceTimeout, 300000)`, so an existing 20 s or 60 s rebalance timeout cannot turn into a 20 s or 60 s handler deadline. If you set both, `maxPollInterval` wins | `300000` (5 minutes, from `maxPollInterval`) |
| heartbeatInterval | Classic heartbeat interval in milliseconds. Positive integer lower than sessionTimeout. A value above a third of the session logs a warning. Under groupProtocol: 'consumer', the coordinator supplies the interval and this option cannot be set. | `3000` |
| metadataMaxAge         | The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions                                                                                                                                                       | `300000` (5 minutes)              |
| allowAutoTopicCreation | Allow topic creation for literal-only metadata requests; consumers with a client-side RegExp suppress it, including for mixed literal names                                                                                                                                                                                                                                                                                | `true`                            |
| maxBytesPerPartition   | The maximum amount of data per-partition the server will return. This size must be at least as large as the maximum message size the server allows or else it is possible for the producer to send messages larger than the consumer can fetch. If that happens, the consumer can get stuck trying to fetch a large message on a certain partition | `1048576` (1MB)                   |
| minBytes               | Minimum amount of data the server should return for a fetch request, otherwise wait up to `maxWaitTimeInMs` for more data to accumulate.                                                                                                                                                                                                           | `1`                               |
| maxBytes | Fetch response target in bytes, not a heap ceiling. An oversize first batch, decompression and concurrent per-broker fetches can exceed it. | `10485760` (10 MiB) |
| maxPrefetchBatches     | Accepted with a value of `0` or `1`, and **inert in this release**: the fetch loop behaves identically at either value. Reserved for a later release, which may use it to let a fetcher keep one fetch on the wire while the previous response is still being handled. Any other value is rejected when you create the consumer. See [Prefetching](#prefetching) | `0` |
| maxWaitTimeInMs        | The maximum amount of time in milliseconds the server will block before answering the fetch request if there isn’t sufficient data to immediately satisfy the requirement given by `minBytes`                                                                                                                                                      | `5000`                            |
| fetchSessions | Enable incremental Fetch sessions (KIP-227); `false` keeps full requests. The consumer falls back to full requests when the broker cannot allocate a session. | `false` |
| checkCrcs              | Verify the CRC32C checksum of every record batch received. Default `true` (Java's `check.crcs`). A mismatch halts the partition like any corrupt batch (see [Decompression bound](#decompression-bound)): the records before the batch are delivered, `consumer.partition_halted` fires with reason `checksum` and no `nextOffset`, the other partitions keep delivering, and the partition is not fetched again until `consumer.seek`. Set to `false` to skip the check and restore the pre-3.0 behaviour, where a corrupt batch was delivered to the application as if it were valid. | `true` |
| maxDecompressedBatchBytes | The most one record batch may decompress to, in bytes, for every codec; also sets a ceiling of `max(1, floor(bytes / 512))` records per batch. An integer from 1 to 2147483647. A batch past either halts the partition. Not a limit on the memory of the process: see [Decompression bound](#decompression-bound) | `1073741824` _(1 GiB)_ |
| retry                  | See [retry](Configuration.md#retry) for more information                                                                                                                                                                                                                                                                                           | `{ retries: 5 }`                 |
| readUncommitted | Configures transactional isolation. The default false selects read_committed and filters aborted transactional records. Set true for read_uncommitted. Committed reads alone do not provide end-to-end exactly-once processing. | `false` |
| maxInFlightRequests | Maximum requests in progress per connection. `null` means unbounded; `0` is rejected. | `5` |
| rackId                 | Configure the "rack" in which the consumer resides to enable [follower fetching](#follower-fetching)                 | `null` _(fetch from the leader always)_ |
| `rackAwareAssignment` | Match replica racks in the local `range` assigner, within range quotas. Other local assigners ignore it; consumer-protocol assignment is server-side. Requires a boolean. | `false` |
| groupInstanceId        | Static membership id (KIP-345). See [Static membership](#static-membership)                                          | `null` _(dynamic membership)_ |
| groupProtocol          | The group membership protocol: `'classic'` or `'consumer'` (KIP-848, Kafka 4.0+). See [Consumer group protocols](#group-protocols) | `'classic'` |
| groupRemoteAssignor    | The server-side assignor to ask the coordinator for, under `groupProtocol: 'consumer'` only. `null` lets the coordinator pick its first configured assignor | `null` _(coordinator's choice)_ |

`allowAutoTopicCreation` defaults to `true` for literal-only consumers. A consumer with a client-side `RegExp` refreshes all topics, suppresses automatic creation on every naming route, and does not auto-create a literal subscribed beside the pattern. This keeps a deleted matched topic absent. The existing `metadataMaxAge` controls refresh eligibility at processing opportunities; blocked handlers can delay discovery. A narrower pattern reduces matching and assignment work, not the size of the all-topics metadata response. Applied configuration, pause and seek state retire only after the matching revision applies. Initial discovery uses ordinary committed offsets; a later addition without known incarnation continuity starts from its reset policy, including a name first discovered later.

The classic session default is 45 seconds. A member that stops heartbeating can therefore be detected up to 15 seconds later than with the previous 30-second default, and a static member can restart within the longer session window. Set `sessionTimeout: 30000` to keep the previous value. `sessionTimeout` and `heartbeatInterval` reject non-integers, zero, negative values, NaN, Infinity and strings; the session also rejects values above 2147483647. `maxPollInterval` and its deprecated rebalanceTimeout alias retain their existing semantics. Under `groupProtocol: 'consumer'`, session and heartbeat settings remain broker-controlled, and explicitly supplying either classic option is rejected.

The kept Fetch wait is `maxWaitTimeInMs: 5000`, and the kept response target is `maxBytes: 10485760`. These values are configuration defaults, not memory measurements. Response sizing measurements for large assignments are reported with their workload and denominators when available.

### <a name="large-assignment-memory"></a> Memory with large assignments

`maxBytes` remains 10485760 (10 MiB). It is a response target, not a heap ceiling. An oversized first batch, decompression and concurrent Fetch requests to brokers can exceed it. Measurements for a consumer with 1000 populated partitions and response targets of 10, 25 and 50 MiB are pending on the release benchmark runners. No measured memory sizing recommendation is available yet. Size the heap for concurrent responses, decompressed batches and application-held records, and measure the application's actual workload before raising the response target.

## <a name="static-membership"></a> Static membership

By default, a consumer is a **dynamic member** of its group: every time it disconnects (even briefly, e.g. a deploy), it leaves the group and the broker immediately rebalances the remaining members. Setting `groupInstanceId` makes the consumer a **static member** (KIP-345) instead — it keeps its identity and assigned partitions across a restart, as long as it reconnects within `sessionTimeout`.

```javascript
const consumer = kafka.consumer({ groupId: 'my-group', groupInstanceId: 'my-group-consumer-1' })
```

`groupInstanceId` must be unique within the group — think of it as a stable identifier for one deployed instance (one per pod/host), not a value shared across replicas.

Static membership changes two behaviors:

- **`consumer.disconnect()` does not send LeaveGroup.** A static member keeps its slot in the group; the broker only reassigns its partitions once `sessionTimeout` elapses without a heartbeat. This means a rolling restart no longer triggers a rebalance for every replica, at the cost of the group holding those partitions idle until `sessionTimeout` elapses — unless the new instance rejoins under the same `groupInstanceId` first, which takes over immediately (see below). To make a static member leave at once anyway, pass `{ leaveGroup: true }` to `disconnect()` (see [Leaving the group on disconnect](#leaving-the-group)); to bound the delay otherwise, lower `sessionTimeout` for that consumer, or stop passing `groupInstanceId`.
- **A duplicate `groupInstanceId` fences the *existing* member, not the new one.** If a second consumer connects with a `groupInstanceId` already held by a live member, the broker immediately admits the new joiner — rebinding it to the existing assignment, no rebalance — and fences the *incumbent* on its next heartbeat with `FENCED_INSTANCE_ID` (error code 82). In practice: a restarted instance takes over right away, and it is the **old process** that crashes. That old consumer does **not** restart (`CRASH` event payload has `restart: false`, regardless of any `retry.restartOnFailure` callback) — restarting would immediately re-fence into the same loop, since another live member already holds that `groupInstanceId`. This is the expected, healthy path during a rolling restart that briefly runs two instances under the same id; outside of that, it means two processes were misconfigured with the same `groupInstanceId` and one of them needs a different one.

Requires the broker to support JoinGroup v5 / SyncGroup v3 / Heartbeat v3 / LeaveGroup v3 (Kafka 2.4+); this fork's floor is Kafka 4.0+, so this is always available. If a broker somehow negotiates an older JoinGroup version, a one-time warning is logged and static membership is silently inactive (the consumer behaves as a dynamic member).

## <a name="leaving-the-group"></a> Leaving the group on disconnect

By default `consumer.disconnect()` decides for itself how the member leaves its group: a dynamic member leaves; a static member (`groupInstanceId`) keeps its slot, by sending nothing under `'classic'` and the leave-for-now heartbeat (epoch -2) under `'consumer'`. Pass `leaveGroup` to override that for one disconnect (KIP-1092):

```javascript
await consumer.disconnect({ leaveGroup: true }) // hand the partitions over now, even a static member
await consumer.disconnect({ leaveGroup: false }) // do not tell the group, even a dynamic member
```

| `leaveGroup`      | `'classic'`, dynamic | `'classic'`, static                                          | `'consumer'`, dynamic | `'consumer'`, static                                 |
| ----------------- | -------------------- | ------------------------------------------------------------ | --------------------- | ---------------------------------------------------- |
| not set (default) | LeaveGroup           | nothing is sent                                              | leaves (epoch -1)     | leaves for now (epoch -2), the member keeps its slot |
| `true`            | LeaveGroup           | **LeaveGroup**, with the member id and the `groupInstanceId` | leaves (epoch -1)     | **leaves for good (epoch -1)**, the slot is released |
| `false`           | **nothing is sent**  | nothing is sent                                              | **nothing is sent**   | leaves for now (epoch -2), the member keeps its slot |

- **`true` hands the partitions over at once.** The coordinator rebalances as soon as it learns the member is gone. A static member that leaves this way gives up its slot: a restart under the same `groupInstanceId` joins as a new member, so the group rebalances again.
- **`false` is the consequence to know about.** A member that does not tell the group it is leaving is still a member: the group keeps assigning it its partitions, and nobody consumes them, until its session expires (`sessionTimeout` under the classic protocol, the broker's `group.consumer.session.timeout.ms` under the consumer protocol), and the group then rebalances. It suits a member that is about to come back as the same member, which is what a static member is; for a dynamic member it only moves the rebalance to the end of the session. For a static member `false` is what the default already does.
- Graceful stopping waits for eligible running work and the required close callbacks, then attempts eligible final automatic progress before leaving. Classic consumers now have this final attempt; consumer-protocol leave retains its existing one. leaveGroup controls the membership leave, independently of an eligible offset commit. Ownership loss, disabled automatic commits, no group identity, halt bounds or an exhausted timeout can suppress the attempt. Under the consumer protocol the consumer still commits what it owns and emits `PARTITIONS_REVOKED`, whatever `leaveGroup` says.
- Stop ends the handlers' API authority at once, before it waits for them. A handler's explicit `commitOffsets()` that is still waiting at that point (for example for the group coordinator) may reject with `CONSUMER_STOPPING`, and there is no promise that it completes. Offsets committed earlier stay committed. A commit from outside the handlers, and one made by an authorized close hook, may still complete. If a commit must finish, drain your work and await the commit before you call stop or disconnect.
- After stop is requested, a handler's `heartbeat()` callback resolves without sending; if it needs heartbeats to keep its membership while draining, finish the handler before calling stop or disconnect.
- The first concurrent disconnect controls the entire close, including timeout and leaveGroup. A later call does not renew or shorten it. stop takes no timeout option. After a completed stop there is no membership left for a later disconnect to leave again. A consumer that crashes and restarts always uses the default.
- A nonobject disconnect argument is ignored, preserving signal-handler compatibility. An object with an invalid timeout rejects with KafkaJSInvalidConfigurationError (invalidConfiguration); invalid leaveGroup retains KafkaJSNonRetriableError. Both reject before stopping. timeout is an integer from 0 to 2147483647 milliseconds; omission keeps natural waiting and zero starts no final commit or leave. `disconnect(true)` does not mean "leave", and `process.on('SIGTERM', consumer.disconnect)` still works.

await consumer.disconnect({timeout:3000,leaveGroup:true}) bounds cleanup for the run captured at entry. At the boundary, client-owned timers, waits and queued unsent work are retired, and old continuations cannot change a replacement run. Application promises are detached if still pending. Operational cleanup of a valid call resolves; invalid options reject before state changes. A commit already transmitted before the boundary can still apply remotely, so read broker offsets before deciding its outcome.

`connect()` or `run()` called while an earlier `disconnect()` is still running waits for that close to finish (a forced `timeout` ends the wait at its deadline), so a late close never disconnects the new connection. Calls made from a handler or rebalance callback of the run being closed do not wait.

A classic static member that leaves with `true` sends its member id and its `groupInstanceId`, so the coordinator checks the pair and answers `FENCED_INSTANCE_ID` for a member id that is no longer the instance's current one instead of removing someone else's membership. The Java client sends the member id alone.

## <a name="pause-resume"></a> Pause & Resume

In order to pause and resume consuming from one or more topics, the `Consumer` provides the methods `pause` and `resume`. It also provides the `paused` method to get the list of all paused topics. Note that pausing a topic means that it won't be fetched in the next cycle and subsequent messages within the current batch won't be passed to an `eachMessage` handler.

A pause for a topic outside the subscription is retained until the next subscription lifecycle point. paused() lists only assigned partitions. Pause state survives ordinary eager, cooperative and consumer-protocol rebalances, and is retired when the topic leaves the subscription, the run ends or the topic is recreated. Calling `resume` with a topic that is not paused is also a no-op.

> Note: Calling `resume` or `pause` while the consumer is not running will throw an error.

resume(), seek(), an applied assignment or routing change, a topic reset and stop wake idle fetchers immediately. maxWaitTimeInMs still defaults to 5000 and continues to bound broker long polling; waking an idle fetcher does not cancel an in-flight Fetch. Admin offset operations release idle local group state after their last overlapping operation, while other holders and pending ledger work continue to protect shared state. A routing change that arrives while a fetcher is preparing its positions is picked up by one retry in the same round; during continuous routing churn a round prepares at most twice, and an idle round whose routing changed returns to the fetch loop instead of sleeping.

Example: A situation where this could be useful is when an external dependency used by the consumer is under too much load. Here we want to `pause` consumption from a topic when this happens, and after a predefined interval we `resume` again:

```javascript
await consumer.connect()
await consumer.subscribe({ topics: ['jobs'] })

await consumer.run({ eachMessage: async ({ topic, message }) => {
    try {
        await sendToDependency(message)
    } catch (e) {
        if (e instanceof TooManyRequestsError) {
            consumer.pause([{ topic }])
            setTimeout(() => consumer.resume([{ topic }]), e.retryAfter * 1000)
        }

        throw e
    }
}})
```

For finer-grained control, specific partitions of topics can also be paused, rather than the whole topic. The ability to pause and resume on a per-partition basis, means it can be used to isolate the consuming (and processing) of messages.

Example: in combination with [consuming messages per partition concurrently](#concurrent-processing), it can prevent having to stop processing all partitions because of a slow process in one of the other partitions.

```javascript
consumer.run({
    partitionsConsumedConcurrently: 3, // Default: 1
    eachMessage: async ({ topic, partition, message }) => {
      // This will be called up to 3 times concurrently
        try {
            await sendToDependency(message)
        } catch (e) {
            if (e instanceof TooManyRequestsError) {
                consumer.pause([{ topic, partitions: [partition] }])
                // Other partitions will keep fetching and processing, until if / when
                // they also get throttled
                setTimeout(() => {
                    consumer.resume([{ topic, partitions: [partition] }])
                    // Other partitions that are paused will continue to be paused
                }, e.retryAfter * 1000)
            }

            throw e
        }
    },
})
```

As a convenience, the `eachMessage` callback provides a `pause` function to pause the specific topic-partition of the message currently being processed.

```javascript
await consumer.connect()
await consumer.subscribe({ topics: ['jobs'] })

await consumer.run({ eachMessage: async ({ topic, message, pause }) => {
    try {
        await sendToDependency(message)
    } catch (e) {
        if (e instanceof TooManyRequestsError) {
            const resumeThisPartition = pause()
            // Other partitions that are paused will continue to be paused
            setTimeout(resumeThisPartition, e.retryAfter * 1000)
        }

        throw e
    }
}})
```

It's possible to access the list of paused topic partitions using the `paused` method.

```javascript
const pausedTopicPartitions = consumer.paused()

for (const topicPartitions of pausedTopicPartitions) {
  const { topic, partitions } = topicPartitions
  console.log({ topic, partitions })
}
```

## <a name="manual-assignment"></a> Manual assignment

Use `await consumer.assign([{ topic, partitions }], options?)` before `run()` to select partitions yourself. A later call replaces the earlier selection. Topics must be nonempty strings and partition lists must be nonempty, contain unique integers from 0 to 2147483647, and exist in refreshed metadata. An invalid replacement leaves the earlier selection usable. Call `stop()` before replacing a running assignment. `assign([])` rejects; use `await consumer.unsubscribe()` to clear it.

Manual assignment sends no group membership requests and starts no heartbeat or poll-membership timer. With a `groupId`, offset reads and commits use simple-consumer identity: generation -1, empty member ID and no instance ID. A broker refusal is returned without joining a group. Other members using that group may cause a simple offset commit to be refused. A lost fetch or commit connection takes the ordinary crash path: `retry.restartOnFailure` decides the restart, and the default restarts on a retriable error.

A consumer that only assigns partitions may omit `groupId`:

```js
const consumer = kafka.consumer({})
await consumer.connect()
await consumer.assign([{ topic: 'events', partitions: [0] }], { offsetReset: 'earliest' })
consumer.seek({ topic: 'events', partition: 0, offset: '12' })
await consumer.run({ eachMessage: async ({ message }) => processEvent(message) })
```

Without a group, automatic commits default to false and explicit `autoCommit: true` rejects. No coordinator, OffsetFetch or OffsetCommit is used, and local offsets are private to this consumer. `subscribe`, `commitOffsets`, `committed`, `describeGroup`, `groupMetadata`, a rebalance listener and `enforceRebalance` require a group and reject with `Consumer groupId must be a non-empty string.` Only omitted or undefined group IDs enable this mode; null, empty and non-string IDs reject at construction.

Subscription and manual assignment are exclusive until `await consumer.unsubscribe()`. Unsubscribe stops a running consumer, clears subscription and manual selection, and keeps its connection. You may then choose either mode again. Stop alone preserves the chosen mode. A manual seek queued before run applies once at the first start and is not replayed after a crash restart. The assignment options use the same `fromBeginning` and `offsetReset` rules as subscribe, including conflicts between explicitly supplied forms.

## <a name="consumer-getters"></a> Assignment, subscription and offsets

`consumer.assignment()` returns a sorted copy of the acquired topic-partitions. It is empty before the first assignment and after stop; a manual consumer acquires its selected partitions when run starts. `consumer.subscription()` returns a sorted copy of subscribed topic names, including current client-regex matches, before or during run and after stop. It is empty for a manual consumer. A broker-regex expression is not a topic name. Changing either returned array does not change the consumer.

`await consumer.position({ topic, partition })` applies pending seek/reset work and returns the next processing or fetch position as a decimal string. It requires a currently acquired partition. A resolved handler offset advances this position by one; a sparse or control offset need not identify a deliverable record. A seek moves position within the same acquisition; a revoke/reacquire, stop, rebuild or topic recreation retires an in-flight position read.

`await consumer.committed([{ topic, partition }])` reads stable committed offsets from the broker for this group, including unassigned partitions. It returns `{ topic, partition, offset, metadata }` rows; offset is a decimal string or null when no offset was committed, and metadata is null whenever offset is null; otherwise it preserves the stored string (including the empty string) or null. An empty array returns an empty array without a request. This observation is independent of local position and does not move it. OffsetFetch v8/v9 responses naming another group are refused with `KafkaJSInvariantViolation` naming the requested and returned groups.

If retry.maxTotalTime is set, commitOffsets, committed, position and describeGroup use one deadline captured at public entry. It covers coordinator lookup, connect/authentication, encoding, queueing, response, retry and seek/reset resolution for that call. Expiry raises KafkaJSDeliveryTimeout and refuses late local effects and unsent writes. Unset retains the prior waiting behavior. position remains the next applicable processing/fetch position; committed is a live RequireStable group read. An API timeout does not establish whether an already-transmitted commit applied at the broker.

## <a name="rebalance-listeners"></a> Awaited rebalance listeners

Pass optional `onPartitionsRevoked`, `onPartitionsAssigned` and `onPartitionsLost` functions in `consumer.run({ rebalanceListener })`. Each receives `{ groupId, memberId, generationId, partitions }` and may return a promise. The listener belongs to the run: crash restart retains it; stop followed by run may replace it. A manual consumer rejects listeners.

On an ordinary eager round, revoked runs before joining, including the initial empty assignment. On cooperative and consumer-protocol rounds, admission stops only for revoked partitions; when a revoked callback is registered, their running handlers finish before the eligible final commit and revoked callback. Retained partitions continue. Assignment is visible inside assigned, while added partitions remain held from Fetch and handler admission until that callback completes. Assigned runs for every applied round, including an empty delta. Internal holds are separate from `paused()` and do not invalidate valid running handlers.

Loss fences immediately, without waiting for handlers or committing; a nonempty lost set invokes lost once. If lost is absent, revoked receives that loss instead. Supply an explicit lost callback to avoid running normal offset-saving logic after loss. A stable close calls revoked before its eligible final commit and leave; a lost or in-progress round uses lost. Close logs callback failures and continues cleanup.

Callbacks share the remaining rebalance deadline; each callback does not receive a new timeout. An ordinary exception records the first `KafkaJSRebalanceListenerError`, completes only the still-live transitions, then reaches CRASH and `restartOnFailure` without retrying the callback. Error throws retain the original cause; other throws retain their exact `thrownValue` and an Error cause. Expiry terminates and fences the round rather than publishing an assignment. External user side effects cannot be cancelled.

Within assigned, await `committed()` to read broker offsets and metadata, then use `seek()` and await `position()` to load an external starting offset before delivery. Within revoked, `position()` reads quiescent next progress. Position is not proof of a durable broker commit. Hook seeks and local resolution do not settle a recreation reset; only the confirmed current-incarnation durable commit does.

Client calls from a settled, expired or retired callback are refused. Valid callbacks can request stop, disconnect or unsubscribe without waiting on their own round; the returned promise settles on the next microtask and does not wait for cleanup completion; an outside caller of the same API waits for the whole shared close. Calling run or subscribe from a hook is rejected. User promises that continue later keep their original authority and cannot mutate a replacement run.

`await consumer.enforceRebalance(reason?)` requests a classic revoked rejoin and resolves when accepted. The default reason is `enforced-rebalance`; an explicit request while a round runs is a no-op. Manual and consumer-protocol consumers reject it. Custom assigner protocol metadata is rebuilt for the new join.

Instrumentation events keep their existing timing and payloads and are not awaited callbacks. A group consumer's pre-run seek remains an error; an awaited assigned callback plus seek provides the equivalent. `currentLag` remains available through batch/admin lag recipes, and `max.poll.records` is not an option for push delivery; use byte bounds and eachMessage for one-record handler units.

## <a name="seek"></a> Seek

To move the offset position in a topic/partition the `Consumer` provides the method `seek`. This method has to be called after the consumer is initialized and is running (after consumer#run).

```javascript
await consumer.connect()
await consumer.subscribe({ topics: ['example'] })

// you don't need to await consumer#run
consumer.run({ eachMessage: async ({ topic, message }) => true })
consumer.seek({ topic: 'example', partition: 0, offset: 12384 })
```

Upon seeking to an offset, any messages in active batches are marked as stale and discarded, making sure the next message read for the partition is from the offset sought to. Make sure to check `isStale()` before processing a message using [the `eachBatch` interface](#each-batch) of `consumer.run`.

For a concrete offset, the consumer commits the sought offset by default; set [`autoCommit`](#auto-commit) to `false` to disable that commit. A sentinel seek to `'-2'` (earliest) or `'-1'` (latest) instead requests that partition's exact boundary through ListOffsets on the next resolution, independently of `fromBeginning` and `autoCommit`. The following fetch starts at the returned concrete offset. A sentinel seek never commits the negative sentinel. Pending sentinel intent is discarded if ownership is lost or the consumer stops. If the consumer stops before a sentinel's boundary arrives, a pending topic-recreation reset stays owed and the next run applies it.

```javascript
consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, message }) => true
})
// This will now only resolve the previous offset, not commit it
consumer.seek({ topic: 'example', partition: 0, offset: "12384" })
```

After a manual assignment, a seek before run is queued once for the first start. A grouped consumer still requires run before seek; an awaited assigned hook is the group equivalent.

## <a name="halted-partitions"></a> Halted partitions

When a fetched record batch is corrupt or past a decoding bound (a length below the minimum or negative, a magic byte outside 0 to 2, an impossible record count or one above the derived ceiling, a malformed control record, a CRC32C mismatch, or a compressed body that is malformed or past `maxDecompressedBatchBytes`), the consumer delivers the records before it and then **halts that partition**: it is no longer fetched, the other partitions and brokers keep delivering, and `consumer.partition_halted` fires once with `{ groupId, memberId, topic, partition, offset, reason, error, nextOffset?, codec?, limit? }`, where `offset` is the corrupt batch's first offset and `error` is the `KafkaJSCorruptRecordError`. One error is logged with it. Offsets the consumer derives itself (resolved offsets, automatic commits) never move past the batch, because it is never delivered; an offset you commit yourself keeps its ordinary meaning.

Configured commit cadence and graceful close keep the same halt boundary. Eligible resolved records before a corrupt or unreadable batch can be committed automatically, including by the final close attempt; automatic progress does not cross the unreadable batch. A no-offset halt adds no derived position. Caller-supplied explicit offsets retain their existing ownership and halt rules. A failed or unconfirmed final commit does not by itself discharge a recreation reset obligation.

Nothing skips the batch for you. To go on, `consumer.seek` the partition: past the batch to skip it, or at or before it to read it again (it halts again, and the event fires again, if the batch is still corrupt). The event fires when the batch is fetched, before the records ahead of it are processed. A seek made inside the `consumer.partition_halted` listener is applied at the start of the next fetch round, and like any seek it also drops those records if they are not yet processed (`eachMessage` skips them, `eachBatch` gets them with `isStale()` true); to keep them, seek once they have been processed. A rebalance that revokes the partition, a recreation of its topic and a restart also clear the halt; a recreated topic never inherits the halt of its previous incarnation. `pause()` and `resume()` do not clear it. An answer fetched before a seek, a reset or a recreation reached the partition never halts it.

A CRC32C mismatch also halts its partition, with reason `checksum` and no `nextOffset`; the other partitions keep delivering (see `checkCrcs` in [Options](#options)).

## <a name="partition-assigners"></a> Built-in partition assigners

`PartitionAssigners` exports four assigners. The default is unchanged: a consumer you create without `partitionAssigners` uses `[PartitionAssigners.roundRobin]`, exactly as before.

The Java client's default list is `[Range, CooperativeSticky]`. Range is eager, so that list does not enable cooperative operation. This client keeps `[PartitionAssigners.roundRobin]`. To enable cooperative operation, use the two deployments in [Cooperative rebalancing, and how to roll it out](#cooperative-rebalancing): first teach every member the cooperative assigner while keeping the eager assigner, then remove the eager assigner only after the first deployment is complete.

| Assigner | Name on the wire | What it does |
| --- | --- | --- |
| `roundRobin` | `RoundRobinAssigner` | The default. Deals every partition of every subscribed topic out to the members in turn. No Java client understands this name, so a group using it is a JavaScript-only group. |
| `range` | `range` | Kafka's `RangeAssignor`: per topic, the partitions are split into contiguous ranges, one per member. Members subscribed to the same few topics tend to get the same partition numbers of each. |
| `sticky` | `sticky` | Kafka's `StickyAssignor`: balances partitions while moving as few of them as possible from their current owners. |
| `cooperativeSticky` | `cooperative-sticky` | Kafka's `CooperativeStickyAssignor` (KIP-429): the same balancing as `sticky`, plus incremental rebalancing — a member keeps the partitions it is not losing instead of giving up everything at the start of each round. |

Every group member receives an encoded assignment, including members that receive no partitions. An assignment containing zero partitions is valid; a missing or zero-length assignment payload is different. The round-robin assigner's protocol name and subscription version remain unchanged. Assignment codecs preserve an explicitly supplied null userdata value and keep the existing empty userdata default when it is omitted.

`range`, `sticky` and `cooperativeSticky` use the same names and the same algorithms as the Java client, so a group can mix this client with Java consumers. Set `rackAwareAssignment: true` to enable rack matching in the `range` assigner when this member is the group leader. It defaults to `false`. Members need `rackId`, and brokers need rack labels in Metadata. Range matches replica racks within its partition quotas and preserves co-partitioning; balance wins when alignment cannot satisfy the quotas. Missing or nonintersecting racks leave partition ownership unchanged. `sticky` and `cooperativeSticky` keep their current rack-blind assignment; `roundRobin` ignores the flag too. A custom assigner receives the additive `rackAware` factory argument and may ignore it. With the flag off, a Java range leader can still match racks while a JavaScript range leader uses plain range, so a mixed group's assignment can differ with its leader. Consumer-protocol groups use the broker's server-side assignor.

When members subscribe to different topics, none of the four gives a member a partition of a topic it does not subscribe to: `roundRobin` skips such members as Java's `RoundRobinAssignor` does, and `sticky` and `cooperativeSticky` never hand a partition back to a previous owner that has left its topic.

```javascript
const { PartitionAssigners: { sticky } } = require('@countly/kafkajs')

kafka.consumer({ groupId: 'my-group', partitionAssigners: [sticky] })
```

### <a name="cooperative-rebalancing"></a> Cooperative rebalancing, and how to roll it out

With the default (eager) rebalancing, every member gives up all of its partitions at the start of a rebalance and gets a new set at the end, so the whole group stops consuming for the length of the round. With cooperative rebalancing a member only gives up the partitions it is actually losing, and keeps consuming the rest throughout.

The protocol is chosen once, when you create the consumer: it is the strongest protocol *every* assigner in your list supports. `cooperativeSticky` supports both, and the other three support only eager — so a list containing any of them is an eager list. That is what makes a safe rollout possible, and it is why turning cooperative rebalancing on takes **two deploys**:

```javascript
const { PartitionAssigners: { roundRobin, cooperativeSticky } } = require('@countly/kafkajs')

// Deploy 1 — every member learns the new assigner. Still eager.
kafka.consumer({ groupId: 'my-group', partitionAssigners: [roundRobin, cooperativeSticky] })

// Deploy 2 — only once every member is running deploy 1. Now cooperative.
kafka.consumer({ groupId: 'my-group', partitionAssigners: [cooperativeSticky] })
```

Every consumer logs one INFO line when you create it, naming the rebalance protocol it selected and the assigners it selected it from, so you can see which of the two deploys a running process is on.

Going straight to deploy 2 splits the group: members still running the old code and members running the new one share no assigner name, and the coordinator rejects the join with `INCONSISTENT_GROUP_PROTOCOL` until the rollout finishes. During deploy 2 the group is briefly mixed — some members computing eagerly, some cooperatively — which is safe, because an eager member's JoinGroup advertises no owned partitions at all — it gives everything up before it rejoins — and the cooperative algorithm never hands a partition to a member that still owns it.

Classic assignment runs on the group leader. For very large assignments, `groupProtocol: 'consumer'` moves assignment to the broker. The retained large sticky-assignment workload and its measured limits are reported with the performance evidence; an unmeasured optimization is not a sizing guarantee.

## <a name="group-protocols"></a> Consumer group protocols

A consumer group can run on either of two membership protocols. You choose with `groupProtocol`, and the default is unchanged.

| | `'classic'` (default) | `'consumer'` (KIP-848) |
| --- | --- | --- |
| Who computes the assignment | one member, elected leader | the group coordinator |
| How a member learns its assignment | JoinGroup + SyncGroup round trip | its own heartbeats |
| What a rebalance costs the group | every member waits for the round (the whole assignment with an eager assigner, the part it loses with a cooperative one) | nothing group-wide: each member reconciles its own share while the others keep consuming |
| Assigners | your `partitionAssigners` run in the client | the broker's, named by `groupRemoteAssignor` |
| Broker requirement | any supported broker | Kafka 4.0 or newer |

Under `'consumer'` there is no group-wide barrier: the coordinator hands each member a target assignment on a heartbeat response, and the member reconciles towards it one step at a time — giving up what it no longer owns, then acknowledging what it took on. A member that is slow to reconcile delays nobody but itself.

### Turning it on

```javascript
const consumer = kafka.consumer({ groupId: 'my-group', groupProtocol: 'consumer' })
```

### What the broker owns

Under `'consumer'` the coordinator owns the session timeout, the heartbeat interval and the assignment itself. Passing an option that would silently be ignored is an error rather than a surprise, so creating the consumer throws a `KafkaJSNonRetriableError`:

| Configuration | Message |
| --- | --- |
| `sessionTimeout`, `heartbeatInterval` or `partitionAssigners` with `groupProtocol: 'consumer'` | `partitionAssigners, heartbeatInterval, sessionTimeout cannot be set when groupProtocol=consumer` (only the options you actually set are named, in that order) |
| `groupRemoteAssignor` with `groupProtocol: 'classic'` | `groupRemoteAssignor cannot be set when groupProtocol=classic` |
| Any other `groupProtocol` value | `groupProtocol must be one of "classic" or "consumer"` |
| `groupRemoteAssignor` set to something other than a non-empty string | `Argument "groupRemoteAssignor" must be a non-empty string` |

`rebalanceTimeout` and `maxPollInterval` are legal under both protocols and keep their meaning: `maxPollInterval` is your handler deadline, and the rebalance timeout is what the member tells the coordinator it needs to give partitions up.

### Choosing a server-side assignor

```javascript
kafka.consumer({ groupId: 'my-group', groupProtocol: 'consumer', groupRemoteAssignor: 'uniform' })
```

A stock 4.x broker offers `uniform` and `range`. Leave `groupRemoteAssignor` unset (`null`) to let the coordinator pick its first configured assignor. A name the cluster does not offer is a fatal membership error: the consumer crashes and does not restart, because every rejoin would be answered the same way.

A consumer-protocol target can contain a topic ID before its name is available. The client makes one immediate naming attempt when the known subset converges, then retries at metadata refresh opportunities after the fast attempt budget; it does not abandon the ID permanently. Responses from an old target or membership cannot update metadata for a replacement. A leave heartbeat sends member identity with the protocol's no-update defaults for subscription, assignment, rack and assignor fields. The next join sends full state. Connection and coordinator outages are retried without spending the fetch loop's retry budget until recovery, learned membership loss, poll expiry or an explicit stop; the classic session timeout is not a consumer-protocol expiry timer.

### Regex subscriptions

Under `'consumer'` you can hand the coordinator a pattern instead of topic names:

```javascript
await consumer.subscribe({ regex: '^orders-.*', fromBeginning: true })
```

- The pattern is **RE2, not JavaScript**. It is sent verbatim and evaluated by the broker, so what matches is decided by Kafka's dialect, not by `RegExp`. A pattern the broker rejects is a fatal membership error.
- The coordinator holds the pattern and re-evaluates it as topics come and go, so a topic created *after* you subscribe joins the subscription on its own — no client-side metadata poll, no resubscribe.
- One consumer subscribes to names or to a regex, never both: mixing them throws `Subscribe to topics or to a regex, not both`. Under `'classic'` the form itself is rejected, with `A regex subscription requires groupProtocol: 'consumer'`.
- A JavaScript `RegExp` inside `topics` is matched client-side under both protocols and re-evaluated at existing metadata refresh opportunities, not once at `subscribe()` time. Its clones ignore `g` and `y`, and internal topics require an explicit literal name.
- Under `groupProtocol: 'consumer'`, `subscribe({ topics: ['orders'] })` succeeds while `orders` does not exist and automatic topic creation is off; the topic is picked up soon after it is created. Under `'classic'` the same call rejects with `UNKNOWN_TOPIC_OR_PARTITION`.
- Resolved broker-regex names follow the coordinator's current target after it applies. A name no longer in that target is retired unless it is retained by an explicit literal subscription; old applied state remains available through final handlers, eligible commits and revoke work. The client does not interpret the broker's RE2 pattern or add a client-side discovery timer.

### Fencing and loss

Under `'consumer'` the two ways a member can be separated from its partitions stay clearly apart:

- **Fenced, or the poll deadline expired.** The coordinator rejected this member's epoch or id, or your handler took longer than `maxPollInterval`. The member emits [`PARTITIONS_LOST`](InstrumentationEvents.md#consumer) for its **whole** owned set and commits nothing for it: the epoch those offsets were resolved under is gone, so a commit would be rejected anyway. It then rejoins from scratch.
- **Reconciled away.** The coordinator moved some partitions to another member. With `autoCommit: true` the offsets your handlers resolved on those partitions are committed **first**, and then [`PARTITIONS_REVOKED`](InstrumentationEvents.md#consumer) reports them. The partitions you keep are never interrupted.

From the moment a member learns it was fenced, or that its poll deadline expired, it delivers no further record: `eachMessage` stops before the next message and batches already fetched are dropped. An `eachBatch` handler that is already running keeps its batch, but its commits are refused. Whoever owns the partition next, this member included after it rejoins, gets those records again from the last commit. A heartbeat that fails is retried with the client's `retry` backoff, capped at 1 000 ms after jitter as in the Java client. This cap applies to the heartbeat backoff delay; coordinator lookup still uses the cluster's retry policy, and lookup and request duration can make the time between heartbeat requests longer.

A poll-interval overrun is detected on a heartbeat tick and never while a reconciliation is in flight, so under `'consumer'` detection is bounded by the heartbeat interval the coordinator asked for rather than by one you configured.

### Migrating in either direction

`groupProtocol` is a per-consumer option, so a group can be moved one deployment at a time. See the [migration guide](MigrationGuide-3-0-0.md) for what changes for callers in each direction, including the behaviour changes that also apply to classic consumers.

### Limitations

- **No client-side assigners.** The broker assigns; `partitionAssigners` is rejected, and a custom assigner has no equivalent under this protocol.
- **Rack-aware assignment is the broker's.** Your `rackId` is sent to the coordinator, but rack-aware assignment is still work in progress upstream.
- **Consumer groups only.** `groupProtocol` does not address share groups or streams groups.

## <a name="fetch-sessions"></a> Fetch sessions

`fetchSessions` is a boolean consumer option, default `false`. Set `fetchSessions: true` to enable incremental Fetch sessions (KIP-227). A consumer then sends each broker added or changed partitions and removes partitions it no longer wants; the broker may omit unchanged partitions from replies. Setting `fetchSessions: false` keeps full requests. Session availability does not change delivered records or offset semantics. If the broker's session cache is full, the consumer processes the full response and retries session creation through subsequent full requests. The broker option `max.incremental.fetch.session.cache.slots` defaults to 1 000 and is read-only; it is a broker-wide cache limit, not a per-group member count. A stopped consumer closes positive-ID sessions after its fetchers stop, in parallel and best-effort. An explicit disconnect timeout supplies the remaining budget; otherwise this session cleanup is capped at the smaller of requestTimeout and 2 000 ms. A failed session close is logged at debug and does not fail the close.

## <a name="prefetching"></a> Prefetching

`maxPrefetchBatches` is accepted with a value of `0` or `1`, and **is inert in this release**: the fetch loop behaves identically at either value, and a fetcher still sends the next Fetch only after your handlers have finished with the previous response's batches.

```javascript
const consumer = kafka.consumer({ groupId: 'my-group', maxPrefetchBatches: 1 })
```

The one-deep prefetch the option was to control measured +2.45 % throughput against the ≥ +10 % the design required of it, and was withdrawn on that number. The option is kept, and reserved, so a later release can turn the prefetch on without asking you to change your configuration.

Any value other than `0` or `1` is rejected when you create the consumer, with `maxPrefetchBatches must be 0 or 1; deeper prefetch is not supported yet`.

## <a name="custom-partition-assigner"></a> Custom partition assigner

It's possible to configure the strategy the consumer will use to distribute partitions amongst the consumer group. KafkaJS has a round robin assigner configured by default.

assign receives { members, topics, groupTopics, groupTopicMetadata }. topics remains the leader's own subscription. groupTopics is the sorted union of every member's subscription. groupTopicMetadata is a ReadonlyMap from topic name to partition metadata. When the map is present, a missing name has no partitions; consult cluster metadata only when the entire map argument is absent. A leader obtains missing names from one bounded snapshot without adding permanent targets. The leader keeps watching the topics only other members subscribe to: it rereads them, at most once per metadataMaxAge, without creating topics or adding targets, and rejoins through the ordinary stale-assignment path when one gains partitions. A topic error is excluded; leaderless partition IDs remain available to the assigner.

A partition assigner is a function which returns an object with the following interface:

```javascript
const MyPartitionAssigner = ({ cluster }) => ({
    name: 'MyPartitionAssigner',
    version: 1,
    async assign({ members, topics }) {},
    protocol({ topics }) {}
})
```

The method `assign` has to return an assignment plan with partitions per topic. A partition plan consists of a list of `memberId` and `memberAssignment`. The member assignment has to be encoded, use the `MemberAssignment` utility for that. Example:

```javascript
const { AssignerProtocol: { MemberAssignment } } = require('@countly/kafkajs')

const MyPartitionAssigner = ({ cluster }) => ({
    version: 1,
    async assign({ members, topics }) {
        // perform assignment
        return myCustomAssignmentArray.map(memberId => ({
            memberId,
            memberAssignment: MemberAssignment.encode({
                version: this.version,
                assignment: assignment[memberId],
            })
        }))
    }
})
```

The method `protocol` has to return `name` and `metadata`. Metadata has to be encoded, use the `MemberMetadata` utility for that. Example:

```javascript
const { AssignerProtocol: { MemberMetadata } } = require('@countly/kafkajs')

const MyPartitionAssigner = ({ cluster }) => ({
    name: 'MyPartitionAssigner',
    version: 1,
    protocol({ topics }) {
        return {
            name: this.name,
            metadata: MemberMetadata.encode({
            version: this.version,
            topics,
            }),
        }
    }
})
```

Your `protocol` method will probably look like the example, but it's not implemented by default because extra data can be included as `userData`. Take a look at the `MemberMetadata#encode` for more information.

As of 3.0.0 the object passed to `protocol` carries three more keys beside `topics` — `ownedPartitions` (the partitions this member holds right now), `generationId` and `rackId` — which an assigner that only cares about topics can keep ignoring, exactly as the example above does. Two optional methods go with them, and both may be left out: `supportedProtocols()` returns the rebalance protocols your assigner can take part in (leaving it out means `['eager']`, which is what every 2.2.4 assigner gets), and `onAssignment(assignment, metadata)` is called after every successful rebalance — and with `null` when the member leaves the group or loses its generation — if your assigner needs to remember what it was given.

The example above declares `version: 1`, and its code is still correct as written, but one thing about it changed in 3.0.0: the subscription format is positional, so the version you declare now decides which fields go on the wire. An assigner declaring `version: 1` therefore emits the v1 layout (four more bytes, an empty `ownedPartitions` list). That is harmless between members of this client, and it is what a Java group leader needs in order to read the subscription at all. Declare `version: 0` if your assigner only ever sends topics and `userData`.

Once your assigner is done, add it to the list of assigners. It's important to keep the default assigner there to allow the old consumers to have a common ground with the new consumers when deploying.

```javascript
const { PartitionAssigners: { roundRobin } } = require('@countly/kafkajs')

kafka.consumer({
    groupId: 'my-group',
    partitionAssigners: [
        MyPartitionAssigner,
        roundRobin
    ]
})
```

## <a name="describe-group"></a> Describe group

> **Experimental** - This feature may be removed or changed in new versions of KafkaJS

The `retry.maxTotalTime` deadline for this call is described under [Assignment, subscription and offsets](#consumer-getters).

Returns metadata for the configured consumer group. It is routed exactly as [`admin.describeGroups`](Admin.md#describe-groups) is, so a group running either membership protocol describes itself correctly and both return the same row shape:

```javascript
const data = await consumer.describeGroup()
// {
//  errorCode: 0,
//  errorMessage: null,
//  groupId: 'consumer-group-id-f104efb0e1044702e5f6',
//  members: [
//    {
//      clientHost: '/172.19.0.1',
//      clientId: 'test-3e93246fe1f4efa7380a',
//      memberAssignment: Buffer,
//      memberId: 'test-3e93246fe1f4efa7380a-ff87d06d-5c87-49b8-a1f1-c4f8e3ffe7eb',
//      memberMetadata: Buffer,
//      groupInstanceId: null,
//      rackId: null,
//      memberEpoch: null,
//      assignment: [{ topic: 'topic-a', partitions: [0, 1] }],
//      targetAssignment: null,
//      memberType: null,
//      subscribedTopics: [],
//      subscribedTopicRegex: null,
//    },
//  ],
//  protocol: 'RoundRobinAssigner',
//  protocolType: 'consumer',
//  state: 'Stable',
//  groupType: 'classic',
//  groupEpoch: null,
//  assignmentEpoch: null,
//  assignorName: null,
//  authorizedOperations: null,
// },
```

Under `groupProtocol: 'consumer'` the same call is answered by `ConsumerGroupDescribe`, so `groupType` is `'consumer'`, `groupEpoch`/`assignmentEpoch`/`assignorName` carry the coordinator's values, `protocol` is the server-side assignor's name, and each member carries its `memberEpoch`, `assignment`, `targetAssignment`, `subscribedTopics` and `subscribedTopicRegex`. `authorizedOperations` is `null` unless authorized operations were requested. See [Admin: describe groups](Admin.md#describe-groups) for the full field list.

## <a name="compression"></a> Compression

KafkaJS only support GZIP natively, but [other codecs can be supported](Producing.md#compression-other).

## <a name="follower-fetching"></a> Follower Fetching

KafkaJS supports "follower fetching", where the consumer tries to fetch data preferentially from a broker in the same "rack", rather than always going to the leader. This can considerably reduce operational costs if data transfer across "racks" is metered. There may also be performance benefits if the network speed between these "racks" is limited.

The meaning of "rack" is very flexible, and can be used to model setups such as data centers, regions/availability zones, or other topologies.

See also [this blog post](https://www.confluent.io/blog/multi-region-data-replication/) for the bigger context.

Rack-aware assignment and follower fetching are separate features. Range's opt-in flag changes which member owns a partition. Follower fetching uses the broker's preferred-read-replica hint and the member's `rackId`; an eligible partition has a leader outside that rack and an online in-rack follower. A partition whose leader is already local need not fetch from a follower. Assignment alignment alone does not prove the next Fetch went to a follower.

A preferred follower is used only while its preference is valid and the replica is not listed in `offlineReplicas`. If that preferred replica is offline, the consumer falls back to the leader. Listing only the leader as offline does not exclude an online preferred follower. A partition with no leader waits for metadata while healthy partitions continue; it does not trigger a group rejoin solely because its leader is missing.

After a leader move, an idle fetcher starts the moved partition in the next round. A fetcher already waiting for a Fetch response starts it when that request completes: at most `maxWaitTimeInMs` plus one round trip. An in-flight Fetch is not cancelled.

The consumer has one fetcher per broker and at most one Fetch in flight per broker. A fetcher waits for its batches to drain before fetching again. There is no separate lower numeric concurrency cap; use `maxBytes` and handler processing to control retained response work.
