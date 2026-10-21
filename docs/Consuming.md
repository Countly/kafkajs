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

When suppling a regular expression, the consumer will not match topics created after the subscription. If your broker has `topic-A` and `topic-B`, you subscribe to `/topic-.*/`, then `topic-C` is created, your consumer would not be automatically subscribed to `topic-C`.

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
* `commitOffsetsIfNecessary(offsets?): Promise<void>` is used to commit offsets based on the autoCommit configurations (`autoCommitInterval` and `autoCommitThreshold`). Note that auto commit won't happen in `eachBatch` if `commitOffsetsIfNecessary` is not invoked. Take a look at [autoCommit](#auto-commit) for more information.
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

The messages are always fetched in batches from Kafka, even when using the `eachMessage` handler. All resolved offsets will be committed to Kafka after processing the whole batch.

Committing offsets periodically during a batch allows the consumer to recover from group rebalancing, stale metadata and other issues before it has completed the entire batch. However, committing more often increases network traffic and slows down processing. Auto-commit offers more flexibility when committing offsets; there are two flavors available:

`autoCommitInterval`: The consumer will commit offsets after a given period, for example, five seconds. Value in milliseconds. Default: `null`

```javascript
consumer.run({
  autoCommitInterval: 5000,
  // ...
})
```

`autoCommitThreshold`: The consumer will commit offsets after resolving a given number of messages, for example, a hundred messages. Default: `null`

```javascript
consumer.run({
  autoCommitThreshold: 100,
  // ...
})
```

Having both flavors at the same time is also possible, the consumer will commit the offsets if any of the use cases (interval or number of messages) happens.

`autoCommit`: Advanced option to disable auto committing altogether. Instead, you can [manually commit offsets](#manual-commits). Default: `true`

## <a name="membership-errors"></a> Membership errors during rebalance

When the group coordinator rejects a heartbeat or a fetch, the consumer tells apart two kinds of error and reacts differently:

| Error | Code | Meaning | Consumer action |
|---|--:|---|---|
| `ILLEGAL_GENERATION` | 22 | member is **LOST** | discard uncommitted offsets, never autocommit, rejoin (the member id is kept — the generation is stale, not the member id) |
| `UNKNOWN_MEMBER_ID` | 25 | member is **LOST** | same, plus reset the member id |
| `FENCED_INSTANCE_ID` | 82 | member is **LOST** (fenced by a duplicate static member) | same, and do not rejoin |
| `REBALANCE_IN_PROGRESS` | 27 | partitions are being **REVOKED** | a final commit is allowed before rejoining |
| `NOT_COORDINATOR_FOR_GROUP` | 16 | the coordinator moved | a final commit is allowed once the new coordinator is found |
| `GROUP_LOAD_IN_PROGRESS` | 14 | the coordinator is still loading the group | a final commit is allowed once the join retries |

A LOST member's generation, member id, or static instance id has already been invalidated by the coordinator, so any offsets committed under it would be rejected forever — that used to surface as a repeating "Specified group generation id is not valid" error. A REVOKED member is still a valid part of the group, so committing its already-resolved offsets before rejoining is safe — this covers every rebalance-class error above and beyond it, not just 27/16. On the fetch path, a LOST or REVOKED error emits the same public `REBALANCING` instrumentation event before the consumer rejoins.

Two more things happen around a rebalance, both new in 3.0.0. A handler that has not returned within `maxPollInterval` (see [Options](#options)) is treated as a lost member: the consumer logs a warning, emits `POLL_INTERVAL_EXCEEDED`, gives up its partitions and rejoins, and the batch that handler eventually returns is dropped and redelivered afterwards. The deadline is reset on every batch handed to your handler *and* on every fetch round that comes back empty, and there is one fetcher per broker, so the guard fires only when every fetcher is stalled behind the overrunning handler — in practice a single-broker assignment with `partitionsConsumedConcurrently: 1`. And on the **cooperative** path a `REBALANCE_IN_PROGRESS` no longer comes out of the `heartbeat()` callback into your handler — the call resolves and the rejoin runs in the background while your handler keeps the partitions it is not losing. On the default eager path that callback still throws, as before.

Not every fetch-path error is a rebalance signal, though: a CRC32C mismatch on a record batch (`checkCrcs`, see [Options](#options)) is a **non-retriable** error, handled like any other non-retriable error — the consumer crashes rather than rejoining, and the offset is not advanced past the corrupt batch.

## <a name="manual-commits"></a> Manual committing

When disabling [`autoCommit`](#auto-commit) you can still manually commit message offsets, in a couple of different ways:

- By using the `commitOffsetsIfNecessary` method available in the `eachBatch` callback. The `commitOffsetsIfNecessary` method will still respect the other autoCommit options if set.
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

If the partition's leader changes and the new leader's log diverges from what this consumer had already consumed — the tail of the old leader's log was never replicated — the consumer detects it before its next fetch and repositions the partition. When the divergence point is known, the partition moves to the first offset known to diverge and resumes from there; when it is not, the partition is reset per its `fromBeginning` setting, and that reset offset is committed to the coordinator even under `autoCommit: false`, which an offset that is out of range has always done. Either way the consumer logs a warning and emits [`LOG_TRUNCATION`](InstrumentationEvents.md#consumer). A paused partition can be repositioned this way and stays paused — pausing is your intent, not offset state.

## <a name="topic-recreation"></a> Topic recreation

If a subscribed topic is deleted and recreated, the new topic is a different entity from the broker's point of view even though it has the same name — it gets a new topic id and starts back at offset 0. A committed offset from the old topic is meaningless against it, and because cleanup of the internal `__consumer_offsets` topic is asynchronous, that stale offset can still be there — and can still look like a perfectly valid offset into the new topic's log.

When the consumer notices a subscribed topic has come back under a new id, it resets that topic's offsets once: it ignores the coordinator's committed offset for it and resolves the next offset from the configured boundary instead, per that subscription's [`fromBeginning`](#from-beginning) setting. This happens automatically and needs no configuration. Paused partitions stay paused across a recreation, since pausing is your intent, not offset state, and offsets committed against the *new* topic afterwards are never touched by *that consumer's* reset — each member of a group resets once, independently, the first time it notices the recreation.

A reset still owed survives the consumer's automatic restart after a crash; `consumer.disconnect()` drops it.

If you call [`seek`](#seek) around the time of a recreation, the newer intent wins: a seek issued *after* the consumer has noticed the recreation targets the current topic and is honored instead of the reset for that partition, while a seek issued *before* it is dropped, since its target offset belonged to the topic's previous incarnation.

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
| sessionTimeout         | Timeout in milliseconds used to detect failures. The consumer sends periodic heartbeats to indicate its liveness to the broker. If no heartbeats are received by the broker before the expiration of this session timeout, then the broker will remove this consumer from the group and initiate a rebalance                                       | `30000`                           |
| maxPollInterval        | The maximum time, in milliseconds, your `eachMessage`/`eachBatch` handler may take to return, and the rebalance timeout sent to the coordinator. It is the Java client's `max.poll.interval.ms`. A handler that takes longer loses its partitions and the consumer rejoins the group; see [Membership errors during rebalance](#membership-errors) | `300000` (5 minutes)              |
| rebalanceTimeout       | **Deprecated — use `maxPollInterval`.** The maximum time that the coordinator will wait for each member to rejoin when rebalancing the group. Still sent on the wire when it is the only one of the two you set, and setting it logs a deprecation warning once per consumer you create. It never becomes your handler deadline: that stays at `max(rebalanceTimeout, 300000)`, so an existing 20 s or 60 s rebalance timeout cannot turn into a 20 s or 60 s handler deadline. If you set both, `maxPollInterval` wins | `300000` (5 minutes, from `maxPollInterval`) |
| heartbeatInterval      | The expected time in milliseconds between heartbeats to the consumer coordinator. Heartbeats are used to ensure that the consumer's session stays active. The value must be set lower than session timeout                                                                                                                                         | `3000`                            |
| metadataMaxAge         | The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions                                                                                                                                                       | `300000` (5 minutes)              |
| allowAutoTopicCreation | Allow topic creation when querying metadata for non-existent topics; disable this to keep an operator-deleted subscribed topic absent                                                                                                                                                                                                                                                                                | `true`                            |
| maxBytesPerPartition   | The maximum amount of data per-partition the server will return. This size must be at least as large as the maximum message size the server allows or else it is possible for the producer to send messages larger than the consumer can fetch. If that happens, the consumer can get stuck trying to fetch a large message on a certain partition | `1048576` (1MB)                   |
| minBytes               | Minimum amount of data the server should return for a fetch request, otherwise wait up to `maxWaitTimeInMs` for more data to accumulate.                                                                                                                                                                                                           | `1`                               |
| maxBytes               | Maximum amount of bytes to accumulate in the response. Supported by Kafka >= `0.10.1.0`                                                                                                                                                                                                                                                            | `10485760` (10MB)                 |
| maxPrefetchBatches     | Accepted with a value of `0` or `1`, and **inert in this release**: the fetch loop behaves identically at either value. Reserved for a later release, which may use it to let a fetcher keep one fetch on the wire while the previous response is still being handled. Any other value is rejected when you create the consumer. See [Prefetching](#prefetching) | `0` |
| maxWaitTimeInMs        | The maximum amount of time in milliseconds the server will block before answering the fetch request if there isn’t sufficient data to immediately satisfy the requirement given by `minBytes`                                                                                                                                                      | `5000`                            |
| checkCrcs              | Verify the CRC32C checksum of every record batch received. Default `true` (Java's `check.crcs`). A mismatch is treated as a **non-retriable** error: no message from that batch is delivered, the consumer crashes with `KafkaJSPartialMessageError` (marked with `crcMismatch: true`, distinguishing it from an ordinary truncated fetch tail; the `CRASH` event's `restart` follows `retry.restartOnFailure` as for any other non-retriable error), and the committed offset is **not** advanced past the corrupt batch — so a restart re-reads it. Set to `false` to skip the check and restore the pre-3.0 behaviour, where a corrupt batch was delivered to the application as if it were valid. | `true` |
| retry                  | See [retry](Configuration.md#retry) for more information                                                                                                                                                                                                                                                                                           | `{ retries: 5 }`                 |
| readUncommitted        | Configures the consumer isolation level. If `false` (default), the consumer will not return any transactional messages which were not committed.                                                                                                                                                                                                   | `false`                           |
| maxInFlightRequests | Maximum requests in progress per connection. `null` means unbounded; `0` is rejected. | `5` |
| rackId                 | Configure the "rack" in which the consumer resides to enable [follower fetching](#follower-fetching)                 | `null` _(fetch from the leader always)_ |
| groupInstanceId        | Static membership id (KIP-345). See [Static membership](#static-membership)                                          | `null` _(dynamic membership)_ |
| groupProtocol          | The group membership protocol: `'classic'` or `'consumer'` (KIP-848, Kafka 4.0+). See [Consumer group protocols](#group-protocols) | `'classic'` |
| groupRemoteAssignor    | The server-side assignor to ask the coordinator for, under `groupProtocol: 'consumer'` only. `null` lets the coordinator pick its first configured assignor | `null` _(coordinator's choice)_ |

`allowAutoTopicCreation` defaults to `true`, preserving existing behavior. If an operator deletes a topic while a consumer remains subscribed, a later metadata request can recreate it with broker defaults. Set `allowAutoTopicCreation: false` when deletion must keep the topic absent; metadata then reports the unknown topic instead of requesting its recreation. Topic recreation is a new incarnation, so the consumer resets its starting offset according to the subscription’s offset policy.

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
- **Stopping itself does not change.** The consumer still stops fetching and waits for the handler that is running; under the consumer protocol it also still commits what it owns and emits `PARTITIONS_REVOKED`, whatever `leaveGroup` says.
- **A second `disconnect()` that joins one already running keeps the first call's choice.** `consumer.stop()` takes no option, and a consumer that crashes and restarts always uses the default. `leaveGroup` applies to the stop that `disconnect()` itself performs: after a `stop()` there is nothing left to leave, so `disconnect({ leaveGroup: true })` sends nothing, and the same holds after the member has already been removed, for instance by a poll-interval overrun.
- **Called without an argument, or with anything but an options object, `disconnect()` changes nothing.** An argument that is not an object is ignored, as it was in kafkajs 2.2.4, so `process.on('SIGTERM', consumer.disconnect)` (which passes the signal name) still works, and `disconnect(true)` does not mean "leave". An options object whose `leaveGroup` is anything but `true`, `false` or `undefined` throws `KafkaJSNonRetriableError` before anything is stopped.

A classic static member that leaves with `true` sends its member id and its `groupInstanceId`, so the coordinator checks the pair and answers `FENCED_INSTANCE_ID` for a member id that is no longer the instance's current one instead of removing someone else's membership. The Java client sends the member id alone.

## <a name="pause-resume"></a> Pause & Resume

In order to pause and resume consuming from one or more topics, the `Consumer` provides the methods `pause` and `resume`. It also provides the `paused` method to get the list of all paused topics. Note that pausing a topic means that it won't be fetched in the next cycle and subsequent messages within the current batch won't be passed to an `eachMessage` handler.

Calling `pause` with a topic that the consumer is not subscribed to is a no-op, calling `resume` with a topic that is not paused is also a no-op.

> Note: Calling `resume` or `pause` while the consumer is not running will throw an error.

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

For a concrete offset, the consumer commits the sought offset by default; set [`autoCommit`](#auto-commit) to `false` to disable that commit. A sentinel seek to `'-2'` (earliest) or `'-1'` (latest) instead requests that partition's exact boundary through ListOffsets on the next resolution, independently of `fromBeginning` and `autoCommit`. The following fetch starts at the returned concrete offset. A sentinel seek never commits the negative sentinel. Pending sentinel intent is discarded if ownership is lost or the consumer stops.

```javascript
consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, message }) => true
})
// This will now only resolve the previous offset, not commit it
consumer.seek({ topic: 'example', partition: 0, offset: "12384" })
```

## <a name="partition-assigners"></a> Built-in partition assigners

`PartitionAssigners` exports four assigners. The default is unchanged: a consumer you create without `partitionAssigners` uses `[PartitionAssigners.roundRobin]`, exactly as before.

| Assigner | Name on the wire | What it does |
| --- | --- | --- |
| `roundRobin` | `RoundRobinAssigner` | The default. Deals every partition of every subscribed topic out to the members in turn. No Java client understands this name, so a group using it is a JavaScript-only group. |
| `range` | `range` | Kafka's `RangeAssignor`: per topic, the partitions are split into contiguous ranges, one per member. Members subscribed to the same few topics tend to get the same partition numbers of each. |
| `sticky` | `sticky` | Kafka's `StickyAssignor`: balances partitions while moving as few of them as possible from their current owners. |
| `cooperativeSticky` | `cooperative-sticky` | Kafka's `CooperativeStickyAssignor` (KIP-429): the same balancing as `sticky`, plus incremental rebalancing — a member keeps the partitions it is not losing instead of giving up everything at the start of each round. |

`range`, `sticky` and `cooperativeSticky` use the same names and the same algorithms as the Java client, so a group can mix this client with Java consumers. Rack-aware assignment is not implemented: your `rackId` is sent to the leader and ignored when partitions are assigned.

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

### Regex subscriptions

Under `'consumer'` you can hand the coordinator a pattern instead of topic names:

```javascript
await consumer.subscribe({ regex: '^orders-.*', fromBeginning: true })
```

- The pattern is **RE2, not JavaScript**. It is sent verbatim and evaluated by the broker, so what matches is decided by Kafka's dialect, not by `RegExp`. A pattern the broker rejects is a fatal membership error.
- The coordinator holds the pattern and re-evaluates it as topics come and go, so a topic created *after* you subscribe joins the subscription on its own — no client-side metadata poll, no resubscribe.
- One consumer subscribes to names or to a regex, never both: mixing them throws `Subscribe to topics or to a regex, not both`. Under `'classic'` the form itself is rejected, with `A regex subscription requires groupProtocol: 'consumer'`.
- A JavaScript `RegExp` inside `topics` is unchanged under both protocols: it is matched client-side once, at `subscribe()` time, against the topics that exist then.
- The set of topic names a regex subscription has resolved to only grows for the life of the membership. A topic the pattern stops matching — because it was deleted, or renamed out of the pattern — stays in the subscription until the consumer rebuilds its membership (a restart, or a crash recovery). The set is bounded by the number of distinct topics the pattern ever matched.

### Fencing and loss

Under `'consumer'` the two ways a member can be separated from its partitions stay clearly apart:

- **Fenced, or the poll deadline expired.** The coordinator rejected this member's epoch or id, or your handler took longer than `maxPollInterval`. The member emits [`PARTITIONS_LOST`](InstrumentationEvents.md#consumer) for its **whole** owned set and commits nothing for it: the epoch those offsets were resolved under is gone, so a commit would be rejected anyway. It then rejoins from scratch.
- **Reconciled away.** The coordinator moved some partitions to another member. With `autoCommit: true` the offsets your handlers resolved on those partitions are committed **first**, and then [`PARTITIONS_REVOKED`](InstrumentationEvents.md#consumer) reports them. The partitions you keep are never interrupted.

A poll-interval overrun is detected on a heartbeat tick and never while a reconciliation is in flight, so under `'consumer'` detection is bounded by the heartbeat interval the coordinator asked for rather than by one you configured.

### Migrating in either direction

`groupProtocol` is a per-consumer option, so a group can be moved one deployment at a time. See the [migration guide](MigrationGuide-3-0-0.md) for what changes for callers in each direction, including the behaviour changes that also apply to classic consumers.

### Limitations

- **No client-side assigners.** The broker assigns; `partitionAssigners` is rejected, and a custom assigner has no equivalent under this protocol.
- **Rack-aware assignment is the broker's.** Your `rackId` is sent to the coordinator, but rack-aware assignment is still work in progress upstream.
- **Consumer groups only.** `groupProtocol` does not address share groups or streams groups.

## <a name="prefetching"></a> Prefetching

`maxPrefetchBatches` is accepted with a value of `0` or `1`, and **is inert in this release**: the fetch loop behaves identically at either value, and a fetcher still sends the next Fetch only after your handlers have finished with the previous response's batches.

```javascript
const consumer = kafka.consumer({ groupId: 'my-group', maxPrefetchBatches: 1 })
```

The one-deep prefetch the option was to control measured +2.45 % throughput against the ≥ +10 % the design required of it, and was withdrawn on that number. The option is kept, and reserved, so a later release can turn the prefetch on without asking you to change your configuration.

Any value other than `0` or `1` is rejected when you create the consumer, with `maxPrefetchBatches must be 0 or 1; deeper prefetch is not supported yet`.

## <a name="custom-partition-assigner"></a> Custom partition assigner

It's possible to configure the strategy the consumer will use to distribute partitions amongst the consumer group. KafkaJS has a round robin assigner configured by default.

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
