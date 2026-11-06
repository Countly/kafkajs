# Share consumer

Share groups require Kafka 4.2 or later with share groups enabled. Use `kafka.shareConsumer({ groupId })`. This is a separate client from `kafka.consumer()`; existing consumer, producer and admin behavior is unchanged. Import from `@countly/kafkajs` in CommonJS or ESM. Internal source paths are not public exports.

```js
const { Kafka } = require('@countly/kafkajs')
const kafka = new Kafka({ brokers: ['localhost:9092'] })
const share = kafka.shareConsumer({ groupId: 'orders' })
await share.connect()
await share.subscribe({ topics: ['orders'] })
await share.run({
  eachMessage: async ({ message, acknowledge, renew, isRunning }) => {
    if (message.deliveryCount >= 4) return acknowledge('reject')
    await handleOrder(message)
  },
})
```

`connect()` checks the advertised API versions and finalized share feature before any share traffic. A cluster with missing APIs, preview-only ShareFetch v1, a finalized disabled share feature or a runtime refusal rejects with `KafkaJSShareGroupsNotSupportedError`. Its `reason`, `apiKey`, `apiName` and range metadata explain the refusal. Kafka 4.0 and 4.1 cannot run this client's share consumer. Feature information with epoch −1 leaves the decision to the runtime response.

`run()` resolves after the first accepted group join. Fatal join errors reject it. Retriable coordinator failures keep it pending until a join or stop. Later terminal failures emit `crash`. Run exactly one `eachMessage` or `eachBatch` handler. Nodes run in parallel; one node invokes one handler at a time and does not prefetch another acquisition while handlers still own its records. There is no ordering guarantee across consumers.

## Options and snapshots

| Option | Default | Accepted values |
|---|---|---|
| `groupId` | required | non-empty string |
| `acknowledgementMode` | `implicit` | `implicit`, `explicit` |
| `acquireMode` | `record_limit` | `record_limit`, `batch_optimized` |
| `maxRecords` | 500 | integer 1..2147483647 |
| `minBytes` | 1 | integer 0..2147483647 |
| `maxBytes` | 10485760 | integer 1..2147483647; response target, not memory ceiling |
| `maxWaitTimeInMs` | 500 | integer 0..2147483647 |
| `metadataMaxAge`, `maxPollInterval` | 300000 each | integer 1..2147483647 |
| `rackId` | empty string | string; empty means no rack |
| `retry` | consumer retry policy, with five retries unless overridden | consumer retry options, including `restartOnFailure` |
| `maxInFlightRequests` | 5 | `null` or integer at least 1 |
| `allowAutoTopicCreation`, `checkCrcs` | `true` | booleans |

Validation happens before creating the client's cluster, emitter, timers or requests. Unknown option names are ignored. Zero `minBytes` and `maxWaitTimeInMs` are legal; fast empty replies are paced to at most twenty requests per second per node. Share decode has a fixed 1073741824-byte decompressed-batch ceiling and a derived 2097152-record ceiling; `maxBytes` does not change them.

`subscribe({ topics: ['a', 'b'] })` replaces the subscription while stopped. Names must be distinct, non-empty strings. A regex, `topic`, `regex`, `fromBeginning` or `offsetReset` in the subscription is rejected. To replace topics, use `await stop()`, then `subscribe`, then `run`. `subscription()` returns a fresh array, initially `[]`, and retains names after stop. `assignment()` returns fresh topic/partition snapshots, initially and after stop `[]`. `acquisitionLockTimeoutMs()` starts as `null` and then reports the latest broker lock duration, including a successful renew.

These options are rejected when their value is not `undefined`: `fromBeginning` (use group config `share.auto.offset.reset`), `readUncommitted` (use `share.isolation.level`), `groupInstanceId` (no static membership), `partitionAssigners`, `groupProtocol`, `groupRemoteAssignor` (the broker assigns), `sessionTimeout` and `rebalanceTimeout` (use `share.session.timeout.ms`), `heartbeatInterval` (use `share.heartbeat.interval.ms`), `maxBytesPerPartition` (the released share fetch has no per-partition limit), and `maxPrefetchBatches` (prefetch is not offered). There is no seek, position, commit, pause, resume, manual assignment, rebalance listener or running subscription replacement.

## Decisions and renewal

In implicit mode, a normal handler return accepts undecided messages. A throw releases undecided messages, reports the original error in `handler_error` and continues after retry backoff. Existing explicit decisions survive a throw. In explicit mode every delivered message must be acknowledged; an omission is released before a `KafkaJSShareAcknowledgementError` with reason `unacknowledged` crashes the run with `restart: false`. Throwing is different from a normal return with an omission: a throw releases and continues in both modes.

`acknowledge()` defaults to `accept`. `accept` means done, `release` allows redelivery to any member, and `reject` archives the record. A decision is provisional until its handler returns: the last call wins, including in implicit mode. A call after return, after loss of its acquisition, or for a foreign message throws `KafkaJSNonRetriableError`.

An `eachBatch` handler receives `{ batch, acknowledge, renew, isRunning }`. `batch` contains `topic`, `partition`, `messages` and `acquisitionLockTimeoutMs`. Call `acknowledge(message, type)` or `acknowledge({ offset }, type)` in any order. Offsets are exact decimal strings, including values above JavaScript's safe-integer range. Do not convert them to numbers.

```js
await share.run({
  eachBatch: async ({ batch, acknowledge, renew, isRunning }) => {
    for (const message of batch.messages) {
      if (!isRunning()) break
      await processMessage(message)
      acknowledge(message)
    }
  },
})
```

When a current batch handler returns after stop was requested, undecided messages are released in both modes, with no omission crash. Already-decided messages retain their choices. A current successful `eachMessage` call still accepts its message. A return from a fenced old handler has no further effect. `isRunning()` reflects this invocation's run and membership and becomes false after stop; a data-session reset alone does not change it.

`renew()` in `eachMessage` renews that message. `renew()` in `eachBatch` renews the whole current batch; `renew([message, { offset }])` renews a distinct subset. It resolves with the broker-applied duration in milliseconds. Renew and acknowledgement requests share the node's FIFO. Renewal is nonterminal, does not increase delivery count, and extends only selected entry deadlines from that renew request's pre-write time. A refusal such as `INVALID_RECORD_STATE` rejects with `KafkaJSShareAcknowledgementError` and changes no deadline. Do not start a renewal after returning from the handler. An unawaited renewal is still serialized before terminal preparation; observe its returned promise for application error handling.

A record is acquired under a lock. Size `maxRecords × handler time` below the lock duration; use handler-driven renewal for long calls. `record_limit` bounds records per node, so a member may hold up to `maxRecords × nodes`; `batch_optimized` can exceed `maxRecords` at batch boundaries. A start gate drops expired unstarted records and emits `lock_expired`; it does not suppress a current handler's late acknowledgement, whose acceptance belongs to the broker. Decided entries may flush at handler boundaries when less than half their own lock duration remains. No background auto-renewal or per-record timer exists.

Every acquisition increases delivery count. The default `share.delivery.count.limit` is 5; configure it for the group when repeated processing failures need more attempts. The broker lock budget is governed by `group.share.partition.max.record.locks`; size work and `maxRecords` with that budget in mind. Set an admissible `share.record.lock.duration.ms` and wait for GROUP readback before a member joins. A release, stop, revocation, throw or lock expiry at the broker's delivery-count limit can archive the record. Release therefore does not guarantee another delivery. On any decode halt, including checksum failure, valid prefix records and proven gaps are preserved, the undecoded acquired remainder is released compactly, and the original error is reported. No automatic reject is inferred from a failed batch header. A partial trailing batch ends the complete-batch walk; remaining acquired offsets are gaps, matching the released Java client.

## Stop, disconnect and recovery

`stop()` turns fetching off, waits naturally for current handlers, then attempts one final acknowledgement per established node and one group leave. It retains subscription and may retain healthy connections. A handler must call `stop()` without awaiting it; awaiting stop from that handler waits on itself. `disconnect()` also closes this client's captured sockets. A later `run()` starts new heartbeat/poll state and a new member ID.

`disconnect({ timeout })` accepts integer milliseconds from 0 through 2147483647. The deadline covers the whole call. Reserve `min(1000, floor(timeout / 4))` milliseconds for final traffic and close; remaining handlers are abandoned at the boundary before that reserve. For example, timeout 400 abandons them at 300 ms. Their later decisions throw. Non-object signal-handler arguments are ignored. Invalid object timeouts reject before changing state. Concurrent closes share one captured episode, and the first valid disconnect supplies its deadline. Timeout zero synchronously revokes unsent final traffic, including leave, and closes only captured sockets.

After the handler boundary, all finals and leave together have a finite local `requestTimeout` cap, including when transport timeout enforcement is disabled. A still-busy node skips its final. A lost or failed final result remains failed or remotely unknown; local decisions alone do not prove broker acceptance or release. No acknowledgement is resent after uncertain application. The close captures original membership, sessions, connections and terminal ranges; it cannot reconnect a replacement generation or clear a later run.

A crash fences old work immediately and finishes bounded cleanup before restart supervision. The exact original terminal error is passed to `retry.restartOnFailure`. A callback rejection is logged and defaults to restart as for the ordinary consumer. Configuration, authorization, broker refusal and explicit omission force `restart: false`. Stop or disconnect while supervision is pending prevents resurrection. Poll expiry tracks every active node handler's own start; progress on a fast node cannot conceal a slow one. Idle members do not expire for lack of records. Rejoin waits for captured expired calls to finish or be abandoned and for their close episode to end.

## Isolation

Share isolation defaults to read_uncommitted, unlike consumer's read_committed; set share.isolation.level=read_committed on the GROUP resource and wait for describeConfigs readback before joining.

The readUncommitted option is rejected; configure share.isolation.level for the GROUP resource.

An open transaction blocks a read_committed group's delivery.

The broker applies isolation for the whole group. ShareFetch sends no client isolation setting and returns no aborted-transaction list or last stable offset. The client applies no transactional filtering. Acquired control records and completed-walk missing offsets are Gap acknowledgements, never handler messages. Set the desired GROUP configuration with `admin.incrementalAlterConfigs` and poll `admin.describeConfigs` until it reads back before joining; joining first can initialize share-partition state with `latest`.

## Events and diagnostics

`share.events` has eighteen public names: `heartbeat`, `group_join`, `assignment_changed`, `fetch`, `acknowledge`, `session_reset`, `lock_expired`, `handler_error`, `connect`, `disconnect`, `stop`, `crash`, `network.request`, `network.request_timeout`, `network.request_queue_size`, `network.authentication_failure`, `cluster.topic_recreated`, `cluster.rebootstrap`, each prefixed with `share_consumer.`. Subscribe with `share.on(share.events.ACKNOWLEDGE, listener)`; it returns a remover. Unknown names throw synchronously. Sync listener throws and rejected returns are logged and contained.

Every name is mirrored at `kafkajs.share_consumer.<name>` on `node:diagnostics_channel`. Node handles an exception from a diagnostics subscriber as an uncaught exception, as for existing clients. No event contains record values or keys. `acknowledge` contains exactly `topic`, `partition`, `firstOffset`, `lastOffset`, `type`, `error`. Its inclusive offsets and error `ranges` remain compact even for a million absent offsets. A null error confirms broker acceptance. A typed failure preserves a reason and, when available, protocol cause. Terminal offsets settle at most once; `renew` events are nonterminal and may repeat. `handler_error` and `lock_expired` offsets refer only to delivered records.

The share-specific administration methods are `describeShareGroups`, `listShareGroupOffsets`, `alterShareGroupOffsets`, and `deleteShareGroupOffsets`; use existing `deleteGroups` to delete an empty share group. Administrative reset recipes, interoperability observations and tested equivalents are documented with their verified examples when available. Telemetry instance IDs and metric-registration extensions are outside this surface.
