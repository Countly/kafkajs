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

When a current batch handler returns after stop or disconnect was requested, undecided messages are released in both modes, with no omission crash. Already-decided messages retain their choices. A current successful `eachMessage` call still accepts its message. A return from a fenced old handler has no further effect. `isRunning()` reflects this invocation's run and membership and becomes false after stop or disconnect; a data-session reset alone does not change it. A handler that outlives such a reset, or the loss of its node's socket, does not end the run: its acquisition settles as lost, and the node reopens its session under the retry backoff.

`renew()` in `eachMessage` renews that message. `renew()` in `eachBatch` renews the whole current batch; `renew([message, { offset }])` renews a distinct subset. It resolves with the broker-applied duration in milliseconds; an answer that keeps the duration resolves with the one that acquisition last had, from its fetch or its last successful renewal. Renew and acknowledgement requests share the node's FIFO. Renewal is nonterminal, does not increase delivery count, and extends only selected entry deadlines from that renew request's pre-write time. A refusal such as `INVALID_RECORD_STATE` rejects with `KafkaJSShareAcknowledgementError` and changes no deadline. Do not start a renewal after returning from the handler. An unawaited renewal is still serialized before terminal preparation; observe its returned promise for application error handling.

A record is acquired under a lock. Size `maxRecords × handler time` below the lock duration; use handler-driven renewal for long calls. `record_limit` bounds records per node, so a member may hold up to `maxRecords × nodes`; `batch_optimized` can exceed `maxRecords` at batch boundaries. A start gate drops expired unstarted records and emits `lock_expired`; it does not suppress a current handler's late acknowledgement, whose acceptance belongs to the broker. Decided entries may flush at handler boundaries when less than half their own lock duration remains. No background auto-renewal or per-record timer exists.

Every acquisition increases delivery count. The default `share.delivery.count.limit` is 5; configure it for the group when repeated processing failures need more attempts. The broker lock budget is governed by `group.share.partition.max.record.locks`; size work and `maxRecords` with that budget in mind. Set an admissible `share.record.lock.duration.ms` and wait for GROUP readback before a member joins. A release, stop, revocation, throw or lock expiry at the broker's delivery-count limit can archive the record. Release therefore does not guarantee another delivery. On any decode halt, including checksum failure, valid prefix records and proven gaps are preserved, the undecoded acquired remainder is released compactly, and the original error is reported. No automatic reject is inferred from a failed batch header. A partial trailing batch ends the complete-batch walk; remaining acquired offsets are gaps, matching the released Java client.

## Stop, disconnect and recovery

`stop()` turns fetching off, waits naturally for current handlers, then attempts one final acknowledgement per established node and one group leave. It retains subscription and may retain healthy connections. A handler must call `stop()` without awaiting it; awaiting stop from that handler waits on itself. `disconnect()` also closes this client's captured sockets. A later `run()` starts new heartbeat/poll state and a new member ID.

`disconnect({ timeout })` accepts integer milliseconds from 0 through 2147483647. The deadline covers the whole call. Reserve `min(1000, floor(timeout / 4))` milliseconds for final traffic and close; remaining handlers are abandoned at the boundary before that reserve. For example, timeout 400 abandons them at 300 ms. Their later decisions throw. Non-object signal-handler arguments are ignored. Invalid object timeouts reject before changing state. Concurrent closes share one captured episode, and the first valid disconnect supplies its deadline. Timeout zero synchronously revokes unsent final traffic, including leave, and closes only captured sockets. It also ends the wait for a final that was already written and is unanswered, even during a close that started earlier: that final settles as lost, its socket closes and nothing is resent.

After the handler boundary, all finals and leave together have a finite local `requestTimeout` cap, including when transport timeout enforcement is disabled. A still-busy node skips its final. A lost or failed final result remains failed or remotely unknown; local decisions alone do not prove broker acceptance or release. No acknowledgement is resent after uncertain application. The close captures original membership, sessions, connections and terminal ranges; it cannot reconnect a replacement generation or clear a later run.

A crash fences old work immediately, also when it interrupts a stop that is waiting for handlers, and finishes bounded cleanup before restart supervision. A stop or disconnect that arrives while a fence or poll-expiry recovery is closing ends the run once that cleanup is done. The heartbeat chain is suspended for the length of a recovery close, so no join overlaps its final traffic. The exact original terminal error is passed to `retry.restartOnFailure`. A callback rejection is logged and defaults to restart as for the ordinary consumer. Configuration, authorization, broker refusal and explicit omission force `restart: false`. Stop or disconnect while supervision is pending prevents resurrection. Poll expiry tracks every active node handler's own start; progress on a fast node cannot conceal a slow one. Idle members do not expire for lack of records. Rejoin waits for captured expired calls to finish or be abandoned and for their close episode to end.

## Isolation

Share isolation defaults to read_uncommitted, unlike consumer's read_committed; set share.isolation.level=read_committed on the GROUP resource and wait for describeConfigs readback before joining.

The readUncommitted option is rejected; configure share.isolation.level for the GROUP resource.

An open transaction blocks a read_committed group's delivery.

The broker applies isolation for the whole group. ShareFetch sends no client isolation setting and returns no aborted-transaction list or last stable offset. The client applies no transactional filtering. Acquired control records and completed-walk missing offsets are Gap acknowledgements, never handler messages. Set the desired GROUP configuration with `admin.incrementalAlterConfigs` and poll `admin.describeConfigs` until it reads back before joining; joining first can initialize share-partition state with `latest`.

## Events and diagnostics

`share.events` has eighteen public names: `heartbeat`, `group_join`, `assignment_changed`, `fetch`, `acknowledge`, `session_reset`, `lock_expired`, `handler_error`, `connect`, `disconnect`, `stop`, `crash`, `network.request`, `network.request_timeout`, `network.request_queue_size`, `network.authentication_failure`, `cluster.topic_recreated`, `cluster.rebootstrap`, each prefixed with `share_consumer.`. `session_reset` reports a data-session reset or, with reason `membership`, a session the run discarded when its membership closed; `recordsDropped` counts the delivered records that reset discarded, not automatic gap spans or records already settled. Subscribe with `share.on(share.events.ACKNOWLEDGE, listener)`; it returns a remover. Unknown names throw synchronously. Sync listener throws and rejected returns are logged and contained.

Every name is mirrored at `kafkajs.share_consumer.<name>` on `node:diagnostics_channel`. Node handles an exception from a diagnostics subscriber as an uncaught exception, as for existing clients. No event contains record values or keys. `acknowledge` contains exactly `topic`, `partition`, `firstOffset`, `lastOffset`, `type`, `error`. Its inclusive offsets and error `ranges` remain compact even for a million absent offsets. A null error confirms broker acceptance. A typed failure preserves a reason and, when available, protocol cause. Terminal offsets settle at most once; `renew` events are nonterminal and may repeat. `handler_error` and `lock_expired` offsets refer only to delivered records.

## Java API equivalents and boundaries

Use `run({ eachMessage })` or `run({ eachBatch })` for Java's polling role. The promise resolves after the first accepted join; messages arrive through handlers. There is no application `poll()` call. Handler return and the node loop send acknowledgements asynchronously; there is no `commitAsync()` call to trigger or a `commitSync()` call to wait for them. A local `acknowledge()` call records a decision and does not confirm broker acceptance.

| Java `KafkaShareConsumer` | This client |
|---|---|
| `subscribe(Collection)`, `subscription()` | `subscribe({ topics })` while stopped; `subscription()` returns a fresh array |
| `unsubscribe()` | none; `stop()` keeps the names |
| `poll(Duration)` | `run({ eachMessage })` or `run({ eachBatch })` |
| `acknowledge(...)` with `ACCEPT`, `RELEASE` or `REJECT` | `acknowledge(type?)` in `eachMessage`; `acknowledge(message \| { offset }, type?)` in `eachBatch` |
| `AcknowledgeType.RENEW` | `renew()` in the handler payload |
| `commitSync(...)`, `commitAsync()` | none; sending is automatic and settles through the `acknowledge` event |
| `setAcknowledgementCommitCallback(...)` | `share.on(share.events.ACKNOWLEDGE, listener)` |
| `acquisitionLockTimeoutMs()` | `acquisitionLockTimeoutMs()`, `null` until a broker answer carries it |
| `close()`, `close(Duration)` | `disconnect()`, `disconnect({ timeout })` |
| `wakeup()` | none; use `stop()` |
| `metrics()`, `clientInstanceId(Duration)`, metric registration | none on this surface |

There is no callable synchronous flush-and-wait, no clear-subscription method and no subscription replacement while running. stop retains the subscribed names; use stop → subscribe → run to replace them. Automatic acknowledgement sending reports each homogeneous terminal range once; renew results are nonterminal. This sequence does not preserve running-replacement semantics.

For example, outside a handler:

```js
await share.stop()
await share.subscribe({ topics: ['replacement-orders'] })
await share.run({ eachMessage: async ({ message }) => handleOrder(message) })
```

`stop()` leaves the group and retains the subscription. `subscription()` returns a fresh array; changing it does not change the client. There is no `unsubscribe()` operation that clears names. Running `subscribe()` rejects before changing the stored names or sending a replacement heartbeat. This stopped sequence has a membership gap and cannot promise Java's running subscription replacement semantics. `wakeup()` has no separate role because an application thread is not blocked in `poll()`; use `stop()` to end the asynchronous loop.

## Acknowledgement settlement

```js
const remove = share.on(share.events.ACKNOWLEDGE, ({ payload }) => {
  const { topic, partition, firstOffset, lastOffset, type, error } = payload
  recordAcknowledgement({ topic, partition, firstOffset, lastOffset, type, error })
})
```

The payload has exactly `topic`, `partition`, `firstOffset`, `lastOffset`, `type`, `error`. Ranges are inclusive decimal strings; never expand a range into an array or convert offsets to Number. Different outcomes split ranges even when they came from one request. Successful, failed and unconfirmed terminal subsets are disjoint, and each terminal offset is reported once. `error: null` on a terminal acknowledgement confirms the broker result; it is not emitted merely because a handler chose a type. A lost response leaves the remote outcome unknown and must not become success or cause an acknowledgement resend. Renewal reports `type: 'renew'`, is nonterminal and can recur before a terminal decision. A missing partition in an answer is an unconfirmed failure, never inferred success. Invoke the returned `remove()` to stop this listener.

There are 43 ordinary channels and 18 share channels. ShareConsumer emits instrumentation events and diagnostics channels; this package does not create share-consumer spans.

There is no built-in metrics registry or `metrics()` method on this surface. Use the instrumentation events and their diagnostics channels for counters and timing; the event contract supplies observations without creating an OpenTelemetry span. Client instance identity and metric subscription APIs are separate telemetry work; no ShareConsumer telemetry method is added here.

## Administration and resetting offsets

The four share methods are `describeShareGroups`, `listShareGroupOffsets`, `alterShareGroupOffsets`, and `deleteShareGroupOffsets`. See [Admin](Admin.md) for their result shapes, raw authorization masks and per-call deadlines. Use `describeShareGroups` for share membership. `listGroups({ types: ['share'] })` selects share groups and returns lower-case `groupType: 'share'`. The generic `describeGroups` result is broker-defined; use the share-specific method when you need share fields.

Stop every group member before altering offsets. Compose `fetchTopicOffsets` with `alterShareGroupOffsets` for earliest/latest, or `fetchTopicOffsetsByTimestamp` with `alterShareGroupOffsets` for a timestamp. The existing timestamp lookup falls back to the partition's latest offset when the requested timestamp has no match. Offsets remain decimal strings, including values above Number.MAX_SAFE_INTEGER. This is an application recipe, not a new admin method, seek operation or atomic reset across calls. A member can rejoin between inspection and mutation; the broker's refusal remains authoritative. Group errors reject; inspect per-partition errors before treating a reset as successful. Do not automatically retry an unanswered mutation to infer its outcome.

```js
/**
 * Resets one empty share group's selected topic using exact broker offsets.
 * Stop all its members before calling; the broker enforces emptiness at mutation time.
 * Each public call has its own admin deadline; this sequence is not atomic.
 * @param {import('@countly/kafkajs').Admin} admin Connected admin client.
 * @param {string} groupId Empty share group identifier.
 * @param {string} topic Topic name.
 * @param {'earliest'|'latest'|number} position Boundary or nonnegative epoch milliseconds.
 * @returns {Promise<import('@countly/kafkajs').ShareGroupAlterOffsetsResult[]>} Per-partition results.
 * @throws {Error} For invalid input, nonempty/missing group, invalid lookup or broker failure.
 */
async function resetShareOffsets(admin, groupId, topic, position) {
  if (position !== 'earliest' && position !== 'latest' &&
      (!Number.isSafeInteger(position) || position < 0)) {
    throw new Error('Use earliest, latest or nonnegative epoch milliseconds')
  }
  const { groups } = await admin.describeShareGroups([groupId])
  if (groups.length !== 1 || groups[0].groupId !== groupId ||
      groups[0].errorCode !== 0 || groups[0].members.length !== 0) {
    throw new Error('The share group must exist and have no members')
  }
  const rows = typeof position === 'number'
    ? await admin.fetchTopicOffsetsByTimestamp(topic, position)
    : await admin.fetchTopicOffsets(topic)
  const partitions = rows.map((row) => {
    const offset = position === 'earliest' ? row.low :
      position === 'latest' ? row.high : row.offset
    if (typeof offset !== 'string' || !/^(0|[1-9][0-9]*)$/.test(offset) ||
        BigInt(offset) > 9223372036854775807n) {
      throw new Error('The offset lookup must return a nonnegative signed int64 string')
    }
    return { partition: row.partition, offset }
  })
  if (partitions.length === 0) throw new Error('The lookup returned no partitions')
  const results = await admin.alterShareGroupOffsets({
    groupId,
    topics: [{ topic, partitions }],
  })
  for (const result of results) {
    for (const partition of result.partitions) {
      if (partition.errorCode !== 0) {
        throw partition.error || new Error('The broker refused a partition reset')
      }
    }
  }
  return results
}

await resetShareOffsets(admin, 'orders-workers', 'orders', 'earliest')
await resetShareOffsets(admin, 'orders-workers', 'orders', 'latest')
await resetShareOffsets(admin, 'orders-workers', 'orders', 1700000000000)
```

Use `deleteGroups(['orders-workers'])` for an empty share group; no `deleteShareGroups` alias is added. A group with an active member cannot be treated as successfully deleted. `deleteShareGroupOffsets` instead deletes stored offsets for topics to which the group is no longer subscribed. Neither operation is an unsubscribe or a reset of an active member.

## Public surface and test doubles

`ShareConsumer` has `connect`, `disconnect`, `subscribe`, `subscription`, `assignment`, `run`, `stop`, `acquisitionLockTimeoutMs`, `on`, `logger`, and the `events` property. `ShareConsumer` is a TypeScript type and a factory-created client, not a named runtime constructor export. The root CommonJS and ESM packages export `KafkaJSShareGroupsNotSupportedError` and `KafkaJSShareAcknowledgementError`. Hand-written Kafka/Admin test doubles need the added members. Existing ordinary consumer methods, options and event contracts continue to apply to `consumer()`.
