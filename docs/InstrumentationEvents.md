---
id: instrumentation-events
title: Instrumentation Events
---

Some operations are instrumented using `EventEmitter`. To receive the events use the method `consumer.on()`, `producer.on()` and `admin.on()`, example:

```javascript
const { HEARTBEAT } = consumer.events
const removeListener = consumer.on(HEARTBEAT, e => console.log(`heartbeat at ${e.timestamp}`))

// Remove the listener by invoking removeListener()
```

Listeners run synchronously, in the code that emits the event, so a slow listener slows the client down. A listener may be an `async` function: nobody awaits it, and a rejection is logged as `Failed to execute listener`. An error a listener throws synchronously is not caught by the client (only the cluster's and the lock's events are guarded), so a listener that can throw should catch its own errors.

Instrumentation Event:

```javascript
{
  id: <Number>,
  type: <String>,
  timestamp: <Number>,
  payload: <Object>
}
```

## <a name="list"> List of available events:

### <a name="consumer"></a> Consumer

| event               | payload                                                                                                                                                                                                       | description |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| REQUEST             | {`broker`, `clientId`, `correlationId`, `size`, `createdAt`, `sentAt`, `pendingDuration`, `duration`, `apiName`, `apiKey`, `apiVersion`} | Emitted on every network request to a broker |
| CONNECT             |                                                                                                                                                                                                               | Consumer connected to a broker. |
| GROUP_JOIN          | {`groupId`, `memberId`, `leaderId`, `isLeader`, `memberAssignment`, `groupProtocol`, `membershipProtocol`, `rebalanceProtocol`, `duration`}                                                                                                                   | Consumer has joined the group. `rebalanceProtocol` is the protocol this consumer selected from its assigners, `'eager'` or `'cooperative'`. `groupProtocol` is unchanged — still the name of the assignor selected for the group; under the consumer protocol that is the server-side assignor's name once the coordinator has named one, and `''` before then. `membershipProtocol` is the membership protocol this member joined with, `'classic'` or `'consumer'` (the `groupProtocol` **option**). Under `'consumer'` the group has no leader, so `leaderId` is `null`, `isLeader` is `false`, and `rebalanceProtocol` is reported as `'cooperative'`. |
| FETCH_START         | {`nodeId`}                                                                                                                                                                                                          | Starting to fetch messages from brokers. |
| FETCH               | {`numberOfBatches`, `duration`, `nodeId`}                                                                                                                                                                           | Finished fetching messages from the brokers. |
| START_BATCH_PROCESS | {`topic`, `partition`, `highWatermark`, `offsetLag`, `offsetLagLow`, `batchSize`, `firstOffset`, `lastOffset`}                                                                                | Starting user processing of a batch of messages. |
| END_BATCH_PROCESS   | {`topic`, `partition`, `highWatermark`, `offsetLag`, `offsetLagLow`, `batchSize`, `firstOffset`, `lastOffset`, `duration`}                                                                  | Finished processing a batch. This includes user-land processing via `eachMessage`/`eachBatch`. |
| COMMIT_OFFSETS      | {`groupId`, `memberId`, `groupGenerationId`, `groupInstanceId`, `topics`}                                                                                                                                                | Committed offsets. `groupInstanceId` (KIP-345 static membership) is `null` unless the consumer was created with a `groupInstanceId`. Each entry of `topics[].partitions` is `{partition, offset, leaderEpoch}` for an auto-commit or a `commitOffsets()` with no arguments — `leaderEpoch` is the leader epoch of the record at that offset, and `-1` when no record backs it. Offsets you pass to `commitOffsets(offsets)` are forwarded as given and carry no `leaderEpoch`. |
| STOP                |                                                                                                                                                                                                               | Consumer has stopped. |
| DISCONNECT          |                                                                                                                                                                                                               | Consumer has disconnected. |
| CRASH               | {`error`, `groupId`, `restart`}                                                                                                                                                                                      | Consumer has crashed. CRASH fires exactly once for every terminal death of the runner, and `restart` is the decision that was actually taken (whether the consumer will restart itself or stop and exit). `consumer.run()` resolves as soon as the runner starts, so its resolution does **not** mean the consumer is still alive — CRASH is the supervision signal to listen to for that. If your application wants to react to the error, such as by cleanly shutting down resources,</br>restarting the consumer itself, or exiting the process entirely, it should listen to the CRASH event. |
| HEARTBEAT           | {`groupId`, `memberId`, `groupGenerationId`, `groupInstanceId`}                                                                                                                                          | Heartbeat sent to the coordinator. Fires once per heartbeat actually sent, whether it came from the background heartbeat timer or from the `heartbeat()` callback in your handler. Under `groupProtocol: 'consumer'` it fires once per ConsumerGroupHeartbeat **sent**, and `groupGenerationId` carries the member epoch that request went out under. `groupInstanceId` (KIP-345 static membership) is `null` unless the consumer was created with a `groupInstanceId`. |
| REBALANCING         | {`groupId`, `memberId`, `membershipProtocol`, `reason`}                                                                                                                                                            | Consumer Group has started rebalancing. `membershipProtocol` is `'classic'` or `'consumer'` and is carried by every emit of this event. `reason` is one of `rebalance-in-progress`, `lost`, `poll-interval-exceeded`, `partitions-revoked`, `stale-assignment`, `coordinator-changed`, `assignment-changed` (the coordinator sent this member a different assignment) or `fenced` (the coordinator rejected this member's epoch or id, so it rejoins). The last two occur only under `'consumer'`. The **first** join of a run emits no REBALANCING at all, so every event you receive describes a rejoin and carries a reason. `memberId` is `null` when the coordinator rejected this member's id (`UNKNOWN_MEMBER_ID`), because the id is dropped before the rejoin is reported. |
| PARTITIONS_ASSIGNED | {`groupId`, `memberId`, `generationId`, `partitions`} | Partitions this member gained in the rebalance that just completed. `partitions` is `[{topic, partitions: number[]}]`. Fires on the default eager path too, where it carries the whole new assignment, and after a round that recovers lost partitions, where it also carries the whole new assignment. |
| PARTITIONS_REVOKED  | {`groupId`, `memberId`, `generationId`, `partitions`} | Partitions this member gave up in the rebalance that just completed, emitted before PARTITIONS_ASSIGNED. On the eager path this is the whole previous assignment. A round that recovers partitions the member LOST emits no PARTITIONS_REVOKED at all — PARTITIONS_LOST has already named them. |
| PARTITIONS_LOST     | {`groupId`, `memberId`, `generationId`, `partitions`} | This member's whole assignment was taken away without a chance to commit — the coordinator invalidated its generation, member id or epoch, or the poll-interval guard fired. `partitions` is the **entire** owned set, and **no commit is issued** for it: the membership those offsets were resolved under is gone. Always emitted before the member rejoins — under `groupProtocol: 'classic'` before the JoinGroup of the recovery round, under `'consumer'` before the joining heartbeat — so in-flight work can be dropped. Contrast PARTITIONS_REVOKED, which is a rebalance or a reconciliation taking partitions away and **is** preceded by a commit when `autoCommit` is on. `memberId` is `null` when the id was the thing the coordinator rejected. |
| POLL_INTERVAL_EXCEEDED | {`groupId`, `memberId`, `generationId`, `maxPollInterval`, `elapsedMs`, `isStatic`} | The handler took longer than `maxPollInterval` to return. `elapsedMs` is the measured gap; `isStatic` says whether this is a static member (`groupInstanceId` is set). Under `groupProtocol: 'classic'` that means no LeaveGroup was sent at all and the member keeps its group slot until the session timeout; under `'consumer'` a leave heartbeat carrying epoch −2 is sent even for a static member. There is no `partitions` field: the partitions lost are exactly what the following PARTITIONS_LOST reports. `memberId` is `null` if this member has not joined successfully yet. The deadline is not evaluated while a rebalance round is in flight: a join the coordinator holds open is not handler time, and the round restarts the deadline when it completes. |
| REQUEST_TIMEOUT     | {`broker`, `clientId`, `correlationId`, `createdAt`, `sentAt`, `pendingDuration`, `apiName`, `apiKey`, `apiVersion`}                                 | Request to a broker has timed out. |
| REQUEST_QUEUE_SIZE  | {`broker`, `clientId`, `queueSize`}                                                                                                                                                      | All requests go through a request queue where concurrency is managed (`maxInflightRequests`). Whenever the size of the queue changes, this event is emitted. |
| RECEIVED_UNSUBSCRIBED_TOPICS | {`groupId`, `generationId`, `memberId`, `assignedTopics`, `topicsSubscribed`, `topicsNotSubscribed`} | Event emitted when some members of your consumer group are subscribed to some topics, and some other members of the group are subscribed to a different set of topics. |
| AUTHENTICATION_FAILURE | {`broker`, `mechanism`, `errorMessage`} | A broker rejected SASL authentication (`SASL_AUTHENTICATION_FAILED`), carrying its own error message. The broker is then blacked out for `max(authenticationTimeout, 1000ms)` — 10s out of the box, since `authenticationTimeout` defaults to 10000ms — so a (re)authentication attempt against it fails fast with `KafkaJSSASLAuthenticationError` instead of retrying immediately; an already-authenticated connection is unaffected. |
| TOPIC_RECREATED     | {`topic`, `previousTopicId`, `topicId`}                                                    | A subscribed topic came back from a metadata refresh with a different topic id — it was deleted and recreated. The consumer resets that topic's offsets once, per its subscription's `fromBeginning`. |
| REBOOTSTRAP         | {`reason`}                                                                                 | The client rebuilt its connections and metadata from the configured broker list (KIP-899/1102). `reason` is `'timeout'` (metadata failed continuously for `metadataRecoveryRebootstrapTriggerMs`), `'required'` (a broker returned `REBOOTSTRAP_REQUIRED`), or `'all-brokers-unavailable'` (every known broker became unreachable). Only fires when `metadataRecoveryStrategy` is `'rebootstrap'` (the default). |
| LOG_TRUNCATION      | {`topic`, `partition`, `offset`, `divergingOffset`, `leaderEpoch`} | The consumer detected that the partition's log diverged from what it had consumed, after a leader change. `offset` is where the consumer was; `divergingOffset` is where it resumes (`null` when the divergence point could not be determined and the partition was reset per its `fromBeginning` setting). `leaderEpoch` is the epoch the leader answered with, `-1` when it could not determine one. |

### <a name="producer"></a> Producer

| event               | payload                                                                                                                                                                                                       | description |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| REQUEST             | {`broker`, `clientId`, `correlationId`, `size`, `createdAt`, `sentAt`, `pendingDuration`, `duration`, `apiName`, `apiKey`, `apiVersion`} | Emitted on every network request to a broker. |
| CONNECT            |                                                                                                                                                                                                               | Producer connected to a broker. |
| INITIALIZED        |                                                                                                                                                                                                               | An idempotent producer has obtained a producer id — safe to `send()`. Only emitted for idempotent producers (there's nothing to initialize otherwise); fires again after a failed `InitProducerId` self-heals on a later `send()`/`sendBatch()`. |
| DISCONNECT         |                                                                                                                                                                                                               | Producer has disconnected. |
| REQUEST_TIMEOUT    | {`broker`, `clientId`, `correlationId`, `createdAt`, `sentAt`, `pendingDuration`, `apiName`, `apiKey`, `apiVersion`}                                 | Request to a broker has timed out. |
| REQUEST_QUEUE_SIZE | {`broker`, `clientId`, `queueSize`}                                                                                                                                                      | All requests go through a request queue where concurrency is managed (`maxInflightRequests`). Whenever the size of the queue changes, this event is emitted. |
| AUTHENTICATION_FAILURE | {`broker`, `mechanism`, `errorMessage`} | A broker rejected SASL authentication (`SASL_AUTHENTICATION_FAILED`), carrying its own error message. The broker is then blacked out for `max(authenticationTimeout, 1000ms)` — 10s out of the box, since `authenticationTimeout` defaults to 10000ms — so a (re)authentication attempt against it fails fast with `KafkaJSSASLAuthenticationError` instead of retrying immediately; an already-authenticated connection is unaffected. |
| LOCK_TIMEOUT | {`description`, `waiting`, `timeout`} | No longer emitted: the producer sends without a per-broker lock, so no producer listener for it is ever called. Until 3.0.0 an idempotent or transactional producer serialized its requests to each broker behind a mutex (a 65535ms timeout) and emitted this, right before rejecting with `KafkaJSLockTimeout`, when a caller waited longer than that; the event stays declared and its payload is documented for code that still subscribes. `description` was the broker's address (`host:port`), `waiting` the number of callers still queued (including the one that timed out), `timeout` the configured wait in milliseconds. `KafkaJSLockTimeout` itself is still raised by connection and metadata paths. |
| TOPIC_RECREATED     | {`topic`, `previousTopicId`, `topicId`}                                                    | A topic this client had metadata for came back from a metadata refresh with a different topic id — it was deleted and recreated. Observational: this client takes no action of its own. (A consumer additionally resets that topic's offsets once — see the consumer table.) |
| REBOOTSTRAP         | {`reason`}                                                                                 | The client rebuilt its connections and metadata from the configured broker list (KIP-899/1102). `reason` is `'timeout'` (metadata failed continuously for `metadataRecoveryRebootstrapTriggerMs`), `'required'` (a broker returned `REBOOTSTRAP_REQUIRED`), or `'all-brokers-unavailable'` (every known broker became unreachable). Only fires when `metadataRecoveryStrategy` is `'rebootstrap'` (the default). |


### <a name="admin"></a> Admin

| event               | payload                                                                                                                                                                                                       | description |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| REQUEST             | {`broker`, `clientId`, `correlationId`, `size`, `createdAt`, `sentAt`, `pendingDuration`, `duration`, `apiName`, `apiKey`, `apiVersion`} | Emitted on every network request to a broker. |
| CONNECT            |                                                                                                                                                                                                               | Admin client connected to a broker |
| DISCONNECT         |                                                                                                                                                                                                               | Admin client has disconnected. |
| REQUEST_TIMEOUT    | {`broker`, `clientId`, `correlationId`, `createdAt`, `sentAt`, `pendingDuration`, `apiName`, `apiKey`, `apiVersion`}                                 | Request to a broker has timed out. |
| REQUEST_QUEUE_SIZE | {`broker`, `clientId`, `queueSize`}                                                                                                                                                      | All requests go through a request queue where concurrency is managed (`maxInflightRequests`). Whenever the size of the queue changes, this event is emitted. |
| AUTHENTICATION_FAILURE | {`broker`, `mechanism`, `errorMessage`} | A broker rejected SASL authentication (`SASL_AUTHENTICATION_FAILED`), carrying its own error message. The broker is then blacked out for `max(authenticationTimeout, 1000ms)` — 10s out of the box, since `authenticationTimeout` defaults to 10000ms — so a (re)authentication attempt against it fails fast with `KafkaJSSASLAuthenticationError` instead of retrying immediately; an already-authenticated connection is unaffected. |
| TOPIC_RECREATED     | {`topic`, `previousTopicId`, `topicId`}                                                    | A topic this client had metadata for came back from a metadata refresh with a different topic id — it was deleted and recreated. Observational: this client takes no action of its own. (A consumer additionally resets that topic's offsets once — see the consumer table.) |
| REBOOTSTRAP         | {`reason`}                                                                                 | The client rebuilt its connections and metadata from the configured broker list (KIP-899/1102). `reason` is `'timeout'` (metadata failed continuously for `metadataRecoveryRebootstrapTriggerMs`), `'required'` (a broker returned `REBOOTSTRAP_REQUIRED`), or `'all-brokers-unavailable'` (every known broker became unreachable). Only fires when `metadataRecoveryStrategy` is `'rebootstrap'` (the default). |

## <a name="channels"></a> Channels (`node:diagnostics_channel`)

Every event above is also published on [`node:diagnostics_channel`](https://nodejs.org/api/diagnostics_channel.html), on the channel named `kafkajs.` plus the event's name: the consumer's `REQUEST` is on `kafkajs.consumer.network.request`. A library can then observe every client in the process (a metrics layer, a tracer) without holding a reference to any of them. The 42 channels exist from the moment the package is loaded, and while a channel has no subscriber and the event has no `on()` listener nothing is allocated and nothing is published.

```javascript
const diagnosticsChannel = require('node:diagnostics_channel')

diagnosticsChannel.subscribe('kafkajs.consumer.end_batch_process', (event) => {
  const { topic, batchSize, duration } = event.payload
  console.log(`${topic}: ${batchSize} messages handled in ${duration} ms`)
})
```

The message is the instrumentation event an `on()` listener receives, the same object: `{ id, type, timestamp, payload }` with `type` set to the event's name, `consumer.end_batch_process` here. When an event has both a subscriber and an `on()` listener, the channel's subscribers run first. Use `diagnosticsChannel.subscribe(name, fn)` to subscribe and `diagnosticsChannel.unsubscribe(name, fn)` to stop.

A subscriber:

- runs synchronously, inside the code that emits the event (the fetch loop, the request queue), so it must return quickly and must not wait on anything;
- must not throw: Node reports an error thrown by a subscriber as an `'uncaughtException'`, which ends the process unless the application handles it;
- must treat the message as read-only: the payload is the object the client built, and some payloads (the offset commit's, the group join's) are objects the client goes on using;
- must not retain the message or its payload after it returns: copy what it needs.

A subscriber is process-wide and receives the events of every client in the process. The request events (`network.request`, `network.request_timeout` and `network.request_queue_size`) carry `clientId` and `broker`, and the consumer's group events (`group_join`, `heartbeat`, `commit_offsets`, `rebalancing`, `crash`, the partition events and `poll_interval_exceeded`) carry `groupId`; an event such as `consumer.fetch` or `consumer.start_batch_process` carries neither, so two clients in one process cannot be told apart by it.

The payloads are those in the tables above.

### Consumer channels

| channel | event |
|---------|-------|
| `kafkajs.consumer.heartbeat` | `consumer.events.HEARTBEAT` |
| `kafkajs.consumer.log_truncation` | `consumer.events.LOG_TRUNCATION` |
| `kafkajs.consumer.commit_offsets` | `consumer.events.COMMIT_OFFSETS` |
| `kafkajs.consumer.group_join` | `consumer.events.GROUP_JOIN` |
| `kafkajs.consumer.fetch` | `consumer.events.FETCH` |
| `kafkajs.consumer.fetch_start` | `consumer.events.FETCH_START` |
| `kafkajs.consumer.start_batch_process` | `consumer.events.START_BATCH_PROCESS` |
| `kafkajs.consumer.end_batch_process` | `consumer.events.END_BATCH_PROCESS` |
| `kafkajs.consumer.connect` | `consumer.events.CONNECT` |
| `kafkajs.consumer.disconnect` | `consumer.events.DISCONNECT` |
| `kafkajs.consumer.stop` | `consumer.events.STOP` |
| `kafkajs.consumer.crash` | `consumer.events.CRASH` |
| `kafkajs.consumer.rebalancing` | `consumer.events.REBALANCING` |
| `kafkajs.consumer.partitions_assigned` | `consumer.events.PARTITIONS_ASSIGNED` |
| `kafkajs.consumer.partitions_revoked` | `consumer.events.PARTITIONS_REVOKED` |
| `kafkajs.consumer.partitions_lost` | `consumer.events.PARTITIONS_LOST` |
| `kafkajs.consumer.poll_interval_exceeded` | `consumer.events.POLL_INTERVAL_EXCEEDED` |
| `kafkajs.consumer.received_unsubscribed_topics` | `consumer.events.RECEIVED_UNSUBSCRIBED_TOPICS` |
| `kafkajs.consumer.network.request` | `consumer.events.REQUEST` |
| `kafkajs.consumer.network.request_timeout` | `consumer.events.REQUEST_TIMEOUT` |
| `kafkajs.consumer.network.request_queue_size` | `consumer.events.REQUEST_QUEUE_SIZE` |
| `kafkajs.consumer.network.authentication_failure` | `consumer.events.AUTHENTICATION_FAILURE` |
| `kafkajs.consumer.cluster.topic_recreated` | `consumer.events.TOPIC_RECREATED` |
| `kafkajs.consumer.cluster.rebootstrap` | `consumer.events.REBOOTSTRAP` |

### Producer channels

| channel | event |
|---------|-------|
| `kafkajs.producer.connect` | `producer.events.CONNECT` |
| `kafkajs.producer.initialized` | `producer.events.INITIALIZED` |
| `kafkajs.producer.disconnect` | `producer.events.DISCONNECT` |
| `kafkajs.producer.network.request` | `producer.events.REQUEST` |
| `kafkajs.producer.network.request_timeout` | `producer.events.REQUEST_TIMEOUT` |
| `kafkajs.producer.network.request_queue_size` | `producer.events.REQUEST_QUEUE_SIZE` |
| `kafkajs.producer.network.authentication_failure` | `producer.events.AUTHENTICATION_FAILURE` |
| `kafkajs.producer.network.lock_timeout` | `producer.events.LOCK_TIMEOUT` |
| `kafkajs.producer.cluster.topic_recreated` | `producer.events.TOPIC_RECREATED` |
| `kafkajs.producer.cluster.rebootstrap` | `producer.events.REBOOTSTRAP` |

### Admin channels

| channel | event |
|---------|-------|
| `kafkajs.admin.connect` | `admin.events.CONNECT` |
| `kafkajs.admin.disconnect` | `admin.events.DISCONNECT` |
| `kafkajs.admin.network.request` | `admin.events.REQUEST` |
| `kafkajs.admin.network.request_timeout` | `admin.events.REQUEST_TIMEOUT` |
| `kafkajs.admin.network.request_queue_size` | `admin.events.REQUEST_QUEUE_SIZE` |
| `kafkajs.admin.network.authentication_failure` | `admin.events.AUTHENTICATION_FAILURE` |
| `kafkajs.admin.cluster.topic_recreated` | `admin.events.TOPIC_RECREATED` |
| `kafkajs.admin.cluster.rebootstrap` | `admin.events.REBOOTSTRAP` |
