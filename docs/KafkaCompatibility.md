# Kafka protocol compatibility

Generated from the committed Apache Kafka **4.3.1** message definitions in `scripts/compat/messages/4.3.1/`. **Do not edit by hand** — run `npm run compat:generate`; `npm run compat:check` fails when this file and the generator disagree.

Gap classes: `current` (fork max equals the Kafka max) · `behind:N` · `stub` (the key is known but unimplemented) · `missing` (planned, no codec) · `not-planned`. `on-floor` and `below-floor` are reported alongside the class, not instead of it.

## Summary

| API | Key | Kafka 4.3.1 | Flexible from | Fork | Gap | Notes |
|---|--:|---|---|---|---|---|
| Produce | 0 | 3-13 | v9+ | 3-13 | current | v8 per-record errors (KIP-467), v9 flexible, v10 leader hints (KIP-951), v11 TRANSACTION_ABORTABLE (KIP-890), v12 transaction-version-2 implicit add-partitions (KIP-890 part 2), v13 topic IDs (KIP-516, Kafka 4.1; may return UNKNOWN_TOPIC_ID). |
| Fetch | 1 | 4-18 | v12+ | 4-18 | current | v11 rack / preferred read replica (KIP-392), now fully handled: OFFSET_NOT_AVAILABLE (78) skips the partition for one round and keeps the follower, and an OFFSET_OUT_OF_RANGE from a follower drops the preferred replica and re-resolves against the leader. v12 flexible + LastFetchedEpoch (sent at its -1 default; truncation is detected through OffsetForLeaderEpoch instead), v13 topic IDs replace names (KIP-516; a request whose topics are not all known by id is capped at v12), v14 OFFSET_MOVED_TO_TIERED_STORAGE (KIP-405), v15 ReplicaState (KIP-903), v16 leader hints (KIP-951), v17 ReplicaDirectoryId (KIP-853), v18 HighWatermark (KIP-1166). Fetch sessions (KIP-227) are not implemented: every fetch is a full fetch (sessionId 0, sessionEpoch -1). |
| ListOffsets | 2 | 1-11 | v6+ | 1-9 | behind:2 | v4 leader epoch (KIP-320), v6 flexible, v7 max-timestamp (KIP-734), v8 earliest-local offset (KIP-405), v9 last-tiered offset (KIP-1005). v10 (async remote list offsets, KIP-1075) and v11 (earliest pending upload offset, KIP-1023) are deliberate gaps, not a waiver: both are tiered-storage features this client does not use. |
| Metadata | 3 | 0-13 | v9+ | 0-13 | current | v7 leader epoch (KIP-320), v9 flexible, v10+ topic IDs, v13 adds a top-level error code and is where REBOOTSTRAP_REQUIRED surfaces (KIP-1102). |
| OffsetCommit | 8 | 2-10 | v8+ | 2-8 | behind:2 | v6 committed leader epoch (KIP-320), v7 group instance id, v8 flexible, v9 member epoch (KIP-848), v10 topic IDs. |
| OffsetFetch | 9 | 1-10 | v6+ | 1-8 | behind:2 | v5 leader epoch, v6 flexible, v7 require-stable (KIP-447), v8 multi-group batching, v9 member epoch (KIP-848), v10 topic IDs. The fork has no v0 codec, which is correct: v0 read offsets from ZooKeeper. |
| FindCoordinator | 10 | 0-6 | v3+ | 0-6 | current | v3 flexible, v4 multi-key batch (KIP-699), v5 TRANSACTION_ABORTABLE (KIP-890), v6 KeyType SHARE=2 (KIP-932, broker-internal). apiKeys.js still calls key 10 GroupCoordinator, the pre-0.11 name. |
| JoinGroup | 11 | 0-9 | v6+ | 0-5 | behind:4 | v5 encodes groupInstanceId but nothing plumbs it: Broker.joinGroup() has no such parameter, so static membership (KIP-345) is inert today. |
| Heartbeat | 12 | 0-4 | v4+ | 0-3 | behind:1 |  |
| LeaveGroup | 13 | 0-5 | v4+ | 0-3 | behind:2 | v3 batch removal by group instance id (KIP-345): the codec exists, but there is no admin removeMembersFromConsumerGroup to drive it. |
| SyncGroup | 14 | 0-5 | v4+ | 0-3 | behind:2 |  |
| DescribeGroups | 15 | 0-6 | v5+ | 0-2 | behind:4 | Below v6 the broker reports KIP-848 consumer-protocol groups as Dead; v6 adds ErrorMessage. Full coverage also needs ConsumerGroupDescribe (key 69) routing. |
| ListGroups | 16 | 0-5 | v3+ | 0-2 | behind:3 | No GroupState filter (v4) or GroupType field (v5), so consumer-protocol groups cannot be told apart from classic ones. |
| SaslHandshake | 17 | 0-1 | never | 0-1 | current | Never flexible, and the only API in the matrix that is fully current. |
| ApiVersions | 18 | 0-4 | v3+ | 0-4 | current | v0-v3 omit every SupportedFeatures entry whose MinVersion is 0 (KAFKA-17011, fixed in v4 / Kafka 3.9), so a v3-only client sees a truncated feature list. The flexible-protocol pilot targets v4 and falls back to v3 only for brokers below 3.9. |
| CreateTopics | 19 | 2-7 | v5+ | 2-3 | behind:4 | v5 flexible, v7 returns topic IDs. |
| DeleteTopics | 20 | 1-6 | v4+ | 1 | behind:5 (on-floor) | v6 deletes by topic ID. The fork's v1 sits exactly on the 4.x floor. |
| DeleteRecords | 21 | 0-2 | v2+ | 0-1 | behind:1 |  |
| InitProducerId | 22 | 0-6 | v2+ | 0-5 | behind:1 | v2+ is flexible, so even v3 needs the flexible-protocol work first. v3 producer-id/epoch re-init (KIP-360), v4 PRODUCER_FENCED, v5 TRANSACTION_ABORTABLE, v6 two-phase commit (KIP-939) and latestVersionUnstable, so brokers advertise 0-5 by default. |
| OffsetForLeaderEpoch | 23 | 2-4 | v4+ | 3-4 | current | Leader-epoch fencing (KIP-320): v3 and v4 are implemented. v2 is a deliberate gap, not a waiver - versions below 3 require CLUSTER permission, which clients are not typically granted, and the Java consumer pins its own floor at v3 for that reason. |
| AddPartitionsToTxn | 24 | 0-5 | v3+ | 0-1 | behind:4 | Unused under transaction version 2 (KIP-890 part 2), where Produce carries the add-partitions call. |
| AddOffsetsToTxn | 25 | 0-4 | v3+ | 0-1 | behind:3 | Unused under transaction version 2 (KIP-890 part 2). |
| EndTxn | 26 | 0-5 | v3+ | 0-1 | behind:4 | v5 returns a bumped producer id and epoch under transaction version 2. The TV2 signal is the transaction.version finalized feature in ApiVersions v3+, not EndTxn's advertised max version. |
| TxnOffsetCommit | 28 | 0-5 | v3+ | 0-1 | behind:4 | v2 adds the committed leader epoch; v3 is KIP-447 group metadata, which the fork does not implement at all. |
| DescribeAcls | 29 | 1-3 | v2+ | 1 | behind:2 (on-floor) | v1 is the 4.x floor and is also the fork's maximum. |
| CreateAcls | 30 | 1-3 | v2+ | 1 | behind:2 (on-floor) | v1 is the 4.x floor and is also the fork's maximum. |
| DeleteAcls | 31 | 1-3 | v2+ | 1 | behind:2 (on-floor) | v1 is the 4.x floor and is also the fork's maximum. |
| DescribeConfigs | 32 | 1-4 | v4+ | 1-2 | behind:2 |  |
| AlterConfigs | 33 | 0-2 | v2+ | 0-1 | behind:1 | IncrementalAlterConfigs (key 44) is the modern replacement and is unimplemented, so config changes still overwrite rather than merge. |
| SaslAuthenticate | 36 | 0-2 | v2+ | 0-2 | current | v1 adds SessionLifetimeMs (KIP-368 re-authentication), v2 is flexible. The mechanism-provider contract is unchanged at every version: a provider returns INT32-length-prefixed bytes from request.encode() and receives them in response.decode() - v2 strips and re-wraps that prefix inside its own codec. |
| CreatePartitions | 37 | 0-3 | v2+ | 0-1 | behind:2 |  |
| DeleteGroups | 42 | 0-2 | v2+ | 0-1 | behind:1 |  |
| IncrementalAlterConfigs | 44 | 0-1 | v1+ | — | stub | The replacement for AlterConfigs; needed before any admin config change can be merge-semantics rather than overwrite. |
| AlterPartitionReassignments | 45 | 0-1 | v0+ | 0 | behind:1 | One of the only two flexible-encoded codecs in the fork today: it hand-rolls the tag buffer inside the request body because the request header is still hardcoded to v1. It must migrate when real flexible header selection lands. v1 adds AllowReplicationFactorChange. |
| ListPartitionReassignments | 46 | 0 | v0+ | 0 | current | The other hand-rolled flexible codec; same migration applies. Kafka defines only v0, so the fork is current by accident. |
| ConsumerGroupHeartbeat | 68 | 0-1 | v0+ | 0-1 | current | Required for the KIP-848 consumer group protocol (groupProtocol: 'consumer'), GA since Kafka 4.0. |
| ConsumerGroupDescribe | 69 | 0-1 | v0+ | 0-1 | current | Admin-side counterpart of KIP-848; needed so group descriptions stop reporting consumer-protocol groups as Dead. |
| GetTelemetrySubscriptions | 71 | 0 | v0+ | — | not-planned | Client telemetry (KIP-714); deferred. |
| PushTelemetry | 72 | 0 | v0+ | — | not-planned | Client telemetry (KIP-714); deferred. |
| DescribeTopicPartitions | 75 | 0 | v0+ | — | missing | Admin describe at scale, and the only way to read eligible leader replicas. The API does not exist on brokers below 3.8, so the Metadata path must stay as a fallback; ELR arrays are null unless the cluster runs eligible.leader.replicas.version=1. |

## Floor watch

These APIs implement exactly the minimum version the pinned broker still accepts. The next KIP-896 floor raise breaks them with no warning.

| API | Key | Fork max | Kafka 4.3.1 minimum |
|---|--:|--:|--:|
| DeleteTopics | 20 | v1 | v1 |
| DescribeAcls | 29 | v1 | v1 |
| CreateAcls | 30 | v1 | v1 |
| DeleteAcls | 31 | v1 | v1 |

## Below-floor waivers

Versions implemented here that the pinned broker rejects outright: dead code, and dangerous while version negotiation ignores the broker minimum. Each is waived until the phase named below deletes it; an unwaived below-floor version fails `npm run compat:check`.

None — the waiver list is empty.

## Cross-cutting gaps

Not per-API, and not derivable from the message definitions. Each row names the phase that closes it, so this table doubles as a burn-down.

| Gap | Where | Impact | Owner |
|---|---|---|---|
| Topic IDs (UUIDs) are not yet used by every API that accepts them | offset-commit and offset-fetch paths, topic administration | Cluster metadata caches each topic's id, and Fetch and Produce are keyed by it (falling back to names for a request whose ids are not all known). What remains: OffsetCommit v10 and OffsetFetch v10, KIP-848 assignments (phase-6), and topic ids on CreateTopics v7 / DeleteTopics v6 (phase-8). | phase-6 |
| No fetch sessions (KIP-227) | fetch layer | Every fetch is a full request - sessionId 0, sessionEpoch -1, an empty forgotten-topics list - so a poll re-sends every assigned partition rather than only what changed. This is a decision, not an oversight: 3.0.0 ships sessionless (decision D31), and the three fetch-session error codes are mapped defensively so an unexpected one cannot crash a consumer. Incremental fetch is the prerequisite for reducing request size on a large assignment. | backlog |
| Dead message-format v0/v1 write path (KIP-724) | src/protocol/message/v0, src/protocol/message/v1, src/protocol/messageSet encoders | Unreachable against a 4.x broker, which never negotiates Produce below v3 or Fetch below v4. The decoders must stay: old segments can still be returned inside a modern Fetch response. | phase-2 |

## Error table

`src/protocol/error.js` defines 134 codes; Kafka 4.3.1 defines 134 (excluding `NONE`, which is the success sentinel). `retriable` is derived from each error's exception hierarchy upstream and checked against this table: a mismatch is a failure.

- Defined upstream, missing here: **0** (reported, not a failure).
- Defined here, unknown to Kafka 4.3.1: **0** (a failure).
- Type-name mismatches on a shared code: **0** (a failure), plus **5** accepted historical aliases.

Accepted aliases (the KafkaJS name is public API and is deliberately kept):

| Code | This client | Kafka 4.3.1 | Why |
|--:|---|---|---|
| -1 | UNKNOWN | UNKNOWN_SERVER_ERROR | KafkaJS has exposed this name as KafkaJSProtocolError.type since 1.x; renaming it is a breaking public-API change. |
| 6 | NOT_LEADER_FOR_PARTITION | NOT_LEADER_OR_FOLLOWER | Kafka renamed the constant in 2.6 (KIP-392); the KafkaJS name is public API. |
| 14 | GROUP_LOAD_IN_PROGRESS | COORDINATOR_LOAD_IN_PROGRESS | Kafka renamed the constant in 0.11 when coordinators stopped being group-only; the KafkaJS name is public API. |
| 15 | GROUP_COORDINATOR_NOT_AVAILABLE | COORDINATOR_NOT_AVAILABLE | Kafka renamed the constant in 0.11; the KafkaJS name is public API. |
| 16 | NOT_COORDINATOR_FOR_GROUP | NOT_COORDINATOR | Kafka renamed the constant in 0.11; the KafkaJS name is public API. |

## Untracked API keys

Keys this client declares but deliberately does not track in the matrix: they are broker-internal, ZooKeeper-era, or not planned.

| API | Key | apiKeys.js name | Why untracked |
|---|--:|---|---|
| LeaderAndIsr | 4 | LeaderAndIsr | ZooKeeper-era controller-to-broker API; still defined upstream at 4.3.1 but never sent by a client. Stub in requests/index.js. |
| StopReplica | 5 | StopReplica | ZooKeeper-era controller-to-broker API. Stub in requests/index.js. |
| UpdateMetadata | 6 | UpdateMetadata | ZooKeeper-era controller-to-broker API. Stub in requests/index.js. |
| ControlledShutdown | 7 | ControlledShutdown | Broker shutdown handshake, not a client API. Stub in requests/index.js. |
| WriteTxnMarkers | 27 | WriteTxnMarkers | Transaction-coordinator to broker only. Stub in requests/index.js. |
| AlterReplicaLogDirs | 34 | AlterReplicaLogDirs | Log-directory administration; not planned. Stub in requests/index.js. |
| DescribeLogDirs | 35 | DescribeLogDirs | Log-directory administration; not planned. Stub in requests/index.js. |
| CreateDelegationToken | 38 | CreateDelegationToken | Delegation tokens (KIP-48); not planned. Stub in requests/index.js. |
| RenewDelegationToken | 39 | RenewDelegationToken | Delegation tokens (KIP-48); not planned. Stub in requests/index.js. |
| ExpireDelegationToken | 40 | ExpireDelegationToken | Delegation tokens (KIP-48); not planned. Stub in requests/index.js. |
| DescribeDelegationToken | 41 | DescribeDelegationToken | Delegation tokens (KIP-48); not planned. Stub in requests/index.js. |
| ElectLeaders | 43 | ElectPreferredLeaders | Preferred-leader election; not planned. apiKeys.js still uses the pre-2.4 name ElectPreferredLeaders while requests/index.js registers the entry as ElectLeaders, so requests[apiKeys name] is undefined - reported by compat:check, fixed outside Phase 3. |
