---
id: admin
title: Admin Client
---

## Admin lifecycle

`admin.disconnect({ timeout })` closes by the deadline. `timeout` is an integer from 0 to
2147483647 milliseconds; zero starts no new wire work. With no timeout, the admin drains
calls already admitted. An invalid object timeout rejects `KafkaJSInvalidConfigurationError`
before changing the lifecycle. Legacy non-object signal or callback arguments are ignored.
Concurrent closes share the first valid call's deadline and one cleanup episode.

Admin operations after disconnect reject without dialing until an explicit connect opens a new lifecycle.
Call `await admin.connect()` to resume. A connect issued during close joins its cleanup first.
An old lookup, authentication, encoder, queued write, response or temporary consumer cannot
send, reconnect or clear the replacement lifecycle. A forced close reports an unknown outcome;
a mutation already sent may have been applied. Inspect the broker state before repeating it.

## Admin API ledger

The following rows cover the complete public Admin method surface and its direct options.
Array item fields and positional arguments are described in the method sections. Every wire
family has four evidence kinds and exactly one live arm in the family table. These references
are the test contract; acceptance requires the recorded outcomes on the tested artifact.
`createTopics` returns a boolean, and legacy alterConfigs retains its compatibility contract.
No file's presence proves a live run passed.

| Public method | Direct options on any overload | Family |
|---|---|---|
| `describeProducers` | apiTimeout, brokerId, topicPartitions | transactions |
| `describeTransactions` | apiTimeout, transactionalIds | transactions |
| `listTransactions` | apiTimeout, durationFilter, producerIds, states, transactionalIdPattern | transactions |
| `abortTransaction` | apiTimeout, coordinatorEpoch, partition, producerEpoch, producerId, topic | transactions |
| `fenceProducers` | apiTimeout, transactionTimeout, transactionalIds | transactions |
| `connect` | none | lifecycle |
| `disconnect` | timeout | lifecycle |
| `clientInstanceId` | timeout | lifecycle |
| `listTopics` | apiTimeout | metadata |
| `createTopics` | apiTimeout, timeout, topics, validateOnly, waitForLeaders | topics |
| `deleteTopics` | apiTimeout, timeout, topicIds, topics | topics |
| `createPartitions` | apiTimeout, timeout, topicPartitions, validateOnly | topics |
| `fetchTopicMetadata` | apiTimeout, includeTopicAuthorizedOperations, topics, useDescribeTopicPartitions | metadata |
| `deleteOffsets` | apiTimeout, groupId, topics | offsets |
| `fetchOffsetsForGroups` | apiTimeout, groups, requireStable | offsets |
| `fetchOffsets` | apiTimeout, groupId, requireStable, resolveOffsets, topics | offsets |
| `describeLogDirs` | apiTimeout, brokers, topicPartitions | log directories |
| `alterReplicaLogDirs` | apiTimeout, replicas | log directories |
| `describeReplicaLogDirs` | apiTimeout, replicas | log directories |
| `fetchTopicOffsets` | apiTimeout, isolationLevel | offsets |
| `fetchTopicOffsetsByTimestamp` | apiTimeout, isolationLevel | offset specifications |
| `electLeaders` | apiTimeout, electionType, topicPartitions | cluster and features |
| `describeFeatures` | apiTimeout, brokerId | cluster and features |
| `updateFeatures` | apiTimeout, features, validateOnly | cluster and features |
| `listConfigResources` | apiTimeout, resourceTypes | cluster and features |
| `unregisterController` | apiTimeout, controllerId | cluster and features |
| `describeCluster` | apiTimeout, includeAuthorizedOperations, includeFencedBrokers | cluster and features |
| `setOffsets` | apiTimeout, groupId, partitions, topic | offsets |
| `resetOffsets` | apiTimeout, earliest, groupId, topic | offsets |
| `describeConfigs` | apiTimeout, includeDocumentation, includeSynonyms, resources | configuration |
| `alterConfigs` | apiTimeout, resources, validateOnly | configuration |
| `incrementalAlterConfigs` | apiTimeout, resources, validateOnly | configuration |
| `listGroups` | apiTimeout, states, types | groups |
| `deleteGroups` | apiTimeout | groups |
| `removeMembersFromConsumerGroup` | apiTimeout, groupId, members, reason | groups |
| `describeGroups` | apiTimeout, includeAuthorizedOperations | groups |
| `describeShareGroups` | apiTimeout, includeAuthorizedOperations | groups |
| `listShareGroupOffsets` | apiTimeout, groupId, topics | groups |
| `alterShareGroupOffsets` | apiTimeout, groupId, topics | groups |
| `deleteShareGroupOffsets` | apiTimeout, groupId, topics | groups |
| `describeAcls` | apiTimeout, host, operation, permissionType, principal, resourceName, resourcePatternType, resourceType | ACLs |
| `deleteAcls` | apiTimeout, filters | ACLs |
| `createAcls` | acl, apiTimeout | ACLs |
| `deleteTopicRecords` | apiTimeout, partitions, topic | offsets |
| `alterPartitionReassignments` | allowReplicationFactorChange, apiTimeout, timeout, topics | reassignment |
| `listPartitionReassignments` | apiTimeout, timeout, topics | reassignment |
| `describeClientQuotas` | apiTimeout, components, strict | client quotas |
| `alterClientQuotas` | apiTimeout, entries, validateOnly | client quotas |
| `describeUserScramCredentials` | apiTimeout, users | SCRAM credentials |
| `alterUserScramCredentials` | apiTimeout, deletions, upsertions | SCRAM credentials |
| `createDelegationToken` | apiTimeout, maxLifetimeMs, owner, renewers | delegation tokens |
| `renewDelegationToken` | apiTimeout, hmac, renewPeriodMs | delegation tokens |
| `expireDelegationToken` | apiTimeout, expiryTimePeriodMs, hmac | delegation tokens |
| `describeDelegationToken` | apiTimeout, owners | delegation tokens |
| `logger` | none | instrumentation |
| `on` | none | instrumentation |
| `registerMetricForSubscription` | attributes, name, type, unit, value | instrumentation |
| `unregisterMetricFromSubscription` | attributes, name | instrumentation |

## Admin evidence by family

Goldens include every negotiated version's independent request and response body fixtures,
registered provenance and the protocol inventory check. The capability and partial-result
columns name seam witnesses; inherited contracts also run in the cold admin group. Each
live script below is a single owner even when it contains several cases or fixture controls.

| Family | Goldens | Capability and downgrade | Partial result and error | Live arm |
|---|---|---|---|---|
| topics | src/protocol/requests/createTopics/; src/protocol/requests/deleteTopics/; src/protocol/requests/createPartitions/ | src/admin/__tests__/createTopicsWait.spec.js | src/admin/__tests__/aggregateOutcome.spec.js | test:e2e:admin-ledger |
| metadata | src/protocol/requests/metadata/; src/protocol/requests/describeTopicPartitions/ | src/admin/__tests__/authorizedOperations.spec.js | src/admin/__tests__/describeTopicPartitionsAdmin.spec.js | test:e2e:admin-ledger |
| ACLs | src/protocol/requests/describeAcls/; src/protocol/requests/createAcls/; src/protocol/requests/deleteAcls/ | src/admin/__tests__/aclResourceUser.spec.js | src/admin/__tests__/errorMessage.spec.js | test:e2e:admin-ledger |
| configuration | src/protocol/requests/describeConfigs/; src/protocol/requests/alterConfigs/; src/protocol/requests/incrementalAlterConfigs/ | src/admin/__tests__/configRouting.spec.js | src/admin/__tests__/validateOnly.spec.js | test:e2e:admin-ledger |
| groups | src/protocol/requests/describeGroups/; src/protocol/requests/listGroups/; src/protocol/requests/deleteGroups/; src/protocol/requests/leaveGroup/; src/protocol/requests/shareGroupDescribe/; src/protocol/requests/describeShareGroupOffsets/; src/protocol/requests/alterShareGroupOffsets/; src/protocol/requests/deleteShareGroupOffsets/ | src/admin/__tests__/describeGroupsRouting.spec.js; src/admin/__tests__/shareGroups.spec.js | src/admin/__tests__/adminGroupApis.spec.js; src/admin/__tests__/shareGroups.spec.js | test:e2e:admin-ledger |
| offsets | src/protocol/requests/offsetFetch/; src/protocol/requests/offsetCommit/; src/protocol/requests/offsetDelete/; src/protocol/requests/listOffsets/; src/protocol/requests/deleteRecords/ | src/admin/__tests__/fetchOffsetsIsolation.spec.js | src/admin/__tests__/fetchOffsetsForGroups.spec.js | test:e2e:admin-ledger |
| offset specifications | src/protocol/requests/listOffsets/ | src/admin/__tests__/listOffsetsSpecs.spec.js | src/admin/__tests__/listOffsetsSpecs.spec.js | test:e2e:admin-tiered |
| reassignment | src/protocol/requests/alterPartitionReassignments/; src/protocol/requests/listPartitionReassignments/ | src/admin/__tests__/alterPartitionReassignmentsV1.spec.js | src/admin/__tests__/alterPartitionReassignmentsV1.spec.js | test:e2e:admin-ledger |
| transactions | src/protocol/requests/describeProducers/; src/protocol/requests/describeTransactions/; src/protocol/requests/listTransactions/; src/protocol/requests/writeTxnMarkers/; src/protocol/requests/initProducerId/ | src/admin/__tests__/transactionDiagnostics.spec.js | src/admin/__tests__/abortFence.spec.js | test:e2e:admin-ledger |
| cluster and features | src/protocol/requests/describeCluster/; src/protocol/requests/electLeaders/; src/protocol/requests/apiVersions/; src/protocol/requests/updateFeatures/; src/protocol/requests/listConfigResources/; src/protocol/requests/unregisterController/ | src/admin/__tests__/features.spec.js | src/admin/__tests__/configResources.spec.js | test:e2e:admin-ledger |
| log directories | src/protocol/requests/describeLogDirs/; src/protocol/requests/alterReplicaLogDirs/ | src/admin/__tests__/logDirs.spec.js | src/admin/__tests__/logDirs.spec.js | test:e2e:admin-logdirs |
| client quotas | src/protocol/requests/describeClientQuotas/; src/protocol/requests/alterClientQuotas/ | src/admin/__tests__/clientQuotas.spec.js | src/admin/__tests__/securityAdminFailures.spec.js | test:e2e:admin-quota |
| SCRAM credentials | src/protocol/requests/describeUserScramCredentials/; src/protocol/requests/alterUserScramCredentials/ | src/admin/__tests__/scramCredentials.spec.js | src/admin/__tests__/securityAdminFailures.spec.js | test:e2e:admin-ledger |
| delegation tokens | src/protocol/requests/createDelegationToken/; src/protocol/requests/renewDelegationToken/; src/protocol/requests/expireDelegationToken/; src/protocol/requests/describeDelegationToken/ | src/admin/__tests__/delegationTokens.spec.js | src/admin/__tests__/delegationTokens.spec.js | test:e2e:admin-tokens |
| lifecycle | not applicable: no wire codec | src/admin/__tests__/lifecycle.spec.js | src/admin/__tests__/lifecycle.spec.js | test:e2e:admin-lifecycle |
| instrumentation | not applicable: no wire codec | src/admin/__tests__/lifecycle.spec.js | src/admin/__tests__/lifecycle.spec.js | test:e2e:admin-lifecycle |

## Java administration dispositions

This inventory follows Apache Kafka 4.3.1 Admin, including deprecated aliases, plus the 4.4 unregisterController method. Every method
also has the inherited AbstractOptions timeout unless the row states an excluded or deferred
surface. Share administration is implemented; Streams administration
and quorum/voter administration are excluded from this package. Client telemetry belongs to
the telemetry extension; JVM metric registration has no JavaScript equivalent.

| Java method or configuration | Disposition | JavaScript equivalent or reason | Java options and differences |
|---|---|---|---|
| abortTransaction | implemented | abortTransaction | AbortTransactionSpec producer identity; partition; coordinator epoch |
| addRaftVoter | excluded | quorum and voter administration excluded | not implemented |
| alterClientQuotas | implemented | alterClientQuotas | validateOnly; exact/default/any entity matching |
| alterConsumerGroupOffsets | equivalent | setOffsets / resetOffsets | ordinary method fields and apiTimeout; no semantic option is silently dropped |
| alterPartitionReassignments | implemented | alterPartitionReassignments | allowReplicationFactorChange; timeout |
| alterReplicaLogDirs | implemented | alterReplicaLogDirs | replicas |
| alterShareGroupOffsets | implemented | alterShareGroupOffsets | group id; topic partitions with exact decimal offsets; apiTimeout; a partition error is data and a group error throws |
| alterStreamsGroupOffsets | excluded | Streams-group administration excluded | not implemented |
| alterUserScramCredentials | implemented | alterUserScramCredentials | upsertions; deletions; salt; iterations; mechanism |
| clientInstanceId | telemetry | client telemetry or JVM-only metrics | not part of admin RPC parity |
| close | equivalent | disconnect({ timeout }) | natural drain or integer timeout; final no-argument overload |
| createAcls | implemented | createAcls | ordinary method fields and apiTimeout; no semantic option is silently dropped |
| createDelegationToken | implemented | createDelegationToken | owner; renewers; maxLifetimeMs |
| createPartitions | implemented | createPartitions | validateOnly; existing quota retry |
| createTopics | implemented | createTopics | validateOnly; retryOnQuotaViolation uses existing quota retry; timeoutMs maps to apiTimeout; broker timeout stays timeout |
| deleteAcls | implemented | deleteAcls | ordinary method fields and apiTimeout; no semantic option is silently dropped |
| deleteConsumerGroupOffsets | equivalent | deleteOffsets | ordinary method fields and apiTimeout; no semantic option is silently dropped |
| deleteConsumerGroups | equivalent | deleteGroups | ordinary method fields and apiTimeout; no semantic option is silently dropped |
| deleteRecords | equivalent | deleteTopicRecords | ordinary method fields and apiTimeout; no semantic option is silently dropped |
| deleteShareGroupOffsets | implemented | deleteShareGroupOffsets | group id; topic names; apiTimeout; a topic error is data and a group error throws |
| deleteShareGroups | equivalent | deleteGroups | no separate method; the existing call takes the group id and apiTimeout |
| deleteStreamsGroupOffsets | excluded | Streams-group administration excluded | not implemented |
| deleteStreamsGroups | excluded | Streams-group administration excluded | not implemented |
| deleteTopics | implemented | deleteTopics | name or topic ID; retryOnQuotaViolation uses existing quota retry; timeoutMs maps to apiTimeout |
| describeAcls | implemented | describeAcls | ordinary method fields and apiTimeout; no semantic option is silently dropped |
| describeClassicGroups | equivalent | describeGroups | includeAuthorizedOperations; same classic group description facade |
| describeClientQuotas | implemented | describeClientQuotas | components; strict |
| describeCluster | implemented | describeCluster | includeAuthorizedOperations; includeFencedBrokers |
| describeConfigs | implemented | describeConfigs | includeSynonyms; includeDocumentation; resource type routing |
| describeConsumerGroups | equivalent | describeGroups | includeAuthorizedOperations |
| describeDelegationToken | implemented | describeDelegationToken | owners |
| describeFeatures | implemented | describeFeatures | nodeId maps to brokerId |
| describeLogDirs | implemented | describeLogDirs | brokers; topicPartitions |
| describeMetadataQuorum | excluded | quorum and voter administration excluded | not implemented |
| describeProducers | implemented | describeProducers | brokerId; partitions |
| describeReplicaLogDirs | implemented | describeReplicaLogDirs | replicas |
| describeShareGroups | implemented | describeShareGroups | group ids; includeAuthorizedOperations as a raw signed mask; apiTimeout; rows in caller order |
| describeStreamsGroups | excluded | Streams-group administration excluded | not implemented |
| describeTopics | equivalent | fetchTopicMetadata | includeAuthorizedOperations maps to includeTopicAuthorizedOperations; partitionSizeLimitPerResponse uses the internal responsePartitionLimit |
| describeTransactions | implemented | describeTransactions | transactionalIds |
| describeUserScramCredentials | implemented | describeUserScramCredentials | users optional |
| electLeaders | implemented | electLeaders | ElectionType; partitions |
| expireDelegationToken | implemented | expireDelegationToken | hmac; expiryTimePeriodMs |
| fenceProducers | implemented | fenceProducers | transactionTimeout; timeoutMs maps to apiTimeout |
| forceTerminateTransaction | equivalent | fenceProducers({ transactionalIds: [id] }) | ordinary method fields and apiTimeout; no semantic option is silently dropped |
| incrementalAlterConfigs | implemented | incrementalAlterConfigs | validateOnly |
| listClientMetricsResources | equivalent | listConfigResources({ resourceTypes: [CLIENT_METRICS] }) | ordinary method fields and apiTimeout; no semantic option is silently dropped |
| listConfigResources | implemented | listConfigResources | resourceTypes; CLIENT_METRICS-only downgrade |
| listConsumerGroupOffsets | equivalent | fetchOffsets / fetchOffsetsForGroups | requireStable; partitions; multi-group batching by coordinator |
| listConsumerGroups | equivalent | listGroups | inStates maps to states; withTypes maps to types |
| listGroups | implemented | listGroups | states; types |
| listOffsets | equivalent | fetchTopicOffsets / fetchTopicOffsetsByTimestamp | earliest; latest; timestamp; maxTimestamp; earliestLocal; latestTiered; earliestPendingUpload; isolationLevel |
| listPartitionReassignments | implemented | listPartitionReassignments | ordinary method fields and apiTimeout; no semantic option is silently dropped |
| listShareGroupOffsets | implemented | listShareGroupOffsets | group id; optional topic partitions; apiTimeout; offsets and lag as decimal strings, unknown lag null |
| listStreamsGroupOffsets | excluded | Streams-group administration excluded | not implemented |
| listTopics | implemented | listTopics | listInternal: fetchTopicMetadata returns isInternal for caller filtering |
| listTransactions | implemented | listTransactions | filterStates maps to states; filterProducerIds maps to producerIds; filterOnDuration maps to durationFilter; filterOnTransactionalIdPattern maps to transactionalIdPattern |
| metrics | telemetry | client telemetry or JVM-only metrics | not part of admin RPC parity |
| registerMetricForSubscription | telemetry | client telemetry or JVM-only metrics | not part of admin RPC parity |
| removeMembersFromConsumerGroup | implemented | removeMembersFromConsumerGroup | ordinary method fields and apiTimeout; no semantic option is silently dropped |
| removeRaftVoter | excluded | quorum and voter administration excluded | not implemented |
| renewDelegationToken | implemented | renewDelegationToken | hmac; renewPeriodMs |
| unregisterBroker | excluded | quorum and voter administration excluded | not implemented |
| unregisterController | implemented | unregisterController | controllerId; apiTimeout; an evolving method that needs Kafka 4.4 |
| unregisterMetricFromSubscription | telemetry | client telemetry or JVM-only metrics | not part of admin RPC parity |
| updateFeatures | implemented | updateFeatures | validateOnly; upgradeType; features |
| alterConfigs | retained compatibility | legacy alterConfigs | retained although removed from Java Admin; validateOnly and separate errorMessage |
| AbstractOptions.timeoutMs | implemented | apiTimeout on each operation | preserves broker timeout as a distinct option |
| default.api.timeout.ms | implemented | AdminConfig.apiTimeout | 60000 ms; integer 1 through 2147483647 |
| retries | retained compatibility | retry | counted retry limits differ from Java INT_MAX; the API deadline still bounds the call |

UnregisterController (RPC 94) is a member of the Java 4.4 Admin interface and is implemented as
`unregisterController`. Deprecated listConsumerGroups/listClientMetricsResources and
describeClassicGroups/listGroups keep explicit equivalent rows above.

The admin client hosts all the cluster operations, such as: `createTopics`, `createPartitions`, etc.

## Call deadlines

`kafka.admin({ apiTimeout: 60000 })` sets the default end-to-end budget of each admin operation. Every operation accepts a per-call `apiTimeout`; positional operations take an optional trailing options object. Values must be integers from 1 to 2147483647. Undefined uses the configured default.

The budget includes discovery, broker selection, setup/authentication waiting, encoding, client-side queueing, retries and response waiting. Nested helpers share the original deadline. Expiry rejects with `KafkaJSTimeout`, clears local retry/page waits and unsent queued work, and prevents later call-state publication. A shared physical setup is not cancelled when one caller stops waiting.

A request already transmitted is not recalled. A mutation may have been applied; a timeout reports an unknown outcome. Check the state before choosing whether to repeat it. A later call has its own deadline. The `timeout` option of mutation APIs remains the broker's timeout; `apiTimeout` does not rewrite it.

Retry counts and backoff remain unchanged. The call ends at the earlier of retry exhaustion and its API deadline. This differs from Java's effectively unbounded default retry count within the API deadline.

For example, `createTopics({ topics, timeout: 30000, apiTimeout: 5000 })` can time out locally while the broker continues creating the topics. The error names the method and the API budget.

## Broker client telemetry

Admin config inherits Kafka's `enableMetricsPush: false` and `telemetryMaxBytes: 1048576`; explicit client values override them. The returned admin exposes `clientInstanceId({ timeout: 60000 })`, `registerMetricForSubscription(metric)` and `unregisterMetricFromSubscription(identity)`.

```js
const admin = kafka.admin({ enableMetricsPush: true, telemetryMaxBytes: 4096 })
await admin.connect()
const id = await admin.clientInstanceId({ timeout: 60000 })
admin.registerMetricForSubscription({ name: 'com.example.admin.depth', type: 'gauge', value: 1 })
admin.unregisterMetricFromSubscription({ name: 'com.example.admin.depth' })
await admin.disconnect()
```

No group or transaction identity is emitted for admin. The three admin connection/RTT metrics are documented extensions. Existing apiTimeout and broker-side timeout meanings stay unchanged, and telemetry adds no disconnect option. Telemetry cleanup runs concurrently with the first valid close, for at most min(1000 ms, its remaining budget); `disconnect({ timeout: 0 })` sends no final. It cannot replace the close result or error. Disable it with explicit false. See [Client Telemetry](ClientTelemetry.md) for limits, loss, errors and corrective-reopen behavior.

## Authorized operations

`describeGroups(groupIds, { includeAuthorizedOperations: true })` and `fetchTopicMetadata({ topics, includeTopicAuthorizedOperations: true })` can return `authorizedOperations?: number | null`. A requested supported value is the raw signed INT32 mask, including unknown bits. The sentinel `-2147483648`, an absent or unsupported field, and a field not requested are `null`. Metadata supports its request flag from v8; v7 completes with a null mask. No conversion to an operation-name array is performed.

```javascript
const kafka = new Kafka(...)
const admin = kafka.admin()

// remember to connect and disconnect when you are done
await admin.connect()
await admin.disconnect()
```

The option `retry` can be used to customize the configuration for the admin.

Take a look at [Retry](Configuration.md#default-retry) for more information.

## <a name="list-topics"></a> List topics

`listTopics` lists the names of all existing topics, and returns an array of strings.
The method will throw exceptions in case of errors.

```javascript
await admin.listTopics()
// [ 'topic-1', 'topic-2', 'topic-3', ... ]
```

## <a name="create-topics"></a> Create topics

`createTopics` will resolve to `true` if the topic was created successfully or `false` if it already exists. The method will throw exceptions in case of errors.

When a topic cannot be created the rejection is a `KafkaJSAggregateError`: `errors` lists one
error per failed topic. The aggregate is `retriable` only when it has at least one child and every
child is retriable, and its `category` is `refreshRetriable`, `retriable` or
`applicationRecoverable` to match, so a retrier of your own around `createTopics` (or
`alterPartitionReassignments`) stops at once on an aggregate that holds a failure that cannot
succeed, with a `KafkaJSNonRetriableError` whose `cause` is the aggregate, and retries one whose
failures are all retriable.

`createTopics` and `alterPartitionReassignments` answer per topic or per partition, and a failure that names only some of them does not repeat the rest. Each retry sends only the items the broker said could still go through (`retriable`; a partition keeps the replicas you gave), an item the broker confirmed is never sent again, and an item it refused for good (for example `INVALID_PARTITIONS`) is remembered across the retries. When the attempts end with an item refused for good, the call rejects with a `KafkaJSAggregateError` that lists every such item, followed by the failures of the attempt that ran out of retries or was cut short by a transport or controller error (for `createTopics`, a topic that already exists counts as refused for good, but when nothing else went wrong the call resolves `false` instead). If nothing was refused for good the call ends with what ended the retries, unchanged (`KafkaJSNumberOfRetriesExceeded`, for example). `createTopics` resolves `false` only when every topic that did not go through already exists, whatever else was created; with `waitForLeaders` it waits once, after the last attempt, for the leaders of the topics this call created; a call whose attempts fail does not enter that wait, and a metadata error or timeout from the wait itself rejects the call directly, not as an aggregate. An item the broker's answer does not list as failed counts as created; a request whose answer never arrived (a transport failure) leaves its outcome unknown, and `validateOnly` creates nothing. `retry: { retries: 0 }` makes one attempt and reports every failure it saw, in the same two ways.

```javascript
await admin.createTopics({
    validateOnly: <boolean>,
    waitForLeaders: <boolean>
    timeout: <Number>,
    topics: <ITopicConfig[]>,
})
```

`ITopicConfig` structure:

```javascript
{
    topic: <String>,
    numPartitions: <Number>,     // default: -1 (uses broker `num.partitions` configuration)
    replicationFactor: <Number>, // default: -1 (uses broker `default.replication.factor` configuration)
    replicaAssignment: <Array>,  // Example: [{ partition: 0, replicas: [0,1,2] }] - default: []
    configEntries: <Array>       // Example: [{ name: 'cleanup.policy', value: 'compact' }] - default: []
}
```

| property       | description                                                                                           | default |
| -------------- | ----------------------------------------------------------------------------------------------------- | ------- |
| topics         | Topic definition                                                                                      |         |
| validateOnly   | If this is `true`, the request will be validated, but the topic won't be created.                     | false   |
| timeout        | The time in ms to wait for a topic to be completely created on the controller node                    | 5000    |
| waitForLeaders | If this is `true` it will wait until metadata for the new topics doesn't throw `LEADER_NOT_AVAILABLE` | true    |

`createTopics` resolves a boolean and does not return topic ids: read them with [`fetchTopicMetadata`](#fetch-topic-metadata), which returns the `topicId` of every topic.

With `waitForLeaders` the wait also continues while the broker answers `UNKNOWN_TOPIC_OR_PARTITION` for a topic the controller has just created: a KRaft broker can lag the controller by a few seconds. The wait gives up with a `KafkaJSTimeout` once `timeout` has passed (10 s when `timeout` is not set); any other error fails the call at once.

## <a name="delete-topics"></a> Delete topics

```javascript
await admin.deleteTopics({
    topics: <String[]>,
    timeout: <Number>, // default: 5000
})
```

Topic deletion is disabled by default in Apache Kafka versions prior to `1.0.0`. To enable it set the server config.

```yml
delete.topic.enable=true
```

Delete by topic ID with `deleteTopics({ topicIds: [topicId], timeout })`. Exactly one of `topics` and `topicIds` is required. IDs must be unique canonical UUID strings and cannot be the zero UUID; letter case is normalized. ID deletion needs DeleteTopics v6. Both forms resolve with no value on success and keep the existing thrown-error contract. Failed ID entries retain `topicId`; their legacy string `topic` field is the broker's name when present, otherwise the canonical ID. Every response must cover exactly the IDs attempted on that round before results are accepted. Quota retries resend only the refused identities. A failure during the follow-up metadata refresh propagates without resending a confirmed deletion. `timeout` remains the broker timeout and `apiTimeout` bounds the whole call.

## <a name="create-partitions"></a> Create partitions

`createPartitions` will resolve in case of success. The method will throw exceptions in case of errors.

```javascript
await admin.createPartitions({
    validateOnly: <boolean>,
    timeout: <Number>,
    topicPartitions: <TopicPartition[]>,
})
```

`TopicPartition` structure:

```javascript
{
    topic: <String>,
    count: <Number>,     // partition count
    assignments: <Array<Array<Number>>> // Example: [[0,1],[1,2],[2,0]]
}
```

| property       | description                                                                                           | default |
| -------------- | ----------------------------------------------------------------------------------------------------- | ------- |
| topicPartitions| Topic partition definition                                                                                      |         |
| validateOnly   | If this is `true`, the request will be validated, but the topic won't be created.                     | false   |
| timeout        | The time in ms to wait for a topic to be completely created on the controller node                    | 5000    |
| count          | New partition count, mandatory                                                                                   |         |
| assignments    | Assigned brokers for each new partition                                                               | null    |

### Throttled requests

When a controller-mutation quota throttles `createTopics`, `deleteTopics` or `createPartitions` (KIP-599), the broker refuses the topics it did not process with `THROTTLING_QUOTA_EXCEEDED` (89) and reports how long to wait. The admin client sends only those topics again, within the `retry` budget, and the connection holds the next request back for the throttle time the answer carried, so a call that a quota only slows still succeeds, a little later. The topics the broker did process are not sent again. Once the `retries` are spent the call rejects with `KafkaJSNumberOfRetriesExceeded`, whose `cause` is the quota error (for `createTopics`, the `KafkaJSAggregateError` that lists the refused topics). For `deleteTopics` and `createPartitions`, the quota alone sends nothing again when another topic failed in the same answer, and that failure is handled as it always has been (a retried `NOT_CONTROLLER`, or for `deleteTopics` `UNKNOWN_TOPIC_OR_PARTITION`, still sends the whole request again, quota-refused topics included); `createTopics` sends again every topic whose failure is retriable, the refused ones included (see above). Java's `Admin` retries these refusals until its timeout (`retryOnQuotaViolation`). The throttle wait comes on top of the retry backoff: `retry.maxRetryTime` only caps one backoff delay, so by default nothing bounds the call as a whole, and the worst case is about `retries` throttle times plus the backoff delays, where Java bounds the whole call at its default 60 s (`default.api.timeout.ms`). To bound it, set `retry.maxTotalTime`: it counts the time already spent, throttle waits included, and the call then rejects with `KafkaJSDeliveryTimeout`, whose `cause` is the quota error; the attempt it lets start can still wait out one more throttle time.

## <a name="fetch-topic-metadata"></a> Fetch topic metadata

```javascript
await admin.fetchTopicMetadata({
    topics: <Array<String>>,
    useDescribeTopicPartitions: <boolean> // default: false
})
```

`TopicsMetadata` structure:

```javascript
{
    topics: <Array<TopicMetadata>>,
}
```

`TopicMetadata` structure:

```javascript
{
    name: <String>,
    partitions: <Array<PartitionMetadata>>, // default: 1
    topicId: <String|null>,                 // the topic's uuid, null when the broker sends none
    isInternal: <Boolean>
}
```

`PartitionMetadata` structure:

```javascript
{
    partitionErrorCode: <Number>, // default: 0
    partitionId: <Number>,
    leader: <Number>,
    replicas: <Array<Number>>,
    isr: <Array<Number>>,
    offlineReplicas: <Array<Number>>,
    leaderEpoch: <Number>,
    // only with useDescribeTopicPartitions:
    eligibleLeaderReplicas: <Array<Number>|null>,
    lastKnownElr: <Array<Number>|null>
}
```

The admin client will throw an exception if any of the provided topics do not already exist.

If you omit the `topics` argument the admin client will fetch metadata for all topics:

```javascript
await admin.fetchTopicMetadata()
```

Without `topics`, a partition that has no leader is returned as the broker reports it, with its `partitionErrorCode` (`LEADER_NOT_AVAILABLE`, 5) and `leader: -1`: the call neither waits for a leader nor fails. `listTopics` and `describeCluster` do not fail on such a partition either, and neither do the admin calls that refresh the metadata first (`createTopics` and `describeConfigs`, for example), unless the partition belongs to a topic the admin refreshes by name: one it has read or reset offsets of, or deleted records from. To wait for the leaders of topics you just created, name them: a request that names topics still waits while one of their partitions has no leader.

On the Metadata path, `fetchTopicMetadata`, `listTopics` and `describeCluster` reject, when every broker is connected to and every Metadata request fails, with the last broker's own error (its class and `cause` intact) after one pass over the brokers. `listTopics` and `describeCluster` always take this path; `fetchTopicMetadata` takes it when `useDescribeTopicPartitions` is not set, when no topics are named, or when the broker does not offer DescribeTopicPartitions. A broker that cannot be found or connected to is retried within the client's `retry` budget first (the `retry` given to `new Kafka()`; `admin({ retry })` does not apply); if that budget runs out the call rejects with `KafkaJSNumberOfRetriesExceeded` (`KafkaJSDeliveryTimeout` when `retry.maxTotalTime` ends it first), whose `cause` is the connection error, or `KafkaJSBrokerNotFound` when the broker table is empty. On the Metadata path, the polling wait rejects with `KafkaJSTimeout` if it is still unsettled 10 s after that path is entered, whether waiting for a leader or for a reachable broker. DescribeTopicPartitions has its own deadline of 60 s (below); time spent there before an unsupported-API fallback is additional. While the broker it asks last answers `LEADER_NOT_AVAILABLE` it asks again every 50 ms. Any other error, an authorization failure for example, is thrown at once.

### Describing topics partition by partition

With `useDescribeTopicPartitions: true` and at least one topic name, the topics are read with DescribeTopicPartitions (KIP-966) instead of Metadata:

- Any broker answers. The client follows the broker's pages to the end (up to 2000 partitions a page, fewer if the broker's `max.request.partition.size.limit` is lower), so a topic with many partitions is not cut off; a broker whose pages repeat, or that does not answer for a topic, fails the call with `KafkaJSNonRetriableError` instead of returning a partial answer. A request that fails in a way that can pass (a dropped connection, a timeout) is asked again on the `retry` budget after a metadata refresh, as the other admin calls are. The topics come back in name order.
- The whole read, every page and every retry included, has one deadline of 60 s (Java's `default.api.timeout.ms`). At the deadline the call rejects with `KafkaJSTimeout`, whatever request is stalled and also with `enforceRequestTimeout: false`; a page that arrives later is ignored, and the call starts no new attempt and creates no new page request. Work already in progress at the deadline is not cancelled, and its result is ignored: a metadata refresh may finish its own retries (bounded by `retry`, and shared with other calls on the same client), a broker selection may go on to connect to the next broker, and a page already queued may still be sent. A topic whose id changes between two pages (it was deleted and recreated while it was read) fails the attempt retriably, and the read starts again after a metadata refresh. If your cluster pages more slowly than that, use the default Metadata path, `useDescribeTopicPartitions: false`.
- Each partition also carries `eligibleLeaderReplicas` and `lastKnownElr`. They are `null` when the cluster does not track eligible leader replicas, `[]` when it does and none are eligible, and broker ids otherwise; accept all three. (Eligible leader replicas are off by default on Kafka 4.0 and on by default for new clusters from Kafka 4.1.)
- A partition without a leader is returned with `leader: -1`. Kafka 4.1.2 and 4.3 brokers leave its `partitionErrorCode` at 0 (a Metadata answer sets `LEADER_NOT_AVAILABLE`, 5), so check `leader` to find it; a non-zero `partitionErrorCode` is passed through when a broker sends one. Unlike a Metadata request for named topics, this path does not wait for leaders.
- A topic that does not exist, or that you may not describe, fails the call with that topic's error once every page has been read; when several fail, the first in name order is thrown. DescribeTopicPartitions never creates a topic. A Metadata request may, when the cluster allows automatic topic creation and `allowAutoTopicCreation` is on.
- The option applies only to topics named explicitly. When `topics` is empty or omitted, the call goes through Metadata and `eligibleLeaderReplicas` and `lastKnownElr` are then absent. The same happens when the broker does not offer the API.

```javascript
const { topics } = await admin.fetchTopicMetadata({
    topics: ['topic-name'],
    useDescribeTopicPartitions: true
})
```

`listTopics()` returns only names as `string[]`. `fetchTopicMetadata()` with no topics returns all those topics with `name`, `topicId`, `isInternal` and partition detail from one fresh Metadata request. Omitted `topicId` support is represented as null. Listing all topics does not add them to the target-topic set.

## <a name="fetch-topic-offsets"></a> Fetch topic offsets

`fetchTopicOffsets(topic, options)` returns partition offsets, with `offset` equal to `high`. `options.isolationLevel` is `'read_committed'` by default or `'read_uncommitted'`. During an open transaction, the committed high is the last stable offset; the uncommitted high is the high watermark. `low` is the earliest retained offset. The existing no-options call keeps its default. `options.apiTimeout` bounds this invocation, independently of broker timeouts.

```javascript
await admin.fetchTopicOffsets(topic)
// [
//   { partition: 0, offset: '31004', high: '31004', low: '421' },
//   { partition: 1, offset: '54312', high: '54312', low: '3102' },
//   { partition: 2, offset: '32103', high: '32103', low: '518' },
//   { partition: 3, offset: '28', high: '28', low: '0' },
// ]
```

`fetchTopicOffsets` and consumer offset resolution keep their existing behavior. Named offset specifications are selected through `fetchTopicOffsetsByTimestamp`.

## <a name="fetch-topic-offsets-by-timestamp"></a> Fetch topic offsets by timestamp

Specify a `timestamp` to get the earliest offset on each partition where the message's timestamp is greater than or equal to the given timestamp.

The optional third argument accepts the same `isolationLevel` and `apiTimeout`. Both the timestamp lookup and its high-offset fallback use that isolation. The default stays `'read_committed'`.

```javascript
await admin.fetchTopicOffsetsByTimestamp(topic, timestamp)
// [
//   { partition: 0, offset: '3244' },
//   { partition: 1, offset: '3113' },
// ]
```

`fetchTopicOffsetsByTimestamp(topic, timestamp, options)` also accepts `'maxTimestamp'`, `'earliestLocal'`, `'latestTiered'` and `'earliestPendingUpload'`. Their broker timestamps are -3, -4, -5 and -6; they need ListOffsets v7, v8, v9 and v11 respectively. An unsupported specification rejects with `KafkaJSServerDoesNotSupportApiKey`. Named specifications return the raw broker offset, including -1 when no offset exists. Numeric timestamps, including decimal strings such as a consumed message's `timestamp`, keep their existing high-offset fallback. Both forms keep `read_committed` by default and accept `{ isolationLevel: 'read_uncommitted' }`.

ListOffsets v10 carries the broker timeout for asynchronous remote reads; v11 adds the earliest pending upload specification. On a plain topic, `earliestLocal` is the log start and `latestTiered` is -1. Tiered storage must be configured on the broker to observe uploaded/local boundaries.

## Committed lag

Committed lag and processing lag measure different positions. Read group commits from the broker with `resolveOffsets: false` and the high watermark with `isolationLevel: 'read_uncommitted'`. The recipe below uses `BigInt` over decimal strings and reports an absent commit separately; its lag then covers the retained log from `low`. An omitted response partition is an invalid observation, not evidence of an absent commit.

```javascript
/** Computes committed lag from fresh broker observations, using lossless decimal arithmetic.
 * @param {object} admin connected admin client
 * @param {string} groupId group to observe
 * @param {string} topic topic to observe
 * @returns {Promise<object[]>} partition, hasCommit and decimal lag
 * @throws {Error} broker refusal or an omitted requested partition
 */
async function committedLagFor(admin, groupId, topic) {
  const fetched = await admin.fetchOffsets({ groupId, topics: [topic], resolveOffsets: false })
  const bounds = await admin.fetchTopicOffsets(topic, { isolationLevel: 'read_uncommitted' })
  const offsets = fetched.find((row) => row.topic === topic)?.partitions ?? []
  return bounds.map(({ partition, high, low }) => {
    const raw = offsets.find((row) => row.partition === partition)
    if (!raw) throw new Error('The broker omitted a requested partition')
    const hasCommit = raw.offset !== '-1'
    return {
      partition,
      hasCommit,
      lag: (BigInt(high) - BigInt(hasCommit ? raw.offset : low)).toString(),
    }
  })
}
```

Batch processing lag is `HW - (lastBatchOffset + 1)`. It equals committed lag only after the committed **next** offset equals that exact batch-next offset at a stable high watermark, with no append between observations. For high100/lastBatch19, batch lag80 and commits0/20/50 give committed lag100/80/50. Merely selecting the same isolation does not align positions.

## <a name="fetch-offsets"></a> Fetch consumer group offsets

`fetchOffsets` returns the consumer group offset for a list of topics.

```javascript
await admin.fetchOffsets({ groupId, topics: ['topic1', 'topic2'] })
// [
//   {
//     topic: 'topic1',
//     partitions: [
//       { partition: 0, offset: '31004' },
//       { partition: 1, offset: '54312' },
//       { partition: 2, offset: '32103' },
//       { partition: 3, offset: '28' },
//     ],
//   },
//   {
//     topic: 'topic2',
//     partitions: [
//       { partition: 0, offset: '1234' },
//       { partition: 1, offset: '4567' },
//     ],
//   },
// ]
```

Omit `topics` altogether if you want to get the consumer group offsets for all topics with committed offsets.

Include the optional `resolveOffsets` flag to resolve the offsets without having to start a consumer, useful when fetching directly after calling [resetOffsets](#a-name-reset-offsets-a-reset-consumer-group-offsets):

```javascript
await admin.resetOffsets({ groupId, topic })
await admin.fetchOffsets({ groupId, topics: [topic], resolveOffsets: false })
// [
//   { partition: 0, offset: '-1' },
//   { partition: 1, offset: '-1' },
//   { partition: 2, offset: '-1' },
//   { partition: 3, offset: '-1' },
// ]

await admin.resetOffsets({ groupId, topic })
await admin.fetchOffsets({ groupId, topics: [topic], resolveOffsets: true })
// [
//   { partition: 0, offset: '31004' },
//   { partition: 1, offset: '54312' },
//   { partition: 2, offset: '32103' },
//   { partition: 3, offset: '28' },
// ]
```

`fetchOffsets({ groupId, topics, resolveOffsets, requireStable, apiTimeout })` keeps its existing result arrays and sentinel behavior. `requireStable` defaults to false; true asks the coordinator for stable transactional commits and requires OffsetFetch v7 or later. `resolveOffsets: false` reads raw commits and does not resolve/write sentinel values.

`fetchOffsetsForGroups({ groups: [{ groupId, topics: [{ topic, partitions }] }], requireStable, apiTimeout })` returns one result per requested group in caller order. Omitted topics or partitions select all; explicit empty read selections return empty without wire work. Groups are batched per coordinator on OffsetFetch v8/v9; an older coordinator gets one request per group. Every group and partition has `errorCode`, `error` and nullable `errorMessage`; partition offsets are decimal strings, metadata is `string | null`, and leaderEpoch is `number | null`. Group or partition errors remain data, so inspect them even when the promise resolves. Stable-read retries do not re-send completed partitions. A local unresolved failure uses errorCode -1 and retains its Error. The existing single-group `fetchOffsets` throw/result contract is unchanged.

## Delete consumer group offsets

```javascript
const result = await admin.deleteOffsets({
  groupId: 'inactive-group',
  topics: [{topic: 'orders', partitions: [0, 1]}],
  apiTimeout: 60000,
})
for (const topic of result.topics) {
  for (const partition of topic.partitions) {
    if (partition.error) console.error(topic.topic, partition.partition, partition.errorCode)
  }
}
```

`deleteOffsets` deletes the requested committed offsets through the group coordinator using OffsetDelete v0. It returns one partition result per requested partition in caller order; partition errors are data. Group authorization failure, invalid/missing group or non-empty-group refusal rejects with `KafkaJSProtocolError`. Coordinator loading/movement is retried within the configured retry budget; completed partitions are retained and not sent again. The group/topic/partition mutation lists must be nonempty and unique. A timed-out deletion may already have reached the coordinator; the deadline does not prove that it was not applied.

## <a name="reset-offsets"></a> Reset consumer group offsets

`resetOffsets` resets the consumer group offset to the earliest or latest offset (latest by default).
The consumer group must have no running instances when performing the reset. Otherwise, the command will be rejected.

```javascript
await admin.resetOffsets({ groupId, topic }) // latest by default
// await admin.resetOffsets({ groupId, topic, earliest: true })
```

## <a name="set-offsets"></a> Set consumer group offsets

`setOffsets` allows you to set the consumer group offset to any value.

```javascript
await admin.setOffsets({
    groupId: <String>,
    topic: <String>,
    partitions: <SeekEntry[]>,
})
```

`SeekEntry` structure:

```javascript
{
    partition: <Number>,
    offset: <String>,
}
```

Example:

```javascript
await admin.setOffsets({
    groupId: 'my-consumer-group',
    topic: 'custom-topic',
    partitions: [
        { partition: 0, offset: '35' },
        { partition: 3, offset: '19' },
    ]
})
```

## <a name="reset-offsets-by-timestamp"></a> Reset consumer group offsets by timestamp

Combining `fetchTopicOffsetsByTimestamp` and `setOffsets` can reset a consumer group's offsets on each partition to the earliest offset whose timestamp is greater than or equal to the given timestamp.
The consumer group must have no running instances when performing the reset. Otherwise, the command will be rejected.

```javascript
await admin.setOffsets({ groupId, topic, partitions: await admin.fetchTopicOffsetsByTimestamp(topic, timestamp) })
```

## <a name="describe-cluster"></a> Describe cluster

`admin.describeCluster()` returns `{ brokers, controller, clusterId }`. Each broker has `nodeId`, `host` and `port`. A missing controller is `null`. With no true flag, the method keeps the existing Metadata request and result shape.

`admin.describeCluster({ includeAuthorizedOperations: true })` asks for the raw signed authorization mask as `authorizedOperations`; the unsupported sentinel becomes `null`. It uses DescribeCluster where available. Otherwise Metadata returns the mask when its negotiated version can represent it, or `null`.

`admin.describeCluster({ includeFencedBrokers: true })` adds `brokers[].fenced` and requires DescribeCluster v2. An older broker receives no request with the flag silently removed. Both flags are optional booleans, default `false`, and may be combined. Per-call `apiTimeout` uses the shared call deadline described in Call deadlines.

```javascript
await admin.describeCluster()
// {
//   brokers: [
//     { nodeId: 0, host: 'localhost', port: 9092 }
//   ],
//   controller: 0,
//   clusterId: 'f8QmWTB8SQSLE6C99G4qzA'
// }
```

## Elect leaders

`admin.electLeaders({ electionType: 'preferred', topicPartitions: [{ topic: 'orders', partitions: [0, 2] }] })` returns topic entries containing partition outcomes: `{ partition, errorCode, error, errorMessage }`. The default election type is `preferred`. `topicPartitions: null`, or an omitted selection, elects for all partitions. An explicit empty list returns [] without metadata, controller discovery or a request. Nonempty lists require unique topic/partition identities and nonblank topic names without NUL.

`unclean` requires ElectLeaders v1 or later. Calls go to the controller. Only unresolved eligible partitions are retried; successful elections are not resent. Completed outcomes are retained before a controller refresh. A later refresh failure affects only unresolved partitions; when no retry remains, no refresh starts and the last per-partition error stays in the result. `ELECTION_NOT_NEEDED` and `PREFERRED_LEADER_NOT_AVAILABLE` are result data. Top-level failures and authentication refusals reject the call with the original typed error. `apiTimeout` is the client call budget, separate from the broker request's fixed 60000 ms timeout. A timeout after transmission can have an unknown remote outcome.

## Describe features

`admin.describeFeatures({ brokerId })` returns `{ supportedFeatures, finalizedFeatures, finalizedFeaturesEpoch }`. Omit `brokerId` to use a connected broker. Every call sends a fresh ApiVersions request. Supported features map each name to `{ minVersion, maxVersion }`; finalized features map each name to `{ minVersionLevel, maxVersionLevel }`. The epoch is a decimal string, or `null` when unavailable. This read does not replace the connection's version-negotiation snapshot. Features are not fields of `describeCluster`.

## Update features

`admin.updateFeatures({ features: [{ name: 'metadata.version', maxVersionLevel: 25, upgradeType: 'upgrade' }], validateOnly: true })` validates without applying the change. Feature names must be unique, nonblank and without NUL; levels are integers from 0 through 32767. The default upgrade type is `upgrade`; `safe-downgrade` and `unsafe-downgrade` are also available. The optional `validateOnly` boolean defaults to `false`. Validation and non-default upgrade types require UpdateFeatures v1 or later.

The result is an array of `{ feature, errorCode, error, errorMessage }`. Older protocol versions return individual feature results; v2 success covers all requested features and has no individual wire result array. Top-level broker failures and authentication refusals reject the call with the original typed error. Retries preserve completed feature outcomes before controller refresh. A later refresh failure affects only unresolved features with its original typed error; when no retry remains, no refresh starts and last per-feature errors remain. A successful validation causes no follow-up mutation or local feature-cache update. Read fresh feature state with `describeFeatures`. Per-call `apiTimeout` covers discovery, retries and the response, and a transmitted mutation may have been applied when it expires.

## List configuration resources

`admin.listConfigResources({ resourceTypes: [ConfigResourceTypes.CLIENT_METRICS] })` returns `{ resources: [{ type, name }] }`. `resourceTypes` is an optional array of unique known `ConfigResourceTypes`; omitted or empty selects the version's default supported resources. A nonempty filter requires v1 except exactly `[CLIENT_METRICS]`, which is the explicit v0 equivalent. A v0 broker lists client-metrics resources only. Other filters are refused rather than silently dropped. `apiTimeout` is supported.

The existing `describeConfigs`, `alterConfigs` and `incrementalAlterConfigs` resource shapes and routing stay unchanged, including `CLIENT_METRICS` and `GROUP`. The equivalent of Java's deprecated `listClientMetricsResources` is `listConfigResources({ resourceTypes: [CLIENT_METRICS] })`. No separate deprecated method is added. Telemetry subscription/push operations are a different feature.

## Unregister a controller

`admin.unregisterController({ controllerId })` removes one controller registration from a Kafka cluster (KIP-1312) and resolves `undefined` on success. The method is evolving: Kafka marks the Java method unstable, so its shape can change. It requires Kafka 4.4 or newer. <!-- placeholder: capability sentence to be finalised from the committed Kafka 4.4.0 capture -->

`controllerId` must be an integer from 0 through 2147483647. Any other value rejects with `KafkaJSInvalidConfigurationError` before the client looks up a broker or sends a request. The call goes to the first known broker, lowest node id first, that offers API key 94. A broker without it is skipped, and `KafkaJSServerDoesNotSupportApiKey` is thrown only after every known broker lacks it. A connection or authentication failure is thrown as it is and is never reported as unsupported.

A broker refusal is thrown as the standard protocol error with its `code`, `type` and `retriable` flag. `brokerErrorMessage` is `null` when the broker sent no message and `''` when it sent an empty one; non-empty text is also appended to `message`. `CONTROLLER_ID_NOT_REGISTERED` (136) and `CLUSTER_AUTHORIZATION_FAILED` (31) do not retry; retriable codes retry under the client retry options. `apiTimeout` covers discovery, retries and the response, and a request that was sent may have been applied when it expires.

## <a name="describe-configs"></a> Describe configs

Get the configuration for the specified resources.

```javascript
await admin.describeConfigs({
  includeSynonyms: <boolean>,
  includeDocumentation: <boolean>, // default: false
  resources: <ResourceConfigQuery[]>
})
```

`ResourceConfigQuery` structure:

```javascript
{
    type: <ConfigResourceType>,
    name: <String>,
    configNames: <String[]>
}
```

Returning all configs for a given resource:

```javascript
const { ConfigResourceTypes } = require('@countly/kafkajs')

await admin.describeConfigs({
  includeSynonyms: false,
  resources: [
    {
      type: ConfigResourceTypes.TOPIC,
      name: 'topic-name'
    }
  ]
})
```

Returning specific configs for a given resource:

```javascript
const { ConfigResourceTypes } = require('@countly/kafkajs')

await admin.describeConfigs({
  includeSynonyms: false,
  resources: [
    {
      type: ConfigResourceTypes.TOPIC,
      name: 'topic-name',
      configNames: ['cleanup.policy']
    }
  ]
})
```

Take a look at [configResourceTypes](../src/protocol/configResourceTypes.js) for a complete list of resources.

The `name` of a resource is a topic name, a group id, a client-metrics subscription name or a broker id, by `type`. A `BROKER` resource may also have the empty name `''`, which is the cluster-wide default broker config: its entries are the ones set as cluster defaults, so it is empty on a cluster that has none. Every other type needs a non-empty name. A broker config for one broker id, and a `BROKER_LOGGER`, are answered by that broker only; everything else, a `BROKER` resource with the empty name included, is asked of the controller.

`includeDocumentation: true` asks the broker for the documentation of each entry, which it leaves out otherwise (`documentation` is `null`). Every entry also carries `configType`, the type of its value: a number, see `ConfigTypes` (`BOOLEAN`, `STRING`, `INT`, `SHORT`, `LONG`, `DOUBLE`, `LIST`, `CLASS` or `PASSWORD`). Both keys need DescribeConfigs v3, which every Kafka 4.x broker offers. The documentation makes the response large, so ask for it only when you show it.

Example response:

```javascript
{
    resources: [
        {
            configEntries: [{
                configName: 'cleanup.policy',
                configValue: 'delete',
                isDefault: true,
                configSource: 5,
                isSensitive: false,
                readOnly: false,
                configType: 7,
                documentation: null
            }],
            errorCode: 0,
            errorMessage: null,
            resourceName: 'topic-name',
            resourceType: 2
        }
    ],
    throttleTime: 0
}
```

## Validation-only requests and broker messages

`createTopics`, `createPartitions`, `alterConfigs`, `incrementalAlterConfigs` and `updateFeatures` accept `validateOnly: true`. Their validation request does not apply a mutation or make a follow-up request to wait for effects. Other methods support it only when their request schema has that field; a read-only or destructive method without the field does not gain validation behavior. Broker `errorMessage` is a separate `string | null` on legacy AlterConfigs errors/results and on `KafkaJSCreateTopicError`; stable `message`, class, type, code and retry properties retain their meanings. An empty broker message remains an empty string.

| Method | Wire validation field | Minimum version |
| --- | --- | --- |
| `createTopics` | `validateOnly` | v1 |
| `createPartitions` | `ValidateOnly` | v0 |
| `alterConfigs` | `ValidateOnly` | v0 |
| `incrementalAlterConfigs` | `ValidateOnly` | v0 |
| `updateFeatures` | UpdateFeatures v1+ | `validateOnly: true` validates without applying the change; no follow-up mutation or local cache update. |

Destructive methods such as `deleteTopics`, `deleteTopicRecords` and offset deletion have no validation flag. Membership changes, ACL mutations and partition reassignments likewise have no validation field; reads and lifecycle calls do not validate mutations.

## <a name="alter-configs"></a> Alter configs

Update the configuration for the specified resources.

`alterConfigs` **replaces** the whole dynamic config of each resource: every entry you leave out is deleted. [`incrementalAlterConfigs`](#incremental-alter-configs) changes only the entries it names and is the better choice for almost every change. A `BROKER` resource with the empty name `''` is the cluster-wide default broker config, and `alterConfigs` on it deletes every cluster-wide dynamic default you do not list (use `incrementalAlterConfigs` to change one); every other type needs a non-empty name. With eligible leader replicas enabled, the controller refuses to delete the cluster-level `min.insync.replicas`, so a replacement that leaves it out fails with `INVALID_CONFIG`: read what the default holds with `describeConfigs` and send it back.

```javascript
await admin.alterConfigs({
    validateOnly: false,
    resources: <ResourceConfig[]>
})
```

`ResourceConfig` structure:

```javascript
{
    type: <ConfigResourceType>,
    name: <String>,
    configEntries: <ResourceConfigEntry[]>
}
```

`ResourceConfigEntry` structure:

```javascript
{
    name: <String>,
    value: <String>
}
```

Example:

```javascript
const { ConfigResourceTypes } = require('@countly/kafkajs')

await admin.alterConfigs({
    resources: [{
        type: ConfigResourceTypes.TOPIC,
        name: 'topic-name',
        configEntries: [{ name: 'cleanup.policy', value: 'compact' }]
    }]
})
```

Take a look at [configResourceTypes](../src/protocol/configResourceTypes.js) for a complete list of resources.

Example response:

```javascript
{
    resources: [{
        errorCode: 0,
        errorMessage: null,
        resourceName: 'topic-name',
        resourceType: 2,
    }],
    throttleTime: 0,
}
```

## <a name="incremental-alter-configs"></a> Incremental alter configs

Change individual config entries (KIP-339). Each entry names an operation, so the entries a request leaves out stay as they are, unlike [`alterConfigs`](#alter-configs), which replaces the whole config of the resource.

```javascript
await admin.incrementalAlterConfigs({
    validateOnly: <boolean>, // default: false
    resources: <IncrementalResourceConfig[]>
})
```

`IncrementalResourceConfig` structure:

```javascript
{
    type: <ConfigResourceType>,
    name: <String>,
    configEntries: <IncrementalConfigEntry[]>
}
```

`IncrementalConfigEntry` structure:

```javascript
{
    name: <String>,
    operation: <AlterConfigOpTypes>, // required
    value: <String>                  // required, except for DELETE
}
```

| operation  | what it does                                                                                           |
| ---------- | ------------------------------------------------------------------------------------------------------ |
| `SET`      | Sets the entry to `value`.                                                                             |
| `DELETE`   | Reverts the entry to its default. `value` is ignored.                                                  |
| `APPEND`   | List configs only: adds the comma-separated `value` items that the list does not hold yet.             |
| `SUBTRACT` | List configs only: removes the comma-separated `value` items. Removing all of them leaves an empty list, not the default: use `DELETE` for that. |

`operation` has no default, so a forgotten field can never turn into an overwrite. An `APPEND` or `SUBTRACT` on an entry that is not a list is answered with `INVALID_CONFIG` (40). On an entry that has not been set, they start from its default.

Example:

```javascript
const { ConfigResourceTypes, AlterConfigOpTypes } = require('@countly/kafkajs')

const { resources } = await admin.incrementalAlterConfigs({
    resources: [{
        type: ConfigResourceTypes.TOPIC,
        name: 'topic-name',
        configEntries: [
            { name: 'retention.ms', operation: AlterConfigOpTypes.SET, value: '86400000' },
            { name: 'cleanup.policy', operation: AlterConfigOpTypes.APPEND, value: 'compact' },
            { name: 'segment.ms', operation: AlterConfigOpTypes.DELETE },
        ]
    }]
})
```

The call resolves with one result for each resource, in the order you gave them. **It does not reject when a resource fails**, so check each `errorCode`:

```javascript
{
    resources: [{
        errorCode: 0,
        errorMessage: null,
        resourceType: 2,
        resourceName: 'topic-name',
    }],
}
```

A resource is changed as a whole or not at all, but the resources of one call are independent: some can succeed while others fail, and each broker gets its own request, so a call that rejects (a connection failure to one broker, say) may have applied the resources that went to the others. Repeating the whole call is safe, because every operation can be repeated. A result is never retried, whatever its code, with one exception: `NOT_CONTROLLER` means the request reached the wrong broker, and the client then sends the whole call again, which is safe because every operation can be repeated. The call rejects for an invalid argument (`KafkaJSNonRetriableError`, before anything is sent), for a connection failure, and with `KafkaJSNumberOfRetriesExceeded` when `NOT_CONTROLLER` does not go away.

Where the resources go: a `BROKER` resource with a name and a `BROKER_LOGGER` go to the broker with that id, one request for each broker; every other resource goes to one broker, whichever the client picks. A `BROKER` resource with the empty name `''` is the cluster-wide default broker config and takes the same path as a topic. The empty name is refused for every other type.

The client refuses these before sending anything, naming the resource or config: a `resources` array that is empty, a resource or a config name that appears twice (a config name counts once whatever its operation), a missing or unknown `operation`, and a `SET`, `APPEND` or `SUBTRACT` without a string `value`. The empty string is a value.

`GROUP` resources take a group id and its group configs, such as `consumer.session.timeout.ms`; `CLIENT_METRICS` resources take a subscription name and its `metrics`, `interval.ms` and `match` entries.

## <a name="list-groups"></a> List groups

List groups available on the broker.

```javascript
await admin.listGroups()
```

Example response:

```javascript
{
    groups: [
        {groupId: 'testgroup', protocolType: 'consumer', groupState: 'Stable', groupType: 'classic'}
    ]
}
```

Every row carries two fields beyond the group id and its protocol type:

| field | value |
| --- | --- |
| `groupState` | the group's state as the broker spells it, e.g. `'Stable'`, `'Empty'`, `'PreparingRebalance'` |
| `groupType` | the group's type, lower-cased by this client: `'classic'`, `'consumer'`, `'share'`, `'streams'` or `'unknown'`. A type a newer broker introduces is passed through lower-cased rather than dropped, so treat this as an open set and give an exhaustive `switch` over it a `default` branch |

Both read `''` against a broker whose `ListGroups` version does not carry them, so one row shape works everywhere.

### Filtering

You can ask the brokers to filter instead of filtering the answer yourself:

```javascript
// Only KIP-848 consumer groups
await admin.listGroups({ types: ['consumer'] })

// Only groups that are stable or empty
await admin.listGroups({ states: ['stable', 'empty'] })

// Both filters at once
await admin.listGroups({ states: ['stable'], types: ['classic', 'consumer'] })
```

Filter values are given in lower case and are mapped to the names the wire uses:

| filter | accepted values |
| --- | --- |
| `states` | `unknown`, `preparingrebalance`, `completingrebalance`, `stable`, `dead`, `empty`, `assigning`, `reconciling`, `notready` |
| `types` | `unknown`, `classic`, `consumer`, `share`, `streams` |

A value Kafka does not define throws a `KafkaJSNonRetriableError` naming the accepted values, before anything is sent.

Older brokers cannot answer every filter, and this client follows the Java client's rules rather than quietly widening your query:

1. **A `states` filter needs `ListGroups` v4.** Against an older broker the call fails with `KafkaJSServerDoesNotSupportApiKey` — answering without the filter would be a different question.
2. **A `types` filter needs v5**, with one exception: asking for exactly `['classic']`, or for `['classic', 'consumer']`, is answered by an older broker with the filter dropped, because every group on such a broker is classic and the result is the same set.
3. **Any other type filter fails on an older broker**, `['consumer']` included: dropping it would answer "all groups" to "consumer groups only".

A filter a broker's version cannot serve fails on the first attempt and is not retried. A broker whose coordinator is loading or not yet available is retried on the configured retry budget.

The call still fans out to every broker, one after another, and is all-or-nothing: a single unreachable broker fails the whole call.

## <a name="describe-groups"></a> Describe groups

Describe consumer groups by `groupId`s. This is similar to [consumer.describeGroup()](Consuming.md#describe-group), except
it allows you to describe multiple groups and does not require you to have a consumer be part of any of those groups.

Classic groups and KIP-848 consumer groups are both described, in one shape:

```js
await admin.describeGroups([ 'testgroup' ])
// {
//   groups: [{
//     groupId: 'testgroup',
//     protocol: 'RoundRobinAssigner',    // the server-side assignor's name for a consumer group
//     protocolType: 'consumer',          // synthesised as 'consumer' for a consumer group, which carries none on the wire
//     state: 'Stable',
//     errorCode: 0,                      // group-level error for this id; 0 when the description succeeded
//     errorMessage: null,                // the broker's message for that error, when it sent one
//     groupType: 'classic',              // 'classic' or 'consumer'
//     groupEpoch: null,                  // consumer groups only; null for a classic group
//     assignmentEpoch: null,             // consumer groups only; null for a classic group
//     assignorName: null,                // consumer groups only; null for a classic group
//     authorizedOperations: null,        // raw mask when requested, otherwise null
//     members: [
//       {
//         clientHost: '/172.19.0.1',
//         clientId: 'test-3e93246fe1f4efa7380a',
//         memberId: 'test-3e93246fe1f4efa7380a-ff87d06d-5c87-49b8-a1f1-c4f8e3ffe7eb',
//         memberAssignment: Buffer,      // for a consumer-group member this client builds it from the assignment the broker sent
//         memberMetadata: Buffer,        // empty for a consumer-group member: the protocol carries none
//         groupInstanceId: null,         // static membership id, null for a dynamic member
//         rackId: null,
//         memberEpoch: null,             // consumer-group members only
//         assignment: [],                // [{ topic, partitions }] — decoded current assignment
//         targetAssignment: null,        // consumer-group members only
//         memberType: null,              // 'classic' or 'consumer', reported by ConsumerGroupDescribe v1 only
//         subscribedTopics: [],          // consumer-group members only
//         subscribedTopicRegex: null,    // consumer-group members only
//       },
//     ],
//   }]
// }
```

Two of those fields are produced by this client rather than sent by the broker, so that tooling written against a classic description keeps working against a consumer group: a consumer group's `protocolType` (`'consumer'`), and each consumer-group member's `memberAssignment` buffer, which is encoded here from the assignment the coordinator reported.

Errors are reported per group id, not per call:

- A group that does not exist comes back as a row in state `Dead` with `errorCode: 69` (`GROUP_ID_NOT_FOUND`) and the broker's `errorMessage`. The call resolves normally.
- Any other group-level error `ConsumerGroupDescribe` reports — an authorization failure, for instance — is carried on that group's row as `errorCode`/`errorMessage` while the other ids come back described. An id that falls back to `DescribeGroups` has no such row: that API reports any code other than `GROUP_ID_NOT_FOUND` by failing the call, as it always has.
- A coordinator that is loading or has moved is retried on the configured retry budget; a transport or coordinator error that outlives the budget still fails the call.
- **Rows are not returned in the order you passed the ids in.** Match them by `groupId`.

These calls model classic groups and KIP-848 consumer groups. A group id neither of those paths recognises falls back to `DescribeGroups`, so whatever that API answers for it is what you get, on a row carrying `groupType: 'classic'` because that is the API the row came from — against a 4.x broker expect a `Dead` row with `errorCode: 69` rather than a description. Streams groups are not modelled here. Share groups have their own call, [`describeShareGroups`](#describe-share-groups); `describeGroups` with a share group id is not modelled either and returns whatever the broker answers for that id.
Helper function to decode `memeberMetadata` and `memberAssignment` is available in `AssignerProtocol`

Example: 

`const memberMetadata = AssignerProtocol.MemberMetadata.decode(memberMetadata)`

`const memberAssignment = AssignerProtocol.MemberAssignment.decode(memberAssignment)`


## <a name="delete-groups"></a> Delete groups

Delete groups by `groupId`.

Note that you can only delete groups with no connected consumers.

```javascript
await admin.deleteGroups([groupId])
```

Example:

```javascript
await admin.deleteGroups(['group-test'])
```

Example response:

```javascript
[
    {groupId: 'testgroup', errorCode: 'consumer'}
]
```

Because this method accepts multiple `groupId`s, it can fail to delete one or more of the provided groups. In case of failure, it will throw an error containing the failed groups:

```javascript
try {
    await admin.deleteGroups(['a', 'b', 'c'])
} catch (error) {
  // error.name 'KafkaJSDeleteGroupsError'
  // error.groups = [{
  //   groupId: a
  //   error: KafkaJSProtocolError
  // }]
}
```

On brokers supporting DeleteGroups v3, each result includes `errorMessage: string | null`. Failed results in `KafkaJSDeleteGroupsError` retain the broker's per-group message. Older versions omit this optional field. The existing rejection and retry contracts are unchanged, except that a retry sends only the groups no coordinator has answered yet, so a group that was deleted is never sent again.

## <a name="describe-share-groups"></a> Describe share groups

Kafka 4.2+ share administration adds four methods. Each accepts the admin client's end-to-end `apiTimeout` budget (default 60000 ms). An unsupported API or version, or a disabled share feature, rejects with `KafkaJSShareGroupsNotSupportedError`. A local timeout after transmission leaves a mutation's remote outcome unknown.

```javascript
const { groups } = await admin.describeShareGroups(['orders-workers'], {
  includeAuthorizedOperations: true,
  apiTimeout: 10000,
})
```

Groups are batched by group coordinator. Each group result contains `groupId`, `state`, `groupEpoch`, `assignmentEpoch`, `assignorName`, `members`, `errorCode`, `errorMessage` and `authorizedOperations`. Members contain `memberId`, `rackId`, `memberEpoch`, `clientId`, `clientHost`, `subscribedTopics` and `assignment`; each assignment names `topic`, `topicId` and `partitions`. A failure of one group stays on its own result beside the successful groups.

Repeated ids share their work and produce fresh result snapshots in caller order, including repeated rows. `authorizedOperations` is a raw signed INT32 number that keeps unknown bits, or `null` when it was not requested or the broker sent the absent value. It is never converted to an array of operation names.

## <a name="list-share-group-offsets"></a> List share group offsets

```javascript
const all = await admin.listShareGroupOffsets({ groupId: 'orders-workers' })
const selected = await admin.listShareGroupOffsets({
  groupId: 'orders-workers',
  topics: [{ topic: 'orders', partitions: [0, 1] }],
  apiTimeout: 10000,
})
```

Omitting `topics` requests every topic the group has offsets for. An explicit empty array requests an empty selection. Results are arrays of `{ topic, topicId, partitions }`; partitions contain `partition`, a decimal-string `startOffset`, a numeric `leaderEpoch`, `lag`, `errorCode` and an optional `KafkaJSProtocolError`. Lag is a decimal string, or `null` when the broker reports it as unknown. Partition errors are data; a group-level error throws.

## <a name="alter-share-group-offsets"></a> Alter share group offsets

```javascript
const altered = await admin.alterShareGroupOffsets({
  groupId: 'orders-workers',
  topics: [{ topic: 'orders', partitions: [{ partition: 0, offset: '8' }] }],
  apiTimeout: 10000,
})
```

The group must have no members. Every offset must be an exact non-negative decimal string within signed int64; the whole selection is checked before any lookup or request. Results are arrays of `{ topic, topicId, partitions: [{ partition, errorCode, error? }] }`. Partition failures are data and group failures throw. An earliest, latest or by-timestamp reset composes `fetchTopicOffsets` or `fetchTopicOffsetsByTimestamp` with this call.

## <a name="delete-share-group-offsets"></a> Delete share group offsets

```javascript
const deleted = await admin.deleteShareGroupOffsets({
  groupId: 'orders-workers',
  topics: ['orders'],
  apiTimeout: 10000,
})
```

The group must no longer subscribe to the named topics. Results are arrays of `{ topic, topicId, errorCode, error? }`; topic failures are data and group failures throw. No `deleteShareGroups` method exists: delete a share group with [`deleteGroups`](#delete-groups). On a broker that offers DeleteGroups v3, each result carries the broker's `errorMessage`.

Use describeShareGroups for share-group membership. The existing describeGroups call retains its generic-group behavior. Captured public outcomes (group ids normalized) are: Kafka 4.3.1, empty: {"kind":"return","value":{"groups":[{"groupId":"share-group","protocolType":"","protocol":"","state":"Dead","errorCode":69,"errorMessage":"Group share-group is not a consumer group.","groupType":"classic","groupEpoch":null,"assignmentEpoch":null,"assignorName":null,"authorizedOperations":null,"members":[]}]}}; Kafka 4.3.1, member-present: {"kind":"return","value":{"groups":[{"groupId":"share-group","protocolType":"","protocol":"","state":"Dead","errorCode":69,"errorMessage":"Group share-group is not a consumer group.","groupType":"classic","groupEpoch":null,"assignmentEpoch":null,"assignorName":null,"authorizedOperations":null,"members":[]}]}}; Kafka 4.4.0, empty: {"kind":"return","value":{"groups":[{"groupId":"share-group","protocolType":"","protocol":"","state":"Dead","errorCode":69,"errorMessage":"Group share-group is not a consumer group.","groupType":"classic","groupEpoch":null,"assignmentEpoch":null,"assignorName":null,"authorizedOperations":null,"members":[]}]}}; Kafka 4.4.0, member-present: {"kind":"return","value":{"groups":[{"groupId":"share-group","protocolType":"","protocol":"","state":"Dead","errorCode":69,"errorMessage":"Group share-group is not a consumer group.","groupType":"classic","groupEpoch":null,"assignmentEpoch":null,"assignorName":null,"authorizedOperations":null,"members":[]}]}}. These are captured broker/client results; they do not describe share membership. The fixture src/admin/__tests__/fixtures/describeGroupsShareGroup.json and its replay pin this behavior.

## <a name="reset-share-group-offsets"></a> Reset an empty share group's offsets

Stop every group member before altering offsets. Compose `fetchTopicOffsets` with `alterShareGroupOffsets` for earliest/latest, or `fetchTopicOffsetsByTimestamp` with `alterShareGroupOffsets` for a timestamp. The existing timestamp lookup falls back to the partition's latest offset when the requested timestamp has no match. Offsets remain decimal strings, including values above Number.MAX_SAFE_INTEGER. This is an application recipe, not a new admin method, seek operation or atomic reset across calls. A member can rejoin between inspection and mutation; the broker's refusal remains authoritative. Group errors reject; inspect per-partition errors before treating a reset as successful. Do not automatically retry an unanswered mutation to infer its outcome.

```javascript
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

## <a name="delete-share-group"></a> Delete an empty share group

Use `await admin.deleteGroups(['orders-workers'])`; no `deleteShareGroups` alias is added. Stop all members first and inspect the real result. The offset deletion method has a different precondition: the group must no longer subscribe to the named topics. Use `describeShareGroups` for membership and share assignment fields.

## <a name="remove-members"></a> Remove members from a consumer group

Remove members from a group that no longer has the instances behind them, for example a static member whose pod is gone and whose slot would otherwise hold its partitions until `sessionTimeout` passes. It is not a way to stop a consumer: a member that is still running rejoins on its next heartbeat.

```javascript
await admin.removeMembersFromConsumerGroup({
    groupId: <String>,
    members: <{ groupInstanceId: String }[]>, // optional
    reason: <String> // optional
})
```

- With `members`, each entry names a **static** member by its `groupInstanceId` (an entry repeated is sent once). An empty list, or a blank `groupInstanceId`, throws `KafkaJSNonRetriableError`.
- Without `members`, every member `describeGroups` reports for the group is removed, static ones by instance id and dynamic ones by member id, under both group protocols. A group with no member, or that does not exist, resolves `[]` and sends nothing; a group of another protocol type (a Kafka Connect group, say) is refused with `KafkaJSNonRetriableError`.
- `reason` is what the coordinator logs. It defaults to `"member was removed by an admin"`, which is also used for a `null` or empty `reason`, as in Java; it is cut to 255 characters and sent to brokers that offer LeaveGroup v5, which every Kafka 4.x broker does.

A member that could not be removed is a result, not an error. The call resolves one result per member sent, and only a failure of the request itself rejects:

```javascript
const results = await admin.removeMembersFromConsumerGroup({ groupId: 'orders', members: [{ groupInstanceId: 'orders-pod-3' }] })
// [{ memberId: '', groupInstanceId: 'orders-pod-3', errorCode: 0, error: null }]
//
// errorCode 25, error.type 'UNKNOWN_MEMBER_ID': the group has no such member (a group that does not exist answers this for every member)

const failed = results.filter(({ error }) => error)
```

A coordinator that is not the right one, is not available or is still loading the group is asked again, on the admin's `retry` budget; any other error of the request rejects.

## <a name="delete-topic-records"></a> Delete Topic Records

Delete records for a selected topic. This will delete all records from the earliest offset up to - but not including - the provided target offset for the given partition(s). To delete all records in a partition, use a target offset of `-1`.

Note that you cannot delete records in an arbitrary range (it will always be from the earliest available offset)

The topic's offsets are read first, by a metadata request that names the topic: a partition without a leader is waited for within the `retry` budget, and one that stays without a leader fails the call as it fails `fetchTopicOffsets`, with a `KafkaJSNonRetriableError` whose `type` is `LEADER_NOT_AVAILABLE`, before anything is deleted. Each partition's request then goes to the leader the metadata names. A request that fails with a retriable error, such as a lost connection, is retried within the `retry` budget, at the same broker, and a broker that already deleted its partitions is not asked again; once the budget is spent the call rejects with `KafkaJSNumberOfRetriesExceeded`, whose `cause` is the last error. A partition the topic does not have rejects at once with `KafkaJSDeleteTopicRecordsError`, before anything is deleted, once that offsets read has succeeded: on a topic that stays without a leader, the offsets read fails first, with the `KafkaJSNonRetriableError` above.

This call returns `Promise<void>`: full success resolves to `undefined` only after all requested partitions succeed. A partial broker refusal retains the existing error shape: the typed `KafkaJSDeleteTopicRecordsError` in the error/cause chain lists only failing partitions and their errors. A non-retriable broker refusal keeps its `KafkaJSNonRetriableError` wrapper; exhausted retries keep `KafkaJSNumberOfRetriesExceeded`. A successful broker is not sent again on retries. This method returns no low watermarks; read them with `fetchTopicOffsets`.

```javascript
await admin.deleteTopicRecords({
    topic: <String>,
    partitions: <SeekEntry[]>,
})
```

Example:

```javascript
await admin.deleteTopicRecords({
    topic: 'custom-topic',
    partitions: [
        { partition: 0, offset: '30' }, // delete up to and including offset 29
        { partition: 3, offset: '-1' }, // delete all available records on this partition
    ]
})
```

## <a name="create-acl"></a> Create ACL

```javascript
const {
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
  ResourcePatternTypes,
} = require('@countly/kafkajs')

const acl = [
  {
    resourceType: AclResourceTypes.TOPIC,
    resourceName: 'topic-name',
    resourcePatternType: ResourcePatternTypes.LITERAL,
    principal: 'User:bob',
    host: '*',
    operation: AclOperationTypes.ALL,
    permissionType: AclPermissionTypes.DENY,
  },
  {
    resourceType: AclResourceTypes.TOPIC,
    resourceName: 'topic-name',
    resourcePatternType: ResourcePatternTypes.LITERAL,
    principal: 'User:alice',
    host: '*',
    operation: AclOperationTypes.ALL,
    permissionType: AclPermissionTypes.ALLOW,
  },
]

await admin.createAcls({ acl })
```

`resourceType` takes `TOPIC`, `GROUP`, `CLUSTER`, `TRANSACTIONAL_ID`, `DELEGATION_TOKEN` or `USER` (a user principal) from `AclResourceTypes`; `ANY` is for filters only, and the broker refuses `ANY` and `UNKNOWN` on create. The same values work in the filters of [Delete ACL](#delete-acl) and [Describe ACL](#describe-acl). Every Kafka 4.x broker offers the ACL APIs at the version that carries it.

Be aware that the security features might be disabled in your cluster. In that case, the operation will throw an error:

```sh
KafkaJSProtocolError: Security features are disabled
```

## <a name="delete-acl"></a> Delete ACL

```javascript
const {
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
  ResourcePatternTypes,
} = require('@countly/kafkajs')

const acl = {
  resourceName: 'topic-name,
  resourceType: AclResourceTypes.TOPIC,
  host: '*',
  permissionType: AclPermissionTypes.ALLOW,
  operation: AclOperationTypes.ANY,
  resourcePatternType: ResourcePatternTypes.LITERAL,
}

await admin.deleteAcls({ filters: [acl] })
// {
//   filterResponses: [
//     {
//     errorCode: 0,
//     errorMessage: null,
//     matchingAcls: [
//         {
//         errorCode: 0,
//         errorMessage: null,
//         resourceType: AclResourceTypes.TOPIC,
//         resourceName: 'topic-name',
//         resourcePatternType: ResourcePatternTypes.LITERAL,
//         principal: 'User:alice',
//         host: '*',
//         operation: AclOperationTypes.ALL,
//         permissionType: AclPermissionTypes.ALLOW,
//         },
//     ],
//     },
//   ],
// }
```

Be aware that the security features might be disabled in your cluster. In that case, the operation will throw an error:

```sh
KafkaJSProtocolError: Security features are disabled
```

## <a name="describe-acl"></a> Describe ACL

```javascript
const {
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
  ResourcePatternTypes,
} = require('@countly/kafkajs')

await admin.describeAcls({
  resourceName: 'topic-name,
  resourceType: AclResourceTypes.TOPIC,
  host: '*',
  permissionType: AclPermissionTypes.ALLOW,
  operation: AclOperationTypes.ANY,
  resourcePatternTypeFilter: ResourcePatternTypes.LITERAL,
})
// {
//   resources: [
//     {
//       resourceType: AclResourceTypes.TOPIC,
//       resourceName: 'topic-name,
//       resourcePatternType: ResourcePatternTypes.LITERAL,
//       acls: [
//         {
//           principal: 'User:alice',
//           host: '*',
//           operation: AclOperationTypes.ALL,
//           permissionType: AclPermissionTypes.ALLOW,
//         },
//       ],
//     },
//   ],
// }
```

Be aware that the security features might be disabled in your cluster. In that case, the operation will throw an error:

```sh
KafkaJSProtocolError: Security features are disabled
```

## Log directories

`describeLogDirs({ brokers, topicPartitions })` reads the selected brokers, or all cached brokers when omitted. Omitted or null `topicPartitions` selects all topics; an explicit empty filter selects none. Results retain each broker and directory's `errorCode`, typed `error` and `errorMessage`. Partition `size` and `offsetLag` are decimal strings. `totalBytes` and `usableBytes` require DescribeLogDirs v4, and `isCordoned` requires v5; unsupported fields are null.

`alterReplicaLogDirs({ replicas: [{ topic, partition, brokerId, logDir }] })` moves each replica to an absolute directory configured on its broker and returns per-replica outcomes. Successful mutations are retained while unresolved replicas retry. `describeReplicaLogDirs({ replicas })` reports current and future directories and offset lags; a replica absent on the broker has null locations. Source storage errors remain visible. Capability, authentication and validation failures reject the whole call even if another broker has completed; a late route or transport failure can be retained on the unresolved broker or item. Completed peers are not resent or disconnected. All three methods accept `apiTimeout`; an expired mutation may have reached the broker and has an unknown outcome.

## <a name="alter-partition-reassignments"></a> Alter Partition Reassignments
This is used to reassign the replicas that partitions are on. This method will throw exceptions in the case of errors.

```typescript
await admin.alterPartitionReassignments({
  topics: <PartitionReassignment[]>,
  timeout: <Number> // optional - 5000 default
})
```

PartitionReassignment Structure:
```typescript
{
  topic: <String>,
  partitionAssignment: <Number[]> // Example: [{ partition: 0, replicas: [0,1,2] }]
}
```

When some partitions are refused and others are not, the retries send only the partitions the broker said could still go through, each with the replicas you gave, and a topic whose partitions all went through is not sent again. A partition refused for good rejects the call with a `KafkaJSAggregateError`, as described under [Create topics](#create-topics). A topic whose `partitionAssignment` is empty is not sent.

`allowReplicationFactorChange` defaults to true. Set it to false to reject a replication-factor change while allowing other reassignments. False requires AlterPartitionReassignments v1; a v0 broker rejects the option before sending. The method keeps its void success and thrown aggregate errors, retaining terminal children and retrying only the retriable subset.

## <a name="list-partition-reassignments"></a> List Partition Reassignments
This is used to list current partition reassignments in progress. This method will throw exceptions in the case of errors and resolve to ListPartitionReassignmentsResponse on success. If a requested partition does not exist it will not be included in the response.

```javascript
await admin.listPartitionReassignments({
  topics: <TopicPartitions[]>, // optional, if null then all topics will be returned.
  timeout: <Number> // optional - 5000 default
})
```

TopicPartitions Structure:
```typescript
{
  topic: <String>,
  partitions: <Array>
}
```

Resulting ListPartitionReassignmentsResponse Structure:
```typescript
{
  topics: <OngoingTopicReassignment[]>
}
```
OngoingTopicReassignment Structure:
```typescript
{
  topic: <String>,
  partitions: <OngoingPartitionReassignment[]>
}
```
OngoingPartitionReassignment Structure:
```typescript
{
  partitionIndex: <Number>,
  replicas: <Number[]>, // The current replica set
  addingReplicas: <Number[]> // The set of replicas being added
  removingReplicas: <Number[]> // The set of replicas being removed
}
```
**Note:** If a partition is not going through a reassignment, its AddingReplicas and RemovingReplicas fields will simply be empty.

## Transaction administration

`describeProducers` queries partition leaders by default. Pass `brokerId` to query that replica for every selected partition. A fixed replica's `NOT_LEADER_OR_FOLLOWER` is returned as partition data; automatic leader discovery relocates on that error. Producer state includes `producerId`, `producerEpoch`, `lastSequence`, `lastTimestamp`, `coordinatorEpoch` and `currentTxnStartOffset`. Every 64-bit value is a decimal string.

```javascript
const producers = await admin.describeProducers({
  topicPartitions: [{ topic: 'orders', partitions: [0, 2] }],
  apiTimeout: 10000,
})
const descriptions = await admin.describeTransactions({ transactionalIds: ['payments'] })
const listed = await admin.listTransactions({
  states: ['Ongoing'],
  producerIds: ['9007199254740993'],
  durationFilter: 60000,
  transactionalIdPattern: '^payments-.*$',
})
```

`describeTransactions` locates each transaction coordinator and batches ids sharing one coordinator. Its per-id results include state, timeout, start time, producer identity and participating topics. `listTransactions` asks every broker sequentially and returns `{ transactions, unknownStateFilters }`; any broker's final failure rejects the whole call. State names and unknown filters are broker strings. `transactionalIdPattern` is a RE2 string evaluated by the broker; it is never compiled as a JavaScript `RegExp`. A meaningful pattern needs ListTransactions v2; a duration filter needs v1. An unsupported meaningful filter throws `KafkaJSServerDoesNotSupportApiKey` naming the option, the version it needs and the broker's offered range. Omitted or empty filter arrays, and an empty pattern, mean no filter.

`describeProducers`, `describeTransactions` and `fenceProducers` return `errorCode`, typed `error` and nullable `errorMessage` on each requested partition or id. Code 0 means success, a protocol code means a broker failure, and code -1 with a typed `error` means an unresolved local route, connection-setup or transport failure; that `error` is the original one, even when it carries a protocol code. Completed items are retained through retries and exhaustion. Unresolved local transaction descriptions use `Unknown`, timeout 0, decimal `'-1'` start/id, epoch -1 and empty topics; unsuccessful fences with no granted identity use null producerId/epoch.

```javascript
await admin.abortTransaction({
  topic: 'orders',
  partition: 0,
  producerId: '9007199254740993',
  producerEpoch: 7,
  coordinatorEpoch: 9,
})
const fenced = await admin.fenceProducers({
  transactionalIds: ['payments'],
  transactionTimeout: 60000,
})
```

`abortTransaction` sends one administrative abort marker to one partition leader, requires administrative broker permission and resolves `undefined` on success. It checks the exact marker identity. It does not erase records from the log or call the producer's `abort()` method; aborted transactional data remains physically present and is hidden from a `read_committed` consumer. `fenceProducers` uses FindCoordinator plus InitProducerId, with no existing producer identity, for each id. Its `transactionTimeout` defaults to 60000 ms and is independent of the API deadline. The Java `forceTerminateTransaction(id)` equivalent is `admin.fenceProducers({ transactionalIds: [id] })`. Once a producer observes its fencing failure, create a new producer instance as described in the transaction failure guidance.

Public producer IDs must be nonnegative safe integral numbers or canonical decimal strings through 9223372036854775807. Use strings above Number.MAX_SAFE_INTEGER. Partitions/broker ids are integers 0..2147483647; abort producer epochs 0..32767 and coordinator epochs 0..2147483647. Transaction ids, topics and mutation lists are nonempty and unique. `durationFilter` is nonnegative safe integral milliseconds; `transactionTimeout` is integer 1..2147483647 ms. All five APIs accept `apiTimeout` and inherit the configured admin default. A timed-out mutation can already have applied remotely; inspect the broker state before deciding to repeat it.

## Client quotas

`describeClientQuotas({ components, strict = false, apiTimeout })` reads matching entities. `components` is an array of `{ entityType, matchType, match? }`; entity types are `user`, `client-id` and `ip`. Each type appears once. An `exact` match requires a nonempty `match`; `default` and `any` omit it. Empty components selects all. Results are `{ entity, values }`, with numeric quota values.

`alterClientQuotas({ entries, validateOnly = false, apiTimeout })` accepts nonempty entries of `{ entity, ops }`. Entity parts are `{ entityType, entityName }`, with null selecting the default entity. An operation is `{ key, value }` for a finite nonnegative numeric assignment or `{ key, remove: true }` for removal. Removal forbids value. Keys are the broker's strings; the client validates their shape and lets the broker validate their meaning. Duplicate entities, tuple types and operation keys are refused before discovery.

Results retain input entity order and carry `errorCode`, a typed `error` or null, and `errorMessage`. Only unresolved retriable entries are retried. Completed successes and terminal results remain retained. A later route or connection-setup failure gives unresolved entries code -1 and the exact original local error, even when that error has a protocol code. Completed mutations are not sent again. `validateOnly: true` sends the broker's validation flag without applying changes or making an effects-polling request. Check state after an API timeout because an already-sent mutation may have applied.

```js
await admin.alterClientQuotas({
  entries: [{ entity: [{ entityType: 'user', entityName: 'service' }], ops: [{ key: 'producer_byte_rate', value: 1048576 }] }],
  validateOnly: true,
})
const quotas = await admin.describeClientQuotas({ components: [{ entityType: 'user', matchType: 'exact', match: 'service' }] })
```

## SCRAM credentials

`describeUserScramCredentials({ users, apiTimeout }?)` returns per-user credential mechanisms/iteration counts and typed result errors. Omitted or null users selects all; an explicit empty list returns no results without a request. The request's own top-level broker error rejects the call. After users are known, a later route or setup failure retains completed users and gives unresolved users code -1 with the original typed local error. Passwords and derived credentials are never returned.

`alterUserScramCredentials({ upsertions, deletions, apiTimeout })` requires at least one change. An upsertion is `{ user, mechanism, iterations, password, salt? }`; a deletion is `{ user, mechanism }`. Mechanisms are `SCRAM-SHA-256` or `SCRAM-SHA-512`, iterations are integers from 4096 to 16384, and password is a nonempty string. Salt is an optional Buffer of 1..1024 bytes; the default is 32 random bytes. Each user/mechanism pair appears once across both lists.

The client derives the salted password locally, sends the salt and derived bytes rather than plaintext, and erases its owned working buffers after the call. A supplied salt Buffer is copied and remains caller-owned. Credential data and raw broker echoes are omitted from logs and result messages; diagnose from the typed error and protocol code. Results are one entry per affected user, in first-request order. Controller relocation and retriable rows resend only unresolved users. A later route or setup failure retains completed users and attaches the original typed local error with code -1 only to unresolved users. Capability, validation, invariant and deadline refusals still reject the whole call. The bootstrap `scripts/createScramCredentials.sh` remains available.

```js
await admin.alterUserScramCredentials({
  upsertions: [{ user: 'service', mechanism: 'SCRAM-SHA-256', iterations: 4096, password: process.env.SCRAM_PASSWORD }],
})
const credentials = await admin.describeUserScramCredentials({ users: ['service'] })
```

## Delegation tokens

`createDelegationToken({ renewers = [], maxLifetimeMs = -1, owner, apiTimeout }?)` creates a token. Principal pairs use nonempty `principalType` and `principalName`. An explicit owner requires broker support for CreateDelegationToken v3 and is refused below it. The default owner is the authenticated request principal.

`renewDelegationToken({ hmac, renewPeriodMs = -1, apiTimeout })` and `expireDelegationToken({ hmac, expiryTimePeriodMs = -1, apiTimeout })` take a nonempty Buffer HMAC and return `{ expiryTimestampMs }`. Durations accept safe integral numbers or canonical decimal int64 strings. Create/renew accept -1 for the broker default or a positive value; expire also accepts 0 for immediate expiration. Other negative values, fractions, numeric strings with exponents/whitespace and unsafe numbers are refused.

`describeDelegationToken({ owners, apiTimeout }?)` selects all visible owners when owners is omitted. An explicit empty list returns no results without a request. Owner/renewer lists reject duplicate principals. A token has `tokenId`, Buffer `hmac`, `owner`, `renewers`, and decimal-string `issueTimestampMs`, `expiryTimestampMs`, `maxTimestampMs`.

HMAC is returned only to the authorized caller and is not cached or logged by the client. Token-admin diagnostics omit response data and failure payloads even when buffer diagnostics are enabled. A broker without token authentication returns the typed `DELEGATION_TOKEN_AUTH_DISABLED` error. Use an authenticated SASL listener on a broker with delegation tokens configured.

```js
const token = await admin.createDelegationToken({ renewers: [{ principalType: 'User', principalName: 'service' }] })
await admin.renewDelegationToken({ hmac: token.hmac })
const tokens = await admin.describeDelegationToken({ owners: [{ principalType: 'User', principalName: 'service' }] })
await admin.expireDelegationToken({ hmac: token.hmac, expiryTimePeriodMs: 0 })
```

### Validation-only method table

Exactly six Kafka requests carry a validation flag. `true` requests validation without applying a mutation, follow-up effects request or hypothetical cache update. UpdateFeatures refuses `true` on v0.

| Method | Wire support | Guarantee |
|---|---|---|
| `createTopics` | CreateTopics from v1 | Validates without mutation, follow-up effects request or hypothetical cache update. |
| `createPartitions` | CreatePartitions from v0 | Validates without mutation, follow-up effects request or hypothetical cache update. |
| `alterConfigs` | AlterConfigs from v0 | Validates without mutation, follow-up effects request or hypothetical cache update. |
| `incrementalAlterConfigs` | IncrementalAlterConfigs from v0 | Validates without mutation, follow-up effects request or hypothetical cache update. |
| `alterClientQuotas` | AlterClientQuotas from v0 | Validates without mutation, follow-up effects request or hypothetical cache update. |
| `updateFeatures` | UpdateFeatures v1+ | `validateOnly: true` validates without applying the change; no follow-up mutation or local cache update. |

Delete operations, offset commits/reset helpers, ACL mutation, member removal, partition reassignment, leader election, transaction abort/fencing, replica-directory changes, SCRAM changes and token mutations have no wire validation form. Fetch/list/describe operations are reads; connect/disconnect are lifecycle operations. These methods do not offer `validateOnly`.
