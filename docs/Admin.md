---
id: admin
title: Admin Client
---

The admin client hosts all the cluster operations, such as: `createTopics`, `createPartitions`, etc.

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

When a controller-mutation quota throttles `createTopics`, `deleteTopics` or `createPartitions` (KIP-599), the broker refuses the topics it did not process with `THROTTLING_QUOTA_EXCEEDED` (89) and reports how long to wait. The admin client sends only those topics again, within the `retry` budget, and the connection holds the next request back for the throttle time the answer carried, so a call that a quota only slows still succeeds, a little later. The topics the broker did process are not sent again. Once the `retries` are spent the call rejects with `KafkaJSNumberOfRetriesExceeded`, whose `cause` is the quota error (for `createTopics`, the `KafkaJSAggregateError` that lists the refused topics). When another topic failed in the same answer, nothing is sent again for the quota and that failure is handled as it always has been. Java's `Admin` retries these refusals until its timeout (`retryOnQuotaViolation`). The throttle wait comes on top of the retry backoff: `retry.maxRetryTime` only caps one backoff delay, so by default nothing bounds the call as a whole, and the worst case is about `retries` throttle times plus the backoff delays, where Java bounds the whole call at its default 60 s (`default.api.timeout.ms`). To bound it, set `retry.maxTotalTime`: it counts the time already spent, throttle waits included, and the call then rejects with `KafkaJSDeliveryTimeout`, whose `cause` is the quota error; the attempt it lets start can still wait out one more throttle time.

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

### Describing topics partition by partition

With `useDescribeTopicPartitions: true` and at least one topic name, the topics are read with DescribeTopicPartitions (KIP-966) instead of Metadata:

- Any broker answers. The client follows the broker's pages to the end (up to 2000 partitions a page, fewer if the broker's `max.request.partition.size.limit` is lower), so a topic with many partitions is not cut off; a broker whose pages repeat, or that does not answer for a topic, fails the call with `KafkaJSNonRetriableError` instead of returning a partial answer. A request that fails in a way that can pass (a dropped connection, a timeout) is asked again on the `retry` budget after a metadata refresh, as the other admin calls are. The topics come back in name order.
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

## <a name="fetch-topic-offsets"></a> Fetch topic offsets

`fetchTopicOffsets` returns most recent offset for a topic.

```javascript
await admin.fetchTopicOffsets(topic)
// [
//   { partition: 0, offset: '31004', high: '31004', low: '421' },
//   { partition: 1, offset: '54312', high: '54312', low: '3102' },
//   { partition: 2, offset: '32103', high: '32103', low: '518' },
//   { partition: 3, offset: '28', high: '28', low: '0' },
// ]
```

## <a name="fetch-topic-offsets-by-timestamp"></a> Fetch topic offsets by timestamp

Specify a `timestamp` to get the earliest offset on each partition where the message's timestamp is greater than or equal to the given timestamp.

```javascript
await admin.fetchTopicOffsetsByTimestamp(topic, timestamp)
// [
//   { partition: 0, offset: '3244' },
//   { partition: 1, offset: '3113' },
// ]
```

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

Allows you to get information about the broker cluster. This is mostly useful
for monitoring or operations, and is usually not relevant for typical event processing.

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
//     authorizedOperations: null,        // always null today: authorized operations are not requested
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

These calls model classic groups and KIP-848 consumer groups. A group id neither of those paths recognises falls back to `DescribeGroups`, so whatever that API answers for it is what you get, on a row carrying `groupType: 'classic'` because that is the API the row came from — against a 4.x broker expect a `Dead` row with `errorCode: 69` rather than a description. Share groups and streams groups are not modelled here and are not covered by these calls.
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