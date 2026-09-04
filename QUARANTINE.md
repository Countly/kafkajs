# Quarantined tests

Tests skipped while bringing the fork up on Apache Kafka 4.x (Phase 1, Task 9). Every row is a
`test.skip` / `it.skip` in the named spec, marked in-file with a comment pointing here. No file
under `src/` other than `*.spec.js` and `__snapshots__/*.snap` was changed to make the suite green.

| Spec | Test | Reason | Class | Owner phase |
|---|---|---|---|---|
| `src/consumer/__tests__/instrumentationEvents.spec.js` | Consumer > Instrumentation Events &gt; emits request timeout events | flaky: `requestTimeout: 1` races a sub-millisecond loopback round trip, so the timeout often never fires (`Number of calls: 0`); observed once in ~12 group runs | (c) | Phase 2 (rewrite: drive the timeout from a stubbed socket instead of a 1 ms budget) |
| `src/broker/__tests__/listPartitionReassignments.spec.js` | Broker > alterPartitionReassignments &gt; request | flaky: the assertion requires the reassignment to still be *in flight* (`addingReplicas: [2], removingReplicas: [1]`), but the partition is empty and the controller completes the reassignment in milliseconds, so under load the list request arrives afterwards and `response.topics` is `[]` (`TypeError: Cannot read properties of undefined (reading 'partitions')`). Observed once in ~7 broker-group runs; 8/8 green when the spec runs alone, i.e. it fails exactly when the group is slow — which is the CI condition. Cannot be stabilised from test infra: replication throttles do not hold an empty partition. The v0 response decoder is still covered by `src/protocol/requests/listPartitionReassignments/v0/response.spec.js` | (c) | Phase 2 (rewrite: produce data into the partition and apply a follower replication throttle so the reassignment stays pending, or assert the completed shape instead) |

## Root causes

- **R1 — auto topic creation / metadata propagation (retired Phase 2, task 25).** In KRaft the
  broker answers a `Metadata` request for a not-yet-created topic with `UNKNOWN_TOPIC_OR_PARTITION`
  and creates the topic asynchronously through the controller; ZooKeeper-era brokers created it
  inline and answered `LEADER_NOT_AVAILABLE`. `BrokerPool#refreshMetadata` (`src/cluster/brokerPool.js`)
  and `Admin`'s `retryOnLeaderNotAvailable` (`src/admin/index.js`) now also retry on
  `UNKNOWN_TOPIC_OR_PARTITION` when the broker/cluster allows auto topic creation, matching the
  existing `LEADER_NOT_AVAILABLE` guard; with auto creation disabled the error still surfaces on
  the first attempt. Adopts upstream [tulios/kafkajs#1558](https://github.com/tulios/kafkajs/pull/1558)
  ("refreshMetadata should retry if topic does not exist and allowAutoTopicCreation…"), reported as
  [#1752](https://github.com/tulios/kafkajs/issues/1752) (see also #815). The previously-quarantined
  test — `src/admin/__tests__/fetchTopicMetadata.spec.js` > Admin > fetchTopicMetadata > creates a
  new topic if the topic does not exist and "allowAutoTopicCreation" is true — is unskipped and
  green (verified 3/3 cold runs, plus the full `admin` and `cluster` groups).

  `testHelpers.createTopic`'s bounded retry and the explicit-topic-creation specs
  (`src/cluster/__tests__/brokerPool.spec.js`, `src/broker/__tests__/describeGroups.spec.js`) are
  test infrastructure, not part of the quarantine, and are left as-is.

## Not quarantined

`--ci` runs also report skips that are **not** quarantine: the version-conditional helpers in
`testHelpers/index.js` (`testIfKafkaAtMost_0_10`, `testIfKafkaAtLeast_0_11`,
`testIfKafkaAtLeast_1_1_0`) and the OAUTHBEARER gates (`describeIfOauthbearerEnabled` /
`describeIfOauthbearerDisabled`), which select tests by `KAFKA_VERSION` and `OAUTHBEARER_ENABLED`.
Those are pre-existing and unrelated to this task.

There is also one pre-existing upstream skip that this task did not add and does not own:
`src/consumer/__tests__/instrumentationEvents.spec.js` — `emits request queue size events`
(commented "This test is too flaky, we need to think about a better way of testing this").

Counting check: `grep -rn '\.skip(' --include='*.spec.js' src/` returns 3 — the 2 rows above plus
that one pre-existing skip.
