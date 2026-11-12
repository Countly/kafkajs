# Quarantined tests

Tests skipped while bringing the fork up on Apache Kafka 4.x. A quarantined test is a
`test.skip` / `it.skip` in its spec, marked in-file with a comment pointing here, and is listed
below until it is rewritten. No file under `src/` other than `*.spec.js` and
`__snapshots__/*.snap` was changed to make the suite green.

No test is quarantined. The last three skipped specs were rewritten with controlled timing and
run again: the request timeout event and the request queue size events
(`src/consumer/__tests__/instrumentationEvents.spec.js`) on the in-process scripted cluster, and
the partition reassignment listing (`src/broker/__tests__/listPartitionReassignments.spec.js`)
as its completed shape on the live cluster and its in-flight shape on the scripted cluster.

A test that fails and then passes on a rerun of the same commit follows the flake rule in
[docs/Testing.md](docs/Testing.md#skipped-tests): it is fixed or listed here before the next merge
to `master`.

## Root causes

- **R1 — auto topic creation / metadata propagation (retired).** In KRaft the
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

Counting check: `grep -rn '\.skip(' --include='*.spec.js' src/` returns 0.
