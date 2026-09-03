# Quarantined tests

Tests skipped while bringing the fork up on Apache Kafka 4.x (Phase 1, Task 9). Every row is a
`test.skip` / `it.skip` in the named spec, marked in-file with a comment pointing here. No file
under `src/` other than `*.spec.js` and `__snapshots__/*.snap` was changed to make the suite green.

| Spec | Test | Reason | Class | Owner phase |
|---|---|---|---|---|
| `src/admin/__tests__/fetchTopicMetadata.spec.js` | Admin > fetchTopicMetadata &gt; creates a new topic if the topic does not exist and "allowAutoTopicCreation" is true | Phase 2 (R1): client-side auto topic creation is broken on KRaft — the broker answers UNKNOWN_TOPIC_OR_PARTITION while creating the topic and the client only retries LEADER_NOT_AVAILABLE. Upstream: tulios/kafkajs#1558, issue #1752 | (b) | Phase 2 |
| `src/broker/__tests__/listOffsets.spec.js` | Broker > ListOffsets &gt; v0 &gt; request | Phase 2 (R2): version negotiation ignores the broker `minVersion`; Kafka 4.0 removed ListOffsets v0 (KIP-896), so pinning v0 closes the connection instead of returning UNSUPPORTED_VERSION | (b) | Phase 2 |
| `src/consumer/__tests__/instrumentationEvents.spec.js` | Consumer > Instrumentation Events &gt; emits request timeout events | flaky: `requestTimeout: 1` races a sub-millisecond loopback round trip, so the timeout often never fires (`Number of calls: 0`); observed once in ~12 group runs | (c) | Phase 2 (rewrite: drive the timeout from a stubbed socket instead of a 1 ms budget) |
| `src/consumer/__tests__/consumeMessages.spec.js` | Consumer > transactions &gt; respects offsets sent by a committed transaction ("consume-transform-produce" flow) | Phase 2 (R3): `eosManager.sendOffsets()` does not retry `CONCURRENT_TRANSACTIONS` on AddOffsetsToTxn (`src/producer/eosManager/index.js:385`), although `initProducerId` in the same file does (`:170`) and Kafka expects clients to retry that code. When un-skipping, restore `testIfKafkaAtLeast_0_11` in place of `test.skip` | (b) | Phase 2 |

## Root causes

- **R1 — auto topic creation / metadata propagation.** In KRaft the broker answers a `Metadata`
  request for a not-yet-created topic with `UNKNOWN_TOPIC_OR_PARTITION` and creates the topic
  asynchronously through the controller; ZooKeeper-era brokers created it inline and answered
  `LEADER_NOT_AVAILABLE`. `src/cluster/brokerPool.js:208-213` (`refreshMetadata`: re-throw only on
  `LEADER_NOT_AVAILABLE`, otherwise `bail`) and `src/admin/index.js:33-45`
  (`retryOnLeaderNotAvailable`) therefore give up on the first attempt. Reproduced directly: a first
  `broker.metadata([newTopic])` throws `UNKNOWN_TOPIC_OR_PARTITION`, the second succeeds. Upstream
  fix in flight: [tulios/kafkajs#1558](https://github.com/tulios/kafkajs/pull/1558) ("refreshMetadata
  should retry if topic does not exist and allowAutoTopicCreation…"), reported as
  [#1752](https://github.com/tulios/kafkajs/issues/1752).

  Only the one test above is quarantined for R1 — the single place where auto creation is the
  behavior under test. Everywhere else the race only affected *fixture setup*, and is handled
  without hiding the bug: `testHelpers.createTopic` retries the two transient error types with a
  bounded backoff (10 × 200 ms), and specs that need a topic now create it explicitly instead of
  relying on the client to auto-create it (`src/cluster/__tests__/brokerPool.spec.js`,
  `src/broker/__tests__/describeGroups.spec.js`).

- **R3 — CONCURRENT_TRANSACTIONS not retried on AddOffsetsToTxn.** Kafka returns this code while a
  previous transaction on the same transactional id is still completing and expects the client to
  retry (kafkajs' own comment in `src/protocol/error.js:322-327` says exactly that). `eosManager`
  retries it around `initProducerId` and `addPartitionsToTxn` but not around `sendOffsets`.
  Observed twice in ~14 group runs. One line to fix.

- **R2 — protocol version floors.** `src/protocol/requests/index.js` `lookup` picks
  `min(ourMax, brokerMax)` and ignores the broker's `minVersion`. Kafka 4.0 removed pre-2.1 request
  versions (KIP-896); the live cluster reports `ListOffsets(2): 1 to 11`.

## Not quarantined

`--ci` runs also report skips that are **not** quarantine: the version-conditional helpers in
`testHelpers/index.js` (`testIfKafkaAtMost_0_10`, `testIfKafkaAtLeast_0_11`,
`testIfKafkaAtLeast_1_1_0`) and the OAUTHBEARER gates (`describeIfOauthbearerEnabled` /
`describeIfOauthbearerDisabled`), which select tests by `KAFKA_VERSION` and `OAUTHBEARER_ENABLED`.
Those are pre-existing and unrelated to this task.

There is also one pre-existing upstream skip that this task did not add and does not own:
`src/consumer/__tests__/instrumentationEvents.spec.js` — `emits request queue size events`
(commented "This test is too flaky, we need to think about a better way of testing this").

Counting check: `grep -rn '\.skip(' --include='*.spec.js' src/` returns 5 — the 4 rows above plus
that one pre-existing skip. (Match on `.skip(` with the paren: a comment in
`consumeMessages.spec.js` mentions `test.skip` in prose and would otherwise be counted.)
