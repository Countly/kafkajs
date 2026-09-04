# Changelog

All notable changes to `@countly/kafkajs` are recorded here. The fork starts from kafkajs 2.2.4 (Feb 2023).
Versions before 3.0.0 are internal alpha tags and are not published.

## Unreleased — 3.0.0

### Phase 1 — foundation
- Package renamed to `@countly/kafkajs`; strict `exports` map (`src/**` no longer importable) and ESM entry `index.mjs`.
- Node.js 22+ required; Jest 30; TypeScript 5 for the type tests.
- Test cluster moved to `apache/kafka` 4.x (KRaft); Kafka < 4.0 is no longer supported.

### Phase 2 — bug fixes and dead-path cleanup

#### Behavior changes
- Request versions below the Kafka 4.0 floors (KIP-896, KIP-724) are no longer implemented: Produce v0–2, Fetch v0–3, ListOffsets v0, OffsetCommit v0–1, CreateTopics v0–1, DeleteTopics v0, DescribeAcls/CreateAcls/DeleteAcls v0, DescribeConfigs v0. A 4.x broker never negotiates them. The MessageSet (magic 0/1) *read* path is kept because old segments can still be served inside Fetch ≥ v4.
- The pre-KIP-152 raw SASL token path (used only when a broker lacked `SaslAuthenticate`) is removed; every mechanism goes through `SaslAuthenticate`.
- Version negotiation now rejects an API whose broker range does not overlap the client range with `KafkaJSServerDoesNotSupportApiKey` (message names both ranges) instead of sending a version the broker rejects or selecting one the client does not implement.
- `retry.restartOnFailure`, when supplied, is now consulted for every terminal consumer crash, retriable or not — previously it was only invoked for errors KafkaJS considered retriable, so a callback returning `true`/`false` was silently ignored on non-retriable errors. Callers who never set `restartOnFailure` see no change: the default remains "restart on retriable errors, stop on non-retriable ones". If the consumer is stopped or disconnected while the callback is still deciding, no restart is scheduled.

#### Fixes
- Request queue: an idle, unthrottled queue no longer re-arms a timer after every response (hundreds of wakeups per second at idle, `TimeoutNegativeWarning` on Node ≥ 24). Informed by upstream #1755, #1768, #1667, #1572 (issues #1704, #1751, #1556); implementation follows the Countly production patch and keeps the 10 ms re-check for a queue blocked by the in-flight limit. Retires Countly patch 1a.
- SyncGroup gets its own request timeout derived from `rebalanceTimeout` (or `sessionTimeout`) plus 5 s, as JoinGroup already did, so a rebalance whose assignment takes longer than the client-wide `requestTimeout` no longer dies with `KafkaJSRequestTimeoutError` and loops. No upstream issue exists for this; retires Countly patch 1b.
- The consumer's cached group coordinator is now re-discovered (via `FindCoordinator`) instead of being reconnected under its old, possibly stale, node id, so a coordinator failover no longer leaves `commitOffsets` looping forever on `NOT_COORDINATOR_FOR_GROUP`/`GROUP_COORDINATOR_NOT_AVAILABLE`. The consumer also now treats `GROUP_LOAD_IN_PROGRESS` (code 14) as a rebalance condition, so a JoinGroup against a still-loading coordinator retries the join instead of failing the consumer. Adopts upstream PR #1779 (issues #1536, #1630, #1654); retires Countly patch 2b. Alongside this, the dead string-based matches on the legacy error names for codes 14/15/16 in the idempotent producer's `sendOffsets` and in `admin#deleteGroups` are replaced with code-based matching, making their retry/coordinator-refresh branches reachable for the first time.
- CRASH now fires exactly once for every terminal death of the consumer runner, even when the cleanup `disconnect()` call inside the crash handler itself throws (e.g. the broker connection is already gone) — previously that rejection was unhandled and swallowed the CRASH event entirely, leaving the consumer silently dead with `consumer.run()` already resolved and nothing to observe it. The runner's crash-handling `.catch`/`start()` paths now `await` the crash handler and log (rather than drop) any error it still raises. Informed by upstream issue family #659, #1513; retires Countly patch 2a.

#### Tooling
- ESLint 9 flat config and Prettier 3 replace ESLint 6 / Prettier 1 (`eslint-config-standard` rules ported explicitly).
- Coverage is measured and gated: `test:coverage` enforces a floor of statements 94 / branches 84 / functions 96 / lines 94 (baseline 95.6 / 85.7 / 97.7 / 95.6 on the full suite against Kafka 4.3.1).
- The generated compatibility inventory (`docs/KafkaCompatibility.md`) has no below-floor waivers left.
- Toolchain switched from Yarn 1 to npm only (`package-lock.json`, `engines.npm >=10`); scripts collapsed (the six `test:group:*:ci` wrappers, `jest`, `test:local`, `test:local:watch` are gone in favor of `test:kafka -- "<command>"` plus environment defaults moved into `testHelpers/setup.js`); `lint`/`format` now cover the whole tree via `.gitignore`/`.prettierignore` instead of shelling out to `git ls-files`. Removed devDependencies `jest-junit`, `execa` and `glob` (replaced by `node:child_process` and `fs.globSync`); added `@eslint/compat`.

Adopted upstream PRs are credited here by number as they land (Phase 2 onward).
