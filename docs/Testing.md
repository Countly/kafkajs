---
id: testing
title: Testing
---

We rely on a suite of largely integration tests, using `jest` as a test runner, alongside a smaller set of broker-free unit tests.

## Unit tests

`npm run test:unit` runs the tests that don't need a running Kafka cluster (protocol encoding/decoding, utilities, retry logic, partitioners, assigners, loggers, instrumentation). Use this for fast, iterative feedback while working on broker-free code.

## Integration tests

Most of the test suite talks to a real Kafka cluster. `npm test` runs `npm run lint`, `npm run compat:check` and `npm run protocol:check`, boots the cluster, runs everything, and tears the cluster down again — it's a convenient local shortcut, but it is not what CI runs.

CI (`.github/workflows/ci.yml`) has four kinds of job. `lint` runs `npm run lint`, `npm run compat:check` and `npm run test:types`; `unit` runs `npm run test:unit` on Node 22, 24 and 26; `pack` runs `npm run test:pack`; and `integration` starts a cluster per matrix row and calls `./scripts/testWithKafka.sh "npm run test:group:<name> -- --ci --maxWorkers=4"` directly, for the six groups (`broker`, `admin`, `producer`, `consumer`, `others`, `oauthbearer`) on Node 24 and for `broker` and `others` on Node 22 and 26, all against Kafka 4.3.1. `.github/workflows/weekly.yml` runs the six groups on Node 22, 24 and 26 against Kafka 4.1.2, 4.2.2 and 4.3.1 every Monday and on demand; its matrix leaves out Node 22 with 4.2.2 and Node 26 with 4.1.2 and 4.2.2.

For development, it's usually more convenient to boot the cluster yourself and keep the tests running in watch mode. After starting the cluster as described in [Development Environment](DevelopmentEnvironment.md), run:

```sh
npx jest --watch
```

`jest`'s interactive mode lets you select which tests to run. `NODE_ENV`, `KAFKA_VERSION` (default `4.3.1`) and `KAFKAJS_DEBUG_PROTOCOL_BUFFERS` all have defaults set in `testHelpers/setup.js`, so a bare `npx jest` works with no environment variables.

Alternatively, `npm run test:kafka -- '<command>'` (a thin wrapper over `./scripts/testWithKafka.sh`) starts the cluster if it isn't already running, waits for it, creates SCRAM credentials, runs `<command>`, and tears the cluster down afterwards:

```sh
npm run test:kafka -- "npx jest"
```

Set `DO_NOT_STOP=1` to leave the cluster running after the command finishes.

By default the suite targets `KAFKA_VERSION=4.3.1`; override it to test against a different broker version:

```sh
KAFKA_VERSION=4.1.2 npm run test:kafka -- "npx jest"
```

There are also narrower scripts per area — `npm run test:group:broker`, `npm run test:group:admin`, `npm run test:group:producer`, `npm run test:group:consumer`, `npm run test:group:others`, `npm run test:group:oauthbearer` — each run through `test:kafka` the same way, e.g. `npm run test:kafka -- "npm run test:group:broker"`. The OAUTHBEARER group needs the OAUTHBEARER-enabled compose file:

```sh
COMPOSE_FILE=docker-compose.oauthbearer.yml npm run test:kafka -- "npm run test:group:oauthbearer"
```

### Topics in tests

Create a spec's topics with `createTopic` from `testHelpers`. On KRaft the controller commits a new topic before every broker knows about it, so `createTopic` calls `admin.createTopics({ waitForLeaders: true })`, then waits (up to 30 seconds) until every broker serves metadata for the topic with a leader for each partition. It makes up to ten attempts, and repeats the whole attempt while a broker still answers `UNKNOWN_TOPIC_OR_PARTITION` or `LEADER_NOT_AVAILABLE`.

That repeat hides a race: on a freshly started cluster the broker that `admin.createTopics({ waitForLeaders: true })` asks for the new topic's leaders can answer `UNKNOWN_TOPIC_OR_PARTITION` for a moment, and the client's own wait now keeps waiting through it (within `timeout`). A spec that creates its topics with `createTopic` cannot show whether that wait works. To exercise it on a cold cluster, call `admin.createTopics` directly, as `src/admin/__tests__/createTopicsColdLive.spec.js` does (`npm run test:e2e:create-topics-cold`).

### Live scenarios

A scenario that needs a cluster in a particular state, or a long time, is a separate spec gated by an environment variable: it skips itself without the variable, so the groups above only skip it, and it has an `npm run test:e2e:<name>` script that starts the cluster through `test:kafka` and sets the variable, for example `npm run test:e2e:create-topics-cold`. Most scenarios write what they observed to `.tmp/e2e/<name>.jsonl` (through `writeEvidence` in `testHelpers`), which Git ignores. `npm run probe:api-versions` prints, as one JSON line, the version range the broker at `localhost:9092` offers for each API the admin and group codecs use (`KAFKA_VERSION` selects another supported cluster).

Producer delivery and compression scenarios have dedicated gated commands. These package scripts still use the legacy `test:kafka` wrapper; they do not invoke the guarded live entry. Controller acceptance runs must use the separately reviewed guarded entry:

| Command | Scenario |
|---|---|
| `test:e2e:delivery-timeout` | An applied Produce with a lost answer ends at the delivery deadline. |
| `test:e2e:oversized-record` | Client request limits, ordered splitting, broker topic limits and valid acknowledgement configurations. |
| `test:e2e:ack-lost` | Lost Produce, Fetch and OffsetCommit answers with separate replay oracles. |
| `test:e2e:topic-lifecycle-produce` | Delete, grow and recreate topics while producing. |
| `test:e2e:not-enough-replicas` | One follower down with minimum in-sync replicas of three or two. |
| `test:e2e:compression` | JavaScript and Java interoperability for each installed built-in codec, plus authentic librdkafka Snappy replay. |
| `test:e2e:decompression-bound` | A compressed batch above the configured output bound halts one partition and permits explicit recovery. |

Each command records its broker, Node, wire versions, configuration, reviewed tree and lane in stamped JSONL evidence. Physical log contents, committed offsets and handler deliveries are separate checks. The transaction-restart scenario also verifies that a forced close with an unknown commit never publishes staged offsets locally, while the broker's commit may already have applied. The guarded live entry validates Compose ownership and cached images, prohibits pulls, and retains logs before removal; its admission probe must pass before final disconnect.

The authentic librdkafka Snappy capture is in `src/protocol/fixtures/librdkafka/`: one batch of five records that `kcat` 1.7.1 (librdkafka 2.8.0) wrote to a throwaway Apache Kafka 4.3.1 broker, byte for byte as the broker stored it. `snappy-record-batch.json` is its manifest (producer, provenance, digests, framing, the raw records and the expected public records), `snappy-record-data.bin` is the uncompressed record section that snappy-java 1.1.10.7 produced from the batch's body, and `corpus.txt`, `kafka-dump-log.txt` and `kcat-consume.jsonl` are the original corpus and the reference decoders' output. The body is raw Snappy, not xerial framing. `src/protocol/message/compression/librdkafkaSnappy.spec.js` needs no broker: it checks the digests and the raw framing, compares the records a strict local parser reads from the acquired record section with the manifest, decompresses the stored body with the built-in codec and compares it byte for byte with that section, and holds the Fetch decoder and a public consumer over a scripted Fetch to the manifest's expected public records. It never recompresses the body. The live replay in `test:e2e:compression` sends the captured body unchanged and checks what a broker stores and serves; this validator says nothing about that. JavaScript or Java generated data is not a substitute for the capture, and `src/protocol/message/compression/fixtures/librdkafkaCapture.js` refuses to run without it.

`npm run test:e2e:exactly-once-chaos` checks committed transactional-copy output against 20,000 input records over six partitions, including connection replacement and clean and hard rolling broker restarts. Aborted physical records are counted separately. `npm run test:e2e:txn-epoch-overflow` exercises 32,768 committed transactions on a broker with transaction version 2. `npm run test:e2e:txn-id-expiry` uses `docker-compose.txn-expiry.yml`; it checks the configured broker expiry values before committing, idling and resuming the same transactional id. The transactional arm of `npm run test:e2e:idempotent-pipelining` checks a single Produce request in flight.

The awaited-listener scenario is `src/consumer/__tests__/rebalanceListenerLive.spec.js` (`REBALANCE_LISTENER_E2E=1`, `npm run test:e2e:rebalance-listener`, evidence `rebalance-listener.jsonl`). It checks eager, cooperative and consumer-protocol groups with two members, six partitions and 600 records, each arm under a 90000 ms case budget. It proves hook return before first delivery and compares each successful eligible final commit with that episode's saved position using a broker read before release. Run it serially through the reviewed guard (one owner and atomic reservation per assigned slot); the package script is a developer entry point, not acceptance. Additional broker/Node lanes, the floor and benchmark gates run in P10 CI.

### Scenario lanes

A scenario that holds with no fault injected runs on two lanes from one file: `describe.each(lanes())` from `testHelpers` gives it `fake` (the unit lane, in process) or `docker` (under `npm run test:kafka`, which exports `KAFKAJS_LANE=docker`), and `KAFKAJS_SCENARIO_LANES=fake,docker` asks for both. A scenario that injects a fault (a held answer, a dropped frame) runs on the fake lane unless the fault can be injected on a live cluster too.

`Gate`, `Schedule` and `interleavings` in `testHelpers` hold a call until a test releases it and list the orders in which several held calls can be released (every order up to six calls, a seeded sample above): that is how a spec puts two operations in a chosen order. `createResourceProbe` reports listeners, sockets, queue depth, ledger and obligation sizes, event-loop resources and the settled heap, for specs that compare what a client holds before it is built and after it is torn down.

### Evidence stamps

`writeEvidence` stamps every line with `at`, `brokerVersion` (the version the running broker reports through `docker compose exec`, checked against `KAFKA_VERSION`: a mismatch refuses the line), `wireVersions` (the version each API was sent at, when the scenario states them), `node`, `config` (the client options; the value of any option named like a credential, such as a password, secret, token, key, certificate or AWS access key id, is replaced by `[redacted]`), `tree` (`KAFKAJS_TREE`, `unfrozen` when unset) and `lane`. The stamps always come from the run: a record cannot claim another one, and a `toJSON` on the record is ignored. A unit spec supplies them with `useEvidenceRun({ … })` instead of Docker. `node scripts/evidence/verify.js --broker <version> <files>`, run from a clean checkout while the cluster that produced the files is still up, recomputes the checked-out commit itself and exits 0 only when every line carries every stamp and its `tree`, `node` and `brokerVersion` equal the verified values on the docker lane.

### Soak and benchmark drivers

`npm run soak:first-run` is the 15-minute screen: 5 minutes each of an idle member, member churn and a broker outage (`--profile idle|member-churn|regex-topic-churn|outage|first-run` picks another, `--phase-seconds <n>` shortens each phase for a smoke run). Under `node --expose-gc` it reads listeners, sockets, request queues, operation ledgers, event-loop resources and the post-collection heap every 30 s into `.tmp/e2e/soak.jsonl`, and exits non-zero when a bound breaks: the readings that must be zero at the end of each phase and after disconnect, and the heap's growth (2 MiB, the first three samples after a 3-minute warm-up against the last three) and slope (256 KiB a minute). It is a short-run screen, not proof of bounded memory.

`npm run soak:producer-churn` is a 15-minute screen with five minutes each of steady production, rolling broker outages and buffer pressure. It uses one six-partition topic, an idempotent producer and a raw reader. Every acknowledged record must be read back once; a record with an unknown send outcome may be absent or present once. It records latency, pressure and recovery controls as well as listeners, sockets, queues, admission and retained heap in `.tmp/e2e/producer-churn.jsonl`. The heap is judged phase by phase, because the buffer-pressure phase holds a full buffer by design: inside each phase, after a 60-second warm-up, the median of its last three post-collection samples may exceed the median of its first three by at most 2 MiB and its slope may be at most 256 KiB a minute, from at least six samples; and what is retained at each later phase end and after the disconnect may exceed the first phase end's reading by at most 2 MiB. A shortened `--phase-seconds` run checks the driver only. The screen exits naturally and does not prove bounded long-run memory. The broker-bounce scenario is also rerun at release gates with its own durability, delivery and ownership checks.

`npm run bench:frame-decode` times the response-frame loop of a `Connection` over a fake socket (`--shape 64b|64k|1m|coalesced|byte-chunks`); `scripts/benchmarks/consumeThroughput.js` reports the gap between consecutive handler entries and the retained heap (`--max-bytes-per-partition`, `--autocommit-threshold`, `--otel 1`, `--intern-offsets 1`). `scripts/benchmarks/session.js` can run either as an arm: it interleaves the arms, adds an A/A control and voids a session whose control differs from its reference by more than 3 %. Each script has an untimed `--control 1` mode that counts the calls a timed run must have made (`RequestQueue#fulfillRequest`; `OffsetOperations#begin` after the first delivered batch) and prints a pass or fail line; the consume script refuses it on a tree without the ledger.

Consume measurements keep delivered records, delivered batches, handler entries and actual delivery-gap samples separate. One `eachBatch` entry may deliver many records: messages per second uses the sum of actual batch lengths, while batch gaps come only from consecutive batch entries. `--control 1` runs finite counted controls through the same handler wiring before evaluating the existing live offset-ledger control; `--control 0` retains timed warm-up and measurement. Keep the numeric value on either option.

Throughput stops at drain before graceful teardown. Report `deliveredMessages`, `deliveredBatches`, `handlerEntries`, `gapSamples` and `deliveredBytes` with each sample. A batch arm needs at least 1000 actual batch gaps with `--max-bytes-per-partition 32768`; a message arm needs at least 200000 actual message gaps, which requires at least 200001 handler entries. Smaller samples are inconclusive for the tail gate. Compare identical driver bytes and counters on both trees, keep separate identical-configuration A/A controls, and retain the declared throughput, p99 and memory limits. Listener acceptance additionally requires independently counted assigned and revoked hook invocations; teardown is outside the timed interval.

The pause/resume scenario uses maxWaitTimeInMs 5000, requires resumed delivery in less than 1000 ms, and checks that paused partitions remain paused through an ordinary rebalance; each case has a 60000 ms budget. `npm run test:e2e:pause-resume` starts its cluster through the legacy `test:kafka` wrapper and does not call the guarded live entry: controller acceptance runs the scenario through the separately reviewed guarded entry, never through this package row. Local correctness uses Kafka 4.3.1/Node 26; the remaining matrix and fixed performance measurements run in P10 CI. Offline retention controls inspect real registry and ledger instances; temporary consumer disposal alone is not registry-release evidence.

The leader-union scenario runs classic roundRobin and range with unequal subscriptions and Java in both seats, each of those cases with a 120000 ms budget. A further case loses the first follow-up JoinGroup responses of a joining member and checks that it keeps the member id the broker assigned, with a 90000 ms budget. It records union snapshot completion, topic creation during the first round, first completion, subscriber detection (the follower's own stale-assignment rebalance; the leader keeps the default metadata age so that only the subscriber can start the later round) and subsequent assignment. The late-topic row is written even when the case fails, with each member's Metadata calls and answers since the creation, its join, rebalance (with reason) and crash events, and the topic-creation time. Deliveries are compared as (topic, partition, value); the Java-seat rows are assignment-only, because the Java group member prints its assignments and neither prints nor commits records. wireVersions comes from the JavaScript members' REQUEST events; tree comes from KAFKAJS_TREE. The JDK and cached pinned Java dependencies are required. `npm run test:e2e:leader-union` starts its cluster through the legacy `test:kafka` wrapper and does not call the guarded live entry: controller acceptance runs the scenario through the separately reviewed guarded entry, never through this package row.

`npm run test:e2e:consumer-protocol-states` runs the Reconciling and Assigning capture arms, each with a 120000 ms budget, and a separate case for a named topic created after subscription, whose delivery must complete within 30000 ms of the topic's creation inside a whole-case budget of 120000 ms. Gate arrival alone is not a state capture: Describe must remain serviceable and expose the required committed epoch and assignment window, and missing evidence blocks acceptance. The package row starts its cluster through the legacy `test:kafka` wrapper with the state fixture's compose file and does not call the guarded live entry: controller acceptance runs the scenario through the separately reviewed guarded entry, never through this package row, and verifies this run's stamped evidence before teardown. Local correctness uses Kafka 4.3.1 and Node 26; the other supported Kafka and Node lanes run in CI.

`npm run test:e2e:offset-reset` enables OFFSET_RESET_E2E for offsetResetLive.spec.js. The reset cases use explicit record timestamps, three partitions, autoCommit false and a separate explicit-commit broker-truth arm. Each of the four cases has a 90000 ms budget under the shared live-case harness (a 92000 ms Jest timeout); a timeout, abort or failed cleanup taints the run, and every later case in the file refuses to start. The same fault-free none/duration cases also run through the scripted lane. The package script is a developer entry point through `test:kafka`; acceptance runs through the controller's guarded entry, whose shared live reservation owns startup, evidence verification and teardown. Local live acceptance uses Kafka 4.3.1 and Node 26; the other declared Kafka/Node lines and the floor run in CI.

`npm run test:e2e:rack-aware` runs a 150000 ms arm that matches the replication factor 2 Java vector for range assignment with the flag off and on, a separate 150000 ms arm that sends the next Fetch to an in-rack follower with either flag, and a third 150000 ms arm of no-op controls for missing, disjoint and uniform racks. Rack-aware assignment and follower fetching are separate features: assignment alignment alone does not prove the next Fetch went to a follower. The package row starts its cluster through the legacy `test:kafka` wrapper with `docker-compose.racks.yml` and does not call the guarded live entry: controller acceptance runs the scenario through the separately reviewed guarded entry, never through this package row, and verifies this run's stamped evidence before teardown. Local correctness uses Kafka 4.3.1 and Node 26; the other supported Kafka and Node lanes and the floor run in CI.

`npm run test:e2e:topic-recreated` enables TOPIC_RECREATED_E2E for topicRecreatedLive.spec.js. Its four cases each have a 120000 ms budget and check that a deleted and recreated topic fires TOPIC_RECREATED and resumes from fromBeginning, ignores a stale committed offset inside the recreated range, resets the position when the partition count is unchanged, and resets again after stop and run when the recreation came before the first confirmed commit. The package row starts its cluster through the legacy `test:kafka` wrapper and does not call the guarded live entry: controller acceptance runs the scenario through the separately reviewed guarded entry, never through this package row, and verifies this run's stamped evidence (`topic-recreated.jsonl`) before teardown. Local correctness uses Kafka 4.3.1 and Node 26; the other supported Kafka and Node lanes run in CI.

`npm run test:e2e:regex-discovery` enables REGEX_DISCOVERY_E2E for regexDiscoveryLive.spec.js. The isolated regex discovery scenario checks a topic created after subscription, removal without automatic recreation, and a fresh incarnation's offsets under classic cooperative and consumer protocols. It uses `metadataMaxAge: 5000` and explicit commits of processed current-incarnation records with automatic commits disabled. Raw-log, delivery and broker-commit evidence is keyed by topic incarnation and verified before a topic is deleted. Each protocol arm runs under a 180000 ms case budget. Its discovery bound is measured with unblocked handlers; metadata age is eligibility at processing opportunities, not a wall-clock guarantee. The package script is a developer entry point through `test:kafka`; acceptance runs through the controller's guarded entry, never through this package row.

`npm run test:e2e:direct-consumer` selects the gated `directConsumerLive.spec.js` scenarios for manual assignment with and without a group. The scenarios check actual per-API traffic, confirmed offsets and handler delivery. Each scenario has a 90000 ms budget. They are skipped without DIRECT_CONSUMER_E2E=1. The package script starts a cluster through the `test:kafka` wrapper for development; acceptance runs use the reviewed guarded live entry. Local runs use Kafka 4.3.1 and Node 26; other supported broker and Node versions and the compatibility floor run in CI.

`scripts/benchmarks/fetchRound.js --partitions 1000 --control` checks required lookup, request and batch work on deterministic stubs without a broker, including each node's share of the partitions. It exposes grouping, lookup, request-building and response intervals; timed allocation samples require --expose-gc --max-semi-space-size=256 and an observed no-GC window. `scripts/benchmarks/fetchSessions.js --control` checks independent Fetch frame accounting and the three-broker byte denominator. At this milestone its padded frames are accounting fixtures, not session protocol correctness or performance evidence. The final session driver adds public-consumer arms and the retained-memory sensitivity control. Benchmarks run on P10 CI runners using the pre-declared workloads, A/A controls and fixed thresholds; no local timing gate is inferred from these readiness controls.

### Chaos, broker bounces and the 4.0 floor

`testHelpers/chaosSocket.js` wraps a `socketFactory` so a test can disturb a connection at the application level: kill it after a lifetime, blackhole it (both directions swallowed, nothing closed), delay either direction, drop the next response frames or the next requests of one API (read from the request header, across partial reads), or deliver some bytes and then close. Pass `createChaosSocketFactory(inner)` as a client's `socketFactory`, or set `KAFKAJS_TEST_CHAOS=1` to route the helpers' shared factory through it, unarmed; `factory.arm({ port }, apply)` arms the sockets of one endpoint, including those a reconnect opens later. It is not a proxy and proves nothing about kernel-level resets, keepalive or a real network partition. `npm run test:e2e:request-timeout-close` is its live scenario.

`testHelpers/leaderElection.js` pauses, stops, kills and restarts the compose brokers one at a time (`BrokerBouncer`): it refuses a step while any broker is down or does not answer, and every step restores its broker in a `finally` and waits until it answers again and, after a restart, until every partition's replicas are back in sync. `testHelpers/rawLog.js` reads a topic's log straight from the partition leaders (`readLog`), and `committedRecords` gives what a `read_committed` consumer would receive. `npm run test:e2e:broker-bounce` keeps an idempotent producer and two groups running through every kind of outage and judges the log, the deliveries, the rebalances and the committed positions from observed state; it takes 6 to 10 minutes, so run it as the only cluster user. `npm run test:e2e:send-offsets-visibility` drops a transaction coordinator's EndTxn request or its answer to show when transactional offsets become visible to the same client and to a fresh read.

`npm run test:floor` runs the core cases against the oldest supported broker line, `KAFKA_VERSION=4.0.2`: version negotiation against the ranges recorded in `scripts/protocol/brokerRanges.json`, admin, idempotent and transactional produce, the classic eager and cooperative groups and the consumer protocol, with every exclusion stated by capability in the spec's header.

### Skipped tests

Some tests are intentionally skipped against certain broker versions or environments. The reasons are tracked in `QUARANTINE.md` at the repository root — check there before assuming a skip is accidental.

## Linting and types

- `npm run lint` runs ESLint over the whole tree; files listed in `.gitignore` are skipped (Prettier additionally honours `.prettierignore`), so there is no `git ls-files` shell-out.
- `npm run test:types` type-checks the TypeScript definitions in `types/`.

## Packaging smoke test

`npm run test:pack` packs the library with `npm pack`, fails if the tarball holds a spec file, a fixture, a type-test file or anything from `otel/`, installs the tarball into a throwaway directory, and runs an ESM/CJS interop smoke test plus a Node 16-module-resolution types check against it. This is the closest local equivalent to what a real consumer installing the package would get.

## OpenTelemetry package

`otel/` is a separate package, the OpenTelemetry instrumentation for this library, with its own `package.json`, lockfile and dependencies. `npm run test:otel` packs the library, installs the tarball into `otel/` the way a consumer would, and runs that package's own tests. It needs the npm registry and is not part of `npm test`.

## Logging

Logs are usually disabled during test runs. To enable logging, override the log level with the environment variable `KAFKAJS_LOG_LEVEL`.

In order to avoid overloading the terminal with huge logs, we filter any buffers that would otherwise be printed from the protocol debug logs. If you need to see the actual buffer values in your logs, override this with the environment variable `KAFKAJS_DEBUG_PROTOCOL_BUFFERS`:

```sh
KAFKAJS_LOG_LEVEL=debug npx jest --watch
```

Because the `Fetch` response log can be extremely large, we always filter the `Fetch` response payload, unless the `KAFKAJS_DEBUG_EXTENDED_PROTOCOL_BUFFERS` environment variable is *also* set. To see the full `Fetch` response buffer values, run with both flags set:

```sh
KAFKAJS_LOG_LEVEL=debug KAFKAJS_DEBUG_EXTENDED_PROTOCOL_BUFFERS=1 npx jest --watch
```

## Helpers

[`testHelpers/index.js`](../testHelpers/index.js) provides several helpers for connecting to your cluster and asserting upon its state. Tests are typically co-located with the code that they are testing. See [`src/consumer/__tests__/consumeMessages.spec.js`](../src/consumer/__tests__/consumeMessages.spec.js) for an example that makes use of the helpers to implement an integration test.

The scripted-cluster fixtures check consumer errors and Fetch-session fields against independent wire templates. Their request readers consume every forgotten-topic entry and reject trailing bytes. A successful scripted scenario checks assertClean() before teardown completes; deliberately malformed requests assert the precise refusal instead.

Consumer evidence includes independent Java subscription v0/v2, valid empty, scarce-partition, rack-aware range and Fetch session/forgotten-topic vectors. `scripts/protocol/javaVectors/runAssignors.sh assignors <output-dir>` and `scripts/protocol/javaVectors/runAssignors.sh messages <output-dir>` regenerate them with the pinned Kafka client jar. Preserve the recorded inputs, byte vectors and provenance together; compare a second unchanged generation byte for byte. These tools require a working JDK and classpath. A failed Java probe is a failed prerequisite.

The Java group helper accepts one topic or a comma-separated list in its existing topic argument. `scripts/e2e/javaConsumer/run.sh --probe` checks its prerequisites, and `--probe-topics t-a,t-b` checks the nonjoining topic parser. Actual mixed-client assignment and delivery still require the live interop scenarios.

`docker-compose.racks.yml` supplies distinct broker racks and the rack-aware replica selector. `docker-compose.fetch-sessions.yml` separately sets the incremental Fetch cache to eight slots. The source-pinned image fixture under `testHelpers/consumerGroupStates/` can defer target assignment without waiting on the coordinator executor. Its arrival event alone is not a state capture: a test must independently read the committed group and assignment epochs through a successful Describe, observe the required state, then release the matching nonce and prove convergence. Both Assigning and a separately held live revocation window for Reconciling must be captured on every supported lane; a missing window fails that gate.

`scripts/benchmarks/subscriptionResolve.js --control` checks five patterns against 10000 metadata topics, including 1000 internal topics, and requires exactly the 9000 external matches and their configurations. Timed evaluation and the consumer regression measurements run in CI under the declared performance procedure; a work-control pass does not establish a timing result.

## Countly canary (manual until 3.0.0)

The canary runs the unit tests of the Countly platform's Kafka plugin against the packed library. It is manual and not part of CI. Never run it in the Countly checkout you work in: it swaps a dependency and rewrites several of its files (the patch files and, in step 7, `observability/otel.js`). Use a separate checkout of Countly (a worktree or a copy) derived from its `main` branch, called `$COUNTLY_COPY` below. A worktree has no `node_modules`: run `npm ci` at its root first (a plain copy of a working checkout already has them).

1. In this repository, run `PACK_DIR=$(mktemp -d) npm run test:pack`. It packs the library, checks the tarball and prints its path (`tarball: <directory>/countly-kafkajs-<version>.tgz`); the file stays in that directory.
2. In `$COUNTLY_COPY/plugins/kafka`, install the tarball under the name `kafkajs`: `npm install "kafkajs@file:<absolute path of the tarball>"`. Countly requires `kafkajs`, and a plain `npm install <tarball>` adds a second copy named `@countly/kafkajs` and leaves 2.2.4 in place, so the run would test 2.2.4. Do not commit the change to the manifest or the lockfile.
3. Prove which package loads. From `$COUNTLY_COPY/plugins/kafka`, run `node -p "require('kafkajs/package.json').name + ' ' + require('kafkajs/package.json').version"`. It must print `@countly/kafkajs` and the tarball's version, for example `@countly/kafkajs 3.0.0-alpha.8`. If it prints `kafkajs 2.2.4`, stop: Countly lists `kafkajs` at its root and in `plugins/kafka`, and an install in the wrong place silently tests 2.2.4.
4. In `$COUNTLY_COPY/plugins/kafka`, stand in for Countly's deep-path patches (the paths in this step are relative to that directory). `api/lib/kafkaRequestQueueFix.js` and `api/lib/kafkaSyncGroupTimeoutFix.js` require files under `kafkajs/src/`, which the `exports` map of this package does not expose, so against it they throw `ERR_PACKAGE_PATH_NOT_EXPORTED` and cannot load; they patch behaviour this library implements itself. Countly's `api/lib/kafkaClient.js` still calls `kafkaRequestQueueFix.isPatched()` and throws when it is false, so a stand-in keeps the export names. Replace `api/lib/kafkaRequestQueueFix.js` with `module.exports = { isPatched: () => true, restore: () => {} }`. Replace any other patch file your copy still has, such as `api/lib/kafkaSyncGroupTimeoutFix.js`, with a module that exports the same names as the real file (read them from it first), each a function that does nothing, or answers true for a check named `isPatched`.
5. From the root of `$COUNTLY_COPY`, run the plugin's unit tests without `plugins/kafka/tests/unit/kafkaClientTimeouts.unit.js`, which belongs to the patches (these are the options of Countly's own `test:unit` script, narrowed to the plugin):

   ```sh
   NODE_PATH=./node_modules/ npx mocha --preserve-symlinks --preserve-symlinks-main \
     $(ls plugins/kafka/tests/unit/*.unit.js | grep -v kafkaClientTimeouts) \
     --reporter spec --timeout 50000 --exit
   ```

6. Run the same fixture twice from identical copies derived from the same Countly `main` snapshot: first with the previous sub-phase tag's tarball and then with the candidate tarball. Repeat steps 2–5 for each arm, keeping the same stand-ins, excluded timeout test, environment and dependencies. In step 5 replace `--reporter spec` with `--reporter json --reporter-option output=previous.json` (or `output=current.json`), so nothing a test prints to stdout can corrupt the report; keep `NODE_PATH=./node_modules/`, `--preserve-symlinks`, `--preserve-symlinks-main`, `--timeout 50000` and `--exit`. Keep both Mocha exit statuses and reports even if known fixture failures make Mocha exit nonzero. From this repository run `node scripts/canary/compare.js previous.json current.json`: exit 0 means identical full test titles and statuses, exit 1 prints the removed and added identity/status pairs, and exit 2 means invalid input. Matching aggregate counts are insufficient. Retain the package-version proofs and identity diff in the release test log. Delete the disposable copies after the instrumentation proof when required.
7. To canary the instrumentation as well, pack it with `npm pack --pack-destination <directory>` in `otel/`, install that tarball at the root of `$COUNTLY_COPY`, and in `observability/otel.js` require `@countly/opentelemetry-instrumentation-kafkajs` instead of `@opentelemetry/instrumentation-kafkajs`. The step 5 tests never load that file, so load it yourself from the root of `$COUNTLY_COPY`. Countly's `otel.js` imports its instrumentations at module top level, so a missing module (`Cannot find module`) makes this command exit nonzero before initialization; it is not a caught initialization failure line. With telemetry on, a caught initialization failure can leave the exit status at 0, so require exit 0 as well as the success line: `OTEL_ENABLED=true node -e "require('./observability/otel.js')"` must print exactly one `[OTEL] ✅ OpenTelemetry initialized successfully!` line and neither a `[OTEL] ❌ Failed to initialize OpenTelemetry:` line nor an `[OTEL] retryWithBackoff exhausted:` line. Without `OTEL_ENABLED=true` it skips initialization but still imports the instrumentation modules. The instrumentation hooks the library under either name, `@countly/kafkajs` or `kafkajs` as step 2 installs it, so Countly's spans would come from it; this step shows only that the package loads, since no test of step 5 produces a span.

When instrumentation or package hooks change, step 7 is required in addition to both unit-canary arms. Exit 0 from the load command alone is insufficient: assert the initialized line and absence of a failure line, then run the same isolated application's configured Kafka send path with topic `canary-spans` and wait for its exporter to emit a `send canary-spans` span. Assert that the destination is `canary-spans`, that exactly the intended message was acknowledged and that the observed span belongs to this send (use a unique message key or trace id). A load-only check never proves span creation. The controller records the exact environment-specific send/export command and assertions in the release test log; this repository has no command that sends through Countly's bootstrapped application. The broker-dependent send proof runs only with the assigned cluster slot.
