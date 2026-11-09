---
id: testing
title: Testing
---

We rely on a suite of largely integration tests, using `jest` as a test runner, alongside a smaller set of broker-free unit tests.

## Unit tests

`npm run test:unit` runs the tests that don't need a running Kafka cluster (protocol encoding/decoding, utilities, retry logic, partitioners, assigners, loggers, instrumentation). Use this for fast, iterative feedback while working on broker-free code.

To rerun one soak unit shard, use `npm run test:unit:soak -- --shard=1/3` (or `2/3`, `3/3`). Its Jest sequencer keeps the single spec in each job, and its Node environment assigns cases round-robin before hooks run; ordinary Jest file sharding would leave two jobs empty. The local `test:unit`, `test:coverage:unit` and full `test:coverage` commands still include the whole spec, with their existing coverage settings and thresholds.

Transaction administration has brokerless routing, capability, partial-result, validation and fake-socket coverage in `transactionDiagnostics.spec.js` and `abortFence.spec.js`. The four transaction codec families and seven wire pairs are checked against independent Apache Java vectors before registration. Live open-transaction, administrative abort visibility and producer-fencing evidence is owned by the admin-ledger live arm; the brokerless cases alone do not establish those outcomes.

The protocol inventory tests pin the released share-group message sources, API names, flexible headers and recorded broker ranges. Source inventory alone does not establish runtime share-group support.

The share-group codec tests compare requests and responses with independently generated Apache Kafka 4.4.0 message vectors. They cover compact fields, nested tagged fields, exact integer domains, nullable topics and malformed envelopes. Regenerate the message vectors with `KAFKA_CLIENTS_VERSION=4.4.0 scripts/protocol/javaVectors/runAssignors.sh messages` using a JDK 17 or newer.

Share broker tests use in-process sockets to verify request routing, connection generations, pre-write guards and capability refusals. The ShareGroupHeartbeat v1, ShareFetch v2 and ShareAcknowledge v2 wire pairs are compared in both directions with the independent Java vectors, and the capability check is judged against the committed broker captures of every recorded line. The broker-free `shareConnectionPool` suite is separate from the broker integration suite; admin request frames and public connection refusal tests have separate acceptance cases.

Share membership tests use scripted sockets for heartbeat fields, membership errors, assignments and guarded topic-ID discovery. Fake-clock scheduling tests reuse HeartbeatManager and the shared heartbeat backoff, and track the oldest active handler call across nodes. A GROUP_ID_NOT_FOUND answer ends a current membership but completes a leave, and a late answer from a retired membership is discarded. Ordinary consumer backoff cases stay unchanged; public share shutdown, restart and parallel-handler integration have separate acceptance cases.

Share record-walk tests check exact acquired ranges, control-record gaps and compact release of the undecoded remainder after every decode halt. Ledger and lock tests check compact acknowledgement encoding, local wire validation, single-use outcomes and pre-write lock deadlines. Fixed property seeds cover boundary offsets and overlapping, adjacent and gapped ranges. The internal walk benchmark checks independent identities and work-skipping controls; timing and memory acceptance require a quiet host. Public share-session, renewal and range-event behavior has separate integration acceptance.

Share session and node-loop tests check serialized request epochs, piggybacked acknowledgements and the first eligible forgotten-partition frame. Scripted socket cases check renewal and flush ordering, independent fetch and acknowledgement errors, leader hints and empty-round pacing. Per-node record limits and cleanup have separate assertions. Public shutdown and session fault matrices provide the later integration acceptance.

Share consumer tests check factory validation, first-join readiness, implicit and explicit handler decisions, handler-driven renewal and fresh subscription/assignment snapshots. Scripted socket cases hold ordinary and captured-final work at connect, encode, queue and response boundaries. Actual-clock deadlines, parallel-handler poll expiry, crash/restart supervision and listener failures have separate assertions. Compact range results stay compact through real acknowledgement bytes, events and errors. The guide and tarball guard check group-level isolation and inclusion of the public share guide. Broker regressions and throughput measurements run through the reserved controller guard; later share live scenarios supply the named broker proofs.

Share-consumer API comparisons cover handler delivery, automatic acknowledgement sending, compact homogeneous settlement events and offset-reset method composition. Separate boundary cases cover the absence of a callable synchronous flush, retained subscription after stop and the stopped subscription-replacement sequence. A synthetic DeleteGroups v3 case checks Admin-to-Broker plumbing and success/error mapping for a share group id at the codec level. Documentation checks preserve the isolation recipe, exact offsets and the boundary between share events and OpenTelemetry spans.

The share fault matrices run without a broker in `shareMembershipFaults.spec.js`, `shareSessionFaults.spec.js` and `shareAckFaults.spec.js`. Their literal fault, phase and expected-outcome tables drive the public share consumer through the scripted cluster. Each case checks record identities, current and captured closing authority, replacement progress, compact acknowledgement outcomes and resource cleanup. These seam cases supplement the real-cluster session and shutdown scenarios; an unanswered mutation is never reported as broker-accepted.

Share live scenarios verify exact append identities, broker share-partition positions and handler delivery multiplicities independently. Delivery, acknowledgement types and isolation use the base fixture; short locks and membership use the share fixture; session limits use three distinct one-member groups on one common leader. Shutdown reads back a 15000 ms lock before joining, checks captured final results and a fresh timeout-zero call, then exits naturally with resource counts at baseline. Each scenario records stamped JSONL, verifies it before teardown and deletes its groups, topics and dynamic group configs even after failure. The controller runs these scenarios serially through the reviewed live guard. Legacy describeGroups outcomes on share ids are captured per supported broker line and replayed against a stubbed broker; use describeShareGroups for share membership.

Share-consumer soak checks thirty join/leave cycles, ten session resets, membership churn and independent log, broker-position and handler identities. The short profile has 32-second slots: 960 seconds of profile work, a 1020-second inclusive child budget and a 1440-second cold lifecycle cap. The full profile has 64-second slots: 1920/1980/2400 seconds respectively. Both retain all 30 cycles, 10 resets, a 5000 ms assigned active observation in every cycle and a separate 5000 ms final observation. They require natural exit within 10000 ms of completed cleanup, final resource counts and no retained delivered entries or compact acknowledgement ranges. Fit post-warm-up heap samples separately inside each scored phase. Independently compare the three disconnected phase-end heaps using their original monotonic observation times, with an additional 98304-byte/minute slope limit. Keep the existing quiet-heap endpoint, resource, source-validity and minimum-sample checks. Do not fit active samples from different phases as one working set. These are declared acceptance bounds; live acceptance remains unverified until fresh complete receipts exist.

The `Memory gates` workflow runs the short share soak with `node scripts/ci/naturalExit.js soak` on its own cold Kafka 4.3.1/Node 24 base cluster, preserving natural-exit bounds, stamped evidence verification, teardown and artifact upload. It runs manually or on pull requests from `p9m/` branches; the follow-up memory PR must pass it before the 3.0.0 release. Phase PRs require all functional lanes to pass. CI's manual `lanes` input selects comma-separated integration groups (for example, `share-base,admin`), including every broker/Node variant of each group; empty input, pushes and pull requests retain the full matrix.

Run guarded short acceptance with `npm run test:e2e:share-soak`; its outer TERM deadline is 1440 seconds. Full acceptance also uses the sole reviewed guard and its original reservation/teardown owner:

```sh
timeout --signal=TERM --kill-after=15s 2400s node "${KAFKAJS_LIVE_GUARD:?Set the reviewed live guard path}" "${KAFKA_VERSION:-4.3.1}" tv2 SHARE_SOAK_FULL scripts/soak/soak.js
```

The guard starts the full `share-consumer` CLI child with exposed GC. Both guarded commands retain a 15-second failed-run KILL grace; it never extends acceptance. Controller integration must select the matching 1020/1980-second child watchdog and 1440/2400-second cold cap, admit the full child envelope with outstanding owner-close obligations, and validate the archived primitives before teardown. E13/M-final evidence references and E14's frozen inventory must bind these selector-specific schedules, source identities, configuration readbacks, original heartbeat observations and diagnostic rows to fresh receipts. Historical receipts with matching duration numbers cannot authenticate a new selector or workload. The evidence verifier independently recomputes the quiet slope from original `snapshot.heapAtMs`/`heapUsed` observations, requires ordered baseline, three named phase ends and teardown, and checks the archived `measured.quietSlope` against those sources. Old receipts without monotonic timestamps fail this contract. See [share soak tooling](ShareSoakTooling.md) for the evidence schema and controller hand-off. E14 direct contracts require cap plus 30 seconds (1470/2430 seconds); external guard/validator integration and live feasibility remain pending controller/CI proof.

Benchmark controls count batch callbacks separately from messages and compare complete record identities. Measured share, record-walk and ordinary-regression benchmark sessions run on CI in a later performance phase, with their declared frame, latency, memory and A/A gates.

### Telemetry wire evidence

The telemetry v0 codecs have independent request and response vectors under each
protocol directory's `fixtures/java/`. The development Java generator uses a pinned,
verified Kafka 4.4.0 client jar. Codec tests compare encoded requests to those bytes
and decoded responses to literal fields. Two strict wire pairs read the same request
vectors and build the same response vectors; the telemetry profile adds only those
two versions to the default scripted profile. Finite controls reject null Metrics,
trailing bytes, truncated fields, missing pairs and unadvertised versions. These
checks need no broker. Seeded response-corpus controls are separate from these
deterministic tests and retain their independent source vectors.

### Numeric telemetry evidence

Broker-free collector tests cover metric names, selectors, resource whitelists,
physical-attempt identity, node retirement, frozen gauge windows, exact BigInt sums,
uint64 timestamps and bounded numeric registration on four collector kinds. A fake
socket test follows real Connection preparation, queueing and response completion;
its held encoder proves an old node slot cannot be relabeled. Public client methods,
configured broker export and performance gates have separate integration owners.

### Independent OTLP evidence

The telemetry numeric-subset vectors pin OpenTelemetry proto v1.9.0, protoc 25.5,
protobuf-java 3.25.5 and the independent generated-bindings digest. The separate
GenerateTelemetryVectors.java uses constant inputs and generated Java builders;
it never calls the JavaScript writer. Compare its exact bytes with the retained
fixtures, then decode candidate bodies through the same independent bindings.
See scripts/protocol/javaVectors/README.md for the offline classpath and commands.
No tool or dependency is downloaded by these commands or by a client at runtime.
Java regeneration is a required acceptance check; a hand-computed candidate vector
or a JavaScript round trip does not replace it.

Broker-free tests cover zero oneofs, non-ASCII nested lengths, signed integer and
unsigned timestamp domains, whole-point fitting, paired node gauges, both byte caps,
broker-ordered built-ins and retired native completions. Configured exporter and
Java GZIP/ZSTD decode remain separate integration checks. A missing independent
toolchain is an unrun check, never an equivalent pass.

### Telemetry subscription transport checks

Broker-free capability, subscription and error specs exercise atomic replacement,
UUID retention, pacing, local caps, finite recovery and provisional reconnect. The
broker request spec drives actual Connection queues with independent version-0
wire pairs; the transport spec negotiates the actual private socket. Captured
authority reaches setup, authentication, encoding, queue dispatch, socket write and
response installation. Public lifecycle release schedules and configured receiver
evidence are separate checks. Controller live runs use the existing guarded
foreground runner with its shared reservation and the tv2 profile.

Capability inspection runs at subscription scheduling boundaries. Extra nullable
guard and transmission-callback checks on the ordinary request path must be
measured with the actual Connection prepare/queue/fulfill driver, interleaved
previous and candidate arms, recorded host load, and the fixed A/A controls.

### Public telemetry factories and types

The broker-free telemetry factory, instance-ID and options specs use the unit/coverage telemetry wildcard. They cover independent instances, root/client overrides, default-off traffic, registration storage, getter validation/deadlines/suspension and inherited close budgets. Run one exact physical pattern at a time with --maxWorkers=2 and a private cache. The TypeScript legacy, Node16 and package-export/alias checks preserve constructor and trailing disconnect tuples and signal callbacks. A valid TS2578 unused-expect-error control must fail compilation. All four returned kinds, including actual ShareConsumer declarations, are required before accepting this surface. Live exporter and natural-exit proofs have separate owners; a construction witness is not a live result.

### Broker telemetry fixture and export checks

The configured broker telemetry fixture is one self-contained `docker-compose.telemetry.yml`, selected for startup, readiness, inspection, verification and teardown. Local execution uses the reserved guard and project `kafkajs`; each ephemeral CI runner uses an explicit unique `COMPOSE_PROJECT_NAME`. The four retained reporter and Java client jars are committed as checksum-pinned test fixtures. `scripts/e2e/telemetry/run.sh verify` checks them offline against the manifest before startup. CI never rebuilds them; `build.sh` records historical provenance, and `fetch.sh` separately prepares checksum-pinned decoder dependencies. The npm tarball excludes the fixture and jars. Host endpoints are `localhost:19092`, `localhost:19095` and `localhost:19098`, with IPv4 loopback publication. Tests validate Metadata host and port before dialing and record the actual JS socket address. TLS/SASL host listeners are not published by this fixture.

The reporter queue is bounded to 128 entries, each broker sink to 2 MiB and each JSON detail record to 4096 bytes. Overflow, parse failure, missing accepted exports, a stub or an observer limit breach fails the fixture. A cursor belongs to each broker independently. Full metric validation uses the pinned independent Java decoder on captured Push bytes and matches the decompressed hash, UUID and broker node to the sink and successful response; capped JSON detail alone is insufficient. Exporter push-interval context must equal the configured 1000 ms. Empty selection, a successful zero-byte Push and a broker without an exporter have separate controls.

The reporter case connects actual producer, consumer, admin and ShareConsumer factories, subscribes to `org.apache.kafka.` through the public CLIENT_METRICS APIs and requires two independently decoded accepted normal pushes per instance in 15 seconds after full readiness. It checks distinct stable UUIDs, applicable resources and metric names, with normal connection and RTT identities recorded separately. Auxiliary admins disable telemetry. Records, commits and handlers are not used by this case.

The `telemetry-reporter` CI lane runs two top-level tests, `reporter export` and `reporter no-resource control`, in one exact file with an anchored two-title selector. The `telemetry-base` lane runs the single `reporter no-plugin control` title. CI invokes these Jest group scripts directly after owned startup and full warm-up. The local nine `test:e2e:telemetry-*` scripts still invoke the sole reserved guard directly, profile `tv2`, with one literal file and exact title. The controller supplies `P9_CLUSTER_GUARD`, `KAFKA_VERSION` and one `COMPOSE_FILE`. No telemetry script uses a forced exit. Shutdown and soak select open-handle detection; soak requires its separately prepared, still-reserved fixture.

CI ownership uses `scripts/e2e/telemetry/ci.js`: a unique project, empty container/network/volume preflight, bounded startup/test/verification/cleanup, then `down -v --remove-orphans` and proof that all three resource kinds are gone. Development CI has eleven distinct Node 24 rows: the eight configured groups (`telemetry-reporter`, `telemetry-prefix`, `telemetry-custom`, `telemetry-payload`, `telemetry-java`, `telemetry-outcomes`, `telemetry-shutdown`, `telemetry-soak`) on Kafka 4.3.1; `telemetry-base` on 4.3.1 and 4.4.0; and `telemetry-unsupported` on 4.3.1. The base 4.4.0 smoke is additional coverage. Configured telemetry lanes check mounted reporter jars against the pinned manifest; base lanes verify broker identity without requiring a reporter jar. The required exit catalogue has 75 rows: for each Node 22/24/26, all eight configured groups on 4.3.1/4.2.2, both base groups on 4.3.1/4.2.2/4.1.2/4.0.2, and base 4.1.2 outcomes. Optional 4.4.0 rows are not an exit minimum. A separate `resources` job runs the resource measurements once per Node line and is not one of these rows. Ready pull requests also run the full exit workflow on updates; drafts receive development coverage. Capable ShareConsumers perform ordinary handler and broker-confirmed acknowledgement work under both options; older lines retain exact share capability refusals and N/A acknowledgement dimensions. All physical sockets require ordinary-work ownership, including setup-only connections.

Each client has an ordinary REQUEST listener joined to its independent socket transcript. A bounded public `describeConfigs` call installs admin topology before the RTT control. Foreground bootstrap connections count in the Sum, but requests made without a known-node slot cannot invent gauges. Every applicable descriptor/node identity and metric window is audited. One successful decode consumes a sink row once and later assertions reuse it. Immutable per-case archives retain transmitted/decompressed bytes, full independent decoded JSON, load proofs, identities, request arithmetic and natural observer cleanup. The stamped JSONL references relative paths, sizes and SHA-256 digests; `evidence.js` verifies the manifest and exact scenario catalogue before teardown, and the final cleanup manifest links those stamps to the removal receipt.

Delete the test's CLIENT_METRICS resource and verify absence, disconnect every created client, close observer children and sockets naturally, verify the stamped evidence while brokers remain up, then let the owner remove its project resources. Keep failures and cleanup diagnostics. See `scripts/e2e/telemetry/README.md` for the committed artifact identities, the offline verifier and decoder inputs and the bounded sink contract.

#### Close and reopen (`telemetry-shutdown`)

The case runs the producer, classic consumer, admin and share consumer concurrently, twenty rounds each on one returned object, inside one 118000 ms case: 98000 ms of work and 20000 ms of cleanup inside the 120000 ms test timeout. Each round selects the next observer segment before the public close, calls the same object's public connect in the close's own continuation with no checkpoint, read or await in between, and then requires a Get with the cached UUID and an accepted nonterminating, nonempty Push on the replacement socket, written strictly after that Get's response. The closing segment must hold exactly one accepted final inside its close window and no Get, connect or authentication after the close began. Four zero-timeout arms, one per kind, must write no final and no telemetry request after the close began, and one admin arm blackholes only its captured telemetry socket while foreground metadata work succeeds beside a close with a finite budget below 1000 ms.

Broker logs are set to TRACE once and read once per round for all four kinds in contiguous windows; the old connection's registered-cache removal must appear exactly once and no not-found line may follow. The archive keeps at most two undischarged segments per kind, seals each closed segment after its sockets closed, joins every accepted Push to its successful Get, transmitted bytes, decompressed hash, full independent decode and a sink row that no other Push consumed, and requires the event-loop resource counts of the start after a one second quiet window. A run that cannot finish twenty rounds in the bound fails; no count or time is relaxed. Live execution happens only in CI. The cleanup manifest lists each segment index with its digest, format and segment count.

### Telemetry lifecycle and outcome schedules

The client telemetry checks use public producer, consumer, admin and share-consumer factories with deterministic gates on setup, authentication, encoding, queue dispatch, socket writes and results. They compare request bytes and captured identities together, including repeated subscription IDs and delayed timer delivery. Shutdown checks distinguish ordinary work from the captured final push and test the original close budget.

Observer, serialization, compression and logging failures are checked against the original foreground result or rejection and invocation count. Awaited rebalance hooks keep their documented control-flow behavior. These broker-free schedules complement the configured telemetry, delivery and cleanup tests; they do not prove broker export or native resource bounds.

The mandatory O17–O19 meter/span/context/getter/logger containment matrix runs in `otel/test/failureContainment.test.js` through `npm run test:otel` and the CI `otel` job. The runner installs the packed root candidate and optional dependencies before executing the matrix; a missing or nonidentical candidate fails the lane. The plain unit lane runs the remaining telemetry schedules without requiring an OTel installation.

### Telemetry benchmark evidence

This is a driver-only delivery. A20/A31 measured acceptance remains pending P10 CI measurements, F08 configured-outcome evidence, an accepted previous baseline, final public lifecycle delivery and the reviewed broker-guard adapter. No measured CI job or measured pass is delivered here.

The telemetry benchmark driver checks observation, bounded serialization and public producer-to-consumer delivery. Its broker-free spec validates schedules, actual work, complete identities, legal metric descriptors, payload bytes and evidence schemas. These correctness tests contain no performance timing assertions.

Planned P10 CI benchmark jobs compare the accepted previous snapshot and candidate with identical driver bytes. Sessions retain every declared observation, rotate arms through every position and add an independent A/A reference. One session warmup per arm is discarded separately from each workload's warmup. CPU or throughput noise above 3%, or p99 noise above 5%, makes the session void. Failed and void evidence remains available.

Request denominators count completed correlated pairs. Payloads use one legal point or 128 validated custom series plus 129 built-ins and report actual raw and transmitted bytes. Delivery counts actual message entries or the sum of batch message lengths, separately from callbacks and observed gaps. Durable-log identities, handler identities and raw broker commits are independent checks. Same-path observer, serializer and handler controls must detect overhead. A unit-test pass does not establish a performance result or baseline.

Run `npm run bench:telemetry --` with explicit request, payload or delivery arguments. Delivery jobs require the controller's reserved broker guard and configured fixture. The driver does not start brokers. A worker timeout, partial session, missing codec prerequisite or incorrect artifact cannot produce a passing benchmark result. Retain the manifest, source and driver hashes, all worker artifacts, noise verdict and cleanup evidence with each result.

### Telemetry scenario evidence

Each telemetry scenario runs in its own CI lane, `npm run test:group:telemetry-prefix`,
`-custom`, `-payload`, `-unsupported`, `-java`, `-outcomes` or `-shutdown`, which
selects one exact file and one top-level title. Every lane owns one selected
Compose file and a unique project under the isolated ownership contract; no raw
broker wrapper or second cleanup authority is used. Filtered sibling titles may
be pending, but the selected title must pass exactly once. Java and shutdown
children must exit naturally; watchdog termination is failure.

Capture zero-byte Push on the wire, prefix replacement through its actual
subscription error/Get cycle, later write-fenced snapshots after unregistration,
and both raw and transmitted payload sizes. The 4096-byte fixture also uses an
independent 4097-byte raw control with its own UUID to verify size refusal.
Strict seam recovery receipts remain labelled separately from live responses.

Java producer and consumer each require at least two complete accepted normal exports and exactly one complete UUID-specific terminating export within their close episode. Java admin requires at least two complete accepted normal exports, including a fresh normal export collected after a successful public admin operation at its explicit pre-close checkpoint. The parent must independently validate that export before allowing admin close. Admin then calls public `close(Duration.ofSeconds(3))`, returns, exits naturally within the existing 10000 ms close-to-exit bound with drained pipes, and passes the fixed all-node drain/quiescence observation. Report every observed admin final, permitting zero or one; validate its complete payload and lifecycle if present. The admin exception is qualified by the pinned Java client identity and shutdown sources. Every Java role still exports exactly its connection counter and the three unchanged custom controls in four ResourceMetrics. All KafkaJS final/timeout/reopen requirements remain unchanged.

The pinned 4.3.1 admin reporter closes before network shutdown can guarantee a final. The completed listTopics/closeReady checkpoint must precede every proving point's collection time; exporter receive time is insufficient. One bounded acknowledgement binds checkpoint ID, UUID and the archived reporter row hash. Raw closeReleased/closing/closed markers, monotonic close-call duration, real child-close and drained pipes are replayed. The parent retains the subscription through 2000 ms drain and 2000 ms quiet, with timed reads and hashed prefixes from all three nodes; late collection, activity in quiet or incomplete capture fails. Optional admin finals retain full payload/lifecycle checks, while producer/consumer finals remain mandatory. Java evidence version 2 validates the actual pinned client/source identities before deriving the admin policy; old evidence and a bare optional-final flag fail. The corrected `org.apache.kafka.admin.client.connection.creation.total` plus three controls must still be observed live in exactly four ResourceMetrics. The configured interval, selected 150000 ms Jest budget, supervisors, output and archive caps remain unchanged. Offline client-check and broker-free sender/selection controls cannot replace live receipts. Keep the earlier failed archive intact and run both the selected assertion path and same-candidate archive verifier in the next CI attempt.

Foreground evidence has three independent dimensions: durable record identities,
raw broker commits or share acknowledgements, and actual handler entries. Stable
telemetry-only faults permit no extra ordinary delivery. Socket faults select
the captured telemetry transport, never every connection to a broker endpoint.
Immediate public reconnect runs before any wait for broker cache retirement;
new normal pushes and independent broker disconnect/cache evidence are required.

Any expiry, abort or cleanup failure taints the spec run and prevents later
cases from passing. The lane verifies the stamped evidence while the brokers are
up, then removes only its recorded project resources and proves their absence.
Keep the existing share shutdown case and its own fixture, lock and
acknowledgement contract. The ordinary telemetry quiet-path floor case belongs
in the existing broker floor file after the earlier cases.

Other broker lines, the 4.0.2 floor, Node 22, 24 and 26 and measured benchmarks
are judged by the CI lanes. Twenty-cycle resource, short-soak and the program's
long-run evidence retain their separate owners.

## Integration tests

Most of the test suite talks to a real Kafka cluster. `npm test` runs `npm run lint`, `npm run compat:check` and `npm run protocol:check`, boots the cluster, runs everything, and tears the cluster down again — it's a convenient local shortcut, but it is not what CI runs.

CI (`.github/workflows/ci.yml`) plans its integration matrix and runs seven kinds of test job. `lint` runs `npm run lint`, `npm run compat:check` and `npm run test:types`; `unit` runs `npm run test:unit` excluding `scripts/soak/__tests__/soak.spec.js` on Node 22, 24 and 26; `unit-soak` runs `npm run test:unit:soak` for that spec on the same Node versions in three independent case shards with a ten-minute job timeout; `pack` runs `npm run test:pack`; `otel` runs `npm run test:otel` on Node 24; and `integration` starts a cluster per matrix row and calls `./scripts/testWithKafka.sh "npm run test:group:<name> -- --ci --maxWorkers=4"` directly, for the six groups (`broker`, `admin`, `producer`, `consumer`, `others`, `oauthbearer`) on Node 24 and for `broker` and `others` on Node 22 and 26, all against Kafka 4.3.1. Three share lanes run on Node 24 against Kafka 4.3.1 and 4.4.0: `share-base` on the base cluster (also the share capability, bounce and diagnostics-channel arms), `share` on `docker-compose.share.yml` (also the Java interop arms, which need a JDK; the 4.4.0 rows use the 4.4.0 Java client), and `share-limit` (4.3.1 only) on `docker-compose.share-limit.yml`; they run their specs serially (no `--maxWorkers`), and each uploads its `.tmp/e2e` evidence as a workflow artifact. CI stamps share evidence with the checked-out commit, verifies only this run's JSONL against the observed broker before teardown, and runs shutdown separately without `--forceExit`, enforcing its 120000 ms process and 10000 ms post-cleanup exit bounds. The seventh job, `telemetry`, verifies retained jars offline and calls the shared telemetry workflow with the eleven development rows listed above. The existing exit workflow supplies the 75 required rows under its non-draft, master and manual-dispatch condition. Each selected scenario verifies its complete transcript, raw payloads, independent decoder receipts and oracle sidecars before teardown, then uploads its ownership/removal receipt. Every accepted nonempty Push is decoded; only exact request-level close or fault cancellations excuse unanswered exchanges. Required owners, Java roles, ordinary arms and fault modes are fixed independently of the archive. The close-and-reopen archive is versioned and segmented (`segmented-1`): each public-client lifecycle is one sealed observer segment, and the catalogue is fixed by the schedule before any result is read: 21 lifecycles for each of the four kinds, one segment for each of the four zero-timeout arms and one for the admin blackhole arm, 89 in all. Every segment is packed into exactly two files, its complete transcript and a payload/proof bundle, and the observer, sink and archive bounds (64 sockets, 4096 records and 2 MiB per observer, 2 MiB per sink, 512 files and 16 MiB per archive) are unchanged; overflow fails and never omits evidence. A final archive without this format cannot satisfy the lane. Every L7 JS export must retain its common counter, every custom lifecycle window has exact metric/resource/point counts, and share evidence joins created-topic metadata, raw acquisitions, handlers and settlements by topic and offset. Configured share recovery requires a successful API 71 Get followed strictly by a nonempty, nonterminating accepted API 72 Push with its complete decode/sink join. Share acquisition ranges and lock origins require fetch-partition success; broker settlements require acknowledgement success, including capable base-fixture share work. Archive negative controls retain positive baselines, refreshed hashes and specific intended diagnostics. A listed strict seam is inventory until the same-candidate unit job supplies its execution receipt. `.github/workflows/weekly.yml` runs the six groups on Node 22, 24 and 26 against Kafka 4.1.2, 4.2.2 and 4.3.1 every Monday and on demand; its matrix leaves out Node 22 with 4.2.2 and Node 26 with 4.1.2 and 4.2.2.

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
| `npm run test:e2e:bounded-close` | Captured consumer close, coordinator blackhole, handler and hook detachment, and the actual broker commit versus the local timeout; each case has a 90000 ms budget, with a 3000 ms disconnect deadline plus a 2000 ms teardown allowance, a 5000 ms request timeout and a 10000 ms session timeout for classic only. |
| `npm run test:e2e:auto-commit` | Opportunity-driven cadence: 600 records over 30 seconds with an interval of 5000 ms and at most 8 commit frames in the quiet window, then a graceful broker-confirmed prefix versus a declared connection-loss replay; each case has a 90000 ms budget. |
| `npm run test:e2e:group-faults` | `GROUP_FAULTS_E2E=1`: a duplicate static instance fences exactly one member without rebalancing its peers, a static member returns within its session, and short and long coordinator outages of the group's actual coordinator, one broker at a time with the stop-issued time starting attribution; each arm has a 300000 ms budget, runs alone, and acceptance runs use the reviewed guarded live entry. |
| `npm run test:e2e:fetch-semantics` | `FETCH_SEMANTICS_E2E=1`: read_committed and read_uncommitted groups over open and aborted transactions and control records, a compacted variant, and an oversized first record that is delivered once and committed past; the visibility arms have a 120000 ms budget, the oversized record 90000 ms, and acceptance runs use the reviewed guarded live entry. |
| `npm run test:e2e:fetch-routing` | Healthy partitions progress while a sibling has no leader, and hinted and unhinted leader moves meet the idle and busy bounds; each case has a 120000 ms budget. The eligible follower wire routing is proved by the rack-aware scenario's next-Fetch arm. |
| `npm run test:e2e:fetch-sessions` | `FETCH_SESSIONS_E2E=1`: forced-on Fetch-session correctness on the racks compose, a 2001-partition limit probe before large-assignment conclusions, 20-partition delivery in every assignment mode, pause, seek, final-partition forgetting, leader move, recreation, bounce and socket-fault cases, and a sparse eligible-follower case with either assignment flag; each case has a 120000 ms budget. |
| `npm run test:e2e:fetch-sessions-capacity` | `FETCH_SESSIONS_CAPACITY_E2E=1`: the eight-slot compose variant, more than eight independently assigned consumers, observed fallback, rejection or eviction, and a same-group oracle control; each case has a 180000 ms budget. |
| `npm run soak:consumer-churn` | Four measured consumer phases with independent durability, offset, delivery and resource checks; run through the guarded entry on a reserved cluster for acceptance. |

Routing evidence delimits stable membership and reads actual committed offsets. A complete Fetch response with a cut trailing record batch is covered by the unit Fetch-round suite: complete preceding batches remain deliverable without a partition halt. This differs from a truncated network frame and from a complete corrupt record batch.

Run each physical spec alone through the configured shared live guard, with its existing reservation, Compose project kafkajs and cached images. Guarded execution verifies fresh stamped evidence while the observed-version cluster is still running, then captures logs and cleans only owned resources. Physical log records, broker committed offsets and handler deliveries are separate observations. Local acceptance uses Kafka 4.3.1 and Node 26; CI owns the other declared lanes and floor. Performance controls and fixed throughput, latency and retained-heap bounds run in the release CI measurement phase.

Each command records its broker, Node, wire versions, configuration, reviewed tree and lane in stamped JSONL evidence. Physical log contents, committed offsets and handler deliveries are separate checks. The transaction-restart scenario also verifies that a forced close with an unknown commit never publishes staged offsets locally, while the broker's commit may already have applied. The guarded live entry validates Compose ownership and cached images, prohibits pulls, and retains logs before removal; its admission probe must pass before final disconnect.

Both scenarios run through the single guarded-live owner and reservation, with verification while owned brokers are alive and before teardown. Local runs use Kafka 4.3.1 and Node 26; the other supported Kafka/Node lines run in CI. Session correctness is required even when the default stays false. Timed default selection and hot-path costs run in P10 CI; a default change requires a dated amendment after all fixed controls and thresholds pass.

The authentic librdkafka Snappy capture is in `src/protocol/fixtures/librdkafka/`: one batch of five records that `kcat` 1.7.1 (librdkafka 2.8.0) wrote to a throwaway Apache Kafka 4.3.1 broker, byte for byte as the broker stored it. `snappy-record-batch.json` is its manifest (producer, provenance, digests, framing, the raw records and the expected public records), `snappy-record-data.bin` is the uncompressed record section that snappy-java 1.1.10.7 produced from the batch's body, and `corpus.txt`, `kafka-dump-log.txt` and `kcat-consume.jsonl` are the original corpus and the reference decoders' output. The body is raw Snappy, not xerial framing. `src/protocol/message/compression/librdkafkaSnappy.spec.js` needs no broker: it checks the digests and the raw framing, compares the records a strict local parser reads from the acquired record section with the manifest, decompresses the stored body with the built-in codec and compares it byte for byte with that section, and holds the Fetch decoder and a public consumer over a scripted Fetch to the manifest's expected public records. It never recompresses the body. The live replay in `test:e2e:compression` sends the captured body unchanged and checks what a broker stores and serves; this validator says nothing about that. JavaScript or Java generated data is not a substitute for the capture, and `src/protocol/message/compression/fixtures/librdkafkaCapture.js` refuses to run without it.

`npm run test:e2e:exactly-once-chaos` checks committed transactional-copy output against 20,000 input records over six partitions, including connection replacement and clean and hard rolling broker restarts. Aborted physical records are counted separately. `npm run test:e2e:txn-epoch-overflow` exercises 32,768 committed transactions on a broker with transaction version 2. `npm run test:e2e:txn-id-expiry` uses `docker-compose.txn-expiry.yml`; it checks the configured broker expiry values before committing, idling and resuming the same transactional id. The transactional arm of `npm run test:e2e:idempotent-pipelining` checks a single Produce request in flight.

The awaited-listener scenario is `src/consumer/__tests__/rebalanceListenerLive.spec.js` (`REBALANCE_LISTENER_E2E=1`, `npm run test:e2e:rebalance-listener`, evidence `rebalance-listener.jsonl`). It checks eager, cooperative and consumer-protocol groups with two members, six partitions and 600 records, each arm under a 90000 ms case budget. It proves hook return before first delivery and compares each successful eligible final commit with that episode's saved position using a broker read before release. Run it serially through the reviewed guard (one owner and atomic reservation per assigned slot); the package script is a developer entry point, not acceptance. Additional broker/Node lanes, the floor and benchmark gates run in P10 CI.

The coordinator scenarios keep static replacement separate from short-outage membership windows. The unit Fetch chunking suite splits independent response frames through the real connection and broker path and retains any failing seed. Classic subscription versions zero and two, valid empty assignments and scarce partitions have independent Java bytes and fields. Live evidence keeps broker commits separate from log and handler delivery.

Connection setup is covered by deterministic fake-socket cases for dial, ApiVersions, whole-SASL expiry, cancellation, node-state retention and late work. `blackhole(id)` holds future scripted dials until the client budget expires; `up(id)` restores only future dials. The connection-setup live scenario checks the declared attempt windows, reset after recovery and acknowledged record identity on the configured broker.

`test:e2e:connect-setup` checks growing setup budgets, a reset after ready and the explicit flat legacy timeout. `test:e2e:idle-reap` checks producer close/reopen, raw durable records, committed offsets and stable consumer delivery, plus a held long poll. Run them only through the reviewed shared cluster guard and reservation, with its required environment configured. The idle soak uses a separately counted producer with `connectionsMaxIdle: 5000` and the production-default metadata age. Each cycle has three sends and two 6000 ms idle waits. Both idle readings require zero open sockets, no live socket identities and no queued requests before explicit disconnect. Every reopened socket must have a fresh creation ID, with at most one live socket per current advertised or bootstrap endpoint. A metadata refresh may open an additional healthy endpoint. The stamped cycle includes warmup, warmIdle, before, idle and reopened, with live identities and the current endpoint inventory. Its polling member is counted separately. All arms must exit naturally; a skipped arm, shortened soak or missing stamped evidence is not acceptance. Other broker and Node lines run in CI.

The GSSAPI scenario uses `docker-compose.kerberos.yml` with the cached KDC fixture, realm `KAFKAJS.TEST`, service `kafka/localhost@KAFKAJS.TEST`, a separate one-minute nonrenewed ticket cache and a 15000ms reauthentication setting. Run `test:e2e:gssapi` through the shared live reservation after the fixture and exact optional peer are prepared. It proves service selection, no-layer negotiation, observed ticket expiry, renewed reconnect, same-socket reauthentication and a produce/consume round trip against raw log, commit and handler oracles. With GSSAPI_E2E unset the spec skips; that is never a passing gate. With it set, a missing peer, wrong version, unreadable keytab, uncached image or unbootable KDC fails or blocks the gate. Images are never pulled by the live runner. The Kerberos broker logs `org.apache.kafka.common.security` and the network `Selector` at DEBUG, because the broker reports a failed GSSAPI exchange below INFO. The packed smoke separately proves import and PLAIN work with the addon absent and that GSSAPI then reports the actionable typed error. Native platform coverage is recorded by CI.

`test:e2e:tls-negative` runs `src/network/__tests__/tlsNegativeLive.spec.js` on `docker-compose.tls-required.yml`: the base cluster plus a listener on 9193–9195 that requires a client certificate. The client certificate is issued at run time from the tracked test CA by `scripts/generateKafkaCertificates.sh --client <dir>` into an ignored directory, and removed afterwards; no private key is committed. A client-certificate request completes first. Then the one-way listener refuses an untrusted CA and a wrong server name, the required listener refuses a client without a certificate, a plaintext client on 9093 and 9193 reaches the frame-cap diagnostic (the bytes each listener sent are recorded), and a TLS client on 9092 keeps its connection error and logs one warning. Run it only through the reviewed shared cluster guard. `src/network/__tests__/tlsNegative.spec.js` covers the same outcomes against local servers without a broker.

Admin lifecycle and ledger checks run without a broker. The admin live scenarios run against the cluster the run owns: through the reviewed shared cluster guard when `P9_CLUSTER_GUARD` and `P9_CLUSTER_MANIFEST` are both set, and against the Compose cluster `COMPOSE_FILE` selects when neither is, which is how the exit gate runs them. Setting only one of the two is refused. A skipped scenario or missing fixture is not acceptance. Each run records the actual broker version, negotiated API versions, candidate tree, settings, oracle observations and cleanup result before teardown. Local runs use Kafka 4.3.1 and Node 26; other supported broker and Node lines belong to CI. Raw receipts, such as the broker byte counter of the quota scenario, go to `LIVE_WORK` when it is set and to `.tmp/e2e` otherwise.

| Script | Fixture | Evidence |
|---|---|---|
| test:e2e:admin-ledger | ordinary cluster and separate ACL companion | admin-ledger.jsonl |
| test:e2e:admin-logdirs | two formatted log directories | admin-logdirs.jsonl |
| test:e2e:admin-tokens | per-run delegation-token key and SASL | admin-tokens.jsonl |
| test:e2e:admin-lifecycle | ordinary cluster and held transport | admin-lifecycle.jsonl |
| test:e2e:admin-quota | ordinary SASL cluster and broker byte counter | quota-throttle.jsonl |
| test:e2e:admin-tiered | pinned remote-storage plugin | tiered-offsets.jsonl |

The tiered scenario requires a real upload, local eviction and retained-record read. Plain-topic sentinels cannot replace it. Quota acceptance requires the fixed matched control, quota and recovery windows and an independent broker byte receipt. Run credentials are generated and removed per invocation; diagnostic evidence contains no credential material.

The exit gate (`.github/workflows/gate.yml`, with its checks in `scripts/ci/gate.json`) runs each live arm as its own job on a fresh runner. A job passes only when Jest reports every title listed for it as passed and none as skipped, so a spec that skips itself fails the job. A check may set `kafkaVersion` (`major.minor.patch`) to run against another broker release; the workflow passes it to Compose as `KAFKA_VERSION`, and without it the gate's default release applies.

| Check | Compose file | Flag | What the job prepares |
|---|---|---|---|
| e2e-gssapi | `docker-compose.kerberos.yml` | `GSSAPI_E2E` | the KDC image built from `testHelpers/kerberos/Dockerfile` onto a pinned Ubuntu base, the exact optional `kerberos` peer, and a fixture directory |
| e2e-tls-negative | `docker-compose.tls-required.yml` | `TLS_NEGATIVE_E2E` | the tracked TLS fixtures; the spec issues its client certificate |
| e2e-oauth-helpers | `docker-compose.oauthbearer.yml` | `OAUTH_HELPERS_E2E` | nothing |
| e2e-idle-reap | `docker-compose.yml` | `IDLE_REAP_E2E` | nothing |
| e2e-admin-ledger | `docker-compose.yml` | `ADMIN_LEDGER_E2E` | nothing |
| e2e-admin-lifecycle | `docker-compose.yml` | `ADMIN_LIFECYCLE_E2E` | nothing |
| e2e-admin-quota | `docker-compose.yml` | `ADMIN_QUOTA_E2E` | nothing; the brokers expose JMX on a container-local port through the image's own launcher, so `kafka-*.sh` tools run with `docker exec` are not affected |
| e2e-admin-tokens | `docker-compose.tokens.yml` | `ADMIN_TOKENS_E2E` | a per-run delegation-token key, masked in the job log |
| e2e-admin-logdirs | `docker-compose.logdirs.yml` | `ADMIN_LOGDIRS_E2E` | nothing; the brokers format both directories |
| e2e-admin-tiered | `docker-compose.tiered.yml` | `ADMIN_TIERED_E2E` | the Kafka 4.3.1 storage test jar from Maven Central, checked against its SHA-256, and a remote-tier directory shared by the brokers |

DeleteGroups v3: `KAFKA_VERSION=4.4.0 npm run test:e2e:delete-groups-v3`; repeat on 4.3.1 for the old-version control. The scenario deletes a stopped group that holds a committed offset and checks the result, the group's absence and its missing commit, then refuses a missing group and a group with a member under their own ids; at v3 each result carries `errorMessage`, at v2 it does not. This command uses the sole cluster guard and requires observed-version evidence.

Transactional topic IDs: `KAFKA_VERSION=4.4.0 npm run test:e2e:txn-offset-commit-v6`; repeat with 4.3.1 for v5. The guarded scenario records raw committed next offsets and separate physical and committed-visible record sets: a transaction writes records and commits the group's next offset with them, the raw offset stays at its baseline while the offsets are staged and while the commit is held, an aborted transaction's larger offset never lands, and a topic whose id the cache does not hold keeps the whole request at v5. The version is read from the wire and from the table the broker advertised. This command uses the sole cluster guard and requires observed-version evidence.

Broker identity: `KAFKA_VERSION=4.4.0 npm run test:e2e:api-identity`; repeat with 4.3.1 for the old-version control. The scenario reads the ApiVersions requests off the wire: a bootstrap broker is named no identity and a known node is named its cluster id and node id. The misroute arm sends a node's connection to another broker: a 4.4 broker answers `REBOOTSTRAP_REQUIRED`, the client recovers once and the next public operation succeeds, while a broker below 4.4 completes the negotiation at v4 and does not notice. This command uses the sole cluster guard and requires observed-version evidence.

The development-only Java share member uses the existing Java launcher and its cached Maven
jars. Run `timeout 600 scripts/e2e/javaConsumer/run.sh --role share-member --probe` to compile
and eagerly load its share classes without contacting a broker. The role uses explicit ACCEPT
decisions and reports exact decimal-string offsets with optional delivery counts. Its
`--halt-after N` arm halts after flushing the Nth record, before acknowledging it or committing
that poll, with no close or shutdown hook. See the [Java member instructions](../scripts/e2e/javaConsumer/README.md)
for the argument, output and exit contracts. Broker-free launcher tests use tool doubles;
Java compilation and a successful probe are prerequisites, and do not certify interoperability.

Share-group scenarios run on `docker-compose.share.yml` (a broker minimum share-record lock of 1000 ms, share sessions of 6000 ms and heartbeats of 1000 ms) or on `docker-compose.share-limit.yml` (the same, plus at most 2 share sessions per broker and at most 2 members per share group), selected with `COMPOSE_FILE`; the base cluster refuses a group lock below 15000 ms. Neither variant sets a dead-letter queue (`errors.deadletterqueue.*`), and a tooling test fails if one does. Before its first member joins, a scenario sets its group configs with `setShareGroupConfigs` from `testHelpers/shareFixtures.js`, which resolves only once `describeConfigs` reads every value back as a group config, and names its topics `e2e-p9e-<scenario>-…`. `scripts/protocol/brokerFeatures.json` records the finalized `share.version` of each broker line, captured on 2026-10-18 from default clusters: 1 on 4.2.2 and 4.3.1, 0 on 4.1.2 (supported, not finalized) and no key on 4.0.2 (not supported). A broker line is added by probing that broker, never written by hand. Inside the reviewed guard-owned fixture, the probe flags `--all --record=scripts/protocol/brokerRanges.json --record-features=scripts/protocol/brokerFeatures.json` record both tables again; the `probe:api-versions` package script forwards one flag only, so it cannot record both.

### Scenario lanes

A scenario that holds with no fault injected runs on two lanes from one file: `describe.each(lanes())` from `testHelpers` gives it `fake` (the unit lane, in process) or `docker` (under `npm run test:kafka`, which exports `KAFKAJS_LANE=docker`), and `KAFKAJS_SCENARIO_LANES=fake,docker` asks for both. A scenario that injects a fault (a held answer, a dropped frame) runs on the fake lane unless the fault can be injected on a live cluster too.

`Gate`, `Schedule` and `interleavings` in `testHelpers` hold a call until a test releases it and list the orders in which several held calls can be released (every order up to six calls, a seeded sample above): that is how a spec puts two operations in a chosen order. `createResourceProbe` reports listeners, sockets, queue depth, ledger and obligation sizes, event-loop resources and the settled heap, for specs that compare what a client holds before it is built and after it is torn down.

### Evidence stamps

`writeEvidence` stamps every line with `at`, `brokerVersion` (the version the running broker reports through `docker compose exec`, checked against `KAFKA_VERSION`: a mismatch refuses the line), `wireVersions` (the version each API was sent at, when the scenario states them), `node`, `config` (the client options; the value of any option named like a credential, such as a password, secret, token, key, certificate or AWS access key id, is replaced by `[redacted]`), `tree` (`KAFKAJS_TREE`, `unfrozen` when unset) and `lane`. The stamps always come from the run: a record cannot claim another one, and a `toJSON` on the record is ignored. A unit spec supplies them with `useEvidenceRun({ … })` instead of Docker. `node scripts/evidence/verify.js --broker <version> <files>`, run from a clean checkout while the cluster that produced the files is still up, recomputes the checked-out commit itself and exits 0 only when every line carries every stamp and its `tree`, `node` and `brokerVersion` equal the verified values on the docker lane.

Consumer churn and broker bounce use separate fresh run identities and evidence archives. Run each through the configured guarded entry on the reserved cluster; the owner checks the observed broker version and reviewed tree, verifies evidence while the brokers are still available, captures diagnostics and then cleans up its validated resources. A timeout, missing control or failed cleanup is a failed run. Numerical performance comparisons run at the approved benchmark location. The remaining broker and Node version combinations run in CI.

### Soak and benchmark drivers

`npm run soak:first-run` is the 15-minute screen: 5 minutes each of an idle member, member churn and a broker outage (`--profile idle|member-churn|regex-topic-churn|outage|first-run` picks another, `--phase-seconds <n>` shortens each phase for a smoke run). Under `node --expose-gc` it reads listeners, sockets, request queues, operation ledgers, event-loop resources and the post-collection heap every 30 s into `.tmp/e2e/soak.jsonl`, and exits non-zero when a bound breaks: the readings that must be zero at the end of each phase and after disconnect, and the heap's growth (2 MiB, the first three samples after a 3-minute warm-up against the last three) and slope (256 KiB a minute). It is a short-run screen, not proof of bounded memory.

`npm run soak:producer-churn` is a 15-minute screen with five minutes each of steady production, rolling broker outages and buffer pressure. It uses one six-partition topic, an idempotent producer and a raw reader. Every acknowledged record must be read back once; a record with an unknown send outcome may be absent or present once. It records latency, pressure and recovery controls as well as listeners, sockets, queues, admission and retained heap in `.tmp/e2e/producer-churn.jsonl`. The heap is judged phase by phase, because the buffer-pressure phase holds a full buffer by design: inside each phase, after a 60-second warm-up, the median of its last three post-collection samples may exceed the median of its first three by at most 2 MiB and its slope may be at most 256 KiB a minute, from at least six samples; and what is retained at each later phase end and after the disconnect may exceed the first phase end's reading by at most 2 MiB. A shortened `--phase-seconds` run checks the driver only. The screen exits naturally and does not prove bounded long-run memory. The broker-bounce scenario is also rerun at release gates with its own durability, delivery and ownership checks.

`soak:consumer-churn` runs member churn, regex-topic churn, pause/resume with topic recreation, and a mostly idle 1,000-partition assignment. All four measured phases last five minutes, following one initial three-minute warm-up before the first phase. Guarded acceptance has a 2,200-second overall budget, including cluster startup, child setup, all measured phases, transitions, checks, evidence verification and cleanup. Consumers disable automatic commits and explicitly commit only processed positions for the current ownership and topic incarnation. The profile reads every acknowledged log record back with a separate reader consumer, in a group of its own that commits nothing, before it deletes the topic; it checks actual broker offsets and records handler deliveries with declared replay boundaries. A shortened driver smoke run (`--phase-seconds`) is not acceptance evidence.

The profile records actual handler-entry gap samples, rates, listener/socket/queue/active-resource baselines and matched post-GC heap observations. Its short-run heap screen allows at most 2 MiB median endpoint growth and 256 KiB per minute slope after warm-up. It does not prove bounded memory over a long run. The separate broker-bounce scenario retains its own log, broker-offset, delivery and rebalance evidence.

`npm run bench:frame-decode` times the response-frame loop of a `Connection` over a fake socket (`--shape 64b|64k|1m|coalesced|byte-chunks`); `scripts/benchmarks/consumeThroughput.js` reports the gap between consecutive handler entries and the retained heap (`--max-bytes-per-partition`, `--autocommit-threshold`, `--otel 1`, `--intern-offsets 1`). `scripts/benchmarks/session.js` can run either as an arm: it interleaves the arms, adds an A/A control and voids a session whose control differs from its reference by more than 3 %. Each script has an untimed `--control 1` mode that counts the calls a timed run must have made (`RequestQueue#fulfillRequest`; `OffsetOperations#begin` after the first delivered batch) and prints a pass or fail line; the consume script refuses it on a tree without the ledger.

Consume measurements keep delivered records, delivered batches, handler entries and actual delivery-gap samples separate. One `eachBatch` entry may deliver many records: messages per second uses the sum of actual batch lengths, while batch gaps come only from consecutive batch entries. `--control 1` runs finite counted controls through the same handler wiring before evaluating the existing live offset-ledger control; `--control 0` retains timed warm-up and measurement. Keep the numeric value on either option.

Throughput stops at drain before graceful teardown. Report `deliveredMessages`, `deliveredBatches`, `handlerEntries`, `gapSamples` and `deliveredBytes` with each sample. A batch arm needs at least 1000 actual batch gaps with `--max-bytes-per-partition 32768`; a message arm needs at least 200000 actual message gaps, which requires at least 200001 handler entries. Smaller samples are inconclusive for the tail gate. Compare identical driver bytes and counters on both trees, keep separate identical-configuration A/A controls, and retain the declared throughput, p99 and memory limits. Listener acceptance additionally requires independently counted assigned and revoked hook invocations; teardown is outside the timed interval.

The pause/resume scenario uses maxWaitTimeInMs 5000, requires resumed delivery in less than 1000 ms, and checks that paused partitions remain paused through an ordinary rebalance; each case has a 60000 ms budget. `npm run test:e2e:pause-resume` starts its cluster through the legacy `test:kafka` wrapper and does not call the guarded live entry: controller acceptance runs the scenario through the separately reviewed guarded entry, never through this package row. Local correctness uses Kafka 4.3.1/Node 26; the remaining matrix and fixed performance measurements run in P10 CI. Offline retention controls inspect real registry and ledger instances; temporary consumer disposal alone is not registry-release evidence.

The leader-union scenario runs classic roundRobin and range with unequal subscriptions and Java in both seats, each of those cases with a 120000 ms budget. A further case loses the first follow-up JoinGroup responses of a joining member and checks that it keeps the member id the broker assigned, with a 90000 ms budget. It records union snapshot completion, topic creation during the first round, first completion, subscriber detection (the follower's own stale-assignment rebalance; the leader keeps the default metadata age so that only the subscriber can start the later round) and subsequent assignment. The late-topic row is written even when the case fails, with each member's Metadata calls and answers since the creation, its join, rebalance (with reason) and crash events, and the topic-creation time. Deliveries are compared as (topic, partition, value); the Java-seat rows are assignment-only, because the Java group member prints its assignments and neither prints nor commits records. wireVersions comes from the JavaScript members' REQUEST events; tree comes from KAFKAJS_TREE. The JDK and cached pinned Java dependencies are required. `npm run test:e2e:leader-union` starts its cluster through the legacy `test:kafka` wrapper and does not call the guarded live entry: controller acceptance runs the scenario through the separately reviewed guarded entry, never through this package row.

`npm run test:e2e:consumer-protocol-states` runs the Reconciling and Assigning capture arms, each with a 120000 ms budget, and a separate case for a named topic created after subscription, whose delivery must complete within 30000 ms of the topic's creation inside a whole-case budget of 120000 ms. Gate arrival alone is not a state capture: Describe must remain serviceable and expose the required committed epoch and assignment window, and missing evidence blocks acceptance. The package row starts its cluster through the legacy `test:kafka` wrapper with the state fixture's compose file and does not call the guarded live entry: controller acceptance runs the scenario through the separately reviewed guarded entry, never through this package row, and verifies this run's stamped evidence before teardown. Local correctness uses Kafka 4.3.1 and Node 26; the other supported Kafka and Node lanes run in CI.

`npm run test:e2e:offset-reset` enables OFFSET_RESET_E2E for offsetResetLive.spec.js. The reset cases use explicit record timestamps, three partitions, autoCommit false and a separate explicit-commit broker-truth arm. Each of the four cases has a 90000 ms budget under the shared live-case harness (a 92000 ms Jest timeout); a timeout, abort or failed cleanup taints the run, and every later case in the file refuses to start. The same fault-free none/duration cases also run through the scripted lane. The package script is a developer entry point through `test:kafka`; acceptance runs through the controller's guarded entry, whose shared live reservation owns startup, evidence verification and teardown. Local live acceptance uses Kafka 4.3.1 and Node 26; the other declared Kafka/Node lines and the floor run in CI.

`npm run test:e2e:rack-aware` runs a 150000 ms arm that matches the replication factor 2 Java vector for range assignment with the flag off and on, a separate 150000 ms arm that sends the next Fetch to an in-rack follower with either flag, and a third 150000 ms arm of no-op controls for missing, disjoint and uniform racks. Rack-aware assignment and follower fetching are separate features: assignment alignment alone does not prove the next Fetch went to a follower. The package row starts its cluster through the legacy `test:kafka` wrapper with `docker-compose.racks.yml` and does not call the guarded live entry: controller acceptance runs the scenario through the separately reviewed guarded entry, never through this package row, and verifies this run's stamped evidence before teardown. Local correctness uses Kafka 4.3.1 and Node 26; the other supported Kafka and Node lanes and the floor run in CI.

Producer racks: `KAFKA_VERSION=4.4.0 npm run test:e2e:rack-aware-producer`; repeat on 4.3.1 with the racks fixture (`docker-compose.racks.yml`). The scenario creates a topic of six partitions led two to a rack, reads the racks and leaders from Metadata before and after each case, and fails if they are not the ones asked for. A producer with `rackAware: true` and a `rackId` sends keyless records only to the partitions led in its rack (the default partitioner to one of them, `DefaultPartitioner` alternating over exactly them), falls back to every partition for a rack no leader is in, and spreads over every partition with a `rackId` alone or `rackAware: false`; explicit-partition and keyed records keep their partitions, and a custom partitioner keeps its own choice of a partition outside the rack. After each case the topic is read from the partition leaders, with no consumer group, and holds exactly the acknowledged records at their acknowledged offsets. The whole file has a 180000 ms budget. The consumer's `test:e2e:rack-aware` is a separate script and flag. These commands use the sole cluster guard and require observed-version evidence.

`npm run test:e2e:topic-recreated` enables TOPIC_RECREATED_E2E for topicRecreatedLive.spec.js. Its four cases each have a 120000 ms budget and check that a deleted and recreated topic fires TOPIC_RECREATED and resumes from fromBeginning, ignores a stale committed offset inside the recreated range, resets the position when the partition count is unchanged, and resets again after stop and run when the recreation came before the first confirmed commit. The package row starts its cluster through the legacy `test:kafka` wrapper and does not call the guarded live entry: controller acceptance runs the scenario through the separately reviewed guarded entry, never through this package row, and verifies this run's stamped evidence (`topic-recreated.jsonl`) before teardown. Local correctness uses Kafka 4.3.1 and Node 26; the other supported Kafka and Node lanes run in CI.

`npm run test:e2e:regex-discovery` enables REGEX_DISCOVERY_E2E for regexDiscoveryLive.spec.js. The isolated regex discovery scenario checks a topic created after subscription, removal without automatic recreation, and a fresh incarnation's offsets under classic cooperative and consumer protocols. It uses `metadataMaxAge: 5000` and explicit commits of processed current-incarnation records with automatic commits disabled. Raw-log, delivery and broker-commit evidence is keyed by topic incarnation and verified before a topic is deleted. Each protocol arm runs under a 180000 ms case budget. Its discovery bound is measured with unblocked handlers; metadata age is eligibility at processing opportunities, not a wall-clock guarantee. The package script is a developer entry point through `test:kafka`; acceptance runs through the controller's guarded entry, never through this package row.

`npm run test:e2e:direct-consumer` selects the gated `directConsumerLive.spec.js` scenarios for manual assignment with and without a group. The scenarios check actual per-API traffic, confirmed offsets and handler delivery. Each scenario has a 90000 ms budget. They are skipped without DIRECT_CONSUMER_E2E=1. The package script starts a cluster through the `test:kafka` wrapper for development; acceptance runs use the reviewed guarded live entry. Local runs use Kafka 4.3.1 and Node 26; other supported broker and Node versions and the compatibility floor run in CI.

`scripts/benchmarks/fetchRound.js --partitions 1000 --control` checks required lookup, request and batch work on deterministic stubs without a broker, including each node's share of the partitions. It exposes grouping, lookup, request-building and response intervals; timed allocation samples require --expose-gc --max-semi-space-size=256 and an observed no-GC window.

`scripts/benchmarks/fetchSessions.js --control` is a socket-free readiness check. It parses real Fetch request headers and bodies, verifies a 334/333/333 partition cycle over three brokers, compares request and response bytes against an independent raw transcript, distinguishes delivered records from callbacks and actual batch-entry gaps, and requires the double-GC retained-memory sensitivity control. It uses `--expose-gc`; no live cluster is needed. Timed S1/S2/L1/L2/L3 arms use only the public consumer API and socketFactory with clientId `fs-gate`. A separate process produces the corpus. Raw socket evidence is spooled outside retained V8 arrays and replayed after timing and heap snapshots; teardown is outside the measured interval. Benchmark gates run in P10 CI with the fixed workloads, controls and thresholds; these offline checks do not select a session default.

Fetch-round CPU and allocation measurements run in P10 CI using the same driver on both comparison trees. They retain isolated site intervals, three-node whole-round work counters, interleaved samples, independent identical-configuration noise controls and observed no-GC allocation windows. An optional optimization is retained only with its declared site gain and whole-round bounds. Missing or inconclusive evidence is recorded and does not establish a gain.

Consumer measurements count delivered records, batch callbacks and actual handler-entry gaps separately. Listener controls observe the initial assigned callback and the graceful revoked callback; teardown is outside the throughput interval. Fetch-session measurements correlate request and response bytes on every connection and use consumer-process CPU over the same delivery window. Idle measurements use bytes per second. Identical-tree, identical-configuration repetitions check noise separately from the previous-release comparison. A noisy session, insufficient gap samples, a failed work control or a collection inside an allocation window cannot establish a passing result. Large-assignment memory observations keep the consumer alive through the final collections and verify data on every assigned partition.

`scripts/benchmarks/connectSetup.js` measures 2000 actual setup episodes per arm against an in-process server. Each episode establishes a distinct socket and negotiates ApiVersions; timing ends at ready, and cleanup completes outside the measured interval before the next episode. Run the same bytes on the previous and new trees with an interleaved A/A control. `--control 1` adds one measured millisecond per setup; it must exceed the declared 3% limit. Resource checks require zero remaining timers and sockets. A noisy or void session is not a pass.

`src/network/__tests__/idleReaping.spec.js` checks exact expiry, preparation and pending/in-flight exclusion, stale timer and encoder generations, authentication and socket cleanup on a deterministic seam. `scripts/benchmarks/requestQueueStamp.js` measures 2000000 completed pairs through Connection preparation, queue and fulfillment; the fixed interleaved previous/new/A-A session and ten-clock positive control are described in the benchmark plan. A unit smoke checks the driver, not its timing gate.

### Telemetry resource and churn evidence

Run `node --expose-gc scripts/soak/soak.js --profile telemetry` (`soak:telemetry`) against the configured telemetry fixture; CI runs it as the `telemetry-soak` lane under the isolated ownership contract. The existing producer-churn and consumer profiles keep their own schedules and limits. Shortening telemetry with `--phase-seconds` is refused so a smoke run cannot be mistaken for resource acceptance.

The shutdown file selects `close and reopen` separately from `telemetry churn`, both with open-handle detection and natural exit. Close/reopen checks one established final or no final at timeout zero, blackholes only the telemetry socket and caps telemetry cleanup at the smaller of 1000 ms and the inherited remaining deadline, plus 100 ms scheduling allowance. The next public action is reconnect on the same object; successful normal pushes and the independently asserted old-connection broker-cache event are both required.

Churn runs a 60-second warm-up and 300-second measurement with at least 10 settled samples, then cleanup within 120 seconds and total 540 seconds. It changes subscriptions every 10 seconds, reconnects telemetry every 30 seconds and closes/reopens publicly every 60 seconds. Four one-client workers prevent memory averaging. Measure heapUsed plus external after two collections; arrayBuffers is already included in external. Compare the same descriptor/UUID population, check exact timer/handle/listener/socket identities and record RSS/native peak separately. Independent durable-record, broker-commit or share-delivery, and handler-invocation identities remain separate oracles.

The single-client memory and native arms of `src/telemetry/__tests__/resources.spec.js` need a quiet host: CI runs that file alone in its own `resources` job with `TELEMETRY_RESOURCES_QUIET=1`, and the shared unit lane skips those arms.

Retain all memory inputs, stamped sidecar hashes, worker disconnect receipts, close events and stdout/stderr. Every child must close naturally with code zero and no signal, unexpected stderr or unhandled rejection, within 10 seconds of its explicit-disconnect receipt. A watchdog, missing resource source, missing client kind, changed baseline or failed restoration fails the run.

Local resource gates use Kafka 4.3.1 and Node 26 on a quiet host. Other Kafka and Node lines and the separate performance measurements remain CI hand-offs. A short telemetry pass never closes the final 24-hour idle/churn gate.

### Telemetry resource measurements

The `resources` job in `.github/workflows/telemetry.yml` runs `src/telemetry/__tests__/resources.spec.js`
alone on one idle runner per Node line (`--runInBand --detectOpenHandles`, no forced exit,
`TELEMETRY_RESOURCES_QUIET=1`). It has two steps: the single-client memory arms first, while the host is
idle, then every bounded-resource schedule. The variable only selects the quiet arms; it does not prove a
quiet host, so every arm records its own host context at five points (before and after the child, and
inside it before the control, at the control-to-enabled boundary and at the end) and a noisy host is
inconclusive.

Each memory arm runs in its own `node --expose-gc` child, one per kind, with no Jest, coverage or
inspector inside it, and is recorded as `resources-<arm>-<kind>.json` under
`TELEMETRY_RESOURCES_ARTIFACT_DIR`. The parent issues each child's identity, run and tree and records the
process it spawned; the child declares them with its Node, V8, platform and architecture, and the verdict
binds those declarations to the producing process and to the record's stamp (tree from `KAFKAJS_TREE`,
Node, platform and run). The parent reads the child's output incrementally against a fixed catalogue: at
most one explicit disconnect receipt, from the public arm only, then exactly one readings message. A
malformed, foreign, duplicate, late or unterminated line, output past 64 KiB, or a receipt for another kind,
child or client instance id is a recorded failure. The record carries the exit facts (code, signal, timeout,
stdout and stderr bytes, the output verdict, the receipt and when the parent saw it, time to close after it,
or after the readings where no receipt is owed) and every reading the verdict is computed from. The file is
written before any assertion, and the job uploads the directory even when a step fails.

| Arm | What it measures |
|---|---|
| `single client resource memory for <kind>` | An actual public producer, consumer, admin or share consumer. The child takes its startup memory before importing anything, loads every module either mode needs, then runs a **telemetry-disabled control** of the same kind in the same process (the same scripted wire and the same fixed foreground operations, three per public open), releases it, and only then builds the enabled client: 128 maximum-size custom descriptors registered through the public API, the assigned client instance id, a matched disconnected reading, real GZIP and, where supported, ZSTD pushes, one retired native completion per codec with a public close and an immediate same-object reopen, and a final public close. Each reopen's connect and the final close's disconnect receipt run in the public disconnect's own resolution continuation, and the observed order (the resolution, that call, then the continuation's first yield) is recorded for replay. The registered descriptors are journaled from the public calls that succeeded. At every retained checkpoint the client must still refuse a 129th descriptor, and every journaled descriptor is registered again: the fingerprint covers only those the client updated in place, so a descriptor swapped for another of the same size leaves it one short. The child must close by itself within 10 s of the receipt. |
| `collector only resource memory for <kind>` | Supplementary only. The strict 64-node, 257-point collector and one persistent payload builder with a current and a retired native build for each codec, judged against its own same-kind disabled control in its own child. It reaches sizes a one-node public client cannot and never stands in for the public arm. |

Telemetry memory is judged as an increment over the disabled control, never as an absolute number. The
control is read twice (C0 after its first public open and C1 after its last, both connected and idle) after a
real collection, a 100 ms pause, a second collection and the callbacks of both. Let B be the lower of the two
RSS readings and Eref the lower of the two external readings. The absolute peak is the kernel's nonresetting
high water of the child, `/proc/self/status` `VmHWM`, read before the first import and at every phase
boundary through cleanup; the raw `maxRSS` is recorded beside it and never substituted for it or subtracted.
P is the largest of those high-water readings, of every recorded current reading (each native submission
and settlement included) and of the sampled peak; the external peak is taken the same way.

| Gate | Limit |
|---|---|
| Control stability: C1 against C0 | RSS within 2 MiB, heap plus external within 256 KiB |
| Setup and control absolute peak (through the control's release) | at most 96 MiB |
| Telemetry RSS peak above the control, P minus B | at most 64 MiB |
| Whole-arm absolute peak P | at most 160 MiB |
| External memory peak above the control | at most 64 MiB (`arrayBuffers` is inside external and is never added again) |
| Heap plus external above the matched disconnected baseline | at most 8 MiB while active, at most 256 KiB once closed |

The matched baseline is a separate reading: the enabled client disconnected with its registered descriptors,
its UUID and its counters retained. It is not the control, and it supplies only the last row. Native work is
journaled from the production codec calls (weak-issued job, input and result identities, byte lengths,
distinct backing stores counted once, a reading at each submission and settlement, readings every 20 ms
while work is pending, and per pending window its first and last reading time, reading count and peaks) and
at most one job may be occupied at a time. Each window must span its job from submission to settlement, the
next phase mark must cover each native reading, and the sampled peak must cover each window. An unstable or inflated control, a
setup above 96 MiB, a missing exposed collection function, a missing or incoherent peak source, collections
that were not observed or a noisy host make the result inconclusive and are never rescued by subtraction. A
broken fact (a control that performed telemetry, a wrong phase, a replaced collection function, a decreasing
peak, a missing codec, a failed exit) or an observed violation fails, and a failure outranks an
inconclusive measurement. After the public close no target handle may remain and no unknown handle may
appear, by async resource role and trigger ancestry.

`node scripts/e2e/telemetry/resourceArtifact.js <directory> --tree <hash> --node <version>` re-judges the
directory by recomputing every difference and ceiling from the recorded readings: one stamped artifact per
arm and kind, the current schema version only (an older or aggregate-only record is rejected), a child that
ran on the stamped Node, a child identity on every artifact and none shared by two, the fixed phase and
foreground catalogues, and the gates above. A missing, malformed, foreign or noisy artifact fails or is inconclusive and
the job fails.

The hundred-cycle schedules (`holds the native result through public cycles ...`, `holds the auth supplier
through public cycles ...`) keep one returned client per kind through 100 public close and immediate
connect attempts while one native job or one authentication supplier is held. They run on an opt-in bounded
capture mode of the lifecycle and scripted-wire helpers: per-object patches live in a live-owner table and are
restored when their owner completes, closed fake sockets are dropped with their callbacks while issued
counters keep their numbers, completed requests are kept as counts, and a live bound that is exceeded is a
recorded failure. After every reconnect one ordinary action of the kind completes through the real client (a
producer send, a consumer message, an admin metadata request, a share record with its acknowledgement) with
its original receiver, arguments and outcome; a share record counts only once the broker has answered its
Accept and the client has confirmed it. Each cycle calls connect in the public disconnect's own resolution
continuation and reaches its checkpoint through an observed settlement (the replacement's Get answered, or
for the supplier its setup attempted and refused behind the held call). It journals a fixed-shape scalar row:
the witnessed reconnect order, weak-issued owner identities by role, the dedicated socket's emitter and raw
listener identities, the pending telemetry timers by owning role, and the populations of every capture root.
The held shell must be the same objects in all 100 cycles, its native input being the payload's own buffer;
owners that live across cycles (a sample, an operation, a queued request) must repeat the first cycle's,
which has none, since every checkpoint is reached with no telemetry operation running; owners each lifecycle
replaces must be as many and all issued after the previous cycle; the only pending timer is the scheduler's,
issued in the cycle; and the second connection of every dedicated pool is counted and must never have been
opened. `bounded capture controls` run both families for every kind and late outcome and require a specific
diagnostic for a substituted owner of the same count, a stale or unowned timer, a reconnect deferred by one
await, a share acknowledgement the broker rejects, a completed owner left in the restore table, a held late
outcome released early, a write to a closed socket, a live bound that is exceeded and an issued number that
falls. The native family, whose checkpoints hold one dedicated socket, also requires one for a retired
socket, emitter or listener and for a duplicated listener; the supplier family's checkpoints hold no socket.
Every kind, the admin client included, bounds its close with a public `disconnect({ timeout })`: for each, a
held final that crosses its close deadline while the close timer is withheld writes no byte and records no
final, and the reopened client still completes a Get and a Push. Disabled clients, from the root option and
from a client override, do the same ordinary work and open nothing.

### Telemetry churn (`telemetry churn`)

The `telemetry-soak` lane runs the same `main('telemetry')` owner the CLI runs, in one prepared run:
60 s of warm-up, 300 s of measurement, at most 48 s of setup and at most 120 s of cleanup counted from
the common end-of-measurement boundary, 538 000 ms inclusive inside the 540 000 ms case. A verdict is
stamped only after the last cleanup step.

One parent supervises four one-client worker processes (producer, consumer, admin, share consumer). The
parent owns the fixed tick catalogue: every 10 s a subscription transition, every 30 s a retirement of
the telemetry socket, every 60 s a public close and immediate reopen of the same object. At every tick
the stages are joined across all four workers in this order: close and immediate connect, telemetry
retirement, then the broker set and readback followed by each worker's confirmation. A tick more than
5 s late fails the run and is never caught up. Each confirmation is a successful Get that carries and
returns the client's instance ID with exactly the requested prefix list and a 1000 ms interval; a
lifecycle change also owes a nonempty, nonterminating, accepted Push on the very socket of its
replacement Get, written strictly after that Get's response. An empty selection keeps the resource and
polls; it is never deleted into the 300 000 ms fallback.

Every public lifecycle of a worker is one sealed observer segment (seven per kind: setup, initial and
five reopens), written as a transcript file and a payload/proof bundle. Every accepted Push keeps its
bytes, response, node, subscription and sink join. The Pushes that are decoded in full are selected by
a rule over the transcript alone: the first accepted Push of every telemetry socket, the first accepted
Push governed by each custom-prefix Get, and the accepted final of every segment; the last segment's
final is decoded by the parent after the worker has exited. A close owes exactly one final, written
inside it and answered with error 0, when the selection in force as it begins is nonempty, and none
when it is empty. The parent decodes the selected Pushes with the pinned Java reporter, keeps the real
receipts and reduces them to the hash, broker line and identity a replay joins; the worker's memory
never includes a decoder, and each decoder child's own peak memory is reported separately from its
`/proc` status, or explicitly as unavailable.

A telemetry retirement faults exactly the captured telemetry socket, right after the next telemetry
request is written, so exactly one exchange is cut off and joined to its declaration, while ordinary
work on core sockets still succeeds. Admission is paused, in-flight work finished and commits confirmed
before every public close, and resumed after the reconnect.

The ordinary corpora are three finite sets of 10 000 uniquely keyed 200 byte records, offered only in
the measurement, one batch per fixed slot. The producer's offers and resolutions, the raw log, the
handler entries, the processed ends and the raw broker committed offsets (`resolveOffsets: false`) are
separate ledgers. The share consumer uses its own corpus and the accepted-prefix and rest-release
behaviour: every offset is accepted exactly once at the broker and publicly, a release is the only
reason for a second delivery, no accepted record is delivered again, and at each public close the
handled prefix is accepted and the rest released.

Memory is judged per worker against its own telemetry-disabled control of the same kind in the same
process: the telemetry-attributable resident-size peak is at most 64 MiB above the control, the control
and setup peak at most 96 MiB, the whole worker at most 160 MiB, and the external-memory peak at most
64 MiB above the control. The peak source is the kernel's lifetime high-water mark of the executable
image (`/proc/self/status`), read before any application module is loaded and at every phase boundary;
the raw `maxRSS` is recorded beside it and never subtracted. A control that drifts by more than 2 MiB
of resident size or 256 KiB of retained heap, a setup above its allowance, a missing collection or peak
capability, or a noisy host (one-minute load per CPU above 0.75, free memory below 256 MiB, memory
pressure above 5) makes the measurement inconclusive, with the facts retained; a broken or contradicted
fact fails. The retained-heap gates are unchanged: at most 8 MiB active and 256 KiB closed above the
matched disconnected baseline, a slope of at most 64 KiB per minute, last minus first at most 1 MiB,
and at least ten post-collection readings spanning at least 270 s inside a measurement of at least
300 s, each recorded with its real admission and completion time.

The evidence verifier recomputes everything from the archive instead of trusting a boolean: the fixed
segment catalogue, every proof, boundary, retirement and transition, the declared proof selection, the
decoder receipts, the ordinary and share oracles from their archived identities, the four resource
verdicts, the natural exits and the budgets from their raw stamps. Cancellation allowances come from
the scheduled retirements, each worker's driver source must match the run's own
`scripts/soak/telemetryChurn.js` and `scripts/soak/soak.js`, and each native journal must cover every
compressed Push. It also requires the pinned image, reporter and compose identities of the configured
fixture. Each child must close by itself within 10 s of its one explicit-disconnect receipt with
drained pipes and no unexpected output; a child still running after that bound is killed and joined
within a bound, and its row stays failed.

### Chaos, broker bounces and the 4.0 floor

`testHelpers/chaosSocket.js` wraps a `socketFactory` so a test can disturb a connection at the application level: kill it after a lifetime, blackhole it (both directions swallowed, nothing closed), delay either direction, drop the next response frames or the next requests of one API (read from the request header, across partial reads), or deliver some bytes and then close. Pass `createChaosSocketFactory(inner)` as a client's `socketFactory`, or set `KAFKAJS_TEST_CHAOS=1` to route the helpers' shared factory through it, unarmed; `factory.arm({ port }, apply)` arms the sockets of one endpoint, including those a reconnect opens later. It is not a proxy and proves nothing about kernel-level resets, keepalive or a real network partition. `npm run test:e2e:request-timeout-close` is its live scenario.

`testHelpers/leaderElection.js` pauses, stops, kills and restarts the compose brokers one at a time (`BrokerBouncer`): it refuses a step while any broker is down or does not answer, and every step restores its broker in a `finally` and waits until it answers again and, after a restart, until every partition's replicas are back in sync. `testHelpers/rawLog.js` reads a topic's log straight from the partition leaders (`readLog`), and `committedRecords` gives what a `read_committed` consumer would receive. `npm run test:e2e:broker-bounce` keeps an idempotent producer and two groups running through every kind of outage and judges the log, the deliveries, the rebalances and the committed positions from observed state; it takes 6 to 10 minutes, so run it as the only cluster user. `npm run test:e2e:send-offsets-visibility` drops a transaction coordinator's EndTxn request or its answer to show when transactional offsets become visible to the same client and to a fresh read.

`npm run test:floor` runs the core cases against the oldest supported broker line, `KAFKA_VERSION=4.0.2`: version negotiation against the ranges recorded in `scripts/protocol/brokerRanges.json`, admin, idempotent and transactional produce, the classic eager and cooperative groups and the consumer protocol, with every exclusion stated by capability in the spec's header.

### Broker floor variants

| Script | Compose fixture | Required capability proof |
|---|---|---|
| `test:floor` | base | Core, codecs, consumer and admin paths, connection budgets and exact unsupported-option diagnostics |
| `test:floor:oauth` | OAuth validator | All helper forms, refresh and file rotation |
| `test:floor:gssapi` | KDC | Initial authentication, observed ticket expiry, reconnect and re-authentication |
| `test:floor:tokens` | Tokens enabled | Token lifecycle, plaintext refusal and secret-free diagnostics |
| `test:floor:conditional` | Racks | ApiVersions, TxnOffsetCommit and DeleteGroups keep the versions the floor offers, and the producer's rack preference falls back and keeps a custom partitioner's choice |

Kafka 4.0.2 floor executions belong to CI. Local gates use Kafka 4.3.1 and Node 26; a local broker run supplies no floor result. Run every floor variant cold through the shared guard and reservation, with the actual broker version stamped and evidence verified before owned teardown. A missing or unbootable image blocks acceptance. The final candidate repeats the complete base and variant family; an earlier floor receipt does not certify a later candidate. A skipped suite supplies no acceptance.

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
