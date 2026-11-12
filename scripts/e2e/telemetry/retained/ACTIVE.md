# Active compression ownership diagnostic

The `active-owner-census-node{22,24,26}-{scalar,capture}` CI jobs are broker-free.
Each runs all four kinds serially, using `publicMemoryChild.js` and exactly
`MEMORY_FLAGS`, as the public resource arm does. Scalar and capture variants use
separate runners. No acceptance limit, estimator, M placement, warm-up lifecycle,
workload, retirement/reopen sequence or six-pair settle protocol changes. All new
records declare `diagnostic: true`, `acceptanceEligible: false` and
`attributionComplete: false`. These jobs do not discharge the memory verdict.

## Product liveness from the code

`src/telemetry/payload.js` owns one `#job`. `buildPayload` returns null while that
slot is occupied, including after retirement until native completion. It does not
queue samples. `#fit` runs synchronously, before `#compress` waits:

- At most 193 omission groups and at most 257 encoded points, with at most eight
  point attributes and five resource attributes (`src/telemetry/otlp.js`). Binary
  fitting makes at most eight size probes. Group arrays, sorted point/attribute
  arrays, temporary Sets/Maps, JSON grouping keys, `MetricsDataEncoding` and
  `ProtoWriter` instances are construction scratch. They have no job field that
  retains them after fitting returns. A raw counter can still include their
  uncollected storage.
- One exactly sized raw Buffer, at most
  `min(telemetryMaxBytes, brokerMaxBytes, penaltyMaxBytes)` with a local maximum of
  1,048,576 bytes. `job.metrics` initially aliases `job.raw`; it is not a second
  allocation. The codec argument `{buffer: job.raw}` and the native operation
  also reference that input. The 128 maximum descriptors in this workload charge
  354,304 metadata bytes before wire overhead; exact input/backing bytes are in
  each native journal row. Descriptor text is not an exact JS heap size.
- `includedIdentities` and `omittedIdentities` together partition at most 257
  point identities; they refer to existing identity objects. `accepted` copies
  at most 128 signed INT8 values into one JS array. The deduplication Set visits
  at most 128 values and only two built-in codec types are attempted, serially.
  The job also holds scalar counts, the cap, revision and authority predicate.
- The current immutable snapshot has at most 257 point objects, their identities,
  a points array, up to five resource attributes and one scope. The collector's
  `#pending` holds that snapshot and a Map of up to 257 state/point/frozen-window
  entries (`src/telemetry/metrics.js`). `ClientTelemetry.run` holds the same
  snapshot via `op.snapshot` and its authority closure, needed for currentness,
  commit and abandon (`src/telemetry/client.js`). These are aliases to a pending
  sample, not a second raw payload. Across retirement the outstanding payload
  slot still forbids launching a replacement native job.
- GZIP/ZSTD use promisified asynchronous `node:zlib` calls. Their output backing
  and codec state are runtime allocations whose exact bytes are not bounded by
  the payload cap during compression; the result is checked against the cap
  after completion. No output size is inferred from the input cap. The native
  journal records actual result/backing sizes after settlement.

`#clearJob` releases authority, raw/result aliases and identity lists on retirement
and completion. The raw input remains needed during the await: failed codecs can
try the next codec, and unusable compressed output falls back to the raw payload.
After successful compression, completion immediately clears the raw field. Source
inspection proves bounded construction for one payload job; it proves neither an
extra queued payload nor an unnecessary copy during the await. **No product
change is justified or made.** Exact JS object self sizes and retaining roots must
come from the captured graph; source cardinalities cannot establish an 8 MiB
live-heap charge.

## Capture selection and interpretation

The diagnostic records the original settled M, then the **first ordinary native
submission inside each codec's existing measured active-settlement call**. The
selection is fixed by phase and submission order, independent of byte size. It
never arms on disabled control, warm-up, a retirement, or a final close. Every
submission still uses the real production codec. Missing codec coverage fails the
diagnostic without retrying or selecting a smaller reading.

The observer also uses the existing `after-turn-2` seam of `ProcessMemory.settled`
to capture the first original checkpoint per codec that still has a pending job.
It adds no waits or turns. Submission-edge and checkpoint captures have distinct
names. `checkpointCoverage` reports absent overlap explicitly; submission evidence
alone must not be described as reproducing an original over-limit checkpoint.

At each selected observation, `raw` records heapUsed, external, arrayBuffers, RSS
and `retained = heapUsed + external` while NativeJournal identifies the pending
job. The capture variant synchronously requests `{type:'major',execution:'sync'}`,
records `afterMajorGc`, and calls the existing retained `captureSink`. M plus at
most four codec snapshots are written directly to disk and checked against the
existing 512 MiB per-snapshot limit. Each sink uses its existing 64 KiB hash
scratch. The scalar companion takes the same phase/submission observation with
no extra GC or heap snapshots. Neither variant changes the original settle loop.
Every child scalar reading and complete native history remain archived. Capture
variant readings are perturbed by the probe; compare the separate scalar runner
and the unchanged acceptance resource artifacts rather than treating them as
interchangeable.

The submission-edge census is within the active observation window; the separate
`<codec>-checkpoint` census observes the original post-turn checkpoint seam.
Neither claims to reproduce every over-limit scalar checkpoint. The callback stack still
contains the codec wrapper's argument array and encoder wrapper. Those observer
and product stack roots must be interpreted explicitly. A native thread can finish
while synchronous capture blocks JS; `pending` means its promise completion has
not been observed. Offline replay requires that the same journal job brackets the
raw/GC/snapshot pair. It does not claim native CPU work was still running throughout.

`raw - afterMajorGc` describes the GC-sensitive process-byte difference, including
probe effects; it is not a pure dead-scratch allocation count. Snapshot creation
also collects and allocates. Live graph self bytes, engine counters, external
backing and RSS have distinct accounting/time scopes. No graph or harness bytes
are subtracted from acceptance, and no row is dropped when it exceeds a limit.

## Artifacts to read

Each job uploads `active-owner-census-node<N>-<variant>`, including partial failures:

- `active-summary.json`: source hashes, tree/run/Node/V8/flags, child IDs and
  natural-exit completeness. `sources/` archives the executed sources.
- `<kind>/result.json`: original `readings.matched`, `readings.actives` and their
  entire `gc.checkpoints`, native inputs/results/backings and submission/settlement
  times under `readings.native.rows`, plus the child exit/receipt facts.
- `<kind>/census-events.jsonl`: original M and codec `raw`, `afterMajorGc`,
  `rawMinusM`, `afterMajorGcMinusM`, `pending` scalar identities/input/backing
  sizes, `stage`, original GC-pair `round` for checkpoint captures, capture times, snapshot filenames/byte lengths/SHA-256. Scalar
  rows have null afterMajorGc and no snapshots.
- `active-ownership.json`: `analysisComplete`, per-kind `checkpointCoverage`, and `checkpoints` with the
  paired counters, exclusive `totals` and `topOwners`/`newOwners`. Each owner has
  category/type/name, additive `selfBytes`, count and representative
  `examples[].rootPath` with named nodes and edges. `newOwners` ranks objects
  allocated since that child's original M, preserving their graph indexes.
- `<kind>/census-<name>.json` (M, gzip, zstd and observed codec-checkpoints): full retained census nodes, normalized parent
  paths, weak/ephemeron dependencies, unique backing stores, and `originalM`
  additive category deltas with explicit added/released stable IDs. These allow
  unresolved owners to be investigated beyond the bounded twenty-group summary.
  Original `.heapsnapshot` files remain available.

A large raw value that falls after major GC supports a transient population. A
large post-GC value with named product roots supports live product ownership at
this seam; harness and unknown roots remain separately visible. This diagnostic
cannot turn either observation into an acceptance pass or fully attribute every
active peak without reviewing the evidence.
