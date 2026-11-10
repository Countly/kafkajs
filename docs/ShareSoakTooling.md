# Share soak tooling and CI acceptance

Fit post-warm-up heap samples separately inside each scored phase. Independently compare the three disconnected phase-end heaps using their original monotonic observation times, with an additional 98304-byte/minute slope limit. Keep the existing quiet-heap endpoint, resource, source-validity and minimum-sample checks. Do not fit active samples from different phases as one working set.

## Evidence and validation

Every share resource snapshot includes `heapAtMs`, assigned immediately after its original resource probe resolves, as `owner.clock.now() - owner.startedAt`. The origin is the monotonic owner clock at run entry. Wall-clock `at`, periodic `atMs`/`phaseMs`, and nominal schedule lengths cannot replace it. The periodic in-memory projection still contains exactly `heapUsed`, `shareRecords`, `shareRanges`, and `shareSessions`; the evidence sink receives complete original snapshots, and quiet snapshots stay complete.

Require one baseline, exactly three disconnected phase ends in order `share-steady`, `share-reset`, `share-members`, then one teardown. Each observation needs a nonnegative safe-integer `heapUsed` and a finite nonnegative `heapAtMs`; all five times must increase strictly. Missing, invalid, duplicate, reversed or inconsistent sources fail closed.

The resource verdict adds:

```js
measured.quietSlope = {
  points: [
    { phase: 'share-steady', heapAtMs, heapUsed },
    { phase: 'share-reset', heapAtMs, heapUsed },
    { phase: 'share-members', heapAtMs, heapUsed },
  ],
  bytesPerMinute,
}
```

`points` archives the original phase-end values even on failure. Missing fields are represented as `null`, never zero. Invalid chronology or heap sources produce `bytesPerMinute: null`. Otherwise fit the existing least-squares function to `[heapAtMs / 60000, heapUsed]` for these three points only. A nonfinite fit or an unrounded value greater than 98304 fails with `phase-end quiet heap slope screen`. Exactly 98304 passes. Baseline and teardown establish chronology and retain their independent checks; neither participates in this fit. The 2-MiB endpoint reference remains the first phase end, including the teardown comparison.

`scripts/evidence/verify.js` applies `verifyShareQuietHeap` from `scripts/evidence/shareQuietHeap.js` to original share evidence grouped by scenario, after checking the normal evidence stamps. The validator independently derives the three points, validates ordering and source counts, and recomputes the slope. It rejects missing original observations, a failed resource verdict, or a claimed measurement that differs from the originals. A passed title, `case-complete` or supplied summary alone cannot pass. Unrelated profiles keep their existing stamp-validation behavior.

## Priming and budgets

Before scored setup, execute exactly one fixed short-shaped priming pass in the same child process and module realm: thirty cycles, ten resets, 31 distinct assigned verifiers and a separate corpus, topic, group and clients. Complete strict joined priming cleanup and its boundary before admitting scored setup. Priming has a separate 1020000-ms allocation and contributes nothing to scored cycles, identities, verifiers or heap fits. The original owner, parent and CI lifecycle clocks start before their respective work, include priming and are never restarted. Priming is finite; no repeat-until-stable behavior is permitted. All original scored workload, baseline, observation, heap, resource, configuration, terminal and cleanup requirements remain binding. Historical failures remain failed.

Short/full scored slots remain 32000/64000 ms and scored spans remain 960000/1920000 ms. Inclusive child totals and watchdogs are 2040000/3000000 ms; original-owner work cutoffs are 2030000/2990000 ms. Natural exit must also occur within 10000 ms of independently validated scored cleanup. Keep the original scored stage allocations, warm-up 10000 ms, settle 1000 ms and periodic cadences 4000/20000 ms. Retain the 262144-B/min within-phase slope, 2097152-byte endpoints, six-sample minimum, all thirty scored cycles, ten resets, 1800 identities and every assigned 5000-ms verifier window. Unused priming time cannot enlarge scored work.

## CI acceptance

Under D141, GitHub CI is the sole live judge for the r6 share-soak demonstrations. Obtain fresh cold short Jest and full CLI receipts on Kafka4.3.1/Node24/share-base, each in its own isolated CI job with its own newly started base Compose cluster. The short job retains scripts/ci/naturalExit.js soak with exposed GC, the exact short test identity, --detectOpenHandles and --runInBand, without forced exit. The full job uses a fixed parent observer for node --expose-gc scripts/soak/soak.js --profile share-consumer. The dedicated short CI receipt satisfies both the short-harness and cold-short demonstration requirements; preserve every other required tuple and frozen repeat. No local sole-guard invocation or developer-host live run is required or authorized for these demonstrations.

Before scored setup, execute exactly one fixed short-shaped priming pass in the same child process and module realm: thirty cycles, ten resets, 31 distinct assigned verifiers and a separate corpus, topic, group and clients. Complete strict joined priming cleanup and its boundary before admitting scored setup. Priming has a separate 1020000-ms allocation and contributes nothing to scored cycles, identities, verifiers or heap fits. The original owner, parent and CI lifecycle clocks start before their respective work, include priming and are never restarted. Priming is finite; no repeat-until-stable behavior is permitted. All original scored workload, baseline, observation, heap, resource, configuration, terminal and cleanup requirements remain binding. Historical failures remain failed.

Inclusive short/full child and parent watchdog bounds remain 2040000/3000000 ms, original-owner work cutoffs remain 2030000/2990000 ms, and natural exit must also occur within 10000 ms of the independently validated scored cleanup timestamp. Parent CI step limits are 35/51 minutes; dedicated producing job limits are 55/71 minutes, including explicitly allocated setup, verification, failure cleanup, sealing and upload. Admission must preserve the full selected parent step and every outstanding post-run obligation against the original lifecycle deadline. Never clip the watchdog, renew a clock or use unused priming time to enlarge scored work. The former 2460/3420-second local guard caps, 42/58-minute whole-guard steps, 2490/3450-second E14 direct execution contracts and 4290/5250-second host accounting are superseded for these CI demonstrations, not added to their budgets.

Job isolation replaces the host reservation and teardown transaction. Archive its original ownership, startup, admission, validation, teardown and absence facts. The fixed parent must observe actual natural child exit. A separate validator must reconstruct both scopes and every inherited acceptance predicate from authenticated archived primitives before teardown and again from the sealed CI artifact. Bind commit/tree identities, source digests, fixed command/environment, observed protocol/configuration inventory and platform run/artifact provenance into the receipt. Missing, failed, forged, late, cancelled, partial or unreconstructible evidence fails closed. Removing containers is not product cleanup proof. All post-priming sensitivity/isolation/error/population controls, independent reviews and remaining functional coverage stay required.

Fit post-warm-up heap samples separately inside each scored phase. Independently compare the three disconnected phase-end heaps using their original monotonic observation times, with an additional 98304-byte/minute slope limit. Keep the existing quiet-heap endpoint, resource, source-validity and minimum-sample checks. Do not fit active samples from different phases as one working set.

The historical guarded `test:e2e:share-soak` alias is superseded for these demonstrations. The owner's serialized `schedule.lifecycleMs` field is historical and cannot select a CI deadline. CI invokes the fixed parents `node scripts/ci/naturalExit.js soak` and `node scripts/ci/naturalExit.js soak-full`. The short argv additionally includes `--json --outputFile=.tmp/e2e/share-soak-jest.json`; exactly its selected test must pass.

The required check names are `share-soak-short`, `share-soak-full`, `share-soak-verify-short`, `share-soak-verify-full` and `share-soak-verdict`. The exit inventory requires `share-soak-verdict` **and the existing Exit gate** for the same tested commit. E14 consumes CI evidence contracts for both profiles and every predeclared frozen repeat/tuple; it does not execute a local soak or accept another run's green badge. A manual run supplies repeat evidence only for its immutable tested commit; the required pull-request receipt runs on the memory-gates branch. Prior failed/cancelled/partial attempts remain failed/cancelled/partial.

The original setup allowance is shared: checkout 1 minute, Node setup 1, dependencies 4, freeze/start/readiness 4. Before spawning, short/full admission requires 2700000/3660000 ms remaining from the original 55/71-minute lifecycle, including the complete 35/51-minute parent stage, 540000 ms of post-run work and 60000 ms reserve. Verification has 3 minutes, logs 1, owned teardown 2, sealing 1 and upload 2. All bounds are inclusive; a one-millisecond overrun fails.

`scripts/ci/shareSoakJob.js` freezes all tracked source bytes and the fixed command/environment before any Compose effect, refuses existing daemon resources, and owns the base fixture only. Each cleanup stage remains registered with `always()`. The final deterministic seal inventories relative paths, byte lengths and SHA-256 digests under `.tmp/e2e`, excluding only `seal.json` itself. Artifacts include hidden files, use profile/run/attempt names and retain evidence at least 14 days. GitHub job/artifact outcome and ZIP digest are authenticated separately after upload. No self-declared pre-upload success field replaces platform provenance.

`scripts/evidence/shareSoakAcceptance.js --contract <contract.json> --archive <directory> --provenance <platform.json>` reconstructs mandatory two-scope originals and compares summaries afterward. The before-teardown process uses the same entry with final sealing/provenance deferred; archive revalidation has no Docker or broker access. The verifier authenticates its own matching frozen source separately from the producer's archived Node24 stamp. Missing primitive producer/raw/membership/cleanup records fail closed; a passing child summary cannot fill them. CI receipts and independent review remain required.

The historical receipts identified below stay failed for `share-steady: retained-heap screen`; they also lack the required monotonic timestamps. Fresh complete short/full CI receipts remain necessary. No production fix or passing live heap screen is claimed. Diagnostic heap-snapshot work needs a separately authorized envelope and cannot supply a passing acceptance receipt.

## Broker-free controls

The soak spec covers 128/256/384 KiB/min in both selectors, the old short-profile sensitivity gap at 128, full-profile endpoint rejection, a first-use constant step, and first-phase-only growth followed by a quiet plateau. Boundary controls cover flat/declining heaps, exactly 98304, a fractional slope that rounds to 98304 but must fail, irregular translated clocks, invalid sources and chronology, independent endpoints/resources, original-owner observation timestamps and failed-verdict archival. Existing timer provenance, pre/post resource comparisons, product-retained socket/listener failures and bounded periodic-history controls remain in place.

`shareHeapReplays.json` contains lossless screen-input projections of the two frozen archives, plus their original resource measurements. Periodic snapshots retain the four screen inputs; quiet snapshots preserve their original resource and timer provenance. These fixtures do not replace the complete diagnostic archives. Source SHA-256 identities:

- `3bc5685f963f5be0be55cd6adc117254083b500c8c077687bb1e05006deb77b9`
- `2e58df7a7396b316368b126f37d245a58d3db6ce9b9663c08ac7a96b9207ba89`

The replay requires exact equality with the original 74-sample-per-phase measurements, including steady slopes 285358.8288424998 and 289501.63388316595 B/min respectively. Full diagnostic versus projected in-memory histories must produce identical measurements and violations. The evidence-validator specs independently exercise original-evidence mutations, forged summaries, missing chronology, per-scenario isolation and both selectors through the stamp-verification entry point.
