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

The `Memory gates` workflow (`.github/workflows/memory-gates.yml`) runs on `p9m/` pull requests and manual dispatch. Share-soak acceptance is required before 3.0.0, not for phase merges. CI jobs replace the local guard for these demonstrations. Each profile requires a fresh cold Kafka 4.3.1/Node 24 base cluster in its own isolated CI job. The workflow currently has only the short job, using `node scripts/ci/naturalExit.js soak` with its exposed-GC Jest child, `--detectOpenHandles`, `--runInBand` and no forced exit. Its parent step and job limits are 35 and 55 minutes.

The full-profile CI job and the complete independent acceptance validator are still owed to the memory-gates follow-up. The full job must use a fixed parent to observe `node --expose-gc scripts/soak/soak.js --profile share-consumer`, with a 51-minute parent step and 71-minute job limit. Admission must preserve the full selected parent step and outstanding validation, cleanup, sealing and upload allocations against the original CI lifecycle deadline. Never clip the watchdog or restart a clock.

Job isolation replaces the local reservation and teardown transaction. Archive original ownership, startup, admission, validation, teardown and absence facts. The fixed parent must observe actual natural child exit. The complete independent validator must reconstruct both scopes and every acceptance predicate from authenticated archived primitives before teardown and again from the sealed CI artifact. This includes periodic and quiet heaps, resources, schedules, per-cycle identities, membership, resets, terminal/verifier evidence, protocol/configuration observations, cleanup and natural exit. Bind commit/tree identities, source digests, fixed command/environment and platform run/artifact provenance into the receipt. Missing, failed, forged, late, cancelled, partial or unreconstructible evidence fails closed; removing containers is not product cleanup proof. Archive-only revalidation and a combined short/full verdict remain part of the follow-up. The current evidence verifier and quiet-heap helper are not the complete independent acceptance validator.

The local `npm run test:e2e:share-soak` alias is historical and superseded for these demonstrations. The former 2460/3420-second guard caps, 2490/3450-second direct-execution contracts and host accounting are retired for this CI path. The retained `lifecycleMs` values in the soak source describe those historical guard envelopes, not CI timeouts or permission for local acceptance runs.

The historical receipts identified below stay failed for `share-steady: retained-heap screen`; they also lack the required monotonic timestamps. Fresh complete short/full CI receipts remain necessary. No production fix or passing live heap screen is claimed. Diagnostic heap-snapshot work needs a separately authorized envelope and cannot supply a passing acceptance receipt.

## Broker-free controls

The soak spec covers 128/256/384 KiB/min in both selectors, the old short-profile sensitivity gap at 128, full-profile endpoint rejection, a first-use constant step, and first-phase-only growth followed by a quiet plateau. Boundary controls cover flat/declining heaps, exactly 98304, a fractional slope that rounds to 98304 but must fail, irregular translated clocks, invalid sources and chronology, independent endpoints/resources, original-owner observation timestamps and failed-verdict archival. Existing timer provenance, pre/post resource comparisons, product-retained socket/listener failures and bounded periodic-history controls remain in place.

`shareHeapReplays.json` contains lossless screen-input projections of the two frozen archives, plus their original resource measurements. Periodic snapshots retain the four screen inputs; quiet snapshots preserve their original resource and timer provenance. These fixtures do not replace the complete diagnostic archives. Source SHA-256 identities:

- `3bc5685f963f5be0be55cd6adc117254083b500c8c077687bb1e05006deb77b9`
- `2e58df7a7396b316368b126f37d245a58d3db6ce9b9663c08ac7a96b9207ba89`

The replay requires exact equality with the original 74-sample-per-phase measurements, including steady slopes 285358.8288424998 and 289501.63388316595 B/min respectively. Full diagnostic versus projected in-memory histories must produce identical measurements and violations. The evidence-validator specs independently exercise original-evidence mutations, forged summaries, missing chronology, per-scenario isolation and both selectors through the stamp-verification entry point.
