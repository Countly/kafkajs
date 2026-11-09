# E12 share soak tooling and controller hand-off

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

## Controller integration

The sole reviewed live guard must invoke this validator on each scenario's original `soak-resources` baseline, phase-end, teardown and resource-verdict rows, in archive order, after authenticating the tree, source digest, run identity, selector and broker tuple. Call `verifyShareQuietHeap(originalRows)` and fail if `problems.length !== 0`; alternatively use the repository `verifyFiles` pipeline for the additional predicate. Retain all existing guard recomputations of periodic heaps, resources, schedules, terminal/verifier evidence, cleanup and natural exit. This helper validates only the added predicate and is not a second guard. External guard wiring and manifest/seal rebinding remain controller-owned and must precede acceptance.

Keep short/full slots 32000/64000 ms, scored spans 960000/1920000 ms, child totals/watchdogs 1020000/1980000 ms, cold caps 1440000/2400000 ms, stage allocations, warm-up 10000 ms, settle 1000 ms and periodic cadences 4000/20000 ms. Retain the 262144-B/min within-phase slope, 2097152-byte endpoints, six-sample minimum, all thirty cycles, ten resets, 1800 identities and every assigned 5000-ms verifier window. There is no added workload, probe, collection, delay or acceptance budget.

The historical receipts identified below stay failed for `share-steady: retained-heap screen`; they also lack the newly required monotonic timestamps. Fresh short/full receipts and separate heap-retainer attribution remain necessary. No production fix or passing live heap screen is claimed. Diagnostic heap-snapshot work needs a separately authorized envelope and cannot supply a passing acceptance receipt.

## Broker-free controls

The soak spec covers 128/256/384 KiB/min in both selectors, the old short-profile sensitivity gap at 128, full-profile endpoint rejection, a first-use constant step, and first-phase-only growth followed by a quiet plateau. Boundary controls cover flat/declining heaps, exactly 98304, a fractional slope that rounds to 98304 but must fail, irregular translated clocks, invalid sources and chronology, independent endpoints/resources, original-owner observation timestamps and failed-verdict archival. Existing timer provenance, pre/post resource comparisons, product-retained socket/listener failures and bounded periodic-history controls remain in place.

`shareHeapReplays.json` contains lossless screen-input projections of the two frozen archives, plus their original resource measurements. Periodic snapshots retain the four screen inputs; quiet snapshots preserve their original resource and timer provenance. These fixtures do not replace the complete diagnostic archives. Source SHA-256 identities:

- `3bc5685f963f5be0be55cd6adc117254083b500c8c077687bb1e05006deb77b9`
- `2e58df7a7396b316368b126f37d245a58d3db6ce9b9663c08ac7a96b9207ba89`

The replay requires exact equality with the original 74-sample-per-phase measurements, including steady slopes 285358.8288424998 and 289501.63388316595 B/min respectively. Full diagnostic versus projected in-memory histories must produce identical measurements and violations. The evidence-validator specs independently exercise original-evidence mutations, forged summaries, missing chronology, per-scenario isolation and both selectors through the stamp-verification entry point.
