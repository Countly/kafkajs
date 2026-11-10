# Retained diagnostic observations

Run the existing Node-24 diagnostic selector in its isolated CI lane. This remains
`diagnostic: true`, `acceptanceEligible: false`. The sixteen serial children, seven
capture checkpoints, six block ends, original M, acceptance limits, flags and time
budgets are unchanged. Scalar children use the same catalogue without snapshots.

Each settled reading now records five observations per existing GC pair:

1. Immediately after the second GC.
2. Before settlement turn one.
3. After settlement turn one.
4. Before settlement turn two.
5. After settlement turn two.

The observer reserves thirty rows of thirteen numbers (3120 bytes) before C0 and
reuses them for each reading. There are no extra collections, waits, settlement
turns, intervals or async observers. After the original final reading, the bounded
transport writes one `settlement` event containing all completed pair observations,
the operation phase, measurement ID and original GC checkpoints. Existing native
edge marks carry submission/settlement row IDs, weak-issued job IDs and codec names;
observations carry the latest submitted/settled IDs and codecs (1 = GZIP, 4 = ZSTD),
and whether that job is still pending. JSON rows retain no job, view or store.
The existing event/byte capacities remain in force; overflow makes the diagnostic
incomplete rather than dropping facts.

Independent replay checks IDs, pair counts, stage order, arithmetic, chronology,
native edge identities and correspondence with each original returned reading.
A completed child's `record.json` includes `settlement.pairs`: signed deltas across
each turn and from the immediate post-GC observation to the original final reading,
plus native edges during that interval. `postGcMinusM` and `readingMinusM` use the
original M reading, including for the later blocks. Incomplete records retain raw
sidecars and problems without a derived settlement report.

Offline `analyse.js` additionally writes `cold-ownership.json` for each capture
child, linked from `ownership.json` and `ownership-summary.json`. It includes:

- Stable store IDs, self bytes, owning ArrayBuffers, aliased views and named root
  paths at M, closed-0, closed-5 and all-released; consecutive additions, removals,
  replacements and surviving identities are explicit.
- Material new/resized owners at closed-0 relative to M, followed by ID through
  closed-5 and all-released, with semantic liveness, category and root path.
- Original-M and consecutive additive self-byte ledgers, including unknown
  additions/removals, survivor resizing and category transfers.

Positive post-GC deltas locate net allocation after the collection. A violation
that appears only across settlement turns supports post-GC allocation at that
reading; a native settlement in that interval establishes overlap, not causation.
Surviving ownership instead requires the cold stable-ID ledger and named retaining
paths, with release/replacement checked at the later existing captures. Both
phenomena can occur together. An immediate post-GC overage alone proves no owner,
and a released JS wrapper proves no native scratch release. Observer allocations
and snapshot perturbation remain diagnostic effects. Unknown material ownership
keeps `attributionComplete: false` until reviewed. No snapshot totals or temporal
deltas are subtracted from heapUsed, external, original M or an acceptance result.
