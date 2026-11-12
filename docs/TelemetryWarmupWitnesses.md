# Warm-up lifecycle witness diagnostic

The `warmup-witnesses-node24` and `warmup-witnesses-node26` CI jobs investigate late release of
warm-up objects. A successful diagnostic means the evidence is complete. It does not mean any
memory acceptance gate passed. Every new record has `acceptanceEligible: false`.

Each job runs 64 children serially, without retries: four client kinds, four public modes
(`unseeded`, `close`, `close-warmup`, `warmup-only`), and a scalar/witness pair for each public
arm and collector contrast. The collector is always unseeded, including contrasts next to seeded
public modes. Its artifact declares `seed: null` and `collectorUnseededContrast: true`.

Both variants execute the original `publicMemoryChild.js` or `resourceChild.js` with the original
`MEMORY_FLAGS`, catalogue and collection protocol. The scalar child has no witness observer.
The witness child checks weak references immediately after each existing final settled reading:

| Artifact point | Existing boundary |
| --- | --- |
| `m0` | Matched position in the warm-up pass |
| `warmupEnd` | Warm-up catalogue closed, drained and released |
| `matched` | Measured matched baseline M |
| `firstActive` | First measured codec reading |
| `closed` | Measured final close reading |

No collection, settle, turn, favourable endpoint selection or paired subtraction is added. The
observer restores its factory wrappers before the original warm-up-end settle. Existing
convergence failures, limits, ceilings and verdicts remain unchanged.

The inventory assigns stable IDs to warm-up clients, foreground/wire objects, descriptors,
collector inputs, subscriptions, slots, snapshots, payload builders, native temporaries and
observer objects. It recursively follows own data properties, map/set entries and backing
stores. Synchronous factory wrappers also witness receivers, arguments and results of the
collector's `register`, `bindNode`, `select`, `snapshot` and payload `buildPayload` methods.
Pre-existing module exports and shared meter/role objects are excluded before warm-up. IDs are
deduplicated; the census retains only native WeakRefs, weak identity maps and scalar metadata.
Capacity exhaustion fails instead of truncating. History storage is prefilled before control.
Issued WeakRef wrappers remain allocated through closed, including wrappers for dead targets.

Private fields, closure contents and weak-collection entries are opaque, and the artifact
declares `attributionComplete: false`. A target's death is first observed at a checkpoint; it is
not an exact finalization time. `deref()` keeps a live target alive through its current job, and
instrumentation can perturb memory and timing. The original scalar companion preserves the
uninstrumented comparison; the two processes are never subtracted. Deliberately retained seed
objects also receive witnesses and can be identified by their `seed:root` labels.

Download the matching `warmup-witnesses-node24` or `warmup-witnesses-node26` artifact. In
`run/summary.json`, select `pairs[]` by `arm`, `kind` and `mode`. Read:

- `analysis.answer.aliveAtM`: witness IDs alive at M.
- `analysis.answer.survivedPastM`: IDs observed alive at first active or closed.
- `analysis.answer.diedAfterM`: IDs alive at M but dead later, with `from`, `to` and that
  interval's `dataDelta`.
- `analysis.answer.stillAliveAtClosed`: IDs still alive at the final checkpoint.
- `analysis.answer.closedDataDelta` and `scalarClosedDataDelta`: each process's own closed−M
  data delta.
- `analysis.answer.closedNegativeCoincidesWithPostMDeath`: true only when closed−M data is
  negative and a post-M witness death interval also has a negative data delta. This is temporal
  coincidence, never causal attribution (`causationEstablished: false`).

`analysis.scalar.points` and `analysis.witness.points` contain exact heap/external/raw retained,
`code`, `bytecode` and `data` values at all five points. Their `checkpoints[]` preserve every
collection pair, including original times, observed GC counts and code counters. Data is exactly
`heapUsed + external - code - bytecode`; `arrayBuffers` is already inside external.

Follow `witnessFile` to `readings.warmupWitnesses.objects[]` to resolve IDs to labels and the five
`alive[]` states. `null` means not issued yet, `true` means alive and `false` means dead. The
parallel `points[]` records phase, exact reading time and issued count. Factory call counts and
capacity are also archived. `scalarFile` and `witnessFile` retain the full original child output,
including every other settled reading, native journal, seed schedule and lifecycle fact.
`summary.json.sources` identifies archived source bytes by SHA-256 and records the frozen tree,
Node and flags. A `pairs[].failure` leaves both raw records available and fails evidence
completeness; there is no rerun or acceptance fallback.

CI invokes `node scripts/e2e/telemetry/warmupWitness.js --diagnostic-only <new-directory>` on a
quiet, broker-free runner. Runtime execution belongs in CI; local review uses static checks.
