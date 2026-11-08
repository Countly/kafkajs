# Share soak heap diagnostics

This branch and workflow are diagnostic only. They never produce acceptance receipts.
The workflow runs only on pushes to `diag/heap-*`, on Node 24 and Kafka 4.3.1 using
`docker-compose.yml` (the existing share-base tuple). No normal soak, validator,
workflow, product code, heap limit or workload budget is changed.

## Capture envelope

The workflow records `SHARE_SOAK_HEAP_STEP_STARTED_AT` as decimal epoch milliseconds
before cluster setup, then runs `SHARE_SOAK_HEAP_DIAG=1 node --expose-gc scripts/diag/shareHeap.js`.
The runner refuses missing/future timestamps or setup that leaves less than its
1,955,000-ms work-and-exit envelope inside the 40-minute step.
Without that flag the command skips before loading or creating clients. Normal soak
commands never import the diagnostic runner. Use a fresh `.tmp/e2e` directory.

The runner reuses the short owner's setup, thirty original cycles, per-cycle
oracles, periodic sampling, disconnected phase observations and resource screen.
It captures after setup/baseline and after `share-steady`, `share-reset` and
`share-members`. Capture runs after the timed quiet transition returns, with the
sampler joined at phase ends and no scored slot or owner stage active. Each capture
forces two GCs, reads heapUsed, and synchronously writes a V8 snapshot. The manifest
records the file name, uncompressed size, heapUsed, original owner elapsed times
and capture duration. The diagnostic watchdog is an additional known timer owner.
Snapshot instrumentation and its allocations are part of this diagnostic heap.

`SHARE_SOAK_HEAP_EXTRA_CYCLES=10` appends exactly ten steady-shaped cycles
(default 10; every other count is refused), with sixty new records per cycle,
membership churn, unchanged cycle oracles and 32-second slots. Plans stay BigInt
inside the driver/oracle; only each diagnostic row's `first` is a decimal string.
The original periodic sampler, compact history projection and full disk archive
also run during repetitions; compact samples continue in the same history.
After the block, admin and producer disconnect, sockets settle and `repeat-010`
is captured. These are separate diagnostic cycles 30–39. The original final
verifier runs at the original 1800-identity end before repetition; cleanup follows
the 2400-identity end (800 per partition). Original final proof and strict cleanup
share 20,000 ms of actual work; repetition/capture time has its separately declared
allocation. Nothing is borrowed from the 10,000-ms exit reserve.

The diagnostic process envelope is 1,020,000 ms plus 120,000 ms per capture, plus
32,000 ms per extra cycle and 15,000 ms per extra block for connections. The last
10,000 ms is reserved for exit. Each capture must finish within 120,000 ms.
Original stage/cycle limits remain enforced around the original work; capture
pauses consume only this separate enclosing envelope. The diagnostic workflow
allows 40 minutes for cluster setup/capture, ten for analysis and 55 for the job.
These allowances are not changes to acceptance lanes. Run only in the authorized
CI envelope, never on a host where live workloads are prohibited.

The evidence file is `diag-share-heap.jsonl`; every row carries `diagnostic: true`,
`acceptanceEligible: false` and selector `DIAG_SHARE_HEAP`. Resource-screen failures
remain in the archived verdict. A zero diagnostic exit means capture/cleanup
finished, not that any acceptance screen passed. There is no `case-complete` row,
acceptance verdict or natural-exit receipt. Workload/oracle/capture errors fail the
job, and available partial evidence is still compressed and uploaded. Primary errors
survive phase-close and final-close failures; secondary errors are attached as
`cleanupErrors` and archived separately with nested causes. Both taint the run.
`diag-complete` is written only after observer restoration and independent evidence
checks: original cycles 0–29, ten reset rows, ten ordered extra-cycle rows 30–39,
exactly five ordered manifest/JSONL captures, separate original-final proof,
strict cleanup with raw before/after inventories, teardown and resource verdict.
The checks replay the unchanged cycle, terminal-position and verifier oracles.
An incomplete upload remains explicitly INCOMPLETE in both analysis formats, and
the analysis step fails after saving the partial report. It cannot turn a claimed
`diag-complete` into complete evidence by itself. The original failed resource
verdict stays failed even when diagnostic workload/capture/cleanup completes.
The workflow retains the complete JSONL, raw compressed heaps, manifest, source
digests, tuple/configuration/wire evidence and console error log under its own run ID.

## Read the artifact

Download `diag-share-heap-kafka431-node24-<run id>` from the diagnostic workflow.
Start with `diag-heap/analysis.txt`; use `diag-heap/analysis.json` for complete
baseline and end constructor/type tables and deltas. `diag-heap/snapshots.jsonl`
records capture order and original measurements. Its sizes/names describe the raw
files; the uploaded files have an additional `.gz` suffix.

To inspect in Chrome DevTools Memory, gunzip the selected `.heapsnapshot.gz` files,
load baseline and the desired end, and inspect Comparison and Retainers. Stable
object IDs apply within this single captured process. Follow growing client-owned
paths into their actual teardown or reuse code before assigning ownership.

To regenerate the offline report (raw and gzipped inputs are both supported):

```sh
NODE_PATH=. node --max-old-space-size=4096 scripts/diag/heapDiff.js \
  --manifest diag-heap/snapshots.jsonl \
  --evidence diag-share-heap.jsonl --out diag-heap/analysis
```

The script compares baseline to each end and each consecutive capture pair separately. It prints the twenty largest
positive changes by count, self bytes and retained bytes; the end totals; and up to
three objects per growing group with stable IDs, baseline survival, first observed
capture, a shortest strong path from the snapshot GC root, edge labels and
immediate-dominator chains. Samples rank individual retained growth, including
surviving owners, and show baseline retained bytes and dominators for comparison.
Each constructor also has deltas since the preceding capture to inspect repetition
increments separately from cumulative growth.
The JSON includes all constructor/type totals, even groups absent at an endpoint.
Strings are grouped by V8 type rather than their contents.

Retained bytes use immediate dominators on the snapshot graph with weak edges
removed. Each constructor/type total is the **union** of its dominated subtrees,
so nested owners of that same group do not count twice. Different groups can retain
the same descendants; their retained totals must not be added. Self bytes are
additive. Strong-unreachable nodes and bytes are reported separately. Conditional
WeakMap/ephemeron liveness is not reconstructed, so corroborate those cases in
DevTools. Root paths are strong paths, not proof that every edge is independently
sufficient to keep an object alive.

Owner hints distinguish timer/observer metadata, corpus/sample/summary storage,
client metadata/connections/sessions and V8 internals. These are name/path search
hints, not automatic ownership verdicts. Unknown owners remain unclassified. A VM
node appearing on a root path does not prove that the VM owns the growth. Use the
sample IDs and dominators for attribution and lifetime investigation across all
captures; later flattening is not a passing receipt or an attribution result.

Snapshots are gzipped before upload. The workflow refuses uploads of artifacts
with payloads at or above 1,900,000,000 bytes, reserving space below the 2 GB
archive cap, rather than silently discarding captures.

## Ownership and matched quiet points

`sincePrevious` contains a full comparison with previous-node retained bytes,
paths and dominators, rather than group-only deltas against baseline samples.
`ownership.categories` partitions every strong-reachable node's self bytes once.
Each ledger reconciles added IDs minus released IDs, surviving-ID self-size
changes and category transfers to its net byte delta. The separate type totals
expose the identity-string counts. Retained subtrees remain inspection aids;
they are never summed across owners or subtracted from measured heapUsed.

Actual named properties anchor `acknowledged`, `owner.final`, compact `samples`,
`phaseEnds`, timing/count summaries, timer-observer `owners`, module-cache exports
and live driver client/journal properties. Only domination by a property target
permits inherited classification. Shared/unknown owners remain unclassified.
Source strings are unique targets of actual `internal:source` edges. Script,
compiled-method and feedback self bytes have separate ledgers; these labels
classify no application descendants by themselves. A module-cache path shows
where storage is held, not that the whole subtree is an allowable first-use cost.

Incremental samples cover each owner category and node kind, including plain
objects, closures, arrays, buffers, Maps and feedback, independently of the old
constructor rankings. Each has direct incoming edges, actual owning properties,
a concrete root path and its previous stable-ID counterpart. Reference audits
follow feedback, sampled closures/Maps/buffers and the share prototypes through
internal/context/property edges. The original investigated feedback ID is also
inspected when present. Audits report weak edges and traversal truncation; neither
proves retired instance data absent. Consequential conditional ephemeron paths
still require a semantics-aware retaining-path review in DevTools.

Use these source lifetime expectations when reviewing actual paths:

| Owner/property | Implementation and expected quiet lifetime |
| --- | --- |
| `ShareSoakDriver.acknowledged` | `cycle()` appends sixty identities; 600/1200/1800 at original ends and 2400 after repetition. Cumulative terminal proofs intentionally retain them. |
| `ShareSoak.final` | Original `final()` proof is stored after the members capture; its new objects are distinct from the corpus strings shared with `acknowledged`. |
| `Soak.samples` | `runPhase()` keeps compact four-field observations; extra-cycle samples extend this array and have a separate ledger. |
| `Soak.phaseEnds`, `ShareSoak.timing` | Three original quiet summaries and the original stage/phase/slot history remain. Repetition has its own quiet summary. |
| `ShareTimerObserver.owners` | `snapshot()` removes destroyed timer owners; its bounded live inventory must reconcile with raw counts without drops. |
| Driver `admin`, `producer` | `closePhase()` nulls these properties before joining disconnects. They must not root retired client graphs at matched quiet points. |
| Driver `lastCycle`, `activeCycle` | The original owner and diagnostic repetition drop their per-cycle journals after archival. Generic objects elsewhere still require path review. |
| Share class prototypes and feedback | Prototypes/functions can survive module loading; trace any referenced contexts/Maps/objects into their teardown/reuse code. Zero named session counts are insufficient. |

The snapshots themselves allocate GC/heap-writer, closure/context and archival
state. Function `shared → script` edges identify the diagnostic source file,
separating the diagnostic closures and their direct contexts from arbitrary functions
named `capture`. `diagnosticSelfBytes` is a subset within each disjoint ledger,
never an extra amount to add to its total. No context descendants receive a blanket
diagnostic exemption. Such effects stay in the raw heap and accounting. Review
unclassified paths through `captureHeap`, `capture` and `runDiagnostic` as diagnostic
effects only when their retaining edges establish that ownership. Do not relabel
unknown bytes as VM or diagnostic overhead. Compare new/released IDs and self
bytes of source/script/compiled/feedback storage separately from the changing
corpus, original-final proof and compact samples. The report deliberately leaves
`attributionComplete: false`: completing repetition does not answer the remaining
ownership/lifetime questions or authorize a warm-up exclusion.

## Cleanup controls and current status

Broker-free controls use the actual ShareSoak owner, CleanupStack, baseline and
strict cleanup helpers with raw process inventories and timer allocation stacks.
Successful cleanup observes four diagnostic wrapper timers at both points.
Exceptional cleanup outside the enclosing envelope observes two fewer wrappers
and still fails/taints; this known choreography mismatch remains open. No timer
normalization, equality relaxation or shared-helper change is included. Existing
resource controls still reject product/unattributed timers, inventory drops,
sockets, listeners and deletion-proof failures.

Diagnostic run 42504768939 supports compilation, module-source and intentional harness attribution but does not clear the first-use exclusion gate. The first extra diagnostic cycle failed archival on a BigInt; the repetition block and strict cleanup did not complete. No new first-use workload, sample exclusion, baseline change or acceptance budget is authorized by r4. Historical failing receipts remain failed.

The failed run's original artifacts are immutable. New diagnostics use fresh
evidence directories, their own source digests and run-specific artifacts. No
broker, Docker, live workload or soak is executed as part of local verification.
