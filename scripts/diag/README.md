# Share soak heap diagnostics

This branch and workflow are diagnostic only. They never produce acceptance receipts.
The workflow runs only on pushes to `diag/heap-*`, on Node 24 and Kafka 4.3.1 using
`docker-compose.yml` (the existing share-base tuple). No normal soak, validator,
workflow, product code, heap limit or workload budget is changed.

## Capture envelope

The standalone command is `SHARE_SOAK_HEAP_DIAG=1 node --expose-gc scripts/diag/shareHeap.js`.
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

`SHARE_SOAK_HEAP_EXTRA_CYCLES=N` optionally appends exactly N steady-shaped cycles
(0–60, default 0; workflow 10), with sixty new records, membership churn, original
cycle oracles and 32-second slots. The original periodic sampler, compact history
projection and full disk archive also run during repetitions; compact samples
continue in the same history. Cumulative corpus and offsets continue. Every ten
cycles, and after a final partial block, admin and producer disconnect, sockets
settle and another snapshot is captured. These are separate diagnostic blocks,
never acceptance cycles. The original final verifier runs at its original corpus
end before these extra blocks; strict group/topic/resource cleanup follows them.
Snapshots can reveal both bounded owner reuse and intentional corpus growth; this
mode alone does not establish saturation or prove a leak absent.

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
job, and available partial evidence is still compressed and uploaded.

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
node --max-old-space-size=4096 scripts/diag/heapDiff.js \
  diag-heap/heap-baseline.heapsnapshot.gz \
  diag-heap/heap-share-steady.heapsnapshot.gz \
  diag-heap/heap-share-reset.heapsnapshot.gz \
  diag-heap/heap-share-members.heapsnapshot.gz \
  diag-heap/heap-repeat-010.heapsnapshot.gz \
  --out diag-heap/analysis
```

The script compares baseline to each end separately. It prints the twenty largest
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
