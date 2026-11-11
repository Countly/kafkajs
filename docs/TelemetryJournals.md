# Telemetry churn journal storage

The telemetry workers reserve and touch all journal backing buffers before the telemetry-disabled
`c0` reading. The same buffers remain owned through the closed reading. Archives materialize
later. The reference evidence declares the allocation sizes and schedule capacities. No reading,
limit, measurement boundary, GC protocol or workload is adjusted or subtracted.

The share fact archive uses one-byte field codes and exact small integers; fractional values,
large numbers and negative zero retain Float64 representation. Well-formed strings use UTF-8;
a separate per-string tag preserves lone surrogates with UTF-16LE. Repeated schema labels occupy
64 fixed 256-byte slots per share journal, containing only bytes. Each label may use up to 249
encoded bytes; longer strings use the ordinary lossless encoding and consume the byte arena.
The fixed workload needs fewer than 64 labels (lifecycle names, group, topic identifiers, topic,
decision types and error names/reasons). Row, byte or label exhaustion fails the run, including
when an outer workload catches the exception. Storage never grows, wraps or overwrites.

The share journals use the packed layout (`share-packed-1`). A lower-case 64-character SHA-256
string in `requestSha256` or `responseSha256` is stored as 32 raw bytes, and a canonical decimal
int64 string in `firstOffset` or `lastOffset` (an optional leading `-`, no leading zeros, no `-0`,
inside the int64 range) as 8 raw bytes. Every other value keeps its ordinary lossless encoding:
upper-case or wrong-length hex, an out-of-range or non-canonical offset text, a number, `null`, a
missing field or a lone surrogate. Each fact therefore decodes to exactly what was pushed, and an
archive cannot differ from the one the legacy columns layout (`share-columns-utf8-1`, kept as the
exactness oracle) decodes. Text that cannot be packed costs more than its reserve and overflows
fatally; it is never truncated.

## Capacity derivation

These are bounds on the fixed workload, not averages measured from an archive:

- The corpus has 10,000 records, admitted in 100 batches of 100; reserve 11,000 record operations
  and 110 producer slots, including an explicit 10% delivery/duplicate margin. Beyond that margin,
  a run fails instead of claiming complete evidence.
- The topic has three partitions, hence at most three simultaneous node sessions. An empty or
  error round owes at least 50 ms in `shareFetchLoop.js`. Over the entire 540,000 ms case envelope,
  this admits 3 × ceil(540,000 / 50) = 32,400 paced rounds. The 500 ms broker wait is a maximum,
  not a minimum interval. A productive fetch may return only one record, so its bound is 11,000,
  not 100 and not ceil(records / the default 500-record node limit).
- Allow one separate acknowledgement flush per record operation (11,000). This handler requests
  no renewals. With a reopen every 60,000 ms, reserve 3 × (ceil(540,000 / 60,000) + 2) = 33
  session episodes, including setup/close margin, and four opening/final/reset exchanges per
  episode (132). The exchange row bound is therefore 32,400 + 22,000 + 132 = **54,532**.
- Every reserve below is the proved worst-case encoded size of its row in the packed layout,
  rounded up to a multiple of 16.
- Headers reserve 176 bytes each: ordinal/socket/API/version/correlation scalars, exact request
  and response clocks, two raw 32-byte SHA-256 digests, schema/array prefixes and label-slot
  indices. The full maximum header occupies at most 167 bytes with int32 identifiers; the
  remainder is explicit shape margin. Digests are whole 32-byte values, never truncated
  fingerprints.
- The maximum topic shape is one topic and three partitions. At a paced empty-round boundary,
  the node assignments together contain at most three partitions, already counted by the three
  node rounds above. Every other exchange may name all three. Reserve 80 bytes per request/response
  partition pair: 32,400 + 3 × (22,000 + 132) = **98,796 pairs**. Field codes, topic label indices,
  partition/status scalars and empty batch/range array prefixes fit within that bound (72 bytes at
  most, with each pair's own topic wrappers).
- Acquired and acknowledgement ranges may each split down to one range per record, with a
  separate acknowledgement type per record. Reserve **64 bytes per record operation** for both
  sides together: an acquired range with two 8-byte int64 offsets and a delivery count (29 bytes)
  and an acknowledgement batch with two offsets and one type element (34 bytes), 63 in all. A node
  response can contain up to 500 acquired records; the global record budget covers those ranges
  regardless of their batching or partition distribution.
- Public acknowledgement rows reserve 80 bytes each for topic/type/error label indices,
  partition, two 8-byte int64 offsets, clock and schema prefixes. The error-bearing shape, the
  largest, takes 67.
- Ordinary commit/pause receipts reserve 2 × (ceil(540,000 / 60,000) + 3) = 24 rows per journal;
  the extra episodes cover initial/final receipts. The existing 256-byte receipt row, 65,536-byte
  sample reserve and 1,024 native/pending-window rows remain conservative fixed envelopes.

| Storage owner | Rows / sizing | Allocated bytes |
| --- | --- | ---: |
| Ordinary offered + resolved IDs | 2 × 11,000 × 16 | 352,000 |
| Ordinary handlers + completeness bitmap | 11,000 × 32 + 10,000 | 362,000 |
| Ordinary commit + pause receipts | 2 × 24 × 256 | 12,288 |
| Ordinary producer slot lateness | 110 × 16 | 1,760 |
| Native compression journal | 1,024 × 2,048 | 2,097,152 |
| Pending native windows | 1,024 × 256 | 262,144 |
| Memory samples | (11 + 4) × 65,536 | 983,040 |
| Engine code readings (shadow) | 64 × 64 | 4,096 |
| Share exchanges | 54,532 × 176 + 98,796 × 80 + 11,000 × 64 + 16,384 label bytes | 18,221,696 |
| Share public acknowledgements | 11,000 × 80 + 16,384 label bytes | 896,384 |
| **Producer / consumer / admin total before c0** | Ordinary + native + windows + samples + engine code | **4,074,480** |
| **ShareConsumer total before c0** | All owners | **23,192,560** |

Before packing the share reserve was 25,416,640 bytes (23,288,256 exchanges, 2,128,384
acknowledgements) and the share worker declared 29,487,024 bytes; the ordinary reserve is 728,048
bytes. The packed layout lowers the share reserve to 19,118,080 bytes and the share worker to
**23,188,464 bytes**, **6,294,464 bytes (6.00 MiB, 21.3%)** less including the new 4,096-byte engine code journal, without shrinking any row bound
from observed traffic: each reserve is derived from the worst case of the new encoding. The
producer, consumer and admin workers each declare 4,074,480 bytes, including that engine code journal. All declared bytes are
allocated and touched before `c0`; moving the allocation changes the disabled RSS baseline, which
is disclosed, not a subtraction from the 64 MiB delta verdict.

## Setup allowance of the share consumer

All declared journal bytes count toward the baseline, the setup peak and the 160 MiB absolute
peak. The share consumer's setup allowance is **96 MiB plus its declared journal bytes**,
100,663,296 + 23,192,560 = 123,855,856 bytes (118.12 MiB); every other kind keeps 96 MiB. The
64 MiB increase over the control and the 160 MiB absolute peak are unchanged for every kind.
The broker-free resource lane keeps its own unchanged 96 MiB allowance.

The extra allowance is an exact constant, never a measurement: `declaredJournalBytes(kind)` sums
the allocations that the journal limits imply. The worker records it as
`journals.setupAllowanceBytes` beside the allocation it made, and the verdict stores it as
`measured.setupAllowanceBytes`. The verdict and the artifact validator, which re-evaluates every
recorded input, recompute the allowance from the kind and fail the run when the recorded
allowance, or the recorded share allocation, differs from the recomputed bytes.

## Predictions, not acceptance results

The following arithmetic uses the Node 22 / Kafka 4.2.2 H2 archive from CI run 42988230730,
head `7fca0ccf`, for the reserves before packing. Assume fully resident buffers and identical
non-journal memory: add the new ordinary/share reserve to the control and setup peak; change the
absolute peak by new minus old ordinary/share storage. Native/windows/sample storage was already
present before the control. The resulting delta is the old delta minus the old after-control
reserve. This model does not predict allocator, GC or compilation effects and does not establish
a passing run.

| Worker | Headroom against 64 MiB delta | Headroom against 96 MiB setup | Headroom against 160 MiB absolute |
| --- | ---: | ---: | ---: |
| Producer | +15.63 MiB | +12.58 MiB | +31.13 MiB |
| Consumer | −2.93 MiB | +9.95 MiB | +8.65 MiB |
| Admin | +22.25 MiB | +14.52 MiB | +38.16 MiB |
| ShareConsumer | −6.29 MiB | −10.62 MiB | −15.59 MiB |

Negative headroom predicts an overage. Consumer RSS remains a separate pre-existing concern.
Under the same model the packed share layout lowers the share worker's setup and absolute peaks by
6,294,464 bytes (6.00 MiB), including the engine code journal, and leaves its delta unchanged, because the reserve precedes
`c0`. Against the 160 MiB absolute peak the share headroom becomes −9.58 MiB, so packing alone is
not predicted to clear that gate. Against its own setup allowance (96 MiB plus the declared bytes)
the headroom is 17.50 MiB, whatever the declared size, since the allowance moves with the
reserve. The closed-transient verdict is outside this storage fix. Fresh CI evidence must judge
all unchanged limits.

## Regression controls

`telemetryJournals.spec.js` replays full identities and share facts against the independent array
reference, preserves UTF-8/lone-surrogate strings, checks exact numeric boundaries, fixed capacity,
atomic overflow and duplicate/invalid bitmap IDs. It also decodes the legacy columns layout and
the packed layout of the same edge-case facts (digest and offset fallbacks, UTF-16, fractional
times, missing fields, errors, duplicates) to identical archives, proves the worst-case size of
every share row against its reserve, drives every dimension (rows, bytes, label slots, text that
cannot be packed) to an overflow that stays recorded when the caller catches it, and checks that
`declaredJournalBytes` equals the bytes each worker allocates and fills. `soak.spec.js` checks
the share setup allowance, that other kinds and the 64 MiB and 160 MiB limits are unchanged, and
that a recorded allowance or allocation that differs from the recomputed value fails the verdict.
`telemetryChurn.spec.js` injects missing, duplicate, reordered and wrong-timestamp decoded
journal facts into the archive read by the production evidence verifier, keeping an unmodified
journal archive positive case.

The controls have two layers. Fast unit discovery uses deterministic virtual-time journal replay
for exact IDs, facts, exchange hashes, overflow and malformed archive cases, and seeded evaluator
vectors. Virtual timestamps never accompany an absolute real-memory slope assertion.

The existing `resources` job in `memory-gates.yml` runs the second layer serially on Node 22, 24
and 26. `scripts/soak/journalControls.js` runs paired virtual-time ownership children for each
workload, then real-time seeded/unseeded children in both legacy and fixed modes. The job runs the
producer, consumer and share workloads (`--kinds=producer,consumer,shareConsumer
--protocols=resource`); its 105-minute budget includes the original resource measurements and
twelve six-minute calibration arms. The separate `journal-controls` job runs, per Node line, the
admin workload under both protocols and the live-flags arms of the other three, six six-minute arms
each, so neither job's timeout grows. The `resource` protocol uses the resource workers'
executable, `MEMORY_FLAGS` and unchanged `ProcessMemory.settled()` protocol (two collections 100
ms apart, two event-loop turns, up to six pairs). The `live` protocol runs the fixed journal under
the telemetry worker's exact flags (`WORKER_FLAGS`, `--expose-gc` alone) and its settled reading
(two collections 100 ms apart, two event-loop turns, one pass), with the same warm-up, window, grid
and evaluator; its results also carry the shadow raw, code and data series of its samples. It waits for the original eleven sample admissions over the real 60-second warm-up and
300-second measurement window. Actual admission, completion, clock origin and elapsed times are
archived; no virtual duration is supplied to a memory verdict.

Each real-time child allocates one positive-root WeakRef and a non-zero-prefilled scalar reading
archive before M, with no per-record weak observation or observer maps. Their counts and byte
lengths must remain constant at M, every sample and closed; overflow fails. Workload journal
storage and cursors are recorded separately. The intentionally growing per-record WeakRefs and
WeakMap in the separate ownership replay are inventoried, together with its fixed archive buffers,
and never supply calibration readings. Seeded arms retain a real product snapshot and 4,096
bytes per record; unseeded arms release them. Each arm has a role, written into its summary row:

| Arm | Role | Judged by |
|---|---|---|
| fixed, unseeded | `null` | no violation |
| fixed or legacy, seeded | `seeded-positive` | both violations |
| legacy, unseeded, consumer and share | `contamination-positive` | the retained-slope violation; the endpoint violation is also asserted whenever its measured value exceeds the limit |
| legacy, unseeded, producer | `contamination-positive` | paired ordering only: legacy slope and endpoint above the fixed null's, on the same Node |
| legacy, unseeded, admin | `no-contamination-recorded` | recorded only: the admin writes no legacy journal |
| fixed, unseeded, live flags | `live-configuration-null` | recorded only |
| fixed, seeded, live flags | `seeded-positive` | both violations |

The legacy unseeded arm is a contamination-positive control, not a null: the old array journals
really grew, so detecting them shows the method sees harness contamination. A crashed, timed-out
or malformed child, or a verdict without measured values, is a failure and never a detection or a
null. No limits, bytes or samples are adjusted to make a control pass; legacy journal growth and
runtime effects remain visible.

The real-time verdict is explicitly limited to slope and endpoint components with the unchanged
limits and least-squares calculation. It does not invoke the full resource verdict, so it claims
no identity, RSS or closed-transient acceptance. JSON preserves every heap, external,
arrayBuffers and engine code/bytecode reading and the complete component verdict. Each result is
written before assertions; remaining arms still run after a failure, and all failures are saved
in `journal-controls/summary.json` beneath the resource artifact directory.

Legacy/fixed ownership rows must agree at M, first, last and closed, including exact hashes,
released target IDs and the deliberately retained positive product root. Fixed storage format and
byte length are checked at every checkpoint; legacy arrays report no fixed-storage metadata.
These diagnostics are not live-worker attribution or acceptance evidence.

## Engine code shadow

Every worker also records the engine's code sizes beside each reading it takes: loaded, c0, c1,
control-released, M, each sample, closed and final. An `EngineCodeJournal` keeps `v8.getHeapCodeStatistics()`
`code_and_metadata_size` and `bytecode_and_metadata_size` as scalars in 64 fixed rows, touched before
c0 and counted in the allocation. It has its own owner entry in each snapshot's journal state, so the
constancy check covers it, and an overflow throws.

The parent's `resource-verdict` row carries a `shadow` object next to `result`: for the settled samples
it reports the least-squares slope per minute and the first-to-last change of the raw series
(`heapUsed + external`), of the code series (code plus bytecode) and of the data series (raw minus
code). The verdict, its recomputation from `input` and every limit are unchanged, and nothing in
acceptance reads the shadow. The live-flags calibration arms report the same shadow for their samples.
