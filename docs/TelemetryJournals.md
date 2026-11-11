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
- Headers reserve 256 bytes each: ordinal/socket/API/version/correlation scalars, exact request
  and response clocks, two 64-character SHA-256 strings, schema/array prefixes and label-slot
  indices. The full maximum header occupies at most 242 bytes with int32 identifiers; the
  remainder is explicit shape margin. SHA strings stay UTF-8, not truncated fingerprints.
- The maximum topic shape is one topic and three partitions. At a paced empty-round boundary,
  the node assignments together contain at most three partitions, already counted by the three
  node rounds above. Every other exchange may name all three. Reserve 80 bytes per request/response
  partition pair: 32,400 + 3 × (22,000 + 132) = **98,796 pairs**. Field codes, topic label indices,
  partition/status scalars and empty batch/range array prefixes fit within that bound.
- Acquired and acknowledgement ranges may each split down to one range per record, with a
  separate acknowledgement type per record. Reserve **128 bytes per record operation** for both
  sides together, including signed int64 offset strings (up to 20 characters), delivery count,
  type elements and range prefixes. A node response can contain up to 500 acquired records; the
  global record budget covers those ranges regardless of their batching or partition distribution.
- Public acknowledgement rows reserve 192 bytes each for topic/type/error label indices,
  partition, two signed int64 offsets, clock and schema prefixes. Even the error-bearing shape
  fits with margin.
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
| Share exchanges | 54,532 × 256 + 98,796 × 80 + 11,000 × 128 + 16,384 label bytes | 23,288,256 |
| Share public acknowledgements | 11,000 × 192 + 16,384 label bytes | 2,128,384 |
| **Producer / consumer / admin total before c0** | Ordinary + native + windows + samples | **4,070,384** |
| **ShareConsumer total before c0** | All owners | **29,487,024** |

Previously the ordinary reserve was 1,405,840 bytes and the share reserve 29,573,120 bytes,
allocated after the disabled control. The new ordinary reserve is 728,048 bytes and the new share
reserve 25,416,640 bytes. The exchange arena grows to cover the corrected count bound; the much
smaller acknowledgement and ordinary reserves reduce total share-worker storage by **4,834,272
bytes (4.61 MiB)**. All 29,487,024 bytes now count toward the 96 MiB setup and 160 MiB absolute
allowances. Moving the allocation changes the disabled RSS baseline; it is explicitly disclosed,
not a subtraction from the 64 MiB delta verdict.

## Predictions, not acceptance results

The following arithmetic uses the Node 22 / Kafka 4.2.2 H2 archive from CI run 42988230730,
head `7fca0ccf`. Assume fully resident buffers and identical non-journal memory: add the new
ordinary/share reserve to the control and setup peak; change the absolute peak by new minus old
ordinary/share storage. Native/windows/sample storage was already present before the control.
The resulting delta is the old delta minus the old after-control reserve. This model does not
predict allocator, GC or compilation effects and does not establish a passing run.

| Worker | Headroom against 64 MiB delta | Headroom against 96 MiB setup | Headroom against 160 MiB absolute |
| --- | ---: | ---: | ---: |
| Producer | +15.63 MiB | +12.58 MiB | +31.13 MiB |
| Consumer | −2.93 MiB | +9.95 MiB | +8.65 MiB |
| Admin | +22.25 MiB | +14.52 MiB | +38.16 MiB |
| ShareConsumer | −6.29 MiB | −10.62 MiB | −15.59 MiB |

Negative headroom predicts an overage. Consumer RSS remains a separate pre-existing concern.
The share journal change alone is not predicted to clear its RSS gates; preallocation also raises
its setup peak. The closed-transient verdict is outside this storage fix. Fresh CI evidence must
judge all unchanged limits.

## Regression controls

`telemetryJournals.spec.js` replays full identities and share facts against the independent array
reference, preserves UTF-8/lone-surrogate strings, checks exact numeric boundaries, fixed capacity,
atomic overflow and duplicate/invalid bitmap IDs. `telemetryChurn.spec.js` injects missing,
duplicate, reordered and wrong-timestamp decoded journal facts into the archive read by the
production evidence verifier, keeping an unmodified journal archive positive case.

The controls have two layers. Fast unit discovery uses deterministic virtual-time journal replay
for exact IDs, facts, exchange hashes, overflow and malformed archive cases, and seeded evaluator
vectors. Virtual timestamps never accompany an absolute real-memory slope assertion.

The existing `resources` job in `memory-gates.yml` runs the second layer serially on Node 22, 24
and 26. `scripts/soak/journalControls.js` runs paired virtual-time ownership children for each
workload, then real-time seeded/unseeded children in both legacy and fixed modes. Its 105-minute
job budget includes the original resource measurements and twelve six-minute calibration arms.
The calibration uses the resource workers' executable, `MEMORY_FLAGS` and unchanged
`ProcessMemory.settled()` protocol (two collections 100 ms apart, two event-loop turns, up to six
pairs). It waits for the original eleven sample admissions over the real 60-second warm-up and
300-second measurement window. Actual admission, completion, clock origin and elapsed times are
archived; no virtual duration is supplied to a memory verdict.

Each real-time child allocates one positive-root WeakRef and a non-zero-prefilled scalar reading
archive before M, with no per-record weak observation or observer maps. Their counts and byte
lengths must remain constant at M, every sample and closed; overflow fails. Workload journal
storage and cursors are recorded separately. The intentionally growing per-record WeakRefs and
WeakMap in the separate ownership replay are inventoried, together with its fixed archive buffers,
and never supply calibration readings. Seeded arms retain a real product snapshot and 4,096
bytes per record; unseeded arms release them. Every unseeded real-time arm must remain under
both limits, and every seeded arm must trip both. No limits, bytes or samples are adjusted to
make a control pass; legacy journal growth and runtime effects remain visible and can fail it.

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
