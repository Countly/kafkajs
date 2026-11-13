# Share reopen and cleanup allocation diagnostic

This is diagnostic evidence, never a memory acceptance input. The 160 MiB peak and
64 MiB delta ceilings, original reserve, workload, sampling grid and GC protocol
are unchanged. No product telemetry code changes are included.

## Ranked source candidates

References below use the current source. The original bulk sink read is visible
in the parent of the incremental-reader change, at
`testHelpers/telemetryFixtures.js:103–115`.

| Rank | Path and allocation | Size estimate and relevance |
| --- | --- | --- |
| 1 | `TelemetryExportReader.readExports`, `testHelpers/telemetryFixtures.js:74`; called by observer joins at `:1618` and health checks at `:1289`. Previously allocated a whole-file Buffer, decoded the whole file, split it, and parsed every row for each decode. | Each of three nodes allows 2,097,152 B; four unreclaimed scans of one full node can account for 8,388,608 B of buffers and another 8–16 MiB of strings, before parsed row objects. The archived enabled runs have only one sink: 2,041,609 B (4.2.2) and 2,055,489 B (4.3.1). This is the strongest candidate for the observed transient. |
| 2 | `TelemetryWorker.seal` at `scripts/soak/telemetryChurn.js:2888` and `cleanup` at `:3240`: selected decodes start with `Promise.allSettled` at `:2899` / `:3261`. | Amplifies rank 1: each uncached selected Push enters a separate sink scan before awaiting the decoder. The selection is first Push per socket/custom Get plus every accepted final (`:979`), not every Push. Observer records cap at 4,096; raw/transmitted payloads cap at 4,096 B each. Copying decoded results adds heap work; concurrency alone does not establish a single 8 MiB allocation. |
| 3 | Observer transcript copying at `testHelpers/telemetryFixtures.js:971` / `:1813`, disposal JSON at `:1783`. | Up to 4,096 record copies plus a whole transcript string. A successfully packed transcript is at most 2 MiB; old records, copied objects, per-record stringify/parse and final serialization coexist. The observer's 2 MiB accounting charges metadata only 64 B/record, not its complete serialized size; transcript size is checked after serialization. Thus the declared accounting is not a strict temporary heap bound. Archived share transcript maxima are only about 253 KB, making this a weaker explanation for these peaks. |
| 4 | `packSegment`, `testHelpers/telemetrySegments.js:158–202`: all payload/proof objects, per-line JSON strings, joined bundle, validation parse, then `Buffer.from(bundle)`. | Transcript and bundle bounds are 2 MiB each. At the bundle limit, joined text and output buffer are each 2 MiB, with another 2 MiB of line strings plus parsed/base64/proof copies; base64 expands binary by about 4/3. Several representations can total 8 MiB or more, but the final output Buffer is capped at 2 MiB. Actual share bundles are about 77–81 KB. No concat of all Push blobs exists here. |
| 5 | Share facts/hash reconstruction at `scripts/soak/telemetryChurn.js:1670`, and parent terminal/archive verification at `:2276–2330`; archive reader bounds at `scripts/e2e/telemetry/evidence.js:41,439`. | Share facts rebuild fixed-journal objects and their hash input. These materialize after the worker cleanup operation, whereas the prior rows already show the spike. Whole archives allow 16 MiB and 512 files, so a large read/string can exceed 8 MiB, but terminal archive decoding runs in the parent after child exit and cannot explain the child's reopen peak. Hex doubles byte lengths in raw share traces; those are drained per exchange, not a concatenated telemetry history. |
| 6 | Product payload fitting/encoding, `src/telemetry/payload.js:89–112,163–183`, `src/telemetry/otlp.js:204–211`. | One job per client, raw body at most 1,048,576 B in the product and 4,096 B for this fixture subscription. This path allocates point/omission arrays and codec output, but has no whole-history serializer or identified single 8 MiB Buffer. Pending native work was zero at the supplied peak rows; this does not exclude native work between polls. |

For the two enabled artifacts, four sink Buffers would total **8,166,436 B** and
**8,221,956 B**. Assuming a 16 B sequential-string header, one-byte text excluding
the final newline and 8 B alignment, four whole-file strings would occupy
`4 * roundUp8(fileBytes - 1 + 16)` = **8,166,496 B** and **8,222,016 B**. These
match the respective recorded new-large-object used bytes exactly. The remaining
roughly 15.5 MB of new-space objects is consistent with repeated parsing of all
sink rows. This is a strong source/size inference, not a measured allocation stack
or an exact attribution of RSS. CI profiles must confirm the stacks and assess
the change; no local workload execution is authorized.

## Incremental, lossless sink parsing

`TelemetryExportReader.#readRows` uses an 8 KiB scratch Buffer and reads at most
4 KiB per call. It decodes UTF-8 only at complete newline boundaries. It still
reads the original stat-sized snapshot, parses every complete row, sorts by
sequence, validates all rows before returning selected evidence, refuses short
reads, overlong records and overflow markers, and retries an incomplete tail on
the next call. It does not cache, rewrite or discard source evidence. Per-row
JSON strings are at most 4 KiB; the 2 MiB whole-file Buffer/string allocations
are removed. Parsed row objects still exist and are intentionally unchanged.

The regression spec uses a greater-than-1-MiB sink with out-of-order rows,
multibyte UTF-8 across read boundaries, another UUID, a cursor and an incomplete
tail. It compares every returned field and unchanged file bytes/digest, completes
the tail and verifies the next row, and enforces bounded reads. Existing full
archive, tamper, sequence, cap and observer specs remain selected by CI. Archive
packing and its digests are unchanged.

## CI allocation artifacts

The existing `share-peak` job uploads the diagnostic output directory, including
its new `allocations/` subdirectory. The enabled share worker alone starts an
inspector sampling session around each reopen (including completion of the
previous segment's background seal) and around terminal cleanup. Normal workers,
other kinds and the disabled diagnostic arm never start the profiler. Reopen
acknowledgements can be delayed by joining the seal; these profiled peaks include
extra diagnostic overhead and must not replace the original acceptance arms.

Sampling uses a fixed **65,536 B interval** and includes objects collected by both
minor and major GC. These options are documented in the [Node 22 V8 inspector
protocol](https://raw.githubusercontent.com/nodejs/node/v22.x/deps/v8/include/js_protocol.pdl)
(checked 2026-11-12). No GC is requested by this diagnostic.

Each stage exports:

- `<index>-<stage>-<tick|none>.heapprofile`: the complete V8 tree and samples,
  including collected allocations; never just the top stacks.
- The matching `.json`: stage/tick, monotonic sampling boundaries, work outcome,
  Node/V8 versions, settings, profile byte length and SHA-256, total estimated
  self bytes and the top 25 allocation nodes with full stacks and one-based
  file/line/column coordinates. Self sizes exclude children; totals do not add
  inclusive subtree sizes.
- `shadow.sharePeak.allocationProfiles` in the original result and
  `share-peak.json`: bounded inventory and diagnostic completeness. The driver
  requires one successful profile per executed reopen plus one cleanup profile.

Limits are 17 stages, 4 MiB per raw profile and 1 MiB per summary: at most 34 files
and 85 MiB. Overflow fails the diagnostic and is reported in the result/failure
inventory; profiles are never truncated. These artifacts live outside the
original 16 MiB/512-file evidence archive. Sampling, extraction, ranking and disk
export add observer overhead. Only work is bracketed; ranking/export happen after
sampling stops. No thresholds, acceptance verdict dependencies, raw memory
observations, high-water decreases or original evidence files are altered.

CI should compare the profiler-only revision with the incremental-reader
revision, inspecting `readExports` / `#readRows`, decode and sealing stacks.
Sampling self sizes estimate JavaScript allocations; they do not directly
partition external backing bytes or RSS. Product dominance, if observed, needs a
separate product investigation rather than a change to this harness patch.
