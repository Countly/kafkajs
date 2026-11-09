# Retained diagnostic lifecycle ownership

Evidence: CI run 42606255014, telemetry / retained-diagnostic, producing tree
`6b47c39fbe7820352022c37e7179c55052413dd0`, Node `v24.22.2`, V8
`13.6.233.17-node.63`, flags `--expose-gc --no-lazy --no-lazy-feedback-allocation`.
Independent source-digest and snapshot-hash replay confirms all sixteen archived
children completed their declared diagnostic catalogue. This is diagnostic
attribution, never an acceptance receipt. Original numerical
failures remain failed; no limit, baseline, schedule, codec or GC policy changes.

## Finding and classification

The material repeated public-arm data growth belongs to the **diagnostic harness
(class a)**. `WeakOwners.issue()` appended an object `{id, kind, weak}` and a new
`WeakRef` for every issued target, including every metric point. `read()` kept
these rows and wrappers even after the targets died. Between public producer
closed-0 and closed-5, issued targets increased from 3500 to 16985; 15655 of the
final IDs describe points. The observer's growing history survives all-released,
while the observed product collectors and builders do not.

The 131072-target limit was an overflow bound, not preallocated storage: the old
constructor used `rows = []`. The 32-MiB `DiagnosticLog.buffer` was allocated
before C0 and retains the same store identity at the later checkpoints; it does
not explain repeated growth. The observer's per-target allocation was missing
from the old ownership categories and appeared as unresolved instance data.

The scalar public retained series increases in every block. The assertion that
all capture series do likewise is false: capture admin and share-consumer have
large negative code/metadata steps. Both variants create observer history, but
snapshot effects make their byte curves different. Neither variant is a control
to subtract from the other.

## Per-block owner table

Each cell is **cumulative issued IDs / newly allocated row-and-WeakRef self bytes**
for the capture variant. Snapshots show 48 B per row plus 32 B per WeakRef. Bytes
are `80 * new IDs`, excluding element-array capacity and kind strings. Blocks
1–4 have no snapshots by design: their counts are exact journal facts; their
byte columns apply the layout observed at the two captured endpoints, rather
than claiming an intermediate heap census. Block 0 includes prior issuance.

| Block | producer | consumer | admin | shareConsumer |
| --- | --- | --- | --- | --- |
| 0 | 3500 / 280000 | 3543 / 283440 | 2953 / 236240 | 3264 / 261120 |
| 1 | 6446 / 235680 | 6103 / 204800 | 5628 / 214000 | 5969 / 216400 |
| 2 | 9254 / 224640 | 8944 / 227280 | 8442 / 225120 | 8671 / 216160 |
| 3 | 12060 / 224480 | 11785 / 227280 | 10979 / 202960 | 12357 / 294880 |
| 4 | 14730 / 213600 | 14762 / 238160 | 13655 / 214080 | 15901 / 283520 |
| 5 | 16985 / 180400 | 17317 / 204400 | 16468 / 225040 | 18886 / 238800 |

For comparison, these are the untouched signed `closed.retained - M.retained`
readings in bytes. They are not snapshot self-byte totals.

Capture:

| Block | producer | consumer | admin | shareConsumer |
| --- | --- | --- | --- | --- |
| 0 | -337576 | 84912 | 1009504 | -996640 |
| 1 | 115936 | 516856 | -791816 | -2027304 |
| 2 | 485584 | 974360 | -414504 | -1640792 |
| 3 | 859760 | 1393592 | -95232 | -1154864 |
| 4 | 1151864 | 1744192 | -1339152 | -711664 |
| 5 | 1472480 | 2086944 | -978256 | -374016 |

Scalar:

| Block | producer | consumer | admin | shareConsumer |
| --- | --- | --- | --- | --- |
| 0 | 1274536 | 1493496 | 1080488 | 1155888 |
| 1 | 1682824 | 1981824 | 1528624 | 1713800 |
| 2 | 2089304 | 2435592 | 1913712 | 2222960 |
| 3 | 2446080 | 2956672 | 2224648 | 2606976 |
| 4 | 2730072 | 3402704 | 2545336 | 3005768 |
| 5 | 3100728 | 3669496 | 2907032 | 3349792 |

## Additive stable-ID accounting, closed-0 to closed-5

Self bytes are assigned once per node. Additions, removals, surviving-size
changes and category transfers reconcile separately before taking a delta.
Dominated subtree totals are not added together. Engine metadata here includes
code, feedback, shapes, weak lists and compiler/literal metadata; it never
exempts arbitrary instance descendants. “Other” contains the remaining named
core/operation/buffer/weak-table categories, not a balancing unknown bucket.
`WeakOwners.ids` contributes a 16384-B identity-table capacity increase in public
consumer, admin and share-consumer (zero in producer), included in Other; it is
harness capacity, not compiled code.

| Arm/kind | Weak history | Other journals | Engine metadata | Unresolved | Other | Total self delta |
| --- | --- | --- | --- | --- | --- | --- |
| public/producer | 1234896 | 153496 | 417312 | -1544 | 4776 | 1808936 |
| public/consumer | 1258064 | 152328 | 569600 | -1248 | 22264 | 2001008 |
| public/admin | 1237232 | 146624 | -3390000 | -3416 | 20744 | -1988816 |
| public/shareConsumer | 1406816 | 174152 | -981080 | 1736 | 19976 | 621600 |
| collector/producer | 182880 | 36544 | -1890824 | -3632 | 10296 | -1664736 |
| collector/consumer | 182880 | 36576 | 185632 | 184 | 10616 | 415888 |
| collector/admin | 182880 | 36576 | -2086536 | -3400 | 10296 | -1860184 |
| collector/shareConsumer | 182800 | 36512 | 184648 | 184 | 10616 | 414760 |

Public weak history at the endpoints is 317696→1552592 B (producer),
321136→1579200 B (consumer), 273712→1510944 B (admin), and
298704→1705520 B (share-consumer). Producer history alone reconciles as
1271312 B added − 36416 B removed + 0 resized + 0 transferred = 1234896 B.
Its row/WeakRef increase is 1078800 B, array-capacity increase 145200 B,
and kind-string increase 10896 B. These are disjoint self-byte counts.

After attributing history, public unresolved **additions** are respectively
19800, 19672, 22032 and 21296 B; removals are 21344, 20920, 25648 and 19616 B.
Admin has +200 B of category transfers; share has +56 B. The small net changes
therefore do not hide a material new unresolved graph between these endpoints.
However the absolute unresolved populations at closed-5 remain about 4.05 MB
per public capture. They predate closed-0 and may still contain cold first-use
retention; this analysis does not certify all cold ownership or product correctness.

## Named paths, identities and lifetime

The full paths have no depth cap. The producer observer path is
`Stack roots → Context@7237.weak → WeakOwners@135123.rows → Array@286077`.
Array@286077 retains the row objects, their `weak` wrappers and `kind` strings.
The weak edges do not retain their observed metric points. At all-released the
array still accounts for 1552872 B. Source owners are
`retained/weakOwners.js` (`issue`, `read`) and the `snapshot`/`buildPayload`
wrappers installed by `retainedDiagnosticChild.js`.

At each public closed endpoint there is one `TelemetryMetrics`, one
`TelemetryPayload`, two Brokers and four Connections. After all-released all of
these product instances are absent. An empty `MemoryForeground` shell can still
be present in the cleanup continuation; its `client`, `wire` and `harness`
fields have been cleared. At M and all closed ends, the client remains the same
returned object; all 128 descriptors, their charge/hash and UUID fingerprints
match the original registry in every block. The final block's weak results have
one latest `closing` episode/close promise and one last `running` capture with
its cancel/handle/subscription, rather than a growing live episode history.
All are gone after all-released. These source-owned final-close continuations
are distinct from the many dead observation wrappers that the harness kept.

The backing-store ledger distinguishes old/new identities and aliases:

| Public kind | Closed-0 frame store (65536 B) | Closed-5 frame store (65536 B) | Node allocPool closed-0 → closed-5 | After all-released |
| --- | --- | --- | --- | --- |
| producer | 449637 | 449637 | 444161 → 528713 | frame absent; pool 528713 |
| consumer | 461963 | 515295 | 462939 → 515183 | frame absent; pool 515183 |
| admin | 441093 | 477055 | 440823 → 513545 | frame absent; pool 513545 |
| shareConsumer | 448569 | 487617 | 455765 → 471485 | frame absent; pool 471485 |

A concrete producer frame path is
`MemoryForeground.client → clientInstanceId.context → TelemetryFactory.this →
ClientTelemetry.machine → closing.handle → isConnected.context →
ConnectionPool.pool → pool[0] → Connection.frames → FrameBuffer.prefix →
Buffer.buffer → ArrayBuffer.backing_store`. The frame aliases and Node pool
aliases can reuse the same native store identity over time. For producer,
store 449637 survives but its owning ArrayBuffer changes 448745→498605;
old allocPool store 444161 disappears, replaced by store 528713. The surviving
65536-B pool is not a growing list of retired codec outputs. No 128-KiB native
store appears in these public closed snapshots. Node's `bufferSep` and TLS
session-prefix stores are separate stable 65536-B owners. Native allocator
scratch outside the graph is not certified by wrapper death.

Semantic ephemeron analysis requires both the table and key to be live; ordinary
strong-edge dominators alone are not a release proof. Existing capture analysis
contains the dependency-removal checks. Public conditional-only values number
4/5/4/4 at each closed endpoint, falling to one runtime value after all-released.
Their IDs and uncapped retaining paths remain in the ownership evidence.
The material-reference audit for new/enlarged feedback and named closures has
no qualifying >=16384-B owner in this comparison; generic data remains in the
explicit unresolved ledger above, rather than being relabelled as code.

## Correction and next diagnostic

`WeakOwners` now reserves all scalar row slots before C0, issues into those
slots, and clears a dead WeakRef at the existing read boundary while retaining
its ID, kind and dead result. Live references continue to be checked at later
reads. This does not drop observations, add a checkpoint or change which
production targets are observed. Pending weak wrappers can still allocate
within one block; their observed live population and the WeakMap's capacity
remain diagnostic effects. It is not a new acceptance warm-up or subtraction.

The ownership analyser labels only the actual observer array/elements, rows,
WeakRefs and dominated kind strings. An extra strong row property holding a
product object remains unresolved. `analyse.js` records the observer's lifetime
expectation explicitly.

The existing broker-free specs have failing-first controls for preallocation,
capacity/duplicate IDs, archiving dead wrappers while a held target stays live,
and separating history from weak targets and accidental strong descendants.
The unchanged one-time 1-MiB held-store/released controls cover all four public
kinds and the collector. There is no demonstrated product accumulation here
requiring a speculative `src/` fix.

The next Node-24 diagnostic must keep all sixteen children, the seven captures,
all six original-M ends, actual codec/foreground work, natural cleanup, source
hashes and the existing budgets. It must show the reserved scalar slots already
at C0, dead wrappers no longer accumulating across blocks, all issued IDs and
alive/dead outcomes preserved, stable descriptor/UUID populations, and separate
accounting for remaining marks/native/window/operation journals and engine
changes. Any remaining material transient graph needs its own owner path.
No retained pass, exact plateau or cold ownership completion is predicted by
these local unit controls. A19/A32 and the prior acceptance failures remain open.
