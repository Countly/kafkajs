# Runbook: the Apache dumps and the core checker

Applies to a smoke, rehearsal or acceptance run of `scripts/soak/finalSoakNode.sh`. The canonical dump procedure is the driver's own
`scripts/soak/finalSoakDump.js`, which `finalSoakNode.sh` runs; this page says what it must leave in the artifact, how to
reproduce one dump by hand if ever needed, and how to run and read the checker.

## 0. One checkout for driver and checker

`finalSoakNode.sh` runs the checker itself (stage `core`, section 4a) from the run's own checkout, so the matrices always
match there. A replay by hand must do the same:

The checker compares the archived matrix (`<artifact>/archive/scripts/soak/finalSoakMatrix.json`) byte for byte with the
matrix of the checkout it runs from. Run it from the same commit the driver ran:

Set `out` and `checkout` to absolute paths first (the `node -p 'require(...)'` lines below need an absolute file path):

```bash
out=$(realpath /path/to/artifact)                  # the artifact directory
checkout=$(realpath /path/to/checkout)              # the run's checkout
tree=$(node -p 'require(process.argv[1]).treeSha' "$out/header.json")
git -C "$checkout" rev-parse HEAD            # must print $tree
cd "$checkout" && node scripts/evidence/finalSoakCore.js "$out"
```

A checkout with a different matrix fails K7 with "the archived matrix is missing or differs from the matrix of the checkout this checker
runs from (<path>)", and every other row is INCONCLUSIVE. The checker reads only the artifact, so it can run after the
cluster is gone.

## 1. Order in `finalSoakNode.sh`

driver exits → heartbeat reaped → `finalSoak.js --outer-exit` (writes `outer-exit.json`) → `finalSoakDump.js` (the dumps)
→ `finalSoak.js --finalize` (the header binds every file) → summary → cleanup (deletes the run's topics and groups and the
containers). The dumps must exist before finalize and before cleanup. A failed dump does not stop finalize; the checker
turns it into an INCONCLUSIVE row.

## 2. Reproducing one dump by hand

Only while the run's Compose cluster is still up (before cleanup), from the repository root of the run's checkout, with the
run's Compose environment (`finalSoakNode.sh` exports both):

```bash
set +e                                              # a failed tool must still leave its end marker
out=$(realpath /path/to/artifact)                  # absolute: node's require needs it
export COMPOSE_PROJECT_NAME=kafkajs
export COMPOSE_FILE="docker-compose.telemetry.yml:$out/broker/compose.json"
bootstrap=kafka1:29092,kafka2:29095,kafka3:29098
mkdir -p "$out/apache/groups"

# Topic dump: role is main or transactions; topic is config.json topics.<role>
role=main
topic=$(node -p 'require(process.argv[1]).topics[process.argv[2]]' "$out/config.json" "$role")
docker compose exec -T kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server "$bootstrap" --topic "$topic" --partition 0 --offset earliest \
  --isolation-level read_committed --timeout-ms 60000 \
  --formatter-property print.partition=true --formatter-property print.offset=true \
  --formatter-property print.key=true \
  > "$out/apache/$role.dump" 2> "$out/apache/$role.dump.stderr"
printf 'exit=%d lines=%d\n' "$?" "$(wc -l < "$out/apache/$role.dump")" > "$out/apache/$role.dump.end"

# Group describe: group is one ordinary consumer group id from config.json clients[].group
group=$(node -p 'require(process.argv[1]).clients.find((c) => c.slot === "c01").group' "$out/config.json")
docker compose exec -T kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server "$bootstrap" --describe --group "$group" \
  > "$out/apache/groups/$group.describe" 2> "$out/apache/groups/$group.describe.stderr"
printf 'exit=%d lines=%d\n' "$?" "$(wc -l < "$out/apache/groups/$group.describe")" > "$out/apache/groups/$group.describe.end"
```

The shell form records the tool's own status; `finalSoakDump.js` records `exit=-1` when the tool could not start or was
killed. A file written by hand after finalize is not bound by the header, and the checker refuses unbound files.

Facts about the tools (Apache Kafka 4.4.0):

- `--formatter-property`, not `--property`: the old option prints a deprecation warning into the dump's stdout.
- `--timeout-ms` ends the tool with exit status 0 after a `TimeoutException`; the status never proves the end of the log.
  The checker proves completeness from the describes' `LOG-END-OFFSET` and the journal.
- `kafka-consumer-groups.sh` does not describe share groups; the driver writes `groups/<share group>.unavailable` instead.

## 1a. The `core` stage of `finalSoakNode.sh`

After sealing and after the workload check (`finalSoak.js --check-workload`), the runbook runs
`node scripts/evidence/finalSoakCore.js "$out" > "$out.core.txt"`, prints the report, and writes
`core checker verdict: <verdict> (exit status <n>), report <file>` to the log (and, when `GITHUB_STEP_SUMMARY` is set, the
note and the whole report to the job summary). The report sits beside `<artifact>.runbook.json`, outside the directory the
header binds, so it never changes the sealed artifact; the workflow uploads it with the rest of the artifact root.

| Mode | Verdict and checker exit status | Stage `core` |
|---|---|---|
| smoke | `NOT ACCEPTED`, 1 | passes; the log and job summary say "A smoke is never ACCEPTED; NOT ACCEPTED with no FAIL row is its expected verdict." |
| rehearsal | `NOT ACCEPTED`, 1 | passes; the log says "A rehearsal is never ACCEPTED; …" as for a smoke |
| acceptance | `ACCEPTED`, 0 | passes |
| any | `REJECTED`, no `CORE VERDICT:` line (the checker crashed), any other pairing, or acceptance `NOT ACCEPTED` | fails: receipt `failedStage: "core"`, exit status 1 |

Both places that run the checker use one shell function, `run_core` (write `<artifact>.core.txt`, print it, keep the exit
status). A failed workload check never reaches stage `core`; instead `finish()` calls `run_core` for diagnosis after cleanup,
when the checkout has the checker, and logs `core checker report for diagnosis only (the run failed at stage workload): <file>`.
That verdict never changes the receipt: `failedStage` stays `workload`, exit status 1.

## 3. Files the checker reads

| File | Read for | Missing, unbound or malformed |
|---|---|---|
| `header.json`, `config.json`, archived matrix | structure, run identity, manifest | K7 FAIL, others INCONCLUSIVE; JSON side files are read only up to 16 MiB |
| `stream.ndjson` | every journal fact, streamed once | K7 FAIL |
| `supervisor.json` | the run's own verdict | INCONCLUSIVE (missing) or K7 FAIL (bound, reports a problem) |
| `outer-exit.json` | must agree with `header.exit` (status = code, same `observedAt`) | K7 FAIL |
| `apache/{main,transactions}.dump` + `.stderr` + `.end` | K1, K2 (stderr must say `Processed a total of <lines> messages` once) | K1, K2 INCONCLUSIVE |
| `apache/groups/<group>.describe` + `.end` (ordinary groups only) | K4, and the log ends K1 uses | K4 (and K1) INCONCLUSIVE |
| `heap/summary.json` (or `heap/summary-error.txt` when finalize could not write it), every `heap/<slot>.<boundary>.heapsnapshot` | K5 (section 6a) | K5 INCONCLUSIVE; the summary is read only up to 16 MiB, a snapshot up to 1 GiB |

The checker does not read `.cmd` files, the describes' `.stderr` or the share group's `.unavailable` file.

## 4. The checker

```bash
node scripts/evidence/finalSoakCore.js "$out"        # exit 0 only when the verdict is ACCEPTED
```

It streams `stream.ndjson` once (no scratch store), validates config, header, matrix and stream with the merged schema, reads
the dumps, and prints one block per row. Memory: about 86 bytes of typed arrays per offered-id slot (about 172 MB at the
2-million cap, `traffic.maxOffers`, touched only as ids arrive: the full payload digest and one ownership serial per judged
group) plus one id string and map entry per offered id. A synthetic replay of about one day of offers (60 copies of a smoke)
took about 15 minutes and stayed under 0.6 GiB RSS; plan for under 1 GiB RSS and tens of minutes.

Verdicts: `ACCEPTED` (all seven rows PASS in an acceptance run; a smoke or a rehearsal is diagnostic and never reaches it),
`REJECTED` (any FAIL), `NOT ACCEPTED` (otherwise). Exit status 0 for ACCEPTED, 1 otherwise, 2 for a usage error.

## 5. What each row proves

- **K1** every acknowledged id and every non-rejected member of a committed transaction is in the read-committed dump once, at
  its acknowledged offset when the offset is known; aborted members never; a second, contradictory send result or transaction
  record makes K1 INCONCLUSIVE. Every offset of each standing log below Apache's log end is explained by exactly one fact: a
  dumped record, an acknowledged member of an aborted transaction at its acknowledged offset, or a control marker. Two aborted
  members acknowledged at one offset, one acknowledged at a dumped offset, or one at or beyond the log end leave K1
  INCONCLUSIVE. The main dump is complete when it holds exactly the log end's count. The console consumer reads in order and
  can only be cut off, so the transactions dump is complete up to its last record; the offsets after it are matched one to
  one: each acknowledged aborted member placed there takes its own offset, and each transaction whose marker must follow the
  last dumped record (that record's own transaction, and each transaction with a member placed there) takes one free offset
  after its last record. They must fill those offsets exactly; anything else there (a retried marker, a visible record the
  dump was cut off before) leaves K1 INCONCLUSIVE. The driver's final drain ends the transactions log with one
  nontransactional closing record (the `offered` record on the transactions topic whose `transaction` is null), produced
  once no transaction is open, so a complete dump ends at the log end minus one and nothing follows it. A retried marker
  before the last dumped record is harmless.
- **K2** every handler entry matches its record (topic, partition, full payload digest, the record's log offset); every
  visible record entered a handler in every recipient group; no judged group entered a record a read-committed reader cannot
  see, or a record of a topic it does not read.
- **K3** ownership interval = (group, client run, partition) from an applied `acquire` with a new acquisition to `revoke-end`
  or `lost`. An entry outside the open interval of its run, the same id entered twice by one interval (whatever the order of
  offsets), and an entry or boundary attributed to another client, run or process FAIL. Repeats across intervals or runs are
  counted only. Entries that wait for their id's `offered` record are judged together when it arrives, with every interval
  among them, even ones that have closed since.
- **K4** each ordinary group equals `CURRENT-OFFSET` to `LOG-END-OFFSET` on partition 0 of every standing topic it reads:
  the main topic for every subject and the seed's control group, the transactions topic for the subject groups the matrix
  topology names as its recipients (the share group commits nothing and is not judged); a member row without an
  assignment is skipped; a LAG that disagrees, a repeated partition row or a non-Kafka header is INCONCLUSIVE.
- **K5** endpoint and least-squares slope of the **judged heap** of each subject process (p01 to p12): every live heap byte
  of the heap snapshot it wrote after each hourly quiet boundary, except V8 code (section 6a), whoever owns it. A boundary
  counts only when its snapshot was written without error inside the hour's quiet window and its summary record agrees with
  the supervisor's receipt (the process's own file of that boundary, the size the worker recorded, the same start time);
  the checker then re-derives the totals of every counted snapshot from the file itself, so no judged number rests on the
  summary alone. Any boundary that does not count, or a file that does not confirm its record, leaves K5 INCONCLUSIVE, never
  PASS. The seed's snapshots are the positive control: unless its judged endpoint exceeds its breach bound (2,097,152 bytes)
  K5 is INCONCLUSIVE; the seed never fails K5. The whole-process `heapUsed` of the quiet triplets is printed as information
  only (section 5b).
- **K6** final resource counts at or below baseline; the baseline must be the process's first sample and the final its last,
  taken after the end of traffic: the supervisor's own `traffic` shape end (written after the final drain), which must lie
  between the run's end and the supervisor's observed end. The supervisor is the config's one `collector` process: the
  header's `exit.supervisorProcess` must name it, and the stream's only `process` exit from source `outer` (the record the
  outer handover appends from `outer-exit.json`) must be its exit. Any disagreement leaves K6 INCONCLUSIVE and fails K7, so
  a header cannot relabel another process as the supervisor.
- **K7** exits, run length, every periodic slot admitted inside `[slot − jitter, slot + jitter]`, clean supervisor receipt,
  outer exit receipt, an attributable supervisor (as in K6), valid and digest-matching stream.

## 5a. Stream order

Processes write the journal concurrently; the stream keeps each source's own order (the schema checks `seq` and `at` per
source) but not the order between sources. The checker therefore never relies on the order between processes: a send result
or handler entry whose `offered` record comes later is held (at most the shared capacity, `traffic.maxOffers`; beyond it K1 to
K6 are INCONCLUSIVE with "more than N records wait for an id not yet offered") and judged when the offer arrives. An entry's
ownership is judged on arrival, against its own process's boundaries. At the end of the stream a held entry fails K2 ("handler
entries name an id never offered") and a held send result is a journal problem. A transaction decision may precede its members;
the end of traffic is the latest such record from the supervisor.

## 5b. Information printed but not judged

Under "NOT JUDGED information": each broker `down` and `ready` instant (two per scheduled outage of a day, 24, then a count
of any more) with the configured down time and the container's stop
or start instant (`downMs`, `processAt`), the wave transfer, return and not-observed counts from the `wave` records with, per event, the paused keys and the keys the
member held from their `wave-key` records, every wave whose counted `wave-key` records did not all arrive (`incomplete wave:
<record id> has <n> of <m> wave-key records`; the stream validator also fails such a stream, so the run is REJECTED by K7), the probe sends that settled after
their step (`late` diagnostics), the drain commits skipped as expected (`expected skip:` diagnostics) and, when the stream has
`share-outcome` records, `unresolved share acknowledgements: <n> (<reason> <count>, …) covering <k> distinct offsets` (the
outcomes whose reason is not null; a null reason is a confirmed accept), and the heap lines: `whole-process heapUsed <slot>`
(the endpoint and slope of the median quiet-triplet `heapUsed` of each subject, the measured client and the driver's worker
together, compilation cache and code included) and `judged heap of harness-only <slot>` (the reader p14 and the decoder p16,
the same rule as K5 on processes without the measured client). None of them changes a verdict. An acknowledged send with `offset: null` must be in the dump exactly once, at any offset.

## 6. After a smoke

Read `lateSteps` (K7) and every INCONCLUSIVE reason. A late supervisor step is information only: `lateSteps` never changes a
row or the verdict, as with the items under NOT JUDGED. Only a late sample changes a row (a periodic slot outside its jitter
fails K7). Do not start the day until a two-outage smoke with one committed and one aborted transaction replays from its own
checkout with K1 to K4, K6 and K7 PASS and K5 INCONCLUSIVE only for its single boundary ("has 1 of 1 usable heap snapshots").

## 6a. The judged heap and the triage of a K5 FAIL

**What K5 judges.** Right after each quiet boundary's last reading, in every mode (acceptance included), every subject
(p01 to p12), the seed (p13), the reader (p14) and the decoder (p16) write `heap/<slot>.<boundary>.heapsnapshot`, all at
once, while the scheduler holds work; each write has 60 s of real time. The supervisor (p15) writes the stream and takes
none. The seed's retained offset map (a `Map` of `<partition>:<offset>` strings to offset strings, started after `h00` and
cleared at the end) is judged bytes like any other, so the seed's judged endpoint must exceed its breach bound in every run
that judges K5: that shows, inside the run, that capture, summary and checker see a leak. V8 runs a full collection before it writes, so a snapshot holds only live nodes. The judged
bytes of a snapshot are the self bytes of every node except those of the excluded node type below; K5 applies the matrix
endpoint (2,097,152 bytes, median of the last three boundaries minus the first three) and slope (262,144 B/min over the
snapshot start times) bounds to them, in every mode that judges the row.

Excluded node types: `code`.

V8 gives the `code` type to compiled code and its bookkeeping and to nothing a program allocates. Its name categories, with
what a rehearsal (run 3afda68743d9, consumer p03 at `h02`, 10,220,632 excluded and 13,816,934 judged bytes) held in each:

| Category | p03 bytes | What it is |
|---|---|---|
| `(instruction stream)` | 1,923,904 | optimised machine code of a function (`(instruction stream for <fn>)`) |
| `system / TrustedByteArray` | 1,402,120 | relocation info, deoptimisation data and source positions of compiled code |
| `(unnamed)` | 1,238,576 | unnamed instruction streams and the script records of unnamed scripts |
| `system / BytecodeArray` | 860,112 | bytecode |
| `system / ScopeInfo` | 632,736 | scope infos of compiled functions |
| `(function)` | 615,680 | shared function infos, named by their function |
| `system / FeedbackVector` | 549,400 | feedback vectors, allocated after a function's eighth call |
| `(constant pool)` | 352,344 | bytecode constant pools |
| `system / ProtectedFixedArray` | 338,200 | deoptimisation literals of optimised code |
| `(BASELINE instruction stream)` | 259,904 | baseline (Sparkplug) machine code |
| `(builtin code)` | 231,840 | code objects of V8 builtins (`(<name> builtin code)`) |
| `(code)` | 228,672 | code objects of compiled functions (`(code)`, `(code for <fn>)`) |
| `system / FeedbackCell` | 210,984 | feedback cells of closures |
| `system / WeakFixedArray` | 160,384 | weak lists owned by code |
| `system / FeedbackMetadata` | 152,896 | feedback layouts of functions |
| `system / LoadHandler` | 123,424 | inline-cache load handlers |
| `(context local names)` | 118,056 | scope-info name tables |
| `(shared function info)` | 113,328 | shared function infos without a function name |
| `system / UncompiledDataWithoutPreparseData` | 110,280 | functions loaded but never compiled |
| `system / ObjectBoilerplateDescription` | 107,136 | object-literal templates of compiled code |
| `system / CodeWrapper` | 83,648 | sandbox wrappers of code objects |
| `(script)` | 80,784 | script records of loaded modules, named by their path (the compilation cache) |
| `system / BytecodeWrapper` | 71,248 | sandbox wrappers of bytecode |
| `system / TrustedWeakFixedArray` | 56,544 | weak lists of optimised code |
| `(BASELINE code)` | 41,376 | baseline code objects |
| `system / AllocationSite` | 40,200 | allocation-site feedback of literals |
| `(host-defined options)` | 30,448 | module host options of compiled scripts |
| `(script line ends)` | 24,224 | line tables of a script, computed once for a stack trace |
| `system / WeakArrayList` | 20,080 | dependent-code lists |
| `system / ClosureFeedbackCellArray` | 18,064 | feedback cells of the closures a function creates |
| `(constant elements)` | 9,552 | array-literal elements of compiled code |
| `system / ArrayBoilerplateDescription` | 7,560 | array-literal templates |
| `system / PreparseData` | 2,208 | preparse results of lazy functions |
| `system / StoreHandler` | 2,176 | inline-cache store handlers |
| `system / UncompiledDataWithPreparseData` | 1,440 | lazy functions with preparse results |
| `system / IrRegExpData` | 720 | compiled regular expressions |
| `system / RegExpBoilerplateDescription` | 256 | regular-expression literals of compiled code |
| `(object elements)` | 80 | elements of a code-owned array |
| `system / AtomRegExpData` | 48 | compiled literal regular expressions |

Why code is excluded: over a rehearsal hour 88 to 98 % of the growth was in these rows, for functions that become hot for
the first time on rarely run paths (outage, recreation, regex churn); none was ever freed and the count of functions that can
still tier up gives no ceiling, so on the day it would fail the bound without any product leak (run 3afda68743d9: consumers
grew 0.27 to 0.39 MB an hour in `heapUsed`, judged bytes 27 to 30 KB an hour). The product compiles no code at run time
(no `eval`, `new Function` or `vm` under `src/`), so a leak of the client cannot hide in it. Everything else stays judged:
maps and hidden classes (`object shape`), `hidden` system objects even when their name looks like code (a `hidden`
`system / WeakArrayList` or `system / ProtectedFixedArray`), strings, `native` nodes, and any node type a future V8 adds.
There is no unattributed escape: a byte counts whether the summary attributes it to the product, the harness, node
internals, a dependency or nothing. The backing stores of `Buffer` and `ArrayBuffer` are `native` nodes, so a leak of them
is judged in full although `heapUsed` misses it (it counts them as external memory).

After its first snapshot a process keeps V8's object-move tracking on for the rest of the run. That costs native memory and
some collector time, never judged bytes, and every measured process now pays it in every mode.

The compilation cache that a process's first snapshot frees (about 0.9 to 1.2 MB, part of it old-space and script-source
bytes) does not reach the judged bytes: every snapshot, the first included, is written after V8's own full collection, so
all readings stand on the same footing. In run 3afda68743d9 the judged growth from `h00` to `h01` and from `h01` to `h02` was
+30,456 and +26,712 B (p03), +9,952 and +11,352 B (p09), +20,768 and +21,864 B (p14) and +2,168 and +3,040 B (p16), while
`heapUsed` read before the first snapshot dropped by 0.9 to 1.2 MB after it.

**What is sealed.** After the run, `--finalize` (outside every measured process, before anything is hashed) reads each
receipt's snapshot and writes one record per receipt to `heap/summary.json`: the receipt (`at`, `ms`, `error`), the file's
size and sha256, its node count, total, judged and excluded bytes, the excluded bytes by the categories above, the judged
bytes by node type and by owner class, and the ten constructors and closures that grew most since the previous usable
snapshot, each with the retainer path of one new instance. A receipt that names another file than its own process's of
that boundary, or a size other than the file's, gets a record with that error and no totals. Every snapshot file stays in
the artifact. When the summary cannot be written, finalize records why in `heap/summary-error.txt`, still seals the run, and
K5 is INCONCLUSIVE with that reason.

Sizes for a day (estimates from rehearsal 3afda68743d9, not yet measured on a day): 14 processes × 24 snapshots of 15 to
25 MB, plus the seed's 24 growing with its map to about 200 MB, about 7.5 to 11 GB in all, which the run's disk monitor keeps
free (16 GiB reserved); a rehearsal writes 15 × 3 snapshots, about 0.7 to 1 GB. Summarising takes about one second and
140 MB to 400 MB of RSS per 15 MB snapshot (about ten minutes for a day, the seed's last snapshots up to about 1 GB of RSS,
inside the finalize limit). The checker reads every counted snapshot, one at a time (subjects, seed, reader and decoder,
360 on a day, about the same time again, inside its own limit), and re-derives node count, total, judged and excluded
bytes, size and sha256 from the file itself; any disagreement leaves that process without numbers and K5 INCONCLUSIVE. The
summary's other fields (`growing`, owner classes) are triage aids and judge nothing.

**Triage of a FAIL.** A FAIL names a process whose live non-code heap grew past the bound; it is not yet a client leak.

1. Read `growing` in the failing process's summary records: the constructors and closures that grew, their owner class
   (product `src/`, harness `scripts/soak/` and `testHelpers/`, `node:` internals, dependency, unattributed) and the retainer
   path of a new instance.
2. Compare with the reader (p14) and the decoder (p16) under `judged heap of harness-only`: growth of the same shape there
   points at the harness.
3. Run `node scripts/soak/heapSnapshotReport.js <artifact dir> <slot>` on the snapshots for the full owner rows, the files that grew and twenty constructors with their retainer paths. It holds two
   snapshots at once, at about five times their file size each. A node is owned by the script of its constructor or
   closure, else by its dominator.

The verdict stays REJECTED either way; the triage decides what gets fixed.

## 6b. A rehearsal before the day

A smoke has one quiet boundary per measured process, so K5 is always INCONCLUSIVE there. A rehearsal
(`finalSoakNode.sh <sha> rehearsal`) runs the first three hours of the day plan unchanged: the same hourly executions,
outages on even hours, idle windows, quiet triplets and heap snapshots (`h00` to `h02`) and seed timing, for 10,800,000 ms
(`acceptance.modes.rehearsal`). The checker judges it with the day's rules: every periodic slot of its length, and K5 on the
judged heap of its three boundaries with the same endpoint bound (absolute bytes, not scaled to the length, so it fails above
about 1 MiB of sustained judged growth per hour instead of about 0.1 MiB on the day) and the same slope bound per minute. Its
verdict is at best NOT ACCEPTED. `FINAL_SOAK_DIAGNOSTIC=1` is accepted, as for a smoke.

On a hosted runner, dispatch the `Final soak smoke (diagnostic)` workflow with `mode: rehearsal`. A push to one of the
workflow's smoke branches always runs a smoke. The job takes about 3.5 hours (three hours of plan, about 15 minutes of start,
teardown, dumps, heap summary, check and archive, and the upload); its runbook step is limited to 17,400 s and the job to 335
minutes, inside the runner's six hours. Read K5 with section 6a before the day: forecast the day from the judged growth per
hour of each subject (about 21 hours separate the day's endpoint medians). Rehearsal 3afda68743d9 snapshotted only two
subjects (p03 and p09, forecast 0.60 and 0.22 MB for the day); a forecast for the other ten from it is an extrapolation
from their `heapUsed`, not a measurement, until a rehearsal of this tree snapshots every subject.

## 7. The load profile (separate from the day plan)

`scripts/soak/loadRun.js` measures the client at a target record rate with a loss and duplicate oracle that does not grow
with the record count. It shares no code path with the day plan: `finalSoakMatrix.json`, the scheduler and the checker above
are unchanged by it, and nothing here judges a final soak.

### 7.1 Running it

```bash
# 3-broker Compose cluster (docker-compose.yml) up, then:
node scripts/soak/loadRun.js --out .tmp/load-profile --rate 10000 --record-bytes 1024 --topics 12 --partitions 16 \
  --producers 3 --producer-kinds idempotent,plain,transactional --consumers 3 --duration 600 \
  --compression none --faults rolling-restarts --producer-processes 1
node scripts/soak/loadRun.js --recheck .tmp/load-profile   # replay the sealed artifact offline
```

`--producer-processes N` (at most `--producers`) runs the producers in N child processes (`loadProducerChild.js`),
producer `p<i>` in process `i % N`; consumers, the fault schedule and the judgement stay in the run's own process. A record
key names its producer, so each producer still writes its own sequence per partition from one process and the oracle is
unchanged. Each child offers its share on the same deadline schedule from the window's start, joins its producers and
sends back its send ledgers, counts, latency histogram and CPU; a run that is missing a child's ledgers is not judged
(exit 2). `slow-broker` needs one process: its flow verdict counts acknowledgements in the run's process. With more than
one process, `cluster.metadata` counts the consumers' Metadata requests only.

Every flag has a default (`LOAD_DEFAULTS` in `scripts/soak/loadProfile.js`). On a hosted runner, dispatch the
`Load profile (diagnostic)` workflow (`.github/workflows/load-profile.yml`): choice inputs for rate, record size, topics,
partitions, duration, codec, fault schedule, replication factor, producer processes and Node; a push to a branch named `p12/load-profile*` runs
it with the defaults.
The job is limited to 30 minutes; the 600 s default window leaves about 10 minutes of slack, 900 s leaves little.

### 7.2 The oracle

Each record's key is `<producer>.<incarnation>.<sequence>`: the sequence rises by one per record per partition per producer
incarnation (a recreated producer is a new incarnation). Producers record what they issued and which sequences were not
acknowledged; the one consumer group records the offset ranges each member was delivered. After the run every partition is
read once and judged (`scripts/soak/sequenceOracle.js`):

| Producer kind | An acknowledged record | A rejected send's record | Order (same incarnation, partition) |
|---|---|---|---|
| idempotent (default producer) | exactly once | absent or once | strictly increasing |
| transactional (read_committed) | exactly once if its transaction committed; never if it aborted | absent or once | strictly increasing |
| plain (`idempotent: false`, 5 in flight) | at least once (a retry after a lost answer repeats it) | absent or any number | may reorder with more than one request in flight; counted, not failed |

These follow `docs/Producing.md` (idempotence: every message exactly once per partition, in order; a rejected send has an
unknown outcome) and `docs/Transactions.md`. The rejected-send column is why the oracle never counts an unacknowledged
record as lost. Consumer rule: every record of the read below the group's committed offset was delivered to some member
(`skippedCommit`), and no committed offset is past the log end (`commitBeyondLog`); both come from Apache's
`kafka-consumer-groups.sh --describe`, not from the client.

Memory: one cell per producer incarnation and partition, plus one range per discontinuity (a rejected or aborted send on the
producer side; a re-delivery or a transaction marker on the consumer side). It holds no entry per record; the spec asserts
the structures are the same size at 120,000 and 600,000 records.

### 7.3 The independent read

Apache's `kafka-console-consumer.sh` in the kafka1 container, one JVM per topic, three topics at a time (the `many-topics`
schedule instead reads every topic in one JVM: see 7.5), `read_committed`,
from the beginning, with `print.partition`, `print.offset` and `print.key` on and `print.value` off. Kafka 4.4's
`DefaultMessageFormatter` ends the line after the key when the value is not printed, so each record costs about 30 bytes
of output however large it is. It shares no code with the client under test. The tool ends after 10 s without a record;
the read is complete when every partition reaches the last offset an acknowledgement returned, and each topic's line count
must equal the tool's own `Processed a total of` count. Estimate for a hosted runner (unverified until the first run): a
600 s window at 10,000 records/s is 6,000,000 lines, about 1 to 2 minutes of reading; the checker takes about 1.3 million
records a second (measured with plain Node on the development host), so the tool, not the checker, sets the pace.

### 7.4 The artifact and the verdict

`profile.json`, `producers.json`, `consumers.json`, `committed.json`, `faults.json`, `apache/<topic>.dump.gz` with
`.end`, `apache/<group>.describe`, `report.json`, and `header.json`, written last, which binds every other file by SHA-256
(the seal; `--recheck` refuses an artifact that differs from it). `report.json` holds the verdict, offered against sent
and acknowledged rates, latency percentiles, event-loop delay, heap and RSS samples every 2 s with each phase's peak and
end level (`memory`), the client measurements (`cluster`: topic creation time, each member's time from `subscribe` to its
first assignment, each rebalance with the worst event-loop lag around it, Metadata requests per minute and their response
sizes, fetch round durations), per-producer and per-member counts, the fault schedule's phases and outages, the flow
verdict of `slow-broker`, and the oracle's findings with examples.

Latency is measured from each record's due time on the offer schedule (record n of a producer is due n / rate after the
start), not from when the client accepted it, so a client that falls behind shows its backlog in the percentiles (no
coordinated omission). Transactional records are timed to their commit.

| Exit | Product | Profile | Meaning |
|---|---|---|---|
| 0 | PASS | REACHED | no oracle finding, the offered rate was met |
| 1 | FAIL | either | loss, duplicate or reordering beyond the kind's guarantee, an aborted record visible, a foreign or malformed key, a skipped commit, a commit past the log end |
| 2 | INCONCLUSIVE | either | the read was incomplete or failed, or the driver failed, with no product finding |
| 3 | PASS | RATE_NOT_REACHED | the runner did not send, or the client did not acknowledge, 95 % of the offered records: no claim about the product at that rate |
| 4 | PASS | either | `slow-broker` only, `flow` STALLED: a flow rule of 7.5 was broken (an inconclusive flow exits 2) |

A shortfall names whether records were not sent (the runner or the unsettled cap) or not acknowledged, and how many sends
the client rejected; rejections are the client refusing, so read them before blaming the runner.

**Why a rate was not reached.** `report.cpu` holds what the CPU did: `cpus` (cores the run may use), the 1-minute load
average (`loadAverage1m`, its maximum over the window's samples and its last), the run's own process per stage
(`stages.offer`, `.join`, `.drain`: CPU as a share of one core, and the machine's busy share of all its cores; producers and
consumers share the process, and the drain is the consumers' alone), the same per fault phase (`phases`), each producer
process's offer and join (`producerProcesses`, with `--producer-processes` above 1), and the broker containers
(`brokers`: `docker stats --no-stream` every 10 s while the window is open, mean and maximum percent of one core per
container; a failed call is counted). `rate.limitedBy` (null when the rate was reached; `rateLimitedBy` in
`loadProfile.js`):

| Value | Rule (offer window) | Read it as |
|---|---|---|
| `client-cpu` | a load process used 90 % of one core or more | the client's JavaScript runs on one thread: spread the producers (`--producer-processes`) or measure the client |
| `machine` | every core of the machine 90 % busy or more | brokers, clients and reads share the runner: the runner, not the client, set the rate |
| `unknown` | neither | look at the brokers' CPU, the unsettled cap, rejected sends and latency |

### 7.5 Fault schedules

`FAULT_SCHEDULES` in `loadProfile.js`. `none` is the plain profile. `rolling-restarts`: from a third of the window, every
broker stopped and restarted in turn, then every broker killed and restarted in turn, 20 s apart, through `BrokerBouncer`
(each step waits for the node to answer and the ISR to be full).

A schedule never runs past the offer window. When the window ends, a broker already down is restarted by its own step, no
later broker is taken down, and each fault not run is in `faults` with `skipped: "window ended"`; that is not an error and
does not make the run inconclusive. Only a fault mechanism's own failure is reported as `fault schedule: ...`. With
thousands of partitions each restart waits much longer for the full ISR, so a 600 s window may hold only part of the
`rolling-restarts` schedule: read `faults` to see which restarts ran.

**`slow-broker`** (needs `--duration` ≥ 195). In the middle third of the window, kafka2 (node 2) is:

| Phase | What | Length (600 s window) |
|---|---|---|
| slow | kafka2's answers to clients delayed 300 ms | (third − 45 s) / 2 = 77.5 s |
| frozen | `docker compose pause kafka2` (a `BrokerBouncer` pause step), then unpause; the step waits until kafka2 answers | 45 s, above the 30 s client request timeout |
| recovery | nothing; flow is judged | to two thirds of the window |

The delay is `tc netem` run from the host inside kafka2's network namespace (`sudo nsenter --target <pid> --net tc ...`), on
egress from the client port 9095 only: a hosted Ubuntu runner has passwordless `sudo`, `nsenter` and `tc`, so neither a new
image nor `NET_ADMIN` on the broker container is needed, and replication stays fast (with RF 3 on three brokers every
partition has a replica on kafka2, so delaying all its traffic would slow every partition through its follower and leave
no healthy partition to compare). The run applies and removes a 1 ms rule before it creates anything and stops with exit 2
and `slow-broker needs sudo, nsenter, tc and the netem qdisc ...` when that fails; it is never a silent no-op. The delay is
always removed when the phase ends, and the pause step always unpauses.

The schedule offers 10 ticks a second (one request per broker per producer every 100 ms, under the 5 in flight / 300 ms
that one connection to the delayed broker can complete) and sends each tick's records as one batch per partition
leader (taken before the window), so a slow partition's acknowledgement never holds back another leader's batch inside the
harness. The flow verdict judges the idempotent and plain producers' acknowledgements per original leader in one-second
buckets (a transaction waits for every partition it wrote, so the transactional producer is left out):

| Phase | Leaders judged | Rule | Why this number |
|---|---|---|---|
| slow | not node 2 | acknowledged ≥ 90 % of offered | their brokers are untouched, so only the client can slow them; 10 % covers the 100 ms tick and the cap's ramp |
| frozen | not node 2 | longest stall ≤ 20 s and acknowledged ≥ 50 % | the broker holds them about 11 s (kafka2 stays in their ISR until the controller fences it: `broker.session.timeout.ms` 9 s plus a 2 s heartbeat); a client waiting on kafka2's requests holds them a full 30 s request timeout; 20 s separates the two; 25 of 45 s flowing is above 50 % |
| recovery | every leader | acknowledged ≥ 90 % of offered | the client recovered without a restart |

A phase with fewer than 5 whole seconds, or a kafka2 that led nothing, makes the flow INCONCLUSIVE. `flow.table` gives each
phase's offered, acknowledged and delivered rate and longest stall per leader (`baseline` and `after` included, not
judged); `memory` gives each phase's heap and RSS peak and end level, to read the frozen phase's growth and the level after
recovery. kafka2's partitions fail over during the freeze and stay on the other brokers until a preferred-leader election
(`leader.imbalance.check.interval.seconds`, 300 s by default), so the recovery rule proves the client resumed those
partitions, not that it reconnected to kafka2.

**`many-topics`** (needs `--topics × --partitions` ≥ 1000; dispatch with topics 2000, partitions 1, rate 2000, duration
300). Topics are created 250 per request, each record carries a `topic` header, and Apache's console consumer reads every
topic in one JVM (`--include`, `print.headers`, 30 s idle end) into `apache/all.dump.gz`: one JVM per topic would cost
2,000 JVM starts and idle timeouts. One clean rolling restart from a third of the window (each step waits up to 5 minutes
for the full ISR). `cluster` reports the measurements; nothing in them is judged.

What a hosted runner (4 vCPU, 16 GB, three brokers with the image's 1 GB heap each) can hold, unverified until the first
run: 2,000 single-partition topics at RF 3 are 2,000 replicas on every broker, within Kafka's usual few thousand per broker;
every broker holds about 6,000 open segment files and 4,000 index maps, below Docker's default open-file limit and
`vm.max_map_count` 65,530. The first run reports the creation time and restart outages; if creation or a restart does not
finish, dispatch again with replication factor 1 (2,000 replicas in all), and record that the RF 3 claim is not proven.

### 7.6 Expected rates and the first CI run

Expected on a hosted 4-core runner with three brokers, RF 3, 1 KiB records and every client in one Node process: about
10,000 to 20,000 records/s sustained (an estimate from the 2,000/s producer-churn soak and the short benchmark rows, not a
measurement). The workflow offers 10,000/s by default; above about 20,000/s expect exit 3.

What the first run must show, and how to read it (the `Print the report` step, or `report.json` in the artifact):

- `verdict` `{ product: 'PASS', profile: 'REACHED', exitCode: 0 }`; any other value is the finding.
- `rate.offeredRate` 10000, `sentRate` and `ackedRate` at least 9500, `shortfall` null.
- every `oracle.findings.*.count` 0; `oracle.counts.records` about 6,000,000 minus the deliberately aborted tenth of the
  transactional producer's records; `oracle.size.gapRanges` small (one per rejected send).
- `oracle.reads`: one entry per topic with `exit` 0 and `lines` equal to `processed`; their `seconds` say whether the read
  fits the time limit at higher rates.
- `faults`: six outages, three `stop` then three `kill`, each with `outageMs`.
- `consumers[].crashes` and `producers[].errors`: expected few, all inside the restart window.
- `latencyMs.p99` and `eventLoopDelayMs.p99`: recorded, not judged.
- the artifact replays: `node scripts/soak/loadRun.js --recheck <artifact>` exits 0 with the same findings.

What the first `slow-broker` run (rate 10000, duration 600) must show:

- no `slow-broker needs ...` error: if it appears, the runner lacks the mechanism and nothing was run.
- `faults`: `slow` from about 200,000 ms to 277,500 ms with `delayMs` 300, `frozen` about 45,000 ms long with `outageMs`,
  `recovery` ending at about 400,000 ms.
- `cluster.leaders`: node 2 leads about a third of the partitions.
- `flow.verdict` KEPT_FLOWING and `verdict.exitCode` 0; STALLED names the phase and leader in `flow.reasons`: a stall of
  about 30 s on leader 1 or 3 in `frozen` is the client waiting on kafka2 (head-of-line), the finding this schedule exists for.
- `flow.table`: node 2's `ackedRate` falls in `slow` and is near 0 in the first part of `frozen`; nodes 1 and 3 stay near their
  `offeredRate` in `slow`.
- `memory`: the `frozen` peak and the `after` end level, with every oracle count 0 as in the plain run.

What the first `many-topics` run (topics 2000, partitions 1, rate 2000, duration 300, RF 3) must show:

- `cluster.createTopicsMs` (expected under 2 minutes) and `cluster.firstAssignmentMs` per member.
- `cluster.rebalances`: one per member at the start and more around the restart, each with `partitions` (about 667 for three
  members), `joinMs` and `loopDelayMaxMs`, the event loop's worst lag around applying the assignment.
- `cluster.metadata.perMinute`, `meanBytes` and `maxBytes` (an estimate: a full answer for 2,000 topics is about 100 to 200 KB) and
  `cluster.fetchRoundMs`.
- `faults`: one `rolling-restart` phase then three `stop` outages; `oracle.reads` one entry, topic `*`, `exit` 0, `lines`
  equal to `processed`; every oracle count 0; exit 0.
