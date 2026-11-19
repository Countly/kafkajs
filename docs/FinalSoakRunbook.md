# Runbook: the Apache dumps and the core checker

Applies to a smoke or acceptance run of `scripts/soak/finalSoakNode.sh`. The canonical dump procedure is the driver's own
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

Verdicts: `ACCEPTED` (all seven rows PASS; a smoke cannot reach it, because K5 needs two hourly boundaries), `REJECTED` (any
FAIL), `NOT ACCEPTED` (otherwise). Exit status 0 for ACCEPTED, 1 otherwise, 2 for a usage error.

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
- **K5** endpoint and least-squares slope over the hourly quiet triplets; a boundary counts only when its readings lie inside
  its hour's quiet window. `heapUsed` is the whole process: the measured client and the driver's worker share it. A K5 FAIL
  is read by the triage rule in section 6a before anyone calls it a client leak.
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
outcomes whose reason is not null; a null reason is a confirmed accept). None of them changes a verdict. An acknowledged send with `offset: null` must be in the dump exactly once, at any offset.

## 6. After a smoke

Read `lateSteps` (K7) and every INCONCLUSIVE reason. A late supervisor step is information only: `lateSteps` never changes a
row or the verdict, as with the items under NOT JUDGED. Only a late sample changes a row (a periodic slot outside its jitter
fails K7, a quiet reading outside its window leaves K5 INCONCLUSIVE). Do not start the day until a two-outage smoke with one committed and one
aborted transaction replays from its own checkout with K1 to K4, K6 and K7 PASS and K5 INCONCLUSIVE only for its single
boundary.

## 6a. Triage of a K5 FAIL

K5 measures the post-GC `heapUsed` of each subject process (p01 to p12). That number covers the client under test and the
driver's worker code in the same process, so a FAIL alone does not say whose heap grew. Until it is explained, report the FAIL
as **unattributed**, not as a client leak:

1. Compare with the processes that run the same harness without the measured client: the reader (p14), the supervisor (p15)
   and the decoder (p16). Growth of the same shape there points at the harness.
2. Read the periodic post-GC grid of the failing processes (`sample` records, role `periodic`, one per slot): a rise that
   flattens (warm-up of code paths that run for the first time) differs from a steady slope through the run, and growth only
   during traffic differs from growth that also continues in the idle windows.
3. Explain the retained bytes (for example with a heap snapshot from a rerun) before the FAIL is attributed to the client or to
   the driver. The verdict stays REJECTED either way; the triage decides what gets fixed.
