# Java group member (development only)

A single Apache Kafka Java consumer, run as a child process by the mixed Java/JavaScript
interop scenario so a JS member and a Java member can be proved to share one group under the
same assignor.

It is **not** part of `npm test`, `npm run test:unit` or any test group. It runs only from
`npm run test:e2e:java-interop`, which sets `JAVA_INTEROP_E2E=1`.

## Usage

```bash
scripts/e2e/javaConsumer/run.sh [--group-protocol classic|consumer] [--report-group-type] \
  <bootstrapServers> <groupId> <topic> <assignor> <runForMs>
scripts/e2e/javaConsumer/run.sh --probe                       # download, compile and load only
scripts/e2e/javaConsumer/run.sh --describe-group <bootstrapServers> <groupId>
```

`--group-protocol` defaults to `classic`. Under `consumer` the `<assignor>` argument is the
**server** assignor name (`uniform`, `range`) and goes out as `group.remote.assignor`; the three
classic-only settings are not sent at all, because the Java `ConsumerConfig` rejects them
alongside `group.protocol=consumer`. `--report-group-type` prints one extra line once the member
holds partitions; `--describe-group` prints that line for any group and exits without joining.

`run.sh` downloads `kafka-clients` and `slf4j-api` from Maven Central into a temporary
directory, compiles `JavaGroupMember.java` there and runs it. **The jars and the compiled
classes are never committed**; only the source files and this README are.

`groupConversionProbe.js` (run by `npm run probe:group-conversion`) drives this member together
with two classic members of this client to answer one question: does a running classic group
convert to a `Consumer` group when a consumer-protocol member joins, and convert back when it
leaves? It prints one JSON line and is development-only — no test suite runs it.

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_CLIENTS_VERSION` | `4.3.1` | client jar to download |
| `SLF4J_VERSION` | `1.7.36` | logging API the client's static loggers need |
| `JAVA_E2E_WORK_DIR` | a fresh `mktemp -d` | reuse a cached download/compile directory; when set, the directory is kept rather than removed on exit |

## Output contract

One JSON line per rebalance callback on stdout, plus a final `{"event":"done"}`:

```json
{"event":"assignment","memberId":"...","partitions":{"my-topic":[0,1,2]}}
```

`--report-group-type` and `--describe-group` print one more line, read through the Java admin
client:

```json
{"event":"group-type","groupId":"...","groupType":"Consumer","groupState":"Stable"}
```

A group the coordinator does not know prints that line with empty `groupType` and `groupState`. An
admin client that could not answer at all prints **no** line, reports the exception on stderr and
makes `--describe-group` exit non-zero — so a failed measurement is never read as a group whose type
happens to be empty.

`partitions` is always the member's **full** owned set. Cooperative callbacks report only the
delta of a round, so the component maintains its own owned set and prints the whole of it —
otherwise a cooperative round's delta could be mistaken for an assignment.

## Requirements

A JDK on `PATH` (`javac` and `java`) and network access to Maven Central on the first run. When
either is missing, or a class fails to load, the scenario skips with the reason logged.
