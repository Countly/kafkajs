# Java golden vectors

A development-only generator that produces golden protocol byte vectors from Apache Kafka's own
generated message codecs (`org.apache.kafka.common.message.*Data`, from the published
`kafka-clients` jar), so this repo's hand-written flexible-protocol codecs can be checked
byte-for-byte against the reference implementation, not just against captured broker traffic.

Run `scripts/protocol/javaVectors/run.sh` from the repo root. It needs a JDK on `PATH` (`javac`
and `java`) and, on first run, network access to fetch
`https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.3.1/kafka-clients-4.3.1.jar` from
Maven Central. `GenerateVectors.java` is compiled against that jar and run once, writing
`v<N>_<request|response>_java.json` fixtures under `src/protocol/requests/<api>/fixtures/`:

| API | Versions |
|---|---|
| ApiVersions | 3, 4 |
| Metadata | 9, 10, 11, 12, 13 |
| Produce | 8, 9, 10, 11, 12, 13 |
| FindCoordinator | 3, 4, 5, 6 |
| OffsetCommit | 6, 7, 8 |
| OffsetFetch | 5, 6, 7, 8 |
| ListOffsets | 4, 5, 6, 7, 8, 9 |
| InitProducerId | 2, 3, 4, 5 |
| Fetch | 12, 13, 14, 15, 16, 17, 18 |
| OffsetForLeaderEpoch | 3, 4 |
| SaslAuthenticate | 2 |

Both directions are emitted for every version. Every value is a constant of `GenerateVectors.java`,
so a spec can rebuild the same request through this repo's encoder and compare the two buffers
byte-for-byte. The exception is Produce: its request carries a record batch whose timestamp and CRC
no second encoder can reproduce, so those vectors are emitted with a null `Records` field and the
Produce request specs decode the vector's framing instead of comparing bytes. Response vectors are
always decoded rather than byte-compared — this client has no response encoder.

The jar itself is downloaded into a temporary directory and is never committed to this repository
— only the emitted `*_java.json` fixtures are. Re-run the script whenever a codec's fixture values
change, and commit the regenerated files alongside the codec change.

## Assignor vectors

`scripts/protocol/javaVectors/runAssignors.sh` is a second generator with the same shape,
compiling `GenerateAssignorVectors.java` against the same jar plus `slf4j-api` and writing
`src/consumer/assigners/fixtures/<assignor>/<caseId>_java.json`. It runs Apache Kafka's own
`RangeAssignor`, `StickyAssignor` and `CooperativeStickyAssignor` through
`ConsumerPartitionAssignor#assign(Cluster, GroupSubscription)` over 33 subscription, ownership and
generation shapes, so the built-in `range`, `sticky` and `cooperative-sticky` assigners are checked
against the reference implementation rather than against a reading of it. The extra jar is not
optional: all three assignors extend `AbstractPartitionAssignor`, which holds a static
`LoggerFactory.getLogger(...)`, so loading any of them without SLF4J on the classpath throws
`NoClassDefFoundError`.

| Assignor | Vectors |
|---|---|
| `range` | 8 |
| `sticky` | 20 |
| `cooperative-sticky` | 5 |

### Fixture schema

Each `<caseId>_java.json` is one case. All of it is generated; none of it is edited by hand.

| Field | Meaning |
|---|---|
| `assignor` | wire name — `range`, `sticky` or `cooperative-sticky`; also the directory |
| `caseId` | the case name, matching the file name |
| `subscriptionVersion` | the `ConsumerProtocolSubscription` version the member subscriptions are encoded at. **3 for every vector except `cooperative_no_generation_id`, which is 1.** v0 and v2 have no Java vector at all |
| `assignmentVersion` | the `ConsumerProtocolAssignment` version `expectedAssignmentBytesHex` is encoded at (always 3) |
| `nodes` | the cluster's brokers — `{ id, host, port }`, no rack |
| `topics` | topic name to `[{ partition, leader }]`, the cluster metadata the leader sees |
| `members[]` | `memberId`, `groupInstanceId`, `topics`, `ownedPartitions` (`[{ topic, partition }]`), `generationId`, `rackId`, `userDataHex` (the assignor payload, `null` when the member sends none) and `subscriptionBytesHex` (the full encoded subscription) |
| `expected` | Kafka's assignment: member id to topic to ascending partitions, with an entry for **every** member including empty ones |
| `expectedAssignmentBytesHex` | member id to Kafka's encoded `ConsumerProtocolAssignment` |
| `expectedForkAssignment` | **present on one vector only.** This client's assignment for a shape where both answers are correct — see the divergence below |

Three details of the generator are load-bearing:

- **`setGroupInstanceId` is called after the five-argument `Subscription` constructor**, which has
  no such parameter. A generator that forgets it silently records the assignment member-id ordering
  alone produces, and `group_instance_ids_mixed` exists to detect exactly that.
- **The cluster is built from `PartitionInfo` lists, not partition counts.**
  `CooperativeStickyAssignor`'s withhold step lives in `assignPartitions(Map<String,
  List<PartitionInfo>>, Map<String, Subscription>)`, so a count-map generator emits non-withheld
  cooperative vectors.
- **Every subscription is round-tripped through the wire before it is assigned from.** The leader is
  handed bytes, never the object a member built, so only a round trip lets the serialisation version
  decide which fields the assignor sees — `cooperative_no_generation_id` is recorded at
  subscription v1, which carries `OwnedPartitions` but no `GenerationId`, and without the round trip
  the in-memory subscription would still carry one. Reading a subscription also consumes its
  `userData` buffer, so that payload's hex is recorded before the assignor runs.

Assignor `userData` is never hand-rolled. The V1 writers are package-private in
`org.apache.kafka.clients.consumer`, so every recorded payload is produced by driving a real
assignor instance through `onAssignment` and reading back `subscriptionUserData(...)` — one
instance per member, because the state is per-instance.

### The one documented divergence

`sticky/scaleup_tiebreak_divergent_java.json` is the only vector this client does **not** reproduce,
and it is committed deliberately. Seven partitions, one incumbent owning four and another owning
three, a third member joining: `minQuota` is 2, `maxQuota` 3, and exactly one member may sit above
the minimum. Whichever over-quota incumbent the assignor visits first keeps three partitions. Kafka
iterates its `HashMap` in hash-bucket order; this client iterates in member-info order. Both are
deterministic, both produce a balanced 3/2/2 over the same seven partitions, and they disagree about
which incumbent loses one:

| | `m-1` (joining) | `m-2` (owns 4) | `m-3` (owns 3) |
|---|---|---|---|
| Apache Kafka | `[2, 3]` | `[0, 1]` | `[4, 5, 6]` |
| this client | `[3, 6]` | `[0, 1, 2]` | `[4, 5]` |

The divergence is accepted and documented rather than emulated: reproducing it would mean
reproducing bucket order across several maps plus Kafka's resize and treeification rules, for no
correctness gain, since one leader computes each round and every member follows it. The consequence
is that stickiness across a leader change between a Java member and a member of this client may move
one more partition than Java alone would.

So the vector is pinned on **both** sides — the fixture records Kafka's answer in `expected` and this
client's in `expectedForkAssignment`, and the spec asserts this client matches the second, still
differs from the first, and that the two cover the same partitions with the same balance. A change on
either side becomes a failing test rather than silent drift. `scaleup_tiebreak` is the same shape with
the owners rotated onto `m-1` and `m-2`, where the two orders happen to agree; it is an ordinary
matching vector, and the pair together is what makes the cause legible.

### What the vectors do not pin

- **The order partitions are placed in.** Kafka's `Assignment.partitions()` is a `List` whose order
  is an assignor internal, not something the wire format or any consumer can observe, so the spec
  compares each topic's partitions as an ascending set. They genuinely differ: **30 member
  assignments carry identical contents in a different order** — 27 in `large_heterogeneous`, 2 in
  `heterogeneous_general` and `m-1` in `one_member_leaving` (`[0,1,2,3]` here against `[0,2,1,3]` in
  Kafka). All three are general-path or re-assignment shapes; every constrained-path vector places
  partitions in the same order Kafka does.
- **Subscription versions 0 and 2.** 31 vectors are v3 and one is v1; the other two layouts rest on
  hand-written fixtures elsewhere in the tree.
- **Rack-aware assignment**, by construction: nodes carry no rack and no member sends a `rackId`,
  which keeps Kafka's rack-aware pass off on both sides.
- **The exact subscription byte sequence a member would send unaided.** `serializeSubscription`
  sorts `Topics` and `OwnedPartitions`; this repo's codec does not, so the harness applies Kafka's
  sorting before encoding. The vectors pin the field layout, not the caller's field order.

Nodes carry no rack, which keeps Kafka's rack-aware path off and the vectors reproducible by an
implementation that ignores racks. The vectors are reproduced by
`src/consumer/assigners/__tests__/javaVectors.spec.js`, which fails rather than skips when one
cannot be reproduced.
