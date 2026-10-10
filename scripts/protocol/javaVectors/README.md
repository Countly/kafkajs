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

## Message vectors for the consumer-protocol, group-visibility and transaction APIs

`scripts/protocol/javaVectors/runAssignors.sh messages` is the third entry point. It compiles
`GenerateMessageVectors.java` against the same two jars and writes 108 fixtures under
`src/protocol/requests/<api>/fixtures/java/`, one per case, covering:

| API | Key | Versions | Vectors |
|---|---|---|---|
| ConsumerGroupHeartbeat | 68 | 0, 1 | 17 |
| ConsumerGroupDescribe | 69 | 0, 1 | 6 |
| ListGroups | 16 | 3, 4, 5 | 8 |
| DescribeGroups | 15 | 3, 4, 5, 6 | 9 |
| OffsetCommit | 8 | 9 | 2 |
| OffsetFetch | 9 | 7, 8, 9 | 9 |
| InitProducerId | 22 | 3, 4, 5 | 9 |
| AddPartitionsToTxn | 24 | 2, 3 | 6 |
| AddOffsetsToTxn | 25 | 2, 3, 4 | 9 |
| EndTxn | 26 | 2, 3, 4, 5 | 17 |
| TxnOffsetCommit | 28 | 2, 3, 4, 5 | 16 |

`runAssignors.sh` with no subcommand, or with `assignors`, does exactly what it did before, and
`run.sh`/`GenerateVectors.java` are unchanged and still own every vector the message generator
does not write.

### Fixture schema

These files are named `<message>-v<version>-<case>.json` and live in a `java/` subdirectory, which
is what distinguishes them from the older `v<N>_<request|response>_java.json` fixtures written
directly under `<api>/fixtures/`. Each holds one case:

| Field | Meaning |
|---|---|
| `message` | the Apache message name, e.g. `ConsumerGroupHeartbeatRequest` |
| `version` | the version the bytes were serialized at |
| `case` | the case name, matching the file name |
| `hex` | the message body Kafka's own codec produced, lower-case hex, no request header |
| `fields` | the values the message was built from, in the **Java message field names** (lower-camel: `throttleTime`, `topics`, `leaderEpoch`, …) — **not** this client's decoded key names, which the shipped decoders choose per API (`responses`, `committedLeaderEpoch`, `clientSideThrottleTime`, …). A codec test compares the **hex** both ways and maps `fields` onto its own decoded shape explicitly; it must never assume the two key sets coincide |

`fields` speaks the decoder's vocabulary rather than the schema's — `topic` not `Name`,
`state`/`protocol` not `GroupState`/`ProtocolData`, `groupGenerationId` not
`GenerationIdOrMemberEpoch`, and an int64 offset as a string — so a spec can encode from `fields`
and compare against `hex`, and decode `hex` and compare against `fields`, in one `toEqual` each. A
field a version does not carry is rendered as the value the decoder reports for it (`""`,
`null`), never omitted — except in the transaction-API vectors, where it is left out of `fields`
(see "What the transaction vectors pin"). `memberMetadata`/`memberAssignment` travel as hex strings
so they compare cleanly with a `Buffer`.

The topic id constant is written in canonical 8-4-4-4-12 form and parsed through
`java.util.UUID`, because `Uuid.fromString` decodes Kafka's base64 uuid form instead.

### The two cases that exist to pin one byte

Neither distinction is visible in the message JSON, and getting either backwards is silent:

- **A null versus a present `Assignment`** in `ConsumerGroupHeartbeatResponse`. The field is
  `nullableVersions: 0+`, so a flexible message writes a one-byte presence marker before the
  struct. `-v0-emptyAssignment` and `-v0-nullAssignment` are identical except that byte (`0x01`
  present, `0xff` null), and `-v0-assignment` differs from the empty one only inside the struct.
  The trio separates "one topic", "you own nothing" and "nothing new to reconcile toward".
- **An empty versus a null `SubscribedTopicNames`.** The wire default is null, meaning "unchanged
  since the last heartbeat"; the empty list is the only way to say "no names", which is what a
  regex subscription must send. Compact-array null is `0x00` and empty is `0x01`.
  `consumerGroupHeartbeatRequest-v1-regexSubscription.json` is the named vector for the empty
  form.

The same reasoning applies to `ListGroups`' two filters, which are not nullable: "no filter" is
the empty compact array, so every version emits both a filtered and an unfiltered request.

### What the transaction vectors pin

Every transaction-API response carries a non-zero `ThrottleTimeMs` (25), so a decoder that does not
split it into `clientSideThrottleTime` (KIP-219) fails against the vector rather than passing on a zero.
Pairs that differ in one field, and the field they differ in:

- `offsetFetchRequest-v{7,8,9}-requireStable` / `-notRequireStable`: the `RequireStable` boolean, the
  last byte before the request's tag buffer (KIP-447).
- `initProducerIdRequest-v{3,4,5}-firstInit` / `-reinit`: the ten bytes of `ProducerId` and
  `ProducerEpoch`, `-1/-1` on a first initialization and the held pair on a KIP-360 re-initialization.
- `initProducerIdRequest-v{3,4,5}-firstInit` / `-nullTransactionalId`: the `TransactionalId` alone, the
  compact nullable string `null` (the single byte `0x00`) of a producer that is idempotent but not
  transactional - what every default producer sends once idempotence is the default.
- `endTxnRequest-v{2..5}-commit` / `-abort`: the `Committed` byte.
- `endTxnResponse-v5-producerIdAndEpoch` / `-defaults`: the ten bytes of the pair the coordinator hands
  back after bumping the epoch (KIP-890). Both fields are plain `5+` fields, not tagged, so every v5 body
  carries them; `defaults` is that body with both at `-1`. There is no separate v5 `ok` case: it would
  be one of these two.
- `txnOffsetCommitRequest-v2-legacy` / `-leaderEpoch`: the four bytes of `CommittedLeaderEpoch`.
  The `GenerationId`/`MemberId`/`GroupInstanceId` triple only exists from v3, so `groupMetadata` starts
  at v3.

AddPartitionsToTxn stops at v3: the schema reserves v4 and above for brokers. Several versions change
what the broker does or answers, not the bytes, and the vectors show it: EndTxn requests v3–v5,
TxnOffsetCommit v3–v5, AddOffsetsToTxn v3–v4 and InitProducerId v3–v5 are identical per case.

Unlike the vectors above, a field a version does not carry is left out of `fields`, because the `5+`/`3+`
schema gives it no wire presence there: the EndTxn v2–v4 responses name no `producerId`/`producerEpoch`,
and the OffsetFetch v7–v8 requests name no `memberId`/`memberEpoch`. Kafka's writer treats such a field
by whether the schema marks it `ignorable`: EndTxn's `ProducerId`/`ProducerEpoch` below v5 are dropped
silently, whatever the builder set, while a non-ignorable one - the TxnOffsetCommit group triple below
v3 - makes the writer throw on a non-default value. The one field the vectors do render below its first
version is that triple, at its defaults (`-1`, `""`, `null`): every `txnOffsetCommitRequest` has the same
shape at v2 to v5.

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
