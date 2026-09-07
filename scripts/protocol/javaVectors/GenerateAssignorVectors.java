import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * Generates golden assignment vectors by running Apache Kafka's own partition assignors.
 *
 * <p>Development-only: run via {@code scripts/protocol/javaVectors/runAssignors.sh}. Never invoked
 * from {@code npm test} or any committed build step - the {@code kafka-clients} jar is not a
 * project dependency, only a tool used once to produce fixtures that are then committed.
 *
 * <p>Two details are load-bearing. Each {@link ConsumerPartitionAssignor.Subscription} is built
 * with the five-argument constructor and {@code setGroupInstanceId} is called afterwards, because
 * the constructor has no such parameter; a generator that forgets it silently produces
 * member-id-only ordering. And the cluster is built from real {@link PartitionInfo} lists rather
 * than partition counts, because {@code CooperativeStickyAssignor}'s withhold step reads them.
 *
 * <p>Fixture nodes carry no rack, which keeps the rack-aware path off and the vectors reproducible
 * by an implementation that ignores racks.
 *
 * <p>Assignor {@code userData} is never hand-rolled: the payload writers are package-private in
 * {@code org.apache.kafka.clients.consumer}, so every vector's {@code userData} is produced by
 * driving a real assignor instance through {@code onAssignment} and reading back what it would
 * send in its next subscription.
 *
 * @see <a href="https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerPartitionAssignor.java">ConsumerPartitionAssignor.java</a>
 * @see <a href="https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractStickyAssignor.java">AbstractStickyAssignor.java</a>
 */
public final class GenerateAssignorVectors {

  // Per case, not global: `cooperative_no_generation_id` must be serialised at v1, and the
  // phase's goal is a v0-v3 codec, so the vector set covers more than the top version.
  private static final short DEFAULT_SUBSCRIPTION_VERSION = 3;
  private static final short ASSIGNMENT_VERSION = 3;
  private static final String CLUSTER_ID = "golden-cluster";
  private static final String GROUP_ID = "golden";
  private static final int DEFAULT_GENERATION = -1;

  private static final String RANGE = "range";
  private static final String STICKY = "sticky";
  private static final String COOPERATIVE = "cooperative-sticky";

  /**
   * What this client's `sticky` assigner computes for `scaleup_tiebreak_divergent` - a different,
   * equally balanced assignment than Kafka's, because the two visit the over-quota owners in a
   * different order. Recorded once from the assigner and reviewed; it is a constant here so a
   * regenerated fixture stays byte-identical and so the divergence cannot drift unnoticed on
   * either side.
   */
  private static final String FORK_SCALEUP_TIEBREAK =
      "{\n"
          + "    \"m-1\": {\n      \"t-a\": [3, 6]\n    },\n"
          + "    \"m-2\": {\n      \"t-a\": [0, 1, 2]\n    },\n"
          + "    \"m-3\": {\n      \"t-a\": [4, 5]\n    }\n"
          + "  }";

  /** One member of one case, before it becomes a Subscription. */
  private static final class MemberSpec {
    final String memberId;
    final String groupInstanceId;
    final List<String> topics;
    final List<TopicPartition> ownedPartitions;
    final int generationId;
    final String rackId;
    final ByteBuffer userData;

    MemberSpec(
        String memberId,
        String groupInstanceId,
        List<String> topics,
        List<TopicPartition> ownedPartitions,
        int generationId,
        String rackId,
        ByteBuffer userData) {
      this.memberId = memberId;
      this.groupInstanceId = groupInstanceId;
      this.topics = topics;
      this.ownedPartitions = ownedPartitions;
      this.generationId = generationId;
      this.rackId = rackId;
      this.userData = userData;
    }
  }

  /** One case: an assignor, a topology and a set of members. */
  private static final class CaseSpec {
    final String caseId;
    final String assignor;
    /** LinkedHashMap, never Map.of: the JSON below and the JS side must agree on topic order. */
    final Map<String, Integer> topics;
    final List<MemberSpec> members;
    final short subscriptionVersion;
    /**
     * Set only for a shape where a different but equally balanced assignment is correct, and this
     * client computes the other one. Rendered verbatim as the fixture's
     * {@code expectedForkAssignment}, so the vector pins both outputs instead of pinning neither.
     * Null for every ordinary vector.
     */
    final String forkAssignment;

    CaseSpec(
        String caseId,
        String assignor,
        Map<String, Integer> topics,
        List<MemberSpec> members,
        short subscriptionVersion,
        String forkAssignment) {
      this.caseId = caseId;
      this.assignor = assignor;
      this.topics = topics;
      this.members = members;
      this.subscriptionVersion = subscriptionVersion;
      this.forkAssignment = forkAssignment;
    }
  }

  private static final List<Node> NODES =
      List.of(new Node(0, "broker-0", 9092), new Node(1, "broker-1", 9092));

  /**
   * @param args optional output directory; defaults to the working directory
   * @throws IOException when a fixture cannot be written
   */
  public static void main(String[] args) throws IOException {
    Path outDir = Paths.get(args.length > 0 ? args[0] : ".");

    for (CaseSpec spec : cases()) {
      Path dir = outDir.resolve("src/consumer/assigners/fixtures").resolve(spec.assignor);
      Files.createDirectories(dir);
      Files.writeString(
          dir.resolve(spec.caseId + "_java.json"), run(spec) + "\n", StandardCharsets.UTF_8);
      System.out.println("wrote " + spec.assignor + "/" + spec.caseId + "_java.json");
    }
  }

  /**
   * @param name the wire name of the assignor
   * @return a fresh instance of Apache Kafka's assignor of that name
   * @throws IllegalArgumentException for any other name
   */
  private static ConsumerPartitionAssignor assignorFor(String name) {
    switch (name) {
      case RANGE:
        return new RangeAssignor();
      case STICKY:
        return new StickyAssignor();
      case COOPERATIVE:
        return new CooperativeStickyAssignor();
      default:
        throw new IllegalArgumentException("unknown assignor " + name);
    }
  }

  /**
   * @param topics topic name to partition count, in the order the JSON should carry
   * @return a rack-free cluster whose partitions are real PartitionInfo instances
   */
  private static Cluster clusterFor(Map<String, Integer> topics) {
    List<PartitionInfo> partitions = new ArrayList<>();
    for (Map.Entry<String, Integer> entry : topics.entrySet()) {
      for (int partition = 0; partition < entry.getValue(); partition++) {
        Node leader = NODES.get(partition % NODES.size());
        Node[] replicas = new Node[] {leader};
        partitions.add(new PartitionInfo(entry.getKey(), partition, leader, replicas, replicas));
      }
    }

    return new Cluster(
        CLUSTER_ID, NODES, partitions, Collections.emptySet(), Collections.emptySet());
  }

  /**
   * @param spec the case to run
   * @return the fixture JSON for that case, without a trailing newline
   */
  private static String run(CaseSpec spec) {
    Cluster cluster = clusterFor(spec.topics);
    Map<String, ConsumerPartitionAssignor.Subscription> subscriptions = new LinkedHashMap<>();
    Map<String, String> subscriptionBytes = new LinkedHashMap<>();
    Map<String, String> userDataBytes = new LinkedHashMap<>();

    for (MemberSpec member : spec.members) {
      ConsumerPartitionAssignor.Subscription sent =
          new ConsumerPartitionAssignor.Subscription(
              member.topics,
              member.userData,
              member.ownedPartitions,
              member.generationId,
              Optional.ofNullable(member.rackId));

      ByteBuffer bytes = ConsumerProtocol.serializeSubscription(sent, spec.subscriptionVersion);
      subscriptionBytes.put(member.memberId, hex(bytes));
      userDataBytes.put(member.memberId, hex(member.userData));

      // The leader is handed bytes, never the object a member built, so the subscription is
      // round-tripped: only then does the serialisation version actually decide which fields the
      // assignor sees, and only then are the topics and owned partitions in the order the wire
      // format sorts them into. Reading also consumes the userData buffer, which is why its hex is
      // recorded above rather than below.
      ConsumerPartitionAssignor.Subscription received =
          ConsumerProtocol.deserializeSubscription(bytes.duplicate());
      // Not a payload field - it travels in JoinGroup - and not a constructor parameter either. A
      // generator that omits this line silently produces member-id-only ordering, which several of
      // these cases exist to detect.
      received.setGroupInstanceId(Optional.ofNullable(member.groupInstanceId));

      subscriptions.put(member.memberId, received);
    }

    ConsumerPartitionAssignor.GroupAssignment assignment =
        assignorFor(spec.assignor)
            .assign(cluster, new ConsumerPartitionAssignor.GroupSubscription(subscriptions));

    StringBuilder json = new StringBuilder();
    json.append("{\n");
    json.append("  \"assignor\": ").append(quote(spec.assignor)).append(",\n");
    json.append("  \"caseId\": ").append(quote(spec.caseId)).append(",\n");
    json.append("  \"subscriptionVersion\": ").append(spec.subscriptionVersion).append(",\n");
    json.append("  \"assignmentVersion\": ").append(ASSIGNMENT_VERSION).append(",\n");
    json.append("  \"nodes\": [\n");
    for (int i = 0; i < NODES.size(); i++) {
      Node node = NODES.get(i);
      json.append("    { \"id\": ")
          .append(node.id())
          .append(", \"host\": ")
          .append(quote(node.host()))
          .append(", \"port\": ")
          .append(node.port())
          .append(" }")
          .append(i + 1 < NODES.size() ? ",\n" : "\n");
    }
    json.append("  ],\n");

    json.append("  \"topics\": {\n");
    int t = 0;
    for (Map.Entry<String, Integer> entry : spec.topics.entrySet()) {
      json.append("    ").append(quote(entry.getKey())).append(": [");
      for (int partition = 0; partition < entry.getValue(); partition++) {
        json.append("{ \"partition\": ")
            .append(partition)
            .append(", \"leader\": ")
            .append(NODES.get(partition % NODES.size()).id())
            .append(" }")
            .append(partition + 1 < entry.getValue() ? ", " : "");
      }
      json.append("]").append(++t < spec.topics.size() ? ",\n" : "\n");
    }
    json.append("  },\n");

    json.append("  \"members\": [\n");
    for (int i = 0; i < spec.members.size(); i++) {
      MemberSpec member = spec.members.get(i);
      json.append("    {\n");
      json.append("      \"memberId\": ").append(quote(member.memberId)).append(",\n");
      json.append("      \"groupInstanceId\": ").append(quote(member.groupInstanceId)).append(",\n");
      json.append("      \"topics\": ").append(stringArray(member.topics)).append(",\n");
      json.append("      \"ownedPartitions\": ")
          .append(topicPartitionArray(member.ownedPartitions))
          .append(",\n");
      json.append("      \"generationId\": ").append(member.generationId).append(",\n");
      json.append("      \"rackId\": ").append(quote(member.rackId)).append(",\n");
      json.append("      \"userDataHex\": ")
          .append(quote(userDataBytes.get(member.memberId)))
          .append(",\n");
      json.append("      \"subscriptionBytesHex\": ")
          .append(quote(subscriptionBytes.get(member.memberId)))
          .append("\n");
      json.append("    }").append(i + 1 < spec.members.size() ? ",\n" : "\n");
    }
    json.append("  ],\n");

    json.append("  \"expected\": {\n");
    List<String> memberIds = new ArrayList<>(assignment.groupAssignment().keySet());
    Collections.sort(memberIds);
    for (int i = 0; i < memberIds.size(); i++) {
      String memberId = memberIds.get(i);
      ConsumerPartitionAssignor.Assignment memberAssignment =
          assignment.groupAssignment().get(memberId);
      json.append("    ").append(quote(memberId)).append(": {\n");
      Map<String, List<Integer>> byTopic = new LinkedHashMap<>();
      for (TopicPartition tp : memberAssignment.partitions()) {
        byTopic.computeIfAbsent(tp.topic(), key -> new ArrayList<>()).add(tp.partition());
      }
      List<String> topics = new ArrayList<>(byTopic.keySet());
      Collections.sort(topics);
      for (int j = 0; j < topics.size(); j++) {
        List<Integer> partitions = byTopic.get(topics.get(j));
        Collections.sort(partitions);
        json.append("      ")
            .append(quote(topics.get(j)))
            .append(": ")
            .append(intArray(partitions))
            .append(j + 1 < topics.size() ? ",\n" : "\n");
      }
      json.append("    }").append(i + 1 < memberIds.size() ? ",\n" : "\n");
    }
    json.append("  },\n");

    json.append("  \"expectedAssignmentBytesHex\": {\n");
    for (int i = 0; i < memberIds.size(); i++) {
      String memberId = memberIds.get(i);
      json.append("    ")
          .append(quote(memberId))
          .append(": ")
          .append(
              quote(
                  hex(
                      ConsumerProtocol.serializeAssignment(
                          assignment.groupAssignment().get(memberId), ASSIGNMENT_VERSION))))
          .append(i + 1 < memberIds.size() ? ",\n" : "\n");
    }
    json.append("  }");

    if (spec.forkAssignment != null) {
      json.append(",\n  \"expectedForkAssignment\": ").append(spec.forkAssignment).append("\n");
    } else {
      json.append("\n");
    }

    json.append("}");

    return json.toString();
  }

  /**
   * @param buffer a buffer whose position is left untouched
   * @return lower-case hex of the remaining bytes, or null for a null buffer
   */
  private static String hex(ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }

    ByteBuffer duplicate = buffer.duplicate();
    StringBuilder out = new StringBuilder();
    while (duplicate.hasRemaining()) {
      out.append(String.format("%02x", duplicate.get()));
    }
    return out.toString();
  }

  /**
   * @param value any string
   * @return the JSON literal for it: a quoted, escaped string, or the token {@code null}
   */
  private static String quote(String value) {
    return value == null ? "null" : "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  /**
   * @param values the strings to render
   * @return a JSON array of them, on one line
   */
  private static String stringArray(List<String> values) {
    List<String> quoted = new ArrayList<>();
    for (String value : values) {
      quoted.add(quote(value));
    }
    return "[" + String.join(", ", quoted) + "]";
  }

  /**
   * @param values the numbers to render
   * @return a JSON array of them, on one line
   */
  private static String intArray(List<Integer> values) {
    List<String> parts = new ArrayList<>();
    for (Integer value : values) {
      parts.add(String.valueOf(value));
    }
    return "[" + String.join(", ", parts) + "]";
  }

  /**
   * @param values the partitions to render
   * @return a JSON array of {@code { topic, partition }} objects, on one line
   */
  private static String topicPartitionArray(List<TopicPartition> values) {
    List<String> parts = new ArrayList<>();
    for (TopicPartition tp : values) {
      parts.add("{ \"topic\": " + quote(tp.topic()) + ", \"partition\": " + tp.partition() + " }");
    }
    return "[" + String.join(", ", parts) + "]";
  }

  // ---------------------------------------------------------------------------------------------
  // Case construction
  // ---------------------------------------------------------------------------------------------

  /**
   * @param nameThenCount alternating topic name and partition count
   * @return the topology, in insertion order
   */
  private static Map<String, Integer> topology(Object... nameThenCount) {
    Map<String, Integer> topics = new LinkedHashMap<>();
    for (int i = 0; i < nameThenCount.length; i += 2) {
      topics.put((String) nameThenCount[i], (Integer) nameThenCount[i + 1]);
    }
    return topics;
  }

  /**
   * @param topic the topic every returned partition belongs to
   * @param partitions the partition numbers
   * @return the partitions as TopicPartition instances
   */
  private static List<TopicPartition> tps(String topic, int... partitions) {
    List<TopicPartition> out = new ArrayList<>();
    for (int partition : partitions) {
      out.add(new TopicPartition(topic, partition));
    }
    return out;
  }

  /**
   * Produces the {@code userData} Java's {@code StickyAssignor} would send after being assigned
   * {@code partitions} in {@code generationId}. The V1 writer is package-private, so the assignor
   * itself is driven rather than imitated.
   *
   * @param memberId the member the payload belongs to
   * @param topics the member's subscription, passed on to {@code subscriptionUserData}
   * @param partitions what the member holds
   * @param generationId the generation it holds them in
   * @return the V1 payload, byte-accurate
   */
  private static ByteBuffer stickyUserData(
      String memberId, List<String> topics, List<TopicPartition> partitions, int generationId) {
    StickyAssignor assignor = new StickyAssignor();
    assignor.onAssignment(
        new ConsumerPartitionAssignor.Assignment(partitions),
        new ConsumerGroupMetadata(GROUP_ID, generationId, memberId, Optional.empty()));
    return assignor.subscriptionUserData(new HashSet<>(topics));
  }

  /**
   * Produces the four-byte {@code userData} Java's {@code CooperativeStickyAssignor} would send in
   * {@code generationId}.
   *
   * @param memberId the member the payload belongs to
   * @param topics the member's subscription
   * @param generationId the generation it last held
   * @return the four-byte payload
   */
  private static ByteBuffer cooperativeUserData(
      String memberId, List<String> topics, int generationId) {
    CooperativeStickyAssignor assignor = new CooperativeStickyAssignor();
    assignor.onAssignment(
        new ConsumerPartitionAssignor.Assignment(Collections.emptyList()),
        new ConsumerGroupMetadata(GROUP_ID, generationId, memberId, Optional.empty()));
    return assignor.subscriptionUserData(new HashSet<>(topics));
  }

  /**
   * Runs one round to seed each member's retained state, then reads the userData it would send
   * next time. The serialisation helpers are package-private in
   * {@code org.apache.kafka.clients.consumer}, so the assignor itself is the only way to produce a
   * byte-accurate V1 payload; one instance per member, because the state is per-instance.
   *
   * @param topics the topology of the prior round
   * @param members the members of the prior round, each subscribing as it will next round
   * @param generationId the generation the prior round assigned
   * @return member id to the userData that member would send in the next subscription
   */
  private static Map<String, ByteBuffer> priorRoundUserData(
      Map<String, Integer> topics, List<MemberSpec> members, int generationId) {
    Cluster cluster = clusterFor(topics);
    Map<String, ConsumerPartitionAssignor.Subscription> first = new LinkedHashMap<>();
    Map<String, StickyAssignor> assignors = new LinkedHashMap<>();

    for (MemberSpec member : members) {
      assignors.put(member.memberId, new StickyAssignor());
      first.put(
          member.memberId,
          new ConsumerPartitionAssignor.Subscription(
              member.topics, null, Collections.emptyList(), DEFAULT_GENERATION, Optional.empty()));
    }

    // One shared instance performs the leader's assignment; each member's own instance is then
    // told what it received, which is exactly what a real member does.
    ConsumerPartitionAssignor.GroupAssignment round =
        new StickyAssignor().assign(cluster, new ConsumerPartitionAssignor.GroupSubscription(first));

    Map<String, ByteBuffer> userData = new LinkedHashMap<>();
    for (MemberSpec member : members) {
      StickyAssignor assignor = assignors.get(member.memberId);
      ConsumerPartitionAssignor.Assignment mine = round.groupAssignment().get(member.memberId);
      assignor.onAssignment(
          mine == null ? new ConsumerPartitionAssignor.Assignment(Collections.emptyList()) : mine,
          new ConsumerGroupMetadata(GROUP_ID, generationId, member.memberId, Optional.empty()));
      userData.put(member.memberId, assignor.subscriptionUserData(new HashSet<>(member.topics)));
    }

    return userData;
  }

  /**
   * A member that has never been assigned anything: no instance id, no owned partitions, the
   * default generation and no userData.
   *
   * @param memberId the member id
   * @param topics its subscription
   * @return the member spec
   */
  private static MemberSpec fresh(String memberId, List<String> topics) {
    return new MemberSpec(
        memberId, null, topics, Collections.emptyList(), DEFAULT_GENERATION, null, null);
  }

  /**
   * The same member, carrying whatever a cooperative peer of that generation would send: its owned
   * partitions on the wire and the generation in both the protocol field and the userData.
   *
   * @param memberId the member id
   * @param topics its subscription
   * @param owned what it still holds
   * @param generationId the generation it holds them in
   * @return the member spec
   */
  private static MemberSpec cooperativeMember(
      String memberId, List<String> topics, List<TopicPartition> owned, int generationId) {
    return new MemberSpec(
        memberId,
        null,
        topics,
        owned,
        generationId,
        null,
        cooperativeUserData(memberId, topics, generationId));
  }

  /**
   * A sticky member whose retained state claims {@code owned} in {@code generationId}. The
   * protocol {@code GenerationId} field carries the same number as the userData, which is what a
   * real member sends and what keeps the two implementations comparable.
   *
   * @param memberId the member id
   * @param topics its subscription
   * @param owned what its userData claims
   * @param generationId the generation the claim is made in
   * @return the member spec
   */
  private static MemberSpec stickyMember(
      String memberId, List<String> topics, List<TopicPartition> owned, int generationId) {
    return new MemberSpec(
        memberId,
        null,
        topics,
        Collections.emptyList(),
        generationId,
        null,
        stickyUserData(memberId, topics, owned, generationId));
  }

  /**
   * The ~30 cases, one per rule the JavaScript implementation claims to follow.
   *
   * <p>Every topology is a constant here - node ids, topic names, partition counts, member ids and
   * instance ids - so a re-run produces byte-identical goldens. Every map is a
   * {@link LinkedHashMap}: iteration order reaches the JSON, and {@code Map.of} does not promise
   * one.
   *
   * @return the case list, in the order the fixtures are written
   */
  private static List<CaseSpec> cases() {
    List<CaseSpec> cases = new ArrayList<>();

    List<String> a = List.of("t-a");
    List<String> ab = List.of("t-a", "t-b");

    // t-a:3, one member. The trivial shape every assignor must agree on.
    Map<String, Integer> oneTopic = topology("t-a", 3);
    cases.add(vector("one_topic_one_member", RANGE, oneTopic, List.of(fresh("m-1", a))));
    cases.add(vector("one_topic_one_member", STICKY, oneTopic, List.of(fresh("m-1", a))));
    cases.add(
        vector(
            "one_topic_one_member",
            COOPERATIVE,
            oneTopic,
            List.of(cooperativeMember("m-1", a, Collections.emptyList(), DEFAULT_GENERATION))));

    // t-a:4 over two members: the exact-quota case.
    Map<String, Integer> evenSplit = topology("t-a", 4);
    List<MemberSpec> twoFresh = List.of(fresh("m-1", a), fresh("m-2", a));
    cases.add(vector("even_split", RANGE, evenSplit, twoFresh));
    cases.add(vector("even_split", STICKY, evenSplit, twoFresh));

    // t-a:7 over three members: the remainder goes to the first members in order.
    Map<String, Integer> unevenSplit = topology("t-a", 7);
    List<MemberSpec> threeFresh = List.of(fresh("m-1", a), fresh("m-2", a), fresh("m-3", a));
    cases.add(vector("uneven_split", RANGE, unevenSplit, threeFresh));
    cases.add(vector("uneven_split", STICKY, unevenSplit, threeFresh));

    // Two members must end up with nothing at all.
    Map<String, Integer> scarce = topology("t-a", 2);
    List<MemberSpec> fourFresh =
        List.of(fresh("m-1", a), fresh("m-2", a), fresh("m-3", a), fresh("m-4", a));
    cases.add(vector("more_consumers_than_partitions", RANGE, scarce, fourFresh));
    cases.add(vector("more_consumers_than_partitions", STICKY, scarce, fourFresh));

    // Range divides each topic separately; sticky divides the pooled partitions.
    Map<String, Integer> twoTopics = topology("t-a", 4, "t-b", 7);
    List<MemberSpec> threeOnBoth = List.of(fresh("m-1", ab), fresh("m-2", ab), fresh("m-3", ab));
    cases.add(vector("two_topics_different_counts", RANGE, twoTopics, threeOnBoth));
    cases.add(vector("two_topics_different_counts", STICKY, twoTopics, threeOnBoth));

    // Every member subscribes identically, so sticky takes its constrained path.
    List<String> abc = List.of("t-a", "t-b", "t-c");
    cases.add(
        vector(
            "identical_subscriptions_constrained",
            STICKY,
            topology("t-a", 6, "t-b", 6, "t-c", 6),
            List.of(fresh("m-1", abc), fresh("m-2", abc), fresh("m-3", abc), fresh("m-4", abc))));

    // Overlapping but unequal subscriptions force sticky's general (KIP-54) path.
    cases.add(
        vector(
            "heterogeneous_general",
            STICKY,
            topology("t-a", 4, "t-b", 4, "t-c", 4, "t-d", 4),
            List.of(
                fresh("m-1", List.of("t-a")),
                fresh("m-2", List.of("t-a", "t-b")),
                fresh("m-3", List.of("t-b", "t-c")),
                fresh("m-4", List.of("t-c", "t-d")))));

    // Static members sort first, by instance id, so the result differs from member-id ordering.
    Map<String, Integer> sixPartitions = topology("t-a", 6);
    List<MemberSpec> mixedInstanceIds =
        List.of(
            fresh("m-1", a),
            fresh("m-2", a),
            fresh("m-3", a),
            new MemberSpec("m-4", "i-1", a, Collections.emptyList(), DEFAULT_GENERATION, null, null),
            new MemberSpec(
                "m-5", "i-2", a, Collections.emptyList(), DEFAULT_GENERATION, null, null));
    cases.add(vector("group_instance_ids_mixed", RANGE, sixPartitions, mixedInstanceIds));
    cases.add(vector("group_instance_ids_mixed", STICKY, sixPartitions, mixedInstanceIds));

    // A second round: each member's userData carries what the first round gave it.
    Map<String, ByteBuffer> carried = priorRoundUserData(evenSplit, twoFresh, 1);
    MemberSpec carried1 = withUserData(fresh("m-1", a), carried.get("m-1"), 1);
    MemberSpec carried2 = withUserData(fresh("m-2", a), carried.get("m-2"), 1);
    cases.add(vector("ownership_carried_over", STICKY, evenSplit, List.of(carried1, carried2)));
    cases.add(vector("one_member_leaving", STICKY, evenSplit, List.of(carried1)));
    cases.add(
        vector(
            "one_member_joining",
            STICKY,
            evenSplit,
            List.of(carried1, carried2, fresh("m-3", a))));

    // Two members claim the same partition in the same generation: neither keeps it.
    cases.add(
        vector(
            "same_generation_conflict",
            STICKY,
            scarce,
            List.of(
                stickyMember("m-1", a, tps("t-a", 0), 4),
                stickyMember("m-2", a, tps("t-a", 0), 4))));

    // The same claim at different generations: the newer one keeps it.
    cases.add(
        vector(
            "higher_generation_wins",
            STICKY,
            scarce,
            List.of(
                stickyMember("m-1", a, tps("t-a", 0), 4),
                stickyMember("m-2", a, tps("t-a", 0), 5))));

    // Nobody has ever been assigned anything, so there is no stickiness to honour.
    cases.add(vector("default_generation_first_join", STICKY, evenSplit, twoFresh));

    // A scale-up whose quotas leave one over-quota owner able to keep its extra partition and the
    // other not: seven partitions, minQuota 2, maxQuota 3, and room for exactly one member above
    // the minimum. Whichever over-quota owner the assignor visits first keeps three. Kafka visits
    // its HashMap in hash-bucket order; this client visits in member-info order. Both are
    // deterministic and both produce a balanced 3/2/2, so the two agree only when the orders
    // happen to agree - which is what this pair of vectors separates.
    Map<String, Integer> sevenPartitions = topology("t-a", 7);

    // Here they agree: m-1 owns four and is first in both orders.
    cases.add(
        vector(
            "scaleup_tiebreak",
            STICKY,
            sevenPartitions,
            List.of(
                stickyMember("m-1", a, tps("t-a", 0, 1, 2, 3), 1),
                stickyMember("m-2", a, tps("t-a", 4, 5, 6), 1),
                fresh("m-3", a))));

    // The same shape with the owners rotated onto m-2 and m-3, which inverts the two orders: Kafka
    // visits m-3 before m-2 and keeps m-3 whole, this client visits m-2 first and keeps m-2 whole.
    // Both assignments are balanced and cover the same partitions; they differ in which incumbent
    // loses one. The divergence is accepted rather than emulated, so BOTH outputs are recorded and
    // the spec asserts both - a change on either side is then a failing test rather than a silent
    // drift.
    cases.add(
        new CaseSpec(
            "scaleup_tiebreak_divergent",
            STICKY,
            sevenPartitions,
            List.of(
                fresh("m-1", a),
                stickyMember("m-2", a, tps("t-a", 0, 1, 2, 3), 1),
                stickyMember("m-3", a, tps("t-a", 4, 5, 6), 1)),
            DEFAULT_SUBSCRIPTION_VERSION,
            FORK_SCALEUP_TIEBREAK));

    // t-a-1 must move from m-1 to m-2, so it is withheld for a follow-up round.
    Map<String, Integer> pair = topology("t-a", 2);
    List<MemberSpec> withhold =
        List.of(
            cooperativeMember("m-1", a, tps("t-a", 0, 1), 1),
            cooperativeMember("m-2", a, Collections.emptyList(), DEFAULT_GENERATION));
    cases.add(vector("cooperative_withhold", COOPERATIVE, pair, withhold));

    // t-a-1 is owned by nobody, so it transfers in the same round.
    cases.add(
        vector(
            "cooperative_immediate_transfer",
            COOPERATIVE,
            pair,
            List.of(
                cooperativeMember("m-1", a, tps("t-a", 0), 1),
                cooperativeMember("m-2", a, Collections.emptyList(), DEFAULT_GENERATION))));

    // Subscription v1 carries OwnedPartitions but no GenerationId, so the generation is read back
    // out of the userData instead.
    cases.add(
        new CaseSpec("cooperative_no_generation_id", COOPERATIVE, pair, withhold, (short) 1, null));

    // Sticky is eager: OwnedPartitions is on the wire and deliberately ignored.
    cases.add(
        vector(
            "sticky_ignores_owned_partitions",
            STICKY,
            evenSplit,
            List.of(
                new MemberSpec(
                    "m-1", null, a, tps("t-a", 0, 1), DEFAULT_GENERATION, null, null),
                new MemberSpec(
                    "m-2", null, a, tps("t-a", 2, 3), DEFAULT_GENERATION, null, null))));

    // A member subscribed to nothing still appears in the group.
    List<MemberSpec> withEmpty = List.of(fresh("m-1", a), fresh("m-2", List.of()));
    cases.add(vector("empty_subscription_member", RANGE, evenSplit, withEmpty));
    cases.add(vector("empty_subscription_member", STICKY, evenSplit, withEmpty));

    // t-b exists in the cluster and nobody subscribes to it, so it is never assigned.
    Map<String, Integer> twoEqualTopics = topology("t-a", 4, "t-b", 4);
    cases.add(vector("unassigned_topic", RANGE, twoEqualTopics, twoFresh));
    cases.add(vector("unassigned_topic", STICKY, twoEqualTopics, twoFresh));

    cases.add(largeHeterogeneous());

    return cases;
  }

  /**
   * @param caseId the case name
   * @param assignor the wire name of the assignor to run
   * @param topics the topology
   * @param members the members
   * @return a case serialised at the default subscription version
   */
  private static CaseSpec vector(
      String caseId, String assignor, Map<String, Integer> topics, List<MemberSpec> members) {
    return new CaseSpec(caseId, assignor, topics, members, DEFAULT_SUBSCRIPTION_VERSION, null);
  }

  /**
   * @param member the member to copy
   * @param userData the retained payload it should report
   * @param generationId the generation that payload was written in
   * @return the same member carrying that userData and generation
   */
  private static MemberSpec withUserData(MemberSpec member, ByteBuffer userData, int generationId) {
    return new MemberSpec(
        member.memberId,
        member.groupInstanceId,
        member.topics,
        member.ownedPartitions,
        generationId,
        member.rackId,
        userData);
  }

  /**
   * Ten topics of a hundred partitions each, fifty members, each subscribed to a rolling window of
   * five topics - the shape the general path's cost is argued on.
   *
   * @return the case
   */
  private static CaseSpec largeHeterogeneous() {
    Map<String, Integer> topics = new LinkedHashMap<>();
    for (int i = 0; i < 10; i++) {
      topics.put("t-" + i, 100);
    }

    List<MemberSpec> members = new ArrayList<>();
    for (int n = 1; n <= 50; n++) {
      List<String> subscribed = new ArrayList<>();
      for (int offset = 0; offset < 5; offset++) {
        subscribed.add("t-" + ((n + offset) % 10));
      }
      members.add(fresh("m-" + n, subscribed));
    }

    return vector("large_heterogeneous", STICKY, topics, members);
  }
}
