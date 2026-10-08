import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

/**
 * Generates golden byte vectors for the APIs the KIP-848 consumer protocol, its admin surface and
 * the transaction protocol need, by running Apache Kafka's own generated message codecs
 * ({@code org.apache.kafka.common.message.*Data}) over pinned values.
 *
 * <p>Development-only: run via {@code scripts/protocol/javaVectors/runAssignors.sh messages}.
 * Never invoked from {@code npm test} or any committed build step — the {@code kafka-clients} jar
 * is not a project dependency, only a tool used once to produce fixtures that are then committed.
 *
 * <p>Each vector is written as {@code {"message","version","case","hex","fields"}} under
 * {@code src/protocol/requests/<api>/fixtures/java/}. {@code fields} restates the values this
 * class encoded, so a JavaScript spec can drive its own encoder from them and compare the bytes,
 * and can decode {@code hex} and compare the values — both directions from one file.
 *
 * <p>Every value here is a constant of this class. Two cases exist only to pin a one-byte
 * distinction that is invisible in the schema: a null versus an empty {@code SubscribedTopicNames}
 * (compact-array null {@code 0x00} versus empty {@code 0x01}), and a null versus a present
 * {@code Assignment} struct.
 *
 * <p>The transaction vectors cover the versions a client sends: AddPartitionsToTxn v2-v3 (the
 * schema reserves v4+ for brokers), AddOffsetsToTxn v2-v4, EndTxn v2-v5, TxnOffsetCommit v2-v5,
 * InitProducerId v3-v5 and OffsetFetch v7-v9. A field the schema gives no wire presence in a
 * version ({@code 5+}, {@code 3+}) is left out of that vector's {@code fields}: the writer drops an
 * {@code ignorable} one silently (the EndTxn response's ProducerId and ProducerEpoch below v5) and
 * throws on a non-default value of any other (TxnOffsetCommit's GenerationId, MemberId and
 * GroupInstanceId below v3), which is why {@code groupMetadata} starts at v3. The one exception is
 * a TxnOffsetCommit request, whose {@code fields} always name that triple, at its defaults below
 * v3, so one request shape serves all four versions.
 *
 * @see <a href="https://kafka.apache.org/43/design/protocol">Kafka protocol guide</a>
 * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/ConsumerGroupHeartbeatRequest.json">ConsumerGroupHeartbeatRequest.json</a>
 * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/ConsumerGroupDescribeResponse.json">ConsumerGroupDescribeResponse.json</a>
 * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/EndTxnResponse.json">EndTxnResponse.json</a>
 * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/TxnOffsetCommitRequest.json">TxnOffsetCommitRequest.json</a>
 */
public final class GenerateMessageVectors {

  private static final String GROUP_ID = "test-group";
  private static final String OTHER_GROUP_ID = "other-group";
  private static final String MEMBER_ID = "test-member";
  private static final String GROUP_INSTANCE_ID = "test-instance";
  private static final String CLIENT_ID = "test-client";
  private static final String CLIENT_HOST = "/127.0.0.1";
  private static final String TOPIC_NAME = "test-topic";
  private static final String TOPIC_ID = "01020304-0506-0708-090a-0b0c0d0e0f10";
  private static final String RACK_ID = "test-rack";
  private static final String SERVER_ASSIGNOR = "uniform";
  private static final String TOPIC_REGEX = "^test-.*";
  private static final String PROTOCOL_TYPE = "consumer";
  private static final String ASSIGNOR_NAME = "RoundRobinAssigner";

  private static final int REBALANCE_TIMEOUT_MS = 300000;
  private static final int HEARTBEAT_INTERVAL_MS = 500;
  private static final int MEMBER_EPOCH = 5;
  private static final int GROUP_EPOCH = 7;
  private static final int GENERATION_ID = 5;
  private static final long COMMITTED_OFFSET = 10L;
  private static final int COMMITTED_LEADER_EPOCH = 3;
  private static final int NO_AUTHORIZED_OPERATIONS = -2147483648;
  private static final int PARTITION_INDEX = 0;

  private static final String MEMBER_METADATA_HEX = "000300000001";
  private static final byte[] MEMBER_BYTES = {0, 3, 0, 0, 0, 1};

  /** FENCED_MEMBER_EPOCH and GROUP_ID_NOT_FOUND, the two codes the routing rules turn on. */
  private static final short FENCED_MEMBER_EPOCH = 110;
  private static final short GROUP_ID_NOT_FOUND = 69;

  /** TOPIC_AUTHORIZATION_FAILED, the per-group code the mixed-errors case pins. */
  private static final short TOPIC_AUTHORIZATION_FAILED = 29;

  /** Code 110's own message, so a spec that asserts on it is asserting something true. */
  private static final String FENCED_MEMBER_EPOCH_MESSAGE =
      "The member epoch is fenced by the group coordinator."
          + " The member must abandon all its partitions and rejoin.";

  /** Code 69's message, for both describe APIs' not-found cases. */
  private static final String GROUP_ID_NOT_FOUND_MESSAGE = "Group test-group not found.";

  private static final String TOPIC_AUTHORIZATION_FAILED_MESSAGE = "Topic authorization failed.";

  private static final List<Integer> PARTITIONS = Arrays.asList(0, 2);

  private static final String TRANSACTIONAL_ID = "test-transactional-id";
  private static final int TRANSACTION_TIMEOUT_MS = 30000;
  private static final long PRODUCER_ID = 1000L;
  private static final short PRODUCER_EPOCH = 3;

  /** The epoch an EndTxn v5 answer hands back after bumping {@link #PRODUCER_EPOCH} (KIP-890). */
  private static final short BUMPED_PRODUCER_EPOCH = 4;

  /** A non-zero throttle on every transaction-API response, so a decoder's KIP-219 split shows. */
  private static final int TXN_THROTTLE_TIME_MS = 25;

  private static final String COMMITTED_METADATA = "test-metadata";

  /** The error cases' codes: CONCURRENT_TRANSACTIONS, ILLEGAL_GENERATION and PRODUCER_FENCED. */
  private static final short CONCURRENT_TRANSACTIONS = 51;
  private static final short ILLEGAL_GENERATION = 22;
  private static final short PRODUCER_FENCED = 90;

  private GenerateMessageVectors() {}

  /** One vector: a message, the version to serialize it at, and the fields it was built from. */
  private static final class Vector {
    final String api;
    final String message;
    final short version;
    final String caseId;
    final Message data;
    final String fields;

    Vector(String api, String message, short version, String caseId, Message data, String fields) {
      this.api = api;
      this.message = message;
      this.version = version;
      this.caseId = caseId;
      this.data = data;
      this.fields = fields;
    }

    String fileName() {
      char[] name = message.toCharArray();
      name[0] = Character.toLowerCase(name[0]);
      return new String(name) + "-v" + version + "-" + caseId + ".json";
    }
  }

  /**
   * @param args {@code args[0]} is the repository root the fixtures are written under, matching
   *     the layout of {@code src/protocol/requests/<api>/fixtures/java/}.
   * @throws IOException if a fixture file cannot be written
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("usage: GenerateMessageVectors <output-dir>");
      System.exit(1);
    }

    Path outputDir = Paths.get(args[0]);
    int written = 0;

    for (Vector vector : vectors()) {
      Path dir = outputDir.resolve("src/protocol/requests/" + vector.api + "/fixtures/java");
      Files.createDirectories(dir);
      Path path = dir.resolve(vector.fileName());
      Files.writeString(path, document(vector), StandardCharsets.UTF_8);
      System.out.println("wrote " + path);
      written++;
    }

    System.out.println("vectors: " + written);
  }

  /** @return every vector this generator emits, in file order per API */
  private static List<Vector> vectors() {
    List<Vector> vectors = new ArrayList<>();
    vectors.addAll(consumerGroupHeartbeat());
    vectors.addAll(consumerGroupDescribe());
    vectors.addAll(listGroups());
    vectors.addAll(describeGroups());
    vectors.addAll(offsetCommit());
    vectors.addAll(offsetFetch());
    vectors.addAll(offsetFetchRequireStable());
    vectors.addAll(initProducerId());
    vectors.addAll(addPartitionsToTxn());
    vectors.addAll(addOffsetsToTxn());
    vectors.addAll(endTxn());
    vectors.addAll(txnOffsetCommit());
    return vectors;
  }

  /**
   * ConsumerGroupHeartbeat (68) v0-v1, request and response.
   *
   * <p>The v0 cases walk the send-if-changed states the client actually produces: a full join, a
   * bare identity-only heartbeat, an owned-partitions report, and both leave epochs. The v1 cases
   * add the regex field, including the two forms that carry meaning — a regex together with an
   * EMPTY name list, and the empty-string clear.
   *
   * @return the key-68 vectors
   */
  private static List<Vector> consumerGroupHeartbeat() {
    List<Vector> vectors = new ArrayList<>();
    String api = "consumerGroupHeartbeat";
    String request = "ConsumerGroupHeartbeatRequest";
    String response = "ConsumerGroupHeartbeatResponse";

    ConsumerGroupHeartbeatRequestData join =
        new ConsumerGroupHeartbeatRequestData()
            .setGroupId(GROUP_ID)
            .setMemberId(MEMBER_ID)
            .setMemberEpoch(0)
            .setRebalanceTimeoutMs(REBALANCE_TIMEOUT_MS)
            .setSubscribedTopicNames(Collections.singletonList(TOPIC_NAME))
            .setTopicPartitions(Collections.emptyList());
    vectors.add(
        new Vector(api, request, (short) 0, "join", join,
            obj(
                kv("groupId", str(GROUP_ID)),
                kv("memberId", str(MEMBER_ID)),
                kv("memberEpoch", "0"),
                kv("instanceId", "null"),
                kv("rackId", "null"),
                kv("rebalanceTimeoutMs", String.valueOf(REBALANCE_TIMEOUT_MS)),
                kv("subscribedTopicNames", strArray(Collections.singletonList(TOPIC_NAME))),
                kv("serverAssignor", "null"),
                kv("topicPartitions", "[]"))));

    // Everything the send-if-changed rule can omit is omitted: only the three always-sent fields
    // carry a value, and every nullable one takes its wire default.
    ConsumerGroupHeartbeatRequestData stable =
        new ConsumerGroupHeartbeatRequestData()
            .setGroupId(GROUP_ID)
            .setMemberId(MEMBER_ID)
            .setMemberEpoch(MEMBER_EPOCH)
            .setRebalanceTimeoutMs(-1);
    vectors.add(
        new Vector(api, request, (short) 0, "stableMinimal", stable,
            obj(
                kv("groupId", str(GROUP_ID)),
                kv("memberId", str(MEMBER_ID)),
                kv("memberEpoch", String.valueOf(MEMBER_EPOCH)),
                kv("instanceId", "null"),
                kv("rackId", "null"),
                kv("rebalanceTimeoutMs", "-1"),
                kv("subscribedTopicNames", "null"),
                kv("serverAssignor", "null"),
                kv("topicPartitions", "null"))));

    ConsumerGroupHeartbeatRequestData owned =
        new ConsumerGroupHeartbeatRequestData()
            .setGroupId(GROUP_ID)
            .setMemberId(MEMBER_ID)
            .setMemberEpoch(MEMBER_EPOCH)
            .setRebalanceTimeoutMs(-1)
            .setTopicPartitions(
                Collections.singletonList(
                    new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                        .setTopicId(topicId())
                        .setPartitions(PARTITIONS)));
    vectors.add(
        new Vector(api, request, (short) 0, "ownedPartitions", owned,
            obj(
                kv("groupId", str(GROUP_ID)),
                kv("memberId", str(MEMBER_ID)),
                kv("memberEpoch", String.valueOf(MEMBER_EPOCH)),
                kv("instanceId", "null"),
                kv("rackId", "null"),
                kv("rebalanceTimeoutMs", "-1"),
                kv("subscribedTopicNames", "null"),
                kv("serverAssignor", "null"),
                kv("topicPartitions",
                    array(obj(kv("topicId", str(TOPIC_ID)), kv("partitions", intArray(PARTITIONS))))))));

    ConsumerGroupHeartbeatRequestData leaveDynamic =
        new ConsumerGroupHeartbeatRequestData()
            .setGroupId(GROUP_ID)
            .setMemberId(MEMBER_ID)
            .setMemberEpoch(-1)
            .setRebalanceTimeoutMs(-1);
    vectors.add(
        new Vector(api, request, (short) 0, "leaveDynamic", leaveDynamic,
            obj(
                kv("groupId", str(GROUP_ID)),
                kv("memberId", str(MEMBER_ID)),
                kv("memberEpoch", "-1"),
                kv("instanceId", "null"),
                kv("rackId", "null"),
                kv("rebalanceTimeoutMs", "-1"),
                kv("subscribedTopicNames", "null"),
                kv("serverAssignor", "null"),
                kv("topicPartitions", "null"))));

    ConsumerGroupHeartbeatRequestData leaveStatic =
        new ConsumerGroupHeartbeatRequestData()
            .setGroupId(GROUP_ID)
            .setMemberId(MEMBER_ID)
            .setMemberEpoch(-2)
            .setInstanceId(GROUP_INSTANCE_ID)
            .setRebalanceTimeoutMs(-1);
    vectors.add(
        new Vector(api, request, (short) 0, "leaveStatic", leaveStatic,
            obj(
                kv("groupId", str(GROUP_ID)),
                kv("memberId", str(MEMBER_ID)),
                kv("memberEpoch", "-2"),
                kv("instanceId", str(GROUP_INSTANCE_ID)),
                kv("rackId", "null"),
                kv("rebalanceTimeoutMs", "-1"),
                kv("subscribedTopicNames", "null"),
                kv("serverAssignor", "null"),
                kv("topicPartitions", "null"))));

    ConsumerGroupHeartbeatRequestData staticJoin =
        new ConsumerGroupHeartbeatRequestData()
            .setGroupId(GROUP_ID)
            .setMemberId(MEMBER_ID)
            .setMemberEpoch(0)
            .setInstanceId(GROUP_INSTANCE_ID)
            .setRackId(RACK_ID)
            .setRebalanceTimeoutMs(REBALANCE_TIMEOUT_MS)
            .setSubscribedTopicNames(Collections.singletonList(TOPIC_NAME))
            .setServerAssignor(SERVER_ASSIGNOR)
            .setTopicPartitions(Collections.emptyList());
    vectors.add(
        new Vector(api, request, (short) 0, "staticJoinWithAssignor", staticJoin,
            obj(
                kv("groupId", str(GROUP_ID)),
                kv("memberId", str(MEMBER_ID)),
                kv("memberEpoch", "0"),
                kv("instanceId", str(GROUP_INSTANCE_ID)),
                kv("rackId", str(RACK_ID)),
                kv("rebalanceTimeoutMs", String.valueOf(REBALANCE_TIMEOUT_MS)),
                kv("subscribedTopicNames", strArray(Collections.singletonList(TOPIC_NAME))),
                kv("serverAssignor", str(SERVER_ASSIGNOR)),
                kv("topicPartitions", "[]"))));

    // v1 == v0 plus SubscribedTopicRegex. This case exists so a JS spec can prove the ONLY
    // difference between the two versions is that field.
    ConsumerGroupHeartbeatRequestData joinV1 =
        new ConsumerGroupHeartbeatRequestData()
            .setGroupId(GROUP_ID)
            .setMemberId(MEMBER_ID)
            .setMemberEpoch(0)
            .setRebalanceTimeoutMs(REBALANCE_TIMEOUT_MS)
            .setSubscribedTopicNames(Collections.singletonList(TOPIC_NAME))
            .setTopicPartitions(Collections.emptyList());
    vectors.add(
        new Vector(api, request, (short) 1, "join", joinV1,
            obj(
                kv("groupId", str(GROUP_ID)),
                kv("memberId", str(MEMBER_ID)),
                kv("memberEpoch", "0"),
                kv("instanceId", "null"),
                kv("rackId", "null"),
                kv("rebalanceTimeoutMs", String.valueOf(REBALANCE_TIMEOUT_MS)),
                kv("subscribedTopicNames", strArray(Collections.singletonList(TOPIC_NAME))),
                kv("subscribedTopicRegex", "null"),
                kv("serverAssignor", "null"),
                kv("topicPartitions", "[]"))));

    // The named regex vector: an EMPTY name list (compact array 0x01) beside a non-null regex.
    // A null list (0x00) would mean "unchanged since the last heartbeat", which on a joining
    // heartbeat means nothing at all.
    ConsumerGroupHeartbeatRequestData regexJoin =
        new ConsumerGroupHeartbeatRequestData()
            .setGroupId(GROUP_ID)
            .setMemberId(MEMBER_ID)
            .setMemberEpoch(0)
            .setRebalanceTimeoutMs(REBALANCE_TIMEOUT_MS)
            .setSubscribedTopicNames(Collections.emptyList())
            .setSubscribedTopicRegex(TOPIC_REGEX)
            .setTopicPartitions(Collections.emptyList());
    vectors.add(
        new Vector(api, request, (short) 1, "regexSubscription", regexJoin,
            obj(
                kv("groupId", str(GROUP_ID)),
                kv("memberId", str(MEMBER_ID)),
                kv("memberEpoch", "0"),
                kv("instanceId", "null"),
                kv("rackId", "null"),
                kv("rebalanceTimeoutMs", String.valueOf(REBALANCE_TIMEOUT_MS)),
                kv("subscribedTopicNames", "[]"),
                kv("subscribedTopicRegex", str(TOPIC_REGEX)),
                kv("serverAssignor", "null"),
                kv("topicPartitions", "[]"))));

    // The empty string clears a regex; null would mean "unchanged".
    ConsumerGroupHeartbeatRequestData regexClear =
        new ConsumerGroupHeartbeatRequestData()
            .setGroupId(GROUP_ID)
            .setMemberId(MEMBER_ID)
            .setMemberEpoch(MEMBER_EPOCH)
            .setRebalanceTimeoutMs(-1)
            .setSubscribedTopicNames(Collections.singletonList(TOPIC_NAME))
            .setSubscribedTopicRegex("");
    vectors.add(
        new Vector(api, request, (short) 1, "regexClear", regexClear,
            obj(
                kv("groupId", str(GROUP_ID)),
                kv("memberId", str(MEMBER_ID)),
                kv("memberEpoch", String.valueOf(MEMBER_EPOCH)),
                kv("instanceId", "null"),
                kv("rackId", "null"),
                kv("rebalanceTimeoutMs", "-1"),
                kv("subscribedTopicNames", strArray(Collections.singletonList(TOPIC_NAME))),
                kv("subscribedTopicRegex", str("")),
                kv("serverAssignor", "null"),
                kv("topicPartitions", "null"))));

    // The four response cases are emitted at BOTH versions. v1's response body is identical to
    // v0's, and the pair of files is what lets the v1 specs be the same two files against the
    // `-v1-` vectors rather than a version-special-cased subset.
    for (short version : new short[] {0, 1}) {
      vectors.addAll(consumerGroupHeartbeatResponses(api, response, version));
    }

    return vectors;
  }

  /**
   * The four ConsumerGroupHeartbeatResponse cases, at one version.
   *
   * <p>Three of them exist as a set rather than individually: an assignment with one topic, an
   * EMPTY assignment, and a null one. The first two differ only inside the struct; the second and
   * third differ only by the struct's one-byte presence marker. "You own nothing" and "there is
   * nothing new to reconcile toward" are different answers, and this is the only place the
   * distinction is visible in bytes.
   *
   * @param api the fixtures directory name
   * @param message the Apache message name
   * @param version 0 or 1
   * @return the four vectors
   */
  private static List<Vector> consumerGroupHeartbeatResponses(
      String api, String message, short version) {
    List<Vector> vectors = new ArrayList<>();

    String assignmentFields =
        obj(kv("topicPartitions",
            array(obj(kv("topicId", str(TOPIC_ID)), kv("partitions", intArray(PARTITIONS))))));

    ConsumerGroupHeartbeatResponseData assigned =
        new ConsumerGroupHeartbeatResponseData()
            .setThrottleTimeMs(0)
            .setErrorCode((short) 0)
            .setMemberId(MEMBER_ID)
            .setMemberEpoch(MEMBER_EPOCH)
            .setHeartbeatIntervalMs(HEARTBEAT_INTERVAL_MS)
            .setAssignment(
                new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(
                        Collections.singletonList(
                            new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId())
                                .setPartitions(PARTITIONS))));
    vectors.add(
        new Vector(api, message, version, "assignment", assigned,
            obj(
                kv("throttleTime", "0"),
                kv("errorCode", "0"),
                kv("errorMessage", "null"),
                kv("memberId", str(MEMBER_ID)),
                kv("memberEpoch", String.valueOf(MEMBER_EPOCH)),
                kv("heartbeatIntervalMs", String.valueOf(HEARTBEAT_INTERVAL_MS)),
                kv("assignment", assignmentFields))));

    // A PRESENT assignment that holds no partitions - the coordinator's way of saying "you own
    // nothing now". The reconciliation treats it as a target; a null one it does not.
    ConsumerGroupHeartbeatResponseData emptyAssignment =
        new ConsumerGroupHeartbeatResponseData()
            .setThrottleTimeMs(0)
            .setErrorCode((short) 0)
            .setMemberId(MEMBER_ID)
            .setMemberEpoch(MEMBER_EPOCH)
            .setHeartbeatIntervalMs(HEARTBEAT_INTERVAL_MS)
            .setAssignment(
                new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(Collections.emptyList()));
    vectors.add(
        new Vector(api, message, version, "emptyAssignment", emptyAssignment,
            obj(
                kv("throttleTime", "0"),
                kv("errorCode", "0"),
                kv("errorMessage", "null"),
                kv("memberId", str(MEMBER_ID)),
                kv("memberEpoch", String.valueOf(MEMBER_EPOCH)),
                kv("heartbeatIntervalMs", String.valueOf(HEARTBEAT_INTERVAL_MS)),
                kv("assignment", obj(kv("topicPartitions", "[]"))))));

    // The null-Assignment twin of the case above: the two differ by the struct's presence marker
    // and by nothing else, which is what makes the marker's encoding evidence rather than belief.
    ConsumerGroupHeartbeatResponseData noAssignment =
        new ConsumerGroupHeartbeatResponseData()
            .setThrottleTimeMs(0)
            .setErrorCode((short) 0)
            .setMemberId(MEMBER_ID)
            .setMemberEpoch(MEMBER_EPOCH)
            .setHeartbeatIntervalMs(HEARTBEAT_INTERVAL_MS)
            .setAssignment(null);
    vectors.add(
        new Vector(api, message, version, "nullAssignment", noAssignment,
            obj(
                kv("throttleTime", "0"),
                kv("errorCode", "0"),
                kv("errorMessage", "null"),
                kv("memberId", str(MEMBER_ID)),
                kv("memberEpoch", String.valueOf(MEMBER_EPOCH)),
                kv("heartbeatIntervalMs", String.valueOf(HEARTBEAT_INTERVAL_MS)),
                kv("assignment", "null"))));

    // The error message is code 110's own text, taken from Errors.java rather than invented, so a
    // spec can assert the decoded message against the vector and still be asserting something
    // true about the code it pins.
    // @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java
    ConsumerGroupHeartbeatResponseData fenced =
        new ConsumerGroupHeartbeatResponseData()
            .setThrottleTimeMs(0)
            .setErrorCode(FENCED_MEMBER_EPOCH)
            .setErrorMessage(FENCED_MEMBER_EPOCH_MESSAGE)
            .setMemberId(null)
            .setMemberEpoch(0)
            .setHeartbeatIntervalMs(0)
            .setAssignment(null);
    vectors.add(
        new Vector(api, message, version, "fencedError", fenced,
            obj(
                kv("throttleTime", "0"),
                kv("errorCode", String.valueOf(FENCED_MEMBER_EPOCH)),
                kv("errorMessage", str(FENCED_MEMBER_EPOCH_MESSAGE)),
                kv("memberId", "null"),
                kv("memberEpoch", "0"),
                kv("heartbeatIntervalMs", "0"),
                kv("assignment", "null"))));

    return vectors;
  }

  /**
   * ConsumerGroupDescribe (69) v0-v1, request and response.
   *
   * <p>v1's REQUEST is byte-identical to v0's - the version exists for the response's MemberType
   * field (KIP-1099) - so the two request vectors carry different values rather than different
   * shapes, and the identity is what the codec spec asserts.
   *
   * @return the key-69 vectors
   */
  private static List<Vector> consumerGroupDescribe() {
    List<Vector> vectors = new ArrayList<>();
    String api = "consumerGroupDescribe";
    String request = "ConsumerGroupDescribeRequest";
    String response = "ConsumerGroupDescribeResponse";

    ConsumerGroupDescribeRequestData twoGroups =
        new ConsumerGroupDescribeRequestData()
            .setGroupIds(Arrays.asList(GROUP_ID, OTHER_GROUP_ID))
            .setIncludeAuthorizedOperations(false);
    vectors.add(
        new Vector(api, request, (short) 0, "twoGroups", twoGroups,
            obj(
                kv("groupIds", strArray(Arrays.asList(GROUP_ID, OTHER_GROUP_ID))),
                kv("includeAuthorizedOperations", "false"))));

    ConsumerGroupDescribeRequestData authorized =
        new ConsumerGroupDescribeRequestData()
            .setGroupIds(Collections.singletonList(GROUP_ID))
            .setIncludeAuthorizedOperations(true);
    vectors.add(
        new Vector(api, request, (short) 1, "authorizedOperations", authorized,
            obj(
                kv("groupIds", strArray(Collections.singletonList(GROUP_ID))),
                kv("includeAuthorizedOperations", "true"))));

    for (short version : new short[] {0, 1}) {
      String caseId = version == 0 ? "describedGroup" : "memberType";
      byte memberType = version == 0 ? (byte) -1 : (byte) 1;

      ConsumerGroupDescribeResponseData.Member member =
          new ConsumerGroupDescribeResponseData.Member()
              .setMemberId(MEMBER_ID)
              .setInstanceId(GROUP_INSTANCE_ID)
              .setRackId(null)
              .setMemberEpoch(MEMBER_EPOCH)
              .setClientId(CLIENT_ID)
              .setClientHost(CLIENT_HOST)
              .setSubscribedTopicNames(Collections.singletonList(TOPIC_NAME))
              .setSubscribedTopicRegex(null)
              .setAssignment(describeAssignment())
              .setTargetAssignment(describeAssignment());

      if (version >= 1) {
        member.setMemberType(memberType);
      }

      ConsumerGroupDescribeResponseData described =
          new ConsumerGroupDescribeResponseData()
              .setThrottleTimeMs(0)
              .setGroups(
                  Collections.singletonList(
                      new ConsumerGroupDescribeResponseData.DescribedGroup()
                          .setErrorCode((short) 0)
                          .setErrorMessage(null)
                          .setGroupId(GROUP_ID)
                          .setGroupState("Stable")
                          .setGroupEpoch(GROUP_EPOCH)
                          .setAssignmentEpoch(GROUP_EPOCH)
                          .setAssignorName(SERVER_ASSIGNOR)
                          .setMembers(Collections.singletonList(member))
                          .setAuthorizedOperations(NO_AUTHORIZED_OPERATIONS)));

      String memberFields =
          obj(
              kv("memberId", str(MEMBER_ID)),
              kv("instanceId", str(GROUP_INSTANCE_ID)),
              kv("rackId", "null"),
              kv("memberEpoch", String.valueOf(MEMBER_EPOCH)),
              kv("clientId", str(CLIENT_ID)),
              kv("clientHost", str(CLIENT_HOST)),
              kv("subscribedTopicNames", strArray(Collections.singletonList(TOPIC_NAME))),
              kv("subscribedTopicRegex", "null"),
              kv("assignment", describeAssignmentFields()),
              kv("targetAssignment", describeAssignmentFields()),
              // Null at v0, where the field does not exist at all: "the broker did not say" and
              // "the broker said unknown" (-1) are different answers.
              kv("memberType", version == 0 ? "null" : String.valueOf(memberType)));

      vectors.add(
          new Vector(api, response, version, caseId, described,
              obj(
                  kv("throttleTime", "0"),
                  kv("groups",
                      array(
                          obj(
                              kv("errorCode", "0"),
                              kv("errorMessage", "null"),
                              kv("groupId", str(GROUP_ID)),
                              kv("groupState", str("Stable")),
                              kv("groupEpoch", String.valueOf(GROUP_EPOCH)),
                              kv("assignmentEpoch", String.valueOf(GROUP_EPOCH)),
                              kv("assignorName", str(SERVER_ASSIGNOR)),
                              kv("members", array(memberFields)),
                              kv("authorizedOperations",
                                  String.valueOf(NO_AUTHORIZED_OPERATIONS))))))));
    }

    // The routing signal: this API answers GROUP_ID_NOT_FOUND for a group that is not a
    // consumer-protocol group, and the caller asks DescribeGroups for that id instead. Every
    // other field takes the empty form the coordinator sends beside the error.
    ConsumerGroupDescribeResponseData notFound =
        new ConsumerGroupDescribeResponseData()
            .setThrottleTimeMs(0)
            .setGroups(
                Collections.singletonList(
                    new ConsumerGroupDescribeResponseData.DescribedGroup()
                        .setErrorCode(GROUP_ID_NOT_FOUND)
                        .setErrorMessage(GROUP_ID_NOT_FOUND_MESSAGE)
                        .setGroupId(GROUP_ID)
                        .setGroupState("")
                        .setGroupEpoch(0)
                        .setAssignmentEpoch(0)
                        .setAssignorName("")
                        .setMembers(Collections.emptyList())
                        .setAuthorizedOperations(NO_AUTHORIZED_OPERATIONS)));
    vectors.add(
        new Vector(api, response, (short) 0, "groupIdNotFound", notFound,
            obj(
                kv("throttleTime", "0"),
                kv("groups",
                    array(
                        obj(
                            kv("errorCode", String.valueOf(GROUP_ID_NOT_FOUND)),
                            kv("errorMessage", str(GROUP_ID_NOT_FOUND_MESSAGE)),
                            kv("groupId", str(GROUP_ID)),
                            kv("groupState", str("")),
                            kv("groupEpoch", "0"),
                            kv("assignmentEpoch", "0"),
                            kv("assignorName", str("")),
                            kv("members", "[]"),
                            kv("authorizedOperations",
                                String.valueOf(NO_AUTHORIZED_OPERATIONS))))))));

    // One group id that is authorized and one that is not, in one response: the per-group error
    // contract has to hold when only SOME rows carry a code.
    ConsumerGroupDescribeResponseData mixed =
        new ConsumerGroupDescribeResponseData()
            .setThrottleTimeMs(0)
            .setGroups(
                Arrays.asList(
                    new ConsumerGroupDescribeResponseData.DescribedGroup()
                        .setErrorCode((short) 0)
                        .setErrorMessage(null)
                        .setGroupId(GROUP_ID)
                        .setGroupState("Empty")
                        .setGroupEpoch(GROUP_EPOCH)
                        .setAssignmentEpoch(GROUP_EPOCH)
                        .setAssignorName(SERVER_ASSIGNOR)
                        .setMembers(Collections.emptyList())
                        .setAuthorizedOperations(NO_AUTHORIZED_OPERATIONS),
                    new ConsumerGroupDescribeResponseData.DescribedGroup()
                        .setErrorCode(TOPIC_AUTHORIZATION_FAILED)
                        .setErrorMessage(TOPIC_AUTHORIZATION_FAILED_MESSAGE)
                        .setGroupId(OTHER_GROUP_ID)
                        .setGroupState("")
                        .setGroupEpoch(0)
                        .setAssignmentEpoch(0)
                        .setAssignorName("")
                        .setMembers(Collections.emptyList())
                        .setAuthorizedOperations(NO_AUTHORIZED_OPERATIONS)));
    vectors.add(
        new Vector(api, response, (short) 0, "mixedErrors", mixed,
            obj(
                kv("throttleTime", "0"),
                kv("groups",
                    array(
                        obj(
                            kv("errorCode", "0"),
                            kv("errorMessage", "null"),
                            kv("groupId", str(GROUP_ID)),
                            kv("groupState", str("Empty")),
                            kv("groupEpoch", String.valueOf(GROUP_EPOCH)),
                            kv("assignmentEpoch", String.valueOf(GROUP_EPOCH)),
                            kv("assignorName", str(SERVER_ASSIGNOR)),
                            kv("members", "[]"),
                            kv("authorizedOperations",
                                String.valueOf(NO_AUTHORIZED_OPERATIONS))),
                        obj(
                            kv("errorCode", String.valueOf(TOPIC_AUTHORIZATION_FAILED)),
                            kv("errorMessage", str(TOPIC_AUTHORIZATION_FAILED_MESSAGE)),
                            kv("groupId", str(OTHER_GROUP_ID)),
                            kv("groupState", str("")),
                            kv("groupEpoch", "0"),
                            kv("assignmentEpoch", "0"),
                            kv("assignorName", str("")),
                            kv("members", "[]"),
                            kv("authorizedOperations",
                                String.valueOf(NO_AUTHORIZED_OPERATIONS))))))));

    return vectors;
  }

  /** @return one assignment carrying a single topic, by id AND by name */
  private static ConsumerGroupDescribeResponseData.Assignment describeAssignment() {
    return new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(
            Collections.singletonList(
                new ConsumerGroupDescribeResponseData.TopicPartitions()
                    .setTopicId(topicId())
                    .setTopicName(TOPIC_NAME)
                    .setPartitions(PARTITIONS)));
  }

  /** @return the JSON for {@link #describeAssignment()} */
  private static String describeAssignmentFields() {
    return obj(
        kv("topicPartitions",
            array(
                obj(
                    kv("topicId", str(TOPIC_ID)),
                    kv("topicName", str(TOPIC_NAME)),
                    kv("partitions", intArray(PARTITIONS))))));
  }

  /**
   * ListGroups (16) v3-v5, request and response.
   *
   * <p>v3 is the first flexible version and has no request field at all, so its whole body is one
   * tag buffer. v4 adds StatesFilter (KIP-518) and v5 adds TypesFilter (KIP-848). Every version
   * emits BOTH a filtered and an unfiltered request: neither filter is nullable, so "no filter"
   * is the empty compact array and not the null one, and that is a one-byte difference.
   *
   * @return the key-16 vectors
   */
  private static List<Vector> listGroups() {
    List<Vector> vectors = new ArrayList<>();
    String api = "listGroups";
    String request = "ListGroupsRequest";
    String response = "ListGroupsResponse";

    List<String> stateFilter = Collections.singletonList("Stable");
    List<String> typeFilter = Collections.singletonList("Consumer");

    vectors.add(
        new Vector(api, request, (short) 3, "noFilters", new ListGroupsRequestData(), obj()));

    vectors.add(
        new Vector(api, request, (short) 4, "noFilters",
            new ListGroupsRequestData().setStatesFilter(Collections.emptyList()),
            obj(kv("states", "[]"))));

    vectors.add(
        new Vector(api, request, (short) 4, "statesFilter",
            new ListGroupsRequestData().setStatesFilter(stateFilter),
            obj(kv("states", strArray(stateFilter)))));

    vectors.add(
        new Vector(api, request, (short) 5, "noFilters",
            new ListGroupsRequestData()
                .setStatesFilter(Collections.emptyList())
                .setTypesFilter(Collections.emptyList()),
            obj(kv("states", "[]"), kv("types", "[]"))));

    vectors.add(
        new Vector(api, request, (short) 5, "statesAndTypes",
            new ListGroupsRequestData().setStatesFilter(stateFilter).setTypesFilter(typeFilter),
            obj(kv("states", strArray(stateFilter)), kv("types", strArray(typeFilter)))));

    for (short version : new short[] {3, 4, 5}) {
      ListGroupsResponseData.ListedGroup listed =
          new ListGroupsResponseData.ListedGroup()
              .setGroupId(GROUP_ID)
              .setProtocolType(PROTOCOL_TYPE);

      if (version >= 4) {
        listed.setGroupState("Stable");
      }

      if (version >= 5) {
        listed.setGroupType("Consumer");
      }

      ListGroupsResponseData listGroups =
          new ListGroupsResponseData()
              .setThrottleTimeMs(0)
              .setErrorCode((short) 0)
              .setGroups(Collections.singletonList(listed));

      vectors.add(
          new Vector(api, response, version, "groups", listGroups,
              obj(
                  kv("throttleTime", "0"),
                  kv("errorCode", "0"),
                  kv("groups",
                      array(
                          obj(
                              kv("groupId", str(GROUP_ID)),
                              kv("protocolType", str(PROTOCOL_TYPE)),
                              // Reported as the empty string, never absent, below the version
                              // that carries the field: one row shape at every version.
                              kv("groupState", version >= 4 ? str("Stable") : str("")),
                              kv("groupType", version >= 5 ? str("Consumer") : str(""))))))));
    }

    return vectors;
  }

  /**
   * DescribeGroups (15) v3-v6, request and response.
   *
   * <p>v3 adds IncludeAuthorizedOperations, v4 adds GroupInstanceId to each member, v5 is the
   * first flexible version and v6 adds a per-group ErrorMessage together with GROUP_ID_NOT_FOUND
   * for a group that is not there (KIP-1043). v4's request is byte-identical to v3's and v6's to
   * v5's; only the framing moves, at v5.
   *
   * @return the key-15 vectors
   */
  private static List<Vector> describeGroups() {
    List<Vector> vectors = new ArrayList<>();
    String api = "describeGroups";
    String request = "DescribeGroupsRequest";
    String response = "DescribeGroupsResponse";

    for (short version : new short[] {3, 4, 5, 6}) {
      DescribeGroupsRequestData oneGroup =
          new DescribeGroupsRequestData()
              .setGroups(Collections.singletonList(GROUP_ID))
              .setIncludeAuthorizedOperations(true);
      vectors.add(
          new Vector(api, request, version, "oneGroup", oneGroup,
              obj(
                  kv("groupIds", strArray(Collections.singletonList(GROUP_ID))),
                  kv("includeAuthorizedOperations", "true"))));

      DescribeGroupsResponseData.DescribedGroupMember member =
          new DescribeGroupsResponseData.DescribedGroupMember()
              .setMemberId(MEMBER_ID)
              .setClientId(CLIENT_ID)
              .setClientHost(CLIENT_HOST)
              .setMemberMetadata(MEMBER_BYTES)
              .setMemberAssignment(MEMBER_BYTES);

      if (version >= 4) {
        member.setGroupInstanceId(GROUP_INSTANCE_ID);
      }

      DescribeGroupsResponseData.DescribedGroup group =
          new DescribeGroupsResponseData.DescribedGroup()
              .setErrorCode((short) 0)
              .setGroupId(GROUP_ID)
              .setGroupState("Stable")
              .setProtocolType(PROTOCOL_TYPE)
              .setProtocolData(ASSIGNOR_NAME)
              .setMembers(Collections.singletonList(member))
              .setAuthorizedOperations(NO_AUTHORIZED_OPERATIONS);

      if (version >= 6) {
        group.setErrorMessage(null);
      }

      DescribeGroupsResponseData described =
          new DescribeGroupsResponseData()
              .setThrottleTimeMs(0)
              .setGroups(Collections.singletonList(group));

      // `state` and `protocol` are this client's 2.2.4 key names for GroupState and ProtocolData;
      // the fixture speaks the decoder's vocabulary, not the schema's, so a spec can compare a
      // decoded row with `fields` directly.
      String memberFields =
          obj(
              kv("memberId", str(MEMBER_ID)),
              kv("groupInstanceId", version >= 4 ? str(GROUP_INSTANCE_ID) : "null"),
              kv("clientId", str(CLIENT_ID)),
              kv("clientHost", str(CLIENT_HOST)),
              kv("memberMetadata", str(MEMBER_METADATA_HEX)),
              kv("memberAssignment", str(MEMBER_METADATA_HEX)));

      vectors.add(
          new Vector(api, response, version, "describedGroup", described,
              obj(
                  kv("throttleTime", "0"),
                  kv("groups",
                      array(
                          obj(
                              kv("errorCode", "0"),
                              kv("errorMessage", "null"),
                              kv("groupId", str(GROUP_ID)),
                              kv("state", str("Stable")),
                              kv("protocolType", str(PROTOCOL_TYPE)),
                              kv("protocol", str(ASSIGNOR_NAME)),
                              kv("members", array(memberFields)),
                              kv("authorizedOperations",
                                  String.valueOf(NO_AUTHORIZED_OPERATIONS))))))));
    }

    // v6 only: the code and the message a 4.x broker sends for a group that does not exist. Below
    // v6 the same question is answered with a `Dead` row and no error at all.
    DescribeGroupsResponseData notFound =
        new DescribeGroupsResponseData()
            .setThrottleTimeMs(0)
            .setGroups(
                Collections.singletonList(
                    new DescribeGroupsResponseData.DescribedGroup()
                        .setErrorCode(GROUP_ID_NOT_FOUND)
                        .setErrorMessage(GROUP_ID_NOT_FOUND_MESSAGE)
                        .setGroupId(GROUP_ID)
                        .setGroupState("Dead")
                        .setProtocolType("")
                        .setProtocolData("")
                        .setMembers(Collections.emptyList())
                        .setAuthorizedOperations(NO_AUTHORIZED_OPERATIONS)));
    vectors.add(
        new Vector(api, response, (short) 6, "groupIdNotFound", notFound,
            obj(
                kv("throttleTime", "0"),
                kv("groups",
                    array(
                        obj(
                            kv("errorCode", String.valueOf(GROUP_ID_NOT_FOUND)),
                            kv("errorMessage", str(GROUP_ID_NOT_FOUND_MESSAGE)),
                            kv("groupId", str(GROUP_ID)),
                            kv("state", str("Dead")),
                            kv("protocolType", str("")),
                            kv("protocol", str("")),
                            kv("members", "[]"),
                            kv("authorizedOperations",
                                String.valueOf(NO_AUTHORIZED_OPERATIONS))))))));

    return vectors;
  }

  /**
   * OffsetCommit (8) v9, request and response.
   *
   * <p>Every value here is the one {@code GenerateVectors.java} uses for v6-v8 - same topic, same
   * partition, same offset, same committed leader epoch (-1, the value this client always sends),
   * same group, generation, member and instance. That is what makes "v9's request is byte-identical
   * to v8's" provable against a vector captured in an earlier release rather than against this
   * repo's own v8 encoder.
   *
   * @return the key-8 vectors
   */
  private static List<Vector> offsetCommit() {
    List<Vector> vectors = new ArrayList<>();
    String api = "offsetCommit";

    OffsetCommitRequestData commit =
        new OffsetCommitRequestData()
            .setGroupId(GROUP_ID)
            .setGenerationIdOrMemberEpoch(GENERATION_ID)
            .setMemberId(MEMBER_ID)
            .setGroupInstanceId(GROUP_INSTANCE_ID)
            .setTopics(
                Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName(TOPIC_NAME)
                        .setPartitions(
                            Collections.singletonList(
                                new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                    .setPartitionIndex(PARTITION_INDEX)
                                    .setCommittedOffset(COMMITTED_OFFSET)
                                    .setCommittedLeaderEpoch(-1)
                                    .setCommittedMetadata(null)))));
    vectors.add(
        new Vector(api, "OffsetCommitRequest", (short) 9, "basic", commit,
            obj(
                kv("groupId", str(GROUP_ID)),
                kv("groupGenerationId", String.valueOf(GENERATION_ID)),
                kv("memberId", str(MEMBER_ID)),
                kv("groupInstanceId", str(GROUP_INSTANCE_ID)),
                kv("topics",
                    array(
                        obj(
                            kv("topic", str(TOPIC_NAME)),
                            kv("partitions",
                                array(
                                    obj(
                                        kv("partition", String.valueOf(PARTITION_INDEX)),
                                        kv("offset", str(String.valueOf(COMMITTED_OFFSET))),
                                        kv("leaderEpoch", "-1"),
                                        kv("metadata", "null"))))))))));

    OffsetCommitResponseData committed =
        new OffsetCommitResponseData()
            .setThrottleTimeMs(0)
            .setTopics(
                Collections.singletonList(
                    new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName(TOPIC_NAME)
                        .setPartitions(
                            Collections.singletonList(
                                new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                    .setPartitionIndex(PARTITION_INDEX)
                                    .setErrorCode((short) 0)))));
    vectors.add(
        new Vector(api, "OffsetCommitResponse", (short) 9, "basic", committed,
            obj(
                kv("throttleTime", "0"),
                kv("topics",
                    array(
                        obj(
                            kv("topic", str(TOPIC_NAME)),
                            kv("partitions",
                                array(
                                    obj(
                                        kv("partition", String.valueOf(PARTITION_INDEX)),
                                        kv("errorCode", "0"))))))))));

    return vectors;
  }

  /**
   * OffsetFetch (9) v9, request and response.
   *
   * <p>Two request cases, and the difference between them is the whole version: one carries a
   * member id and epoch, the other carries their wire defaults (null and -1), which is what a
   * classic member and an admin client mean and what v8 said by not having the fields.
   *
   * @return the key-9 vectors
   */
  private static List<Vector> offsetFetch() {
    List<Vector> vectors = new ArrayList<>();
    String api = "offsetFetch";
    String request = "OffsetFetchRequest";

    OffsetFetchRequestData.OffsetFetchRequestTopics topics =
        new OffsetFetchRequestData.OffsetFetchRequestTopics()
            .setName(TOPIC_NAME)
            .setPartitionIndexes(Collections.singletonList(PARTITION_INDEX));

    OffsetFetchRequestData withMember =
        new OffsetFetchRequestData()
            .setGroups(
                Collections.singletonList(
                    new OffsetFetchRequestData.OffsetFetchRequestGroup()
                        .setGroupId(GROUP_ID)
                        .setMemberId(MEMBER_ID)
                        .setMemberEpoch(MEMBER_EPOCH)
                        .setTopics(Collections.singletonList(topics))))
            .setRequireStable(false);
    vectors.add(
        new Vector(api, request, (short) 9, "memberFields", withMember,
            obj(
                kv("groupId", str(GROUP_ID)),
                kv("memberId", str(MEMBER_ID)),
                kv("memberEpoch", String.valueOf(MEMBER_EPOCH)),
                kv("topics",
                    array(
                        obj(
                            kv("topic", str(TOPIC_NAME)),
                            kv("partitions",
                                array(obj(kv("partition",
                                    String.valueOf(PARTITION_INDEX)))))))))));

    // A null Topics list asks for every topic in the group, exactly as at v8.
    OffsetFetchRequestData withoutMember =
        new OffsetFetchRequestData()
            .setGroups(
                Collections.singletonList(
                    new OffsetFetchRequestData.OffsetFetchRequestGroup()
                        .setGroupId(GROUP_ID)
                        .setMemberId(null)
                        .setMemberEpoch(-1)
                        .setTopics(null)))
            .setRequireStable(false);
    vectors.add(
        new Vector(api, request, (short) 9, "nullMember", withoutMember,
            obj(
                kv("groupId", str(GROUP_ID)),
                kv("memberId", "null"),
                kv("memberEpoch", "-1"),
                kv("topics", "null"))));

    OffsetFetchResponseData fetched =
        new OffsetFetchResponseData()
            .setThrottleTimeMs(0)
            .setGroups(
                Collections.singletonList(
                    new OffsetFetchResponseData.OffsetFetchResponseGroup()
                        .setGroupId(GROUP_ID)
                        .setErrorCode((short) 0)
                        .setTopics(
                            Collections.singletonList(
                                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                                    .setName(TOPIC_NAME)
                                    .setPartitions(
                                        Collections.singletonList(
                                            new OffsetFetchResponseData
                                                    .OffsetFetchResponsePartitions()
                                                .setPartitionIndex(PARTITION_INDEX)
                                                .setCommittedOffset(COMMITTED_OFFSET)
                                                .setCommittedLeaderEpoch(COMMITTED_LEADER_EPOCH)
                                                .setMetadata(null)
                                                .setErrorCode((short) 0)))))));
    vectors.add(
        new Vector(api, "OffsetFetchResponse", (short) 9, "basic", fetched,
            obj(
                kv("throttleTime", "0"),
                kv("groupId", str(GROUP_ID)),
                kv("errorCode", "0"),
                kv("topics",
                    array(
                        obj(
                            kv("topic", str(TOPIC_NAME)),
                            kv("partitions",
                                array(
                                    obj(
                                        kv("partition", String.valueOf(PARTITION_INDEX)),
                                        kv("offset", str(String.valueOf(COMMITTED_OFFSET))),
                                        kv("leaderEpoch",
                                            String.valueOf(COMMITTED_LEADER_EPOCH)),
                                        kv("metadata", "null"),
                                        kv("errorCode", "0"))))))))));

    return vectors;
  }

  /**
   * OffsetFetch (9) v7-v9 requests, once with RequireStable and once without (KIP-447). The two
   * of a version differ in that one boolean, the last byte before the request's tag buffer. v7
   * names one group at the top level; v8 and v9 carry a one-element Groups list, and v9 its
   * member fields at their wire defaults - what a classic member and an admin client send.
   *
   * @return the vectors
   */
  private static List<Vector> offsetFetchRequireStable() {
    List<Vector> vectors = new ArrayList<>();
    String api = "offsetFetch";

    for (short version : new short[] {7, 8, 9}) {
      for (boolean requireStable : new boolean[] {true, false}) {
        OffsetFetchRequestData data;
        if (version == 7) {
          data =
              new OffsetFetchRequestData()
                  .setGroupId(GROUP_ID)
                  .setTopics(
                      Collections.singletonList(
                          new OffsetFetchRequestData.OffsetFetchRequestTopic()
                              .setName(TOPIC_NAME)
                              .setPartitionIndexes(Collections.singletonList(PARTITION_INDEX))))
                  .setRequireStable(requireStable);
        } else {
          data =
              new OffsetFetchRequestData()
                  .setGroups(
                      Collections.singletonList(
                          new OffsetFetchRequestData.OffsetFetchRequestGroup()
                              .setGroupId(GROUP_ID)
                              .setMemberId(null)
                              .setMemberEpoch(-1)
                              .setTopics(
                                  Collections.singletonList(
                                      new OffsetFetchRequestData.OffsetFetchRequestTopics()
                                          .setName(TOPIC_NAME)
                                          .setPartitionIndexes(
                                              Collections.singletonList(PARTITION_INDEX))))))
                  .setRequireStable(requireStable);
        }

        String topics =
            array(
                obj(
                    kv("topic", str(TOPIC_NAME)),
                    kv("partitions", array(obj(kv("partition", String.valueOf(PARTITION_INDEX)))))));
        String fields =
            version == 9
                ? obj(
                    kv("groupId", str(GROUP_ID)),
                    kv("memberId", "null"),
                    kv("memberEpoch", "-1"),
                    kv("topics", topics),
                    kv("requireStable", String.valueOf(requireStable)))
                : obj(
                    kv("groupId", str(GROUP_ID)),
                    kv("topics", topics),
                    kv("requireStable", String.valueOf(requireStable)));

        vectors.add(
            new Vector(api, "OffsetFetchRequest", version,
                requireStable ? "requireStable" : "notRequireStable", data, fields));
      }
    }

    return vectors;
  }

  /**
   * InitProducerId (22) v3-v5 requests: a first initialization, which carries the -1/-1 defaults,
   * a KIP-360 re-initialization, which carries the producer id and epoch the client holds, and the
   * first initialization of a producer with no transactional id - every default producer's first
   * InitProducerId once idempotence is the default - whose TransactionalId is the null compact
   * string, the single byte 0x00. The first two differ in the ten bytes of producer id and epoch;
   * the third differs from the first in the TransactionalId alone.
   *
   * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/InitProducerIdRequest.json">InitProducerIdRequest.json</a>
   * @return the vectors
   */
  private static List<Vector> initProducerId() {
    List<Vector> vectors = new ArrayList<>();
    String api = "initProducerId";

    for (short version : new short[] {3, 4, 5}) {
      vectors.add(
          initProducerIdRequest(api, version, "firstInit", TRANSACTIONAL_ID, -1L, (short) -1));
      vectors.add(
          initProducerIdRequest(
              api, version, "reinit", TRANSACTIONAL_ID, PRODUCER_ID, PRODUCER_EPOCH));
      vectors.add(
          initProducerIdRequest(api, version, "nullTransactionalId", null, -1L, (short) -1));
    }

    return vectors;
  }

  /**
   * One InitProducerId request vector.
   *
   * @param api the fixtures directory name
   * @param version 3, 4 or 5
   * @param caseId the case name, which is also the file name's last part
   * @param transactionalId the transactional id, or null for a producer that is idempotent but not
   *     transactional
   * @param producerId the producer id the client holds, or -1 on a first initialization
   * @param producerEpoch the producer epoch the client holds, or -1 on a first initialization
   * @return the vector
   */
  private static Vector initProducerIdRequest(
      String api, short version, String caseId, String transactionalId, long producerId,
      short producerEpoch) {
    return new Vector(api, "InitProducerIdRequest", version, caseId,
        new InitProducerIdRequestData()
            .setTransactionalId(transactionalId)
            .setTransactionTimeoutMs(TRANSACTION_TIMEOUT_MS)
            .setProducerId(producerId)
            .setProducerEpoch(producerEpoch),
        obj(
            kv("transactionalId", str(transactionalId)),
            kv("transactionTimeout", String.valueOf(TRANSACTION_TIMEOUT_MS)),
            kv("producerId", str(String.valueOf(producerId))),
            kv("producerEpoch", String.valueOf(producerEpoch))));
  }

  /**
   * AddPartitionsToTxn (24) v2-v3, request and response - what a client sends while transaction
   * version 2 is off. v3 is the first flexible version and the client ceiling: the schema reserves
   * v4 and above for brokers. The request uses the {@code V3AndBelow*} fields; the response keeps
   * one topic with two partitions, the second of which carries the error case's code.
   *
   * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/AddPartitionsToTxnRequest.json">AddPartitionsToTxnRequest.json</a>
   * @return the vectors
   */
  private static List<Vector> addPartitionsToTxn() {
    List<Vector> vectors = new ArrayList<>();
    String api = "addPartitionsToTxn";

    for (short version : new short[] {2, 3}) {
      AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection topics =
          new AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection();
      topics.add(
          new AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic()
              .setName(TOPIC_NAME)
              .setPartitions(PARTITIONS));

      vectors.add(
          new Vector(api, "AddPartitionsToTxnRequest", version, "basic",
              new AddPartitionsToTxnRequestData()
                  .setV3AndBelowTransactionalId(TRANSACTIONAL_ID)
                  .setV3AndBelowProducerId(PRODUCER_ID)
                  .setV3AndBelowProducerEpoch(PRODUCER_EPOCH)
                  .setV3AndBelowTopics(topics),
              obj(
                  kv("transactionalId", str(TRANSACTIONAL_ID)),
                  kv("producerId", str(String.valueOf(PRODUCER_ID))),
                  kv("producerEpoch", String.valueOf(PRODUCER_EPOCH)),
                  kv("topics",
                      array(
                          obj(kv("topic", str(TOPIC_NAME)), kv("partitions", intArray(PARTITIONS))))))));

      vectors.add(addPartitionsToTxnResponse(api, version, "ok", (short) 0));
      vectors.add(addPartitionsToTxnResponse(api, version, "error", CONCURRENT_TRANSACTIONS));
    }

    return vectors;
  }

  /**
   * One AddPartitionsToTxn response: one topic, two partitions, the first always clean.
   *
   * @param api the fixtures directory name
   * @param version 2 or 3
   * @param caseId the case name, which is also the file name's last part
   * @param secondError the error code the second partition carries, 0 for none
   * @return the vector
   */
  private static Vector addPartitionsToTxnResponse(
      String api, short version, String caseId, short secondError) {
    AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResultCollection results =
        new AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResultCollection();
    results.add(
        new AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult()
            .setPartitionIndex(PARTITIONS.get(0))
            .setPartitionErrorCode((short) 0));
    results.add(
        new AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult()
            .setPartitionIndex(PARTITIONS.get(1))
            .setPartitionErrorCode(secondError));

    AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection topicResults =
        new AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection();
    topicResults.add(
        new AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult()
            .setName(TOPIC_NAME)
            .setResultsByPartition(results));

    return new Vector(api, "AddPartitionsToTxnResponse", version, caseId,
        new AddPartitionsToTxnResponseData()
            .setThrottleTimeMs(TXN_THROTTLE_TIME_MS)
            .setResultsByTopicV3AndBelow(topicResults),
        obj(
            kv("throttleTime", String.valueOf(TXN_THROTTLE_TIME_MS)),
            kv("errors",
                array(
                    obj(
                        kv("topic", str(TOPIC_NAME)),
                        kv("partitionErrors",
                            array(
                                obj(
                                    kv("partition", String.valueOf(PARTITIONS.get(0))),
                                    kv("errorCode", "0")),
                                obj(
                                    kv("partition", String.valueOf(PARTITIONS.get(1))),
                                    kv("errorCode", String.valueOf(secondError))))))))));
  }

  /**
   * AddOffsetsToTxn (25) v2-v4, request and response. v3 is the first flexible version; v4 only
   * widens the error codes the broker may answer, so v3 and v4 are the same bytes.
   *
   * @return the vectors
   */
  private static List<Vector> addOffsetsToTxn() {
    List<Vector> vectors = new ArrayList<>();
    String api = "addOffsetsToTxn";

    for (short version : new short[] {2, 3, 4}) {
      vectors.add(
          new Vector(api, "AddOffsetsToTxnRequest", version, "basic",
              new AddOffsetsToTxnRequestData()
                  .setTransactionalId(TRANSACTIONAL_ID)
                  .setProducerId(PRODUCER_ID)
                  .setProducerEpoch(PRODUCER_EPOCH)
                  .setGroupId(GROUP_ID),
              obj(
                  kv("transactionalId", str(TRANSACTIONAL_ID)),
                  kv("producerId", str(String.valueOf(PRODUCER_ID))),
                  kv("producerEpoch", String.valueOf(PRODUCER_EPOCH)),
                  kv("groupId", str(GROUP_ID)))));

      for (short errorCode : new short[] {0, CONCURRENT_TRANSACTIONS}) {
        vectors.add(
            new Vector(api, "AddOffsetsToTxnResponse", version, errorCode == 0 ? "ok" : "error",
                new AddOffsetsToTxnResponseData()
                    .setThrottleTimeMs(TXN_THROTTLE_TIME_MS)
                    .setErrorCode(errorCode),
                obj(
                    kv("throttleTime", String.valueOf(TXN_THROTTLE_TIME_MS)),
                    kv("errorCode", String.valueOf(errorCode)))));
      }
    }

    return vectors;
  }

  /**
   * EndTxn (26) v2-v5, request and response. Commit and abort differ in the Committed byte alone.
   * v5's response carries the producer id and epoch the coordinator moved the producer to
   * (KIP-890): {@code producerIdAndEpoch} is a TV2 commit's answer, {@code defaults} the same
   * success with the fields at their -1/-1 defaults - every v5 body carries both fields, they are
   * not tagged - and {@code error} a fenced producer's answer. Below v5 the fields do not exist and
   * {@code fields} does not name them.
   *
   * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/EndTxnResponse.json">EndTxnResponse.json</a>
   * @return the vectors
   */
  private static List<Vector> endTxn() {
    List<Vector> vectors = new ArrayList<>();
    String api = "endTxn";

    for (short version : new short[] {2, 3, 4, 5}) {
      for (boolean committed : new boolean[] {true, false}) {
        vectors.add(
            new Vector(api, "EndTxnRequest", version, committed ? "commit" : "abort",
                new EndTxnRequestData()
                    .setTransactionalId(TRANSACTIONAL_ID)
                    .setProducerId(PRODUCER_ID)
                    .setProducerEpoch(PRODUCER_EPOCH)
                    .setCommitted(committed),
                obj(
                    kv("transactionalId", str(TRANSACTIONAL_ID)),
                    kv("producerId", str(String.valueOf(PRODUCER_ID))),
                    kv("producerEpoch", String.valueOf(PRODUCER_EPOCH)),
                    kv("transactionResult", String.valueOf(committed)))));
      }

      if (version < 5) {
        vectors.add(endTxnResponse(api, version, "ok", (short) 0, -1L, (short) -1));
        vectors.add(endTxnResponse(api, version, "error", PRODUCER_FENCED, -1L, (short) -1));
      } else {
        vectors.add(
            endTxnResponse(api, version, "producerIdAndEpoch", (short) 0, PRODUCER_ID,
                BUMPED_PRODUCER_EPOCH));
        vectors.add(endTxnResponse(api, version, "defaults", (short) 0, -1L, (short) -1));
        vectors.add(endTxnResponse(api, version, "error", PRODUCER_FENCED, -1L, (short) -1));
      }
    }

    return vectors;
  }

  /**
   * One EndTxn response. The producer id and epoch are written, and named in {@code fields}, at v5
   * only; below v5 the writer drops them whatever the arguments are.
   *
   * @param api the fixtures directory name
   * @param version 2, 3, 4 or 5
   * @param caseId the case name, which is also the file name's last part
   * @param errorCode the response's error code, 0 for none
   * @param producerId the producer id the coordinator hands back, -1 for the default
   * @param producerEpoch the producer epoch the coordinator hands back, -1 for the default
   * @return the vector
   */
  private static Vector endTxnResponse(
      String api, short version, String caseId, short errorCode, long producerId,
      short producerEpoch) {
    EndTxnResponseData data =
        new EndTxnResponseData().setThrottleTimeMs(TXN_THROTTLE_TIME_MS).setErrorCode(errorCode);
    String fields;

    if (version >= 5) {
      data.setProducerId(producerId).setProducerEpoch(producerEpoch);
      fields =
          obj(
              kv("throttleTime", String.valueOf(TXN_THROTTLE_TIME_MS)),
              kv("errorCode", String.valueOf(errorCode)),
              kv("producerId", str(String.valueOf(producerId))),
              kv("producerEpoch", String.valueOf(producerEpoch)));
    } else {
      fields =
          obj(
              kv("throttleTime", String.valueOf(TXN_THROTTLE_TIME_MS)),
              kv("errorCode", String.valueOf(errorCode)));
    }

    return new Vector(api, "EndTxnResponse", version, caseId, data, fields);
  }

  /**
   * TxnOffsetCommit (28) v2-v5, request and response. v2 adds CommittedLeaderEpoch; v3 is the first
   * flexible version and adds the GenerationId/MemberId/GroupInstanceId triple (KIP-447), which is
   * why v2 has a {@code leaderEpoch} case where v3-v5 have {@code groupMetadata}: Kafka's writer
   * refuses a non-default triple at v2. {@code legacy} is what the 2.2.4-shaped
   * {@code sendOffsets({ consumerGroupId })} sends - every new field at its default.
   *
   * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/TxnOffsetCommitRequest.json">TxnOffsetCommitRequest.json</a>
   * @return the vectors
   */
  private static List<Vector> txnOffsetCommit() {
    List<Vector> vectors = new ArrayList<>();
    String api = "txnOffsetCommit";

    for (short version : new short[] {2, 3, 4, 5}) {
      vectors.add(txnOffsetCommitRequest(api, version, "legacy", -1, "", null, -1, null));
      if (version == 2) {
        vectors.add(
            txnOffsetCommitRequest(api, version, "leaderEpoch", -1, "", null,
                COMMITTED_LEADER_EPOCH, null));
      } else {
        vectors.add(
            txnOffsetCommitRequest(api, version, "groupMetadata", GENERATION_ID, MEMBER_ID,
                GROUP_INSTANCE_ID, COMMITTED_LEADER_EPOCH, COMMITTED_METADATA));
      }

      vectors.add(txnOffsetCommitResponse(api, version, "ok", (short) 0));
      vectors.add(txnOffsetCommitResponse(api, version, "error", ILLEGAL_GENERATION));
    }

    return vectors;
  }

  /**
   * One TxnOffsetCommit request for one partition of {@link #TOPIC_NAME}. The writer throws on a
   * non-default {@code generationId}, {@code memberId} or {@code groupInstanceId} below v3.
   *
   * @param api the fixtures directory name
   * @param version 2, 3, 4 or 5
   * @param caseId the case name, which is also the file name's last part
   * @param generationId the consumer group generation, -1 when the producer sends no group metadata
   * @param memberId the consumer group member id, empty when there is no group metadata
   * @param groupInstanceId the static member id, or null
   * @param leaderEpoch the committed leader epoch, -1 when unknown
   * @param metadata the committed metadata string, or null
   * @return the vector
   */
  private static Vector txnOffsetCommitRequest(
      String api, short version, String caseId, int generationId, String memberId,
      String groupInstanceId, int leaderEpoch, String metadata) {
    TxnOffsetCommitRequestData data =
        new TxnOffsetCommitRequestData()
            .setTransactionalId(TRANSACTIONAL_ID)
            .setGroupId(GROUP_ID)
            .setProducerId(PRODUCER_ID)
            .setProducerEpoch(PRODUCER_EPOCH)
            .setGenerationId(generationId)
            .setMemberId(memberId)
            .setGroupInstanceId(groupInstanceId)
            .setTopics(
                Collections.singletonList(
                    new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                        .setName(TOPIC_NAME)
                        .setPartitions(
                            Collections.singletonList(
                                new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                                    .setPartitionIndex(PARTITION_INDEX)
                                    .setCommittedOffset(COMMITTED_OFFSET)
                                    .setCommittedLeaderEpoch(leaderEpoch)
                                    .setCommittedMetadata(metadata)))));

    return new Vector(api, "TxnOffsetCommitRequest", version, caseId, data,
        obj(
            kv("transactionalId", str(TRANSACTIONAL_ID)),
            kv("groupId", str(GROUP_ID)),
            kv("producerId", str(String.valueOf(PRODUCER_ID))),
            kv("producerEpoch", String.valueOf(PRODUCER_EPOCH)),
            kv("generationId", String.valueOf(generationId)),
            kv("memberId", str(memberId)),
            kv("groupInstanceId", str(groupInstanceId)),
            kv("topics",
                array(
                    obj(
                        kv("topic", str(TOPIC_NAME)),
                        kv("partitions",
                            array(
                                obj(
                                    kv("partition", String.valueOf(PARTITION_INDEX)),
                                    kv("offset", str(String.valueOf(COMMITTED_OFFSET))),
                                    kv("leaderEpoch", String.valueOf(leaderEpoch)),
                                    kv("metadata", str(metadata))))))))));
  }

  /**
   * One TxnOffsetCommit response for one partition of {@link #TOPIC_NAME}.
   *
   * @param api the fixtures directory name
   * @param version 2, 3, 4 or 5
   * @param caseId the case name, which is also the file name's last part
   * @param errorCode the error code the partition carries, 0 for none
   * @return the vector
   */
  private static Vector txnOffsetCommitResponse(
      String api, short version, String caseId, short errorCode) {
    return new Vector(api, "TxnOffsetCommitResponse", version, caseId,
        new TxnOffsetCommitResponseData()
            .setThrottleTimeMs(TXN_THROTTLE_TIME_MS)
            .setTopics(
                Collections.singletonList(
                    new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                        .setName(TOPIC_NAME)
                        .setPartitions(
                            Collections.singletonList(
                                new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                                    .setPartitionIndex(PARTITION_INDEX)
                                    .setErrorCode(errorCode))))),
        obj(
            kv("throttleTime", String.valueOf(TXN_THROTTLE_TIME_MS)),
            kv("topics",
                array(
                    obj(
                        kv("topic", str(TOPIC_NAME)),
                        kv("partitions",
                            array(
                                obj(
                                    kv("partition", String.valueOf(PARTITION_INDEX)),
                                    kv("errorCode", String.valueOf(errorCode))))))))));
  }

  /**
   * {@code Uuid.fromString} decodes Kafka's base64 uuid form, not the canonical 8-4-4-4-12 hex
   * form {@link #TOPIC_ID} is written in, so the constant is parsed through {@link UUID} and its
   * two halves handed to {@link Uuid} directly.
   *
   * @return {@link #TOPIC_ID} as a Kafka {@link Uuid}
   */
  private static Uuid topicId() {
    UUID standard = UUID.fromString(TOPIC_ID);
    return new Uuid(standard.getMostSignificantBits(), standard.getLeastSignificantBits());
  }

  /**
   * Serializes one vector and renders its fixture document.
   *
   * @param vector the vector to render
   * @return the fixture's JSON text, newline-terminated
   */
  private static String document(Vector vector) {
    ObjectSerializationCache cache = new ObjectSerializationCache();
    int size = vector.data.size(cache, vector.version);
    ByteBuffer buffer = ByteBuffer.allocate(size);
    vector.data.write(new ByteBufferAccessor(buffer), cache, vector.version);
    buffer.flip();

    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);

    StringBuilder hex = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      hex.append(String.format("%02x", b));
    }

    return "{\n"
        + "  \"message\": " + str(vector.message) + ",\n"
        + "  \"version\": " + vector.version + ",\n"
        + "  \"case\": " + str(vector.caseId) + ",\n"
        + "  \"hex\": " + str(hex.toString()) + ",\n"
        + "  \"fields\": " + vector.fields + "\n"
        + "}\n";
  }

  // --- the smallest JSON writer that renders these fixtures, so the tool needs no dependency ---

  /** @param value a string or null @return the JSON literal for it */
  private static String str(String value) {
    if (value == null) {
      return "null";
    }
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  /** @param values strings or null @return a JSON array of strings, or {@code null} */
  private static String strArray(List<String> values) {
    if (values == null) {
      return "null";
    }
    List<String> rendered = new ArrayList<>();
    for (String value : values) {
      rendered.add(str(value));
    }
    return "[" + String.join(", ", rendered) + "]";
  }

  /** @param values integers or null @return a JSON array of numbers, or {@code null} */
  private static String intArray(List<Integer> values) {
    if (values == null) {
      return "null";
    }
    List<String> rendered = new ArrayList<>();
    for (Integer value : values) {
      rendered.add(String.valueOf(value));
    }
    return "[" + String.join(", ", rendered) + "]";
  }

  /** @param entries already-rendered {@code "key": value} pairs @return a JSON object */
  private static String obj(String... entries) {
    return "{ " + String.join(", ", entries) + " }";
  }

  /** @param key field name @param value already-rendered JSON @return one object entry */
  private static String kv(String key, String value) {
    return str(key) + ": " + value;
  }

  /** @param values already-rendered JSON values @return a JSON array */
  private static String array(String... values) {
    return "[" + String.join(", ", values) + "]";
  }
}
