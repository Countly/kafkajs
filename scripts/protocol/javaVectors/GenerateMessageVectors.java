import org.apache.kafka.common.message.WriteTxnMarkersResponseData;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.message.DescribeProducersRequestData;
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
import java.util.function.Function;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

/**
 * Generates golden byte vectors for the APIs the KIP-848 consumer protocol and its admin surface,
 * the transaction protocol, the admin client (configs, topics, records, groups, ACLs and topic
 * partitions) and the classic group protocol (JoinGroup, SyncGroup, Heartbeat, LeaveGroup) need,
 * by running Apache Kafka's own generated message codecs
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

  /** ThrottleTimeMs on every admin and group response, so a decoder that skips it fails. */
  private static final int THROTTLE_TIME_MS = 25;

  /** The admin client's default request timeout, sent as the broker-side timeout. */
  private static final int ADMIN_TIMEOUT_MS = 5000;

  private static final String OTHER_TOPIC_NAME = "other-topic";
  private static final String OTHER_TOPIC_ID = "11121314-1516-1718-191a-1b1c1d1e1f20";

  /** ConfigResource and ACL resource types, AlterConfigOp types. */
  private static final int RESOURCE_TOPIC = 2;
  private static final int RESOURCE_BROKER = 4;
  private static final int RESOURCE_USER = 7;
  private static final int OP_SET = 0;
  private static final int OP_DELETE = 1;
  private static final int OP_APPEND = 2;
  private static final int OP_SUBTRACT = 3;

  /** ACL enum values: PatternType LITERAL, AclOperation READ, AclPermissionType ALLOW. */
  private static final int PATTERN_LITERAL = 3;
  private static final int ACL_READ = 3;
  private static final int ACL_ALLOW = 3;
  private static final int ANY = 1;

  /** The error codes the admin error cases carry. */
  private static final short UNKNOWN_TOPIC_OR_PARTITION = 3;
  private static final short TOPIC_ALREADY_EXISTS = 36;
  private static final short INVALID_PARTITIONS = 37;
  private static final short INVALID_CONFIG = 40;
  private static final short INVALID_REQUEST = 42;
  private static final short NON_EMPTY_GROUP = 68;
  private static final short TOPIC_DELETION_DISABLED = 73;

  /** The ACL the {@code basic} cases carry, and the same on a {@code USER} resource (v3). */
  private static final Acl TOPIC_ACL =
      new Acl(RESOURCE_TOPIC, TOPIC_NAME, PATTERN_LITERAL, "User:alice", "*", ACL_READ, ACL_ALLOW);
  private static final Acl USER_ACL =
      new Acl(RESOURCE_USER, "User:bob", PATTERN_LITERAL, "User:alice", "*", ACL_READ, ACL_ALLOW);

  /** A filter that matches anything: every enum ANY, every string null. */
  private static final Acl ANY_FILTER = new Acl(ANY, null, ANY, null, null, ANY, ANY);

  /** The group and offset error codes the cases carry. */
  private static final short UNKNOWN_MEMBER_ID = 25;
  private static final short REBALANCE_IN_PROGRESS = 27;
  private static final short MEMBER_ID_REQUIRED = 79;
  private static final short UNKNOWN_TOPIC_ID = 100;

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

    /** @return the fixture file name: lower-camel message, version and case, then .json */
    String fileName() {
      char[] name = message.toCharArray();
      name[0] = Character.toLowerCase(name[0]);
      return new String(name) + "-v" + version + "-" + caseId + ".json";
    }
  }

  /**
   * @param args the repository root, or {@code produce} followed by the root for fixed-record
   *     Produce vectors; output follows {@code src/protocol/requests/<api>/fixtures/java/}.
   * @throws IOException if a fixture file cannot be written
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("usage: GenerateMessageVectors <output-dir>");
      System.exit(1);
    }

    if (args[0].equals("produce")) {
      writeProduceRecords(Paths.get(args[1]));
      return;
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

  /**
   * Generates two independent, record-containing Produce bodies from fixed Java inputs.
   * @param root repository root receiving the structured fixtures
   * @throws IOException when writing a vector fails
   */
  private static void writeProduceRecords(Path root) throws IOException {
    var headers = new org.apache.kafka.common.header.Header[] {
        new org.apache.kafka.common.header.internals.RecordHeader("trace", "first".getBytes(StandardCharsets.UTF_8))};
    var records = org.apache.kafka.common.record.internal.MemoryRecords.withTransactionalRecords(
        (byte) 2, 0L, org.apache.kafka.common.compress.Compression.NONE, 1000L, (short) 3, 7, 0,
        new org.apache.kafka.common.record.internal.SimpleRecord(1700000000000L,
            "key-0".getBytes(StandardCharsets.UTF_8), "value-0".getBytes(StandardCharsets.UTF_8), headers),
        new org.apache.kafka.common.record.internal.SimpleRecord(1700000000001L,
            "key-1".getBytes(StandardCharsets.UTF_8), "value-1".getBytes(StandardCharsets.UTF_8)));
    byte[] batchBytes = new byte[records.buffer().remaining()];
    records.buffer().duplicate().get(batchBytes);
    String batchHex = java.util.HexFormat.of().formatHex(batchBytes);
    String digest;
    try {
      digest = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-1")
          .digest(java.util.HexFormat.of().parseHex(batchHex)));
    } catch (java.security.NoSuchAlgorithmException error) { throw new IllegalStateException(error); }
    String fields = "{\"transactionalId\":\"tx-1\",\"acks\":-1,\"timeoutMs\":1000,"
        + "\"topics\":[{\"topic\":\"test-topic\",\"partitions\":[{\"partition\":0,\"batches\":[{"
        + "\"producerId\":\"1000\",\"producerEpoch\":3,\"baseSequence\":7,\"recordCount\":2,"
        + "\"transactional\":true,\"control\":false,\"digest\":\"" + digest + "\"}]}]}]}";
    for (short version : new short[] {11, 12}) {
      var partition = new org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData()
          .setIndex(0).setRecords(records);
      var topic = new org.apache.kafka.common.message.ProduceRequestData.TopicProduceData()
          .setName("test-topic").setPartitionData(List.of(partition));
      var topics = new org.apache.kafka.common.message.ProduceRequestData.TopicProduceDataCollection();
      topics.add(topic);
      var request = new org.apache.kafka.common.message.ProduceRequestData().setTransactionalId("tx-1")
          .setAcks((short) -1).setTimeoutMs(1000).setTopicData(topics);
      var cache = new ObjectSerializationCache();
      var buffer = ByteBuffer.allocate(request.size(cache, version));
      request.write(new ByteBufferAccessor(buffer), cache, version);
      String json = "{\n  \"source\": {\"generator\":\"GenerateMessageVectors.java produce\","
          + "\"library\":\"kafka-clients 4.3.1\",\"representation\":\"Produce request body, flexible header excluded\","
          + "\"timestamps\":[1700000000000,1700000000001],\"keys\":[\"key-0\",\"key-1\"],"
          + "\"values\":[\"value-0\",\"value-1\"],\"headers\":[{\"trace\":\"first\"},{}],"
          + "\"baseOffset\":0,\"leaderEpoch\":0,\"producerId\":1000,\"producerEpoch\":3,\"baseSequence\":7},\n"
          + "  \"hex\":\"" + java.util.HexFormat.of().formatHex(buffer.array()) + "\",\n"
          + "  \"batchHex\":\"" + batchHex + "\",\n  \"fields\":" + fields + "\n}\n";
      Path directory = root.resolve("src/protocol/requests/produce/fixtures/java");
      Files.createDirectories(directory);
      Files.writeString(directory.resolve("produceRequest-v" + version + "-records.json"), json);
    }
  }

  /** Generates transaction administration bodies with Apache codecs.
   * @return vectors; all int64 fixture fields stay decimal strings
   */
  private static List<Vector> transactionAdmin() {
    List<Vector> vectors = new ArrayList<>();
vectors.add(new Vector("describeProducers", "DescribeProducersRequest", (short) 0, "basic",
new DescribeProducersRequestData()
.setTopics(Arrays.asList(new DescribeProducersRequestData.TopicRequest()
.setName("orders")
.setPartitionIndexes(Arrays.asList(0, 2)))),
"{\"topics\":[{\"topic\":\"orders\",\"partitions\":[0,2]}]}"));
vectors.add(new Vector("describeProducers", "DescribeProducersResponse", (short) 0, "ok",
new DescribeProducersResponseData()
.setThrottleTimeMs(17)
.setTopics(Arrays.asList(new DescribeProducersResponseData.TopicResponse()
.setName("orders")
.setPartitions(Arrays.asList(new DescribeProducersResponseData.PartitionResponse()
.setPartitionIndex(0)
.setErrorCode((short) 0)
.setErrorMessage(null)
.setActiveProducers(Arrays.asList(new DescribeProducersResponseData.ProducerState()
.setProducerId(9007199254740993L)
.setProducerEpoch(65537)
.setLastSequence(23)
.setLastTimestamp(9007199254740994L)
.setCoordinatorEpoch(9)
.setCurrentTxnStartOffset(-1L))), new DescribeProducersResponseData.PartitionResponse()
.setPartitionIndex(2)
.setErrorCode((short) 6)
.setErrorMessage("moved")
.setActiveProducers(Collections.emptyList()))))),
"{\"throttleTime\":17,\"topics\":[{\"topic\":\"orders\",\"partitions\":[{\"partition\":0,\"errorCode\":0,\"errorMessage\":null,\"activeProducers\":[{\"producerId\":\"9007199254740993\",\"producerEpoch\":65537,\"lastSequence\":23,\"lastTimestamp\":\"9007199254740994\",\"coordinatorEpoch\":9,\"currentTxnStartOffset\":\"-1\"}]},{\"partition\":2,\"errorCode\":6,\"errorMessage\":\"moved\",\"activeProducers\":[]}]}]}"));
vectors.add(new Vector("describeTransactions", "DescribeTransactionsRequest", (short) 0, "basic",
new DescribeTransactionsRequestData()
.setTransactionalIds(Arrays.asList("tx-a", "tx-b")),
"{\"transactionalIds\":[\"tx-a\",\"tx-b\"]}"));
vectors.add(new Vector("describeTransactions", "DescribeTransactionsResponse", (short) 0, "ok",
new DescribeTransactionsResponseData()
.setThrottleTimeMs(17)
.setTransactionStates(Arrays.asList(new DescribeTransactionsResponseData.TransactionState()
.setErrorCode((short) 0)
.setTransactionalId("tx-a")
.setTransactionState("Ongoing")
.setTransactionTimeoutMs(60000)
.setTransactionStartTimeMs(9007199254740994L)
.setProducerId(9007199254740993L)
.setProducerEpoch((short) 7)
.setTopics(new DescribeTransactionsResponseData.TopicDataCollection(Arrays.asList(new DescribeTransactionsResponseData.TopicData()
.setTopic("orders")
.setPartitions(Arrays.asList(0, 2))).iterator())), new DescribeTransactionsResponseData.TransactionState()
.setErrorCode((short) 53)
.setTransactionalId("tx-b")
.setTransactionState("Unknown")
.setTransactionTimeoutMs(0)
.setTransactionStartTimeMs(-1L)
.setProducerId(-1L)
.setProducerEpoch((short) -1)
.setTopics(new DescribeTransactionsResponseData.TopicDataCollection()))),
"{\"throttleTime\":17,\"transactions\":[{\"errorCode\":0,\"transactionalId\":\"tx-a\",\"state\":\"Ongoing\",\"timeoutMs\":60000,\"startTimeMs\":\"9007199254740994\",\"producerId\":\"9007199254740993\",\"producerEpoch\":7,\"topics\":[{\"topic\":\"orders\",\"partitions\":[0,2]}]},{\"errorCode\":53,\"transactionalId\":\"tx-b\",\"state\":\"Unknown\",\"timeoutMs\":0,\"startTimeMs\":\"-1\",\"producerId\":\"-1\",\"producerEpoch\":-1,\"topics\":[]}]}"));
vectors.add(new Vector("listTransactions", "ListTransactionsRequest", (short) 0, "basic",
new ListTransactionsRequestData()
.setStateFilters(Arrays.asList("Ongoing", "FutureState"))
.setProducerIdFilters(Arrays.asList(9007199254740993L)),
"{\"states\":[\"Ongoing\",\"FutureState\"],\"producerIds\":[\"9007199254740993\"]}"));
vectors.add(new Vector("listTransactions", "ListTransactionsResponse", (short) 0, "ok",
new ListTransactionsResponseData()
.setThrottleTimeMs(17)
.setErrorCode((short) 0)
.setUnknownStateFilters(Arrays.asList("FutureState"))
.setTransactionStates(Arrays.asList(new ListTransactionsResponseData.TransactionState()
.setTransactionalId("tx-a")
.setProducerId(9007199254740993L)
.setTransactionState("Ongoing"))),
"{\"throttleTime\":17,\"errorCode\":0,\"unknownStateFilters\":[\"FutureState\"],\"transactions\":[{\"transactionalId\":\"tx-a\",\"producerId\":\"9007199254740993\",\"state\":\"Ongoing\"}]}"));
vectors.add(new Vector("listTransactions", "ListTransactionsRequest", (short) 1, "basic",
new ListTransactionsRequestData()
.setStateFilters(Arrays.asList("Ongoing", "FutureState"))
.setProducerIdFilters(Arrays.asList(9007199254740993L))
.setDurationFilter(250L),
"{\"states\":[\"Ongoing\",\"FutureState\"],\"producerIds\":[\"9007199254740993\"],\"durationFilter\":\"250\"}"));
vectors.add(new Vector("listTransactions", "ListTransactionsResponse", (short) 1, "ok",
new ListTransactionsResponseData()
.setThrottleTimeMs(17)
.setErrorCode((short) 0)
.setUnknownStateFilters(Arrays.asList("FutureState"))
.setTransactionStates(Arrays.asList(new ListTransactionsResponseData.TransactionState()
.setTransactionalId("tx-a")
.setProducerId(9007199254740993L)
.setTransactionState("Ongoing"))),
"{\"throttleTime\":17,\"errorCode\":0,\"unknownStateFilters\":[\"FutureState\"],\"transactions\":[{\"transactionalId\":\"tx-a\",\"producerId\":\"9007199254740993\",\"state\":\"Ongoing\"}]}"));
vectors.add(new Vector("listTransactions", "ListTransactionsRequest", (short) 2, "basic",
new ListTransactionsRequestData()
.setStateFilters(Arrays.asList("Ongoing", "FutureState"))
.setProducerIdFilters(Arrays.asList(9007199254740993L))
.setDurationFilter(250L)
.setTransactionalIdPattern("^tx-.*$"),
"{\"states\":[\"Ongoing\",\"FutureState\"],\"producerIds\":[\"9007199254740993\"],\"durationFilter\":\"250\",\"transactionalIdPattern\":\"^tx-.*$\"}"));
vectors.add(new Vector("listTransactions", "ListTransactionsResponse", (short) 2, "ok",
new ListTransactionsResponseData()
.setThrottleTimeMs(17)
.setErrorCode((short) 0)
.setUnknownStateFilters(Arrays.asList("FutureState"))
.setTransactionStates(Arrays.asList(new ListTransactionsResponseData.TransactionState()
.setTransactionalId("tx-a")
.setProducerId(9007199254740993L)
.setTransactionState("Ongoing"))),
"{\"throttleTime\":17,\"errorCode\":0,\"unknownStateFilters\":[\"FutureState\"],\"transactions\":[{\"transactionalId\":\"tx-a\",\"producerId\":\"9007199254740993\",\"state\":\"Ongoing\"}]}"));
vectors.add(new Vector("writeTxnMarkers", "WriteTxnMarkersRequest", (short) 1, "basic",
new WriteTxnMarkersRequestData()
.setMarkers(Arrays.asList(new WriteTxnMarkersRequestData.WritableTxnMarker()
.setProducerId(9007199254740993L)
.setProducerEpoch((short) 7)
.setTransactionResult(false)
.setTopics(Arrays.asList(new WriteTxnMarkersRequestData.WritableTxnMarkerTopic()
.setName("orders")
.setPartitionIndexes(Arrays.asList(2))))
.setCoordinatorEpoch(9))),
"{\"markers\":[{\"producerId\":\"9007199254740993\",\"producerEpoch\":7,\"transactionResult\":false,\"topics\":[{\"topic\":\"orders\",\"partitions\":[2]}],\"coordinatorEpoch\":9}]}"));
vectors.add(new Vector("writeTxnMarkers", "WriteTxnMarkersResponse", (short) 1, "ok",
new WriteTxnMarkersResponseData()
.setMarkers(Arrays.asList(new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
.setProducerId(9007199254740993L)
.setTopics(Arrays.asList(new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
.setName("orders")
.setPartitions(Arrays.asList(new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
.setPartitionIndex(2)
.setErrorCode((short) 0))))))),
"{\"markers\":[{\"producerId\":\"9007199254740993\",\"topics\":[{\"topic\":\"orders\",\"partitions\":[{\"partition\":2,\"errorCode\":0}]}]}]}"));
vectors.add(new Vector("writeTxnMarkers", "WriteTxnMarkersRequest", (short) 2, "basic",
new WriteTxnMarkersRequestData()
.setMarkers(Arrays.asList(new WriteTxnMarkersRequestData.WritableTxnMarker()
.setProducerId(9007199254740993L)
.setProducerEpoch((short) 7)
.setTransactionResult(false)
.setTopics(Arrays.asList(new WriteTxnMarkersRequestData.WritableTxnMarkerTopic()
.setName("orders")
.setPartitionIndexes(Arrays.asList(2))))
.setCoordinatorEpoch(9)
.setTransactionVersion((byte) 0))),
"{\"markers\":[{\"producerId\":\"9007199254740993\",\"producerEpoch\":7,\"transactionResult\":false,\"topics\":[{\"topic\":\"orders\",\"partitions\":[2]}],\"coordinatorEpoch\":9,\"transactionVersion\":0}]}"));
vectors.add(new Vector("writeTxnMarkers", "WriteTxnMarkersResponse", (short) 2, "ok",
new WriteTxnMarkersResponseData()
.setMarkers(Arrays.asList(new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
.setProducerId(9007199254740993L)
.setTopics(Arrays.asList(new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
.setName("orders")
.setPartitions(Arrays.asList(new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
.setPartitionIndex(2)
.setErrorCode((short) 0))))))),
"{\"markers\":[{\"producerId\":\"9007199254740993\",\"topics\":[{\"topic\":\"orders\",\"partitions\":[{\"partition\":2,\"errorCode\":0}]}]}]}"));
vectors.add(new Vector("writeTxnMarkers", "WriteTxnMarkersRequest", (short) 2, "transactionVersionTwo",
new WriteTxnMarkersRequestData()
.setMarkers(Arrays.asList(new WriteTxnMarkersRequestData.WritableTxnMarker()
.setProducerId(9007199254740993L)
.setProducerEpoch((short) 7)
.setTransactionResult(false)
.setTopics(Arrays.asList(new WriteTxnMarkersRequestData.WritableTxnMarkerTopic()
.setName("orders")
.setPartitionIndexes(Arrays.asList(2))))
.setCoordinatorEpoch(9)
.setTransactionVersion((byte) 2))),
"{\"markers\":[{\"producerId\":\"9007199254740993\",\"producerEpoch\":7,\"transactionResult\":false,\"topics\":[{\"topic\":\"orders\",\"partitions\":[2]}],\"coordinatorEpoch\":9,\"transactionVersion\":2}]}"));
vectors.add(new Vector("writeTxnMarkers", "WriteTxnMarkersResponse", (short) 2, "error47",
new WriteTxnMarkersResponseData()
.setMarkers(Arrays.asList(new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
.setProducerId(9007199254740993L)
.setTopics(Arrays.asList(new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
.setName("orders")
.setPartitions(Arrays.asList(new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
.setPartitionIndex(2)
.setErrorCode((short) 47))))))),
"{\"markers\":[{\"producerId\":\"9007199254740993\",\"topics\":[{\"topic\":\"orders\",\"partitions\":[{\"partition\":2,\"errorCode\":47}]}]}]}"));
vectors.add(new Vector("writeTxnMarkers", "WriteTxnMarkersResponse", (short) 2, "error52",
new WriteTxnMarkersResponseData()
.setMarkers(Arrays.asList(new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
.setProducerId(9007199254740993L)
.setTopics(Arrays.asList(new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
.setName("orders")
.setPartitions(Arrays.asList(new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
.setPartitionIndex(2)
.setErrorCode((short) 52))))))),
"{\"markers\":[{\"producerId\":\"9007199254740993\",\"topics\":[{\"topic\":\"orders\",\"partitions\":[{\"partition\":2,\"errorCode\":52}]}]}]}"));
vectors.add(new Vector("writeTxnMarkers", "WriteTxnMarkersResponse", (short) 2, "error31",
new WriteTxnMarkersResponseData()
.setMarkers(Arrays.asList(new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
.setProducerId(9007199254740993L)
.setTopics(Arrays.asList(new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
.setName("orders")
.setPartitions(Arrays.asList(new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
.setPartitionIndex(2)
.setErrorCode((short) 31))))))),
"{\"markers\":[{\"producerId\":\"9007199254740993\",\"topics\":[{\"topic\":\"orders\",\"partitions\":[{\"partition\":2,\"errorCode\":31}]}]}]}"));
return vectors;
}

  /** @return every vector this generator emits, in file order per API */
  private static List<Vector> vectors() {
    List<Vector> transactionVectors = transactionAdmin();
    List<Vector> vectors = new ArrayList<>();
    vectors.addAll(transactionVectors);
    vectors.addAll(consumerGroupHeartbeat());
    vectors.addAll(consumerGroupDescribe());
    vectors.addAll(listGroups());
    vectors.addAll(describeGroups());
    vectors.addAll(offsetCommit());
    vectors.addAll(fetchSessions());
    vectors.addAll(offsetFetch());
    vectors.addAll(offsetFetchRequireStable());
    vectors.addAll(offsetFetchIdentity());
    vectors.addAll(initProducerId());
    vectors.addAll(addPartitionsToTxn());
    vectors.addAll(addOffsetsToTxn());
    vectors.addAll(endTxn());
    vectors.addAll(txnOffsetCommit());
    vectors.addAll(incrementalAlterConfigs());
    vectors.addAll(describeConfigs());
    vectors.addAll(alterConfigs());
    vectors.addAll(createTopics());
    vectors.addAll(deleteTopics());
    vectors.addAll(createPartitions());
    vectors.addAll(deleteRecords());
    vectors.addAll(deleteGroups());
    vectors.addAll(acls());
    vectors.addAll(describeTopicPartitions());
    vectors.addAll(joinGroup());
    vectors.addAll(syncGroup());
    vectors.addAll(heartbeat());
    vectors.addAll(leaveGroup());
    vectors.addAll(offsetCommitByTopicId());
    vectors.addAll(offsetFetchByTopicId());
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
   * OffsetFetch (9) v8 and v9 responses that name a group: the identity the client compares with
   * the group it asked about. v8 and v9 share one response layout, so each case is emitted at both.
   *
   * <p>{@code groupMatch} answers for {@code test-group} and {@code groupMismatch} for
   * {@code other-group}; both carry one partition with a committed offset, a leader epoch and a
   * non-null metadata string, and a throttle of 17 ms. {@code twoGroups} and {@code noGroups} are
   * the cardinality controls: the client asks about one group, so neither is a valid answer.
   *
   * @return the key-9 identity vectors
   */
  private static List<Vector> offsetFetchIdentity() {
    List<Vector> vectors = new ArrayList<>();
    String api = "offsetFetch";
    String response = "OffsetFetchResponse";
    int throttleMs = 17;
    String metadata = "read-check";

    for (short version : new short[] {8, 9}) {
      OffsetFetchResponseData.OffsetFetchResponseGroup match = identityGroup(GROUP_ID, metadata);
      OffsetFetchResponseData.OffsetFetchResponseGroup other =
          identityGroup(OTHER_GROUP_ID, metadata);

      vectors.add(
          new Vector(api, response, version, "groupMatch",
              new OffsetFetchResponseData()
                  .setThrottleTimeMs(throttleMs)
                  .setGroups(Collections.singletonList(match)),
              obj(
                  kv("throttleTime", String.valueOf(throttleMs)),
                  identityGroupFields(GROUP_ID, metadata))));
      vectors.add(
          new Vector(api, response, version, "groupMismatch",
              new OffsetFetchResponseData()
                  .setThrottleTimeMs(throttleMs)
                  .setGroups(Collections.singletonList(other)),
              obj(
                  kv("throttleTime", String.valueOf(throttleMs)),
                  identityGroupFields(OTHER_GROUP_ID, metadata))));
      vectors.add(
          new Vector(api, response, version, "twoGroups",
              new OffsetFetchResponseData()
                  .setThrottleTimeMs(throttleMs)
                  .setGroups(Arrays.asList(match, other)),
              obj(
                  kv("throttleTime", String.valueOf(throttleMs)),
                  kv("groups",
                      array(
                          obj(identityGroupFields(GROUP_ID, metadata)),
                          obj(identityGroupFields(OTHER_GROUP_ID, metadata)))))));
      vectors.add(
          new Vector(api, response, version, "noGroups",
              new OffsetFetchResponseData()
                  .setThrottleTimeMs(throttleMs)
                  .setGroups(Collections.emptyList()),
              obj(kv("throttleTime", String.valueOf(throttleMs)), kv("groups", "[]"))));
    }

    return vectors;
  }

  /**
   * One group of an OffsetFetch response: a single committed partition of the test topic.
   *
   * @param groupId the group the answer names
   * @param metadata the committed metadata string
   * @return the response group
   */
  private static OffsetFetchResponseData.OffsetFetchResponseGroup identityGroup(
      String groupId, String metadata) {
    return new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId(groupId)
        .setErrorCode((short) 0)
        .setTopics(
            Collections.singletonList(
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                    .setName(TOPIC_NAME)
                    .setPartitions(
                        Collections.singletonList(
                            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                .setPartitionIndex(PARTITION_INDEX)
                                .setCommittedOffset(COMMITTED_OFFSET)
                                .setCommittedLeaderEpoch(COMMITTED_LEADER_EPOCH)
                                .setMetadata(metadata)
                                .setErrorCode((short) 0)))));
  }

  /**
   * The expected fields of {@link #identityGroup}, written as literals independent of the builder.
   *
   * @param groupId the group the answer names
   * @param metadata the committed metadata string
   * @return the group's members as comma-separated key-value entries, without enclosing braces
   */
  private static String identityGroupFields(String groupId, String metadata) {
    return String.join(", ",
        kv("groupId", str(groupId)),
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
                                kv("leaderEpoch", String.valueOf(COMMITTED_LEADER_EPOCH)),
                                kv("metadata", str(metadata)),
                                kv("errorCode", "0"))))))));
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

  /** @param value any number @return its JSON literal */
  private static String num(long value) {
    return String.valueOf(value);
  }

  /** @param items the elements @param render how one element becomes JSON @return a JSON array */
  private static <T> String list(List<T> items, Function<T, String> render) {
    List<String> rendered = new ArrayList<>();
    for (T item : items) {
      rendered.add(render.apply(item));
    }
    return "[" + String.join(", ", rendered) + "]";
  }

  /** @param uuid canonical form, or null @return Kafka's {@link Uuid} for it, zero for null */
  private static Uuid uuid(String uuid) {
    if (uuid == null) {
      return Uuid.ZERO_UUID;
    }
    UUID standard = UUID.fromString(uuid);
    return new Uuid(standard.getMostSignificantBits(), standard.getLeastSignificantBits());
  }

  /** One IncrementalAlterConfigs entry: the Kafka data and the fixture fields it came from. */
  private record Alter(String name, int operation, String value) {
    /** @return the Kafka {@code AlterableConfig} for this entry */
    IncrementalAlterConfigsRequestData.AlterableConfig data() {
      return new IncrementalAlterConfigsRequestData.AlterableConfig()
          .setName(name)
          .setConfigOperation((byte) operation)
          .setValue(value);
    }

    /** @return this entry as the JSON of a fixture's {@code fields} */
    String json() {
      return obj(
          kv("name", str(name)), kv("operation", num(operation)), kv("value", str(value)));
    }
  }

  /** One IncrementalAlterConfigs resource. */
  private record AlterResource(int type, String name, List<Alter> alters) {
    /** @return the Kafka {@code AlterConfigsResource} for this resource */
    IncrementalAlterConfigsRequestData.AlterConfigsResource data() {
      return new IncrementalAlterConfigsRequestData.AlterConfigsResource()
          .setResourceType((byte) type)
          .setResourceName(name)
          .setConfigs(
              new IncrementalAlterConfigsRequestData.AlterableConfigCollection(
                  alters.stream().map(Alter::data).iterator()));
    }

    /** @return this resource as the JSON of a fixture's {@code fields} */
    String json() {
      return obj(
          kv("type", num(type)),
          kv("name", str(name)),
          kv("configEntries", list(alters, Alter::json)));
    }
  }

  /** One per-resource result of AlterConfigs and IncrementalAlterConfigs. */
  private record ResourceResult(int errorCode, String errorMessage, int type, String name) {
    /** @return the IncrementalAlterConfigs response row for this result */
    IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse incremental() {
      return new IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse()
          .setErrorCode((short) errorCode)
          .setErrorMessage(errorMessage)
          .setResourceType((byte) type)
          .setResourceName(name);
    }

    /** @return the AlterConfigs response row for this result */
    AlterConfigsResponseData.AlterConfigsResourceResponse legacy() {
      return new AlterConfigsResponseData.AlterConfigsResourceResponse()
          .setErrorCode((short) errorCode)
          .setErrorMessage(errorMessage)
          .setResourceType((byte) type)
          .setResourceName(name);
    }

    /** @return this result as the JSON of a fixture's {@code fields} */
    String json() {
      return obj(
          kv("errorCode", num(errorCode)),
          kv("errorMessage", str(errorMessage)),
          kv("resourceType", num(type)),
          kv("resourceName", str(name)));
    }
  }

  /**
   * IncrementalAlterConfigs (44) v0-v1, request and response (KIP-339).
   *
   * <p>The request cases isolate one byte each: the operation (SET, APPEND, SUBTRACT, DELETE), the
   * DELETE's null Value (a classic null string is {@code ffff}, a compact one {@code 00}), a second
   * resource, and ValidateOnly. v1 is v0's body in flexible framing.
   *
   * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/IncrementalAlterConfigsRequest.json">IncrementalAlterConfigsRequest.json</a>
   * @return the key-44 vectors
   */
  private static List<Vector> incrementalAlterConfigs() {
    List<Vector> vectors = new ArrayList<>();
    AlterResource set =
        new AlterResource(
            RESOURCE_TOPIC, TOPIC_NAME, List.of(new Alter("retention.ms", OP_SET, "60000")));
    ResourceResult ok = new ResourceResult(0, null, RESOURCE_TOPIC, TOPIC_NAME);

    for (short version : new short[] {0, 1}) {
      vectors.add(incrementalRequest(version, "set", false, set));
      vectors.add(
          incrementalRequest(version, "append", false,
              new AlterResource(RESOURCE_TOPIC, TOPIC_NAME,
                  List.of(new Alter("cleanup.policy", OP_APPEND, "compact")))));
      vectors.add(
          incrementalRequest(version, "subtract", false,
              new AlterResource(RESOURCE_TOPIC, TOPIC_NAME,
                  List.of(new Alter("cleanup.policy", OP_SUBTRACT, "delete")))));
      vectors.add(
          incrementalRequest(version, "delete", false,
              new AlterResource(RESOURCE_TOPIC, TOPIC_NAME,
                  List.of(new Alter("retention.ms", OP_DELETE, null)))));
      vectors.add(
          incrementalRequest(version, "twoResources", false, set,
              new AlterResource(RESOURCE_BROKER, "1",
                  List.of(new Alter("background.threads", OP_SET, "20"),
                      new Alter("log.cleaner.threads", OP_DELETE, null)))));
      vectors.add(incrementalRequest(version, "validateOnly", true, set));

      vectors.add(incrementalResponse(version, "ok", ok));
      vectors.add(
          incrementalResponse(version, "error", ok,
              new ResourceResult(INVALID_CONFIG,
                  "Invalid value compact,foo for configuration cleanup.policy",
                  RESOURCE_TOPIC, OTHER_TOPIC_NAME)));
    }

    return vectors;
  }

  /**
   * @param version the request version
   * @param caseId the case name
   * @param validateOnly the {@code ValidateOnly} flag
   * @param resources the resources to alter
   * @return one IncrementalAlterConfigs request vector
   */
  private static Vector incrementalRequest(
      short version, String caseId, boolean validateOnly, AlterResource... resources) {
    IncrementalAlterConfigsRequestData data =
        new IncrementalAlterConfigsRequestData()
            .setResources(
                new IncrementalAlterConfigsRequestData.AlterConfigsResourceCollection(
                    Arrays.stream(resources).map(AlterResource::data).iterator()))
            .setValidateOnly(validateOnly);
    return new Vector("incrementalAlterConfigs", "IncrementalAlterConfigsRequest", version, caseId,
        data,
        obj(
            kv("resources", list(Arrays.asList(resources), AlterResource::json)),
            kv("validateOnly", String.valueOf(validateOnly))));
  }

  /**
   * @param version the response version
   * @param caseId the case name
   * @param results one result per altered resource
   * @return one IncrementalAlterConfigs response vector
   */
  private static Vector incrementalResponse(
      short version, String caseId, ResourceResult... results) {
    IncrementalAlterConfigsResponseData data =
        new IncrementalAlterConfigsResponseData()
            .setThrottleTimeMs(THROTTLE_TIME_MS)
            .setResponses(
                Arrays.stream(results).map(ResourceResult::incremental).toList());
    return new Vector("incrementalAlterConfigs", "IncrementalAlterConfigsResponse", version, caseId,
        data,
        obj(
            kv("throttleTime", num(THROTTLE_TIME_MS)),
            kv("resources", list(Arrays.asList(results), ResourceResult::json))));
  }

  /** One DescribeConfigs resource in a request. */
  private record DescribedResource(int type, String name, List<String> configNames) {
    /** @return the Kafka {@code DescribeConfigsResource} for this resource */
    DescribeConfigsRequestData.DescribeConfigsResource data() {
      return new DescribeConfigsRequestData.DescribeConfigsResource()
          .setResourceType((byte) type)
          .setResourceName(name)
          .setConfigurationKeys(configNames);
    }

    /** @return this resource as the JSON of a fixture's {@code fields} */
    String json() {
      return obj(
          kv("type", num(type)), kv("name", str(name)), kv("configNames", strArray(configNames)));
    }
  }

  /** One synonym of a DescribeConfigs entry. */
  private record Synonym(String name, String value, int source) {
    /** @return the Kafka {@code DescribeConfigsSynonym} for this synonym */
    DescribeConfigsResponseData.DescribeConfigsSynonym data() {
      return new DescribeConfigsResponseData.DescribeConfigsSynonym()
          .setName(name)
          .setValue(value)
          .setSource((byte) source);
    }

    /** @return this synonym as the JSON of a fixture's {@code fields} */
    String json() {
      return obj(
          kv("configName", str(name)),
          kv("configValue", str(value)),
          kv("configSource", num(source)));
    }
  }

  /** One DescribeConfigs entry; {@code configType} and {@code documentation} are v3+. */
  private record ConfigEntry(
      String name, String value, boolean readOnly, int source, boolean sensitive,
      List<Synonym> synonyms, int configType, String documentation) {
    /** @return the Kafka {@code DescribeConfigsResourceResult} for this entry */
    DescribeConfigsResponseData.DescribeConfigsResourceResult data() {
      return new DescribeConfigsResponseData.DescribeConfigsResourceResult()
          .setName(name)
          .setValue(value)
          .setReadOnly(readOnly)
          .setConfigSource((byte) source)
          .setIsSensitive(sensitive)
          .setSynonyms(synonyms.stream().map(Synonym::data).toList())
          .setConfigType((byte) configType)
          .setDocumentation(documentation);
    }

    /** @return this entry as the JSON of a fixture's {@code fields} */
    String json() {
      return obj(
          kv("configName", str(name)),
          kv("configValue", str(value)),
          kv("readOnly", String.valueOf(readOnly)),
          kv("configSource", num(source)),
          kv("isSensitive", String.valueOf(sensitive)),
          kv("configSynonyms", list(synonyms, Synonym::json)),
          kv("configType", num(configType)),
          kv("documentation", str(documentation)));
    }
  }

  /**
   * DescribeConfigs (32) v3-v4, request and response (KIP-569).
   *
   * <p>Four request cases, each a single distinction: {@code keys} is a non-empty key list (the
   * compact string array a generic array writer would silently empty), {@code allKeys} the null
   * list (a classic null array is {@code ffffffff}, a compact one {@code 00}),
   * {@code includeDocumentation} differs from {@code allKeys} in the last boolean alone, and
   * {@code noSynonyms} in IncludeSynonyms alone. {@code isDefault} is derived by the client from
   * {@code configSource} and is not a field.
   *
   * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/DescribeConfigsResponse.json">DescribeConfigsResponse.json</a>
   * @return the key-32 vectors
   */
  private static List<Vector> describeConfigs() {
    List<Vector> vectors = new ArrayList<>();
    DescribedResource keys =
        new DescribedResource(
            RESOURCE_TOPIC, TOPIC_NAME, List.of("cleanup.policy", "retention.ms"));
    DescribedResource all = new DescribedResource(RESOURCE_TOPIC, TOPIC_NAME, null);
    List<ConfigEntry> entries =
        List.of(
            new ConfigEntry("retention.ms", "60000", false, 1, false,
                List.of(new Synonym("retention.ms", "60000", 1),
                    new Synonym("log.retention.ms", null, 5)),
                5, "The retention time"),
            new ConfigEntry("cleanup.policy", "delete", false, 5, false, List.of(), 7, null));

    for (short version : new short[] {3, 4}) {
      vectors.add(describeRequest(version, "keys", true, false, keys));
      vectors.add(describeRequest(version, "allKeys", true, false, all));
      vectors.add(describeRequest(version, "includeDocumentation", true, true, all));
      vectors.add(describeRequest(version, "noSynonyms", false, false, all));

      DescribeConfigsResponseData data =
          new DescribeConfigsResponseData()
              .setThrottleTimeMs(THROTTLE_TIME_MS)
              .setResults(
                  List.of(
                      new DescribeConfigsResponseData.DescribeConfigsResult()
                          .setErrorCode((short) 0)
                          .setErrorMessage(null)
                          .setResourceType((byte) RESOURCE_TOPIC)
                          .setResourceName(TOPIC_NAME)
                          .setConfigs(entries.stream().map(ConfigEntry::data).toList())));
      vectors.add(
          new Vector("describeConfigs", "DescribeConfigsResponse", version, "entries", data,
              obj(
                  kv("throttleTime", num(THROTTLE_TIME_MS)),
                  kv("resources",
                      array(
                          obj(
                              kv("errorCode", "0"),
                              kv("errorMessage", "null"),
                              kv("resourceType", num(RESOURCE_TOPIC)),
                              kv("resourceName", str(TOPIC_NAME)),
                              kv("configEntries", list(entries, ConfigEntry::json))))))));
    }

    return vectors;
  }

  /**
   * @param version the request version
   * @param caseId the case name
   * @param synonyms the {@code IncludeSynonyms} flag
   * @param documentation the {@code IncludeDocumentation} flag
   * @param resource the resource to describe
   * @return one DescribeConfigs request vector
   */
  private static Vector describeRequest(
      short version, String caseId, boolean synonyms, boolean documentation,
      DescribedResource resource) {
    DescribeConfigsRequestData data =
        new DescribeConfigsRequestData()
            .setResources(List.of(resource.data()))
            .setIncludeSynonyms(synonyms)
            .setIncludeDocumentation(documentation);
    return new Vector("describeConfigs", "DescribeConfigsRequest", version, caseId, data,
        obj(
            kv("resources", array(resource.json())),
            kv("includeSynonyms", String.valueOf(synonyms)),
            kv("includeDocumentation", String.valueOf(documentation))));
  }

  /**
   * AlterConfigs (33) v2, request and response: v1's body in flexible framing. The request sets two
   * keys of one topic ({@code basic}) or the same with ValidateOnly ({@code validateOnly}); the
   * response is a per-resource {@code ok} or {@code error}.
   *
   * @return the key-33 vectors
   */
  private static List<Vector> alterConfigs() {
    List<Vector> vectors = new ArrayList<>();
    String api = "alterConfigs";

    for (boolean validateOnly : new boolean[] {false, true}) {
      AlterConfigsRequestData data =
          new AlterConfigsRequestData()
              .setResources(
                  new AlterConfigsRequestData.AlterConfigsResourceCollection(
                      List.of(
                              new AlterConfigsRequestData.AlterConfigsResource()
                                  .setResourceType((byte) RESOURCE_TOPIC)
                                  .setResourceName(TOPIC_NAME)
                                  .setConfigs(
                                      new AlterConfigsRequestData.AlterableConfigCollection(
                                          List.of(
                                                  new AlterConfigsRequestData.AlterableConfig()
                                                      .setName("retention.ms")
                                                      .setValue("60000"),
                                                  new AlterConfigsRequestData.AlterableConfig()
                                                      .setName("cleanup.policy")
                                                      .setValue("compact"))
                                              .iterator())))
                          .iterator()))
              .setValidateOnly(validateOnly);
      vectors.add(
          new Vector(api, "AlterConfigsRequest", (short) 2, validateOnly ? "validateOnly" : "basic",
              data,
              obj(
                  kv("resources",
                      array(
                          obj(
                              kv("type", num(RESOURCE_TOPIC)),
                              kv("name", str(TOPIC_NAME)),
                              kv("configEntries",
                                  array(
                                      obj(kv("name", str("retention.ms")),
                                          kv("value", str("60000"))),
                                      obj(kv("name", str("cleanup.policy")),
                                          kv("value", str("compact")))))))),
                  kv("validateOnly", String.valueOf(validateOnly)))));
    }

    ResourceResult ok = new ResourceResult(0, null, RESOURCE_TOPIC, TOPIC_NAME);
    ResourceResult bad =
        new ResourceResult(INVALID_REQUEST, "Invalid config", RESOURCE_BROKER, "1");
    for (ResourceResult[] results :
        new ResourceResult[][] {new ResourceResult[] {ok}, new ResourceResult[] {ok, bad}}) {
      AlterConfigsResponseData data =
          new AlterConfigsResponseData()
              .setThrottleTimeMs(THROTTLE_TIME_MS)
              .setResponses(Arrays.stream(results).map(ResourceResult::legacy).toList());
      vectors.add(
          new Vector(api, "AlterConfigsResponse", (short) 2, results.length == 1 ? "ok" : "error",
              data,
              obj(
                  kv("throttleTime", num(THROTTLE_TIME_MS)),
                  kv("resources", list(Arrays.asList(results), ResourceResult::json)))));
    }

    return vectors;
  }

  /** A replica assignment of one new topic partition. */
  private record Assignment(int partition, List<Integer> replicas) {
    /** @return the Kafka {@code CreatableReplicaAssignment} for this assignment */
    CreateTopicsRequestData.CreatableReplicaAssignment data() {
      return new CreateTopicsRequestData.CreatableReplicaAssignment()
          .setPartitionIndex(partition)
          .setBrokerIds(replicas);
    }

    /** @return this assignment as the JSON of a fixture's {@code fields} */
    String json() {
      return obj(kv("partition", num(partition)), kv("replicas", intArray(replicas)));
    }
  }

  /** A name and a nullable value: a CreateTopics config entry. */
  private record Entry(String name, String value) {
    /** @return the Kafka {@code CreatableTopicConfig} for this entry */
    CreateTopicsRequestData.CreatableTopicConfig data() {
      return new CreateTopicsRequestData.CreatableTopicConfig().setName(name).setValue(value);
    }

    /** @return this entry as the JSON of a fixture's {@code fields} */
    String json() {
      return obj(kv("name", str(name)), kv("value", str(value)));
    }
  }

  /** One CreateTopics request topic. */
  private record NewTopic(
      String name, int numPartitions, int replicationFactor, List<Assignment> assignments,
      List<Entry> configs) {
    /** @return the Kafka {@code CreatableTopic} for this topic */
    CreateTopicsRequestData.CreatableTopic data() {
      return new CreateTopicsRequestData.CreatableTopic()
          .setName(name)
          .setNumPartitions(numPartitions)
          .setReplicationFactor((short) replicationFactor)
          .setAssignments(
              new CreateTopicsRequestData.CreatableReplicaAssignmentCollection(
                  assignments.stream().map(Assignment::data).iterator()))
          .setConfigs(
              new CreateTopicsRequestData.CreatableTopicConfigCollection(
                  configs.stream().map(Entry::data).iterator()));
    }

    /** @return this topic as the JSON of a fixture's {@code fields} */
    String json() {
      return obj(
          kv("topic", str(name)),
          kv("numPartitions", num(numPartitions)),
          kv("replicationFactor", num(replicationFactor)),
          kv("replicaAssignment", list(assignments, Assignment::json)),
          kv("configEntries", list(configs, Entry::json)));
    }
  }

  /** One config the broker read back for a created topic (v5+). */
  private record TopicConfig(
      String name, String value, boolean readOnly, int source, boolean sensitive) {
    /** @return the Kafka {@code CreatableTopicConfigs} for this config */
    CreateTopicsResponseData.CreatableTopicConfigs data() {
      return new CreateTopicsResponseData.CreatableTopicConfigs()
          .setName(name)
          .setValue(value)
          .setReadOnly(readOnly)
          .setConfigSource((byte) source)
          .setIsSensitive(sensitive);
    }

    /** @return this config as the JSON of a fixture's {@code fields} */
    String json() {
      return obj(
          kv("name", str(name)),
          kv("value", str(value)),
          kv("readOnly", String.valueOf(readOnly)),
          kv("configSource", num(source)),
          kv("isSensitive", String.valueOf(sensitive)));
    }
  }

  /** One CreateTopics request case: the topic it creates and whether it only validates. */
  private record CreateCase(String caseId, NewTopic topic, boolean validateOnly) {}

  /** One CreateTopics result topic; the fields after {@code errorMessage} are v5+, topicId v7. */
  private record CreatedTopic(
      String name, String topicId, int errorCode, String errorMessage, int topicConfigErrorCode,
      int numPartitions, int replicationFactor, List<TopicConfig> configs) {
    /**
     * @param version the response version, which decides the fields written
     * @return the Kafka {@code CreatableTopicResult} for this topic
     */
    CreateTopicsResponseData.CreatableTopicResult data(short version) {
      CreateTopicsResponseData.CreatableTopicResult data =
          new CreateTopicsResponseData.CreatableTopicResult()
              .setName(name)
              .setErrorCode((short) errorCode)
              .setErrorMessage(errorMessage);
      if (version >= 5) {
        data.setTopicConfigErrorCode((short) topicConfigErrorCode)
            .setNumPartitions(numPartitions)
            .setReplicationFactor((short) replicationFactor)
            .setConfigs(configs == null ? null : configs.stream().map(TopicConfig::data).toList());
      }
      if (version >= 7) {
        data.setTopicId(uuid(topicId));
      }
      return data;
    }

    /**
     * @param version the response version, which decides the fields written
     * @return this topic as the JSON of a fixture's {@code fields}
     */
    String json(short version) {
      List<String> entries = new ArrayList<>();
      entries.add(kv("topic", str(name)));
      if (version >= 7) {
        entries.add(kv("topicId", str(topicId)));
      }
      entries.add(kv("errorCode", num(errorCode)));
      entries.add(kv("errorMessage", str(errorMessage)));
      if (version >= 5) {
        entries.add(kv("topicConfigErrorCode", num(topicConfigErrorCode)));
        entries.add(kv("numPartitions", num(numPartitions)));
        entries.add(kv("replicationFactor", num(replicationFactor)));
        entries.add(kv("configs", configs == null ? "null" : list(configs, TopicConfig::json)));
      }
      return obj(entries.toArray(new String[0]));
    }
  }

  /**
   * CreateTopics (19) v4-v7, request and response (KIP-464, KIP-525, KIP-516).
   *
   * <p>v4 is v3's classic body; v5 is flexible and its response adds NumPartitions,
   * ReplicationFactor, a nullable Configs list and, in tag 0, TopicConfigErrorCode, which the
   * writer leaves out while it is 0 ({@code topicConfigError} is the case that sets it); v7 puts
   * TopicId between Name and ErrorCode. The request is identical at v5, v6 and v7. A failed topic
   * carries the zero TopicId, which the client decodes as null.
   *
   * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/CreateTopicsResponse.json">CreateTopicsResponse.json</a>
   * @return the key-19 vectors
   */
  private static List<Vector> createTopics() {
    List<Vector> vectors = new ArrayList<>();
    String api = "createTopics";
    NewTopic defaults = new NewTopic(TOPIC_NAME, -1, -1, List.of(), List.of());
    NewTopic explicit =
        new NewTopic(TOPIC_NAME, 3, 2, List.of(),
            List.of(new Entry("retention.ms", "60000"), new Entry("cleanup.policy", null)));
    NewTopic assigned =
        new NewTopic(TOPIC_NAME, -1, -1,
            List.of(new Assignment(0, List.of(1, 2)), new Assignment(1, List.of(2, 3))), List.of());
    List<TopicConfig> read =
        List.of(
            new TopicConfig("retention.ms", "60000", false, 1, false),
            new TopicConfig("cleanup.policy", "delete", false, 5, false));
    CreatedTopic ok = new CreatedTopic(TOPIC_NAME, TOPIC_ID, 0, null, 0, 3, 2, read);
    CreatedTopic exists =
        new CreatedTopic(TOPIC_NAME, null, TOPIC_ALREADY_EXISTS,
            "Topic 'test-topic' already exists.", 0, -1, -1, null);
    CreatedTopic configError =
        new CreatedTopic(TOPIC_NAME, TOPIC_ID, 0, null, TOPIC_AUTHORIZATION_FAILED, 3, 2, null);

    List<CreateCase> cases =
        List.of(
            new CreateCase("defaults", defaults, false),
            new CreateCase("explicit", explicit, true),
            new CreateCase("assigned", assigned, false));

    for (short version : new short[] {4, 5, 6, 7}) {
      for (CreateCase c : cases) {
        CreateTopicsRequestData data =
            new CreateTopicsRequestData()
                .setTopics(
                    new CreateTopicsRequestData.CreatableTopicCollection(
                        List.of(c.topic().data()).iterator()))
                .setTimeoutMs(ADMIN_TIMEOUT_MS)
                .setValidateOnly(c.validateOnly());
        vectors.add(
            new Vector(api, "CreateTopicsRequest", version, c.caseId(), data,
                obj(
                    kv("topics", array(c.topic().json())),
                    kv("timeout", num(ADMIN_TIMEOUT_MS)),
                    kv("validateOnly", String.valueOf(c.validateOnly())))));
      }

      vectors.add(createResponse(version, "ok", ok));
      vectors.add(createResponse(version, "error", exists));
      if (version >= 5) {
        vectors.add(createResponse(version, "topicConfigError", configError));
      }
    }

    return vectors;
  }

  /**
   * @param version the response version
   * @param caseId the case name
   * @param topic the one topic result
   * @return one CreateTopics response vector
   */
  private static Vector createResponse(short version, String caseId, CreatedTopic topic) {
    CreateTopicsResponseData data =
        new CreateTopicsResponseData()
            .setThrottleTimeMs(THROTTLE_TIME_MS)
            .setTopics(
                new CreateTopicsResponseData.CreatableTopicResultCollection(
                    List.of(topic.data(version)).iterator()));
    return new Vector("createTopics", "CreateTopicsResponse", version, caseId, data,
        obj(
            kv("throttleTime", num(THROTTLE_TIME_MS)),
            kv("topicErrors", array(topic.json(version)))));
  }

  /** One DeleteTopics result topic; topicId is v6, errorMessage v5. */
  private record DeletedTopic(String name, String topicId, int errorCode, String errorMessage) {
    /**
     * @param version the response version, which decides the fields written
     * @return the Kafka {@code DeletableTopicResult} for this topic
     */
    DeleteTopicsResponseData.DeletableTopicResult data(short version) {
      DeleteTopicsResponseData.DeletableTopicResult data =
          new DeleteTopicsResponseData.DeletableTopicResult()
              .setName(name)
              .setErrorCode((short) errorCode);
      if (version >= 5) {
        data.setErrorMessage(errorMessage);
      }
      if (version >= 6) {
        data.setTopicId(uuid(topicId));
      }
      return data;
    }

    /**
     * @param version the response version, which decides the fields written
     * @return this topic as the JSON of a fixture's {@code fields}
     */
    String json(short version) {
      List<String> entries = new ArrayList<>();
      entries.add(kv("topic", str(name)));
      if (version >= 6) {
        entries.add(kv("topicId", str(topicId)));
      }
      entries.add(kv("errorCode", num(errorCode)));
      if (version >= 5) {
        entries.add(kv("errorMessage", str(errorMessage)));
      }
      return obj(entries.toArray(new String[0]));
    }
  }

  /**
   * DeleteTopics (20) v2-v6, request and response.
   *
   * <p>v2 is v1's body, v3 may answer TOPIC_DELETION_DISABLED, v4 is flexible, v5 adds ErrorMessage
   * and v6 replaces TopicNames with Topics[{Name, TopicId}] and puts TopicId between Name and
   * ErrorCode in the answer. The client deletes by name, so every request case names two topics
   * and, from v6, sends each with the zero TopicId; the builder takes names at every version.
   *
   * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/DeleteTopicsRequest.json">DeleteTopicsRequest.json</a>
   * @return the key-20 vectors
   */
  private static List<Vector> deleteTopics() {
    List<Vector> vectors = new ArrayList<>();
    String api = "deleteTopics";
    DeletedTopic ok = new DeletedTopic(TOPIC_NAME, TOPIC_ID, 0, null);
    DeletedTopic okOther = new DeletedTopic(OTHER_TOPIC_NAME, OTHER_TOPIC_ID, 0, null);
    DeletedTopic unknown =
        new DeletedTopic(TOPIC_NAME, null, UNKNOWN_TOPIC_OR_PARTITION,
            "This server does not host this topic-partition.");
    DeletedTopic disabled =
        new DeletedTopic(
            OTHER_TOPIC_NAME, null, TOPIC_DELETION_DISABLED, "Topic deletion is disabled.");

    for (short version : new short[] {2, 3, 4, 5, 6}) {
      DeleteTopicsRequestData request =
          new DeleteTopicsRequestData().setTimeoutMs(ADMIN_TIMEOUT_MS);
      if (version >= 6) {
        request.setTopics(
            List.of(
                new DeleteTopicsRequestData.DeleteTopicState()
                    .setName(TOPIC_NAME)
                    .setTopicId(Uuid.ZERO_UUID),
                new DeleteTopicsRequestData.DeleteTopicState()
                    .setName(OTHER_TOPIC_NAME)
                    .setTopicId(Uuid.ZERO_UUID)));
      } else {
        request.setTopicNames(List.of(TOPIC_NAME, OTHER_TOPIC_NAME));
      }
      vectors.add(
          new Vector(api, "DeleteTopicsRequest", version, version >= 6 ? "byName" : "names",
              request,
              obj(
                  kv("topics", strArray(List.of(TOPIC_NAME, OTHER_TOPIC_NAME))),
                  kv("timeout", num(ADMIN_TIMEOUT_MS)))));

      vectors.add(deleteResponse(version, "ok", ok, okOther));
      vectors.add(deleteResponse(version, "error", unknown));
      if (version >= 3) {
        vectors.add(deleteResponse(version, "deletionDisabled", disabled));
      }
    }

    return vectors;
  }

  /**
   * @param version the response version
   * @param caseId the case name
   * @param topics one result per deleted topic
   * @return one DeleteTopics response vector
   */
  private static Vector deleteResponse(short version, String caseId, DeletedTopic... topics) {
    DeleteTopicsResponseData data =
        new DeleteTopicsResponseData()
            .setThrottleTimeMs(THROTTLE_TIME_MS)
            .setResponses(
                new DeleteTopicsResponseData.DeletableTopicResultCollection(
                    Arrays.stream(topics).map(t -> t.data(version)).iterator()));
    return new Vector("deleteTopics", "DeleteTopicsResponse", version, caseId, data,
        obj(
            kv("throttleTime", num(THROTTLE_TIME_MS)),
            kv("topicErrors", list(Arrays.asList(topics), t -> t.json(version)))));
  }

  /**
   * CreatePartitions (37) v2-v3, request and response.
   *
   * <p>Assignments is a nullable array: {@code noAssignments} writes it null (a compact null is
   * {@code 00}, which the 4.3 controller reads as "choose the replicas") and
   * {@code explicitEmptyAssignments} writes an empty list ({@code 01}, which it reads as an
   * explicit and wrong assignment). The second exists to be told apart from the first: a client
   * writes neither an absent nor an empty list as {@code 01}, so its {@code fields} are not builder
   * input.
   *
   * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/CreatePartitionsRequest.json">CreatePartitionsRequest.json</a>
   * @return the key-37 vectors
   */
  private static List<Vector> createPartitions() {
    List<Vector> vectors = new ArrayList<>();
    String api = "createPartitions";
    List<Expansion> expansions =
        List.of(
            new Expansion("noAssignments", null, "null"),
            new Expansion("explicitEmptyAssignments", List.of(), "[]"),
            new Expansion(
                "assignments", List.of(List.of(1, 2), List.of(2, 3)), "[[1, 2], [2, 3]]"));

    for (short version : new short[] {2, 3}) {
      for (Expansion e : expansions) {
        CreatePartitionsRequestData data =
            new CreatePartitionsRequestData()
                .setTopics(
                    new CreatePartitionsRequestData.CreatePartitionsTopicCollection(
                        List.of(
                                new CreatePartitionsRequestData.CreatePartitionsTopic()
                                    .setName(TOPIC_NAME)
                                    .setCount(3)
                                    .setAssignments(
                                        e.assignments() == null
                                            ? null
                                            : e.assignments().stream()
                                                .map(
                                                    brokerIds ->
                                                        new CreatePartitionsRequestData
                                                                .CreatePartitionsAssignment()
                                                            .setBrokerIds(brokerIds))
                                                .toList()))
                            .iterator()))
                .setTimeoutMs(ADMIN_TIMEOUT_MS)
                .setValidateOnly(false);
        vectors.add(
            new Vector(api, "CreatePartitionsRequest", version, e.caseId(), data,
                obj(
                    kv("topicPartitions",
                        array(
                            obj(
                                kv("topic", str(TOPIC_NAME)),
                                kv("count", "3"),
                                kv("assignments", e.json())))),
                    kv("timeout", num(ADMIN_TIMEOUT_MS)),
                    kv("validateOnly", "false"))));
      }

      vectors.add(createPartitionsResponse(version, "ok", (short) 0, null));
      vectors.add(
          createPartitionsResponse(
              version, "error", INVALID_PARTITIONS, "Topic already has 3 partitions."));
    }

    return vectors;
  }

  /** One CreatePartitions request case: the Assignments it writes (null for none), and its JSON. */
  private record Expansion(String caseId, List<List<Integer>> assignments, String json) {}

  /**
   * @param version the response version
   * @param caseId the case name
   * @param errorCode the topic's error code
   * @param errorMessage the topic's error message, or null
   * @return one CreatePartitions response vector
   */
  private static Vector createPartitionsResponse(
      short version, String caseId, short errorCode, String errorMessage) {
    return new Vector("createPartitions", "CreatePartitionsResponse", version, caseId,
        new CreatePartitionsResponseData()
            .setThrottleTimeMs(THROTTLE_TIME_MS)
            .setResults(
                List.of(
                    new CreatePartitionsResponseData.CreatePartitionsTopicResult()
                        .setName(TOPIC_NAME)
                        .setErrorCode(errorCode)
                        .setErrorMessage(errorMessage))),
        obj(
            kv("throttleTime", num(THROTTLE_TIME_MS)),
            kv("topicErrors",
                array(
                    obj(
                        kv("topic", str(TOPIC_NAME)),
                        kv("errorCode", num(errorCode)),
                        kv("errorMessage", str(errorMessage)))))));
  }

  /**
   * DeleteRecords (21) v2, request and response: v1's body in flexible framing. Two partitions are
   * truncated, one to an offset and one to -1 (the high watermark); the answer carries each
   * partition's new low watermark, or OFFSET_OUT_OF_RANGE (1).
   *
   * @return the key-21 vectors
   */
  private static List<Vector> deleteRecords() {
    List<Vector> vectors = new ArrayList<>();
    String api = "deleteRecords";

    DeleteRecordsRequestData request =
        new DeleteRecordsRequestData()
            .setTopics(
                List.of(
                    new DeleteRecordsRequestData.DeleteRecordsTopic()
                        .setName(TOPIC_NAME)
                        .setPartitions(List.of(truncation(0, 5L), truncation(2, -1L)))))
            .setTimeoutMs(ADMIN_TIMEOUT_MS);
    String requested =
        array(
            obj(kv("partition", "0"), kv("offset", str("5"))),
            obj(kv("partition", "2"), kv("offset", str("-1"))));
    vectors.add(
        new Vector(api, "DeleteRecordsRequest", (short) 2, "basic", request,
            obj(
                kv("topics", array(obj(kv("topic", str(TOPIC_NAME)), kv("partitions", requested)))),
                kv("timeout", num(ADMIN_TIMEOUT_MS)))));

    for (short code : new short[] {0, 1}) {
      long low = code == 0 ? 12L : -1L;
      DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection partitions =
          new DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection(
              List.of(result(0, 5L, (short) 0), result(2, low, code)).iterator());
      DeleteRecordsResponseData response =
          new DeleteRecordsResponseData()
              .setThrottleTimeMs(THROTTLE_TIME_MS)
              .setTopics(
                  new DeleteRecordsResponseData.DeleteRecordsTopicResultCollection(
                      List.of(
                              new DeleteRecordsResponseData.DeleteRecordsTopicResult()
                                  .setName(TOPIC_NAME)
                                  .setPartitions(partitions))
                          .iterator()));
      String answered =
          array(
              obj(kv("partition", "0"), kv("lowWatermark", str("5")), kv("errorCode", "0")),
              obj(kv("partition", "2"), kv("lowWatermark", str(String.valueOf(low))),
                  kv("errorCode", num(code))));
      vectors.add(
          new Vector(api, "DeleteRecordsResponse", (short) 2, code == 0 ? "ok" : "error", response,
              obj(
                  kv("throttleTime", num(THROTTLE_TIME_MS)),
                  kv("topics",
                      array(obj(kv("topic", str(TOPIC_NAME)), kv("partitions", answered)))))));
    }

    return vectors;
  }

  /**
   * @param partition the partition index
   * @param offset the offset to truncate before
   * @return one DeleteRecords request partition
   */
  private static DeleteRecordsRequestData.DeleteRecordsPartition truncation(
      int partition, long offset) {
    return new DeleteRecordsRequestData.DeleteRecordsPartition()
        .setPartitionIndex(partition)
        .setOffset(offset);
  }

  /**
   * @param partition the partition index
   * @param lowWatermark the new low watermark
   * @param errorCode the partition's error code
   * @return one DeleteRecords response partition
   */
  private static DeleteRecordsResponseData.DeleteRecordsPartitionResult result(
      int partition, long lowWatermark, short errorCode) {
    return new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
        .setPartitionIndex(partition)
        .setLowWatermark(lowWatermark)
        .setErrorCode(errorCode);
  }

  /**
   * DeleteGroups (42) v2, request and response: v1's body in flexible framing. Two groups are
   * deleted; the answer is both deleted ({@code ok}) or one refused as non-empty and one unknown
   * ({@code error}).
   *
   * @return the key-42 vectors
   */
  private static List<Vector> deleteGroups() {
    List<Vector> vectors = new ArrayList<>();
    String api = "deleteGroups";

    vectors.add(
        new Vector(api, "DeleteGroupsRequest", (short) 2, "basic",
            new DeleteGroupsRequestData().setGroupsNames(List.of(GROUP_ID, OTHER_GROUP_ID)),
            obj(kv("groupIds", strArray(List.of(GROUP_ID, OTHER_GROUP_ID))))));

    for (short[] codes : new short[][] {{0, 0}, {NON_EMPTY_GROUP, GROUP_ID_NOT_FOUND}}) {
      DeleteGroupsResponseData response =
          new DeleteGroupsResponseData()
              .setThrottleTimeMs(THROTTLE_TIME_MS)
              .setResults(
                  new DeleteGroupsResponseData.DeletableGroupResultCollection(
                      List.of(deleted(GROUP_ID, codes[0]), deleted(OTHER_GROUP_ID, codes[1]))
                          .iterator()));
      String results =
          array(
              obj(kv("groupId", str(GROUP_ID)), kv("errorCode", num(codes[0]))),
              obj(kv("groupId", str(OTHER_GROUP_ID)), kv("errorCode", num(codes[1]))));
      vectors.add(
          new Vector(api, "DeleteGroupsResponse", (short) 2, codes[0] == 0 ? "ok" : "error",
              response,
              obj(kv("throttleTime", num(THROTTLE_TIME_MS)), kv("results", results))));
    }

    return vectors;
  }

  /**
   * @param groupId the group
   * @param code its error code
   * @return one DeleteGroups response result
   */
  private static DeleteGroupsResponseData.DeletableGroupResult deleted(
      String groupId, short code) {
    return new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId(groupId)
        .setErrorCode(code);
  }

  /** One ACL (or ACL filter): the seven values the three ACL APIs share. */
  private record Acl(
      int resourceType, String resourceName, int patternType, String principal, String host,
      int operation, int permissionType) {
    /** @return this ACL as the JSON of a fixture's {@code fields} */
    String json() {
      return obj(
          kv("resourceType", num(resourceType)),
          kv("resourceName", str(resourceName)),
          kv("resourcePatternType", num(patternType)),
          kv("principal", str(principal)),
          kv("host", str(host)),
          kv("operation", num(operation)),
          kv("permissionType", num(permissionType)));
    }
  }

  /** One ACL case: its name and the ACL every API of the case carries. */
  private record AclCase(String caseId, Acl acl) {}

  /**
   * A DescribeAcls response resource carrying one ACL description.
   *
   * @param acl the resource and the ACL bound to it
   * @return the Kafka {@code DescribeAclsResource} holding one {@code AclDescription}
   */
  private static DescribeAclsResponseData.DescribeAclsResource describedResource(Acl acl) {
    return new DescribeAclsResponseData.DescribeAclsResource()
        .setResourceType((byte) acl.resourceType())
        .setResourceName(acl.resourceName())
        .setPatternType((byte) acl.patternType())
        .setAcls(
            List.of(
                new DescribeAclsResponseData.AclDescription()
                    .setPrincipal(acl.principal())
                    .setHost(acl.host())
                    .setOperation((byte) acl.operation())
                    .setPermissionType((byte) acl.permissionType())));
  }

  /**
   * Describe, Create and DeleteAcls (29, 30, 31) v2-v3: v2 is flexible and v3 is v2's body, which
   * now accepts resource type 7 (USER). {@code basic} is a topic ACL; at v3 {@code userResource}
   * is the same with a USER resource, so the pair differs in the resource type byte (and name)
   * alone. The two filter APIs also carry {@code anyFilter}, whose nullable strings are null.
   *
   * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/CreateAclsRequest.json">CreateAclsRequest.json</a>
   * @return the key-29, 30 and 31 vectors
   */
  private static List<Vector> acls() {
    List<Vector> vectors = new ArrayList<>();

    for (short version : new short[] {2, 3}) {
      List<AclCase> aclCases = new ArrayList<>(List.of(new AclCase("basic", TOPIC_ACL)));
      if (version >= 3) {
        aclCases.add(new AclCase("userResource", USER_ACL));
      }
      for (AclCase c : aclCases) {
        String caseId = c.caseId();
        Acl acl = c.acl();
        vectors.add(describeAclsRequest(version, caseId, acl));
        vectors.add(describeAclsResponse(version, caseId, acl));
        vectors.add(createAclsRequest(version, caseId, acl));
        vectors.add(deleteAclsRequest(version, caseId, acl));
        vectors.add(deleteAclsResponse(version, caseId, acl));
      }

      vectors.add(describeAclsRequest(version, "anyFilter", ANY_FILTER));
      vectors.add(deleteAclsRequest(version, "anyFilter", ANY_FILTER));

      for (String caseId : new String[] {"ok", "error"}) {
        boolean ok = caseId.equals("ok");
        short code = ok ? 0 : INVALID_REQUEST;
        String message = ok ? null : "Invalid resource name";
        vectors.add(
            new Vector("createAcls", "CreateAclsResponse", version, caseId,
                new CreateAclsResponseData()
                    .setThrottleTimeMs(THROTTLE_TIME_MS)
                    .setResults(
                        List.of(
                            new CreateAclsResponseData.AclCreationResult()
                                .setErrorCode(code)
                                .setErrorMessage(message))),
                obj(
                    kv("throttleTime", num(THROTTLE_TIME_MS)),
                    kv("creationResponses",
                        array(
                            obj(kv("errorCode", num(code)), kv("errorMessage", str(message))))))));
      }
    }

    return vectors;
  }

  /**
   * @param version the response version
   * @param caseId the case name
   * @param acl the one ACL the matching resource carries
   * @return one DescribeAcls response vector
   */
  private static Vector describeAclsResponse(short version, String caseId, Acl acl) {
    String described =
        obj(
            kv("principal", str(acl.principal())),
            kv("host", str(acl.host())),
            kv("operation", num(acl.operation())),
            kv("permissionType", num(acl.permissionType())));
    return new Vector("describeAcls", "DescribeAclsResponse", version, caseId,
        new DescribeAclsResponseData()
            .setThrottleTimeMs(THROTTLE_TIME_MS)
            .setErrorCode((short) 0)
            .setErrorMessage(null)
            .setResources(List.of(describedResource(acl))),
        obj(
            kv("throttleTime", num(THROTTLE_TIME_MS)),
            kv("errorCode", "0"),
            kv("errorMessage", "null"),
            kv("resources",
                array(
                    obj(
                        kv("resourceType", num(acl.resourceType())),
                        kv("resourceName", str(acl.resourceName())),
                        kv("resourcePatternType", num(acl.patternType())),
                        kv("acls", array(described)))))));
  }

  /**
   * @param version the request version
   * @param caseId the case name
   * @param acl the one ACL to create
   * @return one CreateAcls request vector
   */
  private static Vector createAclsRequest(short version, String caseId, Acl acl) {
    return new Vector("createAcls", "CreateAclsRequest", version, caseId,
        new CreateAclsRequestData()
            .setCreations(
                List.of(
                    new CreateAclsRequestData.AclCreation()
                        .setResourceType((byte) acl.resourceType())
                        .setResourceName(acl.resourceName())
                        .setResourcePatternType((byte) acl.patternType())
                        .setPrincipal(acl.principal())
                        .setHost(acl.host())
                        .setOperation((byte) acl.operation())
                        .setPermissionType((byte) acl.permissionType()))),
        obj(kv("creations", array(acl.json()))));
  }

  /**
   * @param version the request version
   * @param caseId the case name
   * @param acl the filter to describe
   * @return one DescribeAcls request vector
   */
  private static Vector describeAclsRequest(short version, String caseId, Acl acl) {
    return new Vector("describeAcls", "DescribeAclsRequest", version, caseId,
        new DescribeAclsRequestData()
            .setResourceTypeFilter((byte) acl.resourceType())
            .setResourceNameFilter(acl.resourceName())
            .setPatternTypeFilter((byte) acl.patternType())
            .setPrincipalFilter(acl.principal())
            .setHostFilter(acl.host())
            .setOperation((byte) acl.operation())
            .setPermissionType((byte) acl.permissionType()),
        acl.json());
  }

  /**
   * @param version the request version
   * @param caseId the case name
   * @param acl the filter to delete by
   * @return one DeleteAcls request vector
   */
  private static Vector deleteAclsRequest(short version, String caseId, Acl acl) {
    return new Vector("deleteAcls", "DeleteAclsRequest", version, caseId,
        new DeleteAclsRequestData()
            .setFilters(
                List.of(
                    new DeleteAclsRequestData.DeleteAclsFilter()
                        .setResourceTypeFilter((byte) acl.resourceType())
                        .setResourceNameFilter(acl.resourceName())
                        .setPatternTypeFilter((byte) acl.patternType())
                        .setPrincipalFilter(acl.principal())
                        .setHostFilter(acl.host())
                        .setOperation((byte) acl.operation())
                        .setPermissionType((byte) acl.permissionType()))),
        obj(kv("filters", array(acl.json()))));
  }

  /**
   * @param version the response version
   * @param caseId the case name
   * @param acl the one ACL the filter matched and deleted
   * @return one DeleteAcls response vector
   */
  private static Vector deleteAclsResponse(short version, String caseId, Acl acl) {
    return new Vector("deleteAcls", "DeleteAclsResponse", version, caseId,
        new DeleteAclsResponseData()
            .setThrottleTimeMs(THROTTLE_TIME_MS)
            .setFilterResults(
                List.of(
                    new DeleteAclsResponseData.DeleteAclsFilterResult()
                        .setErrorCode((short) 0)
                        .setErrorMessage(null)
                        .setMatchingAcls(
                            List.of(
                                new DeleteAclsResponseData.DeleteAclsMatchingAcl()
                                    .setErrorCode((short) 0)
                                    .setErrorMessage(null)
                                    .setResourceType((byte) acl.resourceType())
                                    .setResourceName(acl.resourceName())
                                    .setPatternType((byte) acl.patternType())
                                    .setPrincipal(acl.principal())
                                    .setHost(acl.host())
                                    .setOperation((byte) acl.operation())
                                    .setPermissionType((byte) acl.permissionType()))))),
        obj(
            kv("throttleTime", num(THROTTLE_TIME_MS)),
            kv("filterResponses",
                array(
                    obj(
                        kv("errorCode", "0"),
                        kv("errorMessage", "null"),
                        kv("matchingAcls",
                            array(
                                obj(
                                    kv("errorCode", "0"),
                                    kv("errorMessage", "null"),
                                    kv("resourceType", num(acl.resourceType())),
                                    kv("resourceName", str(acl.resourceName())),
                                    kv("resourcePatternType", num(acl.patternType())),
                                    kv("principal", str(acl.principal())),
                                    kv("host", str(acl.host())),
                                    kv("operation", num(acl.operation())),
                                    kv("permissionType", num(acl.permissionType()))))))))));
  }

  /**
   * A DescribeTopicPartitions cursor: where the next page starts.
   *
   * @param topicName the topic the next page starts in
   * @param partitionIndex the first partition of that topic the next page returns
   * @return the cursor as the JSON of a fixture's {@code fields}
   */
  private static String cursorJson(String topicName, int partitionIndex) {
    return obj(kv("topicName", str(topicName)), kv("partitionIndex", num(partitionIndex)));
  }

  /** One DescribeTopicPartitions partition row; the ELR lists are nullable. */
  private record DtpPartition(
      int partition, int leader, int leaderEpoch, List<Integer> replicas, List<Integer> isr,
      List<Integer> eligibleLeaderReplicas, List<Integer> lastKnownElr, List<Integer> offline) {
    /** @return the Kafka {@code DescribeTopicPartitionsResponsePartition} for this partition */
    DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition data() {
      return new DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition()
          .setErrorCode((short) 0)
          .setPartitionIndex(partition)
          .setLeaderId(leader)
          .setLeaderEpoch(leaderEpoch)
          .setReplicaNodes(replicas)
          .setIsrNodes(isr)
          .setEligibleLeaderReplicas(eligibleLeaderReplicas)
          .setLastKnownElr(lastKnownElr)
          .setOfflineReplicas(offline);
    }

    /** @return this partition as the JSON of a fixture's {@code fields} */
    String json() {
      return obj(
          kv("errorCode", "0"),
          kv("partitionIndex", num(partition)),
          kv("leaderId", num(leader)),
          kv("leaderEpoch", num(leaderEpoch)),
          kv("replicaNodes", intArray(replicas)),
          kv("isrNodes", intArray(isr)),
          kv("eligibleLeaderReplicas", intArray(eligibleLeaderReplicas)),
          kv("lastKnownElr", intArray(lastKnownElr)),
          kv("offlineReplicas", intArray(offline)));
    }
  }

  /** One DescribeTopicPartitions topic. */
  private record DtpTopic(
      int errorCode, String name, String topicId, boolean internal, List<DtpPartition> partitions,
      int authorizedOperations) {
    /** @return the Kafka {@code DescribeTopicPartitionsResponseTopic} for this topic */
    DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic data() {
      return new DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic()
          .setErrorCode((short) errorCode)
          .setName(name)
          .setTopicId(uuid(topicId))
          .setIsInternal(internal)
          .setPartitions(partitions.stream().map(DtpPartition::data).toList())
          .setTopicAuthorizedOperations(authorizedOperations);
    }

    /** @return this topic as the JSON of a fixture's {@code fields} */
    String json() {
      return obj(
          kv("errorCode", num(errorCode)),
          kv("name", str(name)),
          kv("topicId", str(topicId)),
          kv("isInternal", String.valueOf(internal)),
          kv("partitions", list(partitions, DtpPartition::json)),
          kv("topicAuthorizedOperations", num(authorizedOperations)));
    }
  }

  /**
   * DescribeTopicPartitions (75) v0 (KIP-966), request and response. The message is flexible from
   * v0.
   *
   * <p>Cursor and NextCursor are nullable structs, which carry a presence byte the schema does not
   * show: {@code ff} for null, {@code 01} before a present struct. {@code noCursor} and
   * {@code cursor} differ in it. The ELR lists are nullable arrays: {@code elrNull} writes
   * {@code 00}, {@code elrEmpty} writes {@code 01}, {@code elrPopulated} a list. A topic the caller
   * may not describe comes back with its name, the zero TopicId (read as null), no partitions and
   * the unset authorized-operations value ({@code unauthorizedZeroId}).
   *
   * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/DescribeTopicPartitionsResponse.json">DescribeTopicPartitionsResponse.json</a>
   * @return the key-75 vectors
   */
  private static List<Vector> describeTopicPartitions() {
    List<Vector> vectors = new ArrayList<>();
    String api = "describeTopicPartitions";
    short v0 = 0;

    vectors.add(
        new Vector(api, "DescribeTopicPartitionsRequest", v0, "noCursor",
            new DescribeTopicPartitionsRequestData()
                .setTopics(
                    List.of(
                        new DescribeTopicPartitionsRequestData.TopicRequest()
                            .setName(OTHER_TOPIC_NAME),
                        new DescribeTopicPartitionsRequestData.TopicRequest()
                            .setName(TOPIC_NAME)))
                .setResponsePartitionLimit(2000)
                .setCursor(null),
            obj(
                kv("topics", strArray(List.of(OTHER_TOPIC_NAME, TOPIC_NAME))),
                kv("responsePartitionLimit", "2000"),
                kv("cursor", "null"))));
    vectors.add(
        new Vector(api, "DescribeTopicPartitionsRequest", v0, "cursor",
            new DescribeTopicPartitionsRequestData()
                .setTopics(
                    List.of(
                        new DescribeTopicPartitionsRequestData.TopicRequest()
                            .setName(TOPIC_NAME)))
                .setResponsePartitionLimit(1)
                .setCursor(
                    new DescribeTopicPartitionsRequestData.Cursor()
                        .setTopicName(TOPIC_NAME)
                        .setPartitionIndex(2)),
            obj(
                kv("topics", strArray(List.of(TOPIC_NAME))),
                kv("responsePartitionLimit", "1"),
                kv("cursor", cursorJson(TOPIC_NAME, 2)))));

    List<Integer> replicas = List.of(1, 2, 3);
    List<Integer> isr = List.of(1, 2);
    List<Integer> offline = List.of(3);
    DtpTopic elrNull =
        new DtpTopic(0, TOPIC_NAME, TOPIC_ID, false,
            List.of(new DtpPartition(0, 1, 5, replicas, isr, null, null, offline)),
            NO_AUTHORIZED_OPERATIONS);
    DtpTopic elrEmpty =
        new DtpTopic(0, TOPIC_NAME, TOPIC_ID, false,
            List.of(new DtpPartition(0, 1, 5, replicas, isr, List.of(), List.of(), offline)),
            NO_AUTHORIZED_OPERATIONS);
    DtpTopic elrPopulated =
        new DtpTopic(0, TOPIC_NAME, TOPIC_ID, false,
            List.of(new DtpPartition(0, 1, 5, replicas, isr, List.of(1), List.of(2), offline)),
            NO_AUTHORIZED_OPERATIONS);
    DtpTopic unauthorized =
        new DtpTopic(TOPIC_AUTHORIZATION_FAILED, TOPIC_NAME, null, false, List.of(),
            NO_AUTHORIZED_OPERATIONS);
    DtpTopic internal =
        new DtpTopic(0, "__consumer_offsets", OTHER_TOPIC_ID, true,
            List.of(new DtpPartition(0, 2, 7, replicas, isr, null, null, List.of())),
            NO_AUTHORIZED_OPERATIONS);

    vectors.add(dtpResponse("elrNull", null, elrNull));
    vectors.add(dtpResponse("elrEmpty", null, elrEmpty));
    vectors.add(dtpResponse("elrPopulated", null, elrPopulated));
    vectors.add(dtpResponse("nextCursor", cursorJson(TOPIC_NAME, 1), elrNull));
    vectors.add(dtpResponse("noNextCursor", null, elrNull, internal));
    vectors.add(dtpResponse("unauthorizedZeroId", null, unauthorized));

    return vectors;
  }

  /**
   * @param caseId the case name
   * @param nextCursorJson the {@code nextCursor} fields JSON, or null for no next cursor
   * @param topics the described topics
   * @return one DescribeTopicPartitions v0 response vector
   */
  private static Vector dtpResponse(String caseId, String nextCursorJson, DtpTopic... topics) {
    DescribeTopicPartitionsResponseData data =
        new DescribeTopicPartitionsResponseData()
            .setThrottleTimeMs(THROTTLE_TIME_MS)
            .setTopics(
                new DescribeTopicPartitionsResponseData
                    .DescribeTopicPartitionsResponseTopicCollection(
                    Arrays.stream(topics).map(DtpTopic::data).iterator()))
            .setNextCursor(
                nextCursorJson == null
                    ? null
                    : new DescribeTopicPartitionsResponseData.Cursor()
                        .setTopicName(TOPIC_NAME)
                        .setPartitionIndex(1));
    return new Vector(
        "describeTopicPartitions", "DescribeTopicPartitionsResponse", (short) 0, caseId,
        data,
        obj(
            kv("throttleTime", num(THROTTLE_TIME_MS)),
            kv("topics", list(Arrays.asList(topics), DtpTopic::json)),
            kv("nextCursor", nextCursorJson == null ? "null" : nextCursorJson)));
  }

  /** A JoinGroup protocol: a name and its subscription bytes. */
  private record JoinProtocol(String name, byte[] metadata) {
    /** @return the Kafka {@code JoinGroupRequestProtocol} for this protocol */
    JoinGroupRequestData.JoinGroupRequestProtocol data() {
      return new JoinGroupRequestData.JoinGroupRequestProtocol()
          .setName(name)
          .setMetadata(metadata);
    }

    /** @return this protocol as the JSON of a fixture's {@code fields} */
    String json() {
      return obj(kv("name", str(name)), kv("metadata", str(hex(metadata))));
    }
  }

  /** @param bytes any bytes @return lower-case hex, the form {@code fields} carries bytes in */
  private static String hex(byte[] bytes) {
    StringBuilder hex = new StringBuilder();
    for (byte b : bytes) {
      hex.append(String.format("%02x", b));
    }
    return hex.toString();
  }

  /** One member of a JoinGroup response. */
  private record JoinMember(String memberId, String groupInstanceId, byte[] metadata) {
    /** @return the Kafka {@code JoinGroupResponseMember} for this member */
    JoinGroupResponseData.JoinGroupResponseMember data() {
      return new JoinGroupResponseData.JoinGroupResponseMember()
          .setMemberId(memberId)
          .setGroupInstanceId(groupInstanceId)
          .setMetadata(metadata);
    }

    /** @return this member as the JSON of a fixture's {@code fields} */
    String json() {
      return obj(
          kv("memberId", str(memberId)),
          kv("groupInstanceId", str(groupInstanceId)),
          kv("memberMetadata", str(hex(metadata))));
    }
  }

  /**
   * JoinGroup (11) v6-v8, request and response (KIP-345, KIP-800, KIP-814).
   *
   * <p>v6 is v5's body in flexible framing; v7 adds ProtocolType to the response and makes its
   * ProtocolName nullable; v8 adds a nullable Reason as the request's last field. The client sends
   * a null Reason, which is {@code basic}'s (a v8 default), so {@code withReason} is the case that
   * shows where the field sits. v9 is not generated: its SkipAssignment is not used.
   *
   * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/JoinGroupResponse.json">JoinGroupResponse.json</a>
   * @return the key-11 vectors
   */
  private static List<Vector> joinGroup() {
    List<Vector> vectors = new ArrayList<>();
    List<JoinProtocol> protocols = List.of(new JoinProtocol(ASSIGNOR_NAME, MEMBER_BYTES));
    List<JoinMember> members =
        List.of(
            new JoinMember(MEMBER_ID, null, MEMBER_BYTES),
            new JoinMember("other-member", GROUP_INSTANCE_ID, MEMBER_BYTES));

    for (short version : new short[] {6, 7, 8}) {
      vectors.add(joinRequest(version, "basic", "", null, null, protocols));
      vectors.add(
          joinRequest(version, "staticMember", MEMBER_ID, GROUP_INSTANCE_ID, null, protocols));
      if (version >= 8) {
        vectors.add(
            joinRequest(version, "withReason", MEMBER_ID, GROUP_INSTANCE_ID,
                "the consumer is being closed", protocols));
      }

      vectors.add(joinResponse(version, "ok", 0, 5, null, ASSIGNOR_NAME, MEMBER_ID, members));
      if (version >= 7) {
        vectors.add(
            joinResponse(version, "protocolType", 0, 5, PROTOCOL_TYPE, ASSIGNOR_NAME, MEMBER_ID,
                members));
        vectors.add(
            joinResponse(version, "nullProtocolName", MEMBER_ID_REQUIRED, -1, PROTOCOL_TYPE, null,
                MEMBER_ID, List.of()));
      }
    }

    return vectors;
  }

  /**
   * @param version the request version
   * @param caseId the case name
   * @param memberId the member id, empty on a first join
   * @param groupInstanceId the static membership id, or null
   * @param reason the join reason (v8+), or null
   * @param protocols the offered protocols
   * @return one JoinGroup request vector
   */
  private static Vector joinRequest(
      short version, String caseId, String memberId, String groupInstanceId, String reason,
      List<JoinProtocol> protocols) {
    JoinGroupRequestData data =
        new JoinGroupRequestData()
            .setGroupId(GROUP_ID)
            .setSessionTimeoutMs(30000)
            .setRebalanceTimeoutMs(60000)
            .setMemberId(memberId)
            .setGroupInstanceId(groupInstanceId)
            .setProtocolType(PROTOCOL_TYPE)
            .setProtocols(
                new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
                    protocols.stream().map(JoinProtocol::data).iterator()));
    List<String> entries =
        new ArrayList<>(
            List.of(
                kv("groupId", str(GROUP_ID)),
                kv("sessionTimeout", "30000"),
                kv("rebalanceTimeout", "60000"),
                kv("memberId", str(memberId)),
                kv("groupInstanceId", str(groupInstanceId)),
                kv("protocolType", str(PROTOCOL_TYPE)),
                kv("groupProtocols", list(protocols, JoinProtocol::json))));
    if (version >= 8) {
      data.setReason(reason);
      entries.add(kv("reason", str(reason)));
    }
    return new Vector("joinGroup", "JoinGroupRequest", version, caseId, data,
        obj(entries.toArray(new String[0])));
  }

  /**
   * @param version the response version
   * @param caseId the case name
   * @param errorCode the error code
   * @param generationId the generation
   * @param protocolType the chosen protocol type (v7+), or null
   * @param protocolName the chosen protocol name, or null
   * @param memberId the assigned member id
   * @param members the members the response lists
   * @return one JoinGroup response vector
   */
  private static Vector joinResponse(
      short version, String caseId, int errorCode, int generationId, String protocolType,
      String protocolName, String memberId, List<JoinMember> members) {
    JoinGroupResponseData data =
        new JoinGroupResponseData()
            .setThrottleTimeMs(THROTTLE_TIME_MS)
            .setErrorCode((short) errorCode)
            .setGenerationId(generationId)
            .setProtocolName(protocolName)
            .setLeader(errorCode == 0 ? MEMBER_ID : "")
            .setMemberId(memberId)
            .setMembers(members.stream().map(JoinMember::data).toList());
    List<String> entries =
        new ArrayList<>(
            List.of(
                kv("throttleTime", num(THROTTLE_TIME_MS)),
                kv("errorCode", num(errorCode)),
                kv("generationId", num(generationId))));
    if (version >= 7) {
      data.setProtocolType(protocolType);
      entries.add(kv("protocolType", str(protocolType)));
    }
    entries.add(kv("groupProtocol", str(protocolName)));
    entries.add(kv("leaderId", str(errorCode == 0 ? MEMBER_ID : "")));
    entries.add(kv("memberId", str(memberId)));
    entries.add(kv("members", list(members, JoinMember::json)));
    return new Vector("joinGroup", "JoinGroupResponse", version, caseId, data,
        obj(entries.toArray(new String[0])));
  }

  /**
   * SyncGroup (14) v4-v5, request and response. v4 is v3's body in flexible framing; v5 adds
   * nullable ProtocolType and ProtocolName to both directions. {@code assignment} and {@code ok}
   * carry them null at v5 (what the client sends); {@code withProtocol} and {@code protocol} set
   * them.
   *
   * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/SyncGroupRequest.json">SyncGroupRequest.json</a>
   * @return the key-14 vectors
   */
  private static List<Vector> syncGroup() {
    List<Vector> vectors = new ArrayList<>();

    for (short version : new short[] {4, 5}) {
      vectors.add(syncRequest(version, "assignment", null, null));
      vectors.add(syncResponse(version, "ok", null, null));
      if (version >= 5) {
        vectors.add(syncRequest(version, "withProtocol", PROTOCOL_TYPE, ASSIGNOR_NAME));
        vectors.add(syncResponse(version, "protocol", PROTOCOL_TYPE, ASSIGNOR_NAME));
      }
    }

    return vectors;
  }

  /**
   * @param version the request version
   * @param caseId the case name
   * @param protocolType the protocol type (v5+), or null
   * @param protocolName the protocol name (v5+), or null
   * @return one SyncGroup request vector
   */
  private static Vector syncRequest(
      short version, String caseId, String protocolType, String protocolName) {
    SyncGroupRequestData data =
        new SyncGroupRequestData()
            .setGroupId(GROUP_ID)
            .setGenerationId(GENERATION_ID)
            .setMemberId(MEMBER_ID)
            .setGroupInstanceId(GROUP_INSTANCE_ID)
            .setAssignments(
                List.of(
                    new SyncGroupRequestData.SyncGroupRequestAssignment()
                        .setMemberId(MEMBER_ID)
                        .setAssignment(MEMBER_BYTES)));
    List<String> entries =
        new ArrayList<>(
            List.of(
                kv("groupId", str(GROUP_ID)),
                kv("generationId", num(GENERATION_ID)),
                kv("memberId", str(MEMBER_ID)),
                kv("groupInstanceId", str(GROUP_INSTANCE_ID))));
    if (version >= 5) {
      data.setProtocolType(protocolType).setProtocolName(protocolName);
      entries.add(kv("protocolType", str(protocolType)));
      entries.add(kv("protocolName", str(protocolName)));
    }
    entries.add(
        kv("groupAssignment",
            array(
                obj(kv("memberId", str(MEMBER_ID)),
                    kv("memberAssignment", str(hex(MEMBER_BYTES)))))));
    return new Vector("syncGroup", "SyncGroupRequest", version, caseId, data,
        obj(entries.toArray(new String[0])));
  }

  /**
   * @param version the response version
   * @param caseId the case name
   * @param protocolType the protocol type (v5+), or null
   * @param protocolName the protocol name (v5+), or null
   * @return one SyncGroup response vector
   */
  private static Vector syncResponse(
      short version, String caseId, String protocolType, String protocolName) {
    SyncGroupResponseData data =
        new SyncGroupResponseData()
            .setThrottleTimeMs(THROTTLE_TIME_MS)
            .setErrorCode((short) 0)
            .setAssignment(MEMBER_BYTES);
    List<String> entries =
        new ArrayList<>(
            List.of(kv("throttleTime", num(THROTTLE_TIME_MS)), kv("errorCode", "0")));
    if (version >= 5) {
      data.setProtocolType(protocolType).setProtocolName(protocolName);
      entries.add(kv("protocolType", str(protocolType)));
      entries.add(kv("protocolName", str(protocolName)));
    }
    entries.add(kv("memberAssignment", str(hex(MEMBER_BYTES))));
    return new Vector("syncGroup", "SyncGroupResponse", version, caseId, data,
        obj(entries.toArray(new String[0])));
  }

  /**
   * Heartbeat (12) v4, request and response: v3's body in flexible framing, from a static member.
   *
   * @return the key-12 vectors
   */
  private static List<Vector> heartbeat() {
    List<Vector> vectors = new ArrayList<>();
    String api = "heartbeat";

    vectors.add(
        new Vector(api, "HeartbeatRequest", (short) 4, "basic",
            new HeartbeatRequestData()
                .setGroupId(GROUP_ID)
                .setGenerationId(GENERATION_ID)
                .setMemberId(MEMBER_ID)
                .setGroupInstanceId(GROUP_INSTANCE_ID),
            obj(
                kv("groupId", str(GROUP_ID)),
                kv("groupGenerationId", num(GENERATION_ID)),
                kv("memberId", str(MEMBER_ID)),
                kv("groupInstanceId", str(GROUP_INSTANCE_ID)))));
    for (short code : new short[] {0, REBALANCE_IN_PROGRESS}) {
      vectors.add(
          new Vector(api, "HeartbeatResponse", (short) 4, code == 0 ? "ok" : "error",
              new HeartbeatResponseData().setThrottleTimeMs(THROTTLE_TIME_MS).setErrorCode(code),
              obj(kv("throttleTime", num(THROTTLE_TIME_MS)), kv("errorCode", num(code)))));
    }

    return vectors;
  }

  /** One LeaveGroup member; the reason is v5. */
  private record Leaver(String memberId, String groupInstanceId, String reason) {
    /**
     * @param version the request version; the reason is written from v5
     * @return the Kafka {@code MemberIdentity} for this member
     */
    LeaveGroupRequestData.MemberIdentity data(short version) {
      LeaveGroupRequestData.MemberIdentity data =
          new LeaveGroupRequestData.MemberIdentity()
              .setMemberId(memberId)
              .setGroupInstanceId(groupInstanceId);
      return version >= 5 ? data.setReason(reason) : data;
    }

    /**
     * @param version the request version; the reason is carried from v5
     * @return this member as the JSON of a fixture's {@code fields}
     */
    String json(short version) {
      return version >= 5
          ? obj(
              kv("memberId", str(memberId)),
              kv("groupInstanceId", str(groupInstanceId)),
              kv("reason", str(reason)))
          : obj(kv("memberId", str(memberId)), kv("groupInstanceId", str(groupInstanceId)));
    }
  }

  /**
   * LeaveGroup (13) v4-v5, request and response (KIP-345, KIP-800). v4 is v3's body in flexible
   * framing; v5 adds a nullable Reason to each member. {@code oneMember} is a dynamic member and
   * {@code twoMembers} adds a static one; at v5 their reasons are null and {@code withReason} sets
   * them.
   *
   * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/LeaveGroupRequest.json">LeaveGroupRequest.json</a>
   * @return the key-13 vectors
   */
  private static List<Vector> leaveGroup() {
    List<Vector> vectors = new ArrayList<>();
    Leaver dynamic = new Leaver(MEMBER_ID, null, null);
    Leaver staticMember = new Leaver("other-member", GROUP_INSTANCE_ID, null);

    for (short version : new short[] {4, 5}) {
      vectors.add(leaveRequest(version, "oneMember", dynamic));
      vectors.add(leaveRequest(version, "twoMembers", dynamic, staticMember));
      if (version >= 5) {
        vectors.add(
            leaveRequest(version, "withReason",
                new Leaver(MEMBER_ID, null, "the consumer is being closed"),
                new Leaver("other-member", GROUP_INSTANCE_ID, "member was removed by an admin")));
      }

      vectors.add(leaveResponse(version, "ok", (short) 0));
      vectors.add(leaveResponse(version, "memberError", UNKNOWN_MEMBER_ID));
    }

    return vectors;
  }

  /**
   * @param version the request version
   * @param caseId the case name
   * @param members the members leaving
   * @return one LeaveGroup request vector
   */
  private static Vector leaveRequest(short version, String caseId, Leaver... members) {
    return new Vector("leaveGroup", "LeaveGroupRequest", version, caseId,
        new LeaveGroupRequestData()
            .setGroupId(GROUP_ID)
            .setMembers(Arrays.stream(members).map(m -> m.data(version)).toList()),
        obj(
            kv("groupId", str(GROUP_ID)),
            kv("members", list(Arrays.asList(members), m -> m.json(version)))));
  }

  /**
   * @param version the response version
   * @param caseId the case name
   * @param memberCode the error code of the second member
   * @return one LeaveGroup response vector
   */
  private static Vector leaveResponse(short version, String caseId, short memberCode) {
    return new Vector("leaveGroup", "LeaveGroupResponse", version, caseId,
        new LeaveGroupResponseData()
            .setThrottleTimeMs(THROTTLE_TIME_MS)
            .setErrorCode((short) 0)
            .setMembers(
                List.of(
                    new LeaveGroupResponseData.MemberResponse()
                        .setMemberId(MEMBER_ID)
                        .setGroupInstanceId(null)
                        .setErrorCode((short) 0),
                    new LeaveGroupResponseData.MemberResponse()
                        .setMemberId("other-member")
                        .setGroupInstanceId(GROUP_INSTANCE_ID)
                        .setErrorCode(memberCode))),
        obj(
            kv("throttleTime", num(THROTTLE_TIME_MS)),
            kv("errorCode", "0"),
            kv("members",
                array(
                    obj(kv("memberId", str(MEMBER_ID)), kv("groupInstanceId", "null"),
                        kv("errorCode", "0")),
                    obj(kv("memberId", str("other-member")),
                        kv("groupInstanceId", str(GROUP_INSTANCE_ID)),
                        kv("errorCode", num(memberCode)))))));
  }

  /**
   * OffsetCommit (8) v10, request and response (KIP-848): {@code TopicId} replaces {@code Name}.
   * The values are {@code offsetCommit()}'s v9 ones with the topic named by id, which is what a
   * consumer-protocol member sends. {@code unknownTopicId} answers UNKNOWN_TOPIC_ID (100).
   *
   * @return the key-8 v10 vectors
   */
  private static List<Vector> offsetCommitByTopicId() {
    List<Vector> vectors = new ArrayList<>();
    String api = "offsetCommit";

    OffsetCommitRequestData.OffsetCommitRequestPartition committed =
        new OffsetCommitRequestData.OffsetCommitRequestPartition()
            .setPartitionIndex(PARTITION_INDEX)
            .setCommittedOffset(COMMITTED_OFFSET)
            .setCommittedLeaderEpoch(-1)
            .setCommittedMetadata(null);
    String partition =
        obj(
            kv("partition", num(PARTITION_INDEX)),
            kv("offset", str(String.valueOf(COMMITTED_OFFSET))),
            kv("leaderEpoch", "-1"),
            kv("metadata", "null"));
    vectors.add(
        new Vector(api, "OffsetCommitRequest", (short) 10, "byTopicId",
            new OffsetCommitRequestData()
                .setGroupId(GROUP_ID)
                .setGenerationIdOrMemberEpoch(GENERATION_ID)
                .setMemberId(MEMBER_ID)
                .setGroupInstanceId(GROUP_INSTANCE_ID)
                .setTopics(
                    List.of(
                        new OffsetCommitRequestData.OffsetCommitRequestTopic()
                            .setTopicId(topicId())
                            .setPartitions(List.of(committed)))),
            obj(
                kv("groupId", str(GROUP_ID)),
                kv("groupGenerationId", num(GENERATION_ID)),
                kv("memberId", str(MEMBER_ID)),
                kv("groupInstanceId", str(GROUP_INSTANCE_ID)),
                kv("topics",
                    array(
                        obj(kv("topicId", str(TOPIC_ID)), kv("partitions", array(partition))))))));

    for (short code : new short[] {0, UNKNOWN_TOPIC_ID}) {
      OffsetCommitResponseData.OffsetCommitResponsePartition answered =
          new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(PARTITION_INDEX)
              .setErrorCode(code);
      String partitionJson =
          obj(kv("partition", num(PARTITION_INDEX)), kv("errorCode", num(code)));
      vectors.add(
          new Vector(api, "OffsetCommitResponse", (short) 10, code == 0 ? "ok" : "unknownTopicId",
              new OffsetCommitResponseData()
                  .setThrottleTimeMs(THROTTLE_TIME_MS)
                  .setTopics(
                      List.of(
                          new OffsetCommitResponseData.OffsetCommitResponseTopic()
                              .setTopicId(topicId())
                              .setPartitions(List.of(answered)))),
              obj(
                  kv("throttleTime", num(THROTTLE_TIME_MS)),
                  kv("topics",
                      array(
                          obj(kv("topicId", str(TOPIC_ID)),
                              kv("partitions", array(partitionJson))))))));
    }

    return vectors;
  }

  /**
   * OffsetFetch (9) v10, request and response (KIP-848): the {@code Groups[]} form of v9 with
   * {@code TopicId} in place of {@code Name}. {@code byTopicId} asks for one partition of one topic
   * as a member; {@code allTopics} is the null topic list of an admin client.
   * {@code unknownTopicId} answers UNKNOWN_TOPIC_ID (100) for the partition.
   *
   * @return the key-9 v10 vectors
   */
  private static List<Vector> offsetFetchByTopicId() {
    List<Vector> vectors = new ArrayList<>();
    String api = "offsetFetch";

    OffsetFetchRequestData.OffsetFetchRequestGroup member =
        new OffsetFetchRequestData.OffsetFetchRequestGroup()
            .setGroupId(GROUP_ID)
            .setMemberId(MEMBER_ID)
            .setMemberEpoch(MEMBER_EPOCH)
            .setTopics(
                List.of(
                    new OffsetFetchRequestData.OffsetFetchRequestTopics()
                        .setTopicId(topicId())
                        .setPartitionIndexes(List.of(PARTITION_INDEX))));
    vectors.add(
        new Vector(api, "OffsetFetchRequest", (short) 10, "byTopicId",
            new OffsetFetchRequestData().setGroups(List.of(member)).setRequireStable(false),
            obj(
                kv("groupId", str(GROUP_ID)),
                kv("memberId", str(MEMBER_ID)),
                kv("memberEpoch", num(MEMBER_EPOCH)),
                kv("topics",
                    array(
                        obj(kv("topicId", str(TOPIC_ID)),
                            kv("partitions", array(obj(kv("partition", num(PARTITION_INDEX)))))))),
                kv("requireStable", "false"))));

    OffsetFetchRequestData.OffsetFetchRequestGroup anyTopic =
        new OffsetFetchRequestData.OffsetFetchRequestGroup()
            .setGroupId(GROUP_ID)
            .setMemberId(null)
            .setMemberEpoch(-1)
            .setTopics(null);
    vectors.add(
        new Vector(api, "OffsetFetchRequest", (short) 10, "allTopics",
            new OffsetFetchRequestData().setGroups(List.of(anyTopic)).setRequireStable(false),
            obj(
                kv("groupId", str(GROUP_ID)),
                kv("memberId", "null"),
                kv("memberEpoch", "-1"),
                kv("topics", "null"),
                kv("requireStable", "false"))));

    for (short code : new short[] {0, UNKNOWN_TOPIC_ID}) {
      long offset = code == 0 ? COMMITTED_OFFSET : -1L;
      int epoch = code == 0 ? COMMITTED_LEADER_EPOCH : -1;
      OffsetFetchResponseData.OffsetFetchResponsePartitions answered =
          new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(PARTITION_INDEX)
              .setCommittedOffset(offset)
              .setCommittedLeaderEpoch(epoch)
              .setMetadata(null)
              .setErrorCode(code);
      OffsetFetchResponseData.OffsetFetchResponseGroup group =
          new OffsetFetchResponseData.OffsetFetchResponseGroup()
              .setGroupId(GROUP_ID)
              .setErrorCode((short) 0)
              .setTopics(
                  List.of(
                      new OffsetFetchResponseData.OffsetFetchResponseTopics()
                          .setTopicId(topicId())
                          .setPartitions(List.of(answered))));
      String partition =
          obj(
              kv("partition", num(PARTITION_INDEX)),
              kv("offset", str(String.valueOf(offset))),
              kv("leaderEpoch", num(epoch)),
              kv("metadata", "null"),
              kv("errorCode", num(code)));
      vectors.add(
          new Vector(api, "OffsetFetchResponse", (short) 10, code == 0 ? "ok" : "unknownTopicId",
              new OffsetFetchResponseData()
                  .setThrottleTimeMs(THROTTLE_TIME_MS)
                  .setGroups(List.of(group)),
              obj(
                  kv("throttleTime", num(THROTTLE_TIME_MS)),
                  kv("groupId", str(GROUP_ID)),
                  kv("errorCode", "0"),
                  kv("topics",
                      array(
                          obj(kv("topicId", str(TOPIC_ID)),
                              kv("partitions", array(partition))))))));
    }

    return vectors;
  }

  /**
   * Session requests and responses from independent, pinned inputs. Versions 12-18 cover the
   * name/id boundary and the replica-id removal without advertising a new scripted wire pair.
   * @return full, incremental, forgotten and close requests, and full, sparse and error responses
   */
  private static List<Vector> fetchSessions() {
    List<Vector> vectors = new ArrayList<>();
    for (short version = 12; version <= 18; version++) {
      for (String name : List.of("sessionFull", "sessionIncremental", "sessionForgotten", "sessionClose")) {
        boolean full = name.equals("sessionFull");
        boolean forgotten = name.equals("sessionForgotten");
        boolean close = name.equals("sessionClose");
        int id = full ? 0 : 37;
        int epoch = full ? 0 : forgotten ? 2 : close ? -1 : 1;
        boolean topicPresent = full || name.equals("sessionIncremental");
        long offset = full ? 42 : 43;
        FetchRequestData request = new FetchRequestData()
            .setMaxWaitMs(5000).setMinBytes(1).setMaxBytes(10485760)
            .setIsolationLevel((byte) 1).setSessionId(id).setSessionEpoch(epoch).setRackId("rack-a");
        if (version <= 14) request.setReplicaId(-1);
        String topicFields = version == 12 ? kv("topic", str(TOPIC_NAME)) : kv("topicId", str(TOPIC_ID));
        String partitionFields = obj(kv("partition", "0"), kv("currentLeaderEpoch", "5"),
            kv("fetchOffset", str(Long.toString(offset))), kv("lastFetchedEpoch", "-1"),
            kv("logStartOffset", str("-1")), kv("maxBytes", "32768"));
        if (topicPresent) {
          FetchRequestData.FetchTopic topic = new FetchRequestData.FetchTopic().setPartitions(List.of(
              new FetchRequestData.FetchPartition().setPartition(0).setCurrentLeaderEpoch(5)
                  .setFetchOffset(offset).setLastFetchedEpoch(-1).setLogStartOffset(-1).setPartitionMaxBytes(32768)));
          if (version == 12) topic.setTopic(TOPIC_NAME); else topic.setTopicId(topicId());
          request.setTopics(List.of(topic));
        }
        if (forgotten) {
          FetchRequestData.ForgottenTopic topic = new FetchRequestData.ForgottenTopic().setPartitions(List.of(0));
          if (version == 12) topic.setTopic(TOPIC_NAME); else topic.setTopicId(topicId());
          request.setForgottenTopicsData(List.of(topic));
        }
        String topics = topicPresent ? array(obj(topicFields, kv("partitions", array(partitionFields)))) : array();
        String forgottenTopics = forgotten ? array(obj(topicFields, kv("partitions", array("0")))) : array();
        vectors.add(new Vector("fetch", "FetchRequest", version, name, request,
            obj(kv("replicaId", version <= 14 ? "-1" : "null"), kv("maxWaitMs", "5000"),
                kv("minBytes", "1"), kv("maxBytes", "10485760"), kv("isolationLevel", "1"),
                kv("sessionId", Integer.toString(id)), kv("sessionEpoch", Integer.toString(epoch)),
                kv("rackId", str("rack-a")), kv("topics", topics), kv("forgottenTopics", forgottenTopics))));
      }
      for (String name : List.of("sessionFull", "sessionSparse", "sessionError70", "sessionError71", "sessionError106")) {
        boolean full = name.equals("sessionFull");
        short code = name.startsWith("sessionError") ? Short.parseShort(name.substring(12)) : 0;
        int id = code == 0 ? 37 : 0;
        FetchResponseData response = new FetchResponseData().setThrottleTimeMs(0).setErrorCode(code).setSessionId(id);
        if (full) {
          FetchResponseData.FetchableTopicResponse topic = new FetchResponseData.FetchableTopicResponse()
              .setPartitions(List.of(new FetchResponseData.PartitionData().setPartitionIndex(0)
                  .setErrorCode((short) 0).setHighWatermark(100).setLastStableOffset(100)
                  .setLogStartOffset(0).setPreferredReadReplica(-1)));
          if (version == 12) topic.setTopic(TOPIC_NAME); else topic.setTopicId(topicId());
          response.setResponses(List.of(topic));
        }
        vectors.add(new Vector("fetch", "FetchResponse", version, name, response,
            obj(kv("errorCode", Short.toString(code)), kv("sessionId", Integer.toString(id)),
                kv("includeTopic", Boolean.toString(full)), kv("partition", "0"),
                kv("highWatermark", str("100")), kv("lastStableOffset", str("100")),
                kv("logStartOffset", str("0")), kv("preferredReadReplica", "-1"))));
      }
    }
    return vectors;
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
