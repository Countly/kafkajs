import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.UUID;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

/**
 * Generates golden protocol byte vectors by running Apache Kafka's own generated message codecs
 * ({@code org.apache.kafka.common.message.*Data}) against the values used by this repo's hand
 * fixtures, so both sources can be compared byte-for-byte.
 *
 * <p>Development-only: run via {@code scripts/protocol/javaVectors/run.sh}. Never invoked from
 * {@code npm test} or any committed build step — the {@code kafka-clients} jar is not a project
 * dependency, only a tool used once to produce fixtures that are then committed on their own.
 *
 * <p>Every value written here is a constant of this class, so a JavaScript spec can reproduce the
 * same inputs through this repo's own encoder and compare the two buffers byte-for-byte. The one
 * exception is {@code Produce}: its request carries a record batch whose timestamp and CRC cannot
 * be reproduced across two independent encoders, so the request vectors are emitted with a null
 * {@code Records} field and the specs decode them instead of comparing bytes.
 *
 * @see <a href="https://kafka.apache.org/43/design/protocol">Kafka protocol guide</a>
 * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/ApiVersionsRequest.json">ApiVersionsRequest.json</a>
 * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/MetadataRequest.json">MetadataRequest.json</a>
 * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/FetchRequest.json">FetchRequest.json</a>
 */
public final class GenerateVectors {

  private static final String CLIENT_SOFTWARE_NAME = "countly-kafkajs";
  private static final String CLIENT_SOFTWARE_VERSION = "3.0.0-alpha.4";
  private static final String TOPIC_NAME = "test-topic";
  private static final String RESPONSE_TOPIC_ID = "01020304-0506-0708-090a-0b0c0d0e0f10";

  private static final String GROUP_ID = "test-group";
  private static final String MEMBER_ID = "test-member";
  private static final String GROUP_INSTANCE_ID = "test-instance";
  private static final String TRANSACTIONAL_ID = "test-transactional-id";
  private static final String BROKER_HOST = "127.0.0.1";
  private static final int BROKER_PORT = 9092;

  private static final int PARTITION_INDEX = 0;
  private static final int GENERATION_ID = 5;
  private static final int CURRENT_LEADER_EPOCH = 5;
  private static final int COMMITTED_LEADER_EPOCH = 3;
  private static final long COMMITTED_OFFSET = 10L;
  private static final long EARLIEST_TIMESTAMP = -2L;
  private static final long BASE_OFFSET = 100L;
  private static final int TRANSACTION_TIMEOUT_MS = 30000;
  private static final int PRODUCE_TIMEOUT_MS = 30000;
  private static final short PRODUCE_ACKS = -1;
  private static final long PRODUCER_ID = 1000L;
  private static final short PRODUCER_EPOCH = 0;
  private static final int LEADER_ID = 2;
  private static final int LEADER_EPOCH = 7;

  /** Consumers identify themselves with -1 in both Fetch and OffsetForLeaderEpoch. */
  private static final int CONSUMER_REPLICA_ID = -1;

  private static final int FETCH_MAX_WAIT_MS = 5000;
  private static final int FETCH_MIN_BYTES = 1;
  private static final int FETCH_MAX_BYTES = 10485760;
  private static final int PARTITION_MAX_BYTES = 1048576;
  private static final long FETCH_OFFSET = 42L;
  private static final long LOG_START_OFFSET = -1L;
  private static final int LAST_FETCHED_EPOCH = -1;
  private static final byte READ_COMMITTED = 1;
  private static final long HIGH_WATERMARK = 100L;

  private static final int VALIDATED_LEADER_EPOCH = 4;
  private static final long EPOCH_END_OFFSET = 120L;

  private static final String SASL_AUTH_BYTES = "server-final-message";
  private static final long SASL_SESSION_LIFETIME_MS = 3600000L;

  private GenerateVectors() {}

  /**
   * @param args {@code args[0]} is the repository root the fixtures are written under, matching
   *     the layout of {@code src/protocol/requests/<api>/fixtures/}.
   * @throws IOException if a fixture file cannot be written
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("usage: GenerateVectors <output-dir>");
      System.exit(1);
    }

    Path outputDir = Paths.get(args[0]);

    writeApiVersionsVectors(fixturesDir(outputDir, "apiVersions"));
    writeMetadataVectors(fixturesDir(outputDir, "metadata"));
    writeProduceVectors(fixturesDir(outputDir, "produce"));
    writeFindCoordinatorVectors(fixturesDir(outputDir, "findCoordinator"));
    writeOffsetCommitVectors(fixturesDir(outputDir, "offsetCommit"));
    writeOffsetFetchVectors(fixturesDir(outputDir, "offsetFetch"));
    writeListOffsetsVectors(fixturesDir(outputDir, "listOffsets"));
    writeInitProducerIdVectors(fixturesDir(outputDir, "initProducerId"));
    writeFetchVectors(fixturesDir(outputDir, "fetch"));
    writeOffsetForLeaderEpochVectors(fixturesDir(outputDir, "offsetForLeaderEpoch"));
    writeSaslAuthenticateVectors(fixturesDir(outputDir, "saslAuthenticate"));
  }

  /**
   * Resolves and creates one API's fixtures directory under the repository root.
   *
   * @param outputDir repository root
   * @param api the API directory name under {@code src/protocol/requests}
   * @return the {@code fixtures} directory for that API
   * @throws IOException if the directory cannot be created
   */
  private static Path fixturesDir(Path outputDir, String api) throws IOException {
    Path dir = outputDir.resolve("src/protocol/requests/" + api + "/fixtures");
    Files.createDirectories(dir);
    return dir;
  }

  /**
   * Emits the ApiVersions v3/v4 request and response vectors. v3 and v4 share one wire format
   * (KAFKA-17011), so both versions are generated from the same data.
   *
   * @param dir target fixtures directory
   * @throws IOException if a fixture file cannot be written
   */
  private static void writeApiVersionsVectors(Path dir) throws IOException {
    for (short version : new short[] {3, 4}) {
      writeVector(dir, "v" + version + "_request_java.json", buildApiVersionsRequest(), version);
      writeVector(dir, "v" + version + "_response_java.json", buildApiVersionsResponse(), version);
    }
  }

  /**
   * Emits the Metadata v9-v13 request and response vectors, one topic requested/returned by name.
   *
   * @param dir target fixtures directory
   * @throws IOException if a fixture file cannot be written
   */
  private static void writeMetadataVectors(Path dir) throws IOException {
    for (short version : new short[] {9, 10, 11, 12, 13}) {
      writeVector(dir, "v" + version + "_request_java.json", buildMetadataRequest(version), version);
      writeVector(dir, "v" + version + "_response_java.json", buildMetadataResponse(version), version);
    }
  }

  /**
   * Emits the Produce v8-v13 request and response vectors: one topic, one partition. From v13 the
   * topic is keyed by id instead of by name (KIP-516).
   *
   * @param dir target fixtures directory
   * @throws IOException if a fixture file cannot be written
   */
  private static void writeProduceVectors(Path dir) throws IOException {
    for (short version : new short[] {8, 9, 10, 11, 12, 13}) {
      writeVector(dir, "v" + version + "_request_java.json", buildProduceRequest(version), version);
      writeVector(dir, "v" + version + "_response_java.json", buildProduceResponse(version), version);
    }
  }

  /**
   * Emits the FindCoordinator v3-v6 request and response vectors, one group-coordinator lookup.
   *
   * @param dir target fixtures directory
   * @throws IOException if a fixture file cannot be written
   */
  private static void writeFindCoordinatorVectors(Path dir) throws IOException {
    for (short version : new short[] {3, 4, 5, 6}) {
      writeVector(
          dir, "v" + version + "_request_java.json", buildFindCoordinatorRequest(version), version);
      writeVector(
          dir,
          "v" + version + "_response_java.json",
          buildFindCoordinatorResponse(version),
          version);
    }
  }

  /**
   * Emits the OffsetCommit v6-v8 request and response vectors, one topic and one partition.
   *
   * @param dir target fixtures directory
   * @throws IOException if a fixture file cannot be written
   */
  private static void writeOffsetCommitVectors(Path dir) throws IOException {
    for (short version : new short[] {6, 7, 8}) {
      writeVector(
          dir, "v" + version + "_request_java.json", buildOffsetCommitRequest(version), version);
      writeVector(dir, "v" + version + "_response_java.json", buildOffsetCommitResponse(), version);
    }
  }

  /**
   * Emits the OffsetFetch v5-v8 request and response vectors, one group with one topic partition.
   *
   * @param dir target fixtures directory
   * @throws IOException if a fixture file cannot be written
   */
  private static void writeOffsetFetchVectors(Path dir) throws IOException {
    for (short version : new short[] {5, 6, 7, 8}) {
      writeVector(
          dir, "v" + version + "_request_java.json", buildOffsetFetchRequest(version), version);
      writeVector(
          dir, "v" + version + "_response_java.json", buildOffsetFetchResponse(version), version);
    }
  }

  /**
   * Emits the ListOffsets v4-v9 request and response vectors, one topic and one partition.
   *
   * @param dir target fixtures directory
   * @throws IOException if a fixture file cannot be written
   */
  private static void writeListOffsetsVectors(Path dir) throws IOException {
    for (short version : new short[] {4, 5, 6, 7, 8, 9}) {
      writeVector(dir, "v" + version + "_request_java.json", buildListOffsetsRequest(), version);
      writeVector(dir, "v" + version + "_response_java.json", buildListOffsetsResponse(), version);
    }
  }

  /**
   * Emits the InitProducerId v2-v5 request and response vectors for one transactional id.
   *
   * @param dir target fixtures directory
   * @throws IOException if a fixture file cannot be written
   */
  private static void writeInitProducerIdVectors(Path dir) throws IOException {
    for (short version : new short[] {2, 3, 4, 5}) {
      writeVector(
          dir, "v" + version + "_request_java.json", buildInitProducerIdRequest(version), version);
      writeVector(dir, "v" + version + "_response_java.json", buildInitProducerIdResponse(), version);
    }
  }

  private static ApiVersionsRequestData buildApiVersionsRequest() {
    return new ApiVersionsRequestData()
        .setClientSoftwareName(CLIENT_SOFTWARE_NAME)
        .setClientSoftwareVersion(CLIENT_SOFTWARE_VERSION);
  }

  /**
   * One broker api key (0,3,13), one controller api key (18,0,4), one supported feature and one
   * finalized feature named {@code metadata.version} — the same values the hand-built v3/v4
   * response fixtures use.
   *
   * @return a populated {@code ApiVersionsResponseData}
   */
  private static ApiVersionsResponseData buildApiVersionsResponse() {
    ApiVersionsResponseData.ApiVersionCollection apiKeys =
        new ApiVersionsResponseData.ApiVersionCollection();
    apiKeys.add(
        new ApiVersionsResponseData.ApiVersion()
            .setApiKey((short) 0)
            .setMinVersion((short) 3)
            .setMaxVersion((short) 13));
    apiKeys.add(
        new ApiVersionsResponseData.ApiVersion()
            .setApiKey((short) 18)
            .setMinVersion((short) 0)
            .setMaxVersion((short) 4));

    ApiVersionsResponseData.SupportedFeatureKeyCollection supportedFeatures =
        new ApiVersionsResponseData.SupportedFeatureKeyCollection();
    supportedFeatures.add(
        new ApiVersionsResponseData.SupportedFeatureKey()
            .setName("metadata.version")
            .setMinVersion((short) 1)
            .setMaxVersion((short) 25));

    ApiVersionsResponseData.FinalizedFeatureKeyCollection finalizedFeatures =
        new ApiVersionsResponseData.FinalizedFeatureKeyCollection();
    finalizedFeatures.add(
        new ApiVersionsResponseData.FinalizedFeatureKey()
            .setName("metadata.version")
            .setMaxVersionLevel((short) 25)
            .setMinVersionLevel((short) 1));

    return new ApiVersionsResponseData()
        .setErrorCode((short) 0)
        .setApiKeys(apiKeys)
        .setThrottleTimeMs(0)
        .setSupportedFeatures(supportedFeatures)
        .setFinalizedFeaturesEpoch(7L)
        .setFinalizedFeatures(finalizedFeatures)
        .setZkMigrationReady(false);
  }

  /**
   * One topic requested by name. {@code TopicId} is sent as the zero uuid from v10 (this client
   * never requests by id) and {@code IncludeClusterAuthorizedOperations} only exists through v10
   * (KIP-700 moved cluster-authorized-ops to DescribeCluster from v11).
   *
   * @param version the Metadata request version, 9-13
   * @return a populated {@code MetadataRequestData}
   */
  private static MetadataRequestData buildMetadataRequest(short version) {
    MetadataRequestData.MetadataRequestTopic topic =
        new MetadataRequestData.MetadataRequestTopic().setName(TOPIC_NAME);
    if (version >= 10) {
      topic.setTopicId(Uuid.ZERO_UUID);
    }

    MetadataRequestData request =
        new MetadataRequestData()
            .setTopics(Collections.singletonList(topic))
            .setAllowAutoTopicCreation(true)
            .setIncludeTopicAuthorizedOperations(false);

    if (version <= 10) {
      request.setIncludeClusterAuthorizedOperations(false);
    }

    return request;
  }

  /**
   * One broker, one topic with one partition, matching the values the hand-built v9-v12 response
   * fixtures use. {@code TopicId} is populated from v10, {@code ClusterAuthorizedOperations} only
   * through v10, and the top-level {@code ErrorCode} (KIP-1102's REBOOTSTRAP_REQUIRED transport)
   * only from v13, where it is written as "no error".
   *
   * @param version the Metadata response version, 9-13
   * @return a populated {@code MetadataResponseData}
   */
  private static MetadataResponseData buildMetadataResponse(short version) {
    MetadataResponseData.MetadataResponseBrokerCollection brokers =
        new MetadataResponseData.MetadataResponseBrokerCollection();
    brokers.add(
        new MetadataResponseData.MetadataResponseBroker()
            .setNodeId(0)
            .setHost(BROKER_HOST)
            .setPort(BROKER_PORT)
            .setRack(null));

    MetadataResponseData.MetadataResponsePartition partition =
        new MetadataResponseData.MetadataResponsePartition()
            .setErrorCode((short) 0)
            .setPartitionIndex(0)
            .setLeaderId(1)
            .setLeaderEpoch(5)
            .setReplicaNodes(Collections.singletonList(1))
            .setIsrNodes(Collections.singletonList(1))
            .setOfflineReplicas(Collections.emptyList());

    MetadataResponseData.MetadataResponseTopic topic =
        new MetadataResponseData.MetadataResponseTopic()
            .setErrorCode((short) 0)
            .setName(TOPIC_NAME)
            .setIsInternal(false)
            .setPartitions(Collections.singletonList(partition))
            .setTopicAuthorizedOperations(0x80000000);
    if (version >= 10) {
      topic.setTopicId(responseTopicId());
    }

    MetadataResponseData.MetadataResponseTopicCollection topics =
        new MetadataResponseData.MetadataResponseTopicCollection();
    topics.add(topic);

    MetadataResponseData response =
        new MetadataResponseData()
            .setThrottleTimeMs(0)
            .setBrokers(brokers)
            .setClusterId("test-cluster-id")
            .setControllerId(0)
            .setTopics(topics);

    if (version <= 10) {
      response.setClusterAuthorizedOperations(0x80000000);
    }
    if (version >= 13) {
      response.setErrorCode((short) 0);
    }

    return response;
  }

  /**
   * One topic, one partition, and a null {@code Records} field. A record batch carries a wall-clock
   * timestamp and a CRC over it, so no second encoder can reproduce one byte-for-byte; the JS specs
   * therefore decode this vector's framing rather than comparing it against an encoded request.
   * The topic travels as {@code Name} through v12 and as {@code TopicId} from v13 (KIP-516).
   *
   * @param version the Produce request version, 8-13
   * @return a populated {@code ProduceRequestData}
   */
  private static ProduceRequestData buildProduceRequest(short version) {
    ProduceRequestData.TopicProduceData topic =
        new ProduceRequestData.TopicProduceData()
            .setPartitionData(
                Collections.singletonList(
                    new ProduceRequestData.PartitionProduceData()
                        .setIndex(PARTITION_INDEX)
                        .setRecords(null)));
    if (version <= 12) {
      topic.setName(TOPIC_NAME);
    } else {
      topic.setTopicId(responseTopicId());
    }

    ProduceRequestData.TopicProduceDataCollection topicData =
        new ProduceRequestData.TopicProduceDataCollection();
    topicData.add(topic);

    return new ProduceRequestData()
        .setTransactionalId(null)
        .setAcks(PRODUCE_ACKS)
        .setTimeoutMs(PRODUCE_TIMEOUT_MS)
        .setTopicData(topicData);
  }

  /**
   * One topic, one partition at {@code BaseOffset} 100. From v10 the KIP-951 tagged fields are
   * populated too — a {@code CurrentLeader} on the partition and a matching {@code NodeEndpoints}
   * entry at the top level — so the decoder's tagged-field handling is exercised, not just skipped.
   *
   * The topic travels as {@code Name} through v12 and as {@code TopicId} from v13 (KIP-516).
   *
   * @param version the Produce response version, 8-13
   * @return a populated {@code ProduceResponseData}
   */
  private static ProduceResponseData buildProduceResponse(short version) {
    ProduceResponseData.PartitionProduceResponse partition =
        new ProduceResponseData.PartitionProduceResponse()
            .setIndex(PARTITION_INDEX)
            .setErrorCode((short) 0)
            .setBaseOffset(BASE_OFFSET)
            .setLogAppendTimeMs(-1L)
            .setLogStartOffset(0L)
            .setRecordErrors(Collections.emptyList())
            .setErrorMessage(null);

    if (version >= 10) {
      partition.setCurrentLeader(
          new ProduceResponseData.LeaderIdAndEpoch()
              .setLeaderId(LEADER_ID)
              .setLeaderEpoch(LEADER_EPOCH));
    }

    ProduceResponseData.TopicProduceResponse topic =
        new ProduceResponseData.TopicProduceResponse()
            .setPartitionResponses(Collections.singletonList(partition));
    if (version <= 12) {
      topic.setName(TOPIC_NAME);
    } else {
      topic.setTopicId(responseTopicId());
    }

    ProduceResponseData.TopicProduceResponseCollection responses =
        new ProduceResponseData.TopicProduceResponseCollection();
    responses.add(topic);

    ProduceResponseData response =
        new ProduceResponseData().setResponses(responses).setThrottleTimeMs(0);

    if (version >= 10) {
      ProduceResponseData.NodeEndpointCollection endpoints =
          new ProduceResponseData.NodeEndpointCollection();
      endpoints.add(
          new ProduceResponseData.NodeEndpoint()
              .setNodeId(LEADER_ID)
              .setHost(BROKER_HOST)
              .setPort(BROKER_PORT)
              .setRack(null));
      response.setNodeEndpoints(endpoints);
    }

    return response;
  }

  /**
   * One group-coordinator lookup. Through v3 the key travels in the single {@code Key} field; from
   * v4 it is one element of the {@code CoordinatorKeys} batch (KIP-699).
   *
   * @param version the FindCoordinator request version, 3-6
   * @return a populated {@code FindCoordinatorRequestData}
   */
  private static FindCoordinatorRequestData buildFindCoordinatorRequest(short version) {
    FindCoordinatorRequestData request = new FindCoordinatorRequestData().setKeyType((byte) 0);

    if (version <= 3) {
      request.setKey(GROUP_ID);
    } else {
      request.setCoordinatorKeys(Collections.singletonList(GROUP_ID));
    }

    return request;
  }

  /**
   * One coordinator. Through v3 it is the flat {@code NodeId}/{@code Host}/{@code Port} triple;
   * from v4 it is a single entry of the {@code Coordinators} list (KIP-699).
   *
   * @param version the FindCoordinator response version, 3-6
   * @return a populated {@code FindCoordinatorResponseData}
   */
  private static FindCoordinatorResponseData buildFindCoordinatorResponse(short version) {
    if (version <= 3) {
      return new FindCoordinatorResponseData()
          .setThrottleTimeMs(0)
          .setErrorCode((short) 0)
          .setErrorMessage(null)
          .setNodeId(1)
          .setHost(BROKER_HOST)
          .setPort(BROKER_PORT);
    }

    return new FindCoordinatorResponseData()
        .setThrottleTimeMs(0)
        .setCoordinators(
            Collections.singletonList(
                new FindCoordinatorResponseData.Coordinator()
                    .setKey(GROUP_ID)
                    .setNodeId(1)
                    .setHost(BROKER_HOST)
                    .setPort(BROKER_PORT)
                    .setErrorCode((short) 0)
                    .setErrorMessage(null)));
  }

  /**
   * One topic, one partition, offset 10 with the committed leader epoch left at its own default
   * (-1) — the value this client always sends. {@code GroupInstanceId} exists from v7 (KIP-345).
   *
   * @param version the OffsetCommit request version, 6-8
   * @return a populated {@code OffsetCommitRequestData}
   */
  private static OffsetCommitRequestData buildOffsetCommitRequest(short version) {
    OffsetCommitRequestData.OffsetCommitRequestTopic topic =
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
            .setName(TOPIC_NAME)
            .setPartitions(
                Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestPartition()
                        .setPartitionIndex(PARTITION_INDEX)
                        .setCommittedOffset(COMMITTED_OFFSET)
                        .setCommittedLeaderEpoch(-1)
                        .setCommittedMetadata(null)));

    OffsetCommitRequestData request =
        new OffsetCommitRequestData()
            .setGroupId(GROUP_ID)
            .setGenerationIdOrMemberEpoch(GENERATION_ID)
            .setMemberId(MEMBER_ID)
            .setTopics(Collections.singletonList(topic));

    if (version >= 7) {
      request.setGroupInstanceId(GROUP_INSTANCE_ID);
    }

    return request;
  }

  /**
   * One topic, one partition, no error. v6-v8 share this shape; only the framing changes at v8.
   *
   * @return a populated {@code OffsetCommitResponseData}
   */
  private static OffsetCommitResponseData buildOffsetCommitResponse() {
    return new OffsetCommitResponseData()
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
  }

  /**
   * One group, one topic, one partition. Through v7 the group travels in the flat
   * {@code GroupId}/{@code Topics} pair; from v8 it is one element of {@code Groups} (KIP-709).
   *
   * @param version the OffsetFetch request version, 5-8
   * @return a populated {@code OffsetFetchRequestData}
   */
  private static OffsetFetchRequestData buildOffsetFetchRequest(short version) {
    if (version <= 7) {
      return new OffsetFetchRequestData()
          .setGroupId(GROUP_ID)
          .setTopics(
              Collections.singletonList(
                  new OffsetFetchRequestData.OffsetFetchRequestTopic()
                      .setName(TOPIC_NAME)
                      .setPartitionIndexes(Collections.singletonList(PARTITION_INDEX))));
    }

    return new OffsetFetchRequestData()
        .setGroups(
            Collections.singletonList(
                new OffsetFetchRequestData.OffsetFetchRequestGroup()
                    .setGroupId(GROUP_ID)
                    .setTopics(
                        Collections.singletonList(
                            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                                .setName(TOPIC_NAME)
                                .setPartitionIndexes(
                                    Collections.singletonList(PARTITION_INDEX))))));
  }

  /**
   * One group, one topic, one partition at offset 10 with committed leader epoch 3. Through v7 the
   * result is the flat {@code Topics}/{@code ErrorCode} pair; from v8 it is wrapped in
   * {@code Groups} (KIP-709).
   *
   * @param version the OffsetFetch response version, 5-8
   * @return a populated {@code OffsetFetchResponseData}
   */
  private static OffsetFetchResponseData buildOffsetFetchResponse(short version) {
    if (version <= 7) {
      return new OffsetFetchResponseData()
          .setThrottleTimeMs(0)
          .setTopics(
              Collections.singletonList(
                  new OffsetFetchResponseData.OffsetFetchResponseTopic()
                      .setName(TOPIC_NAME)
                      .setPartitions(
                          Collections.singletonList(
                              new OffsetFetchResponseData.OffsetFetchResponsePartition()
                                  .setPartitionIndex(PARTITION_INDEX)
                                  .setCommittedOffset(COMMITTED_OFFSET)
                                  .setCommittedLeaderEpoch(COMMITTED_LEADER_EPOCH)
                                  .setMetadata(null)
                                  .setErrorCode((short) 0)))))
          .setErrorCode((short) 0);
    }

    return new OffsetFetchResponseData()
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
                                        new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                            .setPartitionIndex(PARTITION_INDEX)
                                            .setCommittedOffset(COMMITTED_OFFSET)
                                            .setCommittedLeaderEpoch(COMMITTED_LEADER_EPOCH)
                                            .setMetadata(null)
                                            .setErrorCode((short) 0)))))));
  }

  /**
   * One topic, one partition asking for the earliest offset (-2) at current leader epoch 5, from a
   * consumer ({@code ReplicaId} -1) reading uncommitted data ({@code IsolationLevel} 0).
   *
   * @return a populated {@code ListOffsetsRequestData}
   */
  private static ListOffsetsRequestData buildListOffsetsRequest() {
    return new ListOffsetsRequestData()
        .setReplicaId(-1)
        .setIsolationLevel((byte) 0)
        .setTopics(
            Collections.singletonList(
                new ListOffsetsRequestData.ListOffsetsTopic()
                    .setName(TOPIC_NAME)
                    .setPartitions(
                        Collections.singletonList(
                            new ListOffsetsRequestData.ListOffsetsPartition()
                                .setPartitionIndex(PARTITION_INDEX)
                                .setCurrentLeaderEpoch(CURRENT_LEADER_EPOCH)
                                .setTimestamp(EARLIEST_TIMESTAMP)))));
  }

  /**
   * One topic, one partition answering offset 100 at leader epoch 5. v4-v9 share this shape; only
   * the framing changes at v6.
   *
   * @return a populated {@code ListOffsetsResponseData}
   */
  private static ListOffsetsResponseData buildListOffsetsResponse() {
    return new ListOffsetsResponseData()
        .setThrottleTimeMs(0)
        .setTopics(
            Collections.singletonList(
                new ListOffsetsResponseData.ListOffsetsTopicResponse()
                    .setName(TOPIC_NAME)
                    .setPartitions(
                        Collections.singletonList(
                            new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                                .setPartitionIndex(PARTITION_INDEX)
                                .setErrorCode((short) 0)
                                .setTimestamp(-1L)
                                .setOffset(BASE_OFFSET)
                                .setLeaderEpoch(CURRENT_LEADER_EPOCH)))));
  }

  /**
   * One transactional id with a 30s transaction timeout. From v3 the producer id and epoch are
   * carried too, at their own defaults (-1) — what a client that has never held a producer id
   * sends, and what this client always sends.
   *
   * @param version the InitProducerId request version, 2-5
   * @return a populated {@code InitProducerIdRequestData}
   */
  private static InitProducerIdRequestData buildInitProducerIdRequest(short version) {
    InitProducerIdRequestData request =
        new InitProducerIdRequestData()
            .setTransactionalId(TRANSACTIONAL_ID)
            .setTransactionTimeoutMs(TRANSACTION_TIMEOUT_MS);

    if (version >= 3) {
      request.setProducerId(-1L).setProducerEpoch((short) -1);
    }

    return request;
  }

  /**
   * A granted producer id at epoch 0, no error. v2-v5 share this shape.
   *
   * @return a populated {@code InitProducerIdResponseData}
   */
  private static InitProducerIdResponseData buildInitProducerIdResponse() {
    return new InitProducerIdResponseData()
        .setThrottleTimeMs(0)
        .setErrorCode((short) 0)
        .setProducerId(PRODUCER_ID)
        .setProducerEpoch(PRODUCER_EPOCH);
  }

  /**
   * Emits the Fetch v12-v18 request and response vectors: one topic, one partition, a full (never
   * incremental) fetch. The topic is keyed by name through v12 and by id from v13 (KIP-516); the
   * response carries one {@code NodeEndpoint} from v16 (KIP-951).
   *
   * @param dir target fixtures directory
   * @throws IOException if a fixture file cannot be written
   */
  private static void writeFetchVectors(Path dir) throws IOException {
    for (short version : new short[] {12, 13, 14, 15, 16, 17, 18}) {
      writeVector(dir, "v" + version + "_request_java.json", buildFetchRequest(version), version);
      writeVector(dir, "v" + version + "_response_java.json", buildFetchResponse(version), version);
    }
  }

  /**
   * Emits the OffsetForLeaderEpoch v3/v4 request and response vectors, one topic and one
   * partition. v4 re-encodes v3's fields as compact; no field is added or removed.
   *
   * @param dir target fixtures directory
   * @throws IOException if a fixture file cannot be written
   */
  private static void writeOffsetForLeaderEpochVectors(Path dir) throws IOException {
    for (short version : new short[] {3, 4}) {
      writeVector(
          dir,
          "v" + version + "_request_java.json",
          buildOffsetForLeaderEpochRequest(),
          version);
      writeVector(
          dir,
          "v" + version + "_response_java.json",
          buildOffsetForLeaderEpochResponse(),
          version);
    }
  }

  /**
   * Emits the SaslAuthenticate v2 request and response vectors, the flexible re-encoding of v1.
   *
   * @param dir target fixtures directory
   * @throws IOException if a fixture file cannot be written
   */
  private static void writeSaslAuthenticateVectors(Path dir) throws IOException {
    short version = 2;
    writeVector(dir, "v2_request_java.json", buildSaslAuthenticateRequest(), version);
    writeVector(dir, "v2_response_java.json", buildSaslAuthenticateResponse(), version);
  }

  /**
   * A consumer's full fetch: no session ({@code SessionId} 0, {@code SessionEpoch} -1), no
   * forgotten topics, no rack. {@code ReplicaId} closes at v14 and its v15 replacement
   * ({@code ReplicaState}, tag 1) is replica-only, so it is left at its default and never written.
   *
   * @param version the Fetch request version, 12-18
   * @return a populated {@code FetchRequestData}
   */
  private static FetchRequestData buildFetchRequest(short version) {
    FetchRequestData.FetchPartition partition =
        new FetchRequestData.FetchPartition()
            .setPartition(PARTITION_INDEX)
            .setCurrentLeaderEpoch(CURRENT_LEADER_EPOCH)
            .setFetchOffset(FETCH_OFFSET)
            .setLastFetchedEpoch(LAST_FETCHED_EPOCH)
            .setLogStartOffset(LOG_START_OFFSET)
            .setPartitionMaxBytes(PARTITION_MAX_BYTES);

    FetchRequestData.FetchTopic topic =
        new FetchRequestData.FetchTopic().setPartitions(Collections.singletonList(partition));
    if (version <= 12) {
      topic.setTopic(TOPIC_NAME);
    } else {
      topic.setTopicId(responseTopicId());
    }

    FetchRequestData request =
        new FetchRequestData()
            .setMaxWaitMs(FETCH_MAX_WAIT_MS)
            .setMinBytes(FETCH_MIN_BYTES)
            .setMaxBytes(FETCH_MAX_BYTES)
            .setIsolationLevel(READ_COMMITTED)
            .setSessionId(0)
            .setSessionEpoch(-1)
            .setTopics(Collections.singletonList(topic))
            .setForgottenTopicsData(Collections.emptyList())
            .setRackId("");

    if (version <= 14) {
      request.setReplicaId(CONSUMER_REPLICA_ID);
    }

    return request;
  }

  /**
   * One topic, one partition at high watermark 100 with no records — a null {@code Records} block,
   * which is the compact null every idle fetch answers with. From v16 one {@code NodeEndpoints}
   * entry (tag 0) is populated so the KIP-951 hint is exercised rather than skipped.
   *
   * @param version the Fetch response version, 12-18
   * @return a populated {@code FetchResponseData}
   */
  private static FetchResponseData buildFetchResponse(short version) {
    FetchResponseData.PartitionData partition =
        new FetchResponseData.PartitionData()
            .setPartitionIndex(PARTITION_INDEX)
            .setErrorCode((short) 0)
            .setHighWatermark(HIGH_WATERMARK)
            .setLastStableOffset(HIGH_WATERMARK)
            .setLogStartOffset(0L)
            .setAbortedTransactions(Collections.emptyList())
            .setPreferredReadReplica(-1)
            .setRecords(null);

    FetchResponseData.FetchableTopicResponse topic =
        new FetchResponseData.FetchableTopicResponse()
            .setPartitions(Collections.singletonList(partition));
    if (version <= 12) {
      topic.setTopic(TOPIC_NAME);
    } else {
      topic.setTopicId(responseTopicId());
    }

    FetchResponseData response =
        new FetchResponseData()
            .setThrottleTimeMs(0)
            .setErrorCode((short) 0)
            .setSessionId(0)
            .setResponses(Collections.singletonList(topic));

    if (version >= 16) {
      FetchResponseData.NodeEndpointCollection endpoints =
          new FetchResponseData.NodeEndpointCollection();
      endpoints.add(
          new FetchResponseData.NodeEndpoint()
              .setNodeId(LEADER_ID)
              .setHost(BROKER_HOST)
              .setPort(BROKER_PORT)
              .setRack(null));
      response.setNodeEndpoints(endpoints);
    }

    return response;
  }

  /**
   * One topic, one partition asking whether leader epoch 4 is still on the leader's log, at cached
   * leader epoch 5. {@code ReplicaId} is -1: the definition's own default is -2 ("debugging
   * consumer"), and a consumer sends -1.
   *
   * @return a populated {@code OffsetForLeaderEpochRequestData}
   */
  private static OffsetForLeaderEpochRequestData buildOffsetForLeaderEpochRequest() {
    OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection topics =
        new OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection();
    topics.add(
        new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic()
            .setTopic(TOPIC_NAME)
            .setPartitions(
                Collections.singletonList(
                    new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition()
                        .setPartition(PARTITION_INDEX)
                        .setCurrentLeaderEpoch(CURRENT_LEADER_EPOCH)
                        .setLeaderEpoch(VALIDATED_LEADER_EPOCH))));

    return new OffsetForLeaderEpochRequestData()
        .setReplicaId(CONSUMER_REPLICA_ID)
        .setTopics(topics);
  }

  /**
   * One topic, one partition answering end offset 120 at leader epoch 5, no error.
   *
   * @return a populated {@code OffsetForLeaderEpochResponseData}
   */
  private static OffsetForLeaderEpochResponseData buildOffsetForLeaderEpochResponse() {
    OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResultCollection topics =
        new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResultCollection();
    topics.add(
        new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult()
            .setTopic(TOPIC_NAME)
            .setPartitions(
                Collections.singletonList(
                    new OffsetForLeaderEpochResponseData.EpochEndOffset()
                        .setErrorCode((short) 0)
                        .setPartition(PARTITION_INDEX)
                        .setLeaderEpoch(CURRENT_LEADER_EPOCH)
                        .setEndOffset(EPOCH_END_OFFSET))));

    return new OffsetForLeaderEpochResponseData().setThrottleTimeMs(0).setTopics(topics);
  }

  /**
   * One mechanism payload, carried as compact bytes at v2. The payload's content is opaque to the
   * codec, so the same bytes serve both directions.
   *
   * @return a populated {@code SaslAuthenticateRequestData}
   */
  private static SaslAuthenticateRequestData buildSaslAuthenticateRequest() {
    return new SaslAuthenticateRequestData()
        .setAuthBytes(SASL_AUTH_BYTES.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * A successful authentication with a one-hour session lifetime and no error message.
   *
   * @return a populated {@code SaslAuthenticateResponseData}
   */
  private static SaslAuthenticateResponseData buildSaslAuthenticateResponse() {
    return new SaslAuthenticateResponseData()
        .setErrorCode((short) 0)
        .setErrorMessage(null)
        .setAuthBytes(SASL_AUTH_BYTES.getBytes(StandardCharsets.UTF_8))
        .setSessionLifetimeMs(SASL_SESSION_LIFETIME_MS);
  }

  /**
   * {@code Uuid.fromString} decodes Kafka's base64 uuid form, not the canonical 8-4-4-4-12 hex
   * string this repo's fixtures use, so the fixed response topic id is built from its raw 128
   * bits via {@link java.util.UUID} instead.
   *
   * @return the fixed response topic id {@code 01020304-0506-0708-090a-0b0c0d0e0f10}
   */
  private static Uuid responseTopicId() {
    UUID standard = UUID.fromString(RESPONSE_TOPIC_ID);
    return new Uuid(standard.getMostSignificantBits(), standard.getLeastSignificantBits());
  }

  /**
   * Serializes one message at one version, following the standard {@code ApiMessage}
   * serialization pattern, and writes it as a {@code {"type":"Buffer","data":[...]}} fixture —
   * the shape every fixture in this repo's {@code src/protocol/requests/*}/fixtures} uses.
   *
   * @param dir target fixtures directory
   * @param fileName fixture file name
   * @param data the message to serialize
   * @param version the message version to serialize at
   * @throws IOException if the fixture file cannot be written
   */
  private static void writeVector(Path dir, String fileName, Message data, short version)
      throws IOException {
    ObjectSerializationCache cache = new ObjectSerializationCache();
    int size = data.size(cache, version);
    ByteBuffer buffer = ByteBuffer.allocate(size);
    data.write(new ByteBufferAccessor(buffer), cache, version);
    buffer.flip();

    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);

    Path path = dir.resolve(fileName);
    Files.write(path, toBufferJson(bytes).getBytes(StandardCharsets.UTF_8));
    System.out.println("wrote " + path);
  }

  /**
   * @param bytes serialized message body
   * @return the bytes as a {@code {"type":"Buffer","data":[...]}} JSON document
   */
  private static String toBufferJson(byte[] bytes) {
    StringBuilder json = new StringBuilder("{\"type\":\"Buffer\",\"data\":[");
    for (int i = 0; i < bytes.length; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append(bytes[i] & 0xFF);
    }
    return json.append("]}\n").toString();
  }
}
