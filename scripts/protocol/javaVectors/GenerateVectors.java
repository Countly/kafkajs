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
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
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
 * @see <a href="https://kafka.apache.org/43/design/protocol">Kafka protocol guide</a>
 * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/ApiVersionsRequest.json">ApiVersionsRequest.json</a>
 * @see <a href="https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/MetadataRequest.json">MetadataRequest.json</a>
 */
public final class GenerateVectors {

  private static final String CLIENT_SOFTWARE_NAME = "countly-kafkajs";
  private static final String CLIENT_SOFTWARE_VERSION = "3.0.0-alpha.4";
  private static final String TOPIC_NAME = "test-topic";
  private static final String RESPONSE_TOPIC_ID = "01020304-0506-0708-090a-0b0c0d0e0f10";

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
    Path apiVersionsDir = outputDir.resolve("src/protocol/requests/apiVersions/fixtures");
    Path metadataDir = outputDir.resolve("src/protocol/requests/metadata/fixtures");
    Files.createDirectories(apiVersionsDir);
    Files.createDirectories(metadataDir);

    writeApiVersionsVectors(apiVersionsDir);
    writeMetadataVectors(metadataDir);
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
   * Emits the Metadata v9-v12 request and response vectors, one topic requested/returned by name.
   *
   * @param dir target fixtures directory
   * @throws IOException if a fixture file cannot be written
   */
  private static void writeMetadataVectors(Path dir) throws IOException {
    for (short version : new short[] {9, 10, 11, 12}) {
      writeVector(dir, "v" + version + "_request_java.json", buildMetadataRequest(version), version);
      writeVector(dir, "v" + version + "_response_java.json", buildMetadataResponse(version), version);
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
   * @param version the Metadata request version, 9-12
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
   * fixtures use. {@code TopicId} is populated from v10 and {@code ClusterAuthorizedOperations}
   * only through v10, same rules as the request.
   *
   * @param version the Metadata response version, 9-12
   * @return a populated {@code MetadataResponseData}
   */
  private static MetadataResponseData buildMetadataResponse(short version) {
    MetadataResponseData.MetadataResponseBrokerCollection brokers =
        new MetadataResponseData.MetadataResponseBrokerCollection();
    brokers.add(
        new MetadataResponseData.MetadataResponseBroker()
            .setNodeId(0)
            .setHost("127.0.0.1")
            .setPort(9092)
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

    return response;
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
