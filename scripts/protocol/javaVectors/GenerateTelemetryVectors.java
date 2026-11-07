import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.resource.v1.Resource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;

/** Produces constant numeric-subset vectors with the independently generated v1.9.0 bindings.
 * All inputs are literals; neither production JavaScript nor a JavaScript decoder is loaded.
 */
public final class GenerateTelemetryVectors {
  /** Stores constant semantic inputs alongside independently encoded bytes. */
  record Vector(String name, String fields, MetricsData data) {}

  /** @param key literal key @param value literal string @return string-only KeyValue */
  static KeyValue kv(String key, String value) {
    return KeyValue.newBuilder().setKey(key).setValue(AnyValue.newBuilder().setStringValue(value)).build();
  }

  /** @param start unsigned decimal start @param time unsigned decimal time
   * @param value exact boxed Long or Double @param attributes canonical literals
   * @return one fixed64/sfixed64/double data point
   * @throws NumberFormatException if an unsigned timestamp is out of range
   */
  static NumberDataPoint point(String start, String time, Number value, List<KeyValue> attributes) {
    NumberDataPoint.Builder b = NumberDataPoint.newBuilder()
      .setStartTimeUnixNano(Long.parseUnsignedLong(start)).setTimeUnixNano(Long.parseUnsignedLong(time))
      .addAllAttributes(attributes);
    if (value instanceof Long) b.setAsInt(value.longValue());
    else b.setAsDouble(value.doubleValue());
    return b.build();
  }

  /** @param name literal name @param unit unit @param sum true for Sum
   * @param temporality 1 or 2 for Sum @param points canonical data points
   * @return independently encoded aggregate; primitive signed extrema may be Gauges
   */
  static Metric metric(String name, String unit, boolean sum, int temporality, List<NumberDataPoint> points) {
    Metric.Builder b = Metric.newBuilder().setName(name).setUnit(unit);
    if (sum) b.setSum(Sum.newBuilder().addAllDataPoints(points).setAggregationTemporalityValue(temporality).setIsMonotonic(true));
    else b.setGauge(Gauge.newBuilder().addAllDataPoints(points));
    return b.build();
  }

  /** @param attributes resource literals @param dropped omitted resource count
   * @param name scope name @param version scope version @param metrics canonical aggregates
   * @return zero-byte MetricsData for no points, otherwise exactly one resource and scope
   */
  static MetricsData data(List<KeyValue> attributes, int dropped, String name, String version, List<Metric> metrics) {
    if (metrics.isEmpty()) return MetricsData.getDefaultInstance();
    return MetricsData.newBuilder().addResourceMetrics(ResourceMetrics.newBuilder()
      .setResource(Resource.newBuilder().addAllAttributes(attributes).setDroppedAttributesCount(dropped))
      .addScopeMetrics(ScopeMetrics.newBuilder().setScope(InstrumentationScope.newBuilder().setName(name).setVersion(version))
        .addAllMetrics(metrics))).build();
  }

  /** @return all fixed inputs, in stable filename order */
  static List<Vector> vectors() {
    return List.of(
    new Vector("cumulative","{\"revision\":\"1\",\"subscriptionRevision\":\"1\",\"points\":[{\"identity\":{\"key\":\"[\\\"com.example.count\\\",[]]\",\"revision\":\"1\"},\"name\":\"com.example.count\",\"type\":\"sum\",\"unit\":\"1\",\"attributes\":[],\"value\":\"9223372036854775807\",\"timeUnixNano\":\"18446744073709551615\",\"startTimeUnixNano\":\"9223372036854775808\",\"aggregationTemporality\":2,\"isMonotonic\":true}],\"resources\":[],\"droppedResourceAttributesCount\":0,\"scope\":{\"name\":\"countly-kafkajs\",\"version\":\"3.0.0\"}}",data(List.of(),0,"countly-kafkajs","3.0.0", List.of(metric("com.example.count","1",true,2, List.of(point("9223372036854775808","18446744073709551615",Long.valueOf("9223372036854775807"),List.of())))))),
    new Vector("empty","{\"revision\":\"1\",\"subscriptionRevision\":\"1\",\"points\":[],\"resources\":[],\"droppedResourceAttributesCount\":0,\"scope\":{\"name\":\"countly-kafkajs\",\"version\":\"3.0.0\"}}",data(List.of(),0,"countly-kafkajs","3.0.0", List.of())),
    new Vector("multibyte","{\"revision\":\"1\",\"subscriptionRevision\":\"1\",\"points\":[{\"identity\":{\"key\":\"[\\\"com.example.utf8\\\",[[\\\"label\\\",\\\"温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温\\\"]]]\",\"revision\":\"1\"},\"name\":\"com.example.utf8\",\"type\":\"gauge\",\"unit\":\"1\",\"attributes\":[{\"key\":\"label\",\"value\":\"温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温\"}],\"value\":1,\"timeUnixNano\":\"1700000000123456789\"}],\"resources\":[{\"key\":\"client_rack\",\"value\":\"温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温\"}],\"droppedResourceAttributesCount\":1,\"scope\":{\"name\":\"countly-kafkajs\",\"version\":\"3.0.0\"}}",data(List.of(kv("client_rack","温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温")),1,"countly-kafkajs","3.0.0", List.of(metric("com.example.utf8","1",false,0, List.of(point("0","1700000000123456789",Double.valueOf(1),List.of(kv("label","温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温温")))))))),
    new Vector("negative-zero","{\"revision\":\"1\",\"subscriptionRevision\":\"1\",\"points\":[{\"identity\":{\"key\":\"[\\\"com.example.gauge\\\",[]]\",\"revision\":\"1\"},\"name\":\"com.example.gauge\",\"type\":\"gauge\",\"unit\":\"1\",\"attributes\":[],\"value\":-0.0,\"timeUnixNano\":\"1\"}],\"resources\":[],\"droppedResourceAttributesCount\":0,\"scope\":{\"name\":\"countly-kafkajs\",\"version\":\"3.0.0\"}}",data(List.of(),0,"countly-kafkajs","3.0.0", List.of(metric("com.example.gauge","1",false,0, List.of(point("0","1",Double.valueOf(-0.0),List.of())))))),
    new Vector("nested","{\"revision\":\"1\",\"subscriptionRevision\":\"1\",\"points\":[{\"identity\":{\"key\":\"[\\\"com.example.count\\\",[]]\",\"revision\":\"1\"},\"name\":\"com.example.count\",\"type\":\"sum\",\"unit\":\"1\",\"attributes\":[],\"value\":\"9007199254740993\",\"timeUnixNano\":\"1700000000123456789\",\"startTimeUnixNano\":\"1700000000000000000\",\"aggregationTemporality\":1,\"isMonotonic\":true},{\"identity\":{\"key\":\"[\\\"com.example.gauge\\\",[[\\\"zone\\\",\\\"温度\\\"]]]\",\"revision\":\"1\"},\"name\":\"com.example.gauge\",\"type\":\"gauge\",\"unit\":\"1\",\"attributes\":[{\"key\":\"zone\",\"value\":\"温度\"}],\"value\":-2.25,\"timeUnixNano\":\"1700000000123456789\"},{\"identity\":{\"key\":\"[\\\"com.example.gauge\\\",[[\\\"zone\\\",\\\"a\\\"]]]\",\"revision\":\"1\"},\"name\":\"com.example.gauge\",\"type\":\"gauge\",\"unit\":\"1\",\"attributes\":[{\"key\":\"zone\",\"value\":\"a\"}],\"value\":0,\"timeUnixNano\":\"1700000000123456789\"}],\"resources\":[{\"key\":\"group_id\",\"value\":\"订单\"},{\"key\":\"client_rack\",\"value\":\"r1\"}],\"droppedResourceAttributesCount\":1,\"scope\":{\"name\":\"countly-kafkajs\",\"version\":\"3.0.0\"}}",data(List.of(kv("client_rack","r1"),kv("group_id","订单")),1,"countly-kafkajs","3.0.0", List.of(metric("com.example.count","1",true,1, List.of(point("1700000000000000000","1700000000123456789",Long.valueOf("9007199254740993"),List.of()))),metric("com.example.gauge","1",false,0, List.of(point("0","1700000000123456789",Double.valueOf(0),List.of(kv("zone","a"))),point("0","1700000000123456789",Double.valueOf(-2.25),List.of(kv("zone","温度")))))))),
    new Vector("signed-max","{\"revision\":\"1\",\"subscriptionRevision\":\"1\",\"points\":[{\"identity\":{\"key\":\"[\\\"primitive\\\",[]]\",\"revision\":\"1\"},\"name\":\"primitive\",\"type\":\"gauge\",\"unit\":\"1\",\"attributes\":[],\"value\":\"9223372036854775807\",\"timeUnixNano\":\"1\",\"valueKind\":\"int\"}],\"resources\":[],\"droppedResourceAttributesCount\":0,\"scope\":{\"name\":\"countly-kafkajs\",\"version\":\"3.0.0\"}}",data(List.of(),0,"countly-kafkajs","3.0.0", List.of(metric("primitive","1",false,0, List.of(point("0","1",Long.valueOf("9223372036854775807"),List.of())))))),
    new Vector("signed-min","{\"revision\":\"1\",\"subscriptionRevision\":\"1\",\"points\":[{\"identity\":{\"key\":\"[\\\"primitive\\\",[]]\",\"revision\":\"1\"},\"name\":\"primitive\",\"type\":\"gauge\",\"unit\":\"1\",\"attributes\":[],\"value\":\"-9223372036854775808\",\"timeUnixNano\":\"18446744073709551615\",\"valueKind\":\"int\"}],\"resources\":[],\"droppedResourceAttributesCount\":0,\"scope\":{\"name\":\"countly-kafkajs\",\"version\":\"3.0.0\"}}",data(List.of(),0,"countly-kafkajs","3.0.0", List.of(metric("primitive","1",false,0, List.of(point("0","18446744073709551615",Long.valueOf("-9223372036854775808"),List.of())))))),
    new Vector("zero","{\"revision\":\"1\",\"subscriptionRevision\":\"1\",\"points\":[{\"identity\":{\"key\":\"[\\\"com.example.zero\\\",[]]\",\"revision\":\"1\"},\"name\":\"com.example.zero\",\"type\":\"gauge\",\"unit\":\"1\",\"attributes\":[],\"value\":0,\"timeUnixNano\":\"0\"},{\"identity\":{\"key\":\"[\\\"com.example.count\\\",[]]\",\"revision\":\"1\"},\"name\":\"com.example.count\",\"type\":\"sum\",\"unit\":\"1\",\"attributes\":[],\"value\":\"0\",\"timeUnixNano\":\"0\",\"startTimeUnixNano\":\"0\",\"aggregationTemporality\":1,\"isMonotonic\":true}],\"resources\":[],\"droppedResourceAttributesCount\":0,\"scope\":{\"name\":\"countly-kafkajs\",\"version\":\"3.0.0\"}}",data(List.of(),0,"countly-kafkajs","3.0.0", List.of(metric("com.example.count","1",true,1, List.of(point("0","0",Long.valueOf("0"),List.of()))),metric("com.example.zero","1",false,0, List.of(point("0","0",Double.valueOf(0),List.of()))))))
    );
  }

  /** @param file verified input @return full SHA-256 @throws Exception for file/hash failure */
  static String digest(Path file) throws Exception {
    return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
  }

  /** @param text bounded JSON string @return escaped JSON string literal */
  static String quote(String text) {
    return "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
  }

  /** Reads candidate bytes through the generated Java parser, exposing presence and exact scalar values.
   * @param data independently parsed message @return deterministic protobuf text plus exact decimal/bit audit
   */
  static String decoded(MetricsData data) {
    StringBuilder result = new StringBuilder(data.toString());
    for (ResourceMetrics rm : data.getResourceMetricsList()) {
      result.append("resourceDropped=").append(Integer.toUnsignedString(rm.getResource().getDroppedAttributesCount())).append('\n');
      for (ScopeMetrics sm : rm.getScopeMetricsList()) for (Metric m : sm.getMetricsList()) {
        List<NumberDataPoint> points = m.hasSum() ? m.getSum().getDataPointsList() : m.getGauge().getDataPointsList();
        for (NumberDataPoint p : points) {
          result.append(m.getName()).append('|').append(p.getValueCase()).append('|')
            .append(Long.toUnsignedString(p.getStartTimeUnixNano())).append('|')
            .append(Long.toUnsignedString(p.getTimeUnixNano())).append('|');
          if (p.hasAsInt()) result.append(Long.toString(p.getAsInt()));
          else result.append(Double.toString(p.getAsDouble())).append('|').append(Long.toUnsignedString(Double.doubleToRawLongBits(p.getAsDouble()), 16));
          result.append('\n');
        }
      }
    }
    return result.toString();
  }

  /** Generates new vectors or decodes a candidate body. Never fetches dependencies.
   * @param args --decode input.bin output.txt, --verify input-directory output-directory,
   * or output-directory source-file bindings-jar protobuf-jar
   * @throws Exception for wrong arguments, digests, encoding, I/O or independent decode failure
   */
  public static void main(String[] args) throws Exception {
    if (args.length == 3 && args[0].equals("--decode")) {
      Files.writeString(Path.of(args[2]), decoded(MetricsData.parseFrom(Files.readAllBytes(Path.of(args[1])))));
      return;
    }
    if (args.length == 3 && args[0].equals("--verify")) {
      Path out = Path.of(args[2]);
      Files.createDirectories(out);
      for (Vector v : vectors()) {
        byte[] actual = Files.readAllBytes(Path.of(args[1]).resolve(v.name()+".bin"));
        MetricsData parsed = MetricsData.parseFrom(actual);
        if (!parsed.equals(v.data())) throw new IllegalStateException("Candidate semantics differ: " + v.name());
        if (!java.util.Arrays.equals(actual, v.data().toByteArray())) throw new IllegalStateException("Candidate bytes differ: " + v.name());
        Files.writeString(out.resolve(v.name()+".decoded.txt"), decoded(parsed));
      }
      return;
    }
    if (args.length != 4) throw new IllegalArgumentException("Expected output, source, bindings and protobuf paths");
    Path out = Path.of(args[0]);
    Files.createDirectories(out);
    String source = digest(Path.of(args[1]));
    if (!digest(Path.of(args[2])).equals("99676bd7a6eff2b8e068a1ab10fe30cadf27958e82914adc6516699c88e266e7") ||
        !digest(Path.of(args[3])).equals("8540247fad9e06baefa8fb45eb313802d019f485f14300e0f9d6b556ed88e753"))
      throw new IllegalArgumentException("Independent classpath digest mismatch");
    for (Vector v : vectors()) {
      byte[] bytes = v.data().toByteArray();
      MetricsData parsed = MetricsData.parseFrom(bytes);
      if (!parsed.equals(v.data())) throw new IllegalStateException("Independent round trip differs");
      String provenance = "{\"protoTag\":\"v1.9.0\",\"protoSha256\":{\"common/v1/common.proto\":\"354d818af9cb0ed67f7447b9cf8408f1ac8911cedea0db73c5309484ec2bcc04\",\"metrics/v1/metrics.proto\":\"d0a53dda45ad100edbf25675f5b339ce8bf689bdc51816f8cf19b0d51464e1a4\",\"resource/v1/resource.proto\":\"e0a7cdc0ffcfeffaa2606e8611839735ebffaa2d6acdf33e9356f2c48ae692d3\"},\"generator\":\"scripts/protocol/javaVectors/GenerateTelemetryVectors.java\",\"bindingsArtifact\":\"otlp-v1.9.0-bindings-unrelocated.jar\",\"bindingsProvenance\":\"frozen SHA-256-pinned artifact\",\"bindingsSha256\":\"99676bd7a6eff2b8e068a1ab10fe30cadf27958e82914adc6516699c88e266e7\",\"protobufJavaCoordinates\":\"com.google.protobuf:protobuf-java:3.25.5\",\"protobufJavaSha1\":\"5ae5c9ec39930ae9b5a61b32b93288818ec05ec1\",\"protobufJavaSha1Url\":\"https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.25.5/protobuf-java-3.25.5.jar.sha1\",\"protobufJavaSha256\":\"8540247fad9e06baefa8fb45eb313802d019f485f14300e0f9d6b556ed88e753\",\"protocVersion\":\"25.5\",\"protocSha256\":\"d69b0a9ac8264bb3d464ae22742430943aa0a6cdf3734180af5a607c0bf8adaa\",\"status\":\"generated with pinned Java bindings\"";
      String json = "{\"message\":\"MetricsData\",\"version\":\"1.9.0\",\"case\":" + quote(v.name()) +
        ",\"hex\":" + quote(HexFormat.of().formatHex(bytes)) + ",\"fields\":" + v.fields() +
        ",\"provenance\":" + provenance + ",\"generatorSha256\":" + quote(source) + "}}\n";
      Files.writeString(out.resolve(v.name()+".json"), json);
      Files.writeString(out.resolve(v.name()+".decoded.txt"), decoded(parsed));
    }
  }
}
