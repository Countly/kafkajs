package org.countly.kafkajs.telemetry;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.telemetry.ClientTelemetryContext;
import org.apache.kafka.server.telemetry.ClientTelemetryExporter;
import org.apache.kafka.server.telemetry.ClientTelemetryExporterProvider;
import org.apache.kafka.server.telemetry.ClientTelemetryPayload;
import org.countly.kafkajs.telemetry.otlp.common.v1.AnyValue;
import org.countly.kafkajs.telemetry.otlp.common.v1.InstrumentationScope;
import org.countly.kafkajs.telemetry.otlp.common.v1.KeyValue;
import org.countly.kafkajs.telemetry.otlp.metrics.v1.Metric;
import org.countly.kafkajs.telemetry.otlp.metrics.v1.MetricsData;
import org.countly.kafkajs.telemetry.otlp.metrics.v1.NumberDataPoint;
import org.countly.kafkajs.telemetry.otlp.metrics.v1.ResourceMetrics;
import org.countly.kafkajs.telemetry.otlp.metrics.v1.ScopeMetrics;
import org.countly.kafkajs.telemetry.otlp.resource.v1.Resource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only broker plugin: a {@link MetricsReporter} that is also a {@link ClientTelemetryExporterProvider}.
 * Each pushed client-metrics payload is decoded with the independent OTLP v1.9.0 bindings and written as one
 * JSON line (at most {@value #MAX_RECORD_BYTES} bytes) to a bounded JSONL sink. The sink is a 128-entry queue
 * drained by one daemon thread into a file capped at 2 MiB; any overflow writes an OVERFLOW marker file and
 * must be treated as a fixture failure. Nothing accumulates in broker heap beyond that queue.
 *
 * <p>Sink directory: env {@code TELEMETRY_EVIDENCE_DIR} (default {@code /var/telemetry-evidence}). Files per broker
 * node: {@code reporter-<node>.json} (load proof), {@code exports-<node>.jsonl}, {@code OVERFLOW-<node>.json}.
 *
 * <p>{@code main}: {@code decode <hex>|--file <path>} prints the same JSON for offline golden-vector decoding;
 * {@code check} loads the plugin interfaces and the OTLP bindings and decodes an empty payload; {@code stress <hex> <count>}
 * runs the overload test of the bounded sink.
 */
public class CountingTelemetryReporter implements MetricsReporter, ClientTelemetryExporterProvider {

    static final int QUEUE_CAPACITY = 128;
    static final int MAX_RECORD_BYTES = 4096;
    static final long MAX_FILE_BYTES = 2L * 1024 * 1024;
    private static final int MAX_STRING = 96;
    private static final int MAX_ATTRS = 16;
    private static final int MAX_POINTS = 4;

    private final BlockingQueue<String> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final AtomicLong seq = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final ClientTelemetryExporter exporter = this::export;
    private volatile String node = "unknown";
    private volatile Path dir;
    private volatile Thread writer;
    private volatile boolean closed;
    private volatile String firstReason = "";

    /** Reads {@code node.id} and starts the sink writer; writes the load-proof file. */
    @Override
    public void configure(Map<String, ?> configs) {
        Object id = configs.get("node.id");
        if (id == null) id = configs.get("broker.id");
        if (id != null) node = String.valueOf(id);
        String env = System.getenv("TELEMETRY_EVIDENCE_DIR");
        dir = Path.of(env == null || env.isEmpty() ? "/var/telemetry-evidence" : env);
        try {
            Files.createDirectories(dir);
            String proof = "{\"v\":1,\"node\":" + q(node) + ",\"class\":" + q(getClass().getName())
                + ",\"kafkaClients\":" + q(AppInfoParser.getVersion()) + ",\"kafkaCommit\":" + q(AppInfoParser.getCommitId())
                + ",\"java\":" + q(System.getProperty("java.runtime.version"))
                + ",\"protobufJar\":" + q(codeSource(com.google.protobuf.Message.class))
                + ",\"otlpBindings\":" + q(codeSource(MetricsData.class))
                + ",\"reporterJar\":" + q(codeSource(CountingTelemetryReporter.class))
                + ",\"queueCapacity\":" + QUEUE_CAPACITY + ",\"maxRecordBytes\":" + MAX_RECORD_BYTES
                + ",\"maxFileBytes\":" + MAX_FILE_BYTES + ",\"loadedAtMs\":" + System.currentTimeMillis() + "}\n";
            Files.writeString(dir.resolve("reporter-" + node + ".json"), proof);
        } catch (Throwable t) {
            overflow("init:" + t);
        }
        Thread w = new Thread(this::drain, "counting-telemetry-sink-" + node);
        w.setDaemon(true);
        writer = w;
        w.start();
    }

    private static String codeSource(Class<?> c) {
        try {
            return c.getProtectionDomain().getCodeSource().getLocation().getPath();
        } catch (Throwable t) {
            return "unknown";
        }
    }

    @Override public void init(List<KafkaMetric> metrics) { }
    @Override public void metricChange(KafkaMetric metric) { }
    @Override public void metricRemoval(KafkaMetric metric) { }

    /** Stops the writer after draining what is queued. */
    @Override
    public void close() {
        closed = true;
        Thread w = writer;
        if (w != null) {
            w.interrupt();
            try { w.join(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        if (dropped.get() > 5) writeMarker();
    }

    @Override
    public ClientTelemetryExporter clientTelemetryExporter() {
        return exporter;
    }

    private void export(ClientTelemetryContext context, ClientTelemetryPayload payload) {
        try {
            ByteBuffer buf = payload.data().asReadOnlyBuffer();
            byte[] data = new byte[buf.remaining()];
            buf.get(data);
            AuthorizableRequestContext rc = context.authorizableRequestContext();
            StringBuilder head = new StringBuilder(256);
            head.append("{\"v\":1,\"node\":").append(q(node))
                .append(",\"seq\":").append(seq.incrementAndGet())
                .append(",\"tsMs\":").append(System.currentTimeMillis())
                .append(",\"clientInstanceId\":").append(q(String.valueOf(payload.clientInstanceId())))
                .append(",\"terminating\":").append(payload.isTerminating())
                .append(",\"contentType\":").append(q(payload.contentType()))
                .append(",\"pushIntervalMs\":").append(context.pushIntervalMs())
                .append(",\"clientId\":").append(q(rc == null ? null : rc.clientId()))
                .append(",\"listener\":").append(q(rc == null ? null : rc.listenerName()))
                .append(",\"principal\":").append(q(rc == null || rc.principal() == null ? null : rc.principal().toString()))
                .append(",");
            enqueue(head + decodeBody(data, true));
        } catch (Throwable t) {
            // never throw into the broker request path
            overflow("export:" + t);
        }
    }

    /**
     * Decodes OTLP MetricsData bytes into the record tail (fields after the identity head, closing brace included).
     *
     * @param data the decompressed payload
     * @param capped whether to shed detail so that head plus tail stays within the per-record cap
     * @return the JSON tail, starting with {@code "dataBytes":}
     */
    static String decodeBody(byte[] data, boolean capped) throws Exception {
        String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        StringBuilder tail = new StringBuilder(512);
        tail.append("\"dataBytes\":").append(data.length).append(",\"dataSha256\":").append(q(sha));
        MetricsData md;
        try {
            md = MetricsData.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            tail.append(",\"parse\":").append(q("error:" + e.getMessage())).append(",\"truncated\":false}");
            return tail.toString();
        }
        int unknown = md.getUnknownFields().getSerializedSize();
        int budget = capped ? MAX_RECORD_BYTES - 700 : Integer.MAX_VALUE;
        StringBuilder res = new StringBuilder("[");
        StringBuilder met = new StringBuilder("[");
        int metricsTotal = 0, metricsKept = 0, pointsTotal = 0;
        boolean truncated = false;
        int ri = 0;
        for (ResourceMetrics rm : md.getResourceMetricsList()) {
            Resource r = rm.getResource();
            if (ri > 0) res.append(',');
            res.append("{\"attrs\":").append(attrs(r.getAttributesList()))
                .append(",\"droppedAttributes\":").append(Integer.toUnsignedString(r.getDroppedAttributesCount()))
                .append(",\"hasResource\":").append(rm.hasResource())
                .append(",\"schemaUrl\":").append(q(rm.getSchemaUrl())).append('}');
            int si = 0;
            for (ScopeMetrics sm : rm.getScopeMetricsList()) {
                InstrumentationScope sc = sm.getScope();
                for (Metric m : sm.getMetricsList()) {
                    metricsTotal++;
                    StringBuilder e = new StringBuilder(160);
                    e.append("{\"res\":").append(ri).append(",\"scope\":").append(si)
                        .append(",\"scopeName\":").append(q(sc.getName())).append(",\"scopeVersion\":").append(q(sc.getVersion()))
                        .append(",\"scopeDroppedAttributes\":").append(Integer.toUnsignedString(sc.getDroppedAttributesCount()))
                        .append(",\"name\":").append(q(m.getName())).append(",\"unit\":").append(q(m.getUnit()))
                        .append(",\"kind\":").append(q(m.getDataCase().name()));
                    List<NumberDataPoint> pts = List.of();
                    switch (m.getDataCase()) {
                        case GAUGE -> pts = m.getGauge().getDataPointsList();
                        case SUM -> {
                            pts = m.getSum().getDataPointsList();
                            e.append(",\"temporality\":").append(q(m.getSum().getAggregationTemporality().name()))
                                .append(",\"monotonic\":").append(m.getSum().getIsMonotonic());
                        }
                        case HISTOGRAM -> e.append(",\"pointCount\":").append(m.getHistogram().getDataPointsCount());
                        case EXPONENTIAL_HISTOGRAM -> e.append(",\"pointCount\":").append(m.getExponentialHistogram().getDataPointsCount());
                        case SUMMARY -> e.append(",\"pointCount\":").append(m.getSummary().getDataPointsCount());
                        default -> { }
                    }
                    pointsTotal += pts.size();
                    e.append(",\"points\":[");
                    int n = 0;
                    for (NumberDataPoint p : pts) {
                        if (n == MAX_POINTS && capped) break;
                        if (n++ > 0) e.append(',');
                        e.append("{\"attrs\":").append(attrs(p.getAttributesList()))
                            .append(",\"startNs\":").append(q(Long.toUnsignedString(p.getStartTimeUnixNano())))
                            .append(",\"timeNs\":").append(q(Long.toUnsignedString(p.getTimeUnixNano())))
                            .append(",\"flags\":").append(Integer.toUnsignedString(p.getFlags()))
                            .append(",\"valueKind\":").append(q(p.getValueCase().name()));
                        switch (p.getValueCase()) {
                            case AS_INT -> e.append(",\"asInt\":").append(q(Long.toString(p.getAsInt())));
                            case AS_DOUBLE -> e.append(",\"asDouble\":").append(q(Double.toString(p.getAsDouble())))
                                .append(",\"asDoubleBits\":").append(q(Long.toHexString(Double.doubleToRawLongBits(p.getAsDouble()))));
                            default -> { }
                        }
                        e.append('}');
                    }
                    e.append("],\"pointsInMessage\":").append(pts.size()).append('}');
                    if (!truncated && met.length() + e.length() + res.length() <= budget) {
                        if (metricsKept++ > 0) met.append(',');
                        met.append(e);
                    } else {
                        truncated = true;
                    }
                }
                si++;
            }
            ri++;
        }
        res.append(']');
        met.append(']');
        tail.append(",\"parse\":\"ok\",\"unknownFieldBytes\":").append(unknown)
            .append(",\"resourceMetricsCount\":").append(md.getResourceMetricsCount())
            .append(",\"metricsTotal\":").append(metricsTotal).append(",\"pointsTotal\":").append(pointsTotal)
            .append(",\"metricsKept\":").append(metricsKept).append(",\"truncated\":").append(truncated)
            .append(",\"resources\":").append(res).append(",\"metrics\":").append(met).append('}');
        return tail.toString();
    }

    private static String attrs(List<KeyValue> kvs) {
        StringBuilder b = new StringBuilder("[");
        int n = 0;
        for (KeyValue kv : kvs) {
            if (n >= MAX_ATTRS) { b.append(",[\"...\",\"more\",").append(kvs.size() - n).append(']'); break; }
            if (n++ > 0) b.append(',');
            AnyValue v = kv.getValue();
            String t, s;
            switch (v.getValueCase()) {
                case STRING_VALUE -> { t = "string"; s = v.getStringValue(); }
                case BOOL_VALUE -> { t = "bool"; s = Boolean.toString(v.getBoolValue()); }
                case INT_VALUE -> { t = "int"; s = Long.toString(v.getIntValue()); }
                case DOUBLE_VALUE -> { t = "double"; s = Double.toString(v.getDoubleValue()); }
                default -> { t = v.getValueCase().name().toLowerCase(); s = ""; }
            }
            b.append('[').append(q(kv.getKey())).append(',').append(q(t)).append(',').append(q(s)).append(']');
        }
        return b.append(']').toString();
    }

    private void enqueue(String line) {
        String record = line;
        if (record.getBytes(StandardCharsets.UTF_8).length > MAX_RECORD_BYTES) {
            // detail shedding failed (huge identity fields): keep only a fixed-size stub, flagged
            record = "{\"v\":1,\"node\":" + q(node) + ",\"seq\":" + seq.get() + ",\"stub\":true,\"truncated\":true}";
        }
        if (!queue.offer(record + "\n")) overflow("queue-full");
    }

    private void overflow(String reason) {
        long n = dropped.incrementAndGet();
        if (n == 1) firstReason = reason;
        if (n <= 5) writeMarker();
    }

    /** Writes or rewrites the overflow marker with the first reason and the drop count so far (best effort). */
    private void writeMarker() {
        try {
            Path d = dir;
            if (d != null) {
                Files.writeString(d.resolve("OVERFLOW-" + node + ".json"), "{\"reason\":" + q(firstReason) + ",\"droppedCount\":"
                    + dropped.get() + ",\"tsMs\":" + System.currentTimeMillis() + "}\n");
            }
        } catch (Throwable ignored) {
            // nothing more can be done; the marker is best effort
        }
    }

    private void drain() {
        Path file = dir.resolve("exports-" + node + ".jsonl");
        long written = 0;
        try { if (Files.exists(file)) written = Files.size(file); } catch (Throwable ignored) { }
        while (true) {
            String line;
            try {
                line = closed ? queue.poll() : queue.take();
            } catch (InterruptedException e) {
                line = queue.poll();
                if (line == null) return;
            }
            if (line == null) return;
            byte[] b = line.getBytes(StandardCharsets.UTF_8);
            if (written + b.length > MAX_FILE_BYTES) { overflow("file-cap"); continue; }
            try {
                Files.write(file, b, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                written += b.length;
            } catch (Throwable t) {
                overflow("write:" + t);
            }
        }
    }

    private static String q(String s) {
        if (s == null) return "null";
        String t = s.length() > MAX_STRING ? s.substring(0, MAX_STRING) : s;
        StringBuilder b = new StringBuilder(t.length() + 2).append('"');
        for (int i = 0; i < t.length(); i++) {
            char c = t.charAt(i);
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                default -> { if (c < 0x20) b.append(String.format("\\u%04x", (int) c)); else b.append(c); }
            }
        }
        return b.append('"').toString();
    }

    /**
     * Overload test without a broker: pushes {@code count} exports of the given payload as fast as possible through the
     * real exporter into {@code TELEMETRY_EVIDENCE_DIR} (node id "stress"), then closes and prints the sink state.
     * The sink must stay within 2 MiB and 4096-byte lines, and the overflow marker must appear once it is exceeded.
     */
    private static void stress(byte[] data, int count) throws Exception {
        CountingTelemetryReporter r = new CountingTelemetryReporter();
        r.configure(Map.of("node.id", "stress"));
        ClientTelemetryExporter e = r.clientTelemetryExporter();
        ClientTelemetryContext ctx = new ClientTelemetryContext() {
            @Override public int pushIntervalMs() { return 1000; }
            @Override public AuthorizableRequestContext authorizableRequestContext() { return null; }
        };
        for (int i = 0; i < count; i++) {
            ClientTelemetryPayload p = new ClientTelemetryPayload() {
                private final org.apache.kafka.common.Uuid id = org.apache.kafka.common.Uuid.randomUuid();
                @Override public org.apache.kafka.common.Uuid clientInstanceId() { return id; }
                @Override public boolean isTerminating() { return false; }
                @Override public String contentType() { return "OTLP"; }
                @Override public ByteBuffer data() { return ByteBuffer.wrap(data); }
            };
            e.exportMetrics(ctx, p);
        }
        r.close();
        Path d = r.dir;
        long size = Files.size(d.resolve("exports-stress.jsonl"));
        long maxLine = 0, lines = 0;
        for (String l : Files.readAllLines(d.resolve("exports-stress.jsonl"))) { lines++; maxLine = Math.max(maxLine, l.getBytes(StandardCharsets.UTF_8).length + 1); }
        System.out.println("{\"offered\":" + count + ",\"lines\":" + lines + ",\"fileBytes\":" + size + ",\"maxLineBytes\":" + maxLine
            + ",\"droppedCount\":" + r.dropped.get() + ",\"overflowMarker\":" + Files.exists(d.resolve("OVERFLOW-stress.json")) + "}");
    }

    /** Offline decoder: {@code decode <hex>} or {@code decode --file <path>} prints the record tail JSON. */
    public static void main(String[] args) throws Exception {
        if (args.length >= 2 && args[0].equals("decode")) {
            byte[] data = args[1].equals("--file")
                ? Files.readAllBytes(Path.of(args[2]))
                : HexFormat.of().parseHex(args[1]);
            System.out.println("{" + decodeBody(data, false));
        } else if (args.length == 1 && args[0].equals("check")) {
            CountingTelemetryReporter r = new CountingTelemetryReporter();
            if (!(r instanceof MetricsReporter) || !(r instanceof ClientTelemetryExporterProvider)
                || r.clientTelemetryExporter() == null) throw new IllegalStateException("interfaces not satisfied");
            String tail = decodeBody(new byte[0], true);
            if (!tail.contains("\"parse\":\"ok\"")) throw new IllegalStateException("empty MetricsData did not decode: " + tail);
            System.out.println("check ok kafkaClients=" + AppInfoParser.getVersion()
                + " reporter=" + codeSource(CountingTelemetryReporter.class)
                + " bindings=" + codeSource(MetricsData.class) + " protobuf=" + codeSource(com.google.protobuf.Message.class));
        } else if (args.length == 3 && args[0].equals("stress")) {
            stress(HexFormat.of().parseHex(args[1]), Integer.parseInt(args[2]));
        } else {
            System.err.println("usage: decode <hex> | decode --file <path> | check | stress <hex> <count>");
            System.exit(2);
        }
    }
}
