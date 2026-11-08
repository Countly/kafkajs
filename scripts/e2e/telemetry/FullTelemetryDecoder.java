package org.countly.kafkajs.telemetry;

import com.google.protobuf.InvalidProtocolBufferException;
import org.countly.kafkajs.telemetry.otlp.common.v1.AnyValue;
import org.countly.kafkajs.telemetry.otlp.common.v1.InstrumentationScope;
import org.countly.kafkajs.telemetry.otlp.common.v1.KeyValue;
import org.countly.kafkajs.telemetry.otlp.metrics.v1.Metric;
import org.countly.kafkajs.telemetry.otlp.metrics.v1.MetricsData;
import org.countly.kafkajs.telemetry.otlp.metrics.v1.NumberDataPoint;
import org.countly.kafkajs.telemetry.otlp.metrics.v1.ResourceMetrics;
import org.countly.kafkajs.telemetry.otlp.metrics.v1.ScopeMetrics;
import org.countly.kafkajs.telemetry.otlp.resource.v1.Resource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;

/**
 * Complete offline JSON projection of the retained independent OTLP bindings.
 * Unlike the broker sink projection, it never clips strings, attributes or points.
 * Input is capped at 4096 bytes; the caller supervises the JVM and output bound.
 */
public final class FullTelemetryDecoder {
    /** No instances; this class owns no broker state. */
    private FullTelemetryDecoder() { }

    /**
     * Decodes one bounded raw MetricsData file with the retained protobuf bindings.
     * @param args one absolute raw payload path
     * @throws Exception for invalid arguments, oversized input or a decoding failure
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 1 || !Path.of(args[0]).isAbsolute())
            throw new IllegalArgumentException("Expected one absolute raw payload path");
        Path file = Path.of(args[0]);
        if (Files.size(file) > 4096) throw new IllegalArgumentException("Payload exceeds 4096 bytes");
        byte[] data = Files.readAllBytes(file);
        if (data.length > 4096) throw new IllegalArgumentException("Payload exceeds 4096 bytes");
        System.out.println("{" + decodeBody(data));
    }

    /**
     * Decodes OTLP MetricsData bytes into the record tail (fields after the identity head, closing brace included).
     *
     * @param data the decompressed payload
     * @return the JSON tail, starting with {@code "dataBytes":}
     * @throws Exception for a failed digest or protobuf decode
     */
    static String decodeBody(byte[] data) throws Exception {
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
        StringBuilder res = new StringBuilder("[");
        StringBuilder met = new StringBuilder("[");
        int metricsTotal = 0, metricsKept = 0, pointsTotal = 0;
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
                    if (metricsKept++ > 0) met.append(',');
                    met.append(e);
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
            .append(",\"metricsKept\":").append(metricsKept).append(",\"truncated\":").append(false)
            .append(",\"resources\":").append(res).append(",\"metrics\":").append(met).append('}');
        return tail.toString();
    }

    /**
     * Preserves every attribute's key, type and complete string value in wire order.
     * @param kvs independently parsed protobuf attributes
     * @return their JSON array
     */
    private static String attrs(List<KeyValue> kvs) {
        StringBuilder b = new StringBuilder("[");
        int n = 0;
        for (KeyValue kv : kvs) {
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

    /**
     * Quotes a complete JSON string without changing its value.
     * @param value the protobuf string
     * @return a valid JSON string literal
     */
    private static String q(String value) {
        StringBuilder b = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
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
}
