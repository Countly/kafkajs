package org.countly.kafkajs.telemetry;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

/**
 * Deterministic Java telemetry client for the TF1 fixture (KIP-714 with the KIP-1076 registration methods). It starts a
 * producer with {@code enable.metrics.push=true}, registers three application metrics with known values (a double gauge
 * 42.5, a long gauge 4242 and a cumulative sum recorded once as 7.0), sends one record so the connection exists, prints
 * the client instance id the Java client itself obtained, runs for the given time and then closes, which sends the
 * terminating push (one record is left in flight at close, because an idle Java sender skips the terminating push).
 * Every push carries the gauges, so no push is empty.
 *
 * <p>Usage: {@code JavaTelemetryClient <bootstrap> <clientId> <runMs>}. Prints one JSON line:
 * {@code {"clientInstanceId":"...","closed":true}} (the id is Kafka's base64url form, as in the sink).
 */
public class JavaTelemetryClient {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, args[1]);
        props.put(ProducerConfig.ENABLE_METRICS_PUSH_CONFIG, "true");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        long runMs = Long.parseLong(args[2]);
        MetricConfig config = new MetricConfig();
        Object lock = new Object();
        Time time = Time.SYSTEM;

        CumulativeSum sum = new CumulativeSum();
        sum.record(config, 7.0, time.milliseconds());
        KafkaMetric[] metrics = {
            new KafkaMetric(lock, new MetricName("gauge.double", "tf1.custom", "known double gauge", Map.of()), (Gauge<Double>) (c, now) -> 42.5, config, time),
            new KafkaMetric(lock, new MetricName("gauge.long", "tf1.custom", "known long gauge", Map.of()), (Gauge<Long>) (c, now) -> 4242L, config, time),
            new KafkaMetric(lock, new MetricName("sum.total", "tf1.custom", "known cumulative sum", Map.of()), sum, config, time),
        };
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (KafkaMetric m : metrics) producer.registerMetricForSubscription(m);
            producer.send(new ProducerRecord<>("p9f-tf1-java", "k", "v")).get();
            String id = String.valueOf(producer.clientInstanceId(Duration.ofSeconds(15)));
            Thread.sleep(runMs);
            System.out.println("{\"clientInstanceId\":\"" + id + "\",\"closed\":false}");
            // The Java sender only creates the terminating push while it still polls; an idle close() skips it, so
            // leave one produce request in flight when close() starts.
            producer.send(new ProducerRecord<>("p9f-tf1-java", "k", "last"));
        }
        System.out.println("{\"closed\":true}");
    }
}
