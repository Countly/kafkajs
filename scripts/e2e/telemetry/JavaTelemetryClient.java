package org.countly.kafkajs.telemetry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;

/**
 * Runs one bounded Java producer, classic consumer or admin telemetry role. The three-argument
 * producer form preserves the prepared fixture's active-close control. A readiness check validates
 * arguments without constructing a client. Receiver evidence, rather than process exit, proves finals.
 */
public final class JavaTelemetryClient {
    /** No instances are required by the command-line entry point. */
    private JavaTelemetryClient() { }

    /** Immutable, validated command arguments; no client is constructed by parsing. */
    private record Options(String role, String bootstrap, String clientId, String topic,
                           String group, long runMs, boolean check) {
        /**
         * Parses the legacy producer form or the explicit role form, optionally prefixed by --check.
         * @param args literal CLI arguments
         * @return validated owned values
         * @throws IllegalArgumentException for unsupported roles, arity, endpoints or durations
         */
        static Options parse(String[] args) {
            boolean check = args.length > 0 && args[0].equals("--check");
            int start = check ? 1 : 0;
            int length = args.length - start;
            String role, bootstrap, clientId, topic, group, duration;
            if (length == 3) {
                role = "producer";
                bootstrap = args[start];
                clientId = args[start + 1];
                topic = "p9f-tf1-java";
                group = "telemetry-java-control";
                duration = args[start + 2];
            } else if (length == 7) {
                role = args[start];
                bootstrap = args[start + 1];
                clientId = args[start + 2];
                topic = args[start + 3];
                group = args[start + 4];
                duration = args[start + 5];
                if (!args[start + 6].equals("active-close"))
                    throw new IllegalArgumentException("Active close control required");
            } else {
                throw new IllegalArgumentException("Expected producer control or explicit role arguments");
            }
            if (!List.of("producer", "consumer", "admin").contains(role))
                throw new IllegalArgumentException("Unsupported client role");
            if (!(length == 3 && bootstrap.matches("kafka[123]:29092")) && !bootstrap.matches("(?:localhost|127\\.0\\.0\\.1):190(?:92|95|98)(?:,(?:localhost|127\\.0\\.0\\.1):190(?:92|95|98))*"))
                throw new IllegalArgumentException("Expected configured host endpoints");
            for (String value : List.of(clientId, topic, group))
                if (!value.matches("[A-Za-z0-9_.-]{1,95}"))
                    throw new IllegalArgumentException("Invalid client, topic or group identity");
            if (!duration.matches("[0-9]{1,5}"))
                throw new IllegalArgumentException("Invalid observation duration");
            long runMs = Long.parseLong(duration);
            if (runMs < 3000 || runMs > 15000)
                throw new IllegalArgumentException("Observation duration outside 3000..15000 ms");
            return new Options(role, bootstrap, clientId, topic, group, runMs, check);
        }
    }

    /** Common role contract. All KafkaConsumer operations stay on the caller's single thread. */
    private abstract static class Role implements AutoCloseable {
        /**
         * Executes one bounded foreground operation or polls the consumer network.
         * @return nothing
         * @throws Exception on foreground failure; telemetry does not replace it
         */
        abstract void work() throws Exception;
        /**
         * Reads an already assigned instance ID without parking the consumer's poll thread.
         * @return the Java client's assigned UUID
         * @throws TimeoutException when the ID is not assigned yet
         */
        abstract Uuid id();
        /**
         * Issues fresh role work immediately before close; it does not claim a final was exported.
         * @return nothing
         * @throws Exception on failure to issue the foreground work
         */
        abstract void armClose() throws Exception;
        /**
         * Closes this role through its public API with the original three-second bound.
         * @return nothing
         * @throws Exception on close failure
         */
        public abstract void close() throws Exception;
    }

    /** Producer with the prepared send-immediately-before-close control. */
    private static final class ProducerRole extends Role {
        private final KafkaProducer<String, String> client;
        private final String topic;
        /**
         * Opens the producer and registers the known metric controls.
         * @param options validated CLI values
         * @throws Exception on setup failure; closes a partially initialized client
         */
        ProducerRole(Options options) throws Exception {
            topic = options.topic();
            Properties props = properties(options);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            client = new KafkaProducer<>(props);
            try {
                for (KafkaMetric metric : controls()) client.registerMetricForSubscription(metric);
                client.send(new ProducerRecord<>(topic, "k", "v")).get(3, TimeUnit.SECONDS);
            } catch (Exception failure) {
                try { close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }
        /** @return nothing @throws InterruptedException when interrupted */
        @Override void work() throws InterruptedException { Thread.sleep(50); }
        /** @return assigned UUID @throws TimeoutException if not assigned */
        @Override Uuid id() { return client.clientInstanceId(Duration.ZERO); }
        /** @return nothing @throws RuntimeException on send refusal */
        @Override void armClose() { client.send(new ProducerRecord<>(topic, "k", "last")); }
        /** @return nothing @throws RuntimeException on close failure */
        @Override public void close() { client.close(Duration.ofSeconds(3)); }
    }

    /** Classic consumer with an established fetch session and an asynchronous commit at close. */
    private static final class ConsumerRole extends Role {
        private final KafkaConsumer<String, String> client;
        /**
         * Constructs and subscribes the consumer; the main thread subsequently polls it.
         * @param options validated CLI values
         * @throws Exception on setup failure; closes a partially initialized client
         */
        ConsumerRole(Options options) throws Exception {
            Properties props = properties(options);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, options.group());
            props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
            props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
            props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");
            client = new KafkaConsumer<>(props);
            try {
                for (KafkaMetric metric : controls()) client.registerMetricForSubscription(metric);
                client.subscribe(List.of(options.topic()));
            } catch (Exception failure) {
                try { close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }
        /**
         * Brackets each public poll with group metadata, independently of telemetry resource rows.
         * Kafka's classic coordinator updates the member resource during a successful JoinGroup.
         * @return nothing
         * @throws RuntimeException on poll or commit failure
         */
        @Override void work() {
            long before = System.currentTimeMillis();
            String beforeMember = client.groupMetadata().memberId();
            boolean received = !client.poll(Duration.ofMillis(100)).isEmpty();
            System.out.println("{\"membership\":true,\"fromEpochMs\":" + before
                + ",\"epochMs\":" + System.currentTimeMillis()
                + ",\"beforeMemberId\":\"" + beforeMember
                + "\",\"memberId\":\"" + client.groupMetadata().memberId() + "\"}");
            System.out.flush();
            if (received) client.commitSync(Duration.ofSeconds(3));
        }
        /** @return assigned UUID @throws TimeoutException if not assigned */
        @Override Uuid id() { return client.clientInstanceId(Duration.ZERO); }
        /** @return nothing @throws IllegalStateException without an established assignment */
        @Override void armClose() {
            if (client.assignment().isEmpty()) throw new IllegalStateException("Consumer assignment missing");
            client.commitAsync();
        }
        /** @return nothing @throws RuntimeException on close failure */
        @Override public void close() { client.close(Duration.ofSeconds(3)); }
    }

    /** Admin with a pending external metadata call immediately before close. */
    private static final class AdminRole extends Role {
        private final Admin client;
        /**
         * Constructs the admin and registers the known metric controls.
         * @param options validated CLI values
         * @throws Exception on setup failure; closes a partially initialized client
         */
        AdminRole(Options options) throws Exception {
            client = Admin.create(properties(options));
            try {
                for (KafkaMetric metric : controls()) client.registerMetricForSubscription(metric);
            } catch (Exception failure) {
                try { close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }
        /** @return nothing @throws Exception on metadata failure or interruption */
        @Override void work() throws Exception {
            client.listTopics().names().get(3, TimeUnit.SECONDS);
            Thread.sleep(50);
        }
        /** @return assigned UUID @throws TimeoutException if not assigned */
        @Override Uuid id() { return client.clientInstanceId(Duration.ZERO); }
        /** @return nothing @throws RuntimeException on metadata-call refusal */
        @Override void armClose() { client.listTopics().names(); }
        /** @return nothing @throws RuntimeException on close failure */
        @Override public void close() { client.close(Duration.ofSeconds(3)); }
    }

    /**
     * Builds common bounded plaintext configuration without broker administration.
     * @param options validated CLI values
     * @return owned properties; telemetry is explicitly enabled
     */
    private static Properties properties(Options options) {
        Properties props = new Properties();
        props.put("bootstrap.servers", options.bootstrap());
        props.put("client.id", options.clientId());
        props.put("enable.metrics.push", "true");
        props.put("request.timeout.ms", "3000");
        props.put("default.api.timeout.ms", "5000");
        props.put("max.block.ms", "5000");
        return props;
    }

    /**
     * Creates the prepared double, long and delta-sum controls, unchanged in name and value.
     * @return three independent Kafka metrics; the receiver verifies their exact values
     */
    private static KafkaMetric[] controls() {
        MetricConfig config = new MetricConfig();
        Object lock = new Object();
        Time time = Time.SYSTEM;
        CumulativeSum sum = new CumulativeSum();
        sum.record(config, 7.0, time.milliseconds());
        return new KafkaMetric[] {
            new KafkaMetric(lock, new MetricName("gauge.double", "tf1.custom", "known double gauge", Map.of()),
                (Gauge<Double>) (c, now) -> 42.5, config, time),
            new KafkaMetric(lock, new MetricName("gauge.long", "tf1.custom", "known long gauge", Map.of()),
                (Gauge<Long>) (c, now) -> 4242L, config, time),
            new KafkaMetric(lock, new MetricName("sum.total", "tf1.custom", "known cumulative sum", Map.of()),
                sum, config, time),
        };
    }

    /**
     * Runs one role, or checks its arguments without opening any network connection.
     * @param args legacy producer arguments or explicit role arguments
     * @throws Exception on invalid arguments, missing UUID, foreground or close failure
     */
    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        if (options.check()) {
            System.out.println("{\"ready\":true,\"role\":\"" + options.role() + "\",\"network\":false}");
            return;
        }
        System.out.println("{\"jdk\":\"" + System.getProperty("java.runtime.version") + "\",\"vendor\":\"" + System.getProperty("java.vendor") + "\"}");
        try (Role role = switch (options.role()) {
            case "producer" -> new ProducerRole(options);
            case "consumer" -> new ConsumerRole(options);
            case "admin" -> new AdminRole(options);
            default -> throw new IllegalArgumentException("Unsupported client role");
        }) {
            long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            Uuid id = null;
            while (id == null && System.nanoTime() < limit) {
                role.work();
                try { id = role.id(); } catch (TimeoutException notAssigned) { }
            }
            if (id == null || Uuid.RESERVED.contains(id)) throw new IllegalStateException("Assigned UUID missing");
            System.out.println("{\"role\":\"" + options.role() + "\",\"clientInstanceId\":\"" + id + "\",\"closed\":false}");
            long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.runMs());
            while (System.nanoTime() < end) role.work();
            if (!id.equals(role.id())) throw new IllegalStateException("Instance ID changed");
            System.out.println("{\"role\":\"" + options.role() + "\",\"closing\":true,\"epochMs\":" + System.currentTimeMillis() + "}");
            role.armClose();
        }
        System.out.println("{\"role\":\"" + options.role() + "\",\"closed\":true,\"epochMs\":" + System.currentTimeMillis() + "}");
    }
}
