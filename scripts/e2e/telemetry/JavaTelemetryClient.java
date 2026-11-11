package org.countly.kafkajs.telemetry;

import java.time.Duration;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ArrayList;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryUtils;
import org.apache.kafka.common.telemetry.internals.TelemetryMetricNamingConvention;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsResponse;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
 * arguments without constructing a client. Producer/consumer finals require receiver evidence. Admin
 * close requires completed work, a parent-validated fresh export, public close and cessation evidence.
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
    private abstract static class Role {
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

    /** Producer whose final record remains queued until public close drains it. */
    private static final class ProducerRole extends Role {
        private static final int CLOSE_LINGER_MS = 60000;
        private final KafkaProducer<String, String> client;
        private final String topic;
        private Future<RecordMetadata> closeWork;
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
            // Longer than the 45 s supervisor: only flush or close can release these tiny batches.
            props.put(ProducerConfig.LINGER_MS_CONFIG, CLOSE_LINGER_MS);
            client = new KafkaProducer<>(props);
            try {
                for (KafkaMetric metric : controls()) client.registerMetricForSubscription(metric);
                var seed = client.send(new ProducerRecord<>(topic, "k", "v"));
                client.flush();
                seed.get(3, TimeUnit.SECONDS);
            } catch (Exception failure) {
                try { close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }
        /** @return nothing @throws InterruptedException when interrupted */
        @Override void work() throws InterruptedException { Thread.sleep(50); }
        /** @return assigned UUID @throws TimeoutException if not assigned */
        @Override Uuid id() { return client.clientInstanceId(Duration.ZERO); }
        /** @return nothing @throws RuntimeException on refusal or prematurely completed work */
        @Override void armClose() {
            closeWork = client.send(new ProducerRecord<>(topic, "k", "last"));
            if (closeWork.isDone()) throw new IllegalStateException("Producer close work already completed");
            System.out.println("{\"producerCloseWork\":true,\"pending\":true,\"lingerMs\":" + CLOSE_LINGER_MS + "}");
            System.out.flush();
        }
        /** @return nothing @throws Exception on close failure or undelivered final work */
        @Override public void close() throws Exception {
            client.close(Duration.ofSeconds(3));
            if (closeWork != null) closeWork.get(0, TimeUnit.MILLISECONDS);
        }
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
        @Override public void close() { client.close(CloseOptions.timeout(Duration.ofSeconds(3))); }
    }

    /** Admin kept open for a parent-validated fresh export after completed public metadata work. */
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
        /** @return nothing @throws Exception on metadata failure */
        @Override void armClose() throws Exception { client.listTopics().names().get(3, TimeUnit.SECONDS); }
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
     * Reads exactly one ASCII command through EOF on the main thread, with an interruptible channel.
     * The joined deadline thread owns no input and cannot survive this method. No Kafka internals
     * participate in this protocol. The parent must close stdin after its single command.
     * @param checkpointId expected run-bound checkpoint
     * @param id expected Java UUID
     * @return one bounded command
     * @throws Exception on EOF, partial/duplicate/oversized input or expiry
     */
    static String closeCommand(String checkpointId, String id) throws Exception {
        Thread owner = Thread.currentThread();
        Thread deadline = Thread.ofPlatform().daemon().start(() -> {
            try { Thread.sleep(10000); owner.interrupt(); }
            catch (InterruptedException cancelled) { /* Joined below. */ }
        });
        try (FileChannel input = FileChannel.open(Path.of("/dev/stdin"), StandardOpenOption.READ)) {
            ByteBuffer bytes = ByteBuffer.allocate(513);
            while (input.read(bytes) != -1) {
                if (!bytes.hasRemaining()) throw new IllegalArgumentException("Oversized close command");
            }
            String command = new String(bytes.array(), 0, bytes.position(), StandardCharsets.US_ASCII);
            if (!command.matches("CLOSE [A-Za-z0-9_-]{22} [A-Za-z0-9_-]{22} [123] [1-9][0-9]{0,9} [0-9a-f]{64}\\n"))
                throw new IllegalArgumentException("Missing, partial or duplicate close command");
            String[] fields = command.strip().split(" ");
            if (!fields[1].equals(checkpointId) || !fields[2].equals(id))
                throw new IllegalArgumentException("Foreign close checkpoint or UUID");
            return command;
        } finally {
            deadline.interrupt();
            boolean interrupted = Thread.interrupted();
            deadline.join();
            if (interrupted) throw new IllegalStateException("Close command deadline expired");
        }
    }

    /**
     * Publishes completed admin work and waits for a checkpoint/UUID-bound parent release.
     * @param options assigned role identity
     * @param role owned admin
     * @param id stable Java UUID
     * @throws Exception on work, command or identity failure; the caller still closes the client
     */
    private static void checkpoint(Options options, Role role, Uuid id) throws Exception {
        long started = System.currentTimeMillis();
        role.armClose();
        long completed = System.currentTimeMillis();
        String checkpointId = Uuid.randomUuid().toString();
        System.out.println("{\"closeReady\":true,\"role\":\"admin\",\"clientId\":\"" + options.clientId()
            + "\",\"clientInstanceId\":\"" + id + "\",\"checkpointId\":\"" + checkpointId
            + "\",\"epochMs\":" + System.currentTimeMillis() + ",\"operation\":{\"api\":\"listTopics\",\"success\":true,\"startedEpochMs\":"
            + started + ",\"completedEpochMs\":" + completed + "}}");
        System.out.flush();
        String[] command = closeCommand(checkpointId, id.toString()).strip().split(" ");
        if (!command[1].equals(checkpointId) || !command[2].equals(id.toString()))
            throw new IllegalArgumentException("Foreign close checkpoint or UUID");
        System.out.println("{\"closeReleased\":true,\"role\":\"admin\",\"clientId\":\"" + options.clientId()
            + "\",\"clientInstanceId\":\"" + id + "\",\"checkpointId\":\"" + checkpointId
            + "\",\"node\":\"" + command[3] + "\",\"seq\":" + command[4] + ",\"dataSha256\":\"" + command[5]
            + "\",\"epochMs\":" + System.currentTimeMillis() + "}");
        System.out.flush();
    }

    /**
     * Builds an actual pinned reporter inside enclosing Metrics, without a networked Admin.
     * @param reporter actual client reporter
     * @return enclosing registry matching the admin reporter-close path
     */
    private static Metrics controlMetrics(ClientTelemetryReporter reporter) {
        reporter.configure(Map.of("client.id", "offline-admin", "request.timeout.ms", 3000));
        Metrics metrics = new Metrics(new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext("kafka.admin.client"));
        Sensor connections = metrics.sensor("connections-created");
        connections.add(new Meter(metrics.metricName("connection-creation-rate", "admin-client-metrics"),
            metrics.metricName("connection-creation-total", "admin-client-metrics")));
        connections.record();
        for (KafkaMetric metric : controls()) reporter.metricChange(metric);
        return metrics;
    }

    /**
     * Supplies a deterministic successful subscription to the real sender state machine.
     * @param reporter actual pinned reporter
     * @param requested narrow prefixes
     * @throws Exception when a control fails
     */
    private static void controlSubscribe(ClientTelemetryReporter reporter, List<String> requested) throws Exception {
        if (reporter.telemetrySender().createRequest().isEmpty()) throw new IllegalStateException("Missing Get");
        reporter.telemetrySender().handleResponse(new GetTelemetrySubscriptionsResponse(
            new GetTelemetrySubscriptionsResponseData().setClientInstanceId(Uuid.randomUuid())
                .setSubscriptionId(1).setPushIntervalMs(1000).setTelemetryMaxBytes(4096)
                .setDeltaTemporality(true).setAcceptedCompressionTypes(List.of((byte) 0))
                .setRequestedMetrics(requested)));
    }

    /**
     * Exercises actual upstream naming, registry, collector, selection, emitter and sender states.
     * These controls are not live final evidence or a public shutdown guarantee.
     * @throws Exception on any changed naming/selection or close-state result
     */
    private static void controlsCheck() throws Exception {
        String javaName = "org.apache.kafka.admin.client.connection.creation.total";
        String jsName = "org.apache.kafka.admin.connection.creation.total";
        String actual = TelemetryMetricNamingConvention.getClientTelemetryMetricNamingStrategy("org.apache.kafka")
            .metricKey(new MetricName("connection-creation-total", "admin-client-metrics", "", Map.of())).name();
        if (!actual.equals(javaName)) throw new IllegalStateException("Wrong Java admin name");
        for (boolean corrected : List.of(false, true)) {
            ClientTelemetryReporter reporter = new ClientTelemetryReporter(Time.SYSTEM);
            Metrics metrics = controlMetrics(reporter);
            try {
                List<String> requested = new ArrayList<>(List.of(jsName, "org.apache.kafka.tf1.custom."));
                if (corrected) requested.add(javaName);
                controlSubscribe(reporter, requested);
                PushTelemetryRequest push = (PushTelemetryRequest) reporter.telemetrySender().createRequest().orElseThrow().build();
                var decoded = ClientTelemetryUtils.deserializeMetricsData(push.data().metrics());
                int count = decoded.getResourceMetricsCount();
                if (count != (corrected ? 4 : 3)) throw new IllegalStateException("Wrong selected metric count");
                if (!metrics.metrics().containsKey(new MetricName("connection-creation-total", "admin-client-metrics", "", Map.of())))
                    throw new IllegalStateException("Counter not registered");
            } finally { metrics.close(); }
        }
        for (boolean adminOrder : List.of(false, true)) {
            ClientTelemetryReporter reporter = new ClientTelemetryReporter(Time.SYSTEM);
            Metrics metrics = controlMetrics(reporter);
            try {
                controlSubscribe(reporter, List.of(javaName, "org.apache.kafka.tf1.custom."));
                reporter.initiateClose();
                if (adminOrder) metrics.close();
                var request = reporter.telemetrySender().createRequest();
                if (adminOrder) {
                    if (request.isPresent() || reporter.telemetrySender().timeToNextUpdate(0) != Long.MAX_VALUE)
                        throw new IllegalStateException("Admin reporter not terminated");
                } else if (!(request.orElseThrow().build() instanceof PushTelemetryRequest push) || !push.data().terminating()) {
                    throw new IllegalStateException("Missing state-machine terminating Push");
                }
            } finally { metrics.close(); }
        }
        ClientTelemetryReporter reporter = new ClientTelemetryReporter(Time.SYSTEM);
        Metrics metrics = controlMetrics(reporter);
        try {
            reporter.initiateClose();
            metrics.close();
            if (reporter.telemetrySender().createRequest().isPresent()) throw new IllegalStateException("Push without subscription");
        } finally { metrics.close(); }
        System.out.println("{\"offlineControls\":true,\"failedPrefixMetrics\":3,\"correctedPrefixMetrics\":4,\"senderFinal\":true,\"adminOrderTerminated\":true,\"noSubscriptionPush\":false,\"network\":false}");
    }

    /**
     * Runs one role, or checks its arguments without opening any network connection.
     * @param args legacy producer arguments or explicit role arguments
     * @throws Exception on invalid arguments, missing UUID, foreground or close failure
     */
    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("--controls")) { controlsCheck(); return; }
        if (args.length == 3 && args[0].equals("--command-check")) {
            System.out.print(closeCommand(args[1], args[2])); return;
        }
        Options options = Options.parse(args);
        if (options.check()) {
            System.out.println("{\"ready\":true,\"role\":\"" + options.role() + "\",\"network\":false}");
            return;
        }
        System.out.println("{\"jdk\":\"" + System.getProperty("java.runtime.version") + "\",\"vendor\":\"" + System.getProperty("java.vendor") + "\"}");
        Role role = switch (options.role()) {
            case "producer" -> new ProducerRole(options);
            case "consumer" -> new ConsumerRole(options);
            case "admin" -> new AdminRole(options);
            default -> throw new IllegalArgumentException("Unsupported client role");
        };
        try {
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
            if (options.role().equals("admin")) checkpoint(options, role, id);
            else role.armClose();
        } finally {
            long closeStartedMs = System.currentTimeMillis();
            System.out.println("{\"role\":\"" + options.role() + "\",\"closing\":true,\"requestedCloseMs\":3000,\"epochMs\":" + closeStartedMs + "}");
            System.out.flush();
            long closeStartedNs = System.nanoTime();
            role.close();
            System.out.println("{\"role\":\"" + options.role() + "\",\"closed\":true,\"epochMs\":" + System.currentTimeMillis()
                + ",\"closeElapsedNs\":\"" + (System.nanoTime() - closeStartedNs) + "\"}");
            System.out.flush();
        }
    }
}
