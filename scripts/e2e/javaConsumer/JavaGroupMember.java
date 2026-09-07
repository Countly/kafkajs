import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * A single Java group member, used to prove that this client and the Java client can share a
 * group under the same assignor.
 *
 * <p>Development-only, driven by {@code scripts/e2e/javaConsumer/run.sh} from a live test. It
 * prints one JSON line to stdout whenever its assignment changes:
 * {@code {"event":"assignment","memberId":"...","partitions":{"topic":[0,1]}}}
 * and exits after the deadline its caller gives it, so a hung child fails one row rather than the
 * whole suite. The printed set is always this member's FULL assignment, not the round's delta:
 * cooperative callbacks report deltas, and the JavaScript side compares full sets.
 *
 * <p>Arguments: bootstrapServers groupId topic assignorClassName runForMs
 *
 * @see <a href="https://kafka.apache.org/documentation/#consumerconfigs">Consumer configuration</a>
 */
public final class JavaGroupMember {

  /**
   * Everything a real run resolves off the classpath. The JVM loads classes lazily, so a hole in
   * the classpath is invisible until the moment it is used; the pre-flight loads all of them,
   * static initializers included, so a missing dependency is reported where the caller can still
   * turn it into a skip.
   */
  private static final String[] PROBE_CLASSES = {
    "org.apache.kafka.clients.consumer.KafkaConsumer",
    "org.apache.kafka.clients.consumer.RangeAssignor",
    "org.apache.kafka.clients.consumer.StickyAssignor",
    "org.apache.kafka.clients.consumer.CooperativeStickyAssignor",
  };

  /**
   * Runs one group member until its deadline, or answers the classpath pre-flight.
   *
   * @param args bootstrapServers groupId topic assignorClassName runForMs, or the single
   *     argument {@code --probe}
   */
  public static void main(String[] args) {
    // FIRST, before any positional read: the pre-flight wants to know only whether this run could
    // work at all. `args[4]` below would throw for a one-argument invocation.
    if (args.length == 1 && "--probe".equals(args[0])) {
      probe();
      return;
    }

    String bootstrapServers = args[0];
    String groupId = args[1];
    String topic = args[2];
    String assignor = args[3];
    long runForMs = Long.parseLong(args[4]);

    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, assignor);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
    properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
    // Pinned, not inherited: `classic` is the 4.3 default but changes in a later release, and
    // KAFKA_CLIENTS_VERSION is env-overridable - a member that silently joined with the
    // consumer protocol would void every assertion in this scenario.
    // @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
    properties.put("group.protocol", "classic");

    long deadline = System.currentTimeMillis() + runForMs;

    // The member's own view of what it holds, maintained across every rebalance callback,
    // because cooperative callbacks report deltas.
    final Set<TopicPartition> owned = new LinkedHashSet<>();

    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
      consumer.subscribe(
          Collections.singletonList(topic),
          new ConsumerRebalanceListener() {
            // Under cooperative-sticky these callbacks carry a DELTA, not the full set: only
            // the partitions gained or given up in that round. The JS side compares full
            // assignments, so the component keeps its own owned set and prints the FULL set
            // after every rebalance.
            @Override
            public void onPartitionsRevoked(java.util.Collection<TopicPartition> partitions) {
              owned.removeAll(partitions);
              print("revoked", consumer.groupMetadata().memberId(), owned);
            }

            @Override
            public void onPartitionsAssigned(java.util.Collection<TopicPartition> partitions) {
              owned.addAll(partitions);
              print("assignment", consumer.groupMetadata().memberId(), owned);
            }

            @Override
            public void onPartitionsLost(java.util.Collection<TopicPartition> partitions) {
              owned.removeAll(partitions);
              print("lost", consumer.groupMetadata().memberId(), owned);
            }
          });

      while (System.currentTimeMillis() < deadline) {
        consumer.poll(Duration.ofMillis(500));
      }
    }

    System.out.println("{\"event\":\"done\"}");
    System.out.flush();
  }

  /**
   * Loads every class a real run needs, running their static initializers, and exits non-zero
   * naming the first one that could not be resolved. The consumer and the three assignors all
   * hold static {@code LoggerFactory.getLogger(...)} fields, so a classpath missing slf4j fails
   * here rather than silently producing a member that never joins.
   */
  private static void probe() {
    for (String className : PROBE_CLASSES) {
      try {
        Class.forName(className, true, JavaGroupMember.class.getClassLoader());
      } catch (ClassNotFoundException | LinkageError e) {
        System.err.println("NoClassDefFoundError while loading " + className + ": " + e);
        System.exit(1);
      }
    }

    System.out.println("{\"event\":\"probe-ok\"}");
    System.out.flush();
  }

  /**
   * Prints one member-assignment line as JSON, partitions grouped by topic and sorted.
   *
   * @param event {@code assignment}, {@code revoked} or {@code lost}
   * @param memberId the coordinator-assigned member id at the time of the callback
   * @param partitions the member's FULL owned set, never the round's delta
   */
  private static void print(
      String event, String memberId, java.util.Collection<TopicPartition> partitions) {
    Map<String, List<Integer>> byTopic = new TreeMap<>();
    for (TopicPartition tp : partitions) {
      byTopic.computeIfAbsent(tp.topic(), key -> new ArrayList<>()).add(tp.partition());
    }

    StringBuilder json =
        new StringBuilder("{\"event\":\"")
            .append(event)
            .append("\",\"memberId\":\"")
            .append(memberId)
            .append("\",\"partitions\":{");
    int i = 0;
    for (Map.Entry<String, List<Integer>> entry : byTopic.entrySet()) {
      Collections.sort(entry.getValue());
      json.append("\"").append(entry.getKey()).append("\":[");
      for (int j = 0; j < entry.getValue().size(); j++) {
        json.append(entry.getValue().get(j)).append(j + 1 < entry.getValue().size() ? "," : "");
      }
      json.append("]").append(++i < byTopic.size() ? "," : "");
    }
    json.append("}}");

    System.out.println(json);
    System.out.flush();
  }
}
