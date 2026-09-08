import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.GroupListing;
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
 * <p>Arguments: [--group-protocol classic|consumer] [--report-group-type] bootstrapServers
 * groupId topic assignor runForMs. Under {@code --group-protocol consumer} the assignor
 * argument is the SERVER assignor name. {@code --describe-group bootstrapServers groupId}
 * prints one group's type and state without joining.
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
    "org.apache.kafka.clients.admin.Admin",
    "org.apache.kafka.common.GroupType",
    "org.apache.kafka.common.GroupState",
  };

  /**
   * Runs one group member until its deadline, answers the classpath pre-flight, or reports one
   * group's type and exits.
   *
   * @param args {@code [--group-protocol classic|consumer] [--report-group-type]
   *     bootstrapServers groupId topic assignor runForMs}, or {@code --probe}, or
   *     {@code --describe-group bootstrapServers groupId}
   */
  public static void main(String[] args) {
    List<String> flagless = new ArrayList<>();
    String groupProtocol = "classic";
    boolean reportGroupType = false;

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--probe":
          probe();
          return;
        case "--group-protocol":
          groupProtocol = args[++i];
          break;
        case "--report-group-type":
          reportGroupType = true;
          break;
        case "--describe-group":
          // Non-zero when the admin call failed, so a caller can tell "the group is unknown"
          // (a line with empty values) from "the cluster could not be asked" (no line at all).
          if (!printGroupType(args[i + 1], args[i + 2])) {
            System.exit(1);
          }
          return;
        default:
          flagless.add(args[i]);
      }
    }

    if (!"classic".equals(groupProtocol) && !"consumer".equals(groupProtocol)) {
      System.err.println("--group-protocol must be classic or consumer, got " + groupProtocol);
      System.exit(2);
    }

    String bootstrapServers = flagless.get(0);
    String groupId = flagless.get(1);
    String topic = flagless.get(2);
    String assignor = flagless.get(3);
    long runForMs = Long.parseLong(flagless.get(4));

    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // Pinned rather than inherited: `classic` is the 4.3 default but changes in a later release,
    // and KAFKA_CLIENTS_VERSION is env-overridable - a member that silently joined with the
    // other protocol would void every assertion the caller makes.
    // @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
    properties.put("group.protocol", groupProtocol);

    if ("consumer".equals(groupProtocol)) {
      // The coordinator owns both timings under this protocol and ConsumerConfig REJECTS all
      // three classic keys outright, so they are not merely unused here: setting any of them
      // fails construction. The 4th argument is the SERVER assignor name (uniform, range)
      // rather than a client assignor class, because client-side assignors do not exist here.
      // @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerConfig.java
      properties.put("group.remote.assignor", assignor);
    } else {
      properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, assignor);
      properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
      properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
    }

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

      boolean reported = false;
      while (System.currentTimeMillis() < deadline) {
        consumer.poll(Duration.ofMillis(500));
        if (reportGroupType && !reported && !owned.isEmpty()) {
          reported = printGroupType(bootstrapServers, groupId);
        }
      }
    }

    System.out.println("{\"event\":\"done\"}");
    System.out.flush();
  }

  /**
   * Prints one group's type and state as JSON, read through the Java admin client, and closes
   * the client. Both values are optional on the wire: a broker that predates the field, or a
   * group the coordinator does not know, yields the empty string rather than a crash.
   *
   * <p>An admin client that could not answer at all prints NOTHING on stdout and reports the
   * exception on stderr, so a failed measurement can never be read as a group whose type is
   * empty - the two would otherwise produce the same line.
   *
   * @param bootstrapServers the cluster to ask
   * @param groupId the group to look for
   * @return {@code true} when a line was printed, {@code false} when the admin call threw
   * @see <a href="https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/admin/GroupListing.java">GroupListing.java</a>
   */
  private static boolean printGroupType(String bootstrapServers, String groupId) {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    String type = "";
    String state = "";
    try (Admin admin = Admin.create(properties)) {
      for (GroupListing listing : admin.listGroups().all().get()) {
        if (!listing.groupId().equals(groupId)) {
          continue;
        }
        type = listing.type().map(Object::toString).orElse("");
        state = listing.groupState().map(Object::toString).orElse("");
      }
    } catch (Exception e) {
      System.err.println("could not read the group type: " + e);
      return false;
    }

    System.out.println(
        "{\"event\":\"group-type\",\"groupId\":\""
            + groupId
            + "\",\"groupType\":\""
            + type
            + "\",\"groupState\":\""
            + state
            + "\"}");
    System.out.flush();
    return true;
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
