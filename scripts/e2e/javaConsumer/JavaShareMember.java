import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;

/**
 * Development-only explicit-acknowledgement share member for mixed Java/JavaScript scenarios.
 *
 * <p>Arguments: {@code --role share-member [--halt-after N] [--report-polls] [--close-file path]
 * bootstrapServers groupId topic runForMs}, or {@code --role share-member --probe}. Stdout contains record JSON
 * lines and a final {@code {"event":"done"}} after successful close. Offsets are decimal strings
 * and a missing delivery count is JSON null. A deliberate halt prints its last record before
 * halting, without acknowledging that record, committing that poll, closing, or running shutdown
 * hooks.
 *
 * <p>With {@code --report-polls} every nonempty poll is first flushed whole as a {@code poll}
 * line, before any record line, acknowledgement or halt, and a {@code committed} line follows
 * only once its commit reported no partition error.
 * The optional close file requests normal close between polls; runForMs remains the lifetime cap.
 */
public final class JavaShareMember {
  /** Prevents construction of this command-line entry point. */
  private JavaShareMember() {}

  /**
   * Validates arguments before constructing a client, or performs the classpath-only probe.
   *
   * @param args role, optional halt count, and four positional arguments, or role and probe
   */
  public static void main(String[] args) {
    try {
      List<String> positional = new ArrayList<>();
      String role = null;
      boolean probe = false;
      boolean reportPolls = false;
      Path closeFile = null;
      int haltAfter = 0;
      for (int i = 0; i < args.length; i++) {
        switch (args[i]) {
          case "--role":
            role = args[++i];
            break;
          case "--halt-after":
            haltAfter = Integer.parseInt(args[++i]);
            if (haltAfter <= 0) throw new IllegalArgumentException();
            break;
          case "--report-polls":
            reportPolls = true;
            break;
          case "--close-file":
            closeFile = Path.of(args[++i]);
            break;
          case "--probe":
            probe = true;
            break;
          default:
            if (args[i].startsWith("--")) throw new IllegalArgumentException();
            positional.add(args[i]);
        }
      }
      if (!"share-member".equals(role)) throw new IllegalArgumentException();
      if (probe) {
        if (!positional.isEmpty() || haltAfter != 0 || reportPolls || closeFile != null) {
          throw new IllegalArgumentException();
        }
        probe();
        return;
      }
      if (positional.size() != 4) throw new IllegalArgumentException();
      long runForMs = Long.parseLong(positional.get(3));
      if (runForMs <= 0 || runForMs > Integer.MAX_VALUE) throw new IllegalArgumentException();
      if (haltAfter > 0 && closeFile != null) throw new IllegalArgumentException();
      consume(positional.get(0), positional.get(1), positional.get(2), runForMs,
          haltAfter, reportPolls, closeFile);
    } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
      System.err.println("Usage: --role share-member [--halt-after N] [--report-polls] [--close-file path] bootstrapServers groupId topic runForMs; or --role share-member --probe");
      System.exit(2);
    }
  }

  /**
   * Polls with explicit ACCEPT decisions and checks every reported commit failure.
   *
   * @param bootstrapServers broker bootstrap list
   * @param groupId share group identity, also suffixed to the Java client id
   * @param topic the single topic to subscribe to
   * @param runForMs positive polling lifetime in milliseconds, bounded by Integer.MAX_VALUE
   * @param haltAfter positive number of emitted records before halt, or zero to finish normally
   * @param reportPolls whether to flush the complete poll before any decision, then its commit receipt
   * @param closeFile optional parent request to close normally after the current poll is committed
   */
  private static void consume(String bootstrapServers, String groupId, String topic,
                              long runForMs, int haltAfter, boolean reportPolls, Path closeFile) {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", bootstrapServers);
    properties.put("group.id", groupId);
    properties.put("client.id", "java-share-" + groupId);
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("share.acknowledgement.mode", "explicit");
    KafkaShareConsumer<byte[], byte[]> consumer = null;
    try {
      consumer = new KafkaShareConsumer<>(properties);
      consumer.subscribe(Collections.singletonList(topic));
      long started = System.nanoTime();
      long lifetime = Duration.ofMillis(runForMs).toNanos();
      int count = 0;
      long pollId = 0;
      while (System.nanoTime() - started < lifetime
          && (closeFile == null || !Files.exists(closeFile))) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        if (!records.isEmpty() && reportPolls) printPoll(++pollId, records, consumer.acquisitionLockTimeoutMs());
        for (ConsumerRecord<byte[], byte[]> record : records) {
          printRecord(record);
          count++;
          if (haltAfter > 0 && count == haltAfter) Runtime.getRuntime().halt(137);
          consumer.acknowledge(record, AcknowledgeType.ACCEPT);
        }
        if (!records.isEmpty()) {
          Optional<Integer> lockTimeoutMs = consumer.acquisitionLockTimeoutMs();
          Duration timeout = Duration.ofMillis(Math.max(1, Math.min(5000, lockTimeoutMs.orElse(5000))));
          Map<TopicIdPartition, Optional<KafkaException>> results = consumer.commitSync(timeout);
          for (Optional<KafkaException> error : results.values()) {
            if (error.isPresent()) throw error.get();
          }
          if (reportPolls) print("{\"event\":\"committed\",\"pollId\":" + quote(Long.toString(pollId)) + "}");
        }
      }
      consumer.close(Duration.ofSeconds(5));
      consumer = null;
      print("{\"event\":\"done\"}");
    } catch (Exception e) {
      if (consumer != null) {
        try {
          consumer.close(Duration.ofSeconds(5));
        } catch (Exception closeError) {
          e.addSuppressed(closeError);
        }
      }
      print("{\"event\":\"error\",\"error\":" + quote(e.getClass().getSimpleName())
          + ",\"message\":" + quote(e.getMessage()) + "}");
      System.exit(4);
    }
  }

  /**
   * Emits one observation before any decision is sent; it does not claim broker acceptance.
   *
   * @param record a delivered record whose exact offset and optional delivery count are reported
   */
  private static void printRecord(ConsumerRecord<byte[], byte[]> record) {
    Optional<Short> deliveryCount = record.deliveryCount();
    print("{\"event\":\"record\",\"topic\":" + quote(record.topic())
        + ",\"partition\":" + record.partition() + ",\"offset\":" + quote(Long.toString(record.offset()))
        + ",\"deliveryCount\":" + (deliveryCount.isPresent() ? deliveryCount.get().toString() : "null") + "}");
  }

  /**
   * Flushes every record of the returned poll before any record is acknowledged or a halt occurs.
   * A committed receipt is emitted separately only after commitSync reports no partition error.
   *
   * @param pollId monotonic nonempty-poll identity, serialized as an exact decimal string
   * @param records complete returned poll, including records not subsequently printed by a halt
   * @param lockTimeoutMs observed acquisition lock, or absent when the client has not learnt it
   */
  private static void printPoll(long pollId, ConsumerRecords<byte[], byte[]> records,
                                Optional<Integer> lockTimeoutMs) {
    StringBuilder json = new StringBuilder("{\"event\":\"poll\",\"pollId\":")
        .append(quote(Long.toString(pollId))).append(",\"lockTimeoutMs\":")
        .append(lockTimeoutMs.isPresent() ? lockTimeoutMs.get().toString() : "null")
        .append(",\"records\":[");
    boolean first = true;
    for (ConsumerRecord<byte[], byte[]> record : records) {
      if (!first) json.append(',');
      first = false;
      Optional<Short> count = record.deliveryCount();
      json.append("{\"topic\":").append(quote(record.topic()))
          .append(",\"partition\":").append(record.partition())
          .append(",\"offset\":").append(quote(Long.toString(record.offset())))
          .append(",\"keyHex\":").append(quote(hex(record.key())))
          .append(",\"valueHex\":").append(quote(hex(record.value())))
          .append(",\"deliveryCount\":").append(count.isPresent() ? count.get().toString() : "null")
          .append('}');
    }
    print(json.append("]}").toString());
  }

  /**
   * Encodes nullable bytes without changing their identity or expanding signed bytes incorrectly.
   * @param value original key or value bytes
   * @return lowercase hexadecimal, or null for an absent value
   */
  private static String hex(byte[] value) {
    if (value == null) return null;
    char[] digits = "0123456789abcdef".toCharArray();
    char[] result = new char[value.length * 2];
    for (int i = 0; i < value.length; i++) {
      int b = value[i] & 0xff;
      result[2 * i] = digits[b >>> 4];
      result[2 * i + 1] = digits[b & 0xf];
    }
    return new String(result);
  }

  /** Loads required classes eagerly without constructing a client or opening a connection. */
  private static void probe() {
    for (String name : new String[] {
        "org.apache.kafka.clients.consumer.KafkaShareConsumer",
        "org.apache.kafka.clients.consumer.AcknowledgeType",
        "org.apache.kafka.common.serialization.ByteArrayDeserializer"
    }) {
      try {
        Class.forName(name, true, JavaShareMember.class.getClassLoader());
      } catch (ClassNotFoundException | LinkageError e) {
        System.err.println("NoClassDefFoundError while loading " + name + ": " + e);
        System.exit(1);
      }
    }
    print("{\"event\":\"probe-ok\"}");
  }

  /**
   * Encodes JSON string syntax, including every control character.
   * @param value a nullable string
   * @return its JSON literal
   */
  private static String quote(String value) {
    if (value == null) return "null";
    StringBuilder out = new StringBuilder("\"");
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == '"' || c == '\\') out.append('\\').append(c);
      else if (c < 32) out.append(String.format("\\u%04x", (int) c));
      else out.append(c);
    }
    return out.append('"').toString();
  }

  /**
   * Flushes a complete JSON line before a possible abrupt halt.
   * @param json one complete JSON value
   */
  private static void print(String json) {
    System.out.println(json);
    System.out.flush();
  }
}
