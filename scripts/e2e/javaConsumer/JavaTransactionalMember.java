import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * A Java transactional producer, or a read_committed consumer, used to prove that this client's
 * transactions interoperate with the Java client's: fencing in both directions, one transactional
 * id passed between the two clients, recovery from a producer that crashed mid-transaction, and
 * reads across the two.
 *
 * <p>Development-only, driven by {@code scripts/e2e/javaConsumer/run.sh --role ...} from a live
 * test. It prints one JSON line per step on stdout and exits when its work or its deadline is
 * done, so a hung child fails one case rather than the whole suite.
 *
 * <p>Arguments: {@code --role transactional-producer [--abort-every N] [--hold-open-ms MS]
 * [--crash-after-produce] bootstrapServers transactionalId topic transactions
 * recordsPerTransaction}, or {@code --role read-committed-consumer bootstrapServers groupId topic
 * runForMs}, or {@code --role <either> --probe}. Every record's key and value is
 * {@code <transactionalId>-<transaction>-<index>}.
 *
 * @see <a href="https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html">KafkaProducer</a>
 */
public final class JavaTransactionalMember {

  /** Exit code when another owner of the transactional id fenced this producer. */
  private static final int EXIT_FENCED = 3;

  /** Exit code when any other client error ended the run. */
  private static final int EXIT_ERROR = 4;

  /** Exit code of the deliberate crash between Produce and EndTxn, the shell's SIGKILL code. */
  private static final int EXIT_CRASH = 137;

  /** Everything a real run resolves off the classpath, loaded eagerly by {@code --probe}. */
  private static final String[] PROBE_CLASSES = {
    "org.apache.kafka.clients.producer.KafkaProducer",
    "org.apache.kafka.clients.consumer.KafkaConsumer",
    "org.apache.kafka.common.serialization.StringSerializer",
    "org.apache.kafka.common.serialization.StringDeserializer",
  };

  private JavaTransactionalMember() {}

  /**
   * Runs one role until its work or its deadline is done, or answers the classpath pre-flight.
   *
   * @param args see the class documentation
   */
  public static void main(String[] args) {
    String role = null;
    String compression = "none";
    boolean probe = false;
    int abortEvery = 0;
    long holdOpenMs = 0;
    boolean crashAfterProduce = false;
    List<String> positional = new ArrayList<>();

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--role":
          role = args[++i];
          break;
        case "--compression":
          compression = args[++i];
          break;
        case "--probe":
          probe = true;
          break;
        case "--abort-every":
          abortEvery = Integer.parseInt(args[++i]);
          break;
        case "--hold-open-ms":
          holdOpenMs = Long.parseLong(args[++i]);
          break;
        case "--crash-after-produce":
          crashAfterProduce = true;
          break;
        default:
          positional.add(args[i]);
      }
    }

    if (probe) {
      probe();
      return;
    }

    if ("transactional-producer".equals(role)) {
      produce(
          positional.get(0),
          positional.get(1),
          positional.get(2),
          Integer.parseInt(positional.get(3)),
          Integer.parseInt(positional.get(4)),
          abortEvery,
          holdOpenMs,
          crashAfterProduce);
    } else if ("compressed-producer".equals(role)) {
      compressedProduce(positional.get(0), positional.get(1), positional.get(2), Integer.parseInt(positional.get(3)), compression);
    } else if ("read-committed-consumer".equals(role)) {
      consume(positional.get(0), positional.get(1), positional.get(2), Long.parseLong(positional.get(3)));
    } else {
      System.err.println("--role must be transactional-producer, compressed-producer or read-committed-consumer, got " + role);
      System.exit(2);
    }
  }

  /**
   * Initializes a transactional producer and runs {@code transactions} transactions of
   * {@code recordsPerTransaction} records each. Every {@code abortEvery}-th transaction aborts,
   * the others commit. The last one can be held open before it ends, or ended by a crash after its
   * records were acknowledged and before any EndTxn - {@code halt}, not {@code exit}, so no
   * shutdown hook and no {@code close()} reaches the broker.
   */
  private static void produce(
      String bootstrapServers,
      String transactionalId,
      String topic,
      int transactions,
      int recordsPerTransaction,
      int abortEvery,
      long holdOpenMs,
      boolean crashAfterProduce) {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaProducer<String, String> producer = null;
    try {
      producer = new KafkaProducer<>(properties);
      producer.initTransactions();
      print("{\"event\":\"initialized\",\"transactionalId\":" + quote(transactionalId) + "}");

      for (int t = 0; t < transactions; t++) {
        producer.beginTransaction();
        for (int i = 0; i < recordsPerTransaction; i++) {
          String key = transactionalId + "-" + t + "-" + i;
          producer.send(new ProducerRecord<>(topic, key, key));
        }
        producer.flush();
        print("{\"event\":\"produced\",\"transaction\":" + t + ",\"records\":" + recordsPerTransaction + "}");

        boolean last = t == transactions - 1;
        if (crashAfterProduce && last) {
          Runtime.getRuntime().halt(EXIT_CRASH);
        }
        if (holdOpenMs > 0 && last) {
          Thread.sleep(holdOpenMs);
        }

        if (abortEvery > 0 && (t + 1) % abortEvery == 0) {
          producer.abortTransaction();
          print("{\"event\":\"aborted\",\"transaction\":" + t + "}");
        } else {
          producer.commitTransaction();
          print("{\"event\":\"committed\",\"transaction\":" + t + "}");
        }
      }
    } catch (Exception e) {
      Throwable fencing = fencingCause(e);
      if (producer != null) {
        producer.close(Duration.ZERO);
      }
      if (fencing != null) {
        print("{\"event\":\"fenced\",\"error\":" + quote(fencing.getClass().getSimpleName()) + "}");
        System.exit(EXIT_FENCED);
      }
      exitWithError(e);
    }

    producer.close();
    print("{\"event\":\"done\"}");
  }

  /**
   * Produces an ordered UTF-8 corpus with the requested broker codec. Each key and value is
   * {@code source + "-" + index}; all records target partition zero. Flush confirms delivery
   * before the done line. Existing transactional and consumer roles keep their own settings.
   *
   * @param bootstrapServers broker endpoints
   * @param source corpus prefix
   * @param topic target topic
   * @param records corpus length
   * @param compression gzip, snappy, lz4 or zstd
   * @throws IllegalArgumentException for an unsupported codec or negative record count
   */
  private static void compressedProduce(String bootstrapServers, String source, String topic,
      int records, String compression) {
    if (!List.of("gzip", "snappy", "lz4", "zstd").contains(compression) || records < 0) {
      throw new IllegalArgumentException("Unsupported compression or record count");
    }
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
    properties.put(ProducerConfig.LINGER_MS_CONFIG, "5");
    properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
      List<java.util.concurrent.Future<?>> sends = new ArrayList<>();
      for (int i = 0; i < records; i++) {
        String value = source + "-" + i;
        sends.add(producer.send(new ProducerRecord<>(topic, 0, value, value)));
      }
      producer.flush();
      for (java.util.concurrent.Future<?> send : sends) send.get();
      print("{\"event\":\"done\",\"records\":" + records + ",\"compression\":" + quote(compression) + "}");
    } catch (Exception error) {
      exitWithError(error);
    }
  }

  /**
   * Reads one topic at {@code read_committed} from the earliest offset until the deadline, printing
   * every record it is handed. A client failure prints an {@code error} line and exits with
   * {@link #EXIT_ERROR}.
   */
  private static void consume(String bootstrapServers, String groupId, String topic, long runForMs) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // Pinned for the same reason JavaGroupMember pins it: a later default must not change the run.
    properties.put("group.protocol", "classic");

    long deadline = System.currentTimeMillis() + runForMs;
    int records = 0;
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
      consumer.subscribe(Collections.singletonList(topic));
      while (System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
          print(
              "{\"event\":\"record\",\"partition\":"
                  + record.partition()
                  + ",\"offset\":"
                  + record.offset()
                  + ",\"key\":"
                  + quote(record.key())
                  + ",\"value\":"
                  + quote(record.value())
                  + "}");
          records++;
        }
      }
    } catch (Exception e) {
      exitWithError(e);
    }

    print("{\"event\":\"done\",\"records\":" + records + "}");
  }

  /** Prints one {@code error} line for a client failure and ends the JVM with {@link #EXIT_ERROR}. */
  private static void exitWithError(Exception error) {
    print(
        "{\"event\":\"error\",\"error\":"
            + quote(error.getClass().getSimpleName())
            + ",\"message\":"
            + quote(String.valueOf(error.getMessage()))
            + "}");
    System.exit(EXIT_ERROR);
  }

  /**
   * The fencing error in a failure's cause chain: a newer owner of the transactional id either
   * fenced this producer outright or bumped the epoch it holds.
   *
   * @param error what the producer threw
   * @return the fencing cause, or {@code null} when the failure is something else
   */
  private static Throwable fencingCause(Throwable error) {
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      if (cause instanceof ProducerFencedException || cause instanceof InvalidProducerEpochException) {
        return cause;
      }
    }
    return null;
  }

  /**
   * Loads every class a run needs, static initializers included, and exits non-zero naming the
   * first one that could not be resolved.
   */
  private static void probe() {
    for (String className : PROBE_CLASSES) {
      try {
        Class.forName(className, true, JavaTransactionalMember.class.getClassLoader());
      } catch (ClassNotFoundException | LinkageError e) {
        System.err.println("NoClassDefFoundError while loading " + className + ": " + e);
        System.exit(1);
      }
    }

    print("{\"event\":\"probe-ok\"}");
  }

  /** @param value a string or null @return its JSON literal */
  private static String quote(String value) {
    if (value == null) {
      return "null";
    }
    return "\""
        + value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r")
        + "\"";
  }

  /** @param line one JSON line, printed and flushed so a reader sees it before any later crash */
  private static void print(String line) {
    System.out.println(line);
    System.out.flush();
  }
}
