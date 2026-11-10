import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.telemetry.ClientTelemetryContext;
import org.apache.kafka.server.telemetry.ClientTelemetryPayload;
import org.countly.kafkajs.telemetry.CountingTelemetryReporter;

/** Broker-free control: hold the first callback after it allocates its sequence identity. */
public class ReporterOrder {
    public static void main(String[] args) throws Exception {
        CountingTelemetryReporter reporter = new CountingTelemetryReporter();
        CountDownLatch allocated = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ClientTelemetryPayload payload = (ClientTelemetryPayload) Proxy.newProxyInstance(
            ReporterOrder.class.getClassLoader(), new Class<?>[]{ClientTelemetryPayload.class},
            (proxy, method, values) -> switch (method.getName()) {
                case "data" -> ByteBuffer.allocate(0);
                case "clientInstanceId" -> Uuid.ZERO_UUID;
                case "isTerminating" -> false;
                case "contentType" -> "application/x-protobuf";
                default -> throw new AssertionError(method.getName());
            });
        ClientTelemetryContext context = (ClientTelemetryContext) Proxy.newProxyInstance(
            ReporterOrder.class.getClassLoader(), new Class<?>[]{ClientTelemetryContext.class},
            (proxy, method, values) -> switch (method.getName()) {
                case "authorizableRequestContext" -> null;
                case "pushIntervalMs" -> {
                    if (Thread.currentThread().getName().equals("first")) {
                        allocated.countDown();
                        if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("release timeout");
                    }
                    yield 1000;
                }
                default -> throw new AssertionError(method.getName());
            });
        Field field = CountingTelemetryReporter.class.getDeclaredField("queue");
        field.setAccessible(true);
        BlockingQueue<?> queue = (BlockingQueue<?>) field.get(reporter);
        Thread first = new Thread(() -> reporter.clientTelemetryExporter().exportMetrics(context, payload), "first");
        Thread second = new Thread(() -> reporter.clientTelemetryExporter().exportMetrics(context, payload), "second");
        try {
            first.start();
            if (!allocated.await(3, TimeUnit.SECONDS)) throw new AssertionError("first callback did not allocate");
            second.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (second.isAlive() && second.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline)
                Thread.yield();
            if (!queue.isEmpty()) throw new AssertionError("sequence 2 became visible while sequence 1 was pending: " + queue);
        } finally {
            release.countDown();
            first.join(3000);
            second.join(3000);
        }
        String one = String.valueOf(queue.poll());
        String two = String.valueOf(queue.poll());
        if (!one.contains("\"seq\":1,") || !two.contains("\"seq\":2,") || !queue.isEmpty())
            throw new AssertionError("non-contiguous export order: " + one + two);
        System.out.println("reporter order control passed");
    }
}
