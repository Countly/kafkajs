package org.apache.kafka.coordinator.group;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/** Broker-free checks of gate control and bounded release; this is not a Describe capture. */
public final class GateProbe {
    private static int checks;

    /** @param value condition @param message assertion name @throws AssertionError failed control */
    private static void check(boolean value, String message) {
        if (!value) throw new AssertionError(message);
        checks++;
    }

    /** @param args unused @throws Exception fixture setup or assertion failure */
    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("consumer-assignment-gate-");
        long[] now = {0};
        AssignmentGate gate = new AssignmentGate(dir, () -> now[0]);
        try {
            check(!new AssignmentGate(null, () -> 0).held("group", 2, 1), "disabled");
            check(!gate.held("group", 2, 1), "unarmed");
            Files.writeString(dir.resolve("arm"), "group\nfirst\n");
            check(!gate.held("other", 2, 1), "different group");
            check(!gate.held("group", 1, 1), "no epoch delta");
            check(gate.held("group", 2, 1), "arrival holds target construction");
            String arrival = Files.readString(dir.resolve("events.jsonl"));
            check(arrival.contains("\"groupEpoch\":2,\"assignmentEpoch\":1"), "epoch evidence");
            check(gate.held("group", 2, 1), "repeated call is nonblocking and still held");
            check(Files.readAllLines(dir.resolve("events.jsonl")).size() == 1, "single arrival");
            Files.writeString(dir.resolve("release"), "wrong");
            check(gate.held("group", 2, 1), "wrong nonce cannot release");
            Files.writeString(dir.resolve("release"), "first");
            check(!gate.held("group", 2, 1), "matching release");
            Files.delete(dir.resolve("release"));
            check(!gate.held("group", 2, 1), "released nonce stays retired");
            Files.writeString(dir.resolve("arm"), "group\nsecond\n");
            check(gate.held("group", 3, 2), "new nonce rearms");
            now[0] = 30_000_000_000L;
            check(!gate.held("group", 3, 2), "expiry at exact boundary");
            check(Files.readString(dir.resolve("events.jsonl")).contains("\"status\":\"expired\""), "expiry evidence");
            check(!gate.held("group", 3, 2), "expired nonce stays retired");
            Files.writeString(dir.resolve("arm"), "bad");
            try {
                gate.held("group", 4, 3);
                throw new AssertionError("malformed arm accepted");
            } catch (IllegalStateException expected) {
                check(expected.getMessage().contains("group and nonce"), "malformed command refusal");
            }
            System.out.println("{\"checks\":" + checks + ",\"pass\":true}");
        } finally {
            try (var paths = Files.walk(dir)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
            }
        }
    }
}
