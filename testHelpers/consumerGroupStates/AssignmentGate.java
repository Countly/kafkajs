package org.apache.kafka.coordinator.group;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.function.LongSupplier;

/** A fixture-only, nonblocking assignment hold for one explicitly armed group and nonce. */
public final class AssignmentGate {
    private static final long MAX_HOLD_NANOS = 30_000_000_000L;
    private static final AssignmentGate INSTANCE = new AssignmentGate(
        System.getProperty("consumer.assignment.gate.dir") == null ? null
            : Path.of(System.getProperty("consumer.assignment.gate.dir")), System::nanoTime);
    private final Path directory;
    private final LongSupplier clock;
    private String token;
    private long deadline;
    private boolean retired;

    /** @param directory fixture control directory, or null to disable @param clock monotonic nanos */
    AssignmentGate(Path directory, LongSupplier clock) {
        this.directory = directory;
        this.clock = clock;
    }

    /**
     * Returns immediately so the coordinator can commit the epoch record and service Describe.
     * @param groupId the exact group being processed
     * @param proposedEpoch epoch about to be committed
     * @param assignmentEpoch last target assignment epoch
     * @return whether only target-assignment construction must be deferred
     */
    public static boolean shouldHold(String groupId, int proposedEpoch, int assignmentEpoch) {
        return INSTANCE.held(groupId, proposedEpoch, assignmentEpoch);
    }

    /**
     * Reads a two-line arm file (group, nonce). A matching release file or 30-second expiry retires
     * that nonce. One nonce is retained; no worker, timer, sleeping call or historical map exists.
     * @param groupId exact group
     * @param proposedEpoch new epoch
     * @param assignmentEpoch prior target epoch
     * @return true only inside the armed, unreleased window
     * @throws IllegalStateException if a configured control or evidence write cannot be read
     */
    synchronized boolean held(String groupId, int proposedEpoch, int assignmentEpoch) {
        if (directory == null || proposedEpoch <= assignmentEpoch) return false;
        Path arm = directory.resolve("arm");
        if (!Files.exists(arm)) return false;
        try {
            List<String> lines = Files.readAllLines(arm, StandardCharsets.UTF_8);
            if (lines.size() != 2 || !lines.get(1).matches("[a-zA-Z0-9_-]{1,64}")) {
                throw new IllegalStateException("assignment gate arm must contain a group and nonce");
            }
            if (!lines.get(0).equals(groupId)) return false;
            String next = lines.get(0) + "\n" + lines.get(1);
            if (!next.equals(token)) {
                token = next;
                deadline = clock.getAsLong() + MAX_HOLD_NANOS;
                retired = false;
                event("arrived", groupId, lines.get(1), proposedEpoch, assignmentEpoch);
            }
            if (retired) return false;
            Path release = directory.resolve("release");
            if (Files.exists(release) && Files.readString(release).trim().equals(lines.get(1))) {
                retired = true;
                event("released", groupId, lines.get(1), proposedEpoch, assignmentEpoch);
                return false;
            }
            if (clock.getAsLong() - deadline >= 0) {
                retired = true;
                event("expired", groupId, lines.get(1), proposedEpoch, assignmentEpoch);
                return false;
            }
            return true;
        } catch (IOException error) {
            throw new IllegalStateException("assignment gate control failed", error);
        }
    }

    /** @param value arbitrary group name @return a JSON string with control characters escaped */
    private static String quote(String value) {
        StringBuilder out = new StringBuilder("\"");
        for (int index = 0; index < value.length(); index++) {
            char c = value.charAt(index);
            if (c == '"' || c == '\\') out.append('\\');
            if (c < 32) out.append(String.format("\\u%04x", (int) c));
            else out.append(c);
        }
        return out.append('"').toString();
    }

    /** @param status transition @param groupId exact group @param nonce arm identity @param epoch proposed epoch @param assignment prior assignment @throws IOException evidence failure */
    private void event(String status, String groupId, String nonce, int epoch, int assignment) throws IOException {
        String json = "{\"status\":" + quote(status) + ",\"groupId\":" + quote(groupId)
            + ",\"nonce\":" + quote(nonce) + ",\"groupEpoch\":" + epoch
            + ",\"assignmentEpoch\":" + assignment + "}\n";
        Files.writeString(directory.resolve("events.jsonl"), json, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
