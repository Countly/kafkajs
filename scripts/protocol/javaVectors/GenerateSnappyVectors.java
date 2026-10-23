import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.regex.Pattern;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

/** Independent Snappy byte vectors and read-back checks for JavaScript encoder artifacts. */
public final class GenerateSnappyVectors {
    /**
     * Generates fixed-input raw/xerial vectors, or checks committed JavaScript output with Java.
     * @param args output directory, or --read-back and artifact directory
     * @throws Exception for I/O, compression or unequal read-back bytes
     */
    public static void main(String[] args) throws Exception {
        Path directory = Path.of(args[args.length - 1]);
        if (args[0].equals("--read-back")) {
            try (var paths = Files.list(directory)) {
                for (Path path : paths.filter(p -> p.toString().endsWith(".json")).sorted().toList()) {
                    String json = Files.readString(path);
                    byte[] expected = corpus(textField(json, "input"), Integer.parseInt(textField(json, "length")));
                    byte[] raw = Snappy.uncompress(HexFormat.of().parseHex(field(json, "rawHex")));
                    byte[] framed;
                    try (var in = new SnappyInputStream(new ByteArrayInputStream(
                            HexFormat.of().parseHex(field(json, "hex"))))) {
                        framed = in.readAllBytes();
                    }
                    if (!Arrays.equals(expected, raw) || !Arrays.equals(expected, framed)) {
                        throw new IllegalStateException("Read-back mismatch: " + path);
                    }
                    System.out.println("read-back ok " + path.getFileName());
                }
            }
            return;
        }
        Files.createDirectories(directory);
        String[] names = {"empty", "one", "short29", "json", "random", "zeros", "mixed", "edge32", "edge64"};
        int[] sizes = {0, 1, 29, 1024, 1024, 4096, 4096, 32769, 65537};
        for (int index = 0; index < names.length; index++) {
            String name = names[index];
            byte[] input = corpus(name, sizes[index]);
            byte[] raw = Snappy.compress(input);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (var stream = new SnappyOutputStream(out, 32768)) { stream.write(input); }
            byte[][] forms = {raw, out.toByteArray()};
            String[] labels = {"raw", "xerial"};
            for (int form = 0; form < forms.length; form++) {
                String json = "{\n  \"source\": {\"generator\": \"GenerateSnappyVectors.java\", "
                    + "\"library\": \"snappy-java 1.1.10.7\", \"input\": \"" + name
                    + "\", \"length\": " + input.length + ", \"seed\": 305419896, \"blockSize\": 32768},\n"
                    + "  \"hex\": \"" + HexFormat.of().formatHex(forms[form]) + "\",\n"
                    + "  \"fields\": {\"size\": " + input.length + ", \"sha256\": \""
                    + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(input)) + "\"}\n}\n";
                Files.writeString(directory.resolve("snappy-" + name + "-" + labels[form] + ".json"), json);
            }
        }
    }

    /**
     * Builds deterministic input from a named recipe. Integer operations use uint32 xorshift.
     * @param name corpus kind
     * @param size exact byte count
     * @return source bytes
     */
    private static byte[] corpus(String name, int size) {
        byte[] bytes = new byte[size];
        byte[] text = "{\"key\":\"value\",\"count\":123,\"active\":true}\n".getBytes(StandardCharsets.UTF_8);
        int state = 0x12345678;
        for (int i = 0; i < size; i++) {
            state ^= state << 13; state ^= state >>> 17; state ^= state << 5;
            bytes[i] = (byte) (name.equals("random") || name.equals("mixed") && i % 256 < 64
                ? state : name.equals("zeros") ? 0 : text[i % text.length]);
        }
        return bytes;
    }

    /**
     * Reads a corpus name or size from the closed artifact schema.
     * @param json artifact text
     * @param key required field
     * @return unquoted scalar value
     * @throws IllegalArgumentException for a missing field
     */
    private static String textField(String json, String key) {
        var match = Pattern.compile("\\\"" + key + "\\\"\\s*:\\s*\\\"?([A-Za-z0-9]+)").matcher(json);
        if (!match.find()) throw new IllegalArgumentException("Missing " + key);
        return match.group(1);
    }

    /**
     * Reads a hex string from the closed, generator-owned artifact schema.
     * @param json artifact text
     * @param key required field
     * @return field value
     * @throws IllegalArgumentException for a missing field
     */
    private static String field(String json, String key) {
        var match = Pattern.compile("\\\"" + key + "\\\"\\s*:\\s*\\\"([0-9a-f]*)\\\"").matcher(json);
        if (!match.find()) throw new IllegalArgumentException("Missing " + key);
        return match.group(1);
    }
}
