# Java golden vectors

A development-only generator that produces golden protocol byte vectors from Apache Kafka's own
generated message codecs (`org.apache.kafka.common.message.*Data`, from the published
`kafka-clients` jar), so this repo's hand-written flexible-protocol codecs can be checked
byte-for-byte against the reference implementation, not just against captured broker traffic.

Run `scripts/protocol/javaVectors/run.sh` from the repo root. It needs a JDK on `PATH` (`javac`
and `java`) and, on first run, network access to fetch
`https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.3.1/kafka-clients-4.3.1.jar` from
Maven Central. `GenerateVectors.java` is compiled against that jar and run once, writing
`v<N>_<request|response>_java.json` fixtures under `src/protocol/requests/<api>/fixtures/`:

| API | Versions |
|---|---|
| ApiVersions | 3, 4 |
| Metadata | 9, 10, 11, 12, 13 |
| Produce | 8, 9, 10, 11 |
| FindCoordinator | 3, 4, 5, 6 |
| OffsetCommit | 6, 7, 8 |
| OffsetFetch | 5, 6, 7, 8 |
| ListOffsets | 4, 5, 6, 7, 8, 9 |
| InitProducerId | 2, 3, 4, 5 |

Both directions are emitted for every version. Every value is a constant of `GenerateVectors.java`,
so a spec can rebuild the same request through this repo's encoder and compare the two buffers
byte-for-byte. The exception is Produce: its request carries a record batch whose timestamp and CRC
no second encoder can reproduce, so those vectors are emitted with a null `Records` field and the
Produce request specs decode the vector's framing instead of comparing bytes.

The jar itself is downloaded into a temporary directory and is never committed to this repository
— only the emitted `*_java.json` fixtures are. Re-run the script whenever a codec's fixture values
change, and commit the regenerated files alongside the codec change.
