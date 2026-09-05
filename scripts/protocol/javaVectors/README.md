# Java golden vectors

A development-only generator that produces golden protocol byte vectors from Apache Kafka's own
generated message codecs (`org.apache.kafka.common.message.*Data`, from the published
`kafka-clients` jar), so this repo's hand-written flexible-protocol codecs can be checked
byte-for-byte against the reference implementation, not just against captured broker traffic.

Run `scripts/protocol/javaVectors/run.sh` from the repo root. It needs a JDK on `PATH` (`javac`
and `java`) and, on first run, network access to fetch
`https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.3.1/kafka-clients-4.3.1.jar` from
Maven Central. `GenerateVectors.java` is compiled against that jar and run once, writing
`*_java.json` fixtures under `src/protocol/requests/apiVersions/fixtures/` and
`src/protocol/requests/metadata/fixtures/`.

The jar itself is downloaded into a temporary directory and is never committed to this repository
— only the emitted `*_java.json` fixtures are. Re-run the script whenever a codec's fixture values
change, and commit the regenerated files alongside the codec change.
