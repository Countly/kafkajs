#!/usr/bin/env bash
# Builds fixtures/counting-telemetry-reporter-<kafka-version>.jar (reporter + relocated OTLP v1.9.0 bindings) and the
# unrelocated bindings jar used by the offline vector generator. Usage: build.sh <4.3.1|4.2.2>
# Env: TF1_CACHE (downloaded inputs, see fetch.sh), TF1_WORK (scratch build dir), TF1_OUT (default ./fixtures).
# Deterministic: fixed jar timestamps, so two builds of the same inputs give the same SHA-256.
set -euo pipefail
V=${1:?kafka version, 4.3.1 or 4.2.2}
HERE=$(cd "$(dirname "$0")" && pwd)
CACHE=${TF1_CACHE:?set TF1_CACHE}
WORK=${TF1_WORK:?set TF1_WORK}/$V
OUT=${TF1_OUT:-$HERE/fixtures}
PROTOC=$CACHE/tools/protoc-3.25.5-linux-x86_64.exe
PB=$CACHE/jars/protobuf-java-3.25.5.jar
SLF=$CACHE/jars/slf4j-api-1.7.36.jar
KC=$CACHE/jars/kafka-clients-$V.jar
PROTOS="opentelemetry/proto/common/v1/common.proto opentelemetry/proto/resource/v1/resource.proto opentelemetry/proto/metrics/v1/metrics.proto"
DATE=2026-01-01T00:00:00Z
(cd "$CACHE" && sha256sum -c "$HERE/fixtures/inputs.sha256" >/dev/null)
rm -rf "$WORK"; mkdir -p "$WORK" "$OUT"

# 1. generate: unrelocated (offline generator) and relocated (broker classpath) Java bindings
mkdir -p "$WORK/gen-orig" "$WORK/gen-reloc" "$WORK/proto-reloc"
(cd "$CACHE/proto" && "$PROTOC" -I . --java_out="$WORK/gen-orig" $PROTOS)
cp -r "$CACHE/proto/opentelemetry" "$WORK/proto-reloc/"
find "$WORK/proto-reloc" -name '*.proto' -exec sed -i 's|"io\.opentelemetry\.proto\.|"org.countly.kafkajs.telemetry.otlp.|' {} +
(cd "$WORK/proto-reloc" && "$PROTOC" -I . --java_out="$WORK/gen-reloc" $PROTOS)

# 2. compile and package bindings
for kind in orig reloc; do
  mkdir -p "$WORK/classes-$kind"
  javac --release 17 -nowarn -cp "$PB" -d "$WORK/classes-$kind" $(find "$WORK/gen-$kind" -name '*.java')
done
jar --create --date=$DATE --file "$OUT/otlp-v1.9.0-bindings-unrelocated.jar" -C "$WORK/classes-orig" .

# 3. compile the reporter against the pinned Kafka clients jar and the relocated bindings
mkdir -p "$WORK/classes-reporter"
javac --release 17 -Xlint:all -cp "$KC:$PB:$WORK/classes-reloc" -d "$WORK/classes-reporter" "$HERE/CountingTelemetryReporter.java"
mkdir -p "$WORK/jar" && cp -r "$WORK/classes-reloc/." "$WORK/jar/" && cp -r "$WORK/classes-reporter/." "$WORK/jar/"
JAR=$OUT/counting-telemetry-reporter-$V.jar
jar --create --date=$DATE --file "$JAR" -C "$WORK/jar" .

# 3b. deterministic Java telemetry client (KIP-1076 registration), compiled against the same pinned clients jar
mkdir -p "$WORK/classes-client"
javac --release 17 -Xlint:all -cp "$KC:$SLF" -d "$WORK/classes-client" "$HERE/JavaTelemetryClient.java"
jar --create --date=$DATE --file "$OUT/java-telemetry-client-$V.jar" -C "$WORK/classes-client" .

# 4. load proof: the plugin interfaces, the bindings and an empty-payload decode, from the jar alone
java -cp "$JAR:$KC:$PB:$SLF" org.countly.kafkajs.telemetry.CountingTelemetryReporter check
sha256sum "$JAR" "$OUT/otlp-v1.9.0-bindings-unrelocated.jar" "$OUT/java-telemetry-client-$V.jar"
(cd "$WORK/proto-reloc" && sha256sum $PROTOS) > "$WORK/relocated-protos.sha256"
cat "$WORK/relocated-protos.sha256"
