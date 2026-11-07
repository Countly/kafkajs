#!/usr/bin/env bash
# Downloads the pinned build inputs into $TF1_CACHE and verifies every SHA-256 against fixtures/inputs.sha256.
# Only Maven Central and the OpenTelemetry proto repo at tag v1.9.0 are contacted; nothing is fetched at broker start.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
CACHE=${TF1_CACHE:?set TF1_CACHE to the download cache directory}
M=https://repo1.maven.org/maven2
O=https://github.com/open-telemetry/opentelemetry-proto/raw/refs/tags/v1.9.0/opentelemetry/proto
while read -r rel url; do
  mkdir -p "$CACHE/$(dirname "$rel")"
  [ -s "$CACHE/$rel" ] || timeout 280 curl -fsSL --max-time 270 -o "$CACHE/$rel" "$url"
done <<LIST
tools/protoc-3.25.5-linux-x86_64.exe $M/com/google/protobuf/protoc/3.25.5/protoc-3.25.5-linux-x86_64.exe
jars/protobuf-java-3.25.5.jar $M/com/google/protobuf/protobuf-java/3.25.5/protobuf-java-3.25.5.jar
jars/kafka-clients-4.3.1.jar $M/org/apache/kafka/kafka-clients/4.3.1/kafka-clients-4.3.1.jar
jars/kafka-clients-4.2.2.jar $M/org/apache/kafka/kafka-clients/4.2.2/kafka-clients-4.2.2.jar
jars/slf4j-api-1.7.36.jar $M/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar
proto/opentelemetry/proto/common/v1/common.proto $O/common/v1/common.proto
proto/opentelemetry/proto/resource/v1/resource.proto $O/resource/v1/resource.proto
proto/opentelemetry/proto/metrics/v1/metrics.proto $O/metrics/v1/metrics.proto
LIST
chmod +x "$CACHE/tools/protoc-3.25.5-linux-x86_64.exe"
(cd "$CACHE" && sha256sum -c "$HERE/fixtures/inputs.sha256")
