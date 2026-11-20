#!/usr/bin/env bash
# Downloads the pinned build inputs into $TF1_CACHE through download.sh, which retries and publishes a file only once
# its SHA-256 matches fixtures/inputs.sha256, then verifies the whole set again.
# Only Maven Central, the OpenTelemetry proto repo at tag v1.9.0 and, when Maven Central fails for protoc, the protobuf
# v25.5 release archive (its bin/protoc carries the same pinned digest) are contacted; nothing is fetched at broker start.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/download.sh"
CACHE=${TF1_CACHE:?set TF1_CACHE to the download cache directory}
M=https://repo1.maven.org/maven2
O=https://github.com/open-telemetry/opentelemetry-proto/raw/refs/tags/v1.9.0/opentelemetry/proto
while read -r rel url second; do
  fetch "$HERE/fixtures/inputs.sha256" "$CACHE" "$rel" "$url" ${second:+"$second"}
done <<LIST
tools/protoc-3.25.5-linux-x86_64.exe $M/com/google/protobuf/protoc/3.25.5/protoc-3.25.5-linux-x86_64.exe https://github.com/protocolbuffers/protobuf/releases/download/v25.5/protoc-25.5-linux-x86_64.zip#bin/protoc
jars/protobuf-java-3.25.5.jar $M/com/google/protobuf/protobuf-java/3.25.5/protobuf-java-3.25.5.jar
jars/kafka-clients-4.4.0.jar $M/org/apache/kafka/kafka-clients/4.4.0/kafka-clients-4.4.0.jar
jars/kafka-clients-4.3.1.jar $M/org/apache/kafka/kafka-clients/4.3.1/kafka-clients-4.3.1.jar
jars/kafka-clients-4.2.2.jar $M/org/apache/kafka/kafka-clients/4.2.2/kafka-clients-4.2.2.jar
jars/slf4j-api-1.7.36.jar $M/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar
proto/opentelemetry/proto/common/v1/common.proto $O/common/v1/common.proto
proto/opentelemetry/proto/resource/v1/resource.proto $O/resource/v1/resource.proto
proto/opentelemetry/proto/metrics/v1/metrics.proto $O/metrics/v1/metrics.proto
LIST
chmod +x "$CACHE/tools/protoc-3.25.5-linux-x86_64.exe"
(cd "$CACHE" && sha256sum -c "$HERE/fixtures/inputs.sha256")
