#!/usr/bin/env bash
# Downloads the five pinned Java 4.3.1 interop jars into the flat directory $TELEMETRY_INTEROP_CACHE and verifies each
# SHA-256 against interop.sha256; download.sh retries and publishes a jar only once its digest matches. Each jar comes
# from Maven Central, then the same path on Google's Maven Central mirror, never at broker start. run.sh client
# re-verifies them.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/download.sh"
CACHE=${TELEMETRY_INTEROP_CACHE:?set TELEMETRY_INTEROP_CACHE to the flat interop jar directory}
case "$CACHE" in /*) ;; *) exit 2 ;; esac
M=https://repo1.maven.org/maven2
G=https://maven-central.storage-download.googleapis.com/maven2
mkdir -p "$CACHE"
while read -r -a entry; do
  fetch "$HERE/interop.sha256" "$CACHE" "${entry[@]}"
done <<LIST
kafka-clients-4.3.1.jar $M/org/apache/kafka/kafka-clients/4.3.1/kafka-clients-4.3.1.jar $G/org/apache/kafka/kafka-clients/4.3.1/kafka-clients-4.3.1.jar
slf4j-api-1.7.36.jar $M/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar $G/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar
lz4-java-1.10.2.jar $M/at/yawk/lz4/lz4-java/1.10.2/lz4-java-1.10.2.jar $G/at/yawk/lz4/lz4-java/1.10.2/lz4-java-1.10.2.jar
snappy-java-1.1.10.7.jar $M/org/xerial/snappy/snappy-java/1.1.10.7/snappy-java-1.1.10.7.jar $G/org/xerial/snappy/snappy-java/1.1.10.7/snappy-java-1.1.10.7.jar
zstd-jni-1.5.6-10.jar $M/com/github/luben/zstd-jni/1.5.6-10/zstd-jni-1.5.6-10.jar $G/com/github/luben/zstd-jni/1.5.6-10/zstd-jni-1.5.6-10.jar
LIST
(cd "$CACHE" && sha256sum -c "$HERE/interop.sha256")
