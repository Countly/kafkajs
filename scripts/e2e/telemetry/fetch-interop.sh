#!/usr/bin/env bash
# Downloads the five pinned Java 4.3.1 interop jars into the flat directory $TELEMETRY_INTEROP_CACHE and verifies each
# SHA-256 against interop.sha256. Only Maven Central is contacted, never at broker start. run.sh client re-verifies them.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
CACHE=${TELEMETRY_INTEROP_CACHE:?set TELEMETRY_INTEROP_CACHE to the flat interop jar directory}
case "$CACHE" in /*) ;; *) exit 2 ;; esac
M=https://repo1.maven.org/maven2
mkdir -p "$CACHE"
while read -r name url; do
  [ -s "$CACHE/$name" ] || timeout 120 curl -fsSL --max-time 110 -o "$CACHE/$name" "$url"
done <<LIST
kafka-clients-4.3.1.jar $M/org/apache/kafka/kafka-clients/4.3.1/kafka-clients-4.3.1.jar
slf4j-api-1.7.36.jar $M/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar
lz4-java-1.10.2.jar $M/at/yawk/lz4/lz4-java/1.10.2/lz4-java-1.10.2.jar
snappy-java-1.1.10.7.jar $M/org/xerial/snappy/snappy-java/1.1.10.7/snappy-java-1.1.10.7.jar
zstd-jni-1.5.6-10.jar $M/com/github/luben/zstd-jni/1.5.6-10/zstd-jni-1.5.6-10.jar
LIST
(cd "$CACHE" && sha256sum -c "$HERE/interop.sha256")
