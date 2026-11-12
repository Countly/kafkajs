#!/usr/bin/env bash
# Prepares a scratch client source for the selected Kafka API without changing retained source bytes.
# Usage: prepare-client-source.sh <4.4.0|4.3.1|4.2.2> <scratch source directory>
set -euo pipefail
V=${1:?kafka version, 4.4.0, 4.3.1 or 4.2.2}
OUT=${2:?scratch source directory}
HERE=$(cd "$(dirname "$0")" && pwd)
case "$V" in
  4.4.0|4.3.1|4.2.2) ;;
  *) exit 2 ;;
esac
mkdir -p "$OUT"
SOURCE=$OUT/JavaTelemetryClient.java
if [ "$V" = 4.4.0 ]; then
  sed -e 's/^import org.apache.kafka.common.telemetry.internals.ClientTelemetryUtils;$/import io.opentelemetry.proto.metrics.v1.MetricsData;/' \
    -e 's/ClientTelemetryUtils.deserializeMetricsData(push.data().metrics())/MetricsData.parseFrom(push.metricsData(4096))/' \
    "$HERE/JavaTelemetryClient.java" > "$SOURCE"
else
  cp "$HERE/legacy/JavaTelemetryClient.java" "$SOURCE"
fi
printf '%s\n' "$SOURCE"
