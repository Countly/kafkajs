#!/usr/bin/env bash
# Prepares a scratch reporter source for the selected Kafka API without changing retained source bytes.
# Usage: prepare-reporter-source.sh <4.4.0|4.3.1|4.2.2> <scratch source directory>
set -euo pipefail
V=${1:?kafka version, 4.4.0, 4.3.1 or 4.2.2}
OUT=${2:?scratch source directory}
HERE=$(cd "$(dirname "$0")" && pwd)
case "$V" in
  4.4.0|4.3.1|4.2.2) ;;
  *) exit 2 ;;
esac
mkdir -p "$OUT"
SOURCE=$OUT/CountingTelemetryReporter.java
if [ "$V" = 4.4.0 ]; then
  sed 's/^import org.apache.kafka.common.utils.AppInfoParser;$/import org.apache.kafka.common.utils.internals.AppInfoParser;/' \
    "$HERE/CountingTelemetryReporter.java" > "$SOURCE"
else
  cp "$HERE/CountingTelemetryReporter.java" "$SOURCE"
fi
printf '%s\n' "$SOURCE"
