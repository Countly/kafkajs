#!/usr/bin/env bash
# Broker-free concurrency control. TF1_CACHE holds the pinned jars from fetch.sh.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
CACHE=${TF1_CACHE:?set TF1_CACHE to the pinned fixture inputs}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
for V in 4.2.2 4.3.1 4.4.0; do
  CP="$HERE/fixtures/counting-telemetry-reporter-$V.jar:$CACHE/jars/kafka-clients-$V.jar:$CACHE/jars/protobuf-java-3.25.5.jar:$CACHE/jars/slf4j-api-1.7.36.jar"
  mkdir -p "$WORK/source" "$WORK/tests"
  javac --release 17 -Xlint:all -cp "$CP" -d "$WORK/tests" "$HERE/__tests__/fixtures/ReporterOrder.java"
  javac --release 17 -Xlint:all -cp "$CP" -d "$WORK/source" "$HERE/CountingTelemetryReporter.java"
  java -cp "$WORK/tests:$WORK/source:$CP" ReporterOrder
  java -cp "$WORK/tests:$CP" ReporterOrder
done
