#!/usr/bin/env bash
# Offline provenance build only; CI consumes the separately pinned decoder jar.
set -euo pipefail
: "${TELEMETRY_JAVA_CACHE:?set the checksum-verified retained Java cache}"
: "${TMPDIR:?set a private temporary directory}"
HERE=$(cd "$(dirname "$0")" && pwd)
WORK=$(mktemp -d "$TMPDIR/full-telemetry-decoder.XXXXXX")
trap 'rm -rf "$WORK"' EXIT
mkdir "$WORK/classes"
javac --release 17 -Xlint:all \
  -cp "$HERE/fixtures/counting-telemetry-reporter-4.3.1.jar:$TELEMETRY_JAVA_CACHE/jars/protobuf-java-3.25.5.jar" \
  -d "$WORK/classes" "$HERE/FullTelemetryDecoder.java"
jar --create --date=2026-01-01T00:00:00Z \
  --file "$WORK/full-telemetry-decoder.jar" -C "$WORK/classes" .
sha256sum "$WORK/full-telemetry-decoder.jar" "$HERE/fixtures/full-telemetry-decoder.jar"
cmp "$WORK/full-telemetry-decoder.jar" "$HERE/fixtures/full-telemetry-decoder.jar"
