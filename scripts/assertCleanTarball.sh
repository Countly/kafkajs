#!/bin/bash
# Fails when a package tarball holds a test, a fixture, a types test file or anything from otel/.
# Usage: assertCleanTarball.sh <tarball.tgz>
set -e

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <tarball.tgz>" >&2
  exit 2
fi

# Listing first, so a tarball tar cannot read fails the script instead of passing as empty.
FILES="$(tar -tzf "$1")"

if echo "$FILES" | grep -E '\.spec\.js$|__tests__/|__snapshots__/|types/tests|fixtures/'; then
  echo "tarball contains test files" >&2
  exit 1
fi

if echo "$FILES" | grep -E '^package/otel(/|$)'; then
  echo "tarball contains otel/ entries" >&2
  exit 1
fi

# Ship exactly the public share and client telemetry guides from docs/.
for guide in ShareConsumer ClientTelemetry; do
  if ! echo "$FILES" | grep -Fxq "package/docs/$guide.md"; then
    echo "tarball is missing the $guide guide" >&2
    exit 1
  fi
  if tar -xOf "$1" "package/docs/$guide.md" | grep -E '/var/tmp/|/home/[^ /]+/|\.superpowers/'; then
    echo "$guide guide contains a local path" >&2
    exit 1
  fi
done
if echo "$FILES" | grep -E '^package/docs/' | grep -Ev '^package/docs/(ShareConsumer|ClientTelemetry)\.md$'; then
  echo "tarball contains unintended documentation" >&2
  exit 1
fi
# Telemetry tooling and the package-export witness stay out of the tarball.
if echo "$FILES" | grep -E '^package/(scripts/e2e/telemetry/|.*\.jar$|.*\.proto$|types/package-exports\.mts$)'; then
  echo "tarball contains telemetry tooling" >&2
  exit 1
fi
