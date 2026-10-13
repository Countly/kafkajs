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
