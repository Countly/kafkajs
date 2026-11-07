#!/bin/bash -e
# Tests the otel/ sub-package against the packed root tarball, the way a consumer installs it.
# `npm ci` deletes node_modules, so the tarball goes in after it. --no-save keeps it out of
# otel/package.json and the lockfile. --save-dev is needed too: the client is an optional peer,
# and npm reads an install of a declared peer as an edit of its range and installs nothing.
# The client must be a real directory in node_modules, never a link (`npm link`, or a `file:`
# directory dependency): a symlinked package has no node_modules segment in its path, so the
# instrumentation hooks never fire.
set -eo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PACK_DIR="$(mktemp -d)"
trap 'rm -rf "$PACK_DIR"' EXIT

TARBALL="$(cd "$ROOT" && npm pack --loglevel=error --pack-destination "$PACK_DIR")"

cd "$ROOT/otel"
# --ignore-scripts: no dependency needs a lifecycle script, so none runs on a developer machine.
npm ci --loglevel=error --no-audit --no-fund --ignore-scripts
npm install --loglevel=error --no-audit --no-fund --no-save --save-dev --ignore-scripts "$PACK_DIR/$TARBALL"
if [ ! -f node_modules/@countly/kafkajs/package.json ] || [ -L node_modules/@countly/kafkajs ]; then
  echo "the packed client is not installed as a regular node_modules directory" >&2
  exit 1
fi

# An explicit reporter keeps CI parsing independent of terminal detection and Node defaults.
if [ -n "${NODE_TEST_REPORT:-}" ]; then
  : > "$NODE_TEST_REPORT"
fi
run_tests() {
  if [ -n "${NODE_TEST_REPORT:-}" ]; then
    node --test-reporter=tap "$@" | tee -a "$NODE_TEST_REPORT"
  else
    node "$@"
  fi
}

# Includes the mandatory O17–O19 meter/span/context/getter/logger containment matrix.
run_tests --test test/*.test.js

# Each file runs in its own process: a loader or a registration hook applies to a whole process.
for file in test/loader/*.test.js; do
  [ -e "$file" ] || continue
  echo "loader: $file"
  run_tests --experimental-loader=@opentelemetry/instrumentation/hook.mjs --test "$file"
done

for file in test/plain/*.test.js; do
  [ -e "$file" ] || continue
  echo "plain: $file"
  run_tests --test "$file"
done

for file in test/register/*.test.js; do
  [ -e "$file" ] || continue
  echo "register: $file"
  run_tests --import ./test/register/register.mjs --test "$file"
done
