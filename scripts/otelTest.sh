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
# Install dependencies with lifecycle scripts disabled: no dependency needs one.
npm ci --loglevel=error --no-audit --no-fund --ignore-scripts
# The same tarball also goes in under the npm alias `kafkajs`, as `"kafkajs":
# "npm:@countly/kafkajs@<version>"` installs it. Both in one command: a later --no-save install
# removes the package an earlier one added.
npm install --loglevel=error --no-audit --no-fund --no-save --save-dev --ignore-scripts \
  "$PACK_DIR/$TARBALL" "kafkajs@file:$PACK_DIR/$TARBALL"
for dir in node_modules/@countly/kafkajs node_modules/kafkajs; do
  if [ ! -f "$dir/package.json" ] || [ -L "$dir" ] ||
    [ "$(node -p "require('./$dir/package.json').name")" != "@countly/kafkajs" ]; then
    echo "the packed client is not installed as a regular directory $dir" >&2
    exit 1
  fi
done

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

# Includes the required meter, span, context, getter and logger containment checks.
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
