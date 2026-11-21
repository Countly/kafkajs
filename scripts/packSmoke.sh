#!/bin/bash -e
# Installs the library's tarball into a throwaway project and runs the smoke tests against it.
# Usage: packSmoke.sh [<tarball.tgz>]
# With a tarball, smokes exactly that file and never packs; without one, packs the library first.
# The project is PACK_DIR when set (kept afterwards), else a temporary directory removed on exit.
set -eo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [ "$#" -gt 1 ]; then
  echo "usage: $0 [<tarball.tgz>]" >&2
  exit 2
fi
if [ "$#" -eq 1 ]; then
  if [[ "$1" != *.tgz ]] || [ ! -f "$1" ]; then
    echo "not a regular .tgz file: $1" >&2
    exit 2
  fi
  name="$(tar -xOzf "$1" package/package.json | node -p 'JSON.parse(require("fs").readFileSync(0, "utf8")).name')" || {
    echo "cannot read package/package.json from $1" >&2
    exit 2
  }
  if [ "$name" != "@countly/kafkajs" ]; then
    echo "$1 holds $name, expected @countly/kafkajs" >&2
    exit 2
  fi
  TARBALL_PATH="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
fi

if [ -z "$PACK_DIR" ]; then
  PACK_DIR="$(mktemp -d)"
  # Absolute, so the trap still finds it after the cd below when TMPDIR is relative.
  PACK_DIR="$(cd "$PACK_DIR" && pwd)"
  trap 'rm -rf "$PACK_DIR"' EXIT
fi
export PACK_DIR

if [ "$#" -eq 0 ]; then
  TARBALL_PATH="$PACK_DIR/$(cd "$ROOT" && npm pack --silent --pack-destination "$PACK_DIR")"
fi
echo "tarball: $TARBALL_PATH"

# The tarball must not contain tests, fixtures or the otel/ sub-package.
"$ROOT/scripts/assertCleanTarball.sh" "$TARBALL_PATH"
# The guard also requires the public share guide and excludes other documentation.

cd "$PACK_DIR"
[ -f package.json ] || echo '{ "name": "pack-smoke", "private": true, "type": "commonjs" }' > package.json
# --ignore-scripts: neither install needs a lifecycle script, so none runs.
npm install --silent --no-audit --no-fund --ignore-scripts "$TARBALL_PATH"
# Complete dependency installation before creating the alias: reification can prune symlinks.
npm install --silent --no-audit --no-fund --no-save --ignore-scripts @types/node@^24
# The supported `kafkajs` alias resolves to this one installation, so module identity is shared.
[ -e node_modules/kafkajs ] || ln -s @countly/kafkajs node_modules/kafkajs
node -e 'if (require.resolve("kafkajs") !== require.resolve("@countly/kafkajs") || require("kafkajs") !== require("@countly/kafkajs")) throw new Error("Package alias differs")'
cp "$ROOT/scripts/packageSmoke.test.mjs" ./packageSmoke.test.mjs
node --input-type=module -e 'import { createRequire } from "node:module"; import { oauthBearer } from "@countly/kafkajs"; const require = createRequire(import.meta.url); if (oauthBearer !== require("@countly/kafkajs").oauthBearer) throw new Error("OAuth namespace differs")'
NODE_PATH= node --test ./packageSmoke.test.mjs

# Types condition check: resolve the package name under node16 resolution from the temp project.
# index.d.ts references the Node builtin types installed above (Buffer, net, tls).
cp "$ROOT/types/tests.node16.ts" ./tests.node16.ts
cp "$ROOT/types/tsconfig.node16.json" ./tsconfig.node16.json
"$ROOT/node_modules/.bin/tsc" -p ./tsconfig.node16.json
echo "node16 types check passed"

# Package exports and the supported alias under nodenext resolution.
cp "$ROOT/types/package-exports.mts" ./package-exports.mts
"$ROOT/node_modules/.bin/tsc" --module nodenext --moduleResolution nodenext --target es2022 --strict --noEmit ./package-exports.mts
echo "package exports types check passed"
