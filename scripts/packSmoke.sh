#!/bin/bash -e
# Packs the library, installs the tarball into a throwaway project and runs the smoke tests against it.
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PACK_DIR="${PACK_DIR:-$(mktemp -d)}"
export PACK_DIR

TARBALL="$(cd "$ROOT" && npm pack --silent --pack-destination "$PACK_DIR")"
echo "tarball: $PACK_DIR/$TARBALL"

# The tarball must not contain tests, fixtures or the otel/ sub-package.
"$ROOT/scripts/assertCleanTarball.sh" "$PACK_DIR/$TARBALL"
# The guard also requires the public share guide and excludes other documentation.

cd "$PACK_DIR"
[ -f package.json ] || echo '{ "name": "pack-smoke", "private": true, "type": "commonjs" }' > package.json
npm install --silent --no-audit --no-fund "./$TARBALL"
# Complete dependency installation before creating the alias: reification can prune symlinks.
npm install --silent --no-audit --no-fund --no-save @types/node@^24
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
