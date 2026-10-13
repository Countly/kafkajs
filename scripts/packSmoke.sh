#!/bin/bash -e
# Packs the library, installs the tarball into a throwaway project and runs the smoke tests against it.
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PACK_DIR="${PACK_DIR:-$(mktemp -d)}"
export PACK_DIR

TARBALL="$(cd "$ROOT" && npm pack --silent --pack-destination "$PACK_DIR")"
echo "tarball: $PACK_DIR/$TARBALL"

# The tarball must not contain tests, fixtures or the otel/ sub-package.
"$ROOT/scripts/assertCleanTarball.sh" "$PACK_DIR/$TARBALL"

cd "$PACK_DIR"
[ -f package.json ] || echo '{ "name": "pack-smoke", "private": true, "type": "commonjs" }' > package.json
npm install --silent --no-audit --no-fund "./$TARBALL"
cp "$ROOT/scripts/packageSmoke.test.mjs" ./packageSmoke.test.mjs
node --test ./packageSmoke.test.mjs

# Types condition check: resolve the package name under node16 resolution from the temp project.
# index.d.ts references Node builtin types (Buffer, net, tls), same as any real TS consumer would need.
npm install --silent --no-audit --no-fund --no-save @types/node@^24
cp "$ROOT/types/tests.node16.ts" ./tests.node16.ts
cp "$ROOT/types/tsconfig.node16.json" ./tsconfig.node16.json
"$ROOT/node_modules/.bin/tsc" -p ./tsconfig.node16.json
echo "node16 types check passed"
