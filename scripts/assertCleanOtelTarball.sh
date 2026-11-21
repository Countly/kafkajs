#!/bin/bash
# Fails unless a tarball is exactly this repository's OTel instrumentation package: its name, the
# version and dependencies of otel/package.json, every otel/src file, the licence and readme, and
# nothing else (no tests, fixtures, local paths or main-package files).
# Usage: assertCleanOtelTarball.sh <tarball.tgz>
set -eo pipefail
OTEL="$(cd "$(dirname "$0")/../otel" && pwd)"

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <tarball.tgz>" >&2
  exit 2
fi

# Listing first, so a tarball tar cannot read fails the script instead of passing as empty.
FILES="$(tar -tzf "$1")"

if echo "$FILES" | grep -E '\.(spec|test)\.[cm]?js$|__tests__/|__snapshots__/|^package/test/|fixtures/'; then
  echo "tarball contains test files" >&2
  exit 1
fi

if echo "$FILES" | grep -Ev '^package/(package\.json|LICENSE|README\.md|src/.*)$'; then
  echo "tarball contains files outside the instrumentation package" >&2
  exit 1
fi

# The packed files must be exactly the manifest, licence, readme and every otel/src file.
EXPECTED="$({
  printf 'package/%s\n' package.json LICENSE README.md
  cd "$OTEL" && find src -type f | sed 's|^|package/|'
} | LC_ALL=C sort)"
PACKED="$(echo "$FILES" | LC_ALL=C sort)"

if LC_ALL=C comm -13 <(echo "$EXPECTED") <(echo "$PACKED") | grep .; then
  echo "tarball contains files otel/src does not have" >&2
  exit 1
fi

for file in $(LC_ALL=C comm -23 <(echo "$EXPECTED") <(echo "$PACKED")); do
  echo "tarball is missing ${file#package/}" >&2
  exit 1
done

if tar -xOzf "$1" | grep -aE '/var/tmp/|/home/[^ /]+/|\.superpowers/'; then
  echo "tarball contains a local path" >&2
  exit 1
fi

tar -xOzf "$1" package/package.json | node -e '
const fs = require("fs")
const packed = JSON.parse(fs.readFileSync(0, "utf8"))
const declared = JSON.parse(fs.readFileSync(process.argv[1], "utf8"))
const problems = []
if (packed.name !== "@countly/opentelemetry-instrumentation-kafkajs") problems.push(`name ${packed.name}`)
if (packed.version !== declared.version) {
  problems.push(`version ${packed.version}, expected ${declared.version}`)
}
for (const key of [
  "dependencies",
  "peerDependencies",
  "peerDependenciesMeta",
  "optionalDependencies",
  "bundleDependencies",
  "bundledDependencies",
]) {
  if (JSON.stringify(packed[key]) !== JSON.stringify(declared[key])) {
    problems.push(`${key} differ from otel/package.json`)
  }
}
if (problems.length > 0) {
  console.error(`tarball package.json: ${problems.join("; ")}`)
  process.exit(1)
}
' "$OTEL/package.json"
