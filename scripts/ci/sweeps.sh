#!/bin/bash
# The exit gate's sweeps over the frozen checkout's tracked files, run from its root: the leak grep, the
# label grep against alpha.9b, test-only exports, focused or failing tests, host paths, TODO text and leftover .orig/.rej files. Each
# hit is compared as `file:text` (line numbers dropped) with the enumerated exceptions, as a
# multiset, so an exception that moves still matches and a second copy of one is a new hit.
# Usage: sweeps.sh <out directory>     Writes <out>/sweeps.txt, whose last line is the verdict.
# Every `git grep` is checked by its own exit status (0 hits, 1 none, anything else a failure);
# a failure stops the sweeps with `sweeps: FAIL`, never with a clean verdict.
set -euo pipefail
out=${1:?usage: sweeps.sh <out directory> <base tag>}
base=${2:?base tag required}
git rev-parse --verify "refs/tags/$base" >/dev/null
mkdir -p "$out"
report="$out/sweeps.txt"
: > "$report"
fatal() {
  echo "sweeps: $*" >> "$report"
  echo "sweeps: FAIL" >> "$report"
  exit 1
}
failed=0
LABEL='Phase [0-9]|Task [0-9]|\b9[A-F]\b|\bB[0-9]{2}\b|\bW[0-9]{2}[ab]?\b|\bD9[0-9]\b|R3-|\bRS-[A-Z][0-9]|Amendment|§'
SHIPPED=(src otel/src otel/test types testHelpers scripts docs README.md SECURITY.md RELEASING.md QUARANTINE.md CONTRIBUTING.md index.js index.mjs CHANGELOG.md examples .github)

# hits <name> <git grep arguments...>: writes <out>/<name>.hits, sorted `file:text` lines.
hits() {
  local name=$1 s=0 found
  shift
  found=$(git grep -I "$@") || s=$?
  [ "$s" -le 1 ] || fatal "git grep for $name exited $s"
  [ "$s" = 0 ] || found=''
  while IFS= read -r line; do printf '%s\n' "${line#"$base:"}"; done <<< "$found" | sed -E 's/^([^:]+):[0-9]+:/\1:/' | sed '/^$/d' | LC_ALL=C sort > "$out/$name.hits" ||
    fatal "cannot write the $name hits"
}
# check <name> <allowed file:text lines>: reports the hits of <name> that no exception covers.
check() {
  local name=$1 extra
  printf '%s\n' "$2" | sed '/^$/d' | LC_ALL=C sort > "$out/$name.allowed" || fatal "cannot write the $name exceptions"
  extra=$(LC_ALL=C comm -23 "$out/$name.hits" "$out/$name.allowed") || fatal "comm failed for $name"
  { echo "== $name: $(wc -l < "$out/$name.hits") hit(s)" && cat "$out/$name.hits"; } >> "$report" || fatal "cannot write the report"
  if [ -n "$extra" ]; then
    { echo "-- $name: not an enumerated exception:" && printf '%s\n' "$extra"; } >> "$report" || fatal "cannot write the report"
    failed=1
  fi
}

# stale_exceptions <name> <exceptions> <hits file>: an exception that no hit uses is a misplaced or stale one.
stale_exceptions() {
  local name=$1 unused
  printf '%s\n' "$2" | sed '/^$/d' | LC_ALL=C sort > "$out/$name.allowed" || fatal "cannot write the $name exceptions"
  unused=$(LC_ALL=C comm -23 "$out/$name.allowed" "$3") || fatal "comm failed for $name"
  if [ -n "$unused" ]; then
    { echo "-- $name: an exception no hit uses:" && printf '%s\n' "$unused"; } >> "$report" || fatal "cannot write the report"
    failed=1
  fi
}

TODOS=$(cat <<'LIST'
src/broker/index.js:    // TODO: validate topics not null/empty
src/broker/index.js:    // TODO: validate groupId, mandatory
LIST
)

# leftovers <name> <git ls-files pathspecs...>: writes <out>/<name>.hits, the tracked file names that match.
leftovers() {
  local name=$1 s=0 found
  shift
  found=$(git ls-files -- "$@") || s=$?
  [ "$s" = 0 ] || fatal "git ls-files for $name exited $s"
  sed '/^$/d' <<< "$found" | LC_ALL=C sort > "$out/$name.hits" || fatal "cannot write the $name hits"
}

LEAKS=$(printf '%b\n' "$(cat <<'LIST'
.gitignore:# cloned competitor repos used only for source-level comparison (\x75pgrade-ref/lib-comparison.md)
.gitignore:.\x73uperpowers/
.gitignore:\x43LAUDE.md
.gitignore:docs/\x73uperpowers/
.gitignore:\x75pgrade-ref-index.md
.gitignore:\x75pgrade-ref/
.npmignore:.\x73uperpowers/
.npmignore:\x43LAUDE.md
.npmignore:\x75pgrade-ref-index.md
.npmignore:\x75pgrade-ref/
scripts/__tests__/testingDoc.spec.js:      /\\/Users\\/|\\/home\\/|new_countly|~\\/|C:\\\\|\x73uperpowers|\x75pgrade-ref|CLAUDE\\.md|AGENT\\.md|EXECUTION\\.md|\\b(?:Phase|Task|Wave) \\d/
scripts/compat/__tests__/notes.spec.js:    for (const ignored of ['\x75pgrade-ref', 'docs/\x73uperpowers', '\x43LAUDE.md', 'CODEX_brainstorm']) {
scripts/compat/__tests__/render.spec.js:      for (const ignored of ['\x75pgrade-ref', 'docs/\x73uperpowers', '\x43LAUDE.md', 'CODEX_brainstorm']) {
scripts/compat/config.json:    "json": "docs/\x6bafka-compatibility.json"
scripts/compat/index.js: *   npm run compat:generate   write docs/KafkaCompatibility.md + docs/\x6bafka-compatibility.json
src/oauth/__tests__/oauthDocs.spec.js:      /\\/Users\\/|\\/home\\/|\\/var\\/tmp|new_countly|~\\/|C:\\\\|\x73uperpowers|\x75pgrade-ref|CLAUDE\\.md|AGENT\\.md|EXECUTION\\.md|\\b(?:Phase|Task|Wave) \\d/
scripts/compat/__tests__/build.spec.js:      expect(JSON.parse(read('docs', '\x6bafka-compatibility.json')).generatedFrom.kafkaTag).toBe(tag)
scripts/assertCleanTarball.sh:  if tar -xOf "$1" "package/docs/$guide.md" | grep -E '/\x76ar/tmp/|/\x68ome/[^ /]+/|\\.\x73uperpowers/'; then
src/consumer/share/__tests__/shareIsolationDocs.spec.js:    expect(guide).not.toMatch(/\\/var\\/tmp\\/|\\/\x68ome\\/[^ /]+\\/|\\.\x73uperpowers\\//)
LIST
)")
NEW_LABELS=$(printf '%b\n' "$(cat <<'LIST'
CHANGELOG.md:### \x50hase \x39C — Consumer parity
src/__tests__/equivalentApis.spec.js:it('R1\u00a7B1:currentLag', async () => {
src/__tests__/equivalentApis.spec.js:it('R1\u00a7B1:seek', async () => {
src/__tests__/equivalentApis.spec.js:it('R1\u00a7B2:default.api.timeout.ms', async () => {
src/__tests__/equivalentApis.spec.js:it('R1\u00a7B2:enable.auto.commit', async () => {
src/__tests__/equivalentApis.spec.js:it('R1\u00a7B2:max.poll.records', async () => {
src/__tests__/equivalentApis.spec.js:it('R2\u00a73:MaxConcurrentFetches', async () => {
src/__tests__/equivalentApis.spec.js:it('R2\u00a73:OnOffsetsFetched', async () => {
CHANGELOG.md:### \x50hase \x39D — Client, security and admin breadth
docs/OAuth.md:With `clientAssertion` in place of the secret, the client authenticates with a signed JWT (RFC 7523 \u00a72.2): the body carries `client_assertion_type`, `client_assertion` and `client_id`, and there is no Authorization header. The supplier is called once for each token request; it may return the assertion or a promise of it.
docs/OAuth.md:`clientCredentials` sends the OAuth 2.0 client-credentials grant (RFC 6749 \u00a74.4). With `clientSecret`, the client id and secret are form-encoded and sent as HTTP Basic authentication; the body is `grant_type=client_credentials`, plus `scope` when given.
docs/OAuth.md:`jwtBearer` sends the JWT-bearer grant (RFC 7523 \u00a72.1): the assertion is the grant, as `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=…`, plus `scope` when given.
src/__tests__/equivalentApis.spec.js:  it('R1\u00a7C:listTopics', async () => {
src/__tests__/equivalentApis.spec.js:  it('R2\u00a75:kadm.Lag', async () => {
src/__tests__/equivalentApis.spec.js:it('R1\u00a7C:forceTerminateTransaction', async () => {
src/__tests__/equivalentApis.spec.js:it('R1\u00a7C:deleteShareGroups', async () => {
src/__tests__/equivalentApis.spec.js:it('R1\u00a7E:poll', async () => {
src/__tests__/equivalentApis.spec.js:it('R1\u00a7E:commitAsync', async () => {
src/__tests__/equivalentApis.spec.js:it('R1\u00a7E:ackCallback', async () => {
src/__tests__/equivalentApis.spec.js:it('R1\u00a7E:reset-offsets', async () => {
src/__tests__/equivalentApis.spec.js:it('R1\u00a7E:commitSync', async () => {
src/__tests__/equivalentApis.spec.js:it('R1\u00a7E:unsubscribe', async () => {
src/__tests__/equivalentApis.spec.js:it('R1\u00a7E:resubscribe', async () => {
src/oauth/clientCredentials.js: * @returns {string} the value form-encoded, as RFC 6749 \u00a72.3.1 requires before HTTP Basic
src/oauth/clientCredentials.js: * Basic authentication; with `clientAssertion` (RFC 7523 \u00a72.2, KIP-1258) it carries the supplied
src/oauth/clientCredentials.js: * The OAuth `error` codes RFC 6749 defines (\u00a75.2; \u00a74.1.2.1 for the last two) that a helper error
src/oauth/clientCredentials.js: * \u00a74.4, KIP-768), over one single-flight token cache. With `clientSecret` the request carries HTTP
src/oauth/clientCredentials.js:/** RFC 7523 \u00a72.2: the client authenticates with a JWT assertion. */
src/oauth/jwtBearer.js: * Builds the existing `oauthBearerProvider` callback for the JWT-bearer grant (RFC 7523 \u00a72.1,
src/oauth/jwtBearer.js:/** RFC 7523 \u00a72.1: a JWT is the authorization grant. */
src/oauth/signAssertion.js:   *   key needs at least 2048 bits (RFC 7518 \u00a73.3)
src/oauth/signAssertion.js: * Builds an assertion supplier that signs a new JWT on every call (RFC 7523 \u00a73), for the
src/oauth/signAssertion.js:/** The JOSE algorithms of RFC 7518 \u00a73.3 and \u00a73.4 a signer accepts, with the key each needs. */
types/index.d.ts:  /** SASL extensions sent with the token (RFC 7628 \u00a73.1). */
CHANGELOG.md:### \x50hase \x39E — Share groups
CHANGELOG.md:### \x50hase \x39F — Client telemetry
CHANGELOG.md:### \x50hase 10 — Release, CI and benchmarks
scripts/benchmarks/__tests__/telemetry.spec.js:      milestone: '\x39E',
scripts/benchmarks/telemetry.js:    assert.equal(accepted.milestone, '\x39E')
scripts/benchmarks/telemetry.js: * Candidate arms share one snapshot. Previous requires a separately hashed accepted-\x39E receipt with
scripts/benchmarks/telemetry.js: * {milestone:'\x39E',status:'accepted',tree,sourceSha256,tarballSha256} matching its frozen inputs.
scripts/e2e/telemetry/fixtures/manifest.json:  "purpose": "TF1 fixture for \x50hase \x39F: broker-side KIP-714 reporter, independent OTLP decode, golden vectors. Test-only; never in the client tarball.",
scripts/soak/telemetryChurn.js:   * @returns {void} @throws {Error} unless the facts re-derive exactly what the \x39E readers read
scripts/soak/telemetryChurn.js: * against the existing independent \x39E readers, and its frames are forgotten, so the trace never
LIST
)")

hits leak -n -i 'superpowers\|upgrade-ref\|kafka-compatibility\|CLAUDE.md\|EXECUTION.md'
SELF_LEAKS=$(printf '%b\n' "$(cat <<'LIST'
scripts/ci/sweeps.sh:hits leak -n -i '\x73uperpowers\\|\x75pgrade-ref\\|\x6bafka-compatibility\\|\x43LAUDE.md\\|\x45XECUTION.md'
LIST
)")
check leak "$(printf '%s\n%s\n' "$LEAKS" "$SELF_LEAKS")"
hits baseline-label -n -E "$LABEL" "$base" -- "${SHIPPED[@]}"
baseline=$(cat "$out/baseline-label.hits") || fatal "cannot read the alpha.9b labels"
[ -n "$baseline" ] || fatal "the alpha.9b label grep found nothing, so the comparison would be empty"
hits label -n -E "$LABEL" -- "${SHIPPED[@]}"
# Literal identities and the sweep's own exception text are enumerated, never exempted by path.
SELF_LABELS=$(printf '%b\n' "$(cat <<'LIST'
scripts/ci/gate.json:        "src/__tests__/equivalentApis.spec.js › R1\u00a7A1:send",
scripts/ci/gate.json:        "src/__tests__/equivalentApis.spec.js › R1\u00a7A1:initTransactions",
scripts/ci/gate.json:        "src/__tests__/equivalentApis.spec.js › R1\u00a7B1:poll",
scripts/ci/gate.json:        "src/__tests__/equivalentApis.spec.js › R1\u00a7B1:wakeup",
scripts/ci/gate.json:        "src/__tests__/equivalentApis.spec.js › R1\u00a7B1:seekToBeginning",
scripts/ci/gate.json:        "src/__tests__/equivalentApis.spec.js › R1\u00a7B1:listTopics",
scripts/ci/gate.json:        "src/__tests__/equivalentApis.spec.js › R1\u00a7B1:currentLag",
scripts/ci/gate.json:        "src/__tests__/equivalentApis.spec.js › R1\u00a7B2:max.poll.records",
scripts/ci/gate.json:        "src/__tests__/equivalentApis.spec.js › R1\u00a7B2:default.api.timeout.ms",
scripts/ci/gate.json:        "src/__tests__/equivalentApis.spec.js › R1\u00a7B2:enable.auto.commit",
scripts/ci/gate.json:        "src/__tests__/equivalentApis.spec.js › R1\u00a7A2:compression.type",
scripts/ci/gate.json:        "src/__tests__/equivalentApis.spec.js › R1\u00a7A1:partitionsFor",
scripts/ci/gate.json:        "src/__tests__/equivalentApis.spec.js › R2\u00a72:ManualFlushing",
scripts/ci/gate.json:        "src/__tests__/equivalentApis.spec.js › R1\u00a7B1:seek",
scripts/ci/gate.json:        "src/__tests__/equivalentApis.spec.js › R2\u00a73:OnOffsetsFetched",
scripts/ci/gate.json:        "src/__tests__/equivalentApis.spec.js › R2\u00a73:MaxConcurrentFetches",
scripts/ci/gate.json:        "src/producer/state/__tests__/accumulatorMemory.spec.js › Producer > the buffer > the bound on held records R2\u00a72:MaxBufferedRecords",
scripts/ci/sweeps.sh:LABEL='\x50hase [0-9]|\x54ask [0-9]|\\b9[A-F]\\b|\\bB[0-9]{2}\\b|\\bW[0-9]{2}[ab]?\\b|\\bD9[0-9]\\b|\x523-|\\bRS-[A-Z][0-9]|\x41mendment|\u00a7'
LIST
)")
check label "$(printf '%s\n%s\n%s\n' "$baseline" "$NEW_LABELS" "$SELF_LABELS")"
stale_exceptions label-new "$NEW_LABELS" "$out/label.hits"
hits test-only-export -n -E 'forTest|testOnly|__test' -- 'src/*.js' ':!*.spec.js' ':!*__tests__*' ':!*/fixtures/*'
check test-only-export ''
hits focused-or-failing -n -E '\b(it|test|describe)\.(failing|only)\(|\bf(it|describe)\(' -- src scripts testHelpers otel/test
# Private JavaScript methods are not focused Jest calls.
PRIVATE_METHODS=$(printf '%b\n' "$(cat <<'LIST'
src/telemetry/payload.js:      ...this.#\x66it(snapshot, cap),
src/telemetry/payload.js:  #\x66it(snapshot, cap) {
LIST
)")
check focused-or-failing "$PRIVATE_METHODS"
hits host-path -n -E "$(printf '\057var\057tmp|\057home\057uj|(^|[^-])p[9][a-f]-')" -- . ':!.gitignore' ':!.npmignore'
# Pinned fixture identities and the tarball path guard are not machine-local paths.
FIXTURE_IDENTITIES=$(printf '%b\n' "$(cat <<'LIST'
scripts/assertCleanTarball.sh:  if tar -xOf "$1" "package/docs/$guide.md" | grep -E '/\x76ar/tmp/|/\x68ome/[^ /]+/|\\.\x73uperpowers/'; then
scripts/e2e/telemetry/JavaTelemetryClient.java:                topic = "\x709f-tf1-java";
scripts/e2e/telemetry/fixtures/golden/vectors.json:    "nonzero": {"hex": "0ad2030a610a210a0c736572766963652e6e616d6512110a0f7039662d7466312d6e6f6e7a65726f0a140a0e686f73742e6370752e636f756e74120218080a100a0a666561747572652e6f6e120210010a120a05726174696f120921000000000000e03f100312ec020a1c0a117039662e6e6f6e7a65726f2e73636f70651205312e322e33200212690a296f72672e6170616368652e6b61666b612e7466312e6e6f6e7a65726f2e67617567652e646f75626c651a026d732a380a361915cd853dfe9c971721000000000000f83f3a110a05746f70696312080a066f72646572733a0f0a09706172746974696f6e1202180312420a266f72672e6170616368652e6b61666b612e7466312e6e6f6e7a65726f2e67617567652e696e741a0242792a140a121915cd853dfe9c9717312a00000000000000124c0a246f72672e6170616368652e6b61666b612e7466312e6e6f6e7a65726f2e73756d2e696e741a01313a210a1b1100002a36fe9c97171915cd853dfe9c971731010000000000200010021801124f0a276f72672e6170616368652e6b61666b612e7466312e6e6f6e7a65726f2e73756d2e646f75626c651a01733a210a1b1100002a36fe9c97171915cd853dfe9c9717215839b4c8d61cc840100118010a9c010a240a200a0c736572766963652e6e616d6512100a0e7039662d7466312d7365636f6e64100112740a0c0a0a7039662e7365636f6e6412640a216f72672e6170616368652e6b61666b612e7466312e7365636f6e642e67617567651a01312a3c0a1c1915cd853dfe9c97172100000000000008403a080a016b12030a01610a1c1916cd853dfe9c97172100000000000010403a080a016b12030a0162", "bytes": 628, "sha256": "2b6f920dbd1c4bb5ea54e0baa3330f3bce25cb9a925725163f5f9462df044c05", "semantic": {"resources":[{"attrs":[["service.name","string","\x709f-tf1-nonzero"],["host.cpu.count","int","8"],["feature.on","bool","true"],["ratio","double","0.5"]],"droppedAttributes":3},{"attrs":[["service.name","string","\x709f-tf1-second"]],"droppedAttributes":1}],"metrics":[{"res":0,"scope":0,"scopeName":"p9f.nonzero.scope","scopeVersion":"1.2.3","scopeDroppedAttributes":2,"name":"org.apache.kafka.tf1.nonzero.gauge.double","unit":"ms","kind":"GAUGE","points":[{"attrs":[["topic","string","orders"],["partition","int","3"]],"startNs":"0","timeNs":"1700000000123456789","flags":0,"valueKind":"AS_DOUBLE","asDouble":"1.5","asDoubleBits":"3ff8000000000000"}],"pointsInMessage":1},{"res":0,"scope":0,"scopeName":"p9f.nonzero.scope","scopeVersion":"1.2.3","scopeDroppedAttributes":2,"name":"org.apache.kafka.tf1.nonzero.gauge.int","unit":"By","kind":"GAUGE","points":[{"attrs":[],"startNs":"0","timeNs":"1700000000123456789","flags":0,"valueKind":"AS_INT","asInt":"42"}],"pointsInMessage":1},{"res":0,"scope":0,"scopeName":"p9f.nonzero.scope","scopeVersion":"1.2.3","scopeDroppedAttributes":2,"name":"org.apache.kafka.tf1.nonzero.sum.int","unit":"1","kind":"SUM","temporality":"AGGREGATION_TEMPORALITY_CUMULATIVE","monotonic":true,"points":[{"attrs":[],"startNs":"1700000000000000000","timeNs":"1700000000123456789","flags":0,"valueKind":"AS_INT","asInt":"9007199254740993"}],"pointsInMessage":1},{"res":0,"scope":0,"scopeName":"p9f.nonzero.scope","scopeVersion":"1.2.3","scopeDroppedAttributes":2,"name":"org.apache.kafka.tf1.nonzero.sum.double","unit":"s","kind":"SUM","temporality":"AGGREGATION_TEMPORALITY_DELTA","monotonic":true,"points":[{"attrs":[],"startNs":"1700000000000000000","timeNs":"1700000000123456789","flags":0,"valueKind":"AS_DOUBLE","asDouble":"12345.678","asDoubleBits":"40c81cd6c8b43958"}],"pointsInMessage":1},{"res":1,"scope":0,"scopeName":"p9f.second","scopeVersion":"","scopeDroppedAttributes":0,"name":"org.apache.kafka.tf1.second.gauge","unit":"1","kind":"GAUGE","points":[{"attrs":[["k","string","a"]],"startNs":"0","timeNs":"1700000000123456789","flags":0,"valueKind":"AS_DOUBLE","asDouble":"3.0","asDoubleBits":"4008000000000000"},{"attrs":[["k","string","b"]],"startNs":"0","timeNs":"1700000000123456790","flags":0,"valueKind":"AS_DOUBLE","asDouble":"4.0","asDoubleBits":"4010000000000000"}],"pointsInMessage":2}]}},
scripts/e2e/telemetry/fixtures/golden/vectors.json:    "zero": {"hex": "0a9e020a200a1e0a0c736572766963652e6e616d65120e0a0c7039662d7466312d7a65726f12f9010a130a0e7039662e7a65726f2e73636f706512013012380a266f72672e6170616368652e6b61666b612e7466312e7a65726f2e67617567652e646f75626c651a01312a0b0a0921000000000000000012350a236f72672e6170616368652e6b61666b612e7466312e7a65726f2e67617567652e696e741a01312a0b0a0931000000000000000012370a216f72672e6170616368652e6b61666b612e7466312e7a65726f2e73756d2e696e741a01313a0f0a093100000000000000001001180112380a246f72672e6170616368652e6b61666b612e7466312e7a65726f2e73756d2e646f75626c651a01313a0d0a092100000000000000001002", "bytes": 289, "sha256": "5bb85252a816d643e1b91673f92147da3b627e18066633eae7b895617cb28612", "semantic": {"resources":[{"attrs":[["service.name","string","\x709f-tf1-zero"]],"droppedAttributes":0}],"metrics":[{"res":0,"scope":0,"scopeName":"p9f.zero.scope","scopeVersion":"0","scopeDroppedAttributes":0,"name":"org.apache.kafka.tf1.zero.gauge.double","unit":"1","kind":"GAUGE","points":[{"attrs":[],"startNs":"0","timeNs":"0","flags":0,"valueKind":"AS_DOUBLE","asDouble":"0.0","asDoubleBits":"0"}],"pointsInMessage":1},{"res":0,"scope":0,"scopeName":"p9f.zero.scope","scopeVersion":"0","scopeDroppedAttributes":0,"name":"org.apache.kafka.tf1.zero.gauge.int","unit":"1","kind":"GAUGE","points":[{"attrs":[],"startNs":"0","timeNs":"0","flags":0,"valueKind":"AS_INT","asInt":"0"}],"pointsInMessage":1},{"res":0,"scope":0,"scopeName":"p9f.zero.scope","scopeVersion":"0","scopeDroppedAttributes":0,"name":"org.apache.kafka.tf1.zero.sum.int","unit":"1","kind":"SUM","temporality":"AGGREGATION_TEMPORALITY_DELTA","monotonic":true,"points":[{"attrs":[],"startNs":"0","timeNs":"0","flags":0,"valueKind":"AS_INT","asInt":"0"}],"pointsInMessage":1},{"res":0,"scope":0,"scopeName":"p9f.zero.scope","scopeVersion":"0","scopeDroppedAttributes":0,"name":"org.apache.kafka.tf1.zero.sum.double","unit":"1","kind":"SUM","temporality":"AGGREGATION_TEMPORALITY_CUMULATIVE","monotonic":false,"points":[{"attrs":[],"startNs":"0","timeNs":"0","flags":0,"valueKind":"AS_DOUBLE","asDouble":"0.0","asDoubleBits":"0"}],"pointsInMessage":1}]}},
scripts/e2e/telemetry/telemetryWireProbe.js:  const body = Buffer.concat([compactStr('\x709f-tf1-wire-probe'), compactStr('0'), Buffer.from([0])])
scripts/e2e/telemetry/telemetryWireProbe.js:  const r = new Reader(await exchange(c, frame(header(18, 4, 1, '\x709f-tf1-wire-probe'), body)))
LIST
)")
check host-path "$FIXTURE_IDENTITIES"
hits todo -n -E '\b(TODO|FIXME|XXX|HACK)\b' -- "${SHIPPED[@]}"
SELF_TODOS=$(printf '%b\n' "$(cat <<'LIST'
scripts/ci/sweeps.sh:# label grep against alpha.9b, test-only exports, focused or failing tests, host paths, \x54ODO text and leftover .orig/.rej files. Each
scripts/ci/sweeps.sh:src/broker/index.js:    // \x54ODO: validate topics not null/empty
scripts/ci/sweeps.sh:src/broker/index.js:    // \x54ODO: validate groupId, mandatory
scripts/ci/sweeps.sh:hits todo -n -E '\\b(\x54ODO|\x46IXME|\x58XX|\x48ACK)\\b' -- "${SHIPPED[@]}"
scripts/ci/checkNodeTests.js:      const end = /^( *)(not ok|ok) (\\d+) - (.*?)(?: # (SKIP|\x54ODO)\\b.*)?$/.exec(line)
scripts/ci/checkNodeTests.js:          failed: end[2] !== 'ok' || end[5] === '\x54ODO',
scripts/ci/__tests__/checkNodeTests.spec.js:    ['todo test', report(tap({ skip: ' # \x54ODO' }))],
LIST
)")
check todo "$(printf '%s\n%s\n' "$TODOS" "$SELF_TODOS")"
stale_exceptions todo "$TODOS" "$out/todo.hits"
leftovers leftover '*.orig' '*.rej' '*.swp' '*~'
check leftover ''
[ "$failed" = 0 ] && echo "sweeps: clean" >> "$report" || echo "sweeps: FAIL" >> "$report"
tail -n 1 "$report"
[ "$failed" = 0 ]
