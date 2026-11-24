# Releasing

This repository ships two npm packages:

| Package | Source | Published |
|---|---|---|
| `@countly/kafkajs` | repository root | on every `v*` tag (`next` for a release candidate, `latest` for a final version) |
| `@countly/opentelemetry-instrumentation-kafkajs` | `otel/` (its own `package.json` and lockfile) | only with the final release, on the `v3.0.0` tag; never inside the main tarball |

A release is a `v*` tag on a commit of `master`. Pushing the tag starts `.github/workflows/release.yml`. The workflow checks the tag, packs and checks both tarballs and, after a person approves, publishes them to npm with provenance. Nothing publishes without that approval.

Supported brokers: Apache Kafka 4.0–4.4.

Each step below is an action, its expected result, and what to do when it fails.

## The release workflow

```
git push origin v3.0.0-rc.1
        |
        v
  +----------+     +------------+     +-------------------------------+
  |  verify  | --> |    pack    | --> |  publish (main package)       |
  | rules    |     | main .tgz  |     |  environment: npm-release     |
  | 1-11     |     | clean, SRI |     |  -> a person approves         |
  +----------+     +------------+     +-------------------------------+
                         |
                         v
                   +------------+     +-------------------------------+
                   | pack-otel  | --> |  publish-otel (v3.0.0 only)   |
                   | otel .tgz  |     |  needs publish and pack-otel  |
                   | guard,smoke|     |  environment: npm-release     |
                   +------------+     +-------------------------------+
```

| Job | Does | Stops the release when |
|---|---|---|
| `verify` | Runs `scripts/release/verify.js` on the tag (rules below). Outputs the version, dist-tag, commit and OTel version. | Any rule fails. |
| `pack` | Checks the checkout is the verified commit, runs `npm ci --ignore-scripts`, `npm pack` once and `scripts/assertCleanTarball.sh`, records the tarball's SRI (`sha512-…`) in the job summary and uploads artifact `release-tarball`. | Wrong commit, or the tarball is not clean. |
| `pack-otel` | Every tag. Packs `otel/` once, runs `scripts/assertCleanOtelTarball.sh`, smokes it with `scripts/otelPackSmoke.sh` against the downloaded main tarball (never repacked), runs `verify.js --post-pack` (rules 9 and 10), writes the OTel SRI to the job summary and uploads artifact `otel-tarball`. | Any check fails. |
| `publish` | Waits for approval of `npm-release`. Checks the tarball's name and version and npm 11.5.1 or newer, writes its SRI to the job summary ("Release tarball"), then publishes **that tarball** with `--provenance --ignore-scripts`. | Mismatch, old npm, or the approver rejects it. |
| `publish-otel` | Only on `v3.0.0`. Waits for approval of `npm-release`. Checks the `otel-tarball` name, version and npm, writes its SRI ("OTel release tarball"), stops if that version is already on npm, then publishes that archive under `latest` with provenance. | Mismatch, old npm, the version already exists, or the approver rejects it. |

The workflow has no npm token. It publishes through trusted publishing (OIDC). The file name `release.yml` is registered at npm for both packages: do not rename it.

### What `verify` checks (exact SHA)

All checks are about the exact commit the tag points at. A run on a branch or a pull request proves the code, not the release.

| Rule | Must hold |
|---|---|
| 1 | The tag is `v` + the `package.json` version, and the version is `X.Y.Z` or `X.Y.Z-pre`. |
| 2 | The tagged commit is on `origin/master`. |
| 3 | `CHANGELOG.md` has a `## <version>` heading (a date or other text may follow after a space). |
| 4 | The newest **push** run on `master` of `ci.yml` and of `gate.yml` for that commit is `completed/success`. Dispatch and pull-request runs do not count. |
| 5 | Some `weekly.yml` run for that commit is `completed/success`. |
| 6 | `otel/package.json` names `@countly/opentelemetry-instrumentation-kafkajs` and has a strict semver version. |
| 7 | Its `@countly/kafkajs` peer range includes the tagged version. |
| 8 | On `v3.0.0` only: that OTel version is not on npm yet. |
| 9 | (`pack-otel`) Each tarball holds its package's name and the checkout's version. |
| 10 | (`pack-otel`) The main tarball's SRI equals the one `pack` recorded. |
| 11 | Some `workflow_dispatch` run of `memory-gates.yml` for that commit is green with the full selection and long-window replicates 1, 2 and 3. Every tag, rc or final, needs its own run. |

`verify` reads every page of the run lists (100 runs per page, 10 pages). If page 10 is still full it fails and does not judge a truncated list.

## One-time setup

### The `npm-release` Environment

Create it in GitHub (Settings → Environments → New environment) **before any `v*` tag is pushed**. A workflow that names a missing Environment creates it with no protection.

| Setting | Value |
|---|---|
| Name | `npm-release` |
| Required reviewers | The release owner |
| Prevent self-reviews | Off |
| Deployment branches and tags | Selected, tag pattern `v*` |
| Allow administrators to bypass configured protection rules | **Off** |

Read the settings back before the first tag, before each trusted-publisher step and before every release tag. Expected: the first command shows the reviewer, administrator bypass off and custom branch and tag policies on; the second lists exactly one tag policy, `v*`. A mismatch stops the step until it is corrected.

```sh
gh api repos/Countly/kafkajs/environments/npm-release
gh api --paginate repos/Countly/kafkajs/environments/npm-release/deployment-branch-policies
```

### Repository settings

| Setting | Expected | Read back |
|---|---|---|
| Secret scanning and push protection | both `enabled` | `gh api repos/Countly/kafkajs --jq .security_and_analysis` |
| Actions default workflow permissions | `read`; Actions cannot create or approve pull requests | `gh api repos/Countly/kafkajs/actions/permissions/workflow` (`default_workflow_permissions: "read"`, `can_approve_pull_request_reviews: false`) |

A mismatch blocks the release candidate tag until it is corrected in Settings.

### npm account

1. The publisher's npm account can publish to the `@countly` scope. Create the `@countly` organisation if it does not exist.
2. For each manual bootstrap publish (main rc.0 below, and the OTel bootstrap if npm needs one), create a short-lived granular token. Revoke it right after that publish.

## First publish of the main package

`@countly/kafkajs` does not exist on npm until the first publish, and trusted publishing cannot be set up for a package that does not exist. So rc.0 is published by hand, and CI publishes from rc.1 on.

| Step | Who | Action | Expected | On failure |
|---|---|---|---|---|
| U1 | Release owner | Create the `npm-release` Environment as above. | The read-back matches. | Correct the settings; read back again. |
| U1b | Release owner | Negative control: push the throwaway tag `v0.0.0-release-check`, then delete it: `git push origin --delete v0.0.0-release-check`. | The `release.yml` run shows `verify` failing and no deployment. | Stop. Fix the workflow or Environment before any real tag. |
| U2 | Release owner | Confirm npm access to `@countly` and create the short-lived token. | Access confirmed. | Fix the npm account first. |
| U3 | Release owner | Checked bootstrap of `3.0.0-rc.0` (below). | Every check prints its expected result. | Stop at the first failure; nothing is published. |
| U4 | Release owner | Immediately before U5: npmjs.com → `@countly/kafkajs` settings → Trusted Publisher → GitHub Actions. Owner `Countly`, repository `kafkajs`, workflow `release.yml`, environment `npm-release`. **Allowed actions: tick `npm publish`. Leave `npm dist-tag` off.** | Saved. | npm does not check it on save; a wrong value shows as a refused publish in U5. |
| U5 | Release owner | Push tag `v3.0.0-rc.1` on a candidate commit that has passed the release gate up to box 9. Approve the `npm-release` deployment. | `publish` is green. | See "When the release run fails". |
| U6 | Anyone | Run "After every CI publish" for `@countly/kafkajs@3.0.0-rc.1`. | Both checks OK. | The release is blocked. |
| U6b | Release owner (optional) | `npm dist-tag add @countly/kafkajs@3.0.0-rc.1 latest` while logged in. | `latest` moves from rc.0 to rc.1. | — |

### U3: checked bootstrap of rc.0

First, **accept in writing that npm may also point `latest` at rc.0**: the first publish of a new package gets `latest` even with `--tag next`. If that is not acceptable, stop: nothing is published.

From a clean checkout of the tag `3.0.0-rc.0` (the candidate commit), with the token from U2:

```sh
npm ci --ignore-scripts
tgz=$(npm pack --silent)                     # pack once; every step below uses this file
./scripts/assertCleanTarball.sh "$tgz"       # expected: exit 0
./scripts/packSmoke.sh "$tgz"                # smokes exactly this file, never repacks; expected: exit 0
SRI=$(./scripts/sri.sh "$tgz"); echo "$SRI"  # record this sha512-… value
npm publish "./$tgz" --access public --tag next --ignore-scripts
test "$(npm view @countly/kafkajs@3.0.0-rc.0 dist.integrity)" = "$SRI" && echo "integrity OK"
npm view @countly/kafkajs dist-tags --json   # record it
```

Then revoke the token. rc.0 has no provenance: it and the OTel bootstrap below are the only manual publishes. If a check fails before the publish, fix the cause on a new candidate. If the read-back does not print `integrity OK` (repeat it after a minute or two first, the registry can lag), the release is blocked: do not publish again over it.

Why U4 is late: npm requires a new trusted-publisher configuration to complete its first successful publish within 2 days, and does not check the configuration when you save it. Configurations created after 2026-09-03 allow `npm stage publish` by default, so a direct `npm publish` is refused unless it is ticked.

Tag names:

- rc.0 is the one tag without `v` (`3.0.0-rc.0`). It is pushed by hand and `release.yml` does not run for it.
- CI tags start at `v3.0.0-rc.1`.
- Until 3.0.0 is released, `latest` may also point at a release candidate. Install release candidates by exact version.

## The OpenTelemetry package

`F` is the version in `otel/package.json` on the `v3.0.0` commit (currently `0.1.1`). It does not change at release candidates. Every `v*` run packs, guards and smokes the OTel tarball and uploads it as `otel-tarball`, but only `v3.0.0` publishes it.

| Step | Who | Action | Expected | On failure |
|---|---|---|---|---|
| OT1 | Release owner | **Only if npm needs a manual first publish to set up trusted publishing for the new name.** Bootstrap `B` = `F-rc.0` (currently `0.1.1-rc.0`), checked procedure below. | `integrity OK`. | Stop; nothing more is published until the cause is fixed. |
| OT2 | Release owner | Trusted Publisher for `@countly/opentelemetry-instrumentation-kafkajs`, same values as U4 (`npm publish` ticked, `npm dist-tag` off), right before pushing `v3.0.0`. | Saved. | A wrong value shows as a refused `publish-otel`. |
| OT3 | Release owner | Push `v3.0.0` (release gate box 13). `publish-otel` publishes F with provenance. | `publish-otel` is green. | See "When the release run fails". |

### OT1: checked bootstrap of B

In a separate clean checkout of the `v3.0.0` candidate, with a short-lived token. The final tag's checkout keeps F; only this bootstrap checkout changes:

```sh
npm ci --ignore-scripts
main=$(npm pack --silent)                                        # the main tarball to smoke against
./scripts/assertCleanTarball.sh "$main"
cd otel && npm version 0.1.1-rc.0 --no-git-tag-version && cd ..  # B, in otel/package.json and its lockfile
(cd otel && npm ci --ignore-scripts)
otel=$(cd otel && npm pack --silent --pack-destination ..)       # pack once
./scripts/assertCleanOtelTarball.sh "$otel"                      # expected: exit 0
./scripts/otelPackSmoke.sh "$otel" "$main"                       # expected: exit 0
SRI=$(./scripts/sri.sh "$otel"); echo "$SRI"                     # record it
npm publish "./$otel" --access public --tag next --ignore-scripts
test "$(npm view @countly/opentelemetry-instrumentation-kafkajs@0.1.1-rc.0 dist.integrity)" = "$SRI" && echo "integrity OK"
```

Revoke the token. B has no provenance and does not replace the final CI publication of F.

## After every CI publish

Run for each name and version a `release.yml` run published: the main package on every tag, and the OTel package at F on `v3.0.0`. The manual bootstraps (rc.0 and B) have no workflow run or provenance; their read-back is the one in their own procedure.

Take the `sha512-…` value from that run's `publish` job summary ("Release tarball") or `publish-otel` job summary ("OTel release tarball").

```sh
NAME=@countly/kafkajs          # or @countly/opentelemetry-instrumentation-kafkajs
VERSION=3.0.0-rc.1             # or F
TAG=v3.0.0-rc.1                # the tag that published it
EXPECTED='sha512-<value from the job summary>'

# 1. The registry's integrity equals the job summary.
test "$(npm view "$NAME@$VERSION" dist.integrity)" = "$EXPECTED" && echo "integrity OK"

# 2. The provenance names this repository's release.yml, the tag and its commit, and its subject is the archive.
npm view "$NAME@$VERSION" dist.attestations.url | xargs curl -fsS | EXPECTED="$EXPECTED" TAG="$TAG" \
  SHA="$(git rev-parse "$TAG^{commit}")" node -e '
const { attestations } = JSON.parse(require("fs").readFileSync(0, "utf8"))
const provenance = attestations.find((a) => a.predicateType === "https://slsa.dev/provenance/v1")
if (!provenance) { console.error("no provenance attestation"); process.exit(1) }
const { subject, predicate } = JSON.parse(Buffer.from(provenance.bundle.dsseEnvelope.payload, "base64"))
const hex = Buffer.from(process.env.EXPECTED.replace(/^sha512-/, ""), "base64").toString("hex")
if (subject.length !== 1 || subject[0].digest.sha512 !== hex) { console.error("provenance subject does not match"); process.exit(1) }
const { workflow } = predicate.buildDefinition.externalParameters
const commit = predicate.buildDefinition.resolvedDependencies[0].digest.gitCommit
if (workflow.repository !== "https://github.com/Countly/kafkajs" || workflow.path !== ".github/workflows/release.yml" ||
    workflow.ref !== `refs/tags/${process.env.TAG}` || commit !== process.env.SHA) {
  console.error("provenance names another build:", JSON.stringify({ workflow, commit })); process.exit(1)
}
console.log("provenance OK")
'

# 3. npm verifies the signatures and the attestation of the installed copy.
(dir=$(mktemp -d) && cd "$dir" && npm init -y >/dev/null && npm install --ignore-scripts "$NAME@$VERSION" && npm audit signatures)
```

Expected: `integrity OK`, `provenance OK`, and `npm audit signatures` reporting verified registry signatures and verified attestations with no invalid or missing one.

A refusal by npm, a missing provenance, a failed signature or a mismatch blocks the release. The registry can lag a minute or two after a publish: repeat a failed read before you treat it as a mismatch. Then investigate and correct the cause. Do not repack, publish from a directory, fall back to a token or waive the provenance.

Optional after U5 (and after OT3 for the OTel package): in npm, turn on "require two-factor authentication and disallow tokens". Trusted publishing keeps working.

## Release gate

Work top to bottom. Each box has its own evidence; keep the run URLs. **C** is the candidate commit (box 5). Any source, doc or tooling change after C makes a new candidate, and every box from 5 on is done again for it.

- [ ] **1. Alpha work merged and tagged.** Expected: tags `3.0.0-alpha.9e`, `3.0.0-alpha.9f`, `3.0.0-alpha.9g` and `3.0.0-alpha.10` exist on `origin` (`git ls-remote --tags origin '3.0.0-alpha.*'`). Missing: tag the merged commit first.
- [ ] **2. Settings and policy.** The `npm-release` Environment and repository settings read back as in "One-time setup". The release owner confirms the private reporting contact in `SECURITY.md` (`security@count.ly`). Mismatch: correct it before box 5.
- [ ] **3. `QUARANTINE.md` lists no quarantined test**, or the release owner has accepted each listed test in writing.
- [ ] **4. Dependency advisories.** Run the four `npm audit` commands in "Dependency advisories". Expected: no production advisory, and every development advisory is in that table. A production advisory blocks the release; a new development one is fixed or added to the table with its reachability (that change is a new candidate).
- [ ] **5. Candidate commit C on `master`.** Version `3.0.0-rc.N` in `package.json` and both version fields of `package-lock.json`, a `## 3.0.0-rc.N` CHANGELOG heading and the README status text, set before any evidence below. `otel/package.json` keeps F. Check: `node -e "const p=require('./package.json'),l=require('./package-lock.json');process.exit(p.version===l.version&&l.packages[''].version===p.version?0:1)"` exits 0.
- [ ] **6. CI on C.** The `ci.yml` and `gate.yml` push runs on `master` for C are green (Kafka 4.4.0 included), and `weekly.yml` dispatched on `master` at C is green (`gh workflow run weekly.yml --ref master`). The decoder performance check in `gate.yml` passes; read its job summary: rows accepted as host noise are listed there, each with its reason. Red: fix on a new candidate; a runner or network failure is rerun once (see "When the release run fails").
- [ ] **7. Memory gates on C.** While `master` is at C: `gh workflow run memory-gates.yml --ref master` (no `only`, `replicates` 3). Expected: `memory-gates-verdict` green with long-window replicates 1, 2 and 3 (rule 11). A partial selection or fewer replicates never counts. Red: investigate the failing job; a product fix is a new candidate.
- [ ] **8. Benchmark report on C.** `gh workflow run bench.yml --ref master`, then read `bench-report.md` (artifact or job summary). No threshold gates the release: read every row, each `error`, `invalid` or `omitted` row with its reason, and the adaptive partitioner rows (`stickyBalance-*`), then the release owner decides. A regression the owner will not accept is fixed on a new candidate.
- [ ] **9. Mutation report on C.** `gh workflow run mutants.yml --ref master -f ref=<C>`, then read `mutation-report.md` (artifact `mutation-report` or job summary). It gates nothing by itself. Expected: the live mutants ran with guard evidence (not `invalid` or `not-run`), or the release owner accepted their exclusion in writing; the owner reads every survivor (see [Testing](docs/Testing.md#mutation-report)) and decides.
- [ ] **10. Final 12-hour soak on C** on the release owner's own machine or a dedicated machine provisioned for the run ("Final soak" below). Expected: core report `ACCEPTED`, all seven rows PASS. Anything else: triage as the runbook says; a product fix is a new candidate.
- [ ] **11. Publish the candidate.** rc.0 by hand (U3), later ones by pushing `v3.0.0-rc.N` (U5), then "After every CI publish" for the main package. The same release run's `pack-otel` job is green and its `otel-tarball` artifact holds F.
- [ ] **12. Countly canary.** Before it starts, record the rollback pin (the previous client and instrumentation versions and the set of workarounds in use) and the stop conditions. Install `@countly/kafkajs@3.0.0-rc.N` by exact version (never by tag) and the OTel tarball from the `otel-tarball` artifact of the same release run (`gh run download <run id> -n otel-tarball`). Record both installed versions and integrities (the `version` and `integrity` of both packages in the canary's `package-lock.json`, and `./scripts/sri.sh <otel tgz>`). Expected: `plugins/kafka` green. A stop condition met: roll back to the pin and fix on a new candidate.
- [ ] **13. Release commit.** Only the version, the lockfile and the CHANGELOG heading (`## 3.0.0 — <date>`) differ from the canaried rc (`git diff --stat v3.0.0-rc.N..HEAD`). On that exact SHA: `ci.yml` and `gate.yml` push runs, a dispatched `weekly.yml` and a full `memory-gates.yml` dispatch are green (rules 4, 5 and 11 need each for the tagged commit). Anything else changed: cut a new rc and repeat from box 5.
- [ ] **14. The release owner says go.** OT1 if npm needs it, then OT2. Push `v3.0.0` and approve both deployments (`publish`, then `publish-otel`). Run "After every CI publish" for `@countly/kafkajs@3.0.0` and for the OTel package at F. Expected: all checks OK and `latest` is 3.0.0 for the main package and F for the OTel package (`npm view <name> dist-tags --json`). Publication of both packages is complete only after these read-backs; the bootstraps and the rc runs never close it.

Useful read-only commands:

```sh
# Push runs on master for a commit (replace the SHA)
gh run list --branch master --json headSha,conclusion,event,workflowName | node -e '
const sha = process.argv[1]
const runs = JSON.parse(require("fs").readFileSync(0, "utf8")).filter((r) => r.headSha === sha)
console.table(runs.map(({ workflowName, event, conclusion }) => ({ workflowName, event, conclusion })))
' "$(git rev-parse HEAD)"

# dist-tags on the registry
npm view @countly/kafkajs dist-tags --json
```

### Release commit rules

- The version is set in `package.json` and in the lockfile (`npm version <version> --no-git-tag-version`).
- `CHANGELOG.md` has a `## <version>` heading for it. `verify` rule 3 fails without one.
- Between the canaried rc and the release commit, nothing else changes, `otel/package.json` included. If something else must change, cut a new rc and canary it again.

## Flaky tests

A test that fails and then passes on a rerun of the same SHA is either fixed or quarantined with an entry in `QUARANTINE.md` before the next merge to `master`. The release gate (box 3) needs that file to list no quarantined test, or each one accepted.

A test that needs a missing broker feature on 4.0.2 is skipped only with capture evidence; a product bug is fixed.

## When the release run fails

| Situation | What to do |
|---|---|
| `verify` fails | Read the rule number in the log. Rules 4, 5 or 11 usually mean a missing or red run for that SHA: get it green (rerun the push run, dispatch `weekly.yml` or `memory-gates.yml`), then rerun the failed `release.yml` run. Rules 1 to 3, 6 and 7 need a new commit, so a new tag. Rule 8: see "OTel version already on npm". |
| `pack` or `pack-otel` fails a check | The tarball is wrong: fix the cause on a new candidate. Never publish a tarball that failed a check. |
| The run fails for a reason outside the code (a runner or network problem) | Rerun the failed jobs of that run. Telemetry inputs are fetched from two pinned sources with retries and a cache; nothing is downloaded by hand. Re-pushing an unchanged tag triggers nothing. |
| `publish` or `publish-otel` is rejected at approval | The release owner starts it again with "Re-run failed jobs" on the same run. |
| `publish` succeeded, `publish-otel` failed before F reached npm | Correct the cause, then "Re-run failed jobs" on the same run, which reuses the same `otel-tarball` artifact. |
| OTel version already on npm (rule 8 or `publish-otel` stops) | Stop rerunning publication. Run "After every CI publish" for both packages and record the run and attempt that published F. An existing version or a failed job proves neither success nor a need to publish again. A missing or mismatched check keeps the release blocked. Do not repack, publish F by hand, fall back to a token or re-push the tag. |
| Provenance or integrity check fails | See "After every CI publish". The release is blocked. |

A tag is never moved or deleted after a publish. If a published version is wrong, publish a new version.

## Dependency advisories

Neither package's locked production graph has an `npm audit` advisory (checked 2026-11-18 for `package-lock.json` and `otel/package-lock.json`, with and without `--omit=dev`). The root lockfile still has these development-only advisories, for which no compatible fix exists:

| Package (locked copies) | Advisory | Reached through | Why it can wait |
|---|---|---|---|
| `picomatch` 2.3.2, 4.0.7 | GHSA-fc5q-39xc-vjpf (ReDoS through repeated extglobs) | Jest file discovery and matching, `eslint-plugin-jest` | No patched release exists. Only the repository's own glob patterns reach it; a pull request that changes them already runs its own code in CI. |
| `minimatch` 10.2.6, 9.0.9 | GHSA-g3hv-75wg-m249 (pattern amplification DoS) | Jest discovery and coverage (`glob`, `test-exclude`) | The fix is minimatch 11, a major that Jest 30 does not accept. Same reachability as above. |
| `glob` 13.0.7, 10.5.0 | GHSA-3g3m-2fjr-6pw4 (walk continues after an AbortSignal) | Jest config, reporters, runtime and coverage | Nothing here cancels a glob walk with an AbortSignal. 13.0.7 contains the fix although the advisory still lists it; 10.5.0 comes from `test-exclude` 7, which Jest 30 requires. |

Before a release candidate, run `npm audit --package-lock-only` and `npm audit --package-lock-only --omit=dev` at the root and in `otel/`. A production advisory blocks the release. A new development advisory is fixed by a lockfile refresh when a compatible release exists, or added to this table with its reachability.

## Benchmarks

`bench.yml` is a manual workflow. It runs the rows of `scripts/benchmarks/manifest.json` on Kafka 4.4.0 for the packed fork and, through the public API, for kafkajs 2.2.4. It has no thresholds. Start it with `gh workflow run bench.yml --ref master`, then read `bench-report.md` from the run's artifacts (`bench-report`) or its job summary. A benchmark that fails shows as an error row; the report is uploaded either way.

## Final soak

The acceptance run is 12 hours from the readiness barrier, on C, on the release owner's own machine or a dedicated machine provisioned for the run (for example a dedicated cloud node); either way it must meet the same quiet-host floors. The verdict is the core checker's report (`scripts/evidence/finalSoakCore.js`): seven rows K1 to K7 and one `CORE VERDICT` line. A smoke or a rehearsal is never `ACCEPTED`; `NOT ACCEPTED` with no FAIL row is its expected verdict. [The final soak runbook](docs/FinalSoakRunbook.md) describes the dumps, the checker, what each row proves and how to triage a heap (K5) FAIL.

A 12-hour run cannot show what a 24-hour run could: a defect whose first evidence appears after hour 12 (a leak or a lost, duplicated or late record that starts only then) goes unseen.

Each run of `bash scripts/soak/finalSoakNode.sh <C> <smoke|rehearsal|acceptance>` writes `<artifact>.tar.gz`, the receipt `<artifact>.runbook.json`, the log `<artifact>.runbook.log` and the core report `<artifact>.core.txt`; once the driver stage has run it also writes `<artifact>.summary.json`. The receipt is rewritten before every stage after the driver with that stage as `pendingStage`, so a hung stage still leaves the driver's status and the stage on record.

**K5, the heap row.** Every subject process writes a heap snapshot after each hourly quiet boundary, so an acceptance run gives each process 12 boundaries (h00 to h11); the endpoint compares the medians of the last three and the first three. K5 judges every live heap byte of those snapshots except V8 compiled code, against an endpoint bound and a slope bound. On an acceptance run the endpoint bound is 898,779 bytes: the matrix's 2 MiB (2,097,152 bytes) set for the 21 hours between the endpoint medians of a 24-hour run, scaled to the 9 hours of a 12-hour run, so the same steady growth per hour (about 100 KB) still fails; a rehearsal keeps the 2 MiB in bytes. Whole-process `heapUsed` is printed as information only. A missing, late or unconfirmed snapshot makes K5 INCONCLUSIVE, never PASS. The seed process leaks on purpose: its judged heap must exceed 2 MiB in the same run, or K5 is INCONCLUSIVE.

**Before.**

1. Dispatch the `Final soak smoke (diagnostic)` workflow with `mode: rehearsal` on C. Expected core report: K1 to K4, K6 and K7 PASS, K5 judged on three boundaries, `NOT ACCEPTED`. A K5 FAIL is triaged as the runbook says before the acceptance run.
2. Prepare the machine: a clean separate checkout at the full SHA of C; Node 22, 24 or 26; Docker Compose; the Kafka 4.4.0 image; a free `kafkajs` Compose project and ports 19092/19095/19098 (the runbook stops and restarts brokers and removes the project's volumes); the pinned reporter inputs (`TF1_CACHE=<dir> bash scripts/e2e/telemetry/fetch.sh`, which verifies every file's SHA-256; then set `TELEMETRY_JAVA_CACHE=<dir>`).
3. Disk: set `FINAL_SOAK_ARTIFACTS` outside the checkout on a volume with at least 200 GiB free (the preflight floor). The stream can reach about 15 GiB (an estimate made for 24 hours; a 12-hour run writes about half), raw share wire about 2 GiB and the heap snapshots about 3 to 5 GB (the run's disk monitor keeps 16 GiB free for them); keep room for the tarball and copies.
4. Pause all other workloads for the whole run. The preflight's quiet-host check ignores only the processes inside this run's own broker containers; any other `java`, Kafka or test process fails it. Run it in the host's pid namespace: the check passes only when the process census sees all three broker container roots (the pid Docker reports, confirmed by its cgroup), and otherwise fails with "the process census cannot see the host", recorded under `censusView` in `preflight.json`. Leave `FINAL_SOAK_DIAGNOSTIC` unset (acceptance refuses it). `FINAL_SOAK_COMPANIONS` only archives companion files; the checker does not judge them.
5. Run `bash scripts/soak/finalSoakNode.sh <C> smoke` in a persistent terminal. Expected: preflight confirms the exact SHA and clean checkout, the image digests, three brokers, finalized `share.version`, the share group reset read-back, the reporter and decoded telemetry preflight, and the host floors (at least 12 GiB available memory, 200 GiB free disk, one-minute load at most 3); core report K1 to K4, K6 and K7 PASS, K5 INCONCLUSIVE (one boundary), `NOT ACCEPTED`. A preflight failure names the floor: fix the machine and run the smoke again.
6. Run the same command with `acceptance`. A run whose expected offers exceed 80% of `traffic.maxOffers` refuses to start.

**During.** Keep the runbook alive through cleanup and archiving. `<artifact>/heartbeat` is rewritten every 60 s as `<time> stream-bytes=<n> unchanged-s=<s>`: a growing stream is progress, a rising `unchanged-s` is a stall. The runbook stops the driver itself (TERM, then KILL after 60 s, named in `<artifact>.watchdog`) past its limit (about 13 h; `node scripts/soak/finalSoak.js --driver-limit --mode acceptance --out <dir>` prints it) or after 15 minutes with no stream byte. Watch available memory (4 GiB abort floor), the disk reserve and the client log summaries. If the run stalls, keep the log and partial files and let the watchdog end it; the run is then not acceptance evidence.

**After.**

1. Expected log line: `core checker verdict: ACCEPTED (exit status 0), report <file>`. Expected receipt: no `failedStage`, exit status 0; no `<artifact>/incomplete.json` and no `<artifact>.watchdog`.
2. Replay from the checkout of C: `node scripts/evidence/finalSoakCore.js <artifact dir>`. Expected: the header's tree SHA is C, all seven rows PASS, `ACCEPTED`, exit status 0.
3. Keep the tarball, runbook log, receipt, summary and core report together (and the Compose log on a failure).
4. Record every FAIL or INCONCLUSIVE row, the NOT JUDGED items, the share drain's unresolved-accept count (evidence only), the expected refusals and any log truncation.

Any verdict other than `ACCEPTED`: box 10 is open. Triage with the runbook; a product fix is a new candidate and a new acceptance run.

Known limits a green run does not cover: recovery handler and commit pairing, share recovery settlement, recreation pause/resume and assignment coverage, regex consumption, per-probe delivery on auxiliary topics and the last recreated incarnations' final drain.
