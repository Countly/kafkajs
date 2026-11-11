# Releasing

A release is a `v*` tag on a commit of `master`. Pushing the tag starts `.github/workflows/release.yml`. The workflow checks the tag, builds the tarball and, after a person approves, publishes that tarball to npm with provenance. Nothing publishes without that approval.

Supported brokers: Apache Kafka 4.0–4.4.

## The release workflow

```
git push origin v3.0.0-rc.1
        |
        v
  +-----------+     +----------+     +----------------------------+
  |  verify   | --> |   pack   | --> |          publish           |
  | (rules 1-5|     | npm ci   |     | environment: npm-release   |
  |  see below)|    | npm pack |     |  -> a person approves      |
  +-----------+     | clean    |     | npm publish <tarball>      |
   outputs:         | check    |     |  --provenance --tag <tag>  |
   version, tag,    +----------+     | sha512 goes to the summary |
   sha                               +----------------------------+
```

| Job | Does | Stops the release when |
|---|---|---|
| `verify` | Runs `scripts/release/verify.js` on the tag. | Any rule below fails. |
| `pack` | Checks the checkout is the verified commit, runs `npm ci --ignore-scripts`, `npm pack`, and the clean-tarball check. Uploads the one tarball. | The checkout is not the verified commit, or the tarball is not clean. |
| `publish` | Waits for approval of the `npm-release` Environment. Checks the tarball name and version and npm 11.5.1 or newer. Writes the tarball's sha512 to the job summary, then publishes **that tarball**. | The tarball does not match, npm is too old, or the approver rejects it. |

The workflow has no npm token. It publishes through trusted publishing (OIDC). The file name `release.yml` is registered at npm: do not rename it.

### What `verify` checks (exact SHA)

All checks are about the exact commit the tag points at. A run on a branch or a pull request proves the code, not the release.

| Rule | Must hold |
|---|---|
| 1 | The tag is `v` + the `package.json` version, and the version is `X.Y.Z` or `X.Y.Z-pre`. |
| 2 | The tagged commit is on `origin/master`. |
| 3 | `CHANGELOG.md` has a `## <version>` heading (a date or other text may follow after a space). |
| 4 | The newest **push** run on `master` of `ci.yml` and of `gate.yml` for that commit is `completed/success`. Dispatch and pull-request runs do not count. |
| 5 | Some `weekly.yml` run for that commit is `completed/success`. |

`verify` reads every page of the run lists (100 runs per page, 10 pages). If page 10 is still full it fails and does not judge a truncated list.

The npm dist-tag follows the version: a version with a pre-release part (`3.0.0-rc.1`) publishes as `next`, a plain version (`3.0.0`) as `latest`.

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

Read the settings back and check them before the first tag, before the trusted-publisher step, and before every release tag. Read the response for the reviewer, the administrator-bypass flag and the deployment policy:

```sh
gh api repos/Countly/kafkajs/environments/npm-release
```

A mismatch stops the step.

### npm account

1. The publisher's npm account can publish to the `@countly` scope. Create the `@countly` organisation if it does not exist.
2. Create a short-lived granular token for the first publish only (U3 below). Revoke it right after.

## First publish sequence

`@countly/kafkajs` does not exist on npm until the first publish, and trusted publishing cannot be set up for a package that does not exist. So the first version is published by hand, and CI publishes from the second version on.

| Step | Who | Action |
|---|---|---|
| U1 | Release owner | Create the `npm-release` Environment as above. Tell the agent. The agent reads the Environment back (command above). |
| U1b | Release owner | Negative control: push the throwaway tag `v0.0.0-p10-check`. The `release.yml` run must show `verify` failing and no deployment. Then delete the tag: `git push origin --delete v0.0.0-p10-check`. |
| U2 | Release owner | Confirm npm access to the `@countly` scope and create the short-lived token (above). |
| U3 | Release owner | **Accept in writing that npm may also point `latest` at rc.0.** The first publish of a new package gets `latest` even with `--tag next`. If that is not acceptable, stop: nothing is published. Otherwise, from a clean checkout of the tag `3.0.0-rc.0`: `npm publish --access public --tag next`. Then revoke the token. Record `npm view @countly/kafkajs dist-tags --json`. |
| U4 | Release owner | Immediately before U5: npmjs.com → package settings → Trusted Publisher → GitHub Actions. Owner `Countly`, repository `kafkajs`, workflow `release.yml`, environment `npm-release`. **Allowed actions: tick `npm publish`. Leave `npm dist-tag` off.** |
| U5 | Release owner | Push tag `v3.0.0-rc.1` on a `master` commit with green exact-SHA runs (gate step 5). Approve the `npm-release` deployment in GitHub. |
| U6 | Anyone | Run the checks in "After every publish". A failure blocks the release. |
| U6b | Release owner (optional) | `npm dist-tag add @countly/kafkajs@3.0.0-rc.1 latest` while logged in. This moves `latest` from rc.0 to rc.1. Install instructions still say to use exact versions. |

Why U4 is late: npm requires a new trusted-publisher configuration to complete its first successful publish within 2 days, and npm does not check the configuration when you save it. So U4 happens right before U5. Configurations created after 2026-09-03 allow `npm stage publish` by default, so a direct `npm publish` is refused unless it is ticked.

Tag names:

- rc.0 is the one tag without `v` (`3.0.0-rc.0`). It is pushed by hand and `release.yml` does not run for it.
- CI tags start at `v3.0.0-rc.1`.
- Until 3.0.0, `latest` also points at a release candidate. Install release candidates by exact version.

## After every publish

Take the `sha512-…` value from the `publish` job summary (the "Release tarball" section). Set `VERSION` and `EXPECTED` and run both checks.

```sh
VERSION=3.0.0-rc.1
EXPECTED='sha512-<value from the job summary>'

# 1. The registry's integrity equals the job summary.
test "$(npm view "@countly/kafkajs@$VERSION" dist.integrity)" = "$EXPECTED" && echo "integrity OK"

# 2. The provenance subject digest equals the archive's SHA-512.
npm view "@countly/kafkajs@$VERSION" dist.attestations.url | xargs curl -fsS | EXPECTED="$EXPECTED" node -e '
const { attestations } = JSON.parse(require("fs").readFileSync(0, "utf8"))
const provenance = attestations.find((a) => a.predicateType === "https://slsa.dev/provenance/v1")
if (!provenance) { console.error("no provenance attestation"); process.exit(1) }
const { subject } = JSON.parse(Buffer.from(provenance.bundle.dsseEnvelope.payload, "base64"))
const hex = Buffer.from(process.env.EXPECTED.replace(/^sha512-/, ""), "base64").toString("hex")
if (subject.length !== 1 || subject[0].digest.sha512 !== hex) { console.error("provenance subject does not match"); process.exit(1) }
console.log("provenance OK")
'
```

The publish job publishes the downloaded tarball; compare the published `dist.integrity` and the provenance subject digest with the sha512 in the job summary; a mismatch or a missing provenance blocks the release.

A refusal by npm, a missing provenance or a mismatch blocks the release. Investigate and correct the cause. Do not repack, publish from a directory, fall back to a token or waive the provenance. The registry can lag a minute or two after a publish: repeat a failed read before you treat it as a mismatch.

Optional after U5: in npm, turn on "require two-factor authentication and disallow tokens". Trusted publishing keeps working.

## Release gate

Do these in order. Each box has its own evidence.

- [ ] **1. 9E, 9F and 9G are merged and tagged** (`3.0.0-alpha.9e`, `9f`, `9g` exist on `origin`).
- [ ] **2. `memory-gates.yml` is green** after the `p9m/memory-gates` PR is merged. Keep the run URL.
- [ ] **3. The Phase 10 PR is merged** (every functional lane green). Straight after the merge, dispatch the benchmark workflow on `master`: `gh workflow run bench.yml --ref master`.
- [ ] **4. `QUARANTINE.md` lists no quarantined test**, or the release owner has accepted each listed test in writing.
- [ ] **5. Release candidate commit.** Version `3.0.0-rc.N` in `package.json` and the lockfile, a `## 3.0.0-rc.N` CHANGELOG heading, and the README status text updated. On that exact SHA, the `ci.yml` and `gate.yml` push runs are green on Kafka 4.4.0, and `weekly.yml` dispatched on `master` is green. Publish the candidate (rc.0 by hand, later ones by CI).
- [ ] **6. Read the benchmark report** (`bench-report.md` from a `bench.yml` run on the rc commit). No threshold gates the release: the owner reads it and decides.
- [ ] **7. Countly canary** (`plugins/kafka`) is green against the **published** rc, installed by exact version (`@countly/kafkajs@3.0.0-rc.1`, never by tag).
- [ ] **8. Release commit.** Only the version, the lockfile and the CHANGELOG heading (`## 3.0.0 — <date>`) differ from the canaried rc. The `ci.yml` and `gate.yml` push runs and a dispatched `weekly.yml` are green on that exact SHA.
- [ ] **9. The release owner says go.** Push tag `v3.0.0`, approve the deployment, then run "After every publish". Check that `latest` is 3.0.0.

Useful read-only commands:

```sh
# Push runs on master for a commit (replace the SHA)
gh run list --branch master --json headSha,conclusion,event,workflowName | node -e '
const sha = process.argv[1]
const runs = JSON.parse(require("fs").readFileSync(0, "utf8")).filter((r) => r.headSha === sha)
console.table(runs.map(({ workflowName, event, conclusion }) => ({ workflowName, event, conclusion })))
' "$(git rev-parse HEAD)"

# Files that differ between the canaried rc tag and the release commit (set RC to your rc tag)
RC=v3.0.0-rc.1
git diff --stat "$RC"..HEAD

# dist-tags on the registry
npm view @countly/kafkajs dist-tags --json
```

### Release commit rules

- The version is set in `package.json` and in the lockfile (`npm version <version> --no-git-tag-version`).
- `CHANGELOG.md` has a `## <version>` heading for it. `verify` rule 3 fails without one.
- Between the canaried rc and the release commit, nothing else changes. If something else must change, cut a new rc and canary it again.

## Flaky tests

A test that fails and then passes on a rerun of the same SHA is either fixed or quarantined with an entry in `QUARANTINE.md` before the next merge to `master`. The release gate (step 4) needs that file to list no quarantined test, or each one accepted.

A test that needs a missing broker feature on 4.0.2 is skipped only with capture evidence; a product bug is fixed.

## When the release run fails

| Situation | What to do |
|---|---|
| `verify` fails | Read the rule number in the log. Rule 4 or 5 usually means a missing or red run for that SHA: get it green (rerun the push run, dispatch `weekly.yml`), then rerun the failed `release.yml` run. Rules 1 to 3 need a new commit, so they need a new tag. |
| The run fails for a reason outside the code (a runner or network problem) | Rerun the push run first, then rerun the failed `release.yml` run. Re-pushing an unchanged tag triggers nothing. |
| `publish` is rejected at approval | The release owner starts it again by rerunning the failed `release.yml` run. |
| Provenance or integrity check fails | See "After every publish". The release is blocked. |

A tag is never moved or deleted after a publish. If a published version is wrong, publish a new version.

## Benchmarks

`bench.yml` is a manual workflow. It runs the benchmark manifest against one Compose cluster on Kafka 4.4.0 for the packed fork and for kafkajs 2.2.4 through the public API. It has no thresholds. Start it with `gh workflow run bench.yml --ref master`, then read `bench-report.md` from the run's artifacts (`bench-report`) or its job summary. A benchmark that fails shows as an error row; the report is uploaded either way.
