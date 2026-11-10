/**
 * Release verifier: the first job of `release.yml` runs it on a pushed tag and the release goes on only
 * when it exits 0. It reads the tagged checkout and GitHub's run lists; it never writes outside
 * `$GITHUB_OUTPUT` and never prints the token.
 */
const { spawnSync } = require('node:child_process')
const fs = require('node:fs')
const path = require('node:path')

/**
 * @typedef {object} WorkflowRun
 * @property {number} id
 * @property {string} event
 * @property {string} head_branch
 * @property {string} head_sha
 * @property {string} status
 * @property {string|null} conclusion
 * @property {string} created_at ISO 8601 timestamp
 */

/**
 * @typedef {object} RunQuery
 * @property {string} [event] workflow event the run was started by
 * @property {string} [branch] branch the run was started on
 * @property {string} head_sha commit the run was started for
 */

/** @typedef {(workflow: string, query: RunQuery) => Promise<WorkflowRun[]>} ListRuns */

const API = 'https://api.github.com'
const PER_PAGE = 100
const MAX_PAGES = 10
const VERSION_PATTERN = /^\d+\.\d+\.\d+(-[0-9a-zA-Z.-]+)?$/
const COMMIT_PATTERN = /^[0-9a-f]{40}$/
const REPOSITORY_PATTERN = /^[\w.-]+\/[\w.-]+$/
const PUSH_WORKFLOWS = ['ci.yml', 'gate.yml']
/** Run field that each query key selects, so results are re-checked here whatever the server filtered. */
const RUN_FIELD = { event: 'event', branch: 'head_branch', head_sha: 'head_sha' }

/** Names the rule that failed: messages from `verifyRelease` start with `rule <n>: `. */
class ReleaseVerificationError extends Error {
  /** @param {string} message */
  constructor(message) {
    super(message)
    this.name = 'ReleaseVerificationError'
  }
}

/**
 * @param {number|undefined} rule number of the verification rule, when one failed
 * @param {string} message
 * @returns {never}
 * @throws {ReleaseVerificationError} always
 */
const fail = (rule, message) => {
  throw new ReleaseVerificationError(rule ? `rule ${rule}: ${message}` : message)
}

/**
 * The npm dist-tag a version is published under.
 * @param {string} version `X.Y.Z` or `X.Y.Z-pre`
 * @returns {'next'|'latest'} `next` for a version with a pre-release part, `latest` otherwise
 * @throws {ReleaseVerificationError} when the version is neither shape
 */
const distTagOf = (version) => {
  if (typeof version !== 'string' || !VERSION_PATTERN.test(version)) {
    fail(undefined, `version ${JSON.stringify(version)} is not X.Y.Z or X.Y.Z-pre`)
  }
  return version.includes('-') ? 'next' : 'latest'
}

/**
 * Whether the changelog has a level-two heading for exactly this version, optionally followed by a
 * space and a suffix (a date). Compared as text, so dots are literal and `3.0.0-rc.1` never satisfies `3.0.0`.
 * @param {string} changelog
 * @param {string} version
 * @returns {boolean}
 */
const hasChangelogHeading = (changelog, version) =>
  changelog
    .split(/\r\n|\n|\r/)
    .some((line) => line === `## ${version}` || line.startsWith(`## ${version} `))

/**
 * The run that was created last; the higher run id breaks a timestamp tie.
 * @param {WorkflowRun[]} runs
 * @returns {WorkflowRun|undefined} undefined for no runs
 * @throws {ReleaseVerificationError} when a run's `created_at` cannot be read
 */
const newestRun = (runs) => {
  const dated = runs.map((run) => ({ run, time: Date.parse(run.created_at) }))
  const unreadable = dated.find(({ time }) => Number.isNaN(time))
  if (unreadable) fail(4, `run ${unreadable.run.id} has no readable created_at`)
  const newer = (a, b) => (b.time > a.time || (b.time === a.time && b.run.id > a.run.id) ? b : a)
  return dated.reduce(newer, dated[0])?.run
}

/**
 * Lists a workflow's runs and keeps those that match every query key, whatever the server filtered.
 * @param {ListRuns} listRuns
 * @param {string} workflow
 * @param {RunQuery} query
 * @returns {Promise<WorkflowRun[]>}
 */
const matchingRuns = async (listRuns, workflow, query) =>
  (await listRuns(workflow, query)).filter((run) =>
    Object.entries(query).every(([key, value]) => run[RUN_FIELD[key]] === value)
  )

/** @param {WorkflowRun} run @returns {boolean} */
const succeeded = (run) => run.status === 'completed' && run.conclusion === 'success'

/**
 * Checks, in order, that a tag may be released: it is `v` + the package version (1), its commit is on
 * master (2), the changelog has the version's heading (3), the newest push run on master of `ci.yml` and
 * `gate.yml` for that commit succeeded (4), and some `weekly.yml` run for that commit succeeded (5).
 * Dispatch and pull-request runs never count for rules 4 and 5's push workflows.
 * @param {object} input
 * @param {string} input.tag for example `v3.0.0-rc.1`
 * @param {string} input.sha the tagged commit
 * @param {{ version: string }} input.packageJson
 * @param {string} input.changelog
 * @param {(sha: string) => boolean} input.isOnMaster
 * @param {ListRuns} input.listRuns reads every page of a workflow's runs
 * @returns {Promise<{ version: string, distTag: 'next'|'latest' }>}
 * @throws {ReleaseVerificationError} naming the first rule that does not hold
 */
const verifyRelease = async ({ tag, sha, packageJson, changelog, isOnMaster, listRuns }) => {
  const version = packageJson.version
  if (tag !== `v${version}`) fail(1, `tag ${tag} is not v${version}`)
  if (typeof version !== 'string' || !VERSION_PATTERN.test(version)) {
    fail(1, `version ${JSON.stringify(version)} is not X.Y.Z or X.Y.Z-pre`)
  }
  if (!isOnMaster(sha)) fail(2, `commit ${sha} is not on master`)
  if (!hasChangelogHeading(changelog, version))
    fail(3, `CHANGELOG.md has no "## ${version}" heading`)

  for (const workflow of PUSH_WORKFLOWS) {
    const query = { event: 'push', branch: 'master', head_sha: sha }
    const newest = newestRun(await matchingRuns(listRuns, workflow, query))
    if (!newest) fail(4, `no push run of ${workflow} on master for commit ${sha}`)
    if (!succeeded(newest)) {
      fail(
        4,
        `the newest ${workflow} run ${newest.id} is ${newest.status}/${newest.conclusion}, not completed/success`
      )
    }
  }

  const weekly = await matchingRuns(listRuns, 'weekly.yml', { head_sha: sha })
  if (!weekly.some(succeeded)) fail(5, `no successful weekly.yml run for commit ${sha}`)

  return { version, distTag: distTagOf(version) }
}

/**
 * Builds a `listRuns` over the GitHub REST API. Each call requests pages 1, 2, … (100 runs per page) and
 * joins them, stopping at the first page with fewer than 100 runs.
 * @param {{ repository: string, token: string, fetch?: typeof fetch }} options `fetch` is the test seam
 * @returns {ListRuns}
 * @throws {ReleaseVerificationError} for a repository that is not `owner/name`; the returned function
 *   throws on a non-2xx answer, an answer without a run list, or when page 10 is still full (a truncated
 *   list is never judged)
 */
const createListRuns = ({ repository, token, fetch: request = globalThis.fetch }) => {
  if (!REPOSITORY_PATTERN.test(repository))
    fail(undefined, `repository ${JSON.stringify(repository)} is not owner/name`)
  const headers = { Accept: 'application/vnd.github+json', Authorization: `Bearer ${token}` }
  return async (workflow, query) => {
    const runs = []
    for (let page = 1; page <= MAX_PAGES; page++) {
      const url = new URL(`${API}/repos/${repository}/actions/workflows/${workflow}/runs`)
      for (const [key, value] of Object.entries({ ...query, per_page: PER_PAGE, page })) {
        if (value !== undefined) url.searchParams.set(key, String(value))
      }
      const response = await request(url, { headers })
      if (!response.ok)
        fail(undefined, `GitHub answered ${response.status} listing ${workflow} runs`)
      const { workflow_runs: pageRuns } = await response.json()
      if (!Array.isArray(pageRuns))
        fail(undefined, `GitHub's ${workflow} answer has no workflow_runs list`)
      runs.push(...pageRuns)
      if (pageRuns.length < PER_PAGE) return runs
    }
    return fail(
      undefined,
      `more than ${MAX_PAGES * PER_PAGE} ${workflow} runs match; the list is truncated and is not judged`
    )
  }
}

/**
 * Runs git in `cwd`.
 * @param {string} cwd
 * @param {string[]} args
 * @returns {{ status: number|null, stdout: string, detail: string }}
 */
const git = (cwd, args) => {
  const result = spawnSync('git', args, { cwd, encoding: 'utf8' })
  const detail = result.error ? result.error.message : (result.stderr || '').trim()
  return { status: result.status, stdout: (result.stdout || '').trim(), detail }
}

/**
 * @param {string} cwd
 * @param {string[]} args
 * @returns {string} trimmed stdout of a git command that must succeed
 * @throws {ReleaseVerificationError} when git exits non-zero
 */
const gitOutput = (cwd, args) => {
  const { status, stdout, detail } = git(cwd, args)
  if (status !== 0) fail(undefined, `git ${args[0]} failed (exit ${status}): ${detail}`)
  return stdout
}

/**
 * `git merge-base --is-ancestor`: exit 0 is an ancestor, exit 1 is not, anything else (a missing
 * `origin/master`, a bad object) is an error and is never read as "not on master".
 * @param {string} cwd
 * @returns {(sha: string) => boolean}
 */
const ancestorOfMaster = (cwd) => (sha) => {
  const { status, detail } = git(cwd, ['merge-base', '--is-ancestor', sha, 'origin/master'])
  if (status === 0) return true
  if (status === 1) return false
  return fail(undefined, `git merge-base failed (exit ${status}): ${detail}`)
}

/**
 * Command line: verifies the tag that triggered the workflow in the checked-out repository, then appends
 * `version=`, `dist-tag=` and `sha=` (the commit that was judged) to `$GITHUB_OUTPUT`. Nothing is written
 * when verification fails.
 * @param {Record<string, string|undefined>} [env] needs GITHUB_REF_NAME, GITHUB_SHA, GITHUB_REPOSITORY,
 *   GITHUB_TOKEN and GITHUB_OUTPUT; GITHUB_WORKSPACE defaults to the current directory
 * @returns {Promise<void>}
 * @throws {ReleaseVerificationError} when a variable is missing, the checkout is not at the tagged
 *   commit, or a rule fails
 */
const main = async (env = process.env) => {
  const need = (name) => env[name] || fail(undefined, `${name} is not set`)
  const [tag, eventSha, repository, token, output] = [
    'GITHUB_REF_NAME',
    'GITHUB_SHA',
    'GITHUB_REPOSITORY',
    'GITHUB_TOKEN',
    'GITHUB_OUTPUT',
  ].map(need)
  if (!COMMIT_PATTERN.test(eventSha)) fail(undefined, 'GITHUB_SHA is not a 40-hex commit id')
  const workspace = env.GITHUB_WORKSPACE || process.cwd()

  // An annotated tag's object id peels to the commit it points at.
  const sha = gitOutput(workspace, ['rev-parse', '--verify', `${eventSha}^{commit}`])
  const head = gitOutput(workspace, ['rev-parse', 'HEAD'])
  if (head !== sha) fail(undefined, `the checkout is at ${head}, not at the tagged commit ${sha}`)

  const { version, distTag } = await verifyRelease({
    tag,
    sha,
    packageJson: JSON.parse(fs.readFileSync(path.join(workspace, 'package.json'), 'utf8')),
    changelog: fs.readFileSync(path.join(workspace, 'CHANGELOG.md'), 'utf8'),
    isOnMaster: ancestorOfMaster(workspace),
    listRuns: createListRuns({ repository, token }),
  })
  fs.appendFileSync(output, `version=${version}\ndist-tag=${distTag}\nsha=${sha}\n`)
  console.log(`verified ${tag} at ${sha}: version ${version}, dist-tag ${distTag}`)
}

if (require.main === module) {
  main().catch((error) => {
    console.error(
      error instanceof ReleaseVerificationError
        ? `release verification failed: ${error.message}`
        : error
    )
    process.exitCode = 1
  })
}

module.exports = { verifyRelease, distTagOf, createListRuns, ReleaseVerificationError, main }
