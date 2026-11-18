/**
 * Release verifier. Pre-pack (no argument): the first job of `release.yml` runs it on a pushed tag and
 * the release goes on only when it exits 0; it reads the tagged checkout, GitHub's run and job lists and,
 * on the final tag, the npm registry. Post-pack (`--post-pack <main tgz> <otel tgz>`): the OTel pack job
 * checks both tarballs against their manifests and the main tarball against the pack job's SRI. It never
 * writes outside `$GITHUB_OUTPUT` and never prints the token.
 */
const { spawnSync } = require('node:child_process')
const crypto = require('node:crypto')
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

/**
 * @typedef {object} WorkflowJob
 * @property {string} name the job's display name, with matrix values in parentheses
 * @property {string|null} conclusion
 */

/** @typedef {(runId: number) => Promise<WorkflowJob[]>} ListJobs the latest attempt's jobs of a run */

/** @typedef {(name: string, version: string) => Promise<boolean>} Lookup true when the registry holds it */

/**
 * @typedef {object} PackedTarball
 * @property {{ name?: string, version?: string }} manifest the package's package.json in the checkout
 * @property {{ name?: string, version?: string }} packed the tarball's package/package.json
 * @property {string} sri the tarball's `sha512-<base64>` integrity
 */

const API = 'https://api.github.com'
const PER_PAGE = 100
const MAX_PAGES = 10
const VERSION_PATTERN = /^\d+\.\d+\.\d+(-[0-9a-zA-Z.-]+)?$/
const COMMIT_PATTERN = /^[0-9a-f]{40}$/
const REPOSITORY_PATTERN = /^[\w.-]+\/[\w.-]+$/
const PUSH_WORKFLOWS = ['ci.yml', 'gate.yml']
const MAIN_NAME = '@countly/kafkajs'
const OTEL_NAME = '@countly/opentelemetry-instrumentation-kafkajs'
/** The only tag whose release run publishes the OTel package. */
const OTEL_PUBLISH_TAG = 'v3.0.0'
const REGISTRY = 'https://registry.npmjs.org'
const NUMBER = '0|[1-9]\\d*'
const IDENTIFIER = `(?:${NUMBER}|\\d*[a-zA-Z-][0-9a-zA-Z-]*)`
/** Semver 2.0.0 without build metadata: no leading zeroes and no empty pre-release identifiers. */
const SEMVER = new RegExp(
  `^(${NUMBER})\\.(${NUMBER})\\.(${NUMBER})(?:-(${IDENTIFIER}(?:\\.${IDENTIFIER})*))?$`
)
const PARTIAL = new RegExp(`^(${NUMBER})(?:\\.(${NUMBER}))?$`)
const MAX_VERSION_LENGTH = 256
/** Whether a comparison result (negative, zero, positive) meets each range operator. */
const OPERATORS = {
  '>=': (order) => order >= 0,
  '>': (order) => order > 0,
  '<=': (order) => order <= 0,
  '<': (order) => order < 0,
  '=': (order) => order === 0,
}
const COMPARATOR = /^(>=|<=|>|<|=)?(.+)$/
const MEMORY_WORKFLOW = 'memory-gates.yml'
/** The job that fails on a partial selection and on any needed job that did not succeed. */
const MEMORY_VERDICT_JOB = 'memory-gates-verdict'
/** A long-window matrix job: `telemetry-long-window (<node>, <kafka>, <arm>, <replicate>)`. */
const LONG_WINDOW_JOB = /^telemetry-long-window \((.+), (\d+)\)$/
/** The one long-window arm that runs a single replicate by design. */
const WARMUP_ARM = ', warmup-seed'
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
 * Why a memory-gates run is not a full green three-replicate verdict, read from its jobs because the
 * runs API does not return a dispatch's inputs. A partial selection fails or skips the verdict job.
 * @param {WorkflowRun} run
 * @param {ListJobs} listJobs
 * @returns {Promise<string|undefined>} undefined when the run is full and green
 */
const memoryRunProblem = async (run, listJobs) => {
  if (!succeeded(run)) return `run ${run.id} is ${run.status}/${run.conclusion}`
  const jobs = await listJobs(run.id)
  const verdict = jobs.find(({ name }) => name === MEMORY_VERDICT_JOB)
  if (verdict?.conclusion !== 'success') {
    return `run ${run.id}: ${MEMORY_VERDICT_JOB} is ${verdict ? verdict.conclusion : 'missing'}`
  }
  const bad = jobs.find(({ conclusion }) => conclusion !== 'success' && conclusion !== 'skipped')
  if (bad) return `run ${run.id}: job ${bad.name} is ${bad.conclusion}`

  const replicates = new Map()
  for (const { name, conclusion } of jobs) {
    const match = LONG_WINDOW_JOB.exec(name)
    if (!match || match[1].endsWith(WARMUP_ARM) || conclusion !== 'success') continue
    replicates.set(match[1], [...(replicates.get(match[1]) ?? []), Number(match[2])])
  }
  if (replicates.size === 0) return `run ${run.id} has no long-window replicate jobs`
  for (const [group, ran] of replicates) {
    const sorted = ran.sort((a, b) => a - b).join(',')
    if (sorted !== '1,2,3')
      return `run ${run.id}: long-window ${group} ran replicates ${sorted}, not 1,2,3`
  }
  return undefined
}

/**
 * Rule 11: some `workflow_dispatch` run of `memory-gates.yml` for the tagged commit completed
 * successfully with the full selection and three long-window replicates. Pull-request runs and other
 * commits' runs never count, so every release tag, rc or final, needs its own run.
 * @param {object} input
 * @param {string} input.sha the tagged commit
 * @param {ListRuns} input.listRuns
 * @param {ListJobs} input.listJobs
 * @returns {Promise<void>}
 * @throws {ReleaseVerificationError} rule 11, naming each run's problem
 */
const verifyMemoryEvidence = async ({ sha, listRuns, listJobs }) => {
  const query = { event: 'workflow_dispatch', head_sha: sha }
  const candidates = await matchingRuns(listRuns, MEMORY_WORKFLOW, query)
  if (candidates.length === 0)
    fail(11, `no workflow_dispatch run of ${MEMORY_WORKFLOW} for commit ${sha}`)
  const problems = []
  for (const run of candidates) {
    const problem = await memoryRunProblem(run, listJobs)
    if (!problem) return
    problems.push(problem)
  }
  fail(11, `no full green ${MEMORY_WORKFLOW} run for commit ${sha}: ${problems.join('; ')}`)
}

/**
 * Reads a strict semver version, as npm's `semver.valid` accepts it, without build metadata and with
 * every numeric part a safe integer.
 * @param {unknown} version `X.Y.Z` or `X.Y.Z-pre`
 * @returns {{ core: number[], pre: string[] }|null} null when the version is not that
 */
const parseVersion = (version) => {
  const match =
    typeof version === 'string' && version.length <= MAX_VERSION_LENGTH && SEMVER.exec(version)
  if (!match) return null
  const core = match.slice(1, 4).map(Number)
  const pre = match[4] ? match[4].split('.') : []
  const numeric = [...core, ...pre.filter((id) => /^\d+$/.test(id)).map(Number)]
  return numeric.every(Number.isSafeInteger) ? { core, pre } : null
}

/**
 * Semver precedence: the numeric core, then a version without a pre-release above one with it, then
 * pre-release identifiers left to right (numeric ones numerically and below alphanumeric ones).
 * @param {{ core: number[], pre: string[] }} a
 * @param {{ core: number[], pre: string[] }} b
 * @returns {number} negative, zero or positive as `a` is below, equal to or above `b`
 */
const compareVersions = (a, b) => {
  for (let i = 0; i < 3; i++) if (a.core[i] !== b.core[i]) return a.core[i] - b.core[i]
  if (a.pre.length === 0 || b.pre.length === 0) return b.pre.length - a.pre.length
  for (let i = 0; i < Math.min(a.pre.length, b.pre.length); i++) {
    const [x, y] = [a.pre[i], b.pre[i]]
    if (x === y) continue
    const [xn, yn] = [/^\d+$/.test(x), /^\d+$/.test(y)]
    if (xn && yn) return Number(x) - Number(y)
    if (xn || yn) return xn ? -1 : 1
    return x < y ? -1 : 1
  }
  return a.pre.length - b.pre.length
}

/**
 * Whether a version satisfies an npm range, with npm's default pre-release rule: a pre-release only
 * matches a comparator set that names a pre-release of the same `X.Y.Z`. Only `||` and space-joined
 * `>=`, `>`, `<=`, `<`, `=` comparators are read; a partial version is accepted after `>=` (missing
 * parts are 0) and `<` (`<4` is `<4.0.0-0`). Any other syntax is refused rather than guessed.
 * @param {string} version
 * @param {string} range
 * @returns {boolean}
 * @throws {ReleaseVerificationError} for a version or range this reader does not accept
 */
const satisfies = (version, range) => {
  const target = parseVersion(version)
  if (!target) fail(7, `version ${JSON.stringify(version)} is not X.Y.Z or X.Y.Z-pre`)
  const comparator = (text) => {
    const [, op = '=', bound] = COMPARATOR.exec(text) ?? []
    const partial = (op === '>=' || op === '<') && PARTIAL.exec(bound)
    if (partial) {
      const core = [partial[1], partial[2] ?? 0, 0].map(Number)
      if (core.every(Number.isSafeInteger))
        return { op, bound: { core, pre: op === '<' ? ['0'] : [] } }
    }
    const version = parseVersion(bound)
    if (!version) {
      fail(7, `the peer range ${JSON.stringify(range)} uses syntax this verifier does not read`)
    }
    return { op, bound: version }
  }
  const holds = ({ op, bound }) => OPERATORS[op](compareVersions(target, bound))
  return range.split('||').some((set) => {
    const comparators = set.trim().split(/\s+/).filter(Boolean).map(comparator)
    if (comparators.length === 0)
      fail(7, `the peer range ${JSON.stringify(range)} has an empty set`)
    const preAllowed =
      target.pre.length === 0 ||
      comparators.some(
        ({ bound }) =>
          bound.pre.length > 0 && bound.core.every((part, i) => part === target.core[i])
      )
    return preAllowed && comparators.every(holds)
  })
}

/**
 * Pre-pack checks of the OTel package's manifest: its name and version (6), its `@countly/kafkajs` peer
 * range includes the main version (7), and on the final tag the registry does not hold that version yet
 * (8). The registry is read only on the final tag, the one release that publishes the OTel package. Any
 * existing copy stops the release, whatever its integrity or provenance: there is no automatic recovery,
 * and the release owner reads the registry copy back by hand.
 * @param {object} input
 * @param {string} input.tag
 * @param {string} input.mainVersion the verified main package version
 * @param {{ name?: string, version?: string, peerDependencies?: Record<string, string> }} input.otelPackageJson
 * @param {Lookup} input.lookup
 * @returns {Promise<{ otelVersion: string }>}
 * @throws {ReleaseVerificationError} naming the first rule that does not hold
 */
const verifyOtelManifest = async ({ tag, mainVersion, otelPackageJson, lookup }) => {
  const { name, version, peerDependencies = {} } = otelPackageJson
  if (name !== OTEL_NAME) fail(6, `otel/package.json names ${name}, not ${OTEL_NAME}`)
  if (!parseVersion(version)) {
    fail(6, `otel/package.json version ${JSON.stringify(version)} is not X.Y.Z or X.Y.Z-pre`)
  }
  const range = peerDependencies[MAIN_NAME]
  if (typeof range !== 'string' || !satisfies(mainVersion, range)) {
    fail(7, `the ${MAIN_NAME} peer range ${JSON.stringify(range)} does not include ${mainVersion}`)
  }
  if (tag === OTEL_PUBLISH_TAG && (await lookup(OTEL_NAME, version))) {
    fail(
      8,
      `${OTEL_NAME}@${version} is already on the registry; automatic recovery is disabled, so complete the integrity and verified-provenance readback in RELEASING.md and do not publish it again`
    )
  }
  return { otelVersion: version }
}

/**
 * Post-pack checks: each tarball holds its package's name and the checkout's version (9), and the main
 * tarball is byte-identical to the one the pack job recorded (10).
 * @param {object} input
 * @param {PackedTarball} input.main
 * @param {PackedTarball} input.otel
 * @param {string} input.recordedMainSri the SRI the pack job wrote
 * @returns {{ otelVersion: string }}
 * @throws {ReleaseVerificationError} naming the first rule that does not hold
 */
const verifyPackedTarballs = ({ main, otel, recordedMainSri }) => {
  for (const [{ manifest, packed }, expected] of [
    [main, MAIN_NAME],
    [otel, OTEL_NAME],
  ]) {
    if (
      packed.name !== expected ||
      manifest.name !== expected ||
      packed.version !== manifest.version
    ) {
      fail(
        9,
        `a tarball holds ${packed.name}@${packed.version}, expected ${expected}@${manifest.version}`
      )
    }
  }
  if (main.sri !== recordedMainSri) {
    fail(10, `the main tarball is ${main.sri}, but the pack job recorded ${recordedMainSri}`)
  }
  return { otelVersion: otel.packed.version }
}

/**
 * Builds a `Lookup` over the public npm registry's version document.
 * @param {{ fetch?: typeof fetch }} [options] `fetch` is the test seam
 * @returns {Lookup} resolves false on 404 and true on any 2xx answer
 * @throws {ReleaseVerificationError} from the returned function on any other answer
 */
const createRegistry =
  ({ fetch: request = globalThis.fetch } = {}) =>
  async (name, version) => {
    const url = `${REGISTRY}/${name.replace('/', '%2f')}/${version}`
    const response = await request(url)
    if (response.status === 404) return false
    if (!response.ok) fail(undefined, `the registry answered ${response.status} for ${url}`)
    return true
  }

/**
 * Builds a reader of one paged GitHub REST list. Each call requests pages 1, 2, … (100 items per page)
 * and joins them, stopping at the first page with fewer than 100 items.
 * @param {{ repository: string, token: string, fetch?: typeof fetch }} options `fetch` is the test seam
 * @returns {(route: string, params: object, key: string, label: string) => Promise<object[]>} reads the
 *   `key` list of `/repos/<repository>/<route>`
 * @throws {ReleaseVerificationError} for a repository that is not `owner/name`; the returned function
 *   throws on a non-2xx answer, an answer without the list, or when page 10 is still full (a truncated
 *   list is never judged)
 */
const createPagedList = ({ repository, token, fetch: request = globalThis.fetch }) => {
  if (!REPOSITORY_PATTERN.test(repository))
    fail(undefined, `repository ${JSON.stringify(repository)} is not owner/name`)
  const headers = { Accept: 'application/vnd.github+json', Authorization: `Bearer ${token}` }
  return async (route, params, key, label) => {
    const items = []
    for (let page = 1; page <= MAX_PAGES; page++) {
      const url = new URL(`${API}/repos/${repository}/${route}`)
      for (const [name, value] of Object.entries({ ...params, per_page: PER_PAGE, page })) {
        if (value !== undefined) url.searchParams.set(name, String(value))
      }
      const response = await request(url, { headers })
      if (!response.ok) fail(undefined, `GitHub answered ${response.status} listing ${label}`)
      const { [key]: pageItems } = await response.json()
      if (!Array.isArray(pageItems)) fail(undefined, `GitHub's ${label} answer has no ${key} list`)
      items.push(...pageItems)
      if (pageItems.length < PER_PAGE) return items
    }
    return fail(
      undefined,
      `more than ${MAX_PAGES * PER_PAGE} ${label} match; the list is truncated and is not judged`
    )
  }
}

/**
 * Builds a `listRuns` over the GitHub REST API, reading every page of a workflow's runs.
 * @param {{ repository: string, token: string, fetch?: typeof fetch }} options `fetch` is the test seam
 * @returns {ListRuns}
 * @throws {ReleaseVerificationError} as `createPagedList`
 */
const createListRuns = (options) => {
  const list = createPagedList(options)
  return (workflow, query) =>
    list(`actions/workflows/${workflow}/runs`, query, 'workflow_runs', `${workflow} runs`)
}

/**
 * Builds a `listJobs` over the GitHub REST API, reading every page of a run's latest-attempt jobs.
 * @param {{ repository: string, token: string, fetch?: typeof fetch }} options `fetch` is the test seam
 * @returns {ListJobs}
 * @throws {ReleaseVerificationError} as `createPagedList`, and from the returned function for a run id
 *   that is not a positive integer
 */
const createListJobs = (options) => {
  const list = createPagedList(options)
  return async (runId) => {
    if (!Number.isSafeInteger(runId) || runId <= 0)
      fail(undefined, `run id ${JSON.stringify(runId)} is not a number`)
    return list(`actions/runs/${runId}/jobs`, { filter: 'latest' }, 'jobs', `run ${runId} jobs`)
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
 * @param {string} file a gzip tarball
 * @returns {object} its package/package.json
 * @throws {ReleaseVerificationError} when tar cannot read it
 */
const readPackedManifest = (file) => {
  const result = spawnSync('tar', ['-xOzf', file, 'package/package.json'], { encoding: 'utf8' })
  if (result.status !== 0) fail(9, `cannot read package/package.json from ${file}`)
  return JSON.parse(result.stdout)
}

/** @param {string} file @returns {string} its `sha512-<base64>` integrity, as npm computes it */
const sriOf = (file) =>
  `sha512-${crypto.createHash('sha512').update(fs.readFileSync(file)).digest('base64')}`

/**
 * Command line, post-pack mode: checks the two packed tarballs; writes nothing.
 * @param {(name: string) => string} need reads a required environment variable
 * @param {string} workspace
 * @param {string[]} files `[main tgz, otel tgz]`
 * @throws {ReleaseVerificationError} for a wrong argument count or a failing rule
 */
const postPack = (need, workspace, files) => {
  if (files.length !== 2) fail(undefined, 'usage: verify.js --post-pack <main tgz> <otel tgz>')
  const [tag, recordedMainSri] = ['GITHUB_REF_NAME', 'MAIN_SRI'].map(need)
  const readJson = (file) => JSON.parse(fs.readFileSync(path.join(workspace, file), 'utf8'))
  const tarball = (file, manifest) => ({
    manifest,
    packed: readPackedManifest(file),
    sri: sriOf(file),
  })
  const { otelVersion } = verifyPackedTarballs({
    main: tarball(files[0], readJson('package.json')),
    otel: tarball(files[1], readJson('otel/package.json')),
    recordedMainSri,
  })
  console.log(`verified the packed tarballs for ${tag}: OTel ${otelVersion}`)
}

/**
 * Command line. With no argument, verifies the tag that triggered the workflow in the checked-out
 * repository, its memory-gates evidence and the OTel manifest, then appends `version=`, `dist-tag=`,
 * `sha=` (the commit that was judged) and `otel-version=` to `$GITHUB_OUTPUT`; nothing is written when
 * verification fails. With `--post-pack <main tgz> <otel tgz>`, runs the post-pack checks instead
 * (needs GITHUB_REF_NAME and MAIN_SRI).
 * @param {Record<string, string|undefined>} [env] needs GITHUB_REF_NAME, GITHUB_SHA, GITHUB_REPOSITORY,
 *   GITHUB_TOKEN and GITHUB_OUTPUT; GITHUB_WORKSPACE defaults to the current directory
 * @param {string[]} [args] command-line arguments
 * @returns {Promise<void>}
 * @throws {ReleaseVerificationError} when a variable is missing, the checkout is not at the tagged
 *   commit, or a rule fails
 */
const main = async (env = process.env, args = process.argv.slice(2)) => {
  const need = (name) => env[name] || fail(undefined, `${name} is not set`)
  const workspace = env.GITHUB_WORKSPACE || process.cwd()
  if (args[0] === '--post-pack') return postPack(need, workspace, args.slice(1))

  const [tag, eventSha, repository, token, output] = [
    'GITHUB_REF_NAME',
    'GITHUB_SHA',
    'GITHUB_REPOSITORY',
    'GITHUB_TOKEN',
    'GITHUB_OUTPUT',
  ].map(need)
  if (!COMMIT_PATTERN.test(eventSha)) fail(undefined, 'GITHUB_SHA is not a 40-hex commit id')

  // An annotated tag's object id peels to the commit it points at.
  const sha = gitOutput(workspace, ['rev-parse', '--verify', `${eventSha}^{commit}`])
  const head = gitOutput(workspace, ['rev-parse', 'HEAD'])
  if (head !== sha) fail(undefined, `the checkout is at ${head}, not at the tagged commit ${sha}`)

  const readJson = (file) => JSON.parse(fs.readFileSync(path.join(workspace, file), 'utf8'))
  const listRuns = createListRuns({ repository, token })
  const { version, distTag } = await verifyRelease({
    tag,
    sha,
    packageJson: readJson('package.json'),
    changelog: fs.readFileSync(path.join(workspace, 'CHANGELOG.md'), 'utf8'),
    isOnMaster: ancestorOfMaster(workspace),
    listRuns,
  })
  await verifyMemoryEvidence({ sha, listRuns, listJobs: createListJobs({ repository, token }) })
  const { otelVersion } = await verifyOtelManifest({
    tag,
    mainVersion: version,
    otelPackageJson: readJson('otel/package.json'),
    lookup: createRegistry(),
  })
  fs.appendFileSync(
    output,
    `version=${version}\ndist-tag=${distTag}\nsha=${sha}\notel-version=${otelVersion}\n`
  )
  console.log(
    `verified ${tag} at ${sha}: version ${version}, dist-tag ${distTag}, OTel ${otelVersion}`
  )
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

module.exports = {
  verifyRelease,
  distTagOf,
  createListRuns,
  createListJobs,
  createRegistry,
  verifyMemoryEvidence,
  verifyOtelManifest,
  verifyPackedTarballs,
  ReleaseVerificationError,
  main,
}
