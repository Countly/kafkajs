const fs = require('node:fs')
const path = require('node:path')
const { execFileSync } = require('node:child_process')
const {
  PROFILES,
  childArgs,
  childEnvironment,
  environmentIdentity,
  digest,
} = require('./shareSoakContract')
const { monotonicMs } = require('./naturalExit')

const OUTPUT = '.tmp/e2e'
const STATE = `${OUTPUT}/lifecycle.json`
const CONTRACT = `${OUTPUT}/contract.json`
const STAGES = Object.freeze({
  verify: 180000,
  logs: 60000,
  teardown: 120000,
  seal: 60000,
  upload: 120000,
})

/** @param {string} file JSON file. @returns {object} Parsed original. @throws {Error} On missing/malformed bytes. */
const read = (file) => JSON.parse(fs.readFileSync(file, 'utf8'))
/** @param {string} file Output file. @param {object} value Original observation. @returns {void} */
const write = (file, value) => fs.writeFileSync(file, JSON.stringify(value, null, 2) + '\n')
/** @param {string[]} args Git arguments. @returns {string} Trimmed stdout. @throws {Error} On git failure. */
const git = (args) => execFileSync('git', args, { encoding: 'utf8' }).trim()

/**
 * Verifies the exact tested PR merge parents, including shallow runner checkouts.
 * @param {object} pr Original GitHub PR head/base identities.
 * @param {Function} [run=git] Git command reader in the tested checkout.
 * @returns {void}
 * @throws {Error} On a non-merge or mismatched parent pair.
 */
const verifyMergeParents = (pr, run = git) => {
  const parents = run(['cat-file', '-p', 'HEAD'])
    .split('\n\n', 1)[0]
    .split('\n')
    .filter((line) => line.startsWith('parent '))
    .map((line) => line.slice(7))
  if (parents.length !== 2 || !parents.includes(pr.head.sha) || !parents.includes(pr.base.sha))
    throw new Error('Unexpected tested merge relationship')
}

/**
 * Freezes every tracked byte, including the full source dependency closure.
 * Dependency installs and the single output root are the only untracked exceptions.
 * @returns {object} Commit, Git tree object and sorted path/length/hash inventory.
 * @throws {Error} On dirty files, symlinked sources or untracked shadows.
 */
const sourceIdentity = () => {
  if (git(['status', '--porcelain', '--untracked-files=no']))
    throw new Error('Dirty tracked source')
  const shadows = git(['ls-files', '--others', '--exclude=node_modules/', '--exclude=.tmp/'])
    .split('\n')
    .filter((name) => name && name !== 'node_modules')
    .join('\n')
  if (shadows) throw new Error(`Untracked source shadows: ${shadows}`)
  const files = git(['ls-files', '-z'])
    .split('\0')
    .filter(Boolean)
    .sort()
    .map((name) => {
      if (!fs.lstatSync(name).isFile()) throw new Error(`Non-regular tracked source: ${name}`)
      const bytes = fs.readFileSync(name)
      return { path: name, bytes: bytes.length, sha256: digest(bytes) }
    })
  return {
    commit: git(['rev-parse', 'HEAD']),
    gitTreeObject: git(['rev-parse', 'HEAD^{tree}']),
    files,
  }
}

/**
 * Computes original remaining-time admission with all outstanding obligations funded.
 * @param {object} state Immutable lifecycle origin/deadline and profile.
 * @param {number} now Absolute monotonic observation on the original runner.
 * @returns {number} Remaining milliseconds.
 * @throws {Error} On renewed origin, exhausted setup or insufficient remaining time.
 */
const admit = (state, now) => {
  const required = PROFILES[state.profile]?.parentMs + 600000
  const profile = PROFILES[state.profile]
  if (
    !profile ||
    ![state.origin, state.deadline, now, required].every(Number.isSafeInteger) ||
    state.deadline !== state.origin + profile.jobMs ||
    now < state.origin ||
    now - state.origin > 600000 ||
    state.deadline - now < required
  )
    throw new Error('Original lifecycle admission refused')
  return state.deadline - now
}

/**
 * Inventories exact archive bytes. Symlinks and path traversal are never accepted.
 * The root is the archive directory; seal.json alone is excluded to avoid recursive hashing.
 * @param {string} root Archive directory.
 * @returns {object[]} Sorted relative path, byte length and SHA-256 inventory.
 * @throws {Error} On non-regular entries.
 */
const inventory = (root) => {
  const files = []
  const visit = (relative) => {
    for (const name of fs.readdirSync(path.join(root, relative)).sort()) {
      const entry = path.posix.join(relative, name)
      if (entry === 'seal.json') continue
      const stat = fs.lstatSync(path.join(root, entry))
      if (stat.isDirectory()) visit(entry)
      else if (stat.isFile()) {
        const bytes = fs.readFileSync(path.join(root, entry))
        files.push({ path: entry, bytes: bytes.length, sha256: digest(bytes) })
      } else throw new Error(`Non-regular archive entry: ${entry}`)
    }
  }
  visit('')
  return files.sort((a, b) => a.path.localeCompare(b.path, 'en'))
}

/**
 * Records one owned CI lifecycle. OS/Compose ports are replaceable for broker-free controls.
 * No method selects an arbitrary workload or adopts an existing cluster.
 */
class ShareSoakJob {
  /**
   * @param {object} [ports] Clock, bounded command execution and environment.
   */
  constructor({
    now = monotonicMs,
    run = execFileSync,
    env = process.env,
    source = sourceIdentity,
  } = {}) {
    Object.assign(this, { now, run, env, source })
  }

  /** @param {string} command Executable. @param {string[]} args Fixed operation arguments. @returns {string} Stdout. @throws {Error} On command failure/timeout. */
  command(command, args) {
    return this.run(command, args, { encoding: 'utf8', timeout: 30000, env: this.env }).trim()
  }

  /** @param {string[]} args Fixed Compose operation. @returns {string} Stdout. @throws {Error} On Compose failure. */
  compose(args) {
    return this.command('docker', [
      'compose',
      '-f',
      'docker-compose.yml',
      '-p',
      this.state.project,
      ...args,
    ])
  }

  /**
   * Binds the first pre-checkout observation to frozen source before any cluster effect.
   * @param {'short'|'full'} profile Fixed matrix profile.
   * @returns {void}
   * @throws {Error} On source/runtime/event/origin mismatch or late setup.
   */
  freeze(profile) {
    const selected = PROFILES[profile]
    if (!selected) throw new Error('Unknown share soak profile')
    if (
      process.versions.node.split('.')[0] !== '24' ||
      this.env.RUNNER_ENVIRONMENT !== 'github-hosted'
    )
      throw new Error('Expected GitHub-hosted Node24 producer')
    const origin = read(this.env.KAFKAJS_SOAK_ORIGIN_FILE)
    if (
      origin.runId !== this.env.GITHUB_RUN_ID ||
      origin.attempt !== this.env.GITHUB_RUN_ATTEMPT ||
      origin.profile !== profile
    )
      throw new Error('Foreign lifecycle origin')
    fs.mkdirSync(OUTPUT, { recursive: true })
    if (fs.readdirSync(OUTPUT).length)
      throw new Error('Evidence output already populated before freeze')
    if (
      this.env.GITHUB_EVENT_NAME !== 'workflow_dispatch' &&
      !(
        this.env.GITHUB_EVENT_NAME === 'pull_request' &&
        this.env.GITHUB_HEAD_REF?.startsWith('p9m/')
      )
    )
      throw new Error('Ineligible memory-gates event')
    const source = sourceIdentity()
    if (source.commit !== this.env.GITHUB_SHA)
      throw new Error('Checkout differs from tested GitHub commit')
    const project = `share-soak-${profile}-${origin.runId}-${origin.attempt}`
    this.env.COMPOSE_PROJECT_NAME = project
    this.env.KAFKAJS_SOAK_PROFILE = profile
    this.env.KAFKAJS_TREE = source.commit
    if (this.env.GITHUB_EVENT_NAME === 'pull_request') {
      const pr = read(this.env.GITHUB_EVENT_PATH).pull_request
      verifyMergeParents(pr)
    }
    const environment = environmentIdentity(childEnvironment(profile, this.env))
    const event = read(this.env.GITHUB_EVENT_PATH)
    const contract = {
      schema: 'share-soak-ci-v1',
      profile,
      selected,
      source,
      workflow: '.github/workflows/memory-gates.yml',
      workflowRef: this.env.GITHUB_WORKFLOW_REF,
      workflowSha: this.env.GITHUB_WORKFLOW_SHA,
      event: this.env.GITHUB_EVENT_NAME,
      ref: this.env.GITHUB_REF,
      head: event.pull_request?.head?.sha ?? source.commit,
      headRef: this.env.GITHUB_HEAD_REF ?? null,
      base: event.pull_request?.base?.sha ?? null,
      repository: this.env.GITHUB_REPOSITORY,
      runId: origin.runId,
      attempt: origin.attempt,
      runUrl: `${this.env.GITHUB_SERVER_URL}/${this.env.GITHUB_REPOSITORY}/actions/runs/${origin.runId}/attempts/${origin.attempt}`,
      job: `share-soak-${profile}`,
      artifact: `share-soak-${profile}-${origin.runId}-${origin.attempt}`,
      node: process.version,
      executable: process.execPath,
      argv: childArgs(profile),
      environment,
      tuple: {
        kafka: '4.3.1',
        node: 24,
        lane: 'docker',
        fixture: 'docker-compose.yml',
        profile: 'tv2',
      },
      historicalFields: ['schedule.lifecycleMs'],
      controls: [
        ['scripts/ci/__tests__/naturalExit.spec.js', 'CI natural exit'],
        [
          'scripts/ci/__tests__/shareSoakJob.spec.js',
          'share soak CI lifecycle controls|share soak CI owned lifecycle serialization|required memory gate composition',
        ],
        [
          'scripts/evidence/__tests__/shareSoakAcceptance.spec.js',
          'share soak independent acceptance controls|share soak CI receipt consumption controls',
        ],
        [
          'scripts/soak/__tests__/sharePriming.spec.js',
          'fixed same-realm share priming|recorded priming integrity|original absolute priming allocations|streamed share protocol evidence',
        ],
        ['scripts/soak/__tests__/originalSharePriming.spec.js', 'two-scope original share driver'],
        ['scripts/soak/__tests__/shareCleanupControls.spec.js', 'share owner cleanup populations'],
        [
          'scripts/evidence/__tests__/shareQuietHeap.spec.js',
          'independent share quiet-heap evidence validation',
        ],
      ].map(([file, pattern]) => ({
        file,
        pattern,
        sha256: source.files.find((entry) => entry.path === file).sha256,
      })),
      validatorRevision: source.files.find(
        (entry) => entry.path === 'scripts/evidence/shareSoakAcceptance.js'
      ).sha256,
    }
    this.state = {
      ...origin,
      deadline: origin.origin + selected.jobMs,
      project,
      stages: [],
      errors: [],
      ownership: null,
    }
    admit(this.state, this.now())
    write(CONTRACT, contract)
    write(STATE, this.state)
    fs.appendFileSync(
      this.env.GITHUB_ENV,
      `KAFKAJS_TREE=${source.commit}\nCOMPOSE_PROJECT_NAME=${project}\nKAFKAJS_SOAK_PROFILE=${profile}\n`
    )
  }

  /**
   * Records both failure and secondary errors; a later success cannot erase a failed stage.
   * @param {string} name Fixed stage name.
   * @param {Function} work Bounded original operation.
   * @param {number} cap Stage allocation.
   * @returns {*} Original result.
   * @throws {Error} Original failure, with persistent stage facts.
   */
  stage(name, work, cap) {
    this.state = read(STATE)
    const startedAt = this.now()
    const prior = this.state.stages.map((stage) => stage.name)
    if (prior.includes(name)) throw new Error(`Repeated lifecycle stage: ${name}`)
    const row = { name, startedAt, remainingMs: this.state.deadline - startedAt, cap }
    try {
      const later =
        name === 'parent'
          ? 600000
          : Object.entries(STAGES)
              .slice(Object.keys(STAGES).indexOf(name))
              .reduce((sum, [, ms]) => sum + ms, 60000)
      if (name === 'startup') admit(this.state, startedAt)
      else if (
        this.state.deadline - startedAt <
        (name === 'parent' ? PROFILES[this.state.profile].parentMs + later : later)
      ) {
        row.admissionFailure = 'Outstanding lifecycle obligations are not funded'
        if (name === 'parent') throw new Error(row.admissionFailure)
      }
      const result = work()
      if (
        this.now() - startedAt > cap ||
        this.now() > this.state.deadline ||
        (name === 'startup' && this.now() - this.state.origin > 600000)
      )
        throw new Error('Original stage deadline exceeded')
      if (row.admissionFailure) throw new Error(row.admissionFailure)
      row.status = 'passed'
      return result
    } catch (error) {
      row.status = 'failed'
      row.error = error.message
      this.state.errors.push({ stage: name, message: error.message })
      throw error
    } finally {
      row.completedAt = this.now()
      this.state.stages.push(row)
      write(STATE, this.state)
    }
  }

  /**
   * Joins asynchronous independent verification before recording its completed stage.
   * @param {Function} work Independent validation against original archived inputs.
   * @returns {Promise<*>} Original result.
   * @throws {Error} On verification, remaining reserve or deadline failure.
   */
  async verifyStage(work) {
    this.state = read(STATE)
    const startedAt = this.now()
    const row = {
      name: 'verify',
      startedAt,
      cap: STAGES.verify,
      remainingMs: this.state.deadline - startedAt,
    }
    try {
      if (this.state.stages.some((s) => s.name === 'verify'))
        throw new Error('Repeated verify stage')
      const result = await work()
      if (this.now() - startedAt > STAGES.verify || this.state.deadline - this.now() < 420000)
        throw new Error('Independent verification exceeded original reserve')
      row.status = 'passed'
      return result
    } catch (error) {
      row.status = 'failed'
      row.error = error.message
      this.state.errors.push({ stage: 'verify', message: error.message })
      throw error
    } finally {
      row.completedAt = this.now()
      this.state.stages.push(row)
      write(STATE, this.state)
    }
  }

  /**
   * Establishes empty-daemon ownership before startup and retains it on partial failure.
   * @returns {void}
   * @throws {Error} On any pre-existing container/network/volume or wrong new identities.
   */
  startup() {
    if (
      this.command('docker', ['ps', '-aq']) ||
      this.command('docker', ['volume', 'ls', '-q']) ||
      this.command('docker', [
        'network',
        'ls',
        '--filter',
        'label=com.docker.compose.project',
        '-q',
      ])
    )
      throw new Error('Foreign or reused daemon resources')
    this.state.ownership = {
      project: this.state.project,
      emptyAt: this.now(),
      containers: [],
      runner: this.env.RUNNER_NAME,
    }
    write(STATE, this.state)
    this.compose(['up', '-d'])
    let lastError
    const due = Math.min(this.state.origin + 600000, this.now() + 180000)
    while (this.now() <= due) {
      try {
        this.facts()
        return
      } catch (error) {
        lastError = error
      }
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 1000)
    }
    throw lastError ?? new Error('Cluster readiness exhausted')
  }

  /**
   * Captures original container/image/configuration identities and executable broker versions.
   * @returns {void}
   * @throws {Error} On wrong project, services, image, nodes, state or broker version.
   */
  facts() {
    const ids = this.compose(['ps', '-aq']).split('\n').filter(Boolean)
    const containers = JSON.parse(this.command('docker', ['inspect', ...ids]))
    if (
      new Set(containers.map((c) => c.Config.Labels['com.docker.compose.service'])).size !== 3 ||
      containers.length !== 3 ||
      new Set(ids).size !== 3 ||
      containers.some(
        (c) =>
          c.Config.Labels['com.docker.compose.project'] !== this.state.project ||
          !['kafka1', 'kafka2', 'kafka3'].includes(c.Config.Labels['com.docker.compose.service']) ||
          c.Config.Hostname !== c.Config.Labels['com.docker.compose.service'] ||
          c.Config.Image !== 'apache/kafka:4.3.1' ||
          c.State.Running !== true
      )
    )
      throw new Error('Wrong owned cluster tuple')
    const versions = containers.map((c) => ({
      id: c.Id,
      version: this.command('docker', [
        'exec',
        c.Id,
        '/opt/kafka/bin/kafka-topics.sh',
        '--version',
      ]),
    }))
    if (versions.some((v) => !/^4\.3\.1(?:\s|$)/.test(v.version)))
      throw new Error('Wrong broker version')
    // Kafka CLI readiness queries must succeed on all three original nodes.
    for (const c of containers)
      this.command('docker', [
        'exec',
        c.Id,
        '/opt/kafka/bin/kafka-broker-api-versions.sh',
        '--bootstrap-server',
        `${c.Config.Hostname}:${{ kafka1: 29092, kafka2: 29095, kafka3: 29098 }[c.Config.Hostname]}`,
      ])
    const observedIds = containers.map((c) => c.Id)
    if (
      this.state.ownership.containers.length &&
      JSON.stringify([...this.state.ownership.containers].sort()) !==
        JSON.stringify([...observedIds].sort())
    )
      throw new Error('Original owned container identities changed')
    this.state.ownership.containers = observedIds
    const facts = {
      at: this.now(),
      containers,
      versions,
      images: JSON.parse(
        this.command('docker', ['image', 'inspect', ...new Set(containers.map((c) => c.Image))])
      ),
      compose: this.compose(['config']),
    }
    if (!fs.existsSync(`${OUTPUT}/broker-startup.json`))
      write(`${OUTPUT}/broker-startup.json`, facts)
    write(`${OUTPUT}/broker-facts.json`, facts)
  }

  /**
   * Admits and runs exactly one fixed parent with its full, unshortened watchdog allocation.
   * @returns {void}
   * @throws {Error} On admission refusal or unsuccessful parent.
   */
  parent(work) {
    admit(this.state, this.now())
    const at = this.now()
    write(`${OUTPUT}/admission.json`, {
      at,
      origin: this.state.origin,
      deadline: this.state.deadline,
      requiredMs: PROFILES[this.state.profile].parentMs + 600000,
      remainingMs: this.state.deadline - at,
    })
    const contract = read(CONTRACT)
    if (
      contract.executable !== process.execPath ||
      JSON.stringify(contract.environment) !==
        JSON.stringify(environmentIdentity(childEnvironment(this.state.profile, this.env)))
    )
      throw new Error('Parent runtime/environment differs from frozen contract')
    if (JSON.stringify(this.source()) !== JSON.stringify(contract.source))
      throw new Error('Source changed before parent spawn')
    const result = work()
    if (JSON.stringify(this.source()) !== JSON.stringify(contract.source))
      throw new Error('Source changed during parent')
    return result
  }

  /** @returns {void} Logs captured while the owned cluster exists. @throws {Error} On logging failure. */
  logs() {
    fs.writeFileSync(`${OUTPUT}/compose.log`, this.compose(['logs', '--no-color']))
  }

  /**
   * Cleans only resources whose original empty-daemon ownership was established, even after partial startup.
   * @returns {void}
   * @throws {Error} On foreign project identity, failed down or remaining owned resources.
   */
  teardown() {
    if (!this.state.ownership) throw new Error('No ownership authority for destructive cleanup')
    const ids = this.compose(['ps', '-aq']).split('\n').filter(Boolean)
    if (ids.length) {
      const rows = JSON.parse(this.command('docker', ['inspect', ...ids]))
      if (rows.some((c) => c.Config.Labels['com.docker.compose.project'] !== this.state.project))
        throw new Error('Foreign cleanup target')
    }
    this.compose(['down', '--volumes', '--remove-orphans'])
    const filter = `label=com.docker.compose.project=${this.state.project}`
    const remaining = {
      containers: this.command('docker', ['ps', '-aq', '--filter', filter]),
      networks: this.command('docker', ['network', 'ls', '-q', '--filter', filter]),
      volumes: this.command('docker', ['volume', 'ls', '-q', '--filter', filter]),
    }
    write(`${OUTPUT}/teardown.json`, { at: this.now(), project: this.state.project, remaining })
    if (Object.values(remaining).some(Boolean)) throw new Error('Owned resources remain')
  }
}

/**
 * Seals complete or partial evidence after all stage records are closed. Never rewrites inputs.
 * @param {string} root Exact archive directory.
 * @returns {object} Deterministic inventory seal, including incomplete disposition.
 * @throws {Error} On source mismatch; partial files can still be inventoried by the caller.
 */
const seal = (root) => {
  const contract = read(path.join(root, 'contract.json'))
  const state = read(path.join(root, 'lifecycle.json'))
  const source = sourceIdentity()
  if (JSON.stringify(source) !== JSON.stringify(contract.source))
    throw new Error('Final source mismatch')
  const complete =
    !state.errors.length &&
    ['startup', 'parent', 'verify', 'logs', 'teardown', 'seal'].every((name) =>
      state.stages.some((s) => s.name === name && s.status === 'passed')
    )
  const result = {
    schema: 'share-soak-seal-v1',
    root: '.',
    excluded: ['seal.json'],
    complete,
    contractSha256: digest(fs.readFileSync(path.join(root, 'contract.json'))),
    files: inventory(root),
  }
  write(path.join(root, 'seal.json'), result)
  return result
}

/** @param {string[]} argv Fixed action/profile. @returns {void} @throws {Error} On failed action. */
const main = async (argv) => {
  const [action, profile] = argv
  const job = new ShareSoakJob()
  if (action === 'freeze') return job.freeze(profile)
  if (!fs.existsSync(STATE)) {
    fs.mkdirSync(OUTPUT, { recursive: true })
    write(`${OUTPUT}/${action}-error.json`, {
      action,
      message: 'Lifecycle was not frozen; no cleanup authority',
      at: monotonicMs(),
    })
    throw new Error('Missing frozen lifecycle')
  }
  job.state = read(STATE)
  if (action === 'seal') {
    let failed
    try {
      job.stage(
        'seal',
        () => {
          if (JSON.stringify(sourceIdentity()) !== JSON.stringify(read(CONTRACT).source))
            throw new Error('Source mismatch')
        },
        STAGES.seal
      )
    } catch (error) {
      failed = error
    }
    // Failure archives remain uploadable, but never carry complete:true.
    try {
      seal(OUTPUT)
    } catch (error) {
      write(`${OUTPUT}/seal-error.json`, { message: error.message })
      write(`${OUTPUT}/seal.json`, {
        schema: 'share-soak-seal-v1',
        complete: false,
        files: inventory(OUTPUT),
      })
    }
    if (failed || job.state.errors.length) throw failed ?? new Error('Incomplete lifecycle')
    return
  }
  if (action === 'verify')
    return job.verifyStage(async () => {
      const { validateArchive } = require('../evidence/shareSoakAcceptance')
      job.facts()
      const result = await validateArchive(read(CONTRACT), OUTPUT, { sealed: false })
      write(`${OUTPUT}/validation.json`, result)
      if (result.problems.length) throw new Error(result.problems.join('\n'))
    })
  if (!['startup', 'logs', 'teardown'].includes(action)) throw new Error('Unknown lifecycle action')
  return job.stage(
    action,
    () => job[action](),
    action === 'startup'
      ? 240000
      : action === 'parent'
        ? PROFILES[job.state.profile].parentMs
        : STAGES[action]
  )
}
if (require.main === module) {
  main(process.argv.slice(2)).catch((error) => {
    console.error(error)
    process.exitCode = 1
  })
}
module.exports = {
  ShareSoakJob,
  sourceIdentity,
  verifyMergeParents,
  admit,
  inventory,
  seal,
  STAGES,
}
