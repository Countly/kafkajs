/**
 * The mutation runner. It runs an inventory of mutants, one at a time, and writes one JSON line per
 * record: `{ id, area, target, line, operator, original, mutated, witness, verdict, reason, durationMs }`.
 *
 * A record mutates one target file, either by a committed patch (`patches/<id>.patch`, pinned by
 * `patchSha`) or inline (line `line` must read `original`; it becomes `mutated`). For each record the
 * runner checks the mutation still applies, copies the tree, runs the witness as a control on the
 * untouched copy, applies the mutation to the copy and runs the witness again.
 *
 * Kill rule. The control must pass with exit 0 and no signal, hook failure or run error.
 * The mutant must end with Jest's ordinary failing exit 1, without a signal or process timeout.
 * The run must contain exactly one suite, the frozen spec. Exactly one test must have the frozen full name.
 * Its sole registration must match the frozen spec and Jest's reported location.
 * The environment records lifecycle events for that same test object.
 * The witness must fail through its own ordinary test_fn_failure, on its only attempt.
 * The sole hook exception permits ordinary afterEach failures after that body failure.
 * That exception requires hookTimeoutsRecorded === true and exactly this recorded sequence:
 * test_start, test_fn_start, test_fn_failure, zero or more hook_failure:afterEach, test_done.
 * Controls never use this exception. All run hook failures and all other witness hook failures
 * remain invalid. Body timeouts, hook timeouts, run errors, retries and .failing remain invalid.
 * A body success followed by a failed test is invalid, including snapshot and assertion-count errors.
 * An import or setup failure cannot supply the witness's body failure, nor can a body failure Jest
 * raised loading a module (parse, transform or resolution). A mutation that does not compile is invalid.
 * Spec bytes must equal the control's before, between and after both runs.
 * Target bytes must equal the patched bytes after the mutant run.
 * A mutation never edits its witness spec. Self-targeting records and spec-changing patches are invalid.
 * Stale hunks, absent or ambiguous witnesses, survivors and missing event evidence cannot be kills.
 *
 * The control freezes the witness's identity: full name and the line and column of its registration.
 * The mutant run must report exactly the control's identities, so a test that moved, appeared, vanished
 * or was skipped under the mutation makes the run invalid.
 *
 * A record without a title (a generated mutant) names only its spec. Every test that ran in it is
 * judged by the same rule: the control needs every one to pass; the mutant is killed when at least one
 * fails through its own body and none is refused. The killing test is reported as the witness.
 *
 * A record may list several witness specs in `witnesses` (two or more, the first equal to `spec`). Each
 * runs on its own copy of the tree with its own control, in order, and the first kill decides: the
 * mutant is killed by that witness and the rest do not run. Without a kill it survives when any witness
 * survived, and is otherwise as invalid as its first witness; the reason lists every witness. A time cap
 * that passes between two witnesses leaves the record `not-run`, so it still bounds a record's length;
 * the reason lists the verdicts of the witnesses that already ran.
 *
 * Verdicts: `killed`, `survived`, `invalid` (the evidence cannot decide), `stale` (the mutation no longer
 * applies) and `not-run` (`live` records run without `--live-evidence`, and records past the time cap).
 *
 * A `live` record runs only with `--live-evidence <file>`: guard evidence (see `liveEvidence.js`) with
 * every field present, lane `docker` and the SHA of the tree under test. Otherwise it is `invalid` with
 * the evidence's refusal; without the option it is located and reported `not-run: live`.
 *
 * Threat model: the specs are trusted repository content. The runner guards against accidental
 * misattribution and stale or foreign evidence, not against a deliberately hostile spec. The
 * environment keeps its record in memory and writes it once, at teardown, to a parent-chosen file
 * carrying a per-run nonce; spec and target are hashed after each run.
 *
 * Isolation: the runner never writes the checkout. Each record runs in a disposable directory,
 * `.tmp/mutants/record-<random>/`: a copy of the tree (everything but `.git`, `.tmp`, `coverage` and
 * `node_modules`, which is linked) where the mutation is applied and both runs happen, with their
 * Jest cache, report and event record beside it. The directory is removed when the record ends. A
 * process a run leaves behind can only write a copy no later record reads, so the checkout needs no
 * restoring after a crash, a timeout or a kill, and runs on one checkout never block one another.
 * `.tmp/` is ignored by Git and npm. Links would defeat the copy, so a tree whose copy holds a link
 * resolving outside the copy, or whose `node_modules` holds a package resolving into the tree, is
 * refused before anything runs. The packages `node_modules` shares with the copy are trusted.
 *
 * Each command runs in a new session whose process group is killed when the command returns. That is
 * hygiene, not a safety proof: a descendant that leaves the group lives on until it ends by itself.
 *
 * The CLI runs the records in a worker in its own session. On SIGTERM, SIGINT or SIGHUP it kills the
 * worker. It exits with the worker's status, or 128 + the signal that stopped the worker.
 *
 * Usage:
 *   node scripts/mutants/runner.js <inventory.json>... [--root <dir>] [--patches <dir>] [--out <ledger.jsonl>]
 *     [--shard <i>/<n>] [--time-cap <minutes>] [--only <id>] [--live-evidence <file>]
 * An inventory is `{ records: [...] }`; `generate.js` prints one. Without `--out` the lines go to stdout.
 * The exit status is 0 when the run completed, whatever the verdicts: the result is a report.
 */
const crypto = require('crypto')
const fs = require('fs')
const os = require('os')
const path = require('path')
const vm = require('vm')
const { spawn, spawnSync } = require('child_process')
const { LiveEvidence } = require('./liveEvidence')

const CLASSES = ['unit', 'live']
const SIGNALS = ['SIGTERM', 'SIGINT', 'SIGHUP']
const NOT_COPIED = ['.git', '.tmp', 'coverage', 'node_modules']
const POPULATION_CHANGED = "the mutant run's tests are not the control's"
const sha256 = (data) => crypto.createHash('sha256').update(data).digest('hex')
/**
 * @param {string} dir an absolute directory
 * @param {string} file an absolute path
 * @returns {boolean} whether the path is the directory or lies under it
 */
const inside = (dir, file) => file === dir || file.startsWith(dir + path.sep)
/**
 * @param {string} file a path
 * @returns {string|null} its real path, or null when it resolves nowhere
 */
const realPath = (file) => {
  try {
    return fs.realpathSync(file)
  } catch {
    return null
  }
}

/**
 * `child_process.spawnSync` in a new session; the session's process group is killed once the command
 * returns, so the processes it left in that group do not outlive it.
 * @param {string} command
 * @param {string[]} args
 * @param {import('child_process').SpawnSyncOptions} [options]
 * @returns {import('child_process').SpawnSyncReturns<string|Buffer>} its result
 */
const spawnGroup = (command, args, options = {}) => {
  const result = spawnSync(command, args, { ...options, detached: true })
  try {
    if (result.pid) process.kill(-result.pid, 'SIGKILL')
  } catch {}
  return result
}

/** Runs mutant records against a tree and reports a verdict for each. */
class MutantRunner {
  /**
   * @param {object} [options]
   * @param {(record: object, root: string, outFile: string, run: { evidence: string, nonce: string, cache: string }) => { status: number, signal?: string|null }} [options.exec]
   *   runs the record's witness once in `root`, the record's copy of the tree, writes Jest JSON to
   *   `outFile` and the environment's event record to `run.evidence` under `run.nonce`; `run.cache` is the
   *   record's Jest cache. The default runs Jest for the one spec (and exact title) under a timeout.
   * @param {() => number} [options.now] the clock, in milliseconds
   * @param {string} [options.liveEvidence] the guard-evidence file live records need to run
   * @param {(root: string) => string} [options.treeSha] the commit of the tree under test; the default asks git
   */
  constructor({ exec, now, liveEvidence, treeSha } = {}) {
    this.exec = exec || MutantRunner.jestExec
    this.now = now || Date.now
    this.liveEvidence = liveEvidence
    this.treeSha =
      treeSha ||
      ((root) =>
        spawnSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).stdout.trim())
  }

  /**
   * The default witness execution: one spec, the exact title when there is one, test locations in the
   * report, a private cache, and the runner's own test environment, which extends the project's and
   * writes its event record to `run.evidence`.
   * @param {{ spec: string, title?: string }} record
   * @param {string} root the record's copy of the tree
   * @param {string} outFile the Jest JSON file to write
   * @param {{ evidence: string, nonce: string, cache: string }} run the parent-chosen event file, the
   *   run's nonce and the Jest cache directory
   * @returns {{ status: number, signal: string|null }} the exit status (124 or 137 for a timeout or kill) and any signal
   */
  static jestExec(record, root, outFile, run) {
    const escape = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    const jest = ['--kill-after=30', '600', 'node', 'node_modules/jest/bin/jest.js']
    jest.push('--maxWorkers=2', `--cacheDirectory=${run.cache}`)
    const shown = spawnGroup(
      'timeout',
      ['--kill-after=30', '120', ...jest.slice(2), '--showConfig'],
      {
        cwd: root,
        encoding: 'utf8',
      }
    )
    let baseEnvironment
    try {
      baseEnvironment = JSON.parse(shown.stdout).configs[0].testEnvironment
    } catch {
      return { status: -1, signal: null }
    }
    const args = [
      ...jest,
      `--testEnvironment=${path.join(__dirname, 'mutantEnvironment.js')}`,
      '--testLocationInResults',
      '--json',
      `--outputFile=${outFile}`,
      '--testPathPatterns',
      `^${escape(record.spec)}$`,
    ]
    if (record.title) args.push('--testNamePattern', `(^| )${escape(record.title)}$`)
    const r = spawnGroup('timeout', args, {
      cwd: root,
      encoding: 'utf8',
      env: {
        ...process.env,
        MUTANT_BASE_ENVIRONMENT: baseEnvironment,
        MUTANT_EVIDENCE_FILE: run.evidence,
        MUTANT_NONCE: run.nonce,
      },
    })
    return { status: r.status ?? -1, signal: r.signal || null }
  }

  /**
   * Judges the witness's events. Controls require one plain body success and no failed hook.
   * A failed mutant may have ordinary afterEach failures only in the exact sequence below.
   * The recorder must distinguish hook timeouts. Legacy records cannot use this exception.
   * Run errors, other failed hooks, retries, .failing and timeouts remain invalid.
   * @param {{ failing: boolean, hookTimeoutsRecorded?: boolean, events: string[] }} test the witness record
   * @param {string[]} run events that belong to no test
   * @param {'failed'|'passed'} reported the witness status in Jest's report
   * @returns {{ status: 'failed'|'passed'|'setup'|'error', diagnostic: string }} the body outcome or refusal
   */
  static judgeEvents(test, run, reported = 'failed') {
    const { events } = test
    if (
      events.includes('test_retry') ||
      run.includes('describe_retry') ||
      events.filter((e) => e === 'test_start').length > 1
    )
      return { status: 'error', diagnostic: 'a retry ran: retries are not supported' }
    if (test.failing)
      return { status: 'error', diagnostic: 'a failing-mode witness is not supported' }
    if (run.includes('error'))
      return {
        status: 'error',
        diagnostic: 'an error the body does not own (unhandled rejection, timer or process error)',
      }
    if (events.includes('test_fn_failure:timeout'))
      return { status: 'error', diagnostic: 'the body timed out' }
    if (events.includes('test_fn_failure:load'))
      return {
        status: 'setup',
        diagnostic: 'the body failed loading a module (parse, transform or resolution)',
      }
    if ([...events, ...run].some((e) => e.startsWith('hook_failure:') && e.endsWith(':timeout')))
      return { status: 'error', diagnostic: 'a hook timed out' }
    const failedBodyThenCleanup =
      reported === 'failed' &&
      test.hookTimeoutsRecorded === true &&
      events.length >= 4 &&
      events[0] === 'test_start' &&
      events[1] === 'test_fn_start' &&
      events[2] === 'test_fn_failure' &&
      events.at(-1) === 'test_done' &&
      events.slice(3, -1).every((e) => e === 'hook_failure:afterEach')
    if (
      run.some((e) => e.startsWith('hook_failure')) ||
      (events.some((e) => e.startsWith('hook_failure')) && !failedBodyThenCleanup)
    )
      return { status: 'setup', diagnostic: 'a hook failed' }
    if (reported === 'passed') {
      const at = events.indexOf('test_fn_start')
      const ok = events.indexOf('test_fn_success')
      const plain =
        at !== -1 &&
        ok > at &&
        events.lastIndexOf('test_done') > ok &&
        !events.some((e) => e.startsWith('test_fn_failure'))
      return plain
        ? { status: 'passed', diagnostic: '' }
        : { status: 'error', diagnostic: 'no plain body success was recorded' }
    }
    if (events.includes('test_fn_success'))
      return {
        status: 'error',
        diagnostic:
          'the body passed but the test failed (inline snapshot, expect.assertions or a late error)',
      }
    if (events.includes('test_fn_failure') && events.includes('test_done'))
      return { status: 'failed', diagnostic: '' }
    return { status: 'error', diagnostic: 'no body failure was recorded' }
  }

  /**
   * Validates an inventory's records.
   * @param {object[]} records
   * @returns {object[]} the same records
   * @throws {Error} on a malformed, duplicate, self-targeting or escaping record, or one whose target
   *   lies under `node_modules`, which every copy shares
   */
  static validate(records) {
    if (!Array.isArray(records)) throw new Error('an inventory holds a records array')
    const seen = new Set()
    for (const r of records) {
      const id = r && r.id
      for (const f of ['id', 'area', 'target', 'spec', 'commandClass'])
        if (typeof r[f] !== 'string' || r[f] === '')
          throw new Error(`a record lacks ${f}: ${JSON.stringify(r)}`)
      if (r.title !== undefined && typeof r.title !== 'string')
        throw new Error(`${id}: title is not a string`)
      if (!CLASSES.includes(r.commandClass))
        throw new Error(`${id}: unknown commandClass ${r.commandClass}`)
      const inline =
        Number.isInteger(r.line) &&
        r.line > 0 &&
        typeof r.original === 'string' &&
        typeof r.mutated === 'string'
      const patched = typeof r.patchSha === 'string' && /^[0-9a-f]{64}$/.test(r.patchSha)
      const stale = typeof r.stale === 'string' && r.stale !== ''
      if (!inline && !patched && !stale)
        throw new Error(`${id}: neither a patch, an inline mutation nor a stale reason`)
      if (r.witnesses !== undefined) {
        const w = r.witnesses
        if (
          !Array.isArray(w) ||
          w.length < 2 ||
          w.some((p) => typeof p !== 'string' || p === '') ||
          new Set(w).size !== w.length ||
          w[0] !== r.spec
        )
          throw new Error(
            `${id}: witnesses lists at least two distinct specs, the first being spec`
          )
      }
      const specs = MutantRunner.specsOf(r)
      if (specs.includes(r.target))
        throw new Error(`${id}: a mutation never edits its witness spec`)
      for (const p of [r.target, ...specs])
        if (path.isAbsolute(p) || p.split(/[\\/]/).includes('..'))
          throw new Error(`${id}: a path escapes the tree`)
      if (path.normalize(r.target).split(/[\\/]/).includes('node_modules'))
        throw new Error(`${id}: a mutation never edits node_modules`)
      if (seen.has(id)) throw new Error(`${id} appears twice`)
      seen.add(id)
    }
    return records
  }

  /**
   * @param {string} text `<i>/<n>`, 1 ≤ i ≤ n
   * @returns {{ index: number, count: number }} the zero-based shard index and the shard count
   * @throws {Error} on anything else
   */
  static parseShard(text) {
    const m = /^(\d+)\/(\d+)$/.exec(String(text))
    const index = m && Number(m[1])
    const count = m && Number(m[2])
    if (!m || index < 1 || count < 1 || index > count)
      throw new Error(`--shard needs <i>/<n> with 1 <= i <= n, got ${text}`)
    return { index: index - 1, count }
  }

  /**
   * @param {{ spec: string, witnesses?: string[] }} record
   * @returns {string[]} the record's witness specs, in the order they run
   */
  static specsOf(record) {
    return record.witnesses || [record.spec]
  }

  /**
   * @param {{ spec: string, title?: string, witnesses?: string[] }} record
   * @param {string} [name] the test that decided, for a spec-level witness
   * @returns {string} the witness as `spec › full name`; without one, every witness spec of the record
   */
  static witnessName(record, name = record.title) {
    return name ? `${record.spec} › ${name}` : MutantRunner.specsOf(record).join(', ')
  }

  /**
   * Reads one witness result from a Jest report and the environment's event record.
   * @param {string} file Jest JSON
   * @param {string} eventsFile the environment's event record
   * @param {{ spec: string, title?: string }} record
   * @param {string} root the tree the run used
   * @param {string} nonce the run's nonce, which the event record must carry
   * @returns {{ status: 'passed'|'failed'|'absent'|'error'|'setup'|'pending', diagnostic: string, name?: string, tests?: string[] }}
   *   `error` includes a witness without a test location, a trustworthy registration or a body-owned
   *   failure; `setup` is a failure the body did not cause; `name` is the test that decided; `tests`,
   *   for `passed` and `failed`, are the identities (full name, line, column) of the tests judged
   */
  witness(file, eventsFile, record, root, nonce) {
    const result = (status, diagnostic, name) => ({ status, diagnostic, name })
    const identities = (judged) =>
      judged.map((a) => `${a.fullName}\u0000${a.location.line}:${a.location.column}`).sort()
    let r
    try {
      r = JSON.parse(fs.readFileSync(file, 'utf8'))
    } catch {
      return result('error', 'no readable Jest JSON')
    }
    if (r.numRuntimeErrorTestSuites > 0 || r.wasInterrupted)
      return result('error', 'a suite runtime error or an interrupted run')
    const suites = r.testResults || []
    if (suites.length !== 1 || path.relative(root, suites[0].name || '') !== record.spec)
      return result('error', `the run did not hold exactly ${record.spec}`)
    const [suite] = suites
    const readEvidence = () => {
      let evidence
      try {
        evidence = JSON.parse(fs.readFileSync(eventsFile, 'utf8'))
      } catch {
        return 'no readable event record'
      }
      if (
        typeof nonce !== 'string' ||
        evidence.nonce !== nonce ||
        !Array.isArray(evidence.tests) ||
        !Array.isArray(evidence.run)
      )
        return "the event record was not written by this run's environment"
      return evidence
    }
    /** Judges one reported test against its own registration and events. */
    const judge = (a, evidence) => {
      const diagnostic = (a.failureMessages || [])[0]?.split('\n')[0] || ''
      const { line, column } = a.location || {}
      if (!Number.isInteger(line) || !Number.isInteger(column))
        return result('error', 'the report carries no test location', a.fullName)
      const hits = evidence.tests.filter((t) => t.fullName === a.fullName)
      if (hits.length !== 1)
        return result('error', `${hits.length} registrations have the frozen full name`, a.fullName)
      const [test] = hits
      if (!Array.isArray(test.events))
        return result('error', 'the event record of the witness is malformed', a.fullName)
      if (test.file !== suite.name)
        return result('error', 'the witness was registered from another file', a.fullName)
      if (test.line !== line || test.column !== column)
        return result(
          'error',
          'the registration and the reported test location disagree',
          a.fullName
        )
      if (a.status !== 'failed' && a.status !== 'passed')
        return result(a.status, diagnostic, a.fullName)
      const judged = MutantRunner.judgeEvents(test, evidence.run, a.status)
      const why =
        judged.status === 'passed'
          ? ''
          : judged.status === 'failed'
            ? diagnostic
            : judged.diagnostic
      return result(judged.status, why, a.fullName)
    }

    if (record.title) {
      const matches = suite.assertionResults.filter((a) => a.fullName === record.title)
      if (matches.length !== 1)
        return result('absent', `${matches.length} assertions have the frozen full name`)
      const evidence = readEvidence()
      if (typeof evidence === 'string') return result('error', evidence)
      const judged = judge(matches[0], evidence)
      if (judged.status === 'passed' || judged.status === 'failed')
        judged.tests = identities(matches)
      return judged
    }

    const ran = suite.assertionResults.filter((a) => a.status === 'passed' || a.status === 'failed')
    if (ran.length === 0) return result('absent', 'no test of the spec ran')
    const evidence = readEvidence()
    if (typeof evidence === 'string') return result('error', evidence)
    let killer
    for (const a of ran) {
      const judged = judge(a, evidence)
      if (judged.status !== 'passed' && judged.status !== 'failed') return judged
      if (judged.status === 'failed' && !killer) killer = judged
    }
    return { ...(killer || result('passed', '')), tests: identities(ran) }
  }

  /**
   * @param {string} file the target, relative or absolute
   * @param {string} text its source
   * @returns {string|null} why a JavaScript or JSON source does not compile, or null (other files are not checked)
   */
  static compileError(file, text) {
    try {
      if (file.endsWith('.json')) JSON.parse(text)
      else if (/\.c?js$/.test(file))
        vm.compileFunction(text, ['exports', 'require', 'module', '__filename', '__dirname'], {
          filename: file,
        })
      return null
    } catch (error) {
      return error.message
    }
  }

  /**
   * Checks that a record's mutation still applies to the tree, without changing it.
   * @param {object} record
   * @param {string} root
   * @param {string} patchDir directory with `<id>.patch`
   * @returns {{ line?: number, stale?: string, invalid?: string }} where it applies, or why not
   */
  locate(record, root, patchDir) {
    const target = path.join(root, record.target)
    if (!fs.existsSync(target)) return { stale: 'the target no longer exists' }
    if (record.patchSha) {
      const patchFile = path.resolve(patchDir, `${record.id}.patch`)
      if (!fs.existsSync(patchFile) || sha256(fs.readFileSync(patchFile)) !== record.patchSha)
        return { invalid: 'the patch is missing or is not the frozen one' }
      const dry = spawnSync('patch', MutantRunner.patchArgs(root, patchFile, '--dry-run'), {
        encoding: 'utf8',
      })
      if (dry.status !== 0) return { stale: `stale hunk: ${MutantRunner.lastLine(dry.stdout)}` }
      const offset = /Hunk #1 succeeded at \d+ \(offset (-?\d+) lines?\)/.exec(dry.stdout)
      const shift = offset ? Number(offset[1]) : 0
      return { line: Number.isInteger(record.line) ? record.line + shift : undefined }
    }
    const lines = fs.readFileSync(target, 'utf8').split('\n')
    if (lines[record.line - 1] !== record.original)
      return { stale: `line ${record.line} no longer reads the original` }
    return { line: record.line }
  }

  /**
   * Applies a record's mutation to the tree.
   * @param {object} record
   * @param {string} root
   * @param {string} patchDir
   * @returns {boolean} whether it applied
   */
  apply(record, root, patchDir) {
    if (record.patchSha) {
      const patchFile = path.resolve(patchDir, `${record.id}.patch`)
      return spawnSync('patch', MutantRunner.patchArgs(root, patchFile)).status === 0
    }
    const target = path.join(root, record.target)
    const lines = fs.readFileSync(target, 'utf8').split('\n')
    if (lines[record.line - 1] !== record.original) return false
    lines[record.line - 1] = record.mutated
    fs.writeFileSync(target, lines.join('\n'))
    return true
  }

  /**
   * @param {string} root
   * @param {string} patchFile
   * @param {...string} extra more `patch` flags
   * @returns {string[]} the `patch` arguments: exact context, from the tree's root
   */
  static patchArgs(root, patchFile, ...extra) {
    return [
      '--batch',
      '--forward',
      '--no-backup-if-mismatch',
      '--fuzz=0',
      '-p1',
      ...extra,
      '-d',
      root,
      '-i',
      patchFile,
    ]
  }

  /**
   * @param {string} text
   * @returns {string} its last non-empty line
   */
  static lastLine(text) {
    return String(text).trim().split('\n').filter(Boolean).at(-1) || 'patch refused it'
  }

  /**
   * Copies a tree for one record, leaving out what {@link NOT_COPIED} names and linking its
   * `node_modules`. Links that would let a run read or write the checkout are refused before anything
   * runs: every link in the copy must resolve inside the copy, and no package of `node_modules` (top
   * level or scoped) may resolve into the tree outside `node_modules`. Other dependencies are shared
   * and trusted.
   * @param {string} root the checkout's real path
   * @param {string} copy the new directory
   * @returns {string} the copy's real path
   * @throws {Error} when the copy fails or a link leaves it
   */
  static copy(root, copy) {
    fs.mkdirSync(copy)
    const tree = fs.realpathSync(copy)
    const items = fs.readdirSync(root).filter((name) => !NOT_COPIED.includes(name))
    const r = spawnSync('cp', ['-R', '--', ...items, tree], { cwd: root, encoding: 'utf8' })
    if (r.status !== 0)
      throw new Error(`cannot copy ${root}: ${(r.stderr || r.error?.message || '').trim()}`)
    const walk = (dir) => {
      for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
        const file = path.join(dir, e.name)
        if (e.isDirectory()) walk(file)
        else if (e.isSymbolicLink() && !inside(tree, realPath(file) ?? ''))
          throw new Error(`the tree links outside its copy: ${path.relative(tree, file)}`)
      }
    }
    walk(tree)
    const modules = path.join(root, 'node_modules')
    const shared = realPath(modules)
    const packages = shared
      ? fs
          .readdirSync(shared)
          .flatMap((n) =>
            n.startsWith('@') ? fs.readdirSync(path.join(shared, n)).map((p) => `${n}/${p}`) : [n]
          )
      : []
    for (const name of packages) {
      const to = realPath(path.join(shared, name))
      if (to && inside(root, to) && !inside(shared, to))
        throw new Error(`a dependency links into the tree: node_modules/${name}`)
    }
    fs.symlinkSync(modules, path.join(tree, 'node_modules'))
    return tree
  }

  /**
   * Runs one record: location, then each witness in a disposable copy of the tree, in order, until one
   * kills the mutant. The tree itself is only read.
   * @param {object} record
   * @param {string} root the tree under test, absolute or relative to the working directory
   * @param {string} patchDir directory with `<id>.patch`, absolute or relative to the working directory
   * @param {number} [stopAt] the clock time after which no further witness of the record starts; a
   *   record cut there without a kill is `not-run`
   * @returns {object} the report line, with its verdict
   * @throws {Error} when the tree cannot be copied or links outside its copy
   */
  runOne(record, root, patchDir, stopAt) {
    root = fs.realpathSync(root)
    patchDir = path.resolve(patchDir)
    const started = this.now()
    const entry = { ...this.skeleton(record), verdict: 'invalid', reason: '', durationMs: 0 }
    const finish = (verdict, reason) =>
      Object.assign(entry, { verdict, reason, durationMs: this.now() - started })
    const specs = MutantRunner.specsOf(record)
    if (record.stale) return finish('stale', record.stale)
    if (specs.includes(record.target))
      return finish('invalid', 'a mutation never edits its witness spec')
    const located = this.locate(record, root, patchDir)
    if (located.invalid) return finish('invalid', located.invalid)
    if (located.stale) return finish('stale', located.stale)
    if (located.line) entry.line = located.line
    if (record.commandClass === 'live') {
      if (!this.liveEvidence) return finish('not-run', 'live')
      const refusal = LiveEvidence.refusal(LiveEvidence.read(this.liveEvidence), {
        sha: this.treeSha(root),
      })
      if (refusal) return finish('invalid', refusal)
    }
    const outcomes = []
    const listed = () =>
      outcomes.map((o) => `${o.spec}: ${o.verdict}${o.reason ? ` (${o.reason})` : ''}`).join('; ')
    for (const spec of specs) {
      if (outcomes.length > 0 && stopAt !== undefined && this.now() >= stopAt)
        return finish(
          'not-run',
          `time cap: ${outcomes.length} of ${specs.length} witnesses ran (${listed()})`
        )
      const outcome = { spec, ...this.runWitness({ ...record, spec }, root, patchDir) }
      outcomes.push(outcome)
      if (outcome.verdict === 'killed' || outcome.verdict === 'stale') break
    }
    const last = outcomes.at(-1)
    if (last.verdict === 'killed' || last.verdict === 'stale') {
      if (last.witness) entry.witness = last.witness
      return finish(last.verdict, last.reason)
    }
    const decisive = outcomes.find((o) => o.verdict === 'survived') || outcomes[0]
    if (outcomes.length === 1) return finish(decisive.verdict, decisive.reason)
    return finish(decisive.verdict, listed())
  }

  /**
   * Runs one witness of a record in its own disposable copy of the tree: the control, the mutation
   * and the mutant run of {@link MutantRunner#decide}.
   * @param {object} record the record, its `spec` the witness to run
   * @param {string} root the tree under test
   * @param {string} patchDir directory with `<id>.patch`
   * @returns {{ verdict: string, reason: string, witness?: string }} the witness's verdict
   * @throws {Error} when the tree cannot be copied or links outside its copy
   */
  runWitness(record, root, patchDir) {
    if (!fs.existsSync(path.join(root, record.spec)))
      return { verdict: 'invalid', reason: 'the witness spec is missing' }
    const scratch = path.join(root, '.tmp', 'mutants')
    fs.mkdirSync(scratch, { recursive: true })
    const dir = fs.mkdtempSync(path.join(scratch, 'record-'))
    try {
      const tree = MutantRunner.copy(root, path.join(dir, 'tree'))
      try {
        return this.decide(record, tree, patchDir, dir)
      } catch (error) {
        return { verdict: 'invalid', reason: `the run crashed: ${error.message}` }
      }
    } finally {
      try {
        fs.rmSync(dir, { recursive: true, force: true, maxRetries: 3 })
      } catch {}
    }
  }

  /**
   * The control, the mutation and the mutant run of {@link MutantRunner#runOne}, in the record's copy.
   * @param {object} record
   * @param {string} root the record's copy of the tree
   * @param {string} patchDir directory with `<id>.patch`
   * @param {string} dir the record's directory, for reports, event records and the Jest cache
   * @returns {{ verdict: string, reason: string, witness?: string }} the verdict, and the killing test
   */
  decide(record, root, patchDir, dir) {
    const decided = (verdict, reason, witness) => ({ verdict, reason, witness })
    const target = path.join(root, record.target)
    const specFile = path.join(root, record.spec)
    const original = fs.readFileSync(target)
    const specSha = sha256(fs.readFileSync(specFile))
    const targetSha = sha256(original)
    const shaOf = (file) => (fs.existsSync(file) ? sha256(fs.readFileSync(file)) : null)
    const cache = path.join(dir, 'jest-cache')
    /** One witness run: its verdict, exit, and the spec and target hashes right after it. */
    const execute = (name) => {
      const report = path.join(dir, `${name}.json`)
      const nonce = crypto.randomBytes(16).toString('hex')
      const evidence = path.join(dir, `${name}.${nonce}.events`)
      const run = this.exec(record, root, report, { evidence, nonce, cache })
      return {
        exit: run.status,
        signal: run.signal || null,
        specSha: shaOf(specFile),
        targetSha: shaOf(target),
        verdict: this.witness(report, evidence, record, root, nonce),
      }
    }
    const control = execute('control')
    const c = control.verdict
    if (control.exit !== 0 || control.signal || c.status !== 'passed')
      return decided(
        'invalid',
        `the control is ${c.status}${c.diagnostic ? `: ${c.diagnostic}` : ''} (exit ${control.exit}${
          control.signal ? `, ${control.signal}` : ''
        })`
      )
    if (control.specSha !== specSha || control.targetSha !== targetSha)
      return decided('invalid', 'the spec or the target changed while the control ran')
    if (!this.apply(record, root, patchDir))
      return decided('stale', 'stale hunk: the mutation does not apply')
    const patchedSha = shaOf(target)
    if (shaOf(specFile) !== specSha) return decided('invalid', 'the patch changed the witness spec')
    const broken = MutantRunner.compileError(target, fs.readFileSync(target, 'utf8'))
    if (broken && !MutantRunner.compileError(target, original.toString('utf8')))
      return decided('invalid', `the mutation does not compile: ${broken}`)
    const mutant = execute('mutant')
    const m = mutant.verdict
    if (mutant.exit === 124 || mutant.exit === 137 || mutant.signal)
      return decided(
        'invalid',
        `the mutant run was cut off (exit ${mutant.exit}${mutant.signal ? `, ${mutant.signal}` : ''})`
      )
    if (mutant.specSha !== specSha || mutant.targetSha !== patchedSha)
      return decided('invalid', 'the spec or the target changed while the mutant ran')
    if (
      (m.status === 'failed' || m.status === 'passed') &&
      m.tests.join('\n') !== c.tests.join('\n')
    )
      return decided('invalid', POPULATION_CHANGED)
    if (m.status === 'setup')
      return decided(
        'invalid',
        'the witness body did not fail: a hook, import or setup failure is not a kill'
      )
    if (m.status === 'failed' && mutant.exit !== 1)
      return decided(
        'invalid',
        `Jest ended with exit ${mutant.exit}, not its ordinary failing exit`
      )
    if (m.status === 'failed')
      return decided('killed', '', MutantRunner.witnessName(record, m.name))
    if (m.status === 'passed') return decided('survived', 'the witness still passes')
    return decided('invalid', `the witness is ${m.status}: ${m.diagnostic}`)
  }

  /**
   * Runs the records of one shard, serially, and writes each report line as it is decided. Records
   * the time cap leaves unstarted are reported `not-run`, so every record has a line.
   * @param {object[]} records validated records
   * @param {object} options
   * @param {string} options.root the tree under test
   * @param {string} options.patchDir directory with `<id>.patch`
   * @param {string} [options.out] JSONL file, appended; stdout when absent and `print` is set
   * @param {boolean} [options.print] write lines to stdout when there is no `out`
   * @param {{ index: number, count: number }} [options.shard] keep records whose position modulo `count` is `index`
   * @param {number} [options.timeCapMs] start no record once this much time has passed
   * @param {string} [options.only] run only this id
   * @returns {object[]} the report lines
   * @throws {Error} when `only` names no record of the shard, or the tree cannot be copied or links outside its copy
   */
  run(records, { root, patchDir, out, print, shard, timeCapMs, only }) {
    const started = this.now()
    const mine = records.filter(
      (r, i) => (!shard || i % shard.count === shard.index) && (!only || r.id === only)
    )
    if (only && mine.length === 0) throw new Error(`no record ${only} in this shard`)
    const lines = []
    for (const record of mine) {
      const capped = timeCapMs && this.now() - started >= timeCapMs
      const entry = capped
        ? { ...this.skeleton(record), verdict: 'not-run', reason: 'time cap', durationMs: 0 }
        : this.runOne(record, root, patchDir, timeCapMs ? started + timeCapMs : undefined)
      const text = JSON.stringify(entry) + '\n'
      if (out) fs.appendFileSync(out, text)
      else if (print) process.stdout.write(text)
      lines.push(entry)
    }
    return lines
  }

  /**
   * @param {object} record
   * @returns {object} the record's report fields before any verdict
   */
  skeleton(record) {
    return {
      id: record.id,
      area: record.area,
      target: record.target,
      line: record.line ?? null,
      operator: record.operator ?? null,
      original: record.original ?? null,
      mutated: record.mutated ?? null,
      witness: MutantRunner.witnessName(record),
    }
  }
}

/**
 * The CLI's worker: parses the arguments and runs the records.
 * @param {string[]} args the command-line arguments
 */
const work = (args) => {
  const option = (name) => {
    const at = args.indexOf(name)
    return at === -1 ? undefined : args.splice(at, 2)[1]
  }
  try {
    const root = path.resolve(option('--root') || '.')
    const patchDir = path.resolve(option('--patches') || path.join(__dirname, 'patches'))
    const out = option('--out')
    const shardText = option('--shard')
    const cap = option('--time-cap')
    const only = option('--only')
    const liveEvidence = option('--live-evidence')
    if (args.length === 0) throw new Error('usage: runner.js <inventory.json>... [options]')
    if (cap !== undefined && !(Number(cap) > 0))
      throw new Error(`--time-cap needs minutes > 0, got ${cap}`)
    const records = MutantRunner.validate(
      args.flatMap((file) => JSON.parse(fs.readFileSync(file, 'utf8')).records)
    )
    new MutantRunner({ liveEvidence: liveEvidence && path.resolve(liveEvidence) }).run(records, {
      root,
      patchDir,
      out,
      print: !out,
      shard: shardText ? MutantRunner.parseShard(shardText) : undefined,
      timeCapMs: cap ? Number(cap) * 60000 : undefined,
      only,
    })
  } catch (error) {
    console.error(`mutants: ${error.message}`)
    process.exitCode = 1
  }
}

/**
 * The CLI's supervisor. The worker runs in its own session, so terminal and job signals reach only
 * the supervisor, which does not block. On SIGTERM, SIGINT or SIGHUP it kills the worker. It exits
 * with the worker's status, or 128 + the signal that stopped the worker. The checkout needs nothing
 * after a killed worker: only its record's copy was written.
 * @param {string[]} args the command-line arguments
 */
const supervise = (args) => {
  const worker = spawn(process.execPath, [__filename, ...args], {
    detached: true,
    stdio: 'inherit',
    env: { ...process.env, MUTANT_WORKER: '1' },
  })
  let stopped
  const stop = (signal) => {
    if (stopped || !worker.pid) return
    stopped = signal
    try {
      process.kill(worker.pid, 'SIGKILL')
    } catch {}
  }
  for (const signal of SIGNALS) process.on(signal, stop)
  worker.on('error', (error) => {
    console.error(`mutants: ${error.message}`)
    process.exitCode = 1
  })
  worker.on('exit', (code, signal) => {
    const by = stopped || signal
    process.exitCode = by ? 128 + os.constants.signals[by] : (code ?? 1)
  })
}

module.exports = { MutantRunner }

if (require.main === module) {
  if (process.env.MUTANT_WORKER === '1') work(process.argv.slice(2))
  else supervise(process.argv.slice(2))
}
