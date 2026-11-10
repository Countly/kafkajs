#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const path = require('node:path')
const crypto = require('node:crypto')
const { spawn, execFileSync } = require('node:child_process')
const { performance } = require('node:perf_hooks')
const {
  MEMORY_FLAGS,
  hostContext,
  collectionsSettled,
} = require('../../../testHelpers/processMemory')
const {
  CAPACITY,
  BUDGET,
  CATALOGUE,
  METHOD,
  digest,
  validateEvents,
  validateCatalogue,
} = require('./retained/contract')

const { settlementReport } = require('./retained/settlement')

const ROOT = path.resolve(__dirname, '../../..')

/**
 * Archives executed JS/fixture/judge sources with actual digests, including untracked diagnostic tooling.
 * @param {string} directory evidence root @returns {object[]} stable source manifest
 */
const archiveSources = (directory) => {
  const files = []
  const visit = (relative) => {
    const absolute = path.join(ROOT, relative)
    if (fs.statSync(absolute).isDirectory()) {
      for (const name of fs.readdirSync(absolute).sort()) visit(path.join(relative, name))
    } else if (/\.(js|json)$/.test(relative)) files.push(relative)
  }
  for (const source of ['src', 'testHelpers', 'scripts/e2e/telemetry', 'index.js', 'package.json'])
    visit(source)
  return files.sort().map((file) => {
    const bytes = fs.readFileSync(path.join(ROOT, file))
    const target = path.join(directory, 'sources', file)
    fs.mkdirSync(path.dirname(target), { recursive: true })
    fs.writeFileSync(target, bytes, { flag: 'wx' })
    return { file, bytes: bytes.length, sha256: digest(bytes) }
  })
}

/** @param {string} directory evidence root @param {object[]} sources declared source identities @returns {void} @throws {Error} on missing/changed source bytes */
const verifySources = (directory, sources) => {
  if (!Array.isArray(sources) || sources.length === 0) throw new Error('missing source digests')
  for (const source of sources) {
    if (path.isAbsolute(source.file) || source.file.split(/[\\/]/).includes('..'))
      throw new Error('foreign source path')
    const file = path.join(directory, 'sources', source.file)
    if (fs.lstatSync(file).isSymbolicLink()) throw new Error('source symlink')
    const bytes = fs.readFileSync(file)
    if (bytes.length !== source.bytes || digest(bytes) !== source.sha256)
      throw new Error('wrong source digest')
  }
}

/**
 * One child, an inclusive 90 s watchdog and a 120 s outer join bound; no retries.
 * @param {string} directory new child directory @param {object} identity parent-issued tuple
 * @param {Function} [launch] injectable spawn for broker-free supervision controls
 * @returns {Promise<object>} natural close facts even on failure; no unjoined successful result
 */
const supervise = (directory, identity, launch = spawn) =>
  new Promise((resolve) => {
    fs.mkdirSync(directory, { recursive: true })
    fs.writeFileSync(path.join(directory, 'identity.json'), JSON.stringify(identity))
    const env = {
      ...process.env,
      RETAINED_IDENTITY_FILE: path.join(directory, 'identity.json'),
      RETAINED_DIRECTORY: directory,
    }
    delete env.NODE_V8_COVERAGE
    delete env.NODE_OPTIONS
    const started = performance.now()
    const before = hostContext()
    const child = launch(
      process.execPath,
      [
        ...MEMORY_FLAGS,
        path.join(ROOT, 'src/telemetry/__tests__/fixtures/retainedDiagnosticChild.js'),
      ],
      { env, stdio: ['ignore', 'pipe', 'pipe'] }
    )
    let stdoutBytes = 0
    let stderrBytes = 0
    let pending = ''
    let failure = null
    let timedOut = false
    let receipts = 0
    let results = 0
    let receiptAt = null
    let resultAt = null
    let done = false
    const kill = () => child.kill('SIGKILL')
    const watchdog = setTimeout(() => {
      timedOut = true
      kill()
    }, BUDGET.child)
    let terminalTimer
    const outer = setTimeout(() => {
      failure = [failure, 'outer case envelope exhausted without joined pipes']
        .filter(Boolean)
        .join('; ')
      timedOut = true
      kill()
      finish(null, 'SIGKILL')
    }, BUDGET.case)
    const finish = (code, signal) => {
      if (done) return
      done = true
      clearTimeout(watchdog)
      clearTimeout(outer)
      clearTimeout(terminalTimer)
      const elapsedMs = performance.now() - started
      resolve({
        code,
        signal,
        failure,
        timedOut,
        stdoutBytes,
        stderrBytes,
        receipts,
        results,
        closedAfterReceiptMs: receiptAt === null ? null : elapsedMs - receiptAt,
        elapsedMs,
        resultAt,
        context: { before, after: hostContext() },
        pid: child.pid,
      })
    }
    child.stdout.on('data', (chunk) => {
      stdoutBytes += chunk.length
      if (stdoutBytes > 65536) {
        failure = 'diagnostic stdout overflow'
        kill()
        return
      }
      fs.appendFileSync(path.join(directory, 'stdout.txt'), chunk)
      pending += chunk.toString('utf8')
      while (pending.includes('\n')) {
        const at = pending.indexOf('\n')
        const line = pending.slice(0, at)
        pending = pending.slice(at + 1)
        try {
          const row = JSON.parse(line)
          if (row.childId !== identity.childId) throw new Error('foreign stdout identity')
          if (row.terminal === true) {
            receipts++
            if (receipts !== 1 || results !== 0) throw new Error('duplicate/late terminal receipt')
            receiptAt = performance.now() - started
            terminalTimer = setTimeout(() => {
              timedOut = true
              kill()
            }, BUDGET.receipt)
          } else if (row.result === true) {
            results++
            resultAt = performance.now() - started
            if (results !== 1 || receipts !== 1 || row.failed !== false)
              throw new Error('early/failed/duplicate result')
          } else throw new Error('unknown diagnostic output')
        } catch (error) {
          failure = [failure, error.message].filter(Boolean).join('; ')
          kill()
        }
      }
    })
    child.stderr.on('data', (chunk) => {
      stderrBytes += chunk.length
      if (stderrBytes <= 65536) fs.appendFileSync(path.join(directory, 'stderr.txt'), chunk)
      else {
        failure = 'diagnostic stderr overflow'
        kill()
      }
    })
    child.on('error', (error) => {
      failure = error.message
    })
    child.on('close', (code, signal) => {
      if (pending) failure = 'unterminated diagnostic output'
      finish(code, signal)
    })
  })

/**
 * Verifies sidecar bytes and snapshot hashes without loading them into a measured process.
 * @param {string} directory child directory @param {object} identity expected tuple
 * @param {object} exit joined facts @returns {Promise<object>} completion and numerical screens
 */
const replay = async (directory, identity, exit) => {
  const problems = []
  let events = []
  try {
    const file = path.join(directory, 'events.jsonl')
    if (fs.statSync(file).size > CAPACITY.eventBytes)
      throw new Error('diagnostic event byte overflow')
    events = fs.readFileSync(file, 'utf8').trim().split('\n').map(JSON.parse)
    problems.push(...validateEvents(events, identity, exit))
    for (const capture of events.filter((event) => event.type === 'checkpoint' && event.file)) {
      if (
        path.basename(capture.file) !== capture.file ||
        capture.file !== `${capture.name}.heapsnapshot`
      )
        throw new Error('foreign snapshot path')
      const target = path.join(directory, capture.file)
      if (!fs.lstatSync(target).isFile() || fs.lstatSync(target).isSymbolicLink())
        throw new Error('snapshot is not a regular file')
      if (fs.statSync(target).size !== capture.bytes)
        throw new Error('snapshot byte length mismatch')
      const hash = crypto.createHash('sha256')
      for await (const chunk of fs.createReadStream(target)) hash.update(chunk)
      if (hash.digest('hex') !== capture.sha256) throw new Error('snapshot digest mismatch')
    }
  } catch (error) {
    problems.push(error.message)
  }
  return {
    schema: 1,
    method: METHOD,
    diagnostic: true,
    acceptanceEligible: false,
    complete: problems.length === 0,
    identity,
    exit,
    problems,
    screens: screen(events),
    settlement: problems.length === 0 ? settlementReport(events) : null,
  }
}

/** Diagnostic numerical screens preserve failures, never produce an acceptance verdict. @param {object[]} events scalar events @returns {object} signed original deltas and observed violations */
const screen = (events) => {
  const { LIMITS } = require('./resourceArtifact')
  const problems = []
  const inconclusive = []
  const control = events.find((row) => row.type === 'control')
  const marks = events.filter((row) => row.type === 'mark').map((row) => row.mark)
  if (control) {
    if (Math.abs(control.c1.retained - control.c0.retained) > LIMITS.controlRetained)
      inconclusive.push('control retained bytes are unstable')
    if (Math.abs(control.c1.rss - control.c0.rss) > LIMITS.controlRss)
      inconclusive.push('control RSS is unstable')
  }
  let previous = 0
  for (const mark of marks) {
    if (mark.vmhwm === null) inconclusive.push('peak capability missing')
    else {
      if (mark.vmhwm < previous) problems.push('authoritative peak decreased')
      if (mark.vmhwm < mark.covered)
        inconclusive.push('peak source does not cover a current reading')
      previous = mark.vmhwm
    }
  }
  const readings = events
    .flatMap((event) =>
      event.type === 'checkpoint'
        ? [event.pre, event.post]
        : event.type === 'block'
          ? [event.closed, ...(event.actives ?? [])]
          : []
    )
    .flatMap((reading) => [reading, ...(reading?.gc?.checkpoints ?? [])])
  for (const event of events.filter((row) => row.type === 'checkpoint')) {
    const pairs = event.pre?.gc?.checkpoints
    if (pairs?.length >= 2 && !collectionsSettled(pairs.at(-2), pairs.at(-1)))
      inconclusive.push(`${event.name} collections did not settle`)
  }
  const peak = Math.max(
    0,
    ...marks.map((mark) => Math.max(mark.vmhwm ?? 0, mark.covered ?? 0)),
    ...readings.map((reading) => reading?.rss ?? 0)
  )
  const externalPeak = Math.max(
    0,
    ...marks.map((mark) => mark.coveredExternal ?? 0),
    ...readings.map((reading) => reading?.external ?? 0)
  )
  const baseline = control ? Math.min(control.c0.rss, control.c1.rss) : null
  const externalReference = control ? Math.min(control.c0.external, control.c1.external) : null
  const matchedMark = marks.findIndex((mark) => mark.phase === 'M')
  const setupPeak = Math.max(
    0,
    ...marks
      .slice(0, matchedMark < 0 ? marks.length : matchedMark)
      .map((mark) => Math.max(mark.vmhwm ?? 0, mark.covered ?? 0))
  )
  if (setupPeak > LIMITS.setupPeak) inconclusive.push('setup exceeds 96 MiB')
  if (peak > LIMITS.absolutePeak) problems.push('absolute current-image peak exceeds 160 MiB')
  if (inconclusive.length === 0 && control) {
    if (peak - baseline > LIMITS.rssDelta)
      problems.push('RSS peak exceeds 64 MiB above disabled control')
    if (externalPeak - externalReference > LIMITS.externalDelta)
      problems.push('external peak exceeds 64 MiB above disabled control')
  }
  const closedDeltas = []
  for (const block of events.filter((row) => row.type === 'block')) {
    closedDeltas.push(block.delta)
    if (block.delta > LIMITS.closedTransient)
      problems.push(`block ${block.index}: closed transient bytes exceed 256 KiB`)
    for (const reading of block.actives ?? [])
      for (const point of [reading, ...(reading.gc?.checkpoints ?? [])])
        if (point.retained - block.matchedRetained > LIMITS.retained)
          problems.push(`block ${block.index}: active retained bytes exceed 8 MiB`)
  }
  return {
    peak,
    externalPeak,
    baseline,
    externalReference,
    setupPeak,
    controlRetainedDelta: control ? control.c1.retained - control.c0.retained : null,
    controlRssDelta: control ? control.c1.rss - control.c0.rss : null,
    closedDeltas,
    problems: [...new Set(problems)],
    inconclusive: [...new Set(inconclusive)],
  }
}

/** @param {string[]} argv diagnostic selector and output directory @returns {Promise<void>} sixteen serial children; partial evidence survives errors @throws {Error} outside explicit CI diagnostic use */
const main = async (argv) => {
  const [selector, directory] = argv
  if (
    selector !== '--diagnostic-only' ||
    !directory ||
    process.env.CI !== 'true' ||
    process.env.TELEMETRY_RESOURCES_QUIET !== '1' ||
    !process.version.startsWith('v24.') ||
    process.env.TELEMETRY_RESOURCES_ARTIFACT_DIR
  )
    throw new Error(
      'Use --diagnostic-only <new-directory> only in isolated Node-24 CI; no live/acceptance selector'
    )
  fs.mkdirSync(directory, { recursive: true })
  if (fs.readdirSync(directory).length)
    throw new Error('diagnostic evidence directory must be fresh')
  const sources = archiveSources(directory)
  verifySources(directory, sources)
  const tree = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: ROOT, encoding: 'utf8' }).trim()
  const results = []
  for (const tuple of CATALOGUE) {
    const identity = {
      ...tuple,
      schema: 1,
      diagnostic: true,
      acceptanceEligible: false,
      method: METHOD,
      childId: crypto.randomUUID(),
      tree,
      runId: process.env.GITHUB_RUN_ID,
      node: process.version,
      v8: process.versions.v8,
      flags: MEMORY_FLAGS,
      sources,
    }
    const target = path.join(directory, `${tuple.arm}-${tuple.kind}-${tuple.variant}`)
    const exit = await supervise(target, identity)
    const record = await replay(target, identity, exit)
    fs.writeFileSync(path.join(target, 'record.json'), JSON.stringify(record))
    results.push({
      arm: tuple.arm,
      kind: tuple.kind,
      variant: tuple.variant,
      childId: identity.childId,
      complete: record.complete,
      problems: record.problems,
      screens: record.screens,
    })
    fs.writeFileSync(
      path.join(directory, 'diagnostic-summary.json'),
      JSON.stringify({
        schema: 1,
        diagnostic: true,
        acceptanceEligible: false,
        method: METHOD,
        results,
        complete:
          validateCatalogue(results).length === 0 && results.every((result) => result.complete),
        problems: validateCatalogue(results),
      })
    )
  }
  if (results.some((result) => !result.complete)) process.exitCode = 1
}

module.exports = { archiveSources, verifySources, supervise, replay, screen, main }
if (require.main === module)
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  })
