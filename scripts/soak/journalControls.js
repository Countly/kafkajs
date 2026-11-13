'use strict'

const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const { MEMORY_FLAGS } = require('../../testHelpers/processMemory')
const { TELEMETRY } = require('./soak')
const { WORKER_FLAGS } = require('./telemetryChurn')
const { JournalConstancy } = require('../../testHelpers/fixedJournal')

const KINDS = Object.freeze(['producer', 'consumer', 'admin', 'shareConsumer'])
const PROTOCOLS = Object.freeze(['resource', 'live'])
const SEEDED_VIOLATIONS = Object.freeze(['retained slope', 'retained endpoint'])

/**
 * @param {string} protocol `resource` (the resource workers) or `live` (the telemetry worker)
 * @returns {readonly string[]} the exact node flags a child of that protocol must run with
 */
const flagsOf = (protocol) => (protocol === 'live' ? WORKER_FLAGS : MEMORY_FLAGS)

/**
 * What one calibration arm is held to. A seeded arm must be detected whoever's journal it is; the
 * fixed unseeded arm is the null and must pass; the legacy unseeded arm is a contamination-positive
 * control, since the old harness journals really grew (consumer and share: judged by the retained
 * slope; producer: by paired ordering against its fixed null, as its contamination sits near the
 * limit). The admin writes no legacy journal, so its legacy arm only records. A live-flags unseeded
 * arm records the live-configuration null and is never judged.
 * @param {{kind:string, mode:string, seeded:boolean, protocol:string}} arm the arm
 * @returns {{role:string, judgedBy:string}} its role and what judges it
 */
const armOf = ({ kind, mode, seeded, protocol }) => {
  if (seeded) return { role: 'seeded-positive', judgedBy: 'both violations' }
  if (protocol === 'live') return { role: 'live-configuration-null', judgedBy: 'record only' }
  if (mode === 'fixed') return { role: 'null', judgedBy: 'no violation' }
  if (kind === 'admin') return { role: 'no-contamination-recorded', judgedBy: 'record only' }
  if (kind === 'producer') return { role: 'contamination-positive', judgedBy: 'paired ordering' }
  return { role: 'contamination-positive', judgedBy: 'slope violation' }
}

/**
 * Requires a real component verdict: the child's own evidence-failure fallback has no measured
 * values, so a crash-shaped, partial or inconsistent result is never a detection or a null.
 * @param {object} verdict the child's component verdict
 * @returns {{violations:string[], measured:object}} the verified violations and measured values
 * @throws {assert.AssertionError} unless the scope, measured values and violation labels agree
 */
const wellFormed = (verdict) => {
  assert.equal(verdict?.scope, 'slope-and-endpoint-only', 'component verdict scope')
  const { violations, measured } = verdict
  assert(Array.isArray(violations), 'violations list')
  assert(
    Number.isFinite(measured?.slopeBytesPerMinute) && Number.isFinite(measured?.endpointBytes),
    `measured slope and endpoint (${violations})`
  )
  assert.equal(
    violations.includes('retained slope'),
    measured.slopeBytesPerMinute > TELEMETRY.slopeBytesPerMinute,
    'slope violation agrees with its measured value'
  )
  assert.equal(
    violations.includes('retained endpoint'),
    measured.endpointBytes > TELEMETRY.endpointBytes,
    'endpoint violation agrees with its measured value'
  )
  assert.equal(violations.length, new Set(violations).size)
  assert(
    violations.every((label) => SEEDED_VIOLATIONS.includes(label)),
    'violation labels'
  )
  assert.equal(verdict.ok, violations.length === 0, 'ok agrees with violations')
  return { violations, measured }
}

/**
 * Holds one arm's component verdict to its role. The endpoint violation of a contamination-positive
 * arm is asserted whenever the measured endpoint exceeds the limit (agreement in `wellFormed`).
 * @param {{kind:string, mode:string, seeded:boolean, protocol:string}} arm the arm
 * @param {object} verdict the child's component verdict
 * @returns {void}
 * @throws {assert.AssertionError} for a missed detection, an unexpected legacy pass, or a failed null
 */
const judgeArm = (arm, verdict) => {
  const { violations } = wellFormed(verdict)
  const { role, judgedBy } = armOf(arm)
  if (judgedBy === 'both violations') assert.deepEqual(violations, SEEDED_VIOLATIONS, role)
  else if (judgedBy === 'no violation') assert.deepEqual(violations, [], `${role} failed`)
  else if (judgedBy === 'slope violation')
    assert(violations.includes('retained slope'), 'the legacy contamination was not detected')
}

/**
 * Holds the producer's legacy unseeded arm to its paired ordering: on the same Node and workload,
 * the legacy slope and endpoint both exceed the fixed null's.
 * @param {object} legacy the legacy unseeded component verdict
 * @param {object} fixed the fixed unseeded component verdict
 * @returns {void}
 * @throws {assert.AssertionError} unless both are real verdicts and legacy is above fixed in both
 */
const judgeOrdering = (legacy, fixed) => {
  const old = wellFormed(legacy).measured
  const now = wellFormed(fixed).measured
  assert(
    old.slopeBytesPerMinute > now.slopeBytesPerMinute,
    `legacy slope ${old.slopeBytesPerMinute} is not above fixed ${now.slopeBytesPerMinute}`
  )
  assert(
    old.endpointBytes > now.endpointBytes,
    `legacy endpoint ${old.endpointBytes} is not above fixed ${now.endpointBytes}`
  )
}

/**
 * Runs serial diagnostic children on the resource lane's idle runner, outside fast unit discovery.
 * Every result is saved before assertions, and a failed control never suppresses later arms. A
 * child that crashes, times out or prints malformed JSON is a failure and is never judged as a
 * detection. The `resource` protocol keeps the resource workers' flags and settled reading; the
 * `live` protocol runs the fixed journal under the telemetry worker's exact flags and reading.
 * @param {string} directory CI artifact destination
 * @param {{kinds?:string[], protocols?:string[]}} [options] the workloads and protocols to run
 * @returns {void} after all ownership pairs and real-time controls have passed
 * @throws {AggregateError} with every failed child or assertion, after preserving all available JSON
 */
const run = (directory, { kinds = KINDS, protocols = PROTOCOLS } = {}) => {
  assert(
    kinds.every((kind) => KINDS.includes(kind)),
    'unknown workload kind'
  )
  assert(
    protocols.every((protocol) => PROTOCOLS.includes(protocol)),
    'unknown protocol'
  )
  fs.mkdirSync(directory, { recursive: true })
  const failures = []
  const summary = []
  const verdicts = new Map()

  /** @param {string} name archive stem @param {Function} check assertions @returns {void} */
  const check = (name, check) => {
    try {
      check()
    } catch (error) {
      failures.push({ name, message: error.stack })
    }
  }

  /**
   * @param {string} file child fixture @param {string} mode journal mode @param {string} kind workload
   * @param {boolean} seeded retain products @param {string} [protocol] flags and reading of the arm
   * @returns {object} original child evidence @throws {Error} on any unsuccessful exit
   */
  const child = (file, mode, kind, seeded, protocol = 'resource') => {
    const sensitivity = file === 'journalSensitivityChild'
    const name = `${file}-${kind}-${mode}-${seeded ? 'seeded' : 'unseeded'}${
      protocol === 'live' ? '-live' : ''
    }`
    const result = spawnSync(
      process.execPath,
      [
        ...flagsOf(protocol),
        path.join(__dirname, '__tests__/fixtures', `${file}.js`),
        mode,
        kind,
        String(seeded),
        ...(sensitivity ? [protocol] : []),
      ],
      {
        timeout: sensitivity ? 390000 : 45000,
        encoding: 'utf8',
        maxBuffer: 16 * 1024 * 1024,
      }
    )
    fs.writeFileSync(path.join(directory, `${name}.json`), result.stdout || '{}', { flush: true })
    fs.writeFileSync(path.join(directory, `${name}.stderr`), result.stderr || '', { flush: true })
    if (result.error) throw result.error
    assert.equal(result.status, 0, `${name}: unsuccessful child exit (${result.signal})`)
    return JSON.parse(result.stdout)
  }

  /** @param {string} kind workload @param {boolean} seeded retain products @returns {void} */
  const ownership = (kind, seeded) => {
    const old = child('journalOwnershipChild', 'legacy', kind, seeded)
    const fixed = child('journalOwnershipChild', 'fixed', kind, seeded)
    for (const key of ['corpusSha256', 'scheduleSha256', 'idsSha256', 'factsSha256'])
      assert.equal(fixed[key], old[key], key)
    assert.equal(fixed.storage.mode, 'fixed')
    assert.equal(old.storage.mode, 'legacy')
    assert.equal(old.storage.before, null)
    assert.equal(old.storage.closed, null)
    assert(fixed.storage.before.byteLength > 0)
    assert.equal(fixed.storage.closed.byteLength, fixed.storage.before.byteLength)
    assert.equal(fixed.storage.before.columns[0].columns[0].format, 'scalar-utf8-1')
    if (kind === 'shareConsumer')
      assert.equal(fixed.storage.before.columns[1].columns[0].format, 'share-packed-1')
    for (const result of [old, fixed]) {
      assert.equal(result.diagnostic, true)
      assert.equal(result.acceptanceEligible, false)
      assert.equal(result.attributionComplete, false)
      assert.equal(result.calibration, null)
      assert.deepEqual(
        result.ownership.map((row) => row.checkpoint),
        ['M', 'first', 'last', 'closed']
      )
      assert(result.ownership.every((row) => row.positiveAlive))
      if (seeded) assert.equal(result.ownership.at(-1).recordTargetsAlive, 10000)
      else assert(result.ownership.at(-1).recordTargetsAlive < 10)
    }
    for (const [index, row] of fixed.ownership.entries()) {
      const { journals, observer, ...targets } = row
      const { journals: oldJournals, observer: oldObserver, ...oldTargets } = old.ownership[index]
      assert.deepEqual(targets, oldTargets)
      const { archives, ...weakInventory } = observer
      const { archives: oldArchives, ...oldWeakInventory } = oldObserver
      assert.deepEqual(weakInventory, oldWeakInventory)
      assert(archives.byteLength > 0 && oldArchives.byteLength > 0)
      assert.equal(oldJournals, null)
      assert.equal(journals.byteLength, fixed.storage.before.byteLength)
    }
  }

  /**
   * @param {{kind:string, mode:string, seeded:boolean, protocol:string}} arm the arm to run
   * @returns {void} @throws {Error} for a crashed child, broken integrity or a verdict off its role
   */
  const sensitivity = (arm) => {
    const { kind, mode, seeded, protocol } = arm
    const result = child('journalSensitivityChild', mode, kind, seeded, protocol)
    summary.push({
      kind,
      mode,
      seeded,
      protocol,
      ...armOf(arm),
      measurementMs: result.measurementMs,
      verdict: result.verdict,
    })
    assert.equal(result.diagnostic, true)
    assert.equal(result.acceptanceEligible, false)
    assert.deepEqual(result.runtime.flags, flagsOf(protocol))
    assert.equal(result.runtime.executable, process.execPath)
    assert.equal(result.protocol.name, protocol)
    assert.equal(result.measurementMs, result.boundaryAt - result.phaseOrigin - TELEMETRY.warmupMs)
    assert(result.measurementMs >= TELEMETRY.measureMs)
    assert.deepEqual(result.sampler.skipped, [])
    assert.equal(result.records, 10000)
    assert.equal(result.seededRoots, seeded ? 10000 : 0)
    assert.deepEqual(
      result.samples.map((row) => row.scheduledAtMs),
      TELEMETRY.sampleAt
    )
    const geometry = result.baseline.observer
    for (const row of [result.baseline, ...result.samples, result.closed]) {
      assert.deepEqual(row.observer, geometry)
      assert.equal(row.observer.recordWeakRefs, 0)
      assert.equal(row.observer.weakRefs, 1)
      assert.equal(row.snapshot.journals.positiveAlive, true)
      assert.equal(row.admittedAtMs, row.admittedAt - row.origin)
      assert.equal(row.completedAtMs, row.completedAt - row.origin)
      const memory = row.snapshot.memory
      assert(memory.gc.admittedAt >= row.admittedAt)
      assert(memory.gc.completedAt <= row.completedAt)
      for (const field of ['heapUsed', 'external', 'arrayBuffers', 'retained'])
        assert(Number.isFinite(memory[field]), field)
      assert.equal(memory.retainedBytes, memory.heapUsed + memory.external)
      if (protocol === 'live') assert(Number.isFinite(memory.gc.observed))
      else {
        assert(memory.gc.checkpoints.length >= 2 && memory.gc.checkpoints.length <= 6)
        for (const checkpoint of memory.gc.checkpoints)
          assert(
            Number.isFinite(checkpoint.engine.code) && Number.isFinite(checkpoint.engine.bytecode)
          )
      }
      if (mode === 'fixed')
        assert.equal(row.snapshot.journals.workload.byteLength, result.storage.before.byteLength)
    }
    if (protocol === 'live') {
      assert.equal(result.shadow.scope, 'shadow')
      assert.equal(result.shadow.acceptance, false)
      assert.equal(result.shadow.complete, true)
    }
    for (const owner of mode === 'fixed' ? ['workload', 'observer'] : ['observer']) {
      const state = (row) => ({ journals: row.snapshot.journals[owner] })
      assert.deepEqual(
        JournalConstancy.violations({
          baseline: state(result.baseline),
          samples: result.samples.map((row) => ({ snapshot: state(row) })),
          teardown: state(result.closed),
        }),
        []
      )
    }
    verdicts.set(`${protocol}-${kind}-${mode}-${seeded}`, result.verdict)
    judgeArm(arm, result.verdict)
  }

  for (const protocol of protocols) {
    const modes = protocol === 'live' ? ['fixed'] : ['legacy', 'fixed']
    for (const kind of kinds) {
      if (protocol === 'resource' && kind !== 'admin')
        for (const seeded of [false, true])
          check(`ownership-${kind}-${seeded}`, () => ownership(kind, seeded))
      for (const mode of modes)
        for (const seeded of [false, true])
          check(`sensitivity-${kind}-${mode}-${seeded}${protocol === 'live' ? '-live' : ''}`, () =>
            sensitivity({ kind, mode, seeded, protocol })
          )
      if (protocol === 'resource' && kind === 'producer')
        check(`ordering-${kind}`, () =>
          judgeOrdering(
            verdicts.get(`${protocol}-${kind}-legacy-false`),
            verdicts.get(`${protocol}-${kind}-fixed-false`)
          )
        )
    }
  }
  fs.writeFileSync(
    path.join(directory, 'summary.json'),
    JSON.stringify({ kinds, protocols, summary, failures }, null, 2)
  )
  if (failures.length)
    throw new AggregateError(
      failures.map((row) => new Error(`${row.name}: ${row.message}`)),
      'Journal controls failed'
    )
}

/**
 * @param {string[]} argv `<directory> [--kinds=a,b] [--protocols=resource,live]`
 * @returns {{directory:string, options:{kinds?:string[], protocols?:string[]}}} the parsed command
 * @throws {Error} for an option that is not one of the two
 */
const parseArguments = (argv) => {
  const [directory, ...flags] = argv
  const options = {}
  for (const flag of flags) {
    const found = /^--(kinds|protocols)=(.+)$/.exec(flag)
    if (!found) throw new Error(`Unknown option ${flag}`)
    options[found[1]] = found[2].split(',')
  }
  return { directory, options }
}

if (require.main === module) {
  const { directory, options } = parseArguments(process.argv.slice(2))
  run(directory, options)
}

module.exports = { run, armOf, judgeArm, judgeOrdering, parseArguments, KINDS, PROTOCOLS }
