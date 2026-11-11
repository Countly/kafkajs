'use strict'

const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const { MEMORY_FLAGS } = require('../../testHelpers/processMemory')
const { TELEMETRY } = require('./soak')
const { JournalConstancy } = require('../../testHelpers/fixedJournal')

/**
 * Runs serial diagnostic children on the resource lane's idle runner, outside fast unit discovery.
 * Every result is saved before assertions, and a failed control never suppresses later arms.
 * @param {string} directory CI artifact destination
 * @returns {void} after all ownership pairs and real-time controls have passed
 * @throws {AggregateError} with every failed child or assertion, after preserving all available JSON
 */
const run = (directory) => {
  fs.mkdirSync(directory, { recursive: true })
  const failures = []
  const summary = []

  /** @param {string} name archive stem @param {Function} check assertions @returns {void} */
  const check = (name, check) => {
    try {
      check()
    } catch (error) {
      failures.push({ name, message: error.stack })
    }
  }

  /** @param {string} file child fixture @param {string} mode journal mode @param {string} kind workload @param {boolean} seeded retain products @returns {object} original child evidence @throws {Error} on any unsuccessful exit */
  const child = (file, mode, kind, seeded) => {
    const name = `${file}-${kind}-${mode}-${seeded ? 'seeded' : 'unseeded'}`
    const result = spawnSync(
      process.execPath,
      [
        ...MEMORY_FLAGS,
        path.join(__dirname, '__tests__/fixtures', `${file}.js`),
        mode,
        kind,
        String(seeded),
      ],
      {
        timeout: file === 'journalSensitivityChild' ? 390000 : 45000,
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

  for (const kind of ['producer', 'consumer', 'shareConsumer']) {
    for (const seeded of [false, true]) {
      check(`ownership-${kind}-${seeded}`, () => {
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
          assert.equal(fixed.storage.before.columns[1].columns[0].format, 'share-columns-utf8-1')
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
          const {
            journals: oldJournals,
            observer: oldObserver,
            ...oldTargets
          } = old.ownership[index]
          assert.deepEqual(targets, oldTargets)
          const { archives, ...weakInventory } = observer
          const { archives: oldArchives, ...oldWeakInventory } = oldObserver
          assert.deepEqual(weakInventory, oldWeakInventory)
          assert(archives.byteLength > 0 && oldArchives.byteLength > 0)
          assert.equal(oldJournals, null)
          assert.equal(journals.byteLength, fixed.storage.before.byteLength)
        }
      })
    }
    for (const mode of ['legacy', 'fixed']) {
      for (const seeded of [false, true]) {
        check(`sensitivity-${kind}-${mode}-${seeded}`, () => {
          const result = child('journalSensitivityChild', mode, kind, seeded)
          summary.push({
            kind,
            mode,
            seeded,
            measurementMs: result.measurementMs,
            verdict: result.verdict,
          })
          assert.equal(result.diagnostic, true)
          assert.equal(result.acceptanceEligible, false)
          assert.deepEqual(result.runtime.flags, MEMORY_FLAGS)
          assert.equal(result.runtime.executable, process.execPath)
          assert.equal(
            result.measurementMs,
            result.boundaryAt - result.phaseOrigin - TELEMETRY.warmupMs
          )
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
            assert(memory.gc.checkpoints.length >= 2 && memory.gc.checkpoints.length <= 6)
            for (const checkpoint of memory.gc.checkpoints)
              assert(
                Number.isFinite(checkpoint.engine.code) &&
                  Number.isFinite(checkpoint.engine.bytecode)
              )
            if (mode === 'fixed') {
              assert.equal(
                row.snapshot.journals.workload.byteLength,
                result.storage.before.byteLength
              )
            }
          }
          assert.equal(result.verdict.scope, 'slope-and-endpoint-only')
          assert.deepEqual(
            result.verdict.violations,
            seeded ? ['retained slope', 'retained endpoint'] : []
          )
          assert.equal(result.verdict.ok, !seeded)
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
        })
      }
    }
  }
  fs.writeFileSync(
    path.join(directory, 'summary.json'),
    JSON.stringify({ summary, failures }, null, 2)
  )
  if (failures.length)
    throw new AggregateError(
      failures.map((row) => new Error(`${row.name}: ${row.message}`)),
      'Journal controls failed'
    )
}

if (require.main === module) run(process.argv[2])

module.exports = { run }
