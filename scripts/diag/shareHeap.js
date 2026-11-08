#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const path = require('node:path')
const v8 = require('node:v8')

const SNAPSHOT_MS = 120000
const BLOCK_CYCLES = 10
const BLOCK_TRANSITION_MS = 15000

/**
 * Reads opt-in settings. The ordinary soak entry points never load this module.
 * @param {object} env Environment variables.
 * @returns {object|null} Fixed diagnostic envelope, or null when disabled.
 */
const diagnosticOptions = (env) => {
  if (env.SHARE_SOAK_HEAP_DIAG !== '1') return null
  const count = env.SHARE_SOAK_HEAP_EXTRA_CYCLES ?? '10'
  if (count !== '10') throw new Error('Diagnostic extra cycles must be exactly 10')
  const extraCycles = Number(count)
  const blocks = Math.ceil(extraCycles / BLOCK_CYCLES)
  return {
    extraCycles,
    blockCycles: BLOCK_CYCLES,
    snapshotMs: SNAPSHOT_MS,
    // Separate controller-owned envelope: original short bound, capture pauses and extra work.
    totalMs:
      1020000 + (4 + blocks) * SNAPSHOT_MS + extraCycles * 32000 + blocks * BLOCK_TRANSITION_MS,
  }
}

/**
 * Captures only after timed quiet observation has returned, with no workload slot active.
 * @param {object} options Snapshot location, owner clock and injectable broker-free capture ports.
 * @returns {object} File facts and original monotonic times, never acceptance evidence.
 */
const captureHeap = ({
  directory,
  label,
  owner,
  gc = globalThis.gc,
  memory = process.memoryUsage,
  write = v8.writeHeapSnapshot,
  stat = fs.statSync,
}) => {
  if (typeof gc !== 'function') throw new Error('Heap diagnostics require --expose-gc')
  const startedAt = owner.clock.now()
  gc()
  gc()
  const heapUsed = memory().heapUsed
  const ownerAtMs = owner.clock.now() - owner.startedAt
  const file = write(path.join(directory, `heap-${label}.heapsnapshot`))
  const completedAt = owner.clock.now()
  if (completedAt - startedAt > SNAPSHOT_MS)
    throw new Error('Diagnostic snapshot deadline exceeded')
  return {
    diagnostic: true,
    acceptanceEligible: false,
    label,
    file: path.basename(file),
    sizeBytes: stat(file).size,
    heapUsed,
    clock: 'owner-performance-ms',
    ownerAtMs,
    completedOwnerAtMs: completedAt - owner.startedAt,
    snapshotMs: completedAt - startedAt,
  }
}

/**
 * Runs extra steady-shaped cycles with the same corpus size, original per-cycle oracles and
 * fixed 32-second slots. Cumulative offsets continue; each block closes admin and producer.
 * @param {object} owner Original ShareSoak owner.
 * @param {number} count Fixed extra cycle count.
 * @param {Function} capture Quiet snapshot callback.
 * @param {object} soak Original Soak's quiet settling and resource probe.
 * @returns {Promise<void>}
 */
const repeatSteady = async (owner, count, capture, soak) => {
  const { ShareCycleOracle, SHARE_SOAK, Soak } = require('../soak/soak')
  const { bounded } = require('../../src/producer/__tests__/fixtures/bounded')
  for (let first = 0; first < count; first += BLOCK_CYCLES) {
    const end = Math.min(count, first + BLOCK_CYCLES)
    const runBlock = async () => {
      let primary
      try {
        await bounded(owner.scope, BLOCK_TRANSITION_MS, (scope) => owner.driver.openPhase(scope))
        const blockStartedAt = owner.clock.now()
        for (let extra = first; extra < end; extra++) {
          const cycle = SHARE_SOAK.cycles + extra
          const plan = { ...ShareCycleOracle.plan(cycle), phase: 'share-steady', reset: false }
          const startedAt = owner.clock.now()
          const due = blockStartedAt + (extra - first + 1) * owner.slotMs
          await bounded(owner.scope, due - owner.clock.now(), async (scope) => {
            const observation = await owner.driver.cycle(plan, scope, { due, clock: owner.clock })
            const violations = owner.verifyCycle(plan, observation)
            owner.row('diag-extra-cycle', {
              extra: extra + 1,
              plan: { ...plan, first: String(plan.first) },
              observation,
              violations,
              slot: {
                windowStart: due - owner.slotMs,
                startedAt,
                due,
                completedAt: owner.clock.now(),
              },
            })
            owner.driver.lastCycle = null
            owner.driver.activeCycle = null
            if (violations.length) throw new Error(violations.join('; '))
            if (owner.clock.now() > due)
              throw new Error('Diagnostic repetition exceeded its cycle slot')
          })
          while (owner.clock.now() < due) await owner.pause(owner.scope, due - owner.clock.now())
        }
      } catch (error) {
        primary = error
      }
      try {
        await bounded(owner.scope, BLOCK_TRANSITION_MS, () => owner.driver.closePhase())
      } catch (cleanupError) {
        if (!primary) primary = cleanupError
        else {
          primary.cleanupErrors = [...(primary.cleanupErrors ?? []), cleanupError]
          recordFailure(owner, 'diag-cleanup-failed', cleanupError)
        }
      }
      if (primary) throw primary
    }
    const repetition = new Soak({
      probe: soak.probe,
      sampleMs: soak.sampleMs,
      settleMs: soak.settleMs,
      sampleSnapshot: soak.sampleSnapshot,
      record: (row) => owner.row('diag-repetition-resources', { ...row, extraCycles: end }),
      workloads: { diagnosticSteady: runBlock },
      transition: (work) => bounded(owner.scope, BLOCK_TRANSITION_MS, work),
    })
    // Keep the same compact history and full disk archive as the original steady phase.
    repetition.samples = soak.samples
    await bounded(owner.scope, (end - first) * owner.slotMs + BLOCK_TRANSITION_MS, () =>
      repetition.runPhase(
        {
          name: `diag-repeat-${end}`,
          workload: 'diagnosticSteady',
          ms: (end - first) * owner.slotMs,
        },
        Date.now()
      )
    )
    capture(`repeat-${String(end).padStart(3, '0')}`)
  }
}

/**
 * Archives nested cleanup causes separately without replacing the error if the sink also fails.
 * @param {object} owner Original diagnostic owner and taint gate.
 * @param {string} event Failure event.
 * @param {Error} error Original exception.
 * @returns {void}
 */
const recordFailure = (owner, event, error) => {
  const describe = (failure, seen = new Set()) => {
    if (seen.has(failure)) return { message: 'Circular error reference' }
    seen.add(failure)
    return {
      name: failure?.name,
      message: String(failure?.message ?? failure),
      stack: failure?.stack,
      errors: failure?.errors?.map((child) => describe(child, seen)),
      cause: failure?.cause ? describe(failure.cause, seen) : undefined,
      cleanupErrors: failure?.cleanupErrors?.map((child) => describe(child, seen)),
    }
  }
  owner.fail(error)
  try {
    owner.row(event, { error: describe(error), status: 'failed', tainted: true })
  } catch (archiveError) {
    owner.fail(archiveError)
    error.archiveErrors = [...(error.archiveErrors ?? []), archiveError]
  }
}

/**
 * Opt-in standalone diagnostic runner. Reuses the real short workload and resource screen,
 * but never invokes the acceptance completion writer or natural-exit receipt generator.
 * Captures run after the timed transitions; only this process's enclosing bound is larger.
 * @param {object} [options] Environment override for broker-free disabled-mode controls.
 * @returns {Promise<object|null>} Ineligible diagnostic result, or null when disabled.
 */
const runDiagnostic = async ({ env = process.env } = {}) => {
  const options = diagnosticOptions(env)
  if (!options) return null
  if (typeof globalThis.gc !== 'function') throw new Error('Heap diagnostics require --expose-gc')
  const stepStartedAt = Number(env.SHARE_SOAK_HEAP_STEP_STARTED_AT)
  if (
    !Number.isFinite(stepStartedAt) ||
    stepStartedAt > Date.now() ||
    Date.now() - stepStartedAt + options.totalMs > 40 * 60000
  )
    throw new Error('Diagnostic setup, work and exit cannot fit the 40-minute step')
  const { buildShareSoak } = require('../soak/soak')
  const { evidenceStatus } = require('./evidence')
  const { evidencePath, writeEvidence } = require('../../testHelpers/e2eEvidence')
  const { bounded, NEVER } = require('../../src/producer/__tests__/fixtures/bounded')
  const directory = path.join(path.dirname(evidencePath('diag-share-heap')), 'diag-heap')
  fs.mkdirSync(directory, { recursive: true })
  const manifest = path.join(directory, 'snapshots.jsonl')
  if (fs.existsSync(manifest) || fs.existsSync(evidencePath('diag-share-heap')))
    throw new Error('Use a fresh diagnostic evidence directory')
  const owner = buildShareSoak({
    brokers: ['localhost:9092', 'localhost:9095', 'localhost:9098'],
    short: true,
  })
  owner.stem = 'diag-share-heap'
  owner.evidence = {
    write: (row) =>
      writeEvidence('diag-share-heap', {
        ...row,
        stem: 'diag-share-heap',
        selector: 'DIAG_SHARE_HEAP',
        diagnostic: true,
        acceptanceEligible: false,
      }),
  }
  owner.startedAt = owner.clock.now()
  owner.workDue = owner.startedAt + options.totalMs - owner.budget.exitMs
  const capture = (label) => {
    const facts = captureHeap({ directory, label, owner })
    fs.appendFileSync(manifest, `${JSON.stringify(facts)}\n`)
    owner.row('diag-heap-snapshot', facts)
  }
  let pending = null
  owner.soakOptions = {
    record: (row) => {
      owner.recordResources(row)
      if (['baseline', 'phase-end'].includes(row.kind)) pending = row.phase ?? 'baseline'
    },
    transition: async (work, stage = 'transition') => {
      const result = await owner.stage(stage, work)
      // No owner stage or scored slot is active while V8 stops the process.
      if (pending !== null) {
        const label = pending
        pending = null
        capture(label)
      }
      return result
    },
    beforeTeardown: async () => {
      // Preserve the original final proof at the original corpus end before extending it.
      const finalStartedAt = owner.clock.now()
      const remaining = owner.budget.cleanupMs - owner.spent.cleanup
      try {
        owner.final = await bounded(owner.scope, remaining, (scope) =>
          owner.driver.final(scope, { clock: owner.clock, due: finalStartedAt + remaining })
        )
      } finally {
        owner.spent.cleanup += owner.clock.now() - finalStartedAt
      }
      owner.row('diag-original-timing', { timing: owner.timing })
      owner.row('diag-original-final', { final: owner.final })
      await bounded(owner.scope, BLOCK_TRANSITION_MS, () => owner.driver.closePhase())
      await repeatSteady(owner, options.extraCycles, capture, soak)
      await owner.stage('cleanup', () => owner.cleanupStack())
      owner.row('diag-cleanup', {
        status: 'passed',
        completedAt: owner.driver.completedAt,
        spentMs: owner.spent.cleanup,
        budgetMs: owner.budget.cleanupMs,
        baseline: owner.driver.baseline,
        after: owner.driver.after,
        clock: owner.driver.cleanupClock,
      })
    },
  }
  const soak = owner.soak()
  let primary
  let result
  try {
    owner.gate.assertClean()
    const digest = (file) =>
      require('node:crypto').createHash('sha256').update(fs.readFileSync(file)).digest('hex')
    owner.row('diag-source', {
      harness: 'standalone-short-share',
      sources: [__filename, require.resolve('./evidence'), require.resolve('../soak/soak')].map(
        (file) => ({ file: path.relative(process.cwd(), file), sha256: digest(file) })
      ),
    })
    owner.row('diag-envelope', { ...options, acceptanceBudgets: owner.budget })
    const resources = await bounded(
      NEVER,
      options.totalMs - owner.budget.exitMs,
      async (scope) => {
        owner.scope = scope
        await owner.prepare()
        return soak.run()
      },
      { label: 'Diagnostic share heap envelope' }
    )
    result = {
      diagnostic: true,
      acceptanceEligible: false,
      resources,
      extraCycles: options.extraCycles,
    }
  } catch (error) {
    primary = error
    recordFailure(owner, 'diag-failed', error)
  }
  try {
    await owner.close()
  } catch (cleanupError) {
    recordFailure(owner, 'diag-cleanup-failed', cleanupError)
    if (primary) primary.cleanupErrors = [...(primary.cleanupErrors ?? []), cleanupError]
    else primary = cleanupError
  }
  if (primary) throw primary
  try {
    const read = (file) => fs.readFileSync(file, 'utf8').trim().split('\n').map(JSON.parse)
    const status = evidenceStatus(read(evidencePath('diag-share-heap')), read(manifest), false)
    if (!status.complete)
      throw new Error(`Incomplete diagnostic evidence: ${status.violations.join('; ')}`)
    owner.row('diag-complete', result)
    return result
  } catch (error) {
    recordFailure(owner, 'diag-failed', error)
    throw error
  }
}

if (require.main === module) {
  runDiagnostic().then(
    (result) => {
      console.log(JSON.stringify(result ?? { diagnostic: false, skipped: true }))
    },
    (error) => {
      console.error(error)
      process.exitCode = 1
    }
  )
}

module.exports = { captureHeap, diagnosticOptions, runDiagnostic }
