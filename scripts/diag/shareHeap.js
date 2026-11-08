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
  const count = env.SHARE_SOAK_HEAP_EXTRA_CYCLES ?? '0'
  if (!/^(0|[1-9][0-9]*)$/.test(count) || Number(count) > 60)
    throw new Error('Diagnostic extra cycles must be an integer from 0 to 60')
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
      try {
        await bounded(owner.scope, BLOCK_TRANSITION_MS, (scope) => owner.driver.openPhase(scope))
        const blockStartedAt = owner.clock.now()
        for (let extra = first; extra < end; extra++) {
          const cycle = SHARE_SOAK.cycles + extra
          const plan = { ...ShareCycleOracle.plan(cycle), phase: 'share-steady', reset: false }
          const due = blockStartedAt + (extra - first + 1) * owner.slotMs
          await bounded(owner.scope, due - owner.clock.now(), async (scope) => {
            const observation = await owner.driver.cycle(plan, scope, { due, clock: owner.clock })
            const violations = owner.verifyCycle(plan, observation)
            owner.row('diag-extra-cycle', { extra: extra + 1, plan, observation, violations })
            owner.driver.lastCycle = null
            owner.driver.activeCycle = null
            if (violations.length) throw new Error(violations.join('; '))
            if (owner.clock.now() > due)
              throw new Error('Diagnostic repetition exceeded its cycle slot')
          })
          while (owner.clock.now() < due) await owner.pause(owner.scope, due - owner.clock.now())
        }
      } finally {
        await bounded(owner.scope, BLOCK_TRANSITION_MS, () => owner.driver.closePhase())
      }
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
  const { buildShareSoak } = require('../soak/soak')
  const { evidencePath, writeEvidence } = require('../../testHelpers/e2eEvidence')
  const { bounded, NEVER } = require('../../src/producer/__tests__/fixtures/bounded')
  const directory = path.join(path.dirname(evidencePath('diag-share-heap')), 'diag-heap')
  fs.mkdirSync(directory, { recursive: true })
  const manifest = path.join(directory, 'snapshots.jsonl')
  if (fs.existsSync(manifest)) throw new Error('Use a fresh diagnostic evidence directory')
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
      owner.final = await bounded(owner.scope, owner.budget.cleanupMs, (scope) =>
        owner.driver.final(scope, {
          clock: owner.clock,
          due: owner.clock.now() + owner.budget.cleanupMs,
        })
      )
      owner.row('diag-original-final', { final: owner.final })
      await bounded(owner.scope, BLOCK_TRANSITION_MS, () => owner.driver.closePhase())
      await repeatSteady(owner, options.extraCycles, capture, soak)
      await owner.stage('cleanup', () => owner.cleanupStack())
    },
  }
  const soak = owner.soak()
  try {
    owner.gate.assertClean()
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
    const result = {
      diagnostic: true,
      acceptanceEligible: false,
      resources,
      extraCycles: options.extraCycles,
    }
    owner.row('diag-complete', result)
    return result
  } catch (error) {
    owner.fail(error)
    throw error
  } finally {
    await owner.close()
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
