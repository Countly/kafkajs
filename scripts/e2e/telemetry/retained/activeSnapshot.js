'use strict'

const THRESHOLD = 4 * 1024 * 1024
const FLAGS = { diagnostic: true, acceptanceEligible: false, attributionComplete: false }

/** Captures original M and the first recorded active scalar overage, without requesting GC. */
class ActiveSnapshot {
  /**
   * @param {object} options diagnostic-only dependencies
   * @param {object} options.memory original ProcessMemory
   * @param {Function} options.capture synchronous retained snapshot sink
   */
  constructor({ memory, capture }) {
    Object.assign(this, { memory, capture })
    this.matched = null
    this.attempted = false
    this.records = []
  }

  /**
   * @param {object} reading original recorded matched reading
   * @returns {void} writes exactly one reference snapshot
   * @throws {Error} on duplicate M or capture failure
   */
  baseline(reading) {
    if (this.matched) throw new Error('duplicate active snapshot M')
    this.matched = reading
    this.record('M', reading, { phase: 'matched', round: null, delta: 0 })
  }

  /**
   * @param {object} reading already recorded active reading/checkpoint
   * @param {object} seam phase and original GC round (null for completed reading)
   * @returns {void} at most one trigger, strictly above four MiB relative to original M
   * @throws {Error} on capture failure; never retries or adds a collection
   */
  active(reading, seam) {
    if (!this.matched || this.attempted) return
    const delta = reading.retained - this.matched.retained
    if (delta <= THRESHOLD) return
    this.attempted = true
    this.record('trigger', reading, { ...seam, delta })
  }

  /**
   * @param {string} name fixed snapshot name @param {object} reading original scalar
   * @param {object} seam original phase, round and delta
   * @returns {void} stores pre/post snapshot counters; snapshots themselves collect and allocate
   * @throws {Error} on snapshot or scalar read failure
   */
  record(name, reading, seam) {
    const facts = { ...FLAGS, name, reading, ...seam, processRetainedBytes: reading.retained }
    this.capture(name, facts)
    this.records.push({ ...facts, afterSnapshot: this.memory.current() })
  }

  /** @returns {object} bounded diagnostic facts, including an explicit no-trigger outcome */
  summary() {
    return {
      ...FLAGS,
      threshold: THRESHOLD,
      triggered: this.attempted,
      complete: this.records.length === (this.attempted ? 2 : 1),
      records: this.records,
    }
  }
}

/**
 * @param {object} options optional environment and diagnostic dependencies
 * @returns {ActiveSnapshot|null} no imports, storage or callbacks when the switch is absent
 * @throws {Error} on invalid diagnostic environment or output creation failure
 */
const createActiveSnapshot = ({ env = process.env, memory, capture } = {}) => {
  if (!env.RESOURCE_ACTIVE_SNAPSHOT_DIRECTORY) return null
  if (env.ACTIVE_CENSUS_DIRECTORY || env.RESOURCE_WITNESSES || env.RESOURCE_SEED)
    throw new Error('active snapshot requires the unseeded scalar path')
  const sink =
    capture ??
    require('./liveCensus').captureSink(env.RESOURCE_ACTIVE_SNAPSHOT_DIRECTORY, {
      ...FLAGS,
      childId: env.RESOURCE_CHILD_ID,
      tree: env.RESOURCE_TREE,
      runId: env.RESOURCE_RUN_ID,
      kind: process.argv[2],
      variant: 'capture',
    })
  return new ActiveSnapshot({ memory, capture: sink })
}

module.exports = { ActiveSnapshot, createActiveSnapshot, THRESHOLD }
