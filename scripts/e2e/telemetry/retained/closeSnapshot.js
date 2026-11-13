'use strict'

const FLAGS = { diagnostic: true, acceptanceEligible: false, attributionComplete: false }

/** Captures only the original matched and closed readings, without extra settling calls. */
class CloseSnapshot {
  /**
   * @param {object} options diagnostic dependencies
   * @param {object} options.memory original ProcessMemory
   * @param {Function} options.capture retained capture/scalar sink
   * @param {Function} [options.counters] engine and heap-space counter reader
   */
  constructor({ memory, capture, counters = engineCounters }) {
    Object.assign(this, { memory, capture, counters })
    this.records = []
  }

  /**
   * @param {'M'|'closed'} name original seam @param {object} reading original settled reading
   * @returns {void} captures once; snapshots collect and allocate
   * @throws {Error} on wrong order or failed capture; never retries
   */
  record(name, reading) {
    if (name !== ['M', 'closed'][this.records.length]) throw new Error('close snapshot order')
    const facts = {
      ...FLAGS,
      name,
      reading,
      processRetainedBytes: reading.retained,
      countersBefore: this.counters(),
    }
    this.capture(name, facts)
    this.records.push({
      ...facts,
      afterSnapshot: this.memory.current(),
      countersAfter: this.counters(),
    })
  }

  /** @returns {object} bounded records; never acceptance evidence */
  summary() {
    return { ...FLAGS, complete: this.records.length === 2, records: this.records }
  }
}

/** @returns {object} contemporaneous engine counters; not a replacement memory reading */
const engineCounters = () => {
  const v8 = require('node:v8')
  return {
    code: v8.getHeapCodeStatistics(),
    heap: v8.getHeapStatistics(),
    spaces: v8.getHeapSpaceStatistics(),
  }
}

/**
 * @param {object} [options] diagnostic environment and dependencies
 * @returns {CloseSnapshot|null} no diagnostic imports or callbacks without the switch
 * @throws {Error} on mixed instrumentation or invalid capture configuration
 */
const createCloseSnapshot = ({ env = process.env, memory, capture } = {}) => {
  if (!env.RESOURCE_CLOSE_SNAPSHOT_DIRECTORY) return null
  if (
    env.ACTIVE_CENSUS_DIRECTORY ||
    env.RESOURCE_WITNESSES ||
    env.RESOURCE_ACTIVE_SNAPSHOT_DIRECTORY
  )
    throw new Error('close snapshot needs isolated instrumentation')
  if (!['scalar', 'capture'].includes(env.RESOURCE_CLOSE_SNAPSHOT_VARIANT))
    throw new Error('close snapshot needs a scalar or capture variant')
  const sink =
    capture ??
    require('./liveCensus').captureSink(env.RESOURCE_CLOSE_SNAPSHOT_DIRECTORY, {
      ...FLAGS,
      childId: env.RESOURCE_CHILD_ID,
      tree: env.RESOURCE_TREE,
      runId: env.RESOURCE_RUN_ID,
      kind: process.argv[2],
      variant: env.RESOURCE_CLOSE_SNAPSHOT_VARIANT,
      seed: env.RESOURCE_SEED || null,
    })
  return new CloseSnapshot({ memory, capture: sink })
}

module.exports = { FLAGS, CloseSnapshot, createCloseSnapshot }
