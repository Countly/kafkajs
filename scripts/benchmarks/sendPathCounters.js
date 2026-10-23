/**
 * A module this tree may not have.
 * @param {string} path
 * @param {(path: string) => object} [load]
 * @returns {object|null} the module, or null when it is the module itself that is missing
 * @throws {Error} any other failure to load it, a missing module it requires included
 */
const optional = (path, load = require) => {
  try {
    return load(path)
  } catch (error) {
    if (error.code === 'MODULE_NOT_FOUND' && error.message.includes(`'${path}'`)) {
      return null
    }
    throw error
  }
}

/**
 * The positive control of the send-path benchmarks: counts, in the tree it is required from, the
 * calls that show each send went through the path being measured. `appendCalls` counts
 * `Accumulator#append`, in every tree that has the accumulator; `acquireCalls` counts
 * `Admission#acquire` and `deadlines` the `Deadline`s an append starts, in a tree that has them,
 * and are `null` in one that does not. `flushTracked` counts `Accumulator#track`, the sends the
 * flush inventory counts, in a tree that has it, and is `null` in one that does not. Each equals
 * the number of sends. Require it before the client, so the accumulator takes the counted
 * `Deadline`; the wrappers add one function call per send, the same in every tree that has the
 * code they wrap.
 * @returns {{ read: () => { appendCalls: number, acquireCalls: number|null, deadlines: number|null,
 *   flushTracked: number|null }, reset: () => void }} `read` gives the counts so far, `reset` starts
 *   them again from 0
 */
const sendPathCounters = () => {
  const counts = { appendCalls: 0, acquireCalls: null, deadlines: null, flushTracked: null }
  let appending = 0

  const deadline = optional('../../src/producer/deadline')
  if (deadline !== null) {
    counts.deadlines = 0
    /** Counts deadlines constructed during append while preserving Deadline behavior. */
    const Counted = class extends deadline.Deadline {
      /** @param {...*} args the arguments forwarded unchanged to Deadline */
      constructor(...args) {
        super(...args)
        if (appending > 0) counts.deadlines += 1
      }
    }
    deadline.Deadline = Counted
  }

  const admission = optional('../../src/producer/state/admission')
  if (admission !== null) {
    counts.acquireCalls = 0
    const { acquire } = admission.Admission.prototype
    /**
     * Counts admission attempts and forwards their synchronous result or waiter promise.
     * @param {...*} args the unchanged Admission.acquire arguments
     * @returns {*} the reservation or promise returned by Admission.acquire
     * @throws {Error} any synchronous admission refusal, unchanged
     */
    admission.Admission.prototype.acquire = function (...args) {
      counts.acquireCalls += 1
      return acquire.apply(this, args)
    }
  }

  const Accumulator = require('../../src/producer/state/accumulator')
  if (typeof Accumulator.prototype.track === 'function') {
    counts.flushTracked = 0
    const { track } = Accumulator.prototype
    /**
     * Counts the sends the flush inventory tracks.
     * @param {...*} args the unchanged Accumulator.track arguments
     * @returns {void}
     */
    Accumulator.prototype.track = function (...args) {
      counts.flushTracked += 1
      return track.apply(this, args)
    }
  }
  const { append } = Accumulator.prototype
  /**
   * Counts appends and marks their synchronous deadline-construction interval.
   * @param {...*} args the unchanged Accumulator.append arguments
   * @returns {Promise<object[]>} the original append operation
   * @throws {Error} any synchronous failure of the wrapped implementation, unchanged
   */
  Accumulator.prototype.append = function (...args) {
    counts.appendCalls += 1
    appending += 1
    try {
      return append.apply(this, args)
    } finally {
      appending -= 1
    }
  }
  return {
    /** @returns {{ appendCalls: number, acquireCalls: number|null, deadlines: number|null, flushTracked: number|null }} a counter snapshot */
    read: () => ({ ...counts }),
    /** Resets supported counters to zero, preserving null for absent modules. @returns {void} */
    reset: () => {
      counts.appendCalls = 0
      if (counts.acquireCalls !== null) counts.acquireCalls = 0
      if (counts.deadlines !== null) counts.deadlines = 0
      if (counts.flushTracked !== null) counts.flushTracked = 0
    },
  }
}

/**
 * The heap the process uses after a full collection, for the memory ceiling of the send-path
 * benchmarks (retained heap within +1 MiB of the previous tree's).
 * @returns {number|null} bytes; null when the process was not started with `--expose-gc`
 */
const heapAfterGc = () => {
  if (typeof global.gc !== 'function') {
    return null
  }
  global.gc()
  return process.memoryUsage().heapUsed
}

sendPathCounters.optional = optional
sendPathCounters.heapAfterGc = heapAfterGc

module.exports = sendPathCounters
