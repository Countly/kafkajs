'use strict'

const crypto = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')
const { Session } = require('node:inspector')

const PROFILE_LIMITS = Object.freeze({
  samplingInterval: 65536,
  stages: 17,
  profileBytes: 4 * 1024 * 1024,
  summaryBytes: 1024 * 1024,
  top: 25,
})

/**
 * Ranks allocation nodes by their own estimated bytes, never their inclusive subtree sizes.
 * The full tree and samples remain in the companion heapprofile; only this index selects a top N.
 * @param {object} profile V8 SamplingHeapProfile
 * @param {number} [limit] maximum ranked stacks
 * @returns {object} total self bytes, node counts and stacks with one-based source coordinates
 */
const topStacks = (profile, limit = PROFILE_LIMITS.top) => {
  const pending = [{ node: profile.head, stack: [] }]
  const ranked = []
  let totalSelfBytes = 0
  let allocationNodes = 0
  while (pending.length) {
    const { node, stack } = pending.pop()
    const frame = node.callFrame
    const frames = [
      ...stack,
      {
        functionName: frame.functionName,
        url: frame.url,
        line: frame.lineNumber + 1,
        column: frame.columnNumber + 1,
      },
    ]
    if (node.selfSize > 0) {
      totalSelfBytes += node.selfSize
      allocationNodes++
      ranked.push({ nodeId: node.id, selfBytes: node.selfSize, stack: frames })
      ranked.sort((a, b) => b.selfBytes - a.selfBytes || a.nodeId - b.nodeId)
      if (ranked.length > limit) ranked.pop()
    }
    for (const child of node.children ?? []) pending.push({ node: child, stack: frames })
  }
  return {
    totalSelfBytes,
    allocationNodes,
    omittedNodes: allocationNodes - ranked.length,
    top: ranked,
  }
}

/** Diagnostic-only allocation sampling of individual reopen and cleanup intervals. */
class ShareAllocationProfile {
  /**
   * @param {object} options artifact destination and optional protocol seam
   * @param {string} options.directory directory inside the existing diagnostic artifact root
   * @param {Function} [options.session] creates an inspector Session
   * @param {Function} [options.now] monotonic milliseconds
   * @throws {Error} for a missing absolute output directory
   */
  constructor({ directory, session = () => new Session(), now = () => performance.now() }) {
    if (!directory || !path.isAbsolute(directory))
      throw new Error('Allocation profiles require an absolute diagnostic directory')
    Object.assign(this, { directory, session, now })
    this.stages = []
    this.errors = []
    this.active = false
  }

  /**
   * @param {object} session connected inspector session
   * @param {string} method profiler command
   * @param {object} [params] protocol parameters
   * @returns {Promise<object>} protocol reply
   * @throws {Error} when the inspector rejects the command
   */
  post(session, method, params = {}) {
    return new Promise((resolve, reject) => {
      session.post(method, params, (error, result) => (error ? reject(error) : resolve(result)))
    })
  }

  /**
   * Starts before work, stops after it settles, and exports outside the sampled interval. Collected
   * objects are included to attribute transient allocations. No GC is requested. A failed capture
   * fails this diagnostic, never an acceptance arm; workload failures retain their original object.
   * @param {'reopen'|'cleanup'} stage operation being bracketed
   * @param {number|null} tick original parent tick, or null for cleanup
   * @param {Function} work original operation, including the previous segment's background seal
   * @returns {Promise<*>} original result
   * @throws {Error|AggregateError} workload, protocol, overlap or artifact-bound failures
   */
  async capture(stage, tick, work) {
    if (!['reopen', 'cleanup'].includes(stage) || (tick !== null && !Number.isSafeInteger(tick)))
      throw new Error('Invalid allocation profile stage')
    if (this.active || this.stages.length >= PROFILE_LIMITS.stages) {
      const error = new Error('Allocation profile overlap or stage bound')
      this.errors.push(error.message)
      throw error
    }
    this.active = true
    const entry = {
      stage,
      tick,
      index: this.stages.length,
      startedAt: this.now(),
      workCompleted: false,
    }
    this.stages.push(entry)
    const errors = []
    let session
    let connected = false
    let started = false
    let profile
    let result
    try {
      session = this.session()
      session.connect()
      connected = true
      await this.post(session, 'HeapProfiler.startSampling', {
        samplingInterval: PROFILE_LIMITS.samplingInterval,
        includeObjectsCollectedByMajorGC: true,
        includeObjectsCollectedByMinorGC: true,
      })
      started = true
      entry.samplingAt = this.now()
      result = await work()
      entry.workCompleted = true
    } catch (error) {
      errors.push(error)
    } finally {
      entry.workEndedAt = this.now()
      if (started)
        try {
          profile = (await this.post(session, 'HeapProfiler.stopSampling')).profile
        } catch (error) {
          errors.push(error)
        }
      entry.stoppedAt = this.now()
      if (connected)
        try {
          session.disconnect()
        } catch (error) {
          errors.push(error)
        }
      this.active = false
    }
    if (started && (!profile?.head || !Array.isArray(profile.samples))) {
      errors.push(new Error('Missing allocation sampling profile'))
      profile = undefined
    }
    try {
      this.export(entry, profile, errors)
    } catch (error) {
      errors.push(error)
    }
    if (errors.length) {
      this.errors.push(...errors.map((error) => String(error.message)))
      if (errors.length === 1) throw errors[0]
      throw new AggregateError(errors, 'Share allocation capture failed')
    }
    return result
  }

  /**
   * Writes a complete V8 profile and a ranked summary, or fails before writing oversized data.
   * Bounds apply per file and per stage; nothing is truncated, and original harness evidence is
   * untouched. Raw profiles carry all stacks and samples, not just the ranked top N.
   * @param {object} entry stage identity and clocks
   * @param {object|undefined} profile protocol result, absent after a start/stop failure
   * @param {Error[]} errors original failures
   * @returns {void}
   * @throws {Error} for a bound violation, duplicate file or failed write
   */
  export(entry, profile, errors) {
    fs.mkdirSync(this.directory, { recursive: true })
    const stem = `${entry.index}-${entry.stage}-${entry.tick ?? 'none'}`
    const summary = {
      version: 1,
      diagnostic: true,
      acceptanceEligible: false,
      ...entry,
      node: process.version,
      v8: process.versions.v8,
      samplingInterval: PROFILE_LIMITS.samplingInterval,
      includeObjectsCollectedByMajorGC: true,
      includeObjectsCollectedByMinorGC: true,
      limits: PROFILE_LIMITS,
      errors: errors.map((error) => String(error.message)),
      interpretation:
        'Self bytes are V8 sampling estimates including collected objects, not retained sizes or exact RSS/external attribution. Sampling, profile extraction and export add diagnostic overhead. No bytes are subtracted from a verdict. Full stacks and samples are in the heapprofile; top stacks are ranked by node selfSize without inclusive double counting.',
    }
    if (profile) {
      const json = JSON.stringify(profile)
      const bytes = Buffer.byteLength(json)
      if (bytes > PROFILE_LIMITS.profileBytes) throw new Error('Allocation profile byte bound')
      summary.profile = {
        name: `${stem}.heapprofile`,
        bytes,
        sha256: crypto.createHash('sha256').update(json).digest('hex'),
      }
      Object.assign(summary, topStacks(profile))
      const text = JSON.stringify(summary)
      if (Buffer.byteLength(text) > PROFILE_LIMITS.summaryBytes)
        throw new Error('Allocation summary byte bound')
      fs.writeFileSync(path.join(this.directory, summary.profile.name), json, {
        flag: 'wx',
        mode: 0o600,
      })
      entry.profile = summary.profile
    }
    const text = JSON.stringify(summary)
    if (Buffer.byteLength(text) > PROFILE_LIMITS.summaryBytes)
      throw new Error('Allocation summary byte bound')
    fs.writeFileSync(path.join(this.directory, `${stem}.json`), text, { flag: 'wx', mode: 0o600 })
    entry.summary = `${stem}.json`
  }

  /** @returns {object} bounded capture inventory; completeness never changes the resource verdict */
  report() {
    return {
      version: 1,
      diagnostic: true,
      acceptanceEligible: false,
      complete:
        !this.active &&
        this.errors.length === 0 &&
        this.stages.some((entry) => entry.stage === 'reopen') &&
        this.stages.filter((entry) => entry.stage === 'cleanup').length === 1 &&
        this.stages.every((entry) => entry.workCompleted && entry.profile && entry.summary),
      limits: PROFILE_LIMITS,
      stages: this.stages.map((entry) => ({ ...entry })),
      errors: [...this.errors],
    }
  }
}

module.exports = { ShareAllocationProfile, topStacks, PROFILE_LIMITS }
