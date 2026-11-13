'use strict'

const fs = require('node:fs')
const path = require('node:path')
const v8 = require('node:v8')
const { performance } = require('node:perf_hooks')
const crypto = require('node:crypto')
const { CAPACITY } = require('./contract')

/** Selects ownership boundaries after the original scalar read, only in diagnostic children. */
class LiveCensus {
  /**
   * @param {object} options diagnostic child configuration
   * @param {number[]} options.sampleAt unchanged ordered admission grid
   * @param {Function} options.capture synchronous capture or scalar companion sink
   */
  constructor({ sampleAt, capture }) {
    this.sampleAt = sampleAt
    this.capture = capture
    this.names = new Set()
    this.sampleIndex = 0
  }

  /**
   * Observes the existing record seam without additional GC or waiting turns.
   * @param {object} row original Soak record @returns {void} at most four captures
   * @throws {Error} for a duplicate checkpoint
   */
  observe(row) {
    let name
    if (row.kind === 'baseline') name = 'M'
    if (row.kind === 'teardown') name = 'closed'
    if (row.kind === 'sample') {
      // Admission can be slightly late; select by the original grid slot, never byte size.
      const slot = this.sampleAt.findIndex(
        (at, index) =>
          row.admittedPhaseMs >= at &&
          (index === this.sampleAt.length - 1 || row.admittedPhaseMs < this.sampleAt[index + 1])
      )
      if (slot === 0) name = 'first'
      if (slot === this.sampleAt.length - 1) name = 'last'
      this.sampleIndex++
    }
    if (!name) return
    if (this.names.has(name)) throw new Error('duplicate live census checkpoint')
    this.names.add(name)
    const reading = row.snapshot.memory
    this.capture(name, {
      diagnostic: true,
      acceptanceEligible: false,
      attributionComplete: false,
      reading,
      journals: row.snapshot.journals,
      processRetainedBytes: reading.heapUsed + reading.external,
      admittedPhaseMs: row.admittedPhaseMs ?? null,
      completedPhaseMs: row.phaseMs ?? null,
    })
  }

  /** @returns {boolean} all four checkpoints and every grid sample were observed */
  complete() {
    return this.names.size === 4 && this.sampleIndex === this.sampleAt.length
  }
}

/**
 * Writes four authenticated same-process captures or scalar-only companion records.
 * @param {string} directory fresh child output @param {object} identity parent-issued run identity
 * @returns {Function} synchronous checkpoint sink @throws {Error} on capture or disk failure
 */
const captureSink = (directory, identity) => {
  fs.mkdirSync(directory, { recursive: true })
  const events = path.join(directory, 'census-events.jsonl')
  fs.writeFileSync(events, '', { flag: 'wx' })
  fs.writeFileSync(
    path.join(directory, 'census-identity.json'),
    JSON.stringify({
      ...identity,
      pid: process.pid,
      origin: performance.timeOrigin,
      node: process.version,
      v8: process.versions.v8,
      flags: process.execArgv,
    })
  )
  const scratch = Buffer.alloc(65536, 0xa5)
  return (name, facts) => {
    const engineBefore = v8.getHeapCodeStatistics()
    const startedAt = performance.now()
    const file = identity.variant === 'capture' ? `${name}.heapsnapshot` : null
    if (file) v8.writeHeapSnapshot(path.join(directory, file))
    const endedAt = performance.now()
    let bytes = 0
    let sha256 = null
    if (file) {
      const target = path.join(directory, file)
      bytes = fs.statSync(target).size
      if (bytes > CAPACITY.snapshotBytes) throw new Error('live census snapshot overflow')
      const fd = fs.openSync(target, 'r')
      const hash = crypto.createHash('sha256')
      try {
        let count
        while ((count = fs.readSync(fd, scratch, 0, scratch.length, null)) > 0)
          hash.update(scratch.subarray(0, count))
        sha256 = hash.digest('hex')
      } finally {
        fs.closeSync(fd)
        scratch.fill(0xa5)
      }
    }
    fs.appendFileSync(
      events,
      JSON.stringify({
        ...facts,
        name,
        file,
        startedAt,
        endedAt,
        bytes,
        sha256,
        engineBefore,
        engineAfter: v8.getHeapCodeStatistics(),
        recordedAt: performance.now(),
        observerStorageBytes: scratch.length,
      }) + '\n'
    )
  }
}

module.exports = { LiveCensus, captureSink }
