'use strict'

const fs = require('node:fs')
const os = require('node:os')
const v8 = require('node:v8')
const crypto = require('node:crypto')
const { ProcessMemory, PendingSampler } = require('../../../../testHelpers/processMemory')
const { CAPACITY, BUDGET } = require('./contract')

/** Diagnostic sampler preserves every window on disk without changing the acceptance sampler. */
class DiagnosticSampler extends PendingSampler {
  /** @param {object} memory process meter @returns {DiagnosticSampler} empty bounded sampler */
  constructor(memory) {
    super(memory)
    this.completed = []
  }

  /** @returns {object} settlement edge @throws {Error} on diagnostic capacity exhaustion */
  end() {
    const window = this.window
    const reading = super.end()
    if (this.pending === 0) {
      if (this.completed.length >= CAPACITY.windows) throw new Error('diagnostic window overflow')
      this.completed.push({ ...window })
      this.windowRows.length = 0
    }
    return reading
  }

  /** @returns {object} all diagnostic pending windows */
  summary() {
    return { ...super.summary(), pendingWindows: this.completed.map((row) => ({ ...row })) }
  }
}

/** Adds raw source facts and disk captures only in diagnostic processes. */
class DiagnosticMemory extends ProcessMemory {
  /** @param {object} log scalar disk recorder @param {string} directory child artifact directory @param {string} variant capture or scalar @returns {DiagnosticMemory} meter */
  constructor(log, directory, variant) {
    super()
    Object.assign(this, { log, directory, variant })
    this.startedAt = this.now()
    this.processStart = fs
      .readFileSync('/proc/self/stat', 'utf8')
      .replace(/^.*\) /, '')
      .split(' ')[19]
  }

  /** @param {string} phase phase boundary @returns {object} unchanged acceptance-style mark @throws {Error} if raw evidence overflows */
  mark(phase) {
    const current = this.current()
    const before = this.now()
    const status = fs.readFileSync('/proc/self/status', 'utf8')
    const after = this.now()
    if (Buffer.byteLength(status) > CAPACITY.statusBytes)
      throw new Error('diagnostic status overflow')
    const mark = super.mark(phase)
    this.log.write('mark', {
      mark,
      raw: {
        before,
        after,
        current,
        status,
        parsed: Object.fromEntries(
          ['VmHWM', 'VmRSS'].map((name) => {
            const match = new RegExp(`^${name}:\\s+(\\d+) kB$`, 'm').exec(status)
            return [name, match ? Number(match[1]) * 1024 : null]
          })
        ),
        maxRss: process.resourceUsage().maxRSS * 1024,
        pid: process.pid,
        processStart: this.processStart,
        kernel: os.release(),
        os: os.type(),
        arch: process.arch,
        flags: process.execArgv,
      },
    })
    return mark
  }

  /** @returns {void} @throws {Error} when capture/work consumed the ten-second cleanup reserve */
  reserve() {
    if (this.now() - this.startedAt >= BUDGET.child - BUDGET.reserve)
      throw new Error('diagnostic exit reserve exhausted')
  }

  /** @param {string} name fixed checkpoint @param {object} reading pre-capture scalar reading @returns {Promise<void>} hash completion; snapshot bytes never enter the target heap */
  async checkpoint(name, reading) {
    this.reserve()
    const startedAt = this.now()
    let file = null
    let bytes = 0
    let sha256 = null
    if (this.variant === 'capture') {
      file = `${name}.heapsnapshot`
      v8.writeHeapSnapshot(`${this.directory}/${file}`)
      bytes = fs.statSync(`${this.directory}/${file}`).size
      if (bytes > CAPACITY.snapshotBytes) throw new Error('diagnostic snapshot overflow')
      const hash = crypto.createHash('sha256')
      for await (const chunk of fs.createReadStream(`${this.directory}/${file}`)) hash.update(chunk)
      sha256 = hash.digest('hex')
    }
    const endedAt = this.now()
    this.log.write('checkpoint', {
      name,
      observedAt: reading.at,
      startedAt,
      endedAt,
      pre: reading,
      post: this.current(),
      file,
      bytes,
      sha256,
    })
    this.mark(`checkpoint:${name}:after`)
  }
}

module.exports = { DiagnosticMemory, DiagnosticSampler }
