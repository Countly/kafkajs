'use strict'

const fs = require('node:fs')
const os = require('node:os')
const v8 = require('node:v8')
const crypto = require('node:crypto')
const { ProcessMemory, PendingSampler } = require('../../../../testHelpers/processMemory')
const { CAPACITY, BUDGET } = require('./contract')
const { STAGES, FIELDS } = require('./settlement')

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
  /** @param {object} log scalar disk recorder @param {string} directory child artifact directory @param {string} variant capture or scalar @param {object} [sources] scripted meter sources @returns {DiagnosticMemory} meter */
  constructor(log, directory, variant, sources) {
    super(sources)
    Object.assign(this, { log, directory, variant })
    this.operationPhase = 'startup'
    this.settlementId = 0
    this.settlementStorage = new Float64Array(CAPACITY.settlementPoints * FIELDS.length)
    this.nativeState = {
      submittedId: 0,
      submittedJobId: 0,
      submittedCodec: 0,
      settledId: 0,
      settledJobId: 0,
      settledCodec: 0,
    }
    this.startedAt = this.now()
    this.processStart = fs
      .readFileSync('/proc/self/stat', 'utf8')
      .replace(/^.*\) /, '')
      .split(' ')[19]
  }

  /** @param {string} phase phase boundary @param {object|null} [native] scalar native edge identity @returns {object} unchanged acceptance-style mark @throws {Error} if raw evidence overflows */
  mark(phase, native = null) {
    if (native) {
      const prefix = native.event === 'submitted' ? 'submitted' : 'settled'
      this.nativeState[`${prefix}Id`] = native.id
      this.nativeState[`${prefix}JobId`] = native.jobId
      this.nativeState[`${prefix}Codec`] = native.codec === 'gzip' ? 1 : 4
    }
    const current = this.current()
    const before = this.now()
    const status = fs.readFileSync('/proc/self/status', 'utf8')
    const after = this.now()
    if (Buffer.byteLength(status) > CAPACITY.statusBytes)
      throw new Error('diagnostic status overflow')
    const mark = super.mark(phase)
    this.log.write('mark', {
      mark,
      operationPhase: this.operationPhase,
      native,
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

  /**
   * Records into reserved numeric storage before allocating transport rows. No targets, handles,
   * GC calls or extra turns are retained or created by these synchronous observations.
   * @param {number} round zero-based GC pair @param {string} stage fixed turn boundary
   * @returns {void} @throws {Error} on an out-of-catalogue observation
   */
  observeSettlement(round, stage) {
    const index = round * STAGES.length + STAGES.indexOf(stage)
    if (round < 0 || index >= CAPACITY.settlementPoints || !STAGES.includes(stage))
      throw new Error('diagnostic settlement overflow')
    const usage = this.usage()
    const at = this.now()
    this.covered = Math.max(this.covered, usage.rss)
    this.coveredExternal = Math.max(this.coveredExternal, usage.external)
    const offset = index * FIELDS.length
    const state = this.nativeState
    const storage = this.settlementStorage
    storage[offset] = at
    storage[offset + 1] = usage.rss
    storage[offset + 2] = usage.heapUsed
    storage[offset + 3] = usage.external
    storage[offset + 4] = usage.arrayBuffers
    storage[offset + 5] = usage.heapUsed + usage.external
    storage[offset + 6] = state.submittedId
    storage[offset + 7] = state.submittedJobId
    storage[offset + 8] = state.submittedCodec
    storage[offset + 9] = state.settledId
    storage[offset + 10] = state.settledJobId
    storage[offset + 11] = state.settledCodec
    storage[offset + 12] = state.submittedId === state.settledId ? 0 : 1
  }

  /**
   * Uses the unchanged pair/turn catalogue, flushing bounded scalar facts only after the final read.
   * @param {string} phase operation/checkpoint name @returns {Promise<object>} original last reading
   */
  settled(phase = this.operationPhase) {
    this.operationPhase = phase
    return super.settled(
      (round, stage) => this.observeSettlement(round, stage),
      (reading) => this.recordSettlement(phase, reading)
    )
  }

  /**
   * Flushes scalar scratch synchronously after the original final reading, without another await.
   * @param {string} phase operation name @param {object} reading original final reading
   * @returns {void} @throws {Error} on finite transport exhaustion
   */
  recordSettlement(phase, reading) {
    const count = reading.gc.checkpoints.length * STAGES.length
    const observations = Array.from({ length: count }, (_, index) => ({
      round: Math.floor(index / STAGES.length),
      stage: STAGES[index % STAGES.length],
      ...Object.fromEntries(
        FIELDS.map((key, field) => [key, this.settlementStorage[index * FIELDS.length + field]])
      ),
    }))
    reading.gc.diagnosticId = ++this.settlementId
    this.log.write('settlement', {
      id: this.settlementId,
      phase,
      readingAt: reading.at,
      checkpoints: reading.gc.checkpoints,
      observations,
    })
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
