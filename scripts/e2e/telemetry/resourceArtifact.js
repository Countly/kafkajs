'use strict'

/**
 * Judges and validates the machine-readable measurements the quiet resource job records, so the
 * numbers that decide a run live in one place and a recorded artifact can be re-checked after the
 * run. An artifact that is missing, malformed, from another tree or Node, of an older schema,
 * taken on a noisy host or above a limit is a failure or inconclusive, never a pass.
 *
 * The measurement is incremental. Each arm runs a telemetry-disabled control of the same kind in
 * the same process, after everything is loaded and before telemetry exists, and the telemetry
 * peaks are judged above that control. The judge recomputes every difference and ceiling from the
 * recorded readings and never trusts an advertised delta or verdict:
 *
 * ```
 * B       = min(C0.rss, C1.rss)               Eref = min(C0.external, C1.external)
 * P       = max over marks of max(high water, current RSS covered by the mark)
 * Epeak   = max over marks of the external bytes covered by the mark
 * RSS     delta P - B <= 64 MiB,  P <= 160 MiB,  external delta Epeak - Eref <= 64 MiB
 * control C1 and C0 settled readings within 2 MiB RSS and 256 KiB retained; setup peak <= 96 MiB
 * ```
 *
 * Usage: node scripts/e2e/telemetry/resourceArtifact.js <directory> --tree <hash> --node <version>
 */
const fs = require('node:fs')
const path = require('node:path')
const { compareSnapshots } = require('../../../testHelpers/resourceProbe')

/** The version of the recorded shape; an older artifact is not evidence for the current gate. */
const SCHEMA = 2

const KINDS = Object.freeze(['producer', 'consumer', 'admin', 'shareConsumer'])
const ARMS = Object.freeze(['public', 'collector'])

/** The acceptance values of one single-client memory arm, in bytes and milliseconds. */
const LIMITS = Object.freeze({
  rssDelta: 67108864,
  externalDelta: 67108864,
  setupPeak: 100663296,
  absolutePeak: 167772160,
  controlRss: 2097152,
  controlRetained: 262144,
  retained: 8388608,
  closedTransient: 262144,
  closeAfterMarkMs: 10000,
  outputBytes: 65536,
  descriptors: 128,
  descriptorBytes: 354304,
  rawBytes: 1048576,
  gcPauseMs: 100,
  minCollections: 2,
  acceptedPushes: 3,
})

/** What a host may look like at the start of a run before its numbers stop meaning anything. */
const NOISE = Object.freeze({ loadPerCpu: 0.75, minFreeBytes: 268435456, memoryPressureAvg10: 5 })

/** The absolute peak source the Linux lane selects before any work. */
const PEAK_SOURCE = Object.freeze({ name: 'proc-self-status-vmhwm', units: 'bytes' })

/** The fixed foreground catalogue: how many operations each public open runs, and their size. */
const OPS_PER_LIFECYCLE = 3
const RECORD_BYTES = 256

/** The ordinary operation each kind runs, by name. */
const OPERATION = Object.freeze({
  producer: 'send',
  admin: 'fetchTopicMetadata',
  consumer: 'commitOffsets',
  shareConsumer: 'handle',
})

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
const READING_FIELDS = Object.freeze(['heapUsed', 'external', 'arrayBuffers', 'retained', 'rss'])
const MARK_FIELDS = Object.freeze(['rss', 'maxRss', 'covered', 'coveredExternal'])

/**
 * @param {*} value
 * @returns {boolean} whether it is a finite non-negative integer
 */
const count = (value) => Number.isSafeInteger(value) && value >= 0

/**
 * @param {*} value
 * @returns {boolean} whether it is a finite non-negative number
 */
const time = (value) => Number.isFinite(value) && value >= 0

/**
 * @param {*} a @param {*} b @returns {boolean} whether two JSON values are the same
 */
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b)

/**
 * The fixed order of the phases an arm passes, independent of what a run recorded.
 * @param {'public'|'collector'} arm which arm
 * @param {boolean} zstdSupported whether this Node line offers ZSTD
 * @returns {string[]} every phase at which a high-water mark is taken
 */
const phaseCatalogue = (arm, zstdSupported) => {
  const prefix = arm === 'public' ? 'enabled' : 'collector'
  return [
    'startup',
    'loaded',
    'control:start',
    'control:c0',
    'control:c1',
    'control:released',
    `${prefix}:start`,
    ...(arm === 'public' ? ['enabled:registered'] : []),
    `${prefix}:matched`,
    ...['gzip', ...(zstdSupported ? ['zstd'] : [])].map((codec) => `${prefix}:${codec}`),
    `${prefix}:held`,
    `${prefix}:closed`,
    'final',
  ]
}

/**
 * @param {string} kind public client kind
 * @param {number} lifecycles how many times the client is opened and works
 * @returns {{lifecycles:number, opsPerLifecycle:number, recordBytes:number,
 *   ops:Array<{lifecycle:number, op:string, bytes:number, ok:boolean}>}} the fixed foreground
 *   catalogue, with the outcome every operation must have
 */
const foregroundCatalogue = (kind, lifecycles) => ({
  lifecycles,
  opsPerLifecycle: OPS_PER_LIFECYCLE,
  recordBytes: RECORD_BYTES,
  ops: Array.from({ length: lifecycles * OPS_PER_LIFECYCLE }, (_, index) => ({
    lifecycle: Math.floor(index / OPS_PER_LIFECYCLE),
    op: OPERATION[kind],
    bytes: RECORD_BYTES,
    ok: true,
  })),
})

/**
 * @param {object} context one host context reading
 * @returns {boolean} whether it carries every mandatory field; an unavailable pressure source is
 *   an explicit `null`, a missing key is an incomplete context
 */
const completeContext = (context) =>
  count(context?.cpus) &&
  context.cpus > 0 &&
  Array.isArray(context.load) &&
  context.load.length === 3 &&
  context.load.every(Number.isFinite) &&
  count(context.freeBytes) &&
  count(context.totalBytes) &&
  (context.psi === null || Number.isFinite(context.psi?.memorySomeAvg10))

/**
 * @param {object} context a complete host context
 * @returns {string[]} the ways the host was too busy to trust the run
 */
const noiseOf = (context) => {
  const noisy = []
  if (context.load[0] / context.cpus > NOISE.loadPerCpu) noisy.push('host load was high')
  if (context.freeBytes < NOISE.minFreeBytes) noisy.push('free memory was low')
  if (context.psi && context.psi.memorySomeAvg10 > NOISE.memoryPressureAvg10)
    noisy.push('memory pressure was high')
  return noisy
}

/** Judges one recorded memory arm by recomputing every fact, difference and ceiling from it. */
class MemoryArmJudge {
  /**
   * @param {object} record `{ schema, arm, kind, stamp, exit, context, readings }` as the resource
   *   spec writes it
   * @returns {MemoryArmJudge} a judge that has not looked at the record yet
   */
  constructor(record) {
    this.record = record ?? {}
    this.readings = this.record.readings
    this.failures = []
    this.inconclusive = []
  }

  /** @param {string} problem a broken fact or an observed violation @returns {void} */
  fail(problem) {
    if (!this.failures.includes(problem)) this.failures.push(problem)
  }

  /** @param {string} problem why the numbers cannot decide @returns {void} */
  wait(problem) {
    if (!this.inconclusive.includes(problem)) this.inconclusive.push(problem)
  }

  /**
   * @returns {{ verdict: 'pass'|'fail'|'inconclusive', problems: string[] }} `fail` for any broken
   *   fact or observed gate violation, `inconclusive` when only a missing capability, an
   *   inadmissible control or the host stops the numbers deciding
   */
  evaluate() {
    this.envelope()
    if (this.record.schema === SCHEMA && this.readings) this.measurements()
    else if (this.record.schema === SCHEMA) this.fail('readings are missing')
    const verdict = this.failures.length
      ? 'fail'
      : this.inconclusive.length
        ? 'inconclusive'
        : 'pass'
    return { verdict, problems: [...this.failures, ...this.inconclusive] }
  }

  /** @returns {void} judges the schema, the child's exit and the host contexts */
  envelope() {
    const { schema, arm, kind, exit, context } = this.record
    if (schema !== SCHEMA) this.fail('unknown schema')
    if (!ARMS.includes(arm)) this.fail('unknown arm')
    if (!KINDS.includes(kind)) this.fail('unknown kind')

    if (exit?.timedOut !== false) this.fail('child exceeded its bound')
    if (exit?.code !== 0 || exit?.signal !== null)
      this.fail('child did not exit with code 0 and no signal')
    if (exit?.stderrBytes !== 0) this.fail('child wrote to stderr')
    if (!count(exit?.stdoutBytes) || exit.stdoutBytes > LIMITS.outputBytes)
      this.fail('child output exceeded its bound')
    const ran = this.readings?.manifest?.gc?.available !== false
    if (exit?.receipts !== (arm === 'public' && ran ? 1 : 0))
      this.fail('child sent the wrong number of disconnect receipts')
    if (!(exit?.closedAfterMarkMs <= LIMITS.closeAfterMarkMs))
      this.fail('child did not close within 10 s of its receipt')

    const current = schema === SCHEMA && this.readings
    const contexts = current ? this.readings.contexts : undefined
    for (const [label, reading, child] of [
      ['', context?.before, false],
      [' after cleanup', context?.after, false],
      [' before the control', contexts?.control, true],
      [' at the control boundary', contexts?.boundary, true],
      [' at the end', contexts?.final, true],
    ]) {
      if (child && !current) continue
      if (!completeContext(reading)) this.fail(`host context is incomplete${label}`)
      else for (const noisy of noiseOf(reading)) this.wait(`${noisy}${label}`)
    }
  }

  /** @returns {void} judges everything the readings carry */
  measurements() {
    const { manifest } = this.readings
    if (manifest?.gc?.available === false) {
      this.wait('real exposed GC is unavailable')
      return
    }
    if (!this.structure()) return
    const peaks = this.peaks()
    const admitted = this.control(peaks)
    this.limits(peaks, admitted)
    this.native()
    if (this.record.arm === 'public') this.publicFacts()
    else this.collectorFacts()
  }

  /**
   * @param {string} name its name in a problem @param {*} reading one reading
   * @param {boolean} settled whether it must carry the collection facts of a settled reading
   * @returns {boolean} whether it is complete and its arithmetic holds
   */
  checkReading(name, reading, settled) {
    const complete = READING_FIELDS.every((field) => count(reading?.[field])) && time(reading?.at)
    if (!complete) {
      this.fail(`${name} reading is incomplete`)
      return false
    }
    if (reading.retained !== reading.heapUsed + reading.external)
      this.fail(`${name} retained bytes are not heap plus external`)
    if (reading.arrayBuffers > reading.external)
      this.fail(`${name} array buffers exceed external bytes`)
    if (settled) {
      const gc = reading.gc
      if (!gc || !time(gc.admittedAt) || gc.completedAt !== reading.at || !count(gc.observed))
        this.fail(`${name} collection facts are incomplete`)
      else {
        if (gc.completedAt - gc.admittedAt < LIMITS.gcPauseMs)
          this.fail(`${name} collection pause was shortened`)
        if (gc.observed < LIMITS.minCollections) this.wait(`${name} collections were not observed`)
      }
    }
    return true
  }

  /**
   * @returns {boolean} whether the fixed catalogues, source declarations and reading shapes hold
   *   well enough for the numbers to be read
   */
  structure() {
    const r = this.readings
    const { manifest, marks } = r
    const before = this.failures.length
    if (!manifest || typeof manifest !== 'object') this.fail('manifest is missing')
    if (!Array.isArray(marks))
      this.fail('peak marks are missing: aggregate evidence is not accepted')
    if (this.failures.length > before) return false

    if (manifest.kind !== this.record.kind || manifest.arm !== this.record.arm)
      this.fail('manifest is for another kind or arm')
    if (Object.values(manifest.instrumented ?? { missing: true }).some(Boolean))
      this.fail('child ran under an inspector, coverage or Jest')
    if (!manifest.execArgv?.includes('--expose-gc')) this.fail('child did not expose GC')
    if (manifest.gc?.native !== true) this.fail('GC function is not the engine collection')
    const codecCount = manifest.codecs?.zstd === true ? 2 : 1
    const catalogue = phaseCatalogue(this.record.arm, manifest.codecs?.zstd === true)
    if (!same(manifest.phases, catalogue)) this.fail('manifest phases are not the fixed catalogue')
    if (
      !same(
        marks.map((mark) => mark?.phase),
        catalogue
      )
    )
      this.fail('peak marks are not the fixed phase catalogue')
    if (!same(manifest.foreground, foregroundCatalogue(this.record.kind, 1 + 2 * codecCount)))
      this.fail('foreground catalogue is not the fixed one')

    if (manifest.peakSource === null) this.wait('no usable absolute peak source')
    else if (!same(manifest.peakSource, PEAK_SOURCE))
      this.fail('peak source is not the selected one')

    for (const [name, reading] of [
      ['startup', r.startup],
      ['loaded', r.loaded],
    ])
      this.checkReading(name, reading, false)
    const settled = [
      ['c0', r.c0],
      ['c1', r.c1],
      ['released', r.released],
      ['matched', r.matched],
      ...(Array.isArray(r.actives) ? r.actives.map((a, i) => [`active ${i + 1}`, a]) : []),
      ['closed', r.closed],
    ]
    if (!Array.isArray(r.actives) || r.actives.length !== codecCount + 1)
      this.fail('active readings are not one per codec and one held')
    for (const [name, reading] of settled) this.checkReading(name, reading, true)
    const ordered = [r.startup, r.loaded, ...settled.map(([, reading]) => reading)]
    if (ordered.every((reading) => time(reading?.at)))
      ordered.reduce((previous, reading) => {
        if (reading.at < previous) this.fail('readings are out of order')
        return Math.max(previous, reading.at)
      }, 0)
    return this.failures.length === before
  }

  /**
   * Validates the marks and recomputes the peaks and baselines from them.
   * @returns {{B:number, Eref:number, P:number, Epeak:number, setupPeak:number,
   *   usable:boolean}|null} the quantities every gate uses, or null when the marks are unusable
   */
  peaks() {
    const { marks, c0, c1, manifest } = this.readings
    const claimed = manifest.peakSource !== null
    let usable = claimed
    let previous = null
    for (const mark of marks) {
      if (!MARK_FIELDS.every((field) => count(mark?.[field])) || !time(mark?.at)) {
        this.fail(`${mark?.phase} mark is incomplete`)
        return null
      }
      if (claimed && !count(mark.vmhwm)) {
        this.fail('peak source is claimed but a mark has no reading')
        return null
      }
      if (previous) {
        if (mark.at < previous.at) this.fail('marks are out of order')
        if (mark.maxRss < previous.maxRss) this.fail('raw maximum decreased')
        if (claimed && mark.vmhwm < previous.vmhwm) this.fail('authoritative peak decreased')
      }
      if (claimed && mark.vmhwm < mark.covered) {
        this.wait('peak source does not cover a current reading')
        usable = false
      }
      previous = mark
    }
    const peakOf = (mark) => Math.max(claimed ? mark.vmhwm : 0, mark.covered)
    const upToSetup = marks.slice(
      0,
      marks.findIndex((mark) => mark.phase === 'control:released') + 1
    )
    return {
      B: Math.min(c0.rss, c1.rss),
      Eref: Math.min(c0.external, c1.external),
      P: Math.max(...marks.map(peakOf)),
      Epeak: Math.max(...marks.map((mark) => mark.coveredExternal)),
      setupPeak: Math.max(...upToSetup.map(peakOf)),
      usable,
    }
  }

  /**
   * Judges the control's facts, release and admission, and that no reading is understated.
   * @param {object|null} peaks what `peaks` returned
   * @returns {boolean} whether the control is admitted, so the incremental gates can decide
   */
  control(peaks) {
    const r = this.readings
    const { manifest, control, resources } = r
    const arm = this.record.arm
    const prefix = arm === 'public' ? 'enabled' : 'collector'
    if (
      !control ||
      control.enabled !== false ||
      control.api71 !== 0 ||
      control.api72 !== 0 ||
      control.nativeSubmissions !== 0 ||
      control.probe !== 'disabled'
    )
      this.fail('control performed telemetry')
    if (control && (control.liveSockets !== 0 || control.liveRequests !== 0))
      this.fail('control was not released')
    const roles = resources?.released?.control
    if (!roles) this.fail('control resource role is missing')
    else if (Object.values(roles).some((ids) => ids.length))
      this.fail('control handles survive its release')
    if (control && !same(control.ops, manifest.foreground.ops))
      this.fail('control foreground differs from the fixed catalogue')
    if (arm === 'public' && !same(r.enabled?.ops, manifest.foreground.ops))
      this.fail('enabled foreground differs from the fixed catalogue')

    if (!peaks) return false
    const byPhase = new Map(r.marks.map((mark) => [mark.phase, mark]))
    const covers = [
      ['startup', r.startup],
      ['loaded', r.loaded],
      ['control:c0', r.c0],
      ['control:c1', r.c1],
      ['control:released', r.released],
      [`${prefix}:matched`, r.matched],
      ...r.actives.map((reading) => [`${prefix}:${reading.phase}`, reading]),
      [`${prefix}:closed`, r.closed],
    ]
    for (const [phase, reading] of covers) {
      const mark = byPhase.get(phase)
      if (mark.covered < reading.rss || mark.coveredExternal < reading.external)
        this.fail('a peak mark understates a reading')
    }
    const covered = Math.max(...r.marks.map((mark) => mark.covered))
    const coveredExternal = Math.max(...r.marks.map((mark) => mark.coveredExternal))
    const samples = r.samples
    if (!samples || !count(samples.count) || !time(samples.maxGapMs) || !time(samples.peak?.rss))
      this.fail('sampling facts are incomplete')
    else if (samples.peak.rss > covered || samples.peak.external > coveredExternal)
      this.fail('a peak mark understates the sampled peak')

    let admitted = peaks.usable && manifest.gc.available === true
    if (Math.abs(r.c1.rss - r.c0.rss) > LIMITS.controlRss) {
      this.wait('control RSS is unstable')
      admitted = false
    }
    if (Math.abs(r.c1.retained - r.c0.retained) > LIMITS.controlRetained) {
      this.wait('control retained bytes are unstable')
      admitted = false
    }
    if (peaks.setupPeak > LIMITS.setupPeak) {
      this.wait('setup exceeds the 96 MiB admission allowance')
      admitted = false
    }
    return admitted
  }

  /**
   * Judges the incremental RSS and external gates against the admitted control, the absolute
   * backstop, and the retained and transient gates against the matched baseline.
   * @param {object|null} peaks what `peaks` returned @param {boolean} admitted the control verdict
   * @returns {void}
   */
  limits(peaks, admitted) {
    const r = this.readings
    if (peaks && peaks.usable) {
      if (peaks.P > LIMITS.absolutePeak) this.fail('absolute current-image peak exceeds 160 MiB')
      if (admitted) {
        if (peaks.P - peaks.B > LIMITS.rssDelta)
          this.fail('telemetry RSS peak exceeds 64 MiB above the disabled control')
        if (peaks.Epeak - peaks.Eref > LIMITS.externalDelta)
          this.fail('external memory peak exceeds 64 MiB above the disabled control')
      }
    }
    for (const active of r.actives)
      if (active.retained - r.matched.retained > LIMITS.retained)
        this.fail('active retained bytes exceed the matched baseline by more than 8 MiB')
    if (r.closed.retained - r.matched.retained > LIMITS.closedTransient)
      this.fail('closed transient bytes exceed 256 KiB')
  }

  /**
   * Judges the native job journal of both arms: bounded, ordered, with real current and retired
   * completions of each codec and distinct backing stores counted once.
   * @returns {void}
   */
  native() {
    const { native, samples, manifest } = this.readings
    if (!native || !Array.isArray(native.rows)) {
      this.fail('native journal is missing')
      return
    }
    if (native.overflow !== false) this.fail('native journal overflowed')
    if (native.live !== 0) this.fail('native work is still pending')
    if (native.maxLive > 1) this.fail('more than one native job was occupied at once')
    const positive = (value) => Number.isSafeInteger(value) && value > 0
    let last = 0
    for (const row of native.rows) {
      const name = `${row?.codec} job ${row?.id}`
      if (!(row?.id > last)) this.fail(`${name} is out of order`)
      last = row?.id
      if (![row?.jobId, row?.inputId, row?.inputBackingId].every(positive))
        this.fail(`${name} lacks issued identities`)
      if (!(row?.inputBytes > 1000 && row.inputBytes <= LIMITS.rawBytes))
        this.fail(`${name} raw bytes are out of range`)
      if (row?.outcome !== 'resolved') this.fail(`${name} did not resolve`)
      else {
        if (![row.resultId, row.resultBackingId].every(positive))
          this.fail(`${name} lacks a result identity`)
        if (!(row.resultBytes > 0 && row.resultBytes < row.inputBytes))
          this.fail(`${name} payload was not compressed`)
        if (row.resultBytes > LIMITS.rawBytes)
          this.fail(`${name} transmitted bytes are out of range`)
        const distinct =
          row.inputBackingId === row.resultBackingId
            ? row.inputBackingBytes
            : row.inputBackingBytes + row.resultBackingBytes
        if (row.distinctBackingBytes !== distinct)
          this.fail(`${name} backing stores are not counted once each`)
        if (row.inputBackingBytes < row.inputBytes || row.resultBackingBytes < row.resultBytes)
          this.fail(`${name} backing capacity is below its bytes`)
      }
      for (const edge of [row?.submitted, row?.settled])
        if (!edge || !READING_FIELDS.every((field) => count(edge[field])) || !time(edge.at))
          this.fail(`${name} lacks a reading at submission or settlement`)
      if (row?.submitted?.at > row?.settled?.at)
        this.fail(`${name} settled before it was submitted`)
    }
    for (const codec of ['gzip', ...(manifest.codecs?.zstd === true ? ['zstd'] : [])]) {
      const rows = native.rows.filter((row) => row.codec === codec && row.outcome === 'resolved')
      if (!rows.some((row) => row.retired === false) || !rows.some((row) => row.retired === true))
        this.fail(`${codec} native work lacks a current or a retired completion`)
    }
    if (samples && samples.count < 2 * native.rows.length)
      this.fail('too few current readings around native work')
  }

  /** @returns {void} judges what only the public-client arm records */
  publicFacts() {
    const r = this.readings
    const codecs = ['gzip', ...(r.manifest.codecs?.zstd === true ? ['zstd'] : [])]
    if (!UUID.test(r.uuid) || r.uuidHeld !== r.uuid)
      this.fail('client instance id is missing or changed')
    if (r.descriptors !== LIMITS.descriptors) this.fail('descriptor count is not 128')
    if (
      r.manifest.fixture?.descriptors !== LIMITS.descriptors ||
      r.manifest.fixture?.descriptorBytes !== LIMITS.descriptorBytes
    )
      this.fail('descriptor population is not the maximum')
    if (!same(r.matchedState, { connected: false, descriptors: LIMITS.descriptors, uuid: r.uuid }))
      this.fail('matched baseline is not the disconnected registry')
    if (!(r.enabled?.api71 >= 1) || !(r.enabled?.api72 >= 1))
      this.fail('enabled client made no telemetry request')
    for (const codec of codecs) {
      const first = r.native?.rows?.find((row) => row.codec === codec && row.retired === false)
      const pushes = r.pushes?.[codec]
      const decoded = r.decoded?.find((entry) => entry.codec === codec)
      if (!(pushes?.accepted >= LIMITS.acceptedPushes))
        this.fail(`${codec} accepted pushes are fewer than required`)
      if (!r.reopens?.some((reopen) => reopen.codec === codec && reopen.immediate === true))
        this.fail(`${codec} has no immediate reopen from the close continuation`)
      if (!decoded || decoded.format !== true)
        this.fail(`${codec} payload is not in its codec format`)
      else if (
        first &&
        (decoded.rawBytes !== first.inputBytes ||
          decoded.transmittedBytes !== first.resultBytes ||
          pushes?.transmittedBytes !== first.resultBytes)
      )
        this.fail(`${codec} decoded payload differs from the native evidence`)
    }
    const { matched, closed } = r.resources ?? {}
    if (!matched || !closed) this.fail('resource roles are missing')
    else {
      this.failures.push(
        ...compareSnapshots(
          { resources: matched },
          { resources: closed },
          { resources: ['target', 'fixture'] }
        ).map((problem) => `resource roles: ${problem}`)
      )
      if (Object.values(closed.target ?? {}).some((ids) => ids.length))
        this.fail('target handles survive the public close')
    }
  }

  /** @returns {void} judges what only the collector-only arm records */
  collectorFacts() {
    const { builds, manifest } = this.readings
    const codecs = ['gzip', ...(manifest.codecs?.zstd === true ? ['zstd'] : [])]
    if (
      !same(
        builds?.map((build) => build.codec),
        codecs
      )
    )
      this.fail('native builds are not the expected codecs')
    for (const build of builds ?? []) {
      if (build.compressionType !== (build.codec === 'zstd' ? 4 : 1))
        this.fail(`${build.codec} build used another codec`)
      if (!(build.rawBytes > 1000 && build.rawBytes <= LIMITS.rawBytes))
        this.fail(`${build.codec} raw bytes are out of range`)
      if (!(build.transmittedBytes > 0 && build.transmittedBytes < build.rawBytes))
        this.fail(`${build.codec} payload was not compressed`)
      if (build.points !== 257 || build.retiredCompletion !== true)
        this.fail(`${build.codec} collector build is incomplete`)
    }
  }
}

/**
 * Judges one recorded memory arm.
 * @param {object} record `{ schema, arm, kind, stamp, exit, context, readings }` as the resource
 *   spec writes it
 * @returns {{ verdict: 'pass'|'fail'|'inconclusive', problems: string[] }} `fail` for any broken
 *   fact or observed gate violation, `inconclusive` when only a missing capability, an
 *   inadmissible control or the host stops the numbers deciding
 */
const judgeMemoryArm = (record) => new MemoryArmJudge(record).evaluate()

/**
 * Validates every arm of a run's artifact directory.
 * @param {string} directory holds `resources-<arm>-<kind>.json`, one file per arm and kind
 * @param {{ tree: string, node: string }} expected the tree and Node this run belongs to
 * @returns {{ ok: boolean, results: Array<{ arm: string, kind: string, verdict: string,
 *   problems: string[] }> }} one result per expected arm and kind; `ok` only when all pass. The
 *   child must also have run on the Node the artifact is stamped for. Two
 *   artifacts that claim the same child identity fail both, so one child's control cannot serve
 *   another arm.
 */
const validateResourceArtifacts = (directory, { tree, node }) => {
  const results = []
  const children = new Map()
  for (const arm of ARMS)
    for (const kind of KINDS) {
      const file = path.join(directory, `resources-${arm}-${kind}.json`)
      let verdict = { verdict: 'fail', problems: ['artifact is missing'] }
      if (fs.existsSync(file)) {
        try {
          const record = JSON.parse(fs.readFileSync(file, 'utf8'))
          const childNode = record.readings?.manifest?.node
          const stamped =
            record.stamp?.tree === tree &&
            record.stamp?.node === node &&
            (childNode === undefined || childNode === node)
          verdict = judgeMemoryArm(record)
          const child = record.readings?.manifest?.childId
          if (child !== undefined)
            children.set(child, [...(children.get(child) ?? []), results.length])
          if (!stamped || record.arm !== arm || record.kind !== kind)
            verdict = {
              verdict: 'fail',
              problems: [
                'artifact is not stamped for this tree, Node, arm and kind',
                ...verdict.problems,
              ],
            }
        } catch (error) {
          verdict = { verdict: 'fail', problems: [`artifact is not valid JSON: ${error.message}`] }
        }
      }
      results.push({ arm, kind, ...verdict })
    }
  for (const indexes of children.values())
    if (indexes.length > 1)
      for (const index of indexes)
        results[index] = {
          ...results[index],
          verdict: 'fail',
          problems: ['artifact shares a child identity with another', ...results[index].problems],
        }
  return { ok: results.every((result) => result.verdict === 'pass'), results }
}

/**
 * @param {string[]} argv `<directory> --tree <hash> --node <version>`
 * @returns {number} the exit code: 0 only when every arm passed
 */
const main = (argv) => {
  const [directory, ...rest] = argv
  const option = (name) => rest[rest.indexOf(name) + 1]
  if (!directory || !option('--tree') || !option('--node')) {
    process.stderr.write('Usage: resourceArtifact.js <directory> --tree <hash> --node <version>\n')
    return 2
  }
  const summary = validateResourceArtifacts(directory, {
    tree: option('--tree'),
    node: option('--node'),
  })
  fs.writeFileSync(path.join(directory, 'summary.json'), `${JSON.stringify(summary, null, 2)}\n`)
  process.stdout.write(`${JSON.stringify(summary)}\n`)
  return summary.ok ? 0 : 1
}

module.exports = {
  ARMS,
  KINDS,
  LIMITS,
  NOISE,
  OPERATION,
  OPS_PER_LIFECYCLE,
  PEAK_SOURCE,
  RECORD_BYTES,
  SCHEMA,
  foregroundCatalogue,
  judgeMemoryArm,
  phaseCatalogue,
  validateResourceArtifacts,
}

if (require.main === module) process.exitCode = main(process.argv.slice(2))
