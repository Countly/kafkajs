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
 * P       = max over marks of max(high water, current RSS covered by the mark), every recorded
 *           current reading (native submission and settlement included) and the sampled peak
 * Epeak   = the same maximum over the external bytes
 * data(x) = heapUsed + external - (V8 bytecode + compiled code), at the final settle checkpoint
 * RSS     delta P - B <= 64 MiB,  P <= 160 MiB,  external delta Epeak - Eref <= 64 MiB
 * control C1 and C0 settled readings within 2 MiB RSS and 256 KiB data; setup peak <= 96 MiB
 * closed  data(closed) - data(M) <= 256 KiB; below -256 KiB is inconclusive
 * warm-up one full throwaway pass of the same kind and arm runs before M; M0 is its reading at the
 *         M position, and data(M) - data(M0) <= 1 MiB
 * ```
 *
 * Only the control, closed and first-use gates use `data`; the RSS, external, peak and active
 * retained gates read the raw bytes. A record that declares a seed is a control, never acceptance.
 *
 * Usage: node scripts/e2e/telemetry/resourceArtifact.js <directory> --tree <hash> --node <version>
 */
const fs = require('node:fs')
const path = require('node:path')
const crypto = require('node:crypto')
const { StringDecoder } = require('node:string_decoder')
const { compareSnapshots } = require('../../../testHelpers/resourceProbe')
const {
  MEMORY_FLAGS,
  SETTLE,
  collectionsSettled,
  dataBytes,
} = require('../../../testHelpers/processMemory')

/** The version of the recorded shape; an older artifact is not evidence for the current gate. */
const SCHEMA = 6

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
  firstUse: 1048576,
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

/** The child declarations every arm records, and those its stamp must repeat. */
const CHILD_CONTEXT = Object.freeze([
  'childId',
  'pid',
  'tree',
  'runId',
  'node',
  'v8',
  'platform',
  'arch',
])
const STAMPED_CONTEXT = Object.freeze(['tree', 'runId', 'node', 'platform', 'arch'])

/** The sampling period while native work is pending, in milliseconds. */
const SAMPLE_MS = 20

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
 * @param {number} index descriptor number
 * @returns {{name: string, unit: string, type: string, attributes: Record<string, string>}} the
 *   metadata of the largest descriptor the public fields allow: name 200, unit 16, eight 63-byte
 *   keys and eight 256-byte values, 2768 bytes in all
 */
const maximumDescriptor = (index) => ({
  name: `com.example.resource.${index}`.padEnd(200, 'x'),
  unit: 'u'.repeat(16),
  type: 'sum',
  attributes: Object.fromEntries(
    Array.from({ length: 8 }, (_, a) => [`a${a}`.padEnd(63, 'k'), 'x'.repeat(256)])
  ),
})

/**
 * @param {{name: string, type: string, unit?: string, attributes?: Record<string, string>}} metric
 *   a descriptor the public API accepted
 * @returns {{identity: string, charge: number}} a hash of its identity and metadata, and the bytes
 *   the registry charges for it (name, unit, keys and values)
 */
const registryEntry = (metric) => {
  const attributes = Object.entries(metric.attributes ?? {}).sort(([a], [b]) => a.localeCompare(b))
  const unit = metric.unit ?? ''
  const charge = attributes.reduce(
    (sum, [key, value]) => sum + Buffer.byteLength(key) + Buffer.byteLength(value),
    Buffer.byteLength(metric.name) + Buffer.byteLength(unit)
  )
  const metadata = JSON.stringify([metric.name, metric.type, unit, attributes])
  return { identity: crypto.createHash('sha256').update(metadata).digest('hex'), charge }
}

/** @returns {Array<{identity: string, charge: number}>} the fixed maximum registration catalogue */
const registryCatalogue = () =>
  Array.from({ length: LIMITS.descriptors }, (_, index) => registryEntry(maximumDescriptor(index)))

/**
 * @param {Array<{identity: string, charge: number}>} entries successful registrations, in order
 * @param {string} uuid the assigned client instance id
 * @returns {{uuid: string, descriptors: number, charge: number, hash: string}} the fingerprint of
 *   the registered population a retained comparison must keep
 */
const registryFingerprint = (entries, uuid) => ({
  uuid,
  descriptors: entries.length,
  charge: entries.reduce((sum, entry) => sum + entry.charge, 0),
  hash: crypto.createHash('sha256').update(JSON.stringify(entries)).digest('hex'),
})

/**
 * What the registry probe of a retained checkpoint must observe on the production client besides
 * the fingerprint of the descriptors it updated in place: a new identity refused at the limit.
 */
const REGISTRY_PROBE = Object.freeze({ overflow: 'limit' })

/**
 * @param {Array<{action: string, at: number}>} events what a disconnect continuation observed
 * @param {string} action the call that must come next
 * @returns {boolean} whether the public disconnect resolved, `action` ran next and only then did
 *   the continuation first yield, at nondecreasing times
 */
const immediateBoundary = (events, action) =>
  Array.isArray(events) &&
  same(
    events.map((event) => event?.action),
    ['disconnect', action, 'yield']
  ) &&
  events.every(
    (event, index) => time(event.at) && (index === 0 || event.at >= events[index - 1].at)
  )

/**
 * Reads a memory child's stdout incrementally, within the output bound, and keeps only its fixed
 * message catalogue: at most one explicit disconnect receipt from the public arm, then exactly one
 * readings message. A malformed, foreign, duplicate, out-of-order or unterminated line is a failure,
 * never skipped. The first failure is kept.
 */
class MemoryOutput {
  /**
   * @param {{arm: string, kind: string, childId: string}} expected the identity the parent issued
   * @param {() => number} [now] the parent's clock, in milliseconds
   * @returns {MemoryOutput} a reader that has seen nothing
   */
  constructor(expected, now = Date.now) {
    Object.assign(this, { expected, now })
    this.decoder = new StringDecoder('utf8')
    this.pending = ''
    this.bytes = 0
    this.failure = null
    this.receipt = null
    this.receiptAt = null
    this.readings = null
    this.readingsAt = null
    this.results = 0
  }

  /** @param {string} problem what went wrong @returns {void} keeps the first failure only */
  fail(problem) {
    this.failure ??= problem
  }

  /**
   * @param {Buffer} chunk bytes from the pipe
   * @returns {void} consumes every complete line; past the bound nothing more is read
   */
  write(chunk) {
    this.bytes += chunk.length
    if (this.bytes > LIMITS.outputBytes) {
      this.fail('child output exceeded its bound')
      return
    }
    this.pending += this.decoder.write(chunk)
    let end
    while ((end = this.pending.indexOf('\n')) !== -1) {
      const line = this.pending.slice(0, end)
      this.pending = this.pending.slice(end + 1)
      this.line(line)
    }
  }

  /** @param {string} line one complete message @returns {void} checks its identity and order */
  line(line) {
    let message
    try {
      message = JSON.parse(line)
    } catch {
      this.fail('unreadable output line')
      return
    }
    if (message?.receipt === 'disconnect') this.disconnect(message)
    else if (message && same(Object.keys(message), ['readings'])) {
      this.results += 1
      if (this.results !== 1) this.fail('duplicate readings message')
      else {
        this.readings = message.readings
        this.readingsAt = this.now()
      }
    } else this.fail('foreign output message')
  }

  /** @param {object} message a disconnect receipt @returns {void} admits it once, before readings */
  disconnect(message) {
    const valid =
      this.expected.arm === 'public' &&
      same(Object.keys(message).sort(), ['at', 'childId', 'kind', 'receipt', 'uuid']) &&
      message.kind === this.expected.kind &&
      message.childId === this.expected.childId &&
      UUID.test(message.uuid) &&
      time(message.at)
    if (!valid) this.fail('foreign disconnect receipt')
    else if (this.receipt || this.results) this.fail('duplicate or late disconnect receipt')
    else {
      this.receipt = message
      this.receiptAt = this.now()
    }
  }

  /**
   * Ends the stream once the child closed.
   * @returns {{failure: string|null, receipt: object|null, receipts: number, results: number,
   *   readings: object|null, disconnectObservedAt: number|null, closedAfterMarkMs: number|null}}
   *   the catalogue facts; the close is timed from the receipt, or from the readings where the
   *   child owes no receipt
   */
  finish() {
    this.pending += this.decoder.end()
    if (this.pending) this.fail('unterminated output line')
    const owed = this.expected.arm === 'public' && this.readings?.manifest?.gc?.available !== false
    if (this.results !== 1) this.fail('missing readings message')
    if (Boolean(this.receipt) !== owed) this.fail('missing or unexpected disconnect receipt')
    if (this.receipt && this.receipt.uuid !== this.readings?.uuid)
      this.fail('disconnect receipt UUID differs from the readings')
    const mark = owed ? this.receiptAt : this.readingsAt
    return {
      failure: this.failure,
      receipt: this.receipt,
      receipts: this.receipt ? 1 : 0,
      results: this.results,
      readings: this.readings,
      disconnectObservedAt: this.receiptAt,
      closedAfterMarkMs: mark === null ? null : this.now() - mark,
    }
  }
}

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
    'warmup:matched',
    'warmup:closed',
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
   *   inadmissible control or the host stops the numbers deciding. Seeded controls always fail
   *   eligibility after any structurally valid measurements have been judged.
   */
  evaluate() {
    this.envelope()
    if (this.record.schema === SCHEMA && this.readings) this.measurements()
    else if (this.record.schema === SCHEMA) this.fail('readings are missing')
    if (this.readings?.manifest?.seed != null)
      this.fail('seeded control is not acceptance evidence')
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
    if (this.record.diagnostic === true || this.record.acceptanceEligible === false)
      this.fail('diagnostic records are ineligible for acceptance')
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
    if (exit?.failure !== null) this.fail('child output or spawn failed')
    if (exit?.results !== 1) this.fail('child sent the wrong number of readings messages')
    if (!time(exit?.closedAfterMarkMs) || !(exit.closedAfterMarkMs <= LIMITS.closeAfterMarkMs))
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
    this.imports()
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
      this.collectionHistory(name, reading)
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
   * Replays every collection pair and convergence, without trusting a declared settled flag.
   * @param {string} name checkpoint name @param {object} reading the final reading and GC journal
   * @returns {void} records incomplete facts or an unconverged measurement
   */
  collectionHistory(name, reading) {
    const gc = reading.gc
    const points = gc?.checkpoints
    if (!Array.isArray(points) || points.length < 2 || points.length > SETTLE.rounds) {
      this.fail(`${name} collection checkpoints are incomplete`)
      return
    }
    let previous = gc.admittedAt
    let observed = 0
    for (const [index, point] of points.entries()) {
      if (!this.checkReading(`${name} collection`, point, false)) return
      if (
        !time(point.admittedAt) ||
        point.admittedAt < previous ||
        point.at - point.admittedAt < LIMITS.gcPauseMs ||
        !count(point.observed) ||
        !count(point.engine?.bytecode) ||
        !count(point.engine?.code)
      )
        this.fail(`${name} collection checkpoint facts are incomplete`)
      if (point.observed < LIMITS.minCollections) this.wait(`${name} collections were not observed`)
      if (index > 0 && index < points.length - 1 && collectionsSettled(points[index - 1], point))
        this.fail(`${name} collection continued after convergence`)
      observed += point.observed
      previous = point.at
    }
    if (observed !== gc.observed) this.fail(`${name} collection totals differ from its checkpoints`)
    const last = points.at(-1)
    if (last.at !== reading.at || READING_FIELDS.some((field) => last[field] !== reading[field]))
      this.fail(`${name} final reading differs from its collection checkpoint`)
    if (!collectionsSettled(points.at(-2), last)) this.wait(`${name} collections did not settle`)
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
    if (!r.warmup || typeof r.warmup !== 'object') this.fail('warm-up readings are missing')
    if (this.failures.length > before) return false

    this.childContext(manifest)
    if (manifest.kind !== this.record.kind || manifest.arm !== this.record.arm)
      this.fail('manifest is for another kind or arm')
    if (Object.values(manifest.instrumented ?? { missing: true }).some(Boolean))
      this.fail('child ran under an inspector, coverage or Jest')
    if (!manifest.execArgv?.includes('--expose-gc')) this.fail('child did not expose GC')
    if (!MEMORY_FLAGS.slice(1).every((flag) => manifest.execArgv?.includes(flag)))
      this.fail('child did not declare eager JavaScript compilation')
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
      ['warm-up matched', r.warmup.m0],
      ['warm-up closed', r.warmup.end],
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
   * @returns {void} rejects missing module facts and keeps late imports inconclusive; their heap
   *   cost cannot be attributed to telemetry or removed from an observed limit violation
   */
  imports() {
    const { marks } = this.readings
    const loaded = marks.find((mark) => mark.phase === 'loaded')?.imports
    if (!count(loaded?.count) || loaded.count === 0 || !same(loaded.late, [])) {
      this.fail('loaded module population is incomplete')
      return
    }
    for (const mark of marks.slice(2)) {
      const imports = mark.imports
      if (
        !count(imports?.count) ||
        !Array.isArray(imports.late) ||
        imports.late.some((file) => typeof file !== 'string' || file.length === 0) ||
        new Set(imports.late).size !== imports.late.length
      ) {
        this.fail('module population facts are incomplete')
        continue
      }
      if (imports.count !== loaded.count || imports.late.length)
        this.wait('modules loaded after the disabled reference')
    }
  }

  /**
   * Binds the child's declared run, build and platform to the process the parent spawned and to
   * the record's stamp, so one child's evidence cannot stand in for another's.
   * @param {object} manifest the child's manifest
   * @returns {void}
   */
  childContext(manifest) {
    const { stamp, exit } = this.record
    const declared = (key) =>
      key === 'pid'
        ? Number.isSafeInteger(manifest[key]) && manifest[key] > 0
        : typeof manifest[key] === 'string' && manifest[key].length > 0
    if (!CHILD_CONTEXT.every(declared)) this.fail('child context declarations are incomplete')
    const producer = exit?.producer
    if (
      !producer ||
      producer.arm !== this.record.arm ||
      producer.kind !== this.record.kind ||
      !CHILD_CONTEXT.every((key) => manifest[key] === producer[key]) ||
      !STAMPED_CONTEXT.every((key) => manifest[key] === stamp?.[key])
    )
      this.fail('child context differs from the producing process or stamp')
  }

  /**
   * @returns {object[]} every recorded current reading: the settled checkpoints and the reading
   *   at each native submission and settlement
   */
  allReadings() {
    const r = this.readings
    const edges = Array.isArray(r.native?.rows)
      ? r.native.rows.flatMap((row) => [row?.submitted, row?.settled])
      : []
    const checkpoints = [
      r.c0,
      r.c1,
      r.released,
      r.warmup.m0,
      r.warmup.end,
      r.matched,
      ...r.actives,
      r.closed,
    ]
    return [
      ...checkpoints.flatMap((reading) => reading.gc?.checkpoints ?? []),
      r.startup,
      r.loaded,
      r.c0,
      r.c1,
      r.released,
      r.warmup.m0,
      r.warmup.end,
      r.matched,
      ...r.actives,
      r.closed,
      ...edges,
    ]
  }

  /**
   * Validates the marks and recomputes the peaks and baselines from them.
   * @returns {{B:number, Eref:number, P:number, Epeak:number, setupPeak:number,
   *   usable:boolean}|null} the quantities every gate uses, or null when the marks are unusable
   */
  peaks() {
    const { marks, c0, c1, manifest } = this.readings
    const readings = this.allReadings()
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
    const sampled = this.readings.samples?.peak
    const largest = (field) =>
      Math.max(0, sampled?.[field] ?? 0, ...readings.map((r) => r?.[field] ?? 0))
    return {
      B: Math.min(c0.rss, c1.rss),
      Eref: Math.min(c0.external, c1.external),
      P: Math.max(...marks.map(peakOf), largest('rss')),
      Epeak: Math.max(...marks.map((mark) => mark.coveredExternal), largest('external')),
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
      ['warmup:matched', r.warmup.m0],
      ['warmup:closed', r.warmup.end],
      [`${prefix}:matched`, r.matched],
      ...r.actives.map((reading) => [`${prefix}:${reading.phase}`, reading]),
      [`${prefix}:closed`, r.closed],
    ]
    for (const [phase, reading] of covers) {
      const mark = byPhase.get(phase)
      const readings = [reading, ...(reading.gc?.checkpoints ?? [])]
      if (
        readings.some((point) => mark.covered < point.rss || mark.coveredExternal < point.external)
      )
        this.fail('a peak mark understates a reading')
    }
    const covered = Math.max(...r.marks.map((mark) => mark.covered))
    const coveredExternal = Math.max(...r.marks.map((mark) => mark.coveredExternal))
    const samples = r.samples
    if (!samples || !count(samples.count) || !time(samples.maxGapMs) || !time(samples.peak?.rss))
      this.fail('sampling facts are incomplete')
    else if (samples.peak.rss > covered || samples.peak.external > coveredExternal)
      this.fail('a peak mark understates the sampled peak')

    let admitted =
      peaks.usable &&
      manifest.gc.available === true &&
      !['c0', 'c1'].some((name) => this.inconclusive.includes(`${name} collections did not settle`))
    if (Math.abs(r.c1.rss - r.c0.rss) > LIMITS.controlRss) {
      this.wait('control RSS is unstable')
      admitted = false
    }
    if (Math.abs(dataBytes(r.c1) - dataBytes(r.c0)) > LIMITS.controlRetained) {
      this.wait('control data bytes are unstable')
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
   * backstop, the active retained gate on raw bytes, and the first-use and closed transient gates
   * on data bytes (heap plus external without V8 code) against the matched baseline. Data below
   * the matched baseline by more than the transient ceiling cannot be read as a pass.
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
    for (const active of r.actives.flatMap((reading) => [reading, ...reading.gc.checkpoints]))
      if (active.retained - r.matched.retained > LIMITS.retained)
        this.fail('active retained bytes exceed the matched baseline by more than 8 MiB')
    if (dataBytes(r.matched) - dataBytes(r.warmup.m0) > LIMITS.firstUse)
      this.fail('warm-up first-use data bytes exceed 1 MiB')
    const closed = dataBytes(r.closed) - dataBytes(r.matched)
    if (closed > LIMITS.closedTransient) this.fail('closed transient data bytes exceed 256 KiB')
    else if (closed < -LIMITS.closedTransient)
      this.wait('closed data bytes are more than 256 KiB below the matched baseline')
  }

  /**
   * Judges the native job journal of both arms: bounded, ordered, with real current and retired
   * completions of each codec and distinct backing stores counted once.
   * @returns {void}
   */
  native() {
    const { native, samples, manifest } = this.readings
    if (manifest.sampleMs !== SAMPLE_MS || samples?.intervalMs !== SAMPLE_MS)
      this.fail('native sampling interval is not 20 ms')
    if (!native || !Array.isArray(native.rows)) {
      this.fail('native journal is missing')
      return
    }
    if (
      !Array.isArray(samples?.pendingWindows) ||
      samples.overflow !== false ||
      samples.windows !== samples.pendingWindows.length ||
      samples.windows !== native.rows.length
    )
      this.fail('native pending windows are incomplete')
    if (native.overflow !== false) this.fail('native journal overflowed')
    if (native.live !== 0) this.fail('native work is still pending')
    if (native.maxLive > 1) this.fail('more than one native job was occupied at once')
    const positive = (value) => Number.isSafeInteger(value) && value > 0
    let last = 0
    native.rows.forEach((row, index) => {
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
      const edges = [row?.submitted, row?.settled]
      if (!edges.every((edge) => edge && this.checkReading(name, edge, false))) {
        this.fail(`${name} lacks a reading at submission or settlement`)
        return
      }
      for (const edge of edges) this.covered(name, edge)
      if (row.submitted.at > row.settled.at) this.fail(`${name} settled before it was submitted`)
      this.pendingWindow(name, row, samples?.pendingWindows?.[index], samples?.peak)
    })
    for (const codec of ['gzip', ...(manifest.codecs?.zstd === true ? ['zstd'] : [])]) {
      const rows = native.rows.filter((row) => row.codec === codec && row.outcome === 'resolved')
      if (!rows.some((row) => row.retired === false) || !rows.some((row) => row.retired === true))
        this.fail(`${codec} native work lacks a current or a retired completion`)
    }
    if (samples && samples.count < 2 * native.rows.length)
      this.fail('too few current readings around native work')
  }

  /**
   * @param {string} name the job in a problem @param {object} edge a native submission or
   *   settlement reading
   * @returns {void} fails a reading outside the measured interval, or one the next mark misses
   */
  covered(name, edge) {
    const mark = this.readings.marks.find((candidate) => candidate.at >= edge.at)
    if (!mark || edge.at < this.readings.loaded.at)
      this.fail(`${name} is outside chronological peak coverage`)
    else if (mark.covered < edge.rss || mark.coveredExternal < edge.external)
      this.fail('a peak mark understates a native reading')
  }

  /**
   * @param {string} name the job in a problem @param {object} row its journal row
   * @param {object|undefined} window the sampler's window for it
   * @param {object|undefined} peak the sampled peak
   * @returns {void} fails a window that does not span the job from submission to settlement, holds
   *   fewer than its two edge readings, misses an edge, or exceeds the sampled peak
   */
  pendingWindow(name, row, window, peak) {
    const { submitted, settled } = row
    if (
      !window ||
      window.begin !== submitted.at ||
      window.end !== settled.at ||
      !count(window.count) ||
      window.count < 2 ||
      !count(window.rss) ||
      !count(window.external) ||
      window.rss < Math.max(submitted.rss, settled.rss) ||
      window.external < Math.max(submitted.external, settled.external)
    )
      this.fail(`${name} pending window does not cover its native work`)
    else if (!(window.rss <= peak?.rss) || !(window.external <= peak?.external))
      this.fail('sampled peak understates a pending window')
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
    const catalogue = registryCatalogue()
    const retained = { ...registryFingerprint(catalogue, r.uuid), ...REGISTRY_PROBE }
    if (
      !same(r.registry, catalogue) ||
      ![r.matched, ...r.actives, r.closed].every((reading) => same(reading.registry, retained))
    )
      this.fail('retained registry fingerprint differs from successful public registrations')
    const receipt = this.record.exit?.receipt
    if (
      receipt?.kind !== this.record.kind ||
      receipt.childId !== r.manifest.childId ||
      receipt.uuid !== r.uuid ||
      receipt.at !== r.disconnect?.[1]?.at ||
      !time(this.record.exit.disconnectObservedAt) ||
      !immediateBoundary(r.disconnect, 'receipt')
    )
      this.fail('disconnect receipt is not at the public boundary')
    if (!(r.enabled?.api71 >= 1) || !(r.enabled?.api72 >= 1))
      this.fail('enabled client made no telemetry request')
    for (const codec of codecs) {
      const first = r.native?.rows?.find((row) => row.codec === codec && row.retired === false)
      const pushes = r.pushes?.[codec]
      const decoded = r.decoded?.find((entry) => entry.codec === codec)
      if (!(pushes?.accepted >= LIMITS.acceptedPushes))
        this.fail(`${codec} accepted pushes are fewer than required`)
      if (
        !r.reopens?.some(
          (reopen) => reopen.codec === codec && immediateBoundary(reopen.events, 'connect')
        )
      )
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
            record.stamp?.tree === tree && record.stamp?.node === node && childNode === node
          verdict = judgeMemoryArm(record)
          const child = record.readings?.manifest?.childId
          if (typeof child === 'string' && child.length > 0)
            children.set(child, [...(children.get(child) ?? []), results.length])
          else
            verdict = {
              verdict: 'fail',
              problems: ['artifact has no child identity', ...verdict.problems],
            }
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
  MemoryOutput,
  NOISE,
  OPERATION,
  OPS_PER_LIFECYCLE,
  PEAK_SOURCE,
  RECORD_BYTES,
  REGISTRY_PROBE,
  SCHEMA,
  foregroundCatalogue,
  immediateBoundary,
  judgeMemoryArm,
  maximumDescriptor,
  phaseCatalogue,
  registryCatalogue,
  registryEntry,
  registryFingerprint,
  validateResourceArtifacts,
}

if (require.main === module) process.exitCode = main(process.argv.slice(2))
