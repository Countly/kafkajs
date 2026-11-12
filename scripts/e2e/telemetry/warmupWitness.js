#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const path = require('node:path')
const crypto = require('node:crypto')
const { types } = require('node:util')
const { spawn, execFileSync } = require('node:child_process')
const { MEMORY_FLAGS, SETTLE } = require('../../../testHelpers/processMemory')
const { phaseCatalogue, foregroundCatalogue } = require('./resourceArtifact')

const ROOT = path.resolve(__dirname, '../../..')
const POINTS = Object.freeze(['m0', 'warmupEnd', 'matched', 'firstActive', 'closed'])
const MODES = Object.freeze(['unseeded', 'close', 'close-warmup', 'warmup-only'])
const KINDS = Object.freeze(['producer', 'consumer', 'admin', 'shareConsumer'])
const object = (value) => value !== null && ['object', 'function'].includes(typeof value)

/**
 * Visits strong, inspectable own edges without invoking getters or traversing prototypes.
 * Private fields, weak collections and closure environments are opaque; no heap ownership claim
 * is inferred from this inventory. Typed views include their backing store, never each byte.
 * @param {object} value source object @returns {Array<[string, object]>} strong child edges
 */
const edges = (value) => {
  const children = []
  if (ArrayBuffer.isView(value)) return [['buffer', value.buffer]]
  // Brand checks: a Map/Set subclass prototype passes instanceof but is not a collection.
  if (types.isMap(value)) {
    let index = 0
    for (const [key, item] of Map.prototype.entries.call(value)) {
      children.push([`key${index}`, key], [`value${index++}`, item])
    }
  } else if (types.isSet(value)) {
    let index = 0
    for (const item of Set.prototype.values.call(value)) children.push([`item${index++}`, item])
  }
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key)
    if ('value' in descriptor) children.push([String(key), descriptor.value])
  }
  return children.filter(([, child]) => object(child))
}

/** Weak-only warm-up inventory with prefilled history; no target leaves a synchronous call. */
class WarmupWitnesses {
  /**
   * @param {object} [options] bounded storage and replaceable weak references for static controls
   * @param {number} [options.capacity] maximum issued identities, overflow is fatal
   * @param {Function} [options.weak] creates a native WeakRef in real children
   * @returns {WarmupWitnesses} empty census; all history storage exists before control/M0
   */
  constructor({ capacity = 65536, weak = (target) => new WeakRef(target) } = {}) {
    this.capacity = capacity
    this.weak = weak
    this.rows = Array.from({ length: capacity }, () => null)
    this.history = new Uint8Array(capacity * POINTS.length).fill(2)
    this.ids = new WeakMap()
    this.excluded = new WeakSet()
    this.count = 0
    this.points = []
    this.restores = []
    this.factories = {}
  }

  /**
   * Excludes pre-existing shared objects and their own strong descendants before warm-up starts.
   * @param {...object} roots module exports, meter and roles shared by both passes
   * @returns {void}
   */
  exclude(...roots) {
    const queue = roots.filter(object)
    for (let index = 0; index < queue.length; index++) {
      const value = queue[index]
      if (this.excluded.has(value)) continue
      this.excluded.add(value)
      for (const [, child] of edges(value)) queue.push(child)
    }
  }

  /**
   * Adds every inspectable object of a warm-up root, retaining only WeakRefs and scalar labels.
   * Re-visits existing roots to discover fields created by later foreground/catalogue work.
   * @param {string} label fixture-owned root/path @param {object} target warm-up value
   * @returns {void} @throws {Error} on overflow or registration after warm-up-end
   */
  track(label, target) {
    if (this.points.length >= 2) throw new Error('witness registration after warm-up')
    const seen = new WeakSet()
    const queue = [[label, target]]
    for (let index = 0; index < queue.length; index++) {
      const [name, value] = queue[index]
      if (!object(value) || this.excluded.has(value) || seen.has(value)) continue
      seen.add(value)
      if (!this.ids.has(value)) {
        if (this.count >= this.capacity) throw new Error('warm-up witness capacity exhausted')
        const id = ++this.count
        this.ids.set(value, id)
        this.rows[id - 1] = {
          id,
          label: name,
          issuedAfter: this.points.length,
          weak: this.weak(value),
        }
      }
      for (const [key, child] of edges(value)) queue.push([`${name}.${key}`.slice(0, 256), child])
    }
  }

  /**
   * Observes receiver/input/result objects at original product factory boundaries during warm-up.
   * A promise result is observed as a promise; no continuation, await or collection is added.
   * @param {object} prototype shared production prototype @param {string[]} methods original methods
   * @returns {void} @throws {Error} for a missing method or witness overflow
   */
  observe(prototype, methods) {
    for (const method of methods) {
      const original = prototype[method]
      if (typeof original !== 'function') throw new Error(`missing witness factory ${method}`)
      const census = this
      prototype[method] = function (...args) {
        census.factories[method] = (census.factories[method] ?? 0) + 1
        census.track(`factory:${method}:receiver`, this)
        for (const input of args) census.track(`factory:${method}:input`, input)
        const result = original.apply(this, args)
        census.track(`factory:${method}:result`, result)
        return result
      }
      this.restores.push(() => {
        prototype[method] = original
      })
    }
  }

  /** @returns {void} restores only shared factory methods, retaining no observed instance */
  restore() {
    for (const restore of this.restores.splice(0)) restore()
  }

  /**
   * Checks only after the existing settled final reading; deref can keep a target alive until
   * this job ends. The next original settle still has its unchanged two-GC/100 ms pairs.
   * @param {string} phase fixed point @param {object} reading unchanged final settled reading
   * @returns {void} @throws {Error} on missing/repeated/out-of-order points
   */
  check(phase, reading) {
    const point = this.points.length
    if (POINTS[point] !== phase) throw new Error('witness phase is missing or out of order')
    for (let index = 0; index < this.count; index++) {
      const row = this.rows[index]
      const alive = row.weak.deref() !== undefined
      this.history[index * POINTS.length + point] = alive ? 1 : 0
    }
    this.points.push({ phase, at: reading.at, issued: this.count })
  }

  /** @returns {object} scalar history materialised only after the final original reading */
  summary() {
    return {
      diagnostic: true,
      acceptanceEligible: false,
      attributionComplete: false,
      coverage:
        'fixture roots and recursive own data/map/set/backing edges; no getters or prototypes',
      opaque: ['private fields', 'closure environments', 'weak collection entries'],
      capacity: this.capacity,
      historyBytes: this.history.byteLength,
      factories: this.factories,
      points: this.points,
      objects: this.rows.slice(0, this.count).map(({ id, label, issuedAfter }, index) => ({
        id,
        label,
        issuedAfter,
        alive: Array.from(
          this.history.subarray(index * POINTS.length, (index + 1) * POINTS.length),
          (value) => (value === 2 ? null : value === 1)
        ),
      })),
    }
  }
}

/** @param {boolean} condition required evidence fact @param {string} message error @returns {void} @throws {Error} on missing/corrupt evidence */
const requireFact = (condition, message) => {
  if (!condition) throw new Error(message)
}

/**
 * Recomputes every original collection pair, checking exact endpoint/code joins.
 * @param {object} reading original settled record @returns {object} exact scalars and every pair
 * @throws {Error} for incomplete or nonfinite evidence; convergence failures remain evidence
 */
const scalarPoint = (reading) => {
  const checkpoints = reading?.gc?.checkpoints
  requireFact(
    Array.isArray(checkpoints) && checkpoints.length >= 2 && checkpoints.length <= SETTLE.rounds,
    'missing collection pairs'
  )
  let previousAt = reading.gc.admittedAt
  const rows = checkpoints.map((row) => {
    for (const value of [
      row.at,
      row.admittedAt,
      row.observed,
      row.rss,
      row.heapUsed,
      row.external,
      row.arrayBuffers,
      row.retained,
      row.engine?.code,
      row.engine?.bytecode,
    ])
      requireFact(Number.isFinite(value) && value >= 0, 'nonfinite collection scalar')
    requireFact(
      row.at >= row.admittedAt && row.admittedAt >= previousAt && row.observed >= 2,
      'collection order or observation mismatch'
    )
    requireFact(row.retained === row.heapUsed + row.external, 'raw retained mismatch')
    previousAt = row.at
    const { code, bytecode } = row.engine
    return { ...row, code, bytecode, data: row.retained - code - bytecode }
  })
  const last = rows.at(-1)
  for (const key of ['at', 'rss', 'heapUsed', 'external', 'arrayBuffers', 'retained'])
    requireFact(reading[key] === last[key], 'final checkpoint mismatch')
  requireFact(
    reading.gc.completedAt === last.at &&
      reading.gc.observed === checkpoints.reduce((count, row) => count + row.observed, 0),
    'collection completion mismatch'
  )
  return { ...last, checkpoints: rows, gc: reading.gc }
}

/** @param {object} readings original child output @returns {object} fixed five-point mapping */
const lifecycle = (readings) => ({
  m0: readings.warmup?.m0,
  warmupEnd: readings.warmup?.end,
  matched: readings.matched,
  firstActive: readings.actives?.[0],
  closed: readings.closed,
})

/**
 * Answers temporal coincidence only; never subtracts a pair, changes a verdict or infers cause.
 * @param {{scalar: object, witness: object}} pair independently exited original children
 * @returns {object} diagnostic analysis plus exact source readings for the five points
 * @throws {Error} on missing/mismatched producer, phase or witness evidence
 */
const analysePair = ({ scalar, witness }) => {
  const analyses = {}
  for (const run of [scalar, witness]) {
    requireFact(
      run?.code === 0 &&
        run.signal === null &&
        !run.timedOut &&
        !run.failure &&
        run.stderrBytes === 0,
      'child failed'
    )
    const { identity, readings } = run
    const manifest = readings.manifest
    requireFact(
      manifest.pid === run.pid && Number.isSafeInteger(run.pid) && run.pid > 0,
      'producer pid mismatch'
    )
    requireFact(
      manifest.codecs?.gzip === true && typeof manifest.codecs.zstd === 'boolean',
      'codec catalogue missing'
    )
    const zstd = manifest.codecs.zstd
    requireFact(
      JSON.stringify(manifest.phases) === JSON.stringify(phaseCatalogue(identity.arm, zstd)),
      'phase catalogue mismatch'
    )
    requireFact(
      JSON.stringify(manifest.foreground) ===
        JSON.stringify(foregroundCatalogue(identity.kind, zstd ? 5 : 3)),
      'foreground catalogue mismatch'
    )
    const fixture =
      identity.arm === 'public'
        ? { descriptors: 128, descriptorBytes: 128 * 2768 }
        : { nodes: 64, points: 257 }
    for (const [field, value] of Object.entries(fixture))
      requireFact(manifest.fixture?.[field] === value, 'fixture catalogue mismatch')
    requireFact(readings.actives?.[0]?.phase === 'gzip', 'first active phase mismatch')
    for (const key of ['arm', 'kind', 'childId', 'tree', 'runId', 'node', 'v8', 'platform', 'arch'])
      requireFact(identity[key] === manifest[key], `producer ${key} mismatch`)
    requireFact(
      JSON.stringify(manifest.execArgv) === JSON.stringify(MEMORY_FLAGS),
      'original flags mismatch'
    )
    requireFact(
      manifest.gc?.available &&
        manifest.gc.native &&
        ['inspector', 'coverage', 'jest'].every((key) => manifest.instrumented?.[key] === false),
      'instrumented or missing GC'
    )
    requireFact((manifest.seed?.mode ?? null) === identity.seed, 'seed mismatch')
    requireFact(
      identity.seed ===
        (identity.arm === 'public' && identity.mode !== 'unseeded' ? identity.mode : null),
      'collector or public seed mismatch'
    )
    const source = lifecycle(readings)
    const points = Object.fromEntries(POINTS.map((point) => [point, scalarPoint(source[point])]))
    for (let index = 1; index < POINTS.length; index++)
      requireFact(
        points[POINTS[index]].gc.admittedAt >= points[POINTS[index - 1]].at,
        'lifecycle order mismatch'
      )
    analyses[identity.variant] = {
      points,
      closedDataDelta: points.closed.data - points.matched.data,
      firstUseDataDelta: points.matched.data - points.m0.data,
    }
  }
  requireFact(
    scalar.identity.variant === 'scalar' &&
      witness.identity.variant === 'witness' &&
      scalar.identity.childId !== witness.identity.childId,
    'pair variant mismatch'
  )
  for (const key of [
    'arm',
    'kind',
    'mode',
    'seed',
    'tree',
    'node',
    'v8',
    'runId',
    'platform',
    'arch',
  ])
    requireFact(scalar.identity[key] === witness.identity[key], `pair ${key} mismatch`)
  const census = witness.readings.warmupWitnesses
  requireFact(
    census?.acceptanceEligible === false &&
      census.points.length === POINTS.length &&
      census.objects.length > 0 &&
      census.objects.length <= census.capacity &&
      census.historyBytes === census.capacity * POINTS.length,
    'missing witness history'
  )
  census.points.forEach((point, index) => {
    requireFact(
      point.phase === POINTS[index] &&
        point.at === analyses.witness.points[point.phase].at &&
        point.issued === census.objects.filter((row) => row.issuedAfter <= index).length,
      'witness phase/time mismatch'
    )
  })
  const aliveAtM = []
  const survivedPastM = []
  const diedAfterM = []
  const stillAliveAtClosed = []
  census.objects.forEach((row, index) => {
    requireFact(
      row.id === index + 1 &&
        typeof row.label === 'string' &&
        Number.isInteger(row.issuedAfter) &&
        row.issuedAfter >= 0 &&
        row.issuedAfter <= 1 &&
        row.alive.length === POINTS.length,
      'witness identity mismatch'
    )
    let dead = false
    row.alive.forEach((alive, point) => {
      requireFact(
        point < row.issuedAfter ? alive === null : typeof alive === 'boolean' && (!dead || !alive),
        'witness resurrected or unissued'
      )
      if (alive === false) dead = true
    })
    if (row.alive[2]) aliveAtM.push(row.id)
    if (row.alive[3] || row.alive[4]) survivedPastM.push(row.id)
    if (row.alive[4]) stillAliveAtClosed.push(row.id)
    if (row.alive[2] && !row.alive[4]) {
      const end = row.alive[3] ? 4 : 3
      const from = POINTS[end - 1]
      const to = POINTS[end]
      diedAfterM.push({
        id: row.id,
        from,
        to,
        dataDelta: analyses.witness.points[to].data - analyses.witness.points[from].data,
      })
    }
  })
  return {
    diagnostic: true,
    acceptanceEligible: false,
    attributionComplete: false,
    ...analyses,
    answer: {
      aliveAtM,
      survivedPastM,
      diedAfterM,
      stillAliveAtClosed,
      closedDataDelta: analyses.witness.closedDataDelta,
      scalarClosedDataDelta: analyses.scalar.closedDataDelta,
      closedNegativeCoincidesWithPostMDeath:
        analyses.witness.closedDataDelta < 0 && diedAfterM.some((death) => death.dataDelta < 0),
      causationEstablished: false,
    },
  }
}

/**
 * Runs one original child with an inclusive watchdog and bounded output; close joins both pipes.
 * Only the no-flush-bytecode diagnostic variant adds a V8 flag; scalar/witness flags are unchanged.
 * @param {object} identity parent-issued tuple @param {Function} [launch] replaceable spawn
 * @returns {Promise<object>} diagnostic record, including failures for archival before analysis
 */
const supervise = (identity, launch = spawn) =>
  new Promise((resolve) => {
    const env = { ...process.env }
    for (const name of ['NODE_OPTIONS', 'NODE_V8_COVERAGE', 'RESOURCE_SEED', 'RESOURCE_WITNESSES'])
      delete env[name]
    const seed = identity.arm === 'public' && identity.mode !== 'unseeded' ? identity.mode : null
    if (seed) env.RESOURCE_SEED = seed
    if (identity.variant === 'witness') env.RESOURCE_WITNESSES = '1'
    Object.assign(env, {
      RESOURCE_CHILD_ID: identity.childId,
      RESOURCE_TREE: identity.tree,
      RESOURCE_RUN_ID: identity.runId,
    })
    const script = identity.arm === 'public' ? 'publicMemoryChild.js' : 'resourceChild.js'
    const flags =
      identity.variant === 'no-flush-bytecode'
        ? [...MEMORY_FLAGS, '--no-flush-bytecode']
        : MEMORY_FLAGS
    const child = launch(
      process.execPath,
      [...flags, path.join(ROOT, 'src/telemetry/__tests__/fixtures', script), identity.kind],
      { env, stdio: ['ignore', 'pipe', 'pipe'] }
    )
    let stdout = ''
    let stderr = ''
    let stdoutBytes = 0
    let stderrBytes = 0
    let failure = null
    let timedOut = false
    const timer = setTimeout(() => {
      timedOut = true
      child.kill('SIGKILL')
    }, 90000)
    child.stdout.on('data', (chunk) => {
      stdoutBytes += chunk.length
      if (stdoutBytes > 32 * 1024 * 1024) {
        failure = 'stdout capacity exhausted'
        child.kill('SIGKILL')
      } else stdout += chunk
    })
    child.stderr.on('data', (chunk) => {
      stderrBytes += chunk.length
      if (stderrBytes <= 65536) stderr += chunk
      if (stderrBytes > 65536) {
        failure = 'stderr capacity exhausted'
        child.kill('SIGKILL')
      }
    })
    child.on('error', (error) => {
      failure = error.message
    })
    child.on('close', (code, signal) => {
      clearTimeout(timer)
      let readings = null
      let receipt = null
      try {
        const rows = stdout
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line))
        const results = rows.filter((row) => row.readings)
        requireFact(
          results.length === 1 && rows.length === (identity.arm === 'public' ? 2 : 1),
          'missing/duplicate child output'
        )
        readings = results[0].readings
        receipt = rows.find((row) => row.receipt === 'disconnect') ?? null
        if (identity.arm === 'public')
          requireFact(receipt?.childId === identity.childId, 'missing disconnect receipt')
      } catch (error) {
        failure = failure ?? error.message
      }
      resolve({
        diagnostic: true,
        acceptanceEligible: false,
        identity: { ...identity, seed },
        code,
        signal,
        pid: child.pid,
        timedOut,
        failure,
        stdoutBytes,
        stderrBytes,
        stderr,
        ...(failure ? { rawStdout: stdout } : {}),
        readings,
        receipt,
      })
    })
  })

/**
 * Runs 64 serial children (four modes × four kinds × two arms × scalar/witness), no retries.
 * Collector contrasts remain unseeded for every mode; only the public fixture owns seed closes.
 * @param {string[]} args explicit diagnostic-only switch and a fresh evidence directory
 * @returns {Promise<void>} @throws {Error} after archiving all pairs if any evidence is incomplete
 */
const main = async (args) => {
  requireFact(
    args.length === 2 && args[0] === '--diagnostic-only',
    'usage: warmupWitness.js --diagnostic-only <new directory>'
  )
  const directory = path.resolve(args[1])
  fs.mkdirSync(directory)
  const tree = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: ROOT, encoding: 'utf8' }).trim()
  const sources = require('./retainedDiagnostic').archiveSources(directory)
  const summary = {
    schema: 1,
    diagnostic: true,
    acceptanceEligible: false,
    tree,
    node: process.version,
    flags: MEMORY_FLAGS,
    settlement: SETTLE,
    schedule: { modes: MODES, kinds: KINDS, children: 64, childTimeoutMs: 90000, retries: 0 },
    sources,
    pairs: [],
  }
  const write = (file, value) =>
    fs.writeFileSync(path.join(directory, file), JSON.stringify(value, null, 2) + '\n')
  write('summary.json', summary)
  for (const mode of MODES)
    for (const kind of KINDS)
      for (const arm of ['public', 'collector']) {
        const id = `${mode}-${kind}-${arm}`
        const pair = {}
        for (const variant of ['scalar', 'witness']) {
          const identity = {
            mode,
            kind,
            arm,
            variant,
            tree,
            childId: crypto.randomUUID(),
            runId: process.env.GITHUB_RUN_ID || 'diagnostic',
            node: process.version,
            v8: process.versions.v8,
            platform: process.platform,
            arch: process.arch,
          }
          pair[variant] = await supervise(identity)
          write(`${id}-${variant}.json`, pair[variant])
        }
        const record = {
          id,
          mode,
          kind,
          arm,
          seed: pair.scalar.identity.seed,
          collectorUnseededContrast: arm === 'collector',
          scalarFile: `${id}-scalar.json`,
          witnessFile: `${id}-witness.json`,
        }
        try {
          record.analysis = analysePair(pair)
        } catch (error) {
          record.failure = error.message
        }
        summary.pairs.push(record)
        write('summary.json', summary)
        process.stdout.write(`${id}: ${record.failure || 'recorded (diagnostic only)'}\n`)
      }
  requireFact(
    summary.pairs.every((pair) => !pair.failure),
    'incomplete warm-up witness evidence; see summary.json'
  )
}

if (require.main === module)
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  })

module.exports = { WarmupWitnesses, analysePair, supervise, main }
