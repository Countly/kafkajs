'use strict'

const fs = require('node:fs')
const crypto = require('node:crypto')
const { collectionsSettled } = require('../../../../testHelpers/processMemory')
const { ARMS, KINDS, foregroundCatalogue } = require('../resourceArtifact')

/** Diagnostic-only bounds, declared before any child starts; acceptance capacities stay unchanged. */
const CAPACITY = Object.freeze({
  events: 2048,
  eventBytes: 33554432,
  native: 512,
  windows: 512,
  operations: 90,
  weakTargets: 131072,
  statusBytes: 16384,
  snapshotBytes: 536870912,
})
const BUDGET = Object.freeze({ child: 90000, case: 120000, reserve: 10000, receipt: 10000 })
const CAPTURES = Object.freeze([
  'C0',
  'C1',
  'disabled-released',
  'M',
  'closed-0',
  'closed-5',
  'all-released',
])
const METHOD = 'retained-cold-ownership-v1'
const CATALOGUE = Object.freeze(
  ARMS.flatMap((arm) =>
    KINDS.flatMap((kind) => ['capture', 'scalar'].map((variant) => ({ arm, kind, variant })))
  )
)

/** @param {Buffer|string} bytes source bytes @returns {string} SHA-256 */
const digest = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')

/** Disk transport holds scalar JSON only; storage is reserved before C0 and never grows. */
class DiagnosticLog {
  /** @param {string} file new event sidecar @returns {DiagnosticLog} bounded recorder */
  constructor(file) {
    this.file = file
    this.buffer = Buffer.alloc(CAPACITY.eventBytes)
    this.count = 0
    this.bytes = 0
    fs.writeFileSync(file, '', { flag: 'wx' })
  }

  /** @param {string} type event name @param {object} facts serializable proof @returns {void} @throws {Error} on overflow or serialization failure */
  write(type, facts) {
    const json = JSON.stringify({ sequence: this.count, type, ...facts }) + '\n'
    const length = Buffer.byteLength(json)
    if (this.count >= CAPACITY.events || this.bytes + length > CAPACITY.eventBytes)
      throw new Error('diagnostic transport overflow')
    this.buffer.write(json, 0, length)
    fs.appendFileSync(this.file, this.buffer.subarray(0, length))
    this.buffer.fill(0, 0, length)
    this.bytes += length
    this.count++
  }
}

/**
 * Checks independent disk replay, rather than trusting a child's completion boolean.
 * @param {object[]} events parsed sidecar @param {object} expected parent-issued identity
 * @param {object} exit joined process facts @returns {string[]} incomplete or broken facts
 */
const validateEvents = (events, expected, exit) => {
  const problems = []
  const fail = (condition, message) => {
    if (!condition) problems.push(message)
  }
  const manifest = events[0]?.manifest
  fail(
    events[0]?.type === 'manifest' &&
      manifest?.schema === 1 &&
      manifest?.method === METHOD &&
      manifest?.diagnostic === true &&
      manifest?.acceptanceEligible === false,
    'diagnostic envelope'
  )
  for (const key of ['childId', 'arm', 'kind', 'variant', 'tree', 'node', 'v8', 'runId'])
    fail(manifest?.[key] === expected[key], `foreign ${key}`)
  fail(
    manifest?.gc?.available === true && manifest?.gc?.native === true,
    'real native GC capability'
  )
  fail(JSON.stringify(manifest?.capacity) === JSON.stringify(CAPACITY), 'capacity declaration')
  fail(JSON.stringify(manifest?.sources) === JSON.stringify(expected.sources), 'source digests')
  fail(
    manifest?.node?.startsWith('v24.') &&
      JSON.stringify(manifest?.flags) === JSON.stringify(expected.flags),
    'measurement tuple'
  )
  fail(
    events.length <= CAPACITY.events && events.every((event, index) => event.sequence === index),
    'event ordering/capacity'
  )
  fail(events.filter((event) => event.type === 'error').length === 0, 'workload or cleanup error')
  const checkReading = (reading) => {
    const valid = (point) =>
      ['heapUsed', 'external', 'arrayBuffers', 'retained', 'rss'].every(
        (key) => Number.isSafeInteger(point?.[key]) && point[key] >= 0
      ) &&
      Number.isFinite(point.at) &&
      point.retained === point.heapUsed + point.external
    if (!valid(reading)) return false
    const points = reading.gc?.checkpoints
    if (!Array.isArray(points) || points.length < 2 || points.length > 6 || !points.every(valid))
      return false
    if (!points.every((point) => point.observed >= 2 && point.at - point.admittedAt >= 100))
      return false
    if (points.slice(1, -1).some((point, index) => collectionsSettled(points[index], point)))
      return false
    return (
      ['at', 'heapUsed', 'external', 'arrayBuffers', 'retained', 'rss'].every(
        (field) => reading[field] === points.at(-1)[field]
      ) &&
      (points.length === 6 || collectionsSettled(points.at(-2), points.at(-1)))
    )
  }
  const marks = events.filter((event) => event.type === 'mark')
  fail(
    [
      'startup',
      'loaded',
      'C0',
      'C1',
      'disabled-released',
      'M',
      'closed:0',
      'closed:1',
      'closed:2',
      'closed:3',
      'closed:4',
      'closed:5',
      'cleanup',
    ].every((phase) => marks.some((event) => event.mark?.phase === phase)),
    'missing phase/source evidence'
  )
  for (const event of marks) {
    const raw = event.raw
    fail(
      raw &&
        typeof raw.status === 'string' &&
        Buffer.byteLength(raw.status) <= CAPACITY.statusBytes &&
        raw.before <= raw.after &&
        raw.current?.at <= raw.before &&
        Number.isSafeInteger(raw.pid) &&
        raw.pid > 0 &&
        typeof raw.processStart === 'string' &&
        typeof raw.kernel === 'string' &&
        typeof raw.os === 'string' &&
        typeof raw.arch === 'string' &&
        JSON.stringify(raw.flags) === JSON.stringify(expected.flags),
      'raw peak-source provenance'
    )
    for (const name of ['VmHWM', 'VmRSS']) {
      const match = new RegExp(`^${name}:\\s+(\\d+) kB$`, 'm').exec(raw?.status ?? '')
      fail(
        raw?.parsed?.[name] === (match ? Number(match[1]) * 1024 : null),
        'raw status parse mismatch'
      )
    }
  }
  const captures = events.filter((event) => event.type === 'checkpoint')
  fail(
    JSON.stringify(captures.map((event) => event.name)) === JSON.stringify(CAPTURES),
    'missing/reordered/duplicated captures'
  )
  let previous = -1
  for (const event of captures) {
    fail(checkReading(event.pre), 'invalid GC journal/arithmetic')
    fail(
      event.observedAt >= previous &&
        event.startedAt >= event.observedAt &&
        event.endedAt >= event.startedAt &&
        event.post?.at >= event.endedAt,
      'capture chronology'
    )
    previous = event.post?.at
    fail(
      expected.variant === 'capture'
        ? /^[a-f0-9]{64}$/.test(event.sha256) &&
            event.bytes > 0 &&
            event.bytes <= CAPACITY.snapshotBytes
        : event.file === null && event.bytes === 0 && event.sha256 === null,
      'capture shape'
    )
  }
  const blocks = events.filter((event) => event.type === 'block')
  fail(
    JSON.stringify(blocks.map((block) => block.index)) === '[0,1,2,3,4,5]',
    'missing/reordered/duplicated blocks'
  )
  for (const block of blocks) {
    fail(
      checkReading(block.closed) && block.actives?.length >= 2 && block.actives.every(checkReading),
      'block GC journals/completion'
    )
    fail(
      block.native?.rows?.length <= CAPACITY.native &&
        block.samples?.windows <= CAPACITY.windows &&
        block.samples?.pendingWindows?.length === block.samples?.windows,
      'native/window capacity'
    )
    for (const row of block.native?.rows ?? []) {
      const window = block.samples?.pendingWindows?.find(
        (window) => window.begin === row.submitted?.at && window.end === row.settled?.at
      )
      fail(
        row.outcome === 'resolved' &&
          row.inputBytes > 1000 &&
          row.resultBytes > 0 &&
          row.resultBytes < row.inputBytes &&
          row.inputBackingId > 0 &&
          row.resultBackingId > 0 &&
          row.submitted.at <= row.settled.at &&
          window?.count >= 2,
        'incomplete native job/edge window'
      )
      fail(
        row.distinctBackingBytes ===
          row.inputBackingBytes +
            (row.inputBackingId === row.resultBackingId ? 0 : row.resultBackingBytes),
        'aliased backing arithmetic'
      )
    }
    fail(
      block.closed?.retained === block.closed?.heapUsed + block.closed?.external &&
        block.delta === block.closed?.retained - block.matchedRetained,
      'original-M arithmetic'
    )
    fail(block.matchedRetained === blocks[0]?.matchedRetained, 'baseline changed')
    fail(
      block.native?.live === 0 &&
        block.native?.maxLive <= 1 &&
        block.native?.overflow === false &&
        block.samples?.overflow === false,
      'native completion/capacity'
    )
    for (const codec of ['gzip', 'zstd']) {
      const rows = block.native?.rows?.filter((row) => row.codec === codec) ?? []
      fail(
        rows.some((row) => row.outcome === 'resolved' && row.retired === true) &&
          rows.some((row) => row.outcome === 'resolved' && row.retired === false),
        `missing ${codec} actual work`
      )
    }
    if (expected.arm === 'public')
      fail(
        block.ops?.length === 15 &&
          block.ops.every(
            (op, index) =>
              op.ok === true &&
              op.op === foregroundCatalogue(expected.kind, 5).ops[index].op &&
              op.bytes === 256
          ) &&
          block.registry?.descriptors === 128 &&
          block.uuid === block.registry?.uuid &&
          block.decoded?.length === 2 &&
          block.decoded.every((row) => row.format === true),
        'public block population/completion'
      )
    else
      fail(
        block.builds?.length === 2 &&
          block.builds.every(
            (build) =>
              build.points === 257 && build.retiredCompletion === true && build.transmittedBytes > 0
          ) &&
          block.disconnectedPoints === 129,
        'collector block population/completion'
      )
  }
  const control = events.find((event) => event.type === 'control')
  fail(
    control?.facts?.enabled === false &&
      control.facts.api71 === 0 &&
      control.facts.api72 === 0 &&
      control.facts.nativeSubmissions === 0 &&
      control.facts.probe === 'disabled' &&
      JSON.stringify(control.facts.ops) ===
        JSON.stringify(foregroundCatalogue(expected.kind, 5).ops) &&
      control.facts.liveSockets === 0 &&
      control.facts.liveRequests === 0,
    'disabled control'
  )
  fail(
    control &&
      checkReading(control.c0) &&
      checkReading(control.c1) &&
      checkReading(control.released),
    'control GC journals'
  )
  const position = (type, name) =>
    events.findIndex((event) => event.type === type && (name === undefined || event.name === name))
  fail(
    events.filter((event) => event.type === 'manifest').length === 1 &&
      events.filter((event) => event.type === 'complete').length === 1,
    'duplicate manifest/completion'
  )
  fail(
    position('checkpoint', 'disabled-released') < position('control') &&
      position('control') < position('checkpoint', 'M') &&
      position('checkpoint', 'M') < position('checkpoint', 'closed-0') &&
      position('checkpoint', 'closed-0') < position('block') &&
      events.findLastIndex((event) => event.type === 'block') <
        position('checkpoint', 'all-released') &&
      position('checkpoint', 'all-released') < position('terminal'),
    'fixed schedule order'
  )
  fail(
    events.every((event) =>
      [
        'manifest',
        'mark',
        'checkpoint',
        'block',
        'control',
        'weak',
        'terminal',
        'complete',
        'error',
      ].includes(event.type)
    ),
    'unknown event'
  )
  const terminal = events.filter((event) => event.type === 'terminal')
  fail(
    terminal.length === 1 && events.at(-1)?.type === 'complete' && terminal[0]?.cleanup === true,
    'terminal cleanup/completion ordering'
  )
  fail(
    exit?.code === 0 &&
      exit?.signal === null &&
      exit?.timedOut === false &&
      exit?.failure === null &&
      exit?.stderrBytes === 0 &&
      exit?.stdoutBytes <= 65536 &&
      exit?.receipts === 1 &&
      exit?.results === 1 &&
      Number.isFinite(exit?.closedAfterReceiptMs) &&
      exit.closedAfterReceiptMs >= 0 &&
      exit.closedAfterReceiptMs <= BUDGET.receipt &&
      exit?.elapsedMs <= BUDGET.child,
    'natural child exit/receipt'
  )
  return problems
}

/** @param {object[]} results per-child replay results @returns {string[]} duplicate/foreign/missing child identities */
const validateCatalogue = (results) => {
  const problems = []
  const seen = new Set()
  for (const [index, tuple] of CATALOGUE.entries()) {
    const row = results[index]
    if (!row || ['arm', 'kind', 'variant'].some((field) => row[field] !== tuple[field]))
      problems.push('missing or foreign catalogue child')
    if (!row?.childId || seen.has(row.childId)) problems.push('duplicate or missing child identity')
    seen.add(row?.childId)
  }
  if (results.length !== CATALOGUE.length) problems.push('wrong child count')
  return problems
}

module.exports = {
  CAPACITY,
  BUDGET,
  CAPTURES,
  METHOD,
  CATALOGUE,
  DiagnosticLog,
  digest,
  validateEvents,
  validateCatalogue,
}
