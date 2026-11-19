'use strict'
/**
 * Final soak evidence collection: the collector (the sole stream writer), each child's bounded
 * source channel, the sampler that cuts an inventory before two forced collections, and the host
 * monitor that aborts a run on low memory or a short disk reservation.
 */
const fs = require('node:fs')
const { valueOf } = require('./finalSoakScheduler')

const SUPERVISOR = 'p15'
const OUTER = 'outer'
const MIB = 1024 * 1024
const ENQUEUE_TIMEOUT_MS = 5000
/** How long one sink write may take before the collection stops. */
const WRITE_TIMEOUT_MS = 60000
const EXPLICIT_RESOURCES = ['TCPSocketWrap', 'Timeout']
/** JSON bytes kept of a diagnostic's message, so the record always fits the event line cap. */
const DIAGNOSTIC_BYTES = 600

/**
 * Cuts free text to a JSON size: whole characters are kept while the quoted, escaped text stays
 * within `maxBytes`, and a cut text ends with how long it was.
 * @param {string} text @param {number} maxBytes the JSON-encoded size allowed, quotes included
 * @returns {string}
 */
const clip = (text, maxBytes) => {
  if (Buffer.byteLength(JSON.stringify(text)) <= maxBytes) return text
  const suffix = `… (${text.length} characters)`
  let bytes = Buffer.byteLength(JSON.stringify(suffix))
  let kept = ''
  for (const char of text) {
    bytes += Buffer.byteLength(JSON.stringify(char)) - 2
    if (bytes > maxBytes) break
    kept += char
  }
  return kept + suffix
}

/**
 * The sole routine stream writer. Records arrive with their envelope from each source; the
 * collector checks per-source sequence continuity, stamps the append time `t` (never decreasing),
 * enforces the line and stream caps and streams lines to the sink through a byte-bounded queue.
 * When the queue is full, appends wait in arrival order, so no source is reordered; a wait over five
 * seconds stops collection. An append is acknowledged only once all of its line's bytes were
 * written. Nothing is truncated or silently dropped: a line over its cap is replaced, in its place,
 * by a diagnostic naming it, and every failure is listed in `failures` and told to `onFailure` only
 * after the record that caused it has taken its place, so a record the failure handler writes comes
 * after it. A sink write that fails or does not settle in time stops collection and refuses every
 * record not yet written. State is fixed in size: one sequence counter per source and the bounded
 * queue; waiting appends are bounded by each source's own unacknowledged-byte cap.
 */
class Collector {
  /**
   * @param {object} options
   * @param {{ write: (b: Buffer) => Promise<unknown>, sync: () => Promise<void> }} options.sink
   * @param {{ now: () => number, sleepUntil: (at: number) => Promise<void> }} options.clock
   * @param {object} options.matrix the reviewed matrix, for the caps
   * @param {number} [options.maxQueueBytes] write queue bound, 32 MiB by default
   * @param {number} [options.maxStreamBytes] stream cap, the matrix's by default
   * @param {string} [options.source] the collector's own source slot, for its own records
   * @param {number} [options.writeTimeoutMs] how long one sink write may take
   */
  constructor({
    sink,
    clock,
    matrix,
    maxQueueBytes = 32 * MIB,
    maxStreamBytes,
    source = SUPERVISOR,
    writeTimeoutMs = WRITE_TIMEOUT_MS,
  }) {
    this.sink = sink
    this.writeTimeoutMs = writeTimeoutMs
    this.clock = clock
    this.maxQueueBytes = maxQueueBytes
    this.maxStreamBytes = maxStreamBytes ?? valueOf(matrix.host.maxStreamBytes)
    this.maxLine = valueOf(matrix.host.maxEventLineBytes)
    this.maxInventoryLine = valueOf(matrix.host.maxInventoryLineBytes)
    this.source = source
    this.queue = []
    this.queuedBytes = 0
    this.streamBytes = 0
    this.records = 0
    this.lastT = 0
    this.lastLine = null
    this.nextSeq = new Map()
    this.ownSeq = 0
    this.failures = []
    this.stopped = false
    this.writing = null
    this.waiters = []
    this.onFailure = null
  }

  /**
   * Appends one source record.
   * @param {object} record a record with its envelope except `t`
   * @returns {Promise<boolean>} whether all of the record's bytes were written
   */
  append(record) {
    if (this.stopped) return Promise.resolve(false)
    const expected = this.nextSeq.get(record.source) ?? 0
    const gap =
      record.seq === expected
        ? []
        : [`${record.source} sequence ${record.seq} follows ${expected - 1}`]
    this.nextSeq.set(record.source, record.seq + 1)
    return this.enqueue(record, gap)
  }

  /**
   * Appends a record of the collector's own source, giving it the next own sequence number.
   * @param {object} record fields without source and seq; `recordId` defaults to a fresh one
   * @returns {Promise<boolean>}
   */
  own(record) {
    const seq = this.ownSeq++
    return this.enqueue({
      recordId: `${this.source}.c${seq}`,
      source: this.source,
      seq,
      at: this.clock.now(),
      execution: null,
      ...record,
    })
  }

  /**
   * Appends a diagnostic from the collector itself; its message is clipped to fit the line cap.
   * @param {string} message @param {object} [extra] open diagnostic fields
   * @returns {Promise<boolean>}
   */
  note(message, extra = {}) {
    return this.own({ kind: 'diagnostic', message: clip(message, DIAGNOSTIC_BYTES), ...extra })
  }

  /**
   * Lists a failure of the collection and tells `onFailure` (the supervisor), which stops
   * admitting work.
   * @param {string} problem
   * @returns {void}
   */
  fail(problem) {
    if (this.failures.length < 100) this.failures.push(problem)
    this.onFailure?.(problem)
  }

  /** @param {object} record @returns {string} the record's line with the current append time */
  line(record) {
    this.lastT = Math.max(this.lastT, this.clock.now())
    return JSON.stringify({ ...record, t: this.lastT }) + '\n'
  }

  /**
   * Queues one record's line. A line over its cap fails the collection with the record's id, kind
   * and size, and a diagnostic with the record's own envelope takes its place, so its source's
   * sequence stays gap-free and the source keeps delivering (its final sample included). The
   * failures are told only once that place is taken.
   * @param {object} record @param {string[]} [problems] failures the record already caused
   * @returns {Promise<boolean>} whether all bytes of its place in the stream were written
   */
  async enqueue(record, problems = []) {
    let line = this.line(record)
    const cap = record.kind === 'inventory' ? this.maxInventoryLine : this.maxLine
    const size = Buffer.byteLength(line) - 1
    if (size > cap) {
      const { recordId, source, seq, at, execution, kind } = record
      problems = [...problems, `${recordId} ${kind} line of ${size} bytes exceeds ${cap} bytes`]
      const message = `oversize ${kind} record ${recordId} of ${size} bytes not written`
      record = { kind: 'diagnostic', recordId, source, seq, at, execution, message }
      line = this.line(record)
    }
    const told = (result) => {
      for (const problem of problems) this.fail(problem)
      return result
    }
    if (
      this.waiters.length > 0 ||
      this.queuedBytes + Buffer.byteLength(line) > this.maxQueueBytes
    ) {
      if (!(await this.turn(Buffer.byteLength(line)))) return told(false)
      line = this.line(record)
    }
    const bytes = Buffer.byteLength(line)
    if (this.stopped) return told(false)
    if (this.streamBytes + bytes > this.maxStreamBytes) {
      this.fail(`stream exceeds ${this.maxStreamBytes} bytes`)
      this.stop()
      return told(false)
    }
    this.streamBytes += bytes
    this.records++
    this.lastLine = line.slice(0, -1)
    const entry = { line, resolve: null }
    const written = new Promise((resolve) => (entry.resolve = resolve))
    this.queue.push(entry)
    this.queuedBytes += bytes
    this.writing ??= this.drain()
    this.release()
    told(true)
    return written
  }

  /** @param {number} bytes @returns {Promise<boolean>} waits in arrival order for queue space */
  turn(bytes) {
    return new Promise((resolve) => {
      const timer = new AbortController()
      const waiter = {
        bytes,
        resolve: (admitted) => {
          timer.abort()
          resolve(admitted)
        },
      }
      this.waiters.push(waiter)
      this.clock.sleepUntil(this.clock.now() + ENQUEUE_TIMEOUT_MS, timer.signal).then(() => {
        if (!this.waiters.includes(waiter)) return
        this.fail(`write queue did not drain within ${ENQUEUE_TIMEOUT_MS} ms`)
        this.stop()
      })
    })
  }

  /** Lets the first waiter in, when its line fits. */
  release() {
    const [first] = this.waiters
    if (first && this.queuedBytes + first.bytes <= this.maxQueueBytes) {
      this.waiters.shift()
      first.resolve(true)
    }
  }

  /** Stops collection; waiting appends are refused. */
  stop() {
    this.stopped = true
    for (const waiter of this.waiters.splice(0)) waiter.resolve(false)
  }

  /**
   * @param {Promise<unknown>} write one sink write or sync
   * @param {string} [what] what is named when it does not settle
   * @returns {Promise<unknown>} its result
   * @throws {Error} when it does not settle within `writeTimeoutMs`
   */
  async bounded(write, what = 'a write') {
    let timer = null
    try {
      return await Promise.race([
        write,
        new Promise((resolve, reject) => {
          timer = setTimeout(
            () => reject(new Error(`${what} did not settle within ${this.writeTimeoutMs} ms`)),
            this.writeTimeoutMs
          )
        }),
      ])
    } finally {
      clearTimeout(timer)
    }
  }

  /**
   * Writes queued lines until the queue is empty, and acknowledges each line once all of its bytes
   * were written. Every chunk is written whole: a short write is continued from where it stopped,
   * and a write that fails, takes no bytes or does not settle in time stops collection and refuses
   * every line not wholly written (the chunk in flight and what is queued behind it), never
   * truncating or reordering a line already written.
   * @returns {Promise<void>}
   */
  async drain() {
    let batch = []
    try {
      while (this.queue.length > 0) {
        batch = this.queue.splice(0, this.queue.length)
        const chunk = Buffer.from(batch.map((entry) => entry.line).join(''))
        for (let offset = 0; offset < chunk.length;) {
          const result = await this.bounded(this.sink.write(chunk.subarray(offset)))
          const bytes = result?.bytesWritten ?? chunk.length - offset
          if (!(bytes > 0)) throw new Error('the sink accepted no bytes')
          offset += bytes
        }
        this.queuedBytes -= chunk.length
        for (const entry of batch.splice(0)) entry.resolve(true)
        this.release()
      }
    } catch (e) {
      for (const entry of [...batch, ...this.queue.splice(0)]) entry.resolve(false)
      this.queuedBytes = 0
      this.fail(`sink write failed: ${e.message}`)
      this.stop()
    }
    this.writing = null
  }

  /**
   * Flushes and syncs the stream; later appends are refused. Every write and the sync are bounded
   * by `writeTimeoutMs`, so a sink that stops completing I/O cannot hold the close.
   * @returns {Promise<void>}
   * @throws {Error} when the sync fails or does not settle in time, recorded as a failure first
   */
  async close() {
    while (this.writing || this.waiters.length > 0)
      await (this.writing ?? new Promise(setImmediate))
    this.stopped = true
    try {
      await this.bounded(this.sink.sync(), 'the stream sync')
    } catch (e) {
      this.failures.push(e.message)
      throw e
    }
  }
}

/**
 * A child process's end of the stream: assigns each record its source, gap-free `seq` and event
 * time `at` when it is handed over, and sends it to the collector. At most `maxBytes` may await the
 * collector's acknowledgement; beyond that records wait in call order, and a wait over five seconds
 * marks the source incomplete and stops it. Evidence is never silently dropped or reordered, and
 * memory never grows with the run.
 */
class SourceChannel {
  /**
   * @param {object} options
   * @param {string} options.source the process slot
   * @param {{ now: () => number, sleepUntil: (at: number) => Promise<void> }} options.clock
   * @param {(message: object, done: (refused?: boolean) => void) => void} options.send sends; `done`
   *   on acknowledgement, with `true` when the collector refused the record
   * @param {number} [options.maxBytes] the pending bound, 8 MiB by default
   */
  constructor({ source, clock, send, maxBytes = 8 * MIB }) {
    this.source = source
    this.clock = clock
    this.send = send
    this.maxBytes = maxBytes
    this.seq = 0
    this.pendingBytes = 0
    this.incomplete = null
    this.waiters = []
  }

  /**
   * Hands a record over. With room and nobody waiting it is sent at once, synchronously, so a cut's
   * chunks take their place before any later record.
   * @param {object} record a record without source and seq; `at` defaults to now
   * @returns {Promise<boolean>} whether the record was sent
   */
  emit(record) {
    if (this.incomplete) return Promise.resolve(false)
    const waiter = {
      record,
      at: record.at ?? this.clock.now(),
      bytes: Buffer.byteLength(JSON.stringify(record)) + 96,
    }
    if (this.waiters.length === 0 && this.fits(waiter)) {
      this.dispatch(waiter)
      return Promise.resolve(true)
    }
    return new Promise((resolve) => {
      waiter.resolve = resolve
      this.waiters.push(waiter)
      this.clock.sleepUntil(this.clock.now() + ENQUEUE_TIMEOUT_MS).then(() => {
        if (!this.waiters.includes(waiter)) return
        this.incomplete = `${this.source} could not enqueue ${record.recordId} within ${ENQUEUE_TIMEOUT_MS} ms`
        for (const w of this.waiters.splice(0)) w.resolve(false)
      })
    })
  }

  /** @param {{ bytes: number }} waiter @returns {boolean} */
  fits(waiter) {
    return this.pendingBytes + waiter.bytes <= this.maxBytes
  }

  /** @param {{ record: object, at: number, bytes: number }} waiter sends it with the next seq */
  dispatch({ record, at, bytes }) {
    const message = {
      ...record,
      source: this.source,
      seq: this.seq++,
      at,
      execution: record.execution ?? null,
    }
    this.pendingBytes += bytes
    this.send({ type: 'record', record: message }, (refused = false) => {
      this.pendingBytes -= bytes
      if (refused) {
        this.incomplete ??= `${this.source} record ${message.seq} was refused by the collector`
        for (const w of this.waiters.splice(0)) w.resolve(false)
        return
      }
      while (this.waiters.length > 0 && this.fits(this.waiters[0])) {
        const next = this.waiters.shift()
        this.dispatch(next)
        next.resolve(true)
      }
    })
  }
}

/**
 * Takes retained samples: a synchronous inventory cut first, then two new full collections at least
 * the GC gap apart with event-loop turns between, then the memory reading. Every reading carries its
 * GC witness; IDs and the cumulative forced count are never reused.
 */
class Sampler {
  /**
   * @param {object} options
   * @param {{ now: () => number, sleepUntil: (at: number) => Promise<void> }} options.clock
   * @param {() => void} [options.gc] a full collection, `globalThis.gc` by default
   * @param {() => Promise<void>} [options.turn] one event-loop turn for finalizers
   * @param {() => { rss: number, heapUsed: number, external: number }} [options.memory]
   * @param {() => string[]} [options.resources] active resource type names
   * @param {number} [options.gcGapMs] the gap between the two collections
   * @param {string} [options.process] the process slot, for GC ids
   */
  constructor({
    clock,
    gc = globalThis.gc,
    turn = () => new Promise((resolve) => setImmediate(resolve)),
    memory = () => process.memoryUsage(),
    // eslint-disable-next-line n/no-unsupported-features/node-builtins -- present on every supported Node; the only list of live resource types
    resources = () => process.getActiveResourcesInfo(),
    gcGapMs = 100,
    process: slot = 'p00',
  }) {
    if (typeof gc !== 'function') throw new Error('sampling needs --expose-gc')
    Object.assign(this, { clock, gc, turn, memory, resources, gcGapMs, slot })
    this.forced = 0
  }

  /**
   * @param {object} options
   * @param {string} options.role baseline, periodic, quiet, closed or final
   * @param {number|null} options.slot periodic slot
   * @param {string|null} options.boundary quiet boundary
   * @param {number|null} options.reading quiet reading
   * @param {() => string} options.cut takes the inventory cut synchronously, returns its id
   * @returns {Promise<object>} the sample fields without envelope or `process`/`clients`
   */
  async read({ role, slot = null, boundary = null, reading = null, cut }) {
    const admittedAt = this.clock.now()
    const inventory = cut()
    const collect = async () => {
      const begin = this.clock.now()
      this.gc()
      const end = this.clock.now()
      this.forced++
      await this.turn()
      return [begin, end]
    }
    const [firstBeginAt, firstEndAt] = await collect()
    await this.clock.sleepUntil(firstEndAt + this.gcGapMs)
    await this.turn()
    const [secondBeginAt, secondEndAt] = await collect()
    const { rss, heapUsed, external } = this.memory()
    const counts = new Map(EXPLICIT_RESOURCES.map((type) => [type, 0]))
    for (const type of this.resources()) counts.set(type, (counts.get(type) ?? 0) + 1)
    const extra = [...counts.keys()].filter((t) => !EXPLICIT_RESOURCES.includes(t)).sort()
    return {
      role,
      slot,
      boundary,
      reading,
      admittedAt,
      completedAt: this.clock.now(),
      rss,
      heapUsed,
      external,
      activeResources: [...EXPLICIT_RESOURCES, ...extra].map((type) => ({
        type,
        count: counts.get(type),
      })),
      gc: {
        id: `${this.slot}.g${this.forced / 2}`,
        firstBeginAt,
        firstEndAt,
        secondBeginAt,
        secondEndAt,
        forcedCollections: 2,
        cumulativeForced: this.forced,
      },
      inventory,
    }
  }
}

/** @param {object[]} entries @param {number} budget @returns {object[][]} entries split by serialized size */
const chunk = (entries, budget) => {
  const parts = [[]]
  let size = 0
  for (const entry of entries) {
    const bytes = Buffer.byteLength(JSON.stringify(entry)) + 1
    if (size + bytes > budget && parts.at(-1).length > 0) {
      parts.push([])
      size = 0
    }
    parts.at(-1).push(entry)
    size += bytes
  }
  return parts
}

/**
 * Watches the host while the run is live: MemAvailable every 30 s and free disk every minute. Below
 * the matrix abort floor, or with less free disk than the remaining reservation, it reports one
 * abort reason; the supervisor then stops admitting work and tears the run down naturally, so the
 * failing evidence is kept and the attempt is non-green.
 */
class HostMonitor {
  /**
   * @param {object} options
   * @param {{ memAvailable: () => number, freeDisk: () => number }} options.read host readers
   * @param {number} options.minMemAvailable MemAvailable below this aborts
   * @param {() => number} options.reserve the free bytes the rest of the run still needs
   * @param {number} [options.memEveryMs] memory check period
   * @param {number} [options.diskEveryMs] disk check period
   */
  constructor({ read, minMemAvailable, reserve, memEveryMs = 30000, diskEveryMs = 60000 }) {
    Object.assign(this, { read, minMemAvailable, reserve, memEveryMs, diskEveryMs })
    this.lastMem = -Infinity
    this.lastDisk = -Infinity
    this.reason = null
    this.timer = null
  }

  /**
   * Takes the checks that are due at `now`.
   * @param {number} now run-clock ms
   * @returns {{ memAvailable?: number, freeDisk?: number, reserve?: number, abort: string|null }}
   *   the readings taken and the first abort reason, which never changes once set
   */
  check(now) {
    const reading = { abort: null }
    if (now - this.lastMem >= this.memEveryMs) {
      this.lastMem = now
      reading.memAvailable = this.read.memAvailable()
      if (reading.memAvailable < this.minMemAvailable)
        this.reason ??= `MemAvailable ${reading.memAvailable} below ${this.minMemAvailable}`
    }
    if (now - this.lastDisk >= this.diskEveryMs) {
      this.lastDisk = now
      reading.freeDisk = this.read.freeDisk()
      reading.reserve = this.reserve()
      if (reading.freeDisk < reading.reserve)
        this.reason ??= `free disk ${reading.freeDisk} below the remaining reservation ${reading.reserve}`
    }
    reading.abort = this.reason
    return reading
  }

  /**
   * Checks on a timer until stopped.
   * @param {{ now: () => number }} clock the run clock
   * @param {(reading: object) => void} onReading told every reading, including the aborting one
   * @returns {void}
   */
  start(clock, onReading) {
    const tick = () => {
      onReading(this.check(clock.now()))
      this.timer = setTimeout(tick, Math.min(this.memEveryMs, this.diskEveryMs))
    }
    tick()
  }

  /** @returns {void} cancels the timer, so it never holds the process open */
  stop() {
    clearTimeout(this.timer)
    this.timer = null
  }
}

/** @returns {number} Linux MemAvailable in bytes */
const memAvailable = () =>
  Number(/MemAvailable:\s+(\d+) kB/.exec(fs.readFileSync('/proc/meminfo', 'utf8'))[1]) * 1024

/** @param {string} dir @returns {number} bytes free to an unprivileged writer on dir's volume */
const freeDisk = (dir) => {
  const s = fs.statfsSync(dir)
  return s.bavail * s.bsize
}

module.exports = {
  SUPERVISOR,
  OUTER,
  MIB,
  EXPLICIT_RESOURCES,
  DIAGNOSTIC_BYTES,
  clip,
  Collector,
  SourceChannel,
  Sampler,
  HostMonitor,
  chunk,
  memAvailable,
  freeDisk,
}
