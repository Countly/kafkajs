const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const { TelemetryWireObserver, OBSERVER_LIMITS } = require('./telemetryFixtures')

/** Version tag every segmented archive and its index carry. */
const SEGMENT_FORMAT = 'segmented-1'
/** Undischarged observer segments one client kind may hold, including one waiting for disposal. */
const LIVE_SEGMENTS = 2
/** Largest sealed transcript or payload bundle file. */
const FILE_BOUND = 2 * 1024 * 1024
const PAYLOAD_BOUND = 4096

/**
 * Fixed segment schedules, derived from each scenario's own plan before any result is read.
 * `perKind` lifecycles are planned for every ordinary kind and `cap` is the most one kind may open;
 * `extras` are the separate one-segment arms.
 */
const SCHEDULES = Object.freeze({
  'telemetry-shutdown': Object.freeze({
    kinds: Object.freeze(['producer', 'consumer', 'admin', 'share']),
    perKind: 21,
    cap: 21,
    extras: Object.freeze(['producer-t0', 'consumer-t0', 'admin-t0', 'share-t0', 'admin-bh']),
    name: (kind, index) => `${kind}-ep-${index}`,
  }),
  'telemetry-churn': Object.freeze({
    kinds: Object.freeze(['producer', 'consumer', 'admin', 'shareConsumer']),
    perKind: 7,
    cap: 16,
    extras: Object.freeze([]),
    name: (kind, index) => `${kind}-lc-${index}`,
  }),
})

/**
 * @param {string} scenario a scheduled scenario name
 * @returns {object} its fixed schedule
 * @throws {Error} for a scenario without a segment schedule
 */
const scheduleOf = (scenario) => {
  if (!Object.hasOwn(SCHEDULES, scenario)) throw new Error(`No segment schedule for ${scenario}`)
  return SCHEDULES[scenario]
}

/**
 * @param {string} scenario a scheduled scenario name
 * @returns {string[]} every segment identity the schedule requires, sorted
 */
const segmentCatalogue = (scenario) => {
  const schedule = scheduleOf(scenario)
  return [
    ...schedule.kinds.flatMap((kind) =>
      Array.from({ length: schedule.perKind }, (_, index) => schedule.name(kind, index))
    ),
    ...schedule.extras,
  ].sort()
}

/**
 * @param {string} scenario a scheduled scenario name
 * @returns {Map<string,{kind:string,role:string,index:number}>} each required segment's identity
 */
const segmentIdentities = (scenario) => {
  const schedule = scheduleOf(scenario)
  const identities = new Map()
  for (const kind of schedule.kinds)
    for (let index = 0; index < schedule.perKind; index++)
      identities.set(schedule.name(kind, index), { kind, role: 'ordinary', index })
  for (const id of schedule.extras) {
    const role = id.endsWith('-t0') ? 'timeout0' : 'blackhole'
    identities.set(id, { kind: id.slice(0, -3), role, index: 0 })
  }
  return identities
}

/**
 * @param {Buffer|string} bytes data
 * @returns {string} lower-case SHA-256 hex
 */
const sha = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')

/**
 * Validates one segment's two files against each other: owner label, the complete payload catalogue
 * (including empty payloads), lengths, hashes, and the proof entries of accepted nonempty Pushes.
 * Used when packing and again, unchanged, when replaying.
 * @param {object} input the files and the proof rule
 * @param {string} input.id expected segment identity
 * @param {Buffer|string} input.transcript the segment transcript file
 * @param {string} input.bundle the payload/proof JSONL file
 * @param {'all'|string[]} [input.proofs='all'] accepted nonempty Pushes that must carry a full proof:
 *   all of them, or exactly the named request keys
 * @returns {{transcript:object,pushes:Map<string,object>,proofs:Map<string,object>}} parsed content
 * @throws {Error} for a differing owner, a missing, extra, duplicate or altered payload or proof
 */
const checkSegmentFiles = ({ id, transcript, bundle, proofs = 'all' }) => {
  if (Buffer.byteLength(bundle) > FILE_BOUND || Buffer.byteLength(transcript) > FILE_BOUND)
    throw new Error(`Segment ${id} file exceeds its bound`)
  const parsed = JSON.parse(transcript)
  if (parsed.owner !== id) throw new Error(`Segment owner differs for ${id}`)
  const pushes = new Map()
  const proofMap = new Map()
  for (const line of bundle.split('\n')) {
    if (!line) continue
    const item = JSON.parse(line)
    const target = item.t === 'push' ? pushes : item.t === 'proof' ? proofMap : null
    if (!target || !/^r\d+$/.test(item.key)) throw new Error(`Invalid bundle entry in ${id}`)
    if (target.has(item.key)) throw new Error(`Duplicate bundle ${item.t} ${item.key} in ${id}`)
    if (item.t === 'push') {
      const blob = Buffer.from(item.b64, 'base64')
      if (blob.length !== item.bytes || blob.length > PAYLOAD_BOUND || sha(blob) !== item.sha256)
        throw new Error(`Segment payload ${item.key} of ${id} differs from its bundle entry`)
      target.set(item.key, { ...item, blob })
    } else {
      const raw = Buffer.from(item.rawB64, 'base64')
      if (
        sha(raw) !== item.rawSha256 ||
        item.decoded?.rawSha256 !== item.rawSha256 ||
        item.decoded?.requestKey !== item.key ||
        !Array.isArray(item.receipts)
      )
        throw new Error(`Segment proof ${item.key} of ${id} is inconsistent`)
      target.set(item.key, { ...item, raw })
    }
  }
  const pushRequests = parsed.records.filter((r) => r.type === 'request' && r.apiKey === 72)
  const requested = pushRequests.map((r) => r.requestKey).sort()
  if (JSON.stringify([...pushes.keys()].sort()) !== JSON.stringify(requested))
    throw new Error(`Segment ${id} payload catalogue differs from its requests`)
  for (const request of pushRequests) {
    const entry = pushes.get(request.requestKey)
    if (entry.bytes !== request.metricsBytes || entry.sha256 !== request.metricsSha256)
      throw new Error(`Segment payload ${request.requestKey} of ${id} differs from its request`)
  }
  const accepted = pushRequests
    .filter((r) => r.response?.errorCode === 0 && r.metricsBytes > 0)
    .map((r) => r.requestKey)
  const required = proofs === 'all' ? accepted : proofs
  for (const key of required)
    if (!proofMap.has(key)) throw new Error(`Missing proof ${key} in segment ${id}`)
  for (const key of proofMap.keys())
    if (!accepted.includes(key) || (proofs !== 'all' && !required.includes(key)))
      throw new Error(`Extra proof ${key} in segment ${id}`)
  return { transcript: parsed, pushes, proofs: proofMap }
}

/**
 * Packs a disposed observer directory into exactly two bounded files and removes the directory.
 * Every request, cancellation, failure, payload, raw body, decoded body and decoder receipt is kept.
 * @param {object} input what to pack
 * @param {string} input.id segment identity
 * @param {string} input.source the observer's directory
 * @param {string} input.target directory receiving the two files
 * @param {'all'|string[]} [input.proofs='all'] proof rule, see `checkSegmentFiles`
 * @returns {object} the sealed summary: file names with sizes and digests, accepted and empty Pushes
 * @throws {Error} for a missing transcript, a bound violation or an inconsistent pair of files
 */
const packSegment = ({ id, source, target, proofs = 'all' }) => {
  const transcriptFile = path.join(source, 'transcript.json')
  if (!fs.existsSync(transcriptFile)) throw new Error(`Segment ${id} has no cleanup receipt`)
  const transcript = fs.readFileSync(transcriptFile)
  const names = fs.readdirSync(source)
  const keyOf = (name) => /\d+/.exec(name)[0]
  const lines = []
  for (const name of names.filter((n) => /^push-r\d+\.bin$/.test(n))) {
    const blob = fs.readFileSync(path.join(source, name))
    lines.push({
      t: 'push',
      key: `r${keyOf(name)}`,
      bytes: blob.length,
      sha256: sha(blob),
      b64: blob.toString('base64'),
    })
  }
  const receipts = names
    .filter((n) => /^decoder-\d+\.json$/.test(n))
    .map((n) => JSON.parse(fs.readFileSync(path.join(source, n), 'utf8')))
  for (const name of names.filter((n) => /^decoded-r\d+\.json$/.test(n))) {
    const decoded = JSON.parse(fs.readFileSync(path.join(source, name), 'utf8'))
    const raw = fs.readFileSync(path.join(source, `raw-${decoded.rawSha256}.bin`))
    lines.push({
      t: 'proof',
      key: `r${keyOf(name)}`,
      rawSha256: decoded.rawSha256,
      rawB64: raw.toString('base64'),
      decoded,
      receipts: receipts.filter((r) => r.rawSha256 === decoded.rawSha256),
    })
  }
  const bundle = lines.map((line) => JSON.stringify(line) + '\n').join('')
  const checked = checkSegmentFiles({ id, transcript, bundle, proofs })
  fs.mkdirSync(target, { recursive: true })
  const files = {}
  for (const [kind, name, bytes] of [
    ['transcript', `${id}.transcript.json`, transcript],
    ['bundle', `${id}.bundle.jsonl`, Buffer.from(bundle)],
  ]) {
    fs.writeFileSync(path.join(target, name), bytes, { flag: 'wx', mode: 0o600 })
    files[kind] = { name, bytes: bytes.length, sha256: sha(bytes) }
  }
  fs.rmSync(source, { recursive: true, force: true })
  const records = checked.transcript.records.filter((r) => r.type === 'request' && r.apiKey === 72)
  return {
    id,
    ...files,
    accepted: records
      .filter((r) => r.response?.errorCode === 0 && r.metricsBytes > 0)
      .map((r) => r.requestKey),
    empty: records.filter((r) => r.metricsBytes === 0).map((r) => r.requestKey),
  }
}

/**
 * Owns the observer segments of one public client across its lifecycles. A stable socket factory
 * routes every new socket to the selected segment. The next segment is created and selected before
 * the public close, so sockets the next lifecycle opens belong to it, while sockets already open
 * stay with their original observer. A closed segment is sealed independently of the next one.
 */
class TelemetrySegments {
  #id
  #planned
  #cap
  #directory
  #observerOptions
  #clock
  #segments = []
  #active = null
  #journal = []

  /**
   * @param {object} options segment plan and observer configuration
   * @param {function(number):string} options.id segment identity for a lifecycle index
   * @param {number} options.planned lifecycles the schedule requires
   * @param {number} options.cap most lifecycles that may be opened
   * @param {string} options.directory absolute run directory, also receiving sealed files
   * @param {{now:function():number}} options.clock monotonic milliseconds
   * @param {object} options.observer options passed to every `TelemetryWireObserver`
   * @throws {Error} for a plan that exceeds its cap
   */
  constructor({ id, planned, cap, directory, clock, observer = {} }) {
    if (!Number.isSafeInteger(planned) || planned < 1 || cap < planned)
      throw new Error('A segment plan must be positive and within its cap')
    this.#id = id
    this.#planned = planned
    this.#cap = cap
    this.#directory = directory
    this.#clock = clock
    this.#observerOptions = { completeStream: true, ...observer, directory, clock }
    this.socketFactory = (options) => this.#route(options)
    this.#active = this.#create()
  }

  /**
   * @param {string} scenario scheduled scenario
   * @param {string} kind ordinary client kind
   * @param {object} shared directory, clock and observer options common to the scenario
   * @returns {TelemetrySegments} the owner of that kind's lifecycles
   */
  static ordinary(scenario, kind, shared) {
    const schedule = scheduleOf(scenario)
    if (!schedule.kinds.includes(kind)) throw new Error(`Unknown scheduled kind ${kind}`)
    return new TelemetrySegments({
      ...shared,
      id: (index) => schedule.name(kind, index),
      planned: schedule.perKind,
      cap: schedule.cap,
    })
  }

  /**
   * @param {string} scenario scheduled scenario
   * @param {string} id one of the scenario's extra segment identities
   * @param {object} shared directory, clock and observer options common to the scenario
   * @returns {TelemetrySegments} the owner of that one-segment arm
   */
  static single(scenario, id, shared) {
    if (!scheduleOf(scenario).extras.includes(id)) throw new Error(`Unscheduled segment ${id}`)
    return new TelemetrySegments({ ...shared, id: () => id, planned: 1, cap: 1 })
  }

  /** @returns {object} the selected segment: `id`, `index`, `observer` and `state` */
  current() {
    return this.#active
  }

  /** @returns {object[]} segments that are not sealed yet, including one awaiting disposal */
  undischarged() {
    return this.#segments.filter((segment) => segment.state !== 'sealed')
  }

  /** @returns {object[]} copy of the ordering journal: select, resolve and connect notes */
  journal() {
    return this.#journal.map((note) => ({ ...note }))
  }

  /**
   * @param {object} options the factory arguments of the client
   * @returns {import('net').Socket} a socket owned by the selected segment
   * @throws {Error} when no segment is selected
   */
  #route(options) {
    if (!this.#active || this.#active.state === 'sealed')
      throw new Error('No observer segment is selected')
    return this.#active.observer.socketFactory(options)
  }

  /**
   * @returns {object} a fresh active segment with its own observer
   * @throws {Error} at the cap, or while two segments are already undischarged
   */
  #create() {
    if (this.#segments.length >= this.#cap)
      throw new Error(`Segment cap of ${this.#cap} reached for ${this.#id(0)}`)
    if (this.undischarged().length >= LIVE_SEGMENTS)
      throw new Error('Two undischarged observer segments exist: the previous one has not drained')
    const index = this.#segments.length
    const id = this.#id(index)
    const segment = {
      id,
      index,
      state: 'active',
      observer: new TelemetryWireObserver({
        limits: OBSERVER_LIMITS,
        ...this.#observerOptions,
        owner: id,
      }),
    }
    this.#segments.push(segment)
    return segment
  }

  /**
   * @param {string} type journal entry kind
   * @param {object} fields entry details
   */
  #note(type, fields) {
    this.#journal.push({ seq: this.#journal.length + 1, type, at: this.#clock.now(), ...fields })
  }

  /**
   * Replaces the lifecycle: selects the next segment, closes the public client, and calls the same
   * object's public connect in the close's own continuation, with nothing in between.
   * @param {object} client public client
   * @param {object} [closeOptions] public disconnect arguments
   * @returns {{from:object,to:object,closed:Promise<void>,reopened:Promise<void>}} both segments,
   *   the disconnect outcome and the connect outcome (which also rejects when the close does)
   * @throws {Error} before touching the client at the cap or the two-segment limit
   */
  cycle(client, closeOptions) {
    const from = this.#active
    const to = this.#create()
    this.#note('select', { from: from.id, to: to.id })
    this.#active = to
    from.state = 'closing'
    const closed = from.observer.closeClient(client, closeOptions)
    from.closing = closed
    const reopened = closed.then(() => {
      from.state = 'closed'
      this.#note('disconnect-resolved', { from: from.id, to: to.id })
      const connecting = client.connect()
      this.#note('connect-called', { from: from.id, to: to.id })
      return connecting
    })
    reopened.catch(() => {})
    return { from, to, closed, reopened }
  }

  /**
   * Selects the next lifecycle's segment once the selected one has been closed, without touching a
   * client: the caller opens the next lifecycle itself.
   * @returns {{from:object,to:object}} the closed and the newly selected segment
   * @throws {Error} while the selected segment is not closed, at the cap, or at the two-segment limit
   */
  advance() {
    const from = this.#active
    if (from.state !== 'closed') throw new Error(`Segment ${from.id} is still ${from.state}`)
    const to = this.#create()
    this.#note('select', { from: from.id, to: to.id })
    this.#active = to
    return { from, to }
  }

  /**
   * Seals one closed segment: after its client sockets closed, dispose its observer, require a clean
   * cleanup, pack its files and release the observer. Sealing starts once; repeats share it.
   * @param {object} segment a segment returned by `cycle` or `current`
   * @param {object} [options] `closeMs` bound for its sockets to close; `force` seals whatever remains
   * @param {'all'|string[]} [options.proofs='all'] proof rule of the packed bundle
   * @returns {Promise<object>} its sealed summary
   * @throws {Error} while it is active, still owns an open socket, or is not clean
   */
  async discharge(segment, { closeMs = 1000, force = false, proofs = 'all' } = {}) {
    if (segment.sealing) return segment.sealing
    if (!force && segment.state !== 'closed')
      throw new Error(`Segment ${segment.id} is still ${segment.state}`)
    if (!force) {
      const { observer } = segment
      let timer
      await Promise.race([
        observer.socketsClosed(),
        new Promise((resolve) => {
          timer = setTimeout(resolve, closeMs)
        }),
      ])
      clearTimeout(timer)
      if (observer.openSockets() > 0)
        throw new Error(`Segment ${segment.id} still owns ${observer.openSockets()} open sockets`)
    }
    segment.sealing ??= this.#seal(segment, proofs)
    return segment.sealing
  }

  /**
   * @param {object} segment the segment to seal
   * @param {'all'|string[]} proofs proof rule of the packed bundle
   * @returns {Promise<object>} its sealed summary
   * @throws {Error} when the observer is not clean or its files are inconsistent
   */
  async #seal(segment, proofs) {
    const { observer } = segment
    await observer.dispose()
    observer.assertClean({ allowUnanswered: observer.cancellationKeys() })
    segment.pack = packSegment({
      id: segment.id,
      source: observer.directory,
      target: path.join(this.#directory, 'segments'),
      proofs,
    })
    segment.state = 'sealed'
    segment.observer = null
    return segment.pack
  }

  /**
   * Closes the selected segment's public client once, without opening another lifecycle.
   * @param {object} client public client
   * @param {object} [closeOptions] public disconnect arguments
   * @returns {Promise<void>} after the original disconnect settled
   * @throws {Error} when the selected segment was already closed
   */
  close(client, closeOptions) {
    const segment = this.#active
    if (segment.state !== 'active')
      throw new Error(`Segment ${segment.id} is already ${segment.state}`)
    segment.state = 'closing'
    segment.closing = segment.observer.closeClient(client, closeOptions).then(() => {
      segment.state = 'closed'
    })
    return segment.closing
  }

  /**
   * Closes the selected terminal segment through the public client unless it is already closed, then
   * seals every segment that is not sealed. Every release is attempted; failures are reported
   * together. Calling it again after success only returns the sealed summaries.
   * @param {object} client public client
   * @param {object} [closeOptions] public disconnect arguments
   * @param {object} [options] primary scenario failure for incomplete-schedule diagnostics
   * @param {Error} [options.cause] failure that prevented the remaining lifecycles from opening
   * @returns {Promise<object[]>} sealed summaries of all segments, in lifecycle order
   * @throws {Error|AggregateError} every failure, after all releases were attempted
   */
  async finish(client, closeOptions, { cause } = {}) {
    const errors = []
    const attempt = async (work) => {
      try {
        await work()
      } catch (error) {
        errors.push(error)
      }
    }
    const last = this.#active
    if (last.state === 'active') this.close(client, closeOptions)
    if (last.closing) await attempt(() => last.closing)
    if (last.state === 'closing') last.state = 'closed'
    for (const segment of this.undischarged())
      await attempt(() => this.discharge(segment, { force: errors.length > 0 }))
    if (!errors.length && this.#segments.length !== this.#planned)
      errors.push(
        new Error(
          `Expected ${this.#planned} segments, found ${this.#segments.length}` +
            (cause ? ` after scenario failure: ${cause.message}` : ''),
          { cause }
        )
      )
    if (errors.length === 1) throw errors[0]
    if (errors.length) throw new AggregateError(errors, 'Segment cleanup failed')
    return this.#segments.map((segment) => segment.pack)
  }

  /** @returns {object[]} sealed summaries so far, in lifecycle order */
  sealed() {
    return this.#segments.filter((segment) => segment.pack).map((segment) => segment.pack)
  }
}

module.exports = {
  FILE_BOUND,
  LIVE_SEGMENTS,
  SCHEDULES,
  SEGMENT_FORMAT,
  TelemetrySegments,
  checkSegmentFiles,
  packSegment,
  segmentCatalogue,
  segmentIdentities,
}
