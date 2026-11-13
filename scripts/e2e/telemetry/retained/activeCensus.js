'use strict'

/** Bounded observations of ordinary codec submissions during the original active settle call. */
class ActiveCensus {
  /**
   * @param {object} options diagnostic-only observers
   * @param {object} options.memory original ProcessMemory
   * @param {Function} options.capture retained capture sink
   * @param {string} options.variant capture or scalar companion without added GC
   * @returns {ActiveCensus} empty diagnostic observer
   */
  constructor({ memory, capture, variant }) {
    Object.assign(this, { memory, capture, variant })
    this.phase = null
    this.matched = null
    this.seen = new Set()
    this.attempted = new Set()
  }

  /** @param {object} reading original M, never moved or rebased @returns {void} baseline capture */
  baseline(reading) {
    this.matched = reading
    this.capture('M', {
      diagnostic: true,
      acceptanceEligible: false,
      attributionComplete: false,
      reading,
      processRetainedBytes: reading.retained,
    })
  }

  /**
   * Selects the first submission per codec within its measured active settlement, never by bytes.
   * The JS completion cannot run during this synchronous probe; the native thread is not held.
   * @param {object} row scalar NativeJournal submission, never the native argument
   * @param {object} journal original journal @returns {void} bounded paired evidence
   * @throws {Error} on GC, capture or capacity failure; never retries a failed capture
   */
  submit(row, journal) {
    if (row) this.record(row.codec, row, journal, 'submission-during-active-settle')
  }

  /**
   * Selects the first original post-turn checkpoint per codec with a pending native job.
   * @param {number} round original GC-pair index @param {string} stage original synchronous seam
   * @param {object} journal original native journal @returns {void} no capture without overlap
   */
  observe(round, stage, journal) {
    if (stage !== 'after-turn-2' || !this.phase) return
    const [row] = journal.live
    if (row) this.record(`${row.codec}-checkpoint`, row, journal, stage, round)
  }

  /**
   * Stores at most two observations per codec, using scalar job identities only.
   * @param {string} name fixed capture name @param {object} row native scalar row
   * @param {object} journal native journal @param {string} stage selected seam
   * @param {number|null} [round] original GC pair @returns {void}
   * @throws {Error} on invalid phase, GC or capture failure; failed captures are not retried
   */
  record(name, row, journal, stage, round = null) {
    if (this.phase !== row.codec || this.attempted.has(name) || !journal.live.has(row)) return
    if (this.attempted.size >= 4 || !['gzip', 'zstd'].includes(row.codec) || !this.matched)
      throw new Error('active census capacity/phase')
    this.attempted.add(name)
    const raw = this.memory.current()
    const pending = [...journal.live].map((job) => ({
      id: job.id,
      jobId: job.jobId,
      codec: job.codec,
      inputBytes: job.inputBytes,
      inputBackingBytes: job.inputBackingBytes,
      submittedAt: job.submitted.at,
    }))
    const majorGc = this.variant === 'capture'
    if (majorGc) this.memory.gc({ type: 'major', execution: 'sync' })
    const afterMajorGc = majorGc ? this.memory.current() : null
    this.capture(name, {
      diagnostic: true,
      acceptanceEligible: false,
      attributionComplete: false,
      stage,
      round,
      pending,
      majorGc,
      raw,
      afterMajorGc,
      rawMinusM: raw.retained - this.matched.retained,
      afterMajorGcMinusM: afterMajorGc ? afterMajorGc.retained - this.matched.retained : null,
      reading: afterMajorGc ?? raw,
      processRetainedBytes: (afterMajorGc ?? raw).retained,
    })
    this.seen.add(name)
  }

  /** @param {string[]} codecs detected codecs @returns {object} original-checkpoint overlap coverage @throws {Error} for missing evidence */
  complete(codecs) {
    if (
      !this.matched ||
      this.attempted.size !== this.seen.size ||
      codecs.some((c) => !this.seen.has(c))
    )
      throw new Error('missing active census')
    return {
      checkpointCoverage: codecs.map((codec) => ({
        codec,
        pendingCheckpointCaptured: this.seen.has(`${codec}-checkpoint`),
      })),
    }
  }
}

/**
 * Ranks additive graph self bytes by category/type/name with three representative root paths.
 * Unknown owners remain explicit; these are neither allocation stacks nor dominator sizes.
 * @param {object} census retained self-byte census @param {object} graph retained graph
 * @param {Set<number>|null} [ids] optional stable-ID cohort, preserving original graph indexes
 * @returns {object[]} twenty largest groups, with named ordinary strong root paths
 */
const topOwners = (census, graph, ids = null) => {
  const owners = new Map()
  for (const [index, node] of census.nodes.entries()) {
    if (ids && !ids.has(node.id)) continue
    const key = JSON.stringify([node.category, node.type, node.name])
    if (!owners.has(key))
      owners.set(key, {
        category: node.category,
        type: node.type,
        name: node.name,
        selfBytes: 0,
        count: 0,
        examples: [],
      })
    const owner = owners.get(key)
    owner.selfBytes += node.selfBytes
    owner.count++
    owner.examples.push({ index, id: node.id, selfBytes: node.selfBytes })
    owner.examples.sort((a, b) => b.selfBytes - a.selfBytes || a.id - b.id)
    if (owner.examples.length > 3) owner.examples.pop()
  }
  return [...owners.values()]
    .sort((a, b) => b.selfBytes - a.selfBytes)
    .slice(0, 20)
    .map((owner) => ({
      ...owner,
      examples: owner.examples.map(({ index, ...node }) => ({
        ...node,
        rootPath: graph.rootPath(index),
      })),
    }))
}

/**
 * Checks the scalar pair against the original M and the completed native journal.
 * @param {object} event retained sink row @param {object} options expected variant, matched and native
 * @returns {void} @throws {Error} on foreign, nonfinite, unpaired or reordered facts
 */
const validatePoint = (event, { variant, matched, native }) => {
  const validReading = (point) =>
    point &&
    Number.isFinite(point.at) &&
    ['heapUsed', 'external', 'arrayBuffers', 'rss', 'retained'].every(
      (key) => Number.isSafeInteger(point[key]) && point[key] >= 0
    ) &&
    point.retained === point.heapUsed + point.external
  if (
    event.diagnostic !== true ||
    event.acceptanceEligible !== false ||
    event.attributionComplete !== false ||
    !validReading(event.reading) ||
    event.processRetainedBytes !== event.reading.retained ||
    !Number.isFinite(event.startedAt) ||
    !Number.isFinite(event.endedAt) ||
    event.startedAt < event.reading.at ||
    event.endedAt < event.startedAt
  )
    throw new Error('invalid active scalar reading')
  if (event.name === 'M') {
    if (JSON.stringify(event.reading) !== JSON.stringify(matched))
      throw new Error('active baseline changed')
    return
  }
  const pending = event.pending?.[0]
  if (
    event.pending?.length !== 1 ||
    !native ||
    native.codec !== event.name.replace(/-checkpoint$/, '') ||
    (event.name.endsWith('-checkpoint')
      ? event.stage !== 'after-turn-2' ||
        !Number.isInteger(event.round) ||
        event.round < 0 ||
        event.round >= 6
      : event.stage !== 'submission-during-active-settle' || event.round !== null) ||
    pending.id !== native.id ||
    pending.jobId !== native.jobId ||
    pending.submittedAt !== native.submitted.at ||
    pending.inputBytes !== native.inputBytes ||
    pending.inputBackingBytes !== native.inputBackingBytes ||
    !validReading(event.raw) ||
    native.submitted.at > event.raw.at ||
    native.settled?.at < event.endedAt ||
    !Number.isFinite(native.settled?.at) ||
    event.rawMinusM !== event.raw.retained - matched.retained ||
    (variant === 'capture'
      ? event.majorGc !== true ||
        !validReading(event.afterMajorGc) ||
        event.afterMajorGc.at < event.raw.at ||
        event.afterMajorGcMinusM !== event.afterMajorGc.retained - matched.retained ||
        JSON.stringify(event.reading) !== JSON.stringify(event.afterMajorGc)
      : event.majorGc !== false ||
        event.afterMajorGc !== null ||
        event.afterMajorGcMinusM !== null ||
        JSON.stringify(event.reading) !== JSON.stringify(event.raw))
  )
    throw new Error('active checkpoint not paired with a pending job')
}

module.exports = { ActiveCensus, topOwners, validatePoint }
