const gzip = require('../protocol/message/compression/gzip')
const zstd = require('../protocol/message/compression/zstd')
const otlp = require('./otlp')

/** Orders complete omission groups without changing the collector sample.
 * @param {Array} points immutable point list @returns {Array<Array>} connection, numeric node pairs, custom
 */
const omissionGroups = (points) => {
  const connection = []
  const nodes = new Map()
  const custom = []
  for (const point of points) {
    if (
      point.name.startsWith('org.apache.kafka.') &&
      point.name.endsWith('.connection.creation.total')
    )
      connection.push([point])
    else if (
      point.name.startsWith('org.apache.kafka.') &&
      /\.node\.request\.latency\.(avg|max)$/.test(point.name)
    ) {
      const nodeId = Number(point.attributes.find((a) => a.key === 'node_id').value)
      if (!nodes.has(nodeId)) nodes.set(nodeId, [])
      nodes.get(nodeId).push(point)
    } else custom.push([point])
  }
  custom.sort((a, b) =>
    a[0].identity.key < b[0].identity.key ? -1 : a[0].identity.key > b[0].identity.key ? 1 : 0
  )
  return [
    ...connection,
    ...[...nodes].sort((a, b) => a[0] - b[0]).map(([, pair]) => pair),
    ...custom,
  ]
}

/** Owns bounded payload jobs and codec suppression for one returned client, across transport replacements. */
class TelemetryPayload {
  #transportRevision
  #suppressed = new Set()
  #job = null
  idleWaiters = null

  /** @param {object|bigint} transportRevision non-reused transport token
   * @returns {TelemetryPayload} encoder with no active job or timer
   */
  constructor(transportRevision) {
    this.#transportRevision = transportRevision
  }

  /** Starts a new codec policy episode while preserving an outstanding native-job shell.
   * @param {object|bigint} transportRevision fresh token @returns {void} old results become stale
   * @throws {RangeError} if the old token is reused
   */
  resetTransport(transportRevision) {
    if (transportRevision === this.#transportRevision)
      throw new RangeError('Transport token reused')
    this.#clearJob()
    this.#transportRevision = transportRevision
    this.#suppressed.clear()
  }

  /** Registers a removable wait only when the codec slot is occupied.
   * @returns {{promise:Promise<void>,cancel:Function}|null} idle notification or null when idle;
   * cancellation resolves the wait and detaches it without releasing the native-job slot
   */
  waitForIdle() {
    if (!this.#job) return null
    const waiters = this.idleWaiters ?? (this.idleWaiters = new Set())
    let resolve
    const promise = new Promise((done) => {
      resolve = done
    })
    waiters.add(resolve)
    return {
      promise,
      cancel: () => {
        waiters.delete(resolve)
        resolve()
      },
    }
  }

  /** Invalidates current authority without releasing an outstanding native-job slot early.
   * @returns {void} stale completion cannot suppress a replacement transport's codec
   */
  retire() {
    this.#clearJob()
    this.#transportRevision = null
    this.#suppressed.clear()
  }

  /** Releases retired sample references while retaining the occupied native-job shell.
   * @returns {void} a replacement cannot launch before the old promise settles
   */
  #clearJob() {
    if (!this.#job) return
    this.#job.current = null
    this.#job.raw = null
    this.#job.metrics = null
    this.#job.includedIdentities = []
    this.#job.omittedIdentities = []
    this.#job.accepted = []
  }

  /** @param {object} job issued job shell @returns {boolean} current transport and caller authority */
  #current(job) {
    return job.revision === this.#transportRevision && job.current !== null && job.current()
  }

  /** Measures a bounded prefix before allocating one raw buffer.
   * @param {object} snapshot immutable sample @param {number} cap positive byte bound
   * @returns {object} complete points, exact sizes and omission identities
   * @throws {RangeError} for malformed private data
   */
  #fit(snapshot, cap) {
    const groups = omissionGroups(snapshot.points)
    if (groups.length > 193) throw new RangeError('Invalid telemetry group count')
    let low = 0
    let lowSize = 0
    let high = groups.length
    let sizeProbes = 0
    while (low < high) {
      const mid = Math.ceil((low + high) / 2)
      const measured = otlp.measureMetricsData(snapshot, groups.slice(0, mid).flat())
      sizeProbes += 1
      if (measured <= cap) {
        low = mid
        lowSize = measured
      } else high = mid - 1
    }
    const points = groups.slice(0, low).flat()
    const size = lowSize
    const raw = otlp.encodeMetricsData(snapshot, { points, maxBytes: cap })
    if (raw.length !== size) throw new RangeError('Telemetry size changed')
    const includedIdentities = points.map((p) => p.identity)
    const included = new Set(includedIdentities)
    return {
      raw,
      rawBytes: raw.length,
      includedIdentities,
      omittedIdentities: snapshot.points
        .filter((p) => !included.has(p.identity))
        .map((p) => p.identity),
      sizeProbes,
    }
  }

  /** Runs at most one native job at once, trying at most the two built-ins.
   * @param {object} job current or retired shell @returns {Promise<void>} completion; no retries after retirement
   * @throws {Error} for an authority predicate failure; callers contain it
   */
  async #compress(job) {
    if (job.rawBytes <= 1000) return
    const seen = new Set()
    for (const type of job.accepted) {
      if (seen.has(type) || this.#suppressed.has(type) || (type !== 1 && type !== 4)) continue
      seen.add(type)
      if (type === 4 && !zstd.isSupported) continue
      job.codecAttempts += 1
      let compressed
      try {
        compressed = await (type === 4 ? zstd : gzip).compress({ buffer: job.raw })
      } catch {
        if (!this.#current(job)) return
        this.#suppressed.add(type)
        continue
      }
      if (!this.#current(job)) return
      if (!Buffer.isBuffer(compressed) || compressed.length > job.cap) return
      job.metrics = compressed
      job.compressionType = type
      return
    }
  }

  /** Fits whole points and ordered built-ins without committing collector state.
   * @param {object} snapshot immutable MetricSnapshot
   * @param {object} options positive telemetryMaxBytes/brokerMaxBytes/penaltyMaxBytes,
   * acceptedCompressionTypes (at most 128 signed INT8 values), isCurrent private authority predicate
   * @returns {Promise<object|null>} bounded payload or null for stale/busy work; never queues samples
   * @throws {RangeError} for invalid private metadata; the caller contains telemetry failure
   * @throws {Error} for unexpected encode/authority failure; no raw error text is logged
   */
  async buildPayload(snapshot, options) {
    if (this.#job || this.#transportRevision === null) return null
    const {
      telemetryMaxBytes = 1048576,
      brokerMaxBytes,
      penaltyMaxBytes,
      acceptedCompressionTypes = [],
    } = options
    const limits = [telemetryMaxBytes, brokerMaxBytes, penaltyMaxBytes].filter(
      (v) => v !== undefined
    )
    if (
      limits.some((v) => !Number.isInteger(v) || v < 1) ||
      telemetryMaxBytes > 1048576 ||
      acceptedCompressionTypes.length > 128 ||
      acceptedCompressionTypes.some((v) => !Number.isInteger(v) || v < -128 || v > 127) ||
      typeof options.isCurrent !== 'function'
    )
      throw new RangeError('Invalid telemetry payload options')
    if (!options.isCurrent()) return null
    const cap = Math.min(...limits)
    const job = {
      ...this.#fit(snapshot, cap),
      revision: this.#transportRevision,
      current: options.isCurrent,
      cap,
      accepted: [...acceptedCompressionTypes],
      metrics: null,
      compressionType: 0,
      codecAttempts: 0,
    }
    snapshot = null
    options = null
    job.metrics = job.raw
    this.#job = job
    try {
      if (!this.#current(job)) return null
      await this.#compress(job)
      if (!this.#current(job)) return null
      return {
        metrics: job.metrics,
        compressionType: job.compressionType,
        rawBytes: job.rawBytes,
        transmittedBytes: job.metrics.length,
        includedIdentities: job.includedIdentities,
        omittedIdentities: job.omittedIdentities,
        fittingPasses: Math.max(1, job.sizeProbes),
        sizeProbes: job.sizeProbes,
        codecAttempts: job.codecAttempts,
      }
    } finally {
      this.#clearJob()
      this.#job = null
      const waiters = this.idleWaiters
      this.idleWaiters = null
      if (waiters) {
        for (const resolve of waiters) resolve()
        waiters.clear()
      }
    }
  }
}

module.exports = TelemetryPayload
