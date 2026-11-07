const { performance } = require('node:perf_hooks')
const ClientTelemetry = require('./client')
const TelemetryMetrics = require('./metrics')
const {
  KafkaJSTelemetryError,
  KafkaJSInvalidConfigurationError,
  KafkaJSNonRetriableError,
} = require('../errors')

const CLOCK = { now: () => performance.now(), random: Math.random, setTimeout, clearTimeout }
const DEFAULTS = Object.freeze({ enableMetricsPush: false, telemetryMaxBytes: 1048576 })
const RESOURCE_KEYS = [
  'client_rack',
  'group_id',
  'group_instance_id',
  'group_member_id',
  'transactional_id',
]

/**
 * Snapshots the known option fields and the two telemetry scalars exactly once, including
 * inherited fields, before any factory effect. Unknown properties are never read. A throwing
 * getter or Proxy trap becomes a fixed error that carries no caller-controlled text.
 * @param {*} source caller options; property access boxes primitives as before
 * @param {string[]} names existing factory fields, excluding the two telemetry fields
 * @param {{enableMetricsPush:boolean,telemetryMaxBytes:number}} [parent] owned Kafka-wide defaults
 * @returns {{options:object, config:{enableMetricsPush:boolean,telemetryMaxBytes:number}}} owned
 *   prototype-free field snapshot and the frozen resolved telemetry configuration
 * @throws {KafkaJSInvalidConfigurationError} failed option access, a non-boolean opt-in or a byte
 *   limit that is not an integer from 1 to 1048576, even while telemetry is disabled
 * @throws {TypeError} for a nullish source, as destructuring it always did
 */
const snapshotOptions = (source, names, parent = DEFAULTS) => {
  const options = Object.create(null)
  try {
    for (const name of [...names, 'enableMetricsPush', 'telemetryMaxBytes']) {
      options[name] = source[name]
    }
  } catch (error) {
    if (source === undefined || source === null) throw error
    throw new KafkaJSInvalidConfigurationError('Invalid client telemetry options')
  }
  const enabled =
    options.enableMetricsPush === undefined ? parent.enableMetricsPush : options.enableMetricsPush
  const max =
    options.telemetryMaxBytes === undefined ? parent.telemetryMaxBytes : options.telemetryMaxBytes
  if (typeof enabled !== 'boolean' || !Number.isInteger(max) || max < 1 || max > 1048576) {
    throw new KafkaJSInvalidConfigurationError('Invalid client telemetry options')
  }
  return { options, config: Object.freeze({ enableMetricsPush: enabled, telemetryMaxBytes: max }) }
}

/**
 * Copies the whitelisted resource strings; a later source overrides an earlier one, and an empty
 * string or non-string removes the key.
 * @param {...object} sources trusted copied lifecycle values
 * @returns {object} frozen prototype-free resource values
 */
const copyResources = (...sources) => {
  const copy = Object.create(null)
  for (const source of sources) {
    for (const key of RESOURCE_KEYS) {
      const value = source?.[key]
      if (value === undefined) continue
      if (typeof value === 'string' && value) copy[key] = value
      else delete copy[key]
    }
  }
  return Object.freeze(copy)
}

/**
 * Owns one returned client's telemetry attachment and resource snapshots. The machine owns all
 * lifecycle work and the only close; a disabled binding owns copied configuration and method stubs
 * only, with no collector, machine, adapter, timer or socket.
 */
class TelemetryFactory {
  /**
   * @param {object} options trusted factory values
   * @param {'producer'|'consumer'|'admin'|'shareConsumer'} options.kind returned client kind
   * @param {{enableMetricsPush:boolean,telemetryMaxBytes:number}} options.config resolved configuration
   * @param {object|null} [options.logger] root logger, used by the collector and machine
   * @param {{now:Function,random:Function,setTimeout:Function,clearTimeout:Function}} [options.clock]
   *   monotonic millisecond clock shared with the machine
   * @returns {TelemetryFactory} one independent binding; an enabled one owns one collector
   */
  constructor({ kind, config, logger = null, clock = CLOCK }) {
    Object.assign(this, { kind, config, logger, clock })
    this.resources = copyResources()
    this.resourceRevision = 0n
    this.resourceReader = null
    this.finalResources = null
    this.metrics = null
    this.machine = null
    this.failed = false
    if (config.enableMetricsPush) {
      try {
        this.metrics = new TelemetryMetrics({ kind, logger })
      } catch {
        this.failed = true
      }
    }
  }

  /**
   * @param {string} reason member of the closed observational reason union
   * @returns {KafkaJSTelemetryError} fixed-message public error
   */
  error(reason) {
    return new KafkaJSTelemetryError(`Client telemetry ${reason}`, { reason })
  }

  /**
   * Creates the machine once the core cluster exists. It opens no socket and starts no timer.
   * @param {object} cluster this returned client's own cluster, never a shared one
   * @returns {void} a failure marks only telemetry as failed; core construction is unaffected
   */
  bind(cluster) {
    if (!this.metrics || this.machine) return
    try {
      this.machine = new ClientTelemetry({
        kind: this.kind,
        metrics: this.metrics,
        transport: cluster.createTelemetryTransport(this.clock),
        clock: this.clock,
        logger: this.logger,
        config: {
          ...this.config,
          makeError: (reason) => this.error(reason),
          resources: () => this.captureResources(),
        },
      })
    } catch {
      this.failed = true
    }
  }

  /**
   * Installs trusted copied static values outside the request path. Every call fences earlier
   * ordinary snapshots, even for equal values.
   * @param {object} values applicable config strings: client_rack, group_id, group_instance_id,
   *   transactional_id
   * @returns {void} a no-op while disabled
   */
  updateResources(values) {
    if (!this.metrics) return
    this.resources = copyResources(values)
    this.resourceRevision += 1n
  }

  /**
   * Installs a private lifecycle reader; it is never an application callback and is only invoked
   * at a push boundary.
   * @param {() => ({values:object,isCurrent:() => boolean}|null)} read current copied member values
   *   plus the captured original authority, or null while the client has no member
   * @returns {void} a no-op while disabled
   */
  setResourceReader(read) {
    if (this.metrics) this.resourceReader = read
  }

  /**
   * Captures owned resource values and a currentness predicate. A captured final shadows ordinary
   * sampling until its close ends.
   * @returns {{values:object,isCurrent:() => boolean}} the final capture while a close owns one,
   *   otherwise the current static and member values; an unreadable lifecycle is never current
   */
  captureResources() {
    if (this.finalResources) return this.finalResources
    const revision = this.resourceRevision
    try {
      const current = this.resourceReader?.()
      const values = copyResources(this.resources, current?.values)
      return {
        values,
        isCurrent: () => this.resourceRevision === revision && (!current || current.isCurrent()),
      }
    } catch {
      return { values: this.resources, isCurrent: () => false }
    }
  }

  /**
   * Arms telemetry after the owner's original usability check, reopening a closed lifecycle.
   * @returns {void} idempotent while connected; contained
   */
  resume() {
    try {
      if (this.machine && !this.machine.connected) this.finalResources = null
      this.machine?.resume()
    } catch {
      this.failed = true
    }
  }

  /**
   * Synchronously fences crash work. It keeps the UUID, registrations and totals and sends no final.
   * @returns {void} contained
   */
  suspend() {
    try {
      this.machine?.suspend()
    } catch {
      this.failed = true
    }
  }

  /**
   * Captures the final resources and the original close budget, then delegates to the machine's
   * one close. The owner joins the returned promise concurrently with its unchanged close; no time
   * is reserved from or subtracted from it.
   * @param {{deadline?:number,remaining?:() => number,zero?:boolean}} budget the owner's first
   *   valid close authority, in the owner's own time domain
   * @returns {Promise<void>} contained and bounded; it never rejects, and repeated calls share the
   *   one episode
   */
  close(budget) {
    if (!this.machine) return Promise.resolve()
    if (this.machine.closing) return this.machine.closing.promise
    const resources = this.captureResources()
    let usable = false
    try {
      usable = resources.isCurrent()
    } catch {}
    const capture = {
      values: resources.values,
      isCurrent: () => usable && this.finalResources === capture,
    }
    this.finalResources = capture
    const release = () => {
      if (this.finalResources === capture) this.finalResources = null
    }
    try {
      return Promise.resolve(this.machine.close(budget))
        .catch(() => {})
        .finally(release)
    } catch {
      release()
      this.failed = true
      return Promise.resolve()
    }
  }

  /**
   * Delegates entry-time validation and the bounded wait to the machine; never connects.
   * @param {{timeout?:number}} [options] public timeout options
   * @returns {Promise<string>} the cached or accepted canonical UUID
   * @throws {KafkaJSInvalidConfigurationError|KafkaJSTelemetryError} as a rejection only
   */
  async clientInstanceId(options) {
    if (this.machine) return this.machine.clientInstanceId(options)
    ClientTelemetry.waitTimeout(options)
    throw this.error(
      this.config.enableMetricsPush ? (this.failed ? 'stopped' : 'not-connected') : 'disabled'
    )
  }

  /**
   * Stores a numeric snapshot; it schedules nothing and calls no callback.
   * @param {object} metric Gauge or monotonic BigInt Sum descriptor
   * @returns {void} allowed before connect, after close and while stopped
   * @throws {KafkaJSTelemetryError} `disabled`, or `limit` at the custom-series cap
   * @throws {KafkaJSInvalidConfigurationError} invalid descriptor, decrease or name type/unit change
   */
  registerMetricForSubscription(metric) {
    if (!this.config.enableMetricsPush) throw this.error('disabled')
    if (!this.metrics) throw this.error('stopped')
    try {
      this.metrics.register(metric)
    } catch (error) {
      if (error instanceof KafkaJSNonRetriableError && error.reason === 'limit') {
        throw this.error('limit')
      }
      throw error
    }
  }

  /**
   * Removes a registration. The identity is validated even while disabled; a valid absent
   * identity is a synchronous no-op.
   * @param {object} identity name and attributes; other descriptor fields are ignored
   * @returns {void} no connection, timer or loop restart
   * @throws {KafkaJSInvalidConfigurationError} invalid identity
   */
  unregisterMetricFromSubscription(identity) {
    if (this.metrics) this.metrics.unregister(identity)
    else TelemetryMetrics.validateIdentity(identity)
  }

  /** @returns {object} the three additive public methods, bound to this binding only */
  methods() {
    return {
      clientInstanceId: (options) => this.clientInstanceId(options),
      registerMetricForSubscription: (metric) => this.registerMetricForSubscription(metric),
      unregisterMetricFromSubscription: (identity) =>
        this.unregisterMetricFromSubscription(identity),
    }
  }
}

module.exports = { TelemetryFactory, snapshotOptions }
