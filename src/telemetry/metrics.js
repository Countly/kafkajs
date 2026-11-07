const { KafkaJSInvalidConfigurationError, KafkaJSNonRetriableError } = require('../errors')
const { version } = require('../../package.json')

const INT64_MAX = (1n << 63n) - 1n
const UINT64_MAX = (1n << 64n) - 1n
const RESOURCE_KEYS = [
  'client_rack',
  'group_id',
  'group_instance_id',
  'group_member_id',
  'transactional_id',
]
const RESERVED = new Set([
  ...RESOURCE_KEYS,
  'client_instance_id',
  'client_id',
  'client_software_name',
  'client_software_version',
  'client_source_address',
  'client_source_port',
  'principal',
  'node_id',
])
const CLOCK = { wallMs: () => Date.now(), monotonicNs: () => process.hrtime.bigint() }

/**
 * Rejects explicit configuration without retaining an application's thrown value.
 * @returns {KafkaJSInvalidConfigurationError} fixed sanitized diagnostic
 */
const invalid = () => new KafkaJSInvalidConfigurationError('Invalid telemetry metric configuration')

/**
 * Makes a bounded internal refusal; the public facade translates this reason to its public error.
 * @returns {KafkaJSNonRetriableError} internal capacity error, with no application data
 */
const limit = () =>
  Object.assign(new KafkaJSNonRetriableError('Telemetry metric limit exceeded'), {
    reason: 'limit',
  })

/** @param {unknown} value explicit input @returns {boolean} non-array record test */
const record = (value) => value !== null && typeof value === 'object' && !Array.isArray(value)

/**
 * Copies the explicit call's own properties once; inherited properties are ignored.
 * @param {object} value application record
 * @returns {object} prototype-free data, never retained from the caller
 * @throws {Error} converted by the explicit validation boundary
 */
const own = (value) => {
  if (!record(value)) throw invalid()
  const copy = Object.create(null)
  for (const key of Object.keys(value)) copy[key] = value[key]
  return copy
}

/**
 * Snapshots canonical name and attributes inside the explicit synchronous call only.
 * @param {object} value descriptor or identity
 * @param {boolean} full whether type, unit and value must also be copied
 * @returns {object} immutable validated metadata and exact value
 * @throws {KafkaJSInvalidConfigurationError} invalid input, including a throwing accessor
 */
const descriptor = (value, full) => {
  try {
    if (!record(value) || !Object.hasOwn(value, 'name')) throw invalid()
    const name = value.name
    const attributes = Object.hasOwn(value, 'attributes') ? value.attributes : undefined
    if (
      typeof name !== 'string' ||
      !/^[A-Za-z][A-Za-z0-9_.]{0,199}$/.test(name) ||
      !name.includes('.') ||
      name.startsWith('org.apache.kafka.')
    )
      throw invalid()
    const data = attributes === undefined ? Object.create(null) : own(attributes)
    const keys = Object.keys(data).sort()
    if (keys.length > 8) throw invalid()
    let bytes = Buffer.byteLength(name)
    const pairs = keys.map((key) => {
      const value = data[key]
      if (
        !/^[A-Za-z_][A-Za-z0-9_]{0,62}$/.test(key) ||
        RESERVED.has(key) ||
        typeof value !== 'string' ||
        Buffer.byteLength(value) > 256
      )
        throw invalid()
      bytes += Buffer.byteLength(key) + Buffer.byteLength(value)
      return Object.freeze({ key, value })
    })
    const result = { name, attributes: Object.freeze(pairs), key: JSON.stringify([name, pairs]) }
    if (!full) return result
    const unit = Object.hasOwn(value, 'unit') ? value.unit : undefined
    const type = Object.hasOwn(value, 'type') ? value.type : undefined
    const number = Object.hasOwn(value, 'value') ? value.value : undefined
    const normalizedUnit = unit === undefined ? '1' : unit
    if (typeof normalizedUnit !== 'string' || !/^[\x20-\x7e]{1,16}$/.test(normalizedUnit))
      throw invalid()
    if (
      type === 'gauge'
        ? typeof number !== 'number' || !Number.isFinite(number)
        : type !== 'sum' || typeof number !== 'bigint' || number < 0n || number > INT64_MAX
    )
      throw invalid()
    bytes += Buffer.byteLength(normalizedUnit)
    if (bytes > 4096) throw invalid()
    return { ...result, unit: normalizedUnit, type, value: number, bytes }
  } catch {
    throw invalid()
  }
}

/** @returns {object} bounded mutable gauge accumulator */
const window = () => ({ count: 0, sum: 0, max: 0, invalid: false })

/**
 * One numeric collector per returned client. It owns no socket, timer, callback supplier or event listener.
 * Metadata work is outside response completion; observation allocates no per-request objects.
 */
module.exports = class TelemetryMetrics {
  #kind
  #clock
  #logger
  #warnedSubscription = null
  #wall
  #mono
  #lastTime
  #revision = 0n
  #tokens = new WeakSet()
  #nodes = new Map()
  #slots = new WeakMap()
  #custom = new Map()
  #names = new Map()
  #metadataBytes = 0
  #connection
  #subscription = null
  #observing = true
  #pending = null
  #first = null
  #last = null

  /**
   * @param {{kind:'producer'|'consumer'|'admin'|'shareConsumer',clock?:object,logger?:object}} options
   * clock supplies integer wallMs() and bigint monotonicNs(); the default is anchored once.
   * @throws {KafkaJSInvalidConfigurationError} invalid kind or clock domain
   */
  constructor({ kind, clock = CLOCK, logger = null }) {
    if (!['producer', 'consumer', 'admin', 'shareConsumer'].includes(kind)) throw invalid()
    this.#kind = kind
    this.#clock = clock
    this.#logger = logger
    const wall = clock.wallMs()
    const mono = clock.monotonicNs()
    if (!Number.isSafeInteger(wall) || wall < 0 || typeof mono !== 'bigint' || mono < 0n)
      throw invalid()
    this.#wall = BigInt(wall) * 1000000n
    this.#mono = mono
    this.#lastTime = this.#wall
    if (this.#wall > UINT64_MAX) throw invalid()
    this.#connection = this.#series(
      this.#prefix() + 'connection.creation.total',
      'sum',
      '1',
      Object.freeze([]),
      0n
    )
  }

  /** @returns {string} explicit standard or admin extension prefix */
  #prefix() {
    return `org.apache.kafka.${this.#kind === 'shareConsumer' ? 'consumer' : this.#kind}.`
  }

  /**
   * Derives exact nanoseconds from the captured wall/monotonic origin, clamping clock reversal.
   * @returns {bigint} uint64 timestamp
   * @throws {KafkaJSInvalidConfigurationError} out-of-domain timestamp
   */
  #now() {
    const mono = this.#clock.monotonicNs()
    if (typeof mono !== 'bigint' || mono < 0n) throw invalid()
    const time = this.#wall + (mono > this.#mono ? mono - this.#mono : 0n)
    if (time > UINT64_MAX) throw invalid()
    if (time > this.#lastTime) this.#lastTime = time
    return this.#lastTime
  }

  /**
   * @param {string} name fixed name @param {string} type sum or gauge @param {string} unit OTLP unit
   * @param {Array} attributes frozen canonical attributes @param {number|bigint} value observed value
   * @returns {object} issued metric state; selected origin is independent of numeric subscription IDs
   */
  #series(name, type, unit, attributes, value) {
    const time = this.#now()
    const state = {
      name,
      type,
      unit,
      attributes,
      value,
      origin: time,
      baseline: 0n,
      baselineTime: time,
      revision: ++this.#revision,
      selected: this.#subscription === null,
      everSelected: false,
      live: window(),
    }
    state.key = JSON.stringify([name, attributes])
    return state
  }

  /**
   * Defers a fixed diagnostic so application logging cannot interrupt a commit/socket write.
   * @returns {void} warns at most once per subscription revision; logger failures are contained
   */
  #warn() {
    const revision = this.#subscription?.revision
    if (this.#warnedSubscription === revision) return
    this.#warnedSubscription = revision
    Promise.resolve().then(() => {
      try {
        this.#logger?.warn('Telemetry numeric observation was discarded')
      } catch {}
    })
  }

  /**
   * Counts one successful normal physical attempt; weak identity deduplication retains no socket map.
   * @param {object} attemptToken unique object captured before dialing
   * @returns {void} duplicate or invalid tokens contribute nothing
   */
  observeConnection(attemptToken) {
    if (!record(attemptToken) || this.#tokens.has(attemptToken)) return
    this.#tokens.add(attemptToken)
    if (this.#connection.value === INT64_MAX) return this.#warn()
    this.#connection.value += 1n
  }

  /**
   * Binds known metadata outside request completion. At capacity evicts inactive LRU, then observed LRU.
   * Endpoint reuse retires the old slot before issuing another; unknown IDs never become labels.
   * @param {{nodeId:number,endpoint:string,endpointRevision:unknown}} binding current endpoint identity
   * @returns {object|null} opaque issued slot reference, or null for unknown/bootstrap node
   */
  bindNode({ nodeId, endpoint, endpointRevision }) {
    if (
      !Number.isInteger(nodeId) ||
      nodeId < 0 ||
      nodeId > 2147483647 ||
      typeof endpoint !== 'string'
    )
      return null
    const existing = this.#nodes.get(nodeId)
    if (
      existing &&
      existing.endpoint === endpoint &&
      existing.endpointRevision === endpointRevision
    ) {
      existing.active = true
      return existing.ref
    }
    for (const state of this.#nodes.values()) {
      if (state.nodeId === nodeId || state.endpoint === endpoint) this.retireNode(state.ref)
    }
    if (this.#nodes.size === 64) {
      let victim = this.#first
      for (let node = this.#first; node; node = node.next) {
        if (!node.active) {
          victim = node
          break
        }
      }
      this.retireNode(victim.ref)
    }
    const ref = Object.freeze({ revision: ++this.#revision })
    const attributes = Object.freeze([Object.freeze({ key: 'node_id', value: String(nodeId) })])
    const avg = this.#series(
      this.#prefix() + 'node.request.latency.avg',
      'gauge',
      'ms',
      attributes,
      0
    )
    const max = this.#series(
      this.#prefix() + 'node.request.latency.max',
      'gauge',
      'ms',
      attributes,
      0
    )
    const state = {
      nodeId,
      endpoint,
      endpointRevision,
      ref,
      current: true,
      active: true,
      previous: null,
      next: null,
      avg,
      max,
    }
    this.#nodes.set(nodeId, state)
    this.#slots.set(ref, state)
    this.#touch(state)
    if (this.#subscription)
      for (const series of [avg, max])
        this.#selectSeries(series, this.#subscription, this.#now(), false)
    return ref
  }

  /**
   * Marks connection inactivity for eviction priority without relabeling in-flight responses.
   * @param {object} ref issued slot @returns {void}
   */
  releaseNode(ref) {
    const state = this.#slots.get(ref)
    if (state?.current) state.active = false
  }

  /**
   * Retires an endpoint identity outside completion; captured old requests can no longer observe it.
   * @param {object} ref issued slot @returns {void}
   */
  retireNode(ref) {
    const state = this.#slots.get(ref)
    if (!state?.current) return
    state.current = false
    this.#detach(state)
    this.#nodes.delete(state.nodeId)
    if (this.#pending) this.abandon(this.#pending.snapshot)
  }

  /**
   * Unlinks one bounded node without retaining a retired map entry.
   * @param {object} node slot state @returns {void}
   */
  #detach(node) {
    if (node.previous) node.previous.next = node.next
    else if (this.#first === node) this.#first = node.next
    if (node.next) node.next.previous = node.previous
    else if (this.#last === node) this.#last = node.previous
    node.previous = null
    node.next = null
  }

  /**
   * Maintains observation LRU with pointer updates and no per-request allocation.
   * @param {object} node current slot state @returns {void}
   */
  #touch(node) {
    if (this.#last === node) return
    this.#detach(node)
    node.previous = this.#last
    if (this.#last) this.#last.next = node
    else this.#first = node
    this.#last = node
  }

  /**
   * Updates existing numeric windows in constant time, with no label or point allocation.
   * @param {object|null} ref slot captured when the request is created
   * @param {number} durationMs successful response RTT, excluding pending queue time
   * @returns {void} stale/absent slots, paused observation and invalid samples are omitted
   */
  observeRequest(ref, durationMs) {
    if (!this.#observing || !ref) return
    const state = this.#slots.get(ref)
    if (
      !state?.current ||
      typeof durationMs !== 'number' ||
      !Number.isFinite(durationMs) ||
      durationMs < 0
    )
      return
    this.#touch(state)
    const avg = state.avg.live
    const max = state.max.live
    if (state.avg.selected && !avg.invalid) {
      if (avg.count === Number.MAX_SAFE_INTEGER || avg.sum + durationMs > Number.MAX_SAFE_INTEGER) {
        avg.invalid = true
        this.#warn()
      } else {
        avg.count += 1
        avg.sum += durationMs
      }
    }
    if (state.max.selected && !max.invalid) {
      if (max.count === Number.MAX_SAFE_INTEGER) {
        max.invalid = true
        this.#warn()
      } else {
        max.count += 1
        max.max = Math.max(max.max, durationMs)
      }
    }
  }

  /**
   * Pauses RTT work for unsupported capability. Selection acceptance re-enables only selected RTT names.
   * @returns {void}
   */
  pauseObservation() {
    this.#observing = false
  }

  /**
   * Validates and stores one explicit numeric snapshot atomically; no supplier is retained or invoked.
   * @param {object} metric complete Gauge or monotonic BigInt Sum descriptor
   * @returns {void} updates retain origin/revision; an existing identity can update at capacity
   * @throws {KafkaJSInvalidConfigurationError} invalid metadata/value, decrease, or name type/unit change
   * @throws {KafkaJSNonRetriableError} internal limit reason; the public facade translates it
   */
  register(metric) {
    const data = descriptor(metric, true)
    const previous = this.#custom.get(data.key)
    const declaration = this.#names.get(data.name)
    if (declaration && (declaration.type !== data.type || declaration.unit !== data.unit))
      throw invalid()
    if (previous && data.type === 'sum' && data.value < previous.value) throw invalid()
    if (!previous && (this.#custom.size === 128 || this.#metadataBytes + data.bytes > 524288))
      throw limit()
    if (previous) {
      previous.value = data.value
      return
    }
    const state = this.#series(data.name, data.type, data.unit, data.attributes, data.value)
    state.bytes = data.bytes
    this.#custom.set(data.key, state)
    this.#metadataBytes += data.bytes
    this.#names.set(data.name, {
      type: data.type,
      unit: data.unit,
      count: (declaration?.count || 0) + 1,
    })
    if (this.#subscription) this.#selectSeries(state, this.#subscription, this.#now(), false)
  }

  /**
   * Removes a validated identity and retires any unsent snapshot; absent identities are no-ops.
   * @param {object} identity name and optional attributes; type/unit/value getters are not read
   * @returns {void} no network activity, including while the facade is disabled or closed
   * @throws {KafkaJSInvalidConfigurationError} invalid identity
   */
  unregister(identity) {
    const data = descriptor(identity, false)
    const state = this.#custom.get(data.key)
    if (!state) return
    this.#custom.delete(data.key)
    this.#metadataBytes -= state.bytes
    const declaration = this.#names.get(state.name)
    if (--declaration.count === 0) this.#names.delete(state.name)
    if (this.#pending) this.abandon(this.#pending.snapshot)
  }

  /**
   * @param {string} name exact case-sensitive name @param {object} subscription copied selector
   * @returns {boolean} singleton star selects all; an embedded star is a literal prefix
   */
  #matches(name, subscription) {
    const prefixes = subscription.requestedMetrics
    return (
      (prefixes.length === 1 && prefixes[0] === '*') ||
      prefixes.some((prefix) => name.startsWith(prefix))
    )
  }

  /** @returns {Array<object>} bounded state list, never called on response completion */
  #states() {
    return [
      this.#connection,
      ...[...this.#nodes.values()].flatMap((node) => [node.avg, node.max]),
      ...this.#custom.values(),
    ]
  }

  /**
   * @param {object} state metric state @param {object} subscription accepted subscription
   * @param {bigint} time boundary time @param {boolean} switched cumulative to delta boundary
   * @param {boolean} [acceptedBoundary=false] existing series at an accepted replacement boundary
   * @returns {void} retained selected delta metrics preserve their baselines on numeric-ID-only changes
   */
  #selectSeries(state, subscription, time, switched, acceptedBoundary = false) {
    const selected = this.#matches(state.name, subscription)
    if (state.selected && !selected) {
      state.selected = false
      state.revision = ++this.#revision
      state.live = window()
    } else if (selected && !state.selected) {
      state.selected = true
      if (state.everSelected) {
        state.revision = ++this.#revision
        state.origin = time
      }
      const reset = state.everSelected || (acceptedBoundary && this.#subscription !== null)
      state.baseline = reset ? state.value : 0n
      state.baselineTime = reset ? time : state.origin
    }
    if (selected && switched && state.type === 'sum') {
      state.baseline = state.value
      state.baselineTime = time
    }
    if (selected) state.everSelected = true
  }

  /**
   * Applies an accepted subscription boundary, before snapshot construction or queued work.
   * @param {{revision:bigint,requestedMetrics:string[],deltaTemporality:boolean}} subscription local revision
   * @returns {void} freezes selectors; invalidates pending work even if numeric IDs and clocks repeat
   * @throws {KafkaJSInvalidConfigurationError} invalid selector domains
   */
  select(subscription) {
    const { revision, requestedMetrics, deltaTemporality } = subscription
    if (
      typeof revision !== 'bigint' ||
      typeof deltaTemporality !== 'boolean' ||
      !Array.isArray(requestedMetrics) ||
      requestedMetrics.length > 256 ||
      requestedMetrics.some((p) => typeof p !== 'string' || Buffer.byteLength(p) > 256) ||
      requestedMetrics.reduce((n, p) => n + Buffer.byteLength(p), 0) > 16384
    )
      throw invalid()
    if (this.#subscription?.revision === revision) return
    if (this.#pending) this.abandon(this.#pending.snapshot)
    const next = Object.freeze({
      revision,
      requestedMetrics: Object.freeze([...requestedMetrics]),
      deltaTemporality,
    })
    const switched =
      this.#subscription !== null && !this.#subscription.deltaTemporality && deltaTemporality
    const time = this.#now()
    for (const state of this.#states()) this.#selectSeries(state, next, time, switched, true)
    this.#subscription = next
    this.#observing = ['node.request.latency.avg', 'node.request.latency.max'].some((suffix) =>
      this.#matches(this.#prefix() + suffix, next)
    )
  }

  /**
   * Takes only whitelisted applicable resource strings from a trusted lifecycle snapshot.
   * @param {object} resources captured lifecycle data, never an application config object
   * @returns {{attributes:Array,dropped:number}} immutable string attributes and oversize count
   */
  #resources(resources) {
    const allowed =
      this.#kind === 'producer'
        ? ['client_rack', 'transactional_id']
        : this.#kind === 'admin'
          ? ['client_rack']
          : this.#kind === 'shareConsumer'
            ? ['client_rack', 'group_id', 'group_member_id']
            : RESOURCE_KEYS.filter((key) => key !== 'transactional_id')
    const attributes = []
    let dropped = 0
    for (const key of allowed) {
      if (key.startsWith('group_') && !resources.group_id) continue
      const value = resources[key]
      if (typeof value !== 'string' || !value) continue
      if (Buffer.byteLength(value) > 512) dropped += 1
      else attributes.push(Object.freeze({ key, value }))
    }
    return {
      attributes: Object.freeze(attributes.sort((a, b) => (a.key < b.key ? -1 : 1))),
      dropped,
    }
  }

  /**
   * Freezes one exact sample and swaps built-in gauge windows. New observations continue separately.
   * @param {object} subscription previously accepted local revision; never a raw response
   * @param {object} resources trusted captured lifecycle whitelist inputs
   * @returns {object} immutable MetricSnapshot; values and nanosecond times never pass through Number
   * @throws {KafkaJSInvalidConfigurationError} mismatched subscription, nested snapshot or timestamp overflow
   */
  snapshot(subscription, resources = {}) {
    if (
      !this.#subscription ||
      this.#subscription.revision !== subscription.revision ||
      this.#pending
    )
      throw invalid()
    const time = this.#now()
    const points = []
    const entries = new Map()
    const resource = this.#resources(resources)
    for (const state of this.#states()) {
      if (!state.selected) continue
      const builtinGauge = state.type === 'gauge' && !this.#custom.has(state.key)
      const frozen = builtinGauge ? state.live : null
      if (builtinGauge && (!frozen.count || frozen.invalid)) {
        state.live = window()
        continue
      }
      const value = builtinGauge
        ? state.name.endsWith('.avg')
          ? frozen.sum / frozen.count
          : frozen.max
        : state.type === 'sum' && this.#subscription.deltaTemporality
          ? state.value - state.baseline
          : state.value
      const identity = Object.freeze({ key: state.key, revision: state.revision })
      const point = Object.freeze({
        identity,
        name: state.name,
        type: state.type,
        unit: state.unit,
        attributes: state.attributes,
        value,
        startTimeUnixNano:
          state.type === 'sum'
            ? this.#subscription.deltaTemporality
              ? state.baselineTime
              : state.origin
            : undefined,
        timeUnixNano: time,
        aggregationTemporality:
          state.type === 'sum' ? (this.#subscription.deltaTemporality ? 1 : 2) : undefined,
        isMonotonic: state.type === 'sum' ? true : undefined,
      })
      points.push(point)
      entries.set(state.key, { state, point, frozen, total: state.value })
      if (builtinGauge) state.live = window()
    }
    points.sort((a, b) => (a.identity.key < b.identity.key ? -1 : 1))
    const snapshot = Object.freeze({
      revision: ++this.#revision,
      subscriptionRevision: subscription.revision,
      points: Object.freeze(points),
      resources: resource.attributes,
      droppedResourceAttributesCount: resource.dropped,
      scope: Object.freeze({ name: 'countly-kafkajs', version }),
    })
    this.#pending = { snapshot, entries }
    return snapshot
  }

  /**
   * Checks collector authority as one part of the machine's complete pre-write guard.
   * @param {object} snapshot issued immutable sample @returns {boolean} still pending and current
   */
  isCurrent(snapshot) {
    return (
      this.#pending?.snapshot === snapshot &&
      this.#subscription?.revision === snapshot.subscriptionRevision
    )
  }

  /**
   * Merges an omitted/unsent frozen gauge with later observations; never subtracts maxima.
   * @param {object} state current metric @param {object|null} frozen earlier window
   * @returns {void} invalid/overflowed windows are discarded, with a deferred fixed diagnostic
   */
  #merge(state, frozen) {
    if (!frozen) return
    const live = state.live
    if (
      live.invalid ||
      live.count + frozen.count > Number.MAX_SAFE_INTEGER ||
      live.sum + frozen.sum > Number.MAX_SAFE_INTEGER
    ) {
      live.invalid = true
      this.#warn()
      return
    }
    live.count += frozen.count
    live.sum += frozen.sum
    live.max = Math.max(live.max, frozen.max)
  }

  /**
   * Commits only included issued identities at actual write, independent of response success or loss.
   * @param {object} snapshot issued sample
   * @param {Array<{key:string,revision:bigint}>} includedIdentities points actually encoded, not names
   * @returns {void} no application callbacks; obsolete/repeated commits cannot advance replacement baselines
   */
  written(snapshot, includedIdentities) {
    if (!this.isCurrent(snapshot)) return
    const included = new Map(includedIdentities.map((id) => [id.key, id.revision]))
    for (const [key, entry] of this.#pending.entries) {
      const { state, point, frozen, total } = entry
      if (state.revision !== point.identity.revision) continue
      if (included.get(key) === point.identity.revision) {
        if (state.type === 'sum') {
          state.baseline = total
          state.baselineTime = point.timeUnixNano
        }
      } else this.#merge(state, frozen)
    }
    this.#pending = null
  }

  /**
   * Abandons a pending unsent sample, merging only still-current windows. Repeated calls are inert.
   * @param {object} snapshot issued sample @returns {void} no Sum baseline advancement
   */
  abandon(snapshot) {
    if (this.#pending?.snapshot !== snapshot) return
    for (const { state, point, frozen } of this.#pending.entries.values()) {
      if (state.revision === point.identity.revision) this.#merge(state, frozen)
    }
    this.#pending = null
  }

  /**
   * Releases transient windows on suspension/close; custom descriptions and exact Sum totals survive.
   * @returns {void} no resource ownership or implicit reconnection
   */
  resetTransient() {
    this.#pending = null
    for (const state of this.#states()) state.live = window()
    this.#observing = false
    for (const node of this.#nodes.values()) node.current = false
    this.#nodes.clear()
    this.#first = null
    this.#last = null
  }
}
