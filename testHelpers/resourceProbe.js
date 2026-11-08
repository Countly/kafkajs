const v8 = require('v8')
// eslint-disable-next-line n/no-unsupported-features/node-builtins -- a development helper that follows resource ancestry; createHook is still the only way to see it
const { AsyncLocalStorage, createHook } = require('async_hooks')
const sleep = require('../src/utils/sleep')
const { readProcStatus, selectPeakSource } = require('./processMemory')

/**
 * What a probe reports. Only the parts whose source was given are present.
 * @typedef {object} ResourceSnapshot
 * @property {Record<string, number>} [listeners] listeners on each tracked emitter, all event
 *   names together
 * @property {{ created: number, destroyed: number, open: number }} [sockets] sockets the product
 *   opened, how many of them it destroyed, and the difference
 * @property {number} [queueDepth] requests pending plus in flight
 * @property {number} [ledgerSize] entries the operation ledger holds
 * @property {number} [obligationSize] reset obligations still owed
 * @property {number} [shareSessions] actual share session-map entries
 * @property {number} [shareRecords] delivered-record entries still held
 * @property {number} [shareRanges] compact acknowledgement range entries still held
 * @property {Record<string, number>} [activeResources] what the event loop is still holding, by
 *   type (`Timeout`, `TCPSocketWrap`, ...)
 * @property {number} [heapUsed] bytes of the V8 heap in use after two collections
 * @property {Record<string, number[]>} [identities] stable weak object IDs by caller-owned role;
 *   repeated entries represent multiple registrations and are not deduplicated
 * @property {{ heapUsed: number, external: number, retainedBytes: number, rss: number,
 *   peakRss: number, rawMaxRss: number, highWaterRss: number|null, highWaterSource: string|null }} [memory]
 *   settled heap plus external with separate RSS readings: `peakRss` and `rawMaxRss` are the raw
 *   `maxRSS * 1024` (which can predate this executable's own image), `highWaterRss` the absolute
 *   `/proc/self/status` high water where that source exists, never a substitute for the raw value
 * @property {Record<string, { id: number, events: Record<string, number[]> }>} [registrations]
 *   per named emitter: its own ID and, per event, the IDs of its raw listeners with multiplicity
 * @property {Record<string, Record<string, number[]>>} [resources] live async resources by role
 *   then type, as sorted issued IDs
 */

/** The gap between the two collections that precede a heap reading, so finalizers have run. */
const GC_GAP_MS = 100

/**
 * @param {Record<string, import('events').EventEmitter>} emitters
 * @returns {Record<string, number>} the listener count of each emitter, over every event name
 */
const listenerCounts = (emitters) =>
  Object.fromEntries(
    Object.entries(emitters).map(([name, emitter]) => [
      name,
      emitter.eventNames().reduce((total, event) => total + emitter.listenerCount(event), 0),
    ])
  )

/**
 * What the event loop still holds, counted by resource type.
 * @returns {Record<string, number>}
 * @throws {Error} while `setTimeout` is one of jest's fake timers, modern (they carry a `clock`)
 *   or legacy (a mock function), which `process.getActiveResourcesInfo()` cannot see: a count
 *   taken then would read as "no timers" whatever is scheduled
 */
const activeResourceCounts = () => {
  if (setTimeout.clock || setTimeout._isMockFunction) {
    throw new Error('process.getActiveResourcesInfo() does not see fake timers')
  }

  const counts = {}
  // eslint-disable-next-line n/no-unsupported-features/node-builtins -- stable from Node 22.16 and 24.0, above this package's floor of 22.0, and this is a development helper
  for (const type of process.getActiveResourcesInfo()) {
    counts[type] = (counts[type] || 0) + 1
  }

  return counts
}

/**
 * The V8 heap in use once garbage has been collected twice, 100 ms apart.
 * @param {() => void} gc a function that runs a full collection
 * @param {(ms: number) => Promise<void>} pause
 * @returns {Promise<number>}
 */
const settledHeap = async (gc, pause) => {
  gc()
  await pause(GC_GAP_MS)
  gc()
  return v8.getHeapStatistics().used_heap_size
}

/** Resource types a `ResourceRoles` observer follows unless told otherwise. */
const DEFAULT_RESOURCE_TYPES = Object.freeze(['Timeout', 'TCPWRAP', 'PIPEWRAP', 'MESSAGEPORT'])

/** The role of a resource nobody declared. */
const UNKNOWN_ROLE = 'unknown'

/**
 * Follows live async resources with the role their creator declared and the tracked resource that
 * triggered them. A resource made outside every `run` scope inherits its tracked parent's role and
 * otherwise reads as `unknown`, so a surviving handle is never silently excluded.
 */
class ResourceRoles {
  /**
   * @param {object} [options]
   * @param {ReadonlyArray<string>} [options.types] async resource types to follow
   * @param {ReadonlyArray<string>} [options.roles] roles every reading carries even while empty,
   *   so an absent role is a missing source and never an empty one
   * @returns {ResourceRoles} a stopped observer
   */
  constructor({ types = DEFAULT_RESOURCE_TYPES, roles = [] } = {}) {
    this.types = new Set(types)
    this.declared = [...new Set([...roles, UNKNOWN_ROLE])]
    this.scope = new AsyncLocalStorage()
    this.live = new Map()
    this.hook = createHook({
      init: (id, type, trigger) => {
        if (!this.types.has(type)) return
        const parent = this.live.get(trigger)
        this.live.set(id, {
          type,
          role: this.scope.getStore() ?? parent?.role ?? UNKNOWN_ROLE,
          parent: parent ? trigger : null,
        })
      },
      destroy: (id) => {
        this.live.delete(id)
      },
    })
  }

  /** @returns {ResourceRoles} this observer, now following resources created from here on */
  start() {
    this.hook.enable()
    return this
  }

  /** @returns {void} stops following and forgets every live entry */
  stop() {
    this.hook.disable()
    this.live.clear()
  }

  /**
   * Runs `work` so every followed resource it creates, directly or through its continuations,
   * carries `role`.
   * @template T
   * @param {string} role for example `target` or `fixture`
   * @param {() => T} work
   * @returns {T} what `work` returned
   */
  run(role, work) {
    return this.scope.run(role, work)
  }

  /**
   * @returns {Record<string, Record<string, number[]>>} live resources by role then type, as the
   *   sorted async IDs, so two readings compare identity and multiplicity and not a total
   */
  read() {
    const roles = Object.fromEntries(this.declared.map((role) => [role, {}]))
    for (const [id, { type, role }] of this.live) {
      const byType = (roles[role] ??= {})
      ;(byType[type] ??= []).push(id)
    }
    for (const byType of Object.values(roles))
      for (const ids of Object.values(byType)) ids.sort((a, b) => a - b)
    return roles
  }

  /**
   * @returns {Array<{ id: number, type: string, role: string, parent: number|null }>} every live
   *   followed resource with the tracked resource that triggered it, as plain numbers and strings
   */
  ancestry() {
    return [...this.live]
      .map(([id, { type, role, parent }]) => ({ id, type, role, parent }))
      .sort((a, b) => a.id - b.id)
  }
}

/**
 * Compares two snapshots by identity, never by total. A source named in `required` that either
 * snapshot omits is a difference of its own, so a missing source cannot pass as an unchanged one.
 * Resources of role `unknown` may only be ones the first snapshot already had.
 * @param {ResourceSnapshot} before
 * @param {ResourceSnapshot} after
 * @param {{ identities?: string[], registrations?: string[], resources?: string[] }} [required]
 *   the roles, emitter names and resource roles both snapshots must carry
 * @returns {string[]} one sentence per difference; empty when the two readings are the same
 */
const compareSnapshots = (before, after, required = {}) => {
  const problems = []
  const same = (a, b) => JSON.stringify(a) === JSON.stringify(b)
  const compare = (kind, left, right, names) => {
    for (const name of names ?? []) {
      if (left?.[name] === undefined || right?.[name] === undefined)
        problems.push(`missing ${kind} source ${name}`)
    }
    for (const name of new Set([...Object.keys(left ?? {}), ...Object.keys(right ?? {})])) {
      if (
        left?.[name] !== undefined &&
        right?.[name] !== undefined &&
        !same(left[name], right[name])
      )
        problems.push(`${kind} ${name} changed`)
    }
  }
  compare('identities', before.identities, after.identities, required.identities)
  compare('registrations', before.registrations, after.registrations, required.registrations)
  compare('resources', before.resources, after.resources, required.resources)
  for (const [type, ids] of Object.entries(after.resources?.[UNKNOWN_ROLE] ?? {})) {
    const known = new Set(before.resources?.[UNKNOWN_ROLE]?.[type] ?? [])
    if (ids.some((id) => !known.has(id))) problems.push(`unknown ${type} resource survives`)
  }
  return problems
}

/**
 * Builds a probe that measures what a client holds on to: listeners, sockets, queued requests,
 * ledger and obligation entries, share session and entry counts, event-loop resources and the
 * settled heap. A test compares two
 * snapshots, taken before a client is built and after it is torn down, and expects the same
 * figures.
 *
 * Each measurement is a source the caller wires; one it leaves out is absent from the snapshot, so
 * a probe never reports a zero it did not measure.
 * @param {object} [sources]
 * @param {Record<string, import('events').EventEmitter>} [sources.emitters] emitters whose
 *   listeners are counted, by name
 * @param {() => { created: number, destroyed: number }} [sources.sockets] for example
 *   `() => socketCounts(fakeFactoryState)`
 * @param {() => number} [sources.queueDepth] requests pending plus in flight
 * @param {() => number} [sources.ledgerSize] `OffsetOperations#size()`
 * @param {() => number} [sources.obligationSize] reset obligations owed
 * @param {() => { shareSessions: number, shareRecords: number, shareRanges: number }} [sources.shareState]
 *   the actual session, delivered-record and compact-range counts; a missing or malformed count
 *   fails the snapshot, so a probe never reports a count it did not read
 * @param {boolean} [sources.activeResources=false] count `process.getActiveResourcesInfo()` by
 *   type; real-timer runs only, a snapshot refuses under fake timers
 * @param {() => Record<string, Iterable<object|Function>>} [sources.identities] currently owned
 *   objects by role or emitter/event; sources own their bounded populations, not the probe
 * @param {() => Record<string, import('events').EventEmitter>} [sources.registrations] emitters by
 *   role whose every event keeps its raw listener identities with multiplicity, so a swapped
 *   emitter, a swapped listener or a duplicate registration changes the reading
 * @param {ResourceRoles} [sources.resources] a started observer of live async resources by role
 * @param {{issued: WeakMap<object, number>, next: number}} [sources.identityScope] explicitly shared
 *   weak identity issuer for readings compared across probes; defaults to a private issuer
 * @param {boolean} [sources.memory=false] collect twice and read heap plus external and RSS;
 *   arrayBuffers is already included in external and is not added again
 * @param {boolean} [sources.heap=false] read the settled heap
 * @param {(() => void)|undefined} [sources.gc] a full collection; defaults to `global.gc`, which
 *   needs `node --expose-gc`
 * @param {(ms: number) => Promise<void>} [sources.pause] defaults to a real sleep
 * @returns {{ snapshot: () => Promise<ResourceSnapshot> }}
 * @throws {Error} when `heap` is asked for and no `gc` is available
 */
const createResourceProbe = ({
  emitters,
  sockets,
  queueDepth,
  ledgerSize,
  obligationSize,
  shareState,
  activeResources = false,
  identities,
  identityScope = { issued: new WeakMap(), next: 0 },
  registrations,
  resources,
  memory = false,
  heap = false,
  gc = globalThis.gc,
  pause = sleep,
} = {}) => {
  if ((heap || memory) && typeof gc !== 'function') {
    throw new Error('Reading the heap needs a gc function: run node with --expose-gc')
  }

  const { issued } = identityScope

  /**
   * @param {object|Function} object a currently owned object
   * @returns {number} its stable issued ID, kept without keeping the object alive
   * @throws {TypeError} for a primitive, which cannot carry weak identity
   */
  const identify = (object) => {
    if ((typeof object !== 'object' || object === null) && typeof object !== 'function')
      throw new TypeError('Resource identity must be an object')
    if (!issued.has(object)) issued.set(object, ++identityScope.next)
    return issued.get(object)
  }

  /**
   * Copies current resource identities without keeping their objects alive.
   * @returns {Record<string, number[]>} sorted IDs with multiplicity preserved
   * @throws {TypeError} for primitive entries, which cannot carry weak identity
   */
  const readIdentities = () =>
    Object.fromEntries(
      Object.entries(identities()).map(([role, objects]) => [
        role,
        [...objects].map(identify).sort((a, b) => a - b),
      ])
    )

  /**
   * Copies each named emitter's own ID and, per event, its raw listener IDs: `once` wrappers keep
   * their own identity and a function registered twice appears twice.
   * @returns {Record<string, { id: number, events: Record<string, number[]> }>}
   * @throws {TypeError} when a source value is not an emitter
   */
  const readRegistrations = () =>
    Object.fromEntries(
      Object.entries(registrations()).map(([name, emitter]) => [
        name,
        {
          id: identify(emitter),
          events: Object.fromEntries(
            emitter.eventNames().map((event) => [
              String(event),
              emitter
                .rawListeners(event)
                .map(identify)
                .sort((a, b) => a - b),
            ])
          ),
        },
      ])
    )

  return {
    /**
     * Takes every measurement after heap collection, when enabled, so weak owners are not pinned
     * by the reading and transient resources in the collection gap are not reported as retained.
     * @returns {Promise<ResourceSnapshot>}
     */
    async snapshot() {
      const snapshot = {}

      // Read weak owners only after collection: dereferencing them would keep them alive through it.
      if (heap || memory) {
        const heapUsed = await settledHeap(gc, pause)
        if (heap) snapshot.heapUsed = heapUsed
        if (memory) {
          const usage = process.memoryUsage()
          const rawMaxRss = process.resourceUsage().maxRSS * 1024
          const status = readProcStatus()
          snapshot.memory = {
            heapUsed: usage.heapUsed,
            external: usage.external,
            retainedBytes: usage.heapUsed + usage.external,
            rss: usage.rss,
            peakRss: rawMaxRss,
            rawMaxRss,
            highWaterRss: status ? status.vmhwm : null,
            highWaterSource: selectPeakSource(() => status)?.name ?? null,
          }
        }
      }
      if (emitters) {
        snapshot.listeners = listenerCounts(emitters)
      }
      if (sockets) {
        const { created, destroyed } = sockets()
        snapshot.sockets = { created, destroyed, open: created - destroyed }
      }
      if (queueDepth) {
        snapshot.queueDepth = queueDepth()
      }
      if (ledgerSize) {
        snapshot.ledgerSize = ledgerSize()
      }
      if (obligationSize) {
        snapshot.obligationSize = obligationSize()
      }
      if (shareState) {
        const counts = shareState()
        for (const name of ['shareSessions', 'shareRecords', 'shareRanges']) {
          if (!Number.isSafeInteger(counts?.[name]) || counts[name] < 0) {
            throw new Error(`Missing share resource measurement: ${name}`)
          }
          snapshot[name] = counts[name]
        }
      }
      if (activeResources) {
        snapshot.activeResources = activeResourceCounts()
      }
      if (identities) snapshot.identities = readIdentities()
      if (registrations) snapshot.registrations = readRegistrations()
      if (resources) snapshot.resources = resources.read()
      return snapshot
    },
  }
}

module.exports = { createResourceProbe, compareSnapshots, ResourceRoles, UNKNOWN_ROLE }
