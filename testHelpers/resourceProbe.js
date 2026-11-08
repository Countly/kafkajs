const v8 = require('v8')
const sleep = require('../src/utils/sleep')

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
  heap = false,
  gc = globalThis.gc,
  pause = sleep,
} = {}) => {
  if (heap && typeof gc !== 'function') {
    throw new Error('Reading the heap needs a gc function: run node with --expose-gc')
  }

  return {
    /**
     * Takes every measurement after heap collection, when enabled, so weak owners are not pinned
     * by the reading and transient resources in the collection gap are not reported as retained.
     * @returns {Promise<ResourceSnapshot>}
     */
    async snapshot() {
      const snapshot = {}

      // Read weak owners only after collection: dereferencing them would keep them alive through it.
      if (heap) {
        snapshot.heapUsed = await settledHeap(gc, pause)
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
      return snapshot
    },
  }
}

module.exports = { createResourceProbe }
