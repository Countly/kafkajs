// eslint-disable-next-line n/no-unsupported-features/node-builtins -- share-soak allocation diagnostics require the async_hooks init hook
const { createHook } = require('node:async_hooks')

const MAX_OWNERS = 4096
const MAX_STACK = 2048

/** Attributes timers allocated while a share observer is installed, without retaining owners. */
class ShareTimerObserver {
  /** Starts with bounded live-owner storage and no process hook. */
  constructor() {
    this.owners = new Map()
    this.dropped = 0
    this.hook = null
  }

  /** @returns {void} Enables allocation and destruction tracking only for this share observer. */
  install() {
    const observer = this
    const init = function (asyncId, type, triggerAsyncId, resource) {
      if (type !== 'Timeout' && type !== 'Immediate') return
      if (observer.owners.size >= MAX_OWNERS) {
        observer.dropped++
        return
      }
      const trace = {}
      Error.captureStackTrace(trace, init)
      observer.owners.set(asyncId, {
        type,
        asyncId,
        triggerAsyncId,
        stack: String(trace.stack).slice(0, MAX_STACK),
        resource: new WeakRef(resource),
      })
    }
    this.hook = createHook({ init, destroy: (id) => this.owners.delete(id) })
    this.hook.enable()
  }

  /**
   * Reads only live timer resources; intervals have type Timeout and a positive repeatMs.
   * Timers created before installation cannot be attributed by async_hooks; process resource
   * counts remain independent and include them. A dropped allocation fails the quiet screen.
   * @returns {{ rows: object[], dropped: number }} Bounded, serializable owner evidence.
   */
  snapshot() {
    const rows = []
    for (const [id, { resource, ...owner }] of this.owners) {
      const live = resource.deref()
      if (!live || live._destroyed) {
        this.owners.delete(id)
        continue
      }
      rows.push({
        ...owner,
        hasRef: typeof live.hasRef === 'function' ? live.hasRef() : null,
        durationMs: Number.isFinite(live._idleTimeout) ? live._idleTimeout : null,
        repeatMs: Number.isFinite(live._repeat) ? live._repeat : null,
      })
    }
    return { rows, dropped: this.dropped }
  }

  /**
   * Recognizes only Jest's 100 ms status debounce at its allocating frame. A product allocation
   * with a reporter further down its stack is not a reporter-owned timer.
   * @param {object} owner Captured allocation and snapshot-time timer metadata.
   * @returns {boolean} Whether this referenced, one-shot timer belongs to the status reporter.
   */
  static isStatusDebounce(owner) {
    if (
      owner.type !== 'Timeout' ||
      owner.hasRef !== true ||
      owner.durationMs !== 100 ||
      owner.repeatMs !== null ||
      !Number.isSafeInteger(owner.asyncId) ||
      owner.asyncId < 1
    )
      return false
    const allocatingFrame = owner.stack
      ?.split('\n')
      .find((line) => /^\s+at /.test(line) && !line.includes('(node:'))
    return /^\s+at Status\._debouncedEmit \(.*[/\\]node_modules[/\\]@jest[/\\]reporters[/\\]build[/\\]index\.js:\d+:\d+\)$/.test(
      allocatingFrame ?? ''
    )
  }

  /**
   * Compares product resources with the same harness population at each observation. Raw process
   * counts and full allocation rows remain in the evidence. No unattributed timer is excluded.
   * @param {Record<string, number>|undefined} counts Original same-turn process inventory.
   * @param {{ rows: object[] }|undefined} owners Original same-turn allocation inventory.
   * @returns {Record<string, number>|undefined} Counts excluding exactly attributed status timers.
   */
  static comparableResources(counts, owners) {
    if (!counts) return counts
    const harness = new Set(
      (owners?.rows ?? []).filter(ShareTimerObserver.isStatusDebounce).map(({ asyncId }) => asyncId)
    )
    if (!harness.size) return counts
    return { ...counts, Timeout: (counts.Timeout ?? 0) - harness.size }
  }

  /**
   * Reconciles an already captured owner inventory with its independent same-turn counts.
   * @param {{ rows: object[], dropped: number }} owners Captured owner inventory.
   * @param {Record<string, number>} [counts] Independent event-loop counts, when enabled.
   * @returns {object} Evidence with pre-installation or dropped resource counts explicitly
   *   unattributed. Only the precisely attributed Jest status debounce is excluded from resource
   *   comparisons; its raw count, allocation row and id remain evidence.
   */
  static evidence(owners, counts) {
    const unattributed = counts
      ? Object.fromEntries(
          ['Timeout', 'Immediate'].map((type) => [
            type,
            Math.max(
              0,
              (counts[type] ?? 0) -
                owners.rows.filter((row) => row.type === type && row.hasRef).length
            ),
          ])
        )
      : null
    return {
      ...owners,
      unattributed,
      harnessAsyncIds: owners.rows
        .filter(ShareTimerObserver.isStatusDebounce)
        .map(({ asyncId }) => asyncId),
    }
  }

  /** @returns {void} Disables the hook and releases all observer storage. */
  restore() {
    this.hook?.disable()
    this.hook = null
    this.owners.clear()
  }
}

module.exports = { ShareTimerObserver }
