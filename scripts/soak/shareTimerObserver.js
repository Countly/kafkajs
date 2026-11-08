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
   * Reconciles an already captured owner inventory with its independent same-turn counts.
   * @param {{ rows: object[], dropped: number }} owners Captured owner inventory.
   * @param {Record<string, number>} [counts] Independent event-loop counts, when enabled.
   * @returns {object} Evidence with pre-installation or dropped resource counts explicitly
   *   unattributed. These counts never subtract from the quiet screen's resource comparison.
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
    return { ...owners, unattributed }
  }

  /** @returns {void} Disables the hook and releases all observer storage. */
  restore() {
    this.hook?.disable()
    this.hook = null
    this.owners.clear()
  }
}

module.exports = { ShareTimerObserver }
