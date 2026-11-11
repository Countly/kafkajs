const path = require('path')
const { performance } = require('perf_hooks')
// eslint-disable-next-line n/no-unsupported-features/node-builtins -- development-only lifetime diagnostics
const { createHook } = require('async_hooks')

const ROOT = path.resolve(__dirname, '..') + path.sep
const STACK_FRAMES = 8
const FRAME_CHARS = 512

/**
 * Bounded, case-local Timeout ownership diagnostics for telemetry evidence. Holds scalar facts
 * only: neither Timeout objects, callbacks nor Error objects escape an init hook. Destroyed entries
 * are removed immediately; only live entries can appear in the final quiet-check archive.
 */
class TimeoutDiagnostics {
  /**
   * @param {object} [options] diagnostic limits
   * @param {number} [options.capacity=4096] maximum simultaneous timer facts
   * @returns {TimeoutDiagnostics} a stopped observer; enabling it creates no async resources
   * @throws {RangeError} for an invalid capacity
   */
  constructor({ capacity = 4096 } = {}) {
    if (!Number.isSafeInteger(capacity) || capacity < 1 || capacity > 4096)
      throw new RangeError('Timeout diagnostic capacity must be between 1 and 4096')
    this.capacity = capacity
    this.live = new Map()
    this.created = 0
    this.destroyed = 0
    this.lastDestroyedAt = null
    this.dropped = 0
    this.hook = createHook({
      init: (id, type, triggerAsyncId) => {
        if (type !== 'Timeout') return
        this.created++
        if (this.live.size >= this.capacity) {
          this.dropped++
          return
        }
        const createdAt = performance.now()
        const stack = String(new Error().stack)
          .split('\n')
          .filter(
            (line) =>
              line.includes(ROOT) && !line.includes(__filename) && !line.includes('node_modules')
          )
          .slice(0, STACK_FRAMES)
          .map((line) => line.trim().split(ROOT).join('').slice(0, FRAME_CHARS))
          .join('\n')
        this.live.set(id, { id, triggerAsyncId, createdAt, destroyedAt: null, stack })
      },
      destroy: (id) => {
        if (!this.live.has(id)) return
        this.lastDestroyedAt = performance.now()
        this.destroyed++
        this.live.delete(id)
      },
    })
  }

  /** @returns {TimeoutDiagnostics} this observer, now recording timer creation and destruction */
  start() {
    this.hook.enable()
    return this
  }

  /** @returns {void} disables the hook and drops all stored facts without creating resources */
  stop() {
    this.hook.disable()
    this.live.clear()
  }

  /**
   * @returns {{created:number, destroyed:number, lastDestroyedAt:number|null, dropped:number, live:object[]}} copied scalar
   *   facts for timers still alive at this reading, plus tracked destruction time/count and overflow;
   *   dropped diagnostics are explicit and never alter the independent resource verdict
   */
  read() {
    return {
      created: this.created,
      destroyed: this.destroyed,
      lastDestroyedAt: this.lastDestroyedAt,
      dropped: this.dropped,
      live: [...this.live.values()].map((fact) => ({ ...fact })).sort((a, b) => a.id - b.id),
    }
  }
}

module.exports = { TimeoutDiagnostics }
