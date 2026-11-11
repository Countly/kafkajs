const path = require('path')
const { performance } = require('perf_hooks')
// eslint-disable-next-line n/no-unsupported-features/node-builtins -- development-only lifetime diagnostics
const { createHook } = require('async_hooks')

const ROOT = path.resolve(__dirname, '..') + path.sep
const STACK_FRAMES = 8
const FRAME_CHARS = 512
const STACK_LIMIT = 64

/**
 * Captures bounded scalar frames inside an init hook, restoring the caller's stack limit even if
 * stack formatting throws. Repo frames take precedence; otherwise raw caller frames identify
 * dependencies or Node callers beyond the async-hooks machinery.
 * @returns {{stack:string, source:string}} at most eight frames of at most 512 characters each
 */
function creationStack() {
  const previousLimit = Error.stackTraceLimit
  let frames
  try {
    Error.stackTraceLimit = STACK_LIMIT
    frames = String(new Error().stack).split('\n').slice(1)
  } finally {
    Error.stackTraceLimit = previousLimit
  }
  const repo = frames.filter(
    (line) => line.includes(ROOT) && !line.includes(__filename) && !line.includes('node_modules')
  )
  const selected = repo.length
    ? repo
    : frames.filter(
        (line) =>
          !line.includes(__filename) &&
          !line.includes('node:internal') &&
          !line.includes('async_hooks')
      )
  return {
    source: repo.length ? 'repo' : 'raw',
    stack: selected
      .slice(0, STACK_FRAMES)
      .map((line) => line.trim().split(ROOT).join('').slice(0, FRAME_CHARS))
      .join('\n'),
  }
}

/**
 * Bounded, case-local Timeout ownership diagnostics for telemetry evidence. Holds scalar facts
 * only: neither Timeout objects, callbacks nor Error objects escape an init hook. Destroyed timer entries
 * are removed immediately; only live entries can appear in the final quiet-check archive.
 * Resource creation facts must be captured before a resource becomes a timer trigger. Both that
 * bounded history and the facts pinned by live timers contain only scalar fields.
 */
class TimeoutDiagnostics {
  /**
   * @param {object} [options] diagnostic limits
   * @param {number} [options.capacity=4096] maximum entries in each scalar-fact map
   * @returns {TimeoutDiagnostics} a stopped observer; enabling it creates no async resources
   * @throws {RangeError} for an invalid capacity
   */
  constructor({ capacity = 4096 } = {}) {
    if (!Number.isSafeInteger(capacity) || capacity < 1 || capacity > 4096)
      throw new RangeError('Timeout diagnostic capacity must be between 1 and 4096')
    this.capacity = capacity
    this.live = new Map()
    this.resources = new Map()
    this.triggers = new Map()
    this.resourceDropped = 0
    this.created = 0
    this.destroyed = 0
    this.lastDestroyedAt = null
    this.dropped = 0
    this.hook = createHook({
      init: (id, type, triggerAsyncId) => {
        const createdAt = type === 'Timeout' ? performance.now() : null
        let creation
        if (this.resources.size < this.capacity) {
          creation = creationStack()
          this.resources.set(id, { type, ...creation })
        } else {
          this.resourceDropped++
        }
        if (type !== 'Timeout') return
        this.created++
        if (this.live.size >= this.capacity) {
          this.dropped++
          return
        }
        const trigger = this.triggers.get(triggerAsyncId)
        if (trigger) {
          trigger.timers++
        } else {
          const parent = this.resources.get(triggerAsyncId)
          this.triggers.set(triggerAsyncId, {
            type: parent?.type ?? null,
            stack: parent?.stack ?? '',
            source: parent?.source ?? null,
            timers: 1,
          })
        }
        this.live.set(id, {
          id,
          triggerAsyncId,
          createdAt,
          destroyedAt: null,
          ...(creation ?? creationStack()),
        })
      },
      destroy: (id) => {
        this.resources.delete(id)
        const timer = this.live.get(id)
        if (!timer) return
        this.lastDestroyedAt = performance.now()
        this.destroyed++
        this.live.delete(id)
        const trigger = this.triggers.get(timer.triggerAsyncId)
        if (--trigger.timers === 0) this.triggers.delete(timer.triggerAsyncId)
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
    this.resources.clear()
    this.triggers.clear()
  }

  /**
   * @returns {{created:number, destroyed:number, lastDestroyedAt:number|null, dropped:number, resourceDropped:number, live:object[]}}
   *   copied scalar facts for live timers: stack/source describe timer creation; triggerType,
   *   triggerStack and triggerSource describe the trigger's creation, even after its destruction.
   *   Null trigger type/source means its creation was outside the bounded observation history.
   *   Timer and resource-history drops are counted separately and never alter the resource verdict.
   */
  read() {
    return {
      created: this.created,
      destroyed: this.destroyed,
      lastDestroyedAt: this.lastDestroyedAt,
      dropped: this.dropped,
      resourceDropped: this.resourceDropped,
      live: [...this.live.values()]
        .map((fact) => {
          const trigger = this.triggers.get(fact.triggerAsyncId)
          return {
            ...fact,
            triggerType: trigger.type,
            triggerStack: trigger.stack,
            triggerSource: trigger.source,
          }
        })
        .sort((a, b) => a.id - b.id),
    }
  }
}

module.exports = { TimeoutDiagnostics }
