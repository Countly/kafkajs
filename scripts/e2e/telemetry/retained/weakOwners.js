'use strict'

/**
 * Weak-only development observation with scalar history reserved before the first checkpoint.
 * Neither restoration tables nor journals hold observed targets. WeakRef wrappers for dead
 * targets retire at each read; an issued ID and its final dead result remain in the history.
 */
class WeakOwners {
  /** @param {number} [capacity] finite issued-target capacity @returns {WeakOwners} empty observer */
  constructor(capacity = 32768) {
    this.capacity = capacity
    this.ids = new WeakMap()
    this.count = 0
    this.rows = Array.from({ length: capacity }, (_, index) => ({
      id: index + 1,
      kind: null,
      weak: null,
    }))
    this.restores = []
  }

  /** @param {object} target actual production value @param {string} kind source-owned category @returns {number} stable issued ID @throws {Error} on missing target or capacity overflow */
  issue(target, kind) {
    if (!target || !['object', 'function'].includes(typeof target))
      throw new Error('missing target issuance')
    if (this.ids.has(target)) return this.ids.get(target)
    if (this.count >= this.capacity) throw new Error('weak observation overflow')
    const row = this.rows[this.count++]
    row.kind = kind
    row.weak = new WeakRef(target)
    this.ids.set(target, row.id)
    return row.id
  }

  /** @param {object} prototype production prototype @param {string} method synchronous factory @param {string} kind result category @param {string[]} [stateFields] source-owned episode fields @returns {void} wrapper returns exactly the original result */
  observe(prototype, method, kind, stateFields = []) {
    const original = prototype[method]
    const observer = this
    prototype[method] = function (...args) {
      const value = original.apply(this, args)
      observer.issue(value, kind)
      for (const field of stateFields) {
        const target = this[field]
        if (!target || !['object', 'function'].includes(typeof target)) continue
        observer.issue(target, field)
        for (const name of ['cancel', 'handle', 'subscription']) {
          const child = target[name]
          if (child && ['object', 'function'].includes(typeof child))
            observer.issue(child, `${field}:${name}`)
        }
      }
      if (Array.isArray(value?.points))
        for (const point of value.points) observer.issue(point, 'point')
      return value
    }
    this.restores.push(() => {
      prototype[method] = original
    })
  }

  /**
   * Archives dead observations without keeping their WeakRef wrappers. IDs and kinds survive in
   * the slots reserved at construction; live targets remain observable at subsequent checkpoints.
   * @returns {object[]} scalar results for issued targets only; call after distinct jobs and real GC
   */
  read() {
    const observations = []
    for (let index = 0; index < this.count; index++) {
      const row = this.rows[index]
      const alive = row.weak !== null && row.weak.deref() !== undefined
      if (!alive) row.weak = null
      observations.push({ id: row.id, kind: row.kind, alive })
    }
    return observations
  }

  /** @returns {object} diagnostic inventory; WeakMap entries are not enumerable, so issuance is an upper bound */
  inventory() {
    return {
      capacity: this.capacity,
      issued: this.count,
      weakRefs: this.rows.reduce((count, row) => count + (row.weak === null ? 0 : 1), 0),
      weakMaps: 1,
      weakMapEntriesUpperBound: this.count,
      maps: 0,
      rowSlots: this.rows.length,
      restorationSlots: this.restores.length,
      buffers: 0,
      byteLength: 0,
    }
  }

  /** @param {string[]} required issued categories @returns {object} missing issuance or live targets never certify release */
  retirement(required) {
    const observations = this.read()
    const missing = required.filter((kind) => !observations.some((row) => row.kind === kind))
    return {
      outcome: missing.length
        ? 'incomplete'
        : observations.some((row) => row.alive)
          ? 'unreleased'
          : 'released',
      missing,
      observations,
    }
  }

  /** @returns {void} restore only module functions; never target objects */
  restore() {
    for (const restore of this.restores.splice(0)) restore()
  }
}
module.exports = { WeakOwners }
