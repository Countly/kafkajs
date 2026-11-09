'use strict'

/** Weak-only development observation; neither restoration tables nor journals hold observed targets. */
class WeakOwners {
  /** @param {number} [capacity] finite issued-target capacity @returns {WeakOwners} empty observer */
  constructor(capacity = 32768) {
    this.capacity = capacity
    this.ids = new WeakMap()
    this.rows = []
    this.restores = []
  }

  /** @param {object} target actual production value @param {string} kind source-owned category @returns {number} stable issued ID @throws {Error} on missing target or capacity overflow */
  issue(target, kind) {
    if (!target || !['object', 'function'].includes(typeof target))
      throw new Error('missing target issuance')
    if (this.ids.has(target)) return this.ids.get(target)
    if (this.rows.length >= this.capacity) throw new Error('weak observation overflow')
    const id = this.rows.length + 1
    this.ids.set(target, id)
    this.rows.push({ id, kind, weak: new WeakRef(target) })
    return id
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

  /** @returns {object[]} scalar results; call once after distinct jobs and completed real GC, never in a polling loop */
  read() {
    return this.rows.map(({ id, kind, weak }) => ({ id, kind, alive: weak.deref() !== undefined }))
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
