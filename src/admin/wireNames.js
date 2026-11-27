const Encoder = require('../protocol/encoder')

/**
 * An independent copy of plain data: arrays, plain objects and Buffers are copied at every depth,
 * and any other value (a string, a number, a boolean, null, a Long) is returned as it is, because
 * it cannot be changed through the copy. Fields are not listed, so a field added to a description
 * is copied as long as it is made of these.
 * @param {any} value
 * @returns {any}
 */
const copyOf = (value) => {
  if (Buffer.isBuffer(value)) return Buffer.from(value)
  if (Array.isArray(value)) return value.map(copyOf)
  if (value !== null && typeof value === 'object') {
    const prototype = Object.getPrototypeOf(value)
    if (prototype === Object.prototype || prototype === null) {
      return Object.fromEntries(Object.entries(value).map(([key, field]) => [key, copyOf(field)]))
    }
  }
  return value
}

/**
 * The names a caller gave in one call and the distinct names a request writes for them.
 *
 * A lone surrogate is written as U+FFFD, so a broker answers for the written name, which can be the
 * same for several names the caller gave (a lone surrogate and a literal U+FFFD, or two different
 * lone surrogates) and is the same for a name given twice. A call sends, routes, retries and
 * matches each written name ONCE (`ids`), settles one outcome for it, and only then gives that
 * outcome to the caller's spellings with `project`.
 *
 * A call whose names are all well formed and distinct builds no per-name lookups and renames no
 * row: it holds one copy of the array and one Set that finds the repeats, and `project` hands the
 * row back as it is. The per-name lookups of `project` are built when it is first used with
 * a repeated or rewritten name.
 */
class WireNames {
  #occurrences = null
  #spellings = null

  /**
   * @param {string[]} names the names as the caller gave them; copied, so a later change of the
   *   caller's array does not change the call
   */
  constructor(names) {
    /** @type {string[]} the caller's names, in call order, repeats kept */
    this.names = names.slice()
    const changed = this.names.some((name) => Encoder.sentForm(name) !== name)
    /** @type {string[]} the written form of each name, parallel to `names` */
    this.wire = changed ? this.names.map((name) => Encoder.sentForm(name)) : this.names
    const distinct = new Set(this.wire)
    /** @type {boolean} whether every name is written as it is and no name is repeated */
    this.plain = !changed && distinct.size === this.names.length
    /** @type {string[]} each written name once, in the order it first appears */
    this.ids = this.plain ? this.wire : [...distinct]
  }

  /**
   * Builds, once, the caller's names for each written name.
   * @returns {void}
   */
  #index() {
    if (this.#occurrences !== null) return
    this.#occurrences = new Map()
    this.names.forEach((name, index) => {
      const callers = this.#occurrences.get(this.wire[index])
      if (callers) callers.push(name)
      else this.#occurrences.set(this.wire[index], [name])
    })
    this.#spellings = new Map(
      [...this.#occurrences].map(([id, callers]) => [id, [...new Set(callers)]])
    )
  }

  /**
   * Adds the rows for one written name's outcome, one per name the caller gave for it: one per
   * occurrence in call order (`repeats`), or one per distinct spelling in the order each first
   * appears. A row already under the caller's spelling is added as it is, the first time only; every
   * other row is an independent copy of it (nested arrays, objects and Buffers included) under that
   * spelling, so changing one row never changes another and no spelling is renamed to another.
   * A row for a name the caller did not give is added as it is.
   * @param {object[]} out receives the rows
   * @param {string} wireId the written name the row answers
   * @param {{ groupId: string }} row the one outcome for `wireId`; not changed
   * @param {boolean} repeats whether a name given twice gets two rows
   * @returns {void}
   */
  project(out, wireId, row, repeats) {
    let callers
    if (!this.plain) {
      this.#index()
      callers = (repeats ? this.#occurrences : this.#spellings).get(wireId)
    }
    if (callers === undefined) {
      out.push(row)
      return
    }
    let used = false
    for (const groupId of callers) {
      if (groupId === row.groupId && !used) {
        used = true
        out.push(row)
      } else {
        const copy = copyOf(row)
        copy.groupId = groupId
        out.push(copy)
      }
    }
  }
}

module.exports = { WireNames, copyOf }
