'use strict'

/** Tags for an internal, lossless scalar archive. Numbers always occupy an IEEE Float64. */
const TAG = Object.freeze({
  undefined: 0,
  null: 1,
  false: 2,
  true: 3,
  number: 4,
  string: 5,
  array: 6,
  object: 7,
})

/**
 * Counts the bytes before any write, rejecting product instances and cyclic or excessive nesting.
 * @param {*} value scalar, array or plain fact object @param {number} [depth] nesting level
 * @returns {number} exact encoded length @throws {Error} for a non-fact value
 */
const encodedSize = (value, depth = 0) => {
  if (depth > 64) throw new Error('Journal facts are cyclic or too deeply nested')
  if (value == null || typeof value === 'boolean') return 1
  if (typeof value === 'number') return 9
  if (typeof value === 'string') return 5 + Buffer.byteLength(value, 'utf16le')
  if (typeof value !== 'object') throw new Error('Journal accepts scalars and plain facts only')
  if (Array.isArray(value))
    return 5 + value.reduce((sum, item) => sum + encodedSize(item, depth + 1), 0)
  if (![Object.prototype, null].includes(Object.getPrototypeOf(value)))
    throw new Error('Journal cannot retain a product instance')
  return (
    5 +
    Object.keys(value).reduce(
      (sum, key) => sum + encodedSize(key, depth + 1) + encodedSize(value[key], depth + 1),
      0
    )
  )
}

/** Fixed, non-zero-prefilled byte storage; appends copy facts, never retain their objects/strings. */
class FixedJournal {
  /**
   * @param {object} options @param {string} options.name overflow label
   * @param {number} options.capacity maximum entries @param {number} options.bytes arena capacity
   * @returns {FixedJournal} fully touched storage, to allocate before the measurement baseline
   */
  constructor({ name, capacity, bytes }) {
    if (
      !Number.isSafeInteger(capacity) ||
      capacity < 1 ||
      !Number.isSafeInteger(bytes) ||
      bytes < 1
    )
      throw new Error('Journal needs positive fixed capacities')
    Object.assign(this, { name, capacity })
    this.storage = Buffer.alloc(bytes, 0xa5)
    this.cursor = 0
    this.length = 0
    this.overflow = false
  }

  /** @returns {number} the physical byte capacity, never the used prefix */
  get byteLength() {
    return this.storage.byteLength
  }

  /**
   * Copies entries atomically after checking both bounds; no wrap, truncation or retained reference.
   * @param {...*} entries scalar facts @returns {number} the new entry count
   * @throws {Error} on capacity exhaustion or a non-fact value, before writing anything
   */
  push(...entries) {
    const bytes = entries.reduce((sum, entry) => sum + 4 + encodedSize(entry), 0)
    if (this.length + entries.length > this.capacity || this.cursor + bytes > this.byteLength) {
      this.overflow = true
      throw new Error(
        `${this.name} journal overflow (rows ${this.length}/${this.capacity}, bytes ${this.cursor}+${bytes}/${this.byteLength})`
      )
    }
    for (const entry of entries) {
      const size = encodedSize(entry)
      this.storage.writeUInt32LE(size, this.cursor)
      this.cursor += 4
      this.write(entry)
      this.length++
    }
    return this.length
  }

  /**
   * @private @param {*} value preflighted fact @returns {void} writes only into reserved space
   */
  write(value) {
    const tag =
      value === null
        ? TAG.null
        : Array.isArray(value)
          ? TAG.array
          : typeof value === 'boolean'
            ? TAG[String(value)]
            : TAG[typeof value]
    this.storage[this.cursor++] = tag
    if (tag === TAG.number) {
      this.storage.writeDoubleLE(value, this.cursor)
      this.cursor += 8
    } else if (tag === TAG.string) {
      const size = Buffer.byteLength(value, 'utf16le')
      this.storage.writeUInt32LE(size, this.cursor)
      this.cursor += 4
      this.storage.write(value, this.cursor, size, 'utf16le')
      this.cursor += size
    } else if (tag === TAG.array || tag === TAG.object) {
      const keys = tag === TAG.array ? null : Object.keys(value)
      this.storage.writeUInt32LE(keys ? keys.length : value.length, this.cursor)
      this.cursor += 4
      if (keys)
        for (const key of keys) {
          this.write(key)
          this.write(value[key])
        }
      else for (const item of value) this.write(item)
    }
  }

  /**
   * Materialises only for post-reading archives; it never releases or resizes the storage.
   * @returns {Array<*>} lossless facts in original order, including undefined object properties
   */
  values() {
    let cursor = 0
    const read = () => {
      const tag = this.storage[cursor++]
      if (tag === TAG.undefined) return undefined
      if (tag === TAG.null) return null
      if (tag === TAG.false) return false
      if (tag === TAG.true) return true
      if (tag === TAG.number) {
        const value = this.storage.readDoubleLE(cursor)
        cursor += 8
        return value
      }
      const length = this.storage.readUInt32LE(cursor)
      cursor += 4
      if (tag === TAG.string) {
        const value = this.storage.toString('utf16le', cursor, cursor + length)
        cursor += length
        return value
      }
      const value = tag === TAG.array ? [] : {}
      for (let i = 0; i < length; i++) {
        if (tag === TAG.array) value.push(read())
        else {
          const key = read()
          Object.defineProperty(value, key, {
            value: read(),
            enumerable: true,
            writable: true,
            configurable: true,
          })
        }
      }
      return value
    }
    return Array.from({ length: this.length }, () => {
      cursor += 4
      return read()
    })
  }

  /** @returns {{byteLength:number,cursor:number,length:number,capacity:number,overflow:boolean}} scalar evidence */
  journalState() {
    return {
      byteLength: this.byteLength,
      cursor: this.cursor,
      length: this.length,
      capacity: this.capacity,
      overflow: this.overflow,
    }
  }
}

/** Fixed handler columns: partition/index Int32, offset int64, entry/exit Float64, plus seen bits. */
class HandlerJournal {
  /** @param {number} records finite corpus @param {number} capacity corpus plus duplicate margin */
  constructor(records, capacity) {
    this.capacity = capacity
    this.storage = Buffer.alloc(capacity * 32, 0xa5)
    this.seen = Buffer.alloc(records, 0xa5)
    this.length = 0
    this.unique = 0
    this.overflow = false
  }

  /**
   * @param {number} partition partition @param {string} offset int64 offset
   * @param {number} index corpus index @param {number} at Float64 clock
   * @returns {number} the row to finish @throws {Error} on overflow or an invalid int64 offset
   */
  enter(partition, offset, index, at) {
    if (this.length === this.capacity) {
      this.overflow = true
      throw new Error('handler journal overflow')
    }
    const exact = BigInt(offset)
    if (exact < -(1n << 63n) || exact >= 1n << 63n)
      throw new Error('handler offset is outside int64')
    const row = this.length++
    const byte = row * 32
    this.storage.writeInt32LE(partition, byte)
    this.storage.writeInt32LE(index, byte + 4)
    this.storage.writeBigInt64LE(exact, byte + 8)
    this.storage.writeDoubleLE(at, byte + 16)
    this.storage.writeDoubleLE(NaN, byte + 24)
    if (index >= 0 && index < this.seen.length && this.seen[index] !== 1) {
      this.seen[index] = 1
      this.unique++
    }
    return row
  }

  /** @param {number} row admitted row @param {number} at Float64 exit clock @returns {void} */
  exit(row, at) {
    this.storage.writeDoubleLE(at, row * 32 + 24)
  }

  /** @returns {Array<number[]>} original public archive shape, after the final reading only */
  values() {
    return Array.from({ length: this.length }, (_, row) => {
      const byte = row * 32
      const exited = this.storage.readDoubleLE(byte + 24)
      return [
        this.storage.readInt32LE(byte),
        Number(this.storage.readBigInt64LE(byte + 8)),
        this.storage.readInt32LE(byte + 4),
        this.storage.readDoubleLE(byte + 16),
        Number.isNaN(exited) ? null : exited,
      ]
    })
  }

  /** @returns {object} capacity and progress, counting the fixed seen bitmap in both sizes */
  journalState() {
    return {
      byteLength: this.storage.byteLength + this.seen.byteLength,
      cursor: this.length * 32 + this.seen.byteLength,
      length: this.length,
      capacity: this.capacity,
      overflow: this.overflow,
    }
  }
}

/**
 * Adds sizes without holding any new journal or fact population.
 * @param {Array<{journalState:Function}>} journals fixed storage owners @returns {object} evidence
 */
const journalState = (journals) => {
  const columns = journals.map((journal) => journal.journalState())
  return {
    ...columns.reduce(
      (sum, state) => ({
        byteLength: sum.byteLength + state.byteLength,
        cursor: sum.cursor + state.cursor,
      }),
      { byteLength: 0, cursor: 0 }
    ),
    columns,
  }
}

/** A separate harness integrity check; resource limits and their numerical verdict stay untouched. */
class JournalConstancy {
  /**
   * @param {object} input original M, every sample and closed reading
   * @returns {string[]} integrity failures, never a subtraction from the measured values
   */
  static violations({ baseline, samples = [], teardown }) {
    const snapshots = [baseline, ...samples.map((sample) => sample.snapshot), teardown]
    const violations = new Set()
    let original = null
    let previous = null
    const flatten = (state, path = 'journal', rows = {}) => {
      rows[path] = state
      for (const [name, child] of Object.entries(state?.owners ?? {}))
        flatten(child, `${path}.${name}`, rows)
      for (const [index, child] of (state?.columns ?? []).entries())
        flatten(child, `${path}.${index}`, rows)
      return rows
    }
    for (const snapshot of snapshots) {
      const rows = flatten(snapshot?.journals)
      original ??= rows
      if (JSON.stringify(Object.keys(rows)) !== JSON.stringify(Object.keys(original)))
        violations.add('journal storage constancy')
      for (const [name, state] of Object.entries(rows)) {
        if (state?.overflow) violations.add('journal overflow')
        const bytes = original[name]?.byteLength
        if (!Number.isSafeInteger(bytes) || bytes < 1 || state?.byteLength !== bytes)
          violations.add('journal storage constancy')
        if (
          !state ||
          !Number.isSafeInteger(state.cursor) ||
          state.cursor < (previous?.[name]?.cursor ?? 0) ||
          state.cursor > bytes
        )
          violations.add('journal cursor bounds')
      }
      previous = rows
    }
    return [...violations]
  }

  /**
   * @param {object} verdict unchanged resource verdict @param {object} input journal evidence
   * @returns {object} resource verdict conjoined with harness integrity
   */
  static apply(verdict, input) {
    const violations = [...verdict.violations, ...this.violations(input)]
    return violations.length ? { ...verdict, ok: false, outcome: 'failed', violations } : verdict
  }
}

module.exports = { FixedJournal, HandlerJournal, JournalConstancy, journalState }
