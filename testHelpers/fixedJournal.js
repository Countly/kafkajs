'use strict'

/** Tags for an internal, lossless scalar archive. Non-integral numbers and -0 retain their exact IEEE Float64 representation. */
const TAG = Object.freeze({
  undefined: 0,
  null: 1,
  false: 2,
  true: 3,
  number: 4,
  string: 5,
  array: 6,
  object: 7,
  columns: 8,
  utf16: 9,
  int32: 10,
  int16: 11,
  symbol: 12,
  digest: 13,
  int64: 14,
})

/** Fixed field names and bounded byte-only symbols for the share fact schema. */
const COLUMNS = Object.freeze([
  'n',
  'segment',
  'socket',
  'apiKey',
  'apiVersion',
  'correlationId',
  'at',
  'responseAt',
  'requestSha256',
  'responseSha256',
  'request',
  'response',
  'groupId',
  'isRenewAck',
  'topics',
  'topicId',
  'partitions',
  'partition',
  'batches',
  'firstOffset',
  'lastOffset',
  'types',
  'errorCode',
  'lockMs',
  'fetchErrorCode',
  'acknowledgeErrorCode',
  'acquired',
  'deliveryCount',
  'topic',
  'type',
  'error',
  'name',
  'reason',
])
const SYMBOL_FIELDS = new Set(['segment', 'groupId', 'topicId', 'topic', 'type', 'name', 'reason'])
const SYMBOL_SLOTS = 64
const SYMBOL_BYTES = SYMBOL_SLOTS * 256
/** Share fields that the packed layout stores as raw bytes when the text is exactly reconstructible. */
const DIGEST_FIELDS = new Set(['requestSha256', 'responseSha256'])
const OFFSET_FIELDS = new Set(['firstOffset', 'lastOffset'])
const DIGEST = /^[0-9a-f]{64}$/
const INT64 = /^(?:0|-?[1-9][0-9]{0,18})$/
const INT64_LIMIT = 2n ** 63n
/** Share layouts: field codes with label slots, and the same with raw digests and int64 offsets. */
const COLUMNS_LAYOUT = 'columns'
const PACKED_LAYOUT = 'packed'

/** Format names by layout, as reported in the storage evidence. */
const FORMATS = Object.freeze({
  [COLUMNS_LAYOUT]: 'share-columns-utf8-1',
  [PACKED_LAYOUT]: 'share-packed-1',
  plain: 'scalar-utf8-1',
})

/** @param {string} value arbitrary JS string @returns {string} its lossless byte encoding */
const stringEncoding = (value) => (value.isWellFormed() ? 'utf8' : 'utf16le')

/**
 * @param {string} value text @returns {bigint|null} its value when the text is the canonical decimal
 *   of an int64, so that `String(result) === value`; otherwise null
 */
const int64Of = (value) => {
  if (!INT64.test(value)) return null
  const exact = BigInt(value)
  return exact >= -INT64_LIMIT && exact < INT64_LIMIT ? exact : null
}

/**
 * @param {string} value a well-formed string fact @param {string|null} key its containing field
 * @returns {number|null} `TAG.digest` for lower-case SHA-256 hex or `TAG.int64` for a canonical
 *   int64 offset under their share fields, otherwise null: the ordinary string encoding is exact
 */
const packedTag = (value, key) => {
  if (DIGEST_FIELDS.has(key)) return DIGEST.test(value) ? TAG.digest : null
  if (OFFSET_FIELDS.has(key)) return int64Of(value) === null ? null : TAG.int64
  return null
}

/** @param {number} value scalar @returns {number} smallest exact numeric tag, preserving -0 */
const numberTag = (value) => {
  if (!Number.isInteger(value) || Object.is(value, -0)) return TAG.number
  if (value >= -32768 && value <= 32767) return TAG.int16
  if (value >= -2147483648 && value <= 2147483647) return TAG.int32
  return TAG.number
}

/**
 * Counts the bytes before any write, rejecting product instances and cyclic or excessive nesting.
 * @param {*} value scalar, array or plain fact object @param {number} [depth] nesting level
 * @param {string|null} [layout] share layout, or null for plain scalars
 * @param {string|null} [key] containing field
 * @param {Set<string>|null} [symbols] temporary preflight labels, never retained
 * @returns {number} exact encoded length @throws {Error} for a non-fact value
 */
const encodedSize = (value, depth = 0, layout = null, key = null, symbols = null) => {
  if (depth > 64) throw new Error('Journal facts are cyclic or too deeply nested')
  if (value == null || typeof value === 'boolean') return 1
  if (typeof value === 'number')
    return numberTag(value) === TAG.int16 ? 3 : numberTag(value) === TAG.int32 ? 5 : 9
  if (typeof value === 'string') {
    const tag = layout === PACKED_LAYOUT ? packedTag(value, key) : null
    if (tag) return tag === TAG.digest ? 33 : 9
    const bytes = Buffer.byteLength(value, stringEncoding(value))
    if (layout && SYMBOL_FIELDS.has(key) && bytes <= 249) {
      symbols?.add(value)
      return 3
    }
    return 5 + bytes
  }
  if (typeof value !== 'object') throw new Error('Journal accepts scalars and plain facts only')
  if (Array.isArray(value))
    return (
      5 + value.reduce((sum, item) => sum + encodedSize(item, depth + 1, layout, null, symbols), 0)
    )
  if (![Object.prototype, null].includes(Object.getPrototypeOf(value)))
    throw new Error('Journal cannot retain a product instance')
  return (
    5 +
    Object.keys(value).reduce(
      (sum, key) =>
        sum +
        (layout
          ? 1 + (COLUMNS.includes(key) ? 0 : encodedSize(key, depth + 1))
          : encodedSize(key, depth + 1)) +
        encodedSize(value[key], depth + 1, layout, key, symbols),
      0
    )
  )
}

/** Fixed, non-zero-prefilled byte storage; appends copy facts, never retain their objects/strings. */
class FixedJournal {
  /**
   * @param {object} options @param {string} options.name overflow label
   * @param {number} options.capacity maximum entries @param {number} options.bytes arena capacity
   * @param {boolean} [options.columns] encode field names and repeated share labels in fixed columns
   * @param {boolean} [options.packed] also store lower-case SHA-256 hex and canonical int64 offset
   *   strings of the share fields as raw 32 and 8 bytes; implies `columns`, and any other string
   *   keeps its ordinary lossless encoding
   * @returns {FixedJournal} fully touched storage, to allocate before the measurement baseline
   */
  constructor({ name, capacity, bytes, columns = false, packed = false }) {
    if (
      !Number.isSafeInteger(capacity) ||
      capacity < 1 ||
      !Number.isSafeInteger(bytes) ||
      bytes < 1
    )
      throw new Error('Journal needs positive fixed capacities')
    const layout = packed ? PACKED_LAYOUT : columns ? COLUMNS_LAYOUT : null
    Object.assign(this, { name, capacity, layout, arenaBytes: bytes })
    this.storage = Buffer.alloc(bytes + (layout ? SYMBOL_BYTES : 0), 0xa5)
    this.symbols = 0
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
    const symbols = this.layout ? new Set() : null
    const bytes = entries.reduce(
      (sum, entry) => sum + 4 + encodedSize(entry, 0, this.layout, null, symbols),
      0
    )
    const newSymbols = symbols
      ? [...symbols].filter((value) => this.symbolIndex(value) < 0).length
      : 0
    if (this.symbols + newSymbols > SYMBOL_SLOTS) {
      this.overflow = true
      throw new Error(`${this.name} journal symbol overflow`)
    }
    if (this.length + entries.length > this.capacity || this.cursor + bytes > this.arenaBytes) {
      this.overflow = true
      throw new Error(
        `${this.name} journal overflow (rows ${this.length}/${this.capacity}, bytes ${this.cursor}+${bytes}/${this.byteLength})`
      )
    }
    for (const entry of entries) {
      const size = encodedSize(entry, 0, this.layout)
      this.storage.writeUInt32LE(size, this.cursor)
      this.cursor += 4
      this.write(entry)
      this.length++
    }
    return this.length
  }

  /**
   * @private @param {*} value preflighted fact @param {string|null} [key] containing field
   * @returns {void} writes only into reserved space
   */
  write(value, key = null) {
    let tag =
      value === null
        ? TAG.null
        : Array.isArray(value)
          ? TAG.array
          : typeof value === 'boolean'
            ? TAG[String(value)]
            : TAG[typeof value]
    if (tag === TAG.number) tag = numberTag(value)
    if (tag === TAG.object && this.layout) tag = TAG.columns
    if (tag === TAG.string && stringEncoding(value) === 'utf16le') tag = TAG.utf16
    const packed =
      this.layout === PACKED_LAYOUT && tag === TAG.string ? packedTag(value, key) : null
    if (packed === TAG.digest) {
      this.storage[this.cursor++] = TAG.digest
      this.cursor += this.storage.write(value, this.cursor, 32, 'hex')
      return
    }
    if (packed === TAG.int64) {
      this.storage[this.cursor++] = TAG.int64
      this.cursor = this.storage.writeBigInt64LE(int64Of(value), this.cursor)
      return
    }
    if (
      (tag === TAG.string || tag === TAG.utf16) &&
      this.layout &&
      SYMBOL_FIELDS.has(key) &&
      Buffer.byteLength(value, stringEncoding(value)) <= 249
    ) {
      this.storage[this.cursor++] = TAG.symbol
      this.storage.writeUInt16LE(this.symbol(value), this.cursor)
      this.cursor += 2
      return
    }
    this.storage[this.cursor++] = tag
    if (tag === TAG.int16 || tag === TAG.int32) {
      if (tag === TAG.int16) this.storage.writeInt16LE(value, this.cursor)
      else this.storage.writeInt32LE(value, this.cursor)
      this.cursor += tag === TAG.int16 ? 2 : 4
    } else if (tag === TAG.number) {
      this.storage.writeDoubleLE(value, this.cursor)
      this.cursor += 8
    } else if (tag === TAG.string || tag === TAG.utf16) {
      const encoding = tag === TAG.string ? 'utf8' : 'utf16le'
      const size = Buffer.byteLength(value, encoding)
      this.storage.writeUInt32LE(size, this.cursor)
      this.cursor += 4
      this.storage.write(value, this.cursor, size, encoding)
      this.cursor += size
    } else if (tag === TAG.array || tag === TAG.object || tag === TAG.columns) {
      const keys = tag === TAG.array ? null : Object.keys(value)
      this.storage.writeUInt32LE(keys ? keys.length : value.length, this.cursor)
      this.cursor += 4
      if (keys)
        for (const key of keys) {
          if (tag === TAG.columns) this.storage[this.cursor++] = COLUMNS.indexOf(key) + 1
          if (tag !== TAG.columns || !COLUMNS.includes(key)) this.write(key)
          this.write(value[key], key)
        }
      else for (const item of value) this.write(item)
    }
  }

  /**
   * Finds a label by its encoded bytes without retaining a JS string.
   * @private @param {string} value label @returns {number} slot, or -1 if absent
   */
  symbolIndex(value) {
    const encoding = stringEncoding(value)
    const length = Buffer.byteLength(value, encoding)
    for (let index = 0; index < this.symbols; index++) {
      const start = this.arenaBytes + index * 256
      if (
        this.storage.readUInt16LE(start) === length &&
        this.storage[start + 2] === (encoding === 'utf8' ? 0 : 1) &&
        this.storage.toString(encoding, start + 3, start + 3 + length) === value
      )
        return index
    }
    return -1
  }

  /**
   * Interns schema label bytes in fixed slots after preflight, retaining no string references.
   * @private @param {string} value label @returns {number} slot @throws {Error} on exhaustion
   */
  symbol(value) {
    const existing = this.symbolIndex(value)
    if (existing >= 0) return existing
    const encoding = stringEncoding(value)
    const length = Buffer.byteLength(value, encoding)
    if (this.symbols === SYMBOL_SLOTS) {
      this.overflow = true
      throw new Error(`${this.name} journal symbol overflow`)
    }
    const index = this.symbols++
    const start = this.arenaBytes + index * 256
    this.storage.writeUInt16LE(length, start)
    this.storage[start + 2] = encoding === 'utf8' ? 0 : 1
    this.storage.write(value, start + 3, length, encoding)
    return index
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
      if (tag === TAG.int16 || tag === TAG.int32) {
        const value =
          tag === TAG.int16 ? this.storage.readInt16LE(cursor) : this.storage.readInt32LE(cursor)
        cursor += tag === TAG.int16 ? 2 : 4
        return value
      }
      if (tag === TAG.symbol) {
        const start = this.arenaBytes + this.storage.readUInt16LE(cursor) * 256
        cursor += 2
        const length = this.storage.readUInt16LE(start)
        return this.storage.toString(
          this.storage[start + 2] === 0 ? 'utf8' : 'utf16le',
          start + 3,
          start + 3 + length
        )
      }
      if (tag === TAG.number) {
        const value = this.storage.readDoubleLE(cursor)
        cursor += 8
        return value
      }
      if (tag === TAG.digest) {
        cursor += 32
        return this.storage.toString('hex', cursor - 32, cursor)
      }
      if (tag === TAG.int64) {
        cursor += 8
        return String(this.storage.readBigInt64LE(cursor - 8))
      }
      const length = this.storage.readUInt32LE(cursor)
      cursor += 4
      if (tag === TAG.string || tag === TAG.utf16) {
        const value = this.storage.toString(
          tag === TAG.string ? 'utf8' : 'utf16le',
          cursor,
          cursor + length
        )
        cursor += length
        return value
      }
      const value = tag === TAG.array ? [] : {}
      for (let i = 0; i < length; i++) {
        if (tag === TAG.array) value.push(read())
        else {
          const field = tag === TAG.columns ? this.storage[cursor++] : 0
          const key = field ? COLUMNS[field - 1] : read()
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
      cursor: this.cursor + this.symbols * 256,
      format: FORMATS[this.layout ?? 'plain'],
      symbolCount: this.symbols,
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

module.exports = { FixedJournal, HandlerJournal, JournalConstancy, SYMBOL_BYTES, journalState }
