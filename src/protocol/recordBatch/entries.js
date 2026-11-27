const { isProxy } = require('util').types
const Long = require('../../utils/long')
const {
  KafkaJSInvalidConfigurationError,
  KafkaJSInvariantViolation,
  KafkaJSNonRetriableError,
} = require('../../errors')
const { uvarintSize, writeUVarint } = require('./varint')

/**
 * Records as bytes the library owns. An entry is one record:
 *
 * ```
 * entry = [timestamp int64 BE][tail length uint32 BE][call position uint32 BE][tail]
 * tail  = varint(keyLen | -1) key  varint(valueLen | -1) value  varint(headerCount)
 *         ( varint(nameLen) name  varint(headerValueLen) headerValue )*
 * ```
 *
 * The tail is byte for byte what a v2 record carries after its offset delta, so a batch writer
 * only adds each record's length, attributes and deltas. The timestamp is always concrete.
 */

/** The bytes of an entry before its tail. */
const ENTRY_HEADER = 16
const TWO_TO_32 = 4294967296
const MAX_TIMESTAMP = 2n ** 63n - 1n
const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER)

const TypedArray = Object.getPrototypeOf(Uint8Array.prototype)
const intrinsic = (name) => Object.getOwnPropertyDescriptor(TypedArray, name).get
const toStringTag = intrinsic(Symbol.toStringTag)
const byteOffset = intrinsic('byteOffset')
const byteLength = intrinsic('byteLength')
const backingStore = intrinsic('buffer')
const { at, set } = TypedArray
const getTime = Date.prototype.getTime
const Bytes = Uint8Array

const CHANGED = 'A Buffer changed size while its send was being read'
/** Marks a `RecordEntries` in every loaded copy of this module. */
const BRAND = Symbol.for('@countly/kafkajs.RecordEntries')
/**
 * The key under which the producer hands `Broker#produce` a partition's records already taken
 * over, beside the plain `messages` it keeps for everything else.
 */
const OWNED_RECORDS = Symbol('owned records')

/** What each field of a record must be. */
const ENCODABLE = {
  key: 'a string, a Buffer or null',
  value: 'a string, a Buffer or null',
  headers: 'an object whose values are strings, Buffers or arrays of them',
  timestamp:
    'an integer from 0 to 2^63 - 1: a number, a numeric string, a Date, a BigInt or a Long',
}

/**
 * The language's own typed-array validation, without reading an element
 * (`%TypedArray%.prototype.at`): it throws for a view whose store is detached or that lies out of
 * bounds of a resized store, empty or not, and never for a view over a shared store.
 * @param {Uint8Array} view a genuine typed array
 * @param {string} [message]
 * @returns {void}
 * @throws {KafkaJSNonRetriableError} when the view cannot be read
 */
const probe = (view, message = CHANGED) => {
  try {
    at.call(view, Infinity)
  } catch {
    throw new KafkaJSNonRetriableError(message)
  }
}

/**
 * @param {*} value
 * @returns {boolean} whether it is a Buffer whose intrinsics work: a `Proxy` of one is not
 */
const isGenuineBuffer = (value) =>
  Buffer.isBuffer(value) && toStringTag.call(value) === 'Uint8Array'

/**
 * @param {Uint8Array} view a genuine typed array
 * @returns {number} its intrinsic length, which an own `length` cannot change
 */
const lengthOf = (view) => byteLength.call(view)

/**
 * @param {Buffer} buffer
 * @param {number} cursor where writing ended
 * @returns {void}
 * @throws {KafkaJSInvariantViolation} when the writes did not fill `buffer` exactly
 */
const assertWritten = (buffer, cursor) => {
  if (cursor !== buffer.length) {
    throw new KafkaJSInvariantViolation(`${cursor} of ${buffer.length} bytes were written`)
  }
}

/**
 * @param {number} index
 * @param {keyof ENCODABLE} field
 * @returns {KafkaJSInvalidConfigurationError}
 */
const refusal = (index, field) =>
  new KafkaJSInvalidConfigurationError(
    `Invalid message at index ${index}: its ${field} must be ${ENCODABLE[field]}`
  )

/**
 * @param {bigint} value
 * @returns {number|bigint|null} `value` as a number when it is safe, null outside `[0, 2^63 - 1]`
 */
const inRange = (value) =>
  value < 0n || value > MAX_TIMESTAMP ? null : value <= MAX_SAFE ? Number(value) : value

/**
 * @param {*} timestamp neither absent nor a send-time spelling
 * @returns {bigint} its exact value: a `Date` by its time, a library `Long` by its `value`
 * @throws {Error} whatever the one conversion throws
 */
const exactOf = (timestamp) => {
  if (timestamp instanceof Date) {
    return BigInt(getTime.call(timestamp))
  }
  if (timestamp instanceof Long) {
    return BigInt(timestamp.value)
  }
  return BigInt(timestamp)
}

/**
 * A record's timestamp as the exact int64 its batch carries, converted once. `undefined`, `null`,
 * `0`, `-0`, `0n` and `false` are the send time; `true` is 1; a string is parsed as an integer
 * (`''` is refused); a `Date` gives its time, a library `Long` its `value`, any other object what
 * `BigInt` makes of it. A converted 0 is the epoch, not the send time.
 * @param {*} timestamp
 * @param {number} now the send time, a non-negative safe integer
 * @returns {number|bigint|null} a number when safe, a `bigint` above 2^53 - 1, null when it is
 *   not an integer from 0 to 2^63 - 1 or its conversion throws
 */
const timestampOf = (timestamp, now) => {
  if (
    timestamp === undefined ||
    timestamp === null ||
    timestamp === 0 ||
    timestamp === 0n ||
    timestamp === false
  ) {
    return now
  }
  if (Number.isSafeInteger(timestamp) && timestamp > 0) {
    return timestamp
  }
  if (timestamp === true) {
    return 1
  }
  if (timestamp === '') {
    return null
  }
  try {
    return inRange(exactOf(timestamp))
  } catch {
    return null
  }
}

/**
 * @param {object} own the record's own enumerable properties
 * @param {object} message the record
 * @param {string} name
 * @returns {*} the field from `own`, or read once from the record when absent there
 */
const fieldOf = (own, message, name) => (Object.hasOwn(own, name) ? own[name] : message[name])

/**
 * Appends a string or Buffer field to `slots` as its item and byte length. A Buffer is probed
 * before its offset and length are read and kept as a fixed view of its store.
 * @param {Array} slots
 * @param {*} value
 * @returns {number} the bytes it adds to a tail, -1 when it is neither
 */
const addBytes = (slots, value) => {
  let item = value
  let length
  if (typeof value === 'string') {
    length = Buffer.byteLength(value, 'utf8')
  } else if (isGenuineBuffer(value)) {
    probe(value)
    length = byteLength.call(value)
    item = new Bytes(backingStore.call(value), byteOffset.call(value), length)
  } else {
    return -1
  }
  slots.push(item, length)
  return uvarintSize(2 * length) + length
}

/**
 * Appends a key or value, null when absent.
 * @param {Array} slots
 * @param {*} value
 * @param {number} index
 * @param {'key'|'value'} field
 * @returns {number} the bytes it adds to the tail
 * @throws {KafkaJSInvalidConfigurationError}
 */
const addNullable = (slots, value, index, field) => {
  if (value === undefined || value === null) {
    slots.push(null, -1)
    return 1
  }
  const added = addBytes(slots, value)
  if (added < 0) {
    throw refusal(index, field)
  }
  return added
}

/**
 * Appends one header.
 * @param {Array} slots
 * @param {string} name
 * @param {*} value
 * @param {number} index
 * @returns {number} the bytes it adds to the tail
 * @throws {KafkaJSInvalidConfigurationError} when the value is not a string or a Buffer
 */
const addHeader = (slots, name, value, index) => {
  const added = addBytes(slots, name)
  const valueBytes = addBytes(slots, value)
  if (valueBytes < 0) {
    throw refusal(index, 'headers')
  }
  return added + valueBytes
}

/**
 * Appends a record's headers as its count and pairs: each own enumerable key in `Object.keys`
 * order, an array value as one header per element, holes skipped. A string container gives one
 * header per UTF-16 code unit, a number, boolean, `bigint` or symbol none, as the record encoder
 * read them.
 * @param {Array} slots
 * @param {*} headers
 * @param {number} index
 * @returns {number} the bytes they add to the tail
 * @throws {KafkaJSInvalidConfigurationError} for `null` or a value that is not bytes
 */
const addHeaders = (slots, headers, index) => {
  const countAt = slots.length
  slots.push(0)
  if (headers === undefined) {
    return 1
  }
  if (headers === null) {
    throw refusal(index, 'headers')
  }
  let count = 0
  let added = 0
  const names = Object.keys(headers)
  for (let n = 0; n < names.length; n++) {
    const name = names[n]
    const header = headers[name]
    if (!Array.isArray(header)) {
      added += addHeader(slots, name, header, index)
      count += 1
      continue
    }
    for (let i = 0, length = header.length; i < length; i++) {
      if (i in header) {
        added += addHeader(slots, name, header[i], index)
        count += 1
      }
    }
  }
  slots[countAt] = count
  return uvarintSize(2 * count) + added
}

/**
 * Writes a field kept by `addBytes` or `addNullable`. A fixed view is probed again right before
 * its copy, so a store detached or shrunk since its capture refuses the call.
 * @param {Buffer} store
 * @param {number} cursor
 * @param {string|Uint8Array|null} item
 * @param {number} length
 * @returns {number} the cursor after it
 * @throws {KafkaJSNonRetriableError}
 */
const writeField = (store, cursor, item, length) => {
  if (item === null) {
    store[cursor] = 1
    return cursor + 1
  }
  cursor = writeUVarint(store, cursor, 2 * length)
  if (typeof item === 'string') {
    store.write(item, cursor, length, 'utf8')
  } else {
    probe(item)
    set.call(store, item, cursor)
  }
  return cursor + length
}

/**
 * Writes a timestamp as an int64.
 * @param {Buffer} store
 * @param {number} offset
 * @param {number|bigint} timestamp from 0 to 2^63 - 1
 * @returns {void}
 */
const writeTimestamp = (store, offset, timestamp) => {
  if (typeof timestamp === 'bigint') {
    store.writeBigInt64BE(timestamp, offset)
    return
  }
  store.writeUInt32BE(Math.floor(timestamp / TWO_TO_32), offset)
  store.writeUInt32BE(timestamp % TWO_TO_32, offset + 4)
}

/** Reads the varints and byte fields of one tail. */
class TailReader {
  /** @param {Buffer} buffer a tail */
  constructor(buffer) {
    this.buffer = buffer
    this.offset = 0
  }

  /** @returns {number} the next unsigned varint */
  uvarint() {
    let value = 0
    let scale = 1
    let byte
    do {
      byte = this.buffer[this.offset++]
      value += (byte & 0x7f) * scale
      scale *= 0x80
    } while (byte >= 0x80)
    return value
  }

  /** @returns {Buffer|null} the next length-prefixed field, as a view of the tail */
  bytes() {
    const zigzag = this.uvarint()
    if (zigzag === 1) {
      return null
    }
    const end = this.offset + zigzag / 2
    const view = this.buffer.subarray(this.offset, end)
    this.offset = end
    return view
  }
}

/**
 * Adds a decoded header as an own data property; a repeated name becomes an array.
 * @param {object} headers
 * @param {string} name
 * @param {Buffer|null} value
 * @returns {void}
 */
const putHeader = (headers, name, value) => {
  const existing = Object.hasOwn(headers, name) ? headers[name] : undefined
  if (Array.isArray(existing)) {
    existing.push(value)
    return
  }
  Object.defineProperty(headers, name, {
    value: existing === undefined ? value : [existing, value],
    writable: true,
    enumerable: true,
    configurable: true,
  })
}

/**
 * One partition's records as entries in one store the library owns, in order: nothing in it
 * refers to a caller's object. `first` and `max` are the lowest and highest timestamps, the send
 * time for no record.
 */
class RecordEntries {
  /**
   * @param {Buffer} store
   * @param {number[]} starts each entry's offset in `store`
   * @param {number|bigint} first
   * @param {number|bigint} max
   */
  constructor(store, starts, first, max) {
    Object.defineProperty(this, BRAND, { value: true })
    this.store = store
    this.starts = starts
    this.first = first
    this.max = max
  }

  /**
   * Whether `value` is a `RecordEntries`, also one made by another loaded copy of this module (a
   * request codec can be loaded later than the broker that captured the entries), checked without
   * running a getter or a trap: a `Proxy` never is one.
   * @param {*} value
   * @returns {boolean}
   */
  static is(value) {
    return (
      value !== null && typeof value === 'object' && !isProxy(value) && Object.hasOwn(value, BRAND)
    )
  }

  /** @returns {number} how many records */
  get count() {
    return this.starts.length
  }

  /**
   * @param {number} index
   * @returns {number|bigint} the record's timestamp: a `bigint` for every record when `max` is one
   */
  timestamp(index) {
    const start = this.starts[index]
    return typeof this.max === 'bigint'
      ? this.store.readBigInt64BE(start)
      : this.store.readUInt32BE(start) * TWO_TO_32 + this.store.readUInt32BE(start + 4)
  }

  /**
   * @param {number} index
   * @returns {number} the bytes of the record's tail
   */
  tailLength(index) {
    return this.store.readUInt32BE(this.starts[index] + 8)
  }

  /**
   * @param {number} index
   * @returns {number} the bytes of the whole entry
   */
  entryBytes(index) {
    return ENTRY_HEADER + this.tailLength(index)
  }

  /**
   * Copies the record's tail into `target`.
   * @param {number} index
   * @param {Buffer} target
   * @param {number} offset
   * @returns {number} the offset after it
   */
  copyTail(index, target, offset) {
    const start = this.starts[index] + ENTRY_HEADER
    return offset + this.store.copy(target, offset, start, start + this.tailLength(index))
  }

  /**
   * The record as written, detached from this store: its Buffers are views of one fresh store
   * made for this call; a repeated header name gives an array.
   * @param {number} index
   * @returns {{ key: Buffer|null, value: Buffer|null, headers: Object<string, Buffer|Buffer[]>,
   *   timestamp: string }}
   */
  decodeEntry(index) {
    const tail = Buffer.allocUnsafeSlow(this.tailLength(index))
    this.copyTail(index, tail, 0)
    const reader = new TailReader(tail)
    const key = reader.bytes()
    const value = reader.bytes()
    const headers = {}
    for (let count = reader.uvarint() / 2; count > 0; count--) {
      const name = reader.bytes().toString('utf8')
      putHeader(headers, name, reader.bytes())
    }
    const timestamp = this.store.readBigInt64BE(this.starts[index]).toString()
    return { key, value, headers, timestamp }
  }
}

/**
 * Takes a partition's plain records over as entries: one pass reads each record once, a second
 * writes every entry into one store of exactly their size. Each record is read by index from the
 * length read once: one spread of its own enumerable properties, then each of `key`, `value`,
 * `headers` and `timestamp` absent from them read once with an ordinary get (own non-enumerable
 * and inherited fields included; an own enumerable `undefined` is kept). No method or getter of a
 * Buffer runs: each is probed, measured and viewed through the typed-array intrinsics, and its
 * bytes are those visible through that fixed view when it is copied.
 * @param {object[]} messages
 * @param {{ now: number }} options `now` is the send time
 * @returns {RecordEntries}
 * @throws {KafkaJSNonRetriableError} when `messages` is not an array, for a hole or a record that
 *   is not an object, naming its index, and for a Buffer detached or out of bounds at its capture
 *   or its copy
 * @throws {KafkaJSInvalidConfigurationError} for a field that cannot be written, naming the index
 *   and the field, never its value
 */
const captureEntries = (messages, { now }) => {
  if (!Array.isArray(messages)) {
    throw new KafkaJSNonRetriableError('Invalid messages: they must be an array')
  }
  const count = messages.length
  const slots = []
  let bytes = 0
  let first
  let max
  for (let index = 0; index < count; index++) {
    const message = messages[index]
    if (message === null || typeof message !== 'object') {
      throw new KafkaJSNonRetriableError(`Invalid message at index ${index}: it must be an object`)
    }
    const own = { ...message }
    const base = slots.length
    slots.push(0, 0)
    let tail = addNullable(slots, fieldOf(own, message, 'key'), index, 'key')
    tail += addNullable(slots, fieldOf(own, message, 'value'), index, 'value')
    tail += addHeaders(slots, fieldOf(own, message, 'headers'), index)
    const timestamp = timestampOf(fieldOf(own, message, 'timestamp'), now)
    if (timestamp === null) {
      throw refusal(index, 'timestamp')
    }
    slots[base] = timestamp
    slots[base + 1] = tail
    if (first === undefined || timestamp < first) first = timestamp
    if (max === undefined || timestamp > max) max = timestamp
    bytes += ENTRY_HEADER + tail
  }

  const store = Buffer.allocUnsafeSlow(bytes)
  const starts = new Array(count)
  let offset = 0
  let s = 0
  for (let index = 0; index < count; index++) {
    starts[index] = offset
    writeTimestamp(store, offset, slots[s])
    store.writeUInt32BE(slots[s + 1], offset + 8)
    store.writeUInt32BE(index, offset + 12)
    offset = writeField(store, offset + ENTRY_HEADER, slots[s + 2], slots[s + 3])
    offset = writeField(store, offset, slots[s + 4], slots[s + 5])
    const headerCount = slots[s + 6]
    offset = writeUVarint(store, offset, 2 * headerCount)
    s += 7
    for (let h = 0; h < headerCount; h++, s += 4) {
      offset = writeField(store, offset, slots[s], slots[s + 1])
      offset = writeField(store, offset, slots[s + 2], slots[s + 3])
    }
  }
  assertWritten(store, offset)
  return new RecordEntries(store, starts, first ?? now, max ?? now)
}

module.exports = {
  captureEntries,
  timestampOf,
  RecordEntries,
  probe,
  isGenuineBuffer,
  lengthOf,
  assertWritten,
  writeTimestamp,
  OWNED_RECORDS,
}
