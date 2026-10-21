'use strict'

/**
 * Walks the bytes of one message body against an Apache message definition, field by field, and
 * reports where the bytes stop agreeing with it. It reads the wire exactly as the definition says
 * it is written: classic lengths below the first flexible version, compact lengths and a tag
 * buffer after every structure from it (KIP-482). It holds no value and builds no object, so it is
 * a check on bytes, never a codec and never a source of expected values.
 *
 * Zero dependencies beyond `scripts/compat/parse`, CommonJS, Node >= 22.
 */

const { parseVersionRange } = require('../compat/parse')

const FIXED_BYTES = {
  int8: 1,
  uint8: 1,
  int16: 2,
  uint16: 2,
  int32: 4,
  uint32: 4,
  int64: 8,
  uint64: 8,
  float64: 8,
  bool: 1,
  uuid: 16,
}

/**
 * @param {string|null} versions a version range such as `3+`, `0-2` or `1`
 * @param {number} version
 * @returns {boolean} whether `version` is in the range
 */
const alive = (versions, version) => {
  const range = parseVersionRange(versions)
  return range !== null && version >= range.min && (range.max === null || version <= range.max)
}

/**
 * Rebuilds the nesting that `scripts/compat/parse#flattenFields` flattened, from each row's
 * dotted path: the rows are in depth-first order, so a stack of the open parents is enough.
 * @param {{ path: string }[]} rows
 * @returns {{ row: object, children: object[] }[]} the top-level nodes
 */
const treeOf = (rows) => {
  const roots = []
  const stack = []
  for (const row of rows) {
    const depth = row.path.split('.').length - 1
    const node = { row, children: [] }
    stack.length = depth
    ;(depth === 0 ? roots : stack[depth - 1].children).push(node)
    stack[depth] = node
  }
  return roots
}

/** Reads one body: a cursor over a buffer that throws a plain `Error` naming the field and offset. */
class BodyWalker {
  /**
   * @param {Buffer} bytes
   * @param {number} version
   * @param {boolean} flexible whether `version` is flexible for this message
   */
  constructor(bytes, version, flexible) {
    this.bytes = bytes
    this.version = version
    this.flexible = flexible
    this.at = 0
  }

  /**
   * @param {number} count
   * @param {string} what the field, for the error
   * @returns {void}
   * @throws {Error} when fewer than `count` bytes remain
   */
  take(count, what) {
    if (count < 0 || this.at + count > this.bytes.length) {
      throw new Error(
        `${what} needs ${count} byte(s) at ${this.at}, ${this.bytes.length - this.at} remain`
      )
    }
    this.at += count
  }

  /** @returns {number} an unsigned varint, at most five bytes */
  uvarint(what) {
    let value = 0
    for (let shift = 0; shift < 35; shift += 7) {
      this.take(1, what)
      const byte = this.bytes[this.at - 1]
      value += (byte & 0x7f) * 2 ** shift
      if ((byte & 0x80) === 0) return value
    }
    throw new Error(`${what} is a varint longer than five bytes at ${this.at}`)
  }

  /**
   * The length of a string, bytes or records field and of an array: classic is an int16 (string)
   * or int32 (the rest) with -1 for null; compact is a varint of the length plus one with 0 for
   * null.
   * @returns {number} the length, or -1 for null
   */
  length(type, what) {
    if (this.flexible) return this.uvarint(what) - 1
    const size = type === 'string' ? 2 : 4
    this.take(size, what)
    return size === 2 ? this.bytes.readInt16BE(this.at - 2) : this.bytes.readInt32BE(this.at - 4)
  }

  /** Reads a tag buffer: ascending tags, each with a size that fits. */
  tags(what) {
    const count = this.uvarint(`${what} tag count`)
    let last = -1
    for (let i = 0; i < count; i++) {
      const tag = this.uvarint(`${what} tag`)
      if (tag <= last) throw new Error(`${what} tag ${tag} is not above ${last} at ${this.at}`)
      last = tag
      this.take(this.uvarint(`${what} tag ${tag} size`), `${what} tag ${tag}`)
    }
  }

  /**
   * @param {{ row: object, children: object[] }[]} nodes the fields of a structure
   * @param {string} what the structure, for the error
   */
  fields(nodes, what) {
    for (const { row, children } of nodes) {
      if (row.tag !== null || !alive(row.versions, this.version)) continue
      this.field(row, children, `${what}.${row.path.slice(row.path.lastIndexOf('.') + 1)}`)
    }
    if (this.flexible) this.tags(what)
  }

  field(row, children, what) {
    const isArray = row.type.startsWith('[]')
    const type = isArray ? row.type.slice(2) : row.type
    const nullable = alive(row.nullableVersions, this.version)
    const scalar = (name) => {
      if (name in FIXED_BYTES) return () => this.take(FIXED_BYTES[name], what)
      if (name === 'string' || name === 'bytes' || name === 'records') {
        return () => this.take(Math.max(this.length(name, what), 0), what)
      }
      return () => this.fields(children, what)
    }
    const one = scalar(type)

    if (isArray) {
      const count = this.length('array', what)
      for (let i = 0; i < count; i++) one()
    } else if (children.length > 0 && nullable) {
      this.take(1, what)
      if (this.bytes.readInt8(this.at - 1) !== -1) one()
    } else {
      one()
    }
  }
}

/**
 * Walks `bytes` against a parsed message definition at `version`.
 * @param {{ fields: object[], flexibleFrom: number|null }} spec a message from `scripts/compat/parse#readSpec`
 * @param {number} version
 * @param {Buffer} bytes one message body, without its header or size prefix
 * @returns {{ ok: true }|{ ok: false, reason: string }} whether the bytes are exactly one body
 */
const walkBody = (spec, version, bytes) => {
  const flexible = spec.flexibleFrom !== null && version >= spec.flexibleFrom
  const walker = new BodyWalker(bytes, version, flexible)
  try {
    walker.fields(treeOf(spec.fields), 'body')
    if (walker.at !== bytes.length) {
      return {
        ok: false,
        reason: `${bytes.length - walker.at} byte(s) left after the body at ${walker.at}`,
      }
    }
    return { ok: true }
  } catch (error) {
    return { ok: false, reason: error.message }
  }
}

module.exports = { walkBody, treeOf }
