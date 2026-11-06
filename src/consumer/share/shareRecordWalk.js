const { KafkaJSNonRetriableError } = require('../../errors')
const MAX_OFFSET = 9223372036854775807n
const OFFSET_TYPES = new Set(['string', 'bigint', 'number'])
const OFFSET_TEXT = /^(0|[1-9][0-9]*)$/

/**
 * Parses an exact nonnegative signed-int64 offset without rounding.
 * @param {string|bigint|number} value Canonical decimal text, bigint, or safe integer.
 * @returns {bigint} Exact offset.
 * @throws {KafkaJSNonRetriableError} For malformed, negative, unsafe or overflowing input.
 */
const parseOffset = (value) => {
  if (
    !OFFSET_TYPES.has(typeof value) ||
    (typeof value === 'number' && !Number.isSafeInteger(value)) ||
    !OFFSET_TEXT.test(String(value))
  ) {
    throw new KafkaJSNonRetriableError('Offset must be an exact nonnegative integer')
  }
  const offset = BigInt(value)
  if (offset > MAX_OFFSET) throw new KafkaJSNonRetriableError('Offset exceeds signed int64')
  return offset
}

/** A range-sized priority heap; the earliest input row wins each overlap. */
class RangePriority {
  /** @returns {RangePriority} Empty heap. */
  constructor() {
    this.values = []
  }

  /** @param {number} value Input-row index. @returns {void} Inserts one row. */
  add(value) {
    let i = this.values.length
    this.values.push(value)
    while (i > 0) {
      const parent = (i - 1) >> 1
      if (this.values[parent] <= value) break
      this.values[i] = this.values[parent]
      i = parent
    }
    this.values[i] = value
  }

  /** @returns {void} Removes the smallest row index; callers ensure a nonempty heap. */
  remove() {
    const end = this.values.pop()
    if (!this.values.length) return
    let i = 0
    while (2 * i + 1 < this.values.length) {
      let child = 2 * i + 1
      if (child + 1 < this.values.length && this.values[child + 1] < this.values[child]) child++
      if (this.values[child] >= end) break
      this.values[i] = this.values[child]
      i = child
    }
    this.values[i] = end
  }

  /** @param {boolean[]} active Row activity. @returns {number|undefined} First active input row. */
  first(active) {
    while (this.values.length && !active[this.values[0]]) this.remove()
    return this.values[0]
  }
}

/**
 * Appends or coalesces a compact inclusive interval without enumerating offsets.
 * @param {object[]} output Ascending disjoint ranges.
 * @param {bigint} first Inclusive lower bound.
 * @param {bigint} last Inclusive upper bound; an empty interval is ignored.
 * @param {number} [deliveryCount] Acquisition count when normalizing ownership.
 * @returns {void} Updates only output.
 */
const appendRange = (output, first, last, deliveryCount) => {
  if (first > last) return
  const previous = output[output.length - 1]
  if (
    previous &&
    BigInt(previous.lastOffset) + 1n === first &&
    previous.deliveryCount === deliveryCount
  ) {
    previous.lastOffset = String(last)
  } else {
    const range = { firstOffset: String(first), lastOffset: String(last) }
    if (deliveryCount !== undefined) range.deliveryCount = deliveryCount
    output.push(range)
  }
}

/** A forward-only cursor over decoded records that parses and orders each offset exactly once. */
class RecordCursor {
  /**
   * @param {object[]} messages Decoded records with decimal offsets, ascending (equal offsets allowed).
   * @returns {RecordCursor} Cursor on the first record, already validated.
   * @throws {KafkaJSNonRetriableError} If the first offset is malformed.
   */
  constructor(messages) {
    this.messages = messages
    this.index = -1
    this.offset = -1n
    this.advance()
  }

  /** @returns {boolean} True once every record has been consumed. */
  get done() {
    return this.index >= this.messages.length
  }

  /** @returns {object} Current record; callers check `done` first. */
  get record() {
    return this.messages[this.index]
  }

  /**
   * Moves to the next record and validates its offset against the previous one.
   * @returns {void} After the last record, `done` is true and `offset` keeps the last offset.
   * @throws {KafkaJSNonRetriableError} For a malformed, unsafe, overflowing or descending offset.
   */
  advance() {
    this.index++
    if (this.done) return
    const offset = parseOffset(this.messages[this.index].offset)
    if (offset < this.offset) throw new KafkaJSNonRetriableError('Record offsets must be ascending')
    this.offset = offset
  }
}

/** Exact acquired-range normalization and two-pointer classification of a decoded partition. */
class ShareRecordWalk {
  /**
   * Normalizes ownership in O(r log r) time/O(r) space with original-row precedence.
   * @param {object[]} acquiredRecords Inclusive firstOffset/lastOffset and positive int16 deliveryCount.
   * @param {object} [logger] Logger with warn(); conflicts emit one warning per call.
   * @returns {object[]} Ascending disjoint ranges, coalesced only when delivery counts agree.
   * @throws {KafkaJSNonRetriableError} For invalid offsets, reversed ranges or delivery counts.
   */
  static normalize(acquiredRecords, logger) {
    const rows = acquiredRecords.map(({ firstOffset, lastOffset, deliveryCount }) => {
      const first = parseOffset(firstOffset)
      const last = parseOffset(lastOffset)
      if (
        first > last ||
        !Number.isInteger(deliveryCount) ||
        deliveryCount < 1 ||
        deliveryCount > 32767
      ) {
        throw new KafkaJSNonRetriableError('Invalid acquired range or delivery count')
      }
      return { first, last, deliveryCount }
    })
    const events = []
    rows.forEach((row, index) => {
      events.push({ at: row.first, index, add: true }, { at: row.last + 1n, index, add: false })
    })
    events.sort((a, b) => (a.at < b.at ? -1 : a.at > b.at ? 1 : 0))
    const active = []
    const counts = new Map()
    const heap = new RangePriority()
    const output = []
    let conflict = false
    let i = 0
    while (i < events.length) {
      const at = events[i].at
      while (i < events.length && events[i].at === at) {
        const { index, add } = events[i++]
        active[index] = add
        const count = rows[index].deliveryCount
        const remaining = (counts.get(count) || 0) + (add ? 1 : -1)
        if (remaining) counts.set(count, remaining)
        else counts.delete(count)
        if (add) heap.add(index)
      }
      const winner = heap.first(active)
      if (winner !== undefined && i < events.length) {
        appendRange(output, at, events[i].at - 1n, rows[winner].deliveryCount)
        if (counts.size > 1) conflict = true
      }
    }
    if (conflict && logger) logger.warn('Overlapping acquired ranges disagree on delivery count')
    return output
  }

  /**
   * Walks normalized ranges and ascending records once; delivery references original records.
   * With a halt, only offsets through the last decoded record are classified. Failed-batch
   * coordinates cannot supply a boundary; remaining acquired offsets are released, never rejected.
   * @param {object} input Decoded partition fields.
   * @param {object[]} input.acquiredRecords Raw acquisition ranges.
   * @param {object[]} input.messages Complete decoded prefix, including flagged control records.
   * @param {object} [input.corruption] Existing reason/offset/error marker; presence means halted.
   * @param {object} [logger] Existing logger with warn().
   * @returns {{deliver:object[],gaps:object[],release:object[],reject:object[]}} Compact classification.
   * @throws {KafkaJSNonRetriableError} For malformed acquisitions or malformed or descending record
   * offsets, including records past the last acquired range; each offset is parsed once.
   */
  static walk({ acquiredRecords, messages, corruption }, logger) {
    const ranges = ShareRecordWalk.normalize(acquiredRecords, logger)
    if (corruption && logger)
      logger.error('Share fetch decoding halted', { error: corruption.error })
    const frontier = !corruption
      ? MAX_OFFSET
      : messages.length
        ? parseOffset(messages[messages.length - 1].offset)
        : -1n
    const records = new RecordCursor(messages)
    const output = { deliver: [], gaps: [], release: [], reject: [] }
    for (const range of ranges) {
      const first = BigInt(range.firstOffset)
      const last = BigInt(range.lastOffset)
      const end = last < frontier ? last : frontier
      let cursor = first
      while (!records.done && records.offset < first) records.advance()
      while (!records.done && records.offset <= end) {
        const { record, offset: at } = records
        records.advance()
        if (at < cursor) continue
        appendRange(output.gaps, cursor, at - 1n)
        if (record.isControlRecord) appendRange(output.gaps, at, at)
        else output.deliver.push({ record, deliveryCount: range.deliveryCount })
        cursor = at + 1n
      }
      appendRange(output.gaps, cursor, end)
      appendRange(output.release, end < first ? first : end + 1n, last)
    }
    while (!records.done) records.advance()
    return output
  }
}

module.exports = { ShareRecordWalk, parseOffset, appendRange }
