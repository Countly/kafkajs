const Long = require('../../../../utils/long')
const HeaderDecoder = require('../../header/v0/decoder')
const TimestampTypes = require('../../../timestampTypes')

/**
 * v0
 * Record =>
 *   Length => Varint
 *   Attributes => Int8
 *   TimestampDelta => Varlong
 *   OffsetDelta => Varint
 *   Key => varInt|Bytes
 *   Value => varInt|Bytes
 *   Headers => [HeaderKey HeaderValue]
 *     HeaderKey => VarInt|String
 *     HeaderValue => VarInt|Bytes
 */

/**
 * Stores one header value as an own property of the header object.
 * A plain assignment would be intercepted by the inherited `__proto__` setter when a record carries
 * a header literally named `__proto__` - it would change the object's prototype and drop the header
 * instead of storing it. Header keys come from the producer, so the guard is not theoretical.
 * @param {object} headers the accumulator being filled
 * @param {string} key the header key as sent on the wire
 * @param {Buffer|null|Array<Buffer|null>} value the value, or the array built for a repeated key
 * @returns {void}
 */
const setHeader = (headers, key, value) => {
  if (key === '__proto__') {
    Object.defineProperty(headers, key, {
      value,
      writable: true,
      enumerable: true,
      configurable: true,
    })
    return
  }

  headers[key] = value
}

/**
 * The record timestamp as a decimal string: a double sum when the base, the delta and the sum are
 * all safe integers, else exact 64-bit arithmetic. Both render through `Long`, like offsets.
 * @param {string} firstTimestamp the batch first timestamp
 * @param {number} [firstTimestampNumber] `Number(firstTimestamp)`, or undefined
 * @param {Long} timestampDelta the record delta
 * @returns {string}
 */
const addTimestamp = (firstTimestamp, firstTimestampNumber, timestampDelta) => {
  const delta = timestampDelta.toNumber()
  const sum = firstTimestampNumber + delta
  return Number.isSafeInteger(firstTimestampNumber) &&
    Number.isSafeInteger(delta) &&
    Number.isSafeInteger(sum)
    ? Long.fromNumber(sum).toString()
    : Long.fromValue(firstTimestamp).add(timestampDelta).toString()
}

/**
 * Whether a fresh empty object already sees any message key, that is, whether `Object.prototype`
 * carries one as a property of any kind. `Object.prototype` is the only object on the chain of an
 * empty literal and its own prototype is always null, so when this is false an assignment of a
 * message key to that object creates an own data property, exactly as a literal defines one. When
 * it is true (a polluted or hardened prototype) an assignment could run an inherited setter or be
 * refused by an inherited read-only property, so the caller uses the literal instead. Checked per
 * record: the properties of `Object.prototype` can change at any time.
 * @param {object} empty an object literal with no own properties
 * @returns {boolean}
 */
const messageKeyIsInherited = (empty) =>
  'magicByte' in empty ||
  'attributes' in empty ||
  'timestamp' in empty ||
  'offset' in empty ||
  'key' in empty ||
  'value' in empty ||
  'headers' in empty ||
  'isControlRecord' in empty ||
  'batchContext' in empty

/**
 * Decodes one record of a v0 record batch.
 * @param {Decoder} decoder positioned at the start of one record's body
 * @param {object} [batchContext] the batch-level values shared by every record in the batch; this
 *   object is attached to each decoded message as `batchContext`, so its shape is public
 * @param {number} [firstOffsetNumber] `Number(batchContext.firstOffset)`, computed once per batch.
 *   `firstOffset` itself is a string and `offsetDelta` a number, so adding the raw fields would
 *   concatenate rather than add - which is why the numeric base is passed separately and the fast
 *   path is guarded on both the base and the sum being safe integers. Above that, or when the base
 *   is absent, the exact 64-bit arithmetic is used unchanged. Either way the resulting string comes
 *   from `Long`, never from `Number#toString`.
 * @param {number} [firstTimestampNumber] `Number(batchContext.firstTimestamp)`, computed once per
 *   batch, the timestamp counterpart of `firstOffsetNumber` with the same guards. The delta is a
 *   varlong, so it is guarded too: an unsafe delta can still give a safe but inexact sum.
 * @param {{ remaining: number, limit: number }} [headerBudget] the headers the batch may still
 *   decode and its header ceiling, shared by every record of the batch and debited by this one.
 *   The batch decoder always passes it; the unbounded default serves calls on a lone record.
 * @returns {object} the decoded record: `{ magicByte, attributes, timestamp, offset, key, value,
 *   headers, isControlRecord, batchContext }`, with `timestamp` and `offset` as decimal strings
 * @throws {KafkaJSPartialMessageError} when the header count is negative (other than -1, an empty
 *   set) or above the bytes left in the record, before any header is read, or when a header runs
 *   past the record. The header array grows as headers decode, without preallocation from the
 *   untrusted count.
 * @throws {KafkaJSCorruptRecordError} with reason `count` when the header count is above what
 *   `headerBudget` has left, before any header is read
 */
module.exports = (
  decoder,
  batchContext = {},
  firstOffsetNumber,
  firstTimestampNumber,
  headerBudget = { remaining: Infinity, limit: Infinity }
) => {
  const {
    firstOffset,
    firstTimestamp,
    magicByte,
    isControlBatch = false,
    timestampType,
    maxTimestamp,
  } = batchContext
  const attributes = decoder.readInt8()

  const timestampDelta = decoder.readVarLong()
  const timestamp =
    timestampType === TimestampTypes.LOG_APPEND_TIME && maxTimestamp
      ? maxTimestamp
      : addTimestamp(firstTimestamp, firstTimestampNumber, timestampDelta)

  const offsetDelta = decoder.readVarInt()
  const offsetSum = firstOffsetNumber + offsetDelta
  // The sum is rendered through `Long`, not `Number#toString`. Both produce the same string, but
  // `Number#toString` interns its result in V8's number-to-string cache, and a consumer decodes one
  // distinct offset per record - which measurably slows the consumer's later pass over the decoded
  // messages, without costing decode itself. See `scripts/benchmarks/consumeThroughput.js`.
  const offset =
    Number.isSafeInteger(firstOffsetNumber) && Number.isSafeInteger(offsetSum)
      ? Long.fromNumber(offsetSum).toString()
      : Long.fromValue(firstOffset).add(offsetDelta).toString()

  const key = decoder.readVarIntBytes()
  const value = decoder.readVarIntBytes()

  // One accumulator object mutated in place instead of a spread per header. The duplicate-key
  // semantics are unchanged: the first occurrence stores the value, the second turns it into a
  // two-element array, later ones push onto it.
  const headers = {}
  for (const { key: headerKey, value: headerValue } of decoder.readVarIntArrayBytesBounded(
    HeaderDecoder,
    headerBudget
  )) {
    const existing = Object.hasOwn(headers, headerKey) ? headers[headerKey] : undefined

    if (existing === undefined) {
      setHeader(headers, headerKey, headerValue)
    } else if (Array.isArray(existing)) {
      existing.push(headerValue)
    } else {
      setHeader(headers, headerKey, [existing, headerValue])
    }
  }

  // The message is an empty literal filled by assignments, not a populated literal: V8 may switch
  // a populated literal's allocation site to old-space allocation once a batch of its objects
  // outlives a young-generation collection, and an empty literal has no such site (a V8 heuristic,
  // not a guarantee). The check is the last step before the stores: the reads above can run
  // inherited getters.
  const message = {}
  if (messageKeyIsInherited(message)) {
    return {
      magicByte,
      attributes, // Record level attributes are presently unused
      timestamp,
      offset,
      key,
      value,
      headers,
      isControlRecord: isControlBatch,
      batchContext,
    }
  }

  message.magicByte = magicByte
  message.attributes = attributes // Record level attributes are presently unused
  message.timestamp = timestamp
  message.offset = offset
  message.key = key
  message.value = value
  message.headers = headers
  message.isControlRecord = isControlBatch
  message.batchContext = batchContext
  return message
}
