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
 * @returns {object} the decoded record: `{ magicByte, attributes, timestamp, offset, key, value,
 *   headers, isControlRecord, batchContext }`, with `timestamp` and `offset` as decimal strings
 */
module.exports = (decoder, batchContext = {}, firstOffsetNumber) => {
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
      : Long.fromValue(firstTimestamp).add(timestampDelta).toString()

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
  for (const { key: headerKey, value: headerValue } of decoder.readVarIntArray(HeaderDecoder)) {
    const existing = Object.hasOwn(headers, headerKey) ? headers[headerKey] : undefined

    if (existing === undefined) {
      setHeader(headers, headerKey, headerValue)
    } else if (Array.isArray(existing)) {
      existing.push(headerValue)
    } else {
      setHeader(headers, headerKey, [existing, headerValue])
    }
  }

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
