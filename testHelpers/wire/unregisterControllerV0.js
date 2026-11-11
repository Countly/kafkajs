/** UnregisterController v0 request id from UnregisterControllerRequest.json (Apache Kafka 4.4.0). */
const API_KEY = 94

/**
 * @param {number} value Non-negative integer below 2^31.
 * @returns {Buffer} Unsigned varint (base-128, least significant group first).
 */
const unsignedVarint = (value) => {
  const bytes = []
  let rest = value
  while (rest >= 0x80) {
    bytes.push((rest & 0x7f) | 0x80)
    rest = Math.floor(rest / 128)
  }
  bytes.push(rest)
  return Buffer.from(bytes)
}

/**
 * @param {string|null} value Text to write; null is the compact nullable sentinel.
 * @returns {Buffer} Compact nullable string: unsigned varint of (byte length + 1), then UTF-8 bytes.
 */
const compactNullableString = (value) => {
  if (value === null) return unsignedVarint(0)
  const bytes = Buffer.from(value, 'utf8')
  return Buffer.concat([unsignedVarint(bytes.length + 1), bytes])
}

/**
 * @param {{tag: number, value: Buffer}[]} tags Unknown tagged fields in ascending tag order.
 * @returns {Buffer} Tag buffer: field count, then (tag, size, bytes) for each field.
 */
const taggedFields = (tags) =>
  Buffer.concat([
    unsignedVarint(tags.length),
    ...tags.flatMap(({ tag, value }) => [unsignedVarint(tag), unsignedVarint(value.length), value]),
  ])

/**
 * Independent UnregisterController v0 wire pair. It uses only Buffer operations and the literal field
 * order of the Apache Kafka 4.4.0 message definitions; it imports no client encoder, decoder or codec.
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/resources/common/message/UnregisterControllerResponse.json
 */
module.exports = {
  api: 'UnregisterController',
  apiKey: API_KEY,
  version: 0,
  /**
   * @param {object} [fields] Response fields.
   * @param {number} [fields.throttleTimeMs=0] Wire throttle in milliseconds.
   * @param {number} [fields.errorCode=0] Broker error code.
   * @param {string|null} [fields.errorMessage=null] Top-level message; null is distinct from ''.
   * @param {{tag: number, value: Buffer}[]} [fields.tags=[]] Unknown tagged fields to append.
   * @returns {Buffer} Response body without its header.
   */
  buildResponse({ throttleTimeMs = 0, errorCode = 0, errorMessage = null, tags = [] } = {}) {
    const head = Buffer.alloc(6)
    head.writeInt32BE(throttleTimeMs, 0)
    head.writeInt16BE(errorCode, 4)
    return Buffer.concat([head, compactNullableString(errorMessage), taggedFields(tags)])
  },
  /**
   * @param {Buffer} body Request body without its header.
   * @returns {{controllerId: number}} The one request field.
   * @throws {Error} Truncated body, truncated tag, or a byte after the tag buffer.
   */
  readRequest(body) {
    if (body.length < 5) throw new Error('UnregisterController v0 request is truncated')
    const controllerId = body.readInt32BE(0)
    if (body.length !== 5 || body[4] !== 0)
      throw new Error('UnregisterController v0 request must end with an empty tag buffer')
    return { controllerId }
  },
}
