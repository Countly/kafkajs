const { KafkaJSPartialMessageError } = require('../../src/errors')
/** Reads a strict classic or compact array, preserving nullable empty selectors.
 * @param {object} decoder positioned decoder
 * @param {Function} read nested struct reader
 * @param {boolean} compact compact framing
 * @param {boolean} nullable schema permits null
 * @returns {object[]|null} decoded values
 * @throws {Error} truncated, negative, null-forbidden or oversized framing
 */
module.exports = (decoder, read, compact, nullable) => {
  const size = compact ? decoder.readUVarInt() - 1 : decoder.readInt32()
  if (size === -1 && nullable) return null
  if (size < 0) throw new KafkaJSPartialMessageError('Invalid array length')
  decoder.ensureReadable(size, 'array')
  return Array.from({ length: size }, () => read(decoder))
}
