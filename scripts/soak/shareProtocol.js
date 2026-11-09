const Decoder = require('../../src/protocol/decoder')
const { readRequestHeader } = require('../../testHelpers/fakeSocket')

/**
 * Reads actual Produce batch attributes, including every concatenated batch, from original bytes.
 * Only the modern record-batch formats used by this workload are admitted.
 * @param {Buffer} frame Original length-prefixed request.
 * @returns {number[]} Original codec attributes (zero means None).
 * @throws {Error} Unsupported, empty, incomplete or malformed framing.
 */
const produceCodecs = (frame) => {
  const header = readRequestHeader(frame)
  if (header.apiKey !== 0 || header.apiVersion < 3 || header.apiVersion > 13)
    throw new Error('Unsupported Produce frame')
  const flexible = header.apiVersion >= 9
  const decoder = new Decoder(frame.subarray(header.byteLength))
  const string = () => (flexible ? decoder.readUVarIntString() : decoder.readString())
  const array = (read) => (flexible ? decoder.readUVarIntArray(read) : decoder.readArray(read))
  const tags = () => {
    if (flexible) decoder.readTaggedFields()
  }
  string()
  decoder.readInt16()
  decoder.readInt32()
  const codecs = []
  array(() => {
    if (header.apiVersion >= 13) decoder.readUuid()
    else string()
    array(() => {
      decoder.readInt32()
      const length = flexible ? decoder.readUVarInt() - 1 : decoder.readInt32()
      if (length <= 0 || decoder.offset + length > decoder.buffer.length)
        throw new Error('Missing Produce batches')
      const end = decoder.offset + length
      while (decoder.offset < end) {
        const start = decoder.offset
        if (start + 23 > end || decoder.buffer[start + 16] !== 2)
          throw new Error('Invalid Produce batch header')
        const next = start + 12 + decoder.buffer.readInt32BE(start + 8)
        if (next > end || next <= start + 23) throw new Error('Invalid Produce batch length')
        codecs.push(decoder.buffer.readInt16BE(start + 21) & 7)
        decoder.offset = next
      }
      tags()
    })
    tags()
  })
  tags()
  if (!codecs.length || decoder.offset !== decoder.buffer.length)
    throw new Error('Incomplete Produce inventory')
  return codecs
}

module.exports = { produceCodecs }
