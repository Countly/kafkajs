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

/** The independent v2 request readers of ShareFetch (78) and ShareAcknowledge (79). */
const SHARE_READERS = {
  78: require('../../testHelpers/wire/shareFetchV2'),
  79: require('../../testHelpers/wire/shareAcknowledgeV2'),
}

/**
 * Summarizes one outgoing share request from its original bytes: version, session epoch and every
 * acknowledgement range it carries. It never throws, because it runs on the client's write path:
 * a frame it cannot read yields nulls and the reader's reason.
 * @param {Buffer} frame Original length-prefixed ShareFetch or ShareAcknowledge request.
 * @returns {{ apiVersion: number|null, sessionEpoch: number|null, acknowledged: object[]|null,
 *   error: string|null }} `acknowledged` rows are `{ topicId, partition, from, to, types }`
 */
const shareRequestSummary = (frame) => {
  try {
    const header = readRequestHeader(frame)
    const reader = SHARE_READERS[header.apiKey]
    if (reader?.version !== header.apiVersion)
      throw new Error(`no reader for api ${header.apiKey} v${header.apiVersion}`)
    const request = reader.readRequest(frame.subarray(header.byteLength))
    const acknowledged = []
    for (const { topicId, partitions } of request.topics)
      for (const { partitionIndex, acknowledgementBatches } of partitions)
        for (const batch of acknowledgementBatches)
          acknowledged.push({
            topicId,
            partition: partitionIndex,
            from: batch.firstOffset,
            to: batch.lastOffset,
            types: batch.acknowledgeTypes,
          })
    return {
      apiVersion: header.apiVersion,
      sessionEpoch: request.shareSessionEpoch,
      acknowledged,
      error: null,
    }
  } catch (e) {
    return { apiVersion: null, sessionEpoch: null, acknowledged: null, error: e.message }
  }
}

module.exports = { produceCodecs, shareRequestSummary }
