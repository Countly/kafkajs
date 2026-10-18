const crypto = require('crypto')
const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')

/** Tag of `CurrentLeader` on a partition response and of `NodeEndpoints` on the response. */
const KIP_951_TAG = 0

const BATCH_HEADER_SIZE = 61
const TRANSACTIONAL = 0x10
const CONTROL = 0x20

const encodeLeader = ({ leaderId, leaderEpoch }) =>
  new Encoder().writeInt32(leaderId).writeInt32(leaderEpoch).writeTaggedFields()

const encodePartition = ({
  partition,
  errorCode,
  baseOffset,
  logAppendTime,
  logStartOffset,
  leader,
}) =>
  new Encoder()
    .writeInt32(partition)
    .writeInt16(errorCode)
    .writeInt64(baseOffset)
    .writeInt64(logAppendTime)
    .writeInt64(logStartOffset)
    .writeUVarIntArray([])
    .writeUVarIntString(null)
    .writeTaggedFields(leader ? [{ tag: KIP_951_TAG, value: encodeLeader(leader) }] : [])

const encodeEndpoint = ({ nodeId, host, port, rack }) =>
  new Encoder()
    .writeInt32(nodeId)
    .writeUVarIntString(host)
    .writeInt32(port)
    .writeUVarIntString(rack)
    .writeTaggedFields()

/**
 * The record batches of one partition's `Records` bytes, each reduced to the header fields that
 * decide order and idempotence. Read straight from the RecordBatch v2 layout: base offset (8),
 * batch length (4), leader epoch (4), magic (1), crc (4), attributes (2), last offset delta (4),
 * two timestamps (16), producer id (8), producer epoch (2), base sequence (4), record count (4).
 * @param {Buffer} records
 * @returns {Array<{ producerId: string, producerEpoch: number, baseSequence: number,
 *   recordCount: number, transactional: boolean, control: boolean, digest: string }>}
 * @throws {Error} when the bytes are not whole batches: a header cut short, or a length that is
 *   below a header or past the end
 */
const readBatches = (records) => {
  const batches = []
  let start = 0
  while (start < records.length) {
    if (start + BATCH_HEADER_SIZE > records.length) {
      throw new Error(
        `Records end with ${records.length - start} bytes that are not a batch header`
      )
    }

    const length = records.readInt32BE(start + 8)
    const end = start + 12 + length
    if (length < BATCH_HEADER_SIZE - 12 || end > records.length) {
      throw new Error(
        `a batch says it is ${length} bytes long, ${records.length - start - 12} follow`
      )
    }

    const attributes = records.readInt16BE(start + 21)
    batches.push({
      producerId: records.readBigInt64BE(start + 43).toString(),
      producerEpoch: records.readInt16BE(start + 51),
      baseSequence: records.readInt32BE(start + 53),
      recordCount: records.readInt32BE(start + 57),
      transactional: (attributes & TRANSACTIONAL) !== 0,
      control: (attributes & CONTROL) !== 0,
      digest: crypto.createHash('sha1').update(records.subarray(start, end)).digest('hex'),
    })
    start = end
  }

  return batches
}

/**
 * The one layout Produce v11, v12 and v13 share (flexible, request header v2); a version differs
 * only in how a topic is keyed: by name through v12, by id from v13. The KIP-951 tagged fields of
 * the response (a partition's `leader` and the `nodeEndpoints`) are written only when given, as a
 * broker sends them only when leadership moved.
 * @param {11|12|13} version
 * @returns {{ buildResponse: (fields: object) => Buffer, readRequest: (body: Buffer) => object }}
 */
const createProduceLayout = (version) => {
  const keyedById = version >= 13

  const encodeTopic = ({ topic, topicId, partitions }) => {
    const key = keyedById
      ? new Encoder().writeUuid(topicId)
      : new Encoder().writeUVarIntString(topic)
    return new Encoder()
      .writeEncoder(key)
      .writeUVarIntArray(partitions.map(encodePartition))
      .writeTaggedFields()
  }

  const readTopic = (decoder) => {
    const key = keyedById ? { topicId: decoder.readUuid() } : { topic: decoder.readUVarIntString() }
    const partitions = decoder.readUVarIntArray((p) => {
      const partition = p.readInt32()
      const records = p.readUVarIntBytes()
      p.readTaggedFields()
      return { partition, batches: records === null ? null : readBatches(records) }
    })
    decoder.readTaggedFields()
    return { ...key, partitions }
  }

  return {
    buildResponse(response) {
      return new Encoder()
        .writeUVarIntArray(response.responses.map(encodeTopic))
        .writeInt32(response.throttleTime)
        .writeTaggedFields(
          response.nodeEndpoints.length > 0
            ? [
                {
                  tag: KIP_951_TAG,
                  value: new Encoder().writeUVarIntArray(
                    response.nodeEndpoints.map(encodeEndpoint)
                  ),
                },
              ]
            : []
        ).buffer
    },

    readRequest(body) {
      const decoder = new Decoder(body)
      const request = {
        transactionalId: decoder.readUVarIntString(),
        acks: decoder.readInt16(),
        timeoutMs: decoder.readInt32(),
        topics: decoder.readUVarIntArray(readTopic),
      }
      decoder.readTaggedFields()
      if (decoder.canReadBytes(1)) {
        throw new Error(`Produce v${version} request has a byte after its tag buffer`)
      }

      return request
    },
  }
}

module.exports = { createProduceLayout, readBatches }
