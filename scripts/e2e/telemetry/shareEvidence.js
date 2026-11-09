const assert = require('assert/strict')
const { performance } = require('perf_hooks')
const Decoder = require('../../../src/protocol/decoder')
const { readRequestHeader } = require('../../../testHelpers/fakeSocket')
const fetch = require('../../../testHelpers/wire/shareFetchV2')
const acknowledge = require('../../../testHelpers/wire/shareAcknowledgeV2')

/**
 * Independently reads the acknowledgement fields in the finite ShareFetch/Acknowledge v2 forms.
 * Records are skipped as opaque bytes; this oracle never uses the candidate acknowledgement codec.
 * @param {number} apiKey 78 or 79
 * @param {Buffer} bytes complete broker response including size/correlation/header tags
 * @returns {object} group status, fetch partition status and acknowledgement status
 * @throws {Error} for malformed fields or trailing bytes
 */
const acknowledgementResponse = (apiKey, bytes) => {
  const d = new Decoder(bytes.subarray(8))
  d.readTaggedFields()
  d.readInt32()
  const errorCode = d.readInt16()
  d.readUVarIntString()
  const acquisitionLockTimeoutMs = d.readInt32()
  const topics = d.readUVarIntArray((d) => {
    const topicId = d.readUuid()
    const partitions = d.readUVarIntArray((d) => {
      const partitionIndex = d.readInt32()
      const partitionErrorCode = d.readInt16()
      d.readUVarIntString()
      const acknowledgeErrorCode = apiKey === 78 ? d.readInt16() : partitionErrorCode
      if (apiKey === 78) d.readUVarIntString()
      d.readInt32()
      d.readInt32()
      d.readTaggedFields()
      let acquired = []
      if (apiKey === 78) {
        d.readUVarIntBytes()
        acquired = d.readUVarIntArray((d) => {
          const range = {
            firstOffset: String(d.readInt64()),
            lastOffset: String(d.readInt64()),
            deliveryCount: d.readInt16(),
          }
          d.readTaggedFields()
          return range
        })
      }
      d.readTaggedFields()
      return {
        partitionIndex,
        fetchErrorCode: apiKey === 78 ? partitionErrorCode : null,
        acknowledgeErrorCode,
        acquired,
      }
    })
    d.readTaggedFields()
    return { topicId, partitions }
  })
  d.readUVarIntArray((d) => {
    d.readInt32()
    d.readUVarIntString()
    d.readInt32()
    d.readUVarIntString()
    d.readTaggedFields()
    return null
  })
  d.readTaggedFields()
  assert(!d.canReadBytes(1), 'Trailing acknowledgement response bytes')
  return { errorCode, topics, acquisitionLockTimeoutMs }
}

/**
 * Replays raw same-socket requests and broker confirmations to exact per-offset decisions.
 * @param {object[]} frames bounded original wire frames as hex
 * @param {string} group expected share group
 * @returns {object[]} confirmed offset/type/time facts without a set projection hiding duplicates
 * @throws {Error} for missing/mismatched responses or malformed acknowledgement data
 */
const confirmedDecisions = (frames, group) =>
  frames.flatMap((frame) => {
    if (!frame.responseHex) return []
    const requestBytes = Buffer.from(frame.requestHex, 'hex')
    const header = readRequestHeader(requestBytes)
    assert.equal(header.apiVersion, 2)
    const request = (header.apiKey === 78 ? fetch : acknowledge).readRequest(
      requestBytes.subarray(header.byteLength)
    )
    if (request.groupId !== group || request.isRenewAck) return []
    const responseBytes = Buffer.from(frame.responseHex, 'hex')
    assert.equal(responseBytes.readInt32BE(4), header.correlationId)
    const response = acknowledgementResponse(header.apiKey, responseBytes)
    if (response.errorCode !== 0) return []
    return request.topics.flatMap((topic) =>
      topic.partitions.flatMap((partition) => {
        const replies = response.topics
          .filter((t) => t.topicId === topic.topicId)
          .flatMap((t) => t.partitions)
          .filter((p) => p.partitionIndex === partition.partitionIndex)
        if (replies.length !== 1 || replies[0].acknowledgeErrorCode !== 0) return []
        return partition.acknowledgementBatches.flatMap((batch) => {
          const facts = []
          for (
            let offset = BigInt(batch.firstOffset);
            offset <= BigInt(batch.lastOffset);
            offset++
          ) {
            assert(facts.length < 100, 'Unbounded share settlement range')
            facts.push({
              topicId: topic.topicId,
              partition: partition.partitionIndex,
              offset: String(offset),
              type:
                batch.acknowledgeTypes.length === 1
                  ? batch.acknowledgeTypes[0]
                  : batch.acknowledgeTypes[Number(offset - BigInt(batch.firstOffset))],
              errorCode: 0,
              at: frame.responseAt,
            })
          }
          return facts
        })
      })
    )
  })

/**
 * Joins the holder's acquisition member to its single successful final acknowledgement.
 * A final share session releases undecided locks; it does not acknowledge those offsets.
 * @param {object[]} frames original holder exchanges
 * @param {string} group owning group
 * @param {string} topicId independently created topic
 * @returns {object} final raw exchange, after all successful acquisitions
 * @throws {Error} for missing, duplicate, foreign, failed or non-final acknowledgement
 */
const finalAcknowledgement = (frames, group, topicId) => {
  const requests = frames.map((frame) => {
    const bytes = Buffer.from(frame.requestHex, 'hex')
    const header = readRequestHeader(bytes)
    assert.equal(header.apiVersion, 2)
    assert([78, 79].includes(header.apiKey))
    const request = (header.apiKey === 78 ? fetch : acknowledge).readRequest(
      bytes.subarray(header.byteLength)
    )
    assert.equal(request.groupId, group, 'Foreign share final group')
    return { frame, header, request }
  })
  const finals = requests.filter((r) => r.request.shareSessionEpoch === -1)
  assert.equal(finals.length, 1, 'Missing or duplicate share final acknowledgement')
  const [final] = finals
  assert.equal(final, requests.at(-1), 'Share exchange after final acknowledgement')
  assert.equal(final.header.apiKey, 79, 'Share final must be an acknowledgement')
  assert.equal(final.request.isRenewAck, false)
  assert(final.request.memberId, 'Missing share final member')
  assert(final.frame.responseHex, 'Missing share final response')
  const bytes = Buffer.from(final.frame.responseHex, 'hex')
  assert.equal(bytes.readInt32BE(4), final.header.correlationId)
  const response = acknowledgementResponse(79, bytes)
  assert.equal(response.errorCode, 0, 'Failed share final acknowledgement')
  assert.deepEqual(
    response.topics.map((t) => [
      t.topicId,
      t.partitions.map((p) => [p.partitionIndex, p.acknowledgeErrorCode]),
    ]),
    [[topicId, [[0, 0]]]],
    'Failed or foreign share final partition'
  )
  assert.deepEqual(
    final.request.topics.map((t) => [t.topicId, t.partitions.map((p) => p.partitionIndex)]),
    [[topicId, [0]]],
    'Foreign share final request'
  )
  assert(Number.isFinite(final.frame.at) && final.frame.responseAt >= final.frame.at)
  for (const { frame, request } of requests.slice(0, -1)) {
    assert.equal(request.memberId, final.request.memberId, 'Share final member differs from holder')
    assert(frame.responseAt <= final.frame.at, 'Share final precedes acquisition response')
  }
  return final.frame
}

/**
 * Retains successful raw acquisition ranges with their topic and partition identities.
 * @param {object[]} frames original same-socket request/response frames
 * @param {string} group owning group
 * @returns {object[]} independently acquired ranges, including duplicates
 * @throws {Error} for correlation mismatch or malformed frames
 */
const heldAcquisitions = (frames, group) =>
  frames.flatMap((frame) => {
    if (!frame.responseHex) return []
    const bytes = Buffer.from(frame.requestHex, 'hex')
    const header = readRequestHeader(bytes)
    if (header.apiKey !== 78) return []
    const request = fetch.readRequest(bytes.subarray(header.byteLength))
    if (request.groupId !== group) return []
    const reply = Buffer.from(frame.responseHex, 'hex')
    assert.equal(reply.readInt32BE(4), header.correlationId)
    const response = acknowledgementResponse(78, reply)
    if (response.errorCode !== 0) return []
    return response.topics.flatMap((topic) =>
      topic.partitions.flatMap((partition) => {
        if (partition.fetchErrorCode !== 0) return []
        return partition.acquired.map((range) => ({
          ...range,
          topicId: topic.topicId,
          partition: partition.partitionIndex,
          at: frame.at,
          lockMs: response.acquisitionLockTimeoutMs,
        }))
      })
    )
  })

/**
 * Supplies a conservative lock origin before the broker could acquire the held records.
 * @param {object[]} frames original requests and responses
 * @param {string} group owning share group
 * @returns {number} earliest acquisition request write time
 * @throws {Error} without an independently observed acquisition
 */
const acquisitionStart = (frames, group) => {
  const starts = frames.flatMap((frame) => {
    if (!frame.responseHex) return []
    const bytes = Buffer.from(frame.requestHex, 'hex')
    const header = readRequestHeader(bytes)
    if (
      header.apiKey !== 78 ||
      fetch.readRequest(bytes.subarray(header.byteLength)).groupId !== group
    )
      return []
    const response = acknowledgementResponse(78, Buffer.from(frame.responseHex, 'hex'))
    const acquired =
      response.errorCode === 0 &&
      response.topics.some((t) =>
        t.partitions.some((p) => p.fetchErrorCode === 0 && p.acquired.length > 0)
      )
    if (acquired)
      assert.equal(response.acquisitionLockTimeoutMs, 15000, 'Unexpected share lock duration')
    return acquired ? [frame.at] : []
  })
  assert(starts.length > 0, 'Missing raw share acquisition')
  return Math.min(...starts)
}

/** Owns a bounded passive raw acknowledgement trace for one share member. */
class ShareTrace {
  /** @returns {ShareTrace} empty trace */
  constructor() {
    this.frames = []
    this.failures = []
  }

  /**
   * Wraps only the supplied member's socket and removes its listeners at physical close.
   * @param {object} socket original client socket
   * @returns {object} same socket
   * @throws {Error} none synchronously; capture errors are retained for verification
   */
  wrap(socket) {
    const pending = new Map()
    const buffers = { out: Buffer.alloc(0), in: Buffer.alloc(0) }
    const capture = (direction, chunk) => {
      try {
        let bytes = Buffer.concat([buffers[direction], Buffer.from(chunk)])
        assert(bytes.length <= 2 * 1024 * 1024, 'Share trace byte bound')
        while (bytes.length >= 4 && bytes.length >= bytes.readInt32BE(0) + 4) {
          const length = bytes.readInt32BE(0) + 4
          assert(length >= 8)
          const frame = bytes.subarray(0, length)
          bytes = bytes.subarray(length)
          if (direction === 'out') {
            const header = readRequestHeader(frame)
            if (![78, 79].includes(header.apiKey)) continue
            assert(this.frames.length < 512, 'Share trace frame bound')
            const fact = { requestHex: frame.toString('hex'), at: performance.now() }
            this.frames.push(fact)
            pending.set(header.correlationId, fact)
          } else {
            const fact = pending.get(frame.readInt32BE(4))
            if (!fact) continue
            pending.delete(frame.readInt32BE(4))
            fact.responseHex = frame.toString('hex')
            fact.responseAt = performance.now()
          }
        }
        buffers[direction] = bytes
      } catch (error) {
        this.failures.push(error.message)
      }
    }
    const original = socket.write
    const write = function (chunk, ...rest) {
      capture('out', chunk)
      return Reflect.apply(original, this, [chunk, ...rest])
    }
    const receive = (chunk) => capture('in', chunk)
    socket.write = write
    socket.on('data', receive)
    socket.once('close', () => {
      socket.removeListener('data', receive)
      if (socket.write === write) socket.write = original
    })
    return socket
  }

  /** @returns {object[]} exact raw frames @throws {Error} on a capture failure */
  snapshot() {
    assert.deepEqual(this.failures, [])
    return JSON.parse(JSON.stringify(this.frames))
  }
}

module.exports = {
  ShareTrace,
  acknowledgementResponse,
  confirmedDecisions,
  finalAcknowledgement,
  acquisitionStart,
  heldAcquisitions,
}
