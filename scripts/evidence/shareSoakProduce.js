const { isDeepStrictEqual: same } = require('node:util')
const Decoder = require('../../src/protocol/decoder')
const decodeBatch = require('../../src/protocol/recordBatch/v0/decoder')
const { readRequestHeader } = require('../../testHelpers/fakeSocket')
const { produceCodecs } = require('../soak/shareProtocol')

/** Reconciles shared producer-drain frames with original public calls and their record corpus. */
class ShareProduceEvidence {
  /** @param {object[]} rows One scope's original wire, public-call and producer observations. */
  constructor(rows) {
    this.sends = rows.filter(
      (r) =>
        r.event === 'soak-wire' &&
        r.kind === 'operation' &&
        r.name === 'send' &&
        r.status === 'passed'
    )
    this.starts = new Map(
      rows
        .filter((r) => r.event === 'soak-wire' && r.kind === 'operation' && r.status === 'started')
        .map((r) => [r.id, r.at])
    )
    this.produced = rows.filter((r) => r.event === 'soak-producer')
    this.claimed = new Set()
    this.observed = new Set()
  }

  /**
   * Locates candidates by partition acknowledgement; `claim` checks their names against Metadata.
   * @param {object} value Public or decoded partition result.
   * @returns {object} Partition identity, offset and error fields.
   */
  result({ partition, baseOffset, errorCode }) {
    return { partition, baseOffset, errorCode }
  }

  /**
   * Reads every original request partition and CRC-checked record batch, independently of send results.
   * The response topic key must identify exactly the topic written in this request.
   * @param {object} request Original framed request.
   * @param {object} decoded Decoded reply with names resolved from earlier wire metadata.
   * @returns {Promise<object[]>} Original topic, partition and ordered record facts.
   * @throws {Error} On unknown topics, non-None codecs, duplicate partitions or malformed bytes.
   */
  async partitions(request, decoded) {
    const bytes = Buffer.from(request.bytes, 'base64')
    const header = readRequestHeader(bytes)
    if (produceCodecs(bytes).some((codec) => codec !== 0))
      throw new Error('Produce codec must be None')
    const decoder = new Decoder(bytes.subarray(header.byteLength))
    const flexible = header.apiVersion >= 9
    const string = () => (flexible ? decoder.readUVarIntString() : decoder.readString())
    const array = (read) =>
      flexible ? decoder.readUVarIntArrayAsync(read) : decoder.readArrayAsync(read)
    const tags = () => {
      if (flexible) decoder.readTaggedFields()
    }
    string()
    if (decoder.readInt16() !== -1) throw new Error('Produce acknowledgement mode differs')
    decoder.readInt32()
    const seen = new Set()
    const matchedTopics = new Set()
    const topics = await array(async () => {
      const key = header.apiVersion >= 13 ? decoder.readUuid() : string()
      const found = decoded.topics.filter(
        (t) => (header.apiVersion >= 13 ? t.topicId : t.topicName) === key
      )
      if (found.length !== 1 || typeof found[0].topicName !== 'string')
        throw new Error('Produce topic lacks matching request, response and metadata')
      if (matchedTopics.has(found[0])) throw new Error('Duplicate requested Produce topic')
      matchedTopics.add(found[0])
      const topic = found[0].topicName
      const partitions = await array(async () => {
        const partition = decoder.readInt32()
        const identity = JSON.stringify([topic, partition])
        if (seen.has(identity)) throw new Error('Duplicate Produce partition')
        seen.add(identity)
        const length = flexible ? decoder.readUVarInt() - 1 : decoder.readInt32()
        if (length <= 0 || !decoder.canReadBytes(length)) throw new Error('Missing Produce records')
        const batches = new Decoder(decoder.readBytes(length))
        const messages = []
        while (batches.canReadBytes(1)) {
          const batch = await decodeBatch(batches, { checkCrcs: true })
          for (const record of batch.records) {
            if (record.isControlRecord || Object.keys(record.headers).length)
              throw new Error('Unexpected Produce control record or headers')
            messages.push({
              partition,
              key: record.key?.toString('hex') ?? null,
              value: record.value?.toString('hex') ?? null,
              timestamp: record.timestamp,
            })
          }
        }
        tags()
        const replies = found[0].partitions.filter((p) => p.partition === partition)
        if (replies.length !== 1) throw new Error('Produce response partition differs')
        return { topic, partition, messages, response: replies[0], decodedTopic: found[0] }
      })
      tags()
      return partitions
    })
    tags()
    const partitions = topics.flat()
    if (matchedTopics.size !== decoded.topics.length)
      throw new Error('Produce response topic inventory differs from request and metadata')
    if (
      decoder.offset !== decoder.buffer.length ||
      partitions.length !== decoded.topics.reduce((n, t) => n + t.partitions.length, 0)
    )
      throw new Error('Incomplete Produce partition inventory')
    return partitions
  }

  /**
   * Joins one wire partition to exactly one in-flight public call and one original payload corpus.
   * A drain's inherited async context is not the invocation identity of every batch it drains.
   * @param {object} answer Original request, decoded reply and reply time.
   * @param {object} part Independently decoded request partition.
   * @returns {object} A partition-scoped view for ordinary public-result reconciliation.
   * @throws {Error} On absent, ambiguous, reused, out-of-window or altered send evidence.
   */
  claim(answer, part) {
    const { request, answeredAt } = answer
    const { topic, partition, messages, response, decodedTopic } = part
    const result = this.result(response)
    const calls = this.sends.filter(
      (op) =>
        op.target?.topic === topic &&
        op.result?.length === 1 &&
        same(this.result(op.result[0]), result) &&
        Number.isFinite(this.starts.get(op.id)) &&
        this.starts.get(op.id) <= request.at &&
        request.at <= answeredAt &&
        answeredAt <= op.at
    )
    const observations = this.produced.filter(
      (row) =>
        row.topic === topic &&
        row.partition === partition &&
        row.sent?.length === 1 &&
        same(this.result(row.sent[0]), result)
    )
    if (calls.length !== 1 || observations.length !== 1)
      throw new Error('Produce partition lacks one original invocation and corpus')
    const [operation] = calls
    const [observation] = observations
    if (operation.result[0].topicName !== topic)
      throw new Error('Public Produce acknowledgement topic differs from metadata')
    if (observation.sent[0].topicName !== topic)
      throw new Error('Observed Produce acknowledgement topic differs from metadata')
    if (this.claimed.has(operation.id) || this.observed.has(observation))
      throw new Error('Produce partition reused original send evidence')
    const expected = observation.messages.map(({ partition, key, value, timestamp }) => ({
      partition,
      key,
      value,
      timestamp,
    }))
    if (!messages.length || !same(messages, expected))
      throw new Error('Decoded Produce records differ')
    this.claimed.add(operation.id)
    this.observed.add(observation)
    return {
      ...answer,
      request: { ...request, operationId: operation.id },
      decoded: { ...answer.decoded, topics: [{ ...decodedTopic, partitions: [response] }] },
    }
  }

  /**
   * Reconciles all successful Produce partitions without changing the original archive's context stamps.
   * Standalone codec controls omit the workload corpus; full scope validation requires all producer rows.
   * @param {object[]} answers Original independently decoded replies.
   * @returns {Promise<{answers: object[], problems: string[]}>} Partition-owned views and failures.
   */
  async reconcile(answers) {
    if (!this.produced.length) return { answers, problems: [] }
    const linked = []
    const problems = []
    for (const answer of answers) {
      if (answer.request.apiKey !== 0) {
        linked.push(answer)
        continue
      }
      try {
        for (const part of await this.partitions(answer.request, answer.decoded))
          linked.push(this.claim(answer, part))
      } catch (error) {
        problems.push(
          `Original Produce ${answer.request.socket}:${answer.request.frame}: ${error.message}`
        )
      }
    }
    if (this.observed.size !== this.produced.length)
      problems.push('Original Produce corpus lacks unique wire coverage')
    return { answers: linked, problems }
  }
}

module.exports = { ShareProduceEvidence }
