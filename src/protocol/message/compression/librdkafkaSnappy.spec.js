const fs = require('fs')
const path = require('path')
const { isUtf8 } = require('buffer')
const { Kafka } = require('../../../../index')
const { createScriptedCluster, groupScript } = require('../../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../../consumer/__tests__/fixtures/fetchHarness')
const sleep = require('../../../utils/sleep')
const waitFor = require('../../../utils/waitFor')
const compression = require('./index')
const { recordsField, decodeField } = require('./fixtures/compressedBatch')
const {
  CAPTURE_DIR,
  hexOf,
  loadCapture,
  publicRecord,
  sha256,
} = require('./fixtures/librdkafkaCapture')
const { validateCapture } = require('./fixtures/rawRecordOracle')

/** The bytes before the compressed records of a v2 batch. */
const BODY_OFFSET = 61

/** What a xerial-framed Snappy stream starts with: 0x82, `SNAPPY`, 0x00. */
const XERIAL_MAGIC = Buffer.from('82534e4150505900', 'hex')

/**
 * Reads a byte string front to back and refuses to run past any bound.
 */
class SectionReader {
  /** @param {Buffer} buffer */
  constructor(buffer) {
    this.buffer = buffer
    this.at = 0
  }

  /** @returns {number} the bytes not read yet */
  get remaining() {
    return this.buffer.length - this.at
  }

  /**
   * @param {number} length
   * @returns {Buffer} the next `length` bytes
   * @throws {RangeError} for a negative length, or one that runs past the end
   */
  take(length) {
    if (!Number.isSafeInteger(length) || length < 0 || length > this.remaining) {
      throw new RangeError(`A length of ${length} runs outside the bytes it sits in`)
    }
    const bytes = this.buffer.subarray(this.at, this.at + length)
    this.at += length
    return bytes
  }

  /**
   * @param {32|64} bits the width of the value
   * @returns {bigint} the next zigzag varint, in at most `ceil(bits / 7)` bytes
   * @throws {RangeError} for a longer encoding, or a value wider than `bits`
   */
  varint(bits) {
    let zigzag = 0n
    for (let index = 0; index < Math.ceil(bits / 7); index += 1) {
      const [byte] = this.take(1)
      zigzag |= BigInt(byte & 0x7f) << BigInt(7 * index)
      if (byte < 0x80) {
        if (zigzag >= 1n << BigInt(bits))
          throw new RangeError(`A varint is wider than ${bits} bits`)
        return (zigzag >> 1n) ^ -(zigzag & 1n)
      }
    }
    throw new RangeError(`A varint is longer than ${Math.ceil(bits / 7)} bytes`)
  }

  /** @returns {Buffer|null} a byte string behind a varint length, null for length -1 */
  nullable() {
    const length = this.varint(32)
    return length === -1n ? null : this.take(Number(length))
  }
}

/**
 * Reads the acquired, uncompressed record section with its own strict parser: it imports no
 * production record decoder or header formatter, so the client under test never supplies its own
 * expectation. Header entries keep every duplicate and their full wire order.
 * @param {Buffer} data the records, back to back, with nothing around them
 * @returns {object[]} the records in wire order, in the shape of the manifest's `rawRecords`
 * @throws {RangeError} for a bound crossed, a negative count, a record or the section not used up
 *   exactly, a non-zero record attribute, or a header key that is not UTF-8 round-tripping to its bytes
 */
const parseRawRecords = (data) => {
  const section = new SectionReader(data)
  const records = []
  while (section.remaining > 0) {
    const record = new SectionReader(section.take(Number(section.varint(32))))
    if (record.take(1)[0] !== 0) throw new RangeError('A record has a non-zero attribute')
    const timestampDelta = record.varint(64)
    const offsetDelta = record.varint(32)
    const key = record.nullable()
    const value = record.nullable()
    const headerCount = record.varint(32)
    if (headerCount < 0n) throw new RangeError('A record has a negative header count')
    const headers = []
    for (let index = 0n; index < headerCount; index += 1n) {
      const keyBytes = record.take(Number(record.varint(32)))
      const keyUtf8 = keyBytes.toString('utf8')
      if (!isUtf8(keyBytes) || !Buffer.from(keyUtf8).equals(keyBytes)) {
        throw new RangeError('A header key is not UTF-8 that round-trips to its bytes')
      }
      headers.push({ keyHex: hexOf(keyBytes), keyUtf8, valueHex: hexOf(record.nullable()) })
    }
    if (record.remaining !== 0) throw new RangeError('A record is longer than its fields')
    records.push({
      offsetDelta: String(offsetDelta),
      timestampDelta: String(timestampDelta),
      keyHex: hexOf(key),
      valueHex: hexOf(value),
      headers,
    })
  }
  return records
}

/**
 * The public records as the librdkafka consumer printed them, read from its retained output and
 * not from anything the client under test returns: each distinct header key once, keys sorted by
 * their UTF-8 bytes, a duplicate key's values in the order printed.
 * @param {string} jsonl the output of `kcat -C -J`, one JSON object per line
 * @returns {object[]} records in the shape of the manifest's `expectedPublicRecords`
 */
const projectConsumerOutput = (jsonl) =>
  jsonl
    .trim()
    .split('\n')
    .map((line) => {
      const { offset, ts, key, payload, headers } = JSON.parse(line)
      const grouped = new Map()
      for (let index = 0; index < headers.length; index += 2) {
        const name = headers[index]
        grouped.set(name, [...(grouped.get(name) ?? []), hexOf(headers[index + 1])])
      }
      return {
        offset: String(offset),
        timestamp: String(ts),
        keyHex: hexOf(key),
        valueHex: hexOf(payload),
        headers: [...grouped]
          .sort(([a], [b]) => Buffer.compare(Buffer.from(a), Buffer.from(b)))
          .map(([name, values]) => ({ key: name, values })),
      }
    })

describe('Protocol > Compression > the authentic librdkafka Snappy capture', () => {
  let capture

  beforeAll(() => {
    capture = loadCapture()
  })

  test('matches its recorded provenance, digests and raw framing, and reads back as its independently acquired records', async () => {
    const { batch, data, manifest } = capture
    const body = batch.subarray(BODY_OFFSET)
    const { framing, ...recorded } = manifest.batch
    const retained = (name) => fs.readFileSync(path.join(CAPTURE_DIR, name))

    expect(sha256(batch)).toBe(manifest.sha256)
    expect(batch.readInt8(16)).toBe(2)
    expect(batch.readInt16BE(21) & 7).toBe(2)
    expect(batch.readInt16BE(21) & 8).toBe(0)
    expect(batch.readInt16BE(21) & 0x30).toBe(0)
    expect(sha256(body)).toBe(manifest.compressedSha256)
    expect(sha256(data)).toBe(manifest.recordDataSha256)
    expect(framing).toBe('raw')
    expect(body.subarray(0, XERIAL_MAGIC.length).equals(XERIAL_MAGIC)).toBe(false)
    expect({
      magic: batch.readInt8(16),
      attributes: batch.readInt16BE(21),
      recordCount: batch.readInt32BE(57),
      baseOffset: String(batch.readBigInt64BE(0)),
      baseTimestamp: String(batch.readBigInt64BE(27)),
      lastOffsetDelta: String(batch.readInt32BE(23)),
    }).toEqual(recorded)
    expect(batch.readInt32BE(8) + 12).toBe(batch.length)
    expect(batch.readBigInt64BE(43)).toBe(-1n)

    expect(manifest.provenance.referenceDecoder.outputs).toEqual(
      Object.fromEntries(
        Object.keys(manifest.provenance.referenceDecoder.outputs).map((name) => [
          name,
          sha256(retained(name)),
        ])
      )
    )
    expect(projectConsumerOutput(retained('kcat-consume.jsonl').toString('utf8'))).toEqual(
      manifest.expectedPublicRecords
    )

    const records = parseRawRecords(data)
    expect(records).toHaveLength(batch.readInt32BE(57))
    expect(records).toEqual(manifest.rawRecords)
    expect(
      records.map(({ offsetDelta, timestampDelta }) => ({
        offset: String(BigInt(recorded.baseOffset) + BigInt(offsetDelta)),
        timestamp: String(BigInt(recorded.baseTimestamp) + BigInt(timestampDelta)),
      }))
    ).toEqual(
      manifest.expectedPublicRecords.map(({ offset, timestamp }) => ({ offset, timestamp }))
    )
    expect(() => validateCapture(capture)).not.toThrow()

    const snappy = compression.lookupCodec(compression.Types.Snappy)
    const decompressedBody = await snappy.decompress(body, { maxOutputLength: data.length })
    expect(decompressedBody).toEqual(data)
    expect(sha256(decompressedBody)).toBe(manifest.recordDataSha256)
  })

  test('is read by the Fetch decoder as the expected public records', async () => {
    const { batch, manifest } = capture

    const decoded = await decodeField(recordsField(batch))

    expect(decoded.corruption).toBeUndefined()
    expect(decoded.messages.map(publicRecord)).toEqual(manifest.expectedPublicRecords)
  })

  test('is delivered by a public consumer over a scripted Fetch as the expected public records', async () => {
    const { batch, manifest } = capture
    const { baseOffset, lastOffsetDelta } = manifest.batch
    const nextOffset = String(BigInt(baseOffset) + BigInt(lastOffsetDelta) + 1n)
    let fetches = 0
    const cluster = groupScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node-0', port: 9092 }] }),
      { committed: { 0: baseOffset } }
    ).answer('Fetch', async ({ maxWaitMs, topics }) => {
      fetches += 1
      const first = fetches === 1
      if (!first) await sleep(maxWaitMs)
      return {
        partitions: topics[0].partitions.map(({ partition }) => ({
          partition,
          errorCode: 0,
          records: first ? batch : false,
        })),
      }
    })
    const fetchOffsets = () =>
      cluster
        .requests()
        .filter(({ api }) => api === 'Fetch')
        .flatMap(({ fields }) =>
          fields.topics.flatMap(({ partitions }) =>
            partitions.map(({ fetchOffset }) => fetchOffset)
          )
        )
    const consumer = new Kafka(cluster.kafkaOptions()).consumer({
      groupId: 'librdkafka-snappy',
      maxWaitTimeInMs: 20,
    })
    const delivered = []
    let failure = null
    consumer.on(consumer.events.CRASH, ({ payload }) => {
      failure ??= payload.error
    })

    try {
      await consumer.connect()
      await consumer.subscribe({ topics: [TOPIC] })
      await consumer.run({
        autoCommit: false,
        eachMessage: async ({ message }) => {
          delivered.push(publicRecord(message))
        },
      })
      const count = manifest.expectedPublicRecords.length
      await waitFor(() => failure !== null || delivered.length >= count, {
        maxWait: 5000,
        timeoutMessage: 'The consumer was not delivered the records',
      })
      if (failure !== null) throw failure
      await waitFor(() => fetchOffsets().includes(nextOffset), { maxWait: 5000 })
      await consumer.stop()
    } finally {
      await consumer.disconnect()
    }

    expect(delivered).toEqual(manifest.expectedPublicRecords)
    expect(fetchOffsets().slice(0, 2)).toEqual([baseOffset, nextOffset])
    cluster.assertClean()
  })
})
