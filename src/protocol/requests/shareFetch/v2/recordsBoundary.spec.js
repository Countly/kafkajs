const zlib = require('zlib')
const { plain } = require('../../../__tests__/fixtures/fuzzResponseVectors')
const { record } = require('../../fetch/__tests__/fixtures/recordBatchBytes')
const {
  COMPRESSORS,
  compressedBatch,
} = require('../../../message/compression/fixtures/compressedBatch')
const { KafkaJSCorruptRecordError } = require('../../../../errors')
const codec = require('./response')
const vector = require('../fixtures/java/shareFetchResponse-v2-recordsAndErrors.json')

// eslint-disable-next-line n/no-unsupported-features/node-builtins -- feature detection, not a call
const hasZstd = typeof zlib.zstdCompressSync === 'function'
let hasLz4 = true
try {
  require('lz4-napi')
} catch (e) {
  hasLz4 = false
}
const CODECS = ['gzip', 'snappy', ...(hasZstd ? ['zstd'] : []), ...(hasLz4 ? ['lz4'] : [])]

const JAVA = Buffer.from(vector.hex, 'hex')
// Partition 0's Records are a compact length at byte 48 and one 70-byte batch from byte 49 to 118.
const RECORDS_AT = 48
const RECORDS_END = 119
const BASE = 9007199254740993n

/** @param {number} value @returns {Buffer} the unsigned varint of `value`, as a compact length writes it */
const uvarint = (value) => {
  const bytes = []
  do {
    const low = value & 0x7f
    value >>>= 7
    bytes.push(value ? low | 0x80 : low)
  } while (value)
  return Buffer.from(bytes)
}

/**
 * The Java ShareFetch body with the Records of its first partition replaced; every other byte is
 * Java's own.
 * @param {...Buffer} batches record batches written from the documented layout
 * @returns {Buffer}
 */
const envelope = (...batches) => {
  const records = Buffer.concat(batches)
  return Buffer.concat([
    JAVA.subarray(0, RECORDS_AT),
    uvarint(records.length + 1),
    records,
    JAVA.subarray(RECORDS_END),
  ])
}

/**
 * Decodes an envelope and proves that everything but the first partition's Records is what Java
 * sent: the response fields, that partition's acquired ranges and leader, the whole second
 * partition and the node endpoints.
 * @param {object} decoded the decoded response
 * @returns {{ messages: object[], corruption?: object }} the first partition's Records result
 */
const restOf = (decoded) => {
  const { responses, nodeEndpoints, ...scalars } = decoded
  const { responses: javaResponses, nodeEndpoints: javaEndpoints, ...javaScalars } = vector.fields
  expect(plain(scalars)).toEqual(javaScalars)
  expect(nodeEndpoints).toEqual(javaEndpoints)
  expect(responses).toHaveLength(1)
  expect(responses[0].topicId).toBe(javaResponses[0].topicId)
  expect(responses[0].partitions).toHaveLength(2)

  const [{ messages, corruption, ...first }, second] = responses[0].partitions
  const javaFirst = { ...javaResponses[0].partitions[0] }
  delete javaFirst.messages
  expect(first).toEqual(javaFirst)
  expect(plain(second)).toEqual(javaResponses[0].partitions[1])
  return { messages, corruption }
}

/** The marker of a halted partition: its reason and first offset, and nothing the codec invented. */
const expectHalt = (corruption, reason) => {
  expect(corruption).toMatchObject({ reason, offset: String(BASE) })
  expect(Object.keys(corruption).sort()).toEqual(['error', 'offset', 'reason'])
  expect(corruption.error).toBeInstanceOf(KafkaJSCorruptRecordError)
  expect(corruption.error.reason).toBe(reason)
}

describe('ShareFetch v2 Records boundaries', () => {
  test('the splice rebuilds the Java body from its own Records', () => {
    expect(envelope(JAVA.subarray(RECORDS_AT + 1, RECORDS_END))).toEqual(JAVA)
  })

  describe('the decompressed bytes of a batch', () => {
    const VALUE_BYTES = 5000
    const HELD = record({ key: 'k', value: Buffer.alloc(VALUE_BYTES, 7) })
    const OVER = record({ key: 'k', value: Buffer.alloc(VALUE_BYTES + 1, 7) })
    const BOUND = HELD.length

    test('are N and N + 1 bytes for the two payloads', () => {
      expect(OVER.length).toBe(BOUND + 1)
    })

    describe.each(CODECS)('%s', (name) => {
      let held
      let over

      beforeAll(async () => {
        const batch = async (payload) =>
          compressedBatch({
            codec: name,
            records: 1,
            baseOffset: BASE,
            body: await COMPRESSORS[name](payload),
          })
        held = await batch(HELD)
        over = await batch(OVER)
      })

      describe.each([true, false])('with checkCrcs %s', (checkCrcs) => {
        const options = { checkCrcs, maxDecompressedBatchBytes: BOUND }

        test('a payload of N bytes under a bound of N keeps its record and every other field', async () => {
          const { messages, corruption } = restOf(await codec.decode(envelope(held), options))

          expect(corruption).toBeUndefined()
          expect(messages).toHaveLength(1)
          expect(messages[0].offset).toBe(String(BASE))
          expect(messages[0].key.toString()).toBe('k')
          expect(messages[0].value).toEqual(Buffer.alloc(VALUE_BYTES, 7))
        })

        test('a payload of N + 1 bytes under a bound of N halts as decompressed-size and keeps every other field', async () => {
          const { messages, corruption } = restOf(await codec.decode(envelope(over), options))

          expect(messages).toEqual([])
          expectHalt(corruption, 'decompressed-size')
          expect(corruption.error).toMatchObject({ codec: name, limit: BOUND })
        })
      })
    })
  })

  describe('the record count of a batch', () => {
    describe.each([
      ['gzip', 'gzip'],
      ['uncompressed', 'none'],
    ])('%s', (_, name) => {
      describe.each([true, false])('with checkCrcs %s', (checkCrcs) => {
        // 5120 bytes allow 10 records, 511 bytes allow 1 (and never fewer than 1)
        test.each([
          [5120, 10],
          [511, 1],
        ])(
          'a bound of %d bytes holds %d records and refuses one more, keeping every other field',
          async (bound, ceiling) => {
            const options = { checkCrcs, maxDecompressedBatchBytes: bound }
            const decode = async (records) =>
              restOf(
                await codec.decode(
                  envelope(await compressedBatch({ codec: name, records, baseOffset: BASE })),
                  options
                )
              )

            const held = await decode(ceiling)
            const over = await decode(ceiling + 1)

            expect(held.corruption).toBeUndefined()
            expect(
              held.messages.map((m) => [m.offset, m.key.toString(), m.value.toString()])
            ).toEqual(
              Array.from({ length: ceiling }, (item, i) => [
                String(BASE + BigInt(i)),
                `k${i}`,
                `v${i}`,
              ])
            )
            expect(over.messages).toEqual([])
            expectHalt(over.corruption, 'count')
            expect(over.corruption.error.limit).toBe(ceiling)
          }
        )
      })
    })
  })
})
