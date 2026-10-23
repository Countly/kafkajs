const zlib = require('zlib')
const Decoder = require('../../../decoder')
const decodeMessages = require('../v4/decodeMessages')
const { KafkaJSCorruptRecordError, KafkaJSPartialMessageError } = require('../../../../errors')
const { fetchResponse } = require('../../../../consumer/__tests__/fixtures/fetchHarness')
const { crc32c, varint, record, batch, controlBatch } = require('./fixtures/recordBatchBytes')

const FIXTURES = '../fixtures'

/** The RECORDS field of a classic Fetch partition: an int32 length, then the batches. */
const records = (...batches) => {
  const block = Buffer.concat(batches)
  const size = Buffer.alloc(4)
  size.writeInt32BE(block.length)
  return Buffer.concat([size, block])
}

const decode = (block, options) => decodeMessages(new Decoder(block), options)

/** A valid batch of `count` records from `baseOffset`, each keyed `k<offset>` and valued `v<offset>`. */
const valid = (baseOffset, count = 2) =>
  batch({
    baseOffset: BigInt(baseOffset),
    records: Array.from({ length: count }, (_, index) =>
      record({
        offsetDelta: index,
        key: `k${baseOffset + index}`,
        value: `v${baseOffset + index}`,
      })
    ),
  })

/** What a decoded message is, from the values `valid` gave it. */
const literal = (offset) => ({ offset: String(offset), key: `k${offset}`, value: `v${offset}` })
const firstOf = (count) => Array.from({ length: count }, (_, index) => literal(index))
const asLiteral = ({ offset, key, value }) => ({
  offset,
  key: key.toString(),
  value: value.toString(),
})

/** More records than a call can take as arguments: `push(...records)` stops at about 125 000. */
const MANY = 150000
let manyBatch
const many = () =>
  (manyBatch ??= batch({
    records: Array.from({ length: MANY }, (_, index) => record({ offsetDelta: index })),
  }))

describe('Protocol > Requests > Fetch > record envelope', () => {
  describe('the batches these cases are built from', () => {
    test('are byte for byte the batch a Kafka broker sent, CRC included', () => {
      const sent = Buffer.from(require(`${FIXTURES}/v12_response_records.json`)).subarray(59, 129)

      expect(
        batch({
          baseOffset: 100n,
          partitionLeaderEpoch: 5,
          firstTimestamp: 1700000000000n,
          maxTimestamp: 1700000000000n,
          producerEpoch: 0,
          baseSequence: 0,
          records: [record({ key: 'k', value: 'v' })],
        })
      ).toEqual(sent)
    })

    test('use a CRC-32C that gives the published check value', () => {
      // the check value of CRC-32/ISCSI in the CRC catalogue (reveng.sourceforge.io/crc-catalogue)
      expect(crc32c(Buffer.from('123456789'))).toBe(0xe3069283)
    })
  })

  describe.each([true, false])('with checkCrcs %s', (checkCrcs) => {
    const options = { checkCrcs }
    const [first, second] = [valid(0), valid(2)]
    const firstMessages = [literal(0), literal(1)]

    describe('a decoded block that is whole', () => {
      test('returns every record of every batch and no corruption', async () => {
        const decoded = await decode(records(first, second), options)

        expect(decoded.messages.map(asLiteral)).toEqual([...firstMessages, literal(2), literal(3)])
        expect(decoded).not.toHaveProperty('corruption')
      })
    })

    describe('a partial tail is the end of the block, not an error', () => {
      test.each(Array.from({ length: 16 }, (_, index) => index + 1))(
        'a RECORDS field of %i byte(s) decodes nothing',
        async (bytes) => {
          await expect(decode(records(first.subarray(0, bytes)), options)).resolves.toEqual({
            messages: [],
          })
        }
      )

      test('a first batch cut anywhere decodes nothing', async () => {
        for (let bytes = 17; bytes < first.length; bytes++) {
          const decoded = await decode(records(first.subarray(0, bytes)), options)

          expect({ bytes, decoded }).toEqual({ bytes, decoded: { messages: [] } })
        }
      })

      test('a second batch cut anywhere keeps the first batch and nothing of its own', async () => {
        for (let bytes = 1; bytes < second.length; bytes++) {
          const decoded = await decode(records(first, second.subarray(0, bytes)), options)

          expect({ bytes, messages: decoded.messages.map(asLiteral) }).toEqual({
            bytes,
            messages: firstMessages,
          })
          expect(decoded).not.toHaveProperty('corruption')
        }
      })

      test('a declared length above the bytes that follow is a partial tail, whatever it declares', async () => {
        const declaring = batch({ baseOffset: 2n, length: 0x7fffffff, records: [record()] })

        expect(await decode(records(first, declaring), options)).not.toHaveProperty('corruption')
      })

      test('a magic 1 batch after a magic 2 batch ends the block: the next fetch starts there', async () => {
        const legacy = batch({
          baseOffset: 2n,
          magic: 1,
          records: [record({ key: 'k2', value: 'v2' })],
        })
        const decoded = await decode(records(first, legacy), options)

        expect(decoded.messages.map(asLiteral)).toEqual(firstMessages)
        expect(decoded).not.toHaveProperty('corruption')
      })
    })

    describe('a corrupt complete batch is typed, carried and never swallowed', () => {
      const corrupt = async (forged, before = [first]) => {
        const decoded = await decode(records(...before, forged), options)
        expect(decoded).toHaveProperty('corruption')
        expect(decoded.corruption.error).toBeInstanceOf(KafkaJSCorruptRecordError)
        expect(decoded.corruption.error.reason).toBe(decoded.corruption.reason)
        expect(decoded.corruption.error.offset).toBe(decoded.corruption.offset)
        return decoded
      }

      test.each([-100, -49, -1, 0, 5, 13])(
        'a batch length of %i is corrupt, and the records before it are delivered',
        async (length) => {
          const { messages, corruption } = await corrupt(
            batch({ baseOffset: 2n, length, records: [record({ key: 'k2', value: 'v2' })] })
          )

          expect(messages.map(asLiteral)).toEqual(firstMessages)
          expect({ reason: corruption.reason, offset: corruption.offset }).toEqual({
            reason: 'length',
            offset: '2',
          })
        }
      )

      test.each([14, 30, 48])(
        'a magic 2 batch of length %i is shorter than its own header: corrupt',
        async (length) => {
          const { corruption } = await corrupt(
            batch({ baseOffset: 2n, length, records: [record({ key: 'k2', value: 'v2' })] })
          )

          expect(corruption).toMatchObject({ reason: 'length', offset: '2' })
        }
      )

      test('12 to 16 bytes with a length below the minimum are corrupt, as in Java; a valid length there is partial', async () => {
        const offsetAndLength = (length) => {
          const bytes = Buffer.alloc(12)
          bytes.writeBigInt64BE(2n)
          bytes.writeInt32BE(length, 8)
          return bytes
        }

        await expect(decode(records(first, offsetAndLength(-1)), options)).resolves.toMatchObject({
          corruption: { reason: 'length', offset: '2' },
        })
        expect(await decode(records(first, offsetAndLength(61)), options)).not.toHaveProperty(
          'corruption'
        )
      })

      test.each([-128, -1, 3, 7, 127])('a magic byte of %i is corrupt', async (magic) => {
        const { messages, corruption } = await corrupt(
          batch({ baseOffset: 2n, magic, records: [record({ key: 'k2', value: 'v2' })] })
        )

        expect(messages.map(asLiteral)).toEqual(firstMessages)
        expect(corruption).toMatchObject({ reason: 'magic', offset: '2' })
      })

      test('a corrupt first batch is carried at once with no records', async () => {
        const { messages, corruption } = await corrupt(
          batch({ baseOffset: 0n, length: -1, records: [record()] }),
          []
        )

        expect(messages).toEqual([])
        expect(corruption).toMatchObject({ reason: 'length', offset: '0' })
      })

      test('a first batch with a magic byte of 5 is corrupt, not a message set', async () => {
        const { messages, corruption } = await corrupt(
          batch({ baseOffset: 7n, magic: 5, records: [record()] }),
          []
        )

        expect(messages).toEqual([])
        expect(corruption).toMatchObject({ reason: 'magic', offset: '7' })
      })

      test.each([
        ['length', { length: -1 }],
        ['magic', { magic: 9 }],
      ])(
        'the %s is judged before the CRC, so a wrong CRC cannot turn it into a crash',
        async (reason, forgery) => {
          const { corruption } = await corrupt(
            batch({ baseOffset: 2n, crc: 0xdeadbeef, records: [record()], ...forgery })
          )

          expect(corruption.reason).toBe(reason)
        }
      )

      test.each([-2147483648, -5, -1, 0x7fffffff, 100])(
        'a record count of %i, which the batch has no room for, is corrupt',
        async (count) => {
          const { messages, corruption } = await corrupt(
            batch({ baseOffset: 2n, count, records: [record({ key: 'k2', value: 'v2' })] })
          )

          expect(messages.map(asLiteral)).toEqual(firstMessages)
          expect(corruption).toMatchObject({ reason: 'count', offset: '2' })
        }
      )

      test('a count of 1 more than the batch can hold at 7 bytes per record is corrupt, 7 bytes per record exactly is not', async () => {
        const empties = Array.from({ length: 3 }, (_, index) => record({ offsetDelta: index }))
        const holds = 3 * record().length

        expect(holds).toBe(21)
        expect(
          await decode(records(batch({ records: empties, count: 3 })), options)
        ).not.toHaveProperty('corruption')
        await expect(
          decode(records(batch({ records: empties, count: 4 })), options)
        ).resolves.toMatchObject({ corruption: { reason: 'count' } })
      })

      test('the count guard protects checkCrcs: false, the CRC protects the rest', async () => {
        const forged = batch({ baseOffset: 2n, count: 0x7fffffff, crc: 1, records: [record()] })

        if (checkCrcs) {
          await expect(decode(records(first, forged), options)).resolves.toMatchObject({
            corruption: { reason: 'checksum', offset: '2' },
          })
        } else {
          await expect(decode(records(first, forged), options)).resolves.toMatchObject({
            corruption: { reason: 'count' },
          })
        }
      })

      test('a record whose length runs past its batch is a malformed body: corrupt, not a partial tail', async () => {
        const overrun = Buffer.from([0xd0, 0x0f, 0, 0, 0, 0, 0, 0, 0])
        const { messages, corruption } = await corrupt(
          batch({ baseOffset: 2n, records: [overrun] })
        )

        expect(messages.map(asLiteral)).toEqual(firstMessages)
        expect(corruption).toMatchObject({ reason: 'length', offset: '2' })
        expect(corruption.error.cause).toBeInstanceOf(KafkaJSPartialMessageError)
      })

      test('a record of length -1 is a malformed body: corrupt, not a TypeError', async () => {
        const nullLength = Buffer.concat([varint(-1), Buffer.alloc(6)])
        const { messages, corruption } = await corrupt(
          batch({ baseOffset: 2n, count: 1, records: [nullLength] })
        )

        expect(messages.map(asLiteral)).toEqual(firstMessages)
        expect(corruption).toMatchObject({ reason: 'length', offset: '2' })
      })

      test('a count above the records the batch holds, within the bound, is a malformed body', async () => {
        const held = [
          record({ value: 'x'.repeat(30) }),
          record({ offsetDelta: 1, value: 'y'.repeat(30) }),
        ]

        expect(Buffer.concat(held).length / 7).toBeGreaterThanOrEqual(3)

        const { corruption } = await corrupt(batch({ baseOffset: 2n, count: 3, records: held }))

        expect(corruption).toMatchObject({ reason: 'length', offset: '2' })
      })

      test('a timestamp delta of more than ten continuation bytes is a malformed body', async () => {
        const body = Buffer.concat([Buffer.from([0]), Buffer.alloc(11, 0x80), Buffer.alloc(6)])
        const overlong = Buffer.concat([varint(body.length), body])
        const { corruption } = await corrupt(batch({ baseOffset: 2n, records: [overlong] }))

        expect(corruption).toMatchObject({ reason: 'length', offset: '2' })
      })

      test('a CRC mismatch is carried like any other corrupt batch, with the batches before it', async () => {
        const wrongCrc = batch({ baseOffset: 2n, crc: 0xdeadbeef, records: [record()] })

        if (checkCrcs) {
          const { messages, corruption } = await decode(records(first, wrongCrc), options)

          expect(messages.map(asLiteral)).toEqual(firstMessages)
          expect(corruption).toMatchObject({ reason: 'checksum', offset: '2' })
          expect(corruption.error).not.toHaveProperty('nextOffset')
        } else {
          expect(await decode(records(first, wrongCrc), options)).not.toHaveProperty('corruption')
        }
      })
    })

    describe('records of a batch', () => {
      const gzipped = (batchRecords, overrides = {}) =>
        batch({
          baseOffset: 2n,
          attributes: 1,
          records: [zlib.gzipSync(Buffer.concat(batchRecords))],
          count: batchRecords.length,
          ...overrides,
        })
      const pair = [
        record({ key: 'k2', value: 'v2' }),
        record({ offsetDelta: 1, key: 'k3', value: 'v3' }),
      ]

      test('a batch with none is whole: nothing is decoded and nothing is carried', async () => {
        const decoded = await decode(records(first, batch({ baseOffset: 2n })), options)

        expect(decoded.messages.map(asLiteral)).toEqual(firstMessages)
        expect(decoded).not.toHaveProperty('corruption')
      })

      test('a compressed batch with none is whole too: its payload is never opened', async () => {
        const unreadable = batch({
          baseOffset: 2n,
          attributes: 1,
          records: [Buffer.from('not gzip')],
          count: 0,
        })

        const decoded = await decode(records(first, unreadable), options)

        expect(decoded.messages.map(asLiteral)).toEqual(firstMessages)
        expect(decoded).not.toHaveProperty('corruption')
      })

      test('a batch of more records than a call can take as arguments is decoded whole', async () => {
        const decoded = await decode(records(many(), valid(MANY)), options)

        expect(decoded.messages).toHaveLength(MANY + 2)
        expect(decoded.messages[0].offset).toBe('0')
        expect(decoded.messages[MANY - 1].offset).toBe(String(MANY - 1))
        expect(decoded.messages.slice(MANY).map(asLiteral)).toEqual([
          literal(MANY),
          literal(MANY + 1),
        ])
        expect(decoded).not.toHaveProperty('corruption')
      })

      test('a gzip batch is decompressed and decoded', async () => {
        const decoded = await decode(records(first, gzipped(pair)), options)

        expect(decoded.messages.map(asLiteral)).toEqual([...firstMessages, literal(2), literal(3)])
        expect(decoded).not.toHaveProperty('corruption')
      })

      test.each([-1, 0x7fffffff, 100])(
        'a gzip batch with a count of %i, which its decompressed bytes cannot hold, is corrupt',
        async (count) => {
          const decoded = await decode(records(first, gzipped(pair, { count })), options)

          expect(decoded.messages.map(asLiteral)).toEqual(firstMessages)
          expect(decoded.corruption).toMatchObject({ reason: 'count', offset: '2' })
        }
      )
    })

    describe('control records', () => {
      const control = async (forged) => {
        const decoded = await decode(records(first, forged), options)
        return { ...decoded, messages: decoded.messages.slice(firstMessages.length) }
      }

      test.each(['ABORT', 'COMMIT'])('a %s marker decodes as a control record', async (type) => {
        const decoded = await control(controlBatch({ type, baseOffset: 2n }))

        expect(decoded).not.toHaveProperty('corruption')
        expect(decoded.messages).toHaveLength(1)
        expect(decoded.messages[0]).toMatchObject({
          offset: '2',
          isControlRecord: true,
          key: Buffer.from([0, 0, 0, type === 'ABORT' ? 0 : 1]),
          value: Buffer.from([0, 0, 0, 0, 0, 117]),
        })
      })

      test.each([
        ['null', null],
        ['3 bytes', Buffer.from([0, 0, 0])],
        ['a negative version', Buffer.from([0xff, 0xff, 0, 1])],
      ])('a control key that is %s is corrupt', async (_, key) => {
        const forged = batch({
          baseOffset: 2n,
          attributes: 0x30,
          records: [record({ key, value: Buffer.from([0, 0, 0, 0, 0, 117]) })],
        })
        const { corruption } = await control(forged)

        expect(corruption).toMatchObject({ reason: 'control', offset: '2' })
      })

      test.each([
        ['null', null],
        ['one byte', Buffer.from([0])],
        ['a negative version', Buffer.from([0x80, 0, 0, 0, 0, 117])],
      ])('the value of a transaction marker that is %s is corrupt', async (_, value) => {
        const { corruption } = await control(controlBatch({ baseOffset: 2n, value }))

        expect(corruption).toMatchObject({ reason: 'control', offset: '2' })
      })

      test('a key version above the highest known is read as the highest, not rejected', async () => {
        const decoded = await control(
          controlBatch({ baseOffset: 2n, key: Buffer.from([0, 9, 0, 1]) })
        )

        expect(decoded).not.toHaveProperty('corruption')
      })

      test('an unknown control type is skipped: its record is decoded and nothing is carried', async () => {
        const decoded = await control(
          controlBatch({ type: 5, baseOffset: 2n, value: Buffer.from([1]) })
        )

        expect(decoded).not.toHaveProperty('corruption')
        expect(decoded.messages).toHaveLength(1)
        expect(decoded.messages[0]).toMatchObject({
          isControlRecord: true,
          key: Buffer.from([0, 0, 0, 5]),
        })
      })

      test('a data record with the key of an abort marker is legal data', async () => {
        const data = batch({
          baseOffset: 2n,
          records: [record({ key: Buffer.from([0, 0, 0, 0]), value: 'v2' })],
        })
        const decoded = await control(data)

        expect(decoded).not.toHaveProperty('corruption')
        expect(decoded.messages[0]).toMatchObject({
          isControlRecord: false,
          key: Buffer.from([0, 0, 0, 0]),
        })
      })

      test('a data record with a key shorter than a control key is legal data', async () => {
        const decoded = await control(
          batch({ baseOffset: 2n, records: [record({ key: Buffer.from([1]), value: 'v2' })] })
        )

        expect(decoded).not.toHaveProperty('corruption')
      })
    })
  })

  describe('a legacy message set', () => {
    /**
     * A magic 1 message with no key: the CRC (which the decoder does not check), the magic, the
     * attributes, the timestamp, the key and the value.
     */
    const legacyMessage = ({ attributes = 0, value = null } = {}) => {
      const head = Buffer.alloc(18)
      head.writeInt8(1, 4)
      head.writeInt8(attributes, 5)
      head.writeInt32BE(-1, 14)
      const valueSize = Buffer.alloc(4)
      valueSize.writeInt32BE(value === null ? -1 : value.length)
      return Buffer.concat([head, valueSize, ...(value === null ? [] : [value])])
    }

    /** A message set entry: the offset, the size, the message. */
    const entry = (offset, message) => {
      const header = Buffer.alloc(12)
      header.writeBigInt64BE(BigInt(offset))
      header.writeInt32BE(message.length, 8)
      return Buffer.concat([header, message])
    }

    test('a gzip wrapper of more messages than a call can take as arguments is decoded whole', async () => {
      // the inner offsets are relative and the wrapper holds the absolute offset of the last one
      const inner = Array.from({ length: MANY }, (_, index) => entry(index, legacyMessage()))
      const lastOffset = 1000 + MANY - 1
      const wrapper = entry(
        lastOffset,
        legacyMessage({ attributes: 1, value: zlib.gzipSync(Buffer.concat(inner)) })
      )

      const { messages } = await decode(records(wrapper))

      expect(messages).toHaveLength(MANY)
      expect(messages[0].offset).toBe('1000')
      expect(messages[MANY - 1].offset).toBe(String(lastOffset))
    })
  })

  describe('the Fetch response codecs carry the marker on the partition', () => {
    const scala = Buffer.from(require(`${FIXTURES}/v4_from_scala_producer_response.json`))
    const SCALA_BATCHES = [91, 198, 305, 412, 519, 626]

    /** The fixture with the length of its batch at `at` replaced by -1. */
    const withLengthOf = (fixture, at, length = -1) => {
      const copy = Buffer.from(fixture)
      copy.writeInt32BE(length, at + 8)
      return copy
    }

    const partitionOf = (decoded) => decoded.responses[0].partitions[0]

    test('v4 delivers the batches before a corrupt one and names the corrupt one', async () => {
      const { decode: decodeV4 } = require('../v4/response')
      const partition = partitionOf(await decodeV4(withLengthOf(scala, SCALA_BATCHES[3])))

      expect(partition.messages.map(({ offset }) => offset)).toEqual(['0', '1', '2'])
      expect(partition.corruption).toMatchObject({ reason: 'length', offset: '3' })
      expect(partition.corruption.error).toBeInstanceOf(KafkaJSCorruptRecordError)
    })

    test('v4 leaves no corruption property on a partition that has none', async () => {
      const { decode: decodeV4 } = require('../v4/response')
      const partition = partitionOf(await decodeV4(scala))

      expect(partition.messages).toHaveLength(6)
      expect('corruption' in partition).toBe(false)
    })

    test.each([
      ['v4', 'v4_response', 75],
      ['v5', 'v5_response', 125],
      ['v7', 'v7_response', 131],
      ['v8', 'v8_response', 131],
      ['v11', 'v11_response', 135],
    ])('%s carries it too, from a corrupt first batch', async (version, name, at) => {
      const { decode: decodeVersion } = require(`../${version}/response`)
      const fixture = Buffer.from(require(`${FIXTURES}/${name}.json`))
      const corrupt = partitionOf(await decodeVersion(withLengthOf(fixture, at)))
      const intact = partitionOf(await decodeVersion(fixture))

      expect(corrupt.messages).toEqual([])
      expect(corrupt.corruption).toMatchObject({ reason: 'length', offset: '0' })
      expect(intact.messages.map(({ offset }) => offset)).toEqual(['0', '1', '2'])
      expect('corruption' in intact).toBe(false)
    })

    test.each([12, 13, 18])('v%i carries it through the compact RECORDS field', async (version) => {
      const { decode: decodeVersion } = require(`../v${version}/response`)
      const body = (block) =>
        fetchResponse([{ partition: 0, errorCode: 0, records: block }], { apiVersion: version })
      const partition = async (block) => partitionOf(await decodeVersion(body(block)))

      const corrupt = await partition(
        Buffer.concat([valid(0), batch({ baseOffset: 2n, length: 5 })])
      )
      const intact = await partition(valid(0))

      expect(corrupt.messages.map(asLiteral)).toEqual(firstOf(2))
      expect(corrupt.corruption).toMatchObject({ reason: 'length', offset: '2' })
      expect(intact.messages.map(asLiteral)).toEqual(firstOf(2))
      expect('corruption' in intact).toBe(false)
    })
  })
})
