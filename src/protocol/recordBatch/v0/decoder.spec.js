const zlib = require('zlib')
const Decoder = require('../../decoder')
const recordBatchDecoder = require('./decoder')
const { KafkaJSCorruptRecordError } = require('../../../errors')
const { batch, record } = require('../../requests/fetch/__tests__/fixtures/recordBatchBytes')
const {
  compressedBatch,
  recordsField,
  decodeField,
} = require('../../message/compression/fixtures/compressedBatch')

const { HEADER_CEILING_BYTES, RECORD_CEILING_BYTES } = recordBatchDecoder

describe('Protocol > RecordBatch > v0', () => {
  test('decodes', async () => {
    const decoder = new Decoder(Buffer.from(require('../fixtures/v0_recordbatch.json')))
    const decoded = await recordBatchDecoder(decoder)

    expect(decoded).toEqual({
      firstOffset: '0',
      firstTimestamp: '1597425188775',
      partitionLeaderEpoch: 0,
      inTransaction: false,
      isControlBatch: false,
      lastOffsetDelta: 0,
      producerId: '-1',
      producerEpoch: 0,
      firstSequence: 0,
      maxTimestamp: '1597425188809',
      timestampType: 1,
      records: expect.any(Object),
    })
  })

  describe('header ceiling', () => {
    // At least 8, and a bound of at least two records so the record ceiling never fires first.
    const CEILING = Math.max(8, Math.ceil((RECORD_CEILING_BYTES * 2) / HEADER_CEILING_BYTES))
    const BOUND = HEADER_CEILING_BYTES * CEILING
    const OPTIONS = { maxDecompressedBatchBytes: BOUND }

    /**
     * @param {number} count
     * @param {string} [prefix='h']
     * @returns {Array<[string, string]>} `count` headers with distinct keys
     */
    const headers = (count, prefix = 'h') =>
      Array.from({ length: count }, (_, n) => [`${prefix}${n}`, `v${n}`])

    /** @returns {{ readName: jest.SpyInstance }} a spy on the reader every header key goes through */
    const watchHeaderNames = () => ({ readName: jest.spyOn(Decoder.prototype, 'readVarIntString') })

    afterEach(() => jest.restoreAllMocks())

    test('refuses a batch whose header count crosses the header ceiling before reading the headers', async () => {
      const { readName } = watchHeaderNames()

      const { messages, corruption } = await decodeField(
        recordsField(batch({ records: [record({ headers: headers(CEILING + 1) })] })),
        OPTIONS
      )

      expect(messages).toEqual([])
      expect(corruption).toMatchObject({ reason: 'count', offset: '0' })
      expect(corruption.error).toBeInstanceOf(KafkaJSCorruptRecordError)
      expect(corruption.error).toMatchObject({
        reason: 'count',
        limit: CEILING,
        offset: '0',
        nextOffset: '1',
      })
      expect(readName).not.toHaveBeenCalled()
    })

    test('charges headers of every record against one batch budget', async () => {
      const half = Math.floor(CEILING / 2) + 1
      const { readName } = watchHeaderNames()

      const { messages, corruption } = await decodeField(
        recordsField(
          batch({
            records: [
              record({ headers: headers(half) }),
              record({ offsetDelta: 1, headers: headers(half) }),
            ],
          })
        ),
        OPTIONS
      )

      expect(messages).toEqual([])
      expect(corruption).toMatchObject({ reason: 'count', offset: '0' })
      expect(corruption.error).toMatchObject({ reason: 'count', limit: CEILING, nextOffset: '2' })
      expect(readName).toHaveBeenCalledTimes(half)
    })

    test('refuses a compressed record whose headers cross the ceiling before any header object is built', async () => {
      const ceiling = 512
      const nullHeaders = Array.from({ length: 1000 }, () => ['', null])
      const body = zlib.gzipSync(record({ headers: nullHeaders }))
      const readArray = Decoder.prototype.readVarIntArrayBytesBounded
      let elements = 0
      const arrayReader = jest
        .spyOn(Decoder.prototype, 'readVarIntArrayBytesBounded')
        .mockImplementation(function (reader, ...rest) {
          const counted = (decoder) => {
            elements += 1
            return reader(decoder)
          }
          return readArray.call(this, counted, ...rest)
        })

      const { messages, corruption } = await decodeField(
        recordsField(await compressedBatch({ codec: 'gzip', records: 1, body })),
        { maxDecompressedBatchBytes: HEADER_CEILING_BYTES * ceiling }
      )

      expect(messages).toEqual([])
      expect(corruption).toMatchObject({ reason: 'count', offset: '0' })
      expect(corruption.error).toMatchObject({
        reason: 'count',
        limit: ceiling,
        codec: 'gzip',
        nextOffset: '1',
      })
      expect(arrayReader).toHaveBeenCalledTimes(1)
      expect(elements).toBe(0)
    })

    test('reports the batch ceiling as the limit when a later record crosses it', async () => {
      const { messages, corruption } = await decodeField(
        recordsField(
          batch({
            records: [
              record({ headers: headers(CEILING) }),
              record({ offsetDelta: 1, headers: headers(1, 'late') }),
            ],
          })
        ),
        OPTIONS
      )

      expect(messages).toEqual([])
      expect(corruption.error).toBeInstanceOf(KafkaJSCorruptRecordError)
      expect(corruption.error).toMatchObject({ reason: 'count', limit: CEILING, nextOffset: '2' })
      expect(corruption.error.message).toBe(
        `Record header count 1 is above the 0 headers left of the batch header ceiling ${CEILING}`
      )
    })

    test('accepts a batch whose headers exactly fill the ceiling', async () => {
      const entries = headers(CEILING)

      const { messages, corruption } = await decodeField(
        recordsField(batch({ records: [record({ key: 'k', headers: entries })] })),
        OPTIONS
      )

      expect(corruption).toBeUndefined()
      expect(messages).toHaveLength(1)
      expect(messages[0].key).toEqual(Buffer.from('k'))
      expect(messages[0].headers).toEqual(
        Object.fromEntries(entries.map(([name, content]) => [name, Buffer.from(content)]))
      )
    })
  })
})
