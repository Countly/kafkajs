const zlib = require('zlib')
const gzip = require('../../message/compression/gzip')
const {
  compressedBatch,
  recordsField,
  decodeField,
} = require('../../message/compression/fixtures/compressedBatch')

/** Decodes one batch as a Fetch partition's RECORDS block does. */
const decode = async (fields, options) =>
  decodeField(recordsField(await compressedBatch(fields)), options)

describe('Protocol > RecordBatch > v0 > the record count of a batch', () => {
  describe.each([
    ['gzip', { codec: 'gzip' }],
    ['uncompressed', { codec: 'none' }],
  ])('%s', (_, shape) => {
    describe.each([true, false])('with checkCrcs %s', (checkCrcs) => {
      // 5120 bytes allow 10 records, 511 bytes allow 1 (and never fewer than 1)
      test.each([
        [5120, 10],
        [511, 1],
      ])('a bound of %d bytes holds %d records and refuses one more', async (bound, ceiling) => {
        const options = { checkCrcs, maxDecompressedBatchBytes: bound }

        const held = await decode({ ...shape, records: ceiling }, options)
        const over = await decode({ ...shape, records: ceiling + 1 }, options)

        expect(held.corruption).toBeUndefined()
        expect(held.messages).toHaveLength(ceiling)
        expect(over.messages).toEqual([])
        expect(over.corruption).toMatchObject({ reason: 'count', offset: '0' })
        expect(over.corruption.error).toMatchObject({
          reason: 'count',
          limit: ceiling,
          nextOffset: String(ceiling + 1),
        })
        expect(over.corruption.error.message).toContain(`${ceiling + 1}`)
        expect(over.corruption.error.message).toContain(`${ceiling} records`)
      })
    })
  })

  test('a compressed batch names its codec when its count is refused', async () => {
    const { corruption } = await decode(
      { codec: 'gzip', records: 2 },
      { maxDecompressedBatchBytes: 511 }
    )

    expect(corruption.error).toMatchObject({ codec: 'gzip', limit: 1 })
  })

  test('an uncompressed batch has no codec on the error', async () => {
    const { corruption } = await decode(
      { codec: 'none', records: 2 },
      { maxDecompressedBatchBytes: 511 }
    )

    expect(corruption.error).not.toHaveProperty('codec')
  })

  describe('before anything is decompressed or allocated', () => {
    test.each([2147483647, 33000000])('a count of %d never reaches the codec', async (count) => {
      const decompress = jest.spyOn(gzip, 'decompress')
      try {
        const { corruption } = await decode({ codec: 'gzip', records: 3, count })

        expect(corruption).toMatchObject({ reason: 'count' })
        expect(corruption.error).toMatchObject({ limit: 2097152 })
        expect(decompress).not.toHaveBeenCalled()
      } finally {
        decompress.mockRestore()
      }
    })

    test('a count of 33 million over an empty gzip payload grows the heap by under 64 MiB', async () => {
      const empty = zlib.gzipSync(Buffer.alloc(0))
      const before = process.memoryUsage().heapUsed

      const { corruption } = await decode({
        codec: 'gzip',
        records: 0,
        count: 33000000,
        body: empty,
      })

      expect(corruption).toMatchObject({ reason: 'count' })
      expect(process.memoryUsage().heapUsed - before).toBeLessThan(64 * 1048576)
    })

    test('a count within the ceiling that the decompressed bytes cannot hold is refused before its array', async () => {
      const empty = zlib.gzipSync(Buffer.alloc(0))
      const options = { maxDecompressedBatchBytes: 2147483647 }
      const before = process.memoryUsage().heapUsed

      // the largest count the ceiling of this bound allows: 2147483647 / 512
      const { corruption } = await decode(
        { codec: 'gzip', records: 0, count: 4194303, body: empty },
        options
      )

      expect(corruption).toMatchObject({ reason: 'count' })
      expect(corruption.error).not.toHaveProperty('limit')
      expect(process.memoryUsage().heapUsed - before).toBeLessThan(16 * 1048576)
    })

    test('a count of zero over a compressed body is an empty batch', async () => {
      const { messages, corruption } = await decode({ codec: 'gzip', records: 3, count: 0 })

      expect(messages).toEqual([])
      expect(corruption).toBeUndefined()
    })
  })

  describe('the offset after a refused batch', () => {
    const tooMany = { codec: 'none', records: 2 }
    const options = { maxDecompressedBatchBytes: 511 }

    test.each([
      [0n, 1, '2'],
      [100n, 10, '111'],
      [9007199254740993n, 1, '9007199254740995'],
    ])(
      'a batch at %s with a last offset delta of %d ends before %s',
      async (baseOffset, delta, next) => {
        const { corruption } = await decode(
          { ...tooMany, baseOffset, lastOffsetDelta: delta },
          options
        )

        expect(corruption.error.nextOffset).toBe(next)
      }
    )

    test('is absent when the last offset delta is negative', async () => {
      const { corruption } = await decode({ ...tooMany, lastOffsetDelta: -1 }, options)

      expect(corruption).toMatchObject({ reason: 'count' })
      expect(corruption.error).not.toHaveProperty('nextOffset')
    })
  })
})
