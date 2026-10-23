const { requests } = require('../index')
const decoderCache = require('./decoderCache')
const { MIB, bodyOf, bombOf } = require('../../message/compression/fixtures/hostileCases')
const { compressedBatch } = require('../../message/compression/fixtures/compressedBatch')

const VERSIONS = Array.from({ length: 15 }, (_, index) => 4 + index)

describe('Protocol > Requests > Fetch > the decode options of a request', () => {
  describe.each(VERSIONS)('version %d', (version) => {
    const response = require(`./v${version}/response`)
    let decode

    beforeEach(() => {
      decode = jest.spyOn(response, 'decode').mockResolvedValue({ responses: [] })
    })

    afterEach(() => {
      decode.mockRestore()
    })

    const built = (options) =>
      requests.Fetch.protocol({ version })({
        replicaId: -1,
        maxWaitTime: 5000,
        minBytes: 1,
        maxBytes: 1048576,
        topics: [],
        topicIds: new Map(),
        ...options,
      })

    test('hands the checksum switch and the bound to the response decoder', async () => {
      await built({ checkCrcs: false, maxDecompressedBatchBytes: 777 }).response.decode(
        Buffer.alloc(4)
      )

      expect(decode).toHaveBeenCalledWith(expect.any(Buffer), {
        checkCrcs: false,
        maxDecompressedBatchBytes: 777,
      })
    })

    test('leaves the bound to the default when the request names none', async () => {
      await built({}).response.decode(Buffer.alloc(4))

      expect(decode).toHaveBeenCalledWith(expect.any(Buffer), {
        checkCrcs: true,
        maxDecompressedBatchBytes: undefined,
      })
    })
  })

  describe('the reader chains', () => {
    const build = () => jest.fn((options) => async () => options)

    test('are built once for a pair of options, however it is spelled', () => {
      const builder = build()
      const chainFor = decoderCache(builder)

      const first = chainFor({ checkCrcs: true, maxDecompressedBatchBytes: 1073741824 })

      expect(chainFor()).toBe(first)
      expect(chainFor({})).toBe(first)
      expect(chainFor({ checkCrcs: undefined })).toBe(first)
      expect(builder).toHaveBeenCalledTimes(1)
      expect(builder).toHaveBeenCalledWith({
        checkCrcs: true,
        maxDecompressedBatchBytes: 1073741824,
      })
    })

    test('differ by the checksum switch and by the bound', () => {
      const chainFor = decoderCache(build())

      const chains = new Set([
        chainFor({ checkCrcs: true }),
        chainFor({ checkCrcs: false }),
        chainFor({ checkCrcs: true, maxDecompressedBatchBytes: 4096 }),
        chainFor({ checkCrcs: false, maxDecompressedBatchBytes: 4096 }),
      ])

      expect(chains.size).toBe(4)
    })

    test('are at most 16, the oldest evicted first', () => {
      const builder = build()
      const chainFor = decoderCache(builder)

      for (let bound = 1; bound <= 17; bound++) {
        chainFor({ maxDecompressedBatchBytes: bound })
      }
      expect(builder).toHaveBeenCalledTimes(17)

      chainFor({ maxDecompressedBatchBytes: 17 })
      chainFor({ maxDecompressedBatchBytes: 2 })
      expect(builder).toHaveBeenCalledTimes(17)

      chainFor({ maxDecompressedBatchBytes: 1 })
      expect(builder).toHaveBeenCalledTimes(18)
    })

    test('keep two consumers with different bounds apart in the one module', async () => {
      const { decode } = require('./v12/response')
      const bomb = bodyOf(
        await compressedBatch({ codec: 'gzip', body: await bombOf('gzip', 8), records: 3 })
      )
      const markerOf = async (bound) =>
        (await decode(bomb, { checkCrcs: false, maxDecompressedBatchBytes: bound })).responses[0]
          .partitions[0].corruption.reason

      // 8 MiB of zeros: refused under 4 MiB, decompressed and found malformed under 9 MiB
      expect(await markerOf(4 * MIB)).toBe('decompressed-size')
      expect(await markerOf(9 * MIB)).toBe('length')
      expect(await markerOf(4 * MIB)).toBe('decompressed-size')
    })
  })
})
