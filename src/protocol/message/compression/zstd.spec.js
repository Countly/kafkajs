/* eslint-disable n/no-unsupported-features/node-builtins -- the zstd cases run only when zlib has zstd */
const zlib = require('zlib')
const { Types, lookupCodec } = require('./index')

const hasZstd = typeof zlib.zstdCompressSync === 'function'
const describeIfZstd = hasZstd ? describe : describe.skip

// Deterministic, "varied" (non-repeating) text: real compressors respond to compression level
// mainly on data with actual structure — a short repeated unit collapses to near-nothing at
// every level, and uniform random bytes barely compress at any level, so neither would catch a
// dropped or misspelled level option.
const variedPayload = () => {
  const words = [
    'lorem',
    'ipsum',
    'dolor',
    'sit',
    'amet',
    'consectetur',
    'adipiscing',
    'elit',
    'sed',
    'do',
    'eiusmod',
    'tempor',
    'incididunt',
    'ut',
    'labore',
  ]
  let seed = 42
  const next = () => {
    seed = (seed * 1103515245 + 12345) % 2147483648
    return seed / 2147483648
  }
  let text = ''
  for (let i = 0; i < 5000; i++) {
    text += words[Math.floor(next() * words.length)] + ' '
  }
  return Buffer.from(text)
}

describe('Protocol > Compression > ZSTD', () => {
  it('does not throw at require time on Node without zstd support', () => {
    expect(() => require('./zstd')).not.toThrow()
  })

  describe('when node:zlib lacks zstd support', () => {
    afterEach(() => {
      jest.dontMock('zlib')
      jest.resetModules()
    })

    it('does not throw at require time, and gives an actionable error naming the Node version', () => {
      jest.doMock('zlib', () => {
        const actualZlib = jest.requireActual('zlib')
        const stub = { ...actualZlib }
        delete stub.zstdCompressSync
        delete stub.zstdCompress
        delete stub.zstdDecompress
        return stub
      })

      // The codec lookup only calls `require('./zstd')`/`require('./gzip')` once `lookupCodec`
      // actually runs, so the assertions have to happen inside `isolateModules` too — outside
      // it, they'd hit the normal, unmocked module registry again.
      jest.isolateModules(() => {
        let isolatedZstd
        expect(() => {
          isolatedZstd = require('./zstd')
        }).not.toThrow()
        expect(isolatedZstd.isSupported).toBe(false)

        const isolatedIndex = require('./index')
        expect(() => isolatedIndex.lookupCodec(isolatedIndex.Types.ZSTD)).toThrow(/22\.15/)

        // GZIP is unaffected by the missing zstd support.
        expect(isolatedIndex.lookupCodec(isolatedIndex.Types.GZIP)).toBeTruthy()
      })
    })
  })

  describeIfZstd('when node:zlib provides zstd', () => {
    it('round-trips a buffer', async () => {
      const codec = lookupCodec(Types.ZSTD)
      const payload = Buffer.from('the quick brown fox '.repeat(200))
      const compressed = await codec.compress({ buffer: payload })
      expect(compressed.length).toBeLessThan(payload.length)
      await expect(codec.decompress(compressed)).resolves.toEqual(payload)
    })

    it('honors the compression level option', async () => {
      const codec = require('./zstd')
      const payload = variedPayload()

      const low = await codec.compress({ buffer: payload }, { level: 1 })
      const high = await codec.compress({ buffer: payload }, { level: 19 })
      expect(high.length).toBeLessThan(low.length)

      // No options at all must match the documented default (level 3) byte-for-byte.
      const defaultLevel = await codec.compress({ buffer: payload })
      const explicitLevel3 = await codec.compress({ buffer: payload }, { level: 3 })
      expect(defaultLevel).toEqual(explicitLevel3)
    })

    describe('a stream cut short', () => {
      const codec = require('./zstd')
      const payload = variedPayload()
      const framed = (params = {}) =>
        zlib.zstdCompressSync(payload, {
          params: { [zlib.constants.ZSTD_c_checksumFlag]: 0, ...params },
        })
      const cutAt = (frame, at) => frame.subarray(0, at)

      it.each([
        ['inside the frame header', 5],
        ['inside the first block header', 7],
        ['inside a block body', 40],
      ])('is refused %s, like on Node 26', async (_, at) => {
        await expect(codec.decompress(cutAt(framed(), at))).rejects.toMatchObject({
          code: 'Z_BUF_ERROR',
          errno: -5,
          message: 'unexpected end of file',
        })
      })

      it('is refused when the last block is missing', async () => {
        const frame = framed()
        await expect(codec.decompress(cutAt(frame, frame.length - 1))).rejects.toMatchObject({
          code: 'Z_BUF_ERROR',
        })
      })

      it('is refused inside the checksum', async () => {
        const frame = framed({ [zlib.constants.ZSTD_c_checksumFlag]: 1 })
        await expect(codec.decompress(frame)).resolves.toEqual(payload)
        for (const cut of [1, 2, 3, 4]) {
          await expect(codec.decompress(cutAt(frame, frame.length - cut))).rejects.toMatchObject({
            code: 'Z_BUF_ERROR',
          })
        }
      })

      it('is refused when it is empty or holds only the magic number', async () => {
        await expect(codec.decompress(Buffer.alloc(0))).rejects.toMatchObject({
          code: 'Z_BUF_ERROR',
        })
        await expect(codec.decompress(Buffer.from([0x28, 0xb5, 0x2f, 0xfd]))).rejects.toMatchObject(
          { code: 'Z_BUF_ERROR' }
        )
      })

      it('is refused at every cut point of a multi-block frame', async () => {
        const big = Buffer.concat(Array.from({ length: 40 }, variedPayload))
        const frame = zlib.zstdCompressSync(big, {
          params: { [zlib.constants.ZSTD_c_checksumFlag]: 1 },
        })
        await expect(codec.decompress(frame)).resolves.toEqual(big)
        for (let at = 0; at < frame.length; at += Math.ceil(frame.length / 97)) {
          await expect(codec.decompress(cutAt(frame, at))).rejects.toMatchObject({
            code: 'Z_BUF_ERROR',
          })
        }
      })
    })

    describe('a complete stream', () => {
      const codec = require('./zstd')
      const payload = variedPayload()

      it('decompresses with and without a checksum', async () => {
        for (const checksum of [0, 1]) {
          const frame = zlib.zstdCompressSync(payload, {
            params: { [zlib.constants.ZSTD_c_checksumFlag]: checksum },
          })
          await expect(codec.decompress(frame)).resolves.toEqual(payload)
        }
      })

      it('decompresses a RLE block and a raw block', async () => {
        // single segment, content size 3, one last RLE block of 3 bytes of 0x07
        const rle = Buffer.from('28b52ffd20031b000007', 'hex')
        await expect(codec.decompress(rle)).resolves.toEqual(Buffer.alloc(3, 7))
        await expect(codec.decompress(rle.subarray(0, -1))).rejects.toMatchObject({
          code: 'Z_BUF_ERROR',
        })
        const raw = zlib.zstdCompressSync(Buffer.from([1, 2, 3]))
        await expect(codec.decompress(raw)).resolves.toEqual(Buffer.from([1, 2, 3]))
      })

      // node:zlib decodes the first frame only and ignores what follows it, on every version.
      it('leaves a frame that follows the first one to node:zlib', async () => {
        const frame = zlib.zstdCompressSync(payload)
        const skippable = Buffer.from([0x50, 0x2a, 0x4d, 0x18, 2, 0, 0, 0, 1, 2])
        const twice = Buffer.concat([frame, frame])
        const expected = zlib.zstdDecompressSync(twice)
        await expect(codec.decompress(twice)).resolves.toEqual(expected)
        const skipped = Buffer.concat([skippable, frame])
        await expect(codec.decompress(skipped)).resolves.toEqual(zlib.zstdDecompressSync(skipped))
      })
    })

    it('still rejects what is not zstd', async () => {
      const codec = require('./zstd')
      await expect(codec.decompress(Buffer.from('not a zstd stream'))).rejects.toMatchObject({
        code: 'ZSTD_error_prefix_unknown',
      })
      const reserved = Buffer.from([0x28, 0xb5, 0x2f, 0xfd, 0x20, 0x00, 0x07, 0x00, 0x00])
      await expect(codec.decompress(reserved)).rejects.toMatchObject({
        code: 'ZSTD_error_corruption_detected',
      })
    })

    it('refuses a cut-short frame whose header sets the reserved bit', async () => {
      const codec = require('./zstd')
      const headerOnly = Buffer.from('28b52ffd28', 'hex')
      await expect(codec.decompress(headerOnly)).rejects.toMatchObject({ code: 'Z_BUF_ERROR' })
    })

    it('takes its output bound from node:zlib: exactly at it passes, one byte over is refused while it decompresses', async () => {
      const codec = lookupCodec(Types.ZSTD)
      const compressed = await codec.compress({ buffer: Buffer.alloc(100000, 7) })

      await expect(codec.decompress(compressed, { maxOutputLength: 100000 })).resolves.toHaveLength(
        100000
      )
      await expect(codec.decompress(compressed, { maxOutputLength: 99999 })).rejects.toMatchObject({
        name: 'RangeError',
        code: 'ERR_BUFFER_TOO_LARGE',
      })
    })
  })
})
