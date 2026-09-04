const zlib = require('zlib')
const { Types, lookupCodec } = require('./index')

// eslint-disable-next-line n/no-unsupported-features/node-builtins -- feature detection, not a call
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
  })
})
