const zlib = require('zlib')
const { Types, lookupCodec } = require('./index')

// eslint-disable-next-line n/no-unsupported-features/node-builtins -- feature detection, not a call
const hasZstd = typeof zlib.zstdCompressSync === 'function'
const describeIfZstd = hasZstd ? describe : describe.skip

describe('Protocol > Compression > ZSTD', () => {
  it('does not throw at require time on Node without zstd support', () => {
    expect(() => require('./zstd')).not.toThrow()
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
      const payload = Buffer.from('abcabcabc'.repeat(5000))
      const low = await codec.compress({ buffer: payload }, { level: 1 })
      const high = await codec.compress({ buffer: payload }, { level: 19 })
      expect(high.length).toBeLessThanOrEqual(low.length)
    })
  })
})
