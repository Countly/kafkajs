const { Types, lookupCodec } = require('./index')

let hasLz4 = true
try {
  require('lz4-napi')
} catch (e) {
  hasLz4 = false
}
const describeIfLz4 = hasLz4 ? describe : describe.skip

describe('Protocol > Compression > LZ4', () => {
  it('gives an actionable error when the optional dependency is missing', () => {
    if (hasLz4) {
      return expect(() => lookupCodec(Types.LZ4)).not.toThrow()
    }
    expect(() => lookupCodec(Types.LZ4)).toThrow(/lz4-napi/)
  })

  describeIfLz4('when lz4-napi is installed', () => {
    it('round-trips a buffer through the LZ4 frame format', async () => {
      const codec = lookupCodec(Types.LZ4)
      const payload = Buffer.from('the quick brown fox '.repeat(200))
      const compressed = await codec.compress({ buffer: payload })
      await expect(codec.decompress(compressed)).resolves.toEqual(payload)
    })
  })
})
