const { Types, Codecs, lookupCodec } = require('./index')

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

  it('lets a user-registered codec win over the built-in one', () => {
    const customCodec = { compress: jest.fn(), decompress: jest.fn() }
    const originalCodec = Codecs[Types.LZ4]
    Codecs[Types.LZ4] = () => customCodec
    try {
      expect(lookupCodec(Types.LZ4)).toBe(customCodec)
    } finally {
      Codecs[Types.LZ4] = originalCodec
    }
  })

  describe('when lz4-napi is genuinely absent', () => {
    afterEach(() => {
      jest.dontMock('lz4-napi')
      jest.resetModules()
    })

    it('does not throw at require time, and gives an actionable error naming lz4-napi', () => {
      jest.doMock('lz4-napi', () => {
        throw Object.assign(new Error("Cannot find module 'lz4-napi'"), {
          code: 'MODULE_NOT_FOUND',
        })
      })

      // The codec lookup only calls `require('./lz4')` once `lookupCodec` actually runs, so the
      // assertion has to happen inside `isolateModules` too — outside it, it would hit the
      // normal, unmocked module registry again.
      jest.isolateModules(() => {
        let isolatedLz4
        expect(() => {
          isolatedLz4 = require('./lz4')
        }).not.toThrow()
        expect(isolatedLz4.isSupported).toBe(false)

        const isolatedIndex = require('./index')
        expect(() => isolatedIndex.lookupCodec(isolatedIndex.Types.LZ4)).toThrow(/lz4-napi/)
      })
    })
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
