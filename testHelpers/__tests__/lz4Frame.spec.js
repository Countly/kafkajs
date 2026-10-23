const lz4 = require('lz4-napi')
const { xxh32, sequence, runBlock, block, frame, extension } = require('../lz4Frame')

describe('testHelpers > lz4Frame', () => {
  describe('xxh32', () => {
    // The seed-0 vectors of xxHash's own sanity check (cli/xsum_sanity_check.c): the test buffer
    // of that file, a stream of the top bytes of a 64-bit multiplicative generator. 222 bytes
    // run the four-lane loop.
    const sanityBuffer = (length) => {
      const buffer = Buffer.alloc(length)
      let generator = 2654435761n
      for (let at = 0; at < length; at++) {
        buffer[at] = Number((generator >> 56n) & 255n)
        generator = (generator * 11400714785074694797n) & 0xffffffffffffffffn
      }
      return buffer
    }

    test.each([
      [0, 0x02cc5d05],
      [1, 0xcf65b03e],
      [14, 0x1208e7e2],
      [222, 0x5bd11dbd],
    ])('hashes the sanity buffer of %d bytes', (length, expected) => {
      expect(xxh32(sanityBuffer(length))).toBe(expected)
    })
  })

  describe('frame', () => {
    test.each([
      ['default flags', {}],
      ['block checksums', { blockChecksums: true }],
      ['a content checksum', { contentChecksumOf: Buffer.from('x') }],
    ])('writes the descriptor an independent encoder writes, %s', async (_, fields) => {
      const written = await lz4.compressFrame(Buffer.from('x'), {
        blockChecksums: Boolean(fields.blockChecksums),
        contentChecksum: Boolean(fields.contentChecksumOf),
      })
      const built = frame({ ...fields, blocks: [block(Buffer.from([0x10, 0x78]))] })

      expect(built.subarray(0, 7)).toEqual(written.subarray(0, 7))
    })

    test('a stored block decodes to its bytes', async () => {
      const data = Buffer.from('abcdefgh')

      await expect(
        lz4.decompressFrame(frame({ blocks: [block(data, true)], blockChecksums: true }))
      ).resolves.toEqual(data)
    })

    test('a content size and a content checksum are accepted by the decoder', async () => {
      const content = Buffer.alloc(1000, 65)

      await expect(
        lz4.decompressFrame(
          frame({
            contentSize: 1000,
            contentChecksumOf: content,
            blocks: [block(runBlock(65, 1000))],
          })
        )
      ).resolves.toEqual(content)
    })
  })

  describe('runBlock', () => {
    test.each([10, 11, 270, 65536])('decodes to %d copies of the byte', async (length) => {
      await expect(
        lz4.decompressFrame(frame({ blocks: [block(runBlock(7, length))] }))
      ).resolves.toEqual(Buffer.alloc(length, 7))
    })
  })

  describe('sequence and extension', () => {
    test('a length of 15 or more continues in bytes of 255, ended by a byte below 255', () => {
      expect([...extension(0)]).toEqual([0])
      expect([...extension(254)]).toEqual([254])
      expect([...extension(255)]).toEqual([255, 0])
      expect([...extension(600)]).toEqual([255, 255, 90])
    })

    test('a literals-only sequence has a token and the literals', () => {
      expect([...sequence({ literals: Buffer.from('ab') })]).toEqual([0x20, 0x61, 0x62])
    })

    test('a match carries a little-endian offset and the length above 4', () => {
      expect([...sequence({ offset: 0x0102, match: 6 })]).toEqual([0x02, 0x02, 0x01])
      expect([...sequence({ offset: 1, match: 4 + 15 + 255 })]).toEqual([0x0f, 1, 0, 255, 0])
    })
  })
})
