const fs = require('fs')
const path = require('path')
const { Types, Codecs, lookupCodec, decompressBounded } = require('./index')
const { KafkaJSCorruptRecordError } = require('../../../errors')
const { sequence, runBlock, block, frame } = require('../../../../testHelpers/lz4Frame')

let hasLz4 = true
try {
  require('lz4-napi')
} catch (e) {
  hasLz4 = false
}
const describeIfLz4 = hasLz4 ? describe : describe.skip
const lz4Napi = hasLz4 ? require('lz4-napi') : null

const stored = (bytes) => block(Buffer.alloc(bytes, 1), true)
const runs = (byte, length) => block(runBlock(byte, length))

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

  describeIfLz4('the binding as installed', () => {
    // These pin what the bound before the native call relies on. A different release of lz4-napi,
    // or of the decoder inside it, has to pass them again before the pin moves.
    const { decompressFrame } = lz4Napi || {}
    const first = frame({ blocks: [runs(65, 1000)] })
    const content = Buffer.alloc(1000, 65)

    it('decodes only the first frame, whatever follows it', async () => {
      const second = frame({ blocks: [runs(66, 500)] })
      const skippable = Buffer.from([0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0])

      for (const after of [second, Buffer.from([1, 2, 3]), skippable, Buffer.alloc(0)]) {
        await expect(decompressFrame(Buffer.concat([first, after]))).resolves.toEqual(content)
      }
    })

    it('refuses a skippable frame before the frame, and bytes that are not a frame', async () => {
      const skippable = Buffer.from([0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0])

      await expect(decompressFrame(Buffer.concat([skippable, first]))).rejects.toBeDefined()
      await expect(decompressFrame(Buffer.from('not an lz4 frame'))).rejects.toBeDefined()
    })

    it('answers an empty buffer with an empty output and accepts a missing EndMark', async () => {
      await expect(decompressFrame(Buffer.alloc(0))).resolves.toEqual(Buffer.alloc(0))
      await expect(
        decompressFrame(frame({ blocks: [stored(8)], endMark: false }))
      ).resolves.toEqual(Buffer.alloc(8, 1))
    })

    it('checks a content size at the end and never allocates from it', async () => {
      const before = process.memoryUsage().arrayBuffers

      for (const claim of [999, 1001, 2n ** 32n]) {
        await expect(
          decompressFrame(frame({ contentSize: claim, blocks: [runs(65, 1000)] }))
        ).rejects.toBeDefined()
      }
      await expect(
        decompressFrame(frame({ contentSize: 1000, blocks: [runs(65, 1000)] }))
      ).resolves.toEqual(content)

      expect(process.memoryUsage().arrayBuffers - before).toBeLessThan(16 * 1048576)
    })

    it('refuses a reserved bit, a dictionary id, a wrong header checksum and a wrong block or content checksum', async () => {
      const sealed = frame({
        blockChecksums: true,
        contentChecksumOf: content,
        blocks: [runs(65, 1000)],
      })
      const flip = (buffer, at) => {
        const copy = Buffer.from(buffer)
        copy[at] ^= 1
        return copy
      }
      const checksumAt = sealed.length - 4
      const blockChecksumAt = sealed.length - 4 - 4 - 4

      for (const bad of [
        frame({ flg: 0x62, blocks: [runs(65, 1000)] }),
        frame({ flg: 0x61, blocks: [runs(65, 1000)] }),
        frame({ bd: 0x41, blocks: [runs(65, 1000)] }),
        flip(first, 6),
        flip(sealed, checksumAt),
        flip(sealed, blockChecksumAt),
      ]) {
        await expect(decompressFrame(bad)).rejects.toBeDefined()
      }
      await expect(decompressFrame(sealed)).resolves.toEqual(content)
    })

    it('refuses a truncation inside a block', async () => {
      await expect(decompressFrame(first.subarray(0, 20))).rejects.toBeDefined()
    })

    it('refuses a stored block above the block maximum and accepts one equal to it', async () => {
      await expect(decompressFrame(frame({ blocks: [stored(65537)] }))).rejects.toBeDefined()
      await expect(decompressFrame(frame({ blocks: [stored(65536)] }))).resolves.toHaveLength(65536)
    })

    it('holds an independent block to the block maximum: one that decodes above it is refused', async () => {
      await expect(decompressFrame(frame({ blocks: [runs(65, 65536)] }))).resolves.toHaveLength(
        65536
      )
      await expect(decompressFrame(frame({ blocks: [runs(65, 100000)] }))).rejects.toBeDefined()
    })

    it('decodes a match that reaches into the block before it in a linked frame only', async () => {
      // 'abcdefghij', then 'k', a match of 4 at offset 11 (reaching back to 'abcd'), then 'zzzzz'
      const before = block(sequence({ literals: Buffer.from('abcdefghij') }))
      const across = block(
        Buffer.concat([
          sequence({ literals: Buffer.from('k'), offset: 11, match: 4 }),
          sequence({ literals: Buffer.from('zzzzz') }),
        ])
      )

      await expect(
        decompressFrame(frame({ independent: false, blocks: [before, across] }))
      ).resolves.toEqual(Buffer.from('abcdefghijkabcdzzzzz'))
      await expect(decompressFrame(frame({ blocks: [before, across] }))).rejects.toBeDefined()
    })

    it('lets a linked block decode above the block maximum once its buffer has rolled over', async () => {
      const linked = (length) =>
        frame({
          independent: false,
          blocks: [stored(65536), stored(61440), stored(65536), runs(65, length)],
        })

      // a compressed block of 100000 bytes is refused before the buffer rolls over, and so is an
      // independent one; after it, 100000 and 120000 bytes are accepted and 130000 is not
      await expect(
        decompressFrame(frame({ independent: false, blocks: [stored(65536), runs(65, 100000)] }))
      ).rejects.toBeDefined()
      await expect(decompressFrame(linked(100000))).resolves.toHaveLength(292512)
      await expect(decompressFrame(linked(120000))).resolves.toHaveLength(312512)
      await expect(decompressFrame(linked(130000))).rejects.toBeDefined()
    })
  })

  describeIfLz4('the bound before the native call', () => {
    const lz4Codec = lookupCodec(Types.LZ4)
    let native

    beforeEach(() => {
      native = jest.spyOn(lz4Napi, 'decompressFrame')
    })

    afterEach(() => {
      native.mockRestore()
    })

    /** The refusal, with the proof that the binding was never called. */
    const refused = async (buffer, limit) => {
      const error = await decompressBounded(lz4Codec, 3, buffer, limit).catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSCorruptRecordError)
      expect(error.codec).toBe('lz4')
      expect(error.limit).toBe(limit)
      expect(native).not.toHaveBeenCalled()
      return error
    }

    /** The decoded bytes, with the proof that the binding was called once. */
    const decoded = async (buffer, limit) => {
      const output = await decompressBounded(lz4Codec, 3, buffer, limit)

      expect(native).toHaveBeenCalledTimes(1)
      return output
    }

    describe('an independent frame', () => {
      it('passes at the limit and is refused one byte over, before the native call', async () => {
        const one = frame({ blocks: [runs(65, 1000)] })

        await expect(decoded(one, 1000)).resolves.toEqual(Buffer.alloc(1000, 65))
        native.mockClear()
        await expect(refused(one, 999)).resolves.toMatchObject({ reason: 'decompressed-size' })
      })

      it('passes a frame whose last block is partial exactly at the limit, which the fast bound alone would refuse', async () => {
        // 64 KiB blocks of 65536, 65536, 65536 and 1000 decoded bytes: the fast bound counts the
        // 1000-byte block as its compressed size times 255, far above its real size
        const partial = frame({
          blocks: [runs(65, 65536), runs(65, 65536), runs(65, 65536), runs(65, 1000)],
        })
        const total = 3 * 65536 + 1000

        await expect(decoded(partial, total)).resolves.toHaveLength(total)
        native.mockClear()
        await expect(refused(partial, total - 1)).resolves.toMatchObject({
          reason: 'decompressed-size',
        })
      })

      it('counts the first frame only', async () => {
        const two = Buffer.concat([
          frame({ blocks: [runs(65, 1000)] }),
          frame({ blocks: [runs(66, 4000)] }),
        ])

        await expect(decoded(two, 1000)).resolves.toEqual(Buffer.alloc(1000, 65))
      })

      it('counts stored blocks by their size', async () => {
        await expect(
          refused(frame({ blocks: [stored(65536), stored(1)] }), 65536)
        ).resolves.toMatchObject({
          reason: 'decompressed-size',
        })
      })

      it('skips the checksums that follow each block', async () => {
        const sealed = frame({ blockChecksums: true, blocks: [runs(65, 1000), runs(65, 1000)] })

        await expect(decoded(sealed, 2000)).resolves.toHaveLength(2000)
        native.mockClear()
        await expect(refused(sealed, 1999)).resolves.toMatchObject({ reason: 'decompressed-size' })
      })
    })

    describe('a linked frame', () => {
      const linked = (...blocks) => frame({ independent: false, blocks })

      it('is refused when its blocks obey the block maximum and add up past the limit', async () => {
        const frames = linked(stored(65536), stored(65536), stored(65536))

        await expect(refused(frames, 100000)).resolves.toMatchObject({
          reason: 'decompressed-size',
        })
      })

      it('is walked even when it would fit the fast bound, and passes within the limit', async () => {
        await expect(decoded(linked(runs(65, 1000), runs(66, 1000)), 2000)).resolves.toHaveLength(
          2000
        )
      })

      it('is refused as decompression, ahead of the size, when a compressed block expands beyond the block maximum', async () => {
        const overflow = linked(stored(65536), stored(61440), stored(65536), runs(65, 100000))

        await expect(refused(overflow, 100)).resolves.toMatchObject({ reason: 'decompression' })
        native.mockClear()
        await expect(refused(overflow, 2147483647)).resolves.toMatchObject({
          reason: 'decompression',
        })
      })
    })

    describe('the content size in the header', () => {
      it('is a claim: one equal to the decoded length above the limit is decompressed-size', async () => {
        const claimed = frame({ contentSize: 1000, blocks: [runs(65, 1000)] })

        await expect(refused(claimed, 999)).resolves.toMatchObject({ reason: 'decompressed-size' })
      })

      it('is corruption when it disagrees with the data, whether the data fits the limit or not', async () => {
        const bigClaim = frame({ contentSize: 2n ** 40n, blocks: [runs(65, 1000)] })

        await expect(refused(bigClaim, 10)).resolves.toMatchObject({ reason: 'decompression' })
        native.mockClear()
        await expect(
          refused(frame({ contentSize: 999, blocks: [runs(65, 1000)] }), 10)
        ).resolves.toMatchObject({ reason: 'decompression' })
      })

      it.each([999, 1001])(
        'understated or overstated (%d) is decompression when the frame fits the limit',
        async (claim) => {
          const error = await decompressBounded(
            lz4Codec,
            3,
            frame({ contentSize: claim, blocks: [runs(65, 1000)] }),
            5000
          ).catch((e) => e)

          expect(error).toMatchObject({ reason: 'decompression', codec: 'lz4' })
        }
      )

      it('is not refused for being large: a huge claim over a frame that is the right size fails only at the end', async () => {
        const before = process.memoryUsage().arrayBuffers
        const error = await decompressBounded(
          lz4Codec,
          3,
          frame({ contentSize: 2n ** 40n, blocks: [runs(65, 1000)] }),
          5000
        ).catch((e) => e)

        expect(error).toMatchObject({ reason: 'decompression' })
        expect(process.memoryUsage().arrayBuffers - before).toBeLessThan(16 * 1048576)
      })
    })

    describe('a frame the binding refuses', () => {
      const linked = (...blocks) => frame({ independent: false, blocks })
      const bad = (body) => block(body)

      it.each([
        [
          'a skippable frame in front',
          () =>
            Buffer.concat([
              Buffer.from([0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0]),
              frame({ blocks: [stored(8)] }),
            ]),
        ],
        ['a magic number that is not LZ4', () => Buffer.from('not an lz4 frame at all')],
        ['a reserved flag bit', () => frame({ flg: 0x62, blocks: [stored(8)] })],
        ['a dictionary id', () => frame({ flg: 0x61, blocks: [stored(8)] })],
        ['a reserved block descriptor bit', () => frame({ bd: 0x41, blocks: [stored(8)] })],
        ['a block maximum code below 4', () => frame({ bd: 0x30, blocks: [stored(8)] })],
        ['a block larger than the block maximum', () => frame({ blocks: [stored(65537)] })],
        [
          'a block that runs past the input',
          () => frame({ blocks: [runs(65, 1000)] }).subarray(0, 20),
        ],
        ['a literal length that runs past the block', () => linked(bad(Buffer.from([0xf0, 255])))],
        ['literals that run past the block', () => linked(bad(Buffer.from([0x50, 1, 2])))],
        ['a match offset of zero', () => linked(bad(Buffer.from([0x10, 0x61, 0, 0])))],
        ['a match offset cut short', () => linked(bad(Buffer.from([0x10, 0x61, 1])))],
        [
          'a match length that runs past the block',
          () => linked(bad(Buffer.from([0x1f, 0x61, 1, 0, 255]))),
        ],
      ])('%s is decompression', async (_, build) => {
        const error = await decompressBounded(lz4Codec, 3, build(), 2147483647).catch((e) => e)

        expect(error).toMatchObject({ reason: 'decompression', codec: 'lz4' })
        expect(error).toBeInstanceOf(KafkaJSCorruptRecordError)
      })

      it('is left to the binding when the input ends inside the descriptor', async () => {
        const cut = (length) => frame({ blocks: [stored(8)] }).subarray(0, length)

        const sized = frame({ contentSize: 8, blocks: [stored(8)] })

        await expect(decoded(cut(0), 10)).resolves.toEqual(Buffer.alloc(0))
        native.mockClear()
        for (const input of [cut(5), cut(6), sized.subarray(0, 12)]) {
          await expect(decompressBounded(lz4Codec, 3, input, 10)).rejects.toMatchObject({
            reason: 'decompression',
            codec: 'lz4',
          })
        }
        expect(native).toHaveBeenCalledTimes(3)
      })
    })

    describe('the pass over the frame', () => {
      it('gives the event loop a turn every 256 KiB of input', async () => {
        // one block of about 910 KB of short sequences, each decoding to 14 bytes
        const one = sequence({ literals: Buffer.from('aaaaaaaaaa'), offset: 1, match: 4 })
        const dense = block(
          Buffer.concat([...Array(70000).fill(one), sequence({ literals: Buffer.from('zzzzz') })])
        )
        const turns = jest.spyOn(global, 'setImmediate')
        const refuse = (buffer) =>
          decompressBounded(lz4Codec, 3, buffer, 100).catch(() => turns.mock.calls.length)

        try {
          const small = await refuse(frame({ maxSizeCode: 6, blocks: [runs(65, 1000)] }))
          const large = await refuse(frame({ maxSizeCode: 6, blocks: [dense] }))

          expect(large - small).toBeGreaterThanOrEqual(3)
        } finally {
          turns.mockRestore()
        }
      })

      describe('a length extension that fills a block of 4 MiB', () => {
        const SLICE = 262144
        const LITERALS = [0xf0]
        const MATCH = [0x1f, 0x41, 0x01, 0x00]

        /** A linked frame of one 4 MiB block of 255s: `head` first, `last` as its final byte. */
        const hostile = (head, last) => {
          const data = Buffer.alloc(4 * 1048576, 0xff)
          data.set(head)
          data[data.length - 1] = last
          return frame({ independent: false, maxSizeCode: 7, blocks: [block(data)] })
        }

        /** The most bytes of `buffer` the codec reads between two turns of the event loop. */
        const widestRun = async (buffer) => {
          const turn = global.setImmediate
          let reads = 0
          let widest = 0
          const counted = new Proxy(buffer, {
            get: (target, key) => {
              if (typeof key === 'string' && Number.isInteger(Number(key))) reads++
              return Reflect.get(target, key, target)
            },
          })
          const turns = jest.spyOn(global, 'setImmediate').mockImplementation((callback) => {
            widest = Math.max(widest, reads)
            reads = 0
            return turn(callback)
          })

          try {
            await decompressBounded(lz4Codec, 3, counted, 64 * 1048576).catch(() => {})
          } finally {
            turns.mockRestore()
          }
          return Math.max(widest, reads)
        }

        it.each([
          ['a terminated literal length', LITERALS, 0],
          ['an unterminated literal length', LITERALS, 0xff],
          ['a terminated match length', MATCH, 0],
          ['an unterminated match length', MATCH, 0xff],
        ])(
          '%s is refused as decompression without a long read between turns',
          async (_, head, last) => {
            const bad = hostile(head, last)

            await expect(refused(bad, 64 * 1048576)).resolves.toMatchObject({
              reason: 'decompression',
              codec: 'lz4',
            })
            expect(await widestRun(bad)).toBeLessThanOrEqual(SLICE)
          }
        )

        it.each([
          ['a terminated literal length', LITERALS, 0],
          ['an unterminated literal length', LITERALS, 0xff],
          ['a terminated match length', MATCH, 0],
          ['an unterminated match length', MATCH, 0xff],
        ])('%s is refused as above the largest block', async (_, head, last) => {
          const error = await refused(hostile(head, last), 64 * 1048576)

          expect(error.cause).toMatchObject({
            name: 'Lz4FrameError',
            message: 'LZ4 length is above the largest block',
          })
        })
      })
    })

    describe('the preflight work slicing', () => {
      test('walks the input without a generator step for each byte, sequence or skip', async () => {
        // two blocks of 20000 sequences with a literal and a match extension each, then a stored
        // block, all with a block checksum: about 1.2 MB for the fast pass to skip and the exact
        // pass to walk to its end
        const long = sequence({ literals: Buffer.alloc(20, 1), offset: 1, match: 30 })
        const compressed = block(
          Buffer.concat([...Array(20000).fill(long), sequence({ literals: Buffer.alloc(5) })])
        )
        const input = frame({
          maxSizeCode: 6,
          blockChecksums: true,
          blocks: [compressed, compressed, stored(200000)],
        })
        const next = jest.spyOn(Object.getPrototypeOf(function* () {}).prototype, 'next')
        const turns = jest.spyOn(global, 'setImmediate')

        try {
          await expect(refused(input, 100)).resolves.toMatchObject({ reason: 'decompressed-size' })

          // each turn resumes the measuring generator and the one block walker it delegates to
          expect(next.mock.calls.length).toBeLessThanOrEqual(3 * (turns.mock.calls.length + 2))
          expect(turns.mock.calls.length).toBeGreaterThanOrEqual(8)
        } finally {
          next.mockRestore()
          turns.mockRestore()
        }
      })

      test('extension work respects a partly consumed slice', async () => {
        const data = Buffer.alloc(3 * 87380 + 1 + 16450, 255)
        for (let i = 0; i < 87380; i++) {
          data[i * 3] = 0
          data[i * 3 + 1] = 0
          data[i * 3 + 2] = 1
        }
        data[3 * 87380] = 0xf0
        const header = Buffer.from([0x04, 0x22, 0x4d, 0x18, 0x40, 0x70, 0])
        const length = Buffer.alloc(4)
        length.writeUInt32LE(data.length)
        const input = Buffer.concat([header, length, data, Buffer.alloc(4)])
        let reads = 0
        let widest = 0
        const proxy = new Proxy(input, {
          get(target, key) {
            if (typeof key === 'string' && Number.isInteger(Number(key))) reads++
            return Reflect.get(target, key, target)
          },
        })
        const immediate = global.setImmediate
        const turns = jest.spyOn(global, 'setImmediate').mockImplementation((cb) => {
          widest = Math.max(widest, reads)
          reads = 0
          return immediate(cb)
        })
        try {
          await expect(
            lz4Codec.decompress(proxy, { maxOutputLength: 64 * 1048576 })
          ).rejects.toMatchObject({
            reason: 'decompression',
          })
          widest = Math.max(widest, reads)
          expect(widest).toBeLessThanOrEqual(262144)
        } finally {
          turns.mockRestore()
        }
      })
    })
  })

  describe('the audited release', () => {
    const root = path.join(__dirname, '..', '..', '..', '..')
    const lock = JSON.parse(fs.readFileSync(path.join(root, 'package-lock.json'), 'utf8')).packages[
      ''
    ]
    const manifest = JSON.parse(fs.readFileSync(path.join(root, 'package.json'), 'utf8'))

    it('is the exact release the bound was checked against, in the manifest and the lock', () => {
      expect(manifest.optionalDependencies['lz4-napi']).toBe('2.10.0')
      expect(lock.optionalDependencies['lz4-napi']).toBe('2.10.0')
      expect(lock.dependencies['lz4-napi']).toBe('2.10.0')
    })

    describeIfLz4('when it is installed', () => {
      it('is that release', () => {
        const installed = JSON.parse(
          fs.readFileSync(
            path.join(path.dirname(require.resolve('lz4-napi')), 'package.json'),
            'utf8'
          )
        )

        expect(installed.version).toBe('2.10.0')
      })
    })
  })
})
