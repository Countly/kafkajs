const os = require('os')
const crc32C = require('./index')
const { createCrc32C } = require('./crc32C')

describe('Protocol > RecordBatch > crc32C', () => {
  test('perform CRC32C computations', () => {
    const longString =
      'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Morbi mollis cursus metus vel tristique. Proin congue massa massa, a malesuada dolor ullamcorper a. Nulla eget leo vel orci venenatis placerat. Donec semper condimentum justo, vel sollicitudin dolor consequat id. Nunc sed aliquet felis, eget congue nisi. Mauris eu justo suscipit, elementum turpis ut, molestie tellus. Mauris ornare rutrum fringilla. Nulla dignissim luctus pretium. Nullam nec eros hendrerit sapien pellentesque sollicitudin. Integer eget ligula dui. Mauris nec cursus nibh. Nunc interdum elementum leo, eu sagittis eros sodales nec. Duis dictum nulla sed tincidunt malesuada. Quisque in vulputate sapien. Sed sit amet tellus a est porta rhoncus sed eu metus. Mauris non pulvinar nisl, volutpat luctus enim. Suspendisse est nisi, sagittis at risus quis, ultricies rhoncus sem. Donec ullamcorper purus eget sapien facilisis, eu eleifend felis viverra. Suspendisse elit neque, semper aliquet neque sed, egestas tempus leo. Duis condimentum turpis duis.'
    const buffer = Buffer.from(longString)
    expect(crc32C(buffer)).toEqual(1796588439)
  })

  test('match the java CRC32C code', () => {
    const buffer = Buffer.from(require('./fixtures/crcPayload.json'))
    expect(crc32C(buffer)).toEqual(818496390)
  })

  test('samples', () => {
    const samples = require('./fixtures/samples')
    for (const sample of samples) {
      const buffer = Buffer.from(sample.input)
      expect(crc32C(buffer)).toEqual(sample.output)
    }
  })

  test('empty', () => {
    expect(crc32C(Buffer.alloc(0))).toEqual(0)
  })

  test('unicode null', () => {
    expect(crc32C(Buffer.from('\u0000'))).toEqual(1383945041)
  })

  /** Deterministic bytes, so every length crosses the eight-byte fold differently. */
  const bytes = (length) => Buffer.from(Array.from({ length }, (_, i) => (i * 31 + 7) & 0xff))

  test('the standard check value of "123456789" is 0xE3069283', () => {
    expect(crc32C(Buffer.from('123456789'))).toEqual(0xe3069283)
  })

  // Computed with a bit-at-a-time reference over the reflected Castagnoli polynomial 0x82F63B78.
  const BY_LENGTH = [
    0x00000000, 0x86b737ba, 0x8d10d7a1, 0x765a7c83, 0x65f1c5dc, 0x5d2e4463, 0x63c6d8d2, 0x5110a112,
    0x40795c72, 0x6fe35da6, 0x6ade9a0a, 0xb63ea091, 0x2e6e07e6, 0x7fd38d92, 0xd8dbd813, 0x9b0c1517,
    0xcf7845a4, 0x10c70233,
  ]

  test.each(BY_LENGTH.map((expected, length) => [length, expected]))(
    'a buffer of %i bytes matches the bitwise reference',
    (length, expected) => {
      expect(crc32C(bytes(length))).toEqual(expected)
    }
  )

  test('a 100003-byte buffer matches the bitwise reference', () => {
    expect(crc32C(bytes(100003))).toEqual(0xd351be60)
  })

  describe('word and byte paths', () => {
    // Plain byte-at-a-time table lookup, independent of the slicing code under test.
    const TABLE = Array.from({ length: 256 }, (_, byte) => {
      let crc = byte
      for (let bit = 0; bit < 8; bit++) crc = crc & 1 ? (crc >>> 1) ^ 0x82f63b78 : crc >>> 1
      return crc >>> 0
    })
    const reference = (buffer) => {
      let crc = 0xffffffff
      for (const byte of buffer) crc = TABLE[(crc ^ byte) & 0xff] ^ (crc >>> 8)
      return (crc ^ 0xffffffff) >>> 0
    }

    /** Deterministic pseudo-random bytes (a 32-bit LCG), so failures reproduce. */
    const randomBytes = (length) => {
      const buffer = Buffer.alloc(length)
      let state = 0x1234567
      for (let i = 0; i < length; i++) {
        state = (Math.imul(state, 1664525) + 1013904223) >>> 0
        buffer[i] = state >>> 24
      }
      return buffer
    }

    const pool = randomBytes(8192)
    const nativeOrder = createCrc32C(os.endianness() === 'LE')
    const byteOrderIndependent = createCrc32C(false)

    test('every length from 0 to 200 at every alignment from 0 to 7 matches a byte-at-a-time reference', () => {
      for (let length = 0; length <= 200; length++) {
        for (let alignment = 0; alignment < 8; alignment++) {
          const buffer = pool.subarray(alignment, alignment + length)
          const expected = reference(buffer)
          expect([crc32C(buffer), nativeOrder(buffer), byteOrderIndependent(buffer)]).toEqual([
            expected,
            expected,
            expected,
          ])
        }
      }
    })

    test('a large random buffer at an odd offset matches a byte-at-a-time reference', () => {
      const buffer = randomBytes(300007).subarray(3)
      const expected = reference(buffer)
      expect([crc32C(buffer), nativeOrder(buffer), byteOrderIndependent(buffer)]).toEqual([
        expected,
        expected,
        expected,
      ])
    })

    test('a slice of a slice and a plain Uint8Array view give the same checksum as a copy', () => {
      const slice = pool.subarray(5).subarray(2, 2 + 1001)
      const view = new Uint8Array(slice.buffer, slice.byteOffset, slice.length)
      const expected = reference(Buffer.from(slice))
      expect([crc32C(slice), crc32C(view), byteOrderIndependent(view)]).toEqual([
        expected,
        expected,
        expected,
      ])
    })

    test('a buffer ending at the end of its store, at a length that is not a multiple of 4, matches a byte-at-a-time reference', () => {
      for (let length = 64; length < 80; length++) {
        const store = new ArrayBuffer(length)
        const bytes = new Uint8Array(store)
        bytes.set(pool.subarray(0, length))
        for (let offset = 0; offset < 8; offset++) {
          const buffer = Buffer.from(store, offset, length - offset)
          expect(nativeOrder(buffer)).toEqual(reference(buffer))
        }
      }
    })

    test('a view over shared memory matches a byte-at-a-time reference', () => {
      const buffer = Buffer.from(new SharedArrayBuffer(1024), 1, 1001)
      buffer.set(pool.subarray(0, 1001))
      expect([crc32C(buffer), nativeOrder(buffer)]).toEqual([reference(buffer), reference(buffer)])
    })
  })
})
