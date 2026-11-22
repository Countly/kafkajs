const crc32C = require('./index')

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
})
