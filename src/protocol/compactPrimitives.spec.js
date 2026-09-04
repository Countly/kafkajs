const Encoder = require('./encoder')
const Decoder = require('./decoder')
const { KafkaJSPartialMessageError } = require('../errors')

const bytes = (...values) => Buffer.from(values)

describe('Protocol > compact primitives (KIP-482)', () => {
  describe('unsigned varint boundaries', () => {
    const cases = [
      [0, [0x00]],
      [1, [0x01]],
      [127, [0x7f]],
      [128, [0x80, 0x01]],
      [300, [0xac, 0x02]],
      [16383, [0xff, 0x7f]],
      [16384, [0x80, 0x80, 0x01]],
      [2097151, [0xff, 0xff, 0x7f]],
      [2097152, [0x80, 0x80, 0x80, 0x01]],
      [268435455, [0xff, 0xff, 0xff, 0x7f]],
      [268435456, [0x80, 0x80, 0x80, 0x80, 0x01]],
      [2147483647, [0xff, 0xff, 0xff, 0xff, 0x07]],
    ]

    test.each(cases)('encodes %i', (value, expected) => {
      expect([...new Encoder().writeUVarInt(value).buffer]).toEqual(expected)
    })

    test.each(cases)('decodes %i', (value, encoded) => {
      expect(new Decoder(bytes(...encoded)).readUVarInt()).toEqual(value)
    })
  })

  describe('compact bytes', () => {
    test('encodes length + 1 and decodes exactly the payload', () => {
      const buffer = new Encoder().writeUVarIntBytes(bytes(1, 2, 3)).writeInt8(9).buffer
      expect([...buffer]).toEqual([4, 1, 2, 3, 9])

      const decoder = new Decoder(buffer)
      expect([...decoder.readUVarIntBytes()]).toEqual([1, 2, 3])
      expect(decoder.readInt8()).toEqual(9)
    })

    test('encodes empty as 0x01 and decodes it as an empty buffer', () => {
      const buffer = new Encoder().writeUVarIntBytes(Buffer.alloc(0)).writeInt8(9).buffer
      expect([...buffer]).toEqual([1, 9])

      const decoder = new Decoder(buffer)
      expect([...decoder.readUVarIntBytes()]).toEqual([])
      expect(decoder.readInt8()).toEqual(9)
    })

    test('encodes null as 0x00 and decodes it as null', () => {
      const buffer = new Encoder().writeUVarIntBytes(null).writeInt8(9).buffer
      expect([...buffer]).toEqual([0, 9])

      const decoder = new Decoder(buffer)
      expect(decoder.readUVarIntBytes()).toBeNull()
      expect(decoder.readInt8()).toEqual(9)
    })

    test('throws instead of over-reading a truncated payload', () => {
      expect(() => new Decoder(bytes(4, 1, 2)).readUVarIntBytes()).toThrow(
        KafkaJSPartialMessageError
      )
    })
  })

  describe('compact strings', () => {
    test('non-nullable: empty string is 0x01, never 0x00', () => {
      expect([...new Encoder().writeUVarIntString('').buffer]).toEqual([1])
      expect(new Decoder(bytes(1)).readUVarIntString()).toEqual('')
    })

    test('nullable: 0x00 is null, and is distinct from the empty string', () => {
      expect([...new Encoder().writeUVarIntString(null).buffer]).toEqual([0])
      expect(new Decoder(bytes(0)).readUVarIntString()).toBeNull()
    })

    test('round-trips multi-byte UTF-8 with a byte length, not a character length', () => {
      const buffer = new Encoder().writeUVarIntString('héllo').writeInt8(9).buffer
      expect(buffer[0]).toEqual(7) // 6 UTF-8 bytes + 1
      const decoder = new Decoder(buffer)
      expect(decoder.readUVarIntString()).toEqual('héllo')
      expect(decoder.readInt8()).toEqual(9)
    })
  })

  describe('compact arrays', () => {
    test('null is 0, empty is 1, and k means k - 1 elements', () => {
      expect(new Decoder(bytes(0)).readUVarIntArray((d) => d.readInt8())).toBeNull()
      expect(new Decoder(bytes(1)).readUVarIntArray((d) => d.readInt8())).toEqual([])
      expect(new Decoder(bytes(3, 7, 8)).readUVarIntArray((d) => d.readInt8())).toEqual([7, 8])
    })
  })

  describe('readRawUVarIntBytes', () => {
    test('reads exactly N bytes for a raw (not length + 1) prefix', () => {
      const decoder = new Decoder(bytes(3, 1, 2, 3, 9))
      expect([...decoder.readRawUVarIntBytes()]).toEqual([1, 2, 3])
      expect(decoder.readInt8()).toEqual(9)
    })

    test('a zero length is an empty buffer, not null', () => {
      const decoder = new Decoder(bytes(0, 9))
      expect([...decoder.readRawUVarIntBytes()]).toEqual([])
      expect(decoder.readInt8()).toEqual(9)
    })

    test('throws on a truncated payload', () => {
      expect(() => new Decoder(bytes(5, 1, 2)).readRawUVarIntBytes()).toThrow(
        KafkaJSPartialMessageError
      )
    })
  })
})
