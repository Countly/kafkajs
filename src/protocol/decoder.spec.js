const Decoder = require('./decoder')
const {
  KafkaJSInvalidLongError,
  KafkaJSInvalidVarIntError,
  KafkaJSPartialMessageError,
} = require('../errors')

describe('Protocol > Decoder', () => {
  describe('bounds checking on VarInt/UVarInt-prefixed readers', () => {
    it('readVarIntString throws when the declared length exceeds the buffer', () => {
      // VarInt zigzag-encodes 2 as the length; only 1 byte of payload follows
      const decoder = new Decoder(Buffer.from([0x04, 0x61]))
      expect(() => decoder.readVarIntString()).toThrow(KafkaJSPartialMessageError)
    })

    it('readVarIntString returns null on a VarInt -1 length', () => {
      // zigzag(-1) = 1
      const decoder = new Decoder(Buffer.from([0x01]))
      expect(decoder.readVarIntString()).toBeNull()
    })

    it('readVarIntBytes throws when the declared length exceeds the buffer', () => {
      const decoder = new Decoder(Buffer.from([0x04, 0x61]))
      expect(() => decoder.readVarIntBytes()).toThrow(KafkaJSPartialMessageError)
    })

    it('readVarIntBytes returns null on a VarInt -1 length', () => {
      const decoder = new Decoder(Buffer.from([0x01]))
      expect(decoder.readVarIntBytes()).toBeNull()
    })

    it('readUVarIntString throws when the declared length exceeds the buffer', () => {
      // UVarInt 3 => byteLength - 1 = 2 payload bytes expected, only 1 follows
      const decoder = new Decoder(Buffer.from([0x03, 0x61]))
      expect(() => decoder.readUVarIntString()).toThrow(KafkaJSPartialMessageError)
    })

    it('readUVarIntString reads an empty compact string at the end of the buffer', () => {
      // UVarInt 1 => byteLength - 1 = 0 payload bytes
      const decoder = new Decoder(Buffer.from([0x01]))
      expect(decoder.readUVarIntString()).toEqual('')
    })

    it('readUVarIntBytes throws when the declared length exceeds the buffer', () => {
      const decoder = new Decoder(Buffer.from([0x03, 0x61]))
      expect(() => decoder.readUVarIntBytes()).toThrow(KafkaJSPartialMessageError)
    })

    it('readUVarIntArray throws when the declared length exceeds the buffer', () => {
      // UVarInt 65 => length - 1 = 64 elements required, buffer has none left
      const decoder = new Decoder(Buffer.from([0x41]))
      expect(() => decoder.readUVarIntArray((d) => d.readInt8())).toThrow(
        KafkaJSPartialMessageError
      )
    })

    it('readTaggedFields throws on a truncated tag entry', () => {
      // 1 tagged field declared, but no tag/length bytes follow
      const decoder = new Decoder(Buffer.from([0x01]))
      expect(() => decoder.readTaggedFields()).toThrow(KafkaJSPartialMessageError)
    })

    it('readArrayAsync throws when the declared length exceeds the buffer', async () => {
      const decoder = new Decoder(Buffer.from([0x7f, 0xff, 0xff, 0xff]))
      await expect(decoder.readArrayAsync(async (d) => d.readInt8())).rejects.toThrow(
        KafkaJSPartialMessageError
      )
    })

    it('readArrayAsync returns [] on an int32 -1 length', async () => {
      const decoder = new Decoder(Buffer.from([0xff, 0xff, 0xff, 0xff]))
      await expect(decoder.readArrayAsync(async (d) => d.readInt8())).resolves.toEqual([])
    })
  })
  describe('requireArray', () => {
    it('returns an empty array and a filled one as they are', () => {
      const empty = new Decoder(Buffer.from([0x01])).readUVarIntArray((d) => d.readInt8())
      const filled = new Decoder(Buffer.from([0x02, 0x07])).readUVarIntArray((d) => d.readInt8())

      expect(Decoder.requireArray(empty, 'Message.Field')).toBe(empty)
      expect(Decoder.requireArray(filled, 'Message.Field')).toBe(filled)
    })

    it('refuses what a compact length of 0 read as, naming the field', () => {
      const array = new Decoder(Buffer.from([0x00])).readUVarIntArray((d) => d.readInt8())

      expect(() => Decoder.requireArray(array, 'Message.Field')).toThrow(KafkaJSPartialMessageError)
      expect(() => Decoder.requireArray(array, 'Message.Field')).toThrow(/Message\.Field/)
    })

    it('refuses what an async compact read of length 0 returned', async () => {
      const array = await new Decoder(Buffer.from([0x00])).readUVarIntArrayAsync(async (d) =>
        d.readInt8()
      )

      expect(() => Decoder.requireArray(array, 'Message.Field')).toThrow(KafkaJSPartialMessageError)
    })
  })

  describe('signed varint', () => {
    const read = (bytes) => new Decoder(Buffer.from(bytes)).readVarInt()

    it('rejects a signed varint longer than 5 bytes', () => {
      // Without a cap the shift wraps: the first input used to decode to 512
      for (const bytes of [
        [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01],
        [0xff, 0xff, 0xff, 0xff, 0xff, 0x0f],
        [0x80, 0x80, 0x80, 0x80, 0x80],
      ]) {
        expect(() => read(bytes)).toThrow(KafkaJSInvalidVarIntError)
        expect(() => read(bytes)).toThrow('Invalid VarInt, must contain 5 bytes or less')
      }
    })

    it('decodes the INT32 minimum and maximum', () => {
      const minimum = new Decoder(Buffer.from([0xff, 0xff, 0xff, 0xff, 0x0f]))
      expect(minimum.readVarInt()).toBe(-2147483648)
      expect(minimum.offset).toBe(5)

      const maximum = new Decoder(Buffer.from([0xfe, 0xff, 0xff, 0xff, 0x0f]))
      expect(maximum.readVarInt()).toBe(2147483647)
      expect(maximum.offset).toBe(5)
    })

    it('rejects a truncated signed varint', () => {
      for (const bytes of [[], [0x80, 0x80], [0xff, 0xff, 0xff, 0xff]]) {
        expect(() => read(bytes)).toThrow(KafkaJSPartialMessageError)
      }
    })
  })

  describe('varint terminal byte', () => {
    const decoder = (bytes) => new Decoder(Buffer.from(bytes))
    const tenBytes = (first, last) => [first, ...Array(8).fill(0xff), last]

    it('rejects a fifth byte above 0x0f in readVarInt', () => {
      expect(() => decoder([0x80, 0x80, 0x80, 0x80, 0x10]).readVarInt()).toThrow(
        KafkaJSInvalidVarIntError
      )
    })

    it('rejects a fifth byte above 0x0f in readUVarInt', () => {
      expect(() => decoder([0x80, 0x80, 0x80, 0x80, 0x10]).readUVarInt()).toThrow(
        KafkaJSInvalidVarIntError
      )
    })

    it('rejects a tenth byte above 0x01 in readVarLong', () => {
      expect(() => decoder([...Array(9).fill(0x80), 0x02]).readVarLong()).toThrow(
        KafkaJSInvalidLongError
      )
    })

    it('rejects a 0x7f terminal byte', () => {
      const fiveBytes = [0x80, 0x80, 0x80, 0x80, 0x7f]
      expect(() => decoder(fiveBytes).readVarInt()).toThrow(KafkaJSInvalidVarIntError)
      expect(() => decoder(fiveBytes).readUVarInt()).toThrow(KafkaJSInvalidVarIntError)
      expect(() => decoder([...Array(9).fill(0x80), 0x7f]).readVarLong()).toThrow(
        KafkaJSInvalidLongError
      )
    })

    it('keeps the largest valid five- and ten-byte values', () => {
      expect(decoder([0xff, 0xff, 0xff, 0xff, 0x0f]).readVarInt()).toBe(-2147483648)
      expect(decoder([0xfe, 0xff, 0xff, 0xff, 0x0f]).readVarInt()).toBe(2147483647)
      expect(decoder([0xff, 0xff, 0xff, 0xff, 0x0f]).readUVarInt()).toBe(4294967295)

      const minimum = decoder(tenBytes(0xff, 0x01))
      expect(minimum.readVarLong().toString()).toBe('-9223372036854775808')
      expect(minimum.offset).toBe(10)
      expect(decoder(tenBytes(0xfe, 0x01)).readVarLong().toString()).toBe('9223372036854775807')
    })

    it('keeps the over-long message for a fifth byte with its continuation bit', () => {
      const fiveContinued = [0x80, 0x80, 0x80, 0x80, 0x80, 0x01]
      expect(() => decoder(fiveContinued).readVarInt()).toThrow(
        'Invalid VarInt, must contain 5 bytes or less'
      )
      expect(() => decoder(fiveContinued).readUVarInt()).toThrow(
        'Invalid VarInt, must contain 5 bytes or less'
      )
      expect(() => decoder([...Array(10).fill(0x80), 0x01]).readVarLong()).toThrow(
        'Invalid Long, must contain 9 bytes or less'
      )
    })
  })

  describe('readInt64', () => {
    const read = (bytes) => new Decoder(Buffer.from(bytes)).readInt64()

    it('decodes -1, both extremes and a set top byte from the sign byte up', () => {
      expect(read([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff])).toBe(-1n)
      expect(read([0x80, 0, 0, 0, 0, 0, 0, 0])).toBe(-(2n ** 63n))
      expect(read([0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff])).toBe(2n ** 63n - 1n)
      expect(read([0x01, 0, 0, 0, 0, 0, 0, 0])).toBe(2n ** 56n)
    })
  })

  describe('exact-fit reads do not throw', () => {
    it.each([
      ['readInt8', [0x01], 1],
      ['readInt16', [0x00, 0x01], 1],
      ['readInt32', [0x00, 0x00, 0x00, 0x01], 1],
      ['readDouble', [0x3f, 0xf0, 0, 0, 0, 0, 0, 0], 1],
    ])('%s consumes a buffer that holds exactly one value', (reader, bytes, expected) => {
      const decoder = new Decoder(Buffer.from(bytes))
      expect(decoder[reader]()).toEqual(expected)
      expect(decoder.offset).toEqual(bytes.length)
    })

    it('readInt64 consumes exactly eight bytes', () => {
      const decoder = new Decoder(Buffer.from([0, 0, 0, 0, 0, 0, 0, 0x2a]))
      expect(decoder.readInt64().toString()).toEqual('42')
      expect(decoder.offset).toEqual(8)
    })

    it('readString and readBytes consume a payload that ends the buffer', () => {
      expect(new Decoder(Buffer.from([0x00, 0x02, 0x68, 0x69])).readString()).toEqual('hi')
      expect(new Decoder(Buffer.from([0x00, 0x00, 0x00, 0x01, 0x07])).readBytes()).toEqual(
        Buffer.from([0x07])
      )
    })
  })
})
