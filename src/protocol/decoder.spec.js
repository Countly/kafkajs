const Decoder = require('./decoder')
const { KafkaJSPartialMessageError } = require('../errors')

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

    it('readVarIntArray throws when the declared length exceeds the buffer', () => {
      // zigzag(64) requires 64 elements, buffer has none left
      const decoder = new Decoder(Buffer.from([0x80, 0x01]))
      expect(() => decoder.readVarIntArray((d) => d.readInt8())).toThrow(KafkaJSPartialMessageError)
    })

    it('readVarIntArray returns [] on a VarInt -1 length', () => {
      const decoder = new Decoder(Buffer.from([0x01]))
      expect(decoder.readVarIntArray((d) => d.readInt8())).toEqual([])
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
