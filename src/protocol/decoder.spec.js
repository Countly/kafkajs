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
})
