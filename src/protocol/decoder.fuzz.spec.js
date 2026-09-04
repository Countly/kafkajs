const Decoder = require('./decoder')
const { KafkaJSPartialMessageError } = require('../errors')

const READERS = [
  'readInt8',
  'readInt16',
  'readInt32',
  'readInt64',
  'readDouble',
  'readString',
  'readBytes',
  'readVarInt',
  'readVarLong',
]

describe('Protocol > Decoder > bounds checking', () => {
  it.each(READERS)('%s throws KafkaJSPartialMessageError on an empty buffer', (reader) => {
    const decoder = new Decoder(Buffer.alloc(0))
    expect(() => decoder[reader]()).toThrow(KafkaJSPartialMessageError)
  })

  it('readString throws when the declared length exceeds the buffer', () => {
    // int16 length = 100, but only 2 bytes of payload follow
    const decoder = new Decoder(Buffer.from([0x00, 0x64, 0x61, 0x62]))
    expect(() => decoder.readString()).toThrow(KafkaJSPartialMessageError)
  })

  it('readBytes throws when the declared length exceeds the buffer', () => {
    const decoder = new Decoder(Buffer.from([0x00, 0x00, 0x00, 0x64, 0x61]))
    expect(() => decoder.readBytes()).toThrow(KafkaJSPartialMessageError)
  })

  it('readArray does not allocate an absurd array from a corrupt length', () => {
    const decoder = new Decoder(Buffer.from([0x7f, 0xff, 0xff, 0xff]))
    expect(() => decoder.readArray((d) => d.readInt8())).toThrow(KafkaJSPartialMessageError)
  })

  // Positive cases: the guards must not fire on legitimate empty/null encodings.
  it('reads an empty compact array at the very end of the buffer', () => {
    // A compact array carries n + 1, so an empty one is a single UVarInt 1 with no payload.
    const decoder = new Decoder(Buffer.from([0x01]))
    expect(decoder.readUVarIntArray((d) => d.readInt8())).toEqual([])
  })

  it('reads a null compact array (UVarInt 0)', () => {
    const decoder = new Decoder(Buffer.from([0x00]))
    expect(decoder.readUVarIntArray((d) => d.readInt8())).toBeNull()
  })

  it('reads a null array (int32 -1) and a null string (int16 -1)', () => {
    expect(
      new Decoder(Buffer.from([0xff, 0xff, 0xff, 0xff])).readArray((d) => d.readInt8())
    ).toEqual([])
    expect(new Decoder(Buffer.from([0xff, 0xff])).readString()).toBeNull()
    expect(new Decoder(Buffer.from([0xff, 0xff, 0xff, 0xff])).readBytes()).toBeNull()
  })

  it('reads an empty tagged-field section at the end of the buffer', () => {
    const decoder = new Decoder(Buffer.from([0x00]))
    expect(decoder.readTaggedFields()).toBeNull()
  })

  it('survives random garbage without an ERR_BUFFER_OUT_OF_BOUNDS', () => {
    for (let i = 0; i < 500; i++) {
      const size = 1 + Math.floor(Math.random() * 64)
      const buffer = Buffer.from(
        Array.from({ length: size }, () => Math.floor(Math.random() * 256))
      )
      const decoder = new Decoder(buffer)
      try {
        decoder.readArray((d) => ({ a: d.readString(), b: d.readInt64(), c: d.readBytes() }))
      } catch (e) {
        expect(e.name).not.toEqual('RangeError')
        expect(e.code).not.toEqual('ERR_BUFFER_OUT_OF_BOUNDS')
      }
    }
  })
})
