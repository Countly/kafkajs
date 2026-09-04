const Encoder = require('./encoder')
const Decoder = require('./decoder')
const { KafkaJSPartialMessageError } = require('../errors')

const ZERO = Buffer.alloc(16)
// 4.3's MetadataResponse carries topic ids as raw 16-byte big-endian values.
const SAMPLE_HEX = '0102030405060708090a0b0c0d0e0f10'
const SAMPLE_STRING = '01020304-0506-0708-090a-0b0c0d0e0f10'

describe('Protocol > uuid', () => {
  test('writes 16 raw big-endian bytes from the canonical string', () => {
    const buffer = new Encoder().writeUuid(SAMPLE_STRING).buffer
    expect(buffer.length).toEqual(16)
    expect(buffer.toString('hex')).toEqual(SAMPLE_HEX)
  })

  test('writes a Buffer unchanged', () => {
    const raw = Buffer.from(SAMPLE_HEX, 'hex')
    expect(new Encoder().writeUuid(raw).buffer.toString('hex')).toEqual(SAMPLE_HEX)
  })

  test('writes null as the all-zero sentinel', () => {
    expect([...new Encoder().writeUuid(null).buffer]).toEqual([...ZERO])
  })

  test('reads the canonical lowercase string', () => {
    const decoder = new Decoder(Buffer.concat([Buffer.from(SAMPLE_HEX, 'hex'), Buffer.from([9])]))
    expect(decoder.readUuid()).toEqual(SAMPLE_STRING)
    expect(decoder.readInt8()).toEqual(9)
  })

  test('reads the all-zero sentinel as null', () => {
    expect(new Decoder(ZERO).readUuid()).toBeNull()
  })

  test('round-trips', () => {
    expect(new Decoder(new Encoder().writeUuid(SAMPLE_STRING).buffer).readUuid()).toEqual(
      SAMPLE_STRING
    )
    expect(new Decoder(new Encoder().writeUuid(null).buffer).readUuid()).toBeNull()
  })

  test('rejects a malformed uuid instead of writing truncated bytes', () => {
    expect(() => new Encoder().writeUuid('not-a-uuid')).toThrow(/uuid/)
    expect(() => new Encoder().writeUuid(Buffer.alloc(8))).toThrow(/uuid/)
  })

  test('throws when the frame is too short', () => {
    expect(() => new Decoder(Buffer.alloc(8)).readUuid()).toThrow(KafkaJSPartialMessageError)
  })
})
