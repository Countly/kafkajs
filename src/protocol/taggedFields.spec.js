const Decoder = require('./decoder')
const { KafkaJSPartialMessageError } = require('../errors')

const bytes = (...values) => Buffer.from(values)

describe('Protocol > tagged fields (KIP-482 TAG_BUFFER)', () => {
  test('an empty section is a single 0x00 and yields an empty Map', () => {
    const decoder = new Decoder(bytes(0x00, 0x09))
    const fields = decoder.readTaggedFields()

    expect(fields).toBeInstanceOf(Map)
    expect(fields.size).toEqual(0)
    expect(decoder.readInt8()).toEqual(9)
  })

  test('the empty section returns one shared, never-mutated Map', () => {
    const first = new Decoder(bytes(0x00)).readTaggedFields()
    const second = new Decoder(bytes(0x00)).readTaggedFields()

    expect(first).toBe(second)
    expect(first.size).toEqual(0)
    expect(() => first.set(1, Buffer.alloc(0))).toThrow(TypeError)
    expect(() => first.delete(1)).toThrow(TypeError)
    expect(() => first.clear()).toThrow(TypeError)
    expect(first.size).toEqual(0)
  })

  test('returns raw values keyed by tag and leaves the offset after the section', () => {
    // 2 fields: tag 0 -> [0xaa, 0xbb], tag 3 -> [0x01]; then a sentinel int8
    const decoder = new Decoder(bytes(0x02, 0x00, 0x02, 0xaa, 0xbb, 0x03, 0x01, 0x01, 0x09))
    const fields = decoder.readTaggedFields()

    expect([...fields.keys()]).toEqual([0, 3])
    expect([...fields.get(0)]).toEqual([0xaa, 0xbb])
    expect([...fields.get(3)]).toEqual([0x01])
    expect(decoder.readInt8()).toEqual(9)
  })

  test('skips an unknown tag by its length without failing', () => {
    // tag 0 -> 1 byte, tag 99 -> 3 bytes (unknown to any codec), then a sentinel
    const decoder = new Decoder(bytes(0x02, 0x00, 0x01, 0x07, 0x63, 0x03, 0xde, 0xad, 0xbe, 0x09))
    const fields = decoder.readTaggedFields()

    expect([...fields.keys()]).toEqual([0, 99])
    expect([...fields.get(99)]).toEqual([0xde, 0xad, 0xbe])
    expect(decoder.readInt8()).toEqual(9)
  })

  test('a zero-length tagged value is an empty buffer, not null', () => {
    const decoder = new Decoder(bytes(0x01, 0x05, 0x00, 0x09))
    const fields = decoder.readTaggedFields()

    expect([...fields.get(5)]).toEqual([])
    expect(decoder.readInt8()).toEqual(9)
  })

  test('rejects tags that are not in ascending order', () => {
    const decoder = new Decoder(bytes(0x02, 0x03, 0x01, 0x01, 0x00, 0x01, 0x02))
    expect(() => decoder.readTaggedFields()).toThrow(KafkaJSPartialMessageError)
    expect(() =>
      new Decoder(bytes(0x02, 0x03, 0x01, 0x01, 0x00, 0x01, 0x02)).readTaggedFields()
    ).toThrow(/ascending tag order/)
  })

  test('rejects a duplicate tag', () => {
    const decoder = new Decoder(bytes(0x02, 0x01, 0x01, 0x01, 0x01, 0x01, 0x02))
    expect(() => decoder.readTaggedFields()).toThrow(KafkaJSPartialMessageError)
  })

  test('rejects a tagged value that runs past the end of the frame', () => {
    const decoder = new Decoder(bytes(0x01, 0x00, 0x08, 0xaa))
    expect(() => decoder.readTaggedFields()).toThrow(KafkaJSPartialMessageError)
  })

  test('handles a multi-byte tag number', () => {
    // tag 300 (0xac 0x02) -> 1 byte
    const decoder = new Decoder(bytes(0x01, 0xac, 0x02, 0x01, 0x2a, 0x09))
    const fields = decoder.readTaggedFields()

    expect([...fields.get(300)]).toEqual([0x2a])
    expect(decoder.readInt8()).toEqual(9)
  })
})
