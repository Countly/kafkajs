const Encoder = require('../encoder')
const { uvarintSize, writeUVarint, bigUvarintSize, writeBigUVarint } = require('./varint')

/** Every byte-count boundary of a zigzag value: the last even one of n bytes and the first of n + 1. */
const BOUNDARIES = [0, 7, 14, 21, 28, 35, 42, 49].flatMap((bits) =>
  bits === 0 ? [0] : [2 ** bits - 2, 2 ** bits]
)

/**
 * @param {(buffer: Buffer, offset: number) => number} write
 * @returns {{ bytes: string, end: number }} what `write` put at offset 3 of a 16-byte buffer
 */
const written = (write) => {
  const buffer = Buffer.alloc(16)
  const end = write(buffer, 3)
  return { bytes: buffer.subarray(3, end).toString('hex'), end: end - 3 }
}

describe('Protocol > RecordBatch > varint', () => {
  test.each(BOUNDARIES)(
    'the unsigned varint of %p is the zigzag varlong of its half, in size and bytes',
    (value) => {
      const expected = new Encoder().writeVarLong(value / 2).buffer

      expect(written((buffer, offset) => writeUVarint(buffer, offset, value))).toEqual({
        bytes: expected.toString('hex'),
        end: expected.length,
      })
      expect(uvarintSize(value)).toBe(Encoder.sizeOfVarLong(value / 2))
    }
  )

  test.each([
    [0, 1],
    [127, 1],
    [128, 2],
    [16383, 2],
    [16384, 3],
    [2097151, 3],
    [2097152, 4],
    [268435455, 4],
    [268435456, 5],
    [34359738367, 5],
    [34359738368, 6],
    [4398046511103, 6],
    [4398046511104, 7],
    [562949953421311, 7],
    [562949953421312, 8],
    [2 ** 54, 8],
  ])('%p takes %p bytes as an unsigned varint', (value, size) => {
    expect(uvarintSize(value)).toBe(size)
    expect(written((buffer, offset) => writeUVarint(buffer, offset, value)).end).toBe(size)
  })

  test('the zigzag of every record length is the varint the record encoder writes', () => {
    for (const length of [0, 1, 63, 64, 8191, 8192, 1048575, 1048576, 2 ** 30, 2 ** 31 - 1]) {
      const expected = new Encoder().writeVarInt(length).buffer

      expect(written((buffer, offset) => writeUVarint(buffer, offset, 2 * length)).bytes).toBe(
        expected.toString('hex')
      )
      expect(uvarintSize(2 * length)).toBe(Encoder.sizeOfVarInt(length))
    }
  })

  test.each([0n, 63n, 64n, 2n ** 52n, 2n ** 53n, 2n ** 56n, 2n ** 62n, 2n ** 63n - 1n])(
    'a bigint delta of %p is the zigzag varlong the record encoder writes',
    (delta) => {
      const expected = new Encoder().writeVarLong(delta).buffer

      expect(written((buffer, offset) => writeBigUVarint(buffer, offset, 2n * delta))).toEqual({
        bytes: expected.toString('hex'),
        end: expected.length,
      })
      expect(bigUvarintSize(2n * delta)).toBe(Encoder.sizeOfVarLong(delta))
    }
  )
})
