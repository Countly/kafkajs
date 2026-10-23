const fs = require('fs')
const path = require('path')
const { createHash } = require('crypto')
const { Codecs, Types, lookupCodec } = require('./index')
const snappy = new Proxy({}, { get: (_, key) => lookupCodec(Types.Snappy)[key] })

/**
 * Reconstructs the independently recorded Java generator inputs.
 * @param {string} name corpus kind
 * @param {number} length input bytes
 * @returns {Buffer} input bytes
 */
const corpus = (name, length) => {
  const bytes = Buffer.alloc(length)
  const text = Buffer.from('{"key":"value","count":123,"active":true}\n')
  let state = 0x12345678
  for (let i = 0; i < length; i += 1) {
    state ^= state << 13
    state ^= state >>> 17
    state ^= state << 5
    bytes[i] =
      name === 'random' || (name === 'mixed' && i % 256 < 64)
        ? state
        : name === 'zeros'
          ? 0
          : text[i % text.length]
  }
  return bytes
}

const XERIAL_HEADER = Buffer.from('82534e41505059000000000100000001', 'hex')
const MOST_UNITS_PER_TURN = 4096

/**
 * Encodes a Snappy length preamble.
 * @param {number} n decoded length
 * @returns {Buffer} varint bytes
 */
const preamble = (n) => {
  const bytes = []
  do {
    bytes.push((n & 127) | (n > 127 ? 128 : 0))
    n = Math.floor(n / 128)
  } while (n)
  return Buffer.from(bytes)
}

/**
 * Builds an xerial stream of identical framed blocks.
 * @param {string} frameHex one frame: four length bytes then the raw block
 * @param {number} count frame repetitions
 * @returns {Buffer} xerial stream
 */
const framedOf = (frameHex, count) => {
  const frame = Buffer.from(frameHex, 'hex')
  const stream = Buffer.alloc(16 + frame.length * count)
  XERIAL_HEADER.copy(stream)
  for (let at = 16; at < stream.length; at += frame.length) frame.copy(stream, at)
  return stream
}

/**
 * Builds a raw stream of identical tokens that decode to 97-valued bytes.
 * @param {string} tokenHex one token
 * @param {number} count token repetitions
 * @param {number} length decoded length
 * @param {string} [first] a leading token, needed before a copy
 * @returns {Buffer} raw stream
 */
const rawOf = (tokenHex, count, length, first = '') => {
  const token = Buffer.from(tokenHex, 'hex')
  const body = Buffer.alloc(token.length * count)
  for (let at = 0; at < body.length; at += token.length) token.copy(body, at)
  return Buffer.concat([preamble(length), Buffer.from(first, 'hex'), body])
}

/**
 * Decodes while recording the real event-loop turns. Output is captured zero-filled, so the
 * prefix of 97-valued bytes counts the blocks or tokens done at each turn.
 * @param {Buffer} input encoded stream
 * @param {number} length decoded length
 * @returns {Promise<{ output: Buffer, scanTurns: number, turns: number, progress: number[] }>}
 *   turns before the output allocation, all turns, and the decoded prefix at each later turn
 */
const observeTurns = async (input, length) => {
  const realImmediate = setImmediate
  const progress = []
  let target
  let turns = 0
  let scanTurns = 0
  const allocate = jest.spyOn(Buffer, 'allocUnsafe').mockImplementation((size) => {
    const bytes = Buffer.alloc(size)
    if (size === length) {
      target = bytes
      scanTurns = turns
    }
    return bytes
  })
  const immediate = jest.spyOn(global, 'setImmediate').mockImplementation((fn, ...args) => {
    turns += 1
    const done = target?.indexOf(0)
    if (target) progress.push(done < 0 ? length : done)
    return realImmediate(fn, ...args)
  })
  try {
    return { output: await snappy.decompress(input), scanTurns, turns, progress }
  } finally {
    immediate.mockRestore()
    allocate.mockRestore()
  }
}

/**
 * Largest run of work between two turns.
 * @param {number[]} progress decoded prefix at each turn
 * @param {number} length final decoded length
 * @param {number} perUnit output bytes one block or token produces
 * @returns {number} most blocks or tokens done in one slice
 */
const longestSlice = (progress, length, perUnit) =>
  Math.max(...[...progress, length].map((done, i, all) => (done - (all[i - 1] ?? 0)) / perUnit))

const vectors = fs
  .readdirSync(path.join(__dirname, 'fixtures'))
  .filter((file) => /^snappy-.*\.json$/.test(file))
  .map((file) => [file, require(`./fixtures/${file}`)])

describe('Snappy raw and xerial codec', () => {
  test.each(vectors)('decodes the independent Java vector %s', async (_, vector) => {
    const expected = corpus(vector.source.input, vector.source.length)
    expect(createHash('sha256').update(expected).digest('hex')).toBe(vector.fields.sha256)
    const output = await snappy.decompress(Buffer.from(vector.hex, 'hex'))
    expect(output).toEqual(expected)
    expect(output.length).toBe(vector.fields.size)
  })

  test.each([0, 1, 29, 60, 61, 255, 256, 257, 32767, 32768, 32769, 65535, 65536, 65537])(
    'encodes %i bytes with independent 32 KiB blocks inside its derived bound',
    async (length) => {
      for (const name of ['random', 'zeros', 'mixed']) {
        const input = corpus(name, length)
        const encoded = await snappy.compress({ buffer: input })
        expect(encoded.length).toBeLessThanOrEqual(snappy.encodedBound(length))
        expect(encoded.subarray(0, 16).toString('hex')).toBe('82534e41505059000000000100000001')
        let blocks = 0
        for (let at = 16; at < encoded.length;) {
          const end = at + 4 + encoded.readUInt32BE(at)
          expect(end).toBeLessThanOrEqual(encoded.length)
          expect(
            (await snappy.decompress(encoded.subarray(at + 4, end))).length
          ).toBeLessThanOrEqual(32768)
          at = end
          blocks += 1
        }
        expect(blocks).toBe(Math.ceil(length / 32768))
        expect(await snappy.decompress(encoded)).toEqual(input)
      }
    }
  )

  test('the bound reserves the exact framing constant for each additional block', () => {
    for (const n of [0, 1, 32768, 32769, 65536, 65537]) {
      expect(snappy.encodedBound(n)).toBe(16 + n + 12 * Math.ceil(n / 32768) + Math.floor(n / 32))
    }
    expect(() => snappy.encodedBound(-1)).toThrow(RangeError)
    expect(() => snappy.encodedBound(1.5)).toThrow(RangeError)
    expect(() => snappy.encodedBound(Number.MAX_SAFE_INTEGER)).toThrow(RangeError)
  })

  test('adversarial literal and short-copy runs stay within the bound', async () => {
    const inputs = [
      Buffer.concat(
        Array.from({ length: 4096 }, (_, i) => Buffer.from([i & 255, 97, 98, 99, 100]))
      ),
      Buffer.concat([corpus('random', 32760), corpus('random', 8)]),
      ...[61, 257].map((n) => Buffer.concat(Array.from({ length: 90 }, () => corpus('random', n)))),
    ]
    for (const buffer of inputs) {
      const encoded = await snappy.compress({ buffer })
      expect(encoded.length).toBeLessThanOrEqual(snappy.encodedBound(buffer.length))
      expect(await snappy.decompress(encoded)).toEqual(buffer)
    }
  })

  test.each([
    ['one-byte offset', '0500610101', 'aaaaa'],
    ['two-byte offset', '0500610e0100', 'aaaaa'],
    ['four-byte offset', '0500610f01000000', 'aaaaa'],
    ['extended literal', '3df03c' + '61'.repeat(61), 'a'.repeat(61)],
  ])('accepts a hand-computed %s token', async (_, hex, expected) => {
    expect(await snappy.decompress(Buffer.from(hex, 'hex'))).toEqual(Buffer.from(expected))
  })

  test.each([
    '',
    '80',
    'ffffffff10',
    '0104',
    '01006100',
    '01020100',
    '010300000000',
    '020061',
    '010c61626364',
    '01f0',
    '82534e4150505900000000010000000100000005',
    '82534e4150505900000000010000000100000000',
    '82534e4150505900000000010000000100',
    '82534e41505059000000000000000001',
  ])('rejects malformed bytes %s with a module-local error', async (hex) => {
    await expect(snappy.decompress(Buffer.from(hex, 'hex'))).rejects.toMatchObject({
      name: 'SnappyDecodeError',
      reason: 'decompression',
    })
  })

  test('checks the entire xerial declared sum before output allocation', async () => {
    const input = Buffer.from('82534e415050590000000001000000010000000301006100000003010062', 'hex')
    lookupCodec(Types.Snappy)
    const allocate = jest.spyOn(Buffer, 'allocUnsafe')
    try {
      await expect(snappy.decompress(input, { maxOutputLength: 1 })).rejects.toMatchObject({
        reason: 'decompressed-size',
      })
      expect(allocate).not.toHaveBeenCalled()
    } finally {
      allocate.mockRestore()
    }
    expect(await snappy.decompress(input, { maxOutputLength: 2 })).toEqual(Buffer.from('ab'))
    await expect(
      snappy.decompress(Buffer.from('020061', 'hex'), { maxOutputLength: 1 })
    ).rejects.toMatchObject({ reason: 'decompressed-size' })
  })

  test('the built-in is registered and a plugin assignment wins', () => {
    const saved = Codecs[Types.Snappy]
    expect(lookupCodec(Types.Snappy)).toBe(require('./snappy'))
    const plugin = { compress: jest.fn(), decompress: jest.fn() }
    try {
      Codecs[Types.Snappy] = () => plugin
      expect(lookupCodec(Types.Snappy)).toBe(plugin)
    } finally {
      Codecs[Types.Snappy] = saved
    }
  })

  test.each([
    ['empty', '0000000100', 0],
    ['one-byte', '00000003010061', 1],
  ])(
    'the pre-scan gives %s framed blocks a turn at least every 4096 blocks',
    async (_, frame, bytes) => {
      const blocks = 120000
      const { output, scanTurns } = await observeTurns(framedOf(frame, blocks), blocks * bytes)
      expect(output).toEqual(Buffer.alloc(blocks * bytes, 97))
      expect(blocks).toBeLessThanOrEqual(MOST_UNITS_PER_TURN * (scanTurns + 1))
    }
  )

  test('decoding empty framed blocks gives the event loop a turn at least every 4096 blocks', async () => {
    const blocks = 120000
    const { scanTurns, turns } = await observeTurns(framedOf('0000000100', blocks), 0)
    expect(blocks).toBeLessThanOrEqual(MOST_UNITS_PER_TURN * (turns - scanTurns + 1))
  })

  test('decoding one-byte framed blocks gives the event loop a turn at least every 4096 blocks', async () => {
    const blocks = 120000
    const { progress } = await observeTurns(framedOf('00000003010061', blocks), blocks)
    expect(longestSlice(progress, blocks, 1)).toBeLessThanOrEqual(MOST_UNITS_PER_TURN)
  })

  test.each([
    ['one-byte literals', '0061', 0, 1, 400000],
    ['four-byte copies', '0101', 1, 4, 100000],
  ])(
    'raw %s give the event loop a turn at least every 4096 tokens',
    async (_, token, lead, bytes, count) => {
      const length = lead + bytes * count
      const { output, progress } = await observeTurns(
        rawOf(token, count, length, lead ? '0061' : ''),
        length
      )
      expect(output).toEqual(Buffer.alloc(length, 97))
      expect(longestSlice(progress, length, bytes)).toBeLessThanOrEqual(MOST_UNITS_PER_TURN)
    }
  )

  test('a long literal is copied in slices of at most 256 KiB', async () => {
    const length = 1048576
    const input = Buffer.concat([
      preamble(length),
      Buffer.from('f8ffff0f', 'hex'),
      Buffer.alloc(length, 97),
    ])
    const { output, progress } = await observeTurns(input, length)
    expect(output).toEqual(Buffer.alloc(length, 97))
    expect(longestSlice(progress, length, 1)).toBeLessThanOrEqual(262144)
  })

  test('large encode, decode, long literals and the tiny-block pre-scan yield to the event loop', async () => {
    const input = corpus('mixed', 1024 * 1024)
    const compressed = await snappy.compress({ buffer: input })
    const million = framedOf('00000003010061', 1000000)
    const literal = Buffer.concat([Buffer.from('808020f8ffff07', 'hex'), Buffer.alloc(524288, 97)])
    for (const operation of [
      () => snappy.compress({ buffer: input }),
      () => snappy.decompress(compressed),
      () => snappy.decompress(million),
      () => snappy.decompress(literal),
    ]) {
      let ticked = false
      const tick = new Promise((resolve) =>
        setImmediate(() => {
          ticked = true
          resolve()
        })
      )
      await operation()
      expect(ticked).toBe(true)
      await tick
    }
  })
})
