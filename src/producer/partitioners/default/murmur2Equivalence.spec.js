const Long = require('../../../utils/long')
const murmur2 = require('./murmur2')

const SEED = Long.fromValue(0x9747b28c)
const M = Long.fromValue(0x5bd1e995)
const R = Long.fromValue(24)

/**
 * The 64-bit-emulated murmur2 this module computed with before it moved to plain 32-bit integer
 * arithmetic, kept verbatim as the oracle: the rewrite must agree with it on every input, not only
 * on the Java-generated fixtures.
 * @param {Buffer|string|number} key
 * @returns {number}
 */
const longMurmur2 = (key) => {
  const data = Buffer.isBuffer(key) ? key : Buffer.from(String(key))
  const length = data.length
  let h = Long.fromValue(SEED.xor(length))
  const length4 = Math.floor(length / 4)

  for (let i = 0; i < length4; i++) {
    const i4 = i * 4
    let k =
      (data[i4 + 0] & 0xff) +
      ((data[i4 + 1] & 0xff) << 8) +
      ((data[i4 + 2] & 0xff) << 16) +
      ((data[i4 + 3] & 0xff) << 24)
    k = Long.fromValue(k)
    k = k.multiply(M)
    k = k.xor(k.toInt() >>> R)
    k = Long.fromValue(k).multiply(M)
    h = h.multiply(M)
    h = h.xor(k)
  }

  switch (length % 4) {
    case 3:
      h = h.xor((data[(length & ~3) + 2] & 0xff) << 16)
    // falls through
    case 2:
      h = h.xor((data[(length & ~3) + 1] & 0xff) << 8)
    // falls through
    case 1:
      h = h.xor(data[length & ~3] & 0xff)
      h = h.multiply(M)
  }

  h = h.xor(h.toInt() >>> 13)
  h = h.multiply(M)
  h = h.xor(h.toInt() >>> 15)

  return h.toInt()
}

/**
 * mulberry32: a seeded generator, so the corpus is the same on every run and a failing input can
 * be rebuilt from its index.
 * @param {number} seed
 * @returns {() => number} uniform in [0, 1)
 */
const seeded = (seed) => () => {
  seed = (seed + 0x6d2b79f5) | 0
  let t = Math.imul(seed ^ (seed >>> 15), 1 | seed)
  t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296
}

/**
 * Every key shape the hash has to agree on: the fixture-style keys, empty and non-ASCII strings,
 * every buffer of 0–4 bytes over the byte values that exercise the sign and the tail (1,555 of
 * them), and 5,000 seeded random buffers of 0–300 bytes.
 * @returns {Array<Buffer|string|number>}
 */
const corpus = () => {
  const keys = [0, 1, 128, '100:48069', '', 'clé', '日本語', '🙂', 'a'.repeat(1000)]
  const edgeBytes = [0x00, 0x01, 0x7f, 0x80, 0xfe, 0xff]
  let level = [[]]
  for (let length = 0; length <= 4; length++) {
    keys.push(...level.map((bytes) => Buffer.from(bytes)))
    level = level.flatMap((bytes) => edgeBytes.map((byte) => [...bytes, byte]))
  }

  const random = seeded(0x5eed)
  for (let i = 0; i < 5000; i++) {
    const length = Math.floor(random() * 301)
    keys.push(Buffer.from(Array.from({ length }, () => Math.floor(random() * 256))))
  }

  return keys
}

describe('Producer > Partitioner > Default > murmur2, against the implementation it replaced', () => {
  test('agrees with it on every key of the corpus', () => {
    const keys = corpus()
    const mismatches = keys
      .map((key, index) => ({ index, expected: longMurmur2(key), actual: murmur2(key) }))
      .filter(({ expected, actual }) => expected !== actual)

    expect(keys.length).toEqual(6564)
    expect(mismatches).toEqual([])
  })

  test('returns a signed 32-bit integer', () => {
    for (const key of ['', 'a', 'abcd', Buffer.from([0xff, 0xff, 0xff, 0xff, 0x80])]) {
      const hash = murmur2(key)
      expect(hash).toEqual(hash | 0)
    }
  })

  test('hashes without loading the 64-bit Long utility', () => {
    const key = Buffer.from('a sixteen-byte k')
    jest.doMock('../../../utils/long', () => {
      throw new Error('Long loaded')
    })

    try {
      jest.isolateModules(() => {
        expect(require('./murmur2')(key)).toEqual(murmur2(key))
      })
    } finally {
      jest.dontMock('../../../utils/long')
    }
  })
})
