const path = require('path')
const zlib = require('zlib')
const { loadCorpus, runCases, caseLabel } = require('../../../../testHelpers/fuzz')
const { frame, block, runBlock } = require('../../../../testHelpers/lz4Frame')
const { Types, lookupCodec, decompressBounded } = require('./index')
const { MIB, BOUND, bodyOf, bombOf, mutationCases, classCases } = require('./fixtures/hostileCases')
const { compressedBatch, COMPRESSORS } = require('./fixtures/compressedBatch')
const handler = require('../../__tests__/fixtures/compressedDecodeHandler')

const HANDLER = path.join(
  __dirname,
  '..',
  '..',
  '__tests__',
  'fixtures',
  'compressedDecodeHandler.js'
)
const RETAINED = path.join(__dirname, '..', '..', 'fixtures', 'fuzz', 'compressed')
const SEED = 20261019
const SLOW_MS = 2000

// eslint-disable-next-line n/no-unsupported-features/node-builtins -- feature detection, not a call
const hasZstd = typeof zlib.zstdCompressSync === 'function'
let hasLz4 = true
try {
  require('lz4-napi')
} catch (e) {
  hasLz4 = false
}
const CODECS = ['gzip', 'snappy', ...(hasZstd ? ['zstd'] : []), ...(hasLz4 ? ['lz4'] : [])]
const describeIfLz4 = hasLz4 ? describe : describe.skip

/**
 * The peak resident set size a piece of work adds, sampled every 2 ms. Decompression runs on the
 * thread pool, so the sampler keeps running while it does.
 * @param {() => Promise<void>} work
 * @returns {Promise<number>} bytes above the size before the work
 */
const peakGrowth = async (work) => {
  const before = process.memoryUsage.rss()
  let peak = before
  const sampler = setInterval(() => {
    peak = Math.max(peak, process.memoryUsage.rss())
  }, 2)
  try {
    await work()
  } finally {
    clearInterval(sampler)
  }
  return Math.max(peak, process.memoryUsage.rss()) - before
}

// These run one at a time: a figure of resident memory is only meaningful when nothing else in
// the process is allocating.
describe('Protocol > Compression > a bomb built by streaming', () => {
  const LIMIT = 64 * MIB
  const ALLOWANCE = LIMIT + 16 * MIB

  test.each(['gzip', ...(hasZstd ? ['zstd'] : [])])(
    'a %s stream of 512 MiB is refused with a rise of at most the bound plus 16 MiB',
    async (name) => {
      const bomb = await bombOf(name, 512)
      const codec = lookupCodec(Types[name === 'gzip' ? 'GZIP' : 'ZSTD'])
      let error

      const growth = await peakGrowth(async () => {
        error = await decompressBounded(codec, name === 'gzip' ? 1 : 4, bomb, LIMIT).catch((e) => e)
      })

      expect(error).toMatchObject({ reason: 'decompressed-size', codec: name, limit: LIMIT })
      expect(growth).toBeLessThanOrEqual(ALLOWANCE)
    },
    60000
  )

  describeIfLz4('LZ4', () => {
    test('a frame of 4 GiB in 4 MiB blocks is refused before the native decoder, with no rise', async () => {
      const native = jest.spyOn(require('lz4-napi'), 'decompressFrame')
      try {
        const bomb = frame({
          maxSizeCode: 7,
          blocks: Array.from({ length: 1000 }, () => block(runBlock(0, 4 * MIB))),
        })
        const codec = lookupCodec(Types.LZ4)
        const started = Date.now()
        let error

        const growth = await peakGrowth(async () => {
          error = await decompressBounded(codec, 3, bomb, LIMIT).catch((e) => e)
        })

        expect(error).toMatchObject({ reason: 'decompressed-size', codec: 'lz4', limit: LIMIT })
        expect(native).not.toHaveBeenCalled()
        expect(growth).toBeLessThanOrEqual(16 * MIB)
        expect(Date.now() - started).toBeLessThan(SLOW_MS)
      } finally {
        native.mockRestore()
      }
    })

    test('a legitimate frame of 300 MiB in 4 MiB blocks passes under the default bound of 1 GiB', async () => {
      const legitimate = frame({
        maxSizeCode: 7,
        blocks: Array.from({ length: 75 }, () => block(runBlock(65, 4 * MIB))),
      })
      const codec = lookupCodec(Types.LZ4)

      const output = await decompressBounded(codec, 3, legitimate, 1073741824)

      expect(output.length).toBe(300 * MIB)
      expect([output[0], output[output.length - 1]]).toEqual([65, 65])
    }, 60000)
  })

  test('a Snappy block declaring 2 GiB is refused before anything is allocated for it', async () => {
    const lying = Buffer.from([0x80, 0x80, 0x80, 0x80, 0x08, 0x00, 0x61])
    const codec = lookupCodec(Types.Snappy)
    let error

    const growth = await peakGrowth(async () => {
      error = await decompressBounded(codec, 2, lying, LIMIT).catch((e) => e)
    })

    expect(error).toMatchObject({ reason: 'decompressed-size', codec: 'snappy' })
    expect(growth).toBeLessThanOrEqual(16 * MIB)
  })

  test('a Snappy stream of 64 MiB of zeros decodes to exactly the declared size', async () => {
    const compressed = await COMPRESSORS.snappy(Buffer.alloc(64 * MIB))
    const codec = lookupCodec(Types.Snappy)

    const output = await decompressBounded(codec, 2, compressed, 64 * MIB)

    expect(output.length).toBe(64 * MIB)
    await expect(decompressBounded(codec, 2, compressed, 64 * MIB - 1)).rejects.toMatchObject({
      reason: 'decompressed-size',
    })
  }, 60000)
})

/**
 * The outcome declared for each class of hostile input: what the decoder carries in the partition,
 * and for a refusal by size the bound it names.
 */
const OUTCOME = {
  'stream cut short': { outcome: 'corrupt:decompression' },
  'count past the bytes': { outcome: 'corrupt:count' },
  'bomb past the bound': { outcome: 'corrupt:decompressed-size', limit: BOUND },
  junk: { outcome: 'corrupt:decompression' },
  'content size understated': { outcome: 'corrupt:decompression' },
  'declared length over the bound': { outcome: 'corrupt:decompressed-size', limit: BOUND },
  'declared lengths over the bound in all': { outcome: 'corrupt:decompressed-size', limit: BOUND },
}

describe('Protocol > Compression > the outcome of each class of hostile input', () => {
  let cases

  beforeAll(async () => {
    cases = await classCases(CODECS)
  })

  test('has a case for every class of every codec of this build', () => {
    // stream cut short, count past the bytes, bomb past the bound and junk, per codec, then the
    // lz4 content size and the two Snappy lying lengths
    expect(cases).toHaveLength(CODECS.length * 4 + (hasLz4 ? 1 : 0) + 2)
    expect(new Set(cases.map(({ kind }) => kind))).toEqual(
      new Set(Object.keys(OUTCOME).filter((kind) => hasLz4 || kind !== 'content size understated'))
    )
  })

  test('every case is refused with its declared outcome, its codec and the bound, within two seconds', async () => {
    const wrong = []

    for (const found of cases) {
      const result = await handler(found)
      const { outcome, limit } = OUTCOME[found.kind]

      if (
        result.outcome !== outcome ||
        result.violation !== null ||
        result.milliseconds > SLOW_MS ||
        result.error.codec !== found.codec ||
        (limit && result.error.limit !== limit)
      ) {
        wrong.push(
          `${found.name}: ${result.outcome} ${JSON.stringify(result.error)} ${result.violation}`
        )
      }
    }

    expect(wrong).toEqual([])
  })

  test('a refused batch names the offset after it when the header was accepted', async () => {
    const [bomb] = cases.filter(({ name }) => name === 'gzip bomb past the bound')

    const result = await handler(bomb)

    expect(result.error).toMatchObject({
      reason: 'decompressed-size',
      offset: '0',
      nextOffset: '3',
    })
  })
})

describe('Protocol > Compression > seeded mutations of a valid compressed batch', () => {
  test.each(CODECS)(
    '%s: every mutation is decoded or refused with a typed error, never a RangeError or TypeError',
    async (name) => {
      const cases = await mutationCases(name, SEED)

      const results = await runCases({ handler: HANDLER, cases, seed: SEED })
      const violations = results
        .map((result, index) => ({ result, index }))
        .filter(({ result }) => result.violation !== null || result.milliseconds > SLOW_MS)

      expect(
        violations
          .slice(0, 5)
          .map(
            ({ result, index }) =>
              `${result.violation}: ${caseLabel({ seed: SEED, index, buffer: cases[index].buffer })}`
          )
      ).toEqual([])
      expect(results).toHaveLength(cases.length)
      expect(results.some(({ error }) => error && error.reason === 'decompression')).toBe(true)
    },
    120000
  )

  test('the body of a case is a Fetch response that carries a compressed batch', async () => {
    const batch = await compressedBatch({ codec: 'gzip', records: 3 })

    expect(bodyOf(batch).includes(batch)).toBe(true)
    expect(BOUND).toBe(4 * MIB)
  })
})

describe('Protocol > Compression > the retained cases', () => {
  const retained = loadCorpus(RETAINED)

  test('hold a case for every codec', () => {
    expect(new Set(retained.map(({ expect: { codec } }) => codec))).toEqual(new Set(CODECS))
  })

  test.each(retained.map((found) => [found.name, found]))(
    '%s is refused as it was declared to be',
    async (_, found) => {
      const result = await handler({
        buffer: Buffer.from(found.hex, 'hex'),
        options: found.options,
      })

      expect(result).toMatchObject({
        outcome: found.expect.outcome,
        violation: found.expect.violation,
      })
      expect(result.error).toMatchObject({
        codec: found.expect.codec,
        ...(found.expect.limit ? { limit: found.expect.limit } : {}),
      })
      expect(result.milliseconds).toBeLessThan(SLOW_MS)
    }
  )

  test('grow the heap by under the bound plus 16 MiB when all are decoded', async () => {
    const before = process.memoryUsage.rss()

    for (const found of retained) {
      await handler({ buffer: Buffer.from(found.hex, 'hex'), options: found.options })
    }

    expect(process.memoryUsage.rss() - before).toBeLessThan(BOUND + 16 * MIB)
  })
})
