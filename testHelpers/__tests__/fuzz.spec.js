const fs = require('fs')
const os = require('os')
const path = require('path')
const {
  CASES_PER_WORKER,
  byteMutations,
  caseLabel,
  chunks,
  int32Mutations,
  loadCorpus,
  mulberry32,
  mutations,
  repeatedSlices,
  runCases,
  runInWorker,
  shrink,
  truncations,
} = require('../fuzz')
const schedule = require('../schedule')

const handler = require.resolve('./fixtures/fuzzHandler')

/** A body of `length` bytes none of which is one of the values a byte mutation writes. */
const bodyOf = (length) => Buffer.alloc(length, 0x11)

describe('testHelpers > fuzz', () => {
  test('the generator is the schedule helper’s, so a seed means one thing across the helpers', () => {
    expect(mulberry32).toBe(schedule.mulberry32)
    expect([mulberry32(5)(), mulberry32(5)()]).toEqual([mulberry32(5)(), mulberry32(5)()])
  })

  describe('caseLabel', () => {
    test('prints the seed, the case index and the bytes, as a failure is replayed from them', () => {
      expect(caseLabel({ seed: 7, index: 12, buffer: Buffer.from([0, 1, 0xff]) })).toBe(
        'seed=7 case=12 hex=0001ff'
      )
    })
  })

  describe('truncations', () => {
    test('are every proper prefix, from the empty one', () => {
      const body = Buffer.from([1, 2, 3, 4])

      expect([...truncations(body)].map(({ buffer }) => [...buffer])).toEqual([
        [],
        [1],
        [1, 2],
        [1, 2, 3],
      ])
    })
  })

  describe('byte mutations', () => {
    test('write each of five values over every byte of a small body, one byte at a time', () => {
      const body = bodyOf(6)
      const found = [...byteMutations(body, 1)]

      expect(found).toHaveLength(6 * 5)
      for (const { buffer } of found) {
        expect([...buffer].filter((byte, index) => byte !== body[index])).toHaveLength(1)
      }
      expect(new Set(found.map(({ buffer }) => buffer.toString('hex'))).size).toBe(6 * 5)
    })

    test('skip a value the byte already has', () => {
      const found = [...byteMutations(Buffer.from([0x7f]), 1)]

      expect(found.map(({ buffer }) => buffer[0])).toEqual([0x00, 0x01, 0x80, 0xff])
    })

    test('sample 2000 positions of a body above 512 bytes, the same ones for the same seed', () => {
      const body = bodyOf(3000)
      const positions = (seed) =>
        [...byteMutations(body, seed)].map(({ name }) => name.split(' ')[1])
      const first = positions(11)

      expect(new Set(first).size).toBe(2000)
      expect(first).toEqual(positions(11))
      expect(first).not.toEqual(positions(12))
    })

    test('use every position of a body of exactly 512 bytes', () => {
      expect(
        new Set([...byteMutations(bodyOf(512), 3)].map(({ name }) => name.split(' ')[1])).size
      ).toBe(512)
    })
  })

  describe('int32 mutations', () => {
    test('write -1, 0, 2^31 - 1, -2^31 and the bytes that follow, less one and plus one, at every offset', () => {
      const body = bodyOf(8)
      const atFirst = [...int32Mutations(body, 1)].filter(({ name }) => name.startsWith('int32 0 '))

      expect(atFirst.map(({ buffer }) => buffer.readInt32BE(0))).toEqual([
        -1, 0, 0x7fffffff, -0x80000000, 3, 5,
      ])
      expect([...int32Mutations(body, 1)]).toHaveLength(28)
      for (const { buffer } of atFirst) {
        expect(buffer.subarray(4)).toEqual(body.subarray(4))
      }
    })

    test('skip a value the field already has, and write a value that two rules give once', () => {
      const found = [...int32Mutations(Buffer.from([0, 0, 0, 0]), 1)]

      expect(found.map(({ buffer }) => buffer.readInt32BE(0))).toEqual([
        -1, 0x7fffffff, -0x80000000, 1,
      ])
    })
  })

  describe('repeated slices', () => {
    test('repeat a slice where it ends, keeping the start of the body, the same ones for the same seed', () => {
      const body = Buffer.from(Array.from({ length: 100 }, (_, index) => index))
      const found = [...repeatedSlices(body, 9, 50)]

      expect(found).toHaveLength(50)
      for (const { buffer } of found) {
        expect(buffer.length).toBeGreaterThan(body.length)
        expect(buffer.subarray(0, 1)).toEqual(body.subarray(0, 1))
      }
      expect(found.map(({ buffer }) => buffer.toString('hex'))).toEqual(
        [...repeatedSlices(body, 9, 50)].map(({ buffer }) => buffer.toString('hex'))
      )
    })

    test('make nothing of a body of one byte', () => {
      expect([...repeatedSlices(Buffer.from([1]), 1)]).toEqual([])
    })
  })

  test('mutations are the byte, int32 and slice streams, in that order', () => {
    const names = [...mutations(bodyOf(5), 1)].map(({ name }) => name.split(' ')[0])

    expect(names[0]).toBe('byte')
    expect(names.lastIndexOf('byte')).toBeLessThan(names.indexOf('int32'))
    expect(names.lastIndexOf('int32')).toBeLessThan(names.indexOf('slice'))
  })

  describe('shrink', () => {
    test('cuts the end off and zeroes what does not matter', async () => {
      const stillFails = (candidate) => candidate.length >= 3 && candidate[2] === 3

      await expect(shrink(Buffer.from([9, 8, 3, 7, 6]), stillFails)).resolves.toEqual(
        Buffer.from([0, 0, 3])
      )
    })

    test('refuses an input that does not fail', async () => {
      await expect(shrink(Buffer.from([1]), () => false)).rejects.toThrow(
        'shrink needs an input that fails'
      )
    })

    test('keeps what is needed to fail, even when that is every byte', async () => {
      const input = Buffer.from([1, 2, 3])

      await expect(shrink(input, (candidate) => candidate.equals(input))).resolves.toEqual(input)
    })
  })

  describe('loadCorpus', () => {
    let directory

    beforeEach(() => {
      directory = fs.mkdtempSync(path.join(os.tmpdir(), 'fuzz-corpus-'))
    })

    afterEach(() => {
      fs.rmSync(directory, { recursive: true, force: true })
    })

    const write = (name, content) =>
      fs.writeFileSync(path.join(directory, name), JSON.stringify(content))
    const found = {
      seed: 1,
      hex: '00',
      api: 'Fetch',
      version: 4,
      direction: 'response',
      expect: {},
    }

    test('reads every JSON case in name order, naming each by its file', () => {
      write('b.json', found)
      write('a.json', { ...found, seed: 2 })
      fs.writeFileSync(path.join(directory, 'notes.txt'), 'not a case')

      expect(loadCorpus(directory).map(({ name, seed }) => [name, seed])).toEqual([
        ['a', 2],
        ['b', 1],
      ])
    })

    test('names the file and the fields of a case that lacks any', () => {
      write('bad.json', { seed: 1, hex: '00' })

      expect(() => loadCorpus(directory)).toThrow(
        'Corpus case bad.json has no api, version, direction, expect'
      )
    })
  })

  describe('chunks', () => {
    test('cut cases into runs of 500', () => {
      expect(CASES_PER_WORKER).toBe(500)
      expect(
        chunks(Array.from({ length: 1203 }, (_, index) => index)).map((run) => run.length)
      ).toEqual([500, 500, 203])
      expect(chunks([])).toEqual([])
    })
  })

  describe('workers', () => {
    test('run the handler on every case in order, and hand it bytes as a Uint8Array', async () => {
      const results = await runInWorker({
        handler,
        seed: 1,
        cases: [{ n: 1 }, { n: 2, buffer: Buffer.from([1, 2, 3]) }],
      })

      expect(results).toEqual([
        { n: 1, bytes: 0, isBuffer: false },
        { n: 2, bytes: 3, isBuffer: false },
      ])
    })

    test('terminate a handler that never returns, naming the seed and the case it was on', async () => {
      await expect(
        runInWorker({
          handler,
          seed: 42,
          watchdogMs: 1500,
          firstIndex: 1000,
          cases: [{ n: 1 }, { n: 2 }, { hang: true }, { n: 4 }],
        })
      ).rejects.toThrow('watchdog: seed=42 case=1002 did not return in 1500 ms')
    })

    test('report the error a handler failed with', async () => {
      await expect(
        runInWorker({ handler, seed: 1, cases: [{ n: 1 }, { fail: true }] })
      ).rejects.toThrow('handler failed')
    })

    test('report a handler that cannot be loaded', async () => {
      await expect(
        runInWorker({
          handler: path.join(os.tmpdir(), 'fuzz-no-such-handler.js'),
          seed: 1,
          cases: [{ n: 1 }],
        })
      ).rejects.toThrow('Cannot find module')
    })

    test('runCases splits a long run over workers and returns the results in order', async () => {
      const cases = Array.from({ length: CASES_PER_WORKER * 2 + 7 }, (_, n) => ({ n }))

      const results = await runCases({ handler, seed: 1, cases, concurrency: 2 })

      expect(results.map(({ n }) => n)).toEqual(cases.map(({ n }) => n))
    })

    test('runCases starts no batch once one has failed', async () => {
      const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'fuzz-'))
      const marker = path.join(directory, 'ran')
      const batch = (first) => [first, ...Array.from({ length: CASES_PER_WORKER - 1 }, () => ({}))]
      const cases = [
        ...batch({ fail: true }),
        ...batch({ sleepMs: 1000 }),
        ...batch({ mark: marker }),
      ]

      try {
        await expect(runCases({ handler, seed: 1, cases, concurrency: 2 })).rejects.toThrow(
          'handler failed'
        )
        await new Promise((resolve) => setTimeout(resolve, 1500))

        expect(fs.existsSync(marker)).toBe(false)
      } finally {
        fs.rmSync(directory, { recursive: true })
      }
    })

    test('runCases counts the index of a hung case from the start of the whole run', async () => {
      const cases = Array.from({ length: CASES_PER_WORKER + 5 }, (_, n) => ({ n }))
      cases[CASES_PER_WORKER + 2] = { hang: true }

      await expect(runCases({ handler, seed: 3, cases, watchdogMs: 1500 })).rejects.toThrow(
        `watchdog: seed=3 case=${CASES_PER_WORKER + 2} did not return in 1500 ms`
      )
    })
  })
})
