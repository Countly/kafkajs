const fs = require('fs')
const { spawnSync } = require('child_process')
const os = require('os')
const path = require('path')
const { parseArgs, run, compare } = require('../accumulatorScan')

const ROOT = path.join(__dirname, '../../..')
const SMALL = ['--topics', '2', '--partitions', '3', '--brokers', '3', '--records', '600']

describe('Benchmarks > accumulatorScan', () => {
  test('flags default to one topic of 1 000 partitions on 3 brokers, linger 5, this tree', () => {
    expect(parseArgs([])).toEqual({
      topics: 1,
      partitions: 1000,
      brokers: 3,
      linger: 5,
      records: 100000,
      yieldEvery: 20,
      maxRequestSize: 1048576,
      valueBytes: 200,
      perSend: 1,
      runs: 9,
      tree: ROOT,
      baselineTree: null,
      raw: null,
    })
    expect(
      parseArgs(['--yield-every', '5', '--baseline-tree', '.', '--raw', 'scan.jsonl'])
    ).toEqual(
      expect.objectContaining({
        yieldEvery: 5,
        baselineTree: path.resolve('.'),
        raw: path.resolve('scan.jsonl'),
      })
    )
  })

  test.each([
    [['--partitions', '0'], '--partitions needs a positive integer'],
    [['--records', '1.5'], '--records needs a positive integer'],
    [['--max-request-size', '0'], '--max-request-size needs a positive integer'],
    [['--pace', '1'], 'unknown or incomplete flag --pace'],
    [['--topics'], 'unknown or incomplete flag --topics'],
    [['--baseline-tree', '.'], '--raw is needed with --baseline-tree'],
  ])('%j is refused', (argv, message) => {
    expect(() => parseArgs(argv)).toThrow(message)
  })

  test.each(['1', '2', '4', '7', '10'])(
    'a comparison of %s runs is refused: its three arms would not take every position equally often',
    (runs) => {
      expect(() => parseArgs(['--baseline-tree', '.', '--raw', 'x', '--runs', runs])).toThrow(
        '--runs needs a multiple of 3 with --baseline-tree'
      )
    }
  )

  test.each(['3', '6', '9'])('a comparison of %s runs is accepted', (runs) => {
    expect(parseArgs(['--baseline-tree', '.', '--raw', 'x', '--runs', runs]).runs).toBe(
      Number(runs)
    )
  })

  test('any number of runs is accepted for a single run, which has one arm', () => {
    expect(parseArgs(['--runs', '4']).runs).toBe(4)
  })

  test('a run settles every send and counts one Produce request per leader of a hand-off', async () => {
    const result = await run(parseArgs(SMALL))

    expect(result).toEqual(
      expect.objectContaining({ tree: ROOT, topics: 2, partitions: 3, brokers: 3, records: 600 })
    )
    expect(result.handOffs).toBeGreaterThan(0)
    // Six partitions over three leaders: a hand-off that carries several partitions is several requests.
    expect(result.produceRequests).toBeGreaterThan(result.handOffs)
    expect(result.produceRequests).toBeLessThanOrEqual(result.handOffs * 3)
    expect(result.largestPayloadBytes).toBeGreaterThanOrEqual(200)
    expect(result.largestPayloadBytes % 200).toBe(0)
    expect(result.largestRequestEstimate).toBeGreaterThan(result.largestPayloadBytes)
  })

  /** Three partitions of 52 records of 100 KiB, each send one partition's 5.2 MiB. */
  const CORPUS = [
    '--partitions',
    '3',
    '--records',
    '156',
    '--per-send',
    '52',
    '--value-bytes',
    '102400',
    '--linger',
    '1',
  ]

  test('on 5 MiB per partition the modelled requests of this tree stay within --max-request-size', async () => {
    const result = await run(parseArgs(CORPUS))

    // five full chunks of ten records and one of two, per partition
    expect(result.produceRequests).toBe(18)
    expect(result.largestPayloadBytes).toBe(10 * 102400)
    expect(result.largestRequestEstimate).toBeLessThanOrEqual(1048576)
    expect(result.largestRequestEstimate).toBeGreaterThan(1048576 - 2 * 102432)
  })

  describe('the leaders of a run', () => {
    let dir

    /**
     * A tree whose accumulator and metadata cache are this repository's, each wrapped to record
     * how the scan uses it: the cache records every `leader()` lookup, the accumulator the options
     * it was built with. With `withLeader` false the cache has no `leader()`, like an older tree.
     */
    const recordingTree = (withLeader) => {
      const tree = fs.mkdtempSync(path.join(dir, 'tree-'))
      const module = (file, source) => {
        fs.mkdirSync(path.dirname(path.join(tree, file)), { recursive: true })
        fs.writeFileSync(path.join(tree, file), source)
      }
      const real = (file) => JSON.stringify(path.join(ROOT, file))
      module(
        'src/cluster/metadataCache.js',
        `const MetadataCache = require(${real('src/cluster/metadataCache')})
class Recording extends MetadataCache {
  ${
    withLeader
      ? `leader(topic, partition) {
    const leader = super.leader(topic, partition)
    Recording.lookups.push([topic, partition, leader])
    return leader
  }`
      : 'leader = undefined'
  }
}
Recording.lookups = []
module.exports = Recording
`
      )
      module(
        'src/producer/state/accumulator.js',
        `const Accumulator = require(${real('src/producer/state/accumulator')})
module.exports = class Recording extends Accumulator {
  constructor(options) {
    super(options)
    module.exports.options = options
  }
}
`
      )
      return {
        tree,
        lookups: () => require(path.join(tree, 'src/cluster/metadataCache')).lookups,
        options: () => require(path.join(tree, 'src/producer/state/accumulator')).options,
      }
    }

    const shape = (topics, partitions, tree) =>
      parseArgs([
        '--topics',
        String(topics),
        '--partitions',
        String(partitions),
        '--brokers',
        '3',
        '--records',
        '600',
        '--tree',
        tree,
      ])

    beforeAll(() => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), 'accumulator-scan-leaders-'))
    })

    afterAll(() => {
      fs.rmSync(dir, { recursive: true, force: true })
    })

    test.each([
      ['many topics', 4, 1],
      ['many partitions', 1, 5],
      ['topics and partitions', 2, 3],
    ])(
      'the accumulator asks the cache for the leader of every partition: %s',
      async (_, topics, partitions) => {
        const { tree, lookups } = recordingTree(true)
        const leaders = Array.from({ length: topics * partitions }, (_, pair) => [
          `t${Math.floor(pair / partitions)}`,
          pair % partitions,
          pair % 3,
        ])

        await run(shape(topics, partitions, tree))

        expect(new Set(lookups().map((lookup) => JSON.stringify(lookup)))).toEqual(
          new Set(leaders.map((lookup) => JSON.stringify(lookup)))
        )
      }
    )

    test('a cache without leader() is not asked, and the accumulator gets no leaderOf', async () => {
      const { tree, lookups, options } = recordingTree(false)

      const result = await run(shape(2, 3, tree))

      expect(result.handOffs).toBeGreaterThan(0)
      expect(options()).not.toHaveProperty('leaderOf')
      expect(lookups()).toEqual([])
    })

    test('a tree without a request-size plan sends each partition unsplit, past --max-request-size', async () => {
      const { tree, options } = recordingTree(true)

      const result = await run(parseArgs([...CORPUS, '--tree', tree]))

      expect(options()).not.toHaveProperty('budget')
      expect(result.produceRequests).toBe(3)
      expect(result.largestRequestEstimate).toBeGreaterThan(5 * 1048576)
    })

    test('a cache with leader() hands the accumulator a leaderOf that reads it', async () => {
      const { tree, lookups, options } = recordingTree(true)
      await run(shape(2, 3, tree))
      const before = lookups().length

      const leader = options().leaderOf('t1', 2)

      expect(leader).toBe(2)
      expect(lookups().length).toBe(before + 1)
      expect(lookups().slice(-1)[0]).toEqual(['t1', 2, 2])
    })
  })

  describe('the command line', () => {
    const script = path.join(__dirname, '../accumulatorScan.js')
    const cli = (args) => spawnSync(process.execPath, [script, ...args], { encoding: 'utf8' })

    test('a single run prints one JSON line', () => {
      const { status, stdout } = cli(SMALL)

      expect(status).toBe(0)
      expect(JSON.parse(stdout)).toEqual(expect.objectContaining({ tree: ROOT, records: 600 }))
    })

    test('a comparison of runs that are not a multiple of 3 exits 2 and runs nothing', () => {
      const { status, stdout, stderr } = cli(['--runs', '4', '--baseline-tree', '.', '--raw', 'x'])

      expect(status).toBe(2)
      expect(stdout).toBe('')
      expect(stderr).toContain('--runs needs a multiple of 3 with --baseline-tree')
    })

    test('a bad argument exits 2 with the reason', () => {
      const { status, stderr } = cli(['--partitions', '0'])

      expect(status).toBe(2)
      expect(stderr).toContain('--partitions needs a positive integer')
    })

    test('a comparison whose baseline cannot run exits 1 and prints no summary', () => {
      const raw = path.join(os.tmpdir(), `accumulator-scan-cli-${process.pid}.jsonl`)

      try {
        const { status, stdout, stderr } = cli([
          ...SMALL,
          '--runs',
          '3',
          '--baseline-tree',
          path.join(os.tmpdir(), 'no-such-tree'),
          '--raw',
          raw,
        ])

        expect(status).toBe(1)
        expect(stdout).toBe('')
        expect(stderr).toContain('the run on')
      } finally {
        fs.rmSync(raw, { force: true })
      }
    })
  })

  describe('a comparison', () => {
    let dir

    beforeAll(() => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), 'accumulator-scan-spec-'))
    })

    afterAll(() => {
      fs.rmSync(dir, { recursive: true, force: true })
    })

    test('runs this tree, the baseline and a control copy of it, and keeps every observation', () => {
      const raw = path.join(dir, 'raw.jsonl')

      const summary = compare(
        parseArgs([...SMALL, '--runs', '3', '--baseline-tree', ROOT, '--raw', raw])
      )

      expect(Object.keys(summary.arms)).toEqual(['tree', 'baseline', 'baseline~AA'])
      Object.values(summary.arms).forEach((arm) => {
        expect(arm.wallMs.n).toBe(3)
        expect(arm.largestPayloadBytes.max).toBeGreaterThanOrEqual(200)
      })
      expect(summary.comparisons).toEqual([
        expect.objectContaining({ label: 'tree', against: 'baseline', over: expect.any(Boolean) }),
      ])
      expect(Object.keys(summary.control.percent)).toEqual(['wallMs', 'cpuMs'])
      const observations = fs.readFileSync(raw, 'utf8').trim().split('\n').map(JSON.parse)
      expect(observations).toHaveLength(12)
      expect(observations.filter(({ warmup }) => warmup)).toHaveLength(3)
    })

    test('limits the tree to 10 % above the baseline on the two timings, and the control on them', () => {
      const summarize = jest.fn(() => ({ comparisons: [] }))
      jest.isolateModules(() => {
        jest.doMock('../session', () => ({
          ...jest.requireActual('../session'),
          runSession: () => [],
          summarize,
        }))
        const scan = require('../accumulatorScan')
        scan.compare(
          scan.parseArgs([
            ...SMALL,
            '--runs',
            '3',
            '--baseline-tree',
            ROOT,
            '--raw',
            path.join(dir, 'y'),
          ])
        )
      })
      jest.dontMock('../session')

      expect(summarize).toHaveBeenCalledWith(
        [],
        expect.objectContaining({
          pairs: [['tree', 'baseline', '+10']],
          controlOn: ['wallMs', 'cpuMs'],
        })
      )
    })

    test('refuses runs that are not a multiple of its three arms even when the flags were not parsed', () => {
      const options = parseArgs([...SMALL, '--baseline-tree', ROOT, '--raw', path.join(dir, 'z')])

      expect(() => compare({ ...options, runs: 4 })).toThrow(
        '4 runs cannot put each of 3 arms in every position equally often'
      )
    })

    test('stops with the error of a run that fails', () => {
      const missing = path.join(dir, 'no-such-tree')

      expect(() =>
        compare(
          parseArgs([
            ...SMALL,
            '--runs',
            '3',
            '--baseline-tree',
            missing,
            '--raw',
            path.join(dir, 'x'),
          ])
        )
      ).toThrow(/the run on .*no-such-tree failed/)
    })
  })
})
