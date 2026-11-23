const fs = require('node:fs')
const path = require('node:path')
const yaml = require('js-yaml')
const adapter = require('../adapters/clientCompare')
const manifest = require('../manifest.json')

const ROOT = path.join(__dirname, '..', '..', '..')
const ARMS = ['fork', 'kafkajs@2.2.4']
const CONSUME_METRICS = [
  'messagesPerSecond',
  'cpuMicrosPerMessage',
  'mainThreadCpuMicrosPerMessage',
  'fullGcCount',
  'eventLoopDelayP50',
  'eventLoopDelayP99',
  'eventLoopDelayMax',
  'fetchRequests',
  'bytesPerFetch',
  'messagesPerFetch',
  'elapsedMs',
]
const compareRows = manifest.entries.filter((entry) => entry.name.startsWith('compare-'))
const valueOf = (command, flag) =>
  command.includes(flag) ? command[command.indexOf(flag) + 1] : undefined
const entry = {
  name: 'compare-x',
  command: ['scripts/benchmarks/clientCompare.js', '--workload', 'consume-throughput'],
  metrics: [
    { key: 'messagesPerSecond', unit: 'msg/s' },
    { key: 'elapsedMs', unit: 'ms' },
  ],
  arms: ARMS,
}
const output = (messagesPerSecond, elapsedMs, extra = {}) => ({
  messagesPerSecond,
  elapsedMs,
  ...extra,
})

describe('Benchmark report clientCompare adapter', () => {
  describe('manifest', () => {
    test('every compare row runs both arms through clientCompare.js with this adapter and a 30-second window or a corpus', () => {
      expect(compareRows.length).toBeGreaterThan(0)
      for (const row of compareRows) {
        const workload = valueOf(row.command, '--workload')

        expect([row.name, row.adapter, row.arms, row.command[0]]).toEqual([
          row.name,
          'clientCompare',
          ARMS,
          'scripts/benchmarks/clientCompare.js',
        ])
        if (workload === 'consume-throughput') {
          expect([row.name, Number(valueOf(row.command, '--messages')) >= 1e6]).toEqual([
            row.name,
            true,
          ])
          expect(row.metrics.map(({ key }) => key)).toEqual(CONSUME_METRICS)
        } else {
          expect([row.name, Number(valueOf(row.command, '--duration-ms'))]).toEqual([
            row.name,
            30000,
          ])
          expect([row.name, valueOf(row.command, '--messages')]).toEqual([row.name, undefined])
        }
      }
    })

    test('the compare rows cover 200-byte and 1 KiB records, lz4, 12 partitions and a same-settings consume row', () => {
      const shape = (row) => [
        valueOf(row.command, '--workload'),
        valueOf(row.command, '--size') ?? '200',
        valueOf(row.command, '--compression') ?? 'none',
        valueOf(row.command, '--partitions') ?? '3',
        valueOf(row.command, '--settings') ?? 'defaults',
      ]

      expect(Object.fromEntries(compareRows.map((row) => [row.name, shape(row)]))).toEqual({
        'compare-produce-latency': ['produce-latency', '200', 'none', '3', 'defaults'],
        'compare-produce-latency-same': ['produce-latency', '200', 'none', '3', 'same'],
        'compare-produce-throughput': ['produce-throughput', '200', 'none', '3', 'defaults'],
        'compare-produce-throughput-same': ['produce-throughput', '200', 'none', '3', 'same'],
        'compare-produce-throughput-1k': ['produce-throughput', '1024', 'none', '3', 'defaults'],
        'compare-produce-throughput-lz4': ['produce-throughput', '200', 'lz4', '3', 'defaults'],
        'compare-produce-throughput-12p': ['produce-throughput', '200', 'none', '12', 'defaults'],
        'compare-consume-throughput': ['consume-throughput', '200', 'none', '3', 'defaults'],
        'compare-consume-throughput-same': ['consume-throughput', '200', 'none', '3', 'same'],
        'compare-consume-throughput-1k': ['consume-throughput', '1024', 'none', '3', 'defaults'],
        'compare-consume-throughput-1k-same': ['consume-throughput', '1024', 'none', '3', 'same'],
        'compare-consume-throughput-lz4': ['consume-throughput', '200', 'lz4', '3', 'defaults'],
        'compare-consume-throughput-12p': ['consume-throughput', '200', 'none', '12', 'defaults'],
      })
    })

    test('each consume corpus gives a client reading 1,000,000 200-byte records per second, or twice the fastest 1 KiB rate of the fork, at least 35 seconds of measured work', () => {
      // The fork reads 200-byte records at about 670,000 per second, so those rows allow
      // 1,000,000; the 1 KiB rows allow twice the fork's fastest 1 KiB rate (226,457).
      const fastest = {
        'compare-consume-throughput': 1e6,
        'compare-consume-throughput-same': 1e6,
        'compare-consume-throughput-1k': 2 * 226457,
        'compare-consume-throughput-1k-same': 2 * 226457,
        'compare-consume-throughput-lz4': 1e6,
        'compare-consume-throughput-12p': 1e6,
      }
      const consumeRows = compareRows.filter(
        (row) => valueOf(row.command, '--workload') === 'consume-throughput'
      )

      expect(consumeRows.map(({ name }) => name).sort()).toEqual(Object.keys(fastest).sort())
      for (const row of consumeRows) {
        const records = Number(valueOf(row.command, '--messages'))
        const passes = Number(valueOf(row.command, '--passes') ?? 1)
        const measured = records * passes - Math.floor(records * 0.1)
        expect([row.name, measured / fastest[row.name] >= 35]).toEqual([row.name, true])
        expect([
          row.name,
          records * Number(valueOf(row.command, '--size') ?? 200) <= 4.5e9,
        ]).toEqual([row.name, true])
      }
    })

    test('the same-settings produce and consume rows and both 1 KiB consume rows take one CPU profile per arm, and no other row does', () => {
      expect(compareRows.filter((row) => row.profile).map(({ name }) => name)).toEqual([
        'compare-produce-throughput-same',
        'compare-consume-throughput-same',
        'compare-consume-throughput-1k',
        'compare-consume-throughput-1k-same',
      ])
      expect(compareRows.filter((row) => 'profile' in row && row.profile !== true)).toEqual([])
    })

    test('the 1 KiB same-settings consume row is the 1 KiB consume row with --settings same', () => {
      const byName = (name) => compareRows.find((row) => row.name === name)
      const defaults = byName('compare-consume-throughput-1k')
      const same = byName('compare-consume-throughput-1k-same')

      expect(same).toEqual({
        ...defaults,
        name: 'compare-consume-throughput-1k-same',
        command: [...defaults.command, '--settings', 'same'],
      })
      expect(valueOf(defaults.command, '--settings')).toBeUndefined()
    })

    test('the planned compare minutes fit the bench job timeout with 20 minutes left for setup', () => {
      const workflow = yaml.load(
        fs.readFileSync(path.join(ROOT, '.github', 'workflows', 'bench.yml'), 'utf8')
      )
      const minutes = compareRows.reduce((sum, row) => sum + adapter.plannedSeconds(row), 0) / 60

      expect(Math.round(minutes)).toBe(208)
      expect(minutes + 20).toBeLessThanOrEqual(workflow.jobs.bench['timeout-minutes'])
    })
  })

  describe('plannedSeconds', () => {
    test('counts both arms, five runs, the warm-up and the corpus at the planning rates', () => {
      const produce = {
        command: ['--workload', 'produce-throughput', '--duration-ms', '30000'],
        arms: ARMS,
      }
      const consume = {
        command: ['--workload', 'consume-throughput', '--messages', '7000000', '--size', '1000'],
        arms: ARMS,
      }
      const corpus = (7e6 * (3.4 + 1000 * 0.0074)) / 1e6
      const read = (7e6 * (2.1 + 1000 * 0.0025)) / 1e6

      expect(adapter.plannedSeconds(produce)).toBe((2 + 5 + 30) * 5 * 2)
      expect(adapter.plannedSeconds(consume)).toBeCloseTo((2 + corpus + read) * 5 * 2, 6)
      expect(adapter.plannedSeconds({ ...produce, arms: ['fork'] }, 1)).toBe(37)
    })

    test('counts every pass over a corpus once it is produced, and one more run per arm for a profile', () => {
      const command = ['--workload', 'consume-throughput', '--messages', '1000000', '--passes', '4']
      const corpus = (1e6 * (3.4 + 200 * 0.0074)) / 1e6
      const read = (4e6 * (2.1 + 200 * 0.0025)) / 1e6

      expect(adapter.plannedSeconds({ command, arms: ARMS })).toBeCloseTo(
        (2 + corpus + read) * 5 * 2,
        6
      )
      expect(
        adapter.plannedSeconds({ command: ['--duration-ms', '30000'], arms: ARMS, profile: true })
      ).toBe(37 * 6 * 2)
    })
  })

  describe('spreadOf', () => {
    test('is the range over the median in percent, and undefined for a median of 0', () => {
      expect(adapter.spreadOf({ median: 200, min: 190, max: 230 })).toBe(20)
      expect(adapter.spreadOf({ median: 0, min: 0, max: 1 })).toBeUndefined()
    })
  })

  describe('rows', () => {
    test('runs five interleaved runs per arm', () => {
      expect([adapter.runs, adapter.interleave, adapter.MIN_WINDOW_MS]).toEqual([5, true, 30000])
    })

    test('an arm with a run under 30 seconds of measured work is inconclusive and names the run', () => {
      const outputs = [output(100, 31000), output(110, 29999.5), output(90, 30000)]

      const { rows, detail } = adapter.rows(entry, 'fork', { outputs }, 3)

      const reason = 'measured window under 30000 ms in run 2 (29999.5 ms)'
      expect(rows.map(({ metric, status, reason: why }) => [metric, status, why])).toEqual([
        ['messagesPerSecond', 'inconclusive', reason],
        ['elapsedMs', 'inconclusive', reason],
      ])
      expect(rows[0]).toMatchObject({ median: 100, min: 90, max: 110 })
      expect(detail.status).toBe('inconclusive')
      expect(detail.controls).toEqual([
        {
          arm: 'fork',
          name: 'measured window',
          passed: false,
          detail: 'under 30000 ms in run 2 (29999.5 ms)',
        },
      ])
    })

    test('an arm shows median, min, max and spread per metric, and its settings, codec and Fetch version', () => {
      const extra = {
        settings: { defaults: { checkCrcs: true, maxInFlightRequests: 5 } },
        codec: { codec: 'registered', package: 'lz4-napi', version: '2.10.0' },
        fetchVersion: 11,
      }
      const outputs = [output(200, 40000, extra), output(190, 30000), output(230, 35000)]

      const { rows, detail } = adapter.rows(entry, 'kafkajs@2.2.4', { outputs }, 3)

      expect(rows.map(({ status }) => status)).toEqual(['valid', 'valid'])
      expect(detail).toEqual({
        status: 'valid',
        controls: [
          {
            arm: 'kafkajs@2.2.4',
            name: 'measured window',
            passed: true,
            detail: 'every run measured at least 30000 ms after its warm-up',
          },
        ],
        notes: [
          "kafkajs@2.2.4: 3 of 3 runs, interleaved with the other arm; each run's warm-up is not measured",
          "consumer settings in force (this package's defaults): checkCrcs=true, maxInFlightRequests=5",
          'lz4: lz4-napi@2.10.0, a frame codec this benchmark registers through CompressionCodecs (2.2.4 has no built-in LZ4)',
          'Fetch version: 11',
        ],
        table: {
          caption:
            'kafkajs@2.2.4: median, minimum, maximum and spread ((max - min) / median) of each metric',
          header: ['metric', 'median', 'min', 'max', 'spread %', 'unit'],
          rows: [
            ['messagesPerSecond', '200', '190', '230', '20', 'msg/s'],
            ['elapsedMs', '35000', '30000', '40000', '28.57', 'ms'],
          ],
        },
      })
    })

    test('the same-settings and built-in codec notes say so', () => {
      const extra = {
        settings: { same: { checkCrcs: false } },
        codec: { codec: 'built-in', package: 'lz4-napi', version: '2.10.0' },
      }

      const { detail } = adapter.rows(entry, 'fork', { outputs: [output(1, 30000, extra)] }, 1)

      expect(detail.notes.slice(1)).toEqual([
        'consumer settings in force (the same explicit settings on both arms): checkCrcs=false',
        "lz4: lz4-napi@2.10.0, the package's built-in codec",
      ])
    })

    test('a produce arm notes its producer settings, a multi-pass corpus says so, and a profiled arm names its profile outside the medians', () => {
      const produce = {
        ...entry,
        command: ['scripts/benchmarks/clientCompare.js', '--workload', 'produce-throughput'],
      }
      const same = { same: { linger: 0, retry: { retries: 5 }, partitioner: 'DefaultPartitioner' } }
      const outcome = {
        outputs: [output(1, 30000, { settings: same })],
        profile: 'bench-raw/compare-x/fork-cpu-profile/',
      }

      const produced = adapter.rows(produce, 'fork', outcome, 1).detail.notes.slice(1)
      const consumed = adapter.rows(
        entry,
        'fork',
        { outputs: [output(1, 30000, { passes: 4 })] },
        1
      )

      expect(produced).toEqual([
        'producer settings in force (the same explicit settings on both arms): linger=0, retry={"retries":5}, partitioner="DefaultPartitioner"',
        'CPU profile (one extra run, not in the medians): bench-raw/compare-x/fork-cpu-profile/',
      ])
      expect(consumed.detail.notes.slice(1)).toEqual([
        'the corpus is read 4 times: each partition is sought back to its first offset after its last record',
      ])
    })

    test('a consume arm notes its corpus layout', () => {
      const corpus = { partitioning: 'record i to partition i % partitions', recordsPerSend: 500 }

      const { detail } = adapter.rows(entry, 'fork', { outputs: [output(1, 30000, { corpus })] }, 1)

      expect(detail.notes.slice(1)).toEqual([
        'corpus layout (the same on both arms, each written by the arm\'s own client): partitioning="record i to partition i % partitions", recordsPerSend=500',
        "each partition's record count was checked against the layout before measuring; the order of the batches inside a partition was not",
      ])
    })

    test('an arm whose every run says its Node has no process.threadCpuUsage leaves main-thread CPU out with a note instead of an error row', () => {
      const withThread = {
        ...entry,
        metrics: [
          { key: 'messagesPerSecond', unit: 'msg/s' },
          { key: 'mainThreadCpuMicrosPerMessage', unit: 'us' },
          { key: 'fullGcCount', unit: 'collections' },
        ],
      }
      const runs = [output(100, 30000, { fullGcCount: 4 }), output(120, 30000, { fullGcCount: 6 })]

      const absent = adapter.rows(
        withThread,
        'kafkajs@2.2.4',
        { outputs: runs.map((run) => ({ ...run, threadCpuUsageSupported: false })) },
        2
      )
      const present = adapter.rows(
        withThread,
        'fork',
        {
          outputs: runs.map((run, i) => ({
            ...run,
            threadCpuUsageSupported: true,
            mainThreadCpuMicrosPerMessage: 3 + i,
          })),
        },
        2
      )

      expect(absent.rows.map(({ metric, status }) => [metric, status])).toEqual([
        ['messagesPerSecond', 'valid'],
        ['fullGcCount', 'valid'],
      ])
      expect(absent.detail.status).toBe('valid')
      expect(absent.detail.notes).toContain(
        'mainThreadCpuMicrosPerMessage: not reported, the Node running this arm has no process.threadCpuUsage'
      )
      expect(present.rows.map(({ metric, status, median }) => [metric, status, median])).toEqual([
        ['messagesPerSecond', 'valid', 120],
        ['mainThreadCpuMicrosPerMessage', 'valid', 4],
        ['fullGcCount', 'valid', 6],
      ])
      expect(present.detail.notes.some((note) => note.includes('not reported'))).toBe(false)
    })

    test('a run that can measure main-thread CPU but omits it is an error row, and the arm is an error', () => {
      const withThread = {
        ...entry,
        metrics: [{ key: 'mainThreadCpuMicrosPerMessage', unit: 'us' }],
      }
      const supported = { threadCpuUsageSupported: true }
      const outputs = [
        output(1, 30000, { ...supported, mainThreadCpuMicrosPerMessage: 3 }),
        output(1, 30000, supported),
      ]

      const { rows, detail } = adapter.rows(withThread, 'fork', { outputs }, 2)

      const reason = 'run 2 of 2: mainThreadCpuMicrosPerMessage is not a finite number'
      expect(rows.map(({ status, reason: why }) => [status, why])).toEqual([['error', reason]])
      expect(detail).toMatchObject({ status: 'error', reason })
      expect(detail.notes.some((note) => note.includes('not reported'))).toBe(false)
    })

    test('runs that omit main-thread CPU without saying their Node lacks it are errors, not a left-out metric', () => {
      const withThread = {
        ...entry,
        metrics: [{ key: 'mainThreadCpuMicrosPerMessage', unit: 'us' }],
      }
      const outputs = [
        output(1, 30000),
        output(1, 30000, { threadCpuUsageSupported: false }),
        output(1, 30000),
      ]

      const { rows, detail } = adapter.rows(withThread, 'fork', { outputs }, 3)

      const reason = 'run 1 of 3: mainThreadCpuMicrosPerMessage is not a finite number'
      expect(rows.map(({ status, reason: why }) => [status, why])).toEqual([['error', reason]])
      expect(detail).toMatchObject({ status: 'error', reason })
    })

    test('an arm that failed is an error with its reason and no table', () => {
      const { rows, detail } = adapter.rows(
        entry,
        'fork',
        { outputs: [], error: 'run 1 of 5: boom' },
        5
      )

      expect(rows.map(({ status, reason }) => [status, reason])).toEqual([
        ['error', 'run 1 of 5: boom'],
        ['error', 'run 1 of 5: boom'],
      ])
      expect(detail).toMatchObject({ status: 'error', reason: 'run 1 of 5: boom' })
      expect(detail.controls[0].passed).toBe(false)
      expect(detail.table).toBeUndefined()
    })
  })
})
