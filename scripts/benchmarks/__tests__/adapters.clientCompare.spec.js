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
        'compare-produce-throughput': ['produce-throughput', '200', 'none', '3', 'defaults'],
        'compare-produce-throughput-1k': ['produce-throughput', '1024', 'none', '3', 'defaults'],
        'compare-produce-throughput-lz4': ['produce-throughput', '200', 'lz4', '3', 'defaults'],
        'compare-produce-throughput-12p': ['produce-throughput', '200', 'none', '12', 'defaults'],
        'compare-consume-throughput': ['consume-throughput', '200', 'none', '3', 'defaults'],
        'compare-consume-throughput-same': ['consume-throughput', '200', 'none', '3', 'same'],
        'compare-consume-throughput-1k': ['consume-throughput', '1024', 'none', '3', 'defaults'],
        'compare-consume-throughput-lz4': ['consume-throughput', '200', 'lz4', '3', 'defaults'],
        'compare-consume-throughput-12p': ['consume-throughput', '200', 'none', '12', 'defaults'],
      })
    })

    test('the planned compare minutes fit the bench job timeout with 20 minutes left for setup', () => {
      const workflow = yaml.load(
        fs.readFileSync(path.join(ROOT, '.github', 'workflows', 'bench.yml'), 'utf8')
      )
      const minutes = compareRows.reduce((sum, row) => sum + adapter.plannedSeconds(row), 0) / 60

      expect(Math.round(minutes)).toBe(206)
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

      expect(adapter.plannedSeconds(produce)).toBe((15 + 5 + 30) * 5 * 2)
      expect(adapter.plannedSeconds(consume)).toBe((15 + 7e9 / 25e6 + 7e9 / 35e6) * 5 * 2)
      expect(adapter.plannedSeconds({ ...produce, arms: ['fork'] }, 1)).toBe(50)
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
