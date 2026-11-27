const fs = require('node:fs')
const path = require('node:path')
const yaml = require('js-yaml')
const adapter = require('../adapters/clientCompare')
const manifest = require('../manifest.json')
const { RequestBudget, encodedBound } = require('../../../src/producer/state/recordSize')
const { Types } = require('../../../src/protocol/message/compression')
const Record = require('../../../src/protocol/recordBatch/record/v0')

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

    test('the compare rows cover 50-byte to 1 MB records, 15 and 17 KiB, lz4, gzip, 1, 12 and 100 partitions and a same-settings consume row', () => {
      const produce = (size, compression = 'none', partitions = '3') => [
        'produce-throughput',
        size,
        compression,
        partitions,
        'defaults',
      ]
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
        'compare-produce-throughput-100k': produce('102400'),
        'compare-produce-throughput-1m': produce('1000000'),
        'compare-produce-throughput-15k': produce('15360'),
        'compare-produce-throughput-17k': produce('17408'),
        'compare-produce-throughput-12p-keyed': produce('200', 'none', '12'),
        'compare-produce-throughput-string-200': produce('200'),
        'compare-produce-throughput-string-1k': produce('1024'),
        'compare-produce-throughput-100k-gzip': produce('102400', 'gzip'),
        'compare-produce-throughput-100k-lz4': produce('102400', 'lz4'),
        'compare-produce-throughput-coalesced': produce('200', 'none', '1'),
        'compare-produce-throughput-fanout-100p': produce('50', 'none', '100'),
        'compare-produce-throughput-custom': produce('200', 'none', '12'),
        'compare-produce-throughput-headers': produce('200'),
        'compare-produce-throughput-linger0': produce('200'),
        'compare-produce-throughput-12p-linger0': produce('200', 'none', '12'),
        'compare-produce-throughput-12p-keyed-linger0': produce('200', 'none', '12'),
        'compare-produce-throughput-fanout-100p-linger0': produce('50', 'none', '100'),
        'compare-produce-throughput-custom-linger0': produce('200', 'none', '12'),
        'compare-produce-throughput-coalesced-linger0': produce('200', 'none', '1'),
        'compare-produce-throughput-12p-keyed-linger0-plain': produce('200', 'none', '12'),
        'compare-produce-throughput-fanout-100p-linger0-plain': produce('50', 'none', '100'),
        'compare-produce-throughput-custom-linger0-plain': produce('200', 'none', '12'),
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

    test('the new produce rows set the flags their shape needs and no other', () => {
      const shapeFlags = [
        '--batch',
        '--unawaited',
        '--keys',
        '--headers',
        '--partitioner',
        '--value-type',
      ]
      const extras = (name) => {
        const row = compareRows.find((candidate) => candidate.name === name)
        return Object.fromEntries(
          shapeFlags
            .filter((flag) => valueOf(row.command, flag) !== undefined)
            .map((flag) => [flag, valueOf(row.command, flag)])
        )
      }
      const names = compareRows
        .map(({ name }) => name)
        .filter((name) => name.startsWith('compare-produce-throughput'))

      expect(Object.fromEntries(names.map((name) => [name, extras(name)]))).toEqual({
        'compare-produce-throughput': {},
        'compare-produce-throughput-same': {},
        'compare-produce-throughput-1k': {},
        'compare-produce-throughput-lz4': {},
        'compare-produce-throughput-12p': {},
        'compare-produce-throughput-100k': { '--batch': '8' },
        'compare-produce-throughput-1m': { '--batch': '1' },
        'compare-produce-throughput-15k': { '--batch': '32' },
        'compare-produce-throughput-17k': { '--batch': '32' },
        'compare-produce-throughput-12p-keyed': { '--keys': 'index' },
        'compare-produce-throughput-string-200': { '--value-type': 'string' },
        'compare-produce-throughput-string-1k': { '--value-type': 'string' },
        'compare-produce-throughput-100k-gzip': { '--batch': '8' },
        'compare-produce-throughput-100k-lz4': { '--batch': '8' },
        'compare-produce-throughput-coalesced': { '--unawaited': '100' },
        'compare-produce-throughput-fanout-100p': { '--keys': 'index' },
        'compare-produce-throughput-custom': { '--partitioner': 'custom' },
        'compare-produce-throughput-headers': { '--headers': '20' },
        'compare-produce-throughput-linger0': {},
        'compare-produce-throughput-12p-linger0': {},
        'compare-produce-throughput-12p-keyed-linger0': { '--keys': 'index' },
        'compare-produce-throughput-fanout-100p-linger0': { '--keys': 'index' },
        'compare-produce-throughput-custom-linger0': { '--partitioner': 'custom' },
        'compare-produce-throughput-coalesced-linger0': { '--unawaited': '100' },
        'compare-produce-throughput-12p-keyed-linger0-plain': { '--keys': 'index' },
        'compare-produce-throughput-fanout-100p-linger0-plain': { '--keys': 'index' },
        'compare-produce-throughput-custom-linger0-plain': { '--partitioner': 'custom' },
      })
    })

    test("a send of every produce-throughput row, with records at their real encoded size at the longest key and all their headers, fits one request under the producer's request budget (framing, topic name, a batch header and partition framing for every partition it can reach, the built-in codec's worst case) at the default 1 MiB limit", () => {
      const produceRows = compareRows.filter(
        (row) => valueOf(row.command, '--workload') === 'produce-throughput'
      )
      const codecs = { lz4: Types.LZ4, gzip: Types.GZIP }
      const budget = new RequestBudget({
        maxRequestSize: 1048576,
        clientId: 'bench-compare-produce-throughput',
      })
      const topic = `bench-compare-produce-throughput-${process.pid}-${Date.now()}`
      const planned = {}

      expect(produceRows.length).toBeGreaterThan(0)
      expect(encodedBound(Types.GZIP, 1000)).toBeGreaterThan(1000)
      expect(encodedBound(Types.LZ4, 1000)).toBeGreaterThan(1000)
      for (const row of produceRows) {
        const size = Number(valueOf(row.command, '--size') ?? 200)
        const headerCount = Number(valueOf(row.command, '--headers') ?? 0)
        const partitions = Number(valueOf(row.command, '--partitions') ?? 3)
        const perSend = Number(
          valueOf(row.command, '--unawaited') ?? valueOf(row.command, '--batch') ?? 500
        )
        const headers = Object.fromEntries(
          Array.from({ length: headerCount }, (_, i) => [`h${i}`, `v${i}`])
        )
        const record = Record({
          offsetDelta: perSend - 1,
          timestampDelta: 1e9,
          key: valueOf(row.command, '--keys') ? Buffer.from('9999999') : null,
          value: Buffer.alloc(size, 0x78),
          headers,
        })
        const spread = Math.min(partitions, perSend)
        const wire = Math.ceil(perSend / spread) * record.buffer.length
        const compression = codecs[valueOf(row.command, '--compression')]
        const batch = budget.batchBytes({ compression, messages: [], wire })
        planned[row.name] = budget.topicBytes(topic) + spread * batch <= budget.maxRequestBytes
      }

      expect(planned).toEqual(Object.fromEntries(produceRows.map(({ name }) => [name, true])))
    })

    test('the 15 KiB and 17 KiB rows put their values 1 KiB under and over 16 KiB', () => {
      const sizeOf = (name) =>
        Number(valueOf(compareRows.find((row) => row.name === name).command, '--size'))

      expect(sizeOf('compare-produce-throughput-15k')).toBe(16384 - 1024)
      expect(sizeOf('compare-produce-throughput-17k')).toBe(16384 + 1024)
    })

    test('every produce-throughput row measures rate, CPU and window in its five runs, and the send-call, memory and garbage-collection metrics in one extra run with --probe on that no command carries', () => {
      const produceRows = compareRows.filter(
        (row) => valueOf(row.command, '--workload') === 'produce-throughput'
      )

      expect(produceRows.length).toBeGreaterThan(0)
      for (const row of produceRows) {
        expect([row.name, row.metrics, row.command.includes('--probe')]).toEqual([
          row.name,
          [
            { key: 'messagesPerSecond', unit: 'msg/s' },
            { key: 'cpuMicrosPerMessage', unit: 'us' },
            { key: 'elapsedMs', unit: 'ms' },
          ],
          false,
        ])
        expect([row.name, row.diagnostic]).toEqual([
          row.name,
          {
            args: ['--probe', 'on'],
            metrics: [
              { key: 'sendCallMicrosP50', unit: 'us' },
              { key: 'sendCallMicrosP99', unit: 'us' },
              { key: 'peakRssBytes', unit: 'bytes' },
              { key: 'peakArrayBuffers', unit: 'bytes' },
              { key: 'gcCountPer10kMessages', unit: 'collections' },
              { key: 'gcPauseMsPer10kMessages', unit: 'ms' },
              { key: 'produceRequestsPerRoundP50', unit: 'requests' },
              { key: 'produceRequestsPerRoundMax', unit: 'requests' },
            ],
          },
        ])
      }
      for (const row of compareRows.filter((candidate) => !produceRows.includes(candidate))) {
        expect([row.name, 'diagnostic' in row]).toEqual([row.name, false])
      }
    })

    test('the five produce rows that ran before the record-handling rows keep their commands and their three metrics', () => {
      const byName = (name) => compareRows.find((row) => row.name === name)
      const base = ['scripts/benchmarks/clientCompare.js', '--client', '{client}', '--package']
      const common = [...base, '{package}', '--workload', 'produce-throughput']

      expect(byName('compare-produce-throughput').command).toEqual([
        ...common,
        '--duration-ms',
        '30000',
      ])
      expect(byName('compare-produce-throughput-same').command).toEqual([
        ...common,
        '--duration-ms',
        '30000',
        '--settings',
        'same',
      ])
      expect(byName('compare-produce-throughput-1k').command).toEqual([
        ...common,
        '--duration-ms',
        '30000',
        '--size',
        '1024',
      ])
      expect(byName('compare-produce-throughput-lz4').command).toEqual([
        ...common,
        '--duration-ms',
        '30000',
        '--compression',
        'lz4',
      ])
      expect(byName('compare-produce-throughput-12p').command).toEqual([
        ...common,
        '--duration-ms',
        '30000',
        '--partitions',
        '12',
      ])
      for (const name of [
        'compare-produce-throughput',
        'compare-produce-throughput-same',
        'compare-produce-throughput-1k',
        'compare-produce-throughput-lz4',
        'compare-produce-throughput-12p',
      ]) {
        expect([name, byName(name).metrics.map(({ key }) => key)]).toEqual([
          name,
          ['messagesPerSecond', 'cpuMicrosPerMessage', 'elapsedMs'],
        ])
      }
    })

    test('the rows that measure the producer record handling run only when named, and no other compare row does', () => {
      expect(compareRows.filter((row) => row.namedOnly).map(({ name }) => name)).toEqual([
        'compare-produce-throughput-100k',
        'compare-produce-throughput-1m',
        'compare-produce-throughput-15k',
        'compare-produce-throughput-17k',
        'compare-produce-throughput-12p-keyed',
        'compare-produce-throughput-string-200',
        'compare-produce-throughput-string-1k',
        'compare-produce-throughput-100k-gzip',
        'compare-produce-throughput-100k-lz4',
        'compare-produce-throughput-coalesced',
        'compare-produce-throughput-fanout-100p',
        'compare-produce-throughput-custom',
        'compare-produce-throughput-headers',
        'compare-produce-throughput-linger0',
        'compare-produce-throughput-12p-linger0',
        'compare-produce-throughput-12p-keyed-linger0',
        'compare-produce-throughput-fanout-100p-linger0',
        'compare-produce-throughput-custom-linger0',
        'compare-produce-throughput-coalesced-linger0',
        'compare-produce-throughput-12p-keyed-linger0-plain',
        'compare-produce-throughput-fanout-100p-linger0-plain',
        'compare-produce-throughput-custom-linger0-plain',
      ])
      expect(compareRows.filter((row) => 'namedOnly' in row && row.namedOnly !== true)).toEqual([])
    })

    test('each -linger0 row is its twin row with --fork-linger 0 added and run only when named, nothing else changed, and each -plain row is the -linger0 row with --fork-idempotent false added', () => {
      const byName = (name) => compareRows.find((row) => row.name === name)
      const lingerTwins = {
        'compare-produce-throughput-linger0': 'compare-produce-throughput',
        'compare-produce-throughput-12p-linger0': 'compare-produce-throughput-12p',
        'compare-produce-throughput-12p-keyed-linger0': 'compare-produce-throughput-12p-keyed',
        'compare-produce-throughput-fanout-100p-linger0': 'compare-produce-throughput-fanout-100p',
        'compare-produce-throughput-custom-linger0': 'compare-produce-throughput-custom',
        'compare-produce-throughput-coalesced-linger0': 'compare-produce-throughput-coalesced',
      }
      const plainTwins = {
        'compare-produce-throughput-12p-keyed-linger0-plain':
          'compare-produce-throughput-12p-keyed-linger0',
        'compare-produce-throughput-fanout-100p-linger0-plain':
          'compare-produce-throughput-fanout-100p-linger0',
        'compare-produce-throughput-custom-linger0-plain':
          'compare-produce-throughput-custom-linger0',
      }

      for (const [name, twin] of Object.entries(lingerTwins)) {
        const original = byName(twin)
        expect([name, byName(name)]).toEqual([
          name,
          {
            ...original,
            name,
            command: [...original.command, '--fork-linger', '0'],
            namedOnly: true,
          },
        ])
      }
      for (const [name, twin] of Object.entries(plainTwins)) {
        const original = byName(twin)
        expect([name, byName(name)]).toEqual([
          name,
          { ...original, name, command: [...original.command, '--fork-idempotent', 'false'] },
        ])
      }
    })

    test('only the nine twin rows set the fork-only flags: --fork-linger 0 on all nine, --fork-idempotent false on the three -plain rows', () => {
      const withFlag = (flag) =>
        compareRows
          .filter((row) => row.command.includes(flag))
          .map((row) => [row.name, valueOf(row.command, flag)])

      expect(withFlag('--fork-linger')).toEqual(
        [
          'compare-produce-throughput-linger0',
          'compare-produce-throughput-12p-linger0',
          'compare-produce-throughput-12p-keyed-linger0',
          'compare-produce-throughput-fanout-100p-linger0',
          'compare-produce-throughput-custom-linger0',
          'compare-produce-throughput-coalesced-linger0',
          'compare-produce-throughput-12p-keyed-linger0-plain',
          'compare-produce-throughput-fanout-100p-linger0-plain',
          'compare-produce-throughput-custom-linger0-plain',
        ].map((name) => [name, '0'])
      )
      expect(withFlag('--fork-idempotent')).toEqual(
        [
          'compare-produce-throughput-12p-keyed-linger0-plain',
          'compare-produce-throughput-fanout-100p-linger0-plain',
          'compare-produce-throughput-custom-linger0-plain',
        ].map((name) => [name, 'false'])
      )
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

    test('the planned compare minutes of the rows that run by default, the extra diagnostic runs included, leave a 20-minute planning allowance below the bench job timeout', () => {
      // Planning headroom from the planning costs below, not a measurement of setup time.
      const workflow = yaml.load(
        fs.readFileSync(path.join(ROOT, '.github', 'workflows', 'bench.yml'), 'utf8')
      )
      const minutesOf = (rows) =>
        rows.reduce((sum, row) => sum + adapter.plannedSeconds(row), 0) / 60
      const limit = workflow.jobs.bench['timeout-minutes']

      expect(Math.round(minutesOf(compareRows.filter((row) => !row.namedOnly)))).toBe(215)
      expect(minutesOf(compareRows.filter((row) => !row.namedOnly)) + 20).toBeLessThanOrEqual(limit)
    })

    test('the named-only compare rows, the extra diagnostic runs included, leave a 20-minute planning allowance together, and with the default compare rows they would not', () => {
      // Planning headroom from the planning costs below, not a measurement of setup time.
      const workflow = yaml.load(
        fs.readFileSync(path.join(ROOT, '.github', 'workflows', 'bench.yml'), 'utf8')
      )
      const minutesOf = (rows) =>
        rows.reduce((sum, row) => sum + adapter.plannedSeconds(row), 0) / 60
      const limit = workflow.jobs.bench['timeout-minutes']

      expect(Math.round(minutesOf(compareRows.filter((row) => row.namedOnly)))).toBe(163)
      expect(minutesOf(compareRows.filter((row) => row.namedOnly)) + 20).toBeLessThanOrEqual(limit)
      expect(minutesOf(compareRows) + 20).toBeGreaterThan(limit)
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

    test('counts one more run per arm for a diagnostic run, on top of a profile run', () => {
      const command = ['--duration-ms', '30000']
      const diagnostic = { args: ['--probe', 'on'], metrics: [] }

      expect(adapter.plannedSeconds({ command, arms: ARMS, diagnostic })).toBe(37 * 6 * 2)
      expect(adapter.plannedSeconds({ command, arms: ARMS, profile: true, diagnostic })).toBe(
        37 * 7 * 2
      )
    })
  })

  describe('diagnostic rows', () => {
    const probed = {
      ...entry,
      command: ['scripts/benchmarks/clientCompare.js', '--workload', 'produce-throughput'],
      diagnostic: {
        args: ['--probe', 'on'],
        metrics: [
          { key: 'sendCallMicrosP50', unit: 'us' },
          { key: 'peakRssBytes', unit: 'bytes' },
        ],
      },
    }
    const outputs = [output(100, 30000), output(110, 31000), output(90, 32000)]
    const probe = (elapsedMs = 30500, extra = { sendCallMicrosP50: 12.5, peakRssBytes: 4e8 }) =>
      output(50, elapsedMs, extra)
    const view = ({ rows }) =>
      rows.map(({ metric, status, runs, median, min, max, reason }) => ({
        metric,
        status,
        runs,
        median,
        min,
        max,
        reason,
      }))
    const single = (metric, value) => ({
      metric,
      status: 'valid',
      runs: 1,
      median: value,
      min: value,
      max: value,
    })
    const diagnosticOf = (result) =>
      view(result)
        .slice(2)
        .map(({ metric, status, reason }) => [metric, status, reason])
    const medianOf = (result) =>
      view(result)
        .slice(0, 2)
        .map(({ status, median }) => [status, median])

    test('an arm reports its diagnostic metrics from the one extra run beside the medians of its runs, keeps them out of the spread table and says where they come from', () => {
      const result = adapter.rows(probed, 'fork', { outputs, diagnostic: { output: probe() } }, 3)

      expect(view(result)).toEqual([
        { metric: 'messagesPerSecond', status: 'valid', runs: 3, median: 100, min: 90, max: 110 },
        { metric: 'elapsedMs', status: 'valid', runs: 3, median: 31000, min: 30000, max: 32000 },
        single('sendCallMicrosP50', 12.5),
        single('peakRssBytes', 4e8),
      ])
      expect(result.detail.status).toBe('valid')
      expect(result.detail.table.rows.map(([metric]) => metric)).toEqual([
        'messagesPerSecond',
        'elapsedMs',
      ])
      expect(result.detail.notes).toContain(
        'sendCallMicrosP50, peakRssBytes: from one extra run with --probe on, not in the medians; the 3 runs above do not measure them'
      )
    })

    test('a diagnostic run that failed, reported nothing or measured under 30 seconds changes no median: its rows are error or inconclusive and the arm says so', () => {
      const failed = adapter.rows(
        probed,
        'fork',
        { outputs, diagnostic: { error: 'diagnostic run: exit 1: boom' } },
        3
      )
      const silent = adapter.rows(
        probed,
        'fork',
        { outputs, diagnostic: { output: probe(30500, { sendCallMicrosP50: 12.5 }) } },
        3
      )
      const short = adapter.rows(
        probed,
        'fork',
        { outputs, diagnostic: { output: probe(29000) } },
        3
      )
      const arm = adapter.rows(
        probed,
        'fork',
        { outputs: [output(100, 30000)], error: 'run 2 of 3: exit 1: boom' },
        3
      )

      for (const result of [failed, silent, short]) {
        expect(medianOf(result)).toEqual([
          ['valid', 100],
          ['valid', 31000],
        ])
      }
      const boom = 'diagnostic run: exit 1: boom'
      expect(diagnosticOf(failed)).toEqual([
        ['sendCallMicrosP50', 'error', boom],
        ['peakRssBytes', 'error', boom],
      ])
      expect(failed.detail).toMatchObject({ status: 'error', reason: boom })
      expect(diagnosticOf(silent)).toEqual([
        ['sendCallMicrosP50', 'valid', undefined],
        ['peakRssBytes', 'error', 'run 1 of 1: peakRssBytes is not a finite number'],
      ])
      expect(silent.detail.status).toBe('error')
      const window = 'diagnostic window 29000 ms is under 30000 ms'
      expect(diagnosticOf(short)).toEqual([
        ['sendCallMicrosP50', 'inconclusive', window],
        ['peakRssBytes', 'inconclusive', window],
      ])
      expect(short.detail.status).toBe('valid')
      const skipped = 'no diagnostic run: run 2 of 3: exit 1: boom'
      expect(diagnosticOf(arm)).toEqual([
        ['sendCallMicrosP50', 'error', skipped],
        ['peakRssBytes', 'error', skipped],
      ])
    })

    test('an entry without diagnostic has no diagnostic rows and no note about them', () => {
      const { rows, detail } = adapter.rows(entry, 'fork', { outputs }, 3)

      expect(rows.map(({ metric }) => metric)).toEqual(['messagesPerSecond', 'elapsedMs'])
      expect(detail.notes.some((note) => note.includes('extra run'))).toBe(false)
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

    test('a produce arm notes its workload shape and a built-in gzip codec', () => {
      const produce = {
        ...entry,
        command: ['scripts/benchmarks/clientCompare.js', '--workload', 'produce-throughput'],
      }
      const extra = {
        codec: { name: 'gzip', codec: 'built-in', package: 'zlib', version: '1.3.1' },
        shape: {
          batch: 8,
          keys: 'index',
          headers: 20,
          partitioner: 'custom',
          valueType: 'string',
        },
      }

      const { detail } = adapter.rows(produce, 'fork', { outputs: [output(1, 30000, extra)] }, 1)

      expect(detail.notes.slice(1)).toEqual([
        "gzip: zlib@1.3.1, the package's built-in codec",
        'workload shape (the same on both arms): batch=8, keys="index", headers=20, partitioner="custom", valueType="string"',
      ])
    })

    test('an arm notes the overrides it applied, and an arm that ignored the fork-only overrides of its row says so', () => {
      const produce = {
        ...entry,
        command: ['scripts/benchmarks/clientCompare.js', '--workload', 'produce-throughput'],
      }
      const overrides = { linger: 0, idempotent: false }
      const forked = output(1, 30000, { settings: { defaults: { linger: 5 }, overrides } })
      const baseline = output(1, 30000, {
        settings: { defaults: { linger: 'none' }, ignoredOverrides: overrides },
      })

      const fork = adapter.rows(produce, 'fork', { outputs: [forked] }, 1)
      const old = adapter.rows(produce, 'kafkajs@2.2.4', { outputs: [baseline] }, 1)

      expect(fork.detail.notes.slice(1)).toEqual([
        "producer settings in force (this package's defaults): linger=5",
        'producer overrides in force on this arm only, each replacing the default of the same name: linger=0, idempotent=false',
      ])
      expect(old.detail.notes.slice(1)).toEqual([
        'producer settings in force (this package\'s defaults): linger="none"',
        "this arm ran with its own defaults: the row's fork-only overrides (linger=0, idempotent=false) do not apply to it",
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
