const {
  ENDPOINT_BYTES,
  MIN_PHASE_SAMPLES,
  PHASE_WARMUP_MS,
  SAMPLE_MS,
  SLOPE_BYTES_PER_MINUTE,
  WARMUP_MS,
  evaluate,
  evaluatePhases,
  median,
  slope,
} = require('../bounds')
const {
  PROFILES,
  TELEMETRY,
  TelemetryResourceVerdict,
  WORKLOADS,
  Soak,
  createTopic,
  forgettingDestroyed,
  instrument,
  parseArgs,
  privateOf,
} = require('../soak')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../../src/consumer/__tests__/fixtures/fetchHarness')
const { PartitionAssigners } = require('../../../index')
const { startBroker, stopBroker } = require('../../../testHelpers/leaderElection')
const { Clients } = require('../../clients')

jest.mock('../../../testHelpers/leaderElection', () => ({
  stopBroker: jest.fn(),
  startBroker: jest.fn(),
}))

const MINUTE_MS = 60000
const MIB = 1024 * 1024

/** A quiet reading: nothing held, nothing open. */
const quiet = (overrides = {}) => ({
  listeners: { requestQueues: 0, sockets: 0 },
  sockets: { created: 4, destroyed: 4, open: 0 },
  queueDepth: 0,
  ledgerSize: 0,
  obligationSize: 0,
  activeResources: { Timeout: 1 },
  ...overrides,
})

/** `count` heap samples from `warmupMs` on, 30 s apart, from `heapAt(index)` bytes. */
const heapSeries = (count, heapAt, warmupMs = 0) =>
  Array.from({ length: count }, (_, index) => ({
    atMs: warmupMs + index * SAMPLE_MS,
    snapshot: quiet({ heapUsed: heapAt(index) }),
  }))

/** A run that holds every bound: a flat heap, quiet points, a closed teardown. */
const clean = (overrides = {}) => ({
  baseline: quiet(),
  phaseEnds: [{ phase: 'idle', snapshot: quiet() }],
  samples: heapSeries(12, () => 50 * MIB),
  teardown: quiet(),
  warmupMs: 0,
  ...overrides,
})

describe('Soak > bounds', () => {
  test('are the declared values of a short-run screen', () => {
    expect({ SAMPLE_MS, WARMUP_MS, ENDPOINT_BYTES, SLOPE_BYTES_PER_MINUTE }).toEqual({
      SAMPLE_MS: 30000,
      WARMUP_MS: 180000,
      ENDPOINT_BYTES: 2 * MIB,
      SLOPE_BYTES_PER_MINUTE: 256 * 1024,
    })
  })

  describe('the arithmetic', () => {
    test('median is the upper middle of an even count', () => {
      expect(median([5, 1, 3])).toBe(3)
      expect(median([4, 1, 3, 2])).toBe(3)
    })

    test('slope is the least-squares line through the points, in y per x', () => {
      expect(
        slope([
          [0, 10],
          [1, 12],
          [2, 14],
          [3, 16],
        ])
      ).toBeCloseTo(2, 12)
      expect(
        slope([
          [0, 7],
          [2, 7],
          [5, 7],
        ])
      ).toBe(0)
      expect(
        slope([
          [0, 1],
          [1, 5],
          [2, 4],
          [3, 8],
        ])
      ).toBeCloseTo(2, 12)
    })

    test('slope refuses points that do not span two x', () => {
      expect(() =>
        slope([
          [1, 1],
          [1, 2],
        ])
      ).toThrow('a slope needs at least two different x')
      expect(() => slope([[1, 1]])).toThrow('a slope needs at least two different x')
    })
  })

  describe('a run that holds every bound', () => {
    test('passes, and reports what it measured', () => {
      expect(evaluate(clean())).toEqual({
        ok: true,
        violations: [],
        measured: { heapSamples: 12, endpointBytes: 0, slopeBytesPerMinute: 0 },
      })
    })

    test('counts only the samples after the warm-up', () => {
      const samples = [
        ...heapSeries(3, () => 400 * MIB),
        ...heapSeries(8, () => 50 * MIB, WARMUP_MS),
      ]

      expect(evaluate(clean({ samples, warmupMs: WARMUP_MS })).measured).toMatchObject({
        heapSamples: 8,
        endpointBytes: 0,
      })
    })
  })

  describe('each bound catches what it is for', () => {
    const violations = (run) => evaluate(run).violations

    test('a listener left on a tracked emitter after a phase', () => {
      const phaseEnds = [
        { phase: 'member-churn', snapshot: quiet({ listeners: { requestQueues: 2, sockets: 0 } }) },
      ]

      expect(violations(clean({ phaseEnds }))).toEqual([
        'after member-churn: 2 listeners on requestQueues, 0 at the start',
      ])
    })

    test.each([
      ['queueDepth', { queueDepth: 1 }],
      ['ledgerSize', { ledgerSize: 2 }],
      ['obligationSize', { obligationSize: 3 }],
    ])('%s above 0 after a phase', (gauge, overrides) => {
      const phaseEnds = [{ phase: 'outage', snapshot: quiet(overrides) }]

      expect(violations(clean({ phaseEnds }))).toEqual([
        `after outage: ${gauge} is ${Object.values(overrides)[0]}, not 0`,
      ])
    })

    test('a socket still open after disconnect', () => {
      const teardown = quiet({ sockets: { created: 4, destroyed: 3, open: 1 } })

      expect(violations(clean({ teardown }))).toEqual(['after disconnect: 1 sockets still open'])
    })

    test.each(['TCPSocketWrap', 'Timeout'])('a %s that was not there at the start', (type) => {
      const teardown = quiet({
        activeResources: { Timeout: 1, [type]: (type === 'Timeout' ? 1 : 0) + 1 },
      })

      expect(violations(clean({ teardown }))).toEqual([
        `after disconnect: ${type === 'Timeout' ? 2 : 1} ${type}, ${type === 'Timeout' ? 1 : 0} at the start`,
      ])
    })

    test('a heap that grew more than 2 MiB from the first samples to the last', () => {
      const samples = heapSeries(12, (index) => 50 * MIB + (index >= 6 ? 3 * MIB : 0))

      expect(violations(clean({ samples }))).toContain(
        `the heap grew ${3 * MIB} bytes, more than ${2 * MIB}`
      )
    })

    test('a heap that climbs more than 256 KiB a minute while its ends are within 2 MiB', () => {
      const samples = heapSeries(8, (index) => 50 * MIB + Math.round((index * 300 * 1024) / 2))

      const run = evaluate(clean({ samples }))

      expect(run.measured.endpointBytes).toBeLessThan(2 * MIB)
      expect(run.measured.slopeBytesPerMinute).toBeCloseTo(300 * 1024, -2)
      expect(run.violations).toEqual([
        expect.stringMatching(/^the heap slope is 3\d{5} bytes a minute, more than 262144$/),
      ])
    })

    test('a heap series too short to judge, and a run with no heap readings at all', () => {
      expect(violations(clean({ samples: heapSeries(5, () => 50 * MIB) }))).toEqual([
        '5 heap samples after warm-up, 6 are needed',
      ])
      expect(
        violations(
          clean({
            samples: [
              { atMs: 0, snapshot: quiet() },
              { atMs: MINUTE_MS, snapshot: quiet() },
            ],
          })
        )
      ).toEqual(['0 heap samples after warm-up, 6 are needed'])
    })

    test('a socket still open at the end of a phase, though the teardown is clean', () => {
      const phaseEnds = [
        { phase: 'idle', snapshot: quiet({ sockets: { created: 4, destroyed: 3, open: 1 } }) },
      ]

      expect(violations(clean({ phaseEnds }))).toEqual(['after idle: 1 sockets still open'])
    })

    test.each(['TCPSocketWrap', 'Timeout'])(
      '%s left at the end of a phase, though the teardown is clean',
      (type) => {
        const now = type === 'Timeout' ? 2 : 1
        const phaseEnds = [
          { phase: 'outage', snapshot: quiet({ activeResources: { Timeout: 1, [type]: now } }) },
        ]

        expect(violations(clean({ phaseEnds }))).toEqual([
          `after outage: ${now} ${type}, ${now - 1} at the start`,
        ])
      }
    )

    test('a socket and a timer left at the end of a phase, with a flat heap and a clean teardown', () => {
      const phaseEnds = [
        {
          phase: 'idle',
          snapshot: quiet({
            sockets: { created: 1, destroyed: 0, open: 1 },
            activeResources: { TCPSocketWrap: 1, Timeout: 2 },
          }),
        },
      ]

      const run = evaluate(clean({ phaseEnds }))

      expect(run.ok).toBe(false)
      expect(run.violations).toHaveLength(3)
    })

    test('a tracked emitter with a listener left at the teardown', () => {
      const teardown = quiet({ listeners: { requestQueues: 1, sockets: 0 } })

      expect(violations(clean({ teardown }))).toEqual([
        'after disconnect: 1 listeners on requestQueues, 0 at the start',
      ])
    })

    test.each([
      ['queueDepth', { queueDepth: 1 }],
      ['ledgerSize', { ledgerSize: 1 }],
      ['obligationSize', { obligationSize: 1 }],
    ])('%s above 0 at the teardown', (gauge, overrides) => {
      expect(violations(clean({ teardown: quiet(overrides) }))).toEqual([
        `after disconnect: ${gauge} is 1, not 0`,
      ])
    })

    test('a later phase end is held to the bounds too, and named', () => {
      const phaseEnds = [
        { phase: 'idle', snapshot: quiet() },
        { phase: 'outage', snapshot: quiet({ sockets: { created: 4, destroyed: 3, open: 1 } }) },
      ]

      expect(violations(clean({ phaseEnds }))).toEqual(['after outage: 1 sockets still open'])
    })

    test('says every bound that broke, not the first', () => {
      const phaseEnds = [{ phase: 'idle', snapshot: quiet({ queueDepth: 4, ledgerSize: 1 }) }]
      const teardown = quiet({ sockets: { created: 2, destroyed: 1, open: 1 } })

      expect(violations(clean({ phaseEnds, teardown }))).toHaveLength(3)
    })
  })
})

/**
 * What a producer-churn run measured when its heap failed the whole-run rule: the samples by phase as
 * `[atMs, heapUsed]`, when each phase began on the run's clock, and the quiet points.
 */
const LIVE_RUN = {
  phases: [
    {
      phase: 'steady',
      beganMs: 0,
      samples: [
        [30343, 12913488],
        [60462, 12835392],
        [90580, 13094288],
        [120697, 13116416],
        [150814, 13071184],
        [180931, 13155824],
        [211047, 13115888],
        [241169, 13166112],
        [271285, 13109840],
      ],
      end: 12179872,
    },
    {
      phase: 'outage',
      beganMs: 301000,
      samples: [
        [331195, 13624080],
        [361349, 31481016],
        [391470, 14260560],
        [421587, 14253184],
        [451706, 14266248],
        [481826, 14196464],
        [511944, 14233552],
        [542064, 14308552],
        [572184, 14367752],
      ],
      end: 13258920,
    },
    {
      phase: 'buffer-pressure',
      beganMs: 601600,
      samples: [
        [631856, 20277656],
        [661977, 20263304],
        [692099, 20275504],
        [722227, 20296872],
        [752355, 20333792],
        [782479, 20383088],
        [812610, 20391560],
        [842737, 20417552],
        [872860, 20405016],
        [902980, 15752344],
      ],
      end: 13281504,
    },
  ],
  teardown: 13263984,
}

describe('Soak > bounds for the producer churn', () => {
  /** `count` samples of one phase, `everyMs` apart from `everyMs` into it, each `heapAt(index)` bytes. */
  const phaseSamples = (phase, count, heapAt, { beganMs = 0, everyMs = SAMPLE_MS } = {}) =>
    Array.from({ length: count }, (_, index) => ({
      phase,
      atMs: beganMs + (index + 1) * everyMs,
      phaseMs: (index + 1) * everyMs,
      snapshot: quiet({ heapUsed: heapAt(index) }),
    }))

  /** A run of three phases with nine samples each; `heap` and `end` override a phase's bytes by name. */
  const churnRun = ({
    phases = ['steady', 'outage', 'pressure'],
    heap = {},
    end = {},
    ...rest
  } = {}) => ({
    baseline: quiet(),
    phaseEnds: phases.map((phase) => ({
      phase,
      snapshot: quiet({ heapUsed: end[phase] ?? 50 * MIB }),
    })),
    samples: phases.flatMap((phase, order) =>
      phaseSamples(phase, 9, heap[phase] ?? (() => 50 * MIB), { beganMs: order * 300000 })
    ),
    teardown: quiet({ heapUsed: end.teardown ?? 50 * MIB }),
    ...rest,
  })

  const violations = (run) => evaluatePhases(run).violations

  test('a phase is warmed up for 60 s and keeps at least six samples', () => {
    expect({ PHASE_WARMUP_MS, MIN_PHASE_SAMPLES }).toEqual({
      PHASE_WARMUP_MS: 60000,
      MIN_PHASE_SAMPLES: 6,
    })
  })

  test('a run that holds every bound passes, and reports what each phase and quiet point measured', () => {
    expect(evaluatePhases(churnRun())).toEqual({
      ok: true,
      violations: [],
      measured: {
        phases: ['steady', 'outage', 'pressure'].map((phase) => ({
          phase,
          heapSamples: 8,
          endpointBytes: 0,
          slopeBytesPerMinute: 0,
        })),
        quietPoints: [
          { when: 'after outage', aboveFirstBytes: 0 },
          { when: 'after pressure', aboveFirstBytes: 0 },
          { when: 'after disconnect', aboveFirstBytes: 0 },
        ],
      },
    })
  })

  test('a high but flat phase passes where the series of the whole run does not', () => {
    const heap = { steady: () => 13 * MIB, outage: () => 14 * MIB, pressure: () => 20 * MIB }
    const end = { steady: 13 * MIB, outage: 13 * MIB, pressure: 13 * MIB, teardown: 13 * MIB }
    const run = churnRun({ heap, end })

    expect(violations(run)).toEqual([])
    expect(evaluate({ ...run, warmupMs: 0 }).violations).toEqual([
      `the heap grew ${7 * MIB} bytes, more than ${2 * MIB}`,
      expect.stringMatching(/^the heap slope is \d+ bytes a minute, more than 262144$/),
    ])
  })

  test('the readings of the live run that failed the whole-run rule pass it phase by phase', () => {
    const samples = LIVE_RUN.phases.flatMap(({ phase, beganMs, samples: readings }) =>
      readings.map(([atMs, heapUsed]) => ({
        phase,
        atMs,
        phaseMs: atMs - beganMs,
        snapshot: quiet({ heapUsed }),
      }))
    )
    const run = {
      baseline: quiet(),
      phaseEnds: LIVE_RUN.phases.map(({ phase, end }) => ({
        phase,
        snapshot: quiet({ heapUsed: end }),
      })),
      samples,
      teardown: quiet({ heapUsed: LIVE_RUN.teardown }),
    }

    expect(evaluate({ ...run, warmupMs: WARMUP_MS }).violations).toEqual([
      'the heap grew 7249192 bytes, more than 2097152',
      'the heap slope is 539287 bytes a minute, more than 262144',
    ])
    const judged = evaluatePhases(run)
    expect(judged.violations).toEqual([])
    expect(judged.measured.phases.map(({ heapSamples }) => heapSamples)).toEqual([8, 8, 9])
    expect(judged.measured.quietPoints.map(({ aboveFirstBytes }) => aboveFirstBytes)).toEqual([
      1079048, 1101632, 1084112,
    ])
  })

  test.each([0, 5000])(
    'a 300 s phase sampled every 30 s, each collection taking %i ms, keeps six samples after the warm-up',
    (collectionMs) => {
      const everyMs = SAMPLE_MS + collectionMs
      const samples = phaseSamples('steady', Math.floor(300000 / everyMs), () => 50 * MIB, {
        everyMs,
      })

      const run = evaluatePhases(churnRun({ phases: ['steady'], samples }))

      expect(run.violations).toEqual([])
      expect(run.measured.phases[0].heapSamples).toBeGreaterThanOrEqual(6)
    }
  )

  test('growth inside one phase fails and names it, though the heap is back at its quiet point', () => {
    const heap = { outage: (index) => 50 * MIB + index * MIB }

    expect(violations(churnRun({ heap }))).toEqual([
      `outage: the heap grew ${5 * MIB} bytes, more than ${2 * MIB}`,
      `outage: the heap slope is ${2 * MIB} bytes a minute, more than ${SLOPE_BYTES_PER_MINUTE}`,
    ])
  })

  test('a slope above the bound fails though the growth stays under its own', () => {
    const heap = { steady: (index) => 50 * MIB + index * 160 * 1024 }

    expect(violations(churnRun({ heap }))).toEqual([
      `steady: the heap slope is ${320 * 1024} bytes a minute, more than ${SLOPE_BYTES_PER_MINUTE}`,
    ])
  })

  test('a phase with fewer samples than it needs after its warm-up fails and names it', () => {
    const run = {
      ...churnRun({ phases: ['steady'] }),
      samples: phaseSamples('steady', 6, () => 50 * MIB),
    }

    expect(violations(run)).toEqual(['steady: 5 heap samples after warm-up, 6 are needed'])
    expect(violations({ ...run, minSamples: 2 })).toEqual([])
  })

  test('only the samples after the phase warm-up count', () => {
    const heap = { steady: (index) => (index < 2 ? 10 * MIB : 50 * MIB) }
    const run = churnRun({ phases: ['steady'], heap })

    expect(violations({ ...run, warmupMs: 90000 })).toEqual([])
    expect(violations({ ...run, warmupMs: 0 })).toContain(
      `steady: the heap grew ${40 * MIB} bytes, more than ${2 * MIB}`
    )
  })

  test('growth that survives into the quiet points fails, at each one, though every phase is flat', () => {
    const end = { steady: 50 * MIB, outage: 51 * MIB, pressure: 53 * MIB, teardown: 53 * MIB }

    expect(violations(churnRun({ end }))).toEqual([
      `after pressure: ${3 * MIB} bytes retained above after steady, more than ${2 * MIB}`,
      `after disconnect: ${3 * MIB} bytes retained above after steady, more than ${2 * MIB}`,
    ])
  })

  test('a quiet point with no heap reading fails, and a first one without it is named once', () => {
    const run = churnRun()
    const withoutHeap = (entry) => ({ ...entry, snapshot: quiet() })
    const [steady, outage, pressure] = run.phaseEnds

    expect(violations({ ...run, phaseEnds: [steady, withoutHeap(outage), pressure] })).toEqual([
      'after outage: no heap reading',
    ])
    expect(violations({ ...run, phaseEnds: [withoutHeap(steady), outage, pressure] })).toEqual([
      'after steady: no heap reading',
    ])
  })

  test('the quiet-point bounds of every run hold here too, and are named', () => {
    const run = churnRun()
    const [steady, ...later] = run.phaseEnds
    const open = {
      ...steady,
      snapshot: { ...steady.snapshot, sockets: { created: 1, destroyed: 0, open: 1 } },
    }

    expect(violations({ ...run, phaseEnds: [open, ...later] })).toEqual([
      'after steady: 1 sockets still open',
    ])
  })
})

describe('Soak > profiles', () => {
  test.each([
    [['--profile', 'idle'], { profile: 'idle', phaseSeconds: null }],
    [['--profile', 'first-run'], { profile: 'first-run', phaseSeconds: null }],
    [['--profile', 'outage', '--phase-seconds', '20'], { profile: 'outage', phaseSeconds: 20 }],
  ])('%j selects the profile, and may shorten its phases', (argv, parsed) => {
    expect(parseArgs(argv)).toEqual(parsed)
  })

  test.each([
    [[]],
    [['--profile']],
    [['--profile', 'nap']],
    [['idle']],
    [['--profile', 'idle', 'x']],
    [['--profile', 'idle', '--phase-seconds']],
    [['--profile', 'idle', '--phase-seconds', '0']],
    [['--profile', 'idle', '--phase-seconds', '1.5']],
    [['--profile', 'idle', '--phase-seconds', '5', 'x']],
  ])('%j is refused with the usage line', (argv) => {
    expect(() => parseArgs(argv)).toThrow(
      /^usage: node --expose-gc scripts\/soak\/soak\.js --profile .* \[--phase-seconds <n>\]$/
    )
  })

  test('the first run is 5 minutes each of idle, member churn and outage', () => {
    expect(PROFILES['first-run'].map(({ name, ms }) => [name, ms])).toEqual([
      ['idle', 300000],
      ['member-churn', 300000],
      ['outage', 300000],
    ])
  })

  describe('a profile of one phase', () => {
    const standalone = Object.entries(PROFILES)
      .filter(([name, phases]) => name !== 'telemetry' && phases.length === 1)
      .map(([name, [{ ms }]]) => [name, ms])

    test.each(standalone)('%s runs long enough for six samples after the warm-up', (_, ms) => {
      for (const collectionMs of [0, 5000]) {
        const samples = []
        for (let atMs = SAMPLE_MS + collectionMs; atMs <= ms; atMs += SAMPLE_MS + collectionMs) {
          samples.push({ atMs, snapshot: quiet({ heapUsed: 50 * MIB }) })
        }

        const run = evaluate({ baseline: quiet(), phaseEnds: [], samples, teardown: quiet() })

        expect(run.violations).toEqual([])
        expect(run.measured.heapSamples).toBeGreaterThanOrEqual(6)
      }
    })
  })

  test('member churn rebalances cooperatively', async () => {
    const configs = []
    const client = {
      connect: async () => {},
      subscribe: async () => {},
      run: async () => {},
      send: async () => {},
      disconnect: async () => {},
    }
    const kafka = {
      producer: () => client,
      consumer: (config) => {
        configs.push(config)
        return client
      },
    }

    await WORKLOADS.memberChurn({ kafka, topic: TOPIC, groupId: 'soak-churn' }, 0)

    expect(configs.map(({ partitionAssigners }) => partitionAssigners)).toEqual([
      [PartitionAssigners.cooperativeSticky],
      [PartitionAssigners.cooperativeSticky],
    ])
  })

  test('every phase names a workload that exists', () => {
    for (const phases of Object.values(PROFILES)) {
      for (const { workload } of phases) {
        expect(WORKLOADS[workload]).toEqual(expect.any(Function))
      }
    }
    expect(Object.keys(WORKLOADS)).toEqual(
      expect.arrayContaining(['producerSteady', 'producerOutage', 'producerPressure'])
    )
  })
})

describe('Soak > workloads disconnect every client they built', () => {
  const CALLS = [
    'connect',
    'subscribe',
    'run',
    'send',
    'createTopics',
    'deleteTopics',
    'disconnect',
  ]
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

  /**
   * A `kafka` of fake clients named `producer-1`, `consumer-2` and so on, in the order they are
   * built. Each call is logged; a call named in `failing` as `<client>.<call>` rejects with the
   * error `failures` holds for it.
   */
  const fakeKafka = (failing = []) => {
    const log = []
    const counts = {}
    const failures = new Map(failing.map((call) => [call, new Error(`${call} failed`)]))
    const build = (kind) => {
      counts[kind] = (counts[kind] || 0) + 1
      const name = `${kind}-${counts[kind]}`
      return Object.fromEntries(
        CALLS.map((call) => [
          call,
          async () => {
            log.push(`${name}.${call}`)
            if (failures.has(`${name}.${call}`)) {
              throw failures.get(`${name}.${call}`)
            }
          },
        ])
      )
    }

    return {
      kafka: {
        producer: () => build('producer'),
        consumer: () => build('consumer'),
        admin: () => build('admin'),
      },
      log,
      failures,
      disconnectOrder: () =>
        log.filter((entry) => entry.endsWith('.disconnect')).map((entry) => entry.split('.')[0]),
      disconnected: () =>
        log
          .filter((entry) => entry.endsWith('.disconnect'))
          .map((entry) => entry.split('.')[0])
          .sort(),
    }
  }
  const context = (kafka) => ({ kafka, topic: TOPIC, groupId: 'soak-fake' })

  test('an idle member whose send rejects', async () => {
    const { kafka, disconnected } = fakeKafka(['producer-1.send'])

    await expect(WORKLOADS.idle(context(kafka), 1000)).rejects.toThrow('producer-1.send failed')

    expect(disconnected()).toEqual(['consumer-1', 'producer-1'])
  })

  test('an idle member whose setup rejects half way', async () => {
    const { kafka, disconnected } = fakeKafka(['consumer-1.connect'])

    await expect(WORKLOADS.idle(context(kafka), 1000)).rejects.toThrow('consumer-1.connect failed')

    expect(disconnected()).toEqual(['consumer-1', 'producer-1'])
  })

  test('an idle member that ends well but whose disconnect rejects still disconnects the rest', async () => {
    const { kafka, disconnected } = fakeKafka(['consumer-1.disconnect'])

    await expect(WORKLOADS.idle(context(kafka), 0)).rejects.toThrow('consumer-1.disconnect failed')

    expect(disconnected()).toEqual(['consumer-1', 'producer-1'])
  })

  test('a failed workload keeps its own error when a disconnect rejects too', async () => {
    const { kafka, disconnected } = fakeKafka(['producer-1.send', 'consumer-1.disconnect'])

    await expect(WORKLOADS.idle(context(kafka), 1000)).rejects.toThrow('producer-1.send failed')

    expect(disconnected()).toEqual(['consumer-1', 'producer-1'])
  })

  test('member churn whose first member fails to start', async () => {
    const { kafka, disconnected } = fakeKafka(['consumer-1.run'])

    await expect(WORKLOADS.memberChurn(context(kafka), 1000)).rejects.toThrow(
      'consumer-1.run failed'
    )

    expect(disconnected()).toEqual(['consumer-1', 'producer-1'])
  })

  test('member churn whose replacement is half built disconnects each client once', async () => {
    const { kafka, disconnected } = fakeKafka(['consumer-3.subscribe'])

    await expect(WORKLOADS.memberChurn(context(kafka), 20)).rejects.toThrow(
      'consumer-3.subscribe failed'
    )

    expect(disconnected()).toEqual(['consumer-1', 'consumer-2', 'consumer-3', 'producer-1'])
  })

  test('a regex subscriber whose topic administration rejects', async () => {
    const { kafka, disconnected } = fakeKafka(['admin-1.createTopics'])

    await expect(WORKLOADS.regexTopicChurn(context(kafka), 1000)).rejects.toThrow(
      'admin-1.createTopics failed'
    )

    expect(disconnected()).toEqual(['admin-1', 'consumer-1', 'producer-1'])
  })

  test.each([
    [
      'an idle member whose producer connect rejects never starts the consumer connect',
      'idle',
      'producer-1.connect',
      ['consumer-1.connect'],
      ['consumer-1', 'producer-1'],
    ],
    [
      'an idle member whose consumer connect rejects after the producer connected',
      'idle',
      'consumer-1.connect',
      [],
      ['consumer-1', 'producer-1'],
    ],
    [
      'a regex subscriber whose admin connect rejects never starts the other connects',
      'regexTopicChurn',
      'admin-1.connect',
      ['producer-1.connect', 'consumer-1.connect'],
      ['consumer-1', 'producer-1', 'admin-1'],
    ],
    [
      'a regex subscriber whose producer connect rejects never starts the consumer connect',
      'regexTopicChurn',
      'producer-1.connect',
      ['consumer-1.connect'],
      ['consumer-1', 'producer-1', 'admin-1'],
    ],
    [
      'a regex subscriber whose consumer connect rejects after the others connected',
      'regexTopicChurn',
      'consumer-1.connect',
      [],
      ['consumer-1', 'producer-1', 'admin-1'],
    ],
  ])('%s', async (_, workload, failing, neverCalled, order) => {
    const { kafka, log, failures, disconnectOrder } = fakeKafka([failing])

    await expect(WORKLOADS[workload](context(kafka), 1000)).rejects.toBe(failures.get(failing))
    const settled = [...log]
    await sleep(100)

    expect(log).toEqual(settled)
    for (const call of neverCalled) {
      expect(log).not.toContain(call)
    }
    expect(disconnectOrder()).toEqual(order)
  })

  describe('an outage', () => {
    const SERVICE = 'kafka3'
    const idleCall = 'producer-1.connect'

    test.each([
      ['ends well when every part of it does', false, false, false, null],
      [
        'whose idle workload rejects early is over, with the broker back, when it rejects',
        true,
        false,
        false,
        'idle',
      ],
      ['whose stop throws starts the broker again at once', false, true, false, 'stop'],
      ['whose restart throws', false, false, true, 'start'],
      ['whose stop and restart both throw', false, true, true, 'stop'],
      ['whose idle workload rejects and stop throws', true, true, false, 'idle'],
      ['whose idle workload rejects and restart throws', true, false, true, 'idle'],
    ])('%s', async (_, idleFails, stopFails, startFails, rejectsWith) => {
      const events = []
      const thrown = { stop: new Error('stop failed'), start: new Error('start failed') }
      stopBroker.mockReset().mockImplementation((service) => {
        events.push(`stop ${service}`)
        if (stopFails) throw thrown.stop
      })
      startBroker.mockReset().mockImplementation((service) => {
        events.push(`start ${service}`)
        if (startFails) throw thrown.start
      })
      const { kafka, failures, disconnectOrder } = fakeKafka(idleFails ? [idleCall] : [])
      thrown.idle = failures.get(idleCall)

      const error = await WORKLOADS.outage(context(kafka), 0).then(
        () => null,
        (rejection) => rejection
      )

      expect(error).toBe(rejectsWith ? thrown[rejectsWith] : null)
      expect(events).toEqual([`stop ${SERVICE}`, `start ${SERVICE}`])
      expect(disconnectOrder()).toEqual(['consumer-1', 'producer-1'])
    })

    test('whose stop throws restarts the broker in the same instant, while the idle branch is still pending', async () => {
      jest.useFakeTimers({
        doNotFake: [
          'hrtime',
          'nextTick',
          'performance',
          'queueMicrotask',
          'setImmediate',
          'clearImmediate',
        ],
      })
      try {
        const begun = Date.now()
        const events = []
        const thrown = new Error('stop failed')
        stopBroker.mockReset().mockImplementation((service) => {
          events.push(`stop ${service} +${Date.now() - begun}`)
          throw thrown
        })
        startBroker.mockReset().mockImplementation((service) => {
          events.push(`start ${service} +${Date.now() - begun}`)
        })
        const { kafka, disconnectOrder } = fakeKafka()
        let settled = false
        const outage = WORKLOADS.outage(context(kafka), 90).then(
          () => null,
          (rejection) => rejection
        )
        outage.then(() => {
          settled = true
        })

        await jest.advanceTimersByTimeAsync(30)

        expect(events).toEqual([`stop ${SERVICE} +30`, `start ${SERVICE} +30`])
        expect(settled).toBe(false)
        expect(disconnectOrder()).toEqual([])

        await jest.advanceTimersByTimeAsync(1000)

        expect(await outage).toBe(thrown)
        expect(events).toHaveLength(2)
        expect(disconnectOrder()).toEqual(['consumer-1', 'producer-1'])
      } finally {
        jest.useRealTimers()
      }
    })
  })

  test('a sampler whose evidence write fails is joined after the workload ends and its clients are disconnected', async () => {
    const failure = new Error('evidence write failed')
    const { kafka, disconnectOrder } = fakeKafka()
    const soak = new Soak({
      kafka,
      probe: { snapshot: async () => quiet() },
      phases: [{ name: 'idle', workload: 'idle', ms: 80 }],
      context: {},
      sampleMs: 5,
      warmupMs: 0,
      settleMs: 20,
      record: (line) => {
        if (line.kind === 'sample') throw failure
      },
      workloads: {
        idle: (_, ms) =>
          Clients.use(kafka, async (clients) => {
            await clients.producer().connect()
            await sleep(ms)
          }),
      },
    })

    await expect(soak.run()).rejects.toBe(failure)

    expect(disconnectOrder()).toEqual(['producer-1'])
  })

  test('the topic the run is set up with is made by an admin that is always disconnected', async () => {
    const made = fakeKafka()
    await createTopic(made.kafka, 'soak-topic')
    expect(made.log).toEqual(['admin-1.connect', 'admin-1.createTopics', 'admin-1.disconnect'])

    const failed = fakeKafka(['admin-1.createTopics'])
    await expect(createTopic(failed.kafka, 'soak-topic')).rejects.toThrow(
      'admin-1.createTopics failed'
    )
    expect(failed.disconnected()).toEqual(['admin-1'])
  })
})

describe('Soak > a run on the scripted cluster', () => {
  // Another spec in this Jest worker may have exposed gc process-wide; these runs read no heap.
  const exposedGc = globalThis.gc
  beforeEach(() => {
    if (exposedGc) globalThis.gc = undefined
  })
  afterEach(() => {
    if (exposedGc) globalThis.gc = exposedGc
  })

  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

  /** Closes a fake socket the way a real one ends: `end()` leads to its destruction. */
  const closing = (factory) => (args) => {
    const socket = factory(args)
    const end = socket.end.bind(socket)
    socket.end = () => {
      end()
      socket.destroy()
    }
    return socket
  }

  const consumerOnly = async ({ kafka, topic, groupId }, ms) => {
    const consumer = kafka.consumer({ groupId, maxWaitTimeInMs: 20 })
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })
    await consumer.run({ eachMessage: async () => {} })
    await sleep(ms)
    await consumer.disconnect()
  }

  const smoke = async (extra = {}) => {
    const cluster = groupScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }] }),
      {
        batches: 0,
      }
    )
    const options = cluster.kafkaOptions()
    const { kafka, probe } = instrument(
      { ...options, socketFactory: closing(options.socketFactory) },
      { activeResources: false }
    )
    const lines = []
    const soak = new Soak({
      kafka,
      probe,
      phases: [{ name: 'idle', workload: 'idle', ms: 1000 }],
      context: { kafka, topic: TOPIC, groupId: 'soak-smoke' },
      sampleMs: 250,
      warmupMs: 0,
      settleMs: 200,
      record: (line) => lines.push(line),
      workloads: { idle: consumerOnly },
      ...extra,
    })

    return { cluster, lines, verdict: await soak.run() }
  }

  test('samples while it runs, reads the quiet points, and ends with nothing open', async () => {
    const { cluster, lines, verdict } = await smoke()

    const kinds = lines.map(({ kind }) => kind)
    expect(kinds[0]).toBe('baseline')
    expect(kinds.filter((kind) => kind === 'sample').length).toBeGreaterThanOrEqual(2)
    expect(kinds.slice(-3)).toEqual(['phase-end', 'teardown', 'verdict'])
    const phaseEnd = lines.find(({ kind }) => kind === 'phase-end')
    expect(phaseEnd.snapshot).toMatchObject({ queueDepth: 0, ledgerSize: 0, obligationSize: 0 })
    expect(phaseEnd.snapshot.sockets.open).toBe(0)
    expect(lines.find(({ kind }) => kind === 'teardown').snapshot.sockets.created).toBeGreaterThan(
      0
    )
    cluster.assertClean()
    // no `--expose-gc` here, so there is no heap series: the only bound that cannot hold
    expect(verdict.violations).toEqual(['0 heap samples after warm-up, 6 are needed'])
  }, 30000)

  test('hands its judge every sample with its phase and how long into it, and returns its verdict', async () => {
    const judge = jest.fn(() => ({ ok: true, violations: [] }))
    const { lines, verdict } = await smoke({ judge })

    expect(verdict).toEqual({ ok: true, violations: [] })
    expect(judge).toHaveBeenCalledTimes(1)
    const { samples, phaseEnds, warmupMs } = judge.mock.calls[0][0]
    expect(warmupMs).toBe(0)
    expect(phaseEnds.map(({ phase }) => phase)).toEqual(['idle'])
    expect(samples.length).toBeGreaterThanOrEqual(2)
    expect(samples.every(({ phase }) => phase === 'idle')).toBe(true)
    const phaseMs = samples.map((sample) => sample.phaseMs)
    expect(phaseMs).toEqual([...phaseMs].sort((a, b) => a - b))
    expect(phaseMs[0]).toBeGreaterThanOrEqual(200)
    expect(lines.find(({ kind }) => kind === 'sample')).toMatchObject({
      phase: 'idle',
      phaseMs: phaseMs[0],
    })
  }, 30000)

  test('reads a client that holds on to a socket as a violation', async () => {
    const cluster = groupScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }] }),
      {
        batches: 0,
      }
    )
    const options = cluster.kafkaOptions()
    const { kafka, probe } = instrument(
      {
        ...options,
        socketFactory: (args) => {
          const socket = options.socketFactory(args)
          socket.destroy = () => socket
          return socket
        },
      },
      { activeResources: false }
    )
    const lines = []
    const soak = new Soak({
      kafka,
      probe,
      phases: [{ name: 'idle', workload: 'idle', ms: 300 }],
      context: { kafka, topic: TOPIC, groupId: 'soak-smoke' },
      sampleMs: 100,
      warmupMs: 0,
      settleMs: 200,
      record: (line) => lines.push(line),
      workloads: { idle: consumerOnly },
    })

    const verdict = await soak.run()

    expect(verdict.violations).toContainEqual(
      expect.stringMatching(/^after disconnect: \d+ sockets still open$/)
    )
  }, 30000)

  test('an idle member whose producer connect rejects leaves no connection activity behind', async () => {
    const cluster = groupScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }] }),
      { batches: 0 }
    )
    const options = cluster.kafkaOptions({
      retry: { retries: 1, initialRetryTime: 20, factor: 0, multiplier: 1 },
    })
    const slow = ({ onConnect, ...rest }) =>
      options.socketFactory({ ...rest, onConnect: () => setTimeout(onConnect, 80) })
    const { kafka, probe } = instrument(
      { ...options, socketFactory: closing(slow) },
      { activeResources: false }
    )
    const failure = new Error('producer connect failed')
    const mixed = {
      producer: () => ({
        connect: async () => {
          await sleep(20)
          throw failure
        },
        disconnect: async () => {},
      }),
      consumer: (config) => kafka.consumer(config),
    }

    await expect(
      WORKLOADS.idle({ kafka: mixed, topic: TOPIC, groupId: 'soak-real' }, 1)
    ).rejects.toBe(failure)
    const settled = (await probe.snapshot()).sockets
    await sleep(300)
    const later = (await probe.snapshot()).sockets

    expect(later).toEqual(settled)
    expect(later.open).toBe(0)
  })

  test('a phase ends when its workload does, not when the next sample is due', async () => {
    const probe = { snapshot: async () => quiet() }
    const soak = new Soak({
      kafka: {},
      probe,
      phases: [{ name: 'idle', workload: 'idle', ms: 50 }],
      context: {},
      sampleMs: 60000,
      warmupMs: 0,
      settleMs: 50,
      record: () => {},
      workloads: { idle: () => sleep(50) },
    })
    let giveUp
    const late = new Promise((_, reject) => {
      giveUp = setTimeout(() => reject(new Error('the phase waited for its sampler')), 3000)
    })

    try {
      const verdict = await Promise.race([soak.run(), late])

      expect(verdict.violations).toEqual(['0 heap samples after warm-up, 6 are needed'])
    } finally {
      clearTimeout(giveUp)
    }
  })

  test('reads the baseline only after the sockets closed by the setup are gone', async () => {
    let closed = false
    const closing = setTimeout(() => {
      closed = true
    }, 150)
    const probe = {
      snapshot: async () =>
        quiet({
          listeners: { requestQueues: 0, sockets: closed ? 0 : 1 },
          sockets: { created: 1, destroyed: closed ? 1 : 0, open: closed ? 0 : 1 },
        }),
    }
    const lines = []
    const soak = new Soak({
      kafka: {},
      probe,
      phases: [{ name: 'idle', workload: 'idle', ms: 0 }],
      context: {},
      sampleMs: 60000,
      warmupMs: 0,
      settleMs: 2000,
      record: (line) => lines.push(line),
      workloads: { idle: async () => {} },
    })

    try {
      const verdict = await soak.run()

      expect(lines[0]).toMatchObject({
        kind: 'baseline',
        snapshot: { listeners: { sockets: 0 }, sockets: { open: 0 } },
      })
      expect(verdict.violations).toEqual(['0 heap samples after warm-up, 6 are needed'])
    } finally {
      clearTimeout(closing)
    }
  })

  test('waits out an obsolete connection that drains for up to the default request timeout before a quiet reading', async () => {
    jest.useFakeTimers()
    let closed = false
    setTimeout(() => {
      closed = true
    }, 30000)
    const probe = {
      snapshot: async () =>
        quiet({ sockets: { created: 1, destroyed: closed ? 1 : 0, open: closed ? 0 : 1 } }),
    }
    const lines = []
    const soak = new Soak({
      kafka: {},
      probe,
      phases: [{ name: 'idle', workload: 'idle', ms: 0 }],
      context: {},
      sampleMs: 60000,
      warmupMs: 0,
      record: (line) => lines.push(line),
      workloads: { idle: async () => {} },
    })

    try {
      const running = soak.run()
      await jest.advanceTimersByTimeAsync(31000)
      await running

      expect(lines[0]).toMatchObject({ kind: 'baseline', snapshot: { sockets: { open: 0 } } })
    } finally {
      jest.useRealTimers()
    }
  })

  test('reads the private members of a client by name, and says when one is gone', () => {
    const stand = { [Symbol('private:Kafka:operations')]: new Map() }

    expect(privateOf(stand, 'operations').value).toBeInstanceOf(Map)
    expect(() => privateOf(stand, 'createCluster')).toThrow(
      "The soak driver reads Kafka's private createCluster, which this client does not have"
    )
  })
})

describe('Consumer journal boundaries', () => {
  const { ConsumerChurnOracle } = require('../soak')
  test('has an independent journal verifier', () => {
    expect(typeof ConsumerChurnOracle).toBe('function')
  })
  const record = {
    incarnation: 'incarnation-a',
    topic: 't',
    partition: 0,
    offset: '0',
    sequence: 's0',
    group: 'g',
    owner: 'owner-a',
  }
  const observation = (kind, fields = {}) => ({ ...record, kind, ...fields })
  const journal = () => [
    observation('produce'),
    observation('acquire'),
    observation('read'),
    observation('handler'),
    observation('commit', { next: '1', brokerNext: '1' }),
    observation('revoke'),
    observation('delete'),
  ]
  const verify = (records) => new ConsumerChurnOracle(records, ['g']).verify()

  test('keeps three independent sets with an actual broker commit', async () => {
    await expect(verify(journal())).resolves.toEqual({ acknowledged: 1, delivered: 1, commits: 1 })
  })
  test.each([
    ['read', 'acknowledged log loss'],
    ['handler', 'delete before handler drain'],
  ])('detects a missing %s observation', async (kind, diagnostic) => {
    await expect(
      verify(
        journal().filter(
          (r) => r.kind !== kind && r.kind !== 'commit' && (kind !== 'read' || r.kind !== 'delete')
        )
      )
    ).rejects.toThrow(diagnostic)
  })
  test('refuses deletion before the acknowledged log is read', async () => {
    const rows = journal()
    rows.splice(2, 0, observation('delete'))
    await expect(verify(rows)).rejects.toThrow('delete before log verification')
  })
  test('refuses stale incarnation progress with equal numeric offsets', async () => {
    const rows = journal()
    rows[4].incarnation = 'incarnation-old'
    await expect(verify(rows)).rejects.toThrow('commit after ownership boundary')
  })
  test('refuses a commit inferred from a locally resolved promise', async () => {
    const rows = journal()
    delete rows[4].brokerNext
    await expect(verify(rows)).rejects.toThrow('unconfirmed broker commit')
  })
  test('refuses a new handler after successful revoke', async () => {
    const rows = journal()
    rows.splice(6, 0, observation('handler'))
    await expect(verify(rows)).rejects.toThrow('handler after ownership boundary')
  })
  test('refuses a commit by a superseded owner after it lost the partition', async () => {
    const rows = journal()
    rows.splice(5, 0, observation('loss'))
    rows.splice(
      6,
      0,
      observation('acquire', { owner: 'owner-b' }),
      observation('commit', { next: '1', brokerNext: '1' })
    )
    await expect(verify(rows)).rejects.toThrow('commit after ownership boundary')
  })
  test('refuses replay below the confirmed commit even after reacquisition', async () => {
    const rows = journal()
    rows.splice(
      6,
      0,
      observation('acquire', { owner: 'owner-b' }),
      observation('replay', { reason: 'crash', from: '1', to: '1' }),
      observation('handler', { owner: 'owner-b' })
    )
    await expect(verify(rows)).rejects.toThrow('undeclared handler replay')
  })
  test('accepts an explicit same-incarnation seek interval', async () => {
    const rows = journal()
    rows.splice(
      4,
      0,
      observation('replay', { reason: 'seek', from: '0', to: '1' }),
      observation('handler')
    )
    await expect(verify(rows)).resolves.toEqual({ acknowledged: 1, delivered: 2, commits: 1 })
  })
  describe('an uncommitted crash replay', () => {
    const second = { offset: '1', sequence: 's1' }
    const crashJournal = (...replayed) => [
      observation('produce'),
      observation('produce', second),
      observation('acquire'),
      observation('read'),
      observation('read', second),
      observation('handler'),
      observation('commit', { next: '1', brokerNext: '1' }),
      observation('handler', second),
      observation('loss'),
      observation('acquire', { owner: 'owner-b' }),
      observation('replay', { reason: 'crash', from: '1', to: '2' }),
      ...replayed,
      observation('revoke', { owner: 'owner-b' }),
      observation('delete'),
    ]
    test('starts at the last broker-confirmed position and is accepted once', async () => {
      await expect(
        verify(crashJournal(observation('handler', { ...second, owner: 'owner-b' })))
      ).resolves.toEqual({ acknowledged: 2, delivered: 3, commits: 1 })
    })
    test('expires once the replayed interval is handled again', async () => {
      const again = observation('handler', { ...second, owner: 'owner-b' })
      await expect(verify(crashJournal(again, again))).rejects.toThrow('undeclared handler replay')
    })
  })
  describe('a retry after a handler threw', () => {
    const second = { offset: '1', sequence: 's1' }
    const retry = (from = '1', fields = {}) =>
      observation('replay', { reason: 'retry', from, to: String(BigInt(from) + 1n), ...fields })
    const retried = (...after) => [
      observation('produce'),
      observation('produce', second),
      observation('acquire'),
      observation('position', { next: '0' }),
      observation('read'),
      observation('read', second),
      observation('handler'),
      observation('handler', second),
      ...after,
      observation('revoke', {
        owner: after.findLast((row) => row.kind === 'acquire')?.owner ?? 'owner-a',
      }),
      observation('delete'),
    ]
    const again = () => observation('handler', second)
    const counts = { acknowledged: 2, delivered: 3, commits: 0 }
    test('accepts the failed record once more inside the same ownership', async () => {
      await expect(verify(retried(retry(), again()))).resolves.toEqual(counts)
    })
    test('is spent by that one redelivery', async () => {
      await expect(verify(retried(retry(), again(), again()))).rejects.toThrow(
        'undeclared handler replay'
      )
    })
    test('is not a blanket allowance for a repeat nobody declared', async () => {
      await expect(verify(retried(again()))).rejects.toThrow('undeclared handler replay')
    })
    test('covers only the record it names', async () => {
      await expect(verify(retried(retry(), observation('handler')))).rejects.toThrow(
        'undeclared handler replay'
      )
    })
    test('names the last record the owner handled, not an earlier one', async () => {
      await expect(verify(retried(retry('0'), observation('handler')))).rejects.toThrow(
        'retry of a record that is not the last handled'
      )
    })
    test('names exactly one record', async () => {
      const wide = retry('1', { to: '3' })
      await expect(verify(retried(wide, again()))).rejects.toThrow('retry is not one record')
    })
    test('is refused once the ownership ended', async () => {
      const rows = retried(observation('loss'), retry(), again())
      await expect(verify(rows)).rejects.toThrow('retry after ownership boundary')
    })
    test('does not outlive a new ownership', async () => {
      const rows = retried(
        retry(),
        observation('revoke'),
        observation('acquire', { owner: 'owner-b' }),
        observation('position', { owner: 'owner-b', next: '2' }),
        observation('handler', { ...second, owner: 'owner-b' })
      )
      await expect(verify(rows)).rejects.toThrow('undeclared handler replay')
    })
    test('leaves a pending crash replay window to its own records', async () => {
      const crash = [
        observation('loss'),
        observation('acquire', { owner: 'owner-b' }),
        observation('position', { owner: 'owner-b', next: '0' }),
        observation('handler', { owner: 'owner-b' }),
        retry('0', { owner: 'owner-b' }),
        observation('handler', { owner: 'owner-b' }),
        observation('handler', { ...second, owner: 'owner-b' }),
      ]
      await expect(verify(retried(...crash))).resolves.toEqual({
        acknowledged: 2,
        delivered: 5,
        commits: 0,
      })
    })
  })
  describe('a replay after an ownership ended without a confirmed commit', () => {
    const second = { offset: '1', sequence: 's1' }
    const again = (fields = {}) => observation('handler', { owner: 'owner-b', ...fields })
    const reacquired = (end, position, ...after) => [
      observation('produce'),
      observation('produce', second),
      observation('acquire'),
      observation('position', { next: '0' }),
      observation('read'),
      observation('read', second),
      observation('handler'),
      observation('handler', second),
      ...end,
      observation('acquire', { owner: 'owner-b' }),
      observation('position', { owner: 'owner-b', next: position }),
      ...after,
      observation('revoke', { owner: 'owner-b' }),
      observation('delete'),
    ]
    const counts = { acknowledged: 2, delivered: 4, commits: 0 }
    test.each([['loss'], ['revoke']])(
      'accepts each record once again after a %s with nothing committed',
      async (kind) => {
        await expect(
          verify(reacquired([observation(kind)], '0', again(), again(second)))
        ).resolves.toEqual(counts)
      }
    )
    test('accepts only the records from the position the broker returned', async () => {
      const committed = [observation('commit', { next: '1', brokerNext: '1' }), observation('loss')]
      const rows = (...after) => reacquired(committed, '1', ...after)
      await expect(verify(rows(again(second)))).resolves.toEqual({
        acknowledged: 2,
        delivered: 3,
        commits: 1,
      })
      await expect(verify(rows(again()))).rejects.toThrow('undeclared handler replay')
    })
    test('refuses a record replayed twice by the new owner', async () => {
      await expect(
        verify(reacquired([observation('loss')], '0', again(), again(), again(second)))
      ).rejects.toThrow('undeclared handler replay')
    })
    test('refuses a position below a confirmed commit', async () => {
      const committed = [observation('commit', { next: '2', brokerNext: '2' }), observation('loss')]
      await expect(verify(reacquired(committed, '1', again(second)))).rejects.toThrow(
        'position below confirmed commit'
      )
    })
    test('grants nothing to a position that moves a held ownership', async () => {
      const rows = journal()
      rows.splice(4, 0, observation('position', { next: '0' }), observation('handler'))
      await expect(verify(rows)).rejects.toThrow('undeclared handler replay')
    })
    test('grants nothing to a delivery repeated inside one ownership', async () => {
      const rows = journal()
      rows.splice(4, 0, observation('handler'))
      await expect(verify(rows)).rejects.toThrow('undeclared handler replay')
    })
  })
  describe('contiguous eligible progress', () => {
    const at = (offset) => ({ offset: String(offset), sequence: `s${offset}` })
    const log = (offsets, deliver) => [
      ...offsets.map((offset) => observation('produce', at(offset))),
      observation('acquire'),
      ...offsets.map((offset) => observation('read', at(offset))),
      ...deliver.map((offset) => observation('handler', at(offset))),
    ]
    test('refuses a commit past an undelivered lower record', async () => {
      const rows = [
        ...log([0, 1, 2], [0, 2]),
        observation('commit', { next: '3', brokerNext: '3' }),
      ]
      await expect(verify(rows)).rejects.toThrow('commit beyond processed position')
    })
    test('accepts a commit up to the first undelivered record', async () => {
      const rows = [
        ...log([0, 1, 2], [0, 2]),
        observation('commit', { next: '1', brokerNext: '1' }),
        observation('revoke'),
      ]
      const oracle = new ConsumerChurnOracle(rows, ['g'])
      await oracle.feed(rows.slice(0, -1))
      expect(oracle.counts.commits).toBe(1)
    })
    test('crosses offsets that were never acknowledged but not the end of the log', async () => {
      const rows = log([0, 2], [0, 2])
      await expect(
        verify(
          [...rows, observation('commit', { next: '3', brokerNext: '3' })].concat([
            observation('revoke'),
            observation('delete', at(0)),
          ])
        )
      ).resolves.toEqual({ acknowledged: 2, delivered: 2, commits: 1 })
      await expect(
        verify([...rows, observation('commit', { next: '4', brokerNext: '4' })])
      ).rejects.toThrow('commit beyond processed position')
    })
    test('starts at the broker position a group acquired', async () => {
      const rows = [
        ...[0, 1].map((offset) => observation('produce', at(offset))),
        observation('acquire'),
        observation('position', { next: '1' }),
        ...[0, 1].map((offset) => observation('read', at(offset))),
        observation('handler', at(1)),
        observation('commit', { next: '2', brokerNext: '2' }),
      ]
      const oracle = new ConsumerChurnOracle(rows, ['g'])
      await oracle.feed(rows)
      expect(oracle.counts.commits).toBe(1)
    })
  })
  describe('streaming segments', () => {
    test('carries ownership across segments and evicts a deleted incarnation', async () => {
      const rows = journal()
      const oracle = new ConsumerChurnOracle([], ['g'])
      await oracle.feed(rows.slice(0, 4))
      expect(oracle.retained()).toEqual({ acknowledged: 1, scopes: 1 })
      await oracle.feed(rows.slice(4))
      expect(oracle.retained()).toEqual({ acknowledged: 0, scopes: 0 })
      await expect(oracle.finish()).resolves.toEqual({ acknowledged: 1, delivered: 1, commits: 1 })
    })
    test('keeps live state bounded by the incarnations in flight', async () => {
      const oracle = new ConsumerChurnOracle([], ['g'])
      for (let i = 0; i < 200; i++) {
        await oracle.feed(
          journal().map((r) => ({ ...r, incarnation: `incarnation-${i}`, sequence: `s${i}` }))
        )
      }
      expect(oracle.retained()).toEqual({ acknowledged: 0, scopes: 0 })
      expect(oracle.counts.acknowledged).toBe(200)
    })
  })
  test('refuses an unacknowledged record and overlapping owners', async () => {
    const rows = journal()
    rows.splice(3, 0, observation('handler', { sequence: 'other' }))
    await expect(verify(rows)).rejects.toThrow('unexpected handler record')
    const overlap = journal()
    overlap.splice(2, 0, observation('acquire', { owner: 'owner-b' }))
    await expect(verify(overlap)).rejects.toThrow('overlapping ownership')
  })
  test('does not round large offsets through Number', async () => {
    const rows = journal().map((r) => ({ ...r, offset: '9007199254740993' }))
    rows[4].next = rows[4].brokerNext = '9007199254740994'
    await expect(verify(rows)).resolves.toEqual({ acknowledged: 1, delivered: 1, commits: 1 })
  })
  test('refuses missing delivery at a closed segment boundary', async () => {
    const rows = journal().filter((row) => !['handler', 'commit', 'delete'].includes(row.kind))
    await expect(verify(rows)).rejects.toThrow('acknowledged handler loss')
  })
  test('refuses a broker commit beyond processed progress', async () => {
    const rows = journal()
    rows[4].next = rows[4].brokerNext = '2'
    await expect(verify(rows)).rejects.toThrow('commit beyond processed position')
  })
  test('refuses a crash replay interval below a confirmed boundary', async () => {
    const rows = journal()
    rows.splice(5, 0, observation('replay', { reason: 'crash', from: '0', to: '1' }))
    await expect(verify(rows)).rejects.toThrow('replay boundary differs from confirmed commit')
  })
  test('requires every intended group to receive its corpus', async () => {
    await expect(new ConsumerChurnOracle(journal(), ['g', 'silent']).verify()).rejects.toThrow(
      'delete before handler drain'
    )
  })
  test('yields while indexing a large closed journal', async () => {
    const rows = Array.from({ length: 4096 }, (_, i) =>
      journal().map((r) => ({
        ...r,
        sequence: `s${i}`,
        incarnation: `incarnation-${i}`,
      }))
    ).flat()
    let heartbeatRan = false
    setImmediate(() => {
      heartbeatRan = true
    })
    const pending = verify(rows)
    expect(heartbeatRan).toBe(false)
    await pending
    expect(heartbeatRan).toBe(true)
  })
})

const { IdleSocketCheck } = require('../soak')

describe('Idle producer socket checks', () => {
  const fixture = ({ retain = false, fakeReopen = false, alter = (row) => row } = {}) => {
    let reads = 0
    const producer = {
      send: jest.fn(async () => {}),
    }
    const probe = {
      snapshot: jest.fn(async () => {
        reads++
        const created = reads < 3 ? 2 : reads < 5 || fakeReopen ? 4 : 6
        const open = (reads === 2 && !retain) || reads === 4 ? 0 : 2
        const first = reads < 3 ? 1 : reads < 5 || fakeReopen ? 3 : 5
        return alter(
          {
            socketNodes: ['leader', 'metadata'],
            sockets: {
              created,
              destroyed: created - open,
              open,
              live: open
                ? [
                    { node: 'leader', id: first },
                    { node: 'metadata', id: first + 1 },
                  ]
                : [],
            },
            queueDepth: 0,
          },
          reads
        )
      }),
    }
    const record = jest.fn()
    const nap = jest.fn(async () => {})
    return {
      check: new IdleSocketCheck({ producer, probe, topic: 'idle', record, nap }),
      producer,
      record,
      nap,
    }
  }
  test('observes zero before reopening and checks bounded fresh node identities', async () => {
    const { check, producer, record, nap } = fixture()
    await check.cycle()
    expect(producer.send).toHaveBeenCalledTimes(3)
    expect(nap).toHaveBeenCalledWith(6000)
    expect(record).toHaveBeenCalledWith(
      expect.objectContaining({
        kind: 'idle-cycle',
        idle: expect.objectContaining({ sockets: expect.objectContaining({ open: 0 }) }),
      })
    )
  })
  test('a surviving socket fails before the second send', async () => {
    const { check, producer } = fixture({ retain: true })
    await expect(check.cycle()).rejects.toThrow('not reaped')
    expect(producer.send).toHaveBeenCalledTimes(1)
  })
  test('the old socket count cannot impersonate a reconnect', async () => {
    const { check } = fixture({ fakeReopen: true })
    await expect(check.cycle()).rejects.toThrow('did not reopen')
  })

  test.each([
    [
      'duplicate endpoint',
      (row) => {
        row.sockets.live[1].node = 'leader'
      },
    ],
    [
      'unknown endpoint',
      (row) => {
        row.sockets.live[1].node = 'unknown'
      },
    ],
    [
      'stale identity with increased creation count',
      (row) => {
        row.sockets.live[0].id = 3
      },
    ],
    [
      'duplicate identity',
      (row) => {
        row.sockets.live[1].id = 5
      },
    ],
  ])('rejects a reopened population with %s', async (_name, change) => {
    const { check } = fixture({
      alter: (row, read) => {
        if (read === 5) change(row)
        return row
      },
    })
    await expect(check.cycle()).rejects.toThrow('did not reopen')
  })
  test.each([
    [
      'socket',
      (row) => {
        row.sockets.live = [{ node: 'leader', id: 4 }]
      },
    ],
    [
      'request',
      (row) => {
        row.queueDepth = 1
      },
    ],
  ])('rejects a surviving %s in the second idle interval', async (_name, change) => {
    const { check, producer } = fixture({
      alter: (row, read) => {
        if (read === 4) change(row)
        return row
      },
    })
    await expect(check.cycle()).rejects.toThrow('not reaped')
    expect(producer.send).toHaveBeenCalledTimes(2)
  })
})

const { EventEmitter } = require('events')
const Encoder = require('../../../src/protocol/encoder')
const { ShareAckLedger } = require('../../../src/consumer/share/shareAckLedger')
const { shareRecordIdentity } = require('../../../testHelpers/shareFixtures')
const { CleanupStack } = require('../../../src/producer/__tests__/fixtures/liveProbe')
const {
  LiveRun,
  liveRun,
  NEVER,
  bounded,
} = require('../../../src/producer/__tests__/fixtures/bounded')
const {
  SHARE_SOAK,
  SHARE_SOAK_TITLE,
  ShareCycleOracle,
  ShareCycleJournal,
  ShareSoakDriver,
  ShareMember,
  shareSoakNaturalExit,
  ShareFetchTap,
  ShareSoak,
  ShareSoakScreen,
  ShareStateObserver,
  instrumentShare,
  main,
} = require('../soak')

/** Returns complete real-source-shaped facts. No local heap measurement occurs. */
const shareScreenRun = () => {
  const quietShare = (heapUsed = 10000000) => ({
    queueDepth: 0,
    shareSessions: 0,
    shareRecords: 0,
    shareRanges: 0,
    sockets: { open: 0 },
    activeResources: { Timeout: 1 },
    activeResourcesBeforeCollection: { Timeout: 1 },
    namedListeners: {},
    heapUsed,
  })
  const phases = ['share-steady', 'share-reset', 'share-members']
  return {
    baseline: { ...quietShare(), heapAtMs: 0 },
    warmupMs: 10000,
    samples: phases.flatMap((phase, index) =>
      Array.from({ length: 6 }, (_, i) => ({
        phase,
        phaseMs: 12000 + i * 4000,
        snapshot: quietShare(10000000 + index * 10000000),
      }))
    ),
    phaseEnds: phases.map((phase, index) => ({
      phase,
      snapshot: { ...quietShare(), heapAtMs: (index + 1) * 320000 },
    })),
    teardown: { ...quietShare(), heapAtMs: 970000 },
  }
}

describe('Share soak resource screen', () => {
  test('declares thirty cycles ten resets and the record bound before execution', () => {
    expect(SHARE_SOAK).toMatchObject({
      cycles: 30,
      resets: 10,
      maxRecords: 100,
      nodes: 3,
      shortMs: 960000,
      fullMs: 1920000,
      endpointBytes: 2097152,
      slopeBytesPerMinute: 262144,
      quietSlopeBytesPerMinute: 98304,
    })
  })

  test('screens each working set separately and compares phase-end quiet points', () => {
    const result = ShareSoakScreen.evaluate(shareScreenRun())
    expect(result.violations).toEqual([])
    expect(result.ok).toBe(true)
    expect(result.measured.phases.map((reading) => reading.endpointBytes)).toEqual([0, 0, 0])
  })

  test('accepts the flat working sets that the whole-run rule refuses', () => {
    const run = shareScreenRun()
    const old = evaluate({
      baseline: quiet(),
      phaseEnds: run.phaseEnds.map(({ phase }) => ({ phase, snapshot: quiet() })),
      samples: run.samples.map((sample, index) => ({
        atMs: index * 4000,
        snapshot: quiet({ heapUsed: sample.snapshot.heapUsed }),
      })),
      teardown: quiet(),
      warmupMs: 0,
    })
    expect(old.ok).toBe(false)
    expect(ShareSoakScreen.evaluate(run).ok).toBe(true)
  })

  test.each(['queueDepth', 'shareSessions', 'shareRecords', 'shareRanges'])(
    'refuses retained %s',
    (gauge) => {
      const run = shareScreenRun()
      run.teardown[gauge] = 1
      expect(ShareSoakScreen.evaluate(run).ok).toBe(false)
    }
  )

  test('refuses missing sources and extra resource types', () => {
    const run = shareScreenRun()
    delete run.teardown.shareRanges
    expect(ShareSoakScreen.evaluate(run).ok).toBe(false)
    const extra = shareScreenRun()
    extra.teardown.activeResources.TCPSocketWrap = 1
    expect(ShareSoakScreen.evaluate(extra).ok).toBe(false)
  })

  test('refuses a pre-collection surplus even when the post-collection reading matches', () => {
    const run = shareScreenRun()
    run.teardown.activeResourcesBeforeCollection.Timeout = 2
    expect(ShareSoakScreen.evaluate(run).violations).toContain(
      'teardown: activeResourcesBeforeCollection.Timeout baseline=1 after=2'
    )
    delete run.teardown.activeResourcesBeforeCollection
    expect(ShareSoakScreen.evaluate(run).ok).toBe(false)
  })

  test('compares quiet resources without the attributed Jest status debounce in either inventory', () => {
    const { ShareTimerObserver } = require('../shareTimerObserver')
    const reporter = {
      type: 'Timeout',
      asyncId: 35619,
      triggerAsyncId: 4246,
      stack:
        'Error: \n' +
        '    at emitInitNative (node:internal/async_hooks:207:43)\n' +
        '    at setTimeout (node:timers:125:19)\n' +
        '    at Status._debouncedEmit (/ci/node_modules/@jest/reporters/build/index.js:1507:7)\n' +
        '    at Status._tick (/ci/node_modules/@jest/reporters/build/index.js:1514:10)',
      hasRef: true,
      durationMs: 100,
      repeatMs: null,
    }
    const owners = (rows) => ShareTimerObserver.evidence({ rows, dropped: 0 }, { Timeout: 2 })
    const run = shareScreenRun()
    run.baseline.activeResourcesBeforeCollection.Timeout = 2
    run.baseline.timerOwners = {
      beforeCollection: owners([reporter]),
      afterCollection: owners([]),
    }
    run.teardown.activeResources.Timeout = 2
    run.teardown.timerOwners = {
      beforeCollection: owners([]),
      afterCollection: owners([reporter]),
    }
    expect(run.baseline.timerOwners.beforeCollection.harnessAsyncIds).toEqual([35619])
    expect(ShareSoakScreen.evaluate(run).violations).toEqual([])

    // A product timeout with the same duration and a reporter ancestor is still a surplus.
    const product = {
      ...reporter,
      asyncId: 35620,
      stack: reporter.stack.replace(
        '    at Status._debouncedEmit',
        '    at RequestQueue.scheduleRequestTimeoutCheck (/ci/src/network/requestQueue/index.js:88:7)\n' +
          '    at Status._debouncedEmit'
      ),
    }
    run.teardown.activeResourcesBeforeCollection.Timeout = 2
    run.teardown.timerOwners.beforeCollection = owners([product])
    expect(run.teardown.timerOwners.beforeCollection.harnessAsyncIds).toEqual([])
    expect(ShareSoakScreen.evaluate(run).violations).toContain(
      'teardown: activeResourcesBeforeCollection.Timeout baseline=1 after=2'
    )
    run.teardown.activeResources.Timeout = 3
    run.teardown.timerOwners.afterCollection = owners([reporter, product])
    expect(ShareSoakScreen.evaluate(run).violations).toContain(
      'teardown: activeResources.Timeout baseline=1 after=2'
    )
  })

  test('names each differing source with its baseline and quiet-point reading', () => {
    const run = shareScreenRun()
    run.teardown.queueDepth = 1
    delete run.teardown.shareRanges
    run.teardown.sockets.open = 1
    run.teardown.activeResources.Timeout = 2
    run.teardown.namedListeners['owner1:error'] = 1
    expect(ShareSoakScreen.evaluate(run).violations).toEqual([
      'teardown: resources differ or a source is absent',
      'teardown: queueDepth baseline=0 after=1',
      'teardown: shareRanges baseline=0 after=absent',
      'teardown: sockets.open baseline=0 after=1',
      'teardown: activeResources.Timeout baseline=1 after=2',
      'teardown: namedListeners.owner1:error baseline=0 after=1',
    ])
  })

  test('refuses a listener swap that leaves the total unchanged', () => {
    const run = shareScreenRun()
    run.baseline.namedListeners = { 's:data': 1 }
    run.teardown.namedListeners = { 's:close': 1 }
    expect(ShareSoakScreen.evaluate(run).ok).toBe(false)
  })

  test('refuses a phase with fewer than six samples', () => {
    const run = shareScreenRun()
    run.samples.shift()
    expect(ShareSoakScreen.evaluate(run).violations.join(' ')).toMatch('six post-warm-up')
  })

  test('refuses a growing phase and a growing quiet series independently', () => {
    const phase = shareScreenRun()
    phase.samples.slice(0, 6).forEach((reading, i) => {
      reading.snapshot.heapUsed += i * 1048576
    })
    expect(ShareSoakScreen.evaluate(phase).ok).toBe(false)
    const quietSeries = shareScreenRun()
    quietSeries.phaseEnds[1].snapshot.heapUsed += 2097153
    expect(ShareSoakScreen.evaluate(quietSeries).violations).toContain(
      'phase-end quiet heap screen'
    )
  })

  test('refuses more than maxRecords times nodes held records', () => {
    const run = shareScreenRun()
    run.samples[1].snapshot.shareRecords = 301
    expect(ShareSoakScreen.evaluate(run).violations).toContain('share-steady: held-record ceiling')
  })

  test('refuses a sample that is missing the compact-state measurement', () => {
    const run = shareScreenRun()
    delete run.samples[7].snapshot.shareSessions
    expect(ShareSoakScreen.evaluate(run).violations).toContain(
      'share-reset: missing compact-state measurement'
    )
  })

  test('refuses a baseline that is not quiet or a run with a missing quiet point', () => {
    const noisy = shareScreenRun()
    noisy.baseline.shareRecords = 1
    expect(ShareSoakScreen.evaluate(noisy).violations).toContain(
      'baseline: resources differ or a source is absent'
    )
    const short = shareScreenRun()
    short.phaseEnds.pop()
    expect(ShareSoakScreen.evaluate(short).violations).toContain('phase-end quiet heap screen')
  })
})

/**
 * Builds complete periodic evidence over the declared scored spans, with a common quiet set.
 * @param {{ short?: boolean, rate: number, firstUseBytes?: number }} options elapsed growth in B/min
 * @returns {object} original-source-shaped quiet and active observations with distinct working sets
 */
const retainedShareRun = ({ short = true, rate, firstUseBytes = 0 }) => {
  const run = shareScreenRun()
  const mode = short ? 'short' : 'full'
  const phaseMs = (short ? SHARE_SOAK.shortMs : SHARE_SOAK.fullMs) / 3
  const retained = (atMs) => 10000000 + firstUseBytes + (rate * atMs) / 60000
  // Integral heap bytes on a 1/32-minute grid; phase work retains its declared span.
  const observationTime = (atMs) => Math.ceil(atMs / 1875) * 1875
  let startedAt = 0
  run.samples = run.phaseEnds.flatMap(({ phase, snapshot: end }, index) => {
    const start = startedAt
    const samples = Array.from(
      { length: Math.floor(phaseMs / SHARE_SOAK.sampleMs[mode]) - 1 },
      (_, i) => {
        const atMs = observationTime(start + (i + 1) * SHARE_SOAK.sampleMs[mode])
        return {
          phase,
          phaseMs: atMs - start,
          atMs,
          snapshot: {
            ...run.baseline,
            heapAtMs: atMs,
            heapUsed: retained(atMs) + index * 12000000,
          },
        }
      }
    )
    end.heapAtMs = observationTime(start + phaseMs + SHARE_SOAK.settleMs)
    end.heapUsed = retained(end.heapAtMs)
    startedAt = end.heapAtMs
    return samples
  })
  run.teardown.heapAtMs = observationTime(startedAt + SHARE_SOAK.settleMs)
  run.teardown.heapUsed = retained(run.teardown.heapAtMs)
  return run
}

describe('Share quiet heap slope', () => {
  const failure = 'phase-end quiet heap slope screen'

  test.each([true, false])('detects sustained 128 KiB/min, short=%s', (short) => {
    const result = ShareSoakScreen.evaluate(retainedShareRun({ short, rate: 128 * 1024 }))
    expect(result.measured.quietSlope.bytesPerMinute).toBe(131072)
    expect(result.violations).toContain(failure)
    expect(
      result.measured.phases.every(
        ({ slopeBytesPerMinute, endpointBytes }) =>
          slopeBytesPerMinute < 262144 && endpointBytes < 2097152
      )
    ).toBe(true)
    expect(result.violations).toEqual(short ? [failure] : ['phase-end quiet heap screen', failure])
  })

  test.each([true, false])('rejects 256 and above 256 KiB/min, short=%s', (short) => {
    for (const rate of [256 * 1024, 384 * 1024]) {
      const result = ShareSoakScreen.evaluate(retainedShareRun({ short, rate }))
      expect(result.measured.quietSlope.bytesPerMinute).toBe(rate)
      expect(result.violations).toContain(failure)
      if (rate > 262144)
        expect(result.violations).toEqual(
          expect.arrayContaining(
            SHARE_SOAK.phases.map((phase) => `share-${phase}: retained-heap screen`)
          )
        )
    }
  })

  test.each([true, false])('a first-use constant step cannot change the fit, short=%s', (short) => {
    const plain = ShareSoakScreen.evaluate(retainedShareRun({ short, rate: 131072 }))
    const stepped = ShareSoakScreen.evaluate(
      retainedShareRun({ short, rate: 131072, firstUseBytes: 8000000 })
    )
    expect(stepped.measured.quietSlope.bytesPerMinute).toBe(
      plain.measured.quietSlope.bytesPerMinute
    )
    expect(stepped.violations).toContain(failure)
  })

  test('keeps first-phase periodic growth failing when all later quiet heaps plateau', () => {
    const run = retainedShareRun({ rate: 384 * 1024 })
    const plateau = run.phaseEnds[0].snapshot.heapUsed
    run.phaseEnds.forEach(({ snapshot }) => {
      snapshot.heapUsed = plateau
    })
    run.teardown.heapUsed = plateau
    run.samples
      .filter(({ phase }) => phase !== 'share-steady')
      .forEach(({ snapshot }) => {
        snapshot.heapUsed = plateau
      })
    const result = ShareSoakScreen.evaluate(run)
    expect(result.measured.quietSlope.bytesPerMinute).toBe(0)
    expect(result.violations).toEqual(['share-steady: retained-heap screen'])
  })

  test.each([0, -98304, 98304])(
    'accepts %s B/min at irregular, translated elapsed times',
    (rate) => {
      const run = shareScreenRun()
      const shift = 1234560000
      run.baseline.heapAtMs = shift
      run.phaseEnds.forEach(({ snapshot }, i) => {
        const minutes = [1, 3, 8][i]
        snapshot.heapAtMs = shift + minutes * 60000
        snapshot.heapUsed = 10000000 + rate * minutes
      })
      run.teardown.heapAtMs = shift + 9 * 60000
      const result = ShareSoakScreen.evaluate(run)
      expect(result.ok).toBe(true)
      expect(result.measured.quietSlope.bytesPerMinute).toBe(rate)
    }
  )

  test('compares an above-bound fractional slope without rounding', () => {
    const run = shareScreenRun()
    run.phaseEnds.forEach(({ snapshot }, i) => {
      const minutes = [1, 3, 8][i]
      snapshot.heapAtMs = minutes * 60000
      snapshot.heapUsed = 10000000 + 98304 * minutes + (i === 2 ? 1 : 0)
    })
    const result = ShareSoakScreen.evaluate(run)
    const rate = result.measured.quietSlope.bytesPerMinute
    expect(rate).toBeGreaterThan(98304)
    expect(Math.round(rate)).toBe(98304)
    expect(result.violations).toEqual([failure])
    expect(result.measured.quietSlope.points).toEqual(
      run.phaseEnds.map(({ phase, snapshot }) => ({
        phase,
        heapAtMs: snapshot.heapAtMs,
        heapUsed: snapshot.heapUsed,
      }))
    )
  })

  test.each([undefined, null, NaN, Infinity, -Infinity, -1, '320000'])(
    'rejects invalid observation time %s at every quiet point',
    (time) => {
      for (let index = 0; index < 5; index++) {
        const run = shareScreenRun()
        const points = [
          run.baseline,
          ...run.phaseEnds.map(({ snapshot }) => snapshot),
          run.teardown,
        ]
        points[index].heapAtMs = time
        const result = ShareSoakScreen.evaluate(run)
        expect(result.violations).toContain(failure)
        expect(result.measured.quietSlope.bytesPerMinute).toBeNull()
        expect(result.measured.quietSlope.points).toHaveLength(3)
      }
    }
  )

  test.each([undefined, null, NaN, Infinity, -1, 0.5, Number.MAX_SAFE_INTEGER + 1])(
    'rejects invalid heap source %s at every quiet point',
    (heap) => {
      for (let index = 0; index < 5; index++) {
        const run = shareScreenRun()
        const points = [
          run.baseline,
          ...run.phaseEnds.map(({ snapshot }) => snapshot),
          run.teardown,
        ]
        points[index].heapUsed = heap
        const result = ShareSoakScreen.evaluate(run)
        expect(result.violations).toContain(failure)
        expect(result.measured.quietSlope.bytesPerMinute).toBeNull()
      }
    }
  )

  test.each([0, -1])(
    'rejects duplicate or reversed adjacent observation times, delta=%s',
    (delta) => {
      for (let index = 1; index < 5; index++) {
        const run = shareScreenRun()
        const points = [
          run.baseline,
          ...run.phaseEnds.map(({ snapshot }) => snapshot),
          run.teardown,
        ]
        points[index].heapAtMs = points[index - 1].heapAtMs + delta
        const result = ShareSoakScreen.evaluate(run)
        expect(result.violations).toContain(failure)
        expect(result.measured.quietSlope.bytesPerMinute).toBeNull()
      }
    }
  )

  test.each(['missing', 'duplicate', 'reordered', 'wrong name', 'absent source'])(
    'rejects %s phase ends',
    (damage) => {
      const run = shareScreenRun()
      if (damage === 'missing') run.phaseEnds.pop()
      if (damage === 'duplicate') run.phaseEnds.push(run.phaseEnds[2])
      if (damage === 'reordered') run.phaseEnds.reverse()
      if (damage === 'wrong name') run.phaseEnds[1].phase = 'share-steady'
      if (damage === 'absent source') delete run.phaseEnds[1].snapshot
      const result = ShareSoakScreen.evaluate(run)
      expect(result.violations).toContain(failure)
      expect(result.measured.quietSlope.bytesPerMinute).toBeNull()
    }
  )

  test('keeps teardown endpoint and resource failures independent of a flat quiet slope', () => {
    const run = shareScreenRun()
    run.baseline.heapUsed = 1
    run.teardown.heapUsed = run.phaseEnds[0].snapshot.heapUsed + 2097153
    run.teardown.shareRecords = 1
    const result = ShareSoakScreen.evaluate(run)
    expect(result.measured.quietSlope.bytesPerMinute).toBe(0)
    expect(result.violations).toContain('phase-end quiet heap screen')
    expect(result.violations).toContain('teardown: resources differ or a source is absent')
    expect(result.violations).not.toContain(failure)
  })

  test.each(require('./fixtures/shareHeapReplays.json'))(
    'replays frozen screen inputs $sha256',
    (receipt) => {
      const { run, measured } = receipt
      const result = ShareSoakScreen.evaluate(run)
      const compact = {
        ...run,
        samples: run.samples.map((sample) => ({
          ...sample,
          snapshot: {
            heapUsed: sample.snapshot.heapUsed,
            shareRecords: sample.snapshot.shareRecords,
            shareRanges: sample.snapshot.shareRanges,
            shareSessions: sample.snapshot.shareSessions,
          },
        })),
      }
      expect(ShareSoakScreen.evaluate(compact)).toEqual(result)
      expect(result.measured.phases).toEqual(measured.phases)
      expect(result.measured.quietHeap).toEqual(measured.quietHeap)
      expect(result.violations).toEqual(['share-steady: retained-heap screen', failure])
      expect(result.measured.quietSlope.bytesPerMinute).toBeNull()
      expect(result.measured.quietSlope.points.every(({ heapAtMs }) => heapAtMs === null)).toBe(
        true
      )
    }
  )
})

describe('Share soak profiles', () => {
  const OLD_PROFILES = {
    idle: [['idle', 'idle', 420000]],
    'member-churn': [['member-churn', 'memberChurn', 420000]],
    'regex-topic-churn': [['regex-topic-churn', 'regexTopicChurn', 420000]],
    outage: [['outage', 'outage', 420000]],
    'producer-churn': [
      ['steady', 'producerSteady', 300000],
      ['outage', 'producerOutage', 300000],
      ['buffer-pressure', 'producerPressure', 300000],
    ],
    'consumer-churn': [
      ['member-churn', 'consumerMemberChurn', 300000],
      ['regex-topics', 'consumerRegexTopics', 300000],
      ['pause-recreate', 'consumerPauseRecreate', 300000],
      ['mostly-idle', 'consumerMostlyIdle', 300000],
    ],
    'first-run': [
      ['idle', 'idle', 300000],
      ['member-churn', 'memberChurn', 300000],
      ['outage', 'outage', 300000],
    ],
  }

  test.each(Object.entries(OLD_PROFILES))('%s keeps its inherited phases', (name, phases) => {
    expect(PROFILES[name]).toEqual(
      phases.map(([phase, workload, ms]) => ({ name: phase, workload, ms }))
    )
  })

  test.each([
    ['share-consumer-short', 320000, 960000],
    ['share-consumer', 640000, 1920000],
  ])('%s is three phases of %i ms, %i ms in all', (name, phaseMs, total) => {
    expect(PROFILES[name]).toEqual(
      ['share-steady', 'share-reset', 'share-members'].map((phase) => ({
        name: phase,
        workload: 'shareConsumer',
        ms: phaseMs,
      }))
    )
    expect(PROFILES[name].reduce((sum, { ms }) => sum + ms, 0)).toBe(total)
    expect(WORKLOADS.shareConsumer).toEqual(expect.any(Function))
  })

  test.each([
    [['--profile', 'share-consumer'], { profile: 'share-consumer', phaseSeconds: null }],
    [
      ['--profile', 'share-consumer-short'],
      { profile: 'share-consumer-short', phaseSeconds: null },
    ],
  ])('%j selects the share profile', (argv, parsed) => {
    expect(parseArgs(argv)).toEqual(parsed)
  })

  test.each([
    [['--profile', 'share-consumer', '--phase-seconds', '5']],
    [['--profile', 'share-consumer-short', '--phase-seconds', '5']],
  ])('%j is refused: the phases and cycles of a share profile are fixed', (argv) => {
    expect(() => parseArgs(argv)).toThrow(/^usage: node --expose-gc scripts\/soak\/soak\.js/)
  })

  test.each(['short', 'full'])('the %s budget holds its stages and its cycles', (mode) => {
    const budget = SHARE_SOAK.budgets[mode]
    const { primingMs, setupMs, profileMs, transitionMs, cleanupMs, exitMs, totalMs } = budget
    expect(setupMs + profileMs + transitionMs + cleanupMs + exitMs).toBeLessThanOrEqual(totalMs)
    expect(profileMs).toBe(mode === 'short' ? SHARE_SOAK.shortMs : SHARE_SOAK.fullMs)
    expect(SHARE_SOAK.cycles * SHARE_SOAK.slotMs[mode]).toBe(profileMs)
    const phaseMs = profileMs / SHARE_SOAK.phases.length
    const readings = Math.floor((phaseMs - SHARE_SOAK.warmupMs) / SHARE_SOAK.sampleMs[mode])
    expect(readings).toBeGreaterThanOrEqual(6)
    expect(totalMs).toBe(mode === 'short' ? 2040000 : 3000000)
    expect(totalMs - primingMs - profileMs).toBe(60000)
    expect(totalMs - (primingMs + setupMs + profileMs + transitionMs + cleanupMs + exitMs)).toBe(
      10000
    )
    expect(budget.watchdogMs).toBe(totalMs)
    expect(budget.lifecycleMs).toBe(mode === 'short' ? 2460000 : 3420000)
    expect(budget.lifecycleMs - totalMs).toBe(420000)
  })
})

describe('Share soak resource sources', () => {
  /** Fake owners, so each case wraps and restores its own prototypes. */
  const owners = () => {
    class Run {
      constructor(nodes = new Map()) {
        this.nodes = nodes
      }

      start() {
        return this
      }

      syncNodes(value) {
        return [this, value]
      }
    }
    class Emitter {
      constructor() {
        this.emitter = new EventEmitter()
      }

      addListener(name, listener) {
        this.emitter.on(name, listener)
        return () => this.emitter.off(name, listener)
      }
    }
    class Ledger {
      constructor(counts) {
        this.held = counts
      }

      get counts() {
        return this.held
      }

      invalidate() {
        return this
      }
    }
    return { ShareRun: Run, ShareAckLedger: Ledger, InstrumentationEventEmitter: Emitter }
  }
  const node = (...ledgers) => ({
    loop: { cohorts: new Map([['key', ledgers.map((ledger) => ({ ledger }))]]) },
  })
  let observer
  afterEach(() => observer?.restore())
  const observe = () => {
    const fakes = owners()
    observer = new ShareStateObserver(fakes)
    observer.install()
    return {
      observer,
      ...fakes,
      Ledger: fakes.ShareAckLedger,
      Run: fakes.ShareRun,
      Emitter: fakes.InstrumentationEventEmitter,
    }
  }

  test('sums the sessions, records and ranges of the original runs', () => {
    const { Run, Ledger } = observe()
    const one = new Run(
      new Map([
        [1, node(new Ledger({ records: 2, ranges: 1 }), new Ledger({ records: 1, ranges: 0 }))],
        [2, node()],
      ])
    )
    const two = new Run(new Map([[3, node(new Ledger({ records: 4, ranges: 2 }))]]))
    one.start()
    two.start()
    expect(observer.counts()).toEqual({ shareSessions: 3, shareRecords: 7, shareRanges: 3 })
  })

  test('reads an owner that holds nothing as measured zeros', () => {
    const { Run } = observe()
    expect(observer.counts()).toEqual({ shareSessions: 0, shareRecords: 0, shareRanges: 0 })
    new Run().start()
    expect(observer.counts()).toEqual({ shareSessions: 0, shareRecords: 0, shareRanges: 0 })
  })

  test('still measures a live ledger after its cohort and node have been detached', () => {
    const { Run, Ledger } = observe()
    const ledger = new Ledger({ records: 1, ranges: 1 })
    const original = node(ledger)
    const run = new Run(new Map([[1, original]]))
    run.start()
    ledger.invalidate()
    original.loop.cohorts.clear()
    run.nodes.clear()
    expect(observer.counts()).toEqual({ shareSessions: 0, shareRecords: 1, shareRanges: 1 })
    ledger.held = { records: 0, ranges: 0 }
    expect(observer.counts()).toEqual({ shareSessions: 0, shareRecords: 0, shareRanges: 0 })
    expect(observer.ledgers[0]).toBeInstanceOf(WeakRef)
  })

  test('counts a node that has no loop yet as a session with nothing held', () => {
    const { Run } = observe()
    new Run(new Map([[1, {}]])).start()
    expect(observer.counts()).toEqual({ shareSessions: 1, shareRecords: 0, shareRanges: 0 })
  })

  test('passes every call through unchanged and remembers each run once', () => {
    const { Run } = observe()
    const run = new Run()
    expect(run.start()).toBe(run)
    expect(run.start()).toBe(run)
    expect(run.syncNodes('x')).toEqual([run, 'x'])
    expect(observer.runs).toHaveLength(1)
  })

  test('holds every owner weakly and keeps no client it was not given', () => {
    const { Run, Emitter } = observe()
    new Run().start()
    new Emitter().addListener('event', () => {})
    expect(observer.runs[0]).toBeInstanceOf(WeakRef)
    expect(observer.emitters[0]).toBeInstanceOf(WeakRef)
    expect(observer.tracked.size).toBe(0)
  })

  test.each([
    ['no counts at all', { counts: undefined }],
    ['a negative count', { counts: { records: -1, ranges: 0 } }],
    ['a fractional count', { counts: { records: 0, ranges: 0.5 } }],
    ['a missing range count', { counts: { records: 1 } }],
  ])('refuses a ledger with %s', (_name, ledger) => {
    const { Run } = observe()
    new Run(new Map([[1, node(ledger)]])).start()
    expect(() => observer.counts()).toThrow('Missing share resource source: ShareAckLedger#counts')
  })

  test('refuses a run that shows no node map', () => {
    const { Run } = observe()
    new Run(null).start()
    expect(() => observer.counts()).toThrow('Missing share resource source: ShareRun#nodes')
  })

  test('refuses to wrap anything while the ledger offers no counts accessor', () => {
    class Bare {}
    const fakes = { ...owners(), ShareAckLedger: Bare }
    const before = fakes.ShareRun.prototype.start
    expect(() => new ShareStateObserver(fakes).install()).toThrow(
      'Missing share resource source: ShareAckLedger#counts'
    )
    expect(fakes.ShareRun.prototype.start).toBe(before)
  })

  test('puts every wrapper back, and says when it cannot', () => {
    const fakes = owners()
    const before = fakes.ShareRun.prototype.start
    observer = new ShareStateObserver(fakes)
    observer.install()
    expect(fakes.ShareRun.prototype.start).not.toBe(before)
    observer.restore()
    expect(fakes.ShareRun.prototype.start).toBe(before)

    observer = new ShareStateObserver(fakes)
    observer.install()
    const inner = fakes.ShareRun.prototype.start
    fakes.ShareRun.prototype.start = () => 'another wrapper'
    expect(() => observer.restore()).toThrow('could not be restored')
    fakes.ShareRun.prototype.start = inner
    observer = new ShareStateObserver(fakes)
  })

  test('names listeners by stable emitter and event, so a swap shows where a total does not', () => {
    const { Emitter } = observe()
    const first = new Emitter()
    const second = new Emitter()
    const data = () => {}
    first.addListener('data', data)
    first.addListener('data', () => {})
    const remove = second.addListener('close', () => {})
    expect(observer.named()).toEqual({ 'client1:data': 2, 'client2:close': 1 })
    remove()
    second.addListener('error', () => {})
    expect(observer.named()).toEqual({ 'client1:data': 2, 'client2:error': 1 })
  })

  test('tracks a public client until it reports its disconnect', () => {
    observe()
    const handlers = {}
    const off = jest.fn()
    const client = {
      events: { DISCONNECT: 'disconnect' },
      on: jest.fn((name, listener) => {
        handlers[name] = listener
        return off
      }),
    }
    expect(observer.track(client)).toBe(client)
    expect(observer.tracked.size).toBe(1)
    handlers.disconnect()
    expect(observer.tracked.size).toBe(0)
    expect(off).toHaveBeenCalledTimes(1)
  })

  test('the real client and ledger either offer counts or the observer fails closed', () => {
    const offered =
      typeof Object.getOwnPropertyDescriptor(ShareAckLedger.prototype, 'counts')?.get === 'function'
    if (offered) {
      const shared = instrumentShare(
        { clientId: 'soak', brokers: ['localhost:1'] },
        { activeResources: false }
      )
      shared.shareResources.restore()
      expect(shared.shareResources.snapshot).toEqual(expect.any(Function))
    } else {
      expect(() =>
        instrumentShare({ clientId: 'soak', brokers: ['localhost:1'] }, { activeResources: false })
      ).toThrow('Missing share resource source: ShareAckLedger#counts')
    }
  })

  test('one million offsets of a compact range are one ledger entry, never a million slots', () => {
    const ledger = new ShareAckLedger({
      topic: 'soak',
      topicId: '00000000-0000-4000-8000-000000000001',
      partition: 0,
      token: Symbol('acquisition'),
      sentAt: performance.now(),
      lockDuration: 30000,
      walk: {
        deliver: { records: [], deliveryCounts: new Uint16Array(0) },
        gaps: [{ firstOffset: '0', lastOffset: '999999' }],
        release: [],
        reject: [],
      },
    })
    expect(ledger.size).toBe(1)
  })

  test('the probe of an ordinary client reports no share counts', async () => {
    const { probe } = instrument(
      { clientId: 'soak', brokers: ['localhost:1'] },
      { activeResources: false }
    )
    const snapshot = await probe.snapshot()
    expect(snapshot).not.toHaveProperty('shareSessions')
    expect(snapshot).not.toHaveProperty('shareRecords')
    expect(snapshot).not.toHaveProperty('shareRanges')
  })

  test('a client given a share source reports its counts beside every other reading', async () => {
    const { probe } = instrument(
      { clientId: 'soak', brokers: ['localhost:1'] },
      {
        activeResources: false,
        shareState: () => ({ shareSessions: 1, shareRecords: 2, shareRanges: 3 }),
      }
    )
    expect(await probe.snapshot()).toMatchObject({
      shareSessions: 1,
      shareRecords: 2,
      shareRanges: 3,
      queueDepth: 0,
    })
  })
})

describe('Share soak ShareFetch tap', () => {
  /** One whole request frame: length, header, then `bytes` of body. */
  const frame = (apiKey, correlationId, bytes = 3) => {
    const header = new Encoder()
      .writeInt16(apiKey)
      .writeInt16(2)
      .writeInt32(correlationId)
      .writeString('soak')
      .writeTaggedFields([])
    const inner = Buffer.concat([header.buffer, Buffer.alloc(bytes)])
    const length = Buffer.alloc(4)
    length.writeInt32BE(inner.length)
    return Buffer.concat([length, inner])
  }
  const fakeSocket = () => {
    const written = []
    return {
      destroyed: false,
      written,
      write: jest.fn((data, ...rest) => {
        written.push(Buffer.from(data))
        return rest.length === 0
      }),
      destroy() {
        this.destroyed = true
      },
    }
  }
  const wire = require('../../../testHelpers/wire/shareFetchV2')
  let reader
  beforeEach(() => {
    reader = jest.spyOn(wire, 'readRequest').mockReturnValue({ shareSessionEpoch: 4 })
  })
  afterEach(() => reader.mockRestore())

  test('forwards every byte and return value untouched', () => {
    const tap = new ShareFetchTap()
    const socket = tap.wrap(fakeSocket(), 'node:1')
    const bytes = frame(78, 9)
    expect(socket.write(bytes)).toBe(true)
    expect(socket.write(bytes, () => {})).toBe(false)
    expect(socket.written).toEqual([bytes, bytes])
  })

  test('reads the session epoch of each whole ShareFetch frame, however the writes cut it', () => {
    const tap = new ShareFetchTap()
    const socket = tap.wrap(fakeSocket(), 'node:1')
    const bytes = Buffer.concat([frame(78, 1), frame(18, 2), frame(78, 3)])
    socket.write(bytes.subarray(0, 5))
    expect(tap.facts).toHaveLength(0)
    socket.write(bytes.subarray(5))
    expect(tap.facts.map(({ epoch, endpoint, socket: id }) => [epoch, endpoint, id])).toEqual([
      [4, 'node:1', 1],
      [4, 'node:1', 1],
    ])
    expect(reader).toHaveBeenCalledTimes(2)
  })

  test('never throws into a write: a frame it cannot read is counted and the write still happens', () => {
    const tap = new ShareFetchTap()
    const socket = tap.wrap(fakeSocket(), 'node:1')
    const junk = Buffer.from([0, 0, 0, 2, 1, 2])
    expect(() => socket.write(junk)).not.toThrow()
    expect(socket.written).toEqual([junk])
    expect(tap.faults).toBe(1)
  })

  test('selects only later requests of one endpoint', () => {
    const tap = new ShareFetchTap()
    const a = tap.wrap(fakeSocket(), 'node:1')
    const b = tap.wrap(fakeSocket(), 'node:2')
    a.write(frame(78, 1))
    const mark = tap.mark()
    b.write(frame(78, 2))
    a.write(frame(78, 3))
    expect(tap.since(mark, 'node:1').map(({ socket }) => socket)).toEqual([1])
    expect(tap.since(mark, 'node:2').map(({ socket }) => socket)).toEqual([2])
    expect(tap.since(0, 'node:1')).toHaveLength(2)
  })

  test('keeps only the most recent facts', () => {
    const tap = new ShareFetchTap(3)
    const socket = tap.wrap(fakeSocket(), 'node:1')
    for (let i = 0; i < 5; i++) socket.write(frame(78, i))
    expect(tap.facts.map(({ sequence }) => sequence)).toEqual([3, 4, 5])
  })

  test('destroys the original socket, and refuses one that is gone', () => {
    const tap = new ShareFetchTap()
    const original = fakeSocket()
    tap.wrap(original, 'node:1')
    tap.destroy(1)
    expect(original.destroyed).toBe(true)
    expect(() => tap.destroy(1)).toThrow('already gone')
    expect(() => tap.destroy(99)).toThrow('already gone')
  })

  test('forgets a destroyed socket when the next one is wrapped', () => {
    const tap = new ShareFetchTap()
    const first = tap.wrap(fakeSocket(), 'node:1')
    first.destroy()
    tap.wrap(fakeSocket(), 'node:1')
    expect([...tap.sockets.keys()]).toEqual([2])
  })

  test.each(['close', 'error', 'end'])(
    'reports actual reset socket events with original times, event=%s',
    async (event) => {
      const { createFakeSocketFactory } = require('../../../testHelpers/fakeSocket')
      let now = 10
      const tap = new ShareFetchTap(64, { now: () => now })
      const observed = []
      const unlisten = tap.listen((step, fields, at) => observed.push({ step, ...fields, at }))
      const { factory } = createFakeSocketFactory()
      const connected = jest.fn()
      const socket = tap.factory(factory)({ host: 'h', port: 9, onConnect: connected })
      await new Promise(setImmediate)
      expect(connected).toHaveBeenCalledTimes(1)
      expect(observed.map(({ step }) => step)).toEqual([
        'socket-connect-start',
        'socket-connect-end',
      ])
      const destroy = socket.destroy.bind(socket)
      if (event === 'error')
        socket.destroy = () => destroy(Object.assign(new Error('reset'), { code: 'ECONNRESET' }))
      if (event === 'end')
        socket.destroy = () => {
          socket.emit('end')
          return destroy()
        }
      now = 25
      tap.destroy(1)
      expect(observed.some(({ step }) => step === 'socket-close')).toBe(false)
      await Promise.resolve()
      expect(observed.filter(({ at }) => at === 25).map(({ step }) => step)).toEqual(
        event === 'close' ? ['socket-close'] : [`socket-${event}`, 'socket-close']
      )
      expect(observed.at(-1)).toEqual({
        step: 'socket-close',
        socket: 1,
        endpoint: 'h:9',
        at: 25,
        hadError: event === 'error',
        destroyed: true,
      })
      expect(socket.listenerCount('end')).toBe(0)
      expect(socket.listenerCount('error')).toBe(0)
      unlisten()
      expect(tap.observers.size).toBe(0)
    }
  )

  test('wraps a factory so its sockets are observed', () => {
    const tap = new ShareFetchTap()
    const socket = fakeSocket()
    const made = tap.factory(() => socket)({ host: 'h', port: 9 })
    made.write(frame(78, 5))
    expect(made).toBe(socket)
    expect(tap.facts[0].endpoint).toBe('h:9')
  })
})

const {
  TOPIC_NAME,
  SOAK_UUID,
  exchangeFacts,
  sealExchange,
  heartbeatBytes,
  positionFacts,
  verifierFacts,
  cycleFacts,
} = require('./fixtures/shareCycleFacts')

/** Three established sessions followed by empty incremental responses across a measured window. */
const incrementalVerifierFacts = () => {
  const verifier = verifierFacts()
  verifier.openedAt = 33522
  verifier.closedAt = 43871
  const traces = [
    {
      partition: 0,
      socket: 31,
      open: 215,
      pool: 216,
      times: [
        [38561, 38813],
        [38813, 39064],
        [39064, 39315],
        [39316, 39568],
        [39568, 39820],
        [39820, 40071],
        [40072, 40323],
        [40324, 40575],
        [40576, 40828],
        [40828, 41081],
        [41082, 41333],
        [41334, 41585],
        [41586, 41837],
        [41838, 42090],
        [42091, 42342],
        [42343, 42594],
        [42595, 42846],
        [42847, 43099],
        [43100, 43352],
        [43353, 43604],
        [43605, 43856],
      ],
    },
    {
      partition: 1,
      socket: 29,
      open: 209,
      pool: 210,
      times: [
        [38556, 38809],
        [38810, 39061],
        [39062, 39313],
        [39314, 39566],
        [39567, 39819],
        [39819, 40072],
        [40073, 40324],
        [40325, 40576],
        [40577, 40829],
        [40829, 41082],
        [41083, 41334],
        [41334, 41586],
        [41587, 41838],
        [41839, 42091],
        [42091, 42343],
        [42343, 42595],
        [42596, 42848],
        [42848, 43100],
        [43101, 43353],
        [43354, 43605],
        [43606, 43857],
      ],
    },
    {
      partition: 2,
      socket: 30,
      open: 212,
      pool: 213,
      times: [
        [38557, 38810],
        [38811, 39062],
        [39063, 39314],
        [39315, 39567],
        [39568, 39820],
        [39821, 40073],
        [40074, 40325],
        [40326, 40577],
        [40578, 40829],
        [40830, 41083],
        [41084, 41335],
        [41336, 41587],
        [41588, 41839],
        [41840, 42091],
        [42092, 42344],
        [42344, 42596],
        [42596, 42848],
        [42849, 43101],
        [43102, 43354],
        [43355, 43606],
        [43607, 43857],
      ],
    },
  ]
  verifier.provenance.exchanges = traces
    .flatMap(({ partition, socket, open, pool, times }) =>
      times.map(([at, answeredAt], epoch) => {
        const exchange = exchangeFacts(10000 + partition * 100 + epoch, partition, [])
        Object.assign(exchange, {
          memberId: verifier.memberId,
          open,
          pool,
          endpoint: `node${partition}:9092`,
        })
        const frame = exchange.frames[0]
        Object.assign(frame, {
          at,
          answeredAt,
          socket,
          endpoint: exchange.endpoint,
          correlationId: epoch,
        })
        Object.assign(frame.request, { memberId: verifier.memberId, shareSessionEpoch: epoch })
        if (epoch > 0) {
          frame.request.topics = []
          exchange.response.responses = []
        }
        return sealExchange(exchange)
      })
    )
    .sort((a, b) => a.frames[0].at - b.frames[0].at)
  const assignment = [{ topic: TOPIC_NAME, partitions: [0, 1, 2] }]
  verifier.events = [
    {
      kind: 'GROUP_JOIN',
      at: 33537,
      payload: { groupId: verifier.groupId, memberId: verifier.memberId },
    },
    { kind: 'ASSIGNMENT_CHANGED', at: 38542, payload: { assignment } },
    { kind: 'window-start', at: 38832, wallAt: 1794039558805, payload: assignment },
    ...verifier.provenance.exchanges.map(({ frames: [frame] }) => ({
      kind: 'REQUEST',
      at: frame.answeredAt,
      payload: {
        apiName: 'ShareFetch',
        correlationId: frame.correlationId,
        broker: frame.endpoint,
        sentAt: 1794039519973 + frame.at,
      },
    })),
    { kind: 'window-end', at: 43833, wallAt: 1794039563806, payload: assignment },
    { kind: 'ASSIGNMENT_CHANGED', at: 43858, payload: { assignment: [] } },
    { kind: 'DISCONNECT', at: 43868, payload: {} },
  ].sort((a, b) => a.at - b.at)
  return verifier
}

describe('Share soak cycle oracle', () => {
  test.each([
    [0, 'share-steady', false, '20'],
    [9, 'share-steady', false, '200'],
    [10, 'share-reset', true, '220'],
    [19, 'share-reset', true, '400'],
    [20, 'share-members', false, '420'],
    [29, 'share-members', false, '600'],
  ])(
    'plans cycle %i in %s with a reset of %s and a cumulative end of %s',
    (cycle, phase, reset, end) => {
      const plan = ShareCycleOracle.plan(cycle)
      expect(plan).toMatchObject({ phase, reset, end, perPartition: 20 })
      expect(plan.records).toHaveLength(60)
      expect(plan.records.map(({ sequence }) => sequence)).toEqual(
        Array.from({ length: 60 }, (_, i) => cycle * 60 + i)
      )
      expect(plan.records.map(({ partition }) => partition)).toEqual(
        Array.from({ length: 60 }, (_, i) => i % 3)
      )
      expect(plan.first).toBe(BigInt(20 * cycle))
    }
  )

  test('plans exactly ten reset cycles and thirty cycles of sixty distinct records', () => {
    const plans = Array.from({ length: 30 }, (_, cycle) => ShareCycleOracle.plan(cycle))
    expect(plans.filter(({ reset }) => reset)).toHaveLength(10)
    expect(new Set(plans.flatMap(({ records }) => records.map(({ key }) => key))).size).toBe(1800)
  })

  test.each([0, 10, 29])('accepts a correct cycle %i', (cycle) => {
    expect(ShareCycleOracle.judge(ShareCycleOracle.plan(cycle), cycleFacts(cycle))).toEqual([])
  })

  test.each([
    [
      'missing invocation',
      (beat) => {
        beat.request.memberEpoch = 1
      },
    ],
    [
      'invocation without a write',
      (beat) => {
        beat.frames = []
      },
    ],
    [
      'unanswered write',
      (beat) => {
        delete beat.frames[0].responseBytes
      },
    ],
    [
      'lost socket',
      (beat) => {
        beat.frames[0].lostAt = beat.at
      },
    ],
    [
      'foreign correlation',
      (beat) => {
        beat.frames[0].correlationId++
      },
    ],
    [
      'foreign member',
      (beat) => {
        beat.frames[0].request.memberId = 'other-member'
      },
    ],
  ])('rejects supplied departure booleans with %s', (_name, damage) => {
    const facts = cycleFacts(0)
    damage(facts.provenance.heartbeats[1])
    expect(ShareCycleOracle.judge(ShareCycleOracle.plan(0), facts).join('; ')).toContain('member-A')
  })

  /** Each case damages one independent fact and names the check that must catch it. */
  const CASES = [
    [
      'a record the producer never acknowledged',
      0,
      (o) => o.produced.pop(),
      'acknowledged records',
    ],
    [
      'an acknowledged offset that is off by one',
      0,
      (o) => {
        const tuple = JSON.parse(o.produced[0].identity)
        tuple[2] = '1'
        o.produced[0].identity = JSON.stringify(tuple)
      },
      'differs from the corpus',
    ],
    ['a raw log that lost an identity', 0, (o) => o.log.pop(), 'raw log does not hold'],
    [
      'a raw log holding another cycle in its place',
      1,
      (o) => {
        o.log[0] = cycleFacts(0).log[0]
      },
      'raw log does not hold',
    ],
    [
      'a raw log that is not the cumulative prefix',
      2,
      (o) => {
        o.logs[1] = { end: '20', count: 20 }
      },
      'cumulative prefix',
    ],
    [
      'an identity that never reached a handler',
      0,
      (o) => o.handler.pop(),
      'never reached a handler',
    ],
    [
      'a handler identity outside the corpus',
      0,
      (o) => o.handler.push({ identity: cycleFacts(1).produced[0].identity, deliveryCount: 1 }),
      'outside the corpus',
    ],
    [
      'a duplicate delivery in a steady cycle',
      0,
      (o) => o.handler.push({ ...o.handler[0], deliveryCount: 2 }),
      'outside the frozen held set',
    ],
    [
      'a duplicate delivery outside the frozen held set of a reset cycle',
      10,
      (o) => {
        const outside = o.produced.find(({ partition }) => partition === 1).identity
        o.handler.push({ identity: outside, deliveryCount: 2 })
      },
      'outside the frozen held set',
    ],
    [
      'a replay that repeats its delivery count',
      10,
      (o) => {
        o.handler.at(-1).deliveryCount = 1
      },
      'did not raise its delivery count',
    ],
    ['a held identity that was never redelivered', 10, (o) => o.handler.pop(), 'was not recovered'],
    [
      'a reset cycle with no frozen held set',
      10,
      (o) => {
        o.held = []
      },
      'no held set was frozen',
    ],
    [
      'a held set in a steady cycle',
      0,
      (o) => {
        o.held = [o.produced[0].identity]
      },
      'outside a reset cycle',
    ],
    [
      'an invalid delivery count',
      0,
      (o) => {
        o.handler[0].deliveryCount = 0
      },
      'not a valid acquisition count',
    ],
    [
      'an Accept that covers the previous cycle instead',
      3,
      (o) => {
        o.accepted = o.accepted.map((range) => ({
          ...range,
          firstOffset: String(BigInt(range.firstOffset) - 20n),
          lastOffset: String(BigInt(range.lastOffset) - 20n),
        }))
      },
      'lack a successful Accept',
    ],
    ['no successful Accept at all', 0, (o) => (o.accepted = []), 'lack a successful Accept'],
    [
      'a partition with positive lag',
      0,
      (o) => {
        o.broker[1].lag = '1'
      },
      'partition 1 is not at its terminal position',
    ],
    [
      'a partition whose lag is missing',
      0,
      (o) => {
        delete o.broker[2].lag
      },
      'partition 2 is not at its terminal position',
    ],
    [
      'a start offset beyond the log end',
      0,
      (o) => {
        o.broker[0].startOffset = '21'
      },
      'partition 0 is not at its terminal position',
    ],
    [
      'a log end that is not the cumulative end',
      0,
      (o) => {
        o.broker[0].logEnd = '19'
      },
      'partition 0 is not at its terminal position',
    ],
    [
      'a partition row with an error',
      0,
      (o) => {
        o.broker[0].errorCode = 3
      },
      'partition 0 is not at its terminal position',
    ],
    ['a missing partition row', 0, (o) => o.broker.pop(), 'three distinct terminal partition rows'],
    [
      'a duplicated partition row',
      0,
      (o) => {
        o.broker[2].partition = 1
      },
      'three distinct terminal partition rows',
    ],
    [
      'an additional member that never left',
      0,
      (o) => {
        o.members.bRemoved = false
      },
      'incomplete',
    ],
    [
      'a member that never joined',
      0,
      (o) => {
        o.members.aJoined = false
      },
      'incomplete',
    ],
    ['a reset cycle without its reset', 10, (o) => (o.reset = null), 'no original session reset'],
    [
      'a reset that did not reopen at epoch zero',
      10,
      (o) => {
        o.reset.nextEpoch = 2
      },
      'no original session reset',
    ],
    [
      'a reset that reopened on the same socket',
      10,
      (o) => {
        o.reset.nextSocket = o.reset.oldSocket
      },
      'no original session reset',
    ],
    [
      'a reset of a session that was never established',
      10,
      (o) => {
        o.reset.oldEpoch = 0
      },
      'no original session reset',
    ],
    [
      'a reset in a steady cycle',
      0,
      (o) => {
        o.reset = cycleFacts(10).reset
      },
      'a reset was observed outside a reset cycle',
    ],
    [
      'a verifier that was never active',
      0,
      (o) => {
        o.verifier.active = false
      },
      'inactive or received a record',
    ],
    [
      'a verifier that received a record',
      0,
      (o) => {
        o.verifier.delivered = [o.produced[0].identity]
      },
      'inactive or received a record',
    ],
    [
      'a client still connected',
      0,
      (o) => {
        o.clientsDisconnected = false
      },
      'still connected',
    ],
  ]

  test.each(CASES)('rejects %s', (_name, cycle, damage, expected) => {
    const facts = cycleFacts(cycle)
    damage(facts)
    const violations = ShareCycleOracle.judge(ShareCycleOracle.plan(cycle), facts)
    expect(violations.join('\n')).toContain(expected)
    expect(violations.every((line) => line.startsWith(`cycle ${cycle}: `))).toBe(true)
  })

  test('judges each oracle on its own facts: one damaged oracle names only itself', () => {
    const facts = cycleFacts(0)
    facts.handler.pop()
    const violations = ShareCycleOracle.judge(ShareCycleOracle.plan(0), facts)
    expect(violations).toEqual(['cycle 0: an identity never reached a handler'])
  })
})

/** A resource reading that is quiet in every measured field. */
const quietShare = () => ({
  listeners: { requestQueues: 0, sockets: 0 },
  sockets: { created: 0, destroyed: 0, open: 0 },
  queueDepth: 0,
  ledgerSize: 0,
  activeResources: {},
  activeResourcesBeforeCollection: {},
  shareSessions: 0,
  shareRecords: 0,
  shareRanges: 0,
  shareClients: 0,
  namedListeners: {},
  heapUsed: 1,
})

describe('Share soak owner', () => {
  const settled = { ok: true, violations: [], measured: {} }

  /** A scripted driver: every cluster effect is a double, each cycle returns correct facts. */
  const scripted = ({ alter = () => {}, cycle } = {}) => ({
    completedAt: 123456,
    cleanup: jest.fn(async () => {}),
    prepare: jest.fn(async function (stack) {
      stack.add('scripted cleanup', this.cleanup)
    }),
    openPhase: jest.fn(async () => {}),
    closePhase: jest.fn(async () => {}),
    cycle:
      cycle ??
      jest.fn(async (plan, _scope, timing) => {
        const facts = cycleFacts(plan.cycle, { startedAt: timing.clock.now(), due: timing.due })
        alter(facts, plan)
        return facts
      }),
    final: jest.fn(async (_scope, { clock, due }) =>
      cycleFacts(29, { startedAt: clock.now(), due, kind: 'final' })
    ),
  })

  const build = ({
    driver = scripted(),
    gate = new LiveRun(),
    slotMs,
    soak = {},
    short = true,
    clock,
    idleWakeMs = 0,
  } = {}) => {
    let now = 0
    const timer = clock ?? { now: () => now }
    const cycle = driver.cycle
    driver.cycle = jest.fn(async (...args) => {
      const facts = await cycle(...args)
      if (!clock) now = Math.max(now, facts.timing?.completedAt ?? now)
      return facts
    })
    const final = driver.final
    driver.final = jest.fn(async (...args) => {
      const facts = await final(...args)
      if (!clock) now = Math.max(now, facts.timing?.completedAt ?? now)
      return facts
    })
    const rows = []
    const shares = { snapshot: jest.fn(async () => quietShare()), restore: jest.fn() }
    const owner = new ShareSoak({
      driver,
      kafka: {},
      shares,
      stack: new CleanupStack(),
      short,
      evidence: { write: (row) => rows.push(row), read: () => rows },
      gate,
      pause: async (_scope, ms) => {
        now += ms + idleWakeMs
      },
      clock: timer,
      slotMs,
      soak: { sampleMs: 1000, warmupMs: 0, settleMs: 1, judge: () => settled, ...soak },
    })
    owner.prime = async () => {
      owner.priming = { status: 'passed', topic: 'primed', groupId: 'primed', topicId: 'primed' }
    }
    return { owner, driver, rows, shares, gate }
  }

  afterEach(() => liveRun.reset())

  test.each([true, false])(
    'runs all three phases and separate verifier windows, short=%s',
    async (short) => {
      const { owner, rows } = build({ short })
      await owner.run()
      await owner.close()
      expect(owner.elapsedProfileMs).toBe(short ? 960000 : 1920000)
      expect(
        rows.filter(({ event }) => event === 'soak-cycle').map(({ verifier }) => verifier.windowMs)
      ).toEqual(Array(30).fill(5000))
      expect(owner.final.verifier.windowMs).toBe(5000)
      expect(owner.timing.slots).toHaveLength(30)
    }
  )

  test.each([true, false])(
    'accepts late idle wakes at every slot and phase without moving work cutoffs, short=%s',
    async (short) => {
      const { owner, rows } = build({ short, idleWakeMs: 3 })
      const verdict = await owner.run()
      await owner.close()

      expect(verdict.ok).toBe(true)
      expect(owner.elapsedProfileMs).toBe((short ? 960000 : 1920000) + 9)
      for (const [index, slot] of owner.timing.slots.entries()) {
        const phase = owner.timing.phases[Math.floor(index / 10)]
        expect(slot.due).toBe(phase.startedAt + ((index % 10) + 1) * (short ? 32000 : 64000))
        expect(slot.completedAt).toBe(slot.due + 3)
        expect(slot.startedAt).toBeGreaterThanOrEqual(
          index ? owner.timing.slots[index - 1].completedAt : phase.startedAt
        )
      }
      for (const [index, phase] of owner.timing.phases.entries()) {
        expect(phase.completedAt).toBe(phase.due + 3)
        if (index)
          expect(phase.startedAt).toBeGreaterThanOrEqual(owner.timing.phases[index - 1].completedAt)
      }
      for (const row of rows.filter(({ event }) => event === 'soak-cycle'))
        expect(row.timing.completedAt).toBeLessThanOrEqual(row.timing.due)
      expect(rows.filter(({ event }) => event === 'case-complete')).toHaveLength(1)
    }
  )

  test('all phases share the idle-wake margin and cannot borrow the exit reserve', async () => {
    const { owner, rows, gate } = build({ idleWakeMs: 3334 })
    await expect(owner.run()).rejects.toThrow('shared profile deadline exceeded')
    await owner.close()
    expect(owner.elapsedProfileMs).toBe(970002)
    expect(owner.timing.stages.filter(({ name }) => name === 'profile').at(-1)).toMatchObject({
      due: 970000,
      completedAt: 970002,
    })
    expect(gate.tainted).toBe(true)
    expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
  })

  test('runs three phases of ten cycles in order and records every cycle, reset and the completion', async () => {
    const { owner, driver, rows, shares } = build()
    const verdict = await owner.run()
    await owner.close()

    expect(verdict).toMatchObject({
      ok: true,
      violations: [],
      counts: { cycles: 30, resets: 10, memberAdds: 30, memberRemoves: 30 },
    })
    expect(driver.cycle.mock.calls.map(([plan]) => plan.cycle)).toEqual(
      Array.from({ length: 30 }, (_, cycle) => cycle)
    )
    expect(driver.openPhase).toHaveBeenCalledTimes(3)
    expect(driver.closePhase).toHaveBeenCalledTimes(3)
    expect(rows.filter(({ event }) => event === 'soak-cycle')).toHaveLength(30)
    expect(rows.filter(({ event }) => event === 'soak-reset').map(({ cycle }) => cycle)).toEqual(
      Array.from({ length: 10 }, (_, i) => 10 + i)
    )
    expect(rows.filter(({ event }) => event === 'case-complete')).toHaveLength(1)
    expect(rows.filter(({ event }) => event === 'case-failed')).toEqual([])
    expect(driver.cleanup).toHaveBeenCalledTimes(1)
    expect(shares.restore).toHaveBeenCalledTimes(1)
  })

  test('completes with exact counts and every physical identity once', async () => {
    const { owner, rows } = build()
    await owner.run()
    await owner.close()

    const done = rows.find(({ event }) => event === 'case-complete')
    expect(done).toMatchObject({
      case: SHARE_SOAK_TITLE,
      status: 'passed',
      tainted: false,
      profile: 'tv2',
      profileMs: 960000,
      memberAdds: 30,
      memberRemoves: 30,
      cleanup: { groupsAbsent: true, topicsAbsent: true, completedAt: 123456 },
      resources: {
        before: { openSockets: 0, ledgerSize: 0 },
        after: { openSockets: 0, ledgerSize: 0 },
      },
    })
    expect(done.oracles.log.expected).toHaveLength(1800)
    expect(new Set(done.oracles.log.actual).size).toBe(1800)
    expect(done.oracles.handler.actual).toHaveLength(2000)
    expect(done.oracles.broker).toHaveLength(3)
  })

  test('records every resource line as a soak-resources row with the generic verdict kept', async () => {
    const { owner, rows } = build()
    await owner.run()
    await owner.close()

    const kinds = rows.filter(({ event }) => event === 'soak-resources').map(({ kind }) => kind)
    expect(kinds[0]).toBe('baseline')
    expect(kinds.filter((kind) => kind === 'phase-end')).toHaveLength(3)
    expect(kinds.slice(-2)).toEqual(['teardown', 'resource-verdict'])
  })

  test('joins the sampler before each quiet reading', async () => {
    const driver = scripted({
      cycle: jest.fn(async (plan, _scope, { clock, due }) => {
        await new Promise((resolve) => setTimeout(resolve, 8))
        return cycleFacts(plan.cycle, { startedAt: clock.now(), due })
      }),
    })
    const { owner, rows } = build({ driver, soak: { sampleMs: 1 } })
    await owner.run()
    await owner.close()

    const lines = rows.filter(({ event }) => event === 'soak-resources')
    expect(lines.some(({ kind }) => kind === 'sample')).toBe(true)
    for (const phase of ['share-steady', 'share-reset', 'share-members']) {
      const end = lines.findIndex((line) => line.kind === 'phase-end' && line.phase === phase)
      expect(lines.slice(end).some((line) => line.kind === 'sample' && line.phase === phase)).toBe(
        false
      )
    }
  })

  test('archives timer attribution but retains only the share screen inputs in sample history', async () => {
    const { ShareTimerObserver } = require('../shareTimerObserver')
    const observer = new ShareTimerObserver()
    const driver = scripted({
      cycle: async (plan, _scope, { clock, due }) => {
        await new Promise((resolve) => setTimeout(resolve, 8))
        return cycleFacts(plan.cycle, { startedAt: clock.now(), due })
      },
    })
    const { owner, rows, shares } = build({ driver, soak: { sampleMs: 1 } })
    observer.install()
    const timer = setTimeout(() => {}, 60000)
    let reading = 0
    shares.snapshot = async () => ({
      ...quietShare(),
      heapUsed: 10000000 + reading++ * 1048576,
      shareRecords: 301,
      shareRanges: 7,
      shareSessions: 2,
      timerOwners: {
        beforeCollection: observer.snapshot(),
        afterCollection: observer.snapshot(),
      },
    })
    const soak = owner.soak()
    owner.soak = () => soak
    try {
      await owner.run()
      const archived = rows.filter(({ kind }) => kind === 'sample')
      expect(archived.length).toBeGreaterThanOrEqual(6)
      expect(soak.samples).toHaveLength(archived.length)
      expect(archived.every(({ snapshot }) => Number.isFinite(snapshot.heapAtMs))).toBe(true)
      for (const [i, sample] of soak.samples.entries()) {
        expect(sample).toEqual({
          phase: archived[i].phase,
          atMs: archived[i].atMs,
          phaseMs: archived[i].phaseMs,
          admittedAtMs: archived[i].admittedAtMs,
          admittedPhaseMs: archived[i].admittedPhaseMs,
          snapshot: {
            heapUsed: archived[i].snapshot.heapUsed,
            shareRecords: 301,
            shareRanges: 7,
            shareSessions: 2,
          },
        })
        expect(archived[i].snapshot.timerOwners.beforeCollection.rows).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              type: 'Timeout',
              durationMs: 60000,
              stack: expect.stringContaining('soak.spec.js'),
            }),
          ])
        )
      }
      for (const { phase } of soak.phaseEnds) {
        const fullViolations = []
        const retainedViolations = []
        const run = { samples: archived, warmupMs: 0 }
        expect(
          ShareSoakScreen.phase(phase, { ...run, samples: soak.samples }, retainedViolations)
        ).toEqual(ShareSoakScreen.phase(phase, run, fullViolations))
        expect(retainedViolations).toEqual(fullViolations)
        expect(retainedViolations).toContain(`${phase}: held-record ceiling`)
        expect(retainedViolations).toContain(`${phase}: retained-heap screen`)
      }
      expect(soak.phaseEnds[0].snapshot.timerOwners.beforeCollection.rows.length).toBeGreaterThan(0)
    } finally {
      clearTimeout(timer)
      observer.restore()
      await owner.close()
    }
  })

  test('archives observation times from the translated original owner clock, including a failed verdict', async () => {
    const origin = 7654321
    let now = origin
    const elapsed = [17, 343711, 691553, 1021727, 1023819]
    const { owner, rows, shares } = build({ clock: { now: () => now } })
    owner.startedAt = origin
    let reading = 0
    shares.snapshot = async () => {
      now = origin + elapsed[reading++]
      await Promise.resolve()
      return { ...quietShare(), heapUsed: 10000000 + reading * 2000000 }
    }
    const soak = owner.soak()
    soak.phases = SHARE_SOAK.phases.map((phase) => ({
      name: `share-${phase}`,
      workload: 'observed',
      ms: 1,
    }))
    soak.workloads = { observed: async () => {} }
    soak.transition = (work) => work()
    soak.beforeTeardown = async () => {}
    soak.settleMs = 0
    soak.judge = ShareSoakScreen.evaluate
    const result = await soak.run()
    const observations = rows.filter(({ kind }) =>
      ['baseline', 'phase-end', 'teardown'].includes(kind)
    )
    expect(observations.map(({ snapshot }) => snapshot.heapAtMs)).toEqual(elapsed)
    expect(owner.startedAt).toBe(origin)
    expect(result.ok).toBe(false)
    expect(result.violations).toContain('phase-end quiet heap slope screen')
    expect(rows.find(({ kind }) => kind === 'resource-verdict').measured.quietSlope).toEqual({
      points: observations.slice(1, 4).map(({ phase, snapshot }) => ({
        phase,
        heapAtMs: snapshot.heapAtMs,
        heapUsed: snapshot.heapUsed,
      })),
      bytesPerMinute: result.measured.quietSlope.bytesPerMinute,
    })
    expect(result.measured.quietSlope.bytesPerMinute).toBeGreaterThan(98304)
  })

  test('judges the run with the per-phase screen by default', () => {
    const { owner } = build()
    owner.soakOptions = {}
    expect(owner.soak().judge).toBe(ShareSoakScreen.evaluate)
  })

  test('registers its cleanup before it creates anything, and runs it once on close after a setup failure', async () => {
    const order = []
    const driver = scripted()
    driver.prepare = jest.fn(async function (stack) {
      stack.add('scripted cleanup', this.cleanup)
      order.push('registered')
      order.push('creating')
      throw new Error('the topic could not be created')
    })
    const { owner, rows, shares, gate } = build({ driver })

    await expect(owner.run()).rejects.toThrow('the topic could not be created')
    expect(order).toEqual(['registered', 'creating'])
    expect(driver.cleanup).not.toHaveBeenCalled()
    await owner.close()
    await owner.close()
    expect(driver.cleanup).toHaveBeenCalledTimes(1)
    expect(shares.restore).toHaveBeenCalledTimes(1)
    expect(gate.tainted).toBe(true)
    expect(rows.find(({ event }) => event === 'case-failed').error.message).toMatch('topic')
    expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
  })

  test.each([
    [
      'a missing reset',
      (facts, plan) => {
        if (plan.cycle === 10) facts.reset = null
      },
      'no original session reset',
      10,
    ],
    [
      'a duplicate outside the frozen held set',
      (facts, plan) => {
        if (plan.cycle === 3) facts.handler.push({ ...facts.handler[0], deliveryCount: 2 })
      },
      'outside the frozen held set',
      3,
    ],
    [
      'a silent verifier',
      (facts, plan) => {
        if (plan.cycle === 5) facts.verifier.active = false
      },
      'inactive or received a record',
      5,
    ],
    [
      'a member that stayed in the group',
      (facts, plan) => {
        if (plan.cycle === 22) facts.members.bRemoved = false
      },
      'incomplete',
      22,
    ],
  ])(
    'stops at %s, records that cycle as failed and never completes',
    async (_name, alter, message, at) => {
      const { owner, rows, gate } = build({ driver: scripted({ alter }) })

      await expect(owner.run()).rejects.toThrow(message)
      await owner.close()
      expect(owner.counts.cycles).toBe(at)
      expect(rows.filter(({ event }) => event === 'soak-cycle').at(-1)).toMatchObject({
        cycle: at,
        status: 'failed',
      })
      expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
      expect(gate.tainted).toBe(true)
    }
  )

  test('a cycle that cannot finish inside its slot fails the scenario and taints the run, and the next case refuses before work', async () => {
    const hang = scripted({ cycle: jest.fn(() => new Promise(() => {})) })
    const first = build({ driver: hang, slotMs: 30 })

    await expect(first.owner.run()).rejects.toThrow('Cycle 0 did not settle within 31 ms')
    await first.owner.close()
    expect(first.gate.tainted).toBe(true)

    const next = build({ gate: first.gate })
    await expect(next.owner.run()).rejects.toThrow('An earlier case may have left work running')
    expect(next.driver.prepare).not.toHaveBeenCalled()
  })

  test('starts no step once the scenario scope is aborted', async () => {
    const { owner } = build()
    const step = jest.fn(async () => {})
    owner.scope = AbortSignal.abort(new Error('the scenario expired'))
    await expect(owner.within(10, step, 'A step')).rejects.toThrow('the scenario expired')
    expect(step).not.toHaveBeenCalled()
  })

  test('refuses a phase of another length and a fourth phase', async () => {
    const { owner, driver } = build()
    await expect(owner.runPhase(320001)).rejects.toThrow('is not the declared 320000 ms')
    expect(driver.openPhase).not.toHaveBeenCalled()
    for (let i = 0; i < 3; i++) await owner.runPhase(320000)
    await expect(owner.runPhase(320000)).rejects.toThrow('Every share phase already ran')
  })

  test('disconnects the phase clients when a cycle fails', async () => {
    const { owner, driver } = build({
      driver: scripted({ cycle: jest.fn(async () => Promise.reject(new Error('cycle failed'))) }),
    })
    await expect(owner.runPhase(320000)).rejects.toThrow('cycle failed')
    expect(driver.closePhase).toHaveBeenCalledTimes(1)
  })

  test('rejects actual slot overrun before the abort timer fires and never admits the next cycle', async () => {
    let now = 0
    const driver = scripted({
      cycle: jest.fn(async (plan) => {
        now += 32001
        return cycleFacts(plan.cycle)
      }),
    })
    const { owner } = build({ driver, clock: { now: () => now } })
    await expect(owner.run()).rejects.toThrow('absolute cutoff')
    expect(driver.cycle).toHaveBeenCalledTimes(1)
    expect(owner.timing.slots[0]).toMatchObject({ due: 32000, completedAt: 32001 })
    await owner.close()
  })

  test('accepts and counts work completed before the cutoff when the idle pause wakes late', async () => {
    let now = 0
    const driver = scripted({
      alter: (facts) => {
        now = facts.timing.due - 6000
        facts.timing.completedAt = now
      },
    })
    const { owner, rows } = build({ driver, clock: { now: () => now } })
    owner.pause = jest.fn(async (_scope, ms) => {
      now += ms + 3
    })

    await expect(owner.runCycle(0)).resolves.toBeUndefined()
    expect(owner.pause).toHaveBeenCalledTimes(1)
    expect(owner.pause).toHaveBeenCalledWith(expect.any(AbortSignal), 6000)
    expect(owner.counts).toEqual({ cycles: 1, resets: 0, memberAdds: 1, memberRemoves: 1 })
    expect(rows.filter(({ event }) => event === 'soak-cycle')).toEqual([
      expect.objectContaining({
        cycle: 0,
        status: 'passed',
        timing: expect.objectContaining({ due: 32000, completedAt: 26000 }),
      }),
    ])
    expect(owner.timing.slots).toEqual([{ cycle: 0, startedAt: 0, due: 32000, completedAt: 32003 }])
  })

  test.each(['before', 'after'])(
    'rejects work completed after the cutoff %s verification without counting or idling',
    async (when) => {
      let now = 0
      const driver = scripted({
        alter: (facts) => {
          now = when === 'before' ? facts.timing.due + 1 : facts.timing.completedAt
        },
      })
      const { owner } = build({ driver, clock: { now: () => now } })
      const verifyCycle = owner.verifyCycle.bind(owner)
      const verify = jest.spyOn(owner, 'verifyCycle').mockImplementation((plan, facts) => {
        const violations = verifyCycle(plan, facts)
        if (when === 'after') now = facts.timing.due + 1
        return violations
      })
      owner.pause = jest.fn()

      await expect(owner.runCycle(0)).rejects.toThrow('Cycle 0 completed after its absolute cutoff')
      expect(verify).toHaveBeenCalledTimes(when === 'before' ? 0 : 1)
      expect(driver.cycle).toHaveBeenCalledTimes(1)
      expect(owner.counts).toEqual({ cycles: 0, resets: 0, memberAdds: 0, memberRemoves: 0 })
      expect(owner.pause).not.toHaveBeenCalled()
      expect(owner.timing.slots).toEqual([
        { cycle: 0, startedAt: 0, due: 32000, completedAt: 32001 },
      ])
    }
  )

  test('several individually small transitions consume one cumulative allowance', async () => {
    let now = 0
    const { owner } = build({ clock: { now: () => now } })
    await owner.stage('transition', async () => {
      now += 2000
    })
    await owner.stage('transition', async () => {
      now += 2000
    })
    await expect(
      owner.stage('transition', async () => {
        now += 1001
      })
    ).rejects.toThrow('shared transition deadline exceeded')
    expect(owner.spent.transition).toBe(5001)
  })

  test('final proof and cleanup share one absolute deadline without borrowing exit time', async () => {
    let now = 0
    const driver = scripted()
    driver.final = async () => {
      now += 15000
      return cycleFacts(29)
    }
    const { owner } = build({ driver, clock: { now: () => now } })
    owner.stack.add('late cleanup', async () => {
      now += 5001
    })
    await expect(owner.finish()).rejects.toThrow('shared cleanup deadline exceeded')
    expect(owner.cleanupDue).toBe(20000)
    const next = jest.fn()
    await expect(owner.stage('cleanup', next)).rejects.toThrow('exhausted')
    expect(next).not.toHaveBeenCalled()
  })

  test('a late cycle cannot move the anchored cutoffs of later slots', async () => {
    let now = 0
    const { owner } = build({ clock: { now: () => now } })
    owner.phaseStartedAt = 0
    owner.phaseDue = 320000
    now = 32001
    await expect(owner.runCycle(0)).rejects.toThrow('deadline exhausted')
    expect(owner.driver.cycle).not.toHaveBeenCalled()
  })

  describe('the combined verdict', () => {
    /**
     * Supplies complete original cycle, timing and final evidence to the real completion decision.
     * @param {Function} [mutate] optional evidence corruption before the decision
     * @param {number} [now=967000] original monotonic decision time
     * @param {object} [resources=settled] resource verdict from the selected screen
     * @returns {{ verdict: object, rows: object[] }} actual verdict and archived rows
     * @throws {Error} when the original completion decision or evidence sink throws
     */
    const complete = (mutate, now = 967000, resources = settled) => {
      const { owner, rows } = build({ clock: { now: () => now } })
      for (let cycle = 0; cycle < 30; cycle++) {
        const facts = cycleFacts(cycle)
        owner.recordCycle(ShareCycleOracle.plan(cycle), facts, [])
      }
      owner.counts = { cycles: 30, resets: 10, memberAdds: 30, memberRemoves: 30 }
      owner.cleanupDue = 980000
      owner.final = cycleFacts(29, { startedAt: 960000, due: 980000, kind: 'final' })
      owner.elapsedProfileMs = SHARE_SOAK.shortMs
      owner.timing.phases = [0, 1, 2].map((phase) => ({
        phase,
        startedAt: phase * 320000,
        due: (phase + 1) * 320000,
        completedAt: (phase + 1) * 320000,
      }))
      owner.timing.slots = Array.from({ length: 30 }, (_, cycle) => ({
        cycle,
        startedAt: cycle * 32000,
        due: (cycle + 1) * 32000,
        completedAt: (cycle + 1) * 32000,
      }))
      owner.timing.stages = owner.timing.phases.map((phase) => ({ ...phase, name: 'profile' }))
      owner.baseline = quietShare()
      owner.teardown = quietShare()
      owner.priming = { status: 'passed' }
      mutate?.(owner, rows)
      return { verdict: owner.complete(resources), rows }
    }

    test('a complete scored trace cannot pass without its first-use boundary', () => {
      const { verdict, rows } = complete((owner) => {
        delete owner.priming
      })
      expect(verdict.violations).toContain('A successful joined priming boundary is required')
      expect(rows.filter(({ event }) => event === 'case-complete')).toHaveLength(0)
    })

    test('passes only when every count, identity and final proof holds', () => {
      const { verdict, rows } = complete()
      expect(verdict.ok).toBe(true)
      expect(rows.filter(({ event }) => event === 'case-complete')).toHaveLength(1)
    })

    test.each(['clean', 'destroyed-socket listener', 'dropped inventory'])(
      'the real completion decision handles a %s resource verdict with complete cycle evidence',
      (damage) => {
        const baseline = quietShare()
        const teardown = quietShare()
        if (damage === 'destroyed-socket listener') teardown.namedListeners['socket1:retained'] = 1
        if (damage === 'dropped inventory')
          teardown.timerOwners = {
            beforeCollection: { rows: [], dropped: 1 },
            afterCollection: { rows: [], dropped: 1 },
          }
        expect(teardown.activeResources).toEqual(baseline.activeResources)
        expect(teardown.sockets.open).toBe(0)
        const violations = []
        ShareSoakScreen.quiet(teardown, baseline, 'teardown', violations)
        const resources = { ok: violations.length === 0, violations, measured: {} }
        const { verdict, rows } = complete(
          (owner) => {
            owner.baseline = baseline
            owner.teardown = teardown
          },
          967000,
          resources
        )
        expect(verdict.resources).toBe(resources)
        expect(rows.filter(({ event }) => event === 'soak-cycle')).toHaveLength(30)
        expect(verdict.ok).toBe(damage === 'clean')
        expect(verdict.violations).toEqual(violations)
        const receipts = rows.filter(({ event }) => event === 'case-complete')
        if (damage === 'clean') {
          expect(violations).toEqual([])
          expect(receipts).toHaveLength(1)
          expect(receipts[0]).toMatchObject({ status: 'passed', tainted: false })
          expect(receipts[0].oracles.log.actual).toHaveLength(1800)
        } else {
          expect(violations).toContain('teardown: resources differ or a source is absent')
          expect(receipts).toEqual([])
        }
      }
    )

    test('accepts exactly the unallocated margin with original anchored deadlines', () => {
      const { verdict, rows } = complete((o) => {
        o.elapsedProfileMs = 970000
        o.timing.phases[2].completedAt = 970000
        o.timing.slots[29].completedAt = 970000
        Object.assign(o.timing.stages[2], { due: 970000, completedAt: 970000 })
        o.final = cycleFacts(29, { startedAt: 970000, due: 980000, kind: 'final' })
      }, 977000)
      expect(verdict).toMatchObject({ ok: true, violations: [] })
      expect(rows.filter(({ event }) => event === 'case-complete')).toHaveLength(1)
    })

    test.each([
      [
        'thirty replays of an old verifier window',
        (o, rows) => {
          const cycles = rows.filter(({ event }) => event === 'soak-cycle')
          for (const row of cycles) row.verifier = cycles[0].verifier
          o.final.verifier = cycles[0].verifier
        },
        'original cycle window',
      ],
      [
        'a final observation copied from the last cycle',
        (o, rows) => {
          const row = rows.filter(({ event }) => event === 'soak-cycle').at(-1)
          o.final.verifier = row.verifier
          o.final.positions = row.positions
          o.final.timing = row.timing
        },
        'distinct post-profile',
      ],
      [
        'a slot overlapping the next start',
        (o) => {
          o.timing.slots[5].completedAt++
        },
        'original slot, phase',
      ],
      [
        'late work hidden inside an allowed idle wake',
        (o, rows) => {
          o.timing.slots[0].completedAt += 3
          const row = rows.find(({ event }) => event === 'soak-cycle')
          row.timing.completedAt = row.timing.due + 1
        },
        'cycle 0: the proof is outside its original slot',
      ],
      [
        'a slot due drifting from its phase anchor',
        (o) => o.timing.slots[5].due++,
        'the original slot, phase or stage timestamps do not prove the fixed schedule',
      ],
      [
        'a profile shorter than its declared duration',
        (o) => o.elapsedProfileMs--,
        'the actual uncapped profile duration does not reach the declared schedule',
      ],
      [
        'profile overshoot beyond the unallocated margin',
        (o) => (o.elapsedProfileMs += 10001),
        'the actual uncapped profile overshoot exceeds the 10000 ms unallocated margin',
      ],
      [
        'a slot whose padding ends before its boundary',
        (o) => o.timing.slots[5].completedAt--,
        'the original slot, phase or stage timestamps do not prove the fixed schedule',
      ],
      [
        'a phase overlapping the next phase start',
        (o) => o.timing.phases[0].completedAt++,
        'the original slot, phase or stage timestamps do not prove the fixed schedule',
      ],
      [
        'non-finite uncapped profile time',
        (o) => (o.elapsedProfileMs = NaN),
        'the actual uncapped profile duration does not reach the declared schedule',
      ],
      [
        'a whole profile with no slot chronology',
        (o) => {
          o.timing.slots = []
        },
        'fixed schedule',
      ],
      [
        'nine real resets that a summary would round to ten',
        (o) => (o.counts.resets = 9),
        'counted',
      ],
      ['twenty-nine member adds', (o) => (o.counts.memberAdds = 29), 'counted'],
      [
        'twenty-nine cycle rows',
        (o, rows) =>
          rows.splice(
            rows.findIndex(({ event }) => event === 'soak-cycle'),
            1
          ),
        'passed cycle rows',
      ],
      [
        'a duplicated physical identity',
        (o, rows) => {
          const [first, second] = rows.filter(({ event }) => event === 'soak-cycle')
          second.oracles.log.expected[0] = first.oracles.log.expected[0]
        },
        '1800 distinct',
      ],
      [
        'a final position that is not the last cumulative end',
        (o) => (o.final.broker[0].logEnd = '599'),
        'final: partition 0',
      ],
      [
        'an inactive final verifier',
        (o) => (o.final.verifier = { active: false, delivered: [] }),
        'final same-group verifier',
      ],
      [
        'a cleanup proof that never completed',
        (o) => (o.driver.completedAt = null),
        'cleanup proof',
      ],
    ])('refuses %s and writes no completion', (_name, mutate, message) => {
      const { verdict, rows } = complete(mutate)
      expect(verdict.ok).toBe(false)
      expect(verdict.violations.join('\n')).toContain(message)
      expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
    })
  })
})

describe('Share soak main', () => {
  const exposed = globalThis.gc
  beforeEach(() => {
    globalThis.gc = () => {}
  })
  afterEach(() => {
    globalThis.gc = exposed
  })

  test.each([
    ['share-consumer', false],
    ['share-consumer-short', true],
  ])('%s is built once as one owner, run and closed', async (profile, short) => {
    const owner = {
      run: jest.fn(async () => ({ ok: true })),
      close: jest.fn(async () => {}),
    }
    const build = jest.fn(() => owner)
    const churn = jest.fn()
    const consumerChurn = jest.fn()

    const verdict = await main(profile, {
      brokers: ['b:1'],
      shareSoak: build,
      churn,
      consumerChurn,
    })

    expect(verdict).toEqual({ ok: true })
    expect(build).toHaveBeenCalledTimes(1)
    expect(build).toHaveBeenCalledWith({ brokers: ['b:1'], short })
    expect(churn).not.toHaveBeenCalled()
    expect(consumerChurn).not.toHaveBeenCalled()
    expect(owner.close).toHaveBeenCalledTimes(1)
  })

  test('closes the owner when its run fails', async () => {
    const owner = {
      run: jest.fn(async () => Promise.reject(new Error('the soak failed'))),
      close: jest.fn(async () => {}),
    }
    await expect(main('share-consumer-short', { shareSoak: () => owner })).rejects.toThrow(
      'the soak failed'
    )
    expect(owner.close).toHaveBeenCalledTimes(1)
  })

  test('refuses a phase length before it builds an owner', async () => {
    const build = jest.fn()
    await expect(main('share-consumer', { phaseSeconds: 5, shareSoak: build })).rejects.toThrow(
      'no phase length'
    )
    expect(build).not.toHaveBeenCalled()
  })
})

describe('Share soak archived primitive controls', () => {
  const violations = (facts, cycle = 10) =>
    ShareCycleOracle.judge(ShareCycleOracle.plan(cycle), facts).join('\n')

  test('rejects redispatch of one original acquisition even with a fabricated cohort and raised count', () => {
    const facts = cycleFacts(10)
    const original = facts.handler[0]
    const replay = facts.handler.find(
      (entry, index) => index > 0 && entry.identity === original.identity
    )
    const source = facts.provenance.acquisitions.find(({ id }) => id === original.acquisition)
    const fabricated = facts.provenance.acquisitions.find(({ id }) => id === replay.acquisition)
    fabricated.exchange = source.exchange
    expect(violations(facts)).toContain('one original acquisition was dispatched more than once')
  })

  test.each([
    ['missing acquisition', (o) => o.provenance.acquisitions.shift(), 'acquisition provenance'],
    [
      'unwritten Accept',
      (o) => {
        o.provenance.exchanges.find((e) => e.tickets.length).frames = []
      },
      'original request and partition receipt',
    ],
    [
      'lost Accept response',
      (o) => {
        delete o.provenance.exchanges.find((e) => e.tickets.length).frames[0].responseBytes
      },
      'original request and partition receipt',
    ],
    [
      'foreign receipt authority',
      (o) => {
        o.provenance.exchanges.find((e) => e.tickets.length).generation++
      },
      'original request and partition receipt',
    ],
    [
      'wrong receipt partition',
      (o) => {
        o.provenance.exchanges.find(
          (e) => e.tickets.length
        ).response.responses[0].partitions[0].partitionIndex = 99
      },
      'original request and partition receipt',
    ],
    [
      'duplicated public receipt',
      (o) => o.accepted.push({ ...o.accepted[0] }),
      'unique original request',
    ],
    [
      'listing disagreement',
      (o) => {
        o.positions[1].all[0].partitions[0].leaderEpoch++
      },
      'terminal listings',
    ],
    [
      'foreign group',
      (o) => {
        o.positions[1].target.groupId = 'other'
      },
      'another group or topic',
    ],
    [
      'foreign UUID',
      (o) => {
        o.positions[1].all[0].topicId = '00000000-0000-4000-8000-000000000002'
      },
      'terminal listings',
    ],
    [
      'changed earlier log identity',
      (o) => {
        o.prefix[0] = o.prefix[1]
      },
      'cumulative raw log identities',
    ],
  ])('rejects %s with unrelated streams intact', (_name, damage, expected) => {
    const facts = cycleFacts(10)
    damage(facts)
    expect(violations(facts)).toContain(expected)
  })

  test.each([1000, 4999])(
    'rejects a measured %i ms window even with a 5000 ms declaration',
    (ms) => {
      const facts = cycleFacts(10)
      facts.verifier.events.find(({ kind }) => kind === 'window-end').at =
        facts.verifier.events.find(({ kind }) => kind === 'window-start').at + ms
      expect(violations(facts)).toContain('measured observation window')
    }
  )

  test.each([
    [
      'pre-window fetch only',
      (v) => {
        v.events
          .filter(({ kind }) => kind === 'REQUEST')
          .forEach(({ payload }) => {
            payload.sentAt = 9999
          })
      },
    ],
    [
      'no fetch',
      (v) => {
        v.events = v.events.filter(({ kind }) => kind !== 'REQUEST')
      },
    ],
    [
      'partial assignment',
      (v) => {
        v.events.find(({ kind }) => kind === 'window-start').payload[0].partitions.pop()
      },
    ],
    [
      'assignment loss',
      (v) => {
        v.events.splice(2, 0, {
          kind: 'ASSIGNMENT_CHANGED',
          at: v.openedAt + 500,
          payload: { assignment: [] },
        })
      },
    ],
    [
      'missing closedAt',
      (v) => {
        delete v.closedAt
      },
    ],
    [
      'nonfinite openedAt',
      (v) => {
        v.openedAt = NaN
      },
    ],
    [
      'reversed close',
      (v) => {
        v.closedAt = v.openedAt - 1
      },
    ],
    [
      'incomplete fetched partitions',
      (v) => {
        v.provenance.exchanges.pop()
      },
    ],
    [
      'mismatched response correlation',
      (v) => {
        v.provenance.exchanges[0].frames[0].correlationId++
      },
    ],
    [
      'wrong original group',
      (v) => {
        v.provenance.exchanges[0].groupId = 'other'
      },
    ],
    [
      'wrong original member',
      (v) => {
        v.memberId = 'other'
      },
    ],
    [
      'wrong original UUID',
      (v) => {
        v.topicId = '00000000-0000-4000-8000-000000000002'
      },
    ],
    [
      'missing joined leave',
      (v) => {
        v.events = v.events.filter(({ kind }) => kind !== 'DISCONNECT')
      },
    ],
  ])('rejects %s despite an active summary', (_name, damage) => {
    const facts = cycleFacts(10)
    damage(facts.verifier)
    expect(violations(facts)).toContain('measured observation window')
  })

  test('archives counts, acquisition links, raw listings and verifier chronology without reducing them', () => {
    const rows = []
    const facts = cycleFacts(10)
    const owner = new ShareSoak({
      driver: {},
      short: true,
      evidence: { write: (row) => rows.push(row) },
    })
    owner.recordCycle(ShareCycleOracle.plan(10), facts, [])
    expect(rows[0].handler).toEqual(facts.handler)
    expect(rows[0].provenance).toEqual(facts.provenance)
    expect(rows[0].positions).toEqual(facts.positions)
    expect(rows[0].verifier.events).toEqual(facts.verifier.events)
  })
})

describe('Share soak incremental verifier evidence', () => {
  const violations = (verifier) => {
    const failures = []
    ShareCycleOracle.verifier(verifier, 5000, (message) => failures.push(message))
    return failures
  }
  const partitionTwo = (verifier) =>
    verifier.provenance.exchanges.filter(({ frames: [frame] }) => frame.socket === 30)

  test.each(['cycle', 'final'])(
    'accepts three established sessions with only empty in-window deltas, %s',
    (kind) => {
      const verifier = incrementalVerifierFacts()
      verifier.kind = kind
      expect(violations(verifier)).toEqual([])
      const start = verifier.events.find(({ kind }) => kind === 'window-start')
      const end = verifier.events.find(({ kind }) => kind === 'window-end')
      expect(end.at - start.at).toBe(5001)
      for (const socket of [29, 30, 31]) {
        const exchanges = verifier.provenance.exchanges.filter((e) => e.frames[0].socket === socket)
        expect(exchanges).toHaveLength(21)
        expect(exchanges[0].frames[0].answeredAt).toBeLessThan(start.at)
        expect(exchanges[1].frames[0].at).toBeLessThan(start.at)
        expect(exchanges[1].frames[0].answeredAt).toBeGreaterThan(start.at)
        const inside = exchanges.filter(
          ({ frames: [frame] }) => frame.at >= start.at && frame.answeredAt <= end.at
        )
        expect(inside).toHaveLength(18)
        for (const exchange of inside) {
          expect(exchange.frames[0].request.topics).toEqual([])
          expect(exchange.response.responses).toEqual([])
          expect(
            ShareCycleOracle.partitionResponse(exchange, { topicId: SOAK_UUID, partition: 2 })
          ).toBeNull()
        }
      }
    }
  )

  test.each([
    [
      'no in-window exchange',
      (v) => {
        v.provenance.exchanges = v.provenance.exchanges.filter(
          (e) => e.frames[0].socket !== 30 || e.frames[0].request.shareSessionEpoch < 2
        )
      },
    ],
    [
      'missing epoch-zero ownership',
      (v) => {
        v.provenance.exchanges = v.provenance.exchanges.filter(
          (e) => e.frames[0].socket !== 30 || e.frames[0].request.shareSessionEpoch !== 0
        )
      },
    ],
    [
      'unobserved session epoch',
      (v) => {
        v.provenance.exchanges = v.provenance.exchanges.filter(
          (e) => e.frames[0].socket !== 30 || e.frames[0].request.shareSessionEpoch !== 1
        )
      },
    ],
    [
      'another socket after establishment',
      (v) => {
        partitionTwo(v)
          .slice(1)
          .forEach((e) => {
            e.frames[0].socket = 999
          })
      },
    ],
    [
      'another open after establishment',
      (v) => {
        partitionTwo(v)
          .slice(1)
          .forEach((e) => {
            e.open = 999
          })
      },
    ],
    [
      'another pool generation after establishment',
      (v) => {
        partitionTwo(v)
          .slice(1)
          .forEach((e) => {
            e.generation++
          })
      },
    ],
    [
      'forgotten partition',
      (v) => {
        const exchange = partitionTwo(v)[1]
        exchange.frames[0].request.forgottenTopicsData = [{ topicId: SOAK_UUID, partitions: [2] }]
        sealExchange(exchange)
      },
    ],
    [
      'decoded empty response conceals wire error',
      (v) => {
        partitionTwo(v)
          .slice(2)
          .forEach((e) => {
            e.response.errorCode = 123
            sealExchange(e)
            e.response.errorCode = 0
          })
      },
    ],
    [
      'decoded empty response conceals wire partition',
      (v) => {
        partitionTwo(v)
          .slice(2)
          .forEach((e) => {
            e.response.responses = [
              {
                topicId: SOAK_UUID,
                partitions: [
                  {
                    partitionIndex: 2,
                    errorCode: 0,
                    acknowledgeErrorCode: 0,
                    acquiredRecords: [{ firstOffset: '0', lastOffset: '0', deliveryCount: 2 }],
                  },
                ],
              },
            ]
            sealExchange(e)
            e.response.responses = []
          })
      },
    ],
    [
      'answer outside the measured window',
      (v) => {
        partitionTwo(v)
          .slice(2)
          .forEach((e) => {
            e.frames[0].answeredAt = 43834
          })
      },
    ],
    [
      'public request sent before the window',
      (v) => {
        v.events
          .filter(({ kind, payload }) => kind === 'REQUEST' && payload.broker === 'node2:9092')
          .forEach(({ payload }) => {
            payload.sentAt = 1794039558804
          })
      },
    ],
    [
      'uncorrelated public completion',
      (v) => {
        v.events
          .filter(({ kind, payload }) => kind === 'REQUEST' && payload.broker === 'node2:9092')
          .forEach(({ payload }) => {
            payload.correlationId += 100
          })
      },
    ],
  ])('rejects %s and identifies the uncovered partition and timestamps', (_name, damage) => {
    const verifier = incrementalVerifierFacts()
    damage(verifier)
    const failures = violations(verifier)
    expect(failures).toHaveLength(1)
    expect(failures[0]).toContain('all-partition in-window wire activity')
    expect(failures[0]).toContain('"missingPartitions":[2]')
    expect(failures[0]).toContain('"window":[38832,43833]')
    expect(failures[0]).toContain('"socket":30')
    expect(failures[0]).toMatch(/"at":\d+,"answeredAt":\d+/)
  })

  test('names an invalid observation duration separately from successful partition coverage', () => {
    const verifier = incrementalVerifierFacts()
    verifier.events.find(({ kind }) => kind === 'window-end').at = 43831
    const [failure] = violations(verifier)
    expect(failure).toContain('measured duration')
    expect(failure).toContain('"coveredPartitions":[0,1,2],"missingPartitions":[]')
  })

  test('does not treat an omitted partition as an Accept receipt', () => {
    const facts = cycleFacts(0)
    const receipt = facts.provenance.exchanges.find((e) => e.tickets.length)
    receipt.response.responses = []
    sealExchange(receipt)
    expect(ShareCycleOracle.judge(ShareCycleOracle.plan(0), facts).join('; ')).toContain(
      'a successful Accept lacks a unique original request and partition receipt'
    )
  })
})

describe('Share soak natural exit observations', () => {
  const observation = () => ({
    origin: 'parent-monotonic',
    cleanupOrigin: 'parent-monotonic',
    enteredAt: 0,
    completedAt: 520000,
    exitedAt: 530000,
    code: 0,
    signal: null,
    watchdog: false,
    forced: false,
  })

  test.each(['short', 'full'])('accepts the exact inclusive exit boundaries for %s', (mode) => {
    const budget = SHARE_SOAK.budgets[mode]
    const facts = observation()
    facts.completedAt = budget.totalMs - 10000
    facts.exitedAt = budget.totalMs
    expect(shareSoakNaturalExit(facts, budget)).toBe(true)
  })

  test.each([
    ['short', 2040000],
    ['full', 3000000],
  ])('rejects inclusive total +1 regardless of passing summary values for %s', (mode, totalMs) => {
    const facts = observation()
    facts.completedAt = totalMs - 1
    facts.exitedAt = totalMs + 1
    const budget = SHARE_SOAK.budgets[mode]
    expect(facts.exitedAt - facts.completedAt).toBeLessThanOrEqual(budget.exitMs)
    expect(shareSoakNaturalExit(facts, budget)).toBe(false)
  })

  test.each([
    [
      'post-cleanup +10001',
      (o) => {
        o.exitedAt++
      },
    ],
    [
      'watchdog',
      (o) => {
        o.watchdog = true
      },
    ],
    [
      'forced exit',
      (o) => {
        o.forced = true
      },
    ],
    [
      'nonzero status',
      (o) => {
        o.code = 1
      },
    ],
    [
      'signal',
      (o) => {
        o.signal = 'SIGTERM'
      },
    ],
    [
      'unrelated clock origin',
      (o) => {
        o.cleanupOrigin = 'unknown'
      },
    ],
    [
      'missing cleanup',
      (o) => {
        delete o.completedAt
      },
    ],
    [
      'missing exit',
      (o) => {
        delete o.exitedAt
      },
    ],
    [
      'reversed timestamps',
      (o) => {
        o.completedAt = -1
      },
    ],
  ])('rejects %s regardless of passing summary values', (_name, damage) => {
    const facts = observation()
    damage(facts)
    expect(shareSoakNaturalExit(facts, SHARE_SOAK.budgets.short)).toBe(false)
  })
})

describe('Share soak real cycle adapters', () => {
  test.each([false, true])(
    'starts all partition sends together and joins them after a failure=%s',
    async (failFirst) => {
      const pending = []
      const error = new Error('partition send failed')
      const driver = new ShareSoakDriver({
        ports: { bounded: { bounded }, fixtures: { shareRecordIdentity } },
      })
      driver.topic = TOPIC_NAME
      driver.producer = {
        send: ({ messages }) =>
          new Promise((resolve, reject) => {
            const partition = messages[0].partition
            pending.push({
              partition,
              finish: () =>
                failFirst && partition === 0
                  ? reject(error)
                  : resolve([{ partition, errorCode: 0, baseOffset: '0' }]),
            })
          }),
      }
      let finished = false
      const work = driver.produce(ShareCycleOracle.plan(0), NEVER)
      work.then(
        () => {
          finished = true
        },
        () => {
          finished = true
        }
      )
      expect(pending.map(({ partition }) => partition)).toEqual([0, 1, 2])
      pending[0].finish()
      await Promise.resolve()
      expect(finished).toBe(false)
      pending[2].finish()
      pending[1].finish()
      if (failFirst) await expect(work).rejects.toBe(error)
      else expect(await work).toEqual(cycleFacts(0).produced)
    }
  )

  /** Public client double with actual emitters and joined handler callbacks during disconnect. */
  const publicClient = () => {
    const emitter = new EventEmitter()
    const client = {
      events: Object.fromEntries(
        [
          'GROUP_JOIN',
          'ASSIGNMENT_CHANGED',
          'ACKNOWLEDGE',
          'SESSION_RESET',
          'CRASH',
          'DISCONNECT',
          'REQUEST',
          'FETCH',
        ].map((event) => [event, event])
      ),
      on: (event, listener) => {
        emitter.on(event, listener)
        return () => emitter.off(event, listener)
      },
      emit: (event, payload) => emitter.emit(event, { payload }),
      connect: async () => {},
      subscribe: async () => {},
      assignment: () => [{ topic: TOPIC_NAME, partitions: [0, 1, 2] }],
      run: async (handlers) => {
        client.handlers = handlers
        client.emit('GROUP_JOIN', { memberId: 'verifier' })
      },
      disconnect: async () => {
        await client.drain?.()
        client.emit('DISCONNECT', {})
      },
    }
    return client
  }

  test.each(['disconnect-start', 'disconnect-end'])(
    'joins disconnect and removes listeners when the progress sink fails at %s',
    async (failedStep) => {
      const client = publicClient()
      const disconnect = jest.spyOn(client, 'disconnect')
      const member = new ShareMember({
        kafka: { shareConsumer: () => client },
        shares: { track: (c) => c },
        label: 'A',
        group: 'soak-group',
        topic: TOPIC_NAME,
        fixtures: { shareRecordIdentity },
        onProgress: (step) => {
          if (step === failedStep) throw new Error('progress sink failed')
        },
      })
      await member.start(NEVER, bounded)
      await expect(member.stop()).rejects.toThrow('progress sink failed')
      expect(disconnect).toHaveBeenCalledTimes(1)
      expect(member.removers).toEqual([])
    }
  )

  test('keeps the workload handler and acknowledgement journals through joined disconnect', async () => {
    const client = publicClient()
    const late = []
    const member = new ShareMember({
      kafka: { shareConsumer: () => client },
      shares: { track: (c) => c },
      label: 'A',
      group: 'soak-group',
      topic: TOPIC_NAME,
      fixtures: { shareRecordIdentity },
      onLateEntry: (entries) => late.push(...entries),
    })
    await member.start(NEVER, bounded)
    const message = {
      offset: '0',
      key: Buffer.from('key'),
      value: Buffer.from('value'),
      deliveryCount: 1,
    }
    const dispatch = () =>
      client.handlers.eachBatch({ batch: { topic: TOPIC_NAME, partition: 0, messages: [message] } })
    await dispatch()
    client.drain = async () => {
      await dispatch()
      client.emit('ACKNOWLEDGE', {
        topic: TOPIC_NAME,
        partition: 0,
        firstOffset: '0',
        lastOffset: '0',
        type: 'accept',
      })
    }
    await member.stop()
    expect(member.finalize()).toHaveLength(2)
    expect(member.accepted()).toHaveLength(1)
    await dispatch()
    expect(late).toHaveLength(1)
  })

  test('captures every earlier raw identity while preserving current-cycle selection', () => {
    const driver = new ShareSoakDriver({ ports: { fixtures: { shareRecordIdentity } } })
    driver.topic = TOPIC_NAME
    const first = ShareCycleOracle.plan(0)
    const second = ShareCycleOracle.plan(1)
    const log = new Map(
      [0, 1, 2].map((partition) => [
        partition,
        {
          endOffset: second.end,
          records: [...first.records, ...second.records]
            .filter((record) => record.partition === partition)
            .map((record, index) => ({
              offset: String(index),
              key: Buffer.from(record.key),
              value: Buffer.from(record.value),
            })),
        },
      ])
    )
    const result = driver.readLogs(second, log)
    expect(result.log).toHaveLength(60)
    expect(result.prefix).toHaveLength(120)
    log.get(0).records[0].value = Buffer.from('altered')
    expect(driver.readLogs(second, log).log).toEqual(result.log)
    expect(driver.readLogs(second, log).prefix).not.toEqual(result.prefix)
  })

  describe('the inherited verifier with scripted public transport', () => {
    beforeEach(() => jest.useFakeTimers())
    afterEach(() => jest.useRealTimers())

    const verify = ({ replayAt, duringDrain = false, assignmentAt = 0, due = 32000 } = {}) => {
      const clients = []
      const kafka = {
        shareConsumer: () => {
          const client = publicClient()
          clients.push(client)
          client.assignment = () =>
            Date.now() >= admittedAt + assignmentAt
              ? [{ topic: TOPIC_NAME, partitions: [0, 1, 2] }]
              : []
          const run = client.run
          client.run = async (handlers) => {
            await run(handlers)
            client.fetchTimer = setInterval(
              () => client.emit('REQUEST', { apiName: 'ShareFetch', sentAt: Date.now() }),
              250
            )
            if (replayAt !== undefined)
              client.replayTimer = setTimeout(() => deliver(client), assignmentAt + 250 + replayAt)
          }
          client.disconnect = async () => {
            clearInterval(client.fetchTimer)
            clearTimeout(client.replayTimer)
            if (duringDrain) await deliver(client)
            client.emit('DISCONNECT', {})
          }
          return client
        },
      }
      const deliver = (client) =>
        client.handlers.eachMessage({
          topic: TOPIC_NAME,
          partition: 0,
          message: {
            offset: '0',
            key: Buffer.from('late'),
            value: Buffer.from('late'),
            deliveryCount: 1,
          },
        })
      const admittedAt = Date.now()
      const driver = new ShareSoakDriver({
        kafka,
        shares: { track: (client) => client },
        tap: new ShareFetchTap(),
        ports: { fixtures: require('../../../testHelpers/shareFixtures') },
      })
      driver.group = 'soak-group'
      driver.topic = TOPIC_NAME
      driver.topicId = SOAK_UUID
      const work = driver.quiet(NEVER, 5000, { due: performance.now() + due })
      work.catch(() => {})
      return { work, clients }
    }

    test('records delayed assignment but rejects public requests without original wire evidence', async () => {
      const { work } = verify({ assignmentAt: 2000 })
      await jest.advanceTimersByTimeAsync(7500)
      const evidence = await work
      const failures = []
      ShareCycleOracle.verifier(evidence, 5000, (message) => failures.push(message))
      expect(failures).toEqual([expect.stringContaining('measured observation window')])
      expect(
        evidence.events
          .filter(({ kind }) => kind === 'window-start' || kind === 'window-end')
          .map(({ kind }) => kind)
      ).toEqual(['window-start', 'window-end'])
      const start = evidence.events.find(({ kind }) => kind === 'window-start')
      const end = evidence.events.find(({ kind }) => kind === 'window-end')
      expect(start.at).toBeGreaterThanOrEqual(2000)
      expect(end.at - start.at).toBeGreaterThanOrEqual(5000)
    })

    test.each([1500, 4999])('retains a replay %i ms into the cycle verifier', async (replayAt) => {
      const { work } = verify({ replayAt })
      await jest.advanceTimersByTimeAsync(6000)
      const evidence = await work
      expect(evidence.delivered).toHaveLength(1)
      const failures = []
      ShareCycleOracle.verifier(evidence, 5000, (message) => failures.push(message))
      expect(failures).toHaveLength(1)
    })

    test('retains an identity delivered during verifier disconnect', async () => {
      const { work } = verify({ duringDrain: true })
      await jest.advanceTimersByTimeAsync(6000)
      expect((await work).delivered).toHaveLength(1)
    })

    test('a delayed assignment cannot start a fresh window beyond the original slot', async () => {
      const { work } = verify({ assignmentAt: 3000, due: 7999 })
      await jest.advanceTimersByTimeAsync(3500)
      await expect(work).rejects.toThrow('complete assigned verifier window')
    })
  })
})

describe('Share soak original authority adapter', () => {
  test('calls through the original acquisition, dispatch, ticket outcome, failure and retirement methods', async () => {
    const { ShareFetchLoop } = require('../../../src/consumer/share/shareFetchLoop')
    const { ShareRun } = require('../../../src/consumer/share/shareRun')
    const source = sealExchange(
      exchangeFacts(7, 0, [{ firstOffset: '0', lastOffset: '0', deliveryCount: 1 }])
    )
    let written = source
    const tap = new ShareFetchTap()
    const journal = new ShareCycleJournal('soak-group', tap)
    const originalRow = {
      token: Symbol('acquisition'),
      ledger: {},
      partition: 0,
      topic: TOPIC_NAME,
    }
    const sentinel = Symbol('original return')
    const spies = [
      jest.spyOn(ShareFetchLoop.prototype, 'send').mockImplementation(() => {
        tap.context.getStore().frames.push(written.frames[0])
        return sentinel
      }),
      jest.spyOn(ShareFetchLoop.prototype, 'complete').mockReturnValue(sentinel),
      jest.spyOn(ShareFetchLoop.prototype, 'adopt').mockReturnValue(originalRow),
      jest.spyOn(ShareFetchLoop.prototype, 'emit').mockReturnValue(sentinel),
      jest.spyOn(ShareFetchLoop.prototype, 'failExchange').mockReturnValue(sentinel),
      jest.spyOn(ShareFetchLoop.prototype, 'onSessionReset').mockReturnValue(sentinel),
      jest
        .spyOn(ShareRun.prototype, 'invoke')
        .mockImplementation(async () => journal.handlers.getStore()),
    ]
    const loop = Object.assign(Object.create(ShareFetchLoop.prototype), {
      groupId: 'soak-group',
      memberId: 'member-A',
      broker: { brokerAddress: 'node:1' },
      session: { token: Symbol('open') },
      cohorts: new Map([['p', [originalRow]]]),
    })
    const exchange = {
      apiKey: 78,
      open: loop.session.token,
      binding: { pool: {}, generation: 1 },
      prepared: [],
      fields: {},
    }
    journal.install()
    try {
      expect(loop.send(exchange)).toBe(sentinel)
      expect(loop.complete(exchange, source.response)).toBe(sentinel)
      expect(
        loop.adopt(exchange, SOAK_UUID, source.response.responses[0].partitions[0], 30000)
      ).toBe(originalRow)
      const context = await ShareRun.prototype.invoke.call({}, null, originalRow)
      expect(context).toEqual({ acquisition: journal.acquisitions[0].id, dispatch: 1 })
      expect(loop.emit([{ type: 'accept', firstOffset: '0', lastOffset: '0' }], originalRow)).toBe(
        sentinel
      )
      const batch = { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] }
      written = exchangeFacts(8, 0, [])
      written.frames[0].request.topics[0].partitions[0].acknowledgementBatches = [batch]
      sealExchange(written)
      const receipt = {
        ...exchange,
        prepared: [
          { row: originalRow, ticket: { acknowledgementBatches: [batch], isRenewAck: false } },
        ],
      }
      loop.send(receipt)
      loop.complete(receipt, written.response)
      const failures = []
      const corpus = new Map([[cycleFacts(0).produced[0].identity, { partition: 0, offset: 0n }]])
      const evidence = {
        accepted: [{ topic: TOPIC_NAME, partition: 0, firstOffset: '0', lastOffset: '0' }],
        provenance: journal.snapshot(),
      }
      ShareCycleOracle.accepts(corpus, evidence, (message) => failures.push(message))
      expect(failures).toEqual([])
      expect(evidence.provenance.outcomes[0]).toMatchObject({ topic: TOPIC_NAME, partition: 0 })
      delete evidence.provenance.outcomes[0].topic
      ShareCycleOracle.accepts(corpus, evidence, (message) => failures.push(message))
      expect(failures).toContain(
        'a successful Accept lacks a unique original request and partition receipt'
      )
      expect(loop.failExchange(exchange, new Error('lost answer'))).toBe(sentinel)
      expect(loop.onSessionReset()).toBe(sentinel)
      const archived = journal.snapshot()
      expect(archived.exchanges[0].frames).toEqual(source.frames)
      expect(archived.exchanges[0].response).toEqual(source.response)
      expect(archived.exchanges[0].error.message).toBe('lost answer')
      expect(archived.outcomes[0].acquisition).toBe(archived.acquisitions[0].id)
      expect(archived.retirements[0].acquisitions).toEqual([archived.acquisitions[0].id])
    } finally {
      journal.restore()
      spies.reverse().forEach((spy) => spy.mockRestore())
    }
  })
})

describe('Share soak original queue and socket listener adapter', () => {
  test('reads share identities, endpoints, counters and listeners together after the collection gap', async () => {
    let socket
    let cluster
    const shared = instrumentShare(
      {
        clientId: 'soak',
        brokers: ['localhost:1'],
        socketFactory: () => Object.assign(new EventEmitter(), { destroyed: false }),
      },
      {
        activeResources: false,
        socketIdentities: true,
        gc: () => {},
        pause: async () => {
          socket.destroyed = true
          socket.removeAllListeners()
          cluster.brokerPool.metadata.brokers = [{ host: 'after', port: 2 }]
        },
      }
    )
    try {
      cluster = privateOf(shared.kafka, 'createCluster').value({})
      cluster.brokerPool.metadata = { brokers: [{ host: 'before', port: 1 }] }
      socket = shared.socketFactory({ host: 'before', port: 1 })
      socket.on('error', () => {})
      const row = await shared.shareResources.snapshot()
      expect(row.sockets).toEqual({ created: 1, destroyed: 1, open: 0, live: [] })
      expect(row.socketNodes).toContain('after:2')
      expect(row.socketNodes).not.toContain('before:1')
      expect(row.namedListeners).toEqual({})
      expect(row.listeners.sockets).toBe(0)
    } finally {
      shared.shareResources.restore()
    }
  })

  test.each([false, true])(
    'rejects a timer present before collection, retained across the gap=%s',
    async (retained) => {
      let timer
      const shared = instrumentShare(
        { clientId: 'soak', brokers: ['localhost:1'] },
        {
          gc: () => {},
          pause: async () => {
            if (!retained && timer) clearTimeout(timer)
          },
        }
      )
      try {
        const baseline = await shared.shareResources.snapshot()
        timer = setTimeout(() => {}, 60000)
        const after = await shared.shareResources.snapshot()
        expect(after.activeResourcesBeforeCollection.Timeout || 0).toBe(
          (baseline.activeResources.Timeout || 0) + 1
        )
        expect(after.activeResources.Timeout || 0).toBe(
          (baseline.activeResources.Timeout || 0) + Number(retained)
        )
        const violations = []
        ShareSoakScreen.quiet(after, baseline, 'timer', violations)
        expect(violations).toContain(
          `timer: activeResourcesBeforeCollection.Timeout baseline=${baseline.activeResources.Timeout || 0} after=${(baseline.activeResources.Timeout || 0) + 1}`
        )
        expect(after.timerOwners.beforeCollection.rows).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ type: 'Timeout', durationMs: 60000, hasRef: true }),
          ])
        )
        expect(
          after.timerOwners.afterCollection.rows.some(({ durationMs }) => durationMs === 60000)
        ).toBe(retained)
      } finally {
        clearTimeout(timer)
        shared.shareResources.restore()
      }
    }
  )

  test('writes bounded timer owners in quiet-point evidence, including intervals and unrefed timers', async () => {
    const shared = instrumentShare(
      { clientId: 'soak', brokers: ['localhost:1'] },
      { gc: () => {}, pause: async () => {} }
    )
    /** Schedules a timer through a stack longer than the evidence limit.
     * @param {number} depth remaining recursive calls
     * @returns {NodeJS.Timeout} the real timer to clear
     */
    const deepTimer = (depth) => (depth === 0 ? setTimeout(() => {}, 60000) : deepTimer(depth - 1))
    const stackLimit = Error.stackTraceLimit
    let timer
    try {
      Error.stackTraceLimit = 100
      timer = deepTimer(60)
    } finally {
      Error.stackTraceLimit = stackLimit
    }
    const RequestQueue = require('../../../src/network/requestQueue')
    const queue = new RequestQueue({
      maxInFlightRequests: 1,
      requestTimeout: 1000,
      enforceRequestTimeout: true,
      clientId: 'soak',
      broker: 'localhost:1',
      logger: { debug: () => {} },
    })
    queue.scheduleRequestTimeoutCheck()
    const immediate = setImmediate(() => {})
    timer.unref()
    const rows = []
    try {
      const snapshot = await shared.shareResources.snapshot()
      ShareSoak.prototype.recordResources.call(
        { row: (event, line) => rows.push({ event, ...JSON.parse(JSON.stringify(line)) }) },
        { kind: 'phase-end', phase: 'share-steady', snapshot }
      )
      expect(rows[0]).toMatchObject({ event: 'soak-resources', kind: 'phase-end' })
      for (const owners of Object.values(rows[0].snapshot.timerOwners)) {
        expect(owners.dropped).toBe(0)
        expect(owners.rows.length).toBeLessThanOrEqual(4096)
        expect(owners.rows).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              type: 'Timeout',
              durationMs: 60000,
              repeatMs: null,
              hasRef: false,
              stack: expect.stringContaining('deepTimer'),
            }),
            expect.objectContaining({
              type: 'Timeout',
              durationMs: 100,
              repeatMs: 100,
              stack: expect.stringContaining('scheduleRequestTimeoutCheck'),
              hasRef: true,
            }),
            expect.objectContaining({ type: 'Immediate', hasRef: true }),
          ])
        )
        expect(owners.rows.find(({ durationMs }) => durationMs === 60000).stack).toHaveLength(2048)
        for (const owner of owners.rows) {
          expect(owner.asyncId).toEqual(expect.any(Number))
          expect(owner.triggerAsyncId).toEqual(expect.any(Number))
          expect(owner.stack.length).toBeLessThanOrEqual(2048)
          expect(owner.stack).toEqual(expect.any(String))
        }
      }
    } finally {
      clearTimeout(timer)
      queue.destroy()
      clearImmediate(immediate)
      shared.shareResources.restore()
    }
  })

  test('bounds owner storage and fails quiet screening when allocation evidence overflows', async () => {
    const { ShareTimerObserver } = require('../shareTimerObserver')
    const observer = new ShareTimerObserver()
    const timers = []
    observer.install()
    try {
      for (let i = 0; i < 4097; i++) timers.push(setTimeout(() => {}, 60000))
      const owners = observer.snapshot()
      expect(owners.rows).toHaveLength(4096)
      expect(owners.dropped).toBe(1)
      const violations = []
      ShareSoakScreen.quiet(
        { ...quietShare(), timerOwners: { beforeCollection: owners, afterCollection: owners } },
        quietShare(),
        'overflow',
        violations
      )
      expect(violations).toContain(
        'overflow: timerOwners.beforeCollection.dropped baseline=0 after=1'
      )
      expect(violations).toContain(
        'overflow: timerOwners.afterCollection.dropped baseline=0 after=1'
      )
      timers.forEach(clearTimeout)
      expect(observer.snapshot().rows).toEqual([])
    } finally {
      timers.forEach(clearTimeout)
      observer.restore()
    }
    const later = setTimeout(() => {}, 60000)
    try {
      expect(observer.snapshot().rows).toEqual([])
    } finally {
      clearTimeout(later)
    }
  }, 30000)

  test.each([false, true])(
    'releases closed chaos sockets but rejects a product owner retaining one, retained=%s',
    async (retained) => {
      const exposed = globalThis.gc
      require('v8').setFlagsFromString('--expose-gc')
      globalThis.gc = require('vm').runInNewContext('gc')
      const createSocket = require('../../../src/network/socket')
      const { createChaosSocketFactory } = require('../../../testHelpers/chaosSocket')
      const chaos = createChaosSocketFactory(() => new (require('net').Socket)())
      const tap = new ShareFetchTap()
      const productOwner = { retiredSocket: null }
      const shared = instrumentShare(
        {
          clientId: 'soak',
          brokers: ['localhost:1'],
          socketFactory: tap.factory(forgettingDestroyed(chaos, { releaseOnClose: true })),
        },
        { activeResources: false }
      )
      try {
        const baseline = await shared.shareResources.snapshot()
        let socket = createSocket({
          socketFactory: shared.socketFactory,
          host: 'localhost',
          port: 1,
          onData: () => {},
          onEnd: () => {},
          onError: () => {},
          onTimeout: () => {},
        })
        createSocket.detachSocket(socket)
        socket.destroy()
        if (retained) productOwner.retiredSocket = socket
        socket = null

        const after = await shared.shareResources.snapshot()
        expect(chaos.sockets).toEqual([])
        expect(after.sockets).toEqual({ created: 1, destroyed: 1, open: 0 })
        expect(Object.values(after.namedListeners)).toEqual(retained ? [1] : [])
        const violations = []
        ShareSoakScreen.quiet(
          { ...after, activeResources: {}, activeResourcesBeforeCollection: {} },
          { ...baseline, activeResources: {}, activeResourcesBeforeCollection: {} },
          'closed',
          violations
        )
        expect(violations.length > 0).toBe(retained)
        if (retained) expect(productOwner.retiredSocket.listenerCount('error')).toBe(1)
        productOwner.retiredSocket = null
        expect((await shared.shareResources.snapshot()).namedListeners).toEqual({})
      } finally {
        shared.shareResources.restore()
        globalThis.gc = exposed
      }
    }
  )

  test('distinguishes a same-event listener moved between original queues or destroyed sockets', async () => {
    const shared = instrumentShare(
      { clientId: 'soak', brokers: ['localhost:1'], socketFactory: () => new EventEmitter() },
      { activeResources: false }
    )
    const RequestQueue = require('../../../src/network/requestQueue')
    const queue = () =>
      new RequestQueue({
        maxInFlightRequests: 1,
        requestTimeout: 1000,
        enforceRequestTimeout: false,
        clientId: 'soak',
        broker: 'localhost:1',
        logger: { debug: () => {} },
      })
    try {
      const first = queue()
      const second = queue()
      first.scheduleRequestTimeoutCheck()
      second.scheduleRequestTimeoutCheck()
      const one = shared.socketFactory({ host: 'localhost', port: 1 })
      const two = shared.socketFactory({ host: 'localhost', port: 1 })
      one.destroyed = true
      two.destroyed = true
      const listener = () => {}
      first.on('data', listener)
      one.on('data', listener)
      const before = (await shared.shareResources.snapshot()).namedListeners
      first.off('data', listener)
      second.on('data', listener)
      one.off('data', listener)
      two.on('data', listener)
      const after = (await shared.shareResources.snapshot()).namedListeners
      expect(Object.values(before).reduce((sum, count) => sum + count, 0)).toBe(2)
      expect(Object.values(after).reduce((sum, count) => sum + count, 0)).toBe(2)
      expect(before).not.toEqual(after)
      second.off('data', listener)
      two.off('data', listener)
    } finally {
      shared.shareResources.restore()
    }
  })
})

describe('Share soak cycle finalization and isolation adapter', () => {
  /** The public member/client interface, driven by the real cycle ordering. */
  const cycle = async ({
    reset = false,
    lateDuringClose = false,
    remaining = 32000,
    holdPost = false,
    holdRaw = false,
    failRaw = false,
    leaveMode = 'answered',
    retained = false,
    retiringWrite = false,
    afterCloseWrite = false,
  } = {}) => {
    const plan = ShareCycleOracle.plan(reset ? 10 : 0)
    const clients = []
    const facts = cycleFacts(plan.cycle)
    const original = { sequence: 1, epoch: 4, socket: 1, endpoint: 'node:1' }
    let broken = false
    let redeliver
    const resetWrites = []
    const tap = new ShareFetchTap(64, { now: () => 0 })
    const Broker = require('../../../src/broker')
    const { respondWithFrame } = require('../../../testHelpers/fakeSocket')
    const socket = tap.wrap(Object.assign(new EventEmitter(), { write: () => true }), 'node:1', 10)
    let correlationId = 0
    const broker = Object.assign(Object.create(Broker.prototype), { brokerAddress: 'node:1' })
    const heartbeat = jest
      .spyOn(Broker.prototype, 'shareGroupHeartbeat')
      .mockImplementation(async (request) => {
        const mode = request.memberId === 'member-1' ? leaveMode : 'answered'
        const response = {
          errorCode: 0,
          memberId: request.memberId,
          memberEpoch: -1,
          assignment: null,
        }
        if (mode === 'no-write') return response
        const correlation = ++correlationId
        socket.write(heartbeatBytes(request, correlation))
        if (mode === 'transport-failure') {
          socket.emit('close', true)
          throw new Error('Leave transport failed')
        }
        if (mode === 'rejected') response.errorCode = 15
        respondWithFrame(
          { socket },
          correlation,
          1,
          require('../../../testHelpers/wire/shareGroupHeartbeatV1').buildResponse(response)
        )
        return response
      })
    tap.mark = () => 0
    tap.since = (mark) =>
      mark === 0 ? [original] : broken ? [{ sequence: 2, epoch: 0, socket: 2 }] : []
    const retiredWrite = () =>
      tap.notify('api78-write', {
        ...original,
        epoch: 5,
        groupId: afterCloseWrite === 'different-context' ? 'foreign-group' : 'soak-group',
        memberId: clients[0].memberId,
        attempted: true,
      })
    tap.destroy = () => {
      broken = true
      if (retiringWrite) retiredWrite()
      tap.notify('socket-close', { socket: original.socket, endpoint: original.endpoint })
      if (afterCloseWrite === 'before-reopen' || afterCloseWrite === 'different-context')
        retiredWrite()
      clients[0].emit('SESSION_RESET', { nodeId: 1, reason: 'connection' })
    }
    const kafka = {
      shareConsumer: () => {
        const bus = new EventEmitter()
        const client = {
          events: Object.fromEntries(
            [
              'GROUP_JOIN',
              'ASSIGNMENT_CHANGED',
              'ACKNOWLEDGE',
              'SESSION_RESET',
              'CRASH',
              'DISCONNECT',
            ].map((kind) => [kind, kind])
          ),
          on: (kind, listener) => {
            bus.on(kind, listener)
            return () => bus.off(kind, listener)
          },
          emit: (kind, payload) => bus.emit(kind, { payload }),
          connect: async () => {},
          subscribe: async () => {},
          run: async (handlers) => {
            client.handlers = handlers
            client.memberId = `member-${clients.length}`
            client.emit('GROUP_JOIN', { memberId: client.memberId })
          },
          disconnect: async () => {
            await client.pending
            if (client === clients[0] && afterCloseWrite === 'during-disconnect') retiredWrite()
            if (lateDuringClose && client === clients[0])
              await client.handlers.eachBatch(client.batch)
            if (client === clients[0])
              for (const range of facts.accepted)
                client.emit('ACKNOWLEDGE', { ...range, type: 'accept' })
            if (client !== clients[0] || leaveMode !== 'no-invocation')
              await broker.shareGroupHeartbeat({
                groupId: 'soak-group',
                memberId: client.memberId,
                memberEpoch: -1,
              })
            client.emit('DISCONNECT', {})
          },
        }
        clients.push(client)
        return client
      },
    }
    const ports = {
      fixtures: {
        shareRecordIdentity,
        waitForShareGroupsEmpty: async (_admin, [groupId], { record }) => {
          record('cleanup-members', {
            group: {
              groupId,
              errorCode: 0,
              state: retained ? 'Stable' : 'Empty',
              members: retained
                ? [
                    {
                      memberId: 'member-1',
                      assignment: [{ topic: TOPIC_NAME, partitions: [0, 1, 2] }],
                    },
                  ]
                : [],
            },
          })
          if (retained) throw new Error('Every share member leaving did not settle within 10000 ms')
        },
      },
      probe: { CleanupStack },
      bounded: {
        bounded,
        pollUntil: async (_scope, _ms, predicate) => {
          for (let i = 0; i < 20; i++) {
            if (await predicate(NEVER)) return
            if (redeliver) {
              const recover = redeliver
              redeliver = null
              await recover()
            } else if (resetWrites.length) {
              tap.notify('api78-write', resetWrites.shift())
              if (!resetWrites.length && afterCloseWrite === 'after-reopen') retiredWrite()
            }
            await Promise.resolve()
          }
          throw new Error('Scripted barrier never converged')
        },
      },
    }
    const steps = []
    const driver = new ShareSoakDriver({
      kafka,
      shares: { track: (client) => client },
      tap,
      ports,
      onProgress: (row) => steps.push(row),
    })
    Object.assign(driver, {
      topic: TOPIC_NAME,
      topicId: SOAK_UUID,
      group: 'soak-group',
      admin: {},
      leaders: new Map([0, 1, 2].map((nodeId) => [nodeId, `node:${nodeId + 1}`])),
      nodes: new Map([0, 1, 2].map((nodeId) => [`node:${nodeId + 1}`, nodeId])),
    })
    driver.awaitMembers = async () => {}
    driver.produce = async () => {
      const a = clients[0]
      const batches = [0, 1, 2].map((partition) => ({
        batch: {
          topic: TOPIC_NAME,
          partition,
          messages: facts.produced
            .filter((entry) => entry.partition === partition)
            .map(({ identity }) => {
              const [, , offset, key, value] = JSON.parse(identity)
              return {
                offset,
                key: Buffer.from(key, 'hex'),
                value: Buffer.from(value, 'hex'),
                deliveryCount: 1,
              }
            }),
        },
      }))
      a.batch = batches[0]
      const tasks = batches.map((batch) => a.handlers.eachBatch(batch))
      a.pending = Promise.all(tasks)
      if (reset)
        redeliver = async () => {
          await a.pending
          await a.handlers.eachBatch({
            batch: {
              ...a.batch.batch,
              messages: a.batch.batch.messages.map((message) => ({ ...message, deliveryCount: 2 })),
            },
          })
          resetWrites.push(
            ...[0, 1, 2].map((nodeId) => ({
              endpoint: `node:${nodeId + 1}`,
              socket: nodeId === 0 ? 2 : nodeId + 3,
              epoch: nodeId === 0 ? 0 : 4,
              groupId: 'soak-group',
              memberId: a.memberId,
              attempted: true,
            }))
          )
        }
      return facts.produced
    }
    let releaseRaw
    let rawArrived
    const raw = new Promise((resolve) => {
      rawArrived = resolve
    })
    driver.rawLog = async () => {
      rawArrived()
      if (holdRaw)
        await new Promise((resolve) => {
          releaseRaw = resolve
        })
      if (failRaw) throw new Error('raw read failed')
      return new Map(
        [0, 1, 2].map((partition) => [
          partition,
          {
            endOffset: plan.end,
            records: facts.prefix
              .map((identity) => JSON.parse(identity))
              .filter((tuple) => tuple[1] === partition)
              .map(([, , offset, key, value]) => ({
                offset,
                key: Buffer.from(key, 'hex'),
                value: Buffer.from(value, 'hex'),
              })),
          },
        ])
      )
    }
    let release
    let postArrived
    const post = new Promise((resolve) => {
      postArrived = resolve
    })
    let polls = 0
    driver.position = async () => {
      if (++polls === 2 && holdPost)
        await new Promise((resolve) => {
          release = resolve
          postArrived()
        })
      return { ...positionFacts(plan), rows: facts.broker }
    }
    driver.quiet = jest.fn(async () => verifierFacts())
    const work = driver
      .cycle(plan, NEVER, { due: remaining, clock: { now: () => 0 } })
      .finally(() => heartbeat.mockRestore())
    work.catch(() => {})
    return {
      driver,
      work,
      clients,
      steps,
      raw,
      post,
      releaseRaw: () => releaseRaw?.(),
      release: () => release?.(),
    }
  }

  test.each(['no-invocation', 'no-write'])(
    'fails the cycle naming the original member after resolved disconnect with %s',
    async (leaveMode) => {
      const { driver, work, steps } = await cycle({ leaveMode })
      await expect(work).rejects.toThrow(/A.*member-1.*leave/)
      expect(steps).toContainEqual(
        expect.objectContaining({ step: 'disconnect-end', member: 'A', status: 'completed' })
      )
      expect(driver.lastCycle.members.aLeft).toBe(false)
      expect(driver.quiet).not.toHaveBeenCalled()
      if (leaveMode === 'no-write')
        expect(steps).toContainEqual(
          expect.objectContaining({ step: 'leave-no-write', member: 'A', memberId: 'member-1' })
        )
      expect(steps.some(({ step, member }) => step === 'leave-write' && member === 'A')).toBe(false)
    }
  )

  test('proves each departure from the invoked, written and matched answered leave', async () => {
    const { driver, work, steps } = await cycle()
    const observed = await work
    expect(observed.members).toMatchObject({ aLeft: true, bRemoved: true })
    for (const [member, memberId] of [
      ['A', 'member-1'],
      ['B', 'member-2'],
    ]) {
      const beat = observed.provenance.heartbeats.find((beat) => beat.memberId === memberId)
      const frame = beat.frames[0]
      expect(ShareCycleOracle.departure(observed.provenance, 'soak-group', memberId)).toEqual(frame)
      for (const step of ['leave-invoked', 'leave-write', 'leave-answer'])
        expect(steps).toContainEqual(
          expect.objectContaining({ step, member, memberId, groupId: 'soak-group' })
        )
      expect(steps).toContainEqual(
        expect.objectContaining({
          step: 'leave-answer',
          member,
          socket: frame.socket,
          endpoint: frame.endpoint,
          correlationId: frame.correlationId,
          writtenAt: frame.at,
          answeredAt: frame.answeredAt,
          errorCode: 0,
        })
      )
    }
    expect(driver.quiet).toHaveBeenCalledTimes(1)
  })

  test('retains full empty-wait descriptions and names members still listed on failure', async () => {
    const { driver, work, steps } = await cycle({ leaveMode: 'no-invocation', retained: true })
    await expect(work).rejects.toThrow(/10000 ms.*member-1/)
    const row = steps.find(({ step }) => step === 'cleanup-members')
    expect(row.group).toEqual({
      groupId: 'soak-group',
      errorCode: 0,
      state: 'Stable',
      members: [
        { memberId: 'member-1', assignment: [{ topic: TOPIC_NAME, partitions: [0, 1, 2] }] },
      ],
    })
    expect(driver.lastCycle.provenance.groupDescriptions).toContainEqual({
      at: row.at,
      group: row.group,
    })
    expect(driver.lastCycle.members.aLeft).toBe(false)
  })

  test.each(['transport-failure', 'rejected'])(
    'does not prove departure from a %s leave',
    async (leaveMode) => {
      const { driver, work, steps } = await cycle({ leaveMode })
      await expect(work).rejects.toThrow(
        leaveMode === 'rejected' ? /member-1.*leave/ : 'Leave transport failed'
      )
      expect(driver.lastCycle.members.aLeft).toBe(false)
      if (leaveMode === 'transport-failure') {
        expect(steps).toContainEqual(
          expect.objectContaining({
            step: 'leave-lost',
            member: 'A',
            memberId: 'member-1',
            lostAt: 0,
          })
        )
        expect(steps).toContainEqual(
          expect.objectContaining({
            step: 'leave-failure',
            member: 'A',
            error: expect.objectContaining({ message: 'Leave transport failed' }),
          })
        )
      } else
        expect(steps).toContainEqual(
          expect.objectContaining({ step: 'leave-answer', member: 'A', errorCode: 15 })
        )
    }
  )

  test.each([false, true])(
    'starts member disconnect during the independent raw read, failing read=%s',
    async (failRaw) => {
      const state = await cycle({ holdRaw: true, failRaw })
      await state.raw
      const closing = state.steps.some(
        ({ step, member }) => step === 'disconnect-start' && member === 'A'
      )
      state.releaseRaw()
      if (failRaw) {
        await expect(state.work).rejects.toThrow('raw read failed')
        expect(state.driver.quiet).not.toHaveBeenCalled()
      } else await state.work
      expect(closing).toBe(true)
      expect(state.steps).toEqual(state.driver.lastCycle.steps)
      expect(state.steps).toContainEqual(
        expect.objectContaining({ step: 'disconnect-end', member: 'A', status: 'completed' })
      )
    }
  )

  test('archives cycle milestones before the verifier and retains them on slot exhaustion', async () => {
    const { driver, work, steps } = await cycle({ remaining: 4999 })
    await expect(work).rejects.toThrow('complete verifier window')
    expect(steps.map(({ step }) => step)).toEqual(
      expect.arrayContaining([
        'cycle-start',
        'produce-start',
        'produce-end',
        'first-delivery',
        'all-delivered',
        'all-acks',
        'member-add-start',
        'member-add-end',
        'member-remove-start',
        'member-remove-end',
        'disconnect-start',
        'disconnect-end',
        'group-empty-end',
      ])
    )
    expect(steps).toEqual(driver.lastCycle.steps)
    expect(
      steps.every(
        ({ at, elapsedMs, due, remainingMs }) =>
          at === 0 && elapsedMs === 0 && due === 4999 && remainingMs === 4999
      )
    ).toBe(true)
    expect(driver.quiet).not.toHaveBeenCalled()
  })

  test.each([false, true])(
    'the real cycle captures handler drain and original reset/reopen, reset=%s',
    async (reset) => {
      const { driver, work } = await cycle({ reset, lateDuringClose: true })
      const observed = await work
      expect(observed.handler).toHaveLength(reset ? 100 : 80)
      expect(observed.accepted).toHaveLength(60)
      expect(observed.positions).toHaveLength(2)
      expect(driver.quiet).toHaveBeenCalledTimes(1)
      if (reset)
        expect(observed.reset).toMatchObject({
          oldEpoch: 4,
          nextEpoch: 0,
          oldSocket: 1,
          nextSocket: 2,
        })
      const problems = []
      ShareCycleOracle.handler(
        ShareCycleOracle.plan(reset ? 10 : 0),
        new Map(observed.produced.map((row) => [row.identity, row])),
        observed,
        (message) => problems.push(message)
      )
      expect(
        problems.some((message) =>
          message.includes(reset ? 'delivery count' : 'duplicate delivery')
        )
      ).toBe(true)
    }
  )

  test.each([false, true])(
    'reserves the reset node diagnostic for its replacement socket after a retiring write=%s',
    async (retiringWrite) => {
      const { work } = await cycle({ reset: true, retiringWrite })
      const observed = await work
      const writes = observed.steps.filter(({ step }) => step === 'post-reset-api78')
      expect(writes).toHaveLength(3)
      expect(writes.map(({ nodeId }) => nodeId)).toEqual([0, 1, 2])
      expect(writes.find(({ nodeId }) => nodeId === observed.resetTarget.nodeId)).toMatchObject({
        socket: observed.reset.nextSocket,
        epoch: 0,
        attempted: true,
      })
      expect(writes.filter(({ nodeId }) => nodeId !== observed.resetTarget.nodeId)).toEqual([
        expect.objectContaining({ socket: 4, epoch: 4 }),
        expect.objectContaining({ socket: 5, epoch: 4 }),
      ])
    }
  )

  test.each(['before-reopen', 'after-reopen', 'during-disconnect', 'different-context'])(
    'rejects a retired socket write after confirmed close, timing=%s',
    async (afterCloseWrite) => {
      const control = await cycle({ reset: true, retiringWrite: true })
      await expect(control.work).resolves.toMatchObject({ reset: { nextSocket: 2, nextEpoch: 0 } })
      const { work, driver } = await cycle({ reset: true, retiringWrite: true, afterCloseWrite })
      await expect(work).rejects.toThrow('Retired reset socket 1 was written after close')
      expect(driver.lastCycle.steps).toContainEqual(
        expect.objectContaining({ step: 'retired-socket-write', socket: 1 })
      )
      expect(driver.lastCycle.steps.filter(({ step }) => step === 'post-reset-api78')).toHaveLength(
        3
      )
      expect(driver.lastCycle.snapshot.workloadJoined).toBe(true)
    }
  )

  test('waits for held identities to be redelivered before closing the workload member', async () => {
    const { work } = await cycle({ reset: true })
    const observed = await work
    const writes = observed.steps.filter((row) => row.step === 'post-reset-api78')
    expect(writes.map((row) => row.nodeId)).toEqual([0, 1, 2])
    const disconnect = observed.steps.findIndex(
      (row) => row.step === 'disconnect-start' && row.member === 'A'
    )
    expect(observed.steps.indexOf(writes[2])).toBeLessThan(disconnect)
    expect(observed.held).toHaveLength(20)
    expect(observed.handler).toHaveLength(80)
    for (const identity of observed.held)
      expect(
        observed.handler
          .filter((entry) => entry.identity === identity)
          .map((entry) => entry.deliveryCount)
      ).toEqual([1, 2])
  })

  test('refuses before invoking the verifier when the slot has only 4999 ms remaining', async () => {
    const { driver, work } = await cycle({ remaining: 4999 })
    await expect(work).rejects.toThrow('complete verifier window')
    expect(driver.quiet).not.toHaveBeenCalled()
  })

  test('keeps a cycle pending until its post-verifier listing resolves', async () => {
    const state = await cycle({ holdPost: true })
    let completed = false
    state.work.then(() => {
      completed = true
    })
    await state.post
    expect(state.driver.quiet).toHaveBeenCalledTimes(1)
    expect(completed).toBe(false)
    state.release()
    await state.work
    expect(completed).toBe(true)
  })
})

describe('Share soak owned raw reader', () => {
  test('uses the counted factory and retains a disconnect failure suppressed by the reader helper', async () => {
    const Broker = require('../../../src/broker')
    const sockets = []
    const shared = instrumentShare(
      {
        clientId: 'soak',
        brokers: ['node:1'],
        socketFactory: () => {
          const socket = new EventEmitter()
          socket.destroyed = false
          sockets.push(socket)
          return socket
        },
      },
      { activeResources: false }
    )
    const owners = []
    const spies = [
      jest.spyOn(Broker.prototype, 'connect').mockImplementation(async function () {
        owners.push(this)
        this.connectionPool.socketFactory({ host: 'node', port: 1 })
        this.connectionPool.pool[0].requestQueue.scheduleRequestTimeoutCheck()
        this.connectionPool.pool[0].requestQueue.destroy()
      }),
      jest.spyOn(Broker.prototype, 'metadata').mockResolvedValue({
        brokers: [{ nodeId: 0, host: 'node', port: 1 }],
        topicMetadata: [
          {
            topic: TOPIC_NAME,
            topicErrorCode: 0,
            partitionMetadata: [{ partitionId: 0, leader: 0 }],
          },
        ],
      }),
      jest
        .spyOn(Broker.prototype, 'listOffsets')
        .mockResolvedValue({ responses: [{ partitions: [{ offset: '0' }] }] }),
      jest
        .spyOn(Broker.prototype, 'disconnect')
        .mockRejectedValue(new Error('raw disconnect failed')),
    ]
    try {
      const driver = new ShareSoakDriver({
        brokers: ['node:1'],
        socketFactory: shared.socketFactory,
        ports: { bounded: { bounded } },
      })
      driver.topic = TOPIC_NAME
      await expect(driver.rawLog(NEVER)).rejects.toThrow('Raw reader cleanup failed')
      expect(owners).toHaveLength(2)
      expect((await shared.shareResources.snapshot()).sockets.open).toBe(2)
      const retained = owners[0].connectionPool.pool[0].requestQueue
      retained.pending.push({})
      expect((await shared.shareResources.snapshot()).queueDepth).toBe(1)
      retained.pending.length = 0
    } finally {
      sockets.forEach((socket) => {
        socket.destroyed = true
      })
      spies.reverse().forEach((spy) => spy.mockRestore())
      shared.shareResources.restore()
    }
  })
})

describe('Share soak complete original-owner schedules', () => {
  afterEach(() => liveRun.reset())
  const { originalShareSchedule: schedule } = require('./fixtures/originalShareSchedule')
  const { ShareTimerObserver } = require('../shareTimerObserver')
  const { setTimeout: realSetTimeout, clearTimeout: realClearTimeout } = require('node:timers')

  /**
   * Checks original reset observations against primitive frames and immediate evidence rows.
   * @param {object[]} cycles Archived cycles.
   * @param {object[]} rows All evidence rows in write order.
   * @param {boolean} closeOnly Whether destruction must emit close without error or end.
   * @returns {void}
   */
  const resetDiagnostics = (cycles, rows, closeOnly) => {
    for (const cycle of cycles.filter(({ reset }) => reset)) {
      const { resetTarget: target, reset, steps, provenance, held, handler } = cycle
      expect(target).toMatchObject({
        socket: reset.oldSocket,
        nodeId: reset.nodeId,
        epoch: reset.oldEpoch,
        acquisition: expect.any(Number),
        dispatch: expect.any(Number),
        held,
      })
      expect(provenance.acquisitions).toEqual(
        expect.arrayContaining([expect.objectContaining({ id: target.acquisition })])
      )
      expect(steps.map(({ step }) => step)).toEqual(
        expect.arrayContaining([
          'reset-target',
          'socket-close',
          'reconnect-start',
          'reconnect-end',
          'post-reset-api78',
          'SESSION_RESET',
          'held-redelivery',
        ])
      )
      expect(steps.find(({ step }) => step === 'socket-close')).toMatchObject({
        socket: target.socket,
        endpoint: target.endpoint,
        nodeId: target.nodeId,
        hadError: !closeOnly,
        destroyed: true,
      })
      if (closeOnly)
        expect(steps.some(({ step }) => step === 'socket-error' || step === 'socket-end')).toBe(
          false
        )
      else expect(steps.some(({ step }) => step === 'socket-error')).toBe(true)
      const firstWrites = steps.filter(({ step }) => step === 'post-reset-api78')
      expect(new Set(firstWrites.map(({ nodeId }) => nodeId)).size).toBe(3)
      expect(firstWrites).toHaveLength(3)
      for (const written of firstWrites) {
        const exchange = provenance.exchanges.find(({ id }) => id === written.exchange)
        const frame = exchange.frames.find(({ id }) => id === written.frame)
        expect(written).toMatchObject({
          at: frame.at,
          socket: frame.socket,
          endpoint: frame.endpoint,
          epoch: frame.request.shareSessionEpoch,
          attempted: true,
        })
      }
      const reopen = firstWrites.find(({ nodeId }) => nodeId === target.nodeId)
      expect(reopen).toMatchObject({ epoch: 0, socket: reset.nextSocket })
      const replays = steps.filter(({ step }) => step === 'held-redelivery')
      expect(replays.map(({ identity }) => identity).sort()).toEqual([...held].sort())
      for (const replay of replays) {
        expect(replay.deliveryCount).toBe(2)
        expect(handler).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              identity: replay.identity,
              acquisition: replay.acquisition,
              dispatch: replay.dispatch,
              deliveryCount: replay.deliveryCount,
            }),
          ])
        )
      }
      expect(
        rows.filter(({ event, cycle: id }) => event === 'soak-cycle-step' && id === cycle.cycle)
      ).toEqual(
        steps.map(({ at, ...step }) => expect.objectContaining({ ...step, monotonicAt: at }))
      )
    }
  }

  test.each([
    ['status debounce alone', null, true],
    ['status debounce with an attributed product timer', 'attributed', false],
    ['status debounce with an unattributed product timer', 'unattributed', false],
  ])(
    'judges a quiet observation containing %s',
    async (_name, product, ok) => {
      const reporter = {
        type: 'Timeout',
        asyncId: Number.MAX_SAFE_INTEGER,
        triggerAsyncId: 1,
        stack:
          'Error:\n' +
          '    at setTimeout (node:timers:125:19)\n' +
          '    at Status._debouncedEmit (/ci/node_modules/@jest/reporters/build/index.js:1507:7)',
        hasRef: true,
        durationMs: 100,
        repeatMs: null,
      }
      let observing = false
      let timer
      let phaseEnds = 0
      const snapshot = ShareTimerObserver.prototype.snapshot
      const injected = jest
        .spyOn(ShareTimerObserver.prototype, 'snapshot')
        .mockImplementation(function () {
          const inventory = snapshot.call(this)
          return observing ? { ...inventory, rows: [...inventory.rows, reporter] } : inventory
        })
      try {
        const { result, error, reason, rows } = await schedule({
          observeRow: (row) => {
            // Final cleanup independently requires the raw process counts to return to baseline.
            if (row.event === 'soak-resources' && row.kind === 'phase-end' && ++phaseEnds === 3) {
              if (product) expect(timer.hasRef()).toBe(true)
              observing = false
              realClearTimeout(timer)
            }
            if (row.event !== 'soak-cycle' || row.cycle !== 9) return
            observing = true
            if (product) timer = realSetTimeout(() => {}, 600000)
          },
          processResources: (original) => {
            const captured = original.timerOwners.afterCollection.rows
            const status = captured.find(({ asyncId }) => asyncId === reporter.asyncId)
            const owners = { rows: status ? [status] : [], dropped: 0 }
            if (status && product) {
              const retained = original.timerOwners.afterCollection.rows.find(
                ({ durationMs }) => durationMs === 600000
              )
              expect(retained).toMatchObject({ type: 'Timeout', hasRef: true })
              expect(ShareTimerObserver.isStatusDebounce(retained)).toBe(false)
              if (product === 'attributed') owners.rows.push(retained)
            }
            return { counts: status ? { Timeout: product ? 2 : 1 } : {}, owners }
          },
        })
        expect(error).toBeUndefined()
        expect(result.ok).toBe(ok)
        const points = rows.filter(
          ({ event, kind }) => event === 'soak-resources' && kind === 'phase-end'
        )
        expect(points).toHaveLength(3)
        for (const { snapshot: point } of points) {
          expect(point.activeResources).toEqual({ Timeout: product ? 2 : 1 })
          expect(point.activeResourcesBeforeCollection).toEqual({ Timeout: product ? 2 : 1 })
          for (const owners of Object.values(point.timerOwners)) {
            expect(owners.harnessAsyncIds).toEqual([reporter.asyncId])
            expect(owners.rows).toContainEqual(reporter)
            expect(owners.unattributed.Timeout).toBe(product === 'unattributed' ? 1 : 0)
          }
        }
        if (product) {
          expect(reason).toContain('share-steady: activeResources.Timeout baseline=0 after=1')
          expect(reason).toContain(
            'share-steady: activeResourcesBeforeCollection.Timeout baseline=0 after=1'
          )
          expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
        } else {
          expect(result.violations).toEqual([])
          expect(result.counts).toMatchObject({ cycles: 30, resets: 10 })
          expect(rows.some(({ event }) => event === 'case-complete')).toBe(true)
        }
      } finally {
        realClearTimeout(timer)
        injected.mockRestore()
      }
    },
    120000
  )

  test.each([true, false])(
    'recovers ten reset cycles with original close-only fake sockets, short=%s',
    async (short) => {
      const { rows, result, error } = await schedule({ short, closeOnly: true })
      expect(error).toBeUndefined()
      expect(result).toMatchObject({
        ok: true,
        violations: [],
        counts: { cycles: 30, resets: 10, memberAdds: 30, memberRemoves: 30 },
      })
      const cycles = rows.filter(({ event }) => event === 'soak-cycle')
      expect(cycles).toHaveLength(30)
      expect(cycles.filter(({ reset }) => reset)).toHaveLength(10)
      resetDiagnostics(cycles, rows, true)
    },
    120000
  )

  test('archives active reset provenance before delayed disconnect joins and keeps the later cleanup separate', async () => {
    let release
    let archived
    const { rows, result, error } = await schedule({
      io: async (event, { driver }) => {
        if (
          event === 'workload-disconnect' &&
          driver.lastCycle?.timing.cycle === 10 &&
          driver.lastCycle.members.bRemoved
        )
          await new Promise((resolve) => {
            release = resolve
          })
      },
      observeRow: (row) => {
        if (row.event !== 'soak-cycle' || row.status !== 'failed' || row.cycle !== 10) return
        archived = JSON.parse(JSON.stringify(row))
        release()
      },
    })
    expect(result?.ok).not.toBe(true)
    expect(error).toBeDefined()
    const failed = rows.find(
      ({ event, status, cycle }) => event === 'soak-cycle' && status === 'failed' && cycle === 10
    )
    expect(failed.provenance.exchanges.length).toBeGreaterThan(0)
    expect(failed.handler.length).toBeGreaterThan(0)
    expect(failed.memberObservations.find(({ member }) => member === 'A')).toMatchObject({
      disconnected: false,
      finalized: false,
    })
    expect(failed.snapshot.workloadJoined).toBe(false)
    expect(failed.cleanup.clientsDisconnected).toBe(false)
    const cleanup = rows.find(({ event, cycle }) => event === 'soak-cycle-cleanup' && cycle === 10)
    expect(cleanup.observation.snapshot).toMatchObject({
      workloadJoined: true,
      at: expect.any(Number),
    })
    expect(cleanup.observation.snapshot.at).toBeGreaterThanOrEqual(failed.snapshot.at)
    expect(cleanup.observation.memberObservations.every(({ disconnected }) => disconnected)).toBe(
      true
    )
    expect(rows.indexOf(cleanup)).toBeGreaterThan(rows.indexOf(failed))
    expect(failed).toEqual(archived)
    expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
  }, 120000)

  test.each([
    [true, 0],
    [false, 0],
    [true, 3],
    [false, 3],
  ])(
    'reconstructs thirty real adapter cycles and ten actual socket resets, short=%s, idleWakeMs=%s',
    async (short, idleWakeMs) => {
      const { owner, rows, result, error, records } = await schedule({ short, idleWakeMs })
      expect(error).toBeUndefined()
      expect(result).toMatchObject({
        ok: true,
        violations: [],
        counts: { cycles: 30, resets: 10, memberAdds: 30, memberRemoves: 30 },
      })
      expect(records.map((rows) => rows.length)).toEqual([600, 600, 600])
      const cycles = rows.filter(({ event }) => event === 'soak-cycle')
      expect(cycles).toHaveLength(30)
      expect(rows.find(({ event }) => event === 'soak-schedule')).toMatchObject({
        selector: short ? 'SHARE_SOAK_E2E' : 'SHARE_SOAK_FULL',
        soakProfile: short ? 'share-consumer-short' : 'share-consumer',
        stem: short ? 'share-soak' : 'share-soak-full',
        schedule: { slotMs: short ? 32000 : 64000, profileMs: short ? 960000 : 1920000 },
      })
      const configuration = rows.find(({ event }) => event === 'soak-configuration').config
      expect(configuration).toMatchObject({
        groupId: owner.driver.group,
        topic: owner.driver.topic,
        topicId: SOAK_UUID,
      })
      expect(configuration.reads).toHaveLength(4)
      expect(configuration.effective).toHaveLength(3)
      for (const { timings } of configuration.effective) {
        expect(timings['heartbeat.interval.ms']).toMatchObject({
          configValue: '5000',
          configSource: 5,
          fallback: false,
        })
        expect(timings['assignment.interval.ms']).toMatchObject({
          configValue: '1000',
          configSource: 4,
          fallback: true,
        })
      }
      for (const cycle of cycles) {
        const at = (step) => cycle.steps.find((row) => row.step === step).at
        expect(at('a-membership-end') - at('a-membership-start')).toBeGreaterThanOrEqual(5000)
        expect(at('member-add-end') - at('member-add-start')).toBeGreaterThanOrEqual(5000)
        expect(at('member-remove-end') - at('member-remove-start')).toBeGreaterThanOrEqual(4500)
        const start = cycle.verifier.events.find(({ kind }) => kind === 'window-start')
        expect(start.at - cycle.verifier.openedAt).toBeGreaterThanOrEqual(5000)
        const beats = cycle.verifier.provenance.heartbeats
        const joined = beats.filter(
          ({ request, response }) => request.memberEpoch === 0 && response?.errorCode === 0
        )
        expect(new Set(joined.map(({ memberId }) => memberId)).size).toBe(3)
        for (const beat of beats.filter(({ response }) => response?.errorCode === 0)) {
          expect(beat.response.heartbeatIntervalMs).toBe(beat.request.memberEpoch === -1 ? 0 : 5000)
          expect(beat.answeredAt).toBeGreaterThanOrEqual(beat.at)
          expect(beat.frames).toEqual(
            expect.arrayContaining([
              expect.objectContaining({
                apiKey: 76,
                request: expect.objectContaining({
                  groupId: owner.driver.group,
                  memberId: beat.memberId,
                }),
                responseBytes: expect.any(String),
                answeredAt: expect.any(Number),
              }),
            ])
          )
        }
      }
      expect(
        new Set([...cycles.map(({ verifier }) => verifier.memberId), owner.final.verifier.memberId])
          .size
      ).toBe(31)
      expect(cycles.map(({ positions }) => positions[1].ends.map(({ high }) => high))).toEqual(
        Array.from({ length: 30 }, (_, c) => Array(3).fill(String(20 * (c + 1))))
      )
      expect(
        cycles
          .filter(({ reset }) => reset)
          .every(
            ({ reset }) =>
              reset.oldSocket !== reset.nextSocket && reset.oldEpoch > 0 && reset.nextEpoch === 0
          )
      ).toBe(true)
      expect(rows.filter(({ event }) => event === 'soak-reset')).toHaveLength(10)
      resetDiagnostics(cycles, rows, false)
      for (const verifier of [...cycles.map(({ verifier }) => verifier), owner.final.verifier])
        expect(
          verifier.events
            .filter(({ kind }) => kind === 'window-start' || kind === 'window-end')
            .map(({ kind }) => kind)
        ).toEqual(['window-start', 'window-end'])
      for (const { held, handler, reset } of cycles) {
        if (!reset) continue
        expect(held).toHaveLength(20)
        for (const identity of held)
          expect(
            handler
              .filter((entry) => entry.identity === identity)
              .map((entry) => entry.deliveryCount)
          ).toEqual([1, 2])
      }
      expect(
        cycles.every(({ verifier }) => {
          const start = verifier.events.find(({ kind }) => kind === 'window-start')
          const end = verifier.events.find(({ kind }) => kind === 'window-end')
          return end.at - start.at >= 5000
        })
      ).toBe(true)
      expect(owner.final.verifier.openedAt).toBeGreaterThanOrEqual(
        owner.timing.phases.at(-1).completedAt
      )
      expect(result.resources.measured.phases.every(({ heapSamples }) => heapSamples >= 6)).toBe(
        true
      )
      expect(owner.elapsedProfileMs).toBe((short ? 960000 : 1920000) + 3 * idleWakeMs)
      for (const [index, slot] of owner.timing.slots.entries()) {
        const phase = owner.timing.phases[Math.floor(index / 10)]
        expect(slot.due).toBe(phase.startedAt + ((index % 10) + 1) * (short ? 32000 : 64000))
        expect(slot.completedAt).toBe(slot.due + idleWakeMs)
        expect(slot.startedAt).toBeGreaterThanOrEqual(
          index ? owner.timing.slots[index - 1].completedAt : phase.startedAt
        )
        expect(cycles[index].timing.completedAt).toBeLessThanOrEqual(slot.due)
      }
      for (const [index, phase] of owner.timing.phases.entries()) {
        expect(phase.completedAt).toBe(phase.due + idleWakeMs)
        if (index)
          expect(phase.startedAt).toBeGreaterThanOrEqual(owner.timing.phases[index - 1].completedAt)
      }
      expect(owner.budget).toMatchObject({
        watchdogMs: short ? 2040000 : 3000000,
        lifecycleMs: short ? 2460000 : 3420000,
      })
    },
    120000
  )

  test.each([
    [
      'a drifting slot deadline',
      (owner) => owner.timing.slots[5].due++,
      'the original slot, phase or stage timestamps do not prove the fixed schedule',
    ],
    [
      'a profile shorter than declared',
      (owner) => owner.elapsedProfileMs--,
      'the actual uncapped profile duration does not reach the declared schedule',
    ],
    [
      'profile overshoot beyond its margin',
      (owner) => (owner.elapsedProfileMs += 10001),
      'the actual uncapped profile overshoot exceeds the 10000 ms unallocated margin',
    ],
  ])(
    'rejects %s when reconstructing the complete original-owner schedule',
    async (_name, damage, message) => {
      const { result, reason, rows } = await schedule({
        finalDamage: (_final, { owner }) => damage(owner),
      })
      expect(result?.ok).not.toBe(true)
      expect(reason).toContain(message)
      expect(
        rows.filter(({ event, status }) => event === 'soak-cycle' && status === 'passed')
      ).toHaveLength(30)
      expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
    },
    120000
  )

  test.each([
    [
      'missing original receipt',
      (row) => {
        row.provenance.outcomes.pop()
      },
      'successful Accept',
    ],
    [
      'misbound receipt',
      (row) => {
        row.provenance.exchanges.find((e) => e.tickets.length).generation++
      },
      'partition receipt',
    ],
    [
      'same acquisition dispatched twice',
      (row) => {
        row.handler.push({ ...row.handler[0] })
      },
      'dispatched more than once',
    ],
    [
      'incomplete verifier partition coverage',
      (row) => {
        row.verifier.provenance.exchanges = row.verifier.provenance.exchanges.filter(
          (e) =>
            !e.response?.responses.some((t) => t.partitions.some((p) => p.partitionIndex === 2))
        )
      },
      'measured observation window',
    ],
    [
      'foreign verifier group',
      (row) => {
        row.verifier.provenance.exchanges.forEach((e) => {
          e.groupId = 'foreign'
        })
      },
      'measured observation window',
    ],
    [
      'missing verifier close',
      (row) => {
        delete row.verifier.closedAt
      },
      'measured observation window',
    ],
    [
      'shortened primitive window',
      (row) => {
        const start = row.verifier.events.find(({ kind }) => kind === 'window-start')
        row.verifier.events.find(({ kind }) => kind === 'window-end').at = start.at + 4999
      },
      'measured observation window',
    ],
    [
      'replayed old window',
      (row, { firstWindow }) => {
        if (row.cycle === 1) row.verifier = firstWindow
      },
      'original cycle window',
    ],
    [
      'replayed old terminal reads',
      (row, { rows }) => {
        if (row.cycle === 1) row.positions = rows.find((r) => r.event === 'soak-cycle').positions
      },
      'terminal listings',
    ],
    [
      'late prior-cycle entry',
      (row, { owner }) => {
        if (row.cycle === 1) owner.driver.onLateEntry([row.handler[0]])
      },
      'A joined workload member dispatched later identities',
    ],
    [
      'missing reset',
      (row) => {
        if (row.cycle === 10) row.reset = null
      },
      'no original session reset',
    ],
    [
      'slot completion one millisecond late',
      (row, { clock }) => {
        clock.skew(32001)
      },
      'absolute cutoff',
    ],
  ])(
    'rejects %s through the same owner and reconstruction',
    async (_name, damage, message) => {
      const { result, reason, rows } = await schedule({ damage })
      expect(result?.ok).not.toBe(true)
      expect(reason).toContain(message)
      expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
    },
    120000
  )

  test.each([1500, 4999])(
    'a real verifier replay at +%i fails before another cycle can start',
    async (replayAt) => {
      const { reason, rows } = await schedule({ replayAt })
      expect(reason).toContain('the same-group verifier was inactive or received a record')
      const cycles = rows.filter(({ event }) => event === 'soak-cycle')
      expect(cycles.map(({ cycle }) => cycle)).toEqual([0])
      expect(cycles[0].verifier.delivered).toHaveLength(1)
      const verifier = cycles[0].verifier
      const start = verifier.events.find(({ kind }) => kind === 'window-start')
      const response = verifier.provenance.exchanges.find(
        (exchange) =>
          exchange.memberId === verifier.memberId &&
          ShareCycleOracle.partitionResponse(exchange, {
            topicId: verifier.topicId,
            partition: 0,
          })?.acquiredRecords?.length === 1
      )
      expect(response).toMatchObject({
        frames: [expect.objectContaining({ answeredAt: start.at + replayAt })],
      })
    },
    120000
  )

  test('the exact screen detects a listener retained by an archived closed socket', async () => {
    const { result, reason, rows } = await schedule({
      socketClosed: (socket) => socket.on('leaked', () => {}),
    })
    expect(result.ok).toBe(false)
    expect(reason).toMatch(/namedListeners\.owner\d+:leaked baseline=0 after=1/)
    expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
  }, 120000)

  test('keeps real verifier observations through its disconnect callback and join', async () => {
    const { reason, rows } = await schedule({ drainReplay: true })
    expect(reason).toContain('verifier')
    expect(rows.find(({ event }) => event === 'soak-cycle').verifier.delivered).toHaveLength(1)
  }, 120000)

  test('starts each complete window only after delayed original assignment and fetching', async () => {
    const { owner, result, error, rows } = await schedule({ assignmentDelay: 2000 })
    expect(error).toBeUndefined()
    expect(result.ok).toBe(true)
    const cycles = rows.filter(({ event }) => event === 'soak-cycle')
    expect(cycles).toHaveLength(30)
    for (const { verifier } of [...cycles, owner.final]) {
      const start = verifier.events.find(({ kind }) => kind === 'window-start')
      const end = verifier.events.find(({ kind }) => kind === 'window-end')
      const assigned = verifier.provenance.heartbeats.find(
        ({ memberId, response }) =>
          memberId === verifier.memberId &&
          response?.assignment?.topicPartitions.some(({ partitions }) => partitions.length > 0)
      )
      expect(assigned).toBeDefined()
      expect(assigned.answeredAt - assigned.at).toBeGreaterThanOrEqual(2000)
      expect(start.at - verifier.openedAt).toBeGreaterThanOrEqual(7000)
      expect(
        verifier.provenance.exchanges
          .filter(({ apiKey, memberId }) => apiKey === 78 && memberId === verifier.memberId)
          .flatMap(({ frames }) => frames)
          .some(
            ({ at, answeredAt, responseBytes }) =>
              responseBytes && at >= assigned.answeredAt && answeredAt <= start.at
          )
      ).toBe(true)
      expect(end.at - start.at).toBeGreaterThanOrEqual(5000)
    }
  }, 120000)

  test.each([
    [
      'silent wire partitions',
      ({ owner, result }) => {
        if (owner.driver.lastCycle?.members.aLeft) result.responses = []
      },
    ],
    [
      'partial wire coverage',
      ({ owner, result }) => {
        if (owner.driver.lastCycle?.members.aLeft)
          result.responses = result.responses.map((topic) => ({
            ...topic,
            partitions: topic.partitions.filter((p) => p.partitionIndex !== 2),
          }))
      },
    ],
  ])(
    'rejects %s with original corpus and public requests intact',
    async (_name, wire) => {
      const { reason, rows } = await schedule({ wire })
      expect(reason).toContain('measured observation window')
      expect(rows.filter(({ event }) => event === 'soak-cycle').map(({ cycle }) => cycle)).toEqual([
        0,
      ])
    },
    120000
  )

  test.each([
    [
      'slot exhausted before verifier admission',
      'terminal-ends',
      ({ driver }) => driver.lastCycle?.positions.length === 0,
      'exhaust',
      'complete verifier window',
    ],
    [
      'post-verifier read crosses slot by one',
      'terminal-ends',
      ({ driver }) => Boolean(driver.lastCycle?.verifier),
      null,
      'absolute cutoff',
    ],
    [
      'verifier leave crosses slot by one',
      'verifier-disconnect',
      () => true,
      null,
      'absolute cutoff',
    ],
    [
      'cumulative phase transitions',
      'phase-open',
      () => true,
      2000,
      'shared transition deadline exhausted',
    ],
    [
      'combined final proof and cleanup',
      'admin-disconnect',
      ({ driver }) => Boolean(driver.after),
      16000,
      'shared cleanup deadline exceeded',
    ],
  ])(
    'rejects %s without renewing its original allocation',
    async (_name, selected, eligible, extra, message) => {
      const { owner, reason, rows } = await schedule({
        io: async (event, context) => {
          if (event !== selected || !eligible(context)) return
          const { driver, clock } = context
          clock.skew(
            extra === 'exhaust'
              ? driver.lastCycle.timing.due - performance.now() - 4999
              : (extra ?? driver.lastCycle.timing.due - performance.now() + 1)
          )
        },
      })
      expect(reason).toContain(message)
      if (selected === 'phase-open')
        expect(owner.spent.transition).toBeGreaterThan(owner.budget.transitionMs)
      if (selected === 'admin-disconnect') {
        expect(owner.final.timing.completedAt - owner.final.timing.startedAt).toBeLessThan(
          owner.budget.cleanupMs
        )
        expect(owner.spent.cleanup).toBeGreaterThan(owner.budget.cleanupMs)
      }
      expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
    },
    120000
  )

  test.each([true, false])(
    'rejects the original anchored slot cutoff plus one millisecond, short=%s',
    async (short) => {
      let crossed = false
      const { owner, reason, rows } = await schedule({
        short,
        io: async (event, { driver, clock }) => {
          if (crossed || event !== 'terminal-ends' || !driver.lastCycle?.verifier) return
          crossed = true
          clock.skew(driver.lastCycle.timing.due - performance.now() + 1)
        },
      })
      expect(crossed).toBe(true)
      expect(reason).toContain('absolute cutoff')
      expect(owner.timing.slots).toHaveLength(1)
      const [slot] = owner.timing.slots
      expect(slot.due).toBe(owner.timing.phases[0].startedAt + (short ? 32000 : 64000))
      expect(slot.completedAt).toBe(slot.due + 1)
      expect(owner.counts.cycles).toBe(0)
      expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
    },
    120000
  )

  test.each(['terminal-ends', 'verifier-disconnect'])(
    'holds the next cycle while %s is unresolved',
    async (selected) => {
      let held = false
      let unchanged = false
      const { result, reason } = await schedule({
        io: async (event, { owner, driver }) => {
          if (
            held ||
            event !== selected ||
            owner.counts.cycles !== 0 ||
            (selected === 'terminal-ends' && !driver.lastCycle?.verifier)
          )
            return
          held = true
          const prefix = [...driver.acknowledged]
          await new Promise((resolve) => setTimeout(resolve, 1000))
          unchanged =
            owner.counts.cycles === 0 &&
            JSON.stringify(driver.acknowledged) === JSON.stringify(prefix)
        },
      })
      expect(reason).toBe('')
      expect(held).toBe(true)
      expect(unchanged).toBe(true)
      expect(result.ok).toBe(true)
    },
    120000
  )

  test('a final window cannot replace the thirty original cycle windows', async () => {
    const { reason, rows } = await schedule({
      damage: (row) => {
        row.verifier.events = []
      },
    })
    expect(reason).toContain('measured observation window')
    expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
  }, 120000)

  test.each([
    [
      'replayed cycle window',
      (final, { rows }) => {
        final.verifier = rows.find((row) => row.event === 'soak-cycle').verifier
      },
    ],
    [
      'missing final response',
      (final) => {
        final.verifier.provenance.exchanges.forEach((e) => {
          e.frames = []
        })
      },
    ],
    [
      'missing final close',
      (final) => {
        delete final.verifier.closedAt
      },
    ],
  ])(
    'rejects %s through the final acceptance path',
    async (_name, finalDamage) => {
      const { reason, rows } = await schedule({ finalDamage })
      expect(reason).toContain('final:')
      expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
    },
    120000
  )
})

/**
 * A complete, flat telemetry resident-size reference of the versioned catalogue: every reading has
 * its phase, time and counters with `retained = heapUsed + external`, settled readings carry real
 * collection facts, the control agrees with itself and replays its frozen catalogue, every peak
 * covers its own and every earlier current reading, so only a deliberate change fails.
 */
const AT = { startup: 0, loaded: 1, c0: 2, c1: 3, 'control-released': 4, settled: 10, final: 20 }
const SETTLED = new Set(['c0', 'c1', 'control-released', 'settled'])
const reading = (phase, over = {}) => {
  const r = {
    phase,
    at: AT[phase],
    rss: 70 * MIB,
    heapUsed: 16 * MIB,
    external: 4 * MIB,
    arrayBuffers: 1 * MIB,
    hwm: 80 * MIB,
    rawMaxRss: 300 * MIB,
    ...over,
  }
  r.retained = r.heapUsed + r.external
  if (SETTLED.has(phase)) r.gc = { admittedAt: r.at - 1, completedAt: r.at, observed: 2 }
  return r
}
/** A settled probe reading whose `retainedBytes` is the given value. */
const settledMemory = (retainedBytes = 10000000, over = {}) => {
  const r = reading('settled', { heapUsed: retainedBytes - 4 * MIB, ...over })
  return { ...r, retainedBytes: r.retained }
}
/** Grows the heap of one settled reading by `bytes`, keeping its arithmetic. */
const grow = (memory, bytes) => {
  memory.heapUsed += bytes
  memory.retained += bytes
  memory.retainedBytes += bytes
}
const host = () => ({
  cpus: 4,
  load1: 0.4,
  loadPerCpu: 0.1,
  freeBytes: 1024 * MIB,
  pressure: { available: true, someAvg10: 0 },
})
const operations = [0, 1].map((n) => ({
  n,
  op: 'send',
  topic: 'ctl',
  valueBytes: 7,
  expected: 'fulfilled',
}))
const outcomes = () => [0, 1].map((n) => ({ n, status: 'fulfilled', at: 2 + n / 10 }))
const reference = () => ({
  version: 'rss-reference-2',
  source: 'proc-status-vmhwm',
  units: 'bytes',
  capability: { gc: true, peak: true },
  startup: reading('startup', { rss: 40 * MIB, hwm: 41 * MIB }),
  loaded: reading('loaded', { rss: 68 * MIB, hwm: 70 * MIB }),
  control: {
    manifest: { kind: 'producer', topic: 't', joins: false, operations, lifecycles: 1 },
    outcomes: outcomes(),
    telemetryFrames: 0,
    openSockets: 0,
    leftoverHandles: 0,
    c0: reading('c0'),
    c1: reading('c1'),
    released: reading('control-released'),
  },
  foreground: outcomes(),
  native: {
    journal: {
      rows: [
        {
          id: 1,
          codec: 'gzip',
          outcome: 'resolved',
          submitted: { rss: 70 * MIB, external: 4 * MIB },
          settled: { rss: 70 * MIB, external: 4 * MIB },
        },
      ],
      maxLive: 1,
      live: 0,
      overflow: false,
    },
    sampler: {
      count: 2,
      windows: 1,
      maxGapMs: 0,
      intervalMs: 20,
      peak: { rss: 70 * MIB, heapUsed: 16 * MIB, external: 4 * MIB, arrayBuffers: MIB },
    },
  },
  final: reading('final'),
  host: { before: host(), boundary: host(), after: host() },
})

describe('telemetry resource evidence', () => {
  const snapshot = (bytes = 10000000, overrides = {}) => ({
    memory: settledMemory(bytes),
    registryDigest: 'retained-state',
    identities: {
      sockets: [],
      'resources:fixture': [],
      'resources:target': [],
      'resources:unknown': [],
      'listener:socket1:data': [1],
    },
    ...overrides,
  })
  const run = (count = 1) => ({
    clientCount: count,
    baseline: snapshot(),
    samples: Array.from({ length: 11 }, (_, i) => ({
      admittedAtMs: 60000 + i * 29900,
      completedAtMs: 60300 + i * 29900,
      snapshot: snapshot(),
    })),
    teardown: snapshot(),
    cleanupMs: 120000,
    measurementMs: 300000,
    reference: reference(),
  })
  const evaluate = (input) => TelemetryResourceVerdict.evaluate(input)

  test('rejects diagnostic or acceptance-ineligible inputs after JSON replay', () => {
    for (const markers of [
      { diagnostic: true },
      { acceptanceEligible: false },
      { diagnostic: true, acceptanceEligible: false },
    ]) {
      const input = JSON.parse(JSON.stringify({ ...run(), ...markers }))
      const verdict = evaluate(input)
      expect(verdict.ok).toBe(false)
      expect(verdict.outcome).toBe('failed')
      expect(verdict.violations).toEqual(['diagnostic inputs are not acceptance-eligible'])
      expect(verdict.inconclusive).toEqual([])
    }
  })

  test('preserves byte-identical unmarked verdicts and inputs for every outcome', () => {
    const expected = {
      ok: true,
      outcome: 'passed',
      violations: [],
      inconclusive: [],
      measured: {
        samples: 11,
        skippedSamples: 0,
        slopeBytesPerMinute: 0,
        endpointBytes: 0,
        controlBaseRss: 73400320,
        controlExternal: 4194304,
        setupPeak: 83886080,
        setupAllowanceBytes: 100663296,
        peak: 83886080,
        rssDelta: 10485760,
        externalPeak: 4194304,
        externalDelta: 0,
        nativeJobs: 1,
      },
    }
    for (const outcome of ['passed', 'failed', 'inconclusive']) {
      const input = run()
      if (outcome === 'failed') input.cleanupMs = 120001
      if (outcome === 'inconclusive') input.reference.control.c1.rss += 2097153
      const original = JSON.stringify(input)
      expect(JSON.stringify(evaluate(input))).toBe(
        JSON.stringify({
          ...expected,
          ok: outcome === 'passed',
          outcome,
          violations: outcome === 'failed' ? ['cleanup budget'] : [],
          inconclusive: outcome === 'inconclusive' ? ['unstable control'] : [],
        })
      )
      expect(JSON.stringify(input)).toBe(original)
    }
  })

  test('reports irregular slopes without judging five-minute slope or endpoint', () => {
    const input = run()
    input.samples.forEach((sample, index) => {
      const jitter = index === 0 ? 0 : index % 2 ? 37 : -19
      sample.admittedAtMs += jitter
      sample.completedAtMs += jitter
      grow(
        sample.snapshot.memory,
        Math.round((131072 * (sample.completedAtMs - input.samples[0].completedAtMs)) / 60000)
      )
    })
    const result = evaluate(input)
    expect(result.violations).not.toContain('retained slope')
    expect(result.measured.slopeBytesPerMinute).toBeCloseTo(131072, 0)
    expect(result.violations).not.toContain('retained endpoint')
    const plateau = run()
    grow(plateau.baseline.memory, 1048576)
    grow(plateau.teardown.memory, 1048576)
    plateau.samples.forEach((sample) => grow(sample.snapshot.memory, 1048576))
    expect(evaluate(plateau).measured.slopeBytesPerMinute).toBeCloseTo(0, 5)
  })

  test('registers the fixed telemetry profile and refuses shortened acceptance', () => {
    expect(parseArgs(['--profile', 'telemetry'])).toEqual({
      profile: 'telemetry',
      phaseSeconds: null,
    })
    expect(PROFILES.telemetry).toEqual([{ name: 'telemetry', workload: 'telemetry', ms: 360000 }])
    expect(() => parseArgs(['--profile', 'telemetry', '--phase-seconds', '1'])).toThrow('usage:')
    expect(parseArgs(['--profile', 'producer-churn', '--phase-seconds', '1']).phaseSeconds).toBe(1)
  })

  test('holds the literal memory and timing limits and accepts complete flat evidence', () => {
    expect(TELEMETRY).toEqual({
      warmupMs: 60000,
      measureMs: 300000,
      cleanupMs: 120000,
      caseMs: 540000,
      exitMs: 10000,
      sampleMs: 30000,
      sampleAt: [
        60000, 90000, 120000, 150000, 180000, 210000, 240000, 270000, 300000, 330000, 359000,
      ],
      minSamples: 10,
      minSpanMs: 270000,
      slopeBytesPerMinute: 65536,
      endpointBytes: 1048576,
      activeBytes: 8388608,
      rssDeltaBytes: 67108864,
      externalDeltaBytes: 67108864,
      setupPeakBytes: 100663296,
      peakBytes: 167772160,
      controlRssDriftBytes: 2097152,
      controlRetainedDriftBytes: 262144,
      noiseLoadPerCpu: 0.75,
      noiseFreeBytes: 268435456,
      noisePressure: 5,
    })
    expect(evaluate(run())).toMatchObject({
      ok: true,
      outcome: 'passed',
      violations: [],
      inconclusive: [],
      measured: {
        samples: 11,
        slopeBytesPerMinute: 0,
        endpointBytes: 0,
        controlBaseRss: 70 * MIB,
        peak: 80 * MIB,
        rssDelta: 10 * MIB,
        externalDelta: 0,
      },
    })
  })

  test.each([
    [
      'missing memory',
      (r) => {
        delete r.samples[2].snapshot.memory
      },
      'post-GC sample shape or retained state',
    ],
    [
      'missing baseline',
      (r) => {
        delete r.baseline.memory
      },
      'settled baseline and teardown',
    ],
    [
      'missing identities',
      (r) => {
        delete r.teardown.identities
      },
      'identity roles',
    ],
    [
      'an omitted required source role',
      (r) => {
        delete r.baseline.identities['resources:target']
        delete r.teardown.identities['resources:target']
      },
      'identity roles',
    ],
    [
      'same-count replacement',
      (r) => {
        r.teardown.identities['listener:socket1:data'] = [2]
      },
      'quiet identity listener:socket1:data',
    ],
    [
      'a surviving unknown handle',
      (r) => {
        r.teardown.identities['resources:unknown'] = [9]
      },
      'quiet identity resources:unknown',
    ],
    [
      'extra event role',
      (r) => {
        r.teardown.identities.unexpected = []
      },
      'identity roles',
    ],
    [
      'retained state changed',
      (r) => {
        r.teardown.registryDigest = 'other'
      },
      'retained descriptor and UUID baseline',
    ],
    [
      'sample state changed',
      (r) => {
        r.samples[2].snapshot.registryDigest = 'other'
      },
      'post-GC sample shape or retained state',
    ],
    [
      'too few samples',
      (r) => {
        r.samples.length = 9
      },
      'post-GC sample count',
    ],
    [
      'short run',
      (r) => {
        r.samples.forEach((s, i) => {
          s.admittedAtMs = 60000 + i
          s.completedAtMs = 60100 + i
        })
      },
      'measurement span',
    ],
    [
      'duplicate admission time',
      (r) => {
        r.samples[2].admittedAtMs = r.samples[1].admittedAtMs
      },
      'post-GC sample shape or retained state',
    ],
    [
      'a reading that completed before it was admitted',
      (r) => {
        r.samples[3].completedAtMs = r.samples[3].admittedAtMs - 1
      },
      'post-GC sample shape or retained state',
    ],
    [
      'a reading that completed after the window',
      (r) => {
        r.samples.at(-1).completedAtMs = 360001
      },
      'post-GC sample shape or retained state',
    ],
    [
      'a reading admitted before the window',
      (r) => {
        r.samples[0].admittedAtMs = 59999
      },
      'post-GC sample shape or retained state',
    ],
    [
      'cleanup overflow',
      (r) => {
        r.cleanupMs = 120001
      },
      'cleanup budget',
    ],
    [
      'a reading that failed after admission',
      (r) => {
        r.sampler = { skipped: [], readFailures: [{ admittedPhaseMs: 330000, message: 'late' }] }
      },
      'sampler read failure',
    ],
    [
      'active overflow',
      (r) => {
        grow(r.samples[5].snapshot.memory, 8388608 + 1)
      },
      'active retained ceiling',
    ],
  ])('rejects %s', (_name, change, label) => {
    const input = run()
    change(input)
    expect(evaluate(input).violations).toContain(label)
    expect(evaluate(input).ok).toBe(false)
  })

  test.each(['steep slope', 'last endpoint'])(
    'accepts %s within the remaining ceilings',
    (shape) => {
      const input = run()
      if (shape === 'steep slope')
        input.samples.forEach((sample, i) => grow(sample.snapshot.memory, i * 32769))
      else grow(input.samples.at(-1).snapshot.memory, 1048576 + 1)
      expect(evaluate(input)).toMatchObject({ ok: true, violations: [] })
    }
  )

  test('accepts closed growth above 256 KiB while keeping the active ceiling at equality', () => {
    const input = run()
    grow(input.teardown.memory, 262145)
    input.samples.forEach((s) => grow(s.snapshot.memory, 8388608))
    expect(evaluate(input).ok).toBe(true)
  })

  test('does not change inherited profile definitions', () => {
    expect(Object.keys(PROFILES)).toEqual([
      'share-consumer',
      'share-consumer-short',
      'idle',
      'member-churn',
      'regex-topic-churn',
      'outage',
      'producer-churn',
      'consumer-churn',
      'first-run',
      'telemetry',
    ])
    expect(PROFILES['producer-churn'].map(({ name, ms }) => [name, ms])).toEqual([
      ['steady', 300000],
      ['outage', 300000],
      ['buffer-pressure', 300000],
    ])
  })
})

describe('telemetry resident-size reference', () => {
  const snapshot = (memory = {}) => ({
    memory: settledMemory(10000000, memory),
    registryDigest: 'one-kind',
    identities: {
      sockets: [],
      'resources:fixture': [],
      'resources:target': [],
      'resources:unknown': [],
    },
  })
  const make = () => ({
    clientCount: 1,
    baseline: snapshot(),
    samples: Array.from({ length: 11 }, (_, i) => ({
      admittedAtMs: 60000 + i * 29900,
      completedAtMs: 60300 + i * 29900,
      snapshot: snapshot(),
    })),
    teardown: snapshot(),
    cleanupMs: 120000,
    measurementMs: 300000,
    reference: reference(),
  })
  const evaluate = (input) => TelemetryResourceVerdict.evaluate(input)
  /** Sets external memory and keeps `retained = heapUsed + external`. */
  const setExternal = (memory, external) => {
    memory.external = external
    memory.retained = memory.heapUsed + external
    if (memory.retainedBytes !== undefined) memory.retainedBytes = memory.retained
  }
  /** Raises the peak of one mid-run reading and everything after it, as a real high water would. */
  const raise = (input, hwm, external) => {
    const later = [
      ...input.samples.slice(4).map((s) => s.snapshot.memory),
      input.teardown.memory,
      input.reference.final,
    ]
    for (const memory of later) {
      memory.hwm = hwm
      if (external !== undefined) setExternal(memory, external)
    }
  }

  test('measures against the disabled control and no longer against an absolute 64 MiB', () => {
    const input = make()
    raise(input, 80 * MIB + 64 * MIB)
    const verdict = evaluate(input)
    expect(verdict.measured.rssDelta).toBe(74 * MIB)
    expect(verdict.violations).toEqual(['telemetry resident-size increase'])
    const inside = make()
    raise(inside, 70 * MIB + 64 * MIB)
    expect(evaluate(inside).measured.rssDelta).toBe(64 * MIB)
    expect(evaluate(inside)).toMatchObject({ ok: true, outcome: 'passed' })
    expect(evaluate(make()).measured.peak).toBe(80 * MIB)
  })

  test('records every peak decrease and retains the running maximum', () => {
    const input = make()
    raise(input, 89 * MIB)
    input.samples[4].snapshot.memory.hwm = 90 * MIB
    input.samples[6].snapshot.memory.hwm = 88 * MIB
    for (const row of [
      ...input.samples.slice(5).map((s) => s.snapshot.memory),
      input.teardown.memory,
      input.reference.final,
    ])
      row.rawMaxRss = 299 * MIB
    input.samples[6].snapshot.memory.rawMaxRss = 298 * MIB
    const original = JSON.stringify(input)
    const replay = JSON.parse(original)
    const verdict = evaluate(replay)
    expect(verdict).toMatchObject({ ok: true, outcome: 'passed', violations: [], inconclusive: [] })
    expect(verdict.measured).toMatchObject({
      peak: 90 * MIB,
      rssDelta: 20 * MIB,
      setupPeak: 80 * MIB,
    })
    expect(verdict.measured.peakDecreases).toEqual([
      {
        index: 11,
        phase: 'settled',
        field: 'rawMaxRss',
        before: 300 * MIB,
        after: 299 * MIB,
        delta: -MIB,
      },
      { index: 11, phase: 'settled', field: 'hwm', before: 90 * MIB, after: 89 * MIB, delta: -MIB },
      {
        index: 12,
        phase: 'settled',
        field: 'rawMaxRss',
        before: 299 * MIB,
        after: 298 * MIB,
        delta: -MIB,
      },
      { index: 12, phase: 'settled', field: 'hwm', before: 89 * MIB, after: 88 * MIB, delta: -MIB },
    ])
    expect(JSON.stringify(replay)).toBe(original)
  })

  test('preserves setup, delta and absolute limits after a peak decrease', () => {
    const input = make()
    input.reference.control.c0.hwm = 96 * MIB + 1
    input.samples[4].snapshot.memory.hwm = 160 * MIB + 1
    const verdict = evaluate(input)
    expect(verdict.measured).toMatchObject({
      peak: 160 * MIB + 1,
      setupPeak: 96 * MIB + 1,
      rssDelta: 90 * MIB + 1,
    })
    expect(verdict.violations).toEqual([
      'telemetry resident-size increase',
      'absolute resident-size peak',
    ])
    expect(verdict.inconclusive).toEqual(['setup peak above its allowance'])
    expect(verdict.measured.peakDecreases).toEqual([
      {
        index: 3,
        phase: 'c1',
        field: 'hwm',
        before: 96 * MIB + 1,
        after: 80 * MIB,
        delta: -16 * MIB - 1,
      },
      {
        index: 11,
        phase: 'settled',
        field: 'hwm',
        before: 160 * MIB + 1,
        after: 80 * MIB,
        delta: -80 * MIB - 1,
      },
    ])
  })

  test('holds the absolute backstop and the increase together at equality, one byte over fails both', () => {
    const equal = make()
    const readings = [
      equal.reference.control.c0,
      equal.reference.control.c1,
      equal.reference.control.released,
      equal.baseline.memory,
      ...equal.samples.map((s) => s.snapshot.memory),
      equal.teardown.memory,
      equal.reference.final,
    ]
    for (const r of readings) Object.assign(r, { rss: 96 * MIB, hwm: 96 * MIB })
    raise(equal, 160 * MIB)
    const verdict = evaluate(equal)
    expect(verdict.measured).toMatchObject({
      peak: 160 * MIB,
      setupPeak: 96 * MIB,
      rssDelta: 64 * MIB,
    })
    expect(verdict).toMatchObject({ ok: true, violations: [], inconclusive: [] })
    raise(equal, 160 * MIB + 1)
    expect(evaluate(equal).violations).toEqual([
      'telemetry resident-size increase',
      'absolute resident-size peak',
    ])
  })

  test('holds the external-memory increase at equality and fails one byte over', () => {
    const input = make()
    raise(input, 80 * MIB, 4 * MIB + 64 * MIB)
    expect(evaluate(input).violations).not.toContain('external-memory increase')
    raise(input, 80 * MIB, 4 * MIB + 64 * MIB + 1)
    expect(evaluate(input).violations).toContain('external-memory increase')
  })

  test('is inconclusive for an unstable control, an inflated setup or a missing capability', () => {
    const drift = make()
    drift.reference.control.c1.rss += 2 * MIB + 1
    expect(evaluate(drift)).toMatchObject({ ok: false, outcome: 'inconclusive' })
    expect(evaluate(drift).inconclusive).toContain('unstable control')
    const retained = make()
    retained.reference.control.c1.heapUsed += 262145
    retained.reference.control.c1.retained += 262145
    expect(evaluate(retained).inconclusive).toContain('unstable control')
    const steady = make()
    steady.reference.control.c1.rss += 2 * MIB
    expect(evaluate(steady).inconclusive).toEqual([])
    const setup = make()
    setup.reference.loaded.hwm = 96 * MIB + 1
    setup.reference.control.c0.hwm = 96 * MIB + 1
    setup.reference.control.c1.hwm = 96 * MIB + 1
    setup.reference.control.released.hwm = 96 * MIB + 1
    setup.baseline.memory.hwm = 96 * MIB + 1
    setup.samples.forEach((s) => (s.snapshot.memory.hwm = 96 * MIB + 1))
    setup.teardown.memory.hwm = 96 * MIB + 1
    setup.reference.final.hwm = 96 * MIB + 1
    expect(evaluate(setup).inconclusive).toContain('setup peak above its allowance')
    const capability = make()
    capability.reference.capability.gc = false
    expect(evaluate(capability)).toMatchObject({ ok: false, outcome: 'inconclusive' })
  })

  /** Every reading that reaches the setup peak or the overall peak, as one high water would. */
  const readingsOf = (input) => [
    input.reference.loaded,
    input.reference.control.c0,
    input.reference.control.c1,
    input.reference.control.released,
    input.baseline.memory,
    ...input.samples.map((s) => s.snapshot.memory),
    input.teardown.memory,
    input.reference.final,
  ]
  const setPeak = (input, hwm) => readingsOf(input).forEach((memory) => (memory.hwm = hwm))
  /** A share-consumer reference that records the allocation and setup allowance it was run under. */
  const shareInput = () => {
    const { declaredJournalBytes } = require('../telemetryChurn')
    const declared = declaredJournalBytes('shareConsumer', TELEMETRY)
    const input = make()
    input.reference.control.kind = 'shareConsumer'
    input.reference.journals = {
      allocation: { byteLength: declared },
      setupAllowanceBytes: 96 * MIB + declared,
    }
    return { input, declared }
  }

  test('allows the share consumer a setup peak of 96 MiB plus its exact declared journal bytes and no more', () => {
    const { input, declared } = shareInput()
    expect(declared).toBe(23192560)
    setPeak(input, 96 * MIB + declared)
    expect(evaluate(input)).toMatchObject({
      ok: true,
      outcome: 'passed',
      violations: [],
      inconclusive: [],
      measured: { setupPeak: 96 * MIB + declared, setupAllowanceBytes: 96 * MIB + declared },
    })
    setPeak(input, 96 * MIB + declared + 1)
    expect(evaluate(input)).toMatchObject({
      ok: false,
      outcome: 'inconclusive',
      violations: [],
      inconclusive: ['setup peak above its allowance'],
    })
  })

  test('keeps the 96 MiB setup allowance for every other client kind', () => {
    for (const kind of ['producer', 'consumer', 'admin']) {
      const input = make()
      input.reference.control.kind = kind
      setPeak(input, 96 * MIB)
      expect(evaluate(input)).toMatchObject({
        ok: true,
        measured: { setupPeak: 96 * MIB, setupAllowanceBytes: 96 * MIB },
      })
      setPeak(input, 96 * MIB + 1)
      expect(evaluate(input).inconclusive).toEqual(['setup peak above its allowance'])
    }
  })

  test('never raises the 64 MiB increase or the 160 MiB peak for the share consumer', () => {
    const { input } = shareInput()
    raise(input, 70 * MIB + 64 * MIB)
    expect(evaluate(input)).toMatchObject({ ok: true, violations: [], inconclusive: [] })
    raise(input, 70 * MIB + 64 * MIB + 1)
    expect(evaluate(input).violations).toEqual(['telemetry resident-size increase'])
    raise(input, 160 * MIB + 1)
    expect(evaluate(input).violations).toEqual([
      'telemetry resident-size increase',
      'absolute resident-size peak',
    ])
  })

  test('rejects a recorded setup allowance or declared allocation that differs from the recomputed value', () => {
    expect(evaluate(shareInput().input).violations).toEqual([])
    for (const [change, violations] of [
      [(journals) => journals.setupAllowanceBytes++, ['recorded setup allowance']],
      [(journals) => journals.setupAllowanceBytes--, ['recorded setup allowance']],
      [(journals) => (journals.setupAllowanceBytes = 96 * MIB), ['recorded setup allowance']],
      [(journals) => delete journals.setupAllowanceBytes, ['recorded setup allowance']],
      [(journals) => journals.allocation.byteLength++, ['declared journal bytes']],
      [(journals) => journals.allocation.byteLength--, ['declared journal bytes']],
      [(journals) => delete journals.allocation, ['declared journal bytes']],
    ]) {
      const { input } = shareInput()
      change(input.reference.journals)
      expect(evaluate(input)).toMatchObject({ ok: false, outcome: 'failed', violations })
    }
    const missing = shareInput().input
    delete missing.reference.journals
    expect(evaluate(missing).violations).toEqual([
      'recorded setup allowance',
      'declared journal bytes',
    ])
    const relabelled = shareInput().input
    relabelled.reference.control.kind = 'producer'
    expect(evaluate(relabelled).violations).toEqual(['recorded setup allowance'])
    const plain = make()
    plain.reference.journals = { setupAllowanceBytes: 96 * MIB + 1 }
    expect(evaluate(plain).violations).toEqual(['recorded setup allowance'])
    plain.reference.journals.setupAllowanceBytes = 96 * MIB
    expect(evaluate(plain)).toMatchObject({ ok: true, violations: [] })
  })

  test('lets a failure take precedence over an inconclusive measurement', () => {
    const input = make()
    input.reference.capability.peak = false
    input.reference.control.telemetryFrames = 2
    const verdict = evaluate(input)
    expect(verdict.outcome).toBe('failed')
    expect(verdict.violations).toContain('telemetry-disabled control')
    expect(verdict.inconclusive).toContain('collection or peak capability')
  })

  test.each([
    [
      'a control that sent telemetry',
      (r) => (r.reference.control.telemetryFrames = 1),
      'telemetry-disabled control',
    ],
    [
      'a control that left a handle',
      (r) => (r.reference.control.leftoverHandles = 1),
      'telemetry-disabled control',
    ],
    [
      'the old schema',
      (r) => (r.reference.version = 'rss-reference-0'),
      'resident-size reference facts',
    ],
    ['an absent reference', (r) => delete r.reference, 'resident-size reference facts'],
    ['a wrong peak source', (r) => (r.reference.source = 'maxrss'), 'resident-size peak source'],
    [
      'a peak below its own reading',
      (r) => {
        r.baseline.memory.rss = 85 * MIB
      },
      'resident-size peak series',
    ],
    [
      'a missing reading field',
      (r) => {
        delete r.reference.loaded.external
      },
      'resident-size counters',
    ],
    [
      'the first schema',
      (r) => (r.reference.version = 'rss-reference-1'),
      'resident-size reference facts',
    ],
    ['missing units', (r) => delete r.reference.units, 'resident-size peak source'],
    [
      'a missing arrayBuffers',
      (r) => delete r.samples[3].snapshot.memory.arrayBuffers,
      'resident-size counters',
    ],
    [
      'a missing raw maxRSS',
      (r) => delete r.reference.control.c0.rawMaxRss,
      'resident-size counters',
    ],
    ['a negative counter', (r) => (r.reference.final.rss = -1), 'resident-size counters'],
    [
      'retained that is not heapUsed plus external',
      (r) => (r.baseline.memory.retainedBytes += 1),
      'resident-size arithmetic',
    ],
    [
      'arrayBuffers outside external',
      (r) => (r.reference.loaded.arrayBuffers = r.reference.loaded.external + 1),
      'resident-size arithmetic',
    ],
    [
      'a nonfinite control retained value',
      (r) => (r.reference.control.c1.retained = NaN),
      'resident-size arithmetic',
    ],
    ['a missing phase label', (r) => delete r.teardown.memory.phase, 'resident-size reading phase'],
    [
      'a swapped phase label',
      (r) => (r.reference.control.c0.phase = 'c1'),
      'resident-size reading phase',
    ],
    ['a missing reading time', (r) => delete r.reference.startup.at, 'resident-size reading time'],
    [
      'readings out of chronological order',
      (r) => (r.reference.loaded.at = r.reference.control.c0.at + 1),
      'resident-size reading order',
    ],
    [
      'a settled reading without collection facts',
      (r) => delete r.samples[2].snapshot.memory.gc,
      'resident-size collection facts',
    ],
    [
      'a settled reading with one collection',
      (r) => (r.reference.control.released.gc.observed = 1),
      'resident-size collection facts',
    ],
    [
      'a collection completed elsewhere',
      (r) => (r.baseline.memory.gc.completedAt = r.baseline.memory.at + 1),
      'resident-size collection facts',
    ],
    [
      'a native journal overflow',
      (r) => (r.reference.native.journal.overflow = true),
      'native work journal',
    ],
    ['pending native work', (r) => (r.reference.native.journal.live = 1), 'native work journal'],
    [
      'an unsettled native job',
      (r) => (r.reference.native.journal.rows[0].outcome = null),
      'native work journal',
    ],
    [
      'native work the sampler did not cover',
      (r) => (r.reference.native.sampler.count = 1),
      'native work journal',
    ],
    ['a missing native journal', (r) => delete r.reference.native, 'native work journal'],
    [
      'an empty native journal',
      (r) => {
        r.reference.native.journal.rows = []
        r.reference.native.journal.maxLive = 0
      },
      'native work journal',
    ],
    [
      'a native journal with only rejected work',
      (r) => (r.reference.native.journal.rows[0].outcome = 'rejected'),
      'native work journal',
    ],
    [
      'an external spike only the native sampler saw',
      (r) => (r.reference.native.sampler.peak.external = 4 * MIB + 64 * MIB + 1),
      'external-memory increase',
    ],
    [
      'a resident spike at a native settlement',
      (r) => (r.reference.native.journal.rows[0].settled.rss = 70 * MIB + 64 * MIB + 1),
      'telemetry resident-size increase',
    ],
    [
      'a control operation that failed',
      (r) => (r.reference.control.outcomes[1].status = 'rejected'),
      'telemetry-disabled control',
    ],
    [
      'a control without its frozen manifest',
      (r) => delete r.reference.control.manifest,
      'telemetry-disabled control',
    ],
    [
      'an enabled worker that did not replay the control catalogue',
      (r) => r.reference.foreground.pop(),
      'enabled replay of the control catalogue',
    ],
    ['a missing host context', (r) => delete r.reference.host.boundary, 'host contexts'],
  ])('fails %s', (_name, change, label) => {
    const input = make()
    change(input)
    expect(evaluate(input).violations).toContain(label)
    expect(evaluate(input).ok).toBe(false)
  })

  test('keeps an unavailable peak source inconclusive rather than a malformed reading', () => {
    const input = make()
    for (const memory of [input.reference.startup, input.reference.final, input.baseline.memory])
      memory.hwm = null
    const verdict = evaluate(input)
    expect(verdict.violations).toEqual([])
    expect(verdict).toMatchObject({ ok: false, outcome: 'inconclusive' })
    expect(verdict.inconclusive).toContain('resident-size peak capability')
  })

  test('is inconclusive on a noisy host and keeps the unavailable pressure capability visible', () => {
    const loaded = make()
    loaded.reference.host.before.load1 = 3.1
    expect(evaluate(loaded).inconclusive).toContain('host noise before')
    const memory = make()
    memory.reference.host.after.freeBytes = 268435455
    expect(evaluate(memory).inconclusive).toContain('host noise after')
    const pressure = make()
    pressure.reference.host.boundary.pressure.someAvg10 = 5.01
    expect(evaluate(pressure).inconclusive).toContain('host noise boundary')
    const quiet = make()
    quiet.reference.host.before.pressure = { available: false }
    expect(evaluate(quiet)).toMatchObject({ ok: true, inconclusive: [] })
  })
})

describe('telemetry sampler ownership', () => {
  test('joins the existing teardown hook before applying the supplied verdict', async () => {
    const order = []
    const verdict = { ok: false, violations: ['held timer'] }
    const soak = new Soak({
      kafka: {},
      phases: [],
      context: {},
      settleMs: 0,
      probe: {
        snapshot: async () => {
          order.push('snapshot')
          return { sockets: { open: 0 } }
        },
      },
      clock: { now: () => 7 },
      record: () => {},
      beforeTeardown: async () => {
        order.push('teardown')
      },
      judge: () => {
        order.push('judge')
        return verdict
      },
    })
    expect(await soak.run()).toBe(verdict)
    expect(order).toEqual(['snapshot', 'teardown', 'snapshot', 'judge'])
  })

  /** A scheduler barrier and an admitted read controlled independently of the wall clock. */
  const deferred = () => {
    let resolve
    let reject
    const promise = new Promise((yes, no) => {
      resolve = yes
      reject = no
    })
    return { promise, resolve, reject }
  }
  const controlled = () => {
    let now = 0
    const held = deferred()
    const admitted = deferred()
    const workload = deferred()
    const rows = []
    let first = true
    const soak = new Soak({
      kafka: {},
      context: {},
      phases: [],
      settleMs: 0,
      sampleAt: [0, 20, 40, 60],
      clock: { now: () => now },
      probe: {
        snapshot: async () => {
          if (!first) return { sockets: { open: 0 } }
          first = false
          admitted.resolve()
          return held.promise
        },
      },
      record: (row) => rows.push(row),
      workloads: { telemetry: () => workload.promise },
    })
    const naps = []
    soak.nap = (ms) => {
      const next = deferred()
      naps.push({ ms, wake: next.resolve })
      soak.wake = next.resolve
      return next.promise
    }
    const running = soak.runPhase({ name: 'telemetry', workload: 'telemetry', ms: 100 }, 0)
    running.catch(() => {})
    return {
      soak,
      held,
      admitted,
      workload,
      rows,
      naps,
      running,
      at: (value) => {
        now = value
      },
    }
  }

  test('endSampling joins a held admitted read and admits nothing after its boundary', async () => {
    const c = controlled()
    c.naps[0].wake()
    await c.admitted.promise
    c.at(50)
    let joined = false
    const joining = c.soak.endSampling().then(() => {
      joined = true
    })
    await Promise.resolve()
    expect(joined).toBe(false)
    c.held.resolve({ sockets: { open: 0 } })
    await joining
    expect(c.soak.samples).toMatchObject([{ admittedPhaseMs: 0, phaseMs: 50 }])
    expect(c.naps).toHaveLength(1)
    c.workload.resolve()
    await c.running
    expect(c.soak.samples).toHaveLength(1)
  })

  test('records an overrun at its actual times and skips exactly the elapsed grid slots', async () => {
    const c = controlled()
    c.naps[0].wake()
    await c.admitted.promise
    c.at(50)
    const sampled = deferred()
    c.soak.record = (row) => {
      c.rows.push(row)
      if (row.kind === 'sample') sampled.resolve()
    }
    c.held.resolve({ sockets: { open: 0 } })
    await sampled.promise
    expect(c.soak.skipped).toEqual([20, 40])
    expect(c.naps[1].ms).toBe(10)
    expect(c.soak.samples[0]).toMatchObject({ admittedPhaseMs: 0, phaseMs: 50 })
    await c.soak.endSampling()
    c.workload.resolve()
    await c.running
    expect(c.soak.samples).toHaveLength(1)
  })

  test('waits out an early timer wake before admitting a planned sample', async () => {
    const c = controlled()
    const settle = async () => {
      for (let i = 0; i < 10; i++) await new Promise(setImmediate)
    }
    c.naps[0].wake()
    await c.admitted.promise
    c.held.resolve({ sockets: { open: 0 } })
    await settle()
    expect(c.naps[1].ms).toBe(20)
    c.at(19)
    c.naps[1].wake()
    await settle()
    expect(c.naps).toHaveLength(3)
    expect(c.naps[2].ms).toBe(1)
    expect(c.soak.samples).toHaveLength(1)
    c.at(20)
    c.naps[2].wake()
    await settle()
    expect(c.soak.samples).toHaveLength(2)
    expect(c.soak.samples[1]).toMatchObject({ admittedPhaseMs: 20 })
    expect(c.soak.skipped).toEqual([])
    await c.soak.endSampling()
    c.workload.resolve()
    await c.running
  })

  test('preserves a held read rejection when stopping and joins without another admission', async () => {
    const c = controlled()
    c.naps[0].wake()
    await c.admitted.promise
    const error = new Error('late read')
    const joining = c.soak.endSampling()
    joining.catch(() => {})
    c.at(50)
    c.held.reject(error)
    await expect(joining).rejects.toBe(error)
    expect(c.soak.readFailures).toEqual([{ admittedPhaseMs: 0, message: 'late read' }])
    c.workload.resolve()
    await expect(c.running).rejects.toBe(error)
    expect(c.naps).toHaveLength(1)
  })
})
