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
  WORKLOADS,
  Soak,
  createTopic,
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
      .filter(([, phases]) => phases.length === 1)
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
    namedListeners: {},
    heapUsed,
  })
  const phases = ['share-steady', 'share-reset', 'share-members']
  return {
    baseline: quietShare(),
    warmupMs: 10000,
    samples: phases.flatMap((phase, index) =>
      Array.from({ length: 6 }, (_, i) => ({
        phase,
        phaseMs: 12000 + i * 4000,
        snapshot: quietShare(10000000 + index * 10000000),
      }))
    ),
    phaseEnds: phases.map((phase) => ({ phase, snapshot: quietShare() })),
    teardown: quietShare(),
  }
}

describe('Share soak resource screen', () => {
  test('declares thirty cycles ten resets and the record bound before execution', () => {
    expect(SHARE_SOAK).toMatchObject({
      cycles: 30,
      resets: 10,
      maxRecords: 100,
      nodes: 3,
      shortMs: 480000,
      fullMs: 960000,
      endpointBytes: 2097152,
      slopeBytesPerMinute: 262144,
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
    ['share-consumer-short', 160000, 480000],
    ['share-consumer', 320000, 960000],
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
    const { setupMs, profileMs, transitionMs, cleanupMs, exitMs, totalMs } = budget
    expect(setupMs + profileMs + transitionMs + cleanupMs + exitMs).toBeLessThanOrEqual(totalMs)
    expect(profileMs).toBe(mode === 'short' ? SHARE_SOAK.shortMs : SHARE_SOAK.fullMs)
    expect(SHARE_SOAK.cycles * SHARE_SOAK.slotMs[mode]).toBe(profileMs)
    const phaseMs = profileMs / SHARE_SOAK.phases.length
    const readings = Math.floor((phaseMs - SHARE_SOAK.warmupMs) / SHARE_SOAK.sampleMs[mode])
    expect(readings).toBeGreaterThanOrEqual(6)
    expect(totalMs).toBe(mode === 'short' ? 540000 : 1020000)
    expect(budget.watchdogMs).toBe(totalMs)
    expect(budget.lifecycleMs).toBe(mode === 'short' ? 600000 : 1080000)
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

  test('wraps a factory so its sockets are observed', () => {
    const tap = new ShareFetchTap()
    const socket = fakeSocket()
    const made = tap.factory(() => socket)({ host: 'h', port: 9 })
    made.write(frame(78, 5))
    expect(made).toBe(socket)
    expect(tap.facts[0].endpoint).toBe('h:9')
  })
})

const TOPIC_NAME = 'soak-share-spec'

/** Every identity of one cycle, as the producer acknowledged it. */
const cycleIdentities = (plan) => {
  const next = [0, 0, 0]
  return plan.records.map((record) => ({
    partition: record.partition,
    identity: shareRecordIdentity({
      topic: TOPIC_NAME,
      partition: record.partition,
      offset: String(plan.first + BigInt(next[record.partition]++)),
      key: Buffer.from(record.key),
      value: Buffer.from(record.value),
    }),
  }))
}

/** What a correct cycle observes, including the held set and replays of a reset cycle. */
const SOAK_UUID = '00000000-0000-4000-8000-000000000001'

/** Primitive original request/response observations for one successful acquisition. */
const exchangeFacts = (id, partition, acquiredRecords) => ({
  id,
  apiKey: 78,
  groupId: 'soak-group',
  memberId: 'member-A',
  endpoint: 'node:1',
  open: 1,
  pool: 1,
  generation: 1,
  responseIdentity: id,
  tickets: [],
  request: {},
  frames: [
    {
      id,
      socket: 1,
      correlationId: id,
      apiKey: 78,
      apiVersion: 2,
      endpoint: 'node:1',
      bytes: 'request',
      responseBytes: 'response',
      request: {
        groupId: 'soak-group',
        memberId: 'member-A',
        topics: [
          {
            topicId: SOAK_UUID,
            partitions: [{ partitionIndex: partition, acknowledgementBatches: [] }],
          },
        ],
      },
    },
  ],
  response: {
    errorCode: 0,
    responses: [
      {
        topicId: SOAK_UUID,
        partitions: [
          { partitionIndex: partition, errorCode: 0, acknowledgeErrorCode: 0, acquiredRecords },
        ],
      },
    ],
  },
})

/** Encodes independent primitive request/answer bytes after the scripted exchange is assembled. */
const sealExchange = (exchange) => {
  const frame = exchange.frames[0]
  const request = frame.request
  const body = new Encoder()
    .writeUVarIntString(request.groupId)
    .writeUVarIntString(request.memberId)
    .writeInt32(0)
    .writeInt32(250)
    .writeInt32(1)
    .writeInt32(1048576)
    .writeInt32(100)
    .writeInt32(0)
    .writeInt8(1)
    .writeBoolean(false)
    .writeUVarIntArray(
      request.topics.map((topic) =>
        new Encoder()
          .writeUuid(topic.topicId)
          .writeUVarIntArray(
            topic.partitions.map((partition) =>
              new Encoder()
                .writeInt32(partition.partitionIndex)
                .writeUVarIntArray(
                  partition.acknowledgementBatches.map((batch) =>
                    new Encoder()
                      .writeInt64(batch.firstOffset)
                      .writeInt64(batch.lastOffset)
                      .writeUVarIntArray(
                        batch.acknowledgeTypes.map((type) => new Encoder().writeInt8(type))
                      )
                      .writeTaggedFields([])
                  )
                )
                .writeTaggedFields([])
            )
          )
          .writeTaggedFields([])
      )
    )
    .writeUVarIntArray([])
    .writeTaggedFields([])
  const header = new Encoder()
    .writeInt16(78)
    .writeInt16(2)
    .writeInt32(frame.correlationId)
    .writeString('soak')
    .writeTaggedFields([])
  const bytes = Buffer.concat([header.buffer, body.buffer])
  frame.bytes = new Encoder().writeInt32(bytes.length).writeBuffer(bytes).buffer.toString('base64')
  frame.request = require('../../../testHelpers/wire/shareFetchV2').readRequest(body.buffer)
  const response = require('../../../testHelpers/wire/shareFetchV2').buildResponse({
    errorCode: 0,
    responses: exchange.response.responses.map((topic) => ({
      ...topic,
      partitions: topic.partitions.map((partition) => ({
        ...partition,
        errorMessage: null,
        acknowledgeErrorMessage: null,
        currentLeader: { leaderId: -1, leaderEpoch: -1 },
        records: '',
      })),
    })),
    nodeEndpoints: [],
  })
  const inner = new Encoder()
    .writeInt32(frame.correlationId)
    .writeTaggedFields([])
    .writeBuffer(response)
  frame.responseBytes = new Encoder()
    .writeInt32(inner.buffer.length)
    .writeBuffer(inner.buffer)
    .buffer.toString('base64')
  return exchange
}

/** Both actual listing forms and independent ends, kept separate from normalized rows. */
const positionFacts = (plan) => {
  const partitions = [0, 1, 2].map((partition) => ({ partition, floor: '0', end: plan.end }))
  const listing = [
    {
      topic: TOPIC_NAME,
      topicId: SOAK_UUID,
      partitions: partitions.map(({ partition }) => ({
        partition,
        startOffset: '0',
        lag: '0',
        errorCode: 0,
        leaderEpoch: 1,
      })),
    },
  ]
  return {
    target: { groupId: 'soak-group', topic: TOPIC_NAME, partitions },
    named: JSON.parse(JSON.stringify(listing)),
    all: JSON.parse(JSON.stringify(listing)),
    topicId: SOAK_UUID,
    ends: partitions.map(({ partition, end }) => ({ partition, high: end })),
  }
}

/** Measured assigned interval, with an actual in-window ShareFetch and joined leave. */
const verifierFacts = (cycle = 0, base = cycle * 16000 + 1000, kind = 'cycle') => {
  const memberId = `verifier-${kind}-${cycle}`
  const exchanges = [0, 1, 2].map((partition) => {
    const exchange = exchangeFacts(9000 + partition, partition, [])
    exchange.memberId = memberId
    Object.assign(exchange.frames[0], { at: base + 100, answeredAt: base + 200 })
    exchange.frames[0].request.memberId = memberId
    return sealExchange(exchange)
  })
  return {
    active: true,
    cycle,
    kind,
    groupId: 'soak-group',
    topic: TOPIC_NAME,
    topicId: SOAK_UUID,
    memberId,
    windowMs: 5000,
    openedAt: base - 100,
    closedAt: base + 5100,
    delivered: [],
    provenance: { exchanges },
    events: [
      { kind: 'GROUP_JOIN', at: base - 50, payload: { memberId, groupId: 'soak-group' } },
      {
        kind: 'window-start',
        at: base,
        wallAt: base + 10000,
        payload: [{ topic: TOPIC_NAME, partitions: [0, 1, 2] }],
      },
      ...exchanges.map(({ frames: [frame] }) => ({
        kind: 'REQUEST',
        at: base + 250,
        payload: {
          apiName: 'ShareFetch',
          sentAt: base + 10100,
          correlationId: frame.correlationId,
          broker: frame.endpoint,
        },
      })),
      {
        kind: 'window-end',
        at: base + 5000,
        wallAt: base + 15000,
        payload: [{ topic: TOPIC_NAME, partitions: [0, 1, 2] }],
      },
      { kind: 'DISCONNECT', at: base + 5100, payload: {} },
    ],
  }
}

const cycleFacts = (
  cycle,
  { startedAt = cycle * 16000, due = startedAt + 16000, kind = 'cycle' } = {}
) => {
  const plan = ShareCycleOracle.plan(cycle)
  const produced = cycleIdentities(plan)
  const identities = produced.map(({ identity }) => identity)
  const last = plan.first + BigInt(plan.perPartition) - 1n
  const held = plan.reset
    ? produced.filter(({ partition }) => partition === 0).map(({ identity }) => identity)
    : []
  const facts = {
    produced,
    log: [...identities],
    logs: Object.fromEntries(
      [0, 1, 2].map((partition) => [partition, { end: plan.end, count: Number(plan.end) }])
    ),
    handler: [
      ...identities.map((identity) => ({ identity, deliveryCount: 1 })),
      ...held.map((identity) => ({ identity, deliveryCount: 2 })),
    ],
    held,
    accepted: [0, 1, 2].map((partition) => ({
      topic: TOPIC_NAME,
      partition,
      firstOffset: String(plan.first),
      lastOffset: String(last),
    })),
    broker: [0, 1, 2].map((partition) => ({
      partition,
      startOffset: '0',
      logEnd: plan.end,
      lag: '0',
      errorCode: 0,
    })),
    members: {
      aJoined: true,
      aLeft: true,
      bAdded: true,
      bRemoved: true,
      bRemovedAt: startedAt + (kind === 'final' ? -200 : 100),
      aLeftAt: startedAt + (kind === 'final' ? -100 : 200),
      emptyAt: startedAt + 500,
    },
    timing: { cycle, kind, startedAt, completedAt: startedAt + 6300, due },
    reset: plan.reset
      ? {
          apiKey: 78,
          nodeId: 1,
          reason: 'connection',
          oldSocket: 3,
          oldEpoch: 4,
          nextSocket: 7,
          nextEpoch: 0,
        }
      : null,
    verifier: verifierFacts(cycle, startedAt + 1000, kind),
    clientsDisconnected: true,
    positions: [
      {
        ...positionFacts(plan),
        cycle,
        kind,
        startedAt: startedAt + 300,
        completedAt: startedAt + 400,
      },
      {
        ...positionFacts(plan),
        cycle,
        kind,
        startedAt: startedAt + 6200,
        completedAt: startedAt + 6300,
      },
    ],
  }
  facts.acknowledgedPrefix = Array.from({ length: cycle + 1 }, (_, i) =>
    cycleIdentities(ShareCycleOracle.plan(i))
  )
    .flat()
    .map(({ identity }) => identity)
  facts.prefix = [...facts.acknowledgedPrefix]
  const provenance = { exchanges: [], acquisitions: [], outcomes: [], retirements: [] }
  for (const [index, entry] of facts.handler.entries()) {
    const [, partition, offset] = JSON.parse(entry.identity)
    const acquisition = index + 1
    entry.acquisition = acquisition
    entry.dispatch = acquisition
    const range = { firstOffset: offset, lastOffset: offset, deliveryCount: entry.deliveryCount }
    const row = {
      id: acquisition,
      exchange: acquisition,
      topic: TOPIC_NAME,
      topicId: SOAK_UUID,
      partition,
      acquiredRecords: [range],
    }
    const exchange = exchangeFacts(acquisition, partition, [range])
    provenance.acquisitions.push(row)
    provenance.exchanges.push(exchange)
  }
  facts.accepted = []
  for (const [index, entry] of facts.handler.entries()) {
    if (facts.handler.slice(index + 1).some((other) => other.identity === entry.identity)) continue
    const [, partition, offset] = JSON.parse(entry.identity)
    const acquisition = entry.acquisition
    const source = provenance.exchanges.find(({ id }) => id === acquisition)
    const batch = { firstOffset: offset, lastOffset: offset, acknowledgeTypes: [1] }
    const exchange = exchangeFacts(1000 + index, partition, [])
    exchange.open = source.open
    exchange.tickets = [{ acquisition, ticket: index, isRenewAck: false, batches: [batch] }]
    exchange.frames[0].request.topics[0].partitions[0].acknowledgementBatches = [batch]
    provenance.exchanges.push(exchange)
    const result = {
      acquisition,
      topic: TOPIC_NAME,
      partition,
      firstOffset: offset,
      lastOffset: offset,
      type: 'accept',
      error: null,
    }
    provenance.outcomes.push(result)
    facts.accepted.push({ topic: TOPIC_NAME, partition, firstOffset: offset, lastOffset: offset })
  }
  provenance.exchanges.forEach(sealExchange)
  facts.provenance = provenance
  return facts
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
        now += ms
      },
      clock: timer,
      slotMs,
      soak: { sampleMs: 1000, warmupMs: 0, settleMs: 1, judge: () => settled, ...soak },
    })
    return { owner, driver, rows, shares, gate }
  }

  afterEach(() => liveRun.reset())

  test.each([true, false])(
    'runs all three phases and separate verifier windows, short=%s',
    async (short) => {
      const { owner, rows } = build({ short })
      await owner.run()
      await owner.close()
      expect(owner.elapsedProfileMs).toBe(short ? 480000 : 960000)
      expect(
        rows.filter(({ event }) => event === 'soak-cycle').map(({ verifier }) => verifier.windowMs)
      ).toEqual(Array(30).fill(5000))
      expect(owner.final.verifier.windowMs).toBe(5000)
      expect(owner.timing.slots).toHaveLength(30)
    }
  )

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
      profileMs: 480000,
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
    await expect(owner.runPhase(160001)).rejects.toThrow('is not the declared 160000 ms')
    expect(driver.openPhase).not.toHaveBeenCalled()
    for (let i = 0; i < 3; i++) await owner.runPhase(160000)
    await expect(owner.runPhase(160000)).rejects.toThrow('Every share phase already ran')
  })

  test('disconnects the phase clients when a cycle fails', async () => {
    const { owner, driver } = build({
      driver: scripted({ cycle: jest.fn(async () => Promise.reject(new Error('cycle failed'))) }),
    })
    await expect(owner.runPhase(160000)).rejects.toThrow('cycle failed')
    expect(driver.closePhase).toHaveBeenCalledTimes(1)
  })

  test('rejects actual slot overrun before the abort timer fires and never admits the next cycle', async () => {
    let now = 0
    const driver = scripted({
      cycle: jest.fn(async (plan) => {
        now += 16001
        return cycleFacts(plan.cycle)
      }),
    })
    const { owner } = build({ driver, clock: { now: () => now } })
    await expect(owner.run()).rejects.toThrow('absolute cutoff')
    expect(driver.cycle).toHaveBeenCalledTimes(1)
    expect(owner.timing.slots[0]).toMatchObject({ due: 16000, completedAt: 16001 })
    await owner.close()
  })

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
    owner.phaseDue = 160000
    now = 16001
    await expect(owner.runCycle(0)).rejects.toThrow('deadline exhausted')
    expect(owner.driver.cycle).not.toHaveBeenCalled()
  })

  describe('the combined verdict', () => {
    const complete = (mutate) => {
      const { owner, rows } = build({ clock: { now: () => 487000 } })
      for (let cycle = 0; cycle < 30; cycle++) {
        const facts = cycleFacts(cycle)
        owner.recordCycle(ShareCycleOracle.plan(cycle), facts, [])
      }
      owner.counts = { cycles: 30, resets: 10, memberAdds: 30, memberRemoves: 30 }
      owner.cleanupDue = 500000
      owner.final = cycleFacts(29, { startedAt: 480000, due: 500000, kind: 'final' })
      owner.elapsedProfileMs = SHARE_SOAK.shortMs
      owner.timing.phases = [0, 1, 2].map((phase) => ({
        phase,
        startedAt: phase * 160000,
        due: (phase + 1) * 160000,
        completedAt: (phase + 1) * 160000,
      }))
      owner.timing.slots = Array.from({ length: 30 }, (_, cycle) => ({
        cycle,
        startedAt: cycle * 16000,
        due: (cycle + 1) * 16000,
        completedAt: (cycle + 1) * 16000,
      }))
      owner.timing.stages = owner.timing.phases.map((phase) => ({ ...phase, name: 'profile' }))
      owner.baseline = quietShare()
      owner.teardown = quietShare()
      mutate?.(owner, rows)
      return { verdict: owner.complete(settled), rows }
    }

    test('passes only when every count, identity and final proof holds', () => {
      const { verdict, rows } = complete()
      expect(verdict.ok).toBe(true)
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
        'an actual slot overrun hidden by a nominal profile total',
        (o) => {
          o.timing.slots[5].completedAt++
        },
        'original slot, phase',
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
    [
      'post-cleanup +10001',
      (o) => {
        o.exitedAt++
      },
    ],
    [
      'inclusive total +1',
      (o) => {
        o.completedAt = 539999
        o.exitedAt = 540001
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

    const verify = ({ replayAt, duringDrain = false, assignmentAt = 0, due = 16000 } = {}) => {
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
    remaining = 16000,
    holdPost = false,
  } = {}) => {
    const plan = ShareCycleOracle.plan(reset ? 10 : 0)
    const clients = []
    const facts = cycleFacts(plan.cycle)
    const original = { sequence: 1, epoch: 4, socket: 1, endpoint: 'node:1' }
    let broken = false
    let redeliver
    const tap = new ShareFetchTap()
    tap.mark = () => 0
    tap.since = (mark) =>
      mark === 0 ? [original] : broken ? [{ sequence: 2, epoch: 0, socket: 2 }] : []
    tap.destroy = () => {
      broken = true
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
            client.emit('GROUP_JOIN', { memberId: `member-${clients.length}` })
          },
          disconnect: async () => {
            await client.pending
            if (lateDuringClose && client === clients[0])
              await client.handlers.eachBatch(client.batch)
            if (client === clients[0])
              for (const range of facts.accepted)
                client.emit('ACKNOWLEDGE', { ...range, type: 'accept' })
            client.emit('DISCONNECT', {})
          },
        }
        clients.push(client)
        return client
      },
    }
    const ports = {
      fixtures: { shareRecordIdentity, waitForShareGroupsEmpty: async () => {} },
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
            }
            await Promise.resolve()
          }
          throw new Error('Scripted barrier never converged')
        },
      },
    }
    const driver = new ShareSoakDriver({ kafka, shares: { track: (client) => client }, tap, ports })
    Object.assign(driver, {
      topic: TOPIC_NAME,
      topicId: SOAK_UUID,
      group: 'soak-group',
      admin: {},
      leaders: new Map([[0, 'node:1']]),
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
        }
      return facts.produced
    }
    driver.rawLog = async () =>
      new Map(
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
    const work = driver.cycle(plan, NEVER, { due: remaining, clock: { now: () => 0 } })
    work.catch(() => {})
    return { driver, work, clients, post, release: () => release?.() }
  }

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

  test('waits for held identities to be redelivered before closing the workload member', async () => {
    const { work } = await cycle({ reset: true })
    const observed = await work
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
  const {
    ShareClock,
    shareClient,
    fetchAnswer,
    ackAnswer,
    part,
    acquired,
    flush,
  } = require('../../../testHelpers/shareHarness')
  const {
    batch,
    record,
  } = require('../../../src/protocol/requests/fetch/__tests__/fixtures/recordBatchBytes')
  const fixtures = require('../../../testHelpers/shareFixtures')
  const diagnostics = require('../../../testHelpers/shareLiveDiagnostics')

  /**
   * Script only external I/O. Membership, session loss/reopen, acquisition, dispatch, ledger
   * decisions, acknowledgements, window observation, resource judging and reconstruction call
   * through their original implementations. The raw corpus is independent of handler evidence.
   */
  const schedule = async ({
    short = true,
    damage = () => {},
    wire = () => {},
    finalDamage = () => {},
    io = async () => {},
    assignmentDelay = 0,
    replayAt,
    drainReplay = false,
  } = {}) => {
    const clock = new ShareClock().install()
    const exposed = globalThis.gc
    globalThis.gc = undefined
    const transport = shareClient({ nodes: 3, topics: {} })
    const { cluster, table } = transport
    const tap = new ShareFetchTap(64, { now: () => performance.now() })
    /**
     * Rejects destroyed-stream writes before they can apply acknowledgements at the broker.
     * @param {object} options Socket endpoint and connection callback.
     * @returns {object} The scripted socket with asynchronous stream-write failures.
     */
    const socketFactory = (options) => {
      const socket = cluster.socketFactory(options)
      const write = socket.write.bind(socket)
      socket.write = (...args) => {
        if (!socket.destroyed) return write(...args)
        const error = Object.assign(new Error('Cannot call write after a stream was destroyed'), {
          code: 'ERR_STREAM_DESTROYED',
        })
        queueMicrotask(() => socket.emit('error', error))
        return false
      }
      return socket
    }
    const instrumented = instrumentShare(
      cluster.kafkaOptions({
        socketFactory: tap.factory(socketFactory),
        retry: { initialRetryTime: 50, maxRetryTime: 100, factor: 0, retries: 3 },
      }),
      { activeResources: false }
    )
    const resources = instrumented.shareResources
    const originalSnapshot = resources.snapshot
    // Heap and process resources are scripted inputs; original client state is always measured.
    resources.snapshot = async () => ({
      ...(await originalSnapshot()),
      heapUsed: 1024,
      activeResources: {},
    })
    const records = [[], [], []]
    const members = new Map()
    const configs = new Map()
    const sessions = new Map()
    const rows = []
    let groupId
    let topic
    let groupPresent = false
    let listingNode = 0
    let firstWindow
    const verifierStarts = []
    const seenMembers = new Map()
    const assignment = (id) => {
      const ids = [...members.keys()]
      return [{ topic, partitions: ids.length < 2 ? [0, 1, 2] : id === ids[0] ? [0, 1] : [2] }]
    }
    const wireAssignment = (id) => ({
      topicPartitions: assignment(id).map(({ partitions }) => ({ topicId: SOAK_UUID, partitions })),
    })
    cluster.answer('ShareGroupHeartbeat', (fields) => {
      if (fields.memberEpoch === -1) {
        members.delete(fields.memberId)
        return {
          errorCode: 0,
          memberId: fields.memberId,
          memberEpoch: -1,
          heartbeatIntervalMs: 1000,
          assignment: null,
        }
      }
      members.set(fields.memberId, true)
      if (!seenMembers.has(fields.memberId)) seenMembers.set(fields.memberId, performance.now())
      const waiting =
        driver.lastCycle?.members.aLeft &&
        performance.now() - seenMembers.get(fields.memberId) < assignmentDelay
      return {
        errorCode: 0,
        memberId: fields.memberId,
        memberEpoch: 1,
        heartbeatIntervalMs: 1000,
        assignment: waiting ? null : wireAssignment(fields.memberId),
      }
    })
    const answer = (fields, call) => {
      const key = `${fields.memberId}:${call.node}:${call.generation}`
      if (fields.shareSessionEpoch === 0) sessions.set(key, new Set())
      const wanted = sessions.get(key) ?? new Set()
      for (const { partitions } of fields.topics)
        for (const { partitionIndex, acknowledgementBatches } of partitions) {
          wanted.add(partitionIndex)
          for (const ack of acknowledgementBatches)
            for (const row of records[partitionIndex])
              if (
                BigInt(row.offset) >= BigInt(ack.firstOffset) &&
                BigInt(row.offset) <= BigInt(ack.lastOffset) &&
                row.owner === key &&
                ack.acknowledgeTypes.every((type) => type === 1)
              )
                row.settled = true
        }
      sessions.set(key, wanted)
      for (const { partitions } of fields.forgottenTopicsData ?? [])
        for (const partition of partitions) wanted.delete(partition)
      if (call.apiKey === 79)
        return ackAnswer([...wanted].map((partition) => ({ topicId: SOAK_UUID, partition })))
      const parts = [...wanted].map((partition) => {
        const available = records[partition].filter((row) => !row.settled && row.owner !== key)
        const acquire = []
        let bytes = ''
        if (available.length) {
          for (const row of available) {
            row.owner = key
            row.count++
          }
          acquire.push(acquired(available[0].offset, available.at(-1).offset, available[0].count))
          bytes = batch({
            baseOffset: BigInt(available[0].offset),
            records: available.map((row, index) =>
              record({ offsetDelta: index, key: row.key, value: row.value })
            ),
          }).toString('hex')
        }
        return part({ topicId: SOAK_UUID, partition, acquire, records: bytes })
      })
      const result = fetchAnswer(parts, { acquisitionLockTimeoutMs: 30000 })
      wire({ fields, call, result, owner, records, members })
      return result
    }
    cluster.answer('ShareFetch', answer)
    cluster.answer('ShareAcknowledge', answer)
    const admin = () => {
      const events = new EventEmitter()
      return {
        events: { REQUEST: 'request' },
        on: (event, listener) => {
          events.on(event, listener)
          return () => events.off(event, listener)
        },
        connect: async () => {},
        disconnect: async () => {},
        createTopics: async ({ topics: [created] }) => {
          topic = created.topic
          table.set(topic, {
            topicId: SOAK_UUID,
            partitions: [0, 1, 2].map((leader) => ({ leader, epoch: 1 })),
          })
        },
        deleteTopics: async () => {
          await io('delete-topic', { owner, driver, clock })
          return table.clear()
        },
        listTopics: async () => {
          events.emit('request', {
            payload: { apiName: 'Metadata', broker: `node${listingNode++ % 3}:9092` },
          })
          return [...table.keys()]
        },
        fetchTopicMetadata: async () => ({
          topics: [
            {
              name: topic,
              topicId: SOAK_UUID,
              partitions: [0, 1, 2].map((partitionId) => ({ partitionId, leader: partitionId })),
            },
          ],
        }),
        describeCluster: async () => ({
          brokers: [0, 1, 2].map((nodeId) => ({ nodeId, host: `node${nodeId}`, port: 9092 })),
        }),
        listGroups: async () => ({ groups: groupPresent ? [{ groupId }] : [] }),
        deleteGroups: async () => {
          groupPresent = false
        },
        describeShareGroups: async ([id]) => ({
          groups: [
            {
              groupId: id,
              errorCode: groupPresent ? 0 : 69,
              state: members.size ? 'Stable' : 'Empty',
              members: [...members.keys()].map((memberId) => ({
                memberId,
                assignment: assignment(memberId),
              })),
            },
          ],
        }),
        listShareGroupOffsets: async () => [
          {
            topic,
            topicId: SOAK_UUID,
            partitions: records.map((rows, partition) => ({
              partition,
              startOffset: '0',
              lag: String(rows.filter(({ settled }) => !settled).length),
              errorCode: 0,
              leaderEpoch: 1,
            })),
          },
        ],
        fetchTopicOffsets: async () => {
          await io('terminal-ends', { owner, driver, clock })
          return records.map((rows, partition) => ({ partition, high: String(rows.length) }))
        },
        incrementalAlterConfigs: async ({ resources: [resource] }) => {
          groupId = resource.name
          groupPresent = true
          for (const { name, value, operation } of resource.configEntries)
            if (operation === 1) configs.delete(name)
            else configs.set(name, value)
          return { resources: [{ errorCode: 0 }] }
        },
        describeConfigs: async ({ resources: [resource] }) => ({
          resources: [
            {
              resourceName: resource.name,
              resourceType: resource.type,
              errorCode: 0,
              configEntries: [...configs].map(([configName, configValue]) => ({
                configName,
                configValue,
                configSource: 8,
              })),
            },
          ],
        }),
      }
    }
    const kafka = {
      shareConsumer: (options) => {
        const client = instrumented.kafka.shareConsumer(options)
        if (!driver.lastCycle?.members.aLeft && owner.counts.cycles !== 30) return client
        const assignment = client.assignment
        const disconnect = client.disconnect
        // The public consumer is frozen: wrap a copy instead of mutating it.
        const observed = { ...client }
        let fetching = false
        let started = false
        let gate
        const remove = client.on(client.events.REQUEST, ({ payload }) => {
          if (payload.apiName === 'ShareFetch') fetching = true
        })
        const replay = () =>
          fetchAnswer(
            [
              part({
                topicId: SOAK_UUID,
                partition: 0,
                acquire: [acquired(0, 0, 2)],
                records: batch({
                  baseOffset: 0n,
                  records: [record({ key: records[0][0].key, value: records[0][0].value })],
                }).toString('hex'),
              }),
            ],
            { acquisitionLockTimeoutMs: 30000 }
          )
        observed.assignment = () => {
          const current = assignment()
          if (!started && fetching && current[0]?.partitions.length === 3) {
            started = true
            verifierStarts.push(performance.now())
            if (replayAt !== undefined || drainReplay) {
              gate = transport.hold({ api: 'ShareFetch', node: 0 })
              if (!drainReplay) setTimeout(() => gate.release(replay()), replayAt)
            }
          }
          return current
        }
        observed.disconnect = async (options) => {
          await io('verifier-disconnect', { owner, driver, clock })
          if (drainReplay && gate?.settle) {
            gate.release(replay())
            await flush()
          }
          try {
            return await disconnect(options)
          } finally {
            remove()
          }
        }
        return observed
      },
      admin,
      producer: () => ({
        connect: async () => io('phase-open', { owner, driver, clock }),
        disconnect: async () => {},
        send: async ({ messages }) => {
          const partition = messages[0].partition
          const baseOffset = String(records[partition].length)
          for (const message of messages)
            records[partition].push({
              ...message,
              offset: String(records[partition].length),
              settled: false,
              owner: null,
              count: 0,
            })
          return [{ partition, baseOffset, errorCode: 0 }]
        },
      }),
    }
    const driver = new ShareSoakDriver({
      kafka,
      shares: resources,
      tap,
      brokers: ['node0:9092'],
      socketFactory: instrumented.socketFactory,
      ports: {
        fixtures,
        diagnostics,
        bounded: require('../../../src/producer/__tests__/fixtures/bounded'),
        probe: require('../../../src/producer/__tests__/fixtures/liveProbe'),
        readLog: async () =>
          new Map(
            records.map((rows, partition) => [
              partition,
              {
                endOffset: String(rows.length),
                records: rows.map(({ offset, key, value }) => ({ offset, key, value })),
              },
            ])
          ),
      },
    })
    const owner = new ShareSoak({
      driver,
      kafka,
      shares: resources,
      short,
      stack: new CleanupStack(),
      clock: { now: () => performance.now() },
      gate: new LiveRun(),
      evidence: {
        read: () => rows,
        write: (row) => {
          if (row.event === 'soak-cycle') {
            if (row.verifier) firstWindow ??= JSON.parse(JSON.stringify(row.verifier))
            if (row.status === 'passed') damage(row, { owner, firstWindow, rows, clock })
          }
          if (row.event === 'soak-resources' && row.kind === 'teardown')
            finalDamage(owner.final, { owner, rows, clock })
          rows.push(row)
        },
      },
    })
    let result
    let error
    let done = false
    const work = owner
      .run()
      .then(
        (value) => {
          result = value
        },
        (failure) => {
          error = failure
        }
      )
      .finally(() => {
        done = true
      })
    const cleanupErrors = []
    try {
      for (let tick = 0; tick < 5000; tick++) {
        if (done) break
        await clock.advance(250)
      }
      if (!done) throw new Error('The scripted schedule did not finish within its frozen envelope')
      await work
      cluster.assertClean()
    } finally {
      let closed = false
      const closing = owner
        .close()
        .catch((failure) => cleanupErrors.push(failure))
        .finally(() => {
          closed = true
        })
      for (let tick = 0; tick < 100; tick++) {
        if (closed) break
        await clock.advance(250)
      }
      try {
        await closing
        await transport.close().catch((failure) => cleanupErrors.push(failure))
        await flush()
      } finally {
        resources.restore()
        globalThis.gc = exposed
        clock.restore()
      }
    }
    if (cleanupErrors.length) {
      if (error) error.cleanupErrors = cleanupErrors
      else if (result?.ok === false) result.cleanupErrors = cleanupErrors
      else throw cleanupErrors[0]
    }
    return {
      owner,
      rows,
      result,
      error,
      records,
      verifierStarts,
      reason: error?.message ?? result?.violations.join('; '),
    }
  }

  test.each([true, false])(
    'reconstructs thirty real adapter cycles and ten actual socket resets, short=%s',
    async (short) => {
      const { owner, rows, result, error, records } = await schedule({ short })
      expect(error).toBeUndefined()
      expect(result).toMatchObject({
        ok: true,
        violations: [],
        counts: { cycles: 30, resets: 10, memberAdds: 30, memberRemoves: 30 },
      })
      expect(records.map((rows) => rows.length)).toEqual([600, 600, 600])
      const cycles = rows.filter(({ event }) => event === 'soak-cycle')
      expect(cycles).toHaveLength(30)
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
      expect(owner.elapsedProfileMs).toBe(short ? 480000 : 960000)
      expect(owner.budget).toMatchObject({
        watchdogMs: short ? 540000 : 1020000,
        lifecycleMs: short ? 600000 : 1080000,
      })
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
        clock.skew(16001)
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
      expect(reason).toContain('verifier')
      const cycles = rows.filter(({ event }) => event === 'soak-cycle')
      expect(cycles.map(({ cycle }) => cycle)).toEqual([0])
      expect(cycles[0].verifier.delivered).toHaveLength(1)
    },
    120000
  )

  test('keeps real verifier observations through its disconnect callback and join', async () => {
    const { reason, rows } = await schedule({ drainReplay: true })
    expect(reason).toContain('verifier')
    expect(rows.find(({ event }) => event === 'soak-cycle').verifier.delivered).toHaveLength(1)
  }, 120000)

  test('starts each complete window only after delayed original assignment and fetching', async () => {
    const { result, error, rows } = await schedule({ assignmentDelay: 2000 })
    expect(error).toBeUndefined()
    expect(result.ok).toBe(true)
    for (const { verifier } of rows.filter(({ event }) => event === 'soak-cycle')) {
      const start = verifier.events.find(({ kind }) => kind === 'window-start')
      const end = verifier.events.find(({ kind }) => kind === 'window-end')
      expect(start.at - verifier.openedAt).toBeGreaterThanOrEqual(2000)
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
      12000,
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
      'shared transition deadline exceeded',
    ],
    [
      'combined final proof and cleanup',
      'delete-topic',
      () => true,
      16000,
      'shared cleanup deadline exceeded',
    ],
  ])(
    'rejects %s without renewing its original allocation',
    async (_name, selected, eligible, extra, message) => {
      const { reason, rows } = await schedule({
        io: async (event, context) => {
          if (event !== selected || !eligible(context)) return
          const { driver, clock } = context
          clock.skew(extra ?? driver.lastCycle.timing.due - performance.now() + 1)
        },
      })
      expect(reason).toContain(message)
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
