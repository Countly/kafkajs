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
const { LiveRun, liveRun } = require('../../../src/producer/__tests__/fixtures/bounded')
const {
  SHARE_SOAK,
  SHARE_SOAK_TITLE,
  ShareCycleOracle,
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
      shortMs: 120000,
      fullMs: 480000,
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
    ['share-consumer-short', 40000, 120000],
    ['share-consumer', 160000, 480000],
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
    expect(totalMs).toBeLessThan(600000)
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
    return { observer, ...fakes, Ledger: fakes.ShareAckLedger, Run: fakes.ShareRun }
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
const cycleFacts = (cycle) => {
  const plan = ShareCycleOracle.plan(cycle)
  const produced = cycleIdentities(plan)
  const identities = produced.map(({ identity }) => identity)
  const last = plan.first + BigInt(plan.perPartition) - 1n
  const held = plan.reset
    ? produced.filter(({ partition }) => partition === 0).map(({ identity }) => identity)
    : []
  return {
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
    members: { aJoined: true, aLeft: true, bAdded: true, bRemoved: true },
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
    verifier: { active: true, delivered: [] },
    clientsDisconnected: true,
    positions: [],
  }
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
      jest.fn(async (plan) => {
        const facts = cycleFacts(plan.cycle)
        alter(facts, plan)
        return facts
      }),
    final: jest.fn(async () => ({
      verifier: { active: true, delivered: [] },
      broker: cycleFacts(29).broker,
    })),
  })

  const build = ({ driver = scripted(), gate = new LiveRun(), slotMs = 200, soak = {} } = {}) => {
    const rows = []
    const shares = { snapshot: jest.fn(async () => quietShare()), restore: jest.fn() }
    const owner = new ShareSoak({
      driver,
      kafka: {},
      shares,
      stack: new CleanupStack(),
      short: true,
      evidence: { write: (row) => rows.push(row), read: () => rows },
      gate,
      pause: async () => {},
      slotMs,
      soak: { sampleMs: 1000, warmupMs: 0, settleMs: 1, judge: () => settled, ...soak },
    })
    return { owner, driver, rows, shares, gate }
  }

  afterEach(() => liveRun.reset())

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
      profileMs: 120000,
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
      cycle: jest.fn(async (plan) => {
        await new Promise((resolve) => setTimeout(resolve, 8))
        return cycleFacts(plan.cycle)
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

    await expect(first.owner.run()).rejects.toThrow('Cycle 0 did not settle within 30 ms')
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
    await expect(owner.runPhase(40001)).rejects.toThrow('is not the declared 40000 ms')
    expect(driver.openPhase).not.toHaveBeenCalled()
    for (let i = 0; i < 3; i++) await owner.runPhase(40000)
    await expect(owner.runPhase(40000)).rejects.toThrow('Every share phase already ran')
  })

  test('disconnects the phase clients when a cycle fails', async () => {
    const { owner, driver } = build({
      driver: scripted({ cycle: jest.fn(async () => Promise.reject(new Error('cycle failed'))) }),
    })
    await expect(owner.runPhase(40000)).rejects.toThrow('cycle failed')
    expect(driver.closePhase).toHaveBeenCalledTimes(1)
  })

  describe('the combined verdict', () => {
    const complete = (mutate) => {
      const { owner, rows } = build()
      for (let cycle = 0; cycle < 30; cycle++) {
        const facts = cycleFacts(cycle)
        owner.recordCycle(ShareCycleOracle.plan(cycle), facts, [])
      }
      owner.counts = { cycles: 30, resets: 10, memberAdds: 30, memberRemoves: 30 }
      owner.final = { verifier: { active: true, delivered: [] }, broker: cycleFacts(29).broker }
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
