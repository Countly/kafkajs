const {
  ENDPOINT_BYTES,
  SAMPLE_MS,
  SLOPE_BYTES_PER_MINUTE,
  WARMUP_MS,
  evaluate,
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

  const smoke = async () => {
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

  test('reads a client that holds on to a socket as a violation', async () => {
    const cluster = groupScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }] }),
      {
        batches: 0,
      }
    )
    const { kafka, probe } = instrument(cluster.kafkaOptions(), { activeResources: false })
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

  test('reads the private members of a client by name, and says when one is gone', () => {
    const stand = { [Symbol('private:Kafka:operations')]: new Map() }

    expect(privateOf(stand, 'operations').value).toBeInstanceOf(Map)
    expect(() => privateOf(stand, 'createCluster')).toThrow(
      "The soak driver reads Kafka's private createCluster, which this client does not have"
    )
  })
})
