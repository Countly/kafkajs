const { webcrypto } = require('crypto')
const { Kafka, Partitioners, logLevel } = require('../../../index')
const createProducer = require('../index')
const { attach } = require('../partitioners/builtIn')
const { KafkaJSInvalidConfigurationError } = require('../../errors')
const { createLogger, LEVELS } = require('../../loggers')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC, TOPIC_ID } = require('../../consumer/__tests__/fixtures/fetchHarness')

const RECORDS = 6
const NODES = [0, 1].map((id) => ({ id, host: `node${id}`, port: 9092 }))
const RACKS = { 0: 'rack-a', 1: 'rack-b' }
/** The topic's partition ids are sparse: 2, 7 and 11, so an array index is never a partition id. */
const IDS = [2, 7, 11]
/** Node 0 (rack-a) leads partitions 2 and 11; node 1 (rack-b) leads partition 7. */
const LEADERS = [0, 1, 0]
/** What the legacy round robin gives when its counter starts at 2: the ids in turn, from the first. */
const ROUND_ROBIN = [2, 7, 11, 2, 7, 11]
/** What a sticky pick gives when `Math.random()` is 0: the first candidate, kept. */
const STICKY = [2, 2, 2, 2, 2, 2]
/** `toPositive(murmur2(key)) % 3` of the keys below, as the Java client maps them. */
const DEFAULT_KEYED = [1, 2, 1, 0, 2, 0]
/** What each pick gives, from the same start, when partition 2 has no leader: only 7 and 11 are candidates. */
const STICKY_LEADERLESS = [7, 7, 7]
const ROUND_ROBIN_LEADERLESS = [11, 7, 11]
/** The same keys under the hash KafkaJS used before 2.0.0. */
const LEGACY_KEYED = [1, 0, 2, 0, 1, 0]
const KEYS = ['a', 'b', 'c', 'key-1', 'key-2', 'order-17']
const OPTED_IN = { rackId: 'rack-b', rackAware: true }

/**
 * Every distinct built-in form a producer takes as `createPartitioner`, with what it picks where
 * no rack decides (`unfiltered`), where it puts the keys of `KEYS` (`keyed`) and what its first
 * three picks are when partition 2 has no leader and no rack decides (`leaderless`).
 * `JavaCompatiblePartitioner` is the same function as `DefaultPartitioner`, so it has no row.
 * @type {Array<[name: string, createPartitioner: Function|undefined, unfiltered: number[], keyed: number[], leaderless: number[]]>}
 */
const FORMS = [
  ['no createPartitioner', undefined, STICKY, DEFAULT_KEYED, STICKY_LEADERLESS],
  [
    'createPartitioner UniformStickyPartitioner()',
    Partitioners.UniformStickyPartitioner(),
    STICKY,
    DEFAULT_KEYED,
    STICKY_LEADERLESS,
  ],
  [
    'createPartitioner UniformStickyPartitioner uncalled',
    Partitioners.UniformStickyPartitioner,
    STICKY,
    DEFAULT_KEYED,
    STICKY_LEADERLESS,
  ],
  [
    'createPartitioner UniformStickyPartitioner({ adaptive: true })',
    Partitioners.UniformStickyPartitioner({ adaptive: true }),
    STICKY,
    DEFAULT_KEYED,
    STICKY_LEADERLESS,
  ],
  [
    'createPartitioner DefaultPartitioner',
    Partitioners.DefaultPartitioner,
    ROUND_ROBIN,
    DEFAULT_KEYED,
    ROUND_ROBIN_LEADERLESS,
  ],
  [
    'createPartitioner LegacyPartitioner',
    Partitioners.LegacyPartitioner,
    ROUND_ROBIN,
    LEGACY_KEYED,
    ROUND_ROBIN_LEADERLESS,
  ],
]

/** Starts every legacy round robin at counter 2, so its first unfiltered pick is the first id. */
const seedRoundRobin = () => {
  const original = webcrypto.getRandomValues.bind(webcrypto)
  jest.spyOn(webcrypto, 'getRandomValues').mockImplementation((view) => {
    if (!Buffer.isBuffer(view) || view.length !== 32) return original(view)
    view.fill(0)
    view.writeUInt32BE(2, 0)
    return view
  })
}

/** @returns {{ partitionId: number, leader: number }[]} a partition list over `ids` */
const metadataOf = (leaders, ids = IDS) =>
  ids.map((partitionId, i) => ({ partitionId, leader: leaders[i] }))

/**
 * A built-in partitioner attached as the producer attaches it, with every `rackOf` read recorded.
 * @returns {{ partitioner: Function, reads: number[] }}
 */
const attached = (createPartitioner, { racks = RACKS, loadOf = () => 0, ...options } = {}) => {
  const reads = []
  const partitioner = createPartitioner()
  attach(partitioner, {
    loadOf,
    rackOf: (nodeId) => {
      reads.push(nodeId)
      return racks[nodeId]
    },
    rackId: '',
    rackAware: false,
    ...options,
  })
  return { partitioner, reads }
}

const pick = (partitioner, partitionMetadata, message = { value: 'v' }) =>
  partitioner({ topic: TOPIC, partitionMetadata, message })

describe('Producer > rack-aware partitioning', () => {
  let producer, cluster
  /** Every producer and cluster `open` made, so the cleanup closes each of them. */
  const opened = []

  /**
   * A connected producer made by the exported `Kafka` class over a scripted cluster of its own
   * with the sparse topic.
   * @returns {Promise<{ producer: object, cluster: object }>}
   */
  const open = async (options = {}, { racks = RACKS, leaders = LEADERS } = {}) => {
    const scripted = producerScript(createScriptedCluster({ nodes: NODES })).answer(
      'Metadata',
      () => ({
        brokers: NODES.map(({ id, host, port }) => ({ nodeId: id, host, port, rack: racks[id] })),
        controllerId: 0,
        topics: [
          {
            errorCode: 0,
            name: TOPIC,
            topicId: TOPIC_ID,
            isInternal: false,
            topicAuthorizedOperations: -2147483648,
            partitions: IDS.map((partitionIndex, i) => ({
              errorCode: 0,
              partitionIndex,
              leaderId: leaders[i],
              leaderEpoch: 5,
              replicas: [leaders[i]],
              isr: [leaders[i]],
              offlineReplicas: [],
            })),
          },
        ],
      })
    )
    const made = new Kafka(scripted.kafkaOptions({ brokers: ['node0:9092'] })).producer({
      linger: 0,
      ...options,
    })
    opened.push({ producer: made, cluster: scripted })
    await made.connect()
    return { producer: made, cluster: scripted }
  }

  /** `open`, kept as the producer and cluster the test sends through. */
  const start = async (...args) => {
    const started = await open(...args)
    producer = started.producer
    cluster = started.cluster
  }

  const sendKeyless = async (count = RECORDS) => {
    for (let i = 0; i < count; i += 1) {
      await producer.send({ topic: TOPIC, messages: [{ value: `v-${i}` }] })
    }
  }

  /** The topic, partition and record count of every batch the client wrote, in order. */
  const written = (scripted = cluster) =>
    scripted
      .requests()
      .filter(({ api }) => api === 'Produce')
      .flatMap(({ fields }) =>
        fields.topics.flatMap(({ topicId, partitions }) =>
          partitions.map(({ partition, batches }) => ({
            topicId,
            partition,
            records: batches.reduce((sum, { recordCount }) => sum + recordCount, 0),
          }))
        )
      )

  const partitionsWritten = (scripted = cluster) =>
    written(scripted).map(({ partition }) => partition)

  beforeEach(() => {
    jest.spyOn(Math, 'random').mockReturnValue(0)
    seedRoundRobin()
  })

  afterEach(async () => {
    for (const { producer: made } of opened) await made.disconnect()
    for (const { cluster: scripted } of opened) scripted.assertClean()
    opened.length = 0
    producer = cluster = null
    jest.restoreAllMocks()
  })

  describe.each(FORMS)('%s', (_name, createPartitioner, unfiltered) => {
    test('unkeyed built-in sends prefer the matching leader rack', async () => {
      await start({ createPartitioner, ...OPTED_IN })

      await sendKeyless()

      expect(written()).toEqual(
        Array.from({ length: RECORDS }, () => ({ topicId: TOPIC_ID, partition: 7, records: 1 }))
      )
    })

    test('no matching leader falls back to the original candidates', async () => {
      await start({ createPartitioner, rackId: 'rack-z', rackAware: true })

      await sendKeyless()

      expect(partitionsWritten()).toEqual(unfiltered)
    })

    test('unknown racks retain the fallback', async () => {
      await start({ createPartitioner, ...OPTED_IN }, { racks: { 0: null, 1: null } })

      await sendKeyless()

      expect(partitionsWritten()).toEqual(unfiltered)
    })

    test.each([
      ['no rack options', {}],
      ['a rackId alone', { rackId: 'rack-b' }],
      ['rackAware false with a rackId', { rackId: 'rack-b', rackAware: false }],
    ])('choices with %s are those made without them', async (_, options) => {
      await start({ createPartitioner, ...options })

      await sendKeyless()

      expect(partitionsWritten()).toEqual(unfiltered)
    })
  })

  test('two producers with different racks each choose among the leaders of their own rack', async () => {
    const createPartitioner = Partitioners.DefaultPartitioner
    const inRackA = await open({ createPartitioner, rackId: 'rack-a', rackAware: true })
    const inRackB = await open({ createPartitioner, rackId: 'rack-b', rackAware: true })

    for (let i = 0; i < 4; i += 1) {
      await inRackA.producer.send({ topic: TOPIC, messages: [{ value: `a-${i}` }] })
      await inRackB.producer.send({ topic: TOPIC, messages: [{ value: `b-${i}` }] })
    }

    expect(partitionsWritten(inRackA.cluster)).toEqual([11, 2, 11, 2])
    expect(partitionsWritten(inRackB.cluster)).toEqual([7, 7, 7, 7])
  })

  test('the preference survives a disconnect and a connect of the same producer', async () => {
    await start(OPTED_IN)
    await sendKeyless(2)
    await producer.disconnect()
    await producer.connect()

    await sendKeyless(2)

    expect(partitionsWritten()).toEqual([7, 7, 7, 7])
  })

  describe('custom partitioners', () => {
    test.each([false, true])(
      'custom partitioners keep three keys and full selection authority with a listener %s',
      async (listening) => {
        const seen = []
        const partition = (args) => {
          seen.push({
            keys: Object.keys(args).sort(),
            partitions: args.partitionMetadata.map(({ partitionId, leader }) => [
              partitionId,
              leader,
            ]),
          })
          return 11
        }
        Object.defineProperty(partition, 'attach', {
          get() {
            throw new Error('a custom partitioner property was read')
          },
        })
        await start({ createPartitioner: () => partition, ...OPTED_IN })
        if (listening) producer.on(producer.events.REQUEST, () => {})

        await sendKeyless(3)

        expect(seen).toEqual(
          Array.from({ length: 3 }, () => ({
            keys: ['message', 'partitionMetadata', 'topic'],
            partitions: [
              [2, 0],
              [7, 1],
              [11, 0],
            ],
          }))
        )
        expect(partitionsWritten()).toEqual([11, 11, 11])
      }
    )
  })

  describe('what the producer hands a built-in partitioner', () => {
    /** A built-in whose `attach` is observed, with the node ids whose racks it reads recorded. */
    const observed = (createPartitioner) => {
      const seen = { contexts: [], reads: [] }
      return {
        seen,
        createPartitioner: () => {
          const partitioner = createPartitioner()
          const original = partitioner.attach
          partitioner.attach = (context) => {
            seen.contexts.push(context)
            original({
              ...context,
              rackOf: (nodeId) => {
                seen.reads.push(nodeId)
                return context.rackOf(nodeId)
              },
            })
          }
          return partitioner
        },
      }
    }

    test.each([
      ['a rackId alone', { rackId: 'rack-b' }, { rackId: 'rack-b', rackAware: false }],
      [
        'rackAware false with a rackId',
        { rackId: 'rack-b', rackAware: false },
        { rackId: 'rack-b', rackAware: false },
      ],
      ['no options', {}, { rackId: '', rackAware: false }],
    ])(
      '%s: the built-in partitioner is attached with the resulting options and no rack is read',
      async (_, options, expected) => {
        const { seen, createPartitioner } = observed(Partitioners.DefaultPartitioner)
        await start({ createPartitioner, ...options })

        await sendKeyless()

        expect(seen.contexts).toHaveLength(1)
        expect(seen.contexts[0]).toMatchObject(expected)
        expect(seen.reads).toEqual([])
        expect(partitionsWritten()).toEqual(ROUND_ROBIN)
      }
    )

    test('an opted-in producer reads the racks of the leaders of the partitions it chooses among', async () => {
      const { seen, createPartitioner } = observed(Partitioners.DefaultPartitioner)
      await start({ createPartitioner, ...OPTED_IN })

      await sendKeyless(1)

      expect(seen.contexts[0]).toMatchObject(OPTED_IN)
      expect([...new Set(seen.reads)].sort()).toEqual([0, 1])
      expect(partitionsWritten()).toEqual([7])
    })
  })
})

describe('Producer > rack-aware partitioning > the partition functions', () => {
  afterEach(() => jest.restoreAllMocks())

  beforeEach(() => seedRoundRobin())

  test('rack changes invalidate an ineligible sticky partition', () => {
    const racks = { 0: 'rack-a', 1: 'rack-b' }
    const random = jest.spyOn(Math, 'random').mockReturnValue(0)
    const { partitioner } = attached(Partitioners.UniformStickyPartitioner({ stickyBytes: 1e9 }), {
      ...OPTED_IN,
      racks,
    })

    const first = metadataOf([0, 1, 0])
    expect([pick(partitioner, first), pick(partitioner, first)]).toEqual([7, 7])

    // partition 7 keeps a leader but leaves the rack; partition 2 is now the one led in it
    expect(pick(partitioner, metadataOf([1, 0, 0]))).toBe(2)

    // the racks themselves swap: node 0 is rack-b, so partitions 7 and 11 are the matching ones
    racks[0] = 'rack-b'
    racks[1] = 'rack-a'
    expect(pick(partitioner, metadataOf([1, 0, 0]))).toBe(7)

    // a sticky partition that is still eligible is kept when the next metadata arrives
    random.mockReturnValue(0.99)
    expect(pick(partitioner, metadataOf([1, 0, 0]))).toBe(7)
  })

  test('a sticky partition is kept when no partition is led in the rack any more', () => {
    const racks = { 0: 'rack-a', 1: 'rack-b' }
    jest.spyOn(Math, 'random').mockReturnValue(0)
    const { partitioner } = attached(Partitioners.UniformStickyPartitioner({ stickyBytes: 1e9 }), {
      ...OPTED_IN,
      racks,
    })
    expect(pick(partitioner, metadataOf([0, 1, 0]))).toBe(7)

    // nothing is led in rack-b now, so every partition with a leader is a candidate again
    expect(pick(partitioner, metadataOf([0, 0, 0]))).toBe(7)
  })

  test('a leader hint that moves the sticky partition out of the rack, in the same partition list, makes the next record leave it', () => {
    jest.spyOn(Math, 'random').mockReturnValue(0)
    const { partitioner } = attached(Partitioners.UniformStickyPartitioner({ stickyBytes: 1e9 }), {
      ...OPTED_IN,
    })
    const partitionMetadata = metadataOf([0, 1, 1])
    expect([pick(partitioner, partitionMetadata), pick(partitioner, partitionMetadata)]).toEqual([
      7, 7,
    ])

    // `MetadataCache.applyLeaderHint` changes the cached partition in place: same array, new leader
    partitionMetadata[1].leader = 0

    expect(pick(partitioner, partitionMetadata)).toBe(11)
  })

  test('a broker that changes rack under the same partition list makes the next record leave a partition that is no longer in the rack', () => {
    jest.spyOn(Math, 'random').mockReturnValue(0)
    const racks = { 0: 'rack-a', 1: 'rack-b' }
    const { partitioner } = attached(Partitioners.UniformStickyPartitioner({ stickyBytes: 1e9 }), {
      ...OPTED_IN,
      racks,
    })
    const partitionMetadata = metadataOf(LEADERS)
    expect([pick(partitioner, partitionMetadata), pick(partitioner, partitionMetadata)]).toEqual([
      7, 7,
    ])

    // the brokers swap racks, as an endpoint hint may change a broker's rack in place
    racks[0] = 'rack-b'
    racks[1] = 'rack-a'

    expect(pick(partitioner, partitionMetadata)).toBe(2)
  })

  test('without the opt-in a sticky partition is checked once for each partition list, not at every record', () => {
    jest.spyOn(Math, 'random').mockReturnValue(0)
    const { partitioner, reads } = attached(
      Partitioners.UniformStickyPartitioner({ stickyBytes: 1e9 }),
      { rackId: 'rack-b' }
    )
    const partitionMetadata = metadataOf(LEADERS)
    expect(pick(partitioner, partitionMetadata)).toBe(2)

    partitionMetadata[0].leader = -1

    expect(pick(partitioner, partitionMetadata)).toBe(2)
    expect(pick(partitioner, metadataOf([-1, 1, 0]))).toBe(7)
    expect(reads).toEqual([])
  })

  test('adaptive choices weigh only matching partitions', () => {
    const ids = [2, 7, 9]
    const loads = { 2: 0, 7: 0, 9: 10 }
    const asked = new Set()
    const random = jest.spyOn(Math, 'random')
    const { partitioner } = attached(
      Partitioners.UniformStickyPartitioner({ stickyBytes: 1, adaptive: true }),
      {
        ...OPTED_IN,
        loadOf: (_, partition) => {
          asked.add(partition)
          return loads[partition]
        },
      }
    )
    const partitionMetadata = metadataOf([0, 1, 1], ids)

    // weights 11 for partition 7 and 1 for partition 9, so 7 holds draws below 11/12 and 9 the
    // rest; a uniform pick would split at 1/2, and send 0.75 to 9
    random.mockReturnValueOnce(0.01).mockReturnValueOnce(0.99).mockReturnValueOnce(0.75)
    const picks = [0, 1, 2].map(() => pick(partitioner, partitionMetadata))

    expect(picks).toEqual([7, 9, 7])
    expect(random).toHaveBeenCalledTimes(3)
    expect([...asked].sort()).toEqual([7, 9])
  })

  test.each(FORMS)(
    'with %s, a rack nobody leads falls back to the partitions that have a leader',
    (_name, createPartitioner, _unfiltered, _keyed, leaderless) => {
      jest.spyOn(Math, 'random').mockReturnValue(0)
      const factory = createPartitioner ?? Partitioners.UniformStickyPartitioner()
      const variants = [
        ['no broker is in the rack', { ...OPTED_IN, rackId: 'rack-z' }],
        ['no broker has a known rack', { ...OPTED_IN, racks: {} }],
      ]
      for (const [, options] of variants) {
        const { partitioner } = attached(factory, options)
        const partitionMetadata = metadataOf([-1, 1, 0])

        expect([0, 1, 2].map(() => pick(partitioner, partitionMetadata))).toEqual(leaderless)
      }
    }
  )

  test.each(FORMS)(
    'explicit partition and keyed mappings remain stable with %s',
    (_name, createPartitioner, _unfiltered, keyed) => {
      const partitionMetadata = metadataOf(LEADERS)
      const variants = [
        ['opted in', OPTED_IN],
        ['no match', { rackId: 'rack-z', rackAware: true }],
        ['opted out', { rackId: 'rack-b', rackAware: false }],
      ]
      const factory = createPartitioner ?? Partitioners.UniformStickyPartitioner()
      for (const [, options] of variants) {
        const { partitioner, reads } = attached(factory, options)

        expect(
          KEYS.map((key) => pick(partitioner, partitionMetadata, { key, value: 'v' }))
        ).toEqual(keyed)
        expect(
          [11, 5, 0].map((partition) => pick(partitioner, partitionMetadata, { partition }))
        ).toEqual([11, 5, 0])
        expect(reads).toEqual([])
      }
    }
  )

  test('JavaCompatiblePartitioner is the DefaultPartitioner function, so it shares its rack choices', () => {
    expect(Partitioners.JavaCompatiblePartitioner).toBe(Partitioners.DefaultPartitioner)
  })

  test('ignoreKeys places a keyed record by rack like a keyless one', () => {
    jest.spyOn(Math, 'random').mockReturnValue(0)
    const { partitioner } = attached(
      Partitioners.UniformStickyPartitioner({ ignoreKeys: true }),
      OPTED_IN
    )

    expect(pick(partitioner, metadataOf(LEADERS), { key: 'a', value: 'v' })).toBe(7)
  })

  test('unknown racks and no leaders retain fallback', () => {
    jest.spyOn(Math, 'random').mockReturnValue(0)
    const noLeaders = metadataOf([-1, -1, -1])
    const sticky = attached(Partitioners.UniformStickyPartitioner(), OPTED_IN).partitioner
    const roundRobin = attached(Partitioners.DefaultPartitioner, OPTED_IN).partitioner
    const unknown = attached(Partitioners.DefaultPartitioner, {
      ...OPTED_IN,
      racks: {},
    }).partitioner

    // with no leader anywhere the ordinary fallbacks apply: the first partition, or the counter's index
    expect(pick(sticky, noLeaders)).toBe(2)
    expect([pick(roundRobin, noLeaders), pick(roundRobin, noLeaders)]).toEqual([0, 1])
    expect([0, 1, 2].map(() => pick(unknown, metadataOf(LEADERS)))).toEqual([2, 7, 11])
  })

  test('a leaderless partition is never chosen while a matching leader exists', () => {
    const { partitioner } = attached(Partitioners.DefaultPartitioner, OPTED_IN)

    expect([0, 1, 2].map(() => pick(partitioner, metadataOf([0, 1, -1])))).toEqual([7, 7, 7])
  })

  test('the registered rack preference reads a rack of a leader only when it has opted in', () => {
    const optedOut = attached(Partitioners.DefaultPartitioner, { rackId: 'rack-b' })
    const optedIn = attached(Partitioners.DefaultPartitioner, OPTED_IN)

    pick(optedOut.partitioner, metadataOf(LEADERS))
    pick(optedIn.partitioner, metadataOf(LEADERS))

    expect(optedOut.reads).toEqual([])
    expect([...new Set(optedIn.reads)].sort()).toEqual([0, 1])
  })
})

describe('Producer > rack-aware partitioning > the options', () => {
  const logger = () => createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} })
  const INVALID = [
    ['rackId null', { rackId: null }, /rackId must be a string, got null/],
    ['rackId a number', { rackId: 7 }, /rackId must be a string, got 7/],
    ['rackAware null', { rackAware: null }, /rackAware must be a boolean, got null/],
    ['rackAware a string', { rackAware: 'true' }, /rackAware must be a boolean, got 'true'/],
    ['rackAware a number', { rackAware: 1 }, /rackAware must be a boolean, got 1/],
    [
      'rackAware true without a rackId',
      { rackAware: true },
      /rackAware requires a non-empty rackId/,
    ],
    [
      'rackAware true with an empty rackId',
      { rackAware: true, rackId: '' },
      /rackAware requires a non-empty rackId/,
    ],
  ]

  test.each(INVALID)('%s is refused by kafka.producer()', (_, options, message) => {
    const kafka = new Kafka({ brokers: ['localhost:9092'], logLevel: logLevel.NOTHING })

    expect(() => kafka.producer(options)).toThrow(KafkaJSInvalidConfigurationError)
    expect(() => kafka.producer(options)).toThrow(message)
  })

  test.each(INVALID)('%s is refused by direct construction', (_, options, message) => {
    const create = () =>
      createProducer({ cluster: createFakeCluster(), logger: logger(), ...options })

    expect(create).toThrow(KafkaJSInvalidConfigurationError)
    expect(create).toThrow(message)
  })

  test.each([
    ['nothing', {}],
    ['undefined values', { rackId: undefined, rackAware: undefined }],
    ['a rackId alone', { rackId: 'rack-a' }],
    ['rackAware false without a rackId', { rackAware: false }],
    ['rackAware true with a rackId', { rackId: 'rack-a', rackAware: true }],
  ])('%s: direct construction accepts the options', (_, options) => {
    const RackPreference = require('../partitioners/rackPreference')

    expect(() =>
      createProducer({ cluster: createFakeCluster(), logger: logger(), ...options })
    ).not.toThrow()
    expect(() => RackPreference.validate(options)).not.toThrow()
  })

  test('only rackAware true with a non-empty rackId turns the preference on', () => {
    const RackPreference = require('../partitioners/rackPreference')
    const eligibleWith = (context) => {
      const preference = new RackPreference()
      preference.attach({ rackOf: (nodeId) => RACKS[nodeId], ...context })
      return preference.eligible(metadataOf(LEADERS)).map(({ partitionId }) => partitionId)
    }

    expect(eligibleWith({})).toEqual(IDS)
    expect(eligibleWith({ rackId: 'rack-b' })).toEqual(IDS)
    expect(eligibleWith({ rackAware: true })).toEqual(IDS)
    expect(eligibleWith({ rackAware: 'yes', rackId: 'rack-b' })).toEqual(IDS)
    expect(eligibleWith(OPTED_IN)).toEqual([7])
  })
})

describe('Producer > rack-aware partitioning > the rack preference', () => {
  const RackPreference = require('../partitioners/rackPreference')

  /** @returns {{ preference: object, reads: number[] }} a preference attached as the producer does */
  const preferenceOf = (context, racks = RACKS) => {
    const reads = []
    const preference = new RackPreference()
    preference.attach({
      rackOf: (nodeId) => {
        reads.push(nodeId)
        return racks[nodeId]
      },
      ...context,
    })
    return { preference, reads }
  }

  test.each([
    ['a partition led in the rack', OPTED_IN, [0, 1, 0], 7, true],
    ['a partition led outside the rack while another is led in it', OPTED_IN, [0, 1, 0], 2, false],
    ['a partition led outside a rack nobody leads', OPTED_IN, [0, 0, 0], 2, true],
    ['a leaderless partition while another has a leader', OPTED_IN, [-1, 1, 0], 2, false],
    ['a leaderless partition while none has a leader', OPTED_IN, [-1, -1, -1], 2, true],
    ['a partition the topic does not have', OPTED_IN, [0, 1, 0], 3, false],
    ['a partition led with no rack known', { ...OPTED_IN, racks: {} }, [0, 1, 0], 2, true],
    ['a led partition with the preference off', {}, [0, 1, 0], 2, true],
    [
      'a leaderless partition with the preference off, another has a leader',
      {},
      [-1, 1, 0],
      2,
      false,
    ],
    [
      'a leaderless partition with the preference off, none has a leader',
      {},
      [-1, -1, -1],
      7,
      true,
    ],
    ['a partition the topic does not have with the preference off', {}, [0, 1, 0], 3, false],
  ])('allows %s', (_name, { racks, ...context }, leaders, partition, allowed) => {
    const { preference } = preferenceOf(context, racks)

    expect(preference.allows(metadataOf(leaders), partition)).toBe(allowed)
  })

  test("allows reads no rack with the preference off, and the sticky partition leader's rack alone while it is in the rack", () => {
    const off = preferenceOf({ rackId: 'rack-b' })
    const on = preferenceOf(OPTED_IN)

    expect(off.preference.allows(metadataOf(LEADERS), 2)).toBe(true)
    expect(on.preference.allows(metadataOf(LEADERS), 7)).toBe(true)

    expect(off.reads).toEqual([])
    expect(on.reads).toEqual([1])
  })

  test('eligible agrees with allows for every partition of every leader layout', () => {
    for (const leaders of [
      [0, 1, 0],
      [0, 0, 0],
      [-1, 1, 0],
      [-1, -1, -1],
      [1, 1, -1],
    ]) {
      for (const context of [OPTED_IN, { rackId: 'rack-b' }]) {
        const { preference } = preferenceOf(context)
        const partitionMetadata = metadataOf(leaders)
        const eligible = preference
          .eligible(partitionMetadata)
          .map(({ partitionId }) => partitionId)
        const candidates = eligible.length > 0 ? eligible : IDS

        expect(IDS.filter((id) => preference.allows(partitionMetadata, id))).toEqual(candidates)
      }
    }
  })
})
