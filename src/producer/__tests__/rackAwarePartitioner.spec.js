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
/** The same keys under the hash KafkaJS used before 2.0.0. */
const LEGACY_KEYED = [1, 0, 2, 0, 1, 0]
const KEYS = ['a', 'b', 'c', 'key-1', 'key-2', 'order-17']
const OPTED_IN = { rackId: 'rack-b', rackAware: true }

/**
 * Every built-in form a producer takes as `createPartitioner`, with what it picks where no rack
 * decides (`unfiltered`) and where it puts the keys of `KEYS` (`keyed`).
 * @type {Array<[name: string, createPartitioner: Function|undefined, unfiltered: number[], keyed: number[]]>}
 */
const FORMS = [
  ['no createPartitioner', undefined, STICKY, DEFAULT_KEYED],
  [
    'createPartitioner UniformStickyPartitioner()',
    Partitioners.UniformStickyPartitioner(),
    STICKY,
    DEFAULT_KEYED,
  ],
  [
    'createPartitioner UniformStickyPartitioner uncalled',
    Partitioners.UniformStickyPartitioner,
    STICKY,
    DEFAULT_KEYED,
  ],
  [
    'createPartitioner UniformStickyPartitioner({ adaptive: true })',
    Partitioners.UniformStickyPartitioner({ adaptive: true }),
    STICKY,
    DEFAULT_KEYED,
  ],
  [
    'createPartitioner DefaultPartitioner',
    Partitioners.DefaultPartitioner,
    ROUND_ROBIN,
    DEFAULT_KEYED,
  ],
  [
    'createPartitioner JavaCompatiblePartitioner',
    Partitioners.JavaCompatiblePartitioner,
    ROUND_ROBIN,
    DEFAULT_KEYED,
  ],
  [
    'createPartitioner LegacyPartitioner',
    Partitioners.LegacyPartitioner,
    ROUND_ROBIN,
    LEGACY_KEYED,
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

  /** A producer made by the exported `Kafka` class over a scripted cluster with the sparse topic. */
  const start = async (options = {}, { racks = RACKS, leaders = LEADERS } = {}) => {
    cluster = producerScript(createScriptedCluster({ nodes: NODES })).answer('Metadata', () => ({
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
    }))
    producer = new Kafka(cluster.kafkaOptions({ brokers: ['node0:9092'] })).producer({
      linger: 0,
      ...options,
    })
    await producer.connect()
  }

  const sendKeyless = async (count = RECORDS) => {
    for (let i = 0; i < count; i += 1) {
      await producer.send({ topic: TOPIC, messages: [{ value: `v-${i}` }] })
    }
  }

  /** The topic, partition and record count of every batch the client wrote, in order. */
  const written = () =>
    cluster
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

  const partitionsWritten = () => written().map(({ partition }) => partition)

  beforeEach(() => {
    jest.spyOn(Math, 'random').mockReturnValue(0)
    seedRoundRobin()
  })

  afterEach(async () => {
    if (producer) await producer.disconnect()
    if (cluster) cluster.assertClean()
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
    const choices = {}
    for (const rackId of ['rack-a', 'rack-b']) {
      await start({ createPartitioner, rackId, rackAware: true })
      await sendKeyless(4)
      choices[rackId] = partitionsWritten()
      await producer.disconnect()
      producer = null
      cluster.assertClean()
    }

    expect(choices).toEqual({ 'rack-a': [11, 2, 11, 2], 'rack-b': [7, 7, 7, 7] })
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
    /** A built-in whose `attach` is observed, with the racks it reads counted. */
    const observed = (createPartitioner) => {
      const seen = { contexts: [], reads: 0 }
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
                seen.reads += 1
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
        expect(seen.reads).toBe(0)
        expect(partitionsWritten()).toEqual(ROUND_ROBIN)
      }
    )

    test('an opted-in producer reads the racks of the leaders of the partitions it chooses among', async () => {
      const { seen, createPartitioner } = observed(Partitioners.DefaultPartitioner)
      await start({ createPartitioner, ...OPTED_IN })

      await sendKeyless(1)

      expect(seen.contexts[0]).toMatchObject(OPTED_IN)
      expect(seen.reads).toBeGreaterThan(0)
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

    // weights 11 for partition 7 and 1 for partition 9: a low draw is 7, a high one is 9
    random.mockReturnValueOnce(0.01).mockReturnValueOnce(0.99)
    const picks = [pick(partitioner, partitionMetadata), pick(partitioner, partitionMetadata)]

    expect(picks).toEqual([7, 9])
    expect([...asked].sort()).toEqual([7, 9])
  })

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
