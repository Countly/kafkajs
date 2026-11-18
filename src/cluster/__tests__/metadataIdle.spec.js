const { inspect } = require('util')
const Cluster = require('../index')
const { createErrorFromCode } = require('../../protocol/error')
const { protocol: metadataProtocol } = require('../../protocol/requests/metadata')
const { Schedule } = require('../../../testHelpers/schedule')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { newLogger, connectionOpts } = require('testHelpers')
const { createFakeCluster } = require('../../producer/__tests__/fixtures/fakeCluster')
const {
  bufferedProducer,
  deferred,
  succeeded,
  flushAll,
  watch,
} = require('../../producer/__tests__/fixtures/bufferHarness')

const RETRY = { retries: 10, initialRetryTime: 10, maxRetryTime: 10, factor: 0 }
const ID_A = '01020304-0506-0708-090a-0b0c0d0e0f10'
const ID_B = '11121314-1516-1718-191a-1b1c1d1e1f20'

/** @returns {object} a controllable answer gate */
const gate = () => {
  let release
  const promise = new Promise((resolve) => {
    release = resolve
  })
  return { promise, release }
}

/** @param {string[]} topics @param {string} id @param {number} epoch @returns {object} */
const answer = (topics, id = ID_A, epoch = 1) => ({
  brokers: [{ nodeId: 0, host: 'localhost', port: 9092 }],
  controllerId: 0,
  topicMetadata: topics.map((topic) => ({
    topic,
    topicId: id,
    partitionMetadata: [
      {
        partitionId: 0,
        leader: 0,
        leaderEpoch: epoch,
        replicas: [0],
        isr: [0],
        offlineReplicas: [],
      },
    ],
  })),
})

/** @param {object} options @returns {object} a real routing/cache path over a local broker double */
const build = (options = {}) => {
  const cluster = new Cluster({
    ...connectionOpts(),
    logger: newLogger(),
    metadataMaxIdle: 5000,
    retry: { retries: 0 },
    ...options,
  })
  const broker = {
    nodeId: 0,
    connectionPool: { host: 'localhost', port: 9092 },
    isConnected: () => true,
    disconnect: async () => {},
    retire: async () => {},
    metadata: jest.fn(async (topics) => answer(topics)),
  }
  cluster.brokerPool.seedBroker = broker
  cluster.brokerPool.brokers = { 0: broker }
  cluster.brokerPool.findConnectedBroker = async () => broker
  return { cluster, broker }
}

describe('producer metadata idle expiry', () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate'] })
  })
  afterEach(() => {
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  test('forgets the last target in both caches without asking for every topic', async () => {
    const { cluster, broker } = build()
    await cluster.addTargetTopic('topic')
    expect(cluster.topicId('topic')).toBe(ID_A)
    await jest.advanceTimersByTimeAsync(5000)
    await cluster.refreshMetadata()
    expect([...cluster.targetTopics]).toEqual([])
    expect(cluster.topicId('topic')).toBeUndefined()
    expect(cluster.topicName(ID_A)).toBeUndefined()
    expect(cluster.brokerPool.metadata.topicMetadata).toEqual([])
    expect(broker.metadata.mock.calls.map(([topics]) => topics)).toEqual([['topic']])
    expect(cluster.targetTopicState.size).toBe(0)
    expect(cluster.topicPins.size).toBe(0)
    expect(jest.getTimerCount()).toBe(0)
  })

  test('keeps replacement routing and ids when an older refresh finishes last', async () => {
    const { cluster, broker } = build()
    await cluster.addTargetTopic('topic')
    const held = gate()
    let entered = false
    broker.metadata.mockImplementationOnce(async () => {
      entered = true
      return held.promise
    })
    const old = cluster.brokerPool.refreshMetadata(['topic'], { isCurrent: () => true })
    try {
      for (let i = 0; i < 20; i += 1) await Promise.resolve()
      expect(entered).toBe(true)
      await jest.advanceTimersByTimeAsync(5000)
      await cluster.refreshMetadataIfNecessary()
      expect([...cluster.targetTopics]).toEqual([])
      broker.metadata.mockResolvedValueOnce(answer(['topic'], ID_B, 2))
      await cluster.addTargetTopic('topic')
      held.release(answer(['topic'], ID_A, 99))
      await old
      expect(cluster.topicId('topic')).toBe(ID_B)
      expect(cluster.topicName(ID_A)).toBeUndefined()
      expect(cluster.leaderEpoch('topic', 0)).toBe(2)
      expect(cluster.findTopicPartitionMetadata('topic')[0].leaderEpoch).toBe(2)
    } finally {
      held.release(answer(['topic'], ID_A, 99))
      await old
    }
  })

  test('an owner pin protects the topic until its release, which never goes below zero', async () => {
    const { cluster, broker } = build()
    await cluster.addTargetTopic('topic')
    cluster.holdTopic('topic')
    await jest.advanceTimersByTimeAsync(6000)
    await cluster.addTargetTopic('other')
    expect(cluster.targetTopics.has('topic')).toBe(true)
    cluster.releaseTopic('topic')
    cluster.releaseTopic('topic')
    await cluster.refreshMetadata()
    expect(cluster.targetTopics.has('topic')).toBe(false)
    expect(cluster.topicPins.size).toBe(0)
    expect(broker.metadata.mock.calls.every(([topics]) => topics.length > 0)).toBe(true)
  })

  test('releases every map entry of the last target and stays empty on repeated refreshes', async () => {
    const { cluster, broker } = build()
    await cluster.addTargetTopic('topic')
    await jest.advanceTimersByTimeAsync(5000)
    for (let i = 0; i < 3; i += 1) await cluster.refreshMetadata()
    expect({
      targets: cluster.targetTopics.size,
      stamps: cluster.targetTopicState.size,
      pins: cluster.topicPins.size,
      ids: cluster.metadataCache.topics.size,
      reverseIds: cluster.metadataCache.topicIds.size,
      routed: cluster.brokerPool.metadata.topicMetadata,
    }).toEqual({ targets: 0, stamps: 0, pins: 0, ids: 0, reverseIds: 0, routed: [] })
    expect(broker.metadata.mock.calls.map(([topics]) => topics)).toEqual([['topic']])
  })

  test('keeps no trace of a forgotten name, only a scalar counter', async () => {
    const { cluster } = build()
    await cluster.addTargetTopic('retired-name')
    const incarnation = cluster.targetTopicState.get('retired-name').incarnation
    await jest.advanceTimersByTimeAsync(5000)
    cluster.pruneTargetTopics()
    expect(cluster.targetTopicState.size).toBe(0)
    expect(cluster.topicPins.size).toBe(0)
    expect(typeof cluster.nextTopicIncarnation).toBe('number')
    expect(cluster.nextTopicIncarnation).toBe(incarnation)
    expect(inspect(cluster, { depth: 3, maxArrayLength: null })).not.toContain('retired-name')
    await cluster.addTargetTopic('retired-name')
    expect(cluster.targetTopicState.get('retired-name').incarnation).toBe(incarnation + 1)
  })

  test('a late reply for a forgotten topic recreates no entry anywhere', async () => {
    const { cluster, broker } = build()
    await cluster.addTargetTopic('topic')
    const held = gate()
    broker.metadata.mockImplementationOnce(async () => held.promise)
    const old = cluster.brokerPool.refreshMetadata(['topic'], { isCurrent: () => true })
    try {
      for (let i = 0; i < 20; i += 1) await Promise.resolve()
      await jest.advanceTimersByTimeAsync(5000)
      await cluster.refreshMetadataIfNecessary()
      held.release(answer(['topic'], ID_A, 99))
      await old
      expect({
        targets: [...cluster.targetTopics],
        id: cluster.topicId('topic'),
        name: cluster.topicName(ID_A),
        epoch: cluster.leaderEpoch('topic', 0),
        routed: cluster.brokerPool.metadata.topicMetadata,
        ids: cluster.metadataCache.topics.size,
        reverseIds: cluster.metadataCache.topicIds.size,
      }).toEqual({
        targets: [],
        id: undefined,
        name: undefined,
        epoch: -1,
        routed: [],
        ids: 0,
        reverseIds: 0,
      })
    } finally {
      held.release(answer(['topic'], ID_A, 99))
      await old
    }
  })

  test('resolves the replacement id after an older refresh finishes last', async () => {
    const { cluster, broker } = build()
    await cluster.addTargetTopic('topic')
    const held = gate()
    broker.metadata.mockImplementationOnce(async () => held.promise)
    const old = cluster.brokerPool.refreshMetadata(['topic'], { isCurrent: () => true })
    try {
      for (let i = 0; i < 20; i += 1) await Promise.resolve()
      await jest.advanceTimersByTimeAsync(5000)
      await cluster.refreshMetadataIfNecessary()
      broker.metadata.mockResolvedValueOnce(answer(['topic'], ID_B, 2))
      await cluster.addTargetTopic('topic')
      held.release(answer(['topic'], ID_A, 99))
      await old
      expect(cluster.topicName(ID_B)).toBe('topic')
      expect(cluster.topicName(ID_A)).toBeUndefined()
      expect(cluster.metadataCache.topicIds.size).toBe(1)
    } finally {
      held.release(answer(['topic'], ID_A, 99))
      await old
    }
  })

  test('each hold is one count, and a release with no hold left never decrements', () => {
    const { cluster } = build()
    cluster.holdTopic('topic')
    cluster.holdTopic('topic')
    expect(cluster.topicPins.get('topic')).toBe(2)
    cluster.releaseTopic('topic')
    expect(cluster.topicPins.get('topic')).toBe(1)
    cluster.releaseTopic('topic')
    cluster.releaseTopic('topic')
    expect(cluster.topicPins.size).toBe(0)
    cluster.holdTopic('topic')
    expect(cluster.topicPins.get('topic')).toBe(1)
    cluster.releaseTopic('topic')
    expect(cluster.topicPins.size).toBe(0)
  })

  describe('a failed add after a prune', () => {
    /** @returns {object} a producer cluster whose scoped Metadata for 'invalid' fails with code 3, and the gate that holds the failure while `control.held` */
    const failing = () => {
      const built = build()
      const refused = gate()
      const control = { held: false }
      built.broker.metadata.mockImplementation(async (topics) => {
        if (topics.includes('invalid')) {
          if (control.held) await refused.promise
          throw createErrorFromCode(3)
        }
        return answer(topics)
      })
      return { ...built, refused, control }
    }

    /**
     * @param {Cluster} cluster
     * @returns {object} what the cluster retains for any topic
     */
    const retained = (cluster) => ({
      targets: [...cluster.targetTopics],
      stamps: [...cluster.targetTopicState.keys()],
      pins: cluster.topicPins.size,
      ids: [...cluster.metadataCache.topics.keys()],
      reverseIds: cluster.metadataCache.topicIds.size,
      routed: cluster.brokerPool.metadata.topicMetadata.map(({ topic }) => topic),
    })
    const NOTHING = { targets: [], stamps: [], pins: 0, ids: [], reverseIds: 0, routed: [] }

    test('leaves neither the pruned nor the refused topic, and a later add starts fresh', async () => {
      const { cluster, broker } = failing()
      await cluster.addTargetTopic('valid')
      const incarnation = cluster.targetTopicState.get('valid').incarnation
      await jest.advanceTimersByTimeAsync(5000)
      await expect(cluster.addTargetTopic('invalid')).rejects.toMatchObject({
        type: 'UNKNOWN_TOPIC_OR_PARTITION',
      })
      expect(broker.metadata.mock.calls.map(([topics]) => topics)).toEqual([['valid'], ['invalid']])
      expect(retained(cluster)).toEqual(NOTHING)
      await cluster.refreshMetadata()
      expect(broker.metadata).toHaveBeenCalledTimes(2)
      await cluster.addTargetTopic('valid')
      expect(cluster.targetTopicState.get('valid').incarnation).toBeGreaterThan(incarnation)
      expect(broker.metadata).toHaveBeenLastCalledWith(['valid'], {
        guard: expect.any(Function),
        retainTopicErrors: true,
      })
      expect(cluster.topicId('valid')).toBe(ID_A)
    })

    test('survives a prune while the refused refresh is held', async () => {
      const { cluster, refused, control } = failing()
      await cluster.addTargetTopic('valid')
      await jest.advanceTimersByTimeAsync(5000)
      control.held = true
      const adding = cluster.addTargetTopic('invalid')
      const outcome = adding.catch((error) => error)
      for (let i = 0; i < 5; i += 1) await new Promise((resolve) => setImmediate(resolve))
      expect([...cluster.targetTopics]).toEqual(['invalid'])
      await jest.advanceTimersByTimeAsync(5000)
      cluster.pruneTargetTopics()
      expect(retained(cluster)).toEqual(NOTHING)
      refused.release()
      expect(await outcome).toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })
      expect(retained(cluster)).toEqual(NOTHING)
    })

    test('keeps the unconfigured rollback as it was', async () => {
      const { cluster, broker } = build({ metadataMaxIdle: undefined })
      await cluster.addTargetTopic('valid')
      broker.metadata.mockRejectedValueOnce(createErrorFromCode(3))
      await expect(cluster.addTargetTopic('invalid')).rejects.toMatchObject({
        type: 'UNKNOWN_TOPIC_OR_PARTITION',
      })
      expect([...cluster.targetTopics]).toEqual(['valid'])
      expect(cluster.targetTopicState).toBeNull()
    })
  })

  describe('a deleted target topic', () => {
    /**
     * @param {object} [options] cluster overrides
     * @returns {object} a producer cluster whose broker parses each answer as Metadata v13 does:
     *   a name in `deleted` answers code 3, a name in `creating` once answers code 5 with no
     *   partitions, as a topic being auto-created does
     */
    const deleting = (options) => {
      const built = build(options)
      const deleted = new Set()
      const creating = new Set()
      built.broker.metadata.mockImplementation(async (topics, requestOptions = {}) => {
        const data = answer(topics)
        data.errorCode = 0
        data.topicMetadata = data.topicMetadata.map((entry) => {
          if (deleted.has(entry.topic)) {
            return { topic: entry.topic, topicErrorCode: 3, partitionMetadata: [] }
          }
          if (creating.delete(entry.topic)) {
            return { topic: entry.topic, topicErrorCode: 5, partitionMetadata: [] }
          }
          return {
            ...entry,
            topicErrorCode: 0,
            partitionMetadata: entry.partitionMetadata.map((partition) => ({
              ...partition,
              partitionErrorCode: 0,
            })),
          }
        })
        return metadataProtocol({ version: 13 })({ topics, ...requestOptions }).response.parse(data)
      })
      return { ...built, deleted, creating }
    }

    test('adding another topic drops the deleted one and succeeds with one request', async () => {
      const { cluster, broker, deleted } = deleting()
      await cluster.addTargetTopic('rotating-0')
      deleted.add('rotating-0')

      await cluster.addTargetTopic('rotating-1')

      expect([...cluster.targetTopics]).toEqual(['rotating-1'])
      expect([...cluster.targetTopicState.keys()]).toEqual(['rotating-1'])
      expect(cluster.topicId('rotating-0')).toBeUndefined()
      expect(cluster.findTopicPartitionMetadata('rotating-0')).toEqual([])
      expect(cluster.findTopicPartitionMetadata('rotating-1')).toHaveLength(1)
      expect(broker.metadata.mock.calls.map(([topics]) => [...topics].sort())).toEqual([
        ['rotating-0'],
        ['rotating-0', 'rotating-1'],
      ])
    })

    test('adding the deleted topic again fails with its own error and keeps the other target', async () => {
      const { cluster, deleted } = deleting()
      await cluster.addTargetTopic('rotating-0')
      deleted.add('rotating-0')
      await cluster.addTargetTopic('rotating-1')

      await expect(cluster.addTargetTopic('rotating-0')).rejects.toMatchObject({
        type: 'UNKNOWN_TOPIC_OR_PARTITION',
      })

      expect([...cluster.targetTopics]).toEqual(['rotating-1'])
      expect(cluster.topicId('rotating-1')).toBe(ID_A)
      expect(cluster.findTopicPartitionMetadata('rotating-1')).toHaveLength(1)
    })

    test('the dropped topic does not fail a retry of the same refresh', async () => {
      const { cluster, broker, deleted, creating } = deleting({ retry: RETRY })
      await cluster.addTargetTopic('rotating-0')
      deleted.add('rotating-0')
      creating.add('rotating-1')

      const adding = cluster.addTargetTopic('rotating-1')
      await jest.advanceTimersByTimeAsync(RETRY.initialRetryTime * 2)
      await adding

      expect([...cluster.targetTopics]).toEqual(['rotating-1'])
      expect(cluster.findTopicPartitionMetadata('rotating-1')[0]).toMatchObject({ leader: 0 })
      expect(broker.metadata).toHaveBeenCalledTimes(3)
    })

    test('a held target refused while another topic is added keeps its routing until released', async () => {
      const { cluster, broker, deleted } = deleting()
      await cluster.addTargetTopic('rotating-0')
      cluster.holdTopic('rotating-0')
      deleted.add('rotating-0')

      await cluster.addTargetTopic('rotating-1')

      expect([...cluster.targetTopics]).toEqual(['rotating-0', 'rotating-1'])
      expect(cluster.findTopicPartitionMetadata('rotating-0')).toHaveLength(1)
      expect(cluster.findTopicPartitionMetadata('rotating-1')).toHaveLength(1)
      cluster.releaseTopic('rotating-0')
      await cluster.refreshMetadata()
      expect([...cluster.targetTopics]).toEqual(['rotating-1'])
      expect(cluster.findTopicPartitionMetadata('rotating-0')).toEqual([])
      expect(broker.metadata.mock.calls.map(([topics]) => [...topics].sort())).toEqual([
        ['rotating-0'],
        ['rotating-0', 'rotating-1'],
        ['rotating-0', 'rotating-1'],
      ])
    })
  })

  describe('what an empty or refused send owns', () => {
    /**
     * @param {object} [options] passed to the buffered producer
     * @returns {object} a producer over a fake cluster that counts topic pins and releases
     */
    const counted = (options) => {
      const cluster = createFakeCluster()
      cluster.holdTopic = jest.fn()
      const released = (cluster.releaseTopic = jest.fn())
      return { cluster, released, ...bufferedProducer({ cluster, idempotent: true, ...options }) }
    }

    test('an empty send takes no pin, timer or charge', async () => {
      const { cluster, producer, admission } = counted()
      await producer.connect()
      const timers = jest.getTimerCount()
      expect(await producer.send({ topic: 'topic', messages: [] })).toEqual([])
      expect(cluster.holdTopic).not.toHaveBeenCalled()
      expect(jest.getTimerCount()).toBe(timers)
      expect(admission.reserved).toBe(0)
      await producer.disconnect()
    })

    test('a record the encoder refuses is refused before any pin is taken', async () => {
      const { cluster, producer, admission } = counted()
      await producer.connect()
      await expect(
        producer.sendBatch({
          topicMessages: [
            { topic: 'valid', messages: [{ value: 'ok' }] },
            { topic: 'invalid', messages: [{ value: 12 }] },
          ],
        })
      ).rejects.toMatchObject({ name: 'KafkaJSInvalidConfigurationError' })
      expect(cluster.holdTopic).not.toHaveBeenCalled()
      expect(admission.reserved).toBe(0)
      await producer.disconnect()
    })

    test('a partitioner that throws releases its pin once', async () => {
      const { cluster, released, producer, admission } = counted({
        createPartitioner: () => () => {
          throw new Error('no partition')
        },
      })
      await producer.connect()
      const sent = watch(producer.send({ topic: 'topic', messages: [{ value: 'v' }] }))
      await flushAll()
      await jest.advanceTimersByTimeAsync(2000)
      await flushAll()
      expect(sent).toMatchObject({ state: 'rejected', value: { message: 'no partition' } })
      expect(cluster.holdTopic.mock.calls).toEqual([['topic']])
      expect(released.mock.calls).toEqual([['topic']])
      expect(admission.reserved).toBe(0)
      await producer.disconnect()
    })

    test('a send the buffer refuses releases its pin once', async () => {
      const { cluster, released, producer, admission } = counted({ capacity: 4000, maxBlockMs: 0 })
      const held = deferred()
      cluster.broker.produce.mockImplementation(async (request) => {
        await held.promise
        return succeeded(request)
      })
      await producer.connect()
      const filler = watch(
        producer.send({ topic: 'filler', messages: [{ value: 'f'.repeat(1500) }] })
      )
      await flushAll()
      const refused = watch(
        producer.send({ topic: 'topic', messages: [{ value: 'w'.repeat(1500) }] })
      )
      await flushAll()
      expect(refused).toMatchObject({
        state: 'rejected',
        value: { name: 'KafkaJSBufferExhaustedError' },
      })
      expect(cluster.holdTopic.mock.calls).toEqual([['filler'], ['topic']])
      expect(released.mock.calls).toEqual([['topic']])
      held.resolve()
      await flushAll()
      expect(filler.state).toBe('fulfilled')
      expect(released.mock.calls).toEqual([['topic'], ['filler']])
      expect(admission.reserved).toBe(0)
      await producer.disconnect()
    })
  })

  test('a cluster without tracking hands out a pin that does nothing', () => {
    const { cluster } = build({ metadataMaxIdle: undefined })
    cluster.holdTopic('topic')
    cluster.releaseTopic('topic')
    expect(cluster.topicPins).toBeNull()
    expect(() => cluster.pruneTargetTopics()).not.toThrow()
    expect(() => cluster.touchTargetTopic('topic')).not.toThrow()
  })

  test('forgetting a topic before any metadata is loaded changes nothing', () => {
    const { cluster } = build()
    expect(() => cluster.brokerPool.forgetTopic('topic')).not.toThrow()
    expect(cluster.brokerPool.metadata).toBeNull()
  })

  test('a refresh with no topic list merges nothing and keeps the routing it holds', async () => {
    const { cluster, broker } = build()
    await cluster.addTargetTopic('topic')
    broker.metadata.mockResolvedValueOnce(answer([], ID_B, 3))
    await cluster.brokerPool.refreshMetadata()
    expect(cluster.brokerPool.metadata.topicMetadata.map(({ topic }) => topic)).toEqual(['topic'])
    expect(cluster.topicId('topic')).toBe(ID_A)
  })

  test.each(['consumer', 'admin'])(
    '%s clusters retain their targets and all-topics requests',
    async () => {
      const { cluster, broker } = build({ metadataMaxIdle: undefined })
      await cluster.addTargetTopic('topic')
      await jest.advanceTimersByTimeAsync(1000000)
      await cluster.refreshMetadata()
      expect(cluster.targetTopics.has('topic')).toBe(true)
      cluster.targetTopics.clear()
      await cluster.refreshMetadata()
      expect(broker.metadata).toHaveBeenLastCalledWith([], { guard: expect.any(Function) })
      expect(cluster.targetTopicState).toBeNull()
    }
  )

  test('the producer keeps a topic pin after local expiry until its request retires', async () => {
    const cluster = createFakeCluster()
    cluster.holdTopic = jest.fn()
    const released = (cluster.releaseTopic = jest.fn())
    const { producer: owned } = bufferedProducer({ cluster, idempotent: true })
    const held = deferred()
    cluster.broker.produce.mockImplementation(async (request) => {
      const refused = request.guard(100)
      if (refused) throw refused
      await held.promise
      return succeeded(request)
    })
    let sent
    try {
      await owned.connect()
      sent = watch(owned.send({ topic: 'topic', messages: [{ value: 'pinned' }] }))
      await flushAll()
      expect(cluster.holdTopic).toHaveBeenCalledWith('topic')
      await jest.advanceTimersByTimeAsync(6000)
      expect(sent).toMatchObject({
        state: 'rejected',
        value: { name: 'KafkaJSDeliveryTimeout', outcome: 'unknown' },
      })
      expect(released).not.toHaveBeenCalled()
      held.resolve()
      await flushAll()
      expect(released.mock.calls).toEqual([['topic']])
    } finally {
      held.resolve()
      await flushAll()
      await owned.disconnect()
    }
  })

  test('a mixed send pins only the topics that contain records', async () => {
    const cluster = createFakeCluster()
    cluster.holdTopic = jest.fn()
    cluster.releaseTopic = jest.fn()
    const { producer } = bufferedProducer({ cluster, idempotent: false })
    await producer.connect()
    try {
      const sent = producer.sendBatch({
        topicMessages: [
          { topic: 'empty', messages: [] },
          { topic: 'full', messages: [{ value: 'value' }] },
        ],
      })
      await flushAll()
      await sent
      await flushAll()
      expect(cluster.holdTopic.mock.calls).toEqual([['full']])
      expect(cluster.releaseTopic.mock.calls).toEqual([['full']])
    } finally {
      await producer.disconnect()
    }
  })
})

describe('producer metadata idle expiry over the wire', () => {
  let w

  /**
   * @param {{ name: string }[]|null} requested the topics a Metadata request names
   * @param {{ ids: Map<string, string>, missing: Set<string>, leaderless: Set<string>,
   *   refused?: Map<string, number>, node?: number }} controls
   * @returns {object} the answer: unknown topics code 3, refused ones their own code, leaderless
   *   ones code 5; `node` (default 0) is the only broker listed and leads every partition
   */
  const metadataFor = (requested, { ids, missing, leaderless, refused = new Map(), node = 0 }) => ({
    brokers: [{ nodeId: node, host: `node${node}`, port: 9092, rack: null }],
    controllerId: node,
    topics: (requested ?? []).map(({ name }) => {
      if (!ids.has(name))
        ids.set(name, `00000000-0000-4000-8000-${String(ids.size + 1).padStart(12, '0')}`)
      const errorCode = missing.has(name) ? 3 : (refused.get(name) ?? 0)
      return {
        errorCode,
        name,
        topicId: ids.get(name),
        isInternal: false,
        topicAuthorizedOperations: -2147483648,
        partitions: errorCode
          ? []
          : [
              {
                errorCode: leaderless.has(name) ? 5 : 0,
                partitionIndex: 0,
                leaderId: leaderless.has(name) ? -1 : node,
                leaderEpoch: 5,
                replicas: [node],
                isr: [node],
                offlineReplicas: [],
              },
            ],
      }
    }),
  })

  /**
   * @param {{ producer?: object, cluster?: object, profile?: string }} [options] overrides for
   *   each, and the scripted cluster's profile
   * @returns {object} a real cluster and producer over the scripted wire, and the wire's controls
   */
  const start = ({
    producer: producerOptions = {},
    cluster: clusterOptions = {},
    profile = 'default',
  } = {}) => {
    const schedule = new Schedule()
    const state = { ids: new Map(), missing: new Set(), leaderless: new Set(), refused: new Map() }
    const scripted = producerScript(
      createScriptedCluster({
        nodes: [
          { id: 0, host: 'node0', port: 9092 },
          { id: 1, host: 'node1', port: 9092 },
        ],
        schedule,
        profile,
      })
    ).answer('Metadata', ({ topics }) => metadataFor(topics, state))
    const cluster = new Cluster({
      logger: newLogger(),
      brokers: ['node0:9092'],
      socketFactory: scripted.socketFactory,
      clientId: 'metadata-idle',
      connectionTimeout: 1000,
      requestTimeout: 1000,
      enforceRequestTimeout: false,
      metadataMaxAge: 300000,
      metadataMaxIdle: 5000,
      retry: RETRY,
      allowAutoTopicCreation: true,
      maxInFlightRequests: 5,
      ...clusterOptions,
    })
    const { producer, admission } = bufferedProducer({
      cluster,
      retry: RETRY,
      deliveryTimeout: 20000,
      ...producerOptions,
    })
    return { schedule, state, scripted, cluster, producer, admission }
  }

  /** @returns {(string[]|null)[]} the sorted topic list of every Metadata request, null for all topics */
  const metadataRequests = () =>
    w.scripted
      .requests()
      .filter(({ api }) => api === 'Metadata')
      .map(({ fields }) => fields.topics?.map(({ name }) => name).sort() ?? null)

  /**
   * Pumps bounded runnable work until the predicate holds.
   * @param {() => boolean} predicate
   * @returns {Promise<void>}
   */
  const until = async (predicate) => {
    for (let i = 0; i < 200 && !predicate(); i += 1) {
      await jest.advanceTimersByTimeAsync(1)
      await new Promise((resolve) => setImmediate(resolve))
    }
    expect(predicate()).toBe(true)
  }

  /**
   * @param {string} topic
   * @param {string} [value]
   * @returns {Promise<object[]>} the public send of one record
   */
  const send = (topic, value = 'v') => w.producer.send({ topic, messages: [{ value }] })

  /**
   * Pumps until the promise settles.
   * @param {Promise<unknown>} promise
   * @returns {Promise<{ state: string, value: unknown }>} its settled state
   */
  const settle = async (promise) => {
    const watched = watch(promise)
    await until(() => watched.state !== 'pending')
    return watched
  }

  /**
   * @param {string} topic
   * @returns {object} a selector for the Produce requests that carry the topic
   */
  const produceOf = (topic) => ({
    api: 'Produce',
    match: ({ fields }) =>
      fields.topics.some(
        (entry) => entry.topic === topic || entry.topicId === w.state.ids.get(topic)
      ),
  })

  /**
   * @param {Error} error
   * @returns {string[]} the protocol type, or else the name, of the error and of each cause
   */
  const chainOf = (error) => {
    const chain = []
    for (let at = error; at; at = at.cause) {
      chain.push(at.name === 'KafkaJSProtocolError' ? at.type : at.name)
    }
    return chain
  }

  /**
   * Advances fake time in 100 ms steps until no send is pending.
   * @param {{ state: string }[]} sends watched sends
   * @returns {Promise<void>}
   */
  const untilSettled = async (sends) => {
    for (let i = 0; i < 250 && sends.some(({ state }) => state === 'pending'); i += 1) {
      await jest.advanceTimersByTimeAsync(100)
      await new Promise((resolve) => setImmediate(resolve))
    }
    expect(sends.map(({ state }) => state)).not.toContain('pending')
  }

  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
  })
  afterEach(async () => {
    if (w) {
      await w.producer.disconnect()
      w.scripted.assertClean()
    }
    w = undefined
    jest.useRealTimers()
  })

  test('a held Produce keeps its topic after the send expired locally, and its retirement frees it', async () => {
    w = start({ producer: { deliveryTimeout: 1000 } })
    const blocked = w.schedule.hold(produceOf('held'))
    let held
    try {
      await w.producer.connect()
      held = watch(send('held'))
      await until(() => blocked.call !== null)
      await jest.advanceTimersByTimeAsync(6000)
      expect(held).toMatchObject({
        state: 'rejected',
        value: { name: 'KafkaJSDeliveryTimeout', outcome: 'unknown' },
      })
      expect(await settle(send('other'))).toMatchObject({ state: 'fulfilled' })
      expect(metadataRequests().at(-1)).toEqual(['held', 'other'])
      expect(w.cluster.targetTopics.has('held')).toBe(true)
      expect(w.cluster.topicPins.get('held')).toBe(1)
      expect(w.cluster.topicId('held')).toBeDefined()
      expect(w.cluster.brokerPool.metadata.topicMetadata.map(({ topic }) => topic)).toContain(
        'held'
      )
      const during = w.admission.reserved
      expect(during).toBeGreaterThan(0)
      blocked.release()
      await until(() => w.cluster.topicPins.size === 0)
      expect(w.admission.reserved).toBe(0)
      expect(await settle(w.cluster.refreshMetadata())).toMatchObject({ state: 'fulfilled' })
      expect(w.cluster.targetTopics.has('held')).toBe(false)
      expect(w.cluster.topicId('held')).toBeUndefined()
      expect(metadataRequests().at(-1)).toEqual(['other'])
    } finally {
      if (blocked.settle) blocked.release()
    }
  })

  /**
   * Runs past the idle limit and prunes, as a refresh does; the pinned topic must stay.
   * @param {{ loaded?: boolean }} [options] whether its metadata has been loaded
   * @returns {Promise<void>}
   */
  const expectKept = async ({ loaded = true } = {}) => {
    const topic = 'topic'
    await jest.advanceTimersByTimeAsync(6000)
    w.cluster.pruneTargetTopics()
    expect(w.cluster.topicPins.get(topic)).toBe(1)
    expect(w.cluster.targetTopics.has(topic)).toBe(true)
    expect(w.cluster.targetTopicState.has(topic)).toBe(true)
    expect(w.cluster.topicId(topic) !== undefined).toBe(loaded)
  }

  /**
   * Once every owner ended, the topic is bookkept nowhere after the idle limit.
   * @returns {Promise<void>}
   */
  const expectExpired = async () => {
    expect(w.cluster.topicPins.size).toBe(0)
    expect(w.admission.reserved).toBe(0)
    await jest.advanceTimersByTimeAsync(5000)
    w.cluster.pruneTargetTopics()
    expect(w.cluster.targetTopics.size).toBe(0)
    expect(w.cluster.targetTopicState.size).toBe(0)
    expect(w.cluster.topicId('topic')).toBeUndefined()
    expect(w.cluster.brokerPool.metadata.topicMetadata).toEqual([])
  }

  test.each(['released', 'rejected'])(
    'a send waiting for buffer room keeps its active topic until it is %s',
    async (ending) => {
      w = start({ producer: { capacity: 4000, maxBlockMs: ending === 'rejected' ? 7000 : 60000 } })
      const blocked = w.schedule.hold(produceOf('filler'))
      try {
        await w.producer.connect()
        expect(await settle(send('topic'))).toMatchObject({ state: 'fulfilled' })
        const filler = watch(send('filler', 'f'.repeat(1500)))
        await until(() => blocked.call !== null)
        const waiting = watch(send('topic', 'w'.repeat(1500)))
        await until(() => w.admission.waitingBytes > 0)
        await expectKept()
        if (ending === 'rejected') {
          await jest.advanceTimersByTimeAsync(1500)
          await until(() => waiting.state === 'rejected')
          expect(waiting.value).toMatchObject({ name: 'KafkaJSBufferExhaustedError' })
        }
        blocked.release()
        await until(() => filler.state === 'fulfilled' && waiting.state !== 'pending')
        expect(waiting.state).toBe(ending === 'rejected' ? 'rejected' : 'fulfilled')
        await until(() => w.cluster.topicPins.size === 0)
        await expectExpired()
      } finally {
        if (blocked.settle) blocked.release()
      }
    }
  )

  test('a send waiting for the producer id keeps its active topic', async () => {
    w = start()
    const blocked = w.schedule.hold({ api: 'InitProducerId' })
    try {
      const connecting = watch(w.producer.connect())
      await until(() => blocked.call !== null)
      expect(await settle(w.cluster.addMultipleTargetTopics(['topic']))).toMatchObject({
        state: 'fulfilled',
      })
      const sent = watch(send('topic'))
      await until(() => w.cluster.topicPins.get('topic') === 1)
      await expectKept()
      blocked.release()
      await until(() => connecting.state !== 'pending' && sent.state !== 'pending')
      expect([connecting.state, sent.state]).toEqual(['fulfilled', 'fulfilled'])
      await expectExpired()
    } finally {
      if (blocked.settle) blocked.release()
    }
  })

  test('a send waiting for its initial partition metadata keeps its topic', async () => {
    w = start()
    const blocked = w.schedule.hold({
      api: 'Metadata',
      match: ({ fields }) => fields.topics?.some(({ name }) => name === 'topic'),
    })
    try {
      await w.producer.connect()
      const sent = watch(send('topic'))
      await until(() => blocked.call !== null)
      await expectKept({ loaded: false })
      blocked.release()
      await until(() => sent.state !== 'pending')
      expect(sent.state).toBe('fulfilled')
      await expectExpired()
    } finally {
      if (blocked.settle) blocked.release()
    }
  })

  test('a send whose topic has no leader yet keeps its topic', async () => {
    const slow = { retries: 10, initialRetryTime: 3000, maxRetryTime: 3000, factor: 0 }
    w = start({ cluster: { retry: slow } })
    w.state.leaderless.add('topic')
    await w.producer.connect()
    const sent = watch(send('topic'))
    await until(() => w.cluster.topicPins.get('topic') === 1)
    await expectKept({ loaded: false })
    w.state.leaderless.delete('topic')
    await jest.advanceTimersByTimeAsync(3000)
    await until(() => sent.state !== 'pending')
    expect(sent.state).toBe('fulfilled')
    await expectExpired()
  })

  test('a queued retry keeps its topic', async () => {
    const backoff = { retries: 10, initialRetryTime: 8000, maxRetryTime: 8000, factor: 0 }
    w = start({ producer: { retry: backoff } })
    w.schedule.once(produceOf('topic'), ({ fields }) => ({
      responses: fields.topics.map(({ topicId, partitions }) => ({
        topicId,
        partitions: partitions.map(({ partition }) => ({
          partition,
          errorCode: 7,
          baseOffset: '-1',
          logAppendTime: '-1',
          logStartOffset: '-1',
          leader: null,
        })),
      })),
      nodeEndpoints: [],
    }))
    await w.producer.connect()
    const sent = watch(send('topic'))
    await until(() => w.scripted.requests().some(({ api }) => api === 'Produce'))
    await until(() =>
      w.scripted.requests().some(({ answeredOrder }) => answeredOrder !== undefined)
    )
    await expectKept()
    await jest.advanceTimersByTimeAsync(3000)
    await until(() => sent.state !== 'pending')
    expect(sent.state).toBe('fulfilled')
    expect(w.scripted.requests().filter(({ api }) => api === 'Produce')).toHaveLength(2)
    await expectExpired()
  })

  describe('a topic deleted after it was forgotten', () => {
    /**
     * Sends to 'gone', lets it go idle, forgets it through another topic's refresh, then deletes it.
     * @param {object} options as `start`
     * @returns {Promise<{ requests: number, metadata: number }>} the request counts at that point
     */
    const forgetAndDelete = async (options) => {
      w = start(options)
      await w.producer.connect()
      expect(await settle(send('gone'))).toMatchObject({ state: 'fulfilled' })
      await jest.advanceTimersByTimeAsync(5000)
      expect(await settle(send('other'))).toMatchObject({ state: 'fulfilled' })
      expect(metadataRequests().at(-1)).toEqual(['other'])
      expect(w.cluster.topicId('gone')).toBeUndefined()
      return { requests: w.scripted.requests().length, metadata: metadataRequests().length }
    }

    /**
     * @param {number} count requests already written
     * @returns {string[]} every topic id a Produce request carried after them
     */
    const producedAfter = (count) =>
      w.scripted
        .requests()
        .slice(count)
        .filter(({ api }) => api === 'Produce')
        .flatMap(({ fields }) => fields.topics.map(({ topicId }) => topicId))

    test('a send to it fails typed within its deadline when auto creation is off', async () => {
      const before = await forgetAndDelete({
        cluster: { allowAutoTopicCreation: false },
        producer: { deliveryTimeout: 3000 },
      })
      const oldId = w.state.ids.get('gone')
      w.state.missing.add('gone')
      const started = Date.now()
      const refused = await settle(send('gone'))
      expect(refused.state).toBe('rejected')
      expect(refused.value).toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })
      expect(Date.now() - started).toBeLessThanOrEqual(3000)
      const fresh = metadataRequests().slice(before.metadata)
      expect(fresh[0]).toContain('gone')
      expect(fresh.every((topics) => topics !== null && topics.length > 0)).toBe(true)
      expect(producedAfter(before.requests)).not.toContain(oldId)
      expect(w.cluster.targetTopics.has('gone')).toBe(false)
      expect(await settle(send('other'))).toMatchObject({ state: 'fulfilled' })
    })

    test('a send to it creates it afresh when auto creation is on', async () => {
      const before = await forgetAndDelete({ producer: { deliveryTimeout: 3000 } })
      const oldId = w.state.ids.get('gone')
      const newId = '00000000-0000-4000-8000-0000000000ff'
      w.state.ids.set('gone', newId)
      expect(await settle(send('gone'))).toMatchObject({ state: 'fulfilled' })
      const fresh = metadataRequests().slice(before.metadata)
      expect(fresh[0]).toContain('gone')
      expect(fresh.every((topics) => topics !== null && topics.length > 0)).toBe(true)
      expect(w.cluster.topicId('gone')).toBe(newId)
      expect(producedAfter(before.requests)).toContain(newId)
      expect(producedAfter(before.requests)).not.toContain(oldId)
    })
  })

  test.each([false, true])(
    'with auto creation %s, a deleted target fails only its own sends and a send to a new topic succeeds at once',
    async (allowAutoTopicCreation) => {
      w = start({ cluster: { allowAutoTopicCreation }, producer: { deliveryTimeout: 3000 } })
      await w.producer.connect()
      expect(await settle(send('rotating-0'))).toMatchObject({ state: 'fulfilled' })
      w.state.missing.add('rotating-0')
      const before = metadataRequests().length
      const started = Date.now()

      expect(await settle(send('rotating-1'))).toMatchObject({ state: 'fulfilled' })
      expect(Date.now() - started).toBeLessThan(100)
      expect(metadataRequests().slice(before)).toEqual([['rotating-0', 'rotating-1']])
      expect([...w.cluster.targetTopics]).toEqual(['rotating-1'])

      const refused = await settle(send('rotating-0'))
      expect(refused.state).toBe('rejected')
      expect(refused.value).toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })
      expect([...w.cluster.targetTopics]).toEqual(['rotating-1'])
      expect(await settle(send('rotating-1'))).toMatchObject({ state: 'fulfilled' })
    }
  )

  test.each(['reconnect', 'rebootstrap'])(
    'after a %s cleared the metadata, a deleted earlier target fails only its own sends and a send to a new topic succeeds at once',
    async (event) => {
      w = start({ producer: { deliveryTimeout: 3000 } })
      await w.producer.connect()
      expect(await settle(send('a'))).toMatchObject({ state: 'fulfilled' })
      expect(await settle(send('b'))).toMatchObject({ state: 'fulfilled' })
      w.state.missing.add('a')

      if (event === 'reconnect') {
        expect(await settle(w.producer.disconnect())).toMatchObject({ state: 'fulfilled' })
        expect(await settle(w.producer.connect())).toMatchObject({ state: 'fulfilled' })
      } else {
        const restarted = w.cluster.brokerPool.rebootstrap('required')
        expect(await settle(restarted)).toMatchObject({ state: 'fulfilled' })
      }
      expect(w.cluster.brokerPool.metadata).toBeNull()
      expect([...w.cluster.targetTopics].sort()).toEqual(['a', 'b'])

      const started = Date.now()
      expect(await settle(send('b'))).toMatchObject({ state: 'fulfilled' })
      expect(await settle(send('c'))).toMatchObject({ state: 'fulfilled' })
      expect(Date.now() - started).toBeLessThan(100)
      expect([...w.cluster.targetTopics].sort()).toEqual(['b', 'c'])

      const refused = await settle(send('a'))
      expect(refused.state).toBe('rejected')
      expect(refused.value).toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })
      expect([...w.cluster.targetTopics].sort()).toEqual(['b', 'c'])
    }
  )

  test('after a reconnect, a fresh send to the deleted earlier target ends with the missing-metadata error while a send to another topic succeeds', async () => {
    w = start()
    await w.producer.connect()
    expect(await settle(send('a'))).toMatchObject({ state: 'fulfilled' })
    expect(await settle(send('b'))).toMatchObject({ state: 'fulfilled' })
    w.state.missing.add('a')
    expect(await settle(w.producer.disconnect())).toMatchObject({ state: 'fulfilled' })
    expect(await settle(w.producer.connect())).toMatchObject({ state: 'fulfilled' })
    expect(w.cluster.brokerPool.metadata).toBeNull()

    const started = Date.now()
    const refused = watch(send('a'))
    expect(await settle(send('b'))).toMatchObject({ state: 'fulfilled' })
    expect(Date.now() - started).toBeLessThan(100)

    await untilSettled([refused])
    expect(refused.state).toBe('rejected')
    expect(chainOf(refused.value)).toEqual([
      'KafkaJSNumberOfRetriesExceeded',
      'KafkaJSMetadataNotLoaded',
    ])
    expect(Date.now() - started).toBeLessThanOrEqual(20000)
    await until(() => w.cluster.topicPins.size === 0)
    expect(w.admission.reserved).toBe(0)

    expect(await settle(w.cluster.refreshMetadata())).toMatchObject({ state: 'fulfilled' })
    expect([...w.cluster.targetTopics]).toEqual(['b'])
  })

  describe('a target refused while sends still own it', () => {
    /**
     * @param {{ topic?: string, topicId?: string }} entry a Produce topic entry, by name or by id
     * @param {string} topic
     * @returns {boolean} whether the entry is the topic's
     */
    const isTopic = (entry, topic) =>
      entry.topic === topic || entry.topicId === w.state.ids.get(topic)

    /**
     * @param {{ topics: object[] }} fields a Produce request's fields
     * @param {(entry: object) => number} codeOf the error code each topic's partitions answer
     * @returns {object} the Produce answer
     */
    const produceAnswer = ({ topics }, codeOf) => ({
      responses: topics.map(({ partitions, ...key }) => ({
        ...key,
        partitions: partitions.map(({ partition }) => ({
          partition,
          errorCode: codeOf(key),
          baseOffset: codeOf(key) ? '-1' : '0',
          logAppendTime: '-1',
          logStartOffset: codeOf(key) ? '-1' : '0',
          leader: null,
        })),
      })),
      nodeEndpoints: [],
    })

    /**
     * Answers the next Metadata request that names `trigger` with `topic` refused with `code`.
     * @param {string} topic
     * @param {number} code
     * @param {string} trigger
     * @returns {void}
     */
    const refuseOnce = (topic, code, trigger) =>
      w.schedule.once(
        {
          api: 'Metadata',
          match: ({ fields }) => fields.topics?.some(({ name }) => name === trigger),
        },
        ({ fields }) =>
          metadataFor(fields.topics, { ...w.state, refused: new Map([[topic, code]]) })
      )

    /**
     * @param {string} topic
     * @param {number} from requests already written
     * @returns {number[]} the base sequence of every batch of `topic` a later Produce carried
     */
    const sequencesOf = (topic, from) =>
      w.scripted
        .requests()
        .slice(from)
        .filter(({ api }) => api === 'Produce')
        .flatMap(({ fields }) => fields.topics.filter((entry) => isTopic(entry, topic)))
        .flatMap(({ partitions }) => partitions.flatMap(({ batches }) => batches))
        .map(({ baseSequence }) => baseSequence)

    /**
     * Starts a producer whose first send to 'a' succeeded and whose next one is held in flight
     * with a third partitioned and queued behind it.
     * @param {object} [options] as `start`; a `tv1`/`tv2` profile makes it transactional
     * @returns {Promise<object>} the sender, the held gate, both sends and the request count
     *   before them
     */
    const owning = async (options = {}) => {
      const transactional = options.profile === 'tv1' || options.profile === 'tv2'
      w = start({
        ...options,
        producer: transactional ? { transactionalId: 'owned', idempotent: true } : {},
      })
      w.scripted.answer('InitProducerId', ({ producerEpoch = -1 }) => ({
        producerId: '1000',
        producerEpoch: Math.max(0, producerEpoch + 1),
      }))
      await w.producer.connect()
      const sender = transactional ? await w.producer.transaction() : w.producer
      const sendTo = (topic, value) => sender.send({ topic, messages: [{ value }] })
      expect(await settle(sendTo('a', 'first'))).toMatchObject({ state: 'fulfilled' })
      const from = w.scripted.requests().length
      const blocked = w.schedule.hold(produceOf('a'))
      const inFlight = watch(sendTo('a', 'second'))
      await until(() => blocked.call !== null)
      const queued = watch(sendTo('a', 'third'))
      await until(() => w.cluster.topicPins.get('a') === 2)
      await flushAll()
      return { sender, sendTo, blocked, inFlight, queued, from }
    }

    test.each(['default', 'tv1', 'tv2'])(
      'under %s, an owned target refused once while another topic is added keeps its routing and its sends succeed in order',
      async (profile) => {
        const { sender, sendTo, blocked, inFlight, queued, from } = await owning({ profile })
        try {
          refuseOnce('a', 3, 'b')
          expect(await settle(sendTo('b', 'other'))).toMatchObject({ state: 'fulfilled' })
          expect([...w.cluster.targetTopics].sort()).toEqual(['a', 'b'])
          expect(w.cluster.findTopicPartitionMetadata('a')).toHaveLength(1)

          blocked.release(produceAnswer(blocked.call.fields, () => 6))
          await until(() => inFlight.state !== 'pending' && queued.state !== 'pending')
          expect([inFlight.state, queued.state]).toEqual(['fulfilled', 'fulfilled'])
          expect(sequencesOf('a', from)).toEqual([1, 1, 2])
          if (profile !== 'default') {
            expect(await settle(sender.commit())).toMatchObject({ state: 'fulfilled' })
          }
          await until(() => w.cluster.topicPins.size === 0)
          expect(w.admission.reserved).toBe(0)
        } finally {
          if (blocked.settle) blocked.release()
        }
      }
    )

    test('a queued retry of an owned target refused once is sent when its backoff ends', async () => {
      const backoff = { retries: 10, initialRetryTime: 3000, maxRetryTime: 3000, factor: 0 }
      w = start({ producer: { retry: backoff } })
      await w.producer.connect()
      expect(await settle(send('a', 'first'))).toMatchObject({ state: 'fulfilled' })
      const from = w.scripted.requests().length
      w.schedule.once(produceOf('a'), ({ fields }) => produceAnswer(fields, () => 7))
      const queued = watch(send('a', 'second'))
      await until(() =>
        w.scripted
          .requests()
          .slice(from)
          .some(({ api, answeredOrder }) => api === 'Produce' && answeredOrder !== undefined)
      )

      refuseOnce('a', 3, 'b')
      expect(await settle(send('b'))).toMatchObject({ state: 'fulfilled' })
      expect([...w.cluster.targetTopics].sort()).toEqual(['a', 'b'])
      expect(queued.state).toBe('pending')

      await jest.advanceTimersByTimeAsync(3000)
      await until(() => queued.state !== 'pending')
      expect(queued.state).toBe('fulfilled')
      expect(sequencesOf('a', from)).toEqual([1, 1])
      await until(() => w.cluster.topicPins.size === 0)
      expect(w.admission.reserved).toBe(0)
    })

    /**
     * Refuses 'a' with `code` in every Metadata and Produce answer while its leader still answers,
     * with one send of 'a' held in flight and another queued behind it. Expects both to end with
     * `chain` once the held request is let go, every pin and reservation released, 'a' dropped and
     * 'b' served.
     * @param {number} code the topic error and Produce error code
     * @param {string[]} chain the cause chain each send of 'a' ends with
     * @returns {Promise<void>}
     */
    const expectRefusedWhileLeaderAnswers = async (code, chain) => {
      const { sendTo, blocked, inFlight, queued } = await owning()
      try {
        w.state.refused.set('a', code)
        w.scripted.answer('Produce', (fields) =>
          produceAnswer(fields, (entry) => (isTopic(entry, 'a') ? code : 0))
        )
        expect(await settle(sendTo('b', 'other'))).toMatchObject({ state: 'fulfilled' })
        expect([...w.cluster.targetTopics].sort()).toEqual(['a', 'b'])

        blocked.release(produceAnswer(blocked.call.fields, () => 6))
        await untilSettled([inFlight, queued])
        for (const sent of [inFlight, queued]) {
          expect(sent.state).toBe('rejected')
          expect(chainOf(sent.value)).toEqual(chain)
        }
        await until(() => w.cluster.topicPins.size === 0)
        expect(w.admission.reserved).toBe(0)

        expect(await settle(w.cluster.refreshMetadata())).toMatchObject({ state: 'fulfilled' })
        expect([...w.cluster.targetTopics]).toEqual(['b'])
        expect(await settle(sendTo('b', 'again'))).toMatchObject({ state: 'fulfilled' })
      } finally {
        if (blocked.settle) blocked.release()
      }
    }

    test('an owned target refused as unauthorized fails its own sends with that error while another topic succeeds', async () => {
      await expectRefusedWhileLeaderAnswers(29, ['TOPIC_AUTHORIZATION_FAILED'])
    })

    test('an owned target refused as unknown while its leader still answers ends its sends with the retry limit and its own error while another topic succeeds', async () => {
      await expectRefusedWhileLeaderAnswers(3, [
        'KafkaJSNumberOfRetriesExceeded',
        'UNKNOWN_TOPIC_OR_PARTITION',
      ])
    })

    /**
     * Expects the owned sends rejected within the 20 s delivery budget, none with the topic's own
     * error, then every pin and reservation released and the refused topic dropped, with another
     * topic still served. A retried send still waiting to find its broker holds the last pin
     * until that lookup fails, about 17 s after its send ended, so the release is awaited for up
     * to 40 s of fake time.
     * @param {object} owned the sender, held gate and sends of `owning`
     * @param {number} released when the held request was let go
     * @param {string[][]} chains the cause chain of the in-flight send and of the queued one
     * @returns {Promise<void>}
     */
    const expectEnded = async ({ sendTo, inFlight, queued }, released, chains) => {
      expect(Date.now() - released).toBeLessThanOrEqual(20000)
      for (const [i, sent] of [inFlight, queued].entries()) {
        expect(sent.state).toBe('rejected')
        expect(chainOf(sent.value)).toEqual(chains[i])
      }
      for (let i = 0; i < 40 && w.cluster.topicPins.size > 0; i += 1) {
        await jest.advanceTimersByTimeAsync(1000)
      }
      expect(w.cluster.topicPins.size).toBe(0)
      expect(w.admission.reserved).toBe(0)
      expect(await settle(w.cluster.refreshMetadata())).toMatchObject({ state: 'fulfilled' })
      expect(w.cluster.targetTopics.has('a')).toBe(false)
      const served = watch(sendTo('b', 'again'))
      await untilSettled([served])
      expect(served.state).toBe('fulfilled')
    }

    test.each([
      ['unknown', 3],
      ['unauthorized', 29],
    ])(
      'an owned target refused as %s whose old leader left the cluster ends its sends with a routing error while another topic succeeds',
      async (_, code) => {
        const owned = await owning()
        try {
          w.state.refused.set('a', code)
          w.state.node = 1
          expect(await settle(owned.sendTo('b', 'other'))).toMatchObject({ state: 'fulfilled' })
          expect([...w.cluster.targetTopics].sort()).toEqual(['a', 'b'])
          expect(w.cluster.brokerPool.getNodeIds()).toEqual(['1'])

          const released = Date.now()
          owned.blocked.release(produceAnswer(owned.blocked.call.fields, () => 6))
          await untilSettled([owned.inFlight, owned.queued])
          const routing = ['KafkaJSNumberOfRetriesExceeded', 'KafkaJSBrokerNotFound']
          await expectEnded(owned, released, [routing, routing])
        } finally {
          if (owned.blocked.settle) owned.blocked.release()
        }
      }
    )

    test.each([
      ['unknown', 3],
      ['unauthorized', 29],
    ])(
      'an owned target refused as %s while the cluster cannot be reached ends its sends at the delivery timeout while another topic succeeds',
      async (_, code) => {
        const owned = await owning()
        try {
          w.state.refused.set('a', code)
          expect(await settle(owned.sendTo('b', 'other'))).toMatchObject({ state: 'fulfilled' })
          expect([...w.cluster.targetTopics].sort()).toEqual(['a', 'b'])

          w.scripted.down(0)
          for (const socket of w.scripted.sockets()) {
            if (!socket.destroyed)
              socket.destroy(Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' }))
          }
          const released = Date.now()
          owned.blocked.release()
          await untilSettled([owned.inFlight, owned.queued])
          w.scripted.up(0)
          await expectEnded(owned, released, [
            ['KafkaJSDeliveryTimeout', 'KafkaJSBrokerNotFound'],
            ['KafkaJSDeliveryTimeout'],
          ])
        } finally {
          w.scripted.up(0)
          if (owned.blocked.settle) owned.blocked.release()
        }
      }
    )

    test('an owned target with no installed metadata after a rebootstrap ends its sends within the delivery budget while another topic succeeds', async () => {
      const owned = await owning()
      try {
        w.state.refused.set('a', 3)
        const restarted = w.cluster.brokerPool.rebootstrap('required')
        expect(await settle(restarted)).toMatchObject({ state: 'fulfilled' })
        expect(w.cluster.brokerPool.metadata).toBeNull()

        expect(await settle(owned.sendTo('b', 'other'))).toMatchObject({ state: 'fulfilled' })
        expect([...w.cluster.targetTopics].sort()).toEqual(['a', 'b'])
        expect(w.cluster.findTopicPartitionMetadata('a')).toEqual([])

        const released = Date.now()
        owned.blocked.release(produceAnswer(owned.blocked.call.fields, () => 6))
        await untilSettled([owned.inFlight, owned.queued])
        const noMetadata = ['KafkaJSNumberOfRetriesExceeded', 'KafkaJSMetadataNotLoaded']
        await expectEnded(owned, released, [noMetadata, noMetadata])
      } finally {
        if (owned.blocked.settle) owned.blocked.release()
      }
    })
  })

  test('sends to ten thousand distinct topics leave bounded bookkeeping', async () => {
    w = start({ producer: { capacity: 33554432 } })
    await w.producer.connect()
    const names = (round) => Array.from({ length: 1000 }, (_, i) => `topic-${round}-${i}`)
    for (let round = 0; round < 10; round += 1) {
      const sent = await settle(
        w.producer.sendBatch({
          topicMessages: names(round).map((topic) => ({ topic, messages: [{ value: 'v' }] })),
        })
      )
      expect(sent.state).toBe('fulfilled')
      expect(w.cluster.topicPins.size).toBe(0)
      expect(w.cluster.targetTopics.size).toBeLessThanOrEqual(2000)
      expect(w.cluster.metadataCache.topics.size).toBeLessThanOrEqual(2000)
      await jest.advanceTimersByTimeAsync(5000)
    }
    const requests = metadataRequests()
    expect(requests).toHaveLength(10)
    expect(Math.max(...requests.map((topics) => topics.length))).toBe(1000)
    expect(await settle(w.cluster.refreshMetadata())).toMatchObject({ state: 'fulfilled' })
    expect(metadataRequests()).toHaveLength(10)
    expect({
      targets: w.cluster.targetTopics.size,
      stamps: w.cluster.targetTopicState.size,
      pins: w.cluster.topicPins.size,
      ids: w.cluster.metadataCache.topics.size,
      reverseIds: w.cluster.metadataCache.topicIds.size,
      routed: w.cluster.brokerPool.metadata.topicMetadata.length,
    }).toEqual({ targets: 0, stamps: 0, pins: 0, ids: 0, reverseIds: 0, routed: 0 })
    expect(w.cluster.nextTopicIncarnation).toBe(10000)
  })
})

describe('producer metadataMaxIdle option', () => {
  let created
  let Kafka

  beforeEach(() => {
    created = []
    jest.isolateModules(() => {
      jest.doMock('../index', () => {
        const Actual = jest.requireActual('../index')
        return class extends Actual {
          constructor(options) {
            super(options)
            created.push(options)
          }
        }
      })
      ;({ Kafka } = require('../../../index'))
    })
  })
  afterEach(() => {
    jest.dontMock('../index')
  })

  /** @returns {object} a client that reaches no broker */
  const kafka = () => new Kafka({ brokers: ['localhost:9092'], logLevel: 0 })

  test('defaults to 300000 on a producer cluster', () => {
    kafka().producer()
    expect(created.map(({ metadataMaxIdle }) => metadataMaxIdle)).toEqual([300000])
  })

  test.each([5000, 2147483647])('accepts %d and hands it to the producer cluster', (value) => {
    kafka().producer({ metadataMaxIdle: value })
    expect(created.map(({ metadataMaxIdle }) => metadataMaxIdle)).toEqual([value])
  })

  test.each([4999, 2147483648, -1, 1.5, NaN, Infinity, '5000', null, true, false])(
    'rejects %p before any cluster exists',
    (value) => {
      let error
      try {
        kafka().producer({ metadataMaxIdle: value })
      } catch (thrown) {
        error = thrown
      }
      expect(error?.name).toBe('KafkaJSInvalidConfigurationError')
      expect(error?.message).toContain('metadataMaxIdle')
      expect(created).toEqual([])
    }
  )

  test('consumer and admin clusters never carry it', () => {
    const client = kafka()
    client.consumer({ groupId: 'group', metadataMaxIdle: 5000 })
    client.admin({ metadataMaxIdle: 5000 })
    expect(created).toHaveLength(2)
    expect(created.map(({ metadataMaxIdle }) => metadataMaxIdle)).toEqual([undefined, undefined])
  })
})
