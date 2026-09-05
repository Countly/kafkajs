const createConsumer = require('../index')
const ConsumerGroup = require('../consumerGroup')
const Runner = require('../runner')
const OffsetManager = require('../offsetManager')
const SeekOffsets = require('../seekOffsets')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { CLUSTER_TOPIC_RECREATED } = require('../../cluster/instrumentationEvents')
const { newLogger, waitFor } = require('testHelpers')

const TOPIC = 'test-topic'
const ID_A = '01020304-0506-0708-090a-0b0c0d0e0f10'
const ID_B = '11121314-1516-1718-191a-1b1c1d1e1f20'

/** A duck-typed logger fixture whose calls can be asserted on, mirroring crash.spec.js's. */
const createFakeLogger = () => {
  const logger = {
    error: jest.fn(),
    warn: jest.fn(),
    info: jest.fn(),
    debug: jest.fn(),
    namespace: () => logger,
  }
  return logger
}

/**
 * A stub Cluster with a real InstrumentationEventEmitter, so `onTopicRecreated` and the listener
 * bookkeeping under test are the real code paths.
 */
const createCluster = ({ topicIds = {} } = {}) => {
  const instrumentationEmitter = new InstrumentationEventEmitter()
  return {
    instrumentationEmitter,
    committedOffsetsByGroup: new Map(),
    connect: async () => {},
    disconnect: async () => {},
    metadata: async () => ({ topicMetadata: [] }),
    addMultipleTargetTopics: async () => {},
    refreshMetadata: async () => {},
    refreshMetadataIfNecessary: async () => {},
    // Backed by the map, exactly as Cluster#committedOffsets is, so a test can assert that a
    // topic key was (or was not) written into the group's shared offsets.
    committedOffsets({ groupId }) {
      if (!this.committedOffsetsByGroup.has(groupId)) {
        this.committedOffsetsByGroup.set(groupId, {})
      }
      return this.committedOffsetsByGroup.get(groupId)
    },
    defaultOffset: ({ fromBeginning }) => (fromBeginning ? -2 : -1),
    topicId: (topic) => topicIds[topic],
    onTopicRecreated: (handler) =>
      instrumentationEmitter.addListener(CLUSTER_TOPIC_RECREATED, handler),
    listenerCount: () => instrumentationEmitter.emitter.listenerCount(CLUSTER_TOPIC_RECREATED),
    emitRecreated: (payload) => instrumentationEmitter.emit(CLUSTER_TOPIC_RECREATED, payload),
  }
}

const build = (cluster, options = {}) =>
  createConsumer({
    cluster,
    groupId: 'test-group',
    logger: newLogger(),
    instrumentationEmitter: cluster.instrumentationEmitter,
    metadataMaxAge: 300000,
    retry: { retries: 0 },
    ...options,
  })

describe('Consumer > topic recreation', () => {
  describe('listener lifecycle', () => {
    test('the listener is registered at construction, before connect() or run()', () => {
      const cluster = createCluster()
      build(cluster)
      expect(cluster.listenerCount()).toEqual(1)
    })

    test('two connect() calls leave exactly one listener and one live unsubscribe', async () => {
      const cluster = createCluster()
      const consumer = build(cluster)
      await consumer.connect()
      await consumer.connect()
      expect(cluster.listenerCount()).toEqual(1)

      await consumer.disconnect()
      expect(cluster.listenerCount()).toEqual(0)
    })

    test('disconnect() removes the listener, including when cluster.disconnect() throws', async () => {
      const cluster = createCluster()
      cluster.disconnect = async () => {
        throw new Error('cluster teardown failed')
      }
      const consumer = build(cluster)
      await expect(consumer.disconnect()).rejects.toThrow('cluster teardown failed')
      expect(cluster.listenerCount()).toEqual(0)
    })

    test('a connect() or run() after disconnect() re-arms the listener', async () => {
      const cluster = createCluster()
      const consumer = build(cluster)
      await consumer.disconnect()
      expect(cluster.listenerCount()).toEqual(0)
      await consumer.connect()
      expect(cluster.listenerCount()).toEqual(1)

      await consumer.disconnect()
      expect(cluster.listenerCount()).toEqual(0)

      const start = jest.spyOn(Runner.prototype, 'start').mockResolvedValue()
      try {
        await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
        await consumer.run({ eachMessage: async () => {} })
        expect(cluster.listenerCount()).toEqual(1)
      } finally {
        start.mockRestore()
      }
    })
  })

  describe('recording a pending reset', () => {
    /**
     * Drives `subscribe()` + `run()` so a real `ConsumerGroup` exists, with no network at all:
     * `Runner#start` is the only thing stubbed, and `Runner` stores the group it was constructed
     * with (`runner.js:52`), so the test can reach both observable handles without production
     * growing a single accessor for it - a spy on `ConsumerGroup#invalidateTopic`, and the
     * `pendingTopicResets` Map the consumer passes into the `ConsumerGroup` constructor and the
     * group stores as `this.pendingTopicResets`.
     */
    const runConsumer = async (cluster) => {
      const start = jest.spyOn(Runner.prototype, 'start').mockResolvedValue()
      const invalidateTopic = jest.spyOn(ConsumerGroup.prototype, 'invalidateTopic')

      const consumer = build(cluster)
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      await consumer.run({ eachMessage: async () => {} })

      const runner = start.mock.instances[0]
      const consumerGroup = runner.consumerGroup

      return {
        consumer,
        consumerGroup,
        invalidateTopic,
        pendingTopicResets: consumerGroup.pendingTopicResets,
      }
    }

    afterEach(() => {
      jest.restoreAllMocks()
    })

    test('a subscribed topic is recorded, stamped with the new topic id', async () => {
      const cluster = createCluster()
      const { pendingTopicResets, invalidateTopic } = await runConsumer(cluster)

      cluster.emitRecreated({ topic: TOPIC, previousTopicId: ID_A, topicId: ID_B })

      expect(pendingTopicResets).toEqual(new Map([[TOPIC, ID_B]]))
      expect(invalidateTopic).toHaveBeenCalledWith(TOPIC)
    })

    test('a topic outside the current subscription is ignored: no key in the shared committed-offsets map and the pending map stays empty', async () => {
      const cluster = createCluster()
      const { pendingTopicResets, invalidateTopic } = await runConsumer(cluster)

      cluster.emitRecreated({ topic: 'someone-elses-topic', previousTopicId: ID_A, topicId: ID_B })

      expect(pendingTopicResets.size).toEqual(0)
      expect(invalidateTopic).not.toHaveBeenCalled()
      expect(cluster.committedOffsetsByGroup.get('test-group') || {}).not.toHaveProperty(
        'someone-elses-topic'
      )
    })

    test('an event before the first sync (offsetManager === null) is a no-op, not a throw', async () => {
      const cluster = createCluster()
      const consumer = build(cluster)
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      expect(() =>
        cluster.emitRecreated({ topic: TOPIC, previousTopicId: ID_A, topicId: ID_B })
      ).not.toThrow()
    })

    test('a consumer that calls only subscribe() and run(), never connect(), still performs the one-shot reset on TOPIC_RECREATED', async () => {
      // The headline case: connect() is optional, so a connect()-time subscription would leave
      // this shape with zero listeners and the reset silently inert.
      const cluster = createCluster({ topicIds: { [TOPIC]: ID_B } })
      const { pendingTopicResets } = await runConsumer(cluster)
      expect(cluster.listenerCount()).toEqual(1)

      cluster.emitRecreated({ topic: TOPIC, previousTopicId: ID_A, topicId: ID_B })
      expect(pendingTopicResets).toEqual(new Map([[TOPIC, ID_B]]))

      // …and the entry really is consumed: a sync's OffsetManager, built with that same Map,
      // ignores the coordinator's committed offset once and resolves per fromBeginning.
      const fetched = []
      cluster.fetchTopicsOffset = async (topics) => {
        fetched.push(topics)
        return topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({ partition, offset: '0' })),
        }))
      }

      const offsetManager = new OffsetManager({
        cluster,
        coordinator: {
          isConnected: () => true,
          offsetFetch: async () => ({
            responses: [
              {
                topic: TOPIC,
                partitions: [{ partition: 0, offset: '99', metadata: null, errorCode: 0 }],
              },
            ],
          }),
        },
        memberAssignment: { [TOPIC]: [0] },
        autoCommit: false,
        topicConfigurations: { [TOPIC]: { fromBeginning: true } },
        instrumentationEmitter: cluster.instrumentationEmitter,
        groupId: 'test-group',
        generationId: 0,
        memberId: 'm',
        pendingTopicResets,
      })

      await offsetManager.resolveOffsets()

      expect(fetched).toHaveLength(1)
      expect(fetched[0][0]).toMatchObject({ topic: TOPIC, fromBeginning: true })
      expect(offsetManager.committedOffsets()[TOPIC][0]).toEqual('0')
      expect(pendingTopicResets.size).toEqual(0)
    })
  })

  describe('ConsumerGroup#invalidateTopic', () => {
    const group = (memberAssignment) => {
      const cluster = createCluster()
      const consumerGroup = new ConsumerGroup({
        retry: { retries: 0 },
        cluster,
        groupId: 'test-group',
        topics: [TOPIC],
        topicConfigurations: { [TOPIC]: { fromBeginning: true } },
        logger: newLogger(),
        instrumentationEmitter: cluster.instrumentationEmitter,
        assigners: [],
        sessionTimeout: 30000,
        rebalanceTimeout: 60000,
        maxBytesPerPartition: 1048576,
        minBytes: 1,
        maxBytes: 1048576,
        maxWaitTimeInMs: 100,
        autoCommit: false,
        autoCommitInterval: null,
        autoCommitThreshold: null,
        isolationLevel: 0,
        rackId: '',
        checkCrcs: true,
        metadataMaxAge: 300000,
        pendingTopicResets: new Map(),
      })

      if (memberAssignment) {
        consumerGroup.offsetManager = new OffsetManager({
          cluster,
          coordinator: {},
          memberAssignment,
          autoCommit: false,
          topicConfigurations: { [TOPIC]: { fromBeginning: true } },
          instrumentationEmitter: cluster.instrumentationEmitter,
          groupId: 'test-group',
          generationId: 0,
          memberId: 'm',
          pendingTopicResets: new Map(),
        })
      }

      return consumerGroup
    }

    test('is a no-op while offsetManager is null', () => {
      expect(() => group(null).invalidateTopic(TOPIC)).not.toThrow()
    })

    test('is a no-op for a topic outside the current member assignment', () => {
      const consumerGroup = group({ 'other-topic': [0] })
      consumerGroup.offsetManager.committedOffsets()[TOPIC] = { 0: '99' }
      consumerGroup.invalidateTopic(TOPIC)
      expect(consumerGroup.offsetManager.committedOffsets()[TOPIC]).toEqual({ 0: '99' })
    })

    test('re-seeds rather than deletes, so a fetch immediately afterwards does not throw TypeError', () => {
      const consumerGroup = group({ [TOPIC]: [0, 1] })
      consumerGroup.offsetManager.committedOffsets()[TOPIC] = { 0: '99', 1: '99' }
      consumerGroup.offsetManager.resolvedOffsets[TOPIC] = { 0: '99', 1: '99' }

      consumerGroup.invalidateTopic(TOPIC)

      expect(consumerGroup.offsetManager.committedOffsets()[TOPIC]).toEqual({})
      expect(consumerGroup.offsetManager.resolvedOffsets[TOPIC]).toEqual({})
      expect(() => consumerGroup.offsetManager.nextOffset(TOPIC, 0)).not.toThrow()
    })

    test('clears preferredReadReplicas and pending seeks for the topic only', () => {
      const consumerGroup = group({ [TOPIC]: [0] })
      consumerGroup.preferredReadReplicasPerTopicPartition[TOPIC] = {
        0: { nodeId: 2, expireAt: Date.now() + 1000 },
      }
      consumerGroup.preferredReadReplicasPerTopicPartition.other = {
        0: { nodeId: 3, expireAt: Date.now() + 1000 },
      }
      consumerGroup.seekOffset.set(TOPIC, 0, '5')
      consumerGroup.seekOffset.set('other', 0, '5')

      consumerGroup.invalidateTopic(TOPIC)

      expect(consumerGroup.preferredReadReplicasPerTopicPartition[TOPIC]).toEqual({})
      expect(consumerGroup.preferredReadReplicasPerTopicPartition.other).not.toEqual({})
      expect(consumerGroup.seekOffset.has(TOPIC, 0)).toBe(false)
      expect(consumerGroup.seekOffset.has('other', 0)).toBe(true)
    })

    test('paused partitions stay paused', () => {
      const consumerGroup = group({ [TOPIC]: [0] })
      consumerGroup.subscriptionState.assign([{ topic: TOPIC, partitions: [0] }])
      consumerGroup.pause([{ topic: TOPIC, partitions: [0] }])
      consumerGroup.invalidateTopic(TOPIC)
      expect(consumerGroup.subscriptionState.isPaused(TOPIC, 0)).toBe(true)
    })
  })

  describe('OffsetManager#resolveOffsets drain', () => {
    const manager = ({ pendingTopicResets, topicIds = {}, committed = '99' }) => {
      const cluster = createCluster({ topicIds })
      const fetched = []
      cluster.fetchTopicsOffset = async (topics) => {
        fetched.push(topics)
        return topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({ partition, offset: '0' })),
        }))
      }

      const offsetManager = new OffsetManager({
        cluster,
        coordinator: {
          isConnected: () => true,
          offsetFetch: async () => ({
            responses: [
              {
                topic: TOPIC,
                partitions: [{ partition: 0, offset: committed, metadata: null, errorCode: 0 }],
              },
            ],
          }),
        },
        memberAssignment: { [TOPIC]: [0] },
        autoCommit: false,
        topicConfigurations: { [TOPIC]: { fromBeginning: true } },
        instrumentationEmitter: cluster.instrumentationEmitter,
        groupId: 'test-group',
        generationId: 0,
        memberId: 'm',
        pendingTopicResets,
      })

      return { offsetManager, fetched }
    }

    test('without a pending reset the coordinator offset is used', async () => {
      const { offsetManager, fetched } = manager({ pendingTopicResets: new Map() })
      await offsetManager.resolveOffsets()
      expect(fetched).toEqual([])
      expect(offsetManager.committedOffsets()[TOPIC][0]).toEqual('99')
    })

    test('resolveOffsets after a reset ignores the coordinator committed offset once and resolves per fromBeginning', async () => {
      const pendingTopicResets = new Map([[TOPIC, ID_B]])
      const { offsetManager, fetched } = manager({
        pendingTopicResets,
        topicIds: { [TOPIC]: ID_B },
      })

      await offsetManager.resolveOffsets()

      expect(fetched).toHaveLength(1)
      expect(fetched[0][0]).toMatchObject({ topic: TOPIC, fromBeginning: true })
      expect(offsetManager.committedOffsets()[TOPIC][0]).toEqual('0')
      expect(pendingTopicResets.size).toEqual(0)

      // one-shot: the next resolution is normal again
      offsetManager.committedOffsets()[TOPIC] = {}
      await offsetManager.resolveOffsets()
      expect(offsetManager.committedOffsets()[TOPIC][0]).toEqual('99')
    })

    test('a pending entry whose stamped topic id differs from a known cached id is dropped without resetting', async () => {
      const pendingTopicResets = new Map([[TOPIC, ID_A]])
      const { offsetManager, fetched } = manager({
        pendingTopicResets,
        topicIds: { [TOPIC]: ID_B },
      })

      await offsetManager.resolveOffsets()

      expect(fetched).toEqual([])
      expect(offsetManager.committedOffsets()[TOPIC][0]).toEqual('99')
      expect(pendingTopicResets.size).toEqual(0)
    })

    test('a rebootstrap between the recreation and the next resolveOffsets does not discard the pending reset', async () => {
      // The cache was cleared, so topicId(TOPIC) is undefined - unknown, not "changed".
      const pendingTopicResets = new Map([[TOPIC, ID_B]])
      const { offsetManager, fetched } = manager({ pendingTopicResets, topicIds: {} })

      await offsetManager.resolveOffsets()

      expect(fetched).toHaveLength(1)
      expect(offsetManager.committedOffsets()[TOPIC][0]).toEqual('0')
    })

    test('a subscribed-but-unassigned topic keeps its pending entry until a sync assigns it', async () => {
      const pendingTopicResets = new Map([['not-assigned', ID_B]])
      const { offsetManager } = manager({ pendingTopicResets, topicIds: { 'not-assigned': ID_B } })

      await offsetManager.resolveOffsets()

      expect(pendingTopicResets.get('not-assigned')).toEqual(ID_B)

      // …and is drained at the first resolveOffsets() after a sync assigns it - a fresh
      // OffsetManager, as a sync would build, sharing the same pendingTopicResets by reference.
      const cluster = createCluster({ topicIds: { 'not-assigned': ID_B } })
      const fetched = []
      cluster.fetchTopicsOffset = async (topics) => {
        fetched.push(topics)
        return topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({ partition, offset: '0' })),
        }))
      }

      const assignedManager = new OffsetManager({
        cluster,
        coordinator: {
          isConnected: () => true,
          offsetFetch: async () => ({
            responses: [
              {
                topic: 'not-assigned',
                partitions: [{ partition: 0, offset: '99', metadata: null, errorCode: 0 }],
              },
            ],
          }),
        },
        memberAssignment: { 'not-assigned': [0] },
        autoCommit: false,
        topicConfigurations: { 'not-assigned': { fromBeginning: true } },
        instrumentationEmitter: cluster.instrumentationEmitter,
        groupId: 'test-group',
        generationId: 0,
        memberId: 'm',
        pendingTopicResets,
      })

      await assignedManager.resolveOffsets()

      expect(fetched).toHaveLength(1)
      expect(assignedManager.committedOffsets()['not-assigned'][0]).toEqual('0')
      expect(pendingTopicResets.size).toEqual(0)
    })
  })

  test('a rejoin between invalidateTopic and the next fetch still resolves per fromBeginning', async () => {
    // pendingTopicResets is passed by reference into each ConsumerGroup and each OffsetManager,
    // so a rebuild of either cannot lose it.
    const pendingTopicResets = new Map([[TOPIC, ID_B]])
    const cluster = createCluster({ topicIds: { [TOPIC]: ID_B } })
    let fetchedFromBeginning = false
    cluster.fetchTopicsOffset = async (topics) => {
      fetchedFromBeginning = topics[0].fromBeginning === true
      return topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({ partition, offset: '0' })),
      }))
    }

    // A brand-new OffsetManager, as a sync would build - its constructor clears all offsets.
    const offsetManager = new OffsetManager({
      cluster,
      coordinator: {
        isConnected: () => true,
        offsetFetch: async () => ({
          responses: [
            {
              topic: TOPIC,
              partitions: [{ partition: 0, offset: '99', metadata: null, errorCode: 0 }],
            },
          ],
        }),
      },
      memberAssignment: { [TOPIC]: [0] },
      autoCommit: false,
      topicConfigurations: { [TOPIC]: { fromBeginning: true } },
      instrumentationEmitter: cluster.instrumentationEmitter,
      groupId: 'test-group',
      generationId: 0,
      memberId: 'm',
      pendingTopicResets,
    })

    await offsetManager.resolveOffsets()
    expect(fetchedFromBeginning).toBe(true)
    expect(offsetManager.committedOffsets()[TOPIC][0]).toEqual('0')
  })

  test('SeekOffsets#deleteTopic removes only that topic entries', () => {
    const seekOffsets = new SeekOffsets()
    seekOffsets.set(TOPIC, 0, '1')
    seekOffsets.set(TOPIC, 1, '2')
    seekOffsets.set('other', 0, '3')

    seekOffsets.deleteTopic(TOPIC)

    expect(seekOffsets.has(TOPIC, 0)).toBe(false)
    expect(seekOffsets.has(TOPIC, 1)).toBe(false)
    expect(seekOffsets.has('other', 0)).toBe(true)
  })

  describe('crash restart', () => {
    afterEach(() => {
      jest.restoreAllMocks()
    })

    test('after N crash-restarts the emitter still has exactly one cluster.topic_recreated listener, and a TOPIC_RECREATED after a restart still performs the one-shot reset', async () => {
      // onCrash calls disconnect() unconditionally, which removes the listener, then restarts
      // through start() alone - reaching neither run() nor connect(). Without the ensure call at
      // the top of start(), the listener the first crash removed never comes back and every
      // post-crash lifetime runs with the reset inert.
      const cluster = createCluster({ topicIds: { [TOPIC]: ID_B } })
      let starts = 0
      jest.spyOn(Runner.prototype, 'start').mockImplementation(async function () {
        starts += 1
        if (starts <= 3) {
          // A crash after the group exists: the runner's onCrash cleanup runs for real.
          this.onCrash(Object.assign(new Error('boom'), { retriable: true, retryTime: 1 }))
        }
      })
      const invalidateTopic = jest.spyOn(ConsumerGroup.prototype, 'invalidateTopic')

      const consumer = build(cluster, { retry: { retries: 0, initialRetryTime: 1 } })
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      await consumer.run({ eachMessage: async () => {} })

      await waitFor(() => starts === 4)
      expect(cluster.listenerCount()).toEqual(1)

      cluster.emitRecreated({ topic: TOPIC, previousTopicId: ID_A, topicId: ID_B })
      expect(invalidateTopic).toHaveBeenCalledWith(TOPIC)
      expect(Runner.prototype.start.mock.instances.at(-1).consumerGroup.pendingTopicResets).toEqual(
        new Map([[TOPIC, ID_B]])
      )
    })
  })

  describe('prototype-chain topic names', () => {
    afterEach(() => {
      jest.restoreAllMocks()
    })

    test('a recreation of a topic literally named "constructor", which this consumer is not subscribed to, is ignored', async () => {
      // `"constructor" in {}` is true and `{}.constructor` is truthy - both would defeat a plain
      // `in`/bracket-lookup guard even though this consumer never subscribed to "constructor".
      const start = jest.spyOn(Runner.prototype, 'start').mockResolvedValue()
      const invalidateTopic = jest.spyOn(ConsumerGroup.prototype, 'invalidateTopic')

      const cluster = createCluster({ topicIds: { constructor: ID_B } })
      const consumer = build(cluster)
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      await consumer.run({ eachMessage: async () => {} })

      cluster.emitRecreated({ topic: 'constructor', previousTopicId: ID_A, topicId: ID_B })

      const runner = start.mock.instances[0]
      expect(runner.consumerGroup.pendingTopicResets.size).toEqual(0)
      expect(invalidateTopic).not.toHaveBeenCalled()
      // `constructor` is inherited on every plain object, so `toHaveProperty` would report a
      // false positive here - `Object.hasOwn` is what actually proves nothing was written.
      expect(
        Object.hasOwn(cluster.committedOffsetsByGroup.get('test-group') || {}, 'constructor')
      ).toBe(false)
    })

    test('ConsumerGroup#invalidateTopic is a no-op for a topic literally named "constructor" outside the member assignment', () => {
      const cluster = createCluster()
      const consumerGroup = new ConsumerGroup({
        retry: { retries: 0 },
        cluster,
        groupId: 'test-group',
        topics: [TOPIC],
        topicConfigurations: { [TOPIC]: { fromBeginning: true } },
        logger: newLogger(),
        instrumentationEmitter: cluster.instrumentationEmitter,
        assigners: [],
        sessionTimeout: 30000,
        rebalanceTimeout: 60000,
        maxBytesPerPartition: 1048576,
        minBytes: 1,
        maxBytes: 1048576,
        maxWaitTimeInMs: 100,
        autoCommit: false,
        autoCommitInterval: null,
        autoCommitThreshold: null,
        isolationLevel: 0,
        rackId: '',
        checkCrcs: true,
        metadataMaxAge: 300000,
        pendingTopicResets: new Map(),
      })
      consumerGroup.offsetManager = new OffsetManager({
        cluster,
        coordinator: {},
        memberAssignment: { [TOPIC]: [0] },
        autoCommit: false,
        topicConfigurations: { [TOPIC]: { fromBeginning: true } },
        instrumentationEmitter: cluster.instrumentationEmitter,
        groupId: 'test-group',
        generationId: 0,
        memberId: 'm',
        pendingTopicResets: new Map(),
      })

      expect(() => consumerGroup.invalidateTopic('constructor')).not.toThrow()
      expect(
        Object.hasOwn(consumerGroup.preferredReadReplicasPerTopicPartition, 'constructor')
      ).toBe(false)
    })
  })

  describe('a throwing invalidateTopic does not escape the listener', () => {
    afterEach(() => {
      jest.restoreAllMocks()
    })

    test('is caught, logged at error level, and the synchronous emit() call itself does not throw', async () => {
      // The handler runs synchronously inside cluster.emit(), which in production is called from
      // BrokerPool#refreshMetadata - an uncaught throw here would surface as a metadata-refresh
      // failure and could itself contribute to a spurious rebootstrap.
      const start = jest.spyOn(Runner.prototype, 'start').mockResolvedValue()
      jest.spyOn(ConsumerGroup.prototype, 'invalidateTopic').mockImplementation(() => {
        throw new Error('boom')
      })

      const cluster = createCluster({ topicIds: { [TOPIC]: ID_B } })
      const logger = createFakeLogger()
      const consumer = build(cluster, { logger })
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      await consumer.run({ eachMessage: async () => {} })

      expect(() =>
        cluster.emitRecreated({ topic: TOPIC, previousTopicId: ID_A, topicId: ID_B })
      ).not.toThrow()

      expect(logger.error).toHaveBeenCalledWith(
        expect.stringContaining('Failed to handle topic recreation'),
        expect.objectContaining({ groupId: 'test-group' })
      )
      // The refresh "completes": the listener is still armed and the map was still updated
      // before the throw, so the reset itself is not lost even though invalidateTopic failed.
      expect(cluster.listenerCount()).toEqual(1)
      const runner = start.mock.instances[0]
      expect(runner.consumerGroup.pendingTopicResets).toEqual(new Map([[TOPIC, ID_B]]))
    })
  })

  describe('seek precedence around a topic recreation', () => {
    /**
     * A real ConsumerGroup + OffsetManager pair sharing one pendingTopicResets Map, so
     * consumerGroup.seek()/.invalidateTopic()/.seekOffsets() drive the actual precedence logic
     * under test rather than a re-implementation of it. The fake coordinator tracks the offset
     * it was last committed, exactly like a real broker: `offsetFetch` echoes back whatever
     * `offsetCommit` last stored, so an autoCommit:true seek is reflected in the very next fetch.
     */
    const buildGroup = ({ autoCommit, coordinatorOffset }) => {
      const cluster = createCluster({ topicIds: { [TOPIC]: ID_B } })
      const fetched = []
      cluster.fetchTopicsOffset = async (topics) => {
        fetched.push(topics)
        return topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({ partition, offset: '0' })),
        }))
      }

      let lastCommittedOffset = coordinatorOffset
      const offsetCommit = jest.fn(async ({ topics }) => {
        lastCommittedOffset = topics[0].partitions[0].offset
      })

      const consumerGroup = new ConsumerGroup({
        retry: { retries: 0 },
        cluster,
        groupId: 'test-group',
        topics: [TOPIC],
        topicConfigurations: { [TOPIC]: { fromBeginning: true } },
        logger: newLogger(),
        instrumentationEmitter: cluster.instrumentationEmitter,
        assigners: [],
        sessionTimeout: 30000,
        rebalanceTimeout: 60000,
        maxBytesPerPartition: 1048576,
        minBytes: 1,
        maxBytes: 1048576,
        maxWaitTimeInMs: 100,
        autoCommit,
        autoCommitInterval: null,
        autoCommitThreshold: null,
        isolationLevel: 0,
        rackId: '',
        checkCrcs: true,
        metadataMaxAge: 300000,
      })

      consumerGroup.offsetManager = new OffsetManager({
        cluster,
        coordinator: {
          isConnected: () => true,
          offsetFetch: async () => ({
            responses: [
              {
                topic: TOPIC,
                partitions: [
                  { partition: 0, offset: lastCommittedOffset, metadata: null, errorCode: 0 },
                ],
              },
            ],
          }),
          offsetCommit,
        },
        memberAssignment: { [TOPIC]: [0] },
        autoCommit,
        topicConfigurations: { [TOPIC]: { fromBeginning: true } },
        instrumentationEmitter: cluster.instrumentationEmitter,
        groupId: 'test-group',
        generationId: 0,
        memberId: 'm',
        pendingTopicResets: consumerGroup.pendingTopicResets,
      })

      return { consumerGroup, fetched, offsetCommit }
    }

    test.each([false, true])(
      'a seek issued before the recreation is dropped by the reset (autoCommit: %s)',
      async (autoCommit) => {
        const { consumerGroup, fetched } = buildGroup({ autoCommit, coordinatorOffset: '99' })

        consumerGroup.seek({ topic: TOPIC, partition: 0, offset: '500' })
        expect(consumerGroup.hasSeekOffset({ topic: TOPIC, partition: 0 })).toBe(true)

        // The recreation: the Consumer listener stamps the map, then calls invalidateTopic().
        consumerGroup.pendingTopicResets.set(TOPIC, ID_B)
        consumerGroup.invalidateTopic(TOPIC)
        expect(consumerGroup.hasSeekOffset({ topic: TOPIC, partition: 0 })).toBe(false)

        await consumerGroup.seekOffsets([{ topic: TOPIC, partitions: [0] }])

        expect(fetched).toHaveLength(1)
        expect(consumerGroup.offsetManager.committedOffsets()[TOPIC][0]).toEqual('0')
      }
    )

    test.each([false, true])(
      'a seek issued after the recreation wins over the reset for that partition (autoCommit: %s)',
      async (autoCommit) => {
        const { consumerGroup, fetched, offsetCommit } = buildGroup({
          autoCommit,
          coordinatorOffset: '99',
        })

        consumerGroup.pendingTopicResets.set(TOPIC, ID_B)
        consumerGroup.invalidateTopic(TOPIC)
        consumerGroup.seek({ topic: TOPIC, partition: 0, offset: '500' })

        await consumerGroup.seekOffsets([{ topic: TOPIC, partitions: [0] }])

        expect(fetched).toEqual([])
        if (autoCommit) {
          expect(offsetCommit).toHaveBeenCalled()
        }
        // Either way the seek's target offset is what the next fetch resumes from, not the '0'
        // a fromBeginning reset would have produced.
        expect(consumerGroup.offsetManager.nextOffset(TOPIC, 0).toString()).toEqual('500')
      }
    )
  })
})
