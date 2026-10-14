const createProducer = require('../../producer')
const createAdmin = require('../../admin')
const createConsumer = require('../../consumer')
const Runner = require('../../consumer/runner')
const ClassicGroupProtocol = require('../../consumer/groupProtocol/classic')
const Batch = require('../../consumer/batch')
const OffsetManager = require('../../consumer/offsetManager')
const MetadataCache = require('../../cluster/metadataCache')
const Lock = require('../../utils/lock')
const { createFakeCluster } = require('../../producer/__tests__/fixtures/fakeCluster')
const { KafkaJSLockTimeout } = require('../../errors')
const { newLogger, waitFor } = require('testHelpers')

/**
 * An emitter that records what the code under test emits, under the internal name it emits it
 * with, and delivers it to nobody.
 */
const createRecorder = () => {
  const events = []
  return {
    events,
    emit: (name, payload) => events.push({ name, payload }),
    addListener: () => () => {},
  }
}

/** The recorded events with this name, in the order they were emitted. */
const named = (recorder, name) => recorder.events.filter((event) => event.name === name)

/** The keys of a payload, sorted: what a listener can rely on being there. */
const keysOf = ({ payload }) => Object.keys(payload).sort()

describe('Instrumentation > where the events are emitted', () => {
  describe('the producer', () => {
    it('emits connect, initialized and disconnect, each without a payload', async () => {
      const recorder = createRecorder()
      const producer = createProducer({
        cluster: createFakeCluster(),
        logger: newLogger(),
        instrumentationEmitter: recorder,
      })

      await producer.connect()
      await producer.disconnect()

      expect(recorder.events).toEqual([
        { name: 'producer.connect', payload: undefined },
        { name: 'producer.initialized', payload: undefined },
        { name: 'producer.disconnect', payload: undefined },
      ])
    })
  })

  describe('the admin client', () => {
    it('emits connect and disconnect, each without a payload', async () => {
      const recorder = createRecorder()
      const admin = createAdmin({
        cluster: { connect: async () => {}, disconnect: async () => {} },
        logger: newLogger(),
        instrumentationEmitter: recorder,
      })

      await admin.connect()
      await admin.disconnect()

      expect(recorder.events).toEqual([
        { name: 'admin.connect', payload: undefined },
        { name: 'admin.disconnect', payload: undefined },
      ])
    })
  })
  describe('the consumer', () => {
    /** A cluster that connects and then has nothing else a consumer needs, so its runner crashes. */
    const createBareCluster = () => ({
      connect: async () => {},
      disconnect: async () => {},
      refreshMetadataIfNecessary: async () => {},
      removeBroker: () => {},
      isConnected: () => true,
      addMultipleTargetTopics: async () => {},
      onTopicRecreated: () => () => {},
      retry: {},
    })

    it('emits connect, stop, disconnect and crash, with the crash carrying its error, group and decision', async () => {
      const recorder = createRecorder()
      const consumer = createConsumer({
        cluster: createBareCluster(),
        groupId: 'call-site-group',
        logger: newLogger(),
        instrumentationEmitter: recorder,
        retry: { retries: 0, restartOnFailure: async () => false },
      })

      await consumer.connect()
      await consumer.subscribe({ topic: 'call-site-topic' })
      await consumer.run({ eachMessage: async () => {} })
      await waitFor(() => named(recorder, 'consumer.crash').length === 1, { maxWait: 5000 })

      // the client's own connect, then the group's, which a run makes before it joins
      expect(named(recorder, 'consumer.connect')).toEqual([
        { name: 'consumer.connect', payload: undefined },
        { name: 'consumer.connect', payload: undefined },
      ])
      expect(named(recorder, 'consumer.stop')).toEqual([
        { name: 'consumer.stop', payload: undefined },
      ])
      expect(named(recorder, 'consumer.disconnect')).toEqual([
        { name: 'consumer.disconnect', payload: undefined },
      ])
      const [crash] = named(recorder, 'consumer.crash')
      expect(keysOf(crash)).toEqual(['error', 'groupId', 'restart'])
      expect(crash.payload).toMatchObject({ groupId: 'call-site-group', restart: false })
      expect(crash.payload.error).toBeInstanceOf(Error)
    })
  })

  describe('the lock', () => {
    it('emits lock_timeout with the holder, the number waiting and the timeout', async () => {
      const recorder = createRecorder()
      const lock = new Lock({
        timeout: 20,
        description: 'broker-1',
        instrumentationEmitter: recorder,
      })
      await lock.acquire()

      await expect(lock.acquire()).rejects.toThrow(KafkaJSLockTimeout)

      expect(recorder.events).toEqual([
        {
          name: 'network.lock_timeout',
          payload: { description: 'broker-1', waiting: 1, timeout: 20 },
        },
      ])
    })
  })

  describe('the metadata cache', () => {
    const ID_A = '3b1b4bd4-b6f1-4a7f-9d4f-2f4d1d5a0001'
    const ID_B = '3b1b4bd4-b6f1-4a7f-9d4f-2f4d1d5a0002'
    const response = (topicId) => ({
      brokers: [{ nodeId: 1, host: 'localhost', port: 9092 }],
      topicMetadata: [
        {
          topic: 'recreated-topic',
          topicId,
          partitionMetadata: [
            {
              partitionErrorCode: 0,
              partitionId: 0,
              leader: 1,
              leaderEpoch: 0,
              replicas: [1],
              isr: [1],
              offlineReplicas: [],
            },
          ],
        },
      ],
    })

    it.each([
      ['a full response', (cache, metadata) => cache.update(metadata, { full: true })],
      ['a response for some topics', (cache, metadata) => cache.updateTopicsById(metadata)],
    ])('emits topic_recreated with the topic and both ids when %s brings a new id', (_, apply) => {
      const recorder = createRecorder()
      const cache = new MetadataCache({ logger: newLogger(), instrumentationEmitter: recorder })
      cache.update(response(ID_A), { full: true })

      apply(cache, response(ID_B))

      expect(recorder.events).toEqual([
        {
          name: 'cluster.topic_recreated',
          payload: { topic: 'recreated-topic', previousTopicId: ID_A, topicId: ID_B },
        },
      ])
    })
  })

  describe('the offset manager', () => {
    it('emits commit_offsets with the group, the member, the generation and what was committed', async () => {
      const recorder = createRecorder()
      const memberAssignment = { 'committed-topic': [0] }
      const offsetManager = new OffsetManager({
        cluster: {
          committedOffsets: jest.fn(() => ({})),
          refreshMetadata: jest.fn(() => ({})),
          findGroupCoordinator: jest.fn(async () => ({ isConnected: () => true, nodeId: 2 })),
        },
        memberAssignment,
        groupId: 'commit-group',
        generationId: 3,
        memberId: 'member-1',
        groupInstanceId: 'instance-1',
        instrumentationEmitter: recorder,
      })
      offsetManager.getCoordinator = jest.fn(() => ({ offsetCommit: jest.fn() }))
      offsetManager.resolveOffset({ topic: 'committed-topic', partition: 0, offset: '4' })

      await offsetManager.commitOffsets()

      const [commit] = named(recorder, 'consumer.commit_offsets')
      expect(recorder.events).toHaveLength(1)
      expect(keysOf(commit)).toEqual([
        'groupGenerationId',
        'groupId',
        'groupInstanceId',
        'memberId',
        'topics',
      ])
      expect(commit.payload).toMatchObject({
        groupId: 'commit-group',
        memberId: 'member-1',
        groupGenerationId: 3,
        groupInstanceId: 'instance-1',
      })
      expect(commit.payload.topics).toEqual([
        {
          topic: 'committed-topic',
          partitions: [{ partition: '0', offset: '5', leaderEpoch: -1 }],
        },
      ])
    })

    it('emits the offsets a caller commits as given, with no leaderEpoch added', async () => {
      const recorder = createRecorder()
      const offsetManager = new OffsetManager({
        cluster: {
          committedOffsets: jest.fn(() => ({})),
          refreshMetadata: jest.fn(() => ({})),
          findGroupCoordinator: jest.fn(async () => ({ isConnected: () => true, nodeId: 2 })),
        },
        memberAssignment: { 'committed-topic': [0] },
        groupId: 'commit-group',
        generationId: 3,
        memberId: 'member-1',
        groupInstanceId: null,
        instrumentationEmitter: recorder,
      })
      offsetManager.getCoordinator = jest.fn(() => ({ offsetCommit: jest.fn() }))
      const topics = [{ topic: 'committed-topic', partitions: [{ partition: 0, offset: '5' }] }]

      await offsetManager.commitOffsets({ topics })

      const [commit] = named(recorder, 'consumer.commit_offsets')
      expect(commit.payload.topics).toStrictEqual([
        { topic: 'committed-topic', partitions: [{ partition: 0, offset: '5' }] },
      ])
      expect(commit.payload.topics[0].partitions[0]).not.toHaveProperty('leaderEpoch')
    })
  })
  describe('the runner', () => {
    const TOPIC = 'runner-topic'
    let recorder
    let runner
    let consumerGroup

    beforeEach(() => {
      recorder = createRecorder()
      consumerGroup = {
        groupId: 'runner-group',
        memberId: 'runner-member',
        generationId: 1,
        leaderId: 'runner-member',
        groupInstanceId: null,
        groupMembership: {},
        assignmentEpoch: 0,
        attach: jest.fn(),
        rebalanceProtocol: jest.fn(() => 'eager'),
        applyRebalanceResult: jest.fn(async () => ({ assignment: [], added: [], revoked: [] })),
        discardUncommittedOffsets: jest.fn(),
        clearStaleAssignmentPending: jest.fn(),
        forgetOwnedPartitions: jest.fn(),
        clearAssignerState: jest.fn(),
        resetGenerationLocally: jest.fn(),
        hasSeekOffset: () => false,
        isAssigned: () => true,
        leave: jest.fn(async () => {}),
        getNodeIds: jest.fn(() => [1]),
        connect: jest.fn(),
        joinAndSync: jest.fn(async () => ({ assignment: {} })),
        fetch: jest.fn(async () => []),
        resolveOffset: jest.fn(),
        commitOffsets: jest.fn(),
        commitOffsetsIfNecessary: jest.fn(),
        uncommittedOffsets: jest.fn(),
        heartbeat: jest.fn(),
        assigned: jest.fn(() => []),
        isLeader: jest.fn(() => true),
        isPaused: jest.fn().mockReturnValue(false),
        kind: 'classic',
        heartbeatInterval: jest.fn(() => 3000),
        onStopRequested: jest.fn(),
        onMembershipError: jest.fn(),
        leaveForPollExpiry: jest.fn(async () => {}),
        needsReconciliation: jest.fn(() => false),
        resolvedOffsetAt: jest.fn(() => undefined),
        pendingTopicResets: new Map(),
      }
      runner = new Runner({
        consumerGroup,
        onCrash: jest.fn(),
        instrumentationEmitter: recorder,
        logger: newLogger(),
        eachBatch: jest.fn(),
        concurrency: 1,
        heartbeatInterval: 3000,
        maxPollInterval: 300000,
      })
      runner.scheduleFetchManager = jest.fn()
    })

    afterEach(async () => {
      runner.running = true
      await runner.stop()
    })

    it('emits fetch_start and fetch around a fetch, each naming the broker', async () => {
      await runner.start()
      recorder.events.length = 0

      await runner.fetch(1)

      expect(recorder.events).toEqual([
        { name: 'consumer.fetch_start', payload: { nodeId: 1 } },
        {
          name: 'consumer.fetch',
          payload: { numberOfBatches: 0, duration: expect.any(Number), nodeId: 1 },
        },
      ])
    })

    it('emits start_batch_process and end_batch_process around the handler, the end adding the duration', async () => {
      const batch = new Batch(TOPIC, 0, {
        partition: 0,
        highWatermark: 5,
        messages: [{ offset: '4', key: '1', value: '2' }],
      })
      await runner.start()
      recorder.events.length = 0

      await runner.handleBatch(batch)

      const [start] = named(recorder, 'consumer.start_batch_process')
      const [end] = named(recorder, 'consumer.end_batch_process')
      expect(keysOf(start)).toEqual([
        'batchSize',
        'firstOffset',
        'highWatermark',
        'lastOffset',
        'offsetLag',
        'offsetLagLow',
        'partition',
        'topic',
      ])
      expect(start.payload).toMatchObject({
        topic: TOPIC,
        partition: 0,
        batchSize: 1,
        firstOffset: '4',
        lastOffset: '4',
      })
      expect(end.payload).toEqual({ ...start.payload, duration: expect.any(Number) })
      expect(recorder.events.map(({ name }) => name)).toEqual([
        'consumer.start_batch_process',
        'consumer.end_batch_process',
      ])
    })

    it('emits both batch events for a batch of nothing but control records, so offset listeners see it', async () => {
      const batch = new Batch(TOPIC, 0, {
        partition: 0,
        highWatermark: 5,
        messages: [{ offset: '4', key: null, value: null, isControlRecord: true }],
      })
      await runner.start()
      recorder.events.length = 0

      await runner.handleBatch(batch)

      const [start] = named(recorder, 'consumer.start_batch_process')
      const [end] = named(recorder, 'consumer.end_batch_process')
      expect(start.payload).toMatchObject({ topic: TOPIC, partition: 0, batchSize: 0 })
      expect(end.payload).toEqual({ ...start.payload, duration: expect.any(Number) })
    })
  })
  describe('the classic group protocol', () => {
    it('emits heartbeat with the group, member, generation and instance id the heartbeat was sent with', async () => {
      const recorder = createRecorder()
      const protocol = new ClassicGroupProtocol({
        cluster: {},
        groupId: 'heartbeat-group',
        groupInstanceId: 'instance-1',
        sessionTimeout: 30000,
        rebalanceTimeout: 60000,
        assigners: [],
        instrumentationEmitter: recorder,
        logger: newLogger(),
      })
      protocol.memberId = 'member-1'
      protocol.generationId = 3
      protocol.coordinator = { heartbeat: jest.fn(async () => ({})) }
      const payload = {
        groupId: 'heartbeat-group',
        memberId: 'member-1',
        groupGenerationId: 3,
        groupInstanceId: 'instance-1',
      }

      await protocol.heartbeat({ interval: 0 })

      expect(protocol.coordinator.heartbeat).toHaveBeenCalledWith(payload)
      expect(recorder.events).toEqual([{ name: 'consumer.heartbeat', payload }])
    })
  })
})
