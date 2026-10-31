const { Kafka } = require('../../index')
const Cluster = require('../../src/cluster')
const { MemberMetadata } = require('../../src/consumer/assignerProtocol')
const Runner = require('../../src/consumer/runner')
const ConsumerGroupProtocol = require('../../src/consumer/groupProtocol/consumer')
const InstrumentationEventEmitter = require('../../src/instrumentation/emitter')
const { MEMBER_STATES } = require('../../src/consumer/groupProtocol/memberState')
const { createScriptedCluster, groupScript, consumerProtocolScript } = require('../scriptedCluster')
const { metadataV13 } = require('../wire/expected')
const { newLogger } = require('../index')
const {
  createFetchGroup,
  prepareRecordBatches,
  fetchResponse,
  NODE_ID,
  TOPIC,
  TOPIC_ID,
} = require('../../src/consumer/__tests__/fixtures/fetchHarness')

const NODES = [{ id: 0, host: 'node0', port: 9092 }]
const UNKNOWN_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'

/** @returns {Promise<void>} yields one event-loop turn without arming a timer. */
const turn = () => new Promise(setImmediate)

/**
 * Builds real cluster transport on the scripted endpoints.
 * @param {object} seam the scripted cluster
 * @returns {Cluster} the client-side cluster
 */
const transport = (seam) =>
  new Cluster({
    ...seam.kafkaOptions(),
    logger: newLogger(),
    retry: { retries: 0 },
    metadataMaxAge: 300000,
    allowAutoTopicCreation: true,
  })

/**
 * Builds a membership whose subscription is already established; it starts no timer.
 * @param {Cluster} cluster the client-side cluster
 * @returns {ConsumerGroupProtocol} the membership
 */
const membership = (cluster) =>
  new ConsumerGroupProtocol({
    cluster,
    groupId: 'consumer-regressions',
    rebalanceTimeout: 300000,
    topics: [TOPIC],
    logger: newLogger(),
    instrumentationEmitter: new InstrumentationEventEmitter(),
  })

describe('consumer regression fixtures', () => {
  beforeAll(prepareRecordBatches)

  describe('paused fetch wait', () => {
    let h, observed, pending
    beforeEach(async () => {
      h = await createFetchGroup()
      h.group.maxWaitTime = 400
      jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick'] })
      h.group.pause([{ topic: TOPIC, partitions: [0] }])
      let settled = false
      pending = h.group.fetch(NODE_ID).then((batches) => {
        settled = true
        return batches
      })
      await jest.advanceTimersByTimeAsync(0)
      expect(jest.getTimerCount()).toBe(1)
      await jest.advanceTimersByTimeAsync(50)
      h.group.resume([{ topic: TOPIC, partitions: [0] }])
      await jest.advanceTimersByTimeAsync(0)
      observed = settled
    })
    afterEach(async () => {
      if (pending) {
        await jest.advanceTimersByTimeAsync(400)
        await pending
      }
      jest.useRealTimers()
      if (h) await h.broker.disconnect()
    })
    it('resuming a paused partition ends the current idle wait', () => {
      expect(observed).toBe(true)
    })
  })

  describe('batch-end commit cadence', () => {
    let h, runner, observed
    beforeEach(async () => {
      h = await createFetchGroup({ autoCommit: true })
      await h.sync()
      h.group.groupMembership.coordinator.heartbeat = async () => {}
      h.group.offsetManager.autoCommitInterval = 60000
      h.group.offsetManager.autoCommitThreshold = null
      h.group.offsetManager.lastCommit = Date.now()
      h.group.offsetManager.committedOffsets()[TOPIC][0] = '0'
      h.setResponse(fetchResponse([{ partition: 0, errorCode: 0, records: true }]))
      const batches = await h.group.fetch(NODE_ID)
      expect(batches).toHaveLength(1)
      expect(batches[0].messages.map(({ offset }) => offset)).toEqual(['0'])
      const processed = jest.fn(async () => {})
      runner = new Runner({
        logger: newLogger(),
        consumerGroup: h.group,
        instrumentationEmitter: new InstrumentationEventEmitter(),
        concurrency: 1,
        eachBatch: processed,
        heartbeatInterval: 3000,
        maxPollInterval: 300000,
        autoCommit: true,
        onCrash: (error) => {
          throw error
        },
      })
      runner.running = true
      const commits = jest.spyOn(h.group.offsetManager, 'sendOffsetCommit')
      await runner.handleBatch(batches[0])
      expect(processed).toHaveBeenCalledTimes(1)
      expect(h.group.offsetManager.resolvedOffsets[TOPIC][0]).toBe('1')
      observed = commits.mock.calls.length
    })
    afterEach(async () => {
      if (runner) {
        runner.running = false
        runner.heartbeatManager.shutdown()
        runner.pollTimer.stop()
      }
      if (h) await h.broker.disconnect()
    })
    it('batch completion preserves a configured commit interval', () => {
      expect(observed).toBe(0)
    })
  })

  describe.each(['g', 'y', 'gy'])('pattern subscription with %s flags', (flags) => {
    let seam, consumer, observed, expression, originalIndex
    const names = ['my-topic-1', 'my-topic-2', 'my-topic-3', 'my-topic-4']
    beforeEach(async () => {
      seam = groupScript(createScriptedCluster({ nodes: NODES }))
      seam.answer('Metadata', () => ({
        ...metadataV13.response,
        brokers: [{ nodeId: 0, host: 'node0', port: 9092, rack: null }],
        controllerId: 0,
        topics: names.map((name, index) => ({
          ...metadataV13.response.topics[0],
          name,
          topicId: `01020304-0506-0708-090a-0b0c0d0e0f1${index}`,
          partitions: [],
        })),
      }))
      consumer = new Kafka(seam.kafkaOptions()).consumer({
        groupId: 'pattern-consumer',
        maxWaitTimeInMs: 10,
      })
      const crashes = []
      consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload.error))
      await consumer.connect()
      expression = new RegExp('topic-', flags)
      expression.lastIndex = 1
      originalIndex = expression.lastIndex
      await consumer.subscribe({ topics: [expression] })
      await consumer.run({ autoCommit: false, eachMessage: async () => {} })
      await turn()
      expect(crashes).toEqual([])
      const join = seam.requests().find(({ api }) => api === 'JoinGroup')
      expect(join).toBeDefined()
      observed = MemberMetadata.decode(
        Buffer.from(join.fields.groupProtocols[0].metadata, 'hex')
      ).topics.sort()
      seam.assertClean()
    })
    afterEach(async () => {
      if (consumer) await consumer.disconnect()
      if (seam) seam.assertClean()
    })
    it('matches every partial topic name and preserves the caller expression', () => {
      expect({ topics: observed, callerIndex: expression.lastIndex }).toEqual({
        topics: names,
        callerIndex: originalIndex,
      })
    })
  })

  describe.each([null, 'static-member'])('leave heartbeat with instance %p', (instanceId) => {
    let seam, cluster, protocol, observed
    beforeEach(async () => {
      seam = consumerProtocolScript(createScriptedCluster({ nodes: NODES, profile: 'consumer' }))
      cluster = transport(seam)
      await cluster.connect()
      protocol = membership(cluster)
      protocol.groupInstanceId = instanceId
      await protocol.connect()
      protocol.memberEpoch = 1
      protocol.state = MEMBER_STATES.PREPARE_LEAVING
      await protocol.sendLeaveHeartbeat()
      const leaves = seam.requests().filter(({ api }) => api === 'ConsumerGroupHeartbeat')
      expect(leaves).toHaveLength(1)
      const request = leaves[0].fields
      expect(request.groupId).toBe('consumer-regressions')
      expect(request.memberId).toBe(protocol.memberId)
      expect(request.memberEpoch).toBe(instanceId ? -2 : -1)
      expect(request.instanceId).toBe(instanceId)
      observed = {
        rebalanceTimeoutMs: request.rebalanceTimeoutMs,
        subscribedTopicNames: request.subscribedTopicNames,
        subscribedTopicRegex: request.subscribedTopicRegex,
        serverAssignor: request.serverAssignor,
        topicPartitions: request.topicPartitions,
        rackId: request.rackId,
      }
      seam.assertClean()
    })
    afterEach(async () => {
      if (cluster) await cluster.disconnect()
      if (seam) seam.assertClean()
    })
    it('sends only identity and no-update fields when leaving the group', () => {
      expect(observed).toEqual({
        rebalanceTimeoutMs: -1,
        subscribedTopicNames: null,
        subscribedTopicRegex: null,
        serverAssignor: null,
        topicPartitions: null,
        rackId: null,
      })
    })
  })

  describe('healthy sibling during leader election', () => {
    let h, observed
    beforeEach(async () => {
      h = await createFetchGroup({ partitions: [0, 1], partitionLeaders: { 0: -1, 1: NODE_ID } })
      h.setResponse(fetchResponse([{ partition: 1, errorCode: 0, records: true }]))
      const batches = await h.group.fetch(NODE_ID)
      for (const [{ error }] of h.requestRejoin.mock.calls) {
        if (error?.type !== 'LEADER_NOT_AVAILABLE' || error.code !== 5) throw error
      }
      observed = {
        partitions: batches.map(({ partition }) => partition),
        fetches: h.requests().filter(({ apiKey }) => apiKey === 1).length,
        rejoins: h.requestRejoin.mock.calls.length,
      }
    })
    afterEach(async () => {
      if (h) await h.broker.disconnect()
    })
    it('fetches a healthy sibling without a routing-induced rejoin', () => {
      expect(observed).toEqual({ partitions: [1], fetches: 1, rejoins: 0 })
    })
  })

  describe('converged named subset with an unresolved target', () => {
    let seam, cluster, protocol, observed
    beforeEach(async () => {
      seam = consumerProtocolScript(createScriptedCluster({ nodes: NODES, profile: 'consumer' }))
      cluster = transport(seam)
      await cluster.connect()
      await cluster.addMultipleTargetTopics([TOPIC])
      expect(cluster.topicName(TOPIC_ID)).toBe(TOPIC)
      protocol = membership(cluster)
      protocol.state = MEMBER_STATES.RECONCILING
      protocol.memberEpoch = 1
      protocol.target = {
        localEpoch: 1,
        byTopicId: new Map([
          [TOPIC_ID, [0]],
          [UNKNOWN_ID, [0]],
        ]),
      }
      protocol.localAssignment = { localEpoch: 1, byTopicId: new Map([[TOPIC_ID, [0]]]) }
      protocol.ownedPartitions = [{ topic: TOPIC, partitions: [0] }]
      protocol.assignedTopicNames.set(TOPIC_ID, TOPIC)
      protocol.lastMetadataRefreshAt = Date.now()
      expect(protocol.recordResolution(protocol.resolveFromCache(protocol.target))).toBe(false)
      expect(protocol.matchesOwned(protocol.resolveFromCache(protocol.target))).toBe(true)
      const refresh = jest.spyOn(cluster, 'refreshMetadataForTopicIds')
      const before = seam.requests().length
      protocol.evaluateTarget({ targetChanged: false })
      for (let index = 0; index < 8; index++) await turn()
      for (const result of refresh.mock.results) await result.value
      const naming = seam
        .requests()
        .slice(before)
        .filter(
          ({ api, fields }) =>
            api === 'Metadata' && fields.topics?.some(({ topicId }) => topicId === UNKNOWN_ID)
        )
      if (refresh.mock.calls.length > 0) expect(naming).toHaveLength(1)
      observed = refresh.mock.calls.length
      seam.assertClean()
    })
    afterEach(async () => {
      if (cluster) await cluster.disconnect()
      if (seam) seam.assertClean()
    })
    it('immediately names a stranded target when both change flags are false', () => {
      expect(observed).toBe(1)
    })
  })
})

describe('independent group offset identity', () => {
  let seam, cluster, observed
  beforeEach(async () => {
    seam = groupScript(createScriptedCluster({ nodes: NODES }))
    seam.answer('OffsetFetch', () => ({
      groupId: 'returned-group',
      topics: [
        {
          topic: TOPIC,
          partitions: [
            {
              partition: 0,
              offset: '7',
              leaderEpoch: -1,
              metadata: 'from-other-group',
              errorCode: 0,
            },
          ],
        },
      ],
    }))
    cluster = transport(seam)
    await cluster.connect()
    const broker = await cluster.findGroupCoordinator({ groupId: 'requested-group' })
    try {
      await broker.offsetFetch({
        groupId: 'requested-group',
        topics: [{ topic: TOPIC, partitions: [{ partition: 0 }] }],
        requireStable: true,
      })
      observed = null
    } catch (error) {
      const { KafkaJSInvariantViolation } = require('../../src/errors')
      if (
        !(error instanceof KafkaJSInvariantViolation) ||
        !error.message.includes('requested-group') ||
        !error.message.includes('returned-group')
      )
        throw error
      observed = error
    }
    expect(seam.requests().filter(({ api }) => api === 'OffsetFetch')).toHaveLength(1)
    seam.assertClean()
  })
  afterEach(async () => {
    if (cluster) await cluster.disconnect()
    if (seam) seam.assertClean()
  })
  it('refuses offsets returned for a different group', () => {
    expect(observed).toEqual(
      expect.objectContaining({
        name: 'KafkaJSInvariantViolation',
        message: expect.stringMatching(/requested-group.*returned-group/),
      })
    )
  })
})

describe('offline preferred read replica', () => {
  let h, observed
  beforeEach(async () => {
    h = await createFetchGroup({ partitions: [0], preferredReadReplica: 1, knownNodes: [0, 1] })
    h.cluster.findTopicPartitionMetadata = () => [
      { partitionId: 0, leader: 0, offlineReplicas: [1] },
    ]
    observed = h.group.findReadReplicaForPartitions(TOPIC, [0])
  })
  afterEach(async () => {
    if (h) await h.broker.disconnect()
  })
  it('uses the leader when the preferred replica is offline', () => {
    expect(observed).toEqual({ 0: [0] })
  })
})
