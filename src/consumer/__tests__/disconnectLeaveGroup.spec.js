const createConsumer = require('../index')
const Runner = require('../runner')
const ConsumerGroup = require('../consumerGroup')
const ClassicGroupProtocol = require('../groupProtocol/classic')
const { ConsumerOperation } = require('../groupProtocol/interface')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const OffsetOperations = require('../offsetManager/offsetOperations')
const leaveGroupV5 = require('../../protocol/requests/leaveGroup/v5/request')
const apiKeys = require('../../protocol/requests/apiKeys')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const { events } = require('../instrumentationEvents')
const { MEMBER_STATES } = require('../groupProtocol/memberState')
const { KafkaJSNonRetriableError } = require('../../errors')
const { newLogger, waitFor } = require('testHelpers')

const GROUP_ID = 'disconnect-leave-group'
const TOPIC = 'topic-a'
const TOPIC_ID = 'kOJl1vDcS4Ct5xLg5rGqaQ'
const INSTANCE_ID = 'instance-1'
const MEMBER_ID = 'member-1'
const LEAVE_REASON = 'the consumer is being closed'
const OWNED = [{ topic: TOPIC, partitions: [0, 1] }]
const LEAVE_GROUP_OK = require('../../protocol/requests/leaveGroup/fixtures/java/leaveGroupResponse-v5-ok.json')

const heartbeatAnswer = {
  throttleTime: 0,
  errorCode: 0,
  errorMessage: null,
  memberId: MEMBER_ID,
  memberEpoch: 3,
  heartbeatIntervalMs: 5000,
  assignment: { topicPartitions: [{ topicId: TOPIC_ID, partitions: [0, 1] }] },
}

/** An assigner named as the classic coordinator double reports the group's protocol. */
const assigner = () => ({
  name: 'a',
  version: 0,
  protocol: () => ({ name: 'a', metadata: Buffer.alloc(0) }),
  assign: async () => [],
})

/** Every consumer a case builds, disconnected after it so no heartbeat timer outlives the case. */
const built = []

/**
 * A consumer on `protocol` over a coordinator double that records every leave it is sent. The real
 * consumer, runner, group and membership run over it; only the network and the fetch loop are
 * missing, so what `disconnect()` sends is what the shipped code decided to send.
 *
 * @param {'classic'|'consumer'} protocol
 * @param {string|null} groupInstanceId
 * @param {object} [options]
 * @param {Function} [options.restartOnFailure] the consumer's `retry.restartOnFailure`
 * @returns {{ consumer: object, cluster: object, unsubscribeTopicRecreated: jest.Mock,
 *   leaves: object[], stop: jest.SpyInstance, group: () => object, runner: () => object }}
 *   `leaves` holds the LeaveGroup requests (classic) or the heartbeats sent with a negative epoch
 *   (consumer protocol), in order; `group()` is the group the runner stopped and `runner()` the
 *   runner `run()` started
 */
const build = (protocol, groupInstanceId, { restartOnFailure } = {}) => {
  const leaves = []
  const coordinator = {
    isConnected: () => true,
    nodeId: 0,
    offsetCommit: jest.fn(async () => {}),
    offsetFetch: jest.fn(async () => ({ responses: [] })),
    negotiatedVersion: jest.fn(() => null),
    joinGroup: jest.fn(async () => ({
      generationId: 1,
      leaderId: 'someone-else',
      memberId: MEMBER_ID,
      members: [],
      groupProtocol: 'a',
    })),
    syncGroup: jest.fn(async () => ({ memberAssignment: Buffer.alloc(0) })),
    heartbeat: jest.fn(async () => {}),
    leaveGroup: jest.fn(async (request) => leaves.push(request)),
    consumerGroupHeartbeat: jest.fn(async (request) => {
      if (request.memberEpoch < 0) leaves.push(request)
      return heartbeatAnswer
    }),
  }
  const unsubscribeTopicRecreated = jest.fn()
  const cluster = {
    connect: jest.fn(async () => {}),
    disconnect: jest.fn(async () => {}),
    isConnected: () => true,
    removeBroker: jest.fn(),
    onTopicRecreated: jest.fn(() => unsubscribeTopicRecreated),
    addMultipleTargetTopics: jest.fn(async () => {}),
    refreshMetadata: jest.fn(async () => {}),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    refreshMetadataForTopicIds: jest.fn(async () => {}),
    committedOffsets: () => ({}),
    offsetOperations: () => new OffsetOperations(),
    topicName: (topicId) => (topicId === TOPIC_ID ? TOPIC : undefined),
    topicId: () => undefined,
    getNodeIds: () => [0],
    findTopicPartitionMetadata: () => [0, 1].map((partitionId) => ({ partitionId, leader: 0 })),
    findGroupCoordinator: jest.fn(async () => coordinator),
    retry: {},
  }
  const stop = jest.spyOn(Runner.prototype, 'stop')
  const runnerStart = jest.spyOn(Runner.prototype, 'start')

  const consumer = createConsumer({
    cluster,
    groupId: GROUP_ID,
    groupInstanceId,
    logger: newLogger(),
    retry: { retries: 0, restartOnFailure },
    ...(protocol === 'consumer'
      ? { groupProtocol: 'consumer' }
      : { partitionAssigners: [assigner], sessionTimeout: 30000 }),
  })
  built.push(consumer)

  return {
    consumer,
    cluster,
    unsubscribeTopicRecreated,
    leaves,
    stop,
    group: () => stop.mock.contexts[0].consumerGroup,
    runner: () => runnerStart.mock.contexts[0],
  }
}

/** Builds a consumer and runs it until it has joined its group. */
const start = async (protocol, groupInstanceId, options) => {
  const member = build(protocol, groupInstanceId, options)
  await member.consumer.subscribe({ topic: TOPIC })
  await member.consumer.run({ eachMessage: async () => {} })
  return member
}

describe('Consumer > disconnect({ leaveGroup })', () => {
  beforeEach(() => {
    jest.spyOn(Runner.prototype, 'scheduleFetchManager').mockImplementation(() => {})
  })

  afterEach(async () => {
    for (const consumer of built.splice(0)) {
      await consumer.disconnect().catch(() => {})
    }
    jest.restoreAllMocks()
  })

  const NOT_SENT = null
  const leaveGroupRequest = (memberId, groupInstanceId) => ({
    memberId,
    groupInstanceId,
    reason: LEAVE_REASON,
  })
  const leaveHeartbeat = (memberEpoch, instanceId) => ({
    groupId: GROUP_ID,
    memberEpoch,
    ...(instanceId ? { instanceId } : {}),
  })

  describe('what each protocol sends', () => {
    // protocol, member kind, leaveGroup, the one request it sends (or none)
    test.each([
      ['classic', 'dynamic', undefined, leaveGroupRequest(MEMBER_ID, null)],
      ['classic', 'dynamic', true, leaveGroupRequest(MEMBER_ID, null)],
      ['classic', 'dynamic', false, NOT_SENT],
      ['classic', 'static', undefined, NOT_SENT],
      ['classic', 'static', true, leaveGroupRequest(MEMBER_ID, INSTANCE_ID)],
      ['classic', 'static', false, NOT_SENT],
      ['consumer', 'dynamic', undefined, leaveHeartbeat(-1)],
      ['consumer', 'dynamic', true, leaveHeartbeat(-1)],
      ['consumer', 'dynamic', false, NOT_SENT],
      ['consumer', 'static', undefined, leaveHeartbeat(-2, INSTANCE_ID)],
      ['consumer', 'static', true, leaveHeartbeat(-1, INSTANCE_ID)],
      ['consumer', 'static', false, leaveHeartbeat(-2, INSTANCE_ID)],
    ])('%s protocol, %s member, leaveGroup %p', async (protocol, kind, leaveGroup, sent) => {
      const groupInstanceId = kind === 'static' ? INSTANCE_ID : null
      const { consumer, cluster, leaves, group } = await start(protocol, groupInstanceId)

      await consumer.disconnect({ leaveGroup })

      if (sent === NOT_SENT) {
        expect(leaves).toEqual([])
      } else {
        expect(leaves).toHaveLength(1)
        expect(leaves[0]).toMatchObject(sent)
      }
      if (protocol === 'consumer' && kind === 'dynamic') {
        expect(leaves.every((request) => request.instanceId === undefined)).toBe(true)
      }
      expect(cluster.disconnect).toHaveBeenCalledTimes(1)

      if (protocol === 'classic') {
        expect(group().memberId).toBeNull()
      } else {
        expect(group().memberEpoch).toEqual(sent ? sent.memberEpoch : -1)
        expect(group().groupMembership.state).toEqual(MEMBER_STATES.UNSUBSCRIBED)
      }
    })

    test.each([
      [undefined, 0],
      [false, 1],
      [true, 1],
    ])(
      "a static classic member told leaveGroup %p reaches the group's leave %i time(s): only the default skips it",
      async (leaveGroup, calls) => {
        const leaveSpy = jest.spyOn(ConsumerGroup.prototype, 'leave')
        const { consumer } = await start('classic', INSTANCE_ID)

        await consumer.disconnect({ leaveGroup })

        expect(leaveSpy).toHaveBeenCalledTimes(calls)
        if (calls) {
          expect(leaveSpy).toHaveBeenCalledWith(
            expect.objectContaining({ leaveGroup, operation: expect.any(ConsumerOperation) })
          )
        }
      }
    )

    test('a static classic member that is told to leave sends both ids and the reason in one LeaveGroup v5 request', async () => {
      const members = [{ memberId: MEMBER_ID, groupInstanceId: INSTANCE_ID, reason: LEAVE_REASON }]
      const expected = (await leaveGroupV5({ groupId: GROUP_ID, members }).encode()).buffer
      const harness = createGroupApiBroker({
        versions: { [apiKeys.LeaveGroup]: { minVersion: 0, maxVersion: 5 } },
        respond: () => Buffer.from(LEAVE_GROUP_OK.hex, 'hex'),
      })
      const protocol = new ClassicGroupProtocol({
        cluster: { findGroupCoordinator: async () => harness.broker },
        groupId: GROUP_ID,
        groupInstanceId: INSTANCE_ID,
        sessionTimeout: 30000,
        rebalanceTimeout: 60000,
        assigners: [],
        instrumentationEmitter: new InstrumentationEventEmitter(),
        logger: newLogger(),
      })
      protocol.coordinator = harness.broker
      protocol.memberId = MEMBER_ID

      try {
        await protocol.leave({ leaveGroup: true })

        expect(harness.requests.map(({ apiVersion, body }) => [apiVersion, body])).toEqual([
          [5, expected],
        ])
        expect(protocol.memberId).toBeNull()
      } finally {
        await harness.pool.destroy()
      }
    })
  })

  describe('what a leave that sends no request still does', () => {
    test('a dynamic consumer-protocol member that remains in the group still commits and reports its partitions as revoked', async () => {
      const commit = jest.spyOn(ConsumerGroup.prototype, 'commitBeforeRelease')
      const { consumer, leaves } = await start('consumer', null)
      const revoked = []
      consumer.on(events.PARTITIONS_REVOKED, ({ payload }) => revoked.push(payload.partitions))

      await consumer.disconnect({ leaveGroup: false })

      expect(commit).toHaveBeenCalledWith(OWNED, { operation: expect.any(ConsumerOperation) })
      expect(revoked).toEqual([OWNED])
      expect(leaves).toEqual([])
    })

    test.each(['classic', 'consumer'])(
      'a %s member that has not joined sends nothing, whatever it is asked',
      async (protocol) => {
        const { consumer, leaves, stop } = build(protocol, null)
        // The runner starts and never joins, so the member has no id and owns nothing.
        Runner.prototype.start.mockImplementation(async function () {
          this.running = true
        })
        await consumer.subscribe({ topic: TOPIC })
        await consumer.run({ eachMessage: async () => {} })

        await consumer.disconnect({ leaveGroup: true })

        expect(stop).toHaveBeenCalledTimes(1)
        expect(leaves).toEqual([])
      }
    )
  })

  describe('an argument that is not understood', () => {
    test.each([['yes'], [1], [0], [null], [{}], [[]]])(
      'leaveGroup %p is rejected before anything is stopped',
      async (leaveGroup) => {
        const { consumer, cluster, stop, unsubscribeTopicRecreated } = await start('classic', null)
        const stopped = []
        consumer.on(events.STOP, () => stopped.push('stop'))
        consumer.on(events.DISCONNECT, () => stopped.push('disconnect'))

        await expect(consumer.disconnect({ leaveGroup })).rejects.toThrow(KafkaJSNonRetriableError)

        expect(stop).not.toHaveBeenCalled()
        expect(cluster.disconnect).not.toHaveBeenCalled()
        expect(unsubscribeTopicRecreated).not.toHaveBeenCalled()
        expect(stopped).toEqual([])
      }
    )

    test.each([[true], ['SIGTERM'], [1], [() => {}]])(
      'an argument that is not an options object (%p) is ignored, so it is disconnect() with none',
      async (argument) => {
        const { consumer, leaves, stop } = await start('classic', INSTANCE_ID)

        await consumer.disconnect(argument)

        expect(leaves).toEqual([])
        expect(stop.mock.calls[0][0]).toBeUndefined()
      }
    )

    test('works as a signal handler: process.on(signal, consumer.disconnect) passes the signal name', async () => {
      const { consumer, leaves } = await start('classic', null)
      const onSignal = consumer.disconnect

      await expect(onSignal('SIGTERM')).resolves.toBeUndefined()

      expect(leaves).toHaveLength(1)
    })

    test('a rejected call leaves the consumer running and able to disconnect', async () => {
      const { consumer, leaves } = await start('classic', null)

      await expect(consumer.disconnect({ leaveGroup: 'yes' })).rejects.toThrow('"leaveGroup"')
      await consumer.disconnect({ leaveGroup: true })

      expect(leaves).toHaveLength(1)
    })

    test.each([[undefined], [null], [{}], [{ leaveGroup: undefined }]])(
      'options %p mean the default',
      async (options) => {
        const { consumer, leaves } = await start('classic', null)

        await consumer.disconnect(options)

        expect(leaves).toHaveLength(1)
      }
    )
  })

  describe('stopping and disconnecting more than once', () => {
    test.each([
      ['a dynamic member still leaves', null, false, 1],
      ['a static member still does not', INSTANCE_ID, true, 0],
    ])('stop() takes no option: %s', async (_, groupInstanceId, leaveGroup, requests) => {
      const { consumer, leaves, stop } = await start('classic', groupInstanceId)

      await consumer.stop({ leaveGroup })

      expect(leaves).toHaveLength(requests)
      expect(stop.mock.calls[0][0]).toBeUndefined()
    })

    test.each([
      ['classic', false, true, 0],
      ['classic', true, false, 1],
      ['consumer', false, true, 0],
      ['consumer', true, false, 1],
    ])(
      "a %s disconnect({ leaveGroup: %p }) that is joined by disconnect({ leaveGroup: %p }) keeps the first caller's choice",
      async (protocol, first, second, requests) => {
        const { consumer, leaves, stop } = await start(protocol, null)

        const calls = [
          consumer.disconnect({ leaveGroup: first }),
          consumer.disconnect({ leaveGroup: second }),
        ]
        await Promise.all(calls)

        expect(stop).toHaveBeenCalledTimes(1)
        expect(stop.mock.calls[0][0]).toEqual({ leaveGroup: first })
        expect(leaves).toHaveLength(requests)
      }
    )

    test('a consumer that never ran disconnects without a request, and so does a second call', async () => {
      const { consumer, cluster, leaves, stop } = build('classic', INSTANCE_ID)

      await consumer.disconnect({ leaveGroup: true })
      await consumer.disconnect({ leaveGroup: true })

      expect(stop).not.toHaveBeenCalled()
      expect(leaves).toEqual([])
      expect(cluster.disconnect).toHaveBeenCalledTimes(2)
    })

    test('a disconnect({ leaveGroup: true }) after stop() has nothing left to leave and sends nothing', async () => {
      const { consumer, leaves, stop } = await start('classic', INSTANCE_ID)

      await consumer.stop()
      await consumer.disconnect({ leaveGroup: true })

      expect(stop).toHaveBeenCalledTimes(1)
      expect(leaves).toEqual([])
    })

    test('a second call after the first has finished sends nothing more', async () => {
      const { consumer, leaves, stop } = await start('classic', null)

      await consumer.disconnect({ leaveGroup: true })
      await consumer.disconnect({ leaveGroup: true })

      expect(stop).toHaveBeenCalledTimes(1)
      expect(leaves).toHaveLength(1)
    })
  })

  describe('a crash', () => {
    const boom = () => new KafkaJSNonRetriableError('fatal misconfiguration')

    test('disconnects with the default, so a static member keeps its slot', async () => {
      const crashes = []
      const { consumer, leaves, stop, runner } = await start('classic', INSTANCE_ID, {
        restartOnFailure: async () => false,
      })
      consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload))

      await runner().onCrash(boom())

      expect(crashes).toHaveLength(1)
      expect(stop).toHaveBeenCalledTimes(1)
      expect(stop.mock.calls[0][0]).toBeUndefined()
      expect(leaves).toEqual([])
    })

    test('is not restarted when disconnect({ leaveGroup }) was called while restartOnFailure was deciding', async () => {
      let decide
      const crashes = []
      const { consumer, runner } = await start('classic', null, {
        restartOnFailure: () => new Promise((resolve) => (decide = resolve)),
      })
      consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload))

      const crashing = runner().onCrash(boom())
      await waitFor(() => decide != null, { delay: 5, maxWait: 2000 })
      await consumer.disconnect({ leaveGroup: true })
      decide(true)
      await crashing

      expect(crashes).toHaveLength(1)
      expect(crashes[0].restart).toBe(false)
    })

    test('is still restarted when a disconnect({ leaveGroup }) rejected for a bad option came in while restartOnFailure was deciding', async () => {
      let decide
      const crashes = []
      const { consumer, runner } = await start('classic', null, {
        restartOnFailure: () => new Promise((resolve) => (decide = resolve)),
      })
      consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload))

      const crashing = runner().onCrash(boom())
      await waitFor(() => decide != null, { delay: 5, maxWait: 2000 })
      await expect(consumer.disconnect({ leaveGroup: 'yes' })).rejects.toThrow(
        KafkaJSNonRetriableError
      )
      decide(true)
      await crashing

      expect(crashes).toHaveLength(1)
      expect(crashes[0].restart).toBe(true)
    })

    test('is not restarted when disconnect({ leaveGroup }) is still stopping a runner as restartOnFailure answers', async () => {
      const realStop = Runner.prototype.stop
      let holdStop = false
      let release
      const gate = new Promise((resolve) => (release = resolve))
      let decide
      const crashes = []
      const { consumer, runner, stop } = await start('classic', null, {
        restartOnFailure: () => new Promise((resolve) => (decide = resolve)),
      })
      stop.mockImplementation(async function (...args) {
        if (holdStop) await gate
        return realStop.apply(this, args)
      })
      consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload))

      const crashing = runner().onCrash(boom())
      await waitFor(() => decide != null, { delay: 5, maxWait: 2000 })
      // The crash's own stop has finished, so there is no runner: running again gives the next
      // disconnect one whose stop can be held open.
      await consumer.run({ eachMessage: async () => {} })
      holdStop = true
      const disconnecting = consumer.disconnect({ leaveGroup: true })
      await waitFor(() => stop.mock.calls.length === 2, { delay: 5, maxWait: 2000 })
      decide(true)
      await crashing
      release()
      await disconnecting

      expect(crashes).toHaveLength(1)
      expect(crashes[0].restart).toBe(false)
    })
  })

  describe('a poll-interval overrun', () => {
    test.each([
      ['a static member leaves for now (-2)', INSTANCE_ID, -2],
      ['a dynamic member leaves (-1)', null, -1],
    ])(
      '%s, whatever disconnect({ leaveGroup }) would have chosen',
      async (_, groupInstanceId, epoch) => {
        const { consumer, leaves, runner } = await start('consumer', groupInstanceId)

        await runner().consumerGroup.leaveForPollExpiry()
        await consumer.disconnect({ leaveGroup: true })

        expect(leaves.map(({ memberEpoch }) => memberEpoch)).toEqual([epoch])
      }
    )
  })
})
