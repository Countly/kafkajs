const createConsumer = require('../index')
const Runner = require('../runner')
const OffsetOperations = require('../offsetManager/offsetOperations')
const { MemberAssignment } = require('../assignerProtocol')
const { COOPERATIVE, EAGER } = require('../groupProtocol/interface')
const { events } = require('../instrumentationEvents')
const { KafkaJSNonRetriableError } = require('../../errors')
const { newLogger } = require('testHelpers')

const GROUP_ID = 'first-round-crash'
const TOPIC = 'topic-a'
const TOPIC_ID = 'kOJl1vDcS4Ct5xLg5rGqaQ'
const INSTANCE_ID = 'instance-1'
const MEMBER_ID = 'member-1'
const LEAVE_REASON = 'the consumer is being closed'
const ASSIGNMENT = MemberAssignment.encode({ version: 0, assignment: { [TOPIC]: [0, 1] } })
const JOINED = {
  generationId: 1,
  leaderId: 'someone-else',
  memberId: MEMBER_ID,
  members: [],
  groupProtocol: 'a',
}
const HEARTBEAT_ANSWER = {
  throttleTime: 0,
  errorCode: 0,
  errorMessage: null,
  memberId: MEMBER_ID,
  memberEpoch: 3,
  heartbeatIntervalMs: 5000,
  assignment: { topicPartitions: [{ topicId: TOPIC_ID, partitions: [0, 1] }] },
}

/** An assigner the coordinator double names as the group's protocol. */
const assigner = (cooperative) => () => ({
  name: 'a',
  version: 0,
  supportedProtocols: () => (cooperative ? [COOPERATIVE, EAGER] : [EAGER]),
  protocol: () => ({ name: 'a', metadata: Buffer.alloc(0) }),
  assign: async () => [],
})

/** Every consumer a case builds, disconnected after it so no timer outlives the case. */
const built = []

/** @returns {Promise<boolean>} whether `done` held within 200 turns of the event loop */
const settles = async (done) => {
  for (let turn = 0; turn < 200 && !done(); turn += 1) {
    await new Promise((resolve) => setImmediate(resolve))
  }
  return done()
}

/**
 * A consumer over a coordinator double. The real consumer, runner, group and membership run over
 * it; only the network and the fetch loop are missing, so what the first round and its cleanup send
 * is what the shipped code decided to send.
 *
 * @param {object} [options]
 * @param {'classic'|'consumer'} [options.protocol='classic']
 * @param {string|null} [options.groupInstanceId=null]
 * @param {boolean} [options.cooperative=false] classic only: a cooperative assigner
 * @param {Function} [options.restartOnFailure] the consumer's `retry.restartOnFailure`
 * @param {number[]} [options.knownPartitions] the partitions the topic's metadata lists
 * @param {Promise<void>} [options.leaveGate] a classic LeaveGroup waits for it
 * @param {(order: string[]) => object} [options.coordinator] members that replace the double's
 * @param {Function} [options.findGroupCoordinator] replaces the coordinator lookup
 * @param {(joined: () => boolean) => Function} [options.refreshMetadata] a metadata refresh made
 *   from the number of joins already sent
 * @returns {{ consumer: object, cluster: object, coordinator: object, order: string[],
 *   leaves: object[], crashes: object[], eachMessage: jest.Mock, runner: () => object }}
 *   `order` logs `join`, `leave`, `disconnect` and `crash` as they happen, plus `assigned` and
 *   `revoked` from the rebalance listener; `leaves` holds the classic LeaveGroup requests or the
 *   consumer-protocol heartbeats sent with a negative epoch
 */
const rig = ({
  protocol = 'classic',
  groupInstanceId = null,
  cooperative = false,
  restartOnFailure = async () => false,
  knownPartitions = [0, 1],
  leaveGate,
  coordinator: replacement,
  findGroupCoordinator,
  refreshMetadata,
} = {}) => {
  const order = []
  const leaves = []
  const crashes = []
  const coordinator = {
    isConnected: () => true,
    nodeId: 0,
    offsetCommit: jest.fn(async () => {}),
    offsetFetch: jest.fn(async () => ({ responses: [] })),
    negotiatedVersion: jest.fn(() => null),
    joinGroup: jest.fn(async () => {
      order.push('join')
      return JOINED
    }),
    syncGroup: jest.fn(async () => ({ memberAssignment: ASSIGNMENT })),
    heartbeat: jest.fn(async () => {}),
    leaveGroup: jest.fn(async (request) => {
      order.push('leave')
      leaves.push(request)
      await leaveGate
    }),
    consumerGroupHeartbeat: jest.fn(async (request) => {
      if (request.memberEpoch === 0) order.push('join')
      if (request.memberEpoch < 0) {
        order.push('leave')
        leaves.push(request)
      }
      return HEARTBEAT_ANSWER
    }),
    ...replacement?.(order),
  }
  const cluster = {
    connect: jest.fn(async () => {}),
    disconnect: jest.fn(async () => {}),
    isConnected: () => true,
    removeBroker: jest.fn(),
    onTopicRecreated: jest.fn(() => () => {}),
    addMultipleTargetTopics: jest.fn(async () => {}),
    refreshMetadata: jest.fn(refreshMetadata?.(() => order.includes('join')) ?? (async () => {})),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    refreshMetadataForTopicIds: jest.fn(async () => {}),
    committedOffsets: () => ({}),
    offsetOperations: () => new OffsetOperations(),
    topicName: (topicId) => (topicId === TOPIC_ID ? TOPIC : undefined),
    topicId: () => undefined,
    getNodeIds: () => [0],
    findTopicPartitionMetadata: () =>
      knownPartitions.map((partitionId) => ({ partitionId, leader: 0 })),
    findGroupCoordinator: findGroupCoordinator ?? jest.fn(async () => coordinator),
    retry: {},
  }
  const runnerStart = jest.spyOn(Runner.prototype, 'start')
  const consumer = createConsumer({
    cluster,
    groupId: GROUP_ID,
    groupInstanceId,
    logger: newLogger(),
    retry: { retries: 0, restartOnFailure },
    ...(protocol === 'consumer'
      ? { groupProtocol: 'consumer' }
      : { partitionAssigners: [assigner(cooperative)], sessionTimeout: 30000 }),
  })
  built.push(consumer)
  consumer.on(events.DISCONNECT, () => order.push('disconnect'))
  consumer.on(events.CRASH, ({ payload }) => {
    order.push('crash')
    crashes.push(payload)
  })

  return {
    consumer,
    cluster,
    coordinator,
    order,
    leaves,
    crashes,
    eachMessage: jest.fn(async () => {}),
    runner: () => runnerStart.mock.contexts[0],
  }
}

/**
 * Subscribes and runs. The first `onPartitionsAssigned` call throws, which fails the first round
 * after the member has joined; a later call returns.
 * @param {object} member from `rig`
 * @param {{ listener?: boolean }} [options] `listener: false` runs without a rebalance listener
 * @returns {Promise<void>} settles once the crash cleanup and the `CRASH` event are done
 */
const run = async (member, { listener = true } = {}) => {
  let failed = false
  const rebalanceListener = listener
    ? {
        onPartitionsAssigned: async () => {
          member.order.push('assigned')
          if (!failed) {
            failed = true
            throw new Error('assigned failed')
          }
        },
        onPartitionsRevoked: async ({ partitions }) => {
          if (partitions.length) member.order.push('revoked')
        },
      }
    : undefined
  await member.consumer.subscribe({ topic: TOPIC })
  await member.consumer.run({ eachMessage: member.eachMessage, rebalanceListener })
}

describe('Consumer > a first round that fails after joining', () => {
  beforeEach(() => {
    jest.spyOn(Runner.prototype, 'scheduleFetchManager').mockImplementation(() => {})
  })

  afterEach(async () => {
    jest.useRealTimers()
    for (const consumer of built.splice(0)) {
      await consumer.disconnect().catch(() => {})
    }
    jest.restoreAllMocks()
  })

  const expectCleanedUp = (member) => {
    const { heartbeatManager } = member.runner()
    expect(heartbeatManager.stopped).toBe(true)
    expect(heartbeatManager.isRunning()).toBe(false)
    expect(member.eachMessage).not.toHaveBeenCalled()
  }

  test.each([
    ['eager', false],
    ['cooperative', true],
  ])(
    'classic dynamic member (%s), onPartitionsAssigned throws once: one LeaveGroup with its member id, after onPartitionsRevoked and before DISCONNECT; no second JoinGroup; no handler call; no heartbeat after the cleanup',
    async (_, cooperative) => {
      const member = rig({ cooperative })

      await run(member)

      expect(member.order).toEqual(['join', 'assigned', 'revoked', 'leave', 'disconnect', 'crash'])
      expect(member.leaves).toHaveLength(1)
      expect(member.leaves[0]).toMatchObject({
        memberId: MEMBER_ID,
        groupInstanceId: null,
        reason: LEAVE_REASON,
      })
      expect(member.crashes).toHaveLength(1)
      expect(member.crashes[0].restart).toBe(false)
      expectCleanedUp(member)
    }
  )

  test('consumer-protocol dynamic member, same failure: one heartbeat with member epoch -1 before DISCONNECT', async () => {
    const member = rig({ protocol: 'consumer' })

    await run(member)

    expect(member.order.filter((entry) => entry !== 'assigned' && entry !== 'revoked')).toEqual([
      'join',
      'leave',
      'disconnect',
      'crash',
    ])
    expect(member.leaves).toHaveLength(1)
    expect(member.leaves[0]).toMatchObject({ groupId: GROUP_ID, memberEpoch: -1 })
    expect(member.leaves[0].instanceId).toBeUndefined()
    expect(member.crashes).toHaveLength(1)
    expectCleanedUp(member)
  })

  test('consumer-protocol static member: one heartbeat with epoch -2', async () => {
    const member = rig({ protocol: 'consumer', groupInstanceId: INSTANCE_ID })

    await run(member)

    expect(member.leaves).toHaveLength(1)
    expect(member.leaves[0]).toMatchObject({
      groupId: GROUP_ID,
      memberEpoch: -2,
      instanceId: INSTANCE_ID,
    })
    expect(member.order.indexOf('leave')).toBeLessThan(member.order.indexOf('disconnect'))
  })

  test('classic static member: no LeaveGroup and the member id is forgotten, as a normal stop', async () => {
    const member = rig({ groupInstanceId: INSTANCE_ID })

    await run(member)

    expect(member.crashes).toHaveLength(1)
    expect(member.leaves).toEqual([])
    expect(member.runner().consumerGroup.memberId).toBeNull()
  })

  test('a metadata refresh that fails after the join in the first round also leaves once', async () => {
    const member = rig({
      knownPartitions: [0],
      refreshMetadata: (joined) => async () => {
        if (joined()) throw new KafkaJSNonRetriableError('refresh failed')
      },
    })

    await run(member, { listener: false })

    expect(member.order).toEqual(['join', 'leave', 'disconnect', 'crash'])
    expect(member.leaves).toHaveLength(1)
    expect(member.leaves[0]).toMatchObject({ memberId: MEMBER_ID })
    expect(member.cluster.refreshMetadata).toHaveBeenCalled()
  })

  test.each(['classic', 'consumer'])(
    'a %s member whose FindCoordinator keeps failing before any join sends no LeaveGroup and no heartbeat',
    async (protocol) => {
      const findGroupCoordinator = jest.fn(async () => {
        throw new KafkaJSNonRetriableError('no coordinator')
      })
      const member = rig({ protocol, findGroupCoordinator })

      await run(member, { listener: false })

      expect(findGroupCoordinator).toHaveBeenCalled()
      expect(member.crashes).toHaveLength(1)
      expect(member.order).toEqual(['disconnect', 'crash'])
      expect(member.leaves).toEqual([])
      expect(member.coordinator.consumerGroupHeartbeat).not.toHaveBeenCalled()
    }
  )

  test('a JoinGroup refused before any member id was assigned sends no LeaveGroup', async () => {
    const member = rig({
      coordinator: (order) => ({
        joinGroup: jest.fn(async () => {
          order.push('join')
          throw new KafkaJSNonRetriableError('join refused')
        }),
      }),
    })

    await run(member, { listener: false })

    expect(member.order).toEqual(['join', 'disconnect', 'crash'])
    expect(member.leaves).toEqual([])
  })

  test('a later disconnect({ leaveGroup: true }) after that cleanup sends nothing more', async () => {
    const member = rig()
    await run(member)
    expect(member.leaves).toHaveLength(1)

    await member.consumer.disconnect({ leaveGroup: true })

    expect(member.leaves).toHaveLength(1)
    expect(member.coordinator.leaveGroup).toHaveBeenCalledTimes(1)
  })

  test('a first-round join that times out leaves once, and the restart rejoins', async () => {
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick'] })
    let joins = 0
    const member = rig({
      restartOnFailure: async () => true,
      coordinator: (order) => ({
        joinGroup: jest.fn(async (_, options) => {
          order.push('join')
          joins += 1
          if (joins > 1) return JOINED
          // MEMBER_ID_REQUIRED: the broker assigned the member id, which the membership keeps as
          // its pending id; the follow-up join then never answers.
          options?.onMemberIdRequired?.(MEMBER_ID)
          return new Promise(() => {})
        }),
      }),
    })

    const running = run(member, { listener: false })
    await jest.advanceTimersByTimeAsync(61000)
    await running
    await jest.advanceTimersByTimeAsync(5000)

    expect(await settles(() => member.order.filter((entry) => entry === 'join').length === 2)).toBe(
      true
    )
    expect(member.crashes[0].restart).toBe(true)
    expect(member.order.filter((entry) => entry === 'join' || entry === 'leave')).toEqual([
      'join',
      'leave',
      'join',
    ])
    expect(member.leaves).toHaveLength(1)
    expect(member.leaves[0]).toMatchObject({ memberId: MEMBER_ID })
    expect(member.coordinator.joinGroup.mock.calls.map(([request]) => request.memberId)).toEqual([
      '',
      MEMBER_ID,
    ])
  })

  test('the cleanup stop runs once: a disconnect() racing the crash cleanup joins it', async () => {
    let release
    const leaveGate = new Promise((resolve) => (release = resolve))
    const member = rig({ leaveGate })

    const running = run(member)
    const leaving = await settles(() => member.leaves.length === 1)
    const disconnecting = member.consumer.disconnect()
    release()
    await Promise.all([running, disconnecting])

    expect(leaving).toBe(true)
    expect(member.leaves).toHaveLength(1)
    expect(member.crashes).toHaveLength(1)
  })
})
