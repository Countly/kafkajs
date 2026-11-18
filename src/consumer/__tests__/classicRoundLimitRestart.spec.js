const createConsumer = require('../index')
const Runner = require('../runner')
const OffsetOperations = require('../offsetManager/offsetOperations')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { MemberAssignment } = require('../assignerProtocol')
const { KafkaJSProtocolError, KafkaJSRebalanceNotConvergedError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const { newLogger } = require('testHelpers')

const GROUP_ID = 'round-limit-group'
const TOPIC = 'topic-a'
const MEMBER_ID = 'member-1'
const REBALANCE_IN_PROGRESS = 27
const RESTART_DELAY = 300

/** A cooperative assigner the coordinator double names as the group's protocol. */
const assigner = () => ({
  name: 'coop',
  version: 0,
  supportedProtocols: () => ['cooperative'],
  protocol: () => ({ name: 'coop', metadata: Buffer.alloc(0) }),
  assign: async () => [],
})

/** @param {number[]} partitions @returns {Buffer} a SyncGroup member assignment of TOPIC */
const assignmentOf = (partitions) =>
  MemberAssignment.encode({ version: 0, assignment: { [TOPIC]: partitions } })

/**
 * A real classic consumer, with its real runner, group and fetch loop, over a coordinator double.
 * While `churn` is on, every SyncGroup moves the member to the other partition, so every
 * cooperative round revokes one and asks for another; otherwise it answers partition 0.
 * @param {object} retry the consumer's `retry` options
 * @returns {object} the consumer, the double's records and its switches
 */
const build = (retry) => {
  const state = { churn: false, flip: false, fetchFails: 0 }
  const joins = []
  const fetches = []
  const crashes = []
  let generation = 0
  const coordinator = {
    isConnected: () => true,
    nodeId: 0,
    negotiatedVersion: jest.fn(() => null),
    offsetCommit: jest.fn(async () => {}),
    offsetFetch: jest.fn(async ({ topics }) => ({
      responses: topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({
          partition,
          offset: '5',
          metadata: null,
          errorCode: 0,
        })),
      })),
    })),
    joinGroup: jest.fn(async () => {
      joins.push(Date.now())
      return {
        generationId: ++generation,
        leaderId: 'someone-else',
        memberId: MEMBER_ID,
        members: [],
        groupProtocol: 'coop',
      }
    }),
    syncGroup: jest.fn(async () => {
      state.flip = state.churn ? !state.flip : false
      return { memberAssignment: assignmentOf([state.flip ? 1 : 0]) }
    }),
    heartbeat: jest.fn(async () => {}),
    leaveGroup: jest.fn(async () => {}),
  }
  const broker = {
    fetch: jest.fn(async ({ topics }) => {
      for (const { topic, partitions } of topics) {
        for (const { partition, fetchOffset } of partitions) {
          fetches.push({ topic, partition, fetchOffset: String(fetchOffset), at: Date.now() })
        }
      }
      if (state.fetchFails > 0) {
        state.fetchFails--
        throw new KafkaJSProtocolError(createErrorFromCode(REBALANCE_IN_PROGRESS))
      }
      await new Promise((resolve) => setTimeout(resolve, 50))
      return { responses: [] }
    }),
  }
  const cluster = {
    instrumentationEmitter: new InstrumentationEventEmitter(),
    connect: jest.fn(async () => {}),
    disconnect: jest.fn(async () => {}),
    isConnected: () => true,
    removeBroker: jest.fn(),
    onTopicRecreated: jest.fn(() => () => {}),
    addMultipleTargetTopics: jest.fn(async () => {}),
    refreshMetadata: jest.fn(async () => {}),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    committedOffsets: () => ({}),
    offsetOperations: () => new OffsetOperations(),
    defaultOffset: () => -2,
    topicId: () => undefined,
    leaderEpoch: () => -1,
    getNodeIds: () => [0],
    findTopicPartitionMetadata: () => [0, 1].map((partitionId) => ({ partitionId, leader: 0 })),
    findBroker: jest.fn(async () => broker),
    findGroupCoordinator: jest.fn(async () => coordinator),
    fetchTopicsOffset: jest.fn(async (requests) =>
      requests.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({ partition, offset: '0' })),
      }))
    ),
    retry: {},
  }
  const consumer = createConsumer({
    cluster,
    groupId: GROUP_ID,
    logger: newLogger(),
    instrumentationEmitter: cluster.instrumentationEmitter,
    partitionAssigners: [assigner],
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
    retry: { initialRetryTime: RESTART_DELAY, ...retry },
  })
  consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload))
  return { consumer, coordinator, broker, state, joins, fetches, crashes }
}

/**
 * Advances fake time in 10 ms steps until `done()` holds, for at most `budget` ms.
 * @param {() => boolean} done
 * @param {number} [budget]
 * @returns {Promise<void>}
 */
const until = async (done, budget = 2000) => {
  for (let spent = 0; !done() && spent < budget; spent += 10) {
    await jest.advanceTimersByTimeAsync(10)
  }
}

describe('Consumer > a classic cooperative chain that the fetch loop starts and that reaches the round limit', () => {
  let consumer

  beforeEach(() => jest.useFakeTimers())
  afterEach(async () => {
    const stopping = consumer.disconnect().catch(() => {})
    await jest.advanceTimersByTimeAsync(1000)
    await stopping
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  /**
   * Runs a consumer with fetch retries left, lets it fetch, then answers one Fetch with
   * REBALANCE_IN_PROGRESS while every SyncGroup moves the member to the other partition, until the
   * crash handler is called.
   * @param {object} [retry] extra `retry` options
   * @returns {Promise<object>} the built consumer and its records, with `joinsAtCrash` and
   *   `starts`, the runners started so far
   */
  const crashAtTheLimit = async (retry = {}) => {
    const built = build({ retries: 3, ...retry })
    consumer = built.consumer
    const start = jest.spyOn(Runner.prototype, 'start')
    await consumer.subscribe({ topic: TOPIC })
    await consumer.run({ eachMessage: async () => {} })
    await until(() => built.fetches.length > 0)
    expect(built.joins).toHaveLength(1)

    built.state.churn = true
    built.state.fetchFails = 1
    await until(
      () => built.crashes.length > 0 || built.coordinator.syncGroup.mock.calls.length > 40
    )
    built.state.churn = false
    return { ...built, joinsAtCrash: built.joins.length, starts: start }
  }

  it('reports the limit error itself and restarts under the default policy, rejoining and fetching from the committed offset', async () => {
    const run = await crashAtTheLimit()

    expect(run.crashes).toHaveLength(1)
    const [{ error, groupId, restart }] = run.crashes
    expect(error).toBeInstanceOf(KafkaJSRebalanceNotConvergedError)
    expect({ message: error.message, retriable: error.retriable, groupId, restart }).toEqual({
      message: 'cooperative rebalance did not converge in 20 rounds',
      retriable: true,
      groupId: GROUP_ID,
      restart: true,
    })
    expect(run.joinsAtCrash).toEqual(21)

    const crashedAt = Date.now()
    await jest.advanceTimersByTimeAsync(RESTART_DELAY - 20)
    expect(run.joins).toHaveLength(21)

    await until(() => run.fetches.some(({ at }) => at > crashedAt + RESTART_DELAY - 20))
    expect(run.joins).toHaveLength(22)
    expect(run.starts).toHaveBeenCalledTimes(2)
    expect(run.starts.mock.contexts[1]).not.toBe(run.starts.mock.contexts[0])
    expect(run.fetches.filter(({ at }) => at > crashedAt)).toContainEqual(
      expect.objectContaining({ topic: TOPIC, partition: 0, fetchOffset: '5' })
    )
    expect(run.crashes).toHaveLength(1)
  })

  it('hands a custom restartOnFailure the limit error itself and stays stopped when it returns false', async () => {
    const restartOnFailure = jest.fn(async () => false)
    const run = await crashAtTheLimit({ restartOnFailure })

    expect(restartOnFailure).toHaveBeenCalledTimes(1)
    const [[error]] = restartOnFailure.mock.calls
    expect(error).toBeInstanceOf(KafkaJSRebalanceNotConvergedError)
    expect(run.crashes).toEqual([{ error, groupId: GROUP_ID, restart: false }])

    const fetchesAtCrash = run.fetches.length
    await jest.advanceTimersByTimeAsync(RESTART_DELAY * 4)
    expect(run.joins).toHaveLength(run.joinsAtCrash)
    expect(run.fetches).toHaveLength(fetchesAtCrash)
    expect(run.starts).toHaveBeenCalledTimes(1)
  })

  it('does not restart when consumer.stop() is called during the restart delay', async () => {
    const run = await crashAtTheLimit()
    expect(run.crashes.map(({ restart }) => restart)).toEqual([true])

    await jest.advanceTimersByTimeAsync(RESTART_DELAY / 3)
    const stopping = consumer.stop()
    await jest.advanceTimersByTimeAsync(RESTART_DELAY * 4)
    await stopping

    expect(run.joins).toHaveLength(run.joinsAtCrash)
    expect(run.starts).toHaveBeenCalledTimes(1)
    expect(run.crashes).toHaveLength(1)
  })
})
