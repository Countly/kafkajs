const ConsumerGroup = require('../consumerGroup')
const Runner = require('../runner')
const Broker = require('../../broker')
const apiKeys = require('../../protocol/requests/apiKeys')
const { newLogger } = require('testHelpers')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')

const GROUP_INSTANCE_ID = 'static-1'

describe('Consumer > static membership (KIP-345)', () => {
  let coordinator, group

  beforeEach(() => {
    coordinator = {
      joinGroup: jest.fn(async () => ({
        generationId: 1,
        leaderId: 'm1',
        memberId: 'm1',
        members: [],
        groupProtocol: 'a',
      })),
      syncGroup: jest.fn(async () => ({ memberAssignment: Buffer.alloc(0) })),
      heartbeat: jest.fn(async () => {}),
      leaveGroup: jest.fn(async () => {}),
      // No usable JoinGroup version advertised: warnIfStaticMembershipInactive returns quietly,
      // matching this suite's focus on the groupInstanceId plumbing rather than the warning itself.
      negotiatedVersion: jest.fn(() => null),
    }

    group = new ConsumerGroup({
      logger: newLogger(),
      topics: ['t'],
      topicConfigurations: {},
      retry: { retries: 0 },
      cluster: {
        findGroupCoordinator: jest.fn(async () => coordinator),
        refreshMetadata: jest.fn(async () => {}),
        refreshMetadataIfNecessary: jest.fn(async () => {}),
        connect: jest.fn(async () => {}),
        isConnected: () => true,
        getNodeIds: () => [],
        findTopicPartitionMetadata: () => [],
        committedOffsets: () => ({}),
      },
      groupId: 'g',
      groupInstanceId: GROUP_INSTANCE_ID,
      assigners: [
        {
          name: 'a',
          protocol: () => ({ name: 'a', metadata: Buffer.alloc(0) }),
          assign: async () => ({}),
        },
      ],
      instrumentationEmitter: new InstrumentationEventEmitter(),
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
      maxBytesPerPartition: 1024,
      minBytes: 1,
      maxBytes: 1024,
      maxWaitTimeInMs: 100,
      isolationLevel: 1,
      metadataMaxAge: 1000,
    })
  })

  it('sends groupInstanceId on JoinGroup, SyncGroup and Heartbeat', async () => {
    await group.joinAndSync()
    expect(coordinator.joinGroup).toHaveBeenCalledWith(
      expect.objectContaining({ groupInstanceId: GROUP_INSTANCE_ID })
    )
    expect(coordinator.syncGroup).toHaveBeenCalledWith(
      expect.objectContaining({ groupInstanceId: GROUP_INSTANCE_ID })
    )

    group.lastRequest = 0
    await group.heartbeat({ interval: 0 })
    expect(coordinator.heartbeat).toHaveBeenCalledWith(
      expect.objectContaining({ groupInstanceId: GROUP_INSTANCE_ID })
    )
  })

  it('does not send LeaveGroup on stop for a static member (KIP-345 default)', async () => {
    await group.joinAndSync()

    const runner = new Runner({
      logger: newLogger(),
      consumerGroup: group,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      concurrency: 1,
      onCrash: jest.fn(),
    })
    runner.running = true
    runner.fetchManager = { stop: jest.fn(async () => {}) }

    await runner.stop()
    expect(coordinator.leaveGroup).not.toHaveBeenCalled()
  })

  it('still sends LeaveGroup for a dynamic member', async () => {
    group.groupInstanceId = null
    await group.joinAndSync()

    const runner = new Runner({
      logger: newLogger(),
      consumerGroup: group,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      concurrency: 1,
      onCrash: jest.fn(),
    })
    runner.running = true
    runner.fetchManager = { stop: jest.fn(async () => {}) }

    await runner.stop()
    expect(coordinator.leaveGroup).toHaveBeenCalled()
  })

  describe('the negotiated JoinGroup version comes from the protocol layer', () => {
    const brokerStubPool = () => ({
      host: '127.0.0.1',
      port: 9092,
      sasl: null,
      generation: 0,
      connectionTimeout: 1000,
      isConnected: () => true,
      isAuthenticated: () => true,
      setVersions: () => {},
      destroy: async () => {},
    })

    const createConsumerGroup = ({ groupInstanceId }) =>
      new ConsumerGroup({
        logger: newLogger(),
        topics: ['t'],
        topicConfigurations: {},
        retry: { retries: 0 },
        cluster: {
          findGroupCoordinator: jest.fn(async () => {}),
          refreshMetadata: jest.fn(async () => {}),
          refreshMetadataIfNecessary: jest.fn(async () => {}),
          connect: jest.fn(async () => {}),
          isConnected: () => true,
          getNodeIds: () => [],
          findTopicPartitionMetadata: () => [],
          committedOffsets: () => ({}),
        },
        groupId: 'g',
        groupInstanceId,
        assigners: [],
        instrumentationEmitter: new InstrumentationEventEmitter(),
        sessionTimeout: 30000,
        rebalanceTimeout: 60000,
        maxBytesPerPartition: 1024,
        minBytes: 1,
        maxBytes: 1024,
        maxWaitTimeInMs: 100,
        isolationLevel: 1,
        metadataMaxAge: 1000,
      })

    const runWarn = (coordinatorVersions) => {
      const group = createConsumerGroup({ groupInstanceId: 'instance-1' })
      group.coordinator = {
        versions: coordinatorVersions,
        negotiatedVersion: (apiKey, definition) =>
          new Broker({
            connectionPool: brokerStubPool(),
            logger: newLogger(),
            versions: coordinatorVersions,
          }).negotiatedVersion(apiKey, definition),
      }
      const warn = jest.spyOn(group.logger, 'warn')
      group.warnIfStaticMembershipInactive()
      return warn
    }

    it('warns on a coordinator advertising JoinGroup max 4', () => {
      const warn = runWarn({ [apiKeys.JoinGroup]: { minVersion: 0, maxVersion: 4 } })
      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining('negotiated JoinGroup v4'),
        expect.objectContaining({ negotiatedVersion: 4 })
      )
    })

    it('does not warn on a coordinator advertising JoinGroup max 9', () => {
      const warn = runWarn({ [apiKeys.JoinGroup]: { minVersion: 0, maxVersion: 9 } })
      expect(warn).not.toHaveBeenCalled()
    })

    it('returns quietly when the coordinator advertises no JoinGroup at all', () => {
      const warn = runWarn({})
      expect(warn).not.toHaveBeenCalled()
    })
  })
})
