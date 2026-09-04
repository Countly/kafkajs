const OffsetManager = require('../index')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')

describe('Consumer > OffsetManager > getCoordinator', () => {
  let offsetManager, mockCluster, mockCoordinator, mockNewCoordinator

  beforeEach(() => {
    mockCoordinator = { isConnected: jest.fn(() => true), nodeId: 1 }
    mockNewCoordinator = { isConnected: jest.fn(() => true), nodeId: 2 }
    mockCluster = {
      committedOffsets: jest.fn(() => ({})),
      findGroupCoordinator: jest.fn(async () => mockNewCoordinator),
    }

    offsetManager = new OffsetManager({
      cluster: mockCluster,
      coordinator: mockCoordinator,
      memberAssignment: { topic1: [0] },
      groupId: 'test-group-id',
      generationId: 1,
      memberId: 'member-1',
      instrumentationEmitter: new InstrumentationEventEmitter(),
    })
  })

  it('returns the current coordinator when it is connected', async () => {
    await expect(offsetManager.getCoordinator()).resolves.toBe(mockCoordinator)
    expect(mockCluster.findGroupCoordinator).not.toHaveBeenCalled()
  })

  it('re-discovers the coordinator when the cached one is disconnected', async () => {
    mockCoordinator.isConnected.mockReturnValue(false)
    await expect(offsetManager.getCoordinator()).resolves.toBe(mockNewCoordinator)
    expect(mockCluster.findGroupCoordinator).toHaveBeenCalledWith({ groupId: 'test-group-id' })
  })

  it('asks the cluster which broker is authoritative after a failover, not findBroker', async () => {
    // findBroker(staleCoordinator) would have returned the same stale broker
    offsetManager.coordinator = { isConnected: jest.fn(() => false), nodeId: 5 }
    await expect(offsetManager.getCoordinator()).resolves.toBe(mockNewCoordinator)
    expect(mockCluster.findGroupCoordinator).toHaveBeenCalledWith({ groupId: 'test-group-id' })
  })
})
