const OffsetManager = require('../index')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { createErrorFromCode } = require('../../../protocol/error')
const Generation = require('../../../producer/generation')
const { createPatternGroup } = require('../../__tests__/fixtures/patternGroup')

/** @returns {{promise: Promise<void>, resolve: Function}} an explicitly released boundary */
const gate = () => {
  let resolve
  const promise = new Promise((done) => (resolve = done))
  return { promise, resolve }
}

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

  it('re-discovers the coordinator when none is cached', async () => {
    offsetManager.coordinator = null
    await expect(offsetManager.getCoordinator()).resolves.toBe(mockNewCoordinator)
    expect(mockCluster.findGroupCoordinator).toHaveBeenCalledWith({ groupId: 'test-group-id' })
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

describe.each(['classic', 'consumer'])(
  'Consumer > OffsetManager > coordinator publication > %s',
  (groupProtocol) => {
    const offsets = { topics: [{ topic: 'orders-a', partitions: [{ partition: 0, offset: '7' }] }] }

    /**
     * Builds the real membership-to-offset-manager discovery callback through an assignment.
     * @returns {Promise<object>} the group, cluster and manager, before any commit discovery
     */
    const assigned = async () => {
      const harness = createPatternGroup({
        declarations: ['orders-a'],
        catalog: { 'orders-a': 'AAAAAAAAAAAAAAAAAAAAAg' },
        group: { groupProtocol },
      })
      await harness.refresh()
      await harness.round()
      const { group, cluster } = harness
      const manager = group.offsetManager
      manager.committedOffsets()['orders-a'][0] = '1'
      return { group, cluster, manager }
    }

    it.each(['ownership replacement', 'caller fence'])(
      'keeps the replacement coordinator when %s overtakes recovery metadata refresh',
      async (reason) => {
        const { group, cluster, manager } = await assigned()
        const original = createErrorFromCode(16)
        group.coordinator.offsetCommit.mockRejectedValue(original)
        group.coordinator = null
        const arrived = gate()
        const refresh = gate()
        cluster.refreshMetadata.mockImplementation(async () => {
          arrived.resolve()
          await refresh.promise
        })
        const obsolete = { nodeId: 'late-recovery', isConnected: () => true }
        cluster.findGroupCoordinator.mockResolvedValue(obsolete)
        const fence = new Generation()
        const pending = manager.commitOffsets(offsets, { fence }).catch((error) => error)
        await arrived.promise
        if (reason === 'ownership replacement') group.ownership.renew()
        else fence.close(new Error('commit caller retired'))
        const replacement = { nodeId: 'replacement', isConnected: () => true }
        group.coordinator = replacement
        manager.coordinator = replacement
        refresh.resolve()

        expect(await pending).toBe(original)
        expect(group.coordinator).toBe(replacement)
        expect(manager.coordinator).toBe(replacement)
        expect(manager.committedOffsets()['orders-a'][0]).toBe('1')
        expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
      }
    )

    it.each(['initial', 'recovery'])(
      'publishes no coordinator when the originating commit guard refuses the %s lookup',
      async (stage) => {
        const { group, cluster, manager } = await assigned()
        const original = createErrorFromCode(16)
        if (stage === 'recovery') group.coordinator.offsetCommit.mockRejectedValue(original)
        else manager.coordinator = null
        group.coordinator = null
        const arrived = gate()
        const lookup = gate()
        const obsolete = { nodeId: 'obsolete', isConnected: () => true }
        cluster.findGroupCoordinator.mockImplementation(async () => {
          arrived.resolve()
          await lookup.promise
          return obsolete
        })
        let refusal = null
        const pending = manager.commitOffsets(offsets, { guard: () => refusal }).catch((e) => e)
        await arrived.promise
        refusal = new Error('commit caller retired')
        const cached = manager.coordinator
        lookup.resolve()

        expect(await pending).toBe(stage === 'initial' ? refusal : original)
        expect(group.coordinator).toBeNull()
        expect(manager.coordinator).toBe(cached)
        expect(manager.committedOffsets()['orders-a'][0]).toBe('1')
        expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
      }
    )
  }
)
