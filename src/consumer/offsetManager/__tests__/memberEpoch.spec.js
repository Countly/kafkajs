const OffsetManager = require('../index')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { KafkaJSProtocolError } = require('../../../errors')
const { createErrorFromCode } = require('../../../protocol/error')
const { sentGeneration } = require('../../rebalanceErrorContract')
const { newLogger } = require('testHelpers')

const STALE_MEMBER_EPOCH = 113
const TOPIC = 'topic-a'

const staleEpoch = () => new KafkaJSProtocolError(createErrorFromCode(STALE_MEMBER_EPOCH))

const build = ({ membershipProtocol = 'classic', offsetCommit, offsetFetch, logger } = {}) => {
  const committedOffsets = {}
  const coordinator = {
    isConnected: () => true,
    offsetCommit: offsetCommit || jest.fn(async () => {}),
    offsetFetch: offsetFetch || jest.fn(async () => ({ responses: [] })),
  }
  const cluster = {
    committedOffsets: () => committedOffsets,
    findGroupCoordinator: jest.fn(async () => coordinator),
    refreshMetadata: jest.fn(async () => {}),
    fetchTopicsOffset: jest.fn(async () => []),
    topicId: () => undefined,
  }

  const manager = new OffsetManager({
    cluster,
    coordinator,
    memberAssignment: { [TOPIC]: [0, 1] },
    autoCommit: true,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    topicConfigurations: { [TOPIC]: {} },
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: 'group-a',
    generationId: 3,
    memberId: 'member-a',
    membershipProtocol,
    staleEpochTimeout: 1000,
    logger: logger || newLogger(),
  })

  return { manager, coordinator, cluster, committedOffsets }
}

describe('Consumer > OffsetManager > useMembership', () => {
  it('replaces the membership without touching the assignment or the offsets', () => {
    const { manager } = build()
    manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '5' })
    const coordinator = { isConnected: () => true }

    manager.useMembership({ generationId: 9, memberId: 'member-b', coordinator })

    expect(manager.generationId).toEqual(9)
    expect(manager.memberId).toEqual('member-b')
    expect(manager.coordinator).toBe(coordinator)
    expect(manager.memberAssignment).toEqual({ [TOPIC]: [0, 1] })
    expect(manager.resolvedOffsetAt(TOPIC, 0)).toEqual('6')
  })

  it('is what a commit issued right after it sends', async () => {
    const offsetCommit = jest.fn(async () => {})
    const { manager } = build({ offsetCommit })
    manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '5' })

    manager.useMembership({
      generationId: 9,
      memberId: 'member-b',
      coordinator: manager.coordinator,
    })
    await manager.commitOffsets()

    expect(offsetCommit).toHaveBeenCalledWith(
      expect.objectContaining({ groupGenerationId: 9, memberId: 'member-b' })
    )
  })
})

describe('Consumer > OffsetManager > resolvedOffsetAt', () => {
  it('reads without seeding, allocating or throwing', () => {
    const { manager, committedOffsets } = build()

    expect(manager.resolvedOffsetAt(TOPIC, 0)).toBeUndefined()
    expect(manager.resolvedOffsetAt('never-assigned', 7)).toBeUndefined()
    expect(manager.resolvedOffsets[TOPIC]).toEqual({})

    committedOffsets[TOPIC] = { 0: '20' }
    expect(manager.resolvedOffsetAt(TOPIC, 0)).toBeUndefined()

    manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '20' })
    expect(manager.resolvedOffsetAt(TOPIC, 0)).toEqual('21')
  })

  it('survives a topic whose entries were cleared', () => {
    const { manager } = build()
    manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '5' })
    delete manager.resolvedOffsets[TOPIC]

    expect(() => manager.resolvedOffsetAt(TOPIC, 0)).not.toThrow()
    expect(manager.resolvedOffsetAt(TOPIC, 0)).toBeUndefined()
  })
})

describe('Consumer > OffsetManager > the partition filter', () => {
  it('commits only the named partitions, read at send time', async () => {
    const offsetCommit = jest.fn(async () => {})
    const { manager } = build({ offsetCommit })
    manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '5' })
    manager.resolveOffset({ topic: TOPIC, partition: 1, offset: '7' })

    await manager.commitOffsets({}, { partitions: [{ topic: TOPIC, partitions: [1] }] })

    const [{ topics }] = offsetCommit.mock.calls[0]
    expect(topics).toEqual([
      { topic: TOPIC, partitions: [{ partition: '1', offset: '8', leaderEpoch: -1 }] },
    ])
  })

  it('sends nothing when the filter selects nothing', async () => {
    const offsetCommit = jest.fn(async () => {})
    const { manager } = build({ offsetCommit })
    manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '5' })

    await manager.commitOffsets({}, { partitions: [{ topic: 'other', partitions: [0] }] })
    expect(offsetCommit).not.toHaveBeenCalled()
  })

  it('leaves an explicit offsets argument alone', async () => {
    const offsetCommit = jest.fn(async () => {})
    const { manager } = build({ offsetCommit })
    const topics = [{ topic: TOPIC, partitions: [{ partition: 0, offset: '99' }] }]

    await manager.commitOffsets({ topics }, { partitions: [{ topic: TOPIC, partitions: [1] }] })
    expect(offsetCommit).toHaveBeenCalledWith(expect.objectContaining({ topics }))
  })
})

describe('Consumer > OffsetManager > STALE_MEMBER_EPOCH per call site', () => {
  it('does not retry an interval auto-commit, and resets the timer', async () => {
    const offsetCommit = jest.fn(async () => {
      throw staleEpoch()
    })
    const logger = { debug: jest.fn(), error: jest.fn(), warn: jest.fn(), info: jest.fn() }
    const { manager } = build({ membershipProtocol: 'consumer', offsetCommit, logger })
    manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '5' })
    manager.lastCommit = 0

    await expect(manager.commitOffsets({}, { auto: true })).resolves.toBeUndefined()

    expect(offsetCommit).toHaveBeenCalledTimes(1)
    expect(manager.lastCommit).toBeGreaterThan(0)
    expect(logger.error).toHaveBeenCalledTimes(1)
    expect(logger.error).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ memberEpoch: manager.generationId })
    )
  })

  it('runs without a logger, because 23 shipped construction sites do not pass one', async () => {
    const offsetCommit = jest.fn(async () => {
      throw staleEpoch()
    })
    const manager = new OffsetManager({
      cluster: { committedOffsets: () => ({}) },
      coordinator: { isConnected: () => true, offsetCommit },
      memberAssignment: { [TOPIC]: [0] },
      autoCommit: true,
      autoCommitInterval: null,
      autoCommitThreshold: null,
      topicConfigurations: { [TOPIC]: {} },
      instrumentationEmitter: new InstrumentationEventEmitter(),
      groupId: 'group-a',
      generationId: 3,
      memberId: 'member-a',
      membershipProtocol: 'consumer',
    })
    manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '5' })

    await expect(manager.commitOffsets({}, { auto: true })).resolves.toBeUndefined()
  })

  it('fails a user commit with the protocol error and the epoch it was sent under', async () => {
    const offsetCommit = jest.fn(async () => {
      throw staleEpoch()
    })
    const { manager } = build({ membershipProtocol: 'consumer', offsetCommit })
    manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '5' })

    const caught = await manager.commitOffsets().catch((e) => e)

    expect(caught.type).toEqual('STALE_MEMBER_EPOCH')
    expect(caught.retriable).toBe(false)
    expect(caught.message).toContain('The member epoch is stale')
    expect(sentGeneration(caught)).toEqual(3)
    expect(offsetCommit).toHaveBeenCalledTimes(1)
  })

  it('retries an OffsetFetch 113 while an epoch is available, then gives up', async () => {
    jest.useFakeTimers()
    const offsetFetch = jest.fn(async () => {
      throw staleEpoch()
    })
    const { manager } = build({ membershipProtocol: 'consumer', offsetFetch })

    const resolving = manager.resolveOffsets().catch((e) => e)
    await jest.advanceTimersByTimeAsync(2000)
    const caught = await resolving

    expect(offsetFetch.mock.calls.length).toBeGreaterThan(1)
    expect(caught.type).toEqual('STALE_MEMBER_EPOCH')
    jest.useRealTimers()
  })

  it('does not retry an OffsetFetch 113 once the member has no epoch', async () => {
    const offsetFetch = jest.fn(async () => {
      throw staleEpoch()
    })
    const { manager } = build({ membershipProtocol: 'consumer', offsetFetch })
    manager.useMembership({
      generationId: 0,
      memberId: 'member-a',
      coordinator: manager.coordinator,
    })

    await expect(manager.resolveOffsets()).rejects.toMatchObject({ type: 'STALE_MEMBER_EPOCH' })
    expect(offsetFetch).toHaveBeenCalledTimes(1)
  })
})

describe('Consumer > OffsetManager > the OffsetFetch member fields', () => {
  it('sends them under the consumer protocol', async () => {
    const offsetFetch = jest.fn(async () => ({ responses: [] }))
    const { manager } = build({ membershipProtocol: 'consumer', offsetFetch })

    await manager.resolveOffsets()

    expect(offsetFetch).toHaveBeenCalledWith(
      expect.objectContaining({ memberId: 'member-a', memberEpoch: 3 })
    )
  })

  it('sends none under the classic protocol', async () => {
    const offsetFetch = jest.fn(async () => ({ responses: [] }))
    const { manager } = build({ offsetFetch })

    await manager.resolveOffsets()

    const [payload] = offsetFetch.mock.calls[0]
    expect(payload).not.toHaveProperty('memberId')
    expect(payload).not.toHaveProperty('memberEpoch')
  })
})
