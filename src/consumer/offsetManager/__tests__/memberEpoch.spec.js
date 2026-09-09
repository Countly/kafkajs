const OffsetManager = require('../index')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { KafkaJSProtocolError } = require('../../../errors')
const { createErrorFromCode } = require('../../../protocol/error')
const { sentGeneration, isDeletedGroupCommit } = require('../../rebalanceErrorContract')
const { newLogger } = require('testHelpers')

const STALE_MEMBER_EPOCH = 113
const TOPIC = 'topic-a'

const staleEpoch = () => new KafkaJSProtocolError(createErrorFromCode(STALE_MEMBER_EPOCH))

const build = ({
  membershipProtocol = 'classic',
  offsetCommit,
  offsetFetch,
  logger,
  membership,
  isStopping,
} = {}) => {
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
    membership,
    isStopping,
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

  // MF-1 of the fix round's sibling: before the live accessor the loop re-sent the manager's
  // construction-time epoch on every attempt, so no retry could ever be answered anything else.
  it('re-sends under the epoch the heartbeats installed, and succeeds', async () => {
    jest.useFakeTimers()
    let epoch = 3
    const seen = []
    const offsetFetch = jest.fn(async ({ memberEpoch }) => {
      seen.push(memberEpoch)

      if (memberEpoch === 3) {
        throw staleEpoch()
      }

      return { responses: [] }
    })
    const { manager, coordinator } = build({ membershipProtocol: 'consumer', offsetFetch })
    // What `ConsumerGroup` injects: a live read of the membership, not the snapshot the manager
    // was built with.
    manager.membership = () => ({ generationId: epoch, memberId: 'member-a', coordinator })

    const resolving = manager.resolveOffsets()
    await jest.advanceTimersByTimeAsync(150)
    epoch = 4
    await jest.advanceTimersByTimeAsync(300)
    await resolving

    expect(seen[0]).toEqual(3)
    expect(seen[seen.length - 1]).toEqual(4)
    expect(manager.generationId).toEqual(4)
    jest.useRealTimers()
  })

  it('abandons the retry within one sleep when the consumer is stopping', async () => {
    jest.useFakeTimers()
    let stopping = false
    const offsetFetch = jest.fn(async () => {
      throw staleEpoch()
    })
    const { manager } = build({
      membershipProtocol: 'consumer',
      offsetFetch,
      isStopping: () => stopping,
    })

    const resolving = manager.resolveOffsets().catch((e) => e)
    await jest.advanceTimersByTimeAsync(150)
    const attemptsBeforeStop = offsetFetch.mock.calls.length
    stopping = true
    await jest.advanceTimersByTimeAsync(150)
    const caught = await resolving

    expect(caught.type).toEqual('STALE_MEMBER_EPOCH')
    expect(offsetFetch.mock.calls.length).toBeLessThanOrEqual(attemptsBeforeStop + 1)
    jest.useRealTimers()
  })

  it('keeps its coordinator when the membership is between coordinators', async () => {
    // The consumer-protocol membership nulls its own `coordinator` while it re-finds one after a
    // NOT_COORDINATOR; `getCoordinator()` dereferences the field, so the null must not be adopted.
    const { manager, coordinator } = build({ membershipProtocol: 'consumer' })
    manager.membership = () => ({ generationId: 5, memberId: 'member-b', coordinator: null })

    await expect(manager.resolveOffsets()).resolves.toBeUndefined()

    expect(manager.coordinator).toBe(coordinator)
    expect(manager.generationId).toEqual(5)
    expect(manager.memberId).toEqual('member-b')
  })

  it('consults no accessor on the classic path', async () => {
    const membership = jest.fn()
    const isStopping = jest.fn(() => false)
    const { manager, coordinator } = build({ membership, isStopping })

    await manager.resolveOffsets()

    expect(membership).not.toHaveBeenCalled()
    expect(coordinator.offsetFetch).toHaveBeenCalledWith(
      expect.not.objectContaining({ memberEpoch: expect.anything() })
    )
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

// MF-5: the commit error is classified far from where it is raised - an auto-commit's error
// surfaces out of the fetch round - so the request that drew it is recorded on the error itself.
describe('Consumer > OffsetManager > the offset-commit origin stamp', () => {
  const groupIdNotFound = () => new KafkaJSProtocolError(createErrorFromCode(69))

  it.each([['classic'], ['consumer']])(
    'marks a %s commit failure as raised on the commit path',
    async (membershipProtocol) => {
      const offsetCommit = jest.fn(async () => {
        throw groupIdNotFound()
      })
      const { manager } = build({ membershipProtocol, offsetCommit })
      manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '5' })

      const caught = await manager.commitOffsets().catch((e) => e)

      expect(caught.code).toEqual(69)
      expect(isDeletedGroupCommit(caught)).toBe(true)
    }
  )

  it('marks a seek commit too', async () => {
    const offsetCommit = jest.fn(async () => {
      throw groupIdNotFound()
    })
    const { manager } = build({ offsetCommit })

    const caught = await manager.seek({ topic: TOPIC, partition: 0, offset: '5' }).catch((e) => e)

    expect(isDeletedGroupCommit(caught)).toBe(true)
  })

  it('leaves an OffsetFetch failure unstamped, so a read cannot look like a commit', async () => {
    const offsetFetch = jest.fn(async () => {
      throw groupIdNotFound()
    })
    const { manager } = build({ offsetFetch })

    const caught = await manager.resolveOffsets().catch((e) => e)

    expect(caught.code).toEqual(69)
    expect(isDeletedGroupCommit(caught)).toBe(false)
  })
})
