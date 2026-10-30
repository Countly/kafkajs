const ClassicGroupProtocol = require('../groupProtocol/classic')
const ConsumerGroup = require('../consumerGroup')
const { MemberMetadata } = require('../assignerProtocol')

beforeEach(() => {
  expect(typeof ClassicGroupProtocol.prototype.leaderMetadata).toBe('function')
  expect(typeof ConsumerGroup.prototype.beginRebalanceRound).toBe('function')
})

/** @param {string[]} topics @returns {object} decoded subscription bytes */
const member = (topics) => ({
  memberMetadata: MemberMetadata.encode({ version: 0, topics }),
})

describe('leader union snapshot', () => {
  afterEach(() => jest.restoreAllMocks())
  it('sorts a union, excludes bad topic rows and retains leaderless IDs without cache fallback', async () => {
    const group = Object.create(ClassicGroupProtocol.prototype)
    group.logger = { warn: jest.fn() }
    group.cluster = {
      findTopicPartitionMetadata: jest.fn(() => [{ partitionId: 0, leader: 1 }]),
      groupMetadata: jest.fn(async () => ({
        topicMetadata: [
          {
            topic: 'B',
            topicErrorCode: 0,
            partitionMetadata: [{ partitionId: 2, leader: -1, partitionErrorCode: 5 }],
          },
          {
            topic: 'C',
            topicErrorCode: 29,
            partitionMetadata: [{ partitionId: 99 }],
          },
        ],
      })),
    }
    jest.spyOn(performance, 'now').mockReturnValue(800)
    const own = ['A']
    const round = {
      roundDeadlineAt: 1000,
      guard: () => null,
      wait: (work) => work(),
      allowAutoTopicCreation: false,
    }
    const result = await group.leaderMetadata([member(['C', 'B']), member(['A', 'B'])], own, round)
    expect(result.groupTopics).toEqual(['A', 'B', 'C'])
    expect(own).toEqual(['A'])
    expect([...result.groupTopicMetadata.keys()]).toEqual(['A', 'B'])
    expect(result.groupTopicMetadata.get('B')).toEqual([
      { partitionId: 2, leader: -1, partitionErrorCode: 5 },
    ])
    expect(group.cluster.findTopicPartitionMetadata.mock.calls).toEqual([['A']])
    expect(group.cluster.groupMetadata).toHaveBeenCalledTimes(1)
    expect(group.cluster.groupMetadata.mock.calls[0][0]).toMatchObject({
      topics: ['B', 'C'],
      deadlineAt: 900,
      allowAutoTopicCreation: false,
    })
    expect(group.logger.warn).toHaveBeenCalledTimes(1)
  })
  it('makes no extra snapshot when all members have the leader subscription', async () => {
    const group = Object.create(ClassicGroupProtocol.prototype)
    group.cluster = {
      findTopicPartitionMetadata: () => [],
      groupMetadata: jest.fn(),
    }
    const result = await group.leaderMetadata([member(['A'])], ['A'], {
      roundDeadlineAt: performance.now() + 100,
      guard: () => null,
      wait: (work) => work(),
      allowAutoTopicCreation: true,
    })
    expect(result.groupTopics).toEqual(['A'])
    expect(group.cluster.groupMetadata).not.toHaveBeenCalled()
  })
})

const IdleWait = require('../idleWait')
const Cluster = require('../../cluster')
const { connectionOpts, newLogger } = require('testHelpers')

/** @returns {object} actual round methods with private, non-network dependencies */
const roundGroup = () => {
  const group = Object.create(ConsumerGroup.prototype)
  group.idleWait = new IdleWait()
  group.rebalanceTimeout = 1000
  group.cluster = { allowAutoTopicCreation: true }
  group.isStopping = () => false
  return group
}

describe('round deadline ownership', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => {
    jest.restoreAllMocks()
    jest.clearAllTimers()
    jest.useRealTimers()
  })
  it('keeps one deadline when most time was spent before the leader snapshot', async () => {
    let now = 0
    jest.spyOn(performance, 'now').mockImplementation(() => now)
    const group = roundGroup()
    const round = group.beginRebalanceRound()
    now = 800
    const protocol = Object.create(ClassicGroupProtocol.prototype)
    protocol.cluster = {
      findTopicPartitionMetadata: () => [],
      groupMetadata: jest.fn(async () => ({ topicMetadata: [] })),
    }
    await protocol.leaderMetadata([member(['A', 'B'])], ['A'], round)
    expect(round.roundDeadlineAt).toBe(1000)
    expect(protocol.cluster.groupMetadata.mock.calls[0][0].deadlineAt).toBe(900)
    now = 1001
    const called = jest.fn(async () => {})
    await expect(round.wait(called)).rejects.toMatchObject({ type: 'JOIN_DEADLINE_EXCEEDED' })
    expect(called).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(0)
  })
  it('retires waits when a replacement round has equal membership values', async () => {
    const group = roundGroup()
    const first = group.beginRebalanceRound()
    let release
    const held = new Promise((resolve) => {
      release = resolve
    })
    const pending = first.wait(() => held).catch((error) => error)
    await Promise.resolve()
    const next = group.beginRebalanceRound()
    expect(await pending).toMatchObject({ type: 'JOIN_DEADLINE_EXCEEDED' })
    expect(first.leaderAttempt).not.toBe(next.leaderAttempt)
    release()
    await Promise.resolve()
    expect(next.guard()).toBeNull()
    expect(jest.getTimerCount()).toBe(0)
    next.retire()
  })
  it('retries with a new token while retaining the pre-hook token and absolute deadline', () => {
    const group = roundGroup()
    const round = group.beginRebalanceRound()
    const first = round.attempt()
    const second = round.attempt()
    expect(first.leaderAttempt).toBe(round.leaderAttempt)
    expect(second.leaderAttempt).not.toBe(first.leaderAttempt)
    expect(second.roundDeadlineAt).toBe(first.roundDeadlineAt)
    expect(first.guard()).toMatchObject({ type: 'JOIN_DEADLINE_EXCEEDED' })
    expect(second.guard()).toBeNull()
    round.retire()
  })
  it('preserves an original failure and clears its timer', async () => {
    const group = roundGroup()
    const round = group.beginRebalanceRound()
    const original = new Error('transport failed')
    await expect(
      round.wait(async () => {
        throw original
      })
    ).rejects.toBe(original)
    expect(jest.getTimerCount()).toBe(0)
    round.retire()
  })
})

/** @param {Function} assign @returns {object} real classic sync with counted, non-network boundaries */
const leader = (assign) => {
  const protocol = Object.create(ClassicGroupProtocol.prototype)
  Object.assign(protocol, {
    groupId: 'group',
    memberId: 'a',
    leaderId: 'a',
    generationId: 1,
    groupProtocol: 'custom',
    protocolType: 'consumer',
    mode: 'eager',
    members: [
      { memberId: 'a', ...member(['A']) },
      { memberId: 'b', ...member(['B']) },
    ],
    assigners: [{ name: 'custom', assign }],
    logger: { debug() {}, warn() {} },
    cluster: {
      refreshMetadata: jest.fn(async () => {}),
      findTopicPartitionMetadata: () => [{ partitionId: 0 }],
      groupMetadata: jest.fn(async () => ({
        topicMetadata: [{ topic: 'B', topicErrorCode: 0, partitionMetadata: [{ partitionId: 1 }] }],
      })),
    },
    coordinator: {
      syncGroup: jest.fn(async () => ({
        memberAssignment: require('../assignerProtocol').MemberAssignment.encode({
          version: 0,
          assignment: { A: [0] },
        }),
      })),
    },
  })
  return protocol
}

describe('round creation policy', () => {
  it.each([
    ['configured true', true, {}, true],
    ['configured false', false, {}, false],
    ['client regex', true, { regexSubscription: { fromBeginning: false } }, false],
    ['broker regex', true, { subscribedTopicRegex: '^A' }, false],
  ])(
    'sends the %s policy to the leader snapshot',
    async (_, configured, subscription, expected) => {
      const group = roundGroup()
      group.cluster = new Cluster({
        ...connectionOpts(),
        logger: newLogger(),
        retry: { retries: 0 },
        allowAutoTopicCreation: configured,
      })
      Object.assign(group, subscription)
      const round = group.beginRebalanceRound()
      const protocol = Object.create(ClassicGroupProtocol.prototype)
      protocol.cluster = {
        findTopicPartitionMetadata: () => [],
        groupMetadata: jest.fn(async () => ({ topicMetadata: [] })),
      }
      await protocol.leaderMetadata([member(['A', 'B'])], ['A'], round)
      expect(protocol.cluster.groupMetadata.mock.calls[0][0].allowAutoTopicCreation).toBe(expected)
      round.retire()
    }
  )
})

describe('guarded classic leader sync', () => {
  afterEach(() => jest.restoreAllMocks())
  it('passes custom topics unchanged with additive union fields', async () => {
    const assign = jest.fn(async () => [])
    const protocol = leader(assign)
    const group = roundGroup()
    const round = group.beginRebalanceRound()
    const topics = ['A']
    await protocol.sync({
      topics,
      topicsSubscribed: topics,
      beforeSync() {},
      round: round.attempt(),
    })
    expect(assign.mock.calls[0][0].topics).toBe(topics)
    expect(assign.mock.calls[0][0].groupTopics).toEqual(['A', 'B'])
    expect(assign.mock.calls[0][0].groupTopicMetadata.get('B')).toEqual([{ partitionId: 1 }])
    expect(protocol.coordinator.syncGroup).toHaveBeenCalledTimes(1)
    round.retire()
  })
  it('does not send SyncGroup after a held assigner resumes past actual expiry', async () => {
    let now = 0
    let release, arrived
    jest.spyOn(performance, 'now').mockImplementation(() => now)
    const entered = new Promise((resolve) => {
      arrived = resolve
    })
    const held = new Promise((resolve) => {
      release = resolve
    })
    const protocol = leader(async () => {
      arrived()
      return held
    })
    const group = roundGroup()
    const round = group.beginRebalanceRound()
    const pending = protocol
      .sync({ topics: ['A'], topicsSubscribed: ['A'], beforeSync() {}, round: round.attempt() })
      .catch((error) => error)
    await entered
    now = 1001
    release([])
    expect(await pending).toMatchObject({ type: 'JOIN_DEADLINE_EXCEEDED' })
    expect(protocol.coordinator.syncGroup).not.toHaveBeenCalled()
    round.retire()
  })
})
