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
const LeaderUnionWatch = require('../leaderUnionWatch')
const Generation = require('../../producer/generation')
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
    const result = await protocol.sync({
      topics,
      topicsSubscribed: topics,
      beforeSync() {},
      round: round.attempt(),
    })
    expect(result.unionSnapshot).toEqual(new Map([['B', [1]]]))
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

const watchRows = (...ids) => ids.map((partitionId) => ({ partitionId }))
const watchAnswer = (topic, ids, topicErrorCode = 0) => ({
  topicMetadata: [{ topic, topicErrorCode, partitionMetadata: watchRows(...ids) }],
})

describe('LeaderUnionWatch', () => {
  let now
  let cluster
  let watch

  beforeEach(() => {
    now = 0
    jest.spyOn(performance, 'now').mockImplementation(() => now)
    cluster = { groupMetadata: jest.fn(async () => watchAnswer('C', [0])) }
    watch = new LeaderUnionWatch({ cluster, logger: newLogger(), metadataMaxAge: 5000 })
  })
  afterEach(() => jest.restoreAllMocks())

  describe('snapshotOf', () => {
    it('keeps the partition ids of each union-only topic, empty for one without rows', () => {
      const metadata = new Map([['B', watchRows(2, 0)]])

      expect(LeaderUnionWatch.snapshotOf(['B', 'C'], metadata)).toEqual(
        new Map([
          ['B', [2, 0]],
          ['C', []],
        ])
      )
    })

    it('has nothing to watch without a union-only topic', () => {
      expect(LeaderUnionWatch.snapshotOf([], new Map())).toBeNull()
    })
  })

  describe('arm and clear', () => {
    it('is idle until armed and idle again once cleared', () => {
      expect(watch.armed).toBe(false)
      watch.arm(new Map([['C', []]]))
      expect(watch.armed).toBe(true)
      watch.clear()
      expect(watch.armed).toBe(false)
    })

    it.each([[null], [undefined]])('is idle when armed with %s', (snapshot) => {
      watch.arm(new Map([['C', []]]))
      watch.arm(snapshot)
      expect(watch.armed).toBe(false)
    })

    it('forgets a latched drift when it is armed again', async () => {
      watch.arm(new Map([['C', []]]))
      now = 5000
      await watch.poll(new Generation())
      expect(watch.drift).toMatchObject({ topic: 'C', unknownPartitions: [0] })

      watch.arm(new Map([['C', [0]]]))

      expect(watch.drift).toBeNull()
    })
  })

  describe('poll', () => {
    it('reads nothing before the interval has passed, then once', async () => {
      watch.arm(new Map([['C', []]]))
      const fence = new Generation()

      now = 4999
      await watch.poll(fence)
      expect(cluster.groupMetadata).not.toHaveBeenCalled()

      now = 5000
      await watch.poll(fence)
      expect(cluster.groupMetadata).toHaveBeenCalledTimes(1)
      expect(cluster.groupMetadata.mock.calls[0][0]).toMatchObject({
        topics: ['C'],
        allowAutoTopicCreation: false,
        deadlineAt: 10000,
      })
    })

    it('floors the interval at one second', async () => {
      watch = new LeaderUnionWatch({ cluster, logger: newLogger(), metadataMaxAge: 0 })
      watch.arm(new Map([['C', [0]]]))

      now = 999
      await watch.poll(new Generation())
      expect(cluster.groupMetadata).not.toHaveBeenCalled()
      now = 1000
      await watch.poll(new Generation())
      expect(cluster.groupMetadata).toHaveBeenCalledTimes(1)
    })

    it('shares one read between concurrent callers', async () => {
      let release
      cluster.groupMetadata.mockImplementation(
        () =>
          new Promise((resolve) => {
            release = () => resolve(watchAnswer('C', [0]))
          })
      )
      watch.arm(new Map([['C', []]]))
      now = 5000
      const fence = new Generation()

      const first = watch.poll(fence)
      const second = watch.poll(fence)
      release()
      await Promise.all([first, second])

      expect(cluster.groupMetadata).toHaveBeenCalledTimes(1)
      expect(watch.drift).toMatchObject({ topic: 'C' })
    })

    it('latches only gained partitions and ignores a topic the broker refused', async () => {
      watch.arm(
        new Map([
          ['B', [0, 1]],
          ['C', [0]],
        ])
      )
      cluster.groupMetadata.mockResolvedValueOnce({
        topicMetadata: [
          { topic: 'B', topicErrorCode: 0, partitionMetadata: watchRows(1) },
          { topic: 'C', topicErrorCode: 3, partitionMetadata: watchRows(0, 1, 2) },
        ],
      })
      now = 5000
      await watch.poll(new Generation())
      expect(watch.drift).toBeNull()

      cluster.groupMetadata.mockResolvedValueOnce(watchAnswer('C', [0, 1, 2]))
      now = 10000
      await watch.poll(new Generation())
      expect(watch.drift).toEqual({ topic: 'C', unknownPartitions: [1, 2] })
    })

    it('reads no more once a drift is latched', async () => {
      watch.arm(new Map([['C', []]]))
      now = 5000
      await watch.poll(new Generation())
      now = 60000

      await watch.poll(new Generation())

      expect(cluster.groupMetadata).toHaveBeenCalledTimes(1)
    })

    it('swallows a failed read, keeps its snapshot and tries again after the next interval', async () => {
      cluster.groupMetadata.mockRejectedValueOnce(new Error('reset by peer'))
      watch.arm(new Map([['C', []]]))
      now = 5000
      await expect(watch.poll(new Generation())).resolves.toBeUndefined()
      expect(watch.armed).toBe(true)
      expect(watch.drift).toBeNull()

      now = 9999
      await watch.poll(new Generation())
      expect(cluster.groupMetadata).toHaveBeenCalledTimes(1)
      now = 10000
      await watch.poll(new Generation())
      expect(cluster.groupMetadata).toHaveBeenCalledTimes(2)
      expect(watch.drift).toMatchObject({ topic: 'C' })
    })

    it('rejects with the close error of a closed fence and installs nothing', async () => {
      let release
      cluster.groupMetadata.mockImplementation(
        () =>
          new Promise((resolve) => {
            release = () => resolve(watchAnswer('C', [0]))
          })
      )
      watch.arm(new Map([['C', []]]))
      now = 5000
      const fence = new Generation()
      const closing = new Error('closed')

      const polling = watch.poll(fence)
      fence.close(closing)
      await expect(polling).rejects.toBe(closing)
      watch.clear()
      release()
      await Promise.resolve()

      expect(watch.drift).toBeNull()
    })

    it('drops the result of a read begun before the snapshot was replaced', async () => {
      let release
      cluster.groupMetadata.mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            release = () => resolve(watchAnswer('C', [0, 1]))
          })
      )
      watch.arm(new Map([['C', []]]))
      now = 5000
      const polling = watch.poll(new Generation())

      watch.arm(new Map([['C', [0, 1]]]))
      release()
      await polling

      expect(watch.drift).toBeNull()
      expect(watch.reading).toBeNull()
    })

    describe('the transmission guard of a read', () => {
      let isLeader
      let fence
      const guardOfRead = async () => {
        watch.arm(new Map([['C', []]]))
        now = 5000
        await watch.poll(fence)
        return cluster.groupMetadata.mock.calls[0][0].guard
      }

      beforeEach(() => {
        isLeader = true
        fence = new Generation()
        watch = new LeaderUnionWatch({
          cluster,
          logger: newLogger(),
          metadataMaxAge: 5000,
          isLeader: () => isLeader,
        })
      })

      it('allows the current lifetime of a live run led by the member', async () => {
        expect((await guardOfRead())()).toBeNull()
      })

      it.each([
        ['the watch is cleared', () => watch.clear()],
        ['the watch is armed again', () => watch.arm(new Map([['C', [0]]]))],
        ['the run closes', () => fence.close(new Error('closed'))],
        [
          'the member stops leading',
          () => {
            isLeader = false
          },
        ],
      ])('refuses once %s', async (_, retire) => {
        const guard = await guardOfRead()

        retire()

        expect(guard()).toBeInstanceOf(Error)
      })

      it('publishes no drift from an answer that arrives after the member stopped leading', async () => {
        cluster.groupMetadata.mockImplementation(async () => {
          isLeader = false
          return watchAnswer('C', [0])
        })
        watch.arm(new Map([['C', []]]))
        now = 5000

        await watch.poll(fence)

        expect(watch.drift).toBeNull()
      })
    })

    describe('a read retired while it is held', () => {
      let fence
      let registrations

      beforeEach(() => {
        cluster.groupMetadata.mockImplementation(() => new Promise(() => {}))
        fence = new Generation()
        registrations = []
        const register = fence.register.bind(fence)
        jest.spyOn(fence, 'register').mockImplementation((cancel) => {
          const unregister = jest.fn(register(cancel))
          registrations.push(unregister)
          return unregister
        })
        watch.arm(new Map([['C', []]]))
        now = 5000
      })

      it.each([
        ['cleared', () => watch.clear()],
        ['armed again', () => watch.arm(new Map([['C', [0]]]))],
      ])('releases its waiting caller at once when %s', async (_, retire) => {
        const polling = watch.poll(fence)
        retire()

        await expect(polling).resolves.toBeUndefined()

        expect(registrations).not.toHaveLength(0)
        for (const unregister of registrations) expect(unregister).toHaveBeenCalled()
      })

      it('lets a later read start under the new lifetime', async () => {
        const polling = watch.poll(fence)
        watch.arm(new Map([['C', []]]))
        await polling
        now = 10000

        watch.poll(fence)

        expect(cluster.groupMetadata).toHaveBeenCalledTimes(2)
      })

      it('still rejects with the close error when the run closed as well', async () => {
        const closing = new Error('closed')
        const polling = watch.poll(fence)
        fence.close(closing)
        watch.clear()

        await expect(polling).rejects.toBe(closing)
      })
    })
  })
})
