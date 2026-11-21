/**
 * Applying an assignment must stay linear in its size and must not change what it produces:
 * pinned outputs for the subscription state, the offset manager's table, the partition events and
 * the `GROUP_JOIN` payload, plus operation-count guards that cannot flake.
 */

/** Writes made into any internal name table since the last reset. */
let mockTableWrites = 0

// Every internal name table counts its writes, so a test can see a table copied once per topic.
jest.mock('../../utils/dictionary', () =>
  jest.fn((entries = []) => {
    const table = new Proxy(Object.create(null), {
      set(target, key, value) {
        mockTableWrites++
        target[key] = value
        return true
      },
    })
    for (const [key, value] of entries) table[key] = value
    return table
  })
)

const SubscriptionState = require('../subscriptionState')
const ConsumerGroup = require('../consumerGroup')
const RebalanceCoordinator = require('../rebalanceCoordinator')
const PollTimer = require('../pollTimer')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { events } = require('../instrumentationEvents')
const { COOPERATIVE, EAGER } = require('../groupProtocol/interface')
const { newLogger } = require('testHelpers')

/** The assignment install as it was before it was made linear: the equivalence oracle. */
class OracleSubscriptionState extends SubscriptionState {
  assign(topicPartitions = []) {
    this.assignedPartitionsByTopic = topicPartitions.reduce(
      (assigned, { topic, partitions = [] }) =>
        Object.assign(Object.create(null), assigned, { [topic]: { topic, partitions } }),
      Object.create(null)
    )
    this.assignedPartitionSetsByTopic = new Map(
      topicPartitions.map(({ topic, partitions = [] }) => [topic, new Set(partitions)])
    )
    this.onAssignmentChange?.()
  }
}

/** A deterministic generator, so a failing shape can be replayed. */
const random = (seed) => () => {
  seed = (seed * 1103515245 + 12345) % 2147483648
  return seed / 2147483648
}

const AWKWARD_TOPICS = ['__proto__', 'constructor', 'toString', '10', '2', '0', 'b', 'a']

/**
 * @param {number} size entries
 * @param {() => number} next random source
 * @returns {Array<{ topic: string, partitions?: number[] }>} shuffled names, duplicates, missing
 *   and empty partition lists
 */
const generateAssignment = (size, next) =>
  Array.from({ length: size }, (_, index) => {
    const topic =
      next() < 0.3
        ? AWKWARD_TOPICS[Math.floor(next() * AWKWARD_TOPICS.length)]
        : `topic-${Math.floor(next() * size * 2)}`
    const roll = next()
    if (roll < 0.1) return { topic }
    const count = roll < 0.2 ? 0 : 1 + Math.floor(next() * 6)
    return { topic, partitions: Array.from({ length: count }, () => Math.floor(next() * 12)) }
  }).concat(size > 0 ? [{ topic: `topic-${size}`, partitions: [size, 0, 3] }] : [])

/** @returns {object} everything a reader of the subscription state can observe */
const observe = (state, topics) => ({
  keys: Object.keys(state.assignedPartitionsByTopic),
  entries: Object.entries(state.assignedPartitionsByTopic),
  sets: [...state.assignedPartitionSetsByTopic].map(([topic, set]) => [topic, [...set]]),
  assigned: state.assigned(),
  active: state.active(),
  paused: state.paused(),
  isAssigned: topics.flatMap((topic) =>
    [0, 1, 3, 11, 12].map((partition) => state.isAssigned(topic, partition))
  ),
})

describe('Consumer > SubscriptionState > assign', () => {
  it.each([0, 1, 7, 60, 400])(
    'installs exactly what the previous install did for %i generated entries',
    (size) => {
      for (let seed = 1; seed <= 20; seed++) {
        const next = random(seed * 7919 + size)
        const assignment = generateAssignment(size, next)
        const pauses = [
          { topic: '__proto__' },
          { topic: 'topic-1', partitions: [0, 1, 3] },
          { topic: '10', partitions: [2] },
        ]
        const actual = new SubscriptionState({ onAssignmentChange: jest.fn() })
        const oracle = new OracleSubscriptionState({ onAssignmentChange: jest.fn() })
        for (const state of [actual, oracle]) {
          state.pause(pauses)
          state.resume([{ topic: '__proto__', partitions: [1] }])
          state.assign(generateAssignment(3, random(seed)))
          state.assign(assignment)
        }
        const topics = assignment.map(({ topic }) => topic)

        expect(observe(actual, topics)).toEqual(observe(oracle, topics))
        expect(Object.getPrototypeOf(actual.assignedPartitionsByTopic)).toBeNull()
        expect(actual.onAssignmentChange).toHaveBeenCalledTimes(2)
        for (const [topic, entry] of Object.entries(actual.assignedPartitionsByTopic)) {
          const last = assignment.filter((item) => item.topic === topic).pop()
          if (last.partitions) expect(entry.partitions).toBe(last.partitions)
        }
      }
    }
  )

  it('keeps the first position and the last partitions of a repeated topic', () => {
    const state = new SubscriptionState()
    const last = [5]

    state.assign([
      { topic: 'x', partitions: [1] },
      { topic: 'y', partitions: [2] },
      { topic: 'x', partitions: last },
    ])

    expect(Object.keys(state.assignedPartitionsByTopic)).toEqual(['x', 'y'])
    expect(state.assignedPartitionsByTopic.x.partitions).toBe(last)
    expect([...state.assignedPartitionSetsByTopic.keys()]).toEqual(['x', 'y'])
  })

  it('writes the assignment table a number of times linear in the topic count', () => {
    const writesFor = (count) => {
      const assignment = Array.from({ length: count }, (_, index) => ({
        topic: `topic-${index}`,
        partitions: [0, 1],
      }))
      const state = new SubscriptionState()
      mockTableWrites = 0
      state.assign(assignment)
      return mockTableWrites
    }

    const small = writesFor(200)
    const large = writesFor(400)

    expect(small).toBeGreaterThanOrEqual(200)
    expect(large).toBeLessThanOrEqual(2 * small)
  })
})

/**
 * A real `ConsumerGroup` over a stubbed cluster, with a real `RebalanceCoordinator` around it.
 * @param {{ mode?: string, topics: string[], partitions: number }} options
 */
const createRealRound = ({ mode = EAGER, topics, partitions }) => {
  const metadata = Array.from({ length: partitions }, (_, partitionId) => ({ partitionId }))
  const known = new Set(topics)
  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    refreshMetadataIfNecessary: async () => {},
    committedOffsets: () => ({}),
    topicId: () => undefined,
    findTopicPartitionMetadata: (topic) => (known.has(topic) ? metadata : []),
  }
  const group = new ConsumerGroup({
    retry: { retries: 0 },
    cluster,
    groupId: 'linear-group',
    topics,
    topicConfigurations: Object.fromEntries(topics.map((topic) => [topic, {}])),
    logger: newLogger(),
    instrumentationEmitter: new InstrumentationEventEmitter(),
    assigners: [],
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
    maxBytesPerPartition: 1048576,
    minBytes: 1,
    maxBytes: 1048576,
    maxWaitTimeInMs: 100,
    autoCommit: false,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    isolationLevel: 0,
    rackId: '',
    checkCrcs: true,
    rebalanceProtocol: mode,
    metadataMaxAge: 300000,
  })
  group.memberId = 'member-1'
  group.generationId = 1
  group.coordinator = {
    isConnected: () => true,
    nodeId: 0,
    offsetFetch: jest.fn(async () => ({ responses: [] })),
    offsetCommit: jest.fn(async () => {}),
  }
  group.partitionsPerSubscribedTopic = group.generatePartitionsPerSubscribedTopic()

  const emitted = []
  for (const name of [
    events.PARTITIONS_ASSIGNED,
    events.PARTITIONS_REVOKED,
    events.PARTITIONS_LOST,
    events.GROUP_JOIN,
  ]) {
    group.instrumentationEmitter.addListener(name, ({ type, payload }) =>
      emitted.push({ type, payload })
    )
  }

  const coordinator = new RebalanceCoordinator({
    runner: { stopRequested: false, onCrash: jest.fn() },
    consumerGroup: group,
    fetchManager: { start: jest.fn(async () => {}), stop: jest.fn(async () => {}) },
    heartbeatManager: { ensureStarted: jest.fn(), stop: jest.fn(), shutdown: jest.fn() },
    pollTimer: new PollTimer({ pollDeadline: 8000 }),
    instrumentationEmitter: group.instrumentationEmitter,
    logger: newLogger(),
    groupId: 'linear-group',
  })
  group.attach({ rebalanceCoordinator: coordinator, heartbeatManager: null })

  const joinAndSync = jest.spyOn(group, 'joinAndSync')
  const round = async (assignment) => {
    joinAndSync.mockImplementation(async () => ({ assignment }))
    emitted.length = 0
    await coordinator.runRound('rebalance-in-progress', 'revoked')
  }

  return { group, emitted, round }
}

/** The decoded assignment as the protocol hands it over: an ordinary object. */
const decoded = (entries) => Object.fromEntries(entries)

const PINNED_TOPICS = ['b', '__proto__', '10', 'a', '2']

describe('Consumer > ConsumerGroup > applying an assignment', () => {
  it.each([EAGER, COOPERATIVE])(
    'produces the pinned tables, events and GROUP_JOIN payload in the %s protocol',
    async (mode) => {
      const { group, emitted, round } = createRealRound({
        mode,
        topics: PINNED_TOPICS,
        partitions: 4,
      })
      group.pause([{ topic: 'a', partitions: [1] }, { topic: '__proto__' }])

      await round(
        decoded([
          ['b', [3, 1]],
          ['__proto__', [0]],
          ['10', [2, 0]],
          ['a', [1, 0]],
          ['2', [3]],
        ])
      )

      expect(Object.keys(group.offsetManager.memberAssignment)).toEqual([
        '2',
        '10',
        'b',
        '__proto__',
        'a',
      ])
      // Sorted in place: `assigned()` sorts the arrays the offset manager shares.
      expect(Object.entries(group.offsetManager.memberAssignment)).toEqual([
        ['2', [3]],
        ['10', [0, 2]],
        ['b', [1, 3]],
        ['__proto__', [0]],
        ['a', [0, 1]],
      ])
      expect(group.subscriptionState.assigned()).toEqual([
        { topic: '2', partitions: [3] },
        { topic: '10', partitions: [0, 2] },
        { topic: 'b', partitions: [1, 3] },
        { topic: '__proto__', partitions: [0] },
        { topic: 'a', partitions: [0, 1] },
      ])
      expect(group.paused()).toEqual([
        { topic: '__proto__', partitions: [0] },
        { topic: 'a', partitions: [1] },
      ])
      expect(emitted.map(({ type }) => type)).toEqual([
        events.PARTITIONS_ASSIGNED,
        events.GROUP_JOIN,
      ])
      expect(emitted[0].payload.partitions).toEqual([
        { topic: '2', partitions: [3] },
        { topic: '10', partitions: [0, 2] },
        { topic: 'b', partitions: [1, 3] },
        { topic: '__proto__', partitions: [0] },
        { topic: 'a', partitions: [0, 1] },
      ])

      const { memberAssignment } = emitted[1].payload
      expect(Object.getPrototypeOf(memberAssignment)).toBe(Object.prototype)
      expect(Object.entries(memberAssignment)).toEqual([
        ['2', [3]],
        ['10', [0, 2]],
        ['b', [1, 3]],
        ['__proto__', [0]],
        ['a', [0, 1]],
      ])
      expect(Object.hasOwn(memberAssignment, '__proto__')).toBe(true)

      await round(
        decoded([
          ['a', [0]],
          ['b', [1, 2]],
        ])
      )

      expect(Object.entries(group.offsetManager.memberAssignment)).toEqual([
        ['a', [0]],
        ['b', [1, 2]],
      ])
      expect(group.paused()).toEqual([])
      expect(emitted.map(({ type }) => type)).toEqual(
        mode === EAGER
          ? [events.PARTITIONS_REVOKED, events.PARTITIONS_ASSIGNED, events.GROUP_JOIN]
          : [
              events.PARTITIONS_REVOKED,
              events.PARTITIONS_ASSIGNED,
              events.GROUP_JOIN,
              events.GROUP_JOIN,
            ]
      )
      expect(Object.entries(emitted[2].payload.memberAssignment)).toEqual([
        ['a', [0]],
        ['b', [1, 2]],
      ])
    }
  )

  it('writes its name tables a number of times linear in the topic count', async () => {
    const writesFor = async (count) => {
      const topics = Array.from({ length: count }, (_, index) => `topic-${index}`)
      const { group } = createRealRound({ topics, partitions: 1 })
      const assignment = decoded(topics.map((topic) => [topic, [0]]))
      mockTableWrites = 0
      await group.applyRebalanceResult({ assignment }, { rebuildOffsets: true })
      return mockTableWrites
    }

    const small = await writesFor(200)
    const large = await writesFor(400)

    expect(small).toBeGreaterThanOrEqual(2 * 200)
    expect(large).toBeLessThanOrEqual(2 * small + 10)
  })

  it("reads each topic's known partitions a number of times linear in their count", async () => {
    const readsFor = async (count) => {
      const { group } = createRealRound({ topics: ['wide'], partitions: count })
      let reads = 0
      const known = group.partitionsPerSubscribedTopic.get('wide')
      group.partitionsPerSubscribedTopic.set(
        'wide',
        new Proxy(known, {
          get(target, key, receiver) {
            if (typeof key === 'string' && /^\d+$/.test(key)) reads++
            return Reflect.get(target, key, receiver)
          },
        })
      )
      const partitions = Array.from({ length: count }, (_, partition) => partition)
      await group.applyRebalanceResult(
        { assignment: decoded([['wide', partitions]]) },
        { rebuildOffsets: true }
      )
      return reads
    }

    const small = await readsFor(200)
    const large = await readsFor(400)

    expect(small).toBeGreaterThanOrEqual(1)
    expect(large).toBeLessThanOrEqual(2 * small)
  })
})
