const OffsetManager = require('../index')
const OffsetOperations = require('../offsetOperations')
const dictionary = require('../../../utils/dictionary')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')

const TOPIC = 'test-topic'
const EARLIEST = { kind: 'earliest' }
const DURATION = { kind: 'by_duration', durationMs: 1000 }
const NONE = { kind: 'none' }

/** Lets every pending request reach its stub before a case answers one. */
const flush = () => new Promise(setImmediate)

/**
 * One consumer of the group on partition 0 of `TOPIC`: auto-commit off, the coordinator holding no
 * offset, and a ListOffsets answer the case releases itself. Every member shares the group's
 * committed cache and operation ledger, as consumers of one `Kafka` instance and group do.
 * @param {object} shared
 * @param {object} shared.cache the group's committed-offset cache
 * @param {OffsetOperations} shared.operations the group's ledger
 * @param {object} policy the member's reset policy
 * @returns {{manager: OffsetManager, coordinator: object, cluster: object, answer: (offset: string) => void}}
 */
const createMember = ({ cache, operations }, policy) => {
  let answer
  const listOffsets = new Promise((resolve) => {
    answer = (offset) => resolve([{ topic: TOPIC, partitions: [{ partition: 0, offset }] }])
  })
  const coordinator = {
    isConnected: () => true,
    offsetCommit: jest.fn(),
    offsetFetch: jest.fn(async ({ topics }) => ({
      responses: topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({
          partition,
          offset: '-1',
          committedLeaderEpoch: -1,
          metadata: null,
          errorCode: 0,
        })),
      })),
    })),
  }
  const cluster = {
    committedOffsets: () => cache,
    topicId: () => undefined,
    fetchTopicsOffset: jest.fn(() => listOffsets),
  }
  const manager = new OffsetManager({
    cluster,
    coordinator,
    memberAssignment: { [TOPIC]: [0] },
    autoCommit: false,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    topicConfigurations: { [TOPIC]: { resetPolicy: policy } },
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: 'test-group',
    generationId: 1,
    memberId: `member-${policy.kind}`,
    operations,
  })
  return { manager, coordinator, cluster, answer }
}

/** A group whose members `earliest` (E) and `duration` (D) both own partition 0, built in order. */
const createGroup = () => {
  const shared = { cache: dictionary(), operations: new OffsetOperations() }
  const earliest = createMember(shared, EARLIEST)
  const duration = createMember(shared, DURATION)
  return { ...shared, earliest, duration }
}

const next = ({ manager }) => manager.nextOffset(TOPIC, 0).toString()

describe('Consumer > OffsetManager > reset positions stay private to the consumer', () => {
  const settled = (group) => {
    expect(next(group.earliest)).toBe('0')
    expect(next(group.duration)).toBe('2')
    expect(group.cache[TOPIC][0]).toBeUndefined()
    expect(group.earliest.coordinator.offsetCommit).not.toHaveBeenCalled()
    expect(group.duration.coordinator.offsetCommit).not.toHaveBeenCalled()
    expect(group.operations.size()).toBe(0)
  }

  test('each member keeps its own policy answer when the earliest member resolves after the duration member was built', async () => {
    const group = createGroup()
    group.earliest.answer('0')
    await group.earliest.manager.resolveOffsets()
    group.duration.answer('2')
    await group.duration.manager.resolveOffsets()

    expect(group.earliest.cluster.fetchTopicsOffset.mock.calls[0][0][0].fromBeginning).toBe(true)
    expect(
      typeof group.duration.cluster.fetchTopicsOffset.mock.calls[0][0][0].partitions[0].timestamp
    ).toBe('number')
    settled(group)
  })

  test('each member keeps its own policy answer when the duration member resolves first', async () => {
    const group = createGroup()
    group.duration.answer('2')
    await group.duration.manager.resolveOffsets()
    group.earliest.answer('0')
    await group.earliest.manager.resolveOffsets()

    settled(group)
  })

  test.each([
    ['earliest', 'duration'],
    ['duration', 'earliest'],
  ])(
    'two resolutions in flight together publish nothing to each other when %s is applied before %s',
    async (first, second) => {
      const group = createGroup()
      const resolving = [group.earliest, group.duration].map(({ manager }) =>
        manager.resolveOffsets()
      )
      await flush()
      const answers = {
        earliest: () => group.earliest.answer('0'),
        duration: () => group.duration.answer('2'),
      }
      answers[first]()
      await flush()
      answers[second]()
      await Promise.all(resolving)

      settled(group)
    }
  )

  test('a member that already seeded its position keeps it while a sibling resolves', async () => {
    const group = createGroup()
    group.duration.answer('2')
    await group.duration.manager.resolveOffsets()
    expect(next(group.duration)).toBe('2')

    group.earliest.answer('0')
    await group.earliest.manager.resolveOffsets()

    expect(next(group.duration)).toBe('2')
    expect(next(group.earliest)).toBe('0')
  })

  test('a none refusal is held by the refusing member alone and leaves a sibling to resolve', async () => {
    const shared = { cache: dictionary(), operations: new OffsetOperations() }
    const refusing = createMember(shared, NONE)
    const resolving = createMember(shared, EARLIEST)

    await refusing.manager.resolveOffsets()
    resolving.answer('0')
    await resolving.manager.resolveOffsets()

    expect(refusing.manager.takeResetRefusals()).toHaveLength(1)
    expect(resolving.manager.takeResetRefusals()).toHaveLength(0)
    expect(refusing.manager.heldOffsetAt(TOPIC, 0)).toBeUndefined()
    expect(next(resolving)).toBe('0')
    expect(shared.cache[TOPIC][0]).toBeUndefined()
    expect(refusing.cluster.fetchTopicsOffset).not.toHaveBeenCalled()
  })

  test('a real commit by one member is visible to the other, as before', async () => {
    const group = createGroup()
    group.earliest.answer('0')
    group.duration.answer('2')
    await Promise.all([
      group.earliest.manager.resolveOffsets(),
      group.duration.manager.resolveOffsets(),
    ])

    await group.earliest.manager.commitOffsets({
      topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '7' }] }],
    })

    expect(group.earliest.coordinator.offsetCommit).toHaveBeenCalledTimes(1)
    expect(group.cache[TOPIC][0]).toBe('7')
    expect(group.earliest.manager.heldOffsetAt(TOPIC, 0)).toBe('7')
    expect(group.duration.manager.heldOffsetAt(TOPIC, 0)).toBe('7')
    expect(group.duration.coordinator.offsetCommit).not.toHaveBeenCalled()
  })

  test("a member's own commit replaces its reset position, so nothing is left uncommitted", async () => {
    const group = createGroup()
    group.earliest.answer('0')
    await group.earliest.manager.resolveOffsets()
    expect(group.earliest.manager.uncommittedOffsets()).toEqual({ topics: [] })

    group.earliest.manager.nextOffset(TOPIC, 0)
    group.earliest.manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '4' })
    await group.earliest.manager.commitOffsets()

    expect(group.cache[TOPIC][0]).toBe('5')
    expect(group.earliest.manager.heldOffsetAt(TOPIC, 0)).toBe('5')
    expect(group.earliest.manager.uncommittedOffsets()).toEqual({ topics: [] })
  })

  test('a commit landing in the shared cell takes over from the member reset position', async () => {
    const group = createGroup()
    group.duration.answer('2')
    await group.duration.manager.resolveOffsets()
    expect(group.duration.manager.heldOffsetAt(TOPIC, 0)).toBe('2')

    group.cache[TOPIC][0] = '9'

    expect(group.duration.manager.heldOffsetAt(TOPIC, 0)).toBe('9')
  })

  test('a sentinel seek and a topic reset drop the private position of the cell', async () => {
    const group = createGroup()
    group.duration.answer('2')
    await group.duration.manager.resolveOffsets()

    await group.duration.manager.seek({ topic: TOPIC, partition: 0, offset: '-2' })
    expect(group.duration.manager.heldOffsetAt(TOPIC, 0)).toBeUndefined()

    group.earliest.answer('0')
    await group.earliest.manager.resolveOffsets()
    group.earliest.manager.resetOffsetsForTopic(TOPIC)
    expect(group.earliest.manager.heldOffsetAt(TOPIC, 0)).toBeUndefined()
  })
})
