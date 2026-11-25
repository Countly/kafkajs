const OffsetManager = require('../index')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')

const TOPIC = 'test-topic'

/** @returns {OffsetManager} a manager over partitions 0 and 1, with nothing committed or resolved */
const createManager = () =>
  new OffsetManager({
    cluster: { committedOffsets: () => ({}), topicId: () => undefined },
    coordinator: { isConnected: () => true },
    memberAssignment: { [TOPIC]: [0, 1] },
    autoCommit: false,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    topicConfigurations: { [TOPIC]: {} },
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: 'test-group',
    generationId: 1,
    memberId: 'member-1',
  })

/**
 * @param {OffsetManager} manager
 * @param {number} partition
 * @param {string} offset the committed offset the group cache holds for the partition
 * @returns {void}
 */
const commit = (manager, partition, offset) => {
  const cache = manager.committedOffsets()
  cache[TOPIC] ??= {}
  cache[TOPIC][partition] = offset
}

describe('Consumer > OffsetManager > positionOf', () => {
  it('is null for a topic the manager does not hold and for a partition with nothing held, and never throws', () => {
    const manager = createManager()

    expect(manager.positionOf('another-topic', 0)).toBeNull()
    expect(manager.positionOf(TOPIC, 1)).toBeNull()
  })

  it("equals nextOffset's string: the committed offset, the resolved one, and 0 for an invalid offset", () => {
    const manager = createManager()
    commit(manager, 0, '5')
    commit(manager, 1, '-1')

    expect(manager.positionOf(TOPIC, 0)).toBe('5')
    expect(manager.positionOf(TOPIC, 0)).toBe(manager.nextOffset(TOPIC, 0).toString())

    manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '9' })
    expect(manager.positionOf(TOPIC, 0)).toBe('10')
    expect(manager.positionOf(TOPIC, 0)).toBe(manager.nextOffset(TOPIC, 0).toString())

    expect(manager.positionOf(TOPIC, 1)).toBe('0')
    expect(manager.positionOf(TOPIC, 1)).toBe(manager.nextOffset(TOPIC, 1).toString())
  })

  it('does not seed the resolved offset from the committed one', () => {
    const manager = createManager()
    commit(manager, 0, '5')

    manager.positionOf(TOPIC, 0)

    expect(manager.resolvedOffsetAt(TOPIC, 0)).toBeUndefined()
  })
})
