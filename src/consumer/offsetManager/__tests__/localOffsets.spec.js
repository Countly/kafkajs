const OffsetManager = require('../index')
const OffsetOperations = require('../offsetOperations')
const dictionary = require('../../../utils/dictionary')

/**
 * Builds a manager without a group over a cluster that fails any coordinator or offset call.
 * @param {object} [options] extra manager options
 * @returns {{ manager: OffsetManager, cluster: object, operations: OffsetOperations, offsets: object }}
 */
const create = (options = {}) => {
  const cluster = {
    findGroupCoordinator: jest.fn(),
    committedOffsets: jest.fn(),
    topicId: jest.fn(() => 'topic-id'),
    defaultOffset: jest.fn(() => '-2'),
    fetchTopicsOffset: jest.fn(),
  }
  const operations = new OffsetOperations()
  const offsets = dictionary()
  const manager = new OffsetManager({
    cluster,
    memberAssignment: { records: [0, 1] },
    topicConfigurations: { records: { fromBeginning: true, resetPolicy: { kind: 'earliest' } } },
    autoCommit: false,
    operations,
    offsets,
    ...options,
  })
  return { manager, cluster, operations, offsets }
}

describe('OffsetManager without a group', () => {
  test('answers every committed read locally with the no-offset marker', async () => {
    const { manager, cluster } = create()
    const { responses } = await manager.fetchCommittedOffsets([
      { topic: 'records', partitions: [{ partition: 0 }, { partition: 1 }] },
    ])
    expect(responses).toEqual([
      {
        topic: 'records',
        partitions: [0, 1].map((partition) => ({
          partition,
          offset: '-1',
          committedLeaderEpoch: -1,
          metadata: null,
          errorCode: 0,
        })),
      },
    ])
    expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
  })

  test('keeps its offsets in the private dictionary and never asks the cluster for a group entry', () => {
    const { manager, cluster, offsets } = create()
    expect(manager.committedOffsets()).toBe(offsets)
    expect(Object.getPrototypeOf(manager.committedOffsets())).toBeNull()
    expect(cluster.committedOffsets).not.toHaveBeenCalled()
  })

  test('commits nothing explicitly or automatically', async () => {
    const { manager, cluster } = create()
    await manager.commitOffsets({
      topics: [{ topic: 'records', partitions: [{ partition: 0, offset: '5' }] }],
    })
    await manager.commitOffsets({}, { auto: true })
    expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
  })

  test('a default reset sends no commit, clears the cell and discharges no recreation debt', async () => {
    const { manager, cluster, operations } = create()
    const apply = jest.spyOn(operations, 'apply')
    const fence = jest.spyOn(operations, 'fence')
    manager.resolvedOffsets.records[0] = '9'
    await manager.setDefaultOffset({ topic: 'records', partition: 0 })
    expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
    expect(manager.resolvedOffsets.records[0]).toBeUndefined()
    expect(apply).not.toHaveBeenCalled()
    expect(fence).toHaveBeenCalledTimes(1)
  })

  test('a seek keeps the position locally even when auto-commit is on', async () => {
    const { manager, cluster } = create({ autoCommit: true })
    await expect(manager.seek({ topic: 'records', partition: 1, offset: '4' })).resolves.toBe(true)
    expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
    expect(manager.resolvedOffsets.records[1]).toBe('4')
  })
})
