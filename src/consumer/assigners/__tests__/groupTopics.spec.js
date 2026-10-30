const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')
const factories = [
  ['roundRobin', require('../roundRobinAssigner')],
  ['range', require('../rangeAssigner')],
  ['sticky', require('../stickyAssigner')],
  ['cooperative', require('../cooperativeStickyAssigner')],
]
/** @param {object[]} result @returns {Map<string, object>} decoded assignments */
const decoded = (result) =>
  new Map(
    result.map(({ memberId, memberAssignment }) => [
      memberId,
      MemberAssignment.decode(memberAssignment).assignment,
    ])
  )
/** @param {string} memberId @param {string[]} topics @returns {object} a wire member */
const member = (memberId, topics) => ({
  memberId,
  memberMetadata: MemberMetadata.encode({ version: 0, topics }),
})

describe.each(factories)('%s group partition metadata', (_, factory) => {
  it('assigns the union without changing leader topics', async () => {
    const cluster = {
      findTopicPartitionMetadata: jest.fn(() => [{ partitionId: 99 }]),
    }
    const topics = ['A']
    const result = decoded(
      await factory({ cluster }).assign({
        members: [member('a', ['A']), member('b', ['B'])],
        topics,
        groupTopics: ['A', 'B'],
        groupTopicMetadata: new Map([
          ['A', [{ partitionId: 0 }]],
          ['B', [{ partitionId: 1 }]],
        ]),
      })
    )
    expect(result.get('a')).toEqual({ A: [0] })
    expect(result.get('b')).toEqual({ B: [1] })
    expect(topics).toEqual(['A'])
    expect(cluster.findTopicPartitionMetadata).not.toHaveBeenCalled()
  })
  it('never falls back for a missing map name', async () => {
    const cluster = {
      findTopicPartitionMetadata: jest.fn(() => [{ partitionId: 99 }]),
    }
    const result = decoded(
      await factory({ cluster }).assign({
        members: [member('a', ['A', 'B'])],
        topics: ['A'],
        groupTopics: ['A', 'B'],
        groupTopicMetadata: new Map([['A', [{ partitionId: 0 }]]]),
      })
    )
    expect(result.get('a')).toEqual({ A: [0] })
    expect(cluster.findTopicPartitionMetadata).not.toHaveBeenCalled()
  })
  it('uses cluster metadata when the entire map is absent', async () => {
    const cluster = {
      findTopicPartitionMetadata: jest.fn(() => [{ partitionId: 7 }]),
    }
    const result = decoded(
      await factory({ cluster }).assign({
        members: [member('a', ['A'])],
        topics: ['A'],
      })
    )
    expect(result.get('a')).toEqual({ A: [7] })
    expect(cluster.findTopicPartitionMetadata).toHaveBeenCalledWith('A')
  })
  it('assigns a prototype-looking Map key', async () => {
    const cluster = { findTopicPartitionMetadata: jest.fn() }
    const result = decoded(
      await factory({ cluster }).assign({
        members: [member('a', ['__proto__'])],
        topics: [],
        groupTopics: ['__proto__'],
        groupTopicMetadata: new Map([['__proto__', [{ partitionId: 3 }]]]),
      })
    )
    expect(Object.keys(result.get('a'))).toEqual(['__proto__'])
    const [ownName] = Object.keys(result.get('a'))
    expect(result.get('a')[ownName]).toEqual([3])
    expect(cluster.findTopicPartitionMetadata).not.toHaveBeenCalled()
  })
})
