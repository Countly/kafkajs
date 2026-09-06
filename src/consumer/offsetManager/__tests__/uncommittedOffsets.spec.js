const OffsetManager = require('../index')

describe('Consumer > OffsetMananger > uncommittedOffsets', () => {
  let offsetManager, topic1, topic2, memberAssignment

  beforeEach(() => {
    topic1 = 'topic-1'
    topic2 = 'topic-2'

    memberAssignment = {
      [topic1]: [0, 1],
      [topic2]: [0, 1, 2, 3],
    }

    offsetManager = new OffsetManager({ memberAssignment })
  })

  it('returns all resolved offsets which have not been committed', () => {
    const defaultOffsetInt = 2

    Object.keys(memberAssignment).forEach((topic) => {
      memberAssignment[topic].forEach((partition) => {
        offsetManager.resolveOffset({ topic, partition, offset: defaultOffsetInt.toString() })
      })
    })

    const defaultResolvedOffsetInt = defaultOffsetInt + 1

    // If committed offset equal to resolved offset, then partition is marked as committed
    offsetManager.committedOffsets()[topic2][0] = defaultResolvedOffsetInt.toString() // "committed"
    offsetManager.committedOffsets()[topic2][1] = (defaultResolvedOffsetInt - 1).toString() // not "committed"
    offsetManager.committedOffsets()[topic2][2] = (defaultResolvedOffsetInt + 1).toString() // not "committed"

    expect(offsetManager.uncommittedOffsets()).toEqual({
      topics: [
        {
          topic: topic1,
          partitions: [
            { partition: '0', offset: defaultResolvedOffsetInt.toString(), leaderEpoch: -1 },
            { partition: '1', offset: defaultResolvedOffsetInt.toString(), leaderEpoch: -1 },
          ],
        },
        {
          topic: topic2,
          partitions: [
            { partition: '1', offset: defaultResolvedOffsetInt.toString(), leaderEpoch: -1 },
            { partition: '2', offset: defaultResolvedOffsetInt.toString(), leaderEpoch: -1 },
            { partition: '3', offset: defaultResolvedOffsetInt.toString(), leaderEpoch: -1 },
          ],
        },
      ],
    })
  })

  it('tolerates a resolved offset key that holds no offset', () => {
    // Whatever creates it, a key with no value is "nothing resolved": both walkers key off
    // Object.keys, and feeding an absent value to Long is a TypeError, which is not retriable.
    offsetManager.resolvedOffsets[topic1][0] = undefined

    expect(offsetManager.uncommittedOffsets()).toEqual({ topics: [] })
    expect(offsetManager.countResolvedOffsets().toString()).toEqual('0')
  })

  it('nextOffset does not create a resolved offset for a partition with no committed one', () => {
    expect(offsetManager.nextOffset(topic1, 0).toString()).toEqual('0')
    expect(Object.keys(offsetManager.resolvedOffsets[topic1])).toEqual([])

    offsetManager.committedOffsets()[topic1][0] = '7'
    expect(offsetManager.nextOffset(topic1, 0).toString()).toEqual('7')
    expect(offsetManager.resolvedOffsets[topic1][0]).toEqual('7')
  })
})
