const groupMessagesPerPartition = require('./groupMessagesPerPartition')
const { createModPartitioner } = require('testHelpers')

describe('Producer > groupMessagesPerPartition', () => {
  let topic, partitionMetadata, messages, partitioner

  beforeEach(() => {
    topic = 'test-topic'
    partitionMetadata = [
      { partitionId: 1, leader: 1 },
      { partitionId: 2, leader: 2 },
      { partitionId: 0, leader: 0 },
    ]

    messages = [
      { key: '1' },
      { key: '2' },
      { key: '3' },
      { key: '4' },
      { key: '5' },
      { key: '6' },
      { key: '7' },
      { key: '8' },
      { key: '9' },
    ]
    partitioner = createModPartitioner()
  })

  test('group messages per partition', () => {
    const result = groupMessagesPerPartition({ topic, partitionMetadata, messages, partitioner })
    expect(result).toEqual({
      0: [{ key: '3' }, { key: '6' }, { key: '9' }],
      1: [{ key: '1' }, { key: '4' }, { key: '7' }],
      2: [{ key: '2' }, { key: '5' }, { key: '8' }],
    })
  })

  test('returns empty when called with no partition metadata', () => {
    expect(
      groupMessagesPerPartition({ topic, partitionMetadata: [], messages, partitioner })
    ).toEqual({})
  })
})

describe('grouping semantics', () => {
  const partitionMetadata = [{ partitionId: 0 }, { partitionId: 1 }, { partitionId: 2 }]

  it('preserves message order within each partition', () => {
    const messages = Array.from({ length: 300 }, (_, i) => ({ value: `v-${i}` }))
    const partitioner = ({ message }) => Number(message.value.split('-')[1]) % 3

    const grouped = groupMessagesPerPartition({
      topic: 't',
      partitionMetadata,
      messages,
      partitioner,
    })

    expect(Object.keys(grouped).sort()).toEqual(['0', '1', '2'])
    for (const partition of [0, 1, 2]) {
      expect(grouped[partition]).toHaveLength(100)
      const indices = grouped[partition].map((m) => Number(m.value.split('-')[1]))
      expect(indices).toEqual([...indices].sort((a, b) => a - b))
    }
  })

  it('puts every message in exactly one partition', () => {
    const messages = Array.from({ length: 1000 }, (_, i) => ({ value: `v-${i}` }))
    const partitioner = () => 1

    const grouped = groupMessagesPerPartition({
      topic: 't',
      partitionMetadata,
      messages,
      partitioner,
    })

    expect(Object.keys(grouped)).toEqual(['1'])
    expect(grouped[1]).toHaveLength(1000)
    expect(grouped[1][0]).toBe(messages[0])
    expect(grouped[1][999]).toBe(messages[999])
  })

  it('returns a plain object, not a Map (the producer reads it with Object.keys)', () => {
    const grouped = groupMessagesPerPartition({
      topic: 't',
      partitionMetadata,
      messages: [{ value: 'a' }],
      partitioner: () => 0,
    })
    expect(grouped.constructor).toBe(Object)
  })

  it('returns {} when there is no partition metadata', () => {
    expect(
      groupMessagesPerPartition({
        topic: 't',
        partitionMetadata: [],
        messages: [{ value: 'a' }],
        partitioner: () => 0,
      })
    ).toEqual({})
  })

  it('invokes the partitioner exactly once per message with the expected argument shape', () => {
    const messages = [{ value: 'a' }, { value: 'b' }, { value: 'c' }]
    const partitioner = jest.fn(() => 0)

    groupMessagesPerPartition({
      topic: 't',
      partitionMetadata,
      messages,
      partitioner,
    })

    expect(partitioner).toHaveBeenCalledTimes(messages.length)
    messages.forEach((message, i) => {
      expect(partitioner).toHaveBeenNthCalledWith(i + 1, { topic: 't', partitionMetadata, message })
    })
  })
})
