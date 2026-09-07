const RangeAssigner = require('./index')
const { MemberAssignment, MemberMetadata } = require('../../assignerProtocol')

/**
 * Decodes the wire result back into `{ memberId: { topic: partitions } }` so a case can assert
 * the assignment it means rather than a buffer.
 */
const decodeAssignment = (result) =>
  result.reduce(
    (acc, { memberId, memberAssignment }) =>
      Object.assign(acc, { [memberId]: MemberAssignment.decode(memberAssignment).assignment }),
    {}
  )

describe('Consumer > assigners > RangeAssigner', () => {
  let cluster, metadata, assigner

  const partitions = (count) =>
    Array(count)
      .fill()
      .map((_, i) => ({ partitionId: i }))

  const member = (memberId, topics, groupInstanceId = null) => ({
    memberId,
    groupInstanceId,
    memberMetadata: MemberMetadata.encode({ version: 3, topics }),
  })

  beforeEach(() => {
    metadata = {}
    cluster = { findTopicPartitionMetadata: (topic) => metadata[topic] || [] }
    assigner = RangeAssigner({ cluster })
  })

  test('advertises the Java protocol name and eager only', () => {
    expect(assigner.name).toEqual('range')
    expect(assigner.supportedProtocols()).toEqual(['eager'])
  })

  test('assigns every partition of a single topic to a single member', async () => {
    metadata['topic-A'] = partitions(3)

    const result = await assigner.assign({
      members: [member('member-1', ['topic-A'])],
      topics: ['topic-A'],
    })

    expect(decodeAssignment(result)).toEqual({ 'member-1': { 'topic-A': [0, 1, 2] } })
  })

  test('splits evenly into contiguous ascending ranges', async () => {
    metadata['topic-A'] = partitions(4)

    const result = await assigner.assign({
      members: [member('member-2', ['topic-A']), member('member-1', ['topic-A'])],
      topics: ['topic-A'],
    })

    expect(decodeAssignment(result)).toEqual({
      'member-1': { 'topic-A': [0, 1] },
      'member-2': { 'topic-A': [2, 3] },
    })
  })

  test('gives the first `partitions % members` members one extra partition', async () => {
    metadata['topic-A'] = partitions(7)

    const result = await assigner.assign({
      members: [
        member('member-1', ['topic-A']),
        member('member-2', ['topic-A']),
        member('member-3', ['topic-A']),
      ],
      topics: ['topic-A'],
    })

    expect(decodeAssignment(result)).toEqual({
      'member-1': { 'topic-A': [0, 1, 2] },
      'member-2': { 'topic-A': [3, 4] },
      'member-3': { 'topic-A': [5, 6] },
    })
  })

  test('gives the surplus members an entry with an empty assignment', async () => {
    metadata['topic-A'] = partitions(2)

    const result = await assigner.assign({
      members: [
        member('member-1', ['topic-A']),
        member('member-2', ['topic-A']),
        member('member-3', ['topic-A']),
        member('member-4', ['topic-A']),
      ],
      topics: ['topic-A'],
    })

    // A member left out of the group assignment is handed a zero-length buffer by the coordinator,
    // which the Java client throws on; an empty assignment is a decodable buffer.
    expect(decodeAssignment(result)).toEqual({
      'member-1': { 'topic-A': [0] },
      'member-2': { 'topic-A': [1] },
      'member-3': {},
      'member-4': {},
    })
    expect(result.map(({ memberId }) => memberId)).toEqual([
      'member-1',
      'member-2',
      'member-3',
      'member-4',
    ])
    expect(result.every(({ memberAssignment }) => memberAssignment.length > 0)).toBe(true)
  })

  test('gives a member subscribed to nothing an entry with an empty assignment', async () => {
    metadata['topic-A'] = partitions(2)

    const result = await assigner.assign({
      members: [member('member-1', ['topic-A']), member('member-2', [])],
      topics: ['topic-A'],
    })

    expect(decodeAssignment(result)).toEqual({
      'member-1': { 'topic-A': [0, 1] },
      'member-2': {},
    })
  })

  test('divides each topic independently', async () => {
    metadata['topic-A'] = partitions(4)
    metadata['topic-B'] = partitions(3)

    const result = await assigner.assign({
      members: [
        member('member-1', ['topic-A', 'topic-B']),
        member('member-2', ['topic-A', 'topic-B']),
      ],
      topics: ['topic-A', 'topic-B'],
    })

    expect(decodeAssignment(result)).toEqual({
      'member-1': { 'topic-A': [0, 1], 'topic-B': [0, 1] },
      'member-2': { 'topic-A': [2, 3], 'topic-B': [2] },
    })
  })

  test('only divides a topic among the members subscribed to it', async () => {
    metadata['topic-A'] = partitions(4)
    metadata['topic-B'] = partitions(2)

    const result = await assigner.assign({
      members: [
        member('member-1', ['topic-A', 'topic-B']),
        member('member-2', ['topic-A']),
        member('member-3', ['topic-A', 'topic-B']),
      ],
      topics: ['topic-A', 'topic-B'],
    })

    expect(decodeAssignment(result)).toEqual({
      'member-1': { 'topic-A': [0, 1], 'topic-B': [0] },
      'member-2': { 'topic-A': [2] },
      'member-3': { 'topic-A': [3], 'topic-B': [1] },
    })
  })

  test('sorts static members first, which changes the ranges they receive', async () => {
    metadata['topic-A'] = partitions(4)

    const members = [member('member-a', ['topic-A']), member('member-b', ['topic-A'], 'instance-z')]

    const result = await assigner.assign({ members, topics: ['topic-A'] })

    expect(decodeAssignment(result)).toEqual({
      'member-b': { 'topic-A': [0, 1] },
      'member-a': { 'topic-A': [2, 3] },
    })

    const withoutInstanceIds = await assigner.assign({
      members: members.map(({ memberId, memberMetadata }) => ({ memberId, memberMetadata })),
      topics: ['topic-A'],
    })

    expect(decodeAssignment(withoutInstanceIds)).not.toEqual(decodeAssignment(result))
  })

  test('skips a topic no member subscribes to', async () => {
    metadata['topic-A'] = partitions(2)
    metadata['topic-unwanted'] = partitions(4)

    const result = await assigner.assign({
      members: [member('member-1', ['topic-A'])],
      topics: ['topic-A', 'topic-unwanted'],
    })

    expect(decodeAssignment(result)).toEqual({ 'member-1': { 'topic-A': [0, 1] } })
  })

  describe('#subscriptionUserData and #protocol', () => {
    test('sends the wire NULL as userData, which is what Java sends', () => {
      expect(assigner.subscriptionUserData()).toBeNull()

      const { name, metadata: encoded } = assigner.protocol({
        topics: ['topic-A'],
        ownedPartitions: [],
        generationId: -1,
        rackId: null,
      })

      expect(name).toEqual('range')
      expect(MemberMetadata.decode(encoded)).toEqual({
        version: 3,
        topics: ['topic-A'],
        userData: null,
        ownedPartitions: [],
        generationId: -1,
        rackId: null,
      })
    })
  })
})
