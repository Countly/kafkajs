const StickyAssigner = require('./index')
const { stickyUserData } = require('./stickyUserData')
const { DEFAULT_GENERATION } = require('./stickyCore')
const { MemberAssignment, MemberMetadata } = require('../../assignerProtocol')
const Encoder = require('../../../protocol/encoder')

const decodeAssignment = (result) =>
  result.reduce(
    (acc, { memberId, memberAssignment }) =>
      Object.assign(acc, { [memberId]: MemberAssignment.decode(memberAssignment).assignment }),
    {}
  )

describe('Consumer > assigners > stickyUserData', () => {
  test('round trips a V1 payload', () => {
    const data = {
      assignment: [
        { topic: 'topic-A', partitions: [0, 2] },
        { topic: 'topic-B', partitions: [1] },
      ],
      generationId: 9,
    }

    expect(stickyUserData.decode(stickyUserData.encode(data))).toEqual(data)
  })

  test('reads a V0 payload - the array with no trailing generation - as the default generation', () => {
    const v0 = new Encoder().writeArray([
      new Encoder().writeString('topic-A').writeArray([3, 4]),
    ]).buffer

    expect(stickyUserData.decode(v0)).toEqual({
      assignment: [{ topic: 'topic-A', partitions: [3, 4] }],
      generationId: DEFAULT_GENERATION,
    })
  })

  test('reads a null, empty or unreadable buffer as an empty assignment', () => {
    const empty = { assignment: [], generationId: DEFAULT_GENERATION }

    expect(stickyUserData.decode(null)).toEqual(empty)
    expect(stickyUserData.decode(Buffer.alloc(0))).toEqual(empty)
    expect(stickyUserData.decode(Buffer.from([0x00, 0x00, 0x00, 0x05]))).toEqual(empty)
  })

  test('encodes an empty assignment as bytes, which is not the same as no userData at all', () => {
    const encoded = stickyUserData.encode({ assignment: [], generationId: 4 })

    expect(encoded).toEqual(Buffer.from([0, 0, 0, 0, 0, 0, 0, 4]))
    expect(stickyUserData.decode(encoded)).toEqual({ assignment: [], generationId: 4 })
  })
})

describe('Consumer > assigners > StickyAssigner', () => {
  let cluster, metadata, assigner

  const partitions = (count) =>
    Array(count)
      .fill()
      .map((_, i) => ({ partitionId: i }))

  const member = (memberId, { topics = ['topic-A'], owned = null, generationId = 0 } = {}) => ({
    memberId,
    memberMetadata: MemberMetadata.encode({
      version: 3,
      topics,
      userData: owned === null ? null : stickyUserData.encode({ assignment: owned, generationId }),
      ownedPartitions: [],
      generationId,
    }),
  })

  beforeEach(() => {
    metadata = {}
    cluster = { findTopicPartitionMetadata: (topic) => metadata[topic] || [] }
    assigner = StickyAssigner({ cluster })
  })

  test('advertises the Java protocol name and eager only', () => {
    expect(assigner.name).toEqual('sticky')
    expect(assigner.supportedProtocols()).toEqual(['eager'])
  })

  test('accepts a static member alongside a dynamic one', async () => {
    metadata['topic-A'] = partitions(4)

    const result = await assigner.assign({
      members: [{ ...member('member-a'), groupInstanceId: 'instance-z' }, member('member-b')],
      topics: ['topic-A'],
    })

    // The unfilled members are filled in member-id order, which is what Java does; the instance
    // id orders only the pass that reassigns owned partitions.
    expect(decodeAssignment(result)).toEqual({
      'member-a': { 'topic-A': [0, 2] },
      'member-b': { 'topic-A': [1, 3] },
    })
  })

  test('keeps each member on the partitions its userData claims', async () => {
    metadata['topic-A'] = partitions(6)

    const result = await assigner.assign({
      members: [
        member('member-1', { owned: [{ topic: 'topic-A', partitions: [4, 5] }], generationId: 3 }),
        member('member-2', { owned: [{ topic: 'topic-A', partitions: [0, 1] }], generationId: 3 }),
        member('member-3', { owned: [{ topic: 'topic-A', partitions: [2, 3] }], generationId: 3 }),
      ],
      topics: ['topic-A'],
    })

    expect(decodeAssignment(result)).toEqual({
      'member-1': { 'topic-A': [4, 5] },
      'member-2': { 'topic-A': [0, 1] },
      'member-3': { 'topic-A': [2, 3] },
    })
  })

  test('ignores a populated OwnedPartitions when the userData is empty', async () => {
    metadata['topic-A'] = partitions(4)

    const withOwnedPartitionsOnly = (memberId, ownedPartitions) => ({
      memberId,
      memberMetadata: MemberMetadata.encode({
        version: 3,
        topics: ['topic-A'],
        userData: Buffer.alloc(0),
        ownedPartitions,
        generationId: 7,
      }),
    })

    const result = await assigner.assign({
      members: [
        withOwnedPartitionsOnly('member-1', [{ topic: 'topic-A', partitions: [2, 3] }]),
        withOwnedPartitionsOnly('member-2', [{ topic: 'topic-A', partitions: [0, 1] }]),
      ],
      topics: ['topic-A'],
    })

    // Ownership came from nowhere, so the partitions go out round-robin from scratch. Honouring
    // OwnedPartitions would instead have produced [2, 3] and [0, 1].
    expect(decodeAssignment(result)).toEqual({
      'member-1': { 'topic-A': [0, 2] },
      'member-2': { 'topic-A': [1, 3] },
    })
  })

  describe('#subscriptionUserData and #onAssignment', () => {
    test('sends the wire NULL until the member has ever been assigned anything', () => {
      expect(assigner.subscriptionUserData()).toBeNull()

      const { metadata: encoded } = assigner.protocol({
        topics: ['topic-A'],
        ownedPartitions: [],
        generationId: -1,
        rackId: null,
      })

      expect(MemberMetadata.decode(encoded).userData).toBeNull()
    })

    test('carries the retained assignment into the next subscription', () => {
      assigner.onAssignment(
        { partitions: [{ topic: 'topic-A', partitions: [0, 1] }], userData: null },
        { generationId: 12, groupProtocol: 'sticky' }
      )

      expect(stickyUserData.decode(assigner.subscriptionUserData())).toEqual({
        assignment: [{ topic: 'topic-A', partitions: [0, 1] }],
        generationId: 12,
      })

      const { metadata: encoded } = assigner.protocol({
        topics: ['topic-A'],
        ownedPartitions: [],
        generationId: 12,
        rackId: null,
      })

      expect(stickyUserData.decode(MemberMetadata.decode(encoded).userData)).toEqual({
        assignment: [{ topic: 'topic-A', partitions: [0, 1] }],
        generationId: 12,
      })
    })

    test('forgets everything when the assignment is cleared', () => {
      assigner.onAssignment(
        { partitions: [{ topic: 'topic-A', partitions: [0] }], userData: null },
        { generationId: 12, groupProtocol: 'sticky' }
      )
      assigner.onAssignment(null, { generationId: -1, groupProtocol: null })

      expect(assigner.subscriptionUserData()).toBeNull()
    })
  })
})
