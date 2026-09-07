const CooperativeStickyAssigner = require('./index')
const { MemberAssignment, MemberMetadata } = require('../../assignerProtocol')
const Encoder = require('../../../protocol/encoder')

const decodeAssignment = (result) =>
  result.reduce(
    (acc, { memberId, memberAssignment }) =>
      Object.assign(acc, { [memberId]: MemberAssignment.decode(memberAssignment).assignment }),
    {}
  )

describe('Consumer > assigners > CooperativeStickyAssigner', () => {
  let cluster, metadata, assigner

  const partitions = (count) =>
    Array(count)
      .fill()
      .map((_, i) => ({ partitionId: i }))

  const member = (memberId, { topics = ['topic-A'], owned = [], generationId = 4 } = {}) => ({
    memberId,
    memberMetadata: MemberMetadata.encode({
      version: 3,
      topics,
      userData: new Encoder().writeInt32(generationId).buffer,
      ownedPartitions: owned,
      generationId,
    }),
  })

  beforeEach(() => {
    metadata = {}
    cluster = { findTopicPartitionMetadata: (topic) => metadata[topic] || [] }
    assigner = CooperativeStickyAssigner({ cluster })
  })

  test('advertises the Java protocol name and puts cooperative first', () => {
    expect(assigner.name).toEqual('cooperative-sticky')
    expect(assigner.supportedProtocols()).toEqual(['cooperative', 'eager'])
  })

  test('sends exactly four bytes of userData: the retained generation', () => {
    expect(assigner.subscriptionUserData()).toEqual(Buffer.from([0xff, 0xff, 0xff, 0xff]))
    expect(assigner.subscriptionUserData()).toHaveLength(4)

    assigner.onAssignment(
      { partitions: [{ topic: 'topic-A', partitions: [0] }], userData: null },
      { generationId: 6, groupProtocol: 'cooperative-sticky' }
    )

    const userData = assigner.subscriptionUserData()
    expect(userData).toHaveLength(4)
    expect(userData.readInt32BE(0)).toEqual(6)

    assigner.onAssignment(null, { generationId: -1, groupProtocol: null })
    expect(assigner.subscriptionUserData().readInt32BE(0)).toEqual(-1)
  })

  test('takes ownership from OwnedPartitions', async () => {
    metadata['topic-A'] = partitions(6)

    const result = await assigner.assign({
      members: [
        member('member-1', { owned: [{ topic: 'topic-A', partitions: [4, 5] }] }),
        member('member-2', { owned: [{ topic: 'topic-A', partitions: [0, 1] }] }),
        member('member-3', { owned: [{ topic: 'topic-A', partitions: [2, 3] }] }),
      ],
      topics: ['topic-A'],
    })

    expect(decodeAssignment(result)).toEqual({
      'member-1': { 'topic-A': [4, 5] },
      'member-2': { 'topic-A': [0, 1] },
      'member-3': { 'topic-A': [2, 3] },
    })
  })

  test('falls back to the userData generation when the subscription carries none', async () => {
    metadata['topic-A'] = partitions(2)

    // A v1 subscription: OwnedPartitions but no GenerationId, so the generation used to settle
    // the double claim can only come from userData.
    const withoutGenerationId = (memberId, owned, generationId) => ({
      memberId,
      memberMetadata: MemberMetadata.encode({
        version: 1,
        topics: ['topic-A'],
        userData: new Encoder().writeInt32(generationId).buffer,
        ownedPartitions: owned,
      }),
    })

    const result = await assigner.assign({
      members: [
        withoutGenerationId('member-1', [{ topic: 'topic-A', partitions: [0, 1] }], 2),
        withoutGenerationId('member-2', [{ topic: 'topic-A', partitions: [0, 1] }], 9),
      ],
      topics: ['topic-A'],
    })

    // member-2 reported the higher generation, so its claim wins and it keeps partition 0 (its
    // quota is one); member-1's claim is dropped and it is filled with partition 1. Without the
    // fallback both would report the default generation, the claims would cancel out, and the
    // partitions would go out in member order as [0] and [1] instead.
    expect(decodeAssignment(result)).toEqual({
      'member-1': { 'topic-A': [1] },
      'member-2': { 'topic-A': [0] },
    })
  })

  test('falls back to the userData generation when the subscription sends GenerationId -1', async () => {
    metadata['topic-A'] = partitions(2)

    // A full v3 subscription whose GenerationId is the wire default. A member that lost its
    // generation sends -1 there while its userData still names the generation it last held, and
    // that is the one the claim has to be settled on.
    const reset = (memberId, owned, generationId) => ({
      memberId,
      memberMetadata: MemberMetadata.encode({
        version: 3,
        topics: ['topic-A'],
        userData: new Encoder().writeInt32(generationId).buffer,
        ownedPartitions: owned,
        generationId: -1,
      }),
    })

    const result = await assigner.assign({
      members: [
        reset('member-1', [{ topic: 'topic-A', partitions: [0, 1] }], 2),
        reset('member-2', [{ topic: 'topic-A', partitions: [0, 1] }], 9),
      ],
      topics: ['topic-A'],
    })

    // Reading the -1 as a real generation would make both claims equal, cancel them out, and hand
    // the partitions out in member order as [0] and [1].
    expect(decodeAssignment(result)).toEqual({
      'member-1': { 'topic-A': [1] },
      'member-2': { 'topic-A': [0] },
    })
  })

  test('treats a peer with neither a GenerationId nor readable userData as the default', async () => {
    metadata['topic-A'] = partitions(2)

    const bare = (memberId, owned) => ({
      memberId,
      memberMetadata: MemberMetadata.encode({ version: 0, topics: ['topic-A'], userData: null }),
      groupInstanceId: null,
      owned,
    })

    const result = await assigner.assign({
      members: [bare('member-1'), bare('member-2')],
      topics: ['topic-A'],
    })

    expect(decodeAssignment(result)).toEqual({
      'member-1': { 'topic-A': [0] },
      'member-2': { 'topic-A': [1] },
    })
  })

  test('accepts a static member and keeps the Java round-robin order', async () => {
    metadata['topic-A'] = partitions(2)

    const result = await assigner.assign({
      members: [member('member-a'), { ...member('member-b'), groupInstanceId: 'instance-a' }],
      topics: ['topic-A'],
    })

    // Java sorts the unfilled members by member id, not by member info, so the static member's
    // instance id decides nothing here - it only orders the pass that reassigns owned partitions.
    expect(decodeAssignment(result)).toEqual({
      'member-a': { 'topic-A': [0] },
      'member-b': { 'topic-A': [1] },
    })
  })

  test('hands over a partition nobody owned in the same round', async () => {
    metadata['topic-A'] = partitions(4)

    const result = await assigner.assign({
      members: [
        member('member-1', { owned: [{ topic: 'topic-A', partitions: [0, 1] }] }),
        member('member-2', { owned: [{ topic: 'topic-A', partitions: [2] }] }),
      ],
      topics: ['topic-A'],
    })

    // Partition 3 was owned by nobody: it is added to a member without being revoked from one,
    // so it is not withheld.
    expect(decodeAssignment(result)).toEqual({
      'member-1': { 'topic-A': [0, 1] },
      'member-2': { 'topic-A': [2, 3] },
    })
  })

  test('withholds a partition that moves between two members', async () => {
    metadata['topic-A'] = partitions(4)

    const joined = [
      member('member-1', { owned: [{ topic: 'topic-A', partitions: [0, 1, 2, 3] }] }),
      member('member-2', { owned: [] }),
    ]

    const first = decodeAssignment(await assigner.assign({ members: joined, topics: ['topic-A'] }))

    // member-2 should end up with two partitions, but both are being revoked from member-1, so
    // neither member holds them this round.
    expect(first['member-1']['topic-A']).toEqual([0, 1])
    expect(first['member-2']).toEqual({})

    const followUp = decodeAssignment(
      await assigner.assign({
        members: [
          member('member-1', {
            owned: [{ topic: 'topic-A', partitions: [0, 1] }],
            generationId: 5,
          }),
          member('member-2', { owned: [], generationId: 5 }),
        ],
        topics: ['topic-A'],
      })
    )

    expect(followUp['member-1']['topic-A']).toEqual([0, 1])
    expect(followUp['member-2']['topic-A']).toEqual([2, 3])
  })

  test('carries the four-byte userData into the subscription', () => {
    const { name, metadata: encoded } = assigner.protocol({
      topics: ['topic-A'],
      ownedPartitions: [{ topic: 'topic-A', partitions: [0] }],
      generationId: 3,
      rackId: null,
    })

    expect(name).toEqual('cooperative-sticky')
    expect(MemberMetadata.decode(encoded)).toEqual({
      version: 3,
      topics: ['topic-A'],
      userData: Buffer.from([0xff, 0xff, 0xff, 0xff]),
      ownedPartitions: [{ topic: 'topic-A', partitions: [0] }],
      generationId: 3,
      rackId: null,
    })
  })
})
