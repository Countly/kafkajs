const fs = require('fs')
const path = require('path')
const range = require('../assigners/rangeAssigner')
const { MemberAssignment, MemberMetadata } = require('../assignerProtocol')
const Kafka = require('../..')
const CASES = [
  'discriminating',
  'copartitioned',
  'missing',
  'nonintersecting',
  'all_replicas',
  'balance',
]
/** @param {string} name vector suffix @returns {object} independent Java bytes and literal assignment */
const vector = (name) =>
  JSON.parse(
    fs.readFileSync(
      path.join(__dirname, `../assigners/fixtures/range/rack_${name}_java_rack.json`),
      'utf8'
    )
  )
/** @param {object} v fixture @param {boolean} rackAware opt-in
 * @param {Map<string, object[]>|undefined} map authoritative round snapshot
 * @returns {Promise<object[]>} encoded member assignments */
const assign = (v, rackAware, map) => {
  const cluster = {
    brokerPool: { metadata: { brokers: v.nodes.map(({ id, rack }) => ({ nodeId: id, rack })) } },
    findTopicPartitionMetadata: (topic) =>
      v.topics[topic].map(({ partition, ...p }) => ({ ...p, partitionId: partition })),
  }
  return range({ cluster, rackAware }).assign({
    topics: Object.keys(v.topics),
    groupTopicMetadata: map,
    members: v.members.map((m) => ({
      memberId: m.memberId,
      groupInstanceId: m.groupInstanceId,
      memberMetadata: Buffer.from(m.subscriptionBytesHex, 'hex'),
    })),
  })
}
/** @param {object[]} result encoded member assignments @returns {object} decoded partition map */
const decode = (result) =>
  Object.fromEntries(
    result.map(({ memberId, memberAssignment }) => [
      memberId,
      MemberAssignment.decode(memberAssignment).assignment,
    ])
  )

describe('Consumer > rack-aware range', () => {
  test.each(CASES)('matches independent Java bytes for %s', async (name) => {
    const v = vector(name)
    const result = await assign(v, true)
    expect(decode(result)).toEqual(v.expected)
    for (const { memberId, memberAssignment } of result)
      expect(memberAssignment.toString('hex')).toBe(v.expectedAssignmentBytesHex[memberId])
  })
  it('discriminates the flag with RF2 while preserving exact quotas', async () => {
    const v = vector('discriminating')
    expect(decode(await assign(v, false))).toEqual({
      'm-1': { 't-a': [0, 1] },
      'm-2': { 't-a': [2, 3] },
    })
    expect(decode(await assign(v, true))).toEqual({
      'm-1': { 't-a': [2, 3] },
      'm-2': { 't-a': [0, 1] },
    })
  })
  test.each(['missing', 'nonintersecting', 'all_replicas'])(
    'keeps the %s entry-rule branch a no-op',
    async (name) => {
      const v = vector(name)
      expect(decode(await assign(v, true))).toEqual(decode(await assign(v, false)))
    }
  )
  it('keeps uniform-rack co-partitioned topics in ordinary ranges when no topic is eligible', async () => {
    const topics = ['a', 'b']
    const v = {
      nodes: [{ id: 0, rack: 'r' }],
      topics: Object.fromEntries(
        topics.map((topic) => [
          topic,
          [0, 1, 2, 3].map((partition) => ({ partition, replicas: [0] })),
        ])
      ),
      members: [
        ['m-1', 'outside'],
        ['m-2', 'r'],
      ].map(([memberId, rackId]) => ({
        memberId,
        subscriptionBytesHex: MemberMetadata.encode({
          version: 3,
          topics,
          ownedPartitions: [],
          generationId: -1,
          rackId,
          userData: null,
        }).toString('hex'),
      })),
    }
    const expected = {
      'm-1': { a: [0, 1], b: [0, 1] },
      'm-2': { a: [2, 3], b: [2, 3] },
    }
    expect(decode(await assign(v, false))).toEqual(expected)
    expect(decode(await assign(v, true))).toEqual(expected)
  })
  it('opens every co-partitioned rack group when a separate topic group is eligible', async () => {
    const topics = ['a', 'b', 'c']
    const v = {
      nodes: [
        { id: 0, rack: 'r' },
        { id: 1, rack: 's' },
      ],
      topics: {
        a: [0, 1, 2, 3].map((partition) => ({ partition, replicas: [0] })),
        b: [0, 1, 2, 3].map((partition) => ({ partition, replicas: [0] })),
        c: [
          { partition: 0, replicas: [1] },
          { partition: 1, replicas: [0] },
        ],
      },
      members: [
        ['m-1', 'outside'],
        ['m-2', 'r'],
      ].map(([memberId, rackId]) => ({
        memberId,
        subscriptionBytesHex: MemberMetadata.encode({
          version: 3,
          topics,
          ownedPartitions: [],
          generationId: -1,
          rackId,
          userData: null,
        }).toString('hex'),
      })),
    }
    expect(decode(await assign(v, false))).toEqual({
      'm-1': { a: [0, 1], b: [0, 1], c: [0] },
      'm-2': { a: [2, 3], b: [2, 3], c: [1] },
    })
    expect(decode(await assign(v, true))).toEqual({
      'm-1': { a: [2, 3], b: [2, 3], c: [0] },
      'm-2': { a: [0, 1], b: [0, 1], c: [1] },
    })
  })
  it('uses the authoritative snapshot for nonleader topics and never fills its holes from stale cache', async () => {
    const v = vector('discriminating')
    const map = new Map([
      ['t-a', v.topics['t-a'].map(({ partition, ...p }) => ({ ...p, partitionId: partition }))],
    ])
    expect(await assign(v, true, map)).toEqual(await assign(v, true))
    expect(decode(await assign(v, true, new Map()))).toEqual({ 'm-1': {}, 'm-2': {} })
  })
  it('leaves a rackless member eligible without exceeding the range balance', async () => {
    const v = vector('discriminating')
    v.members[0].subscriptionBytesHex = MemberMetadata.encode({
      version: 3,
      topics: ['t-a'],
      ownedPartitions: [],
      generationId: -1,
      rackId: null,
      userData: null,
    }).toString('hex')
    const result = decode(await assign(v, true))
    expect(result).toEqual({ 'm-1': { 't-a': [0, 1] }, 'm-2': { 't-a': [2, 3] } })
    expect(result['m-1']['t-a']).toHaveLength(2)
    expect(result['m-2']['t-a']).toHaveLength(2)
    expect(new Set(Object.values(result).flatMap((a) => a['t-a'])).size).toBe(4)
  })
  test.each([null, 0, 1, '', 'true', {}, [], NaN])(
    'rejects non-boolean option %p before connecting',
    (value) => {
      const kafka = new Kafka({ clientId: 'range-options', brokers: ['localhost:1'] })
      expect(() => kafka.consumer({ groupId: 'g', rackAwareAssignment: value })).toThrow(
        'rackAwareAssignment must be a boolean'
      )
    }
  )
  test.each([undefined, false, true])(
    'forwards rack flag %p to a custom factory through the public constructor',
    (value) => {
      const kafka = new Kafka({ clientId: 'range-options', brokers: ['localhost:1'] })
      const factory = jest.fn(() => ({ name: 'custom', version: 0 }))
      kafka.consumer({ groupId: 'g', partitionAssigners: [factory], rackAwareAssignment: value })
      expect(factory).toHaveBeenCalledWith(expect.objectContaining({ rackAware: value === true }))
    }
  )
  it('keeps the flag-off wire bytes from the previous implementation', async () => {
    const result = await assign(vector('discriminating'), false)
    expect(result.map(({ memberAssignment }) => memberAssignment.toString('hex'))).toEqual([
      '0003000000010003742d6100000002000000000000000100000000',
      '0003000000010003742d6100000002000000020000000300000000',
    ])
  })
  it('preserves co-partitioning when topics have contradictory rack matches', async () => {
    const v = vector('discriminating')
    v.topics['t-b'] = v.topics['t-a'].map((p) => ({
      ...p,
      replicas: p.partition < 2 ? [0, 2] : [1, 2],
    }))
    for (const member of v.members) {
      member.subscriptionBytesHex = MemberMetadata.encode({
        version: 3,
        topics: ['t-a', 't-b'],
        ownedPartitions: [],
        generationId: -1,
        rackId: member.rackId,
        userData: null,
      }).toString('hex')
    }
    expect(decode(await assign(v, true))).toEqual({
      'm-1': { 't-a': [0, 1], 't-b': [0, 1] },
      'm-2': { 't-a': [2, 3], 't-b': [2, 3] },
    })
  })
  it('uses follower replica racks even when only the leader is in the ISR', async () => {
    const v = vector('discriminating')
    v.members[0].subscriptionBytesHex = MemberMetadata.encode({
      version: 3,
      topics: ['t-a'],
      ownedPartitions: [],
      generationId: -1,
      rackId: 'rack-c',
      userData: null,
    }).toString('hex')
    v.topics['t-a'] = v.topics['t-a'].map((p) => ({ ...p, isr: [p.leader] }))
    expect(decode(await assign(v, true))).toEqual({
      'm-1': { 't-a': [0, 1] },
      'm-2': { 't-a': [2, 3] },
    })
  })
  it('uses union-answer replica racks instead of a stale global broker-rack cache', async () => {
    const v = vector('discriminating')
    const helper = require('../assigners/partitionMetadata')
    const rows = v.topics['t-a'].map(({ partition, ...p }) => ({ ...p, partitionId: partition }))
    const map = new Map([
      [
        't-a',
        helper.withReplicaRacks(
          rows,
          v.nodes.map(({ id, rack }) => ({ nodeId: id, rack }))
        ),
      ],
    ])
    v.nodes = v.nodes.map((node) => ({ ...node, rack: 'stale-rack' }))
    expect(decode(await assign(v, true, map))).toEqual(v.expected)
    expect(Object.keys(map.get('t-a')[0])).not.toContain('replicaRacks')
  })
  it('captures union racks at the actual classic leader snapshot boundary', async () => {
    const Classic = require('../groupProtocol/classic')
    const v = vector('discriminating')
    const freshBrokers = v.nodes.map(({ id, rack }) => ({ nodeId: id, rack }))
    const partitions = v.topics['t-a'].map(({ partition, ...p }) => ({
      ...p,
      partitionId: partition,
    }))
    const group = {
      logger: { warn: jest.fn() },
      decodeSubscription: Classic.prototype.decodeSubscription,
      cluster: {
        brokerPool: {
          metadata: { brokers: freshBrokers.map((b) => ({ ...b, rack: 'stale-rack' })) },
        },
        findTopicPartitionMetadata: jest.fn(() => []),
        groupMetadata: jest.fn(async () => ({
          brokers: freshBrokers,
          topicMetadata: [{ topic: 't-a', topicErrorCode: 0, partitionMetadata: partitions }],
        })),
      },
    }
    const round = {
      guard: () => null,
      roundDeadlineAt: performance.now() + 30000,
      allowAutoTopicCreation: false,
      wait: (operation) => operation(),
    }
    const members = v.members.map((m) => ({
      memberMetadata: Buffer.from(m.subscriptionBytesHex, 'hex'),
    }))
    const snapshot = await Classic.prototype.leaderMetadata.call(group, members, [], round)
    expect(group.cluster.groupMetadata).toHaveBeenCalledTimes(1)
    v.nodes = v.nodes.map((node) => ({ ...node, rack: 'stale-rack' }))
    expect(decode(await assign(v, true, snapshot.groupTopicMetadata))).toEqual(v.expected)
    expect(group.cluster.findTopicPartitionMetadata).not.toHaveBeenCalled()
  })
})
