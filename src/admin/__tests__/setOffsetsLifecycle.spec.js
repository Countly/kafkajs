const { Kafka } = require('../../../index')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')
const { describeGroupAs } = require('./fixtures/describeGroupAs')

/** The requests a member of the group would send; the admin sends none of them. */
const MEMBERSHIP = ['JoinGroup', 'SyncGroup', 'Heartbeat', 'LeaveGroup', 'Fetch']

const TOPICS = {
  'topic-a': 'a1a2a3a4-a5a6-a7a8-a9aa-abacadaeafb0',
  'topic-b': 'b1b2b3b4-b5b6-b7b8-b9ba-bbbcbdbebfc0',
}

describe('Admin > setOffsets > commits without a group member', () => {
  let scripted, admin, described

  beforeEach(async () => {
    described = describeGroupAs()
    scripted = groupScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node', port: 9092 }] }),
      {
        committed: { 0: '13' },
      }
    )
    admin = new Kafka(scripted.kafkaOptions()).admin()
    await admin.connect()
  })

  afterEach(async () => {
    described.restore()
    await admin.disconnect()
    scripted.assertClean()
  })

  const apis = () => scripted.requests().map(({ api }) => api)
  const commits = () => scripted.requests().filter(({ api }) => api === 'OffsetCommit')

  test('a group that is still running is rejected before any commit and joins nothing', async () => {
    described.restore()
    described = describeGroupAs({ classic: { state: 'Stable', members: [] } })

    await expect(admin.resetOffsets({ groupId: 'busy-group', topic: TOPIC })).rejects.toThrow(
      'The consumer group must have no running instances, current state: Stable'
    )

    expect(commits()).toHaveLength(0)
    expect(apis().filter((api) => MEMBERSHIP.includes(api))).toEqual([])
  })

  test('an entry with a partition that is not a partition id is refused before any request', async () => {
    const before = scripted.requests().length

    await expect(
      admin.setOffsets({
        groupId: 'set-group',
        topic: TOPIC,
        partitions: [
          { partition: 0, offset: '7' },
          { partition: 1.5, offset: '-2' },
        ],
      })
    ).rejects.toThrow('Invalid partition')

    expect(
      scripted
        .requests()
        .slice(before)
        .map(({ api }) => api)
    ).toEqual([])
  })

  describe('with several topics at once on the one admin', () => {
    beforeEach(() => {
      scripted.answer('Metadata', ({ topics }) => ({
        brokers: [{ nodeId: 0, host: 'node', port: 9092, rack: null }],
        controllerId: 0,
        topics: Object.entries(TOPICS)
          .filter(([name]) => !topics || topics.length === 0 || topics.some((t) => t.name === name))
          .map(([name, topicId]) => ({
            errorCode: 0,
            name,
            topicId,
            isInternal: false,
            topicAuthorizedOperations: -2147483648,
            partitions: [
              {
                errorCode: 0,
                partitionIndex: 0,
                leaderId: 0,
                leaderEpoch: 5,
                replicas: [0],
                isr: [0],
                offlineReplicas: [],
              },
            ],
          })),
      }))
    })

    /** @returns {Array<[string, string]>} the group and topic of every commit, sorted */
    const committed = () =>
      commits()
        .flatMap(({ fields }) => fields.topics.map(({ topic }) => [fields.groupId, topic]))
        .sort()

    test('fetchOffsets resolving two topics commits each topic once and joins nothing', async () => {
      const resolved = await admin.fetchOffsets({
        groupId: 'resolve-group',
        topics: ['topic-a', 'topic-b'],
        resolveOffsets: true,
      })

      expect(resolved.map(({ topic }) => topic).sort()).toEqual(['topic-a', 'topic-b'])
      expect(committed()).toEqual([
        ['resolve-group', 'topic-a'],
        ['resolve-group', 'topic-b'],
      ])
      expect(apis().filter((api) => MEMBERSHIP.includes(api))).toEqual([])
    })

    test('two resolving fetchOffsets and a setOffsets side by side on one admin all complete, each with its own commit', async () => {
      const [resolved, , set] = await Promise.all([
        admin.fetchOffsets({
          groupId: 'resolve-group',
          topics: ['topic-a', 'topic-b'],
          resolveOffsets: true,
        }),
        admin.fetchOffsets({
          groupId: 'other-resolve-group',
          topics: ['topic-a'],
          resolveOffsets: true,
        }),
        admin.setOffsets({
          groupId: 'set-group',
          topic: 'topic-b',
          partitions: [{ partition: 0, offset: '7' }],
        }),
      ])

      expect(resolved.map(({ topic }) => topic).sort()).toEqual(['topic-a', 'topic-b'])
      expect(set).toBeUndefined()
      expect(committed()).toEqual([
        ['other-resolve-group', 'topic-a'],
        ['resolve-group', 'topic-a'],
        ['resolve-group', 'topic-b'],
        ['set-group', 'topic-b'],
      ])
      expect(apis().filter((api) => MEMBERSHIP.includes(api))).toEqual([])
      expect(
        await admin.fetchOffsets({ groupId: 'resolve-group', topics: ['topic-a'] })
      ).toHaveLength(1)
    })
  })
})
