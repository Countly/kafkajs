// The admin's internal consumers are real; only their group description is stubbed (the scripted
// cluster has no DescribeGroups pair), and each one is recorded with the cluster it was given.
const mockMembers = []
let mockDescription = { state: 'Empty', members: [] }
jest.mock('../../consumer', () => {
  const createConsumer = jest.requireActual('../../consumer')
  return (params) => {
    const consumer = Object.assign(createConsumer(params), {
      describeGroup: async () => mockDescription,
    })
    mockMembers.push({ consumer, cluster: params.cluster })
    return consumer
  }
})

const { Kafka } = require('../../../index')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')
const { CLUSTER_TOPIC_RECREATED } = require('../../cluster/instrumentationEvents')

const TOPICS = {
  'topic-a': 'a1a2a3a4-a5a6-a7a8-a9aa-abacadaeafb0',
  'topic-b': 'b1b2b3b4-b5b6-b7b8-b9ba-bbbcbdbebfc0',
}

describe('Admin > setOffsets > the internal consumer is released on every exit', () => {
  let scripted, admin

  beforeEach(async () => {
    mockMembers.length = 0
    mockDescription = { state: 'Empty', members: [] }
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
    await admin.disconnect()
    scripted.assertClean()
  })

  /**
   * @returns {number} how many topic-recreation listeners the admin's cluster holds
   */
  const listeners = () =>
    mockMembers[0].cluster.instrumentationEmitter.emitter.listenerCount(CLUSTER_TOPIC_RECREATED)

  test('two resets leave no listener on the shared cluster', async () => {
    await admin.resetOffsets({ groupId: 'reset-group', topic: TOPIC })
    await admin.resetOffsets({ groupId: 'reset-group', topic: TOPIC, earliest: true })

    expect(listeners()).toBe(0)
    expect(mockMembers[0].cluster.isConnected()).toBe(true)
  })

  test('a rejected sentinel commit leaves no listener', async () => {
    scripted.answer('OffsetCommit', ({ topics }) => ({
      topics: topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({ partition, errorCode: 30 })),
      })),
    }))

    await expect(
      admin.resetOffsets({ groupId: 'reset-group', topic: TOPIC })
    ).rejects.toMatchObject({ type: 'GROUP_AUTHORIZATION_FAILED' })

    expect(listeners()).toBe(0)
  })

  test('a group that is still running is rejected and leaves no listener', async () => {
    mockDescription = { state: 'Stable', members: [{ memberId: 'other' }] }

    await expect(admin.resetOffsets({ groupId: 'busy-group', topic: TOPIC })).rejects.toThrow(
      'The consumer group must have no running instances, current state: Stable'
    )

    expect(listeners()).toBe(0)
  })

  test('a rejected subscribe leaves no listener', async () => {
    await expect(
      admin.setOffsets({
        groupId: 'set-group',
        topic: 5,
        partitions: [{ partition: 0, offset: '1' }],
      })
    ).rejects.toThrow('Invalid topic 5')

    expect(listeners()).toBe(0)
  })

  test('a rejected seek while the consumer joins leaves no listener and no member', async () => {
    await expect(
      admin.setOffsets({
        groupId: 'set-group',
        topic: TOPIC,
        partitions: [{ partition: 'x', offset: '1' }],
      })
    ).rejects.toThrow('Invalid partition')

    expect(listeners()).toBe(0)
    expect(scripted.requests().map(({ api }) => api)).toContain('LeaveGroup')
  })

  test('a reset followed by admin.disconnect() leaves no listener', async () => {
    await admin.resetOffsets({ groupId: 'reset-group', topic: TOPIC })
    await admin.disconnect()

    expect(listeners()).toBe(0)
  })

  describe('with several topics at once on the one admin', () => {
    beforeEach(() => {
      scripted
        .answer('Metadata', ({ topics }) => ({
          brokers: [{ nodeId: 0, host: 'node', port: 9092, rack: null }],
          controllerId: 0,
          topics: Object.entries(TOPICS)
            .filter(
              ([name]) => !topics || topics.length === 0 || topics.some((t) => t.name === name)
            )
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
        .answer('SyncGroup', ({ memberId, groupAssignment }) => ({
          memberAssignment: groupAssignment.find((entry) => entry.memberId === memberId)
            .memberAssignment,
        }))
    })

    const leaves = () => scripted.requests().filter(({ api }) => api === 'LeaveGroup').length

    test('fetchOffsets resolving two topics releases the consumer of each', async () => {
      const resolved = await admin.fetchOffsets({
        groupId: 'resolve-group',
        topics: ['topic-a', 'topic-b'],
        resolveOffsets: true,
      })

      expect(resolved.map(({ topic }) => topic).sort()).toEqual(['topic-a', 'topic-b'])
      expect(mockMembers).toHaveLength(2)
      expect(listeners()).toBe(0)
      expect(leaves()).toBe(2)
    })

    test('two resolving fetchOffsets and a setOffsets side by side on one admin all complete and release', async () => {
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
      expect(mockMembers).toHaveLength(4)
      expect(listeners()).toBe(0)
      expect(leaves()).toBe(4)
      expect(mockMembers[0].cluster.isConnected()).toBe(true)
      expect(
        await admin.fetchOffsets({ groupId: 'resolve-group', topics: ['topic-a'] })
      ).toHaveLength(1)
    })
  })
})
