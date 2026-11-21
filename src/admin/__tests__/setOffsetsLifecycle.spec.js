// The admin's internal consumers are real; only their group description is stubbed (the scripted
// cluster has no DescribeGroups pair), and each one is recorded with the cluster it was given.
const mockMembers = []
const mockClusters = new Set()
let mockDescription = { state: 'Empty', members: [] }
jest.mock('../../consumer', () => {
  const createConsumer = jest.requireActual('../../consumer')
  return (params) => {
    mockClusters.add(params.cluster)
    const consumer = Object.assign(createConsumer(params), {
      describeGroup: async () => mockDescription,
    })
    mockMembers.push({ consumer, cluster: params.cluster })
    return consumer
  }
})

// A test can stall the disposal of the admin's internal consumers by setting a gate here.
let mockDisposalGate = null
jest.mock('../../consumer/disposal', () => {
  const disposal = jest.requireActual('../../consumer/disposal')
  return {
    ...disposal,
    disposeConsumer: async (consumer) => {
      if (mockDisposalGate) await mockDisposalGate.wait()
      return disposal.disposeConsumer(consumer)
    },
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
    mockClusters.clear()
    mockDescription = { state: 'Empty', members: [] }
    mockDisposalGate = null
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
        partitions: [{ partition: 0, offset: '-5' }],
      })
    ).rejects.toThrow('Offset must not be a negative number')

    expect(listeners()).toBe(0)
    // The disposal asks a start that is still connecting to stop first, so it may never join;
    // a join that did happen is always left again.
    const apis = scripted.requests().map(({ api }) => api)
    expect(apis.filter((api) => api === 'LeaveGroup')).toHaveLength(
      apis.filter((api) => api === 'JoinGroup').length
    )
  })

  test('an entry with a partition that is not a partition id is refused before the internal consumer starts', async () => {
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

    expect({
      consumers: mockMembers.length,
      requests: scripted
        .requests()
        .slice(before)
        .map(({ api }) => api),
    }).toEqual({ consumers: 0, requests: [] })
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

  describe('the topic is a target of the admin only while the call runs', () => {
    /** @returns {object} the admin's cluster, as its internal consumers were given it */
    const adminCluster = () => {
      expect(mockClusters.size).toBe(1)
      return [...mockClusters][0]
    }
    const rejectCommits = () =>
      scripted.answer('OffsetCommit', ({ topics }) => ({
        topics: topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({ partition, errorCode: 30 })),
        })),
      }))

    test('after success, a running group and a crashed member', async () => {
      const gate = scripted.schedule.hold({ api: 'OffsetCommit' })
      let settled = false
      const reset = admin
        .resetOffsets({ groupId: 'reset-group', topic: TOPIC })
        .finally(() => (settled = true))
      await gate.arrived()
      await new Promise((resolve) => setTimeout(resolve, 50))
      expect(settled).toBe(false)
      expect([...adminCluster().targetHolds]).toEqual([[TOPIC, 1]])
      expect([...adminCluster().targetTopics]).toEqual([TOPIC])
      gate.release()
      await reset
      expect([...adminCluster().targetTopics]).toEqual([])

      mockDescription = { state: 'Stable', members: [{ memberId: 'other' }] }
      await expect(admin.resetOffsets({ groupId: 'busy-group', topic: TOPIC })).rejects.toThrow(
        'current state: Stable'
      )
      expect([...adminCluster().targetTopics]).toEqual([])

      mockDescription = { state: 'Empty', members: [] }
      rejectCommits()
      await expect(
        admin.setOffsets({
          groupId: 'set-group',
          topic: TOPIC,
          partitions: [{ partition: 0, offset: '3' }],
        })
      ).rejects.toMatchObject({ type: 'GROUP_AUTHORIZATION_FAILED' })
      expect([...adminCluster().targetTopics]).toEqual([])
      expect([...adminCluster().targetHolds]).toEqual([])
    })

    test('a seek commit that never gets an answer keeps the topic a target until the call ends', async () => {
      await admin.disconnect()
      admin = new Kafka(scripted.kafkaOptions({ requestTimeout: 300 })).admin()
      await admin.connect()
      const gate = scripted.schedule.hold({ api: 'OffsetCommit' })
      let settled = false
      const set = admin
        .setOffsets({
          groupId: 'set-group',
          topic: TOPIC,
          partitions: [{ partition: 0, offset: '3' }],
        })
        .then(
          () => null,
          (e) => e
        )
        .finally(() => (settled = true))
      await gate.arrived()
      await new Promise((resolve) => setTimeout(resolve, 200))
      expect(settled).toBe(false)
      expect([...adminCluster().targetHolds]).toEqual([[TOPIC, 1]])
      expect([...adminCluster().targetTopics]).toEqual([TOPIC])

      await set
      expect([...adminCluster().targetHolds]).toEqual([])
      expect([...adminCluster().targetTopics]).toEqual([])
      gate.release()
    })

    test('a member that cannot be built takes no hold', async () => {
      await expect(
        admin.setOffsets({ groupId: {}, topic: TOPIC, partitions: [{ partition: 0, offset: '3' }] })
      ).rejects.toThrow('Consumer groupId must be a non-empty string.')

      expect([...adminCluster().targetHolds]).toEqual([])
      expect([...adminCluster().targetTopics]).toEqual([])
    })

    test('past the API deadline the topic stays a target until the member is disposed', async () => {
      await admin.disconnect()
      admin = new Kafka(scripted.kafkaOptions()).admin({ apiTimeout: 300 })
      await admin.connect()
      const gate = scripted.schedule.hold({ api: 'OffsetCommit' })

      const reset = admin.resetOffsets({ groupId: 'reset-group', topic: TOPIC }).catch((e) => e)
      await gate.arrived()
      expect(await reset).toMatchObject({ name: 'KafkaJSTimeout' })
      gate.release()
      for (let n = 0; n < 100 && adminCluster().targetHolds.size > 0; n++) {
        await new Promise((resolve) => setTimeout(resolve, 20))
      }

      expect([...adminCluster().targetHolds]).toEqual([])
      expect([...adminCluster().targetTopics]).toEqual([])
    })

    /** @returns {object} a gate that stalls every disposal until released, and reports arrival */
    const disposalGate = () => {
      let arrive
      let release
      const arrived = new Promise((resolve) => (arrive = resolve))
      const released = new Promise((resolve) => (release = resolve))
      return {
        arrived,
        release,
        wait: () => {
          arrive()
          return released
        },
      }
    }

    test('a refused call keeps the topic a target until its member is disposed', async () => {
      mockDescription = { state: 'Stable', members: [{ memberId: 'other' }] }
      mockDisposalGate = disposalGate()
      let settled = false
      const set = admin
        .setOffsets({
          groupId: 'set-group',
          topic: TOPIC,
          partitions: [{ partition: 0, offset: '3' }],
        })
        .finally(() => (settled = true))
      await mockDisposalGate.arrived
      await new Promise((resolve) => setTimeout(resolve, 20))

      expect(settled).toBe(false)
      expect([...adminCluster().targetHolds]).toEqual([[TOPIC, 1]])
      expect([...adminCluster().targetTopics]).toEqual([TOPIC])
      mockDisposalGate.release()
      await expect(set).rejects.toThrow('current state: Stable')
      expect([...adminCluster().targetHolds]).toEqual([])
      expect([...adminCluster().targetTopics]).toEqual([])
    })

    test('a disconnect releases the hold of a call whose member disposal never ends, and that call releasing later does nothing', async () => {
      mockDescription = { state: 'Stable', members: [{ memberId: 'other' }] }
      const gate = disposalGate()
      mockDisposalGate = gate
      const set = admin
        .setOffsets({
          groupId: 'set-group',
          topic: TOPIC,
          partitions: [{ partition: 0, offset: '3' }],
        })
        .catch((e) => e)
      await gate.arrived
      const cluster = adminCluster()
      expect([...cluster.targetHolds]).toEqual([[TOPIC, 1]])

      await admin.disconnect({ timeout: 0 })
      expect([...cluster.targetHolds]).toEqual([])
      expect([...cluster.targetTopics]).toEqual([])
      expect(await set).toBeInstanceOf(Error)

      mockDisposalGate = null
      await admin.connect()
      const fresh = cluster.holdTargetTopics([TOPIC])
      cluster.targetTopics.add(TOPIC)
      gate.release()
      await new Promise((resolve) => setTimeout(resolve, 20))
      expect([...cluster.targetHolds]).toEqual([[TOPIC, 1]])
      expect([...cluster.targetTopics]).toEqual([TOPIC])
      fresh()
      expect([...cluster.targetTopics]).toEqual([])
    })

    test('two overlapping calls for different groups keep the topic a target until the second ends', async () => {
      const gate = scripted.schedule.hold({
        api: 'JoinGroup',
        match: ({ fields }) => fields.groupId === 'second-group',
      })
      const second = admin.resetOffsets({ groupId: 'second-group', topic: TOPIC })
      await gate.arrived()

      await admin.resetOffsets({ groupId: 'first-group', topic: TOPIC })
      expect([...adminCluster().targetTopics]).toEqual([TOPIC])
      expect(adminCluster().targetHolds.get(TOPIC)).toBe(1)

      gate.release()
      await second
      expect([...adminCluster().targetTopics]).toEqual([])
    })
  })
})
