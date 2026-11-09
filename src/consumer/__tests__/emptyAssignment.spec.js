const { MemberMetadata, MemberAssignment } = require('../assignerProtocol')

const createConsumer = require('../index')
const { applySyncAnswer } = require('./fixtures/emptySyncApply')

const {
  secureRandom,
  createCluster,
  createTopic,
  newLogger,
  waitFor,
  waitForNextEvent,
} = require('testHelpers')

describe('Consumer', () => {
  let topic, groupId, consumer1, consumer2

  beforeEach(async () => {
    topic = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic, partitions: 1 })
  })

  afterEach(async () => {
    consumer1 && (await consumer1.disconnect())
    consumer2 && (await consumer2.disconnect())
  })

  test('remains in the group without receiving any assignment', async () => {
    // Assigns all topic-partitions to the first member.
    const UnbalancedAssigner = ({ cluster }) => ({
      name: 'UnbalancedAssigner',
      version: 1,
      async assign({ members, topics, userData }) {
        const sortedMembers = members.map(({ memberId }) => memberId).sort()
        const firstMember = sortedMembers[0]
        const assignment = {
          [firstMember]: {},
        }

        topics.forEach((topic) => {
          const partitionMetadata = cluster.findTopicPartitionMetadata(topic)
          const partitions = partitionMetadata.map((m) => m.partitionId)
          assignment[firstMember][topic] = partitions
        })

        return Object.keys(assignment).map((memberId) => ({
          memberId,
          memberAssignment: MemberAssignment.encode({
            version: this.version,
            assignment: assignment[memberId],
            userData,
          }),
        }))
      },
      protocol({ topics, userData }) {
        return {
          name: this.name,
          metadata: MemberMetadata.encode({
            version: this.version,
            topics,
            userData,
          }),
        }
      },
    })

    consumer1 = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 1,
      logger: newLogger(),
      partitionAssigners: [UnbalancedAssigner],
    })

    consumer2 = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 1,
      logger: newLogger(),
      partitionAssigners: [UnbalancedAssigner],
    })

    await Promise.all([consumer1.connect(), consumer2.connect()])

    consumer1.subscribe({ topic })
    consumer2.subscribe({ topic })

    // A first join can belong to a solo generation, before the other member joins and
    // changes which member sorts first. Compare assignments from the same generation.
    const joins = [new Map(), new Map()]
    let crash, groupJoinEvents
    const removeListeners = [consumer1, consumer2].flatMap((consumer, index) => [
      consumer.on(consumer.events.GROUP_JOIN, (event) => {
        // GROUP_JOIN has no generation field; groupMetadata() is recorded before its delivery.
        joins[index].set(consumer.groupMetadata().generationId, event)
      }),
      consumer.on(consumer.events.CRASH, ({ payload }) => {
        crash = payload.error
      }),
    ])

    try {
      consumer1.run({ eachMessage: () => {} })
      consumer2.run({ eachMessage: () => {} })

      groupJoinEvents = await waitFor(
        () => {
          if (crash) throw crash
          for (const [generationId, event1] of joins[0]) {
            const event2 = joins[1].get(generationId)
            if (event2) return [event1, event2]
          }
          return false
        },
        {
          ignoreTimeout: false,
          maxWait: 10000,
          timeoutMessage: `Timeout waiting for both consumers in ${groupId} to join the same generation`,
        }
      )
    } finally {
      removeListeners.forEach((remove) => remove())
    }

    const emptyAssignments = groupJoinEvents.filter(
      ({ payload }) => Object.entries(payload.memberAssignment).length === 0
    )
    expect(emptyAssignments).toHaveLength(1)

    await Promise.all(
      [consumer1, consumer2].map((consumer) => waitForNextEvent(consumer, consumer.events.FETCH))
    )

    // Both consumers should continue to heartbeat even without receiving any assignments
    await Promise.all(
      [consumer1, consumer2].map((consumer) =>
        waitForNextEvent(consumer, consumer.events.HEARTBEAT)
      )
    )
  })
})

describe('Consumer > valid empty Java assignments', () => {
  const vector = require('../assigners/fixtures/range/empty_range_java.json')

  test.each(['m-1', 'm-2'])(
    '%s has a valid empty assignment, distinct from an absent payload',
    (memberId) => {
      const bytes = Buffer.from(vector.expectedAssignmentBytesHex[memberId], 'hex')
      expect(vector.expected[memberId]).toEqual({})
      expect(bytes.toString('hex')).toBe('000300000000ffffffff')
      expect(MemberAssignment.decode(bytes)).toEqual({ version: 3, assignment: {}, userData: null })
      expect(MemberAssignment.encode({ version: 3, assignment: {}, userData: null })).toEqual(bytes)
      expect(MemberAssignment.decode(Buffer.alloc(0))).toBeNull()
    }
  )

  test('a valid empty assignment from the wire is applied by the real group without a halt', async () => {
    const seen = await applySyncAnswer(vector.expectedAssignmentBytesHex['m-1'])
    expect(seen.crashes).toEqual([])
    expect(seen.assigned).toEqual([[]])
    expect(seen.joins[0].memberAssignment).toEqual({})
    expect(seen.fetchesAfter).toBe(0)
    expect(seen.heartbeats).toBeGreaterThan(0)
  })
})
