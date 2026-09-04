const createConsumer = require('../index')
const createAdmin = require('../../admin')
const {
  secureRandom,
  createCluster,
  createTopic,
  newLogger,
  waitForConsumerToJoinGroup,
} = require('testHelpers')

describe('Consumer > static membership (KIP-345) > integration', () => {
  let topicName, groupId, groupInstanceId, admin, consumer, second

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`
    groupInstanceId = `instance-${secureRandom()}`
    await createTopic({ topic: topicName, partitions: 2 })
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
    await admin.connect()
  })

  afterEach(async () => {
    second && (await second.disconnect().catch(() => {}))
    consumer && (await consumer.disconnect().catch(() => {}))
    admin && (await admin.disconnect())
  })

  const makeConsumer = () =>
    createConsumer({
      cluster: createCluster(),
      groupId,
      groupInstanceId,
      logger: newLogger(),
      sessionTimeout: 30000,
      maxWaitTimeInMs: 100,
    })

  it('keeps its slot in the group after disconnect (no LeaveGroup)', async () => {
    consumer = makeConsumer()
    await consumer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
    // Not awaited: consumer#run only resolves after the initial join completes and GROUP_JOIN
    // has already fired, so awaiting it here would race waitForConsumerToJoinGroup's listener
    // against an event that already happened (matches the pattern used throughout this suite,
    // e.g. consumeMessages.spec.js).
    consumer.run({ eachMessage: async () => {} })
    await waitForConsumerToJoinGroup(consumer)

    await consumer.disconnect()
    consumer = null

    // A dynamic member would have sent LeaveGroup and the group would be Empty immediately.
    // A static member holds its slot until sessionTimeout expires.
    //
    // This fork implements DescribeGroups v0-v2; group_instance_id on members only arrives in
    // v4 (deferred to a later phase), so v2's response can't name the member by
    // groupInstanceId. Assert on group shape instead: a dynamic member's disconnect would have
    // made the group Empty with zero members immediately.
    const [description] = await admin.describeGroups([groupId]).then((r) => r.groups)
    expect(description.members.length).toEqual(1)
    expect(description.state).not.toEqual('Empty')
  }, 60000)

  it('fences the first consumer once a second reuses the same groupInstanceId', async () => {
    // KIP-345: a duplicate groupInstanceId does not reject the *new* joiner - the coordinator
    // replaces the group.instance.id -> member.id mapping immediately, so `second` joins
    // successfully, and it is the *older* member (`consumer`) that gets fenced on its next
    // heartbeat (verified directly against a live broker: the coordinator's Heartbeat error
    // message reads "another consumer with the same group instance id has registered with a
    // different member id").
    const crashes = []
    consumer = makeConsumer()
    consumer.on(consumer.events.CRASH, (e) => crashes.push(e.payload))
    await consumer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
    consumer.run({ eachMessage: async () => {} })
    await waitForConsumerToJoinGroup(consumer)

    second = makeConsumer()
    await second.connect()
    await second.subscribe({ topic: topicName, fromBeginning: true })
    second.run({ eachMessage: async () => {} })
    await waitForConsumerToJoinGroup(second)

    // Give the first consumer's heartbeat loop (heartbeatInterval default 3000ms) a couple of
    // cycles to hit the coordinator and observe the fencing.
    await new Promise((resolve) => setTimeout(resolve, 10000))

    expect(crashes.length).toBeGreaterThan(0)
    expect(crashes[0].error.type).toEqual('FENCED_INSTANCE_ID')
    expect(crashes[0].restart).toBe(false)
  }, 60000)
})
