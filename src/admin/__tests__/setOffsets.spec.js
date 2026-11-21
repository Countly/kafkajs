const createAdmin = require('../index')
const createConsumer = require('../../consumer')

const { secureRandom, createCluster, newLogger, createTopic } = require('testHelpers')

describe('Admin', () => {
  let topicName, groupId, admin, consumer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic: topicName })
  })

  afterEach(async () => {
    admin && (await admin.disconnect())
    consumer && (await consumer.disconnect())
  })

  describe('setOffsets', () => {
    test('throws an error if the groupId is invalid', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.setOffsets({ groupId: null })).rejects.toHaveProperty(
        'message',
        'Invalid groupId null'
      )
    })

    test('throws an error if the topic name is not a valid string', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.setOffsets({ groupId: 'groupId', topic: null })).rejects.toHaveProperty(
        'message',
        'Invalid topic null'
      )
    })

    test('throws an error if partitions is invalid', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(
        admin.setOffsets({ groupId: 'groupId', topic: 'topic', partitions: null })
      ).rejects.toHaveProperty('message', 'Invalid partitions')

      await expect(
        admin.setOffsets({ groupId: 'groupId', topic: 'topic', partitions: [] })
      ).rejects.toHaveProperty('message', 'Invalid partitions')
    })

    test('set the consumer group to any offsets, pre-seeding a group that does not exist yet', async () => {
      const cluster = createCluster()
      admin = createAdmin({ cluster, logger: newLogger() })

      await admin.connect()
      await admin.setOffsets({
        groupId,
        topic: topicName,
        partitions: [{ partition: 0, offset: 13 }],
      })

      const offsets = await admin.fetchOffsets({ groupId, topics: [topicName] })
      expect(offsets).toEqual([
        {
          topic: topicName,
          partitions: [{ partition: 0, offset: '13', metadata: null }],
        },
      ])
    })

    test('throws an error if the consumer group is running', async () => {
      consumer = createConsumer({ groupId, cluster: createCluster(), logger: newLogger() })
      await consumer.connect()
      await consumer.subscribe({ topic: topicName })
      await consumer.run({ eachMessage: () => true })

      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })

      await admin.connect()
      const before = await admin.fetchOffsets({ groupId, topics: [topicName] })
      await expect(
        admin.setOffsets({
          groupId,
          topic: topicName,
          partitions: [{ partition: 0, offset: 13 }],
        })
      ).rejects.toHaveProperty(
        'message',
        'The consumer group must have no running instances, current state: Stable'
      )
      expect(await admin.fetchOffsets({ groupId, topics: [topicName] })).toEqual(before)
    })

    test('setOffsets on an empty consumer-protocol group commits without joining or converting it', async () => {
      consumer = createConsumer({
        groupId,
        groupProtocol: 'consumer',
        cluster: createCluster(),
        logger: newLogger(),
      })
      await consumer.connect()
      await consumer.subscribe({ topic: topicName, fromBeginning: true })
      let joined
      const join = new Promise((resolve) => (joined = resolve))
      consumer.on(consumer.events.GROUP_JOIN, () => joined())
      await consumer.run({ eachMessage: async () => {} })
      await join
      await consumer.disconnect()
      consumer = null

      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await admin.connect()
      const describe = async () => (await admin.describeGroups([groupId])).groups[0]
      const before = await describe()
      expect(before).toMatchObject({ groupType: 'consumer', state: 'Empty', members: [] })

      await admin.setOffsets({
        groupId,
        topic: topicName,
        partitions: [{ partition: 0, offset: '7' }],
      })

      expect(await admin.fetchOffsets({ groupId, topics: [topicName] })).toEqual([
        { topic: topicName, partitions: [{ partition: 0, offset: '7', metadata: null }] },
      ])
      expect(await describe()).toMatchObject({
        groupType: 'consumer',
        state: 'Empty',
        members: [],
        groupEpoch: before.groupEpoch,
      })
    })
  })
})
