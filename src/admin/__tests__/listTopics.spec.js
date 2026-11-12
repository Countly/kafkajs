const createAdmin = require('../index')

const { secureRandom, createCluster, newLogger, waitForTopicPropagation } = require('testHelpers')

describe('Admin', () => {
  let cluster, admin, existingTopicNames, setup

  beforeEach(async () => {
    setup = new AbortController()
    cluster = createCluster()
    admin = createAdmin({ cluster, logger: newLogger() })
    existingTopicNames = [
      `test-topic-${secureRandom()}`,
      `test-topic-${secureRandom()}`,
      `test-topic-${secureRandom()}`,
    ]
    const topics = existingTopicNames.map((n) => ({
      topic: n,
      numPartitions: 1,
    }))
    await admin.createTopics({ topics })
    await Promise.all(
      existingTopicNames.map((topic) => waitForTopicPropagation(topic, 1, { signal: setup.signal }))
    )
  })

  afterEach(async () => {
    setup.abort()
    if (admin) {
      await admin.deleteTopics({ topics: existingTopicNames })
      await admin.disconnect()
    }
  })

  describe('listTopics', () => {
    test('list topics', async () => {
      const listTopicsResponse = await admin.listTopics()
      expect(listTopicsResponse).toEqual(expect.arrayContaining(existingTopicNames))
    })
  })
})
