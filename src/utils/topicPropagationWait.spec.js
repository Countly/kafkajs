jest.mock('../broker')
jest.mock('../../index', () => ({ ...jest.requireActual('../../index'), Kafka: jest.fn() }))

const Broker = require('../broker')
const { Kafka } = require('../../index')
const {
  createTopic,
  waitForTopicPropagation,
  disconnectPropagationBrokers,
} = require('testHelpers')

const NOT_PROPAGATED = Object.assign(new Error('not propagated'), {
  type: 'UNKNOWN_TOPIC_OR_PARTITION',
})

/**
 * Makes every broker the helper opens answer metadata requests with `answer`.
 * @param {() => Promise<object>} answer
 */
const brokersAnswering = (answer) => {
  Broker.mockImplementation(() => ({
    connect: jest.fn(async () => {}),
    metadata: jest.fn(answer),
    disconnect: jest.fn(async () => {}),
  }))
}

describe('testHelpers > waiting for a created topic to reach every broker', () => {
  afterEach(async () => {
    jest.useRealTimers()
    await disconnectPropagationBrokers()
  })

  it('keeps waiting through 29 seconds of brokers that have not caught up, then gives up', async () => {
    jest.useFakeTimers()
    brokersAnswering(async () => {
      throw NOT_PROPAGATED
    })
    let outcome = null
    waitForTopicPropagation('topic-a', 1).catch((e) => {
      outcome = e
    })

    await jest.advanceTimersByTimeAsync(29000)
    expect(outcome).toBeNull()

    await jest.advanceTimersByTimeAsync(1200)
    expect(outcome).toMatchObject({
      name: 'KafkaJSTimeout',
      message: 'Timed out waiting for all brokers to serve metadata for topic "topic-a"',
    })
  })

  it('gives the leader wait of the CreateTopics call at least the same bound', async () => {
    const createTopics = jest.fn(async () => true)
    Kafka.mockImplementation(() => ({
      admin: () => ({ connect: async () => {}, disconnect: async () => {}, createTopics }),
    }))
    brokersAnswering(async () => ({
      topicMetadata: [{ topic: 'topic-a', partitionMetadata: [{ leader: 0 }] }],
    }))

    await createTopic({ topic: 'topic-a' })

    expect(createTopics).toHaveBeenCalledTimes(1)
    expect(createTopics.mock.calls[0][0].timeout).toBeGreaterThanOrEqual(30000)
  })
})
