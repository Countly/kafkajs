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

  it('waits for every supplied broker rather than one visible topic', async () => {
    jest.useFakeTimers()
    const owner = new AbortController()
    let ready = false
    const visible = { topicMetadata: [{ topic: 'topic-a', partitionMetadata: [{ leader: 1 }] }] }
    const brokers = [0, 1, 2].map((index) => ({
      connect: jest.fn(async () => {}),
      metadata: jest.fn(async () => {
        if (index === 2 && !ready) throw NOT_PROPAGATED
        return visible
      }),
    }))
    let settled = false
    const pending = waitForTopicPropagation('topic-a', 1, {
      brokers,
      signal: owner.signal,
    }).then(() => {
      settled = true
    })
    await jest.advanceTimersByTimeAsync(200)
    expect(settled).toBe(false)
    expect(brokers.every((broker) => broker.metadata.mock.calls.length > 0)).toBe(true)
    ready = true
    await jest.advanceTimersByTimeAsync(100)
    await pending
    expect(settled).toBe(true)
    expect(brokers[2].metadata).toHaveBeenLastCalledWith(['topic-a'])
  })

  it('owner cancellation stops the propagation wait and later metadata reads', async () => {
    jest.useFakeTimers()
    const owner = new AbortController()
    const broker = {
      connect: jest.fn(async () => {}),
      metadata: jest.fn(async () => {
        throw NOT_PROPAGATED
      }),
    }
    const failure = new Error('cancel topic setup')
    const outcome = waitForTopicPropagation('topic-a', 1, {
      brokers: [broker],
      signal: owner.signal,
    }).catch((error) => error)
    await jest.advanceTimersByTimeAsync(100)
    owner.abort(failure)
    await jest.advanceTimersByTimeAsync(1000)
    expect(await outcome).toBe(failure)
    const reads = broker.metadata.mock.calls.length
    await jest.advanceTimersByTimeAsync(1000)
    expect(broker.metadata).toHaveBeenCalledTimes(reads)
  })

  it('a permanent metadata error ends the all-broker wait immediately', async () => {
    const failure = Object.assign(new Error('not authorized'), {
      type: 'TOPIC_AUTHORIZATION_FAILED',
    })
    const broker = {
      connect: jest.fn(async () => {}),
      metadata: jest.fn(async () => {
        throw failure
      }),
    }
    await expect(
      waitForTopicPropagation('topic-a', 1, {
        brokers: [broker],
        signal: new AbortController().signal,
      })
    ).rejects.toBe(failure)
    expect(broker.metadata).toHaveBeenCalledTimes(1)
  })
})
