const createAdmin = require('../index')
const { createErrorFromCode } = require('../../protocol/error')
const { KafkaJSTimeout } = require('../../errors')
const { newLogger } = require('testHelpers')

const UNKNOWN_TOPIC_OR_PARTITION = 3
const LEADER_NOT_AVAILABLE = 5
const TOPIC_AUTHORIZATION_FAILED = 29

/**
 * An admin over a stub cluster whose controller broker accepts every CreateTopics request and
 * answers the leader wait's Metadata request from `metadata`.
 * @param {jest.Mock} metadata the broker's `metadata`
 */
const adminWith = (metadata) => {
  const broker = { createTopics: jest.fn(async () => true), metadata }
  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    findControllerBroker: jest.fn(async () => broker),
  }
  // the outer retrier is not under test: a single attempt keeps every failure visible
  const admin = createAdmin({
    logger: newLogger(),
    retry: { retries: 0, initialRetryTime: 1, maxRetryTime: 1 },
    cluster,
  })
  return { admin, broker }
}

/** A Metadata stub that fails with `codes` in turn, then resolves. */
const failing = (...codes) => {
  const queue = [...codes]
  return jest.fn(async () => {
    if (queue.length > 0) {
      throw createErrorFromCode(queue.shift())
    }
    return { topicMetadata: [] }
  })
}

describe('Admin > createTopics > waiting for the new topics to have leaders', () => {
  const topics = [{ topic: 'new-topic' }]

  it('keeps waiting while the broker does not know the topic yet', async () => {
    const metadata = failing(UNKNOWN_TOPIC_OR_PARTITION, UNKNOWN_TOPIC_OR_PARTITION)
    const { admin, broker } = adminWith(metadata)

    await expect(admin.createTopics({ topics, timeout: 5000 })).resolves.toBe(true)

    expect(metadata).toHaveBeenCalledTimes(3)
    expect(metadata).toHaveBeenCalledWith(['new-topic'])
    expect(broker.createTopics).toHaveBeenCalledTimes(1)
  })

  it('gives up with the timeout when the topic never becomes known', async () => {
    const metadata = jest.fn(async () => {
      throw createErrorFromCode(UNKNOWN_TOPIC_OR_PARTITION)
    })
    const { admin } = adminWith(metadata)

    await expect(admin.createTopics({ topics, timeout: 350 })).rejects.toThrow(KafkaJSTimeout)

    // one answer per 100 ms wait, so a bound of 350 ms allows a handful, never the default 10 s's worth
    expect(metadata.mock.calls.length).toBeGreaterThan(1)
    expect(metadata.mock.calls.length).toBeLessThan(10)
  })

  it('still waits through LEADER_NOT_AVAILABLE', async () => {
    const metadata = failing(LEADER_NOT_AVAILABLE)
    const { admin } = adminWith(metadata)

    await expect(admin.createTopics({ topics, timeout: 5000 })).resolves.toBe(true)

    expect(metadata).toHaveBeenCalledTimes(2)
  })

  it('fails at once on any other error', async () => {
    const metadata = failing(TOPIC_AUTHORIZATION_FAILED, UNKNOWN_TOPIC_OR_PARTITION)
    const { admin } = adminWith(metadata)

    await expect(admin.createTopics({ topics, timeout: 5000 })).rejects.toMatchObject({
      type: 'TOPIC_AUTHORIZATION_FAILED',
    })

    expect(metadata).toHaveBeenCalledTimes(1)
  })

  it('fails at once with the broker’s answer when the request only validates, since no topic is on its way', async () => {
    const metadata = failing(UNKNOWN_TOPIC_OR_PARTITION, UNKNOWN_TOPIC_OR_PARTITION)
    const { admin } = adminWith(metadata)

    await expect(
      admin.createTopics({ topics, validateOnly: true, timeout: 5000 })
    ).rejects.toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })

    expect(metadata).toHaveBeenCalledTimes(1)
  })

  it('does not ask for leaders when waitForLeaders is false', async () => {
    const metadata = failing(UNKNOWN_TOPIC_OR_PARTITION)
    const { admin } = adminWith(metadata)

    await expect(admin.createTopics({ topics, waitForLeaders: false })).resolves.toBe(true)

    expect(metadata).not.toHaveBeenCalled()
  })
})

describe('Admin > fetchTopicMetadata > UNKNOWN_TOPIC_OR_PARTITION', () => {
  const unknownTopic = () => {
    throw createErrorFromCode(UNKNOWN_TOPIC_OR_PARTITION)
  }
  const adminOver = (cluster) =>
    createAdmin({
      logger: newLogger(),
      retry: { retries: 0, initialRetryTime: 1, maxRetryTime: 1 },
      cluster,
    })

  it('still fails at once when the cluster does not allow auto topic creation', async () => {
    const cluster = { allowAutoTopicCreation: false, metadata: jest.fn(async () => unknownTopic()) }

    await expect(adminOver(cluster).fetchTopicMetadata({ topics: ['t'] })).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
    })

    expect(cluster.metadata).toHaveBeenCalledTimes(1)
  })

  it('still waits through it when the cluster allows auto topic creation', async () => {
    const metadata = jest
      .fn()
      .mockImplementationOnce(async () => unknownTopic())
      .mockResolvedValue({ topicMetadata: [{ topic: 't', partitionMetadata: [] }] })
    const cluster = { allowAutoTopicCreation: true, metadata }

    await expect(adminOver(cluster).fetchTopicMetadata({ topics: ['t'] })).resolves.toEqual({
      topics: [{ name: 't', partitions: [], authorizedOperations: null }],
    })

    expect(metadata).toHaveBeenCalledTimes(2)
  })
})
