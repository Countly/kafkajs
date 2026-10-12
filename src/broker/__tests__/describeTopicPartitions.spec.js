const { createGroupApiBroker } = require('./fixtures/groupApiHarness')
const apiKeys = require('../../protocol/requests/apiKeys')

const DIR = '../../protocol/requests/describeTopicPartitions/fixtures/java/'
const request = (caseId) => require(`${DIR}describeTopicPartitionsRequest-v0-${caseId}.json`)
const response = (caseId) => require(`${DIR}describeTopicPartitionsResponse-v0-${caseId}.json`)

/** A broker that offers DescribeTopicPartitions v0 and answers every request with `caseId`. */
const brokerAnswering = (caseId) =>
  createGroupApiBroker({
    versions: { [apiKeys.DescribeTopicPartitions]: { minVersion: 0, maxVersion: 0 } },
    respond: () => Buffer.from(response(caseId).hex, 'hex'),
  })

describe('Broker > describeTopicPartitions', () => {
  let harness

  afterEach(async () => {
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  test('sends the v0 request with its cursor and resolves the decoded page', async () => {
    harness = brokerAnswering('nextCursor')

    const page = await harness.broker.describeTopicPartitions(request('cursor').fields)

    expect(harness.requests).toHaveLength(1)
    expect(harness.requests[0]).toMatchObject({ apiKey: 75, apiVersion: 0 })
    expect(harness.requests[0].body.toString('hex')).toEqual(request('cursor').hex)
    expect(page).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 25,
      topics: response('nextCursor').fields.topics,
      nextCursor: { topicName: 'test-topic', partitionIndex: 1 },
    })
  })

  test('asks for 2000 partitions and sends no cursor when only topics are given', async () => {
    harness = brokerAnswering('noNextCursor')

    const page = await harness.broker.describeTopicPartitions({
      topics: request('noCursor').fields.topics,
    })

    expect(harness.requests[0].body.toString('hex')).toEqual(request('noCursor').hex)
    expect(page.nextCursor).toBeNull()
    expect(page.topics.map(({ name }) => name)).toEqual(['test-topic', '__consumer_offsets'])
  })

  test('resolves a page whose topic failed, leaving the decision to the caller', async () => {
    harness = brokerAnswering('unauthorizedZeroId')

    const page = await harness.broker.describeTopicPartitions({ topics: ['test-topic'] })

    expect(page.topics).toEqual([
      expect.objectContaining({ name: 'test-topic', errorCode: 29, topicId: null, partitions: [] }),
    ])
  })

  test('keeps the eligible leader replicas of a cluster that does not track them as null', async () => {
    harness = brokerAnswering('elrNull')

    const { topics } = await harness.broker.describeTopicPartitions({ topics: ['test-topic'] })

    expect(topics[0].partitions[0]).toMatchObject({
      eligibleLeaderReplicas: null,
      lastKnownElr: null,
    })
  })

  test('rejects before sending anything when the broker does not offer the API', async () => {
    harness = createGroupApiBroker({ versions: {}, respond: () => Buffer.alloc(0) })

    await expect(
      harness.broker.describeTopicPartitions({ topics: ['test-topic'] })
    ).rejects.toMatchObject({
      name: 'KafkaJSServerDoesNotSupportApiKey',
      apiKey: 75,
      apiName: 'DescribeTopicPartitions',
    })
    expect(harness.requests).toEqual([])
  })
})
