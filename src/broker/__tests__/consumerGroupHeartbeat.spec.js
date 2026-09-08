const { createGroupApiBroker } = require('./fixtures/groupApiHarness')
const apiKeys = require('../../protocol/requests/apiKeys')

const RESPONSE = require('../../protocol/requests/consumerGroupHeartbeat/fixtures/java/consumerGroupHeartbeatResponse-v1-assignment.json')
const REQUEST_V1 = require('../../protocol/requests/consumerGroupHeartbeat/fixtures/java/consumerGroupHeartbeatRequest-v1-join.json')

const versions = (maxVersion) => ({
  [apiKeys.ConsumerGroupHeartbeat]: { minVersion: 0, maxVersion },
})

const build = (maxVersion) =>
  createGroupApiBroker({
    versions: versions(maxVersion),
    respond: () => Buffer.from(RESPONSE.hex, 'hex'),
  })

describe('Broker > consumerGroupHeartbeat', () => {
  let harness

  afterEach(async () => {
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  test('sends the negotiated version and the bytes the codec produces', async () => {
    harness = build(1)

    await harness.broker.consumerGroupHeartbeat(REQUEST_V1.fields)

    expect(harness.requests).toHaveLength(1)
    expect(harness.requests[0].apiKey).toEqual(apiKeys.ConsumerGroupHeartbeat)
    expect(harness.requests[0].apiVersion).toEqual(1)
    expect(harness.requests[0].body.toString('hex')).toEqual(REQUEST_V1.hex)
  })

  test('negotiates down to v0 against a broker that offers only v0', async () => {
    harness = build(0)

    await harness.broker.consumerGroupHeartbeat({ groupId: 'g', memberId: 'm', memberEpoch: 0 })

    expect(harness.requests[0].apiVersion).toEqual(0)
  })

  test('returns the parsed response, assignment included', async () => {
    harness = build(1)

    const response = await harness.broker.consumerGroupHeartbeat({
      groupId: 'g',
      memberId: 'm',
      memberEpoch: 0,
    })

    expect(response).toMatchObject({
      errorCode: 0,
      memberEpoch: RESPONSE.fields.memberEpoch,
      heartbeatIntervalMs: RESPONSE.fields.heartbeatIntervalMs,
      assignment: RESPONSE.fields.assignment,
    })
  })

  test('refuses to send a regex subscription to a broker that only offers v0', async () => {
    // v0 has no SubscribedTopicRegex field, so the request would go out silently subscribing to
    // nothing at all. Failing loudly is the only honest answer.
    harness = build(0)

    await expect(
      harness.broker.consumerGroupHeartbeat({
        groupId: 'g',
        memberId: 'm',
        memberEpoch: 0,
        subscribedTopicNames: [],
        subscribedTopicRegex: '^orders-.*',
      })
    ).rejects.toThrow('requires ConsumerGroupHeartbeat v1')

    expect(harness.requests).toHaveLength(0)
  })

  test('throws when the broker does not advertise the API at all', async () => {
    harness = createGroupApiBroker({ versions: {}, respond: () => Buffer.alloc(0) })

    await expect(
      harness.broker.consumerGroupHeartbeat({ groupId: 'g', memberId: 'm', memberEpoch: 0 })
    ).rejects.toThrow('The Kafka server does not support the requested API version')
  })

  test('reports an unadvertised API as unsupported even when a regex was asked for', async () => {
    // "This broker offers v0" would be a lie about a broker that offers nothing; the regex guard
    // steps aside and the unsupported-API error is the one the caller sees.
    harness = createGroupApiBroker({ versions: {}, respond: () => Buffer.alloc(0) })

    await expect(
      harness.broker.consumerGroupHeartbeat({
        groupId: 'g',
        memberId: 'm',
        memberEpoch: 0,
        subscribedTopicRegex: '^orders-.*',
      })
    ).rejects.toThrow('The Kafka server does not support the requested API version')

    expect(harness.requests).toHaveLength(0)
  })
})
