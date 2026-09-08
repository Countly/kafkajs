const { createGroupApiBroker } = require('./fixtures/groupApiHarness')
const apiKeys = require('../../protocol/requests/apiKeys')
const { requests } = require('../../protocol/requests')

const DIR = '../../protocol/requests/consumerGroupDescribe/fixtures/java/'
const DESCRIBED = require(`${DIR}consumerGroupDescribeResponse-v1-memberType.json`)
const NOT_FOUND = require(`${DIR}consumerGroupDescribeResponse-v0-groupIdNotFound.json`)
const REQUEST_V1 = require(`${DIR}consumerGroupDescribeRequest-v1-authorizedOperations.json`)

const build = (maxVersion, hex) =>
  createGroupApiBroker({
    versions: { [apiKeys.ConsumerGroupDescribe]: { minVersion: 0, maxVersion } },
    respond: () => Buffer.from(hex, 'hex'),
  })

describe('Broker > consumerGroupDescribe', () => {
  let harness

  afterEach(async () => {
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  test('sends the highest version the broker offers, with the codec bytes', async () => {
    harness = build(1, DESCRIBED.hex)

    await harness.broker.consumerGroupDescribe({
      groupIds: ['test-group'],
      includeAuthorizedOperations: true,
    })

    const expected = await requests.ConsumerGroupDescribe.protocol({ version: 1 })({
      groupIds: ['test-group'],
      includeAuthorizedOperations: true,
    }).request.encode()

    expect(harness.requests).toHaveLength(1)
    expect(harness.requests[0].apiKey).toEqual(apiKeys.ConsumerGroupDescribe)
    expect(harness.requests[0].apiVersion).toEqual(1)
    expect(harness.requests[0].body).toEqual(expected.buffer)
    // …and those bytes are the Apache 4.3.1 golden for the same values, so the assertion is not
    // just this repo's encoder agreeing with itself.
    expect(harness.requests[0].body.toString('hex')).toEqual(REQUEST_V1.hex)
  })

  test('negotiates down to v0, where memberType is reported as null', async () => {
    harness = build(0, require(`${DIR}consumerGroupDescribeResponse-v0-describedGroup.json`).hex)

    const response = await harness.broker.consumerGroupDescribe({ groupIds: ['test-group'] })

    expect(harness.requests[0].apiVersion).toEqual(0)
    expect(response.groups[0].members[0].memberType).toBeNull()
  })

  test('returns a described group with both assignments decoded', async () => {
    harness = build(1, DESCRIBED.hex)

    const response = await harness.broker.consumerGroupDescribe({ groupIds: ['test-group'] })
    const [group] = response.groups

    expect(group).toMatchObject({
      errorCode: 0,
      groupId: 'test-group',
      groupState: 'Stable',
      assignorName: 'uniform',
    })
    expect(group.members[0].memberType).toEqual(1)
    expect(group.members[0].assignment.topicPartitions[0]).toEqual({
      topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
      topicName: 'test-topic',
      partitions: [0, 2],
    })
    expect(group.members[0].targetAssignment).toEqual(group.members[0].assignment)
  })

  test('resolves a GROUP_ID_NOT_FOUND row instead of rejecting', async () => {
    // The property the admin routing stands on, asserted at the broker boundary and not only in
    // the codec: a rejection here would make "ask DescribeGroups for that id" unreachable.
    harness = build(1, NOT_FOUND.hex)

    const response = await harness.broker.consumerGroupDescribe({ groupIds: ['test-group'] })

    expect(response.groups[0].errorCode).toEqual(69)
    expect(response.groups[0].errorMessage).toEqual(NOT_FOUND.fields.groups[0].errorMessage)
    expect(response.groups[0].members).toEqual([])
  })

  test('rejects when the broker does not advertise the API at all', async () => {
    // The third fallback trigger the admin routing reads: a broker too old to know key 69.
    harness = createGroupApiBroker({ versions: {}, respond: () => Buffer.alloc(0) })

    await expect(
      harness.broker.consumerGroupDescribe({ groupIds: ['test-group'] })
    ).rejects.toThrow('The Kafka server does not support the requested API version')

    expect(harness.requests).toHaveLength(0)
  })
})
