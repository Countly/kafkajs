const createConsumer = require('../index')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const apiKeys = require('../../protocol/requests/apiKeys')
const { newLogger } = require('testHelpers')

const CGD = '../../protocol/requests/consumerGroupDescribe/fixtures/java/'
const DG = '../../protocol/requests/describeGroups/fixtures/java/'
const CONSUMER_GROUP = require(`${CGD}consumerGroupDescribeResponse-v1-memberType.json`)
const GROUP_ID_NOT_FOUND = require(`${CGD}consumerGroupDescribeResponse-v0-groupIdNotFound.json`)
const CLASSIC_GROUP = require(`${DG}describeGroupsResponse-v6-describedGroup.json`)

/** The group id every committed fixture describes. */
const GROUP_ID = 'test-group'

/**
 * A consumer whose coordinator is a real `Broker` over a fake socket, so `describeGroup()` runs
 * through the shipped codecs rather than a stubbed broker method.
 *
 * @param {string} consumerGroupDescribeHex the ConsumerGroupDescribe response body
 * @param {number} [describeGroupsMaxVersion] `0` leaves the newer API as the only answer
 */
const build = (consumerGroupDescribeHex, describeGroupsMaxVersion = 6) => {
  const harness = createGroupApiBroker({
    versions: {
      [apiKeys.ConsumerGroupDescribe]: { minVersion: 0, maxVersion: 1 },
      [apiKeys.DescribeGroups]: { minVersion: 0, maxVersion: describeGroupsMaxVersion },
    },
    respond: ({ apiKey }) =>
      Buffer.from(
        apiKey === apiKeys.ConsumerGroupDescribe ? consumerGroupDescribeHex : CLASSIC_GROUP.hex,
        'hex'
      ),
  })

  const cluster = {
    refreshMetadata: async () => {},
    findGroupCoordinator: async () => harness.broker,
    onTopicRecreated: () => () => {},
  }

  const consumer = createConsumer({
    cluster,
    groupId: GROUP_ID,
    logger: newLogger(),
    retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 2 },
  })

  return { consumer, harness }
}

describe('Consumer > describeGroup > routing', () => {
  let harness

  afterEach(async () => {
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  // MF-3: this used to ask DescribeGroups directly, which answers GROUP_ID_NOT_FOUND for a
  // consumer-type group - so a live KIP-848 group described itself as `Dead` with no members.
  it('describes its own KIP-848 group through ConsumerGroupDescribe', async () => {
    let consumer
    ;({ consumer, harness } = build(CONSUMER_GROUP.hex))

    const description = await consumer.describeGroup()

    expect(harness.requests.map(({ apiKey }) => apiKey)).toEqual([apiKeys.ConsumerGroupDescribe])
    expect(description).toMatchObject({
      groupId: GROUP_ID,
      errorCode: 0,
      state: 'Stable',
      groupType: 'consumer',
      protocolType: 'consumer',
      protocol: 'uniform',
      groupEpoch: 7,
      assignmentEpoch: 7,
      assignorName: 'uniform',
    })
    expect(description.members[0]).toMatchObject({
      memberId: 'test-member',
      groupInstanceId: 'test-instance',
      memberEpoch: 5,
      memberType: 'consumer',
      subscribedTopics: ['test-topic'],
      assignment: [{ topic: 'test-topic', partitions: [0, 2] }],
    })
  })

  it('falls back to DescribeGroups for a classic group, keeping the 2.2.4 fields', async () => {
    let consumer
    ;({ consumer, harness } = build(GROUP_ID_NOT_FOUND.hex))

    const description = await consumer.describeGroup()

    expect(harness.requests.map(({ apiKey }) => apiKey)).toEqual([
      apiKeys.ConsumerGroupDescribe,
      apiKeys.DescribeGroups,
    ])
    expect(description).toMatchObject({
      groupId: GROUP_ID,
      errorCode: 0,
      state: 'Stable',
      protocolType: 'consumer',
      protocol: 'RoundRobinAssigner',
      groupType: 'classic',
    })
    expect(description.members[0]).toMatchObject({
      memberId: 'test-member',
      clientId: 'test-client',
      clientHost: '/127.0.0.1',
      groupInstanceId: 'test-instance',
    })
    expect(Buffer.isBuffer(description.members[0].memberMetadata)).toBe(true)
    expect(Buffer.isBuffer(description.members[0].memberAssignment)).toBe(true)
  })

  // SF-9: `admin.describeGroups` maps the "not requested" sentinel to null and this now agrees,
  // so one declared `GroupDescription` type describes both.
  it('reports authorizedOperations as null on both paths, as the type says', async () => {
    let consumer
    ;({ consumer, harness } = build(CONSUMER_GROUP.hex))
    await expect(consumer.describeGroup()).resolves.toMatchObject({ authorizedOperations: null })

    await harness.pool.destroy()
    ;({ consumer, harness } = build(GROUP_ID_NOT_FOUND.hex))
    await expect(consumer.describeGroup()).resolves.toMatchObject({ authorizedOperations: null })
  })
})
