const createConsumer = require('../index')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const apiKeys = require('../../protocol/requests/apiKeys')
const { newLogger } = require('testHelpers')

const CGD = '../../protocol/requests/consumerGroupDescribe/fixtures/java/'
const DG = '../../protocol/requests/describeGroups/fixtures/java/'
const CONSUMER_GROUP = require(`${CGD}consumerGroupDescribeResponse-v1-memberType.json`)
const GROUP_ID_NOT_FOUND = require(`${CGD}consumerGroupDescribeResponse-v0-groupIdNotFound.json`)
const CLASSIC_GROUP = require(`${DG}describeGroupsResponse-v6-describedGroup.json`)
const CLASSIC_NOT_FOUND = require(`${DG}describeGroupsResponse-v6-groupIdNotFound.json`)
/** What a broker without the consumer protocol answers: code 35 on the group's row, all else default. */
const UNSUPPORTED_VERSION_HEX =
  '00000000' +
  '02' +
  '0023' +
  '00' +
  '0b746573742d67726f7570' +
  '01' +
  '0000000000000000' +
  '0101' +
  '80000000' +
  '0000'

/** The group id every committed fixture describes. */
const GROUP_ID = 'test-group'

/**
 * A consumer whose coordinator is a real `Broker` over a fake socket, so `describeGroup()` runs
 * through the shipped codecs rather than a stubbed broker method.
 *
 * @param {string} consumerGroupDescribeHex the ConsumerGroupDescribe response body
 * @param {number} [describeGroupsMaxVersion] `0` leaves the newer API as the only answer
 * @param {string} [describeGroupsHex] the DescribeGroups response body
 */
const build = (
  consumerGroupDescribeHex,
  describeGroupsMaxVersion = 6,
  describeGroupsHex = CLASSIC_GROUP.hex
) => {
  const harness = createGroupApiBroker({
    versions: {
      [apiKeys.ConsumerGroupDescribe]: { minVersion: 0, maxVersion: 1 },
      [apiKeys.DescribeGroups]: { minVersion: 0, maxVersion: describeGroupsMaxVersion },
    },
    respond: ({ apiKey }) =>
      Buffer.from(
        apiKey === apiKeys.ConsumerGroupDescribe ? consumerGroupDescribeHex : describeGroupsHex,
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

  // This used to ask DescribeGroups directly, which answers GROUP_ID_NOT_FOUND for a
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

  it('describes a missing group by the DescribeGroups answer on a cluster without the consumer protocol', async () => {
    let consumer
    ;({ consumer, harness } = build(UNSUPPORTED_VERSION_HEX, 6, CLASSIC_NOT_FOUND.hex))

    const description = await consumer.describeGroup()

    expect(harness.requests.map(({ apiKey }) => apiKey)).toEqual([
      apiKeys.ConsumerGroupDescribe,
      apiKeys.DescribeGroups,
    ])
    expect(description).toMatchObject({
      groupId: GROUP_ID,
      state: 'Dead',
      errorCode: 69,
      errorMessage: CLASSIC_NOT_FOUND.fields.groups[0].errorMessage,
      groupType: 'classic',
    })
  })

  // `admin.describeGroups` maps the "not requested" sentinel to null and this now agrees,
  // so one declared `GroupDescription` type describes both.
  it('reports authorizedOperations as null on both paths, as the type says', async () => {
    let consumer
    ;({ consumer, harness } = build(CONSUMER_GROUP.hex))
    await expect(consumer.describeGroup()).resolves.toMatchObject({ authorizedOperations: null })

    await harness.pool.destroy()
    ;({ consumer, harness } = build(GROUP_ID_NOT_FOUND.hex))
    await expect(consumer.describeGroup()).resolves.toMatchObject({ authorizedOperations: null })
  })

  // A lone surrogate is written as U+FFFD, so a broker echoes that and not the string that was asked.
  describe('with a group id the broker echoes as the bytes it read', () => {
    const echoOf = (id) => Buffer.from(id, 'utf8').toString('utf8')
    const consumerRow = (groupId, errorCode = 0) => ({
      errorCode,
      errorMessage: null,
      groupId,
      groupState: 'Stable',
      groupEpoch: 7,
      assignmentEpoch: 7,
      assignorName: 'uniform',
      members: [],
      authorizedOperations: -2147483648,
    })
    const classicRow = (groupId) => ({
      errorCode: 0,
      errorMessage: null,
      groupId,
      state: 'Stable',
      protocolType: 'consumer',
      protocol: 'RoundRobinAssigner',
      members: [],
      authorizedOperations: -2147483648,
    })

    /**
     * @param {string} groupId the consumer's group
     * @param {object} coordinator the stubbed ConsumerGroupDescribe and DescribeGroups
     * @returns {object} a consumer whose coordinator is `coordinator`
     */
    const echoing = (groupId, coordinator) =>
      createConsumer({
        cluster: {
          refreshMetadata: async () => {},
          findGroupCoordinator: async () => coordinator,
          onTopicRecreated: () => () => {},
        },
        groupId,
        logger: newLogger(),
        retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 2 },
      })

    it.each([
      ['lone surrogate', 'team-\uD800'],
      ['ordinary Unicode', 'über-\u{1F600}'],
    ])('describes a %s group id through ConsumerGroupDescribe under its own id', async (_, id) => {
      const coordinator = {
        nodeId: 0,
        consumerGroupDescribe: jest.fn(async ({ groupIds }) => ({
          groups: groupIds.map((groupId) => consumerRow(echoOf(groupId))),
        })),
        describeGroups: jest.fn(),
      }

      const description = await echoing(id, coordinator).describeGroup()

      expect(description).toMatchObject({ groupId: id, groupType: 'consumer', errorCode: 0 })
      expect(coordinator.describeGroups).not.toHaveBeenCalled()
    })

    it('describes a lone-surrogate group id through DescribeGroups under its own id when it is not a consumer-protocol group', async () => {
      const coordinator = {
        nodeId: 0,
        consumerGroupDescribe: async ({ groupIds }) => ({
          groups: groupIds.map((groupId) => consumerRow(echoOf(groupId), 69)),
        }),
        describeGroups: jest.fn(async ({ groupIds }) => ({
          groups: groupIds.map((groupId) => classicRow(echoOf(groupId))),
        })),
      }

      const description = await echoing('team-\uD800', coordinator).describeGroup()

      expect(description).toMatchObject({ groupId: 'team-\uD800', groupType: 'classic' })
      expect(coordinator.describeGroups).toHaveBeenCalledTimes(1)
    })

    it('resolves undefined for a group id when both answers are for another id', async () => {
      const coordinator = {
        nodeId: 0,
        consumerGroupDescribe: async () => ({ groups: [consumerRow('team-��')] }),
        describeGroups: async () => ({ groups: [classicRow('team-��')] }),
      }

      await expect(echoing('team-\uD800', coordinator).describeGroup()).resolves.toBeUndefined()
    })
  })
})
