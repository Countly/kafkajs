const createAdmin = require('../index')
const apiKeys = require('../../protocol/requests/apiKeys')
const { requests } = require('../../protocol/requests')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const { newLogger } = require('testHelpers')

const CGD = '../../protocol/requests/consumerGroupDescribe/fixtures/java/'
const DG = '../../protocol/requests/describeGroups/fixtures/java/'
const LG = '../../protocol/requests/listGroups/fixtures/java/'

const CONSUMER_GROUP = require(`${CGD}consumerGroupDescribeResponse-v1-memberType.json`)
const UNKNOWN_TO_69 = require(`${CGD}consumerGroupDescribeResponse-v0-groupIdNotFound.json`)
const CLASSIC_GROUP = require(`${DG}describeGroupsResponse-v6-describedGroup.json`)
const LISTED = require(`${LG}listGroupsResponse-v5-groups.json`)

/** The group id every Java vector names. */
const GROUP_ID = 'test-group'

/**
 * A real broker over a fake socket that answers each API key with the vector given for it.
 *
 * @param {Object<number, { minVersion: number, maxVersion: number }>} versions the pinned
 *        ApiVersions table; an API key left out of it is one the broker does not offer
 * @param {Object<number, string>} bodies response body hex, by API key
 * @returns {{ broker: object, requests: object[], pool: object }}
 */
const brokerWith = (versions, bodies) =>
  createGroupApiBroker({
    versions,
    respond: ({ apiKey }) => Buffer.from(bodies[apiKey], 'hex'),
  })

/**
 * A cluster reduced to the three seams the two admin methods use. The harness broker is the only
 * node, so every group id batches onto one coordinator.
 * @param {object} broker
 * @returns {object}
 */
const clusterOver = (broker) => ({
  refreshMetadata: async () => {},
  brokerPool: { brokers: { 0: broker } },
  findBroker: async () => broker,
  findGroupCoordinator: async () => broker,
})

describe('Admin > group APIs > bytes on the wire', () => {
  const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 2 }
  let harness

  const adminOver = (versions, bodies) => {
    harness = brokerWith(versions, bodies)
    return createAdmin({ logger: newLogger(), retry, cluster: clusterOver(harness.broker) })
  }

  afterEach(async () => {
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  it('writes one ConsumerGroupDescribe frame and nothing else where the broker offers key 69', async () => {
    const admin = adminOver(
      {
        [apiKeys.ConsumerGroupDescribe]: { minVersion: 0, maxVersion: 1 },
        [apiKeys.DescribeGroups]: { minVersion: 0, maxVersion: 6 },
      },
      { [apiKeys.ConsumerGroupDescribe]: CONSUMER_GROUP.hex }
    )

    const { groups } = await admin.describeGroups([GROUP_ID])
    const expected = await requests.ConsumerGroupDescribe.protocol({ version: 1 })({
      groupIds: [GROUP_ID],
    }).request.encode()

    expect(harness.requests.map(({ apiKey }) => apiKey)).toEqual([apiKeys.ConsumerGroupDescribe])
    expect(harness.requests[0].apiVersion).toEqual(1)
    expect(harness.requests[0].body).toEqual(expected.buffer)
    expect(groups[0]).toMatchObject({ groupId: GROUP_ID, groupType: 'consumer', state: 'Stable' })
  })

  it('writes a DescribeGroups frame after a GROUP_ID_NOT_FOUND row on key 69', async () => {
    const admin = adminOver(
      {
        [apiKeys.ConsumerGroupDescribe]: { minVersion: 0, maxVersion: 0 },
        [apiKeys.DescribeGroups]: { minVersion: 0, maxVersion: 6 },
      },
      {
        [apiKeys.ConsumerGroupDescribe]: UNKNOWN_TO_69.hex,
        [apiKeys.DescribeGroups]: CLASSIC_GROUP.hex,
      }
    )

    const { groups } = await admin.describeGroups([GROUP_ID])
    const fallback = await requests.DescribeGroups.protocol({ version: 6 })({
      groupIds: [GROUP_ID],
    }).request.encode()

    expect(harness.requests.map(({ apiKey, apiVersion }) => [apiKey, apiVersion])).toEqual([
      [apiKeys.ConsumerGroupDescribe, 0],
      [apiKeys.DescribeGroups, 6],
    ])
    expect(harness.requests[1].body).toEqual(fallback.buffer)
    expect(groups[0]).toMatchObject({ groupId: GROUP_ID, groupType: 'classic', state: 'Stable' })
  })

  it('writes no key-69 frame at all against a broker that does not advertise it', async () => {
    const admin = adminOver(
      { [apiKeys.DescribeGroups]: { minVersion: 0, maxVersion: 6 } },
      { [apiKeys.DescribeGroups]: CLASSIC_GROUP.hex }
    )

    const { groups } = await admin.describeGroups([GROUP_ID])

    expect(harness.requests.map(({ apiKey }) => apiKey)).toEqual([apiKeys.DescribeGroups])
    expect(groups[0]).toMatchObject({ groupId: GROUP_ID, groupType: 'classic' })
  })

  it('writes the display-name filters on a ListGroups v5 frame and lower-cases the type back', async () => {
    const admin = adminOver(
      { [apiKeys.ListGroups]: { minVersion: 0, maxVersion: 5 } },
      { [apiKeys.ListGroups]: LISTED.hex }
    )

    const { groups } = await admin.listGroups({ states: ['stable'], types: ['consumer'] })
    // Only a request carrying StatesFilter ['Stable'] and TypesFilter ['Consumer'] can match these
    // bytes, which is the whole claim: the lower-case API values are mapped on the way out.
    const expected = await requests.ListGroups.protocol({ version: 5 })({
      states: ['Stable'],
      types: ['Consumer'],
    }).request.encode()
    const row = LISTED.fields.groups[0]

    expect(harness.requests[0].apiKey).toEqual(apiKeys.ListGroups)
    expect(harness.requests[0].apiVersion).toEqual(5)
    expect(harness.requests[0].body).toEqual(expected.buffer)
    expect(groups[0]).toMatchObject({
      groupId: row.groupId,
      groupState: row.groupState,
      groupType: row.groupType.toLowerCase(),
    })
  })
})
