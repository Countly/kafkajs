const { createGroupApiBroker } = require('./fixtures/groupApiHarness')
const apiKeys = require('../../protocol/requests/apiKeys')

const COMMIT_RESPONSE = require('../../protocol/requests/offsetCommit/fixtures/java/offsetCommitResponse-v9-basic.json')
const FETCH_RESPONSE = require('../../protocol/requests/offsetFetch/fixtures/java/offsetFetchResponse-v9-basic.json')

const COMMIT = {
  groupId: 'test-group',
  groupGenerationId: 5,
  memberId: 'test-member',
  topics: [{ topic: 'test-topic', partitions: [{ partition: 0, offset: '10' }] }],
}
const FETCH = {
  groupId: 'test-group',
  topics: [{ topic: 'test-topic', partitions: [{ partition: 0 }] }],
}

/**
 * @param {number} apiKey
 * @param {number} maxVersion the highest version the broker advertises
 * @param {string} hex the response body to answer with
 */
const brokerAdvertising = (apiKey, maxVersion, hex) =>
  createGroupApiBroker({
    versions: { [apiKey]: { minVersion: 0, maxVersion } },
    respond: () => Buffer.from(hex, 'hex'),
  })

describe('Broker > offsets stay keyed by topic name', () => {
  let harness

  afterEach(async () => {
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  test.each([9, 10, 11])(
    'commits at v9 when the broker advertises v%i, since no entry carries a topic id',
    async (maxVersion) => {
      harness = brokerAdvertising(apiKeys.OffsetCommit, maxVersion, COMMIT_RESPONSE.hex)

      await harness.broker.offsetCommit(COMMIT)

      expect(harness.requests.map(({ apiVersion }) => apiVersion)).toEqual([9])
    }
  )

  test.each([9, 10, 11])(
    'fetches at v9 when the broker advertises v%i, since no entry carries a topic id',
    async (maxVersion) => {
      harness = brokerAdvertising(apiKeys.OffsetFetch, maxVersion, FETCH_RESPONSE.hex)

      await harness.broker.offsetFetch(FETCH)

      expect(harness.requests.map(({ apiVersion }) => apiVersion)).toEqual([9])
    }
  )

  test('fetches every topic of the group at v9 too', async () => {
    harness = brokerAdvertising(apiKeys.OffsetFetch, 10, FETCH_RESPONSE.hex)

    await harness.broker.offsetFetch({ groupId: 'test-group', topics: null })

    expect(harness.requests.map(({ apiVersion }) => apiVersion)).toEqual([9])
  })

  test.each([8, 7])(
    'never raises a lower version: a broker at v%i is asked at v%i',
    async (maxVersion) => {
      harness = brokerAdvertising(apiKeys.OffsetCommit, maxVersion, COMMIT_RESPONSE.hex)

      // Only the first request is answered, and with a v9-shaped body that a v7 or v8 codec rejects:
      // what is under test is the version the request went out at, not the answer.
      await harness.broker.offsetCommit(COMMIT).catch(() => {})

      expect(harness.requests.map(({ apiVersion }) => apiVersion)).toEqual([maxVersion])
    }
  )
})
