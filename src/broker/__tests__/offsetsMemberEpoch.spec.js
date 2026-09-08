const { createGroupApiBroker } = require('./fixtures/groupApiHarness')
const apiKeys = require('../../protocol/requests/apiKeys')
const { requests } = require('../../protocol/requests')
const Decoder = require('../../protocol/decoder')

const FETCH_RESPONSE = require('../../protocol/requests/offsetFetch/fixtures/java/offsetFetchResponse-v9-basic.json')
const COMMIT_RESPONSE = require('../../protocol/requests/offsetCommit/fixtures/java/offsetCommitResponse-v9-basic.json')

const TOPICS = [{ topic: 'test-topic', partitions: [{ partition: 0 }] }]
const COMMIT = {
  groupId: 'test-group',
  groupGenerationId: 5,
  memberId: 'test-member',
  groupInstanceId: 'test-instance',
  topics: [{ topic: 'test-topic', partitions: [{ partition: 0, offset: '10' }] }],
}

const fetchBroker = (maxVersion) =>
  createGroupApiBroker({
    versions: { [apiKeys.OffsetFetch]: { minVersion: 0, maxVersion } },
    respond: () => Buffer.from(FETCH_RESPONSE.hex, 'hex'),
  })

const commitBroker = (maxVersion) =>
  createGroupApiBroker({
    versions: { [apiKeys.OffsetCommit]: { minVersion: 0, maxVersion } },
    respond: () => Buffer.from(COMMIT_RESPONSE.hex, 'hex'),
  })

/**
 * Reads the group element of a recorded v9 OffsetFetch body.
 * @param {Buffer} body
 * @returns {{ groupId: string, memberId: string|null, memberEpoch: number }}
 */
const groupOf = (body) => {
  const decoder = new Decoder(body)
  return decoder.readUVarIntArray((group) => ({
    groupId: group.readUVarIntString(),
    memberId: group.readUVarIntString(),
    memberEpoch: group.readInt32(),
  }))[0]
}

describe('Broker > offsets under a member epoch', () => {
  let harness

  afterEach(async () => {
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  test('puts the member id and epoch on the wire at OffsetFetch v9', async () => {
    harness = fetchBroker(9)

    await harness.broker.offsetFetch({
      groupId: 'test-group',
      topics: TOPICS,
      memberId: 'test-member',
      memberEpoch: 5,
    })

    expect(harness.requests[0].apiVersion).toEqual(9)
    expect(groupOf(harness.requests[0].body)).toEqual({
      groupId: 'test-group',
      memberId: 'test-member',
      memberEpoch: 5,
    })
  })

  test('sends the no-membership defaults at v9 when the caller has none', async () => {
    harness = fetchBroker(9)

    await harness.broker.offsetFetch({ groupId: 'test-group', topics: TOPICS })

    expect(groupOf(harness.requests[0].body)).toEqual({
      groupId: 'test-group',
      memberId: null,
      memberEpoch: -1,
    })
  })

  test('drops both fields at v8 rather than smuggling them into a tag buffer', async () => {
    // The compatibility claim of this whole task, asserted on bytes: a broker that negotiates v8
    // receives exactly what it received before, whatever the caller passed.
    harness = fetchBroker(8)

    await harness.broker.offsetFetch({
      groupId: 'test-group',
      topics: TOPICS,
      memberId: 'test-member',
      memberEpoch: 5,
    })
    await harness.broker.offsetFetch({ groupId: 'test-group', topics: TOPICS })

    const expected = await requests.OffsetFetch.protocol({ version: 8 })({
      groupId: 'test-group',
      topics: TOPICS,
    }).request.encode()

    expect(harness.requests[0].apiVersion).toEqual(8)
    expect(harness.requests[0].body).toEqual(harness.requests[1].body)
    expect(harness.requests[0].body).toEqual(expected.buffer)
  })

  test('decodes the v9 response into the shape every earlier version returns', async () => {
    harness = fetchBroker(9)

    const response = await harness.broker.offsetFetch({
      groupId: 'test-group',
      topics: TOPICS,
      memberId: 'test-member',
      memberEpoch: 5,
    })

    expect(response.responses).toEqual([
      {
        topic: 'test-topic',
        partitions: [
          { partition: 0, offset: '10', committedLeaderEpoch: 3, metadata: null, errorCode: 0 },
        ],
      },
    ])
  })

  test.each([5, 0, -1])(
    'sends generation/epoch %i unchanged at OffsetCommit v9',
    async (generationOrEpoch) => {
      harness = commitBroker(9)

      await harness.broker.offsetCommit({ ...COMMIT, groupGenerationId: generationOrEpoch })

      expect(harness.requests[0].apiVersion).toEqual(9)
      const decoder = new Decoder(harness.requests[0].body)
      expect(decoder.readUVarIntString()).toEqual('test-group')
      expect(decoder.readInt32()).toEqual(generationOrEpoch)
    }
  )

  test('produces the same OffsetCommit body at v8 and v9', async () => {
    harness = commitBroker(9)
    await harness.broker.offsetCommit(COMMIT)
    const atV9 = harness.requests[0].body
    await harness.pool.destroy()

    harness = commitBroker(8)
    await harness.broker.offsetCommit(COMMIT)

    expect(harness.requests[0].apiVersion).toEqual(8)
    expect(harness.requests[0].body).toEqual(atV9)
  })
})
