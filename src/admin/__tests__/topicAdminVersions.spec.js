const createAdmin = require('../index')
const apiKeys = require('../../protocol/requests/apiKeys')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const { newLogger } = require('testHelpers')

/**
 * @param {string} api the lower-camel directory of the API, e.g. `createTopics`
 * @param {'Request'|'Response'} kind
 * @param {number} version
 * @param {string} caseId
 * @returns {{ hex: string, fields: object }} a vector Apache Kafka's own codec produced
 */
const vector = (api, kind, version, caseId) =>
  require(`../../protocol/requests/${api}/fixtures/java/${api}${kind}-v${version}-${caseId}.json`)

describe('Admin > topic, partition and group administration at the newest versions', () => {
  let harness

  /**
   * An admin over one real broker on a fake socket that offers one API up to `maxVersion` and
   * answers every request with `hex`. The harness serves a single request, and each case sends one.
   */
  const adminOver = (apiKey, maxVersion, hex) => {
    harness = createGroupApiBroker({
      versions: { [apiKey]: { minVersion: 0, maxVersion } },
      respond: () => Buffer.from(hex, 'hex'),
    })

    return createAdmin({
      logger: newLogger(),
      retry: { retries: 0 },
      cluster: {
        refreshMetadata: async () => {},
        findControllerBroker: async () => harness.broker,
        findGroupCoordinator: async () => harness.broker,
        targetTopics: new Set(),
      },
    })
  }

  const topics = [{ topic: 'test-topic' }]

  afterEach(async () => {
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  describe('createTopics', () => {
    // v4 has the bytes of v3, so its vectors stand in for a v3 answer.
    test.each([
      [7, 'ok', 7, true],
      [7, 'error', 7, false],
      [7, 'topicConfigError', 7, true],
      [3, 'ok', 4, true],
      [3, 'error', 4, false],
    ])(
      'at v%d the %s answer (vector of v%d) resolves %s',
      async (version, caseId, vectorVersion, resolved) => {
        const { hex } = vector('createTopics', 'Response', vectorVersion, caseId)
        const admin = adminOver(apiKeys.CreateTopics, version, hex)

        await expect(admin.createTopics({ topics, waitForLeaders: false })).resolves.toBe(resolved)
        expect(harness.requests[0].apiVersion).toEqual(version)
      }
    )

    test('the broker answers with each topic id and the new keys beside the 2.2.4 ones', async () => {
      adminOver(apiKeys.CreateTopics, 7, vector('createTopics', 'Response', 7, 'ok').hex)

      const response = await harness.broker.createTopics({ topics })

      expect(response.topicErrors[0]).toMatchObject({
        topic: 'test-topic',
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        errorCode: 0,
        errorMessage: null,
        numPartitions: 3,
        replicationFactor: 2,
        topicConfigErrorCode: 0,
      })
    })
  })

  describe('deleteTopics', () => {
    test('sends each name with the zero uuid at v6 and resolves', async () => {
      const admin = adminOver(
        apiKeys.DeleteTopics,
        6,
        vector('deleteTopics', 'Response', 6, 'ok').hex
      )
      const request = vector('deleteTopics', 'Request', 6, 'byName')

      await expect(admin.deleteTopics({ topics: request.fields.topics })).resolves.toBeUndefined()
      expect(harness.requests[0].body.toString('hex')).toEqual(request.hex)
    })

    test('rejects TOPIC_DELETION_DISABLED, which v3 and later may return', async () => {
      const { hex } = vector('deleteTopics', 'Response', 6, 'deletionDisabled')
      const admin = adminOver(apiKeys.DeleteTopics, 6, hex)

      await expect(admin.deleteTopics({ topics: ['other-topic'] })).rejects.toMatchObject({
        type: 'TOPIC_DELETION_DISABLED',
      })
    })
  })

  describe('createPartitions', () => {
    test('writes an empty list of assignments as the compact null at v3', async () => {
      const admin = adminOver(
        apiKeys.CreatePartitions,
        3,
        vector('createPartitions', 'Response', 3, 'ok').hex
      )

      await admin.createPartitions({
        topicPartitions: [{ topic: 'test-topic', count: 3, assignments: [] }],
      })

      expect(harness.requests[0].body.toString('hex')).toEqual(
        vector('createPartitions', 'Request', 3, 'noAssignments').hex
      )
    })
  })

  describe('deleteRecords', () => {
    test('the broker sends v2 and answers with the shape 2.2.4 callers read', async () => {
      const response = vector('deleteRecords', 'Response', 2, 'ok')
      adminOver(apiKeys.DeleteRecords, 2, response.hex)
      const request = vector('deleteRecords', 'Request', 2, 'basic')

      await expect(
        harness.broker.deleteRecords({ topics: request.fields.topics })
      ).resolves.toEqual({
        throttleTime: 0,
        clientSideThrottleTime: 25,
        topics: [
          {
            topic: 'test-topic',
            partitions: [
              { partition: 0, lowWatermark: 5n, errorCode: 0 },
              { partition: 2, lowWatermark: 12n, errorCode: 0 },
            ],
          },
        ],
      })
      expect(harness.requests[0].body.toString('hex')).toEqual(request.hex)
    })
  })

  describe('deleteGroups', () => {
    const { fields } = vector('deleteGroups', 'Request', 2, 'basic')

    test('resolves the result of each group at v2', async () => {
      const admin = adminOver(
        apiKeys.DeleteGroups,
        2,
        vector('deleteGroups', 'Response', 2, 'ok').hex
      )

      await expect(admin.deleteGroups(fields.groupIds)).resolves.toEqual([
        { groupId: 'test-group', errorCode: 0 },
        { groupId: 'other-group', errorCode: 0 },
      ])
      expect(harness.requests[0].body.toString('hex')).toEqual(
        vector('deleteGroups', 'Request', 2, 'basic').hex
      )
    })

    test('rejects with the error of each failed group, as it always has', async () => {
      const admin = adminOver(
        apiKeys.DeleteGroups,
        2,
        vector('deleteGroups', 'Response', 2, 'error').hex
      )

      await expect(admin.deleteGroups(fields.groupIds)).rejects.toMatchObject({
        name: 'KafkaJSDeleteGroupsError',
        groups: [
          { groupId: 'test-group', errorCode: 68, error: expect.objectContaining({ code: 68 }) },
          { groupId: 'other-group', errorCode: 69, error: expect.objectContaining({ code: 69 }) },
        ],
      })
    })
  })
})
