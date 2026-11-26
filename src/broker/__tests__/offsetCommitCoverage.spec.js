const { createGroupApiBroker } = require('./fixtures/groupApiHarness')
const { ON_DECODED, UNCHECKED_ROWS, acceptedIndex } = require('../privateOptions')
const apiKeys = require('../../protocol/requests/apiKeys')
const offsetCommitV9 = require('../../../testHelpers/wire/offsetCommitV9')
const offsetCommitV10 = require('../../../testHelpers/wire/offsetCommitV10')

const TOPIC = 'test-topic'
const OTHER = 'other-topic'
const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const OTHER_ID = 'ffffffff-0506-0708-090a-0b0c0d0e0f10'
const VERSIONS = [8, 9, 10]

/**
 * One topic entry of an answer, named at v8 and v9 and keyed by id at v10.
 * @param {string} name
 * @param {string} id
 * @param {string} rows partition and error code of each row in answer order, as 'p0:0 p1:0'
 * @returns {{ name: string, id: string, partitions: { partition: number, errorCode: number }[] }}
 */
const topic = (name, id, rows) => ({
  name,
  id,
  partitions: rows
    .split(' ')
    .filter((row) => row !== '')
    .map((row) => {
      const [partition, errorCode] = row.slice(1).split(':').map(Number)
      return { partition, errorCode }
    }),
})

const ASKED = topic(TOPIC, TOPIC_ID, 'p0:0 p1:0')

/**
 * @param {number} version
 * @param {ReturnType<typeof topic>[]} entries
 * @returns {Buffer} the answer's body as the broker writes it at `version`
 */
const answerAt = (version, entries) =>
  version >= 10
    ? offsetCommitV10.buildResponse({
        throttleTime: 0,
        topics: entries.map(({ id, partitions }) => ({ topicId: id, partitions })),
      })
    : offsetCommitV9.buildResponse({
        throttleTime: 0,
        topics: entries.map(({ name, partitions }) => ({ topic: name, partitions })),
      })

/**
 * @param {number} version
 * @param {number[]} partitions the partitions the commit names, in order
 * @returns {object} the `Broker#offsetCommit` request; at v10 its entry carries the topic id
 */
const requestAt = (version, partitions) => ({
  groupId: 'test-group',
  groupGenerationId: 5,
  memberId: 'test-member',
  topics: [
    {
      topic: TOPIC,
      ...(version === 10 ? { topicId: TOPIC_ID } : {}),
      partitions: partitions.map((partition) => ({ partition, offset: '10' })),
    },
  ],
})

/** @returns {object} `acceptedIndex(decoded)` as plain data: accepted partitions by topic */
const indexed = (decoded) =>
  Object.fromEntries([...acceptedIndex(decoded)].map(([name, cells]) => [name, [...cells]]))

describe('Broker > offsetCommit answer coverage', () => {
  const harnesses = []

  afterEach(async () => {
    await Promise.all(harnesses.splice(0).map(({ pool }) => pool.destroy()))
  })

  /**
   * One commit answered with `entries` encoded at `version`, through a real broker and connection.
   * @param {number} version
   * @param {ReturnType<typeof topic>[]} entries
   * @param {object} [options]
   * @param {number[]} [options.partitions=[0, 1]] the partitions the commit names
   * @param {object} [options.internal] the private options the consumer passes
   * @returns {Promise<object>}
   */
  const commit = (version, entries, { partitions = [0, 1], internal } = {}) => {
    const harness = createGroupApiBroker({
      versions: { [apiKeys.OffsetCommit]: { minVersion: 0, maxVersion: version } },
      respond: () => answerAt(version, entries),
    })
    harnesses.push(harness)
    return harness.broker.offsetCommit(requestAt(version, partitions), internal)
  }

  /** @returns {number[]} the OffsetCommit versions the latest broker was asked in */
  const versionsSent = () => harnesses[harnesses.length - 1].requests.map((r) => r.apiVersion)

  /**
   * @param {object} extra private options besides the observer
   * @returns {{ seen: object[], internal: object }} what each decoded reply accepted, and the
   *   private options that collect it
   */
  const observed = (extra = {}) => {
    const seen = []
    return { seen, internal: { ...extra, [ON_DECODED]: (decoded) => seen.push(indexed(decoded)) } }
  }

  test.each([
    ['no rows at all', []],
    ['a missing partition', [topic(TOPIC, TOPIC_ID, 'p0:0')]],
    ['an unrequested topic beside the requested rows', [ASKED, topic(OTHER, OTHER_ID, 'p0:0')]],
    ['an unrequested topic with no partitions', [ASKED, topic(OTHER, OTHER_ID, '')]],
    ['only another topic', [topic(OTHER, OTHER_ID, 'p0:0 p1:0')]],
    ['an unrequested partition', [topic(TOPIC, TOPIC_ID, 'p0:0 p1:0 p7:0')]],
  ])('%s is refused retriably before the decoded-reply hook sees it', async (_, entries) => {
    for (const version of VERSIONS) {
      const { seen, internal } = observed()

      await expect(commit(version, entries, { internal })).rejects.toMatchObject({
        name: 'KafkaJSError',
        retriable: true,
        message: expect.stringMatching(/^OffsetCommit response /),
      })
      expect(seen).toEqual([])
      expect(versionsSent()).toEqual([version])
    }
  })

  test('an answer [p0: 0, p0: 27] for a request naming p0 once is refused before the hook sees it', async () => {
    for (const version of VERSIONS) {
      const { seen, internal } = observed()

      await expect(
        commit(version, [topic(TOPIC, TOPIC_ID, 'p0:0 p0:27')], { partitions: [0], internal })
      ).rejects.toMatchObject({
        name: 'KafkaJSError',
        retriable: true,
        message: expect.stringMatching(/partition 0 of topic test-topic twice/),
      })
      expect(seen).toEqual([])
    }
  })

  test.each([['p0:0 p0:12'], ['p0:12 p0:0']])(
    'a request naming p0 twice, answered %s, rejects with the failed row and accepts no p0',
    async (rows) => {
      for (const version of VERSIONS) {
        const { seen, internal } = observed()

        await expect(
          commit(version, [topic(TOPIC, TOPIC_ID, rows)], { partitions: [0, 0], internal })
        ).rejects.toMatchObject({ type: 'OFFSET_METADATA_TOO_LARGE' })
        expect(seen).toEqual([{ [TOPIC]: [] }])
      }
    }
  )

  test('a request naming p0 twice, answered p0:0 p0:0, resolves and accepts p0', async () => {
    for (const version of VERSIONS) {
      const { seen, internal } = observed()

      await expect(
        commit(version, [topic(TOPIC, TOPIC_ID, 'p0:0 p0:0')], { partitions: [0, 0], internal })
      ).resolves.toMatchObject({
        responses: [
          {
            topic: TOPIC,
            partitions: [
              { partition: 0, errorCode: 0 },
              { partition: 0, errorCode: 0 },
            ],
          },
        ],
      })
      expect(seen).toEqual([{ [TOPIC]: [0] }])
    }
  })

  test('rows in another order are accepted, each partition once', async () => {
    for (const version of VERSIONS) {
      const { seen, internal } = observed()

      await expect(
        commit(version, [topic(TOPIC, TOPIC_ID, 'p1:0 p0:0')], { internal })
      ).resolves.toMatchObject({ responses: [{ topic: TOPIC }] })
      expect(seen).toEqual([{ [TOPIC]: [1, 0] }])
    }
  })

  test('a request-level error answer (every partition refused) is reported as that error, not as a coverage error', async () => {
    for (const version of VERSIONS) {
      const { seen, internal } = observed()

      await expect(
        commit(version, [topic(TOPIC, TOPIC_ID, 'p0:16 p1:16')], { internal })
      ).rejects.toMatchObject({ type: 'NOT_COORDINATOR_FOR_GROUP' })
      expect(seen).toEqual([{ [TOPIC]: [] }])
    }
  })

  test('a caller that reads every row itself is handed an answer that leaves a partition out', async () => {
    for (const version of VERSIONS) {
      const { seen, internal } = observed({ [UNCHECKED_ROWS]: true })

      await expect(
        commit(version, [topic(TOPIC, TOPIC_ID, 'p0:0')], { internal })
      ).resolves.toMatchObject({ responses: [{ topic: TOPIC, partitions: [{ partition: 0 }] }] })
      expect(seen).toEqual([{ [TOPIC]: [0] }])
    }
  })
})
