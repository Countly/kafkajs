const createAdmin = require('../index')
const apiKeys = require('../../protocol/requests/apiKeys')
const { createErrorFromCode } = require('../../protocol/error')
const {
  KafkaJSConnectionError,
  KafkaJSNonRetriableError,
  KafkaJSServerDoesNotSupportApiKey,
} = require('../../errors')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const { newLogger } = require('testHelpers')

const UNKNOWN_TOPIC_OR_PARTITION = 3
const TOPIC_AUTHORIZATION_FAILED = 29
// Explicit, so that an attempt count means the same under every run mode: three attempts in all.
const RETRY = { retries: 2, initialRetryTime: 1, maxRetryTime: 1 }
const ZERO_UUID_AS_NULL = null

/** @returns {object} one partition row as `Broker#describeTopicPartitions` decodes it */
const partition = (partitionIndex, overrides = {}) => ({
  errorCode: 0,
  partitionIndex,
  leaderId: 1,
  leaderEpoch: 5,
  replicaNodes: [1, 2, 3],
  isrNodes: [1, 2],
  eligibleLeaderReplicas: null,
  lastKnownElr: null,
  offlineReplicas: [3],
  ...overrides,
})

/** @returns {object} one topic row; its id is derived from its name */
const topic = (name, partitions, overrides = {}) => ({
  errorCode: 0,
  name,
  topicId: `00000000-0000-0000-0000-00000000000${name.length}`,
  isInternal: false,
  partitions,
  topicAuthorizedOperations: -2147483648,
  ...overrides,
})

/** @returns {object} one page, as the broker method resolves it */
const page = (topics, nextCursor = null) => ({
  throttleTime: 0,
  clientSideThrottleTime: 0,
  topics,
  nextCursor,
})

/**
 * An admin over a stub broker that answers DescribeTopicPartitions with `pages` in turn, and
 * Metadata with `metadata`. `failFirst` lists errors the stub throws before it answers, `failWith`
 * one it throws every time. Every request the stub received is kept, copied, in `requests`. The
 * cluster hands the stub out from `findConnectedBroker`, and has no controller lookup at all.
 */
const adminOver = ({
  pages = [],
  metadata = { topicMetadata: [] },
  failWith,
  failFirst = [],
  retry = { retries: 0 },
} = {}) => {
  const requests = []
  const queue = [...pages]
  const failures = [...failFirst]
  const broker = {
    describeTopicPartitions: jest.fn(async (request) => {
      requests.push({ ...request, topics: [...request.topics] })
      if (failures.length > 0) {
        throw failures.shift()
      }
      if (failWith) {
        throw failWith
      }
      return queue.shift()
    }),
  }
  const cluster = {
    allowAutoTopicCreation: false,
    refreshMetadata: jest.fn(async () => {}),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    findConnectedBroker: jest.fn(async () => broker),
    metadata: jest.fn(async () => metadata),
  }
  const admin = createAdmin({ logger: newLogger(), retry, cluster })

  return { admin, broker, cluster, requests }
}

const fetchWithDtp = (admin, topics) =>
  admin.fetchTopicMetadata({ topics, useDescribeTopicPartitions: true })

describe('Admin > fetchTopicMetadata > DescribeTopicPartitions', () => {
  describe('paging', () => {
    test('keeps the cursor topic in the list, drops finished topics and counts what it has read', async () => {
      const { admin, requests } = adminOver({
        pages: [
          page([topic('a', [partition(0), partition(1)])], { topicName: 'a', partitionIndex: 2 }),
          page([topic('a', [partition(2)]), topic('b', [partition(0)])], {
            topicName: 'b',
            partitionIndex: 1,
          }),
          page([topic('b', [partition(1)])]),
        ],
      })

      const { topics } = await fetchWithDtp(admin, ['b', 'a'])

      expect(requests).toEqual([
        { topics: ['a', 'b'], responsePartitionLimit: 2000, cursor: null },
        {
          topics: ['a', 'b'],
          responsePartitionLimit: 2000,
          cursor: { topicName: 'a', partitionIndex: 2 },
        },
        {
          topics: ['b'],
          responsePartitionLimit: 2000,
          cursor: { topicName: 'b', partitionIndex: 1 },
        },
      ])
      expect(topics.map(({ name }) => name)).toEqual(['a', 'b'])
      expect(topics[0].partitions.map(({ partitionId }) => partitionId)).toEqual([0, 1, 2])
      expect(topics[1].partitions.map(({ partitionId }) => partitionId)).toEqual([0, 1])
    })

    test('resumes at the number of partitions read, whatever index the page names', async () => {
      const { admin, requests } = adminOver({
        pages: [
          page([topic('a', [partition(0), partition(1)])], { topicName: 'a', partitionIndex: 99 }),
          page([topic('a', [partition(2)])]),
        ],
      })

      await fetchWithDtp(admin, ['a'])

      expect(requests[1].cursor).toEqual({ topicName: 'a', partitionIndex: 2 })
    })

    test('sends no cursor when a page ends exactly at a topic boundary', async () => {
      // the broker answers the topic that did not fit as the cursor, at partition 0, and leaves it out
      const { admin, requests } = adminOver({
        pages: [
          page([topic('a', [partition(0)])], { topicName: 'b', partitionIndex: 0 }),
          page([topic('b', [partition(0)])]),
        ],
      })

      const { topics } = await fetchWithDtp(admin, ['a', 'b'])

      expect(requests.map(({ topics: asked, cursor }) => ({ asked, cursor }))).toEqual([
        { asked: ['a', 'b'], cursor: null },
        { asked: ['b'], cursor: null },
      ])
      expect(topics.map(({ name }) => name)).toEqual(['a', 'b'])
    })

    test('sorts the names by UTF-16 code unit and sends a duplicate once', async () => {
      const { admin, requests } = adminOver({
        pages: [
          page([
            topic('B', [partition(0)]),
            topic('a', [partition(0)]),
            topic('b', [partition(0)]),
          ]),
        ],
      })

      const { topics } = await fetchWithDtp(admin, ['b', 'a', 'B', 'a'])

      // localeCompare would put "a" before "B"
      expect(requests[0].topics).toEqual(['B', 'a', 'b'])
      expect(topics.map(({ name }) => name)).toEqual(['B', 'a', 'b'])
    })

    test('ends on a null cursor and never on a count', async () => {
      const partitions = Array.from({ length: 2500 }, (_, index) => partition(index))
      const { admin, broker } = adminOver({ pages: [page([topic('big', partitions)])] })

      const { topics } = await fetchWithDtp(admin, ['big'])

      expect(broker.describeTopicPartitions).toHaveBeenCalledTimes(1)
      expect(topics[0].partitions).toHaveLength(2500)
    })

    test('fails instead of asking for ever when a broker repeats a page', async () => {
      const repeated = page([topic('a', [partition(0)])], { topicName: 'a', partitionIndex: 1 })
      const { admin, requests } = adminOver({ pages: [repeated, repeated, repeated] })

      await expect(fetchWithDtp(admin, ['a'])).rejects.toMatchObject({
        name: 'KafkaJSNonRetriableError',
        message: expect.stringContaining('partition 0 of a where 1 was next'),
      })

      expect(requests).toHaveLength(2)
    })

    test('fails when a page starts a topic anywhere but where the last one stopped', async () => {
      const { admin } = adminOver({ pages: [page([topic('a', [partition(3)])])] })

      await expect(fetchWithDtp(admin, ['a'])).rejects.toMatchObject({
        name: 'KafkaJSNonRetriableError',
        message: expect.stringContaining('partition 3 of a where 0 was next'),
      })
    })

    test.each([
      ['no topic', []],
      ['a topic with nothing in it', [topic('a', [])]],
      ['a topic that was not asked for', [topic('other', [partition(0)])]],
    ])('fails when a page names a cursor and holds %s', async (_, rows) => {
      const { admin, requests } = adminOver({
        pages: [
          page(rows, { topicName: 'a', partitionIndex: 0 }),
          page([topic('a', [partition(0)])]),
        ],
      })

      await expect(fetchWithDtp(admin, ['a'])).rejects.toMatchObject({
        name: 'KafkaJSNonRetriableError',
        message: expect.stringContaining('named the cursor a:0 and sent nothing new'),
      })

      expect(requests).toHaveLength(1)
    })

    test('fails when the broker ends the read without answering for a topic', async () => {
      const { admin } = adminOver({ pages: [page([topic('a', [partition(0)])])] })

      await expect(fetchWithDtp(admin, ['a', 'b'])).rejects.toMatchObject({
        name: 'KafkaJSNonRetriableError',
        message: expect.stringContaining('without answering for b'),
      })
    })

    test('ignores a row for a topic that was not asked for', async () => {
      const { admin } = adminOver({
        pages: [page([topic('other', [partition(0)]), topic('a', [partition(0)])])],
      })

      const { topics } = await fetchWithDtp(admin, ['a'])

      expect(topics.map(({ name }) => name)).toEqual(['a'])
    })

    test('sends no cursor when a topic cut off mid-way is followed by a page that ends at a boundary', async () => {
      const { admin, requests } = adminOver({
        pages: [
          page([topic('a', [partition(0), partition(1)])], { topicName: 'a', partitionIndex: 2 }),
          page([topic('a', [partition(2)])], { topicName: 'b', partitionIndex: 0 }),
          page([topic('b', [partition(0)])]),
        ],
      })

      const { topics } = await fetchWithDtp(admin, ['a', 'b'])

      expect(requests.map(({ topics: asked, cursor }) => ({ asked, cursor }))).toEqual([
        { asked: ['a', 'b'], cursor: null },
        { asked: ['a', 'b'], cursor: { topicName: 'a', partitionIndex: 2 } },
        { asked: ['b'], cursor: null },
      ])
      expect(topics.map(({ partitions }) => partitions.length)).toEqual([3, 1])
    })

    test('fails a topic that is deleted between two pages of it', async () => {
      const { admin, requests } = adminOver({
        pages: [
          page([topic('a', [partition(0), partition(1)])], { topicName: 'a', partitionIndex: 2 }),
          page([topic('a', [], { errorCode: UNKNOWN_TOPIC_OR_PARTITION, topicId: null })]),
        ],
      })

      await expect(fetchWithDtp(admin, ['a'])).rejects.toMatchObject({
        type: 'UNKNOWN_TOPIC_OR_PARTITION',
      })

      expect(requests).toHaveLength(2)
    })

    test('asks for no cursor when the one a page names belongs to a topic that failed', async () => {
      const { admin, requests } = adminOver({
        pages: [
          page(
            [topic('a', [], { errorCode: UNKNOWN_TOPIC_OR_PARTITION, topicId: ZERO_UUID_AS_NULL })],
            { topicName: 'a', partitionIndex: 0 }
          ),
          page([topic('b', [partition(0)])]),
        ],
      })

      await expect(fetchWithDtp(admin, ['a', 'b'])).rejects.toMatchObject({
        type: 'UNKNOWN_TOPIC_OR_PARTITION',
      })

      expect(requests[1]).toMatchObject({ topics: ['b'], cursor: null })
    })
  })

  describe('results', () => {
    test('maps each row onto the Metadata shape and adds the new keys', async () => {
      const { admin } = adminOver({
        pages: [
          page([
            topic(
              'a',
              [
                partition(0, { eligibleLeaderReplicas: [], lastKnownElr: [] }),
                partition(1, { leaderId: -1, leaderEpoch: -1, errorCode: 5, isrNodes: [] }),
                partition(2, { eligibleLeaderReplicas: [2], lastKnownElr: [3] }),
              ],
              { isInternal: true }
            ),
          ]),
        ],
      })

      const { topics } = await fetchWithDtp(admin, ['a'])

      expect(topics).toEqual([
        {
          name: 'a',
          topicId: '00000000-0000-0000-0000-000000000001',
          isInternal: true,
          partitions: [
            {
              partitionErrorCode: 0,
              partitionId: 0,
              leader: 1,
              replicas: [1, 2, 3],
              isr: [1, 2],
              offlineReplicas: [3],
              leaderEpoch: 5,
              eligibleLeaderReplicas: [],
              lastKnownElr: [],
            },
            {
              partitionErrorCode: 5,
              partitionId: 1,
              leader: -1,
              replicas: [1, 2, 3],
              isr: [],
              offlineReplicas: [3],
              leaderEpoch: -1,
              eligibleLeaderReplicas: null,
              lastKnownElr: null,
            },
            {
              partitionErrorCode: 0,
              partitionId: 2,
              leader: 1,
              replicas: [1, 2, 3],
              isr: [1, 2],
              offlineReplicas: [3],
              leaderEpoch: 5,
              eligibleLeaderReplicas: [2],
              lastKnownElr: [3],
            },
          ],
        },
      ])
    })

    test('returns the topics in name order, whatever order the broker listed them in', async () => {
      const { admin } = adminOver({
        pages: [page([topic('b', [partition(0)]), topic('a', [partition(0)])])],
      })

      const { topics } = await fetchWithDtp(admin, ['a', 'b'])

      expect(topics.map(({ name }) => name)).toEqual(['a', 'b'])
    })

    test('fails the call with the error of the first failing topic, after every page was read', async () => {
      const { admin, requests } = adminOver({
        pages: [
          page(
            [
              topic('good', [partition(0)]),
              topic('unauthorized', [], {
                errorCode: TOPIC_AUTHORIZATION_FAILED,
                topicId: ZERO_UUID_AS_NULL,
              }),
              topic('absent', [], { errorCode: UNKNOWN_TOPIC_OR_PARTITION, topicId: null }),
            ],
            { topicName: 'good', partitionIndex: 1 }
          ),
          page([topic('good', [partition(1)])]),
        ],
      })

      await expect(fetchWithDtp(admin, ['good', 'unauthorized', 'absent'])).rejects.toMatchObject({
        type: 'UNKNOWN_TOPIC_OR_PARTITION',
      })

      // sorted by name, "absent" fails first; "good" was still read to its end
      expect(requests[1]).toMatchObject({
        topics: ['good'],
        cursor: { topicName: 'good', partitionIndex: 1 },
      })
    })

    test('does not throw for a partition that has no leader', async () => {
      const { admin } = adminOver({
        pages: [page([topic('a', [partition(0, { errorCode: 5, leaderId: -1 })])])],
      })

      const { topics } = await fetchWithDtp(admin, ['a'])

      expect(topics[0].partitions[0]).toMatchObject({ partitionErrorCode: 5, leader: -1 })
    })

    test('refreshes the metadata only if needed before the first attempt, then asks a connected broker', async () => {
      const { admin, cluster, broker } = adminOver({ pages: [page([topic('a', [partition(0)])])] })

      await fetchWithDtp(admin, ['a'])

      expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalledTimes(1)
      expect(cluster.findConnectedBroker).toHaveBeenCalledTimes(1)
      expect(broker.describeTopicPartitions).toHaveBeenCalledTimes(1)
      expect(cluster.refreshMetadata).not.toHaveBeenCalled()
      expect(cluster.metadata).not.toHaveBeenCalled()
    })
  })

  describe('which path a call takes', () => {
    const metadata = {
      topicMetadata: [
        {
          topic: 'm',
          topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
          isInternal: false,
          partitionMetadata: [{ partitionId: 0, leader: 1 }],
        },
        { topic: 'zero', topicId: null, isInternal: true, partitionMetadata: [] },
      ],
    }
    const metadataResult = {
      topics: [
        {
          name: 'm',
          topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
          isInternal: false,
          partitions: [{ partitionId: 0, leader: 1 }],
        },
        { name: 'zero', topicId: null, isInternal: true, partitions: [] },
      ],
    }

    test('the Metadata path returns the topic id and isInternal, null for a zero id', async () => {
      const { admin, broker, cluster } = adminOver({ metadata })

      await expect(admin.fetchTopicMetadata({ topics: ['m', 'zero'] })).resolves.toEqual(
        metadataResult
      )

      expect(cluster.metadata).toHaveBeenCalledWith({ topics: ['m', 'zero'] })
      expect(broker.describeTopicPartitions).not.toHaveBeenCalled()
    })

    test('takes the Metadata path when it is called with no argument at all', async () => {
      const { admin, cluster } = adminOver({ metadata })

      await expect(admin.fetchTopicMetadata()).resolves.toEqual(metadataResult)

      expect(cluster.metadata).toHaveBeenCalledWith({ topics: [] })
    })

    test.each([[undefined], [null], [[]]])(
      'takes the Metadata path when the option is set and topics is %j',
      async (topics) => {
        const { admin, broker, cluster } = adminOver({ metadata })

        await expect(
          admin.fetchTopicMetadata({ topics, useDescribeTopicPartitions: true })
        ).resolves.toEqual(metadataResult)

        expect(cluster.metadata).toHaveBeenCalledTimes(1)
        expect(broker.describeTopicPartitions).not.toHaveBeenCalled()
      }
    )

    test('falls back to the Metadata path when the broker does not offer the API', async () => {
      const { admin, broker, cluster } = adminOver({
        metadata,
        failWith: new KafkaJSServerDoesNotSupportApiKey('not offered', { apiKey: 75 }),
      })

      await expect(fetchWithDtp(admin, ['m', 'zero'])).resolves.toEqual(metadataResult)

      expect(broker.describeTopicPartitions).toHaveBeenCalledTimes(1)
      expect(cluster.metadata).toHaveBeenCalledTimes(1)
    })

    test('asks again after a metadata refresh when a request fails in a way that can pass', async () => {
      const { admin, broker, cluster } = adminOver({
        metadata,
        retry: RETRY,
        failFirst: [new KafkaJSConnectionError('Connection error: ECONNRESET')],
        pages: [page([topic('m', [partition(0)])])],
      })

      const { topics } = await fetchWithDtp(admin, ['m'])

      expect(topics.map(({ name }) => name)).toEqual(['m'])
      expect(broker.describeTopicPartitions).toHaveBeenCalledTimes(2)
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
      expect(cluster.findConnectedBroker).toHaveBeenCalledTimes(2)
      expect(cluster.metadata).not.toHaveBeenCalled()
    })

    test('asks another connected broker when the first one fails with a connection error', async () => {
      const { admin, broker, cluster } = adminOver({
        metadata,
        retry: RETRY,
        pages: [page([topic('m', [partition(0)])])],
      })
      const dead = {
        describeTopicPartitions: jest.fn(async () => {
          throw new KafkaJSConnectionError('Connection error: ECONNRESET')
        }),
      }
      cluster.findConnectedBroker.mockResolvedValueOnce(dead)

      const { topics } = await fetchWithDtp(admin, ['m'])

      expect(topics.map(({ name }) => name)).toEqual(['m'])
      expect(dead.describeTopicPartitions).toHaveBeenCalledTimes(1)
      expect(broker.describeTopicPartitions).toHaveBeenCalledTimes(1)
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
    })

    test('rejects, and does not fall back, when a retriable failure outlives the retry budget', async () => {
      const failure = new KafkaJSConnectionError('Connection error: ECONNRESET')
      const { admin, broker, cluster } = adminOver({ metadata, retry: RETRY, failWith: failure })

      await expect(fetchWithDtp(admin, ['m'])).rejects.toMatchObject({
        name: 'KafkaJSNumberOfRetriesExceeded',
        cause: failure,
      })

      expect(broker.describeTopicPartitions).toHaveBeenCalledTimes(RETRY.retries + 1)
      expect(cluster.metadata).not.toHaveBeenCalled()
    })

    test('does not retry or fall back for a failure that cannot pass', async () => {
      const failure = new KafkaJSNonRetriableError('refused')
      const { admin, broker, cluster } = adminOver({ metadata, retry: RETRY, failWith: failure })

      await expect(fetchWithDtp(admin, ['m'])).rejects.toBe(failure)

      expect(broker.describeTopicPartitions).toHaveBeenCalledTimes(1)
      expect(cluster.metadata).not.toHaveBeenCalled()
    })

    test('does not fall back for a topic that failed', async () => {
      const { admin, broker, cluster } = adminOver({
        metadata,
        retry: RETRY,
        pages: [page([topic('m', [], { errorCode: TOPIC_AUTHORIZATION_FAILED, topicId: null })])],
      })

      await expect(fetchWithDtp(admin, ['m'])).rejects.toMatchObject({
        type: createErrorFromCode(TOPIC_AUTHORIZATION_FAILED).type,
      })
      expect(broker.describeTopicPartitions).toHaveBeenCalledTimes(1)
      expect(cluster.metadata).not.toHaveBeenCalled()
    })

    test('still refuses an invalid topic name before taking either path', async () => {
      const { admin, broker, cluster } = adminOver({ metadata })

      await expect(fetchWithDtp(admin, ['m', null])).rejects.toHaveProperty(
        'message',
        'Invalid topic null'
      )
      expect(broker.describeTopicPartitions).not.toHaveBeenCalled()
      expect(cluster.metadata).not.toHaveBeenCalled()
    })
  })

  describe('on the wire', () => {
    let harness

    afterEach(async () => {
      await harness.pool.destroy()
    })

    test('a real broker answers the page and the admin keeps the eligible leader replicas as sent', async () => {
      const vector = require('../../protocol/requests/describeTopicPartitions/fixtures/java/describeTopicPartitionsResponse-v0-elrEmpty.json')
      harness = createGroupApiBroker({
        versions: { [apiKeys.DescribeTopicPartitions]: { minVersion: 0, maxVersion: 0 } },
        respond: () => Buffer.from(vector.hex, 'hex'),
      })
      const admin = createAdmin({
        logger: newLogger(),
        retry: { retries: 0 },
        cluster: {
          refreshMetadataIfNecessary: async () => {},
          findConnectedBroker: async () => harness.broker,
        },
      })

      const { topics } = await fetchWithDtp(admin, ['test-topic'])

      expect(harness.requests[0]).toMatchObject({ apiKey: 75, apiVersion: 0 })
      expect(topics).toEqual([
        {
          name: 'test-topic',
          topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
          isInternal: false,
          partitions: [
            {
              partitionErrorCode: 0,
              partitionId: 0,
              leader: 1,
              replicas: [1, 2, 3],
              isr: [1, 2],
              offlineReplicas: [3],
              leaderEpoch: 5,
              eligibleLeaderReplicas: [],
              lastKnownElr: [],
            },
          ],
        },
      ])
    })
  })
})
