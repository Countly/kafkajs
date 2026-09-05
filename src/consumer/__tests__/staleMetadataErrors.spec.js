const { staleMetadata, createErrorFromCode } = require('../../protocol/error')
const ConsumerGroup = require('../consumerGroup')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { newLogger } = require('testHelpers')

/**
 * `ConsumerGroup#recoverFromFetch` is the only reader of the consumer's own list, and its
 * stale-metadata branch is observable: it refreshes metadata and rejoins. Driving it is how this
 * spec tests that list — `STALE_METADATA_ERRORS` is a module-local const in `consumerGroup.js`
 * and stays one; nothing is exported from production for a test.
 */
const buildGroup = () => {
  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    refreshMetadataIfNecessary: async () => {},
    committedOffsets: () => ({}),
    findTopicPartitionMetadata: () => [],
  }

  const consumerGroup = new ConsumerGroup({
    retry: { retries: 0 },
    cluster,
    groupId: 'test-group',
    topics: ['test-topic'],
    topicConfigurations: { 'test-topic': {} },
    logger: newLogger(),
    instrumentationEmitter: new InstrumentationEventEmitter(),
    assigners: [],
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
    maxBytesPerPartition: 1048576,
    minBytes: 1,
    maxBytes: 1048576,
    maxWaitTimeInMs: 100,
    autoCommit: false,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    isolationLevel: 0,
    rackId: '',
    checkCrcs: true,
    metadataMaxAge: 300000,
    pendingTopicResets: new Map(),
  })

  const joinAndSync = jest.spyOn(consumerGroup, 'joinAndSync').mockResolvedValue()
  return { consumerGroup, cluster, joinAndSync }
}

describe('stale-metadata error classification', () => {
  const NEW_CODES = [
    ['UNKNOWN_TOPIC_ID', 100],
    ['INCONSISTENT_TOPIC_ID', 103],
    ['FETCH_SESSION_TOPIC_ID_ERROR', 106],
  ]

  test.each(NEW_CODES)('%s is retriable and stale metadata for the producer', (type, code) => {
    const error = createErrorFromCode(code)
    expect(error.type).toEqual(type)
    expect(error.retriable).toBe(true)
    expect(staleMetadata(error)).toBe(true)
  })

  test.each(NEW_CODES)(
    '%s makes the consumer refresh metadata and rejoin, not crash',
    async (_type, code) => {
      const { consumerGroup, cluster, joinAndSync } = buildGroup()

      await consumerGroup.recoverFromFetch(createErrorFromCode(code))

      expect(cluster.refreshMetadata).toHaveBeenCalled()
      expect(joinAndSync).toHaveBeenCalled()
    }
  )

  test('the two lists keep their existing, deliberately different memberships', async () => {
    // FENCED_LEADER_EPOCH drives a consumer rejoin but is not producer stale metadata.
    expect(staleMetadata(createErrorFromCode(74))).toBe(false)
    expect(staleMetadata(createErrorFromCode(3))).toBe(true) // UNKNOWN_TOPIC_OR_PARTITION

    const { consumerGroup, joinAndSync } = buildGroup()
    await consumerGroup.recoverFromFetch(createErrorFromCode(74))
    expect(joinAndSync).toHaveBeenCalled()
  })

  test('an unrelated error is neither, and still propagates', async () => {
    expect(staleMetadata(createErrorFromCode(87))).toBe(false) // INVALID_RECORD

    const { consumerGroup, cluster, joinAndSync } = buildGroup()
    await expect(consumerGroup.recoverFromFetch(createErrorFromCode(87))).rejects.toMatchObject({
      code: 87,
    })
    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
    expect(joinAndSync).not.toHaveBeenCalled()
  })
})
