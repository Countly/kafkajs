const OffsetManager = require('../index')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { createErrorFromCode } = require('../../../protocol/error')
const { KafkaJSNumberOfRetriesExceeded } = require('../../../errors')
const { ON_WRITE } = require('../../../broker/privateOptions')
const NOT_COORDINATOR_FOR_GROUP_CODE = 16

describe('Consumer > OffsetMananger > commitOffsets', () => {
  let offsetManager,
    topic1,
    topic2,
    memberAssignment,
    mockCluster,
    mockCoordinator,
    groupId,
    generationId,
    memberId

  beforeEach(() => {
    topic1 = 'topic-1'
    topic2 = 'topic-2'
    groupId = 'groupId'
    generationId = 'generationId'
    memberId = 'memberId'

    memberAssignment = {
      [topic1]: [0, 1],
      [topic2]: [0, 1, 2, 3],
    }

    mockCluster = {
      committedOffsets: jest.fn(() => ({})),
      refreshMetadata: jest.fn(() => ({})),
      topicId: jest.fn(),
      findGroupCoordinator: jest.fn(async () => ({ isConnected: () => true, nodeId: 2 })),
    }

    mockCoordinator = {
      offsetCommit: jest.fn(),
    }

    offsetManager = new OffsetManager({
      cluster: mockCluster,
      memberAssignment,
      groupId,
      generationId,
      memberId,
      instrumentationEmitter: new InstrumentationEventEmitter(),
    })

    offsetManager.getCoordinator = jest.fn(() => mockCoordinator)
  })

  it('commits all the resolved offsets that have not already been committed', async () => {
    const defaultOffsetInt = 2

    Object.keys(memberAssignment).forEach((topic) => {
      memberAssignment[topic].forEach((partition) => {
        offsetManager.resolveOffset({ topic, partition, offset: defaultOffsetInt.toString() })
      })
    })

    const defaultResolvedOffsetInt = defaultOffsetInt + 1
    const defaultResolvedOffsetStr = defaultResolvedOffsetInt.toString()

    // If committed offset equal to resolved offset, then partition is marked as committed
    offsetManager.committedOffsets()[topic2][0] = defaultResolvedOffsetInt.toString() // "committed"

    await offsetManager.commitOffsets()

    expect(mockCoordinator.offsetCommit).toHaveBeenCalledWith({
      groupId,
      memberId,
      groupGenerationId: generationId,
      groupInstanceId: null,
      topics: [
        {
          topic: topic1,
          partitions: [
            { partition: '0', offset: defaultResolvedOffsetStr, leaderEpoch: -1 },
            { partition: '1', offset: defaultResolvedOffsetStr, leaderEpoch: -1 },
          ],
        },
        {
          topic: topic2,
          partitions: [
            { partition: '1', offset: defaultResolvedOffsetStr, leaderEpoch: -1 },
            { partition: '2', offset: defaultResolvedOffsetStr, leaderEpoch: -1 },
            { partition: '3', offset: defaultResolvedOffsetStr, leaderEpoch: -1 },
          ],
        },
      ],
    })

    expect(offsetManager.committedOffsets()).toEqual({
      'topic-1': {
        0: defaultResolvedOffsetStr,
        1: defaultResolvedOffsetStr,
      },
      'topic-2': {
        0: defaultResolvedOffsetStr,
        1: defaultResolvedOffsetStr,
        2: defaultResolvedOffsetStr,
        3: defaultResolvedOffsetStr,
      },
    })
  })

  it('commits only the selected partitions and sends nothing when none of them is uncommitted', async () => {
    offsetManager.resolveOffset({ topic: topic1, partition: 0, offset: '2' })
    offsetManager.resolveOffset({ topic: topic2, partition: 0, offset: '2' })
    const partitions = [{ topic: topic1, partitions: [0] }]

    await offsetManager.commitOffsets({}, { partitions })
    await offsetManager.commitOffsets({}, { partitions })

    expect(mockCoordinator.offsetCommit).toHaveBeenCalledTimes(1)
    expect(mockCoordinator.offsetCommit.mock.calls[0][0].topics).toEqual([
      { topic: topic1, partitions: [{ partition: '0', offset: '3', leaderEpoch: -1 }] },
    ])
  })

  it('commits any provided offsets', async () => {
    const offset = Math.random().toString()
    const offsets = { topics: [{ topic: topic1, partitions: [{ partition: '0', offset }] }] }
    await offsetManager.commitOffsets(offsets)

    expect(mockCoordinator.offsetCommit).toHaveBeenCalledWith({
      groupId,
      memberId,
      groupGenerationId: generationId,
      groupInstanceId: null,
      topics: [
        {
          topic: topic1,
          partitions: [{ partition: '0', offset }],
        },
      ],
    })
  })

  it('refreshes metadata on NOT_COORDINATOR_FOR_GROUP protocol error', async () => {
    mockCoordinator.offsetCommit.mockImplementation(() => {
      throw createErrorFromCode(NOT_COORDINATOR_FOR_GROUP_CODE)
    })

    const offset = Math.random().toString()
    const offsets = { topics: [{ topic: topic1, partitions: [{ partition: '0', offset }] }] }

    await expect(offsetManager.commitOffsets(offsets)).rejects.toThrow()
    expect(mockCluster.refreshMetadata).toHaveBeenCalled()
  })

  it.each([
    ['GROUP_COORDINATOR_NOT_AVAILABLE', 15],
    ['NOT_COORDINATOR_FOR_GROUP', 16],
  ])(
    're-discovers the coordinator on %s so the next commit targets the right broker',
    async (type, code) => {
      mockCoordinator.offsetCommit.mockImplementation(() => {
        throw createErrorFromCode(code)
      })

      const offsets = { topics: [{ topic: topic1, partitions: [{ partition: '0', offset: '1' }] }] }

      await expect(offsetManager.commitOffsets(offsets)).rejects.toThrow()
      expect(mockCluster.refreshMetadata).toHaveBeenCalled()
      expect(mockCluster.findGroupCoordinator).toHaveBeenCalledWith({ groupId })
    }
  )

  it('propagates the original retriable error when re-discovery itself fails', async () => {
    const originalError = createErrorFromCode(NOT_COORDINATOR_FOR_GROUP_CODE)
    mockCoordinator.offsetCommit.mockImplementation(() => {
      throw originalError
    })
    mockCluster.findGroupCoordinator.mockImplementation(() => {
      throw new KafkaJSNumberOfRetriesExceeded(new Error('exhausted'), {
        retryCount: 5,
        retryTime: 100,
      })
    })

    const offsets = { topics: [{ topic: topic1, partitions: [{ partition: '0', offset: '1' }] }] }

    // The retrier-wrapped findGroupCoordinator rejecting with a non-retriable
    // KafkaJSNumberOfRetriesExceeded must not replace the retriable original error -- the caller's
    // own retrier needs to see `originalError` to know it can retry the commit.
    await expect(offsetManager.commitOffsets(offsets)).rejects.toBe(originalError)
    expect(mockCluster.refreshMetadata).toHaveBeenCalled()
  })

  /**
   * Starts a default reset of topic-1[0] over a cached committed offset of 7 whose commit fails.
   * @param {boolean} written whether the commit reached the socket before it failed
   * @returns {Promise<void>} once the reset rejected with the commit's error
   */
  const failDefaultReset = async (written) => {
    const failure = createErrorFromCode(NOT_COORDINATOR_FOR_GROUP_CODE)
    mockCluster.defaultOffset = jest.fn(() => '0')
    offsetManager.topicConfigurations = { [topic1]: { fromBeginning: false } }
    offsetManager.committedOffsets()[topic1] = { 0: '7' }
    mockCoordinator.offsetCommit.mockImplementation(async (request, options) => {
      if (written) options[ON_WRITE]()
      throw failure
    })
    await expect(offsetManager.setDefaultOffset({ topic: topic1, partition: 0 })).rejects.toBe(
      failure
    )
    expect(mockCoordinator.offsetCommit).toHaveBeenCalledTimes(1)
  }

  it('forgets the cached committed offset when a default reset commit was written and failed', async () => {
    await failDefaultReset(true)
    expect(offsetManager.committedOffsets()[topic1]).toEqual({})
  })

  it('keeps the cached committed offset when a default reset commit failed before it was written', async () => {
    await failDefaultReset(false)
    expect(offsetManager.committedOffsets()[topic1]).toEqual({ 0: '7' })
  })
})
