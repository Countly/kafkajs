const Runner = require('../runner')
const ConsumerGroup = require('../consumerGroup')
const PositionValidator = require('../positionValidator')
const Batch = require('../batch')
const createWorker = require('../worker')
const createWorkerQueue = require('../workerQueue')
const {
  events: { START_BATCH_PROCESS, END_BATCH_PROCESS },
} = require('../instrumentationEvents')
const Decoder = require('../../protocol/decoder')
const decodeMessages = require('../../protocol/requests/fetch/v4/decodeMessages')
const {
  batch: batchBytes,
  record: recordBytes,
} = require('../../protocol/requests/fetch/__tests__/fixtures/recordBatchBytes')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { newLogger, secureRandom, waitFor } = require('testHelpers')
const sleep = require('../../utils/sleep')
const {
  KafkaJSError,
  KafkaJSProtocolError,
  KafkaJSNotImplemented,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSDeliveryTimeout,
  KafkaJSCommitFailedError,
} = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const { stampOffsetCommit, stampMembership } = require('../rebalanceErrorContract')
const { ConsumerOperation } = require('../groupProtocol/interface')

const UNKNOWN = -1
const REBALANCE_IN_PROGRESS = 27
const rebalancingError = () => new KafkaJSProtocolError(createErrorFromCode(REBALANCE_IN_PROGRESS))

describe('Consumer > Runner', () => {
  let runner,
    consumerGroup,
    onCrash,
    eachBatch,
    topicName,
    partition,
    emptyBatch,
    instrumentationEmitter

  const createTestRunner = (partial) => {
    return new Runner({
      consumerGroup,
      onCrash,
      instrumentationEmitter,
      logger: newLogger(),
      eachBatch,
      concurrency: 1,
      heartbeatInterval: 3000,
      maxPollInterval: 300000,
      ...partial,
    })
  }

  beforeEach(() => {
    topicName = `topic-${secureRandom()}`
    partition = 0

    emptyBatch = new Batch(topicName, 0, {
      partition,
      highWatermark: 5,
      messages: [],
    })

    eachBatch = jest.fn()
    onCrash = jest.fn()
    consumerGroup = {
      groupId: 'test-group',
      memberId: 'test-member',
      generationId: 1,
      leaderId: 'test-member',
      groupInstanceId: null,
      groupMembership: {},
      forgetMemberId() {
        this.memberId = null
      },
      assignmentEpoch: 0,
      attach: jest.fn(),
      rebalanceProtocol: jest.fn(() => 'eager'),
      applyRebalanceResult: jest.fn(async () => ({ assignment: [], added: [], revoked: [] })),
      discardUncommittedOffsets: jest.fn(),
      clearStaleAssignmentPending: jest.fn(),
      forgetOwnedPartitions: jest.fn(),
      clearAssignerState: jest.fn(),
      resetGenerationLocally: jest.fn(),
      hasSeekOffset: () => false,
      isAssigned: () => true,
      leave: jest.fn(async () => {}),
      getNodeIds: jest.fn(() => [1, 2, 3]),
      connect: jest.fn(),
      join: jest.fn(),
      sync: jest.fn(),
      joinAndSync: jest.fn(async () => ({ assignment: {} })),
      fetch: jest.fn(async () => [emptyBatch]),
      resolveOffset: jest.fn(),
      commitOffsets: jest.fn(),
      commitOffsetsIfNecessary: jest.fn(),
      uncommittedOffsets: jest.fn(),
      heartbeat: jest.fn(),
      assigned: jest.fn(() => []),
      isLeader: jest.fn(() => true),
      isPaused: jest.fn().mockReturnValue(false),
      kind: 'classic',
      heartbeatInterval: jest.fn(() => 3000),
      onStopRequested: jest.fn(),
      // The Fetch-session seams of stop and forced retirement; the real group always has both.
      closeFetchSessions: jest.fn(async () => {}),
      retireFetchSessions: jest.fn(),
      retireCoordinatorLookup: jest.fn(),
      startIdleWait: jest.fn(() => ({})),
      beginRebalanceRound: jest.fn(() => ({
        guard: () => null,
        wait: (work) => work(),
        retire: jest.fn(),
      })),
      onMembershipError: jest.fn(),
      leaveForPollExpiry: jest.fn(async () => {}),
      needsReconciliation: jest.fn(() => false),
      resolvedOffsetAt: jest.fn(() => undefined),
    }
    instrumentationEmitter = new InstrumentationEventEmitter()

    runner = createTestRunner()
  })

  afterEach(async () => {
    if (!runner) {
      return
    }

    // Several cases set `running` directly to simulate a stopped consumer. Production only ever
    // clears it inside `stop()` or the crash path, both of which cancel the heartbeat chain, so
    // the flag is restored here and the runner is torn down through its own `stop()`.
    runner.running = true
    await runner.stop()
  })

  describe('heartbeat after stop', () => {
    it.each(['classic', 'consumer'])(
      'skips the ordinary %s heartbeat once stop is latched',
      async (kind) => {
        consumerGroup.kind = kind
        runner.running = true
        runner.latchStop()

        await expect(runner.heartbeat()).resolves.toBeUndefined()

        expect(consumerGroup.heartbeat).not.toHaveBeenCalled()
      }
    )

    it.each(['classic', 'consumer'])(
      'still rejects the %s heartbeat with the retirement reason',
      async (kind) => {
        consumerGroup.kind = kind
        runner.running = true
        const reason = new Error('the run was retired')
        runner.retire(reason)

        await expect(runner.heartbeat()).rejects.toBe(reason)

        expect(consumerGroup.heartbeat).not.toHaveBeenCalled()
      }
    )
  })

  describe('a heartbeat the group reports idle', () => {
    it('ends the heartbeat outage without calling the group heartbeat', async () => {
      consumerGroup.heartbeatIdle = jest.fn(() => true)
      runner.running = true
      runner.outages.heartbeat.since = 5

      await expect(runner.heartbeat()).resolves.toBeUndefined()

      expect(consumerGroup.heartbeatIdle).toHaveBeenCalledTimes(1)
      expect(consumerGroup.heartbeatIdle).toHaveBeenCalledWith(3000)
      expect(consumerGroup.heartbeat).not.toHaveBeenCalled()
      expect(runner.outages.heartbeat.since).toBeNull()
    })

    it('still calls the group heartbeat, with the same interval, and rejects with its error when the group reports it not idle', async () => {
      const failure = new Error('the joined heartbeat failed')
      consumerGroup.heartbeatIdle = jest.fn(() => false)
      consumerGroup.heartbeat.mockRejectedValueOnce(failure)
      runner.running = true
      runner.outages.heartbeat.since = 5

      await expect(runner.heartbeat()).rejects.toBe(failure)

      expect(consumerGroup.heartbeat).toHaveBeenCalledTimes(1)
      expect(consumerGroup.heartbeat).toHaveBeenCalledWith(
        expect.objectContaining({ interval: 3000 })
      )
      expect(runner.outages.heartbeat.since).toBe(5)
    })

    it('rejects with the retirement reason before asking the group whether it is idle', async () => {
      consumerGroup.heartbeatIdle = jest.fn(() => true)
      runner.running = true
      runner.outages.heartbeat.since = 5
      const reason = new Error('the run was retired')
      runner.retire(reason)

      await expect(runner.heartbeat()).rejects.toBe(reason)

      expect(consumerGroup.heartbeatIdle).not.toHaveBeenCalled()
      expect(consumerGroup.heartbeat).not.toHaveBeenCalled()
      expect(runner.outages.heartbeat.since).toBe(5)
    })

    it('does nothing once stop is latched, before asking the group whether it is idle', async () => {
      consumerGroup.heartbeatIdle = jest.fn(() => true)
      runner.running = true
      runner.outages.heartbeat.since = 5
      runner.latchStop()

      await expect(runner.heartbeat()).resolves.toBeUndefined()

      expect(consumerGroup.heartbeatIdle).not.toHaveBeenCalled()
      expect(consumerGroup.heartbeat).not.toHaveBeenCalled()
      expect(runner.outages.heartbeat.since).toBe(5)
    })
  })

  describe('pause from "eachMessage" while no heartbeat is due', () => {
    const twoMessages = () =>
      new Batch(topicName, 0, {
        partition,
        highWatermark: 5,
        messages: [
          { offset: '4', key: '1', value: '2' },
          { offset: '5', key: '3', value: '4' },
        ],
      })

    /** Runs one two-message batch through `eachMessage` and returns the offsets it was handed. */
    const deliveredOffsets = async (eachMessage) => {
      runner = createTestRunner({ eachBatch: undefined, eachMessage })
      runner.scheduleFetchManager = jest.fn()
      await runner.start()
      await runner.handleBatch(twoMessages())
      return eachMessage.mock.calls.map(([{ message }]) => message.offset)
    }

    beforeEach(() => {
      const paused = new Set()
      consumerGroup.heartbeatIdle = jest.fn(() => true)
      consumerGroup.pause = jest.fn((topicPartitions) => {
        for (const { topic, partitions } of topicPartitions) {
          for (const p of partitions) paused.add(`${topic}|${p}`)
        }
      })
      consumerGroup.isPaused = jest.fn((topic, p) => paused.has(`${topic}|${p}`))
    })

    it('delivers both messages when the handler does not pause', async () => {
      const offsets = await deliveredOffsets(jest.fn(async () => {}))

      expect(offsets).toEqual(['4', '5'])
      expect(consumerGroup.heartbeatIdle).toHaveBeenCalled()
      expect(consumerGroup.heartbeat).not.toHaveBeenCalled()
    })

    it('stops before the second message when the handler pauses synchronously', async () => {
      const offsets = await deliveredOffsets(
        jest.fn(async ({ pause }) => {
          pause()
        })
      )

      expect(offsets).toEqual(['4'])
      expect(consumerGroup.resolveOffset).toHaveBeenCalledTimes(1)
      expect(consumerGroup.heartbeatIdle).toHaveBeenCalled()
      expect(consumerGroup.heartbeat).not.toHaveBeenCalled()
    })

    it('stops before the second message when the handler pauses after an awaited operation', async () => {
      const offsets = await deliveredOffsets(
        jest.fn(async ({ pause }) => {
          await new Promise((resolve) => setImmediate(resolve))
          pause()
        })
      )

      expect(offsets).toEqual(['4'])
      expect(consumerGroup.resolveOffset).toHaveBeenCalledTimes(1)
      expect(consumerGroup.heartbeatIdle).toHaveBeenCalled()
      expect(consumerGroup.heartbeat).not.toHaveBeenCalled()
    })
  })

  describe('when the group is rebalancing before the new consumer has joined', () => {
    it('recovers from rebalance in progress and re-join the group', async () => {
      consumerGroup.sync
        .mockImplementationOnce(() => {
          throw rebalancingError()
        })
        .mockImplementationOnce(() => {
          throw rebalancingError()
        })
        .mockImplementationOnce(() => true)

      runner.scheduleFetchManager = jest.fn()
      await runner.start()
      expect(runner.scheduleFetchManager).toHaveBeenCalled()
      expect(onCrash).not.toHaveBeenCalled()
    })
  })

  it('should "commit" offsets during fetch', async () => {
    const batch = new Batch(topicName, 0, {
      partition,
      highWatermark: 5,
      messages: [{ offset: 4, key: '1', value: '2' }],
    })

    runner.scheduleFetchManager = jest.fn()
    await runner.start()
    await runner.handleBatch(batch) // Manually fetch for test
    expect(eachBatch).toHaveBeenCalled()
    expect(consumerGroup.commitOffsets).toHaveBeenCalled()
    expect(onCrash).not.toHaveBeenCalled()
  })

  describe('"eachBatch" callback', () => {
    it('allows providing offsets to "commitOffsetIfNecessary"', async () => {
      const batch = new Batch(topicName, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })

      runner.scheduleFetchManager = jest.fn()
      await runner.start()
      await runner.handleBatch(batch) // Manually fetch for test

      expect(eachBatch).toHaveBeenCalledWith(
        expect.objectContaining({
          commitOffsetsIfNecessary: expect.any(Function),
        })
      )

      const { commitOffsetsIfNecessary } = eachBatch.mock.calls[0][0] // Access the callback

      // Clear state
      consumerGroup.commitOffsetsIfNecessary.mockClear()
      consumerGroup.commitOffsets.mockClear()

      // No offsets provided
      await commitOffsetsIfNecessary()
      expect(consumerGroup.commitOffsetsIfNecessary).toHaveBeenCalledTimes(1)
      expect(consumerGroup.commitOffsets).toHaveBeenCalledTimes(0)

      // Clear state
      consumerGroup.commitOffsetsIfNecessary.mockClear()
      consumerGroup.commitOffsets.mockClear()

      // Provide offsets
      const offsets = {
        topics: [{ topic: topicName, partitions: [{ offset: '1', partition: 0 }] }],
      }

      await commitOffsetsIfNecessary(offsets)
      expect(consumerGroup.commitOffsetsIfNecessary).toHaveBeenCalledTimes(0)
      expect(consumerGroup.commitOffsets).toHaveBeenCalledTimes(1)
      expect(consumerGroup.commitOffsets).toHaveBeenCalledWith(offsets, {
        guard: expect.any(Function),
      })
    })

    it('pauses the partition of the batch through "pause", and resumes it through what it returns', async () => {
      const batch = new Batch(topicName, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })
      consumerGroup.pause = jest.fn()
      consumerGroup.resume = jest.fn()
      let resume
      eachBatch.mockImplementation(async ({ pause }) => {
        resume = pause()
      })

      runner.scheduleFetchManager = jest.fn()
      await runner.start()
      await runner.handleBatch(batch)

      const entry = [{ topic: topicName, partitions: [partition] }]
      expect(consumerGroup.pause).toHaveBeenCalledWith(entry)
      expect(consumerGroup.resume).not.toHaveBeenCalled()

      resume()
      expect(consumerGroup.resume).toHaveBeenCalledWith(entry)
    })
  })

  describe('when eachBatchAutoResolve is set to false', () => {
    beforeEach(() => {
      runner = createTestRunner({ eachBatchAutoResolve: false })
      runner.scheduleFetchManager = jest.fn()
    })

    it('does not call resolveOffset with the last offset', async () => {
      const batch = new Batch(topicName, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })

      await runner.start()
      await runner.handleBatch(batch)
      expect(onCrash).not.toHaveBeenCalled()
      expect(consumerGroup.resolveOffset).not.toHaveBeenCalled()
    })
  })

  describe('when autoCommit is set to false', () => {
    let eachBatchCallUncommittedOffsets

    beforeEach(() => {
      eachBatchCallUncommittedOffsets = jest.fn(async ({ uncommittedOffsets }) => {
        uncommittedOffsets()
      })

      runner = createTestRunner({ autoCommit: false, eachBatch: eachBatchCallUncommittedOffsets })
      runner.scheduleFetchManager = jest.fn(() => runner.consume())
    })

    it('should not commit offsets during fetch', async () => {
      const batch = new Batch(topicName, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })

      runner.scheduleFetchManager = jest.fn()
      await runner.start()
      await runner.handleBatch(batch) // Manually fetch for test

      expect(consumerGroup.commitOffsets).not.toHaveBeenCalled()
      expect(consumerGroup.commitOffsetsIfNecessary).not.toHaveBeenCalled()
      expect(eachBatchCallUncommittedOffsets).toHaveBeenCalled()
      expect(consumerGroup.uncommittedOffsets).toHaveBeenCalled()

      expect(onCrash).not.toHaveBeenCalled()
    })
  })

  it('calls onCrash for any other errors', async () => {
    const unknownError = new KafkaJSProtocolError(createErrorFromCode(UNKNOWN))
    consumerGroup.joinAndSync
      .mockImplementationOnce(() => {
        throw unknownError
      })
      .mockImplementationOnce(() => true)

    runner.scheduleFetchManager = jest.fn()
    await runner.start()

    await waitFor(() => onCrash.mock.calls.length > 0)

    expect(runner.scheduleFetchManager).not.toHaveBeenCalled()
    expect(onCrash).toHaveBeenCalledWith(unknownError)
  })

  it('crashes on KafkaJSNotImplemented errors', async () => {
    const notImplementedError = new KafkaJSNotImplemented('not implemented')
    consumerGroup.fetch.mockImplementationOnce(() => Promise.reject(notImplementedError))

    await runner.start()

    await waitFor(() => onCrash.mock.calls.length > 0)
    expect(onCrash).toHaveBeenCalledWith(notImplementedError)
  })

  it('crashes once the retries are spent when every Fetch round is refused with a retriable error', async () => {
    runner = createTestRunner({ retry: { retries: 2, initialRetryTime: 1, maxRetryTime: 5 } })
    const refusal = new KafkaJSError(
      'Fetch response carries session id 43, which cannot answer session 42 at epoch 1',
      { retriable: true }
    )
    consumerGroup.fetch.mockImplementation(() => Promise.reject(refusal))

    await runner.start()

    await waitFor(() => onCrash.mock.calls.length > 0)
    expect(onCrash.mock.calls[0][0]).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
    expect(onCrash.mock.calls[0][0]).toMatchObject({ message: refusal.message, cause: refusal })
  })

  it('retries a fetch loop failure in place after the client retry.maxTotalTime has passed', async () => {
    runner = createTestRunner({ retry: { maxTotalTime: 1, retries: 3, initialRetryTime: 5 } })
    runner.running = true

    let attempts = 0
    runner.fetchManager = {
      start: jest.fn(async () => {
        attempts += 1
        if (attempts === 1) {
          await sleep(10)
          throw new Error('fetch loop failed')
        }
        runner.running = false
      }),
      stop: jest.fn(async () => {}),
    }

    runner.scheduleFetchManager()
    await waitFor(() => !runner.consuming)

    expect(runner.fetchManager.start).toHaveBeenCalledTimes(2)
    expect(onCrash).not.toHaveBeenCalled()
  })

  describe('commitOffsets', () => {
    let offsets

    beforeEach(async () => {
      offsets = { topics: [{ topic: topicName, partitions: [{ offset: '1', partition }] }] }

      consumerGroup.joinAndSync.mockClear()
      consumerGroup.commitOffsetsIfNecessary.mockClear()
      consumerGroup.commitOffsets.mockClear()
    })

    it('does not retry a commit the group reports as not implemented', async () => {
      const error = new KafkaJSNotImplemented('not implemented')
      consumerGroup.commitOffsets.mockRejectedValue(error)
      await runner.start()
      await expect(runner.commitOffsets(offsets)).rejects.toBe(error)
      expect(consumerGroup.commitOffsets).toHaveBeenCalledTimes(1)
    })

    it('should commit offsets while running', async () => {
      await runner.start()
      await runner.commitOffsets(offsets)

      expect(consumerGroup.commitOffsetsIfNecessary).toHaveBeenCalledTimes(0)
      expect(consumerGroup.commitOffsets.mock.calls.length).toBeGreaterThanOrEqual(1)
      expect(consumerGroup.commitOffsets).toHaveBeenCalledWith(offsets, {
        guard: expect.any(Function),
      })
    })

    it('gives up with KafkaJSDeliveryTimeout once the client retry.maxTotalTime has passed', async () => {
      const error = new Error('commit failed')
      runner = createTestRunner({ retry: { maxTotalTime: 1, retries: 3, initialRetryTime: 5 } })
      runner.running = true
      consumerGroup.commitOffsets.mockImplementation(async () => {
        await sleep(10)
        throw error
      })

      const rejection = await runner.commitOffsets(offsets).catch((e) => e)

      expect(rejection).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(rejection.cause).toBe(error)
      expect(consumerGroup.commitOffsets).toHaveBeenCalledTimes(1)
    })

    it('should throw when group is rebalancing', async () => {
      const error = rebalancingError()
      consumerGroup.commitOffsets.mockImplementationOnce(() => {
        throw error
      })

      runner.scheduleFetchManager = jest.fn()
      await runner.start()

      consumerGroup.joinAndSync.mockClear()

      await expect(runner.commitOffsets(offsets)).rejects.toThrow(error.message)
    })

    it('correctly catch exceptions in parallel "eachBatch" processing', async () => {
      runner = createTestRunner({
        eachBatchAutoResolve: false,
        eachBatch: async () => {
          throw new Error('Error while processing batches in parallel')
        },
        concurrency: 10,
        retry: { retries: 0 },
      })

      const batch = new Batch(topicName, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })

      const longRunningRequest = () =>
        new Promise((resolve) => {
          setTimeout(() => resolve([]), 100)
        })

      consumerGroup.fetch
        .mockImplementationOnce(longRunningRequest)
        .mockImplementationOnce(async () => [batch])

      await runner.start()

      await waitFor(() => onCrash.mock.calls.length > 0)
      await expect(onCrash).toHaveBeenCalledWith(expect.any(KafkaJSNumberOfRetriesExceeded))
    })

    it('correctly catch exceptions in parallel "heartbeat" processing', async () => {
      const batch = new Batch(topicName, 0, {
        partition,
        highWatermark: 5,
        messages: [{ offset: 4, key: '1', value: '2' }],
      })

      const longRunningRequest = () =>
        new Promise((resolve) => {
          setTimeout(() => resolve([]), 100)
        })

      const error = new Error('Error while processing heartbeats in parallel')
      consumerGroup.heartbeat = async () => {
        throw error
      }

      consumerGroup.fetch
        .mockImplementation(longRunningRequest)
        .mockImplementationOnce(async () => [batch])

      await runner.start()

      await waitFor(() => onCrash.mock.calls.length > 0)
      expect(onCrash).toHaveBeenCalledWith(error)
    })

    it('should ignore request errors from fetch on stopped consumer', async () => {
      const rejectedRequest = () =>
        new Promise((resolve, reject) => {
          setTimeout(() => reject(new Error('Failed or manually rejected request')), 10)
        })

      consumerGroup.fetch
        .mockImplementationOnce(rejectedRequest)
        .mockImplementationOnce(async () => {
          await sleep(10)
          return []
        })

      runner.scheduleFetchManager = jest.fn()
      await runner.start()
      runner.running = false

      await runner.fetch()
    })
  })
  describe('batch debug logging', () => {
    const batchOf = (messages) => new Batch(topicName, 0, { partition, highWatermark: 5, messages })

    beforeEach(async () => {
      runner.scheduleFetchManager = jest.fn()
      await runner.start()
    })

    it('records how a batch with messages was handled', async () => {
      jest.spyOn(runner.logger, 'debugEnabled').mockReturnValue(true)
      const debug = jest.spyOn(runner.logger, 'debug')

      await runner.handleBatch(batchOf([{ offset: 4, key: '1', value: '2' }]))

      expect(debug).toHaveBeenCalledWith('Batch', {
        topic: topicName,
        partition,
        fetchedOffset: 0,
        rawMessages: 1,
        withinOffset: 1,
        messages: 1,
        firstOffset: 4,
        lastOffset: 4,
        branch: 'processed',
      })
    })

    it('distinguishes an empty batch from one emptied by filtering', async () => {
      jest.spyOn(runner.logger, 'debugEnabled').mockReturnValue(true)
      const debug = jest.spyOn(runner.logger, 'debug')

      await runner.handleBatch(batchOf([]))
      await runner.handleBatch(batchOf([{ offset: 4, isControlRecord: true }]))

      expect(debug).toHaveBeenCalledWith(
        'Batch',
        expect.objectContaining({ rawMessages: 0, messages: 0, branch: 'empty' })
      )
      expect(debug).toHaveBeenCalledWith(
        'Batch',
        expect.objectContaining({
          rawMessages: 1,
          withinOffset: 1,
          messages: 0,
          branch: 'empty-due-to-filtering',
        })
      )
    })

    it('does not build the payload at the default level', async () => {
      jest.spyOn(runner.logger, 'debugEnabled').mockReturnValue(false)
      const debug = jest.spyOn(runner.logger, 'debug')

      await runner.handleBatch(batchOf([{ offset: 4, key: '1', value: '2' }]))

      expect(debug).not.toHaveBeenCalledWith('Batch', expect.anything())
    })
  })

  describe('when the assignment is dropped while the handler runs', () => {
    const batchWithEpoch = (assignmentEpoch) => {
      const batch = new Batch(topicName, 0, {
        partition,
        highWatermark: 5,
        messages: [
          { offset: 4, key: '1', value: '2' },
          { offset: 5, key: '3', value: '4' },
        ],
      })
      batch.assignmentEpoch = assignmentEpoch
      return batch
    }

    it('does not commit for a batch whose epoch moved on while "eachBatch" ran', async () => {
      eachBatch.mockImplementation(async () => {
        consumerGroup.assignmentEpoch += 1
      })

      runner.scheduleFetchManager = jest.fn()
      await runner.start()
      await runner.handleBatch(batchWithEpoch(0))

      expect(eachBatch).toHaveBeenCalled()
      expect(consumerGroup.commitOffsets).not.toHaveBeenCalled()
      expect(consumerGroup.heartbeat).not.toHaveBeenCalled()
    })

    it('commits for a batch whose epoch is still current', async () => {
      runner.scheduleFetchManager = jest.fn()
      await runner.start()
      await runner.handleBatch(batchWithEpoch(0))

      expect(eachBatch).toHaveBeenCalled()
      expect(consumerGroup.commitOffsets).toHaveBeenCalled()
    })

    it('refuses the per-batch commit when the epoch moves while it is in flight', async () => {
      let refusal = null
      consumerGroup.commitOffsets.mockImplementation(async (offsets, { guard }) => {
        consumerGroup.assignmentEpoch += 1
        refusal = guard()
      })
      runner.scheduleFetchManager = jest.fn()
      await runner.start()
      await runner.handleBatch(batchWithEpoch(0))

      expect(consumerGroup.commitOffsets).toHaveBeenCalledTimes(1)
      expect(consumerGroup.commitOffsets.mock.calls[0][1].auto).toBe(true)
      expect(refusal).toBeInstanceOf(KafkaJSCommitFailedError)
      expect(refusal.message).toBe('The handler outlived its assignment')
    })

    it('fences the per-batch commit by the membership once its ownership is lost', async () => {
      runner.scheduleFetchManager = jest.fn()
      await runner.start()
      await runner.handleBatch(batchWithEpoch(0))
      const lost = new Error('the membership was lost')
      consumerGroup.ownership = { token: () => 7, refusal: (token) => (token === 7 ? lost : null) }

      try {
        expect(consumerGroup.commitOffsets.mock.calls[0][1].guard()).toBe(lost)
      } finally {
        delete consumerGroup.ownership
      }
    })

    it('stops "eachMessage" after the message it was inside, resolving only that offset', async () => {
      const eachMessage = jest.fn(async () => {
        consumerGroup.assignmentEpoch += 1
      })
      runner = createTestRunner({ eachBatch: undefined, eachMessage })

      runner.scheduleFetchManager = jest.fn()
      await runner.start()
      await runner.handleBatch(batchWithEpoch(0))

      expect(eachMessage).toHaveBeenCalledTimes(1)
      expect(consumerGroup.resolveOffset).toHaveBeenCalledTimes(1)
      expect(consumerGroup.commitOffsetsIfNecessary).not.toHaveBeenCalled()
      expect(consumerGroup.commitOffsets).not.toHaveBeenCalled()
      expect(consumerGroup.heartbeat).not.toHaveBeenCalled()
    })
  })

  describe('stop', () => {
    it('stops the fetch manager before waiting for a background rebalance round', async () => {
      const calls = []
      runner.fetchManager = {
        start: jest.fn(async () => {}),
        stop: jest.fn(async () => {
          calls.push('fetchManager.stop')
        }),
      }
      runner.rebalanceCoordinator = {
        clearVerdict: jest.fn(),
        notifyStopRequested: jest.fn(),
        idle: jest.fn(async () => {
          calls.push('rebalanceCoordinator.idle')
          // The round is only reachable if a fetch issued in this window returns instead of
          // spinning the loop it belongs to.
          await expect(runner.fetch(1)).resolves.toEqual([])
        }),
      }

      runner.running = true
      await runner.stop()

      expect(calls).toEqual(['fetchManager.stop', 'rebalanceCoordinator.idle'])
      expect(consumerGroup.leave).toHaveBeenCalled()

      runner = null
    })
  })

  describe('Fetch sessions', () => {
    it('closes them after the fetchers stopped, within the stop scope, before the leave', async () => {
      const calls = []
      const runner = createTestRunner()
      runner.fetchManager = {
        start: jest.fn(async () => {}),
        stop: jest.fn(async () => calls.push('fetchManager.stop')),
      }
      consumerGroup.closeFetchSessions = jest.fn(async () => calls.push('closeFetchSessions'))
      consumerGroup.leave = jest.fn(async () => calls.push('leave'))
      const operation = new ConsumerOperation({ deadlineAt: performance.now() + 60000 })
      runner.running = true

      await runner.stop(undefined, operation)
      operation.finish()

      expect(calls).toEqual(['fetchManager.stop', 'closeFetchSessions', 'leave'])
      const [scope] = consumerGroup.closeFetchSessions.mock.calls[0]
      expect(scope).toEqual({ deadlineAt: operation.deadlineAt, guard: runner.stopScope.guard })
    })

    it('passes no finite deadline when the stop has none, and still closes', async () => {
      const runner = createTestRunner()
      runner.running = true

      await runner.stop()

      const [scope] = consumerGroup.closeFetchSessions.mock.calls[0]
      expect(scope.deadlineAt).toBe(Infinity)
    })

    it('starts no close while a fetcher still holds the stop, and still ends the stop', async () => {
      const runner = createTestRunner()
      runner.fetchManager = { start: jest.fn(), stop: jest.fn(() => new Promise(() => {})) }
      const operation = new ConsumerOperation({ deadlineAt: performance.now() + 20 })
      runner.running = true

      await runner.stop(undefined, operation)
      operation.finish()

      expect(consumerGroup.closeFetchSessions).not.toHaveBeenCalled()
      expect(consumerGroup.leave).not.toHaveBeenCalled()
    })

    it('retires them locally and sends nothing when a forced close retires the run', () => {
      const runner = createTestRunner()
      runner.running = true

      runner.retire(new Error('forced'))
      runner.retire(new Error('again'))

      expect(consumerGroup.retireFetchSessions).toHaveBeenCalledTimes(1)
      expect(consumerGroup.retireCoordinatorLookup.mock.calls).toEqual([[new Error('forced')]])
      expect(consumerGroup.closeFetchSessions).not.toHaveBeenCalled()
    })
  })

  describe('Consumer > Runner > the consumer group protocol', () => {
    const asConsumerProtocol = () => {
      consumerGroup.kind = 'consumer'
      consumerGroup.rebalanceProtocol = jest.fn(() => 'cooperative')
      consumerGroup.heartbeatInterval = jest.fn(() => 500)
    }

    it('tells the membership to stop before it cancels the heartbeat chain', async () => {
      asConsumerProtocol()
      const order = []
      consumerGroup.onStopRequested = jest.fn(() => order.push('onStopRequested'))
      const runner = createTestRunner()
      runner.running = true
      jest
        .spyOn(runner.heartbeatManager, 'stop')
        .mockImplementation(() => order.push('managerStop'))

      await runner.stop()

      expect(order).toEqual(['onStopRequested', 'managerStop'])
    })

    // The reconciliation chain `stop()` awaits can still reach `requestTick()` through an
    // acknowledgement or a coordinator move. Before the shutdown latch that armed a fresh chain,
    // so heartbeats kept going out after LeaveGroup and the timer held the process open.
    it('leaves no armed heartbeat chain, whatever asks for one after stop()', async () => {
      asConsumerProtocol()
      const runner = createTestRunner()
      runner.running = true

      await runner.stop()

      expect(runner.heartbeatManager.isRunning()).toBe(false)

      runner.heartbeatManager.requestTick()
      runner.heartbeatManager.ensureStarted()
      runner.heartbeatManager.requestTick()

      expect(runner.heartbeatManager.isRunning()).toBe(false)
      expect(runner.heartbeatManager.handle).toBeNull()
    })

    it('leaves the group on stop for a static consumer-protocol member', async () => {
      asConsumerProtocol()
      consumerGroup.groupInstanceId = 'instance-1'
      const runner = createTestRunner()
      runner.running = true

      await runner.stop()

      expect(consumerGroup.leave).toHaveBeenCalledTimes(1)
      expect(consumerGroup.memberId).toEqual('test-member')
    })

    it('keeps the classic static rule: no leave, member id cleared', async () => {
      consumerGroup.groupInstanceId = 'instance-1'
      const runner = createTestRunner()
      runner.running = true

      await runner.stop()

      expect(consumerGroup.leave).not.toHaveBeenCalled()
      expect(consumerGroup.memberId).toBeNull()
    })

    it.each(['classic', 'consumer'])('handles heartbeat authorization under %s', async (kind) => {
      consumerGroup.kind = kind
      runner.running = true
      const error = new KafkaJSProtocolError(createErrorFromCode(29))
      consumerGroup.heartbeat.mockRejectedValue(error)
      if (kind === 'consumer') await expect(runner.heartbeat()).resolves.toBeUndefined()
      else await expect(runner.heartbeat()).rejects.toBe(error)
      expect(consumerGroup.onMembershipError).not.toHaveBeenCalled()
      expect(onCrash).not.toHaveBeenCalled()
    })

    it('heartbeats at the membership interval', async () => {
      asConsumerProtocol()
      const runner = createTestRunner()
      runner.running = true

      await runner.heartbeat()

      expect(consumerGroup.heartbeat).toHaveBeenCalledWith({
        interval: 500,
        guard: expect.any(Function),
        onAccepted: expect.any(Function),
      })
    })

    it('routes every LOST verdict into the membership state machine, before the round', async () => {
      asConsumerProtocol()
      const order = []
      consumerGroup.onMembershipError = jest.fn(() => order.push('onMembershipError'))
      const runner = createTestRunner()
      runner.running = true
      jest
        .spyOn(runner.rebalanceCoordinator, 'runRound')
        .mockImplementation(async () => order.push('runRound'))

      const fenced = new KafkaJSProtocolError(createErrorFromCode(110))
      await runner.handleRebalanceError({ kind: 'lost', reason: 'fenced', error: fenced })

      expect(consumerGroup.onMembershipError).toHaveBeenCalledWith(fenced)
      expect(order).toEqual(['onMembershipError', 'runRound'])
    })

    it('does not route a REVOKED verdict into the state machine', async () => {
      asConsumerProtocol()
      const runner = createTestRunner()
      runner.running = true
      jest.spyOn(runner.rebalanceCoordinator, 'runRound').mockResolvedValue()

      await runner.handleRebalanceError({
        kind: 'revoked',
        reason: 'assignment-changed',
        error: new Error('assignment changed'),
      })

      expect(consumerGroup.onMembershipError).not.toHaveBeenCalled()
    })

    // The member epoch must survive until rebalance() step 0 consumes the verdict: zeroing it here
    // would make the fenced member skip step 0 and stall silently.
    it('does not reset the generation locally on a lost verdict', async () => {
      asConsumerProtocol()
      const runner = createTestRunner()
      runner.running = true
      jest.spyOn(runner.rebalanceCoordinator, 'runRound').mockResolvedValue()

      await runner.handleRebalanceError({
        kind: 'lost',
        reason: 'fenced',
        error: new KafkaJSProtocolError(createErrorFromCode(110)),
      })

      expect(consumerGroup.resetGenerationLocally).not.toHaveBeenCalled()
      expect(consumerGroup.discardUncommittedOffsets).toHaveBeenCalledTimes(1)
    })

    // The cooperative branch is reached because rebalanceProtocol() answers COOPERATIVE, which is
    // what keeps every KIP-848 round incremental rather than stop-the-world.
    it('never quiesces the world for a consumer-protocol revocation', async () => {
      asConsumerProtocol()
      const runner = createTestRunner()
      runner.running = true
      const fetchManagerStop = jest.spyOn(runner.fetchManager, 'stop')
      jest.spyOn(runner.rebalanceCoordinator, 'runRound').mockResolvedValue()

      await runner.handleRebalanceError({
        kind: 'revoked',
        reason: 'assignment-changed',
        error: new Error('assignment changed'),
      })

      expect(fetchManagerStop).not.toHaveBeenCalled()
      expect(runner.quiescing).toBe(false)
    })

    it('abandons and rethrows a fatal membership error without a round', async () => {
      asConsumerProtocol()
      const runner = createTestRunner()
      runner.running = true
      const abandon = jest.spyOn(runner.rebalanceCoordinator, 'abandon')
      const runRound = jest.spyOn(runner.rebalanceCoordinator, 'runRound')

      const fatal = new KafkaJSProtocolError(createErrorFromCode(112))
      await expect(
        runner.handleRebalanceError({ kind: 'lost', reason: 'fenced', error: fatal })
      ).rejects.toMatchObject({ code: 112 })

      expect(abandon).toHaveBeenCalledWith('lost')
      expect(runRound).not.toHaveBeenCalled()
    })

    // The fatal-membership rule is CONSUMER-ONLY: a classic consumer's handling of the same codes
    // is unchanged, so it rejoins rather than dying.
    it('leaves a classic member on the rejoin path for the same code', async () => {
      const runner = createTestRunner()
      runner.running = true
      const abandon = jest.spyOn(runner.rebalanceCoordinator, 'abandon')
      const runRound = jest.spyOn(runner.rebalanceCoordinator, 'runRound').mockResolvedValue()

      await runner.handleRebalanceError({
        kind: 'lost',
        reason: 'lost',
        error: new KafkaJSProtocolError(createErrorFromCode(81)),
      })

      expect(abandon).not.toHaveBeenCalled()
      expect(runRound).toHaveBeenCalledWith('lost', 'lost')
    })

    it('keeps the member id across a consumer-protocol stop, and clears it for a classic one', async () => {
      asConsumerProtocol()
      const consumerRunner = createTestRunner()
      consumerRunner.running = true

      await consumerRunner.stop()
      expect(consumerGroup.memberId).toEqual('test-member')

      consumerGroup.kind = 'classic'
      consumerGroup.memberId = 'test-member'
      consumerGroup.groupInstanceId = 'instance-1'
      const classicRunner = createTestRunner()
      classicRunner.running = true

      await classicRunner.stop()
      expect(consumerGroup.memberId).toBeNull()
    })

    it('fetches with the node id alone, exactly as it always has', async () => {
      const runner = createTestRunner()
      runner.running = true

      await runner.fetch(1)

      expect(consumerGroup.fetch).toHaveBeenCalledWith(1)
    })

    // At OffsetCommit v9 the broker answers a deleted group with GROUP_ID_NOT_FOUND (69)
    // where v8 answered ILLEGAL_GENERATION (22). A classic consumer must rejoin either way.
    it('rejoins on a commit answered 69, exactly as it does on a 22', async () => {
      const runner = createTestRunner()
      runner.running = true
      const runRound = jest.spyOn(runner.rebalanceCoordinator, 'runRound').mockResolvedValue()

      const verdictFor = (code) =>
        runner.toVerdict(stampOffsetCommit(new KafkaJSProtocolError(createErrorFromCode(code))))

      expect(verdictFor(69)).toMatchObject({ kind: 'lost', reason: 'lost' })
      expect(verdictFor(69).kind).toEqual(verdictFor(22).kind)
      expect(verdictFor(69).reason).toEqual(verdictFor(22).reason)

      await runner.handleRebalanceError(verdictFor(69))

      expect(runRound).toHaveBeenCalledWith('lost', 'lost')
      expect(consumerGroup.discardUncommittedOffsets).toHaveBeenCalled()
      // The member id is still this member's; only the group is gone.
      expect(consumerGroup.memberId).toEqual('test-member')
    })

    it('still crashes on an unstamped 69, which no commit produced', async () => {
      const runner = createTestRunner()
      runner.running = true
      const error = new KafkaJSProtocolError(createErrorFromCode(69))

      expect(runner.toVerdict(error)).toMatchObject({ kind: null, reason: null })
      await expect(runner.handleRebalanceError(error)).rejects.toMatchObject({ code: 69 })
    })

    // The two places the runner clears the member id are here and in `stop()`; a classic member
    // needs a fresh one only when the coordinator did not recognise the old one.
    it('asks for a fresh member id only for an unrecognized one', async () => {
      const runner = createTestRunner()
      runner.running = true
      jest.spyOn(runner.rebalanceCoordinator, 'runRound').mockResolvedValue()

      await runner.handleRebalanceError({
        kind: 'lost',
        reason: 'lost',
        error: new KafkaJSProtocolError(createErrorFromCode(22)),
      })

      expect(consumerGroup.memberId).toEqual('test-member')

      await runner.handleRebalanceError({
        kind: 'lost',
        reason: 'lost',
        error: new KafkaJSProtocolError(createErrorFromCode(25)),
      })

      expect(consumerGroup.memberId).toBeNull()
    })

    it('forgets the member id on behalf of the membership the answer was sent under', async () => {
      expect(typeof stampMembership).toBe('function')
      const runner = createTestRunner()
      runner.running = true
      jest.spyOn(runner.rebalanceCoordinator, 'runRound').mockResolvedValue()
      const forget = jest.spyOn(consumerGroup, 'forgetMemberId')
      const answered = stampMembership(new KafkaJSProtocolError(createErrorFromCode(25)), 7)

      await runner.handleRebalanceError({ kind: 'lost', reason: 'lost', error: answered })

      expect(forget).toHaveBeenCalledWith(7)
    })

    it('classifies a bare fetch-path error with the membership kind', () => {
      asConsumerProtocol()
      const runner = createTestRunner()
      const fenced = new KafkaJSProtocolError(createErrorFromCode(110))

      expect(runner.toVerdict(fenced)).toMatchObject({ kind: 'lost', reason: 'fenced' })
    })
  })

  describe('Consumer > Runner > the prefetch knob', () => {
    it('stores maxPrefetchBatches without wiring it to anything', () => {
      const runner = createTestRunner({ maxPrefetchBatches: 1 })

      expect(runner.maxPrefetchBatches).toEqual(1)
      expect(runner.isFullyResolved).toBeUndefined()
      expect(runner.isPrefetchStale).toBeUndefined()
    })

    // The lambda the fetch manager is handed, not `Runner#fetch`: the manager destructures a fixed
    // option set, so anything the runner adds here would be dropped without a signal.
    it('hands the fetch manager only the options it destructures', () => {
      let mockManagerOptions

      jest.isolateModules(() => {
        jest.doMock('../fetchManager', () => (options) => {
          mockManagerOptions = options
          return { start: jest.fn(async () => {}), stop: jest.fn(async () => {}) }
        })

        const IsolatedRunner = require('../runner')
        const isolated = new IsolatedRunner({
          consumerGroup,
          onCrash,
          instrumentationEmitter,
          logger: newLogger(),
          eachBatch,
          concurrency: 1,
          heartbeatInterval: 3000,
          maxPollInterval: 300000,
          maxPrefetchBatches: 1,
        })
        isolated.running = true
      })

      expect(Object.keys(mockManagerOptions).sort()).toEqual([
        'concurrency',
        'fetch',
        'getNodeIds',
        'handler',
        'logger',
        'onRetire',
        'retryDelay',
      ])
    })
  })

  describe('close, quiescence and crash edges', () => {
    const roundOf = (guard = () => null, wait = (work) => work()) => ({
      guard,
      wait,
      retire: jest.fn(),
      roundDeadlineAt: performance.now() + 1000,
    })

    it('counts a handler batch down by its ownership key and wakes every waiter', () => {
      const check = jest.fn()
      runner.hookHandlers = new Map([
        ['t|0', 2],
        ['owned', 1],
      ])
      runner.hookWaiters = new Set([check])
      runner.endHookBatch({ topic: 't', partition: 0 })
      expect(runner.hookHandlers.get('t|0')).toBe(1)
      runner.endHookBatch({ topic: 't', partition: 0 })
      runner.endHookBatch({ ownershipKey: 'owned' })
      expect([...runner.hookHandlers.keys()]).toEqual([])
      expect(check).toHaveBeenCalledTimes(3)
      runner.hookHandlers = undefined
      runner.hookWaiters = undefined
      expect(() => runner.endHookBatch({ topic: 't', partition: 0 })).not.toThrow()
    })

    it('wakes the quiescence waiters when the run is retired', () => {
      const check = jest.fn()
      runner.hookWaiters = new Set([check])
      runner.retire(new Error('forced close'))
      expect(check).toHaveBeenCalledTimes(1)
      expect(consumerGroup.retireFetchSessions).toHaveBeenCalledTimes(1)
    })

    it('stops the fetch manager so no fetcher spins after a forced close', () => {
      const runner = createTestRunner()
      runner.fetchManager = { start: jest.fn(), stop: jest.fn(() => Promise.resolve()) }
      runner.running = true

      runner.retire(new Error('forced close'))
      runner.retire(new Error('again'))

      expect(runner.fetchManager.stop).toHaveBeenCalledTimes(1)
    })

    it('returns the retirement reason as the commit refusal of a retired run', () => {
      const reason = new Error('forced close')
      runner.retirement = reason
      expect(runner.commitRefusal()).toBe(reason)
    })

    it('does not start a run that is already running', async () => {
      runner.running = true
      await runner.start()
      expect(consumerGroup.startIdleWait).not.toHaveBeenCalled()
    })

    it('logs a crash handler that fails and still shuts the heartbeat down', async () => {
      consumerGroup.connect.mockRejectedValue(new Error('connect failed'))
      onCrash.mockRejectedValue(new Error('crash handler failed'))
      const error = jest.spyOn(runner.logger, 'error')
      await expect(runner.start()).resolves.toBeUndefined()
      expect(error).toHaveBeenCalledWith(
        'Crash handler failed',
        expect.objectContaining({ error: 'crash handler failed', cause: 'connect failed' })
      )
    })

    it('crashes a manual run whose local apply fails for another reason', async () => {
      consumerGroup.kind = 'manual'
      consumerGroup.applyManualAssignment = jest.fn().mockRejectedValue(new Error('apply failed'))
      await runner.start()
      expect(onCrash).toHaveBeenCalledWith(expect.objectContaining({ message: 'apply failed' }))
    })

    it('removes the stop waiter of a consumer that finishes, and of a close that is retired', async () => {
      runner.consuming = true
      const done = runner.waitForConsumer()
      runner.emit('consuming-stop')
      await done

      let retire
      const operation = { register: jest.fn((callback) => (retire = callback)) }
      runner.waitForConsumer(operation)
      const before = runner.listenerCount('consuming-stop')
      retire()
      expect(runner.listenerCount('consuming-stop')).toBe(before - 1)
      expect(operation.register).toHaveBeenCalledTimes(1)
      runner.consuming = false
    })

    it('refuses a quiescence wait whose round was already retired', async () => {
      const refusal = new Error('round retired')
      const busy = jest.fn(() => true)
      await expect(
        runner.awaitQuiescence(busy, {
          deadlineAt: performance.now() + 1000,
          identity: { round: roundOf(() => refusal) },
        })
      ).rejects.toBe(refusal)
      expect(busy).not.toHaveBeenCalled()
    })

    it('rejects a quiescence wait when the round is retired while it waits', async () => {
      const refusal = new Error('round retired')
      let calls = 0
      const round = roundOf(() => (++calls > 1 ? refusal : null))
      await expect(
        runner.awaitQuiescence(() => true, {
          deadlineAt: performance.now() + 1000,
          identity: { round },
        })
      ).rejects.toBe(refusal)
      expect(runner.hookWaiters.size).toBe(0)
    })

    it('refuses a quiescence that completes in a round retired meanwhile', async () => {
      const refusal = new Error('round retired')
      let retired = false
      let busy = true
      const round = roundOf(
        () => (retired ? refusal : null),
        async (work) => {
          const pending = work()
          busy = false
          for (const check of runner.hookWaiters) check()
          await pending
          retired = true
        }
      )
      await expect(
        runner.awaitQuiescence(() => busy, {
          deadlineAt: performance.now() + 1000,
          identity: { round },
        })
      ).rejects.toBe(refusal)
    })

    it('rethrows the wait failure of a round that holds no refusal', async () => {
      const failure = new Error('wait failed')
      const round = roundOf(
        () => null,
        async () => {
          throw failure
        }
      )
      await expect(
        runner.awaitQuiescence(() => true, {
          deadlineAt: performance.now() + 1000,
          identity: { round },
        })
      ).rejects.toBe(failure)
    })

    it('expires a quiescence once even when the round reports its own join deadline later', async () => {
      jest.useFakeTimers()
      let now = 0
      jest.spyOn(performance, 'now').mockImplementation(() => now)
      try {
        const lose = jest.fn()
        consumerGroup.ownership = { lose }
        const deadlineError = Object.assign(new Error('join deadline'), {
          type: 'JOIN_DEADLINE_EXCEEDED',
        })
        let release
        const released = new Promise((resolve) => (release = resolve))
        let roundFailed = false
        const round = roundOf(
          () => null,
          async (work) => {
            work().catch(() => {})
            await released
            roundFailed = true
            throw deadlineError
          }
        )
        const outcome = runner
          .awaitQuiescence(() => true, { deadlineAt: 20, identity: { round, membership: 3 } })
          .catch((error) => error)
        now = 20
        await jest.advanceTimersByTimeAsync(20)
        expect(lose).toHaveBeenCalledTimes(1)
        expect(roundFailed).toBe(false)
        release()
        expect(await outcome).toMatchObject({ reason: 'timeout' })
        expect(roundFailed).toBe(true)
        expect(lose).toHaveBeenCalledTimes(1)
        expect(round.retire).toHaveBeenCalledTimes(1)
      } finally {
        jest.restoreAllMocks()
        jest.useRealTimers()
      }
    })

    it('settles on an early timer callback once the handlers are no longer busy', async () => {
      jest.useFakeTimers()
      jest.spyOn(performance, 'now').mockImplementation(() => 0)
      try {
        let busy = true
        const baseline = jest.getTimerCount()
        let outcome = 'pending'
        const done = runner
          .awaitQuiescence(() => busy, {
            deadlineAt: 20,
            identity: { round: roundOf() },
          })
          .then(
            () => (outcome = 'resolved'),
            (error) => (outcome = error)
          )
        busy = false
        await jest.advanceTimersByTimeAsync(20)
        expect(outcome).toBe('resolved')
        await done
        expect(runner.hookWaiters.size).toBe(0)
        expect(jest.getTimerCount()).toBe(baseline)
      } finally {
        jest.restoreAllMocks()
        jest.useRealTimers()
      }
    })

    it('quiesces the run once the fetch manager has stopped', async () => {
      runner.fetchManager = { stop: jest.fn(async () => {}) }
      runner.inFlightHandlers = 0
      await runner.quiesceRun({
        deadlineAt: performance.now() + 1000,
        identity: { round: roundOf(), membership: 1 },
      })
      expect(runner.fetchManager.stop).toHaveBeenCalledTimes(1)
    })

    it('rejoins after an eager preparation whose final commit failed', async () => {
      jest.spyOn(runner, 'quiesceRun').mockResolvedValue()
      runner.autoCommitOffsets = jest.fn().mockRejectedValue(new Error('commit failed'))
      const warn = jest.spyOn(runner.logger, 'warn')
      expect(await runner.prepareEagerRound(roundOf())).toBe(false)
      expect(warn).toHaveBeenCalledWith('Pre-rebalance offset commit failed; rejoining anyway', {
        error: 'commit failed',
      })
      expect(runner.admissionQuiescing).toBe(true)
    })

    it('stops an eager preparation whose round was retired during the commit', async () => {
      jest.spyOn(runner, 'quiesceRun').mockResolvedValue()
      runner.autoCommitOffsets = jest.fn().mockRejectedValue(new Error('commit failed'))
      const refusal = new Error('round retired')
      await expect(runner.prepareEagerRound(roundOf(() => refusal))).rejects.toBe(refusal)
    })

    it('runs the close callback without an operation and retires its round afterwards', async () => {
      let round
      consumerGroup.ownership = undefined
      consumerGroup.rebalanceTimeout = 1000
      consumerGroup.rebalanceHooks = { retire: jest.fn() }
      consumerGroup.hookIdentity = jest.fn(() => ({}))
      consumerGroup.takeListenerFailure = jest.fn(() => null)
      consumerGroup.invokeHook = jest.fn(async (hookRound) => {
        round = hookRound
        await hookRound.wait(async () => {})
      })
      await runner.invokeCloseHook({ previous: [{ topic: 't', partitions: [0] }] })
      expect(consumerGroup.invokeHook).toHaveBeenCalledWith(
        expect.any(Object),
        'onPartitionsRevoked',
        [{ topic: 't', partitions: [0] }],
        {}
      )
      expect(round.guard()).toMatchObject({ type: 'CONSUMER_STOPPING' })
      expect(consumerGroup.rebalanceHooks.retire).toHaveBeenCalledTimes(1)
    })
  })

  describe('a queued batch of a paused partition', () => {
    let paused, handled, gate

    const batchOf = (partitionId, offsets, fetchedOffset = offsets[0]) =>
      new Batch(topicName, fetchedOffset, {
        partition: partitionId,
        highWatermark: 100,
        messages: offsets.map((offset) => ({ offset: String(offset), key: 'k', value: 'v' })),
      })

    const pauseOf = (...partitions) => consumerGroup.pause([{ topic: topicName, partitions }])

    /** A handler that records what it is given and holds partitions 0 and 2 until `gate` opens. */
    const recordingHandler = () =>
      jest.fn(async ({ partition: partitionId, message, batch }) => {
        const id = partitionId ?? batch.partition
        handled.push({ partition: id, offset: message?.offset })
        if (id !== 1) await gate.promise
      })

    const start = async (partial) => {
      runner = createTestRunner(partial)
      runner.scheduleFetchManager = jest.fn()
      await runner.start()
    }

    /** The queue and workers the fetch manager gives a runner, over its real `handleBatch`. */
    const queueWith = (workerCount) =>
      createWorkerQueue({
        workers: Array.from({ length: workerCount }, (_, workerId) =>
          createWorker({ handler: (batch) => runner.handleBatch(batch), workerId })
        ),
      })

    const until = async (condition) => {
      for (let turns = 0; turns < 500 && !condition(); turns++) {
        await new Promise(setImmediate)
      }
      if (!condition()) throw new Error('the schedule never reached its hold point')
    }

    const resolvedOf = (partitionId) =>
      consumerGroup.resolveOffset.mock.calls
        .map(([resolved]) => resolved)
        .filter((resolved) => resolved.partition === partitionId)
        .map(({ offset }) => offset)

    beforeEach(() => {
      paused = new Set()
      handled = []
      let open
      gate = { promise: new Promise((resolve) => (open = resolve)), open: () => open() }
      consumerGroup.pause = jest.fn((topicPartitions) => {
        for (const { topic, partitions } of topicPartitions) {
          for (const p of partitions) paused.add(`${topic}|${p}`)
        }
      })
      consumerGroup.resume = jest.fn((topicPartitions) => {
        for (const { topic, partitions } of topicPartitions) {
          for (const p of partitions) paused.delete(`${topic}|${p}`)
        }
      })
      consumerGroup.isPaused = jest.fn((topic, p) => paused.has(`${topic}|${p}`))
      consumerGroup.heartbeatIdle = jest.fn(() => true)
    })

    it.each([
      ['eachMessage', (handler) => ({ eachBatch: undefined, eachMessage: handler })],
      ['eachBatch', (handler) => ({ eachBatch: handler })],
    ])(
      "pause p1 while p0's handler runs: p1's queued batch reaches no handler and resolves nothing with %s",
      async (_name, handlers) => {
        await start(handlers(recordingHandler()))
        const done = queueWith(1).push(batchOf(0, [4, 5]), batchOf(1, [4, 5]))
        await until(() => handled.length === 1)

        pauseOf(1)
        gate.open()
        await done

        expect(handled.filter(({ partition: id }) => id === 1)).toEqual([])
        expect(resolvedOf(1)).toEqual([])
      }
    )

    it('after resume, a replacement batch handed to the worker for p1 is delivered once, in order, and the queued batch delivered nothing', async () => {
      await start({ eachBatch: undefined, eachMessage: recordingHandler() })
      const queue = queueWith(1)
      const first = queue.push(batchOf(0, [4, 5]), batchOf(1, [4, 5]))
      await until(() => handled.length === 1)
      pauseOf(1)
      gate.open()
      await first
      expect(handled.filter(({ partition: id }) => id === 1)).toEqual([])
      expect(resolvedOf(1)).toEqual([])

      consumerGroup.resume([{ topic: topicName, partitions: [1] }])
      await queue.push(batchOf(1, [4, 5]))

      const delivered = handled.filter(({ partition: id }) => id === 1).map(({ offset }) => offset)
      expect(delivered).toEqual(['4', '5'])
      expect(resolvedOf(1)).toEqual(['4', '5'])
    })

    it('concurrency 2 with two other partitions blocked keeps p1 queued, and a pause keeps it from a handler', async () => {
      await start({ eachBatch: undefined, eachMessage: recordingHandler() })
      const done = queueWith(2).push(batchOf(0, [4]), batchOf(2, [4]), batchOf(1, [4, 5]))
      await until(() => handled.length === 2)

      pauseOf(1)
      gate.open()
      await done

      expect(handled.map(({ partition: id }) => id).sort()).toEqual([0, 2])
      expect(resolvedOf(1)).toEqual([])
    })

    describe('a start_batch_process listener that pauses the partition', () => {
      /** Pauses the partition of the first batch that starts, and records the events in order. */
      const pauseOnStart = () => {
        const events = []
        instrumentationEmitter.addListener(START_BATCH_PROCESS, ({ payload }) => {
          events.push('start')
          if (events.length === 1) pauseOf(payload.partition)
        })
        instrumentationEmitter.addListener(END_BATCH_PROCESS, () => events.push('end'))
        return events
      }

      it('keeps the batch from the handler, and end_batch_process still fires', async () => {
        const eachMessage = jest.fn()
        await start({ eachBatch: undefined, eachMessage })
        const events = pauseOnStart()

        await runner.handleBatch(batchOf(1, [4, 5]))

        expect(eachMessage).not.toHaveBeenCalled()
        expect(resolvedOf(1)).toEqual([])
        expect(events).toEqual(['start', 'end'])
      })

      it('resolves nothing for a control-only batch while paused, and moves past the record after resume', async () => {
        await start()
        const control = () =>
          new Batch(topicName, '5', {
            partition: 1,
            highWatermark: 100,
            messages: [{ offset: '5', isControlRecord: true }],
          })

        const events = pauseOnStart()
        await runner.handleBatch(control())
        expect(resolvedOf(1)).toEqual([])
        expect(events).toEqual(['start', 'end'])

        consumerGroup.resume([{ topic: topicName, partitions: [1] }])
        await runner.handleBatch(control())
        expect(resolvedOf(1)).toEqual(['5'])
        expect(events).toEqual(['start', 'end', 'start', 'end'])
      })

      it('resolves nothing for a batch whose records are all below the fetch offset', async () => {
        await start()
        const events = pauseOnStart()
        const compacted = () =>
          new Batch(topicName, '10', {
            partition: 1,
            highWatermark: 100,
            messages: [{ offset: '3', key: 'k', value: 'v' }],
          })

        await runner.handleBatch(compacted())
        expect(resolvedOf(1)).toEqual([])
        expect(events).toEqual(['start', 'end'])

        consumerGroup.resume([{ topic: topicName, partitions: [1] }])
        await runner.handleBatch(compacted())
        expect(resolvedOf(1)).toEqual(['10'])
        expect(events).toEqual(['start', 'end', 'start', 'end'])
      })
    })

    it('does not stop a handler that already runs: eachMessage stops after the current message', async () => {
      await start({ eachBatch: undefined, eachMessage: recordingHandler() })
      const done = queueWith(1).push(batchOf(0, [4, 5]))
      await until(() => handled.length === 1)

      pauseOf(0)
      gate.open()
      await done

      expect(handled.map(({ offset }) => offset)).toEqual(['4'])
      expect(resolvedOf(0)).toEqual(['4'])
    })

    it('admits and delivers the batch as today for a group double without isPaused', async () => {
      delete consumerGroup.isPaused
      const handler = jest.fn()
      await start({ eachBatch: handler })

      await runner.handleBatch(batchOf(1, [4, 5]))

      expect(handler).toHaveBeenCalledTimes(1)
      expect(resolvedOf(1)).toEqual(['5'])
    })
  })

  describe('a batch fetched before a leader epoch change', () => {
    const FETCH_EPOCH = 5
    let epochs, handled, gate, events

    /** A batch of `offsets` fetched at `FETCH_EPOCH`, as `ConsumerGroup#fetch` builds one. */
    const batchOf = (partitionId, offsets) =>
      new Batch(
        topicName,
        String(offsets[0]),
        {
          partition: partitionId,
          highWatermark: 100,
          messages: offsets.map((offset) => ({ offset: String(offset), key: 'k', value: 'v' })),
        },
        FETCH_EPOCH
      )

    /** What the metadata cache does on a new epoch: the epoch changes and routing moves on. */
    const moveEpoch = (partitionId, epoch) => {
      epochs[partitionId] = epoch
      consumerGroup.routingVersion += 1
    }

    /** A handler that records what it is given and holds partition 0 until `gate` opens. */
    const recordingHandler = () =>
      jest.fn(async ({ partition: partitionId, message, batch }) => {
        const id = partitionId ?? batch.partition
        handled.push({ partition: id, offset: message?.offset })
        if (id === 0) await gate.promise
      })

    const start = async (partial) => {
      runner = createTestRunner(partial)
      runner.scheduleFetchManager = jest.fn()
      await runner.start()
    }

    const queueWith = (workerCount) =>
      createWorkerQueue({
        workers: Array.from({ length: workerCount }, (_, workerId) =>
          createWorker({ handler: (batch) => runner.handleBatch(batch), workerId })
        ),
      })

    const until = async (condition) => {
      for (let turns = 0; turns < 500 && !condition(); turns++) {
        await new Promise(setImmediate)
      }
      if (!condition()) throw new Error('the schedule never reached its hold point')
    }

    const resolvedOf = (partitionId) =>
      consumerGroup.resolveOffset.mock.calls
        .map(([resolved]) => resolved)
        .filter((resolved) => resolved.partition === partitionId)
        .map(({ offset }) => offset)

    /** The batch-process events of one partition, in order. */
    const eventsOf = (partitionId) =>
      events.filter(([, id]) => id === partitionId).map(([name]) => name)

    beforeEach(() => {
      epochs = { 0: FETCH_EPOCH, 1: FETCH_EPOCH }
      handled = []
      events = []
      let open
      gate = { promise: new Promise((resolve) => (open = resolve)), open: () => open() }
      // The group's own predicate over a real validator and a cache stand-in, both at epoch 5.
      const positionValidator = new PositionValidator({ logger: newLogger() })
      for (const id of [0, 1]) positionValidator.markValidated(topicName, id, FETCH_EPOCH)
      Object.assign(consumerGroup, {
        cluster: { leaderEpoch: (_topic, id) => epochs[id] },
        positionValidator,
        routingVersion: 0,
        admissionEntry: ConsumerGroup.prototype.admissionEntry,
        validationAllows: ConsumerGroup.prototype.validationAllows,
        validationVersion: ConsumerGroup.prototype.validationVersion,
        heartbeatIdle: jest.fn(() => true),
      })
      instrumentationEmitter.addListener(START_BATCH_PROCESS, ({ payload }) =>
        events.push(['start', payload.partition])
      )
      instrumentationEmitter.addListener(END_BATCH_PROCESS, ({ payload }) =>
        events.push(['end', payload.partition])
      )
    })

    it.each([
      ['eachMessage', (handler) => ({ eachBatch: undefined, eachMessage: handler })],
      ['eachBatch', (handler) => ({ eachBatch: handler })],
    ])(
      'a queued batch fetched at epoch 5 is not handed over once the cached epoch is 6, and end_batch_process pairs with start (%s)',
      async (name, handlers) => {
        await start(handlers(recordingHandler()))
        const done = queueWith(1).push(batchOf(0, [4, 5]), batchOf(1, [4, 5]))
        await until(() => handled.length === 1)

        moveEpoch(1, 6)
        gate.open()
        await done

        expect(handled.filter(({ partition: id }) => id === 1)).toEqual([])
        expect(resolvedOf(1)).toEqual([])
        expect(eventsOf(1)).toEqual([])
        // Partition 0 shares the run and its epoch never moved: delivered as before.
        expect(handled).toEqual(
          name === 'eachMessage'
            ? [
                { partition: 0, offset: '4' },
                { partition: 0, offset: '5' },
              ]
            : [{ partition: 0, offset: undefined }]
        )
        expect(resolvedOf(0)).toEqual(name === 'eachMessage' ? ['4', '5'] : ['5'])
        expect(eventsOf(0)).toEqual(['start', 'end'])
      }
    )

    it.each([['classic'], ['consumer']])(
      'when the cached epoch moves while message 10 is handled, message 11 is not handed to eachMessage (%s)',
      async (kind) => {
        consumerGroup.kind = kind
        if (kind === 'consumer') {
          consumerGroup.rebalanceProtocol = jest.fn(() => 'cooperative')
          consumerGroup.heartbeatInterval = jest.fn(() => 500)
        }
        const eachMessage = jest.fn(async ({ message }) => {
          handled.push(message.offset)
          if (message.offset === '10') moveEpoch(0, 6)
        })
        runner = createTestRunner({ eachBatch: undefined, eachMessage })
        runner.running = true

        await runner.handleBatch(batchOf(0, [10, 11]))

        expect(handled).toEqual(['10'])
        expect(eventsOf(0)).toEqual(['start', 'end'])
      }
    )

    it('a filtered-only batch whose start_batch_process listener moves the epoch resolves nothing and commits nothing', async () => {
      await start()
      instrumentationEmitter.addListener(START_BATCH_PROCESS, ({ payload }) =>
        moveEpoch(payload.partition, 6)
      )
      const control = new Batch(
        topicName,
        '5',
        { partition: 1, highWatermark: 100, messages: [{ offset: '5', isControlRecord: true }] },
        FETCH_EPOCH
      )
      consumerGroup.commitOffsetsIfNecessary.mockClear()

      await runner.handleBatch(control)

      expect(resolvedOf(1)).toEqual([])
      expect(consumerGroup.commitOffsetsIfNecessary).not.toHaveBeenCalled()
      expect(eventsOf(1)).toEqual(['start', 'end'])
    })

    it.each([
      ['eachMessage', (handler) => ({ eachBatch: undefined, eachMessage: handler })],
      ['eachBatch', (handler) => ({ eachBatch: handler })],
    ])(
      'a start_batch_process listener that moves the epoch keeps the batch from the handler (%s)',
      async (_name, handlers) => {
        const handler = jest.fn()
        await start(handlers(handler))
        instrumentationEmitter.addListener(START_BATCH_PROCESS, ({ payload }) =>
          moveEpoch(payload.partition, 6)
        )

        await runner.handleBatch(batchOf(1, [4, 5]))

        expect(handler).not.toHaveBeenCalled()
        expect(resolvedOf(1)).toEqual([])
        expect(eventsOf(1)).toEqual(['start', 'end'])
      }
    )

    it("eachBatch resolveOffset with a numeric offset records that record's leader epoch, not the next position's", async () => {
      const EPOCH = 7
      const handler = jest.fn(async ({ resolveOffset }) => resolveOffset(100))
      await start({ eachBatch: handler, eachBatchAutoResolve: false })
      const body = batchBytes({
        baseOffset: 100n,
        partitionLeaderEpoch: EPOCH,
        records: [
          recordBytes({ key: 'k', value: 'v' }),
          recordBytes({ offsetDelta: 1, key: 'k', value: 'v' }),
        ],
      })
      const size = Buffer.alloc(4)
      size.writeInt32BE(body.length)
      const partitionData = await decodeMessages(new Decoder(Buffer.concat([size, body])))

      await runner.handleBatch(
        new Batch(topicName, '100', { partition: 0, highWatermark: '500', ...partitionData })
      )

      expect(handler).toHaveBeenCalledTimes(1)
      expect(consumerGroup.resolveOffset).toHaveBeenCalledTimes(1)
      expect(consumerGroup.resolveOffset).toHaveBeenCalledWith({
        topic: topicName,
        partition: 0,
        offset: 100,
        leaderEpoch: EPOCH,
      })
    })
  })

  describe('a compacted batch that decoding left without records to deliver', () => {
    const EPOCH = 5

    /** A Batch built from what the Fetch decoder returns for these record batches. */
    const decoded = async (fetchedOffset, ...batches) => {
      const body = Buffer.concat(batches)
      const size = Buffer.alloc(4)
      size.writeInt32BE(body.length)
      const partitionData = await decodeMessages(new Decoder(Buffer.concat([size, body])))
      return new Batch(topicName, fetchedOffset, {
        partition,
        highWatermark: '500',
        ...partitionData,
      })
    }
    const emptyCompacted = () =>
      batchBytes({ baseOffset: 0n, partitionLeaderEpoch: EPOCH, lastOffsetDelta: 99 })

    const start = async (partial) => {
      runner = createTestRunner(partial)
      runner.scheduleFetchManager = jest.fn()
      await runner.start()
    }

    it('moves the position to the end of an empty batch, with its epoch, and delivers the later record', async () => {
      await start()

      await runner.handleBatch(await decoded(0, emptyCompacted()))

      expect(consumerGroup.resolveOffset).toHaveBeenCalledTimes(1)
      expect(consumerGroup.resolveOffset).toHaveBeenCalledWith({
        topic: topicName,
        partition,
        offset: '99',
        leaderEpoch: EPOCH,
      })
      expect(eachBatch).not.toHaveBeenCalled()

      const later = batchBytes({
        baseOffset: 100n,
        partitionLeaderEpoch: EPOCH,
        records: [recordBytes({ key: 'k', value: 'v' })],
      })
      await runner.handleBatch(await decoded(100, later))

      expect(eachBatch).toHaveBeenCalledTimes(1)
      expect(eachBatch.mock.calls[0][0].batch.messages.map(({ offset }) => offset)).toEqual(['100'])
    })

    it('reaches the end of a batch whose only retained record is behind the fetch offset in one step', async () => {
      await start()
      const compacted = batchBytes({
        baseOffset: 0n,
        partitionLeaderEpoch: EPOCH,
        lastOffsetDelta: 99,
        records: [recordBytes({ offsetDelta: 1 })],
      })

      await runner.handleBatch(await decoded(2, compacted))

      expect(consumerGroup.resolveOffset).toHaveBeenCalledTimes(1)
      expect(consumerGroup.resolveOffset).toHaveBeenCalledWith({
        topic: topicName,
        partition,
        offset: '99',
        leaderEpoch: EPOCH,
      })
    })

    it.each([
      ['eachMessage', { eachBatch: undefined, eachMessage: jest.fn() }],
      ['eachBatch', { eachBatch: jest.fn() }],
      ['eachBatch with auto-resolve off', { eachBatch: jest.fn(), eachBatchAutoResolve: false }],
    ])('is resolved without calling %s', async (_name, handlers) => {
      await start(handlers)

      await runner.handleBatch(await decoded(0, emptyCompacted()))

      expect(consumerGroup.resolveOffset).toHaveBeenCalledWith(
        expect.objectContaining({ offset: '99', leaderEpoch: EPOCH })
      )
      expect(handlers.eachMessage || handlers.eachBatch).not.toHaveBeenCalled()
    })

    it('leaves a handler that resolves explicitly with its own resolution', async () => {
      const handler = jest.fn(async ({ resolveOffset }) => resolveOffset('100'))
      await start({ eachBatch: handler, eachBatchAutoResolve: false })
      const retained = batchBytes({
        baseOffset: 100n,
        partitionLeaderEpoch: EPOCH,
        lastOffsetDelta: 99,
        records: [recordBytes({ key: 'k', value: 'v' })],
      })

      await runner.handleBatch(await decoded(100, retained))

      expect(handler).toHaveBeenCalledTimes(1)
      expect(consumerGroup.resolveOffset).toHaveBeenCalledTimes(1)
      expect(consumerGroup.resolveOffset).toHaveBeenCalledWith(
        expect.objectContaining({ offset: '100', leaderEpoch: EPOCH })
      )
    })
  })
})

describe('Consumer > Runner > heartbeat through a real group membership', () => {
  const INTERVAL = 3000
  let now, wire, group, runner

  /**
   * A real `ConsumerGroup` and runner over a coordinator double whose heartbeat answers with
   * `answer()`. A classic member is joined, its interval just restarted.
   */
  const build = ({ groupProtocol = 'classic', answer = async () => {} } = {}) => {
    wire = {
      nodeId: 0,
      isConnected: () => true,
      heartbeat: jest.fn(answer),
      consumerGroupHeartbeat: jest.fn(async () => {
        throw new Error('no consumer-protocol heartbeat is expected on the wire')
      }),
    }
    group = new ConsumerGroup({
      retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 2 },
      cluster: {
        connect: jest.fn(async () => {}),
        refreshMetadata: jest.fn(async () => {}),
        refreshMetadataIfNecessary: jest.fn(async () => {}),
        committedOffsets: () => ({}),
        topicName: () => undefined,
        topicId: () => undefined,
        findTopicPartitionMetadata: () => [],
        findGroupCoordinator: jest.fn(async () => wire),
        addMultipleTargetTopics: jest.fn(async () => {}),
      },
      groupId: 'heartbeat-group',
      groupProtocol,
      topics: ['topic-a'],
      topicConfigurations: { 'topic-a': {} },
      logger: newLogger(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
      assigners: [],
      sessionTimeout: 10000,
      rebalanceTimeout: 60000,
      heartbeatInterval: INTERVAL,
      maxBytesPerPartition: 1048576,
      minBytes: 1,
      maxBytes: 1048576,
      maxWaitTimeInMs: 1000,
      autoCommit: true,
      autoCommitInterval: null,
      autoCommitThreshold: null,
      isolationLevel: 0,
      rackId: '',
      checkCrcs: true,
      metadataMaxAge: 300000,
    })
    group.coordinator = wire
    if (groupProtocol === 'classic') {
      group.memberId = 'member-1'
      group.generationId = 1
      group.lastRequest = now
    }
    runner = new Runner({
      consumerGroup: group,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: newLogger(),
      concurrency: 1,
      heartbeatInterval: INTERVAL,
      maxPollInterval: 300000,
      autoCommit: true,
      onCrash: jest.fn(),
      eachMessage: async () => {},
    })
    runner.running = true
  }

  /** A heartbeat answer held until the test fails it, and a signal that it reached the wire. */
  const heldAnswer = () => {
    const held = {}
    held.written = new Promise((resolve) => (held.onWire = resolve))
    held.answer = () =>
      new Promise((_resolve, reject) => {
        held.fail = reject
        held.onWire()
      })
    return held
  }

  /** @returns {Promise<unknown>} the call's rejection reason, or 'resolved' */
  const outcome = (call) =>
    call.then(
      () => 'resolved',
      (e) => e
    )

  beforeEach(() => {
    now = 1_000_000
    jest.spyOn(Date, 'now').mockImplementation(() => now)
  })

  afterEach(() => jest.restoreAllMocks())

  it('sends the classic heartbeat exactly at the deadline, and ends the heartbeat outage on the calls before it', async () => {
    build()
    runner.outages.heartbeat.since = 5

    now += INTERVAL - 1
    await expect(runner.heartbeat()).resolves.toBeUndefined()
    expect(wire.heartbeat).not.toHaveBeenCalled()
    expect(runner.outages.heartbeat.since).toBeNull()

    now += 1
    await expect(runner.heartbeat()).resolves.toBeUndefined()
    expect(wire.heartbeat).toHaveBeenCalledTimes(1)
    expect(wire.heartbeat).toHaveBeenCalledWith(
      expect.objectContaining({
        groupId: 'heartbeat-group',
        memberId: 'member-1',
        groupGenerationId: 1,
      })
    )
    expect(group.lastRequest).toBe(now)

    await runner.heartbeat()
    expect(wire.heartbeat).toHaveBeenCalledTimes(1)
  })

  it('joins a classic heartbeat in flight, even once the interval reads not due, and its rejection reaches both callers', async () => {
    const held = heldAnswer()
    build({ answer: held.answer })
    const failure = new Error('the heartbeat connection broke')

    now += INTERVAL
    const first = runner.heartbeat()
    await held.written
    // Nothing else restarts the interval in production; this isolates the in-flight check.
    group.lastRequest = now
    const second = runner.heartbeat()
    const firstOutcome = outcome(first)
    const secondOutcome = outcome(second)
    held.fail(failure)

    expect(await firstOutcome).toBe(failure)
    expect(await secondOutcome).toBe(failure)
    expect(wire.heartbeat).toHaveBeenCalledTimes(1)
  })

  it('sends one classic heartbeat for two calls in one turn that straddle the deadline, and a third caller joins it and sees its rejection', async () => {
    const held = heldAnswer()
    build({ answer: held.answer })
    const failure = new Error('the heartbeat connection broke')

    now += INTERVAL - 1
    const before = runner.heartbeat()
    now += 1
    const due = runner.heartbeat()
    await held.written
    const joined = runner.heartbeat()
    const dueOutcome = outcome(due)
    const joinedOutcome = outcome(joined)
    held.fail(failure)

    await expect(before).resolves.toBeUndefined()
    expect(await dueOutcome).toBe(failure)
    expect(await joinedOutcome).toBe(failure)
    expect(wire.heartbeat).toHaveBeenCalledTimes(1)
  })

  it('makes the full group heartbeat call on every consumer-protocol heartbeat', async () => {
    build({ groupProtocol: 'consumer' })
    const membershipHeartbeat = jest
      .spyOn(group.groupMembership, 'heartbeat')
      .mockResolvedValue(undefined)
    const interval = group.heartbeatInterval()

    await runner.heartbeat()
    await runner.heartbeat()

    expect(group.heartbeatIdle(interval)).toBe(false)
    expect(membershipHeartbeat).toHaveBeenCalledTimes(2)
    expect(membershipHeartbeat).toHaveBeenCalledWith(expect.objectContaining({ interval }))
  })
})
