const Runner = require('../runner')
const Batch = require('../batch')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { newLogger, secureRandom, waitFor } = require('testHelpers')
const sleep = require('../../utils/sleep')
const {
  KafkaJSProtocolError,
  KafkaJSNotImplemented,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSDeliveryTimeout,
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
      expect(scope).toEqual({ deadlineAt: operation.deadlineAt, guard: operation.guard })
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

      await runner.heartbeat()

      expect(consumerGroup.heartbeat).toHaveBeenCalledWith({
        interval: 500,
        guard: expect.any(Function),
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
      jest.spyOn(require('node:perf_hooks').performance, 'now').mockImplementation(() => now)
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
      jest.spyOn(require('node:perf_hooks').performance, 'now').mockImplementation(() => 0)
      try {
        let busy = true
        const baseline = jest.getTimerCount()
        const done = runner.awaitQuiescence(() => busy, {
          deadlineAt: 20,
          identity: { round: roundOf() },
        })
        busy = false
        await jest.advanceTimersByTimeAsync(20)
        await expect(done).resolves.toBeUndefined()
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
})
