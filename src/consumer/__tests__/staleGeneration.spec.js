const Runner = require('../runner')
const { newLogger } = require('testHelpers')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const {
  events: { REBALANCING },
} = require('../instrumentationEvents')
const { createErrorFromCode } = require('../../protocol/error')

const ILLEGAL_GENERATION = 22
const UNKNOWN_MEMBER_ID = 25
const REBALANCE_IN_PROGRESS = 27
const NOT_COORDINATOR_FOR_GROUP = 16
const FENCED_INSTANCE_ID = 82

const runners = []

afterEach(async () => {
  while (runners.length) {
    const runner = runners.pop()
    // The fetch-path cases clear `running` from inside a stubbed `joinAndSync` to end the retry
    // loop; production only clears it where the heartbeat chain is cancelled too, so restore it
    // and tear the runner down through its own `stop()`.
    runner.running = true
    await runner.stop()
  }
})

const createRunner = (consumerGroupOverrides) => {
  const consumerGroup = {
    groupId: 'g',
    memberId: 'm',
    generationId: 1,
    leaderId: null,
    groupInstanceId: null,
    groupMembership: {},
    attach: jest.fn(),
    rebalanceProtocol: () => 'eager',
    // `runChain` reads both before it can report a round: `assigned()` before the JoinGroup and
    // `isLeader()` when it builds the GROUP_JOIN payload.
    assigned: () => [],
    isLeader: () => false,
    applyRebalanceResult: jest.fn(async () => ({ assignment: [], added: [], revoked: [] })),
    clearStaleAssignmentPending: jest.fn(),
    forgetOwnedPartitions: jest.fn(),
    clearAssignerState: jest.fn(),
    resetGenerationLocally: jest.fn(),
    leave: jest.fn(async () => {}),
    heartbeat: jest.fn(async () => {}),
    commitOffsetsIfNecessary: jest.fn(async () => {}),
    commitOffsets: jest.fn(async () => {}),
    discardUncommittedOffsets: jest.fn(),
    joinAndSync: jest.fn(async () => ({ assignment: {} })),
    getNodeIds: () => [],
    ...consumerGroupOverrides,
  }

  const instrumentationEmitter = new InstrumentationEventEmitter()
  const runner = new Runner({
    logger: newLogger(),
    consumerGroup,
    instrumentationEmitter,
    autoCommit: true,
    concurrency: 1,
    heartbeatInterval: 3000,
    maxPollInterval: 300000,
    onCrash: jest.fn(),
  })

  runners.push(runner)
  return { runner, consumerGroup, instrumentationEmitter }
}

describe('Consumer > Runner > stale generation', () => {
  it.each([
    ['ILLEGAL_GENERATION', ILLEGAL_GENERATION],
    ['UNKNOWN_MEMBER_ID', UNKNOWN_MEMBER_ID],
    ['FENCED_INSTANCE_ID', FENCED_INSTANCE_ID],
  ])(
    'routes %s as a LOST rejoin, and discards only where the rebalance is handled',
    async (_name, code) => {
      const { runner, consumerGroup } = createRunner({
        heartbeat: jest.fn(async () => {
          throw createErrorFromCode(code)
        }),
      })
      const requestRejoin = jest.spyOn(runner.rebalanceCoordinator, 'requestRejoin')

      await expect(runner.heartbeat()).rejects.toThrow()
      expect(requestRejoin).toHaveBeenCalledWith(expect.objectContaining({ kind: 'lost' }))
      // The callback itself neither discards nor commits any more; both belong to the one site.
      expect(consumerGroup.discardUncommittedOffsets).not.toHaveBeenCalled()
      expect(consumerGroup.commitOffsets).not.toHaveBeenCalled()

      // Only a fenced static member rethrows; 22 and 25 discard and rejoin.
      const handled = runner.handleRebalanceError(createErrorFromCode(code))
      await (code === FENCED_INSTANCE_ID ? expect(handled).rejects.toBeDefined() : handled)

      expect(consumerGroup.discardUncommittedOffsets).toHaveBeenCalled()
      expect(consumerGroup.commitOffsetsIfNecessary).not.toHaveBeenCalled()
      expect(consumerGroup.commitOffsets).not.toHaveBeenCalled()
    }
  )

  it.each([
    ['REBALANCE_IN_PROGRESS', REBALANCE_IN_PROGRESS],
    ['NOT_COORDINATOR_FOR_GROUP', NOT_COORDINATOR_FOR_GROUP],
  ])('still commits on %s (partitions are REVOKED / coordinator moved)', async (_name, code) => {
    const { runner, consumerGroup } = createRunner({
      heartbeat: jest.fn(async () => {
        throw createErrorFromCode(code)
      }),
    })
    const requestRejoin = jest.spyOn(runner.rebalanceCoordinator, 'requestRejoin')

    await expect(runner.heartbeat()).rejects.toThrow()
    expect(requestRejoin).toHaveBeenCalledWith(expect.objectContaining({ kind: 'revoked' }))

    // The rebalance is handled once, and that is where the final commit of a still-valid
    // generation happens - exactly as it did for every isRebalancing() error before.
    await runner.handleRebalanceError(createErrorFromCode(code))
    expect(consumerGroup.commitOffsets).toHaveBeenCalled()
    expect(consumerGroup.discardUncommittedOffsets).not.toHaveBeenCalled()
  })

  // The fetch path, not just heartbeat(): runner.js's scheduleFetchManager tests isRebalancing()
  // before the UNKNOWN_MEMBER_ID branch, and isRebalancing already matches ILLEGAL_GENERATION.
  // Without splitting that guard the fix never reaches the fetch path at all.
  it.each([
    ['ILLEGAL_GENERATION', ILLEGAL_GENERATION, 'm'],
    ['UNKNOWN_MEMBER_ID', UNKNOWN_MEMBER_ID, null],
  ])(
    'discards offsets and rejoins when the fetch manager fails with %s',
    async (_name, code, expectedMemberId) => {
      const { runner, consumerGroup, instrumentationEmitter } = createRunner({})
      const rebalancing = jest.fn()
      instrumentationEmitter.addListener(REBALANCING, rebalancing)
      // Stop the runner once it has rejoined, so the fetch-manager retry loop (which would
      // otherwise re-enter scheduleFetchManager on every settle, since fetchManager.start keeps
      // failing the same way) terminates after a single pass instead of spinning forever.
      consumerGroup.joinAndSync = jest.fn(async () => {
        runner.running = false
        return { assignment: {} }
      })
      runner.running = true
      runner.fetchManager = {
        start: jest.fn(async () => {
          throw createErrorFromCode(code)
        }),
        stop: jest.fn(async () => {}),
      }

      runner.scheduleFetchManager()
      await new Promise((resolve) => setTimeout(resolve, 50))

      expect(consumerGroup.discardUncommittedOffsets).toHaveBeenCalled()
      expect(consumerGroup.joinAndSync).toHaveBeenCalled()
      // A lost member rejoins, so the public REBALANCING event fires just like a revocation.
      expect(rebalancing).toHaveBeenCalledTimes(1)
      // The payload now carries `reason`, and its `memberId` is the one the round will actually
      // join with: an unrecognized id is dropped before the rejoin, so the event reports null
      // rather than the id the coordinator just rejected.
      expect(rebalancing.mock.calls[0][0].payload).toEqual({
        groupId: 'g',
        memberId: expectedMemberId,
        reason: 'lost',
      })
      // Offsets must be discarded before rejoining, not after.
      expect(consumerGroup.discardUncommittedOffsets.mock.invocationCallOrder[0]).toBeLessThan(
        consumerGroup.joinAndSync.mock.invocationCallOrder[0]
      )
      // commitOffsets (not commitOffsetsIfNecessary - nothing on this path ever calls that) is
      // the call that would leak an autocommit under the dead generation.
      expect(consumerGroup.commitOffsets).not.toHaveBeenCalled()
      // ILLEGAL_GENERATION means the generation is stale, not the member id: keep it. Only
      // UNKNOWN_MEMBER_ID needs a fresh member id.
      expect(consumerGroup.memberId).toBe(expectedMemberId)
    }
  )

  it('discards offsets, does not rejoin, and rethrows when the fetch manager is fenced (FENCED_INSTANCE_ID)', async () => {
    const { runner, consumerGroup } = createRunner({})
    runner.running = true
    runner.fetchManager = {
      start: jest.fn(async () => {
        throw createErrorFromCode(FENCED_INSTANCE_ID)
      }),
      stop: jest.fn(async () => {}),
    }

    runner.scheduleFetchManager()
    await new Promise((resolve) => setTimeout(resolve, 50))

    expect(consumerGroup.discardUncommittedOffsets).toHaveBeenCalled()
    expect(consumerGroup.joinAndSync).not.toHaveBeenCalled()
    expect(consumerGroup.memberId).toBe('m')
    expect(consumerGroup.commitOffsets).not.toHaveBeenCalled()
    // A fenced static member must not rejoin under a fresh member id into the same fencing loop;
    // the error rethrows all the way to the crash handler instead (restart-on-fenced is a later
    // task's concern, not implemented here).
    expect(runner.onCrash).toHaveBeenCalled()
  })
})
