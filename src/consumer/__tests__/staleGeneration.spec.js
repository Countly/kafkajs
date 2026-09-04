const Runner = require('../runner')
const { newLogger } = require('testHelpers')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { createErrorFromCode } = require('../../protocol/error')

const ILLEGAL_GENERATION = 22
const UNKNOWN_MEMBER_ID = 25
const REBALANCE_IN_PROGRESS = 27
const NOT_COORDINATOR_FOR_GROUP = 16

const createRunner = (consumerGroupOverrides) => {
  const consumerGroup = {
    groupId: 'g',
    memberId: 'm',
    generationId: 1,
    heartbeat: jest.fn(async () => {}),
    commitOffsetsIfNecessary: jest.fn(async () => {}),
    commitOffsets: jest.fn(async () => {}),
    discardUncommittedOffsets: jest.fn(),
    joinAndSync: jest.fn(async () => {}),
    getNodeIds: () => [],
    ...consumerGroupOverrides,
  }

  const runner = new Runner({
    logger: newLogger(),
    consumerGroup,
    instrumentationEmitter: new InstrumentationEventEmitter(),
    autoCommit: true,
    concurrency: 1,
    onCrash: jest.fn(),
  })

  return { runner, consumerGroup }
}

describe('Consumer > Runner > stale generation', () => {
  it.each([
    ['ILLEGAL_GENERATION', ILLEGAL_GENERATION],
    ['UNKNOWN_MEMBER_ID', UNKNOWN_MEMBER_ID],
  ])('discards offsets and does not autocommit on %s (member is LOST)', async (_name, code) => {
    const { runner, consumerGroup } = createRunner({
      heartbeat: jest.fn(async () => {
        throw createErrorFromCode(code)
      }),
    })

    await expect(runner.heartbeat()).rejects.toThrow()
    expect(consumerGroup.discardUncommittedOffsets).toHaveBeenCalled()
    expect(consumerGroup.commitOffsetsIfNecessary).not.toHaveBeenCalled()
    expect(consumerGroup.commitOffsets).not.toHaveBeenCalled()
  })

  it.each([
    ['REBALANCE_IN_PROGRESS', REBALANCE_IN_PROGRESS],
    ['NOT_COORDINATOR_FOR_GROUP', NOT_COORDINATOR_FOR_GROUP],
  ])('still commits on %s (partitions are REVOKED / coordinator moved)', async (_name, code) => {
    const { runner, consumerGroup } = createRunner({
      heartbeat: jest.fn(async () => {
        throw createErrorFromCode(code)
      }),
    })

    await expect(runner.heartbeat()).rejects.toThrow()
    // heartbeat's autoCommitOffsets() calls commitOffsets() unconditionally, exactly as it did
    // before this fix for every isRebalancing() error - only the LOST codes above now skip it.
    expect(consumerGroup.commitOffsets).toHaveBeenCalled()
    expect(consumerGroup.discardUncommittedOffsets).not.toHaveBeenCalled()
  })

  // The fetch path, not just heartbeat(): runner.js's scheduleFetchManager tests isRebalancing()
  // before the UNKNOWN_MEMBER_ID branch, and isRebalancing already matches ILLEGAL_GENERATION.
  // Without splitting that guard the fix never reaches the fetch path at all.
  it.each([
    ['ILLEGAL_GENERATION', ILLEGAL_GENERATION],
    ['UNKNOWN_MEMBER_ID', UNKNOWN_MEMBER_ID],
  ])('discards offsets and rejoins when the fetch manager fails with %s', async (_name, code) => {
    const { runner, consumerGroup } = createRunner({})
    // Stop the runner once it has rejoined, so the fetch-manager retry loop (which would
    // otherwise re-enter scheduleFetchManager on every settle, since fetchManager.start keeps
    // failing the same way) terminates after a single pass instead of spinning forever.
    consumerGroup.joinAndSync = jest.fn(async () => {
      runner.running = false
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
    expect(consumerGroup.commitOffsetsIfNecessary).not.toHaveBeenCalled()
  })
})
