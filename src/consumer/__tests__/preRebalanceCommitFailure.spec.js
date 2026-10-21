const Runner = require('../runner')
const GroupOwnership = require('../groupOwnership')
const handlerContext = require('../handlerContext')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { KafkaJSProtocolError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const { stampOffsetCommit } = require('../rebalanceErrorContract')
const { newLogger } = require('testHelpers')

/**
 * A runner whose round is observed instead of opening another membership. Commit and round calls
 * are real Runner control flow; the failure is injected only at the group's single commit method.
 * @param {'classic'|'consumer'} kind the group protocol
 * @param {Error|null} failure the pre-rebalance result
 * @returns {object} runner, group, round spy and final cleanup
 */
const build = (kind, failure) => {
  const ownership = new GroupOwnership()
  const group = {
    groupId: 'group',
    memberId: 'member',
    kind,
    groupInstanceId: null,
    ownership,
    groupMembership: {},
    attach() {},
    getNodeIds: () => [],
    rebalanceProtocol: () => 'eager',
    commitOffsets: jest.fn(async () => {
      if (failure) throw failure
    }),
    onMembershipError: jest.fn(),
    discardUncommittedOffsets: jest.fn(),
    heartbeatInterval: () => 1000,
  }
  const runner = new Runner({
    consumerGroup: group,
    instrumentationEmitter: new InstrumentationEventEmitter(),
    logger: newLogger(),
    concurrency: 1,
    heartbeatInterval: 1000,
    maxPollInterval: 300000,
    onCrash: jest.fn(),
    retry: { retries: 3, initialRetryTime: 1000 },
  })
  runner.running = true
  const round = jest.spyOn(runner.rebalanceCoordinator, 'runRound').mockResolvedValue()
  const abandon = jest.spyOn(runner.rebalanceCoordinator, 'abandon')
  return {
    runner,
    group,
    round,
    abandon,
    close: () => {
      runner.heartbeatManager.shutdown()
      runner.pollTimer.stop()
      expect(jest.getTimerCount()).toBe(0)
    },
  }
}

describe.each(['classic', 'consumer'])('eager pre-rebalance commit on %s', (kind) => {
  let fixture
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => {
    fixture?.close()
    fixture = undefined
    jest.useRealTimers()
  })

  it.each([16, 7])(
    'logs code %i and begins the revoked round without retry backoff',
    async (code) => {
      const failure = new KafkaJSProtocolError(createErrorFromCode(code))
      fixture = build(kind, failure)
      const before = Date.now()
      await fixture.runner.resolveRebalanceVerdict({
        kind: 'revoked',
        reason: 'rebalance-in-progress',
        error: new Error('round'),
      })
      expect(fixture.group.commitOffsets).toHaveBeenCalledTimes(1)
      expect(fixture.round).toHaveBeenCalledWith('rebalance-in-progress', 'revoked')
      expect(fixture.group.discardUncommittedOffsets).not.toHaveBeenCalled()
      expect(Date.now()).toBe(before)
      expect(jest.getTimerCount()).toBe(0)
    }
  )

  it('commits before the revoked round whatever handler batch the caller inherited', async () => {
    fixture = build(kind, null)
    const inherited = {
      topic: 'topic',
      partition: 0,
      assignmentEpoch: 1,
      acquireClock: 1,
      ownershipKey: 'topic|0',
    }
    await handlerContext.run(inherited, () =>
      fixture.runner.resolveRebalanceVerdict({
        kind: 'revoked',
        reason: 'rebalance-in-progress',
        error: new Error('round'),
      })
    )
    expect(fixture.group.commitOffsets).toHaveBeenCalledTimes(1)
    expect(fixture.round).toHaveBeenCalledWith('rebalance-in-progress', 'revoked')
  })

  it.each([22, 25, 69, 110])('continues code %i as a lost round in the same turn', async (code) => {
    const failure = stampOffsetCommit(new KafkaJSProtocolError(createErrorFromCode(code)))
    fixture = build(kind, failure)
    const before = Date.now()
    await fixture.runner.resolveRebalanceVerdict({
      kind: 'revoked',
      reason: 'rebalance-in-progress',
      error: new Error('round'),
    })
    expect(fixture.group.commitOffsets).toHaveBeenCalledTimes(1)
    expect(fixture.group.discardUncommittedOffsets).toHaveBeenCalledTimes(1)
    expect(fixture.round).toHaveBeenCalledWith(expect.any(String), 'lost')
    expect(fixture.group.ownership.refusal(fixture.group.ownership.token())).not.toBeNull()
    expect(Date.now()).toBe(before)
  })

  it('sends no commit and continues as a lost round when the loss was learned first', async () => {
    fixture = build(kind, null)
    const { ownership } = fixture.group
    const cause = new KafkaJSProtocolError(createErrorFromCode(25))
    ownership.lose(cause, ownership.token())
    await fixture.runner.resolveRebalanceVerdict({
      kind: 'revoked',
      reason: 'rebalance-in-progress',
      error: new Error('round'),
    })
    expect(fixture.group.commitOffsets).not.toHaveBeenCalled()
    expect(fixture.group.discardUncommittedOffsets).toHaveBeenCalledTimes(1)
    expect(fixture.round).toHaveBeenCalledWith(expect.any(String), 'lost')
    expect(ownership.refusal(ownership.token()).cause).toBe(cause)
  })

  it('abandons a current static fencing rather than starting a replacement round', async () => {
    const failure = new KafkaJSProtocolError(createErrorFromCode(82))
    fixture = build(kind, failure)
    await expect(
      fixture.runner.resolveRebalanceVerdict({
        kind: 'revoked',
        reason: 'rebalance-in-progress',
        error: new Error('round'),
      })
    ).rejects.toBe(failure)
    expect(fixture.round).not.toHaveBeenCalled()
    expect(fixture.abandon).toHaveBeenCalledWith('lost')
    const token = fixture.group.ownership.token()
    expect(fixture.group.ownership.renew()).toBe(token)
  })

  it('commits valid pre-revoke progress once, before starting its round', async () => {
    fixture = build(kind, null)
    const order = []
    fixture.group.commitOffsets.mockImplementation(async () => {
      order.push('commit')
    })
    fixture.round.mockImplementation(async () => {
      order.push('round')
    })
    await fixture.runner.resolveRebalanceVerdict({
      kind: 'revoked',
      reason: 'rebalance-in-progress',
      error: new Error('round'),
    })
    expect(order).toEqual(['commit', 'round'])
    expect(fixture.group.ownership.refusal(fixture.group.ownership.token())).toBeNull()
  })
})
