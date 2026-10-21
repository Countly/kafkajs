const { createCoordinator, lifecycleCells } = require('../../../testHelpers/lifecycle')
const { events } = require('../instrumentationEvents')

describe('Poll expiry observer containment', () => {
  afterEach(() => jest.restoreAllMocks())
  test.each(['classic', 'consumer'])(
    '%s throwing expiry observer cannot stop cleanup',
    async (kind) => {
      const { coordinator, consumerGroup, instrumentationEmitter, heartbeatManager } =
        createCoordinator()
      consumerGroup.kind = kind
      consumerGroup.joinCount = 1
      consumerGroup.heartbeatInterval = () => 1
      consumerGroup.leaveForPollExpiry = jest.fn(async () => {})
      const rejoin = jest.spyOn(coordinator, 'requestRejoin')
      let expiries = 0
      const remove = instrumentationEmitter.addListener(events.POLL_INTERVAL_EXCEEDED, () => {
        expiries++
        throw new Error('observer failure')
      })
      try {
        expect(lifecycleCells()).toContainEqual({
          event: 'poll-expiry',
          boundary: 'poll-expiry:leave',
          protocol: kind === 'classic' ? 'classic-eager' : kind,
          membership: 'dynamic',
        })
        await coordinator.onPollExpired({ elapsedMs: 1001, isStatic: false })
        expect(expiries).toBe(1)
        expect(heartbeatManager.stop).toHaveBeenCalledTimes(1)
        expect(
          kind === 'consumer' ? consumerGroup.leaveForPollExpiry : consumerGroup.leave
        ).toHaveBeenCalledTimes(1)
        expect(consumerGroup.resetGenerationLocally).toHaveBeenCalledTimes(1)
        expect(rejoin).toHaveBeenCalledTimes(1)
      } finally {
        remove()
      }
    }
  )
  test.each(['warning', 'heartbeat stop', 'leave', 'reset', 'request'])(
    'throwing %s step cannot suppress the rest',
    async (step) => {
      const { coordinator, consumerGroup, heartbeatManager } = createCoordinator()
      const fail = () => {
        throw new Error('step failed')
      }
      if (step === 'warning') coordinator.logger.warn = fail
      if (step === 'heartbeat stop') heartbeatManager.stop.mockImplementation(fail)
      if (step === 'leave') consumerGroup.leave.mockImplementation(fail)
      if (step === 'reset') consumerGroup.resetGenerationLocally.mockImplementation(fail)
      if (step === 'request') jest.spyOn(coordinator, 'requestRejoin').mockImplementation(fail)
      coordinator.logger.error = fail
      const rejoin = jest.spyOn(coordinator, 'requestRejoin')
      await expect(
        coordinator.onPollExpired({ elapsedMs: 1001, isStatic: false })
      ).resolves.toBeUndefined()
      expect(rejoin).toHaveBeenCalledTimes(1)
    }
  )
  test.each(['classic', 'consumer'])(
    '%s real heartbeat chain reports a throwing-observer expiry once',
    async (kind) => {
      jest.useFakeTimers()
      const HeartbeatManager = require('../heartbeatManager')
      const PollTimer = require('../pollTimer')
      const { newLogger } = require('testHelpers')
      const { coordinator, consumerGroup, instrumentationEmitter } = createCoordinator()
      consumerGroup.kind = kind
      consumerGroup.joinCount = 1
      consumerGroup.heartbeatInterval = () => 1
      consumerGroup.leaveForPollExpiry = jest.fn(async () => {})
      const pollTimer = new PollTimer({ pollDeadline: 2 })
      coordinator.pollTimer = pollTimer
      const rejoin = jest.spyOn(coordinator, 'requestRejoin').mockImplementation(() => {})
      let count = 0
      const remove = instrumentationEmitter.addListener(events.POLL_INTERVAL_EXCEEDED, () => {
        count++
        throw new Error('observer failed')
      })
      const manager = new HeartbeatManager({
        groupMembership: consumerGroup,
        pollTimer,
        heartbeatInterval: 1,
        logger: newLogger(),
        onError: (error) => {
          throw error
        },
        onPollExpired: (details) => coordinator.onPollExpired({ ...details, isStatic: false }),
      })
      coordinator.heartbeatManager = manager
      try {
        manager.ensureStarted()
        await jest.advanceTimersByTimeAsync(20)
        expect(count).toBe(1)
        expect(rejoin).toHaveBeenCalledTimes(1)
        expect(
          kind === 'consumer' ? consumerGroup.leaveForPollExpiry : consumerGroup.leave
        ).toHaveBeenCalledTimes(1)
        expect(manager.isRunning()).toBe(false)
      } finally {
        remove()
        manager.shutdown()
        jest.useRealTimers()
      }
    }
  )
  test('an observer throwing null is logged before the remaining cleanup', async () => {
    const { coordinator, consumerGroup, instrumentationEmitter, heartbeatManager } =
      createCoordinator()
    const report = jest.spyOn(coordinator.logger, 'error')
    const rejoin = jest.spyOn(coordinator, 'requestRejoin')
    const thrown = null
    const remove = instrumentationEmitter.addListener(events.POLL_INTERVAL_EXCEEDED, () => {
      throw thrown
    })
    try {
      await coordinator.onPollExpired({ elapsedMs: 1001, isStatic: false })
      expect(report).toHaveBeenCalledWith('Poll expiry poll event failed', { error: 'undefined' })
      expect(heartbeatManager.stop).toHaveBeenCalledTimes(1)
      expect(consumerGroup.leave).toHaveBeenCalledTimes(1)
      expect(consumerGroup.resetGenerationLocally).toHaveBeenCalledTimes(1)
      expect(rejoin).toHaveBeenCalledTimes(1)
    } finally {
      remove()
    }
  })
})
