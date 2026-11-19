const {
  protocolError,
  target,
  advanceUntil,
  deferred,
  settled,
  start,
  teardown,
  forget,
  beats,
  inCauseChain,
} = require('./fixtures/scriptedConsumer')

beforeEach(() => jest.useFakeTimers())
afterEach(async () => {
  await teardown()
  jest.useRealTimers()
  jest.restoreAllMocks()
})

/**
 * Holds the established member's next positive-epoch heartbeat; every other heartbeat is answered
 * as before. `first` answers positive-epoch heartbeats sent before the held one.
 * @returns {{ release: (error: Error) => void, sent: () => boolean }}
 */
const holdNextBeat = (run, first = []) => {
  const held = deferred()
  const answer = run.state.answer
  let sent = false
  let positive = 0
  run.state.answer = (req) => {
    if (req.memberEpoch <= 0 || sent) return answer(req)
    if (positive < first.length) return first[positive++]
    sent = true
    return held.promise
  }
  return { release: (error) => held.resolve(Promise.reject(error)), sent: () => sent }
}

describe('Consumer > crash supervision controls', () => {
  describe('a fatal heartbeat answer and the membership it was sent under', () => {
    it('an answer held across an apply that renewed ownership is accepted for the same membership and ends the run', async () => {
      const policy = jest.fn(async () => true)
      const run = await start({ retry: { initialRetryTime: 300, restartOnFailure: policy } })
      const group = run.coordinators[0].consumerGroup
      const membership = group.groupMembership
      // The new target names a partition metadata learns only after a delayed refresh, so its
      // apply lands while the next heartbeat is held.
      run.state.partial.add('topic-a')
      run.state.metadataDelay = 2000
      const beat = holdNextBeat(run, [target(1, { 'topic-a': [0, 1] })])
      await advanceUntil(beat.sent, 5000)
      const tokenAtSend = group.ownership.token()
      const revisionAtSend = membership.membershipRevision
      await advanceUntil(() => group.ownership.token() !== tokenAtSend, 5000)

      expect(beat.sent()).toBe(true)
      expect(group.ownership.token()).not.toBe(tokenAtSend)
      expect(membership.membershipRevision).toBe(revisionAtSend)
      expect(run.crashes).toEqual([])

      const answered = protocolError(69)
      beat.release(answered)
      await advanceUntil(() => run.crashes.length > 0, 10000)
      const joins = beats(run, false)
      await jest.advanceTimersByTimeAsync(60000)

      expect(policy).toHaveBeenCalledTimes(1)
      expect(inCauseChain(policy.mock.calls[0][0], answered)).toBe(true)
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].restart).toBe(false)
      expect(inCauseChain(run.crashes[0].error, answered)).toBe(true)
      expect(beats(run, false)).toBe(joins)
      expect(run.cluster.connect).toHaveBeenCalledTimes(1)
    }, 60000)

    it('an answer held across a forced close and a new run changes neither the closed membership nor the new run', async () => {
      const policy = jest.fn(async () => true)
      const run = await start({ retry: { initialRetryTime: 300, restartOnFailure: policy } })
      const closed = run.coordinators[0].consumerGroup.groupMembership
      const beat = holdNextBeat(run)
      await advanceUntil(beat.sent, 5000)
      const closing = settled(run.consumer.disconnect({ timeout: 0 }))
      await advanceUntil(() => closing.done, 2000)
      expect(closing).toMatchObject({ done: true, value: undefined })
      const closedBefore = [closed.state, closed.memberEpoch]

      const again = settled(run.consumer.run({ eachMessage: async () => {} }))
      await advanceUntil(() => again.done, 5000)
      expect(again).toMatchObject({ done: true, value: undefined })
      await jest.advanceTimersByTimeAsync(1000)
      const current = run.coordinators[run.coordinators.length - 1].consumerGroup.groupMembership
      expect(current).not.toBe(closed)
      const before = { state: current.state, epoch: current.memberEpoch, joins: beats(run, false) }
      const releasedAt = Date.now()

      beat.release(protocolError(69))
      await jest.advanceTimersByTimeAsync(3000)

      expect([closed.state, closed.memberEpoch]).toEqual(closedBefore)
      expect({
        state: current.state,
        epoch: current.memberEpoch,
        joins: beats(run, false),
      }).toEqual(before)
      expect(policy).not.toHaveBeenCalled()
      expect(run.crashes).toEqual([])
      const cadence = run.log
        .filter(({ kind, at }) => kind === 'heartbeat' && at >= releasedAt)
        .map(({ at, epoch }) => [at, epoch])
      expect(cadence.length).toBeGreaterThanOrEqual(5)
      expect(cadence.every(([, epoch]) => epoch === before.epoch)).toBe(true)
      expect(cadence.slice(1).map(([at], i) => at - cadence[i][0])).toEqual(
        Array(cadence.length - 1).fill(500)
      )
    }, 60000)

    it('GROUP_ID_NOT_FOUND answering the leave completes the disconnect with the membership unsubscribed and no CRASH', async () => {
      const policy = jest.fn(async () => true)
      const run = await start({ retry: { initialRetryTime: 300, restartOnFailure: policy } })
      const membership = run.coordinators[0].consumerGroup.groupMembership
      const answer = run.state.answer
      run.state.answer = (req) =>
        req.memberEpoch < 0 ? Promise.reject(protocolError(69)) : answer(req)
      const closing = settled(run.consumer.disconnect())
      await advanceUntil(() => closing.done, 10000)
      await jest.advanceTimersByTimeAsync(5000)

      expect(closing).toMatchObject({ done: true, value: undefined })
      expect(run.log.filter(({ kind, epoch }) => kind === 'heartbeat' && epoch < 0)).toHaveLength(1)
      expect(membership.state).toBe('UNSUBSCRIBED')
      expect(policy).not.toHaveBeenCalled()
      expect(run.crashes).toEqual([])
      expect(run.cluster.disconnect).toHaveBeenCalledTimes(1)
      forget()
    }, 60000)
  })

  describe('answers outside the heartbeat fatal rule keep their outcome', () => {
    /** Waits for a CRASH or 10 s, then 10 s more, and returns what the run did. */
    const outcome = async (run, policy) => {
      await advanceUntil(() => run.crashes.length > 0, 10000)
      await jest.advanceTimersByTimeAsync(10000)
      return {
        crashes: run.crashes.map(({ error, restart }) => [error.code, restart]),
        policy: policy.mock.calls.length,
        joins: beats(run, false),
        connects: run.cluster.connect.mock.calls.length,
        lost: run.log.filter(({ kind }) => kind === 'PARTITIONS_LOST').length,
      }
    }

    it.each([
      ['TOPIC_AUTHORIZATION_FAILED (29)', 29],
      ['COORDINATOR_LOAD_IN_PROGRESS (14)', 14],
      ['COORDINATOR_NOT_AVAILABLE (15)', 15],
      ['NOT_COORDINATOR (16)', 16],
    ])(
      '%s answering a current heartbeat: no CRASH, no policy call, no rejoin, and heartbeats continue',
      async (_, code) => {
        const policy = jest.fn(async () => true)
        const run = await start({
          retry: { initialRetryTime: 300, restartOnFailure: policy },
          fault: () => protocolError(code),
        })
        run.arm()
        const observed = await outcome(run, policy)
        const fault = run.faultAt()

        expect(fault).not.toBeNull()
        expect(observed).toEqual({ crashes: [], policy: 0, joins: 1, connects: 1, lost: 0 })
        expect(
          run.log.filter(({ kind, epoch, at }) => kind === 'heartbeat' && epoch > 0 && at > fault)
            .length
        ).toBeGreaterThanOrEqual(10)
      },
      60000
    )

    it('GROUP_ID_NOT_FOUND answering a current OffsetCommit: the loss recovery rejoins and redelivers, with no CRASH', async () => {
      const policy = jest.fn(async () => true)
      const delivered = []
      const run = await start({
        retry: { initialRetryTime: 300, restartOnFailure: policy },
        eachMessage: async ({ message }) => delivered.push(message.offset),
      })
      const commit = run.coordinator.offsetCommit.getMockImplementation()
      let refused = 0
      run.coordinator.offsetCommit.mockImplementation(async (request) => {
        if (refused++ === 0) throw protocolError(69)
        return commit(request)
      })
      run.state.queue['topic-a|0'] = [40, 41]
      const observed = await outcome(run, policy)

      expect(refused).toBeGreaterThan(1)
      expect(observed).toEqual({ crashes: [], policy: 0, joins: 2, connects: 1, lost: 1 })
      expect(delivered).toEqual(['40', '41', '41'])
    }, 60000)

    it('an unrecognised Fetch error: the policy decides, one CRASH with restart true, then one restart', async () => {
      const policy = jest.fn(async () => true)
      const delivered = []
      const run = await start({
        retry: { initialRetryTime: 300, restartOnFailure: policy },
        eachMessage: async ({ message }) => delivered.push(message.offset),
      })
      await advanceUntil(() => delivered.length === 1)
      const broker = await run.cluster.findBroker()
      const answered = protocolError(1042)
      broker.fetch.mockImplementationOnce(async () => {
        throw answered
      })
      const observed = await outcome(run, policy)

      expect(inCauseChain(policy.mock.calls[0][0], answered)).toBe(true)
      expect(inCauseChain(run.crashes[0].error, answered)).toBe(true)
      expect(observed).toEqual({
        crashes: [[answered.code, true]],
        policy: 1,
        joins: 2,
        connects: 2,
        lost: 0,
      })
    }, 60000)

    it('a handler throwing its own GROUP_ID_NOT_FOUND error: the policy decides, one CRASH with restart true, then one restart', async () => {
      const policy = jest.fn(async () => true)
      const thrown = protocolError(69)
      let handled = 0
      const run = await start({
        retry: { initialRetryTime: 300, restartOnFailure: policy },
        eachMessage: async () => {
          if (handled++ === 0) throw thrown
        },
      })
      const observed = await outcome(run, policy)

      expect(inCauseChain(policy.mock.calls[0][0], thrown)).toBe(true)
      expect(inCauseChain(run.crashes[0].error, thrown)).toBe(true)
      expect(observed).toEqual({ crashes: [[69, true]], policy: 1, joins: 2, connects: 2, lost: 0 })
    }, 60000)
  })
})
