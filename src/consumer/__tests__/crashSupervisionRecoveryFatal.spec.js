const {
  GROUP_AUTHORIZATION_FAILED,
  fenced,
  protocolError,
  advanceUntil,
  deferred,
  start,
  teardown,
  inCauseChain,
} = require('./fixtures/scriptedConsumer')

beforeEach(() => jest.useFakeTimers())
afterEach(async () => {
  await teardown()
  jest.useRealTimers()
  jest.restoreAllMocks()
})

describe('Consumer > crash supervision > a fatal answer to the loss recovery of an independent handler failure', () => {
  it.each([
    ['GROUP_AUTHORIZATION_FAILED (30)', GROUP_AUTHORIZATION_FAILED],
    ['GROUP_ID_NOT_FOUND (69)', 69],
  ])(
    '%s answering the recovery join: CRASH reports that answer with restart false, not the handler failure',
    async (_, code) => {
      const policy = jest.fn(async () => true)
      const failure = Object.assign(new Error('handler failed'), { retriable: false })
      const join = deferred()
      let joinSent = false
      let handled = 0
      const run = await start({
        retry: { initialRetryTime: 300, restartOnFailure: policy },
        fault: fenced,
        onZero: () => {
          joinSent = true
          return join.promise
        },
        eachMessage: async (_payload, held) => {
          if (handled++ > 0) return
          await held.promise
          throw failure
        },
      })
      await advanceUntil(() => handled === 1)
      run.arm()
      await advanceUntil(() => run.faultAt() !== null)
      run.held.resolve()
      await advanceUntil(() => joinSent, 5000)
      expect(joinSent).toBe(true)
      expect(run.crashes).toEqual([])

      const answered = protocolError(code)
      join.resolve(Promise.reject(answered))
      await advanceUntil(() => run.crashes.length > 0, 10000)
      await jest.advanceTimersByTimeAsync(5000)

      expect(run.crashes).toHaveLength(1)
      expect(inCauseChain(run.crashes[0].error, answered)).toBe(true)
      expect(inCauseChain(run.crashes[0].error, failure)).toBe(false)
      expect(run.crashes[0].restart).toBe(false)
      expect(policy).toHaveBeenCalledTimes(1)
      expect(run.cluster.connect).toHaveBeenCalledTimes(1)
    },
    60000
  )
})

describe('Consumer > crash supervision > a recoverable answer to the loss recovery of an independent handler failure', () => {
  it.each([
    ['FENCED_MEMBER_EPOCH (110)', 110],
    ['UNKNOWN_MEMBER_ID (25)', 25],
  ])(
    '%s answering the recovery join: the handler failure ends the run in one CRASH and the loss left by that answer causes no rejoin',
    async (_, code) => {
      const policy = jest.fn(async () => false)
      const failure = Object.assign(new Error('handler failed'), { retriable: false })
      const join = deferred()
      let joins = 0
      let handled = 0
      const run = await start({
        retry: { initialRetryTime: 300, restartOnFailure: policy },
        fault: fenced,
        onZero: () => {
          joins++
          if (joins === 1) return join.promise
        },
        eachMessage: async (_payload, held) => {
          if (handled++ > 0) return
          await held.promise
          throw failure
        },
      })
      await advanceUntil(() => handled === 1)
      run.arm()
      await advanceUntil(() => run.faultAt() !== null)
      run.held.resolve()
      await advanceUntil(() => joins === 1, 5000)
      expect(joins).toBe(1)

      join.resolve(Promise.reject(protocolError(code)))
      await advanceUntil(() => run.crashes.length > 0, 20000)
      await jest.advanceTimersByTimeAsync(5000)

      expect(run.crashes).toHaveLength(1)
      expect(inCauseChain(run.crashes[0].error, failure)).toBe(true)
      expect(run.crashes[0].restart).toBe(false)
      expect(policy).toHaveBeenCalledTimes(1)
      expect(inCauseChain(policy.mock.calls[0][0], failure)).toBe(true)
      expect(joins).toBe(1)
      expect(handled).toBe(1)
      expect(run.log.filter(({ kind }) => kind === 'PARTITIONS_LOST')).toHaveLength(1)
      expect(run.cluster.connect).toHaveBeenCalledTimes(1)
    },
    60000
  )
})
