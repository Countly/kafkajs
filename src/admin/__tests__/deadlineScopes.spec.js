const { KafkaJSTimeout } = require('../../errors')
const {
  AdminDeadline,
  runWithDeadline,
  currentDeadline,
  retrierOf,
  waitForOf,
  withConsumerOperation,
  wrapAdmin,
} = require('../deadline')

const held = () => {
  let resolve
  const promise = new Promise((yes) => {
    resolve = yes
  })
  return { promise, resolve }
}
const flush = async () => {
  for (let n = 0; n < 30; n++) await Promise.resolve()
}
const observe = (promise) => {
  const result = { settled: false }
  promise.then(
    (value) => Object.assign(result, { settled: true, value }),
    (error) => Object.assign(result, { settled: true, error })
  )
  return result
}
const thrown = (fn) => {
  try {
    fn()
  } catch (error) {
    return error
  }
  throw new Error('expected the call to throw')
}
const lifecycleOf = () => ({ guard: () => null, register: () => () => {} })
const operationOf = (overrides = {}) => ({
  guard: jest.fn(() => null),
  check: jest.fn(),
  wait: jest.fn((work) => Promise.resolve(work())),
  register: jest.fn(() => () => {}),
  sleep: jest.fn(() => Promise.resolve()),
  retry: jest.fn((action) => action()),
  ...overrides,
})

describe('admin invocation scopes', () => {
  afterEach(() => {
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  describe('AdminDeadline', () => {
    test('a closed lifecycle refuses the invocation with its own error and clears the timer', () => {
      jest.useFakeTimers()
      const closed = new Error('admin closed')
      const deadline = new AdminDeadline('listTopics', 1000, { guard: () => closed })
      expect(jest.getTimerCount()).toBe(1)

      expect(deadline.expired).toBe(true)

      expect(deadline.refusal()).toBe(closed)
      expect(thrown(() => deadline.check())).toBe(closed)
      expect(jest.getTimerCount()).toBe(0)
    })
  })

  describe('retrierOf', () => {
    test('inside an invocation it retries a retriable failure under that invocation', async () => {
      const options = { retries: 2, initialRetryTime: 1, maxRetryTime: 1, factor: 0, multiplier: 1 }
      const failure = Object.assign(new Error('transient'), { retriable: true })
      const action = jest.fn().mockRejectedValueOnce(failure).mockResolvedValueOnce('done')

      const result = await runWithDeadline('probe', 1000, () => retrierOf(options)(action))

      expect(result).toBe('done')
      expect(action).toHaveBeenCalledTimes(2)
    })
  })

  describe('runWithDeadline', () => {
    test('a nested invocation inherits the outer deadline instead of starting its own', async () => {
      const seen = []

      const result = await runWithDeadline('outer', 1000, async () => {
        seen.push(currentDeadline())
        return runWithDeadline('inner', 1, async () => {
          seen.push(currentDeadline())
          return 'inner result'
        })
      })

      expect(result).toBe('inner result')
      expect(seen[0].method).toBe('outer')
      expect(seen[1]).toBe(seen[0])
    })

    test('a nested invocation inherits only under the same lifecycle', async () => {
      const first = lifecycleOf()
      const second = lifecycleOf()
      const seen = []

      await runWithDeadline(
        'outer',
        1000,
        async () => {
          seen.push(currentDeadline())
          await runWithDeadline('same', 1000, async () => seen.push(currentDeadline()), first)
          await runWithDeadline('other', 1000, async () => seen.push(currentDeadline()), second)
        },
        first
      )

      expect(seen[1]).toBe(seen[0])
      expect(seen[2]).not.toBe(seen[0])
      expect(seen[2].method).toBe('other')
    })
  })

  describe('withConsumerOperation', () => {
    test('routes the scope sleep and counted retry to the consumer operation', async () => {
      const operation = operationOf()
      const options = { retries: 3 }
      const action = jest.fn().mockResolvedValue('retried')

      const result = await withConsumerOperation(operation, async () => {
        await currentDeadline().delay(25)
        return retrierOf(options)(action)
      })

      expect(operation.sleep).toHaveBeenCalledWith(25)
      expect(operation.retry).toHaveBeenCalledWith(action, options)
      expect(result).toBe('retried')
    })
  })

  describe('waitForOf', () => {
    test('without an invocation it polls with the plain waiter', async () => {
      const predicate = jest.fn(async (totalWait) => totalWait >= 2 && 'ready')

      await expect(waitForOf(predicate, { delay: 1 })).resolves.toBe('ready')

      expect(predicate.mock.calls).toEqual([[1], [2]])
    })

    test('rejects with the failure the consumer operation raises while arming', async () => {
      const failure = new Error('consumer stopped')
      const operation = operationOf({
        check: jest.fn(() => {
          throw failure
        }),
      })
      const predicate = jest.fn()

      const error = await withConsumerOperation(operation, () =>
        waitForOf(predicate, { delay: 1 })
      ).then(
        () => null,
        (reason) => reason
      )

      expect(error).toBe(failure)
      expect(predicate).not.toHaveBeenCalled()
    })

    test('rejects with the timeout message without polling when the first poll is due only at maxWait', async () => {
      jest.useFakeTimers()
      const predicate = jest.fn()
      const result = observe(
        runWithDeadline('poll', 1000, () =>
          waitForOf(predicate, { delay: 5, maxWait: 5, timeoutMessage: 'poll not due' })
        )
      )

      await jest.advanceTimersByTimeAsync(5)
      await flush()

      expect(result.settled).toBe(true)
      expect(result.error).toBeInstanceOf(KafkaJSTimeout)
      expect(result.error.message).toBe('poll not due')
      expect(predicate).not.toHaveBeenCalled()
    })

    test('does not poll again when a predicate settles after the cap has rejected', async () => {
      jest.useFakeTimers()
      const pending = held()
      const gate = held()
      const predicate = jest.fn(() => pending.promise)
      let outcome
      const run = runWithDeadline('poll', 1000, async () => {
        outcome = await waitForOf(predicate, {
          delay: 1,
          maxWait: 5,
          timeoutMessage: 'poll gave up',
        }).then(
          () => null,
          (error) => error
        )
        await gate.promise
      })

      await jest.advanceTimersByTimeAsync(5)
      await flush()
      expect(outcome).toBeInstanceOf(KafkaJSTimeout)
      expect(outcome.message).toBe('poll gave up')
      pending.resolve(false)
      await jest.advanceTimersByTimeAsync(10)

      expect(predicate).toHaveBeenCalledTimes(1)
      gate.resolve()
      await run
    })
  })

  describe('wrapAdmin', () => {
    test('refuses a surface that lacks a declared operation', () => {
      expect(() => wrapAdmin({}, { listTopics: 0 }, 1000)).toThrow(
        'Missing admin operation listTopics'
      )
    })

    test('without a lifecycle it runs each call under its own budget', async () => {
      const methods = wrapAdmin(
        { probe: async () => currentDeadline().timeoutMs },
        { probe: 0 },
        1000
      )

      await expect(methods.probe({ apiTimeout: 250 })).resolves.toBe(250)
      await expect(methods.probe()).resolves.toBe(1000)
    })
  })
})
