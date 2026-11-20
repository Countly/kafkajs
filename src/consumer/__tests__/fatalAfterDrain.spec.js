const { advanceUntil, deferred, settled, start, teardown } = require('./fixtures/scriptedConsumer')

beforeEach(() => jest.useFakeTimers())
afterEach(async () => {
  await teardown()
  jest.useRealTimers()
  jest.restoreAllMocks()
})

/** @returns {object} A close callback held on its gate, recording when it starts and ends. */
const heldHook = () => {
  const hook = { gate: deferred(), trace: [], calls: 0 }
  hook.callback = async () => {
    hook.calls++
    hook.trace.push('hook:start')
    await hook.gate.promise
    hook.trace.push('hook:end')
  }
  return hook
}

describe('a stop that drained its handler before the fatal deadline', () => {
  test.each([['disconnect'], ['stop']])(
    'lets its close callback run past the fatal deadline and reports no crash (%s)',
    async (api) => {
      const hook = heldHook()
      const order = []
      let entered = false
      const run = await start({
        hook: { onPartitionsLost: hook.callback, onPartitionsRevoked: hook.callback },
        eachMessage: async (_payload, held) => {
          entered = true
          await held.promise
        },
      })
      await advanceUntil(() => entered)
      run.arm()
      await advanceUntil(() => run.faultAt() !== null)
      const fatalAt = run.faultAt()
      const close = settled(run.consumer[api]())
      await jest.advanceTimersByTimeAsync(100)
      run.held.resolve()
      await advanceUntil(() => hook.calls === 1, 1000)

      await jest.advanceTimersByTimeAsync(fatalAt + 4000 - Date.now() + 50)
      expect(close.done).toBe(false)
      expect(run.crashes).toEqual([])
      expect(hook.trace).toEqual(['hook:start'])

      hook.gate.resolve()
      await advanceUntil(() => close.done, 2000)
      expect(close.error).toBeUndefined()
      const again = settled(
        run.consumer.run({
          eachMessage: async ({ partition, message }) =>
            order.push(`delivered:${partition}:${message.offset}`),
        })
      )
      await advanceUntil(() => again.done && order.length > 0, 2000)
      expect(order[0]).toMatch(/^delivered:0:/)
      expect(hook.trace).toEqual(['hook:start', 'hook:end'])
      expect(hook.calls).toBe(1)
      expect(run.crashes).toEqual([])
    }
  )
})
