const { Kafka } = require('../../../index')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('./fixtures/fetchHarness')

/**
 * Waits a bounded number of event-loop turns for a fake transport observation.
 * @param {() => boolean} predicate condition to observe
 * @returns {Promise<void>} after the condition holds
 * @throws {Error} if the expected fake-wire event never arrives
 */
const until = async (predicate) => {
  for (let turn = 0; turn < 500; turn++) {
    if (predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  throw new Error('Expected fake transport event did not arrive')
}

it.each([false, true])(
  'surfaces the group size limit once with restart override %s',
  async (override) => {
    const active = new Set()
    const setTimer = global.setTimeout
    const clearTimer = global.clearTimeout
    const schedule = jest
      .spyOn(global, 'setTimeout')
      .mockImplementation((callback, ms, ...args) => {
        const timer = setTimer(() => {
          active.delete(timer)
          callback(...args)
        }, ms)
        active.add(timer)
        return timer
      })
    const cancel = jest.spyOn(global, 'clearTimeout').mockImplementation((timer) => {
      active.delete(timer)
      clearTimer(timer)
    })
    const seam = groupScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node', port: 9092 }] }),
      { batches: 0 }
    )
    seam.schedule.once({ api: 'JoinGroup' }, { errorCode: 81 })
    const restartOnFailure = jest.fn(async () => true)
    const consumer = new Kafka(seam.kafkaOptions()).consumer({
      groupId: 'full-group',
      ...(override ? { retry: { restartOnFailure } } : {}),
    })
    const crashes = []
    const remove = consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload))
    try {
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC })
      await consumer.run({ eachMessage: async () => {} })
      await until(() => crashes.length === 1)
      expect(crashes[0].error).toMatchObject({ code: 81, retriable: false })
      expect(crashes[0].restart).toBe(override)
      if (override) {
        await until(() => seam.requests().filter(({ api }) => api === 'SyncGroup').length === 1)
        expect(seam.requests().filter(({ api }) => api === 'JoinGroup')).toHaveLength(2)
        expect(restartOnFailure).toHaveBeenCalledTimes(1)
      } else {
        expect(active.size).toBe(0)
        schedule.mockRestore()
        cancel.mockRestore()
        jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick'] })
        await jest.advanceTimersByTimeAsync(10000)
        expect(seam.requests().filter(({ api }) => api === 'JoinGroup')).toHaveLength(1)
        expect(crashes).toHaveLength(1)
        expect(jest.getTimerCount()).toBe(0)
      }
    } finally {
      jest.useRealTimers()
      remove()
      try {
        await consumer.disconnect()
        seam.assertClean()
        expect(active.size).toBe(0)
      } finally {
        schedule.mockRestore()
        cancel.mockRestore()
        jest.useRealTimers()
      }
    }
  }
)
