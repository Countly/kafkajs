const createFetchManager = require('../fetchManager')
const { createLogger, LEVELS } = require('../../loggers')

const NODE_A = 1
const NODE_B = 2

/** @returns {Promise<void>} lets the continuations and the fetcher's `setImmediate` yield run */
const settle = async () => {
  for (let turn = 0; turn < 5; turn++) await new Promise(setImmediate)
}

describe('Kafka 4.4 client fix audit', () => {
  test('KAFKA-20780 classic fetch remains outstanding while processing another node', async () => {
    const calls = []
    const open = new Map([
      [NODE_A, []],
      [NODE_B, []],
    ])
    let releaseHandler = () => {}
    const handler = jest.fn(
      () =>
        new Promise((resolve) => {
          releaseHandler = resolve
        })
    )
    const manager = createFetchManager({
      logger: createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} }),
      getNodeIds: () => [NODE_A, NODE_B],
      fetch: (nodeId) => {
        calls.push(nodeId)
        return new Promise((resolve) => open.get(nodeId).push(resolve))
      },
      handler,
      concurrency: 1,
    })
    const count = (nodeId) => calls.filter((node) => node === nodeId).length
    const running = manager.start()
    running.catch(() => {})
    await settle()
    expect([count(NODE_A), count(NODE_B)]).toEqual([1, 1])

    // Node B answers and its handler runs; node A's long poll is neither cancelled nor repeated.
    for (let cycle = 1; cycle <= 3; cycle++) {
      open.get(NODE_B).shift()([{ topic: 't', partition: cycle }])
      await settle()
      expect(handler).toHaveBeenCalledTimes(cycle)
      expect(count(NODE_B)).toBe(cycle)
      expect(count(NODE_A)).toBe(1)
      expect(open.get(NODE_A)).toHaveLength(1)

      // Its next fetch starts as soon as the handler is done, with no timer in between.
      releaseHandler()
      await settle()
      expect(count(NODE_B)).toBe(cycle + 1)
      expect(count(NODE_A)).toBe(1)
      expect(open.get(NODE_A)).toHaveLength(1)
    }

    const stopping = manager.stop()
    for (let turn = 0; turn < 10; turn++) {
      for (const queue of open.values()) while (queue.length) queue.shift()([])
      await settle()
    }
    await stopping
    await running
    expect(count(NODE_A)).toBe(1)
  })
})
