const createFetchManager = require('../fetchManager')
const { createLogger, LEVELS } = require('../../loggers')
const { KafkaJSNoBrokerAvailableError } = require('../../errors')

/**
 * Lets the continuations and the fetcher's `setImmediate` yield of each round run.
 * @returns {Promise<void>}
 */
const settle = async () => {
  for (let turn = 0; turn < 5; turn++) await new Promise(setImmediate)
}

/**
 * @param {Promise<unknown>} promise
 * @returns {{ done: boolean }} whether the promise has settled, either way
 */
const track = (promise) => {
  const state = { done: false }
  const finish = () => {
    state.done = true
  }
  promise.then(finish, finish)
  return state
}

/**
 * A fetch manager over a node set the test changes and a fetch the test answers one call at a
 * time, so a long poll stays open for exactly as long as the test wants.
 */
class Rig {
  /**
   * @param {number[]} nodeIds the node ids the fake cluster advertises first
   * @param {(batch: object) => Promise<void>} [handler]
   */
  constructor(nodeIds, handler = async () => {}) {
    this.nodeIds = nodeIds
    /** @type {object[]} what the manager logged under its namespace */
    this.messages = []
    /** @type {number[]} the node of each fetch call, in call order */
    this.calls = []
    /** @type {Map<number, Array<{ resolve: Function, reject: Function }>>} */
    this.pending = new Map()
    /** @type {Map<number, number>} fetches open now, per node */
    this.open = new Map()
    /** @type {Map<number, number>} the most fetches ever open at once, per node */
    this.peak = new Map()
    /** @type {((nodeId: number) => void)|null} synchronous first-round instrumentation */
    this.onFetch = null
    /** @type {((log: object) => void)|null} synchronous log-creator instrumentation */
    this.onLog = null
    this.running = null
    const logger = createLogger({
      level: LEVELS.DEBUG,
      logCreator:
        () =>
        ({ namespace, log }) => {
          if (namespace === 'FetchManager') {
            this.messages.push(log)
            this.onLog?.(log)
          }
        },
    })
    this.manager = createFetchManager({
      logger,
      getNodeIds: () => [...this.nodeIds],
      fetch: (nodeId) => this.fetch(nodeId),
      handler,
      concurrency: 1,
    })
  }

  /**
   * The fake fetch: it stays open until `answer` or `fail`.
   * @param {number} nodeId
   * @returns {Promise<object[]>}
   */
  fetch(nodeId) {
    this.calls.push(nodeId)
    const open = (this.open.get(nodeId) ?? 0) + 1
    this.open.set(nodeId, open)
    this.peak.set(nodeId, Math.max(this.peak.get(nodeId) ?? 0, open))
    const pending = new Promise((resolve, reject) => {
      const queue = this.pending.get(nodeId) ?? []
      queue.push({ resolve, reject })
      this.pending.set(nodeId, queue)
    })
    this.onFetch?.(nodeId)
    return pending
  }

  /**
   * Starts the manager and lets its first round run.
   * @returns {Promise<void>}
   */
  async start() {
    this.running = this.manager.start()
    this.running.catch(() => {})
    await settle()
  }

  /**
   * Ends the oldest open fetch of a node with batches.
   * @param {number} nodeId
   * @param {object[]} [batches]
   * @returns {void}
   */
  answer(nodeId, batches = []) {
    this.open.set(nodeId, this.open.get(nodeId) - 1)
    this.pending.get(nodeId).shift().resolve(batches)
  }

  /**
   * Fails the oldest open fetch of a node.
   * @param {number} nodeId
   * @param {Error} error
   * @returns {void}
   */
  fail(nodeId, error) {
    this.open.set(nodeId, this.open.get(nodeId) - 1)
    this.pending.get(nodeId).shift().reject(error)
  }

  /**
   * @param {number} nodeId
   * @returns {number} how many fetches the node has made
   */
  count(nodeId) {
    return this.calls.filter((node) => node === nodeId).length
  }

  /**
   * @returns {Array<{ added: number[], removed: number[] }>} each reconciliation the manager logged
   */
  reconciliations() {
    return this.messages
      .filter(({ message }) => message === 'Reconciling fetchers')
      .map(({ added, removed }) => ({ added, removed }))
  }

  /**
   * @param {'Fetcher started'|'Fetcher stopped'} message
   * @returns {number[]} the nodes the manager logged it for, in order
   */
  logged(message) {
    return this.messages.filter((log) => log.message === message).map(({ nodeId }) => nodeId)
  }

  /**
   * Stops the manager, answering whatever is still open so that every fetcher can end.
   * @returns {Promise<void>}
   */
  async close() {
    const stopping = this.manager.stop()
    for (let turn = 0; turn < 10; turn++) {
      for (const [nodeId, queue] of this.pending) {
        while (queue.length) this.answer(nodeId)
      }
      await settle()
    }
    await stopping
    await this.running?.catch(() => {})
  }
}

describe('Consumer > FetchManager reconciliation', () => {
  let rig

  afterEach(async () => {
    await rig?.close()
    rig = null
  })

  it('keeps a surviving fetcher in its long poll and starts an added node at once', async () => {
    rig = new Rig([1, 2])
    await rig.start()
    expect(rig.logged('Fetcher started')).toEqual([1, 2])

    rig.nodeIds = [1, 2, 3]
    rig.answer(1)
    await settle()

    expect(rig.reconciliations()).toEqual([{ added: [3], removed: [] }])
    expect(rig.count(3)).toBe(1)
    expect(rig.count(2)).toBe(1)
    expect(rig.open.get(2)).toBe(1)
    expect(rig.manager.getFetchers()).toHaveLength(3)
    expect(rig.logged('Fetcher started')).toEqual([1, 2, 3])
  })

  it('stops the fetcher of a node that left on its own, while the others fetch on', async () => {
    rig = new Rig([1, 2, 3])
    await rig.start()

    rig.nodeIds = [1, 2]
    rig.answer(1)
    await settle()

    expect(rig.reconciliations()).toEqual([{ added: [], removed: [3] }])
    expect(rig.manager.getFetchers()).toHaveLength(2)
    expect(rig.count(1)).toBe(2)
    expect(rig.count(2)).toBe(1)
    expect(rig.open.get(3)).toBe(1)
    expect(rig.logged('Fetcher stopped')).toEqual([])

    rig.answer(3)
    await settle()

    expect(rig.count(3)).toBe(1)
    expect(rig.logged('Fetcher stopped')).toEqual([3])
  })

  it('stops a fetcher whose own round finds its node gone without another Fetch', async () => {
    rig = new Rig([1, 2])
    await rig.start()

    rig.nodeIds = [2]
    rig.answer(1)
    await settle()

    expect(rig.reconciliations()).toEqual([{ added: [], removed: [1] }])
    expect(rig.count(1)).toBe(1)
    expect(rig.logged('Fetcher stopped')).toEqual([1])
    expect(rig.manager.getFetchers()).toHaveLength(1)
    expect(rig.open.get(2)).toBe(1)
  })

  it('starts a node that returned only after its retiring fetcher has fully stopped', async () => {
    rig = new Rig([1, 2])
    await rig.start()

    rig.nodeIds = [1]
    rig.answer(1)
    await settle()
    rig.nodeIds = [1, 2]
    rig.answer(1)
    await settle()

    expect(rig.reconciliations()).toEqual([
      { added: [], removed: [2] },
      { added: [2], removed: [] },
    ])
    expect(rig.count(2)).toBe(1)
    expect(rig.manager.getFetchers()).toHaveLength(1)

    rig.answer(2)
    await settle()

    expect(rig.count(2)).toBe(2)
    expect(rig.peak.get(2)).toBe(1)
    expect(rig.manager.getFetchers()).toHaveLength(2)
    expect(rig.logged('Fetcher stopped')).toEqual([2])
  })

  it('reconciles once when several fetchers notice the same change', async () => {
    rig = new Rig([1, 2, 3])
    await rig.start()

    rig.nodeIds = [1, 2, 3, 4]
    rig.answer(1)
    rig.answer(2)
    rig.answer(3)
    await settle()

    expect(rig.reconciliations()).toEqual([{ added: [4], removed: [] }])
    expect(rig.count(4)).toBe(1)
    expect(rig.logged('Fetcher started')).toEqual([1, 2, 3, 4])
  })

  it('drains every fetcher on stop, one that is retiring included', async () => {
    rig = new Rig([1, 2, 3])
    await rig.start()
    const running = track(rig.running)
    rig.nodeIds = [1, 2]
    rig.answer(1)
    await settle()

    const stopping = track(rig.manager.stop())
    await settle()
    expect(stopping.done).toBe(false)

    rig.answer(1)
    rig.answer(2)
    await settle()
    expect(stopping.done).toBe(false)

    rig.answer(3)
    await settle()
    expect(stopping.done).toBe(true)
    expect(running.done).toBe(true)
    expect(rig.manager.getFetchers()).toEqual([])
  })

  it('starts no fetcher for a node that returns while the manager is stopping', async () => {
    rig = new Rig([1, 2])
    await rig.start()
    rig.nodeIds = [1]
    rig.answer(1)
    await settle()
    rig.nodeIds = [1, 2]

    const stopping = rig.manager.stop()
    rig.answer(1)
    rig.answer(2)
    await settle()
    await stopping

    expect(rig.count(2)).toBe(1)
    expect(rig.logged('Fetcher started')).toEqual([1, 2])
  })

  it('drains a synchronous stop in an added first round without starting the next node', async () => {
    rig = new Rig([1, 2])
    await rig.start()
    const running = track(rig.running)
    let stopping
    rig.onFetch = (nodeId) => {
      if (nodeId === 3) stopping = track(rig.manager.stop())
    }
    rig.nodeIds = [1, 2, 3, 4]
    rig.answer(1)
    await settle()

    expect(rig.logged('Fetcher started')).toEqual([1, 2, 3])
    expect(rig.count(1)).toBe(1)
    expect(rig.count(4)).toBe(0)
    expect(stopping.done).toBe(false)
    rig.answer(2)
    rig.answer(3)
    await settle()

    expect(stopping.done).toBe(true)
    expect(running.done).toBe(true)
    expect(rig.manager.getFetchers()).toEqual([])
  })

  it('drains a synchronous stop from the Fetcher started log', async () => {
    rig = new Rig([1, 2])
    let stopping
    rig.onLog = ({ message }) => {
      if (message === 'Fetcher started') stopping = track(rig.manager.stop())
    }
    await rig.start()
    const running = track(rig.running)

    expect(rig.logged('Fetcher started')).toEqual([1])
    expect(rig.calls).toEqual([1])
    rig.answer(1)
    await settle()

    expect(stopping.done).toBe(true)
    expect(running.done).toBe(true)
    expect(rig.manager.getFetchers()).toEqual([])
  })

  it('drains a synchronous stop in the initial first round', async () => {
    rig = new Rig([1, 2])
    let stopping
    rig.onFetch = () => {
      stopping = track(rig.manager.stop())
    }
    await rig.start()
    const running = track(rig.running)

    expect(rig.calls).toEqual([1])
    rig.answer(1)
    await settle()

    expect(stopping.done).toBe(true)
    expect(running.done).toBe(true)
    expect(rig.manager.getFetchers()).toEqual([])
  })

  it('drains a synchronous stop when release restarts a returned node', async () => {
    rig = new Rig([1, 2])
    await rig.start()
    rig.nodeIds = [1]
    rig.answer(1)
    await settle()
    rig.nodeIds = [1, 2]
    rig.answer(1)
    await settle()
    const running = track(rig.running)
    let stopping
    rig.onFetch = (nodeId) => {
      if (nodeId === 2) stopping = track(rig.manager.stop())
    }
    rig.answer(2)
    await settle()

    expect(rig.count(2)).toBe(2)
    expect(rig.peak.get(2)).toBe(1)
    rig.answer(1)
    rig.answer(2)
    await settle()

    expect(stopping.done).toBe(true)
    expect(running.done).toBe(true)
    expect(rig.manager.getFetchers()).toEqual([])
  })

  it('settles start after a synchronous stop from the Starting log', async () => {
    rig = new Rig([1, 2])
    let stopping
    rig.onLog = ({ message }) => {
      if (message === 'Starting...') stopping = track(rig.manager.stop())
    }
    await rig.start()
    const running = track(rig.running)
    await settle()

    expect(rig.calls).toEqual([])
    expect(stopping.done).toBe(true)
    expect(running.done).toBe(true)
    expect(rig.manager.getFetchers()).toEqual([])
  })

  it('rethrows a fetcher error after every other fetcher has stopped', async () => {
    rig = new Rig([1, 2])
    await rig.start()
    const running = track(rig.running)
    const boom = new Error('boom')

    rig.fail(1, boom)
    await settle()
    expect(running.done).toBe(false)

    rig.answer(2)
    await settle()
    await expect(rig.running).rejects.toBe(boom)
  })

  it('rejects at start when the cluster advertises no node', async () => {
    rig = new Rig([])

    await expect(rig.manager.start()).rejects.toThrow(new KafkaJSNoBrokerAvailableError())
  })

  it('does not treat an empty node list during a run as a change', async () => {
    rig = new Rig([1])
    await rig.start()

    rig.nodeIds = []
    rig.answer(1)
    await settle()

    expect(rig.reconciliations()).toEqual([])
    expect(rig.count(1)).toBe(2)
    expect(rig.manager.getFetchers()).toHaveLength(1)
  })

  it('keeps the partition ownership map across a reconciliation', async () => {
    let openGate
    const gate = new Promise((resolve) => {
      openGate = resolve
    })
    const handler = jest.fn(async (batch) => {
      if (batch.from === 1) await gate
    })
    rig = new Rig([1, 2], handler)
    await rig.start()

    rig.answer(1, [{ topic: 't', partition: 0, from: 1 }])
    await settle()
    expect(handler).toHaveBeenCalledTimes(1)

    rig.nodeIds = [1, 2, 3]
    rig.answer(2)
    await settle()
    expect(rig.count(3)).toBe(1)

    rig.answer(3, [{ topic: 't', partition: 0, from: 3 }])
    await settle()
    expect(rig.count(3)).toBe(2)
    expect(handler).toHaveBeenCalledTimes(1)

    openGate()
    await settle()
    rig.answer(3, [{ topic: 't', partition: 0, from: 3 }])
    await settle()
    expect(handler).toHaveBeenCalledTimes(2)
  })
})
