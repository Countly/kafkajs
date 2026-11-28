const seq = require('../utils/seq')
const waitFor = require('../utils/waitFor')
const sleep = require('../utils/sleep')
const createFetcher = require('./fetcher')
const createWorkerQueue = require('./workerQueue')
const createWorker = require('./worker')
const { newLogger } = require('../../testHelpers')
const Batch = require('./batch')

describe('Fetcher', () => {
  let fetcher, fetch, handler, workerIds, workers, workerQueue, logger, fetcherAssignments

  beforeEach(() => {
    fetch = jest.fn(async (nodeId) => {
      await sleep(1)
      return seq(
        10,
        (index) =>
          new Batch('test-topic', 0, {
            partition: `${nodeId}${index}`,
            highWatermark: '100',
            messages: [],
          })
      )
    })

    handler = jest.fn(async () => {
      await sleep(1)
    })

    logger = newLogger()
    workerIds = seq(5)
    const workerAssignments = new Map()
    workers = workerIds.map((workerId) =>
      createWorker({ workerId, handler, partitionAssignments: workerAssignments, logger })
    )
    workerQueue = createWorkerQueue({ workers })
    fetcherAssignments = new Map()
    fetcher = createFetcher({
      nodeId: 0,
      fetch,
      workerQueue,
      logger,
      partitionAssignments: fetcherAssignments,
    })
  })

  afterEach(async () => {
    fetcher && (await fetcher.stop())
  })

  it('should fetch, but not push to workerQueue before exiting', async () => {
    fetcher.start()
    await fetcher.stop()

    expect(fetch).toHaveBeenCalledTimes(1)
    expect(fetch).toHaveBeenCalledWith(0)
    expect(handler).toHaveBeenCalledTimes(0)
  })

  describe('once stop() has resolved', () => {
    /** Lets `count` event-loop turns pass; setImmediate stays real under these fake timers. */
    const turns = async (count) => {
      for (let i = 0; i < count; i++) await new Promise((resolve) => setImmediate(resolve))
    }

    beforeEach(() => {
      jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] })
    })

    afterEach(() => {
      jest.useRealTimers()
    })

    it('makes no further fetch after a stop() that waited for the worker holding its round', async () => {
      let release
      handler.mockImplementation(() => new Promise((resolve) => (release = resolve)))
      const heldFetch = jest.fn(async () => [
        new Batch('test-topic', 0, { partition: '0', highWatermark: '100', messages: [] }),
      ])
      const held = createFetcher({
        nodeId: 2,
        fetch: heldFetch,
        workerQueue,
        logger,
        partitionAssignments: fetcherAssignments,
      })

      const started = held.start()
      await turns(5)
      expect(handler).toHaveBeenCalledTimes(1)
      let stopped = false
      const stopping = held.stop().then(() => (stopped = true))
      await turns(5)
      expect(stopped).toBe(false)

      release()
      await stopping
      await started
      await jest.advanceTimersByTimeAsync(60000)
      await turns(5)

      expect(heldFetch).toHaveBeenCalledTimes(1)
      expect(handler).toHaveBeenCalledTimes(1)
      expect(jest.getTimerCount()).toBe(0)
    })

    it('makes no further fetch after a stop() that cancelled a retry wait, and leaves no timer behind', async () => {
      const failingFetch = jest.fn(async () => {
        throw new Error('connection lost')
      })
      const retrying = createFetcher({
        nodeId: 3,
        fetch: failingFetch,
        retryDelay: () => 100,
        workerQueue,
        logger,
        partitionAssignments: fetcherAssignments,
      })

      const started = retrying.start()
      await turns(5)
      expect(failingFetch).toHaveBeenCalledTimes(1)
      expect(jest.getTimerCount()).toBe(1)

      await retrying.stop()
      await started
      expect(jest.getTimerCount()).toBe(0)
      await jest.advanceTimersByTimeAsync(60000)
      await turns(5)

      expect(failingFetch).toHaveBeenCalledTimes(1)
      expect(handler).not.toHaveBeenCalled()
    })
  })

  it('should utilize all workers', async () => {
    fetcher.start()

    await waitFor(() => handler.mock.calls.length > workerIds.length, { maxWait: 2000 })
    await fetcher.stop()

    const calledWorkerIds = handler.mock.calls.map(([, { workerId }]) => workerId)
    workerIds.forEach((workerId) => {
      expect(calledWorkerIds).toContain(workerId)
    })
  })

  it('yields to the event loop when a fetch round returns no batches', async () => {
    // Bounded on purpose: a loop that never yields would starve jest's own timeout, so the fetch
    // stops the fetcher itself after enough rounds and the assertion is on how many rounds ran
    // before a zero-delay timer got its turn.
    const SPIN_LIMIT = 1000
    const emptyFetch = jest.fn(async () => {
      if (emptyFetch.mock.calls.length >= SPIN_LIMIT) {
        spinningFetcher.stop()
      }
      return []
    })
    const spinningFetcher = createFetcher({
      nodeId: 1,
      fetch: emptyFetch,
      workerQueue,
      logger,
      partitionAssignments: fetcherAssignments,
    })

    const started = spinningFetcher.start()
    const roundsBeforeTimer = await new Promise((resolve) =>
      setTimeout(() => resolve(emptyFetch.mock.calls.length), 0)
    )
    await spinningFetcher.stop()
    await started

    expect(roundsBeforeTimer).toBeGreaterThan(0)
    expect(roundsBeforeTimer).toBeLessThan(SPIN_LIMIT)
    expect(handler).not.toHaveBeenCalled()
  })

  describe('handles partition reassignment', () => {
    let fetchers

    afterEach(async () => {
      fetchers && (await Promise.all(fetchers.map((f) => f.stop())))
    })

    it('should filter out batches currently processed by another fetcher', async () => {
      const batches = seq(2).map(
        (index) =>
          new Batch('test-topic', 0, {
            partition: index.toString(),
            highWatermark: '100',
            messages: [],
          })
      )
      const fetch = jest.fn(async (_) => {
        await sleep(1)
        const batch = batches.pop()

        if (!batch) {
          return []
        }

        const reassignedPartition = new Batch('test-topic', 0, {
          partition: 'reassigned',
          highWatermark: '100',
          messages: [],
        })
        return [reassignedPartition, batch]
      })
      const fetchers = seq(2).map((index) =>
        createFetcher({
          nodeId: index,
          fetch,
          workerQueue,
          logger,
          partitionAssignments: fetcherAssignments,
        })
      )

      fetchers.forEach((fetcher) => fetcher.start())

      await waitFor(() => handler.mock.calls.length >= 3, { maxWait: 2000 })
      await Promise.all(fetchers.map((fetcher) => fetcher.stop()))

      expect(handler).toHaveBeenCalledTimes(3)
    })

    it('lets another fetcher deliver a partition once the previous fetcher has handled it', async () => {
      const movedBatch = () =>
        new Batch('test-topic', 0, { partition: 'moved', highWatermark: '100', messages: [] })
      let deliveredByOldLeader = false
      const oldLeaderFetch = jest.fn(async () => {
        await sleep(1)
        if (deliveredByOldLeader) return []
        deliveredByOldLeader = true
        return [movedBatch()]
      })
      const newLeaderFetch = jest.fn(async () => {
        await sleep(1)
        return handler.mock.calls.length > 0 ? [movedBatch()] : []
      })
      fetchers = [oldLeaderFetch, newLeaderFetch].map((fetch, nodeId) =>
        createFetcher({
          nodeId,
          fetch,
          workerQueue,
          logger,
          partitionAssignments: fetcherAssignments,
        })
      )

      fetchers.forEach((fetcher) => fetcher.start())
      await waitFor(() => handler.mock.calls.length >= 2, { maxWait: 2000 })
      await Promise.all(fetchers.map((fetcher) => fetcher.stop()))

      expect(handler.mock.calls[0][0].partition).toBe('moved')
      expect(handler.mock.calls[1][0].partition).toBe('moved')
    })
  })
})
