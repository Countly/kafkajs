const createFetcher = require('../fetcher')
const createFetchManager = require('../fetchManager')
const createWorker = require('../worker')
const createWorkerQueue = require('../workerQueue')
const Batch = require('../batch')
const OffsetManager = require('../offsetManager')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const seq = require('../../utils/seq')
const sleep = require('../../utils/sleep')
const { newLogger } = require('testHelpers')

const TOPIC = 'test-topic'

/**
 * A batch whose `lastOffset()` is deterministic: one message at `fetchedOffset`, so the next
 * offset is always `fetchedOffset + 1` and the override map is predictable.
 */
const batchAt = (partition, fetchedOffset) =>
  new Batch(TOPIC, `${fetchedOffset}`, {
    partition,
    highWatermark: '1000',
    messages: [{ offset: `${fetchedOffset}`, key: null, value: Buffer.alloc(0), headers: {} }],
  })

/**
 * Builds a fetcher over a real worker queue, recording every `fetch` call with the options it was
 * given, so both the call ORDER and the per-request override are observable.
 */
const buildFetcher = ({
  maxPrefetchBatches = 0,
  isFullyResolved,
  respond,
  handler = async () => {},
  concurrency = 1,
}) => {
  const calls = []
  const logger = newLogger()
  const workerAssignments = new Map()
  const workers = seq(concurrency, (workerId) =>
    createWorker({ workerId, handler, partitionAssignments: workerAssignments, logger })
  )
  const workerQueue = createWorkerQueue({ workers })

  const fetch = jest.fn(async (nodeId, options) => {
    const call = { nodeId, options, at: calls.length }
    calls.push(call)
    return respond(call)
  })

  const fetcher = createFetcher({
    nodeId: 0,
    fetch,
    workerQueue,
    logger,
    partitionAssignments: new Map(),
    maxPrefetchBatches,
    isFullyResolved,
  })

  return { fetcher, fetch, calls }
}

describe('Consumer > prefetch > maxPrefetchBatches 0 is the shipped loop', () => {
  it('never calls isFullyResolved, never reads lastOffset(), and never passes fetch options', async () => {
    const isFullyResolved = jest.fn(() => true)
    const batch = batchAt(0, 10)
    const lastOffset = jest.spyOn(batch, 'lastOffset')

    const { fetcher, calls } = buildFetcher({
      maxPrefetchBatches: 0,
      isFullyResolved,
      respond: ({ at }) => (at === 0 ? [batch] : []),
    })

    const started = fetcher.start()
    await sleep(50)
    await fetcher.stop()
    await started

    expect(isFullyResolved).not.toHaveBeenCalled()
    expect(lastOffset).not.toHaveBeenCalled()
    expect(calls.every(({ options }) => options === undefined)).toBe(true)
  })

  it('issues exactly one fetch per iteration, in the shipped order', async () => {
    // The call order IS the assertion: fetch, push, fetch, push ... with no overlap. A prefetch
    // armed at N=0 would show two consecutive `fetch` entries.
    const order = []
    const { fetcher } = buildFetcher({
      maxPrefetchBatches: 0,
      isFullyResolved: () => true,
      respond: ({ at }) => {
        order.push(`fetch:${at}`)
        return at < 3 ? [batchAt(0, at)] : []
      },
      handler: async (batch) => {
        order.push(`handle:${batch.fetchedOffset}`)
      },
    })

    const started = fetcher.start()
    await sleep(60)
    await fetcher.stop()
    await started

    expect(order.slice(0, 6)).toEqual([
      'fetch:0',
      'handle:0',
      'fetch:1',
      'handle:1',
      'fetch:2',
      'handle:2',
    ])
  })
})

describe('Consumer > prefetch > maxPrefetchBatches: 1', () => {
  it('never prefetches on the first iteration', async () => {
    const { fetcher, calls } = buildFetcher({
      maxPrefetchBatches: 1,
      isFullyResolved: () => true,
      respond: ({ at }) => (at === 0 ? [batchAt(0, 0)] : []),
      handler: async () => sleep(20),
    })

    const started = fetcher.start()
    // Inside the FIRST handler: `fullyResolved` is still null, so nothing may be in flight.
    await sleep(10)
    expect(calls).toHaveLength(1)

    await fetcher.stop()
    await started
  })

  it('arms the next fetch before push resolves, and the carried value is what the next iteration consumes', async () => {
    const order = []
    const { fetcher, calls } = buildFetcher({
      maxPrefetchBatches: 1,
      isFullyResolved: () => true,
      respond: ({ at, options }) => {
        order.push(`fetch:${at}:${options ? 'prefetch' : 'ordinary'}`)
        return at < 3 ? [batchAt(0, at)] : []
      },
      handler: async (batch) => {
        order.push(`handle:${batch.fetchedOffset}`)
        await sleep(10)
      },
    })

    const started = fetcher.start()
    await sleep(80)
    await fetcher.stop()
    await started

    // Iteration 1 is ordinary and arms nothing (no previous dispatch). Iteration 2 dispatches and
    // arms a prefetch DURING the handler; iteration 3 consumes it rather than issuing its own.
    expect(order.slice(0, 5)).toEqual([
      'fetch:0:ordinary',
      'handle:0',
      'fetch:1:ordinary',
      'handle:1',
      'fetch:2:prefetch',
    ])
    // Exactly one fetch per iteration, never two: the carried value is consumed, not discarded.
    expect(calls.filter(({ options }) => options && options.prefetch).length).toBeLessThanOrEqual(
      calls.length - 1
    )
  })

  it('keeps at most one fetch in flight', async () => {
    let inFlight = 0
    let peak = 0
    const { fetcher } = buildFetcher({
      maxPrefetchBatches: 1,
      isFullyResolved: () => true,
      respond: async ({ at }) => {
        inFlight += 1
        peak = Math.max(peak, inFlight)
        await sleep(5)
        inFlight -= 1
        return at < 6 ? [batchAt(0, at)] : []
      },
      handler: async () => sleep(10),
    })

    const started = fetcher.start()
    await sleep(120)
    await fetcher.stop()
    await started

    expect(peak).toBe(1)
  })

  it('dispatches all five batches of one response and arms exactly one prefetch', async () => {
    const { fetcher, calls } = buildFetcher({
      maxPrefetchBatches: 1,
      isFullyResolved: () => true,
      respond: ({ at }) => (at < 2 ? seq(5, (i) => batchAt(i, at)) : []),
      handler: async () => sleep(5),
      concurrency: 5,
    })

    const started = fetcher.start()
    await sleep(80)
    await fetcher.stop()
    await started

    const prefetches = calls.filter(({ options }) => options && options.prefetch)
    expect(prefetches).toHaveLength(1)
    // One override entry per partition of the dispatch it ran ahead of.
    expect([...prefetches[0].options.offsetOverrides.keys()].sort()).toEqual(
      seq(5, (i) => `${TOPIC}|${i}`).sort()
    )
  })

  it('reads eligibility from the previous dispatch: a handler that resolves nothing arms no further prefetch', async () => {
    const resolvesNothing = buildFetcher({
      maxPrefetchBatches: 1,
      isFullyResolved: () => false,
      respond: ({ at }) => (at < 4 ? [batchAt(0, at)] : []),
      handler: async () => sleep(5),
    })

    let started = resolvesNothing.fetcher.start()
    await sleep(80)
    await resolvesNothing.fetcher.stop()
    await started

    expect(resolvesNothing.calls.filter(({ options }) => options).length).toBe(0)

    const resolvesFully = buildFetcher({
      maxPrefetchBatches: 1,
      isFullyResolved: () => true,
      respond: ({ at }) => (at < 4 ? [batchAt(0, at)] : []),
      handler: async () => sleep(5),
    })

    started = resolvesFully.fetcher.start()
    await sleep(80)
    await resolvesFully.fetcher.stop()
    await started

    expect(resolvesFully.calls.filter(({ options }) => options).length).toBeGreaterThan(0)
  })

  it('builds the override from lastOffset() + 1 and holds no position anywhere', async () => {
    const { fetcher, calls } = buildFetcher({
      maxPrefetchBatches: 1,
      isFullyResolved: () => true,
      respond: ({ at }) => (at < 2 ? [batchAt(0, 40 + at)] : []),
      handler: async () => sleep(5),
    })

    const started = fetcher.start()
    await sleep(60)
    await fetcher.stop()
    await started

    const prefetch = calls.find(({ options }) => options && options.prefetch)
    // The batch at fetchedOffset 41 holds one message at 41, so the run-ahead offset is 42.
    expect(prefetch.options.offsetOverrides.get(`${TOPIC}|0`)).toBe('42')
    // The override travels as an argument and dies with the request: the fetcher exposes no
    // position accessor of any kind.
    expect(Object.keys(fetcher).sort()).toEqual(['getWorkerQueue', 'start', 'stop'])
  })

  it('adds no position field to the offset manager either', () => {
    const manager = new OffsetManager({
      cluster: { committedOffsets: () => ({}) },
      coordinator: { isConnected: () => true },
      memberAssignment: { [TOPIC]: [0] },
      autoCommit: false,
      autoCommitInterval: null,
      autoCommitThreshold: null,
      topicConfigurations: { [TOPIC]: {} },
      instrumentationEmitter: new InstrumentationEventEmitter(),
      groupId: 'prefetch-group',
      generationId: 1,
      memberId: 'member-1',
    })

    // Structural, and the whole soundness argument in one assertion: `resolvedOffsets` stays the
    // only persistent offset state, so nothing a handler does or does not resolve can move where
    // the next ORDINARY fetch reads from. A stored fetch position is what rev 1 cut.
    expect(Object.keys(manager).filter((key) => /position|prefetch|runAhead/i.test(key))).toEqual(
      []
    )
  })

  it('sinks a prefetch rejection instead of reporting it as unhandled', async () => {
    const unhandled = jest.fn()
    process.on('unhandledRejection', unhandled)

    const { fetcher } = buildFetcher({
      maxPrefetchBatches: 1,
      isFullyResolved: () => true,
      respond: async ({ at, options }) => {
        if (options && options.prefetch) {
          throw new Error('connection lost')
        }
        return at < 4 ? [batchAt(0, at)] : []
      },
      handler: async () => sleep(20),
    })

    const started = fetcher.start()
    await sleep(60)
    // The rejection is raised while `await pushed` is still running; the sink must already exist.
    await expect(started).rejects.toThrow('connection lost')
    await sleep(20)

    process.removeListener('unhandledRejection', unhandled)
    expect(unhandled).not.toHaveBeenCalled()
  })

  it('attaches the rejection sink in the turn the prefetch is created, not after the handler returns', async () => {
    // The case above cannot see WHERE the sink sits: Jest swallows a rejection raised inside a
    // mocked fetch, so `unhandledRejection` never reaches a listener a test registers. This one
    // watches the promise instead. `completed` counts finished handlers, so "the same turn" is
    // measurable without depending on when a worker picks a batch up: the sink attached before the
    // dispatch it ran ahead of finished, rather than one `await` later.
    let completed = 0
    let completedAtArm = null
    let completedAtAttach = null
    let at = 0

    const logger = newLogger()
    const workers = seq(1, (workerId) =>
      createWorker({
        workerId,
        handler: async () => {
          await sleep(20)
          completed += 1
        },
        logger,
      })
    )

    const recordAttach = () => {
      if (completedAtAttach === null) {
        completedAtAttach = completed
      }
    }

    const fetch = (nodeId, options) => {
      if (options && options.prefetch) {
        completedAtArm = completed
        const rejection = Promise.reject(new Error('connection lost'))
        const onRejected = rejection.catch.bind(rejection)
        const onSettled = rejection.then.bind(rejection)
        rejection.catch = (...args) => {
          recordAttach()
          return onRejected(...args)
        }
        rejection.then = (...args) => {
          recordAttach()
          return onSettled(...args)
        }
        return rejection
      }

      const batches = at < 4 ? [batchAt(0, at)] : []
      at += 1
      return Promise.resolve(batches)
    }

    const fetcher = createFetcher({
      nodeId: 0,
      fetch,
      workerQueue: createWorkerQueue({ workers }),
      logger,
      partitionAssignments: new Map(),
      maxPrefetchBatches: 1,
      isFullyResolved: () => true,
    })

    await expect(fetcher.start()).rejects.toThrow('connection lost')

    expect(completedAtArm).not.toBeNull()
    expect(completedAtAttach).toBe(completedAtArm)
  })

  it('drains the in-flight prefetch before end fires, on the stop exit', async () => {
    let settled = false
    const { fetcher } = buildFetcher({
      maxPrefetchBatches: 1,
      isFullyResolved: () => true,
      respond: async ({ at, options }) => {
        if (options && options.prefetch) {
          await sleep(40)
          settled = true
          return []
        }
        return at < 4 ? [batchAt(0, at)] : []
      },
      handler: async () => sleep(15),
    })

    const started = fetcher.start()
    // Timing matters and is the reason the drain is testable at all. t=0 ordinary fetch, handler
    // 0-15; t=15 second ordinary fetch, dispatch, prefetch armed (settles at ~t=55), handler
    // 15-30. Stopping at t=25 lands INSIDE that handler, with the prefetch still on the wire - so
    // the loop leaves through `while (isRunning)` with `pending` set, which is the drain. Stopping
    // at t=45 would instead find the loop parked at the head on `await pending`, and the case
    // would pass with the drain deleted.
    await sleep(25)
    await fetcher.stop()
    await started

    // `stop()` resolves on 'end', and 'end' may not fire while a Fetch is still on the wire.
    expect(settled).toBe(true)
  })

  it('drains the in-flight prefetch when the loop exits through the catch path', async () => {
    let settled = false
    const { fetcher } = buildFetcher({
      maxPrefetchBatches: 1,
      isFullyResolved: () => true,
      respond: async ({ at, options }) => {
        if (options && options.prefetch) {
          await sleep(40)
          settled = true
          return []
        }
        return at < 4 ? [batchAt(0, at)] : []
      },
      // The second dispatch's handler throws while the prefetch it armed is still in flight, so
      // the loop leaves through `catch` rather than through the `while` head.
      handler: async (batch) => {
        await sleep(5)
        if (batch.fetchedOffset === '1') {
          throw new Error('handler exploded')
        }
      },
    })

    await expect(fetcher.start()).rejects.toThrow('handler exploded')

    expect(settled).toBe(true)
  })
})

describe('Consumer > prefetch > the eligibility seam is injected, not derived', () => {
  it('forwards maxPrefetchBatches and isFullyResolved to every fetcher', async () => {
    const seen = []
    const isFullyResolved = jest.fn(() => true)
    const manager = createFetchManager({
      logger: newLogger(),
      getNodeIds: () => [1, 2],
      // Every fetcher must actually dispatch, or neither option can be observed: a
      // `fetch: async () => []` would leave `isFullyResolved` untouched and the case vacuous.
      fetch: async (nodeId, options) => {
        seen.push({ nodeId, prefetch: Boolean(options && options.prefetch) })
        return [batchAt(nodeId, seen.length)]
      },
      handler: async () => sleep(5),
      maxPrefetchBatches: 1,
      isFullyResolved,
    })

    const started = manager.start()
    await sleep(80)
    expect(manager.getFetchers()).toHaveLength(2)
    await manager.stop()
    await started

    // Both options reached both fetchers: each node armed at least one prefetch, and eligibility
    // was asked of the injected predicate rather than derived inside the fetch layer.
    expect(isFullyResolved).toHaveBeenCalled()
    for (const nodeId of [1, 2]) {
      expect(seen.some((call) => call.nodeId === nodeId && call.prefetch)).toBe(true)
    }
  })

  it('a fetcher built without isFullyResolved never prefetches', async () => {
    const { fetcher, calls } = buildFetcher({
      maxPrefetchBatches: 1,
      isFullyResolved: undefined,
      respond: ({ at }) => (at < 4 ? [batchAt(0, at)] : []),
      handler: async () => sleep(5),
    })

    const started = fetcher.start()
    await sleep(60)
    await fetcher.stop()
    await started

    expect(calls.filter(({ options }) => options).length).toBe(0)
  })
})
