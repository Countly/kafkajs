const createFetchManager = require('../fetchManager')
const { newLogger, waitFor } = require('testHelpers')

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

describe('Consumer > FetchManager stop', () => {
  it('settles start() even while a rebalance error is re-creating the fetchers', async () => {
    // The pool keeps changing for the whole run, so the `while (true)` loop is genuinely still
    // re-creating fetchers when stop() lands. A single flip would stabilise long before the
    // sleep and the case would pass against the UNFIXED file, proving nothing.
    let generation = 0
    const manager = createFetchManager({
      logger: newLogger(),
      getNodeIds: () => (generation % 2 === 0 ? [1, 2] : [1, 2, 3]),
      fetch: async () => {
        generation += 1
        await sleep(5)
        return []
      },
      handler: async () => {},
    })

    const started = manager.start()
    await sleep(50)
    await manager.stop()

    await expect(started).resolves.toBeUndefined()
  })

  it('does not latch on the internal teardown, so a broker-pool change re-creates in place', async () => {
    let nodeIds = [1, 2]
    let rounds = 0
    const seen = []

    const manager = createFetchManager({
      logger: newLogger(),
      getNodeIds: () => {
        seen.push([...nodeIds])
        return nodeIds
      },
      fetch: async (nodeId) => {
        rounds += 1
        if (rounds === 1) {
          nodeIds = [1, 2, 3]
        }
        await sleep(5)
        return []
      },
      handler: async () => {},
    })

    const started = manager.start()
    // Wait for the KafkaJSFetcherRebalanceError to fire and the loop to re-create. The deadline
    // only bounds a regression: a manager that never re-creates times out instead of hanging.
    // The negative control a single shared latch would fail: fetching continued after the
    // internal teardown, with a fetcher per node of the NEW pool.
    await waitFor(() => manager.getFetchers().length === 3, {
      ignoreTimeout: false,
      maxWait: 5000,
      timeoutMessage: 'fetchers were not re-created for the new broker pool',
    })
    expect(manager.getFetchers()).toHaveLength(3)
    expect(seen.length).toBeGreaterThan(1)

    await manager.stop()
    await started
  })

  it('clears the latch on the next start()', async () => {
    // The pool changes only while `unstable` is true. That gives a deterministic case with a
    // genuine red: during the FIRST start() the pool keeps moving, so the `while (true)` loop is
    // still re-creating fetchers when stop() lands and only the latch can settle it; the SECOND
    // start() runs against a frozen pool, so exactly one fetcher exists and the assertion is
    // parity-independent.
    let unstable = true
    let generation = 0
    const manager = createFetchManager({
      logger: newLogger(),
      getNodeIds: () => (unstable && generation % 2 === 1 ? [1, 2] : [1]),
      fetch: async () => {
        generation += 1
        await sleep(5)
        return []
      },
      handler: async () => {},
    })

    const first = manager.start()
    await sleep(50)
    await manager.stop()
    await expect(first).resolves.toBeUndefined()

    unstable = false
    const second = manager.start()
    await sleep(20)
    expect(manager.getFetchers()).toHaveLength(1)
    await manager.stop()
    await expect(second).resolves.toBeUndefined()
  })

  it('propagates a non-rebalance error and still settles', async () => {
    const boom = new Error('boom')
    const manager = createFetchManager({
      logger: newLogger(),
      getNodeIds: () => [1],
      fetch: async () => {
        throw boom
      },
      handler: async () => {},
    })

    await expect(manager.start()).rejects.toBe(boom)
  })

  it('never propagates an error out of stop()', async () => {
    const manager = createFetchManager({
      logger: newLogger(),
      getNodeIds: () => [1],
      fetch: async () => {
        await sleep(5)
        return []
      },
      handler: async () => {},
    })

    const started = manager.start()
    await sleep(20)

    // A fetcher whose stop() rejects must not turn an un-awaited stop into an unhandled
    // rejection. The replacement STILL stops the real fetcher first: `fetcher.js:77-81` is what
    // lowers `isRunning` and emits 'end', and without it `Promise.all` in start() never settles
    // and this case hangs to the jest timeout.
    const fetcher = manager.getFetchers()[0]
    const realStop = fetcher.stop.bind(fetcher)
    fetcher.stop = async () => {
      await realStop()
      throw new Error('stop failed')
    }

    await expect(manager.stop()).resolves.toBeUndefined()
    await expect(started).resolves.toBeUndefined()
  })
})

describe('Consumer > the fetch layer follows a changed assignment without a restart', () => {
  // A hand-rolled ConsumerGroup stub cannot serve `group.fetch()`: the first round runs
  // `seekOffsets` -> `OffsetManager#resolveOffsets`, which calls `coordinator.offsetFetch(...)`,
  // and `buildFetchRequests`/`resolveTopicIds`/`validatePositions` then need `cluster.leaderEpoch`
  // and `cluster.topicId`. Use the harness five shipped suites already use.
  const {
    prepareRecordBatches,
    createFetchGroup,
    fetchResponse,
    TOPIC,
  } = require('./fixtures/fetchHarness')

  beforeAll(async () => {
    await prepareRecordBatches()
  })

  it('builds no request and returns [] for a node left with no partitions', async () => {
    // Two partitions on two different leaders, so one node can be emptied by an assignment change.
    const { group, findBrokerCalls } = await createFetchGroup({
      partitions: [0, 1],
      partitionLeaders: { 0: 0, 1: 1 },
    })

    const before = findBrokerCalls.length
    group.subscriptionState.assign([{ topic: TOPIC, partitions: [0] }])

    await expect(group.fetch(1)).resolves.toEqual([])
    // No request was built for node 1: it owns nothing now, so it never looked a broker up.
    expect(findBrokerCalls.length).toEqual(before)
  })

  it('keeps the same fetcher instances across an assignment change', async () => {
    // A single node owning both partitions: `findBroker` answers every node id with the same
    // fake broker/connection, so two node ids fetching concurrently would interleave their
    // frames on one shared write log with no way to tell which response belongs to which
    // request. One node keeps the round trip sequential and still exercises the claim - the
    // manager's fetcher identity survives an assignment change - without that harness limit.
    const { group, setResponse, fetchRequestPartitions } = await createFetchGroup({
      partitions: [0, 1],
    })

    // The canned response is built once from the group's full initial assignment; after the
    // assignment shrinks, the next Fetch frame asks for fewer partitions than that response
    // carries, so the reply must be rebuilt from what was actually just requested.
    setResponse(() =>
      fetchResponse(fetchRequestPartitions().map((partition) => ({ partition, errorCode: 0 })))
    )

    const manager = createFetchManager({
      logger: newLogger(),
      getNodeIds: () => [0],
      fetch: (nodeId) => group.fetch(nodeId),
      handler: async () => {},
    })

    const started = manager.start()
    await sleep(30)
    const before = manager.getFetchers()

    group.subscriptionState.assign([{ topic: TOPIC, partitions: [0] }])
    await sleep(30)

    const after = manager.getFetchers()
    expect(after).toHaveLength(before.length)
    after.forEach((fetcher, i) => expect(fetcher).toBe(before[i]))

    await manager.stop()
    await started
  })
})
