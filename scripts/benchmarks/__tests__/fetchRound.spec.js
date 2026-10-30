const { FetchRoundDriver, measure, main } = require('../fetchRound')

test('a complete three-node round counts every required lookup and batch', async () => {
  const driver = new FetchRoundDriver({ partitions: 1000 })
  expect(await driver.round()).toMatchObject({
    lookups: 3,
    batches: 1000,
    requests: 3,
  })
  expect(await driver.round()).toMatchObject({
    lookups: 3,
    batches: 1000,
    requests: 3,
  })
})
test('data responses build the same number of batches as idle responses', async () => {
  expect(await new FetchRoundDriver({ partitions: 100, data: true }).round()).toMatchObject({
    lookups: 3,
    batches: 100,
    requests: 3,
  })
})
test('skipped broker work fails a control instead of reporting a fast round', async () => {
  const driver = new FetchRoundDriver({ partitions: 100 })
  driver.group.cluster.findBroker = async () => ({
    fetch: async () => ({ errorCode: 0, responses: [] }),
  })
  await expect(driver.round()).rejects.toThrow('Incomplete round work')
})

test('each node builds its own share of the partitions after exactly one lookup', async () => {
  for (const [partitions, share] of [
    [100, [34, 33, 33]],
    [1000, [334, 333, 333]],
    [5000, [1667, 1667, 1666]],
  ]) {
    const result = await new FetchRoundDriver({ partitions }).round()
    expect(result.batchesByNode).toEqual(share)
    expect(result.lookupsByNode).toEqual([1, 1, 1])
    expect(result.groupingNs).toBeGreaterThan(0)
    expect(result.buildNs).toBeGreaterThan(0)
    expect(result.responseNs).toBeGreaterThan(0)
  }
})
test('a node that builds another share fails even when the batch total matches', async () => {
  const driver = new FetchRoundDriver({ partitions: 100 })
  const fetch = driver.group.fetch.bind(driver.group)
  driver.group.fetch = async (nodeId) => {
    const batches = await fetch(nodeId)
    return nodeId === 0 ? batches.slice(1) : nodeId === 1 ? [...batches, batches[0]] : batches
  }
  await expect(driver.round()).rejects.toThrow('Incomplete round work')
})
test('close restores the original methods, also after a failed round', async () => {
  const driver = new FetchRoundDriver({ partitions: 100 })
  const { group } = driver
  const cluster = group.cluster
  const wrappedLookup = cluster.findTopicPartitionMetadata
  expect(Object.hasOwn(group, 'filterPartitionsByNode')).toBe(true)
  cluster.findBroker = async () => ({ fetch: async () => ({ errorCode: 0, responses: [] }) })
  await expect(driver.round()).rejects.toThrow('Incomplete round work')
  driver.close()
  driver.close()
  expect(Object.hasOwn(group, 'filterPartitionsByNode')).toBe(false)
  expect(Object.hasOwn(group, 'buildFetchRequests')).toBe(false)
  expect(cluster.findTopicPartitionMetadata).not.toBe(wrappedLookup)
  const lookups = driver.counts.lookups
  cluster.findTopicPartitionMetadata('round')
  expect(driver.counts.lookups).toBe(lookups)
})

describe('measure', () => {
  const entry = (startTime) => ({ startTime })
  const fakes = (entries, heap = [1000, 1000, 5000, 1200]) => {
    const observers = []
    class Observer {
      constructor() {
        this.disconnected = false
        observers.push(this)
      }

      observe() {}
      takeRecords() {
        return entries
      }

      disconnect() {
        this.disconnected = true
      }
    }
    const times = [100, 200]
    return {
      observers,
      env: {
        Observer,
        clock: { now: () => times.shift() },
        heapUsed: () => heap.shift(),
      },
    }
  }
  const stub = (round = async () => ({ lookups: 3, batches: 1000, requests: 3 })) => ({ round })
  let gc
  beforeEach(() => {
    gc = global.gc
    global.gc = jest.fn()
  })
  afterEach(() => {
    global.gc = gc
  })

  test('reports one complete round as allocation and the retained delta after collection', async () => {
    const { env, observers } = fakes([entry(50), entry(250)])
    const result = await measure(stub(), env)
    expect(result).toMatchObject({
      batches: 1000,
      allocatedBytesPerRound: 4000,
      retainedHeapDelta: 200,
    })
    expect(observers[0].disconnected).toBe(true)
  })
  test('a collection inside the window voids it even when delivered after the window ended', async () => {
    const { env } = fakes([entry(150)])
    await expect(measure(stub(), env)).rejects.toThrow('Void no-GC window')
  })
  test('a negative allocation voids the window', async () => {
    const { env } = fakes([], [1000, 1000, 900, 900])
    await expect(measure(stub(), env)).rejects.toThrow('Void no-GC window')
  })
  test('a failed sample disconnects its observer and leaves the driver usable', async () => {
    const { env, observers } = fakes([])
    let calls = 0
    const driver = stub(async () => {
      if (++calls === 2) throw new Error('boom')
      return { lookups: 3 }
    })
    await expect(measure(driver, env)).rejects.toThrow('boom')
    expect(observers[0].disconnected).toBe(true)
    await expect(driver.round()).resolves.toEqual({ lookups: 3 })
  })
  test('collection must be exposed', async () => {
    global.gc = undefined
    await expect(measure(stub())).rejects.toThrow('Use --expose-gc')
  })
})

describe('main', () => {
  test('control mode prints one JSON record of the counted work', async () => {
    const write = jest.spyOn(process.stdout, 'write').mockImplementation(() => true)
    try {
      await main(['--partitions', '100', '--control'])
      expect(JSON.parse(write.mock.calls[0][0])).toMatchObject({ lookups: 3, batches: 100 })
    } finally {
      write.mockRestore()
    }
  })
  test.each([
    [['--nope'], 'Unknown option'],
    [['--partitions'], 'Missing value'],
    [['--partitions', '7', '--control'], 'partitions must be'],
  ])('arguments %j are refused', async (args, message) => {
    await expect(main(args)).rejects.toThrow(message)
  })
})
