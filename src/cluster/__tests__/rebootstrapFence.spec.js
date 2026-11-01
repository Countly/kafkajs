const setupPool = require('../../../testHelpers/setupPool')
const Cluster = require('../index')
const Generation = require('../../producer/generation')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { CLUSTER_REBOOTSTRAP } = require('../instrumentationEvents')
const { KafkaJSConnectionError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const { deferred, watch, flushAll } = require('../../producer/__tests__/fixtures/bufferHarness')
const { newLogger } = require('testHelpers')

/** @param {string} host @returns {object} a connection pool that records its retirement */
const poolDouble = (host) =>
  setupPool({
    host,
    port: 9092,
    retire: jest.fn(),
    destroy: jest.fn(async () => {}),
  })

/**
 * A cluster whose seed fails every metadata request and whose next connection pools are the
 * doubles the test hands out through `builds`.
 * @param {Error} [error] the failure; REBOOTSTRAP_REQUIRED unless given
 * @returns {Promise<{ cluster: Cluster, pool: object, builds: object[], events: object[] }>}
 */
const rebootstrappingCluster = async (error = createErrorFromCode(129)) => {
  const instrumentationEmitter = new InstrumentationEventEmitter()
  const events = []
  instrumentationEmitter.addListener(CLUSTER_REBOOTSTRAP, (event) => events.push(event.payload))
  const cluster = new Cluster({
    logger: newLogger(),
    brokers: ['seed:9092'],
    retry: { retries: 0 },
    metadataRecoveryStrategy: 'rebootstrap',
    metadataRecoveryRebootstrapTriggerMs: 1000,
    metadataMaxAge: 1000,
    instrumentationEmitter,
  })
  await flushAll()
  const pool = cluster.brokerPool
  pool.seedBroker = {
    nodeId: 0,
    isConnected: () => true,
    disconnect: jest.fn(async () => {}),
    retire: jest.fn(async () => {}),
    connectionPool: { host: 'seed', port: 9092 },
    metadata: jest.fn(async () => {
      throw error
    }),
  }
  pool.brokers = {}
  const builds = []
  pool.buildConnectionPool = () => {
    const build = deferred()
    builds.push(build)
    return build.promise
  }
  return { cluster, pool, builds, events }
}

describe('rebootstrap and the producer lifecycle that asked for it', () => {
  test.each([
    { reopened: false, then: 'with no later seed' },
    { reopened: true, then: 'beside a later lifecycle seed' },
  ])('a rebootstrap pool built after the close is retired unused $then', async ({ reopened }) => {
    const { cluster, pool, builds } = await rebootstrappingCluster()
    const stalePool = poolDouble('stale')
    const freshPool = poolDouble('fresh')
    const generation = new Generation()
    const refreshing = watch(cluster.refreshMetadata({ fence: generation }))
    try {
      await flushAll()
      expect(builds).toHaveLength(1)
      generation.close(new Error('closed'))
      await cluster.disconnect({ drainTimeout: 0 })
      if (reopened) {
        const next = pool.createSeedBroker({ fence: new Generation() })
        builds[1].resolve(freshPool)
        await next
      }
      const currentSeed = pool.seedBroker
      builds[0].resolve(stalePool)
      await flushAll()
      expect(refreshing.state).toBe('rejected')
      expect(pool.seedBroker).toBe(currentSeed)
      expect(freshPool.retire).not.toHaveBeenCalled()
      expect(stalePool.retire).toHaveBeenCalledTimes(1)
      expect(stalePool.destroy).toHaveBeenCalledTimes(1)
    } finally {
      builds.forEach((build) => build.resolve(stalePool))
      await flushAll()
      await cluster.disconnect({ drainTimeout: 0 })
    }
  })

  test('a pool built for a timeout rebootstrap after the close is retired unused', async () => {
    const { cluster, pool, builds } = await rebootstrappingCluster(new Error('down'))
    const stalePool = poolDouble('stale')
    const generation = new Generation()
    pool.metadataUnavailableSince = Date.now() - 5000
    const seed = pool.seedBroker
    const refreshing = watch(cluster.refreshMetadata({ fence: generation }))
    try {
      await flushAll()
      expect(builds).toHaveLength(1)
      generation.close(new Error('closed'))
      builds[0].resolve(stalePool)
      await flushAll()
      expect(refreshing.state).toBe('rejected')
      expect(pool.seedBroker).toBe(seed)
      expect(stalePool.retire).toHaveBeenCalledTimes(1)
      expect(stalePool.destroy).toHaveBeenCalledTimes(1)
    } finally {
      builds.forEach((build) => build.resolve(stalePool))
      await flushAll()
      await cluster.disconnect({ drainTimeout: 0 })
    }
  })

  test('a pool built for an all-brokers-unavailable rebootstrap after the close is retired unused', async () => {
    const { cluster, pool, builds } = await rebootstrappingCluster()
    const stalePool = poolDouble('stale')
    const generation = new Generation()
    pool.brokers = {
      1: { isConnected: () => false, retire: jest.fn(async () => {}), disconnect: async () => {} },
    }
    pool.rebootstrapArmed = true
    pool.findBroker = async () => {
      throw new KafkaJSConnectionError('down')
    }
    const seed = pool.seedBroker
    const refreshing = watch(cluster.refreshMetadata({ fence: generation }))
    try {
      await flushAll()
      expect(builds).toHaveLength(1)
      generation.close(new Error('closed'))
      builds[0].resolve(stalePool)
      await flushAll()
      expect(refreshing.state).toBe('rejected')
      expect(pool.seedBroker).toBe(seed)
      expect(stalePool.retire).toHaveBeenCalledTimes(1)
      expect(stalePool.destroy).toHaveBeenCalledTimes(1)
    } finally {
      builds.forEach((build) => build.resolve(stalePool))
      await flushAll()
      await cluster.disconnect({ drainTimeout: 0 })
    }
  })

  test('a closed lifecycle starts no rebootstrap and clears no state', async () => {
    const { cluster, pool } = await rebootstrappingCluster()
    const build = jest.fn(async () => poolDouble('fresh'))
    pool.buildConnectionPool = build
    const generation = new Generation()
    const error = new Error('closed')
    generation.close(error)
    const metadata = { brokers: [], topicMetadata: [] }
    pool.metadata = metadata
    const seed = pool.seedBroker
    try {
      await expect(pool.rebootstrap('required', { fence: generation })).rejects.toBe(error)
      expect(build).not.toHaveBeenCalled()
      expect(pool.metadata).toBe(metadata)
      expect(pool.seedBroker).toBe(seed)
    } finally {
      await cluster.disconnect({ drainTimeout: 0 })
    }
  })

  test('a lifecycle that closes as the seed is installed publishes no recovery', async () => {
    const { cluster, pool, builds, events } = await rebootstrappingCluster()
    const generation = new Generation()
    const error = new Error('closed')
    const closeObsolete = pool.closeObsolete.bind(pool)
    pool.closeObsolete = (broker) => {
      generation.close(error)
      closeObsolete(broker)
    }
    try {
      const rebootstrapping = watch(pool.rebootstrap('required', { fence: generation }))
      await flushAll()
      builds[0].resolve(poolDouble('fresh'))
      await flushAll()
      expect(rebootstrapping.state).toBe('rejected')
      expect(rebootstrapping.value).toBe(error)
      expect(events).toEqual([])
      expect(pool.lastRebootstrapAt).toBeNull()
    } finally {
      await cluster.disconnect({ drainTimeout: 0 })
    }
  })

  test('callers of one lifecycle, or of none, join one rebootstrap', async () => {
    const { cluster, pool, builds } = await rebootstrappingCluster()
    const generation = new Generation()
    try {
      const joined = [
        pool.rebootstrap('required', { fence: generation }),
        pool.rebootstrap('required', { fence: generation }),
      ]
      await flushAll()
      expect(builds).toHaveLength(1)
      builds[0].resolve(poolDouble('fresh'))
      await Promise.all(joined)

      const unfenced = [pool.rebootstrap('required'), pool.rebootstrap('required')]
      await flushAll()
      expect(builds).toHaveLength(2)
      builds[1].resolve(poolDouble('fresh'))
      await Promise.all(unfenced)
    } finally {
      await cluster.disconnect({ drainTimeout: 0 })
    }
  })

  test('a caller of another lifecycle does not join the obsolete rebootstrap', async () => {
    const { cluster, pool, builds } = await rebootstrappingCluster()
    try {
      const first = watch(pool.rebootstrap('required', { fence: new Generation() }))
      const second = watch(pool.rebootstrap('required', { fence: new Generation() }))
      await flushAll()
      expect(builds).toHaveLength(2)
      builds.forEach((build) => build.resolve(poolDouble('fresh')))
      await flushAll()
      expect([first.state, second.state]).toEqual(['fulfilled', 'rejected'])
      expect(second.value).toMatchObject({ name: 'KafkaJSConnectionError' })
    } finally {
      await cluster.disconnect({ drainTimeout: 0 })
    }
  })
})
