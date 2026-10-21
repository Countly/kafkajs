const Cluster = require('../../../cluster')
const ConsumerGroup = require('../../../consumer/consumerGroup')
const { createScriptedCluster } = require('../../../../testHelpers/scriptedCluster')
const { createResourceProbe } = require('../../../../testHelpers/resourceProbe')
const { socketCounts, respondWithFrame } = require('../../../../testHelpers/fakeSocket')
const { responseHeaderVersion } = require('../../../protocol/headers')
const metadataPair = require('../../../../testHelpers/wire/metadataV13')
const { newLogger } = require('testHelpers')

const NODES = [
  { id: 0, host: 'seed', port: 9092 },
  { id: 1, host: 'old', port: 9093 },
  { id: 2, host: 'next', port: 9094 },
  { id: 3, host: 'last', port: 9095 },
]
const BOUND = 50

/**
 * Waits for observed state without advancing the drain clock.
 * @param {() => boolean} predicate required state
 * @returns {Promise<void>}
 * @throws {Error} after 1000 event-loop turns without the state
 */
const until = async (predicate) => {
  for (let turns = 0; turns < 1000; turns++) {
    if (predicate()) return
    await new Promise(setImmediate)
  }
  throw new Error('required network state was not observed')
}

/**
 * Preserves product settlement for ordinary assertions after cleanup boundaries.
 * @param {Promise<unknown>} work operation under observation
 * @returns {Promise<object>} exactly one value or error
 */
const outcome = (work) =>
  work.then(
    (value) => ({ value }),
    (error) => ({ error })
  )

/**
 * Creates the real client stack over the strict broker seam; no transport behavior is stubbed.
 * @param {object} [options] scenario configuration
 * @param {number} [options.target=1] broker to replace
 * @param {boolean} [options.enforceRequestTimeout=false] request enforcement policy
 * @param {string} [options.profile='default'] advertised seam profile
 * @param {number} [options.maxInFlightRequests=1] queue bound
 * @returns {Promise<object>} observed resources, replacement controls and cleanup
 * @throws {Error} when setup cannot connect or read the scripted metadata
 */
const createBoundHarness = async ({
  target = 1,
  enforceRequestTimeout = false,
  profile = 'default',
  maxInFlightRequests = 1,
} = {}) => {
  jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] })
  const seam = createScriptedCluster({ nodes: NODES, profile })
  const logger = newLogger()
  const cluster = new Cluster({
    ...seam.kafkaOptions({ requestTimeout: BOUND, enforceRequestTimeout, maxInFlightRequests }),
    logger,
    retry: { retries: 0 },
  })
  try {
    const probe = createResourceProbe({
      sockets: () => socketCounts(seam.fake.state),
      queueDepth: () =>
        connections.reduce(
          (n, c) => n + c.requestQueue.pending.length + c.requestQueue.inflight.size,
          0
        ),
    })
    const connections = []
    const baseline = await probe.snapshot()
    await cluster.connect()
    await cluster.refreshMetadata()
    const old = await cluster.findBroker({ nodeId: String(target) })
    connections.push(...old.connectionPool.pool)
    const connection = old.connectionPool.pool[0]
    const socket = connection.socket
    const queue = connection.requestQueue
    const queueProbe = createResourceProbe({ emitters: { queue } })
    const queueBaseline = await queueProbe.snapshot()
    const held = []
    /**
     * Wraps the consumed single-use Gate with an idempotent arrival-aware release for cleanup.
     * @param {object} selector request selection
     * @returns {object} arrived/release facade
     */
    const hold = (selector) => {
      const raw = seam.schedule.hold(selector)
      let released = false
      const arrival = raw.arrived()
      const gate = {
        arrived: () => arrival,
        release: (value) => {
          if (released) return
          released = true
          arrival.then(() => raw.release(value))
        },
      }
      held.push(gate)
      return gate
    }

    /**
     * Holds one transmitted request and one pending request; handlers attach immediately.
     * @returns {Promise<object>} both settlements and the held request gate
     * @throws {Error} if the expected in-flight/pending state is not observed
     */
    const overload = async () => {
      const gate = hold({ api: 'Metadata', node: target })
      const first = outcome(old.metadata([]))
      await until(() => seam.requests().some((r) => r.api === 'Metadata' && r.node === target))
      await gate.arrived()
      const second = outcome(old.metadata([]))
      await until(() => queue.inflight.size === 1 && queue.pending.length === 1)
      return { first, second, gate }
    }

    /**
     * Replaces the selected identity through a real lifecycle path without joining the drain.
     * A refresh reads topology from the healthy seed; its unanswered target cannot supply it.
     * @param {string} path seed, rebootstrap, refresh or hint
     * @param {object} [endpoint] replacement endpoint
     * @returns {Promise<void>} recovery completion
     * @throws {Error} for an unknown path or a real recovery failure
     */
    const replace = async (path, endpoint = NODES[2]) => {
      switch (path) {
        case 'seed':
          return cluster.brokerPool.createSeedBroker()
        case 'rebootstrap':
          return cluster.brokerPool.rebootstrap('required')
        case 'hint':
          return cluster.brokerPool.applyNodeEndpoints([{ nodeId: target, ...endpoint }])
        case 'refresh': {
          seam.answer('Metadata', () => ({
            brokers: NODES.filter((n) => n.id !== target && n.id < 2)
              .map((n) => ({ nodeId: n.id, host: n.host, port: n.port, rack: null }))
              .concat([{ nodeId: target, host: endpoint.host, port: endpoint.port, rack: null }]),
            controllerId: 0,
            topics: [],
          }))
          const random = jest.spyOn(Math, 'random').mockReturnValue(1 - Number.EPSILON)
          try {
            return await cluster.refreshMetadata()
          } finally {
            random.mockRestore()
          }
        }
        default:
          throw new Error(`unknown replacement path ${path}`)
      }
    }

    /**
     * Injects old-generation bytes after the seam itself would suppress their delivery.
     * @param {object} gate the previously transmitted request
     * @returns {Promise<void>}
     */
    const lateReply = async (gate) => {
      const record = await gate.arrived()
      respondWithFrame(
        { socket },
        record.correlationId,
        responseHeaderVersion(record.apiKey, record.version),
        metadataPair.buildResponse({ topics: [] })
      )
    }

    /**
     * Releases fixtures, disconnects clients, checks resources and forcibly closes pools in finally.
     * @param {object[]} [clients=[]] additional clients with a disconnect method
     * @returns {Promise<void>}
     * @throws {Error} for failed cleanup, a resource invariant or a recorded seam failure
     */
    const close = async (clients = []) => {
      for (const gate of held) gate.release()
      let done = false
      let clientResult
      const closing = outcome(Promise.all(clients.map((client) => client.disconnect()))).then(
        (result) => {
          done = true
          clientResult = result
        }
      )
      try {
        for (let turn = 0; turn < 100; turn++) {
          if (done) break
          await jest.advanceTimersByTimeAsync(BOUND + 1)
          await new Promise(setImmediate)
        }
        expect(done).toBe(true)
        await closing
        expect(clientResult.error).toBeUndefined()
        await jest.advanceTimersByTimeAsync(BOUND + 1)
        await cluster.disconnect()
        await old.disconnect({ drainTimeout: 0 })
        const after = await probe.snapshot()
        expect(after.queueDepth).toBe(baseline.queueDepth)
        expect(after.sockets.open).toBe(baseline.sockets.open)
        expect(await queueProbe.snapshot()).toEqual(queueBaseline)
        expect(jest.getTimerCount()).toBe(0)
        seam.assertClean()
      } finally {
        try {
          const pools = new Set([
            old.connectionPool,
            cluster.brokerPool.seedBroker?.connectionPool,
            ...Object.values(cluster.brokerPool.brokers).map((broker) => broker.connectionPool),
          ])
          await Promise.all(
            [...pools].filter(Boolean).map((pool) => pool.destroy({ drainTimeout: 0 }))
          )
        } finally {
          jest.useRealTimers()
        }
      }
    }

    return {
      seam,
      cluster,
      old,
      connection,
      socket,
      queue,
      queueProbe,
      queueBaseline,
      overload,
      replace,
      lateReply,
      close,
      held,
      hold,
      until,
    }
  } catch (error) {
    try {
      const pools = new Set([
        cluster.brokerPool.seedBroker?.connectionPool,
        ...Object.values(cluster.brokerPool.brokers).map((broker) => broker.connectionPool),
      ])
      await Promise.allSettled(
        [...pools].filter(Boolean).map((pool) => pool.destroy({ drainTimeout: 0 }))
      )
    } finally {
      jest.useRealTimers()
    }
    throw error
  }
}

/**
 * Builds the real group for a failure-removal reaction without starting a membership loop.
 * @param {Cluster} cluster client routing layer
 * @returns {ConsumerGroup} the group using the shared ledger
 */
const createRemovalGroup = (cluster) =>
  new ConsumerGroup({
    cluster,
    logger: newLogger(),
    groupId: 'removal-group',
    topics: [],
    topicConfigurations: {},
    assigners: [],
    retry: { retries: 0 },
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
    operations: cluster.offsetOperations({ groupId: 'removal-group' }),
  })

module.exports = { BOUND, NODES, until, outcome, createBoundHarness, createRemovalGroup }
