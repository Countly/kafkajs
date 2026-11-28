const Cluster = require('../index')
const Generation = require('../../producer/generation')
const { ConsumerOperation } = require('../../consumer/groupProtocol/interface')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { newLogger } = require('testHelpers')

const SEED = { id: 0, host: 'node0', port: 9092 }
const COORDINATOR = { id: 1, host: 'node1', port: 9092 }
/** Node 1's address after it moved; the scripted cluster calls this endpoint node 2. */
const MOVED = { id: 2, host: 'node1-moved', port: 9092 }
/** A node no Metadata response lists, which only a leader hint names. */
const OTHER = { id: 7, host: 'node7', port: 9092 }
const NOT_COORDINATOR = 16
const COORDINATOR_NOT_AVAILABLE = 15
const GROUP = 'moved-group'
const HEARTBEAT = { groupId: GROUP, groupGenerationId: 1, memberId: 'member-1' }
/** One more lookup after a refused one, at once. */
const RETRY_ONCE = { retries: 1, initialRetryTime: 1, maxRetryTime: 1 }

/**
 * The fields of a FindCoordinator answer for the test group, as a held call is released with them.
 * @param {{ host: string, port: number }} endpoint the address it names
 * @param {object} [options]
 * @param {number} [options.nodeId] the node it names; node 1 when omitted
 * @param {number} [options.errorCode=0]
 * @returns {object}
 */
const coordinatorAnswer = ({ host, port }, { nodeId = COORDINATOR.id, errorCode = 0 } = {}) => ({
  coordinators: [{ key: GROUP, nodeId, host, port, errorCode, errorMessage: null }],
})

/**
 * What a promise settled with, so a rejection is a value to look at.
 * @param {Promise<unknown>} promise
 * @returns {Promise<{ value?: unknown, error?: Error }>}
 */
const outcome = (promise) =>
  promise.then(
    (value) => ({ value }),
    (error) => ({ error })
  )

/**
 * Lets a held call go if it is still held, so cleanup never throws over a failed assertion.
 * @param {{ settle: object|null, release: () => void }} gate a schedule's hold
 * @returns {void}
 */
const letGo = (gate) => {
  if (gate.settle) gate.release()
}

/**
 * A real Cluster over scripted sockets, bootstrapped from node 0. Metadata lists node 0 and node 1
 * with a rack each, node 1 at `listed` (left out while `listed` is null); FindCoordinator names
 * node 1 at `at`. Node 1's first address answers every Heartbeat with NOT_COORDINATOR, as a broker
 * that lost the group does, and its moved address accepts it.
 */
describe('Cluster > a coordinator answer that names a known node at a new address', () => {
  let seam, cluster, at, listed

  beforeEach(async () => {
    at = COORDINATOR
    listed = COORDINATOR
    seam = createScriptedCluster({ nodes: [SEED, COORDINATOR, MOVED, OTHER] })
    seam.answer('Metadata', () => ({
      brokers: [
        { nodeId: SEED.id, host: SEED.host, port: SEED.port, rack: 'rack-0' },
        ...(listed
          ? [{ nodeId: COORDINATOR.id, host: listed.host, port: listed.port, rack: 'rack-1' }]
          : []),
      ],
      controllerId: SEED.id,
      topics: [],
    }))
    seam.answer('FindCoordinator', ({ coordinatorKeys }) => ({
      coordinators: coordinatorKeys.map((key) => ({
        key,
        nodeId: COORDINATOR.id,
        host: at.host,
        port: at.port,
        errorCode: 0,
        errorMessage: null,
      })),
    }))
    seam.answer('Heartbeat', (_fields, { node }) => ({
      errorCode: node === COORDINATOR.id ? NOT_COORDINATOR : 0,
    }))
    cluster = new Cluster({
      ...seam.kafkaOptions({ brokers: [`${SEED.host}:${SEED.port}`], requestTimeout: 1000 }),
      logger: newLogger(),
      retry: { retries: 0 },
    })
    await cluster.connect()
    await cluster.refreshMetadata()
  })

  afterEach(async () => {
    jest.restoreAllMocks()
    await cluster.disconnect()
    for (const socket of seam.sockets()) socket.destroy()
  })

  /** @returns {number[]} the node of every Heartbeat written, in order */
  const heartbeatNodes = () =>
    seam
      .requests()
      .filter(({ api }) => api === 'Heartbeat')
      .map(({ node }) => node)

  it.each([
    ['a group lookup', () => ({ groupId: GROUP })],
    [
      'a transaction lookup under a producer lifecycle',
      () => ({
        groupId: GROUP,
        coordinatorType: COORDINATOR_TYPES.TRANSACTION,
        fence: new Generation(),
      }),
    ],
    [
      'a share membership lookup under its operation',
      () => ({ groupId: GROUP, fence: new ConsumerOperation() }),
    ],
  ])(
    '%s reaches the new address without a Metadata request while the old one answers NOT_COORDINATOR',
    async (_, lookup) => {
      const old = await cluster.findGroupCoordinator(lookup())
      expect(old.connectionPool.host).toBe(COORDINATOR.host)
      at = MOVED
      await expect(old.heartbeat(HEARTBEAT)).rejects.toMatchObject({
        type: 'NOT_COORDINATOR_FOR_GROUP',
      })
      const metadataRequests = seam.tally('Metadata')

      const found = await cluster.findGroupCoordinator(lookup())

      expect(found.connectionPool.host).toBe(MOVED.host)
      await found.heartbeat(HEARTBEAT)
      expect(heartbeatNodes()).toEqual([COORDINATOR.id, MOVED.id])
      expect(seam.tally('Metadata')).toBe(metadataRequests)
      expect(cluster.brokerPool.brokers[COORDINATOR.id]).toBe(found)
      seam.assertClean()
    }
  )

  it('keeps the rack the broker table holds, as a FindCoordinator answer carries none', async () => {
    at = MOVED

    const found = await cluster.findGroupCoordinator({ groupId: GROUP })

    expect(found.connectionPool.host).toBe(MOVED.host)
    expect(found.connectionPool.rack).toBe('rack-1')
  })

  it('an answer that repeats the current address builds no pool and keeps the broker and its rack', async () => {
    const current = cluster.brokerPool.brokers[COORDINATOR.id]
    const built = jest.spyOn(cluster.connectionPoolBuilder, 'build')
    const retired = jest.spyOn(cluster.brokerPool, 'closeObsolete')

    const found = await cluster.findGroupCoordinator({ groupId: GROUP })

    expect(found).toBe(current)
    expect(found.connectionPool.host).toBe(COORDINATOR.host)
    expect(found.connectionPool.rack).toBe('rack-1')
    expect(built).not.toHaveBeenCalled()
    expect(retired).not.toHaveBeenCalled()
  })

  it('a request in flight on the replaced connection settles with its answer, and that connection closes', async () => {
    const old = await cluster.findGroupCoordinator({ groupId: GROUP })
    const gate = seam.schedule.hold({ api: 'Heartbeat', node: COORDINATOR.id })
    try {
      const held = outcome(old.heartbeat(HEARTBEAT))
      await gate.arrived()
      const oldSockets = seam
        .sockets()
        .filter(({ endpoint, destroyed }) => endpoint.host === COORDINATOR.host && !destroyed)
      expect(oldSockets.length).toBeGreaterThan(0)
      at = MOVED

      const found = await cluster.findGroupCoordinator({ groupId: GROUP })
      const drain = cluster.brokerPool.draining.get(old)

      expect(found).not.toBe(old)
      expect(old.retired).toBe(true)
      expect(cluster.brokerPool.draining.has(old)).toBe(true)
      gate.release()
      expect((await held).error).toMatchObject({ type: 'NOT_COORDINATOR_FOR_GROUP' })
      await drain
      expect(oldSockets.every(({ destroyed }) => destroyed)).toBe(true)
      expect(cluster.brokerPool.draining.has(old)).toBe(false)
    } finally {
      letGo(gate)
    }
  })

  it.each([
    [
      'a newer Metadata response',
      async () => {
        listed = MOVED
        await cluster.refreshMetadata()
      },
    ],
    [
      'a leader hint',
      () => {
        listed = MOVED
        return cluster.brokerPool.applyNodeEndpoints([
          { nodeId: COORDINATOR.id, host: MOVED.host, port: MOVED.port, rack: 'rack-1' },
        ])
      },
    ],
  ])(
    'an answer requested before %s moved the node does not move it back, and the lookup asks again',
    async (_, moveNode) => {
      cluster.retryOptions = RETRY_ONCE
      const gate = seam.schedule.hold({ api: 'FindCoordinator' })
      try {
        const lookup = outcome(cluster.findGroupCoordinator({ groupId: GROUP }))
        await gate.arrived()
        await moveNode()
        at = MOVED
        const moved = cluster.brokerPool.brokers[COORDINATOR.id]
        expect(moved.connectionPool.host).toBe(MOVED.host)
        const built = jest.spyOn(cluster.connectionPoolBuilder, 'build')
        const lookups = seam.tally('FindCoordinator')

        gate.release(coordinatorAnswer(COORDINATOR))
        const { value, error } = await lookup

        expect(error).toBeUndefined()
        expect(value).toBe(moved)
        expect(seam.tally('FindCoordinator')).toBe(lookups + 1)
        expect(cluster.brokerPool.brokers[COORDINATOR.id]).toBe(moved)
        expect(moved.connectionPool.host).toBe(MOVED.host)
        expect(built).not.toHaveBeenCalled()
      } finally {
        letGo(gate)
      }
    }
  )

  it('an answer requested before a Metadata response that no longer lists the node does not add it back', async () => {
    const gate = seam.schedule.hold({ api: 'FindCoordinator' })
    try {
      const lookup = outcome(cluster.findGroupCoordinator({ groupId: GROUP }))
      await gate.arrived()
      listed = null
      await cluster.refreshMetadata()
      expect(cluster.brokerPool.hasNode(COORDINATOR.id)).toBe(false)
      const built = jest.spyOn(cluster.connectionPoolBuilder, 'build')

      gate.release(coordinatorAnswer(COORDINATOR))
      const { error } = await lookup

      expect((error?.cause ?? error)?.name).toBe('KafkaJSBrokerNotFound')
      expect(cluster.brokerPool.hasNode(COORDINATOR.id)).toBe(false)
      expect(built).not.toHaveBeenCalled()
    } finally {
      letGo(gate)
    }
  })

  it('an answer requested before a leader hint moved another node still moves its own node', async () => {
    at = MOVED
    const gate = seam.schedule.hold({ api: 'FindCoordinator' })
    try {
      const lookup = outcome(cluster.findGroupCoordinator({ groupId: GROUP }))
      await gate.arrived()
      await cluster.brokerPool.applyNodeEndpoints([
        { nodeId: OTHER.id, host: OTHER.host, port: OTHER.port, rack: null },
      ])
      expect(cluster.brokerPool.brokers[OTHER.id].connectionPool.host).toBe(OTHER.host)

      gate.release()
      const { value, error } = await lookup

      expect(error).toBeUndefined()
      expect(value.connectionPool.host).toBe(MOVED.host)
      expect(cluster.brokerPool.brokers[COORDINATOR.id]).toBe(value)
    } finally {
      letGo(gate)
    }
  })

  it('an unchanged Metadata response published before the request that answers, after a refused attempt, does not keep the old address', async () => {
    cluster.retryOptions = RETRY_ONCE
    const gate = seam.schedule.hold({ api: 'FindCoordinator' })
    try {
      const lookup = outcome(cluster.findGroupCoordinator({ groupId: GROUP }))
      await gate.arrived()
      await cluster.refreshMetadata()
      expect(cluster.brokerPool.brokers[COORDINATOR.id].connectionPool.host).toBe(COORDINATOR.host)
      at = MOVED

      gate.release(
        coordinatorAnswer(
          { host: '', port: -1 },
          { nodeId: -1, errorCode: COORDINATOR_NOT_AVAILABLE }
        )
      )
      const { value, error } = await lookup

      expect(error).toBeUndefined()
      expect(seam.tally('FindCoordinator')).toBe(2)
      expect(value.connectionPool.host).toBe(MOVED.host)
      expect(cluster.brokerPool.brokers[COORDINATOR.id]).toBe(value)
    } finally {
      letGo(gate)
    }
  })
})

/**
 * A real Cluster over scripted sockets whose seed, node 0, is the only node and the coordinator:
 * Metadata lists node 0 at `listed` and FindCoordinator names it at `at`. The seed is adopted as
 * node 0, so every move of node 0 swaps the seed's pool in place.
 */
describe('Cluster > a coordinator answer and a Metadata response that both move the adopted seed', () => {
  const SEED_X = { id: 3, host: 'node0-x', port: 9092 }
  const SEED_Y = { id: 4, host: 'node0-y', port: 9092 }
  let seam, cluster, at, listed

  beforeEach(async () => {
    at = SEED
    listed = SEED
    seam = createScriptedCluster({ nodes: [SEED, SEED_X, SEED_Y] })
    seam.answer('Metadata', () => ({
      brokers: [{ nodeId: SEED.id, host: listed.host, port: listed.port, rack: null }],
      controllerId: SEED.id,
      topics: [],
    }))
    seam.answer('FindCoordinator', ({ coordinatorKeys }) => ({
      coordinators: coordinatorKeys.map((key) => ({
        key,
        nodeId: SEED.id,
        host: at.host,
        port: at.port,
        errorCode: 0,
        errorMessage: null,
      })),
    }))
    cluster = new Cluster({
      ...seam.kafkaOptions({ brokers: [`${SEED.host}:${SEED.port}`], requestTimeout: 1000 }),
      logger: newLogger(),
      retry: { retries: 0 },
    })
    await cluster.connect()
    await cluster.refreshMetadata()
    expect(cluster.brokerPool.brokers[SEED.id]).toBe(cluster.brokerPool.seedBroker)
  })

  afterEach(async () => {
    jest.restoreAllMocks()
    await cluster.disconnect()
    for (const socket of seam.sockets()) socket.destroy()
  })

  /**
   * Holds every pool build for `host` until `release()`, and records the host of every build.
   * @param {string} host
   * @returns {{ arrived: Promise<void>, release: () => void, hosts: string[] }}
   */
  const holdBuild = (host) => {
    const builder = cluster.connectionPoolBuilder
    const build = builder.build.bind(builder)
    const hosts = []
    let release, arrive
    const released = new Promise((resolve) => (release = resolve))
    const arrived = new Promise((resolve) => (arrive = resolve))
    jest.spyOn(builder, 'build').mockImplementation(async (destination) => {
      hosts.push(destination?.host)
      if (destination?.host === host) {
        arrive()
        await released
      }
      return build(destination)
    })
    return { arrived, release, hosts }
  }

  /** @returns {number[]} the node of every Heartbeat written, in order */
  const heartbeatNodes = () =>
    seam
      .requests()
      .filter(({ api }) => api === 'Heartbeat')
      .map(({ node }) => node)

  it("a lookup sent after a Metadata response moved the seed keeps its newer address when that response's pool build finishes later", async () => {
    const held = holdBuild(SEED_X.host)
    try {
      listed = SEED_X
      const refreshing = cluster.refreshMetadata()
      await held.arrived
      at = SEED_Y

      const found = await cluster.findGroupCoordinator({ groupId: GROUP })
      expect(found).toBe(cluster.brokerPool.seedBroker)
      expect(found.connectionPool.host).toBe(SEED_Y.host)
      held.release()
      await refreshing

      expect(cluster.brokerPool.brokers[SEED.id]).toBe(found)
      expect(found.connectionPool.host).toBe(SEED_Y.host)
      await found.heartbeat(HEARTBEAT)
      expect(heartbeatNodes()).toEqual([SEED_Y.id])
    } finally {
      held.release()
    }
  })

  it('an answer whose request was sent before a Metadata response moved the seed is not used while that response builds, and the lookup asks again', async () => {
    cluster.retryOptions = RETRY_ONCE
    const gate = seam.schedule.hold({ api: 'FindCoordinator' })
    const held = holdBuild(SEED_X.host)
    const bestEffort = cluster.refreshMetadataBestEffort.bind(cluster)
    let refuse
    const refused = new Promise((resolve) => (refuse = resolve))
    jest.spyOn(cluster, 'refreshMetadataBestEffort').mockImplementation((fence) => {
      refuse()
      return bestEffort(fence)
    })
    try {
      const lookup = outcome(cluster.findGroupCoordinator({ groupId: GROUP }))
      const hostWhenFound = lookup.then(({ value }) => value?.connectionPool.host)
      await gate.arrived()
      listed = SEED_X
      const refreshing = cluster.refreshMetadata()
      await held.arrived
      at = SEED_X

      gate.release(coordinatorAnswer(SEED_Y, { nodeId: SEED.id }))
      await Promise.race([refused, lookup])
      held.release()
      await refreshing
      const { value, error } = await lookup

      expect(error).toBeUndefined()
      expect(await hostWhenFound).toBe(SEED_X.host)
      expect(value).toBe(cluster.brokerPool.seedBroker)
      expect(value.connectionPool.host).toBe(SEED_X.host)
      expect(held.hosts).not.toContain(SEED_Y.host)
      expect(seam.tally('FindCoordinator')).toBe(2)
      await value.heartbeat(HEARTBEAT)
      expect(heartbeatNodes()).toEqual([SEED_X.id])
    } finally {
      letGo(gate)
      held.release()
    }
  })
})
