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
const NOT_COORDINATOR = 16
const GROUP = 'moved-group'
const HEARTBEAT = { groupId: GROUP, groupGenerationId: 1, memberId: 'member-1' }

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
 * with a rack each, node 1 at `listed`; FindCoordinator names node 1 at `at`. Node 1's first
 * address answers every Heartbeat with NOT_COORDINATOR, as a broker that lost the group does, and
 * its moved address accepts it.
 */
describe('Cluster > a coordinator answer that names a known node at a new address', () => {
  let seam, cluster, at, listed

  beforeEach(async () => {
    at = COORDINATOR
    listed = COORDINATOR
    seam = createScriptedCluster({ nodes: [SEED, COORDINATOR, MOVED] })
    seam.answer('Metadata', () => ({
      brokers: [
        { nodeId: SEED.id, host: SEED.host, port: SEED.port, rack: 'rack-0' },
        { nodeId: COORDINATOR.id, host: listed.host, port: listed.port, rack: 'rack-1' },
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
      () =>
        cluster.brokerPool.applyNodeEndpoints([
          { nodeId: COORDINATOR.id, host: MOVED.host, port: MOVED.port, rack: 'rack-1' },
        ]),
    ],
  ])('an answer requested before %s moved the node does not move it back', async (_, moveNode) => {
    const gate = seam.schedule.hold({ api: 'FindCoordinator' })
    try {
      const lookup = outcome(cluster.findGroupCoordinator({ groupId: GROUP }))
      await gate.arrived()
      await moveNode()
      const moved = cluster.brokerPool.brokers[COORDINATOR.id]
      expect(moved.connectionPool.host).toBe(MOVED.host)
      const built = jest.spyOn(cluster.connectionPoolBuilder, 'build')

      gate.release()
      const { value, error } = await lookup

      expect(error).toBeUndefined()
      expect(value).toBe(moved)
      expect(cluster.brokerPool.brokers[COORDINATOR.id]).toBe(moved)
      expect(moved.connectionPool.host).toBe(MOVED.host)
      expect(built).not.toHaveBeenCalled()
    } finally {
      letGo(gate)
    }
  })
})
