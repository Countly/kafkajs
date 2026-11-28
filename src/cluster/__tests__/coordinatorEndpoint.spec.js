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
const BLOCKER = { id: 8, host: 'node8', port: 9092 }
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
 * Holds builds for one address and records every attempted destination.
 * @param {Cluster} cluster
 * @param {string} host
 * @returns {{ arrived: Promise<void>, release: () => void, hosts: string[] }}
 */
const holdBuild = (cluster, host) => {
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

/**
 * A real Cluster over scripted sockets, bootstrapped from node 0. Metadata lists node 0 and node 1
 * with a rack each, node 1 at `listed` (left out while `listed` is null); FindCoordinator names
 * node 1 at `at`. Node 1's first address answers every Heartbeat with NOT_COORDINATOR, as a broker
 * that lost the group does, and its moved address accepts it.
 */
describe('Cluster > a coordinator answer that names a known node at a new address', () => {
  let seam, cluster, at, listed, includeOther

  beforeEach(async () => {
    at = COORDINATOR
    listed = COORDINATOR
    includeOther = false
    seam = createScriptedCluster({ nodes: [SEED, COORDINATOR, MOVED, OTHER, BLOCKER] })
    seam.answer('Metadata', () => ({
      brokers: [
        ...(includeOther
          ? [{ nodeId: OTHER.id, host: OTHER.host, port: OTHER.port, rack: null }]
          : []),
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
    const held = holdBuild(cluster, OTHER.host)
    const bestEffort = cluster.refreshMetadataBestEffort.bind(cluster)
    let refused
    const retrying = new Promise((resolve) => (refused = resolve))
    jest.spyOn(cluster, 'refreshMetadataBestEffort').mockImplementation((fence) => {
      refused('refused')
      return bestEffort(fence)
    })
    try {
      const lookup = outcome(cluster.findGroupCoordinator({ groupId: GROUP }))
      await gate.arrived()
      listed = null
      includeOther = true
      const refreshing = cluster.refreshMetadata()
      await held.arrived
      expect(cluster.brokerPool.hasNode(COORDINATOR.id)).toBe(true)

      gate.release(coordinatorAnswer(COORDINATOR))
      expect(await Promise.race([retrying, lookup])).toBe('refused')
      held.release()
      await refreshing
      const { error } = await lookup

      expect(error).toMatchObject({
        name: 'KafkaJSNumberOfRetriesExceeded',
        retriable: false,
        cause: { name: 'KafkaJSBrokerNotFound', retriable: true },
      })
      expect(cluster.brokerPool.hasNode(COORDINATOR.id)).toBe(false)
      expect(held.hosts).toEqual([OTHER.host])
    } finally {
      letGo(gate)
      held.release()
    }
  })

  it('a failed pool removal fences a coordinator request already on the wire', async () => {
    const pool = cluster.brokerPool
    const seed = await cluster.findBroker({ nodeId: SEED.id })
    jest
      .spyOn(pool, 'withBroker')
      .mockImplementationOnce((callback) => callback({ nodeId: SEED.id, broker: seed }))
    const gate = seam.schedule.hold({ api: 'FindCoordinator' })
    try {
      const lookup = outcome(cluster.findGroupCoordinator({ groupId: GROUP }))
      await gate.arrived()
      pool.removeBroker(COORDINATOR)
      expect(pool.hasNode(COORDINATOR.id)).toBe(false)
      gate.release(coordinatorAnswer(MOVED))
      expect((await lookup).error).toMatchObject({ cause: { name: 'KafkaJSBrokerNotFound' } })
      at = MOVED
      const found = await cluster.findGroupCoordinator({ groupId: GROUP })
      await found.heartbeat(HEARTBEAT)
      expect(heartbeatNodes()).toEqual([MOVED.id])
    } finally {
      letGo(gate)
    }
  })

  it.each([
    ['an unchanged address', OTHER],
    ['a repeated omission', null],
  ])(
    'coordinator discovery terminates after a stale answer with %s in Metadata',
    async (_, row) => {
      cluster.retryOptions = RETRY_ONCE
      const first = seam.schedule.hold({ api: 'FindCoordinator' })
      const second = seam.schedule.hold({ api: 'FindCoordinator' })
      try {
        const lookup = outcome(cluster.findGroupCoordinator({ groupId: GROUP }))
        await first.arrived()
        listed = row
        await cluster.refreshMetadata()
        first.release(coordinatorAnswer(COORDINATOR))

        await second.arrived()
        await cluster.refreshMetadata()
        await cluster.refreshMetadata()
        second.release(coordinatorAnswer(MOVED))

        const { value, error } = await lookup
        expect(error).toBeUndefined()
        expect(seam.tally('FindCoordinator')).toBe(2)
        expect(value.connectionPool.host).toBe(MOVED.host)
        const setup = cluster.brokerPool.setupStates
        const address = `${MOVED.host}:${MOVED.port}`
        setup.failed(address, 0, setup.begin(address))
        await cluster.refreshMetadata()
        expect(setup.state(address, 0).failures).toBe(1)
        expect(cluster.brokerPool.brokers[COORDINATOR.id]).toBe(value)
        await value.heartbeat(HEARTBEAT)
        expect(heartbeatNodes()).toEqual([MOVED.id])
      } finally {
        letGo(first)
        letGo(second)
      }
    }
  )

  it('a queued FindCoordinator is ordered when its frame is written, with no retries', async () => {
    const pool = cluster.brokerPool
    const seed = await cluster.findBroker({ nodeId: SEED.id })
    const peer = await cluster.findBroker({ nodeId: COORDINATOR.id })
    jest
      .spyOn(pool, 'withBroker')
      .mockImplementationOnce((callback) => callback({ nodeId: SEED.id, broker: seed }))
    jest.spyOn(pool, 'findConnectedBroker').mockResolvedValueOnce(peer)
    const queue = seed.connectionPool.pool[0].requestQueue
    queue.maxInFlightRequests = 1
    const gate = seam.schedule.hold({ api: 'Heartbeat', node: SEED.id })
    const blocker = outcome(seed.heartbeat(HEARTBEAT))
    try {
      await gate.arrived()
      let enqueued
      const queued = new Promise((resolve) => (enqueued = resolve))
      const push = queue.push.bind(queue)
      jest.spyOn(queue, 'push').mockImplementationOnce((request) => {
        push(request)
        enqueued()
      })
      at = MOVED
      const lookup = outcome(cluster.findGroupCoordinator({ groupId: GROUP }))
      await queued
      expect(queue.pending).toHaveLength(1)
      expect(seam.tally('FindCoordinator')).toBe(0)
      listed = OTHER
      await cluster.refreshMetadata()
      expect(pool.brokers[COORDINATOR.id].connectionPool.host).toBe(OTHER.host)
      expect(seam.tally('FindCoordinator')).toBe(0)

      gate.release()
      expect((await blocker).error).toBeUndefined()
      const { value, error } = await lookup
      expect(error).toBeUndefined()
      expect(seam.tally('FindCoordinator')).toBe(1)
      expect(value.connectionPool.host).toBe(MOVED.host)
      await value.heartbeat(HEARTBEAT)
      expect(heartbeatNodes()).toEqual([SEED.id, MOVED.id])
    } finally {
      letGo(gate)
    }
  })

  it.each(['a newer hint', 'disconnect', 'rebootstrap'])(
    'a suspended leader hint cannot publish its later rows after %s',
    async (boundary) => {
      const pool = cluster.brokerPool
      cluster.metadataCache.update({
        brokers: [],
        topicMetadata: [
          {
            topicErrorCode: 0,
            topic: 'topic',
            partitionMetadata: [
              {
                partitionErrorCode: 0,
                partitionId: 0,
                leader: SEED.id,
                leaderEpoch: 0,
                replicas: [SEED.id],
                isr: [SEED.id],
              },
            ],
          },
        ],
      })
      const held = holdBuild(cluster, OTHER.host)
      const publishHint = jest.spyOn(cluster.metadataCache, 'applyLeaderHint')
      const hint = outcome(
        cluster.applyLeaderHint({
          topic: 'topic',
          partition: 0,
          leaderId: COORDINATOR.id,
          leaderEpoch: 1,
          nodeEndpoints: [
            { nodeId: OTHER.id, host: OTHER.host, port: OTHER.port },
            { nodeId: COORDINATOR.id, host: MOVED.host, port: MOVED.port },
          ],
        })
      )
      try {
        await held.arrived
        if (boundary === 'a newer hint') {
          await pool.applyNodeEndpoints([
            { nodeId: COORDINATOR.id, host: COORDINATOR.host, port: COORDINATOR.port },
          ])
        } else if (boundary === 'disconnect') {
          await cluster.disconnect()
        } else {
          await pool.rebootstrap('test')
        }
        held.release()
        expect((await hint).error).toBeUndefined()
        expect(held.hosts).not.toContain(MOVED.host)
        expect(pool.brokers[COORDINATOR.id]?.connectionPool.host).toBe(
          boundary === 'a newer hint' ? COORDINATOR.host : undefined
        )
        if (boundary !== 'a newer hint') expect(publishHint).not.toHaveBeenCalled()
      } finally {
        held.release()
      }
    }
  )

  it.each([
    ['Metadata', 'leader hint'],
    ['leader hint', 'Metadata'],
    ['Metadata', 'coordinator answer'],
    ['coordinator answer', 'Metadata'],
    ['Metadata', 'seed adoption'],
    ['seed adoption', 'Metadata'],
    ['leader hint', 'coordinator answer'],
    ['coordinator answer', 'leader hint'],
    ['leader hint', 'seed adoption'],
    ['seed adoption', 'leader hint'],
    ['coordinator answer', 'seed adoption'],
    ['seed adoption', 'coordinator answer'],
  ])('%s cannot overwrite a later %s for the same node', async (older, newer) => {
    const pool = cluster.brokerPool
    await pool.rebootstrap('test')
    const seed = pool.seedBroker
    const destinations = {
      Metadata: OTHER,
      'leader hint': MOVED,
      'coordinator answer': COORDINATOR,
      'seed adoption': SEED,
    }
    const gate = seam.schedule.hold({ api: 'FindCoordinator' })
    const held = holdBuild(cluster, BLOCKER.host)

    /**
     * Starts a real observation, optionally suspending before installing the target row.
     * @param {string} source
     * @param {boolean} suspend
     * @returns {Promise<unknown>}
     */
    const observe = (source, suspend) => {
      const { host, port } = destinations[source]
      const endpoint = { nodeId: SEED.id, host, port, rack: null }
      const blocked = { nodeId: BLOCKER.id, host: BLOCKER.host, port: BLOCKER.port, rack: null }
      const rows = suspend ? [blocked, endpoint] : [endpoint]
      if (source === 'leader hint') return pool.applyNodeEndpoints(rows)
      if (source === 'coordinator answer') return cluster.findGroupCoordinator({ groupId: GROUP })
      seam.answer('Metadata', () => ({ brokers: rows, controllerId: SEED.id, topics: [] }))
      return pool.refreshMetadataUnshared([])
    }

    try {
      const first = outcome(observe(older, true))
      await (older === 'coordinator answer' ? gate.arrived() : held.arrived)
      // The coordinator answer names the same logical target as Metadata and the hint.
      if (newer === 'coordinator answer') {
        const later = outcome(observe(newer, false))
        await gate.arrived()
        gate.release(coordinatorAnswer(destinations[newer], { nodeId: SEED.id }))
        expect((await later).error).toBeUndefined()
      } else {
        await observe(newer, false)
      }
      if (older === 'coordinator answer') {
        if (newer === 'leader hint') {
          seam.answer('Metadata', () => ({
            brokers: [{ ...destinations[newer], nodeId: SEED.id, rack: null }],
            controllerId: SEED.id,
            topics: [],
          }))
        }
        gate.release(coordinatorAnswer(destinations[older], { nodeId: SEED.id }))
      }
      held.release()
      const { error } = await first
      if (older === 'coordinator answer') {
        expect(error).toMatchObject({ cause: { name: 'KafkaJSBrokerNotFound' } })
      } else {
        expect(error).toBeUndefined()
      }
      const winner = pool.brokers[SEED.id]
      expect(winner.nodeId).toBe(SEED.id)
      expect(winner.connectionPool.host).toBe(destinations[newer].host)
      expect(winner === seed).toBe(newer === 'seed adoption')
      // No claim is left pending, and every node is installed where its newest claim points.
      for (const [nodeId, route] of pool.endpointOrder.routes) {
        const installed = pool.brokers[nodeId]?.connectionPool
        expect([nodeId, route.settled, installed && `${installed.host}:${installed.port}`]).toEqual(
          [nodeId, true, route.address]
        )
      }
      expect(Object.keys(pool.brokers).every((nodeId) => pool.endpointOrder.route(nodeId))).toBe(
        true
      )
    } finally {
      letGo(gate)
      held.release()
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

  /**
   * Holds the first pool build for each of `hosts` until its `release` is called; later builds and
   * other hosts are built at once.
   * @param {string[]} hosts
   * @returns {{ held: { host: string, release: () => void }[], reached: (count: number) => Promise<void> }}
   *   the held builds in call order, and a wait for the first `count` of them
   */
  const holdFirstBuilds = (hosts) => {
    const builder = cluster.connectionPoolBuilder
    const build = builder.build.bind(builder)
    const held = []
    const waits = []
    jest.spyOn(builder, 'build').mockImplementation(async (destination) => {
      const host = destination?.host
      if (hosts.includes(host) && !held.some((gate) => gate.host === host)) {
        const released = new Promise((release) => held.push({ host, release }))
        for (const wait of waits) if (held.length >= wait.count) wait.resolve()
        await released
      }
      return build(destination)
    })
    const reached = (count) =>
      held.length >= count
        ? Promise.resolve()
        : new Promise((resolve) => waits.push({ count, resolve }))
    return { held, reached }
  }

  it.each([
    ['after the Metadata writer lost to it', 'metadata first'],
    ['while the Metadata writer still builds', 'hint first'],
  ])(
    'a hint cancelled before it writes leaves the node to the Metadata response that names the same address, %s',
    async (_, release) => {
      const pool = cluster.brokerPool
      const { held, reached } = holdFirstBuilds([MOVED.host, OTHER.host])
      listed = MOVED
      const refreshing = cluster.refreshMetadata()
      await reached(1)
      const generation = new Generation()
      const hint = outcome(
        pool.applyNodeEndpoints(
          [
            { nodeId: OTHER.id, host: OTHER.host, port: OTHER.port, rack: null },
            { nodeId: COORDINATOR.id, host: MOVED.host, port: MOVED.port, rack: 'rack-1' },
          ],
          { fence: generation }
        )
      )
      await reached(2)
      const closed = new Error('the producer closed')
      if (release === 'metadata first') {
        held[0].release()
        await refreshing
        expect(pool.brokers[COORDINATOR.id].connectionPool.host).toBe(COORDINATOR.host)
        generation.close(closed)
        held[1].release()
        expect((await hint).error).toBe(closed)
        expect(pool.brokers[COORDINATOR.id].connectionPool.host).toBe(COORDINATOR.host)
        await cluster.refreshMetadata()
      } else {
        generation.close(closed)
        held[1].release()
        expect((await hint).error).toBe(closed)
        held[0].release()
        await refreshing
      }
      await cluster.refreshMetadata()

      const broker = await cluster.findBroker({ nodeId: COORDINATOR.id })
      expect(broker.connectionPool.host).toBe(MOVED.host)
      expect(pool.hasNode(OTHER.id)).toBe(false)
      await broker.heartbeat(HEARTBEAT)
      expect(heartbeatNodes()).toEqual([MOVED.id])
      expect(seam.tally('FindCoordinator')).toBe(0)
    }
  )

  it('departed node ids leave no routing state behind, and an answer held from before a departure still loses', async () => {
    const pool = cluster.brokerPool
    const seed = await cluster.findBroker({ nodeId: SEED.id })
    jest
      .spyOn(pool, 'withBroker')
      .mockImplementationOnce((callback) => callback({ nodeId: SEED.id, broker: seed }))
    const gate = seam.schedule.hold({ api: 'FindCoordinator' })
    try {
      const lookup = outcome(cluster.findGroupCoordinator({ groupId: GROUP }))
      await gate.arrived()
      for (let nodeId = 20; nodeId < 40; nodeId++) {
        seam.answer('Metadata', () => ({
          brokers: [
            { nodeId: SEED.id, host: SEED.host, port: SEED.port, rack: 'rack-0' },
            { nodeId, host: `rotated-${nodeId}`, port: 9092, rack: null },
          ],
          controllerId: SEED.id,
          topics: [],
        }))
        await cluster.refreshMetadata()
        expect(Object.keys(pool.brokers).sort()).toEqual([String(nodeId), String(SEED.id)].sort())
        expect(pool.endpointOrder.routes.size).toBe(2)
        expect(pool.metadataEndpointRows.size).toBe(2)
        expect(pool.setupMetadataEndpoints.size).toBe(2)
      }

      gate.release(coordinatorAnswer(COORDINATOR))
      const { error } = await lookup

      expect(error).toMatchObject({ cause: { name: 'KafkaJSBrokerNotFound' } })
      expect(pool.hasNode(COORDINATOR.id)).toBe(false)
      expect(pool.endpointOrder.routes.size).toBe(2)
      expect(pool.coordinatorNodes.size).toBe(0)
    } finally {
      letGo(gate)
    }
  })

  it.each([
    ['takes the node back to its listed address', COORDINATOR],
    ['removes the node it no longer lists', null],
  ])(
    'once its lookup names another node, a route a coordinator answer moved is retired: the next Metadata response %s',
    async (_, row) => {
      const pool = cluster.brokerPool
      at = MOVED
      const moved = await cluster.findGroupCoordinator({ groupId: GROUP })
      expect(moved.connectionPool.host).toBe(MOVED.host)
      listed = row
      await cluster.refreshMetadata()
      expect(pool.brokers[COORDINATOR.id]).toBe(moved)
      expect(moved.retired).toBe(false)

      seam.answer('FindCoordinator', ({ coordinatorKeys }) => ({
        coordinators: coordinatorKeys.map((key) => ({
          key,
          nodeId: SEED.id,
          host: SEED.host,
          port: SEED.port,
          errorCode: 0,
          errorMessage: null,
        })),
      }))
      const seed = await cluster.findGroupCoordinator({ groupId: GROUP })
      expect(seed.nodeId).toBe(SEED.id)
      expect(pool.coordinatorNodes.size).toBe(0)
      await cluster.refreshMetadata()

      expect(moved.retired).toBe(true)
      if (row) {
        expect(pool.brokers[COORDINATOR.id].connectionPool.host).toBe(COORDINATOR.host)
      } else {
        expect(pool.hasNode(COORDINATOR.id)).toBe(false)
        expect(pool.endpointOrder.route(COORDINATOR.id)).toBeUndefined()
      }
    }
  )

  it('an answer at the address the table holds does not stop a later Metadata response from changing the rack', async () => {
    const current = cluster.brokerPool.brokers[COORDINATOR.id]
    expect(current.connectionPool.rack).toBe('rack-1')
    expect(await cluster.findGroupCoordinator({ groupId: GROUP })).toBe(current)
    seam.answer('Metadata', () => ({
      brokers: [
        { nodeId: SEED.id, host: SEED.host, port: SEED.port, rack: 'rack-0' },
        { nodeId: COORDINATOR.id, host: COORDINATOR.host, port: COORDINATOR.port, rack: 'rack-2' },
      ],
      controllerId: SEED.id,
      topics: [],
    }))

    await cluster.refreshMetadata()
    expect(cluster.brokerPool.brokers[COORDINATOR.id]).toBe(current)
    expect(current.connectionPool.rack).toBe('rack-2')
    await cluster.refreshMetadata()
    expect(current.connectionPool.rack).toBe('rack-2')
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
  let seam, cluster, at, listed, listedRows

  beforeEach(async () => {
    at = SEED
    listed = SEED
    listedRows = undefined
    seam = createScriptedCluster({ nodes: [SEED, SEED_X, SEED_Y, BLOCKER] })
    seam.answer('Metadata', () => ({
      brokers: listedRows ?? [
        { nodeId: SEED.id, host: listed.host, port: listed.port, rack: null },
      ],
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

  /** @returns {number[]} the node of every Heartbeat written, in order */
  const heartbeatNodes = () =>
    seam
      .requests()
      .filter(({ api }) => api === 'Heartbeat')
      .map(({ node }) => node)

  it("a lookup sent after a Metadata response moved the seed keeps its newer address when that response's pool build finishes later", async () => {
    const held = holdBuild(cluster, SEED_X.host)
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
    const held = holdBuild(cluster, SEED_X.host)
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

      gate.release(coordinatorAnswer(SEED, { nodeId: SEED.id }))
      expect(await Promise.race([refused, lookup])).toBeUndefined()
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

  it.each([
    ['seed row first', [SEED, SEED_Y]],
    ['other row first', [SEED_Y, SEED]],
  ])(
    'keeps node identities exclusive when Metadata resumes after a seed move, %s',
    async (_, rows) => {
      const pool = cluster.brokerPool
      const held = holdBuild(cluster, BLOCKER.host)
      try {
        listedRows = [BLOCKER, ...rows].map(({ id: nodeId, host, port }) => ({
          nodeId,
          host,
          port,
          rack: null,
        }))
        const refreshing = cluster.refreshMetadata()
        await held.arrived
        at = SEED_Y
        const seed = await cluster.findGroupCoordinator({ groupId: GROUP })
        held.release()
        await refreshing

        const peer = await cluster.findBroker({ nodeId: SEED_Y.id })
        expect(seed).toBe(pool.seedBroker)
        expect(peer.nodeId).toBe(SEED_Y.id)
        expect(seed.nodeId).toBe(SEED.id)
        expect(peer.connectionPool).not.toBe(seed.connectionPool)
        await seed.heartbeat(HEARTBEAT)
        await peer.heartbeat(HEARTBEAT)
        expect(heartbeatNodes()).toEqual([SEED_Y.id, SEED_Y.id])

        await pool.applyNodeEndpoints([{ nodeId: SEED_Y.id, host: SEED_X.host, port: SEED_X.port }])
        const movedPeer = await cluster.findBroker({ nodeId: SEED_Y.id })
        await seed.heartbeat(HEARTBEAT)
        await movedPeer.heartbeat(HEARTBEAT)
        expect(heartbeatNodes()).toEqual([SEED_Y.id, SEED_Y.id, SEED_Y.id, SEED_X.id])
      } finally {
        held.release()
      }
    }
  )
})
