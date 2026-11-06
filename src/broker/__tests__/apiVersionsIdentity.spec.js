const fs = require('fs')
const path = require('path')
const Broker = require('..')
const Cluster = require('../../cluster')
const ConnectionPool = require('../../network/connectionPool')
const Generation = require('../../producer/generation')
const createAdmin = require('../../admin')
const Encoder = require('../../protocol/encoder')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { CLUSTER_REBOOTSTRAP } = require('../../cluster/instrumentationEvents')
const { CLIENT_SOFTWARE_NAME, clientSoftwareVersion } = require('../../protocol/clientSoftware')
const { createLogger } = require('../../loggers')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { newLogger, connectionOpts, readRequestHeader } = require('testHelpers')

const logger = createLogger({ level: 0, logCreator: () => () => {} })
const VECTORS = path.join(__dirname, '..', '..', 'protocol', 'requests', 'apiVersions', 'fixtures')

/** @returns {object} an Apache 4.4.0 vector: the bytes Java wrote and the fields it wrote them from */
const vector = (name) =>
  JSON.parse(fs.readFileSync(path.join(VECTORS, 'java', `${name}.json`), 'utf8'))

/**
 * What a Kafka 4.4 broker answers when the cluster or node a request names is not its own, as Java
 * writes it: no APIs and no features. The throttle time is cleared so the client does not slow down.
 */
const REBOOTSTRAP_REQUIRED = {
  ...vector('apiVersionsResponse-v5-rebootstrapRequired').fields,
  throttleTime: 0,
}

const NODES = [
  { id: 1, host: 'node1', port: 9092 },
  { id: 2, host: 'node2', port: 9092 },
  { id: 3, host: 'node3', port: 9092 },
]
const RETRY = { retries: 0, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }

/** A cluster of brokers that offer ApiVersions v5 (Kafka 4.4), and one that offers up to v4. */
const NEW_BROKER = { advertise: { ApiVersions: [5, 5] } }
const OLD_BROKER = {}

/** @returns {object} the Metadata answer of `clusterId` listing `nodes` */
const metadataOf = (clusterId, nodes) => ({
  clusterId,
  brokers: nodes.map(({ id, host, port }) => ({ nodeId: id, host, port, rack: null })),
  controllerId: nodes[0].id,
  topics: [],
})

/** Polls until `predicate` holds, and fails naming `what` when it never does. */
const until = async (predicate, what) => {
  for (let turn = 0; turn < 400 && !predicate(); turn++) {
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  if (!predicate()) throw new Error(`${what} did not happen`)
}

/** @returns {Buffer} the body of the last ApiVersions request the client wrote, after its header */
const lastApiVersionsBody = (wire) => {
  const frame = wire.fake.state.writes
    .filter((bytes) => readRequestHeader(bytes).apiKey === 18)
    .at(-1)
  return frame.subarray(readRequestHeader(frame).byteLength)
}

describe('ApiVersions v5 identity (KIP-1242)', () => {
  const wires = []
  const closers = []

  /**
   * A real `Cluster` over a scripted cluster whose Metadata names `state.clusterId` and
   * `state.nodes`, both of which a test may change; REBOOTSTRAP events land in `events`.
   */
  const open = ({ strategy, profile = NEW_BROKER } = {}) => {
    const state = { clusterId: 'cluster-A', nodes: NODES.slice(0, 2) }
    const wire = createScriptedCluster({ nodes: NODES, ...profile })
    wire.answer('Metadata', () => metadataOf(state.clusterId, state.nodes))
    const emitter = new InstrumentationEventEmitter()
    const events = []
    emitter.addListener(CLUSTER_REBOOTSTRAP, ({ payload }) => events.push(payload))
    const cluster = new Cluster({
      ...connectionOpts(),
      brokers: ['node1:9092'],
      logger: newLogger(),
      socketFactory: wire.socketFactory,
      instrumentationEmitter: emitter,
      metadataRecoveryStrategy: strategy,
      retry: { retries: 0 },
      enforceRequestTimeout: false,
    })
    wires.push(wire)
    closers.push(() => cluster.disconnect({ drainTimeout: 0 }))

    return { wire, cluster, events, state }
  }

  /** A real `Broker` over a real pool of a scripted cluster, with no pool owning it. */
  const standalone = ({ identity, strategy, profile = NEW_BROKER } = {}) => {
    const wire = createScriptedCluster({ nodes: NODES, ...profile })
    const pool = new ConnectionPool({
      host: 'node2',
      port: 9092,
      logger,
      socketFactory: wire.socketFactory,
      connectionTimeout: 2000,
      authenticationTimeout: 400,
      requestTimeout: 5000,
      enforceRequestTimeout: false,
    })
    const broker = new Broker({
      connectionPool: pool,
      logger,
      ...(identity && { metadataIdentity: () => identity }),
      ...(strategy && { metadataRecoveryStrategy: strategy }),
    })
    wires.push(wire)
    closers.push(() => broker.disconnect({ drainTimeout: 0 }))

    return { wire, broker }
  }

  /** A public admin over the cluster, as `kafka.admin()` builds it, connected. */
  const adminOf = async (cluster, options = {}) => {
    const admin = createAdmin({ logger: newLogger(), cluster, retry: RETRY, ...options })
    closers.unshift(() => admin.disconnect())
    await admin.connect()

    return admin
  }

  afterEach(async () => {
    await Promise.all(closers.splice(0).map((close) => close()))
    for (const wire of wires.splice(0)) {
      wire.assertClean()
      await until(
        () => wire.sockets().every((socket) => socket.destroyed),
        'every socket being closed'
      )
    }
  })

  test('known identities are paired in ApiVersions v5', async () => {
    const { wire, cluster } = open()
    await cluster.connect()
    await cluster.refreshMetadata()
    await cluster.findBroker({ nodeId: 2 })

    const [seed, known] = wire.apiVersionTrace()
    expect(seed).toMatchObject({ node: 1, version: 5, fields: { clusterId: null, nodeId: -1 } })
    expect(known).toMatchObject({ node: 2, version: 5 })
    expect(known.fields).toEqual({
      clientSoftwareName: CLIENT_SOFTWARE_NAME,
      clientSoftwareVersion: clientSoftwareVersion(),
      clusterId: 'cluster-A',
      nodeId: 2,
    })
    // The same, read off the bytes: the cluster id as a compact string, the node as an int32, then
    // the empty tag buffer.
    expect(lastApiVersionsBody(wire).toString('hex').slice(-30)).toBe(
      '0a636c75737465722d41' + '00000002' + '00'
    )

    // The seed, which was built before any metadata was known, names its identity once it is.
    const adopted = cluster.brokerPool.brokers[1]
    expect(adopted).toBe(cluster.brokerPool.seedBroker)
    await adopted.disconnect()
    await cluster.findBroker({ nodeId: 1 })
    expect(wire.apiVersionTrace().at(-1)).toMatchObject({
      node: 1,
      version: 5,
      fields: { clusterId: 'cluster-A', nodeId: 1 },
    })
  })

  test.each([
    ['a fresh seed that knows nothing', undefined],
    ['a cluster id and no node', { clusterId: 'cluster-A', nodeId: -1 }],
    ['a cluster id and a node that is not set', { clusterId: 'cluster-A', nodeId: null }],
    ['a cluster id and a negative node', { clusterId: 'cluster-A', nodeId: -4 }],
    ['a node and no cluster id', { clusterId: null, nodeId: 2 }],
  ])('seed and half-known identities serialize defaults: %s', async (_what, identity) => {
    const { wire, broker } = standalone({ identity })

    await broker.connect()

    const [request] = wire.apiVersionTrace()
    expect(request).toMatchObject({ version: 5, fields: { clusterId: null, nodeId: -1 } })
    // A null cluster id (00), the default node (-1), the empty tag buffer.
    expect(lastApiVersionsBody(wire).toString('hex').slice(-12)).toBe('00' + 'ffffffff' + '00')
  })

  test('identity is refreshed after pool replacement', async () => {
    const { wire, cluster, state } = open()
    await cluster.connect()
    await cluster.refreshMetadata()
    await cluster.findBroker({ nodeId: 2 })
    const seen = wire.apiVersionTrace().length

    // The same addresses now answer for another cluster, and what was node 2 is node 3.
    state.clusterId = 'cluster-B'
    state.nodes = [NODES[0], { id: 3, host: 'node2', port: 9092 }]
    await cluster.refreshMetadata()
    await cluster.brokerPool.brokers[1].disconnect()
    await cluster.findBroker({ nodeId: 1 })
    await cluster.findBroker({ nodeId: 3 })

    const named = () =>
      wire
        .apiVersionTrace()
        .slice(seen)
        .map(({ node, fields }) => [node, fields.clusterId, fields.nodeId])
    expect(named()).toEqual([
      [1, 'cluster-B', 1],
      [2, 'cluster-B', 3],
    ])

    // A seed a rebootstrap builds starts again from nothing, and no request names cluster A or
    // node 2 after the change.
    await cluster.brokerPool.rebootstrap('required')
    await cluster.refreshMetadata()
    expect(wire.apiVersionTrace().at(-1)).toMatchObject({
      node: 1,
      fields: { clusterId: null, nodeId: -1 },
    })
    expect(
      wire
        .apiVersionTrace()
        .slice(seen)
        .filter(({ fields }) => fields.clusterId === 'cluster-A' || fields.nodeId === 2)
    ).toEqual([])
    expect(cluster.brokerPool.metadata.clusterId).toBe('cluster-B')
  })

  test('old brokers complete v4 negotiation', async () => {
    const { wire, cluster } = open({ profile: OLD_BROKER })
    await cluster.connect()
    await cluster.refreshMetadata()
    const broker = await cluster.findBroker({ nodeId: 2 })

    // Per connection, one v5 request the broker refuses and one v4 request it answers.
    const negotiation = () =>
      wire
        .apiVersionTrace()
        .map(({ node, version, discovery }) => [node, version, discovery === true])
    expect(negotiation()).toEqual([
      [1, 5, true],
      [1, 4, false],
      [2, 5, true],
      [2, 4, false],
    ])
    expect(broker.versions[18]).toEqual({ minVersion: 4, maxVersion: 4 })
    expect(broker.isConnected()).toBe(true)

    // A reconnect does it again, once: the fallback does not repeat and is not remembered.
    await broker.disconnect()
    await cluster.findBroker({ nodeId: 2 })
    expect(negotiation().slice(4)).toEqual([
      [2, 5, true],
      [2, 4, false],
    ])
    wire.assertClean()
  })

  test('v4 ignores identity fields', async () => {
    const { wire, cluster } = open({ profile: OLD_BROKER })
    await cluster.connect()
    await cluster.refreshMetadata()
    await cluster.findBroker({ nodeId: 2 })

    const [, , refused, answered] = wire.apiVersionTrace()
    // The v5 request, which a v4 broker refuses, did name the identity ...
    expect(refused).toMatchObject({ version: 5, fields: { clusterId: 'cluster-A', nodeId: 2 } })
    // ... and the v4 request after it has the software and nothing else, byte for byte.
    expect(answered).toMatchObject({ version: 4 })
    expect(answered.fields).toEqual({
      clientSoftwareName: CLIENT_SOFTWARE_NAME,
      clientSoftwareVersion: clientSoftwareVersion(),
    })
    expect(lastApiVersionsBody(wire)).toEqual(
      new Encoder()
        .writeUVarIntString(CLIENT_SOFTWARE_NAME)
        .writeUVarIntString(clientSoftwareVersion())
        .writeTaggedFields().buffer
    )
  })

  test('features and finalized epoch survive v5 read', async () => {
    const { wire, broker } = standalone()
    const featuresAt = (epoch, level) => ({
      ...wire.apiVersionsAnswer(),
      supportedFeatures: [{ name: 'transaction.version', minVersion: 0, maxVersion: 2 }],
      finalizedFeaturesEpoch: epoch,
      finalizedFeatures: [
        { name: 'transaction.version', minVersionLevel: level, maxVersionLevel: level },
      ],
    })
    wire.schedule.once({ api: 'ApiVersions' }, featuresAt('7', 1))
    await broker.connect()
    const connected = { 'transaction.version': { minVersionLevel: 1, maxVersionLevel: 1 } }
    expect(broker.finalizedFeatures).toEqual(connected)
    expect(broker.finalizedFeaturesEpoch).toBe(7)

    wire.schedule.once({ api: 'ApiVersions' }, featuresAt('9007199254740993', 2))
    const fresh = await broker.apiVersions({ includeFeatures: true })

    expect(fresh.finalizedFeaturesEpoch).toBe('9007199254740993')
    expect(fresh.supportedFeatures).toEqual([
      { name: 'transaction.version', minVersion: 0, maxVersion: 2 },
    ])
    expect(fresh.finalizedFeatures).toEqual([
      { name: 'transaction.version', minVersionLevel: 2, maxVersionLevel: 2 },
    ])
    // The snapshot the connection learned is not replaced by a read.
    expect(broker.finalizedFeatures).toEqual(connected)
    expect(broker.finalizedFeaturesEpoch).toBe(7)

    // Without the option the read answers the version map alone, as it always did: one entry per
    // API the broker offers, and none of the response's feature fields.
    const table = Object.fromEntries(
      wire
        .apiVersionsTable()
        .map(({ apiKey, minVersion, maxVersion }) => [apiKey, { minVersion, maxVersion }])
    )
    await expect(broker.apiVersions()).resolves.toEqual(table)
    expect(wire.apiVersionTrace().map(({ version }) => version)).toEqual([5, 5, 5])
  })

  describe('REBOOTSTRAP_REQUIRED', () => {
    test('ApiVersions rebootstrap-required uses the existing recovery path', async () => {
      const { wire, cluster, events } = open()
      await cluster.connect()
      await cluster.refreshMetadata()
      const seed = cluster.brokerPool.seedBroker
      wire.schedule.once({ api: 'ApiVersions', node: 2 }, REBOOTSTRAP_REQUIRED)

      const failure = await cluster.findBroker({ nodeId: 2 }).catch((error) => error)

      expect(failure).toMatchObject({ type: 'REBOOTSTRAP_REQUIRED', code: 129, retriable: false })
      expect(wire.apiVersionTrace().at(-1).fields).toMatchObject({
        clusterId: 'cluster-A',
        nodeId: 2,
      })
      expect(events).toEqual([{ reason: 'required' }])
      expect(cluster.brokerPool.seedBroker).not.toBe(seed)
      expect(seed.retired).toBe(true)
      expect(cluster.brokerPool.metadata).toBeNull()

      // The next attempt resolves the seed again and publishes fresh metadata, naming nothing old.
      await cluster.refreshMetadata()
      expect(cluster.brokerPool.metadata.clusterId).toBe('cluster-A')
      expect(wire.apiVersionTrace().at(-1)).toMatchObject({
        node: 1,
        fields: { clusterId: null, nodeId: -1 },
      })
      expect(events).toHaveLength(1)
    })

    test('one ApiVersions failure is not recovered twice', async () => {
      const { wire, cluster, events } = open()
      await cluster.connect()
      await cluster.refreshMetadata()
      const { brokerPool } = cluster
      expect(brokerPool.seedBroker.nodeId).toBe(1)
      // Only the seed is left to try, so the failure travels out of connect() and then through the
      // refresh's own catch, which handles a metadata failure of that kind.
      await brokerPool.seedBroker.disconnect()
      brokerPool.brokers = {}
      wire.schedule.once({ api: 'ApiVersions' }, REBOOTSTRAP_REQUIRED)

      await expect(cluster.refreshMetadata()).rejects.toMatchObject({
        type: 'REBOOTSTRAP_REQUIRED',
      })

      expect(events).toEqual([{ reason: 'required' }])
    })

    test('none sends no identities and never reboots', async () => {
      const { wire, cluster, events } = open({ strategy: 'none' })
      await cluster.connect()
      await cluster.refreshMetadata()
      const broker = await cluster.findBroker({ nodeId: 2 })
      const seed = cluster.brokerPool.seedBroker
      expect(cluster.brokerPool.metadata.clusterId).toBe('cluster-A')

      wire.schedule.once({ api: 'ApiVersions' }, REBOOTSTRAP_REQUIRED)
      await expect(broker.apiVersions({ includeFeatures: true })).rejects.toMatchObject({
        type: 'REBOOTSTRAP_REQUIRED',
      })
      await broker.disconnect()
      wire.schedule.once({ api: 'ApiVersions', node: 2 }, REBOOTSTRAP_REQUIRED)
      await expect(cluster.findBroker({ nodeId: 2 })).rejects.toMatchObject({
        type: 'REBOOTSTRAP_REQUIRED',
      })

      // The identity was known and withheld; the answer was refused, not accepted, and nothing was
      // torn down.
      expect(
        wire
          .apiVersionTrace()
          .every(({ fields }) => fields.clusterId === null && fields.nodeId === -1)
      ).toBe(true)
      expect(events).toEqual([])
      expect(cluster.brokerPool.seedBroker).toBe(seed)
      expect(seed.retired).toBe(false)
      expect(broker.isConnected()).toBe(false)
      expect(cluster.brokerPool.metadata.clusterId).toBe('cluster-A')
    })

    test('concurrent identity failures share one recovery', async () => {
      const { wire, cluster, events } = open()
      await cluster.connect()
      await cluster.refreshMetadata()
      const first = cluster.brokerPool.seedBroker
      const second = await cluster.findBroker({ nodeId: 2 })
      const gates = [
        wire.schedule.hold({ api: 'ApiVersions', node: 1 }),
        wire.schedule.hold({ api: 'ApiVersions', node: 2 }),
      ]
      const reads = [first, second].map((broker) =>
        broker.apiVersions({ includeFeatures: true }).catch((error) => error)
      )
      await Promise.all(gates.map((gate) => gate.arrived()))

      for (const gate of gates) gate.release(REBOOTSTRAP_REQUIRED)
      const failures = await Promise.all(reads)

      expect(failures.map(({ type }) => type)).toEqual([
        'REBOOTSTRAP_REQUIRED',
        'REBOOTSTRAP_REQUIRED',
      ])
      expect(events).toEqual([{ reason: 'required' }])
    })

    test('a recovery that fails leaves the caller the typed error', async () => {
      const { wire, cluster } = open()
      await cluster.connect()
      await cluster.refreshMetadata()
      cluster.brokerPool.rebootstrap = jest.fn().mockRejectedValue(new Error('no seed'))
      wire.schedule.once({ api: 'ApiVersions', node: 2 }, REBOOTSTRAP_REQUIRED)

      await expect(cluster.findBroker({ nodeId: 2 })).rejects.toMatchObject({
        type: 'REBOOTSTRAP_REQUIRED',
      })

      expect(cluster.brokerPool.rebootstrap).toHaveBeenCalledTimes(1)
      expect(cluster.brokerPool.rebootstrap).toHaveBeenCalledWith('required', { fence: undefined })
    })

    test('a recovery whose producer lifecycle closes while its seed resolves publishes nothing', async () => {
      const { wire, cluster, events } = open()
      await cluster.connect()
      await cluster.refreshMetadata()
      const { brokerPool } = cluster
      const seed = brokerPool.seedBroker
      const metadata = brokerPool.metadata
      // The broker's own connect, kept with its real implementation: it outlives the lifecycle.
      const node = brokerPool.brokers[2]
      const connects = []
      const connect = node.connect.bind(node)
      node.connect = (...args) => {
        const call = connect(...args)
        connects.push(
          call.then(
            () => null,
            (error) => error
          )
        )
        return call
      }
      // The recovery reaches its seed resolution, and waits there.
      const { connectionPoolBuilder } = brokerPool
      const build = connectionPoolBuilder.build.bind(connectionPoolBuilder)
      let resolving
      const resolvingStarted = new Promise((resolve) => (resolving = resolve))
      let resolved
      const resolveSeed = new Promise((resolve) => (resolved = resolve))
      connectionPoolBuilder.build = async (...args) => {
        resolving()
        await resolveSeed
        return build(...args)
      }
      wire.schedule.once({ api: 'ApiVersions', node: 2 }, REBOOTSTRAP_REQUIRED)
      const lifecycle = new Generation()
      const closed = new Error('the producer closed')

      const attempt = brokerPool.findBroker({ nodeId: 2, fence: lifecycle }).catch((error) => error)
      await resolvingStarted
      expect(connects).toHaveLength(1)
      expect(await Promise.race([connects[0], Promise.resolve('unsettled')])).toBe('unsettled')
      expect(events).toEqual([])
      lifecycle.close(closed)
      expect(await attempt).toBe(closed)
      resolved()

      // The broker's connect is over once the recovery is: it fails with the answer it was given.
      expect(await connects[0]).toMatchObject({ type: 'REBOOTSTRAP_REQUIRED' })
      expect(events).toEqual([])
      expect(brokerPool.seedBroker).toBe(seed)
      expect(seed.retired).toBe(false)
      expect(brokerPool.metadata).toBe(metadata)
      expect(brokerPool.lastRebootstrapAt).toBeNull()
    })

    test('a standalone broker rejects the typed error and publishes nothing', async () => {
      const { wire, broker } = standalone({ identity: { clusterId: 'cluster-A', nodeId: 2 } })
      wire.schedule.once({ api: 'ApiVersions' }, REBOOTSTRAP_REQUIRED)

      await expect(broker.connect()).rejects.toMatchObject({ type: 'REBOOTSTRAP_REQUIRED' })

      expect(broker.isConnected()).toBe(false)
      expect(broker.versions).toBeNull()
      expect(broker.finalizedFeaturesEpoch).toBe(-1)
    })
  })

  describe('a fresh ApiVersions read', () => {
    test('fresh feature-read rebootstrap-required uses the existing recovery path', async () => {
      const { wire, cluster, events } = open()
      const admin = await adminOf(cluster)
      await cluster.refreshMetadata()
      const seed = cluster.brokerPool.seedBroker
      wire.schedule.once({ api: 'ApiVersions' }, REBOOTSTRAP_REQUIRED)

      const failure = await admin.describeFeatures().catch((error) => error)

      expect(failure).toMatchObject({ type: 'REBOOTSTRAP_REQUIRED' })
      expect(wire.apiVersionTrace().at(-1).fields).toMatchObject({
        clusterId: 'cluster-A',
        nodeId: 1,
      })
      expect(events).toEqual([{ reason: 'required' }])
      expect(cluster.brokerPool.seedBroker).not.toBe(seed)

      // The next bounded attempt reads fresh features from the resolved seed.
      const features = await admin.describeFeatures()
      expect(features.finalizedFeaturesEpoch).toBe('7')
      expect(features.supportedFeatures['metadata.version']).toEqual({
        minVersion: 1,
        maxVersion: 25,
      })
      expect(events).toHaveLength(1)
    })

    test('a fresh version-map read that gets rebootstrap-required uses the existing recovery path', async () => {
      const { wire, cluster, events } = open()
      await cluster.connect()
      await cluster.refreshMetadata()
      const seed = cluster.brokerPool.seedBroker
      wire.schedule.once({ api: 'ApiVersions' }, REBOOTSTRAP_REQUIRED)

      await expect(seed.apiVersions()).rejects.toMatchObject({ type: 'REBOOTSTRAP_REQUIRED' })

      expect(events).toEqual([{ reason: 'required' }])
      expect(cluster.brokerPool.seedBroker).not.toBe(seed)
    })

    test('none: a fresh feature read rejects the typed error and starts no recovery', async () => {
      const { wire, cluster, events } = open({ strategy: 'none' })
      const admin = await adminOf(cluster)
      await cluster.refreshMetadata()
      const seed = cluster.brokerPool.seedBroker
      wire.schedule.once({ api: 'ApiVersions' }, REBOOTSTRAP_REQUIRED)

      await expect(admin.describeFeatures()).rejects.toMatchObject({ type: 'REBOOTSTRAP_REQUIRED' })

      expect(events).toEqual([])
      expect(cluster.brokerPool.seedBroker).toBe(seed)
      expect(cluster.brokerPool.metadata.clusterId).toBe('cluster-A')
    })

    test('an expired fresh identity read starts no recovery', async () => {
      const { wire, cluster, events } = open()
      const admin = await adminOf(cluster, { apiTimeout: 100 })
      await cluster.refreshMetadata()
      const seed = cluster.brokerPool.seedBroker
      // The seed's own read, kept with its real implementation: it outlives the admin's timeout.
      const reads = []
      const apiVersions = seed.apiVersions.bind(seed)
      seed.apiVersions = (...args) => {
        const read = apiVersions(...args)
        reads.push(
          read.then(
            () => null,
            (error) => error
          )
        )
        return read
      }
      const gate = wire.schedule.hold({ api: 'ApiVersions' })
      const pending = admin.describeFeatures().catch((error) => error)
      const held = await gate.arrived()
      const expired = await pending
      expect(expired).toMatchObject({ name: 'KafkaJSTimeout' })
      expect(reads).toHaveLength(1)
      // The read outlived the timeout: it still waits for the held answer.
      expect(await Promise.race([reads[0], Promise.resolve('unsettled')])).toBe('unsettled')

      gate.release(REBOOTSTRAP_REQUIRED)

      // The read is over once the late answer was read and acted on: it ends with the expiry,
      // never with the answer, so nothing reaches the pool's recovery.
      expect(await reads[0]).toBe(expired)
      const late = wire
        .requests()
        .find(
          ({ node, generation, correlationId }) =>
            node === held.node &&
            generation === held.generation &&
            correlationId === held.correlationId
        )
      expect(late.answeredOrder).toEqual(expect.any(Number))
      expect(events).toEqual([])
      expect(cluster.brokerPool.seedBroker).toBe(seed)
      expect(seed.retired).toBe(false)
      expect(cluster.brokerPool.metadata.clusterId).toBe('cluster-A')
    })

    test('a fresh identity read that outlived its pool starts no recovery on the replacement', async () => {
      const { wire, cluster, events } = open()
      await cluster.connect()
      await cluster.refreshMetadata()
      const broker = await cluster.findBroker({ nodeId: 2 })
      const gate = wire.schedule.hold({ api: 'ApiVersions', node: 2 })
      const read = broker.apiVersions({ includeFeatures: true }).catch((error) => error)
      await gate.arrived()
      const outlived = broker.connectionPool
      const replacement = await cluster.brokerPool.buildConnectionPool({
        host: 'node2',
        port: 9092,
        rack: null,
      })
      expect(broker.replaceConnectionPool(replacement)).toBe(true)

      gate.release(REBOOTSTRAP_REQUIRED)

      // The old read still fails with what it was answered, and nothing else happens.
      expect(await read).toMatchObject({ type: 'REBOOTSTRAP_REQUIRED' })
      expect(events).toEqual([])
      expect(broker.retired).toBe(false)
      expect(broker.connectionPool).toBe(replacement)
      expect(replacement.retired).toBe(false)
      expect(cluster.brokerPool.seedBroker.retired).toBe(false)
      expect(cluster.brokerPool.metadata.clusterId).toBe('cluster-A')
      await outlived.destroy({ drainTimeout: 0 })
    })
  })
})
