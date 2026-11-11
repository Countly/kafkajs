const Broker = require('..')
const ConnectionPool = require('../../network/connectionPool')
const { Kafka } = require('../../..')
const { KafkaJSInvalidConfigurationError } = require('../../errors')
const { CLIENT_SOFTWARE_NAME, clientSoftwareVersion } = require('../../protocol/clientSoftware')
const { createLogger } = require('../../loggers')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const apiVersionsV5 = require('../../../testHelpers/wire/apiVersionsV5')
const { readRequestHeader } = require('testHelpers')

const logger = createLogger({ level: 0, logCreator: () => () => {} })
const NODES = [
  { id: 1, host: 'node1', port: 9092 },
  { id: 2, host: 'node2', port: 9092 },
  { id: 3, host: 'node3', port: 9092 },
]
const NEW_BROKER = { advertise: { ApiVersions: [5, 5] } }
const IDENTITY = { clusterId: 'cluster-a', nodeId: 7 }

/** @param {string|null} text @returns {Buffer} compact nullable string: varint(length + 1), then UTF-8 */
const compact = (text) =>
  text === null
    ? Buffer.from([0])
    : Buffer.concat([Buffer.from([Buffer.byteLength(text) + 1]), Buffer.from(text)])

/** @param {number} node @returns {Buffer} int32 */
const int32 = (node) => {
  const bytes = Buffer.alloc(4)
  bytes.writeInt32BE(node)
  return bytes
}

/**
 * The ApiVersions v5 request body, built from the schema order and not from the codec: software
 * name, software version, cluster id, node id, empty tag buffer.
 * @param {string|null} clusterId intended cluster
 * @param {number} nodeId intended node
 * @returns {Buffer} body after the request header
 */
const goldenV5 = (clusterId, nodeId) =>
  Buffer.concat([
    compact(CLIENT_SOFTWARE_NAME),
    compact(clientSoftwareVersion()),
    compact(clusterId),
    int32(nodeId),
    Buffer.from([0]),
  ])

/** @returns {Buffer} the ApiVersions v4 body: software name, software version, empty tag buffer */
const goldenV4 = () =>
  Buffer.concat([compact(CLIENT_SOFTWARE_NAME), compact(clientSoftwareVersion()), Buffer.from([0])])

/**
 * @param {object} wire scripted cluster
 * @param {number} version ApiVersions version asked for
 * @returns {Buffer} the body of the last ApiVersions request of that version, after its header
 */
const lastBody = (wire, version) => {
  const frame = wire.fake.state.writes
    .filter((bytes) => {
      const header = readRequestHeader(bytes)
      return header.apiKey === 18 && header.apiVersion === version
    })
    .at(-1)
  return frame.subarray(readRequestHeader(frame).byteLength)
}

/** @returns {{clusterId: string|null, nodeId: number}[]} identities of every ApiVersions v5 sent */
const identitiesSent = (wire) =>
  wire
    .apiVersionTrace()
    .filter(({ version }) => version === 5)
    .map(({ fields }) => ({ clusterId: fields.clusterId, nodeId: fields.nodeId }))

describe('Metadata cluster check', () => {
  const wires = []
  const closers = []

  afterEach(async () => {
    await Promise.all(closers.splice(0).map((close) => close()))
    for (const wire of wires.splice(0)) wire.assertClean()
  })

  /** A real Broker over a real pool of a scripted cluster, whose identity getter names IDENTITY. */
  const standalone = ({ strategy, check, profile = NEW_BROKER }) => {
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
      metadataIdentity: () => IDENTITY,
      metadataRecoveryStrategy: strategy,
      ...(check !== undefined && { metadataClusterCheck: check }),
    })
    wires.push(wire)
    closers.push(() => broker.disconnect({ drainTimeout: 0 }))
    return { wire, broker }
  }

  /** A cluster of the scripted nodes built through the public `Kafka` constructor. */
  const open = (options = {}) => {
    const state = { clusterId: 'cluster-A', nodes: NODES.slice(0, 2) }
    const wire = createScriptedCluster({ nodes: NODES, ...NEW_BROKER })
    wire.answer('Metadata', () => ({
      clusterId: state.clusterId,
      brokers: state.nodes.map(({ id, host, port }) => ({ nodeId: id, host, port, rack: null })),
      controllerId: state.nodes[0].id,
      topics: [],
    }))
    const kafka = new Kafka({
      brokers: ['node1:9092'],
      socketFactory: wire.socketFactory,
      logLevel: 0,
      retry: { retries: 0 },
      enforceRequestTimeout: false,
      ...options,
    })
    const factory = Object.getOwnPropertySymbols(kafka).find(
      (symbol) => String(symbol) === 'Symbol(private:Kafka:createCluster)'
    )
    const cluster = kafka[factory]({})
    wires.push(wire)
    closers.push(() => cluster.disconnect({ drainTimeout: 0 }))
    return { wire, cluster, state }
  }

  test('true rebootstrap sends the known cluster and node', async () => {
    const { wire, broker } = standalone({ strategy: 'rebootstrap', check: true })
    await broker.connect()
    const body = lastBody(wire, 5)
    expect(body).toEqual(goldenV5('cluster-a', 7))
    expect(apiVersionsV5.readRequest(body)).toMatchObject({ clusterId: 'cluster-a', nodeId: 7 })
  })

  test('false rebootstrap sends no intended identity', async () => {
    const { wire, broker } = standalone({ strategy: 'rebootstrap', check: false })
    await broker.connect()
    const body = lastBody(wire, 5)
    expect(body).toEqual(goldenV5(null, -1))
    expect(apiVersionsV5.readRequest(body)).toMatchObject({ clusterId: null, nodeId: -1 })
  })

  test('true none sends no intended identity', async () => {
    const { wire, broker } = standalone({ strategy: 'none', check: true })
    await broker.connect()
    const body = lastBody(wire, 5)
    expect(body).toEqual(goldenV5(null, -1))
    expect(apiVersionsV5.readRequest(body)).toMatchObject({ clusterId: null, nodeId: -1 })
  })

  test('false none sends no intended identity', async () => {
    const { wire, broker } = standalone({ strategy: 'none', check: false })
    await broker.connect()
    const body = lastBody(wire, 5)
    expect(body).toEqual(goldenV5(null, -1))
    expect(apiVersionsV5.readRequest(body)).toMatchObject({ clusterId: null, nodeId: -1 })
  })

  test('defaults to checking the cluster', async () => {
    const standaloneDefault = standalone({ strategy: 'rebootstrap' })
    await standaloneDefault.broker.connect()
    expect(lastBody(standaloneDefault.wire, 5)).toEqual(goldenV5('cluster-a', 7))

    const { wire, cluster } = open()
    await cluster.connect()
    await cluster.refreshMetadata()
    await cluster.findBroker({ nodeId: 2 })
    expect(identitiesSent(wire)).toEqual([
      { clusterId: null, nodeId: -1 },
      { clusterId: 'cluster-A', nodeId: 2 },
    ])
    expect(cluster.brokerPool.metadataClusterCheck).toBe(true)
    expect(standaloneDefault.broker.metadataClusterCheck).toBe(true)
  })

  test('rejects a nonboolean option before connecting', () => {
    const socketFactory = jest.fn()
    const construct = (metadataClusterCheck) =>
      new Kafka({ brokers: ['node1:9092'], socketFactory, metadataClusterCheck })
    const invalid = ['true', 'false', 1, 0, null, {}, [], Symbol('check')]
    const results = invalid.map((value) => {
      try {
        construct(value)
        return 'accepted'
      } catch (error) {
        return error instanceof KafkaJSInvalidConfigurationError ? 'rejected' : error.name
      }
    })
    expect(results).toEqual(invalid.map(() => 'rejected'))
    expect(socketFactory).not.toHaveBeenCalled()
    for (const accepted of [true, false, undefined]) expect(() => construct(accepted)).not.toThrow()
  })

  test('replacement brokers inherit the opt out', async () => {
    const { wire, cluster, state } = open({ metadataClusterCheck: false })
    expect(cluster.brokerPool.metadataClusterCheck).toBe(false)
    await cluster.connect()
    await cluster.refreshMetadata()
    await cluster.findBroker({ nodeId: 2 })

    state.clusterId = 'cluster-B'
    state.nodes = [NODES[0], { id: 3, host: 'node2', port: 9092 }]
    await cluster.refreshMetadata()
    await cluster.brokerPool.brokers[1].disconnect()
    await cluster.findBroker({ nodeId: 1 })
    await cluster.findBroker({ nodeId: 3 })
    await cluster.brokerPool.rebootstrap('required')
    await cluster.refreshMetadata()

    const sent = identitiesSent(wire)
    expect(sent.length).toBeGreaterThanOrEqual(5)
    expect(sent).toEqual(sent.map(() => ({ clusterId: null, nodeId: -1 })))
    const brokers = Object.values(cluster.brokerPool.brokers)
    expect(brokers.map((broker) => broker.metadataClusterCheck)).toEqual(brokers.map(() => false))
    expect(cluster.brokerPool.seedBroker.metadataClusterCheck).toBe(false)
  })

  test('older brokers retain their negotiated request bytes', async () => {
    const bodies = []
    for (const check of [true, false]) {
      const { wire, broker } = standalone({
        strategy: 'rebootstrap',
        check,
        profile: {},
      })
      await broker.connect()
      expect(broker.versions[18]).toEqual({ minVersion: 4, maxVersion: 4 })
      bodies.push(lastBody(wire, 4))
    }
    expect(bodies).toEqual([goldenV4(), goldenV4()])
  })
})
