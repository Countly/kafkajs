const net = require('net')
const Cluster = require('../../cluster')
const createAdmin = require('../../admin')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { Kafka, logLevel } = require('../../../index')
const { CLUSTER_REBOOTSTRAP } = require('../../cluster/instrumentationEvents')
const apiVersionsV4 = require('../../../testHelpers/wire/apiVersionsV4')
const apiVersionsV5 = require('../../../testHelpers/wire/apiVersionsV5')
const {
  connectionOpts,
  newLogger,
  plainTextBrokers,
  readRequestHeader,
  writeEvidence,
  wireVersionsOf,
} = require('testHelpers')
const { withTimeout } = require('../../consumer/__tests__/fixtures/liveScenario')

// Gated: it opens connections to a live three-broker cluster and sends some of them to the wrong
// broker. Run it alone through `npm run test:e2e:api-identity`; `KAFKA_VERSION` names the cluster
// under test.
const describeApiIdentity = process.env.API_IDENTITY_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'api-identity'
// The scenario budget is the whole file, from the first hook to the last cleanup; a case and a hook
// have their own, shorter ones.
const SCENARIO_BUDGET_MS = 120000
const CASE_BUDGET_MS = 40000
const HOOK_BUDGET_MS = 20000
const DISCONNECT_BUDGET_MS = 10000

/**
 * The ApiVersions version a broker of the lane's `KAFKA_VERSION` offers: v5, which checks the
 * cluster and node ids (KIP-1242), from Kafka 4.4, and v4 before it.
 * @returns {4|5}
 * @throws {Error} when `KAFKA_VERSION` is not `major.minor.patch`, so no lane is guessed
 */
const offeredVersion = () => {
  const [major, minor] = (process.env.KAFKA_VERSION || '').split('.').map(Number)
  if (!Number.isInteger(major) || !Number.isInteger(minor)) {
    throw new Error(
      `KAFKA_VERSION must name the cluster under test, got ${process.env.KAFKA_VERSION}`
    )
  }

  return major > 4 || (major === 4 && minor >= 4) ? 5 : 4
}

/** @returns {number[]} the versions the client sends, in order, to a broker that offers `offered` */
const versionsAskedOf = (offered) => (offered === 5 ? [5] : [5, 4])

/**
 * Live proof of what ApiVersions names. The client connects through a `socketFactory` that records
 * every ApiVersions request it writes and can send a connection to a broker other than the one the
 * client dialled: the client then holds the right node id and the wrong broker, which a Kafka 4.4
 * broker answers with REBOOTSTRAP_REQUIRED (129). A broker below 4.4 ignores what a v5 request
 * names and completes the negotiation at v4, so the same misroute goes unnoticed there.
 *
 * Requests are read off the wire, never inferred: the cluster id and node id of each one, and the
 * versions each broker was asked, in order. Counts and versions are asserted, never durations.
 *
 * @see https://github.com/apache/kafka/blob/4.4.0/core/src/main/scala/kafka/server/KafkaApis.scala
 */
describeApiIdentity('Broker > ApiVersions identity (live)', () => {
  /** @type {{ clusterId: string, brokers: { nodeId: number, host: string, port: number }[] }} */
  let truth
  let startedAt
  const closers = []

  /**
   * A socket factory that sends the connection of an endpoint in `routes` to the endpoint it maps
   * to, and records the ApiVersions requests written on it, each as the version it asked, the
   * fields its body names and the endpoint it was dialled and sent to.
   * @returns {{ socketFactory: Function, routes: Map<string, { host: string, port: number }>, sent: object[] }}
   */
  const routing = () => {
    const routes = new Map()
    const sent = []
    const socketFactory = ({ host, port, onConnect }) => {
      const target = routes.get(`${host}:${port}`) ?? { host, port }
      const socket = net.connect(target, onConnect)
      socket.setKeepAlive(true, 60000)
      const write = socket.write.bind(socket)
      socket.write = (frame, ...rest) => {
        const header = readRequestHeader(Buffer.from(frame))
        if (header.apiKey === 18) {
          const body = Buffer.from(frame).subarray(header.byteLength)
          const pair = header.apiVersion === 5 ? apiVersionsV5 : apiVersionsV4
          sent.push({
            dialled: `${host}:${port}`,
            routedTo: `${target.host}:${target.port}`,
            version: header.apiVersion,
            fields: pair.readRequest(body),
          })
        }
        return write(frame, ...rest)
      }

      return socket
    }

    return { socketFactory, routes, sent }
  }

  /**
   * A `Cluster` over the live brokers, its REBOOTSTRAP events collected, and a public admin of it.
   * @param {object} options
   * @param {Function} options.socketFactory
   * @param {'rebootstrap'|'none'} [options.strategy]
   */
  const open = ({ socketFactory, strategy }) => {
    const emitter = new InstrumentationEventEmitter()
    const events = []
    emitter.addListener(CLUSTER_REBOOTSTRAP, ({ payload }) => events.push(payload))
    const cluster = new Cluster({
      ...connectionOpts(),
      brokers: plainTextBrokers(),
      logger: newLogger(),
      socketFactory,
      instrumentationEmitter: emitter,
      metadataRecoveryStrategy: strategy,
      retry: { retries: 3, initialRetryTime: 100, maxRetryTime: 500 },
      enforceRequestTimeout: false,
    })
    const admin = createAdmin({
      logger: newLogger(),
      cluster,
      retry: { retries: 3, initialRetryTime: 100, maxRetryTime: 500 },
    })
    closers.push(
      () =>
        withTimeout(
          admin.disconnect().catch(() => {}),
          DISCONNECT_BUDGET_MS
        ),
      () =>
        withTimeout(
          cluster.disconnect().catch(() => {}),
          DISCONNECT_BUDGET_MS
        )
    )

    return { cluster, admin, events }
  }

  const endpointOf = ({ host, port }) => `${host}:${port}`

  /**
   * Picks the node a case connects to and, for a misroute, the node it ends up at: two nodes that
   * are neither the first bootstrap broker the client used (which it knows by a node id too, once
   * it has metadata, but which is not a new connection).
   * @param {object[]} sent the requests recorded so far
   * @returns {{ intended: object, actual: object }}
   */
  const nodesOutside = (sent) => {
    const bootstrap = sent[0].dialled
    const [intended, actual] = truth.brokers.filter((broker) => endpointOf(broker) !== bootstrap)

    return { intended, actual }
  }

  /** @returns {object[]} what was sent to the broker the client dialled at `endpoint` */
  const sentTo = (sent, endpoint) => sent.filter(({ dialled }) => dialled === endpoint)

  beforeAll(async () => {
    startedAt = Date.now()
    const kafka = new Kafka({
      clientId: 'api-identity-live-truth',
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
    })
    const admin = kafka.admin()
    await admin.connect()
    try {
      const { clusterId, brokers } = await admin.describeCluster()
      truth = { clusterId, brokers: brokers.sort((a, b) => a.nodeId - b.nodeId) }
    } finally {
      await withTimeout(
        admin.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      )
    }
    expect(truth.brokers).toHaveLength(3)
  }, HOOK_BUDGET_MS)

  afterEach(async () => {
    await Promise.all(closers.splice(0).map((close) => close()))
    expect(Date.now() - startedAt).toBeLessThan(SCENARIO_BUDGET_MS)
  }, HOOK_BUDGET_MS)

  test(
    'a bootstrap broker is named no identity and a known node is named its cluster and node id',
    async () => {
      const offered = offeredVersion()
      const route = routing()
      const { cluster } = open({ socketFactory: route.socketFactory })

      await cluster.connect()
      await cluster.refreshMetadata()
      const { intended } = nodesOutside(route.sent)
      const broker = await cluster.findBroker({ nodeId: intended.nodeId })

      // The bootstrap connection knew nothing: every request it carried names the defaults.
      const bootstrap = sentTo(route.sent, route.sent[0].dialled)
      expect(bootstrap.map(({ version }) => version)).toEqual(versionsAskedOf(offered))
      expect(bootstrap[0].fields).toMatchObject({ clusterId: null, nodeId: -1 })
      // The known node's connection named its cluster and its own id in the v5 request.
      const known = sentTo(route.sent, endpointOf(intended))
      expect(known.map(({ version }) => version)).toEqual(versionsAskedOf(offered))
      expect(known[0].fields).toMatchObject({ clusterId: truth.clusterId, nodeId: intended.nodeId })
      // Below Kafka 4.4 the completed negotiation is v4, which carries no identity at all.
      expect(broker.versions[18].maxVersion).toBe(offered)
      if (offered === 4) {
        expect(Object.keys(known[1].fields).sort()).toEqual([
          'clientSoftwareName',
          'clientSoftwareVersion',
        ])
      }
      writeEvidence(EVIDENCE, {
        step: 'known-node',
        wireVersions: wireVersionsOf(
          route.sent.map(({ version }) => ({ apiName: 'ApiVersions', apiVersion: version }))
        ),
        expectedVersion: offered,
        negotiated: broker.versions[18].maxVersion,
        clusterId: truth.clusterId,
        named: { nodeId: known[0].fields.nodeId, clusterId: known[0].fields.clusterId },
      })
    },
    CASE_BUDGET_MS
  )

  test(
    'a node that is another broker is refused with REBOOTSTRAP_REQUIRED on 4.4, recovered from once, and not noticed below it',
    async () => {
      const offered = offeredVersion()
      const route = routing()
      const { cluster, admin, events } = open({ socketFactory: route.socketFactory })
      await cluster.connect()
      await cluster.refreshMetadata()
      const { intended, actual } = nodesOutside(route.sent)
      const seed = cluster.brokerPool.seedBroker
      // The client keeps the right node id and the right address, and reaches the wrong broker.
      route.routes.set(endpointOf(intended), { host: actual.host, port: actual.port })

      const attempt = await cluster.findBroker({ nodeId: intended.nodeId }).catch((e) => e)

      const misrouted = sentTo(route.sent, endpointOf(intended))
      expect(misrouted[0]).toMatchObject({
        version: 5,
        routedTo: endpointOf(actual),
        fields: { clusterId: truth.clusterId, nodeId: intended.nodeId },
      })
      if (offered === 5) {
        // The broker that was reached is not node `intended`, so it refuses what the request names.
        expect(attempt).toMatchObject({ type: 'REBOOTSTRAP_REQUIRED', code: 129 })
        expect(events).toEqual([{ reason: 'required' }])
        expect(cluster.brokerPool.seedBroker).not.toBe(seed)
      } else {
        // A broker below 4.4 never reads the identity; it completes at v4 with the wrong broker.
        expect(attempt.versions[18].maxVersion).toBe(4)
        expect(events).toEqual([])
      }

      // The mapping is restored: the client resolves its seed again and the next public operation
      // works against the node it meant.
      route.routes.clear()
      await cluster.refreshMetadata()
      await cluster.findBroker({ nodeId: intended.nodeId })
      const features = await admin.describeFeatures({ brokerId: intended.nodeId })
      expect(features.finalizedFeatures['metadata.version']).toEqual({
        minVersionLevel: expect.any(Number),
        maxVersionLevel: expect.any(Number),
      })
      expect(events).toHaveLength(offered === 5 ? 1 : 0)
      writeEvidence(EVIDENCE, {
        step: 'misroute',
        wireVersions: wireVersionsOf(
          route.sent.map(({ version }) => ({ apiName: 'ApiVersions', apiVersion: version }))
        ),
        expectedVersion: offered,
        refused: offered === 5,
        events,
        misroutedTo: endpointOf(actual),
        intended: { endpoint: endpointOf(intended), nodeId: intended.nodeId },
      })
    },
    CASE_BUDGET_MS
  )

  test(
    'none names no identity, so a misrouted node is neither refused nor recovered from',
    async () => {
      const offered = offeredVersion()
      const route = routing()
      const { cluster, events } = open({ socketFactory: route.socketFactory, strategy: 'none' })
      await cluster.connect()
      await cluster.refreshMetadata()
      const { intended, actual } = nodesOutside(route.sent)
      route.routes.set(endpointOf(intended), { host: actual.host, port: actual.port })

      const broker = await cluster.findBroker({ nodeId: intended.nodeId })

      expect(broker.versions[18].maxVersion).toBe(offered)
      expect(events).toEqual([])
      // Every request names nothing: the defaults at v5, and no field at all at v4.
      expect(
        route.sent.every(
          ({ fields }) => (fields.clusterId ?? null) === null && (fields.nodeId ?? -1) === -1
        )
      ).toBe(true)
      expect(route.sent.some(({ routedTo }) => routedTo === endpointOf(actual))).toBe(true)
      writeEvidence(EVIDENCE, {
        step: 'none-strategy',
        wireVersions: wireVersionsOf(
          route.sent.map(({ version }) => ({ apiName: 'ApiVersions', apiVersion: version }))
        ),
        expectedVersion: offered,
        events,
        requests: route.sent.length,
      })
    },
    CASE_BUDGET_MS
  )
})
