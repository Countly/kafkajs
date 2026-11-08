const crypto = require('crypto')
const fs = require('fs')
const net = require('net')
const path = require('path')
const { performance } = require('perf_hooks')
const { Kafka, logLevel, KafkaJSTelemetryError } = require('../../../index')
const { runCase } = require('../../../scripts/e2e/telemetry/evidence')
const {
  TelemetryExportReader,
  TelemetryWireObserver,
  withTelemetryResource,
  verifyMetricWindows,
} = require('../../../testHelpers/telemetryFixtures')
const pkg = require('../../../package.json')

// Gated: it needs the broker telemetry fixture (docker-compose.telemetry.yml for the configured
// cases, docker-compose.yml for the no-plugin control) and runs through the telemetry lanes of
// .github/workflows/ci.yml with TELEMETRY_E2E=1. A configured failure always fails: the only
// self-skip is the absence of the outer opt-in.
const live = process.env.TELEMETRY_E2E ? test : test.skip

const ROOT = path.resolve(__dirname, '../../..')
const MANIFEST = JSON.parse(
  fs.readFileSync(path.join(ROOT, 'scripts/e2e/telemetry/fixtures/manifest.json'), 'utf8')
)
const PORTS = { configured: [19092, 19095, 19098], base: [9092, 9095, 9098] }
const NODE_OF_PORT = { 19092: '1', 19095: '2', 19098: '3', 9092: '1', 9095: '2', 9098: '3' }
const KINDS = ['producer', 'consumer', 'admin', 'share']
const RACK = 'fixture-rack'
const WINDOW_MS = 15000
const PREFIX = 'org.apache.kafka.'
const ZERO_UUID = '00000000-0000-0000-0000-000000000000'
const UUID_FORMAT = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
const UINT64_MAX = 2n ** 64n - 1n

/**
 * @param {Array<function():Promise<*>>} tasks started together
 * @returns {Promise<void>} when all settled
 * @throws {*} the single failure or an AggregateError of all
 */
const settle = async (tasks) => {
  const results = await Promise.allSettled(tasks.map((task) => task()))
  const errors = results.filter((r) => r.status === 'rejected').map((r) => r.reason)
  if (errors.length === 1) throw errors[0]
  if (errors.length) throw new AggregateError(errors, 'Several telemetry operations failed')
}

/**
 * @param {function():*} ready predicate polled every 100 ms
 * @param {{now:function():number}} clock monotonic clock
 * @param {number} deadline absolute clock value; expiry fails the case
 * @param {string} what description for the failure
 * @returns {Promise<void>} once the predicate holds
 */
const pollUntil = async (ready, clock, deadline, what) => {
  while (!ready()) {
    if (clock.now() > deadline) throw new Error(`Timed out waiting for ${what}`)
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

/** Plaintext IPv4 socket for the telemetry-disabled auxiliary admin. */
const ipv4Factory = ({ host, port, onConnect }) => {
  const socket = net.connect({ host, port, family: 4 }, onConnect)
  socket.setKeepAlive(true, 60000)
  return socket
}

/**
 * @param {string} file absolute path
 * @returns {string} SHA-256 of the file
 */
const fileSha = (file) => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex')

/**
 * Connects the telemetry-disabled auxiliary admin and checks the Metadata host:port/node identity.
 * @param {'configured'|'base'} mode fixture topology
 * @param {Cleanup} cleanup case cleanup
 * @returns {Promise<{stem:string,line:string,directory:string,clock:object,aux:object}>} shared case state
 */
const setup = async (mode, cleanup) => {
  const line = process.env.KAFKA_VERSION
  const directory = process.env.TELEMETRY_EVIDENCE_DIR
  if (!line) throw new Error('KAFKA_VERSION must name the broker line')
  if (!directory || !path.isAbsolute(directory))
    throw new Error('TELEMETRY_EVIDENCE_DIR must name the absolute run directory')
  const stem = cleanup.evidence.record.run.stem
  const kafka = new Kafka({
    clientId: stem + '-aux',
    brokers: PORTS[mode].map((port) => `localhost:${port}`),
    socketFactory: ipv4Factory,
    logLevel: logLevel.NOTHING,
  })
  const aux = kafka.admin({ enableMetricsPush: false, apiTimeout: 3000 })
  cleanup.defer(() => aux.disconnect())
  await aux.connect()
  const cluster = await aux.describeCluster({ timeout: 3000 })
  expect(cluster.brokers.map((b) => [b.nodeId, b.host, b.port]).sort()).toEqual(
    PORTS[mode].map((port, index) => [index + 1, 'localhost', port])
  )
  return { stem, line, directory, clock: { now: () => performance.now() }, aux }
}

/**
 * Builds the four public clients of a case, each on its own observed socket factory. Cleanup
 * order is fixed: disconnect every client, dispose every observer, then require it clean.
 * @param {object} options case state plus `mode`, `enabled` and a name `suffix`
 * @param {Cleanup} cleanup case cleanup
 * @returns {{observers:object,clients:object,closeAll:function():Promise<void>}} clients by kind
 */
const buildClients = ({ mode, stem, line, directory, clock, enabled, suffix = '' }, cleanup) => {
  const observers = {}
  const clients = {}
  cleanup.defer(() =>
    settle(
      Object.values(observers).map((observer) => () => {
        observer.assertClean({ allowUnanswered: observer.cancellationKeys() })
      })
    )
  )
  cleanup.defer(() => settle(Object.values(observers).map((observer) => () => observer.dispose())))
  let closing
  const closeAll = () =>
    (closing ??= settle(
      Object.entries(clients).map(
        ([kind, client]) =>
          () =>
            observers[kind].closeClient(client)
      )
    ))
  cleanup.defer(closeAll)
  for (const kind of KINDS) {
    const observer = new TelemetryWireObserver({
      directory,
      line,
      clock,
      mode,
      owner: kind + suffix,
    })
    observers[kind] = observer
    cleanup.evidence.observers[kind + suffix] = observer
    const kafka = new Kafka({
      clientId: `${stem}-${kind}${suffix}`,
      brokers: PORTS[mode].map((port) => `localhost:${port}`),
      socketFactory: (options) => observer.socketFactory(options),
      logLevel: logLevel.NOTHING,
    })
    const common = { enableMetricsPush: enabled }
    if (kind === 'producer')
      clients[kind] = kafka.producer({ ...common, idempotent: false, rackId: RACK })
    if (kind === 'admin') clients[kind] = kafka.admin(common)
    if (kind === 'consumer')
      clients[kind] = kafka.consumer({
        ...common,
        groupId: `${stem}-group${suffix}`,
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
        maxWaitTimeInMs: 500,
        rackId: RACK,
      })
    if (kind === 'share')
      clients[kind] = kafka.shareConsumer({
        ...common,
        groupId: `${stem}-share-group${suffix}`,
        rackId: RACK,
      })
  }
  return { observers, clients, closeAll }
}

/**
 * @param {string} kind client kind
 * @returns {string} the metric name prefix; the ShareConsumer uses the consumer namespace
 */
const prefixOf = (kind) => PREFIX + (kind === 'share' ? 'consumer' : kind) + '.'

/**
 * @param {string} stem case stem
 * @returns {Object<string,Object<string,string>>} literal applicable resource attributes by kind
 */
const resourcesOf = (stem) => ({
  producer: { client_rack: RACK },
  consumer: { client_rack: RACK, group_id: `${stem}-group` },
  admin: {},
  share: { client_rack: RACK, group_id: `${stem}-share-group` },
})

/**
 * Checks one independently decoded MetricsData against the literal metric contract of a kind.
 * @param {string} kind client kind
 * @param {string} stem case stem
 * @param {object} decoded Java-decoded payload
 * @param {boolean} delta whether the broker requested DELTA temporality
 * @returns {void}
 */
const checkDecoded = (kind, stem, decoded, delta) => {
  expect(decoded.resources).toHaveLength(1)
  expect(decoded.resources[0].droppedAttributes).toBe(0)
  expect(Object.fromEntries(decoded.resources[0].attrs.map(([k, , v]) => [k, v]))).toEqual(
    resourcesOf(stem)[kind]
  )
  expect(decoded.resources[0].attrs.every(([, type]) => type === 'string')).toBe(true)
  const templates = {
    [prefixOf(kind) + 'connection.creation.total']: ['SUM', '1'],
    [prefixOf(kind) + 'node.request.latency.avg']: ['GAUGE', 'ms'],
    [prefixOf(kind) + 'node.request.latency.max']: ['GAUGE', 'ms'],
  }
  expect(decoded.metrics.some((m) => m.name === prefixOf(kind) + 'connection.creation.total')).toBe(
    true
  )
  const seen = {}
  for (const metric of decoded.metrics) {
    expect(Object.keys(templates)).toContain(metric.name)
    const [type, unit] = templates[metric.name]
    expect(metric).toMatchObject({
      scopeName: 'countly-kafkajs',
      scopeVersion: pkg.version,
      scopeDroppedAttributes: 0,
      unit,
      kind: type,
    })
    expect(metric.points.length).toBeGreaterThan(0)
    for (const point of metric.points) {
      expect(BigInt(point.timeNs)).toBeLessThanOrEqual(UINT64_MAX)
      if (type === 'SUM') {
        expect(BigInt(point.startNs)).toBeLessThanOrEqual(BigInt(point.timeNs))
        expect(point.attrs).toEqual([])
        expect(point.valueKind).toBe('AS_INT')
        expect(BigInt(point.asInt)).toBeGreaterThanOrEqual(0n)
      } else {
        expect(point.attrs).toHaveLength(1)
        expect(point.attrs[0].slice(0, 2)).toEqual(['node_id', 'string'])
        expect(['1', '2', '3']).toContain(point.attrs[0][2])
        expect(point.valueKind).toBe('AS_DOUBLE')
        const value = Number(point.asDouble)
        expect(Number.isFinite(value) && value >= 0).toBe(true)
        seen[point.attrs[0][2]] = seen[point.attrs[0][2]] || {}
        seen[point.attrs[0][2]][metric.name.endsWith('.avg') ? 'avg' : 'max'] = value
      }
    }
    if (type === 'SUM') {
      expect(metric.monotonic).toBe(true)
      expect(metric.temporality).toBe(
        delta ? 'AGGREGATION_TEMPORALITY_DELTA' : 'AGGREGATION_TEMPORALITY_CUMULATIVE'
      )
      expect(metric.points).toHaveLength(1)
    }
  }
  for (const pair of Object.values(seen)) {
    expect(typeof pair.avg).toBe('number')
    expect(typeof pair.max).toBe('number')
    expect(pair.max).toBeGreaterThanOrEqual(pair.avg)
  }
}

/**
 * @param {object} decoded decoded payload
 * @param {string} kind client kind
 * @returns {Object<string,{avg:number,max:number}>} node gauge pairs by node
 */
const nodeGauges = (decoded, kind) => {
  const out = {}
  for (const metric of decoded.metrics) {
    if (!metric.name.startsWith(prefixOf(kind) + 'node.request.latency.')) continue
    for (const point of metric.points) {
      const node = point.attrs[0][2]
      out[node] = out[node] || {}
      out[node][metric.name.endsWith('.avg') ? 'avg' : 'max'] = Number(point.asDouble)
    }
  }
  return out
}

/**
 * Joins each ordinary REQUEST event exactly once to this owner's captured socket request.
 * Only the admin's topology-installing discovery binds nodes in this connect-only scenario.
 * @param {object} observer socket observer for one client
 * @param {object[]} events ordinary REQUEST payloads captured before connect
 * @param {number} knownAfter Metadata response time, or Infinity for bootstrap-only clients
 * @returns {object[]} complete normal observation ledger including bootstrap requests
 */
const joinNormal = (observer, events, knownAfter) => {
  const wire = observer.normalRequests()
  const used = new Set()
  const joined = events.map((event) => {
    const node = NODE_OF_PORT[Number(event.broker.split(':').pop())]
    const found = wire.filter(
      (w) =>
        !used.has(w.requestKey) &&
        w.node === node &&
        w.correlation === event.correlationId &&
        w.apiKey === event.apiKey &&
        w.apiVersion === event.apiVersion &&
        w.clientId === event.clientId
    )
    expect(found).toHaveLength(1)
    const [request] = found
    used.add(request.requestKey)
    return {
      ...event,
      requestKey: request.requestKey,
      node,
      socketId: request.socketId,
      writeAt: request.writeAt,
      responseAt: request.response.responseAt,
      knownNode: request.writeAt > knownAfter,
    }
  })
  expect(used.size).toBe(wire.length)
  return joined.sort((a, b) => a.responseAt - b.responseAt)
}

live(
  'reporter export',
  () =>
    runCase('telemetry-reporter', async (cleanup, publish, attempt) => {
      const state = await setup('configured', cleanup)
      const { stem, line, directory, clock, aux } = state
      const reader = new TelemetryExportReader(directory)
      for (const node of ['1', '2', '3']) {
        const proof = JSON.parse(
          fs.readFileSync(path.join(directory, `reporter-${node}.json`), 'utf8')
        )
        expect(proof).toMatchObject({
          node,
          class: MANIFEST.classes.reporter,
          kafkaClients: line,
          queueCapacity: 128,
          maxRecordBytes: 4096,
          maxFileBytes: 2097152,
          reporterJar: '/opt/kafka/libs/counting-telemetry-reporter.jar',
          otlpBindings: '/opt/kafka/libs/counting-telemetry-reporter.jar',
        })
        expect(proof.kafkaCommit).toBe(MANIFEST.lines[line].observedCommit.split(' ')[2])
        expect(proof.protobufJar).toContain('protobuf-java-3.25.5.jar')
        expect(proof.java).toBeTruthy()
        expect(proof.loadedAtMs).toBeGreaterThan(0)
      }
      const compose = path.join(ROOT, 'docker-compose.telemetry.yml')
      expect(fileSha(compose)).toBe(MANIFEST.composeSha256['docker-compose.telemetry.yml'])
      const jar = MANIFEST.lines[line].reporterJar
      expect(fileSha(path.join(ROOT, jar.path))).toBe(jar.sha256)

      const { observers, clients, closeAll } = buildClients(
        { mode: 'configured', stem, line, directory, clock, enabled: true },
        cleanup
      )
      await observers.admin.decodeRaw(Buffer.alloc(0), { budgetMs: 8000 })
      const events = Object.fromEntries(KINDS.map((kind) => [kind, []]))
      for (const kind of KINDS)
        cleanup.defer(
          clients[kind].on(clients[kind].events.REQUEST, (event) =>
            events[kind].push(event.payload)
          )
        )
      const entries = {
        metrics: PREFIX,
        'interval.ms': '1000',
        match: `client_id=^${stem}-(producer|consumer|admin|share)$`,
      }
      const results = {}
      attempt.record.results = results

      await withTelemetryResource(aux, { name: stem, entries }, async () => {
        attempt.stage = 'capture'
        const start = clock.now()
        const deadline = start + WINDOW_MS
        results.window = { start, deadline }
        await settle(KINDS.map((kind) => () => clients[kind].connect()))
        const ids = {}
        attempt.record.uuids = ids
        await settle(
          KINDS.map((kind) => async () => {
            ids[kind] = await clients[kind].clientInstanceId({ timeout: 3000 })
          })
        )
        expect(new Set(Object.values(ids)).size).toBe(4)
        for (const kind of KINDS) observers[kind].confirmForegroundBootstrap()
        const inWindow = (kind) =>
          observers[kind]
            .accepted({ uuid: ids[kind] })
            .filter((p) => p.writeAt >= start && p.responseAt <= deadline && p.metricsBytes > 0)
        for (const kind of KINDS) {
          const gets = observers[kind].subscriptions({ uuid: ids[kind] }).length
          expect(await clients[kind].clientInstanceId({ timeout: 3000 })).toBe(ids[kind])
          expect(observers[kind].subscriptions({ uuid: ids[kind] })).toHaveLength(gets)
        }
        const discoveryCheckpoint = observers.admin.checkpoint()
        const config = await clients.admin.describeConfigs({
          timeout: 3000,
          resources: [{ type: 4, name: '1', configNames: ['node.id'] }],
        })
        expect(config.resources).toEqual([
          expect.objectContaining({
            resourceType: 4,
            resourceName: '1',
            errorCode: 0,
          }),
        ])
        const discovery = observers.admin.normalRequests({ after: discoveryCheckpoint })
        const metadata = discovery.filter((r) => r.apiKey === 3)
        expect(metadata).toHaveLength(1)
        const knownAfter = metadata[0].response.responseAt
        expect(discovery.some((r) => r.apiKey === 32 && r.writeAt > knownAfter)).toBe(true)
        const discovered = clock.now()
        const adminAfter = (since) =>
          observers.admin.accepted({ uuid: ids.admin }).filter((p) => p.writeAt > since)
        await pollUntil(
          () => adminAfter(discovered).length >= 2,
          clock,
          deadline,
          'quiescent samples'
        )

        const checkpoint = observers.admin.checkpoint()
        const from = events.admin.length
        const controlled = clock.now()
        const controlledCluster = await clients.admin.describeCluster({ timeout: 3000 })
        expect(
          controlledCluster.brokers
            .map((broker) => [broker.nodeId, broker.host, broker.port])
            .sort()
        ).toEqual(PORTS.configured.map((port, i) => [i + 1, 'localhost', port]))
        const requestEvents = events.admin.slice(from)
        await pollUntil(
          () =>
            adminAfter(controlled).length >= 3 && KINDS.every((kind) => inWindow(kind).length >= 2),
          clock,
          deadline,
          'two accepted pushes per kind and the controlled samples'
        )
        expect(clock.now()).toBeLessThanOrEqual(deadline)
        const controlledPushes = adminAfter(controlled).slice(0, 3)
        attempt.stage = 'verification'
        const decodedByRequest = Object.fromEntries(KINDS.map((kind) => [kind, new Map()]))
        const wire = observers.admin.normalRequests({ after: checkpoint })
        await closeAll()

        for (const kind of KINDS) {
          const observer = observers[kind]
          expect(observer.undecided()).toEqual([])
          observer.assertHealthy()
          const id = ids[kind]
          expect(id).toMatch(UUID_FORMAT)
          expect(id).not.toBe(ZERO_UUID)
          const subscription = observer.subscriptions({ uuid: id }).find((r) => r.response)
          expect(subscription.response).toMatchObject({
            errorCode: 0,
            pushIntervalMs: 1000,
            telemetryMaxBytes: 4096,
            requestedMetrics: [PREFIX],
          })
          const delta = subscription.response.deltaTemporality
          const accepted = inWindow(kind)
          expect(accepted.length).toBeGreaterThanOrEqual(2)
          const subscriptionIds = new Set(
            observer
              .subscriptions({ uuid: id })
              .filter((r) => r.response?.errorCode === 0)
              .map((r) => r.response.subscriptionId)
          )
          expect(
            accepted.every(
              (p) =>
                p.version === 0 &&
                p.errorCode === 0 &&
                !p.terminating &&
                p.writeAt >= start &&
                p.responseAt <= deadline &&
                subscriptionIds.has(p.subscriptionId)
            )
          ).toBe(true)
          const decoded = []
          const allNormalPushes = observer
            .readTranscript()
            .filter(
              (r) => r.apiKey === 72 && !r.terminating && r.writeAt <= accepted.at(-1).writeAt
            )
          expect(allNormalPushes.map((r) => r.requestKey)).toEqual(
            accepted.map((p) => p.requestKey)
          )
          for (const push of accepted) {
            const item = await observer.decodeAccepted(push)
            decodedByRequest[kind].set(push.requestKey, item)
            decoded.push({ ...item, writeAt: push.writeAt })
          }
          const exportsOfKind = reader.readExports({ uuid: id })
          for (const item of decoded) {
            const row = exportsOfKind.find(
              (e) => e.node === item.exportIdentity.node && e.seq === item.exportIdentity.seq
            )
            expect(row).toMatchObject({
              clientInstanceId: id,
              pushIntervalMs: 1000,
              parse: 'ok',
              terminating: false,
            })
            expect(item.rawBytes).toBeLessThanOrEqual(4096)
            expect(item.transmittedBytes).toBeLessThanOrEqual(4096)
            checkDecoded(kind, stem, item.decoded, delta)
          }
          expect(new Set(decoded.map((p) => JSON.stringify(p.exportIdentity))).size).toBe(
            decoded.length
          )
          const normal = joinNormal(
            observer,
            events[kind],
            kind === 'admin' ? knownAfter : Infinity
          )
          if (kind !== 'admin')
            expect(normal.every((r) => [17, 18, 36].includes(r.apiKey))).toBe(true)
          const arithmetic = verifyMetricWindows({
            kind,
            samples: decoded,
            requests: normal,
            connects: observer.normalConnects(),
            delta,
          })
          results[kind] = {
            clientId: `${stem}-${kind}`,
            uuid: id,
            requestKeys: accepted.map((p) => p.requestKey),
            exports: decoded.map((p) => p.exportIdentity),
            rawSha256: decoded.map((p) => p.rawSha256),
            arithmetic,
          }
        }

        const decodedControlled = []
        for (const push of controlledPushes)
          decodedControlled.push(decodedByRequest.admin.get(push.requestKey).decoded)
        const samples = decodedControlled.map((d) => nodeGauges(d, 'admin'))
        const joined = requestEvents.map((event) => {
          const node = NODE_OF_PORT[Number(event.broker.split(':').pop())]
          const found = wire.filter(
            (w) =>
              w.node === node &&
              w.correlation === event.correlationId &&
              w.apiKey === event.apiKey &&
              w.apiVersion === event.apiVersion
          )
          expect(found).toHaveLength(1)
          expect(found[0].writeAt).toBeGreaterThan(knownAfter)
          return { ...event, requestKey: found[0].requestKey, node }
        })
        expect(joined).toHaveLength(wire.length)
        const emitted = new Set([...Object.keys(samples[0]), ...Object.keys(samples[1])])
        expect(emitted.size).toBeGreaterThan(0)
        expect([...emitted].sort()).toEqual([...new Set(joined.map((r) => r.node))].sort())
        const controlSamples = controlledPushes.map((push, i) => ({
          requestKey: push.requestKey,
          writeAt: push.writeAt,
          decoded: decodedControlled[i],
        }))
        const controlRequests = joinNormal(observers.admin, events.admin, knownAfter).filter((r) =>
          joined.some((j) => j.requestKey === r.requestKey)
        )
        // The full audit above accounts for every window; this ledger isolates the mandatory control.
        results.control = {
          discovery,
          cluster: controlledCluster,
          knownAfter,
          checkpoint,
          controlledAt: controlled,
          requestEvents,
          wire,
          requests: controlRequests,
          samples: controlSamples.map((p) => p.requestKey),
        }
        expect(samples[2]).toEqual({})
        results.window = { start, deadline }
        results.resource = { name: stem, entries, deleted: true }
      })

      publish({
        name: 'telemetry-reporter',
        directory,
        observers,
        record: {
          wireVersions: { 71: 0, 72: 0 },
          run: {
            stem,
            line,
            project: process.env.COMPOSE_PROJECT_NAME,
            composeSha256: fileSha(compose),
          },
          fixture: {
            manifestSha256: fileSha(
              path.join(ROOT, 'scripts/e2e/telemetry/fixtures/manifest.json')
            ),
            reporterJar: jar,
            client: {
              packageName: pkg.name,
              version: pkg.version,
              rootSha256: fileSha(path.join(ROOT, 'index.js')),
            },
          },
          results,
        },
      })
    }),
  90000
)

live(
  'reporter no-resource control',
  () =>
    runCase('telemetry-reporter-no-resource', async (cleanup, publish, attempt) => {
      const state = await setup('configured', cleanup)
      const { stem, line, directory, clock, aux } = state
      const { observers, clients, closeAll } = buildClients(
        { mode: 'configured', stem, line, directory, clock, enabled: true },
        cleanup
      )
      await settle(KINDS.map((kind) => () => clients[kind].connect()))
      attempt.stage = 'capture'
      const ids = {}
      attempt.record.uuids = ids
      await settle(
        KINDS.map((kind) => async () => {
          ids[kind] = await clients[kind].clientInstanceId({ timeout: 3000 })
        })
      )
      expect(new Set(Object.values(ids)).size).toBe(4)
      const deadline = clock.now() + 10000
      for (const kind of KINDS)
        await pollUntil(
          () => observers[kind].subscriptions({ uuid: ids[kind] }).some((r) => r.response),
          clock,
          deadline,
          'the empty subscription answer'
        )
      await new Promise((resolve) => setTimeout(resolve, 3000))
      for (const kind of KINDS) {
        const observer = observers[kind]
        expect(ids[kind]).toMatch(UUID_FORMAT)
        expect(ids[kind]).not.toBe(ZERO_UUID)
        expect(await clients[kind].clientInstanceId({ timeout: 3000 })).toBe(ids[kind])
        const gets = observer.subscriptions({ uuid: ids[kind] })
        expect(gets).toHaveLength(1)
        expect(gets[0].apiVersion).toBe(0)
        expect(gets[0].response).toMatchObject({
          errorCode: 0,
          pushIntervalMs: 300000,
          requestedMetrics: [],
        })
        expect(observer.readTranscript().some((r) => r.apiKey === 72)).toBe(false)
        observer.assertHealthy()
      }
      const listed = await aux.listConfigResources({ resourceTypes: [16], timeout: 3000 })
      expect(listed.resources.filter((r) => r.name === stem)).toEqual([])
      await closeAll()
      publish({
        name: 'telemetry-reporter-no-resource',
        directory,
        observers,
        record: {
          wireVersions: { 71: 0 },
          run: { stem, line, project: process.env.COMPOSE_PROJECT_NAME },
          uuids: ids,
        },
      })
    }),
  60000
)

live(
  'reporter no-plugin control',
  () =>
    runCase('telemetry-reporter-no-plugin', async (cleanup, publish, attempt) => {
      const state = await setup('base', cleanup)
      const { stem, line, directory, clock } = state
      const on = buildClients(
        { mode: 'base', stem, line, directory, clock, enabled: true },
        cleanup
      )
      const off = buildClients(
        { mode: 'base', stem, line, directory, clock, enabled: false, suffix: '-off' },
        cleanup
      )
      const outcome = async (client) => {
        try {
          await client.connect()
          return { status: 'connected' }
        } catch (error) {
          return {
            status: 'rejected',
            name: error?.name,
            message: error?.message,
            reason: error?.reason,
            apiKey: error?.apiKey,
            shareVersion: error?.shareVersion,
          }
        }
      }
      attempt.stage = 'capture'
      const enabled = {}
      const disabled = {}
      attempt.record.outcomes = enabled
      attempt.record.disabledOutcomes = disabled
      await settle(
        KINDS.flatMap((kind) => [
          async () => {
            enabled[kind] = await outcome(on.clients[kind])
          },
          async () => {
            disabled[kind] = await outcome(off.clients[kind])
          },
        ])
      )
      const reasons = {}
      for (const kind of KINDS) {
        expect(enabled[kind]).toEqual(disabled[kind])
        const oldShare = kind === 'share' && ['4.0.2', '4.1.2'].includes(line)
        if (oldShare) {
          expect(enabled[kind]).toMatchObject({
            status: 'rejected',
            name: 'KafkaJSShareGroupsNotSupportedError',
            reason: line === '4.0.2' ? 'api-not-advertised' : 'api-version-range',
            apiKey: line === '4.0.2' ? 76 : 78,
            shareVersion: 0,
          })
        } else expect(enabled[kind]).toEqual({ status: 'connected' })
        for (const [group, reason] of [
          [on, oldShare ? 'not-connected' : 'unsupported'],
          [off, 'disabled'],
        ]) {
          const error = await group.clients[kind].clientInstanceId({ timeout: 3000 }).then(
            () => null,
            (e) => e
          )
          expect(error).toBeInstanceOf(KafkaJSTelemetryError)
          expect(error.reason).toBe(reason)
        }
        const adverts = on.observers[kind].advertisements()
        expect(adverts.length).toBeGreaterThan(0)
        expect(adverts.every((a) => a.ranges[71] === null && a.ranges[72] === null)).toBe(true)
        reasons[kind] = oldShare ? 'not-connected' : 'unsupported'
      }
      for (const kind of KINDS) {
        for (const group of [on, off]) {
          const observer = group.observers[kind]
          expect(observer.subscriptions()).toEqual([])
          expect(observer.telemetrySockets()).toEqual([])
          expect(observer.readTranscript().some((r) => r.apiKey === 71 || r.apiKey === 72)).toBe(
            false
          )
          const adverts = observer.advertisements()
          expect(adverts.length).toBeGreaterThan(0)
          expect(adverts.every((a) => a.ranges[71] === null && a.ranges[72] === null)).toBe(true)
          observer.assertHealthy()
        }
        const dials = (group) =>
          group.observers[kind].readTranscript().filter((r) => r.type === 'connect')
        expect(dials(on).length).toBeGreaterThan(0)
        expect(dials(on)).toHaveLength(dials(off).length)
      }
      await on.closeAll()
      await off.closeAll()
      publish({
        name: 'telemetry-reporter-no-plugin',
        directory,
        observers: {
          ...on.observers,
          ...Object.fromEntries(KINDS.map((k) => [k + '-off', off.observers[k]])),
        },
        record: {
          wireVersions: {},
          run: { stem, line, project: process.env.COMPOSE_PROJECT_NAME },
          outcomes: enabled,
          disabledOutcomes: disabled,
          reasons,
        },
      })
    }),
  60000
)
