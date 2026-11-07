const crypto = require('crypto')
const fs = require('fs')
const net = require('net')
const path = require('path')
const { performance } = require('perf_hooks')
const { Kafka, logLevel, KafkaJSTelemetryError } = require('../../../index')
const { writeEvidence } = require('../../../testHelpers/e2eEvidence')
const {
  TelemetryExportReader,
  TelemetryWireObserver,
  withTelemetryResource,
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
const RACK = 'rack-f07'
const WINDOW_MS = 15000
const PREFIX = 'org.apache.kafka.'
const ZERO_UUID = '00000000-0000-0000-0000-000000000000'
const UUID_FORMAT = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
const UINT64_MAX = 2n ** 64n - 1n

/**
 * Owns the resources of one case: tasks run last-in first-out, all of them, whatever fails.
 */
class Cleanup {
  #tasks = []

  /**
   * @param {function():Promise<void>|void} task released when the case ends
   */
  defer(task) {
    this.#tasks.push(task)
  }

  /**
   * Runs every task and rethrows the case failure first, then the cleanup failures in order.
   * @param {boolean} failed whether the case body threw
   * @param {*} primary what the body threw, possibly a falsy value
   * @returns {Promise<void>} when the case and its cleanup both succeeded
   * @throws {*} the single failure, or an AggregateError of all of them
   */
  async run(failed, primary) {
    const errors = failed ? [primary] : []
    while (this.#tasks.length) {
      try {
        await this.#tasks.pop()()
      } catch (error) {
        errors.push(error)
      }
    }
    if (errors.length === 1) throw errors[0]
    if (errors.length) throw new AggregateError(errors, 'Telemetry case and cleanup failed')
  }
}

/**
 * @param {function(Cleanup):Promise<void>} body the case
 * @returns {Promise<void>} after the body and all its cleanup
 */
const runCase = async (body) => {
  const cleanup = new Cleanup()
  let failed = false
  let primary
  try {
    await body(cleanup)
  } catch (error) {
    failed = true
    primary = error
  }
  await cleanup.run(failed, primary)
}

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
  const stem = 'telemetry-reporter-' + crypto.randomBytes(8).toString('hex')
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
  for (const kind of KINDS) {
    const observer = new TelemetryWireObserver({
      directory,
      line,
      clock,
      mode,
      owner: kind + suffix,
    })
    observers[kind] = observer
    const kafka = new Kafka({
      clientId: `${stem}-${kind}${suffix}`,
      brokers: PORTS[mode].map((port) => `localhost:${port}`),
      socketFactory: (options) => observer.socketFactory(options),
      logLevel: logLevel.NOTHING,
    })
    const common = { enableMetricsPush: enabled }
    if (kind === 'producer') clients[kind] = kafka.producer({ ...common, rackId: RACK })
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
  cleanup.defer(() =>
    settle(
      KINDS.map((kind) => async () => {
        const observer = observers[kind]
        const unanswered = observer
          .readTranscript()
          .filter((r) => r.type === 'request' && r.apiKey === 72 && r.terminating && !r.response)
          .map((r) => r.requestKey)
        observer.assertClean({ allowUnanswered: unanswered })
      })
    )
  )
  cleanup.defer(() => settle(KINDS.map((kind) => () => observers[kind].dispose())))
  let closing
  const closeAll = () => (closing ??= settle(KINDS.map((kind) => () => clients[kind].disconnect())))
  cleanup.defer(closeAll)
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
 * @param {number[]} durations observed completed durations in milliseconds
 * @param {{avg:number,max:number}} pair emitted gauge pair
 * @returns {boolean} whether the pair is exactly their mean and maximum
 */
const matches = (durations, pair) => {
  const mean = durations.reduce((a, b) => a + b, 0) / durations.length
  return (
    Math.abs(pair.avg - mean) <= 1e-9 + 1e-12 * Math.abs(mean) &&
    pair.max === Math.max(...durations)
  )
}

/**
 * Requires the emitted gauge pairs of one node over two ordered samples to account for every
 * observed duration of that node, with no event dropped or reused.
 * @param {number[]} durations time-ordered completed durations for the node
 * @param {Array<{avg:number,max:number}>} pairs the node's emitted pairs, oldest first
 * @returns {boolean} whether a feasible time-ordered partition exists
 */
const accounts = (durations, pairs) => {
  if (pairs.length === 1) return matches(durations, pairs[0])
  for (let split = 1; split < durations.length; split++)
    if (matches(durations.slice(0, split), pairs[0]) && matches(durations.slice(split), pairs[1]))
      return true
  return false
}

live(
  'reporter export',
  () =>
    runCase(async (cleanup) => {
      const state = await setup('configured', cleanup)
      const { stem, line, directory, clock, aux } = state
      const reader = new TelemetryExportReader(directory)
      for (const node of ['1', '2', '3']) {
        const proof = JSON.parse(
          fs.readFileSync(path.join(directory, `reporter-load-proof-${node}.json`), 'utf8')
        )
        expect(proof).toMatchObject({ node, class: MANIFEST.classes.reporter, kafkaClients: line })
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
      const events = []
      cleanup.defer(
        clients.admin.on(clients.admin.events.REQUEST, (event) => events.push(event.payload))
      )
      const entries = {
        metrics: PREFIX,
        'interval.ms': '1000',
        match: `client_id=^${stem}-(producer|consumer|admin|share)$`,
      }
      const results = {}

      await withTelemetryResource(aux, { name: stem, entries }, async () => {
        const start = clock.now()
        const deadline = start + WINDOW_MS
        await settle(KINDS.map((kind) => () => clients[kind].connect()))
        const ids = {}
        await settle(
          KINDS.map((kind) => async () => {
            ids[kind] = await clients[kind].clientInstanceId({ timeout: 3000 })
          })
        )
        const inWindow = (kind) =>
          observers[kind]
            .accepted({ uuid: ids[kind] })
            .filter((p) => p.writeAt >= start && p.responseAt <= deadline && p.metricsBytes > 0)
        for (const kind of KINDS) {
          const gets = observers[kind].subscriptions({ uuid: ids[kind] }).length
          expect(await clients[kind].clientInstanceId({ timeout: 3000 })).toBe(ids[kind])
          expect(observers[kind].subscriptions({ uuid: ids[kind] })).toHaveLength(gets)
        }
        await clients.admin.describeCluster({ timeout: 3000 })
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
        const from = events.length
        const controlled = clock.now()
        await clients.admin.describeCluster({ timeout: 3000 })
        const requestEvents = events.slice(from)
        await pollUntil(
          () =>
            adminAfter(controlled).length >= 3 && KINDS.every((kind) => inWindow(kind).length >= 2),
          clock,
          deadline,
          'two accepted pushes per kind and the controlled samples'
        )
        expect(clock.now()).toBeLessThanOrEqual(deadline)
        const controlledPushes = adminAfter(controlled).slice(0, 3)
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
          for (const push of accepted) decoded.push(await observer.decodeAccepted(push))
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
          const sums = decoded.map((item) =>
            BigInt(
              item.decoded.metrics.find(
                (m) => m.name === prefixOf(kind) + 'connection.creation.total'
              ).points[0].asInt
            )
          )
          const last = accepted[accepted.length - 1]
          const connects = observer.normalConnects().filter((c) => c.at < last.writeAt).length
          const total = delta ? sums.reduce((a, b) => a + b, 0n) : sums[sums.length - 1]
          expect(total).toBe(BigInt(connects))
          results[kind] = {
            clientId: `${stem}-${kind}`,
            uuid: id,
            requestKeys: accepted.map((p) => p.requestKey),
            exports: decoded.map((p) => p.exportIdentity),
            rawSha256: decoded.map((p) => p.rawSha256),
            connects,
          }
        }

        const decodedControlled = []
        for (const push of controlledPushes)
          decodedControlled.push((await observers.admin.decodeAccepted(push)).decoded)
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
          return { node, duration: event.duration, sentAt: event.sentAt }
        })
        expect(joined).toHaveLength(wire.length)
        const emitted = new Set([...Object.keys(samples[0]), ...Object.keys(samples[1])])
        expect(emitted.size).toBeGreaterThan(0)
        for (const node of emitted) {
          const durations = joined
            .filter((e) => e.node === node)
            .sort((a, b) => a.sentAt - b.sentAt)
            .map((e) => e.duration)
          const pairs = [samples[0][node], samples[1][node]].filter(Boolean)
          expect(accounts(durations, pairs)).toBe(true)
        }
        expect(samples[2]).toEqual({})
        results.window = { start, deadline }
      })

      writeEvidence('telemetry-reporter', {
        wireVersions: { 71: 0, 72: 0 },
        run: { stem, line, project: 'kafkajs', composeSha256: fileSha(compose) },
        fixture: {
          manifestSha256: fileSha(path.join(ROOT, 'scripts/e2e/telemetry/fixtures/manifest.json')),
          reporterJar: jar,
        },
        results,
      })
    }),
  90000
)

live(
  'reporter no-resource control',
  () =>
    runCase(async (cleanup) => {
      const state = await setup('configured', cleanup)
      const { stem, line, directory, clock, aux } = state
      const { observers, clients, closeAll } = buildClients(
        { mode: 'configured', stem, line, directory, clock, enabled: true },
        cleanup
      )
      await settle(KINDS.map((kind) => () => clients[kind].connect()))
      const ids = {}
      await settle(
        KINDS.map((kind) => async () => {
          ids[kind] = await clients[kind].clientInstanceId({ timeout: 3000 })
        })
      )
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
      writeEvidence('telemetry-reporter-no-resource', {
        wireVersions: { 71: 0 },
        run: { stem, line, project: 'kafkajs' },
        uuids: ids,
      })
    }),
  60000
)

live(
  'reporter no-plugin control',
  () =>
    runCase(async (cleanup) => {
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
          return { status: 'rejected', name: error?.name, message: error?.message }
        }
      }
      const enabled = {}
      const disabled = {}
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
        if (enabled[kind].status !== 'connected') continue
        for (const [group, reason] of [
          [on, 'unsupported'],
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
        reasons[kind] = 'unsupported'
      }
      for (const kind of KINDS) {
        const observer = on.observers[kind]
        expect(observer.subscriptions()).toEqual([])
        expect(observer.telemetrySockets()).toEqual([])
        expect(observer.readTranscript().some((r) => r.apiKey === 71 || r.apiKey === 72)).toBe(
          false
        )
        const dials = (group) =>
          group.observers[kind].readTranscript().filter((r) => r.type === 'connect')
        expect(dials(on)).toHaveLength(dials(off).length)
        observer.assertHealthy()
      }
      await on.closeAll()
      await off.closeAll()
      writeEvidence('telemetry-reporter-no-plugin', {
        wireVersions: {},
        run: { stem, line, project: 'kafkajs' },
        outcomes: enabled,
        reasons,
      })
    }),
  60000
)
