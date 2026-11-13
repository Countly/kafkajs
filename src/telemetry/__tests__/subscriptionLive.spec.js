const crypto = require('crypto')
const fs = require('fs')
const net = require('net')
const os = require('os')
const path = require('path')
const zlib = require('zlib')
const { performance } = require('perf_hooks')
const { isDeepStrictEqual } = require('util')
const { spawn } = require('child_process')
const { Kafka, logLevel } = require('../../../index')
const { assertCadence, assertCustomShape } = require('../../../scripts/e2e/telemetry/semantics')
const { runCase } = require('../../../scripts/e2e/telemetry/evidence')
const {
  TelemetryExportReader,
  TelemetryWireObserver,
  withTelemetryResource,
} = require('../../../testHelpers/telemetryFixtures')
const {
  bounded,
  caseTimeout,
  liveCase,
  liveRun,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')

// Gated: the configured broker telemetry fixture (docker-compose.telemetry.yml) and the telemetry CI
// lanes. A configured failure always fails; the only self-skip is the absence of the outer opt-in.
const live = process.env.TELEMETRY_E2E ? test : test.skip

const ROOT = path.resolve(__dirname, '../../..')
const MANIFEST_FILE = path.join(ROOT, 'scripts/e2e/telemetry/fixtures/manifest.json')
const PROBE_FILE = path.join(ROOT, 'scripts/e2e/telemetry/telemetryWireProbe.js')
const PROBE_SHA256 = '139afdbec5034f7c1498b90306889fd918ae13097efee1ea9f435b03dcf9b1b1'
const PORTS = [19092, 19095, 19098]
const NO_MATCH = 'never.matches.telemetry.'
const PRODUCER_PREFIX = 'org.apache.kafka.producer.'
const CONNECTION_TOTAL = 'org.apache.kafka.producer.connection.creation.total'
const PAYLOAD_PREFIX = 'com.example.payload.'
const SERIES = 128
const CAP = 4096
const clock = { now: () => performance.now() }

/** The F04 finite seams that carry the runtime-error recoveries a live broker cannot be made to produce. */
const STRICT_SEAMS = Object.freeze([
  ['src/telemetry/__tests__/errors.spec.js', 'halves the effective budget and retains its penalty'],
  ['src/telemetry/__tests__/errors.spec.js', 'allows one immediate refresh for push error %i'],
  ['src/telemetry/__tests__/errors.spec.js', 'waits for both interval and quota after code89'],
  ['src/telemetry/__tests__/errors.spec.js', 'stops ordinary streams on fatal code %i'],
  ['src/telemetry/__tests__/errors.spec.js', 'consumes the single corrective allowance'],
])

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
 * Runs one child under `timeout`, whose own process group the case can prove empty afterwards.
 * Output is capped; every observer is attached before the child can emit.
 */
class Child {
  /**
   * @param {string[]} argv command after `timeout -k 1s 20s`
   * @param {object} [env] child environment
   */
  constructor(argv, env = process.env) {
    this.child = spawn('timeout', ['-k', '1s', '20s', ...argv], {
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    this.stdout = ''
    this.stderr = ''
    this.child.stdout.setEncoding('utf8')
    this.child.stderr.setEncoding('utf8')
    this.done = new Promise((resolve) => {
      const result = (extra) => ({
        code: null,
        signal: null,
        stdout: this.stdout,
        stderr: this.stderr,
        ...extra,
      })
      this.child.stdout.on('data', (d) => (this.stdout = (this.stdout + d).slice(0, 65536)))
      this.child.stderr.on('data', (d) => (this.stderr = (this.stderr + d).slice(0, 65536)))
      this.child.once('error', (error) => resolve(result({ error })))
      this.child.once('close', (code, signal) => resolve(result({ code, signal })))
    })
  }

  /**
   * @returns {Promise<boolean>} whether the supervising group has gone, probed for one second
   */
  async groupGone() {
    for (let attempt = 0; attempt < 20; attempt++) {
      try {
        process.kill(-this.child.pid, 0)
      } catch (error) {
        return error.code === 'ESRCH'
      }
      await new Promise((resolve) => setTimeout(resolve, 50))
    }
    return false
  }
}

/**
 * Owns the clients, resource, observers and children of one subscription scenario. `close` is
 * idempotent, attempts every release in reverse order and taints the live run on any failure.
 */
class SubscriptionCases {
  #tasks = []
  #closing = null

  /**
   * @param {object} options case state
   * @param {AbortSignal} options.signal the case phase scope
   * @param {string} options.line broker line
   * @param {string} options.directory absolute evidence directory
   * @param {string} options.stem unique resource and client prefix
   * @param {object} options.attempt mutable evidence of the running case
   */
  constructor({ signal, line, directory, stem, attempt }) {
    this.signal = signal
    this.line = line
    this.directory = directory
    this.stem = stem
    this.attempt = attempt
    this.reader = new TelemetryExportReader(directory)
    this.pushes = []
    this.pushKeys = new Set()
  }

  /**
   * Records one accepted Push whose decode and sink join the receiver re-verifies, once.
   * @param {string} owner observer owner key
   * @param {string} requestKey the captured request
   * @returns {void}
   */
  evidence(owner, requestKey) {
    if (this.pushKeys.has(`${owner}:${requestKey}`)) return
    this.pushKeys.add(`${owner}:${requestKey}`)
    this.pushes.push({ owner, requestKey })
  }

  /**
   * @param {function():Promise<void>|void} task release attempted by `close`
   * @returns {void}
   */
  defer(task) {
    this.#tasks.push(task)
  }

  /**
   * @returns {Promise<void>} once every release was attempted
   * @throws {Error|AggregateError} failures in release order; the live run is tainted first
   */
  close() {
    this.#closing ??= (async () => {
      const errors = []
      while (this.#tasks.length) {
        try {
          await this.#tasks.pop()()
        } catch (error) {
          errors.push(error)
        }
      }
      if (errors.length) {
        liveRun.taint(errors[0])
        throw errors.length === 1 ? errors[0] : new AggregateError(errors, 'Cleanup failed')
      }
    })()
    return this.#closing
  }

  /**
   * @param {number} ms bound of this one operation
   * @param {string} label failure description
   * @param {function(AbortSignal):*} op the operation
   * @returns {Promise<*>} its result
   */
  within(ms, label, op) {
    return bounded(this.signal, ms, op, { label })
  }

  /**
   * Polls a synchronous or asynchronous lookup until it returns a truthy value.
   * @param {string} label failure description
   * @param {number} ms poll budget
   * @param {function():*} lookup returns the value, or a falsy value when not yet
   * @returns {Promise<*>} the truthy lookup result
   */
  async find(label, ms, lookup) {
    let found
    await pollUntil(this.signal, Math.max(1, ms), async () => (found = await lookup()), {
      delayMs: 100,
      label: `Waiting for ${label}`,
    })
    return found
  }

  /**
   * Connects the telemetry-disabled auxiliary admin and checks Metadata host:port/node identity.
   * @returns {Promise<void>} when the fixture topology is confirmed
   */
  async start() {
    const kafka = new Kafka({
      clientId: this.stem + '-aux',
      brokers: PORTS.map((port) => `localhost:${port}`),
      socketFactory: ipv4Factory,
      logLevel: logLevel.NOTHING,
    })
    this.aux = kafka.admin({ enableMetricsPush: false, apiTimeout: 3000 })
    this.defer(() => this.aux.disconnect())
    await this.within(10000, 'The auxiliary admin connect', () => this.aux.connect())
    const cluster = await this.within(5000, 'The topology check', () =>
      this.aux.describeCluster({ timeout: 3000 })
    )
    expect(cluster.brokers.map((b) => [b.nodeId, b.host, b.port]).sort()).toEqual(
      PORTS.map((port, index) => [index + 1, 'localhost', port])
    )
  }

  /**
   * Builds one telemetry-enabled producer on an observed socket factory. Cleanup runs in the fixed
   * order disconnect, dispose, require clean; each unanswered request needs an exact cancellation
   * joined to the captured public-close boundary.
   * @param {object} [options] producer options beyond `enableMetricsPush`
   * @param {string} [suffix] client-id and owner suffix
   * @returns {{observer:object,client:object,clientId:string,owner:string}} the observed client
   */
  producer(options = {}, suffix = '') {
    const owner = `producer${suffix}`
    const observer = new TelemetryWireObserver({
      completeStream: true,
      directory: this.directory,
      line: this.line,
      clock,
      mode: 'configured',
      owner,
    })
    this.attempt.observers[owner] = observer
    const clientId = `${this.stem}-producer${suffix}`
    const kafka = new Kafka({
      clientId,
      brokers: PORTS.map((port) => `localhost:${port}`),
      socketFactory: (socketOptions) => observer.socketFactory(socketOptions),
      logLevel: logLevel.NOTHING,
    })
    const client = kafka.producer({ enableMetricsPush: true, idempotent: false, ...options })
    this.defer(() => observer.assertClean({ allowUnanswered: observer.cancellationKeys() }))
    this.defer(() => observer.dispose())
    this.defer(() => observer.closeClient(client))
    return { observer, client, clientId, owner }
  }

  /**
   * Changes the owned resource with SET and requires exact per-resource success and readback.
   * An empty list is read back as empty whether the broker answers an empty string or null.
   * @param {Object<string,string>} entries config values to SET
   * @returns {Promise<void>} once the broker shows the values
   */
  async alter(entries) {
    const resource = { type: 16, name: this.stem }
    const answer = await this.within(8000, 'The resource update', () =>
      this.aux.incrementalAlterConfigs({
        timeout: 3000,
        resources: [
          {
            ...resource,
            configEntries: Object.entries(entries).map(([name, value]) => ({
              name,
              operation: 0,
              value,
            })),
          },
        ],
      })
    )
    expect(answer.resources).toEqual([
      expect.objectContaining({ resourceType: 16, resourceName: this.stem, errorCode: 0 }),
    ])
    const valuesOf = (answer) =>
      Object.fromEntries(
        (answer.resources[0]?.configEntries ?? []).map((e) => [e.configName, e.configValue ?? ''])
      )
    // A describe may reach a broker that has not applied the controller's new record yet.
    const read = await this.find('The resource readback of the altered values', 8000, async () => {
      const answer = await this.aux.describeConfigs({
        timeout: 3000,
        resources: [{ ...resource, configNames: Object.keys(entries) }],
      })
      return isDeepStrictEqual(valuesOf(answer), entries) && answer
    })
    expect(read.resources).toHaveLength(1)
    expect(read.resources[0]).toMatchObject({ resourceName: this.stem, errorCode: 0 })
    const actual = Object.fromEntries(
      read.resources[0].configEntries.map((e) => [e.configName, e.configValue ?? ''])
    )
    expect(actual).toEqual(entries)
    const listed = await this.find(
      'The resource listing of the altered resource',
      8000,
      async () => {
        const answer = await this.aux.listConfigResources({ resourceTypes: [16], timeout: 3000 })
        return answer.resources.some((r) => r.name === this.stem) && answer
      }
    )
    expect(listed.resources.filter((r) => r.name === this.stem)).toEqual([
      { type: 16, name: this.stem },
    ])
  }

  /**
   * @param {object} options resource entries and the scenario to run
   * @param {Object<string,string>} options.entries initial resource entries
   * @param {function():Promise<void>} work scenario body run after verified creation
   * @returns {Promise<void>} after the resource is deleted and absence is read back
   */
  async withResource(entries, work) {
    const receipt = await withTelemetryResource(this.aux, { name: this.stem, entries }, work, {
      signal: this.signal,
    })
    const [owner] = Object.keys(this.attempt.observers)
    this.attempt.record.results.subscriptionDeletions = { [owner]: receipt }
  }

  /**
   * Runs exactly one of the three scenarios.
   * @param {'prefix'|'custom'|'payload'} kind the scenario
   * @returns {Promise<void>} when the scenario and its evidence are complete
   */
  async run(kind) {
    await this.start()
    if (kind === 'prefix') return this.prefix()
    if (kind === 'custom') return this.custom()
    if (kind === 'payload') return this.payload()
    throw new Error(`Unknown subscription scenario ${kind}`)
  }

  /**
   * L2: a non-matching prefix sends a zero-byte Push on the wire (no exporter row exists for it),
   * a prefix change goes 117 then Get then filtered Push, and an emptied but still matching
   * resource keeps interval 1000 and polls without a Push. The empty step never deletes the
   * resource, which would select the five-minute no-resource fallback instead.
   * @returns {Promise<void>} when every transition was observed on the wire
   */
  async prefix() {
    const { observer, client, clientId } = this.producer()
    const entries = {
      metrics: NO_MATCH,
      'interval.ms': '1000',
      match: `client_id=^${clientId}$`,
    }
    const results = (this.attempt.record.results = {})
    await this.withResource(entries, async () => {
      await this.within(10000, 'The producer connect', () => client.connect())
      const uuid = await client.clientInstanceId({ timeout: 10000 })
      results.uuid = uuid

      const noMatchGet = await this.find('the non-matching subscription', 10000, () =>
        observer.subscriptions({ uuid }).find((r) => r.response?.errorCode === 0)
      )
      const zero = await this.find('the zero-byte Push', 10000, () =>
        observer.accepted({ uuid, after: noMatchGet.index }).find((p) => p.metricsBytes === 0)
      )
      const noMatch = {
        get: { ...noMatchGet.response, version: noMatchGet.apiVersion },
        push: zero,
      }
      expect(noMatch.get.version).toBe(0)
      expect(noMatch.get.requestedMetrics).toEqual([NO_MATCH])
      expect(noMatch.get.pushIntervalMs).toBe(1000)
      expect(noMatch.get.telemetryMaxBytes).toBeGreaterThan(0)
      expect(noMatch.push.metricsBytes).toBe(0)
      expect(noMatch.push.compressionType).toBe(0)
      expect(noMatch.push.terminating).toBe(false)
      expect(noMatch.push.subscriptionId).toBe(noMatch.get.subscriptionId)
      results.noMatch = noMatch
      results.noMatchGetKey = noMatchGet.requestKey

      const beforeUpdate = observer.checkpoint()
      await this.alter({ metrics: PRODUCER_PREFIX })
      const readbackAt = clock.now()
      const left = () => readbackAt + 10000 - clock.now()
      const refused = await this.find('the old-subscription refusal', left(), () =>
        observer
          .readTranscript({ after: beforeUpdate })
          .find((r) => r.apiKey === 72 && r.response?.errorCode === 117)
      )
      const replacement = await this.find('the replacement Get', left(), () =>
        observer
          .subscriptions({ uuid, after: refused.index })
          .find((r) => r.uuid === uuid && r.response?.errorCode === 0)
      )
      const pushed = await this.find('the filtered Push', left(), () =>
        observer
          .accepted({ uuid, after: replacement.index })
          .find(
            (p) => p.metricsBytes > 0 && p.subscriptionId === replacement.response.subscriptionId
          )
      )
      const decoded = await observer.decodeAccepted(pushed)
      this.evidence('producer', pushed.requestKey)
      const update = {
        oldPush: { errorCode: refused.response.errorCode, throttleMs: refused.response.throttleMs },
        get: { ...replacement.response, requestUuid: replacement.uuid },
        names: decoded.decoded.metrics.map((m) => m.name),
      }
      expect(update.oldPush.errorCode).toBe(117)
      expect(update.get.requestUuid).toBe(uuid)
      expect(update.get.requestedMetrics).toEqual([PRODUCER_PREFIX])
      expect(update.get.subscriptionId).not.toBe(noMatch.get.subscriptionId)
      expect(replacement.socketId).toBe(refused.socketId)
      expect(replacement.writeAt).toBeGreaterThanOrEqual(
        refused.response.responseAt + update.oldPush.throttleMs
      )
      expect(pushed.responseAt - readbackAt).toBeLessThanOrEqual(10000)
      expect(update.names).toContain(CONNECTION_TOTAL)
      expect(update.names.every((name) => name.startsWith(PRODUCER_PREFIX))).toBe(true)
      expect(await client.clientInstanceId({ timeout: 0 })).toBe(uuid)
      results.update = {
        ...update,
        getKey: replacement.requestKey,
        refusedKey: refused.requestKey,
        pushKey: pushed.requestKey,
        readbackAt,
      }

      const beforeEmpty = observer.checkpoint()
      await this.alter({ metrics: '' })
      const emptyBy = clock.now() + 10000
      const refusedAgain = await this.find(
        'the refusal after emptying',
        emptyBy - clock.now(),
        () =>
          observer
            .readTranscript({ after: beforeEmpty })
            .find((r) => r.apiKey === 72 && r.response?.errorCode === 117)
      )
      const emptyGet = await this.find('the empty-selection Get', emptyBy - clock.now(), () =>
        observer
          .subscriptions({ uuid, after: refusedAgain.index })
          .find((r) => r.uuid === uuid && r.response?.errorCode === 0)
      )
      const q0 = emptyGet.response.responseAt
      const nextGet = await this.find(
        'the next poll after one interval',
        emptyBy - clock.now(),
        () =>
          observer
            .subscriptions({ uuid, after: emptyGet.index })
            .find((r) => r.response?.errorCode === 0)
      )
      assertCadence(observer.subscriptions({ uuid, after: emptyGet.index }), q0, 1000)
      const pushesSince = observer
        .readTranscript({ after: emptyGet.index })
        .filter((r) => r.apiKey === 72 && r.index < nextGet.index)
      const empty = {
        get: emptyGet.response,
        nextGet: { ...nextGet.response, writeMs: nextGet.writeAt },
        q0,
        pushWritesInFirstInterval: pushesSince.filter(
          (r) => r.writeAt >= q0 && r.writeAt < q0 + 1000
        ),
        pushesBeforeNextGet: pushesSince,
      }
      expect(empty.get.requestedMetrics).toEqual([])
      expect(empty.get.pushIntervalMs).toBe(1000)
      expect(empty.pushWritesInFirstInterval).toEqual([])
      expect(empty.pushesBeforeNextGet).toEqual([])
      expect(empty.nextGet.requestedMetrics).toEqual([])
      expect(empty.nextGet.pushIntervalMs).toBe(1000)
      expect(empty.nextGet.writeMs - q0).toBeGreaterThanOrEqual(1000)
      expect(await client.clientInstanceId({ timeout: 0 })).toBe(uuid)
      results.empty = { ...empty, pushWritesInFirstInterval: [], pushesBeforeNextGet: [] }
      results.quiet = { owner: 'producer', from: q0, to: q0 + 1000 }
    })
    this.finishRecord(clientId, { allowedErrors: { producer: [117] } })
  }

  /**
   * L3: exact numeric values, an update whose frozen sample may still be written, removal proven
   * only by a request written after the unregister fence and a later sink record on its node, and
   * a re-registration with a fresh origin.
   * @returns {Promise<void>} when the sequence is complete
   */
  async custom() {
    const { observer, client, clientId } = this.producer()
    const attrs = { series: 'live' }
    const metric = (name, type, value) => ({ name, type, value, unit: '1', attributes: attrs })
    const N = (suffix) => `com.example.${suffix}`
    const names = ['anchor', 'zero', 'negative', 'total'].map(N)
    client.registerMetricForSubscription(metric(N('anchor'), 'gauge', 1))
    client.registerMetricForSubscription(metric(N('zero'), 'gauge', 0))
    client.registerMetricForSubscription(metric(N('negative'), 'gauge', -2.5))
    client.registerMetricForSubscription(metric(N('total'), 'sum', 9007199254740993n))
    const entries = {
      metrics: 'com.example.',
      'interval.ms': '1000',
      match: `client_id=^${clientId}$`,
    }
    const results = (this.attempt.record.results = {})
    const point = (decoded, name) => {
      const found = decoded.decoded.metrics.filter((m) => m.name === name)
      expect(found).toHaveLength(1)
      expect(found[0].points).toHaveLength(1)
      return { metric: found[0], ...found[0].points[0] }
    }
    const included = async (after) => {
      const out = []
      for (const p of observer
        .accepted({ uuid: results.uuid, after })
        .filter((x) => x.metricsBytes > 0)) {
        const decoded = await observer.decodeAccepted(p)
        this.evidence('producer', p.requestKey)
        out.push({ push: p, decoded })
      }
      return out
    }
    await this.withResource(entries, async () => {
      await this.within(10000, 'The producer connect', () => client.connect())
      const uuid = (results.uuid = await client.clientInstanceId({ timeout: 10000 }))

      const first = await this.find('the first custom export', 20000, async () => {
        const [head] = await included(0)
        return head
      })
      expect(first.decoded.decoded.metrics.map((m) => m.name).sort()).toEqual([...names].sort())
      const firstPoints = Object.fromEntries(
        names.map((name) => [name.slice('com.example.'.length), point(first.decoded, name)])
      )
      for (const [, p] of Object.entries(firstPoints)) {
        expect(p.metric.unit).toBe('1')
        expect(p.attrs).toEqual([['series', 'string', 'live']])
        expect(BigInt(p.startNs)).toBeLessThanOrEqual(BigInt(p.timeNs))
      }
      expect(firstPoints.total.asInt).toBe('9007199254740993')
      expect(firstPoints.total.metric).toMatchObject({
        kind: 'SUM',
        monotonic: true,
        temporality: 'AGGREGATION_TEMPORALITY_DELTA',
      })
      expect(firstPoints.zero.asDouble).toBe('0.0')
      expect(firstPoints.zero.asDoubleBits).toBe('0')
      expect(firstPoints.negative.asDouble).toBe('-2.5')
      expect(firstPoints.negative.asDoubleBits).toBe('c004000000000000')
      expect(firstPoints.anchor.asDouble).toBe('1.0')
      results.first = firstPoints
      results.firstRequestKey = first.push.requestKey

      client.registerMetricForSubscription(metric(N('zero'), 'gauge', 6.25))
      client.registerMetricForSubscription(metric(N('negative'), 'gauge', -3.5))
      client.registerMetricForSubscription(metric(N('total'), 'sum', 9007199254741002n))
      const updateFence = observer.checkpoint()
      const updatedAt = clock.now()
      const STATES = {
        old: ['0.0', '-2.5', '0'],
        updated: ['6.25', '-3.5', '9'],
        following: ['6.25', '-3.5', '0'],
      }
      const classify = (sample) => {
        assertCustomShape(sample.decoded.decoded, ['anchor', 'negative', 'total', 'zero'].map(N))
        const triple = [
          point(sample.decoded, N('zero')).asDouble,
          point(sample.decoded, N('negative')).asDouble,
          point(sample.decoded, N('total')).asInt,
        ]
        const hit = Object.entries(STATES).find(([, v]) => v.every((x, i) => x === triple[i]))
        if (!hit) throw new Error(`Unexpected post-update values ${JSON.stringify(triple)}`)
        return hit[0]
      }
      let states = []
      let post = []
      await this.find('the updated values and a following zero delta', 25000, async () => {
        post = await included(updateFence)
        states = post.map(classify)
        return /^(old,)?updated,following(,following)*$/.test(states.join(','))
      })
      const updated = post[states.indexOf('updated')]
      expect(updated.push.responseAt - updatedAt).toBeLessThanOrEqual(10000)
      expect(point(updated.decoded, N('total')).asInt).toBe('9')
      expect(point(post[states.indexOf('updated') + 1].decoded, N('total')).asInt).toBe('0')
      results.update = { states, updateFence, requestKeys: post.map((s) => s.push.requestKey) }

      client.unregisterMetricFromSubscription({ name: N('zero'), attributes: attrs })
      client.unregisterMetricFromSubscription({ name: N('total'), attributes: attrs })
      const removeFence = observer.checkpoint()
      const cursor = this.reader.checkpoint()
      const removal = await this.find('a snapshot after the removal fence', 20000, async () => {
        const [head] = await included(removeFence)
        return head
      })
      const removalNames = removal.decoded.decoded.metrics.map((m) => m.name)
      expect(removal.push.index).toBeGreaterThan(removeFence)
      expect(removal.decoded.exportIdentity.seq).toBeGreaterThan(
        cursor[removal.decoded.exportIdentity.node]
      )
      expect(removalNames).toContain(N('anchor'))
      expect(removalNames).not.toContain(N('zero'))
      expect(removalNames).not.toContain(N('total'))
      for (const later of await included(removeFence)) {
        const laterNames = later.decoded.decoded.metrics.map((m) => m.name)
        expect(laterNames).not.toContain(N('zero'))
        expect(laterNames).not.toContain(N('total'))
      }
      results.removal = {
        requestKey: removal.push.requestKey,
        cursor,
        writeOrdinal: removal.push.index,
        removeFence,
        names: removalNames,
      }

      client.registerMetricForSubscription(metric(N('total'), 'sum', 1n))
      const readdFence = observer.checkpoint()
      const readded = await this.find('the re-registered total', 25000, async () =>
        (await included(readdFence)).find((s) =>
          s.decoded.decoded.metrics.some((m) => m.name === N('total'))
        )
      )
      const again = point(readded.decoded, N('total'))
      expect(again.asInt).toBe('1')
      expect(BigInt(again.startNs)).toBeGreaterThan(BigInt(firstPoints.total.startNs))
      expect(await client.clientInstanceId({ timeout: 0 })).toBe(uuid)
      results.readded = {
        requestKey: readded.push.requestKey,
        readdFence,
        startNs: again.startNs,
        asInt: again.asInt,
      }
      results.unregisterFence = observer.checkpoint()
      for (const name of ['anchor', 'negative', 'total'])
        client.unregisterMetricFromSubscription({ name: N(name), attributes: attrs })
    })
    this.finishRecord(clientId, { allowedErrors: {} })
  }

  /**
   * L4: the prepared broker and local cap are 4096 bytes. An independent 4097-byte control with its
   * own UUID proves the broker's size refusal; the candidate registers 128 legal series, whose
   * every accepted Push is whole-point, within the cap raw and transmitted, compressed above 1000
   * bytes and sent once per interval. Runtime-error recoveries are linked strict seams.
   * @returns {Promise<void>} when control, candidate and seam links are recorded
   */
  async payload() {
    expect(fileSha(PROBE_FILE)).toBe(PROBE_SHA256)
    const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-payload-'))
    this.defer(() => fs.rmSync(scratch, { recursive: true, force: true }))
    const { observer, client, clientId } = this.producer({ telemetryMaxBytes: CAP }, '-candidate')
    const rawId = `${this.stem}-raw`
    for (let i = 0; i < SERIES; i++)
      client.registerMetricForSubscription({
        name: `${PAYLOAD_PREFIX}${i}`,
        type: 'gauge',
        value: i,
        unit: '1',
        attributes: { series: String(i), pad: 'x'.repeat(128) },
      })
    const entries = {
      metrics: PAYLOAD_PREFIX,
      'interval.ms': '1000',
      match: `client_id=^(${clientId}|${rawId})$`,
    }
    const results = (this.attempt.record.results = { strictSeams: this.seamLinks() })
    await this.withResource(entries, async () => {
      const probe = async (args) => {
        const child = new Child(['node', PROBE_FILE, ...args])
        const done = await this.within(25000, 'The wire probe', () => child.done)
        expect(await child.groupGone()).toBe(true)
        return {
          ...done,
          lines: done.stdout
            .trim()
            .split('\n')
            .filter(Boolean)
            .map((line) => JSON.parse(line)),
        }
      }
      const advert = await probe(['apiversions', 'localhost', '19092'])
      expect(advert.code).toBe(0)
      expect(advert.lines).toHaveLength(1)
      expect(advert.lines[0]).toMatchObject({ event: 'apiversions', errorCode: 0 })
      expect(advert.lines[0].key71[0]).toBe(0)
      expect(advert.lines[0].key72[0]).toBe(0)
      const vector = path.join(scratch, 'vectors.json')
      fs.writeFileSync(
        vector,
        JSON.stringify({ vectors: { oversize: { hex: '00'.repeat(CAP + 1) } } })
      )
      const control = await probe(['push', 'localhost', '19092', rawId, 'oversize:none', vector])
      expect(control.code).toBe(4)
      expect(control.signal).toBeNull()
      expect(control.lines).toHaveLength(2)
      const [rawGet, rawPush] = control.lines
      expect(rawGet).toMatchObject({ event: 'get', errorCode: 0, fullyConsumed: true })
      expect(rawGet.telemetryMaxBytes).toBe(CAP)
      expect(rawPush).toMatchObject({
        event: 'push',
        errorCode: 118,
        fullyConsumed: true,
        rawBytes: CAP + 1,
        sentBytes: CAP + 1,
        clientInstanceId: rawGet.clientInstanceId,
      })
      const rawControl = {
        uuid: rawGet.clientInstanceId,
        transmittedBytes: rawPush.sentBytes,
        errorCode: rawPush.errorCode,
        rawSha256: rawPush.rawSha256,
        advertisement: advert.lines[0],
      }
      results.rawControl = rawControl
      results.rawOracle = {
        advertisement: advert,
        control,
        vector: JSON.parse(fs.readFileSync(vector)),
        probeSha256: fileSha(PROBE_FILE),
      }

      await this.within(10000, 'The candidate connect', () => client.connect())
      const uuid = (results.uuid = await client.clientInstanceId({ timeout: 10000 }))
      expect(rawControl.uuid).not.toBe(uuid)
      const want = 3
      const accepted = await this.find('three accepted candidate Pushes', 30000, () => {
        const list = observer.accepted({ uuid }).filter((p) => p.metricsBytes > 0)
        return list.length >= want && list
      })
      const get = observer.subscriptions({ uuid }).find((r) => r.response?.errorCode === 0)
      expect(get.response).toMatchObject({
        errorCode: 0,
        telemetryMaxBytes: CAP,
        pushIntervalMs: 1000,
      })
      // eslint-disable-next-line n/no-unsupported-features/node-builtins -- Node >= 22.15 ships it; its absence is a readiness failure
      const zstd = typeof zlib.zstdCompressSync === 'function'
      const expectedCodec = get.response.acceptedCompressionTypes.find(
        (t) => t === 1 || (t === 4 && zstd)
      )
      const candidateSamples = []
      for (const push of accepted.slice(0, want)) {
        const item = await observer.decodeAccepted(push)
        this.evidence('producer-candidate', push.requestKey)
        const ids = []
        const partialPoints = []
        for (const m of item.decoded.metrics)
          for (const p of m.points) {
            const a = Object.fromEntries(p.attrs.map(([k, , v]) => [k, v]))
            const index = Number(a.series)
            const whole =
              m.name === `${PAYLOAD_PREFIX}${index}` &&
              a.pad === 'x'.repeat(128) &&
              p.valueKind === 'AS_DOUBLE' &&
              Number(p.asDouble) === index
            if (whole) ids.push(index)
            else partialPoints.push(m.name)
          }
        candidateSamples.push({
          rawBytes: item.rawBytes,
          transmittedBytes: item.transmittedBytes,
          compressionType: item.compressionType,
          pointIds: ids,
          partialPoints,
          requestKey: push.requestKey,
        })
      }
      for (const sample of candidateSamples) {
        expect(sample.rawBytes).toBeLessThanOrEqual(CAP)
        expect(sample.transmittedBytes).toBeLessThanOrEqual(CAP)
        expect(sample.pointIds.length).toBeGreaterThan(0)
        expect(sample.pointIds.length).toBeLessThan(SERIES)
        expect(new Set(sample.pointIds).size).toBe(sample.pointIds.length)
        expect(sample.partialPoints).toEqual([])
        if (sample.rawBytes > 1000) expect(sample.compressionType).toBe(expectedCodec ?? 0)
      }
      expect(candidateSamples.some((s) => s.rawBytes > 1000 && s.compressionType !== 0)).toBe(true)
      const stream = observer.readTranscript().filter((r) => r.apiKey === 72 && r.uuid === uuid)
      const extraIntervalPushes = stream.filter(
        (r, i) =>
          i > 0 && r.writeAt - (stream[i - 1].response?.responseAt ?? stream[i - 1].writeAt) < 1000
      )
      expect(extraIntervalPushes).toEqual([])
      expect(stream.every((r) => r.response?.errorCode === 0)).toBe(true)
      expect(this.reader.readExports({ uuid: rawControl.uuid })).toEqual([])
      results.candidate = {
        offered: get.response.acceptedCompressionTypes,
        expectedCodec: expectedCodec ?? 0,
        samples: candidateSamples,
      }
    })
    this.finishRecord(clientId, { allowedErrors: {} })
  }

  /**
   * Links each strict seam to the F04 unit title that holds it, so a deleted title fails here.
   * @returns {{file:string,title:string,lane:string}[]} labelled links, never live responses
   */
  seamLinks() {
    return STRICT_SEAMS.map(([file, title]) => {
      expect(fs.readFileSync(path.join(ROOT, file), 'utf8')).toContain(title)
      return { file, title, lane: 'strict seam, unit lane' }
    })
  }

  /**
   * Hands the case record to the shared persistence and verification.
   * @param {string} clientId candidate client id recorded for the receiver
   * @param {{allowedErrors:Object<string,number[]>}} allowed deliberate wire outcomes by owner
   * @returns {void}
   */
  finishRecord(clientId, { allowedErrors }) {
    const manifest = JSON.parse(fs.readFileSync(MANIFEST_FILE, 'utf8'))
    this.attempt.record = {
      wireVersions: { 71: 0, 72: 0 },
      run: {
        stem: this.stem,
        line: this.line,
        project: process.env.COMPOSE_PROJECT_NAME,
        composeSha256: fileSha(path.join(ROOT, 'docker-compose.telemetry.yml')),
      },
      fixture: {
        manifestSha256: fileSha(MANIFEST_FILE),
        reporterJar: manifest.lines[this.line].reporterJar,
      },
      clientId,
      results: this.attempt.record.results,
      pushes: this.pushes,
      allowed: { errors: allowedErrors, faultedSockets: {} },
    }
  }
}

/**
 * Declares one bounded scenario: the Jest budget includes the 2000 ms join margin, cleanup keeps
 * its own reserve inside it and evidence is persisted after cleanup by the shared case runner.
 * @param {string} title exact top-level test title
 * @param {string} name evidence name
 * @param {'prefix'|'custom'|'payload'} kind scenario
 * @param {number} totalMs table budget including the join margin
 * @returns {void}
 */
const scenario = (title, name, kind, totalMs) => {
  const budget = totalMs - 2000
  live(
    title,
    liveCase(budget, (scope) =>
      runCase(name, async (cleanup, publish, attempt) => {
        const line = process.env.KAFKA_VERSION
        const directory = process.env.TELEMETRY_EVIDENCE_DIR
        if (!line) throw new Error('KAFKA_VERSION must name the broker line')
        if (!directory || !path.isAbsolute(directory))
          throw new Error('TELEMETRY_EVIDENCE_DIR must name the absolute run directory')
        const cases = new SubscriptionCases({
          signal: scope,
          line,
          directory,
          stem: attempt.record.run.stem,
          attempt,
        })
        cleanup.defer(() => cases.close())
        await bounded(
          scope,
          budget - 20000,
          (phase) => {
            cases.signal = phase
            return cases.run(kind)
          },
          { label: `The ${title} scenario` }
        )
        attempt.stage = 'verification'
        publish({
          name,
          directory,
          observers: attempt.observers,
          record: attempt.record,
        })
      })
    ),
    caseTimeout(budget)
  )
}

scenario('subscription updates', 'telemetry-prefix', 'prefix', 120000)
scenario('custom numeric snapshots', 'telemetry-custom', 'custom', 90000)
scenario('bounded compressed payload', 'telemetry-payload', 'payload', 120000)
