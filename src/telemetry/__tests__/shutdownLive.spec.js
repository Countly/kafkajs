const crypto = require('crypto')
const fs = require('fs')
const net = require('net')
const path = require('path')
const { performance } = require('perf_hooks')
const { spawn } = require('child_process')
const { Kafka, logLevel } = require('../../../index')
const { runCase } = require('../../../scripts/e2e/telemetry/evidence')
const { cacheRetirement, connectionIdentity } = require('../../../scripts/e2e/telemetry/cacheTrace')
const {
  TelemetryWireObserver,
  withTelemetryResource,
} = require('../../../testHelpers/telemetryFixtures')
const { createChaosSocketFactory, FrameFilter } = require('../../../testHelpers/chaosSocket')
const {
  bounded,
  caseTimeout,
  liveCase,
  liveRun,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')

// Gated: the configured fixture (docker-compose.telemetry.yml) under the isolated CI ownership
// contract, which names the project the broker logs and logger levels are read from. A failure
// always fails; the only self-skip is the absence of the outer opt-in.
const live = process.env.TELEMETRY_E2E ? test : test.skip

const ROOT = path.resolve(__dirname, '../../..')
const MANIFEST_FILE = path.join(ROOT, 'scripts/e2e/telemetry/fixtures/manifest.json')
const PORTS = [19092, 19095, 19098]
const LISTENER = { 19092: '9092', 19095: '9095', 19098: '9098' }
const KINDS = ['producer', 'consumer', 'admin', 'share']
const SUPPORTED_TIMEOUT0 = ['producer', 'consumer', 'share']
const LOGGER = 'org.apache.kafka.server.ClientMetricsManager'
const PROPERTIES =
  'enable.metrics.push=false\nrequest.timeout.ms=3000\ndefault.api.timeout.ms=5000\n'
const SETUP_AND_TELEMETRY_APIS = [17, 18, 36, 71]
const FINAL_CAP_MS = 1000
const SCHEDULING_MS = 100
const QUIET_MS = 1000
const clock = { now: () => performance.now() }

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
 * Bounded access to the owned project's brokers through `docker compose`, scoped by the project and
 * file the isolated CI ownership contract selected. Nothing follows a log or watches a file: every
 * call is one finite command whose output is capped.
 */
class BrokerLogs {
  /**
   * @param {object} options ownership identity
   * @param {string} options.project unique Compose project of this run
   * @param {string} options.file the selected Compose file
   */
  constructor({ project, file }) {
    this.project = project
    this.file = file
    this.receipts = []
  }

  /**
   * @param {string[]} args `docker compose` arguments after the project and file
   * @param {object} [options] `input` for stdin, `ms` for the command bound
   * @returns {Promise<{code:number|null,stdout:string,stderr:string,truncated:boolean}>} the result
   */
  run(args, { input = '', ms = 20000 } = {}) {
    return new Promise((resolve) => {
      const child = spawn(
        'timeout',
        [
          '-k',
          '5s',
          `${ms / 1000}s`,
          'docker',
          'compose',
          '-p',
          this.project,
          '-f',
          this.file,
          ...args,
        ],
        { stdio: ['pipe', 'pipe', 'pipe'], env: process.env }
      )
      const result = { code: null, stdout: '', stderr: '', truncated: false }
      child.stdout.setEncoding('utf8')
      child.stderr.setEncoding('utf8')
      child.stdout.on('data', (data) => {
        if (result.stdout.length + data.length > 4 * 1024 * 1024) result.truncated = true
        else result.stdout += data
      })
      child.stderr.on('data', (data) => {
        if (result.stderr.length < 65536) result.stderr += data
      })
      child.stdin.on('error', () => {})
      child.stdin.end(input)
      child.once('error', (error) => resolve({ ...result, error }))
      child.once('close', (code) => resolve({ ...result, code }))
    })
  }

  /**
   * @param {string} service kafka1, kafka2 or kafka3
   * @param {string} id broker id
   * @param {string[]} action the `kafka-configs.sh` action arguments
   * @returns {Promise<string>} its stdout
   * @throws {Error} for a failed or capped command
   */
  async configs(service, id, action) {
    const result = await this.run(
      [
        'exec',
        '-T',
        service,
        '/opt/kafka/bin/kafka-configs.sh',
        '--bootstrap-server',
        'kafka1:29092',
        '--command-config',
        '/dev/stdin',
        '--entity-type',
        'broker-loggers',
        '--entity-name',
        id,
        ...action,
      ],
      { input: PROPERTIES }
    )
    if (result.code !== 0 || result.truncated || result.error)
      throw new Error(
        `kafka-configs ${action[0]} failed on ${service}: ${result.stderr.slice(0, 300)}`
      )
    this.receipts.push({ service, id, action, result })
    return result.stdout
  }

  /**
   * Reads both owned logger overrides in one CLI invocation, retaining the full readback.
   * @param {string} service owned broker service
   * @param {string} id broker node
   * @returns {Promise<object>} exact listed levels, null for absent overrides
   */
  async snapshot(service, id) {
    const text = await this.configs(service, id, ['--describe'])
    return Object.fromEntries(
      [LOGGER, 'kafka.request.logger'].map((logger) => [
        logger,
        new RegExp(`^\\s*${logger.replaceAll('.', '\\.')}=(\\w+)`, 'm').exec(text)?.[1] ?? null,
      ])
    )
  }

  /**
   * Changes only the two owned logger overrides in one bounded invocation.
   * @param {string} service owned broker service
   * @param {string} id broker node
   * @param {object} levels exact overrides to set or delete
   * @returns {Promise<void>} after the CLI succeeded
   */
  async configure(service, id, levels) {
    const action = ['--alter']
    const set = Object.entries(levels)
      .filter(([, level]) => level !== null)
      .map(([logger, level]) => `${logger}=${level}`)
    const remove = Object.entries(levels)
      .filter(([, level]) => level === null)
      .map(([logger]) => logger)
    if (set.length) action.push('--add-config', set.join(','))
    if (remove.length) action.push('--delete-config', remove.join(','))
    await this.configs(service, id, action)
  }

  /**
   * @param {string} service broker service
   * @param {string} sinceIso RFC 3339 start of the window
   * @returns {Promise<{text:string,truncated:boolean}>} the service log since then
   * @throws {Error} for a failed log command
   */
  async since(service, sinceIso) {
    const result = await this.run([
      'logs',
      '--no-color',
      '--no-log-prefix',
      '--since',
      sinceIso,
      service,
    ])
    if (result.code !== 0 || result.error)
      throw new Error(`Broker log unavailable for ${service}: ${result.stderr.slice(0, 300)}`)
    return { text: result.stdout, truncated: result.truncated }
  }
}

/**
 * Chaos sockets for one observed client, with a passive frame classifier on each: the first
 * request frame of API 71 or 72 marks that very socket object as the telemetry transport. A fault
 * is armed on the classified object only, and every patch is removed again.
 */
class TelemetryFaults {
  /** @param {object} observer the wire observer whose real sockets the chaos layer wraps */
  constructor(observer) {
    const inner = createChaosSocketFactory((options) => observer.socketFactory(options))
    this.records = []
    this.factory = (options) => this.track(inner(options))
  }

  /**
   * @param {object} socket the chaos socket just issued
   * @returns {object} the same socket, now classified and timed
   */
  track(socket) {
    const record = { ordinal: this.records.length + 1, socket, telemetry: false, closed: false }
    const classifier = new FrameFilter({
      headerBytes: 12,
      decide: (header) => {
        if (header.length >= 12 && [71, 72].includes(header.readInt16BE(4))) record.telemetry = true
        return false
      },
    })
    const write = socket.write
    socket.write = function (data, encoding) {
      classifier.push(Buffer.from(data, encoding))
      return write.call(this, data, encoding)
    }
    record.restore = () => {
      delete socket.write
    }
    socket.on('close', () => {
      record.closed = true
      record.closedAt = clock.now()
    })
    this.records.push(record)
    return socket
  }

  /** @returns {object[]} open sockets proven by an actual 71/72 frame to be the telemetry transport */
  telemetry() {
    return this.records.filter((r) => r.telemetry && !r.closed)
  }

  /** @returns {void} removes every classifier patch */
  restore() {
    for (const record of this.records) record.restore()
  }
}

/**
 * L5 contribution: per public kind one close, an immediate public reconnect on the same object,
 * a new accepted normal Push under the same UUID, and an independent broker-side proof that the
 * old connection's cache entry was retired. Also the inherited zero-timeout close and one
 * blackholed telemetry transport. Twenty-cycle resource and soak acceptance belongs to J.
 */
class ReopenCase {
  #tasks = []
  #closing = null

  /**
   * @param {object} options case state
   * @param {AbortSignal} options.signal the case phase scope
   * @param {string} options.line broker line
   * @param {string} options.directory absolute evidence directory
   * @param {string} options.stem unique prefix
   * @param {object} options.attempt mutable evidence of the running case
   */
  constructor({ signal, line, directory, stem, attempt }) {
    this.signal = signal
    this.owner = signal
    this.line = line
    this.directory = directory
    this.stem = stem
    this.attempt = attempt
    this.results = { episodes: {}, timeout0: {} }
    this.pushes = []
    this.members = []
  }

  /** @param {function():Promise<void>|void} task release attempted by `close` @returns {void} */
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
   * Like `within`, for releases: they run after the phase scope has ended, under the case's own.
   * @param {number} ms bound of this one release
   * @param {string} label failure description
   * @param {function(AbortSignal):*} op the release
   * @returns {Promise<*>} its result
   */
  release(ms, label, op) {
    return bounded(this.owner, ms, op, { label })
  }

  /**
   * Polls a lookup until it returns a truthy value.
   * @param {string} label failure description
   * @param {number} ms poll budget
   * @param {function():*} lookup the value, or falsy when not yet
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
   * Builds one observed client of a kind whose sockets pass through the classifying chaos layer.
   * Release order: disconnect, a quiet window with no owned socket or final, dispose, require clean.
   * @param {string} kind producer, consumer, admin or share
   * @param {string} suffix owner and client-id suffix
   * @returns {object} `{ client, observer, faults, kind }`
   */
  build(kind, suffix) {
    const owner = `${kind}-${suffix}`
    const observer = new TelemetryWireObserver({
      completeStream: true,
      directory: this.directory,
      line: this.line,
      clock,
      mode: 'configured',
      owner,
    })
    this.attempt.observers[owner] = observer
    const faults = new TelemetryFaults(observer)
    const kafka = new Kafka({
      clientId: `${this.stem}-${owner}`,
      brokers: PORTS.map((port) => `localhost:${port}`),
      socketFactory: faults.factory,
      logLevel: logLevel.NOTHING,
    })
    const common = { enableMetricsPush: true }
    const client =
      kind === 'producer'
        ? kafka.producer({ ...common, idempotent: false })
        : kind === 'admin'
          ? kafka.admin(common)
          : kind === 'share'
            ? kafka.shareConsumer({ ...common, groupId: `${this.stem}-${owner}-group` })
            : kafka.consumer({
                ...common,
                groupId: `${this.stem}-${owner}-group`,
                sessionTimeout: 30000,
                heartbeatInterval: 3000,
                maxWaitTimeInMs: 500,
              })
    this.defer(() => observer.assertClean({ allowUnanswered: observer.cancellationKeys() }))
    this.defer(() => observer.dispose())
    this.defer(() => faults.restore())
    this.defer(async () => {
      await this.release(
        QUIET_MS + 2000,
        'The quiet window',
        () => new Promise((r) => setTimeout(r, QUIET_MS))
      )
      expect(faults.records.every((r) => r.closed)).toBe(true)
      expect(faults.telemetry()).toEqual([])
      if (suffix === 't0')
        expect(observer.readTranscript().filter((r) => r.apiKey === 72 && r.terminating)).toEqual(
          []
        )
    })
    this.defer(() => observer.closeClient(client))
    const member = { client, observer, faults, kind, owner }
    this.members.push(member)
    return member
  }

  /**
   * Connects a client and waits until two normal Pushes were accepted on its telemetry stream.
   * @param {object} member the observed client
   * @returns {Promise<object>} its UUID, captured telemetry socket and node
   */
  async establish(member) {
    const { client, observer, faults } = member
    const since = new Date().toISOString()
    await this.within(10000, `The ${member.kind} connect`, () => client.connect())
    const uuid = await client.clientInstanceId({ timeout: 5000 })
    const accepted = await this.find(`two ${member.kind} accepted Pushes`, 15000, () => {
      const list = observer.accepted({ uuid }).filter((p) => p.metricsBytes > 0)
      return list.length >= 2 && list
    })
    expect(faults.telemetry()).toHaveLength(1)
    const [captured] = faults.telemetry()
    const port = observer.telemetrySockets().find((s) => s.socketId === captured.ordinal).port
    const get = observer
      .subscriptions({ uuid })
      .find((r) => r.socketId === captured.ordinal && r.response?.errorCode === 0)
    const connect = observer
      .readTranscript()
      .find((r) => r.type === 'connect' && r.socketId === captured.ordinal)
    const logs = await this.readLogs(since)
    const identity = connectionIdentity({
      logs,
      get,
      connect,
      node: get.node,
      localPort: LISTENER[port],
    })
    return {
      uuid,
      ordinal: captured.ordinal,
      node: accepted.at(-1).node,
      port,
      serverConnectionIdentity: identity,
      get,
      connect,
      since,
    }
  }

  /**
   * Reads the complete broker-side disconnect episode after immediate reopen and recovery.
   * @param {object} before the captured connection from `establish`
   * @returns {Promise<object>} the source-qualified retirement; rejects at its own bound
   */
  async observeCacheRetirement(before) {
    const logs = await this.readLogs(before.since)
    const input = {
      logs,
      node: before.node,
      localPort: before.serverConnectionIdentity.localPort,
      get: before.get,
      connect: before.connect,
      uuid: before.uuid,
      since: before.since,
      completedAt: new Date().toISOString(),
      source: this.source,
      loggerReadbacks: this.logs.receipts,
    }
    this.attempt.record.oracles ??= {}
    this.attempt.record.oracles[this.cacheKind] = input
    return cacheRetirement(input)
  }

  /**
   * Reads the complete bounded three-node log window without returning on a removal prefix.
   * @param {string} since epoch window start
   * @returns {Promise<object>} full broker logs
   * @throws {Error} for unavailable or truncated inputs
   */
  async readLogs(since) {
    return Object.fromEntries(
      await Promise.all(
        ['kafka1', 'kafka2', 'kafka3'].map(async (service, index) => [
          String(index + 1),
          await this.logs.since(service, since),
        ])
      )
    )
  }

  /**
   * One close and immediate reopen on the same public object.
   * @param {string} kind producer, consumer, admin or share
   * @returns {Promise<object>} compact episode evidence
   */
  async episode(kind) {
    const member = this.build(kind, 'ep')
    const { client, observer } = member
    const before = await this.establish(member)
    const preClose = observer.checkpoint()
    this.cacheKind = kind
    const cacheObservation = {
      since: before.since,
      start: clock.now(),
      connect: before.connect,
      get: before.get,
    }
    const order = []
    await this.within(8000, `The ${kind} close`, () =>
      observer.closeClient(client, kind === 'admin' ? undefined : { timeout: 3000 })
    )
    const closeEnd = observer.checkpoint()
    order.push('closed')
    const reconnecting = client.connect()
    order.push('connect-called')
    await this.within(10000, `The ${kind} reconnect`, () => reconnecting)
    const cachedUuid = await client.clientInstanceId({ timeout: 0 })
    const closeFrames = observer
      .readTranscript({ after: preClose })
      .filter(
        (r) =>
          r.index <= closeEnd &&
          (r.type === 'connect' || SETUP_AND_TELEMETRY_APIS.includes(r.apiKey))
      )
    const get = await this.find(`the ${kind} nonzero-ID Get`, 15000, () =>
      observer
        .subscriptions({ uuid: before.uuid, after: closeEnd })
        .find((r) => r.uuid === before.uuid && r.response?.errorCode === 0)
    )
    const push = await this.find(`the ${kind} new accepted Push`, 15000, () =>
      observer
        .accepted({ uuid: before.uuid, after: get.index })
        .find((p) => p.socketId === get.socketId && p.metricsBytes > 0)
    )
    await observer.decodeAccepted(push)
    this.pushes.push({ owner: member.owner, requestKey: push.requestKey })
    const rejected = observer
      .readTranscript({ after: closeEnd })
      .filter((r) => r.apiKey === 72 && !r.terminating && r.response?.errorCode)
    const finalsAtClose = observer
      .readTranscript({ after: preClose })
      .filter((r) => r.apiKey === 72 && r.terminating && r.index <= closeEnd)
    const finalResends = observer
      .readTranscript({ after: closeEnd })
      .filter((r) => r.apiKey === 72 && r.terminating)
    order.push('cache-awaited')
    // Replacement work and both logger readbacks fence the complete disconnect episode.
    expect(await this.logs.snapshot(`kafka${before.node}`, before.node)).toEqual({
      [LOGGER]: 'TRACE',
      'kafka.request.logger': 'TRACE',
    })
    const cacheEvent = await this.observeCacheRetirement(before)
    expect(cacheObservation.start).toBeLessThanOrEqual(get.writeAt)
    const reopen = {
      calledBeforeCacheAwait: order.indexOf('connect-called') < order.indexOf('cache-awaited'),
      cachedUuid,
      get: { requestUuid: get.uuid, socketId: get.socketId },
      acceptedNormalPushes: [push.requestKey],
      correctiveExtraPushes: rejected.length,
      finalResends: finalResends.length,
    }
    const close = { getOrConnectOrAuthFrames: closeFrames.filter((r) => r.apiKey !== 72) }
    expect(reopen.calledBeforeCacheAwait).toBe(true)
    expect(reopen.cachedUuid).toBe(before.uuid)
    expect(reopen.get.requestUuid).toBe(before.uuid)
    expect(reopen.get.socketId).not.toBe(before.ordinal)
    expect(reopen.acceptedNormalPushes.length).toBeGreaterThanOrEqual(1)
    expect(reopen.correctiveExtraPushes).toBeLessThanOrEqual(1)
    expect(rejected.every((r) => r.response.errorCode === 42)).toBe(true)
    expect(reopen.finalResends).toBe(0)
    expect(close.getOrConnectOrAuthFrames).toEqual([])
    expect(finalsAtClose.map((r) => r.response?.errorCode)).toEqual([0])
    expect(cacheEvent.source).toBe('broker-disconnect-trace')
    expect(cacheEvent.connectionIdentity).toEqual(before.serverConnectionIdentity)
    expect(member.faults.telemetry()).toHaveLength(1)
    await observer.closeClient(client, kind === 'admin' ? undefined : { timeout: 3000 })
    return {
      before,
      reopen,
      close,
      cacheEvent,
      finalRequestKeys: finalsAtClose.map((r) => r.requestKey),
    }
  }

  /**
   * A zero-timeout close of a supported kind sends no final.
   * @param {string} kind producer, consumer or share
   * @returns {Promise<object>} compact evidence
   */
  async timeoutZero(kind) {
    const member = this.build(kind, 't0')
    const before = await this.establish(member)
    const preClose = member.observer.checkpoint()
    await this.within(8000, `The ${kind} zero-timeout close`, () =>
      member.observer.closeClient(member.client, { timeout: 0 })
    )
    const finalZeroTimeoutWrites = member.observer
      .readTranscript({ after: preClose })
      .filter((r) => r.apiKey === 72 && r.terminating)
    expect(finalZeroTimeoutWrites).toEqual([])
    return { uuid: before.uuid, finalZeroTimeoutWrites: finalZeroTimeoutWrites.length }
  }

  /**
   * The captured telemetry socket of an admin is blackholed: foreground metadata work still
   * succeeds and the telemetry cleanup closes within its own cap plus the scheduling allowance.
   * @returns {Promise<object>} compact evidence
   */
  async blackhole() {
    const member = this.build('admin', 'bh')
    const before = await this.establish(member)
    const [captured] = member.faults.telemetry()
    member.observer.declareFault(captured.ordinal)
    captured.socket.blackhole()
    const cluster = await this.within(8000, 'The foreground metadata work', () =>
      member.client.describeCluster({ timeout: 3000 })
    )
    expect(cluster.brokers.map((b) => b.port).sort()).toEqual(PORTS)
    const started = clock.now()
    await this.within(10000, 'The admin close', () => member.observer.closeClient(member.client))
    const coreCloseMs = clock.now() - started
    await this.find('the blackholed telemetry socket close', 3000, () => captured.closed)
    const telemetryCleanupMs = captured.closedAt - started
    expect(telemetryCleanupMs).toBeLessThanOrEqual(FINAL_CAP_MS + SCHEDULING_MS)
    return { uuid: before.uuid, faultedSocket: before.ordinal, coreCloseMs, telemetryCleanupMs }
  }

  /**
   * @returns {Promise<void>} when every episode, control and level restoration is complete
   */
  async run() {
    const project = process.env.COMPOSE_PROJECT_NAME
    const file = process.env.COMPOSE_FILE
    if (!project || file !== 'docker-compose.telemetry.yml')
      throw new Error(
        'The cache observer needs the isolated CI project and the telemetry Compose file'
      )
    this.logs = new BrokerLogs({ project, file })
    const kafka = new Kafka({
      clientId: `${this.stem}-aux`,
      brokers: PORTS.map((port) => `localhost:${port}`),
      socketFactory: ipv4Factory,
      logLevel: logLevel.NOTHING,
    })
    const aux = kafka.admin({ enableMetricsPush: false, apiTimeout: 3000 })
    this.defer(() => aux.disconnect())
    await this.within(10000, 'The auxiliary admin connect', () => aux.connect())
    const cluster = await this.within(5000, 'The topology check', () =>
      aux.describeCluster({ timeout: 3000 })
    )
    expect(cluster.brokers.map((b) => [b.nodeId, b.host, b.port]).sort()).toEqual(
      PORTS.map((port, index) => [index + 1, 'localhost', port])
    )
    const sourceResult = await this.logs.run([
      'exec',
      '-T',
      'kafka1',
      'sha256sum',
      `/opt/kafka/libs/kafka-server-${this.line}.jar`,
    ])
    expect(sourceResult.code).toBe(0)
    this.source = {
      artifact: `kafka-server-${this.line}.jar`,
      sha256: sourceResult.stdout.split(/\s+/)[0],
      receipt: sourceResult,
      className: 'org.apache.kafka.server.ClientMetricsManager',
    }
    const levels = {}
    const services = ['kafka1', 'kafka2', 'kafka3']
    const traced = { [LOGGER]: 'TRACE', 'kafka.request.logger': 'TRACE' }
    this.defer(() =>
      this.release(18000, 'The logger restoration and readbacks', async () => {
        const outcomes = await Promise.allSettled(
          services.map(async (service, index) => {
            const id = String(index + 1)
            if (!levels[id]) return
            await this.logs.configure(service, id, levels[id])
            const restored = await this.logs.snapshot(service, id)
            expect(restored).toEqual(levels[id])
            this.results.restoredLevels ??= {}
            this.results.restoredLevels[id] = restored
          })
        )
        const errors = outcomes.filter((r) => r.status === 'rejected').map((r) => r.reason)
        if (errors.length) throw new AggregateError(errors, 'Logger restoration failed')
      })
    )
    const outcomes = await Promise.allSettled(
      services.map(async (service, index) => {
        const id = String(index + 1)
        levels[id] = await this.logs.snapshot(service, id)
        await this.logs.configure(service, id, traced)
        expect(await this.logs.snapshot(service, id)).toEqual(traced)
      })
    )
    const errors = outcomes.filter((r) => r.status === 'rejected').map((r) => r.reason)
    if (errors.length) throw new AggregateError(errors, 'Logger setup failed')
    this.results.loggerReceipts = this.logs.receipts
    this.results.originalLevels = levels
    const entries = {
      metrics: 'org.apache.kafka.',
      'interval.ms': '1000',
      match: `client_id=^${this.stem}-`,
    }
    await withTelemetryResource(aux, { name: this.stem, entries }, async () => {
      for (const kind of KINDS) this.results.episodes[kind] = await this.episode(kind)
      for (const kind of SUPPORTED_TIMEOUT0)
        this.results.timeout0[kind] = await this.timeoutZero(kind)
      this.results.blackhole = await this.blackhole()
    })
  }

  /** @returns {void} hands the case record to the shared persistence and verification */
  finishRecord() {
    const manifest = JSON.parse(fs.readFileSync(MANIFEST_FILE, 'utf8'))
    this.attempt.record = {
      oracles: this.attempt.record.oracles,
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
      results: this.results,
      pushes: this.pushes,
      allowed: {
        errors: Object.fromEntries(KINDS.map((kind) => [`${kind}-ep`, [42]])),
        faultedSockets: { 'admin-bh': [this.results.blackhole.faultedSocket] },
      },
    }
  }
}

const BUDGET = 118000
live(
  'close and reopen',
  liveCase(BUDGET, (scope) =>
    runCase('telemetry-shutdown', async (cleanup, publish, attempt) => {
      const line = process.env.KAFKA_VERSION
      const directory = process.env.TELEMETRY_EVIDENCE_DIR
      if (!line) throw new Error('KAFKA_VERSION must name the broker line')
      if (!directory || !path.isAbsolute(directory))
        throw new Error('TELEMETRY_EVIDENCE_DIR must name the absolute run directory')
      const scenario = new ReopenCase({
        signal: scope,
        line,
        directory,
        stem: attempt.record.run.stem,
        attempt,
      })
      cleanup.defer(() => scenario.close())
      await bounded(
        scope,
        BUDGET - 20000,
        (inner) => {
          scenario.signal = inner
          return scenario.run()
        },
        { label: 'The close and reopen scenario' }
      )
      attempt.stage = 'verification'
      scenario.finishRecord()
      publish({
        name: 'telemetry-shutdown',
        directory,
        observers: attempt.observers,
        record: attempt.record,
      })
    })
  ),
  caseTimeout(BUDGET)
)
