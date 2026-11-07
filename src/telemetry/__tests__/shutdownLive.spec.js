const crypto = require('crypto')
const fs = require('fs')
const net = require('net')
const path = require('path')
const v8 = require('v8')
const vm = require('vm')
const { performance } = require('perf_hooks')
const { spawn } = require('child_process')
const { Kafka, logLevel } = require('../../../index')
const {
  episodeLogs,
  joinChunks,
  runCase,
  stableCacheEvent,
  traceChunk,
} = require('../../../scripts/e2e/telemetry/evidence')
const { cacheRetirement, connectionIdentity } = require('../../../scripts/e2e/telemetry/cacheTrace')
const { DecoderGate, withTelemetryResource } = require('../../../testHelpers/telemetryFixtures')
const { SCHEDULES, TelemetrySegments } = require('../../../testHelpers/telemetrySegments')
const { createResourceProbe } = require('../../../testHelpers/resourceProbe')
const { createChaosSocketFactory, FrameFilter } = require('../../../testHelpers/chaosSocket')
const {
  bounded,
  caseTimeout,
  delay,
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
const SCENARIO = 'telemetry-shutdown'
const ROUNDS = 20
const DECODERS = 4
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
   * @param {string} [untilIso] RFC 3339 end of the window; open-ended when omitted
   * @returns {Promise<{text:string,truncated:boolean}>} the service log in that window
   * @throws {Error} for a failed log command
   */
  async since(service, sinceIso, untilIso) {
    const result = await this.run([
      'logs',
      '--no-color',
      '--no-log-prefix',
      '--since',
      sinceIso,
      ...(untilIso ? ['--until', untilIso] : []),
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
 * is armed on the classified object only, every patch is removed again, and a closed socket is
 * released at once so only scalar facts outlive it.
 */
class TelemetryFaults {
  /** @param {TelemetrySegments} segments the owner whose selected segment receives every socket */
  constructor(segments) {
    const inner = createChaosSocketFactory((options) => segments.socketFactory(options))
    this.segments = segments
    this.records = []
    this.factory = (options) => this.track(inner(options), segments.current())
  }

  /**
   * @param {object} socket the chaos socket just issued
   * @param {object} segment the segment whose observer received it
   * @returns {object} the same socket, now classified and timed
   */
  track(socket, segment) {
    const record = {
      segment: segment.id,
      ordinal: segment.observer.socketOwnership().length,
      socket,
      telemetry: false,
      closed: false,
    }
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
      this.release(record)
    })
    this.records.push(record)
    return socket
  }

  /**
   * @param {object} record a socket record
   * @returns {void} removes its patch; the socket itself is dropped only once it has closed, so a
   *   socket that stays open remains visible as an owned identity
   */
  release(record) {
    record.restore?.()
    record.restore = null
    if (record.closed) record.socket = null
  }

  /** @returns {object[]} open sockets proven by an actual 71/72 frame to be the telemetry transport */
  telemetry() {
    return this.records.filter((r) => r.telemetry && !r.closed)
  }

  /** @returns {object[]} every socket still open, whatever it carries */
  openSockets() {
    return this.records.filter((r) => !r.closed).map((r) => r.socket)
  }

  /** @returns {void} removes every classifier patch still installed */
  restore() {
    for (const record of this.records) this.release(record)
  }
}

/**
 * Reads the broker logs once for all four kinds at each round: contiguous, non-overlapping windows
 * whose first window opens before the first connect. A member that arrives waits for the others;
 * the last arrival reads, and a failure anywhere releases every waiter.
 */
class TraceWindows {
  #waiting = new Map()
  #failure = null

  /**
   * @param {object} options window source
   * @param {BrokerLogs} options.logs bounded broker log access
   * @param {number} options.members arrivals that trigger one read
   * @param {function(number,string,function(AbortSignal):Promise<*>):Promise<*>} options.within bounded runner
   */
  constructor({ logs, members, within }) {
    this.logs = logs
    this.members = members
    this.within = within
    this.chunks = []
    this.start = null
  }

  /** @returns {void} opens the first window now */
  open() {
    this.start = new Date().toISOString()
  }

  /**
   * @param {number} round ordinal of the window within the run
   * @returns {Promise<object>} the retained chunk, once every member has arrived and it was read
   * @throws {Error} the failure that released the waiters
   */
  arrive(round) {
    if (this.#failure) return Promise.reject(this.#failure)
    let entry = this.#waiting.get(round)
    if (!entry) {
      entry = { count: 0 }
      entry.promise = new Promise((resolve, reject) => {
        entry.resolve = resolve
        entry.reject = reject
      })
      this.#waiting.set(round, entry)
    }
    entry.count++
    if (entry.count === this.members) this.read(round).then(entry.resolve, entry.reject)
    return entry.promise
  }

  /**
   * @param {Error} error the failure to deliver to every current and later waiter
   * @returns {void}
   */
  fail(error) {
    this.#failure ??= error
    for (const entry of this.#waiting.values()) entry.reject(error)
  }

  /**
   * @param {number} round ordinal of the window to read
   * @returns {Promise<object>} its retained chunk
   */
  async read(round) {
    const since = round === 0 ? this.start : this.chunks[round - 1].until
    const until = new Date().toISOString()
    const logs = Object.fromEntries(
      await this.within(20000, `The trace window ${round}`, () =>
        Promise.all(
          ['kafka1', 'kafka2', 'kafka3'].map(async (service, index) => [
            String(index + 1),
            await this.logs.since(service, since, until),
          ])
        )
      )
    )
    this.chunks[round] = traceChunk({ index: round, since, until, logs })
    return this.chunks[round]
  }

  /**
   * @param {number} ordinal the window to take whole
   * @returns {Object<string,{text:string,truncated:boolean}>} that window's per-node log
   */
  window(ordinal) {
    return joinChunks(this.chunks, ordinal, ordinal)
  }

  /**
   * @param {number} ordinal the window that holds an old connection's Get
   * @param {number} last last window to include, inclusive
   * @returns {Object<string,{text:string,truncated:boolean}>} that connection's episode log
   */
  episode(ordinal, last) {
    return episodeLogs(this.chunks, ordinal, last)
  }
}

/**
 * L5 contribution: per public kind twenty close/immediate-reconnect rounds on one returned object,
 * each with a new accepted normal Push under the same UUID and an independent broker-side proof that
 * the old connection's cache entry was retired. Also the inherited zero-timeout close and one
 * blackholed telemetry transport. Every lifecycle is one sealed observer segment.
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
    this.results = { initial: {}, rounds: {}, journal: {}, timeout0: {}, blackhole: null }
    this.members = []
    this.gate = new DecoderGate(DECODERS)
    this.consumed = new Set()
    attempt.segments = []
    this.probe = createResourceProbe({
      activeResources: true,
      identities: () => ({
        openSockets: this.members.flatMap((m) => m.faults.openSockets()),
        undischarged: this.members.flatMap((m) => m.segments.undischarged()),
      }),
    })
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
   * @param {string} kind producer, consumer, admin or share
   * @returns {object|undefined} the inherited close arguments of that kind; admin has none
   */
  closeOptions(kind) {
    return kind === 'admin' ? undefined : { timeout: 3000 }
  }

  /**
   * Builds one observed client of a kind. Its sockets pass through the classifying chaos layer into
   * the segment the owner has selected; every release is registered, nothing is attempted here.
   * @param {string} kind producer, consumer, admin or share
   * @param {string} suffix ep, t0 or bh
   * @returns {object} `{ client, segments, faults, kind, owner }`
   */
  build(kind, suffix) {
    const owner = `${kind}-${suffix}`
    const shared = {
      directory: this.directory,
      clock,
      observer: { line: this.line, mode: 'configured', gate: this.gate, consumed: this.consumed },
    }
    const segments =
      suffix === 'ep'
        ? TelemetrySegments.ordinary(SCENARIO, kind, shared)
        : TelemetrySegments.single(SCENARIO, owner, shared)
    this.attempt.segments.push(segments)
    const faults = new TelemetryFaults(segments)
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
    const member = { client, segments, faults, kind, owner, draining: null }
    this.members.push(member)
    this.defer(() => faults.restore())
    return member
  }

  /**
   * Connects a client and waits until two normal Pushes were accepted on its telemetry stream.
   * @param {object} member the observed client
   * @returns {Promise<object>} its UUID, telemetry socket, node, port and first successful Get
   */
  async establish(member) {
    const { client, segments, faults, kind } = member
    const { observer } = segments.current()
    await this.within(10000, `The ${kind} connect`, () => client.connect())
    const uuid = await client.clientInstanceId({ timeout: 5000 })
    await this.find(`two ${kind} accepted Pushes`, 15000, () => {
      const list = observer.accepted({ uuid }).filter((p) => p.metricsBytes > 0)
      return list.length >= 2 && list
    })
    expect(faults.telemetry()).toHaveLength(1)
    const [captured] = faults.telemetry()
    return this.lifecycle(observer, uuid, captured.ordinal)
  }

  /**
   * @param {object} observer the segment observer of the lifecycle
   * @param {string} uuid the client's instance UUID
   * @param {number} socketId the telemetry socket of the lifecycle
   * @returns {object} the successful Get, the connect of its socket, node and port
   */
  lifecycle(observer, uuid, socketId) {
    const get = observer
      .subscriptions({ uuid })
      .find((r) => r.socketId === socketId && r.response?.errorCode === 0)
    const connect = observer
      .readTranscript()
      .find((r) => r.type === 'connect' && r.socketId === socketId)
    return { uuid, ordinal: socketId, node: get.node, port: connect.port, get, connect }
  }

  /**
   * @param {object} lifecycle a lifecycle from `establish` or a round
   * @param {number} ordinal the shared window that holds the lifecycle's Get
   * @param {TraceWindows} windows shared windows
   * @returns {object} the exact broker connection the lifecycle's Get arrived on
   */
  serverIdentity(lifecycle, ordinal, windows) {
    return connectionIdentity({
      logs: windows.window(ordinal),
      get: lifecycle.get,
      connect: lifecycle.connect,
      node: lifecycle.node,
      localPort: LISTENER[lifecycle.port],
    })
  }

  /**
   * At a boundary fails the fixture, rather than queueing, when the previous segment has not
   * drained; at the end of the schedule waits for it. Either way surfaces a sealing failure.
   * @param {object} member the client whose previous segment is checked
   * @param {boolean} [last=false] whether no further boundary follows
   * @returns {Promise<void>} once the previous segment is sealed
   * @throws {Error} when it is still draining at a boundary or failed to seal
   */
  async drained(member, last = false) {
    const { draining } = member
    if (!draining) return
    if (last) await draining.promise
    if (!draining.settled) throw new Error(`The previous ${member.kind} segment has not drained`)
    if (draining.error) throw draining.error
  }

  /**
   * Starts sealing a closed segment while the next lifecycle is established.
   * @param {object} member the client that owns the segment
   * @param {object} segment the closed segment
   * @returns {void}
   */
  drain(member, segment) {
    const draining = { settled: false, error: null }
    draining.promise = member.segments.discharge(segment).then(
      () => {
        draining.settled = true
      },
      (error) => {
        draining.settled = true
        draining.error = error
      }
    )
    member.draining = draining
  }

  /**
   * Twenty rounds of close and immediate public reopen on one returned object. The next segment is
   * selected before each close, the connect call follows the close's own continuation, and the old
   * connection's cache retirement is proved from the shared windows before the next round starts.
   * @param {object} member the observed client of an ordinary kind
   * @param {TraceWindows} windows the shared broker log windows
   * @returns {Promise<void>} after the twentieth round and the terminal close are complete
   */
  async rounds(member, windows) {
    const { client, segments, faults, kind } = member
    let current = await this.establish(member)
    const { uuid } = current
    this.results.initial[kind] = {
      segment: segments.current().id,
      uuid,
      get: current.get.requestKey,
    }
    await windows.arrive(0)
    current.identity = this.serverIdentity(current, 0, windows)
    const rounds = (this.results.rounds[kind] = [])
    for (let round = 1; round <= ROUNDS; round++) {
      await this.drained(member)
      const observedAt = clock.now()
      const cycle = segments.cycle(client, this.closeOptions(kind))
      await this.within(8000, `The ${kind} close`, () => cycle.closed)
      this.drain(member, cycle.from)
      await this.within(10000, `The ${kind} reconnect`, () => cycle.reopened)
      const cachedUuid = await client.clientInstanceId({ timeout: 0 })
      const { observer } = cycle.to
      const get = await this.find(`the ${kind} nonzero-ID Get`, 15000, () =>
        observer.subscriptions({ uuid }).find((r) => r.uuid === uuid && r.response?.errorCode === 0)
      )
      const push = await this.find(`the ${kind} new accepted Push`, 15000, () =>
        observer
          .accepted({ uuid, after: get.index })
          .find(
            (p) =>
              p.socketId === get.socketId &&
              p.metricsBytes > 0 &&
              p.writeAt > get.response.responseAt
          )
      )
      const rejected = observer
        .readTranscript()
        .filter((r) => r.apiKey === 72 && !r.terminating && r.response?.errorCode)
      const notes = segments.journal().slice(-3)
      expect(notes.map((n) => n.type)).toEqual(['select', 'disconnect-resolved', 'connect-called'])
      expect(notes[2].seq).toBe(notes[1].seq + 1)
      expect(cachedUuid).toBe(uuid)
      expect(observedAt).toBeLessThanOrEqual(get.writeAt)
      expect(rejected.length).toBeLessThanOrEqual(1)
      expect(rejected.every((r) => r.response.errorCode === 42)).toBe(true)
      expect(faults.telemetry()).toHaveLength(1)
      expect(faults.telemetry()[0].segment).toBe(cycle.to.id)
      await windows.arrive(round)
      const next = this.lifecycle(observer, uuid, get.socketId)
      const retired = cacheRetirement({
        logs: windows.episode(round - 1, round),
        node: current.node,
        localPort: LISTENER[current.port],
        get: current.get,
        connect: current.connect,
        uuid,
        since: windows.chunks[round - 1].since,
        completedAt: new Date().toISOString(),
        source: this.source,
      })
      expect(retired.source).toBe('broker-disconnect-trace')
      expect(retired.connectionIdentity).toEqual(current.identity)
      rounds.push({
        round,
        from: cycle.from.id,
        to: cycle.to.id,
        cachedUuid,
        observedAt,
        get: get.requestKey,
        push: push.requestKey,
        cacheEvent: stableCacheEvent(retired),
      })
      next.identity = this.serverIdentity(next, round, windows)
      current = next
    }
    await this.drained(member, true)
    this.results.journal[kind] = segments.journal()
  }

  /**
   * A zero-timeout close of a supported kind sends no final.
   * @param {object} member the observed client of a supported kind
   * @returns {Promise<void>} after the close and its evidence checks
   */
  async timeoutZero(member) {
    const { kind, segments } = member
    const before = await this.establish(member)
    await this.within(8000, `The ${kind} zero-timeout close`, () =>
      segments.close(member.client, { timeout: 0 })
    )
    this.results.timeout0[kind] = { uuid: before.uuid, finalZeroTimeoutWrites: 0 }
  }

  /**
   * The captured telemetry socket of an admin is blackholed while foreground metadata work runs
   * beside the close: that work succeeds with its original result, and the telemetry cleanup closes
   * within its own cap plus the scheduling allowance. The admin close has no finite budget, so the
   * cap is the inherited final cap.
   * @returns {Promise<void>} after the blackholed close and its evidence checks
   */
  async blackhole() {
    const member = this.build('admin', 'bh')
    const before = await this.establish(member)
    const [captured] = member.faults.telemetry()
    member.segments.current().observer.declareFault(captured.ordinal)
    captured.socket.blackhole()
    const { client } = member
    const original = client.describeCluster
    const options = { timeout: 3000 }
    let witnessed
    client.describeCluster = function (...args) {
      witnessed = { thisArg: this, args, result: Reflect.apply(original, this, args) }
      return witnessed.result
    }
    const started = clock.now()
    let core
    let closing
    try {
      core = client.describeCluster(options)
      closing = member.segments.close(client)
    } finally {
      client.describeCluster = original
    }
    const [cluster] = await this.within(10000, 'The core work and the admin close', () =>
      Promise.all([core, closing])
    )
    const coreCloseMs = clock.now() - started
    await this.find('the blackholed telemetry socket close', 3000, () => captured.closed)
    const telemetryCleanupMs = captured.closedAt - started
    expect(cluster.brokers.map((b) => b.port).sort()).toEqual(PORTS)
    expect(telemetryCleanupMs).toBeLessThanOrEqual(FINAL_CAP_MS + SCHEDULING_MS)
    this.results.blackhole = {
      uuid: before.uuid,
      faultedSocket: before.ordinal,
      coreCloseMs,
      telemetryCleanupMs,
      core: {
        sameResult:
          witnessed.thisArg === client &&
          witnessed.args[0] === options &&
          witnessed.result === core &&
          (await witnessed.result) === cluster,
      },
    }
  }

  /**
   * Closes every client's terminal segment and seals every segment, all kinds concurrently.
   * @returns {Promise<void>} when every segment is sealed
   * @throws {Error|AggregateError} every failure, after all were attempted
   */
  async finishAll() {
    const outcomes = await Promise.allSettled(
      this.members.map((m) => m.segments.finish(m.client, this.closeOptions(m.kind)))
    )
    const errors = outcomes.filter((r) => r.status === 'rejected').map((r) => r.reason)
    if (errors.length === 1) throw errors[0]
    if (errors.length) throw new AggregateError(errors, 'Segment cleanup failed')
  }

  /**
   * Waits the quiet window after every client and observer is gone, then requires the event-loop
   * and owned-socket identities of the baseline, and no owned socket or segment left.
   * @param {object} baseline the snapshot taken before anything was built
   * @returns {Promise<void>} after the comparison; the readings are kept in the case results
   */
  async quiet(baseline) {
    await this.release(QUIET_MS + 2000, 'The quiet window', (scope) => delay(scope, QUIET_MS))
    const final = await this.probe.snapshot()
    this.results.resources = { baseline, final }
    expect(this.members.every((m) => m.faults.records.every((r) => r.closed))).toBe(true)
    expect(this.gate.occupancy()).toEqual({ running: 0, queued: 0 })
    expect(final.identities).toEqual({ openSockets: [], undischarged: [] })
    expect(final.activeResources).toEqual(baseline.activeResources)
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
    const runStart = clock.now()
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
    const windows = new TraceWindows({
      logs: this.logs,
      members: KINDS.length,
      within: (ms, label, op) => this.within(ms, label, op),
    })
    this.windows = windows
    await withTelemetryResource(aux, { name: this.stem, entries }, async () => {
      windows.open()
      const ordinary = KINDS.map((kind) => this.build(kind, 'ep'))
      const zero = SUPPORTED_TIMEOUT0.map((kind) => this.build(kind, 't0'))
      this.defer(() => this.finishAll())
      try {
        await Promise.all([
          ...ordinary.map((member) => this.rounds(member, windows)),
          ...zero.map((member) => this.timeoutZero(member)),
        ])
      } catch (error) {
        windows.fail(error)
        throw error
      }
      await this.blackhole()
      await this.finishAll()
    })
    this.results.workMs = clock.now() - runStart
  }

  /** @returns {void} hands the case record to the shared persistence and verification */
  finishRecord() {
    const manifest = JSON.parse(fs.readFileSync(MANIFEST_FILE, 'utf8'))
    const { name } = SCHEDULES[SCENARIO]
    this.attempt.record = {
      oracles: {
        traces: {
          source: this.source,
          loggerReadbacks: this.logs.receipts,
          chunks: this.windows.chunks,
        },
      },
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
      allowed: {
        errors: Object.fromEntries(
          KINDS.flatMap((kind) =>
            Array.from({ length: SCHEDULES[SCENARIO].perKind }, (_, index) => [
              name(kind, index),
              [42],
            ])
          )
        ),
        faultedSockets: { 'admin-bh': [this.results.blackhole.faultedSocket] },
      },
    }
  }
}

const BUDGET = 118000
live(
  'close and reopen',
  liveCase(BUDGET, (scope) =>
    runCase(SCENARIO, async (cleanup, publish, attempt) => {
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
      const baseline = await scenario.probe.snapshot()
      cleanup.defer(() => scenario.quiet(baseline))
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
        name: SCENARIO,
        directory,
        observers: {},
        record: attempt.record,
      })
    })
  ),
  caseTimeout(BUDGET)
)

const CHURN_BUDGET = 538000

/**
 * Gives this process a real collection function without `--expose-gc`, for the parent's check only:
 * every worker is forked with the flag and takes its own readings.
 * @returns {function():void} restores the previous state
 */
const exposeCollection = () => {
  const had = Object.getOwnPropertyDescriptor(globalThis, 'gc')
  if (typeof globalThis.gc !== 'function') {
    v8.setFlagsFromString('--expose-gc')
    globalThis.gc = vm.runInNewContext('gc')
  }
  return () => {
    if (!had) delete globalThis.gc
  }
}

/**
 * L9: the same `main('telemetry')` owner the CLI runs, with its rows captured into one stamped
 * scenario record. Four one-client workers are judged on their own memory and exact resource
 * identities; the parent's independent record oracle and the natural exit of every child are
 * required. A shortened or forced run cannot satisfy it.
 */
live(
  'telemetry churn',
  liveCase(CHURN_BUDGET, (scope) =>
    runCase('telemetry-churn', async (cleanup, publish, attempt) => {
      const directory = process.env.TELEMETRY_EVIDENCE_DIR
      if (!process.env.KAFKA_VERSION) throw new Error('KAFKA_VERSION must name the broker line')
      if (!directory || !path.isAbsolute(directory))
        throw new Error('TELEMETRY_EVIDENCE_DIR must name the absolute run directory')
      const {
        main,
        buildTelemetryChurn,
        TelemetryResourceVerdict,
      } = require('../../../scripts/soak/soak')
      const rows = []
      const restore = exposeCollection()
      cleanup.defer(() => restore())
      const result = await bounded(
        scope,
        CHURN_BUDGET - 10000,
        () =>
          main('telemetry', {
            brokers: PORTS.map((port) => `localhost:${port}`),
            telemetry: (options) =>
              buildTelemetryChurn({ ...options, record: (row) => rows.push(row) }),
          }),
        { label: 'The telemetry churn' }
      )
      attempt.stage = 'verification'
      expect(result.clientKinds).toEqual(['producer', 'consumer', 'admin', 'shareConsumer'])
      expect(result.workers).toHaveLength(4)
      for (const worker of result.workers) {
        expect(worker.input.clientCount).toBe(1)
        expect(worker.input.samples.length).toBeGreaterThanOrEqual(10)
        expect(worker.input.measurementMs).toBeGreaterThanOrEqual(300000)
        expect(TelemetryResourceVerdict.evaluate(worker.input).ok).toBe(true)
        expect(worker.exit).toMatchObject({
          event: 'close',
          code: 0,
          signal: null,
          stderrBytes: 0,
          unhandled: 0,
          watchdogExpired: false,
        })
        expect(worker.exit.afterDisconnectMs).toBeLessThanOrEqual(10000)
      }
      expect(result.oracle.ok).toBe(true)
      expect(result.cleanupMs).toBeLessThanOrEqual(120000)
      expect(result.totalMs).toBeLessThanOrEqual(540000)
      expect(result.ok).toBe(true)
      attempt.record = {
        ...attempt.record,
        run: {
          ...attempt.record.run,
          composeSha256: fileSha(path.join(ROOT, 'docker-compose.telemetry.yml')),
        },
        results: {
          rows,
          verdict: {
            ok: result.ok,
            setupMs: result.setupMs,
            cleanupMs: result.cleanupMs,
            totalMs: result.totalMs,
          },
        },
      }
      publish({ name: 'telemetry-churn', directory, observers: {}, record: attempt.record })
    })
  ),
  caseTimeout(CHURN_BUDGET)
)
