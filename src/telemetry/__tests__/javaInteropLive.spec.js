const crypto = require('crypto')
const fs = require('fs')
const net = require('net')
const os = require('os')
const path = require('path')
const { performance } = require('perf_hooks')
const { spawn } = require('child_process')
const { Kafka, logLevel } = require('../../../index')
const {
  assertJavaRows,
  assertJsCounter,
  javaClosePolicy,
  assertJavaEvidence,
  assertJavaCaptures,
  readNodeExports,
  assertJavaSubscription,
  JAVA_REQUESTED,
  JavaAdminHandshake,
  unexpectedPinnedJavaStderr,
  javaSampleSummary,
} = require('../../../scripts/e2e/telemetry/semantics')
const { runCase } = require('../../../scripts/e2e/telemetry/evidence')
const {
  TelemetryExportReader,
  TelemetryWireObserver,
  canonicalUuid,
  withTelemetryResource,
} = require('../../../testHelpers/telemetryFixtures')
const { readLog } = require('../../../testHelpers/rawLog')
const {
  bounded,
  caseTimeout,
  liveCase,
  liveRun,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')

// Gated: the configured fixture (docker-compose.telemetry.yml), a JDK 21 and the pinned Java
// interop cache. A missing toolchain fails the case: it is unrun acceptance, never a passing skip.
const live = process.env.TELEMETRY_E2E ? test : test.skip

const ROOT = path.resolve(__dirname, '../../..')
const RUNNER = path.join(ROOT, 'scripts/e2e/telemetry/run.sh')
const MANIFEST_FILE = path.join(ROOT, 'scripts/e2e/telemetry/fixtures/manifest.json')
const PORTS = [19092, 19095, 19098]
const ROLES = ['producer', 'consumer', 'admin']
const KINDS = ['producer', 'consumer', 'admin', 'share']
const WHITELIST = [
  'client_rack',
  'group_id',
  'group_instance_id',
  'group_member_id',
  'transactional_id',
]
const OUTPUT_CAP = 65536
const CLOSE_TO_EXIT_MS = 10000
const clock = { now: () => performance.now() }

/** Plaintext IPv4 socket for the telemetry-disabled auxiliary admin and feeder. */
const ipv4Factory = ({ host, port, onConnect }) => {
  const socket = net.connect({ host, port, family: 4 }, onConnect)
  socket.setKeepAlive(true, 60000)
  return socket
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
 * @param {string} file absolute path
 * @returns {string} SHA-256 of the file
 */
const fileSha = (file) => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex')

/**
 * One Java role process under `timeout`, whose process group the case proves empty afterwards.
 * Every observer is attached before the child can emit; stdout markers carry the parent's
 * monotonic receive time, which is the only clock compared with JS observations.
 */
class JavaRole {
  /**
   * @param {object} options the role's identity
   * @param {string} options.role producer, consumer or admin
   * @param {string} options.clientId assigned Kafka client id
   * @param {string} options.topic the role's own topic
   * @param {string} options.group the role's own group
   * @param {string} options.tmp private temporary directory of this child
   */
  constructor({ role, clientId, topic, group, tmp }) {
    this.role = role
    this.clientId = clientId
    this.markers = []
    this.stdout = ''
    this.stderr = ''
    this.overflow = false
    let partial = ''
    this.child = spawn(
      role === 'admin' ? 'bash' : 'timeout',
      [
        ...(role === 'admin'
          ? [
              '-o',
              'pipefail',
              '-c',
              'cat | "$@"',
              'telemetry-admin-input',
              'timeout',
              '--foreground',
            ]
          : []),
        '-k',
        '3s',
        '45s',
        'bash',
        RUNNER,
        'client',
        '4.3.1',
        role,
        PORTS.map((port) => `localhost:${port}`).join(','),
        clientId,
        topic,
        group,
        '5000',
        'active-close',
      ],
      {
        env: { ...process.env, TMPDIR: tmp },
        stdio: [role === 'admin' ? 'pipe' : 'ignore', 'pipe', 'pipe'],
        detached: true,
      }
    )
    this.child.stdout.setEncoding('utf8')
    this.child.stderr.setEncoding('utf8')
    this.child.stdout.on('data', (data) => {
      if (this.stdout.length + data.length > OUTPUT_CAP) this.overflow = true
      else this.stdout += data
      if (this.overflow) return
      partial += data
      const lines = partial.split('\n')
      partial = lines.pop()
      for (const line of lines) this.markers.push({ at: clock.now(), line })
    })
    this.child.stderr.on('data', (data) => {
      if (this.stderr.length + data.length > OUTPUT_CAP) this.overflow = true
      else this.stderr += data
    })
    this.child.stdin?.on('error', (error) => {
      this.inputError = error
    })
    this.done = new Promise((resolve) => {
      this.child.once('error', (error) => resolve({ code: null, signal: null, error }))
      this.child.once('close', (code, signal) =>
        resolve({
          code,
          signal,
          exitAt: clock.now(),
          pipesDrained: !partial,
          supervisorFailure: code !== 0 || signal !== null,
        })
      )
    })
  }

  /**
   * @param {string} key a marker field such as `clientInstanceId`, `closing` or `closed`
   * @returns {{at:number,fields:object}|undefined} the first stdout JSON line that carries it
   */
  marker(key) {
    const found = this.markers
      .map(({ at, line }) => ({ at, fields: JSON.parse(line) }))
      .filter(
        ({ fields }) =>
          Object.hasOwn(fields, key) &&
          (key !== 'clientInstanceId' || fields.closed === false) &&
          (!['closing', 'closed'].includes(key) || fields[key] === true)
      )
    if (found.length > 1) throw new Error(`Duplicate Java ${key}`)
    return found[0]
  }

  /** @returns {Promise<boolean>} whether the supervising group has gone, probed for one second */
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
 * L7: seven distinct clients under one subscription, three of them Java 4.3.1 roles whose own
 * UUID must show complete exports across all brokers. Producer/consumer require a final; admin
 * requires its acknowledged fresh checkpoint, public close, natural exit and all-node cessation.
 */
class JavaInteropCase {
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
    this.line = line
    this.directory = directory
    this.stem = stem
    this.attempt = attempt
    this.reader = new TelemetryExportReader(directory)
    this.results = { javaEvidenceVersion: 2, java: {}, js: {} }
    this.attempt.record.results = this.results
    this.pushes = []
    this.topics = Object.fromEntries(ROLES.map((role) => [role, `${stem}-j${role[0]}`]))
    this.ids = {
      java: Object.fromEntries(ROLES.map((role) => [role, `${stem}-java-${role}`])),
      js: Object.fromEntries(KINDS.map((kind) => [kind, `${stem}-js-${kind}`])),
    }
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
   * Builds one JS client on an observed socket factory; release order is disconnect, dispose,
   * require clean.
   * @param {string} kind producer, consumer, admin or share
   * @returns {{client:object,observer:object}} the observed client
   */
  buildJs(kind) {
    const observer = new TelemetryWireObserver({
      completeStream: true,
      directory: this.directory,
      line: this.line,
      clock,
      mode: 'configured',
      owner: `js-${kind}`,
    })
    this.attempt.observers[`js-${kind}`] = observer
    const kafka = new Kafka({
      clientId: this.ids.js[kind],
      brokers: PORTS.map((port) => `localhost:${port}`),
      socketFactory: (options) => observer.socketFactory(options),
      logLevel: logLevel.NOTHING,
    })
    const common = { enableMetricsPush: true }
    const client =
      kind === 'producer'
        ? kafka.producer({ ...common, idempotent: false })
        : kind === 'admin'
          ? kafka.admin(common)
          : kind === 'share'
            ? kafka.shareConsumer({ ...common, groupId: `${this.stem}-js-share-group` })
            : kafka.consumer({
                ...common,
                groupId: `${this.stem}-js-group`,
                sessionTimeout: 30000,
                heartbeatInterval: 3000,
                maxWaitTimeInMs: 500,
              })
    this.defer(() => observer.assertClean({ allowUnanswered: observer.cancellationKeys() }))
    this.defer(() => observer.dispose())
    this.defer(() => observer.closeClient(client))
    return { client, observer }
  }

  /**
   * Starts one Java role, registers its release first and returns before anything can be missed.
   * @param {string} role producer, consumer or admin
   * @returns {JavaRole} the running role
   */
  startRole(role) {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), `telemetry-java-${role}-`))
    this.defer(() => fs.rmSync(tmp, { recursive: true, force: true }))
    const javaRole = new JavaRole({
      role,
      clientId: this.ids.java[role],
      topic: this.topics[role],
      group: `${this.stem}-java-${role}-group`,
      tmp,
    })
    this.defer(async () => {
      javaRole.child.stdin?.destroy()
      const exit = await javaRole.done
      const gone = await javaRole.groupGone()
      this.results.children ??= {}
      this.results.children[role] = {
        ...exit,
        liveDescendants: gone ? [] : [javaRole.child.pid],
        overflow: javaRole.overflow,
        stdout: javaRole.stdout,
        stderr: javaRole.stderr,
        markers: javaRole.markers,
      }
      if (!gone) throw new Error(`The ${role} process group survived`)
    })
    return javaRole
  }

  /**
   * @param {string} uuid a Java instance UUID
   * @returns {object[]} its sink rows across all nodes, oldest first, finals after equal times
   */
  rowsOf(uuid) {
    return this.reader
      .readExports({ uuid })
      .sort(
        (a, b) => a.tsMs - b.tsMs || Number(a.terminating) - Number(b.terminating) || a.seq - b.seq
      )
  }

  /**
   * Checks the narrow Java export contract of one role from its complete independent rows.
   * @param {string} role producer, consumer or admin
   * @param {string} uuid the role's canonical UUID
   * @returns {object} compact role evidence
   */
  checkRows(role, uuid) {
    const rows = this.rowsOf(uuid)
    const normal = rows.filter((r) => !r.terminating)
    const finals = rows.filter((r) => r.terminating)
    expect(normal.length).toBeGreaterThanOrEqual(2)
    expect(rows.every((r) => r.parse === 'ok' && r.truncated === false)).toBe(true)
    expect(rows.every((r) => r.clientId === this.ids.java[role] && r.pushIntervalMs === 1000)).toBe(
      true
    )
    const child = this.roles[role]
    assertJavaRows(role, rows, {
      clientId: this.ids.java[role],
      group: `${this.stem}-java-consumer-group`,
      membership: child.stdout
        .trim()
        .split('\n')
        .map(JSON.parse)
        .filter((r) => r.membership === true),
      closing: child.marker('closing')?.fields,
      closed: child.marker('closed')?.fields,
      checkpoint: role === 'admin' ? this.adminProof.checkpoint : undefined,
      cessation: role === 'admin' ? this.adminProof.cessation : undefined,
    })
    const sum = normal[0].metrics.find(
      (m) =>
        m.name ===
        `org.apache.kafka.${role === 'admin' ? 'admin.client' : role}.connection.creation.total`
    )
    return {
      uuid,
      normalExports: normal.length,
      finalExports: finals.length,
      timing: javaSampleSummary(rows),
      exports: rows.map(({ node, seq, terminating, dataSha256 }) => ({
        node,
        seq,
        terminating,
        dataSha256,
      })),
      counter: { kind: sum.kind, monotonic: sum.monotonic, temporality: sum.temporality },
    }
  }

  /**
   * Captures all sink prefixes after reader integrity checks, including other clients' rows.
   * @returns {object} timed read receipt, node byte hashes and sequence cursors
   * @throws {Error} for a missing, partial or corrupt node capture
   */
  capture() {
    this.reader.readExports()
    const nodes = {}
    const cursors = {}
    for (const node of ['1', '2', '3']) {
      const bytes = readNodeExports(this.directory, node)
      if (bytes.length && bytes.at(-1) !== 10) throw new Error('Partial Java node capture')
      const rows = bytes.length ? bytes.toString().trimEnd().split('\n').map(JSON.parse) : []
      nodes[node] = {
        bytes: bytes.length,
        sha256: crypto.createHash('sha256').update(bytes).digest('hex'),
      }
      cursors[node] = rows.length
    }
    return { at: clock.now(), nodes, cursors }
  }

  /**
   * Releases the admin only after a completed operation and a fresh complete accepted export.
   * Started immediately with the child; an early exit rejects the checkpoint without a later wait.
   * @returns {Promise<void>} after the single command is flushed and its input pipe is closed
   * @throws {Error} for child failure, deadline, capture failure or invalid checkpoint
   */
  async adminCheckpoint() {
    const child = this.roles.admin
    const handshake = new JavaAdminHandshake({
      child,
      clientId: this.ids.java.admin,
      signal: this.signal,
      now: clock.now,
      find: async (signal, label, ms, lookup) => {
        let found
        await pollUntil(signal, ms, async () => (found = await lookup()), { delayMs: 100, label })
        return found
      },
      capture: () => this.capture(),
      rowsOf: (uuid) => this.rowsOf(uuid),
      send: (raw) =>
        this.within(
          1000,
          'The admin close command',
          () =>
            new Promise((resolve, reject) => {
              child.child.stdin.end(raw, (error) => (error ? reject(error) : resolve()))
            })
        ),
    })
    this.adminHandshake = handshake
    const task = handshake.run()
    this.defer(async () => {
      handshake.abort()
      child.child.stdin.destroy()
      await task.catch(() => {})
    })
    try {
      this.adminProof = { checkpoint: await task }
      this.results.adminProof = this.adminProof
      if (child.inputError) throw child.inputError
    } catch (error) {
      child.child.stdin.destroy()
      throw error
    }
  }

  /**
   * Observes fixed drain and quiet windows with the subscription and reporter still installed.
   * @returns {Promise<void>} after both windows, charged to the existing case budget
   * @throws {Error} for incomplete capture or a row during quiet
   */
  async adminCessation() {
    const window = async (start) => {
      const reads = [start]
      while (clock.now() - start.at < 2000) {
        await this.within(
          300,
          'The admin capture interval',
          () => new Promise((resolve) => setTimeout(resolve, 100))
        )
        reads.push(this.capture())
      }
      return reads
    }
    const drain = await window(this.capture())
    const quiet = await window(drain.at(-1))
    this.adminProof.cessation = { drain, quiet }
    assertJavaCaptures([this.adminProof.checkpoint.proof, ...drain, ...quiet], this.directory)
  }

  /**
   * @returns {Promise<void>} when the interoperability scenario and its evidence are complete
   */
  async run() {
    const cache = process.env.TELEMETRY_INTEROP_CACHE
    if (!cache || !path.isAbsolute(cache))
      throw new Error('TELEMETRY_INTEROP_CACHE must name the absolute pinned Java interop cache')
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
    expect(
      await this.within(15000, 'The control topics', () =>
        aux.createTopics({
          topics: Object.values(this.topics).map((topic) => ({
            topic,
            numPartitions: 1,
            replicationFactor: 3,
          })),
          waitForLeaders: true,
        })
      )
    ).toBe(true)
    const feeder = kafka.producer({ enableMetricsPush: false, idempotent: true })
    this.defer(() => feeder.disconnect())
    await this.within(10000, 'The consumer seed', async () => {
      await feeder.connect()
      await feeder.send({
        topic: this.topics.consumer,
        acks: -1,
        messages: [{ key: 'seed', value: 'seed', partition: 0 }],
      })
    })
    const all = [...Object.values(this.ids.java), ...Object.values(this.ids.js)]
    const entries = {
      metrics: JAVA_REQUESTED,
      'interval.ms': '1000',
      match: `client_id=^(${all.join('|')})$`,
    }
    this.results.subscriptionOperations = []
    const resourceAdmin = {
      listConfigResources: (options) => aux.listConfigResources(options),
      describeConfigs: (options) => aux.describeConfigs(options),
      incrementalAlterConfigs: async (request) => {
        const operation = { request, startedAt: clock.now() }
        this.results.subscriptionOperations.push(operation)
        operation.response = await aux.incrementalAlterConfigs(request)
        operation.completedAt = clock.now()
        return operation.response
      },
    }
    this.results.subscription = await withTelemetryResource(
      resourceAdmin,
      { name: this.stem, entries },
      () => this.interop(aux),
      { signal: this.signal }
    )
  }

  /** @returns {void} validates actual pinned cache and records candidate/JDK identities */
  provenance() {
    const cache = process.env.TELEMETRY_INTEROP_CACHE
    this.results.provenance = {
      clientVersion: '4.3.1',
      jdk: Object.fromEntries(ROLES.map((role) => [role, this.roles[role].marker('jdk')?.fields])),
      cache: fs
        .readFileSync(path.join(ROOT, 'scripts/e2e/telemetry/interop.sha256'), 'utf8')
        .trim()
        .split('\n')
        .map((line) => {
          const [expected, name] = line.trim().split(/\s+/)
          const actual = fileSha(path.join(cache, name))
          expect(actual).toBe(expected)
          return { name, sha256: actual, bytes: fs.statSync(path.join(cache, name)).size }
        }),
      sourceSha256: fileSha(path.join(ROOT, 'scripts/e2e/telemetry/JavaTelemetryClient.java')),
      runnerSha256: fileSha(RUNNER),
    }
    this.results.readback = {
      metrics: JAVA_REQUESTED,
      interval: 1000,
      ids: [...Object.values(this.ids.java), ...Object.values(this.ids.js)],
    }
  }

  /**
   * Runs the seven clients inside the owned subscription.
   * @param {object} aux telemetry-disabled admin for the control reads
   * @returns {Promise<void>} when every assertion on all seven clients holds
   */
  async interop(aux) {
    const roles = (this.roles = Object.fromEntries(
      ROLES.map((role) => [role, this.startRole(role)])
    ))
    const checkpointTask = this.adminCheckpoint().then(async () => {
      await roles.admin.done
      await this.adminCessation()
    })
    checkpointTask.catch(() => {})
    this.defer(async () => {
      this.adminHandshake.abort()
      roles.admin.child.stdin.destroy()
      await checkpointTask.catch(() => {})
    })
    const js = Object.fromEntries(KINDS.map((kind) => [kind, this.buildJs(kind)]))
    await this.within(15000, 'The JS connects', () =>
      settle(KINDS.map((kind) => () => js[kind].client.connect()))
    )
    await this.within(10000, 'The JS admin work', () => js.admin.client.describeCluster())
    const jsIds = {}
    await this.within(10000, 'The JS instance ids', () =>
      settle(
        KINDS.map((kind) => async () => {
          jsIds[kind] = await js[kind].client.clientInstanceId({ timeout: 8000 })
        })
      )
    )
    const javaIds = {}
    for (const role of ROLES) {
      const id = await this.find(`the ${role} instance id`, 30000, () =>
        roles[role].marker('clientInstanceId')
      )
      javaIds[role] = canonicalUuid(id.fields.clientInstanceId)
    }
    const every = [...Object.values(jsIds), ...Object.values(javaIds)]
    expect(every.every((id) => id !== '00000000-0000-0000-0000-000000000000')).toBe(true)
    expect(new Set(every.map(canonicalUuid)).size).toBe(7)
    for (const kind of KINDS)
      expect(await js[kind].client.clientInstanceId({ timeout: 0 })).toBe(jsIds[kind])

    const jsPushes = {}
    for (const kind of KINDS) {
      jsPushes[kind] = await this.find(`two accepted ${kind} Pushes`, 30000, () => {
        const list = js[kind].observer
          .accepted({ uuid: jsIds[kind] })
          .filter((p) => p.metricsBytes > 0)
        return list.length >= 2 && list
      })
    }
    await checkpointTask
    this.provenance()
    const exits = {}
    for (const role of ROLES)
      exits[role] = await this.within(45000, `The ${role} exit`, () => roles[role].done)
    for (const role of ROLES) {
      exits[role].liveDescendants = (await roles[role].groupGone()) ? [] : [roles[role].child.pid]
    }
    this.results.children = Object.fromEntries(
      ROLES.map((role) => [
        role,
        {
          ...exits[role],
          overflow: roles[role].overflow,
          stdout: roles[role].stdout,
          stderr: roles[role].stderr,
          markers: roles[role].markers,
        },
      ])
    )
    for (const role of ROLES) {
      const child = roles[role]
      const closing = child.marker('closing')
      const closed = child.marker('closed')
      expect(child.overflow).toBe(false)
      expect(unexpectedPinnedJavaStderr(child.stderr)).toEqual([])
      expect(exits[role].error).toBeUndefined()
      expect(exits[role].code).toBe(0)
      expect(exits[role].signal).toBeNull()
      expect(closing).toBeDefined()
      expect(closed).toBeDefined()
      const closeToExitMs = exits[role].exitAt - closing.at
      expect(closeToExitMs).toBeGreaterThanOrEqual(0)
      expect(closeToExitMs).toBeLessThanOrEqual(CLOSE_TO_EXIT_MS)
      const liveDescendants = (await child.groupGone()) ? [] : [child.child.pid]
      expect(liveDescendants).toEqual([])
      if (role !== 'admin')
        await this.find(`the ${role} final export`, 10000, () =>
          this.rowsOf(javaIds[role]).some((r) => r.terminating)
        )
      this.results.java[role] = {
        assignedId: this.ids.java[role],
        closePolicy: javaClosePolicy(role, this.results.provenance, {
          clientSourceSha256: this.results.provenance.sourceSha256,
          runnerSha256: this.results.provenance.runnerSha256,
        }),
        ...(role === 'admin' ? this.adminProof : {}),
        ...this.checkRows(role, javaIds[role]),
        exitCode: exits[role].code,
        exitSignal: exits[role].signal,
        closeToExitMs,
        liveDescendants,
        closing: closing.fields,
        closed: closed.fields,
        markers: child.markers,
        stdout: child.stdout,
        stderr: child.stderr,
      }
      assertJavaEvidence(
        role,
        this.rowsOf(javaIds[role]),
        this.results.java[role],
        this.results.children[role],
        {
          provenance: this.results.provenance,
          fixture: {
            clientSourceSha256: this.results.provenance.sourceSha256,
            runnerSha256: this.results.provenance.runnerSha256,
          },
          readback: this.results.readback,
          group: `${this.stem}-java-consumer-group`,
        }
      )
    }

    const jsRows = {}
    for (const kind of KINDS) {
      const { observer } = js[kind]
      observer.assertHealthy()
      const gets = observer.subscriptions({ uuid: jsIds[kind] }).filter((r) => r.response)
      expect(gets.length).toBeGreaterThan(0)
      for (const get of gets) assertJavaSubscription(get.response)
      const counter = `org.apache.kafka.${kind === 'share' ? 'consumer' : kind}.connection.creation.total`
      const decodedSamples = []
      for (const push of jsPushes[kind]) {
        const item = await observer.decodeAccepted(push)
        assertJsCounter(kind, item.decoded)
        this.pushes.push({ owner: `js-${kind}`, requestKey: push.requestKey })
        decodedSamples.push(item)
        const names = item.decoded.metrics.map((m) => m.name)
        expect(names).toContain(counter)
        expect(
          names.every((name) => JAVA_REQUESTED.split(',').some((prefix) => name.startsWith(prefix)))
        ).toBe(true)
        expect(
          item.decoded.resources
            .flatMap((r) => r.attrs.map(([key]) => key))
            .every((key) => WHITELIST.includes(key))
        ).toBe(true)
      }
      const metric = decodedSamples[0].decoded.metrics.find((m) => m.name === counter)
      expect(metric).toMatchObject({ kind: 'SUM', monotonic: true, unit: '1' })
      expect(metric.temporality).toBe('AGGREGATION_TEMPORALITY_DELTA')
      jsRows[kind] = { uuid: jsIds[kind], counter: metric.name, temporality: metric.temporality }
      if (kind !== 'share') {
        const java = this.results.java[kind]
        expect(java.counter).toEqual({
          kind: metric.kind,
          monotonic: metric.monotonic,
          temporality: metric.temporality,
        })
      }
    }
    await settle(KINDS.map((kind) => () => js[kind].observer.closeClient(js[kind].client)))
    for (const kind of KINDS)
      for (const push of js[kind].observer
        .accepted({ uuid: jsIds[kind] })
        .filter((p) => p.metricsBytes > 0))
        assertJsCounter(kind, (await js[kind].observer.decodeAccepted(push)).decoded)
    this.results.js = jsRows
    this.results.uuids = { java: javaIds, js: jsIds }
    await this.controls(aux)
  }

  /**
   * The separately tracked record-control setup: exact Java producer records and one committed
   * consumer seed. No cross-language record delivery is claimed.
   * @param {object} aux telemetry-disabled admin
   * @returns {Promise<void>} when the control records and commit are as expected
   */
  async controls(aux) {
    const log = (topic) => readLog({ brokers: [`localhost:${PORTS[0]}`], topic })
    const [produced] = [
      ...(
        await this.within(15000, 'The producer topic read', () => log(this.topics.producer))
      ).values(),
    ]
    expect(produced.records.map((r) => [r.key.toString(), r.value.toString()])).toEqual([
      ['k', 'v'],
      ['k', 'last'],
    ])
    const [seeded] = [
      ...(
        await this.within(15000, 'The consumer topic read', () => log(this.topics.consumer))
      ).values(),
    ]
    expect(seeded.records.map((r) => r.key.toString())).toEqual(['seed'])
    const fetched = await this.within(10000, 'The consumer commit read', () =>
      aux.fetchOffsets({
        groupId: `${this.stem}-java-consumer-group`,
        topics: [this.topics.consumer],
        resolveOffsets: false,
      })
    )
    const rows = fetched[0]?.partitions ?? fetched
    expect(rows.map((r) => [r.partition, String(r.offset)])).toEqual([[0, '1']])
    this.results.controls = { producerRecords: 2, seedCommit: '1' }
  }

  /** @returns {void} hands the case record to the shared persistence and verification */
  finishRecord() {
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
        runnerSha256: fileSha(RUNNER),
        clientSourceSha256: fileSha(
          path.join(ROOT, 'scripts/e2e/telemetry/JavaTelemetryClient.java')
        ),
      },
      results: this.results,
      pushes: this.pushes,
      allowed: { errors: {}, faultedSockets: {} },
    }
  }
}

const BUDGET = 148000
live(
  'java telemetry interoperability',
  liveCase(BUDGET, (scope) =>
    runCase('telemetry-java', async (cleanup, publish, attempt) => {
      const line = process.env.KAFKA_VERSION
      const directory = process.env.TELEMETRY_EVIDENCE_DIR
      if (!line) throw new Error('KAFKA_VERSION must name the broker line')
      if (!directory || !path.isAbsolute(directory))
        throw new Error('TELEMETRY_EVIDENCE_DIR must name the absolute run directory')
      const scenario = new JavaInteropCase({
        signal: scope,
        line,
        directory,
        stem: attempt.record.run.stem,
        attempt,
      })
      cleanup.defer(() => scenario.close())
      await bounded(
        scope,
        BUDGET - 30000,
        (inner) => {
          scenario.signal = inner
          return scenario.run()
        },
        { label: 'The Java interoperability scenario' }
      )
      attempt.stage = 'verification'
      scenario.finishRecord()
      publish({
        name: 'telemetry-java',
        directory,
        observers: attempt.observers,
        record: attempt.record,
      })
    })
  ),
  caseTimeout(BUDGET)
)
