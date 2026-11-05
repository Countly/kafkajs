const { getEventListeners } = require('events')
const { execFile } = require('child_process')
const { randomBytes } = require('crypto')
const fs = require('fs')
const { Kafka } = require('../../../../index')
const Broker = require('../../../broker')
const Pool = require('../../../network/connectionPool')
const socketFactory = require('../../../network/socketFactory')
const { createLogger, LEVELS } = require('../../../loggers')
const { createResourceProbe } = require('../../../../testHelpers/resourceProbe')
const path = require('path')
const { plainTextBrokers, writeEvidence, wireVersionsOf } = require('testHelpers')
const { composeArgs } = require('../../../../testHelpers/leaderElection')
const { CleanupStack } = require('../../../producer/__tests__/fixtures/liveProbe')
const {
  bounded,
  pollUntil,
  liveRun,
  NEVER,
} = require('../../../producer/__tests__/fixtures/bounded')

const { AsyncLocalStorage } = require('async_hooks')

/** Carries a cleanup's cancellation into nested guarded commands without cancelling later cleanup. */
class AdminCleanupStack extends CleanupStack {
  /** @param {AsyncLocalStorage<AbortSignal>} scope restoration-local cancellation scope */
  constructor(scope) {
    super()
    this.scope = scope
  }

  /**
   * Registers a restoration before mutation and binds its cancellation to nested commands.
   * @param {string} name restoration identity
   * @param {(signal: AbortSignal) => Promise<unknown>} close restoration operation
   * @returns {void}
   */
  add(name, close) {
    super.add(name, (signal) => this.scope.run(signal, () => close(signal)))
  }
}

/** Where raw receipts go when no guarded run directory is supplied; the gate uploads it. */
const DEFAULT_RECEIPT_DIR = path.join('.tmp', 'e2e')

/** The event-loop resource types a finished scenario must hand back as it found them. */
const HELD_RESOURCES = ['Timeout', 'TCPSocketWrap', 'TCPServerWrap']

/**
 * Loop turns a destroyed handle gets to finish closing before it counts as retained. A closed
 * socket or server stays in `process.getActiveResourcesInfo()` until its close callback has run,
 * which is after the turn that destroyed it, so a count taken in that turn still holds it.
 */
const CLOSE_TURNS = 10

/**
 * Owns one admin scenario, its absolute budget, clients and reverse-order restoration.
 * The scenario runs under a reviewed guard when `P9_CLUSTER_GUARD` and `P9_CLUSTER_MANIFEST` are
 * both set, and against the Compose cluster the process selected (`COMPOSE_FILE`) when neither is.
 */
class AdminLiveProbe {
  /**
   * Validates the ownership environment before allocating any client or issuing work.
   * @param {{stem: string, signal: AbortSignal, budgetMs: number}} options scenario identity/budget
   * @throws {Error} only one of guard and manifest set, unreadable guard files, aborted owner, or
   *   invalid budget
   */
  constructor({ stem, signal, budgetMs }) {
    liveRun.assertClean()
    signal.throwIfAborted()
    if (!Number.isSafeInteger(budgetMs) || budgetMs <= 0) throw new Error('Invalid scenario budget')
    this.guard = process.env.P9_CLUSTER_GUARD
    this.manifest = process.env.P9_CLUSTER_MANIFEST
    if (!this.guard !== !this.manifest) throw new Error('Incomplete cluster ownership environment')
    if (this.guard) {
      fs.accessSync(this.guard, fs.constants.R_OK)
      fs.accessSync(this.manifest, fs.constants.R_OK)
    }
    this.stem = stem
    this.signal = signal
    this.due = performance.now() + budgetMs
    this.reserve = ['admin-ledger', 'tiered-offsets'].includes(stem) ? 60000 : 30000
    if (stem === 'admin-lifecycle') this.reserve = 1000
    if (budgetMs <= this.reserve) throw new Error('Scenario budget must exceed cleanup reserve')
    this.cleanupScope = new AsyncLocalStorage()
    this.cleanup = new AdminCleanupStack(this.cleanupScope)
    this.effects = new AdminCleanupStack(this.cleanupScope)
    this.clients = []
    this.topics = []
    this.credentials = []
    this.requests = []
    this.logs = []
    this.sockets = []
    this.removers = []
    this.baseOptions = {
      clientId: 'e2e-live-admin',
      brokers: plainTextBrokers(),
      retry: { retries: 0 },
    }
    this.logger = createLogger({
      level: LEVELS.DEBUG,
      logCreator: () => (entry) => this.logs.push(entry),
    })
    this.factory = socketFactory()
    this.resourceProbe = createResourceProbe({
      sockets: () => ({
        created: this.sockets.length,
        destroyed: this.sockets.filter((socket) => socket.destroyed).length,
      }),
      activeResources: true,
    })
  }

  /** @returns {number} time remaining inside the original inclusive scenario deadline */
  remaining() {
    return Math.max(0, this.due - performance.now())
  }

  /**
   * Converts the time left into the integer milliseconds the public clients accept.
   * @param {number} [cap=Infinity] most milliseconds to allow
   * @param {number} [held=0] milliseconds to keep unspent, e.g. the cleanup reserve
   * @returns {number} whole milliseconds, never negative
   */
  wholeMs(cap = Infinity, held = 0) {
    return Math.max(0, Math.floor(Math.min(cap, this.remaining() - held)))
  }

  /**
   * Executes one lazy operation without retries or outcome translation.
   * @template T
   * @param {() => Promise<T>} work signal-checked operation
   * @returns {Promise<T>} original result
   * @throws {Error} original failure, abort, taint or insufficient mutation budget
   */
  invoke(work) {
    if (!this.cleaning) liveRun.assertClean()
    const signal = this.cleaning ? this.cleanupScope.getStore() || NEVER : this.signal
    signal.throwIfAborted()
    const allowance = this.remaining() - (this.cleaning ? 0 : this.reserve)
    if (allowance <= 0) throw new Error('Scenario cleanup reserve reached')
    return bounded(signal, allowance, work)
  }

  /**
   * Captures actual product sockets, retaining the standard socket implementation.
   * @param {object} options standard socket-factory options
   * @returns {import('net').Socket} the actual product socket
   */
  socket(options) {
    const socket = this.factory(options)
    this.sockets.push(socket)
    return socket
  }

  /**
   * Creates and connects an admin, registering bounded close before connection starts.
   * @param {object} [options] Kafka constructor overrides
   * @returns {Promise<import('../../../../types').Admin>} connected public admin
   * @throws {Error} no work allowance left, or original construction/connect failure
   */
  async start(options = {}) {
    const apiTimeout = this.wholeMs(60000, this.reserve)
    if (apiTimeout < 1) throw new Error('Scenario cleanup reserve reached')
    return this.client('admin', { apiTimeout }, options)
  }

  /**
   * Owns a public client before its first connection attempt.
   * @param {'admin'|'consumer'|'producer'} kind client factory
   * @param {object} [clientOptions] public factory options
   * @param {object} [options] Kafka constructor overrides
   * @returns {Promise<object>} connected public client
   * @throws {Error} original public-client failure
   */
  async client(kind, clientOptions = {}, options = {}) {
    return this.invoke(async () => {
      const kafka = new Kafka({
        ...this.baseOptions,
        ...options,
        logLevel: LEVELS.DEBUG,
        logCreator: () => (entry) => this.logs.push(entry),
        socketFactory: (args) => this.socket(args),
      })
      // The base options keep admin calls to one attempt; an idempotent producer needs retries.
      const settings =
        kind === 'producer' ? { retry: { retries: 5 }, ...clientOptions } : clientOptions
      const client = kafka[kind](settings)
      this.clients.push(client)
      this.cleanup.add(`${kind} disconnect`, () =>
        client.disconnect({ timeout: this.wholeMs(5000) })
      )
      this.removers.push(
        client.on(client.events.REQUEST, ({ payload }) => this.requests.push(payload))
      )
      await client.connect()
      return client
    })
  }

  /**
   * Creates a uniquely named topic, registering removal before the mutation. Removal reconnects the
   * creating admin first, because a scenario may have closed it, and runs before that admin's own
   * disconnect.
   * @param {object} [options] name suffix and public createTopics settings
   * @returns {Promise<string>} actual topic name
   * @throws {Error} broker failure or incomplete metadata identity
   */
  async topic({
    name = 'data',
    partitions = 1,
    replicationFactor = 2,
    configEntries = [],
    replicaAssignment,
  } = {}) {
    const admin = this.clients.find((client) => typeof client.createTopics === 'function')
    if (!admin) throw new Error('Start an admin before creating topics')
    const topic = `e2e-p9d-${this.stem}-${name}-${randomBytes(6).toString('hex')}`
    this.cleanup.add('delete topic', async () => {
      await admin.connect()
      await admin.deleteTopics({ topics: [topic] })
    })
    await this.invoke(() =>
      admin.createTopics({
        topics: [
          {
            topic,
            // Kafka refuses an explicit assignment together with a count or a factor (42).
            ...(replicaAssignment
              ? { replicaAssignment }
              : { numPartitions: partitions, replicationFactor }),
            configEntries: [{ name: 'retention.ms', value: '-1' }, ...configEntries],
          },
        ],
      })
    )
    const metadata = await this.invoke(() => admin.fetchTopicMetadata({ topics: [topic] }))
    const row = metadata.topics.find((entry) => entry.name === topic)
    if (!row?.topicId) throw new Error('Created topic has no canonical identity')
    this.topics.push(row)
    return topic
  }

  /**
   * Polls under the same absolute work deadline; every new observation is lazy and abort checked.
   * @param {() => Promise<boolean>} predicate independent readback
   * @param {number} [delayMs=250] 100 for moving-directory observations
   * @returns {Promise<void>} once the predicate holds
   * @throws {Error} original read failure, abort or deadline
   */
  poll(predicate, delayMs = 250) {
    return this.invoke(() =>
      pollUntil(this.signal, this.remaining() - this.reserve, predicate, { delayMs })
    )
  }

  /**
   * Builds the foreground command for one exec or control operation.
   * @param {string} operation exec or control
   * @param {string[]} argv literal operands: the service then its command, or the action then service
   * @returns {string[]} executable and literal operands, guarded or standalone
   */
  commandLine(operation, argv) {
    if (this.guard) return [process.execPath, this.guard, operation, this.manifest, ...argv]
    return ['docker', ...composeArgs(operation === 'exec' ? ['exec', '-T', ...argv] : argv)]
  }

  /**
   * Runs one foreground exec/control operation, with bounded output. Under a guard it uses only the
   * guard's exec/control branch; standalone it runs the matching `docker compose` subcommand.
   * @param {string} operation exec or control
   * @param {string[]} argv literal operands
   * @param {string|Buffer} [input] owned command input
   * @returns {Promise<string>} stdout only
   * @throws {Error} guard refusal, output overflow, abort or child failure
   */
  command(operation, argv, input) {
    return this.invoke(
      (scope) =>
        new Promise((resolve, reject) => {
          const seconds = Math.max(
            1,
            Math.floor((this.remaining() - (this.cleaning ? 0 : this.reserve)) / 1000)
          )
          const child = execFile(
            'timeout',
            ['--kill-after=5s', `${seconds}s`, ...this.commandLine(operation, argv)],
            { maxBuffer: 4 * 1048576, signal: scope },
            (error, stdout) => (error ? reject(error) : resolve(stdout))
          )
          child.stdin.end(input)
        })
    )
  }

  /**
   * Appends one raw receipt line under the guarded run directory, or `.tmp/e2e` standalone.
   * @param {string} file receipt file name
   * @param {object} entry JSON-serializable receipt
   * @returns {void}
   * @throws {Error} directory or write failure
   */
  receipt(file, entry) {
    const directory = process.env.LIVE_WORK || DEFAULT_RECEIPT_DIR
    fs.mkdirSync(directory, { recursive: true })
    fs.appendFileSync(path.join(directory, file), `${JSON.stringify(entry)}\n`)
  }

  /**
   * Executes literal argv inside a cluster service; a guarded run never invokes Docker directly.
   * @param {string} service manifest service
   * @param {string[]} argv literal command and operands
   * @param {string|Buffer} [input] optional stdin
   * @returns {Promise<string>} bounded output
   * @throws {Error} guard or command failure
   */
  host(service, argv, input) {
    return this.command('exec', [service, ...argv], input)
  }

  /**
   * Stops/starts a service after registering its unconditional restoration.
   * @param {'stop'|'start'} action supported lifecycle action
   * @param {string} service manifest service
   * @returns {Promise<void>}
   * @throws {Error} unsupported action or guard failure
   */
  async control(action, service) {
    if (!['stop', 'start'].includes(action)) throw new Error('Only stop/start controls are allowed')
    if (action === 'stop')
      this.effects.add('restore broker', () => this.command('control', ['start', service]))
    await this.command('control', [action, service])
  }

  /**
   * Reads uncommitted records directly from independent leader connections, starting at retained low.
   * Control markers and aborted-transaction facts remain separate from data records.
   * @param {string} topic exact created topic name
   * @returns {Promise<Map<number, object>>} partition-ordered raw observations and actual end offsets
   * @throws {Error} protocol error, stalled fetch or scenario deadline; sources always close
   */
  async physical(topic) {
    const readers = new CleanupStack()
    const open = async (host, port) => {
      const broker = new Broker({
        logger: this.logger,
        connectionPool: new Pool({
          host,
          port,
          logger: this.logger,
          socketFactory: (args) => this.socket(args),
          connectionTimeout: 3000,
          requestTimeout: Math.min(30000, this.remaining() - this.reserve),
        }),
      })
      readers.add('raw reader', () =>
        broker.disconnect({ drainTimeout: Math.min(5000, this.remaining()) })
      )
      await this.invoke(() =>
        broker.connect({ guard: () => (this.signal.aborted ? this.signal.reason : null) })
      )
      return broker
    }
    return readers.run(
      async () => {
        const [host, port] = this.baseOptions.brokers[0].split(':')
        const seed = await open(host, Number(port))
        const metadata = await this.invoke(() => seed.metadata([topic]))
        const result = new Map()
        for (const partition of metadata.topicMetadata[0].partitionMetadata) {
          const node = metadata.brokers.find((entry) => entry.nodeId === partition.leader)
          if (!node) throw new Error('Raw reader has no leader')
          const leader = await open(node.host, node.port)
          const boundary = async (timestamp) => {
            const response = await this.invoke(() =>
              leader.listOffsets({
                replicaId: -1,
                isolationLevel: 0,
                topics: [{ topic, partitions: [{ partition: partition.partitionId, timestamp }] }],
              })
            )
            const row = response.responses[0].partitions[0]
            if (row.errorCode !== 0) throw new Error('Raw offset read failed')
            return String(row.offset)
          }
          const low = await boundary(-2)
          const endOffset = await boundary(-1)
          let position = BigInt(low)
          const records = []
          const controls = []
          const abortedTransactions = []
          while (position < BigInt(endOffset)) {
            const answer = await this.invoke(() =>
              leader.fetch({
                isolationLevel: 0,
                maxWaitTime: 100,
                topics: [
                  {
                    topic,
                    partitions: [
                      {
                        partition: partition.partitionId,
                        fetchOffset: String(position),
                        maxBytes: 1048576,
                      },
                    ],
                  },
                ],
              })
            )
            const row = answer.responses[0].partitions[0]
            if (row.errorCode !== 0) throw new Error('Raw fetch failed')
            abortedTransactions.push(...(row.abortedTransactions || []))
            let next = position
            for (const record of row.messages) {
              const offset = BigInt(record.offset)
              if (offset >= position && offset < BigInt(endOffset)) {
                ;(record.batchContext.isControlBatch ? controls : records).push(record)
              }
              if (offset >= next) next = offset + 1n
            }
            if (next === position) throw new Error('Raw reader made no progress')
            position = next
          }
          result.set(partition.partitionId, {
            low,
            endOffset,
            records,
            controls,
            abortedTransactions,
          })
        }
        return result
      },
      { budgetMs: Math.min(10000, this.remaining()) }
    )
  }

  /**
   * Checks explicit item outcomes before success fields are used; does not reinterpret thrown errors.
   * @param {object|object[]} result item or list of items
   * @returns {void}
   * @throws {Error} failed item or missing errorCode
   */
  items(result) {
    for (const item of Array.isArray(result) ? result : [result]) {
      if (item.errorCode !== 0 || item.error)
        throw new Error('Admin item failed', { cause: item.error })
    }
  }

  /**
   * Writes only explicit nonsecret scalar facts and scalar arrays after caller assertions.
   * @param {string} caseName descriptive case identity
   * @param {object} facts allowlisted observation fields
   * @returns {void}
   * @throws {Error} unsupported/secret-shaped evidence field
   */
  record(caseName, facts) {
    const allowed =
      /^(count|.*Count|.*Ms|.*Bytes|.*Offset|.*Id|.*Ids|.*Digest|source|destination|futureObserved|currentChanged|sourceAbsent|throttleRestored|.*Version|.*Code|.*Mask|.*Masks|.*Broker|.*Brokers|.*Boundary|.*Boundaries|.*First|.*Last|.*High|.*Low|.*Present|.*Absent|.*Restored)$/
    const scalar = (value) => ['string', 'number', 'boolean'].includes(typeof value)
    for (const [key, value] of Object.entries(facts)) {
      if (
        (/secret|password|hmac|salt/i.test(key) && typeof value !== 'boolean') ||
        !allowed.test(key) ||
        !(scalar(value) || (Array.isArray(value) && value.every(scalar)))
      ) {
        throw new Error('Unsupported evidence field')
      }
    }
    const text = JSON.stringify({ caseName, facts })
    const markers = this.credentials
      .filter(Buffer.isBuffer)
      .flatMap((credential) =>
        ['utf8', 'hex', 'base64'].map((encoding) => credential.toString(encoding)).filter(Boolean)
      )
    if (markers.some((marker) => text.includes(marker))) throw new Error('Secret evidence value')
    writeEvidence(this.stem, {
      case: caseName,
      wireVersions: wireVersionsOf(this.requests),
      config: { clientId: this.baseOptions.clientId || 'e2e-live-admin' },
      ...facts,
    })
  }

  /**
   * Compares what the scenario holds now with its baseline: open sockets, abort listeners on the
   * owner signal and the event-loop resources of `HELD_RESOURCES`. A difference gets up to
   * `CLOSE_TURNS` loop turns to disappear, because a handle the scenario already destroyed is
   * still counted until the loop has run its close callback.
   * @param {number} signalListeners abort listeners the owner signal had at the baseline
   * @returns {Promise<string[]>} one `<name> (baseline <n>, after <m>)` entry per count that still
   *   differs; counts only, never values
   */
  async retainedResources(signalListeners) {
    let retained = []
    for (let turn = 0; turn < CLOSE_TURNS; turn++) {
      await new Promise((resolve) => setImmediate(resolve))
      const after = await this.resourceProbe.snapshot()
      retained = [
        ['open sockets', this.baseline.sockets.open, after.sockets.open],
        ['abort listeners', signalListeners, getEventListeners(this.signal, 'abort').length],
        ...HELD_RESOURCES.map((key) => [
          key,
          this.baseline.activeResources[key] || 0,
          after.activeResources[key] || 0,
        ]),
      ]
        .filter(([, before, now]) => before !== now)
        .map(([name, before, now]) => `${name} (baseline ${before}, after ${now})`)
      if (!retained.length) break
    }
    return retained
  }

  /**
   * Runs work and every restoration within the inclusive deadline, preserving its primary failure.
   * @template T
   * @param {() => Promise<T>} work complete scenario body
   * @returns {Promise<T>} original work result after verified cleanup
   * @throws {Error} primary failure, cleanup failure, taint or retained resources
   */
  async finish(work) {
    this.baseline = await this.resourceProbe.snapshot()
    const signalListeners = getEventListeners(this.signal, 'abort').length
    let primary
    let failed = false
    let result
    try {
      result = await work()
    } catch (error) {
      primary = error
      failed = true
    }
    this.cleaning = true
    const errors = await this.effects.closeAll({
      budgetMs: Math.min(this.reserve / 2, this.remaining()),
    })
    errors.push(...(await this.cleanup.closeAll({ budgetMs: this.remaining() })))
    for (const remove of this.removers.splice(0)) {
      try {
        remove()
      } catch (error) {
        errors.push(error)
        liveRun.taint(error)
      }
    }
    for (const credential of this.credentials) if (Buffer.isBuffer(credential)) credential.fill(0)
    const retained = await this.retainedResources(signalListeners)
    if (retained.length) {
      const error = new Error(`Scenario retained resources: ${retained.join('; ')}`)
      errors.push(error)
      liveRun.taint(error)
    }
    if (this.remaining() <= 0) {
      const error = new Error('Scenario cleanup exceeded inclusive deadline')
      errors.push(error)
      liveRun.taint(error)
    }
    if (failed) {
      if (primary !== null && typeof primary === 'object') primary.cleanupErrors = errors
      throw primary
    }
    if (errors.length) throw new AggregateError(errors, 'Scenario cleanup failed')
    liveRun.assertClean()
    return result
  }
}

module.exports = { AdminLiveProbe }
