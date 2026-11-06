const crypto = require('crypto')
const fs = require('fs')
const net = require('net')
const os = require('os')
const path = require('path')
const { EventEmitter } = require('events')
const { StringDecoder } = require('string_decoder')
const { spawn, spawnSync } = require('child_process')
const {
  Kafka,
  logLevel,
  AlterConfigOpTypes,
  ConfigResourceTypes,
  ConfigSource,
} = require('../../../../index')
const {
  createTopic,
  plainTextBrokers,
  readRequestHeader,
  secureRandom,
  writeEvidence,
  wireVersionsOf,
} = require('testHelpers')
const {
  ShareHandlerLog,
  assertShareCleanup,
  setShareGroupConfigs,
  shareRecordIdentity,
  shareTopicName,
} = require('../../../../testHelpers/shareFixtures')
const shareAcknowledge = require('../../../../testHelpers/wire/shareAcknowledgeV2')
const shareGroupHeartbeat = require('../../../../testHelpers/wire/shareGroupHeartbeatV1')
const { readLog } = require('../../../../testHelpers/rawLog')
const { createResourceProbe } = require('../../../../testHelpers/resourceProbe')
const {
  awaitTerminalSharePosition,
  assertQuietTerminalGroup,
  ShareAcknowledgementObserver,
} = require('../../../../testHelpers/shareLiveDiagnostics')
const {
  NEVER,
  bounded,
  caseTimeout,
  delay,
  liveCase,
  liveRun,
  pollTolerating,
  pollUntil,
} = require('../../../producer/__tests__/fixtures/bounded')
const { CleanupStack, describeError } = require('../../../producer/__tests__/fixtures/liveProbe')

const RUN_SH = path.join(
  __dirname,
  '..',
  '..',
  '..',
  '..',
  'scripts',
  'e2e',
  'javaConsumer',
  'run.sh'
)
const PROBE_MS = 60000

/**
 * Builds the Java member once, at collection time, so a missing JDK, cache or class is decided
 * before the suite exists: inside a test it could only be a failure or a silent pass. A supplied
 * `JAVA_E2E_WORK_DIR` is a jar cache that is used as it is and never removed.
 * @returns {{ ok: boolean, reason: string, workDir: string|null, owned: boolean }} `owned` is whether
 *   this spec created `workDir` and so removes it
 */
const preflight = () => {
  const supplied = process.env.JAVA_E2E_WORK_DIR
  const owned = !supplied
  const workDir = supplied || fs.mkdtempSync(path.join(os.tmpdir(), 'kafkajs-share-interop-'))
  const probe = spawnSync(RUN_SH, ['--role', 'share-member', '--probe'], {
    env: { ...process.env, JAVA_E2E_WORK_DIR: workDir },
    encoding: 'utf8',
    timeout: PROBE_MS,
  })
  const failed = (reason) => {
    if (owned) fs.rmSync(workDir, { recursive: true, force: true })
    return { ok: false, reason, workDir: null, owned }
  }
  if (probe.error?.code === 'ENOENT') return failed('run.sh not executable')
  if (probe.error) return failed('the Java probe did not finish in time')
  if (probe.status !== 0) {
    const stderr = probe.stderr || ''
    return failed(
      /NoClassDefFoundError/.test(stderr)
        ? 'a class was missing from the classpath'
        : /command not found|javac/.test(stderr)
          ? 'no JDK on PATH'
          : /Missing cached Java dependency/.test(stderr)
            ? 'the cached jars are missing'
            : 'the jars could not be downloaded'
    )
  }
  return { ok: true, reason: '', workDir, owned }
}

// Gated: it needs a share-enabled cluster with fast share-group failure detection
// (docker-compose.share.yml: session 6000 ms, heartbeat 1000 ms) and a JDK, and runs through the
// share lane or the guarded live entry with SHARE_INTEROP_E2E=1.
const GATED = process.env.SHARE_INTEROP_E2E === '1'
const enabled = GATED ? describe : describe.skip

const EVIDENCE = 'share-interop'
const PROFILE = 'tv2'
const TITLES = {
  join: 'shares six hundred records with a joining Java member',
  leave: 'releases unstarted records to Java on a clean leave',
  halt: 'recovers the complete uncommitted Java poll after a halt',
}
const BODY_MS = { join: 70000, leave: 70000, halt: 80000 }
const HOOK_MS = 10000
const CLEANUP_MS = 15000
const PARTITIONS = 4
const RECORDS = 600
const PER_PARTITION = RECORDS / PARTITIONS
const OLD_TIMESTAMP = '1700000000000'
const LOCK_MS = 30000
const DISCONNECT_MS = 1500
const RELEASE_AFTER_MS = 300
const HANDOVER_MS = 2000
const RECOVERY_SLACK_MS = 2000
const JAVA_RUN_MS = { join: 35000, leave: 35000, halt: 45000 }
const JAVA_WATCHDOG = ['--kill-after=5s', '50s']
const SHARE_APIS = new Set([76, 78, 79])
const KEEP_BYTES = new Set([76, 79])
const GROUP_CONFIG = {
  'share.auto.offset.reset': 'earliest',
  'share.record.lock.duration.ms': LOCK_MS,
}
const CLIENT_CONFIG = {
  acknowledgementMode: 'implicit',
  acquireMode: 'record_limit',
  maxRecords: 100,
  maxWaitTimeInMs: 250,
}

/**
 * Splits a byte stream into whole length-prefixed Kafka frames.
 */
class FrameSplitter {
  /** Starts with no partial frame. */
  constructor() {
    this.carry = Buffer.alloc(0)
  }

  /**
   * @param {Buffer} chunk the next bytes of the stream
   * @returns {Buffer[]} copies of the frames this chunk completed, each with its 4-byte length
   */
  push(chunk) {
    let bytes = this.carry.length === 0 ? chunk : Buffer.concat([this.carry, chunk])
    const frames = []
    while (bytes.length >= 4 && bytes.length >= 4 + bytes.readInt32BE(0)) {
      const end = 4 + bytes.readInt32BE(0)
      frames.push(Buffer.from(bytes.subarray(0, end)))
      bytes = bytes.subarray(end)
    }
    this.carry = Buffer.from(bytes)
    return frames
  }
}

/**
 * Wraps the plaintext socket factory of every JavaScript client of one case. It counts sockets and
 * unanswered requests from the bytes actually written and read, and keeps the share frames each
 * socket sent, with the bytes of the heartbeats and acknowledgements. Bytes, order, return values
 * and callbacks of every write are untouched.
 */
class ShareWireTap {
  /**
   * @param {Function} [connect] opens one socket; replaced by a fake in the offline checks
   */
  constructor(connect = ({ host, port, onConnect }) => net.connect({ host, port }, onConnect)) {
    this.connect = connect
    this.sockets = []
    this.frames = []
  }

  /**
   * @param {string} label the client the sockets belong to
   * @returns {Function} a Kafka `socketFactory`
   */
  factory(label) {
    return (options) => {
      const socket = this.connect(options)
      socket.setKeepAlive?.(true, 60000)
      return this.wrap(label, socket)
    }
  }

  /**
   * @param {string} label the owning client
   * @param {object} socket the real socket
   * @returns {object} the same socket, observed
   */
  wrap(label, socket) {
    const entry = {
      label,
      socket,
      writer: new FrameSplitter(),
      reader: new FrameSplitter(),
      outstanding: new Set(),
    }
    this.sockets.push(entry)
    const write = socket.write.bind(socket)
    socket.write = (data, ...rest) => {
      const bytes = Buffer.isBuffer(data)
        ? data
        : Buffer.from(data, typeof rest[0] === 'string' ? rest[0] : undefined)
      entry.writer.push(bytes).forEach((frame) => this.record(entry, frame))
      return write(data, ...rest)
    }
    socket.on('data', (chunk) => {
      entry.reader.push(chunk).forEach((frame) => entry.outstanding.delete(frame.readInt32BE(4)))
    })
    return socket
  }

  /**
   * Notes one written request as unanswered, and keeps its header facts when it is a share API.
   * @param {object} entry the observed socket
   * @param {Buffer} frame one whole request frame
   * @returns {void}
   */
  record(entry, frame) {
    const { apiKey, apiVersion, correlationId, byteLength } = readRequestHeader(frame)
    entry.outstanding.add(correlationId)
    if (!SHARE_APIS.has(apiKey)) return
    this.frames.push({
      label: entry.label,
      socket: this.sockets.indexOf(entry),
      at: performance.now(),
      apiKey,
      apiVersion,
      correlationId,
      headerBytes: byteLength,
      bytes: KEEP_BYTES.has(apiKey) ? frame : null,
    })
  }

  /** @returns {{ created: number, destroyed: number }} sockets opened and sockets since destroyed */
  socketCounts() {
    return {
      created: this.sockets.length,
      destroyed: this.sockets.filter(({ socket }) => socket.destroyed).length,
    }
  }

  /** @returns {number} requests written on live sockets that have no response yet */
  pending() {
    return this.sockets
      .filter(({ socket }) => !socket.destroyed)
      .reduce((total, { outstanding }) => total + outstanding.size, 0)
  }

  /**
   * Reads the closing requests a client wrote with the independent wire readers.
   * @param {number} from index of the first frame to consider
   * @param {string} label the client whose frames are read
   * @returns {{ final: object|undefined, leave: object|undefined }} the first final acknowledgement
   *   (share session epoch -1) and the first leave heartbeat (member epoch -1) it wrote since
   */
  closing(from, label) {
    const read = (reader, key, field) =>
      this.frames
        .slice(from)
        .filter((frame) => frame.label === label && frame.apiKey === key && frame.bytes)
        .map((frame) => ({
          at: frame.at,
          apiVersion: frame.apiVersion,
          [field]: reader.readRequest(frame.bytes.subarray(frame.headerBytes))[field],
        }))
        .find((fact) => fact[field] === -1)
    return {
      final: read(shareAcknowledge, 79, 'shareSessionEpoch'),
      leave: read(shareGroupHeartbeat, 76, 'memberEpoch'),
    }
  }
}

/**
 * @param {string[]} identities identities with repeats
 * @returns {Map<string, number>} how often each occurs
 */
const tally = (identities) =>
  identities.reduce((map, id) => map.set(id, (map.get(id) ?? 0) + 1), new Map())

/**
 * @param {Map<string, number>} map a tally
 * @returns {Array<[string, number]>} its entries in identity order
 */
const sortedEntries = (map) => [...map].sort(([a], [b]) => (a < b ? -1 : 1))

/**
 * @param {Map<string, number>} expected what must be seen, with multiplicity
 * @param {Map<string, number>} actual what was seen
 * @returns {{ missing: string[], extra: string[], count: string[] }} identities not seen,
 *   unexpected, or seen another number of times
 */
const multisetProblems = (expected, actual) => ({
  missing: [...expected.keys()].filter((id) => !actual.has(id)),
  extra: [...actual.keys()].filter((id) => !expected.has(id)),
  count: [...expected]
    .filter(([id, n]) => actual.has(id) && actual.get(id) !== n)
    .map(([id, n]) => `${id} expected ${n} saw ${actual.get(id)}`),
})

/**
 * @param {Array<{ partitions: number[], topic: string }>} assignment an assignment
 * @returns {string} a comparable form independent of order
 */
const normalizeAssignment = (assignment) =>
  JSON.stringify(
    assignment.map(({ topic, partitions }) => [topic, [...partitions].sort((a, b) => a - b)]).sort()
  )

/**
 * @param {Array<{ partitions: number[] }>} assignment an assignment of one topic
 * @returns {number[]} its partitions in order
 */
const partitionsOf = (assignment) =>
  assignment.flatMap(({ partitions }) => partitions).sort((a, b) => a - b)

/** @returns {{ promise: Promise<any>, resolve: (value?: any) => void }} a promise the test settles */
const deferred = () => {
  const gate = {}
  gate.promise = new Promise((resolve) => {
    gate.resolve = resolve
  })
  return gate
}

const DECIMAL = /^(0|[1-9][0-9]*)$/
const HEX = /^(?:[0-9a-f]{2})*$/

/**
 * Reads the output of one Java share member started with `--report-polls`. It keeps every complete
 * line as the member wrote it, in a case-owned file first, then checks each line's shape and the
 * order of the sequence, and maps every record line to its full byte identity through the poll that
 * carried it. A malformed, unexpected or out-of-order line is kept as the first problem and fails the
 * case when it is asked for, never ignored.
 */
class JavaJournal {
  /**
   * @param {string} file fresh, exclusive path that receives the raw bytes as they arrive
   * @throws {Error} on an existing file or filesystem failure
   */
  constructor(file) {
    this.fd = fs.openSync(file, 'wx', 0o600)
    this.decoder = new StringDecoder('utf8')
    this.fragment = ''
    this.lines = []
    this.polls = []
    this.entries = []
    this.committed = []
    this.errors = []
    this.done = false
    this.problem = null
  }

  /**
   * Appends the bytes to the file, then parses every line they complete.
   * @param {Buffer} chunk the next stdout bytes
   * @param {number} at monotonic time the bytes were received
   * @returns {void}
   */
  push(chunk, at) {
    fs.writeSync(this.fd, chunk)
    this.fragment += this.decoder.write(chunk)
    const parts = this.fragment.split('\n')
    this.fragment = parts.pop()
    for (const line of parts) this.accept(line, at)
  }

  /**
   * Closes the file once the stream has ended.
   * @returns {void}
   */
  finish() {
    if (this.fd === null) return
    fs.closeSync(this.fd)
    this.fd = null
    if (this.fragment !== '') this.fail('The Java output ended inside a line')
  }

  /**
   * @param {string} message the first thing wrong with the journal
   * @returns {void}
   */
  fail(message) {
    this.problem ??= message
  }

  /**
   * @returns {void}
   * @throws {Error} the first problem found in the output
   */
  assertValid() {
    if (this.problem) throw new Error(this.problem)
  }

  /**
   * @param {string} line one complete stdout line
   * @param {number} at monotonic time it was received
   * @returns {void}
   */
  accept(line, at) {
    this.lines.push(line)
    let event
    try {
      event = JSON.parse(line)
    } catch {
      this.fail(`The Java output has a line that is not JSON: ${line.slice(0, 80)}`)
      return
    }
    if (this.done) this.fail('The Java output continues after done')
    if (event?.event === 'poll') this.onPoll(event, at)
    else if (event?.event === 'record') this.onRecord(event, at)
    else if (event?.event === 'committed') this.onCommitted(event)
    else if (event?.event === 'done') this.done = true
    else if (event?.event === 'error') this.errors.push(event)
    else this.fail(`The Java output has an unknown event: ${line.slice(0, 80)}`)
  }

  /**
   * @param {object} event a `poll` line
   * @param {number} at monotonic time it was received
   * @returns {void}
   */
  onPoll(event, at) {
    const { pollId, lockTimeoutMs, records } = event
    const bytes = (hex) => hex === null || (typeof hex === 'string' && HEX.test(hex))
    const valid =
      pollId === String(this.polls.length + 1) &&
      (lockTimeoutMs === null || Number.isSafeInteger(lockTimeoutMs)) &&
      Array.isArray(records) &&
      records.length > 0 &&
      records.every(
        (r) =>
          typeof r.topic === 'string' &&
          Number.isInteger(r.partition) &&
          typeof r.offset === 'string' &&
          DECIMAL.test(r.offset) &&
          bytes(r.keyHex) &&
          bytes(r.valueHex) &&
          (r.deliveryCount === null || Number.isInteger(r.deliveryCount))
      )
    if (!valid) {
      this.fail(`The Java output has a malformed poll: ${JSON.stringify(event).slice(0, 120)}`)
      return
    }
    const buffer = (hex) => (hex === null ? null : Buffer.from(hex, 'hex'))
    this.polls.push({
      pollId,
      lockTimeoutMs,
      at,
      records: records.map((r) => ({
        ...r,
        identity: shareRecordIdentity({
          topic: r.topic,
          partition: r.partition,
          offset: r.offset,
          key: buffer(r.keyHex),
          value: buffer(r.valueHex),
        }),
      })),
    })
  }

  /**
   * @param {object} event a `record` line, which must belong to the latest poll
   * @param {number} at monotonic time it was received
   * @returns {void}
   */
  onRecord(event, at) {
    const poll = this.polls[this.polls.length - 1]
    const carried = poll?.records.find(
      ({ topic, partition, offset }) =>
        topic === event.topic && partition === event.partition && offset === event.offset
    )
    if (!carried || carried.deliveryCount !== event.deliveryCount) {
      this.fail(`The Java output has a record its poll did not carry: ${JSON.stringify(event)}`)
      return
    }
    this.entries.push({
      identity: carried.identity,
      deliveryCount: event.deliveryCount,
      pollId: poll.pollId,
      at,
    })
  }

  /**
   * @param {object} event a `committed` line, which must name the latest poll, once
   * @returns {void}
   */
  onCommitted({ pollId }) {
    const poll = this.polls[this.polls.length - 1]
    if (!poll || poll.pollId !== pollId || this.committed.includes(pollId)) {
      this.fail(`The Java output commits a poll it did not just report: ${pollId}`)
      return
    }
    this.committed.push(pollId)
  }

  /** @returns {string} the SHA-256 of the exact lines joined by newlines, with a trailing newline */
  sha256() {
    return crypto
      .createHash('sha256')
      .update(`${this.lines.join('\n')}\n`)
      .digest('hex')
  }

  /**
   * @param {string} identity a record identity
   * @returns {number|undefined} when the member first printed it as a record
   */
  firstEntryAt(identity) {
    return this.entries.find((entry) => entry.identity === identity)?.at
  }
}

/**
 * @param {object} options the Java member's arguments
 * @param {string} options.bootstrap bootstrap servers
 * @param {string} options.group the share group id
 * @param {string} options.topic the topic to read
 * @param {number} options.runForMs how long the member polls
 * @param {number} [options.haltAfter] halt after this many printed records, when positive
 * @returns {string[]} the argv of the watchdog that runs the launcher, with no shell involved
 */
const javaArgv = ({ bootstrap, group, topic, runForMs, haltAfter = 0 }) => [
  ...JAVA_WATCHDOG,
  RUN_SH,
  '--role',
  'share-member',
  '--report-polls',
  ...(haltAfter > 0 ? ['--halt-after', String(haltAfter)] : []),
  bootstrap,
  group,
  topic,
  String(runForMs),
]

/**
 * One Java share member run as a child process under a watchdog. Every observer is installed at
 * spawn, the stdout stream goes to a journal before it is parsed, and the exit is attributed to this
 * child alone.
 */
class JavaShareMember {
  /**
   * @param {object} options
   * @param {string} options.workDir the Java jar and class cache
   * @param {string} options.journalFile fresh path for the raw stdout
   * @param {object} options.argv see `javaArgv`
   */
  constructor({ workDir, journalFile, argv }) {
    this.workDir = workDir
    this.argv = argv
    this.journal = new JavaJournal(journalFile)
    this.stderr = ''
    this.exit = null
    this.spawnError = null
    this.child = null
    this.closed = new Promise((resolve) => {
      this.settle = resolve
    })
  }

  /** @returns {void} spawns the member and observes its stdout, stderr, error, exit and close */
  start() {
    this.startedAt = performance.now()
    const child = spawn('timeout', javaArgv(this.argv), {
      env: { ...process.env, JAVA_E2E_WORK_DIR: this.workDir },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    this.child = child
    child.stdout.on('data', (chunk) => this.journal.push(chunk, performance.now()))
    child.stderr.on('data', (chunk) => {
      this.stderr = `${this.stderr}${chunk}`.slice(-2000)
    })
    child.once('error', (error) => {
      this.spawnError = error
      this.settle()
    })
    child.once('exit', (code, signal) => {
      this.exit = { code, signal, at: performance.now() }
    })
    child.once('close', () => {
      this.journal.finish()
      this.settle()
    })
  }

  /**
   * Ends a member that is still running, and waits for its streams to drain. Only the retained
   * child is signalled.
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   */
  async stop(signal) {
    if (this.child && this.exit === null && !this.spawnError) this.child.kill('SIGTERM')
    await bounded(signal, 8000, () => this.closed, { label: 'The Java member closing' })
    this.journal.finish()
  }

  /**
   * @returns {void}
   * @throws {Error} when the process could not start or its output was malformed
   */
  assertClean() {
    if (this.spawnError) throw this.spawnError
    this.journal.assertValid()
  }

  /**
   * @returns {void}
   * @throws {Error} unless the member polled, closed and finished with exit code zero
   */
  assertNormalExit() {
    this.assertClean()
    if (this.exit?.code !== 0 || !this.journal.done || this.journal.errors.length) {
      throw new Error(
        `The Java member did not finish normally: ${JSON.stringify({
          exit: this.exit,
          done: this.journal.done,
          errors: this.journal.errors,
          stderr: this.stderr,
        })}`
      )
    }
  }
}

/**
 * One live run of an interop scenario: a JavaScript share member and a Java share member read a
 * four-partition topic of 600 fixed records in one group, and every record must be handled with its
 * exact byte identity, the log, the broker position and the handler entries checked independently.
 * The three scenarios are `join`, `leave` and `halt`; each runs in a fresh group and topic.
 */
class ShareInteropCase {
  /**
   * @param {'join'|'leave'|'halt'} kind the scenario
   * @param {{ workDir: string }} java the Java cache the preflight prepared
   */
  constructor(kind, java) {
    this.kind = kind
    this.title = TITLES[kind]
    this.java = java
    this.nonce = secureRandom(6)
    this.topic = shareTopicName(`interop-${kind}`)
    this.group = `e2e-p9e-interop-${kind}-${this.nonce}`
    this.jsClientId = `js-share-${this.nonce}`
    this.javaClientId = `java-share-${this.group}`
    this.dir = fs.mkdtempSync(path.join(os.tmpdir(), 'kafkajs-share-interop-case-'))
    this.stack = new CleanupStack()
    this.tap = new ShareWireTap()
    this.acknowledgements = new ShareAcknowledgementObserver()
    this.openHandlers = 0
    this.probe = createResourceProbe({
      emitters: { process },
      sockets: () => this.tap.socketCounts(),
      queueDepth: () => this.tap.pending(),
      ledgerSize: () => this.openHandlers,
      activeResources: true,
    })
    this.requests = []
    this.expected = []
    this.corpus = new Map()
    this.next = 0
    this.jsEntries = []
    this.jsFirst = new Map()
    this.members = null
    this.hold = null
    this.facts = {}
    this.baseline = null
    this.beforeAdmin = null
    this.js = {
      assignment: [],
      assignments: [],
      acks: [],
      fetches: [],
      crashes: [],
      memberId: null,
    }
  }

  /**
   * @param {string} label the client label, also the start of its clientId
   * @returns {Kafka} a client whose sockets go through the tap
   */
  client(label) {
    return new Kafka({
      clientId: `${label}-${this.nonce}`,
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      socketFactory: this.tap.factory(label),
    })
  }

  /**
   * Appends one evidence row.
   * @param {string} event the row kind
   * @param {object} [fields] the measured facts
   * @returns {void}
   */
  row(event, fields = {}) {
    writeEvidence(EVIDENCE, {
      event,
      case: this.title,
      profile: PROFILE,
      scenario: this.nonce,
      groups: { interop: this.group },
      budgets: { caseMs: BODY_MS[this.kind], cleanupMs: CLEANUP_MS },
      wireVersions: wireVersionsOf(this.requests),
      ...fields,
    })
  }

  /** @returns {object} Progress and public outcomes before cleanup can change them. */
  progress() {
    return {
      jsEntries: this.jsEntries.length,
      javaEntries: this.javaMember?.journal.entries.length,
      javaExit: this.javaMember?.exit,
      javaStderr: this.javaMember?.stderr,
      javaProblem: this.javaMember?.journal.problem,
      acknowledgements: this.js.acks.map((row) => ({ ...row, error: describeError(row.error) })),
      acknowledgementResponses: this.acknowledgements.evidence(this.group),
      crashes: this.js.crashes.map((error) => describeError(error)),
      facts: this.facts,
    }
  }

  /**
   * Runs the whole case: setup, the scenario's schedule, the oracles and strict cleanup.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} the first assertion or fixture failure, with cleanup failures attached
   */
  async run(scope) {
    this.acknowledgements.install()
    try {
      await this.stack.run(
        async () => {
          await this.setup(scope)
          await this[this.kind](scope)
          await this.observe(scope)
          await this.finish(scope)
        },
        { budgetMs: CLEANUP_MS }
      )
      await this.complete(scope)
    } catch (error) {
      this.row('case-failed', { error: describeError(error), ...this.progress() })
      throw error
    } finally {
      this.acknowledgements.restore()
    }
  }

  /**
   * Creates the topic and group config, measures the baseline, and starts the producer and the
   * JavaScript member, which holds all four partitions before anything else joins.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async setup(scope) {
    this.beforeAdmin = await this.probe.snapshot()
    this.admin = this.client('admin').admin()
    this.admin.on(this.admin.events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.stack.add('prove cleanup and disconnect admin', (signal) => this.cleanup(signal))
    this.stack.add('delete topic', (signal) => this.deleteTopic(signal))
    this.stack.add('delete group', (signal) => this.deleteGroup(signal))
    this.stack.add('reset group configs', (signal) => this.resetConfigs(signal))
    this.stack.add('remove case files', () => fs.rmSync(this.dir, { recursive: true, force: true }))
    await bounded(scope, 15000, () => this.admin.connect())
    await bounded(scope, 30000, () =>
      createTopic({
        topic: this.topic,
        partitions: PARTITIONS,
        config: [
          { name: 'retention.ms', value: '-1' },
          { name: 'cleanup.policy', value: 'delete' },
        ],
      })
    )
    const metadata = await bounded(scope, 10000, () =>
      this.admin.fetchTopicMetadata({ topics: [this.topic] })
    )
    this.topicId = metadata.topics.find(({ name }) => name === this.topic).topicId
    await bounded(scope, 15000, () => setShareGroupConfigs(this.admin, this.group, GROUP_CONFIG))
    this.baseline = await bounded(scope, 60000, () =>
      bounded(scope, 10000, () => this.probe.snapshot())
    )
    this.row('config-readback', { group: GROUP_CONFIG, client: CLIENT_CONFIG })
    this.row('resource-baseline', { snapshot: this.baseline })

    this.producer = this.client('producer').producer({ idempotent: true })
    this.producer.on(this.producer.events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.stack.add('disconnect producer', () => this.producer.disconnect())
    await bounded(scope, 15000, () => this.producer.connect())
    await this.startJs(scope)
    await pollUntil(scope, 20000, () => partitionsOf(this.js.assignment).length === PARTITIONS, {
      label: 'The JavaScript member holding every partition',
    })
    this.facts.jsBefore = partitionsOf(this.js.assignment)
  }

  /**
   * Starts the JavaScript member: every listener first, then connect, subscribe and run. Each
   * handler start goes to the handler journal before the handler can block.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async startJs(scope) {
    const consumer = this.client('js-share').shareConsumer({
      groupId: this.group,
      ...CLIENT_CONFIG,
    })
    this.consumer = consumer
    this.handlerLog = new ShareHandlerLog(path.join(this.dir, 'js-handler.jsonl'), this.jsClientId)
    const { events } = consumer
    const js = this.js
    const removers = [
      consumer.on(events.GROUP_JOIN, ({ payload }) => {
        js.memberId = payload.memberId
      }),
      consumer.on(events.ASSIGNMENT_CHANGED, ({ payload }) => {
        js.assignment = payload.assignment
        js.assignments.push({ at: performance.now(), assignment: payload.assignment })
      }),
      consumer.on(events.ACKNOWLEDGE, ({ payload }) => js.acks.push(payload)),
      consumer.on(events.FETCH, ({ payload }) =>
        js.fetches.push({ at: performance.now(), numberOfRecords: payload.numberOfRecords })
      ),
      consumer.on(events.CRASH, ({ payload }) => js.crashes.push(payload.error)),
      consumer.on(events.REQUEST, ({ payload }) => this.requests.push(payload)),
    ]
    this.stack.add('close handler journal', () => this.handlerLog.close())
    this.stack.add('remove JavaScript listeners', () => removers.forEach((remove) => remove()))
    this.stack.add('disconnect JavaScript member', () => consumer.disconnect({ timeout: 5000 }))
    this.stack.add('release held handler', () => this.hold?.release.resolve())
    await bounded(scope, 15000, () => consumer.connect())
    await bounded(scope, 15000, () => consumer.subscribe({ topics: [this.topic] }))
    await bounded(scope, 20000, () =>
      consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const invocation = this.handlerLog.start({ ...message, topic, partition })
          const identity = shareRecordIdentity({ ...message, topic, partition })
          const entry = {
            invocation,
            identity,
            deliveryCount: message.deliveryCount,
            at: performance.now(),
          }
          this.jsEntries.push(entry)
          if (!this.jsFirst.has(identity)) this.jsFirst.set(identity, entry.at)
          this.openHandlers++
          try {
            if (this.hold && !this.hold.taken) {
              this.hold.taken = true
              this.hold.entered.resolve(entry)
              await this.hold.release.promise
            }
          } finally {
            this.openHandlers--
            this.handlerLog.finish(invocation)
          }
        },
      })
    )
  }

  /**
   * Spawns the Java member. Its cleanup is registered first, so a failed spawn is still joined.
   * @param {{ haltAfter?: number }} [options] `haltAfter` is the number of printed records before
   *   the deliberate halt
   * @returns {JavaShareMember} the running member
   */
  startJava({ haltAfter = 0 } = {}) {
    const member = new JavaShareMember({
      workDir: this.java.workDir,
      journalFile: path.join(this.dir, 'java-stdout.jsonl'),
      argv: {
        bootstrap: plainTextBrokers().join(','),
        group: this.group,
        topic: this.topic,
        runForMs: JAVA_RUN_MS[this.kind],
        haltAfter,
      },
    })
    this.javaMember = member
    this.stack.add('stop Java member', (signal) => member.stop(signal))
    member.start()
    return member
  }

  /**
   * @param {number} partition the partition
   * @param {number} seq the record's position in the partition
   * @returns {{ partition: number, key: Buffer, value: Buffer, timestamp: string }} the record
   */
  message(partition, seq) {
    const index = this.next++
    return {
      partition,
      key: Buffer.from(`interop:${String(index).padStart(4, '0')}`),
      value: Buffer.from(`interop:value:${partition}:${seq}`),
      timestamp: OLD_TIMESTAMP,
    }
  }

  /**
   * Appends `count` fixed records to one partition in one acknowledged send, and keeps their
   * identities with the offsets the broker acknowledged.
   * @param {AbortSignal} scope the case scope
   * @param {number} partition the partition
   * @param {number} count how many records
   * @returns {Promise<void>}
   * @throws {Error} when the broker does not acknowledge the send
   */
  async produceTo(scope, partition, count) {
    const have = this.corpus.get(partition) ?? []
    this.corpus.set(partition, have)
    const messages = Array.from({ length: count }, (_, n) =>
      this.message(partition, have.length + n)
    )
    const [sent] = await bounded(scope, 15000, () =>
      this.producer.send({ topic: this.topic, acks: -1, messages })
    )
    expect(sent.errorCode).toBe(0)
    expect(sent.partition).toBe(partition)
    messages.forEach((message, position) => {
      const offset = String(BigInt(sent.baseOffset) + BigInt(position))
      const identity = shareRecordIdentity({ topic: this.topic, partition, offset, ...message })
      have.push({ offset, identity })
      this.expected.push(identity)
    })
  }

  /**
   * Appends the records that bring every partition to its fixed share of the corpus.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async produceRest(scope) {
    for (let partition = 0; partition < PARTITIONS; partition++) {
      const need = PER_PARTITION - (this.corpus.get(partition)?.length ?? 0)
      if (need > 0) await this.produceTo(scope, partition, need)
    }
    expect(this.expected).toHaveLength(RECORDS)
  }

  /**
   * @param {AbortSignal} scope the caller's scope
   * @returns {Promise<object[]>} the group's members as the admin API describes them
   */
  async describeMembers(scope) {
    const { groups } = await bounded(scope, 8000, () =>
      this.admin.describeShareGroups([this.group], { apiTimeout: 6000 })
    )
    return groups[0].members
  }

  /**
   * Waits until the admin API lists exactly the JavaScript and the Java member, the JavaScript
   * member's admin and event assignments agree, and the two own two partitions each without overlap.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<{ js: number[], java: number[] }>} the partitions each member owns
   */
  async awaitMembers(scope) {
    await pollTolerating(
      scope,
      25000,
      async (poll) => {
        const members = await this.describeMembers(poll)
        const js = members.find(({ clientId }) => clientId === this.jsClientId)
        const java = members.find(({ clientId }) => clientId === this.javaClientId)
        if (members.length !== 2 || !js || !java) return false
        if (
          js.memberId !== this.js.memberId ||
          normalizeAssignment(js.assignment) !== normalizeAssignment(this.js.assignment)
        ) {
          return false
        }
        const [mine, theirs] = [partitionsOf(js.assignment), partitionsOf(java.assignment)]
        if (JSON.stringify([...mine, ...theirs].sort()) !== '[0,1,2,3]' || mine.length !== 2) {
          return false
        }
        this.members = members.map(({ memberId, clientId, assignment }) => ({
          memberId,
          clientId,
          partitions: partitionsOf(assignment),
          assignment,
        }))
        return true
      },
      { label: 'The two-member assignment' }
    )
    const [js, java] = [this.jsClientId, this.javaClientId].map(
      (id) => this.members.find(({ clientId }) => clientId === id).partitions
    )
    this.facts.jsAfter = js
    this.facts.assignmentChanged = JSON.stringify(js) !== JSON.stringify(this.facts.jsBefore)
    this.row('assignment', { members: this.members, jsBefore: this.facts.jsBefore })
    return { js, java }
  }

  /**
   * @returns {bigint} offsets the JavaScript member's accepts confirmed
   * @throws {Error} on a terminal event that failed
   */
  acceptedOffsets() {
    let total = 0n
    for (const { type, error, firstOffset, lastOffset } of this.js.acks) {
      if (type === 'renew') continue
      if (error !== null)
        throw new Error(`The JavaScript member reported ${type}: ${error.message}`)
      if (type === 'accept') total += BigInt(lastOffset) - BigInt(firstOffset) + 1n
    }
    return total
  }

  /**
   * Requires one successful JS Accept or one committed Java poll receipt per corpus identity.
   * Releases and uncommitted polls do not settle identities; unknown outcomes never qualify.
   * @returns {boolean} Complete, disjoint successful coverage of the current append prefix.
   */
  successfulAccepts() {
    const journal = this.javaMember?.journal
    const committed = new Set(journal?.committed ?? [])
    if (this.kind === 'halt' && committed.size) return false
    const java = tally(
      (journal?.polls ?? [])
        .filter(({ pollId }) => committed.has(pollId))
        .flatMap(({ records }) => records.map(({ identity }) => identity))
    )
    if ([...java.keys()].some((identity) => !this.expected.includes(identity))) return false
    if (journal && this.kind !== 'halt') {
      if (journal.polls.some(({ pollId }) => !committed.has(pollId))) return false
    }
    const jsHandled = tally(this.jsEntries.map(({ identity }) => identity))
    const javaHandled = tally((journal?.entries ?? []).map(({ identity }) => identity))
    for (const [partition, records] of this.corpus) {
      const types = this.acknowledgements.types(this.group, this.topicId, partition)
      if ([...types.keys()].some((offset) => !records.some((r) => r.offset === offset)))
        return false
      for (const { offset, identity } of records) {
        const decisions = types.get(offset) ?? []
        if (decisions.some((type) => type !== 1 && type !== 2)) return false
        const jsAccepts = decisions.filter((type) => type === 1).length
        const javaAccepts = java.get(identity) ?? 0
        if (jsAccepts + javaAccepts !== 1) return false
        if (jsAccepts && jsHandled.get(identity) !== 1) return false
        if (javaAccepts && javaHandled.get(identity) !== 1) return false
      }
    }
    return this.expected.length > 0
  }

  /**
   * Requires agreeing terminal listings, known zero lag and independent fixed log ends.
   * @param {AbortSignal} scope Case scope.
   * @param {Map<number, string>} ends Independently read log ends.
   * @returns {Promise<object[]>} Actual persisted rows.
   */
  async awaitPositions(scope, ends) {
    const position = await awaitTerminalSharePosition(
      this.admin,
      {
        groupId: this.group,
        topic: this.topic,
        partitions: [...ends].map(([partition, end]) => ({ partition, floor: '0', end })),
      },
      {
        signal: scope,
        timeoutMs: 20000,
        record: (fields) => this.row('broker-position', { ...fields, ...this.progress() }),
      }
    )
    return position.rows
  }

  /**
   * Waits for the Java member to end, and checks its output.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async awaitJavaExit(scope) {
    await bounded(scope, JAVA_RUN_MS[this.kind] + 20000, () => this.javaMember.closed, {
      label: 'The Java member ending',
    })
  }

  /**
   * @returns {number} handler entries of both members, the Java member's by its record lines
   */
  handlerTotal() {
    return this.jsEntries.length + (this.javaMember?.journal.entries.length ?? 0)
  }

  /**
   * One JavaScript member alone reads 120 records, a Java member joins and takes two of the four
   * partitions, and both read the remaining 480. No record is handled twice.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async join(scope) {
    for (let partition = 0; partition < PARTITIONS; partition++) {
      await this.produceTo(scope, partition, PER_PARTITION / 5)
    }
    await pollUntil(
      scope,
      20000,
      () =>
        this.jsEntries.length >= 120 && this.acceptedOffsets() >= 120n && this.successfulAccepts(),
      { label: 'The JavaScript member accepting its first records' }
    )
    const prefix = await this.observeLog(scope)
    this.facts.firstLog = { expected: [...this.expected], actual: [...this.rawIdentities] }
    this.facts.firstPositions = await this.awaitPositions(
      scope,
      new Map([...prefix].map(([partition, { endOffset }]) => [partition, endOffset]))
    )
    this.startJava()
    await this.awaitMembers(scope)
    expect(this.facts.assignmentChanged).toBe(true)
    await this.produceRest(scope)
    await this.awaitJavaExit(scope)
    this.javaMember.assertNormalExit()
    await pollUntil(scope, 15000, () => this.handlerTotal() >= RECORDS, {
      label: 'Every record handled',
    })
    expect(this.jsEntries.length).toBeGreaterThan(0)
    expect(this.javaMember.journal.entries.length).toBeGreaterThan(0)
  }

  /**
   * The JavaScript member's first handler is held while it disconnects with a short timeout; the
   * records it acquired but had not started must reach the Java member within two seconds of the
   * call, and no unstarted record may enter the JavaScript handler.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async leave(scope) {
    this.startJava()
    const { js } = await this.awaitMembers(scope)
    const [target] = js
    this.hold = { taken: false, entered: deferred(), release: deferred() }
    await this.produceTo(scope, target, 100)
    const held = await bounded(scope, 15000, () => this.hold.entered.promise, {
      label: 'The first handler entering',
    })
    await pollUntil(scope, 10000, () => this.js.fetches.some((f) => f.numberOfRecords > 0), {
      label: 'The acquisition',
    })
    const acquired = this.js.fetches.find((f) => f.numberOfRecords > 0).numberOfRecords
    expect(acquired).toBeGreaterThanOrEqual(2)
    const batch = this.corpus.get(target)
    const unstarted = batch.slice(1, acquired).map(({ identity }) => identity)
    expect(held.identity).toBe(batch[0].identity)
    expect(this.jsEntries.map(({ identity }) => identity)).toEqual([batch[0].identity])

    const mark = this.tap.frames.length
    const disconnectAt = performance.now()
    let settledAt = null
    const closing = this.consumer.disconnect({ timeout: DISCONNECT_MS }).then(
      () => {
        settledAt = performance.now()
      },
      (error) => {
        settledAt = performance.now()
        throw error
      }
    )
    closing.catch(() => {})
    await delay(scope, RELEASE_AFTER_MS)
    expect(settledAt).toBeNull()
    expect(this.jsEntries).toHaveLength(1)
    const releasedAt = performance.now()
    this.hold.release.resolve()
    await bounded(scope, DISCONNECT_MS + 2000, () => closing, { label: 'The disconnect' })

    await pollUntil(
      scope,
      HANDOVER_MS + 3000,
      () =>
        unstarted.every((identity) => this.javaMember.journal.firstEntryAt(identity) !== undefined),
      { delayMs: 25, label: 'The unstarted records reaching the Java member' }
    )
    const handoverAt = Math.max(
      ...unstarted.map((identity) => this.javaMember.journal.firstEntryAt(identity))
    )
    const closingFrames = this.tap.closing(mark, 'js-share')
    expect(closingFrames.final).toBeDefined()
    expect(closingFrames.leave).toBeDefined()
    expect(handoverAt - disconnectAt).toBeLessThanOrEqual(HANDOVER_MS)
    expect(this.jsEntries.map(({ identity }) => identity)).toEqual([batch[0].identity])
    const accepts = this.js.acks.filter(
      ({ type, partition }) => type === 'accept' && partition === target
    )
    const covers = (offset) =>
      accepts.some(
        ({ firstOffset, lastOffset }) =>
          BigInt(firstOffset) <= BigInt(offset) && BigInt(offset) <= BigInt(lastOffset)
      )
    expect(covers(batch[0].offset)).toBe(true)
    expect(batch.slice(1, acquired).some(({ offset }) => covers(offset))).toBe(false)
    Object.assign(this.facts, {
      target,
      acquired,
      unstarted,
      disconnectAt,
      releasedAt,
      settledAt,
      handoverAt,
      closing: closingFrames,
      released: this.js.acks.filter(({ type }) => type === 'release'),
    })

    await this.produceRest(scope)
    await this.awaitJavaExit(scope)
    this.javaMember.assertNormalExit()
    await pollUntil(scope, 15000, () => this.handlerTotal() >= RECORDS, {
      label: 'Every record handled',
    })
  }

  /**
   * The Java member halts with a whole poll acquired, printing only its first record and
   * committing nothing; the surviving JavaScript member must handle every record of that poll within
   * the original lock timeout and two seconds of the halt.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async halt(scope) {
    this.startJava({ haltAfter: 1 })
    const { java } = await this.awaitMembers(scope)
    const [target] = java
    await this.produceTo(scope, target, 100)
    await bounded(scope, 25000, () => this.javaMember.closed, { label: 'The Java member halting' })
    const member = this.javaMember
    member.assertClean()
    expect(member.journal.polls).toHaveLength(1)
    const [poll] = member.journal.polls
    const haltedAt = member.exit.at
    expect(member.exit.code).toBe(137)
    expect(haltedAt - member.startedAt).toBeLessThan(49000)
    expect(member.journal.done).toBe(false)
    expect(member.journal.errors).toEqual([])
    expect(member.journal.committed).toEqual([])
    expect(poll).toBeDefined()
    expect(poll.records.length).toBeGreaterThanOrEqual(2)
    expect(Number.isSafeInteger(poll.lockTimeoutMs)).toBe(true)
    expect(member.journal.entries.length).toBeGreaterThanOrEqual(1)
    const held = poll.records.map(({ identity }) => identity)
    expect(new Set(held).size).toBe(held.length)
    expect(held.every((identity) => this.expected.includes(identity))).toBe(true)
    expect(member.journal.entries).toHaveLength(1)
    expect(member.journal.entries[0].identity).toBe(held[0])

    const deadline = haltedAt + poll.lockTimeoutMs + RECOVERY_SLACK_MS
    await pollUntil(
      scope,
      Math.max(1000, deadline - performance.now()),
      () => this.haltRecovered(poll, haltedAt),
      { delayMs: 100, label: 'The halted poll recovering in the surviving member' }
    )
    const recoveredAt = Math.max(...held.map((identity) => this.jsFirst.get(identity)))
    expect(recoveredAt - haltedAt).toBeLessThanOrEqual(poll.lockTimeoutMs + RECOVERY_SLACK_MS)
    Object.assign(this.facts, {
      target,
      haltedAt,
      recoveredAt,
      pollReceivedAt: poll.at,
      heldPoll: held,
      recovered: held.map((identity) => ({ identity, at: this.jsFirst.get(identity) })),
    })

    await this.produceRest(scope)
    const printed = member.journal.entries.length
    await pollUntil(scope, 30000, () => this.handlerTotal() >= RECORDS + printed, {
      label: 'Every record handled',
    })
  }

  /**
   * Checks recovery of the complete original poll, including records Java never printed.
   * @param {object} poll Original Java poll and actual lock timeout.
   * @param {number} haltedAt Observed halt time on the case clock.
   * @returns {boolean} Every acquired identity reached JS within the recovery bound.
   */
  haltRecovered(poll, haltedAt) {
    const deadline = haltedAt + poll.lockTimeoutMs + RECOVERY_SLACK_MS
    return poll.records.every(({ identity }) => {
      const at = this.jsFirst.get(identity)
      return at >= haltedAt && at <= deadline
    })
  }

  /**
   * Checks the durable data identities, control projection and ends of the current append prefix.
   * @param {AbortSignal} scope Case scope.
   * @returns {Promise<Map>} Independently read partitions, records and ends.
   */
  async observeLog(scope) {
    const log = await bounded(scope, 20000, () =>
      readLog({ brokers: plainTextBrokers(), topic: this.topic })
    )
    expect([...log.keys()].sort()).toEqual([...this.corpus.keys()].sort())
    const raw = []
    for (const [partition, { endOffset, records }] of log) {
      expect(endOffset).toBe(String(this.corpus.get(partition).length))
      expect(records.filter(({ batchContext }) => batchContext.isControlBatch)).toEqual([])
      expect(records.map(({ offset }) => String(offset))).toEqual(
        Array.from({ length: this.corpus.get(partition).length }, (_, n) => String(n))
      )
      for (const record of records) {
        raw.push(
          shareRecordIdentity({
            topic: this.topic,
            partition,
            offset: String(record.offset),
            key: record.key,
            value: record.value,
          })
        )
      }
    }
    this.rawIdentities = raw
    expect(multisetProblems(tally(this.expected), tally(raw))).toEqual({
      missing: [],
      extra: [],
      count: [],
    })

    return log
  }

  /**
   * Checks the three independent oracles: raw log, broker offsets and handler entries.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async observe(scope) {
    const log = await this.observeLog(scope)
    const javaEntries = this.javaMember.journal.entries
    const journal = ShareHandlerLog.read(path.join(this.dir, 'js-handler.jsonl'))
    const started = journal.rows.filter(({ event }) => event === 'start').map((r) => r.identity)
    expect(started).toEqual(this.jsEntries.map(({ identity }) => identity))
    this.handled = tally([...started, ...javaEntries.map(({ identity }) => identity)])
    this.expectedHandled = tally(this.expected)
    if (this.kind === 'halt') {
      for (const { identity } of javaEntries) {
        this.expectedHandled.set(identity, this.expectedHandled.get(identity) + 1)
      }
    }
    expect(multisetProblems(this.expectedHandled, this.handled)).toEqual({
      missing: [],
      extra: [],
      count: [],
    })
    if (this.kind !== 'halt') {
      const javaSet = new Set(javaEntries.map(({ identity }) => identity))
      expect(this.jsEntries.filter(({ identity }) => javaSet.has(identity))).toEqual([])
    }
    this.terminalPartitions = [...log].map(([partition, { endOffset }]) => ({
      partition,
      floor: '0',
      end: endOffset,
    }))
    expect(this.terminalPartitions).toHaveLength(PARTITIONS)
    expect(this.terminalPartitions.every(({ end }) => end === String(PER_PARTITION))).toBe(true)
    this.broker = await bounded(scope, 20000, async (inner) => {
      await pollUntil(
        inner,
        Infinity,
        () => {
          this.acceptedOffsets()
          return this.successfulAccepts()
        },
        { label: 'Every identity successfully accepted' }
      )
      return this.awaitPositions(
        inner,
        new Map([...log].map(([partition, { endOffset }]) => [partition, endOffset]))
      )
    })
  }

  /**
   * Disconnects the JavaScript member, then lets a fresh member of the same group listen for five
   * seconds. Both Java and JavaScript members must be gone first.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async finish(scope) {
    await bounded(scope, 10000, () => this.consumer.disconnect({ timeout: 5000 }))
    await bounded(scope, 30000, () => this.javaMember.closed)
    this.javaMember.assertClean()
    this.quiet = await assertQuietTerminalGroup({
      kafka: this.client('verifier'),
      admin: this.admin,
      groupId: this.group,
      topic: this.topic,
      partitions: this.terminalPartitions,
      signal: scope,
      timeoutMs: 30000,
      record: (fields) => this.row('quiet-verifier', fields),
    })
    this.broker = this.quiet.position.rows
    expect(this.quiet.delivered).toEqual([])
    this.acceptedOffsets()
    expect(this.successfulAccepts()).toBe(true)
    expect(
      multisetProblems(
        this.expectedHandled,
        tally([
          ...this.jsEntries.map(({ identity }) => identity),
          ...this.javaMember.journal.entries.map(({ identity }) => identity),
        ])
      )
    ).toEqual({ missing: [], extra: [], count: [] })
    expect(this.js.crashes.map((error) => error.message)).toEqual([])
    this.handlerLog.close()
  }

  /**
   * Proves owned objects are gone and resources are back at baseline, then disconnects the admin.
   * It runs last in the cleanup order.
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   */
  async cleanup(signal) {
    try {
      if (this.baseline) {
        await pollUntil(
          signal,
          3000,
          () =>
            this.tap.socketCounts().created - this.tap.socketCounts().destroyed <=
            this.baseline.sockets.open,
          { delayMs: 50, label: 'The sockets closing' }
        )
        this.after = await assertShareCleanup(
          this.admin,
          [this.group],
          [this.topic],
          this.baseline,
          this.probe,
          { signal }
        )
      }
    } finally {
      await this.admin.disconnect()
    }
  }

  /**
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   * @throws {Error} when the topic is still listed after deletion
   */
  async deleteTopic(signal) {
    await bounded(signal, 10000, () =>
      this.admin.deleteTopics({ topics: [this.topic], timeout: 8000 })
    ).catch((error) => {
      if (error.type !== 'UNKNOWN_TOPIC_OR_PARTITION') throw error
    })
    await pollUntil(
      signal,
      10000,
      async () => !(await this.admin.listTopics()).includes(this.topic),
      { label: 'The topic disappearing' }
    )
  }

  /**
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   * @throws {Error} when the group is still listed after deletion
   */
  async deleteGroup(signal) {
    await pollUntil(
      signal,
      10000,
      async () => {
        try {
          await this.admin.deleteGroups([this.group])
          return true
        } catch (error) {
          const code = error.groups?.[0]?.error?.code
          if (code === 69) return true
          if (code === 68) return false
          throw error
        }
      },
      { label: 'The group deletion' }
    )
    await pollUntil(
      signal,
      10000,
      async () =>
        !(await this.admin.listGroups()).groups.some(({ groupId }) => groupId === this.group),
      { label: 'The group disappearing' }
    )
  }

  /**
   * Removes the owned group config overrides and proves none remain.
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   * @throws {Error} when the broker refuses or an override remains
   */
  async resetConfigs(signal) {
    const names = Object.keys(GROUP_CONFIG)
    const { resources } = await bounded(signal, 8000, () =>
      this.admin.incrementalAlterConfigs({
        resources: [
          {
            type: ConfigResourceTypes.GROUP,
            name: this.group,
            configEntries: names.map((name) => ({ name, operation: AlterConfigOpTypes.DELETE })),
          },
        ],
      })
    )
    if (resources[0].errorCode !== 0) {
      throw new Error(`Config reset refused: ${resources[0].errorCode}`)
    }
    const read = await bounded(signal, 8000, () =>
      this.admin.describeConfigs({
        resources: [{ type: ConfigResourceTypes.GROUP, name: this.group, configNames: names }],
      })
    )
    const left = read.resources[0].configEntries.filter(
      ({ configSource }) => configSource === ConfigSource.GROUP_CONFIG
    )
    if (left.length > 0) {
      throw new Error(`Group config overrides remain: ${left.map((e) => e.configName)}`)
    }
  }

  /**
   * @param {{ sockets: { open: number }, activeResources: Record<string, number>, listeners: Record<string, number>, queueDepth: number, ledgerSize: number }} snapshot a measurement
   * @returns {object} the evidence form of the measurement
   */
  resourceSummary(snapshot) {
    return {
      openSockets: snapshot.sockets.open,
      timers: snapshot.activeResources.Timeout ?? 0,
      listeners: Object.values(snapshot.listeners).reduce((sum, n) => sum + n, 0),
      queueDepth: snapshot.queueDepth,
      ledgerSize: snapshot.ledgerSize,
    }
  }

  /**
   * @returns {object} the interop facts: members, assignments, the original Java journal and the
   *   JavaScript handler entries, so a reader can recompute every identity and the multiset
   */
  interop() {
    const { journal, exit } = this.javaMember
    return {
      groupId: this.group,
      javaClientId: this.javaClientId,
      jsClientId: this.jsClientId,
      members: this.members,
      assignmentChanged: this.facts.assignmentChanged,
      jsBefore: this.facts.jsBefore,
      jsAfter: this.facts.jsAfter,
      jsEntries: this.jsEntries.map(({ identity, deliveryCount, at }) => ({
        identity,
        deliveryCount,
        at,
      })),
      polls: journal.polls.map(({ pollId, lockTimeoutMs, at, records }) => ({
        pollId,
        lockTimeoutMs,
        at,
        identities: records.map(({ identity }) => identity),
      })),
      committedPolls: journal.committed,
      journal: { lines: journal.lines, sha256: journal.sha256() },
      exitCode: exit?.code ?? null,
      exitSignal: exit?.signal ?? null,
      done: journal.done,
      ...this.facts,
    }
  }

  /**
   * Checks the admin's own sockets are closed, then records the single completion row.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async complete(scope) {
    await pollUntil(
      scope,
      5000,
      async () => (await this.probe.snapshot()).sockets.open === this.beforeAdmin.sockets.open,
      { delayMs: 50, label: 'The admin sockets closing' }
    )
    const final = await bounded(scope, 10000, () => this.probe.snapshot())
    this.row('cleanup', { snapshot: final })
    this.row('case-complete', {
      status: 'passed',
      tainted: false,
      oracles: {
        log: { expected: [...this.expected].sort(), actual: [...this.rawIdentities].sort() },
        handler: {
          expected: sortedEntries(this.expectedHandled),
          actual: sortedEntries(this.handled),
        },
        broker: this.broker,
        acknowledgements: this.acknowledgements.evidence(this.group),
        quiet: this.quiet,
      },
      resources: {
        before: this.resourceSummary(this.baseline),
        after: this.resourceSummary(this.after),
      },
      cleanup: {
        groupsAbsent: true,
        topicsAbsent: true,
        completedAt: Number(process.hrtime.bigint() / 1000000n),
      },
      interop: this.interop(),
      shareFrames: this.tap.frames.length,
      wireVersions: wireVersionsOf(this.requests),
      config: { client: CLIENT_CONFIG, group: GROUP_CONFIG },
    })
  }
}

describe('share interop wire tap', () => {
  const frame = (apiKey, correlationId) => {
    const header = Buffer.alloc(11)
    header.writeInt16BE(apiKey, 0)
    header.writeInt16BE(2, 2)
    header.writeInt32BE(correlationId, 4)
    header.writeInt16BE(1, 8)
    header.write('t', 10)
    const body = Buffer.concat([header, Buffer.from([0])])
    const bytes = Buffer.alloc(4 + body.length)
    bytes.writeInt32BE(body.length, 0)
    body.copy(bytes, 4)
    return bytes
  }

  it('splits fragmented writes into whole frames and counts a request until its answer', () => {
    const splitter = new FrameSplitter()
    const first = frame(78, 7)
    const second = frame(79, 8)
    const stream = Buffer.concat([first, second])
    const seen = [
      stream.subarray(0, 3),
      stream.subarray(3, first.length + 2),
      stream.subarray(first.length + 2),
    ].flatMap((chunk) => splitter.push(chunk))
    expect(seen).toEqual([first, second])

    const written = []
    const socket = Object.assign(new EventEmitter(), {
      destroyed: false,
      write: (data) => {
        written.push(Buffer.from(data))
        return true
      },
    })
    const tap = new ShareWireTap(() => socket)
    const wrapped = tap.factory('A')({})
    wrapped.write(stream.subarray(0, 5))
    wrapped.write(stream.subarray(5))
    expect(Buffer.concat(written)).toEqual(stream)
    expect(tap.pending()).toBe(2)
    expect(tap.frames.map(({ apiKey, bytes }) => [apiKey, bytes !== null])).toEqual([
      [78, false],
      [79, true],
    ])
    const answer = Buffer.alloc(8)
    answer.writeInt32BE(4, 0)
    answer.writeInt32BE(7, 4)
    socket.emit('data', answer)
    expect(tap.pending()).toBe(1)
    socket.destroyed = true
    expect(tap.pending()).toBe(0)
    expect(tap.socketCounts()).toEqual({ created: 1, destroyed: 1 })
  })
})

describe('share interop Java journal', () => {
  const key = Buffer.from('k1').toString('hex')
  const value = Buffer.from('v1').toString('hex')
  const poll = (pollId, records) =>
    JSON.stringify({ event: 'poll', pollId, lockTimeoutMs: 30000, records })
  const carried = (offset, extra = {}) => ({
    topic: 't',
    partition: 1,
    offset,
    keyHex: key,
    valueHex: value,
    deliveryCount: 1,
    ...extra,
  })
  const record = (offset, deliveryCount = 1) =>
    JSON.stringify({ event: 'record', topic: 't', partition: 1, offset, deliveryCount })
  const open = () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'kafkajs-share-journal-'))
    return { dir, journal: new JavaJournal(path.join(dir, 'java.jsonl')) }
  }
  const identity = (offset, deliveryCount) =>
    shareRecordIdentity({
      topic: 't',
      partition: 1,
      offset,
      key: Buffer.from('k1'),
      value: Buffer.from('v1'),
      deliveryCount,
    })

  it('maps a record line through its poll and joins lines split across chunks', () => {
    const { dir, journal } = open()
    const text = [
      poll('1', [carried('9007199254740993'), carried('9007199254740994')]),
      record('9007199254740993'),
      JSON.stringify({ event: 'committed', pollId: '1' }),
      JSON.stringify({ event: 'done' }),
    ].join('\n')
    const bytes = Buffer.from(`${text}\n`)
    journal.push(bytes.subarray(0, 40), 1)
    journal.push(bytes.subarray(40), 2)
    journal.finish()
    expect(() => journal.assertValid()).not.toThrow()
    expect(journal.polls[0].records.map(({ identity: id }) => id)).toEqual([
      identity('9007199254740993'),
      identity('9007199254740994'),
    ])
    expect(journal.entries).toEqual([
      { identity: identity('9007199254740993'), deliveryCount: 1, pollId: '1', at: 2 },
    ])
    expect(journal.committed).toEqual(['1'])
    expect(journal.done).toBe(true)
    expect(journal.sha256()).toBe(crypto.createHash('sha256').update(`${text}\n`).digest('hex'))
    expect(fs.readFileSync(path.join(dir, 'java.jsonl'))).toEqual(bytes)
    fs.rmSync(dir, { recursive: true, force: true })
  })

  it('keeps the first problem: a record its poll did not carry, a wrong commit, a truncated line', () => {
    const stray = open()
    stray.journal.push(Buffer.from(`${poll('1', [carried('1')])}\n${record('2')}\n`), 1)
    stray.journal.finish()
    expect(() => stray.journal.assertValid()).toThrow('a record its poll did not carry')

    const commit = open()
    commit.journal.push(
      Buffer.from(`${poll('1', [carried('1')])}\n{"event":"committed","pollId":"2"}\n`),
      1
    )
    commit.journal.finish()
    expect(() => commit.journal.assertValid()).toThrow('commits a poll it did not just report')

    const cut = open()
    cut.journal.push(Buffer.from(`${poll('1', [carried('1')])}\n{"event":"rec`), 1)
    cut.journal.finish()
    expect(() => cut.journal.assertValid()).toThrow('ended inside a line')

    const odd = open()
    odd.journal.push(Buffer.from(`${poll('1', [carried('1', { keyHex: 'abc' })])}\n`), 1)
    odd.journal.finish()
    expect(() => odd.journal.assertValid()).toThrow('malformed poll')
    for (const { dir } of [stray, commit, cut, odd])
      fs.rmSync(dir, { recursive: true, force: true })
  })

  it('builds the watchdog argv with the poll journal flag and an optional halt', () => {
    const base = { bootstrap: 'h:1,h:2', group: 'g', topic: 't', runForMs: 35000 }
    expect(javaArgv(base)).toEqual([
      '--kill-after=5s',
      '50s',
      RUN_SH,
      '--role',
      'share-member',
      '--report-polls',
      'h:1,h:2',
      'g',
      't',
      '35000',
    ])
    expect(javaArgv({ ...base, haltAfter: 1 }).slice(5, 8)).toEqual([
      '--report-polls',
      '--halt-after',
      '1',
    ])
  })
})

describe('share interop halted-poll proof', () => {
  /** @returns {object} Independent corpus, original Java poll and successful JS broker exchange. */
  const haltedState = () => {
    const records = ['0', '1'].map((offset) => ({
      offset,
      identity: shareRecordIdentity({ topic: 't', partition: 0, offset, key: null, value: null }),
    }))
    const acknowledgements = new ShareAcknowledgementObserver()
    acknowledgements.exchanges.push({
      apiKey: 79,
      request: {
        groupId: 'g',
        topics: [
          {
            topicId: 'id',
            partitions: [
              {
                partitionIndex: 0,
                acknowledgementBatches: [
                  { firstOffset: '0', lastOffset: '1', acknowledgeTypes: [1] },
                ],
              },
            ],
          },
        ],
      },
      response: {
        errorCode: 0,
        responses: [{ topicId: 'id', partitions: [{ partitionIndex: 0, errorCode: 0 }] }],
      },
    })
    const poll = { pollId: '1', lockTimeoutMs: LOCK_MS, records }
    return {
      kind: 'halt',
      group: 'g',
      topicId: 'id',
      corpus: new Map([[0, records]]),
      expected: records.map(({ identity }) => identity),
      jsEntries: records.map(({ identity }) => ({ identity })),
      jsFirst: new Map(records.map(({ identity }) => [identity, 200])),
      acknowledgements,
      javaMember: { journal: { polls: [poll], entries: [records[0]], committed: [] } },
    }
  }

  it('requires recovery of unprinted poll records within the actual lock bound', () => {
    const state = haltedState()
    const [poll] = state.javaMember.journal.polls
    const recovered = () => ShareInteropCase.prototype.haltRecovered.call(state, poll, 100)
    expect(recovered()).toBe(true)
    state.jsFirst.delete(poll.records[1].identity)
    expect(recovered()).toBe(false)
    state.jsFirst.set(poll.records[1].identity, 100 + LOCK_MS + RECOVERY_SLACK_MS + 1)
    expect(recovered()).toBe(false)
  })

  it('requires real JS Accepts for printed and unprinted records of an uncommitted poll', () => {
    const state = haltedState()
    const settled = () => ShareInteropCase.prototype.successfulAccepts.call(state)
    expect(settled()).toBe(true)
    const [exchange] = state.acknowledgements.exchanges
    const [partition] = exchange.request.topics[0].partitions
    partition.acknowledgementBatches[0].lastOffset = '0'
    expect(settled()).toBe(false)
    partition.acknowledgementBatches[0].lastOffset = '1'
    delete exchange.response
    exchange.error = { name: 'KafkaJSShareAcknowledgementError', reason: 'session-lost' }
    expect(settled()).toBe(false)
  })

  it('rejects an invented Java commit or handler replay outside the printed set', () => {
    const state = haltedState()
    const settled = () => ShareInteropCase.prototype.successfulAccepts.call(state)
    expect(settled()).toBe(true)
    state.javaMember.journal.committed.push('1')
    expect(settled()).toBe(false)
    state.javaMember.journal.committed = []
    state.jsEntries.push(state.jsEntries[1])
    expect(settled()).toBe(false)
  })
})

const JAVA = GATED
  ? preflight()
  : { ok: false, reason: 'the live gate is off', workDir: null, owned: false }
if (GATED && !JAVA.ok) {
  writeEvidence(EVIDENCE, { event: 'case-skipped', case: 'java-interop', reason: JAVA.reason })
}

enabled('share interop with a Java member', () => {
  if (!JAVA.ok) {
    // A missing prerequisite is a development skip, never a pass; CI treats it as a failure.
    const unrunnable = process.env.CI ? it : it.skip
    for (const title of Object.values(TITLES)) {
      unrunnable(title, () => {
        throw new Error(`The Java interop scenario cannot run: ${JAVA.reason}`)
      })
    }
    return
  }

  beforeAll(
    () =>
      bounded(NEVER, HOOK_MS - 2000, () => {
        liveRun.assertClean()
      }),
    HOOK_MS
  )

  afterAll(() => {
    if (JAVA.owned) fs.rmSync(JAVA.workDir, { recursive: true, force: true })
  })

  for (const kind of ['join', 'leave', 'halt']) {
    const budget = BODY_MS[kind] + CLEANUP_MS
    it(
      TITLES[kind],
      liveCase(budget, (scope) => new ShareInteropCase(kind, JAVA).run(scope)),
      caseTimeout(budget)
    )
  }

  afterEach(() => {
    if (!liveRun.takeAdmission()) return
    liveRun.assertClean()
  })
})
