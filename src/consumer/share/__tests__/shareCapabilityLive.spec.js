const fs = require('fs')
const net = require('net')
const path = require('path')
const { isDeepStrictEqual } = require('util')
const {
  Kafka,
  logLevel,
  AlterConfigOpTypes,
  ConfigResourceTypes,
  ConfigSource,
  KafkaJSServerDoesNotSupportApiKey,
  KafkaJSShareGroupsNotSupportedError,
} = require('../../../../index')
const {
  createTopic,
  execInKafka1,
  plainTextBrokers,
  secureRandom,
  writeEvidence,
  wireVersionsOf,
} = require('testHelpers')
const {
  assertShareCleanup,
  setShareGroupConfigs,
  shareRecordIdentity,
  shareTopicName,
} = require('../../../../testHelpers/shareFixtures')
const {
  awaitTerminalSharePosition,
  assertQuietTerminalGroup,
  ShareAcknowledgementObserver,
} = require('../../../../testHelpers/shareLiveDiagnostics')
const { readLog } = require('../../../../testHelpers/rawLog')
const { createResourceProbe } = require('../../../../testHelpers/resourceProbe')
const {
  SHARE_VERSION,
  featuresOf,
  readApiVersions,
} = require('../../../../scripts/e2e/apiVersionsProbe')
const {
  NEVER,
  bounded,
  caseTimeout,
  liveCase,
  liveRun,
  pollTolerating,
  pollUntil,
} = require('../../../producer/__tests__/fixtures/bounded')
const { CleanupStack, describeError } = require('../../../producer/__tests__/fixtures/liveProbe')
const { FrameLog } = require('../../../producer/__tests__/fixtures/liveWire')

// Gated: it needs the share-capable base cluster (docker-compose.yml) of the broker line named by
// KAFKA_VERSION, and runs through the share lane or the guarded live entry with
// SHARE_CAPABILITY_E2E=1. The line alone picks which public result is expected.
const enabled = process.env.SHARE_CAPABILITY_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'share-capability'
const PROFILE = 'tv2'
const LINE = process.env.KAFKA_VERSION
const TITLE_DEFAULT = 'reports the designated share capability or completes one round trip'
const TITLE_DISABLED = 'reports a disabled share feature and restores its original level'
const DEFAULT_BUDGET_MS = 25000
const DISABLED_BUDGET_MS = 40000
const HOOK_MS = 10000
const CLEANUP_MS = 10000
const OLD_TIMESTAMP = '1700000000000'
const SHARE_API_KEYS = [76, 78, 79]
const CLIENT_CONFIG = { acquireMode: 'record_limit', maxRecords: 1, maxWaitTimeInMs: 250 }
const GROUP_CONFIG = { 'share.auto.offset.reset': 'earliest' }
const FEATURE_TABLE = path.join(__dirname, '../../../../scripts/protocol/brokerFeatures.json')
const FEATURES_TOOL = ['/opt/kafka/bin/kafka-features.sh', '--bootstrap-server', 'kafka1:29092']

/** What a client of a broker that does not advertise share groups, or only an older range, sees. */
const UNSUPPORTED = {
  '4.0.2': {
    reason: 'api-not-advertised',
    apiKey: 76,
    apiName: 'ShareGroupHeartbeat',
    shareVersion: 0,
  },
  '4.1.2': { reason: 'api-version-range', apiKey: 78, apiName: 'ShareFetch', shareVersion: 0 },
}

/** What a client sees once the share feature is switched off on a capable broker. */
const FEATURE_DISABLED = {
  reason: 'feature-disabled',
  apiKey: 76,
  apiName: 'ShareGroupHeartbeat',
  shareVersion: 0,
}

/** The broker lines on which a share round trip must succeed. */
const SUPPORTED_LINES = ['4.2.2', '4.3.1', '4.4.0']

/**
 * @param {string} line a broker version
 * @returns {number} the finalized `share.version` level a fresh cluster of that line starts at
 */
const defaultLevel = (line) => (line === '4.4.0' ? 2 : 1)

/**
 * @param {string|undefined} line the designated broker version
 * @returns {object|null} the refusal the line must give, `{ connected: true, shareVersion }` where
 *   it must connect, or null for a line this scenario has no expectation for
 */
const expectedOutcome = (line) => {
  if (UNSUPPORTED[line]) return UNSUPPORTED[line]
  return SUPPORTED_LINES.includes(line)
    ? { connected: true, shareVersion: defaultLevel(line) }
    : null
}

/**
 * @param {'upgrade'|'downgrade'} action the tool command
 * @param {number} level the `share.version` level to move to
 * @returns {string[]} the argv run inside the broker container
 */
const featureArgv = (action, level) => [
  ...FEATURES_TOOL,
  action,
  '--feature',
  `${SHARE_VERSION}=${level}`,
]

/** @returns {string[]} the argv that prints every feature's finalized state */
const describeArgv = () => [...FEATURES_TOOL, 'describe']

/**
 * @param {string} line a broker version
 * @returns {number|null} the level the committed feature table records for it, or null when the
 *   table is unreadable or has no entry
 */
const recordedLevel = (line) => {
  try {
    return JSON.parse(fs.readFileSync(FEATURE_TABLE, 'utf8'))[line]?.[SHARE_VERSION] ?? null
  } catch {
    return null
  }
}

/**
 * @param {{ sockets: { open: number }, activeResources: Record<string, number>, listeners: Record<string, number>, queueDepth: number, ledgerSize: number }} snapshot a measurement
 * @returns {object} the evidence form of the measurement
 */
const resourceSummary = (snapshot) => ({
  openSockets: snapshot.sockets.open,
  timers: snapshot.activeResources.Timeout ?? 0,
  listeners: Object.values(snapshot.listeners).reduce((sum, n) => sum + n, 0),
  queueDepth: snapshot.queueDepth,
  ledgerSize: snapshot.ledgerSize,
})

/**
 * @param {object} before a measurement
 * @param {object} after a later measurement of the same owners
 * @returns {string[]} the measured resources that differ
 */
const resourceDifferences = (before, after) =>
  [
    ['sockets', before.sockets.open, after.sockets.open],
    ['queueDepth', before.queueDepth, after.queueDepth],
    ['ledgerSize', before.ledgerSize, after.ledgerSize],
    ['listeners', before.listeners, after.listeners],
    ['activeResources', before.activeResources, after.activeResources],
  ]
    .filter(([, was, now]) => !isDeepStrictEqual(was, now))
    .map(([name, was, now]) => `${name}: ${JSON.stringify(was)} -> ${JSON.stringify(now)}`)

/**
 * Logs every request frame the clients of one case write and counts their sockets and unanswered
 * requests from the bytes actually written and read. Every byte passes through untouched.
 */
class SocketLedger {
  /** Starts with no sockets. */
  constructor() {
    this.entries = []
    this.log = new FrameLog()
  }

  /** @returns {Function} a Kafka `socketFactory` whose sockets this ledger observes */
  factory() {
    return this.log.observe(({ host, port, onConnect }) => this.open(host, port, onConnect))
  }

  /**
   * @param {string} host the broker host
   * @param {number} port the broker port
   * @param {Function} onConnect the client's connect callback
   * @returns {import('net').Socket} a plaintext socket, counted
   */
  open(host, port, onConnect) {
    const socket = net.connect({ host, port }, onConnect)
    const entry = { socket, answered: 0, carry: Buffer.alloc(0) }
    socket.on('data', (chunk) => this.read(entry, chunk))
    this.entries.push(entry)
    return socket
  }

  /**
   * @param {{ answered: number, carry: Buffer }} entry the socket's bookkeeping
   * @param {Buffer} chunk the next bytes the broker sent on it
   * @returns {void} counts the response frames the chunk completed
   */
  read(entry, chunk) {
    let bytes = Buffer.concat([entry.carry, chunk])
    while (bytes.length >= 4 && bytes.length >= 4 + bytes.readInt32BE(0)) {
      bytes = bytes.subarray(4 + bytes.readInt32BE(0))
      entry.answered++
    }
    entry.carry = bytes
  }

  /** @returns {{ created: number, destroyed: number }} sockets opened and since destroyed */
  counts() {
    return {
      created: this.entries.length,
      destroyed: this.entries.filter(({ socket }) => socket.destroyed).length,
    }
  }

  /** @returns {number} requests written on live sockets that have no response yet */
  pending() {
    return this.entries.reduce(
      (total, { socket, answered }, index) =>
        socket.destroyed
          ? total
          : total +
            this.log.frames.filter(({ connection }) => connection === index + 1).length -
            answered,
      0
    )
  }

  /** @returns {object[]} every share request frame written, with its header facts */
  shareFrames() {
    return this.log.frames
      .filter(({ apiKey }) => SHARE_API_KEYS.includes(apiKey))
      .map(({ apiKey, apiVersion, correlationId, connection }) => ({
        apiKey,
        version: apiVersion,
        correlationId,
        socketId: connection,
      }))
  }
}

/**
 * One refusal attempt against a feature that is switched off for the occasion. Every step but the
 * last runs with its failure retained, so the public attempt is always made; the original level is
 * restored exactly once afterwards, whatever happened before, and a failure of the restoration is
 * reported next to the primary failure and never replaces it.
 */
class FeatureOffRun {
  /**
   * @param {object} steps the primitive operations, each a function returning a promise
   * @param {() => Promise<{ shareVersion: number }>} steps.capture reads the level before any change
   * @param {() => Promise<void>} steps.downgrade switches the feature off
   * @param {() => Promise<void>} steps.observeOff waits until the switched-off level is visible
   * @param {() => Promise<{ error: Error|null }>} steps.attempt makes the one public connect
   * @param {(original: number|undefined) => Promise<void>} steps.restore puts the level back
   */
  constructor(steps) {
    this.steps = steps
    this.order = []
  }

  /**
   * @param {(state: object) => void} check judges the attempt and throws when it is not the
   *   expected refusal
   * @returns {Promise<{ original: number|undefined, attempt: object|undefined, retained: object }>}
   *   what the steps returned or threw, once the level is restored
   * @throws {Error} the first of: the check's failure, a failed downgrade, an uncaptured original
   *   level; else a failed restoration. A restoration failure after a primary one is attached to it
   *   as `cleanupErrors`.
   */
  async run(check) {
    const state = { original: undefined, attempt: undefined, retained: {} }
    const retain = async (name, step) => {
      this.order.push(name)
      try {
        return await step()
      } catch (error) {
        state.retained[name] = error
        return undefined
      }
    }
    state.original = (await retain('capture', () => this.steps.capture()))?.shareVersion
    await retain('downgrade', () => this.steps.downgrade())
    await retain('observeOff', () => this.steps.observeOff())
    state.attempt = await retain('attempt', () => this.steps.attempt())

    let primary = null
    try {
      check(state)
      primary =
        state.retained.downgrade ??
        (state.original === undefined
          ? new Error('The original share.version level was not captured', {
              cause: state.retained.capture,
            })
          : null)
    } catch (error) {
      primary = error
    }

    let cleanup = null
    this.order.push('restore')
    try {
      await this.steps.restore(state.original)
    } catch (error) {
      cleanup = error
    }
    if (primary) {
      if (cleanup) primary.cleanupErrors = [cleanup]
      throw primary
    }
    if (cleanup) throw cleanup
    return state
  }
}

/**
 * One live run of the capability scenario on the designated broker line: the line decides whether
 * the public client must refuse share groups, and with which reason, or complete one round trip.
 * The disabled run switches the feature off on a capable line and always restores the level it
 * found.
 */
class CapabilityCase {
  /** Builds a case with fresh names; nothing is created until `default` or `disabled`. */
  constructor() {
    this.nonce = secureRandom(6)
    this.line = LINE
    this.topic = shareTopicName('capability')
    this.group = `e2e-p9e-capability-group-${this.nonce}`
    this.sentinel = `e2e-p9e-capability-sentinel-${this.nonce}`
    this.title = null
    this.budgetMs = null
    this.stack = new CleanupStack()
    this.ledger = new SocketLedger()
    this.acknowledgements = new ShareAcknowledgementObserver()
    this.probe = createResourceProbe({
      emitters: { process },
      sockets: () => this.ledger.counts(),
      queueDepth: () => this.ledger.pending(),
      ledgerSize: () => this.openHandlers,
      activeResources: true,
    })
    this.requests = []
    this.crashes = []
    this.acks = []
    this.entries = []
    this.openHandlers = 0
    this.connects = 0
    this.disconnects = 0
    this.connectAttempts = 0
    this.baseline = null
    this.after = null
    this.admin = null
    this.sentinelCreated = false
    this.feature = { transcripts: [] }
  }

  /**
   * @param {string} label the client label, also the start of its clientId
   * @returns {Kafka} a client whose sockets go through the ledger
   */
  client(label) {
    return new Kafka({
      clientId: `${label}-${this.nonce}`,
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      socketFactory: this.ledger.factory(),
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
      groups: { capability: this.group },
      budgets: { caseMs: this.budgetMs, cleanupMs: CLEANUP_MS },
      wireVersions: wireVersionsOf(this.requests),
      ...fields,
    })
  }

  /** @returns {object} the facts kept when a case fails, before cleanup can change them */
  progress() {
    return {
      phase: this.phase,
      connectAttempts: this.connectAttempts,
      acknowledgements: this.acknowledgements.evidence(this.group),
      shareFrames: this.ledger.shareFrames(),
      crashes: this.crashes.map((error) => describeError(error)),
      feature: this.feature,
    }
  }

  /**
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<object>} one measurement of the case's resource owners
   */
  measure(scope) {
    return bounded(scope, 10000, () => this.probe.snapshot())
  }

  /**
   * Reads the share capability the broker announces over its own connection, apart from the
   * client under test. It never throws: a failed read is a retained fact, never a skipped case.
   * @param {AbortSignal} scope the owner of the read
   * @returns {Promise<object>} `{ status: 'ok', shareVersion, ... }` or `{ status: 'failed', ... }`
   */
  async capture(scope) {
    try {
      const answer = await bounded(scope, 10000, () => readApiVersions())
      const ranges = {}
      for (const apiKey of SHARE_API_KEYS) {
        const entry = answer.apiVersions.find((api) => api.apiKey === apiKey)
        ranges[apiKey] = entry ? [entry.minVersion, entry.maxVersion] : null
      }
      return {
        status: 'ok',
        shareVersion: featuresOf(answer)[SHARE_VERSION] ?? null,
        finalizedFeaturesEpoch: String(answer.finalizedFeaturesEpoch),
        ranges,
        recordedShareVersion: recordedLevel(this.line),
      }
    } catch (error) {
      return {
        status: 'failed',
        errorClass: error?.name ?? 'Error',
        message: String(error?.message).slice(0, 200),
      }
    }
  }

  /**
   * Builds the share consumer and makes the one public connect, capturing what it throws before
   * anything is asserted. The consumer's disconnect is registered before the connect starts.
   * @param {AbortSignal} scope the case scope
   * @param {string} [label] the client label
   * @returns {Promise<{ consumer: object, error: Error|null }>} what connect threw, if anything
   */
  async attempt(scope, label = 'capability') {
    const consumer = this.client(label).shareConsumer({
      groupId: this.group,
      acknowledgementMode: 'implicit',
      ...CLIENT_CONFIG,
    })
    const { events } = consumer
    const removers = [
      consumer.on(events.REQUEST, ({ payload }) => this.requests.push(payload)),
      consumer.on(events.CRASH, ({ payload }) => this.crashes.push(payload.error)),
      consumer.on(events.ACKNOWLEDGE, ({ payload }) => this.acks.push(payload)),
      consumer.on(events.CONNECT, () => this.connects++),
      consumer.on(events.DISCONNECT, () => this.disconnects++),
    ]
    this.stack.add(`remove ${label} listeners`, () => removers.forEach((remove) => remove()))
    this.stack.add(`disconnect ${label}`, () => consumer.disconnect({ timeout: 2000 }))
    this.consumer = consumer
    this.connectAttempts++
    try {
      await bounded(scope, 10000, () => consumer.connect())
      return { consumer, error: null }
    } catch (error) {
      return { consumer, error }
    }
  }

  /**
   * @param {Error|null} error what connect threw
   * @param {{ reason: string, apiKey: number, apiName: string, shareVersion: number }} expected
   * @returns {void}
   * @throws {Error} on the first property that is not the expected refusal
   */
  expectRefusal(error, expected) {
    if (error === null) throw new Error('connect resolved on a broker that cannot run share groups')
    expect(error).toBeInstanceOf(KafkaJSShareGroupsNotSupportedError)
    expect(error).toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
    expect(error).toMatchObject({ ...expected, retriable: false })
    expect(error.message).toContain(expected.reason)
  }

  /**
   * Runs the default scenario of the designated line: a refusal on a line that cannot run share
   * groups, one record round trip on a line that can.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} the first assertion or fixture failure, with cleanup failures attached
   */
  async default(scope) {
    this.title = TITLE_DEFAULT
    this.budgetMs = DEFAULT_BUDGET_MS
    this.acknowledgements.install()
    try {
      const expected = expectedOutcome(this.line)
      if (!expected) throw new Error(`No share capability expectation for broker line ${this.line}`)
      if (expected.connected) await this.roundTrip(scope, expected)
      else await this.refused(scope, expected)
    } catch (error) {
      this.row('case-failed', { error: describeError(error), ...this.progress() })
      throw error
    } finally {
      this.acknowledgements.restore()
    }
  }

  /**
   * Runs the disabled scenario: switches the feature off, makes the public attempt, and restores
   * the level that was captured before the change.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} the first assertion or fixture failure, with a restoration failure attached
   */
  async disabled(scope) {
    this.title = TITLE_DISABLED
    this.budgetMs = DISABLED_BUDGET_MS
    try {
      this.baseline = await this.measure(scope)
      this.row('resource-baseline', { snapshot: this.baseline })
      const run = new FeatureOffRun({
        capture: () => this.captureOriginal(scope),
        downgrade: () => this.switchOff(),
        observeOff: () => this.observeOff(scope),
        attempt: () => this.attempt(scope),
        restore: (original) => this.restore(original),
      })
      let state
      await this.stack.run(
        async () => {
          state = await run.run((outcome) => this.checkRefusal(outcome, FEATURE_DISABLED))
        },
        { budgetMs: CLEANUP_MS }
      )
      await this.settleResources(scope)
      this.complete({
        outcome: this.outcomeOf(state.attempt.error),
        capture: this.feature.before,
        featureChange: this.feature.change,
        featureRestore: this.feature.restore,
        featureCommands: this.feature.transcripts,
        order: run.order,
      })
    } catch (error) {
      this.row('case-failed', { error: describeError(error), ...this.progress() })
      throw error
    }
  }

  /**
   * @param {{ attempt: object|undefined, retained: object }} state what the run kept
   * @param {object} expected the refusal to require
   * @returns {void}
   * @throws {Error} when the attempt is not that refusal or wrote a share frame
   */
  checkRefusal(state, expected) {
    const error = state.attempt ? state.attempt.error : (state.retained.attempt ?? null)
    this.refusedFrames = this.ledger.shareFrames()
    this.expectRefusal(error, expected)
    expect(this.refusedFrames).toEqual([])
    expect(this.connects).toBe(0)
  }

  /**
   * @param {Error|null} error what connect threw
   * @returns {object} the evidence form of the public outcome
   */
  outcomeOf(error) {
    const { name, reason, apiKey, apiName, shareVersion, retriable, message } = error
    return {
      kind: 'unsupported',
      className: name,
      reason,
      apiKey,
      apiName,
      shareVersion,
      retriable,
      message,
    }
  }

  /**
   * A line that cannot run share groups: the public client is built and connected once, nothing
   * else is created, and the sockets and listeners the attempt used are all gone afterwards.
   * @param {AbortSignal} scope the case scope
   * @param {object} expected the refusal this line must give
   * @returns {Promise<void>}
   */
  async refused(scope, expected) {
    this.baseline = await this.measure(scope)
    this.row('resource-baseline', { snapshot: this.baseline })
    const capture = await this.capture(scope)
    let attempt
    await this.stack.run(
      async () => {
        attempt = await this.attempt(scope)
        this.checkRefusal({ attempt, retained: {} }, expected)
      },
      { budgetMs: CLEANUP_MS }
    )
    await this.settleResources(scope)
    this.complete({ outcome: this.outcomeOf(attempt.error), capture })
  }

  /**
   * Waits for the sockets and event-loop resources of the case to be back at the baseline.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} naming each resource that still differs at the deadline
   */
  async settleResources(scope) {
    await pollTolerating(
      scope,
      8000,
      async () => {
        this.after = await this.probe.snapshot()
        const differences = resourceDifferences(this.baseline, this.after)
        if (differences.length) throw new Error(`Resources differ: ${differences.join('; ')}`)
        return true
      },
      { delayMs: 50, label: 'The resources returning to baseline' }
    )
  }

  /**
   * Records the single completion row of a refusal.
   * @param {object} facts the public outcome, the optional capture and any feature facts
   * @returns {void}
   */
  complete(facts) {
    this.row('case-complete', {
      status: 'passed',
      tainted: false,
      connectAttempted: this.connectAttempts > 0,
      shareFrames: this.refusedFrames,
      oracles: {
        log: { expected: [], actual: [] },
        handler: { expected: [], actual: [] },
        broker: [],
      },
      resources: { before: resourceSummary(this.baseline), after: resourceSummary(this.after) },
      cleanup: {
        groupsAbsent: true,
        topicsAbsent: true,
        completedAt: Number(process.hrtime.bigint() / 1000000n),
      },
      config: { client: CLIENT_CONFIG, line: this.line },
      ...facts,
    })
  }

  /**
   * Runs one command inside the broker container and keeps its transcript.
   * @param {string[]} argv the command and its arguments
   * @returns {string} what it printed
   * @throws {Error} when it exits non-zero
   */
  command(argv) {
    const entry = { argv, output: null, error: null }
    this.feature.transcripts.push(entry)
    try {
      entry.output = String(execInKafka1(argv)).slice(0, 2000)
      return entry.output
    } catch (error) {
      entry.error = String(error.message).slice(0, 300)
      throw error
    }
  }

  /**
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<{ shareVersion: number }>} the level before any change
   * @throws {Error} when the level could not be read, so nothing is invented to restore
   */
  async captureOriginal(scope) {
    const before = await this.capture(scope)
    this.feature.before = before
    if (before.status !== 'ok' || typeof before.shareVersion !== 'number') {
      throw new Error(`The share.version level could not be captured: ${JSON.stringify(before)}`)
    }
    return before
  }

  /**
   * @returns {Promise<void>} once the tool has switched the feature off and described the result
   * @throws {Error} when the tool fails
   */
  async switchOff() {
    this.feature.change = { attempted: true, observed: null, error: null }
    try {
      this.command(featureArgv('downgrade', 0))
      this.command(describeArgv())
    } catch (error) {
      this.feature.change.error = String(error.message).slice(0, 300)
      throw error
    }
  }

  /**
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>} once an independent read shows level 0
   * @throws {Error} when it does not within the deadline; the public attempt is made anyway
   */
  async observeOff(scope) {
    await pollTolerating(
      scope,
      15000,
      async (poll) => {
        const capture = await this.capture(poll)
        this.feature.change.observed = capture
        return capture.status === 'ok' && capture.shareVersion === 0
      },
      { delayMs: 250, label: 'The share feature switching off' }
    )
  }

  /**
   * Puts the captured level back, proves it by an independent read and, for a level that allows
   * share groups, by one public control connection that is closed again.
   * @param {number|undefined} original the level captured before the change
   * @returns {Promise<void>}
   * @throws {Error} when the level is unknown, the tool fails, the level is not read back or the
   *   control connection is refused
   */
  async restore(original) {
    const restore = {
      attempted: true,
      original: original ?? null,
      observed: null,
      succeeded: false,
    }
    this.feature.restore = restore
    if (original === undefined) {
      throw new Error('The original share.version level is unknown, so none is restored')
    }
    if ((await this.capture(NEVER)).shareVersion !== original) {
      this.command(original >= 1 ? featureArgv('upgrade', original) : featureArgv('downgrade', 0))
    }
    await pollTolerating(
      NEVER,
      20000,
      async () => {
        restore.observed = (await this.capture(NEVER)).shareVersion
        return restore.observed === original
      },
      { delayMs: 250, label: 'The share.version restoration' }
    )
    if (original >= 1) {
      const control = this.client('control').shareConsumer({
        groupId: this.group,
        ...CLIENT_CONFIG,
      })
      try {
        await bounded(NEVER, 10000, () => control.connect())
        restore.control = 'connected'
      } finally {
        await bounded(NEVER, 5000, () => control.disconnect({ timeout: 2000 }))
      }
    }
    restore.succeeded = true
  }

  /**
   * A line that can run share groups: one plain record is delivered once and accepted, with the
   * log, the broker position and the handler checked independently.
   * @param {AbortSignal} scope the case scope
   * @param {{ shareVersion: number }} expected the level this line starts at
   * @returns {Promise<void>}
   */
  async roundTrip(scope, expected) {
    await this.stack.run(
      async () => {
        this.phase = 'setup'
        await this.setup(scope)
        this.phase = 'capture-before'
        this.before = await this.capture(scope)
        this.phase = 'delivery'
        await this.connectAndReceive(scope)
        this.phase = 'capture-after'
        this.afterCapture = await this.capture(scope)
        this.phase = 'observation'
        await this.observe(scope, expected)
        this.phase = 'verification'
        await this.finish(scope)
        this.phase = 'cleanup'
      },
      { budgetMs: CLEANUP_MS }
    )
    await this.completeConnected(scope, expected)
  }

  /**
   * Measures the baseline, creates the topic and the group config, and connects the admin and the
   * producer. An unrelated group is left in place for the cleanup proof to preserve.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async setup(scope) {
    this.beforeAdmin = await this.probe.snapshot()
    this.admin = this.client('admin').admin()
    this.admin.on(this.admin.events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.stack.add('prove cleanup and disconnect admin', (signal) => this.cleanup(signal))
    this.stack.add('delete topic', (signal) => this.deleteTopic(signal))
    this.stack.add('delete group', (signal) => this.deleteGroup(signal, this.group))
    this.stack.add('reset group configs', (signal) => this.resetConfigs(signal))
    await bounded(scope, 15000, () => this.admin.connect())
    await bounded(scope, 30000, () =>
      createTopic({
        topic: this.topic,
        partitions: 1,
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
    this.sentinelCreated = true
    await bounded(scope, 10000, () =>
      this.admin.setOffsets({
        groupId: this.sentinel,
        topic: this.topic,
        partitions: [{ partition: 0, offset: '0' }],
      })
    )
    await bounded(scope, 15000, () => setShareGroupConfigs(this.admin, this.group, GROUP_CONFIG))
    this.configReadAt = performance.now()
    this.baseline = await bounded(scope, 60000, () =>
      bounded(scope, 10000, () => this.probe.snapshot())
    )
    this.row('config-readback', { group: GROUP_CONFIG, client: CLIENT_CONFIG })
    this.row('resource-baseline', { snapshot: this.baseline })

    this.producer = this.client('producer').producer({ idempotent: true })
    this.producer.on(this.producer.events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.stack.add('disconnect producer', () => this.producer.disconnect())
    await bounded(scope, 15000, () => this.producer.connect())
  }

  /**
   * Makes the public connect, subscribes, runs the handler and sends the one record. A refusal
   * fails the case; it is never counted as an unsupported pass on a line that must connect.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async connectAndReceive(scope) {
    const { consumer, error } = await this.attempt(scope)
    this.connectedAt = performance.now()
    if (error) {
      this.outcome = this.outcomeOf(error)
      throw error
    }
    expect(consumer.subscription()).toEqual([])
    expect(consumer.assignment()).toEqual([])
    await bounded(scope, 10000, () => consumer.subscribe({ topics: [this.topic] }))
    expect(consumer.subscription()).toEqual([this.topic])
    await bounded(scope, 15000, () =>
      consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          this.openHandlers++
          try {
            this.entries.push({
              identity: shareRecordIdentity({ ...message, topic, partition }),
              deliveryCount: message.deliveryCount,
            })
          } finally {
            this.openHandlers--
          }
        },
      })
    )
    const message = {
      partition: 0,
      key: Buffer.from('capability:0000'),
      value: Buffer.from('capability:value:0'),
      timestamp: OLD_TIMESTAMP,
    }
    const [sent] = await bounded(scope, 15000, () =>
      this.producer.send({ topic: this.topic, acks: -1, messages: [message] })
    )
    expect(sent.errorCode).toBe(0)
    expect(sent.partition).toBe(0)
    this.expected = shareRecordIdentity({
      topic: this.topic,
      partition: 0,
      offset: sent.baseOffset,
      key: message.key,
      value: message.value,
    })
    await pollUntil(scope, 15000, () => this.entries.length >= 1, { label: 'The delivery' })
    await pollUntil(scope, 10000, () => this.successfulAccepts(), {
      label: 'The terminal accept',
    })
  }

  /**
   * Checks the capture agreement and the three independent oracles: raw log, handler entries and
   * broker position.
   * @param {AbortSignal} scope the case scope
   * @param {{ shareVersion: number }} expected the level this line starts at
   * @returns {Promise<void>}
   */
  async observe(scope, expected) {
    this.outcome = { kind: 'connected', shareVersion: this.afterCapture.shareVersion }
    for (const capture of [this.before, this.afterCapture]) {
      expect(capture.status).toBe('ok')
      expect(capture.shareVersion).toBe(expected.shareVersion)
      if (capture.recordedShareVersion !== null) {
        expect(capture.recordedShareVersion).toBe(expected.shareVersion)
      }
    }
    expect(this.outcome.shareVersion).toBe(expected.shareVersion)
    expect(this.configReadAt).toBeLessThan(this.connectedAt)
    expect(this.connects).toBe(1)

    const log = await bounded(scope, 20000, () =>
      readLog({ brokers: plainTextBrokers(), topic: this.topic })
    )
    const { endOffset, records } = log.get(0)
    expect(endOffset).toBe('1')
    expect(records.filter(({ batchContext }) => batchContext.isControlBatch)).toEqual([])
    this.rawIdentities = records.map(({ offset, key, value }) =>
      shareRecordIdentity({ topic: this.topic, partition: 0, offset: String(offset), key, value })
    )
    expect(this.rawIdentities).toEqual([this.expected])
    expect(this.entries.map(({ identity }) => identity)).toEqual([this.expected])
    expect(this.entries[0].deliveryCount).toBe(1)
    expect(this.successfulAccepts()).toBe(true)
    this.broker = await this.awaitBrokerEnd(scope, endOffset)
    const accepts = this.acks.filter(({ type }) => type !== 'renew')
    expect(accepts.map(({ type, error }) => [type, error])).toEqual([['accept', null]])
    const frames = this.ledger.shareFrames()
    const versions = (apiKey) => [
      ...new Set(frames.filter((f) => f.apiKey === apiKey).map((f) => f.version)),
    ]
    expect(versions(76)).toEqual([1])
    expect(versions(78)).toEqual([2])
    expect(versions(79).filter((version) => version !== 2)).toEqual([])
  }

  /** @returns {boolean} The one fixed identity has a successful original Accept receipt. */
  successfulAccepts() {
    if (
      this.acks.some(({ type, error }) => type !== 'renew' && (type !== 'accept' || error !== null))
    )
      return false
    const types = this.acknowledgements.types(this.group, this.topicId, 0)
    return types.size === 1 && JSON.stringify(types.get('0')) === '[1]'
  }

  /**
   * Requires agreeing terminal listings, known zero lag and the independent fixed log end.
   * @param {AbortSignal} scope Case scope.
   * @param {string} logEnd Independently read log end.
   * @returns {Promise<object[]>} Actual persisted rows.
   */
  async awaitBrokerEnd(scope, logEnd) {
    this.terminalPartitions = [{ partition: 0, floor: '0', end: logEnd }]
    const position = await awaitTerminalSharePosition(
      this.admin,
      { groupId: this.group, topic: this.topic, partitions: this.terminalPartitions },
      {
        signal: scope,
        timeoutMs: 20000,
        record: (fields) => this.row('broker-position', { ...fields, ...this.progress() }),
      }
    )
    return position.rows
  }

  /**
   * Closes the public client, then lets a fresh member of the same group listen for five seconds.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async finish(scope) {
    await bounded(scope, 10000, () => this.consumer.disconnect({ timeout: 5000 }))
    expect(this.disconnects).toBe(1)
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
    expect(this.entries.map(({ identity }) => identity)).toEqual([this.expected])
    expect(this.successfulAccepts()).toBe(true)
    expect(this.crashes.map((error) => error.message)).toEqual([])
  }

  /**
   * Proves owned objects are gone and resources are back at baseline, keeping the unrelated group
   * until the proof is made, then removes it and disconnects the admin. It runs last.
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   * @throws {Error} the first failure of the proof, the removal or the disconnect
   */
  async cleanup(signal) {
    let failure = null
    try {
      if (this.baseline) {
        await pollUntil(
          signal,
          3000,
          () => {
            const { created, destroyed } = this.ledger.counts()
            return created - destroyed <= this.baseline.sockets.open
          },
          { delayMs: 50, label: 'The sockets closing' }
        )
        this.after = await assertShareCleanup(
          this.admin,
          [this.group],
          [this.topic],
          this.baseline,
          this.probe,
          { unrelated: this.sentinelCreated ? [this.sentinel] : [], signal }
        )
      }
    } catch (error) {
      failure = error
    }
    try {
      if (this.sentinelCreated) await this.deleteGroup(signal, this.sentinel)
    } catch (error) {
      failure ??= error
    }
    try {
      await this.admin.disconnect()
    } catch (error) {
      failure ??= error
    }
    if (failure) throw failure
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
   * @param {string} groupId the group to remove
   * @returns {Promise<void>}
   * @throws {Error} when the group is still listed after deletion
   */
  async deleteGroup(signal, groupId) {
    await pollUntil(
      signal,
      10000,
      async () => {
        try {
          await this.admin.deleteGroups([groupId])
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
        !(await this.admin.listGroups()).groups.some((group) => group.groupId === groupId),
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
      throw new Error(`Group config overrides remain: ${left.map((entry) => entry.configName)}`)
    }
  }

  /**
   * Checks the admin's own sockets are closed, then records the single completion row.
   * @param {AbortSignal} scope the case scope
   * @param {{ shareVersion: number }} expected the level this line starts at
   * @returns {Promise<void>}
   */
  async completeConnected(scope, expected) {
    await pollUntil(
      scope,
      5000,
      async () => (await this.probe.snapshot()).sockets.open === this.beforeAdmin.sockets.open,
      { delayMs: 50, label: 'The admin sockets closing' }
    )
    this.row('cleanup', { snapshot: await this.measure(scope) })
    this.row('case-complete', {
      status: 'passed',
      tainted: false,
      connectAttempted: true,
      capture: this.before,
      outcome: this.outcome,
      shareFrames: this.ledger.shareFrames(),
      oracles: {
        log: { expected: [this.expected], actual: [...this.rawIdentities] },
        handler: {
          expected: [[this.expected, 1]],
          actual: [[this.entries[0].identity, this.entries.length]],
        },
        broker: this.broker,
        acknowledgements: this.acknowledgements.evidence(this.group),
        quiet: this.quiet,
      },
      resources: { before: resourceSummary(this.baseline), after: resourceSummary(this.after) },
      cleanup: {
        groupsAbsent: true,
        topicsAbsent: true,
        completedAt: Number(process.hrtime.bigint() / 1000000n),
      },
      config: { client: CLIENT_CONFIG, group: GROUP_CONFIG, level: expected.shareVersion },
    })
  }
}

describe('share capability expectations', () => {
  it.each([
    ['4.0.2', { reason: 'api-not-advertised', apiKey: 76, shareVersion: 0 }],
    ['4.1.2', { reason: 'api-version-range', apiKey: 78, shareVersion: 0 }],
    ['4.2.2', { connected: true, shareVersion: 1 }],
    ['4.3.1', { connected: true, shareVersion: 1 }],
    ['4.4.0', { connected: true, shareVersion: 2 }],
  ])('line %s gives its own public result', (line, expected) => {
    expect(expectedOutcome(line)).toMatchObject(expected)
  })

  it('has no expectation for a line it does not know', () => {
    expect(expectedOutcome('4.3.2')).toBeNull()
    expect(expectedOutcome(undefined)).toBeNull()
  })

  it('moves a feature only to the level the tool is told', () => {
    expect(featureArgv('downgrade', 0).slice(-3)).toEqual([
      'downgrade',
      '--feature',
      'share.version=0',
    ])
    expect(featureArgv('upgrade', 2).slice(-3)).toEqual(['upgrade', '--feature', 'share.version=2'])
  })
})

describe('feature-off run', () => {
  const REFUSED = { error: new Error('refused') }

  /**
   * @param {object} [overrides] steps that replace the default ones
   * @param {number} [original] the level the default capture reports
   * @returns {{ steps: object, calls: string[] }} spies that record each step in call order
   */
  const harness = (overrides = {}, original = 1) => {
    const calls = []
    const step = (name, result) =>
      jest.fn(async (...args) => {
        calls.push(name)
        return typeof result === 'function' ? result(...args) : result
      })
    const steps = {
      capture: step('capture', { shareVersion: original }),
      downgrade: step('downgrade'),
      observeOff: step('observeOff'),
      attempt: step('attempt', REFUSED),
      restore: step('restore'),
    }
    for (const [name, result] of Object.entries(overrides)) steps[name] = step(name, result)
    return { steps, calls }
  }

  const expectRefusal = ({ attempt }) => {
    if (attempt?.error?.message !== 'refused') throw new Error('did not refuse')
  }
  const ORDER = ['capture', 'downgrade', 'observeOff', 'attempt', 'restore']

  it.each([0, 1, 2])('restores exactly the captured level %i, after one attempt', async (level) => {
    const { steps, calls } = harness({}, level)
    await new FeatureOffRun(steps).run(expectRefusal)
    expect(calls).toEqual(ORDER)
    expect(steps.attempt).toHaveBeenCalledTimes(1)
    expect(steps.restore).toHaveBeenCalledTimes(1)
    expect(steps.restore).toHaveBeenCalledWith(level)
  })

  it('still attempts and restores when the capture throws, and fails the run', async () => {
    const { steps, calls } = harness({
      capture: () => {
        throw new Error('no capture')
      },
    })
    await expect(new FeatureOffRun(steps).run(expectRefusal)).rejects.toThrow(
      'The original share.version level was not captured'
    )
    expect(calls).toEqual(ORDER)
    expect(steps.restore).toHaveBeenCalledWith(undefined)
  })

  it('still attempts and restores when the downgrade throws, and fails the run with it', async () => {
    const { steps, calls } = harness({
      downgrade: () => {
        throw new Error('no downgrade')
      },
    })
    await expect(new FeatureOffRun(steps).run(expectRefusal)).rejects.toThrow('no downgrade')
    expect(calls).toEqual(ORDER)
    expect(steps.attempt).toHaveBeenCalledTimes(1)
    expect(steps.restore).toHaveBeenCalledWith(1)
  })

  it('fails and restores when connect resolves instead of refusing', async () => {
    const { steps, calls } = harness({ attempt: { error: null } })
    await expect(new FeatureOffRun(steps).run(expectRefusal)).rejects.toThrow('did not refuse')
    expect(calls).toEqual(ORDER)
    expect(steps.restore).toHaveBeenCalledTimes(1)
  })

  it('accepts the expected refusal that connect throws', async () => {
    const { steps } = harness()
    const state = await new FeatureOffRun(steps).run(expectRefusal)
    expect(state.attempt).toBe(REFUSED)
    expect(steps.restore).toHaveBeenCalledTimes(1)
  })

  it('reports a failed restoration', async () => {
    const { steps, calls } = harness({
      restore: () => {
        throw new Error('no restore')
      },
    })
    await expect(new FeatureOffRun(steps).run(expectRefusal)).rejects.toThrow('no restore')
    expect(calls).toEqual(ORDER)
    expect(steps.attempt).toHaveBeenCalledTimes(1)
  })

  it('keeps the primary failure and attaches a failed restoration to it', async () => {
    const { steps } = harness({
      attempt: { error: null },
      restore: () => {
        throw new Error('no restore')
      },
    })
    const failure = await new FeatureOffRun(steps).run(expectRefusal).catch((error) => error)
    expect(failure.message).toBe('did not refuse')
    expect(failure.cleanupErrors.map(({ message }) => message)).toEqual(['no restore'])
    expect(steps.restore).toHaveBeenCalledTimes(1)
  })
})

enabled('share capability on a live broker', () => {
  beforeAll(
    () =>
      bounded(NEVER, HOOK_MS - 2000, () => {
        liveRun.assertClean()
      }),
    HOOK_MS
  )

  it(
    TITLE_DEFAULT,
    liveCase(DEFAULT_BUDGET_MS, (scope) => new CapabilityCase().default(scope)),
    caseTimeout(DEFAULT_BUDGET_MS)
  )

  if (LINE === '4.3.1') {
    it(
      TITLE_DISABLED,
      liveCase(DISABLED_BUDGET_MS, (scope) => new CapabilityCase().disabled(scope)),
      caseTimeout(DISABLED_BUDGET_MS)
    )
  }

  afterEach(() => {
    if (!liveRun.takeAdmission()) return
    liveRun.assertClean()
  })
})
