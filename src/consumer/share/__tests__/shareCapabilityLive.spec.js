// eslint-disable-next-line n/no-unsupported-features/node-builtins -- Test-only timer allocation diagnostics; the hook is disabled after cleanup.
const { createHook } = require('async_hooks')
const fs = require('fs')
const net = require('net')
const path = require('path')
const { isDeepStrictEqual } = require('util')
const {
  Kafka,
  logLevel,
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
  captureShareCleanupBaseline,
  waitForShareFixtureAbsence,
  setShareGroupConfigs,
  resetShareGroupConfigs,
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
const RequestQueue = require('../../../network/requestQueue')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { ShareAckLedger } = require('../shareAckLedger')
const { ShareRunner } = require('../shareRunner')
const {
  SHARE_VERSION,
  featuresOf,
  readApiVersions,
} = require('../../../../scripts/e2e/apiVersionsProbe')
const {
  CASE_JOIN_MS,
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
const DISABLED_BUDGET_MS = 18000
const FILE_BUDGET_MS = 80000
const FINAL_MS = 10000
let fileDeadline = null
const HOOK_MS = 10000
const CLEANUP_MS = 10000
const OLD_TIMESTAMP = '1700000000000'
const SHARE_API_KEYS = [76, 78, 79]
const CLIENT_CONFIG = { acquireMode: 'record_limit', maxRecords: 1, maxWaitTimeInMs: 250 }
const GROUP_CONFIG = { 'share.auto.offset.reset': 'earliest' }
const FEATURE_TABLE = path.join(__dirname, '../../../../scripts/protocol/brokerFeatures.json')
const FEATURES_TOOL = ['/opt/kafka/bin/kafka-features.sh', '--bootstrap-server', 'kafka1:29092']

/**
 * Limits every selected phase to the original file deadline, including its remaining reserves.
 * @param {number} limit The phase limit in milliseconds.
 * @param {number} [reserve] Time retained for joins and later phases.
 * @returns {number} The phase's remaining allowance.
 * @throws {Error} When the selection has no time left outside its reserves.
 */
const capabilityBudget = (limit, reserve = CASE_JOIN_MS) => {
  const remaining = fileDeadline - performance.now() - reserve
  if (!(remaining > 0)) {
    const error = new Error('The capability file deadline has expired')
    liveRun.taint(error)
    throw error
  }
  return Math.min(limit, remaining)
}

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
 * Logs every request frame the clients of one case write and counts their original sockets.
 * Every byte passes through untouched.
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
    this.entries.push({ socket })
    return socket
  }

  /** @returns {{ created: number, destroyed: number }} sockets opened and since destroyed */
  counts() {
    return {
      created: this.entries.length,
      destroyed: this.entries.filter(({ socket }) => socket.destroyed).length,
    }
  }

  /** @returns {object[]} every share request frame written, with its header facts */
  shareFrames() {
    return this.log.frames
      .filter(({ apiKey }) => SHARE_API_KEYS.includes(apiKey))
      .map(({ apiKey, apiVersion, correlationId, connection, at, endpoint }) => ({
        apiKey,
        version: apiVersion,
        correlationId,
        socketId: connection,
        writtenAt: at,
        endpoint,
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
 * Preserves every aggregate or restoration error without recursing through repeated causes.
 * @param {Error} error The original failure.
 * @param {Set<Error>} [seen] Ancestors already described in this error tree.
 * @returns {object|null} The error and its nested cleanup failures.
 */
const cleanupError = (error, seen = new Set()) => {
  if (error == null) return null
  const detail = {
    ...describeError(error),
    message: String(error.message),
    step: error.step,
    elapsedMs: error.elapsedMs,
  }
  if (seen.has(error)) return detail
  const ancestors = new Set([...seen, error])
  return {
    ...detail,
    cause: error.cause ? cleanupError(error.cause, ancestors) : undefined,
    errors: error.errors?.map((failure) => cleanupError(failure, ancestors)),
    cleanupErrors: error.cleanupErrors?.map((failure) => cleanupError(failure, ancestors)),
  }
}

/** Runs and joins every cleanup inside the case and file deadlines. */
class FileCleanupStack extends CleanupStack {
  /** @param {CapabilityCase} owner The case owning these cleanup operations. */
  constructor(owner) {
    super()
    this.owner = owner
  }

  /**
   * Joins cleanup without overwriting an earlier restoration failure on the primary error.
   * @param {Function} work The case workload.
   * @returns {Promise<*>} The workload result after all cleanup has settled.
   */
  async run(work) {
    let result
    let primary
    try {
      result = await work()
    } catch (error) {
      primary = error
    }
    const failures = await this.closeAll()
    if (primary) {
      primary.cleanupErrors = [...new Set([...(primary.cleanupErrors || []), ...failures])].filter(
        (error) => error !== primary
      )
      throw primary
    }
    if (failures.length) throw new AggregateError(failures, 'Cleanup failed')
    return result
  }

  /** @returns {Promise<Error[]>} Every failed cleanup, with its step and elapsed time retained. */
  async closeAll() {
    const deadline = Math.min(
      this.owner.deadline,
      fileDeadline - FINAL_MS - CASE_JOIN_MS,
      performance.now() + CLEANUP_MS
    )
    const failures = []
    for (const { name, close } of this.cleanups.splice(0).reverse()) {
      let work
      let stopping
      const startedAt = performance.now()
      try {
        await bounded(NEVER, Math.max(0, deadline - performance.now()), async (signal) => {
          const expired = performance.now() >= deadline
          const scope = AbortSignal.any([
            signal,
            this.owner.scope,
            ...(expired ? [AbortSignal.abort(new Error('The cleanup deadline has expired'))] : []),
          ])
          const stop = () => {
            stopping = Promise.allSettled([this.owner.abortClients()])
          }
          scope.addEventListener('abort', stop, { once: true })
          if (scope.aborted) stop()
          work = this.owner.cleanupStep(name, () => close(scope))
          try {
            return await work
          } finally {
            scope.removeEventListener('abort', stop)
          }
        })
      } catch (error) {
        error.step ??= name
        error.elapsedMs ??= performance.now() - startedAt
        liveRun.taint(error)
        failures.push(error)
      } finally {
        const [result] = await Promise.allSettled([work])
        const joined = [result, ...((await stopping) || [])]
        for (const result of joined) {
          if (result.status === 'rejected' && !failures.includes(result.reason)) {
            liveRun.taint(result.reason)
            failures.push(result.reason)
          }
        }
      }
    }
    return failures
  }
}

/**
 * Keeps the case scope active through joined cleanup, reserving the existing join allowance.
 * @param {AbortSignal} scope The enclosing live-case scope.
 * @param {number} budgetMs The unchanged case budget.
 * @param {'default'|'disabled'} method The selected capability arm.
 * @returns {Promise<void>} Once the arm and all aborted client work have settled.
 */
const runCapabilityCase = async (scope, budgetMs, method) => {
  const state = new CapabilityCase()
  const controller = new AbortController()
  const allowance = capabilityBudget(budgetMs, CLEANUP_MS + FINAL_MS + CASE_JOIN_MS)
  state.deadline = performance.now() + allowance - CASE_JOIN_MS
  state.scope = AbortSignal.any([scope, controller.signal])
  state.gauge.install()
  const timer = setTimeout(
    () =>
      controller.abort(new Error('The capability case exhausted its work and cleanup allowance')),
    Math.max(0, state.deadline - performance.now())
  )
  state.gauge.timerLabels.set(timer, 'capability case deadline')
  let stopping
  const stop = () => {
    liveRun.taint(state.scope.reason)
    stopping = Promise.allSettled([state.abortClients()])
  }
  state.scope.addEventListener('abort', stop, { once: true })
  let failure
  try {
    await state[method](state.scope)
  } catch (error) {
    failure = error
  }
  try {
    const joined = await Promise.allSettled([state.sentinelReady, state.verification])
    const stopped = (await stopping) || []
    const failures = [...joined, ...stopped]
      .filter(({ status }) => status === 'rejected')
      .map(({ reason }) => reason)
    if (failures.length) {
      liveRun.taint(failures[0])
      if (!failure) failure = new AggregateError(failures, 'Case join failed')
      else
        failure.cleanupErrors = [
          ...new Set([
            ...(failure.cleanupErrors || []),
            ...failures.filter((error) => error !== failure),
          ]),
        ]
    }
  } finally {
    clearTimeout(timer)
    state.scope.removeEventListener('abort', stop)
  }
  if (failure) {
    state.row('case-failed', { error: cleanupError(failure), ...state.progress() })
    throw failure
  }
}

/** Observes original resource owners without changing method results or receiver identities. */
class ResourceGauge {
  /** Retains owners until after the cleanup comparison, including destroyed connections. */
  constructor() {
    this.ledgers = new Set()
    this.queues = new Set()
    this.emitters = new Set()
    this.restores = []
    this.emitterOrigins = new Map()
    this.timers = new Map()
    this.timerLabels = new WeakMap()
    this.runs = new Set()
  }

  /** @returns {void} Installs call-through observers before clients are constructed. */
  install() {
    if (this.timerHook) return
    this.timerHook = createHook({
      init: (id, type, trigger, resource) => {
        if (type === 'Timeout') {
          this.timers.set(id, {
            handle: new WeakRef(resource),
            createdAt: performance.now(),
            stack: new Error().stack,
          })
        }
      },
      destroy: (id) => this.timers.delete(id),
    }).enable()
    const original = ShareRunner.prototype.newRun
    const gauge = this
    ShareRunner.prototype.newRun = function (...args) {
      const run = original.apply(this, args)
      gauge.runs.add(run)
      return run
    }
    this.restores.push(() => {
      ShareRunner.prototype.newRun = original
    })
    this.watch(
      ShareAckLedger.prototype,
      [
        'canStart',
        'decide',
        'complete',
        'completeRange',
        'dropExpired',
        'needsFlush',
        'prepare',
        'settle',
        'invalidate',
      ],
      this.ledgers
    )
    this.watch(
      RequestQueue.prototype,
      ['push', 'scheduleRequestTimeoutCheck', 'waitForPendingRequests', 'destroy'],
      this.queues
    )
    this.watch(InstrumentationEventEmitter.prototype, ['addListener', 'emit'], this.emitters)
  }

  /**
   * @param {object} prototype Original class prototype.
   * @param {string[]} names Methods to observe.
   * @param {Set<object>} seen Original receivers retained for measurement.
   * @returns {void}
   */
  watch(prototype, names, seen) {
    for (const name of names) {
      const original = prototype[name]
      const gauge = this
      prototype[name] = function (...args) {
        if (prototype === InstrumentationEventEmitter.prototype && !seen.has(this)) {
          gauge.emitterOrigins.set(this, new Error().stack)
        }
        seen.add(this)
        return original.apply(this, args)
      }
      this.restores.push(() => {
        prototype[name] = original
      })
    }
  }

  /** @returns {void} Restores observers only after the final resource proof. */
  uninstall() {
    this.timerHook?.disable()
    this.timerHook = null
    this.timers.clear()
    this.runs.clear()
    this.emitterOrigins.clear()
    for (const restore of this.restores.splice(0).reverse()) restore()
    this.ledgers.clear()
    this.queues.clear()
    this.emitters.clear()
  }

  /** @returns {number} Entries still held by original ledgers, including retired acquisitions. */
  ledgerSize() {
    return [...this.ledgers].reduce((total, ledger) => total + ledger.size, 0)
  }

  /** @returns {number} Pending and in-flight requests, regardless of socket destruction. */
  queueDepth() {
    return [...this.queues].reduce(
      (total, queue) => total + queue.pending.length + queue.inflight.size,
      0
    )
  }

  /**
   * @param {Iterable<import('events').EventEmitter>} emitters Original client or queue emitters.
   * @returns {number} Listeners over every event of every retained emitter.
   */
  listenerCount(emitters) {
    let total = 0
    for (const emitter of emitters)
      for (const event of emitter.eventNames()) total += emitter.listenerCount(event)
    return total
  }

  /**
   * Attributes retained listeners and referenced timers to their original allocations.
   * @returns {object} Owner names and creation stacks, alongside the unchanged count oracle.
   */
  owners() {
    return {
      clients: [...this.emitters].flatMap((owner, index) => {
        const events = owner.emitter.eventNames().map((event) => ({
          event: String(event),
          listeners: owner.emitter.rawListeners(event).map((listener) => listener.name),
        }))
        return events.length
          ? [
              {
                owner: `emitter-${index}`,
                clientIds: [
                  ...new Set(
                    [...this.queues]
                      .filter((queue) => queue.instrumentationEmitter === owner)
                      .map((queue) => queue.clientId)
                  ),
                ],
                origin: this.emitterOrigins.get(owner),
                events,
              },
            ]
          : []
      }),
      queues: [...this.queues].map((queue) => ({
        clientId: queue.clientId,
        broker: queue.broker,
        pending: queue.pending.length,
        inflight: queue.inflight.size,
        listeners: queue.eventNames().map((event) => ({
          event: String(event),
          count: queue.listenerCount(event),
        })),
      })),
      timers: [...this.timers].flatMap(([id, { handle, ...origin }]) => {
        const timer = handle.deref()
        return timer && !timer._destroyed && timer.hasRef()
          ? [
              {
                id,
                owner: this.timerOwner(timer),
                callback: timer._onTimeout?.name,
                delayMs: timer._idleTimeout,
                ...origin,
              },
            ]
          : []
      }),
    }
  }

  /**
   * Names original timer fields without changing or clearing the handles they hold.
   * @param {object} timer The original Timeout resource.
   * @returns {string} Known object and field, or the allocation stack for other owners.
   */
  timerOwner(timer) {
    const label = this.timerLabels.get(timer)
    if (label) return label
    for (const queue of this.queues) {
      for (const field of ['requestTimeoutIntervalId', 'throttleCheckTimeoutId']) {
        if (queue[field] === timer) return `${queue.clientId} ${queue.broker} RequestQueue.${field}`
      }
    }
    for (const run of this.runs) {
      if (run.manager.handle === timer) return `${run.options.groupId} HeartbeatManager.handle`
      if (run.operation.timer === timer) return `${run.options.groupId} ShareRun.operation.timer`
      for (const [id, node] of run.nodes) {
        for (const field of ['operation', 'pacing']) {
          if (node[field]?.timer === timer)
            return `${run.options.groupId} node ${id} ${field}.timer`
        }
      }
    }
    return 'allocation stack'
  }

  /**
   * @param {object} sources Socket, process-listener and event-loop measurements.
   * @returns {{ snapshot: () => Promise<object> }} Probe with fixed keys and measured owner totals.
   */
  probe(sources) {
    const probe = createResourceProbe({
      ...sources,
      queueDepth: () => this.queueDepth(),
      ledgerSize: () => this.ledgerSize(),
    })
    return {
      snapshot: async () => {
        const snapshot = await probe.snapshot()
        return {
          ...snapshot,
          owners: this.owners(),
          listeners: {
            ...snapshot.listeners,
            clients: this.listenerCount([...this.emitters].map(({ emitter }) => emitter)),
            queues: this.listenerCount(this.queues),
          },
        }
      },
    }
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
    this.startedAt = performance.now()
    this.nonce = secureRandom(6)
    this.line = LINE
    this.topic = shareTopicName('capability')
    this.group = `e2e-p9e-capability-group-${this.nonce}`
    this.sentinel = `e2e-p9e-capability-sentinel-${this.nonce}`
    this.title = null
    this.budgetMs = null
    this.stack = new FileCleanupStack(this)
    this.cleanupSteps = []
    this.ledger = new SocketLedger()
    this.acknowledgements = new ShareAcknowledgementObserver()
    this.gauge = new ResourceGauge()
    this.probe = this.gauge.probe({
      emitters: { process },
      sockets: () => this.ledger.counts(),
      activeResources: true,
    })
    this.requests = []
    this.crashes = []
    this.acks = []
    this.entries = []
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
   * Records the duration and original error of one cleanup operation, including nested steps.
   * @param {string} step The operation name.
   * @param {Function} work The operation to run and join.
   * @returns {Promise<*>} The original result.
   */
  async cleanupStep(step, work) {
    const startedAt = performance.now()
    const entry = { step, elapsedMs: null, error: null }
    this.cleanupSteps.push(entry)
    try {
      return await work()
    } catch (error) {
      error.step ??= step
      error.elapsedMs ??= performance.now() - startedAt
      entry.error = cleanupError(error)
      throw error
    } finally {
      entry.elapsedMs = performance.now() - startedAt
      this.row('cleanup-step', entry)
    }
  }

  /**
   * Captures a client's first close deadline inside the remaining case allowance.
   * @param {object} client The client to disconnect.
   * @param {number} limit The existing close allowance in milliseconds.
   * @returns {Promise<void>} The original disconnect result.
   */
  disconnect(client, limit) {
    const timeout = Math.max(0, Math.floor(Math.min(limit, this.deadline - performance.now())))
    return client.disconnect({ timeout })
  }

  /**
   * Fences pending admin calls and closes all case transports when their scope expires.
   * @returns {Promise<void>} Once every forced client close has settled.
   */
  abortClients() {
    this.stopping ??= (async () => {
      for (const { socket } of this.ledger.entries) socket.destroy()
      const results = await Promise.allSettled(
        [
          ['admin', this.admin],
          ['consumer', this.consumer],
          ['producer', this.producer],
          ['sentinel', this.sentinelConsumer],
          ['verifier', this.verifier],
        ]
          .filter(([, client]) => client)
          .map(([name, client]) =>
            this.cleanupStep(`abort ${name}`, () => client.disconnect({ timeout: 0 }))
          )
      )
      const failures = results
        .filter(({ status }) => status === 'rejected')
        .map(({ reason }) => reason)
      if (failures.length) throw new AggregateError(failures, 'Client abort failed')
    })()
    return this.stopping
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
      caseElapsedMs: performance.now() - this.startedAt,
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
      cleanupSteps: this.cleanupSteps,
      resourceOwners: this.resourceOwners ?? this.gauge.owners(),
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
    this.stack.add(`disconnect ${label}`, () => this.disconnect(consumer, 2000))
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
    this.gauge.install()
    this.acknowledgements.install()
    try {
      const expected = expectedOutcome(this.line)
      if (!expected) throw new Error(`No share capability expectation for broker line ${this.line}`)
      if (expected.connected) await this.roundTrip(scope, expected)
      else await this.refused(scope, expected)
    } finally {
      this.acknowledgements.restore()
      this.resourceOwners = this.gauge.owners()
      this.gauge.uninstall()
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
    this.gauge.install()
    try {
      this.baseline = await captureShareCleanupBaseline(this.probe, {
        signal: scope,
        cleanupStack: false,
      })
      this.row('resource-baseline', { snapshot: this.baseline })
      const run = new FeatureOffRun({
        capture: () => this.captureOriginal(scope),
        downgrade: () => bounded(scope, Infinity, () => this.switchOff()),
        observeOff: () => this.observeOff(scope),
        attempt: () => this.attempt(scope),
        restore: (original) =>
          bounded(
            NEVER,
            Math.max(
              0,
              Math.min(
                this.deadline - performance.now(),
                capabilityBudget(CLEANUP_MS, CLEANUP_MS + FINAL_MS + CASE_JOIN_MS)
              )
            ),
            (signal) => this.restore(original, AbortSignal.any([signal, scope]))
          ),
      })
      let state
      await this.stack.run(async () => {
        state = await run.run((outcome) => this.checkRefusal(outcome, FEATURE_DISABLED))
      })
      await this.settleResources(scope)
      this.complete({
        outcome: this.outcomeOf(state.attempt.error),
        capture: this.feature.before,
        featureChange: this.feature.change,
        featureRestore: this.feature.restore,
        featureCommands: this.feature.transcripts,
        order: run.order,
      })
    } finally {
      this.resourceOwners = this.gauge.owners()
      this.gauge.uninstall()
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
    this.baseline = await captureShareCleanupBaseline(this.probe, {
      signal: scope,
      cleanupStack: false,
    })
    this.row('resource-baseline', { snapshot: this.baseline })
    const capture = await this.capture(scope)
    let attempt
    await this.stack.run(async () => {
      attempt = await this.attempt(scope)
      this.checkRefusal({ attempt, retained: {} }, expected)
    })
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
    liveRun.assertClean()
    capabilityBudget(Infinity)
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
      capabilityBudget(Infinity)
      entry.output = String(execInKafka1(argv)).slice(0, 2000)
      capabilityBudget(Infinity)
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
   * @param {AbortSignal} scope The restoration scope, bounded by the file deadline.
   * @returns {Promise<void>}
   * @throws {Error} when the level is unknown, the tool fails, the level is not read back or the
   *   control connection is refused
   */
  async restore(original, scope) {
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
    const observed = await this.capture(scope)
    scope.throwIfAborted()
    if (observed.shareVersion !== original) {
      this.command(original >= 1 ? featureArgv('upgrade', original) : featureArgv('downgrade', 0))
    }
    await pollTolerating(
      scope,
      20000,
      async (signal) => {
        restore.observed = (await this.capture(signal)).shareVersion
        return restore.observed === original
      },
      { delayMs: 250, label: 'The share.version restoration' }
    )
    if (original >= 1) {
      const control = this.client('control').shareConsumer({
        groupId: this.group,
        ...CLIENT_CONFIG,
      })
      this.stack.add('disconnect feature control', () => this.disconnect(control, 2000))
      try {
        await bounded(scope, 10000, () => control.connect())
        restore.control = 'connected'
      } finally {
        await bounded(scope, 5000, () => this.disconnect(control, 2000))
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
    await this.stack.run(async () => {
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
    })
    await this.completeConnected(scope, expected)
  }

  /**
   * Measures the baseline, creates the topic and the group config, and connects the admin and the
   * producer. An unrelated group is left in place for the cleanup proof to preserve.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   */
  async setup(scope) {
    this.beforeAdmin = await captureShareCleanupBaseline(this.probe, { signal: scope })
    this.baseline = this.beforeAdmin
    this.admin = this.client('admin').admin()
    this.stack.add('prove cleanup and disconnect admin', (signal) => this.cleanup(signal))
    this.stack.add(
      'remove admin listeners',
      this.admin.on(this.admin.events.REQUEST, ({ payload }) => this.requests.push(payload))
    )
    this.stack.add('delete owned fixtures', (signal) => this.deleteFixtures(signal))
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
    this.startSentinel(scope)
    await bounded(scope, 15000, () => setShareGroupConfigs(this.admin, this.group, GROUP_CONFIG))
    this.configReadAt = performance.now()
    this.row('config-readback', { group: GROUP_CONFIG, client: CLIENT_CONFIG })
    this.row('resource-baseline', { snapshot: this.baseline })

    this.producer = this.client('producer').producer({ idempotent: true })
    this.stack.add(
      'remove producer listeners',
      this.producer.on(this.producer.events.REQUEST, ({ payload }) => this.requests.push(payload))
    )
    this.stack.add('disconnect producer', () => this.disconnect(this.producer, CLEANUP_MS))
    await bounded(scope, 15000, () => this.producer.connect())
  }

  /**
   * Starts the unrelated group alongside the workload and joins its creation before cleanup proof.
   * The paused sentinel member need not delay the share member's first join.
   * @param {AbortSignal} scope The case scope, including the sentinel's original creation bound.
   * @returns {void}
   */
  startSentinel(scope) {
    this.stack.add('join sentinel creation', (signal) =>
      bounded(signal, Infinity, () => this.sentinelReady)
    )
    this.sentinelCreated = true
    const startedAt = performance.now()
    this.sentinelReady = bounded(scope, 10000, (signal) => this.createSentinel(signal)).then(() =>
      this.row('sentinel-created', { elapsedMs: performance.now() - startedAt })
    )
    this.sentinelReady.catch((error) =>
      this.row('sentinel-failed', {
        elapsedMs: performance.now() - startedAt,
        error: cleanupError(error),
      })
    )
  }

  /**
   * Creates the unrelated committed group through a paused member whose listeners we own.
   * @param {AbortSignal} signal The original sentinel creation scope.
   * @returns {Promise<void>} Once the offset is committed and the temporary member is closed.
   */
  async createSentinel(signal) {
    const consumer = this.client('sentinel').consumer({ groupId: this.sentinel })
    this.sentinelConsumer = consumer
    let joined = false
    // pause() needs the group that run() creates; GROUP_JOIN fires before the first fetch.
    const remove = consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
      consumer.pause([{ topic: this.topic }])
      joined = true
      this.row('sentinel-joined', { memberId: payload.memberId })
    })
    let stopping
    const abort = () => {
      stopping = consumer.disconnect({ timeout: 0 })
      stopping.catch(() => {})
    }
    signal.addEventListener('abort', abort, { once: true })
    try {
      signal.throwIfAborted()
      await bounded(signal, Infinity, () => consumer.connect())
      await bounded(signal, Infinity, () => consumer.subscribe({ topics: [this.topic] }))
      await bounded(signal, Infinity, () =>
        consumer.run({
          eachBatchAutoResolve: false,
          eachBatch: async () => {
            throw new Error('The paused sentinel must not consume records')
          },
        })
      )
      if (!joined) throw new Error('The sentinel did not join its group')
      await bounded(signal, Infinity, () =>
        consumer.commitOffsets([{ topic: this.topic, partition: 0, offset: '0' }])
      )
    } finally {
      try {
        await consumer.disconnect({ timeout: signal.aborted ? 0 : 2000 })
        await stopping
      } finally {
        signal.removeEventListener('abort', abort)
        remove()
      }
    }
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
          this.entries.push({
            identity: shareRecordIdentity({ ...message, topic, partition }),
            deliveryCount: message.deliveryCount,
          })
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
    await this.cleanupStep('disconnect workload before verifier', () =>
      bounded(scope, 10000, () => this.disconnect(this.consumer, 5000))
    )
    expect(this.disconnects).toBe(1)
    this.verification = assertQuietTerminalGroup({
      kafka: this.client('verifier'),
      admin: this.admin,
      groupId: this.group,
      topic: this.topic,
      partitions: this.terminalPartitions,
      signal: scope,
      timeoutMs: 30000,
      record: (fields) => this.row('quiet-verifier', fields),
      onClient: (client) => {
        this.verifier = client
      },
    })
    this.verification.catch(() => {})
    this.quiet = await this.verification
    this.broker = this.quiet.position.rows
    expect(this.quiet.delivered).toEqual([])
    expect(this.entries.map(({ identity }) => identity)).toEqual([this.expected])
    expect(this.successfulAccepts()).toBe(true)
    expect(this.crashes.map((error) => error.message)).toEqual([])
  }

  /**
   * Proves owned fixtures absent while preserving the sentinel, then removes it before one
   * client-free resource comparison. Topic absence on every broker is proved only once.
   * @param {AbortSignal} signal The cleanup scope.
   * @returns {Promise<void>} Once all proofs and the final admin close have settled.
   */
  async cleanup(signal) {
    const failures = []
    const steps = [
      [
        'prove owned fixture absence',
        () =>
          waitForShareFixtureAbsence(this.admin, [this.group], [this.topic], {
            signal,
            unrelated: this.sentinelCreated ? [this.sentinel] : [],
            delayMs: 50,
            record: (event, fields) => this.row(event, fields),
          }),
      ],
      [
        'delete sentinel',
        async () => {
          if (this.sentinelCreated) await this.deleteGroup(signal, this.sentinel)
        },
      ],
      [
        'prove final absence and resources',
        async () => {
          if (this.baseline) {
            this.after = await assertShareCleanup(
              this.admin,
              [this.group, ...(this.sentinelCreated ? [this.sentinel] : [])],
              [],
              this.baseline,
              this.probe,
              { signal, disconnectAdmin: true, record: (event, fields) => this.row(event, fields) }
            )
          }
        },
      ],
      ['disconnect admin', () => this.admin.disconnect({ timeout: 0 })],
    ]
    for (const [step, work] of steps) {
      try {
        await this.cleanupStep(step, work)
      } catch (error) {
        failures.push(error)
      }
    }
    if (failures.length) throw new AggregateError(failures, 'Cleanup proof failed')
  }

  /**
   * Overlaps independent group and topic removal, retaining both outcomes before the proof.
   * @param {AbortSignal} signal The cleanup scope.
   * @returns {Promise<void>} Once both deletion operations have settled.
   */
  async deleteFixtures(signal) {
    const results = await Promise.allSettled([
      this.cleanupStep('delete group', () => this.deleteGroup(signal, this.group)),
      this.cleanupStep('delete topic', () => this.deleteTopic(signal)),
    ])
    const failures = results
      .filter(({ status }) => status === 'rejected')
      .map(({ reason }) => reason)
    if (failures.length) throw new AggregateError(failures, 'Fixture deletion failed')
  }

  /**
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   * @throws {Error} When topic deletion is refused; the shared proof checks absence.
   */
  async deleteTopic(signal) {
    await bounded(signal, 10000, () =>
      this.admin.deleteTopics({ topics: [this.topic], timeout: 8000 })
    ).catch((error) => {
      if (error.type !== 'UNKNOWN_TOPIC_OR_PARTITION') throw error
      this.row('topic-already-absent', { error: cleanupError(error) })
    })
  }

  /**
   * @param {AbortSignal} signal the cleanup scope
   * @param {string} groupId the group to remove
   * @returns {Promise<void>}
   * @throws {Error} When group deletion is refused; the shared proof checks absence.
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
      { delayMs: 50, label: 'The group deletion' }
    )
  }

  /**
   * Removes the owned group config overrides and proves none remain.
   * @param {AbortSignal} signal the cleanup scope
   * @returns {Promise<void>}
   * @throws {Error} when the broker refuses or an override remains
   */
  async resetConfigs(signal) {
    await resetShareGroupConfigs(this.admin, this.group, Object.keys(GROUP_CONFIG), { signal })
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
    liveRun.assertClean()
    capabilityBudget(Infinity)
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

describe('capability sentinel ownership', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => {
    jest.useRealTimers()
    liveRun.reset()
  })

  it.each([false, true])(
    'joins the concurrent creation and retains its failure: %s',
    async (fails) => {
      let resolve
      let reject
      const creation = new Promise((yes, no) => {
        resolve = yes
        reject = no
      })
      const state = new CapabilityCase()
      const cleanup = new CleanupStack()
      state.stack = cleanup
      state.createSentinel = () => creation
      state.row = jest.fn()
      state.startSentinel(NEVER)
      let settled = false
      const pending = cleanup
        .run(async () => {}, { budgetMs: 10000 })
        .then(
          () => {
            settled = true
          },
          (error) => {
            settled = true
            return error
          }
        )
      await jest.advanceTimersByTimeAsync(100)
      expect(settled).toBe(false)
      const failure = new Error('sentinel refused')
      if (fails) reject(failure)
      else resolve()
      await jest.advanceTimersByTimeAsync(0)
      if (fails) expect((await pending).errors.map(({ cause }) => cause)).toContain(failure)
      else await expect(pending).resolves.toBeUndefined()
      expect(jest.getTimerCount()).toBe(0)
    }
  )
})

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
  beforeAll(() => {
    fileDeadline = performance.now() + FILE_BUDGET_MS
    return bounded(NEVER, capabilityBudget(HOOK_MS - CASE_JOIN_MS), () => {
      liveRun.assertClean()
    })
  }, HOOK_MS)

  it(
    TITLE_DEFAULT,
    liveCase(DEFAULT_BUDGET_MS, (scope) => runCapabilityCase(scope, DEFAULT_BUDGET_MS, 'default')),
    caseTimeout(DEFAULT_BUDGET_MS)
  )

  if (LINE === '4.3.1') {
    it(
      TITLE_DISABLED,
      liveCase(DISABLED_BUDGET_MS, (scope) =>
        runCapabilityCase(scope, DISABLED_BUDGET_MS, 'disabled')
      ),
      caseTimeout(DISABLED_BUDGET_MS)
    )
  }

  afterEach(() => {
    if (!liveRun.takeAdmission()) return
    liveRun.assertClean()
  })
})
