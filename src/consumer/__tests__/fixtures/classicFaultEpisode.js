// eslint-disable-next-line n/no-unsupported-features/node-builtins -- test-only failure diagnostics
const { createHook } = require('node:async_hooks')
const { performance } = require('node:perf_hooks')
const { Kafka, logLevel } = require('../../../../index')
const { createScriptedCluster, groupScript } = require('../../../../testHelpers/scriptedCluster')
const { createResourceProbe } = require('../../../../testHelpers/resourceProbe')
const { RecordBatch } = require('../../../protocol/recordBatch/v0')
const RecordEncoder = require('../../../protocol/recordBatch/record/v0')
const { TOPIC } = require('./fetchHarness')

const NODES = [0, 1, 2].map((id) => ({ id, host: `node-${id}`, port: 9092 }))
const COORDINATOR = 1
const PARTITIONS = [0, 1, 2]
const RECORDS_PER_PARTITION = 3
const MEMBER_ID = 'member-1'
const MEMBER_ID_REQUIRED = 79
const WAIT_MS = 12000
const QUIET_MS = 250
const BACKOFF_FLOOR_MS = 50
const REQUEST_TIMEOUT_MS = 300
const ROUND_TIMEOUT_MS = 400
const REBALANCE_TIMEOUT_MS = 30000
const CLOSE_TIMEOUT_MS = 400
const DATA_PLANE = ['Fetch', 'ListOffsets', 'OffsetFetch', 'Metadata', 'ApiVersions']

/** @param {number} ms @returns {Promise<void>} resolves after the real delay */
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/** A frame inside Jest's own packages. */
const JEST_OWN = /[\\/]node_modules[\\/](@jest[\\/]|jest-)/

/**
 * @param {string} stack a creation stack taken inside the async hook
 * @returns {string} the first frame outside the hook, Node's internals and the timers module: the
 *   code that asked for the timer
 */
const callerOf = (stack) =>
  stack
    .split('\n')
    .slice(1)
    .find((frame) => !/AsyncHook\.init|\(node:/.test(frame)) ?? ''

/** Tracers still on; a test that hangs before `close` must not leave its hook on for the next. */
const tracers = new Set()
afterEach(() => {
  for (const tracer of [...tracers]) tracer.disable()
})

/**
 * Records where each timer created while tracing is on was made, until the timer ends. Only
 * timers created while the episode is traced are judged, so timers left elsewhere in a
 * long-lived Jest worker cannot fail it, and a leaked timer cannot hide behind one that ended.
 * A timer whose direct caller is Jest itself is not the episode's: run in band, Jest's reporter
 * shares the test process and debounces its status output with a 100 ms timer.
 * @returns {{ outliving: () => Promise<string[]>, disable: () => void }} for each traced timer
 *   still active, its delay, callback and creation stack; and a stop
 */
const traceTimers = () => {
  const live = new Map()
  const hook = createHook({
    init(id, type, triggerId, timer) {
      if (type !== 'Timeout') return
      const limit = Error.stackTraceLimit
      Error.stackTraceLimit = 40
      const stack = new Error('timer created').stack
      Error.stackTraceLimit = limit
      if (!JEST_OWN.test(callerOf(stack))) live.set(id, { timer, stack })
    },
    destroy(id) {
      live.delete(id)
    },
  }).enable()
  const tracer = {
    outliving: async () => {
      // Node reports a timer's end to `destroy` from a later check phase.
      await new Promise(setImmediate)
      await new Promise(setImmediate)
      // `@` instead of `at`, so Jest keeps the internal frames it would strip from a stack.
      return [...live.values()].map(
        ({ timer, stack }) =>
          `${timer._idleTimeout} ms, ${String(timer._onTimeout).slice(0, 300)}\n` +
          stack.replace(/^(\s+)at /gm, '$1@ ')
      )
    },
    disable: () => {
      hook.disable()
      tracers.delete(tracer)
    },
  }
  tracers.add(tracer)
  return tracer
}

/**
 * @param {() => boolean} predicate
 * @param {string} label what is awaited, for the failure
 * @param {number} [budget=WAIT_MS] the most the wait may last
 * @returns {Promise<void>} when the predicate holds
 * @throws {Error} naming `label` when it does not hold within the budget
 */
const until = async (predicate, label, budget = WAIT_MS) => {
  const deadline = Date.now() + budget
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`Did not happen in time: ${label}`)
    await pause(5)
  }
}

/**
 * One batch holding one record whose value names its partition and offset.
 * @param {number} partition
 * @param {number} offset
 * @returns {Promise<Buffer>} the encoded batch
 */
const batchAt = async (partition, offset) =>
  Buffer.from(
    (
      await RecordBatch({
        firstOffset: String(offset),
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        partitionLeaderEpoch: 5,
        records: [
          RecordEncoder({
            key: Buffer.from(`key-${partition}-${offset}`),
            value: Buffer.from(`value-${partition}-${offset}`),
          }),
        ],
      })
    ).buffer
  )

/**
 * The response fields that make a request's answer carry one error code, laid over the wire pair's
 * defaults.
 * @param {string} api the API name
 * @param {number} code the Kafka error code
 * @returns {object|((call: object) => object)} fields, or a function of the request when the code
 *   sits inside per-partition or per-member entries
 */
const answerWithCode = (api, code) => {
  if (api === 'OffsetCommit') {
    return ({ fields }) => ({
      topics: fields.topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({ partition, errorCode: code })),
      })),
    })
  }
  if (api === 'LeaveGroup' && (code === 25 || code === 22)) {
    return ({ fields }) => ({
      members: fields.members.map((member) => ({ ...member, errorCode: code })),
    })
  }
  return { errorCode: code }
}

/**
 * Drives one row of the independent classic coordinator fault table through the real consumer and
 * a scripted three-node cluster, and observes only the wire, the instrumentation events and the
 * handler. It is test support, not a replacement consumer: it holds no group state, and the
 * expectations it checks are the row's named contract, never a snapshot of what the client did.
 * The independent commit store is advanced only by OffsetCommit frames the cluster answers
 * without an error.
 *
 * Order of use: `open`, `execute`, `recover`, `verify`, always followed by `close`.
 */
class ClassicFaultEpisode {
  /**
   * @param {{api: string, fault: number|string, step: string, expectedState: string, expectedWire:
   *   object, maxRounds: number, config: object}} row one fixture row, kept as given; `config` may
   *   also carry a `requestTimeout` in milliseconds. The
   *   pseudo-fault `none` runs the healthy control and `late` the stale-answer schedules whose old
   *   answer is the healthy one.
   */
  constructor(row) {
    this.row = Object.freeze({ ...row })
    this.stale = row.step === 'old-generation-answer'
    this.cleanup = []
    this.trace = []
    this.deliveries = []
    this.joins = []
    this.crashes = []
    this.timeouts = []
    this.inFlight = new Set()
    this.overlaps = 0
    this.store = new Map()
    this.applied = []
    this.gate = null
    this.armed = false
    this.faulted = null
    this.released = null
    this.sentBeforeRelease = 0
    this.commitOutcome = null
    this.commitResult = null
    this.leaveOutcome = null
    this.leaveResult = null
    this.baseline = null
    this.cluster = null
    this.consumer = null
    this.probe = null
    this.timers = null
  }

  /**
   * Builds the scripted cluster and the real consumer and snapshots the resource baseline. Opens
   * no socket: nothing is dialled until `execute`.
   * @returns {Promise<void>}
   */
  async open() {
    const { api, fault } = this.row
    const batches = new Map()
    for (const partition of PARTITIONS)
      for (let offset = 0; offset < RECORDS_PER_PARTITION; offset++)
        batches.set(`${partition}|${offset}`, await batchAt(partition, offset))

    this.cluster = groupScript(createScriptedCluster({ nodes: NODES }), {
      partitions: PARTITIONS.length,
      memberId: MEMBER_ID,
      coordinator: COORDINATOR,
    })
      .answer('JoinGroup', (fields) => {
        if (fields.memberId === '')
          return { errorCode: MEMBER_ID_REQUIRED, memberId: MEMBER_ID, members: [] }
        const [protocol] = fields.groupProtocols
        return {
          protocolType: fields.protocolType,
          memberId: MEMBER_ID,
          generationId: 1,
          leaderId: MEMBER_ID,
          groupProtocol: protocol.name,
          members: [
            { memberId: MEMBER_ID, groupInstanceId: null, memberMetadata: protocol.metadata },
          ],
        }
      })
      .answer('Fetch', async ({ maxWaitMs, topics }) => {
        const entries = topics[0].partitions.map(({ partition, fetchOffset }) => ({
          partition,
          errorCode: 0,
          records: batches.get(`${partition}|${fetchOffset}`) ?? false,
          abortedTransactions: [],
        }))
        if (entries.every(({ records }) => records === false)) await pause(maxWaitMs)
        return { partitions: entries }
      })
      .answer('OffsetCommit', ({ topics, memberId, groupGenerationId }) => {
        for (const entry of topics)
          for (const { partition, offset } of entry.partitions) {
            this.store.set(`${entry.topic}|${partition}`, offset)
            this.applied.push({ partition, offset, memberId, generation: groupGenerationId })
          }
        return {
          topics: topics.map((entry) => ({
            topic: entry.topic,
            partitions: entry.partitions.map(({ partition }) => ({ partition, errorCode: 0 })),
          })),
        }
      })
    this.recordTrace()

    this.probe = createResourceProbe({
      sockets: () => ({
        created: this.cluster.sockets().length,
        destroyed: this.cluster.sockets().filter((socket) => socket.destroyed).length,
      }),
    })
    this.timers = traceTimers()
    this.baseline = await this.probe.snapshot()

    const { retry, groupProtocol, requestTimeout = REQUEST_TIMEOUT_MS } = this.row.config
    this.consumer = new Kafka(
      this.cluster.kafkaOptions({
        logLevel: logLevel.NOTHING,
        requestTimeout,
        enforceRequestTimeout: true,
        retry,
      })
    ).consumer({
      groupId: 'fault-group',
      groupProtocol,
      maxWaitTimeInMs: 20,
      heartbeatInterval: api === 'Heartbeat' ? 50 : 3000,
      rebalanceTimeout:
        (fault === 'timeout' || this.stale) && (api === 'JoinGroup' || api === 'SyncGroup')
          ? ROUND_TIMEOUT_MS
          : REBALANCE_TIMEOUT_MS,
    })
    const { events } = this.consumer
    const record =
      (list) =>
      ({ payload }) =>
        list.push({ ...payload, at: performance.now() })
    this.cleanup.push(
      this.consumer.on(events.GROUP_JOIN, record(this.joins)),
      this.consumer.on(events.CRASH, record(this.crashes)),
      this.consumer.on(events.REQUEST_TIMEOUT, record(this.timeouts))
    )
  }

  /**
   * Stamps every request the scripted cluster routes with a monotonic time, in an own ordered
   * trace the observations read.
   * @private
   * @returns {void}
   */
  recordTrace() {
    const { schedule } = this.cluster
    const run = schedule.run.bind(schedule)
    schedule.run = (call, fallback) => {
      this.trace.push({
        seq: this.trace.length,
        api: call.api,
        node: call.node,
        generation: call.generation,
        correlationId: call.correlationId,
        fields: call.fields,
        at: performance.now(),
      })
      return run(call, fallback)
    }
  }

  /**
   * @param {string} api the API name
   * @returns {object[]} the trace entries of that API, oldest first
   */
  frames(api) {
    return this.trace.filter((entry) => entry.api === api)
  }

  /** @returns {object[]} the group-control frames written after the faulted request */
  after() {
    return this.trace
      .slice((this.faulted?.seq ?? -1) + 1)
      .filter(({ api }) => !DATA_PLANE.includes(api))
  }

  /**
   * Notes which trace entry the one fault met.
   * @private
   * @param {object} call the routed call the schedule handed over
   * @returns {void}
   */
  mark(call) {
    this.faulted = this.trace.find(
      ({ api, node, correlationId, generation }) =>
        api === call.api &&
        node === call.node &&
        correlationId === call.correlationId &&
        generation === call.generation
    )
  }

  /**
   * Schedules exactly one request fault through the cluster's schedule. A numeric code patches
   * only its error slot of the wire pair's answer; `disconnect` destroys the connection the
   * request came in on; `timeout` holds the answer past the request timeout; a stale schedule
   * holds the answer until `recover`. The selector names the API and, for JoinGroup, the
   * follow-up that carries the assigned member id.
   * @returns {void}
   * @throws {Error} when a fault is armed twice
   */
  arm() {
    const { api, fault } = this.row
    if (fault === 'none') return
    if (this.armed) throw new Error('A fault is armed already')
    this.armed = true
    const selector = {
      api,
      match: (call) => api !== 'JoinGroup' || call.fields.memberId !== '',
    }
    const { schedule } = this.cluster
    if (fault === 'timeout' || this.stale) {
      this.gate = schedule.hold(selector)
      this.gate.arrived().then((call) => this.mark(call))
    } else if (fault === 'disconnect') {
      schedule.once(selector, (call) => {
        this.mark(call)
        return new Error('The scripted connection was cut')
      })
    } else {
      const answer = answerWithCode(api, fault)
      schedule.once(selector, (call) => {
        this.mark(call)
        return typeof answer === 'function' ? answer(call) : answer
      })
    }
  }

  /** @returns {boolean} whether every record was handed to the handler at least once */
  allDelivered() {
    const wanted = PARTITIONS.length * RECORDS_PER_PARTITION
    return new Set(this.deliveries.map(({ id }) => id)).size === wanted
  }

  /**
   * @private
   * @returns {Promise<void>} when the consumer has joined and the handler has seen every record
   */
  async stable() {
    await until(() => this.joins.length >= 1 && this.allDelivered(), 'a joined, delivering group')
  }

  /**
   * Subscribes and runs the real consumer with a handler that records `(topic, partition, value)`
   * identities and notes two handlers running for one partition at once.
   * @private
   * @returns {Promise<void>}
   */
  async start() {
    await this.consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        const key = `${topic}|${partition}`
        if (this.inFlight.has(key)) this.overlaps += 1
        this.inFlight.add(key)
        const value = message.value.toString()
        this.deliveries.push({ id: `${topic}|${partition}|${value}`, at: performance.now() })
        await pause(1)
        this.inFlight.delete(key)
      },
    })
  }

  /**
   * Connects and runs the real consumer, with the fault armed at the point the row names: before
   * startup for JoinGroup and SyncGroup, after an observed join and a delivered corpus for the
   * others. Then triggers the request the fault is armed on: a heartbeat on its own interval, an
   * explicit commit, or the disconnect that sends the leave. A stale row then retires the
   * membership and completes a replacement.
   * @returns {Promise<void>}
   * @throws {Error} when the setup fails or the armed request is never written
   */
  async execute() {
    const { api, fault } = this.row
    const early = api === 'JoinGroup' || api === 'SyncGroup'
    if (early) this.arm()
    await this.consumer.connect()
    if (early) {
      await this.start()
      if (fault !== 'none') await until(() => this.faulted !== null, `the faulted ${api}`)
      if (this.stale) {
        await this.stable()
        await this.quiet()
      }
      return
    }
    await this.start()
    await this.stable()
    this.arm()
    if (api === 'Heartbeat') {
      if (fault === 'none') await until(() => this.frames('Heartbeat').length >= 2, 'heartbeats')
    } else if (api === 'OffsetCommit') {
      this.commitOutcome = this.consumer
        .commitOffsets([{ topic: TOPIC, partition: 0, offset: '3' }])
        .then(
          () => ({ ok: true }),
          (error) => ({ ok: false, code: error.code, type: error.type, name: error.name })
        )
    } else {
      this.leaveOutcome = this.consumer
        .disconnect(this.stale ? { timeout: CLOSE_TIMEOUT_MS } : undefined)
        .then(
          () => ({ ok: true }),
          (error) => ({ ok: false, name: error.name, message: error.message })
        )
    }
    if (fault !== 'none') await until(() => this.faulted !== null, `the faulted ${api}`)
    if (this.stale) await this.replace()
  }

  /**
   * Retires the current membership and completes a replacement that takes the same scalar member
   * id and generation: a stop and a new run, or for a held leave a bounded close and a reconnect.
   * A held join or sync is retired by its round's own expiry, which restarts the run; the held
   * answer then meets the connection that expiry closed.
   * @private
   * @returns {Promise<void>} when the replacement group has joined and is delivering
   */
  async replace() {
    const joined = this.joins.length
    if (this.row.api === 'LeaveGroup') {
      this.leaveResult = await this.leaveOutcome
      await this.consumer.connect()
    } else {
      await this.consumer.stop()
    }
    this.deliveries.length = 0
    await this.start()
    await until(() => this.joins.length > joined, 'the replacement join')
    await this.stable()
    await this.quiet()
  }

  /**
   * @private
   * @returns {number} when the group last did anything worth waiting out: wrote a join, sync or
   *   lookup, joined, timed out or crashed, or met the fault
   */
  lastActivity() {
    const groupFrames = this.trace
      .filter(({ api }) => ['JoinGroup', 'SyncGroup', 'FindCoordinator'].includes(api))
      .map(({ at }) => at)
    return Math.max(
      this.faulted?.at ?? 0,
      ...groupFrames,
      ...this.joins.map(({ at }) => at),
      ...this.timeouts.map(({ at }) => at),
      ...this.crashes.map(({ at }) => at)
    )
  }

  /**
   * @private
   * @returns {Promise<void>} when the group did nothing for a quiet interval
   */
  async quiet() {
    await until(() => performance.now() - this.lastActivity() > QUIET_MS, 'a quiet wire')
  }

  /**
   * Releases the one fault and drives the real event loop until the group is settled again: a
   * timed-out request is seen to have closed its connection and its held answer is let go, a stale
   * answer is let go after its replacement completed, and the wire stays quiet while every record
   * has been delivered.
   * @returns {Promise<void>}
   * @throws {Error} when the group does not settle in time
   */
  async recover() {
    const { api, fault } = this.row
    if (this.stale) {
      this.sentBeforeRelease = this.trace.length
      const late = typeof fault === 'number' ? answerWithCode(api, fault) : undefined
      this.gate.release(typeof late === 'function' ? late(this.gate.call) : late)
      this.released = performance.now()
      await pause(QUIET_MS * 2)
      if (api === 'OffsetCommit') this.commitResult = await this.commitOutcome
      return
    }
    if (fault === 'timeout') {
      const closed =
        api === 'JoinGroup' || api === 'SyncGroup'
          ? () =>
              this.after().some(
                ({ node, generation }) =>
                  node === this.faulted.node && generation > this.faulted.generation
              )
          : () => this.timeouts.length >= 1
      await until(closed, 'the held request to close its connection', 9000)
      this.gate.release()
    }
    if (api === 'LeaveGroup') {
      this.leaveResult = await this.leaveOutcome
      await pause(QUIET_MS)
      return
    }
    if (api === 'OffsetCommit') this.commitResult = await this.commitOutcome
    if (api === 'Heartbeat' && fault !== 'none')
      await until(
        () => this.after().some((frame) => frame.api === 'Heartbeat'),
        'a later heartbeat'
      )
    await this.stable()
    await this.quiet()
  }

  /**
   * @returns {{state: object, wire: object[], ownership: object, assignments: object[], commits:
   *   object, handlers: string[]}} immutable copies of what was observed: the ordered requests with
   *   node, connection, member and generation; the commits as attempted and as applied by the
   *   independent store; and the handler identities
   */
  snapshot() {
    return structuredClone({
      state: {
        joins: this.joins.length,
        crashes: this.crashes.map(({ error }) => ({ code: error?.code, message: error?.message })),
        timeouts: this.timeouts.length,
        faulted: this.faulted?.seq ?? null,
        commit: this.commitResult,
        leave: this.leaveResult,
      },
      wire: this.trace.map(({ api, node, generation, fields }) => ({
        api,
        node,
        connection: generation,
        memberId: fields.memberId,
        generation: fields.generationId ?? fields.groupGenerationId,
      })),
      ownership: { overlaps: this.overlaps },
      assignments: this.joins.map(({ memberId, memberAssignment }) => ({
        memberId,
        memberAssignment,
      })),
      commits: {
        attempted: this.frames('OffsetCommit').length,
        applied: this.applied,
        store: Object.fromEntries(this.store),
      },
      handlers: this.deliveries.map(({ id }) => id),
    })
  }

  /**
   * Asserts the row's literal contract: the named state, the declared wire invariants and the
   * round budget. Every check is written from the independent table, so a client that deviates
   * fails here rather than redefining the expectation.
   * @returns {void}
   * @throws {Error} through `expect`, naming the row, when the contract is not met
   */
  verify() {
    const { api, fault, expectedState, expectedWire, maxRounds } = this.row
    expect(this.cluster.failures()).toEqual([])
    expect(this.overlaps).toBe(expectedWire.duplicateAssignments)
    for (const { memberAssignment } of this.joins) {
      const held = Object.values(memberAssignment).flat()
      expect(new Set(held).size).toBe(held.length)
    }
    if (fault === 'none') {
      this.verifyHealthy()
      return
    }
    expect(this.faulted).not.toBeNull()
    const contract = CONTRACT[`${api}:${expectedState}`]
    expect(contract).toBeDefined()
    contract(this)
    if (!this.stale) {
      const rounds = this.joins.filter(({ at }) => at > this.faulted.at).length
      expect(rounds).toBeLessThanOrEqual(maxRounds)
    }
  }

  /**
   * @private
   * @returns {void}
   * @throws {Error} through `expect` when the unfaulted control misbehaves
   */
  verifyHealthy() {
    const { api } = this.row
    expect(this.crashes).toEqual([])
    expect(this.frames('JoinGroup').map(({ fields }) => fields.memberId)).toEqual(['', MEMBER_ID])
    expect(this.frames('SyncGroup')).toHaveLength(1)
    if (api === 'Heartbeat') expect(this.frames('Heartbeat').length).toBeGreaterThanOrEqual(2)
    if (api === 'OffsetCommit') {
      expect(this.commitResult).toEqual({ ok: true })
      expect(this.store.get(`${TOPIC}|0`)).toBe('3')
      expect(this.applied).toHaveLength(1)
    }
    if (api === 'LeaveGroup') {
      expect(this.leaveResult).toEqual({ ok: true })
      expect(this.frames('LeaveGroup')).toHaveLength(1)
    }
  }

  /**
   * Releases every held gate, closes the consumer, removes the listeners, compares open sockets
   * with the baseline `open` took before building the consumer and requires every timer created
   * while the episode was traced to have ended. Runs in a `finally`, so a failed episode still
   * leaves nothing behind.
   * @returns {Promise<void>}
   * @throws {Error} when the consumer cannot be closed, the cluster refused a request, a socket
   *   outlives the episode, or a timer it created is still active (named by its creation stack)
   */
  async close() {
    try {
      try {
        if (this.gate?.settle) this.gate.release()
      } finally {
        if (this.consumer) await this.consumer.disconnect()
      }
      this.cluster.assertClean()
      await pause(50)
      const after = await this.probe.snapshot()
      expect(after.sockets.open).toBe(this.baseline.sockets.open)
      const outliving = await this.timers.outliving()
      if (outliving.length > 0) {
        throw new Error(
          `${outliving.length} timer(s) created by the episode outlive it:\n${outliving.join('\n\n')}`
        )
      }
    } finally {
      this.timers?.disable()
      for (const off of this.cleanup.splice(0)) off()
    }
  }
}

/**
 * @param {ClassicFaultEpisode} episode
 * @param {string} api
 * @returns {object|undefined} the first control frame of that API after the fault
 */
const next = (episode, api) => episode.after().find((frame) => frame.api === api)

/**
 * @param {ClassicFaultEpisode} episode
 * @returns {string} the member id the faulted request carried
 */
const memberOf = (episode) => episode.faulted.fields.memberId

/**
 * @param {ClassicFaultEpisode} episode
 * @returns {object[]} the rejoin requests after the fault that asked for a member id
 */
const joinsAsking = (episode) =>
  episode.after().filter(({ api, fields }) => api === 'JoinGroup' && fields.memberId === '')

/**
 * Asserts the group was not rebalanced after the fault: no join, sync or leave followed it.
 * @param {ClassicFaultEpisode} episode
 * @returns {void}
 */
const expectNoRebalance = (episode) => {
  for (const api of ['JoinGroup', 'SyncGroup', 'LeaveGroup'])
    expect(episode.after().filter((frame) => frame.api === api)).toEqual([])
}

/**
 * Asserts the next JoinGroup after the fault names the given member id.
 * @param {ClassicFaultEpisode} episode
 * @param {string} memberId the id the rejoin must send
 * @returns {void}
 */
const expectRejoinAs = (episode, memberId) => {
  const join = next(episode, 'JoinGroup')
  expect(join).toBeDefined()
  expect(join.fields.memberId).toBe(memberId)
}

/**
 * Asserts the client asked the cluster for the coordinator again before its next request of an API.
 * @param {ClassicFaultEpisode} episode
 * @param {string} api the retried API
 * @returns {void}
 */
const expectRefindBefore = (episode, api) => {
  const names = episode.after().map((frame) => frame.api)
  expect(names).toContain('FindCoordinator')
  expect(names.indexOf('FindCoordinator')).toBeLessThan(names.indexOf(api))
}

/**
 * Asserts a retry waited for a backoff rather than spinning.
 * @param {ClassicFaultEpisode} episode
 * @param {string} api the retried API
 * @returns {void}
 */
const expectBackoffBefore = (episode, api) => {
  expect(next(episode, api).at - episode.faulted.at).toBeGreaterThanOrEqual(BACKOFF_FLOOR_MS)
}

/**
 * Asserts the next request of an API reached the same node on a newer connection.
 * @param {ClassicFaultEpisode} episode
 * @param {string} api the retried API
 * @returns {void}
 */
const expectNewConnection = (episode, api) => {
  const frame = next(episode, api)
  expect(frame).toBeDefined()
  expect(frame.generation).toBeGreaterThan(episode.faulted.generation)
}

/**
 * @param {object} frame a trace entry
 * @returns {number|undefined} the generation its request named
 */
const generationOf = ({ fields }) => fields.generationId ?? fields.groupGenerationId

/**
 * Asserts the retried request is the one the fault met: the same member, generation and offsets.
 * @param {ClassicFaultEpisode} episode
 * @param {string} api the retried API
 * @returns {void}
 */
const expectSameRequest = (episode, api) => {
  const frame = next(episode, api)
  expect(frame).toBeDefined()
  expect(frame.fields.memberId).toBe(episode.faulted.fields.memberId)
  expect(generationOf(frame)).toBe(generationOf(episode.faulted))
  if (api === 'OffsetCommit') expect(frame.fields.topics).toEqual(episode.faulted.fields.topics)
}

/**
 * Asserts a leave ended locally after exactly one request: one frame, no retry, no lookup and no
 * further group traffic.
 * @param {ClassicFaultEpisode} episode
 * @returns {void}
 */
const expectLeaveForgotten = (episode) => {
  expect(episode.leaveResult).toEqual({ ok: true })
  expect(episode.frames('LeaveGroup')).toHaveLength(1)
  expect(episode.after()).toEqual([])
}

/**
 * Asserts the commit store holds the explicit commit and the call succeeded. A commit that timed
 * out may have been applied before its answer was lost, so it may sit in the store twice.
 * @param {ClassicFaultEpisode} episode
 * @returns {void}
 */
const expectCommitLanded = (episode) => {
  expect(episode.commitResult).toEqual({ ok: true })
  expect(episode.store.get(`${TOPIC}|0`)).toBe('3')
  const { fault } = episode.row
  if (fault === 'timeout') expect(episode.applied.length).toBeGreaterThanOrEqual(1)
  else expect(episode.applied).toHaveLength(1)
  for (const entry of episode.applied)
    expect(entry).toEqual({ partition: 0, offset: '3', memberId: MEMBER_ID, generation: 1 })
}

/**
 * Asserts a commit answered with a loss verdict surfaced its code, wrote nothing afterwards and
 * left the independent store untouched.
 * @param {ClassicFaultEpisode} episode
 * @param {number} code the verdict's error code
 * @returns {void}
 */
const expectCommitLost = (episode, code) => {
  expect(episode.commitResult).toMatchObject({ ok: false, code })
  expect(episode.after().filter(({ api }) => api === 'OffsetCommit')).toEqual([])
  expect(episode.applied).toEqual([])
}

/**
 * A held request that timed out closed its connection, and its held answer settled.
 * @param {ClassicFaultEpisode} episode
 * @returns {void}
 */
const expectGenerationClosed = (episode) => {
  const { api } = episode.row
  if (api === 'JoinGroup' || api === 'SyncGroup') {
    expect(episode.crashes.map(({ error }) => error.message)).toEqual([
      expect.stringContaining('Timed out joining the group'),
    ])
  } else {
    expect(episode.timeouts).toHaveLength(1)
  }
  const reopened = episode
    .after()
    .some(
      ({ node, generation }) =>
        node === episode.faulted.node && generation > episode.faulted.generation
    )
  expect(reopened).toBe(true)
  expect(episode.gate.settle).toBeNull()
}

/**
 * After a late answer for a replaced membership nothing may have been written: no rejoin, sync,
 * leave, lookup or commit, no crash and no new join since it was released; the replacement keeps
 * its scalars.
 * @param {ClassicFaultEpisode} episode
 * @returns {void}
 */
const expectUnchanged = (episode) => {
  const written = episode.trace
    .slice(episode.sentBeforeRelease)
    .filter(({ api }) => !DATA_PLANE.includes(api) && api !== 'Heartbeat')
  expect(written).toEqual([])
  expect(episode.crashes.filter(({ at }) => at > episode.released)).toEqual([])
  expect(episode.applied).toEqual([])
  expect(episode.joins.filter(({ at }) => at > episode.released)).toEqual([])
  for (const join of episode.joins) expect(join.memberId).toBe(MEMBER_ID)
  const later = episode.frames('Heartbeat').filter(({ seq }) => seq >= episode.sentBeforeRelease)
  for (const frame of later)
    expect(frame.fields).toMatchObject({ memberId: MEMBER_ID, groupGenerationId: 1 })
}

/**
 * The independent contract of every named state, keyed `api:expectedState`: what the wire after
 * the faulted answer must show. Each function receives the episode and reads only its trace,
 * events and handler identities.
 * @type {Record<string, (episode: ClassicFaultEpisode) => void>}
 */
const CONTRACT = {
  'JoinGroup:refind-retry': (e) => {
    expectRefindBefore(e, 'JoinGroup')
    expectRejoinAs(e, memberOf(e))
    const names = e.after().map(({ api }) => api)
    expect(names.slice(0, names.indexOf('JoinGroup'))).not.toContain('SyncGroup')
    expect(next(e, 'SyncGroup')).toBeDefined()
  },
  'JoinGroup:retry-backoff': (e) => {
    expectBackoffBefore(e, 'JoinGroup')
    expectRejoinAs(e, memberOf(e))
  },
  'JoinGroup:clear-member-rejoin': (e) => expectRejoinAs(e, ''),
  'JoinGroup:not-a-join-answer': (e) => {
    expect(
      next(e, 'JoinGroup') !== undefined || e.crashes.some(({ error }) => error?.code === 22)
    ).toBe(true)
    expect(e.after().filter(({ api }) => api === 'OffsetCommit')).toEqual([])
  },
  'JoinGroup:retry-join': (e) => {
    expectRejoinAs(e, memberOf(e))
    const names = e.after().map(({ api }) => api)
    expect(names.indexOf('SyncGroup')).toBeGreaterThan(names.indexOf('JoinGroup'))
  },
  'JoinGroup:retry-keep-member': (e) => {
    expectRejoinAs(e, memberOf(e))
    expect(joinsAsking(e)).toEqual([])
    expectNewConnection(e, 'JoinGroup')
  },
  'JoinGroup:generation-close-retry': expectGenerationClosed,
  'SyncGroup:refind-retry': (e) => {
    expect(e.after().map(({ api }) => api)).toContain('FindCoordinator')
    expectSameRequest(e, 'SyncGroup')
  },
  'SyncGroup:retry-sync': (e) => {
    expect(e.after()[0].at - e.faulted.at).toBeGreaterThanOrEqual(BACKOFF_FLOOR_MS)
    expectSameRequest(e, 'SyncGroup')
    expect(joinsAsking(e)).toEqual([])
  },
  'SyncGroup:lost-clear-member': (e) => expectRejoinAs(e, ''),
  'SyncGroup:lost-keep-member': (e) => expectRejoinAs(e, memberOf(e)),
  'SyncGroup:rejoin-keep-generation': (e) => {
    expectRejoinAs(e, memberOf(e))
    expect(next(e, 'SyncGroup')).toBeDefined()
  },
  'SyncGroup:retry-keep-member': (e) => {
    expectRejoinAs(e, memberOf(e))
    expect(joinsAsking(e)).toEqual([])
    expectNewConnection(e, 'JoinGroup')
  },
  'SyncGroup:generation-close-retry': expectGenerationClosed,
  'Heartbeat:refind-no-verdict': (e) => {
    expectRefindBefore(e, 'Heartbeat')
    expectNoRebalance(e)
    expectSameRequest(e, 'Heartbeat')
    expect(e.joins).toHaveLength(1)
  },
  'Heartbeat:next-tick': (e) => {
    expectNoRebalance(e)
    expectSameRequest(e, 'Heartbeat')
    if (e.row.fault === 'disconnect') expectNewConnection(e, 'Heartbeat')
    expect(e.joins).toHaveLength(1)
  },
  'Heartbeat:lost-clear-member': (e) => expectRejoinAs(e, ''),
  'Heartbeat:lost-keep-member': (e) => expectRejoinAs(e, memberOf(e)),
  'Heartbeat:rejoin-only-stable': (e) => {
    expectRejoinAs(e, memberOf(e))
    expect(e.joins).toHaveLength(2)
  },
  'Heartbeat:generation-close-retry': (e) => {
    expectGenerationClosed(e)
    expectNoRebalance(e)
    expectSameRequest(e, 'Heartbeat')
  },
  'OffsetCommit:refind-retry-commit': (e) => {
    expectRefindBefore(e, 'OffsetCommit')
    expectSameRequest(e, 'OffsetCommit')
    expectCommitLanded(e)
  },
  'OffsetCommit:retry-commit': (e) => {
    if (e.row.fault === 'disconnect') expectNewConnection(e, 'OffsetCommit')
    else expectBackoffBefore(e, 'OffsetCommit')
    expectSameRequest(e, 'OffsetCommit')
    expectCommitLanded(e)
  },
  'OffsetCommit:lost-no-commit-clear-member': (e) => {
    expectCommitLost(e, 25)
    expectRejoinAs(e, '')
  },
  'OffsetCommit:lost-no-commit-keep-member': (e) => {
    expectCommitLost(e, 22)
    expectRejoinAs(e, memberOf(e))
  },
  'OffsetCommit:revoked-commit-rejoin': (e) => {
    expect(e.commitResult).toMatchObject({ ok: false, code: 27 })
    expect(e.after().filter(({ api }) => api === 'OffsetCommit')).toEqual([])
  },
  'OffsetCommit:generation-close-retry': (e) => {
    expectGenerationClosed(e)
    expectSameRequest(e, 'OffsetCommit')
    expectCommitLanded(e)
  },
  'LeaveGroup:log-forget': expectLeaveForgotten,
  'LeaveGroup:ignore-forget': expectLeaveForgotten,
  'LeaveGroup:not-a-leave-answer': expectLeaveForgotten,
  'LeaveGroup:generation-close-retry': (e) => {
    expect(e.timeouts).toHaveLength(1)
    expectLeaveForgotten(e)
  },
  'Heartbeat:unchanged': expectUnchanged,
  'OffsetCommit:unchanged': expectUnchanged,
  'JoinGroup:unchanged': expectUnchanged,
  'SyncGroup:unchanged': expectUnchanged,
  'LeaveGroup:unchanged': expectUnchanged,
}

/**
 * The assertions the client does not meet today, each owned by a named later writer. Each runs as
 * `it.failing` only beside the same row's ordinary test, which passes its setup, injection,
 * contract and cleanup, so a failure there never counts as the product defect.
 * @type {Record<string, {title: string, owner: string, hunk: string, check: (episode: ClassicFaultEpisode) => void}>}
 */
const FINDINGS = {
  'JoinGroup:timeout': {
    title: 'a join round that expired keeps the member id its handshake assigned',
    owner: 'C14b',
    hunk: 'src/consumer/consumerGroup.js:1307 raises the round-deadline error, and the crash path in src/consumer/index.js (shutDown, then restart) builds a membership without the pending member id',
    check: (e) => expectRejoinAs(e, memberOf(e)),
  },
  'SyncGroup:timeout': {
    title: 'a sync round that expired keeps the member id the join was accepted under',
    owner: 'C14b',
    hunk: 'src/consumer/consumerGroup.js:1307 raises the round-deadline error, and the crash path in src/consumer/index.js (shutDown, then restart) builds a membership without the accepted member id',
    check: (e) => expectRejoinAs(e, memberOf(e)),
  },
  'OffsetCommit:27': {
    title: 'a commit answered with a rebalance in progress asks the group to rejoin',
    owner: 'C14b',
    hunk: 'src/consumer/index.js commitOffsets and the revoked verdict in src/consumer/runner.js: the explicit commit path surfaces code 27 but requests no rejoin',
    check: (e) => expect(next(e, 'JoinGroup')).toBeDefined(),
  },
}

module.exports = {
  ClassicFaultEpisode,
  CONTRACT,
  FINDINGS,
  MEMBER_ID,
  NODES,
  PARTITIONS,
  batchAt,
}
