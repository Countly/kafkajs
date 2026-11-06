const assert = require('node:assert/strict')
const {
  acquired,
  deferred,
  fetchAnswer,
  flush,
  heartbeatAnswer,
  part,
  recordsHex,
  shareClient,
  topError,
  until,
} = require('../../../../testHelpers/shareHarness')
const ConnectionPool = require('../../../network/connectionPool')
const RequestQueue = require('../../../network/requestQueue')
const { requests } = require('../../../protocol/requests')

/**
 * Checks observations without answering a request or making a client decision.
 * The scenario supplies issuance identities captured from real calls, not wire counters.
 */
class FaultAudit {
  /** @returns {FaultAudit} An empty observer with compact interval storage. */
  constructor() {
    this.finals = new Map()
    this.forgotten = new Map()
    this.settled = new Map()
    this.handlers = new Map()
  }

  /**
   * Validates a compact range without converting absolute offsets to Number.
   * @param {string} first Inclusive decimal first offset.
   * @param {string} last Inclusive decimal last offset.
   * @returns {[bigint,bigint]} Exact bounds.
   * @throws {Error} For noncanonical or out-of-domain offsets.
   */
  range(first, last) {
    for (const value of [first, last]) {
      assert.equal(typeof value, 'string')
      assert.match(value, /^(0|[1-9][0-9]*)$/)
      assert(BigInt(value) <= 9223372036854775807n)
    }
    assert(BigInt(first) <= BigInt(last))
    return [BigInt(first), BigInt(last)]
  }

  /**
   * Reads one independently decoded frame under the scenario's captured authority.
   * This model records only listed and forgotten partitions; it creates no responses.
   * @param {object} record Actual ScriptedCluster RequestRecord.
   * @param {object} authority Test-owned identity, currentness and wanted-set observations.
   * @returns {void} Same-frame last acknowledgements and forget are legal.
   * @throws {Error} For stale writes, malformed requests or later forgotten-partition reuse.
   */
  frame(record, authority) {
    const { apiKey, fields } = record
    assert(authority.allowed, 'unauthorized frame')
    if (apiKey !== 78 && apiKey !== 79) return
    const batches = (fields.topics || []).flatMap((topic) =>
      topic.partitions.flatMap((partition) => partition.acknowledgementBatches)
    )
    for (const batch of batches) this.range(batch.firstOffset, batch.lastOffset)
    if (fields.shareSessionEpoch === 0) {
      assert.equal(apiKey, 78)
      assert.equal(batches.length, 0, 'opening acknowledgements')
    }
    if (fields.shareSessionEpoch === -1) {
      assert.equal(apiKey, 79)
      assert.equal(authority.kind, 'closing')
      assert(authority.episode && ['symbol', 'object'].includes(typeof authority.episode))
      const nodes = this.finals.get(authority.episode) || new Set()
      assert(!nodes.has(record.node), 'duplicate final')
      nodes.add(record.node)
      this.finals.set(authority.episode, nodes)
    }
    if (fields.isRenewAck) assert.equal(apiKey, 79)
    if (apiKey === 79) {
      for (const name of [
        'maxWaitMs',
        'minBytes',
        'maxBytes',
        'maxRecords',
        'batchSize',
        'forgottenTopicsData',
      ])
        assert.equal(Object.prototype.hasOwnProperty.call(fields, name), false)
      return
    }
    assert(fields.maxBytes > 0 && fields.maxRecords > 0)
    let forgotten = this.forgotten.get(authority.open)
    if (!forgotten) {
      forgotten = new Set()
      this.forgotten.set(authority.open, forgotten)
    }
    for (const topic of fields.topics) {
      for (const partition of topic.partitions) {
        const key = `${topic.topicId}:${partition.partitionIndex}`
        assert(!forgotten.has(key) || authority.wanted.has(key), 'forgotten partition listed again')
        if (authority.wanted.has(key)) forgotten.delete(key)
      }
    }
    for (const topic of fields.forgottenTopicsData) {
      for (const partition of topic.partitions) forgotten.add(`${topic.topicId}:${partition}`)
    }
  }

  /**
   * Checks disjoint terminal results within one original acquisition and partition.
   * Renew results are nonterminal and may repeat under distinct invocation identities.
   * @param {object|symbol} acquisition Original opaque acquisition identity from the real owner.
   * @param {object} event Compact result including firstOffset, lastOffset and type.
   * @returns {void} Storage scales with result intervals, never their numeric width.
   * @throws {Error} For a duplicate or overlapping terminal result.
   */
  settlement(acquisition, event) {
    const [first, last] = this.range(event.firstOffset, event.lastOffset)
    if (event.type === 'renew') return
    let partitions = this.settled.get(acquisition)
    if (!partitions) {
      partitions = new Map()
      this.settled.set(acquisition, partitions)
    }
    const key = `${event.topicId}:${event.partition}`
    const prior = partitions.get(key) || []
    for (const interval of prior)
      assert(last < interval[0] || first > interval[1], 'terminal result overlap')
    prior.push([first, last])
    partitions.set(key, prior)
  }

  /**
   * Records starts and finishes of actual application invocations for one stable record identity.
   * @param {string} identity Topic UUID, partition and exact offset.
   * @param {object|symbol} call Original application invocation.
   * @param {boolean} start True for entry; false for its finally boundary.
   * @returns {void} A replacement cannot share an active record with its predecessor.
   * @throws {Error} For concurrent delivery or a foreign completion.
   */
  handler(identity, call, start) {
    if (start) {
      assert(!this.handlers.has(identity), 'concurrent record handler')
      this.handlers.set(identity, call)
    } else {
      assert.equal(this.handlers.get(identity), call)
      this.handlers.delete(identity)
    }
  }
}

/** Every timer the client owns is faked; socket answers still travel on real macrotasks. */
const FAKE = { doNotFake: ['setImmediate', 'queueMicrotask', 'nextTick', 'hrtime'] }
const NAME = '__proto__'
const IDS = {
  A: '01020304-0506-0708-090a-0b0c0d0e0f10',
  B: '11121314-1516-1718-191a-1b1c1d1e1f20',
  U: '21222324-2526-2728-292a-2b2c2d2e2f30',
}
const ALIAS = new Map(Object.entries(IDS).map(([alias, id]) => [id, alias]))
const H = '9007199254740993'
const J = '9007199254740994'
const LEADERS = [0, 1, 1]
const STALE_CALL = 'Handler call is no longer current'

/**
 * @param {number} ms Fake milliseconds.
 * @returns {Promise<void>} Moves the fake clock and runs the client timers that fall due.
 */
const advance = (ms) => jest.advanceTimersByTimeAsync(ms)

/**
 * @returns {object} The metadata the wire serves: topic A named `__proto__` with partitions 0, 1, 2
 *   led by nodes 0, 1, 1. The name is a computed key so it is an own property, not a prototype.
 */
const topicTable = () => ({
  [NAME]: { topicId: IDS.A, partitions: LEADERS.map((leader) => ({ leader, epoch: 1 })) },
})

/**
 * @param {number} partition Partition index.
 * @param {object} [options] Answer options.
 * @param {string} [options.topicId] Topic UUID of the partition.
 * @param {number} [options.count=2] Delivery count of the acquired range.
 * @returns {object} A ShareFetch partition answer acquiring the exact records H and J.
 */
const dataPart = (partition, { topicId = IDS.A, count = 2 } = {}) =>
  part({
    topicId,
    partition,
    acquire: [acquired(H, J, count)],
    records: recordsHex(H, ['value-h', 'value-j']),
  })

/**
 * @param {Error} error A failure, possibly wrapping the original protocol error.
 * @returns {Array<number|string>} Every `code` along its cause chain, outermost first.
 */
const codesOf = (error) => {
  const codes = []
  for (let cursor = error; cursor; cursor = cursor.cause) {
    if (cursor.code !== undefined) codes.push(cursor.code)
  }
  return codes
}

/**
 * @param {Promise<*>} promise A promise the test does not await yet.
 * @returns {{settled: boolean, error: Error|null}} Its live state, so "still pending" is observable.
 */
const track = (promise) => {
  const state = { settled: false, error: null }
  promise.then(
    () => {
      state.settled = true
    },
    (error) => {
      state.settled = true
      state.error = error
    }
  )
  return state
}

/**
 * @param {Array<{firstOffset: string, lastOffset: string}>} ranges Result ranges.
 * @param {string} offset Exact decimal offset.
 * @returns {boolean} Whether a range covers the offset, compared without Number conversion.
 */
const covers = (ranges, offset) =>
  ranges.some(
    (range) =>
      BigInt(range.firstOffset) <= BigInt(offset) && BigInt(offset) <= BigInt(range.lastOffset)
  )

/**
 * The shared driver of every fault case: one real `Kafka` share consumer over the scripted share
 * cluster, call-through observation of its handlers and public events, test-owned gates released
 * in guaranteed cleanup, and the strict baseline. It scripts answers and observes frames; every
 * production transition stays with the client.
 */
class FaultCase {
  /**
   * @param {object} row The literal oracle row; never mutated.
   * @returns {FaultCase} A case holding no socket, timer or promise yet.
   */
  constructor(row) {
    this.row = row
    this.audit = new FaultAudit()
    this.h = null
    this.rounds = row.expected.rounds
    this.seen = {
      acks: [],
      joins: [],
      beats: [],
      crashes: [],
      resets: [],
      stops: [],
      assigned: [],
      fetches: [],
    }
    this.handlers = []
    this.calls = []
    this.holds = new Map()
    this.heldGates = []
    this.auditFailures = []
    this.gates = []
    this.tracked = []
    this.removers = []
    this.restartErrors = []
    this.supply = new Map()
    this.epochs = new Map()
    this.unhandled = []
    this.baseline = null
    this.interval = 50
    this.beatFault = null
    this.beatShape = null
    this.runState = null
    this.onUnhandled = (reason) => this.unhandled.push(reason)
  }

  /** @returns {object} The scripted cluster, available once `start` ran. */
  get cluster() {
    return this.h?.cluster
  }

  /**
   * Creates the client, its scripted cluster and observers. Fake timers are installed first and the
   * resource baseline is read before the client exists.
   * @param {object} [options] Client options.
   * @param {number} [options.requestTimeout=30000] The Kafka request timeout.
   * @param {boolean} [options.enforce=true] The Kafka `enforceRequestTimeout` option.
   * @param {object} [options.share] Overrides of the shareConsumer options.
   * @param {object} [options.retry] Overrides of the share retry policy.
   * @returns {Promise<void>}
   */
  async start({ requestTimeout = 30000, enforce = true, share = {}, retry = {} } = {}) {
    process.on('unhandledRejection', this.onUnhandled)
    jest.useFakeTimers(FAKE)
    await advance(1000)
    this.baseline = { timers: jest.getTimerCount() }
    this.h = shareClient({
      nodes: 3,
      interval: this.interval,
      topics: topicTable(),
      kafka: { requestTimeout, enforceRequestTimeout: enforce },
      share: {
        rackId: 'rack-a',
        ...share,
        retry: {
          restartOnFailure: async (error) => {
            this.restartErrors.push(error)
            return true
          },
          ...retry,
        },
      },
    })
    const { events, on } = this.h.consumer
    const watch = (name, sink, pick = ({ payload }) => payload) =>
      this.removers.push(on(events[name], (event) => sink.push(pick(event))))
    watch('ACKNOWLEDGE', this.seen.acks)
    watch('GROUP_JOIN', this.seen.joins)
    watch('HEARTBEAT', this.seen.beats)
    watch('CRASH', this.seen.crashes)
    watch('SESSION_RESET', this.seen.resets)
    watch('STOP', this.seen.stops, () => true)
    watch('ASSIGNMENT_CHANGED', this.seen.assigned)
    watch('FETCH', this.seen.fetches)
    this.removers.push(on(events.ACKNOWLEDGE, ({ payload }) => this.observeResult(payload)))
    this.cluster.answer('ShareFetch', (fields) => this.fetchAnswerFor(fields))
    this.cluster.answer('ShareGroupHeartbeat', (fields) => this.beatAnswerFor(fields))
    await this.h.consumer.subscribe({ topics: [NAME] })
  }

  /**
   * @param {object} fields A decoded ShareFetch request.
   * @returns {object} Records H and J for every listed partition that still has supply, else quiet.
   */
  fetchAnswerFor(fields) {
    const rows = []
    for (const topic of fields.topics || []) {
      for (const { partitionIndex } of topic.partitions) {
        const key = `${ALIAS.get(topic.topicId)}:${partitionIndex}`
        const left = this.supply.get(key) ?? 0
        if (left > 0) {
          this.supply.set(key, left - 1)
          rows.push(dataPart(partitionIndex, { topicId: topic.topicId }))
        } else rows.push(part({ topicId: topic.topicId, partition: partitionIndex }))
      }
    }
    return fetchAnswer(rows)
  }

  /**
   * @param {object} fields A decoded ShareGroupHeartbeat request.
   * @returns {object} The armed fault when it applies, else the quiet coordinator's answer.
   */
  beatAnswerFor(fields) {
    const fault = this.beatFault?.(fields)
    if (fault) return fault
    const base = heartbeatAnswer(fields, {
      interval: this.interval,
      epochs: this.epochs,
      topics: this.h.table,
    })
    return this.beatShape ? this.beatShape(fields, base) : base
  }

  /**
   * Gives each partition one acquisition of H and J.
   * @param {string[]} keys Partition keys such as `A:0`.
   * @returns {void}
   */
  supplyOnce(...keys) {
    for (const key of keys) this.supply.set(key, 1)
  }

  /**
   * Holds the handler of one exact record until released; cleanup releases whatever remains.
   * @param {string} identity Alias, partition and exact offset such as `A:0@9007199254740993`.
   * @returns {{promise: Promise<void>, resolve: () => void}} The gate.
   */
  hold(identity) {
    const gate = deferred()
    this.holds.set(identity, gate)
    this.heldGates.push({ identity, gate })
    return gate
  }

  /**
   * @param {string} identity An identity given to {@link FaultCase#hold}.
   * @returns {void} Lets the handler held on that record return.
   */
  release(identity) {
    this.heldGates.findLast((held) => held.identity === identity).gate.resolve()
  }

  /**
   * Runs an audit check from a callback whose exceptions the client would contain and log.
   * @param {() => void} check The audit call.
   * @returns {void} A failure is kept in `auditFailures` and thrown by `assertInvariants`.
   */
  audited(check) {
    try {
      check()
    } catch (error) {
      this.auditFailures.push(error)
    }
  }

  /**
   * Holds the wire answer of one request until the test decides how it ends.
   * @param {object} selector A schedule selector.
   * @returns {object} The schedule's gate, released at cleanup when still unsettled.
   */
  holdWire(selector) {
    const gate = this.h.hold(selector)
    this.gates.push(gate)
    return gate
  }

  /**
   * The one eachMessage handler of every run: it records its identity, holds the record the test
   * named, and tells the audit when it started and ended.
   * @param {object} payload The eachMessage payload.
   * @returns {Promise<void>}
   */
  async handle({ partition, message, acknowledge, renew, isRunning, topic }) {
    const alias = ALIAS.get(this.h.table.get(topic)?.topicId) ?? '?'
    const identity = `${alias}:${partition}@${message.offset}`
    const call = {
      identity,
      alias,
      partition,
      offset: message.offset,
      deliveryCount: message.deliveryCount,
      value: message.value.toString(),
      acknowledge,
      renew,
      isRunning,
      abandoned: false,
      token: Symbol(identity),
    }
    this.calls.push(call)
    this.audited(() => this.audit.handler(identity, call, true))
    const gate = this.holds.get(identity)
    this.holds.delete(identity)
    try {
      await this.onStart?.(call)
      await gate?.promise
      await this.onHandled?.(call)
    } finally {
      if (!call.abandoned) this.audited(() => this.audit.handler(identity, call, false))
    }
  }

  /**
   * Starts a run whose handler is {@link FaultCase#handle}; the returned promise is observed at once.
   * @returns {Promise<void>} The run promise, which settles after the first accepted join.
   */
  run() {
    const promise = this.h.consumer.run({
      eachMessage: (payload) => {
        const pending = this.handle(payload)
        this.handlers.push(pending)
        return pending
      },
    })
    this.runState = track(promise)
    this.tracked.push(promise)
    return promise
  }

  /** @returns {Promise<void>} Runs the client and lets the join and its first fetches settle. */
  async join() {
    await this.run()
    await flush()
  }

  /**
   * Records a terminal result in the audit and retires the authority of a handler whose range was
   * lost, so a later acquisition of the same record is not reported as an overlap.
   * @param {object} event A public acknowledge payload.
   * @returns {void}
   */
  observeResult(event) {
    this.audited(() => this.recordResult(event))
  }

  /**
   * @private
   * @param {object} event A public acknowledge payload.
   * @returns {void}
   */
  recordResult(event) {
    const alias = ALIAS.get(this.h.table.get(event.topic)?.topicId) ?? '?'
    const holder = this.calls.findLast(
      (call) =>
        call.partition === event.partition &&
        call.alias === alias &&
        covers([event], call.offset) &&
        !call.abandoned
    )
    this.audit.settlement(holder ? holder.token : `${alias}:${event.partition}`, {
      ...event,
      topicId: IDS[alias],
    })
    const lost = ['session-lost', 'membership-lost'].includes(event.error?.reason)
    if (!lost || event.type === 'renew') return
    for (const call of this.calls) {
      if (call.partition !== event.partition || call.alias !== alias || call.abandoned) continue
      if (!covers([event], call.offset) || !this.audit.handlers.has(call.identity)) continue
      if (this.audit.handlers.get(call.identity) !== call) continue
      call.abandoned = true
      this.audit.handler(call.identity, call, false)
    }
  }

  /**
   * @param {...number} keys API keys.
   * @returns {object[]} The recorded frames of those APIs, oldest first.
   */
  frames(...keys) {
    return this.h.frames(...keys)
  }

  /** @returns {object[]} Every ShareAcknowledge final frame (epoch -1). */
  finals() {
    return this.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1)
  }

  /** @returns {object[]} Every group-leave frame (epoch -1). */
  leaves() {
    return this.frames(76).filter(({ fields }) => fields.memberEpoch === -1)
  }

  /** @returns {object[]} Every join frame (epoch 0). */
  joins() {
    return this.frames(76).filter(({ fields }) => fields.memberEpoch === 0)
  }

  /** @returns {number[]} The member epoch of every heartbeat frame, in wire order. */
  heartbeatEpochs() {
    return this.frames(76).map(({ fields }) => fields.memberEpoch)
  }

  /** @returns {number} How many FindCoordinator requests the client wrote. */
  lookups() {
    return this.cluster.requests().filter(({ api }) => api === 'FindCoordinator').length
  }

  /** @returns {string} A compact transcript of every share and heartbeat frame for failures. */
  transcript() {
    return JSON.stringify(
      this.frames(76, 78, 79).map(({ api, node, generation, fields }) => [
        api,
        node,
        generation,
        fields.memberEpoch ?? fields.shareSessionEpoch,
      ])
    )
  }

  /**
   * @param {number} [ms=50] Fake milliseconds.
   * @returns {Promise<void>} Moves the clock and lets the scripted sockets answer.
   */
  async pump(ms = 50) {
    await advance(ms)
    await flush()
  }

  /**
   * Moves the fake clock in 50 ms rounds until the condition holds, within the row's round bound.
   * @param {() => boolean} predicate The reached-stage condition.
   * @param {string} what What is awaited, named by the failure.
   * @param {number} [limit] Rounds allowed; the row's bound by default.
   * @returns {Promise<void>}
   * @throws {Error} With the frame transcript when the condition does not hold in time.
   */
  async advanceUntil(predicate, what, limit = this.rounds) {
    for (let step = 0; step < limit && !predicate(); step++) await this.pump()
    if (!predicate())
      throw new Error(`${what} did not happen in ${limit} rounds ${this.transcript()}`)
  }

  /**
   * @param {object} gate A gate from {@link FaultCase#holdWire}.
   * @param {string} [what] What is awaited, named by the failure.
   * @returns {Promise<object>} The held call, once it arrived within the real-time bound.
   * @throws {Error} When no call arrives.
   */
  async arrival(gate, what = 'the held frame') {
    await until(() => gate.call !== null, what)
    return gate.call
  }

  /**
   * @param {object} selector A schedule selector for the frame to disturb.
   * @returns {object} The held gate; `fireTransport` ends it with a transport failure.
   */
  armTransport(selector) {
    this.transportGate = this.holdWire(selector)
    return this.transportGate
  }

  /**
   * Ends the armed frame with a typed transport failure: a peer end on its own socket, or the
   * request timeout of the fake clock. The held answer is released late and must change nothing.
   * @param {'disconnect'|'timeout'} kind The failure.
   * @param {number} [requestTimeout=1000] The configured request timeout in fake milliseconds.
   * @returns {Promise<object>} The frame that was disturbed.
   */
  async fireTransport(kind, requestTimeout = 1000) {
    const call = await this.arrival(this.transportGate)
    if (kind === 'disconnect') this.cluster.sockets()[call.generation - 1].emit('end')
    else await this.pump(requestTimeout + 1)
    await flush()
    return call
  }

  /**
   * Replays every frame, handler and result through the shared audit with its issuance identity.
   * @param {(record: object) => Set<string>} [wantedFor] The wanted partition keys at a frame.
   * @returns {void}
   * @throws {Error} For any frame, result or handler the audit refuses.
   */
  replay(wantedFor = () => new Set([0, 1, 2].map((index) => `${IDS.A}:${index}`))) {
    const opens = new Map()
    const episodes = new Map()
    let membership = 0
    for (const record of this.frames(76, 78, 79)) {
      const { apiKey, fields } = record
      if (apiKey === 76 && fields.memberEpoch === 0) membership++
      const closing = fields.shareSessionEpoch === -1
      if (apiKey === 78 && fields.shareSessionEpoch === 0)
        opens.set(record.node, (opens.get(record.node) ?? 0) + 1)
      if (closing && !episodes.has(membership)) episodes.set(membership, {})
      this.audit.frame(record, {
        allowed: true,
        kind: closing ? 'closing' : 'ordinary',
        episode: closing ? episodes.get(membership) : undefined,
        open: `${record.node}#${opens.get(record.node) ?? 0}`,
        wanted: wantedFor(record),
      })
    }
  }

  /**
   * Ends the case: releases every gate once, closes the client with a zero deadline, observes
   * every pending handler and tracked promise, and removes the case's own listeners. Every failure
   * is kept and rethrown together.
   * @returns {Promise<void>}
   * @throws {AggregateError} Every cleanup failure.
   */
  async dispose() {
    if (this.disposed) return
    this.disposed = true
    const failures = []
    const attempt = async (work) => {
      try {
        await work()
      } catch (error) {
        failures.push(error)
      }
    }
    for (const { gate } of this.heldGates) gate.resolve()
    for (const gate of this.gates) if (gate.settle) gate.release()
    if (this.h) await attempt(() => this.h.consumer.disconnect({ timeout: 0 }))
    await flush()
    const settled = await Promise.allSettled([...this.handlers, ...this.tracked])
    for (const remove of this.removers.splice(0)) remove()
    process.off('unhandledRejection', this.onUnhandled)
    const rejected = settled.filter(({ status }) => status === 'rejected')
    this.lateRejections = rejected.map(({ reason }) => reason)
    if (failures.length) throw new AggregateError(failures, 'Share fault cleanup failed')
  }

  /**
   * Compares the actual resources with the baseline read before the client existed.
   * @returns {void}
   * @throws {Error} For an open socket, a surviving timer, a failed cluster or an unhandled rejection.
   */
  assertBaseline() {
    assert.deepEqual(this.unhandled, [], 'unhandled rejections')
    if (!this.h) return
    assert.deepEqual(this.cluster.failures(), [], 'scripted cluster failures')
    const open = this.cluster.sockets().filter((socket) => !socket.destroyed).length
    assert.equal(open, 0, 'open sockets')
    assert(
      jest.getTimerCount() <= this.baseline.timers,
      `timers ${jest.getTimerCount()} above baseline ${this.baseline.timers}`
    )
  }
}

/** Literal fault, phase and outcome rows; the runtime driver must not mutate this oracle. */
const rows = [
  {
    fault: 'heartbeat 14',
    step: 'before join',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: 0,
      cause: 14,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 15',
    step: 'before join',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: 0,
      cause: 15,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 16',
    step: 'before join',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: 0,
      cause: 16,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 25',
    step: 'before join',
    expected: {
      action: 'rejoin',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: 0,
      cause: 25,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 110',
    step: 'before join',
    expected: {
      action: 'rejoin',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: 0,
      cause: 110,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 35',
    step: 'before join',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'current',
      nextEpoch: null,
      cause: 35,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 30',
    step: 'before join',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'current',
      nextEpoch: null,
      cause: 30,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 29',
    step: 'before join',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'current',
      nextEpoch: null,
      cause: 29,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 81',
    step: 'before join',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'current',
      nextEpoch: null,
      cause: 81,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 42',
    step: 'before join',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'current',
      nextEpoch: null,
      cause: 42,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat disconnect',
    step: 'before join',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: 0,
      cause: 'connection',
      finals: 0,
    },
  },
  {
    fault: 'heartbeat timeout',
    step: 'before join',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: 0,
      cause: 'connection',
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 69',
    step: 'before join',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'current',
      nextEpoch: null,
      cause: 69,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 14',
    step: 'after join',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 14,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 15',
    step: 'after join',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 15,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 16',
    step: 'after join',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 16,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 25',
    step: 'after join',
    expected: {
      action: 'rejoin',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'membership-lost',
      nextEpoch: 0,
      cause: 25,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 110',
    step: 'after join',
    expected: {
      action: 'rejoin',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'membership-lost',
      nextEpoch: 0,
      cause: 110,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 35',
    step: 'after join',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 35,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 30',
    step: 'after join',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 30,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 29',
    step: 'after join',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 29,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 81',
    step: 'after join',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 81,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 42',
    step: 'after join',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 42,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat disconnect',
    step: 'after join',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 'connection',
      finals: 0,
    },
  },
  {
    fault: 'heartbeat timeout',
    step: 'after join',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 'connection',
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 69',
    step: 'after join',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 69,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 14',
    step: 'assignment change',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 14,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 15',
    step: 'assignment change',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 15,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 16',
    step: 'assignment change',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 16,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 25',
    step: 'assignment change',
    expected: {
      action: 'rejoin',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'membership-lost',
      nextEpoch: 0,
      cause: 25,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 110',
    step: 'assignment change',
    expected: {
      action: 'rejoin',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'membership-lost',
      nextEpoch: 0,
      cause: 110,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 35',
    step: 'assignment change',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 35,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 30',
    step: 'assignment change',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 30,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 29',
    step: 'assignment change',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 29,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 81',
    step: 'assignment change',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 81,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 42',
    step: 'assignment change',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 42,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat disconnect',
    step: 'assignment change',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 'connection',
      finals: 0,
    },
  },
  {
    fault: 'heartbeat timeout',
    step: 'assignment change',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 'connection',
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 69',
    step: 'assignment change',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 69,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 14',
    step: 'leave',
    expected: {
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 14,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 15',
    step: 'leave',
    expected: {
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 15,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 16',
    step: 'leave',
    expected: {
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 16,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 25',
    step: 'leave',
    expected: {
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 25,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 110',
    step: 'leave',
    expected: {
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 110,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 35',
    step: 'leave',
    expected: {
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 35,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 30',
    step: 'leave',
    expected: {
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 30,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 29',
    step: 'leave',
    expected: {
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 29,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 81',
    step: 'leave',
    expected: {
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 81,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 42',
    step: 'leave',
    expected: {
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 42,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat disconnect',
    step: 'leave',
    expected: {
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 'connection',
      finals: 1,
    },
  },
  {
    fault: 'heartbeat timeout',
    step: 'leave',
    expected: {
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 'connection',
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 69',
    step: 'leave',
    expected: {
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 69,
      finals: 1,
    },
  },
  {
    fault: 'unknown assignment',
    step: 'assignment change',
    expected: {
      action: 'assignment',
      state: 'STABLE',
      rounds: 8,
      assignment: ['A:0', 'A:1', 'A:2'],
      resolved: ['A:0', 'A:1', 'A:2', 'U:0'],
      refresh: 1,
    },
  },
  {
    fault: 'empty assignment',
    step: 'assignment change',
    expected: {
      action: 'assignment',
      state: 'STABLE',
      rounds: 8,
      assignment: [],
      released: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      changes: 1,
    },
  },
  {
    fault: 'equal assignment',
    step: 'assignment change',
    expected: {
      action: 'assignment',
      state: 'STABLE',
      rounds: 8,
      assignment: ['A:0', 'A:1', 'A:2'],
      changes: 0,
    },
  },
  {
    fault: 'heartbeat 69 while unsubscribed',
    step: 'leave',
    expected: {
      action: 'ignored',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 69,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 69 from superseded membership',
    step: 'rejoin',
    expected: {
      action: 'ignored',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'membership-lost',
      nextEpoch: 0,
      cause: 69,
      finals: 1,
    },
  },
  {
    fault: 'held bulk metadata',
    step: 'leave',
    expected: {
      action: 'target guard',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      current: ['B:0@9007199254740993'],
      old: ['A:1@9007199254740993'],
      target: 'distinct',
      replacement: true,
    },
  },
  {
    fault: 'held per-id metadata',
    step: 'fence',
    expected: {
      action: 'target guard',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      current: ['B:0@9007199254740993'],
      old: ['A:1@9007199254740993'],
      target: 'distinct',
      replacement: true,
    },
  },
  {
    fault: 'held by-name metadata',
    step: 'rejoin',
    expected: {
      action: 'target guard',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      current: ['B:0@9007199254740993'],
      old: ['A:1@9007199254740993'],
      target: 'distinct',
      replacement: true,
    },
  },
  {
    fault: 'held unresolved publication',
    step: 'topic replacement',
    expected: {
      action: 'target guard',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      current: ['B:0@9007199254740993'],
      old: ['A:1@9007199254740993'],
      target: 'distinct',
      replacement: true,
    },
  },
  {
    fault: 'equal epoch old answer',
    step: 'replacement run',
    expected: {
      action: 'target guard',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      current: ['B:0@9007199254740993'],
      old: ['A:1@9007199254740993'],
      target: 'distinct',
      replacement: true,
    },
  },
  {
    fault: 'lower epoch answer',
    step: 'after join',
    expected: {
      action: 'target guard',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      current: ['B:0@9007199254740993'],
      old: ['A:1@9007199254740993'],
      target: 'distinct',
      replacement: true,
    },
  },
  {
    fault: 'known ids while unknown retries',
    step: 'assignment change',
    expected: {
      action: 'target guard',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      current: ['B:0@9007199254740993'],
      old: ['A:1@9007199254740993'],
      target: 'distinct',
      replacement: true,
    },
  },
  {
    fault: 'same target preserves backoff',
    step: 'assignment change',
    expected: {
      action: 'target guard',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      current: ['B:0@9007199254740993'],
      old: ['A:1@9007199254740993'],
      target: 'distinct',
      replacement: true,
    },
  },
  {
    fault: 'held heartbeat connect',
    step: 'fence',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 1,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat connect',
    step: 'fence',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 1,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat connect',
    step: 'disconnect zero',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat connect',
    step: 'disconnect zero',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat encode',
    step: 'fence',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 1,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat encode',
    step: 'fence',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 1,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat encode',
    step: 'disconnect zero',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat encode',
    step: 'disconnect zero',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat queue',
    step: 'fence',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 1,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat queue',
    step: 'fence',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 1,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat queue',
    step: 'disconnect zero',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat queue',
    step: 'disconnect zero',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat response',
    step: 'fence',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 1,
      oldEffects: 0,
      closingWrites: 1,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat response',
    step: 'fence',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 1,
      oldEffects: 0,
      closingWrites: 1,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat response',
    step: 'disconnect zero',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 1,
      oldEffects: 0,
      closingWrites: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat response',
    step: 'disconnect zero',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 1,
      oldEffects: 0,
      closingWrites: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
]

/** Heartbeat answer that assigns the partitions of topic A and, optionally, of topic U. */
const assignmentAnswer = (call, partitions, { unknown = false, epoch = 1 } = {}) => ({
  errorCode: 0,
  errorMessage: null,
  memberId: call.fields.memberId,
  memberEpoch: epoch,
  heartbeatIntervalMs: 50,
  assignment: {
    topicPartitions: [
      ...(partitions ? [{ topicId: IDS.A, partitions }] : []),
      ...(unknown ? [{ topicId: IDS.U, partitions: [0] }] : []),
    ],
  },
})

const FAULT_CODE = /^heartbeat (\d+)$/
const STEADY = ({ fields }) => fields.memberEpoch > 0
const LEAVE = ({ fields }) => fields.memberEpoch === -1
const A0_H = `A:0@${H}`
const A1_H = `A:1@${H}`

/**
 * Drives one literal membership row through the public share consumer over the scripted cluster.
 * The row's `expected.action` selects the family: a heartbeat fault at a phase, an assignment cell,
 * a held metadata-flight cell or a held write-boundary cell.
 */
class MembershipFaultCase extends FaultCase {
  /**
   * @param {object} row The literal oracle row.
   * @returns {MembershipFaultCase} A case with its fault parsed from the row.
   */
  constructor(row) {
    super(row)
    const coded = FAULT_CODE.exec(row.fault)
    this.code = coded ? Number(coded[1]) : null
    this.transport = /disconnect$|timeout$/.exec(row.fault)?.[0] ?? null
    this.family = { assignment: 'assignment', 'target guard': 'target', guard: 'guard' }[
      row.expected.action
    ]
    this.family ??= 'fault'
    this.requestTimeout = 1000
  }

  /** @returns {Promise<void>} Creates the client with the row's request-timeout configuration. */
  async startClient() {
    const { enforce } = this.row.expected
    await this.start({
      requestTimeout: this.transport === 'timeout' || this.family === 'guard' ? 1000 : 30000,
      enforce: enforce ?? true,
    })
  }

  /**
   * Joins with node 0 holding A0/H, node 1 finishing A1 and A2, and node 1 idle and established.
   * @param {string[]} [held] Records whose handlers wait.
   * @param {number} [calls=5] Handler entries to wait for.
   * @returns {Promise<void>}
   */
  async establish(held = [A0_H], calls = 5) {
    this.supplyOnce('A:0', 'A:1', 'A:2')
    for (const identity of held) this.hold(identity)
    await this.join()
    await until(() => this.calls.length >= calls, 'the original handlers')
    const idle = held.includes(A1_H) ? 0 : 1
    await until(
      () =>
        this.frames(78).some(({ node, fields }) => node === idle && fields.shareSessionEpoch >= 1),
      'the idle node acknowledgement round'
    )
    await flush()
  }

  /**
   * Holds the next frame matching `match`, moves the fake clock to the heartbeat tick that writes it
   * and waits until it is held.
   * @param {(call: object) => boolean} match Selects the heartbeat frame.
   * @param {number} [ms=50] Fake time to the frame.
   * @returns {Promise<object>} The held gate.
   */
  async holdBeat(match, ms = 50) {
    const gate = this.holdWire({ api: 'ShareGroupHeartbeat', match })
    if (ms) await this.pump(ms)
    await this.arrival(gate)
    await flush()
    return gate
  }

  /** @returns {Promise<void>} Reaches the row's phase before the fault. */
  async reach() {
    await this.startClient()
    const { step } = this.row
    if (this.family === 'fault' && step === 'before join') return
    if (this.family === 'fault' || this.family === 'guard') await this.establish()
    if (this.family === 'assignment') {
      const hold = this.row.fault === 'empty assignment' ? [A1_H] : [A0_H]
      await this.establish(hold, this.row.fault === 'empty assignment' ? 3 : 5)
    }
    if (this.family === 'target') await this.establish([A1_H], 3)
  }

  /** @returns {Promise<void>} Installs the row's fault, answer or transport failure. */
  async inject() {
    await this[`inject_${this.family}`]()
  }

  /** @returns {Promise<void>} Verifies the primary outcome. */
  async verify() {
    await this[`verify_${this.family}`]()
  }

  /** @returns {Promise<void>} Proves a current replacement works. */
  async replacement() {
    await this[`replacement_${this.family}`]()
  }

  /** @returns {Promise<void>} The heartbeat fault family: inject. */
  async inject_fault() {
    if (this.row.expected.action === 'ignored') return this.injectIgnored()
    const { step } = this.row
    this.lookupsBefore = this.lookups()
    const answer = this.code === null ? null : { errorCode: this.code }
    if (step === 'before join') {
      const gate = this.armTransport({ api: 'ShareGroupHeartbeat' })
      this.runPromise = this.run()
      await this.arrival(gate)
      this.lookupsBefore = this.lookups()
      this.faultedFrame = await this.release_fault(gate, answer)
      return
    }
    if (step === 'assignment change') {
      this.beatShape = (fields, base) =>
        this.faulted && fields.memberEpoch > 0
          ? { ...base, assignment: { topicPartitions: [{ topicId: IDS.A, partitions: [0, 1] }] } }
          : base
    }
    if (step === 'leave') {
      this.supplyOnce('A:0')
      const gate = this.armTransport({ api: 'ShareGroupHeartbeat', match: LEAVE })
      this.disconnecting = track(this.h.consumer.disconnect({ timeout: 400 }))
      await this.pump(300)
      await this.arrival(gate)
      await flush()
      this.faultedFrame = await this.release_fault(gate, answer)
      return
    }
    this.supplyOnce('A:0')
    const gate = this.armTransport({ api: 'ShareGroupHeartbeat', match: STEADY })
    await this.pump(50)
    await this.arrival(gate)
    await flush()
    this.faultedFrame = await this.release_fault(gate, answer)
  }

  /**
   * @param {object} gate The held heartbeat answer.
   * @param {object|null} answer The coded answer, or null for the row's transport failure.
   * @returns {Promise<object>} The frame the fault hit.
   */
  async release_fault(gate, answer) {
    const call = gate.call
    if (answer) gate.release(answer)
    else await this.fireTransport(this.transport)
    this.faulted = true
    await flush()
    return call
  }

  /** @returns {object} The first heartbeat frame's fields, which fix the run's member identity. */
  firstBeat() {
    return this.frames(76)[0].fields
  }

  /**
   * @param {object} fields A heartbeat request.
   * @param {number} epoch Expected member epoch.
   * @returns {void}
   * @throws {Error} When the frame is not a full-field frame of this run's member.
   */
  assertFullFrame(fields, epoch) {
    assert.equal(fields.memberEpoch, epoch)
    assert.equal(fields.memberId, this.firstBeat().memberId)
    assert.deepEqual(fields.subscribedTopicNames, [NAME])
    assert.equal(fields.rackId, 'rack-a')
  }

  /** @returns {Promise<void>} The heartbeat fault family: primary outcome per action. */
  async verify_fault() {
    const { action } = this.row.expected
    if (action === 'ignored') return this.verifyIgnored()
    if (action === 'left') return this.verifyLeft()
    if (action === 'fatal') return this.verifyFatal()
    return this.verifyRecovered(action === 'rejoin')
  }

  /**
   * Retry and rejoin: the member and run survive, the next frame repeats every field, a coordinator
   * code forgets and looks up the coordinator again, and a fence retires old ordinary work first.
   * @param {boolean} rejoin Whether the fault is a fence code that rejoins at epoch zero.
   * @returns {Promise<void>}
   */
  async verifyRecovered(rejoin) {
    const { step } = this.row
    const before = step === 'before join'
    const faulted = this.frames(76).length
    if (before) assert.equal(this.runState.settled, false, 'the join stays pending')
    const held = this.calls.find((call) => call.identity === A0_H)
    if (!before) assert.equal(held.isRunning(), !rejoin, 'held handler authority')
    await this.advanceUntil(() => this.frames(76).length > faulted, 'the next heartbeat')
    const next = this.frames(76)[faulted].fields
    this.assertFullFrame(next, before || rejoin ? 0 : 1)
    const forgets = [14, 15, 16, 25, 110].includes(this.code)
    assert.equal(this.lookups(), this.lookupsBefore + (forgets ? 1 : 0), 'coordinator lookups')
    assert.equal(this.seen.crashes.length, 0)
    if (before) {
      await until(() => this.runState.settled, 'the first join')
      assert.equal(this.runState.error, null)
      assert.deepEqual(
        new Set(this.joins().map(({ fields }) => fields.memberId)),
        new Set([this.firstBeat().memberId])
      )
    }
    if (rejoin && !before) {
      assert.equal(this.finals().length, this.row.expected.finals)
      assert.throws(() => held.acknowledge('accept'), { message: STALE_CALL })
      assert.equal(this.joins().length, 2)
    } else assert.equal(this.finals().length, 0)
    if (this.row.step === 'assignment change' && !rejoin) {
      await this.advanceUntil(
        () => this.h.consumer.assignment()[0]?.partitions.length === 2,
        'the shrunk assignment'
      )
    }
  }

  /** @returns {Promise<void>} Fatal codes: one crash with restart false and the exact cause. */
  async verifyFatal() {
    const { step, expected } = this.row
    await until(() => this.seen.crashes.length === 1, 'the crash event')
    await flush()
    assert.equal(this.seen.crashes.length, 1)
    const [crash] = this.seen.crashes
    assert.equal(crash.restart, false)
    if (expected.cause === 35) {
      assert.equal(crash.error.reason, 'refused-by-broker')
      assert.equal(crash.error.apiName, 'ShareGroupHeartbeat')
    } else assert(codesOf(crash.error).includes(expected.cause))
    assert.equal(this.restartErrors.length, 1)
    assert.equal(this.restartErrors[0], crash.error)
    if (step === 'before join') {
      await until(() => this.runState.settled, 'the rejected run')
      assert.equal(this.runState.error, crash.error)
      assert.equal(this.frames(76).length, 1)
    } else {
      assert.equal(this.finals().length, expected.finals)
      assert.equal(this.leaves().length <= 1, true)
      const held = this.calls.find((call) => call.identity === A0_H)
      assert.equal(held.isRunning(), false)
      assert.throws(() => held.acknowledge('accept'), { message: STALE_CALL })
    }
    const frames = this.frames(76).length
    await this.pump(this.rounds * 50)
    assert.equal(this.frames(76).length, frames, 'no heartbeat after a fatal verdict')
    assert.equal(this.seen.crashes.length, 1)
  }

  /** @returns {Promise<void>} Leave: one epoch -1 attempt, no rejoin, terminal close, no crash. */
  async verifyLeft() {
    await until(() => this.disconnecting.settled, 'the disconnect')
    assert.equal(this.disconnecting.error, null)
    assert.equal(this.leaves().length, this.row.expected.finals)
    const [leave] = this.leaves()
    assert.equal(leave.fields.subscribedTopicNames, null)
    assert.equal(leave.fields.rackId, null)
    assert.deepEqual(
      this.finals().map(({ node }) => node),
      [1]
    )
    const held = this.calls.find((call) => call.identity === A0_H)
    assert.equal(held.isRunning(), false)
    assert.throws(() => held.acknowledge('accept'), { message: STALE_CALL })
    const frames = this.frames(76).length
    await this.pump(this.rounds * 50)
    assert.equal(this.frames(76).length, frames, 'no rejoin or new heartbeat')
    assert.equal(this.seen.crashes.length, 0)
    assert.equal(this.seen.stops.length, 1)
  }

  /** @returns {Promise<void>} Fault family replacement: current work or a new run control. */
  async replacement_fault() {
    if (this.row.expected.action === 'ignored') return this.replacementIgnored()
    if (this.row.expected.member === 'same') {
      if (this.row.step === 'before join') {
        await until(() => this.calls.length >= 1, 'the first handler')
        return this.releaseAll()
      }
      this.release(A0_H)
      await this.advanceUntil(() => this.acceptedBoth(0), 'the current terminal acceptance')
      return undefined
    }
    return this.newRunControl()
  }

  /**
   * @param {number} partition Partition of topic A.
   * @returns {boolean} Whether both exact records were accepted without error.
   */
  acceptedBoth(partition) {
    const accepted = this.seen.acks.filter((ack) => !ack.error && ack.partition === partition)
    return covers(accepted, H) && covers(accepted, J)
  }

  /** @returns {Promise<void>} A public run after cleanup: new member, one fresh acquisition. */
  async newRunControl() {
    const previous = this.firstBeat().memberId
    const calls = this.calls.length
    this.releaseAll()
    this.supplyOnce('A:0')
    await this.run()
    await until(() => this.calls.length > calls, 'the control delivery')
    assert.notEqual(this.joins().at(-1).fields.memberId, previous)
  }

  /** @returns {void} Lets every held handler return. */
  releaseAll() {
    for (const { gate } of this.heldGates) gate.resolve()
  }

  /** @returns {Promise<void>} Assignment cells: inject. */
  async inject_assignment() {
    const { fault } = this.row
    this.metadataBefore = this.cluster.requests().filter(({ api }) => api === 'Metadata').length
    this.assignedBefore = this.seen.assigned.length
    if (fault === 'unknown assignment') {
      this.h.table.set('late-topic', {
        topicId: IDS.U,
        partitions: [{ leader: 0, epoch: 1 }],
      })
      this.supplyOnce('U:0')
      this.flight = this.holdWire({
        api: 'Metadata',
        match: ({ fields }) => (fields.topics || []).some((topic) => topic.topicId === IDS.U),
      })
    }
    const gate = await this.holdBeat(STEADY)
    const { call } = gate
    if (fault === 'unknown assignment')
      gate.release(assignmentAnswer(call, [0, 1, 2], { unknown: true }))
    if (fault === 'empty assignment') gate.release(assignmentAnswer(call, null))
    if (fault === 'equal assignment') gate.release(assignmentAnswer(call, [0, 1, 2]))
    await flush()
  }

  /** @returns {Promise<void>} @returns {string[]} The public assignment as sorted alias keys. */
  assignedKeys() {
    return this.h.consumer
      .assignment()
      .flatMap(({ topic, partitions }) =>
        partitions.map((partition) => `${ALIAS.get(this.h.table.get(topic).topicId)}:${partition}`)
      )
      .sort()
  }

  /** @returns {Promise<void>} Assignment cells: primary outcome. */
  async verify_assignment() {
    const { fault, expected } = this.row
    if (fault === 'unknown assignment') {
      await this.arrival(this.flight)
      assert.deepEqual(this.assignedKeys(), expected.assignment, 'known partitions stay usable')
      this.flight.release()
      await until(() => this.assignedKeys().includes('U:0'), 'the resolved topic')
      assert.deepEqual(this.assignedKeys(), expected.resolved)
      await until(() => this.calls.some((call) => call.identity === `U:0@${H}`), 'the U handler')
      await flush()
      assert.equal(this.calls.filter((call) => call.identity === `U:0@${H}`).length, 1)
      const refreshes = this.cluster
        .requests()
        .filter(
          ({ api, fields }) =>
            api === 'Metadata' && (fields.topics || []).some((topic) => topic.topicId === IDS.U)
        )
      assert.equal(refreshes.length, expected.refresh)
      return
    }
    if (fault === 'empty assignment') {
      assert.deepEqual(this.h.consumer.assignment(), [])
      assert.equal(this.seen.assigned.length, this.assignedBefore + expected.changes)
      assert.equal(
        this.calls.some((call) => call.alias === 'A' && call.partition === 2),
        false,
        'revoked partition starts no handler'
      )
      this.release(A1_H)
      const released = () =>
        this.frames(78, 79).some((frame) =>
          (frame.fields.topics || []).some((topic) =>
            topic.partitions.some(
              (partition) =>
                partition.partitionIndex === 2 &&
                partition.acknowledgementBatches.some(
                  (batch) =>
                    batch.firstOffset === H &&
                    batch.lastOffset === J &&
                    batch.acknowledgeTypes.every((type) => type === 2)
                )
            )
          )
        )
      await this.advanceUntil(released, 'the release of the unstarted A2 records')
      assert.equal(
        this.calls.some((call) => call.alias === 'A' && call.partition === 2),
        false
      )
      return
    }
    await flush()
    assert.equal(this.seen.assigned.length, this.assignedBefore + expected.changes)
    assert.deepEqual(this.assignedKeys(), expected.assignment)
    assert.equal(
      this.cluster.requests().filter(({ api }) => api === 'Metadata').length,
      this.metadataBefore,
      'an equal target starts no metadata work'
    )
    const gate = await this.holdBeat(STEADY)
    gate.release({ errorCode: 0, assignment: null, memberEpoch: 1, heartbeatIntervalMs: 50 })
    await flush()
    assert.equal(this.seen.assigned.length, this.assignedBefore)
    assert.deepEqual(this.assignedKeys(), expected.assignment)
  }

  /** @returns {Promise<void>} Assignment cells: a current replacement acquisition. */
  async replacement_assignment() {
    if (this.row.fault !== 'equal assignment') {
      this.releaseAll()
      await flush()
      return
    }
    this.release(A0_H)
    await until(
      () => this.seen.acks.some((ack) => !ack.error && ack.partition === 0 && covers([ack], J)),
      'the current terminal acceptance'
    )
  }

  /**
   * Metadata-flight cells: an old flight of an unresolved topic is held while the membership or
   * target is replaced, then released late.
   * @returns {Promise<void>}
   */
  async inject_target() {
    const { fault } = this.row
    this.h.table.set('late-topic', { topicId: IDS.U, partitions: [{ leader: 0, epoch: 1 }] })
    this.flight = this.holdWire({
      api: 'Metadata',
      match: ({ fields }) => (fields.topics || []).some((topic) => topic.topicId === IDS.U),
    })
    const gate = await this.holdBeat(STEADY)
    gate.release(assignmentAnswer(gate.call, [0, 1, 2], { unknown: true }))
    await this.arrival(this.flight)
    await flush()
    this.assignedAtFlight = this.seen.assigned.length
    this.callsAtFlight = this.calls.length
    this.assignedKeysAtFlight = this.assignedKeys()
    const supersede = {
      'held bulk metadata': () => this.supersedeByStop(),
      'held per-id metadata': () => this.supersedeByFence(),
      'held by-name metadata': () => this.supersedeByFence(true),
      'held unresolved publication': () => this.supersedeByRecreate(),
      'equal epoch old answer': () => this.supersedeByStop(),
      'lower epoch answer': () => this.lowerEpochAnswer(),
      'known ids while unknown retries': () => this.repeatUnknown(false),
      'same target preserves backoff': () => this.repeatUnknown(true),
    }[fault]
    await supersede()
  }

  /** @returns {Promise<void>} Retires the membership by a zero-deadline disconnect. */
  async supersedeByStop() {
    this.supersession = 'stop'
    this.disconnecting = track(this.h.consumer.disconnect({ timeout: 0 }))
    await until(() => this.disconnecting.settled, 'the disconnect')
  }

  /**
   * @param {boolean} [rejoined] Whether to wait for the rejoin before the old flight is released.
   * @returns {Promise<void>} Fences the membership with code 110.
   */
  async supersedeByFence(rejoined = false) {
    this.supersession = rejoined ? 'rejoined' : 'fence'
    const gate = await this.holdBeat(STEADY)
    gate.release({ errorCode: 110 })
    await flush()
    if (rejoined) await this.advanceUntil(() => this.joins().length === 2, 'the rejoin')
  }

  /** @returns {Promise<void>} Recreates the topic under the same name while the flight is held. */
  async supersedeByRecreate() {
    this.h.table.set(NAME, { topicId: IDS.B, partitions: [{ leader: 0, epoch: 2 }] })
    this.supplyOnce('B:0')
    const gate = await this.holdBeat(STEADY)
    gate.release(assignmentAnswer(gate.call, null, { epoch: 2 }))
    await flush()
  }

  /** @returns {Promise<void>} A lower-epoch answer changes nothing before the flight is released. */
  async lowerEpochAnswer() {
    const gate = await this.holdBeat(STEADY)
    gate.release(assignmentAnswer(gate.call, [0], { epoch: 0 }))
    await flush()
    assert.deepEqual(this.assignedKeys(), this.assignedKeysAtFlight)
    assert.equal(this.seen.assigned.length, this.assignedAtFlight)
  }

  /**
   * @param {boolean} same Whether every repeated answer carries the identical target.
   * @returns {Promise<void>} Repeats an unresolved target; only one metadata flight may exist while
   *   the first is pending, and the known partitions keep delivering.
   */
  async repeatUnknown(same) {
    const flights = () =>
      this.cluster
        .requests()
        .filter(
          ({ api, fields }) =>
            api === 'Metadata' && (fields.topics || []).some((topic) => topic.topicId === IDS.U)
        ).length
    const before = flights()
    for (const known of same
      ? [
          [0, 1, 2],
          [0, 1, 2],
          [0, 1, 2],
        ]
      : [
          [0, 1, 2],
          [0, 1],
          [0, 1, 2],
        ]) {
      const gate = await this.holdBeat(STEADY)
      gate.release(assignmentAnswer(gate.call, known, { unknown: true }))
      await flush()
    }
    assert.equal(flights(), before, 'one metadata flight while the first is pending')
    assert(this.calls.every((call) => call.alias !== 'U'))
    await this.supersedeByRecreate()
  }

  /** @returns {Promise<void>} Metadata-flight cells: the late old flight has no effect. */
  async verify_target() {
    const { expected } = this.row
    const assigned = this.seen.assigned.length
    const callsBefore = this.calls.length
    if (this.flight.settle) this.flight.release()
    await flush()
    assert.equal(
      this.calls.slice(callsBefore).filter((call) => call.alias === 'U').length,
      expected.oldEffects,
      'the late flight starts no handler'
    )
    if (this.supersession) {
      assert.equal(this.seen.assigned.length, assigned, 'the late flight publishes nothing')
    }
    assert.equal(this.seen.crashes.length, 0)
    const old = this.calls.find((call) => call.identity === expected.old[0])
    assert(old, 'the old identity was delivered')
    if (this.supersession) assert.throws(() => old.acknowledge('accept'), { message: STALE_CALL })
  }

  /** @returns {Promise<void>} Metadata-flight cells: B0/H arrives under a distinct target. */
  async replacement_target() {
    const { expected } = this.row
    const identity = expected.current[0]
    this.h.table.set(NAME, { topicId: IDS.B, partitions: [{ leader: 0, epoch: 2 }] })
    this.supplyOnce('B:0')
    if (this.supersession === 'stop') await this.run()
    else if (this.supersession !== 'fence') {
      const gate = await this.holdBeat(STEADY)
      gate.release(assignmentAnswer(gate.call, null, { epoch: 2 }))
    }
    const delivered = () => this.calls.some((call) => call.identity === identity)
    await this.advanceUntil(delivered, 'the replacement delivery')
    await flush()
    assert.equal(this.calls.filter((call) => call.identity === identity).length, 1)
    assert.equal(expected.target, 'distinct')
    assert.notEqual(this.h.table.get(NAME).topicId, IDS.A)
    this.releaseAll()
  }

  /** @returns {Promise<void>} Write-boundary cells: hold the next heartbeat at its stage. */
  async inject_guard() {
    this.steadyBefore = this.steadyFrames()
    this.boundary = this.row.fault.replace('held heartbeat ', '')
    this.pending = this.installBoundary(this.boundary)
    await this.pump(50)
    await until(() => this.pending.reached(), `the ${this.boundary} boundary`)
    await flush()
    if (this.row.step === 'fence') {
      this.supersession = 'fence'
      this.h.cluster.answer('ShareFetch', (fields) =>
        fields.shareSessionEpoch >= 1 && !this.fenced
          ? ((this.fenced = true), topError(25))
          : this.fetchAnswerFor(fields)
      )
      await this.pump(50)
      await until(() => this.fenced, 'the fence answer')
      await flush()
    } else {
      this.supersession = 'stop'
      this.disconnecting = track(this.h.consumer.disconnect({ timeout: 0 }))
      await until(() => this.disconnecting.settled, 'the zero-deadline disconnect')
    }
  }

  /** @returns {number} Steady heartbeat frames written so far. */
  steadyFrames() {
    return this.frames(76).filter(({ fields }) => fields.memberEpoch > 0).length
  }

  /**
   * @param {'connect'|'encode'|'queue'|'response'} boundary The write stage to hold.
   * @returns {{reached: () => boolean, release: () => void}} The hold.
   */
  installBoundary(boundary) {
    let reached = false
    if (boundary === 'response') {
      const wire = this.holdWire({ api: 'ShareGroupHeartbeat', match: STEADY })
      wire.arrived().then(() => {
        reached = true
      })
      return { reached: () => reached, release: () => wire.settle && wire.release() }
    }
    const gate = deferred()
    let taken = false
    const take = () => {
      if (taken) return false
      taken = true
      reached = true
      return true
    }
    if (boundary === 'connect') {
      const original = ConnectionPool.prototype.getConnectionByRequest
      jest.spyOn(ConnectionPool.prototype, 'getConnectionByRequest').mockImplementation(function (
        protocolRequest,
        ...rest
      ) {
        if (protocolRequest?.request?.apiKey !== 76 || !take())
          return original.call(this, protocolRequest, ...rest)
        return gate.promise.then(() => original.call(this, protocolRequest, ...rest))
      })
    } else if (boundary === 'encode') {
      const original = requests.ShareGroupHeartbeat.protocol
      jest.spyOn(requests.ShareGroupHeartbeat, 'protocol').mockImplementation((selection) => {
        const factory = original(selection)
        return (options) => {
          const built = factory(options)
          if (!(options?.memberEpoch > 0) || taken) return built
          const { encode } = built.request
          return {
            ...built,
            request: {
              ...built.request,
              encode: async () => {
                take()
                await gate.promise
                return encode()
              },
            },
          }
        }
      })
    } else {
      const original = RequestQueue.prototype.push
      jest.spyOn(RequestQueue.prototype, 'push').mockImplementation(function (pushed) {
        if (pushed.entry.apiKey !== 76 || !take()) return original.call(this, pushed)
        gate.promise.then(() => {
          try {
            original.call(this, pushed)
          } catch (error) {
            pushed.entry.reject(error)
          }
        })
        return undefined
      })
    }
    this.heldGates.push({ identity: `boundary:${boundary}`, gate })
    return { reached: () => reached, release: () => gate.resolve() }
  }

  /** @returns {Promise<void>} Write-boundary cells: old frames, closing frames, late effects. */
  async verify_guard() {
    const { expected } = this.row
    this.pending.release()
    await flush()
    assert.equal(
      this.steadyFrames() - this.steadyBefore,
      expected.oldWrites,
      'old frames written for the held boundary'
    )
    assert.equal(this.finals().length, expected.closingWrites, 'closing frames')
    assert.equal(this.seen.crashes.length, 0)
  }

  /** @returns {Promise<void>} Write-boundary cells: the replacement membership delivers B0/H. */
  async replacement_guard() {
    const identity = this.row.expected.replacement[0]
    this.h.table.set(NAME, { topicId: IDS.B, partitions: [{ leader: 0, epoch: 2 }] })
    this.supplyOnce('B:0')
    if (this.supersession === 'fence')
      await this.advanceUntil(() => this.joins().length >= 2, 'the rejoin')
    else await this.run()
    await this.advanceUntil(
      () => this.calls.some((call) => call.identity === identity),
      'the replacement delivery'
    )
    await flush()
    assert.equal(this.calls.filter((call) => call.identity === identity).length, 1)
    this.releaseAll()
  }

  /**
   * A superseded or already-left membership answered with code 69: the late answer is ignored.
   * @returns {Promise<void>}
   */
  async injectIgnored() {
    const gate = await this.holdBeat(STEADY)
    this.lateGate = gate
    if (this.row.fault.endsWith('unsubscribed')) {
      this.supersession = 'stop'
      this.disconnecting = track(this.h.consumer.disconnect({ timeout: 400 }))
      await this.pump(300)
      await until(() => this.disconnecting.settled, 'the leave')
    } else {
      this.supersession = 'fence'
      this.h.cluster.answer('ShareFetch', (fields) =>
        fields.shareSessionEpoch >= 1 && !this.fenced
          ? ((this.fenced = true), topError(25))
          : this.fetchAnswerFor(fields)
      )
      await this.pump(50)
      await until(() => this.fenced, 'the fence answer')
      await flush()
    }
    this.framesBefore = this.frames(76).length
    gate.release({ errorCode: 69 })
    await flush()
  }

  /** @returns {Promise<void>} The late 69 changes nothing: no crash, no restart, no extra frame. */
  async verifyIgnored() {
    assert.equal(this.seen.crashes.length, 0)
    assert.equal(this.restartErrors.length, 0)
    if (this.supersession === 'stop') {
      assert.equal(this.frames(76).length, this.framesBefore, 'no rejoin after the leave')
      assert.equal(this.leaves().length, 1)
      assert.equal(this.seen.stops.length, 1)
    } else {
      await this.advanceUntil(() => this.joins().length === 2, 'the replacement membership')
      assert.equal(this.joins()[1].fields.memberId, this.firstBeat().memberId)
      assert.equal(this.seen.crashes.length, 0)
    }
  }

  /** @returns {Promise<void>} After an ignored 69 a current membership or run still works. */
  async replacementIgnored() {
    if (this.supersession === 'stop') return this.newRunControl()
    this.supplyOnce('A:0')
    const calls = this.calls.filter((call) => call.identity === A0_H).length
    await this.advanceUntil(
      () => this.calls.filter((call) => call.identity === A0_H).length > calls,
      'the replacement delivery'
    )
    this.releaseAll()
    return undefined
  }

  /** @returns {Promise<void>} Replays frames, handlers and results through the audit. */
  async assertInvariants() {
    this.replay()
    if (this.auditFailures.length) throw this.auditFailures[0]
    assert.deepEqual(this.unhandled, [])
  }
}

describe('share membership faults', () => {
  afterEach(() => {
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  it.each(rows)('$fault at $step (row $#)', async (row) => {
    const scenario = new MembershipFaultCase(row)
    const failures = []
    try {
      await scenario.reach()
      await scenario.inject()
      await scenario.verify()
      await scenario.replacement()
      await scenario.assertInvariants()
    } catch (error) {
      failures.push(error)
    }
    for (const finish of [() => scenario.dispose(), () => scenario.assertBaseline()]) {
      try {
        await finish()
      } catch (error) {
        failures.push(error)
      }
    }
    try {
      scenario.cluster?.assertClean()
    } catch (error) {
      failures.push(error)
    }
    if (failures.length) throw new AggregateError(failures, 'Share fault case failed')
  })
})

describe('membership fault observations', () => {
  test('retains every literal scenario once', () => {
    expect(rows).toHaveLength(81)
    expect(
      new Set(rows.map((row) => `${row.fault} / ${row.step} / ${row.expected.enforce ?? ''}`)).size
    ).toBe(81)
    expect(rows.every((row) => row.expected.rounds >= 8)).toBe(true)
  })

  test('rejects concurrent handlers of the same exact record', () => {
    const audit = new FaultAudit()
    const call = Symbol('call')
    audit.handler('A:0@9007199254740993', call, true)
    expect(() => audit.handler('A:0@9007199254740993', Symbol('replacement'), true)).toThrow(
      'concurrent'
    )
    audit.handler('A:0@9007199254740993', call, false)
    audit.handler('A:0@9007199254740993', Symbol('next'), true)
  })
  test('does not confuse adjacent offsets above the safe integer limit', () => {
    const audit = new FaultAudit()
    const first = Symbol('first')
    const second = Symbol('second')
    audit.handler('A:0@9007199254740993', first, true)
    audit.handler('A:0@9007199254740994', second, true)
    expect(audit.handlers.size).toBe(2)
    expect(() => audit.range(Number('9007199254740993'), '9007199254740993')).toThrow()
  })
  test('refuses an ordinary frame after membership retirement', () => {
    const audit = new FaultAudit()
    expect(() => audit.frame({ apiKey: 76, fields: {} }, { allowed: false })).toThrow(
      'unauthorized'
    )
    expect(() => audit.frame({ apiKey: 76, fields: {} }, { allowed: true })).not.toThrow()
  })
  test('rejects a completion from another invocation', () => {
    const audit = new FaultAudit()
    audit.handler('A:0@9007199254740993', Symbol('first'), true)
    expect(() => audit.handler('A:0@9007199254740993', Symbol('second'), false)).toThrow()
  })
})
