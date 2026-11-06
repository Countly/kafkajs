const assert = require('node:assert/strict')
const {
  acquired,
  ackAnswer,
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

/**
 * @param {{fields: object}} frame A recorded ShareFetch or ShareAcknowledge frame.
 * @returns {object[]} The acknowledgement batches it carried, in wire order.
 */
const batchesOf = ({ fields }) =>
  (fields.topics || []).flatMap((topic) =>
    topic.partitions.flatMap((partition) => partition.acknowledgementBatches)
  )

const A0_H = `A:0@${H}`
const A1_H = `A:1@${H}`
const NODE_ENDPOINT = { nodeId: 2, host: 'node2', port: 9092, rack: null }
const RETRY_250 = {
  initialRetryTime: 250,
  maxRetryTime: 250,
  multiplier: 1,
  factor: 0,
  retries: 100,
}

/**
 * @param {object} fields A decoded ShareFetch or ShareAcknowledge request.
 * @returns {Array<{topicId: string, partition: number}>} Every partition it lists, in wire order.
 */
const listed = (fields) =>
  (fields.topics || []).flatMap((topic) =>
    topic.partitions.map(({ partitionIndex }) => ({
      topicId: topic.topicId,
      partition: partitionIndex,
    }))
  )

/**
 * A fault answer for one share frame: the literal broker response a row injects. Partition 1 of
 * topic A is the selected partition; every other listed partition answers quietly, so a fault never
 * hides behind an omitted sibling unless the row asks for it.
 * @param {78|79} api The frame's API key.
 * @param {object} fields The frame's decoded fields.
 * @param {{code?: number, partition?: number, ack?: number, omit?: boolean, duplicate?: boolean}} spec
 *   `code` is a top-level error; `partition` is the selected partition's error (the fetch error
 *   for API 78, the acknowledgement error for API 79); `ack` is a ShareFetch acknowledge error.
 * @returns {object} The answer object for the scripted cluster.
 */
const faultAnswer = (api, fields, { code, partition, ack, omit, duplicate, target = 1 }) => {
  if (code !== undefined) return api === 78 ? topError(code) : ackAnswer([], { errorCode: code })
  const rows = listed(fields).filter(({ topicId, partition: index }) => {
    return !(omit && topicId === IDS.A && index === target)
  })
  const selected = (row) => row.topicId === IDS.A && row.partition === target
  const build = (row) => {
    if (api === 79) return { ...row, code: selected(row) ? partition : 0 }
    return part({
      topicId: row.topicId,
      partition: row.partition,
      code: selected(row) && ack === undefined ? partition : 0,
      ack: selected(row) ? (ack ?? 0) : 0,
      leader:
        selected(row) && (partition === 6 || partition === 74)
          ? { leaderId: 2, leaderEpoch: 8 }
          : { leaderId: -1, leaderEpoch: -1 },
    })
  }
  const answered = rows.map(build)
  if (duplicate) answered.push(...rows.filter(selected).map(build))
  const extra = partition === 6 || partition === 74 ? { nodeEndpoints: [NODE_ENDPOINT] } : {}
  return api === 78 ? fetchAnswer(answered, extra) : ackAnswer(answered, extra)
}

/**
 * The shared driver of the session and acknowledgement fault rows: one affected frame of node 1
 * (or of node 0 for a renewal of its held handler) is held at the wire, answered with the row's
 * fault, and the effects are read from the public events and the recorded frames.
 */
class DataFaultCase extends FaultCase {
  /**
   * @param {object} row The literal oracle row.
   * @returns {DataFaultCase} A case with no client yet.
   */
  constructor(row) {
    super(row)
    this.lockMs = 15000
    this.spec = null
    this.target = 1
  }

  /**
   * @param {object} fields A decoded ShareFetch request.
   * @returns {object} Records for supplied partitions, with the case's lock duration.
   */
  fetchAnswerFor(fields) {
    const answer = super.fetchAnswerFor(fields)
    return { ...answer, acquisitionLockTimeoutMs: this.lockMs }
  }

  /**
   * Starts the client and the run: node 0 holds A0/H, node 1 serves A1 and A2.
   * @param {object} [options] Case options.
   * @param {string[]} [options.hold] Records whose handlers wait.
   * @param {number} [options.expectCalls] Handler entries to wait for.
   * @param {object} [options.startOptions] Options of {@link FaultCase#start}.
   * @param {string[]} [options.supply] Partitions that get one acquisition of H and J.
   * @returns {Promise<void>}
   */
  async begin({
    hold = [A0_H],
    expectCalls = 5,
    startOptions = {},
    supply = ['A:0', 'A:1', 'A:2'],
  } = {}) {
    await this.start({ retry: RETRY_250, ...startOptions })
    await this.armBeforeJoin?.()
    this.supplyOnce(...supply)
    for (const identity of hold) this.hold(identity)
    await this.join()
    if (expectCalls) await until(() => this.calls.length >= expectCalls, 'the original handlers')
    await flush()
  }

  /**
   * @param {string} identity An identity such as `A:0@9007199254740993`.
   * @returns {object|undefined} The first call that delivered it.
   */
  callOf(identity) {
    return this.calls.find((call) => call.identity === identity)
  }

  /**
   * Holds one frame at the wire.
   * @param {78|79} api The frame's API key.
   * @param {(call: object) => boolean} match Selects the frame.
   * @returns {object} The held gate.
   */
  holdFrame(api, match) {
    return this.holdWire({ api: api === 78 ? 'ShareFetch' : 'ShareAcknowledge', match })
  }

  /**
   * Ends a held frame: a typed transport failure, or the literal fault answer.
   * @param {object} gate The held gate.
   * @param {object} spec The fault spec: a transport kind or an answer spec.
   * @returns {Promise<void>}
   */
  async fire(gate, spec) {
    const call = await this.arrival(gate)
    await flush()
    if (spec.transport) {
      this.transportGate = gate
      await this.fireTransport(spec.transport)
    } else gate.release(faultAnswer(call.apiKey, call.fields, { target: this.target, ...spec }))
    await flush()
  }

  /**
   * @param {number} partition Partition of topic A.
   * @returns {object[]} The public acknowledge results of that partition, oldest first.
   */
  resultsOf(partition) {
    return this.seen.acks.filter((ack) => ack.partition === partition)
  }

  /**
   * @param {number} partition Partition of topic A.
   * @param {string} offset An exact offset.
   * @returns {object[]} The terminal results covering the offset.
   */
  terminalFor(partition, offset) {
    return this.resultsOf(partition).filter((ack) => ack.type !== 'renew' && covers([ack], offset))
  }

  /**
   * Asserts the latest terminal result of the selected record, with its reason and original cause;
   * the audit proves each acquisition settled once.
   * @param {number} partition Partition of topic A.
   * @param {string} offset An exact offset.
   * @param {'accepted'|'session-lost'|'rejected-by-broker'|'membership-lost'} result Expected.
   * @param {number|string|null|undefined} cause Expected protocol code, when numeric.
   * @returns {void}
   */
  assertResult(partition, offset, result, cause) {
    const terminal = this.terminalFor(partition, offset)
    assert(terminal.length >= 1, `a terminal result of ${partition}@${offset}`)
    const outcome = terminal.at(-1)
    if (result === 'accepted') return assert.equal(outcome.error, null)
    assert.equal(outcome.error.reason, result)
    if (typeof cause === 'number') assert(codesOf(outcome.error).includes(cause), 'cause code')
    return undefined
  }

  /**
   * @param {number} node A node id.
   * @returns {object[]} The ShareFetch and ShareAcknowledge frames written to the node.
   */
  frameLog(node) {
    return this.frames(78, 79).filter((frame) => frame.node === node)
  }

  /**
   * @param {number} node A node id.
   * @param {number} from How many frames of that node existed at the fault.
   * @returns {object|undefined} The first frame of the node after the fault.
   */
  nextFrame(node, from) {
    return this.frameLog(node)[from]
  }
}

/** Literal fault, phase and outcome rows; the runtime driver must not mutate this oracle. */
const rows = [
  {
    fault: 'partition 121',
    step: 'flush; held handler',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 121,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
  },
  {
    fault: 'partition 121',
    step: 'flush; held handler and revoked partition',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 121,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'partition 6',
    step: 'flush; held handler',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 6,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
  },
  {
    fault: 'partition 6',
    step: 'flush; held handler and revoked partition',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 6,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'partition 100',
    step: 'flush; held handler',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 100,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
  },
  {
    fault: 'partition 100',
    step: 'flush; held handler and revoked partition',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 100,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'top 122',
    step: 'flush; held handler',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 122,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
  },
  {
    fault: 'top 122',
    step: 'flush; held handler and revoked partition',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 122,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
  },
  {
    fault: 'top 123',
    step: 'flush; held handler',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 123,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
  },
  {
    fault: 'top 123',
    step: 'flush; held handler and revoked partition',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 123,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
  },
  {
    fault: 'top 133',
    step: 'flush; held handler',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 133,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
  },
  {
    fault: 'top 133',
    step: 'flush; held handler and revoked partition',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 133,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'disconnect',
    step: 'flush; held handler',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
  },
  {
    fault: 'disconnect',
    step: 'flush; held handler and revoked partition',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
  },
  {
    fault: 'timeout',
    step: 'flush; held handler',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
  },
  {
    fault: 'timeout',
    step: 'flush; held handler and revoked partition',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
  },
  {
    fault: 'partition 121',
    step: 'piggyback; held handler',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 121,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
  },
  {
    fault: 'partition 121',
    step: 'piggyback; held handler and revoked partition',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 121,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'partition 6',
    step: 'piggyback; held handler',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 6,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
  },
  {
    fault: 'partition 6',
    step: 'piggyback; held handler and revoked partition',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 6,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'partition 100',
    step: 'piggyback; held handler',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 100,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
  },
  {
    fault: 'partition 100',
    step: 'piggyback; held handler and revoked partition',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 100,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'top 122',
    step: 'piggyback; held handler',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 122,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
  },
  {
    fault: 'top 122',
    step: 'piggyback; held handler and revoked partition',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 122,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
  },
  {
    fault: 'top 123',
    step: 'piggyback; held handler',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 123,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
  },
  {
    fault: 'top 123',
    step: 'piggyback; held handler and revoked partition',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 123,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
  },
  {
    fault: 'top 133',
    step: 'piggyback; held handler',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 133,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
  },
  {
    fault: 'top 133',
    step: 'piggyback; held handler and revoked partition',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 133,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
  },
  {
    fault: 'disconnect',
    step: 'piggyback; held handler',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
  },
  {
    fault: 'disconnect',
    step: 'piggyback; held handler and revoked partition',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
  },
  {
    fault: 'timeout',
    step: 'piggyback; held handler',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
  },
  {
    fault: 'timeout',
    step: 'piggyback; held handler and revoked partition',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
  },
  {
    fault: 'partition 121',
    step: 'close; held handler',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 121,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: null,
    },
  },
  {
    fault: 'partition 121',
    step: 'close; held handler and revoked partition',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 121,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: null,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'partition 6',
    step: 'close; held handler',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'session-lost',
      cause: 6,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: null,
    },
  },
  {
    fault: 'partition 6',
    step: 'close; held handler and revoked partition',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'session-lost',
      cause: 6,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: null,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'partition 100',
    step: 'close; held handler',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 100,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: null,
    },
  },
  {
    fault: 'partition 100',
    step: 'close; held handler and revoked partition',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 100,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: null,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'top 122',
    step: 'close; held handler',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 122,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: null,
    },
  },
  {
    fault: 'top 122',
    step: 'close; held handler and revoked partition',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 122,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: null,
      revokedType: 'release',
      revokedResult: 'rejected-by-broker',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'top 123',
    step: 'close; held handler',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 123,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: null,
    },
  },
  {
    fault: 'top 123',
    step: 'close; held handler and revoked partition',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 123,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: null,
      revokedType: 'release',
      revokedResult: 'rejected-by-broker',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'top 133',
    step: 'close; held handler',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 133,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: null,
    },
  },
  {
    fault: 'top 133',
    step: 'close; held handler and revoked partition',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 133,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: null,
      revokedType: 'release',
      revokedResult: 'rejected-by-broker',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'disconnect',
    step: 'close; held handler',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: null,
    },
  },
  {
    fault: 'disconnect',
    step: 'close; held handler and revoked partition',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: null,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'timeout',
    step: 'close; held handler',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: null,
    },
  },
  {
    fault: 'timeout',
    step: 'close; held handler and revoked partition',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: null,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'partition 121',
    step: 'renew; held handler',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 121,
      reset: false,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
  },
  {
    fault: 'partition 121',
    step: 'renew; held handler and revoked partition',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 121,
      reset: false,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'partition 6',
    step: 'renew; held handler',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 6,
      reset: false,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
  },
  {
    fault: 'partition 6',
    step: 'renew; held handler and revoked partition',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 6,
      reset: false,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'partition 100',
    step: 'renew; held handler',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 100,
      reset: false,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
  },
  {
    fault: 'partition 100',
    step: 'renew; held handler and revoked partition',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 100,
      reset: false,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'top 122',
    step: 'renew; held handler',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 122,
      reset: true,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
  },
  {
    fault: 'top 122',
    step: 'renew; held handler and revoked partition',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 122,
      reset: true,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
  },
  {
    fault: 'top 123',
    step: 'renew; held handler',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 123,
      reset: true,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
  },
  {
    fault: 'top 123',
    step: 'renew; held handler and revoked partition',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 123,
      reset: true,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
  },
  {
    fault: 'top 133',
    step: 'renew; held handler',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 133,
      reset: false,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
  },
  {
    fault: 'top 133',
    step: 'renew; held handler and revoked partition',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 133,
      reset: false,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
  },
  {
    fault: 'disconnect',
    step: 'renew; held handler',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
  },
  {
    fault: 'disconnect',
    step: 'renew; held handler and revoked partition',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
  },
  {
    fault: 'timeout',
    step: 'renew; held handler',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
  },
  {
    fault: 'timeout',
    step: 'renew; held handler and revoked partition',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
  },
  {
    fault: 'partition 74',
    step: 'flush',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'session-lost',
      cause: 74,
    },
  },
  {
    fault: 'partition unknown',
    step: 'flush',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: -1,
    },
  },
  {
    fault: 'omitted partition',
    step: 'flush',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 'invalid response',
    },
  },
  {
    fault: 'duplicate partition',
    step: 'flush',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 'invalid response',
    },
  },
  {
    fault: 'top 35',
    step: 'flush',
    expected: {
      action: 'flush',
      state: 'FATAL',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 35,
    },
  },
  {
    fault: 'top 30',
    step: 'flush',
    expected: {
      action: 'flush',
      state: 'FATAL',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 30,
    },
  },
  {
    fault: 'partition 74',
    step: 'piggyback',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'session-lost',
      cause: 74,
    },
  },
  {
    fault: 'partition unknown',
    step: 'piggyback',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: -1,
    },
  },
  {
    fault: 'omitted partition',
    step: 'piggyback',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 'invalid response',
    },
  },
  {
    fault: 'duplicate partition',
    step: 'piggyback',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 'invalid response',
    },
  },
  {
    fault: 'top 35',
    step: 'piggyback',
    expected: {
      action: 'piggyback',
      state: 'FATAL',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 35,
    },
  },
  {
    fault: 'top 30',
    step: 'piggyback',
    expected: {
      action: 'piggyback',
      state: 'FATAL',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 30,
    },
  },
  {
    fault: 'partition 74',
    step: 'close',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'session-lost',
      cause: 74,
    },
  },
  {
    fault: 'partition unknown',
    step: 'close',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: -1,
    },
  },
  {
    fault: 'omitted partition',
    step: 'close',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 'invalid response',
    },
  },
  {
    fault: 'duplicate partition',
    step: 'close',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 'invalid response',
    },
  },
  {
    fault: 'top 35',
    step: 'close',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 35,
    },
  },
  {
    fault: 'top 30',
    step: 'close',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 30,
    },
  },
  {
    fault: 'partition 74',
    step: 'renew',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'session-lost',
      cause: 74,
    },
  },
  {
    fault: 'partition unknown',
    step: 'renew',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: -1,
    },
  },
  {
    fault: 'omitted partition',
    step: 'renew',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 'invalid response',
    },
  },
  {
    fault: 'duplicate partition',
    step: 'renew',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 'invalid response',
    },
  },
  {
    fault: 'top 35',
    step: 'renew',
    expected: {
      action: 'renew',
      state: 'FATAL',
      rounds: 8,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 35,
    },
  },
  {
    fault: 'top 30',
    step: 'renew',
    expected: {
      action: 'renew',
      state: 'FATAL',
      rounds: 8,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 30,
    },
  },
  {
    fault: 'successful ack failed fetch',
    step: 'piggyback',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'accepted',
      cause: null,
      fetchCode: 6,
    },
  },
  {
    fault: 'failed ack successful fetch',
    step: 'piggyback',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 121,
      fetchCode: 0,
    },
  },
  {
    fault: 'held acknowledgement connect',
    step: 'ordinary fenced',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement connect',
    step: 'ordinary fenced',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement connect',
    step: 'captured current',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 1,
      accepted: true,
      finals: 1,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement connect',
    step: 'captured current',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 1,
      accepted: true,
      finals: 1,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement connect',
    step: 'captured revoked',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement connect',
    step: 'captured revoked',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement connect',
    step: 'captured expired',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement connect',
    step: 'captured expired',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement encode',
    step: 'ordinary fenced',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement encode',
    step: 'ordinary fenced',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement encode',
    step: 'captured current',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 1,
      accepted: true,
      finals: 1,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement encode',
    step: 'captured current',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 1,
      accepted: true,
      finals: 1,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement encode',
    step: 'captured revoked',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement encode',
    step: 'captured revoked',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement encode',
    step: 'captured expired',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement encode',
    step: 'captured expired',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement queue',
    step: 'ordinary fenced',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement queue',
    step: 'ordinary fenced',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement queue',
    step: 'captured current',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 1,
      accepted: true,
      finals: 1,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement queue',
    step: 'captured current',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 1,
      accepted: true,
      finals: 1,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement queue',
    step: 'captured revoked',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement queue',
    step: 'captured revoked',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement queue',
    step: 'captured expired',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement queue',
    step: 'captured expired',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement response',
    step: 'ordinary fenced',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 1,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement response',
    step: 'ordinary fenced',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 1,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement response',
    step: 'captured current',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 1,
      accepted: true,
      finals: 1,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement response',
    step: 'captured current',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 1,
      accepted: true,
      finals: 1,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement response',
    step: 'captured revoked',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 1,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement response',
    step: 'captured revoked',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 1,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement response',
    step: 'captured expired',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 1,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held acknowledgement response',
    step: 'captured expired',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 1,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'compact million offset error',
    step: 'compact outcomes',
    expected: {
      action: 'compact',
      state: 'STABLE',
      rounds: 8,
      first: '9007199254740993',
      last: '9007199255740992',
      ranges: 1,
      type: 'release',
      result: 'rejected-by-broker',
      cause: 121,
    },
  },
  {
    fault: 'compact million offset success',
    step: 'compact outcomes',
    expected: {
      action: 'compact',
      state: 'STABLE',
      rounds: 8,
      first: '9007199254740993',
      last: '9007199255740992',
      ranges: 1,
      type: 'gap',
      result: 'accepted',
    },
  },
  {
    fault: 'two flushes settle disjoint terminal subsets',
    step: 'compact outcomes',
    expected: {
      action: 'compact',
      state: 'STABLE',
      rounds: 8,
      first: ['9007199254740993', '9007199254740994'],
      results: ['accept', 'reject'],
      terminalCount: 2,
    },
  },
  {
    fault: 'renew twice then terminal once',
    step: 'compact outcomes',
    expected: {
      action: 'compact',
      state: 'STABLE',
      rounds: 8,
      nonterminal: 2,
      terminal: 1,
      deliveryCount: 2,
    },
  },
  {
    fault: 'unawaited renew blocks terminal prepare',
    step: 'compact outcomes',
    expected: {
      action: 'compact',
      state: 'STABLE',
      rounds: 8,
      order: ['renew answer', 'terminal prepare', 'fetch'],
      unhandled: 0,
    },
  },
  {
    fault: 'renew selected deadline only',
    step: 'compact outcomes',
    expected: {
      action: 'compact',
      state: 'STABLE',
      rounds: 8,
      selected: ['9007199254740993'],
      unchanged: ['9007199254740994'],
      deliveryCount: 2,
    },
  },
  {
    fault: 'same offsets new acquisition rejects old callback',
    step: 'compact outcomes',
    expected: {
      action: 'compact',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      distinct: true,
      replacement: ['A:1@9007199254740993'],
    },
  },
  {
    fault: 'observer reentry cannot settle twice',
    step: 'compact outcomes',
    expected: {
      action: 'compact',
      state: 'STABLE',
      rounds: 8,
      terminal: 1,
      reentry: 'stop',
      listenerFailures: ['sync', 'async'],
    },
  },
]

const A1_J = `A:1@${J}`
const FAULT_SPECS = {
  'partition 121': { partition: 121 },
  'partition 6': { partition: 6 },
  'partition 100': { partition: 100 },
  'partition 74': { partition: 74 },
  'partition unknown': { partition: -1 },
  'omitted partition': { omit: true, partition: 0 },
  'duplicate partition': { duplicate: true, partition: 0 },
  'top 122': { code: 122 },
  'top 123': { code: 123 },
  'top 133': { code: 133 },
  'top 35': { code: 35 },
  'top 30': { code: 30 },
  disconnect: { transport: 'disconnect' },
  timeout: { transport: 'timeout' },
}
const KINDS = ['flush', 'piggyback', 'close', 'renew']
const MILLION_LAST = '9007199255740992'

/**
 * Drives one literal acknowledgement row through the public share consumer over the scripted
 * cluster. The row's step names the request kind: a boundary flush, the acknowledgements riding the
 * next fetch, the captured final of a close, or a handler-driven renewal. Node 0 always holds A0/H;
 * the selected record is A1/H on node 1.
 */
class AcknowledgementFaultCase extends DataFaultCase {
  /**
   * @param {object} row The literal oracle row.
   * @returns {AcknowledgementFaultCase} A case with its kind, fault and family selected.
   */
  constructor(row) {
    super(row)
    this.kind = KINDS.find((kind) => row.step.startsWith(kind))
    this.revoke = row.step.includes('revoked partition')
    const base = FAULT_SPECS[row.fault]
    this.spec = base
    this.family = { guard: 'guard', compact: 'compact' }[row.expected.action] ?? 'kind'
    if (row.fault === 'successful ack failed fetch') this.spec = { partition: 6 }
    else if (row.fault === 'failed ack successful fetch') this.spec = { ack: 121, partition: 0 }
    else if (
      this.kind === 'piggyback' &&
      base?.partition !== undefined &&
      !base.omit &&
      !base.duplicate
    )
      this.spec = { ack: base.partition, partition: 0 }
    this.transport = this.spec?.transport ?? null
    this.lockMs = this.kind === 'flush' ? 400 : 15000
  }

  /** @returns {Promise<void>} Reaches the family's frame. */
  async reach() {
    await this[`reach_${this.family}`]()
  }

  /** @returns {Promise<void>} Injects the row's fault. */
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

  /** @returns {void} Lets every held handler return. */
  releaseAll() {
    for (const { gate } of this.heldGates) gate.resolve()
  }

  /**
   * @param {number} partition Partition of topic A.
   * @returns {boolean} Whether both exact records were accepted without error.
   */
  acceptedBoth(partition) {
    const accepted = this.resultsOf(partition).filter((ack) => !ack.error && ack.type !== 'renew')
    return covers(accepted, H) && covers(accepted, J)
  }

  /** @returns {Promise<object>} The held steady heartbeat gate. */
  async holdBeatSteady() {
    const gate = this.holdWire({
      api: 'ShareGroupHeartbeat',
      match: ({ fields }) => fields.memberEpoch > 0,
    })
    await this.pump(50)
    await this.arrival(gate)
    await flush()
    return gate
  }

  /** @returns {Promise<void>} Answers a heartbeat with A2 revoked from the assignment. */
  async revokeA2() {
    const gate = await this.holdBeatSteady()
    gate.release({
      errorCode: 0,
      errorMessage: null,
      memberId: gate.call.fields.memberId,
      memberEpoch: 1,
      heartbeatIntervalMs: 50,
      assignment: { topicPartitions: [{ topicId: IDS.A, partitions: [0, 1] }] },
    })
    await flush()
  }

  /** @returns {Promise<void>} Reaches the frame of the row's request kind and holds it. */
  async reach_kind() {
    const timeout = this.transport === 'timeout'
    const startOptions = { requestTimeout: timeout ? 1000 : 30000 }
    if (this.kind === 'flush') await this.reachFlush(startOptions)
    else if (this.kind === 'piggyback') await this.reachPiggyback(startOptions)
    else if (this.kind === 'close') await this.reachClose(startOptions)
    else await this.reachRenew(startOptions)
    await this.arrival(this.gate)
  }

  /**
   * Flush: the A1/H handler returns past half of its lock while A1/J still runs, so the lazy
   * acknowledgement request leaves for H alone.
   * @param {object} startOptions Client options.
   * @returns {Promise<void>}
   */
  async reachFlush(startOptions) {
    await this.begin({ hold: [A0_H, A1_H, A1_J], expectCalls: 2, startOptions })
    this.gate = this.holdFrame(
      79,
      ({ node, fields }) => node === 1 && fields.shareSessionEpoch > 0 && !fields.isRenewAck
    )
    if (this.revoke) await this.revokeA2()
    await this.pump(250)
    this.release(A1_H)
  }

  /**
   * Piggyback: the node finishes its handlers and the next fetch carries the terminal decisions.
   * @param {object} startOptions Client options.
   * @returns {Promise<void>}
   */
  async reachPiggyback(startOptions) {
    await this.begin({ hold: [A0_H, A1_H], expectCalls: 2, startOptions })
    this.gate = this.holdFrame(
      78,
      ({ node, fields }) =>
        node === 1 && fields.shareSessionEpoch >= 1 && batchesOf({ fields }).length > 0
    )
    if (this.revoke) await this.revokeA2()
    this.release(A1_H)
  }

  /**
   * Close: the A1/H handler decides H and keeps running, so the captured final carries H and the
   * unstarted records of the node.
   * @param {object} startOptions Client options.
   * @returns {Promise<void>}
   */
  async reachClose(startOptions) {
    await this.start({ retry: RETRY_250, ...startOptions })
    this.onStart = (call) => (call.identity === A1_H ? call.acknowledge('accept') : undefined)
    this.supplyOnce('A:0', 'A:1', 'A:2')
    this.hold(A0_H)
    this.hold(A1_H)
    await this.join()
    await until(() => this.calls.length >= 2, 'the held handlers')
    if (this.revoke) await this.revokeA2()
    this.gate = this.holdFrame(
      79,
      ({ node, fields }) => node === 1 && fields.shareSessionEpoch === -1
    )
    this.closing = track(this.h.consumer.disconnect({ timeout: 400 }))
    await this.pump(300)
  }

  /**
   * Renew: the held A1/H handler asks for a renewal; its frame is the one that fails.
   * @param {object} startOptions Client options.
   * @returns {Promise<void>}
   */
  async reachRenew(startOptions) {
    await this.begin({ hold: [A0_H, A1_H], expectCalls: 2, startOptions })
    if (this.revoke) await this.revokeA2()
    this.gate = this.holdFrame(79, ({ node, fields }) => node === 1 && fields.isRenewAck === true)
    this.renewal = track(this.callOf(A1_H).renew())
  }

  /** @returns {Promise<void>} Releases the held frame with the row's fault. */
  async inject_kind() {
    this.startedAt = Date.now()
    this.resetsBefore = this.seen.resets.length
    this.beforeNode1 = this.frameLog(1).length
    await this.fire(this.gate, this.spec)
  }

  /** @returns {Promise<void>} Kind rows: result, cause, reset, held handler and no resends. */
  async verify_kind() {
    const { expected } = this.row
    const fatal = expected.state === 'FATAL'
    if (fatal) {
      await until(() => this.seen.crashes.length === 1, 'the crash event')
      assert.equal(this.seen.crashes[0].restart, false)
      assert(
        expected.cause === 35
          ? this.seen.crashes[0].error.reason === 'refused-by-broker'
          : codesOf(this.seen.crashes[0].error).includes(expected.cause)
      )
    }
    if (this.kind === 'close') await until(() => this.closing.settled, 'the disconnect')
    if (this.kind === 'renew') {
      await until(() => this.renewal.settled, 'the renewal outcome')
      assert(this.renewal.error, 'the faulted renewal rejects')
      assert.equal(this.renewal.error.reason, expected.result)
      assert.equal(this.terminalFor(1, H).length, 0, 'a renewal settles nothing terminally')
    } else if (this.row.fault !== 'omitted partition' && this.row.fault !== 'duplicate partition') {
      await until(() => this.terminalFor(1, H).length >= 1, 'the terminal result of A1/H')
      this.assertResult(1, H, expected.result, expected.cause)
    } else {
      await until(() => this.terminalFor(1, H).length >= 1, 'the terminal result of A1/H')
      this.assertResult(1, H, expected.result, undefined)
    }
    if (expected.reset) {
      assert.equal(this.seen.resets.length, this.resetsBefore + 1, 'one session reset')
    } else if (!fatal && this.kind !== 'close') {
      assert.equal(this.seen.resets.length, this.resetsBefore, 'no session reset')
    }
    if (this.kind === 'close') {
      assert.equal(this.finals().filter(({ node }) => node === 1).length, 1, 'one final')
      assert.equal(
        this.frameLog(1).filter(({ fields }) => fields.shareSessionEpoch === 0).length,
        1
      )
    }
    const held = this.callOf(A0_H)
    if (fatal || this.kind === 'close') assert.equal(held.isRunning(), false)
    else assert.equal(held.isRunning(), true)
    this.assertNoResends()
    if (this.revoke) await this.verifyRevoked()
  }

  /** @returns {void} No acknowledgement of the lost or consumed ticket is written again. */
  assertNoResends() {
    const written = this.frames(78, 79)
      .flatMap((frame) =>
        (frame.fields.topics || []).flatMap((topic) =>
          topic.partitions
            .filter(({ partitionIndex }) => topic.topicId === IDS.A && partitionIndex === 1)
            .flatMap(({ acknowledgementBatches }) => acknowledgementBatches)
        )
      )
      .filter((batch) => batch.firstOffset === H)
    assert(written.length <= 1, 'the acknowledgement of A1/H is written at most once')
  }

  /** @returns {Promise<void>} The revoked A2 records: never started, result per the row. */
  async verifyRevoked() {
    const { expected } = this.row
    assert.equal(
      this.calls.some((call) => call.alias === 'A' && call.partition === 2),
      false,
      'revoked records start no handler'
    )
    this.releaseAll()
    await this.advanceUntil(
      () => this.resultsOf(2).some((ack) => ack.type === expected.revokedType),
      'the revoked records settle'
    )
    const [outcome] = this.resultsOf(2).filter((ack) => ack.type === expected.revokedType)
    if (expected.revokedResult === 'accepted') assert.equal(outcome.error, null)
    else if (outcome.error) assert.equal(outcome.error.reason, expected.revokedResult)
  }

  /** @returns {Promise<void>} Kind rows: current work or a new run still works. */
  async replacement_kind() {
    const { expected } = this.row
    const fatal = expected.state === 'FATAL'
    if (fatal || this.kind === 'close') {
      this.supplyOnce('A:0')
      const calls = this.calls.length
      this.releaseAll()
      await this.run()
      await until(() => this.calls.length > calls, 'the control delivery')
      return
    }
    this.release(A0_H)
    this.releaseAll()
    await this.advanceUntil(() => this.acceptedBoth(0), 'the current terminal acceptance')
  }

  /** @returns {Promise<void>} Guard rows: an ack frame or captured final held at a write stage. */
  async reach_guard() {
    this.enforce = this.row.expected.enforce
    this.boundary = this.row.fault.replace('held acknowledgement ', '')
    this.captured = this.row.step.startsWith('captured')
    const startOptions = { requestTimeout: 1000, enforce: this.enforce }
    if (this.captured) {
      await this.start({ retry: RETRY_250, ...startOptions })
      this.onStart = (call) => (call.identity === A1_H ? call.acknowledge('accept') : undefined)
      this.supplyOnce('A:0', 'A:1', 'A:2')
      this.hold(A0_H)
      this.hold(A1_H)
      await this.join()
      await until(() => this.calls.length >= 2, 'the held handlers')
      this.framesBefore = this.countFrames()
      this.pending = this.boundaryHold(this.boundary, 79, -1)
      this.closing = track(this.h.consumer.disconnect({ timeout: 400 }))
      await this.pump(300)
    } else {
      this.lockMs = 400
      await this.begin({ hold: [A0_H, A1_H, A1_J], expectCalls: 2, startOptions })
      this.framesBefore = this.countFrames()
      this.pending = this.boundaryHold(this.boundary, 79, 1)
      await this.pump(250)
      this.release(A1_H)
    }
    await until(() => this.pending.reached(), `the ${this.boundary} boundary`)
    this.steadyAtHold = this.countFrames()
  }

  /** @returns {number} ShareAcknowledge frames written to node 1. */
  countFrames() {
    return this.frames(79).filter(({ node }) => node === 1).length
  }

  /**
   * @param {'connect'|'encode'|'queue'|'response'} boundary The write stage to hold on node 1.
   * @param {number} apiKey The held API.
   * @param {number} epoch -1 for a final, 1 for an ordinary acknowledgement.
   * @returns {{reached: () => boolean, release: () => void}} The hold.
   */
  boundaryHold(boundary, apiKey, epoch) {
    let reached = false
    if (boundary === 'response') {
      const wire = this.holdFrame(
        79,
        ({ node, fields }) =>
          node === 1 &&
          (epoch === -1 ? fields.shareSessionEpoch === -1 : fields.shareSessionEpoch > 0)
      )
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
    const isNode1 = (pool) => pool?.host === 'node1'
    if (boundary === 'connect') {
      const original = ConnectionPool.prototype.getConnectionByRequest
      jest.spyOn(ConnectionPool.prototype, 'getConnectionByRequest').mockImplementation(function (
        protocolRequest,
        ...rest
      ) {
        if (protocolRequest?.request?.apiKey !== apiKey || !isNode1(this) || !take())
          return original.call(this, protocolRequest, ...rest)
        return gate.promise.then(() => original.call(this, protocolRequest, ...rest))
      })
    } else if (boundary === 'encode') {
      const original = requests.ShareAcknowledge.protocol
      jest.spyOn(requests.ShareAcknowledge, 'protocol').mockImplementation((selection) => {
        const factory = original(selection)
        return (options) => {
          const built = factory(options)
          const matches =
            epoch === -1 ? options?.shareSessionEpoch === -1 : options?.shareSessionEpoch > 0
          if (!matches || taken) return built
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
        if (pushed.entry.apiKey !== apiKey || !take()) return original.call(this, pushed)
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

  /** @returns {Promise<void>} Guard rows: retire the held request's authority as the row says. */
  async inject_guard() {
    const { step } = this.row
    if (!this.captured) {
      this.fenced = true
      const gate = await this.holdBeatSteady()
      gate.release({ errorCode: 110 })
      await flush()
      return
    }
    if (step === 'captured revoked') {
      this.h.consumer.disconnect({ timeout: 0 }).catch(() => {})
      await flush()
    } else if (step === 'captured expired') await this.pump(2000)
  }

  /** @returns {Promise<void>} Guard rows: writes, finals, acceptance and no late effect. */
  async verify_guard() {
    const { expected } = this.row
    assert.equal(
      this.steadyAtHold - this.framesBefore,
      expected.writes,
      'frames written at the hold'
    )
    const results = this.seen.acks.length
    this.pending.release()
    await flush()
    await flush()
    const finals = this.finals().filter(({ node }) => node === 1).length
    assert.equal(finals, expected.finals, 'final frames of node 1')
    const accepted = this.terminalFor(1, H).some((ack) => !ack.error)
    assert.equal(accepted, expected.accepted, 'the held request is accepted only when current')
    if (!this.captured) assert.equal(this.seen.acks.length, results, 'no late effect')
    assert.equal(this.seen.crashes.length, 0)
  }

  /** @returns {Promise<void>} Guard rows: the replacement membership delivers B0/H. */
  async replacement_guard() {
    const identity = this.row.expected.replacement[0]
    this.h.table.set(NAME, { topicId: IDS.B, partitions: [{ leader: 0, epoch: 2 }] })
    this.supplyOnce('B:0')
    if (this.captured) {
      this.releaseAll()
      await this.run()
    } else {
      const gate = await this.holdBeatSteady()
      gate.release({
        errorCode: 0,
        errorMessage: null,
        memberId: gate.call.fields.memberId,
        memberEpoch: 2,
        heartbeatIntervalMs: 50,
        assignment: { topicPartitions: [{ topicId: IDS.B, partitions: [0] }] },
      })
    }
    await this.advanceUntil(
      () => this.calls.some((call) => call.identity === identity),
      'the replacement delivery'
    )
    await flush()
    assert.equal(this.calls.filter((call) => call.identity === identity).length, 1)
    this.releaseAll()
  }

  /** @returns {Promise<void>} Compact rows: each has its own scenario. */
  async reach_compact() {
    this.compact = COMPACT[this.row.fault]
    assert(this.compact, `no compact driver for ${this.row.fault}`)
    await this.compact.reach.call(this)
  }

  /** @returns {Promise<void>} Compact rows: the scenario's disturbance. */
  async inject_compact() {
    await this.compact.inject?.call(this)
  }

  /** @returns {Promise<void>} Compact rows: the scenario's oracle. */
  async verify_compact() {
    await this.compact.verify.call(this)
  }

  /** @returns {Promise<void>} Compact rows: nothing further. */
  async replacement_compact() {
    this.releaseAll()
    await flush()
  }

  /** @returns {Promise<void>} Replays frames, handlers and results through the audit. */
  async assertInvariants() {
    this.replay()
    if (this.auditFailures.length) throw this.auditFailures[0]
    assert.deepEqual(this.unhandled, [])
  }
}

/**
 * @param {boolean} corrupt Whether the acquired batch is corrupt, so the walk halts and releases.
 * @returns {object} A ShareFetch partition answer acquiring a million offsets with no records.
 */
const millionPart = (corrupt) => {
  const records = corrupt ? Buffer.from(recordsHex(H, ['x']), 'hex') : Buffer.alloc(0)
  if (corrupt) records[records.length - 1] ^= 0xff
  return part({
    topicId: IDS.A,
    partition: 1,
    acquire: [acquired(H, MILLION_LAST, 1)],
    records: records.toString('hex'),
  })
}

/** Compact-result scenarios by row fault, each run with the case as `this`. */
const COMPACT = {
  'compact million offset error': {
    async reach() {
      await this.start({ retry: RETRY_250 })
      this.cluster.answer('ShareFetch', (fields, record) =>
        record.node === 1 && !this.served
          ? ((this.served = true), fetchAnswer([millionPart(true)]))
          : this.fetchAnswerFor(fields)
      )
      this.cluster.answer('ShareAcknowledge', (fields) =>
        ackAnswer(
          listed(fields).map(({ topicId, partition }) => ({ topicId, partition, code: 121 }))
        )
      )
      this.supplyOnce('A:0')
      this.hold(A0_H)
      await this.join()
    },
    async verify() {
      const { expected } = this.row
      await this.advanceUntil(
        () => this.resultsOf(1).some((ack) => ack.error?.ranges),
        'the compact failure'
      )
      const [outcome] = this.resultsOf(1).filter((ack) => ack.error?.ranges)
      assert.equal(outcome.firstOffset, expected.first)
      assert.equal(outcome.lastOffset, expected.last)
      assert.equal(outcome.type, expected.type)
      assert.equal(outcome.error.ranges.length, expected.ranges)
      assert.equal(outcome.error.reason, expected.result)
      assert(codesOf(outcome.error).includes(expected.cause))
    },
  },
  'compact million offset success': {
    async reach() {
      await this.start({ retry: RETRY_250 })
      this.cluster.answer('ShareFetch', (fields, record) =>
        record.node === 1 && !this.served
          ? ((this.served = true), fetchAnswer([millionPart(false)]))
          : this.fetchAnswerFor(fields)
      )
      this.supplyOnce('A:0')
      this.hold(A0_H)
      await this.join()
    },
    async verify() {
      const { expected } = this.row
      await this.advanceUntil(
        () => this.resultsOf(1).some((ack) => ack.type === 'gap'),
        'the compact gap'
      )
      const [outcome] = this.resultsOf(1).filter((ack) => ack.type === 'gap')
      assert.equal(outcome.firstOffset, expected.first)
      assert.equal(outcome.lastOffset, expected.last)
      assert.equal(outcome.error, null)
      assert.equal(this.resultsOf(1).filter((ack) => ack.type === 'gap').length, 1)
    },
  },
  'two flushes settle disjoint terminal subsets': {
    async reach() {
      this.lockMs = 400
      this.onStart = (call) => call.acknowledge(call.identity === A1_J ? 'reject' : 'accept')
      await this.begin({ hold: [A0_H, A1_H, A1_J], expectCalls: 2 })
      await this.pump(250)
      this.release(A1_H)
    },
    async verify() {
      await until(() => this.terminalFor(1, H).length >= 1, 'the first flush result')
      await until(() => this.calls.some((call) => call.identity === A1_J), 'the J handler')
      this.release(A1_J)
      await this.advanceUntil(() => this.terminalFor(1, J).length >= 1, 'the second flush result')
      assert.deepEqual(
        [this.terminalFor(1, H)[0].type, this.terminalFor(1, J)[0].type],
        this.row.expected.results
      )
      assert.equal(
        this.terminalFor(1, H).length + this.terminalFor(1, J).length,
        this.row.expected.terminalCount
      )
    },
  },
  'renew twice then terminal once': {
    async reach() {
      await this.begin({ hold: [A0_H, A1_H], expectCalls: 2 })
      this.call = this.callOf(A1_H)
      await this.call.renew()
      await this.call.renew()
    },
    async verify() {
      const { expected } = this.row
      assert.equal(
        this.resultsOf(1).filter((ack) => ack.type === 'renew').length,
        expected.nonterminal
      )
      this.release(A1_H)
      await this.advanceUntil(() => this.terminalFor(1, H).length >= 1, 'the terminal result')
      assert.equal(this.terminalFor(1, H).length, expected.terminal)
      assert.equal(this.call.deliveryCount, expected.deliveryCount)
    },
  },
  'unawaited renew blocks terminal prepare': {
    async reach() {
      await this.begin({ hold: [A0_H, A1_H], expectCalls: 2 })
      this.call = this.callOf(A1_H)
      this.gate = this.holdFrame(79, ({ node, fields }) => node === 1 && fields.isRenewAck === true)
      this.renewal = track(this.call.renew())
      await this.arrival(this.gate)
    },
    async inject() {
      this.release(A1_H)
      await this.pump(100)
      this.terminalFrames = () =>
        this.frames(79, 78).filter(
          (frame) =>
            frame.node === 1 &&
            frame.fields.shareSessionEpoch > 0 &&
            batchesOf(frame).some((batch) => batch.acknowledgeTypes.some((type) => type !== 4))
        ).length
      this.before = this.terminalFrames()
    },
    async verify() {
      assert.equal(this.terminalFrames(), this.before, 'terminal preparation waits for the renewal')
      this.gate.release()
      await until(() => this.renewal.settled, 'the renewal answer')
      await this.advanceUntil(() => this.terminalFor(1, H).length >= 1, 'the terminal result')
      assert.equal(this.renewal.error, null)
      assert.deepEqual(this.unhandled, [])
    },
  },
  'renew selected deadline only': {
    async reach() {
      await this.begin({ hold: [A0_H, A1_H], expectCalls: 2 })
      this.call = this.callOf(A1_H)
      this.gate = this.holdFrame(79, ({ node, fields }) => node === 1 && fields.isRenewAck === true)
      this.renewal = track(this.call.renew())
      await this.arrival(this.gate)
    },
    async verify() {
      const { expected } = this.row
      const batches = batchesOf(this.gate.call)
      assert.deepEqual(
        batches.map(({ firstOffset, lastOffset }) => [firstOffset, lastOffset]),
        [[expected.selected[0], expected.selected[0]]]
      )
      assert.equal(covers(batches, J), false, 'the unselected sibling is not part of the renewal')
      this.gate.release()
      await until(() => this.renewal.settled, 'the renewal answer')
      assert.equal(this.renewal.error, null)
      assert.equal(this.call.deliveryCount, expected.deliveryCount)
    },
  },
  'same offsets new acquisition rejects old callback': {
    async reach() {
      this.supplyOnce('A:1')
      await this.begin({ hold: [A0_H, A1_H], expectCalls: 2 })
      this.old = this.callOf(A1_H)
      this.gate = this.holdFrame(79, ({ node, fields }) => node === 1 && fields.isRenewAck === true)
      this.renewal = track(this.old.renew())
      await this.arrival(this.gate)
    },
    async inject() {
      this.supplyOnce('A:1')
      this.gate.release(ackAnswer([], { errorCode: 122 }))
      await flush()
    },
    async verify() {
      await this.advanceUntil(
        () => this.calls.filter((call) => call.identity === A1_H).length === 2,
        'the reacquisition'
      )
      const [old, fresh] = this.calls.filter((call) => call.identity === A1_H)
      assert.notEqual(old.token, fresh.token)
      assert.throws(() => old.acknowledge('accept'), { message: STALE_CALL })
      assert.throws(() => old.renew(), { message: STALE_CALL })
      assert.equal(this.row.expected.distinct, true)
      assert.equal(fresh.isRunning(), true)
    },
  },
  'observer reentry cannot settle twice': {
    async reach() {
      this.lockMs = 400
      this.stops = []
      await this.start({ retry: RETRY_250 })
      const { events, on } = this.h.consumer
      this.removers.push(
        on(events.ACKNOWLEDGE, () => {
          throw new Error('sync listener failure')
        }),
        on(events.ACKNOWLEDGE, async () => {
          throw new Error('async listener failure')
        }),
        on(events.ACKNOWLEDGE, () => {
          this.stops.push(this.h.consumer.stop())
        })
      )
      this.supplyOnce('A:0', 'A:1')
      this.hold(A0_H)
      this.hold(A1_H)
      this.hold(A1_J)
      await this.join()
      await until(() => this.calls.length >= 2, 'the held handlers')
      await this.pump(250)
      this.release(A1_H)
    },
    async verify() {
      await this.advanceUntil(() => this.terminalFor(1, H).length >= 1, 'the terminal result')
      await flush()
      assert.equal(this.terminalFor(1, H).length, this.row.expected.terminal)
      assert.equal(this.stops.length >= 1, true)
      this.tracked.push(...this.stops)
    },
  },
}

describe('share acknowledgement faults', () => {
  afterEach(() => {
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  it.each(rows)('$fault at $step (row $#)', async (row) => {
    const scenario = new AcknowledgementFaultCase(row)
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

describe('acknowledgement fault observations', () => {
  test('retains every literal scenario once', () => {
    expect(rows).toHaveLength(130)
    expect(
      new Set(rows.map((row) => `${row.fault} / ${row.step} / ${row.expected.enforce ?? ''}`)).size
    ).toBe(130)
    expect(rows.every((row) => row.expected.rounds >= 8)).toBe(true)
  })

  test('keeps a million-offset result compact and rejects terminal overlap', () => {
    const audit = new FaultAudit()
    const acquisition = Symbol('acquisition')
    const event = {
      topicId: 'A',
      partition: 1,
      firstOffset: '9007199254740993',
      lastOffset: '9007199255740992',
      type: 'release',
    }
    audit.settlement(acquisition, event)
    expect(audit.settled.get(acquisition).get('A:1')).toHaveLength(1)
    expect(() => audit.settlement(acquisition, { ...event, type: 'accept' })).toThrow('overlap')
  })
  test('allows repeated nonterminal renew results before one terminal result', () => {
    const audit = new FaultAudit()
    const acquisition = Symbol('acquisition')
    const event = {
      topicId: 'A',
      partition: 1,
      firstOffset: '9007199254740993',
      lastOffset: '9007199254740993',
      type: 'renew',
    }
    audit.settlement(acquisition, event)
    audit.settlement(acquisition, event)
    audit.settlement(acquisition, { ...event, type: 'accept' })
    expect(audit.settled.get(acquisition).get('A:1')).toHaveLength(1)
  })
  test('does not collapse equal offsets from different acquisitions', () => {
    const audit = new FaultAudit()
    const event = {
      topicId: 'A',
      partition: 1,
      firstOffset: '9007199254740993',
      lastOffset: '9007199254740993',
      type: 'accept',
    }
    audit.settlement(Symbol('first'), event)
    audit.settlement(Symbol('second'), event)
    expect(audit.settled.size).toBe(2)
  })
  test('rejects fetch fields on an acknowledgement request', () => {
    const audit = new FaultAudit()
    expect(() =>
      audit.frame(
        {
          apiKey: 79,
          fields: { shareSessionEpoch: 2, topics: [], maxBytes: 0 },
        },
        { allowed: true }
      )
    ).toThrow()
  })
  test('retains signed int64 boundaries as exact strings', () => {
    const audit = new FaultAudit()
    expect(audit.range('9223372036854775807', '9223372036854775807')).toEqual([
      9223372036854775807n,
      9223372036854775807n,
    ])
    expect(() => audit.range('-1', '0')).toThrow()
    expect(() => audit.range('0', '9223372036854775808')).toThrow()
  })
})
