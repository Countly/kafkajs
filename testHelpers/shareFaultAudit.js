const assert = require('node:assert/strict')
const { AsyncLocalStorage } = require('node:async_hooks')
const { performance: realClock } = require('node:perf_hooks')
const realNow = realClock.now.bind(realClock)
const Broker = require('../src/broker')
const Connection = require('../src/network/connection')
const { ConsumerOperation } = require('../src/consumer/groupProtocol/interface')
const { ShareRun } = require('../src/consumer/share/shareRun')
const { ShareHandlerCall } = require('../src/consumer/share/shareHandlerCall')
const { ShareRunner } = require('../src/consumer/share/shareRunner')
const { ShareFetchLoop } = require('../src/consumer/share/shareFetchLoop')
const { ShareAckLedger } = require('../src/consumer/share/shareAckLedger')
const { socketCounts } = require('./fakeSocket')
const { createResourceProbe } = require('./resourceProbe')
const javaRecord = require('./wire/expected').shareFetchV2
const errors = require('../src/errors')
const observation = new AsyncLocalStorage()
const requestReaders = new Map(
  [
    require('./wire/shareGroupHeartbeatV1'),
    require('./wire/shareFetchV2'),
    require('./wire/shareAcknowledgeV2'),
  ].map((pair) => [pair.apiKey, pair])
)
const {
  acquired,
  deferred,
  fetchAnswer,
  flush,
  heartbeatAnswer,
  part,
  shareClient,
  until,
} = require('./shareHarness')
/**
 * Checks observations without answering a request or making a client decision.
 * The scenario supplies issuance identities captured from real calls, not wire counters.
 */

/**
 * Serializes a value with object keys sorted, so equal observations compare equal whatever their
 * key order.
 * @param {*} value An observation value.
 * @returns {string} Its canonical JSON.
 */
const canonicalJson = (value) =>
  JSON.stringify(value, (key, field) =>
    field && typeof field === 'object' && !Array.isArray(field)
      ? Object.fromEntries(
          Object.keys(field)
            .sort()
            .map((name) => [name, field[name]])
        )
      : field
  )

/**
 * Turns literal observation sets into sorted multisets: arrival order across independent nodes is
 * not part of the oracle, while every field and every duplicate is.
 * @param {Object<string, Array<*>>} sets Observation sets by name.
 * @returns {Object<string, string[]>} Sorted canonical entries per set.
 */
const canonicalSets = (sets) =>
  Object.fromEntries(
    Object.entries(sets).map(([name, values]) => [name, values.map(canonicalJson).sort()])
  )

class FaultAudit {
  /** @returns {FaultAudit} An empty observer with compact interval storage. */
  constructor() {
    this.finals = new Map()
    this.finalOpens = new Set()
    this.models = new Map()
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
      if (authority.open !== undefined) {
        assert(!this.finalOpens.has(authority.open), 'duplicate final for original open')
        this.finalOpens.add(authority.open)
      }
    }
    if (fields.isRenewAck) assert.equal(apiKey, 79)
    let model = this.models.get(authority.open)
    if (!model) {
      model = new Set()
      this.models.set(authority.open, model)
    }
    for (const topic of fields.topics || []) {
      for (const { partitionIndex } of topic.partitions)
        model.add(`${topic.topicId}:${partitionIndex}`)
    }
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
      for (const partition of topic.partitions) {
        const key = `${topic.topicId}:${partition}`
        forgotten.add(key)
        model.delete(key)
      }
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
    records: corpusHex(),
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
 * Returns two independent Java batches; only J's base offset differs from the pinned H batch.
 * @returns {string} Immutable H/J corpus bytes, without a production encoder or decoder.
 * @throws {AssertionError} If the pinned offset or copied record body changes.
 */
const corpusHex = () => {
  const first = Buffer.from(javaRecord.recordsHex, 'hex')
  assert.equal(first.readBigInt64BE(0), BigInt(H))
  const second = Buffer.from(first)
  second.writeBigInt64BE(BigInt(J), 0)
  assert.deepEqual(second.subarray(8), first.subarray(8))
  return Buffer.concat([first, second]).toString('hex')
}

/**
 * Enumerates acknowledgement ranges with their literal UUID and partition, in wire order.
 * @param {object} fields Independently read request fields or an original ticket descriptor.
 * @returns {object[]} Compact mutation descriptors; no per-offset expansion.
 */
const mutationsOf = (fields) =>
  (fields.topics || []).flatMap(({ topicId, partitions }) =>
    partitions.flatMap(({ partitionIndex, acknowledgementBatches }) =>
      acknowledgementBatches.map((batch) => ({ topicId, partition: partitionIndex, ...batch }))
    )
  )

/**
 * Captures original providers by call-through only. Async context separates nested subcases and
 * carries issuance across connect/encode/queue waits without adopting replacement identities.
 */
class FaultCapture {
  /**
   * @param {FaultCase} owner Scenario whose providers are observed.
   * @returns {FaultCapture} An observer that makes no client decisions.
   */
  constructor(owner) {
    this.owner = owner
    this.restores = []
    this.runs = new Set()
    this.loops = new Map()
    this.ledgers = new Map()
    this.operations = new Set()
    this.waits = new Set()
    this.connections = new Set()
    this.emitters = new Map()
    this.sockets = new Map()
    this.payloads = new WeakMap()
    this.assignments = []
    this.writes = []
    this.invocations = []
    this.handlerCalls = new Set()
    this.expired = []
    this.resultEvents = new Set()
    this.issuances = new WeakMap()
    this.appliedModels = new Map()
    this.outcomes = []
    this.published = []
    this.tickets = new Map()
    this.resultContext = null
  }

  /**
   * Executes work under this case's observation context, preserving returned values and errors.
   * @param {Function} work Original work.
   * @param {object} [extra] Original provider references to carry across awaits.
   * @returns {*} Unmodified result.
   */
  scope(work, extra = {}) {
    return observation.run({ owner: this.owner, ...extra }, work)
  }

  /**
   * Wraps one method without suppressing its result or failure; nested observers restore in LIFO order.
   * An explicitly watched instance remains owned across continuations outside the async context.
   * @param {object} target Original prototype.
   * @param {string} name Method name.
   * @param {Function} observe Receives original receiver, arguments and a call-through thunk.
   * @returns {void}
   */
  watch(target, name, observe) {
    const original = target[name]
    const hadOwn = Object.hasOwn(target, name)
    const capture = this
    target[name] = function (...args) {
      const next = () => original.apply(this, args)
      const context = observation.getStore()
      const owned =
        target === this ||
        this === capture.runner ||
        capture.runs.has(this) ||
        capture.loops.has(this) ||
        capture.ledgers.has(this) ||
        capture.operations.has(this) ||
        capture.connections.has(this) ||
        capture.handlerCalls.has(this)
      if (context?.owner !== capture.owner && !owned) return next()
      return observation.run(
        context?.owner === capture.owner ? context : { owner: capture.owner },
        () => observe(this, args, next)
      )
    }
    const wrapped = target[name]
    this.restores.push(() => {
      assert.equal(target[name], wrapped, `observer overwritten: ${name}`)
      if (hadOwn) target[name] = original
      else delete target[name]
    })
  }

  /**
   * Captures an emitter's original listener identities, separately from harness registrations.
   * @param {object} emitter An EventEmitter or instrumentation wrapper.
   * @returns {void}
   */
  emitter(emitter) {
    if (!emitter) return
    const raw = emitter.emitter ?? emitter
    if (!this.emitters.has(raw)) this.emitters.set(raw, this.listeners(raw))
  }

  /**
   * @param {object} emitter Original EventEmitter.
   * @returns {Map<string|symbol, Function[]>} Exact listener identities grouped by event.
   */
  listeners(emitter) {
    return new Map(emitter.eventNames().map((name) => [name, emitter.rawListeners(name)]))
  }

  /**
   * Installs only observation wrappers; production methods retain all transition authority.
   * @returns {void}
   */
  install() {
    this.watch(ShareRunner.prototype, 'api', (runner, args, next) => {
      this.runner = runner
      this.emitter(runner.options.instrumentationEmitter)
      this.emitter(runner.options.cluster.instrumentationEmitter)
      this.routing = runner.options.cluster.metadataCache.routingListeners
      this.routingBaseline = new Set(this.routing)
      return next()
    })
    for (const name of ['connect', 'subscribe', 'run', 'stop', 'disconnect']) {
      this.watch(ShareRunner.prototype, name, (runner, args, next) => {
        const result = next()
        if (['run', 'stop', 'disconnect'].includes(name)) this.owner.observe(result, name)
        return result
      })
    }
    this.watch(ShareHandlerCall.prototype, 'renew', (call, args, next) =>
      this.owner.observe(next(), 'renew')
    )
    this.watch(ShareRunner.prototype, 'newRun', (runner, args, next) => {
      const run = next()
      this.runs.add(run)
      this.operations.add(run.operation)
      return run
    })
    this.watch(ShareRun.prototype, 'assignmentChanged', (run, [snapshot], next) => {
      this.assignments.push({
        run,
        token: run.membership.token,
        target: run.membership.targetRevision,
        assignment: structuredClone(snapshot.assignment),
      })
      return next()
    })
    this.watch(ShareRun.prototype, 'createLoop', (run, args, next) => {
      const loop = next()
      this.loops.set(loop, run)
      this.operations.add(loop.operation)
      for (const method of ['apply', 'applyFinal']) {
        this.watch(loop.session, method, (session, [request, response], next) => {
          const result = next()
          this.owner.audited(() => this.applied(session, request, response))
          return result
        })
      }
      return loop
    })
    this.watch(ShareRun.prototype, 'invoke', (run, [node, cohort, call, payload], next) => {
      const captured = { run, node, cohort, call, payload }
      this.payloads.set(payload, captured)
      this.invocations.push(captured)
      this.handlerCalls.add(call)
      return next()
    })
    this.watch(ShareRun.prototype, 'leave', (run, [episode], next) =>
      this.scope(next, { run, membership: run.membership.token, episode })
    )
    this.watch(ShareRun.prototype, 'transmit', (run, args, next) =>
      this.scope(next, { run, membership: run.membership.token, episode: run.closing })
    )
    this.watch(ShareFetchLoop.prototype, 'newExchange', (loop, args, next) => {
      const exchange = next()
      const run = this.loops.get(loop)
      this.issuances.set(exchange, {
        wanted: new Set(loop.wanted().map(({ topicId, partition }) => `${topicId}:${partition}`)),
        assignment: this.assignments.filter((entry) => entry.run === run).at(-1),
      })
      return exchange
    })
    this.watch(ShareFetchLoop.prototype, 'send', (loop, [exchange], next) =>
      this.scope(next, {
        run: this.loops.get(loop),
        loop,
        exchange,
        open: exchange.open,
        membership: loop.authority.membershipToken,
        prepared: [...exchange.prepared],
        ...this.issuances.get(exchange),
      })
    )
    this.watch(
      ShareFetchLoop.prototype,
      'writeFinal',
      (loop, [operation, guard, captured], next) => {
        this.operations.add(operation)
        const run = this.loops.get(loop)
        return this.scope(next, {
          run,
          loop,
          open: captured.descriptor.token,
          episode: run.closing,
          membership: loop.authority.membershipToken,
          prepared: [...captured.prepared],
          wanted: new Set(loop.wanted().map(({ topicId, partition }) => `${topicId}:${partition}`)),
          assignment: this.assignments.filter((entry) => entry.run === run).at(-1),
        })
      }
    )
    this.watch(ShareFetchLoop.prototype, 'adopt', (loop, args, next) => {
      const cohort = next()
      this.ledgers.set(cohort.ledger, cohort)
      return cohort
    })
    this.watch(ShareAckLedger.prototype, 'prepare', (ledger, args, next) => {
      const result = next()
      if (result.ticket) this.tickets.set(result.ticket, { ledger, settled: false, writes: 0 })
      return result
    })
    for (const method of ['settle', 'invalidate', 'dropExpired']) {
      if (typeof ShareAckLedger.prototype[method] !== 'function') continue
      this.watch(ShareAckLedger.prototype, method, (ledger, args, next) => {
        const result = next()
        if (method === 'settle' && this.tickets.has(args[0]))
          this.tickets.get(args[0]).settled = true
        if (method === 'invalidate') {
          for (const [ticket, state] of this.tickets) {
            if (state.ledger === ledger && ticket !== args[1]) state.settled = true
          }
        }
        const cohort = this.ledgers.get(ledger)
        if (method === 'dropExpired') {
          for (const range of result) this.expired.push({ cohort, range })
        } else {
          for (const event of result) {
            if (this.resultEvents.has(event)) continue
            this.resultEvents.add(event)
            this.outcomes.push({ cohort, event })
          }
        }
        return result
      })
    }
    this.watch(ShareRunner.prototype, 'publishResults', (runner, [cohort, results], next) => {
      const previous = this.resultContext
      this.resultContext = cohort
      try {
        return next()
      } finally {
        this.resultContext = previous
      }
    })
    for (const method of ['wait', 'register']) {
      this.watch(ConsumerOperation.prototype, method, (operation, args, next) => {
        this.operations.add(operation)
        const result = next()
        if (method === 'wait') {
          this.waits.add(result)
          const done = () => this.waits.delete(result)
          result.then(done, done)
        }
        return result
      })
    }
    this.watch(Connection.prototype, 'connect', (connection, args, next) => {
      this.connections.add(connection)
      this.emitter(connection.requestQueue)
      return next()
    })
    this.watch(Connection.prototype, 'send', (connection, args, next) => {
      this.connections.add(connection)
      const { wire } = observation.getStore()
      const request = args[0].request
      if (wire && requestReaders.has(request.apiKey)) {
        const encode = request.encode
        args[0] = {
          ...args[0],
          request: {
            ...request,
            encode: async () => {
              const encoded = await encode.call(request)
              wire.fields = requestReaders
                .get(request.apiKey)
                .readRequest(Buffer.from(encoded.buffer))
              return encoded
            },
          },
        }
      }
      const socket = connection.socket
      if (socket && !this.sockets.has(socket)) {
        this.sockets.set(
          socket,
          new Map(['error', 'close'].map((name) => [name, socket.rawListeners(name)]))
        )
      }
      return next()
    })
    for (const [method, apiKey] of [
      ['shareGroupHeartbeat', 76],
      ['shareFetch', 78],
      ['shareAcknowledge', 79],
    ]) {
      this.watch(Broker.prototype, method, (broker, args, next) => {
        const [fields, options] = args
        const pool = broker.connectionPool
        const issued = {
          ...observation.getStore(),
          pool,
          connection: pool.pool[pool.connectionIndexFor(apiKey)],
          wire: {},
        }
        const guard = options?.guard
        assert.equal(typeof guard, 'function', 'share write must retain its original guard')
        args[1] = {
          ...options,
          guard: (length) => {
            const refused = guard(length)
            if (length !== undefined && refused === null)
              this.owner.audited(() => this.write(broker, apiKey, fields, issued))
            return refused
          },
        }
        return observation.run(issued, next)
      })
    }
  }

  /**
   * Checks the real session model against an independent union/list/forget observation.
   * Acknowledgement-only additions participate; reset/final clear the model without reopening.
   * @param {object} session Original session receiver.
   * @param {object} request Original immutable descriptor.
   * @param {object} response Actual decoded answer.
   * @returns {void}
   * @throws {AssertionError} For a missing or retained partition in the provider's model.
   */
  applied(session, request, response) {
    let expected = this.appliedModels.get(request.token) ?? new Set()
    const reset =
      [122, 123].includes(response.errorCode) ||
      (request.apiKey === 78 && response.errorCode === 133)
    if (request.epoch === -1 || reset) expected = new Set()
    else {
      expected = new Set(expected)
      for (const { topicId, partitions } of request.topics) {
        for (const { partitionIndex } of partitions) expected.add(`${topicId}:${partitionIndex}`)
      }
      for (const { topicId, partitions } of request.forgottenTopicsData ?? []) {
        for (const partition of partitions) expected.delete(`${topicId}:${partition}`)
      }
    }
    this.appliedModels.set(request.token, expected)
    assert.deepEqual(
      [...session.model.keys()].sort(),
      [...expected].sort(),
      'original open partition model'
    )
  }

  /**
   * Records authority at the original transmission guard, immediately before socket.write,
   * alongside every field independently decoded from that request's original encoded body.
   * @param {object} broker Original Broker.
   * @param {number} apiKey Original API.
   * @param {object} fields Original request fields.
   * @param {object} issued Original issuance context and independently read encoded request.
   * @returns {void}
   * @throws {AssertionError} For a write without its original provider context.
   */
  write(broker, apiKey, fields, issued) {
    const { run, loop, open, membership, prepared = [] } = issued
    assert(issued.wire.fields, 'the original request body was independently captured')
    const episode = issued.episode ?? (issued.exchange?.kind === 'final' ? issued.exchange : null)
    assert(run && this.runs.has(run), 'uncaptured run')
    const closing = fields.shareSessionEpoch === -1 || fields.memberEpoch === -1
    const { pool, connection } = issued
    assert.equal(broker.connectionPool, pool, 'original pool replaced before transmission')
    const wanted = new Set(issued.wanted ?? [])
    const allowed = closing
      ? (episode !== null && episode === run.closing && !episode.revoked) ||
        (episode === issued.exchange &&
          issued.exchange?.kind === 'final' &&
          loop.finishing &&
          loop.session.refusal(open) === null)
      : membership === run.membership.token &&
        (apiKey === 76
          ? !run.retired && !run.membership.closed && run.membership.state !== 'FATAL'
          : run.ordinaryRefusal(membership) === null &&
            open === loop.session.token &&
            loop.session.refusal(open) === null)
    const authority = {
      allowed,
      run,
      membership,
      kind: closing ? 'closing' : 'ordinary',
      episode,
      open,
      wanted,
      assignment: issued.assignment,
    }
    this.writes.push({
      apiKey,
      fields: issued.wire.fields,
      authority,
      broker,
      pool,
      connection,
      generation: pool.generationFor(apiKey),
      socket: connection.socket,
      prepared,
    })
    for (const { ticket } of prepared) {
      const state = this.tickets.get(ticket)
      assert(state, 'uncaptured ledger ticket')
      assert.equal(state.writes++, 0, 'original ticket resent')
    }
  }

  /**
   * Restores this observer's wrappers after its original work has drained.
   * @returns {void}
   * @throws {AggregateError} If a wrapper was overwritten or could not be restored.
   */
  restore() {
    const failures = []
    for (const restore of this.restores.reverse()) {
      try {
        restore()
      } catch (error) {
        failures.push(error)
      }
    }
    this.restores = []
    if (failures.length) throw new AggregateError(failures, 'Fault observer restoration failed')
  }
}

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
    this.observer = new FaultCapture(this)
    this.pendingWork = new Set()
    this.observed = new WeakSet()
    this.rejections = []
    this.expectedRejections = []
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
    this.observer.emitter(process)
    this.probe = createResourceProbe({
      emitters: { process },
      sockets: () => socketCounts({ sockets: this.cluster?.sockets() ?? [] }),
      queueDepth: () =>
        [...this.observer.connections].reduce(
          (n, c) => n + c.requestQueue.pending.length + c.requestQueue.inflight.size,
          0
        ),
      ledgerSize: () => [...this.observer.ledgers.keys()].reduce((n, ledger) => n + ledger.size, 0),
    })
    this.baseline = await this.resourceVector()
    assert.deepEqual(await this.resourceVector(), this.baseline, 'stable preclient baseline')
    this.observer.install()
    this.h = this.observer.scope(() =>
      shareClient({
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
    )
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
    await this.observer.scope(() => this.h.consumer.subscribe({ topics: [NAME] }))
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
   * @param {object} payload The eachMessage payload captured from the original invocation.
   * @returns {Promise<void>}
   */
  async handle(payload) {
    const { partition, message, acknowledge, renew, isRunning, topic } = payload
    const original = this.observer.payloads.get(payload)
    assert(original, 'handler payload was not captured from ShareRun.invoke')
    const { cohort, call: providerCall } = original
    const alias = ALIAS.get(cohort.topicId) ?? '?'
    assert.equal(topic, cohort.topic)
    assert.equal(partition, cohort.partition)
    assert([H, J].includes(message.offset), 'record outside the literal corpus')
    assert.deepEqual(message.key, Buffer.from('6b', 'hex'))
    assert.deepEqual(message.value, Buffer.from('76', 'hex'))
    assert.equal(message.deliveryCount, 2)
    assert.equal(message.timestamp, '1700000000000')
    const identity = `${alias}:${partition}@${message.offset}`
    const call = {
      identity,
      alias,
      partition,
      offset: message.offset,
      deliveryCount: message.deliveryCount,
      value: message.value.toString(),
      keyBytes: message.key.toString('hex'),
      valueBytes: message.value.toString('hex'),
      acknowledge,
      renew,
      isRunning,
      abandoned: false,
      topicId: cohort.topicId,
      cohort,
      providerCall,
      token: cohort.token,
    }
    this.calls.push(call)
    this.audited(() =>
      this.audit.handler(`${cohort.topicId}:${partition}@${message.offset}`, providerCall, true)
    )
    const gate = this.holds.get(identity)
    this.holds.delete(identity)
    try {
      await this.onStart?.(call)
      await gate?.promise
      await this.onHandled?.(call)
    } finally {
      if (!call.abandoned)
        this.audited(() =>
          this.audit.handler(
            `${cohort.topicId}:${partition}@${message.offset}`,
            providerCall,
            false
          )
        )
    }
  }

  /**
   * Starts a run whose handler is {@link FaultCase#handle}; the returned promise is observed at once.
   * @returns {Promise<void>} The run promise, which settles after the first accepted join.
   */
  run() {
    const promise = this.observer.scope(() =>
      this.h.consumer.run({
        eachMessage: (payload) => {
          const pending = this.handle(payload)
          this.handlers.push(pending)
          this.observe(pending, 'handler')
          return pending
        },
      })
    )
    this.observe(promise, 'run')
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
    const cohort = this.observer.resultContext
    assert(cohort, 'result was not published by its original cohort')
    const captured = { cohort, event }
    this.observer.published.push(captured)
    this.audit.settlement(cohort.token, { ...event, topicId: cohort.topicId })
    const lost = ['session-lost', 'membership-lost'].includes(event.error?.reason)
    if (!lost || event.type === 'renew') return
    for (const call of this.calls) {
      if (call.cohort !== cohort || call.abandoned || !covers([event], call.offset)) continue
      const identity = `${cohort.topicId}:${cohort.partition}@${call.offset}`
      if (this.audit.handlers.get(identity) !== call.providerCall) continue
      assert.equal(call.providerCall.authority.current(), false, 'lost call retained authority')
      call.abandoned = true
      this.audit.handler(identity, call.providerCall, false)
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
   * Finds a schedule gate's copied call in the independently recorded transmission sequence.
   * @param {object} record The original request or its schedule copy.
   * @returns {number} Its exact node/socket/correlation index.
   * @throws {AssertionError} If the request was not written.
   */
  wireIndex(record) {
    const index = this.frames(76, 78, 79).findIndex(
      (candidate) =>
        candidate.node === record.node &&
        candidate.generation === record.generation &&
        candidate.correlationId === record.correlationId &&
        candidate.apiKey === record.apiKey
    )
    assert(index >= 0, 'original wire record')
    return index
  }

  /**
   * Ends the armed frame with a typed transport failure: a peer end on its own socket, or the
   * request timeout of the fake clock. The held answer is released late and must change nothing.
   * @param {'disconnect'|'timeout'} kind The failure.
   * @returns {Promise<object>} The frame that was disturbed.
   */
  async fireTransport(kind) {
    const call = await this.arrival(this.transportGate)
    if (kind === 'disconnect') this.socketFor(call).emit('end')
    else {
      const captured = this.observer.writes[this.wireIndex(call)]
      const queue = captured.connection.requestQueue
      const request = queue.inflight.get(call.correlationId)
      assert(request, 'the original request is awaiting its timeout')
      const remaining = request.sentAt + request.requestTimeout - Date.now()
      await this.pump(Math.max(0, remaining) + Math.min(queue.requestTimeout, 100) + 1)
    }
    await flush()
    return call
  }

  /**
   * Resolves the seam's per-node socket ordinal, separately from the pool's API generation.
   * @param {object} record Original independently read request.
   * @returns {object} The original socket that wrote it.
   * @throws {AssertionError} For an unknown node or ordinal.
   */
  socketFor(record) {
    const node = this.cluster.nodes.find(({ id }) => id === record.node)
    assert(node, 'unknown request node')
    const socket = this.cluster
      .sockets()
      .filter(({ endpoint }) => endpoint.host === node.host && endpoint.port === node.port)[
      record.generation - 1
    ]
    assert(socket, 'unknown original socket ordinal')
    return socket
  }

  /**
   * Replays every independently decoded frame against authority captured at actual transmission.
   * Complete provider/application handler, ticket/wire mutation and ledger/public result sets must
   * agree, including duplicate occurrences and original cause identities. Optional literal row
   * observations further constrain these complete sets without deriving policy from the client.
   * @returns {void}
   * @throws {AssertionError} For stale writes, missing/extra observations or changed corpus data.
   */
  replay() {
    const audit = new FaultAudit()
    const frames = this.frames(76, 78, 79)
    assert.equal(frames.length, this.observer.writes.length, 'complete transmission set')
    for (let i = 0; i < frames.length; i++) {
      const record = frames[i]
      const captured = this.observer.writes[i]
      assert.equal(record.apiKey, captured.apiKey)
      assert.equal(this.socketFor(record), captured.socket, 'original socket ordinal')
      assert.deepEqual(record.fields, captured.fields, 'complete encoded request fields')
      audit.frame(record, captured.authority)
      const expected = captured.prepared.flatMap(({ row, ticket }) =>
        ticket.acknowledgementBatches.map((batch) => ({
          topicId: row.topicId,
          partition: row.partition,
          ...batch,
        }))
      )
      const canonical = (rows) => rows.map((row) => JSON.stringify(row)).sort()
      assert.deepEqual(
        canonical(mutationsOf(record.fields)),
        canonical(expected),
        'complete literal ticket mutation set'
      )
    }
    assert.equal(this.calls.length, this.observer.invocations.length, 'complete handler set')
    for (let i = 0; i < this.calls.length; i++) {
      assert.equal(this.calls[i].providerCall, this.observer.invocations[i].call)
      assert.equal(this.calls[i].token, this.observer.invocations[i].cohort.token)
    }
    assert.equal(
      this.observer.published.length,
      this.observer.outcomes.length,
      'complete settlement set'
    )
    for (let i = 0; i < this.observer.outcomes.length; i++) {
      const expected = this.observer.outcomes[i]
      const actual = this.observer.published[i]
      assert.equal(actual.cohort, expected.cohort)
      assert.equal(actual.event.topic, expected.cohort.topic)
      assert.equal(actual.event.partition, expected.cohort.partition)
      for (const key of ['firstOffset', 'lastOffset', 'type', 'error'])
        assert.equal(actual.event[key], expected.event[key], key)
    }
    if (this.row.expected.observations) this.assertObservations(this.row.expected.observations)
    assert.deepEqual(this.auditFailures, [], 'callback audit failures')
  }

  /**
   * Compares complete row-owned literal multisets, retaining duplicates and every field.
   * @param {object} expected Literal handlers, mutations and settlements arrays supplied by a row.
   * @returns {void}
   * @throws {AssertionError} For any missing or extra identity/range, byte, count or outcome.
   */
  assertObservations(expected) {
    const handlers = this.calls.map(
      ({ topicId, partition, offset, deliveryCount, keyBytes, valueBytes }) => ({
        topicId,
        partition,
        offset,
        key: keyBytes,
        value: valueBytes,
        deliveryCount,
      })
    )
    const mutations = this.frames(78, 79).flatMap(({ fields }) => mutationsOf(fields))
    const settlements = this.observer.published.map(({ cohort, event }) => ({
      topicId: cohort.topicId,
      partition: cohort.partition,
      firstOffset: event.firstOffset,
      lastOffset: event.lastOffset,
      type: event.type,
      reason: event.error?.reason ?? null,
      code: event.error?.cause?.code ?? null,
    }))
    assert.deepEqual(
      canonicalSets({ handlers, mutations, settlements }),
      canonicalSets(expected),
      'literal row observation sets'
    )
  }

  /**
   * Observes returned work immediately; rejection remains visible to its caller and cleanup.
   * @param {Promise<*>} promise Original promise, not a replacement rejection-swallowing chain.
   * @param {string} [label='work'] Diagnostic identity.
   * @returns {Promise<*>} The same promise.
   */
  observe(promise, label = 'work') {
    if (this.observed.has(promise)) return promise
    this.observed.add(promise)
    this.pendingWork.add(promise)
    promise.then(
      () => this.pendingWork.delete(promise),
      (error) => {
        this.pendingWork.delete(promise)
        this.rejections.push({ promise, label, error })
      }
    )
    return promise
  }

  /**
   * Declares an exact expected rejection for one original returned promise.
   * @param {Promise<*>} promise Original work.
   * @param {{name: string, message: string, reason?: string, code?: number}} expected Literal error oracle.
   * @returns {Promise<*>} The unchanged promise, observed immediately.
   */
  expectRejection(promise, expected) {
    this.expectedRejections.push({ promise, expected })
    return this.observe(promise)
  }

  /**
   * Reads only real provider-owned resource collections, including retired generations.
   * @returns {Promise<object>} Exact vector under the installed fake clock.
   */
  async resourceVector() {
    const probe = await this.probe.snapshot()
    const loops = [...this.observer.loops.keys()]
    const operations = [...this.observer.operations]
    return {
      ...probe,
      listeners: {
        process:
          probe.listeners.process -
          Number(process.listeners('unhandledRejection').includes(this.onUnhandled)),
      },
      timers: jest.getTimerCount(),
      fifoDepth: loops.reduce(
        (n, loop) => n + loop.session.queue.length + Number(loop.session.active),
        0
      ),
      sessionModel: loops.reduce((n, loop) => n + loop.session.model.size, 0),
      activeExchanges: loops.reduce((n, loop) => n + loop.exchanges.size + loop.renewals.size, 0),
      ownedWaits: operations.reduce((n, op) => n + op.waiters.size, 0),
      pendingOperationPromises: this.observer.waits.size,
      ownedDisposers: operations.reduce((n, op) => n + op.disposers.size, 0),
      pendingGates: this.gates.filter((gate) => gate.settle).length,
      unhandled: [...this.unhandled],
    }
  }

  /**
   * Bounds original cleanup by eight drain turns and a real monotonic deadline. Every assertion
   * runs even if disconnect/draining fails; strict cluster.assertClean is always the final check.
   * @returns {Promise<void>}
   * @throws {AggregateError} All cleanup, rejection-classification and baseline/seam failures.
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
    for (const { gate } of this.heldGates) await attempt(() => gate.resolve())
    for (const gate of this.gates) if (gate.settle) await attempt(() => gate.release())
    const deadline = realNow() + 2000
    let disconnected = !this.h
    if (this.h) {
      const promise = this.observer.scope(() => this.h.consumer.disconnect({ timeout: 0 }))
      this.observe(promise, 'disconnect')
      promise.then(
        () => {
          disconnected = true
        },
        (error) => {
          failures.push(error)
          disconnected = true
        }
      )
    }
    for (const promise of [...this.handlers, ...this.tracked]) this.observe(promise)
    for (let turn = 0; turn < 8 && realNow() < deadline; turn++) {
      await attempt(() => flush())
      if (disconnected && this.pendingWork.size === 0) break
    }
    await attempt(() =>
      assert(
        disconnected && this.pendingWork.size === 0,
        'cleanup deadline: original work remains pending'
      )
    )
    await attempt(() => this.assertRejections())
    if (this.h) await attempt(() => this.replay())
    for (const remove of this.removers.splice(0))
      await attempt(() => {
        remove()
      })
    await attempt(() => this.observer.restore())
    await attempt(() => this.assertBaseline())
    process.off('unhandledRejection', this.onUnhandled)
    await attempt(() => this.cluster?.assertClean())
    if (failures.length) throw new AggregateError(failures, 'Share fault cleanup failed')
  }

  /**
   * Requires every late rejection to match one literal row declaration or an already-asserted
   * original fatal run error. An unspecified rejection fails; no class/message wildcard exists.
   * @returns {void}
   * @throws {AssertionError} For an unexpected rejection or an unused expected rejection.
   */
  assertRejections() {
    const remaining = [...(this.row.expected.rejections ?? [])]
    const declared = new Map(
      this.expectedRejections.map(({ promise, expected }) => [promise, expected])
    )
    const fatalMessages = {
      29: 'Not authorized to access topics: [Topic authorization failed]',
      30: 'Not authorized to access group: Group authorization failed',
      35: 'The version of API is not supported',
      42: 'This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details',
      69: 'The group id was not found',
      81: 'The consumer group has reached its max size. It already has the configured maximum number of members',
    }
    let fatalRun = false
    for (const { promise, label, error } of this.rejections) {
      let expected = declared.get(promise)
      if (expected) declared.delete(promise)
      else if (remaining.length) expected = remaining.shift()
      else if (
        label === 'run' &&
        !fatalRun &&
        this.row.step === 'before join' &&
        this.row.expected.action === 'fatal'
      ) {
        const code = this.row.expected.cause
        expected = {
          name: code === 35 ? 'KafkaJSShareGroupsNotSupportedError' : 'KafkaJSNonRetriableError',
          message: fatalMessages[code],
        }
        fatalRun = true
      } else if (label === 'renew') {
        const reason = this.row.expected.request ?? this.row.expected.result
        if (
          ['session-lost', 'membership-lost', 'rejected-by-broker', 'not-sent'].includes(reason)
        ) {
          expected = {
            name: 'KafkaJSShareAcknowledgementError',
            message: 'Acknowledgement was not confirmed',
            reason,
          }
        }
      }
      assert(
        expected && typeof expected.message === 'string',
        `undeclared ${label} rejection: ${error.name}: ${error.message}`
      )
      assert.equal(error.constructor, errors[expected.name], 'literal rejection class')
      for (const [key, value] of Object.entries(expected))
        assert.equal(error[key], value, `literal rejection ${key}`)
    }
    assert.equal(remaining.length, 0, 'missing literal rejection')
    assert.equal(declared.size, 0, 'expected promise did not reject')
  }

  /**
   * Checks exact timer/open-socket/vector baselines and original listener identities. The inert
   * socket error/close handlers retained by Connection are compared separately from active ones.
   * @returns {Promise<void>}
   * @throws {AssertionError} For any leaked resource or unexpected listener change.
   */
  async assertBaseline() {
    assert.deepEqual(this.unhandled, [], 'unhandled rejections')
    if (!this.baseline) return
    const actual = await this.resourceVector()
    assert.equal(actual.sockets.open, 0, 'open sockets')
    const { sockets, ...comparable } = actual
    const { sockets: oldSockets, ...baseline } = this.baseline
    assert.deepEqual(comparable, baseline, 'exact provider resource vector')
    for (const [emitter, before] of this.observer.emitters) {
      if (
        emitter === process &&
        !process.listeners('unhandledRejection').includes(this.onUnhandled)
      ) {
        const after = this.observer.listeners(process)
        const withoutRecorder = new Map(before)
        withoutRecorder.set(
          'unhandledRejection',
          before.get('unhandledRejection').filter((fn) => fn !== this.onUnhandled)
        )
        if (!withoutRecorder.get('unhandledRejection').length)
          withoutRecorder.delete('unhandledRejection')
        assert.deepEqual(after, withoutRecorder, 'process listeners')
      } else
        assert.deepEqual(
          this.observer.listeners(emitter),
          before,
          'internal/user listener baseline'
        )
    }
    if (this.observer.routing)
      assert.deepEqual(this.observer.routing, this.observer.routingBaseline, 'routing listeners')
    for (const state of this.observer.tickets.values())
      assert(state.settled, 'unsettled original ticket')
    for (const socket of this.cluster?.sockets() ?? []) {
      for (const name of ['data', 'end', 'timeout'])
        assert.equal(socket.listenerCount(name), 0, `socket ${name} listeners`)
      const before = this.observer.sockets.get(socket)
      if (before)
        for (const name of ['error', 'close'])
          assert.deepEqual(socket.rawListeners(name), before.get(name), `socket ${name} identities`)
    }
    assert.equal(this.audit.handlers.size, 0, 'active application calls')
    for (const [ledger, cohort] of this.observer.ledgers) {
      assert.equal(ledger.size, 0, 'acquisition ledger')
      for (const offset of [H, J]) {
        if (!cohort.walk.deliver.records.some((record) => record.offset === offset)) continue
        const intervals =
          this.audit.settled.get(cohort.token)?.get(`${cohort.topicId}:${cohort.partition}`) ?? []
        const expired = this.observer.expired
          .filter((entry) => entry.cohort === cohort)
          .map(({ range }) => [BigInt(range.firstOffset), BigInt(range.lastOffset)])
        assert.equal(
          [...intervals, ...expired].filter(
            ([first, last]) => first <= BigInt(offset) && last >= BigInt(offset)
          ).length,
          1,
          'complete terminal corpus settlement'
        )
      }
    }
  }
}

module.exports = {
  FaultAudit,
  FaultCase,
  NAME,
  IDS,
  ALIAS,
  H,
  J,
  STALE_CALL,
  advance,
  dataPart,
  codesOf,
  track,
  covers,
}
