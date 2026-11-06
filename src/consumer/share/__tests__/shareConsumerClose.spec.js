const Broker = require('../../../broker')
const Cluster = require('../../../cluster')
const RequestQueue = require('../../../network/requestQueue')
const Emitter = require('../../../instrumentation/emitter')
const { requests } = require('../../../protocol/requests')
const { KafkaJSNonRetriableError, KafkaJSShareAcknowledgementError } = require('../../../errors')
const { ShareRun } = require('../shareRun')
const {
  A,
  ackAnswer,
  acquire,
  deferred,
  fetchAnswer,
  flush,
  shareClient,
  until,
} = require('../../../../testHelpers/shareHarness')

const TWO_NODES = {
  'topic-a': {
    topicId: A,
    partitions: [
      { leader: 0, epoch: 1 },
      { leader: 1, epoch: 1 },
    ],
  },
}

/**
 * Fake timers plus a monotonic clock the test can move without delivering any timer. A due timer
 * reads the clock when it runs, so `skew` followed by a later `advance` models a delayed timer.
 */
class Clock {
  constructor() {
    this.skewMs = 0
    this.origin = 0
  }

  /** @returns {void} Installs fake timers and a `performance.now` that starts at zero. */
  install() {
    jest.useFakeTimers({
      now: 1000000,
      doNotFake: ['setImmediate', 'queueMicrotask', 'nextTick', 'hrtime', 'performance'],
    })
    this.origin = Date.now()
    jest.spyOn(performance, 'now').mockImplementation(() => Date.now() - this.origin + this.skewMs)
  }

  /**
   * @param {number} ms Milliseconds the clock jumps without any timer being delivered.
   * @returns {void}
   */
  skew(ms) {
    this.skewMs += ms
  }

  /**
   * @param {number} ms Milliseconds of fake time to deliver, timer by timer.
   * @returns {Promise<void>} Resolves once the scripted sockets answered what the timers started.
   */
  async advance(ms) {
    await jest.advanceTimersByTimeAsync(ms)
    await flush()
  }
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
 * @param {() => *} call A handler callback that must refuse.
 * @returns {void}
 */
const expectStale = (call) => {
  let thrown = null
  try {
    call()
  } catch (error) {
    thrown = error
  }
  expect(thrown).toBeInstanceOf(KafkaJSNonRetriableError)
  expect(thrown.message).toBe('Handler call is no longer current')
}

const WIRE = {
  78: { method: 'shareFetch', codec: 'ShareFetch' },
  79: { method: 'shareAcknowledge', codec: 'ShareAcknowledge' },
}

/**
 * @param {object} fields Decoded fields of a ShareAcknowledge request.
 * @returns {boolean} Whether it closes its share session, so a hold can let ordinary traffic pass.
 */
const isFinal = (fields) => fields.shareSessionEpoch === -1

const PROBE_ACK = {
  groupId: 'group',
  memberId: 'probe',
  shareSessionEpoch: 1,
  isRenewAck: false,
  topics: [],
}
const PROBE_BEAT = {
  groupId: 'group',
  memberId: 'probe',
  memberEpoch: 3,
  rackId: null,
  subscribedTopicNames: null,
}

/**
 * @param {object} connection A pooled connection, possibly replaced after a close captured it.
 * @param {'ShareAcknowledge'|'ShareGroupHeartbeat'} api The probe request to send through it.
 * @returns {Promise<object>} The decoded answer, which proves the connection carries traffic.
 */
const sendProbe = (connection, api) =>
  connection.send(
    api === 'ShareAcknowledge'
      ? requests.ShareAcknowledge.protocol({ version: 2 })(PROBE_ACK)
      : requests.ShareGroupHeartbeat.protocol({ version: 1 })(PROBE_BEAT)
  )

/** Holds one share request at one stage of its write path until the test lets it go. */
class WireHold {
  /**
   * @param {78|79} apiKey The held request's API: ShareFetch or ShareAcknowledge.
   * @param {'lookup'|'encode'|'queue'} stage Where the request waits: before the Broker call, in
   *   its encoder, or before the request queue.
   * @param {object} [options] Which request is held.
   * @param {number} [options.skip=0] Matching requests let through first.
   * @param {(fields: object) => boolean} [options.fields] Picks requests by their decoded fields;
   *   the queue stage has no fields and counts every request of the API.
   */
  constructor(apiKey, stage, { skip = 0, fields = () => true } = {}) {
    this.gate = deferred()
    this.arrival = deferred()
    this.failure = null
    this.pending = []
    const { gate, arrival } = this
    const hold = this
    const { method, codec } = WIRE[apiKey]
    let seen = 0
    let taken = false
    const take = (candidate) => {
      if (taken || (candidate !== undefined && !fields(candidate))) return false
      if (seen++ < skip) return false
      taken = true
      return true
    }
    if (stage === 'lookup') {
      const original = Broker.prototype[method]
      jest.spyOn(Broker.prototype, method).mockImplementation(function (request, options) {
        if (!take(request)) return original.call(this, request, options)
        const pending = (async () => {
          arrival.resolve()
          await gate.promise
          if (hold.failure) throw hold.failure
          return original.call(this, request, options)
        })()
        hold.pending.push(pending)
        return pending
      })
    } else if (stage === 'encode') {
      const original = requests[codec].protocol
      jest.spyOn(requests[codec], 'protocol').mockImplementation((selection) => {
        const factory = original(selection)
        return (options) => {
          const built = factory(options)
          if (!take(options)) return built
          const { encode } = built.request
          return {
            ...built,
            request: {
              ...built.request,
              encode: () => {
                const pending = (async () => {
                  arrival.resolve()
                  await gate.promise
                  return encode()
                })()
                hold.pending.push(pending)
                return pending
              },
            },
          }
        }
      })
    } else {
      const original = RequestQueue.prototype.push
      jest.spyOn(RequestQueue.prototype, 'push').mockImplementation(function (pushed) {
        if (pushed.entry.apiKey !== apiKey || !take()) return original.call(this, pushed)
        arrival.resolve()
        hold.pending.push(
          gate.promise.then(() => {
            try {
              original.call(this, pushed)
            } catch (error) {
              pushed.entry.reject(error)
            }
          })
        )
      })
    }
  }

  /** @returns {Promise<void>} Resolves once the request reached the held stage. */
  arrived() {
    return this.arrival.promise
  }

  /** @returns {void} Lets the request continue from its stage. */
  release() {
    this.gate.resolve()
  }

  /**
   * @param {Error} error The external failure the held call ends with.
   * @returns {void}
   */
  fail(error) {
    this.failure = error
    this.gate.resolve()
  }
}

/**
 * A public share client on a scripted cluster under a controlled clock, with one handler that
 * holds until `gate` resolves, plus the observers every close test reads.
 */
class CloseRig {
  /**
   * @param {object} [options] Rig options.
   * @param {number} [options.nodes=1] Node count.
   * @param {object} [options.topics] Topic metadata the wire serves.
   * @param {object} [options.share] Overrides of the shareConsumer options.
   * @param {boolean} [options.enforce=false] Transport request-timeout enforcement.
   * @param {number} [options.interval=600000] The `heartbeatIntervalMs` the coordinator answers.
   */
  constructor({ nodes, topics, share, enforce = false, interval = 600000 } = {}) {
    this.clock = new Clock()
    this.clock.install()
    this.runs = []
    this.nodes = []
    this.clusters = []
    this.holds = []
    this.calls = []
    this.order = []
    this.acks = []
    this.resets = []
    this.settling = []
    this.gate = deferred()
    this.gates = new Map()
    const { runs, clusters } = this
    const started = this.nodes
    const start = ShareRun.prototype.start
    jest.spyOn(ShareRun.prototype, 'start').mockImplementation(function () {
      runs.push(this)
      return start.call(this)
    })
    const startNode = ShareRun.prototype.startNode
    jest.spyOn(ShareRun.prototype, 'startNode').mockImplementation(function (nodeId, rows) {
      startNode.call(this, nodeId, rows)
      started.push(this.nodes.get(nodeId))
    })
    const connect = Cluster.prototype.connect
    jest.spyOn(Cluster.prototype, 'connect').mockImplementation(function (...args) {
      if (!clusters.includes(this)) clusters.push(this)
      return connect.apply(this, args)
    })
    this.h = shareClient({
      nodes,
      topics,
      share,
      interval,
      kafka: { requestTimeout: 1000, enforceRequestTimeout: enforce },
    })
    const { events, on } = this.h.consumer
    for (const name of ['STOP', 'DISCONNECT', 'CRASH'])
      on(events[name], () => this.order.push(name.toLowerCase()))
    on(events.SESSION_RESET, ({ payload }) => this.resets.push(payload.reason))
    on(events.ACKNOWLEDGE, ({ payload }) =>
      this.acks.push([
        payload.partition,
        payload.firstOffset,
        payload.lastOffset,
        payload.type,
        payload.error ? payload.error.reason : null,
      ])
    )
  }

  /**
   * Subscribes, joins, and starts handlers. A handler of a `hold` partition records its call, then
   * waits on its gate, returns, or throws, as `mode` says.
   * @param {object} [options] Start options.
   * @param {string[]} [options.records=['a']] Values the first fetch of partition 0 acquires.
   * @param {Array<object|Function>} [options.fetches] Scripted ShareFetch answers, in order.
   * @param {number[]} [options.hold=[0]] Partitions whose handler is observed.
   * @param {'accept'|'release'|'reject'} [options.decide] An explicit decision an observed handler makes first.
   * @param {'hold'|'return'|'throw'} [options.mode='hold'] Whether an observed handler waits, returns, or throws.
   * @param {Record<number, string>} [options.modes] Per-partition overrides of `mode`.
   * @param {boolean} [options.separate=false] Whether each observed partition has a gate of its own.
   * @returns {Promise<void>} Resolves once an observed handler started.
   */
  async begin({
    records = ['a'],
    fetches,
    hold = [0],
    decide,
    mode = 'hold',
    modes = {},
    separate = false,
  } = {}) {
    await this.h.consumer.subscribe({ topics: ['topic-a'] })
    this.h.script('ShareFetch', fetches ?? [fetchAnswer([acquire(0, 0, records)])])
    if (separate) for (const partition of hold) this.gates.set(partition, deferred())
    await this.h.consumer.run({
      eachMessage: (payload) => {
        const handled = this.handle(payload, { hold, decide, mode, modes, separate })
        this.settling.push(handled)
        return handled
      },
    })
    await until(() => this.calls.length > 0, 'a held handler')
  }

  /**
   * @private
   * @param {object} payload The `eachMessage` payload.
   * @param {object} how The `begin` options that steer the handler.
   * @returns {Promise<void>} The handler's own promise, which the rig awaits when it ends.
   */
  async handle(
    { partition, message, acknowledge, renew, isRunning },
    { hold, decide, mode, modes, separate }
  ) {
    if (!hold.includes(partition)) return
    this.calls.push({ partition, offset: message.offset, acknowledge, renew, isRunning })
    if (decide) acknowledge(decide)
    const completion = modes[partition] ?? mode
    if (completion === 'throw') throw new Error('handler failed')
    if (completion === 'return') return
    await (separate ? this.gates.get(partition) : this.gate).promise
  }

  /** @returns {object[]} Every final frame (ShareAcknowledge at epoch -1). */
  finals() {
    return this.h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1)
  }

  /** @returns {object[]} Every leave frame (ShareGroupHeartbeat at epoch -1). */
  leaves() {
    return this.h.frames(76).filter(({ fields }) => fields.memberEpoch === -1)
  }

  /** @returns {object[]} Every join frame (ShareGroupHeartbeat at epoch 0). */
  joins() {
    return this.h.frames(76).filter(({ fields }) => fields.memberEpoch === 0)
  }

  /** @returns {object[]} The acknowledgement batches of the one final's first partition. */
  finalBatches() {
    const [final] = this.finals()
    return final.fields.topics[0].partitions[0].acknowledgementBatches
  }

  /**
   * @param {{node: number, generation: number}} record A recorded request.
   * @returns {object} The fake socket that request travelled on.
   */
  socketOf({ node, generation }) {
    return this.h.cluster.sockets().filter(({ endpoint }) => endpoint.host === `node${node}`)[
      generation - 1
    ]
  }

  /**
   * @param {number} ms Milliseconds of fake time to deliver.
   * @returns {Promise<void>}
   */
  advance(ms) {
    return this.clock.advance(ms)
  }

  /**
   * @param {78|79} apiKey The API of the request to hold.
   * @param {'lookup'|'encode'|'queue'} stage Where the request waits.
   * @param {object} [options] Which request, as `WireHold` takes them.
   * @returns {WireHold} The hold, installed before the request is made.
   */
  holdWire(apiKey, stage, options) {
    const hold = new WireHold(apiKey, stage, options)
    this.holds.push(hold)
    return hold
  }

  /**
   * @param {() => boolean} predicate The reached-stage condition.
   * @param {string} what What is awaited, named by the failure.
   * @param {number} [step=50] Fake milliseconds delivered per check.
   * @returns {Promise<void>}
   * @throws {Error} When the condition does not hold after 100 checks.
   */
  async advanceUntil(predicate, what, step = 50) {
    for (let turn = 0; turn < 100 && !predicate(); turn++) await this.advance(step)
    if (!predicate()) throw new Error(`${what} did not happen`)
  }

  /**
   * @param {'lookup'|'encode'|'queue'} stage Where the next final waits.
   * @returns {WireHold} The hold, installed before the close starts.
   */
  holdFinal(stage) {
    return this.holdWire(79, stage, { fields: isFinal })
  }

  /**
   * Starts a close whose captured final is held, with the clock a known number of milliseconds
   * short of the bound the test names.
   * @param {'whole-call'|'final-cap'} bound The deadline the close runs against: a disconnect
   *   deadline of 400 ms, or the cap of one request timeout after the handler boundary.
   * @param {'lookup'|'encode'|'queue'|'response'} stage Where the final waits.
   * @returns {Promise<{closing: object, hold: {arrived: Function, release: Function}, remaining: number}>}
   *   The close, the hold on its final, and the milliseconds left until the bound.
   */
  async closeWithHeldFinal(bound, stage) {
    const hold =
      stage === 'response' ? this.h.hold({ api: 'ShareAcknowledge' }) : this.holdFinal(stage)
    let closing
    let remaining
    if (bound === 'whole-call') {
      await this.begin({ decide: 'accept' })
      closing = track(this.h.consumer.disconnect({ timeout: 400 }))
      await this.advance(300)
      remaining = 100
    } else {
      await this.begin()
      closing = track(this.h.consumer.stop())
      await this.advance(250)
      this.gate.resolve()
      remaining = 1000
    }
    await hold.arrived()
    return { closing, hold, remaining }
  }

  /** @returns {{arrived: () => Promise<void>, release: () => void}} A hold on the client's Cluster disconnect, so the state after the captured teardown is observable. */
  holdClusterDisconnect() {
    const original = Cluster.prototype.disconnect
    const arrival = deferred()
    const gate = deferred()
    jest.spyOn(Cluster.prototype, 'disconnect').mockImplementation(async function (...args) {
      arrival.resolve()
      await gate.promise
      return original.apply(this, args)
    })
    this.holds.push({ release: () => gate.resolve() })
    return { arrived: () => arrival.promise, release: () => gate.resolve() }
  }

  /** @returns {Promise<void>} Releases every handler gate and hold, awaits the handlers, restores timers and mocks, and closes the client. */
  async end() {
    this.gate.resolve()
    for (const gate of this.gates.values()) gate.resolve()
    for (const hold of this.holds) hold.release()
    await Promise.allSettled(this.settling)
    await Promise.all(this.holds.map((hold) => Promise.allSettled(hold.pending ?? [])))
    jest.restoreAllMocks()
    jest.useRealTimers()
    await this.h.close()
  }
}

describe('captured share close', () => {
  let rig
  afterEach(async () => {
    await rig?.end()
    rig = undefined
  })

  describe('one episode for concurrent stop and disconnect', () => {
    test('shares one captured close between a stop and later disconnects', async () => {
      rig = new CloseRig()
      await rig.begin()
      const stopping = track(rig.h.consumer.stop())
      await rig.advance(100)
      const first = track(rig.h.consumer.disconnect({ timeout: 400 }))
      const second = track(rig.h.consumer.disconnect({ timeout: 5000 }))
      await rig.advance(299)
      expect(() => rig.calls[0].acknowledge('reject')).not.toThrow()
      expect(rig.finals()).toHaveLength(0)
      expect(rig.leaves()).toHaveLength(0)
      expect([stopping, first, second].map(({ settled }) => settled)).toEqual([false, false, false])
      await rig.advance(1)
      expectStale(() => rig.calls[0].acknowledge('accept'))
      await until(() => stopping.settled && first.settled && second.settled, 'the shared close')
      expect([stopping.error, first.error, second.error]).toEqual([null, null, null])
      expect(rig.finals()).toHaveLength(1)
      expect(rig.finalBatches()).toEqual([
        { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [3] },
      ])
      expect(rig.leaves()).toHaveLength(1)
      expect(rig.order).toEqual(['stop', 'disconnect'])
      expect(rig.acks).toEqual([[0, '0', '0', 'reject', null]])
    })

    test('does not let a later disconnect extend the whole-call deadline', async () => {
      rig = new CloseRig()
      await rig.begin({ decide: 'accept' })
      const final = rig.h.hold({ api: 'ShareAcknowledge' })
      const stopping = track(rig.h.consumer.stop())
      await rig.advance(100)
      const first = track(rig.h.consumer.disconnect({ timeout: 400 }))
      const second = track(rig.h.consumer.disconnect({ timeout: 5000 }))
      await rig.advance(300)
      await final.arrived()
      expect(rig.finals()).toHaveLength(1)
      await rig.advance(99)
      expect([stopping, first, second].map(({ settled }) => settled)).toEqual([false, false, false])
      expect(rig.socketOf(rig.finals()[0]).destroyed).toBe(false)
      await rig.advance(1)
      await until(() => stopping.settled && first.settled && second.settled, 'the 500 ms deadline')
      expect(rig.socketOf(rig.finals()[0]).destroyed).toBe(true)
      expect(rig.leaves()).toHaveLength(0)
      expect(rig.acks).toEqual([[0, '0', '0', 'accept', 'session-lost']])
      expect(rig.order).toEqual(['stop', 'disconnect'])
    })

    test('keeps a natural first disconnect natural when a later call supplies a budget', async () => {
      rig = new CloseRig()
      await rig.begin()
      const first = track(rig.h.consumer.disconnect())
      await rig.advance(100)
      const second = track(rig.h.consumer.disconnect({ timeout: 400 }))
      await rig.advance(5000)
      expect([first.settled, second.settled]).toEqual([false, false])
      expect(() => rig.calls[0].acknowledge('accept')).not.toThrow()
      expect(rig.calls[0].isRunning()).toBe(false)
      expect(rig.finals()).toHaveLength(0)
      expect(rig.leaves()).toHaveLength(0)
      rig.gate.resolve()
      await until(() => first.settled && second.settled, 'the natural close')
      expect(rig.finals()).toHaveLength(1)
      expect(rig.leaves()).toHaveLength(1)
      expect(rig.order).toEqual(['stop', 'disconnect'])
    })
  })

  describe('the reserved handler boundary of a bounded disconnect', () => {
    test('fences a held handler at the boundary and releases what never started', async () => {
      rig = new CloseRig()
      await rig.begin({ records: ['a', 'b', 'c'] })
      const closing = track(rig.h.consumer.disconnect({ timeout: 400 }))
      await rig.advance(299)
      const [call] = rig.calls
      expect(call.isRunning()).toBe(false)
      expect(() => call.acknowledge('reject')).not.toThrow()
      expect(rig.finals()).toHaveLength(0)
      expect(closing.settled).toBe(false)
      await rig.advance(1)
      expectStale(() => call.acknowledge('accept'))
      expectStale(() => call.renew())
      await until(() => closing.settled, 'the close')
      expect(closing.error).toBeNull()
      expect(rig.finalBatches()).toEqual([
        { firstOffset: '0', lastOffset: '2', acknowledgeTypes: [3, 2, 2] },
      ])
      expect(rig.leaves()).toHaveLength(1)
      expect(rig.order).toEqual(['stop', 'disconnect'])
      expect(rig.acks).toEqual([
        [0, '0', '0', 'reject', null],
        [0, '1', '2', 'release', null],
      ])
    })

    test('closes the captured sockets before the client-wide disconnect at 400 ms', async () => {
      rig = new CloseRig()
      await rig.begin({ decide: 'accept' })
      const cluster = rig.holdClusterDisconnect()
      const final = rig.h.hold({ api: 'ShareAcknowledge' })
      const closing = track(rig.h.consumer.disconnect({ timeout: 400 }))
      await rig.advance(300)
      await final.arrived()
      const [frame] = rig.finals()
      const coordinator = rig.socketOf(rig.h.frames(76)[0])
      await rig.advance(99)
      expect(closing.settled).toBe(false)
      expect(rig.socketOf(frame).destroyed).toBe(false)
      expect(coordinator.destroyed).toBe(false)
      await rig.advance(1)
      await cluster.arrived()
      expect(rig.socketOf(frame).destroyed).toBe(true)
      expect(coordinator.destroyed).toBe(true)
      cluster.release()
      await until(() => closing.settled, 'the client-wide disconnect')
      expect(closing.error).toBeNull()
      expect(rig.order).toEqual(['stop', 'disconnect'])
      expect(rig.acks).toEqual([[0, '0', '0', 'accept', 'session-lost']])
    })

    test('writes nothing when the boundary timer is delivered after the whole-call deadline', async () => {
      rig = new CloseRig()
      await rig.begin({ decide: 'reject' })
      const closing = track(rig.h.consumer.disconnect({ timeout: 400 }))
      rig.clock.skew(450)
      await rig.advance(300)
      await until(() => closing.settled, 'the close')
      expect(rig.h.frames(79)).toHaveLength(0)
      expect(rig.leaves()).toHaveLength(0)
      expect(rig.acks).toEqual([[0, '0', '0', 'reject', 'not-sent']])
      expect(rig.order).toEqual(['stop', 'disconnect'])
    })
  })

  describe('an ordinary request held when a disconnect begins', () => {
    const KINDS = {
      fetch: { apiKey: 78, skip: 1, start: { mode: 'return' }, ack: [0, '0', '0', 'accept'] },
      flush: { apiKey: 79, skip: 0, start: { mode: 'throw' }, ack: [0, '0', '0', 'release'] },
    }
    const ROWS = ['fetch', 'flush'].flatMap((kind) =>
      ['lookup', 'encode', 'queue'].flatMap((stage) => [
        [kind, stage, 0],
        [kind, stage, 400],
      ])
    )

    test.each(ROWS)(
      'writes no ordinary %s request held at the %s stage when a disconnect with timeout %p begins',
      async (kind, stage, timeout) => {
        const { apiKey, skip, start, ack } = KINDS[kind]
        rig = new CloseRig()
        const hold = rig.holdWire(apiKey, stage, { skip })
        await rig.begin(start)
        await hold.arrived()
        const written = { fetches: rig.h.frames(78).length, acks: rig.h.frames(79).length }
        const closing = track(rig.h.consumer.disconnect({ timeout }))
        if (timeout > 0) await rig.advance(300)
        await until(() => rig.acks.length === 1, 'the retirement of the held request')
        expect(rig.acks).toEqual([[...ack, 'membership-lost']])
        hold.release()
        await until(() => closing.settled, 'the close')
        await flush()
        expect(closing.error).toBeNull()
        expect(rig.h.frames(78)).toHaveLength(written.fetches)
        expect(rig.h.frames(79)).toHaveLength(written.acks)
        expect(rig.finals()).toHaveLength(0)
        expect(rig.leaves()).toHaveLength(timeout === 0 ? 0 : 1)
        expect(rig.acks).toEqual([[...ack, 'membership-lost']])
        expect(rig.order).toEqual(['stop', 'disconnect'])
      }
    )
  })

  describe('an ordinary lookup beside a held sibling handler', () => {
    const KINDS = {
      fetch: { apiKey: 78, mode: 'return', type: 'accept' },
      flush: { apiKey: 79, mode: 'throw', type: 'release' },
    }

    /**
     * Holds node 0's first mutation before its Broker call while node 1 has a live handler.
     * @param {'fetch'|'flush'} kind The ordinary mutation to hold.
     * @returns {Promise<{hold: WireHold, modes: object}>} The reached hold and mutable handler modes.
     */
    const beginHeld = async (kind) => {
      const { apiKey, mode } = KINDS[kind]
      rig = new CloseRig({
        nodes: 2,
        topics: TWO_NODES,
        interval: 100,
        share: { maxPollInterval: 1000 },
      })
      const hold = rig.holdWire(apiKey, 'lookup', {
        fields: (fields) =>
          fields.shareSessionEpoch === 1 &&
          fields.topics.some(({ partitions }) =>
            partitions.some(({ partitionIndex }) => partitionIndex === 0)
          ),
      })
      const modes = { 0: mode }
      const answer = (_fields, { node }) => fetchAnswer([acquire(node, 0, [`v${node}`])])
      await rig.begin({ hold: [0, 1], modes, separate: true, fetches: [answer, answer] })
      await until(() => rig.calls.length === 2, 'both node handlers')
      await hold.arrived()
      expect(rig.h.frames(apiKey).filter(({ node }) => node === 0)).toHaveLength(
        kind === 'fetch' ? 1 : 0
      )
      expect(rig.acks).toEqual([])
      const pending = track(hold.pending[0])
      await flush()
      expect(pending.settled).toBe(false)
      return { hold, modes }
    }

    test.each(Object.keys(KINDS))(
      'confirms a current ordinary %s after its lookup is released',
      async (kind) => {
        const { hold } = await beginHeld(kind)
        hold.release()
        await until(() => rig.acks.length === 1, 'the current mutation confirmation')
        const { apiKey, type } = KINDS[kind]
        const sent = rig.h
          .frames(apiKey)
          .filter(({ node }) => node === 0)
          .at(-1)
        expect(sent.fields.shareSessionEpoch).toBe(1)
        expect(sent.fields.topics).toEqual([
          {
            topicId: A,
            partitions: [
              {
                partitionIndex: 0,
                acknowledgementBatches: [
                  {
                    firstOffset: '0',
                    lastOffset: '0',
                    acknowledgeTypes: [kind === 'fetch' ? 1 : 2],
                  },
                ],
              },
            ],
          },
        ])
        expect(rig.acks).toEqual([[0, '0', '0', type, null]])
      }
    )

    test.each(
      Object.keys(KINDS).flatMap((kind) =>
        [
          'stop boundary',
          'crash',
          'unknown member',
          'fenced epoch',
          'poll expiry',
          'replacement run',
        ].map((transition) => [kind, transition])
      )
    )(
      'retires an ordinary %s held at lookup across %s and confirms replacement work',
      async (kind, transition) => {
        const { hold, modes } = await beginHeld(kind)
        const { type } = KINDS[kind]
        const [oldRun] = rig.runs
        const oldNode = oldRun.nodes.get(0)
        const oldMember = rig.joins()[0].fields.memberId
        modes[0] = 'hold'
        const opening = rig.h.hold({
          api: 'ShareFetch',
          node: 0,
          match: ({ fields }) => fields.shareSessionEpoch === 0,
        })
        if (transition === 'unknown member' || transition === 'fenced epoch') {
          rig.h.script('ShareGroupHeartbeat', [
            { errorCode: transition === 'unknown member' ? 25 : 110 },
          ])
          await rig.advanceUntil(() => rig.joins().length === 2, 'the replacement membership')
        } else if (transition === 'poll expiry') {
          await rig.advanceUntil(
            () => oldNode.operation.error !== null,
            'the sibling handler poll expiry'
          )
          expect(rig.joins()).toHaveLength(1)
          rig.gates.get(1).resolve()
          await rig.advanceUntil(() => rig.joins().length === 2, 'the post-expiry membership')
        } else {
          if (transition === 'crash') {
            const failure = new Error('terminal lookup failure')
            await oldRun.crash(failure)
            expect(rig.order).toEqual(['crash'])
          } else {
            const stopping = track(rig.h.consumer.stop())
            const disconnecting = track(rig.h.consumer.disconnect({ timeout: 400 }))
            await rig.advance(300)
            await until(() => stopping.settled && disconnecting.settled, 'the stop boundary')
            expect([stopping.error, disconnecting.error]).toEqual([null, null])
          }
          if (transition !== 'replacement run') {
            hold.release()
            await flush()
            expect(rig.h.frames(78).filter(({ node }) => node === 0)).toHaveLength(1)
            expect(rig.h.frames(79).filter(({ node }) => node === 0)).toHaveLength(0)
          }
          await rig.h.consumer.run({
            eachMessage: (payload) => {
              const handled = rig.handle(payload, { hold: [0], modes, separate: true })
              rig.settling.push(handled)
              return handled
            },
          })
        }
        await until(() => opening.call !== null, 'the replacement opening fetch')
        expect(rig.acks.filter(([partition]) => partition === 0)).toEqual([
          [0, '0', '0', type, 'membership-lost'],
        ])
        expectStale(() => rig.calls.find(({ partition }) => partition === 0).acknowledge())
        hold.release()
        await flush()
        const opens = rig.h.frames(78).filter(({ node }) => node === 0)
        expect(opens.map(({ fields }) => fields.shareSessionEpoch)).toEqual([0, 0])
        expect(rig.h.frames(79).filter(({ node }) => node === 0)).toHaveLength(0)
        if (['stop boundary', 'crash', 'replacement run'].includes(transition))
          expect(opens[1].fields.memberId).not.toBe(oldMember)
        opening.release(fetchAnswer([acquire(0, 0, ['fresh'])]))
        await until(
          () => rig.calls.filter(({ partition }) => partition === 0).length === 2,
          'the replacement handler'
        )
        const current = rig.calls.filter(({ partition }) => partition === 0)[1]
        expect(current.isRunning()).toBe(true)
        await expect(current.renew()).resolves.toBe(15000)
        current.acknowledge('accept')
        const stopping = track(rig.h.consumer.stop())
        for (const gate of rig.gates.values()) gate.resolve()
        // A natural stop also waits for the sibling's empty-fetch pacing timer.
        await rig.advanceUntil(() => stopping.settled, 'the replacement stop')
        expect(stopping.error).toBeNull()
        expect(rig.acks.filter(([partition]) => partition === 0)).toEqual([
          [0, '0', '0', type, 'membership-lost'],
          [0, '0', '0', 'renew', null],
          [0, '0', '0', 'accept', null],
        ])
      }
    )
  })

  describe('finals of nodes that are not idle at the forced boundary', () => {
    test('skips the final of a busy node and never revives it', async () => {
      rig = new CloseRig({ nodes: 2, topics: TWO_NODES })
      const busy = rig.h.hold({
        api: 'ShareFetch',
        node: 1,
        match: ({ fields }) => fields.shareSessionEpoch > 0,
      })
      const answer = (fields, call) => fetchAnswer([acquire(call.node, 0, [`v${call.node}`])])
      await rig.begin({ decide: 'accept', fetches: [answer, answer] })
      const held = await busy.arrived()
      const fetches = rig.h.frames(78).length
      const closing = track(rig.h.consumer.disconnect({ timeout: 400 }))
      await rig.advance(300)
      await until(() => closing.settled, 'the close')
      expect(rig.h.frames(79).map(({ node }) => node)).toEqual([0])
      expect(rig.finals()).toHaveLength(1)
      expect(rig.leaves()).toHaveLength(1)
      expect(rig.h.frames(78).filter(({ fields }) => fields.shareSessionEpoch === -1)).toEqual([])
      expect(rig.socketOf(held).destroyed).toBe(true)
      expect(rig.acks.filter(([partition]) => partition === 0)).toEqual([
        [0, '0', '0', 'accept', null],
      ])
      expect(
        rig.acks.filter(([partition]) => partition === 1).every((ack) => ack[4] !== null)
      ).toBe(true)
      busy.release()
      await flush()
      expect(rig.h.frames(79).map(({ node }) => node)).toEqual([0])
      expect(rig.h.frames(78)).toHaveLength(fetches)
    })
  })

  describe('timeout zero while a close is already running', () => {
    test.each(
      ['lookup', 'encode', 'queue'].flatMap((stage) => [
        [stage, false],
        [stage, true],
      ])
    )(
      'writes no final held at the %s stage when a zero timeout arrives (transport enforcement %p)',
      async (stage, enforce) => {
        rig = new CloseRig({ enforce })
        await rig.begin({ decide: 'accept' })
        const hold = rig.holdFinal(stage)
        const closing = track(rig.h.consumer.disconnect({ timeout: 400 }))
        await rig.advance(300)
        await hold.arrived()
        expect(rig.finals()).toHaveLength(0)
        const zero = track(rig.h.consumer.disconnect({ timeout: 0 }))
        expectStale(() => rig.calls[0].acknowledge('reject'))
        hold.release()
        await until(() => closing.settled && zero.settled, 'the revoked close')
        expect(rig.h.frames(79)).toHaveLength(0)
        expect(rig.leaves()).toHaveLength(0)
        expect(rig.acks).toEqual([[0, '0', '0', 'accept', 'not-sent']])
        expect(rig.socketOf(rig.h.frames(78)[0]).destroyed).toBe(true)
      }
    )

    test.each([false, true])(
      'leaves a written final unconfirmed when a zero timeout arrives (transport enforcement %p)',
      async (enforce) => {
        rig = new CloseRig({ enforce })
        await rig.begin({ decide: 'accept' })
        const final = rig.h.hold({ api: 'ShareAcknowledge' })
        const closing = track(rig.h.consumer.disconnect({ timeout: 400 }))
        await rig.advance(300)
        await final.arrived()
        const zero = track(rig.h.consumer.disconnect({ timeout: 0 }))
        await rig.advance(100)
        await until(() => closing.settled && zero.settled, 'the close')
        expect(rig.finals()).toHaveLength(1)
        expect(rig.finals()[0].answeredOrder).toBeUndefined()
        expect(rig.leaves()).toHaveLength(0)
        expect(rig.acks).toEqual([[0, '0', '0', 'accept', 'session-lost']])
        expect(rig.socketOf(rig.finals()[0]).destroyed).toBe(true)
      }
    )

    test('returns at once for a written final when a zero timeout arrives, without a timer', async () => {
      rig = new CloseRig()
      await rig.begin({ decide: 'accept' })
      const final = rig.h.hold({ api: 'ShareAcknowledge' })
      const closing = track(rig.h.consumer.disconnect({ timeout: 400 }))
      await rig.advance(300)
      await final.arrived()
      const zero = track(rig.h.consumer.disconnect({ timeout: 0 }))
      await until(() => closing.settled && zero.settled, 'the close after the zero timeout')
      expect([closing.error, zero.error]).toEqual([null, null])
      expect(rig.acks).toEqual([[0, '0', '0', 'accept', 'session-lost']])
      expect(rig.socketOf(rig.finals()[0]).destroyed).toBe(true)
      expect(rig.leaves()).toHaveLength(0)
    })
  })

  describe('finals and leave under one cap', () => {
    test.each([false, true])(
      'caps a final at one shared deadline after the handler boundary (transport enforcement %p)',
      async (enforce) => {
        rig = new CloseRig({ enforce })
        await rig.begin()
        const final = rig.h.hold({ api: 'ShareAcknowledge' })
        const stopping = track(rig.h.consumer.stop())
        await rig.advance(250)
        expect(rig.finals()).toHaveLength(0)
        expect(stopping.settled).toBe(false)
        rig.gate.resolve()
        await final.arrived()
        expect(rig.finals()).toHaveLength(1)
        await rig.advance(999)
        expect(stopping.settled).toBe(false)
        expect(rig.leaves()).toHaveLength(0)
        expect(rig.socketOf(rig.finals()[0]).destroyed).toBe(false)
        await rig.advance(1)
        await until(() => stopping.settled, 'the capped close')
        expect(stopping.error).toBeNull()
        expect(rig.leaves()).toHaveLength(0)
        expect(rig.socketOf(rig.finals()[0]).destroyed).toBe(true)
        expect(rig.acks).toEqual([[0, '0', '0', 'accept', 'session-lost']])
        expect(rig.order).toEqual(['stop'])
      }
    )

    test('gives the leave no budget of its own after an answered final', async () => {
      rig = new CloseRig()
      await rig.begin()
      const leave = rig.h.hold({
        api: 'ShareGroupHeartbeat',
        match: ({ fields }) => fields.memberEpoch === -1,
      })
      const stopping = track(rig.h.consumer.stop())
      await rig.advance(250)
      rig.gate.resolve()
      await leave.arrived()
      expect(rig.finals()).toHaveLength(1)
      expect(rig.finals()[0].answeredOrder).toBeDefined()
      expect(rig.acks).toEqual([[0, '0', '0', 'accept', null]])
      await rig.advance(999)
      expect(stopping.settled).toBe(false)
      await rig.advance(1)
      await until(() => stopping.settled, 'the capped leave')
      expect(rig.leaves()).toHaveLength(1)
      expect(rig.leaves()[0].answeredOrder).toBeUndefined()
      expect(rig.socketOf(rig.leaves()[0]).destroyed).toBe(true)
      expect(rig.order).toEqual(['stop'])
    })

    test('waits for a held handler without a deadline on a natural stop', async () => {
      rig = new CloseRig()
      await rig.begin()
      const stopping = track(rig.h.consumer.stop())
      await rig.advance(5000)
      expect(stopping.settled).toBe(false)
      expect(rig.finals()).toHaveLength(0)
      expect(rig.leaves()).toHaveLength(0)
      expect(() => rig.calls[0].acknowledge('accept')).not.toThrow()
      rig.gate.resolve()
      await until(() => stopping.settled, 'the natural stop')
      expect(rig.finals()).toHaveLength(1)
      expect(rig.leaves()).toHaveLength(1)
    })
  })

  describe('actual time at every closing boundary', () => {
    const BOUNDS = ['whole-call', 'final-cap']
    const rows = (stages) =>
      stages.flatMap((stage) =>
        BOUNDS.flatMap((bound) => [
          [stage, bound, false],
          [stage, bound, true],
        ])
      )

    test.each(rows(['lookup', 'encode', 'queue']))(
      'writes no final held at the %s stage once the clock reaches the %s bound (transport enforcement %p)',
      async (stage, bound, enforce) => {
        rig = new CloseRig({ enforce })
        const { closing, hold, remaining } = await rig.closeWithHeldFinal(bound, stage)
        rig.clock.skew(remaining)
        hold.release()
        await until(() => closing.settled, 'the refused close')
        expect(closing.error).toBeNull()
        expect(rig.h.frames(79)).toHaveLength(0)
        expect(rig.leaves()).toHaveLength(0)
        expect(rig.acks).toEqual([[0, '0', '0', 'accept', 'not-sent']])
      }
    )

    test.each(rows(['lookup', 'encode', 'queue', 'response']))(
      'writes the final held at the %s stage when it is released a millisecond before the %s bound (transport enforcement %p)',
      async (stage, bound, enforce) => {
        rig = new CloseRig({ enforce })
        const { closing, hold, remaining } = await rig.closeWithHeldFinal(bound, stage)
        rig.clock.skew(remaining - 1)
        hold.release()
        await until(() => closing.settled, 'the control close')
        expect(closing.error).toBeNull()
        expect(rig.finals()).toHaveLength(1)
        expect(rig.finals()[0].answeredOrder).toBeDefined()
        expect(rig.leaves()).toHaveLength(1)
        expect(rig.acks).toEqual([[0, '0', '0', 'accept', null]])
      }
    )

    test.each(
      BOUNDS.flatMap((bound) => [
        [bound, false],
        [bound, true],
      ])
    )(
      'discards the answer of a written final released once the clock reaches the %s bound (transport enforcement %p)',
      async (bound, enforce) => {
        rig = new CloseRig({ enforce })
        const { closing, hold, remaining } = await rig.closeWithHeldFinal(bound, 'response')
        expect(rig.finals()).toHaveLength(1)
        rig.clock.skew(remaining)
        hold.release()
        await until(() => closing.settled, 'the late answer')
        expect(rig.finals()).toHaveLength(1)
        expect(rig.finals()[0].answeredOrder).toBeDefined()
        expect(rig.leaves()).toHaveLength(0)
        expect(rig.acks).toEqual([[0, '0', '0', 'accept', 'session-lost']])
      }
    )
  })

  describe('captured authority after ordinary retirement', () => {
    const lookups = () => rig.h.cluster.requests().filter(({ api }) => api === 'FindCoordinator')

    test('authorizes only the captured final and the leave', async () => {
      rig = new CloseRig()
      await rig.begin({ decide: 'accept' })
      const before = {
        fetches: rig.h.frames(78).length,
        lookups: lookups().length,
        sockets: rig.h.cluster.sockets().length,
      }
      const closing = track(rig.h.consumer.disconnect({ timeout: 400 }))
      await rig.advance(300)
      expectStale(() => rig.calls[0].acknowledge('reject'))
      expectStale(() => rig.calls[0].renew())
      await until(() => closing.settled, 'the close')
      expect(rig.h.frames(79)).toHaveLength(1)
      expect(rig.finals()).toHaveLength(1)
      expect(rig.finals()[0].fields.isRenewAck).toBe(false)
      expect(rig.leaves()).toHaveLength(1)
      expect(rig.h.frames(78)).toHaveLength(before.fetches)
      expect(lookups()).toHaveLength(before.lookups)
      expect(rig.h.cluster.sockets()).toHaveLength(before.sockets)
    })

    test('sends no final once the captured data connection is gone', async () => {
      rig = new CloseRig()
      await rig.begin({ decide: 'accept' })
      rig.socketOf(rig.h.frames(78)[0]).emit('end')
      await flush()
      const closing = track(rig.h.consumer.disconnect({ timeout: 400 }))
      await rig.advance(300)
      await until(() => closing.settled, 'the close')
      expect(rig.h.frames(79)).toHaveLength(0)
      expect(rig.leaves()).toHaveLength(1)
      expect(rig.acks).toEqual([[0, '0', '0', 'accept', 'membership-lost']])
    })
  })

  describe('a final answered with a top-level error', () => {
    test.each([
      [122, 'session-lost'],
      [123, 'session-lost'],
      [133, 'rejected-by-broker'],
      [35, 'rejected-by-broker'],
      [-1, 'rejected-by-broker'],
    ])('does not reopen or retry the session after code %p', async (code, reason) => {
      rig = new CloseRig()
      await rig.begin()
      rig.h.script('ShareAcknowledge', [ackAnswer([], { errorCode: code })])
      const fetches = rig.h.frames(78).length
      const stopping = track(rig.h.consumer.stop())
      rig.gate.resolve()
      await until(() => stopping.settled, 'the stop')
      expect(stopping.error).toBeNull()
      expect(rig.h.frames(79)).toHaveLength(1)
      expect(rig.finals()).toHaveLength(1)
      expect(rig.h.frames(78)).toHaveLength(fetches)
      expect(rig.leaves()).toHaveLength(1)
      expect(rig.acks).toEqual([[0, '0', '0', 'accept', reason]])
      expect(rig.resets).toEqual(['membership'])
      expect(rig.order).toEqual(['stop'])
    })

    test.each([122, 123, 133, 35, -1])(
      'delivers through a healthy replacement run after code %p',
      async (code) => {
        rig = new CloseRig()
        await rig.begin()
        rig.h.script('ShareAcknowledge', [ackAnswer([], { errorCode: code })])
        const stopping = track(rig.h.consumer.stop())
        rig.gate.resolve()
        await until(() => stopping.settled, 'the stop')
        expect(rig.h.frames(79)).toHaveLength(1)
        const delivered = []
        const fetchesBefore = rig.h.frames(78).length
        rig.h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['b'])])])
        await rig.h.consumer.run({
          eachMessage: async ({ message }) => {
            delivered.push(message.value.toString())
          },
        })
        await until(() => delivered.length === 1, 'the replacement delivery')
        expect(delivered).toEqual(['b'])
        expect(new Set(rig.joins().map(({ fields }) => fields.memberId)).size).toBe(2)
        expect(rig.h.frames(78)[fetchesBefore].fields.shareSessionEpoch).toBe(0)
        expect(rig.h.frames(79)).toHaveLength(1)
        expect(rig.leaves()).toHaveLength(1)
      }
    )
  })

  describe('captured connection generations', () => {
    test('keeps a replaced data connection open and carrying traffic when the leave is abandoned', async () => {
      rig = new CloseRig()
      await rig.begin({ decide: 'accept' })
      const cluster = rig.holdClusterDisconnect()
      const leave = rig.h.hold({
        api: 'ShareGroupHeartbeat',
        match: ({ fields }) => fields.memberEpoch === -1,
      })
      const closing = track(rig.h.consumer.disconnect({ timeout: 400 }))
      await rig.advance(300)
      await leave.arrived()
      const { binding } = rig.runs[0].nodes.get(0).captured
      const captured = rig.socketOf(rig.h.frames(78)[0])
      await binding.connection.disconnect({ drainTimeout: 0 })
      await binding.connection.connect()
      const replacement = rig.h.cluster.sockets().at(-1)
      expect(replacement).not.toBe(captured)
      await rig.advance(100)
      await cluster.arrived()
      expect(replacement.destroyed).toBe(false)
      expect(rig.socketOf(rig.leaves()[0]).destroyed).toBe(true)
      const reply = await sendProbe(binding.connection, 'ShareAcknowledge')
      expect(reply.errorCode).toBe(0)
      const probe = rig.h.frames(79).at(-1)
      expect(probe.fields.memberId).toBe('probe')
      expect(rig.socketOf(probe)).toBe(replacement)
      cluster.release()
      await until(() => closing.settled, 'the close')
      expect(replacement.destroyed).toBe(true)
    })

    test('keeps a replaced coordinator connection open and carrying traffic when the final is abandoned', async () => {
      rig = new CloseRig()
      await rig.begin({ decide: 'accept' })
      const cluster = rig.holdClusterDisconnect()
      const final = rig.h.hold({ api: 'ShareAcknowledge' })
      const closing = track(rig.h.consumer.disconnect({ timeout: 400 }))
      await rig.advance(300)
      await final.arrived()
      const { connection } = rig.runs[0].coordinator
      const oldCoordinator = rig.socketOf(rig.h.frames(76)[0])
      await connection.disconnect({ drainTimeout: 0 })
      await connection.connect()
      const replacement = rig.h.cluster.sockets().at(-1)
      expect(replacement).not.toBe(oldCoordinator)
      await rig.advance(100)
      await cluster.arrived()
      expect(replacement.destroyed).toBe(false)
      expect(rig.socketOf(rig.finals()[0]).destroyed).toBe(true)
      expect(rig.leaves()).toHaveLength(0)
      const reply = await sendProbe(connection, 'ShareGroupHeartbeat')
      expect(reply.errorCode).toBe(0)
      expect(reply.memberId).toBe('probe')
      const probe = rig.h.frames(76).at(-1)
      expect(probe.fields.memberId).toBe('probe')
      expect(rig.socketOf(probe)).toBe(replacement)
      cluster.release()
      await until(() => closing.settled, 'the close')
      expect(replacement.destroyed).toBe(true)
    })
  })

  describe('an unanswered final or leave', () => {
    test('never turns a blackholed final into an accepted one', async () => {
      rig = new CloseRig()
      await rig.begin({ decide: 'accept' })
      const final = rig.h.hold({ api: 'ShareAcknowledge' })
      const closing = track(rig.h.consumer.disconnect({ timeout: 400 }))
      await rig.advance(300)
      await final.arrived()
      await rig.advance(100)
      await until(() => closing.settled, 'the local deadline')
      expect(rig.acks).toEqual([[0, '0', '0', 'accept', 'session-lost']])
      expect(rig.finals()[0].answeredOrder).toBeUndefined()
      final.release(ackAnswer([{ partition: 0 }]))
      await flush()
      expect(rig.acks).toEqual([[0, '0', '0', 'accept', 'session-lost']])
      expect(rig.h.frames(79)).toHaveLength(1)
      expect(rig.leaves()).toHaveLength(0)
    })

    test('returns locally for a blackholed leave and never resends it', async () => {
      rig = new CloseRig()
      await rig.begin({ decide: 'accept' })
      const leave = rig.h.hold({
        api: 'ShareGroupHeartbeat',
        match: ({ fields }) => fields.memberEpoch === -1,
      })
      const closing = track(rig.h.consumer.disconnect({ timeout: 400 }))
      await rig.advance(300)
      await leave.arrived()
      expect(rig.acks).toEqual([[0, '0', '0', 'accept', null]])
      await rig.advance(99)
      expect(closing.settled).toBe(false)
      await rig.advance(1)
      await until(() => closing.settled, 'the local deadline')
      expect(rig.leaves()).toHaveLength(1)
      expect(rig.leaves()[0].answeredOrder).toBeUndefined()
      expect(rig.order).toEqual(['stop', 'disconnect'])
    })
  })

  describe('a crash or membership loss while a handler runs', () => {
    test('fences ordinary work in the crash tick and bounds a blackholed final', async () => {
      rig = new CloseRig()
      await rig.begin({ records: ['a', 'b'] })
      const final = rig.h.hold({ api: 'ShareAcknowledge' })
      const crashed = track(rig.runs[0].crash(new Error('terminal')))
      expectStale(() => rig.calls[0].acknowledge('accept'))
      expectStale(() => rig.calls[0].renew())
      await final.arrived()
      expect(rig.finalBatches()).toEqual([
        { firstOffset: '1', lastOffset: '1', acknowledgeTypes: [2] },
      ])
      await rig.advance(999)
      expect(crashed.settled).toBe(false)
      expect(rig.order).toEqual([])
      await rig.advance(1)
      await until(() => crashed.settled, 'the supervised crash')
      expect(crashed.error).toBeNull()
      expect(rig.order).toEqual(['crash'])
      expect(rig.leaves()).toHaveLength(0)
      expect(rig.acks).toEqual([
        [0, '0', '0', 'release', 'membership-lost'],
        [0, '1', '1', 'release', 'session-lost'],
      ])
    })

    test('settles a pending renewal and its undecided record membership-lost when the member crashes', async () => {
      rig = new CloseRig()
      const renewal = rig.h.hold({
        api: 'ShareAcknowledge',
        match: ({ fields }) => fields.isRenewAck === true,
      })
      await rig.begin()
      const renewing = track(rig.calls[0].renew())
      await renewal.arrived()
      const crashed = track(rig.runs[0].crash(new Error('terminal')))
      expectStale(() => rig.calls[0].acknowledge('accept'))
      expectStale(() => rig.calls[0].renew())
      await until(() => crashed.settled && renewing.settled, 'the crash and the renewal')
      expect(renewing.error).toBeInstanceOf(KafkaJSShareAcknowledgementError)
      expect(renewing.error.reason).toBe('membership-lost')
      expect(rig.finals()).toHaveLength(0)
      expect(rig.leaves()).toHaveLength(1)
      expect(rig.acks).toEqual([
        [0, '0', '0', 'renew', 'membership-lost'],
        [0, '0', '0', 'release', 'membership-lost'],
      ])
      expect(rig.order).toEqual(['crash'])
    })

    test('fences ordinary work when the member is removed and resumes after the capped close', async () => {
      rig = new CloseRig({ interval: 100 })
      await rig.begin({ decide: 'accept' })
      let fence = false
      rig.h.script(
        'ShareGroupHeartbeat',
        Array.from({ length: 100 }, () => () => {
          if (!fence) return undefined
          fence = false
          return { errorCode: 110 }
        })
      )
      const final = rig.h.hold({ api: 'ShareAcknowledge' })
      fence = true
      await rig.advance(100)
      await final.arrived()
      expectStale(() => rig.calls[0].acknowledge('reject'))
      expect(rig.finalBatches()).toEqual([
        { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] },
      ])
      expect(rig.joins()).toHaveLength(1)
      await rig.advance(999)
      expect(rig.joins()).toHaveLength(1)
      await rig.advance(1)
      await rig.advanceUntil(() => rig.joins().length === 2, 'the rejoin after the capped close')
      expect(rig.order).toEqual([])
    })
  })

  describe('listeners that fail while a close runs', () => {
    const FAILURES = [
      'Failed to execute listener: ack listener',
      'Failed to execute listener: disconnect listener',
      'Failed to execute listener: heartbeat listener',
      'Failed to execute listener: stop listener',
    ]

    /**
     * @param {'sync'|'async'} style Whether the listener throws or returns a rejected promise.
     * @param {string} message The failure's message.
     * @param {() => void} [before] Work the listener does before it fails.
     * @returns {Function} A listener that fails in that style.
     */
    const failing = (style, message, before) =>
      style === 'sync'
        ? () => {
            before?.()
            throw new Error(message)
          }
        : async () => {
            before?.()
            throw new Error(message)
          }

    test.each(['sync', 'async'])(
      'contains each %s failure once without changing the close order',
      async (style) => {
        rig = new CloseRig()
        const { consumer } = rig.h
        const logged = []
        jest.spyOn(consumer.logger(), 'error').mockImplementation((message) => {
          if (message.startsWith('Failed to execute listener')) logged.push(message)
        })
        const order = []
        const { events, on } = consumer
        for (const name of ['ACKNOWLEDGE', 'STOP', 'DISCONNECT'])
          on(events[name], () => order.push(name.toLowerCase()))
        const reentrant = []
        on(events.HEARTBEAT, failing(style, 'heartbeat listener'))
        on(
          events.ACKNOWLEDGE,
          failing(style, 'ack listener', () => reentrant.push(consumer.stop()))
        )
        on(
          events.STOP,
          failing(style, 'stop listener', () => reentrant.push(consumer.disconnect()))
        )
        on(events.DISCONNECT, failing(style, 'disconnect listener'))
        await rig.begin()
        const stopping = track(consumer.stop())
        rig.gate.resolve()
        await until(() => stopping.settled, 'the stop')
        await until(() => rig.order.includes('disconnect'), 'the reentrant disconnect')
        await Promise.all(reentrant)
        await flush()
        expect(order).toEqual(['acknowledge', 'stop', 'disconnect'])
        expect(logged.sort()).toEqual(FAILURES)
        expect(rig.finals()).toHaveLength(1)
        expect(rig.leaves()).toHaveLength(1)
        expect(rig.runs).toHaveLength(1)
        expect(consumer.assignment()).toEqual([])
      }
    )

    test.each(['sync', 'async'])(
      'contains a %s failing crash listener and still finishes the supervised crash',
      async (style) => {
        rig = new CloseRig()
        const { consumer } = rig.h
        const logged = []
        jest.spyOn(consumer.logger(), 'error').mockImplementation((message) => {
          if (message.startsWith('Failed to execute listener')) logged.push(message)
        })
        consumer.on(consumer.events.CRASH, failing(style, 'crash listener'))
        await rig.begin({ decide: 'accept' })
        await rig.runs[0].crash(new Error('terminal'))
        await flush()
        expect(rig.order).toEqual(['crash'])
        expect(logged).toEqual(['Failed to execute listener: crash listener'])
        expect(rig.finals()).toHaveLength(1)
        expect(rig.runs).toHaveLength(1)
      }
    )
  })

  describe('a poll-interval overrun', () => {
    const POLL = { share: { maxPollInterval: 1000 }, interval: 100 }

    /**
     * Runs until the overrun's capped close has ended with a handler still running.
     * @returns {Promise<void>}
     */
    const overrun = async () => {
      rig = new CloseRig(POLL)
      await rig.begin({ decide: 'accept' })
      const final = rig.h.hold({ api: 'ShareAcknowledge' })
      await rig.advanceUntil(() => final.call !== null, 'the expired handler final')
      expect(rig.runs[0].poll.expired()).toBe(true)
      expectStale(() => rig.calls[0].acknowledge('reject'))
      await rig.advance(1000)
      await until(() => rig.runs[0].closing === null, 'the end of the capped close')
    }

    test('rejoins only after the expired handler finishes', async () => {
      await overrun()
      expect(rig.joins()).toHaveLength(1)
      await rig.advance(5000)
      expect(rig.joins()).toHaveLength(1)
      rig.gate.resolve()
      await rig.advance(1)
      await until(() => rig.joins().length === 2, 'the rejoin')
      expect(rig.order).toEqual([])
    })

    test('rejoins only after every expired handler of two nodes finished', async () => {
      rig = new CloseRig({ ...POLL, nodes: 2, topics: TWO_NODES })
      const answer = (fields, call) => fetchAnswer([acquire(call.node, 0, [`v${call.node}`])])
      await rig.begin({ decide: 'accept', hold: [0, 1], fetches: [answer, answer], separate: true })
      await until(() => rig.calls.length === 2, 'both handlers')
      await rig.advanceUntil(() => rig.finals().length === 2, 'the finals of both expired nodes')
      expect(rig.runs[0].poll.expired()).toBe(true)
      await until(() => rig.runs[0].closing === null, 'the end of the close')
      expect(
        rig
          .finals()
          .map(({ node }) => node)
          .sort()
      ).toEqual([0, 1])
      expect(rig.joins()).toHaveLength(1)
      rig.gates.get(0).resolve()
      await rig.advance(5000)
      expect(rig.joins()).toHaveLength(1)
      rig.gates.get(1).resolve()
      await rig.advance(1)
      await until(() => rig.joins().length === 2, 'the rejoin after both handlers')
      expect(rig.order).toEqual([])
    })

    test.each([
      ['stop', ['stop']],
      ['disconnect', ['stop', 'disconnect']],
    ])('does not rejoin when a %s abandons the expired handler', async (call, order) => {
      await overrun()
      const closing = track(rig.h.consumer[call]())
      await until(() => closing.settled, `the ${call}`)
      expect(closing.error).toBeNull()
      rig.gate.resolve()
      await rig.advance(5000)
      expect(rig.joins()).toHaveLength(1)
      expect(rig.order).toEqual(order)
      expect(rig.runs).toHaveLength(1)
    })
  })

  describe('resources after every close outcome', () => {
    const OUTCOMES = {
      'a clean stop': {
        act: async (r) => {
          const stopping = r.h.consumer.stop()
          r.gate.resolve()
          await stopping
        },
      },
      'a failed final': {
        act: async (r) => {
          r.h.script('ShareAcknowledge', [ackAnswer([], { errorCode: 122 })])
          const stopping = r.h.consumer.stop()
          r.gate.resolve()
          await stopping
        },
      },
      'a zero-timeout disconnect': { act: (r) => r.h.consumer.disconnect({ timeout: 0 }) },
      'an unanswered final at the deadline': {
        act: async (r) => {
          const final = r.h.hold({ api: 'ShareAcknowledge' })
          const closing = r.h.consumer.disconnect({ timeout: 400 })
          await r.advance(300)
          await final.arrived()
          await r.advance(100)
          await closing
          final.release()
        },
      },
      'a crash': { act: (r) => r.runs[0].crash(new Error('terminal')) },
      'a crash and its restart': {
        share: { retry: { restartOnFailure: async () => true } },
        act: async (r) => {
          const crashed = r.runs[0].crash(new Error('terminal'))
          await r.advanceUntil(() => r.runs.length === 2, 'the restarted run')
          await crashed
          await r.advanceUntil(() => r.nodes.length === 2 && r.nodes[1].loop, 'the restarted node')
          const stopping = track(r.h.consumer.stop())
          await r.advanceUntil(() => stopping.settled, 'the stop of the restarted run')
          expect(stopping.error).toBeNull()
        },
      },
    }

    /**
     * @param {object} run A finished run.
     * @returns {void} Asserts it holds no timer, waiter, lookup, node or captured reference.
     */
    const expectReleased = (run) => {
      for (const operation of [
        run.operation,
        run.closing.operation,
        run.closing.boundaryOperation,
      ].filter(Boolean)) {
        expect(operation.timer).toBeNull()
        expect([operation.waiters.size, operation.disposers.size]).toEqual([0, 0])
      }
      expect(run.retired).toBe(true)
      expect(run.closing.run).toBeNull()
      expect(run.closing.nodes.size).toBe(0)
      expect([run.nodes.size, run.waiters.size, run.lookups.size]).toEqual([0, 0, 0])
      expectPoolReleased(run.coordinator.pool)
    }

    /**
     * @param {object} node A node record of any run.
     * @returns {void} Asserts its loop, FIFO, operations and request queues are empty.
     */
    const expectNodeReleased = (node) => {
      expect([node.loop.cohorts.size, node.loop.exchanges.size, node.session.queue.length]).toEqual(
        [0, 0, 0]
      )
      for (const operation of [node.operation, node.pacing])
        expect([operation.waiters.size, operation.disposers.size]).toEqual([0, 0])
      expectPoolReleased(node.loop.broker.connectionPool)
    }

    /**
     * @param {object} pool A real pool retained after its run ended.
     * @returns {void} Asserts both slots have no requests or pool-drain listeners left.
     */
    const expectPoolReleased = (pool) => {
      expect(pool.pool).toHaveLength(2)
      for (const { requestQueue } of pool.pool) {
        expect([requestQueue.pending.length, requestQueue.inflight.size]).toEqual([0, 0])
        expect(requestQueue.eventNames()).toEqual([])
      }
    }

    /**
     * @param {object} emitter The client emitter, whose application listeners survive close.
     * @returns {Map<string|symbol, Function[]>} Exact event names and listener identities.
     */
    const listenerSnapshot = (emitter) =>
      new Map(emitter.eventNames().map((name) => [name, emitter.listeners(name)]))

    test.each(Object.keys(OUTCOMES))('returns to baseline after %s', async (outcome) => {
      const { act, share } = OUTCOMES[outcome]
      rig = new CloseRig({ share })
      await rig.h.consumer.connect()
      const timers = jest.getTimerCount()
      const [cluster] = rig.clusters
      const { emitter } = cluster.instrumentationEmitter
      const userListeners = () =>
        emitter.eventNames().reduce((total, name) => total + emitter.listenerCount(name), 0)
      const observers = userListeners()
      expect(observers).toBe(5)
      const retained = listenerSnapshot(emitter)
      const forwarding = new Set()
      const add = Emitter.prototype.addListener
      jest.spyOn(Emitter.prototype, 'addListener').mockImplementation(function (...args) {
        const remove = add.apply(this, args)
        const owned = { emitter: this, remove }
        forwarding.add(owned)
        return () => {
          remove()
          forwarding.delete(owned)
        }
      })
      const baselinePools = new Set([
        cluster.brokerPool.seedBroker.connectionPool,
        ...Object.values(cluster.brokerPool.brokers).map((broker) => broker.connectionPool),
      ])
      for (const pool of baselinePools) expectPoolReleased(pool)
      await rig.begin({ decide: 'accept' })
      const routing = cluster.metadataCache.routingListeners
      const listeners = routing.size
      await act(rig)
      await flush()
      for (const hold of rig.holds) hold.release()
      rig.gate.resolve()
      await Promise.allSettled(rig.settling)
      await flush()
      const frames = rig.h.cluster.requests().length
      await rig.advance(5000)
      expect(rig.h.cluster.requests()).toHaveLength(frames)
      expect(rig.runs).toHaveLength(outcome === 'a crash and its restart' ? 2 : 1)
      for (const run of rig.runs) expectReleased(run)
      expect(rig.nodes).toHaveLength(rig.runs.length)
      for (const node of rig.nodes) expectNodeReleased(node)
      expect(routing.size).toBe(listeners - 1)
      expect(userListeners()).toBe(observers)
      expect(listenerSnapshot(emitter)).toEqual(retained)
      expect(forwarding.size).toBe(0)
      const pools = new Set([
        ...baselinePools,
        ...rig.runs.map((run) => run.coordinator.pool),
        ...rig.nodes.map((node) => node.loop.broker.connectionPool),
        ...Object.values(cluster.brokerPool.brokers).map((broker) => broker.connectionPool),
      ])
      for (const pool of pools) expectPoolReleased(pool)
      expect(jest.getTimerCount()).toBeLessThanOrEqual(timers)
      const closing = track(rig.h.consumer.disconnect())
      await rig.advanceUntil(() => closing.settled, 'the client-wide disconnect')
      expect(closing.error).toBeNull()
      expect(rig.h.cluster.sockets().every(({ destroyed }) => destroyed)).toBe(true)
      for (const pool of pools) expectPoolReleased(pool)
      for (const socket of rig.h.cluster.sockets()) {
        expect(['data', 'end', 'timeout'].map((event) => socket.listenerCount(event))).toEqual([
          0, 0, 0,
        ])
      }
      expect(listenerSnapshot(emitter)).toEqual(retained)
      expect(forwarding.size).toBe(0)
    })
  })

  describe('late rejected work', () => {
    test('is observed after teardown without sending, scheduling or restarting', async () => {
      rig = new CloseRig()
      await rig.begin({ decide: 'accept' })
      const external = rig.holdFinal('lookup')
      const closing = track(rig.h.consumer.disconnect({ timeout: 400 }))
      await rig.advance(300)
      await external.arrived()
      await rig.advance(100)
      await until(() => closing.settled, 'the local deadline')
      const after = {
        frames: rig.h.cluster.requests().length,
        sockets: rig.h.cluster.sockets().length,
        timers: jest.getTimerCount(),
        listeners: rig.runs[0].options.cluster.metadataCache.routingListeners.size,
        acks: rig.acks.length,
      }
      external.fail(new Error('late external failure'))
      await flush()
      await rig.advance(5000)
      expect(rig.h.cluster.requests()).toHaveLength(after.frames)
      expect(rig.h.cluster.sockets()).toHaveLength(after.sockets)
      expect(jest.getTimerCount()).toBeLessThanOrEqual(after.timers)
      expect(rig.runs[0].options.cluster.metadataCache.routingListeners.size).toBe(after.listeners)
      expect(rig.acks).toHaveLength(after.acks)
      expect(rig.acks).toEqual([[0, '0', '0', 'accept', 'not-sent']])
      expect(rig.order).toEqual(['stop', 'disconnect'])
      expect(rig.runs).toHaveLength(1)
      expect(rig.joins()).toHaveLength(1)
    })
  })
})
