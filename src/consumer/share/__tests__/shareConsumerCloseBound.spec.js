const Cluster = require('../../../cluster')
const Connection = require('../../../network/connection')
const { ShareRun } = require('../shareRun')
const { LEVELS } = require('../../../loggers')
const {
  A,
  ShareClock,
  acquire,
  deferred,
  echo,
  fetchAnswer,
  flush,
  installSessionModel,
  shareClient,
  until,
} = require('../../../../testHelpers/shareHarness')

const REQUEST_TIMEOUT = 1000
const DRAIN = REQUEST_TIMEOUT / 2

/**
 * @param {number} count Partitions, partition `n` led by node `n`.
 * @returns {object} The `topics` option of `shareClient`.
 */
const ledBy = (count) => ({
  'topic-a': {
    topicId: A,
    partitions: Array.from({ length: count }, (_, leader) => ({ leader, epoch: 1 })),
  },
})

/**
 * @param {Promise<*>} promise A close or run promise.
 * @returns {{settled: boolean, error: Error|null}} Its state, updated when it settles.
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
 * A share client over the scripted cluster with the broker session model installed, on the test
 * clock, with transport timeouts disabled. Every gate, held handler and mock is released in `end`.
 */
class BoundRig {
  /**
   * @param {object} [options] Rig options.
   * @param {number} [options.nodes=1] Leader nodes; partition `n` is led by node `n`.
   * @param {object} [options.share] Overrides of the share consumer options.
   * @param {number} [options.interval=600000] The heartbeat interval the coordinator answers.
   */
  constructor({ nodes = 1, share, interval = 600000 } = {}) {
    this.clock = new ShareClock().install()
    this.runs = []
    this.gates = []
    this.settling = []
    this.closes = []
    this.lines = []
    this.acks = []
    const { runs, closes } = this
    const start = ShareRun.prototype.start
    jest.spyOn(ShareRun.prototype, 'start').mockImplementation(function () {
      runs.push(this)
      return start.call(this)
    })
    const disconnect = Connection.prototype.disconnect
    jest.spyOn(Connection.prototype, 'disconnect').mockImplementation(function (...args) {
      closes.push(this.socket)
      return disconnect.apply(this, args)
    })
    this.h = shareClient({
      nodes,
      topics: ledBy(nodes),
      share,
      interval,
      kafka: {
        requestTimeout: REQUEST_TIMEOUT,
        enforceRequestTimeout: false,
        logLevel: LEVELS.WARN,
        logCreator: () => (entry) => this.lines.push(entry),
      },
    })
    this.model = installSessionModel(this.h)
    this.h.consumer.on(this.h.consumer.events.ACKNOWLEDGE, ({ payload }) =>
      this.acks.push([
        payload.partition,
        payload.firstOffset,
        payload.lastOffset,
        payload.type,
        payload.error ? payload.error.reason : null,
      ])
    )
  }

  /** @returns {ShareRun} The first run. */
  get run() {
    return this.runs[0]
  }

  /**
   * Answers each node's opening fetch with one record of its own partition, once, while `armed`
   * says so; every other fetch is quiet.
   * @param {(node: number) => boolean} [armed] Whether a node may deliver now.
   * @returns {void}
   */
  serve(armed = () => true) {
    const given = new Set()
    this.h.cluster.answer('ShareFetch', (fields, { node }) => {
      if (given.has(node) || !armed(node)) return echo('ShareFetch', fields)
      given.add(node)
      return fetchAnswer([acquire(node, 0, [`v${node}`])])
    })
  }

  /**
   * Subscribes and runs `eachMessage`, tracking every handler promise.
   * @param {Function} [handler] The application handler.
   * @returns {Promise<void>} Resolves once the member joined.
   */
  async begin(handler = async () => {}) {
    await this.h.consumer.subscribe({ topics: ['topic-a'] })
    await this.h.consumer.run({
      eachMessage: (payload) => {
        const handled = handler(payload)
        this.settling.push(handled)
        return handled
      },
    })
    await until(() => this.run !== undefined, 'the run')
  }

  /** @returns {{promise: Promise<void>, resolve: () => void}} A handler gate released in `end`. */
  gate() {
    const gate = deferred()
    this.gates.push(gate)
    return gate
  }

  /**
   * Holds the next ordinary fetch of a node at the wire, before the broker executes it.
   * @param {number} node Node id.
   * @returns {object} The schedule gate.
   */
  holdFetch(node) {
    return this.h.hold({
      api: 'ShareFetch',
      node,
      match: ({ fields }) => fields.shareSessionEpoch > 0,
    })
  }

  /**
   * Holds the client-wide Cluster disconnect that follows a share disconnect, so the closes the
   * close episode itself made can be counted before the client closes everything else.
   * @returns {{reached: () => boolean}} Whether the client-wide disconnect began.
   */
  holdClusterDisconnect() {
    const gate = this.gate()
    const original = Cluster.prototype.disconnect
    let reached = false
    jest.spyOn(Cluster.prototype, 'disconnect').mockImplementation(async function (...args) {
      reached = true
      await gate.promise
      return original.apply(this, args)
    })
    return { reached: () => reached, release: () => gate.resolve() }
  }

  /** @returns {object} The schedule gate of the next final (ShareAcknowledge at epoch -1). */
  holdFinal() {
    return this.h.hold({
      api: 'ShareAcknowledge',
      match: ({ fields }) => fields.shareSessionEpoch === -1,
    })
  }

  /**
   * @param {number} node Node id.
   * @returns {boolean} Whether the node's current session is established.
   */
  established(node) {
    return this.run.nodes.get(node)?.session?.epoch > 0
  }

  /**
   * Moves the clock in 50 ms steps until the condition holds.
   * @param {() => boolean} predicate The reached-stage condition.
   * @param {string} what What is awaited, named by the failure.
   * @returns {Promise<void>}
   * @throws {Error} When it does not hold within 40 steps.
   */
  async advanceUntil(predicate, what) {
    for (let step = 0; step < 40 && !predicate(); step++) await this.clock.advance(50)
    if (!predicate()) throw new Error(`${what} did not happen`)
  }

  /** @returns {number} The test clock. */
  now() {
    return performance.now()
  }

  /** @returns {string} The member id of the first join. */
  member() {
    return this.joins()[0].fields.memberId
  }

  /** @returns {object[]} Every final frame (ShareAcknowledge at epoch -1). */
  finals() {
    return this.h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1)
  }

  /** @returns {object[]} Every leave frame. */
  leaves() {
    return this.h.frames(76).filter(({ fields }) => fields.memberEpoch === -1)
  }

  /** @returns {object[]} Every join frame. */
  joins() {
    return this.h.frames(76).filter(({ fields }) => fields.memberEpoch === 0)
  }

  /**
   * @param {{node: number, generation: number}} record A recorded request.
   * @returns {object} The socket it travelled on.
   */
  socketOf({ node, generation }) {
    return this.h.cluster.sockets().filter(({ endpoint }) => endpoint.host === `node${node}`)[
      generation - 1
    ]
  }

  /**
   * @param {{node: number, generation: number}} record A recorded request.
   * @returns {number} How often the client disconnected the connection it travelled on.
   */
  closesOf(record) {
    const socket = this.socketOf(record)
    return this.closes.filter((closed) => closed === socket).length
  }

  /**
   * @param {string} reason A leave-skip reason.
   * @returns {number} How often the leave was reported skipped for it.
   */
  skipped(reason) {
    return this.lines.filter(
      ({ log }) => log.message === 'Share leave skipped' && log.reason === reason
    ).length
  }

  /** @returns {Promise<void>} Releases every gate and handler, closes the client and restores time. */
  async end() {
    for (const gate of this.gates) gate.resolve()
    await Promise.allSettled(this.settling)
    try {
      await this.h.close()
    } finally {
      this.clock.restore()
      jest.restoreAllMocks()
    }
  }
}

describe('bounded share consumer stop', () => {
  let rig
  afterEach(async () => {
    await rig?.end()
    rig = undefined
  })

  test('stop waits for an open handler without a cap, then ends within the request timeout', async () => {
    rig = new BoundRig({ nodes: 2 })
    rig.serve((node) => node === 0)
    const held = rig.gate()
    const busy = rig.holdFetch(1)
    let started = 0
    await rig.begin(async () => {
      started++
      await held.promise
    })
    await rig.advanceUntil(
      () => started === 1 && busy.call !== null,
      'the handler and the held fetch'
    )
    const stopping = track(rig.h.consumer.stop())
    for (let second = 0; second < 10; second++) await rig.clock.advance(1000)
    expect(stopping.settled).toBe(false)
    expect(rig.finals()).toHaveLength(0)
    held.resolve()
    await rig.clock.advance(REQUEST_TIMEOUT - 1)
    await until(() => stopping.settled, 'the stop within the request timeout')
    expect(stopping.error).toBeNull()
    expect(rig.finals().map(({ node }) => node)).toEqual([0])
    expect(rig.closesOf(busy.call)).toBe(1)
    expect(rig.model.sessions()).toEqual([])
  })

  test('stop freezes once at half the request timeout: the idle node finals, the busy node is closed, and the member leaves', async () => {
    rig = new BoundRig({ nodes: 3 })
    const findBroker = Cluster.prototype.findBroker
    jest.spyOn(Cluster.prototype, 'findBroker').mockImplementation(function (args) {
      if (args.nodeId === 2) return new Promise(() => {})
      return findBroker.call(this, args)
    })
    const busy = rig.holdFetch(1)
    await rig.begin()
    await rig.advanceUntil(
      () => rig.established(0) && busy.call !== null,
      'an established idle node and a held fetch'
    )
    await flush()
    const stopping = track(rig.h.consumer.stop())
    await rig.clock.advance(DRAIN - 1)
    expect(rig.finals()).toHaveLength(0)
    expect(rig.closesOf(busy.call)).toBe(0)
    expect(stopping.settled).toBe(false)
    await rig.clock.advance(1)
    await until(() => stopping.settled, 'the stop at the drain bound')
    expect(rig.finals().map(({ node }) => node)).toEqual([0])
    expect(rig.finals()[0].answeredOrder).toBeDefined()
    expect(rig.closesOf(busy.call)).toBe(1)
    expect(rig.model.session(1, rig.member())).toBeNull()
    expect(rig.leaves()).toHaveLength(1)
    expect(rig.h.cluster.requests().filter(({ node }) => node === 2)).toEqual([])
    expect(rig.model.sessions()).toEqual([])
  })

  test('stop freezes at once when every node is idle', async () => {
    rig = new BoundRig({ nodes: 2 })
    await rig.begin()
    await rig.advanceUntil(() => rig.established(0) && rig.established(1), 'two sessions')
    await flush()
    const stopping = track(rig.h.consumer.stop())
    await until(() => stopping.settled, 'the stop without a timer')
    const nodes = rig.finals().map(({ node }) => node)
    expect(nodes.sort()).toEqual([0, 1])
    expect(rig.leaves()).toHaveLength(1)
    expect(rig.model.sessions()).toEqual([])
  })

  test('a final held past the cap is cut at the cap, its connection is closed once and the leave is skipped', async () => {
    rig = new BoundRig()
    await rig.begin()
    await rig.advanceUntil(() => rig.established(0), 'the session')
    await flush()
    const final = rig.holdFinal()
    const stopping = track(rig.h.consumer.stop())
    await rig.advanceUntil(() => final.call !== null, 'the held final')
    await rig.clock.advance(rig.run.closing.finalDeadline - rig.now() - 1)
    expect(stopping.settled).toBe(false)
    expect(rig.closesOf(final.call)).toBe(0)
    await rig.clock.advance(1)
    await until(() => stopping.settled, 'the capped stop')
    expect(rig.closesOf(final.call)).toBe(1)
    expect(rig.leaves()).toHaveLength(0)
    expect(rig.skipped('close-deadline')).toBe(1)
  })

  test('a clean stop keeps no connection to an unfinished share session', async () => {
    rig = new BoundRig({ nodes: 3 })
    const busy = rig.holdFetch(2)
    await rig.begin()
    await rig.advanceUntil(
      () => rig.established(0) && rig.established(1) && busy.call !== null,
      'two idle sessions and a held fetch'
    )
    await flush()
    const stopping = track(rig.h.consumer.stop())
    await rig.clock.advance(DRAIN)
    await until(() => stopping.settled, 'the stop')
    const finals = rig.finals()
    expect(finals.map(({ node }) => node).sort()).toEqual([0, 1])
    for (const final of finals) {
      expect(final.answeredOrder).toBeDefined()
      expect(rig.closesOf(final)).toBe(0)
    }
    expect(rig.closesOf(busy.call)).toBe(1)
    expect(rig.model.sessions()).toEqual([])
  })

  test('a handler that throws during stop sends no extra flush; its release goes into the final', async () => {
    rig = new BoundRig()
    rig.serve()
    const held = rig.gate()
    let started = false
    await rig.begin(async () => {
      started = true
      await held.promise
      throw new Error('handler failed')
    })
    await until(() => started, 'the held handler')
    const stopping = track(rig.h.consumer.stop())
    await flush()
    held.resolve()
    await until(() => stopping.settled, 'the stop')
    const acknowledgements = rig.h.frames(79)
    expect(acknowledgements).toHaveLength(1)
    expect(acknowledgements[0].fields.shareSessionEpoch).toBe(-1)
    expect(acknowledgements[0].fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
      { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [2] },
    ])
    expect(rig.acks).toEqual([[0, '0', '0', 'release', null]])
    expect(rig.model.acquisitionsOf(A, 0, 0).map(({ state }) => state)).toEqual(['AVAILABLE'])
  })

  test('stop while a poll recovery waits for a never-returning expired call resolves and does not rejoin', async () => {
    rig = new BoundRig({ share: { maxPollInterval: 200 }, interval: 50 })
    rig.serve()
    const held = rig.gate()
    let started = false
    await rig.begin(async () => {
      started = true
      await held.promise
    })
    await until(() => started, 'the held handler')
    await rig.advanceUntil(() => rig.leaves().length === 1, 'the poll-expiry leave')
    const stopping = track(rig.h.consumer.stop())
    await rig.advanceUntil(() => stopping.settled, 'the stop')
    await rig.clock.advance(1000)
    expect(stopping.error).toBeNull()
    expect(rig.joins()).toHaveLength(1)
    expect(rig.leaves()).toHaveLength(1)
  })

  test('stop after a rejoin does not wait for a non-expired sibling call that the poll close froze', async () => {
    rig = new BoundRig({ nodes: 2, share: { maxPollInterval: 200 }, interval: 50 })
    let late = false
    rig.serve((node) => node === 0 || late)
    const gates = { 0: rig.gate(), 1: rig.gate() }
    const started = new Set()
    await rig.begin(async ({ partition }) => {
      started.add(partition)
      await gates[partition].promise
    })
    await until(() => started.has(0), 'the first handler')
    await rig.clock.advance(100)
    late = true
    await rig.advanceUntil(() => started.has(1), 'the later sibling handler')
    await rig.advanceUntil(() => rig.leaves().length === 1, 'the poll-expiry leave')
    gates[0].resolve()
    await rig.advanceUntil(() => rig.joins().length === 2, 'the rejoin')
    const stopping = track(rig.h.consumer.stop())
    await rig.advanceUntil(() => stopping.settled, 'the stop beside the frozen sibling')
    expect(stopping.error).toBeNull()
  })

  test('a disconnect with timeout 0 during the drain ends it in the same tick', async () => {
    rig = new BoundRig({ nodes: 2 })
    const busy = rig.holdFetch(1)
    await rig.begin()
    await rig.advanceUntil(() => rig.established(0) && busy.call !== null, 'the held fetch')
    await flush()
    const cluster = rig.holdClusterDisconnect()
    const stopping = track(rig.h.consumer.stop())
    await rig.clock.advance(100)
    expect(stopping.settled).toBe(false)
    const zero = track(rig.h.consumer.disconnect({ timeout: 0 }))
    expect(rig.run.closing.frozen).toBe(true)
    await until(() => stopping.settled && cluster.reached(), 'the revoked close')
    expect(rig.finals()).toHaveLength(0)
    expect(rig.leaves()).toHaveLength(0)
    expect(rig.closesOf(busy.call)).toBe(1)
    cluster.release()
    await until(() => zero.settled, 'the client-wide disconnect')
    expect(zero.error).toBeNull()
  })

  test('a later disconnect with an earlier handler boundary ends the drain exactly at the new boundary', async () => {
    rig = new BoundRig({ nodes: 2 })
    const busy = rig.holdFetch(1)
    await rig.begin()
    await rig.advanceUntil(() => rig.established(0) && busy.call !== null, 'the held fetch')
    await flush()
    const cluster = rig.holdClusterDisconnect()
    const stopping = track(rig.h.consumer.stop())
    await rig.clock.advance(100)
    const bounded = track(rig.h.consumer.disconnect({ timeout: 200 }))
    await rig.clock.advance(149)
    expect(rig.finals()).toHaveLength(0)
    expect(rig.closesOf(busy.call)).toBe(0)
    await rig.clock.advance(1)
    await until(() => stopping.settled && cluster.reached(), 'the close at the new boundary')
    expect(rig.finals().map(({ node }) => node)).toEqual([0])
    expect(rig.closesOf(busy.call)).toBe(1)
    cluster.release()
    await until(() => bounded.settled, 'the client-wide disconnect')
  })

  test('a disconnect during the drain still cuts a hung final at its deadline', async () => {
    rig = new BoundRig({ nodes: 2 })
    const busy = rig.holdFetch(1)
    await rig.begin()
    await rig.advanceUntil(() => rig.established(0) && busy.call !== null, 'the held fetch')
    await flush()
    const final = rig.holdFinal()
    const cluster = rig.holdClusterDisconnect()
    const stopping = track(rig.h.consumer.stop())
    await rig.clock.advance(100)
    const bounded = track(rig.h.consumer.disconnect({ timeout: 200 }))
    await rig.clock.advance(150)
    await final.arrived()
    await rig.clock.advance(49)
    expect(stopping.settled).toBe(false)
    expect(rig.closesOf(final.call)).toBe(0)
    await rig.clock.advance(1)
    await until(() => stopping.settled && cluster.reached(), 'the final cut at the deadline')
    expect(rig.closesOf(final.call)).toBe(1)
    expect(rig.leaves()).toHaveLength(0)
    cluster.release()
    await until(() => bounded.settled, 'the client-wide disconnect')
  })

  test("a flush still queued after a node's drive ended keeps that node busy until the drain ends", async () => {
    rig = new BoundRig()
    rig.serve()
    const flushHeld = rig.h.hold({
      api: 'ShareAcknowledge',
      match: ({ fields }) => fields.shareSessionEpoch > 0,
    })
    await rig.begin(async () => {
      throw new Error('handler failed')
    })
    const held = await flushHeld.arrived()
    const stopping = track(rig.h.consumer.stop())
    await rig.clock.advance(DRAIN - 1)
    expect(rig.closesOf(held)).toBe(0)
    expect(stopping.settled).toBe(false)
    await rig.clock.advance(1)
    await until(() => stopping.settled, 'the stop at the drain bound')
    expect(rig.closesOf(held)).toBe(1)
    expect(rig.finals()).toHaveLength(0)
    expect(rig.model.sessions()).toEqual([])
  })

  test('an escalation during the handler wait skips the drain', async () => {
    rig = new BoundRig({ nodes: 2 })
    rig.serve((node) => node === 0)
    const held = rig.gate()
    const busy = rig.holdFetch(1)
    let started = false
    await rig.begin(async () => {
      started = true
      await held.promise
    })
    await rig.advanceUntil(() => started && busy.call !== null, 'the handler and the held fetch')
    const stopping = track(rig.h.consumer.stop())
    await flush()
    const episode = rig.run.closing
    const crashed = rig.run.crash(new Error('terminal'))
    expect(episode.frozen).toBe(true)
    await until(() => stopping.settled, 'the escalated stop without a timer')
    await crashed
    expect(episode.drainGate).toBeUndefined()
    expect(rig.finals().map(({ node }) => node)).toEqual([0])
  })

  test('stop during the first join resolves and writes nothing after it', async () => {
    rig = new BoundRig()
    const join = rig.h.hold({ api: 'ShareGroupHeartbeat' })
    await rig.h.consumer.subscribe({ topics: ['topic-a'] })
    const running = track(rig.h.consumer.run({ eachMessage: async () => {} }))
    await join.arrived()
    await until(() => rig.run !== undefined, 'the run')
    const stopping = track(rig.h.consumer.stop())
    await until(() => stopping.settled && running.settled, 'the stop and the run promise')
    expect(stopping.error).toBeNull()
    const written = rig.h.cluster.requests().length
    join.release()
    await rig.clock.advance(1000)
    expect(rig.h.cluster.requests()).toHaveLength(written)
    expect(rig.h.frames(78, 79)).toEqual([])
    expect(rig.leaves()).toHaveLength(0)
  })

  test('stop during a reset backoff resolves and writes nothing after it', async () => {
    rig = new BoundRig()
    const resets = []
    rig.h.consumer.on(rig.h.consumer.events.SESSION_RESET, ({ payload }) =>
      resets.push(payload.reason)
    )
    await rig.begin()
    await rig.advanceUntil(() => rig.established(0), 'the session')
    await flush()
    const opens = rig.h.frames(78).length
    const [open] = rig.h.frames(78)
    rig.socketOf(open).emit('end')
    for (let step = 0; step < 10 && resets.length === 0; step++) await rig.clock.advance(10)
    expect(resets).toEqual(['connection'])
    const stopping = track(rig.h.consumer.stop())
    await rig.advanceUntil(() => stopping.settled, 'the stop')
    const written = rig.h.cluster.requests().length
    await rig.clock.advance(1000)
    expect(rig.h.cluster.requests()).toHaveLength(written)
    expect(rig.h.frames(78)).toHaveLength(opens)
    expect(rig.finals()).toHaveLength(0)
  })

  test('a current node whose drive already ended when admission stopped still sends its final', async () => {
    rig = new BoundRig({ nodes: 2 })
    const busy = rig.holdFetch(1)
    await rig.begin()
    await rig.advanceUntil(() => rig.established(0) && busy.call !== null, 'the held fetch')
    await flush()
    const node = rig.run.nodes.get(0)
    const stopping = track(rig.h.consumer.stop())
    const drive = track(node.drive)
    await rig.clock.advance(100)
    await until(() => drive.settled, 'the idle drive ending at the stop')
    expect(rig.finals()).toHaveLength(0)
    expect(rig.run.nodes.get(0)).toBe(node)
    await rig.clock.advance(DRAIN - 100)
    await until(() => stopping.settled, 'the stop')
    expect(rig.finals().map(({ node: id }) => id)).toEqual([0])
    expect(rig.finals()[0].answeredOrder).toBeDefined()
  })

  test('a captured final left unanswered at the cap is closed once in cleanup after its drive settled', async () => {
    rig = new BoundRig()
    await rig.begin()
    await rig.advanceUntil(() => rig.established(0), 'the session')
    await flush()
    const final = rig.holdFinal()
    const cluster = rig.holdClusterDisconnect()
    const node = rig.run.nodes.get(0)
    const closing = track(rig.h.consumer.disconnect())
    await rig.advanceUntil(() => final.call !== null, 'the held final')
    const drive = track(node.drive)
    await until(() => drive.settled, 'the settled drive')
    await rig.clock.advance(rig.run.closing.finalDeadline - rig.now() - 1)
    expect(rig.closesOf(final.call)).toBe(0)
    await rig.clock.advance(1)
    await until(() => cluster.reached(), 'the end of the capped close')
    expect(rig.closesOf(final.call)).toBe(1)
    expect(rig.leaves()).toHaveLength(0)
    cluster.release()
    await until(() => closing.settled, 'the client-wide disconnect')
  })

  test('a handler call frozen by a close that never returns cannot poll-expire the next membership', async () => {
    rig = new BoundRig({ nodes: 2, share: { maxPollInterval: 200 }, interval: 50 })
    let late = false
    rig.serve((node) => node === 0 || late)
    const gates = { 0: rig.gate(), 1: rig.gate() }
    const started = new Set()
    await rig.begin(async ({ partition }) => {
      started.add(partition)
      await gates[partition].promise
    })
    await until(() => started.has(0), 'the first handler')
    await rig.clock.advance(100)
    late = true
    await rig.advanceUntil(() => started.has(1), 'the later sibling handler')
    await rig.advanceUntil(() => rig.leaves().length === 1, 'the poll-expiry leave')
    gates[0].resolve()
    await rig.advanceUntil(() => rig.joins().length === 2, 'the rejoin')
    for (let step = 0; step < 12; step++) await rig.clock.advance(50)
    expect(rig.leaves()).toHaveLength(1)
    expect(rig.joins()).toHaveLength(2)
  })
})

describe('bounded stop with a detached node record', () => {
  let rig
  afterEach(async () => {
    await rig?.end()
    rig = undefined
  })

  /**
   * Revokes the node's only partition, holds the final its drive then sends mid-run, and restores
   * the partition, so the ended record is detached while a replacement record takes the node.
   * @param {object} [options] Fixture options.
   * @param {boolean} [options.stopAtRestore=false] Stop the run in the tick the replacement record
   *   is created, before it can bind a connection.
   * @returns {Promise<{final: object, stopping: object|null, stoppedAt: number|null}>} The held
   *   mid-run final, and the stop started at the restore (if any) with its clock reading.
   */
  const detachHeldFinal = async ({ stopAtRestore = false } = {}) => {
    rig = new BoundRig({ interval: 50 })
    let next = null
    rig.h.script(
      'ShareGroupHeartbeat',
      Array.from({ length: 400 }, () => (fields) => {
        if (fields.memberEpoch <= 0 || next === null) return undefined
        const assignment = next === 'revoke' ? [] : [{ topicId: A, partitions: [0] }]
        next = null
        return {
          errorCode: 0,
          errorMessage: null,
          memberId: fields.memberId,
          memberEpoch: fields.memberEpoch,
          heartbeatIntervalMs: 50,
          assignment: { topicPartitions: assignment },
        }
      })
    )
    let stopping = null
    let stoppedAt = null
    let restoring = false
    const startNode = ShareRun.prototype.startNode
    jest.spyOn(ShareRun.prototype, 'startNode').mockImplementation(function (...args) {
      startNode.apply(this, args)
      if (stopAtRestore && next === null && restoring && stopping === null) {
        stoppedAt = performance.now()
        stopping = track(rig.h.consumer.stop())
      }
    })
    const final = rig.holdFinal()
    await rig.begin()
    await rig.advanceUntil(() => rig.established(0), 'the session')
    const original = rig.run.nodes.get(0)
    next = 'revoke'
    await rig.advanceUntil(() => final.call !== null, 'the held mid-run final')
    restoring = true
    next = 'restore'
    await rig.advanceUntil(() => rig.run.nodes.get(0) !== original, 'the replacement record')
    return { final, stopping, stoppedAt }
  }

  test('after revoke and restore with the old request held, stop ends within the request timeout', async () => {
    await detachHeldFinal()
    await rig.advanceUntil(() => rig.established(0), 'the replacement session')
    await flush()
    const stopping = track(rig.h.consumer.stop())
    await rig.clock.advance(REQUEST_TIMEOUT - 1)
    await until(() => stopping.settled, 'the stop within the request timeout')
    expect(stopping.error).toBeNull()
    expect(rig.leaves()).toHaveLength(1)
  })

  test('a detached node record on its own connection is drained and closed at stop without touching the replacement', async () => {
    const { final, stopping, stoppedAt } = await detachHeldFinal({ stopAtRestore: true })
    expect(stopping).not.toBeNull()
    const written = rig.h.frames(78).length
    await rig.clock.advance(stoppedAt + DRAIN - rig.now() - 1)
    expect(rig.closesOf(final.call)).toBe(0)
    await rig.clock.advance(1)
    await until(() => stopping.settled, 'the stop at the drain bound')
    expect(rig.closesOf(final.call)).toBe(1)
    expect(rig.h.frames(78)).toHaveLength(written)
    expect(rig.finals()).toHaveLength(1)
    expect(rig.model.sessions()).toEqual([])
    expect(rig.run.detached.size).toBe(0)
  })

  test("a detached node record that shares the replacement's connection never closes it before the replacement's final", async () => {
    const { final } = await detachHeldFinal()
    await rig.advanceUntil(() => rig.established(0), 'the replacement session')
    await flush()
    const stopping = track(rig.h.consumer.stop())
    await rig.clock.advance(DRAIN - 1)
    expect(rig.finals()).toHaveLength(1)
    await rig.clock.advance(1)
    await until(() => stopping.settled, 'the stop at the drain bound')
    const [, replacementFinal] = rig.finals()
    expect(rig.finals()).toHaveLength(2)
    expect(replacementFinal.generation).toBe(final.call.generation)
    expect(replacementFinal.answeredOrder).toBeDefined()
    expect(rig.closesOf(final.call)).toBe(0)
    expect(rig.model.sessions()).toEqual([])
  })

  test('a finished close episode keeps no node record, binding or connection', async () => {
    await detachHeldFinal()
    await rig.advanceUntil(() => rig.established(0), 'the replacement session')
    await flush()
    const stopping = track(rig.h.consumer.stop())
    const episode = rig.run.closing
    expect(episode.records).toHaveLength(2)
    await rig.clock.advance(DRAIN)
    await until(() => stopping.settled, 'the stop')
    expect(episode.records).toBeNull()
    expect(episode.bindings.size).toBe(0)
    expect(episode.closedConnections.size).toBe(0)
    expect(rig.run.detached.size).toBe(0)
  })
})
