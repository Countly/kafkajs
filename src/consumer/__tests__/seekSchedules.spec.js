/**
 * Seeded seek schedules checked against an independent model. An exploratory tool, not a gate:
 * it is not part of the unit selectors, and a failing seed is a lead to reproduce as a named case
 * in `seekFencing.spec.js`, not a verdict. Run it by name.
 */
const { AsyncLocalStorage } = require('node:async_hooks')
const Broker = require('../../broker')
const Cluster = require('../../cluster')
const RequestQueue = require('../../network/requestQueue')
const ConsumerGroup = require('../consumerGroup')
const OffsetManager = require('../offsetManager')
const SeekOffsets = require('../seekOffsets')
const { KafkaJSConnectionError, KafkaJSNonRetriableError } = require('../../errors')
const { TOPIC, CloseRig, batchAt, gate } = require('./fixtures/closeRig')

const PARTITIONS = [0, 1]
const LAST = 7
const LEADER_EPOCH = 5
const SETTLE_MS = 3
const QUIET_MS = 2000
const OPEN_MS = 10000
const TICK_MS = 5

/**
 * Advances fake time, running whatever falls due and the I/O turns it schedules.
 * @param {number} ms
 * @returns {Promise<void>}
 */
const advance = (ms) => jest.advanceTimersByTimeAsync(ms)

/**
 * Advances fake time until a promise settles.
 * @template T
 * @param {Promise<T>} promise
 * @param {number} [budget=OPEN_MS] fake milliseconds it may take
 * @returns {Promise<T>} its outcome
 * @throws {Error} its rejection, or when it is still pending after the budget
 */
const drive = async (promise, budget = OPEN_MS) => {
  const state = { done: false }
  const settled = promise.finally(() => {
    state.done = true
  })
  settled.catch(() => {})
  for (let spent = 0; !state.done && spent < budget; spent += TICK_MS) await advance(TICK_MS)
  if (!state.done) throw new Error(`still pending after ${budget} fake ms`)
  return settled
}

/**
 * A seeded linear congruential generator, so a schedule is rebuilt from its seed alone.
 * @param {number} seed
 * @returns {{ int: (n: number) => number, pick: <T>(items: T[]) => T }}
 */
const lcg = (seed) => {
  let state = seed >>> 0
  const next = () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0
    return state / 2 ** 32
  }
  const int = (n) => Math.floor(next() * n)
  return { int, pick: (items) => items[int(items.length)] }
}

/**
 * The steps a schedule is made of, with their weights: seeks (valid and not), handler work and
 * commits of every kind, seek applications, assignment changes, recreation, out-of-range answers,
 * listeners that seek, and the fate of each OffsetCommit and its write.
 */
const STEP_KINDS = [
  [6, (r) => ['seek', r.pick(PARTITIONS), r.int(LAST + 1)]],
  [1, (r) => ['seekInvalid', r.pick(['0', '1', null, ''])]],
  [6, (r) => ['resolve', r.pick(PARTITIONS)]],
  [2, (r) => ['handlerCommit', r.pick(PARTITIONS)]],
  [2, (r) => ['handlerExplicit', r.pick(PARTITIONS)]],
  [3, () => ['defaultCommit']],
  [2, (r) => ['outsideCommit', r.pick(PARTITIONS), r.int(LAST + 3)]],
  [4, (r) => ['position', r.pick(PARTITIONS)]],
  [3, () => ['retain']],
  [1, (r) => ['revoke', r.pick(PARTITIONS)]],
  [1, () => ['rebuild']],
  [1, () => ['recreate']],
  [2, (r) => ['listenerSeek', r.pick(['START', 'COMMIT']), r.pick(PARTITIONS), r.int(LAST + 1)]],
  [6, (r) => ['reply', r.pick(['held', 'held', 'rejected', 'lost', 'lostUnapplied'])]],
  [3, () => ['releaseReplies']],
  [2, () => ['holdWrite']],
  [2, () => ['releaseWrite']],
  [1, (r) => ['outOfRange', r.pick(PARTITIONS)]],
  [1, () => ['releaseFetch']],
  [1, (r) => ['epoch', r.pick(PARTITIONS)]],
  [1, (r) => ['releaseValidation', r.int(LAST + 1)]],
]

/**
 * Schedules that reach the orderings a named regression pins, so a seed can find them without the
 * named test: a default commit whose answer is held while one of its cells is sought, and an
 * accepted seek whose applying call is refused by an assignment change before its answer.
 * Random steps are inserted into a skeleton.
 */
const SKELETONS = [
  [
    ['resolve', 0],
    ['resolve', 1],
    ['reply', 'held'],
    ['defaultCommit'],
    ['seek', 0, 3],
    ['releaseReplies'],
  ],
  [
    ['resolve', 0],
    ['seek', 0, 1],
    ['reply', 'held'],
    ['position', 0],
    ['retain'],
    ['releaseReplies'],
  ],
  [
    ['resolve', 1],
    ['seek', 1, 0],
    ['reply', 'held'],
    ['position', 1],
    ['rebuild'],
    ['releaseReplies'],
  ],
]
const TOTAL_WEIGHT = STEP_KINDS.reduce((sum, [weight]) => sum + weight, 0)

/**
 * @param {number} seed
 * @returns {{ seed: number, autoCommit: boolean, steps: Array<Array<string|number|null>> }}
 */
const generate = (seed) => {
  const r = lcg(seed)
  const random = () => {
    let roll = r.int(TOTAL_WEIGHT)
    for (const [weight, make] of STEP_KINDS) {
      if (roll < weight) return make(r)
      roll -= weight
    }
  }
  const skeleton = r.int(3) === 0 ? r.pick(SKELETONS) : null
  const steps = skeleton ? skeleton.map((step) => [...step]) : []
  const added = skeleton ? r.int(5) : 6 + r.int(8)
  for (let i = 0; i < added; i++) steps.splice(r.int(steps.length + 1), 0, random())
  return { seed, autoCommit: r.int(2) === 0, steps }
}

/**
 * Replaces a prototype method for one schedule, calling the original through `wrap`.
 * @returns {() => void} restores it
 */
const around = (target, name, wrap) => {
  const original = target[name]
  const spy = jest.spyOn(target, name).mockImplementation(function (...args) {
    return wrap.call(this, original, args)
  })
  return () => spy.mockRestore()
}

/** @param {object[]} topics @returns {string} the cells of an OffsetCommit, order-free */
const signature = (topics) =>
  topics
    .flatMap(({ partitions }) => partitions.map((c) => `${c.partition}:${String(c.offset)}`))
    .sort()
    .join(',')

/** @param {string|null|undefined} value a broker offset @returns {string|undefined} its position */
const asPosition = (value) => (value === '-2' ? '0' : value)

/**
 * One run of one schedule against a real consumer on a scripted two-partition topic, on fake time
 * so a seed always gives the same interleaving. Everything the invariants read is an observed
 * event with its order: steps (seek admissions, resolutions, assignment changes, recreations),
 * deliveries, commit selections, every OffsetCommit, OffsetFetch, out-of-range answer and
 * validation answer the broker saw with the operation that sent it and its fate, the start and
 * end of each seek application and when a fetch was built.
 */
class ScheduleRun {
  /**
   * @param {'classic'|'consumer'} protocol
   * @param {{autoCommit: boolean, steps: Array<Array<unknown>>}} schedule
   */
  constructor(protocol, schedule) {
    this.protocol = protocol
    this.schedule = schedule
    this.time = 0
    this.events = []
    this.broker = { 0: '2', 1: '2' }
    this.current = { 0: null, 1: null }
    this.deliveries = 0
    this.pending = []
    this.replies = []
    this.heldReplies = []
    this.heldWrites = []
    this.holdWrites = false
    this.heldFetches = []
    this.outOfRange = new Set()
    this.epochs = { 0: 0, 1: 0 }
    this.armedValidations = new Set()
    this.heldValidations = []
    this.listenerSeeks = []
    this.draining = false
    this.candidates = []
    this.attempts = new Set()
    this.ops = new AsyncLocalStorage()
    this.popped = new WeakSet()
    this.restores = []
    this.group = null
    this.consumer = null
    this.rig = null
    this.violations = []
  }

  /** @returns {number} the next event time */
  tick() {
    return ++this.time
  }

  /**
   * @param {object} event
   * @returns {object} the event, stamped with its time
   */
  record(event) {
    event.t = this.tick()
    this.events.push(event)
    return event
  }

  /** @param {Promise<unknown>} promise @param {object} [op] @returns {void} */
  track(promise, op = {}) {
    const entry = Object.assign(op, { settled: false, t: this.tick() })
    this.pending.push(entry)
    promise.then(
      (value) => Object.assign(entry, { settled: true, settledAt: this.tick(), value }),
      (error) => Object.assign(entry, { settled: true, settledAt: this.tick(), error })
    )
  }

  /** @returns {Promise<void>} */
  async open() {
    const log = {}
    for (const p of PARTITIONS)
      for (let offset = 0; offset <= LAST; offset++) log[`${p}|${offset}`] = await batchAt(offset)
    const committed = this.broker
    this.rig = await CloseRig.open({
      protocol: this.protocol,
      last: -1,
      log,
      committed,
      partitions: 2,
    })
    this.instrument()
    const run = {
      autoCommit: this.schedule.autoCommit,
      autoCommitInterval: 60000,
      autoCommitThreshold: 1,
      eachBatchAutoResolve: false,
      partitionsConsumedConcurrently: 2,
      eachMessage: undefined,
      eachBatch: (payload) => this.handle(payload),
    }
    const { consumer } = await drive(
      this.rig.started({ consumer: { maxWaitTimeInMs: 5 }, run, delivered: 0 })
    )
    this.consumer = consumer
    for (const [name, event] of [
      ['START', consumer.events.START_BATCH_PROCESS],
      ['COMMIT', consumer.events.COMMIT_OFFSETS],
    ]) {
      consumer.on(event, (e) => {
        const index = this.listenerSeeks.findIndex(
          (l) => l.on === name && (name === 'COMMIT' || l.partition === e.payload.partition)
        )
        if (index === -1) return
        const [{ partition, offset }] = this.listenerSeeks.splice(index, 1)
        consumer.seek({ topic: TOPIC, partition, offset: String(offset) })
      })
    }
    for (let spent = 0; !this.group || !this.current[0] || !this.current[1]; spent += TICK_MS) {
      if (spent > OPEN_MS) throw new Error('the first deliveries did not arrive')
      await advance(TICK_MS)
    }
  }

  /** Installs the observers; each is restored by `dispose`. */
  instrument() {
    const run = this
    const { ops } = this
    this.restores.push(
      around(ConsumerGroup.prototype, 'applyRebalanceResult', function (original, args) {
        run.group ??= this
        return original.apply(this, args)
      }),
      around(ConsumerGroup.prototype, 'buildFetchRequests', function (original, args) {
        const requests = original.apply(this, args)
        for (const { partitions } of requests) {
          for (const { partition } of partitions) run.record({ type: 'build', partition })
        }
        return requests
      }),
      around(Cluster.prototype, 'leaderEpoch', function (original, args) {
        const epoch = original.apply(this, args)
        return epoch == null ? epoch : epoch + (run.epochs[args[1]] ?? 0)
      }),
      around(ConsumerGroup.prototype, 'seek', function (original, args) {
        const [{ partition, offset }] = args
        run.record({ type: 'admit', partition, target: offset })
        return original.apply(this, args)
      }),
      around(SeekOffsets.prototype, 'pop', function (original, args) {
        const entry = original.apply(this, args)
        if (entry) run.popped.add(entry)
        return entry
      }),
      around(OffsetManager.prototype, 'seek', function (original, args) {
        const [entry] = args
        if (!run.popped.has(entry)) return original.apply(this, args)
        const admission = run.latest('admit', entry.partition)
        const attempt = run.record({ type: 'attempt', partition: entry.partition, admission })
        run.attempts.add(attempt)
        const finish = (outcome) => {
          run.attempts.delete(attempt)
          run.record({ type: 'attemptEnd', attempt, ...outcome })
        }
        return ops.run({ ...ops.getStore(), seek: attempt }, () =>
          original.apply(this, args).then(
            (applied) => {
              finish({ applied, accepted: args[2]?.accepted === true })
              return applied
            },
            (error) => {
              finish({ error, accepted: args[2]?.accepted === true })
              throw error
            }
          )
        )
      }),
      around(OffsetManager.prototype, 'commitWithOwnership', function (original, args) {
        const outer = ops.getStore()
        if (outer?.commit) return original.apply(this, args)
        const [offsets] = args
        const commit = {
          kind: offsets.topics ? 'explicit' : 'default',
          delivery: outer?.delivery ?? null,
          op: outer?.outside ?? outer?.op ?? null,
          selection: null,
          selectedAt: null,
        }
        if (outer?.outside) outer.outside.commit = commit
        return ops.run({ delivery: outer?.delivery, commit }, () => original.apply(this, args))
      }),
      around(OffsetManager.prototype, 'selectUncommitted', function (original, args) {
        const selected = original.apply(this, args)
        const commit = ops.getStore()?.commit
        if (commit && commit.selection === null) {
          commit.selection = signature(selected).split(',').filter(Boolean)
          commit.selectedAt = run.tick()
        }
        return selected
      }),
      around(ConsumerGroup.prototype, 'recoverFromOffsetOutOfRange', function (original, args) {
        const [e] = args
        const fetched = run.latest('fetchOutOfRange', e.partition)
        return ops.run({ ...ops.getStore(), reset: { partition: e.partition, fetched } }, () =>
          original.apply(this, args)
        )
      }),
      around(Broker.prototype, 'offsetCommit', function (original, args) {
        const [request] = args
        const candidate = { sig: signature(request.topics), store: ops.getStore(), live: true }
        run.candidates.push(candidate)
        const done = () => {
          candidate.live = false
        }
        const result = original.apply(this, args)
        result.then(done, done)
        return result
      }),
      around(RequestQueue.prototype, 'sendSocketRequest', function (original, args) {
        const [request] = args
        if (!run.holdWrites || request.entry.apiName !== 'OffsetCommit') {
          return original.apply(this, args)
        }
        run.holdWrites = false
        const opened = gate()
        run.heldWrites.push(opened)
        // Released after its connection closed, the write throws: the request rejects instead.
        opened.promise.then(() => {
          try {
            original.apply(this, args)
          } catch (error) {
            request.rejected(new KafkaJSConnectionError(`Held write not sent: ${error.message}`))
          }
        })
      })
    )

    const answer = this.rig.cluster.answerers.get('OffsetCommit')
    this.rig.cluster.answer('OffsetCommit', (fields, record) =>
      this.commitArrived(fields, record, answer)
    )
    const offsetFetch = this.rig.cluster.answerers.get('OffsetFetch')
    this.rig.cluster.answer('OffsetFetch', (fields, record) => {
      this.record({ type: 'offsetFetch' })
      return offsetFetch(fields, record)
    })
    this.rig.cluster.answer('OffsetForLeaderEpoch', (fields) => this.validationArrived(fields))
    const fetch = this.rig.cluster.answerers.get('Fetch')
    this.rig.cluster.answer('Fetch', async (fields, record) => {
      const asked = fields.topics[0].partitions.map(({ partition }) => partition)
      const hit = asked.find((p) => this.outOfRange.has(p))
      if (hit === undefined) return fetch(fields, record)
      this.outOfRange.delete(hit)
      const fetched = this.record({
        type: 'fetchOutOfRange',
        partition: hit,
        built: this.latest('build', hit),
      })
      const opened = gate()
      this.heldFetches.push(opened)
      await opened.promise
      // Released by the drain, the answer is an empty one: only a step answers out of range.
      const errorCode = this.draining ? 0 : 1
      if (errorCode) this.record({ type: 'outOfRangeAnswered', partition: hit, fetched })
      return {
        partitions: asked.map((partition) => ({
          partition,
          errorCode: partition === hit ? errorCode : 0,
          records: false,
          abortedTransactions: [],
        })),
      }
    })
  }

  /**
   * The broker side of position validation: a partition whose leader epoch a step moved is held
   * until `releaseValidation` answers its end offset; any other is answered with no truncation.
   */
  async validationArrived({ topics }) {
    const asked = topics[0].partitions.map(({ partition }) => partition)
    const held = asked.filter((p) => this.armedValidations.delete(p))
    const entry = { endOffset: null, opened: gate() }
    if (held.length > 0) {
      for (const partition of held) this.record({ type: 'validationAsked', partition, entry })
      this.heldValidations.push(entry)
      await entry.opened.promise
    }
    return {
      partitions: asked.map((partition) => ({
        partition,
        errorCode: 0,
        leaderEpoch: LEADER_EPOCH + this.epochs[partition],
        endOffset: held.includes(partition) ? String(entry.endOffset) : String(LAST + 1),
      })),
    }
  }

  /**
   * The broker side of every OffsetCommit: the frame is matched to the operation that sent it, and
   * answered as the schedule says: at once, held after it was applied, rejected without applying,
   * applied and lost with its connection, or lost with its connection before it was applied.
   */
  async commitArrived(fields, record, answer) {
    const sig = signature(fields.topics)
    const candidate = [...this.candidates].reverse().find((c) => c.live && c.sig === sig)
    if (candidate) candidate.live = false
    const store = candidate?.store ?? {}
    const outcome = this.draining ? 'ok' : (this.replies.shift() ?? 'ok')
    const cells = fields.topics.flatMap(({ partitions }) =>
      partitions.map(({ partition, offset }) => ({ partition, offset: String(offset) }))
    )
    const arrival = this.record({
      type: 'arrival',
      cells,
      outcome,
      commit: store.commit ?? null,
      seek: store.seek ?? null,
      reset: store.reset ?? null,
    })
    if (outcome === 'rejected') {
      return {
        topics: fields.topics.map((topic) => ({
          ...topic,
          partitions: topic.partitions.map(({ partition }) => ({ partition, errorCode: 7 })),
        })),
      }
    }
    if (outcome === 'lostUnapplied') throw new Error('connection lost before the broker applied it')
    for (const { partition, offset } of cells) this.broker[partition] = offset
    arrival.broker = { ...this.broker }
    if (outcome === 'lost') throw new Error('connection lost after the write')
    const result = await answer(fields, record)
    if (outcome === 'held') {
      const opened = gate()
      this.heldReplies.push(opened)
      await opened.promise
      arrival.answeredAt = this.tick()
    }
    return result
  }

  /** The handler: every delivery is recorded and held until a step ends it. */
  async handle(payload) {
    const { partition, messages } = payload.batch
    if (messages.length === 0) return
    const delivery = this.record({
      type: 'deliver',
      partition,
      first: messages[0].offset,
      last: messages[messages.length - 1].offset,
      payload,
      done: gate(),
    })
    if (this.draining) return
    this.current[partition] = delivery
    await delivery.done.promise
  }

  /**
   * @param {string} type
   * @param {number} partition
   * @param {number} [before=Infinity]
   * @returns {object|null} the latest such event of the partition before a time
   */
  latest(type, partition, before = Infinity) {
    for (let i = this.events.length - 1; i >= 0; i--) {
      const event = this.events[i]
      if (event.t < before && event.type === type && event.partition === partition) return event
    }
    return null
  }

  /** @param {number} partition @returns {object|null} the held delivery, taken */
  take(partition) {
    const delivery = this.current[partition]
    this.current[partition] = null
    return delivery
  }

  /** @param {Array<unknown>} step @returns {Promise<void>} */
  async apply([kind, a, b, c]) {
    const { group, consumer } = this
    if (['retain', 'revoke', 'rebuild', 'recreate'].includes(kind))
      this.record({ type: 'reassign' })
    if (kind === 'revoke') this.record({ type: 'retire', partition: a, kind })
    if (kind === 'rebuild')
      for (const p of PARTITIONS) this.record({ type: 'retire', partition: p, kind })
    const commitIn = (delivery, op, offsets) =>
      this.ops.run({ delivery, op }, () => delivery.payload.commitOffsetsIfNecessary(offsets))
    switch (kind) {
      case 'seek':
        consumer.seek({ topic: TOPIC, partition: a, offset: String(b) })
        break
      case 'seekInvalid': {
        let threw = null
        try {
          consumer.seek({ topic: TOPIC, partition: a, offset: '0' })
        } catch (error) {
          threw = error
        }
        this.record({ type: 'invalidSeek', value: a, threw })
        break
      }
      case 'resolve':
      case 'handlerCommit':
      case 'handlerExplicit': {
        const delivery = this.take(a)
        if (!delivery) break
        const op = { kind, delivery }
        const work = async () => {
          if (kind !== 'handlerExplicit') {
            delivery.payload.resolveOffset(delivery.last)
            this.record({
              type: 'resolve',
              partition: a,
              delivery,
              value: String(Number(delivery.last) + 1),
            })
          }
          if (kind === 'handlerCommit') await commitIn(delivery, op)
          if (kind === 'handlerExplicit') {
            const offset = String(Number(delivery.last) + 1)
            await commitIn(delivery, op, {
              topics: [{ topic: TOPIC, partitions: [{ partition: a, offset }] }],
            })
          }
        }
        const working = work()
        this.track(working, op)
        working.finally(() => delivery.done.resolve()).catch(() => {})
        break
      }
      case 'defaultCommit':
      case 'outsideCommit': {
        const op = { kind, commit: null }
        const call = () =>
          kind === 'defaultCommit'
            ? group.commitOffsets({}, { auto: true })
            : consumer.commitOffsets([{ topic: TOPIC, partition: a, offset: String(b) }])
        this.track(this.ops.run({ outside: op }, call), op)
        break
      }
      case 'position':
        this.track(consumer.position({ topic: TOPIC, partition: a }), { kind })
        break
      case 'retain':
        this.track(
          group.applyRebalanceResult(
            { assignment: { [TOPIC]: PARTITIONS } },
            { rebuildOffsets: false }
          ),
          { kind }
        )
        break
      case 'revoke':
        this.track(
          group
            .applyRebalanceResult({ assignment: { [TOPIC]: [1 - a] } }, { rebuildOffsets: false })
            .then(() =>
              group.applyRebalanceResult(
                { assignment: { [TOPIC]: PARTITIONS } },
                { rebuildOffsets: false }
              )
            ),
          { kind }
        )
        break
      case 'rebuild':
        this.track(group.applyRebalanceResult({ assignment: { [TOPIC]: PARTITIONS } }), { kind })
        break
      case 'recreate':
        this.record({ type: 'recreate' })
        group.invalidateTopic(TOPIC)
        break
      case 'listenerSeek':
        this.listenerSeeks.push({ on: a, partition: b, offset: c })
        break
      case 'reply':
        this.replies.push(a)
        break
      case 'releaseReplies':
        for (const held of this.heldReplies.splice(0)) held.resolve()
        break
      case 'holdWrite':
        this.holdWrites = true
        break
      case 'releaseWrite':
        for (const held of this.heldWrites.splice(0)) held.resolve()
        break
      case 'outOfRange':
        this.outOfRange.add(a)
        break
      case 'releaseFetch':
        for (const held of this.heldFetches.splice(0)) held.resolve()
        break
      case 'epoch':
        this.epochs[a]++
        this.armedValidations.add(a)
        break
      case 'releaseValidation':
        for (const entry of this.heldValidations.splice(0)) {
          entry.endOffset = a
          this.record({ type: 'validationAnswered', entry, endOffset: String(a) })
          entry.opened.resolve()
        }
        break
      default:
        throw new Error(`unknown step ${kind}`)
    }
  }

  /** Releases every hold and every handler, and waits until nothing is in flight. */
  async drain() {
    this.draining = true
    this.holdWrites = false
    this.replies.length = 0
    this.listenerSeeks.length = 0
    this.outOfRange.clear()
    this.armedValidations.clear()
    for (let spent = 0; ; spent += TICK_MS) {
      for (const held of [
        ...this.heldWrites.splice(0),
        ...this.heldReplies.splice(0),
        ...this.heldFetches.splice(0),
      ])
        held.resolve()
      for (const entry of this.heldValidations.splice(0)) {
        entry.endOffset = LAST + 1
        entry.opened.resolve()
      }
      for (const p of PARTITIONS) this.take(p)?.done.resolve()
      const settled = this.pending.every((op) => op.settled) && this.attempts.size === 0
      // An owed seek is applied by the fetch loop on its own; a stranded one is left to checkGates.
      if (settled && !this.group.hasSeekGates()) break
      if (spent > QUIET_MS) {
        if (!settled) {
          this.violate('I6', 'work still in flight after every hold was released', {
            pending: this.pending.filter((op) => !op.settled).map((op) => op.kind),
            attempts: this.attempts.size,
          })
        }
        break
      }
      await advance(TICK_MS)
    }
    await advance(20)
  }

  /** @param {string} check @param {string} message @param {object} [detail] */
  violate(check, message, detail = {}) {
    this.violations.push({ check, message, detail })
  }

  /** @returns {Promise<void>} runs the steps, drains, checks the invariants */
  async execute() {
    await this.open()
    for (const step of this.schedule.steps) {
      await this.apply(step)
      await advance(SETTLE_MS)
    }
    await this.drain()
    new Model(this).check()
  }

  /** @returns {Promise<void>} */
  async dispose() {
    this.draining = true
    for (const held of [...this.heldWrites, ...this.heldReplies, ...this.heldFetches])
      held.resolve()
    for (const entry of this.heldValidations) entry.opened.resolve()
    for (const p of PARTITIONS) this.take(p)?.done.resolve()
    try {
      if (this.rig) await drive(this.rig.dispose())
    } finally {
      for (const restore of this.restores.reverse()) restore()
    }
  }
}

/**
 * The reference model of one partition: what may be committed, what may be delivered, where the
 * position must be, and whether a seek is still owed. It is computed from the steps and the
 * broker's side alone; the client's own state (its acquisitions, positions, cache or fetch
 * offsets) never decides what is right. The client's timing (when an application starts and
 * ends, when a fetch is built) only orders the events.
 */
class Model {
  /** @param {ScheduleRun} run */
  constructor(run) {
    this.run = run
    this.events = run.events
    this.autoCommit = run.schedule.autoCommit
  }

  /** @param {object} delivery @param {number} at @returns {boolean} still entitled to progress */
  authorized(delivery, at) {
    return !this.events.some(
      (e) =>
        e.t > delivery.t &&
        e.t < at &&
        (e.type === 'recreate' ||
          ((e.type === 'admit' || e.type === 'retire') && e.partition === delivery.partition))
    )
  }

  /**
   * Whether anything that makes an answer to an earlier request stale reached the partition
   * between two times: a seek admission, the end of its acquisition or a recreation.
   * @param {number} partition
   * @param {number} from
   * @param {number} to
   * @returns {boolean}
   */
  changedBetween(partition, from, to) {
    return this.events.some(
      (e) =>
        e.t > from &&
        e.t < to &&
        (e.type === 'recreate' ||
          ((e.type === 'admit' || e.type === 'retire') && e.partition === partition))
    )
  }

  /**
   * The state of one partition at a time, derived from the steps and the broker's side alone: its
   * resolved progress, the latest seek admission and whether it is still owed, the broker's
   * committed offset, and whether a reset put the position at the log start.
   * @param {number} partition
   * @param {number} [until=Infinity]
   */
  stateAt(partition, until = Infinity) {
    const state = {
      resolved: null,
      admission: null,
      owed: false,
      applying: false,
      broker: '2',
      reset: false,
      truncation: null,
    }
    for (const e of this.events) {
      if (e.t >= until) break
      if (e.type === 'admit' && e.partition === partition) {
        // Admission keeps the position: a seek that fails leaves the partition where it was.
        state.admission = e
        state.owed = true
        state.applying = false
      } else if (e.type === 'recreate') {
        // The scripted topic keeps its committed offsets, so the new incarnation resumes there.
        state.resolved = null
        state.owed = false
        state.reset = false
      } else if (e.type === 'retire' && e.partition === partition) {
        // A rebuild keeps a seek still queued for the next acquisition; a revoke drops it, and
        // neither keeps one being applied. Either reads the position again from the broker, after
        // the release commit (with autoCommit) wrote the progress.
        state.resolved = null
        if (state.applying || e.kind === 'revoke') state.owed = false
      } else if (e.type === 'attempt' && e.admission === state.admission) {
        state.applying = true
      } else if (e.type === 'offsetFetch') {
        // With no progress the position is read when the broker answers an offset fetch: the
        // committed offset (or the reset). A later commit does not move it.
        if (state.resolved === null && !state.owed) {
          state.resolved = this.position(state)
          state.reset = false
          state.truncation = null
        }
      } else if (e.type === 'resolve' && e.partition === partition) {
        if (this.authorized(e.delivery, e.t)) state.resolved = e.value
      } else if (e.type === 'arrival') {
        const cell = e.cells.find((c) => c.partition === partition)
        if (cell && e.outcome !== 'rejected' && e.outcome !== 'lostUnapplied') {
          state.broker = cell.offset
          state.reset = false
          if (state.truncation === cell.offset) state.truncation = null
        }
        if (cell && e.reset?.partition === partition) state.resolved = null
      } else if (
        e.type === 'outOfRangeAnswered' &&
        e.partition === partition &&
        !this.changedBetween(partition, e.fetched.built?.t ?? e.fetched.t, e.t)
      ) {
        state.resolved = null
        state.reset = true
      } else if (e.type === 'validationAnswered') {
        const asked = this.events.find(
          (a) => a.type === 'validationAsked' && a.entry === e.entry && a.partition === partition
        )
        if (
          asked &&
          !state.owed &&
          !this.changedBetween(partition, asked.t, e.t) &&
          Number(this.position(state)) > Number(e.endOffset)
        ) {
          state.resolved = this.autoCommit ? null : e.endOffset
          state.truncation = this.autoCommit ? e.endOffset : null
          state.reset = false
        }
      } else if (
        e.type === 'attemptEnd' &&
        e.attempt.partition === partition &&
        e.attempt.admission === state.admission &&
        state.owed
      ) {
        state.applying = false
        state.owed = false
        if (e.error === undefined && e.applied) {
          state.reset = false
          state.resolved = this.autoCommit ? null : String(e.attempt.admission.target)
        } else if (e.accepted) {
          // Accepted but not applied here: the replaced position goes and is read back.
          state.resolved = null
        }
      }
    }
    return state
  }

  /** @param {object} state @returns {string} the position the state stands for */
  position(state) {
    if (state.owed) return String(state.admission.target)
    return state.resolved ?? state.truncation ?? (state.reset ? '0' : asPosition(state.broker))
  }

  check() {
    const run = this.run
    const arrivals = this.events.filter((e) => e.type === 'arrival')
    this.checkFrames(arrivals)
    this.checkReplays(arrivals)
    this.checkDeliveries()
    this.checkGates()
    this.checkCallers(arrivals)
    return run
  }

  /** I1 and I3: what each OffsetCommit frame may carry. */
  checkFrames(arrivals) {
    for (const arrival of arrivals) {
      const { commit } = arrival
      if (commit?.kind === 'default') {
        for (const { partition, offset } of arrival.cells) {
          if (!commit.selection?.includes(`${partition}:${offset}`)) {
            this.run.violate('I1', 'a default commit sent a cell outside its original selection', {
              cell: `${partition}:${offset}`,
              selection: commit.selection,
            })
          }
          const admitted = this.events.find(
            (e) =>
              e.type === 'admit' &&
              e.partition === partition &&
              e.t > commit.selectedAt &&
              e.t < arrival.t
          )
          if (admitted) {
            this.run.violate('I1', 'a default commit wrote a cell sought after its selection', {
              cell: `${partition}:${offset}`,
            })
          }
          const admission = this.run.latest('admit', partition, arrival.t)
          if (
            admission &&
            !this.fencedSince(partition, admission.t, arrival.t) &&
            !this.mayCommit(partition, offset, admission, arrival.t)
          ) {
            this.run.violate('I3', 'a commit after a seek carries progress from before it', {
              cell: `${partition}:${offset}`,
              target: admission.target,
            })
          }
        }
      }
      if (commit?.kind === 'explicit' && commit.delivery) {
        if (!this.authorized(commit.delivery, arrival.t)) {
          this.run.violate('I3', 'an older batch handler wrote an explicit commit after a seek', {
            cells: arrival.cells,
          })
        }
      }
      if (arrival.reset?.fetched) {
        const { partition, fetched } = arrival.reset
        if (this.changedBetween(partition, fetched.built?.t ?? fetched.t, arrival.t)) {
          this.run.violate('I5', 'an out-of-range answer older than a seek reset the partition', {
            cells: arrival.cells,
          })
        }
      }
    }
  }

  /**
   * @returns {boolean} whether the partition's acquisition ended between two times (a revoke, a
   *   rebuild or a recreation), after which its position is read again from the broker
   */
  fencedSince(partition, from, to) {
    return this.events.some(
      (e) =>
        e.t > from &&
        e.t < to &&
        (e.type === 'recreate' || (e.type === 'retire' && e.partition === partition))
    )
  }

  /**
   * @returns {boolean} whether a default commit after `admission` may carry `offset`: the seek's
   *   own local position, progress resolved since by a batch fetched after it, or, once the seek
   *   failed without the broker accepting it, the position the partition kept
   */
  mayCommit(partition, offset, admission, at) {
    if (String(admission.target) === offset) return true
    const failed = this.events.some(
      (e) =>
        e.type === 'attemptEnd' &&
        e.attempt.admission === admission &&
        e.t < at &&
        !e.applied &&
        !e.accepted
    )
    if (failed && this.stateAt(partition, admission.t).resolved === offset) return true
    return this.events.some(
      (e) =>
        e.type === 'resolve' &&
        e.partition === partition &&
        e.t > admission.t &&
        e.t < at &&
        e.value === offset &&
        this.authorized(e.delivery, e.t)
    )
  }

  /** I2: no frame the broker accepted is sent again, and no accepted seek is replayed. */
  checkReplays(arrivals) {
    const seen = new Map()
    for (const arrival of arrivals) {
      const key = arrival.seek?.admission ?? arrival.commit
      if (!key) continue
      const earlier = seen.get(key)
      if (earlier && earlier.outcome !== 'rejected') {
        this.run.violate(
          'I2',
          'an operation the broker accepted or may have accepted was sent again',
          {
            kind: arrival.seek ? 'seek' : arrival.commit.kind,
            first: earlier.cells,
            again: arrival.cells,
          }
        )
      }
      if (!earlier || earlier.outcome === 'rejected') seen.set(key, arrival)
    }
  }

  /** I3: no delivery fetched before a seek starts a handler after it, nor while it is owed. */
  checkDeliveries() {
    for (const delivery of this.events.filter((e) => e.type === 'deliver')) {
      const state = this.stateAt(delivery.partition, delivery.t)
      if (state.owed) {
        this.run.violate('I3', 'a delivery started while a seek of its partition was owed', {
          partition: delivery.partition,
          first: delivery.first,
          target: state.admission.target,
        })
        continue
      }
      const admission = state.admission
      if (!admission) continue
      const since = this.events.filter(
        (e) => e.type === 'deliver' && e.partition === delivery.partition && e.t > admission.t
      )
      if (since[0] !== delivery) continue
      const fenced = this.fencedSince(delivery.partition, admission.t, delivery.t)
      const expected = this.position(state)
      if (!fenced && delivery.first !== expected) {
        this.run.violate('I3', 'the first delivery after a seek did not start at its position', {
          partition: delivery.partition,
          first: delivery.first,
          expected,
          target: admission.target,
        })
      }
    }
  }

  /** I6: owed seeks, reachable gates, invalid input. */
  checkGates() {
    const { group } = this.run
    for (const event of this.events.filter((e) => e.type === 'invalidSeek')) {
      if (!(event.threw instanceof KafkaJSNonRetriableError)) {
        this.run.violate('I6', 'a seek with a partition that is not a number was admitted', {
          value: event.value,
        })
      }
    }
    if (group.operations.reserved !== 0) {
      this.run.violate('I6', 'a seek reservation outlived its commit', {
        reserved: group.operations.reserved,
      })
    }
    for (const partition of PARTITIONS) {
      const owed = this.stateAt(partition).owed
      const waiting = group.isSeekGated(TOPIC, partition)
      if (owed !== waiting) {
        this.run.violate('I6', owed ? 'an owed seek was dropped' : 'a settled seek is still owed', {
          partition,
        })
      }
    }
  }

  /** I7: a broker error reaches the caller unchanged, with no request after it. */
  checkCallers(arrivals) {
    for (const op of this.run.pending) {
      if (op.kind !== 'outsideCommit' && op.kind !== 'defaultCommit') continue
      const mine = arrivals.filter((a) => a.commit && a.commit === op.commit)
      if (mine.length === 0) continue
      const reassigned = this.events.some(
        (e) => e.type === 'reassign' && e.t > op.t && e.t < op.settledAt
      )
      if (mine[0].outcome === 'rejected' && !reassigned && op.error?.type !== 'REQUEST_TIMED_OUT') {
        this.run.violate('I7', 'a rejected commit did not reach its caller as the broker error', {
          error: op.error?.message,
        })
      }
    }
  }

  /** @returns {Record<number, string>} the position each partition must report now */
  positions() {
    return Object.fromEntries(PARTITIONS.map((p) => [p, this.position(this.stateAt(p))]))
  }
}

/**
 * @param {object[]} events
 * @param {object} arrival
 * @returns {number} when the operation that sent the frame ended: until then its caller can still
 *   be refused publication
 */
const settledAt = (events, arrival) => {
  if (arrival.seek)
    return events.find((e) => e.type === 'attemptEnd' && e.attempt === arrival.seek)?.t ?? Infinity
  const op = arrival.commit && (arrival.commit.op ?? null)
  if (op) return op.settledAt ?? Infinity
  return arrival.answeredAt ?? arrival.t
}

/**
 * @param {object} arrival
 * @returns {number} when the operation that sent the frame began: its selection, its call or its
 *   seek application
 */
const beganAt = (arrival) =>
  arrival.commit?.selectedAt ?? arrival.commit?.op?.t ?? arrival.seek?.t ?? arrival.t

/**
 * Why the committed cache may differ from the broker for a partition, or null when it may not.
 * The client publishes accepted writes in the order their operations began, so the cache may
 * keep another value when the last write the broker took was never answered, began before
 * another accepted write of the partition, or was refused publication: an assignment change
 * refused its caller, or a seek of the partition was admitted, between its start and its answer.
 * A cache the client forgot is read again and is never compared.
 * @param {object[]} events
 * @param {number} partition
 * @returns {string|null}
 */
const cacheExemption = (events, partition) => {
  const accepted = events.filter(
    (e) =>
      e.type === 'arrival' &&
      e.outcome !== 'rejected' &&
      e.outcome !== 'lostUnapplied' &&
      e.cells.some((c) => c.partition === partition)
  )
  const last = accepted[accepted.length - 1]
  if (!last) return null
  if (last.outcome === 'lost') return 'the last write was never answered'
  const began = beganAt(last)
  if (accepted.some((a) => a !== last && beganAt(a) > began)) {
    return 'the last write began before another one of the partition'
  }
  const answered = settledAt(events, last)
  if (events.some((e) => e.type === 'reassign' && e.t > last.t && e.t < answered)) {
    return 'an assignment change refused the caller before the answer'
  }
  if (
    events.some(
      (e) => e.type === 'admit' && e.partition === partition && e.t > began && e.t < answered
    )
  ) {
    return 'a seek admitted before the answer took the cell, so the write was not published'
  }
  return null
}

/**
 * Starts a new consumer of the group, as a restart does, and returns the first offset it delivers
 * for each partition whose committed offset has a record to deliver.
 * @param {ScheduleRun} run
 * @returns {Promise<Record<number, string>>}
 */
const restart = async (run) => {
  const first = {}
  const awaited = PARTITIONS.filter((p) => Number(asPosition(run.broker[p])) <= LAST)
  await drive(
    run.rig.started({
      consumer: { maxWaitTimeInMs: 5 },
      run: {
        autoCommit: false,
        eachMessage: undefined,
        eachBatch: async ({ batch }) => {
          if (batch.messages.length > 0) first[batch.partition] ??= batch.messages[0].offset
        },
      },
      delivered: 0,
    })
  )
  for (let spent = 0; awaited.some((p) => first[p] === undefined); spent += TICK_MS) {
    if (spent > OPEN_MS) break
    await advance(TICK_MS)
  }
  return first
}

/**
 * Runs a schedule on fake time, with `Math.random` drawn from the seed, and returns what broke:
 * the invariants over the events, the positions and the committed cache against the model, what
 * the final commit wrote, and what a restarted consumer reads back.
 * @param {'classic'|'consumer'} protocol
 * @param {object} schedule
 * @returns {Promise<{check: string, message: string, detail: object}[]>}
 */
const runSchedule = async (protocol, schedule) => {
  jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask'] })
  const random = lcg(schedule.seed ?? 1)
  const seeded = jest.spyOn(Math, 'random').mockImplementation(() => random.int(2 ** 32) / 2 ** 32)
  const run = new ScheduleRun(protocol, schedule)
  try {
    await run.execute()
    const model = new Model(run)
    const expected = model.positions()
    for (const p of PARTITIONS) {
      let actual
      try {
        actual = await drive(run.consumer.position({ topic: TOPIC, partition: p }))
      } catch (error) {
        actual = `error: ${error.message}`
      }
      if (actual !== expected[p]) {
        run.violate('I5', 'the position differs from the model', {
          partition: p,
          expected: expected[p],
          actual,
        })
      }
      const cache = run.group.offsetManager.committedOffsets()[TOPIC]?.[p]
      if (cache != null && String(cache) !== run.broker[p] && !cacheExemption(run.events, p)) {
        run.violate('I7', 'the committed cache holds a value the broker does not', {
          partition: p,
          cache: String(cache),
          broker: run.broker[p],
        })
      }
    }
    if (run.violations.length === 0) {
      const before = run.events.length
      await drive(run.consumer.disconnect())
      const after = run.events.slice(before).filter((e) => e.type === 'arrival')
      new Model(run).checkFrames(after)
      for (const p of PARTITIONS) {
        if (
          schedule.autoCommit &&
          !cacheExemption(run.events, p) &&
          asPosition(run.broker[p]) !== expected[p]
        ) {
          run.violate('I4', 'the final commit did not leave the position at the broker', {
            partition: p,
            expected: expected[p],
            broker: run.broker[p],
          })
        }
      }
      const first = await restart(run)
      for (const p of PARTITIONS) {
        const durable = asPosition(run.broker[p])
        if (Number(durable) <= LAST && first[p] !== durable) {
          run.violate('I4', 'a restarted consumer did not resume at the committed offset', {
            partition: p,
            committed: run.broker[p],
            first: first[p],
          })
        }
      }
    }
  } catch (error) {
    run.violate('run', error.message)
  } finally {
    await run.dispose().catch((error) => run.violate('dispose', error.message))
    seeded.mockRestore()
    jest.useRealTimers()
  }
  Object.defineProperty(run.violations, 'events', { value: run.events })
  return run.violations
}

/**
 * Removes steps one at a time while the schedule still fails with the same check, within a budget.
 * @returns {Promise<object>} the smallest failing schedule found
 */
const minimize = async (protocol, schedule, check) => {
  let best = schedule
  let budget = 25
  for (let i = best.steps.length - 1; i >= 0 && budget > 0; i--) {
    const candidate = { ...best, steps: best.steps.filter((_, j) => j !== i) }
    budget--
    const violations = await runSchedule(protocol, candidate)
    if (violations.some((v) => v.check === check)) best = candidate
  }
  return best
}

const SEEDS = Array.from({ length: 200 }, (_, i) => 1000 + i * 7919)
const CHUNK = 25

describe.each(['classic', 'consumer'])('seeded seek schedules, %s protocol', (protocol) => {
  for (let start = 0; start < SEEDS.length; start += CHUNK) {
    const seeds = SEEDS.slice(start, start + CHUNK)
    test(`keep every seek invariant for seeds ${seeds[0]}..${seeds[seeds.length - 1]}`, async () => {
      const failures = []
      for (const seed of seeds) {
        const schedule = generate(seed)
        const violations = await runSchedule(protocol, schedule)
        if (violations.length === 0) continue
        const minimal = await minimize(protocol, schedule, violations[0].check)
        failures.push({
          seed,
          autoCommit: schedule.autoCommit,
          violations,
          steps: minimal.steps.map((step) => step.join(' ')),
        })
      }
      if (failures.length > 0) throw new Error(JSON.stringify(failures, null, 1))
    }, 120000)
  }
})

describe.each(['classic', 'consumer'])('a seeded seek schedule, %s protocol', (protocol) => {
  test('produces the same events and outcome every time it runs', async () => {
    const trace = (violations) =>
      JSON.stringify([
        violations.events.map(({ t, type, partition, first, outcome, cells, value, target }) => [
          t,
          type,
          partition,
          first,
          outcome,
          cells,
          value,
          target,
        ]),
        violations.map(({ check, message }) => [check, message]),
      ])
    for (const seed of SEEDS.slice(0, 10)) {
      const traces = new Set()
      for (let run = 0; run < 5; run++)
        traces.add(trace(await runSchedule(protocol, generate(seed))))
      expect({ seed, distinct: traces.size }).toEqual({ seed, distinct: 1 })
    }
  }, 120000)
})

module.exports = { generate, runSchedule }
