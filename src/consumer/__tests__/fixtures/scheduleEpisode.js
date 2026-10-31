const { Kafka } = require('../../../../index')
const Runner = require('../../runner')
const IdleWait = require('../../idleWait')
const {
  createScriptedCluster,
  groupScript,
  consumerProtocolScript,
} = require('../../../../testHelpers/scriptedCluster')
const { cooperativeSticky } = require('../../assigners')
const { MemberAssignment } = require('../../assignerProtocol')
const { TOPIC, prepareRecordBatches } = require('./fetchHarness')
const { ScheduleBroker } = require('./scheduleBroker')
const { pump } = require('./lifecycleCase')

const GROUP_ID = 'schedule-group'
const NODE = { id: 0, host: 'node', port: 9092 }
const FENCE = { classic: 22, consumer: 110 }
const UNKNOWN_MEMBER = 25

/** One call or callback held until the episode lets it go; shaped like a scripted-cluster gate. */
class LocalGate {
  constructor() {
    this.call = null
    this.settle = null
  }

  /** @returns {Promise<void>} resolves when `release` runs; records that the boundary was reached */
  enter() {
    this.call = { entered: true }
    return new Promise((resolve) => {
      this.settle = { resolve }
    })
  }

  /** @returns {void} lets the held callback return */
  release() {
    const { settle } = this
    this.settle = null
    settle?.resolve()
  }
}

/**
 * Runs one declarative schedule row, or one generated interleaving, on the real wire stack: the
 * public `Kafka` and its real `Runner` against a `ScriptedCluster` whose broker state is a
 * `ScheduleBroker`. The episode owns every gate it holds, journals every handler delivery by
 * `(incarnation, partition, offset)`, and ends with strict checks: the hold was reached, no commit
 * was written between a learned loss and the next join, no partition had two handlers at once,
 * duplicates stayed inside the declared replay interval, every committed offset belongs to the
 * current incarnation, the sockets are closed and the timers are back at their baseline.
 *
 * Run it under `jest.useFakeTimers({ doNotFake: ['setImmediate'] })`.
 */
class ConsumerScheduleEpisode {
  /**
   * @param {object} row a schedule row: `protocol`, `faultPoint`, `feature`, `config`, `steps`, `replay`
   * @param {object} [options]
   * @param {number} [options.partitions] how many partitions the topic has; the row's own by default
   * @param {boolean} [options.sessionMode] whether Fetch sessions are on; the row's own by default
   * @param {boolean} [options.groupedManual=true] whether a manual consumer commits to a group
   * @param {object} [options.listener] extra rebalance hooks, called after the episode's own record
   */
  constructor(
    row,
    {
      partitions = row.config.partitions.length,
      sessionMode = row.config.fetchSessions,
      groupedManual = true,
    } = {}
  ) {
    this.row = row
    this.partitions = partitions
    this.sessionMode = sessionMode
    this.manual = row.protocol === 'manual'
    this.grouped = !this.manual || groupedManual
    this.kind = row.protocol === 'consumer' ? 'consumer' : 'classic'
    this.trace = []
    this.handled = []
    this.inFlight = new Map()
    this.overlaps = 0
    this.hooks = []
    this.gates = []
    this.pending = []
    this.runners = []
    this.spies = []
    this.errors = []
    this.marks = {}
    this.joins = 0
    this.lost = 0
    this.opened = false
    this.closed = false
    this.heldBody = null
    this.handlerGate = null
    this.hookGate = null
    this.answerGate = null
    this.answerAllowed = false
    this.hookDepth = 0
    this.hookOverlaps = 0
    this.late = []
  }

  /**
   * Executes the row's steps in order.
   * @returns {Promise<void>}
   * @throws {Error} on an unknown step, a hold that was never reached or a failed check
   */
  async execute() {
    let failure
    try {
      for (const step of this.row.steps) await this.run(step)
    } catch (error) {
      failure = error
    }
    try {
      await this.close({ verify: failure === undefined })
    } catch (error) {
      failure ??= error
    }
    if (failure) throw failure
  }

  /**
   * Performs one named step. Unknown steps fail, so a row cannot silently skip work.
   * @param {string} action
   * @returns {Promise<void>}
   * @throws {Error} when the action is unknown
   */
  async run(action) {
    const step = this.#actions()[action]
    if (!step) throw new Error(`Unknown schedule action: ${action}`)
    await this.#open()
    this.trace.push(action)
    await step()
  }

  /**
   * Ends the episode: lets every gate go, disconnects, restores what was spied on and, unless an
   * earlier step already failed, verifies the end state. Safe to call twice.
   * @param {{ verify?: boolean }} [options] `verify: false` skips the end checks after a failed step
   * @returns {Promise<void>}
   * @throws {Error} the first failed check, after every cleanup step ran
   */
  async close({ verify = true } = {}) {
    if (this.closed || !this.opened) return
    this.closed = true
    const failures = []
    const attempt = async (work) => {
      try {
        await work()
      } catch (error) {
        failures.push(error)
      }
    }
    await attempt(() => this.#releaseAll())
    await attempt(async () => {
      const closing = this.#track(this.consumer.disconnect())
      await pump(() => closing.done, 'disconnect')
      if (closing.error) throw closing.error
    })
    await attempt(() => pump(() => jest.getTimerCount() === this.baseline.timers, 'timers drained'))
    for (const remove of this.removers) await attempt(remove)
    for (const spy of this.spies.reverse()) await attempt(() => spy.mockRestore())
    process.removeListener('unhandledRejection', this.onUnhandled)
    if (failures.length > 0) throw failures[0]
    if (verify) this.#verify()
  }

  #actions() {
    const manual = this.manual
    return {
      acquire: () => this.#acquire(),
      assign: () => this.#assign(),
      hold: () => this.#hold(),
      resolve: () => this.#resolve(),
      seek: () => this.#seek(),
      loss: () => this.#fault('loss'),
      epoch: () => this.#fault('epoch'),
      reconnect: () => this.#reconnect(),
      recreate: () => this.#recreate(),
      rebalance: () => this.#rebalance(),
      'hook-release': () => {
        this.marks.hookReleased ??= this.#lastOrder()
        return this.#release(this.hookGate)
      },
      quiesce: () => this.#quiesce(),
      answer: () => this.#answer(),
      wake: () => this.#wake(),
      stop: () => this.#stop(),
      'wake-stop': () => this.#wakeStop(),
      release: () => this.#releaseAll(),
      'observe-broker': async () => {
        this.observed = {
          commits: [...this.broker.commits],
          committed: new Map(this.broker.committed),
        }
      },
      drain: () => this.#drain(),
      close: async () => {
        if (manual && !this.grouped) expect(this.broker.commits).toEqual([])
      },
    }
  }

  async #open() {
    if (this.opened) return
    this.opened = true
    await prepareRecordBatches()
    this.broker = new ScheduleBroker({ partitions: this.partitions, sessions: this.sessionMode })
    const script = this.kind === 'consumer' ? consumerProtocolScript : groupScript
    this.scripted = this.broker.install(
      script(
        createScriptedCluster({
          nodes: [NODE],
          profile: this.kind === 'consumer' ? 'consumer' : 'default',
        }),
        {
          partitions: this.partitions,
          batches: 0,
        }
      ),
      this.kind
    )
    this.baseline = { timers: jest.getTimerCount() }
    this.onUnhandled = (error) => this.errors.push(error)
    process.on('unhandledRejection', this.onUnhandled)
    const start = Runner.prototype.start
    const episode = this
    this.spies.push(
      jest.spyOn(Runner.prototype, 'start').mockImplementation(function (...args) {
        episode.runners.push(this)
        return start.apply(this, args)
      })
    )
    const options = this.manual
      ? {}
      : this.kind === 'consumer'
        ? { groupProtocol: 'consumer' }
        : {
            heartbeatInterval: 100,
            partitionAssigners:
              this.row.protocol === 'classic-cooperative' ? [cooperativeSticky] : undefined,
          }
    this.consumer = new Kafka(
      this.scripted.kafkaOptions({
        maxInFlightRequests: 1,
        requestTimeout: 20000,
        retry: { retries: 1, initialRetryTime: 10, maxRetryTime: 10, factor: 0 },
      })
    ).consumer({
      ...(this.grouped ? { groupId: GROUP_ID } : {}),
      maxInFlightRequests: 1,
      maxWaitTimeInMs: 20,
      fetchSessions: this.sessionMode,
      ...options,
    })
    const { events } = this.consumer
    this.removers = [
      this.consumer.on(events.GROUP_JOIN, () => {
        this.joins += 1
      }),
      this.consumer.on(events.PARTITIONS_LOST, () => {
        this.lost += 1
        this.marks.loss ??= this.scripted.requests().length
      }),
    ]
    await this.consumer.connect()
  }

  /** @returns {object} the runner of the live run */
  get runner() {
    return this.runners[this.runners.length - 1]
  }

  #track(promise) {
    const state = { done: false, error: null }
    state.promise = promise.then(
      () => {
        state.done = true
      },
      (error) => {
        state.done = true
        state.error = error
      }
    )
    this.pending.push(state)
    return state
  }

  /** The partitions of this episode's topic. */
  #ids() {
    return Array.from({ length: this.partitions }, (_, partition) => partition)
  }

  async #handle({ partition, message }) {
    const key = `${partition}`
    if (this.inFlight.get(key)) this.overlaps += 1
    this.inFlight.set(key, true)
    const entry = {
      incarnation: this.broker.incarnation(),
      partition,
      offset: message.offset,
      joins: this.joins,
      order: this.#lastOrder(),
    }
    this.handled.push(entry)
    try {
      if (this.handlerGate && !this.handlerGate.call) await this.handlerGate.enter()
      await this.consumer
        .commitOffsets([{ topic: TOPIC, partition, offset: String(Number(message.offset) + 1) }])
        .catch((error) => this.refused.push(error))
    } finally {
      this.inFlight.set(key, false)
    }
  }

  async #startRun(extra = {}) {
    this.refused = []
    const listener = this.row.faultPoint.startsWith('hook:')
      ? {
          onPartitionsAssigned: async () => {
            this.hooks.push('assigned')
            if (this.hookDepth > 0) this.hookOverlaps += 1
            this.hookDepth += 1
            try {
              if (this.hookGate && !this.hookGate.call) await this.hookGate.enter()
            } finally {
              this.hookDepth -= 1
            }
          },
        }
      : undefined
    this.running = this.#track(
      this.consumer.run({
        autoCommit: false,
        eachMessage: (payload) => this.#handle(payload),
        ...(listener ? { rebalanceListener: listener } : {}),
        ...extra,
      })
    )
    await pump(() => this.runners.length > 0, 'runner capture')
  }

  /** Starts a group run (or a manual one with its partitions assigned) and waits for its first acquisition. */
  async #acquire() {
    if (this.row.faultPoint === 'handler:entered') this.handlerGate = new LocalGate()
    if (this.row.faultPoint === 'hook:assigned') this.hookGate = new LocalGate()
    if (this.row.feature === 'interleaving') this.#holdAnswer()
    await this.consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await this.#startRun()
    if (this.handlerGate) await pump(() => this.handlerGate.call, 'handler entered')
    else if (this.hookGate) await pump(() => this.hookGate.call, 'assigned hook entered')
    else await this.#settled()
    if (this.handlerGate) this.gates.push(this.handlerGate)
    if (this.hookGate) this.gates.push(this.hookGate)
  }

  /** The first acquisition is complete when every partition was handled and committed once. */
  #settled() {
    return pump(
      () =>
        this.#ids().every(
          (partition) =>
            this.handled.some(
              (entry) =>
                entry.partition === partition && entry.incarnation === this.broker.incarnation()
            ) &&
            (!this.grouped || this.broker.committedOf(this.broker.topicId, partition) === '1')
        ),
      'first acquisition handled and committed'
    )
  }

  async #assign() {
    await this.consumer.assign([{ topic: TOPIC, partitions: this.#ids() }], {
      offsetReset: 'earliest',
    })
    if (this.row.faultPoint === 'OffsetFetch:answer') return
    await this.#startRun()
    await this.#settled()
  }

  async #hold() {
    const hold = (selector) => {
      const gate = this.scripted.schedule.hold(selector)
      this.gates.push(gate)
      return gate
    }
    switch (this.row.faultPoint) {
      case 'OffsetFetch:answer':
        this.gate = hold({ api: this.grouped ? 'OffsetFetch' : 'ListOffsets' })
        if (this.manual) await this.#startRun()
        break
      case 'Fetch:answer': {
        this.gate = hold({ api: 'Fetch' })
        const call = await this.#arrival(this.gate)
        this.heldBody = this.broker.fetchBody(call.fields, call).body
        break
      }
      case 'Metadata:answer':
        this.gate = hold({ api: 'Metadata' })
        this.metadataRefresh = this.#track(this.runner.consumerGroup.cluster.refreshMetadata())
        this.heldMetadata = this.broker.metadata(this.scripted.nodes)
        await this.#arrival(this.gate)
        break
      case 'SyncGroup:answer':
      case 'ConsumerGroupHeartbeat:answer':
        this.gate = hold({ api: this.row.faultPoint.split(':')[0] })
        if (this.kind === 'consumer') await this.#arrival(this.gate)
        break
      case 'idle:registered': {
        this.gate = new LocalGate()
        this.gates.push(this.gate)
        const wait = IdleWait.prototype.wait
        const { gate } = this
        this.spies.push(
          jest.spyOn(IdleWait.prototype, 'wait').mockImplementation(async function (...args) {
            if (!gate.call) await gate.enter()
            return wait.apply(this, args)
          })
        )
        this.consumer.pause([{ topic: TOPIC }])
        await this.#arrival(this.gate)
        break
      }
      default:
        await pump(() => this.gates.every((gate) => gate.call), `${this.row.faultPoint} reached`)
    }
    expect(this.gates.length).toBeGreaterThan(0)
  }

  #arrival(gate) {
    return pump(() => gate.call, `${this.row.faultPoint} reached`).then(() => gate.call)
  }

  /** A rebalance round driven the way a revoke verdict drives it. */
  #rebalance() {
    this.refused = this.refused ?? []
    this.round = this.#track(
      this.runner.handleRebalanceError({
        kind: 'revoked',
        reason: 'assignment',
        error: new Error('assignment'),
      })
    )
    return Promise.resolve()
  }

  /** Makes the next round take every partition away first, so each one is acquired and resolved anew. */
  #revokeAll() {
    const { schedule } = this.scripted
    if (this.row.protocol === 'classic-cooperative')
      schedule.once(
        { api: 'SyncGroup' },
        {
          memberAssignment: MemberAssignment.encode({ version: 1, assignment: {} }).toString('hex'),
        }
      )
    if (this.kind === 'consumer') {
      const answer =
        (topicPartitions) =>
        ({ fields }) => ({
          memberId: fields.memberId,
          memberEpoch: fields.memberEpoch + 1,
          heartbeatIntervalMs: 100,
          assignment: { topicPartitions },
        })
      schedule.once({ api: 'ConsumerGroupHeartbeat' }, answer([]))
      schedule.once(
        { api: 'ConsumerGroupHeartbeat' },
        answer([{ topicId: this.broker.topicId, partitions: this.#ids() }])
      )
    }
  }

  async #resolve() {
    this.#revokeAll()
    await this.#rebalance()
    await this.#arrival(this.gate)
    this.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })
    this.moved = true
  }

  async #seek() {
    await this.#arrival(this.gate)
    this.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })
    this.moved = true
  }

  /** Serves the next heartbeat a fenced answer and waits until the client has read it. */
  async #fault(kind) {
    this.broker.failNextHeartbeat(kind === 'loss' ? UNKNOWN_MEMBER : FENCE[this.kind])
    const served = this.broker.faultOrders.length + 1
    await pump(() => this.broker.faultOrders.length >= served, `${kind} served`)
    const [learnedAt] = this.broker.faultOrders.slice(-1)
    await pump(
      () =>
        this.scripted.requests().find((record) => record.order === learnedAt)?.answeredOrder !==
        undefined,
      `${kind} answered`
    )
    for (let turn = 0; turn < 5; turn++) await jest.advanceTimersByTimeAsync(1)
    this.faulted = true
  }

  async #reconnect() {
    for (const socket of this.scripted.sockets()) socket.destroy(new Error('connection reset'))
    this.reconnected = true
    await pump(() => this.scripted.sockets().some((socket) => !socket.destroyed), 'new connection')
  }

  async #recreate() {
    this.broker.recreate()
    this.recreated = true
    if (this.row.faultPoint === 'Metadata:answer') return
    const refresh = this.#track(this.runner.consumerGroup.cluster.refreshMetadata())
    await pump(() => refresh.done, 'metadata refresh')
  }

  #lastOrder() {
    return this.scripted.requests().at(-1)?.order ?? 0
  }

  async #stop() {
    if (this.kind === 'classic' && this.row.faultPoint === 'SyncGroup:answer') {
      await this.#rebalance()
      await this.#arrival(this.gate)
    }
    this.marks.stop = this.#lastOrder()
    this.stopping = this.#track(this.consumer.stop())
  }

  async #wakeStop() {
    this.marks.stop = this.#lastOrder()
    this.stopping = this.#track(this.consumer.stop())
    this.consumer.resume([{ topic: TOPIC }])
  }

  /** Lets everything go and waits for the run to settle or, once stopped, to end. */
  async #quiesce() {
    await this.#releaseAll()
    if (this.stopping) await pump(() => this.stopping.done, 'stop completed')
    await this.#tick()
    await jest.advanceTimersByTimeAsync(300)
  }

  /** Holds the first Fetch answer; it is released once the episode has allowed it and it has arrived. */
  #holdAnswer() {
    this.answerGate = this.scripted.schedule.hold({ api: 'Fetch' })
    this.answerGate.arrived().then(() => {
      if (this.answerAllowed && this.answerGate.settle) this.answerGate.release()
    })
  }

  /** Lets the held answer go now, or as soon as it arrives. @returns {Promise<void>} */
  async #answer() {
    this.answerAllowed = true
    if (this.answerGate?.settle) this.answerGate.release()
    await this.#tick()
  }

  /** Lets a held callback return; a gate that is not held, or already let go, is left alone. */
  async #release(gate) {
    if (gate?.settle) gate.release()
    await this.#tick()
  }

  /** The wake a resume sends to idle fetchers; after a stop the public call may refuse, and that is recorded. */
  async #wake() {
    try {
      this.consumer.resume([{ topic: TOPIC }])
    } catch (error) {
      if (!this.stopping || !error.message.includes('was not initialized')) throw error
      this.late.push(error.message)
    }
    await this.#tick()
  }

  /** Lets the effect of the last action reach the run before the next one starts. */
  async #tick() {
    for (let turn = 0; turn < 3; turn++) {
      await new Promise(setImmediate)
      await jest.advanceTimersByTimeAsync(1)
    }
  }

  async #releaseAll() {
    this.marks.released ??= this.#lastOrder()
    for (const gate of [...this.gates, ...(this.answerGate ? [this.answerGate] : [])]) {
      if (!gate.settle) continue
      if (gate === this.gate && this.heldBody) gate.release(this.heldBody)
      else if (gate === this.gate && this.heldMetadata) gate.release(this.heldMetadata)
      else gate.release()
    }
    await new Promise(setImmediate)
  }

  /** Lets the run converge after the release: every acquired partition handled, nothing pending. */
  async #drain() {
    if (this.stopping) {
      await pump(() => this.stopping.done, 'stop completed')
      return
    }
    await this.#settled()
    for (const state of this.pending)
      await pump(() => state.done || state === this.running, 'pending work')
    await pump(() => jest.getTimerCount() > 0, 'timers armed')
    await jest.advanceTimersByTimeAsync(500)
  }

  /** @returns {Array<{ partition: number, offset: string, incarnation: string }>} the commits of the current incarnation */
  currentCommits() {
    const current = this.broker.incarnation()
    return this.broker.commits.filter((commit) => commit.incarnation === current)
  }

  /**
   * The end checks of the episode.
   * @throws {Error} when any expectation about the wire, the sets or the resources fails
   */
  #verify() {
    const { row, broker, scripted } = this
    const holds = this.gates.filter((gate) => gate.call === null)
    expect(holds).toEqual([])
    expect(this.errors).toEqual([])
    const frames = scripted.requests()
    const commitFramesAfterLoss =
      this.marks.loss === undefined
        ? 0
        : frames
            .slice(this.marks.loss, this.#firstJoinAfterLoss(frames))
            .filter((record) => record.api === 'OffsetCommit').length
    expect(commitFramesAfterLoss).toBe(row.expectedWire.afterLearnedLossOffsetCommit)
    expect(this.#postLossCommitsOfLostWork(frames)).toEqual([])
    if (!this.grouped) expect(frames.some((record) => record.api === 'OffsetCommit')).toBe(false)
    const { convergenceRounds, overlappingOwners = 0, ...counted } = row.expectedWire
    expect(this.overlaps).toBe(overlappingOwners)
    for (const [api, count] of Object.entries(counted)) {
      if (!['afterLearnedLossOffsetCommit', 'unconsumedBytes'].includes(api))
        expect(frames.filter((record) => record.api === api)).toHaveLength(count)
    }
    if (convergenceRounds !== undefined) expect(this.joins).toBeLessThanOrEqual(convergenceRounds)
    for (const commit of broker.commits) expect(Number(commit.offset)).toBeLessThanOrEqual(1)
    this.#verifySets()
    this.#verifyOutcome()
    expect(scripted.sockets().filter((socket) => !socket.destroyed)).toEqual([])
    scripted.assertClean()
    expect(jest.getTimerCount()).toBe(this.baseline.timers)
  }

  /**
   * Offset commits written after a fenced answer was served, for work delivered before it and not
   * delivered again afterwards: only a writer that ignored its refused ownership sends them.
   * @param {object[]} frames the requests the client wrote
   * @returns {Array<{ partition: number, offset: string }>} such commits; empty when none
   */
  #postLossCommitsOfLostWork(frames) {
    const [learnedAt] = this.broker.faultOrders
    if (learnedAt === undefined) return []
    const late = []
    for (const record of frames) {
      if (record.api !== 'OffsetCommit' || record.order < learnedAt) continue
      for (const { partitions } of record.fields.topics)
        for (const { partition, offset } of partitions) {
          const before = this.handled.some((e) => e.partition === partition && e.order < learnedAt)
          const after = this.handled.some((e) => e.partition === partition && e.order >= learnedAt)
          if (before && !after) late.push({ partition, offset })
        }
    }
    return late
  }

  #firstJoinAfterLoss(frames) {
    const rejoin = frames.findIndex(
      (record, index) =>
        index >= this.marks.loss && ['JoinGroup', 'ConsumerGroupHeartbeat'].includes(record.api)
    )
    return rejoin === -1 ? frames.length : rejoin
  }

  /** What the row's own action must have changed, so a step that did nothing cannot pass. */
  #verifyOutcome() {
    const { feature } = this.row
    const count = (partition) => this.handled.filter((e) => e.partition === partition).length
    const sessionEpochs = this.broker.fetches.map((fetch) => fetch.sessionEpoch)
    if (feature === 'offset-overlap') {
      expect(count(0)).toBe(this.manual ? 1 : 2)
      expect(count(1)).toBe(1)
    }
    if (this.row.steps.some((step) => ['loss', 'epoch'].includes(step))) {
      expect(this.lost).toBeGreaterThan(0)
      expect(this.joins).toBeGreaterThan(1)
    }
    if (this.reconnected && this.sessionMode)
      expect(sessionEpochs.filter((epoch) => epoch === 0).length).toBeGreaterThan(1)
    if (!this.sessionMode) expect(sessionEpochs.every((epoch) => epoch === -1)).toBe(true)
    if (this.recreated) {
      for (const incarnation of ['incarnation-a', 'incarnation-b'])
        for (const partition of this.#ids())
          expect(
            this.handled.filter((e) => e.incarnation === incarnation && e.partition === partition)
          ).toHaveLength(1)
    }
    if (this.row.feature === 'interleaving') {
      expect(this.hookOverlaps).toBe(0)
      expect(this.handled.every((e) => e.order >= (this.marks.hookReleased ?? Infinity))).toBe(true)
    } else if (this.row.faultPoint === 'hook:assigned') {
      expect(this.hooks.length).toBeGreaterThan(1)
      expect(this.handled.every((e) => e.order >= this.marks.released)).toBe(true)
    }
    if (this.stopping) {
      const after = this.scripted.requests().filter((record) => record.order > this.marks.stop)
      expect(
        after.filter((record) => ['JoinGroup', 'SyncGroup', 'OffsetCommit'].includes(record.api))
      ).toEqual([])
      expect(this.handled.filter((e) => e.order > this.marks.stop)).toEqual([])
      expect(
        this.broker.fetches.filter((fetch) => fetch.order > this.marks.stop).length
      ).toBeLessThan(3)
    }
  }

  /**
   * The replay this row may produce, fixed from the row before it runs. A row that serves a fenced
   * answer while a handler is held loses that handler's uncommitted record, so it declares the
   * crash interval from the last broker-confirmed position, `[0, 1)` of the first incarnation.
   * @returns {{ from: string, to: string, incarnation: string }|null} the interval, or none
   */
  declaredReplay() {
    if (this.row.replay.interval) return this.row.replay.interval
    return this.row.steps.includes('epoch')
      ? { from: '0', to: '1', incarnation: 'incarnation-a' }
      : null
  }

  /** Handler deliveries are stable-once outside the declared replay interval of their incarnation. */
  #verifySets() {
    const interval = this.declaredReplay()
    const declared = interval
      ? { incarnation: interval.incarnation, from: BigInt(interval.from), to: BigInt(interval.to) }
      : null
    const seen = new Map()
    for (const entry of this.handled) {
      const key = `${entry.incarnation}|${entry.partition}|${entry.offset}`
      const count = (seen.get(key) ?? 0) + 1
      seen.set(key, count)
      if (count > 1) {
        const offset = BigInt(entry.offset)
        const inside =
          declared !== null &&
          offset >= declared.from &&
          offset < declared.to &&
          (entry.incarnation === declared.incarnation || this.recreated)
        if (!inside) throw new Error(`Undeclared replay of ${key}`)
      }
    }
    if (this.row.feature === 'interleaving') return
    for (const partition of this.#ids()) {
      const current = this.handled.filter(
        (entry) => entry.partition === partition && entry.incarnation === this.broker.incarnation()
      )
      expect(current.length).toBeGreaterThan(0)
    }
  }
}

module.exports = { ConsumerScheduleEpisode, GROUP_ID }
