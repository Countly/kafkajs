'use strict'

/**
 * The consumer-churn run and its public workloads. Every client here is built and driven through
 * the package's public API; the only window into the process is the probe's socket factory and the
 * journal the workloads append to. Run it through `scripts/soak/soak.js --profile consumer-churn`.
 */
const { KafkaJSCommitFailedError } = require('../../index')
const {
  CONSUMER_CHURN,
  ChurnDeadline,
  ChurnGroups,
  ConsumerChurnOracle,
  OfferScheduler,
  acknowledgedViolations,
  interningControl,
  judgeQuietHeap,
  quietViolations,
  runControls,
  shapeHeapGrowth,
} = require('./consumerChurn')

const { subscribeNamedTopic } = require('./subscribeNamedTopic')

const COMMIT_EVERY = 25
const DRAIN_POLL_MS = 100
const DRAIN_MS = 45000
const ACK_WAIT_MS = 10000

/** @param {string} key `topic|partition` @returns {[string, number]} its parts; a topic name may hold `|` */
const splitKey = (key) => {
  const split = key.lastIndexOf('|')
  return [key.slice(0, split), Number(key.slice(split + 1))]
}

/**
 * One group member: a public consumer that commits only the contiguous position it has processed,
 * reads the broker back after every commit, and journals its ownership, deliveries and commits in
 * the order they happen. The member never commits for a partition it does not own. Each ownership
 * is journaled under the topic's incarnation at that moment, and when the topic is recreated the
 * member re-acquires its partitions under the new incarnation before it journals anything else.
 */
class ChurnMember {
  /**
   * @param {object} options
   * @param {object} options.consumer a public consumer built for `group`
   * @param {object} options.admin a connected public admin, used to read the broker's offsets
   * @param {string} options.group the group id, already scoped to the phase
   * @param {string} options.label a name unique to this member instance
   * @param {{ append: (observation: object) => void }} options.journal
   * @param {import('./consumerChurn').GapSampler|null} options.gaps null for an unmeasured phase
   * @param {string} options.phase the measured phase, for the gap series
   * @param {{ now: () => number }} options.clock
   * @param {Map<string, number>} options.handled deliveries seen, by `group|incarnation`
   * @param {(loss: { group: string, member: string }) => void} [options.onLoss] told when this member
   *   journals a `loss` round
   * @param {(topic: string) => string|undefined} options.incarnationOf the current incarnation of a
   *   topic; a topic with none is not the profile's and is ignored
   * @param {(topic: string, partition: number, offset: string, incarnation: string) => boolean|Promise<boolean>} options.acknowledged
   *   resolves true once the producer's acknowledgement of that record is journaled, so a delivery is
   *   never journaled before the record it delivers; false for a record that is never acknowledged
   */
  constructor({
    consumer,
    admin,
    group,
    label,
    journal,
    gaps,
    phase,
    clock,
    handled,
    onLoss,
    incarnationOf,
    acknowledged,
  }) {
    Object.assign(this, {
      consumer,
      admin,
      group,
      label,
      journal,
      gaps,
      phase,
      clock,
      handled,
      onLoss,
      incarnationOf,
      acknowledged,
    })
    this.stopping = false
    this.owners = new Map()
    this.processed = new Map()
    this.committed = new Map()
    this.sinceCommit = 0
    this.acquisitions = 0
    this.committing = Promise.resolve()
  }

  /**
   * Subscribes and runs. Named topics retry metadata propagation for up to five seconds.
   * Resolves once `run` returned, not once a partition is assigned, or when stopped.
   * @param {Array<string|RegExp>} topics
   * @param {{ fromBeginning?: boolean }} [options]
   * @returns {Promise<void>}
   * @throws {Error} for connect, subscribe or run failure after the bounded topic retry
   */
  async start(topics, { fromBeginning = true } = {}) {
    await this.consumer.connect()
    for (const topic of topics) {
      if (this.stopping) return
      const options = { topic, fromBeginning }
      if (typeof topic === 'string') {
        if (!(await subscribeNamedTopic(this.consumer, options, () => this.stopping))) return
      } else {
        await this.consumer.subscribe(options)
      }
    }
    if (this.stopping) return
    await this.consumer.run({
      autoCommit: false,
      rebalanceListener: {
        onPartitionsAssigned: ({ partitions }) => this.#assigned(partitions),
        onPartitionsRevoked: ({ partitions }) => this.#left('revoke', partitions),
        onPartitionsLost: ({ partitions }) => this.#left('loss', partitions),
      },
      eachMessage: (payload) => this.#handle(payload),
    })
  }

  /**
   * Commits what is processed, journals the end of every ownership still held, then disconnects:
   * once stop begins the member's deliveries and commits are no longer journaled.
   * @returns {Promise<void>}
   */
  async stop() {
    this.stopping = true
    await this.commitAll().catch(() => {})
    this.#end(
      'revoke',
      [...this.owners.keys()].map((key) => {
        const [topic, partition] = splitKey(key)
        return { topic, partitions: [partition] }
      })
    )
    await this.consumer.disconnect()
  }

  async #assigned(partitions) {
    for (const { topic, partitions: ids } of partitions) {
      const positions = await this.#positions(topic)
      for (const partition of ids) this.#acquire(topic, partition, positions.get(partition) ?? '0')
    }
  }

  /** Journals a new ownership under the topic's current incarnation, with the position it starts at. */
  #acquire(topic, partition, next) {
    const incarnation = this.incarnationOf(topic)
    if (incarnation === undefined) return undefined
    const key = `${topic}|${partition}`
    const held = { owner: `${this.label}-${++this.acquisitions}`, incarnation }
    this.owners.set(key, held)
    this.processed.delete(key)
    this.committed.delete(key)
    const scope = { group: this.group, owner: held.owner, incarnation, topic, partition }
    this.journal.append({ kind: 'acquire', ...scope })
    this.journal.append({ kind: 'position', ...scope, next })
    return held
  }

  /**
   * The ownership of a partition this member holds, renewed first when the topic was recreated: a
   * recreated log starts again at offset 0 under a new incarnation.
   */
  #held(topic, partition) {
    const held = this.owners.get(`${topic}|${partition}`)
    if (held === undefined) return undefined
    const current = this.incarnationOf(topic)
    return current === undefined || current === held.incarnation
      ? held
      : this.#acquire(topic, partition, '0')
  }

  async #left(kind, partitions) {
    if (kind === 'revoke') await this.commitAll().catch(() => {})
    this.#end(kind, partitions)
    if (kind === 'loss') this.onLoss?.({ group: this.group, member: this.label })
  }

  #end(kind, partitions) {
    for (const { topic, partitions: ids } of partitions) {
      for (const partition of ids) {
        const held = this.#held(topic, partition)
        if (held === undefined) continue
        const key = `${topic}|${partition}`
        this.journal.append({
          kind,
          group: this.group,
          owner: held.owner,
          incarnation: held.incarnation,
          topic,
          partition,
        })
        this.owners.delete(key)
        this.processed.delete(key)
        this.committed.delete(key)
      }
    }
  }

  /**
   * Journals one delivery and commits every `COMMIT_EVERY` handled records. A commit that throws
   * fails the handler, so the client retries that record inside the same ownership: it is
   * declared a `retry` replay while the ownership lasts. Once it ended, the next owner's position
   * covers the redelivery.
   */
  async #handle({ topic, partition, message }) {
    const held = this.#held(topic, partition)
    if (held === undefined) return
    const key = `${topic}|${partition}`
    const [incarnation, sequence] = message.value.toString('latin1').split('|')
    const known = await this.acknowledged(topic, partition, message.offset, incarnation)
    if (this.owners.get(key) !== held) return
    const scope = { group: this.group, owner: held.owner, incarnation, topic, partition }
    if (known) {
      this.journal.append({ kind: 'handler', ...scope, offset: message.offset, sequence })
      this.gaps?.enter(this.group, this.phase, this.clock.now())
      const delivered = `${this.group}|${incarnation}`
      this.handled.set(delivered, (this.handled.get(delivered) ?? 0) + 1)
    }
    this.processed.set(key, BigInt(message.offset) + 1n)
    if (++this.sinceCommit % COMMIT_EVERY !== 0) return
    try {
      await this.commitAll()
    } catch (error) {
      if (known && this.owners.get(key) === held) {
        const to = String(BigInt(message.offset) + 1n)
        this.journal.append({ kind: 'replay', reason: 'retry', ...scope, from: message.offset, to })
      }
      throw error
    }
  }

  /**
   * Commits every partition whose contiguous processed position moved, then reads the broker back.
   * A commit the client refuses because ownership ended is not an error and not an observation.
   * Calls run one at a time, so a commit in flight is journaled before a later boundary and is
   * never repeated by it.
   * @returns {Promise<void>}
   */
  commitAll() {
    const run = this.committing.then(() => this.#commitProcessed())
    this.committing = run.catch(() => {})
    return run
  }

  async #commitProcessed() {
    for (const key of [...this.processed.keys()]) {
      const [topic, partition] = splitKey(key)
      const held = this.#held(topic, partition)
      const next = this.processed.get(key)
      if (held === undefined || next === undefined || next <= (this.committed.get(key) ?? 0n))
        continue
      try {
        await this.consumer.commitOffsets([{ topic, partition, offset: String(next) }])
      } catch (error) {
        if (error instanceof KafkaJSCommitFailedError) continue
        throw error
      }
      const brokerNext = (await this.#positions(topic)).get(partition)
      this.committed.set(key, next)
      this.journal.append({
        kind: 'commit',
        group: this.group,
        owner: held.owner,
        incarnation: held.incarnation,
        topic,
        partition,
        next: String(next),
        brokerNext,
      })
    }
  }

  async #positions(topic) {
    const [{ partitions }] = await this.admin.fetchOffsets({ groupId: this.group, topics: [topic] })
    return new Map(
      partitions.map(({ partition, offset }) => [partition, offset === '-1' ? '0' : offset])
    )
  }
}

/**
 * Builds the evidence-writing, budget-bound run of the four phases. The workloads are injected: the
 * real ones are `ChurnWorkloads`, a unit spec passes fakes, and both go through the same verdict.
 */
class ConsumerChurnRun {
  /**
   * @param {object} options
   * @param {{ setup: Function, drain: Function, close: Function, [workload: string]: Function }} options.workloads
   *   each shape workload is `(context, ms) => Promise<void>`, runs in the phase the run has set, and
   *   leaves its clients disconnected
   * @param {object} options.probe a `PublicConsumerProbe`
   * @param {import('./consumerChurn').ChurnJournal} options.journal the issue-ordered observations
   * @param {(line: object) => void} options.record where every evidence line goes
   * @param {ReturnType<import('./consumerChurn').consumerChurnPlan>} options.plan
   * @param {{ now: () => number }} options.clock a monotonic clock
   * @param {(ms: number, signal?: AbortSignal) => Promise<void>} options.sleep
   * @param {import('./consumerChurn').GapSampler} options.gaps
   * @param {string[]} [options.groups=CONSUMER_CHURN.groups] the base groups
   * @param {{ id: (base: string, phase: string) => string }} [options.scope] the mapping from a base
   *   group to the id the clients of a phase join, shared with the workloads; the base itself by default
   * @param {() => Promise<number>} [options.heap] the double-collected heap
   * @param {typeof CONSUMER_CHURN} [options.settings]
   * @param {number} [options.progressMs=CONSUMER_CHURN.progressMs]
   * @param {import('./consumerChurn').ChurnClientLog} [options.clientLog] the client's WARN+ lines;
   *   each `loss` round is recorded as a `loss` evidence row naming the lines near it
   */
  constructor({
    workloads,
    probe,
    journal,
    record,
    plan,
    clock,
    sleep,
    gaps,
    groups = CONSUMER_CHURN.groups,
    scope = { id: (base) => base },
    heap,
    settings = CONSUMER_CHURN,
    progressMs = CONSUMER_CHURN.progressMs,
    clientLog,
  }) {
    Object.assign(this, {
      workloads,
      probe,
      journal,
      record,
      plan,
      clock,
      sleep,
      gaps,
      groups,
      scope,
      heap,
      settings,
      progressMs,
      clientLog,
    })
    const phases = [...plan.warmups, ...plan.phases].map(({ name }) => name)
    this.oracle = new ConsumerChurnOracle(
      [],
      [...new Set(groups.flatMap((base) => phases.map((phase) => scope.id(base, phase))))]
    )
    this.quiets = []
    this.warmQuiets = []
    this.stats = new Map()
    this.unhandled = 0
    this.onUnhandled = () => {
      this.unhandled += 1
    }
    this.closed = false
    this.timer = null
    this.phase = 'setup'
  }

  /**
   * Runs setup, the warm-up, the phases and the final checks inside one inclusive deadline.
   * @returns {Promise<{ ok: boolean, acceptance: boolean, violations: string[] }>} the verdict,
   *   also recorded as the run's one `verdict` line, judged only after every client is closed
   * @throws {Error} the failure of a stage or of the cleanup, after a failing verdict is recorded
   */
  async run() {
    const { plan, settings } = this
    const deadline = new ChurnDeadline(this.clock, settings.childMs)
    process.on('unhandledRejection', this.onUnhandled)
    this.timer = setInterval(
      () =>
        this.record({
          kind: 'progress',
          phase: this.phase,
          atMs: this.clock.now() - deadline.startedAt,
          remainingMs: deadline.remaining(),
        }),
      this.progressMs
    )
    try {
      if (plan.acceptance) {
        const need =
          plan.warmupMs +
          plan.phases.reduce((total, { ms }) => total + ms, 0) +
          3 * settings.transitionMs +
          settings.finalMs
        deadline.require(need, 'the whole plan')
      }
      for (const { name } of plan.phases) this.#prepare(name)
      this.record({ kind: 'config', acceptance: plan.acceptance, plan, groups: this.groups })
      const context = (this.context = this.#context(deadline))
      this.phase = 'setup'
      await this.workloads.setup(context)
      await this.#warmUp(context)
      this.baseline = await this.quiet('warm')
      for (const [index, { name, ms }] of plan.phases.entries()) {
        this.phase = name
        const startedAt = this.clock.now()
        await this.runPhase(name, ms)
        this.stats.set(name, {
          ...(this.stats.get(name) ?? {}),
          measuredMs: this.clock.now() - startedAt,
        })
        const last = index === plan.phases.length - 1
        deadline.require(
          last ? settings.finalMs : settings.transitionMs,
          `the transition after ${name}`
        )
        await this.workloads.drain(context, name)
        await this.#verify()
        await this.quiet(name)
      }
      this.phase = 'final'
      await this.close()
      const verdict = await this.#judge()
      this.record({ kind: 'verdict', ...verdict })
      return verdict
    } catch (error) {
      this.record({
        kind: 'verdict',
        ok: false,
        acceptance: plan.acceptance,
        violations: [`run failed: ${error.message}`],
      })
      throw error
    } finally {
      await this.close()
    }
  }

  /**
   * Runs one short pass of each shape, each in a phase of its own: its own groups and topics, and no
   * gap series. After each pass the journal is verified and a quiet heap is read, kept apart from
   * the measured series. A plan with no warm-up passes does nothing.
   * @param {object} context the run's context
   * @returns {Promise<void>}
   */
  async #warmUp(context) {
    if (this.plan.warmups.length === 0) return
    this.phase = 'warm-up'
    await this.quiet('warm-up', this.warmQuiets)
    for (const { name, workload, ms } of this.plan.warmups) {
      this.phase = name
      await this.workloads[workload](context, ms)
      await this.workloads.drain(context, name)
      await this.#verify()
      await this.quiet(name, this.warmQuiets)
    }
  }

  /**
   * Runs one measured phase's workload with the run's context; `run` calls it for every phase.
   * @param {string} name a phase of the plan
   * @param {number} ms how long the phase lasts
   * @returns {Promise<void>} resolves once the workload left its clients disconnected
   * @throws {Error} for a phase the plan does not have, or the failure of the workload
   */
  async runPhase(name, ms) {
    const phase = this.plan.phases.find((candidate) => candidate.name === name)
    if (!phase) throw new Error(`The consumer churn plan has no phase ${name}`)
    await this.workloads[phase.workload](this.context, ms)
  }

  /**
   * Stops the timers, closes what the workloads still hold and the journal, and takes the
   * unhandled-rejection listener off the process; safe to call again.
   * @returns {Promise<void>}
   * @throws {Error} the first failure, after every step was tried
   */
  async close() {
    if (this.closed) return
    this.closed = true
    clearInterval(this.timer)
    const failures = []
    for (const step of [
      () => this.workloads.close(),
      () => this.#recordLosses(true),
      () => this.clientLog?.close(),
      () => this.journal.close(),
      () => process.off('unhandledRejection', this.onUnhandled),
    ]) {
      try {
        await step()
      } catch (error) {
        failures.push(error)
      }
    }
    if (failures.length > 0) throw failures[0]
  }

  #prepare(phase) {
    for (const base of this.groups) this.gaps.prepare(this.scope.id(base, phase), phase)
  }

  #context(deadline) {
    return {
      journal: this.journal,
      gaps: this.gaps,
      clock: this.clock,
      sleep: this.sleep,
      record: this.record,
      deadline,
      plan: this.plan,
      groups: this.groups,
      stats: this.stats,
      phase: () => this.phase,
      measured: () => this.plan.phases.some(({ name }) => name === this.phase),
      offers: () =>
        new OfferScheduler({ rate: this.settings.rate + 20, clock: this.clock, sleep: this.sleep }),
    }
  }

  /** Feeds the oracle every record journaled since the last check, a segment at a time. */
  async #verify() {
    const delivered = await this.journal.replay((records) => this.oracle.feed(records))
    this.verified = (this.verified ?? 0) + delivered
    this.record({
      kind: 'oracle',
      verified: this.verified,
      retained: this.oracle.retained(),
      counts: this.oracle.counts,
    })
    this.#recordLosses()
  }

  /** Records each lost round whose client-log window has closed, or every one when `all`. */
  #recordLosses(all = false) {
    for (const loss of this.clientLog?.settle(all) ?? [])
      this.record({
        kind: 'loss',
        group: loss.group,
        member: loss.member,
        at: loss.at,
        clientLog: { count: loss.count, first: loss.first },
      })
  }

  /**
   * One quiet point: every client closed and, within a bounded grace, every socket closed too; the
   * minimum number of matched readings taken, and the leftovers compared with the warm baseline.
   * @param {string} boundary
   * @param {object[]} [into=this.quiets] the series the readings join
   * @returns {Promise<object>} the first reading of the point
   */
  async quiet(boundary, into = this.quiets) {
    const graceEnds = this.clock.now() + this.settings.socketGraceMs
    while (this.probe.openSockets() > 0 && this.clock.now() < graceEnds) await this.sleep(100)
    const readings = []
    for (let n = 0; n < this.settings.quietSamples; n++) {
      const snapshot = await this.probe.snapshot()
      const reading = { boundary, atMs: this.clock.now(), snapshot, heapUsed: snapshot.heapUsed }
      readings.push(reading)
      into.push(reading)
      this.record({ kind: 'quiet', boundary, snapshot })
    }
    return readings[0].snapshot
  }

  /** @returns {object[]} each shape's heap growth in its warm-up pass and in its measured pass */
  #shapeHeap() {
    const { warmups, phases } = this.plan
    if (warmups.length === 0) return []
    return shapeHeapGrowth([...this.warmQuiets, ...this.quiets], {
      warm: ['warm-up', ...warmups.map(({ name }) => name)],
      measured: ['warm', ...phases.map(({ name }) => name)],
      shapes: phases.map(({ name }) => name),
    })
  }

  async #judge() {
    const { plan, settings } = this
    const violations = []
    const finished = await this.oracle.finish().catch((error) => {
      violations.push(error.message)
      return null
    })
    const controls = await runControls(this.groups.length > 0 ? ['g'] : [])
    for (const control of controls) {
      if (!control.ok)
        violations.push(
          `control ${control.name}: expected "${control.diagnostic}", got ${control.received}`
        )
    }
    for (const reading of this.quiets) {
      violations.push(
        ...quietViolations(`after ${reading.boundary}`, reading.snapshot, this.baseline)
      )
    }
    const gaps = this.gaps.judge(plan.phases[0].ms)
    if (plan.acceptance) violations.push(...gaps.violations)
    const boundaries = ['warm', ...plan.phases.map(({ name }) => name)]
    const heap = this.heap
      ? judgeQuietHeap(
          this.quiets.filter((reading) => reading.heapUsed !== undefined),
          { boundaries }
        )
      : {
          violations: ['no heap reader: the run cannot judge memory'],
          endpointBytes: null,
          slopeBytesPerMinute: null,
        }
    violations.push(...heap.violations)
    const shapeHeap = this.#shapeHeap()
    const interning = this.heap
      ? await interningControl({ heap: this.heap, count: plan.acceptance ? undefined : 4096 })
      : null
    if (plan.acceptance && (!interning || !interning.ok))
      violations.push('the interning sensitivity control did not see its own growth')
    if (this.unhandled > 0) violations.push(`unhandled rejections: ${this.unhandled}`)
    if (plan.acceptance)
      violations.push(...acknowledgedViolations(this.stats, plan.phases, settings.rate))
    return {
      ok: violations.length === 0,
      acceptance: plan.acceptance,
      violations,
      counts: finished,
      controls,
      gaps: gaps.summary,
      heap,
      shapeHeap,
      interning,
      phases: Object.fromEntries(this.stats),
    }
  }
}

/**
 * Reads acknowledged records back from the log with its own consumer, in a group of its own that
 * commits nothing, and journals one `read` for each record it finds under the incarnation and
 * sequence that were acknowledged. It shares no handler, group or offset with the members it checks.
 */
class ChurnLogReader {
  /**
   * @param {object} options
   * @param {object} options.consumer a public consumer in a group of its own
   * @param {{ append: (observation: object) => void }} options.journal
   * @param {{ now: () => number }} options.clock
   * @param {(ms: number) => Promise<void>} options.sleep
   * @param {number} [options.timeoutMs=DRAIN_MS] how long to wait for the last record
   */
  constructor({ consumer, journal, clock, sleep, timeoutMs = DRAIN_MS }) {
    Object.assign(this, { consumer, journal, clock, sleep, timeoutMs })
  }

  /**
   * Reads from the start of each topic until every acknowledged record was seen once.
   * @param {Array<{ name: string, incarnation: string, acks: Map<string, string> }>} topics `acks` maps
   *   `partition|offset` to the acknowledged sequence
   * @returns {Promise<void>}
   * @throws {Error} when records are still missing after `timeoutMs`
   */
  async read(topics) {
    const wanted = new Map(topics.map((topic) => [topic.name, topic]))
    const seen = new Set()
    const unread = { count: topics.reduce((total, { acks }) => total + acks.size, 0) }
    await this.consumer.connect()
    for (const { name } of topics)
      await subscribeNamedTopic(this.consumer, { topic: name, fromBeginning: true })
    await this.consumer.run({
      autoCommit: false,
      eachMessage: ({ topic: name, partition, message }) => {
        const topic = wanted.get(name)
        if (topic === undefined) return
        const [incarnation, sequence] = message.value.toString('latin1').split('|')
        const id = `${partition}|${message.offset}`
        const key = `${name}|${id}`
        if (incarnation !== topic.incarnation || topic.acks.get(id) !== sequence || seen.has(key))
          return
        seen.add(key)
        unread.count -= 1
        this.journal.append({
          kind: 'read',
          incarnation,
          topic: name,
          partition,
          offset: message.offset,
          sequence,
        })
      },
    })
    const until = this.clock.now() + this.timeoutMs
    while (unread.count > 0 && this.clock.now() < until) await this.sleep(DRAIN_POLL_MS)
    if (unread.count > 0)
      throw new Error(
        `Consumer churn log: ${unread.count} acknowledged records were not read back within ${this.timeoutMs} ms`
      )
  }
}

/**
 * The real workloads, over public clients: a journal of what was acknowledged, owned, delivered and
 * committed, a persistent control topic holding the offered rate and, per phase, its own topics
 * whose acknowledged corpus is read back from the log, drained and committed before they are
 * deleted. Every workload opens the producer and admin it needs and closes them, and every member
 * and reader, before it returns, so the run's quiet points have no client open.
 */
class ChurnWorkloads {
  /**
   * @param {object} options
   * @param {object} options.kafka a public `Kafka` built with the probe's socket factory
   * @param {object} options.probe a `PublicConsumerProbe`, told about every client built
   * @param {string} options.tag a unique stem for topics and groups
   * @param {ChurnGroups} [options.groups] the group ids of every phase, shared with the run
   * @param {import('./consumerChurn').ChurnClientLog} [options.clientLog] told of every lost round
   */
  constructor({ kafka, probe, tag, groups = new ChurnGroups(tag), clientLog }) {
    Object.assign(this, { kafka, probe, tag, groups, clientLog })
    this.nonce = 0
    this.clients = new Map()
    this.handled = new Map()
    this.live = new Map()
  }

  /** @returns {Promise<void>} proves the cluster answers the shared clients, and leaves them closed */
  setup() {
    return this.#session(async () => {})
  }

  #track(client) {
    this.clients.set(client, this.probe.track(client))
    return client
  }

  #untrack(client) {
    this.clients.get(client)?.()
    this.clients.delete(client)
  }

  /** Disconnects each client and forgets it, trying all; the first failure is thrown last. */
  async #disconnect(...clients) {
    const failures = []
    for (const client of clients) {
      try {
        await client.disconnect()
      } catch (error) {
        failures.push(error)
      } finally {
        this.#untrack(client)
      }
    }
    if (failures.length > 0) throw failures[0]
  }

  /**
   * Disconnects every client still open and removes the probe's listeners.
   * @returns {Promise<void>}
   * @throws {Error} the first disconnect failure, after every client was tried
   */
  async close() {
    await this.#disconnect(...this.clients.keys())
  }

  /** Opens the shared producer and admin, runs `body`, and closes them again before returning. */
  async #session(body) {
    this.admin = this.#track(this.kafka.admin())
    this.producer = this.#track(
      this.kafka.producer({
        idempotent: true,
        maxInFlightRequests: 5,
        allowAutoTopicCreation: false,
      })
    )
    try {
      await this.admin.connect()
      await this.producer.connect()
      await body()
    } catch (error) {
      await this.#disconnect(this.producer, this.admin).catch(() => {})
      throw error
    }
    await this.#disconnect(this.producer, this.admin)
  }

  /** Gives a topic name its incarnation, before anything can be assigned or delivered for it. */
  #claim(name, partitions, context) {
    const topic = {
      name,
      incarnation: `${name}.${++this.nonce}`,
      groups: this.groups.forPhase(context.phase()),
      partitions,
      next: 0,
      acked: 0,
      verified: 0,
      acks: new Map(),
      waiting: new Map(),
    }
    this.live.set(name, topic)
    return topic
  }

  /**
   * Creates a topic with the partitions and replication the workload relies on.
   * @param {object} topic the claimed topic
   * @throws {Error} when the topic already exists: a client's metadata request created it first,
   *   with the broker's defaults, and the run would go on over fewer partitions than it offers to
   */
  async #create(topic) {
    const created = await this.admin.createTopics({
      topics: [
        {
          topic: topic.name,
          numPartitions: topic.partitions,
          replicationFactor: 3,
          configEntries: [{ name: 'min.insync.replicas', value: '2' }],
        },
      ],
      waitForLeaders: true,
    })
    if (created === false)
      throw new Error(`Topic ${topic.name} already existed when it was created`)
  }

  async #topic(name, partitions, context) {
    const topic = this.#claim(name, partitions, context)
    await this.#create(topic)
    return topic
  }

  /** Journals a topic's deletion and deletes it; its name has no incarnation afterwards unless a successor claimed it. */
  async #delete(topic, context) {
    context.journal.append({ kind: 'delete', incarnation: topic.incarnation, topic: topic.name })
    await this.admin.deleteTopics({ topics: [topic.name] })
    if (this.live.get(topic.name) === topic) this.live.delete(topic.name)
  }

  /**
   * Resolves true once the record's acknowledgement is journaled, false if none comes within
   * `ACK_WAIT_MS`; a record of an incarnation that is not current is judged by the oracle, not awaited.
   * @param {string} name the topic
   * @param {number} partition
   * @param {string} offset
   * @param {string} incarnation the incarnation the record's value names
   * @returns {boolean|Promise<boolean>}
   */
  acknowledged(name, partition, offset, incarnation) {
    const topic = this.live.get(name)
    const id = `${partition}|${offset}`
    if (topic?.incarnation !== incarnation || topic.acks.has(id)) return true
    return new Promise((resolve) => {
      const waiters = topic.waiting.get(id) ?? new Set()
      const settle = (acked) => {
        clearTimeout(timer)
        waiters.delete(settle)
        if (waiters.size === 0 && topic.waiting.get(id) === waiters) topic.waiting.delete(id)
        resolve(acked)
      }
      const timer = setTimeout(() => settle(false), ACK_WAIT_MS)
      waiters.add(settle)
      topic.waiting.set(id, waiters)
    })
  }

  #offer(topic, context, trafficPartitions = topic.partitions) {
    const index = topic.next++
    const partition = index % trafficPartitions
    const sequence = `s${index}`
    const head = `${topic.incarnation}|${sequence}|`
    const value = Buffer.alloc(CONSUMER_CHURN.payloadBytes, 0x20)
    value.write(head, 'latin1')
    return this.producer
      .send({ topic: topic.name, acks: -1, messages: [{ partition, value }] })
      .then(([meta]) => {
        const id = `${partition}|${meta.baseOffset}`
        topic.acked += 1
        topic.acks.set(id, sequence)
        context.journal.append({
          kind: 'produce',
          incarnation: topic.incarnation,
          topic: topic.name,
          partition,
          offset: String(meta.baseOffset),
          sequence,
          groups: topic.groups,
        })
        for (const settle of [...(topic.waiting.get(id) ?? [])]) settle(true)
        topic.waiting.delete(id)
      })
  }

  #member(context, protocol, base, label, options = {}) {
    const group = this.groups.id(base, context.phase())
    const consumer = this.#track(
      this.kafka.consumer({
        groupId: group,
        ...(protocol === 'consumer'
          ? { groupProtocol: 'consumer' }
          : { partitionAssigners: [require('../../index').PartitionAssigners.cooperativeSticky] }),
        ...(options.instance ? { groupInstanceId: options.instance } : {}),
        metadataMaxAge: options.metadataMaxAge ?? 300000,
        maxWaitTimeInMs: 500,
        allowAutoTopicCreation: false,
      })
    )
    return new ChurnMember({
      consumer,
      admin: this.admin,
      group,
      label,
      journal: context.journal,
      gaps: context.measured() ? context.gaps : null,
      phase: context.phase(),
      clock: context.clock,
      handled: this.handled,
      onLoss: (loss) => this.clientLog?.noteLoss(loss),
      incarnationOf: (name) => this.live.get(name)?.incarnation,
      acknowledged: (name, partition, offset, incarnation) =>
        this.acknowledged(name, partition, offset, incarnation),
    })
  }

  /** Stops a member and forgets its client at once, so retired members do not accumulate. */
  async #stop(member, members) {
    members.splice(members.indexOf(member), 1)
    try {
      await member.stop()
    } finally {
      this.#untrack(member.consumer)
    }
  }

  /** Reads the topics' acknowledged records back from the log with a reader of its own. */
  async #readLog(topics, context) {
    const pending = topics.filter((topic) => topic.verified !== topic.acked)
    if (pending.length === 0) return
    const consumer = this.#track(
      this.kafka.consumer({
        groupId: `${this.tag}-reader-${++this.nonce}`,
        metadataMaxAge: 300000,
        maxWaitTimeInMs: 500,
        allowAutoTopicCreation: false,
      })
    )
    try {
      await new ChurnLogReader({
        consumer,
        journal: context.journal,
        clock: context.clock,
        sleep: context.sleep,
      }).read(pending)
    } finally {
      await this.#disconnect(consumer)
    }
    for (const topic of pending) topic.verified = topic.acked
  }

  /** Waits until every group has seen as many records of each topic as were acknowledged to it. */
  async #drainAll(topics, members, context) {
    const deadline = context.clock.now() + DRAIN_MS
    const missing = () =>
      topics.some((topic) =>
        topic.groups.some(
          (group) => (this.handled.get(`${group}|${topic.incarnation}`) ?? 0) < topic.acked
        )
      )
    while (missing() && context.clock.now() < deadline) await context.sleep(DRAIN_POLL_MS)
    for (const member of members) await member.commitAll()
  }

  async #run(context, ms, { topics, members, fault }) {
    this.handled.clear()
    const scheduler = context.offers()
    const aborted = new AbortController()
    const current = () => (typeof topics === 'function' ? topics() : topics)
    const faults = fault ? fault(aborted.signal) : Promise.resolve()
    const until = context.clock.now() + ms
    await scheduler.run(
      until,
      (index) => {
        const live = current()
        return live.length === 0 ? null : this.#offer(live[index % live.length], context)
      },
      aborted.signal
    )
    aborted.abort()
    await faults
    context.stats.set(context.phase(), {
      acked: scheduler.stats.settled,
      offered: scheduler.stats.offered,
      failed: scheduler.stats.failed,
      slip: scheduler.stats.slip,
      backpressure: scheduler.stats.backpressure,
    })
    const live = current()
    await this.#drainAll(live, members, context)
    return live
  }

  /** Stops the members, reads every topic back from the log, and only then deletes them. */
  async #finish(context, topics, members) {
    for (const member of [...members]) await this.#stop(member, members)
    await this.#readLog(topics, context)
    for (const topic of topics) await this.#delete(topic, context)
  }

  /**
   * Both groups, six partitions, up to three members each with one static, a dynamic one replaced every 20 s.
   * @param {object} context the run's context
   * @param {number} ms how long the phase lasts
   * @returns {Promise<void>}
   */
  memberChurn(context, ms) {
    return this.#session(async () => {
      const topic = await this.#topic(`${this.tag}-member-${++this.nonce}`, 6, context)
      const members = []
      const slots = []
      for (const [index, base] of context.groups.entries()) {
        const protocol = index === 0 ? 'classic' : 'consumer'
        const stable = this.#member(context, protocol, base, `static-${index}`, {
          instance: `static-${index}-${this.tag}`,
        })
        members.push(stable)
        await stable.start([topic.name])
        const dynamic = this.#member(context, protocol, base, `dynamic-${index}-0`)
        members.push(dynamic)
        await dynamic.start([topic.name])
        slots.push({ index, protocol, base, current: dynamic, generation: 0 })
      }
      const fault = async (signal) => {
        while (!signal.aborted) {
          await context.sleep(20000, signal)
          if (signal.aborted) break
          for (const slot of slots) {
            await this.#stop(slot.current, members)
            slot.generation += 1
            slot.current = this.#member(
              context,
              slot.protocol,
              slot.base,
              `dynamic-${slot.index}-${slot.generation}`
            )
            members.push(slot.current)
            await slot.current.start([topic.name])
          }
        }
      }
      const live = await this.#run(context, ms, {
        topics: [topic],
        members,
        fault,
      })
      await this.#finish(context, live, members)
    })
  }

  /**
   * Fifty topic names under one pattern, created, filled, read, drained and deleted in rotation.
   * @param {object} context the run's context
   * @param {number} ms how long the phase lasts
   * @returns {Promise<void>}
   */
  regexTopics(context, ms) {
    return this.#session(async () => {
      const stem = `${this.tag}-rx-${++this.nonce}`
      const control = await this.#topic(`${stem}-control`, 6, context)
      const members = []
      for (const [index, base] of context.groups.entries()) {
        const member = this.#member(
          context,
          index === 0 ? 'classic' : 'consumer',
          base,
          `rx-${index}`,
          { metadataMaxAge: 5000 }
        )
        members.push(member)
        await member.start([new RegExp(`^${stem}`)])
      }
      const used = []
      const fault = async (signal) => {
        for (let n = 0; n < 50 && !signal.aborted; n++) {
          const topic = await this.#topic(`${stem}-${n}`, 3, context)
          used.push(topic)
          for (let record = 0; record < 12; record++) await this.#offer(topic, context)
          await this.#drainAll([topic, ...used.slice(0, -1)], members, context)
          await this.#readLog([topic], context)
          await context.sleep(Math.max(1, Math.floor(ms / 50)), signal)
        }
      }
      const live = await this.#run(context, ms, { topics: [control], members, fault })
      await this.#finish(context, [...used, ...live], members)
    })
  }

  /**
   * Pause and resume one partition every 5 s, recreate the topic every minute, both modes.
   * @param {object} context the run's context
   * @param {number} ms how long the phase lasts
   * @returns {Promise<void>}
   */
  pauseRecreate(context, ms) {
    return this.#session(async () => {
      const name = `${this.tag}-pause-${++this.nonce}`
      let topic = await this.#topic(name, 6, context)
      const members = []
      for (const [index, base] of context.groups.entries()) {
        const member = this.#member(
          context,
          index === 0 ? 'classic' : 'consumer',
          base,
          `pause-${index}`
        )
        members.push(member)
        await member.start([name])
      }
      const fault = async (signal) => {
        let elapsed = 0
        while (!signal.aborted) {
          for (const member of members)
            member.consumer.pause([{ topic: name, partitions: [elapsed % 6] }])
          await context.sleep(2500, signal)
          for (const member of members)
            member.consumer.resume([{ topic: name, partitions: [elapsed % 6] }])
          await context.sleep(2500, signal)
          elapsed += 1
          if (elapsed % 12 === 0 && !signal.aborted) {
            const old = topic
            topic = null
            await this.#drainAll([old], members, context)
            await this.#readLog([old], context)
            const next = this.#claim(name, 6, context)
            await this.#delete(old, context)
            await this.#create(next)
            topic = next
          }
        }
      }
      const live = await this.#run(context, ms, {
        topics: () => (topic ? [topic] : []),
        members,
        fault,
      })
      await this.#finish(context, live, members)
    })
  }

  /**
   * One thousand assigned partitions in each group, with traffic on ten of them.
   * @param {object} context the run's context
   * @param {number} ms how long the phase lasts
   * @returns {Promise<void>}
   */
  mostlyIdle(context, ms) {
    return this.#session(async () => {
      const topic = await this.#topic(`${this.tag}-idle-${++this.nonce}`, 1000, context)
      const members = []
      for (const [index, base] of context.groups.entries()) {
        const member = this.#member(
          context,
          index === 0 ? 'classic' : 'consumer',
          base,
          `idle-${index}`
        )
        members.push(member)
        await member.start([topic.name])
      }
      const scheduler = context.offers()
      const until = context.clock.now() + ms
      await scheduler.run(until, () => this.#offer(topic, context, 10))
      context.stats.set(context.phase(), {
        acked: scheduler.stats.settled,
        offered: scheduler.stats.offered,
      })
      await this.#drainAll([topic], members, context)
      await this.#finish(context, [topic], members)
    })
  }

  /** @returns {Promise<void>} every phase workload ends drained, committed and disconnected, so nothing is left to do */
  async drain() {}
}

module.exports = { ChurnLogReader, ChurnMember, ChurnWorkloads, ConsumerChurnRun }
