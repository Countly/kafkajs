const { isDeepStrictEqual } = require('util')
const { NEVER, STOP_GRACE_MS, bounded, liveRun, pollUntil } = require('./bounded')

/** The whole case, which `liveCase` fails at; the phases and their stop graces must fit inside it. */
const CASE_BUDGET_MS = 180000

/** What the case keeps for the operations that are told to stop, one `STOP_GRACE_MS` per phase. */
const JOIN_RESERVE_MS = 4000

/** Serial sub-budgets of one follower-outage case, in the order they run. */
const PHASE_BUDGET_MS = Object.freeze({
  setup: 15000,
  stop: 25000,
  shrink: 35000,
  measure: 30000,
  restore: 35000,
  tail: 20000,
  cleanup: 15000,
})

/** How the restore phase splits between its steps. */
const RESTORE_STEP_MS = Object.freeze({ start: 15000, reachable: 8000, isr: 12000 })

/** The cluster's own topics whose replication a full-ISR check also reads, when they exist. */
const INTERNAL_TOPICS = Object.freeze(['__consumer_offsets', '__transaction_state'])

/**
 * @param {Array<number>} ids
 * @returns {number[]} a sorted copy, so replica sets compare as sets
 */
const sorted = (ids) => [...ids].sort((a, b) => a - b)

/**
 * The phases of one case under one owner. The owner is an `AbortController` the case creates; each
 * phase is a `bounded` call under its signal with the budget of the table, so a phase that runs out
 * of time aborts its own scope, which the operations it started observe, and the case aborts the
 * owner when it ends. The table is checked when the timeline is made: the phases, plus a stop
 * grace for each (the join reserve), must fit the case, so the serial bound is enforced and not
 * just documented. A phase that must outlive the owner, the restore, runs under another signal. A
 * phase that runs out of time or whose owner aborts rejects at once, and the run is tainted
 * (`liveRun`).
 */
class CaseTimeline {
  /**
   * @param {object} [options]
   * @param {Record<string, number>} [options.phases] the budget of each phase, in ms
   * @param {number} [options.limitMs] the case's whole budget
   * @param {number} [options.reserveMs] what the case keeps for stopping operations
   * @throws {Error} when the phases and their stop graces do not fit the case
   */
  constructor({
    phases = PHASE_BUDGET_MS,
    limitMs = CASE_BUDGET_MS,
    reserveMs = JOIN_RESERVE_MS,
  } = {}) {
    const names = Object.keys(phases)
    const total = names.reduce((sum, name) => sum + phases[name], 0)
    if (total + reserveMs > limitMs || names.length * STOP_GRACE_MS > reserveMs) {
      throw new Error(
        `${names.length} phases of ${total} ms and a reserve of ${reserveMs} ms do not fit ${limitMs} ms`
      )
    }
    this.phases = phases
    this.owner = new AbortController()
    /** @type {Array<{ name: string, budgetMs: number, elapsedMs: number }>} every phase begun */
    this.begun = []
  }

  /** @returns {AbortSignal} the case's, aborted when the case ends */
  get signal() {
    return this.owner.signal
  }

  /**
   * Runs one phase.
   * @template T
   * @param {string} name a key of the table
   * @param {(signal: AbortSignal) => Promise<T>} work every awaited step of it goes through
   *   `bounded` under the signal it is given
   * @param {AbortSignal} [signal] the owner of the phase; the case's by default
   * @returns {Promise<T>}
   * @throws {Error} `work`'s own failure, `The <name> phase did not settle within <ms> ms`, the
   *   reason the owner aborted with, or that the table has no budget for `name`
   */
  async run(name, work, signal = this.signal) {
    const budgetMs = this.phases[name]
    if (budgetMs === undefined) throw new Error(`No budget for the ${name} phase`)
    const timing = { name, budgetMs, elapsedMs: 0 }
    this.begun.push(timing)
    const startedAt = performance.now()
    try {
      return await bounded(signal, budgetMs, work, { label: `The ${name} phase` })
    } finally {
      timing.elapsedMs = Math.round(performance.now() - startedAt)
    }
  }

  /**
   * Ends the case: whatever a phase left running is told to stop.
   * @param {Error} [reason]
   * @returns {void}
   */
  abort(reason) {
    this.owner.abort(reason)
  }

  /** @returns {Array<{ name: string, budgetMs: number, elapsedMs: number }>} for the evidence */
  timings() {
    return this.begun.map((timing) => ({ ...timing }))
  }
}

/**
 * One check of whether every partition of the workload topics, and of the cluster's internal ones,
 * has its whole replica set in sync. Each read goes through `bounded` under `signal`, the scope of
 * the poll that calls it, so it is cut off with that poll's remaining budget and nothing is read
 * once it ran out.
 * @param {AbortSignal} signal
 * @param {object} admin a connected admin client
 * @param {string[]} topics the workload topics
 * @returns {Promise<boolean>}
 */
const isrIsFull = async (signal, admin, topics) => {
  const names = await bounded(signal, Infinity, () => admin.listTopics())
  const internal = names.filter((name) => INTERNAL_TOPICS.includes(name))
  const metadata = await bounded(signal, Infinity, () =>
    admin.fetchTopicMetadata({ topics: [...topics, ...internal] })
  )
  return metadata.topics.every(({ partitions }) =>
    partitions.every(({ isr, replicas }) => isr.length === replicas.length)
  )
}

/**
 * One partition as the cluster reports it.
 * @typedef {object} PartitionState
 * @property {string} topic
 * @property {number} partition
 * @property {number} leader
 * @property {number[]} replicas sorted
 * @property {number[]} isr sorted
 */

/**
 * The topology a one-follower fault must keep: for each workload partition its replica set and its
 * leader as read back before the fault, and the follower that goes down, a replica that leads none
 * of them. Anything else the cluster does meanwhile, a leader that moves, a replica or ISR member
 * that goes missing besides the follower, invalidates the measurement.
 */
class TopologyPin {
  /**
   * @param {object} options
   * @param {Array<Omit<PartitionState, 'isr'>>} options.partitions the partitions as read before the
   *   fault
   * @param {number} options.follower the node that goes down
   * @throws {Error} when the follower is not a replica of every partition, or leads one
   */
  constructor({ partitions, follower }) {
    this.follower = follower
    this.partitions = partitions.map(({ topic, partition, leader, replicas }) => ({
      topic,
      partition,
      leader,
      replicas: sorted(replicas),
    }))
    for (const { topic, partition, leader, replicas } of this.partitions) {
      if (!replicas.includes(follower)) {
        throw new Error(`Node ${follower} is not a replica of ${topic}-${partition}`)
      }
      if (leader === follower) throw new Error(`Node ${follower} leads ${topic}-${partition}`)
    }
  }

  /**
   * @param {Array<{ name: string, partitions: Array<{ partitionId: number, leader: number,
   *   replicas: number[], isr: number[] }> }>} topics what `admin.fetchTopicMetadata` returned
   * @returns {PartitionState[]} the pinned partitions as the metadata shows them, in pin order
   * @throws {Error} when a pinned partition is not in the metadata
   */
  snapshot(topics) {
    return this.partitions.map(({ topic, partition }) => {
      const metadata = topics
        .find(({ name }) => name === topic)
        ?.partitions.find(({ partitionId }) => partitionId === partition)
      if (metadata === undefined)
        throw new Error(`${topic}-${partition} is missing from the metadata`)
      return {
        topic,
        partition,
        leader: metadata.leader,
        replicas: sorted(metadata.replicas),
        isr: sorted(metadata.isr),
      }
    })
  }

  /**
   * The ISR the surviving replicas make while the follower is down.
   * @param {PartitionState} pinned
   * @returns {number[]}
   */
  survivors({ replicas }) {
    return replicas.filter((id) => id !== this.follower)
  }

  /**
   * Before the fault: every partition has its whole replica set in sync.
   * @param {PartitionState[]} snapshot from `snapshot`
   * @returns {void}
   * @throws {Error} naming the first partition that does not
   */
  whole(snapshot) {
    snapshot.forEach((state, index) => {
      this.assertKept(this.partitions[index], state)
      if (!isDeepStrictEqual(state.isr, state.replicas)) {
        throw new Error(
          `${state.topic}-${state.partition} is not in full sync: ${JSON.stringify(state)}`
        )
      }
    })
  }

  /**
   * While the fault is being reached: every partition keeps its leader and its replica set, and
   * its ISR is either whole or exactly the survivors.
   * @param {PartitionState[]} snapshot from `snapshot`
   * @returns {boolean} whether every ISR is now exactly the survivors
   * @throws {Error} naming the first partition that drifted
   */
  establishing(snapshot) {
    let reached = true
    snapshot.forEach((state, index) => {
      const pinned = this.partitions[index]
      this.assertKept(pinned, state)
      const survivors = this.survivors(pinned)
      if (isDeepStrictEqual(state.isr, survivors)) return
      if (!isDeepStrictEqual(state.isr, pinned.replicas)) {
        throw new Error(
          `${state.topic}-${state.partition} ISR moved unexpectedly: ${JSON.stringify(state)}`
        )
      }
      reached = false
    })
    return reached
  }

  /**
   * With the follower down: every partition keeps its leader and replica set and its ISR is
   * exactly the two survivors.
   * @param {PartitionState[]} snapshot from `snapshot`
   * @returns {void}
   * @throws {Error} naming the first partition that does not
   */
  holding(snapshot) {
    snapshot.forEach((state, index) => {
      const pinned = this.partitions[index]
      this.assertKept(pinned, state)
      if (!isDeepStrictEqual(state.isr, this.survivors(pinned))) {
        throw new Error(
          `${state.topic}-${state.partition} is not held by its survivors: ${JSON.stringify(state)}`
        )
      }
    })
  }

  /**
   * @param {PartitionState} pinned
   * @param {PartitionState} state
   * @returns {void}
   * @throws {Error} when the leader or the replica set differs from the pin
   */
  assertKept(pinned, state) {
    if (state.leader !== pinned.leader || !isDeepStrictEqual(state.replicas, pinned.replicas)) {
      throw new Error(
        `${state.topic}-${state.partition} drifted from its pin: ${JSON.stringify(state)}`
      )
    }
  }
}

/**
 * One follower's outage around a workload: the follower is stopped, the workload runs while it is
 * down, and the follower is started again, reachable and back in sync, whatever the workload did.
 * The stopped node is tracked by the bouncer, which refuses a second outage while one is open; a
 * node that could not be restored stays tracked and blocks the next case.
 *
 * The stop and the restore are phases of the timeline: a preflight that outlasts the stop phase
 * fails the stop and this outage issues no stop afterwards, and a broker action it issued is
 * aborted at its deadline and settled before it issues the next one. The restore is one attempt
 * shared by every caller, the case and a separately awaited hook alike, under a signal of its own
 * so that it survives the case's abort; every wait in it is a `bounded` poll, so it settles inside
 * its phase, and a start that ignored its abort is waited for, never overlapped. An attempt that
 * ran out of time leaves the follower owned for the next caller. An operation that ignores its
 * abort, such as a probe or a broker action, may still finish later; that, and a restore that
 * fails, taints the run (`liveRun`), and containing it is the guard's.
 */
class FollowerOutage {
  /**
   * @param {object} options
   * @param {import('../../../../testHelpers/leaderElection').BrokerBouncer} options.bouncer
   * @param {object} options.admin a connected admin client
   * @param {string[]} options.topics the workload topics whose ISR a restore waits for
   * @param {Record<string, number>} [options.phases] the budgets of the case, for the timeline made
   *   when none is given
   * @param {CaseTimeline} [options.timeline] the case's, of which this outage runs the `stop` and
   *   `restore` phases
   * @param {{ start: number, reachable: number, isr: number }} [options.steps] the budgets of the
   *   restore's steps
   */
  constructor({
    bouncer,
    admin,
    topics,
    phases = PHASE_BUDGET_MS,
    timeline = new CaseTimeline({ phases }),
    steps = RESTORE_STEP_MS,
  }) {
    this.bouncer = bouncer
    this.admin = admin
    this.topics = topics
    this.timeline = timeline
    this.steps = steps
    /** @type {string|null} the compose service of the follower this outage still owns */
    this.service = null
    /** @type {Promise<unknown>|null} the last broker action issued, which the next one waits for */
    this.acting = null
    /** @type {Promise<Error|null>|null} the restore attempt in progress */
    this.restoring = null
    this.chronology = {}
  }

  /** @returns {boolean} whether the follower is down and not yet restored */
  get open() {
    return this.service !== null
  }

  /**
   * @template T
   * @param {string} service the follower's compose service
   * @param {() => Promise<T>} workload runs while the follower is stopped
   * @returns {Promise<T>} what the workload returned, once the follower is restored
   * @throws {Error} the workload's own error, with a failed restoration attached as
   *   `cleanupErrors`; without one, the restoration's error
   */
  async run(service, workload) {
    let result
    try {
      await this.timeline.run('stop', (signal) => this.stop(service, signal))
      result = await workload()
    } catch (primary) {
      const failure = await this.restore()
      if (failure && primary !== null && typeof primary === 'object') {
        primary.cleanupErrors = [failure]
      }
      throw primary
    }
    const failure = await this.restore()
    if (failure) throw failure
    return result
  }

  /**
   * @param {string} service
   * @param {AbortSignal} [signal] the stop phase's; the case's when absent
   * @returns {Promise<void>}
   * @throws {Error} when a node is already down or does not answer, the signal aborted before the
   *   stop was issued, or the stop fails; the node is tracked from the moment the stop is issued
   */
  async stop(service, signal = this.timeline.signal) {
    await bounded(signal, Infinity, () => this.bouncer.assertAllUp())
    await bounded(signal, Infinity, (scope) => {
      this.bouncer.down.add(service)
      this.service = service
      return (this.acting = this.bouncer.run('stop', service, { signal: scope }))
    })
    this.chronology.stoppedAt = performance.now()
  }

  /**
   * Starts the follower again if this outage still owns it and waits until it is reachable and
   * every workload partition has its whole replica set in sync. Callers during an attempt share
   * it; after a failed one the next call tries again.
   * @returns {Promise<Error|null>} why the follower is not back, or null once it is, or when this
   *   outage owns none
   */
  restore() {
    if (this.service === null && this.restoring === null) return Promise.resolve(null)
    this.restoring ??= this.attempt().then((failure) => {
      this.restoring = null
      return failure
    })
    return this.restoring
  }

  /**
   * One restore attempt: a `restore` phase under a signal of its own.
   * @returns {Promise<Error|null>} null once the follower is back and in sync within the phase;
   *   otherwise why it is not, which also taints the run
   */
  async attempt() {
    try {
      await this.timeline.run('restore', (signal) => this.bringBack(signal), NEVER)
      return null
    } catch (error) {
      liveRun.taint(error)
      return error
    }
  }

  /**
   * Joins the broker action still running, starts the follower and waits for it and for the full
   * ISR. Nothing is issued once `signal` has aborted, and no start is issued while an earlier one
   * that ignored its abort is still running.
   * @param {AbortSignal} signal the restore phase's
   * @returns {Promise<void>}
   * @throws {Error} when a step fails or the phase runs out of time
   */
  async bringBack(signal) {
    const service = this.service
    const { bouncer, admin, topics, steps, chronology } = this
    await bounded(signal, Infinity, () => this.acting?.catch(() => {}))
    await bounded(
      signal,
      steps.start,
      (scope) => (this.acting = bouncer.run('start', service, { signal: scope })),
      { label: `The start of ${service}` }
    )
    chronology.startedAt = performance.now()
    await pollUntil(signal, steps.reachable, () => bouncer.probe(bouncer.nodes[service]), {
      delayMs: 200,
      label: `The wait for ${service} to answer`,
    })
    chronology.reachableAt = performance.now()
    await pollUntil(signal, steps.isr, (scope) => isrIsFull(scope, admin, topics), {
      label: 'The full ISR wait',
    })
    signal.throwIfAborted()
    chronology.fullIsrAt = performance.now()
    bouncer.down.delete(service)
    this.service = null
  }
}

module.exports = {
  CASE_BUDGET_MS,
  CaseTimeline,
  FollowerOutage,
  JOIN_RESERVE_MS,
  PHASE_BUDGET_MS,
  RESTORE_STEP_MS,
  TopologyPin,
  isrIsFull,
}
