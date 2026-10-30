const { KafkaJSError } = require('../errors')

/**
 * The local refusal of a preparation whose assignment, positions or offset manager were replaced
 * while it ran. It is not a membership fault and is never shown to the application.
 */
class PreparationSuperseded extends KafkaJSError {
  /** @returns {PreparationSuperseded} a non-retriable local refusal */
  constructor() {
    super('The fetch preparation was replaced by a newer assignment', { retriable: false })
    this.name = 'KafkaJSPreparationSuperseded'
  }
}

/**
 * One prepared node snapshot: the node-filtered partitions of one fetch attempt with the stamps
 * they were prepared under. Holds one small record per attempt, never a per-partition cache.
 *
 * `guard()` is the private, immutable identity every descendant of the preparation checks: the run
 * is still open and the assignment, ownership token and offset manager are the captured ones.
 * A routing change alone never makes it refuse, so churn cannot prevent the final attempt from
 * finishing; `isCurrent()` answers that separately in constant time.
 */
class FetchPreparation {
  /**
   * @param {import('./consumerGroup')} group the group whose state is captured, after the filter
   * @param {import('./idleWait')} run the run that owns this fetch
   * @param {import('../../types').TopicPartitions[]} parts the node-filtered assignment
   * @param {number} expiresAt earliest effective preferred-replica expiry in `parts`, or Infinity
   * @param {number} ownershipToken the membership identity the fetch captured before any await
   */
  constructor(group, run, parts, expiresAt, ownershipToken) {
    this.group = group
    this.run = run
    this.parts = parts
    this.expiresAt = expiresAt
    this.ownershipToken = ownershipToken
    this.routingVersion = group.routingVersion
    this.preparationEpoch = group.preparationEpoch
    this.manager = group.offsetManager
    this.assignmentEpoch = group.assignmentEpoch
    this.acquireClock = group.acquireClock
    this.superseded = null
  }

  /** @returns {import('../producer/generation')} the run's terminal fence */
  get fence() {
    return this.run.fence
  }

  /**
   * @returns {Error|null} the stopping error of a retired run, the supersession of a replaced
   *   assignment, or null while the preparation still owns what it operates on
   */
  guard = () => {
    if (this.run.fence.closed) return this.run.fence.error
    if (this.superseded) return this.superseded
    const { group } = this
    if (
      group.preparationEpoch !== this.preparationEpoch ||
      group.offsetManager !== this.manager ||
      group.ownership.token() !== this.ownershipToken
    ) {
      this.superseded = new PreparationSuperseded()
    }
    return this.superseded
  }

  /** @returns {void} @throws {Error} what `guard()` returned */
  check() {
    const refusal = this.guard()
    if (refusal) throw refusal
  }

  /**
   * @param {Error} error a rejection from a preparation step
   * @returns {boolean} whether it is this preparation's own local supersession
   */
  isSuperseded(error) {
    return error === this.superseded
  }

  /**
   * One integer comparison, and the clock only when a preferred-replica lease applies. Never
   * filters.
   * @returns {boolean} whether routing is unchanged since this snapshot was captured
   */
  isCurrent() {
    return (
      this.group.routingVersion === this.routingVersion &&
      (this.expiresAt === Infinity || Date.now() < this.expiresAt)
    )
  }

  /**
   * @param {number} maxWaitTime the configured idle duration
   * @returns {number} how long an idle wait may sleep: not past a known preferred-replica lease
   */
  idleDuration(maxWaitTime) {
    return this.expiresAt === Infinity
      ? maxWaitTime
      : Math.max(0, Math.min(maxWaitTime, this.expiresAt - Date.now()))
  }
}

/**
 * A delay bound to a terminal fence: its timer is cleared on settlement and on cancellation.
 * @param {import('../producer/generation')} fence the run's fence
 * @param {number} [ms] omitted, it yields once to the event loop (`setImmediate`)
 * @returns {Promise<void>} resolves after the delay
 * @throws {Error} the close error, when the fence closes first or was already closed
 */
const fencedDelay = (fence, ms) =>
  new Promise((resolve, reject) => {
    if (fence.closed) return reject(fence.error)
    const done = () => {
      unregister()
      resolve()
    }
    const timer = ms === undefined ? setImmediate(done) : setTimeout(done, ms)
    const unregister = fence.register((error) => {
      if (ms === undefined) clearImmediate(timer)
      else clearTimeout(timer)
      reject(error)
    })
  })

module.exports = { FetchPreparation, PreparationSuperseded, fencedDelay }
