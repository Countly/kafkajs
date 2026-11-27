const { KafkaJSPartialMessageError } = require('../errors')

/** The longest a member waits for a usable assignment, whatever its `maxPollInterval`. */
const MAX_WAIT_MS = 5 * 60 * 1000

const NONE = 'none'
const VERIFYING = 'verifying'
const WAITING = 'waiting'
const ENDED = 'ended'

/**
 * One run's episode of SyncGroup assignments this classic member cannot use, and the record of
 * how the run ends when such answers stop it.
 *
 * - `none` -> `verifying`: the first refused assignment opens the episode, logs an error and starts
 *   its one deadline, `min(maxPollInterval, 5 minutes)`, on Node's monotonic timer clock. The caller
 *   then asks once more in the same generation.
 * - `verifying` -> `waiting`: that answer was refused as well, or none came. The member holds no
 *   partitions, keeps heartbeating and joins only the rebalances the coordinator starts; each
 *   refused generation logs an error. No rebalance, new generation or transport error resets it.
 * - `verifying` | `waiting` -> `none`: a usable assignment was installed.
 * - any -> `ended`: the deadline passed (`onExpired` gets a non-retriable
 *   `KafkaJSPartialMessageError`), a join ended on a refused JoinGroup answer, the leader's own
 *   assigner wrote an unusable assignment (`end`), or the run stopped (`close`). An episode ended
 *   by an error makes the run's stop leave the group, a static member included.
 */
class RefusalEpisode {
  /**
   * @param {object} options
   * @param {number} options.maxPollInterval the run's poll deadline, in milliseconds
   * @param {string} options.groupId
   * @param {import('../../types').Logger} options.logger
   * @param {(error: Error) => void} options.onExpired called once when the deadline passes
   */
  constructor({ maxPollInterval, groupId, logger, onExpired }) {
    this.waitMs = Math.min(maxPollInterval, MAX_WAIT_MS)
    this.groupId = groupId
    this.logger = logger
    this.onExpired = onExpired
    this.state = NONE
    this.timer = null
    /** @type {{ memberId: string|null, generationId: number|null, reason: string }|null} */
    this.last = null
    /** @type {Error|null} the error that ended the run, null while it has not */
    this.error = null
  }

  /** @returns {boolean} whether the member is waiting, holding nothing, for a usable assignment */
  get open() {
    return this.state === VERIFYING || this.state === WAITING
  }

  /**
   * @returns {boolean} whether no handler can owe the run a poll: the member waits holding nothing,
   *   or the run is ending on this episode's error
   */
  get suspendsPollDeadline() {
    return this.open || this.error !== null
  }

  /**
   * Records one refused assignment.
   * @param {Error} refusal what `readSyncAssignment` refused
   * @param {{ memberId: string|null, generationId: number|null }} identity the membership the
   *   assignment was answered to
   * @returns {'verify'|'wait'} `verify` for the first refusal of the episode: ask once more in the
   *   same generation; `wait` otherwise
   */
  refuse(refusal, { memberId, generationId }) {
    if (this.state === ENDED) return 'wait'
    const opening = this.state === NONE
    const newGeneration = !opening && generationId !== this.last.generationId
    this.last = { memberId, generationId, reason: refusal.message }
    if (opening || newGeneration) {
      this.logger.error('Refused an assignment this member cannot use; it holds no partitions', {
        groupId: this.groupId,
        memberId,
        generationId,
        error: refusal.message,
        waitMs: this.waitMs,
      })
    }
    if (!opening) {
      this.state = WAITING
      return 'wait'
    }
    this.state = VERIFYING
    this.timer = setTimeout(() => this.expire(), this.waitMs)
    return 'verify'
  }

  /** A usable assignment was installed: the episode is over. @returns {void} */
  installed() {
    if (!this.open) return
    clearTimeout(this.timer)
    this.timer = null
    this.state = NONE
    this.last = null
  }

  /**
   * Ends the run's episode with the error the run stops with; the first end wins.
   * @param {Error} error
   * @returns {void}
   */
  end(error) {
    if (this.state === ENDED) return
    clearTimeout(this.timer)
    this.timer = null
    this.state = ENDED
    this.error = error
  }

  /** The run stopped: no deadline fires and no later end counts. @returns {void} */
  close() {
    if (this.state === ENDED) return
    clearTimeout(this.timer)
    this.timer = null
    this.state = ENDED
  }

  /**
   * @private
   * @returns {void}
   */
  expire() {
    if (!this.open) return
    const { memberId, reason } = this.last
    const error = new KafkaJSPartialMessageError(
      `Group ${this.groupId} member ${memberId}: no usable assignment within ${this.waitMs} ms of the first refused one: ${reason}`
    )
    this.end(error)
    this.onExpired(error)
  }
}

module.exports = RefusalEpisode
