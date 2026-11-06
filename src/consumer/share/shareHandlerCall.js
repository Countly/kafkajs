const { KafkaJSNonRetriableError, KafkaJSShareAcknowledgementError } = require('../../errors')

const TYPE_CODES = Object.freeze({ accept: 1, release: 2, reject: 3 })

/**
 * Orders two exact non-negative decimal offsets without parsing them.
 * @param {string} a First offset.
 * @param {string} b Second offset.
 * @returns {number} Negative, zero or positive, as a numeric comparison.
 */
const compareOffsets = (a, b) => a.length - b.length || (a < b ? -1 : a > b ? 1 : 0)

/**
 * Owns one handler invocation's provisional decisions and its original authority.
 *
 * It views `records[start, end)` of one acquisition's delivered records, which the walk keeps in
 * ascending offset order, so a message is found by binary search and decisions live in one byte
 * each. No Map, per-call result array or linear lookup exists. The public callbacks are closures
 * of this one invocation: sharing them across invocations would let a late old call authorize a
 * new acquisition, so each record (`eachMessage`) or batch (`eachBatch`) gets its own set.
 */
class ShareHandlerCall {
  /**
   * @param {object} options Original invocation context, captured before invoking user code.
   * @param {import('./shareAckLedger').ShareAckLedger} options.ledger Original acknowledgement ledger.
   * @param {object[]} options.records The acquisition's delivered records, ascending by offset.
   * @param {number} [options.start=0] First record of this call.
   * @param {number} [options.end=records.length] One past the last record of this call.
   * @param {'implicit'|'explicit'} options.mode Acknowledgement mode.
   * @param {boolean} options.batch Whether this is `eachBatch`.
   * @param {object} options.authority The acquisition's original authority, shared by its calls:
   *   `current()` (run, membership, session and acquisition), `running()` (run and membership,
   *   not stopped), `stopping()` (a clean stop was requested) and `renew(offsets)` (serialized
   *   renewal through the node FIFO, resolving with the lock duration).
   * @returns {ShareHandlerCall} A live invocation with no timers.
   */
  constructor({ ledger, records, start = 0, end = records.length, mode, batch, authority }) {
    this.ledger = ledger
    this.records = records
    this.start = start
    this.end = end
    this.mode = mode
    this.batch = batch
    this.authority = authority
    this.open = true
    this.marks = null
    this.decided = 0
  }

  /** @returns {number} How many records this call covers. */
  get size() {
    return this.end - this.start
  }

  /**
   * Builds the public payload of `eachMessage`: the sole message and callbacks of this call.
   * @param {string} topic Topic name.
   * @param {number} partition Partition index.
   * @returns {object} `{ topic, partition, message, acknowledge, renew, isRunning }`.
   */
  messagePayload(topic, partition) {
    return {
      topic,
      partition,
      message: this.records[this.start],
      acknowledge: (type) => this.acknowledge(this.records[this.start], type),
      renew: () => this.renew(),
      isRunning: () => this.isRunning(),
    }
  }

  /**
   * Builds the public payload of `eachBatch`: one topic/partition/acquisition and its callbacks.
   * @param {object} batch The batch handed to the handler.
   * @returns {object} `{ batch, acknowledge, renew, isRunning }`.
   */
  batchPayload(batch) {
    return {
      batch,
      acknowledge: (message, type) => this.acknowledge(message, type),
      renew: (messages) => this.renew(messages),
      isRunning: () => this.isRunning(),
    }
  }

  /**
   * Rejects calls made after return or after invalidation, before any ledger or wire change.
   * @returns {void}
   * @throws {KafkaJSNonRetriableError} If this original handler is no longer authoritative.
   */
  check() {
    if (!this.open || !this.authority.current())
      throw new KafkaJSNonRetriableError('Handler call is no longer current')
  }

  /**
   * Resolves an exact local message or the supported offset-only selector. A full message of
   * another call that has an equal offset stays foreign.
   * @param {object} message Delivered message or an object containing only `offset`.
   * @returns {number} Index of the message within this call's records.
   * @throws {KafkaJSNonRetriableError} For malformed or foreign selectors.
   */
  indexOf(message) {
    const offset = message?.offset
    const found = typeof offset === 'string' ? this.search(offset) : -1
    const local = found === -1 ? undefined : this.records[found]
    const offsetOnly =
      local !== undefined &&
      message !== local &&
      Object.keys(message).length === 1 &&
      Object.hasOwn(message, 'offset')
    if (local === undefined || (message !== local && !offsetOnly)) {
      throw new KafkaJSNonRetriableError('Message does not belong to this handler call')
    }
    return found
  }

  /**
   * Binary search by exact decimal offset.
   * @private
   * @param {string} offset The offset to find.
   * @returns {number} Its index within `[start, end)`, or -1.
   */
  search(offset) {
    let low = this.start
    let high = this.end - 1
    while (low <= high) {
      const middle = (low + high) >>> 1
      const order = compareOffsets(this.records[middle].offset, offset)
      if (order === 0) return middle
      if (order < 0) low = middle + 1
      else high = middle - 1
    }
    return -1
  }

  /**
   * Makes a provisional terminal decision; repeated decisions replace the earlier one.
   * @param {object} message Local message or offset-only selector.
   * @param {'accept'|'release'|'reject'} [type='accept'] Terminal type, never renew.
   * @returns {void}
   * @throws {KafkaJSNonRetriableError} For a stale call, foreign message or invalid type.
   */
  acknowledge(message, type = 'accept') {
    this.check()
    const index = this.indexOf(message)
    this.ledger.decide(this.records[index].offset, type)
    this.marks ??= new Uint8Array(this.size)
    const mark = index - this.start
    if (this.marks[mark] === 0) this.decided++
    this.marks[mark] = TYPE_CODES[type]
  }

  /**
   * Renews the selected current held messages through the node FIFO.
   * @param {object[]} [messages] Local selectors; omission means every message of this call.
   * @returns {Promise<number>} Broker-applied lock duration, without terminal settlement.
   * @throws {KafkaJSNonRetriableError} For invalid local selectors or stale authority; nothing is
   *   queued or sent in that case.
   */
  renew(messages) {
    this.check()
    let offsets
    if (messages === undefined) {
      offsets = this.offsets()
    } else {
      if (!Array.isArray(messages) || messages.length === 0) {
        throw new KafkaJSNonRetriableError('renew requires a non-empty array of local messages')
      }
      offsets = messages.map((message) => this.records[this.indexOf(message)].offset)
      if (new Set(offsets).size !== offsets.length) {
        throw new KafkaJSNonRetriableError('renew messages must be distinct')
      }
    }
    return this.authority.renew(offsets)
  }

  /**
   * Tests run and membership health; a data-session reset alone does not stop the run.
   * @returns {boolean} False after stop or loss of this call's run/membership.
   */
  isRunning() {
    return this.authority.running()
  }

  /**
   * @returns {string[]} The offsets of every message of this call, ascending.
   */
  offsets() {
    const offsets = []
    for (let i = this.start; i < this.end; i++) offsets.push(this.records[i].offset)
    return offsets
  }

  /**
   * Offsets of the messages that have a local decision.
   * @private
   * @param {boolean} decided True for decided messages, false for undecided ones.
   * @returns {string[]} Their offsets, ascending.
   */
  offsetsWhere(decided) {
    const offsets = []
    for (let i = this.start; i < this.end; i++) {
      if ((this.marks !== null && this.marks[i - this.start] !== 0) === decided)
        offsets.push(this.records[i].offset)
    }
    return offsets
  }

  /**
   * Finalizes only an original current return. A fenced return makes no decision.
   * @param {boolean} [failed=false] Whether the handler threw, including a non-Error value.
   * @returns {KafkaJSShareAcknowledgementError|null} The explicit-omission verdict, after the
   *   omitted messages were released; null for a throw, a stop and every non-omission.
   */
  complete(failed = false) {
    if (!this.open) return null
    this.open = false
    if (!this.authority.current()) return null
    const stopping = this.authority.stopping()
    const release = failed || (this.batch && stopping) || this.mode === 'explicit'
    this.ledger.complete(this.offsets(), release ? 'release' : 'accept')
    if (failed || stopping || this.mode !== 'explicit' || this.decided === this.size) return null
    return this.omission()
  }

  /**
   * @private
   * @returns {KafkaJSShareAcknowledgementError} The typed verdict naming the omitted offsets.
   */
  omission() {
    const ranges = []
    let previous = null
    for (const offset of this.offsetsWhere(false)) {
      if (previous !== null && BigInt(offset) === BigInt(previous.lastOffset) + 1n)
        previous.lastOffset = offset
      else ranges.push((previous = { firstOffset: offset, lastOffset: offset }))
    }
    return new KafkaJSShareAcknowledgementError(
      'Handler returned without acknowledging every message',
      {
        reason: 'unacknowledged',
        topic: this.ledger.topic,
        partition: this.ledger.partition,
        ranges,
      }
    )
  }

  /**
   * Promotes only the explicit choices of a handler that is still running at a forced close, so
   * undecided work is lost, not accepted. The call stops being authoritative.
   * @returns {void}
   */
  freeze() {
    if (!this.open) return
    this.open = false
    const offsets = this.offsetsWhere(true)
    if (offsets.length) this.ledger.complete(offsets, 'release')
  }
}

module.exports = { ShareHandlerCall }
