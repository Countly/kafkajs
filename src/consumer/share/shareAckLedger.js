const { KafkaJSNonRetriableError, KafkaJSShareAcknowledgementError } = require('../../errors')
const { parseOffset, appendRange } = require('./shareRecordWalk')
const TYPES = Object.freeze({ gap: 0, accept: 1, release: 2, reject: 3, renew: 4 })
const TYPE_NAMES = Object.keys(TYPES)
const FAILURES = new Set([
  'unacknowledged',
  'session-lost',
  'membership-lost',
  'not-sent',
  'rejected-by-broker',
])

/**
 * Computes a conservative monotonic deadline from a pre-write timestamp, never response time.
 * @param {number} sentAt Monotonic milliseconds captured before the request write.
 * @param {number} duration Nonnegative int32 lock duration in milliseconds.
 * @returns {number} Acquisition deadline; no timer is created.
 * @throws {KafkaJSNonRetriableError} For invalid clock/duration arithmetic.
 */
const lockDeadline = (sentAt, duration) => {
  if (
    !Number.isFinite(sentAt) ||
    sentAt < 0 ||
    !Number.isInteger(duration) ||
    duration < 0 ||
    duration > 2147483647 ||
    sentAt + duration > Number.MAX_SAFE_INTEGER
  ) {
    throw new KafkaJSNonRetriableError('Invalid acquisition lock deadline')
  }
  return sentAt + duration
}

/**
 * Transcribes broker acknowledgement validation, including its permitted touching boundary.
 * @param {object[]} batches firstOffset/lastOffset and acknowledgeTypes int8 array.
 * @param {object} [options] Request kind.
 * @param {boolean} [options.isRenewAck=false] Allows type 4; types 0..3 remain legal.
 * @returns {void} Success is silent; this does not send or mutate batches.
 * @throws {KafkaJSNonRetriableError} Before building any broker-invalid request.
 */
const validateAcknowledgements = (batches, { isRenewAck = false } = {}) => {
  if (typeof isRenewAck !== 'boolean') throw new KafkaJSNonRetriableError('Invalid renew flag')
  let previous = -1n
  for (const { firstOffset, lastOffset, acknowledgeTypes } of batches) {
    const first = parseOffset(firstOffset)
    const last = parseOffset(lastOffset)
    if (
      first > last ||
      first < previous ||
      !Array.isArray(acknowledgeTypes) ||
      acknowledgeTypes.length === 0 ||
      (acknowledgeTypes.length !== 1 && BigInt(acknowledgeTypes.length) !== last - first + 1n)
    ) {
      throw new KafkaJSNonRetriableError('Invalid acknowledgement batch bounds or type count')
    }
    for (const type of acknowledgeTypes) {
      if (!Number.isInteger(type) || type < 0 || type > (isRenewAck ? 4 : 3)) {
        throw new KafkaJSNonRetriableError('Invalid acknowledgement type')
      }
    }
    previous = last
  }
}

/**
 * Encodes sorted disjoint decisions. Automatic spans stay uniform; delivered singleton spans may
 * form mixed arrays. Uniform runs of eleven or more split out, matching the released Java boundary.
 * @param {object[]} entries Entries with bigint first/last, type id and automatic boolean.
 * @param {boolean} [isRenewAck=false] Encodes these held entries as type 4 without changing them.
 * @returns {object[]} Wire batches with decimal offsets; no per-absent-offset array is allocated.
 * @throws {KafkaJSNonRetriableError} For overlap, unbounded mixed spans or invalid wire output.
 */
const encodeAcknowledgements = (entries, isRenewAck = false) => {
  const output = []
  /** @param {object[]} rows Delivered singleton run. @returns {void} Emits bounded mixed pieces. */
  const emitDelivered = (rows) => {
    let start = 0
    let i = 0
    /** @param {number} first Index. @param {number} end Exclusive index. @param {boolean} uniform Collapse types. @returns {void} Appends a batch. */
    const emit = (first, end, uniform) => {
      if (first === end) return
      output.push({
        firstOffset: String(rows[first].first),
        lastOffset: String(rows[end - 1].last),
        acknowledgeTypes: uniform
          ? [isRenewAck ? 4 : rows[first].type]
          : rows.slice(first, end).map((e) => (isRenewAck ? 4 : e.type)),
      })
    }
    while (i < rows.length) {
      let end = i + 1
      while (end < rows.length) {
        if (!isRenewAck && rows[end].type !== rows[i].type) break
        end++
      }
      if ((i === 0 && end === rows.length) || end - i >= 11) {
        emit(start, i, false)
        emit(i, end, true)
        start = end
      }
      i = end
    }
    emit(start, rows.length, false)
  }
  let delivered = []
  let previous = -1n
  for (const entry of entries) {
    if (
      entry.first <= previous ||
      entry.first > entry.last ||
      (!entry.automatic && entry.first !== entry.last)
    ) {
      throw new KafkaJSNonRetriableError('Overlapping or unbounded mixed acknowledgement entries')
    }
    if (entry.automatic || entry.first !== previous + 1n) {
      emitDelivered(delivered)
      delivered = []
    }
    if (entry.automatic) {
      const prior = output[output.length - 1]
      const type = isRenewAck ? 4 : entry.type
      if (
        prior &&
        prior.automatic &&
        BigInt(prior.lastOffset) + 1n === entry.first &&
        prior.acknowledgeTypes[0] === type
      ) {
        prior.lastOffset = String(entry.last)
      } else
        output.push({
          firstOffset: String(entry.first),
          lastOffset: String(entry.last),
          acknowledgeTypes: [type],
          automatic: true,
        })
    } else delivered.push(entry)
    previous = entry.last
  }
  emitDelivered(delivered)
  for (const batch of output) delete batch.automatic
  validateAcknowledgements(output, { isRenewAck })
  return output
}

/**
 * Owns one partition/acquisition's compact decisions and single-use request tickets. Transport,
 * handler authority, error-code classification and event emission belong to callers. No retries,
 * timers, broker access or automatic acquisition of replacement authority occur in this class.
 */
class ShareAckLedger {
  #entries = new Map()
  #pending = new Map()
  #token
  #retired = false
  #flushEntry = null

  /**
   * @param {object} options Acquisition metadata and classified walk.
   * @param {string} options.topic Topic name, used only in outcomes.
   * @param {string} options.topicId Topic UUID, used in request grouping.
   * @param {number} options.partition Partition index.
   * @param {object|symbol} options.token Original opaque acquisition identity, compared by reference.
   * @param {number} options.sentAt Pre-write fetch time.
   * @param {number} options.lockDuration Response AcquisitionLockTimeoutMs.
   * @param {object} options.walk ShareRecordWalk output; `deliver.records[i]` pairs with `deliver.deliveryCounts[i]`,
   * and original record references are retained only while held.
   * @returns {ShareAckLedger} Ledger with held deliveries and already-decided automatic ranges.
   * @throws {KafkaJSNonRetriableError} For invalid locks, overlapping decisions or absent token.
   */
  constructor({ topic, topicId, partition, token, sentAt, lockDuration, walk }) {
    if (!token || !['object', 'symbol'].includes(typeof token))
      throw new KafkaJSNonRetriableError('Acquisition token must be opaque')
    this.topic = topic
    this.topicId = topicId
    this.partition = partition
    this.#token = token
    const deadline = lockDeadline(sentAt, lockDuration)
    const entries = []
    const { records, deliveryCounts } = walk.deliver
    for (let i = 0; i < records.length; i++) {
      const record = records[i]
      const first = parseOffset(record.offset)
      entries.push({
        first,
        last: first,
        record,
        deliveryCount: deliveryCounts[i],
        automatic: false,
        state: 'held',
        type: null,
        deadline,
        duration: lockDuration,
      })
    }
    for (const name of ['gaps', 'release', 'reject']) {
      for (const range of walk[name])
        entries.push({
          first: parseOffset(range.firstOffset),
          last: parseOffset(range.lastOffset),
          automatic: true,
          state: 'decided',
          type: TYPES[name === 'gaps' ? 'gap' : name],
          deadline,
          duration: lockDuration,
        })
    }
    entries.sort((a, b) => (a.first < b.first ? -1 : a.first > b.first ? 1 : 0))
    encodeAcknowledgements(entries.filter((e) => e.automatic))
    let previous = -1n
    for (const entry of entries) {
      if (entry.first <= previous || entry.first > entry.last)
        throw new KafkaJSNonRetriableError('Acquisition classifications overlap')
      this.#entries.set(String(entry.first), entry)
      if (entry.state === 'decided') this.#trackFlush(entry)
      previous = entry.last
    }
  }

  /** @returns {number} Retained entries, not the numeric width of absent ranges. */
  get size() {
    return this.#entries.size
  }

  /**
   * @param {string|bigint|number} offset Delivered offset.
   * @returns {object} Held delivered entry; never returns automatic range entries.
   * @throws {KafkaJSNonRetriableError} For a late, foreign, retired or already-completed decision.
   */
  #held(offset) {
    const entry = this.#entries.get(String(parseOffset(offset)))
    if (this.#retired || !entry || entry.automatic || entry.state !== 'held') {
      throw new KafkaJSNonRetriableError(
        'Acknowledgement is outside its active handler acquisition'
      )
    }
    return entry
  }

  /**
   * @param {string} offset Held record offset.
   * @param {'accept'|'release'|'reject'} [type='accept'] Last local choice before completion wins.
   * @returns {void} Saves a provisional choice; it is not eligible for a terminal request yet.
   * @throws {KafkaJSNonRetriableError} For invalid type or non-held offset.
   */
  decide(offset, type = 'accept') {
    if (!['accept', 'release', 'reject'].includes(type))
      throw new KafkaJSNonRetriableError('Invalid terminal acknowledgement')
    this.#held(offset).type = TYPES[type]
  }

  /**
   * Promotes only the completed handler's offsets. Callers enforce call identity before invoking.
   * @param {string[]} offsets Current handler offsets; duplicate inputs are harmless.
   * @param {'accept'|'release'} defaultType Runner-selected outcome for undecided records.
   * @returns {void} Makes choices eligible together; validates the whole selection before mutation.
   * @throws {KafkaJSNonRetriableError} For invalid default, foreign or no-longer-held entries.
   */
  complete(offsets, defaultType) {
    if (!['accept', 'release'].includes(defaultType))
      throw new KafkaJSNonRetriableError('Invalid default acknowledgement')
    const entries = [...new Set(offsets)].map((offset) => this.#held(offset))
    for (const entry of entries) this.#promote(entry, defaultType)
  }

  /**
   * Same promotion as {@link ShareAckLedger#complete} for the delivered records
   * `records[start, end)`, read in place so a caller on a hot path builds no offset array.
   * @param {Array<{offset: string}>} records The acquisition's delivered records, ascending.
   * @param {number} start First record of the selection.
   * @param {number} end One past the last record of the selection.
   * @param {'accept'|'release'} defaultType Runner-selected outcome for undecided records.
   * @returns {void} Validates the whole selection before any entry changes.
   * @throws {KafkaJSNonRetriableError} For an invalid default or a foreign, retired or
   *   already-completed record.
   */
  completeRange(records, start, end, defaultType) {
    if (defaultType !== 'accept' && defaultType !== 'release')
      throw new KafkaJSNonRetriableError('Invalid default acknowledgement')
    for (let i = start; i < end; i++) this.#held(records[i].offset)
    for (let i = start; i < end; i++) this.#promote(this.#held(records[i].offset), defaultType)
  }

  /**
   * @param {object} entry A held delivered entry.
   * @param {'accept'|'release'} defaultType Outcome used when the entry has no local choice.
   * @returns {void} Makes the entry eligible for a terminal request.
   */
  #promote(entry, defaultType) {
    entry.type = entry.type ?? TYPES[defaultType]
    entry.state = 'decided'
    this.#trackFlush(entry)
  }

  /**
   * Tracks the earliest unsent decided deadline; equal deadlines use the earlier half-lock mark.
   * @param {object} entry Newly decided entry, with deadline and duration columns.
   * @returns {void} Retains one entry reference without a per-handler ledger scan.
   */
  #trackFlush(entry) {
    const earliest = this.#flushEntry
    if (
      !earliest ||
      entry.deadline < earliest.deadline ||
      (entry.deadline === earliest.deadline && entry.duration > earliest.duration)
    ) {
      this.#flushEntry = entry
    }
  }

  /**
   * Reads start eligibility using the conservative deadline. It never gates terminal sending.
   * @param {string} offset Held delivered offset.
   * @param {number} now Monotonic milliseconds.
   * @returns {boolean} True strictly before its deadline; false exactly at/after it.
   * @throws {KafkaJSNonRetriableError} For a non-held offset or invalid clock.
   */
  canStart(offset, now) {
    if (!Number.isFinite(now) || now < 0)
      throw new KafkaJSNonRetriableError('Invalid monotonic clock')
    return now < this.#held(offset).deadline
  }

  /**
   * @param {string[]} offsets Unstarted current-handler candidates only.
   * @param {number} now Monotonic milliseconds.
   * @returns {object[]} Compact expired intervals, removed without acknowledgements; caller emits one before-handler event.
   * @throws {KafkaJSNonRetriableError} For non-held offsets or invalid clock, before any mutation.
   */
  dropExpired(offsets, now) {
    const entries = [...new Set(offsets)].map((offset) => this.#held(offset))
    const expired = entries.filter((entry) => !this.canStart(String(entry.first), now))
    const ranges = []
    expired.sort((a, b) => (a.first < b.first ? -1 : a.first > b.first ? 1 : 0))
    for (const entry of expired) {
      appendRange(ranges, entry.first, entry.last)
      this.#entries.delete(String(entry.first))
    }
    return ranges
  }

  /**
   * @param {number} now Monotonic handler-start/return time.
   * @returns {boolean} Cached O(1) comparison: the earliest decided deadline is less than half its lock duration away.
   * @throws {KafkaJSNonRetriableError} For an invalid clock.
   */
  needsFlush(now) {
    if (!Number.isFinite(now) || now < 0)
      throw new KafkaJSNonRetriableError('Invalid monotonic clock')
    const entry = this.#flushEntry
    return entry !== null && entry.deadline - now < entry.duration / 2
  }

  /**
   * Freezes one partition/acquisition request. A stale token retires all entries without a ticket.
   * An overdue terminal acknowledgement remains eligible; only starting a handler is lock-gated.
   * @param {object} options Captured original identity and request kind.
   * @param {object|symbol} options.token Original identity, never a rebuilt equivalent.
   * @param {string[]} [options.renewOffsets] Held offsets to renew; absent means terminal decisions.
   * @returns {{ticket:object|null,results:object[]}} Ticket is opaque apart from immutable wire fields.
   * @throws {KafkaJSNonRetriableError} For conflicting renewals or malformed selected entries.
   */
  prepare({ token, renewOffsets }) {
    if (this.#retired) return { ticket: null, results: [] }
    if (token !== this.#token) return { ticket: null, results: this.invalidate('session-lost') }
    const isRenewAck = renewOffsets !== undefined
    const entries = isRenewAck
      ? [...new Set(renewOffsets)].map((offset) => this.#held(offset))
      : [...this.#entries.values()].filter((e) => e.state === 'decided')
    if (entries.some((e) => e.renewing))
      throw new KafkaJSNonRetriableError('Renewal already pending')
    if (!entries.length) return { ticket: null, results: [] }
    if (isRenewAck) entries.sort((a, b) => (a.first < b.first ? -1 : a.first > b.first ? 1 : 0))
    const batches = encodeAcknowledgements(entries, isRenewAck)
    for (const batch of batches) {
      Object.freeze(batch.acknowledgeTypes)
      Object.freeze(batch)
    }
    const ticket = Object.freeze({ acknowledgementBatches: Object.freeze(batches), isRenewAck })
    this.#pending.set(ticket, { entries, isRenewAck })
    for (const entry of entries) {
      if (isRenewAck) entry.renewing = true
      else entry.state = 'sent'
    }
    if (!isRenewAck) this.#flushEntry = null
    return { ticket, results: [] }
  }

  /**
   * Converts a homogeneous outcome to compact range results. Failure never means accepted.
   * @param {object[]} entries Selected entries, ascending.
   * @param {boolean} isRenewAck Nonterminal type 4 when true.
   * @param {string} result 'ok' or a declared failure reason.
   * @param {Error} [cause] Broker or local failure.
   * @returns {object[]} Homogeneous exact range outcomes; one compact error per type group.
   */
  #outcomes(entries, isRenewAck, result, cause) {
    const groups = new Map()
    const output = []
    for (const entry of entries) {
      const type = isRenewAck ? 4 : (entry.type ?? TYPES.release)
      if (!groups.has(type)) groups.set(type, [])
      appendRange(groups.get(type), entry.first, entry.last)
    }
    for (const [type, ranges] of groups) {
      const error =
        result === 'ok'
          ? null
          : new KafkaJSShareAcknowledgementError('Acknowledgement was not confirmed', {
              reason: result,
              topic: this.topic,
              partition: this.partition,
              ranges,
              cause,
            })
      for (const range of ranges) output.push({ ...range, type: TYPE_NAMES[type], error })
    }
    return output.sort((a, b) =>
      BigInt(a.firstOffset) < BigInt(b.firstOffset)
        ? -1
        : BigInt(a.firstOffset) > BigInt(b.firstOffset)
          ? 1
          : 0
    )
  }

  /**
   * Consumes a request ticket once. An omitted partition is a terminal invalid-response failure.
   * Renewal success changes only selected deadlines/durations; failure changes no decision/deadline.
   * @param {object} ticket Original ticket from prepare(). Duplicate or obsolete tickets are ignored.
   * @param {object} options Already-classified response, never inferred from a fetch error alone.
   * @param {object|symbol} options.token Original acquisition identity.
   * @param {string} [options.result='ok'] 'ok' or a declared failure reason; unknown remote outcome uses session-lost.
   * @param {boolean} [options.present=true] Whether the acknowledged partition appeared in the answer.
   * @param {Error} [options.cause] Exact broker/transport failure.
   * @param {number} [options.sentAt] Renewal pre-write time, required only on success.
   * @param {number} [options.lockDuration] New duration, absent preserves each selected previous duration.
   * @returns {object[]} Compact results; caller emits terminal events and rejects failed renew calls.
   * @throws {KafkaJSNonRetriableError} For invalid result or successful renewal arithmetic; no partial update.
   */
  settle(ticket, { token, result = 'ok', present = true, cause, sentAt, lockDuration } = {}) {
    const pending = this.#pending.get(ticket)
    if (!pending) return []
    if (token !== this.#token) return this.invalidate('session-lost')
    if (!present) {
      result = 'rejected-by-broker'
      cause = cause || new KafkaJSNonRetriableError('Acknowledgement response omitted a partition')
    }
    if (result !== 'ok' && !FAILURES.has(result))
      throw new KafkaJSNonRetriableError('Invalid acknowledgement outcome')
    const { entries, isRenewAck } = pending
    if (isRenewAck && result === 'ok') {
      for (const entry of entries) lockDeadline(sentAt, lockDuration ?? entry.duration)
      for (const entry of entries) {
        entry.deadline = lockDeadline(sentAt, lockDuration ?? entry.duration)
        entry.duration = lockDuration ?? entry.duration
      }
      this.#flushEntry = null
      for (const entry of this.#entries.values()) {
        if (entry.state === 'decided') this.#trackFlush(entry)
      }
    }
    this.#pending.delete(ticket)
    for (const entry of entries) {
      if (isRenewAck) delete entry.renewing
      else this.#entries.delete(String(entry.first))
    }
    return this.#outcomes(entries, isRenewAck, result, cause)
  }

  /**
   * Retires ordinary authority and settles everything except an optional captured terminal ticket.
   * The runner prepares that ticket synchronously at its closing boundary, then passes the original
   * ticket and separate closing predicate to the session. Retirement never authorizes a write.
   * @param {'session-lost'|'membership-lost'|'not-sent'} reason Loss classification.
   * @param {object|null} [preserveTicket=null] Already-prepared terminal ticket owned by closing.
   * @returns {object[]} Failed compact outcomes once; preserved ticket remains settleable once.
   * @throws {KafkaJSNonRetriableError} For invalid reason or a foreign/nonterminal preserved ticket.
   */
  invalidate(reason, preserveTicket = null) {
    if (!['session-lost', 'membership-lost', 'not-sent'].includes(reason))
      throw new KafkaJSNonRetriableError('Invalid retirement reason')
    const keep = preserveTicket && this.#pending.get(preserveTicket)
    if (preserveTicket && (!keep || keep.isRenewAck))
      throw new KafkaJSNonRetriableError('Invalid captured terminal ticket')
    const preserved = new Set(keep ? keep.entries : [])
    const entries = [...this.#entries.values()].filter((e) => !preserved.has(e))
    const results = []
    for (const [ticket, pending] of this.#pending) {
      if (ticket === preserveTicket) continue
      if (pending.isRenewAck)
        for (const outcome of this.#outcomes(pending.entries, true, reason)) results.push(outcome)
      this.#pending.delete(ticket)
    }
    for (const outcome of this.#outcomes(entries, false, reason)) results.push(outcome)
    for (const entry of entries) this.#entries.delete(String(entry.first))
    this.#retired = true
    this.#flushEntry = null
    return results
  }

  /**
   * Groups prepared tickets for a single session request, refusing multiple acquisitions per partition.
   * @param {{ledger:ShareAckLedger,ticket:object}[]} prepared Current prepared terminal or renew tickets.
   * @returns {object[]} Wire Topics; session caller separately guards node, membership and generation.
   * @throws {KafkaJSNonRetriableError} For foreign tickets, mixed request kinds or duplicate partitions.
   */
  static requestTopics(prepared) {
    const topics = new Map()
    const seen = new Set()
    let kind
    for (const { ledger, ticket } of prepared) {
      if (!ledger.#pending.has(ticket) || (kind !== undefined && kind !== ticket.isRenewAck))
        throw new KafkaJSNonRetriableError('Invalid acknowledgement ticket group')
      kind = ticket.isRenewAck
      const key = `${ledger.topicId}:${ledger.partition}`
      if (seen.has(key))
        throw new KafkaJSNonRetriableError('One acquisition per partition per request is required')
      seen.add(key)
      if (!topics.has(ledger.topicId))
        topics.set(ledger.topicId, { topicId: ledger.topicId, partitions: [] })
      topics.get(ledger.topicId).partitions.push({
        partitionIndex: ledger.partition,
        acknowledgementBatches: ticket.acknowledgementBatches,
      })
    }
    return [...topics.values()]
  }
}

module.exports = { ShareAckLedger, encodeAcknowledgements, validateAcknowledgements, lockDeadline }
