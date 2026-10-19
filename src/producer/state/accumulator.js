const Long = require('../../utils/long')
const { KafkaJSInvalidConfigurationError } = require('../../errors')

/** What each field of a record must be for the record encoder to write it. */
const ENCODABLE = {
  key: 'a string, a Buffer or null',
  value: 'a string, a Buffer or null',
  headers: 'an object whose values are strings, Buffers or arrays of them',
  timestamp:
    'an integer from 0 to 2^63 - 1: a number, a numeric string, a Date, a BigInt or a Long',
}

/**
 * @param {*} value
 * @returns {boolean} whether the record encoder writes `value` as bytes
 */
const isBytes = (value) => typeof value === 'string' || Buffer.isBuffer(value)

/**
 * @param {*} headers
 * @returns {boolean} whether the record encoder can write `headers`: each value bytes, or an
 *   array of them
 */
const isHeaders = (headers) => headers !== null && Object.values(headers).flat().every(isBytes)

/** The largest int64, the upper bound of a timestamp. */
const MAX_TIMESTAMP = 2n ** 63n - 1n

/**
 * @param {*} timestamp
 * @returns {boolean} whether the record encoder can write `timestamp` as an int64: it converts it
 *   with `Long.fromValue` and subtracts it as a number. Its exact value, taken before anything can
 *   wrap it, must lie in `[0, 2^63 - 1]`; Java refuses a negative timestamp, and the bound keeps
 *   the spread of any batch inside an int64.
 */
const isTimestamp = (timestamp) => {
  try {
    const { value } = Long.fromValue(timestamp)
    return Number.isInteger(Number(timestamp)) && value >= 0n && value <= MAX_TIMESTAMP
  } catch {
    return false
  }
}

/**
 * The first field of a record the record encoder cannot write.
 * @param {object} message
 * @returns {keyof ENCODABLE|null} null when it can write every field
 */
const unencodableField = ({ key, value, headers, timestamp }) => {
  if (key != null && !isBytes(key)) {
    return 'key'
  }
  if (value != null && !isBytes(value)) {
    return 'value'
  }
  if (headers !== undefined && !isHeaders(headers)) {
    return 'headers'
  }
  if (timestamp != null && !isTimestamp(timestamp)) {
    return 'timestamp'
  }
  return null
}

/**
 * Refuses a send with a record the encoder cannot write, before any of its records joins a
 * batch: a coalesced batch, and the Produce request carrying it, fail as a whole when one record
 * cannot be encoded, which would fail every other send sharing them.
 * @param {string} topic
 * @param {object[]} messages
 * @returns {void}
 * @throws {KafkaJSInvalidConfigurationError} naming the topic and the field: the record has to be
 *   fixed
 */
const assertEncodable = (topic, messages) => {
  for (const message of messages) {
    const field = unencodableField(message)
    if (field !== null) {
      throw new KafkaJSInvalidConfigurationError(
        `Invalid message for topic "${topic}": its ${field} must be ${ENCODABLE[field]}`
      )
    }
  }
}

/**
 * Reads the list by index, at the length it had when the check began and with an absent entry
 * skipped, as `some` reads it: a record array's accessors may change the list meanwhile.
 * @param {{ messages: object[] }[]} topicMessages
 * @returns {boolean} whether any topic carries a record, as the arrays stand now
 */
const hasRecords = (topicMessages) => {
  for (let i = 0, count = topicMessages.length; i < count; i++) {
    if (i in topicMessages && topicMessages[i].messages.length > 0) {
      return true
    }
  }
  return false
}

/**
 * @param {object[]} messages
 * @returns {number} the bytes of their keys and values, the accumulator's estimate of a batch
 */
const sizeOf = (messages) => {
  let bytes = 0
  for (const { key, value } of messages) {
    if (key != null) {
      bytes += Buffer.byteLength(key)
    }
    if (value != null) {
      bytes += Buffer.byteLength(value)
    }
  }
  return bytes
}

/**
 * A record as its batch carries it: one with a `BigInt` timestamp is copied with that timestamp as
 * a `Long`. The record encoder subtracts a batch's timestamps from each other, and a `BigInt`
 * cannot be mixed with the numbers of the other sends' records.
 * @param {object} message
 * @returns {object}
 */
const withEncodableTimestamp = (message) =>
  typeof message.timestamp === 'bigint'
    ? { ...message, timestamp: Long.fromValue(message.timestamp) }
    : message

/**
 * A batch's `RecordMetadata` for the records a send placed at `index` in it: the base offset moves
 * on by `index`, unless it is unknown (-1, or absent).
 * @param {object} metadata
 * @param {number} index
 * @returns {object}
 */
const offsetBy = (metadata, index) =>
  index === 0 || !(Number(metadata.baseOffset) >= 0)
    ? metadata
    : { ...metadata, baseOffset: String(BigInt(metadata.baseOffset) + BigInt(index)) }

/**
 * A settler that calls `settle` once, however its batch ends.
 * @param {() => void} settle
 * @returns {{ resolve: () => void, reject: () => void }}
 */
const settleOnce = (settle) => {
  let settled = false
  const once = () => {
    if (!settled) {
      settled = true
      settle()
    }
  }
  return { resolve: once, reject: once }
}

/** Sorts `RecordMetadata` by topic name, then partition. */
const byTopicThenPartition = (a, b) =>
  a.topicName < b.topicName ? -1 : a.topicName > b.topicName ? 1 : a.partition - b.partition

/**
 * One `append()`: settles once every partition its records went to has settled, resolving with
 * their `RecordMetadata` by topic, then partition, or rejecting with the first error any of them
 * failed with.
 */
class AppendedSend {
  #pending
  #metadata = []
  #error = null
  #resolve
  #reject

  /** @param {number} count the partitions its records went to */
  constructor(count) {
    this.#pending = count
    this.promise = new Promise((resolve, reject) => {
      this.#resolve = resolve
      this.#reject = reject
    })
  }

  /**
   * One partition delivered.
   * @param {object|undefined} metadata undefined for an `acks: 0` send, which gets no answer
   * @returns {void}
   */
  delivered(metadata) {
    if (metadata !== undefined) {
      this.#metadata.push(metadata)
    }
    this.#settleOne()
  }

  /**
   * One partition failed.
   * @param {Error} error
   * @returns {void}
   */
  failed(error) {
    this.#error = this.#error || error
    this.#settleOne()
  }

  /** @returns {void} */
  #settleOne() {
    this.#pending -= 1
    if (this.#pending > 0) {
      return
    }

    if (this.#error !== null) {
      this.#reject(this.#error)
    } else {
      this.#resolve(this.#metadata.sort(byTopicThenPartition))
    }
  }
}

/**
 * The settler one append's records carry in a batch: it settles that append's share once, and its
 * `RecordMetadata` names the offset of the append's own first record, as the Java client offsets
 * each record by its index in the batch.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/RecordMetadata.java
 */
class Settler {
  #send
  #index
  #settled = false

  /**
   * @param {AppendedSend} send
   * @param {number} index the position of the append's first record in the batch
   */
  constructor(send, index) {
    this.#send = send
    this.#index = index
  }

  /**
   * @param {object|undefined} metadata the batch's `RecordMetadata`
   * @returns {void}
   */
  resolve(metadata) {
    if (this.#settled) {
      return
    }
    this.#settled = true
    this.#send.delivered(metadata === undefined ? undefined : offsetBy(metadata, this.#index))
  }

  /**
   * @param {Error} error
   * @returns {void}
   */
  reject(error) {
    if (this.#settled) {
      return
    }
    this.#settled = true
    this.#send.failed(error)
  }
}

/**
 * @typedef {object} PendingBatch
 * @property {string} key the batch's topic and partition
 * @property {string} options its `acks`, `timeout` and `compression`, as one key
 * @property {string} topic
 * @property {number} partition
 * @property {number} acks
 * @property {number} [timeout]
 * @property {number} [compression]
 * @property {object[]} messages
 * @property {Settler[]} settlers one per append whose records it carries, in arrival order
 * @property {number} bytes its keys' and values' bytes
 * @property {number} createdAt when the earliest of those appends began: its delivery deadline
 *   counts from there
 * @property {number} dueAt when its `linger` runs out
 * @property {string|null} mergeKey the node that led its partition when its `linger` timer was
 *   armed, with its `options`, as one key; null when the node was not known. It only decides which
 *   batches leave together
 * @property {ReturnType<typeof setTimeout>|null} timer
 * @property {boolean} due whether its `linger` has run out
 */

/**
 * The accumulator every send goes through (`linger`, `batchSize`): coalesces the records of
 * several sends into one batch per topic-partition, in the order they reach it, and hands each
 * batch to the send path as it flushes. A batch is due `linger` ms after its first records
 * arrived - at once, with no timer, when `linger` is 0; it goes out at once when it reaches
 * `batchSize` bytes of keys and values, and before records that would take it past `batchSize`,
 * or whose `acks`, `timeout` or `compression` differ, start the next one; records of one send are
 * never split, so a send larger than `batchSize` goes out on its own. The batches one send fills
 * go out together, and so do batches that are due when one of their timers fires, or in the same
 * tick when `linger` is 0, so partitions sent together can share a request. While `holds()` is
 * true - one batch in flight per partition, on an idempotent producer - a due batch whose
 * partition still has a batch handed on and not settled stays open for more records, up to
 * `batchSize`, and goes out when that batch settles: as the Java client's accumulator keeps its
 * last batch open while the sender skips a partition with a batch in flight. A held batch goes out
 * at its delivery deadline at the latest, so the send path expires it then, as the Java client
 * expires the head of a waiting partition's queue. While `lingers()` is false - a transaction is
 * ending, the producer is disconnecting - a batch goes out as soon as it forms. When a batch's
 * `linger` timer fires, every other batch whose `linger` has run out by then goes out with it. So
 * does every other pending batch with the same `acks`, `timeout` and `compression` for a partition
 * led by the node that led the partition of one of those batches when its own timer was armed.
 * Only a firing timer pulls batches that are not yet due: a full batch leaves with the batches
 * already due, `linger: 0` sends in the same tick, and `flush()` hands every batch on. The Java
 * client readies a node on more triggers (a full batch, a flush, a transaction that is completing)
 * and stops draining a node at `max.request.size`, 1 MB by default; this client has no such cap,
 * so one hand-off can carry every batch a node leads. `linger` is when a batch becomes due, not a
 * bound on when its send settles. A held batch neither goes out that way nor makes its node ready:
 * holding a partition behind its batch in flight is this client's rule (one unacknowledged batch
 * per partition), stricter than the Java default. Each pending batch holds at most one timer,
 * cleared when it flushes.
 * `close()` hands everything on and waits until nothing is left to deliver.
 * One accumulator serves one transactional context: a producer's own sends, or its transactions'.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/RecordAccumulator.java
 */
class Accumulator {
  #linger
  #batchSize
  #partition
  #sendPartitioned
  #lingers
  #holds
  #leaderOf
  #deadline
  /** @type {Map<string, PendingBatch>} */
  #pending = new Map()
  /** @type {Map<string, number>} by key, the batches handed on and not settled yet */
  #unsettled = new Map()
  /** Whether a hand-off of the due batches is queued */
  #handOffQueued = false
  /** How many appends are being assigned their partitions, not yet in a batch */
  #appending = 0
  /** @type {(() => void)[]} the `close()` calls waiting for nothing to be left to deliver */
  #closers = []

  /**
   * @param {object} options
   * @param {number} options.linger ms a batch waits for more records; 0 for none
   * @param {number} options.batchSize bytes of keys and values at which a batch is sent
   * @param {(topicMessages: { topic: string, messages: object[] }[]) =>
   *   Promise<{ topic: string, partition: number, messages: object[] }[]>} options.partition
   *   assigns every record its partition once
   * @param {(request: { acks: number, timeout?: number, compression?: number,
   *   batches: PendingBatch[] }) => Promise<object[]>} options.sendPartitioned sends the batches
   *   and settles each one's `settlers` as it settles; it must enqueue them before it returns, so
   *   that a flush is complete when `flush()` returns
   * @param {() => boolean} [options.lingers] whether a batch that forms now may wait for `linger`
   * @param {() => boolean} [options.holds] whether a due batch waits for its partition's batch in
   *   flight to settle
   * @param {(topic: string, partition: number) => number|null|undefined} [options.leaderOf]
   *   the node that leads a partition, read from what the client already knows and never fetched;
   *   asked once for each batch that waits for its `linger`. A negative id, null, undefined or a
   *   throw all mean the leader is not known, and a batch whose leader is not known is never
   *   merged
   * @param {number} [options.deadline] ms from a batch's first send to its delivery deadline, the
   *   send path's own; a held batch goes out when it is reached
   */
  constructor({
    linger,
    batchSize,
    partition,
    sendPartitioned,
    lingers = () => true,
    holds = () => false,
    leaderOf = () => null,
    deadline,
  }) {
    this.#linger = linger
    this.#batchSize = batchSize
    this.#partition = partition
    this.#sendPartitioned = sendPartitioned
    this.#lingers = lingers
    this.#holds = holds
    this.#leaderOf = leaderOf
    this.#deadline = deadline
  }

  /**
   * Adds a send's records to the pending batches of their partitions. Resolves once every batch
   * carrying them has been acknowledged, with one `RecordMetadata` per partition, by topic and then
   * partition, whose `baseOffset` is that of the send's own first record there; rejects with the
   * first error such a batch failed with, after all of them have settled. A send with no record
   * resolves `[]` at once.
   * @param {object} request
   * @param {number} request.acks
   * @param {number} [request.timeout]
   * @param {number} [request.compression]
   * @param {{ topic: string, messages: object[] }[]} request.topicMessages
   * @returns {Promise<object[]>}
   * @throws {KafkaJSInvalidConfigurationError} for a record the encoder cannot write; nothing is
   *   queued
   */
  async append({ acks, timeout, compression, topicMessages }) {
    for (const { topic, messages } of topicMessages) {
      assertEncodable(topic, messages)
    }
    if (!hasRecords(topicMessages)) {
      return []
    }

    const createdAt = Date.now()
    this.#appending += 1
    try {
      return this.#join(await this.#partition(topicMessages), {
        acks,
        timeout,
        compression,
        createdAt,
      })
    } finally {
      this.#appending -= 1
      this.#checkIdle()
    }
  }

  /**
   * Adds one append's records, assigned to their partitions, to the pending batches. When they
   * fill a batch, every batch carrying them goes on in one hand-off - after any older batch of a
   * partition that had to go first, in its own call - so one send usually makes one request per
   * broker. When none fills, its records that join a batch an earlier append began leave with
   * that batch, on its timer, so one send can still reach a broker in two requests.
   * @param {{ topic: string, partition: number, messages: object[] }[]} entries
   * @param {{ acks: number, timeout?: number, compression?: number, createdAt: number }} options
   * @returns {Promise<object[]>} the append's outcome, as `append()` describes it
   */
  #join(entries, { acks, timeout, compression, createdAt }) {
    const send = new AppendedSend(entries.length)
    const options = `${acks}/${timeout}/${compression}`
    // One due time for every batch this append creates, so they fall due together.
    const dueAt = Date.now() + this.#linger
    /** @type {PendingBatch[]} the batches this append fills, out of the pending ones */
    const full = []
    /** @type {PendingBatch[]} the other batches carrying its records, still pending */
    const joined = []
    for (const { topic, partition, messages } of entries) {
      const key = `${topic}/${partition}`
      const bytes = sizeOf(messages)
      let batch = this.#pending.get(key)
      if (
        batch !== undefined &&
        (batch.options !== options || batch.bytes + bytes > this.#batchSize)
      ) {
        this.#flushBatch(batch)
        batch = undefined
      }
      if (batch === undefined) {
        batch = {
          key,
          options,
          topic,
          partition,
          acks,
          timeout,
          compression,
          messages: [],
          settlers: [],
          bytes: 0,
          createdAt,
          dueAt,
          mergeKey: null,
          timer: null,
          due: false,
        }
        this.#pending.set(key, batch)
      }

      batch.createdAt = Math.min(batch.createdAt, createdAt)
      batch.settlers.push(new Settler(send, batch.messages.length))
      for (const message of messages) {
        batch.messages.push(withEncodableTimestamp(message))
      }
      batch.bytes += bytes

      if (batch.bytes >= this.#batchSize || !this.#lingers()) {
        clearTimeout(batch.timer)
        this.#pending.delete(key)
        full.push(batch)
      } else {
        joined.push(batch)
      }
    }

    const due = full.length > 0 || this.#linger === 0
    for (const batch of joined) {
      if (batch.due) {
        continue
      }
      if (due) {
        clearTimeout(batch.timer)
        batch.timer = null
        batch.due = true
      } else if (batch.timer === null) {
        batch.mergeKey = this.#mergeKeyOf(batch)
        batch.timer = setTimeout(() => this.#expire(batch), this.#linger)
      }
    }
    if (full.length > 0) {
      this.#handOnDue(full)
    } else if (this.#linger === 0) {
      this.#queueHandOff()
    }
    return send.promise
  }

  /**
   * Hands every pending batch to the send path now - in one call per `acks`, `timeout` and
   * `compression` - and clears their timers.
   * @returns {Promise<void>} settles once every batch it handed on has settled; never rejects
   */
  async flush() {
    const batches = [...this.#pending.values()]
    batches.forEach((batch) => clearTimeout(batch.timer))
    this.#pending.clear()
    await this.#sendGrouped(batches)
  }

  /**
   * Hands every pending batch on now, as `flush()` does, and settles once nothing is left to
   * deliver: no batch pending, none handed on and unsettled, and no append still being assigned
   * its partitions - so the batches handed on while it waits are delivered too. The caller stops
   * new appends and lingering first.
   * @returns {Promise<void>} never rejects
   */
  close() {
    this.flush()
    return new Promise((resolve) => {
      this.#closers.push(resolve)
      this.#checkIdle()
    })
  }

  /**
   * Settles the waiting `close()` calls once nothing is left to deliver.
   * @returns {void}
   */
  #checkIdle() {
    if (
      this.#closers.length > 0 &&
      this.#appending === 0 &&
      this.#pending.size === 0 &&
      this.#unsettled.size === 0
    ) {
      for (const resolve of this.#closers.splice(0)) {
        resolve()
      }
    }
  }

  /**
   * A batch's `linger` ran out: it goes out with every other batch due by now, and with every
   * batch that merges with one of them - the same node led its partition when its own timer was
   * armed, it asks for the same `acks`, `timeout` and `compression`, and it is not held. Their
   * timers are cleared. A held batch makes no broker ready, so it merges nothing.
   * @param {PendingBatch} expired
   * @returns {void}
   */
  #expire(expired) {
    // Timers run on the event loop's cached clock, which can lag `Date.now()`.
    const now = Math.max(Date.now(), expired.dueAt)
    /** @type {Set<string>} */
    const ready = new Set()
    for (const batch of this.#pending.values()) {
      if (batch === expired || (!batch.due && batch.dueAt <= now)) {
        this.#makeDue(batch)
        if (batch.mergeKey !== null && !this.#isHeld(batch)) {
          ready.add(batch.mergeKey)
        }
      }
    }
    if (ready.size > 0) {
      for (const batch of this.#pending.values()) {
        if (!batch.due && ready.has(batch.mergeKey) && !this.#isHeld(batch)) {
          this.#makeDue(batch)
        }
      }
    }
    this.#handOnDue()
  }

  /**
   * Marks a pending batch due and clears its timer.
   * @param {PendingBatch} batch
   * @returns {void}
   */
  #makeDue(batch) {
    clearTimeout(batch.timer)
    batch.timer = null
    batch.due = true
  }

  /**
   * The key a batch merges under: the node that leads its partition and the request options it
   * asks for, the two things batches must share to leave in one request.
   * @param {PendingBatch} batch
   * @returns {string|null} null when the node is not known: `leaderOf` answered with anything but
   *   a node id, or threw - this runs while a send is being appended, where a throw would lose the
   *   send
   */
  #mergeKeyOf({ topic, partition, options }) {
    try {
      const leader = this.#leaderOf(topic, partition)
      return Number.isInteger(leader) && leader >= 0 ? `${leader}:${options}` : null
    } catch {
      return null
    }
  }

  /**
   * Whether a batch waits for its partition's batch in flight to settle.
   * @param {PendingBatch} batch
   * @returns {boolean}
   */
  #isHeld(batch) {
    return this.#holds() && this.#unsettled.has(batch.key)
  }

  /**
   * Hands on every due batch that is not held for its partition's batch in flight, in one call
   * with `full` per options; a held one with a delivery deadline is timed to go out at it.
   * @param {PendingBatch[]} [full] batches already taken out of the pending ones, never held
   * @returns {void}
   */
  #handOnDue(full = []) {
    const ready = []
    for (const batch of this.#pending.values()) {
      if (!batch.due) {
        continue
      }
      if (!this.#isHeld(batch)) {
        ready.push(batch)
      } else if (this.#deadline !== undefined && batch.timer === null) {
        const remaining = batch.createdAt + this.#deadline - Date.now()
        batch.timer = setTimeout(() => this.#flushBatch(batch), Math.max(0, remaining))
      }
    }
    for (const batch of ready) {
      clearTimeout(batch.timer)
      this.#pending.delete(batch.key)
    }
    this.#sendGrouped(full.length === 0 ? ready : [...full, ...ready])
  }

  /**
   * A batch handed on has settled: its partition's due batch, if any, can go.
   * @param {string} key
   * @returns {void}
   */
  #settled(key) {
    const unsettled = this.#unsettled.get(key) - 1
    if (unsettled > 0) {
      this.#unsettled.set(key, unsettled)
      return
    }
    this.#unsettled.delete(key)
    if (this.#pending.get(key)?.due) {
      // Batches of one Produce response settle together: their partitions go out together.
      this.#queueHandOff()
    }
    this.#checkIdle()
  }

  /**
   * Hands on the due batches once the current microtasks have run, so the batches that fall due
   * in one tick go out together; one hand-off is queued at a time.
   * @returns {void}
   */
  #queueHandOff() {
    if (this.#handOffQueued) {
      return
    }
    this.#handOffQueued = true
    queueMicrotask(() => {
      this.#handOffQueued = false
      this.#handOnDue()
    })
  }

  /**
   * Sends batches in one call per `acks`, `timeout` and `compression`. Batches that share their
   * options - every hand-off of a send that asked for one set of them - go to the send path as
   * they are, with no grouping.
   * @param {PendingBatch[]} batches
   * @returns {Promise<unknown>} settles once they all have; never rejects, unless the send path
   *   throws before it answers, which rejects the promise the hand-off sites leave unawaited
   */
  #sendGrouped(batches) {
    try {
      if (batches.length === 0) {
        return Promise.resolve()
      }
      let shared = true
      for (let i = 1; i < batches.length && shared; i++) {
        shared = batches[i].options === batches[0].options
      }
      if (shared) {
        return this.#send(batches)
      }
      /** @type {Map<string, PendingBatch[]>} */
      const groups = new Map()
      for (const batch of batches) {
        const group = groups.get(batch.options)
        if (group === undefined) {
          groups.set(batch.options, [batch])
        } else {
          group.push(batch)
        }
      }
      return Promise.all(Array.from(groups.values(), (group) => this.#send(group)))
    } catch (error) {
      return Promise.reject(error)
    }
  }

  /**
   * @param {PendingBatch} batch
   * @returns {Promise<void>}
   */
  #flushBatch(batch) {
    clearTimeout(batch.timer)
    this.#pending.delete(batch.key)
    return this.#send([batch])
  }

  /**
   * Sends batches that share their options, each with one more settler that counts it as unsettled
   * until it settles. A failure of the call itself - one that settled no batch - rejects every
   * settler that is still waiting.
   * @param {PendingBatch[]} batches
   * @returns {Promise<void>} never rejects
   */
  #send(batches) {
    for (const batch of batches) {
      const { key } = batch
      this.#unsettled.set(key, (this.#unsettled.get(key) ?? 0) + 1)
      batch.settlers.push(settleOnce(() => this.#settled(key)))
    }
    const [{ acks, timeout, compression }] = batches
    return this.#sendPartitioned({ acks, timeout, compression, batches }).then(
      () => {},
      (error) => {
        for (const batch of batches) {
          for (const settler of batch.settlers) {
            settler.reject(error)
          }
        }
      }
    )
  }
}

module.exports = Accumulator
