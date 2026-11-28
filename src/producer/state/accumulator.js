const Long = require('../../utils/long')
const Fifo = require('../../utils/fifo')
const {
  KafkaJSApplicationRecoverableError,
  KafkaJSDeliveryTimeout,
  KafkaJSInvalidConfigurationError,
  KafkaJSRecordTooLargeError,
  KafkaJSProducerClosedError,
} = require('../../errors')
const { Deadline, ExpiryTimer, MAX_DELAY } = require('../deadline')
const { Admission } = require('./admission')
const JoinOrder = require('./joinOrder')
const {
  chargeOf,
  share,
  SendCopy,
  wire,
  recordsBytes,
  RequestBudget,
  BATCH_DEBIT,
} = require('./recordSize')

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
 * Two functions that call `release` once both of them have been called, each counted once.
 * @param {(() => void)|null|undefined} release
 * @returns {Array<(() => void)|null|undefined>} `release` itself, twice, when it is not a function
 */
const bothOf = (release) => {
  if (typeof release !== 'function') {
    return [release, release]
  }
  let pending = 2
  const half = () => {
    let done = false
    return () => {
      if (done) return
      done = true
      pending -= 1
      if (pending === 0) release()
    }
  }
  return [half(), half()]
}

/**
 * Owns one live batch's actual record array, reservation shares and batch debit. Subdivision
 * transfers the existing debit to the tail and charges a new head. Shared send shells and backing
 * stores remain reserved until every child share and every pre-admission pin ends.
 */
class BatchOwnership {
  #done = false
  /**
   * @param {Admission} admission the producer's buffer
   * @param {PendingBatch} batch the payload holder; copies share its record array
   * @param {boolean} [child=true] clear aliases allocated by subdivision on retirement
   */
  constructor(admission, batch, child = true) {
    this.admission = admission
    this.batch = batch
    this.child = child
  }

  /**
   * Clears every alias of this batch's array before returning its payload bytes, once.
   * @returns {void}
   */
  detach = () => {
    const batch = this.batch
    if (this.#done || batch.owners === null) return
    if (this.child && batch.messages !== null) batch.messages.length = 0
    batch.messages = null
    for (let i = 0; i < batch.owners.length; i += 2) {
      batch.owners[i].give(batch.owners[i + 1])
      batch.owners[i + 1] = 0
    }
  }

  /**
   * Drops payload, ends this owner's shares and repays its debit exactly once. A retired or
   * subdivided owner's late callback cannot release a child's charge.
   * @returns {void}
   */
  retire = () => {
    const batch = this.batch
    if (this.#done || batch.owners === null) return
    if (this.child) this.detach()
    this.#done = true
    const owners = batch.owners
    batch.owners = null
    batch.messages = null
    for (let i = 0; i < owners.length; i += 2) owners[i].unshare(owners[i + 1])
    this.admission.repay(batch.debt)
  }

  /**
   * Transfers the payload and shares into two independent holders before clearing the old array.
   * A straddling reservation gains one share, with no extra payload reservation. Each child can
   * detach, retire or split again independently; the parent becomes inert.
   * @param {number} at first record of the tail, strictly inside the array
   * @returns {object[]} head and tail fields for the send-path batch objects
   */
  cut(at) {
    const batch = this.batch
    const arrays = [batch.messages.slice(0, at), batch.messages.slice(at)]
    const owners = [[], []]
    let headBytes = arrays[0].reduce((sum, message) => sum + share(message), 0)
    for (let i = 0; i < batch.owners.length; i += 2) {
      const holder = batch.owners[i]
      const bytes = batch.owners[i + 1]
      const head = Math.min(bytes, headBytes)
      const tail = bytes - head
      if (head > 0) owners[0].push(holder, head)
      if (tail > 0) owners[1].push(holder, tail)
      if (head > 0 && tail > 0) holder.share(0)
      headBytes -= head
    }
    this.admission.debit(BATCH_DEBIT)
    const children = arrays.map((messages, index) => {
      const child = {
        messages,
        owners: owners[index],
        heldBytes: messages.reduce((sum, message) => sum + share(message), 0),
        debt: index === 0 ? BATCH_DEBIT : batch.debt,
      }
      child.ownership = new BatchOwnership(this.admission, child)
      child.detach = child.ownership.detach
      child.retire = child.ownership.retire
      return child
    })
    batch.messages.length = 0
    batch.messages = null
    batch.owners = null
    this.#done = true
    return children
  }
}

/** Tracks each unsettled leaf independently, including a leaf created after the hand-off. */
class BatchTracker {
  #done = false

  /**
   * @param {() => void} added increments this partition's handed-on batch count
   * @param {() => void} settled decrements that count
   */
  constructor(added, settled) {
    this.added = added
    this.settled = settled
  }

  /**
   * Transfers this count to two leaves, adding exactly one. The old tracker becomes inert.
   * @returns {BatchTracker[]} head and tail trackers
   */
  cut() {
    this.#done = true
    this.added()
    return [new BatchTracker(this.added, this.settled), new BatchTracker(this.added, this.settled)]
  }

  /** Ends the live count once, whatever its outcome. @returns {void} */
  resolve = () => {
    if (this.#done) return
    this.#done = true
    this.settled()
  }

  /** Ends the live count after failure. @returns {void} */
  reject = () => this.resolve()
}

/** The fields of a record the producer reads, beside its own enumerable ones. */
const RECORD_FIELDS = ['key', 'value', 'headers', 'timestamp', 'partition']

/**
 * A timestamp the producer keeps: a `Date` or a `Long` is mutable and the encoder reads it again
 * when it builds the batch, so it is read once now and kept as its exact decimal string, which the
 * encoder and the range check read as the same instant (`Date(0)` and a `Long` of 0 stay the
 * literal epoch). A reading that throws (a `Date` whose time cannot be read, a `value` getter that
 * throws) gives `NaN`, which the encodability check refuses with the typed error it always gave,
 * without reading the caller's object again. Every other form is immutable, or is left to the
 * encodability check, and is kept.
 * @param {*} timestamp
 * @returns {*}
 */
const ownedTimestamp = (timestamp) => {
  if (timestamp === null || typeof timestamp !== 'object') {
    return timestamp
  }
  try {
    if (timestamp instanceof Date) {
      return String(timestamp.getTime())
    }
    const { value } = timestamp
    return typeof value === 'bigint' ? String(value) : timestamp
  } catch {
    return NaN
  }
}

/**
 * A copy of a record's headers, each own key read once: an array value is copied too, element by
 * element up to its length read once, with its holes, without calling its methods. Each Buffer is
 * put in the copy as it is and noted in `copies`.
 * @param {*} headers
 * @param {SendCopy} copies
 * @returns {object}
 */
const ownedHeaders = (headers, copies) => {
  const copy = { ...headers }
  for (const name of Object.keys(copy)) {
    const header = copy[name]
    if (!Array.isArray(header)) {
      copies.note(copy, name)
      continue
    }
    const { length } = header
    const list = new Array(length)
    for (let i = 0; i < length; i++) {
      if (i in header) {
        list[i] = header[i]
        copies.note(list, i)
      }
    }
    copy[name] = list
  }
  return copy
}

/**
 * A plain array of `own(element)` for each element of `list`, holes kept. Built by index, so the
 * result is the producer's own array whatever class, iterator or `map` the caller's list has.
 * @template T, U
 * @param {T[]} list
 * @param {(element: T) => U} own
 * @returns {U[]}
 */
const ownedList = (list, own) => {
  const { length } = list
  const copy = new Array(length)
  for (let i = 0; i < length; i++) {
    if (i in list) {
      copy[i] = own(list[i])
    }
  }
  return copy
}

/**
 * The producer's own copy of a record, each field read once: its own enumerable fields, so a
 * custom partitioner still sees them, and the fields the producer reads, also when they come from
 * a prototype. A header container, a header-value array, a `Date` and a `Long` are copied, so
 * changing them after the call changes nothing; each Buffer is put in the copy as it is and noted
 * in `copies`, which copies those that pin a large store. Anything that is not an object is kept,
 * so validation fails it as before.
 * @param {*} message
 * @param {SendCopy} copies
 * @returns {*}
 */
const ownedRecord = (message, copies) => {
  if (message === null || typeof message !== 'object') {
    return message
  }

  const record = { ...message }
  for (const field of RECORD_FIELDS) {
    if (record[field] === undefined && !Object.hasOwn(record, field)) {
      const value = message[field]
      if (value !== undefined) {
        record[field] = value
      }
    }
  }
  copies.note(record, 'key')
  copies.note(record, 'value')
  if (record.headers != null) {
    record.headers = ownedHeaders(record.headers, copies)
  }
  if (record.timestamp != null) {
    record.timestamp = ownedTimestamp(record.timestamp)
  }
  return record
}

/**
 * A record as its batch carries it: one with a `BigInt` timestamp is copied with that timestamp as
 * a `Long`. The record encoder subtracts a batch's timestamps from each other, and a `BigInt`
 * cannot be mixed with the numbers of the other sends' records. A `0n` stays as it is: like `0` it
 * is falsy and means the send time, which a `Long` of 0 would not.
 * @param {object} message
 * @returns {object}
 */
const withEncodableTimestamp = (message) =>
  typeof message.timestamp === 'bigint' && message.timestamp !== 0n
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

/** Sorts `RecordMetadata` by topic name, then partition. */
const byTopicThenPartition = (a, b) =>
  a.topicName < b.topicName ? -1 : a.topicName > b.topicName ? 1 : a.partition - b.partition

/**
 * One run of a send's records for one partition while its join is prepared: what the run adds to
 * a batch, read from its records once, and the batch it joins. Nothing of it is published until
 * the whole join is.
 */
class JoinStep {
  /** whether it is the last run of its partition in the send */
  last = false
  /** the bytes of its records' keys and values */
  bytes = 0
  /** its records' shares in the producer's buffer */
  shares = 0
  /** @type {?PendingBatch} the batch it joins: a pending one, or a new shell */
  batch = null
  /** whether `batch` is a new shell, linked when the join is published */
  fresh = false
  /** @type {?PendingBatch} the pending batch it does not fit in, handed on after publication */
  displaced = null
  /** @type {?Settler} */
  settler = null
  /** whether its batch leaves in the join's own hand-off */
  full = false
  /** whether publication makes its batch, which stays pending, due at once */
  hurry = false
  /** whether its batch gets a new timer after publication (`cap`, `expiresAt`) */
  armed = false
  /** at most how many ms that timer waits from when it is set: the linger of a new batch */
  cap = Infinity
  /** the epoch ms that timer fires by at the latest: its delivery deadline or original linger */
  expiresAt = Infinity
  /** @type {?PendingBatch} the pending batch placement read for it, which must still be pending */
  found = null
  /** its batch's bytes once the run joins it */
  filled = 0
  /** its batch's wire bytes once the run joins it */
  wired = 0
  /** its batch's record count once the run joins it */
  length = 0

  /**
   * @param {string} topic
   * @param {number} partition
   * @param {object[]} messages
   * @param {number} wireBytes their wire bytes
   * @param {number} origin the position of its first record among the send's records for the
   *   partition
   * @param {number} capacity the chunk capacity of the topic and codec
   */
  constructor(topic, partition, messages, wireBytes, origin, capacity) {
    this.topic = topic
    this.partition = partition
    this.key = `${topic}/${partition}`
    this.messages = messages
    this.wire = wireBytes
    this.origin = origin
    this.capacity = capacity
  }

  /**
   * Reads its records in one walk: their keys' and values' bytes, their shares (see `share`), and
   * each record as its batch carries it (see `withEncodableTimestamp`); `messages` is copied only
   * when a record changes.
   * @returns {void}
   * @throws {*} whatever reading a record throws
   */
  measure() {
    const { messages } = this
    let carried = messages
    let bytes = 0
    let shares = 0
    for (let i = 0; i < messages.length; i++) {
      const message = messages[i]
      const { key, value } = message
      if (key != null) {
        bytes += Buffer.byteLength(key)
      }
      if (value != null) {
        bytes += Buffer.byteLength(value)
      }
      const encodable = withEncodableTimestamp(message)
      if (encodable !== message && carried === messages) {
        carried = messages.slice(0, i)
      }
      if (carried !== messages) {
        carried.push(encodable)
      }
      shares += share(message)
    }
    this.messages = carried
    this.bytes = bytes
    this.shares = shares
  }

  /**
   * Forgets its placement, so the join can place it again against the current pending batches:
   * the batch, shell and settler it chose are dropped unpublished.
   * @returns {void}
   */
  unplace() {
    this.fresh = false
    this.displaced = null
    this.found = null
    this.hurry = false
    this.armed = false
    this.cap = Infinity
    this.expiresAt = Infinity
  }
}

/**
 * Cuts one partition's records of a send at record boundaries into runs that each fit a batch of
 * `capacity` wire bytes, greedily, in order; no record is above `capacity` (`append` refused it).
 * @param {string} topic
 * @param {number} partition
 * @param {object[]} messages
 * @param {number} capacity
 * @param {JoinStep[]} steps takes the runs, in order
 * @returns {void}
 */
const chunksOf = (topic, partition, messages, capacity, steps) => {
  let start = 0
  let bytes = 0
  for (let index = 0; index < messages.length; index++) {
    const size = wire(messages[index])
    if (index > start && bytes + size > capacity) {
      steps.push(
        new JoinStep(topic, partition, messages.slice(start, index), bytes, start, capacity)
      )
      start = index
      bytes = 0
    }
    bytes += size
  }
  steps.push(new JoinStep(topic, partition, messages.slice(start), bytes, start, capacity))
}

/**
 * Two settlers that settle `settler` once both have: with the first one's value, or with the
 * first error either was given.
 * @param {{ resolve: Function, reject: Function }} settler
 * @returns {[{ resolve: Function, reject: Function }, { resolve: Function, reject: Function }]}
 */
const halvesOf = (settler) => {
  let pending = 2
  let value
  let failed = false
  let error
  const end = () => {
    pending -= 1
    if (pending === 0) {
      if (failed) settler.reject(error)
      else settler.resolve(value)
    }
  }
  const half = (first) => {
    let done = false
    return {
      resolve: (metadata) => {
        if (done) return
        done = true
        if (first) value = metadata
        end()
      },
      reject: (failure) => {
        if (done) return
        done = true
        if (!failed) {
          failed = true
          error = failure
        }
        end()
      },
    }
  }
  return [half(true), half(false)]
}

/**
 * A failure's `outcome`, read once: a send path may fail with any value, and one whose `outcome`
 * cannot be read has none, so reading it never interrupts a settlement.
 * @param {*} error
 * @returns {*}
 */
const outcomeOf = (error) => {
  try {
    return error?.outcome
  } catch {
    return undefined
  }
}

/**
 * One `append()`: settles once every share of its records - one per batch carrying some of them -
 * has settled, resolving with one `RecordMetadata` per partition, by topic, then partition, or
 * rejecting with the first error any share failed with, whatever value that is (`null` and
 * `undefined` included). A partition's metadata is that of the
 * share holding its first record, whatever order the answers came in. The send counts as one
 * more share until `joined()`, so no share can settle it while its records are still joining.
 * Settling it ends its holder's place in the flush inventory (`PendingSend#untrack`).
 * It rejects with `outcome: 'unsent'` only when every share failed unsent. When its first error says
 * `'unsent'` but another share was delivered or failed otherwise, it rejects with a new error of the
 * same class, `outcome: 'unknown'`, the first error as `cause`; that error is never changed, as
 * other sends may share it.
 */
class AppendedSend {
  #pending = 1
  /** @type {?object[]} created by the first metadata, so a held send keeps no array */
  #metadata = null
  /** whether a share failed: `#error` holds the first failure, whatever its value */
  #failed = false
  #error
  /** whether the first failure said `outcome: 'unsent'` */
  #unsent = false
  /** whether a share was delivered or failed with an outcome other than `'unsent'` */
  #written = false
  #resolve
  #reject
  #holder

  /**
   * @param {PendingSend} holder the send whose flush tracking ends when this settles
   * @param {(metadata: object[]) => void} resolve settles the append's promise
   * @param {(error: Error) => void} reject
   */
  constructor(holder, resolve, reject) {
    this.#holder = holder
    this.#resolve = resolve
    this.#reject = reject
  }

  /** One more batch carries some of its records. @returns {void} */
  share() {
    this.#pending += 1
  }

  /** Its records have all joined batches. @returns {void} */
  joined() {
    this.#settleOne()
  }

  /**
   * One share delivered.
   * @param {object|undefined} metadata undefined for an `acks: 0` send, which gets no answer
   * @param {number} origin the position of the share's first record among the send's records for
   *   its partition: 0 for the share whose metadata the send resolves with
   * @returns {void}
   */
  delivered(metadata, origin) {
    if (metadata !== undefined && origin === 0) {
      this.#metadata ??= []
      this.#metadata.push(metadata)
    }
    this.#written = true
    this.#settleOne()
  }

  /**
   * One share failed.
   * @param {*} error what the share failed with; any value, as a send path may throw anything
   * @returns {void}
   */
  failed(error) {
    const unsent = outcomeOf(error) === 'unsent'
    if (!this.#failed) {
      this.#failed = true
      this.#error = error
      this.#unsent = unsent
    }
    if (!unsent) this.#written = true
    this.#settleOne()
  }

  /** @returns {void} */
  #settleOne() {
    this.#pending -= 1
    if (this.#pending > 0) {
      return
    }

    if (this.#failed) {
      this.#reject(this.#unsent && this.#written ? this.#widened() : this.#error)
    } else {
      this.#resolve((this.#metadata ?? []).sort(byTopicThenPartition))
    }
    this.#holder.untrack()
    this.#holder = null
  }

  /**
   * The first failure, said `'unsent'`, as `outcome: 'unknown'` for a send some of whose records
   * may have been written; the failure itself when it cannot be read or rebuilt.
   * @returns {*}
   */
  #widened() {
    const error = this.#error
    try {
      const message = `Some records of this send may have been written: ${error.message}`
      return new error.constructor(message, { cause: error, outcome: 'unknown' })
    } catch {
      return error
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
  #origin
  #settled = false

  /**
   * A settler not counted yet: `register()` counts it as one share of `send`.
   * @param {AppendedSend} send
   * @param {number} index the position of the share's first record in the batch
   * @param {number} [origin=0] the position of that record among the send's records for the
   *   partition
   */
  constructor(send, index, origin = 0) {
    this.#send = send
    this.#index = index
    this.#origin = origin
  }

  /**
   * Counts it as one share of its send, once it is in a batch: a join's settlers are made while
   * the join is prepared and registered when it is published.
   * @returns {void}
   */
  register() {
    this.#send.share()
  }

  /** @returns {number} the position of its first record in its batch */
  get index() {
    return this.#index
  }

  /**
   * The settlers of a batch cut in two before anything of it was written: those of records before
   * `at` stay, those after move to the second batch, and a share the cut divides becomes two, the
   * second one counted as one more share of its send. Any other settler - one the accumulator did
   * not make - is settled once both batches have settled it (see `halvesOf`). A BatchTracker
   * transfers its count to two independent leaves rather than waiting on a parent latch.
   * @param {object[]} settlers the batch's settlers, in its order, the send path's own left out
   * @param {number} length the batch's record count
   * @param {number} at the first record of the second batch, from 1 to `length - 1`
   * @returns {[object[], object[]]} the settlers of the first batch and of the second
   */
  static cut(settlers, length, at) {
    const first = []
    const second = []
    const own = settlers.filter((settler) => settler instanceof Settler)
    for (const settler of settlers) {
      if (settler instanceof BatchTracker) {
        const [head, tail] = settler.cut()
        first.push(head)
        second.push(tail)
        continue
      }
      if (!(settler instanceof Settler)) {
        const [head, tail] = halvesOf(settler)
        first.push(head)
        second.push(tail)
        continue
      }
      const next = own[own.indexOf(settler) + 1]
      const end = next === undefined ? length : next.#index
      if (end <= at) {
        first.push(settler)
      } else if (settler.#index >= at) {
        settler.#index -= at
        second.push(settler)
      } else {
        const tail = new Settler(settler.#send, 0, settler.#origin + at - settler.#index)
        tail.register()
        first.push(settler)
        second.push(tail)
      }
    }
    return [first, second]
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
    this.#send.delivered(
      metadata === undefined ? undefined : offsetBy(metadata, this.#index),
      this.#origin
    )
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
 * A promise resolved with it settles two jobs later: after the promise of every send whose
 * settlement emptied a flush epoch, which settles at most two jobs after that step.
 */
const SETTLED = Promise.resolve()

/**
 * The sends begun between two flush snapshots, counted while unsettled. A snapshot closes the
 * current epoch; a closed epoch settles once it and every older epoch have no unsettled send.
 */
class FlushEpoch {
  /** how many of its sends have not settled */
  unsettled = 0
  /** @type {?FlushEpoch} the epoch a snapshot opened when it closed this one */
  newer = null
  /** @type {?Promise<void>} the snapshot's promise, once a snapshot closed it */
  promise = null
  #resolve = null

  /** @param {boolean} oldest whether no older epoch is still waiting */
  constructor(oldest) {
    this.oldest = oldest
  }

  /** One of its sends settled. @returns {void} */
  settle() {
    this.unsettled -= 1
    this.#check()
  }

  /**
   * Closes it for a snapshot.
   * @returns {FlushEpoch} the epoch later sends join
   */
  close() {
    this.promise = new Promise((resolve) => {
      this.#resolve = resolve
    })
    this.newer = new FlushEpoch(false)
    const newer = this.newer
    this.#check()
    return newer
  }

  /** Settles a drained closed epoch and hands the oldest place on. @returns {void} */
  #check() {
    for (let epoch = this; epoch.oldest && epoch.unsettled === 0 && epoch.promise !== null;) {
      epoch.#resolve(SETTLED)
      const newer = epoch.newer
      epoch.newer = null
      newer.oldest = true
      epoch = newer
    }
  }
}

/** The pinner of an accumulator with no producer cluster: it protects no topic. */
const NO_PINNER = { holdTopic() {}, releaseTopic() {} }

/**
 * One send between the public call and the batches that take its records over. The records live
 * here, and nowhere else, until the send has been assigned its partitions: a continuation that
 * waits for the producer id or for metadata holds this object, reads `records` after its await and
 * stops when it is `null`, so a send that expired while it waited keeps none of its records alive.
 * The object itself is retained until the last continuation that holds it ends; `pins` counts them.
 *
 * It also owns the send's reservation in the producer's buffer. When its records join batches,
 * each batch takes its records' shares, released when that batch retires; what no batch took is
 * released when the send leaves the registry, at once when it fails or expires. The shell (1536
 * bytes, the backing stores of its Buffers and their bytes) is released last, once the send has
 * left the registry, no batch carries a share of it and no continuation holds it. Its topics stay
 * pinned on the producer cluster for exactly as long as that shell (`holdTopics`, `releaseTopics`).
 */
class PendingSend {
  /**
   * @param {{ topic: string, messages: object[] }[]} records
   * @param {Deadline} deadline the send's delivery deadline, counted from the public call
   * @param {{ holdTopic(topic: string): void, releaseTopic(topic: string): void }} pinner the
   *   producer cluster's topic pins, which keep its topics' metadata while the shell is charged
   */
  constructor(records, deadline, pinner) {
    /** @type {?{ topic: string, messages: object[] }[]} `null` once the send has settled or joined */
    this.records = records
    this.deadline = deadline
    this.settled = false
    this.pins = 0
    /** @type {?(error: Error) => void} rejects the send; set while it is held */
    this.reject = null
    /** @type {?import('./admission').Reservation} the send's bytes in the buffer, once admitted */
    this.reservation = null
    /** the bytes of its records' shares that no batch has taken yet */
    this.payload = 0
    /** how many batches carry a share of its records and have not retired */
    this.shares = 0
    this.pinner = pinner
    /** @type {?(string|string[])} the topic, or topics, it pins until its shell is released */
    this.topics = null
    /** @type {?FlushEpoch} the flush epoch it counts in until it settles */
    this.epoch = null
    /** @type {?(string|string[])} the topics whose call order it keeps (see `JoinOrder`) */
    this.lanes = null
    /** @type {?object[]} its records assigned to partitions, while it waits to join */
    this.entries = null
    /** @type {?(outcome: Promise<object[]>) => void} resolves the send; set while it is held */
    this.resolve = null
    /** @type {?{ acks: number, timeout?: number, compression?: number }} its request options */
    this.options = null
    /**
     * whether the partitioner call of its current partitioning attempt failed, which queued
     * nothing; set at that call, cleared when an attempt starts
     */
    this.refused = false
  }

  /**
   * Ends its count in its flush epoch, at the step that settles its promise. Every tracked send
   * reaches exactly one such step: a refusal, the end of its wait, or its `AppendedSend`.
   * @returns {void}
   */
  untrack() {
    const epoch = this.epoch
    this.epoch = null
    epoch.settle()
  }

  /**
   * Pins each topic that carries records, before admission can wait. A send of one topic keeps
   * the name itself, so the common send adds no array or closure to what its shell is charged.
   * @returns {void}
   */
  holdTopics() {
    const topics = this.records
      .filter(({ messages }) => messages.length > 0)
      .map(({ topic }) => topic)
    for (const topic of topics) {
      this.pinner.holdTopic(topic)
    }
    this.topics = topics.length === 1 ? topics[0] : topics
  }

  /**
   * Releases the topics `holdTopics` pinned, once: with the shell, or on a refusal before
   * admission.
   * @returns {void}
   */
  releaseTopics() {
    const topics = this.topics
    if (topics === null) {
      return
    }
    this.topics = null
    if (typeof topics === 'string') {
      this.pinner.releaseTopic(topics)
      return
    }
    for (const topic of topics) {
      this.pinner.releaseTopic(topic)
    }
  }

  /**
   * Keeps the send's reservation: all of it but `shell` is its records' shares.
   * @param {import('./admission').Reservation} reservation
   * @param {{ total: number, shell: number }} charge
   * @returns {void}
   */
  admit(reservation, { total, shell }) {
    this.reservation = reservation
    this.payload = total - shell
  }

  /** Counts one continuation that holds this send. @returns {void} */
  pin() {
    this.pins += 1
  }

  /** Counts that continuation's end. @returns {void} */
  unpin() {
    this.pins -= 1
    this.release()
  }

  /**
   * A batch takes `bytes` of the send's records: they are released when it retires.
   * @param {number} bytes
   * @returns {void}
   */
  share(bytes) {
    this.payload -= bytes
    this.shares += 1
  }

  /**
   * A batch carrying `bytes` of the send's records dropped them; it still holds the send, so its
   * share stays counted until it retires.
   * @param {number} bytes
   * @returns {void}
   */
  give(bytes) {
    this.reservation.release(bytes)
  }

  /**
   * A batch carrying `bytes` of the send's records retired.
   * @param {number} bytes
   * @returns {void}
   */
  unshare(bytes) {
    this.reservation.release(bytes)
    this.shares -= 1
    this.release()
  }

  /**
   * Once the send has left the registry: releases what no batch took and, when no batch carries a
   * share of it and no continuation holds it, the rest. Nothing is released twice.
   * @returns {void}
   */
  release() {
    if (!this.settled) {
      return
    }
    if (this.payload > 0) {
      this.reservation.release(this.payload)
      this.payload = 0
    }
    if (this.shares === 0 && this.pins === 0) {
      this.reservation.releaseAll()
      this.releaseTopics()
    }
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
 * @property {object[]|null} messages null once the batch has detached
 * @property {Settler[]} settlers one per append whose records it carries, in arrival order
 * @property {number} bytes its keys' and values' bytes
 * @property {number} wire its records' wire bytes, which `maxRequestSize` plans on
 * @property {number} createdAt when the earliest of those appends began: its delivery deadline
 *   counts from there
 * @property {number} dueAt when its `linger` runs out
 * @property {string|null} mergeKey the node that led its partition when its `linger` timer was
 *   armed, with its `options`, as one key; null when the node was not known. It only decides which
 *   batches leave together
 * @property {ReturnType<typeof setTimeout>|null} timer
 * @property {boolean} due whether its `linger` has run out
 * @property {number} heldBytes the shares of the records it carries in the producer's buffer
 * @property {number} debt its own charge, debited when it was created
 * @property {(PendingSend|number)[]|null} owners each send whose records it carries, followed by
 *   the bytes of their shares still charged; null once it has retired
 * @property {BatchOwnership} ownership transfers the payload and debit when a guard subdivides it
 * @property {() => void} retire releases its shares and repays its debt, once; the send path calls
 *   it when nothing holds the batch any more
 * @property {() => void} detach drops its records and gives back the bytes of their shares, once;
 *   until `retire`, its sends keep their shells and it keeps its debt. The send path calls it when
 *   the batch has settled and no request carries it, while a continuation of its own may still hold
 *   the batch
 */

/**
 * @typedef {object} Outgoing one request's batches, out of `#pending` and waiting in `#outbox`
 * @property {PendingBatch[]} batches the batches, which share their options, in hand-off order
 * @property {?Promise<void>} sent its request's outcome once it began, which never rejects; null
 *   until then, and for good when a forced close refused the batches instead
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
  #expired
  #admission
  #pinner
  #budget
  #getGeneration
  /** the flush epoch sends begun now count in */
  #epoch = new FlushEpoch(true)
  /** @type {Fifo<PendingSend>} the sends waiting for their partitions, in the order of their deadlines */
  #holders = new Fifo()
  #expiry = new ExpiryTimer((now) => this.#expireHolders(now))
  /** @type {Map<string, PendingBatch>} */
  #pending = new Map()
  /**
   * @type {Fifo<Outgoing>} the batches taken out of `#pending` and not handed to the send path
   *   yet, one entry per request, in hand-off order. Every batch leaving `#pending` enters it in
   *   the same step, before anything a caller supplied can run, and leaves it only when its own
   *   request begins (`#dispatch`) or a forced close takes it: so a batch is always in `#pending`,
   *   here, or owned by the send path, and every hand-off, flush and forced close reads both
   */
  #outbox = new Fifo()
  /** how many `flush()` calls began: a join prepared across one hands its batches on at once */
  #flushes = 0
  /** the joins under way refuse with its `error` once a transaction's abort sweep sets it */
  #sweep = { error: null }
  /** @type {Map<string, number>} by key, the batches handed on and not settled yet */
  #unsettled = new Map()
  /** Whether a hand-off of the due batches is queued */
  #handOffQueued = false
  /** How many appends are being assigned their partitions, not yet in a batch */
  #appending = 0
  /** @type {(() => void)[]} the `close()` calls waiting for nothing to be left to deliver */
  #closers = []
  /** the call order sends join their batches in, per topic */
  #order = new JoinOrder((send) => this.#joinHeld(send))

  /**
   * @param {object} options
   * @param {number} options.linger ms a batch waits for more records; 0 for none
   * @param {number} options.batchSize bytes of keys and values at which a batch is sent
   * @param {(send: PendingSend) =>
   *   Promise<{ topic: string, partition: number, messages: object[] }[]>} options.partition
   *   assigns every record its partition once; it reads the records from `send.records` after each
   *   await and stops when that is `null`
   * @param {(request: { acks: number, timeout?: number, compression?: number,
   *   batches: PendingBatch[] }) => Promise<object[]>} options.sendPartitioned sends the batches
   *   and settles each one's `settlers` as it settles; it must enqueue them before it returns, so
   *   that a flush is complete when `flush()` returns. Once it returns it owns them and retires
   *   each; when it throws instead it took none, and the accumulator retires them
   * @param {() => boolean} [options.lingers] whether a batch that forms now may wait for `linger`;
   *   asked while a join is prepared, before any of it is published: a close it causes refuses
   *   that send, and a flush it causes makes the join place its records again and hand them on
   * @param {() => boolean} [options.holds] whether a due batch waits for its partition's batch in
   *   flight to settle; asked once per hand-off of the due batches, before any is taken, and once
   *   per placement of a join that fills a batch
   * @param {(topic: string, partition: number) => number|null|undefined} [options.leaderOf]
   *   the node that leads a partition, read from what the client already knows and never fetched;
   *   asked once for each batch that waits for its `linger`, while the join that makes it is
   *   prepared, as `lingers` is. A negative id, null, undefined or a throw all mean the leader is
   *   not known, and a batch whose leader is not known is never merged
   * @param {number} [options.deadline] ms from a send's public call to its delivery deadline, the
   *   send path's own: a send still waiting for its partitions settles with `KafkaJSDeliveryTimeout`
   *   when it is reached, and a held batch goes out then. `linger`, `batchSize` and `deadline` are
   *   each read once, here, as numbers (null or undefined `deadline`: none), so no later timer or
   *   size decision converts them again
   * @param {(error: Error) => Error} [options.expired] called with the error of a send that
   *   expired before it joined a batch, or that the buffer refused, to arm a transaction; returns
   *   the error to reject with. Not called for a send whose deadline passed while it waited to
   *   retry a failed partitioner call (`PendingSend#refused`): it rejects with the timeout as it is
   * @param {{ holdTopic(topic: string): void, releaseTopic(topic: string): void }} [options.pinner]
   *   the producer cluster's topic pins: each send pins its topics until its shell is released
   * @param {() => (import('../generation')|undefined)} [options.getGeneration] the producer's
   *   current lifecycle: a closed one refuses new sends and a send admitted after a wait
   * @param {Admission} [options.admission] the producer's buffer; without one, every send fits
   * @param {RequestBudget} [options.budget] the producer's request-size plan: a record no request
   *   can carry is refused, a partition's records that one request cannot carry are cut into
   *   batches that each fit, and no batch grows past what fits; without one, nothing is limited
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
    expired = (error) => error,
    admission = new Admission({ capacity: Number.MAX_SAFE_INTEGER, maxBlockMs: 0 }),
    budget = new RequestBudget(),
    pinner = NO_PINNER,
    getGeneration = () => undefined,
  }) {
    this.#linger = Number(linger)
    this.#batchSize = Number(batchSize)
    this.#partition = partition
    this.#sendPartitioned = sendPartitioned
    this.#lingers = lingers
    this.#holds = holds
    this.#leaderOf = leaderOf
    this.#deadline = deadline == null ? undefined : Number(deadline)
    this.#expired = expired
    this.#admission = admission
    this.#pinner = pinner
    this.#budget = budget
    this.#getGeneration = getGeneration
  }

  /**
   * The producer's own copy of a send's records, taken at the public call before anything reads
   * them: every validation, the charge, the partitioner and the encoder see this copy, so changing
   * a record, its headers, a header-value array or a `Date` or `Long` timestamp after the call
   * changes nothing. A record's own enumerable fields are kept beside the fields the producer
   * reads; the bytes of its Buffers are shared, as in 2.2.4, except a small view of a large backing
   * store: all of those in the send are copied into one store of their own (`SendCopy`). Entries
   * and records that are not objects are kept as they are, and so is a `messages` that is not an
   * array, so validation refuses them as before.
   * @param {{ topic: string, messages: object[] }[]} topicMessages
   * @returns {{ topic: string, messages: object[] }[]}
   */
  static snapshot(topicMessages) {
    const copies = new SendCopy()
    const snapshot = ownedList(topicMessages, (entry) => {
      if (entry === null || typeof entry !== 'object') {
        return entry
      }
      const { topic, messages } = entry
      return {
        topic,
        messages: Array.isArray(messages)
          ? ownedList(messages, (message) => ownedRecord(message, copies))
          : messages,
      }
    })
    copies.copy()
    return snapshot
  }

  /**
   * The batches that cut a send at record boundaries, and the settlers they carry, when the send
   * path must cut one of its batches in two at record `at` before anything of it was written:
   * the settlers of the records before `at` stay with the first batch and those after move to the
   * second, and a send whose records straddle the cut gains one share. Product ownership
   * transfers into independent child holders: each clears its actual array before returning
   * payload and repays its own debit once. BatchTracker counts both leaves independently.
   * Only unknown external settlers and callbacks of unowned direct callers use a shared latch.
   * @param {{ messages: object[], settlers: object[], ownership?: BatchOwnership,
   *   retire?: Function, detach?: Function }} batch
   *   `settlers` without the send path's own
   * @param {number} at from 1 to the record count less 1
   * @returns {Array<{ messages: object[], recordCount: number, wire: number, settlers: object[],
   *   ownership?: BatchOwnership, retire?: Function, detach?: Function }>} head then tail fields
   */
  static cut(batch, at) {
    const { messages, settlers, retire, detach, ownership } = batch
    const length = messages.length
    const [first, second] = Settler.cut(settlers, length, at)
    if (ownership) {
      return ownership.cut(at).map((child, index) => ({
        ...child,
        recordCount: child.messages.length,
        wire: recordsBytes(child.messages),
        settlers: index === 0 ? first : second,
      }))
    }
    const retires = bothOf(retire)
    const detaches = bothOf(detach)
    const arrays = [messages.slice(0, at), messages.slice(at)]
    messages.length = 0
    return arrays.map((payload, index) => ({
      messages: payload,
      recordCount: payload.length,
      wire: recordsBytes(payload),
      settlers: index === 0 ? first : second,
      retire: () => {
        payload.length = 0
        retires[index]?.()
      },
      detach: () => {
        payload.length = 0
        detaches[index]?.()
      },
    }))
  }

  /**
   * Adds a send's records to the pending batches of their partitions. Resolves once every batch
   * carrying them has been acknowledged, with one `RecordMetadata` per partition, by topic and then
   * partition, whose `baseOffset` is that of the send's own first record there; rejects with the
   * first error such a batch failed with, after all of them have settled. A send with no record
   * resolves `[]` at once and charges nothing. A send still waiting for its partitions at its
   * delivery deadline rejects with `KafkaJSDeliveryTimeout`, `outcome: 'unsent'`, whatever it
   * waits for. A partition's records that one request cannot carry go out in several batches, in
   * order, each delivered on its own; the send then rejects with the first error any of them
   * failed with, while the others may have been written.
   *
   * Before its first await the send is charged against the producer's buffer (see `chargeOf`) and
   * admitted: at once when it fits and no send waits, else after the sends ahead of it, within
   * `maxBlockMs` and its deadline. Its records are the caller's owned copy (`Accumulator.snapshot`),
   * held by the send while it waits; a refusal drops them. A refusal goes through `expired`, so it
   * arms a transaction and the send rejects with the refusal itself. It takes its place in its
   * topics' call order there too (see `JoinOrder`), so a send admitted after a wait still joins
   * before every later send of its topics; a refusal gives the place up. Admission is FIFO, so no
   * later send holds buffer room while it waits for this one to be admitted.
   * @param {object} request
   * @param {number} request.acks
   * @param {number} [request.timeout]
   * @param {number} [request.compression]
   * @param {{ topic: string, messages: object[] }[]} request.topicMessages the producer's own
   *   copy, which nothing changes afterwards
   * @param {number} [request.startedAt] when the public call began, in epoch ms; now when absent
   * @returns {Promise<object[]>}
   * @throws {KafkaJSInvalidConfigurationError} for a record the encoder cannot write; nothing is
   *   queued
   * @throws {KafkaJSRecordTooLargeError} a record no request under `maxRequestSize` can carry
   *   (`reason: 'maxRequestSize'`), or a record or the send that can never fit in the buffer;
   *   nothing waits
   * @throws {import('../../errors').KafkaJSBufferExhaustedError} no room within `maxBlockMs`, or
   *   too many bytes already waiting
   */
  async append({ acks, timeout, compression, topicMessages, startedAt = Date.now() }) {
    for (const { topic, messages } of topicMessages) {
      assertEncodable(topic, messages)
    }
    if (!hasRecords(topicMessages)) {
      return []
    }

    this.#getGeneration()?.check()
    const send = new PendingSend(
      topicMessages,
      new Deadline(startedAt, this.#deadline),
      this.#pinner
    )
    if (send.deadline.remaining() === 0) {
      throw this.#expired(this.#timedOut(send))
    }

    this.#appending += 1
    send.holdTopics()
    send.lanes = send.topics
    this.#order.enter(send)
    this.track(send)
    try {
      this.#assertFits(topicMessages, compression)
      const charge = chargeOf(topicMessages, this.#admission.capacity)
      const granted = this.#admission.acquire(charge.total, send.deadline)
      send.admit(granted instanceof Promise ? this.#granted(await granted) : granted, charge)
    } catch (error) {
      send.records = null
      send.settled = true
      send.releaseTopics()
      send.untrack()
      this.#end()
      this.#order.passed(send)
      throw this.#expired(error)
    }
    return this.#hold(send, { acks, timeout, compression })
  }

  /**
   * A reservation granted after a wait, unless the producer closed meanwhile: then it is given
   * back and the send is refused as never sent.
   * @param {import('./admission').Reservation} reservation
   * @returns {import('./admission').Reservation}
   * @throws {KafkaJSProducerClosedError} `outcome: 'unsent'`, when the lifecycle closed
   * @throws {KafkaJSApplicationRecoverableError} the exact armed error when initial fencing
   *   revoked the lifecycle before the admission continuation resumed
   */
  #granted(reservation) {
    const lifecycle = this.#getGeneration()
    if (lifecycle?.closed) {
      reservation.releaseAll()
      throw this.#closedError(lifecycle, 'The producer was closed before admission completed')
    }
    return reservation
  }

  /**
   * @param {import('../generation')} lifecycle a closed lifecycle
   * @param {string} message
   * @returns {Error} what a send refused by that close rejects with: the exact armed error when
   *   initial fencing closed it, else `KafkaJSProducerClosedError`, `outcome: 'unsent'`
   */
  #closedError(lifecycle, message) {
    return lifecycle.error instanceof KafkaJSApplicationRecoverableError
      ? lifecycle.error
      : new KafkaJSProducerClosedError(message, { outcome: 'unsent' })
  }

  /**
   * Counts a send in the current flush epoch until its promise settles (`PendingSend#untrack`):
   * one reference on the send, with no promise, closure or collection entry of its own.
   * @param {PendingSend} send
   * @returns {void}
   */
  track(send) {
    send.epoch = this.#epoch
    this.#epoch.unsettled += 1
  }

  /**
   * The flush snapshot: a promise that settles once every send begun before this call has
   * settled, after each such send's own promise; none when no earlier send is unsettled.
   * @returns {Promise<void>[]}
   */
  unsettled() {
    const epoch = this.#epoch
    if (epoch.oldest && epoch.unsettled === 0) {
      return []
    }
    this.#epoch = epoch.close()
    return [epoch.promise]
  }

  /**
   * Rejects every send still waiting for its partitions and every batch that has not left this
   * accumulator, with `outcome: 'unsent'`, and clears their timers. Admission waiters are
   * cancelled by their `Admission`; a held assignment keeps its send's shell until it returns.
   * Every batch is taken out of `#outbox` and `#pending` before the first rejection, so a flush or
   * close that a rejection reaches finds none of them.
   * @param {Error} [surfaced] what the sends reject with instead of that error
   * @returns {void}
   */
  forceClose(surfaced) {
    /** @type {PendingBatch[]} the batches waiting for their request, in hand-off order, then pending */
    const refused = []
    for (let entry = this.#outbox.shift(); entry !== undefined; entry = this.#outbox.shift()) {
      refused.push(...entry.batches)
    }
    for (const batch of this.#pending.values()) {
      refused.push(batch)
    }
    this.#pending.clear()
    this.#order.clear()
    for (let send = this.#holders.peek(); send !== undefined; send = this.#holders.peek()) {
      this.#settleHolder(
        send,
        surfaced ??
          new KafkaJSProducerClosedError(
            'The producer was closed before the send was partitioned',
            {
              outcome: 'unsent',
            }
          )
      )
    }
    for (const batch of refused) {
      this.#refuse(batch, surfaced)
    }
    this.#checkIdle()
  }

  /**
   * Rejects a batch that never left this accumulator, for a forced close, and retires it.
   * @param {PendingBatch} batch
   * @param {Error} [surfaced] what its sends reject with instead of `outcome: 'unsent'`
   * @returns {void}
   */
  #refuse(batch, surfaced) {
    clearTimeout(batch.timer)
    const error =
      surfaced ??
      new KafkaJSProducerClosedError('The producer was closed before sending', {
        outcome: 'unsent',
      })
    batch.settlers.forEach((settler) => settler.reject(error))
    batch.retire()
  }

  /**
   * Refuses every send whose join is under way and has not been published: each rejects with
   * `error`, and none of its records joins a batch. A transaction's abort calls it before it drops
   * the queued batches, so a send whose join a record accessor interrupted with that abort is
   * dropped with them rather than written after it. Joins that begin later are not refused.
   * @param {Error} error
   * @returns {void}
   */
  abortJoins(error) {
    this.#sweep.error = error
    this.#sweep = { error: null }
  }

  /**
   * Refuses a send with a record that no Produce request under `maxRequestSize` can carry: its
   * wire size is above the chunk capacity of its topic and codec (see `RequestBudget`), before
   * anything of the send is charged, queued or written, as the Java producer's
   * `ensureValidRecordSize` refuses it before `buffer.memory`.
   * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java
   * @param {{ topic: string, messages: object[] }[]} topicMessages
   * @param {number} [compression]
   * @returns {void}
   * @throws {KafkaJSRecordTooLargeError} `reason: 'maxRequestSize'`, `scope: 'record'`
   */
  #assertFits(topicMessages, compression) {
    for (const { topic, messages } of topicMessages) {
      const limit = this.#budget.chunkCapacity(topic, compression)
      if (limit === Infinity) {
        continue
      }
      for (const message of messages) {
        const size = wire(message)
        if (size > limit) {
          throw new KafkaJSRecordTooLargeError(
            `A record of ${size} bytes for topic "${topic}" can never fit in a Produce request of ` +
              `maxRequestSize (${this.#budget.maxRequestSize} bytes), which carries at most ${limit} bytes of records`,
            { reason: 'maxRequestSize', scope: 'record', size, limit, topic }
          )
        }
      }
    }
  }

  /**
   * Holds a send while its records are assigned their partitions, then joins them to batches in
   * call order: after every earlier send that shares a topic with it has joined or ended (see
   * `JoinOrder`), so partitioning that finishes out of order (a failed metadata refresh retried
   * on each send's own backoff) cannot reorder records within a partition. The send settles at its
   * deadline even while the assignment never returns or it waits for an earlier send: its records
   * are dropped, and what the assignment does afterwards is ignored.
   * @param {PendingSend} send
   * @param {{ acks: number, timeout?: number, compression?: number }} options
   * @returns {Promise<object[]>} the send's outcome, as `append()` describes it
   */
  #hold(send, options) {
    return new Promise((resolve, reject) => {
      send.reject = reject
      send.resolve = resolve
      send.options = options
      this.#register(send)

      send.pin()
      let assigned
      try {
        assigned = this.#partition(send)
      } catch (error) {
        assigned = Promise.reject(error)
      }
      assigned.then(
        (entries) => {
          send.unpin()
          if (!send.settled) {
            send.entries = entries
            this.#order.ready(send)
          }
        },
        (error) => {
          send.unpin()
          if (this.#leave(send)) {
            send.release()
            reject(error)
            send.untrack()
            this.#end()
            this.#order.passed(send)
          }
        }
      )
    })
  }

  /**
   * Joins a held send's assigned records to their batches, once `JoinOrder` lets it.
   * @param {PendingSend} send not settled, with its `entries`
   * @returns {void}
   */
  #joinHeld(send) {
    const { entries, resolve, reject, options } = send
    this.#leave(send)
    try {
      resolve(this.#join(entries, { ...options, createdAt: send.deadline.startedAt }, send))
    } catch (error) {
      reject(error)
      send.untrack()
    } finally {
      send.release()
      this.#end()
    }
  }

  /**
   * Puts a send in the deadline registry, behind every send due no later than it - which is the
   * back of the queue unless the clock stepped back - and arms the timer for it if it is the
   * earliest.
   * @param {PendingSend} send
   * @returns {void}
   */
  #register(send) {
    const { dueAt } = send.deadline
    let index = this.#holders.size
    while (index > 0 && this.#holders.at(index - 1).deadline.dueAt > dueAt) {
      index -= 1
    }
    this.#holders.insertAt(index, send)
    this.#expiry.arm(dueAt)
  }

  /**
   * Ends a send's wait: it stops holding its records and its place in the deadline registry. The
   * caller then gives the outcome and calls `#end`.
   * @param {PendingSend} send
   * @returns {boolean} whether this call ended it, false when it had already ended
   */
  #leave(send) {
    if (send.settled) {
      return false
    }

    send.settled = true
    send.records = null
    send.entries = null
    send.reject = null
    send.resolve = null
    send.options = null
    if (this.#holders.peek() === send) {
      this.#holders.shift()
    } else {
      this.#holders.remove(send)
    }
    if (this.#holders.size === 0) {
      this.#expiry.clear()
    }
    return true
  }

  /**
   * Ends the append of a send that has left the registry, after its records have joined a batch or
   * its rejection has been given, so that the accumulator is never idle between the two.
   * @returns {void}
   */
  #end() {
    this.#appending -= 1
    this.#checkIdle()
  }

  /**
   * The timer of the deadline registry: every send due now is rejected and dropped.
   * @param {number} now epoch ms
   * @returns {number} the deadline of the next send still waiting, or Infinity
   */
  #expireHolders(now) {
    for (
      let send = this.#holders.peek();
      send !== undefined && send.deadline.dueAt <= now;
      send = this.#holders.peek()
    ) {
      this.#settleHolder(send, this.#timedOut(send), send.refused)
    }

    const next = this.#holders.peek()
    return next === undefined ? Infinity : next.deadline.dueAt
  }

  /**
   * Ends a registered send's wait with `error`, through `expired` unless `refused`. Its shell stays
   * charged until its pins end.
   * @param {PendingSend} send
   * @param {Error} error
   * @param {boolean} [refused=false] the deadline passed while the send waited to retry a failed
   *   partitioner call: the timeout stands as it is and arms nothing. A forced close never passes it
   * @returns {void}
   */
  #settleHolder(send, error, refused = false) {
    const { reject } = send
    this.#leave(send)
    send.release()
    reject(refused ? error : this.#expired(error))
    send.untrack()
    this.#end()
    this.#order.passed(send)
  }

  /**
   * @param {PendingSend} send
   * @returns {KafkaJSDeliveryTimeout} the error of a send whose deadline passed before any request
   *   carried its records
   */
  #timedOut({ deadline }) {
    return new KafkaJSDeliveryTimeout(
      `Gave up before the send reached a broker: its ${deadline.budgetMs} ms delivery deadline passed`,
      { outcome: 'unsent' }
    )
  }

  /**
   * Adds one append's records, assigned to their partitions, to the pending batches. When they
   * fill a batch, every batch carrying them goes on in one hand-off - after any older batch of a
   * partition that had to go first, in its own call - so one send usually makes one request per
   * broker. When none fills, its records that join a batch an earlier append began leave with
   * that batch, on its timer, so one send can still reach a broker in two requests.
   * A partition's records that one batch cannot carry under `maxRequestSize` (its chunk capacity)
   * are cut at record boundaries into chunks that each can, in order; every chunk but the last is
   * full and goes out in this hand-off, and no batch grows past `batchSize` or the chunk capacity.
   * Each batch takes the shares of the send's records it carries (see `PendingSend#share`), and a
   * batch created here debits its own charge from the buffer, without waiting.
   *
   * A join publishes all of the send or nothing. It is prepared first, changing no batch: the
   * records are read (`#measure`, the only place they are), each run's batch is chosen, new
   * batches are made as shells with their settlers, `lingers`, `holds` and `leaderOf` are asked
   * (`#place`, `#plan`) and each timer's intent is decided. A failure there leaves no batch, share,
   * debit or timer behind. Those callbacks may flush or close: a pending batch the placement chose
   * that is no longer pending afterwards, or any flush begun since the join did, makes it place
   * the send again against the pending batches as they are now - after a flush, with every batch
   * handed on at once, as the flush asked, and with no callback asked. Each placement ends at the
   * fence: a send whose producer closed, or whose transaction began its abort sweep, is refused.
   * Publication (`#publish`) only links and counts, and calls nothing.
   *
   * Invariant: from publication until the send path owns it, each batch is in `#pending` or in
   * `#outbox`, both of which every hand-off, flush and forced close reads; a timer is set only
   * after publication and only on a batch still pending. So whatever a timer's creation or a
   * hand-off calls finds the join wholly published, and nothing it closes or flushes is handed on
   * or timed afterwards.
   * @param {{ topic: string, partition: number, messages: object[] }[]} entries
   * @param {{ acks: number, timeout?: number, compression?: number, createdAt: number }} options
   * @param {PendingSend} holder the send whose records these are
   * @returns {Promise<object[]>} the append's outcome, as `append()` describes it
   * @throws {KafkaJSProducerClosedError} `outcome: 'unsent'`, when the lifecycle closed while the
   *   join was prepared; the lifecycle's own error when initial fencing closed it
   * @throws {*} the error `abortJoins` was given, when it was called while the join was prepared;
   *   whatever reading a record, `chunkCapacity`, `lingers` or `holds` throws
   */
  #join(entries, { acks, timeout, compression, createdAt }, holder) {
    const lifecycle = this.#getGeneration()
    const sweep = this.#sweep
    const flushes = this.#flushes
    // One due time for every batch this append creates, so they fall due together.
    const shape = {
      options: `${acks}/${timeout}/${compression}`,
      acks,
      timeout,
      compression,
      createdAt,
      dueAt: Date.now() + this.#linger,
    }
    const steps = this.#measure(entries, compression)
    let send
    const appended = new Promise((resolve, reject) => {
      send = new AppendedSend(holder, resolve, reject)
    })
    /** @type {PendingBatch[]} the batches this append fills, then the due ones going with them */
    const full = []
    /** @type {JoinStep[]} each partition's last run whose batch stays pending after the join */
    const joined = []
    let hurried = false
    let holding
    for (;;) {
      this.#place(steps, shape, send, full, joined, hurried)
      holding = full.length > 0 && this.#holds()
      this.#plan(joined, full.length > 0 || this.#linger === 0, createdAt)
      if (lifecycle?.closed) {
        throw this.#closedError(lifecycle, 'The producer was closed before the send joined a batch')
      }
      if (sweep.error !== null) {
        throw sweep.error
      }
      const flushed = !hurried && this.#flushes !== flushes
      if (!flushed && !this.#moved(steps)) {
        break
      }
      hurried ||= flushed
      full.length = 0
      joined.length = 0
      for (const step of steps) {
        step.unplace()
      }
    }

    const filled = full.length > 0
    const held = this.#publish(steps, full, holder, createdAt, holding)
    this.#arm(steps, joined, full)
    this.#dispatch()
    this.#timeHeld(held)
    if (!filled && this.#linger === 0) {
      this.#queueHandOff()
    }
    send.joined()
    return appended
  }

  /**
   * Whether a callback asked while the join was placed took a pending batch it chose: placement
   * then starts again. A batch never returns to `#pending` once it leaves and no join is published
   * while another is prepared, so each new placement relies on fewer pending batches, and the
   * placements end.
   * @param {JoinStep[]} steps
   * @returns {boolean}
   */
  #moved(steps) {
    for (const { found } of steps) {
      if (found !== null && this.#pending.get(found.key) !== found) {
        return true
      }
    }
    return false
  }

  /**
   * The runs a send's assigned records join, read before anything of the join changes: per entry
   * the chunk capacity of its topic and codec, its records cut into runs that fit it (`chunksOf`),
   * and each run measured (`JoinStep#measure`). This is the only step of a join that reads a
   * record.
   * @param {{ topic: string, partition: number, messages: object[] }[]} entries
   * @param {number} [compression]
   * @returns {JoinStep[]} in the order of `entries`, each entry's runs in order
   * @throws {*} whatever `chunkCapacity` or reading a record throws
   */
  #measure(entries, compression) {
    const steps = []
    for (const { topic, partition, messages } of entries) {
      const capacity = this.#budget.chunkCapacity(topic, compression)
      const bytes = recordsBytes(messages)
      const first = steps.length
      if (bytes <= capacity) {
        steps.push(new JoinStep(topic, partition, messages, bytes, 0, capacity))
      } else {
        chunksOf(topic, partition, messages, capacity, steps)
      }
      for (let i = first; i < steps.length; i++) {
        steps[i].measure()
      }
      steps[steps.length - 1].last = true
    }
    return steps
  }

  /**
   * Chooses each run's batch as publication will leave it, changing none: its partition's pending
   * batch, or a new shell when there is none or when the pending one has other options or would
   * pass `batchSize` or the run's capacity (that one is displaced). Makes the shells and settlers,
   * and decides which batches are full: every run's but the last of its partition, one at
   * `batchSize`, every one when `hurried`, and any while `lingers()` is false, which is asked for
   * each last run whose batch is not full otherwise. Each pending batch it reads is kept as the
   * run's `found`, for `#moved`.
   * @param {JoinStep[]} steps
   * @param {{ options: string, acks: number, timeout?: number, compression?: number,
   *   createdAt: number, dueAt: number }} shape
   * @param {AppendedSend} send
   * @param {PendingBatch[]} full takes the batches that leave in the join's hand-off
   * @param {JoinStep[]} joined takes, per partition, the last run whose batch stays pending
   * @param {boolean} hurried a flush began while the join was prepared: every batch is full
   * @returns {void}
   * @throws {*} whatever `lingers` throws
   */
  #place(steps, shape, send, full, joined, hurried) {
    /** @type {?Map<string, ?JoinStep>} per partition, its last run on a batch left open, or null */
    const open = steps.length > 1 ? new Map() : null
    for (const step of steps) {
      const previous = open?.get(step.key)
      let batch
      let filled = 0
      let wired = 0
      let length = 0
      if (previous === undefined) {
        batch = this.#pending.get(step.key)
        if (batch !== undefined) {
          step.found = batch
          filled = batch.bytes
          wired = batch.wire
          length = batch.messages.length
        }
      } else if (previous !== null) {
        batch = previous.batch
        filled = previous.filled
        wired = previous.wired
        length = previous.length
      }
      if (
        batch !== undefined &&
        (batch.options !== shape.options ||
          filled + step.bytes > this.#batchSize ||
          wired + step.wire > step.capacity)
      ) {
        step.displaced = batch
        batch = undefined
      }
      if (batch === undefined) {
        batch = this.#shell(step, shape)
        step.fresh = true
        filled = 0
        wired = 0
        length = 0
      }
      step.batch = batch
      step.settler = new Settler(send, length, step.origin)
      step.filled = filled + step.bytes
      step.wired = wired + step.wire
      step.length = length + step.messages.length
      step.full = !step.last || step.filled >= this.#batchSize || hurried || !this.#lingers()
      if (step.full) {
        full.push(batch)
      }
      open?.set(step.key, step.full ? null : step)
    }
    for (const step of open === null ? steps : open.values()) {
      if (step !== null && !step.full) {
        joined.push(step)
      }
    }
  }

  /**
   * Decides, before anything of the join is published, what happens to the timer of each batch
   * left pending: in a join that is due, a batch not yet due becomes due and loses its timer;
   * a new shell gets its merge key (`leaderOf` is asked here) and a timer for its `linger`, fired
   * no later than its delivery deadline; a pending batch an older send joins has its timer moved
   * to the earlier deadline - a lingering one still fires no later than its original `linger`,
   * which is not restarted, and a held one goes out at the deadline. Each timer is planned as a
   * `cap` and an absolute `expiresAt`, and its delay is taken from the clock when it is set
   * (`#setTimer`), so time a callback takes here never pushes it past either.
   * @param {JoinStep[]} joined each partition's last run whose batch stays pending
   * @param {boolean} due whether the join hands its batches on now
   * @param {number} createdAt when the send's public call began
   * @returns {void}
   */
  #plan(joined, due, createdAt) {
    for (const step of joined) {
      const { batch } = step
      if (!batch.due && due) {
        step.hurry = true
      } else if (!batch.due && batch.timer === null) {
        // Only a new shell is pending, not due and without a timer.
        batch.mergeKey = this.#mergeKeyOf(batch)
        step.armed = true
        step.cap = this.#linger
        if (this.#deadline !== undefined) {
          step.expiresAt = batch.createdAt + this.#deadline
        }
      } else if (
        createdAt < batch.createdAt &&
        batch.timer !== null &&
        this.#deadline !== undefined
      ) {
        const deadlineAt = createdAt + this.#deadline
        step.armed = true
        step.expiresAt = batch.due ? deadlineAt : Math.min(batch.dueAt, deadlineAt)
      }
    }
  }

  /**
   * A new batch for a run, not linked yet: publication links it and debits its charge.
   * @param {JoinStep} step
   * @param {{ options: string, acks: number, timeout?: number, compression?: number,
   *   createdAt: number, dueAt: number }} shape
   * @returns {PendingBatch}
   */
  #shell({ key, topic, partition }, { options, acks, timeout, compression, createdAt, dueAt }) {
    const batch = {
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
      wire: 0,
      createdAt,
      dueAt,
      mergeKey: null,
      timer: null,
      due: false,
      heldBytes: 0,
      debt: BATCH_DEBIT,
      owners: [],
      retire: null,
      detach: null,
    }
    batch.ownership = new BatchOwnership(this.#admission, batch, false)
    batch.retire = batch.ownership.retire
    batch.detach = batch.ownership.detach
    return batch
  }

  /**
   * Publishes a prepared join in one step that only links and counts: per run it moves the batch
   * the run displaced from `#pending` to `#outbox`, alone, links a new shell and debits its
   * charge, registers the run's settler with its send, adds the run's records, bytes and shares to
   * its batch and its earlier start when it brings one, and takes a full batch out of `#pending`
   * or makes a batch due as `#plan` decided. When the join fills a batch, the due batches not held
   * then leave `#pending` too (`#sweepDue`), and they and the full ones enter `#outbox` together.
   * It reads no record, sets or clears no timer and calls nothing a caller or the producer
   * supplied; the outbox entries, trackers and the arrays and maps it grows are all it allocates.
   * @param {JoinStep[]} steps
   * @param {PendingBatch[]} full the batches the join fills; takes the due batches going with them
   * @param {PendingSend} holder
   * @param {number} createdAt when the send's public call began
   * @param {boolean} holding what `holds()` answered while the join was prepared
   * @returns {?PendingBatch[]} the due batches held behind their partition's batch in flight, which
   *   have a delivery deadline and no timer yet
   */
  #publish(steps, full, holder, createdAt, holding) {
    for (const step of steps) {
      const { batch, displaced } = step
      if (displaced !== null) {
        this.#pending.delete(displaced.key)
        this.#handOn([displaced])
      }
      if (step.fresh) {
        this.#admission.debit(BATCH_DEBIT)
        this.#pending.set(batch.key, batch)
      }
      if (createdAt < batch.createdAt) {
        batch.createdAt = createdAt
      }
      step.settler.register()
      batch.settlers.push(step.settler)
      for (const message of step.messages) {
        batch.messages.push(message)
      }
      batch.bytes += step.bytes
      batch.wire += step.wire
      batch.heldBytes += step.shares
      batch.owners.push(holder, step.shares)
      holder.share(step.shares)
      if (step.full) {
        this.#pending.delete(batch.key)
      } else if (step.hurry) {
        batch.due = true
      }
    }
    if (full.length === 0) {
      return null
    }
    const held = this.#sweepDue(holding, full)
    this.#handOn(full)
    return held
  }

  /**
   * The timer work of a published join, after publication: clears the timers of the batches it
   * moved to `#outbox` and of those it made due or whose timer it moves, then sets the timers
   * `#plan` decided (`#setTimer`).
   * @param {JoinStep[]} steps
   * @param {JoinStep[]} joined each partition's last run whose batch stays pending
   * @param {PendingBatch[]} full the batches the join's hand-off carries
   * @returns {void}
   */
  #arm(steps, joined, full) {
    for (const { displaced } of steps) {
      if (displaced !== null) {
        clearTimeout(displaced.timer)
      }
    }
    for (const batch of full) {
      clearTimeout(batch.timer)
    }
    for (const { hurry, armed, batch } of joined) {
      if (hurry || armed) {
        clearTimeout(batch.timer)
        batch.timer = null
      }
    }
    for (const { armed, batch, cap, expiresAt } of joined) {
      if (armed) {
        this.#setTimer(batch, cap, expiresAt)
      }
    }
  }

  /**
   * Sets a pending batch's one timer: a due batch goes out when it fires (`#flushBatch`), any
   * other batch's `linger` runs out (`#expire`). It fires after `cap` ms and no later than
   * `expiresAt`, read against the clock now, when it is set; at most 2147483647 ms on.
   * Creating a timer can call an application's hook, which may close or flush the producer:
   * nothing is set for a batch that is no longer pending or already has a timer, and a timer whose
   * creation let that happen is cleared at once.
   * @param {PendingBatch} batch
   * @param {number} cap ms
   * @param {number} expiresAt epoch ms, or Infinity
   * @returns {void}
   */
  #setTimer(batch, cap, expiresAt) {
    if (this.#pending.get(batch.key) !== batch || batch.timer !== null) {
      return
    }
    const bound = expiresAt === Infinity ? cap : Math.min(cap, expiresAt - Date.now())
    const delay = Math.min(MAX_DELAY, Math.max(0, bound))
    const timer = batch.due
      ? setTimeout(() => this.#flushBatch(batch), delay)
      : setTimeout(() => this.#expire(batch), delay)
    if (this.#pending.get(batch.key) === batch && batch.timer === null) {
      batch.timer = timer
    } else {
      clearTimeout(timer)
    }
  }

  /**
   * How many batches wait for a partition: its pending batch and the batches handed on and not
   * settled yet, those in `#outbox` included - the queue an adaptive partitioner weighs. Reads two maps by one `topic/partition`
   * key, only when an adaptive partitioner picks.
   * @param {string} topic
   * @param {number} partition
   * @returns {number}
   */
  depthOf(topic, partition) {
    const key = `${topic}/${partition}`
    return (this.#pending.has(key) ? 1 : 0) + (this.#unsettled.get(key) ?? 0)
  }

  /**
   * Hands every pending batch to the send path now - in one call per `acks`, `timeout` and
   * `compression` - and clears their timers; first, in order, the batches already waiting in
   * `#outbox`, which are older than the pending ones of their partitions. Every one of them is in
   * `#outbox` before the first request begins, so a request that closes or flushes the producer
   * meanwhile finds the rest there. A join being prepared meanwhile hands its batches on at once.
   * @returns {Promise<void>} settles once every batch it handed on or found waiting has settled,
   *   whichever call began its request; a batch a forced close refused counts as settled. Never
   *   rejects
   */
  async flush() {
    this.#flushes += 1
    const batches = [...this.#pending.values()]
    batches.forEach((batch) => clearTimeout(batch.timer))
    this.#pending.clear()
    this.#handOn(batches)
    const outgoing = [...this.#outbox]
    this.#dispatch()
    await Promise.all(outgoing.map(({ sent }) => sent))
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
      this.#outbox.size === 0 &&
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
   *   a node id, or threw - a leader that cannot be read only keeps the batch from merging, and
   *   never refuses the send whose join asks for it
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
   * per options; then a held one with a delivery deadline is timed to go out at it
   * (`#timeHeld`). `holds()` is asked once, before any batch is taken, so nothing runs between
   * taking a batch out of `#pending` and putting it in `#outbox`.
   * @returns {void}
   */
  #handOnDue() {
    const ready = []
    const held = this.#sweepDue(this.#holds(), ready)
    for (const batch of ready) {
      clearTimeout(batch.timer)
    }
    this.#handOn(ready)
    this.#dispatch()
    this.#timeHeld(held)
  }

  /**
   * Takes every due batch that is not held out of `#pending`, onto `taken`, and calls nothing:
   * held means that `holding` is true and its partition has a batch handed on and not settled,
   * one waiting in `#outbox` included.
   * @param {boolean} holding what `holds()` answered
   * @param {PendingBatch[]} taken
   * @returns {?PendingBatch[]} the held due batches that have a delivery deadline and no timer
   */
  #sweepDue(holding, taken) {
    /** @type {?PendingBatch[]} */
    let held = null
    for (const batch of this.#pending.values()) {
      if (!batch.due) {
        continue
      }
      if (!holding || !this.#unsettled.has(batch.key)) {
        this.#pending.delete(batch.key)
        taken.push(batch)
      } else if (this.#deadline !== undefined && batch.timer === null) {
        held ??= []
        held.push(batch)
      }
    }
    return held
  }

  /**
   * Times each held due batch to go out at its delivery deadline (`#setTimer`).
   * @param {?PendingBatch[]} held
   * @returns {void}
   */
  #timeHeld(held) {
    if (held === null) {
      return
    }
    for (const batch of held) {
      this.#setTimer(batch, Infinity, batch.createdAt + this.#deadline)
    }
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
   * A due batch's timer fired: it goes out alone, if it is still pending.
   * @param {PendingBatch} batch
   * @returns {void}
   */
  #flushBatch(batch) {
    if (this.#pending.get(batch.key) !== batch) {
      return
    }
    this.#pending.delete(batch.key)
    batch.timer = null
    this.#handOn([batch])
    this.#dispatch()
  }

  /**
   * Puts batches just taken out of `#pending` in `#outbox`, one entry per `acks`, `timeout` and
   * `compression`, in the order of their first batch, and counts each as handed on and unsettled
   * from now, with one more settler that ends that count when it settles. Batches that share
   * their options - every hand-off of a send that asked for one set of them - make one entry as
   * they are, with no grouping. Calls nothing.
   * @param {PendingBatch[]} batches nothing else changes the array afterwards
   * @returns {void}
   */
  #handOn(batches) {
    if (batches.length === 0) {
      return
    }
    for (const batch of batches) {
      const { key } = batch
      this.#unsettled.set(key, (this.#unsettled.get(key) ?? 0) + 1)
      batch.settlers.push(
        new BatchTracker(
          () => this.#unsettled.set(key, (this.#unsettled.get(key) ?? 0) + 1),
          () => this.#settled(key)
        )
      )
    }
    let shared = true
    for (let i = 1; i < batches.length && shared; i++) {
      shared = batches[i].options === batches[0].options
    }
    if (shared) {
      this.#outbox.push({ batches, sent: null })
      return
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
    for (const group of groups.values()) {
      this.#outbox.push({ batches: group, sent: null })
    }
  }

  /**
   * Sends what waits in `#outbox`, oldest entry first, taking each entry out only as its request
   * begins. A request that flushes or closes the producer meanwhile hands on or refuses the rest
   * itself, in the same order, and this loop then finds nothing left.
   * @returns {void}
   */
  #dispatch() {
    for (let entry = this.#outbox.shift(); entry !== undefined; entry = this.#outbox.shift()) {
      entry.sent = this.#send(entry.batches)
    }
  }

  /**
   * Sends batches that share their options. A failure of the call itself - one that settled no
   * batch, whether the send path threw or rejected - rejects every settler that is still waiting,
   * so a hand-off never throws into the join or timer that made it. A send path that throws took
   * none of the batches, so they retire here; one that rejects took them and retires them itself.
   * @param {PendingBatch[]} batches
   * @returns {Promise<void>} never rejects
   */
  #send(batches) {
    const [{ acks, timeout, compression }] = batches
    let sent
    try {
      sent = this.#sendPartitioned({ acks, timeout, compression, batches })
    } catch (error) {
      batches.forEach((batch) => batch.retire())
      sent = Promise.reject(error)
    }
    return sent.then(
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
