const Long = require('../../utils/long')
const Fifo = require('../../utils/fifo')
const {
  KafkaJSDeliveryTimeout,
  KafkaJSInvalidConfigurationError,
  KafkaJSRecordTooLargeError,
  KafkaJSProducerClosedError,
} = require('../../errors')
const { Deadline, ExpiryTimer } = require('../deadline')
const { Admission } = require('./admission')
const {
  chargeOf,
  share,
  ownedBuffer,
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
 * A copy of a record's headers, each own key read once: an array value is copied too, and every
 * Buffer is kept as `ownedBuffer` keeps it.
 * @param {*} headers
 * @returns {object}
 */
const ownedHeaders = (headers) => {
  const copy = { ...headers }
  for (const name of Object.keys(copy)) {
    const header = copy[name]
    copy[name] = Array.isArray(header) ? header.map(ownedBuffer) : ownedBuffer(header)
  }
  return copy
}

/**
 * The producer's own copy of a record, each field read once: its own enumerable fields, so a
 * custom partitioner still sees them, and the fields the producer reads, also when they come from
 * a prototype. A header container, a header-value array, a `Date` and a `Long` are copied, so
 * changing them after the call changes nothing; the bytes of a Buffer are shared, unless it is a
 * small view of a large backing store (`ownedBuffer`). Anything that is not an object is kept, so
 * validation fails it as before.
 * @param {*} message
 * @returns {*}
 */
const ownedRecord = (message) => {
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
  if (Buffer.isBuffer(record.key)) {
    record.key = ownedBuffer(record.key)
  }
  if (Buffer.isBuffer(record.value)) {
    record.value = ownedBuffer(record.value)
  }
  if (record.headers != null) {
    record.headers = ownedHeaders(record.headers)
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
 * Cuts one partition's records of a send at record boundaries into runs that each fit a batch of
 * `capacity` wire bytes, greedily, in order; no record is above `capacity` (`append` refused it).
 * @param {object[]} messages
 * @param {number} capacity
 * @returns {{ messages: object[], wire: number, origin: number }[]} `origin` is the position of
 *   a run's first record among `messages`
 */
const chunksOf = (messages, capacity) => {
  const chunks = []
  let start = 0
  let bytes = 0
  for (let index = 0; index < messages.length; index++) {
    const size = wire(messages[index])
    if (index > start && bytes + size > capacity) {
      chunks.push({ messages: messages.slice(start, index), wire: bytes, origin: start })
      start = index
      bytes = 0
    }
    bytes += size
  }
  chunks.push({ messages: messages.slice(start), wire: bytes, origin: start })
  return chunks
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
  let error
  const end = () => {
    pending -= 1
    if (pending === 0) {
      if (error !== undefined) settler.reject(error)
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
        error = error ?? failure
        end()
      },
    }
  }
  return [half(true), half(false)]
}

/**
 * One `append()`: settles once every share of its records - one per batch carrying some of them -
 * has settled, resolving with one `RecordMetadata` per partition, by topic, then partition, or
 * rejecting with the first error any share failed with. A partition's metadata is that of the
 * share holding its first record, whatever order the answers came in. The send counts as one
 * more share until `joined()`, so no share can settle it while its records are still joining.
 * Settling it ends its holder's place in the flush inventory (`PendingSend#untrack`).
 */
class AppendedSend {
  #pending = 1
  /** @type {?object[]} created by the first metadata, so a held send keeps no array */
  #metadata = null
  #error = null
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
    this.#settleOne()
  }

  /**
   * One share failed.
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
      this.#resolve((this.#metadata ?? []).sort(byTopicThenPartition))
    }
    this.#holder.untrack()
    this.#holder = null
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
   * Counts itself as one share of `send`.
   * @param {AppendedSend} send
   * @param {number} index the position of the share's first record in the batch
   * @param {number} [origin=0] the position of that record among the send's records for the
   *   partition
   */
  constructor(send, index, origin = 0) {
    this.#send = send
    this.#index = index
    this.#origin = origin
    send.share()
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
        first.push(settler)
        second.push(new Settler(settler.#send, 0, settler.#origin + at - settler.#index))
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
   * @param {{ holdTopic(topic: string): void, releaseTopic(topic: string): void }} [pinner] the
   *   producer cluster's topic pins, which keep its topics' metadata while the shell is charged
   */
  constructor(records, deadline, pinner = NO_PINNER) {
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
   * @param {(send: PendingSend) =>
   *   Promise<{ topic: string, partition: number, messages: object[] }[]>} options.partition
   *   assigns every record its partition once; it reads the records from `send.records` after each
   *   await and stops when that is `null`
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
   * @param {number} [options.deadline] ms from a send's public call to its delivery deadline, the
   *   send path's own: a send still waiting for its partitions settles with `KafkaJSDeliveryTimeout`
   *   when it is reached, and a held batch goes out then
   * @param {(error: Error) => Error} [options.expired] called with the error of a send that
   *   expired before it joined a batch, or that the buffer refused, to arm a transaction; returns
   *   the error to reject with
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
    this.#linger = linger
    this.#batchSize = batchSize
    this.#partition = partition
    this.#sendPartitioned = sendPartitioned
    this.#lingers = lingers
    this.#holds = holds
    this.#leaderOf = leaderOf
    this.#deadline = deadline
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
   * store, which is copied. Entries and records that are not objects are kept as they are, and so
   * is a `messages` that is not an array, so validation refuses them as before.
   * @param {{ topic: string, messages: object[] }[]} topicMessages
   * @returns {{ topic: string, messages: object[] }[]}
   */
  static snapshot(topicMessages) {
    return topicMessages.map((entry) => {
      if (entry === null || typeof entry !== 'object') {
        return entry
      }
      const { topic, messages } = entry
      return { topic, messages: Array.isArray(messages) ? messages.map(ownedRecord) : messages }
    })
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
   * arms a transaction and the send rejects with the refusal itself.
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
    this.track(send)
    try {
      this.#assertFits(topicMessages, compression)
      const charge = chargeOf(topicMessages, this.#admission.capacity)
      const granted = this.#admission.acquire(charge.total, send.deadline)
      send.admit(granted instanceof Promise ? this.#granted(await granted) : granted, charge)
    } catch (error) {
      send.records = null
      send.releaseTopics()
      send.untrack()
      this.#end()
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
   */
  #granted(reservation) {
    if (this.#getGeneration()?.closed) {
      reservation.releaseAll()
      throw new KafkaJSProducerClosedError('The producer was closed before admission completed', {
        outcome: 'unsent',
      })
    }
    return reservation
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
   * @returns {void}
   */
  forceClose() {
    for (let send = this.#holders.peek(); send !== undefined; send = this.#holders.peek()) {
      this.#settleHolder(
        send,
        new KafkaJSProducerClosedError('The producer was closed before the send was partitioned', {
          outcome: 'unsent',
        })
      )
    }
    for (const batch of this.#pending.values()) {
      clearTimeout(batch.timer)
      const error = new KafkaJSProducerClosedError('The producer was closed before sending', {
        outcome: 'unsent',
      })
      batch.settlers.forEach((settler) => settler.reject(error))
      batch.retire()
    }
    this.#pending.clear()
    this.#checkIdle()
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
   * Holds a send while its records are assigned their partitions, then joins them to batches. The
   * send settles at its deadline even while the assignment never returns: its records are dropped,
   * and what the assignment does afterwards is ignored.
   * @param {PendingSend} send
   * @param {{ acks: number, timeout?: number, compression?: number }} options
   * @returns {Promise<object[]>} the send's outcome, as `append()` describes it
   */
  #hold(send, options) {
    return new Promise((resolve, reject) => {
      send.reject = reject
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
          if (this.#leave(send)) {
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
        },
        (error) => {
          send.unpin()
          if (this.#leave(send)) {
            send.release()
            reject(error)
            send.untrack()
            this.#end()
          }
        }
      )
    })
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
    send.reject = null
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
      this.#settleHolder(send, this.#timedOut(send))
    }

    const next = this.#holders.peek()
    return next === undefined ? Infinity : next.deadline.dueAt
  }

  /**
   * Ends a registered send's wait with `error`, through `expired`; its shell stays charged until
   * its pins end.
   * @param {PendingSend} send
   * @param {Error} error
   * @returns {void}
   */
  #settleHolder(send, error) {
    const { reject } = send
    this.#leave(send)
    send.release()
    reject(this.#expired(error))
    send.untrack()
    this.#end()
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
   * @param {{ topic: string, partition: number, messages: object[] }[]} entries
   * @param {{ acks: number, timeout?: number, compression?: number, createdAt: number }} options
   * @param {PendingSend} holder the send whose records these are
   * @returns {Promise<object[]>} the append's outcome, as `append()` describes it
   */
  #join(entries, { acks, timeout, compression, createdAt }, holder) {
    let send
    const appended = new Promise((resolve, reject) => {
      send = new AppendedSend(holder, resolve, reject)
    })
    const options = `${acks}/${timeout}/${compression}`
    // One due time for every batch this append creates, so they fall due together.
    const dueAt = Date.now() + this.#linger
    /** @type {PendingBatch[]} the batches this append fills, out of the pending ones */
    const full = []
    /** @type {PendingBatch[]} the other batches carrying its records, still pending */
    const joined = []
    for (const { topic, partition, messages } of entries) {
      const capacity = this.#budget.chunkCapacity(topic, compression)
      const bytes = recordsBytes(messages)
      const chunks =
        bytes <= capacity ? [{ messages, wire: bytes, origin: 0 }] : chunksOf(messages, capacity)
      chunks.forEach((chunk, index) => {
        const batch = this.#add(topic, partition, chunk, capacity, holder, send, {
          options,
          acks,
          timeout,
          compression,
          createdAt,
          dueAt,
        })
        if (index < chunks.length - 1 || batch.bytes >= this.#batchSize || !this.#lingers()) {
          clearTimeout(batch.timer)
          this.#pending.delete(batch.key)
          full.push(batch)
        } else {
          joined.push(batch)
        }
      })
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
        batch.timer = setTimeout(() => this.#expire(batch), this.#lingerOf(batch))
      }
    }
    if (full.length > 0) {
      this.#handOnDue(full)
    } else if (this.#linger === 0) {
      this.#queueHandOff()
    }
    send.joined()
    return appended
  }

  /**
   * Adds one chunk of a send's records to its partition's pending batch, or to a new one when the
   * pending batch has other options or would pass `batchSize` or `capacity`, which hands that one
   * on first.
   * @param {string} topic
   * @param {number} partition
   * @param {{ messages: object[], wire: number, origin: number }} chunk
   * @param {number} capacity the chunk capacity of the topic and codec
   * @param {PendingSend} holder
   * @param {AppendedSend} send
   * @param {{ options: string, acks: number, timeout?: number, compression?: number,
   *   createdAt: number, dueAt: number }} shape
   * @returns {PendingBatch} the batch now carrying the chunk, still pending
   */
  #add(topic, partition, { messages, wire: wireBytes, origin }, capacity, holder, send, shape) {
    const { options, acks, timeout, compression, createdAt, dueAt } = shape
    const key = `${topic}/${partition}`
    const bytes = sizeOf(messages)
    let batch = this.#pending.get(key)
    if (
      batch !== undefined &&
      (batch.options !== options ||
        batch.bytes + bytes > this.#batchSize ||
        batch.wire + wireBytes > capacity)
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
      this.#admission.debit(BATCH_DEBIT)
      this.#pending.set(key, batch)
    }

    if (createdAt < batch.createdAt) {
      batch.createdAt = createdAt
      this.#rearm(batch)
    }
    batch.settlers.push(new Settler(send, batch.messages.length, origin))
    let shares = 0
    for (const message of messages) {
      batch.messages.push(withEncodableTimestamp(message))
      shares += share(message)
    }
    batch.bytes += bytes
    batch.wire += wireBytes
    batch.heldBytes += shares
    batch.owners.push(holder, shares)
    holder.share(shares)
    return batch
  }

  /**
   * How long a batch waits for more records: its `linger`, or less when its delivery deadline
   * comes first, so the send path sees it by then.
   * @param {PendingBatch} batch
   * @returns {number} ms
   */
  #lingerOf(batch) {
    return this.#deadline === undefined
      ? this.#linger
      : Math.max(0, Math.min(this.#linger, batch.createdAt + this.#deadline - Date.now()))
  }

  /**
   * Moves a batch's timer to its delivery deadline after an older send joined it and brought that
   * deadline forward. A lingering batch still waits no longer than its original `linger` limit,
   * which is not restarted; a held batch goes out at the deadline. A batch with no timer is left
   * to the code that arms one.
   * @param {PendingBatch} batch
   * @returns {void}
   */
  #rearm(batch) {
    if (batch.timer === null || this.#deadline === undefined) {
      return
    }

    clearTimeout(batch.timer)
    const deadlineAt = batch.createdAt + this.#deadline
    batch.timer = batch.due
      ? setTimeout(() => this.#flushBatch(batch), Math.max(0, deadlineAt - Date.now()))
      : setTimeout(
          () => this.#expire(batch),
          Math.max(0, Math.min(batch.dueAt, deadlineAt) - Date.now())
        )
  }

  /**
   * How many batches wait for a partition: its pending batch and the batches handed on and not
   * settled yet - the queue an adaptive partitioner weighs. Reads two maps by one `topic/partition`
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
      batch.settlers.push(
        new BatchTracker(
          () => this.#unsettled.set(key, (this.#unsettled.get(key) ?? 0) + 1),
          () => this.#settled(key)
        )
      )
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
