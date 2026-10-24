const { randomBytes } = require('crypto')
const { isDeepStrictEqual } = require('util')
const Broker = require('../../../broker')
const Cluster = require('../../../cluster')
const COORDINATOR_TYPES = require('../../../protocol/coordinatorTypes')
const ISOLATION_LEVEL = require('../../../protocol/isolationLevel')
const Accumulator = require('../../state/accumulator')
const { Admission } = require('../../state/admission')
const Generation = require('../../generation')
const { Kafka, CompressionTypes, ConfigResourceTypes, logLevel } = require('../../../../index')
const { readLog, committedRecords } = require('../../../../testHelpers/rawLog')
const { createConnectionPool, createTopic, newLogger, plainTextBrokers } = require('testHelpers')
const { NEVER, STOP_GRACE_MS, bounded, delay, liveRun } = require('./bounded')

/** The most one probe record carries, an independent conservative bound. */
const MAX_PROBE_RECORD_BYTES = 32768
/** The `max.message.bytes` the probe topic is created with and read back at. */
const PROBE_TOPIC_LIMIT = 1048576
/** The smallest record a probe layout keeps, so the record can hold its id. */
const MIN_PROBE_RECORD_BYTES = 24
/** What a probe send costs beside its records, and what each record's batch is debited. */
const SEND_SHELL = 1536
const BATCH_DEBIT = 1024
/** A record's fixed charge: 21 overhead, 64 held, one byte for the null key, one for no headers. */
const RECORD_FIXED = 87
/** A request's per-partition framing and batch header, which the largest request carries once. */
const PARTITION_FRAMING = 16
const BATCH_HEADER = 61
/** Every probe record carries this timestamp, so no clock reading changes a size. */
const PROBE_TIMESTAMP = 1700000000000
const ID_BYTES = 13
const QUIET_BUDGET = { attempts: 100, delay: 100 } // 10 s
/** What `ResourceLedger#facts` reports when the producer retains nothing. */
const NO_RETAINED_RESOURCES = Object.freeze({
  pins: {},
  sends: [],
  epochs: [],
  registrations: 0,
  chunks: [],
})

/**
 * @param {string|null|undefined} text
 * @returns {number} its UTF-8 bytes; 0 for an absent one
 */
const utf8Bytes = (text) => (text == null ? 0 : Buffer.byteLength(text))

/**
 * The bytes of the zigzag varint of a non-negative length, derived from the encoding itself.
 * @param {number} length
 * @returns {number} from 1 to 5
 */
const varintSize = (length) => {
  let encoded = length * 2
  let size = 1
  while (encoded >= 128) {
    encoded = Math.floor(encoded / 128)
    size += 1
  }
  return size
}

/**
 * The independent layout of the closing admission probe: the fewest records of at most `limit`
 * bytes that fill `capacity` with the send's charge and every record's batch debit, and the
 * arithmetic behind it. The sizes are derived here, never from the producer's own accounting.
 * @param {object} budget
 * @param {number} budget.capacity the producer's `bufferMemory`
 * @param {number} budget.maxRequestSize the producer's `maxRequestSize`
 * @param {string} budget.clientId
 * @param {string} [budget.transactionalId]
 * @param {string} budget.topic the probe topic
 * @param {number} [budget.topicMaxMessageBytes=1048576] the probe topic's read-back limit
 * @returns {{ capacity: number, maxRequestSize: number, framing: number, topicFraming: number,
 *   room: number, limit: number, count: number, lengths: number[], charge: number,
 *   overhead: number, payload: number, residualSlack: number }} `charge` is the bytes the send is
 *   admitted for, `payload` the sum of the record values, `overhead` is `capacity - payload`
 * @throws {Error} when the identifiers leave no room, or no positive record fits
 */
const probeLayout = ({
  capacity,
  maxRequestSize,
  clientId,
  transactionalId,
  topic,
  topicMaxMessageBytes = PROBE_TOPIC_LIMIT,
}) => {
  const framing = 32 + utf8Bytes(clientId) + utf8Bytes(transactionalId)
  const topicFraming = 32 + utf8Bytes(topic)
  if (framing + topicFraming > 4096) {
    throw new Error(`The probe identifiers take ${framing + topicFraming} bytes of framing`)
  }
  const room = maxRequestSize - framing - topicFraming - PARTITION_FRAMING - BATCH_HEADER
  const wire = (length) => length + 23 + varintSize(length)
  let limit = MAX_PROBE_RECORD_BYTES
  while (limit > 0 && (wire(limit) > room || BATCH_HEADER + wire(limit) > topicMaxMessageBytes)) {
    limit -= 1
  }
  if (limit < MIN_PROBE_RECORD_BYTES)
    throw new Error(`No probe record fits a request of ${room} bytes`)

  const perRecord = (length) => length + varintSize(length) + RECORD_FIXED + BATCH_DEBIT
  const budget = capacity - SEND_SHELL
  const count = Math.ceil(budget / perRecord(limit))
  const lengths = Array.from({ length: count - 1 }, () => limit)
  const tailRoom = budget - lengths.length * perRecord(limit) - RECORD_FIXED - BATCH_DEBIT
  let tail = Math.min(limit, tailRoom)
  while (tail > 0 && tail + varintSize(tail) > tailRoom) tail -= 1
  if (tail >= MIN_PROBE_RECORD_BYTES) lengths.push(tail)
  if (lengths.length === 0) throw new Error(`bufferMemory ${capacity} holds no probe record`)

  const payload = lengths.reduce((sum, length) => sum + length, 0)
  const charge =
    SEND_SHELL +
    lengths.reduce((sum, length) => sum + length + varintSize(length) + RECORD_FIXED, 0)
  return {
    capacity,
    maxRequestSize,
    framing,
    topicFraming,
    room,
    limit,
    count: lengths.length,
    lengths,
    charge,
    overhead: capacity - payload,
    payload,
    residualSlack: capacity - (charge + BATCH_DEBIT * lengths.length),
  }
}

/** What each cleanup behind the running one keeps of the stack's budget, at most. */
const CLEANUP_RESERVE_MS = 250

/**
 * Runs cleanups in reverse order of registration, each one whatever the others did, and keeps the
 * primary failure ahead of any cleanup failure. Given a budget, the awaiting code ends inside it:
 * each cleanup may take what is left less a reserve for those behind it, its stop grace included,
 * and a cleanup that outlasts that is aborted, given the grace to stop, and reported as failed.
 * Every failed cleanup taints the run (`liveRun`). A cleanup that ignores its abort, such as a
 * producer's own `disconnect`, may still finish after the stack returned; containing it is the
 * guard's.
 */
class CleanupStack {
  /** Starts empty; nothing runs until `run`. */
  constructor() {
    /** @type {Array<{ name: string, close: (signal: AbortSignal) => (void|Promise<void>) }>} */
    this.cleanups = []
  }

  /**
   * @param {string} name what the cleanup closes, for its failure message
   * @param {(signal: AbortSignal) => (void|Promise<void>)} close should stop what it started when
   *   the signal aborts, which is when its time ran out; one that ignores the signal, as a
   *   producer's own `disconnect` does, is supported: it is reported as failed and taints the run
   * @returns {void}
   */
  add(name, close) {
    this.cleanups.push({ name, close })
  }

  /**
   * Runs `work`, then every cleanup. A failure of `work` stays the thrown error, with the cleanup
   * failures attached as `cleanupErrors`; without one, cleanup failures are thrown together.
   * @template T
   * @param {() => Promise<T>} work
   * @param {object} [options]
   * @param {number} [options.budgetMs] the most the cleanups may take together; unbounded when
   *   absent
   * @returns {Promise<T>}
   * @throws {Error} `work`'s own error, or an `AggregateError` of the failed cleanups
   */
  async run(work, { budgetMs } = {}) {
    let result
    try {
      result = await work()
    } catch (primary) {
      const failures = await this.closeAll({ budgetMs })
      if (primary !== null && typeof primary === 'object') primary.cleanupErrors = failures
      throw primary
    }
    const failures = await this.closeAll({ budgetMs })
    if (failures.length > 0) throw new AggregateError(failures, 'Cleanup failed')
    return result
  }

  /**
   * @param {object} [options]
   * @param {number} [options.budgetMs] see `run`
   * @returns {Promise<Error[]>} what each failed or cut-off cleanup threw, named by what it closes
   */
  async closeAll({ budgetMs = Infinity } = {}) {
    const deadline = performance.now() + budgetMs
    const cleanups = this.cleanups.splice(0).reverse()
    const reserve = Math.min(CLEANUP_RESERVE_MS, budgetMs / (2 * cleanups.length))
    const failures = []
    for (const [index, { name, close }] of cleanups.entries()) {
      const behind = cleanups.length - index - 1
      const allowance = Math.max(0, deadline - performance.now() - reserve * behind)
      const graceMs = Math.min(STOP_GRACE_MS, allowance / 2)
      try {
        await bounded(NEVER, allowance - graceMs, close, { label: 'The cleanup', graceMs })
      } catch (error) {
        const failure = Object.assign(new Error(`${name}: ${error.message}`), { cause: error })
        liveRun.taint(failure)
        failures.push(failure)
      }
    }
    return failures
  }
}

/**
 * One call of `Broker#produce` as the producer made it.
 * @typedef {object} ProduceCall
 * @property {number} at `performance.now()` at the call
 * @property {string} producerId decimal
 * @property {number} producerEpoch
 * @property {number} acks
 * @property {Array<{ topic: string, partition: number, firstSequence: number|undefined,
 *   records: number, first: string }>} partitions the batches it carried; `first` is the start of
 *   the first record's value
 * @property {Array<{ topic: string, partition: number, errorCode: number, baseOffset: string }>|null} results
 *   the broker's answer, absent while pending and when the call failed
 * @property {Error|null} error what the call threw
 */

/**
 * Records every Produce request the producers of this process make and the answer each got, so
 * a scenario can compare what the broker said with what the log holds. The spy calls the original
 * with its own `this`.
 */
class ProduceLedger {
  /** Starts empty and not installed. */
  constructor() {
    /** @type {ProduceCall[]} */
    this.calls = []
    this.spy = null
  }

  /**
   * Spies on `Broker#produce` for every producer of this process. Call `restore` in an
   * unconditional finally.
   * @returns {this}
   */
  install() {
    const ledger = this
    const { produce } = Broker.prototype
    this.spy = jest.spyOn(Broker.prototype, 'produce').mockImplementation(async function (request) {
      const call = ledger.begin(request)
      try {
        const results = await produce.call(this, request)
        call.results = Array.isArray(results)
          ? results.map(({ topicName, partition, errorCode, baseOffset }) => ({
              topic: topicName,
              partition,
              errorCode,
              baseOffset,
            }))
          : null
        return results
      } catch (error) {
        call.error = error
        throw error
      }
    })
    return this
  }

  /**
   * Removes the spy; safe when it was never installed or is already removed.
   * @returns {void}
   */
  restore() {
    this.spy?.mockRestore()
    this.spy = null
  }

  /**
   * @param {object} request the arguments of `Broker#produce`
   * @returns {ProduceCall}
   */
  begin(request) {
    const call = {
      at: performance.now(),
      producerId: String(request.producerId),
      producerEpoch: request.producerEpoch,
      acks: request.acks,
      partitions: request.topicData.flatMap(({ topic, partitions }) =>
        partitions.map(({ partition, firstSequence, messages }) => ({
          topic,
          partition,
          firstSequence,
          records: messages.length,
          first: String(messages[0]?.value).slice(0, 40),
        }))
      ),
      results: null,
      error: null,
    }
    this.calls.push(call)
    return call
  }

  /**
   * @param {string} topic
   * @returns {ProduceCall[]} the calls that carried a batch of `topic`, oldest first
   */
  of(topic) {
    return this.calls.filter(({ partitions }) => partitions.some((entry) => entry.topic === topic))
  }
}

/**
 * Records the calls of one method while a scenario runs, and the error each one ended with. The
 * spy calls the original with its own `this` and changes nothing about its result.
 * @param {object} target the prototype that holds the method
 * @param {string} method
 * @param {(request: object) => object|null} describe what to keep of a call from its first
 *   argument; `null` leaves the call out
 * @returns {{ entries: Array<object>, restore: () => void }} each entry has `at`
 *   (`performance.now()`), what `describe` kept, and `error` (a described error or null)
 */
const traceCalls = (target, method, describe) => {
  const original = target[method]
  const entries = []
  const spy = jest.spyOn(target, method).mockImplementation(async function (...args) {
    const kept = describe(args[0])
    const entry = kept && { at: performance.now(), ...kept, error: null }
    if (entry) entries.push(entry)
    try {
      return await original.apply(this, args)
    } catch (error) {
      if (entry) entry.error = describeError(error)
      throw error
    }
  })
  return { entries, restore: () => spy.mockRestore() }
}

/**
 * @param {object} send a `PendingSend` an accumulator tracked
 * @returns {boolean} whether it still holds records, a rejection hook, pins, shares, payload or a
 *   place in a flush epoch: whether its shell and topics are not fully retired
 */
const retainsResources = (send) =>
  send.records !== null ||
  send.reject !== null ||
  send.topics !== null ||
  send.epoch !== null ||
  send.shares !== 0 ||
  send.pins !== 0 ||
  send.payload !== 0

/**
 * @param {{ holder: object, ownership: object, messages: object[] }} chunk a partition chunk, its
 *   holder being the batch its `BatchOwnership` owns and `messages` the record array it was born
 *   with, which every copy of the batch shares
 * @returns {boolean} whether its holder still carries its record array or its owners, or, for a
 *   subdivision child, whether the shared array still holds a record
 */
const retainsChunk = ({ holder, ownership, messages }) =>
  holder.messages !== null || holder.owners !== null || (ownership.child && messages.length !== 0)

/**
 * What a producer's retained resources did, recorded from the real instances its sends touched: the
 * topic pins held and released on its cluster, the sends its accumulators tracked with the flush
 * epochs they counted in and the settlement snapshots those accumulators handed out, the
 * partition chunks (batches and their `BatchOwnership` holders) from the moment each is born, and
 * the cancellations registered on the lifecycles it ran under. It only records, and `ProducerProbe`
 * feeds it from additive spies; nothing here changes what the producer does. Retired sends,
 * drained epochs and cleared chunks are forgotten when `facts` is read, so it holds only what is
 * still open.
 */
class ResourceLedger {
  /** Starts empty. */
  constructor() {
    /** @type {Map<string, number>} per topic, the holds minus the releases of the captured cluster */
    this.pins = new Map()
    /** @type {Map<object, Accumulator>} each tracked send still open, with its accumulator */
    this.sends = new Map()
    /** @type {Set<object>} the flush epochs those sends counted in */
    this.epochs = new Set()
    /** @type {Set<Accumulator>} every accumulator that tracked a send */
    this.accumulators = new Set()
    /** @type {Map<Accumulator, number>} the promises each accumulator's latest snapshot held */
    this.snapshots = new Map()
    /** @type {Map<Generation, Set<Function>>} the cancellations each lifecycle still holds */
    this.cancels = new Map()
    /** @type {Map<object, { holder: object, ownership: object, messages: object[] }>} by holder,
     *  each chunk not yet seen clean */
    this.chunks = new Map()
    this.totals = { held: 0, released: 0, tracked: 0, retired: 0, registered: 0, chunks: 0 }
  }

  /**
   * @param {string} topic one `Cluster#holdTopic` call
   * @returns {void}
   */
  hold(topic) {
    this.pins.set(topic, (this.pins.get(topic) ?? 0) + 1)
    this.totals.held += 1
  }

  /**
   * @param {string} topic one `Cluster#releaseTopic` call; one the cluster ignores still counts, so
   *   a release without a hold shows as a negative balance
   * @returns {void}
   */
  release(topic) {
    this.pins.set(topic, (this.pins.get(topic) ?? 0) - 1)
    this.totals.released += 1
  }

  /**
   * @param {Accumulator} accumulator
   * @param {object} send the `PendingSend` it has just counted in its current flush epoch
   * @returns {void}
   */
  track(accumulator, send) {
    this.sends.set(send, accumulator)
    this.accumulators.add(accumulator)
    this.epochs.add(send.epoch)
    this.totals.tracked += 1
  }

  /**
   * @param {Accumulator} accumulator one `unsettled()` call; ignored for an accumulator that
   *   tracked nothing
   * @param {number} size how many promises it returned
   * @returns {void}
   */
  snapshot(accumulator, size) {
    if (this.accumulators.has(accumulator)) this.snapshots.set(accumulator, size)
  }

  /**
   * @param {object} ownership a `BatchOwnership` just constructed. Its holder is the batch it owns,
   *   which the retire must clear however many copies of the batch were made; the record array is
   *   kept too, since the retire of a subdivision child must empty it for every copy.
   * @returns {void}
   */
  chunk(ownership) {
    const holder = ownership.batch
    if (this.chunks.has(holder)) return
    this.chunks.set(holder, { holder, ownership, messages: holder.messages })
    this.totals.chunks += 1
  }

  /**
   * @param {Generation} generation
   * @param {Function} cancel a cancellation it accepted, not one a closed lifecycle ran at once
   * @returns {void}
   */
  register(generation, cancel) {
    let held = this.cancels.get(generation)
    if (held === undefined) {
      held = new Set()
      this.cancels.set(generation, held)
    }
    held.add(cancel)
    this.totals.registered += 1
  }

  /**
   * @param {Generation} generation
   * @param {Function} cancel the cancellation its unregister function ended
   * @returns {void}
   */
  unregister(generation, cancel) {
    this.settleCancels(generation, (held) => held.delete(cancel))
  }

  /**
   * @param {Generation} generation one that closed, which cleared every cancellation it held
   * @returns {void}
   */
  close(generation) {
    this.settleCancels(generation, (held) => held.clear())
  }

  /**
   * @param {Generation} generation
   * @param {(held: Set<Function>) => void} change
   * @returns {void}
   */
  settleCancels(generation, change) {
    const held = this.cancels.get(generation)
    if (held === undefined) return
    change(held)
    if (held.size === 0) this.cancels.delete(generation)
  }

  /**
   * What is still retained, which is `NO_RETAINED_RESOURCES` when the producer holds nothing, and
   * what was observed in all, for the evidence.
   * @returns {{ retained: { pins: Record<string, number>, sends: object[], epochs: number[],
   *   registrations: number, chunks: object[] }, inventory: number[], totals: object }} `pins` the
   *   topics whose holds and releases differ; `sends` each tracked send not fully retired;
   *   `epochs` the unsettled count of each flush epoch not drained; `registrations` the
   *   cancellations still registered; `chunks` each partition chunk whose holder still carries
   *   records or owners, or whose subdivision alias still holds a record; `inventory` the size of
   *   each accumulator's latest settlement snapshot when it was not empty, which is only current
   *   right after a flush
   */
  facts() {
    for (const send of [...this.sends.keys()]) {
      if (!retainsResources(send)) {
        this.sends.delete(send)
        this.totals.retired += 1
      }
    }
    for (const epoch of [...this.epochs]) if (epoch.unsettled === 0) this.epochs.delete(epoch)
    for (const [holder, chunk] of [...this.chunks]) {
      if (!retainsChunk(chunk)) this.chunks.delete(holder)
    }
    return {
      retained: {
        pins: Object.fromEntries([...this.pins].filter(([, balance]) => balance !== 0)),
        sends: [...this.sends.keys()].map((send) => ({
          topics: send.topics,
          records: send.records !== null,
          reject: send.reject !== null,
          epoch: send.epoch !== null,
          shares: send.shares,
          pins: send.pins,
          payload: send.payload,
        })),
        epochs: [...this.epochs].map(({ unsettled }) => unsettled),
        registrations: [...this.cancels.values()].reduce((sum, held) => sum + held.size, 0),
        chunks: [...this.chunks.values()].map(({ holder, ownership, messages }) => ({
          records: holder.messages === null ? null : holder.messages.length,
          owners: holder.owners === null ? null : holder.owners.length / 2,
          aliasRecords: ownership.child ? messages.length : 0,
        })),
      },
      inventory: [...this.snapshots.values()].filter((size) => size !== 0),
      totals: {
        ...this.totals,
        accumulators: this.accumulators.size,
        inventoried: this.snapshots.size,
      },
    }
  }

  /**
   * Forgets everything recorded.
   * @returns {void}
   */
  clear() {
    this.pins.clear()
    this.sends.clear()
    this.epochs.clear()
    this.accumulators.clear()
    this.snapshots.clear()
    this.cancels.clear()
    this.chunks.clear()
    Object.keys(this.totals).forEach((name) => (this.totals[name] = 0))
  }
}

/**
 * Observes the producer under test through temporary additive spies on `Admission#acquire`,
 * `Cluster#holdTopic` and `releaseTopic`, `Accumulator#track` and `unsettled` and `Generation#register`
 * and `close`, and through a setter on the prototype of `BatchOwnership` that sees every batch
 * owner as it is constructed. It binds them to the instances that producer's own sends touched, and
 * ends a scenario with the closing admission probe: a public send of `capacity - overhead` payload
 * bytes that must be admitted in the same turn, then delivered and read back from the log. Counters
 * alone never close the acceptance, the same-turn reservation does, and the baselines before the
 * probe, after it and after the producer disconnected cover the pins, sends, flush epochs,
 * settlement snapshots, partition chunks and cancellations as well as the counters. A chunk is
 * observed where it is born, whichever path the batch takes afterwards, so a batch that expires
 * before it is planned is in the ledger like any other, and each is inspected for the record array
 * its holder and its copies still hold. Lifecycles are observed process-wide, so the probe assumes
 * no other producer is mid-operation when it reads a baseline.
 */
class ProducerProbe {
  /**
   * @param {object} options
   * @param {object} options.producer the public producer under test
   * @param {import('./liveWire').FrameLog} [options.frames] the log of its sockets' written frames
   * @param {string} options.clientId
   * @param {string} [options.transactionalId]
   * @param {number} options.maxRequestSize
   * @param {string[]} [options.brokers]
   * @param {{ attempts: number, delay: number }} [options.quietBudget] how long a baseline waits
   */
  constructor({
    producer,
    frames,
    clientId,
    transactionalId,
    maxRequestSize,
    brokers = plainTextBrokers(),
    quietBudget = QUIET_BUDGET,
  }) {
    Object.assign(this, {
      producer,
      frames,
      clientId,
      transactionalId,
      maxRequestSize,
      brokers,
      quietBudget,
    })
    /** @type {Admission|null} */
    this.admission = null
    /** @type {Cluster|null} */
    this.cluster = null
    this.acquires = []
    this.inEntry = false
    this.capturing = false
    this.seenAdmissions = new Set()
    this.seenClusters = new Set()
    this.ledger = new ResourceLedger()
    this.spies = []
    /** @type {{ prototype: object, setter: Function }|null} the observation of `BatchOwnership` */
    this.trap = null
  }

  /**
   * Installs the spies, each calling the original with its own `this` and changing nothing about
   * its result. Call `restore` in an unconditional finally.
   * @returns {this}
   */
  install() {
    const probe = this
    const { acquire } = Admission.prototype
    const { holdTopic, releaseTopic } = Cluster.prototype
    const { track, unsettled } = Accumulator.prototype
    const { register, close } = Generation.prototype
    const mine = (cluster) => probe.capturing || cluster === probe.cluster
    this.spies.push(
      jest.spyOn(Admission.prototype, 'acquire').mockImplementation(function (...args) {
        const call = {
          instance: this,
          bytes: args[0],
          inEntry: probe.inEntry,
          immediate: false,
          error: null,
        }
        probe.acquires.push(call)
        if (probe.capturing) probe.seenAdmissions.add(this)
        try {
          const result = acquire.apply(this, args)
          call.immediate = result != null && typeof result.then !== 'function'
          return result
        } catch (error) {
          call.error = error
          throw error
        }
      }),
      jest.spyOn(Cluster.prototype, 'holdTopic').mockImplementation(function (topic) {
        if (probe.capturing) probe.seenClusters.add(this)
        if (mine(this)) probe.ledger.hold(topic)
        return holdTopic.call(this, topic)
      }),
      jest.spyOn(Cluster.prototype, 'releaseTopic').mockImplementation(function (topic) {
        if (mine(this)) probe.ledger.release(topic)
        return releaseTopic.call(this, topic)
      }),
      jest.spyOn(Accumulator.prototype, 'track').mockImplementation(function (send) {
        const result = track.call(this, send)
        if (probe.capturing || send.pinner === probe.cluster) probe.ledger.track(this, send)
        return result
      }),
      jest.spyOn(Accumulator.prototype, 'unsettled').mockImplementation(function () {
        const snapshot = unsettled.call(this)
        probe.ledger.snapshot(this, snapshot.length)
        return snapshot
      }),
      jest.spyOn(Generation.prototype, 'register').mockImplementation(function (cancel) {
        const wasOpen = !this.closed
        const unregister = register.call(this, cancel)
        if (wasOpen) probe.ledger.register(this, cancel)
        return () => {
          probe.ledger.unregister(this, cancel)
          unregister()
        }
      }),
      jest.spyOn(Generation.prototype, 'close').mockImplementation(function (error) {
        close.call(this, error)
        probe.ledger.close(this)
      })
    )
    return this
  }

  /**
   * Records a batch owner as a chunk of this producer; one of another producer is left out once
   * the probe is bound.
   * @param {object} ownership a `BatchOwnership` under construction, its admission already set
   * @returns {void}
   */
  watch(ownership) {
    if (this.capturing || ownership.admission === this.admission) this.ledger.chunk(ownership)
  }

  /**
   * Puts a setter on the prototype of `BatchOwnership`, whose constructor assigns `batch`: it
   * hands the owner to `watch` and keeps the property as an ordinary own one. The class is the
   * accumulator's private one, reached through `batchOwnership`. Safe to call again. The setter is
   * shared by every case of the file, so a discovery that finishes after `signal` aborted installs
   * nothing.
   * @param {object} [options]
   * @param {AbortSignal} [options.signal] the case's scope; checked before the setter is installed
   * @returns {Promise<void>}
   * @throws {Error} the reason `signal` aborted with
   */
  async observeOwners({ signal = NEVER } = {}) {
    if (this.trap !== null) return
    const { prototype } = await batchOwnership()
    signal.throwIfAborted()
    const probe = this
    const setter = function (batch) {
      Object.defineProperty(this, 'batch', {
        value: batch,
        writable: true,
        enumerable: true,
        configurable: true,
      })
      probe.watch(this)
    }
    Object.defineProperty(prototype, 'batch', {
      get: () => undefined,
      set: setter,
      configurable: true,
    })
    this.trap = { prototype, setter }
  }

  /**
   * Removes every spy and the owner observation and forgets what the ledger recorded; safe to
   * call again. A setter another probe has put there since stays.
   * @returns {void}
   */
  restore() {
    for (const spy of this.spies.splice(0)) spy.mockRestore()
    if (this.trap !== null) {
      const { prototype, setter } = this.trap
      if (Object.getOwnPropertyDescriptor(prototype, 'batch')?.set === setter) {
        delete prototype.batch
      }
      this.trap = null
    }
    this.ledger.clear()
  }

  /**
   * Runs `work`, the producer's own first send, and binds the probe to the one `Admission` and
   * `Cluster` it used. The batch owners of the send are observed from the start. Once `signal`
   * aborts, nothing is installed and `work` is not called.
   * @template T
   * @param {() => Promise<T>} work
   * @param {object} [options]
   * @param {AbortSignal} [options.signal] the case's scope; checked before the owners are observed
   *   and again before `work` is called
   * @returns {Promise<T>}
   * @throws {Error} when `work` touched no producer, or more than one; the reason `signal` aborted
   *   with
   */
  async capture(work, { signal = NEVER } = {}) {
    await this.observeOwners({ signal })
    signal.throwIfAborted()
    this.capturing = true
    let result
    try {
      result = await work()
    } finally {
      this.capturing = false
    }
    expect(this.seenAdmissions.size).toBe(1)
    expect(this.seenClusters.size).toBe(1)
    this.admission = [...this.seenAdmissions][0]
    this.cluster = [...this.seenClusters][0]
    return result
  }

  /**
   * @returns {{ available: number, capacity: number, reserved: number, waitingBytes: number,
   *   debts: number, pins: number }} the producer's buffer and topic pins
   */
  counters() {
    const { admission, cluster } = this
    return {
      available: admission.available,
      capacity: admission.capacity,
      reserved: admission.reserved,
      waitingBytes: admission.waitingBytes,
      debts: admission.debts,
      pins: cluster.topicPins === null ? 0 : cluster.topicPins.size,
    }
  }

  /**
   * @returns {ReturnType<ResourceLedger['facts']>} what the producer's pins, sends, flush epochs,
   *   settlement snapshots and lifecycle cancellations still retain, as observed so far
   */
  resources() {
    return this.ledger.facts()
  }

  /**
   * Waits for every retained send, partition chunk, pin, flush epoch and cancellation to retire,
   * then asserts the producer holds nothing: no counter, and no chunk whose holder or alias still
   * carries a record. It does not flush, so it also reads a producer that was disconnected.
   * @param {object} [options]
   * @param {AbortSignal} [options.signal] stops the wait, which then rejects with its reason
   * @returns {Promise<ReturnType<ProducerProbe['counters']>|null>} the counters at rest; null when
   *   no send ever bound the probe, so there is nothing to read
   * @throws {Error} through a failed expectation when something is still held after the budget
   */
  async settled({ signal = NEVER } = {}) {
    if (this.admission === null) return null
    const nothingHeld = () => {
      const c = this.counters()
      return (
        c.available === c.capacity &&
        !c.reserved &&
        !c.waitingBytes &&
        !c.debts &&
        !c.pins &&
        isDeepStrictEqual(this.resources().retained, NO_RETAINED_RESOURCES)
      )
    }
    for (let attempt = 0; attempt < this.quietBudget.attempts && !nothingHeld(); attempt++) {
      await delay(signal, this.quietBudget.delay)
    }
    const counters = this.counters()
    expect(counters).toEqual({
      available: counters.capacity,
      capacity: counters.capacity,
      reserved: 0,
      waitingBytes: 0,
      debts: 0,
      pins: 0,
    })
    expect(this.resources().retained).toEqual(NO_RETAINED_RESOURCES)
    return counters
  }

  /**
   * `settled`, then the public flush: with nothing retained it must resolve at once, and the
   * settlement snapshot of every accumulator that tracked a send must have been taken and be empty.
   * @param {object} [options]
   * @param {AbortSignal} [options.signal] see `settled`; nothing is flushed once it aborted
   * @returns {Promise<ReturnType<ProducerProbe['counters']>>} the counters at rest
   * @throws {Error} through a failed expectation when anything is held or unsettled
   */
  async quiet({ signal = NEVER } = {}) {
    const counters = await this.settled({ signal })
    signal.throwIfAborted()
    await expect(this.producer.flush({ timeout: 0 })).resolves.toBeUndefined()
    const { inventory, totals } = this.resources()
    expect(inventory).toEqual([])
    expect(totals.inventoried).toBe(totals.accumulators)
    return counters
  }

  /**
   * The closing probe. It sends on a fresh ordinary topic (RF 2), apart from the workload's log.
   * @param {object} options
   * @param {{ send: Function }} [options.sender] the producer, or a fresh transaction of it
   * @param {string} options.topic the probe topic, created here
   * @param {() => Promise<void>} [options.finish] ends a transaction the sender opened, after the
   *   send was delivered
   * @param {number} [options.isolationLevel] how the probe topic is read back; read_committed for
   *   a transaction
   * @param {number} options.budgetMs the producer's delivery budget
   * @param {AbortSignal} [options.signal] the owner's: every step goes through `bounded` under it,
   *   so once it aborts this method issues no topic, configuration read, send or log read itself; an
   *   operation it started that ignores the abort may still finish, which taints the run
   * @returns {Promise<object>} what was observed, for the scenario's evidence
   * @throws {Error} through a failed expectation when the send was not admitted in its own turn,
   *   was not delivered once and in order, left the producer holding anything, or was never seen
   *   as a partition chunk; the reason `signal` aborted with
   */
  async closing({
    sender = this.producer,
    topic,
    finish,
    isolationLevel = ISOLATION_LEVEL.READ_UNCOMMITTED,
    budgetMs,
    signal = NEVER,
  }) {
    const step = (op, ms = Infinity, label) => bounded(signal, ms, op, { label })
    const before = await step((scope) => this.quiet({ signal: scope }))
    const resourcesBefore = this.resources()
    await step(() =>
      createTopic({
        topic,
        partitions: 1,
        replicas: 2,
        config: [{ name: 'max.message.bytes', value: String(PROBE_TOPIC_LIMIT) }],
      })
    )
    const limit = await step((scope) =>
      topicConfig(this.brokers, topic, 'max.message.bytes', scope)
    )
    expect(limit).toBe(String(PROBE_TOPIC_LIMIT))
    const layout = probeLayout({
      capacity: before.capacity,
      maxRequestSize: this.maxRequestSize,
      clientId: this.clientId,
      transactionalId: this.transactionalId,
      topic,
      topicMaxMessageBytes: Number(limit),
    })
    const run = randomBytes(3).toString('hex')
    const ids = layout.lengths.map((_, index) => `${run}:${String(index).padStart(5, '0')}:`)
    const messages = layout.lengths.map((length, index) => {
      const value = Buffer.alloc(length)
      value.write(ids[index])
      return { value, timestamp: PROBE_TIMESTAMP }
    })
    expect(messages.every(({ value }) => value.buffer.byteLength === value.length)).toBe(true)

    signal.throwIfAborted()
    const first = this.acquires.length
    const mark = this.frames?.mark() ?? 0
    this.inEntry = true
    let delivered
    try {
      delivered = settle(
        sender.send({ topic, messages, acks: -1, compression: CompressionTypes.None })
      )
    } finally {
      this.inEntry = false
    }
    const waitingAfterEntry = this.admission.waitingBytes
    const entry = this.resources()
    const outcome = await step(() => delivered, budgetMs + 5000, 'The closing probe send')
    const acquired = this.acquires
      .slice(first)
      .filter(({ instance }) => instance === this.admission)
    expect(acquired).toHaveLength(1)
    expect(acquired[0]).toMatchObject({
      bytes: layout.charge,
      inEntry: true,
      immediate: true,
      error: null,
    })
    expect(waitingAfterEntry).toBe(0)
    expect(entry.retained.pins).toEqual({ [topic]: 1 })
    expect(entry.retained.sends).toHaveLength(1)
    expect(entry.totals.tracked).toBe(resourcesBefore.totals.tracked + 1)
    expect(outcome.error).toBeNull()
    await step(() => finish?.())

    const [log] = (
      await step(() => readLog({ brokers: this.brokers, topic, isolationLevel }))
    ).values()
    const data =
      isolationLevel === ISOLATION_LEVEL.READ_COMMITTED
        ? committedRecords(log)
        : log.records.filter(({ batchContext }) => !batchContext.isControlBatch)
    expect(data.map(({ value }) => value.toString('latin1', 0, ID_BYTES))).toEqual(ids)
    expect(data.map(({ value }) => value.length)).toEqual(layout.lengths)
    const produces = this.frames?.since(0, mark) ?? []
    const largest = produces.reduce((most, { bytes }) => Math.max(most, bytes), 0)
    if (this.frames) {
      expect(produces.length).toBeGreaterThan(0)
      expect(largest).toBeLessThanOrEqual(this.maxRequestSize)
    }
    const after = await step((scope) => this.quiet({ signal: scope }))
    const resourcesAfter = this.resources()
    expect(resourcesAfter.retained.chunks).toEqual([])
    expect(resourcesAfter.totals.chunks).toBeGreaterThan(resourcesBefore.totals.chunks)
    return {
      before,
      after,
      resources: { before: resourcesBefore, entry, after: resourcesAfter },
      layout: {
        ...layout,
        lengths: undefined,
        first: layout.lengths[0],
        last: layout.lengths.at(-1),
      },
      observed: { bytes: acquired[0].bytes, inEntry: acquired[0].inEntry, waitingAfterEntry },
      frames: { produce: produces.length, largest },
      records: data.length,
    }
  }
}

/** @type {Function|null} */
let BatchOwnershipClass = null

/**
 * The accumulator's `BatchOwnership` class. The module does not export it, so one throwaway batch
 * through a private accumulator reveals it; the class is kept for the process.
 * @returns {Promise<Function>}
 */
const batchOwnership = async () => {
  if (BatchOwnershipClass === null) {
    const accumulator = new Accumulator({
      linger: 0,
      batchSize: 1024,
      partition: async ({ records }) =>
        records.map(({ topic, messages }) => ({ topic, partition: 0, messages })),
      sendPartitioned: async ({ batches }) => {
        BatchOwnershipClass = batches[0].ownership.constructor
        for (const batch of batches) {
          batch.settlers.forEach((settler) =>
            settler.resolve({ topicName: 'ownership', partition: 0, baseOffset: '0' })
          )
          batch.retire()
        }
        return []
      },
    })
    await accumulator.append({
      acks: -1,
      topicMessages: [{ topic: 'ownership', messages: [{ value: 'x' }] }],
    })
  }
  return BatchOwnershipClass
}

/**
 * @param {Promise<unknown>} promise
 * @returns {Promise<{ value: unknown, error: Error|null }>} the settled outcome, so a rejection can
 *   be asserted after the steps that follow it
 */
const settle = (promise) =>
  promise.then(
    (value) => ({ value, error: null }),
    (error) => ({ value: null, error })
  )

/**
 * Waits for `promise` for at most `ms`. A promise that outlasts that cannot be stopped from here, so
 * the expiry taints the run (`liveRun`) with the error it rejects with.
 * @template T
 * @param {number} ms
 * @param {Promise<T>} promise
 * @param {string} label
 * @returns {Promise<T>}
 * @throws {Error} when `promise` has not settled within `ms`
 */
const within = (ms, promise, label) => {
  let timer
  const deadline = new Promise((_, reject) => {
    timer = setTimeout(() => {
      const error = new Error(`${label} did not settle within ${ms} ms`)
      liveRun.taint(error)
      reject(error)
    }, ms)
  })
  return Promise.race([promise, deadline]).finally(() => clearTimeout(timer))
}

/**
 * @param {string[]} brokers
 * @param {string} topic
 * @param {string} name a topic config name
 * @param {AbortSignal} [signal] once it aborts, the read is not issued; the admin is still closed
 * @returns {Promise<string>} the value the broker reports for it
 * @throws {Error} the reason `signal` aborted with
 */
const topicConfig = async (brokers, topic, name, signal = NEVER) => {
  const admin = new Kafka({ brokers, logLevel: logLevel.NOTHING }).admin()
  await admin.connect()
  try {
    signal.throwIfAborted()
    const { resources } = await admin.describeConfigs({
      includeSynonyms: false,
      resources: [{ type: ConfigResourceTypes.TOPIC, name: topic, configNames: [name] }],
    })
    return resources[0].configEntries.find(({ configName }) => configName === name).configValue
  } finally {
    await admin.disconnect()
  }
}

/**
 * The node leading a partition, with the address clients dial it by.
 * @param {object} admin a connected admin client
 * @param {string} topic
 * @param {number} [partition=0]
 * @returns {Promise<{ nodeId: number, host: string, port: number }>}
 */
const leaderOf = async (admin, topic, partition = 0) => {
  const {
    topics: [metadata],
  } = await admin.fetchTopicMetadata({ topics: [topic] })
  const { leader } = metadata.partitions.find(({ partitionId }) => partitionId === partition)
  const { brokers } = await admin.describeCluster()
  const { host, port } = brokers.find(({ nodeId }) => nodeId === leader)
  return { nodeId: leader, host, port }
}

/**
 * The broker that coordinates a group or a transactional id.
 * @param {string} key the group id or transactional id
 * @param {number} coordinatorType one of `COORDINATOR_TYPES`
 * @returns {Promise<{ host: string, port: number }>}
 */
const coordinatorOf = async (key, coordinatorType) => {
  const seed = new Broker({ connectionPool: createConnectionPool(), logger: newLogger() })
  try {
    await seed.connect()
    const { coordinator } = await seed.findGroupCoordinator({ groupId: key, coordinatorType })
    return { host: coordinator.host, port: coordinator.port }
  } finally {
    await seed.disconnect()
  }
}

/**
 * Where the idempotent batches of a raw log skip or repeat a sequence number within one producer
 * epoch; plain producers, which carry no sequence, are left out.
 * @param {object[]} records the records of one partition, as `readLog` returns them
 * @returns {Array<{ producerId: string, producerEpoch: number, expected: number, actual: number }>}
 */
const sequenceGaps = (records) => {
  const batches = new Map()
  for (const { batchContext } of records) {
    if (batchContext.isControlBatch || batchContext.producerId === '-1') continue
    batches.set(batchContext.firstOffset, batchContext)
  }
  const ordered = [...batches.values()].sort((a, b) =>
    Number(BigInt(a.firstOffset) - BigInt(b.firstOffset))
  )
  const next = new Map()
  const gaps = []
  for (const batch of ordered) {
    const key = `${batch.producerId}:${batch.producerEpoch}`
    if (next.has(key) && next.get(key) !== batch.firstSequence) {
      gaps.push({
        producerId: batch.producerId,
        producerEpoch: batch.producerEpoch,
        expected: next.get(key),
        actual: batch.firstSequence,
      })
    }
    next.set(key, batch.firstSequence + batch.lastOffsetDelta + 1)
  }
  return gaps
}

/**
 * @param {Map<number, { records: object[] }>} log what `readLog` returned
 * @returns {object[]} the data records of every partition, control records left out, by partition
 *   then offset
 */
const dataRecords = (log) =>
  [...log.entries()]
    .sort(([a], [b]) => a - b)
    .flatMap(([, { records }]) =>
      records.filter(({ batchContext }) => !batchContext.isControlBatch)
    )

/**
 * The data records of a topic as its leaders hold them.
 * @param {string[]} brokers
 * @param {string} topic
 * @param {object} [options]
 * @param {boolean} [options.committed=false] the records a read_committed reader is given, by the
 *   Java client's rules, instead of everything the log holds
 * @returns {Promise<object[]>} the records by partition then offset, control records left out
 */
const readData = async (brokers, topic, { committed = false } = {}) => {
  const log = await readLog({
    brokers,
    topic,
    isolationLevel: committed ? ISOLATION_LEVEL.READ_COMMITTED : ISOLATION_LEVEL.READ_UNCOMMITTED,
  })
  if (!committed) return dataRecords(log)
  return [...log.entries()].sort(([a], [b]) => a - b).flatMap(([, part]) => committedRecords(part))
}

/**
 * @param {Error|null|undefined} error
 * @param {number} [depth=3] how many causes to follow
 * @returns {object|null} what evidence keeps of an error: no stack, no secrets
 */
const describeError = (error, depth = 3) =>
  error == null
    ? null
    : {
        name: error.name,
        type: error.type,
        code: error.code,
        category: error.category,
        retriable: error.retriable,
        outcome: error.outcome,
        reason: error.reason,
        message: String(error.message).slice(0, 300),
        cause: depth > 0 ? (describeError(error.cause, depth - 1) ?? undefined) : undefined,
      }

module.exports = {
  CleanupStack,
  COORDINATOR_TYPES,
  ProduceLedger,
  ProducerProbe,
  batchOwnership,
  coordinatorOf,
  dataRecords,
  describeError,
  leaderOf,
  probeLayout,
  readData,
  sequenceGaps,
  settle,
  topicConfig,
  traceCalls,
  varintSize,
  within,
}
