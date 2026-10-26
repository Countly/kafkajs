'use strict'

const { ConfigResourceTypes, CompressionTypes } = require('../../index')
const Cluster = require('../../src/cluster')
const RequestQueue = require('../../src/network/requestQueue')
const Generation = require('../../src/producer/generation')
const Accumulator = require('../../src/producer/state/accumulator')
const { Admission } = require('../../src/producer/state/admission')
const { RequestBudget } = require('../../src/producer/state/recordSize')
const { readLog } = require('../../testHelpers/rawLog')
const { Clients } = require('../clients')

/** Every probe record carries this timestamp; its topic keeps records forever (`retention.ms: -1`). */
const PROBE_TIMESTAMP = 1700000000000
/** The `max.message.bytes` the probe topic is created with and read back at. */
const PROBE_TOPIC_LIMIT = 8388608
/** What a send costs beside its record, the record's fixed charge, and a request's fixed framing. */
const SEND_SHELL = 1536
const RECORD_FIXED = 87
const PARTITION_FRAMING = 16
const BATCH_HEADER = 61
/** The bytes of a request's size prefix and the batch's own framing around one record. */
const RECORD_WIRE_FIXED = 23

/**
 * @param {number} length a non-negative length
 * @returns {number} the bytes of its zigzag varint, from 1 to 5
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
 * The one value size whose send is charged exactly `capacity`: `1536 + N + 87 + varintSize(N)`,
 * derived here from the sizes, never from the producer's own accounting.
 * @param {number} capacity the producer's `bufferMemory`
 * @returns {number} N
 * @throws {Error} when no value size is charged exactly `capacity`
 */
const exactValueBytes = (capacity) => {
  for (let size = 1; size <= 5; size++) {
    const bytes = capacity - SEND_SHELL - RECORD_FIXED - size
    if (bytes > 0 && varintSize(bytes) === size) return bytes
  }
  throw new Error(`No single value is charged exactly bufferMemory ${capacity}`)
}

/**
 * Wraps a method of a prototype and returns what restores it.
 * @param {object} target
 * @param {string} name
 * @param {(original: Function, self: object, args: unknown[]) => unknown} wrapper
 * @returns {() => void}
 */
const patch = (target, name, wrapper) => {
  const original = target[name]
  target[name] = function (...args) {
    return wrapper(original, this, args)
  }
  return () => {
    target[name] = original
  }
}

/**
 * The soak's closing admission probe. After a phase has gone quiet it sends one standalone value
 * charged exactly the producer's whole `bufferMemory` through the producer's public `send`, and
 * requires that the buffer admits it in the calling turn with nothing waiting, that the broker
 * acknowledges it and a raw read finds it once, and that every byte, pin, lifecycle registration
 * and settlement promise is back afterwards. It watches the producer through additive wraps of
 * `Admission#acquire`, `Cluster#holdTopic` and `releaseTopic`, `Generation#register` and `close` and
 * `Accumulator#unsettled`, and of `RequestQueue#push` for the largest frame, which keep counters and nothing else, so the process holds no growing
 * record of a long run. It stands in for the Jest-bound probe, which a plain `node` run cannot load;
 * `assertProbe` of `testHelpers/producerProbe.js` is its acceptance.
 */
class ClosingProbe {
  /**
   * @param {object} options
   * @param {string[]} options.brokers seed addresses, `host:port`
   * @param {string} options.clientId the producer's client id
   * @param {string} options.topic the probe topic: one partition, created by `prepare`
   * @param {number} options.capacity the producer's `bufferMemory`
   * @param {number} options.maxRequestSize the producer's `maxRequestSize`
   * @param {number} options.budgetMs the producer's delivery budget
   * @param {typeof readLog} [options.read=readLog] reads the whole probe topic
   * @param {number} [options.readBackMs=30000] how long the read waits for the record to show
   * @param {number} [options.quietMs=10000] how long a quiet point waits for the producer to settle
   */
  constructor({
    brokers,
    clientId,
    topic,
    capacity,
    maxRequestSize,
    budgetMs,
    read = readLog,
    readBackMs = 30000,
    quietMs = 10000,
  }) {
    Object.assign(this, { brokers, clientId, topic, capacity, maxRequestSize, budgetMs, read })
    this.readBackMs = readBackMs
    this.quietMs = quietMs
    this.tag = Math.random().toString(16).slice(2, 8)
    /** @type {Admission|null} */
    this.admission = null
    /** @type {Cluster|null} */
    this.cluster = null
    this.capturing = false
    this.seenAdmissions = new Set()
    this.seenClusters = new Set()
    this.inEntry = false
    /** @type {Array<object>|null} the acquires of the probe's own send */
    this.window = null
    this.largestFrame = 0
    this.pins = new Map()
    this.cancels = new Map()
    this.snapshots = new Map()
    this.restores = []
  }

  /**
   * Creates the probe topic and reads its two settings back.
   * @param {object} kafka a `Kafka`
   * @returns {Promise<void>}
   * @throws {Error} when the broker did not apply either setting
   */
  prepare(kafka) {
    const settings = { 'max.message.bytes': String(PROBE_TOPIC_LIMIT), 'retention.ms': '-1' }
    return Clients.use(kafka, async (clients) => {
      const admin = clients.admin()
      await admin.connect()
      await admin.createTopics({
        topics: [
          {
            topic: this.topic,
            numPartitions: 1,
            replicationFactor: 3,
            configEntries: Object.entries(settings).map(([name, value]) => ({ name, value })),
          },
        ],
        waitForLeaders: true,
      })
      const { resources } = await admin.describeConfigs({
        includeSynonyms: false,
        resources: [
          { type: ConfigResourceTypes.TOPIC, name: this.topic, configNames: Object.keys(settings) },
        ],
      })
      for (const [name, value] of Object.entries(settings)) {
        const entry = resources[0].configEntries.find(({ configName }) => configName === name)
        if (entry?.configValue !== value) {
          throw new Error(`The probe topic has ${name} ${entry?.configValue}, not ${value}`)
        }
      }
    })
  }

  /**
   * Installs the wraps. Call `restore` in an unconditional finally.
   * @returns {this}
   */
  install() {
    const probe = this
    this.restores.push(
      patch(Admission.prototype, 'acquire', (acquire, self, args) => {
        if (probe.capturing) probe.seenAdmissions.add(self)
        const call = probe.window && {
          instance: self,
          bytes: args[0],
          inEntry: probe.inEntry,
          immediate: false,
        }
        if (call) probe.window.push(call)
        const reservation = acquire.apply(self, args)
        if (call) call.immediate = reservation != null && typeof reservation.then !== 'function'
        return reservation
      }),
      patch(Cluster.prototype, 'holdTopic', (holdTopic, self, [topic]) => {
        if (probe.capturing) probe.seenClusters.add(self)
        if (probe.capturing || self === probe.cluster) probe.#pin(topic, 1)
        return holdTopic.call(self, topic)
      }),
      patch(Cluster.prototype, 'releaseTopic', (releaseTopic, self, [topic]) => {
        if (probe.capturing || self === probe.cluster) probe.#pin(topic, -1)
        return releaseTopic.call(self, topic)
      }),
      patch(Generation.prototype, 'register', (register, self, [cancel]) => {
        const open = !self.closed
        const unregister = register.call(self, cancel)
        if (open) probe.#register(self, cancel)
        return () => {
          probe.#unregister(self, cancel)
          unregister()
        }
      }),
      patch(Generation.prototype, 'close', (close, self, [error]) => {
        close.call(self, error)
        probe.cancels.delete(self)
      }),
      patch(Accumulator.prototype, 'unsettled', (unsettled, self) => {
        const snapshot = unsettled.call(self)
        probe.snapshots.set(self, snapshot.length)
        return snapshot
      }),
      patch(RequestQueue.prototype, 'push', (push, self, [request]) => {
        if (probe.window && request.entry?.apiName === 'Produce') {
          probe.largestFrame = Math.max(probe.largestFrame, request.frameLength ?? 0)
        }
        return push.call(self, request)
      })
    )
    return this
  }

  /**
   * Removes every wrap and forgets what was counted; safe to call again.
   * @returns {void}
   */
  restore() {
    for (const restore of this.restores.splice(0).reverse()) restore()
    this.pins.clear()
    this.cancels.clear()
    this.snapshots.clear()
  }

  /**
   * @param {string} topic
   * @param {number} step
   * @returns {void}
   */
  #pin(topic, step) {
    const balance = (this.pins.get(topic) ?? 0) + step
    if (balance === 0) this.pins.delete(topic)
    else this.pins.set(topic, balance)
  }

  /**
   * @param {Generation} generation
   * @param {Function} cancel
   * @returns {void}
   */
  #register(generation, cancel) {
    const held = this.cancels.get(generation) ?? new Set()
    held.add(cancel)
    this.cancels.set(generation, held)
  }

  /**
   * @param {Generation} generation
   * @param {Function} cancel
   * @returns {void}
   */
  #unregister(generation, cancel) {
    const held = this.cancels.get(generation)
    if (!held) return
    held.delete(cancel)
    if (held.size === 0) this.cancels.delete(generation)
  }

  /**
   * Runs the producer's own workload send and binds the probe to the one `Admission` and `Cluster`
   * it used.
   * @template T
   * @param {() => Promise<T>} work
   * @returns {Promise<T>}
   * @throws {Error} when the send touched no producer, or more than one
   */
  async capture(work) {
    this.capturing = true
    let result
    try {
      result = await work()
    } finally {
      this.capturing = false
    }
    if (this.seenAdmissions.size !== 1 || this.seenClusters.size !== 1) {
      throw new Error(
        `The captured send used ${this.seenAdmissions.size} buffers and ${this.seenClusters.size} clusters, not one each`
      )
    }
    this.admission = [...this.seenAdmissions][0]
    this.cluster = [...this.seenClusters][0]
    this.seenAdmissions.clear()
    this.seenClusters.clear()
    return result
  }

  /**
   * @returns {{ available: number, capacity: number, reserved: number, waitingBytes: number,
   *   debts: number, pins: number, registrations: number, settlePromises: number }} the producer's
   *   buffer, its topic pins, the cancellations its lifecycles still hold and the size of the
   *   latest settlement snapshot of each accumulator
   * @throws {Error} before a send has been captured
   */
  retained() {
    const { admission } = this
    if (admission === null) throw new Error('The probe has not captured a producer')
    const sum = (values) => [...values].reduce((total, value) => total + value, 0)
    return {
      available: admission.available,
      capacity: admission.capacity,
      reserved: admission.reserved,
      waitingBytes: admission.waitingBytes,
      debts: admission.debts,
      pins: sum([...this.pins.values()].map(Math.abs)),
      registrations: sum([...this.cancels.values()].map((held) => held.size)),
      settlePromises: sum(this.snapshots.values()),
    }
  }

  /**
   * @param {{ available: number, capacity: number }} held `retained()`
   * @returns {boolean} whether nothing is held
   */
  static isQuiet({ available, capacity, reserved, waitingBytes, debts, pins, registrations }) {
    return available === capacity && !reserved && !waitingBytes && !debts && !pins && !registrations
  }

  /**
   * Waits until the producer holds nothing, then requires its flush to find nothing unsettled.
   * @param {object} producer the public producer
   * @returns {Promise<ReturnType<ClosingProbe['retained']>>} what it holds at rest
   * @throws {Error} when something is still held after `quietMs`, or the flush finds a send
   */
  async quiet(producer) {
    const deadline = Date.now() + this.quietMs
    while (!ClosingProbe.isQuiet(this.retained()) && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
    const held = this.retained()
    if (!ClosingProbe.isQuiet(held)) {
      throw new Error(`The producer is not quiet: ${JSON.stringify(held)}`)
    }
    await producer.flush({ timeout: 0 })
    const settled = this.retained()
    if (settled.settlePromises !== 0) {
      throw new Error(`The producer's flush left ${settled.settlePromises} unsettled promises`)
    }
    return settled
  }

  /**
   * The frame arithmetic of the probe record, checked before anything is sent.
   * @returns {{ valueBytes: number, charge: number, wire: number, room: number }}
   * @throws {Error} when the value does not fit one request alone, or the producer's own chunk
   *   capacity disagrees with the independent sum
   */
  layout() {
    const valueBytes = exactValueBytes(this.capacity)
    const charge = SEND_SHELL + valueBytes + RECORD_FIXED + varintSize(valueBytes)
    const wire = valueBytes + RECORD_WIRE_FIXED + varintSize(valueBytes)
    const room =
      this.maxRequestSize -
      (32 + Buffer.byteLength(this.clientId)) -
      (32 + Buffer.byteLength(this.topic)) -
      PARTITION_FRAMING -
      BATCH_HEADER
    const chunk = new RequestBudget({
      maxRequestSize: this.maxRequestSize,
      clientId: this.clientId,
    }).chunkCapacity(this.topic, CompressionTypes.None)
    if (wire > room)
      throw new Error(`The probe record needs ${wire} bytes, a request holds ${room}`)
    if (chunk !== room)
      throw new Error(`The producer's chunk capacity is ${chunk}, the sum is ${room}`)
    return { valueBytes, charge, wire, room }
  }

  /**
   * Sends the probe record and reads off every fact `assertProbe` needs.
   * @param {object} options
   * @param {object} options.producer the public producer, quiet and connected
   * @param {string} options.label names this probe record, for example the phase
   * @returns {Promise<object>} the observation: `instanceMatchesWorkload`, `calls`, `bytes`,
   *   `capacity`, `inEntry`, `reservationImmediate`, `acknowledged` and the buffer, pin,
   *   registration and settlement figures at rest after the send, with the frame and read-back
   * @throws {Error} when the frame arithmetic fails, the producer is not quiet before or after, or
   *   the raw read cannot be made
   */
  async run({ producer, label }) {
    await this.quiet(producer)
    const layout = this.layout()
    const id = `churn-probe:${this.tag}:${label}:`
    const value = Buffer.alloc(layout.valueBytes)
    value.write(id, 0, 'latin1')
    if (value.buffer.byteLength !== value.length) {
      throw new Error('The probe value shares its backing store')
    }

    const window = []
    this.largestFrame = 0
    this.window = window
    let outcome
    let waitingAfterEntry
    try {
      let delivered
      this.inEntry = true
      try {
        delivered = producer
          .send({
            topic: this.topic,
            messages: [{ value, timestamp: PROBE_TIMESTAMP }],
            acks: -1,
            compression: CompressionTypes.None,
          })
          .then(
            (metadata) => ({ metadata }),
            (error) => ({ error })
          )
      } finally {
        this.inEntry = false
      }
      waitingAfterEntry = this.admission.waitingBytes
      outcome = await this.#within(delivered, this.budgetMs + 5000)
    } finally {
      this.window = null
    }

    if (!outcome.error && !(this.largestFrame > 0 && this.largestFrame <= this.maxRequestSize)) {
      throw new Error(
        `The probe's largest Produce frame was ${this.largestFrame} bytes, outside 1 to ${this.maxRequestSize}`
      )
    }
    const onAdmission = window.filter(({ instance }) => instance === this.admission)
    const found = outcome.error ? 0 : await this.#readBack(id, layout.valueBytes)
    const after = await this.quiet(producer)
    return {
      label,
      instanceMatchesWorkload: window.length > 0 && onAdmission.length === window.length,
      calls: onAdmission.length,
      bytes: onAdmission[0]?.bytes,
      inEntry: onAdmission[0]?.inEntry,
      reservationImmediate: onAdmission[0]?.immediate === true && waitingAfterEntry === 0,
      acknowledged: outcome.error === undefined && found === 1,
      ...after,
      layout,
      frame: { largest: this.largestFrame, maxRequestSize: this.maxRequestSize },
      readBack: { found },
      error: outcome.error ? `${outcome.error.name}: ${outcome.error.message}` : null,
    }
  }

  /**
   * @param {Promise<object>} promise
   * @param {number} ms
   * @returns {Promise<object>} what the promise gave
   * @throws {Error} when it did not settle within `ms`; no timer is left running
   */
  async #within(promise, ms) {
    let timer
    try {
      return await Promise.race([
        promise,
        new Promise((resolve, reject) => {
          timer = setTimeout(
            () => reject(new Error(`The probe send did not settle in ${ms} ms`)),
            ms
          )
        }),
      ])
    } finally {
      clearTimeout(timer)
    }
  }

  /**
   * Reads the probe topic until the record shows, or the budget runs out.
   * @param {string} id the record's id prefix
   * @param {number} bytes the record's length
   * @returns {Promise<number>} how many records of the log carry that id and length
   */
  async #readBack(id, bytes) {
    const deadline = Date.now() + this.readBackMs
    for (;;) {
      let found = 0
      try {
        const [log] = (await this.read({ brokers: this.brokers, topic: this.topic })).values()
        found = log.records.filter(
          ({ value, batchContext }) =>
            !batchContext.isControlBatch &&
            value?.length === bytes &&
            value.toString('latin1', 0, id.length) === id
        ).length
      } catch (error) {
        if (
          error?.retriable !== true &&
          !/stalled|No metadata|no known leader/.test(error?.message)
        ) {
          throw error
        }
      }
      if (found > 0 || Date.now() >= deadline) return found
      await new Promise((resolve) => setTimeout(resolve, 500))
    }
  }
}

module.exports = {
  ClosingProbe,
  PROBE_TIMESTAMP,
  PROBE_TOPIC_LIMIT,
  exactValueBytes,
  varintSize,
}
