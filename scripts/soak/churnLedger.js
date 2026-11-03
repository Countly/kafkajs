'use strict'

const fs = require('node:fs')

/**
 * Fixed-capacity bookkeeping of the producer-churn soak. Everything here is allocated once, before
 * the baseline reading, and never grows: a record is one slot of a typed array, not an object. The
 * observer therefore cannot create the heap slope the soak is looking for.
 */

/** What became of one offered send. */
const STATUS = Object.freeze({
  NONE: 0,
  PENDING: 1,
  ACK: 2,
  UNKNOWN: 3,
  UNSENT: 4,
  UNEXPECTED: 5,
})

/** The name of each status, as a key of `OfferLedger#counts`. */
const NAMES = ['none', 'pending', 'ack', 'unknown', 'unsent', 'unexpected']

/** One in this many offered ids carries a produce-to-consume latency sample. */
const LATENCY_SAMPLE_EVERY = 16

/** The bytes of the head of every record value: the offered id and the run tag. */
const VALUE_HEAD_BYTES = 8

/** The bytes of one journal entry. */
const ENTRY_BYTES = 32

/**
 * Builds the value of one offered send: its id and the run tag in the first 8 bytes, zeros after.
 * @param {number} size total bytes, at least `VALUE_HEAD_BYTES`
 * @param {number} id the offered id
 * @param {number} tag the run's tag, which keeps another run's records from passing as these
 * @returns {Buffer}
 */
const encodeValue = (size, id, tag) => {
  const value = Buffer.alloc(size)
  value.writeUInt32BE(id, 0)
  value.writeUInt32BE(tag, 4)
  return value
}

/**
 * @param {Buffer|null} value a record value
 * @param {number} tag the run's tag
 * @returns {number} the offered id the value carries; -1 for a value that is not one of this run's
 */
const decodeId = (value, tag) =>
  Buffer.isBuffer(value) && value.length >= VALUE_HEAD_BYTES && value.readUInt32BE(4) === tag
    ? value.readUInt32BE(0)
    : -1

/**
 * @param {{ offset: string, batchContext: { firstOffset: string, firstSequence: number } }} record
 *   a decoded record
 * @returns {number} the record's producer sequence: its batch's first sequence plus its place in the batch
 */
const sequenceOf = ({ offset, batchContext: { firstOffset, firstSequence } }) =>
  firstSequence + (Number(offset) - Number(firstOffset))

/**
 * The outcome of every offered send, by id, and the timing of a sample of them. Ids are dense
 * integers from 0, so the whole table is one byte per send.
 */
class OfferLedger {
  /**
   * @param {object} options
   * @param {number} options.capacity the most sends that can be offered
   * @param {number} options.partitions how many partitions the topic has
   * @param {number} [options.sampleEvery=LATENCY_SAMPLE_EVERY] one id in this many is timed
   */
  constructor({ capacity, partitions, sampleEvery = LATENCY_SAMPLE_EVERY }) {
    const slots = Math.ceil(capacity / sampleEvery)
    this.capacity = capacity
    this.sampleEvery = sampleEvery
    this.status = new Uint8Array(capacity)
    this.offered = 0
    this.counts = { pending: 0, ack: 0, unknown: 0, unsent: 0, unexpected: 0 }
    this.offeredAt = new Float64Array(slots)
    this.observedAt = new Float64Array(slots)
    this.stages = new Uint8Array(slots)
    this.latencies = new Float64Array(slots)
    this.sampled = 0
    this.invalid = 0
    this.lastAckOffset = new Float64Array(partitions).fill(-1)
    this.lastAckId = new Int32Array(partitions).fill(-1)
  }

  /**
   * Takes the next id for a send about to be made.
   * @param {number} now monotonic ms
   * @returns {number} the id; -1 when the ledger is full
   */
  offer(now) {
    if (this.offered >= this.capacity) return -1
    const id = this.offered++
    this.status[id] = STATUS.PENDING
    this.counts.pending += 1
    if (id % this.sampleEvery === 0) this.offeredAt[id / this.sampleEvery] = now
    return id
  }

  /**
   * Gives a pending send its outcome. A send that is not pending is left alone.
   * @param {number} id
   * @param {number} status one of `STATUS.ACK`, `UNKNOWN`, `UNSENT`, `UNEXPECTED`
   * @param {{ partition: number, offset: number }} [at] where an acknowledged record landed
   * @returns {void}
   */
  settle(id, status, at) {
    if (this.status[id] !== STATUS.PENDING) return
    this.status[id] = status
    this.counts.pending -= 1
    this.counts[NAMES[status]] += 1
    if (status !== STATUS.ACK) return

    if (at && at.offset > this.lastAckOffset[at.partition]) {
      this.lastAckOffset[at.partition] = at.offset
      this.lastAckId[at.partition] = id
    }
    this.#stage(id, 1)
  }

  /**
   * Notes that the reader saw a record, in whichever order it and its acknowledgement arrive.
   * Only the first sighting of a timed id counts.
   * @param {number} id
   * @param {number} now monotonic ms
   * @returns {void}
   */
  observed(id, now) {
    if (id < 0 || id >= this.offered || id % this.sampleEvery !== 0) return
    const slot = id / this.sampleEvery
    if (this.stages[slot] & 2) return
    this.observedAt[slot] = now
    this.#stage(id, 2)
  }

  /**
   * @param {number} id a timed or untimed id
   * @param {number} bit 1 acknowledged, 2 observed
   * @returns {void}
   */
  #stage(id, bit) {
    if (id % this.sampleEvery !== 0) return
    const slot = id / this.sampleEvery
    this.stages[slot] |= bit
    if (this.stages[slot] !== 3) return
    this.stages[slot] |= 4
    const ms = this.observedAt[slot] - this.offeredAt[slot]
    if (ms >= 0) this.latencies[this.sampled++] = ms
    else this.invalid += 1
  }

  /**
   * @returns {{ sampleEvery: number, samples: number, invalid: number, p50: number|null,
   *   p90: number|null, p99: number|null, max: number|null }} the produce-to-consume latency of
   *   the timed ids that were both acknowledged and seen, in ms; `invalid` counts negative ones
   */
  latency() {
    const sorted = this.latencies.slice(0, this.sampled).sort()
    const at = (fraction) =>
      sorted.length === 0
        ? null
        : sorted[Math.min(sorted.length - 1, Math.ceil(fraction * sorted.length) - 1)]
    return {
      sampleEvery: this.sampleEvery,
      samples: this.sampled,
      invalid: this.invalid,
      p50: at(0.5),
      p90: at(0.9),
      p99: at(0.99),
      max: sorted.length === 0 ? null : sorted[sorted.length - 1],
    }
  }
}

/**
 * Counts offered ids and exact raw identities without growing storage. Linear probing uses at
 * most 75% of the slots; each slot is 18 bytes (int64 pid, int32 partition, int16 epoch, int32
 * sequence). At capacity 1,804,096 the table is 43,298,316 bytes, plus capacity bytes for id counts.
 * A sequence of -1 marks an empty slot; valid sequences are nonnegative integers below capacity.
 */
class ObservedLog {
  /**
   * Allocates all storage before sampling begins; at most capacity distinct raw keys are admitted.
   * @param {object} options
   * @param {number} options.capacity how many ids the offering ledger can hold
   */
  constructor({ capacity }) {
    this.capacity = capacity
    this.seen = new Uint8Array(capacity)
    const slots = Math.max(1, Math.ceil(capacity / 0.75))
    this.pids = new BigInt64Array(slots)
    this.partitions = new Int32Array(slots)
    this.epochs = new Int16Array(slots)
    this.sequences = new Int32Array(slots)
    this.reset()
  }

  /**
   * Forgets every observation and reuses all buffers. Empty sequences hide the old key fields.
   * @returns {void}
   */
  reset() {
    this.seen.fill(0)
    this.sequences.fill(-1)
    this.identities = 0
    this.records = 0
    this.repeatedIds = 0
    this.repeatedIdentities = 0
    this.unexpected = 0
    this.badSequences = 0
    this.identityOverflow = 0
  }

  /**
   * Takes one record. Duplicates remain detectable even after the distinct-key limit is reached.
   * Producer ids must be supplied losslessly; use bigint or a decimal string beyond safe integers.
   * @param {number} id the offered id; -1 for a value that is not this run's
   * @param {number} partition int32 Kafka partition
   * @param {bigint|string|number} pid signed int64 producer id
   * @param {number} epoch int16 producer epoch
   * @param {number} sequence integer producer sequence in [0, capacity)
   * @returns {void}
   * @throws {SyntaxError|TypeError|RangeError} if pid is not a lossless signed int64
   */
  add(id, partition, pid, epoch, sequence) {
    this.records += 1
    if (!(id >= 0 && id < this.capacity)) {
      this.unexpected += 1
      return
    }
    if (this.seen[id] > 0) this.repeatedIds += 1
    if (this.seen[id] < 2) this.seen[id] += 1

    if (!(Number.isInteger(sequence) && sequence >= 0 && sequence < this.capacity)) {
      this.badSequences += 1
      return
    }
    if (typeof pid === 'number' && !Number.isSafeInteger(pid)) {
      throw new RangeError('Producer id must be supplied as a lossless int64')
    }
    const producer = BigInt(pid)
    if (BigInt.asIntN(64, producer) !== producer) {
      throw new RangeError('Producer id is outside signed int64 range')
    }
    let hash = Number(BigInt.asUintN(32, producer))
    hash = Math.imul(hash ^ Number(BigInt.asIntN(32, producer >> 32n)), 0x85ebca6b)
    hash = Math.imul(hash ^ partition, 0x85ebca6b)
    hash = Math.imul(hash ^ epoch, 0x85ebca6b)
    hash = Math.imul(hash ^ sequence, 0x85ebca6b)
    hash ^= hash >>> 16
    let slot = (hash >>> 0) % this.sequences.length
    while (this.sequences[slot] !== -1) {
      if (
        this.pids[slot] === producer &&
        this.partitions[slot] === partition &&
        this.epochs[slot] === epoch &&
        this.sequences[slot] === sequence
      ) {
        this.repeatedIdentities += 1
        return
      }
      slot = (slot + 1) % this.sequences.length
    }
    if (this.identities === this.capacity) {
      this.identityOverflow += 1
      return
    }
    this.pids[slot] = producer
    this.partitions[slot] = partition
    this.epochs[slot] = epoch
    this.sequences[slot] = sequence
    this.identities += 1
  }
}

/**
 * Joins what was offered with what was read, by id.
 * Every acknowledged id must be read exactly once, an id of unknown outcome at most once, an id
 * known not to have been sent never, and no record may have an id that was never offered or repeat
 * a `(partition, pid, epoch, sequence)` identity. One pass over the dense tables: no set, no sort.
 * @param {OfferLedger} ledger
 * @param {ObservedLog} observed
 * @param {string} [view='log'] which reading this is, named in each violation
 * @returns {{ ok: boolean, violations: string[], counts: Record<string, number> }} `violations` names
 *   each invariant that broke and its first id
 */
const mergeDurability = (ledger, observed, view = 'log') => {
  const found = { missing: 0, repeatedAck: 0, repeatedUnknown: 0, unsentRead: 0, pending: 0 }
  const first = {}
  const count = { acknowledgedRead: 0, unknownRead: 0, unknownAbsent: 0 }
  const note = (kind, id) => {
    found[kind] += 1
    first[kind] ??= id
  }
  for (let id = 0; id < ledger.offered; id++) {
    const seen = observed.seen[id]
    switch (ledger.status[id]) {
      case STATUS.ACK:
        if (seen === 0) note('missing', id)
        else if (seen > 1) note('repeatedAck', id)
        else count.acknowledgedRead += 1
        break
      case STATUS.UNKNOWN:
      case STATUS.UNEXPECTED:
        if (seen > 1) note('repeatedUnknown', id)
        else if (seen === 1) count.unknownRead += 1
        else count.unknownAbsent += 1
        break
      case STATUS.UNSENT:
        if (seen > 0) note('unsentRead', id)
        break
      default:
        note('pending', id)
    }
  }

  const violations = []
  if (found.missing) {
    violations.push(
      `durability (${view}): acknowledged id ${first.missing} is missing from the log (${found.missing} missing)`
    )
  }
  if (found.repeatedAck) {
    violations.push(
      `durability (${view}): acknowledged id ${first.repeatedAck} was read more than once (${found.repeatedAck} ids)`
    )
  }
  if (found.repeatedUnknown) {
    violations.push(
      `durability (${view}): id ${first.repeatedUnknown} of unknown outcome was read more than once (${found.repeatedUnknown} ids)`
    )
  }
  if (found.unsentRead) {
    violations.push(
      `durability (${view}): id ${first.unsentRead} was refused before sending but is in the log (${found.unsentRead} ids)`
    )
  }
  if (found.pending) {
    violations.push(
      `durability (${view}): id ${first.pending} never got an outcome (${found.pending} ids)`
    )
  }
  if (observed.unexpected) {
    violations.push(
      `durability (${view}): ${observed.unexpected} records carry an id that was never offered`
    )
  }
  if (observed.repeatedIdentities) {
    violations.push(
      `durability (${view}): ${observed.repeatedIdentities} records repeat a (partition, pid, epoch, sequence) identity`
    )
  }
  if (observed.badSequences || observed.identityOverflow) {
    violations.push(
      `durability (${view}): ${observed.badSequences} sequences out of range and ${observed.identityOverflow} records exceed the raw-identity table capacity`
    )
  }

  return {
    ok: violations.length === 0,
    violations,
    counts: {
      ...count,
      ...found,
      records: observed.records,
      repeatedIds: observed.repeatedIds,
      repeatedIdentities: observed.repeatedIdentities,
      unexpected: observed.unexpected,
    },
  }
}

/**
 * A file of fixed 32-byte entries, one per record the live reader saw: id, sequence, producer id,
 * offset, partition, epoch and the ms since the journal began. It is written in batches from one
 * reused buffer and read back in bounded chunks, so the run keeps no list of what it has seen.
 */
class ObservationJournal {
  /**
   * @param {object} options
   * @param {string} options.file where the journal goes; truncated
   * @param {number} [options.entries=4096] entries held before a write
   * @param {Pick<typeof fs, 'openSync'|'writeSync'|'readSync'|'closeSync'>} [options.fs=node:fs]
   */
  constructor({ file, entries = 4096, fs: files = fs }) {
    this.files = files
    this.fd = files.openSync(file, 'w+')
    this.staged = Buffer.alloc(entries * ENTRY_BYTES)
    this.stagedBytes = 0
    this.written = 0
    this.count = 0
  }

  /**
   * @param {number} id
   * @param {number} partition
   * @param {number} pid
   * @param {number} epoch
   * @param {number} sequence
   * @param {number} offset
   * @param {number} atMs ms since the run began
   * @returns {void}
   */
  append(id, partition, pid, epoch, sequence, offset, atMs) {
    if (this.stagedBytes === this.staged.length) this.flush()
    const at = this.stagedBytes
    this.staged.writeUInt32LE(id >>> 0, at)
    this.staged.writeInt32LE(sequence, at + 4)
    this.staged.writeDoubleLE(pid, at + 8)
    this.staged.writeDoubleLE(offset, at + 16)
    this.staged.writeUInt16LE(partition, at + 24)
    this.staged.writeInt16LE(epoch, at + 26)
    this.staged.writeUInt32LE(Math.min(Math.max(0, Math.round(atMs)), 0xffffffff), at + 28)
    this.stagedBytes += ENTRY_BYTES
    this.count += 1
  }

  /**
   * Writes what is staged.
   * @returns {void}
   */
  flush() {
    if (this.stagedBytes === 0) return
    this.files.writeSync(this.fd, this.staged, 0, this.stagedBytes, this.written)
    this.written += this.stagedBytes
    this.stagedBytes = 0
  }

  /**
   * Reads the whole journal back, in chunks of `chunkEntries`, handing each entry on.
   * @param {(id: number, partition: number, pid: number, epoch: number, sequence: number) => void} add
   * @param {number} [chunkEntries=8192]
   * @returns {number} how many entries were replayed
   */
  replay(add, chunkEntries = 8192) {
    this.flush()
    const chunk = Buffer.alloc(chunkEntries * ENTRY_BYTES)
    let replayed = 0
    for (let position = 0; position < this.written;) {
      const read = this.files.readSync(
        this.fd,
        chunk,
        0,
        Math.min(chunk.length, this.written - position),
        position
      )
      if (read === 0) throw new Error(`The journal ends at ${position} of ${this.written} bytes`)
      for (let at = 0; at + ENTRY_BYTES <= read; at += ENTRY_BYTES) {
        add(
          chunk.readUInt32LE(at),
          chunk.readUInt16LE(at + 24),
          chunk.readDoubleLE(at + 8),
          chunk.readInt16LE(at + 26),
          chunk.readInt32LE(at + 4)
        )
        replayed += 1
      }
      position += read
    }
    return replayed
  }

  /**
   * Writes what is staged and closes the file; safe to call again.
   * @returns {void}
   */
  close() {
    if (this.fd === null) return
    try {
      this.flush()
    } finally {
      const { fd } = this
      this.fd = null
      this.files.closeSync(fd)
    }
  }
}

module.exports = {
  STATUS,
  LATENCY_SAMPLE_EVERY,
  VALUE_HEAD_BYTES,
  ENTRY_BYTES,
  encodeValue,
  decodeId,
  sequenceOf,
  OfferLedger,
  ObservedLog,
  ObservationJournal,
  mergeDurability,
}
