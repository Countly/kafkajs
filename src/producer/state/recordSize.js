const { KafkaJSNonRetriableError, KafkaJSRecordTooLargeError } = require('../../errors')
const { Codecs, Types } = require('../../protocol/message/compression')
const snappy = require('../../protocol/message/compression/snappy')

/**
 * The sizes the producer charges against `bufferMemory`. A record's wire size is the Java client's
 * upper bound for it: `DefaultRecord.MAX_RECORD_OVERHEAD` (21) plus its key, value and headers with
 * their length varints, a null or absent field counting one byte. What the producer holds for it is
 * more: the record object, its settler share and the arrays it sits in, so its held size adds 64
 * bytes, and 16 per header. A send adds 1536 bytes (its holder, settlers and promises, and while it
 * waits for room its wait) and the
 * backing stores its Buffer views keep alive beyond the bytes it uses; a batch adds 1024 (its
 * object, timer and arrays). The constants are the measured fit of probes P7 and P12, refitted
 * only through `scripts/benchmarks/admissionAccounting.js`, never the formula. Records are those
 * the encoder accepts: keys, values and header values are strings, Buffers, null or absent.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/common/record/internal/DefaultRecord.java
 */

/** `DefaultRecord.MAX_RECORD_OVERHEAD`: the most a record's length, attributes and deltas take. */
const RECORD_OVERHEAD = 21
/** What a held record costs beyond its wire size. */
const HELD_RECORD = 64
/** What each header of a held record costs beyond its wire size. */
const HELD_HEADER = 16
/** What a send costs beyond its records. */
const SEND_CHARGE = 1536
/** What a batch costs, charged without waiting when the accumulator creates it. */
const BATCH_DEBIT = 1024
/** A Buffer is copied when its backing store is larger than this and than 16 times the Buffer. */
const COPY_STORES_ABOVE = 16384
const COPY_STORES_TIMES = 16
/** The intrinsic length of a typed array, which an own `length` property cannot change. */
const lengthOf = Object.getOwnPropertyDescriptor(
  Object.getPrototypeOf(Uint8Array.prototype),
  'length'
).get

/**
 * Java's `ByteUtils.sizeOfVarint` of a length: the bytes of its zigzag varint.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/common/utils/ByteUtils.java
 * @param {number} length from 0 to 2^31 - 1
 * @returns {number} from 1 to 5
 */
const varintSize = (length) =>
  length < 64 ? 1 : length < 8192 ? 2 : length < 1048576 ? 3 : length < 134217728 ? 4 : 5

/**
 * @param {string|Buffer|null|undefined} field one the encoder accepts
 * @param {{ buffer(view: Buffer): void }} tally told about the field when it is a Buffer
 * @returns {number} its bytes, UTF-8 for a string; -1 for null or absent
 */
const bytesOf = (field, tally) => {
  if (field == null) {
    return -1
  }
  if (typeof field === 'string') {
    return Buffer.byteLength(field)
  }
  tally.buffer(field)
  return field.length
}

/**
 * @param {number} bytes a field's bytes; -1 for null or absent
 * @returns {number} its bytes on the wire with their length varint; 1 for null or absent
 */
const sized = (bytes) => (bytes < 0 ? 1 : varintSize(bytes) + bytes)

/**
 * Reads a record's key, value and headers once, as the record encoder writes them: each own key
 * of its headers, and an array value as one header per element, a hole none.
 * @param {object} record one the encoder accepts
 * @param {{ headers: number, payload: number, buffer(view: Buffer): void }} tally told about each
 *   Buffer among the fields, in that order, then given the record's header count and the bytes of
 *   its key and value
 * @returns {number} its wire size
 */
const walk = ({ key, value, headers }, tally) => {
  const keyBytes = bytesOf(key, tally)
  const valueBytes = bytesOf(value, tally)
  let size = RECORD_OVERHEAD + sized(keyBytes) + sized(valueBytes)
  let count = 0
  if (headers != null) {
    for (const name of Object.keys(headers)) {
      const header = headers[name]
      const nameSize = sized(Buffer.byteLength(name))
      if (!Array.isArray(header)) {
        count += 1
        size += nameSize + sized(bytesOf(header, tally))
        continue
      }
      for (let i = 0; i < header.length; i++) {
        if (i in header) {
          count += 1
          size += nameSize + sized(bytesOf(header[i], tally))
        }
      }
    }
  }
  tally.headers = count
  tally.payload = (keyBytes < 0 ? 0 : keyBytes) + (valueBytes < 0 ? 0 : valueBytes)
  return size + varintSize(count)
}

/** The tally of `wire` and `held`, which keep only the header count; read right after `walk`. */
const COUNT = { headers: 0, payload: 0, buffer() {} }

/**
 * The Java client's upper bound of a record's size in a batch (`recordSizeUpperBound`).
 * @param {object} record
 * @returns {number} bytes, at least 24
 */
const wire = (record) => walk(record, COUNT)

/**
 * What the producer holds for a record: its wire size, 64, and 16 per header.
 * @param {object} record
 * @returns {number} bytes, at least 88
 */
const held = (record) => walk(record, COUNT) + HELD_RECORD + HELD_HEADER * COUNT.headers

/**
 * Adds the bytes from `start` to `end` of a backing store to those this send's earlier views of it
 * cover.
 * @param {number[]} spans the covered bytes, as sorted, disjoint `[start, end, start, end, ...]`
 *   pairs, merged in place
 * @param {number} start
 * @param {number} end
 * @returns {number} how many of those bytes no earlier view covered
 */
const cover = (spans, start, end) => {
  let low = 0
  let high = spans.length / 2
  while (low < high) {
    const middle = (low + high) >> 1
    if (spans[2 * middle + 1] < start) {
      low = middle + 1
    } else {
      high = middle
    }
  }
  const first = 2 * low
  let last = first
  let from = start
  let to = end
  let added = end - start
  for (; last < spans.length && spans[last] <= end; last += 2) {
    added -= Math.max(0, Math.min(end, spans[last + 1]) - Math.max(start, spans[last]))
    from = Math.min(from, spans[last])
    to = Math.max(to, spans[last + 1])
  }
  spans.splice(first, last - first, from, to)
  return added
}

/**
 * What one Buffer of a send changes in the bytes its backing stores keep alive beyond what its
 * views show, the store being the `ArrayBuffer` itself: the first view of a store adds the bytes it
 * does not show, and a later view takes back the bytes it shows that no earlier view did.
 * @param {Map<ArrayBufferLike, number[]>} stores by backing store, the bytes the send's views cover
 * @param {Buffer} view
 * @returns {number}
 */
const storeCharge = (stores, view) => {
  const start = view.byteOffset
  const end = start + view.length
  const spans = stores.get(view.buffer)
  if (spans === undefined) {
    stores.set(view.buffer, [start, end])
    return view.buffer.byteLength - view.length
  }
  return -cover(spans, start, end)
}

/**
 * A send's charge (see `chargeOf`), added up a record at a time: `add` reads each record once
 * (`walk`) and leaves that record's wire size, share and key and value bytes on the charge.
 */
class SendCharge {
  /** the charge of the records added so far, with the send's own 1536 */
  total = SEND_CHARGE
  /** the wire size of the record last added */
  wire = 0
  /** that record's share (see `share`) */
  share = 0
  /** the bytes of that record's key and value, which `batchSize` counts */
  payload = 0
  /** that record's header count */
  headers = 0
  #stores = 0
  #buffered = 0
  /** @type {Map<ArrayBufferLike, number[]>|null} by backing store, the bytes the send's views cover */
  #covered = null

  /** @returns {number} the part no batch releases: 1536, the stores and the bytes of its Buffers */
  get shell() {
    return SEND_CHARGE + this.#stores + this.#buffered
  }

  /**
   * Adds one record, with its Buffers' backing stores.
   * @param {object} record one the encoder accepts
   * @returns {number} its held size (see `held`)
   */
  add(record) {
    const buffered = this.#buffered
    const stores = this.#stores
    this.wire = walk(record, this)
    const size = this.wire + HELD_RECORD + HELD_HEADER * this.headers
    this.share = size - (this.#buffered - buffered)
    this.total += size + this.#stores - stores
    return size
  }

  /**
   * Counts one Buffer of the record being added, and the bytes of its store it keeps alive.
   * @param {Buffer} view
   * @returns {void}
   */
  buffer(view) {
    this.#covered ??= new Map()
    this.#stores += storeCharge(this.#covered, view)
    this.#buffered += view.length
  }
}

/**
 * A record's share of its send's charge, released when the batch carrying it retires: its held
 * size without the bytes of its Buffers, which the send's shell pays for (see `chargeOf`).
 * @param {object} record
 * @returns {number}
 */
const share = (record) => {
  const charge = new SendCharge()
  charge.add(record)
  return charge.share
}

/**
 * What admitting a send charges: the held size of every record, 1536, and every backing store its
 * Buffers keep alive beyond the bytes they show. Views are grouped by store, the store being the
 * `ArrayBuffer` itself: a store several of the send's views share is counted once, and a store
 * other sends also use is counted by each of them. The send's shell is the 1536, the stores and the
 * bytes of its Buffers: the part no batch releases, held until the last batch carrying the send's
 * records retires and the last wait on it ends; the rest is the records' shares (see `share`).
 *
 * The records are charged in order, each with its stores, and the charge of every prefix is
 * checked against `limit` before the next record is read: a record never takes back more than its
 * own held size adds, so the charge only grows, and the first prefix above `limit` ends the pass and
 * is returned, for `acquire` to refuse as `scope: 'send'`. A total within `limit` is exact, and no
 * sum goes past `limit` by more than one record and its stores.
 * @param {{ topic: string, messages: object[] }[]} topicMessages
 * @param {number} limit the producer's `bufferMemory`
 * @returns {{ total: number, shell: number }} `total` is above `limit` when the send cannot fit
 * @throws {KafkaJSRecordTooLargeError} for a record whose held size alone is above `limit`
 */
const chargeOf = (topicMessages, limit) => {
  const charge = new SendCharge()
  for (const { topic, messages } of topicMessages) {
    for (const message of messages) {
      const size = charge.add(message)
      if (size > limit) {
        throw new KafkaJSRecordTooLargeError(
          `A record of ${size} bytes for topic "${topic}" can never fit in bufferMemory (${limit} bytes)`,
          { reason: 'bufferMemory', scope: 'record', size, limit, topic }
        )
      }
      if (charge.total > limit) {
        return { total: charge.total, shell: 0 }
      }
    }
  }
  return { total: charge.total, shell: charge.shell }
}

/**
 * @param {Buffer} view
 * @returns {boolean} whether it is a small view over a large backing store: one above 16 KiB and
 *   above 16 times the view, which a send copies so as not to keep the store alive
 */
const pinsStore = (view) => {
  const { byteLength } = view.buffer
  return byteLength > COPY_STORES_ABOVE && byteLength > COPY_STORES_TIMES * lengthOf.call(view)
}

/**
 * The copy of one send's Buffers that would pin a large store (`pinsStore`). While the send's own
 * records are built, `note` remembers each such Buffer by the own container and key it was put
 * at, with its length read once; `copy` then copies them all into one store of the sum of those
 * lengths and puts each one's slice in its place. Every other Buffer stays the caller's own, as in
 * 2.2.4, so its bytes still reach the wire if the caller changes them; a send with nothing to copy
 * allocates no store. The bytes are captured after every record of the send has been read, with
 * intrinsics only: no method or iterator of the caller runs during the copy.
 *
 * The store is held only through the slices in the send's records, which cover it whole, so
 * `chargeOf` charges it no excess; it may be collected before the send's charge is released when
 * the records still held carry no slice of it. Keeping any slice (a record kept by a custom
 * partitioner or from a delivery error, or the slice passed again in another send) keeps the whole
 * store alive; a later send that carries such a slice unchanged is charged the bytes of the store
 * it does not show. A send that cannot fit is still refused, but which record the error names and
 * the size it reports can differ from a copy per view.
 */
class SendCopy {
  /** @type {Array<object|string|number|Buffer>|null} flat slots: container, key, view, length */
  #slots = null
  #bytes = 0

  /**
   * Remembers `container[key]` for the copy when it is a Buffer that pins a large store.
   * @param {object} container the producer's own record, header object or header-value array
   * @param {string|number} key
   * @returns {void}
   */
  note(container, key) {
    const view = container[key]
    if (!Buffer.isBuffer(view) || !pinsStore(view)) {
      return
    }
    const length = lengthOf.call(view)
    this.#slots ??= []
    this.#slots.push(container, key, view, length)
    this.#bytes += length
  }

  /**
   * Copies every remembered Buffer into one store and puts its slice in its place.
   * @returns {void}
   * @throws {KafkaJSNonRetriableError} when a Buffer's length changed since it was remembered
   */
  copy() {
    const slots = this.#slots
    if (slots === null) {
      return
    }
    const store = Buffer.allocUnsafeSlow(this.#bytes)
    let offset = 0
    for (let i = 0; i < slots.length; i += 4) {
      const view = slots[i + 2]
      const length = slots[i + 3]
      if (lengthOf.call(view) !== length) {
        throw new KafkaJSNonRetriableError('A Buffer changed size while its send was being read')
      }
      store.set(view, offset)
      slots[i][slots[i + 1]] = store.subarray(offset, offset + length)
      offset += length
    }
  }
}

/** `DefaultRecordBatch.RECORD_BATCH_OVERHEAD`: the header of a v2 record batch. */
const BATCH_OVERHEAD = 61
/** What one partition adds to a Produce request beside its batches: its index and lengths. */
const PARTITION_FRAMING = 16
/** The wire size of a record with no key, an empty value and no header. */
const EMPTY_RECORD = 24
/** The codecs this client registers itself; a factory assigned later is the caller's plugin. */
const BUILT_IN = { ...Codecs }

/**
 * @param {*} text a client id, transactional id or topic name; null and absent count 0
 * @returns {number} its UTF-8 bytes
 */
const textBytes = (text) => (text == null ? 0 : Buffer.byteLength(String(text)))

/**
 * What a Produce request costs beside its topics: the length prefix, the request header and the
 * body's own fields, at most 32 bytes in every version, plus the client id and transactional id.
 * @param {string|null} [clientId='kafkajs'] the connection's client id, its default when absent
 * @param {string|null} [transactionalId]
 * @returns {number}
 */
const framing = (clientId = 'kafkajs', transactionalId) =>
  32 + textBytes(clientId) + textBytes(transactionalId)

/**
 * What a topic costs in a Produce request beside its partitions: its name or id, its partition
 * array's length and tags, at most 32 bytes plus the name.
 * @param {string} name
 * @returns {number}
 */
const topicFraming = (name) => 32 + textBytes(name)

/**
 * The bytes of records as a batch carries them, uncompressed: their wire sizes.
 * @param {object[]} records
 * @returns {number}
 */
const recordsBytes = (records) => {
  let bytes = 0
  for (const record of records) {
    bytes += wire(record)
  }
  return bytes
}

/**
 * A v2 batch of `records`, uncompressed, at most: its header and every record's wire size.
 * @param {object[]} records
 * @returns {number}
 */
const batchBytes = (records) => BATCH_OVERHEAD + recordsBytes(records)

/**
 * The most `n` bytes of records can take once a built-in codec compressed them, from each codec's
 * own worst case; `n` for no compression, and for a codec the caller registered, whose bound is
 * unknown (the write-boundary guard catches what it adds).
 * @see https://github.com/madler/zlib/blob/v1.3.1/deflate.c (`deflateBound`, gzip wrapper)
 * @see https://github.com/facebook/zstd/blob/dev/lib/zstd.h (`ZSTD_COMPRESSBOUND`)
 * @see https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md
 * @param {number} codec the request's `compression`, one of `Types`
 * @param {number} n uncompressed bytes
 * @returns {number}
 */
const encodedBound = (codec, n) => {
  if (Codecs[codec] === undefined || Codecs[codec] !== BUILT_IN[codec]) {
    return n
  }
  switch (codec) {
    case Types.GZIP:
      return n + Math.floor(n / 4096) + Math.floor(n / 16384) + Math.floor(n / 33554432) + 25
    case Types.ZSTD:
      return n + Math.floor(n / 256) + (n < 131072 ? Math.floor((131072 - n) / 2048) : 0)
    case Types.LZ4:
      return n + 8 * Math.ceil(n / 65536) + 27
    default:
      return snappy.encodedBound(n)
  }
}

/**
 * The smallest `maxRequestSize` a producer with these identifiers takes: its request framing, one
 * batch header and one empty record.
 * @param {string|null} [clientId]
 * @param {string|null} [transactionalId]
 * @returns {number}
 */
const minimumRequestSize = (clientId, transactionalId) =>
  framing(clientId, transactionalId) + BATCH_OVERHEAD + EMPTY_RECORD

/**
 * One producer's plan of its Produce requests under `maxRequestSize`, on the raw sizes of the
 * records plus each built-in codec's worst case (`encodedBound`): a request is planned as its
 * framing, each topic's framing, and per batch its partition's framing, its header and its
 * records' bound. The plan only ever overestimates a request the encoder builds from the same
 * batches, so a batch planned alone always fits; the write-boundary guard compares the real frame.
 * Without `maxRequestSize` nothing is limited.
 */
class RequestBudget {
  #framing
  /** @type {Map<string, number>} by codec, whether it is built in, and topic name bytes */
  #capacities = new Map()

  /**
   * @param {object} [options]
   * @param {number} [options.maxRequestSize=Infinity] the largest frame, its length prefix included
   * @param {string|null} [options.clientId] absent means the connection's default, `kafkajs`
   * @param {string|null} [options.transactionalId]
   */
  constructor({ maxRequestSize = Infinity, clientId, transactionalId } = {}) {
    this.maxRequestSize = maxRequestSize
    this.#framing = framing(clientId, transactionalId)
  }

  /** @returns {number} the bytes the topics and batches of one request may plan */
  get maxRequestBytes() {
    return this.maxRequestSize - this.#framing
  }

  /**
   * @param {string} topic
   * @returns {number} the planned bytes of the topic's framing in one request
   */
  topicBytes(topic) {
    return topicFraming(topic)
  }

  /**
   * @param {{ compression?: number, messages: object[], wire?: number }} batch `wire` is its
   *   records' wire bytes, computed from `messages` when absent
   * @returns {number} the planned bytes of the batch in a request, its topic's framing apart
   */
  batchBytes({ compression, messages, wire: bytes = recordsBytes(messages) }) {
    return PARTITION_FRAMING + BATCH_OVERHEAD + encodedBound(compression, bytes)
  }

  /**
   * The most wire bytes of records one batch of `topic` may carry for a request of that batch
   * alone to fit: the largest `R` whose plan, `framing + topicFraming + 16 + 61 +
   * encodedBound(codec, R)`, is at most `maxRequestSize`. Found by bisection over the monotone
   * bound, and kept per codec and topic name length.
   * @param {string} topic
   * @param {number} [codec]
   * @returns {number} Infinity without a limit; -1 when not even an empty batch fits
   */
  chunkCapacity(topic, codec) {
    if (this.maxRequestSize === Infinity) {
      return Infinity
    }
    const fixed = this.#framing + topicFraming(topic) + PARTITION_FRAMING + BATCH_OVERHEAD
    const key = `${codec}/${Codecs[codec] === BUILT_IN[codec]}/${fixed}`
    let capacity = this.#capacities.get(key)
    if (capacity === undefined) {
      let low = -1
      let high = Math.max(-1, this.maxRequestSize - fixed)
      while (low < high) {
        const middle = Math.ceil((low + high) / 2)
        if (fixed + encodedBound(codec, middle) <= this.maxRequestSize) {
          low = middle
        } else {
          high = middle - 1
        }
      }
      capacity = low
      this.#capacities.set(key, capacity)
    }
    return capacity
  }
}

module.exports = {
  wire,
  held,
  share,
  chargeOf,
  SendCharge,
  SendCopy,
  varintSize,
  framing,
  topicFraming,
  recordsBytes,
  batchBytes,
  encodedBound,
  minimumRequestSize,
  RequestBudget,
  RECORD_OVERHEAD,
  SEND_CHARGE,
  BATCH_DEBIT,
  BATCH_OVERHEAD,
  PARTITION_FRAMING,
}
