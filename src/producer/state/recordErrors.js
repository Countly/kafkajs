const { isNativeError, isProxy } = require('util').types
const Long = require('../../utils/long')
const { KafkaJSDeliveryTimeout, KafkaJSProtocolError } = require('../../errors')
const eosManager = require('../eosManager')

/** Error objects a Produce answer reported for a partition. */
const answered = new WeakSet()

/** Answered error objects reported more than once: never projected. */
const ambiguous = new WeakSet()

const TypedArray = Object.getPrototypeOf(Uint8Array.prototype)
const byteLengthOf = Object.getOwnPropertyDescriptor(TypedArray, 'byteLength').get
const copyBytes = TypedArray.set

/**
 * Records every partition error of one Produce answer before any of its batches settles. An
 * error object reported a second time, in this answer or a later one, is ambiguous for good.
 * @param {{ error: Error|null }[]} results the answer's per-partition results
 * @returns {true}
 */
const seeAnswers = (results) => {
  for (const { error } of results) {
    if (error == null) continue
    if (answered.has(error)) ambiguous.add(error)
    else answered.add(error)
  }
  return true
}

/**
 * The occurrence of one partition error matched to the batch it answers. It carries no record:
 * only the error and the batch's current token, which a cut replaces.
 * @param {Error} error the partition's error, already seen by `seeAnswers`
 * @param {{ token?: object|null }} batch
 * @returns {Readonly<{ error: Error, token: object }>}
 */
const answerOccurrence = (error, batch) => {
  batch.token ??= Object.freeze({})
  return Object.freeze({ error, token: batch.token })
}

/**
 * @param {Error} error
 * @returns {object[]|null} the error's own `recordErrors` array; an accessor is never called
 */
const rawEntriesOf = (error) => {
  if (isProxy(error)) return null
  const entries = Object.getOwnPropertyDescriptor(error, 'recordErrors')?.value
  return Array.isArray(entries) && !isProxy(entries) ? entries : null
}

/**
 * @param {*} index
 * @param {number} count the batch's record count
 * @returns {boolean} whether `index` names a record of the batch
 */
const resolves = (index, count) => Number.isInteger(index) && index >= 0 && index < count

/**
 * @param {Error} error a partition error
 * @param {number} count the record count of the batch it answers
 * @returns {*[]} the reported indices that name no record of that batch
 */
const unresolvedIndices = (error, count) => {
  const entries = rawEntriesOf(error)
  const unresolved = []
  for (let at = 0; at < (entries?.length ?? 0); at++) {
    const entry = Object.getOwnPropertyDescriptor(entries, at)?.value
    if (entry === null || typeof entry !== 'object' || isProxy(entry)) continue
    const index = Object.getOwnPropertyDescriptor(entry, 'batchIndex')
    if (index !== undefined && Object.hasOwn(index, 'value') && !resolves(index.value, count)) {
      unresolved.push(index.value)
    }
  }
  return unresolved
}

/** Identifies a resolved send result that must be delivered as a rejection. */
const RECORD_FAILURE = Symbol('record failure')

const commonFields = {
  name: 'string',
  message: 'string',
  retriable: 'boolean',
  category: 'string',
  helpUrl: 'string|undefined',
}
const protocolFields = {
  ...commonFields,
  type: 'string',
  code: 'number',
  topic: 'string|null',
  partition: 'number',
}
const responseFields = { errorMessage: 'string|null' }
const timeoutFields = { type: 'string', code: 'number', outcome: 'string' }

/**
 * Reads only known own data fields. Returns null if reconstructing the error would discard an
 * unknown property or carry an object from the source. The native stack is never inspected.
 * @param {Error} error
 * @param {object} required field names and their allowed value types
 * @param {object} optional field names and their allowed value types
 * @param {string[]} ignored keys handled separately or omitted from the public error
 * @returns {object|null}
 */
const detachedFields = (error, required, optional, ignored) => {
  for (const key of Reflect.ownKeys(error)) {
    if (!Object.hasOwn(required, key) && !Object.hasOwn(optional, key) && !ignored.includes(key)) {
      return null
    }
  }

  const fields = {}
  for (const [key, types] of Object.entries({ ...required, ...optional })) {
    const descriptor = Object.getOwnPropertyDescriptor(error, key)
    if (descriptor === undefined && Object.hasOwn(optional, key)) continue
    if (descriptor === undefined || !Object.hasOwn(descriptor, 'value')) return null
    const { value } = descriptor
    const kind = value === null ? 'null' : typeof value
    if (!types.split('|').includes(kind)) return null
    fields[key] = value
  }
  return fields
}

/**
 * Checks for an exact native instance before inspecting its own properties.
 * @param {Error} error
 * @param {Function} constructor
 * @returns {boolean}
 */
const isExactError = (error, constructor) =>
  isNativeError(error) && Object.getPrototypeOf(error) === constructor.prototype

/**
 * Detaches the public fields of an exact protocol answer and its optional delivery timeout.
 * @param {Error} delivered
 * @param {Error} answer
 * @returns {{ protocol: object, timeout: object|null }|null}
 */
const failureFields = (delivered, answer) => {
  if (!isExactError(answer, KafkaJSProtocolError) || eosManager.isExhaustedLookup(answer)) {
    return null
  }
  const cause = Object.getOwnPropertyDescriptor(answer, 'cause')
  if (cause === undefined || !Object.hasOwn(cause, 'value') || cause.value !== undefined)
    return null
  const protocol = detachedFields(answer, protocolFields, responseFields, [
    'stack',
    'cause',
    'recordErrors',
    'currentLeader',
    'nodeEndpoints',
  ])
  if (protocol === null || protocol.message === '') return null

  if (delivered === answer) return { protocol, timeout: null }
  if (
    !isExactError(delivered, KafkaJSDeliveryTimeout) ||
    eosManager.isExhaustedLookup(delivered) ||
    Object.getOwnPropertyDescriptor(delivered, 'cause')?.value !== answer
  ) {
    return null
  }
  const timeout = detachedFields(delivered, commonFields, timeoutFields, ['stack', 'cause'])
  if (
    timeout === null ||
    timeout.type !== protocol.type ||
    timeout.code !== protocol.code ||
    (timeout.outcome !== undefined && !['unsent', 'unknown'].includes(timeout.outcome))
  ) {
    return null
  }
  return { protocol, timeout }
}

/**
 * An enumerable property that materializes an ordinary array only when first read.
 * @param {object[]} entries the frozen raw entries in broker order
 * @param {Map<number, object>|null} records this send's detached culprit records
 * @returns {PropertyDescriptor}
 */
const recordErrorsProperty = (entries, records) => {
  let value
  return {
    configurable: true,
    enumerable: true,
    get() {
      value ??= entries.map((entry) => {
        const record = records?.get(entry.batchIndex)
        return record === undefined ? entry : { ...entry, record }
      })
      return value
    },
  }
}

/**
 * The append promise's fulfillment reaction. Only this send's detached data enters the stack
 * captured by the constructors; successes pass through and raw rejections bypass the reaction.
 * @param {object[]|object} result metadata or a tagged record failure
 * @returns {object[]} the send's metadata
 * @throws {KafkaJSProtocolError|KafkaJSDeliveryTimeout} the send's native record failure
 */
const deliverRecords = (result) => {
  if (Array.isArray(result) || result.kind !== RECORD_FAILURE) return result

  const { protocol, timeout, entries, records, mixedOutcome } = result
  const error = new KafkaJSProtocolError(protocol)
  Object.assign(error, protocol)
  Object.defineProperty(error, 'recordErrors', recordErrorsProperty(entries, records))
  if (timeout === null) throw error

  const expired = new KafkaJSDeliveryTimeout(timeout.message, {
    cause: error,
    outcome: timeout.outcome,
  })
  Object.assign(expired, timeout)
  if (mixedOutcome) {
    throw new KafkaJSDeliveryTimeout(
      `Some records of this send may have been written: ${expired.message}`,
      { cause: expired, outcome: 'unknown' }
    )
  }
  throw expired
}

/**
 * The bytes of one record field.
 * @param {string|Buffer|null|undefined} field
 * @returns {number}
 */
const bytesOf = (field) => {
  if (field == null) return 0
  return typeof field === 'string'
    ? Buffer.byteLength(field, 'utf8')
    : Reflect.apply(byteLengthOf, field, [])
}

/**
 * A record's timestamp as the decimal string the encoder wrote; none for a falsy one (the send
 * time was written) or an object other than this package's `Long`, whose methods are not called.
 * @param {*} timestamp
 * @returns {string|undefined}
 */
const timestampOf = (timestamp) =>
  !timestamp || (typeof timestamp === 'object' && !(timestamp instanceof Long))
    ? undefined
    : String(Long.fromValue(timestamp).value)

/**
 * A record's recognised fields, each byte field passed through `place`: key, value, then each
 * header value in the encoder's order, an array header's holes skipped.
 * @param {object} message a record as its batch carries it
 * @param {(field: *) => *} place
 * @returns {{ key: *, value: *, headers: object, timestamp?: string }}
 */
const shapeOf = ({ key, value, headers, timestamp }, place) => {
  const record = { key: place(key), value: place(value) }
  const placed = []
  for (const name of Object.keys(headers ?? {})) {
    const header = headers[name]
    if (Array.isArray(header)) {
      const parts = []
      header.forEach((part) => parts.push(place(part)))
      placed.push([name, parts])
    } else {
      placed.push([name, place(header)])
    }
  }
  record.headers = Object.fromEntries(placed)
  const written = timestampOf(timestamp)
  if (written !== undefined) record.timestamp = written
  return record
}

/**
 * Copies the recognised fields of the records at `indices` into one exact, unpooled store: each
 * key, value and header value of the result is a view of it. Strings are UTF-8 encoded; Buffer
 * bytes are read through the typed-array intrinsics, so no method or getter of a record's Buffer
 * runs. Extra record fields are never copied.
 * @param {object[]} messages the batch's records
 * @param {Iterable<number>} indices
 * @returns {Map<number, object>} each index's record
 */
const materialiseRecords = (messages, indices) => {
  let total = 0
  for (const index of indices) {
    shapeOf(messages[index], (field) => {
      total += bytesOf(field)
    })
  }
  const store = Buffer.allocUnsafeSlow(total)
  let offset = 0
  const place = (field) => {
    if (field == null) return null
    const length = bytesOf(field)
    if (typeof field === 'string') store.write(field, offset, length, 'utf8')
    else Reflect.apply(copyBytes, store, [field, offset])
    const view = store.subarray(offset, offset + length)
    offset += length
    return view
  }
  const records = new Map()
  for (const index of indices) records.set(index, shapeOf(messages[index], place))
  return records
}

/**
 * The position among `owners` of the one whose records include `index`: the last that starts at
 * or before it. An accumulator batch's first owner starts at 0.
 * @param {{ index: number }[]} owners in batch order
 * @param {number} index
 * @returns {number}
 */
const ownerOf = (owners, index) => {
  let low = 0
  let high = owners.length - 1
  while (low < high) {
    const middle = (low + high + 1) >> 1
    if (owners[middle].index <= index) low = middle
    else high = middle - 1
  }
  return low
}

/**
 * @param {object} entry
 * @param {string} key
 * @returns {PropertyDescriptor|undefined} the descriptor of an own, enumerable data property: the
 *   only kind an entry copy (`{ ...entry }`) and a plain read both carry faithfully
 */
const enumerableData = (entry, key) => {
  const descriptor = Object.getOwnPropertyDescriptor(entry, key)
  return descriptor?.enumerable && Object.hasOwn(descriptor, 'value') ? descriptor : undefined
}

/**
 * Builds only the caller-specific data needed at the send's delivery boundary.
 * @param {object[]} messages
 * @param {{ protocol: object, timeout: object|null }} fields
 * @param {object[]} entries the answer's frozen raw entries
 * @param {{ index: number }[]} owners
 * @returns {Map<object, object>|null} null when no entry names a record
 */
const projectEntries = (messages, fields, entries, owners) => {
  /** @type {Map<number, Set<number>>} owner position to the indices it owns */
  const owned = new Map()
  for (let at = 0; at < entries.length; at++) {
    const entry = Object.getOwnPropertyDescriptor(entries, at)?.value
    if (
      entry === null ||
      typeof entry !== 'object' ||
      isProxy(entry) ||
      Object.getPrototypeOf(entry) !== Object.prototype ||
      !Object.isFrozen(entry) ||
      Reflect.ownKeys(entry).length !== 2
    ) {
      return null
    }
    const index = enumerableData(entry, 'batchIndex')
    const message = enumerableData(entry, 'errorMessage')
    if (
      index === undefined ||
      message === undefined ||
      typeof index.value !== 'number' ||
      (message.value !== null && typeof message.value !== 'string')
    ) {
      return null
    }
    if (!resolves(index.value, messages.length)) continue
    const owner = ownerOf(owners, index.value)
    if (!owned.has(owner)) owned.set(owner, new Set())
    owned.get(owner).add(index.value)
  }
  if (owned.size === 0) return null

  const projections = new Map()
  owners.forEach((owner, at) => {
    const indices = owned.get(at)
    projections.set(owner, {
      kind: RECORD_FAILURE,
      ...fields,
      entries,
      records: indices === undefined ? null : materialiseRecords(messages, indices),
      outcome: fields.timeout?.outcome,
      mixedOutcome: false,
    })
  })
  return projections
}

/**
 * Each owning send's detached failure for a matched, unambiguous answer to this batch. The
 * caller keeps the original error if provenance, error shape or record data is unsuitable.
 * @param {{ token?: object|null, messages: object[] }} batch
 * @param {Error} delivered
 * @param {{ error: Error, token: object }} occurrence
 * @param {{ index: number }[]} owners the batch's accumulator settlers, in batch order
 * @returns {Map<object, object>|null}
 */
const projectionsOf = (batch, delivered, occurrence, owners) => {
  const { error: answer, token } = occurrence
  if (owners.length === 0 || ambiguous.has(answer) || token !== batch.token) return null
  try {
    const fields = failureFields(delivered, answer)
    const entries = fields === null ? null : rawEntriesOf(answer)
    if (
      entries === null ||
      Object.getPrototypeOf(entries) !== Array.prototype ||
      !Object.isFrozen(entries) ||
      Reflect.ownKeys(entries).length !== entries.length + 1
    ) {
      return null
    }
    return projectEntries(batch.messages, fields, entries, owners)
  } catch {
    return null
  }
}

module.exports = {
  seeAnswers,
  answerOccurrence,
  unresolvedIndices,
  projectionsOf,
  deliverRecords,
}
