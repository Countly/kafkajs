const Long = require('../../utils/long')
const { KafkaJSDeliveryTimeout } = require('../../errors')
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
  const entries = Object.getOwnPropertyDescriptor(error, 'recordErrors')?.value
  return Array.isArray(entries) ? entries : null
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
const unresolvedIndices = (error, count) =>
  (rawEntriesOf(error) ?? [])
    .map(({ batchIndex }) => batchIndex)
    .filter((batchIndex) => !resolves(batchIndex, count))

/**
 * The links a projection copies, from the delivered error down to the answer: the answer itself,
 * or a delivery timeout whose own `cause` data property is the answer. No getter runs.
 * @param {Error} delivered
 * @param {Error} answer
 * @returns {Error[]|null}
 */
const linksTo = (delivered, answer) => {
  if (delivered === answer) return [answer]
  const cause =
    delivered instanceof KafkaJSDeliveryTimeout
      ? Object.getOwnPropertyDescriptor(delivered, 'cause')
      : undefined
  return cause?.value === answer ? [delivered, answer] : null
}

/**
 * A copy of an error with its prototype and every own property descriptor, string and symbol
 * keys, with `substitutes` put in place of their values before the copy is created. `stack` is the
 * source's stack text as a data property, since an engine-installed stack accessor reads its
 * receiver.
 * @param {Error} link
 * @param {{ cause?: Error, recordErrors?: object[] }} substitutes
 * @returns {Error}
 */
const copyLink = (link, substitutes) => {
  const descriptors = Object.getOwnPropertyDescriptors(link)
  for (const [key, value] of Object.entries(substitutes)) {
    descriptors[key] = { ...descriptors[key], value }
  }
  descriptors.stack = { value: link.stack, writable: true, enumerable: false, configurable: true }
  return Object.create(Object.getPrototypeOf(link), descriptors)
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
 * Each owner's own error. Every owner gets its own copy of each link, the answer's copy carrying
 * its own `recordErrors` array: a fresh entry with `record` for each index it owns, the shared
 * raw entry for every other.
 * @param {object[]} messages
 * @param {Error[]} links
 * @param {object[]} entries the answer's raw entries
 * @param {{ index: number }[]} owners
 * @returns {Map<object, Error>|null} null when no entry names a record
 */
const projectEntries = (messages, links, entries, owners) => {
  /** @type {Map<number, Set<number>>} owner position to the indices it owns */
  const owned = new Map()
  for (const { batchIndex } of entries) {
    if (!resolves(batchIndex, messages.length)) continue
    const at = ownerOf(owners, batchIndex)
    if (!owned.has(at)) owned.set(at, new Set())
    owned.get(at).add(batchIndex)
  }
  if (owned.size === 0) return null

  const projections = new Map()
  owners.forEach((owner, at) => {
    const indices = owned.get(at)
    const records = indices === undefined ? null : materialiseRecords(messages, indices)
    const recordErrors = entries.map((entry) =>
      records?.has(entry.batchIndex) ? { ...entry, record: records.get(entry.batchIndex) } : entry
    )
    const answer = copyLink(links[links.length - 1], { recordErrors })
    projections.set(owner, links.length === 1 ? answer : copyLink(links[0], { cause: answer }))
  })
  return projections
}

/**
 * Each owning send's own error for a batch that fails with `delivered`, made from the answer
 * occurrence its settlement received. Nothing is projected, and every caller gets `delivered`
 * itself, when the answer was reported more than once, the batch was cut since the answer, the
 * delivered error is neither the answer nor a delivery timeout built around it, a link to copy is
 * a coordinator lookup tracked by identity, the answer names no record of the batch, the batch has
 * no accumulator owner, or copying the records fails.
 * @param {{ token?: object|null, messages: object[] }} batch
 * @param {Error} delivered
 * @param {{ error: Error, token: object }} occurrence
 * @param {{ index: number }[]} owners the batch's accumulator settlers, in batch order
 * @returns {Map<object, Error>|null}
 */
const projectionsOf = (batch, delivered, occurrence, owners) => {
  const { error: answer, token } = occurrence
  if (owners.length === 0 || ambiguous.has(answer) || token !== batch.token) return null
  const links = linksTo(delivered, answer)
  const entries = rawEntriesOf(answer)
  if (links === null || links.some(eosManager.isExhaustedLookup) || entries === null) return null
  try {
    return projectEntries(batch.messages, links, entries, owners)
  } catch {
    return null
  }
}

module.exports = {
  seeAnswers,
  answerOccurrence,
  unresolvedIndices,
  projectionsOf,
}
