const { isDeepStrictEqual } = require('util')
const { performance } = require('perf_hooks')
const {
  KafkaJSCorruptRecordError,
  KafkaJSInvalidLongError,
  KafkaJSInvalidVarIntError,
  KafkaJSNotImplemented,
  KafkaJSPartialMessageError,
  KafkaJSProtocolError,
} = require('../../../errors')
const { Codecs } = require('../../message/compression')
const { codecOf, project } = require('./fuzzVectors')

/**
 * The typed errors a decoder may fail a malformed body with, by class: the varint errors carry the
 * `name` of their base class, so a name cannot tell them apart. Anything else, a `RangeError` and a
 * `TypeError` first, is a defect.
 */
const TYPED_ERRORS = [
  KafkaJSPartialMessageError,
  KafkaJSInvalidVarIntError,
  KafkaJSInvalidLongError,
  KafkaJSCorruptRecordError,
  KafkaJSProtocolError,
  KafkaJSNotImplemented,
]

/** The longest a decoder may take to refuse an impossible count. */
const PROMPT_MS = 50

/** How often a decoder that was slow once may try again before it is called slow. */
const PROMPT_ATTEMPTS = 3

const PROTOTYPE_NAMES = Object.getOwnPropertyNames(Object.prototype).join()

// Compressed batches are outside the corpus: a body mutated into one reaches a codec that
// answers as an unconfigured one does, with a typed error.
for (const type of Object.keys(Codecs)) {
  Codecs[type] = () => {
    throw new KafkaJSNotImplemented('compression is outside the corpus')
  }
}

/**
 * @param {*} value
 * @returns {number} how many array elements `value` holds, at any depth: the units a decoder's
 *   allocation is counted in
 */
const elementsIn = (value) => {
  if (Buffer.isBuffer(value) || value === null || typeof value !== 'object') {
    return 0
  }

  const items = Array.isArray(value) ? value : Object.values(value)
  return (
    (Array.isArray(value) ? value.length : 0) + items.reduce((n, item) => n + elementsIn(item), 0)
  )
}

/**
 * @param {object} decoded
 * @returns {string|null} what is missing from a decoded response that a caller iterates without
 *   looking: the topics, their partitions and each partition's messages are arrays, however a
 *   body spelled a null or empty list
 */
const missingArray = ({ responses }) => {
  if (!Array.isArray(responses)) return 'responses'
  for (const { partitions } of responses) {
    if (!Array.isArray(partitions)) return 'partitions'
    if (!partitions.every(({ messages }) => Array.isArray(messages))) return 'messages'
  }
  return null
}

/**
 * @param {object} decoded
 * @returns {string|null} the first header of a decoded record that is not data: a value has to be
 *   a `Buffer`, null, or an array of them, which a header named like a property of every object
 *   is not when the decoder reads it through the prototype; a legacy message has no headers
 */
const headerNotKeptAsData = ({ responses }) => {
  const isData = (value) => value === null || Buffer.isBuffer(value)
  for (const { partitions } of responses) {
    for (const { messages } of partitions) {
      for (const { headers = {} } of messages) {
        for (const [name, value] of Object.entries(headers)) {
          if (!isData(value) && !(Array.isArray(value) && value.every(isData))) return name
        }
      }
    }
  }
  return null
}

/**
 * Decodes the body, then parses what was decoded, as `Connection` does.
 * @param {object} fuzzCase
 * @param {Buffer} input
 * @param {{ decodes: number }} counter counts the calls of the codec's `decode`: the proof that the
 *   target ran
 * @returns {Promise<{ decoded?: object, error?: Error }>} what was decoded, if anything was, and
 *   the error that ended it
 */
const run = async (fuzzCase, input, counter) => {
  const codec = fuzzCase.codec || codecOf(fuzzCase)
  const decode = (...args) => {
    counter.decodes++
    return codec.decode(...args)
  }
  let decoded
  try {
    decoded = await decode(input, fuzzCase.options)
    await codec.parse(decoded)
    return { decoded }
  } catch (error) {
    return { decoded, error }
  }
}

/**
 * What a decoded value breaks, if it breaks anything: allocation beyond the bytes it came from, a
 * change to `Object.prototype`, a header that is not data, an outcome other than the exact one the
 * case was built to have, or the whole body's values from a body with bytes removed.
 * @param {object} decoded
 * @param {Buffer} input
 * @param {object} fuzzCase
 * @returns {string|null}
 */
const brokenBy = (decoded, input, { expect: expected, control, tailIsEmptyTags }) => {
  if (elementsIn(decoded) > input.length) {
    return `${elementsIn(decoded)} elements from ${input.length} bytes`
  }
  if (Object.getOwnPropertyNames(Object.prototype).join() !== PROTOTYPE_NAMES) {
    return 'Object.prototype changed'
  }
  const missing = missingArray(decoded)
  if (missing !== null) {
    return `${missing} is not an array`
  }
  const header = headerNotKeptAsData(decoded)
  if (header !== null) {
    return `header ${header} is not data`
  }

  if (expected && expected.offsets) {
    const [partition] = decoded.responses[0].partitions
    const found = {
      offsets: partition.messages.map(({ offset }) => offset),
      corruption: partition.corruption ? partition.corruption.reason : null,
    }
    if (!isDeepStrictEqual(found, { offsets: expected.offsets, corruption: expected.corruption })) {
      return `decoded ${JSON.stringify(found)}, expected ${JSON.stringify(expected)}`
    }
  }

  if (
    control &&
    !tailIsEmptyTags &&
    isDeepStrictEqual(JSON.parse(JSON.stringify(project(decoded))), control)
  ) {
    return 'a body with bytes removed decoded to the values of the whole body'
  }
  return null
}

/**
 * Decodes one case through its codec and classifies the outcome.
 * @param {object} fuzzCase
 * @param {number} fuzzCase.version the Fetch version
 * @param {Uint8Array} fuzzCase.buffer the body
 * @param {{ checkCrcs?: boolean }} [fuzzCase.options] the codec's options
 * @param {{ offsets?: string[], corruption?: string|null, error?: string, decoded?: boolean }}
 *   [fuzzCase.expect] the exact outcome of a case built field by field: the offsets of the first
 *   partition's messages and the reason of its corruption marker, the name of the error, or only
 *   that it decodes
 * @param {object} [fuzzCase.control] the vector's control, for a body with bytes removed
 * @param {boolean} [fuzzCase.tailIsEmptyTags] whether every byte removed is an empty tag buffer
 * @param {boolean} [fuzzCase.prompt] whether the decoder has to answer within `PROMPT_MS`
 * @param {{ decode: Function, parse: Function }} [fuzzCase.codec] a codec to use instead of the
 *   version's, which a case run in this thread can give to show what the oracle makes of it
 * @returns {Promise<{ ran: 0|1, category: string, violation: string|null }>} `category` is
 *   `decoded` or `typed:<error class>`; `ran` is 1 when the target's `decode` was called for the
 *   case, and `violation` says what broke
 */
module.exports = async (fuzzCase) => {
  const input = Buffer.from(fuzzCase.buffer)
  const counter = { decodes: 0 }
  let outcome
  let milliseconds

  for (let attempt = 0; attempt < (fuzzCase.prompt ? PROMPT_ATTEMPTS : 1); attempt++) {
    const startedAt = performance.now()
    outcome = await run(fuzzCase, input, counter)
    milliseconds = performance.now() - startedAt
    if (milliseconds <= PROMPT_MS) break
  }

  const { decoded, error } = outcome
  const category = error ? `typed:${error.constructor.name}` : 'decoded'
  const violation = (() => {
    if (error && !TYPED_ERRORS.some((type) => error instanceof type)) {
      return `${error.constructor.name}: ${error.message}`
    }
    if (fuzzCase.prompt && milliseconds > PROMPT_MS) return `took ${milliseconds.toFixed(1)} ms`
    if (fuzzCase.expect && fuzzCase.expect.error) {
      return error && error.constructor.name === fuzzCase.expect.error
        ? null
        : `expected ${fuzzCase.expect.error}, got ${category}`
    }
    if (decoded) return brokenBy(decoded, input, fuzzCase)
    return fuzzCase.expect ? `expected a decoded value, got ${category}` : null
  })()

  return { ran: counter.decodes > 0 ? 1 : 0, category, violation }
}
