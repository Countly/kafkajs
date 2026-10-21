const {
  KafkaJSAggregateError,
  KafkaJSCorruptRecordError,
  KafkaJSDeleteTopicRecordsError,
  KafkaJSInvalidLongError,
  KafkaJSInvalidVarIntError,
  KafkaJSInvariantViolation,
  KafkaJSNotImplemented,
  KafkaJSPartialMessageError,
  KafkaJSProtocolError,
} = require('../../../errors')
const { Codecs } = require('../../message/compression')

/**
 * The typed errors a decoder may fail a malformed body with, by class: the varint errors carry the
 * `name` of their base class, so a name cannot tell them apart. Anything else, a `RangeError` and a
 * `TypeError` first, is a defect. The aggregate error is what a `parse` throws for a body that
 * reports a failure for several items, and the invariant violation what a decoder throws for a
 * body that breaks a rule of its own response (a group response with no group).
 */
const TYPED_ERRORS = [
  KafkaJSPartialMessageError,
  KafkaJSInvalidVarIntError,
  KafkaJSInvalidLongError,
  KafkaJSCorruptRecordError,
  KafkaJSProtocolError,
  KafkaJSNotImplemented,
  KafkaJSAggregateError,
  KafkaJSDeleteTopicRecordsError,
  KafkaJSInvariantViolation,
]

/** The longest a decoder may take to refuse an impossible count. */
const PROMPT_MS = 50

/** How often a decoder that was slow once may try again before it is called slow. */
const PROMPT_ATTEMPTS = 3

const PROTOTYPE_NAMES = Object.getOwnPropertyNames(Object.prototype).join()

/**
 * Compressed batches are outside the corpus: a body mutated into one reaches a codec that answers
 * as an unconfigured one does, with a typed error.
 * @returns {void}
 */
const disableCompression = () => {
  for (const type of Object.keys(Codecs)) {
    Codecs[type] = () => {
      throw new KafkaJSNotImplemented('compression is outside the corpus')
    }
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

module.exports = {
  TYPED_ERRORS,
  PROMPT_MS,
  PROMPT_ATTEMPTS,
  PROTOTYPE_NAMES,
  disableCompression,
  elementsIn,
}
