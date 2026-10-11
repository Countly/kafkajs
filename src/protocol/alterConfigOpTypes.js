/**
 * The operation one config entry carries in an IncrementalAlterConfigs request.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/admin/AlterConfigOp.java
 * @typedef {number} AlterConfigOpTypes
 *
 * @readonly
 * @enum {AlterConfigOpTypes}
 */
module.exports = Object.freeze({
  /** Set the entry to the given value. */
  SET: 0,
  /** Revert the entry to its default; the value is ignored. */
  DELETE: 1,
  /** List entries only: add the given values to the current ones. */
  APPEND: 2,
  /** List entries only: remove the given values; removing every value leaves an empty list. */
  SUBTRACT: 3,
})
