/**
 * From v12 the fetch response's error codes are data, not exceptions: a partition-level code no
 * longer discards the whole node's response, and the top-level code (decoded but never read
 * before v12) is classified by the consumer. `parse` therefore returns the body untouched and the
 * consumer decides, which is what makes a per-partition contract expressible at all.
 * @param {object} data
 * @returns {Promise<object>} the same object
 */
const parse = async (data) => data

module.exports = { parse }
