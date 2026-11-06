const requestV1 = require('../v1/request')
const Encoder = require('../../../encoder')

/**
 * OffsetFetch Request (Version: 2) => group_id [topics]
 *   group_id => STRING
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition
 *       partition => INT32
 */

/** Builds the first nullable-topic request while preserving existing explicit-selection bytes.
 * @param {object} options request selection
 * @param {string} options.groupId group identity
 * @param {object[]|null} options.topics null requests all topics; arrays retain v1 encoding
 * @returns {object} v2 protocol request, with no retry work
 */
module.exports = ({ groupId, topics }) => {
  const request = requestV1({ groupId, topics })
  return {
    ...request,
    apiVersion: 2,
    encode:
      topics == null
        ? async () => new Encoder().writeString(groupId).writeInt32(-1)
        : request.encode,
  }
}
