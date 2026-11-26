const Encoder = require('../../../protocol/encoder')

/**
 * Writes a member assignment the way a leader's assigner may: one row per entry, so a topic or a
 * partition can be named more than once, which `MemberAssignment.encode` cannot do for a topic.
 * @param {Array<{ topic: string, partitions: number[] }>} rows
 * @param {Buffer} [userData=Buffer.alloc(0)]
 * @returns {Buffer} the assignment bytes, version 0
 */
const encodeRows = (rows, userData = Buffer.alloc(0)) =>
  new Encoder()
    .writeInt16(0)
    .writeArray(
      rows.map(({ topic, partitions }) => new Encoder().writeString(topic).writeArray(partitions))
    )
    .writeBytes(userData).buffer

module.exports = encodeRows
