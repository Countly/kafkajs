const crypto = require('crypto')

/** 16 bytes, which is what `Uuid` encodes. */
const MEMBER_ID_BYTES = 16

/**
 * The two ids the Java client refuses to hand out: the all-zero uuid and the `...0001` uuid.
 * Computed from the bytes rather than typed as literals, so the encoding and the constants can
 * never disagree. Module-private: nothing outside this file has a use for it, and the spec derives
 * the same two strings itself rather than importing them.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/Uuid.java
 */
const RESERVED_MEMBER_IDS = Object.freeze(
  [
    Buffer.alloc(MEMBER_ID_BYTES),
    (() => {
      const one = Buffer.alloc(MEMBER_ID_BYTES)
      one[MEMBER_ID_BYTES - 1] = 1
      return one
    })(),
  ].map((bytes) => bytes.toString('base64url'))
)

/**
 * A member id for the KIP-848 protocol: 16 random bytes, base64url without padding, 22 characters.
 *
 * Generated once per membership and never regenerated - a fencing, a rejoin or an epoch reset all
 * keep it, which is what lets the coordinator recognise a member across generations. The loop is
 * Java's: a value equal to one of the reserved uuids, or one whose textual form starts with `-`,
 * is thrown away and re-drawn. Version-4 bits are deliberately not set: the format is the client's
 * choice and the broker only rejects a null or empty id.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-1082%3A+Require+Client-Generated+IDs+for+ConsumerGroupHeartbeatRPC
 * @param {{ randomBytes?: (size: number) => Buffer }} [options] the byte source; defaults to the
 *   platform CSPRNG. Injectable so the reserved-value loop - unreachable with a real CSPRNG - has
 *   an assertion at all.
 * @returns {string} a 22-character base64url id
 */
const generateMemberId = ({ randomBytes = crypto.randomBytes } = {}) => {
  for (;;) {
    const id = randomBytes(MEMBER_ID_BYTES).toString('base64url')

    if (!RESERVED_MEMBER_IDS.includes(id) && !id.startsWith('-')) {
      return id
    }
  }
}

module.exports = { generateMemberId }
