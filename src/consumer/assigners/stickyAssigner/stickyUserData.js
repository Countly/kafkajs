const Encoder = require('../../../protocol/encoder')
const Decoder = require('../../../protocol/decoder')
const { DEFAULT_GENERATION } = require('./stickyCore')

/**
 * The sticky assignor's own `userData` payload, carried inside the subscription's `UserData`
 * field. V1 is `ARRAY<{ STRING topic, ARRAY<INT32> partitions }>` followed by `INT32 generation`;
 * V0 is that array alone. There is no version prefix - the reader tells them apart by whether
 * four bytes are left after the array.
 *
 * This writes V1 and reads V1, then V0, then falls back to `(empty, -1)`.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/StickyAssignor.java
 */
const stickyUserData = {
  /**
   * @param {{ assignment: Array<{topic: string, partitions: number[]}>, generationId: number }} data
   * @returns {Buffer} always the V1 layout
   */
  encode({ assignment, generationId }) {
    return new Encoder()
      .writeArray(
        assignment.map(({ topic, partitions }) =>
          new Encoder().writeString(topic).writeArray(partitions)
        )
      )
      .writeInt32(generationId).buffer
  },

  /**
   * @param {Buffer|null} buffer
   * @returns {{ assignment: Array<{topic: string, partitions: number[]}>, generationId: number }}
   *   `{ assignment: [], generationId: -1 }` for a null, empty or unreadable buffer
   */
  decode(buffer) {
    if (buffer == null || buffer.length === 0) {
      return { assignment: [], generationId: DEFAULT_GENERATION }
    }

    try {
      const decoder = new Decoder(buffer)
      const assignment = decoder.readArray((d) => ({
        topic: d.readString(),
        partitions: d.readArray((p) => p.readInt32()),
      }))

      return {
        assignment,
        generationId: decoder.canReadInt32() ? decoder.readInt32() : DEFAULT_GENERATION,
      }
    } catch (e) {
      return { assignment: [], generationId: DEFAULT_GENERATION }
    }
  },
}

module.exports = { stickyUserData }
