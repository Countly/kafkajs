const Encoder = require('../protocol/encoder')
const Decoder = require('../protocol/decoder')

/**
 * `ConsumerProtocolSubscription`, versions 0-3. `validVersions 0-3`, `flexibleVersions none`.
 *
 * The format is POSITIONAL - no field tags - so the `version` decides which groups are written,
 * and a value the caller omits takes its wire default. Skipping a group whose value happened to
 * be undefined would produce a buffer with a hole that no positional reader, here or in the Java
 * client, can decode.
 *
 * Decoding is version-guided AND bytes-limited: a group is read only when the declared version
 * carries it and bytes remain, so a `version: 1` buffer written by kafkajs 2.2.4 with three
 * fields still decodes to those three fields.
 *
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/ConsumerProtocolSubscription.json
 */
const MemberMetadata = {
  /**
   * @param {object} metadata
   * @param {number} metadata.version 0-3; decides which groups are written
   * @param {string[]} metadata.topics
   * @param {Buffer|null} [metadata.userData] absent or `undefined` writes an empty byte string;
   *   a strict `null` writes the null sentinel, which is what Java's sticky assignor sends
   *   before it has ever been assigned anything
   * @param {Array<{topic: string, partitions: number[]}>} [metadata.ownedPartitions] v1+;
   *   defaults to `[]`
   * @param {number} [metadata.generationId] v2+; defaults to `-1`
   * @param {string|null} [metadata.rackId] v3+; defaults to `null`, and an empty string - the
   *   shipped default of the `rackId` consumer option - is coerced to that null
   * @returns {Buffer}
   */
  encode({ version, topics, userData, ownedPartitions, generationId, rackId }) {
    // `userData === null` must survive as null (INT32 -1); anything else absent is the empty
    // byte string the pre-3.0 destructuring default produced.
    const bytes = userData === undefined ? Buffer.alloc(0) : userData
    const encoder = new Encoder().writeInt16(version).writeArray(topics).writeBytes(bytes)

    if (version >= 1) {
      encoder.writeArray(
        (ownedPartitions || []).map(({ topic, partitions }) =>
          new Encoder().writeString(topic).writeArray(partitions)
        )
      )
    }

    if (version >= 2) {
      encoder.writeInt32(generationId === undefined ? -1 : generationId)
    }

    if (version >= 3) {
      encoder.writeString(rackId === undefined || rackId === '' ? null : rackId)
    }

    return encoder.buffer
  },

  /**
   * @param {Buffer} buffer
   * @returns {{ version: number, topics: string[], userData: Buffer,
   *   ownedPartitions?: Array<{topic: string, partitions: number[]}>,
   *   generationId?: number, rackId?: string|null }} the three v0 fields always, plus each
   *   optional group the buffer actually carries
   */
  decode(buffer) {
    const decoder = new Decoder(buffer)
    const version = decoder.readInt16()
    const metadata = {
      version,
      topics: decoder.readArray((d) => d.readString()),
      userData: decoder.readBytes(),
    }

    // Version-guided AND bytes-limited. The version guard is the format's own rule; the canRead
    // guard is what keeps a v1 buffer written by kafkajs 2.2.4 - which declared a version whose
    // fields it did not write - readable instead of a partial-message throw.
    if (version < 1 || !decoder.canReadInt32()) {
      return metadata
    }

    metadata.ownedPartitions = decoder.readArray((d) => ({
      topic: d.readString(),
      partitions: d.readArray((p) => p.readInt32()),
    }))

    if (version < 2 || !decoder.canReadInt32()) {
      return metadata
    }

    metadata.generationId = decoder.readInt32()

    if (version < 3 || !decoder.canReadInt16()) {
      return metadata
    }

    metadata.rackId = decoder.readString()

    return metadata
  },
}

const MemberAssignment = {
  /**
   * @param {object} options
   * @param {number} options.version
   * @param {Object<String,Array>} options.assignment, example:
   *                               {
   *                                 'topic-A': [0, 2, 4, 6],
   *                                 'topic-B': [0, 2],
   *                               }
   * @param {Buffer} [options.userData=Buffer.alloc(0)]
   *
   * @returns Buffer
   */
  encode({ version, assignment, userData = Buffer.alloc(0) }) {
    return new Encoder()
      .writeInt16(version)
      .writeArray(
        Object.keys(assignment).map((topic) =>
          new Encoder().writeString(topic).writeArray(assignment[topic])
        )
      )
      .writeBytes(userData).buffer
  },

  /**
   * @param {Buffer} buffer
   * @returns {Object|null}
   */
  decode(buffer) {
    const decoder = new Decoder(buffer)
    const decodePartitions = (d) => d.readInt32()
    const decodeAssignment = (d) => ({
      topic: d.readString(),
      partitions: d.readArray(decodePartitions),
    })
    const indexAssignment = (obj, { topic, partitions }) =>
      Object.assign(obj, { [topic]: partitions })

    if (!decoder.canReadInt16()) {
      return null
    }

    return {
      version: decoder.readInt16(),
      assignment: decoder.readArray(decodeAssignment).reduce(indexAssignment, {}),
      userData: decoder.readBytes(),
    }
  },
}

module.exports = {
  MemberMetadata,
  MemberAssignment,
}
