const { MemberMetadata } = require('../assignerProtocol')
const Decoder = require('../../protocol/decoder')

const TOPICS = ['topic-a', 'topic-b']
const USER_DATA = Buffer.from([1, 2, 3, 4])

/**
 * The kafkajs 2.2.4 decoder, inlined byte-for-byte from `assignerProtocol.js:21-27` of that
 * release. A 2.2.4 group leader runs exactly this against whatever a 3.x member sends, so a v3
 * subscription that this cannot read splits a mixed group.
 * @param {Buffer} buffer
 * @returns {{ version: number, topics: string[], userData: Buffer }}
 */
const decodeAs224 = (buffer) => {
  const decoder = new Decoder(buffer)
  return {
    version: decoder.readInt16(),
    topics: decoder.readArray((d) => d.readString()),
    userData: decoder.readBytes(),
  }
}

describe('Consumer > subscription codec', () => {
  describe('encode', () => {
    it('writes the v0 layout when no optional value is given', () => {
      const bytes = MemberMetadata.encode({ version: 0, topics: TOPICS })

      // INT16 version, INT32 array length, two INT16-prefixed strings, INT32 0 userData.
      expect(bytes.length).toEqual(2 + 4 + (2 + 7) + (2 + 7) + 4)
      expect(decodeAs224(bytes)).toEqual({ version: 0, topics: TOPICS, userData: Buffer.alloc(0) })
    })

    it('treats an explicit undefined userData exactly like an absent one', () => {
      expect(MemberMetadata.encode({ version: 0, topics: TOPICS, userData: undefined })).toEqual(
        MemberMetadata.encode({ version: 0, topics: TOPICS })
      )
    })

    it('writes INT32 -1 for a strict null userData', () => {
      // Encoded at version 0, so userData is the LAST field and its four bytes are the tail. At
      // version 3 the tail is RackId's INT16, and the assertion would be reading the wrong field.
      const withNull = MemberMetadata.encode({ version: 0, topics: TOPICS, userData: null })
      const withEmpty = MemberMetadata.encode({ version: 0, topics: TOPICS })

      expect(withNull).not.toEqual(withEmpty)
      expect(withNull.subarray(withNull.length - 4)).toEqual(Buffer.from([255, 255, 255, 255]))
      expect(withEmpty.subarray(withEmpty.length - 4)).toEqual(Buffer.from([0, 0, 0, 0]))
      // And the rule is version-independent: at v3 the same null still round-trips as null.
      expect(
        MemberMetadata.decode(MemberMetadata.encode({ version: 3, topics: TOPICS, userData: null }))
          .userData
      ).toBeNull()
    })

    it('writes the v1 layout for a version-1 caller, defaulting OwnedPartitions to empty', () => {
      // The layout follows the VERSION, not which values happen to be defined: a positional
      // format cannot skip a group and still be readable.
      const v1 = MemberMetadata.encode({ version: 1, topics: TOPICS })
      const v0 = MemberMetadata.encode({ version: 0, topics: TOPICS })

      expect(v1.length).toEqual(v0.length + 4)
      expect(v1.subarray(v1.length - 4)).toEqual(Buffer.from([0, 0, 0, 0]))
      // The first three fields are still where a 2.2.4 reader expects them.
      expect(decodeAs224(v1)).toEqual({ version: 1, topics: TOPICS, userData: Buffer.alloc(0) })
    })

    it('fills every lower group when a higher one is asked for', () => {
      // A "hole" - rackId without ownedPartitions - would be undecodable. v3 always writes all
      // three, so this round-trips.
      const bytes = MemberMetadata.encode({ version: 3, topics: ['topic-a'], rackId: 'rack-1' })

      expect(MemberMetadata.decode(bytes)).toEqual({
        version: 3,
        topics: ['topic-a'],
        userData: Buffer.alloc(0),
        ownedPartitions: [],
        generationId: -1,
        rackId: 'rack-1',
      })
    })

    it('writes each optional group when its value is defined', () => {
      const bytes = MemberMetadata.encode({
        version: 3,
        topics: ['topic-a'],
        userData: USER_DATA,
        ownedPartitions: [{ topic: 'topic-a', partitions: [0, 2] }],
        generationId: 11,
        rackId: 'rack-1',
      })

      expect(MemberMetadata.decode(bytes)).toEqual({
        version: 3,
        topics: ['topic-a'],
        userData: USER_DATA,
        ownedPartitions: [{ topic: 'topic-a', partitions: [0, 2] }],
        generationId: 11,
        rackId: 'rack-1',
      })
    })

    it('coerces an empty rackId to the wire null', () => {
      const bytes = MemberMetadata.encode({ version: 3, topics: ['topic-a'], rackId: '' })

      expect(MemberMetadata.decode(bytes).rackId).toBeNull()
    })
  })

  describe('decode', () => {
    it('returns the v0 shape for a v0 buffer', () => {
      const bytes = MemberMetadata.encode({ version: 0, topics: TOPICS })

      expect(MemberMetadata.decode(bytes)).toEqual({
        version: 0,
        topics: TOPICS,
        userData: Buffer.alloc(0),
      })
    })

    it('does not over-read a v1-declared buffer written by kafkajs 2.2.4', () => {
      // The shipped fixture IS such a buffer: version 1 on the wire, three fields only. Decoding
      // must stop where the bytes stop, or every 2.2.4 member in a mixed group becomes unreadable.
      const shipped = Buffer.from(require('./fixtures/roundRobinAssigner/memberMetadata.json'))

      expect(() => MemberMetadata.decode(shipped)).not.toThrow()
      expect(MemberMetadata.decode(shipped)).toEqual({
        version: 1,
        topics: ['topic-test'],
        userData: Buffer.alloc(0),
      })
    })

    it('reads exactly the groups the declared version carries', () => {
      const v1 = MemberMetadata.encode({
        version: 1,
        topics: ['topic-a'],
        ownedPartitions: [{ topic: 'topic-a', partitions: [1] }],
      })
      const v2 = MemberMetadata.encode({ version: 2, topics: ['topic-a'], generationId: 5 })

      expect(MemberMetadata.decode(v1)).toEqual({
        version: 1,
        topics: ['topic-a'],
        userData: Buffer.alloc(0),
        ownedPartitions: [{ topic: 'topic-a', partitions: [1] }],
      })
      expect(MemberMetadata.decode(v2)).toEqual({
        version: 2,
        topics: ['topic-a'],
        userData: Buffer.alloc(0),
        ownedPartitions: [],
        generationId: 5,
      })
    })

    it('ignores a trailing group a future version would add', () => {
      // Decoding stops at the last group the declared version defines, so a longer tail is left
      // unread at every version - the codec never reads bytes it has no schema for.
      const bytes = Buffer.concat([
        MemberMetadata.encode({ version: 3, topics: ['topic-a'] }),
        Buffer.from([0, 0, 0, 0]),
      ])

      expect(() => MemberMetadata.decode(bytes)).not.toThrow()
      expect(MemberMetadata.decode(bytes)).toEqual({
        version: 3,
        topics: ['topic-a'],
        userData: Buffer.alloc(0),
        ownedPartitions: [],
        generationId: -1,
        rackId: null,
      })
    })
  })

  describe('mixed-version compatibility', () => {
    it('lets a kafkajs 2.2.4 leader read a v3 subscription without throwing', () => {
      const bytes = MemberMetadata.encode({
        version: 3,
        topics: TOPICS,
        userData: USER_DATA,
        ownedPartitions: [{ topic: 'topic-a', partitions: [0] }],
        generationId: 4,
        rackId: 'rack-1',
      })

      expect(() => decodeAs224(bytes)).not.toThrow()
      expect(decodeAs224(bytes)).toEqual({ version: 3, topics: TOPICS, userData: USER_DATA })
    })
  })
})
