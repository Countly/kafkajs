const Encoder = require('../../protocol/encoder')
const { MemberAssignment } = require('../assignerProtocol')
const { readAssignmentRows, readSyncAssignment } = require('../assignmentRows')

const TOPIC = 'topic-a'

/**
 * Assignment bytes written row by row, so a row can repeat a topic, name none, or carry the null
 * partition list.
 * @param {{ topic: string|null, partitions: number[]|null }[]} rows
 * @returns {Buffer}
 */
const rowsOf = (rows) =>
  new Encoder()
    .writeInt16(1)
    .writeArray(
      rows.map(({ topic, partitions }) =>
        partitions === null
          ? new Encoder().writeString(topic).writeInt32(-1)
          : new Encoder().writeString(topic).writeArray(partitions)
      )
    )
    .writeBytes(Buffer.alloc(0)).buffer

/**
 * @param {Buffer|null} bytes
 * @param {string} [subject]
 * @returns {Error|undefined} what `readSyncAssignment` threw
 */
const refusalOf = (bytes, subject) => {
  try {
    readSyncAssignment(bytes, subject)
  } catch (error) {
    return error
  }
  return undefined
}

describe('Consumer > assignmentRows', () => {
  describe('readAssignmentRows', () => {
    test('returns every row as written, repeated topics and null lists included', () => {
      const bytes = new Encoder()
        .writeInt16(3)
        .writeArray([
          new Encoder().writeString(TOPIC).writeArray([1]),
          new Encoder().writeString(TOPIC).writeInt32(-1),
          new Encoder().writeString(null).writeArray([0]),
        ])
        .writeBytes(Buffer.from('user')).buffer

      expect(readAssignmentRows(bytes)).toEqual({
        version: 3,
        topics: [
          { topic: TOPIC, partitions: [1] },
          { topic: TOPIC, partitions: null },
          { topic: null, partitions: [0] },
        ],
        userData: Buffer.from('user'),
      })
    })

    test('returns null topics for the null topic list, and null for a buffer shorter than its version', () => {
      expect(readAssignmentRows(Buffer.from('0001ffffffff00000000', 'hex'))).toEqual({
        version: 1,
        topics: null,
        userData: Buffer.alloc(0),
      })
      expect(readAssignmentRows(Buffer.from([0]))).toBe(null)
    })
  })

  describe('readSyncAssignment', () => {
    test.each([
      ['null bytes', null, 'SyncGroup response carries no assignment'],
      [
        'a one-byte assignment',
        Buffer.from([0]),
        'SyncGroup response carries an assignment shorter than its version',
      ],
      [
        'a null topic list',
        Buffer.from('0001ffffffff00000000', 'hex'),
        'SyncGroup response carries an assignment with a null topic list',
      ],
      [
        'a row with no topic',
        rowsOf([{ topic: null, partitions: [0] }]),
        'SyncGroup response carries an assignment row with no topic',
      ],
      [
        'a row with an empty topic',
        rowsOf([{ topic: '', partitions: [0] }]),
        'SyncGroup response carries an assignment row with no topic',
      ],
      [
        'partition -1',
        rowsOf([{ topic: TOPIC, partitions: [-1] }]),
        `SyncGroup response assigns partition -1 of topic ${TOPIC}`,
      ],
      [
        'a null partition list',
        rowsOf([{ topic: TOPIC, partitions: null }]),
        `SyncGroup response carries a null partition list for topic ${TOPIC}`,
      ],
    ])('refuses %s as a retriable MALFORMED_GROUP_ANSWER', (_, bytes, message) => {
      expect(refusalOf(bytes)).toMatchObject({
        type: 'MALFORMED_GROUP_ANSWER',
        retriable: true,
        message,
      })
    })

    test('refuses an assignment that ends inside its topic list, naming the decoder error', () => {
      const refusal = refusalOf(Buffer.from('000100000001', 'hex'))

      expect(refusal).toMatchObject({ type: 'MALFORMED_GROUP_ANSWER', retriable: true })
      expect(refusal.message).toMatch(/^SyncGroup response carries a truncated assignment: \S/)
    })

    test('names the subject it is given in the refusal', () => {
      expect(refusalOf(null, 'The assigner output for member-1')).toMatchObject({
        message: 'The assigner output for member-1 carries no assignment',
      })
    })

    test('reads zero bytes as nothing assigned', () => {
      expect(readSyncAssignment(Buffer.alloc(0))).toEqual({
        assignment: {},
        userData: null,
        repeatedTopics: [],
        repeated: [],
      })
    })

    test('keeps a row with an empty partition list as no partitions of that topic', () => {
      expect(
        readSyncAssignment(
          rowsOf([
            { topic: TOPIC, partitions: [] },
            { topic: 'topic-b', partitions: [1] },
          ])
        )
      ).toEqual({
        assignment: { [TOPIC]: [], 'topic-b': [1] },
        userData: Buffer.alloc(0),
        repeatedTopics: [],
        repeated: [],
      })
    })

    test('uses each partition once, in its first place, and returns the repeated topics and partitions', () => {
      expect(
        readSyncAssignment(
          rowsOf([
            { topic: TOPIC, partitions: [1, 0] },
            { topic: TOPIC, partitions: [0, 2] },
            { topic: 'topic-b', partitions: [3, 3] },
          ])
        )
      ).toEqual({
        assignment: { [TOPIC]: [1, 0, 2], 'topic-b': [3] },
        userData: Buffer.alloc(0),
        repeatedTopics: [TOPIC],
        repeated: [
          { topic: TOPIC, partition: 0 },
          { topic: 'topic-b', partition: 3 },
        ],
      })
    })

    test('reads an assignment of a later version with bytes after its user data from the fields this member reads', () => {
      const userData = Buffer.from('user-data')
      const bytes = Buffer.concat([
        MemberAssignment.encode({ version: 9, assignment: { [TOPIC]: [1] }, userData }),
        Buffer.from('0102', 'hex'),
      ])

      expect(readSyncAssignment(bytes)).toEqual({
        assignment: { [TOPIC]: [1] },
        userData,
        repeatedTopics: [],
        repeated: [],
      })
    })
  })
})
