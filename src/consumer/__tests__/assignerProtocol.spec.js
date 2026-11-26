const { MemberMetadata, MemberAssignment } = require('../assignerProtocol')
const encodeRows = require('./fixtures/encodeRows')

const FIXTURE_ROUND_ROBIN_METADATA = Buffer.from(
  require('./fixtures/roundRobinAssigner/memberMetadata.json')
)
const FIXTURE_ROUND_ROBIN_ASSIGNER = Buffer.from(
  require('./fixtures/roundRobinAssigner/memberAssignment.json')
)

describe('Consumer > assignerProtocol', () => {
  describe('MemberMetadata', () => {
    test('encode', () => {
      const buffer = MemberMetadata.encode({
        version: 1,
        topics: ['topic-test'],
      })

      // v1 now writes the OwnedPartitions group the version declares; the fixture is the v0
      // prefix, which is byte-identical, plus that group's empty INT32.
      expect(buffer).toEqual(
        Buffer.concat([FIXTURE_ROUND_ROBIN_METADATA, Buffer.from([0, 0, 0, 0])])
      )
    })

    test('decode', () => {
      expect(MemberMetadata.decode(FIXTURE_ROUND_ROBIN_METADATA)).toEqual({
        version: 1,
        topics: ['topic-test'],
        userData: Buffer.alloc(0),
      })
    })
  })

  describe('MemberAssignment', () => {
    test('encode', () => {
      const buffer = MemberAssignment.encode({
        version: 1,
        assignment: { 'topic-test': [2, 5, 4, 1, 3, 0] },
      })

      expect(buffer).toEqual(FIXTURE_ROUND_ROBIN_ASSIGNER)
    })

    test('decode', () => {
      expect(MemberAssignment.decode(FIXTURE_ROUND_ROBIN_ASSIGNER)).toEqual({
        version: 1,
        assignment: { 'topic-test': [2, 5, 4, 1, 3, 0] },
        userData: Buffer.alloc(0),
      })
    })

    test('decode empty assignment', () => {
      expect(MemberAssignment.decode(Buffer.from([]))).toBe(null)
    })

    test('decode gives a topic named in two rows the partitions of both rows, repeats kept', () => {
      const buffer = encodeRows(
        [
          { topic: 'a', partitions: [0, 3] },
          { topic: 'b', partitions: [5] },
          { topic: 'a', partitions: [3, 1] },
        ],
        Buffer.from('user')
      )

      const decoded = MemberAssignment.decode(buffer)

      expect(decoded).toEqual({
        version: 0,
        assignment: { a: [0, 3, 3, 1], b: [5] },
        userData: Buffer.from('user'),
      })
      expect(Object.keys(decoded.assignment)).toEqual(['a', 'b'])
    })

    test('decode keeps a partition named twice in one row, as encode writes it', () => {
      const buffer = MemberAssignment.encode({ version: 1, assignment: { a: [1, 1, 0] } })

      expect(MemberAssignment.decode(buffer).assignment).toEqual({ a: [1, 1, 0] })
    })
  })
})
