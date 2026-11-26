const { MemberAssignment } = require('../assignerProtocol')
const { decodeApplied } = require('../assignmentRows')
const encodeRows = require('./fixtures/encodeRows')

const CONTEXT = { groupId: 'g', memberId: 'm-1', generationId: 7, groupProtocol: 'MyAssigner' }

describe('Consumer > assignmentRows decodeApplied', () => {
  let logger

  beforeEach(() => {
    logger = { warn: jest.fn() }
  })

  test('keeps the first occurrence of a partition, in received order, for each topic', () => {
    const bytes = encodeRows(
      [
        { topic: 'a', partitions: [2, 0, 2] },
        { topic: 'b', partitions: [1, 1, 0] },
      ],
      Buffer.from('user')
    )

    expect(decodeApplied(bytes, logger, CONTEXT)).toEqual({
      version: 0,
      assignment: { a: [2, 0], b: [1, 0] },
      userData: Buffer.from('user'),
    })
  })

  test('gives a topic named in two rows the partitions of both rows, in row order', () => {
    const bytes = encodeRows([
      { topic: 'a', partitions: [0] },
      { topic: 'b', partitions: [5] },
      { topic: 'a', partitions: [3, 0] },
    ])

    expect(decodeApplied(bytes, logger, CONTEXT).assignment).toEqual({ a: [0, 3], b: [5] })
  })

  test('warns once, with the context, the repeated partitions and the repeated topics', () => {
    const bytes = encodeRows([
      { topic: 'a', partitions: [2, 0, 2] },
      { topic: 'b', partitions: [1, 1, 0] },
      { topic: 'c', partitions: [4] },
      { topic: 'c', partitions: [5] },
    ])

    decodeApplied(bytes, logger, CONTEXT)

    expect(logger.warn).toHaveBeenCalledTimes(1)
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('more than once'), {
      ...CONTEXT,
      repeatedPartitions: [
        { topic: 'a', partitions: [2] },
        { topic: 'b', partitions: [1] },
      ],
      repeatedTopics: ['c'],
    })
  })

  test('returns what the wire codec decodes, and logs nothing, when no topic or partition repeats', () => {
    const bytes = MemberAssignment.encode({
      version: 1,
      assignment: { a: [2, 0, 1], b: [] },
      userData: Buffer.from('user'),
    })

    expect(decodeApplied(bytes, logger, CONTEXT)).toEqual(MemberAssignment.decode(bytes))
    expect(logger.warn).not.toHaveBeenCalled()
  })

  test('returns null, and logs nothing, for a buffer without a version header', () => {
    expect(decodeApplied(Buffer.alloc(0), logger, CONTEXT)).toBeNull()
    expect(logger.warn).not.toHaveBeenCalled()
  })
})
