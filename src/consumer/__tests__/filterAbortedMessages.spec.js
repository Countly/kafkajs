const abortedBatch = require('./fixtures/batches/aborted')
const committedBatch = require('./fixtures/batches/committed')
const nontransactionalBatch = require('./fixtures/batches/nontransactional')
const filterAbortedMessages = require('../filterAbortedMessages')

describe('filterAbortedMessages', () => {
  test('filters out all aborted messages', () => {
    const { messages, abortedTransactions } = abortedBatch
    const { messages: nontransactionalMessages } = nontransactionalBatch

    expect(messages).toHaveLength(3)
    expect(abortedTransactions).toHaveLength(1)

    expect(filterAbortedMessages({ messages, abortedTransactions })).toStrictEqual([
      expect.objectContaining({
        key: Buffer.from([0, 0, 0, 0]), // Abort control message
      }),
    ])

    expect(
      filterAbortedMessages({
        messages: [...messages, ...nontransactionalMessages],
        abortedTransactions,
      })
    ).toStrictEqual([
      expect.objectContaining({
        key: Buffer.from([0, 0, 0, 0]),
      }),
      ...nontransactionalMessages,
    ])
  })

  test('filters out aborted messages with malformed keys', () => {
    const { messages, abortedTransactions } = abortedBatch
    const { messages: nontransactionalMessages } = nontransactionalBatch
    messages[messages.length - 2].key = null
    expect(
      filterAbortedMessages({
        messages: [...messages, ...nontransactionalMessages],
        abortedTransactions,
      })
    ).toStrictEqual([
      expect.objectContaining({
        key: Buffer.from([0, 0, 0, 0]),
      }),
      ...nontransactionalMessages,
    ])
  })

  test('returns all committed messages', () => {
    const { messages, abortedTransactions } = committedBatch
    expect(messages).toHaveLength(4)
    expect(abortedTransactions).toHaveLength(0)

    expect(filterAbortedMessages({ messages, abortedTransactions })).toStrictEqual(messages)
  })

  test('returns all nontransactional messages', () => {
    const { messages, abortedTransactions } = nontransactionalBatch
    expect(messages).toHaveLength(3)
    expect(abortedTransactions).toHaveLength(0)

    expect(filterAbortedMessages({ messages, abortedTransactions })).toStrictEqual(messages)
  })
})

describe('filterAbortedMessages > what ends an aborted transaction', () => {
  const ABORT_KEY = Buffer.from([0, 0, 0, 0])
  const COMMIT_KEY = Buffer.from([0, 0, 0, 1])

  /** A record as a decoded batch holds it. */
  const record = ({
    offset,
    key = Buffer.from('key'),
    isControlRecord = false,
    inTransaction = true,
    producerId = '7',
  }) => ({
    offset: String(offset),
    key,
    isControlRecord,
    batchContext: { producerId, inTransaction },
  })

  const abortedFrom = (firstOffset, producerId = '7') => [
    { producerId, firstOffset: String(firstOffset) },
  ]
  const offsetsOf = (messages) => messages.map(({ offset }) => offset)

  test('a data record keyed with four zero bytes does not end the transaction: every record of it is hidden', () => {
    const messages = [
      record({ offset: 10 }),
      record({ offset: 11, key: ABORT_KEY }),
      record({ offset: 12 }),
    ]

    expect(
      offsetsOf(filterAbortedMessages({ messages, abortedTransactions: abortedFrom(10) }))
    ).toEqual([])
  })

  test('a genuine abort marker ends it: later committed and non-transactional data survive', () => {
    const messages = [
      record({ offset: 10 }),
      record({ offset: 11, key: ABORT_KEY }),
      record({ offset: 12 }),
      record({ offset: 13, key: ABORT_KEY, isControlRecord: true }),
      record({ offset: 14 }),
      record({ offset: 15, inTransaction: false }),
    ]

    expect(
      offsetsOf(filterAbortedMessages({ messages, abortedTransactions: abortedFrom(10) }))
    ).toEqual(['13', '14', '15'])
  })

  test('a commit marker changes nothing: the aborted transaction stays hidden past it', () => {
    const messages = [
      record({ offset: 10 }),
      record({ offset: 11, key: COMMIT_KEY, isControlRecord: true }),
      record({ offset: 12 }),
    ]

    expect(
      offsetsOf(filterAbortedMessages({ messages, abortedTransactions: abortedFrom(10) }))
    ).toEqual([])
  })

  test("another producer's abort marker does not end this producer's transaction", () => {
    const messages = [
      record({ offset: 10 }),
      record({ offset: 11, key: ABORT_KEY, isControlRecord: true, producerId: '8' }),
      record({ offset: 12 }),
    ]

    expect(
      offsetsOf(filterAbortedMessages({ messages, abortedTransactions: abortedFrom(10) }))
    ).toEqual(['11'])
  })

  test('a data record keyed with four zero bytes outside an aborted transaction is delivered', () => {
    const messages = [
      record({ offset: 10, key: ABORT_KEY }),
      record({ offset: 11, key: ABORT_KEY, inTransaction: false }),
    ]

    expect(offsetsOf(filterAbortedMessages({ messages, abortedTransactions: [] }))).toEqual([
      '10',
      '11',
    ])
    expect(
      offsetsOf(filterAbortedMessages({ messages, abortedTransactions: abortedFrom(50) }))
    ).toEqual(['10', '11'])
  })

  test('a control record without a key ends nothing', () => {
    const messages = [
      record({ offset: 10 }),
      record({ offset: 11, key: null, isControlRecord: true }),
      record({ offset: 12 }),
    ]

    expect(
      offsetsOf(filterAbortedMessages({ messages, abortedTransactions: abortedFrom(10) }))
    ).toEqual([])
  })
})

describe('filterAbortedMessages > several aborted transactions', () => {
  const ABORT_KEY = Buffer.from([0, 0, 0, 0])
  const COMMIT_KEY = Buffer.from([0, 0, 0, 1])

  /** A record as a decoded batch holds it. */
  const record = ({
    offset,
    producerId,
    key = Buffer.from('key'),
    isControlRecord = false,
    inTransaction = true,
  }) => ({
    offset: String(offset),
    key,
    isControlRecord,
    batchContext: { producerId, inTransaction },
  })

  const abortMarker = (offset, producerId) =>
    record({ offset, producerId, key: ABORT_KEY, isControlRecord: true })
  const aborted = (producerId, firstOffset) => ({
    producerId,
    firstOffset: String(firstOffset),
  })
  const offsetsOf = (messages) => messages.map(({ offset }) => offset)

  test('two transactions that began before the first record are both hidden', () => {
    const messages = [
      record({ offset: 5, producerId: '2' }),
      record({ offset: 6, producerId: '2' }),
      record({ offset: 7, producerId: '1' }),
      abortMarker(8, '2'),
      abortMarker(9, '1'),
      record({ offset: 10, producerId: '2', inTransaction: false }),
    ]
    const abortedTransactions = [aborted('1', 0), aborted('2', 1)]

    expect(offsetsOf(filterAbortedMessages({ messages, abortedTransactions }))).toEqual([
      '8',
      '9',
      '10',
    ])
  })

  test('three overlapping transactions that began before the first record are all hidden', () => {
    const messages = [
      record({ offset: 6, producerId: '3' }),
      record({ offset: 7, producerId: '2' }),
      record({ offset: 8, producerId: '1' }),
      abortMarker(9, '1'),
      abortMarker(10, '2'),
      abortMarker(11, '3'),
      record({ offset: 12, producerId: '3', inTransaction: false }),
      record({ offset: 13, producerId: '1' }),
    ]
    const abortedTransactions = [aborted('1', 0), aborted('2', 2), aborted('3', 4)]

    expect(offsetsOf(filterAbortedMessages({ messages, abortedTransactions }))).toEqual([
      '9',
      '10',
      '11',
      '12',
      '13',
    ])
  })

  test('a list not ordered by first offset still hides every aborted transaction from its start', () => {
    const messages = [
      record({ offset: 10, producerId: '1' }),
      record({ offset: 11, producerId: '2' }),
      abortMarker(12, '2'),
      abortMarker(13, '1'),
      record({ offset: 14, producerId: '1', inTransaction: false }),
    ]
    const abortedTransactions = [aborted('2', 11), aborted('1', 10)]

    expect(offsetsOf(filterAbortedMessages({ messages, abortedTransactions }))).toEqual([
      '12',
      '13',
      '14',
    ])
  })

  test('a committed transaction interleaved with an aborted one is delivered', () => {
    const messages = [
      record({ offset: 10, producerId: '1' }),
      record({ offset: 11, producerId: '2' }),
      record({ offset: 12, producerId: '1' }),
      record({ offset: 13, producerId: '2', key: COMMIT_KEY, isControlRecord: true }),
      abortMarker(14, '1'),
      record({ offset: 15, producerId: '1' }),
    ]
    const abortedTransactions = [aborted('1', 10)]

    expect(offsetsOf(filterAbortedMessages({ messages, abortedTransactions }))).toEqual([
      '11',
      '13',
      '14',
      '15',
    ])
  })

  test('two aborted transactions of one producer are each hidden from their own start', () => {
    const messages = [
      record({ offset: 5, producerId: '1' }),
      abortMarker(6, '1'),
      record({ offset: 7, producerId: '1' }),
      record({ offset: 20, producerId: '1' }),
      abortMarker(21, '1'),
      record({ offset: 22, producerId: '1', inTransaction: false }),
    ]
    const abortedTransactions = [aborted('1', 0), aborted('1', 20)]

    expect(offsetsOf(filterAbortedMessages({ messages, abortedTransactions }))).toEqual([
      '6',
      '7',
      '21',
      '22',
    ])
  })

  test('a transaction that begins after a record does not hide that record', () => {
    const messages = [
      record({ offset: 10, producerId: '1' }),
      record({ offset: 11, producerId: '2' }),
      record({ offset: 12, producerId: '1' }),
      abortMarker(13, '1'),
    ]
    const abortedTransactions = [aborted('1', 12)]

    expect(offsetsOf(filterAbortedMessages({ messages, abortedTransactions }))).toEqual([
      '10',
      '11',
      '13',
    ])
  })

  test('a record of a legacy message set, which has no batch, is delivered beside an aborted transaction', () => {
    const legacy = { offset: '9', key: Buffer.from('key'), isControlRecord: false }
    const messages = [legacy, record({ offset: 10, producerId: '2' }), abortMarker(11, '2')]
    const abortedTransactions = [aborted('2', 10)]

    expect(offsetsOf(filterAbortedMessages({ messages, abortedTransactions }))).toEqual(['9', '11'])
  })

  test('orders first offsets beyond the safe integer range exactly, and hides a record at the first offset', () => {
    const messages = [
      record({ offset: '9007199254740992', producerId: '1' }),
      record({ offset: '9007199254740993', producerId: '2' }),
      record({ offset: '9007199254740994', producerId: '3' }),
      abortMarker('9007199254740995', '1'),
      abortMarker('9007199254740996', '2'),
    ]
    const abortedTransactions = [aborted('2', '9007199254740993'), aborted('1', '9007199254740992')]

    expect(offsetsOf(filterAbortedMessages({ messages, abortedTransactions }))).toEqual([
      '9007199254740994',
      '9007199254740995',
      '9007199254740996',
    ])
  })

  // As Numbers, 9007199254740995 and 9007199254740996 are equal, so a lossy activation would start
  // the second producer's aborted transaction at its earlier commit marker.
  test('activates a transaction beyond the safe integer range exactly: an earlier commit marker of its producer survives', () => {
    const messages = [
      record({ offset: '9007199254740994', producerId: '1' }),
      record({
        offset: '9007199254740995',
        producerId: '2',
        key: COMMIT_KEY,
        isControlRecord: true,
      }),
      record({ offset: '9007199254740996', producerId: '2' }),
      abortMarker('9007199254740997', '1'),
      abortMarker('9007199254740998', '2'),
    ]
    const abortedTransactions = [aborted('2', '9007199254740996'), aborted('1', '9007199254740994')]

    expect(offsetsOf(filterAbortedMessages({ messages, abortedTransactions }))).toEqual([
      '9007199254740995',
      '9007199254740997',
      '9007199254740998',
    ])
  })
})
