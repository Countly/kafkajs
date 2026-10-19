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
