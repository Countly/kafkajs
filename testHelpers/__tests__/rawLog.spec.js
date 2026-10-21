const ISOLATION_LEVEL = require('../../src/protocol/isolationLevel')
const { readLog, endOffsets, committedRecords } = require('../rawLog')

/**
 * A decoded record as `Broker#fetch` returns it.
 * @param {number} offset
 * @param {object} [batch] the batch fields that differ from a plain one-record batch
 * @param {Buffer|null} [key]
 */
const record = (offset, batch = {}, key = null) => ({
  offset: String(offset),
  key,
  value: Buffer.from(`v${offset}`),
  isControlRecord: Boolean(batch.isControlBatch),
  batchContext: {
    firstOffset: String(offset),
    lastOffsetDelta: 0,
    producerId: '-1',
    inTransaction: false,
    isControlBatch: false,
    ...batch,
  },
})

/** A partition whose fetches are answered in turn from `answers`, then with nothing. */
const servedFrom = (answers, aborted = []) => ({
  fetches: [],
  fetch(position) {
    this.fetches.push(position)
    return { messages: answers.shift() || [], abortedTransactions: aborted }
  },
})

/**
 * Fake brokers for `readLog`'s `open`: a seed with metadata, and leaders that answer ListOffsets
 * and Fetch from `logs`.
 */
const cluster = ({ logs, ends, leaders = { 0: 1 }, metadata } = {}) => {
  const opened = []
  const open = async ({ host, port }) => {
    const broker = {
      address: `${host}:${port}`,
      disconnect: jest.fn(async () => {}),
      metadata: async () =>
        metadata || {
          brokers: [1, 2].map((nodeId) => ({ nodeId, host: `node${nodeId}`, port: 9092 })),
          topicMetadata: [
            {
              topic: 't',
              topicErrorCode: 0,
              partitionMetadata: Object.entries(leaders)
                .map(([partitionId, leader]) => ({ partitionId: Number(partitionId), leader }))
                .reverse(),
            },
          ],
        },
      listOffsets: async ({
        isolationLevel,
        topics: [
          {
            partitions: [{ partition }],
          },
        ],
      }) => ({
        responses: [{ partitions: [{ offset: ends(partition, isolationLevel) }] }],
      }),
      fetch: async ({
        topics: [
          {
            partitions: [{ partition, fetchOffset }],
          },
        ],
      }) => ({
        responses: [{ partitions: [{ partition, ...logs[partition].fetch(fetchOffset) }] }],
      }),
    }
    opened.push(broker)
    return broker
  }
  return { open, opened }
}

describe('rawLog', () => {
  test('reads each partition from offset 0 up to the end its leader reported, in order', async () => {
    const logs = {
      0: servedFrom([
        [record(0), record(1)],
        [record(2), record(3)],
        [record(4), record(5)],
      ]),
      1: servedFrom([[record(0)]]),
    }
    const { open, opened } = cluster({
      logs,
      leaders: { 0: 1, 1: 2 },
      ends: (partition) => (partition === 0 ? '5' : '1'),
    })

    const log = await readLog({ brokers: ['seed:9092'], topic: 't', open, maxIdleFetches: 3 })

    expect([...log.keys()]).toEqual([0, 1])
    expect(log.get(0).records.map(({ offset }) => offset)).toEqual(['0', '1', '2', '3', '4'])
    expect(log.get(0).endOffset).toBe('5')
    expect(log.get(1).records.map(({ offset }) => offset)).toEqual(['0'])
    expect(logs[0].fetches).toEqual(['0', '2', '4'])
    expect(opened.map(({ address }) => address)).toEqual(['seed:9092', 'node1:9092', 'node2:9092'])
    expect(opened.every(({ disconnect }) => disconnect.mock.calls.length === 1)).toBe(true)
  })

  test('drops what a fetch returns from before its position, so each record is read once', async () => {
    const logs = { 0: servedFrom([[record(0)], [record(0), record(1), record(2)]]) }
    const { open } = cluster({ logs, ends: () => '3' })

    const log = await readLog({ brokers: ['seed:9092'], topic: 't', open })

    expect(log.get(0).records.map(({ offset }) => offset)).toEqual(['0', '1', '2'])
    expect(logs[0].fetches).toEqual(['0', '1'])
  })

  test('asks for the end under the isolation level it reads with', async () => {
    const asked = []
    const { open } = cluster({
      logs: {
        0: servedFrom([
          [record(0), record(1)],
          [record(0), record(1)],
        ]),
      },
      ends: (partition, isolationLevel) => {
        asked.push(isolationLevel)
        return isolationLevel === ISOLATION_LEVEL.READ_COMMITTED ? '1' : '2'
      },
    })

    const committed = await readLog({
      brokers: ['seed:9092'],
      topic: 't',
      open,
      isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
    })
    const ends = await endOffsets({ brokers: ['seed:9092'], topic: 't', open })

    expect(committed.get(0).records.map(({ offset }) => offset)).toEqual(['0'])
    expect([...ends]).toEqual([[0, '2']])
    expect(asked).toEqual([ISOLATION_LEVEL.READ_COMMITTED, ISOLATION_LEVEL.READ_UNCOMMITTED])
  })

  test('reads two partitions of one leader over one connection, and takes no aborted list as none', async () => {
    const logs = { 0: servedFrom([[record(0)]], null), 1: servedFrom([[record(0)]], null) }
    const { open, opened } = cluster({ logs, leaders: { 0: 1, 1: 1 }, ends: () => '1' })

    const log = await readLog({ brokers: ['seed:9092'], topic: 't', open })

    expect(opened.map(({ address }) => address)).toEqual(['seed:9092', 'node1:9092'])
    expect([...log.values()].map(({ abortedTransactions }) => abortedTransactions)).toEqual([
      [],
      [],
    ])
  })

  test('keeps every aborted transaction a fetch named, once', async () => {
    const aborted = [{ producerId: '7', firstOffset: '1' }]
    const { open } = cluster({
      logs: { 0: servedFrom([[record(0)], [record(1)], [record(2)]], aborted) },
      ends: () => '3',
    })

    const log = await readLog({ brokers: ['seed:9092'], topic: 't', open })

    expect(log.get(0).abortedTransactions).toEqual(aborted)
  })

  test('fails a read that stops making progress, and still disconnects', async () => {
    const logs = { 0: servedFrom([]) }
    const { open, opened } = cluster({ logs, ends: () => '3' })

    await expect(
      readLog({ brokers: ['seed:9092'], topic: 't', open, maxIdleFetches: 4, maxWaitTime: 1 })
    ).rejects.toThrow('Raw read of t-0 stalled at 0, below 3')
    expect(logs[0].fetches).toHaveLength(4)
    expect(opened.every(({ disconnect }) => disconnect.mock.calls.length === 1)).toBe(true)
  })

  test('fails when the topic has no metadata or a leader is unknown', async () => {
    const missing = cluster({
      logs: {},
      ends: () => '0',
      metadata: { brokers: [], topicMetadata: [{ topic: 't', topicErrorCode: 3 }] },
    })
    const leaderless = cluster({ logs: {}, ends: () => '0', leaders: { 0: 9 } })

    await expect(
      readLog({ brokers: ['seed:9092'], topic: 't', open: missing.open })
    ).rejects.toThrow('No metadata for t: 3')
    await expect(
      readLog({ brokers: ['seed:9092'], topic: 't', open: leaderless.open })
    ).rejects.toThrow('t-0 has no known leader (9)')
    expect(missing.opened[0].disconnect).toHaveBeenCalledTimes(1)
  })

  test('still disconnects the brokers it opened when opening another fails', async () => {
    const { open: openSeed, opened } = cluster({ logs: {}, ends: () => '0' })
    let calls = 0
    const open = async (address) => {
      if (calls++ === 0) return openSeed(address)
      throw new Error('unreachable')
    }

    await expect(readLog({ brokers: ['seed:9092'], topic: 't', open })).rejects.toThrow(
      'unreachable'
    )
    expect(opened[0].disconnect).toHaveBeenCalledTimes(1)
  })

  describe('committedRecords', () => {
    const txn = (offset, producerId = '7') =>
      record(offset, { producerId, inTransaction: true }, offset === 3 ? Buffer.alloc(4) : null)
    const marker = (offset, type, producerId = '7') =>
      record(
        offset,
        { producerId, inTransaction: true, isControlBatch: true },
        Buffer.from([0, 0, 0, type])
      )

    test('hides an aborted transaction, data keyed with four zero bytes included, until its ABORT marker', () => {
      const records = [
        record(0),
        txn(1),
        txn(2),
        txn(3),
        txn(4),
        marker(5, 0),
        txn(6),
        marker(7, 1),
        record(8),
      ]

      const visible = committedRecords({
        records,
        abortedTransactions: [{ producerId: '7', firstOffset: '1' }],
      })

      expect(visible.map(({ offset }) => offset)).toEqual(['0', '6', '8'])
    })

    test('a COMMIT marker does not end an aborted transaction, and another producer is untouched', () => {
      const records = [txn(1), marker(2, 1), txn(3), txn(4, '8'), marker(5, 0)]

      const visible = committedRecords({
        records,
        abortedTransactions: [{ producerId: '7', firstOffset: '1' }],
      })

      expect(visible.map(({ offset }) => offset)).toEqual(['4'])
    })

    test('aborted transactions are taken in offset order, whatever order the fetches listed them', () => {
      const records = [txn(1, '8'), marker(2, 0, '8'), txn(5), marker(6, 0), record(7)]

      const visible = committedRecords({
        records,
        abortedTransactions: [
          { producerId: '7', firstOffset: '5' },
          { producerId: '8', firstOffset: '1' },
        ],
      })

      expect(visible.map(({ offset }) => offset)).toEqual(['7'])
    })

    test('an aborted transaction counts only from its first offset, and control records are never given', () => {
      const records = [txn(1), marker(2, 1), txn(3), marker(4, 0)]

      const visible = committedRecords({
        records,
        abortedTransactions: [{ producerId: '7', firstOffset: '3' }],
      })

      expect(visible.map(({ offset }) => offset)).toEqual(['1'])
    })
  })
})
