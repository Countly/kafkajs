const TransactionOffsets = require('../transactionOffsets')
const OffsetOperations = require('../../../consumer/offsetManager/offsetOperations')
const ResetObligations = require('../../../consumer/offsetManager/resetObligations')

const topics = (offset, partition = 0, topic = 'topic') => [
  { topic, partitions: [{ partition, offset, leaderEpoch: 7, metadata: 'saved' }] },
]
const build = () => {
  const logger = { error: jest.fn() }
  const offsets = new TransactionOffsets(logger)
  const ledger = new OffsetOperations()
  const cluster = { markOffsetAsCommitted: jest.fn() }
  const call = (value, group = 'group') => offsets.snapshot(ledger, group, value)
  return { offsets, ledger, cluster, logger, call }
}

describe('Transactional offset staging', () => {
  test('wire fields are immutable copies and the fence precedes an applied mutation', () => {
    const { offsets, ledger, cluster, call } = build()
    const input = topics('9')
    const snapshot = call(input)
    input[0].topic = 'changed'
    input[0].partitions[0].offset = '99'
    input[0].partitions[0].metadata = 'changed'
    input[0].partitions.push({ partition: 1, offset: '55' })
    expect(snapshot.topics).toEqual(topics('9'))
    expect(Object.isFrozen(snapshot.topics[0].partitions[0])).toBe(true)
    ledger.touchTopic('topic')
    offsets.stage(snapshot)
    offsets.publish(cluster)
    expect(cluster.markOffsetAsCommitted).not.toHaveBeenCalled()
    expect(ledger.size()).toBe(0)
  })

  test.each([
    ['9', '2'],
    ['2', '9'],
  ])('the last call wins for %s then %s', (first, last) => {
    const { offsets, ledger, cluster, call } = build()
    offsets.stage(call(topics(first)))
    offsets.stage(call(topics(last)))
    expect(offsets.snapshots.size).toBe(1)
    offsets.publish(cluster)
    expect(cluster.markOffsetAsCommitted).toHaveBeenCalledTimes(1)
    expect(cluster.markOffsetAsCommitted).toHaveBeenCalledWith({
      groupId: 'group',
      topic: 'topic',
      partition: 0,
      offset: last,
    })
    offsets.publish(cluster)
    expect(cluster.markOffsetAsCommitted).toHaveBeenCalledTimes(1)
    expect(ledger.size()).toBe(0)
  })

  test('overlap retains only unreplaced cells, across groups and topics', () => {
    const { offsets, ledger, cluster, call } = build()
    const a = call([
      {
        topic: 'topic',
        partitions: [
          { partition: 0, offset: '8' },
          { partition: 1, offset: '7' },
        ],
      },
    ])
    offsets.stage(a)
    offsets.stage(call(topics('2')))
    offsets.stage(call(topics('3', 0, 'other')))
    offsets.stage(call(topics('4'), 'other-group'))
    expect(offsets.snapshots.size).toBe(4)
    offsets.publish(cluster)
    expect(cluster.markOffsetAsCommitted.mock.calls.map(([value]) => value)).toEqual([
      { groupId: 'group', topic: 'topic', partition: 0, offset: '2' },
      { groupId: 'group', topic: 'topic', partition: 1, offset: '7' },
      { groupId: 'group', topic: 'other', partition: 0, offset: '3' },
      { groupId: 'other-group', topic: 'topic', partition: 0, offset: '4' },
    ])
    expect(ledger.size()).toBe(0)
  })

  test.each([[[]], [[{ topic: 'topic', partitions: [] }]]])(
    'empty topics acquire no fence',
    (input) => {
      const { offsets, ledger, cluster, call } = build()
      const begin = jest.spyOn(ledger, 'begin')
      offsets.stage(call(input))
      offsets.release()
      expect(begin).not.toHaveBeenCalled()
      expect(ledger.size()).toBe(0)
      offsets.publish(cluster)
      expect(cluster.markOffsetAsCommitted).not.toHaveBeenCalled()
    }
  )

  test('a newer applied mutation suppresses only its own cell', () => {
    const { offsets, ledger, cluster, call } = build()
    offsets.stage(
      call([
        {
          topic: 'topic',
          partitions: [
            { partition: 0, offset: '8' },
            { partition: 1, offset: '7' },
          ],
        },
      ])
    )
    const mutation = ledger.begin([{ topic: 'topic', partition: 0 }], { mutation: true })
    ledger.apply(mutation, 'topic', 0)
    ledger.end(mutation)
    offsets.publish(cluster)
    expect(cluster.markOffsetAsCommitted.mock.calls).toEqual([
      [{ groupId: 'group', topic: 'topic', partition: 1, offset: '7' }],
    ])
    expect(ledger.size()).toBe(0)
  })

  test('successful publication settles every attached consumer obligation', () => {
    const { offsets, ledger, cluster, call } = build()
    const tables = [new ResetObligations(), new ResetObligations()]
    for (const table of tables) {
      table.owe('topic', 'incarnation')
      table.attachTo(ledger)
    }
    offsets.stage(call(topics('2')))
    offsets.publish(cluster)
    for (const table of tables) {
      expect(table.isOwed('topic', 0)).toBe(false)
      table.clear()
    }
    expect(ledger.size()).toBe(0)
  })

  test('release ends staged and outstanding fences and permanently forfeits late acceptance', () => {
    const { offsets, ledger, cluster, call } = build()
    offsets.stage(call(topics('2')))
    const waiting = call(topics('9', 1))
    offsets.release()
    offsets.release()
    offsets.stage(waiting)
    offsets.publish(cluster)
    expect(cluster.markOffsetAsCommitted).not.toHaveBeenCalled()
    expect(offsets.snapshots.size).toBe(0)
    expect(ledger.size()).toBe(0)
  })

  test('a failed call ends only its own fence', () => {
    const { offsets, ledger, cluster, call } = build()
    offsets.stage(call(topics('2')))
    const failed = call(topics('9', 1))
    offsets.abandon(failed)
    offsets.abandon(failed)
    offsets.publish(cluster)
    expect(cluster.markOffsetAsCommitted.mock.calls).toEqual([
      [{ groupId: 'group', topic: 'topic', partition: 0, offset: '2' }],
    ])
    expect(ledger.size()).toBe(0)
  })

  test('a retried call owns the chain until it settles, and rejection frees the next slot', async () => {
    const { offsets, call } = build()
    let release
    const held = new Promise((resolve) => {
      release = resolve
    })
    const order = []
    const first = offsets.enqueue(call(topics('2')), async () => {
      order.push('a')
      await held
      order.push('retry')
      throw new Error('call failed')
    })
    const rejection = first.catch((error) => error)
    const second = offsets.enqueue(call(topics('9')), async () => {
      order.push('b')
    })
    await Promise.resolve()
    expect(order).toEqual(['a'])
    release()
    expect(await rejection).toHaveProperty('message', 'call failed')
    await second
    expect(order).toEqual(['a', 'retry', 'b'])
    offsets.release()
  })

  test.each([false, true])(
    'publication failure is contained, including throwing logger=%s',
    (throws) => {
      const { offsets, ledger, cluster, logger, call } = build()
      offsets.stage(call(topics('2')))
      offsets.stage(call(topics('7', 1)))
      cluster.markOffsetAsCommitted.mockImplementationOnce(() => {
        throw new Error('cache failure')
      })
      if (throws)
        logger.error.mockImplementation(() => {
          throw new Error('observer failure')
        })
      expect(() => offsets.publish(cluster)).not.toThrow()
      expect(logger.error).toHaveBeenCalledWith(
        'Committed transaction offset cache publication failed',
        { error: 'cache failure' }
      )
      expect(cluster.markOffsetAsCommitted).toHaveBeenCalledTimes(2)
      expect(ledger.size()).toBe(0)
    }
  )
})
