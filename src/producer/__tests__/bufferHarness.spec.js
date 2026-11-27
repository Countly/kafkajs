const { owned, sentRecords } = require('./fixtures/bufferHarness')

describe('Producer > fixtures > bufferHarness', () => {
  test('owned gives the records a batch holds, the same values in the same order', () => {
    const records = [{ key: 'k', value: 'a' }, { value: Buffer.from('b') }]

    expect(owned(records)).toEqual([{ key: 'k', value: 'a' }, { value: Buffer.from('b') }])
  })

  test('sentRecords reads every batch of a Produce request as a broker reads it', async () => {
    const request = {
      topicData: [
        {
          topic: 'topic-a',
          partitions: [
            {
              partition: 2,
              firstSequence: 7,
              messages: [
                { key: 'k', value: 'a', timestamp: 1000, headers: { h: 'x', many: ['1', '2'] } },
                { value: Buffer.from('b'), timestamp: '1001' },
                { key: Buffer.from('n'), value: null, timestamp: 999 },
              ],
            },
          ],
        },
        {
          topic: 'topic-b',
          partitions: [{ partition: 0, messages: [{ value: 'c', timestamp: 5 }] }],
        },
      ],
    }

    expect(await sentRecords(request)).toEqual([
      {
        topic: 'topic-a',
        partition: 2,
        firstSequence: 7,
        records: [
          {
            key: Buffer.from('k'),
            value: Buffer.from('a'),
            headers: { h: Buffer.from('x'), many: [Buffer.from('1'), Buffer.from('2')] },
            timestamp: '1000',
            offset: '0',
          },
          { key: null, value: Buffer.from('b'), headers: {}, timestamp: '1001', offset: '1' },
          { key: Buffer.from('n'), value: null, headers: {}, timestamp: '999', offset: '2' },
        ],
      },
      {
        topic: 'topic-b',
        partition: 0,
        firstSequence: 0,
        records: [{ key: null, value: Buffer.from('c'), headers: {}, timestamp: '5', offset: '0' }],
      },
    ])
  })

  test('sentRecords reads the batches of an accumulator hand-off in their order', async () => {
    const request = {
      batches: [
        {
          topic: 'topic-a',
          partition: 1,
          firstSequence: 3,
          messages: [{ value: 'x', timestamp: 1 }],
        },
        { topic: 'topic-a', partition: 0, messages: [{ value: 'y', timestamp: 2 }] },
      ],
    }

    const batches = await sentRecords(request)

    expect(
      batches.map(({ topic, partition, firstSequence }) => [topic, partition, firstSequence])
    ).toEqual([
      ['topic-a', 1, 3],
      ['topic-a', 0, 0],
    ])
    expect(batches.map(({ records }) => records.map(({ value }) => String(value)))).toEqual([
      ['x'],
      ['y'],
    ])
  })

  test('sentRecords takes every batch record list before its first await', async () => {
    const messages = [{ value: 'kept', timestamp: 1 }]
    const read = sentRecords({ batches: [{ topic: 'topic-a', partition: 0, messages }] })
    messages.length = 0
    messages.push({ value: 'later', timestamp: 2 })

    const [{ records }] = await read

    expect(records.map(({ value }) => String(value))).toEqual(['kept'])
  })
})
