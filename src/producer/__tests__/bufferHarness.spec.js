const buildRecordBatch = require('../../protocol/requests/produce/buildRecordBatch')
const { Types: Compression } = require('../../protocol/message/compression')
const Long = require('../../utils/long')
const { owned, readBatch, sentRecords, takeSent } = require('./fixtures/bufferHarness')

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

  test('takeSent gives every batch’s place, first sequence and record count at once, without decoding', async () => {
    const sent = takeSent({
      topicData: [
        {
          topic: 'topic-a',
          partitions: [
            { partition: 3, firstSequence: 4, messages: [{ value: 'a' }, { value: 'b' }] },
            { partition: 1, messages: [{ value: 'c' }] },
          ],
        },
      ],
    })

    expect(sent.batches).toEqual([
      { topic: 'topic-a', partition: 3, firstSequence: 4, count: 2 },
      { topic: 'topic-a', partition: 1, firstSequence: undefined, count: 1 },
    ])
    const reading = sent.read()
    expect(sent.read()).toBe(reading)
    const read = await reading
    expect(read.map(({ records }) => records.map(({ value }) => String(value)))).toEqual([
      ['a', 'b'],
      ['c'],
    ])
  })

  test('takeSent observes a record field as it is at each call: a later replacement shows only in a later observation', async () => {
    const record = { key: 'k1', value: 'v1', headers: { h: 'x' }, timestamp: new Date(1000) }
    const request = { batches: [{ topic: 'topic-a', partition: 0, messages: [record] }] }

    const before = takeSent(request)
    record.key = 'k2'
    record.value = 'v2'
    record.headers = { h: 'y' }
    record.timestamp.setTime(2000)
    const after = takeSent(request)

    const [[{ records: early }], [{ records: late }]] = await Promise.all([
      before.read(),
      after.read(),
    ])
    expect(early).toEqual([
      {
        key: Buffer.from('k1'),
        value: Buffer.from('v1'),
        headers: { h: Buffer.from('x') },
        timestamp: '1000',
        offset: '0',
      },
    ])
    expect(late).toEqual([
      {
        key: Buffer.from('k2'),
        value: Buffer.from('v2'),
        headers: { h: Buffer.from('y') },
        timestamp: '2000',
        offset: '0',
      },
    ])
  })

  test('takeSent observes Buffer bytes as they are at each call: a later byte change shows only in a later observation', async () => {
    const value = Buffer.from('aaaa')
    const listed = Buffer.from('l1')
    const record = {
      value,
      headers: { many: [Buffer.from('l0'), listed] },
      timestamp: Long.fromValue(5n),
    }
    const request = { batches: [{ topic: 'topic-a', partition: 0, messages: [record] }] }

    const before = takeSent(request)
    value[0] = 0x62
    listed[1] = 0x32
    record.timestamp.value = 6n
    const after = takeSent(request)

    const [[{ records: early }], [{ records: late }]] = await Promise.all([
      before.read(),
      after.read(),
    ])
    expect(early).toEqual([
      {
        key: null,
        value: Buffer.from('aaaa'),
        headers: { many: [Buffer.from('l0'), Buffer.from('l1')] },
        timestamp: '5',
        offset: '0',
      },
    ])
    expect(late).toEqual([
      {
        key: null,
        value: Buffer.from('baaa'),
        headers: { many: [Buffer.from('l0'), Buffer.from('l2')] },
        timestamp: '6',
        offset: '0',
      },
    ])
  })

  test('takeSent fixes every batch’s record list at the call: later changes to either list are not read', async () => {
    const head = [{ value: Buffer.from('first'), timestamp: 1 }]
    const tail = [{ value: 'second', timestamp: 2 }]
    const sent = takeSent({
      topicData: [
        {
          topic: 'topic-a',
          partitions: [
            { partition: 1, messages: head },
            { partition: 0, messages: tail },
          ],
        },
      ],
    })
    head.push({ value: 'later', timestamp: 3 })
    tail.length = 0
    tail.push({ value: 'replaced', timestamp: 4 })

    const read = await sent.read()

    expect(read.map(({ topic, partition }) => [topic, partition])).toEqual([
      ['topic-a', 1],
      ['topic-a', 0],
    ])
    expect(read.map(({ records }) => records.map(({ value }) => String(value)))).toEqual([
      ['first'],
      ['second'],
    ])
  })

  test('takeSent at the call, its record snapshot included, calls none of Array every, flatMap, shift, map or forEach, Promise.all, Map set, Date.now, performance.now, Math.random, setTimeout or Buffer.from', () => {
    const records = [
      {
        key: Buffer.from('k'),
        value: Buffer.from('a'),
        headers: { one: Buffer.from('x'), many: ['1', Buffer.from('2')] },
        timestamp: new Date(1000),
      },
      { value: 'b', headers: { h: 'y' }, timestamp: Long.fromValue(1001n) },
      { value: null, timestamp: '1002' },
    ]
    const spies = [
      jest.spyOn(Array.prototype, 'every'),
      jest.spyOn(Array.prototype, 'flatMap'),
      jest.spyOn(Array.prototype, 'shift'),
      jest.spyOn(Array.prototype, 'map'),
      jest.spyOn(Array.prototype, 'forEach'),
      jest.spyOn(Promise, 'all'),
      jest.spyOn(Map.prototype, 'set'),
      jest.spyOn(Date, 'now'),
      jest.spyOn(performance, 'now'),
      jest.spyOn(Math, 'random'),
      jest.spyOn(global, 'setTimeout'),
      jest.spyOn(Buffer, 'from'),
    ]
    const calls = []
    try {
      for (let at = 0; at < spies.length; at++) calls[at] = -spies[at].mock.calls.length
      takeSent({
        topicData: [
          {
            topic: 'topic-a',
            partitions: [
              { partition: 0, messages: records },
              { partition: 1, messages: [{ value: 'c' }] },
            ],
          },
        ],
      })
      takeSent({ batches: [{ topic: 'topic-a', partition: 0, messages: records }] })
      for (let at = 0; at < spies.length; at++) calls[at] += spies[at].mock.calls.length
    } finally {
      for (let at = 0; at < spies.length; at++) spies[at].mockRestore()
    }

    expect(calls).toEqual(new Array(12).fill(0))
  })

  describe('takeSent writes what the batch writer writes for the original record', () => {
    beforeEach(() => {
      jest.spyOn(Date, 'now').mockReturnValue(1800000000000)
    })

    afterEach(() => {
      jest.restoreAllMocks()
    })

    test.each([
      ['an absent timestamp', { value: 'v' }],
      ['an undefined timestamp', { value: 'v', timestamp: undefined }],
      ['a null timestamp', { value: 'v', timestamp: null }],
      ['the number 0', { value: 'v', timestamp: 0 }],
      ['the string 0', { value: 'v', timestamp: '0' }],
      ['the Date of the epoch', { value: 'v', timestamp: new Date(0) }],
      ['a Long of 0', { value: 'v', timestamp: Long.fromNumber(0) }],
      ['the BigInt 0', { value: 'v', timestamp: 0n }],
      ['a number', { value: 'v', timestamp: 1700000000001 }],
      ['a numeric string', { value: 'v', timestamp: '1700000000002' }],
      ['a Date', { value: 'v', timestamp: new Date(1700000000003) }],
      ['a Long', { value: 'v', timestamp: Long.fromValue(1700000000004n) }],
      ['a Long of the int64 maximum', { value: 'v', timestamp: Long.fromValue(2n ** 63n - 1n) }],
      ['an empty string key', { key: '', value: 'v', timestamp: 1 }],
      ['an empty Buffer value', { value: Buffer.alloc(0), timestamp: 1 }],
      ['a null value', { key: 'k', value: null, timestamp: 1 }],
      ['a header with an empty value', { value: 'v', headers: { h: '', b: Buffer.alloc(0) } }],
    ])('%s', async (_, record) => {
      const { buffer } = await buildRecordBatch({
        compression: Compression.None,
        messages: [record],
      })
      const expected = (await readBatch(buffer)).records

      const [{ records }] = await takeSent({
        batches: [{ topic: 'topic-a', partition: 0, messages: [record] }],
      }).read()

      expect(records).toEqual(expected)
    })
  })

  test('readBatch refuses a batch with one payload byte changed under its CRC', async () => {
    const { buffer } = await buildRecordBatch({
      compression: Compression.None,
      messages: [{ value: 'payload', timestamp: 1 }],
    })
    const intact = Buffer.from(buffer)
    const changed = Buffer.from(buffer)
    changed[changed.indexOf(Buffer.from('payload'))] ^= 0xff

    expect((await readBatch(intact)).records.map(({ value }) => String(value))).toEqual(['payload'])
    await expect(readBatch(changed)).rejects.toMatchObject({
      name: 'KafkaJSCorruptRecordError',
      reason: 'checksum',
    })
  })
})
