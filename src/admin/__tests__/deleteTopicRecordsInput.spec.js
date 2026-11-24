const createAdmin = require('../index')

const TOPIC = 'records-topic'
const RETRY = { retries: 0, initialRetryTime: 1, maxRetryTime: 1 }

/**
 * An admin over a cluster of one broker leading partitions 0 and 1 of `TOPIC`, with low watermarks
 * from `lows` and a high watermark of 100. Every cluster and broker call is recorded.
 * @param {{ lows?: string[] }} [options]
 * @returns {{ admin: object, cluster: object, broker: object, sent: object[][], logger: object }}
 */
const adminOver = ({ lows = ['0', '0'] } = {}) => {
  const logger = { namespace: () => logger, debug() {}, info() {}, warn: jest.fn(), error() {} }
  const sent = []
  const broker = {
    deleteRecords: jest.fn(async ({ topics }) => {
      sent.push(JSON.parse(JSON.stringify(topics[0].partitions)))
    }),
  }
  const cluster = {
    addTargetTopic: jest.fn(async () => {}),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    refreshMetadata: jest.fn(async () => {}),
    findTopicPartitionMetadata: jest.fn(() => [{ partitionId: 0 }, { partitionId: 1 }]),
    fetchTopicsOffset: jest.fn(async ([{ fromBeginning }]) => [
      {
        topic: TOPIC,
        partitions: [0, 1].map((partition) => ({
          partition,
          offset: fromBeginning ? lows[partition] : '100',
        })),
      },
    ]),
    findLeaderForPartitions: jest.fn((topic, partitions) => ({ 0: partitions })),
    findBroker: jest.fn(async () => broker),
  }
  const admin = createAdmin({ logger, retry: RETRY, cluster })
  return { admin, cluster, broker, sent, logger }
}

/** @returns {unknown[]} a copy of `items` with a hole at `index` */
const holeAt = (items, index) => {
  const copy = [...items]
  delete copy[index]
  return copy
}

/** @returns {number} how many times any method of the fake cluster was called */
const clusterCalls = (cluster) =>
  Object.values(cluster).reduce((count, method) => count + method.mock.calls.length, 0)

describe('Admin > deleteTopicRecords > input', () => {
  test.each([
    ['2^64 - 1, which wraps to -1 on the wire', '18446744073709551615'],
    ['2^63, above the int64 range', '9223372036854775808'],
    ['-2', '-2'],
    ['-5', '-5'],
    ['a word', 'abc'],
    ['a fraction', '1.5'],
    ['a fractional number', 1.5],
    ['NaN', NaN],
    ['null', null],
    ['undefined', undefined],
    ['an empty string', ''],
    ['an object that cannot be turned into text', Object.create(null)],
  ])('refuses the offset %s before any request', async (_, offset) => {
    const { admin, cluster } = adminOver()

    await expect(
      admin.deleteTopicRecords({
        topic: TOPIC,
        partitions: [
          { partition: 0, offset: '5' },
          { partition: 1, offset },
        ],
      })
    ).rejects.toMatchObject({ name: 'KafkaJSNonRetriableError' })
    expect(clusterCalls(cluster)).toBe(0)
  })

  test.each([
    ['NaN', NaN],
    ['a fraction', 0.5],
    ['a negative number', -1],
    ['a numeric string', '0'],
    ['2^31', 2 ** 31],
    ['null', null],
  ])('refuses the partition %s before any request', async (_, partition) => {
    const { admin, cluster } = adminOver()

    await expect(
      admin.deleteTopicRecords({ topic: TOPIC, partitions: [{ partition, offset: '5' }] })
    ).rejects.toMatchObject({ name: 'KafkaJSNonRetriableError' })
    expect(clusterCalls(cluster)).toBe(0)
  })

  test.each([
    ['a hole', holeAt([null, { partition: 0, offset: '5' }], 0)],
    ['a null entry', [null]],
    ['an undefined entry', [undefined]],
    ['a string instead of an array', 'ab'],
    ['an array-like object', { length: 1, 0: { partition: 0, offset: '5' } }],
    [
      'an index that only the prototype fills',
      Object.setPrototypeOf(
        holeAt([null], 0),
        Object.assign(Object.create(Array.prototype), { 0: { partition: 0, offset: '5' } })
      ),
    ],
  ])('refuses partitions with %s before any request', async (_, partitions) => {
    const { admin, cluster } = adminOver()

    await expect(admin.deleteTopicRecords({ topic: TOPIC, partitions })).rejects.toMatchObject({
      name: 'KafkaJSNonRetriableError',
    })
    expect(clusterCalls(cluster)).toBe(0)
  })

  test('refuses a partition listed twice before any request', async () => {
    const { admin, cluster } = adminOver()

    await expect(
      admin.deleteTopicRecords({
        topic: TOPIC,
        partitions: [
          { partition: 0, offset: '5' },
          { partition: 0, offset: '-1' },
        ],
      })
    ).rejects.toMatchObject({
      name: 'KafkaJSNonRetriableError',
      message: 'Invalid partitions, partition 0 is listed more than once',
    })
    expect(clusterCalls(cluster)).toBe(0)
  })

  test('sends -1, 0, the int64 maximum and a safe integer number as decimal offsets', async () => {
    const { admin, sent } = adminOver()

    await admin.deleteTopicRecords({
      topic: TOPIC,
      partitions: [
        { partition: 0, offset: '-1' },
        { partition: 1, offset: '9223372036854775807' },
      ],
    })
    await admin.deleteTopicRecords({
      topic: TOPIC,
      partitions: [
        { partition: 0, offset: '0' },
        { partition: 1, offset: 7 },
      ],
    })

    expect(sent).toEqual([
      [
        { partition: 0, offset: '-1' },
        { partition: 1, offset: '9223372036854775807' },
      ],
      [
        { partition: 0, offset: '0' },
        { partition: 1, offset: '7' },
      ],
    ])
  })

  test('sends the partitions as they were when the call started', async () => {
    const { admin, sent } = adminOver()
    const partitions = [{ partition: 0, offset: '5' }]

    const call = admin.deleteTopicRecords({ topic: TOPIC, partitions })
    partitions[0].offset = '-1'
    partitions.push({ partition: 1, offset: '-1' })
    await call

    expect(sent).toEqual([[{ partition: 0, offset: '5' }]])
  })

  test('reads the partitions getter of the options once, when the call starts', async () => {
    const { admin, sent } = adminOver()
    let reads = 0
    let partitions = [{ partition: 0, offset: '5' }]
    const options = {
      topic: TOPIC,
      get partitions() {
        reads += 1
        return partitions
      },
    }

    const call = admin.deleteTopicRecords(options)
    partitions = [{ partition: 1, offset: '-1' }]
    await call

    expect(reads).toBe(1)
    expect(sent).toEqual([[{ partition: 0, offset: '5' }]])
  })

  test('reads the partition and offset of each entry once', async () => {
    const { admin, sent } = adminOver()
    let partitionReads = 0
    let offsetReads = 0
    const entry = {
      get partition() {
        partitionReads += 1
        return partitionReads === 1 ? 0 : 1
      },
      get offset() {
        offsetReads += 1
        return offsetReads === 1 ? '5' : '-1'
      },
    }

    await admin.deleteTopicRecords({ topic: TOPIC, partitions: [entry] })

    expect(partitionReads).toBe(1)
    expect(offsetReads).toBe(1)
    expect(sent).toEqual([[{ partition: 0, offset: '5' }]])
  })

  test('reads the length of the partitions array once and sends what it first saw', async () => {
    const { admin, sent } = adminOver()
    let lengthReads = 0
    const partitions = new Proxy([{ partition: 0, offset: '5' }], {
      get(target, key, receiver) {
        if (key !== 'length') return Reflect.get(target, key, receiver)
        lengthReads += 1
        return lengthReads === 1 ? 1 : 0
      },
    })

    await admin.deleteTopicRecords({ topic: TOPIC, partitions })

    expect(lengthReads).toBe(1)
    expect(sent).toEqual([[{ partition: 0, offset: '5' }]])
  })

  test.each([
    ['a numeric string', '1'],
    ['an object whose valueOf is 1', { valueOf: () => 1 }],
    ['-1', -1],
    ['NaN', NaN],
  ])('refuses partitions whose length is %s before any request', async (_, length) => {
    const { admin, cluster } = adminOver()
    const partitions = new Proxy([{ partition: 0, offset: '5' }], {
      get: (target, key, receiver) =>
        key === 'length' ? length : Reflect.get(target, key, receiver),
    })

    await expect(admin.deleteTopicRecords({ topic: TOPIC, partitions })).rejects.toMatchObject({
      name: 'KafkaJSNonRetriableError',
    })
    expect(clusterCalls(cluster)).toBe(0)
  })

  test('refuses a call on a disconnected admin before reading its input', async () => {
    const { admin, cluster } = adminOver()
    cluster.disconnect = jest.fn(async () => {})
    await admin.disconnect()
    let reads = 0
    const options = {
      topic: TOPIC,
      get partitions() {
        reads += 1
        return 'not an array'
      },
    }

    await expect(admin.deleteTopicRecords(options)).rejects.toMatchObject({
      name: 'KafkaJSNonRetriableError',
      message: 'The admin is disconnected or disconnecting; call connect() to resume',
    })
    expect(reads).toBe(0)
    expect(cluster.fetchTopicsOffset).not.toHaveBeenCalled()
  })

  test('warns for an offset below the low watermark beyond 2^53', async () => {
    const { admin, logger } = adminOver({ lows: ['9007199254740993', '0'] })

    await admin.deleteTopicRecords({
      topic: TOPIC,
      partitions: [{ partition: 0, offset: '9007199254740992' }],
    })

    expect(logger.warn).toHaveBeenCalledWith(
      'The requested offset is before the earliest offset maintained on the partition - no records will be deleted from this partition',
      { topic: TOPIC, partition: 0, offset: '9007199254740992' }
    )
  })

  test('does not warn for -1 or an offset at the low watermark', async () => {
    const { admin, logger } = adminOver({ lows: ['10', '10'] })

    await admin.deleteTopicRecords({
      topic: TOPIC,
      partitions: [
        { partition: 0, offset: '-1' },
        { partition: 1, offset: '10' },
      ],
    })

    expect(logger.warn).not.toHaveBeenCalled()
  })
})
