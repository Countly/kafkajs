const { KafkaJSCorruptRecordError } = require('../../errors')
const { compressedBatch } = require('../../protocol/message/compression/fixtures/compressedBatch')
const { bombOf, MIB } = require('../../protocol/message/compression/fixtures/hostileCases')
const {
  TOPIC,
  TOPIC_ID,
  NODE_ID,
  fetchResponse,
  createFetchGroup,
} = require('./fixtures/fetchHarness')

const BOUND = 4 * MIB

/** Two plain records at offsets 0 and 1, then a gzip batch at offsets 2 to 6 that decompresses to 8 MiB. */
const partitionWithABomb = async () =>
  Buffer.concat([
    await compressedBatch({ codec: 'none', records: 2 }),
    await compressedBatch({
      codec: 'gzip',
      body: await bombOf('gzip', 8),
      records: 3,
      baseOffset: 2n,
      lastOffsetDelta: 4,
    }),
  ])

const offsetsOf = (partition) => partition.messages.map(({ offset }) => offset)

const request = (extra) => ({
  topics: [
    {
      topic: TOPIC,
      partitions: [0, 1].map((partition) => ({ partition, fetchOffset: '0', maxBytes: 1048576 })),
    },
  ],
  maxVersion: 12,
  ...extra,
})

describe('Consumer > a batch that breaks the decompression bound, at the Fetch response', () => {
  /** Answers every Fetch with `partitions`, as a v12 body. */
  const respondWith = async (partitions, options) => {
    const harness = await createFetchGroup({ partitions: [0, 1], fetchMaxVersion: 12, ...options })
    harness.setResponse(fetchResponse(partitions, { apiVersion: 12 }))
    return harness
  }

  test('keeps the records before the bomb, names the batch after it and leaves the other partition alone', async () => {
    const { broker } = await respondWith([
      { partition: 0, errorCode: 0, records: await partitionWithABomb() },
      {
        partition: 1,
        errorCode: 0,
        records: await compressedBatch({ codec: 'none', records: 2, baseOffset: 5n }),
      },
    ])

    const { responses } = await broker.fetch(request({ maxDecompressedBatchBytes: BOUND }))
    const [halted, healthy] = responses[0].partitions

    expect(offsetsOf(halted)).toEqual(['0', '1'])
    expect(halted.corruption).toMatchObject({ reason: 'decompressed-size', offset: '2' })
    expect(halted.corruption.error).toBeInstanceOf(KafkaJSCorruptRecordError)
    expect(halted.corruption.error).toMatchObject({ codec: 'gzip', limit: BOUND, nextOffset: '7' })
    expect(offsetsOf(healthy)).toEqual(['5', '6'])
    expect(healthy).not.toHaveProperty('corruption')
  })

  test('decompresses the same batch under a bound that holds it, and finds the records malformed', async () => {
    const { broker } = await respondWith([
      { partition: 0, errorCode: 0, records: await partitionWithABomb() },
    ])

    const { responses } = await broker.fetch(request({ maxDecompressedBatchBytes: 9 * MIB }))

    // 8 MiB of zeros is not three records: it passes the bound and fails as a malformed body
    expect(responses[0].partitions[0].corruption).toMatchObject({ reason: 'length', offset: '2' })
  })

  test('holds the batch to the default bound, 1 GiB, when the request names none', async () => {
    const { broker } = await respondWith([
      { partition: 0, errorCode: 0, records: await partitionWithABomb() },
    ])

    const { responses } = await broker.fetch(request())

    expect(responses[0].partitions[0].corruption).toMatchObject({ reason: 'length', offset: '2' })
  })

  test('carries no offset after a batch whose header could not be trusted', async () => {
    const forged = await compressedBatch({ codec: 'gzip', records: 3, lastOffsetDelta: -1 })
    const { broker } = await respondWith([{ partition: 0, errorCode: 0, records: forged }])

    const { responses } = await broker.fetch(request({ maxDecompressedBatchBytes: 511 }))
    const { corruption } = responses[0].partitions[0]

    expect(corruption).toMatchObject({ reason: 'count', offset: '0' })
    expect(corruption.error).not.toHaveProperty('nextOffset')
  })

  test('a corrupt checksum is a halt with no next offset', async () => {
    const wrongCrc = await compressedBatch({ codec: 'gzip', records: 3, crc: 0xdeadbeef })
    const { broker } = await respondWith([{ partition: 0, errorCode: 0, records: wrongCrc }])

    const { responses } = await broker.fetch(request())
    const { messages, corruption } = responses[0].partitions[0]

    expect(messages).toEqual([])
    expect(corruption).toMatchObject({ reason: 'checksum', offset: '0' })
    expect(corruption.error).not.toHaveProperty('nextOffset')
    expect(corruption.error).not.toHaveProperty('codec')
  })

  test.each([true, false])(
    'halts only the partition whose compressed batch does not decompress, with checkCrcs %s',
    async (checkCrcs) => {
      const junk = await compressedBatch({
        codec: 'gzip',
        records: 3,
        body: Buffer.from('this is not a gzip stream'),
      })
      const { broker } = await respondWith([
        { partition: 0, errorCode: 0, records: junk },
        {
          partition: 1,
          errorCode: 0,
          records: await compressedBatch({ codec: 'none', records: 2, baseOffset: 5n }),
        },
      ])

      const { responses } = await broker.fetch(request({ checkCrcs }))
      const [halted, healthy] = responses[0].partitions

      expect(halted.messages).toEqual([])
      expect(halted.corruption).toMatchObject({ reason: 'decompression', offset: '0' })
      expect(halted.corruption.error).toMatchObject({ codec: 'gzip', nextOffset: '3' })
      expect(offsetsOf(healthy)).toEqual(['5', '6'])
      expect(healthy).not.toHaveProperty('corruption')
    }
  )

  test('the consumer group hands its bound to every fetch it makes', async () => {
    const harness = await createFetchGroup({
      partitions: [0],
      fetchMaxVersion: 12,
      maxDecompressedBatchBytes: BOUND,
    })
    const fetch = jest.spyOn(harness.broker, 'fetch')

    await harness.group.fetch(NODE_ID)

    expect(fetch).toHaveBeenCalledWith(
      expect.objectContaining({ maxDecompressedBatchBytes: BOUND })
    )
  })

  test('the topic id version carries the bound the same way', async () => {
    const harness = await createFetchGroup({ partitions: [0], fetchMaxVersion: 18 })
    harness.setResponse(
      fetchResponse([{ partition: 0, errorCode: 0, records: await partitionWithABomb() }], {
        apiVersion: 18,
      })
    )

    const { responses } = await harness.broker.fetch({
      topics: [
        { topic: TOPIC, partitions: [{ partition: 0, fetchOffset: '0', maxBytes: 1048576 }] },
      ],
      topicIds: new Map([[TOPIC, TOPIC_ID]]),
      maxDecompressedBatchBytes: BOUND,
    })

    expect(responses[0].partitions[0].corruption).toMatchObject({ reason: 'decompressed-size' })
  })
})

describe('Consumer > a batch that breaks the decompression bound, in the consumer group', () => {
  const KEY = `${TOPIC}|0`
  let harness
  let events
  let batches

  /**
   * Answers each Fetch as a broker would: only the partitions it asked for, from the offset it
   * asked for. Partition 1 is always healthy; partition 0 answers `zero` from offset 0 and healthy
   * batches from the offsets after it.
   * @param {Buffer} zero the records of partition 0 from offset 0
   */
  const brokerAnswer = (zero) => () => {
    const asked = harness.fetchRequestPartitions()
    const from = harness.fetchRequestOffsets()

    return fetchResponse(
      asked.map((partition, at) => ({
        partition,
        errorCode: 0,
        records:
          partition === 1 ? batches.healthy : from[at] !== '0' ? batches.after[from[at]] : zero,
      })),
      { apiVersion: 12 }
    )
  }

  beforeAll(async () => {
    batches = {
      bomb: await partitionWithABomb(),
      wrongCrc: await compressedBatch({ codec: 'gzip', records: 3, crc: 0xdeadbeef }),
      bombFirst: await compressedBatch({
        codec: 'gzip',
        body: await bombOf('gzip', 8),
        records: 3,
        lastOffsetDelta: 2,
      }),
      healthy: await compressedBatch({ codec: 'none', records: 2, baseOffset: 5n }),
      after: {
        1: await compressedBatch({ codec: 'none', records: 2, baseOffset: 1n }),
        7: await compressedBatch({ codec: 'none', records: 2, baseOffset: 7n }),
      },
    }
  })

  beforeEach(async () => {
    harness = await createFetchGroup({
      partitions: [0, 1],
      fetchMaxVersion: 12,
      maxDecompressedBatchBytes: BOUND,
    })
    harness.setResponse(brokerAnswer(batches.bomb))
    events = []
    harness.group.instrumentationEmitter.addListener('consumer.partition_halted', (event) =>
      events.push(event)
    )
  })

  const offsetsOfPartition = (fetched, partition) =>
    fetched.find((batch) => batch.partition === partition).messages.map(({ offset }) => offset)

  test('delivers the valid prefix and the other partition, and halts the bomb partition once', async () => {
    const fetched = await harness.group.fetch(NODE_ID)

    expect(offsetsOfPartition(fetched, 0)).toEqual(['0', '1'])
    expect(offsetsOfPartition(fetched, 1)).toEqual(['5', '6'])
    expect(harness.group.haltedPartitions.get(KEY)).toMatchObject({
      offset: '2',
      reason: 'decompressed-size',
    })
    expect(events).toHaveLength(1)
    expect(events[0].payload).toMatchObject({
      topic: TOPIC,
      partition: 0,
      offset: '2',
      reason: 'decompressed-size',
      codec: 'gzip',
      limit: BOUND,
      nextOffset: '7',
    })
    expect(events[0].payload.error).toBeInstanceOf(KafkaJSCorruptRecordError)
  })

  test('does not fetch the halted partition again, and does not advance past the batch on its own', async () => {
    await harness.group.fetch(NODE_ID)
    await harness.group.fetch(NODE_ID)

    expect(harness.fetchRequestPartitions()).toEqual([1])
    expect(harness.offsetCommit).not.toHaveBeenCalled()
    expect(events).toHaveLength(1)
  })

  test('resumes at the next offset once the application seeks there, and the halt is gone', async () => {
    await harness.group.fetch(NODE_ID)
    harness.group.seek({ topic: TOPIC, partition: 0, offset: events[0].payload.nextOffset })
    const fetched = await harness.group.fetch(NODE_ID)

    expect(harness.group.haltedPartitions.has(KEY)).toBe(false)
    expect(offsetsOfPartition(fetched, 0)).toEqual(['7', '8'])
    expect(events).toHaveLength(1)
  })

  test('halts again at the same batch after a seek back, as a new episode', async () => {
    await harness.group.fetch(NODE_ID)
    harness.group.seek({ topic: TOPIC, partition: 0, offset: '0' })
    await harness.group.fetch(NODE_ID)

    expect(events).toHaveLength(2)
  })

  test('halts a partition whose first batch is the bomb, with nothing delivered from it', async () => {
    harness.setResponse(brokerAnswer(batches.bombFirst))

    const fetched = await harness.group.fetch(NODE_ID)

    expect(offsetsOfPartition(fetched, 0)).toEqual([])
    expect(offsetsOfPartition(fetched, 1)).toEqual(['5', '6'])
    expect(harness.group.haltedPartitions.get(KEY)).toMatchObject({ offset: '0' })
    expect(events[0].payload).toMatchObject({ offset: '0', nextOffset: '3', codec: 'gzip' })
  })

  test('a listener that throws neither undoes the halt nor stops the other partition', async () => {
    harness.group.instrumentationEmitter.addListener('consumer.partition_halted', () => {
      throw new Error('listener failure')
    })

    const fetched = await harness.group.fetch(NODE_ID)
    await harness.group.fetch(NODE_ID)

    expect(offsetsOfPartition(fetched, 1)).toEqual(['5', '6'])
    expect(harness.group.haltedPartitions.has(KEY)).toBe(true)
    expect(events).toHaveLength(1)
  })

  test('a seek to the next offset made inside the event resumes the partition', async () => {
    harness.group.instrumentationEmitter.addListener('consumer.partition_halted', ({ payload }) =>
      harness.group.seek({
        topic: payload.topic,
        partition: payload.partition,
        offset: payload.nextOffset,
      })
    )

    await harness.group.fetch(NODE_ID)
    const fetched = await harness.group.fetch(NODE_ID)

    expect(offsetsOfPartition(fetched, 0)).toEqual(['7', '8'])
    expect(harness.group.haltedPartitions.has(KEY)).toBe(false)
  })

  test('a checksum mismatch halts the partition with no next offset, and no second event follows recovery', async () => {
    harness.setResponse(brokerAnswer(batches.wrongCrc))

    await harness.group.fetch(NODE_ID)
    expect(harness.group.haltedPartitions.get(KEY)).toMatchObject({
      reason: 'checksum',
      offset: '0',
    })
    expect(events[0].payload.nextOffset).toBeUndefined()

    harness.group.seek({ topic: TOPIC, partition: 0, offset: '1' })
    await harness.group.fetch(NODE_ID)

    expect(events).toHaveLength(1)
    expect(harness.group.haltedPartitions.has(KEY)).toBe(false)
  })
})
