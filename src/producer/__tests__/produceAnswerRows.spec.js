const createProducer = require('../index')
const Decoder = require('../../protocol/decoder')
const Encoder = require('../../protocol/encoder')
const RecordBatchDecoder = require('../../protocol/recordBatch/v0/decoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const { newLogger } = require('testHelpers')

const TOPIC = 'topic-a'
const NOT_LEADER_OR_FOLLOWER = 6
const RETRY = { retries: 3, initialRetryTime: 1, maxRetryTime: 2, factor: 0 }

/**
 * The batches of a Produce v7 request body, as the topic, partition and encoded record batch.
 * @param {Buffer} body
 * @returns {{ topic: string, partition: number, batch: Buffer }[]}
 */
const partitionsOf = (body) => {
  const decoder = new Decoder(body)
  decoder.readString()
  decoder.readInt16()
  decoder.readInt32()
  return decoder
    .readArray((topics) => {
      const topic = topics.readString()
      return topics.readArray((entries) => ({
        topic,
        partition: entries.readInt32(),
        batch: entries.readBytes(),
      }))
    })
    .flat()
}

/**
 * A Produce v7 response body for `TOPIC` with one row per entry, in the order given.
 * @param {{ partition: number, errorCode?: number, offset?: number }[]} rows a success row
 *   carries `offset` (default 100) as its base offset
 * @returns {Buffer}
 */
const answerWith = (rows) =>
  new Encoder()
    .writeArray([
      new Encoder().writeString(TOPIC).writeArray(
        rows.map(({ partition, errorCode = 0, offset = 100 }) =>
          new Encoder()
            .writeInt32(partition)
            .writeInt16(errorCode)
            .writeInt64(errorCode === 0 ? offset : -1)
            .writeInt64(-1)
            .writeInt64(0)
        )
      ),
    ])
    .writeInt32(0).buffer

/** Partition 0 acknowledged, then partition 0 again with a retriable refusal. */
const DUPLICATE_ROWS = [{ partition: 0 }, { partition: 0, errorCode: NOT_LEADER_OR_FOLLOWER }]

describe('Producer > a Produce answer checked against its request', () => {
  const closers = []

  afterEach(async () => {
    await Promise.all(closers.splice(0).map((close) => close()))
  })

  /**
   * A connected producer whose one broker is a real `Broker` over a fake socket. The broker
   * answers each Produce with the next of `answers`, and the last one for every request after.
   * An answer given as a function is built from the batches that request carried.
   * @param {(Buffer|((sent: { topic: string, partition: number }[]) => Buffer))[]} answers
   * @param {object} [options] `createProducer` options
   * @param {number} [partitions=1] partitions of every topic
   * @returns {Promise<{ producer: object, produces: () => { topic: string, partition: number,
   *   batch: Buffer }[][] }>} `produces` lists the batches each request carried
   */
  const connectedProducer = async (answers, options = {}, partitions = 1) => {
    const queue = [...answers]
    const wire = createGroupApiBroker({
      versions: { [apiKeys.Produce]: { minVersion: 3, maxVersion: 7 } },
      respond: () => {
        const next = queue.length > 1 ? queue.shift() : queue[0]
        return typeof next === 'function'
          ? next(partitionsOf(wire.requests[wire.requests.length - 1].body))
          : next
      },
    })
    wire.broker.nodeId = 0
    const cluster = createFakeCluster({ partitions })
    cluster.findBroker = jest.fn(async () => wire.broker)
    const producer = createProducer({
      cluster,
      logger: newLogger(),
      linger: 5,
      retry: RETRY,
      ...options,
    })
    await producer.connect()
    closers.push(async () => {
      await producer.disconnect()
      await wire.pool.destroy()
    })

    return {
      producer,
      produces: () =>
        wire.requests
          .filter(({ apiKey }) => apiKey === apiKeys.Produce)
          .map(({ body }) => partitionsOf(body)),
    }
  }

  test('idempotent: the batch retries with the same producer id, epoch and sequence and resolves from the valid answer', async () => {
    const { producer, produces } = await connectedProducer([
      answerWith(DUPLICATE_ROWS),
      answerWith([{ partition: 0, offset: 200 }]),
    ])

    const results = await producer.send({ topic: TOPIC, messages: [{ value: 'a' }] })

    expect(results).toHaveLength(1)
    expect(results[0]).toMatchObject({ topicName: TOPIC, partition: 0, baseOffset: '200' })
    const requests = produces()
    expect(requests).toHaveLength(2)
    const [first, second] = await Promise.all(
      requests.map(([{ batch }]) => RecordBatchDecoder(new Decoder(batch)))
    )
    expect({
      producerId: second.producerId,
      producerEpoch: second.producerEpoch,
      firstSequence: second.firstSequence,
    }).toEqual({
      producerId: first.producerId,
      producerEpoch: first.producerEpoch,
      firstSequence: first.firstSequence,
    })
    expect(first.firstSequence).toBe(0)
  })

  test('plain: the send rejects or retries, never resolves from the malformed answer', async () => {
    const { producer, produces } = await connectedProducer([answerWith(DUPLICATE_ROWS)], {
      idempotent: false,
      retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 2, factor: 0 },
    })

    const error = await producer.send({ topic: TOPIC, messages: [{ value: 'a' }] }).then(
      () => null,
      (e) => e
    )

    expect(error).not.toBeNull()
    expect([error.message, error.cause?.message].join(' ')).toMatch(/twice/)
    expect(produces().length).toBeGreaterThan(1)
  })

  test('a valid answer with rows in the reverse of the request order still settles every batch', async () => {
    const { producer, produces } = await connectedProducer(
      [(sent) => answerWith(sent.map(({ partition }) => ({ partition })).reverse())],
      {},
      2
    )

    const results = await producer.send({
      topic: TOPIC,
      messages: [
        { value: 'a', partition: 0 },
        { value: 'b', partition: 1 },
      ],
    })

    expect(results.map(({ partition }) => partition).sort()).toEqual([0, 1])
    const requests = produces()
    expect(requests).toHaveLength(1)
    expect(requests[0].map(({ partition }) => partition)).toEqual([0, 1])
  })

  const sendToTwoPartitions = (producer) =>
    producer.send({
      topic: TOPIC,
      messages: [
        { value: 'a', partition: 0 },
        { value: 'b', partition: 1 },
      ],
    })

  describe.each([
    ['idempotent', {}],
    ['plain', { idempotent: false }],
  ])('%s: a request for two partitions answered for one', (_mode, options) => {
    test('acknowledges the answered partition once and sends the missing one again until it is answered', async () => {
      const { producer, produces } = await connectedProducer(
        [answerWith([{ partition: 0, offset: 100 }]), answerWith([{ partition: 1, offset: 200 }])],
        options,
        2
      )

      const results = await sendToTwoPartitions(producer)

      expect(results).toHaveLength(2)
      expect(results.filter(({ partition }) => partition === 0)).toMatchObject([
        { topicName: TOPIC, partition: 0, baseOffset: '100' },
      ])
      expect(results.filter(({ partition }) => partition === 1)).toMatchObject([
        { topicName: TOPIC, partition: 1, baseOffset: '200' },
      ])
      expect(produces().map((batches) => batches.map(({ partition }) => partition))).toEqual([
        [0, 1],
        [1],
      ])
    })
  })
})
