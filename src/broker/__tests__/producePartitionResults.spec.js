const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const { KafkaJSProtocolError } = require('../../errors')
const { createGroupApiBroker } = require('./fixtures/groupApiHarness')

/** The KIP-951 vector: `test-topic` partition 0 answered NOT_LEADER with a leader hint. */
const NOT_LEADER = Buffer.from(
  require('../../protocol/requests/produce/fixtures/v10_response_not_leader.json')
)

const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'

/**
 * A Produce v11-v13 response body for `test-topic`, built field by field.
 * @param {{ partition: number, errorCode: number, baseOffset: number, logStartOffset: number }[]} partitions
 * @param {(topic: Encoder) => Encoder} [writeTopicKey] writes what keys the topic: its name (the
 *   default, v11) or its id (v13)
 * @returns {Buffer}
 */
const responseFor = (
  partitions,
  writeTopicKey = (topic) => topic.writeUVarIntString('test-topic')
) =>
  new Encoder()
    .writeUVarIntArray([
      writeTopicKey(new Encoder())
        .writeUVarIntArray(
          partitions.map(({ partition, errorCode, baseOffset, logStartOffset }) =>
            new Encoder()
              .writeInt32(partition)
              .writeInt16(errorCode)
              .writeInt64(baseOffset)
              .writeInt64(-1)
              .writeInt64(logStartOffset)
              .writeUVarIntArray([])
              .writeUVarIntString(null)
              .writeTaggedFields()
          )
        )
        .writeTaggedFields(),
    ])
    .writeInt32(0)
    .writeTaggedFields().buffer

/** Partition 0 written at offset 100; partition 1 refused with UNKNOWN_PRODUCER_ID, log start 250. */
const MIXED_PARTITIONS = [
  { partition: 0, errorCode: 0, baseOffset: 100, logStartOffset: 0 },
  { partition: 1, errorCode: 59, baseOffset: -1, logStartOffset: 250 },
]
const MIXED = responseFor(MIXED_PARTITIONS)

/** The same answer at v13, where the topic is keyed by its id. */
const MIXED_BY_ID = responseFor(MIXED_PARTITIONS, (topic) => topic.writeUuid(TOPIC_ID))

const topicData = [
  {
    topic: 'test-topic',
    partitions: [0, 1].map((partition) => ({
      partition,
      firstSequence: 0,
      messages: [{ value: 'a', timestamp: 1700000000000 }],
    })),
  },
]

describe('Broker > produce > per-partition results', () => {
  const harnesses = []

  afterEach(async () => {
    await Promise.all(harnesses.splice(0).map(({ pool }) => pool.destroy()))
  })

  /**
   * A real Broker over a fake socket that advertises Produce v3 to `maxVersion` and answers every
   * request with `body`.
   * @param {Buffer} body
   * @param {number} [maxVersion]
   * @returns {{ broker: object, requests: { apiVersion: number }[] }}
   */
  const harnessAnswering = (body, maxVersion = 11) => {
    const harness = createGroupApiBroker({
      versions: { [apiKeys.Produce]: { minVersion: 3, maxVersion } },
      respond: () => body,
    })
    harnesses.push(harness)

    return harness
  }

  /**
   * Sends one produce at v11 and answers with the outcome.
   * @param {Buffer} body
   * @param {object} [request] extra `Broker#produce` arguments
   * @returns {Promise<any>}
   */
  const produceAnswering = (body, request = {}) =>
    harnessAnswering(body).broker.produce({ acks: -1, timeout: 30000, topicData, ...request })

  test('resolves one result per partition, the failing one included, instead of throwing', async () => {
    const results = await produceAnswering(MIXED, { perPartitionResults: true })

    expect(results).toEqual([
      {
        topicName: 'test-topic',
        partition: 0,
        errorCode: 0,
        baseOffset: '100',
        logAppendTime: '-1',
        logStartOffset: '0',
        error: null,
      },
      {
        topicName: 'test-topic',
        partition: 1,
        errorCode: 59,
        baseOffset: '-1',
        logAppendTime: '-1',
        logStartOffset: '250',
        error: expect.any(KafkaJSProtocolError),
      },
    ])
    expect(results[1].error).toMatchObject({
      type: 'UNKNOWN_PRODUCER_ID',
      code: 59,
      topic: 'test-topic',
      partition: 1,
    })
  })

  test('without the flag the same response rejects with the first failing partition, as before', async () => {
    await expect(produceAnswering(MIXED)).rejects.toMatchObject({
      name: 'KafkaJSProtocolError',
      type: 'UNKNOWN_PRODUCER_ID',
      topic: 'test-topic',
      partition: 1,
    })
  })

  test("a result's error is enriched as the thrown error is, the leader hint included", async () => {
    const [result] = await produceAnswering(NOT_LEADER, { perPartitionResults: true })
    const thrown = await produceAnswering(NOT_LEADER).catch((e) => e)

    expect(thrown).toBeInstanceOf(KafkaJSProtocolError)
    for (const key of [
      'name',
      'type',
      'code',
      'topic',
      'partition',
      'errorMessage',
      'recordErrors',
      'currentLeader',
      'nodeEndpoints',
    ]) {
      expect(result.error[key]).toEqual(thrown[key])
    }
    expect(result.error.currentLeader).toEqual({ leaderId: 2, leaderEpoch: 7 })
  })

  test('a fully successful response resolves every partition with a null error', async () => {
    const results = await produceAnswering(
      responseFor([
        { partition: 0, errorCode: 0, baseOffset: 100, logStartOffset: 0 },
        { partition: 1, errorCode: 0, baseOffset: 7, logStartOffset: 0 },
      ]),
      { perPartitionResults: true }
    )

    expect(
      results.map(({ partition, baseOffset, error }) => ({ partition, baseOffset, error }))
    ).toEqual([
      { partition: 0, baseOffset: '100', error: null },
      { partition: 1, baseOffset: '7', error: null },
    ])
  })

  test('at v13 the topic ids resolve to names and the offsets stay decimal strings', async () => {
    const { broker, requests } = harnessAnswering(MIXED_BY_ID, 13)

    const results = await broker.produce({
      acks: -1,
      timeout: 30000,
      topicData,
      topicIds: new Map([['test-topic', TOPIC_ID]]),
      perPartitionResults: true,
    })

    expect(requests[0].apiVersion).toEqual(13)
    expect(results).toEqual([
      {
        topicName: 'test-topic',
        partition: 0,
        errorCode: 0,
        baseOffset: '100',
        logAppendTime: '-1',
        logStartOffset: '0',
        error: null,
      },
      {
        topicName: 'test-topic',
        partition: 1,
        errorCode: 59,
        baseOffset: '-1',
        logAppendTime: '-1',
        logStartOffset: '250',
        error: expect.any(KafkaJSProtocolError),
      },
    ])
    expect(results[1].error).toMatchObject({
      type: 'UNKNOWN_PRODUCER_ID',
      topic: 'test-topic',
      partition: 1,
    })
  })

  test('with acks 0 the broker answers nothing, so the call resolves undefined, flagged or not', async () => {
    await expect(produceAnswering(MIXED, { acks: 0 })).resolves.toBeUndefined()
    await expect(
      produceAnswering(MIXED, { acks: 0, perPartitionResults: true })
    ).resolves.toBeUndefined()
  })
})
