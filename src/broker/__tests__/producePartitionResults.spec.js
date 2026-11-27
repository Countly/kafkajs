const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const { KafkaJSProtocolError } = require('../../errors')
const { createGroupApiBroker } = require('./fixtures/groupApiHarness')
const produceV11 = require('../../../testHelpers/wire/produceV11')

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

  describe('an answer whose rows do not match the request', () => {
    /** Partition 0 acknowledged, then partition 0 again with a retriable refusal. */
    const DUPLICATE = [
      { partition: 0, errorCode: 0, baseOffset: 100, logStartOffset: 0 },
      { partition: 0, errorCode: 6, baseOffset: -1, logStartOffset: 0 },
    ]
    /** Partition 0 acknowledged, then partition 7, which the request did not carry. */
    const EXTRA = [
      { partition: 0, errorCode: 0, baseOffset: 100, logStartOffset: 0 },
      { partition: 7, errorCode: 0, baseOffset: 100, logStartOffset: 0 },
    ]

    /** A Produce v7 response body for `test-topic`, whose partitions are not keyed by id. */
    const responseV7 = (partitions) =>
      new Encoder()
        .writeArray([
          new Encoder()
            .writeString('test-topic')
            .writeArray(
              partitions.map(({ partition, errorCode, baseOffset }) =>
                new Encoder()
                  .writeInt32(partition)
                  .writeInt16(errorCode)
                  .writeInt64(baseOffset)
                  .writeInt64(-1)
                  .writeInt64(0)
              )
            ),
        ])
        .writeInt32(0).buffer

    /**
     * One flagged produce answered with `partitions` encoded at `version`.
     * @param {7|11|13} version
     * @param {{ partition: number, errorCode: number, baseOffset: number, logStartOffset: number }[]} partitions
     * @returns {Promise<any>}
     */
    const produceAt = async (version, partitions) => {
      const body =
        version === 7
          ? responseV7(partitions)
          : responseFor(
              partitions,
              version === 13
                ? (topic) => topic.writeUuid(TOPIC_ID)
                : (topic) => topic.writeUVarIntString('test-topic')
            )
      return harnessAnswering(body, version).broker.produce({
        acks: -1,
        timeout: 30000,
        topicData,
        topicIds: version === 13 ? new Map([['test-topic', TOPIC_ID]]) : undefined,
        perPartitionResults: true,
      })
    }

    test('an answer [p0 success, p0 NOT_LEADER_OR_FOLLOWER] rejects the request retriably', async () => {
      for (const version of [7, 11, 13]) {
        await expect(produceAt(version, DUPLICATE)).rejects.toMatchObject({
          name: 'KafkaJSError',
          retriable: true,
          message: expect.stringMatching(/partition 0 of topic test-topic twice/),
        })
      }
    })

    test('an answer with an extra partition rejects the request retriably', async () => {
      for (const version of [7, 11, 13]) {
        await expect(produceAt(version, EXTRA)).rejects.toMatchObject({
          name: 'KafkaJSError',
          retriable: true,
          message: expect.stringMatching(
            /partition 7 of topic test-topic that the request did not carry/
          ),
        })
      }
    })
  })
})

describe('Broker > produce > records taken over during the call', () => {
  const harnesses = []

  afterEach(async () => {
    jest.restoreAllMocks()
    await Promise.all(harnesses.splice(0).map(({ pool }) => pool.destroy()))
  })

  test('changing a record, its Buffer, its arrays or its topic after the call changes nothing written or checked, and its records are dropped once encoded', async () => {
    const harness = createGroupApiBroker({
      versions: { [apiKeys.Produce]: { minVersion: 3, maxVersion: 11 } },
      respond: () =>
        responseFor([
          { partition: 0, errorCode: 0, baseOffset: 100, logStartOffset: 0 },
          { partition: 1, errorCode: 0, baseOffset: 7, logStartOffset: 0 },
        ]),
    })
    harnesses.push(harness)
    const value = Buffer.from('AAAA')
    const first = [{ key: 'kept', value, timestamp: 1700000000000 }]
    const second = [{ value: 'also kept', timestamp: 1700000000000 }]
    const sent = [
      {
        topic: 'test-topic',
        partitions: [
          { partition: 0, firstSequence: 0, messages: first },
          { partition: 1, firstSequence: 0, messages: second },
        ],
      },
    ]

    const built = []
    const lookup = harness.broker.lookupRequestWithMax.bind(harness.broker)
    jest.spyOn(harness.broker, 'lookupRequestWithMax').mockImplementation((...args) => {
      const build = lookup(...args)
      return (options) => {
        built.push(options)
        return build(options)
      }
    })

    const produced = harness.broker.produce({
      acks: -1,
      timeout: 30000,
      topicData: sent,
      perPartitionResults: true,
    })
    value.fill(0x5a)
    first[0].key = 'changed'
    first.push({ value: 'added' })
    second[0].value = 'changed too'
    sent[0].partitions.pop()
    sent[0].topic = 'other-topic'
    const results = await produced

    const { body } = harness.requests[0]
    const { topics } = produceV11.readRequest(body)
    expect(
      topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition, batches }) => ({
          partition,
          records: batches.map(({ recordCount }) => recordCount),
        })),
      }))
    ).toEqual([
      {
        topic: 'test-topic',
        partitions: [
          { partition: 0, records: [1] },
          { partition: 1, records: [1] },
        ],
      },
    ])
    for (const kept of ['AAAA', 'kept', 'also kept']) {
      expect(body.includes(Buffer.from(kept))).toBe(true)
    }
    for (const changed of ['ZZZZ', 'changed', 'added', 'other-topic']) {
      expect(body.includes(Buffer.from(changed))).toBe(false)
    }
    expect(results.map(({ partition, error }) => ({ partition, error }))).toEqual([
      { partition: 0, error: null },
      { partition: 1, error: null },
    ])
    const [{ topicData: request }] = built
    expect(request).not.toBe(sent)
    expect(request).toEqual([
      {
        topic: 'test-topic',
        partitions: [
          { partition: 0, firstSequence: 0, messages: null },
          { partition: 1, firstSequence: 0, messages: null },
        ],
      },
    ])
  })
})
