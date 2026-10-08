const Encoder = require('../../encoder')
const { KafkaJSProtocolError } = require('../../../errors')
const { parse, partitionResults } = require('./partitionResults')
const { decode: decodeV3 } = require('./v3/response')
const { decode: decodeV8, parse: parseV8 } = require('./v8/response')
const { parse: parseV9 } = require('./v9/response')
const { decode: decodeV10, parse: parseV10 } = require('./v10/response')
const { parse: parseV11 } = require('./v11/response')
const { parse: parseV12 } = require('./v12/response')
const { parse: parseV13 } = require('./v13/response')

const RESULT_KEYS = [
  'topicName',
  'partition',
  'errorCode',
  'baseOffset',
  'logAppendTime',
  'logStartOffset',
  'error',
]

/**
 * A flexible (v10/v11) Produce response body for one topic, built field by field.
 * @param {string} topicName
 * @param {{ partition: number, errorCode: number, baseOffset: number, logStartOffset: number }[]} partitions
 * @returns {Buffer}
 */
const flexibleResponse = (topicName, partitions) =>
  new Encoder()
    .writeUVarIntArray([
      new Encoder()
        .writeUVarIntString(topicName)
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

const fixture = (name) => Buffer.from(require(`./fixtures/${name}.json`))

describe('Protocol > Requests > Produce > partition results', () => {
  test('every version from v8 parses through the one shared function', () => {
    for (const versionParse of [parseV8, parseV9, parseV10, parseV11, parseV12, parseV13]) {
      expect(versionParse).toBe(parse)
    }
  })

  test('parse resolves a fully successful response unchanged', async () => {
    const data = await decodeV10(fixture('v10_response'))

    await expect(parse(data)).resolves.toBe(data)
  })

  test('one result per partition, in response order, with no throw', async () => {
    const data = await decodeV10(
      flexibleResponse('test-topic', [
        { partition: 0, errorCode: 0, baseOffset: 100, logStartOffset: 0 },
        { partition: 1, errorCode: 59, baseOffset: -1, logStartOffset: 250 },
      ])
    )

    const results = partitionResults(data)

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
      errorMessage: null,
      recordErrors: [],
    })
  })

  describe('across topics', () => {
    const partitionOf = (partition, errorCode) => ({
      partition,
      errorCode,
      baseOffset: '-1',
      logAppendTime: '-1',
      logStartOffset: '0',
    })
    const data = {
      topics: [
        { topicName: 'topic-b', partitions: [partitionOf(4, 0), partitionOf(1, 6)] },
        { topicName: 'topic-a', partitions: [partitionOf(0, 59)] },
      ],
    }

    test('results follow the response order of topics and of partitions', () => {
      expect(
        partitionResults(data).map(({ topicName, partition }) => [topicName, partition])
      ).toEqual([
        ['topic-b', 4],
        ['topic-b', 1],
        ['topic-a', 0],
      ])
    })

    test('parse throws the first failing partition of the first failing topic in response order', async () => {
      await expect(parse(data)).rejects.toMatchObject({
        type: 'NOT_LEADER_FOR_PARTITION',
        topic: 'topic-b',
        partition: 1,
      })
    })
  })

  test('a result carries exactly its seven keys, never the decoded failure detail', async () => {
    const [result] = partitionResults(await decodeV10(fixture('v10_response_not_leader')))

    expect(Object.keys(result)).toEqual(RESULT_KEYS)
  })

  test("a result's error is the error parse throws for that partition", async () => {
    const data = await decodeV8(fixture('v8_response_record_errors'))
    const [result] = partitionResults(data)
    const thrown = await parse(data).catch((e) => e)

    expect(result).toMatchObject({
      partition: 0,
      errorCode: 87,
      baseOffset: '100',
      logStartOffset: '0',
    })
    for (const key of [
      'name',
      'type',
      'code',
      'topic',
      'partition',
      'errorMessage',
      'recordErrors',
    ]) {
      expect(result.error[key]).toEqual(thrown[key])
    }
    expect(result.error.recordErrors).toEqual([{ batchIndex: 1, errorMessage: 'record too large' }])
  })

  test('the leader hint the broker attaches to a leadership error rides on the result too', async () => {
    const [result] = partitionResults(await decodeV10(fixture('v10_response_not_leader')))

    expect(result.error).toMatchObject({
      type: 'NOT_LEADER_FOR_PARTITION',
      currentLeader: { leaderId: 2, leaderEpoch: 7 },
      nodeEndpoints: [{ nodeId: 2, host: '127.0.0.1', port: 9095, rack: null }],
    })
  })

  test('below v5 there is no log start offset: the result reads -1, the field default', async () => {
    expect(partitionResults(await decodeV3(fixture('v3_response')))).toEqual([
      {
        topicName: 'test-topic-ebba68879c6f5081d8c2',
        partition: 0,
        errorCode: 0,
        baseOffset: '0',
        logAppendTime: '-1',
        logStartOffset: '-1',
        error: null,
      },
    ])
  })

  test('below v8 a failing partition carries its address and nothing the version lacks', () => {
    const [{ error }] = partitionResults({
      topics: [
        {
          topicName: 'test-topic',
          partitions: [
            {
              partition: 2,
              errorCode: 6,
              baseOffset: '-1',
              logAppendTime: '-1',
              logStartOffset: '0',
            },
          ],
        },
      ],
    })

    expect(error).toMatchObject({
      type: 'NOT_LEADER_FOR_PARTITION',
      topic: 'test-topic',
      partition: 2,
    })
    expect(error).not.toHaveProperty('errorMessage')
    expect(error).not.toHaveProperty('recordErrors')
    expect(error).not.toHaveProperty('currentLeader')
  })
})
