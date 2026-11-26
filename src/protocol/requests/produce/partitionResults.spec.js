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

    test('the results are built in one pass over the answer, with no intermediate array', () => {
      // Every acknowledged send reads its answer through this function.
      const flatMap = jest.spyOn(Array.prototype, 'flatMap')
      const results = partitionResults(data)
      const calls = flatMap.mock.calls.length
      flatMap.mockRestore()

      expect(calls).toBe(0)
      expect(results).toHaveLength(3)
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

  describe('an answer read entry by entry', () => {
    const accepted = (partition, fields = {}) => ({
      partition,
      errorCode: 0,
      baseOffset: String(partition),
      logAppendTime: '-1',
      logStartOffset: '0',
      ...fields,
    })

    test('keeps every entry, a partition id repeated within a topic and across topics too', () => {
      const results = partitionResults({
        topics: [
          { topicName: 'a', partitions: [accepted(1), accepted(1), accepted(2), accepted(1)] },
          { topicName: 'b', partitions: [accepted(1)] },
          { topicName: 'a', partitions: [accepted(1)] },
        ],
      })

      expect(results.map(({ topicName, partition }) => `${topicName}/${partition}`)).toEqual([
        'a/1',
        'a/1',
        'a/2',
        'a/1',
        'b/1',
        'a/1',
      ])
    })

    test('defaults only an undefined log start offset, and passes every other field through as it is', () => {
      const withOffset = (logStartOffset) =>
        partitionResults({
          topics: [{ topicName: null, partitions: [accepted(0, { logStartOffset })] }],
        })[0].logStartOffset

      expect(withOffset(undefined)).toBe('-1')
      for (const kept of [null, 0, '', '0', '250', false]) {
        expect(withOffset(kept)).toBe(kept)
      }
      expect(
        partitionResults({
          topics: [
            {
              topicName: null,
              partitions: [{ partition: 3, errorCode: 0, baseOffset: null, logAppendTime: null }],
            },
          ],
        })
      ).toStrictEqual([
        {
          topicName: null,
          partition: 3,
          errorCode: 0,
          baseOffset: null,
          logAppendTime: null,
          logStartOffset: '-1',
          error: null,
        },
      ])
    })

    test('skips the absent entries of a sparse topic list and of a sparse partition list', () => {
      const topics = new Array(3)
      topics[1] = { topicName: 'a', partitions: Object.assign(new Array(3), { 2: accepted(2) }) }
      topics[2] = { topicName: 'b', partitions: [accepted(0)] }

      const results = partitionResults({ topics })

      expect(results.map(({ topicName, partition }) => `${topicName}/${partition}`)).toEqual([
        'a/2',
        'b/0',
      ])
      expect(results[0]).toStrictEqual({
        topicName: 'a',
        partition: 2,
        errorCode: 0,
        baseOffset: '2',
        logAppendTime: '-1',
        logStartOffset: '0',
        error: null,
      })
    })

    test.each([
      ['a topic', { topics: [undefined] }],
      ['a partition', { topics: [{ topicName: 'a', partitions: [accepted(0), undefined] }] }],
    ])('still throws for %s that is present but undefined', (_, data) => {
      expect(() => partitionResults(data)).toThrow(TypeError)
    })

    test.each([
      [
        'undefined',
        undefined,
        "Cannot destructure property 'topicName' of 'undefined' as it is undefined.",
      ],
      ['null', null, "Cannot destructure property 'topicName' of 'object null' as it is null."],
    ])(
      'the TypeError for the topic entry %s is the one destructuring it gives',
      (_, entry, message) => {
        expect(() => partitionResults({ topics: [entry] })).toThrow(new TypeError(message))
      }
    )

    test('reads the entries that were there when it began, whatever the lists become meanwhile', () => {
      const partitions = [accepted(0), accepted(1), accepted(2)]
      const topics = [{ topicName: 'a', partitions }]
      Object.defineProperty(partitions[0], 'logAppendTime', {
        get() {
          partitions.push(accepted(9))
          topics.push({ topicName: 'late', partitions: [accepted(9)] })
          delete partitions[1]
          return '-1'
        },
      })

      const results = partitionResults({ topics })

      expect(results.map(({ topicName, partition }) => `${topicName}/${partition}`)).toEqual([
        'a/0',
        'a/2',
      ])
    })
  })
})
