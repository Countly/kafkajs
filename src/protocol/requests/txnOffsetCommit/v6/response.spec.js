const { decode, parse, validate } = require('./response')
const { KafkaJSInvariantViolation } = require('../../../../errors')

const vector = (caseId) => require(`../fixtures/java/txnOffsetCommitResponse-v6-${caseId}.json`)

const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const OTHER_TOPIC_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'
const FOREIGN_TOPIC_ID = '21222324-2526-2728-292a-2b2c2d2e2f30'
const NAME_OF = new Map([
  [TOPIC_ID, 'test-topic'],
  [OTHER_TOPIC_ID, 'other-topic'],
])

/** What the vectors' request committed: the first topic's partitions 0 and 1, the second's 0. */
const REQUESTED = [
  { topic: 'test-topic', partitions: [{ partition: 0 }, { partition: 1 }] },
  { topic: 'other-topic', partitions: [{ partition: 0 }] },
]

/** @returns {Promise<object>} the vector decoded with the names its request would have bound */
const decoded = (caseId) => decode(Buffer.from(vector(caseId).hex, 'hex'), { topicNames: NAME_OF })

describe('Protocol > Requests > TxnOffsetCommit > v6 response', () => {
  test.each([
    'ok',
    'groupIdNotFound',
    'staleMemberEpoch',
    'unknownTopicId',
    'mixedOutcomes',
    'unknownTags',
    'noTopics',
  ])(
    'decodes the Apache 4.4.0 %s vector into the v5 shape, each topic named from the request map',
    async (caseId) => {
      const { fields } = vector(caseId)

      expect(await decoded(caseId)).toEqual({
        throttleTime: 0,
        clientSideThrottleTime: fields.throttleTime,
        topics: fields.topics.map(({ topicId, partitions }) => ({
          topic: NAME_OF.get(topicId),
          partitions,
        })),
      })
    }
  )

  test('unknown tagged fields at the partition, topic and top level change nothing a caller sees', async () => {
    const withTags = await decoded('unknownTags')

    expect(vector('unknownTags').hex).not.toEqual(vector('ok').hex)
    expect(withTags).toEqual(await decoded('ok'))
  })

  test.each(['ok', 'unknownTags', 'noTopics'])(
    'fails with a typed error, never a native one, on every truncation of the %s vector',
    async (caseId) => {
      const body = Buffer.from(vector(caseId).hex, 'hex')

      for (let length = 0; length < body.length; length++) {
        await expect(decode(body.subarray(0, length), { topicNames: NAME_OF })).rejects.toThrow(
          expect.objectContaining({ name: expect.stringMatching(/^KafkaJS/) })
        )
      }
    }
  )

  test('keeps an id the request did not carry, with a null topic', async () => {
    const data = await decode(Buffer.from(vector('ok').hex, 'hex'), {
      topicNames: new Map([[TOPIC_ID, 'test-topic']]),
    })

    expect(data.topics).toEqual([
      {
        topic: 'test-topic',
        partitions: [
          { partition: 0, errorCode: 0 },
          { partition: 1, errorCode: 0 },
        ],
      },
      { topic: null, topicId: OTHER_TOPIC_ID, partitions: [{ partition: 0, errorCode: 0 }] },
    ])
  })

  test('decodes without any options, naming nothing', async () => {
    const data = await decode(Buffer.from(vector('ok').hex, 'hex'))

    expect(data.topics.map(({ topic, topicId }) => [topic, topicId])).toEqual([
      [null, TOPIC_ID],
      [null, OTHER_TOPIC_ID],
    ])
  })

  test('parse resolves an answer that committed everything', async () => {
    const data = await decoded('ok')

    await expect(parse(data)).resolves.toBe(data)
  })

  test.each([
    ['groupIdNotFound', 'GROUP_ID_NOT_FOUND', 69, false],
    ['staleMemberEpoch', 'STALE_MEMBER_EPOCH', 113, false],
    ['unknownTopicId', 'UNKNOWN_TOPIC_ID', 100, true],
  ])(
    'parse throws the %s vector as %s (%i), retriable: %p',
    async (caseId, type, code, retriable) => {
      await expect(parse(await decoded(caseId))).rejects.toMatchObject({ type, code, retriable })
    }
  )

  test("parse throws the first failed partition's error when the topics answered differently", async () => {
    await expect(parse(await decoded('mixedOutcomes'))).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_ID',
      code: 100,
    })
  })

  describe('validate', () => {
    test('accepts the answer that correlates with the request, whatever the order it came in', async () => {
      const data = await decoded('ok')
      const reordered = {
        ...data,
        topics: data.topics
          .map((topic) => ({ ...topic, partitions: [...topic.partitions].reverse() }))
          .reverse(),
      }

      expect(() => validate(data, REQUESTED)).not.toThrow()
      expect(() => validate(reordered, REQUESTED)).not.toThrow()
    })

    test('accepts partitions asked for as strings, as the consumer reports them', async () => {
      const asked = REQUESTED.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({ partition: String(partition) })),
      }))

      const data = await decoded('ok')

      expect(() => validate(data, asked)).not.toThrow()
    })

    test('accepts a request that named no topic and an answer that names none', async () => {
      const data = await decoded('noTopics')

      expect(() => validate(data, [])).not.toThrow()
    })

    test.each([
      [
        'a topic id the request never carried',
        ({ topics: [first, second] }) => [
          first,
          { topic: null, topicId: FOREIGN_TOPIC_ID, partitions: second.partitions },
        ],
      ],
      ['the same topic answered twice', ({ topics: [first, second] }) => [first, first, second]],
      ['a requested topic with no answer', ({ topics: [first] }) => [first]],
      ['no topic at all', () => []],
      [
        'the same partition answered twice',
        ({ topics: [first, second] }) => [
          { ...first, partitions: [first.partitions[0], first.partitions[0]] },
          second,
        ],
      ],
      [
        'a partition the request did not carry',
        ({ topics: [first, second] }) => [
          { ...first, partitions: [first.partitions[0], { partition: 7, errorCode: 0 }] },
          second,
        ],
      ],
      [
        'a requested partition with no answer',
        ({ topics: [first, second] }) => [{ ...first, partitions: [first.partitions[0]] }, second],
      ],
      [
        'a topic with no partitions where the request carried one',
        ({ topics: [first, second] }) => [first, { ...second, partitions: [] }],
      ],
    ])('refuses %s as a typed invariant violation', async (_, change) => {
      const data = await decoded('ok')
      const answer = { ...data, topics: change(data) }

      expect(() => validate(answer, REQUESTED)).toThrow(KafkaJSInvariantViolation)
    })
  })
})
