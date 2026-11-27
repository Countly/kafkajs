const { createGroupApiBroker } = require('./fixtures/groupApiHarness')
const apiKeys = require('../../protocol/requests/apiKeys')
const Encoder = require('../../protocol/encoder')
const offsetFetchV7 = require('../../../testHelpers/wire/offsetFetchV7')
const offsetFetchV9 = require('../../../testHelpers/wire/offsetFetchV9')
const offsetFetchV10 = require('../../../testHelpers/wire/offsetFetchV10')

const GROUP = 'group-A'
const TOPIC = 'test-topic'
const OTHER = 'other-topic'
const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const OTHER_ID = 'ffffffff-0506-0708-090a-0b0c0d0e0f10'
const UNKNOWN_ID = '11111111-2222-3333-4444-555555555555'
const VERSIONS = [6, 9, 10]

/**
 * @param {number} partition
 * @param {string} [offset='10']
 * @param {number} [errorCode=0]
 * @returns {object} one partition row of an answer
 */
const cell = (partition, offset = '10', errorCode = 0) => ({
  partition,
  offset,
  leaderEpoch: -1,
  metadata: null,
  errorCode,
})

/**
 * @param {string} name
 * @param {string} id
 * @param {...object} partitions rows from `cell`
 * @returns {{ name: string, id: string, partitions: object[] }} one topic entry of an answer, named
 *   below v10 and keyed by id at v10
 */
const topic = (name, id, ...partitions) => ({ name, id, partitions })

const ASKED = topic(TOPIC, TOPIC_ID, cell(0), cell(1))

/**
 * @param {object} p a row from `cell`
 * @returns {Encoder} the row as v1 writes it: no leader epoch
 */
const rowV1 = (p) =>
  new Encoder()
    .writeInt32(p.partition)
    .writeInt64(p.offset)
    .writeString(p.metadata)
    .writeInt16(p.errorCode)

/**
 * @param {object} p a row from `cell`
 * @returns {Encoder} the row as v5 writes it: with the committed leader epoch
 */
const rowV5 = (p) =>
  new Encoder()
    .writeInt32(p.partition)
    .writeInt64(p.offset)
    .writeInt32(p.leaderEpoch)
    .writeString(p.metadata)
    .writeInt16(p.errorCode)

/**
 * Encodes the answer to an OffsetFetch as the broker writes it at `version`.
 * @param {number} version 1, 5, 6, 9 or 10
 * @param {object} answer
 * @param {ReturnType<typeof topic>[]} [answer.entries]
 * @param {number} [answer.errorCode=0] the group's error code (versions 2 and up)
 * @param {string} [answer.groupId] the group the answer names (versions 8 and up)
 * @returns {Buffer} the response body
 */
const answerAt = (version, { entries = [], errorCode = 0, groupId = GROUP }) => {
  if (version === 1) {
    const rows = ({ name, partitions }) =>
      new Encoder().writeString(name).writeArray(partitions.map(rowV1))
    return new Encoder().writeArray(entries.map(rows)).buffer
  }
  if (version === 5) {
    const rows = ({ name, partitions }) =>
      new Encoder().writeString(name).writeArray(partitions.map(rowV5))
    return new Encoder().writeInt32(0).writeArray(entries.map(rows)).writeInt16(errorCode).buffer
  }
  const named = entries.map(({ name, partitions }) => ({ topic: name, partitions }))
  if (version === 6) {
    return offsetFetchV7.buildResponse({ throttleTime: 0, errorCode, topics: named })
  }
  if (version <= 9) {
    return offsetFetchV9.buildResponse({ throttleTime: 0, groupId, errorCode, topics: named })
  }
  return offsetFetchV10.buildResponse({
    throttleTime: 0,
    groupId,
    errorCode,
    topics: entries.map(({ id, partitions }) => ({ topicId: id, partitions })),
  })
}

/**
 * @param {number} version
 * @param {number[]|null} partitions the partitions of `TOPIC` to read; null reads every topic
 * @returns {object[]|null} the request's topic selection; at v10 its entry carries the topic id
 */
const selectionAt = (version, partitions) =>
  partitions === null
    ? null
    : [
        {
          topic: TOPIC,
          ...(version === 10 ? { topicId: TOPIC_ID } : {}),
          partitions: partitions.map((partition) => ({ partition })),
        },
      ]

describe('Broker > offsetFetch answer coverage', () => {
  const harnesses = []

  afterEach(async () => {
    await Promise.all(harnesses.splice(0).map(({ pool }) => pool.destroy()))
  })

  /**
   * One OffsetFetch answered with `answer` encoded at `version`, through a real broker and
   * connection.
   * @param {number} version
   * @param {object} answer see `answerAt`
   * @param {object} [options]
   * @param {object[]|null} [options.topics] the request's topic selection, partitions 0 and 1 of
   *   `TOPIC` by default
   * @param {boolean} [options.returnErrors]
   * @param {string} [options.groupId]
   * @returns {Promise<object>}
   */
  const fetch = (version, answer, options = {}) => {
    const { topics = selectionAt(version, [0, 1]), returnErrors, groupId = GROUP } = options
    const harness = createGroupApiBroker({
      versions: { [apiKeys.OffsetFetch]: { minVersion: 0, maxVersion: version } },
      respond: () => answerAt(version, answer),
    })
    harnesses.push(harness)
    return harness.broker.offsetFetch({ groupId, topics, returnErrors })
  }

  /** @returns {number[]} the OffsetFetch versions the latest broker was asked in */
  const versionsSent = () => harnesses[harnesses.length - 1].requests.map((r) => r.apiVersion)

  test.each([
    ['no topics at all', [0, 1], []],
    ['a missing partition', [0, 1], [topic(TOPIC, TOPIC_ID, cell(0))]],
    [
      'an unrequested topic beside the requested rows',
      [0, 1],
      [ASKED, topic(OTHER, OTHER_ID, cell(0))],
    ],
    ['an unrequested topic with no partitions', [0, 1], [ASKED, topic(OTHER, OTHER_ID)]],
    ['only another topic', [0, 1], [topic(OTHER, OTHER_ID, cell(0), cell(1))]],
    ['an unrequested partition', [0, 1], [topic(TOPIC, TOPIC_ID, cell(0), cell(1), cell(7))]],
    [
      'a partition answered twice with different offsets',
      [0],
      [topic(TOPIC, TOPIC_ID, cell(0, '10'), cell(0, '9999'))],
    ],
    [
      'one topic answered in two entries',
      [0, 1],
      [topic(TOPIC, TOPIC_ID, cell(0)), topic(TOPIC, TOPIC_ID, cell(1))],
    ],
    [
      'one topic answered in two entries, in reverse order',
      [0, 1],
      [topic(TOPIC, TOPIC_ID, cell(1)), topic(TOPIC, TOPIC_ID, cell(0))],
    ],
    [
      'a requested topic answered again with no partitions',
      [0, 1],
      [ASKED, topic(TOPIC, TOPIC_ID)],
    ],
  ])('%s is refused retriably', async (_, asked, entries) => {
    for (const version of VERSIONS) {
      await expect(
        fetch(version, { entries }, { topics: selectionAt(version, asked) })
      ).rejects.toMatchObject({
        name: 'KafkaJSError',
        retriable: true,
        message: expect.stringMatching(/^OffsetFetch response /),
      })
      expect(versionsSent()).toEqual([version])
    }
  })

  test.each([['-1'], ['50']])(
    'an answer for a topic id the request did not carry, with offset %s, is refused retriably',
    async (offset) => {
      const entries = [topic('unnamed', UNKNOWN_ID, cell(0, offset))]

      await expect(fetch(10, { entries }, { topics: selectionAt(10, [0]) })).rejects.toMatchObject({
        name: 'KafkaJSError',
        retriable: true,
        message: expect.stringMatching(/^OffsetFetch response /),
      })
    }
  )

  test('an answer for another group is refused at v10 with both groups named', async () => {
    await expect(fetch(10, { entries: [ASKED], groupId: 'group-B' })).rejects.toMatchObject({
      name: 'KafkaJSInvariantViolation',
      message: expect.stringContaining('requested group "group-A" but received group "group-B"'),
    })
  })

  test.each([[8], [9]])(
    'an answer for another group is still refused at v%i with both groups named',
    async (version) => {
      await expect(fetch(version, { entries: [ASKED], groupId: 'group-B' })).rejects.toMatchObject({
        name: 'KafkaJSInvariantViolation',
        message: expect.stringContaining('requested group "group-A" but received group "group-B"'),
      })
    }
  )

  // A lone surrogate is written as U+FFFD, so a broker echoes that and not the string that was asked.
  test.each([[8], [9], [10]])(
    'a group id with a lone surrogate answered as the bytes sent resolves at v%i',
    async (version) => {
      const response = await fetch(
        version,
        { entries: [ASKED], groupId: 'team-�' },
        { groupId: 'team-\uD800' }
      )

      expect(response.responses[0].partitions.map(({ partition }) => partition)).toEqual([0, 1])
      expect(versionsSent()).toEqual([version])
    }
  )

  test.each([[8], [9], [10]])(
    'a group id with a lone surrogate is still refused at v%i for an answer for another group',
    async (version) => {
      for (const [asked, answered] of [
        ['team-\uD800', 'team-B'],
        ['team-A', 'team-�'],
      ]) {
        await expect(
          fetch(version, { entries: [ASKED], groupId: answered }, { groupId: asked })
        ).rejects.toMatchObject({ name: 'KafkaJSInvariantViolation' })
      }
    }
  )

  test('an answer that covers the request resolves at every version, rows in any order', async () => {
    const reordered = [topic(TOPIC, TOPIC_ID, cell(1, '11'), cell(0, '10'))]
    for (const version of [1, 5, ...VERSIONS]) {
      const response = await fetch(version, { entries: reordered })

      expect(response.responses[0].partitions.map(({ partition }) => partition)).toEqual([1, 0])
      expect(versionsSent()).toEqual([version])
    }
  })

  test.each([
    [16, 'NOT_COORDINATOR_FOR_GROUP', VERSIONS],
    [30, 'GROUP_AUTHORIZATION_FAILED', VERSIONS],
    [113, 'STALE_MEMBER_EPOCH', [9, 10]],
  ])(
    'a group error %i answered with no topics is reported as %s, not as a coverage error',
    async (errorCode, type, versions) => {
      for (const version of versions) {
        await expect(fetch(version, { entries: [], errorCode })).rejects.toMatchObject({ type })
      }
    }
  )

  test('a v1 answer with the error in every requested row is reported as that error', async () => {
    const entries = [topic(TOPIC, TOPIC_ID, cell(0, '-1', 16), cell(1, '-1', 16))]

    await expect(fetch(1, { entries })).rejects.toMatchObject({
      type: 'NOT_COORDINATOR_FOR_GROUP',
    })
  })

  test('a failed row beside a missing row is reported as the broker error, not as a coverage error', async () => {
    const entries = [topic(TOPIC, TOPIC_ID, cell(0, '-1', 88))]
    for (const version of VERSIONS) {
      await expect(fetch(version, { entries })).rejects.toMatchObject({
        type: 'UNSTABLE_OFFSET_COMMIT',
      })
    }
  })

  test('a topic the request named in two entries resolves when it is answered in two entries or in one', async () => {
    const named = (version) =>
      [0, 1].map((partition) => ({
        topic: TOPIC,
        ...(version === 10 ? { topicId: TOPIC_ID } : {}),
        partitions: [{ partition }],
      }))
    const partitionsOf = (list) =>
      list.flatMap(({ partitions }) => partitions.map(({ partition }) => partition))
    const answers = [
      [topic(TOPIC, TOPIC_ID, cell(0)), topic(TOPIC, TOPIC_ID, cell(1))],
      [topic(TOPIC, TOPIC_ID, cell(1)), topic(TOPIC, TOPIC_ID, cell(0))],
      [ASKED],
    ]
    for (const version of VERSIONS) {
      for (const entries of answers) {
        const response = await fetch(version, { entries }, { topics: named(version) })

        expect(partitionsOf(response.responses)).toEqual(partitionsOf(entries))
      }
    }
  })

  test.each([[null], [[]]])(
    'a read of every topic (%j) answered with two topics resolves at v6 and v9',
    async (topics) => {
      const entries = [topic(TOPIC, TOPIC_ID, cell(0)), topic(OTHER, OTHER_ID, cell(0), cell(3))]
      for (const version of [6, 9]) {
        const response = await fetch(version, { entries }, { topics })

        expect(response.responses.map((entry) => entry.topic)).toEqual([TOPIC, OTHER])
      }
    }
  )

  test.each([[null], [[]]])(
    'a read of every topic (%j) answered with a topic twice is refused at v6 and v9',
    async (topics) => {
      const entries = [topic(TOPIC, TOPIC_ID, cell(0)), topic(TOPIC, TOPIC_ID, cell(1))]
      for (const version of [6, 9]) {
        await expect(fetch(version, { entries }, { topics })).rejects.toMatchObject({
          name: 'KafkaJSInvariantViolation',
        })
      }
    }
  )

  test('an empty selection at v5 reads nothing: an answer with a topic is refused retriably', async () => {
    await expect(fetch(5, { entries: [ASKED] }, { topics: [] })).rejects.toMatchObject({
      name: 'KafkaJSError',
      retriable: true,
      message: expect.stringMatching(/^OffsetFetch response names /),
    })
  })

  test('an empty selection at v5 answered with no topics resolves', async () => {
    await expect(fetch(5, { entries: [] }, { topics: [] })).resolves.toMatchObject({
      responses: [],
    })
  })

  test('an admin read that takes errors as data keeps its own identity refusal for a foreign topic', async () => {
    const entries = [ASKED, topic(OTHER, OTHER_ID, cell(0))]

    await expect(fetch(9, { entries }, { returnErrors: true })).rejects.toMatchObject({
      name: 'KafkaJSInvariantViolation',
    })
  })

  test('an admin read that takes errors as data is handed a group error answer', async () => {
    await expect(
      fetch(9, { entries: [], errorCode: 16 }, { returnErrors: true })
    ).resolves.toMatchObject({ errorCode: 16, responses: [] })
  })
})
