const Encoder = require('../../encoder')
const { versions, protocol, checkPartitions } = require('./index')

const LEADER_NOT_AVAILABLE = 5
const TOPIC_AUTHORIZATION_FAILED = 29

/** A Metadata v1 answer naming `test-topic` with one healthy partition: no fixture holds one. */
const v1Answer = () =>
  new Encoder()
    .writeArray([
      new Encoder().writeInt32(0).writeString('localhost').writeInt32(9092).writeString(null),
    ])
    .writeInt32(0)
    .writeArray([
      new Encoder()
        .writeInt16(0)
        .writeString('test-topic')
        .writeBoolean(false)
        .writeArray([
          new Encoder()
            .writeInt16(0)
            .writeInt32(0)
            .writeInt32(0)
            .writeArray([0], 'int32')
            .writeArray([0], 'int32'),
        ]),
    ]).buffer

/** The answer each version decodes: the Apache vector where one is committed. */
const answers = {
  1: v1Answer,
  9: () => Buffer.from(require('./fixtures/v9_response_java.json')),
  12: () => Buffer.from(require('./fixtures/v12_response_java.json')),
  13: () => Buffer.from(require('./fixtures/v13_response_java.json')),
}

/**
 * The parse a request for `topics` gets at `version`.
 * @param {number} version
 * @param {string[]} topics
 */
const parseFor = (version, topics) => protocol({ version })({ topics }).response.parse

/**
 * A decoded answer at `version` naming `test-topic` with one partition that has no leader.
 * @param {number|string} version
 */
const leaderless = (version) => ({
  brokers: [],
  topicMetadata: [
    {
      topicErrorCode: 0,
      topic: 'test-topic',
      partitionMetadata: [{ partitionErrorCode: LEADER_NOT_AVAILABLE, partitionId: 0, leader: -1 }],
    },
  ],
  ...(Number(version) >= 13 ? { errorCode: 0 } : {}),
})

/**
 * Decodes `version`'s answer, then gives `test-topic`'s partition no leader, as a broker reports
 * one, and adds a topic the client may not describe.
 */
const decodedWithErrors = async (version) => {
  const { response } = protocol({ version })({ topics: [] })
  const data = await response.decode(answers[version]())
  Object.assign(data.topicMetadata[0].partitionMetadata[0], {
    partitionErrorCode: LEADER_NOT_AVAILABLE,
    leader: -1,
  })
  data.topicMetadata.push({
    topicErrorCode: TOPIC_AUTHORIZATION_FAILED,
    topic: 'denied',
    partitionMetadata: [],
  })
  return data
}

describe('Protocol > Requests > Metadata > a request for every topic', () => {
  describe.each([1, 9, 12, 13])('v%d', (version) => {
    test('parses an answer with a topic error and a partition error, keeping both codes', async () => {
      const data = await decodedWithErrors(version)

      await expect(parseFor(version, [])(data)).resolves.toBe(data)
      expect(data.topicMetadata[0].partitionMetadata[0]).toMatchObject({
        partitionErrorCode: LEADER_NOT_AVAILABLE,
        leader: -1,
      })
      expect(data.topicMetadata[1].topicErrorCode).toEqual(TOPIC_AUTHORIZATION_FAILED)
    })

    test('a request naming topics still throws the topic error first, then the partition error', async () => {
      const data = await decodedWithErrors(version)

      await expect(parseFor(version, ['test-topic', 'denied'])(data)).rejects.toMatchObject({
        type: 'TOPIC_AUTHORIZATION_FAILED',
      })
      data.topicMetadata.pop()
      await expect(parseFor(version, ['test-topic'])(data)).rejects.toMatchObject({
        name: 'KafkaJSProtocolError',
        type: 'LEADER_NOT_AVAILABLE',
        retriable: true,
      })
    })
  })

  test('v13 still throws its top-level error code', async () => {
    const { response } = protocol({ version: 13 })({ topics: [] })
    const data = await response.decode(
      Buffer.from(require('./fixtures/v13_response_rebootstrap_required.json'))
    )

    await expect(response.parse(data)).rejects.toMatchObject({
      type: 'REBOOTSTRAP_REQUIRED',
      code: 129,
    })
  })

  test.each(versions)(
    'at v%s, only a request with no topics gets the lenient parse',
    async (version) => {
      await expect(parseFor(version, [])(leaderless(version))).resolves.toMatchObject({
        topicMetadata: [{ topic: 'test-topic' }],
      })
      await expect(parseFor(version, ['test-topic'])(leaderless(version))).rejects.toMatchObject({
        type: 'LEADER_NOT_AVAILABLE',
      })
    }
  )

  test.each(versions)(
    'at v%s, a request built without a topics list is not an all-topics request and throws on a partition error',
    async (version) => {
      const { response } = protocol({ version })({})

      await expect(response.parse(leaderless(version))).rejects.toMatchObject({
        name: 'KafkaJSProtocolError',
        type: 'LEADER_NOT_AVAILABLE',
        retriable: true,
      })
    }
  )
})

describe('Protocol > Requests > Metadata > a request that allows leaderless partitions', () => {
  const allowing = (version, topics) =>
    protocol({ version })({ topics, allowLeaderless: true }).response.parse

  test.each(versions)('at v%s, keeps a partition without a leader in the data', async (version) => {
    const data = leaderless(version)

    await expect(allowing(version, ['test-topic'])(data)).resolves.toBe(data)
    expect(data.topicMetadata[0].partitionMetadata[0]).toMatchObject({
      partitionErrorCode: LEADER_NOT_AVAILABLE,
      leader: -1,
    })
  })

  test.each(versions)(
    'at v%s, still throws a topic error and any other partition error',
    async (version) => {
      const denied = leaderless(version)
      denied.topicMetadata[0].topicErrorCode = TOPIC_AUTHORIZATION_FAILED
      await expect(allowing(version, ['test-topic'])(denied)).rejects.toMatchObject({
        type: 'TOPIC_AUTHORIZATION_FAILED',
      })

      const other = leaderless(version)
      other.topicMetadata[0].partitionMetadata.push({
        partitionErrorCode: TOPIC_AUTHORIZATION_FAILED,
        partitionId: 1,
        leader: -1,
      })
      await expect(allowing(version, ['test-topic'])(other)).rejects.toMatchObject({
        type: 'TOPIC_AUTHORIZATION_FAILED',
      })
    }
  )

  test.each(versions)('at v%s, a request that does not allow it still throws', async (version) => {
    await expect(parseFor(version, ['test-topic'])(leaderless(version))).rejects.toMatchObject({
      type: 'LEADER_NOT_AVAILABLE',
    })
  })
})

describe('Protocol > Requests > Metadata > a request that retains topic errors', () => {
  const UNKNOWN_TOPIC_OR_PARTITION = 3
  const retaining = (version, topics, options = {}) =>
    protocol({ version })({ topics, retainTopicErrors: true, ...options }).response.parse

  /** `leaderless(version)` plus a topic the broker no longer has. */
  const withDeleted = (version) => {
    const data = leaderless(version)
    data.topicMetadata.push({
      topicErrorCode: UNKNOWN_TOPIC_OR_PARTITION,
      topic: 'deleted',
      partitionMetadata: [],
    })
    return data
  }

  test.each(versions)('at v%s, keeps a topic answered with its own error', async (version) => {
    const data = withDeleted(version)
    data.topicMetadata[0].partitionMetadata[0] = {
      partitionErrorCode: 0,
      partitionId: 0,
      leader: 0,
    }

    await expect(retaining(version, ['test-topic', 'deleted'])(data)).resolves.toBe(data)
    expect(data.topicMetadata.map(({ topic, topicErrorCode }) => [topic, topicErrorCode])).toEqual([
      ['test-topic', 0],
      ['deleted', UNKNOWN_TOPIC_OR_PARTITION],
    ])
  })

  test.each(versions)(
    'at v%s, leaves a healthy topic partition error to checkPartitions, which lets a leaderless one through only when allowed',
    async (version) => {
      const data = withDeleted(version)
      await expect(retaining(version, ['test-topic', 'deleted'])(data)).resolves.toBe(data)
      const kept = data.topicMetadata.filter(({ topicErrorCode }) => topicErrorCode === 0)

      expect(() => checkPartitions(kept)).toThrow(
        expect.objectContaining({ type: 'LEADER_NOT_AVAILABLE' })
      )
      expect(checkPartitions(kept, { allowLeaderless: true })).toBeUndefined()
      expect(data.topicMetadata).toHaveLength(2)

      if (Number(version) >= 13) {
        const failed = withDeleted(version)
        failed.errorCode = 129
        await expect(retaining(version, ['test-topic', 'deleted'])(failed)).rejects.toMatchObject({
          type: 'REBOOTSTRAP_REQUIRED',
        })
      }
    }
  )

  test.each(versions)(
    'at v%s, checkPartitions throws a partition error other than a missing leader even when leaderless partitions are allowed',
    async (version) => {
      const data = withDeleted(version)
      data.topicMetadata[0].partitionMetadata[0] = {
        ...data.topicMetadata[0].partitionMetadata[0],
        partitionErrorCode: 6,
      }
      await retaining(version, ['test-topic', 'deleted'])(data)

      expect(() => checkPartitions([data.topicMetadata[0]], { allowLeaderless: true })).toThrow(
        expect.objectContaining({ type: 'NOT_LEADER_FOR_PARTITION' })
      )
    }
  )
})
