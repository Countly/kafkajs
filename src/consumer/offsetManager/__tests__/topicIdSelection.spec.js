const OffsetManager = require('../index')
const createAdmin = require('../../../admin')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { createGroupApiBroker } = require('../../../broker/__tests__/fixtures/groupApiHarness')
const { createErrorFromCode } = require('../../../protocol/error')
const apiKeys = require('../../../protocol/requests/apiKeys')
const { newLogger } = require('testHelpers')

const commitVector = require('../../../protocol/requests/offsetCommit/fixtures/java/offsetCommitRequest-v10-byTopicId.json')
const fetchVector = require('../../../protocol/requests/offsetFetch/fixtures/java/offsetFetchRequest-v10-byTopicId.json')
const commitAnswer = require('../../../protocol/requests/offsetCommit/fixtures/java/offsetCommitResponse-v10-ok.json')
const fetchAnswer = require('../../../protocol/requests/offsetFetch/fixtures/java/offsetFetchResponse-v10-ok.json')
const commitAnswerV9 = require('../../../protocol/requests/offsetCommit/fixtures/java/offsetCommitResponse-v9-basic.json')
const fetchAnswerV9 = require('../../../protocol/requests/offsetFetch/fixtures/java/offsetFetchResponse-v9-basic.json')

const TOPIC = 'test-topic'
const TOPIC_ID = commitVector.fields.topics[0].topicId
const OTHER_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'
const ZERO_ID = '00000000-0000-0000-0000-000000000000'
const PARTITIONS = [{ partition: 0, offset: '10' }]

/** A real Broker over a fake socket that offers `maxVersion` of one offsets API and answers in the version it was asked in. */
const brokerAt = (apiKey, maxVersion, answers) =>
  createGroupApiBroker({
    versions: { [apiKey]: { minVersion: 0, maxVersion } },
    respond: ({ apiVersion }) => Buffer.from(answers[apiVersion >= 10 ? 'v10' : 'v9'], 'hex'),
  })

describe('Broker > offsets keyed by topic id', () => {
  let harness

  afterEach(async () => {
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  const commit = (topics, maxVersion = 10) => {
    harness = brokerAt(apiKeys.OffsetCommit, maxVersion, {
      v9: commitAnswerV9.hex,
      v10: commitAnswer.hex,
    })
    return harness.broker.offsetCommit({ ...commitVector.fields, topics })
  }
  const fetch = (topics, maxVersion = 10) => {
    harness = brokerAt(apiKeys.OffsetFetch, maxVersion, {
      v9: fetchAnswerV9.hex,
      v10: fetchAnswer.hex,
    })
    return harness.broker.offsetFetch({
      groupId: 'test-group',
      topics,
      memberId: 'test-member',
      memberEpoch: 5,
    })
  }
  const versionSent = () => harness.requests.map(({ apiVersion }) => apiVersion)
  const entry = (topicId) => ({ topic: TOPIC, topicId, partitions: PARTITIONS })

  test("commits at v10 when every entry has an id, sending the Apache vector's bytes and naming the answer", async () => {
    const data = await commit([
      { topic: TOPIC, topicId: TOPIC_ID, partitions: commitVector.fields.topics[0].partitions },
    ])

    expect(
      harness.requests.map(({ apiVersion, body }) => [apiVersion, body.toString('hex')])
    ).toEqual([[10, commitVector.hex]])
    expect(data.responses).toEqual([{ topic: TOPIC, partitions: [{ partition: 0, errorCode: 0 }] }])
  })

  test("fetches at v10 when every entry has an id, naming the answer from the request's own map", async () => {
    const data = await fetch([{ topic: TOPIC, topicId: TOPIC_ID, partitions: [{ partition: 0 }] }])

    expect(versionSent()).toEqual([10])
    expect(harness.requests[0].body.toString('hex')).toEqual(fetchVector.hex)
    expect(data.responses.map(({ topic }) => topic)).toEqual([TOPIC])
  })

  test.each([
    ['no id', undefined],
    ['a null id', null],
    ['the all-zero id', ZERO_ID],
  ])('stays at v9 for an entry with %s, though the broker offers v10', async (_, topicId) => {
    await commit([entry(topicId)])
    expect(versionSent()).toEqual([9])
    await harness.pool.destroy()

    await fetch([{ topic: TOPIC, topicId, partitions: [{ partition: 0 }] }])
    expect(versionSent()).toEqual([9])
  })

  test('is all or nothing: one entry without an id keeps the whole request at v9', async () => {
    await commit([entry(TOPIC_ID), { topic: 'other', partitions: PARTITIONS }])

    expect(versionSent()).toEqual([9])
  })

  test.each([[null], [[]]])(
    'fetches every topic of the group (%j) at v9, whatever the broker offers',
    async (topics) => {
      await fetch(topics)

      expect(versionSent()).toEqual([9])
    }
  )

  test('stays at v9 on a 4.0 or 4.1 broker, which does not advertise v10, though every id is known', async () => {
    await commit([entry(TOPIC_ID)], 9)
    expect(versionSent()).toEqual([9])
    await harness.pool.destroy()

    await fetch([{ topic: TOPIC, topicId: TOPIC_ID, partitions: [{ partition: 0 }] }], 9)
    expect(versionSent()).toEqual([9])
  })
})

describe('Consumer > OffsetManager > topic ids', () => {
  const build = ({
    membershipProtocol,
    ids = { [TOPIC]: TOPIC_ID },
    offsetCommit,
    refreshMetadata,
  } = {}) => {
    const coordinator = {
      isConnected: () => true,
      offsetCommit: offsetCommit || jest.fn(async () => {}),
      offsetFetch: jest.fn(async () => ({ responses: [] })),
    }
    const cluster = {
      committedOffsets: () => ({}),
      topicId: jest.fn((topic) => ids[topic]),
      refreshMetadata: refreshMetadata || jest.fn(async () => {}),
      fetchTopicsOffset: jest.fn(async () => []),
    }
    const emitter = new InstrumentationEventEmitter()
    const manager = new OffsetManager({
      cluster,
      coordinator,
      memberAssignment: { [TOPIC]: [0], other: [0] },
      autoCommit: true,
      autoCommitInterval: null,
      autoCommitThreshold: null,
      topicConfigurations: { [TOPIC]: {}, other: {} },
      instrumentationEmitter: emitter,
      groupId: 'group-a',
      generationId: 3,
      memberId: 'member-a',
      membershipProtocol,
      logger: newLogger(),
    })

    return { manager, coordinator, cluster, emitter }
  }
  const topicsOf = (call) => call.topics.map(({ topic, topicId }) => [topic, topicId])
  const commitAll = (manager) =>
    manager.commitOffsets({
      topics: [
        { topic: TOPIC, partitions: PARTITIONS },
        { topic: 'other', partitions: PARTITIONS },
      ],
    })

  test("a consumer-protocol member sends each topic's id with its commit, and the seek that commits as well", async () => {
    const { manager, coordinator } = build({
      membershipProtocol: 'consumer',
      ids: { [TOPIC]: TOPIC_ID, other: OTHER_ID },
    })

    await commitAll(manager)
    await manager.seek({ topic: TOPIC, partition: 0, offset: '3' })

    expect(coordinator.offsetCommit.mock.calls.map(([call]) => topicsOf(call))).toEqual([
      [
        [TOPIC, TOPIC_ID],
        ['other', OTHER_ID],
      ],
      [[TOPIC, TOPIC_ID]],
    ])
  })

  test("a consumer-protocol member sends each topic's id with its offset fetch, and none for a topic it has not seen", async () => {
    const { manager, coordinator } = build({ membershipProtocol: 'consumer' })

    await manager.fetchCommittedOffsets([
      { topic: TOPIC, partitions: [{ partition: 0 }] },
      { topic: 'other', partitions: [{ partition: 0 }] },
    ])

    expect(topicsOf(coordinator.offsetFetch.mock.calls[0][0])).toEqual([
      [TOPIC, TOPIC_ID],
      ['other', undefined],
    ])
  })

  test('a classic member never asks for an id and sends the entries it was given', async () => {
    const { manager, coordinator, cluster } = build({ membershipProtocol: 'classic' })

    await commitAll(manager)
    await manager.fetchCommittedOffsets([{ topic: TOPIC, partitions: [{ partition: 0 }] }])

    expect(cluster.topicId).not.toHaveBeenCalled()
    expect(JSON.stringify(coordinator.offsetCommit.mock.calls[0][0].topics)).not.toContain(
      'topicId'
    )
    expect(JSON.stringify(coordinator.offsetFetch.mock.calls[0][0].topics)).not.toContain('topicId')
  })

  test('the COMMIT_OFFSETS payload a listener receives carries no topic id', async () => {
    const { manager, emitter } = build({ membershipProtocol: 'consumer' })
    const listener = jest.fn()
    emitter.addListener('consumer.commit_offsets', listener)

    await commitAll(manager)

    expect(JSON.stringify(listener.mock.calls[0][0].payload)).not.toContain('topicId')
  })

  describe('UNKNOWN_TOPIC_ID on a commit', () => {
    const refused = () =>
      jest.fn().mockRejectedValueOnce(createErrorFromCode(100)).mockResolvedValue({})

    test('refreshes the metadata, rethrows the retriable error, and the retry carries the new id', async () => {
      const ids = { [TOPIC]: TOPIC_ID, other: OTHER_ID }
      const refreshMetadata = jest.fn(async () => {
        ids[TOPIC] = OTHER_ID
      })
      const { manager, coordinator } = build({
        membershipProtocol: 'consumer',
        ids,
        offsetCommit: refused(),
        refreshMetadata,
      })

      await expect(commitAll(manager)).rejects.toMatchObject({
        type: 'UNKNOWN_TOPIC_ID',
        retriable: true,
      })
      await commitAll(manager)

      expect(refreshMetadata).toHaveBeenCalledTimes(1)
      expect(coordinator.offsetCommit.mock.calls.map(([call]) => topicsOf(call)[0])).toEqual([
        [TOPIC, TOPIC_ID],
        [TOPIC, OTHER_ID],
      ])
    })

    test('rethrows the original error when the refresh itself fails', async () => {
      const refreshMetadata = jest.fn(async () => {
        throw new Error('metadata unavailable')
      })
      const { manager } = build({
        membershipProtocol: 'consumer',
        offsetCommit: refused(),
        refreshMetadata,
      })

      await expect(commitAll(manager)).rejects.toMatchObject({ type: 'UNKNOWN_TOPIC_ID' })
      expect(refreshMetadata).toHaveBeenCalledTimes(1)
    })

    test('does not refresh for another error: UNKNOWN_TOPIC_OR_PARTITION has its own path', async () => {
      const offsetCommit = jest.fn().mockRejectedValue(createErrorFromCode(3))
      const { manager, cluster } = build({ membershipProtocol: 'consumer', offsetCommit })

      await expect(commitAll(manager)).rejects.toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })
      expect(cluster.refreshMetadata).not.toHaveBeenCalled()
    })
  })
})

describe('Admin > offset reads', () => {
  const stubCluster = (coordinator) => ({
    addTargetTopic: async () => {},
    refreshMetadataIfNecessary: async () => {},
    findTopicPartitionMetadata: () => [{ partitionId: 0 }],
    findGroupCoordinator: async () => coordinator,
    topicId: () => TOPIC_ID,
  })

  test('send topic names and never an id, so they stay at v9 or below', async () => {
    const coordinator = { offsetFetch: jest.fn(async () => ({ responses: [] })) }
    const admin = createAdmin({
      logger: newLogger(),
      retry: { retries: 0 },
      cluster: stubCluster(coordinator),
    })

    await admin.fetchOffsets({ groupId: 'g', topics: [TOPIC] })
    await admin.fetchOffsets({ groupId: 'g' })

    expect(coordinator.offsetFetch.mock.calls.map(([call]) => call.topics)).toEqual([
      [{ topic: TOPIC, partitions: [{ partition: 0 }] }],
      [],
    ])
  })
})
