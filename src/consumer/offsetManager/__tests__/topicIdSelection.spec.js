const OffsetManager = require('../index')
const ConsumerGroup = require('../../consumerGroup')
const createAdmin = require('../../../admin')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const Encoder = require('../../../protocol/encoder')
const OffsetFetch = require('../../../protocol/requests/offsetFetch')
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

describe('Consumer > OffsetManager > resolveOffsets across a topic recreation', () => {
  const OLD_ID = TOPIC_ID
  const NEW_ID = OTHER_ID
  const THIRD_ID = '21222324-2526-2728-292a-2b2c2d2e2f30'
  const flush = () => new Promise(setImmediate)

  /** The OffsetFetch v10 answer for partition 0 of one topic id, as the broker puts it on the wire. */
  const wireAnswer = (topicId, offset) =>
    new Encoder()
      .writeInt32(0)
      .writeUVarIntArray([
        new Encoder()
          .writeUVarIntString('test-group')
          .writeUVarIntArray([
            new Encoder()
              .writeUuid(topicId)
              .writeUVarIntArray([
                new Encoder()
                  .writeInt32(0)
                  .writeInt64(offset)
                  .writeInt32(-1)
                  .writeUVarIntString(null)
                  .writeInt16(0)
                  .writeTaggedFields(),
              ])
              .writeTaggedFields(),
          ])
          .writeInt16(0)
          .writeTaggedFields(),
      ])
      .writeTaggedFields().buffer

  /** The OffsetFetch answer for partition 0 of a request sent by topic name, already decoded. */
  const namedAnswer = (offset) => ({
    responses: [
      {
        topic: TOPIC,
        partitions: [
          { partition: 0, offset, committedLeaderEpoch: -1, metadata: null, errorCode: 0 },
        ],
      },
    ],
  })

  /**
   * A consumer-protocol manager over one partition. Its coordinator holds every OffsetFetch until a
   * case answers it with `requests[i].answer(offset)`: through the real v10 codec when the request
   * carried a topic id, as an answer by name when it did not.
   * @param {string|undefined} initialId the topic id the metadata cache holds, or none
   * @param {string} [listOffset] what ListOffsets answers
   * @param {object} [options]
   * @param {boolean} [options.autoCommit] whether a seek commits, as under `autoCommit: true`
   */
  const build = (initialId, listOffset = '0', { autoCommit = false } = {}) => {
    const state = { id: initialId }
    const requests = []
    const pending = new Map()
    const committed = {}
    const cluster = {
      committedOffsets: () => committed,
      topicId: () => state.id,
      fetchTopicsOffset: jest.fn(async () => [
        { topic: TOPIC, partitions: [{ partition: 0, offset: listOffset }] },
      ]),
    }
    const coordinator = {
      isConnected: () => true,
      offsetCommit: jest.fn(async () => ({})),
      offsetFetch: (request) =>
        new Promise((resolve, reject) => {
          const { topicId } = request.topics[0]
          const answer = (offset) => {
            if (topicId === undefined) {
              return resolve(namedAnswer(offset))
            }

            const { response } = OffsetFetch.protocol({ version: 10 })({
              ...request,
              topicIds: new Map([[TOPIC, topicId]]),
            })
            response.decode(wireAnswer(topicId, offset)).then(response.parse).then(resolve, reject)
          }

          requests.push({ topicId, answer })
        }),
    }
    const options = {
      cluster,
      coordinator,
      memberAssignment: { [TOPIC]: [0] },
      autoCommit,
      autoCommitInterval: null,
      autoCommitThreshold: null,
      topicConfigurations: { [TOPIC]: { fromBeginning: true } },
      instrumentationEmitter: new InstrumentationEventEmitter(),
      groupId: 'test-group',
      generationId: 1,
      memberId: 'member-a',
      membershipProtocol: 'consumer',
      pendingTopicResets: pending,
      logger: newLogger(),
    }
    const manager = new OffsetManager(options)
    const rebuild = () => new OffsetManager(options)
    const recreate = (id) => {
      state.id = id
      pending.set(TOPIC, id)
      manager.resetOffsetsForTopic(TOPIC)
    }

    const setId = (id) => {
      state.id = id
    }

    return { manager, rebuild, requests, pending, committed, cluster, recreate, setId }
  }

  /** Resolves again and answers any OffsetFetch it sends with the old committed offset. */
  const resolveAgain = async (manager, requests) => {
    const sent = requests.length
    const round = manager.resolveOffsets()
    await flush()
    requests.slice(sent).forEach(({ answer }) => answer('99'))
    await round
  }

  /**
   * Holds every ListOffsets call until `release(offset)` answers them, and answers the calls that
   * come after that at once. `answerAt(index, offset)` answers one held call and leaves the rest.
   */
  const holdListOffsets = (cluster) => {
    const held = []
    let released
    cluster.fetchTopicsOffset.mockImplementation(
      () =>
        new Promise((resolve) => {
          const answer = (offset) =>
            resolve([{ topic: TOPIC, partitions: [{ partition: 0, offset }] }])
          if (released === undefined) {
            held.push(answer)
          } else {
            answer(released)
          }
        })
    )

    return {
      count: () => held.length,
      answerAt: (index, offset) => held[index](offset),
      release: (offset) => {
        released = offset
        held.forEach((answer) => answer(offset))
      },
    }
  }

  test('an OffsetFetch answer for the old incarnation, landing after the new one resolved, does not move its position', async () => {
    const { manager, requests, pending, committed, recreate } = build(OLD_ID)
    const stale = manager.resolveOffsets()
    await flush()

    recreate(NEW_ID)
    const fresh = manager.resolveOffsets()
    await flush()
    expect(requests.map(({ topicId }) => topicId)).toEqual([OLD_ID, NEW_ID])
    requests[1].answer('77')
    await fresh
    expect(committed[TOPIC]).toEqual({ 0: '0' })
    expect(pending.size).toBe(0)

    requests[0].answer('99')
    await stale

    expect(committed[TOPIC]).toEqual({ 0: '0' })
    expect(manager.nextOffset(TOPIC, 0).toString()).toEqual('0')
  })

  test('an OffsetFetch answer for the old incarnation does not consume the reset of the new one, which the next resolution applies', async () => {
    const { manager, requests, pending, committed, cluster, recreate } = build(OLD_ID)
    const stale = manager.resolveOffsets()
    await flush()
    recreate(NEW_ID)
    requests[0].answer('99')
    await stale

    expect(committed[TOPIC]).toEqual({})
    expect(pending.get(TOPIC)).toEqual(NEW_ID)
    expect(cluster.fetchTopicsOffset).not.toHaveBeenCalled()

    const next = manager.resolveOffsets()
    await flush()
    requests[1].answer('99')
    await next

    expect(committed[TOPIC]).toEqual({ 0: '0' })
    expect(pending.size).toBe(0)
  })

  test('a ListOffsets answer for the old incarnation, landing after the recreation, is not applied', async () => {
    const { manager, requests, pending, committed, cluster, recreate } = build(OLD_ID)
    let releaseListOffsets
    cluster.fetchTopicsOffset.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          releaseListOffsets = () =>
            resolve([{ topic: TOPIC, partitions: [{ partition: 0, offset: '500' }] }])
        })
    )
    const stale = manager.resolveOffsets()
    await flush()
    requests[0].answer('-1')
    await flush()
    expect(cluster.fetchTopicsOffset).toHaveBeenCalledTimes(1)

    recreate(NEW_ID)
    releaseListOffsets()
    await stale

    expect(committed[TOPIC]).toEqual({})
    expect(pending.get(TOPIC)).toEqual(NEW_ID)

    const next = manager.resolveOffsets()
    await flush()
    requests[1].answer('99')
    await next

    expect(committed[TOPIC]).toEqual({ 0: '0' })
    expect(pending.size).toBe(0)
  })

  describe('an id learned or lost during ListOffsets does not discard a reset', () => {
    test('an id learned while ListOffsets is in flight does not discard the reset', async () => {
      const { manager, requests, pending, committed, cluster, setId } = build(undefined)
      pending.set(TOPIC, NEW_ID)
      const held = holdListOffsets(cluster)
      const round = manager.resolveOffsets()
      await flush()
      requests[0].answer('99')
      await flush()
      expect(held.count()).toBe(1)

      setId(NEW_ID)
      held.release('0')
      await round
      expect(committed[TOPIC]).toEqual({ 0: '0' })
      await resolveAgain(manager, requests)

      expect(manager.nextOffset(TOPIC, 0).toString()).toEqual('0')
      expect(pending.size).toBe(0)
    })

    test('a cache cleared during ListOffsets and repopulated with the same id does not discard the reset', async () => {
      const { manager, requests, pending, committed, cluster, setId } = build(NEW_ID)
      pending.set(TOPIC, NEW_ID)
      const held = holdListOffsets(cluster)
      const round = manager.resolveOffsets()
      await flush()
      requests[0].answer('99')
      await flush()
      expect(held.count()).toBe(1)

      setId(undefined)
      held.release('0')
      await round
      expect(committed[TOPIC]).toEqual({ 0: '0' })
      setId(NEW_ID)
      await resolveAgain(manager, requests)

      expect(manager.nextOffset(TOPIC, 0).toString()).toEqual('0')
      expect(pending.size).toBe(0)
    })
  })

  describe('the recreation signal fences what a resolution writes', () => {
    test('a recreation during ListOffsets, while the id was unknown, drops the old answer and the next resolution applies the new reset', async () => {
      const { manager, requests, pending, committed, cluster, setId, recreate } = build(undefined)
      pending.set(TOPIC, OLD_ID)
      let releaseOld
      cluster.fetchTopicsOffset.mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            releaseOld = () =>
              resolve([{ topic: TOPIC, partitions: [{ partition: 0, offset: '500' }] }])
          })
      )
      const stale = manager.resolveOffsets()
      await flush()
      requests[0].answer('99')
      await flush()
      expect(cluster.fetchTopicsOffset).toHaveBeenCalledTimes(1)

      setId(OLD_ID)
      recreate(NEW_ID)
      releaseOld()
      await stale

      expect(committed[TOPIC]).toEqual({})
      expect(pending.get(TOPIC)).toEqual(NEW_ID)

      await resolveAgain(manager, requests)

      expect(manager.nextOffset(TOPIC, 0).toString()).toEqual('0')
      expect(pending.size).toBe(0)
    })

    test('a recreation during OffsetFetch, while the id was unknown, drops the answer and the next resolution applies the new reset', async () => {
      const { manager, requests, pending, committed, cluster, recreate } = build(undefined)
      const stale = manager.resolveOffsets()
      await flush()

      recreate(NEW_ID)
      requests[0].answer('99')
      await stale

      expect(committed[TOPIC]).toEqual({})
      expect(cluster.fetchTopicsOffset).not.toHaveBeenCalled()
      expect(pending.get(TOPIC)).toEqual(NEW_ID)

      await resolveAgain(manager, requests)

      expect(committed[TOPIC]).toEqual({ 0: '0' })
      expect(pending.size).toBe(0)
    })

    test('a seek that lands between two resolvers of one incarnation keeps its position, with no second reset', async () => {
      const { manager, requests, pending, committed, cluster, recreate } = build(OLD_ID, '0', {
        autoCommit: true,
      })
      recreate(NEW_ID)
      const held = holdListOffsets(cluster)
      const first = manager.resolveOffsets()
      await flush()
      const second = manager.resolveOffsets()
      await flush()
      requests[0].answer('99')
      await flush()
      requests[1].answer('99')
      await flush()

      held.answerAt(0, '0')
      await first
      await manager.seek({ topic: TOPIC, partition: 0, offset: '500' })
      held.release('0')
      await second

      const sent = requests.length
      const seeked = new Set([OffsetManager.seekedPartitionKey(TOPIC, 0)])
      const resolving = manager.resolveOffsets(seeked)
      await flush()
      requests.slice(sent).forEach(({ answer }) => answer('500'))
      await resolving

      expect(committed[TOPIC]).toEqual({ 0: '500' })
      expect(manager.nextOffset(TOPIC, 0).toString()).toEqual('500')
      expect(cluster.fetchTopicsOffset).toHaveBeenCalledTimes(1)
      expect(pending.size).toBe(0)
    })

    test('a commit made after a failed ListOffsets survives a rejoin: no token lingers to rewind it', async () => {
      const { manager, rebuild, requests, pending, cluster, recreate } = build(OLD_ID)
      recreate(NEW_ID)
      cluster.fetchTopicsOffset.mockRejectedValueOnce(new Error('list offsets unavailable'))
      const failed = manager.resolveOffsets()
      await flush()
      requests[0].answer('99')
      await expect(failed).rejects.toThrow('list offsets unavailable')

      await manager.commitOffsets({
        topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '50' }] }],
      })
      await manager.resolveOffsets()
      expect(pending.size).toBe(0)

      const rejoined = rebuild()
      const sent = requests.length
      const round = rejoined.resolveOffsets()
      await flush()
      requests.slice(sent).forEach(({ answer }) => answer('50'))
      await round

      expect(rejoined.nextOffset(TOPIC, 0).toString()).toEqual('50')
      expect(pending.size).toBe(0)
    })
  })

  describe('a resolution that consumed the reset fences the answers still in flight', () => {
    /**
     * Resolution `slow` is held in OffsetFetch while the topic is recreated and a sibling resolution
     * runs to the end, consuming the reset, before `slow`'s answer for the old incarnation lands.
     * @param {string|undefined} initialId the topic id the cache holds when `slow` starts
     * @param {(context: object) => void} [afterSibling] runs once the sibling is done
     */
    const raceSibling = async (initialId, afterSibling = () => {}) => {
      const context = build(initialId)
      const { manager, requests, recreate } = context
      const slow = manager.resolveOffsets()
      await flush()

      recreate(NEW_ID)
      const fast = manager.resolveOffsets()
      await flush()
      requests[1].answer('99')
      await fast
      afterSibling(context)

      requests[0].answer('99')
      await slow
      return context
    }

    test('an answer sent while the id was unknown does not overwrite the position the sibling reset', async () => {
      const { manager, committed, pending } = await raceSibling(undefined)

      expect(pending.size).toBe(0)
      expect(committed[TOPIC]).toEqual({ 0: '0' })
      expect(manager.nextOffset(TOPIC, 0).toString()).toEqual('0')
    })

    test('a later resolution has nothing left to repair, and the position stays at the reset', async () => {
      const { manager, requests, committed } = await raceSibling(undefined)

      await resolveAgain(manager, requests)

      expect(committed[TOPIC]).toEqual({ 0: '0' })
      expect(manager.nextOffset(TOPIC, 0).toString()).toEqual('0')
    })

    test('an answer sent under a known id does not overwrite it either, once the cache is cleared', async () => {
      const { manager, committed } = await raceSibling(OLD_ID, ({ setId }) => setId(undefined))

      expect(committed[TOPIC]).toEqual({ 0: '0' })
      expect(manager.nextOffset(TOPIC, 0).toString()).toEqual('0')
    })

    test('a slow answer sent after both resolutions saw the new id and its token is dropped once the token is consumed', async () => {
      const { manager, requests, committed, recreate } = build(OLD_ID)
      recreate(NEW_ID)
      const slow = manager.resolveOffsets()
      await flush()
      const fast = manager.resolveOffsets()
      await flush()
      requests[1].answer('99')
      await fast

      requests[0].answer('99')
      await slow

      expect(committed[TOPIC]).toEqual({ 0: '0' })
      expect(manager.nextOffset(TOPIC, 0).toString()).toEqual('0')
    })

    test('a token the sibling consumed without a reset, because the cached id is another one, still fences', async () => {
      const { manager, requests, pending, committed, setId } = build(undefined)
      const slow = manager.resolveOffsets()
      await flush()

      pending.set(TOPIC, NEW_ID)
      setId(THIRD_ID)
      manager.resetOffsetsForTopic(TOPIC)
      const fast = manager.resolveOffsets()
      await flush()
      requests[1].answer('99')
      await fast
      expect(pending.size).toBe(0)

      requests[0].answer('77')
      await slow

      expect(committed[TOPIC]).toEqual({ 0: '99' })
    })

    test('a ListOffsets answer is dropped when a sibling consumed a newer reset while it was in flight', async () => {
      const { manager, requests, pending, committed, cluster } = build(undefined)
      const held = holdListOffsets(cluster)
      pending.set(TOPIC, OLD_ID)
      const slow = manager.resolveOffsets()
      await flush()
      requests[0].answer('99')
      await flush()
      expect(held.count()).toBe(1)

      pending.set(TOPIC, NEW_ID)
      manager.resetOffsetsForTopic(TOPIC)
      const fast = manager.resolveOffsets()
      await flush()
      requests[1].answer('99')
      await flush()
      held.answerAt(1, '0')
      await fast
      expect(committed[TOPIC]).toEqual({ 0: '0' })

      held.answerAt(0, '500')
      await slow

      expect(committed[TOPIC]).toEqual({ 0: '0' })
      expect(pending.size).toBe(0)
    })
  })

  describe.each([
    ['a known topic id', OLD_ID],
    ['no topic id', undefined],
  ])('with %s, unchanged across the awaits', (_, id) => {
    test('applies the committed offset', async () => {
      const { manager, requests, committed } = build(id)
      const round = manager.resolveOffsets()
      await flush()

      requests[0].answer('99')
      await round

      expect(committed[TOPIC]).toEqual({ 0: '99' })
    })

    test('applies the ListOffsets answer when nothing is committed', async () => {
      const { manager, requests, committed, cluster } = build(id, '5')
      const round = manager.resolveOffsets()
      await flush()

      requests[0].answer('-1')
      await round

      expect(cluster.fetchTopicsOffset).toHaveBeenCalledTimes(1)
      expect(committed[TOPIC]).toEqual({ 0: '5' })
    })

    test('still applies a pending reset', async () => {
      const { manager, requests, pending, committed } = build(id, '5')
      pending.set(TOPIC, OLD_ID)
      const round = manager.resolveOffsets()
      await flush()

      requests[0].answer('99')
      await round

      expect(committed[TOPIC]).toEqual({ 0: '5' })
      expect(pending.size).toBe(0)
    })
  })
})

describe('ConsumerGroup#fetch > a topic id learned during the first OffsetFetch', () => {
  afterEach(() => {
    jest.useRealTimers()
  })

  test('the first assignment is resolved and fetched at once, with no empty-fetch wait', async () => {
    jest.useFakeTimers()
    const state = { id: undefined }
    const brokerFetch = jest.fn(async () => ({ errorCode: 0, responses: [] }))
    const cluster = {
      refreshMetadataIfNecessary: async () => {},
      committedOffsets: () => committed,
      topicId: () => state.id,
      leaderEpoch: () => -1,
      hasNode: () => true,
      findTopicPartitionMetadata: () => [{ partitionId: 0, leader: 0 }],
      findBroker: async () => ({ fetch: brokerFetch }),
      fetchTopicsOffset: async () => [
        { topic: TOPIC, partitions: [{ partition: 0, offset: '5' }] },
      ],
    }
    const committed = {}
    const topicConfigurations = { [TOPIC]: {} }
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const group = new ConsumerGroup({
      retry: { retries: 0 },
      cluster,
      groupId: 'test-group',
      topics: [TOPIC],
      topicConfigurations,
      logger: newLogger(),
      instrumentationEmitter,
      assigners: [],
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
      maxBytesPerPartition: 1048576,
      minBytes: 1,
      maxBytes: 1048576,
      maxWaitTimeInMs: 5000,
      autoCommit: false,
      autoCommitInterval: null,
      autoCommitThreshold: null,
      isolationLevel: 0,
      rackId: '',
      checkCrcs: true,
      metadataMaxAge: 300000,
    })
    group.subscriptionState.assign([{ topic: TOPIC, partitions: [0] }])
    group.offsetManager = new OffsetManager({
      cluster,
      coordinator: {
        isConnected: () => true,
        offsetFetch: async () => {
          state.id = TOPIC_ID
          return {
            responses: [
              {
                topic: TOPIC,
                partitions: [
                  {
                    partition: 0,
                    offset: '-1',
                    committedLeaderEpoch: -1,
                    metadata: null,
                    errorCode: 0,
                  },
                ],
              },
            ],
          }
        },
      },
      memberAssignment: { [TOPIC]: [0] },
      autoCommit: false,
      topicConfigurations,
      instrumentationEmitter,
      groupId: 'test-group',
      generationId: 1,
      memberId: 'member-a',
      logger: newLogger(),
    })

    const round = group.fetch(0)
    await jest.advanceTimersByTimeAsync(0)

    expect(brokerFetch).toHaveBeenCalledTimes(1)
    expect(brokerFetch.mock.calls[0][0].topics).toEqual([
      expect.objectContaining({
        topic: TOPIC,
        partitions: [expect.objectContaining({ fetchOffset: '5' })],
      }),
    ])
    await expect(round).resolves.toEqual([])
  })
})
