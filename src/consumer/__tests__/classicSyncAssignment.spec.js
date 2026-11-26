const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const joinGroupV8 = require('../../../testHelpers/wire/joinGroupV8')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const ConsumerGroup = require('../consumerGroup')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { MemberAssignment } = require('../assignerProtocol')
const { LEVELS } = require('../../loggers')
const { newLogger } = require('testHelpers')

const GROUP_ID = 'classic-answers-group'
const TOPIC = 'topic-a'
const MEMBER = 'member-1'
const PROTOCOL = 'RoundRobinAssigner'
// SyncGroup up to v3, whose assignment is plain BYTES and can be null on the wire.
const VERSIONS = {
  [apiKeys.JoinGroup]: { minVersion: 0, maxVersion: 8 },
  [apiKeys.SyncGroup]: { minVersion: 0, maxVersion: 3 },
}

/**
 * A successful JoinGroup answer to a follower. It asks for no throttling: the next request would
 * otherwise be written after the harness has answered what was sent.
 * @param {object} [fields] over the follower's
 * @returns {Buffer}
 */
const joinAnswer = (fields = {}) =>
  joinGroupV8.buildResponse({
    throttleTime: 0,
    protocolType: 'consumer',
    groupProtocol: PROTOCOL,
    generationId: 1,
    leaderId: 'leader-1',
    memberId: MEMBER,
    members: [],
    ...fields,
  })

/**
 * A successful SyncGroup v3 answer.
 * @param {Buffer|null} assignment
 * @returns {Buffer}
 */
const syncAnswer = (assignment) =>
  new Encoder().writeInt32(0).writeInt16(0).writeBytes(assignment).buffer

/**
 * Assignment bytes written row by row, so a row can repeat a topic or name none.
 * @param {{ topic: string|null, partitions: number[] }[]} rows
 * @returns {Buffer}
 */
const rowsOf = (rows) =>
  new Encoder()
    .writeInt16(1)
    .writeArray(
      rows.map(({ topic, partitions }) => new Encoder().writeString(topic).writeArray(partitions))
    )
    .writeBytes(Buffer.alloc(0)).buffer

const VALID = MemberAssignment.encode({ version: 1, assignment: { [TOPIC]: [0, 1] } })

describe('Consumer > classic group answers no coordinator sends', () => {
  const harnesses = []

  afterEach(async () => {
    await Promise.all(harnesses.splice(0).map(({ pool }) => pool.destroy()))
  })

  /**
   * A classic consumer group whose coordinator is a real broker over a scripted socket. Each
   * JoinGroup and SyncGroup takes the next answer of its list.
   * @param {{ joins: Buffer[], syncs: Buffer[], retries?: number, logger?: object }} script
   * @returns {{ group: ConsumerGroup, apis: () => number[], joinedAs: () => string[] }} `apis` is
   *   the API key of every request written, `joinedAs` the member id of every JoinGroup
   */
  const exchange = ({ joins, syncs, retries = 1, logger = newLogger() }) => {
    const harness = createGroupApiBroker({
      versions: VERSIONS,
      respond: ({ apiKey }) => (apiKey === apiKeys.JoinGroup ? joins : syncs).shift(),
    })
    harnesses.push(harness)
    const group = new ConsumerGroup({
      retry: { retries },
      cluster: {
        connect: async () => {},
        refreshMetadata: async () => {},
        refreshMetadataIfNecessary: async () => {},
        committedOffsets: () => ({}),
        findTopicPartitionMetadata: () => [],
        findGroupCoordinator: async () => harness.broker,
      },
      groupId: GROUP_ID,
      topics: [TOPIC],
      topicConfigurations: { [TOPIC]: {} },
      logger,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      assigners: [
        { name: PROTOCOL, protocol: () => ({ name: PROTOCOL, metadata: Buffer.alloc(0) }) },
      ],
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
      maxBytesPerPartition: 1048576,
      minBytes: 1,
      maxBytes: 1048576,
      maxWaitTimeInMs: 100,
      autoCommit: false,
      autoCommitInterval: null,
      autoCommitThreshold: null,
      isolationLevel: 0,
      rackId: '',
      checkCrcs: true,
      // Static, so the coordinator answers the first join itself instead of MEMBER_ID_REQUIRED.
      groupInstanceId: 'instance-1',
      metadataMaxAge: 300000,
    })
    return {
      group,
      apis: () => harness.requests.map(({ apiKey }) => apiKey),
      joinedAs: () =>
        harness.requests
          .filter(({ apiKey }) => apiKey === apiKeys.JoinGroup)
          .map(({ body }) => joinGroupV8.readRequest(body).memberId),
    }
  }

  const JOIN = apiKeys.JoinGroup
  const SYNC = apiKeys.SyncGroup

  test('a JoinGroup success with no member id is refused and joined again; the membership is not retired', async () => {
    const { group, apis, joinedAs } = exchange({
      joins: [joinAnswer({ memberId: null }), joinAnswer()],
      syncs: [syncAnswer(VALID)],
    })

    await expect(group.joinAndSync()).resolves.toMatchObject({ assignment: { [TOPIC]: [0, 1] } })

    expect(apis()).toEqual([JOIN, JOIN, SYNC])
    expect(joinedAs()).toEqual(['', ''])
    expect(group.memberId).toBe(MEMBER)
    expect(group.groupMembership.membershipRevision).toBe(0)
  })

  test.each([
    ['a null assignment', null],
    ['a one-byte assignment', Buffer.from([0])],
    ['an assignment that ends inside its topic list', Buffer.from('000100000001', 'hex')],
    ['an assignment with a null topic list', Buffer.from('0001ffffffff00000000', 'hex')],
    ['an assignment row with no topic', rowsOf([{ topic: null, partitions: [0] }])],
    ['an assignment row with an empty topic', rowsOf([{ topic: '', partitions: [0] }])],
    ['an assignment naming partition -1', rowsOf([{ topic: TOPIC, partitions: [-1] }])],
  ])('%s is refused and the member joins again', async (_, bytes) => {
    const { group, apis } = exchange({
      joins: [joinAnswer(), joinAnswer()],
      syncs: [syncAnswer(bytes), syncAnswer(VALID)],
    })

    await expect(group.joinAndSync()).resolves.toMatchObject({ assignment: { [TOPIC]: [0, 1] } })

    expect(apis()).toEqual([JOIN, SYNC, JOIN, SYNC])
  })

  test('an assignment that repeats a topic row or a partition is used with each partition once, first place kept, and one warning names the repeats', async () => {
    const lines = []
    const { group, apis } = exchange({
      joins: [joinAnswer()],
      syncs: [
        syncAnswer(
          rowsOf([
            { topic: TOPIC, partitions: [1, 0] },
            { topic: TOPIC, partitions: [0, 2] },
            { topic: 'topic-b', partitions: [3, 3] },
          ])
        ),
      ],
      logger: newLogger({ level: LEVELS.WARN, logCreator: () => (entry) => lines.push(entry) }),
    })

    const result = await group.joinAndSync()

    expect(result.assignment).toEqual({ [TOPIC]: [1, 0, 2], 'topic-b': [3] })
    expect(apis()).toEqual([JOIN, SYNC])
    const warnings = lines.filter(({ log }) => /repeats/.test(log.message))
    expect(warnings).toHaveLength(1)
    expect(warnings[0]).toMatchObject({
      level: LEVELS.WARN,
      log: {
        groupId: GROUP_ID,
        memberId: MEMBER,
        groupProtocol: PROTOCOL,
        repeated: [
          { topic: TOPIC, partition: 0 },
          { topic: 'topic-b', partition: 3 },
        ],
      },
    })
  })

  test('an assignment refused on every attempt fails the join once the retries run out, naming the refusal', async () => {
    const { group, apis } = exchange({
      joins: [joinAnswer(), joinAnswer()],
      syncs: [syncAnswer(Buffer.from([0])), syncAnswer(Buffer.from([0]))],
    })

    await expect(group.joinAndSync()).rejects.toMatchObject({
      name: 'KafkaJSNumberOfRetriesExceeded',
      type: 'MALFORMED_GROUP_ANSWER',
      message: expect.stringMatching(/^SyncGroup response /),
    })

    expect(apis()).toEqual([JOIN, SYNC, JOIN, SYNC])
  })

  test('a zero-length assignment is applied as nothing assigned', async () => {
    const { group, apis } = exchange({
      joins: [joinAnswer()],
      syncs: [syncAnswer(Buffer.alloc(0))],
    })

    await expect(group.joinAndSync()).resolves.toMatchObject({ assignment: {}, userData: null })

    expect(apis()).toEqual([JOIN, SYNC])
  })

  test('a whole assignment with user data is applied as before', async () => {
    const userData = Buffer.from('user-data')
    const { group, apis } = exchange({
      joins: [joinAnswer()],
      syncs: [
        syncAnswer(
          MemberAssignment.encode({
            version: 3,
            assignment: { [TOPIC]: [2, 0], 'topic-b': [] },
            userData,
          })
        ),
      ],
    })

    const result = await group.joinAndSync()

    expect(result.assignment).toEqual({ [TOPIC]: [2, 0], 'topic-b': [] })
    expect(result.userData).toEqual(userData)
    expect(apis()).toEqual([JOIN, SYNC])
  })
})
