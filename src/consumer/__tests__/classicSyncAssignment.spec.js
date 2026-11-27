const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const joinGroupV8 = require('../../../testHelpers/wire/joinGroupV8')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const createConsumer = require('../index')
const ConsumerGroup = require('../consumerGroup')
const OffsetOperations = require('../offsetManager/offsetOperations')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { MemberAssignment } = require('../assignerProtocol')
const { assertJoinAnswer } = require('../../protocol/requests/groupProtocolCheck')
const { createErrorFromCode } = require('../../protocol/error')
const { KafkaJSPartialMessageError, KafkaJSProtocolError } = require('../../errors')
const { LEVELS } = require('../../loggers')
const { newLogger } = require('testHelpers')

const GROUP_ID = 'classic-answers-group'
const TOPIC = 'topic-a'
const MEMBER = 'member-1'
const PROTOCOL = 'RoundRobinAssigner'

/**
 * The versions the harness negotiates against. Up to v3 the SyncGroup assignment is plain BYTES;
 * from v4 it is COMPACT_BYTES, and v5 adds the protocol fields.
 * @param {number} syncMaxVersion
 * @returns {object}
 */
const versionsUpTo = (syncMaxVersion) => ({
  [apiKeys.JoinGroup]: { minVersion: 0, maxVersion: 8 },
  [apiKeys.SyncGroup]: { minVersion: 0, maxVersion: syncMaxVersion },
})

/**
 * @param {object[]} lines where each warning entry is pushed
 * @returns {object} a logger that keeps its warnings
 */
const warnLogger = (lines) =>
  newLogger({ level: LEVELS.WARN, logCreator: () => (entry) => lines.push(entry) })

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
 * A successful SyncGroup answer, with no throttling. v5 reports the protocol this member sent.
 * @param {Buffer|null} assignment null writes the null of the version's bytes type
 * @param {number} [version]
 * @returns {Buffer}
 */
const syncAnswer = (assignment, version = 3) => {
  const encoder = new Encoder().writeInt32(0).writeInt16(0)
  if (version < 4) return encoder.writeBytes(assignment).buffer
  if (version >= 5) encoder.writeUVarIntString('consumer').writeUVarIntString(PROTOCOL)
  return encoder.writeUVarIntBytes(assignment).writeTaggedFields().buffer
}

/**
 * Assignment bytes written row by row, so a row can repeat a topic, name none, or carry the null
 * partition list.
 * @param {{ topic: string|null, partitions: number[]|null }[]} rows
 * @returns {Buffer}
 */
const rowsOf = (rows) =>
  new Encoder()
    .writeInt16(1)
    .writeArray(
      rows.map(({ topic, partitions }) =>
        partitions === null
          ? new Encoder().writeString(topic).writeInt32(-1)
          : new Encoder().writeString(topic).writeArray(partitions)
      )
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
   * @param {{ joins: Buffer[], syncs: Buffer[], retries?: number, maxTotalTime?: number,
   *   logger?: object, syncMaxVersion?: number }} script
   * @returns {{ group: ConsumerGroup, apis: () => number[], joinedAs: () => string[],
   *   syncVersions: () => number[] }} `apis` is the API key of every request written, `joinedAs`
   *   the member id of every JoinGroup, `syncVersions` the version of every SyncGroup
   */
  const exchange = ({
    joins,
    syncs,
    retries = 1,
    maxTotalTime,
    logger = newLogger(),
    syncMaxVersion = 3,
  }) => {
    const harness = createGroupApiBroker({
      versions: versionsUpTo(syncMaxVersion),
      respond: ({ apiKey }) => (apiKey === apiKeys.JoinGroup ? joins : syncs).shift(),
    })
    harnesses.push(harness)
    const group = new ConsumerGroup({
      retry: { retries, ...(maxTotalTime === undefined ? {} : { maxTotalTime }) },
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
      syncVersions: () =>
        harness.requests
          .filter(({ apiKey }) => apiKey === apiKeys.SyncGroup)
          .map(({ apiVersion }) => apiVersion),
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
    ['an assignment row with a null partition list', rowsOf([{ topic: TOPIC, partitions: null }])],
  ])('%s is refused and the member joins again', async (_, bytes) => {
    const { group, apis, joinedAs } = exchange({
      joins: [joinAnswer(), joinAnswer()],
      syncs: [syncAnswer(bytes), syncAnswer(VALID)],
    })

    await expect(group.joinAndSync()).resolves.toMatchObject({ assignment: { [TOPIC]: [0, 1] } })

    expect(apis()).toEqual([JOIN, SYNC, JOIN, SYNC])
    // The refused SyncGroup came after a JoinGroup that gave the member its id.
    expect(joinedAs()).toEqual(['', MEMBER])
  })

  test.each([
    ['v5', 5, 'consumer'],
    ['v4', 4, null],
  ])(
    'a compact null assignment in a SyncGroup %s answer is refused with a warning and the member joins again',
    async (_, version, protocolType) => {
      const lines = []
      const { group, apis, syncVersions } = exchange({
        syncMaxVersion: 5,
        // A join answer without a protocol type caps the SyncGroup at v4.
        joins: [joinAnswer({ protocolType }), joinAnswer({ protocolType })],
        syncs: [syncAnswer(null, version), syncAnswer(VALID, version)],
        logger: warnLogger(lines),
      })

      await expect(group.joinAndSync()).resolves.toMatchObject({
        assignment: { [TOPIC]: [0, 1] },
      })

      expect(apis()).toEqual([JOIN, SYNC, JOIN, SYNC])
      expect(syncVersions()).toEqual([version, version])
      const refused = lines.filter(({ log }) => log.message.startsWith('Refused a group answer'))
      expect(refused).toHaveLength(1)
      expect(refused[0].log).toMatchObject({
        groupId: GROUP_ID,
        memberId: MEMBER,
        error: 'SyncGroup response carries no assignment',
      })
    }
  )

  test.each([
    ['v5', 5, 'consumer'],
    ['v4', 4, null],
  ])(
    'a zero-length assignment in a SyncGroup %s answer is applied as nothing assigned',
    async (_, version, protocolType) => {
      const { group, apis, syncVersions } = exchange({
        syncMaxVersion: 5,
        joins: [joinAnswer({ protocolType })],
        syncs: [syncAnswer(Buffer.alloc(0), version)],
      })

      await expect(group.joinAndSync()).resolves.toMatchObject({ assignment: {}, userData: null })

      expect(apis()).toEqual([JOIN, SYNC])
      expect(syncVersions()).toEqual([version])
    }
  )

  test('an assignment row with an empty partition list is applied with no partitions of that topic', async () => {
    const { group, apis } = exchange({
      joins: [joinAnswer()],
      syncs: [
        syncAnswer(
          rowsOf([
            { topic: TOPIC, partitions: [] },
            { topic: 'topic-b', partitions: [1] },
          ])
        ),
      ],
    })

    const result = await group.joinAndSync()

    expect(result.assignment).toEqual({ [TOPIC]: [], 'topic-b': [1] })
    expect(apis()).toEqual([JOIN, SYNC])
  })

  test.each([
    ['disjoint', [0], [1]],
    ['empty', [0, 1], []],
  ])(
    'a topic named in two rows with %s partitions is used with each partition once, and one warning names the topic',
    async (_, first, second) => {
      const lines = []
      const { group, apis } = exchange({
        joins: [joinAnswer()],
        syncs: [
          syncAnswer(
            rowsOf([
              { topic: TOPIC, partitions: first },
              { topic: 'topic-b', partitions: [2] },
              { topic: TOPIC, partitions: second },
            ])
          ),
        ],
        logger: warnLogger(lines),
      })

      const result = await group.joinAndSync()

      expect(result.assignment).toEqual({ [TOPIC]: [0, 1], 'topic-b': [2] })
      expect(apis()).toEqual([JOIN, SYNC])
      const warnings = lines.filter(({ log }) => /repeats/.test(log.message))
      expect(warnings).toHaveLength(1)
      expect(warnings[0].log).toMatchObject({
        groupId: GROUP_ID,
        memberId: MEMBER,
        groupProtocol: PROTOCOL,
        repeatedTopics: [TOPIC],
        repeated: [],
      })
    }
  )

  test('a partition named twice in one row is used once, and one warning names it and no repeated topic', async () => {
    const lines = []
    const { group, apis } = exchange({
      joins: [joinAnswer()],
      syncs: [syncAnswer(rowsOf([{ topic: TOPIC, partitions: [0, 1, 0] }]))],
      logger: warnLogger(lines),
    })

    const result = await group.joinAndSync()

    expect(result.assignment).toEqual({ [TOPIC]: [0, 1] })
    expect(apis()).toEqual([JOIN, SYNC])
    const warnings = lines.filter(({ log }) => /repeats/.test(log.message))
    expect(warnings).toHaveLength(1)
    expect(warnings[0].log).toMatchObject({
      repeatedTopics: [],
      repeated: [{ topic: TOPIC, partition: 0 }],
    })
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
        repeatedTopics: [TOPIC],
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

    const failure = await group.joinAndSync().catch((error) => error)

    expect(failure).toBeInstanceOf(KafkaJSPartialMessageError)
    expect(failure).toMatchObject({ retriable: false, cause: undefined })
    expect(failure.message).toBe(
      `Group ${GROUP_ID} member ${MEMBER}: the join retries ran out on an answer no coordinator sends: SyncGroup response carries an assignment shorter than its version`
    )
    expect(apis()).toEqual([JOIN, SYNC, JOIN, SYNC])
  })

  test('an assignment refused until retry.maxTotalTime would pass fails the join the same way', async () => {
    const { group, apis } = exchange({
      // The first backoff is longer than 1 ms, so the deadline ends the join after one attempt.
      retries: 5,
      maxTotalTime: 1,
      joins: [joinAnswer()],
      syncs: [syncAnswer(Buffer.from([0]))],
    })

    const failure = await group.joinAndSync().catch((error) => error)

    expect(failure).toBeInstanceOf(KafkaJSPartialMessageError)
    expect(failure).toMatchObject({ retriable: false, cause: undefined })
    expect(failure.message).toBe(
      `Group ${GROUP_ID} member ${MEMBER}: the join retries ran out on an answer no coordinator sends: SyncGroup response carries an assignment shorter than its version`
    )
    expect(apis()).toEqual([JOIN, SYNC])
  })

  test('an assignment of a later version with bytes after its user data is applied from the fields this member reads', async () => {
    const userData = Buffer.from('user-data')
    const { group, apis } = exchange({
      joins: [joinAnswer()],
      syncs: [
        syncAnswer(
          Buffer.concat([
            MemberAssignment.encode({ version: 9, assignment: { [TOPIC]: [1] }, userData }),
            Buffer.from('0102', 'hex'),
          ])
        ),
      ],
    })

    const result = await group.joinAndSync()

    expect(result.assignment).toEqual({ [TOPIC]: [1] })
    expect(result.userData).toEqual(userData)
    expect(apis()).toEqual([JOIN, SYNC])
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

describe('Consumer > classic group answers refused until the join retries run out', () => {
  const HEALTHY = 'member-healthy'
  const REFUSED = 'member-refused'
  const RESTART_DELAY = 300
  const REBALANCE_IN_PROGRESS = 27
  const consumers = []

  beforeEach(() => jest.useFakeTimers())
  afterEach(async () => {
    const stopping = Promise.all(consumers.splice(0).map((c) => c.disconnect().catch(() => {})))
    await jest.advanceTimersByTimeAsync(1000)
    await stopping
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  /** @param {number[]} partitions @returns {Buffer} an assignment of TOPIC */
  const assignmentOf = (partitions) =>
    MemberAssignment.encode({ version: 0, assignment: { [TOPIC]: partitions } })

  /**
   * One classic group behind a coordinator double its members share. A JoinGroup with an empty
   * member id is a new entry, and every other member is asked to rejoin through its next
   * heartbeat, as a coordinator rebalances a group a member enters.
   * @returns {{ members: string[], rejoin: Set<string>, fetches: string[] }} `fetches` names the
   *   member of every partition fetched
   */
  const createGroup = () => ({ members: [], rejoin: new Set(), fetches: [] })

  /**
   * A real classic consumer of `group`. Its JoinGroup answers pass the Broker's own check, so an
   * answer it refuses is refused as on the wire, and every SyncGroup answers `assignment`.
   * @param {object} group from `createGroup`
   * @param {string} memberId the id the coordinator gives this member
   * @param {{ assignment: Buffer, join?: object, retry?: object, groupInstanceId?: string|null }} options
   *   `join` overrides fields of the JoinGroup answer
   * @returns {{ consumer: object, coordinator: object, crashes: object[] }}
   */
  const createMember = (
    group,
    memberId,
    { assignment, join = {}, retry = {}, groupInstanceId }
  ) => {
    const crashes = []
    let generation = 0
    const coordinator = {
      isConnected: () => true,
      nodeId: 0,
      negotiatedVersion: jest.fn(() => null),
      offsetCommit: jest.fn(async () => {}),
      offsetFetch: jest.fn(async ({ topics }) => ({
        responses: topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({
            partition,
            offset: '5',
            metadata: null,
            errorCode: 0,
          })),
        })),
      })),
      joinGroup: jest.fn(async (request) => {
        if (request.memberId === '') {
          for (const other of group.members) if (other !== memberId) group.rejoin.add(other)
        }
        const answer = {
          generationId: ++generation,
          leaderId: 'someone-else',
          memberId,
          members: [],
          groupProtocol: PROTOCOL,
          protocolType: 'consumer',
          ...join,
        }
        assertJoinAnswer(answer, request.groupProtocols)
        return answer
      }),
      syncGroup: jest.fn(async () => ({ memberAssignment: assignment })),
      heartbeat: jest.fn(async () => {
        if (group.rejoin.delete(memberId)) {
          throw new KafkaJSProtocolError(createErrorFromCode(REBALANCE_IN_PROGRESS))
        }
      }),
      leaveGroup: jest.fn(async () => {}),
    }
    const broker = {
      fetch: jest.fn(async ({ topics }) => {
        for (const { partitions } of topics) group.fetches.push(...partitions.map(() => memberId))
        await new Promise((resolve) => setTimeout(resolve, 50))
        return { responses: [] }
      }),
    }
    const cluster = {
      instrumentationEmitter: new InstrumentationEventEmitter(),
      connect: jest.fn(async () => {}),
      disconnect: jest.fn(async () => {}),
      isConnected: () => true,
      removeBroker: jest.fn(),
      onTopicRecreated: jest.fn(() => () => {}),
      addMultipleTargetTopics: jest.fn(async () => {}),
      refreshMetadata: jest.fn(async () => {}),
      refreshMetadataIfNecessary: jest.fn(async () => {}),
      committedOffsets: () => ({}),
      offsetOperations: () => new OffsetOperations(),
      defaultOffset: () => -2,
      // A topic without an id is never fetched.
      topicId: () => '01020304-0506-0708-090a-0b0c0d0e0f10',
      leaderEpoch: () => -1,
      getNodeIds: () => [0],
      findTopicPartitionMetadata: () => [0, 1].map((partitionId) => ({ partitionId, leader: 0 })),
      findBroker: jest.fn(async () => broker),
      findGroupCoordinator: jest.fn(async () => coordinator),
      fetchTopicsOffset: jest.fn(async (requests) =>
        requests.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({ partition, offset: '0' })),
        }))
      ),
      retry: {},
    }
    const consumer = createConsumer({
      cluster,
      groupId: GROUP_ID,
      logger: newLogger(),
      instrumentationEmitter: cluster.instrumentationEmitter,
      partitionAssigners: [
        () => ({
          name: PROTOCOL,
          version: 0,
          protocol: () => ({ name: PROTOCOL, metadata: Buffer.alloc(0) }),
          assign: async () => [],
        }),
      ],
      sessionTimeout: 30000,
      heartbeatInterval: 100,
      retry: { initialRetryTime: RESTART_DELAY, retries: 2, ...retry },
      ...(groupInstanceId ? { groupInstanceId } : {}),
    })
    consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload))
    group.members.push(memberId)
    consumers.push(consumer)
    return { consumer, coordinator, crashes }
  }

  /** @param {{ consumer: object }} member @returns {Promise<void>} */
  const start = async ({ consumer }) => {
    await consumer.subscribe({ topic: TOPIC })
    await consumer.run({ eachMessage: async () => {} })
  }

  /**
   * Advances fake time in 10 ms steps until `done()` holds, for at most `budget` ms.
   * @param {() => boolean} done
   * @param {number} [budget]
   * @returns {Promise<void>}
   */
  const until = async (done, budget = 2000) => {
    for (let spent = 0; !done() && spent < budget; spent += 10) {
      await jest.advanceTimersByTimeAsync(10)
    }
  }

  /** @param {object} group @param {string} memberId @returns {number} */
  const fetchesOf = (group, memberId) => group.fetches.filter((id) => id === memberId).length

  test.each([
    ['dynamic', null],
    ['static', 'instance-refused'],
  ])(
    'a %s member whose assignment is refused on every attempt crashes once with restart false and joins no more, while a healthy member keeps fetching',
    async (_, groupInstanceId) => {
      const group = createGroup()
      const healthy = createMember(group, HEALTHY, { assignment: assignmentOf([0]) })
      const refused = createMember(group, REFUSED, {
        assignment: Buffer.from([0]),
        groupInstanceId,
      })
      await start(healthy)
      await until(() => fetchesOf(group, HEALTHY) > 0)
      await start(refused)
      await until(() => refused.crashes.length > 0, 10000)

      // retries: 2 is three attempts, each a JoinGroup and a refused SyncGroup.
      expect(refused.coordinator.joinGroup).toHaveBeenCalledTimes(3)
      expect(refused.coordinator.syncGroup).toHaveBeenCalledTimes(3)
      expect(refused.crashes).toHaveLength(1)
      const [{ error, groupId, restart }] = refused.crashes
      expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
      expect({ groupId, restart, retriable: error.retriable }).toEqual({
        groupId: GROUP_ID,
        restart: false,
        retriable: false,
      })
      expect(error.message).toBe(
        `Group ${GROUP_ID} member ${REFUSED}: the join retries ran out on an answer no coordinator sends: SyncGroup response carries an assignment shorter than its version`
      )

      await jest.advanceTimersByTimeAsync(2000)
      const healthyJoins = healthy.coordinator.joinGroup.mock.calls.length
      const healthyFetches = fetchesOf(group, HEALTHY)
      await jest.advanceTimersByTimeAsync(RESTART_DELAY * 10)

      expect(refused.coordinator.joinGroup).toHaveBeenCalledTimes(3)
      expect(refused.crashes).toHaveLength(1)
      expect(healthy.coordinator.joinGroup).toHaveBeenCalledTimes(healthyJoins)
      expect(healthy.crashes).toEqual([])
      expect(fetchesOf(group, HEALTHY)).toBeGreaterThan(healthyFetches)
    }
  )

  test('a JoinGroup answer refused on every attempt crashes the member once with restart false, before any SyncGroup', async () => {
    const group = createGroup()
    const refused = createMember(group, REFUSED, {
      assignment: assignmentOf([0]),
      join: { memberId: '' },
    })
    await start(refused)
    await until(() => refused.crashes.length > 0, 10000)

    expect(refused.coordinator.joinGroup).toHaveBeenCalledTimes(3)
    expect(refused.coordinator.syncGroup).not.toHaveBeenCalled()
    expect(refused.crashes).toHaveLength(1)
    const [{ error, groupId, restart }] = refused.crashes
    expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
    expect({ groupId, restart }).toEqual({ groupId: GROUP_ID, restart: false })
    expect(error.message).toBe(
      `Group ${GROUP_ID} member (no id yet): the join retries ran out on an answer no coordinator sends: JoinGroup response has no member id`
    )

    await jest.advanceTimersByTimeAsync(RESTART_DELAY * 10)
    expect(refused.coordinator.joinGroup).toHaveBeenCalledTimes(3)
    expect(refused.crashes).toHaveLength(1)
  })

  test('a restartOnFailure that returns true receives the stop and restarts the member, as for any crash it decides', async () => {
    const restartOnFailure = jest.fn(async () => true)
    const group = createGroup()
    const refused = createMember(group, REFUSED, {
      assignment: Buffer.from([0]),
      retry: { restartOnFailure },
    })
    await start(refused)
    await until(() => refused.crashes.length > 0, 10000)

    expect(restartOnFailure).toHaveBeenCalledTimes(1)
    const [[error]] = restartOnFailure.mock.calls
    expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
    expect(refused.crashes).toEqual([{ error, groupId: GROUP_ID, restart: true }])

    await until(() => refused.coordinator.joinGroup.mock.calls.length > 3, RESTART_DELAY * 4)
    expect(refused.coordinator.joinGroup.mock.calls.length).toBeGreaterThan(3)
  })
})
