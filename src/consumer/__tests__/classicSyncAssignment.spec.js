const { setTimeout: realSetTimeout, clearTimeout: realClearTimeout } = require('timers')
const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const joinGroupV8 = require('../../../testHelpers/wire/joinGroupV8')
const syncGroupV5 = require('../../../testHelpers/wire/syncGroupV5')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const createConsumer = require('../index')
const ConsumerGroup = require('../consumerGroup')
const RefusalEpisode = require('../refusalEpisode')
const OffsetOperations = require('../offsetManager/offsetOperations')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { MemberAssignment, MemberMetadata } = require('../assignerProtocol')
const {
  assertJoinAnswer,
  malformedGroupAnswer,
} = require('../../protocol/requests/groupProtocolCheck')
const { createErrorFromCode } = require('../../protocol/error')
const {
  KafkaJSNonRetriableError,
  KafkaJSPartialMessageError,
  KafkaJSProtocolError,
} = require('../../errors')
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
 * @param {string} [protocolName] what v5 reports
 * @returns {Buffer}
 */
const syncAnswer = (assignment, version = 3, protocolName = PROTOCOL) => {
  const encoder = new Encoder().writeInt32(0).writeInt16(0)
  if (version < 4) return encoder.writeBytes(assignment).buffer
  if (version >= 5) encoder.writeUVarIntString('consumer').writeUVarIntString(protocolName)
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
   *   logger?: object, syncMaxVersion?: number, assignerName?: string,
   *   assign?: Function }} script `assignerName` and `assign` make the one assigner
   * @returns {{ group: ConsumerGroup, apis: () => number[], joinedAs: () => string[],
   *   syncVersions: () => number[], bodies: (apiKey: number) => Buffer[] }} `apis` is the API
   *   key of every request written, `joinedAs` the member id of every JoinGroup, `syncVersions`
   *   the version of every SyncGroup, `bodies` the request bodies of one API
   */
  const exchange = ({
    joins,
    syncs,
    retries = 1,
    maxTotalTime,
    logger = newLogger(),
    syncMaxVersion = 3,
    assignerName = PROTOCOL,
    assign,
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
        {
          name: assignerName,
          protocol: () => ({ name: assignerName, metadata: Buffer.alloc(0) }),
          ...(assign ? { assign } : {}),
        },
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
      bodies: (apiKey) =>
        harness.requests.filter((request) => request.apiKey === apiKey).map(({ body }) => body),
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
  ])('%s is refused: the join returns the refusal and joins no more', async (_, bytes) => {
    const { group, apis, joinedAs } = exchange({
      joins: [joinAnswer(), joinAnswer()],
      syncs: [syncAnswer(bytes), syncAnswer(VALID)],
    })

    const result = await group.joinAndSync()

    expect(result).toEqual({
      refused: expect.objectContaining({ type: 'MALFORMED_GROUP_ANSWER', retriable: true }),
      unionSnapshot: null,
    })
    expect(apis()).toEqual([JOIN, SYNC])
    expect(joinedAs()).toEqual([''])
    // The JoinGroup before the refused SyncGroup gave the member its id and generation.
    expect({ memberId: group.memberId, generationId: group.generationId }).toEqual({
      memberId: MEMBER,
      generationId: 1,
    })
  })

  test.each([
    ['v5', 5, 'consumer'],
    ['v4', 4, null],
  ])(
    'a compact null assignment in a SyncGroup %s answer is refused, and the join returns the refusal',
    async (_, version, protocolType) => {
      const lines = []
      const { group, apis, syncVersions } = exchange({
        syncMaxVersion: 5,
        // A join answer without a protocol type caps the SyncGroup at v4.
        joins: [joinAnswer({ protocolType }), joinAnswer({ protocolType })],
        syncs: [syncAnswer(null, version), syncAnswer(VALID, version)],
        logger: warnLogger(lines),
      })

      const result = await group.joinAndSync()

      expect(result.refused).toMatchObject({
        type: 'MALFORMED_GROUP_ANSWER',
        message: 'SyncGroup response carries no assignment',
      })
      expect(apis()).toEqual([JOIN, SYNC])
      expect(syncVersions()).toEqual([version])
      // The JoinGroup refusal warning is not the assignment's.
      expect(lines.filter(({ log }) => log.message.startsWith('Refused a group answer'))).toEqual(
        []
      )
    }
  )

  /**
   * A run's refusal episode attached to `group`, as the runner attaches it.
   * @param {ConsumerGroup} group
   * @param {object[]} errors where its error logs are pushed
   * @returns {RefusalEpisode}
   */
  const attachEpisode = (group, errors) =>
    (group.refusal = new RefusalEpisode({
      maxPollInterval: 60000,
      groupId: GROUP_ID,
      logger: newLogger({
        level: LEVELS.ERROR,
        logCreator: () => (entry) => errors.push(entry.log),
      }),
      onExpired: () => {},
    }))

  test("a run's first refused assignment is asked for once more in the same generation, uploading nothing, and a usable answer is returned", async () => {
    const errors = []
    const { group, apis, bodies } = exchange({
      syncMaxVersion: 5,
      joins: [joinAnswer({ generationId: 4 })],
      syncs: [syncAnswer(Buffer.from([0]), 5), syncAnswer(VALID, 5)],
    })
    const episode = attachEpisode(group, errors)

    try {
      await expect(group.joinAndSync()).resolves.toMatchObject({
        assignment: { [TOPIC]: [0, 1] },
      })

      expect(apis()).toEqual([JOIN, SYNC, SYNC])
      expect(
        bodies(SYNC).map((body) => {
          const { generationId, memberId, groupAssignment } = syncGroupV5.readRequest(body)
          return { generationId, memberId, groupAssignment }
        })
      ).toEqual([
        { generationId: 4, memberId: MEMBER, groupAssignment: [] },
        { generationId: 4, memberId: MEMBER, groupAssignment: [] },
      ])
      expect(errors).toEqual([
        expect.objectContaining({
          message: 'Refused an assignment this member cannot use; it holds no partitions',
          groupId: GROUP_ID,
          memberId: MEMBER,
          generationId: 4,
          error: 'SyncGroup response carries an assignment shorter than its version',
        }),
      ])
      // Only an installed assignment ends the episode, and the caller installs it.
      expect(episode.open).toBe(true)
    } finally {
      episode.close()
    }
  })

  test('a run whose assignment is refused again in the same generation returns the refusal, logs once and asks no more', async () => {
    const errors = []
    const { group, apis } = exchange({
      joins: [joinAnswer(), joinAnswer()],
      syncs: [syncAnswer(Buffer.from([0])), syncAnswer(Buffer.from([0])), syncAnswer(VALID)],
    })
    const episode = attachEpisode(group, errors)

    try {
      const result = await group.joinAndSync()

      expect(result.refused).toMatchObject({
        type: 'MALFORMED_GROUP_ANSWER',
        message: 'SyncGroup response carries an assignment shorter than its version',
      })
      expect(apis()).toEqual([JOIN, SYNC, SYNC])
      expect(errors).toHaveLength(1)
      expect({ open: episode.open, state: episode.state }).toEqual({
        open: true,
        state: 'waiting',
      })
    } finally {
      episode.close()
    }
  })

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

  test('a refused assignment spends no join retry and is not stopped by retry.maxTotalTime: the join returns it at once', async () => {
    const { group, apis } = exchange({
      retries: 0,
      maxTotalTime: 1,
      joins: [joinAnswer()],
      syncs: [syncAnswer(Buffer.from([0]))],
    })

    const result = await group.joinAndSync()

    expect(result.refused).toMatchObject({
      type: 'MALFORMED_GROUP_ANSWER',
      message: 'SyncGroup response carries an assignment shorter than its version',
    })
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

  test('an assigner whose name has a lone surrogate is offered as its UTF-8 bytes, and the echo of that name is accepted and leads the round', async () => {
    // UTF-8 writes the lone surrogate as U+FFFD, and the coordinator selects and echoes that.
    const echoed = 'planner-�'
    const leaderAnswer = joinAnswer({
      groupProtocol: echoed,
      leaderId: MEMBER,
      members: [
        {
          memberId: MEMBER,
          groupInstanceId: 'instance-1',
          memberMetadata: MemberMetadata.encode({ version: 0, topics: [TOPIC] }),
        },
      ],
    })
    const { group, apis, bodies } = exchange({
      assignerName: 'planner-\uD800',
      assign: async () => [{ memberId: MEMBER, memberAssignment: VALID }],
      syncMaxVersion: 5,
      joins: [leaderAnswer, leaderAnswer],
      syncs: [syncAnswer(VALID, 5, echoed)],
    })

    await expect(group.joinAndSync()).resolves.toMatchObject({ assignment: { [TOPIC]: [0, 1] } })

    expect(apis()).toEqual([JOIN, SYNC])
    expect(
      bodies(JOIN).map((body) =>
        joinGroupV8.readRequest(body).groupProtocols.map(({ name }) => name)
      )
    ).toEqual([[echoed]])
    expect(bodies(SYNC).map((body) => syncGroupV5.readRequest(body))).toMatchObject([
      {
        protocolName: echoed,
        groupAssignment: [{ memberId: MEMBER, memberAssignment: VALID.toString('hex') }],
      },
    ])
  })
})

describe('Consumer > classic group answers refused over a run', () => {
  const HEALTHY = 'member-healthy'
  const REFUSED = 'member-refused'
  const THIRD = 'member-third'
  const RESTART_DELAY = 300
  const REBALANCE_IN_PROGRESS = 27
  const SHORTER = 'SyncGroup response carries an assignment shorter than its version'
  const INVALID = Buffer.from([0])
  const consumers = []
  let currentGroup = null

  beforeEach(() =>
    jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] })
  )
  afterEach(async () => {
    const group = currentGroup
    currentGroup = null
    try {
      const stopping = consumers.splice(0).map((consumer) => consumer.disconnect({ timeout: 1000 }))
      if (stopping.length) {
        await withJournal(group, () => settle('disconnect', Promise.all(stopping), 2000))
      }
    } finally {
      jest.useRealTimers()
      jest.restoreAllMocks()
    }
  })

  /** @param {number[]} partitions @returns {Buffer} an assignment of TOPIC */
  const assignmentOf = (partitions) =>
    MemberAssignment.encode({ version: 0, assignment: { [TOPIC]: partitions } })

  /**
   * Creates a group with a participating leader for the paired cases. In the single-member
   * cases the coordinator scripts an external leader's assignments. `relayed` is what the
   * coordinator hands a member for the bytes its leader published: a case replaces it to stand for
   * a leader on another client that writes bytes this client refuses.
   * @param {string} [leaderId] id of the participating leader
   * @returns {object} coordinator state and the ordered test journal
   */
  const createGroup = (leaderId) =>
    (currentGroup = {
      leaderId,
      members: new Map(),
      generation: 0,
      joining: null,
      joined: new Set(),
      onJoin: null,
      assignments: null,
      rejoining: false,
      relayed: (memberId, bytes) => bytes,
      fetches: [],
      events: [],
      fetchRounds: {},
    })

  /**
   * What a case's group saw, for assertion messages.
   * @param {object} group
   * @returns {{ events: string[], fetchRounds: Object<string, number>, fetches: number }}
   */
  const seen = ({ events, fetchRounds, fetches }) => ({
    events,
    fetchRounds,
    fetches: fetches.length,
  })

  /** @param {object} group @param {() => Promise<void>} work @returns {Promise<void>} */
  const withJournal = async (group, work) => {
    try {
      await work()
    } catch (error) {
      throw new Error(`${error.message}\nGroup journal:\n${JSON.stringify(seen(group), null, 2)}`, {
        cause: error,
      })
    }
  }

  /** @returns {{ promise: Promise<*>, resolve: Function }} a held coordinator answer */
  const heldAnswer = () => {
    let resolve
    const promise = new Promise((answer) => (resolve = answer))
    return { promise, resolve }
  }

  /**
   * A real classic consumer of `group`, over a double of the Broker's group calls. A dynamic
   * member's first JoinGroup goes through the MEMBER_ID_REQUIRED handshake the Broker performs
   * (`onMemberIdRequired` with the new id, then the answer), a static one gets its id at once.
   * The answers pass the Broker's own check. In the paired cases a new member starts a rebalance:
   * a follower's JoinGroup then waits for the leader's, which waits until every known member has
   * joined, and the leader publishes the assignments for the generation; outside a rebalance a
   * known follower keeps its generation and a SyncGroup returns the stored bytes, as in a stable
   * group. With an external leader each JoinGroup starts a generation and SyncGroup draws from
   * `assignment`.
   * @param {object} group from `createGroup`
   * @param {string} memberId the id the coordinator gives this member
   * @param {{ assignment?: Buffer|Buffer[], join?: object, retry?: object,
   *   groupInstanceId?: string|null, maxPollInterval?: number, maxWaitTimeInMs?: number,
   *   assigners?: Function[] }} options `join` overrides fields of the JoinGroup answer;
   *   `assigners` replaces the one assigner
   * @returns {{ consumer: object, coordinator: object, crashes: object[], refused: string[],
   *   refusals: object[], leaves: object[], events: Object<string, object[]> }} `refused` holds the
   *   JoinGroup refusal warnings, `refusals` the refused-assignment error logs with their fake
   *   time, `leaves` each LeaveGroup with its fake time, `events` the payloads of the partition
   *   and poll events
   */
  const createMember = (
    group,
    memberId,
    {
      assignment,
      join = {},
      retry = {},
      groupInstanceId,
      maxPollInterval,
      maxWaitTimeInMs,
      assigners,
    }
  ) => {
    const crashes = []
    const refused = []
    const refusals = []
    const leaves = []
    const events = { lost: [], revoked: [], pollExpired: [] }
    const assignments = [].concat(assignment)
    let fetchedGeneration = null
    let heartbeatGeneration = null
    const note = (event) => group.events.push(`${Date.now()} ${memberId} ${event}`)
    const isLeader = memberId === group.leaderId
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
      joinGroup: jest.fn(async (request, options) => {
        note(`JoinGroup as ${request.memberId || '(no id)'}`)
        if (!request.memberId && !request.groupInstanceId) {
          note(`JoinGroup answered MEMBER_ID_REQUIRED (79), id=${memberId}`)
          options?.onMemberIdRequired?.(memberId)
          const stopped = options?.guard?.()
          if (stopped) throw stopped
          note(`JoinGroup as ${memberId}`)
        }

        if (group.leaderId) {
          // Id allocation does not add a member; a confirmed new one starts a rebalance.
          if (!group.members.has(memberId)) {
            group.members.set(memberId, {
              memberId,
              groupInstanceId: request.groupInstanceId,
              memberMetadata: request.groupProtocols[0].metadata,
            })
            group.rejoining = true
          }
          if (!isLeader && group.rejoining) {
            group.joined.add(memberId)
            group.onJoin?.()
            const joining = (group.joining ??= heldAnswer())
            await joining.promise
          }
          if (isLeader) {
            const everyoneJoined = () =>
              [...group.members.keys()].every((id) => id === memberId || group.joined.has(id))
            while (group.rejoining && !everyoneJoined()) {
              await new Promise((resolve) => (group.onJoin = resolve))
            }
            group.onJoin = null
            group.joined.clear()
            group.generation += 1
            group.assignments = heldAnswer()
            group.rejoining = false
            group.joining?.resolve()
            group.joining = null
          }
        } else {
          group.generation += 1
          group.rejoining = false
        }

        const answer = {
          generationId: group.generation,
          leaderId: group.leaderId || 'someone-else',
          memberId,
          members: isLeader ? [...group.members.values()] : [],
          groupProtocol: PROTOCOL,
          protocolType: 'consumer',
          ...join,
        }
        note(`JoinGroup answered generation=${answer.generationId}, id=${answer.memberId}`)
        assertJoinAnswer(answer, request.groupProtocols)
        return answer
      }),
      syncGroup: jest.fn(async ({ generationId, groupAssignment }) => {
        note(`SyncGroup generation=${generationId}`)
        let memberAssignment
        if (group.leaderId) {
          if (isLeader) group.assignments.resolve(groupAssignment)
          const published = await group.assignments.promise
          memberAssignment = group.relayed(
            memberId,
            published.find((row) => row.memberId === memberId).memberAssignment
          )
        } else {
          memberAssignment = assignments.length > 1 ? assignments.shift() : assignments[0]
        }
        note(
          `SyncGroup answered generation=${generationId}, assignment=${memberAssignment.toString('hex')}`
        )
        return { memberAssignment }
      }),
      heartbeat: jest.fn(async ({ generationId }) => {
        if (group.rejoining) {
          note(`Heartbeat generation=${generationId} answered REBALANCE_IN_PROGRESS`)
          throw new KafkaJSProtocolError(createErrorFromCode(REBALANCE_IN_PROGRESS))
        }
        if (generationId !== heartbeatGeneration) {
          heartbeatGeneration = generationId
          note(`Heartbeat generation=${generationId} answered success`)
        }
      }),
      leaveGroup: jest.fn(async (request) => {
        note(`LeaveGroup as ${request.memberId}`)
        leaves.push({
          at: Date.now(),
          memberId: request.memberId,
          groupInstanceId: request.groupInstanceId || null,
        })
        if (group.members.delete(memberId)) group.rejoining = group.members.size > 0
        group.onJoin?.()
      }),
    }
    const broker = {
      fetch: jest.fn(async ({ topics }) => {
        for (const { partitions } of topics) group.fetches.push(...partitions.map(() => memberId))
        if (fetchedGeneration !== group.generation) {
          fetchedGeneration = group.generation
          note(`Fetch generation=${fetchedGeneration}`)
        }
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
      // Called once by every fetch preparation.
      refreshMetadataIfNecessary: jest.fn(async () => {
        group.fetchRounds[memberId] = (group.fetchRounds[memberId] ?? 0) + 1
      }),
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
      logger: newLogger({
        level: LEVELS.WARN,
        logCreator:
          () =>
          ({ log }) => {
            note(log.message)
            if (log.message.startsWith('Refused a group answer')) refused.push(log.error)
            if (log.message.startsWith('Refused an assignment')) {
              refusals.push({ at: Date.now(), generationId: log.generationId, error: log.error })
            }
          },
      }),
      instrumentationEmitter: cluster.instrumentationEmitter,
      partitionAssigners: assigners ?? [assignerNamed(PROTOCOL)],
      sessionTimeout: 30000,
      heartbeatInterval: 100,
      retry: { initialRetryTime: RESTART_DELAY, retries: 2, factor: 0, ...retry },
      ...(groupInstanceId ? { groupInstanceId } : {}),
      ...(maxPollInterval === undefined ? {} : { maxPollInterval }),
      ...(maxWaitTimeInMs === undefined ? {} : { maxWaitTimeInMs }),
    })
    consumer.on(consumer.events.CRASH, ({ payload }) => {
      note(`CRASH ${payload.error.name} restart=${payload.restart}: ${payload.error.message}`)
      crashes.push(payload)
    })
    consumer.on(consumer.events.GROUP_JOIN, () => note('GROUP_JOIN'))
    consumer.on(consumer.events.REBALANCING, () => note('REBALANCING'))
    consumer.on(consumer.events.STOP, () => note('STOP'))
    consumer.on(consumer.events.PARTITIONS_LOST, ({ payload }) => {
      note('PARTITIONS_LOST')
      events.lost.push(payload.partitions)
    })
    consumer.on(consumer.events.PARTITIONS_REVOKED, ({ payload }) => {
      note('PARTITIONS_REVOKED')
      events.revoked.push(payload.partitions)
    })
    consumer.on(consumer.events.POLL_INTERVAL_EXCEEDED, ({ payload }) => {
      note('POLL_INTERVAL_EXCEEDED')
      events.pollExpired.push(payload)
    })
    consumers.push(consumer)
    return { consumer, coordinator, crashes, refused, refusals, leaves, events }
  }

  /**
   * @param {string} name
   * @param {Function} [assign] how the leader assigns its members
   * @param {string[]} [protocols] the rebalance protocols it supports
   * @returns {Function} the factory of an assigner named `name`, eager unless `protocols` says
   */
  const assignerNamed =
    (name, assign = async () => [], protocols = ['eager']) =>
    () => ({
      name,
      version: 0,
      supportedProtocols: () => protocols,
      protocol: ({ topics }) => ({ name, metadata: MemberMetadata.encode({ version: 0, topics }) }),
      assign,
    })

  /** @param {{ consumer: object }} member @returns {Promise<void>} */
  const start = async ({ consumer }) => {
    await consumer.subscribe({ topic: TOPIC })
    await consumer.run({ eachMessage: async () => {} })
  }

  /**
   * Advances only this case's fake clock. Between ticks the native turn lets a fetcher yield
   * and lets promises arm their next timers. A native watchdog also covers the turn itself.
   * @param {string} stage what the case is waiting for
   * @param {number} budget fake milliseconds to advance at most
   * @param {() => boolean} [done] omit to observe the whole fake interval
   * @param {number} [step] fake milliseconds per tick
   * @returns {Promise<void>}
   */
  const drive = async (stage, budget, done, step = 25) => {
    let alarm
    const cancellation = new AbortController()
    const watchdog = new Promise((_, reject) => {
      alarm = realSetTimeout(() => reject(new Error(`${stage} stalled`)), 10000)
    })
    const driveClock = async () => {
      const deadline = Date.now() + budget
      await new Promise(setImmediate)
      while (!cancellation.signal.aborted && !done?.() && Date.now() < deadline) {
        jest.advanceTimersByTime(Math.min(step, deadline - Date.now()))
        await new Promise(setImmediate)
      }
      if (!cancellation.signal.aborted && done && !done()) {
        throw new Error(`${stage} unfinished after ${budget} fake ms`)
      }
    }

    try {
      await Promise.race([driveClock(), watchdog])
    } finally {
      cancellation.abort()
      realClearTimeout(alarm)
    }
  }

  /** @param {string} stage @param {Promise<*>} operation @param {number} budget @returns {Promise<void>} */
  const settle = async (stage, operation, budget) => {
    let settled = false
    const markSettled = () => {
      settled = true
    }
    operation.then(markSettled, markSettled)
    await drive(stage, budget, () => settled)
    await operation
  }

  /** @param {object} group @param {string} memberId @returns {number} */
  const fetchesOf = (group, memberId) => group.fetches.filter((id) => id === memberId).length

  /** @param {object} member @returns {Array<[number, Array]>} each SyncGroup's generation and upload */
  const syncsOf = (member) =>
    member.coordinator.syncGroup.mock.calls.map(([request]) => [
      request.generationId,
      request.groupAssignment,
    ])

  /** @param {object} member @returns {Array<[number, string]>} each refused generation and reason */
  const refusalsOf = (member) =>
    member.refusals.map(({ generationId, error }) => [generationId, error])

  /** @param {object} member @returns {Array<[string, string|null]>} each LeaveGroup's two ids */
  const leavesOf = (member) =>
    member.leaves.map(({ memberId, groupInstanceId }) => [memberId, groupInstanceId])

  /** @param {number} wait @returns {string} the stop's message after a never-usable assignment */
  const expiredMessage = (wait) =>
    `Group ${GROUP_ID} member ${REFUSED}: no usable assignment within ${wait} ms of the first refused one: ${SHORTER}`

  /**
   * The healthy leader's assigner: partition 0 to itself and 1 to the refused member, both to a
   * lone member, and nothing to anyone else.
   */
  const healthyAssigner = assignerNamed(PROTOCOL, async ({ members }) =>
    members.map(({ memberId }) => ({
      memberId,
      memberAssignment: assignmentOf(
        members.length === 1 ? [0, 1] : memberId === HEALTHY ? [0] : memberId === REFUSED ? [1] : []
      ),
    }))
  )

  /**
   * Starts a healthy leader and lets it fetch, then starts a member whose assignment the
   * coordinator relays as one byte, as a leader on another client may write it, and pumps fake
   * time until that member's first round has settled with its first refusal.
   * @param {object} group from `createGroup(HEALTHY)`
   * @param {object} [options] more `createMember` options for the refused member
   * @returns {Promise<{ healthy: object, refused: object, healthyJoins: number }>} `healthyJoins`
   *   is the leader's JoinGroup count once the refused member waits
   */
  const besideOldLeader = async (group, options = {}) => {
    group.relayed = (memberId, bytes) => (memberId === REFUSED ? INVALID : bytes)
    const healthy = createMember(group, HEALTHY, { assigners: [healthyAssigner] })
    const refused = createMember(group, REFUSED, options)
    await settle('healthy run', start(healthy), 2000)
    await drive('first healthy Fetch', 2000, () => fetchesOf(group, HEALTHY) > 0)
    await settle('refused run', start(refused), 10000)
    expect(refusalsOf(refused)).toEqual([[2, SHORTER]])
    return { healthy, refused, healthyJoins: healthy.coordinator.joinGroup.mock.calls.length }
  }

  /**
   * Pumps fake time to just before the refused member's deadline and checks it is still waiting,
   * then to its stop, and checks the leave, the crash and how long it waited.
   * @param {object} refused the member from `createMember`
   * @param {{ wait: number, groupInstanceId?: string|null, step?: number }} expected
   * @returns {Promise<void>}
   */
  const expectStopAtDeadline = async (refused, { wait, groupInstanceId = null, step = 25 }) => {
    const firstRefusal = refused.refusals[0].at
    await drive('wait before its deadline', firstRefusal + wait - 50 - Date.now(), undefined, step)
    expect({
      crashes: refused.crashes.length,
      leaves: refused.leaves.length,
      pollExpired: refused.events.pollExpired.length,
    }).toEqual({ crashes: 0, leaves: 0, pollExpired: 0 })

    await drive('the stop at the deadline', 2000, () => refused.crashes.length > 0)
    const [{ error, groupId, restart }] = refused.crashes
    expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
    expect({
      leaves: leavesOf(refused),
      retriable: error.retriable,
      message: error.message,
      groupId,
      restart,
      pollExpired: refused.events.pollExpired.length,
    }).toEqual({
      leaves: [[REFUSED, groupInstanceId]],
      retriable: false,
      message: expiredMessage(wait),
      groupId: GROUP_ID,
      restart: false,
      pollExpired: 0,
    })
    const waited = refused.leaves[0].at - firstRefusal
    expect(waited).toBeGreaterThanOrEqual(wait)
    expect(waited).toBeLessThan(wait + 1000)
  }

  test.each([
    ['dynamic', null],
    ['static', 'instance-refused'],
  ])(
    'a %s member whose assignment is never usable holds nothing until its deadline, then leaves once and stops with restart false, causing one rebalance while a healthy member keeps fetching',
    async (_, groupInstanceId) => {
      const group = createGroup(HEALTHY)
      await withJournal(group, async () => {
        const { healthy, refused, healthyJoins } = await besideOldLeader(group, {
          groupInstanceId,
          maxPollInterval: 3000,
        })

        await expectStopAtDeadline(refused, { wait: 3000, groupInstanceId })
        expect({
          joinIds: refused.coordinator.joinGroup.mock.calls.map(([request]) => request.memberId),
          // The first answer and the one same-generation check, which uploads nothing.
          syncs: syncsOf(refused),
          refusals: refusalsOf(refused),
          fetched: fetchesOf(group, REFUSED),
        }).toEqual({
          joinIds: [''],
          syncs: [
            [2, []],
            [2, []],
          ],
          refusals: [[2, SHORTER]],
          fetched: 0,
        })

        const healthyFetches = fetchesOf(group, HEALTHY)
        await drive('the group after the stop', 3000)
        expect({
          refusedJoins: refused.coordinator.joinGroup.mock.calls.length,
          refusedLeaves: refused.leaves.length,
          refusedCrashes: refused.crashes.length,
          // The one rebalance the refused member caused: its leave.
          healthyJoinsSinceRefusal: healthy.coordinator.joinGroup.mock.calls.length - healthyJoins,
          healthyCrashes: healthy.crashes.length,
          healthyFetchedMore: fetchesOf(group, HEALTHY) > healthyFetches,
        }).toEqual({
          refusedJoins: 1,
          refusedLeaves: 1,
          refusedCrashes: 1,
          healthyJoinsSinceRefusal: 1,
          healthyCrashes: 0,
          healthyFetchedMore: true,
        })
      })
    }
  )

  test.each([
    [3000, 3000],
    [300000, 600000],
  ])(
    'the wait for a usable assignment ends %s ms after the first refused one for a maxPollInterval of %s ms',
    async (wait, maxPollInterval) => {
      const group = createGroup()
      await withJournal(group, async () => {
        const member = createMember(group, REFUSED, { assignment: INVALID, maxPollInterval })
        await settle('member run', start(member), 5000)

        await expectStopAtDeadline(member, { wait, step: wait > 10000 ? 1000 : 25 })
        expect(member.coordinator.joinGroup).toHaveBeenCalledTimes(1)
      })
    }
  )

  test('a member holding nothing past maxPollInterval between its fetch rounds raises no poll-interval expiry and leaves only at its deadline', async () => {
    const group = createGroup()
    await withJournal(group, async () => {
      // Idle fetch rounds of 10 s never poll inside the 2 s poll deadline.
      const member = createMember(group, REFUSED, {
        assignment: INVALID,
        maxPollInterval: 2000,
        maxWaitTimeInMs: 10000,
      })
      await settle('member run', start(member), 5000)

      await expectStopAtDeadline(member, { wait: 2000 })
      expect(member.coordinator.joinGroup).toHaveBeenCalledTimes(1)
    })
  })

  test('a refused assignment that the same generation answers usably when asked once more is installed with no rebalance and no second error, and the member fetches', async () => {
    const group = createGroup()
    await withJournal(group, async () => {
      const member = createMember(group, REFUSED, {
        assignment: [INVALID, assignmentOf([1])],
        maxPollInterval: 3000,
      })
      await settle('member run', start(member), 5000)
      await drive('first member Fetch', 5000, () => fetchesOf(group, REFUSED) > 0)
      await drive('past the deadline the refusal opened', 4000)

      expect({
        joins: member.coordinator.joinGroup.mock.calls.length,
        generation: group.generation,
        syncs: syncsOf(member),
        refusals: refusalsOf(member),
        crashes: member.crashes.length,
        leaves: member.leaves.length,
        fetched: fetchesOf(group, REFUSED) > 0,
      }).toEqual({
        joins: 1,
        generation: 1,
        syncs: [
          [1, []],
          [1, []],
        ],
        refusals: [[1, SHORTER]],
        crashes: 0,
        leaves: 0,
        fetched: true,
      })
    })
  })

  test('a refused assignment healed by a later generation another member starts inside the wait is installed: the member fetches, does not crash and is not restarted', async () => {
    const group = createGroup(HEALTHY)
    await withJournal(group, async () => {
      const { healthy, refused } = await besideOldLeader(group, {
        maxPollInterval: 10000,
        maxWaitTimeInMs: 200,
      })
      const firstRefusal = refused.refusals[0].at
      await drive('waiting', 1000)

      // The leader's bytes are usable from now on, and a third member's join starts a generation.
      group.relayed = (memberId, bytes) => bytes
      const third = createMember(group, THIRD, {})
      await settle('third run', start(third), 10000)
      await drive('refused member fetches', 5000, () => fetchesOf(group, REFUSED) > 0)
      await drive('past the first deadline', firstRefusal + 11000 - Date.now())

      expect({
        joins: refused.coordinator.joinGroup.mock.calls.length,
        syncGenerations: syncsOf(refused).map(([generationId]) => generationId),
        refusals: refusalsOf(refused),
        crashes: refused.crashes.length,
        leaves: refused.leaves.length,
        healthyCrashes: healthy.crashes.length,
        thirdCrashes: third.crashes.length,
      }).toEqual({
        joins: 2,
        syncGenerations: [2, 2, 3],
        refusals: [[2, SHORTER]],
        crashes: 0,
        leaves: 0,
        healthyCrashes: 0,
        thirdCrashes: 0,
      })
    })
  })

  test('a rebalance in progress and a new generation inside the wait do not restart it: the member asks no more in the new generation and stops at the first deadline', async () => {
    const group = createGroup(HEALTHY)
    await withJournal(group, async () => {
      const { refused } = await besideOldLeader(group, {
        maxPollInterval: 4000,
        maxWaitTimeInMs: 200,
      })
      await drive('waiting', 1500)

      // A third member's join makes the coordinator answer heartbeats REBALANCE_IN_PROGRESS.
      const third = createMember(group, THIRD, {})
      await settle('third run', start(third), 2000)
      await drive('the refusal in the new generation', 1000, () => refused.refusals.length > 1)
      expect(refusalsOf(refused)).toEqual([
        [2, SHORTER],
        [3, SHORTER],
      ])

      await expectStopAtDeadline(refused, { wait: 4000 })
      expect({
        joins: refused.coordinator.joinGroup.mock.calls.length,
        syncGenerations: syncsOf(refused).map(([generationId]) => generationId),
        thirdCrashes: third.crashes.length,
      }).toEqual({ joins: 2, syncGenerations: [2, 2, 3], thirdCrashes: 0 })
    })
  })

  test('a cooperative member releases the partitions it held as lost at its first refused assignment and fetches nothing while it waits', async () => {
    const group = createGroup()
    await withJournal(group, async () => {
      const member = createMember(group, REFUSED, {
        assignment: [assignmentOf([0, 1]), INVALID],
        maxPollInterval: 10000,
        assigners: [assignerNamed(PROTOCOL, undefined, ['eager', 'cooperative'])],
      })
      await settle('member run', start(member), 5000)
      await drive('first member Fetch', 5000, () => fetchesOf(group, REFUSED) > 0)

      // The coordinator starts a rebalance, as another member's join would.
      group.rejoining = true
      await drive('the refusal', 5000, () => member.refusals.length > 0)
      await drive('the release', 500)
      const fetched = fetchesOf(group, REFUSED)
      await drive('holding nothing', 2000)

      expect({
        lost: member.events.lost,
        revoked: member.events.revoked,
        fetchedWhileWaiting: fetchesOf(group, REFUSED) - fetched,
        joins: member.coordinator.joinGroup.mock.calls.length,
        refusals: refusalsOf(member),
        crashes: member.crashes.length,
        leaves: member.leaves.length,
      }).toEqual({
        lost: [[{ topic: TOPIC, partitions: [0, 1] }]],
        revoked: [],
        fetchedWhileWaiting: 0,
        joins: 2,
        refusals: [[2, SHORTER]],
        crashes: 0,
        leaves: 0,
      })
    })
  })

  test('a stop while a member waits for a usable assignment ends the run without a crash, and its deadline never fires', async () => {
    const group = createGroup()
    await withJournal(group, async () => {
      const member = createMember(group, REFUSED, { assignment: INVALID, maxPollInterval: 5000 })
      await settle('member run', start(member), 5000)
      await drive('waiting', 1000)

      await settle('stop', member.consumer.stop(), 3000)
      await drive('past the deadline', 6000)

      expect({
        crashes: member.crashes.length,
        // A dynamic member's ordinary stop leaves, as before.
        leaves: leavesOf(member),
        joins: member.coordinator.joinGroup.mock.calls.length,
        refusals: refusalsOf(member),
      }).toEqual({
        crashes: 0,
        leaves: [[REFUSED, null]],
        joins: 1,
        refusals: [[1, SHORTER]],
      })
    })
  })

  test('a static leader whose own assigner writes an assignment no member can use stops before its SyncGroup with an error naming the assigner, and leaves once with its instance id', async () => {
    const group = createGroup(REFUSED)
    await withJournal(group, async () => {
      const leader = createMember(group, REFUSED, {
        groupInstanceId: 'instance-refused',
        assigners: [
          assignerNamed(PROTOCOL, async ({ members }) =>
            members.map(({ memberId }) => ({ memberId, memberAssignment: INVALID }))
          ),
        ],
      })
      await settle('leader run and crash cleanup', start(leader), 10000)

      const [{ error, groupId, restart }] = leader.crashes
      expect({
        name: error.name,
        type: error.type,
        retriable: error.retriable,
        message: error.message,
        groupId,
        restart,
        crashes: leader.crashes.length,
        syncs: leader.coordinator.syncGroup.mock.calls.length,
        leaves: leavesOf(leader),
      }).toEqual({
        name: 'KafkaJSNonRetriableError',
        type: 'INVALID_ASSIGNER_OUTPUT',
        retriable: false,
        message: `Partition assigner "${PROTOCOL}" produced an unusable assignment: its output for member ${REFUSED} carries an assignment shorter than its version`,
        groupId: GROUP_ID,
        restart: false,
        crashes: 1,
        syncs: 0,
        leaves: [[REFUSED, 'instance-refused']],
      })
    })
  })

  test('two assigners whose names are written as the same bytes are refused when the consumer is created, and one name listed twice is not', () => {
    const group = createGroup()

    expect(() =>
      createMember(group, REFUSED, {
        assignment: assignmentOf([0]),
        assigners: [assignerNamed('planner-\uD800'), assignerNamed('planner-\uDBFF')],
      })
    ).toThrow(
      new KafkaJSNonRetriableError(
        'The partition assigners "planner-\uD800" and "planner-\uDBFF" are both sent as "planner-�", so the group cannot tell them apart'
      )
    )
    expect(() =>
      createMember(group, REFUSED, {
        assignment: assignmentOf([0]),
        assigners: [assignerNamed('planner'), assignerNamed('planner')],
      })
    ).not.toThrow()
  })

  test('a JoinGroup answer refused on every attempt crashes the member once with restart false, before any SyncGroup', async () => {
    const group = createGroup()
    await withJournal(group, async () => {
      const refused = createMember(group, REFUSED, {
        assignment: assignmentOf([0]),
        join: { memberId: '' },
      })
      await settle('refused run and crash cleanup', start(refused), 10000)

      expect(refused.coordinator.joinGroup).toHaveBeenCalledTimes(3)
      expect(refused.coordinator.syncGroup).not.toHaveBeenCalled()
      expect(refused.crashes).toHaveLength(1)
      const [{ error, groupId, restart }] = refused.crashes
      expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
      expect({ groupId, restart }).toEqual({ groupId: GROUP_ID, restart: false })
      expect(error.message).toBe(
        `Group ${GROUP_ID} member (no id yet): the join ended on a refused group answer: JoinGroup response has no member id`
      )

      await drive('observe stopped member', RESTART_DELAY * 10)
      expect(refused.coordinator.joinGroup).toHaveBeenCalledTimes(3)
      expect(refused.crashes).toHaveLength(1)
    })
  })

  test('a restartOnFailure that returns true receives the stop after the wait and restarts the member, whose new run starts a new wait', async () => {
    const restartOnFailure = jest.fn(async () => true)
    const group = createGroup()
    await withJournal(group, async () => {
      const refused = createMember(group, REFUSED, {
        assignment: INVALID,
        maxPollInterval: 2000,
        retry: { restartOnFailure },
      })
      await settle('refused run', start(refused), 5000)
      await drive('the stop', 5000, () => refused.crashes.length > 0)

      expect(restartOnFailure).toHaveBeenCalledTimes(1)
      const [[error]] = restartOnFailure.mock.calls
      expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
      expect(error.message).toBe(expiredMessage(2000))
      expect(refused.crashes).toEqual([{ error, groupId: GROUP_ID, restart: true }])

      await drive(
        'the restarted run refuses again',
        RESTART_DELAY * 4 + 2000,
        () => refused.refusals.length > 1
      )
      expect({
        joins: refused.coordinator.joinGroup.mock.calls.length,
        leaves: refused.leaves.length,
        crashes: refused.crashes.length,
        refusals: refusalsOf(refused),
      }).toEqual({
        joins: 2,
        leaves: 1,
        crashes: 1,
        refusals: [
          [1, SHORTER],
          [2, SHORTER],
        ],
      })
    })
  })
})

describe('Consumer > classic refused-assignment episode', () => {
  const REFUSAL = malformedGroupAnswer(
    'SyncGroup response carries an assignment shorter than its version'
  )

  beforeEach(() =>
    jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] })
  )
  afterEach(() => jest.useRealTimers())

  /**
   * @param {number} maxPollInterval
   * @returns {{ episode: RefusalEpisode, expired: Error[], errors: object[] }} `expired` holds what
   *   `onExpired` received, `errors` the error logs
   */
  const episodeOf = (maxPollInterval) => {
    const expired = []
    const errors = []
    const episode = new RefusalEpisode({
      maxPollInterval,
      groupId: GROUP_ID,
      logger: newLogger({
        level: LEVELS.ERROR,
        logCreator: () => (entry) => errors.push(entry.log),
      }),
      onExpired: (error) => expired.push(error),
    })
    return { episode, expired, errors }
  }

  test.each([
    [2000, 2000],
    [300000, 600000],
  ])(
    'ends %s ms after its first refused assignment for a maxPollInterval of %s ms, whatever generations are refused meanwhile',
    (wait, maxPollInterval) => {
      const { episode, expired, errors } = episodeOf(maxPollInterval)

      expect(episode.refuse(REFUSAL, { memberId: MEMBER, generationId: 1 })).toBe('verify')
      expect(episode.refuse(REFUSAL, { memberId: MEMBER, generationId: 1 })).toBe('wait')
      jest.advanceTimersByTime(wait - 1)
      expect(episode.refuse(REFUSAL, { memberId: MEMBER, generationId: 2 })).toBe('wait')
      expect(expired).toEqual([])

      jest.advanceTimersByTime(1)
      expect(expired).toHaveLength(1)
      expect(expired[0]).toBeInstanceOf(KafkaJSPartialMessageError)
      expect({
        message: expired[0].message,
        retriable: expired[0].retriable,
        error: episode.error,
        open: episode.open,
        loggedGenerations: errors.map(({ generationId }) => generationId),
      }).toEqual({
        message: `Group ${GROUP_ID} member ${MEMBER}: no usable assignment within ${wait} ms of the first refused one: ${REFUSAL.message}`,
        retriable: false,
        error: expired[0],
        open: false,
        loggedGenerations: [1, 2],
      })
    }
  )

  test('an installed assignment ends the episode, and a later refusal opens a new one with its own deadline', () => {
    const { episode, expired } = episodeOf(2000)

    episode.refuse(REFUSAL, { memberId: MEMBER, generationId: 1 })
    jest.advanceTimersByTime(1500)
    episode.installed()
    jest.advanceTimersByTime(1000)
    expect({ expired: expired.length, open: episode.open }).toEqual({ expired: 0, open: false })

    expect(episode.refuse(REFUSAL, { memberId: MEMBER, generationId: 3 })).toBe('verify')
    jest.advanceTimersByTime(1999)
    expect(expired).toHaveLength(0)
    jest.advanceTimersByTime(1)
    expect(expired).toHaveLength(1)
  })

  test('a closed episode never expires, keeps no error and ignores a later end; an ended one keeps its first error', () => {
    const closed = episodeOf(2000)
    closed.episode.refuse(REFUSAL, { memberId: MEMBER, generationId: 1 })
    closed.episode.close()
    jest.advanceTimersByTime(5000)
    closed.episode.end(new Error('late'))
    expect({
      expired: closed.expired.length,
      error: closed.episode.error,
      suspendsPollDeadline: closed.episode.suspendsPollDeadline,
    }).toEqual({ expired: 0, error: null, suspendsPollDeadline: false })

    const ended = episodeOf(2000)
    const first = new Error('first')
    ended.episode.end(first)
    ended.episode.end(new Error('second'))
    ended.episode.close()
    expect({
      error: ended.episode.error,
      suspendsPollDeadline: ended.episode.suspendsPollDeadline,
      verdict: ended.episode.refuse(REFUSAL, { memberId: MEMBER, generationId: 1 }),
    }).toEqual({ error: first, suspendsPollDeadline: true, verdict: 'wait' })
  })
})
