const { setTimeout: realSetTimeout, clearTimeout: realClearTimeout } = require('timers')
const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const joinGroupV8 = require('../../../testHelpers/wire/joinGroupV8')
const syncGroupV5 = require('../../../testHelpers/wire/syncGroupV5')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const createConsumer = require('../index')
const ConsumerGroup = require('../consumerGroup')
const OffsetOperations = require('../offsetManager/offsetOperations')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { MemberAssignment, MemberMetadata } = require('../assignerProtocol')
const { assertJoinAnswer } = require('../../protocol/requests/groupProtocolCheck')
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
      `Group ${GROUP_ID} member ${MEMBER}: the join ended on a refused group answer: SyncGroup response carries an assignment shorter than its version`
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
      `Group ${GROUP_ID} member ${MEMBER}: the join ended on a refused group answer: SyncGroup response carries an assignment shorter than its version`
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

describe('Consumer > classic group answers refused until the join retries run out', () => {
  const HEALTHY = 'member-healthy'
  const REFUSED = 'member-refused'
  const RESTART_DELAY = 300
  const REBALANCE_IN_PROGRESS = 27
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
   * cases the coordinator scripts an external leader's assignments.
   * @param {string} [leaderId] id of the participating leader
   * @returns {object} coordinator state and the ordered test journal
   */
  const createGroup = (leaderId) =>
    (currentGroup = {
      leaderId,
      members: new Map(),
      generation: 0,
      joining: null,
      assignments: null,
      rejoining: false,
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
   * The answers pass the Broker's own check. In the paired cases the leader publishes the
   * assignments for the generation. With an external leader SyncGroup draws from `assignment`.
   * @param {object} group from `createGroup`
   * @param {string} memberId the id the coordinator gives this member
   * @param {{ assignment?: Buffer|Buffer[], join?: object, retry?: object,
   *   groupInstanceId?: string|null, maxPollInterval?: number,
   *   assigners?: Function[] }} options `join` overrides fields of the JoinGroup answer;
   *   `assigners` replaces the one assigner
   * @returns {{ consumer: object, coordinator: object, crashes: object[], refused: string[] }}
   */
  const createMember = (
    group,
    memberId,
    { assignment, join = {}, retry = {}, groupInstanceId, maxPollInterval, assigners }
  ) => {
    const crashes = []
    const refused = []
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
          // Id allocation does not add a member. A confirmed new follower waits for the
          // leader, and an unchanged follower rejoin in the stable group keeps its generation.
          if (!group.members.has(memberId)) {
            group.members.set(memberId, {
              memberId,
              groupInstanceId: request.groupInstanceId,
              memberMetadata: request.groupProtocols[0].metadata,
            })
            if (!isLeader) {
              group.rejoining = true
              const joining = (group.joining = heldAnswer())
              await joining.promise
            }
          }
          if (isLeader) {
            group.generation += 1
            group.assignments = heldAnswer()
            group.rejoining = false
            group.joining?.resolve()
            group.joining = null
          }
        } else {
          group.generation += 1
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
          memberAssignment = published.find((row) => row.memberId === memberId).memberAssignment
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
      leaveGroup: jest.fn(async () => {
        note('LeaveGroup')
        if (group.members.delete(memberId)) group.rejoining = group.members.size > 0
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
          },
      }),
      instrumentationEmitter: cluster.instrumentationEmitter,
      partitionAssigners: assigners ?? [assignerNamed(PROTOCOL)],
      sessionTimeout: 30000,
      heartbeatInterval: 100,
      retry: { initialRetryTime: RESTART_DELAY, retries: 2, factor: 0, ...retry },
      ...(groupInstanceId ? { groupInstanceId } : {}),
      ...(maxPollInterval === undefined ? {} : { maxPollInterval }),
    })
    consumer.on(consumer.events.CRASH, ({ payload }) => {
      note(`CRASH ${payload.error.name} restart=${payload.restart}: ${payload.error.message}`)
      crashes.push(payload)
    })
    consumer.on(consumer.events.GROUP_JOIN, () => note('GROUP_JOIN'))
    consumer.on(consumer.events.REBALANCING, () => note('REBALANCING'))
    consumer.on(consumer.events.STOP, () => note('STOP'))
    consumers.push(consumer)
    return { consumer, coordinator, crashes, refused }
  }

  /**
   * @param {string} name
   * @param {Function} [assign] how the leader assigns its members
   * @returns {Function} the factory of an eager assigner named `name`
   */
  const assignerNamed =
    (name, assign = async () => []) =>
    () => ({
      name,
      version: 0,
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
   * @returns {Promise<void>}
   */
  const drive = async (stage, budget, done) => {
    let alarm
    const cancellation = new AbortController()
    const watchdog = new Promise((_, reject) => {
      alarm = realSetTimeout(() => reject(new Error(`${stage} stalled`)), 10000)
    })
    const driveClock = async () => {
      const deadline = Date.now() + budget
      await new Promise(setImmediate)
      while (!cancellation.signal.aborted && !done?.() && Date.now() < deadline) {
        jest.advanceTimersByTime(Math.min(25, deadline - Date.now()))
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

  /**
   * Starts a healthy member, lets it fetch, then starts a member whose every assignment is
   * refused and pumps fake time until its first run settles.
   * @param {object} group shared coordinator state
   * @param {string|null} groupInstanceId the refused member's
   * @param {object} [options] more `createMember` options for the refused member
   * @returns {Promise<{ group: object, healthy: object, refused: object }>}
   */
  const refusedBesideHealthy = async (group, groupInstanceId, options = {}) => {
    const invalidAssignment = Buffer.from([0])
    const healthy = createMember(group, HEALTHY, {
      assigners: [
        assignerNamed(PROTOCOL, async ({ members }) =>
          members.map(({ memberId }) => ({
            memberId,
            memberAssignment: memberId === HEALTHY ? assignmentOf([0]) : invalidAssignment,
          }))
        ),
      ],
    })
    const refused = createMember(group, REFUSED, {
      groupInstanceId,
      ...options,
    })
    await settle('healthy run', start(healthy), 2000)
    await drive('first healthy Fetch', 2000, () => fetchesOf(group, HEALTHY) > 0)
    expect(fetchesOf(group, HEALTHY)).toBeGreaterThan(0)
    await settle('refused run and crash cleanup', start(refused), 10000)
    expect(refused.crashes.map(({ error, restart }) => [error.name, restart])).toEqual([
      ['KafkaJSPartialMessageError', false],
    ])
    return { group, healthy, refused }
  }

  /**
   * Checks the stop itself, then that for `observeMs` after the cleanup settled the refused member
   * joins no more and the healthy member keeps fetching without joining again.
   * @param {{ group: object, healthy: object, refused: object }} members
   * @param {{ joins: number, groupInstanceId: string|null, reason: string, observeMs: number }} expected
   * @returns {Promise<void>}
   */
  const expectOneStop = async ({ group, healthy, refused }, expected) => {
    const [{ error, groupId }] = refused.crashes
    expect({
      joinIds: refused.coordinator.joinGroup.mock.calls.map(([request]) => request.memberId),
      syncGenerations: refused.coordinator.syncGroup.mock.calls.map(
        ([request]) => request.generationId
      ),
      refusals: refused.refused,
      // The crash cleanup leaves for a dynamic member; a static one keeps its slot.
      leaves: refused.coordinator.leaveGroup.mock.calls.length,
      groupId,
      retriable: error.retriable,
      message: error.message,
    }).toEqual({
      joinIds: ['', ...Array(expected.joins - 1).fill(REFUSED)],
      syncGenerations: Array(expected.joins).fill(2),
      refusals: Array(expected.joins).fill(expected.reason),
      leaves: expected.groupInstanceId ? 0 : 1,
      groupId: GROUP_ID,
      retriable: false,
      message: `Group ${GROUP_ID} member ${REFUSED}: the join ended on a refused group answer: ${expected.reason}`,
    })

    // Let the leader rejoin after a dynamic member leaves. A static member keeps its slot.
    await drive('healthy member after cleanup', 2000)
    const healthyJoins = healthy.coordinator.joinGroup.mock.calls.length
    expect(healthyJoins).toBe(expected.groupInstanceId ? 2 : 3)
    const healthyFetches = fetchesOf(group, HEALTHY)
    const observedFrom = Date.now()
    await drive('observe stopped member', expected.observeMs)

    expect({
      observedMs: Date.now() - observedFrom,
      refusedJoins: refused.coordinator.joinGroup.mock.calls.length,
      refusedCrashes: refused.crashes.length,
      healthyJoins: healthy.coordinator.joinGroup.mock.calls.length,
      healthyCrashes: healthy.crashes.length,
      healthyFetchedMore: fetchesOf(group, HEALTHY) > healthyFetches,
    }).toEqual({
      observedMs: expected.observeMs,
      refusedJoins: expected.joins,
      refusedCrashes: 1,
      healthyJoins,
      healthyCrashes: 0,
      healthyFetchedMore: true,
    })
  }

  const SHORTER = 'SyncGroup response carries an assignment shorter than its version'

  test.each([
    ['dynamic', null],
    ['static', 'instance-refused'],
  ])(
    'a %s member whose assignment is refused on every attempt crashes once with restart false and joins no more, while a healthy member keeps fetching',
    async (_, groupInstanceId) => {
      const group = createGroup(HEALTHY)
      await withJournal(group, async () => {
        const members = await refusedBesideHealthy(group, groupInstanceId)
        // The Broker's dynamic id handshake is part of the first of three join attempts.
        await expectOneStop(members, {
          joins: 3,
          groupInstanceId,
          reason: SHORTER,
          observeMs: RESTART_DELAY * 10,
        })
      })
    }
  )

  test.each([
    ['dynamic', null],
    ['static', 'instance-refused'],
  ])(
    'a %s member whose round deadline passes in the backoff after a refused assignment crashes once with restart false and joins no more, while a healthy member keeps fetching',
    async (_, groupInstanceId) => {
      const group = createGroup(HEALTHY)
      await withJournal(group, async () => {
        // The round ends at 5 s while the first 10 s backoff is still pending.
        const members = await refusedBesideHealthy(group, groupInstanceId, {
          maxPollInterval: 5000,
          retry: { initialRetryTime: 10000, retries: 5 },
        })

        await expectOneStop(members, {
          joins: 1,
          groupInstanceId,
          reason: SHORTER,
          observeMs: 30000,
        })
      })
    }
  )

  test('a member whose assignment is refused once and then accepted in the same join fetches and does not crash', async () => {
    const group = createGroup()
    await withJournal(group, async () => {
      const member = createMember(group, REFUSED, {
        assignment: [Buffer.from([0]), assignmentOf([1])],
      })
      await settle('member run', start(member), 5000)
      await drive('first member Fetch', 5000, () => fetchesOf(group, REFUSED) > 0)

      expect({
        joins: member.coordinator.joinGroup.mock.calls.length,
        syncs: member.coordinator.syncGroup.mock.calls.length,
        refusals: member.refused,
        crashes: member.crashes.length,
        fetched: fetchesOf(group, REFUSED) > 0,
      }).toEqual({ joins: 2, syncs: 2, refusals: [SHORTER], crashes: 0, fetched: true })
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

  test('a restartOnFailure that returns true receives the stop and restarts the member, as for any crash it decides', async () => {
    const restartOnFailure = jest.fn(async () => true)
    const group = createGroup()
    await withJournal(group, async () => {
      const refused = createMember(group, REFUSED, {
        assignment: Buffer.from([0]),
        retry: { restartOnFailure },
      })
      await settle('refused run and crash cleanup', start(refused), 10000)

      expect(restartOnFailure).toHaveBeenCalledTimes(1)
      const [[error]] = restartOnFailure.mock.calls
      expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
      expect(refused.crashes).toEqual([{ error, groupId: GROUP_ID, restart: true }])

      const restarted = () => refused.coordinator.joinGroup.mock.calls.length > 3
      await drive('member restart', RESTART_DELAY * 4, restarted)
      expect(refused.coordinator.joinGroup.mock.calls.length).toBeGreaterThan(3)
    })
  })
})
