const fs = require('fs')
const path = require('path')
const apiKeys = require('../../protocol/requests/apiKeys')
const Encoder = require('../../protocol/encoder')
const { MemberMetadata, MemberAssignment } = require('../assignerProtocol')
const { createFakeCoordinator, trackFixtures } = require('./fixtures/fetchHarness')
const ClassicGroupProtocol = require('../groupProtocol/classic')
const AssignerOffers = require('../groupProtocol/assignerOffers')
const { COOPERATIVE, EAGER } = require('../groupProtocol/interface')
const cooperativeSticky = require('../assigners/cooperativeStickyAssigner')
const ConsumerGroup = require('../consumerGroup')
const IdleWait = require('../idleWait')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const {
  events: { GROUP_JOIN },
} = require('../instrumentationEvents')
const { newLogger } = require('testHelpers')
const Generation = require('../../producer/generation')
const GroupOwnership = require('../groupOwnership')
const { KafkaJSConnectionError, KafkaJSNonRetriableError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const sleep = require('../../utils/sleep')

const FIXTURES = path.join(__dirname, 'fixtures', 'classicGroupProtocol')

trackFixtures()

/** Rounds the cases below begin, retired after each case. */
const rounds = []

/** @returns {object} a real round over a private idle wait, for a protocol call made directly */
const realRound = () => {
  const group = Object.create(ConsumerGroup.prototype)
  Object.assign(group, {
    idleWait: new IdleWait(),
    rebalanceTimeout: 60000,
    cluster: { allowAutoTopicCreation: true },
    isStopping: () => false,
  })
  const round = group.beginRebalanceRound()
  rounds.push(round)
  return round
}

afterEach(() => rounds.splice(0).forEach((round) => round.retire()))

// Every value here is pinned: a golden frame that contains a timestamp or a generated id records
// nothing. The metadata bytes come from the shipped v0 subscription encoder.
const GROUP_ID = 'golden-group'
const MEMBER_ID = 'golden-member-1'
const GROUP_INSTANCE_ID = 'golden-instance-1'
const GENERATION_ID = 7
const SESSION_TIMEOUT = 30000
const REBALANCE_TIMEOUT = 60000
const TOPICS = ['golden-topic-a', 'golden-topic-b']
const PROTOCOL_METADATA = MemberMetadata.encode({ version: 0, topics: TOPICS })
const MEMBER_ASSIGNMENT = MemberAssignment.encode({
  version: 0,
  assignment: { 'golden-topic-a': [0, 1], 'golden-topic-b': [0] },
})

/**
 * A JoinGroup v5 answer.
 * @param {object} [fields]
 * @param {number} [fields.errorCode]
 * @param {string} [fields.protocolName] the protocol the coordinator selected
 * @param {string} [fields.leaderId]
 * @param {string} [fields.memberId] the id the answer gives this member
 * @param {{ memberId: string, metadata: Buffer, groupInstanceId?: string|null }[]} [fields.members]
 *   the leader's member list; a member's instance id is the golden one unless it names its own
 * @returns {Buffer}
 */
const joinResponse = ({
  errorCode = 0,
  protocolName = 'RoundRobinAssigner',
  leaderId = MEMBER_ID,
  memberId = MEMBER_ID,
  members = [{ memberId: MEMBER_ID, metadata: PROTOCOL_METADATA }],
} = {}) =>
  new Encoder()
    .writeInt32(0)
    .writeInt16(errorCode)
    .writeInt32(GENERATION_ID)
    .writeString(protocolName)
    .writeString(leaderId)
    .writeString(memberId)
    .writeArray(
      members.map((member) =>
        new Encoder()
          .writeString(member.memberId)
          .writeString(
            member.groupInstanceId === undefined ? GROUP_INSTANCE_ID : member.groupInstanceId
          )
          .writeBytes(member.metadata)
      )
    ).buffer

const joinGroupResponse = (protocolName = 'RoundRobinAssigner') => joinResponse({ protocolName })

const syncGroupResponse = () =>
  new Encoder().writeInt32(0).writeInt16(0).writeBytes(MEMBER_ASSIGNMENT).buffer

const emptyResponse = () => new Encoder().writeInt32(0).writeInt16(0).buffer

const leaveGroupResponse = () => new Encoder().writeInt32(0).writeInt16(0).writeArray([]).buffer

const answer = ({ apiKey }) => {
  switch (apiKey) {
    case apiKeys.JoinGroup:
      return joinGroupResponse()
    case apiKeys.SyncGroup:
      return syncGroupResponse()
    case apiKeys.Heartbeat:
      return emptyResponse()
    case apiKeys.LeaveGroup:
      return leaveGroupResponse()
    default:
      throw new Error(`unexpected apiKey ${apiKey}`)
  }
}

/**
 * Reads a committed frame fixture.
 * @param {string} name
 * @returns {{ apiKey: number, apiVersion: number, body: string }}
 */
const readFixture = (name) =>
  JSON.parse(fs.readFileSync(path.join(FIXTURES, `${name}.json`), 'utf8'))

/**
 * Drives the four classic group requests once each, in the order a real member makes them.
 * @returns {Promise<(apiKey: number) => { apiKey: number, apiVersion: number, body: Buffer }>}
 */
const recordFrames = async () => {
  const { broker, bodyOf } = await createFakeCoordinator(answer)

  const assigner = {
    name: 'RoundRobinAssigner',
    protocol: () => ({ name: 'RoundRobinAssigner', metadata: PROTOCOL_METADATA }),
    assign: async () => [{ memberId: MEMBER_ID, memberAssignment: MEMBER_ASSIGNMENT }],
  }

  const protocol = new ClassicGroupProtocol({
    cluster: {
      findGroupCoordinator: async () => broker,
      refreshMetadata: async () => {},
      findTopicPartitionMetadata: () => [],
    },
    groupId: GROUP_ID,
    groupInstanceId: GROUP_INSTANCE_ID,
    sessionTimeout: SESSION_TIMEOUT,
    rebalanceTimeout: REBALANCE_TIMEOUT,
    assigners: [assigner],
    instrumentationEmitter: new InstrumentationEventEmitter(),
    logger: newLogger(),
  })

  // The recorded frames belong to a member that already has an id; a very first join would send
  // an empty one and produce different bytes.
  protocol.memberId = MEMBER_ID

  await protocol.rebalance({
    topics: TOPICS,
    topicsSubscribed: TOPICS,
    beforeSync: () => {},
    round: realRound(),
  })
  await protocol.heartbeat({ interval: 0 })
  await protocol.leave()

  return bodyOf
}

/**
 * A bare classic protocol, with only the constructor fields it actually reads.
 * @param {Object} [overrides] merged last, so a case can set `heartbeatInterval`
 * @returns {ClassicGroupProtocol}
 */
const buildProtocol = (overrides = {}) =>
  new ClassicGroupProtocol({
    cluster: {},
    groupId: 'seam-group',
    groupInstanceId: null,
    sessionTimeout: SESSION_TIMEOUT,
    rebalanceTimeout: REBALANCE_TIMEOUT,
    assigners: [],
    instrumentationEmitter: new InstrumentationEventEmitter(),
    logger: newLogger(),
    ...overrides,
  })

/**
 * A `ConsumerGroup` with a stub cluster, used to exercise the membership accessors and the
 * join/sync retry contract without a broker.
 * @param {number} retries
 * @returns {ConsumerGroup}
 */
const buildAccessorGroup = (retries) =>
  new ConsumerGroup({
    retry: { retries },
    cluster: {
      connect: async () => {},
      refreshMetadata: async () => {},
      refreshMetadataIfNecessary: async () => {},
      committedOffsets: () => ({}),
      findTopicPartitionMetadata: () => [],
    },
    groupId: 'accessor-group',
    topics: ['accessor-topic'],
    topicConfigurations: { 'accessor-topic': {} },
    logger: newLogger(),
    instrumentationEmitter: new InstrumentationEventEmitter(),
    assigners: [],
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
    groupInstanceId: 'accessor-instance',
    metadataMaxAge: 300000,
  })

describe('Consumer > classic group protocol > golden frames', () => {
  const CASES = [
    ['joinGroup_v5_request', apiKeys.JoinGroup, 5],
    ['syncGroup_v3_request', apiKeys.SyncGroup, 3],
    ['heartbeat_v3_request', apiKeys.Heartbeat, 3],
    ['leaveGroup_v3_request', apiKeys.LeaveGroup, 3],
  ]

  let bodyOf

  beforeEach(async () => {
    bodyOf = await recordFrames()
  })

  test.each(CASES)('%s is byte-identical to the recorded frame', (name, apiKey, apiVersion) => {
    const fixture = readFixture(name)
    const written = bodyOf(apiKey)

    expect(written.apiKey).toEqual(fixture.apiKey)
    expect(written.apiVersion).toEqual(fixture.apiVersion)
    expect(written.apiVersion).toEqual(apiVersion)
    expect(written.body.toString('hex')).toEqual(fixture.body)
  })

  it('negotiates the pinned versions rather than whatever the client implements', () => {
    // The negative control for the pinned ApiVersions table: if the table were ignored, a future
    // JoinGroup v6 in this repo would silently re-record every frame.
    expect(CASES.map(([, apiKey]) => bodyOf(apiKey).apiVersion)).toEqual([5, 3, 3, 3])
  })
})

describe('Consumer > classic group protocol > membership accessors', () => {
  const FIELDS = [
    ['memberId', 'member-9'],
    ['generationId', 42],
    ['leaderId', 'leader-9'],
    ['members', [{ memberId: 'member-9' }]],
    ['groupProtocol', 'RoundRobinAssigner'],
    ['groupInstanceId', null],
    ['coordinator', { heartbeat: () => {} }],
    ['lastRequest', 0],
  ]

  test.each(FIELDS)('%s reads and writes through to the protocol', (field, value) => {
    const group = buildAccessorGroup(0)

    group[field] = value

    expect(group.groupMembership[field]).toEqual(value)
    expect(group[field]).toEqual(value)
  })

  it('keeps warnIfStaticMembershipInactive callable on the group', () => {
    const group = buildAccessorGroup(0)
    group.coordinator = { negotiatedVersion: () => 4 }
    // The group's logger object is the protocol's logger object: it is not re-namespaced, so a
    // spy installed here observes what the protocol writes.
    const warn = jest.spyOn(group.logger, 'warn')

    group.warnIfStaticMembershipInactive()

    expect(warn).toHaveBeenCalledWith(
      'groupInstanceId is set but the broker negotiated JoinGroup v4 - static membership is inactive',
      expect.objectContaining({ negotiatedVersion: 4 })
    )
  })
})

describe('Consumer > classic group protocol > joinAndSync contract', () => {
  const rebalancing = () => {
    const error = new Error('rebalance in progress')
    error.type = 'REBALANCE_IN_PROGRESS'
    error.code = 27
    return error
  }

  const buildGroup = (retries) => {
    const group = buildAccessorGroup(retries)
    group.applyRebalanceResult = jest.fn(async () => ({
      assignment: [],
      added: [],
      revoked: [],
    }))
    group.assigned = () => []
    return group
  }

  it('retries the whole join+sync sequence on a rebalance error and returns the sync result', async () => {
    const group = buildGroup(2)
    const rebalance = jest
      .spyOn(group.groupMembership, 'rebalance')
      .mockRejectedValueOnce(rebalancing())
      .mockResolvedValueOnce({ assignment: { 'accessor-topic': [0] } })

    await expect(group.joinAndSync()).resolves.toEqual({
      assignment: { 'accessor-topic': [0] },
    })
    expect(rebalance).toHaveBeenCalledTimes(2)
  })

  it('clears the member id and retries on UNKNOWN_MEMBER_ID', async () => {
    const group = buildGroup(2)
    group.memberId = 'stale-member'
    const unknown = new Error('unknown member')
    unknown.type = 'UNKNOWN_MEMBER_ID'

    jest
      .spyOn(group.groupMembership, 'rebalance')
      .mockRejectedValueOnce(unknown)
      .mockResolvedValueOnce({ assignment: {} })

    await group.joinAndSync()

    // The remap ran before the retry: the stale id was dropped, so the second attempt joined
    // with an empty member id and took whatever the coordinator handed back.
    expect(group.memberId).toBeNull()
    expect(group.groupMembership.rebalance).toHaveBeenCalledTimes(2)
  })

  it('retries a lost connection without spending a retry while the outage is shorter than the session timeout', async () => {
    const group = buildGroup(0)
    const lost = new KafkaJSConnectionError('Connection error: connect ECONNREFUSED', {
      code: 'ECONNREFUSED',
    })
    const rebalance = jest
      .spyOn(group.groupMembership, 'rebalance')
      .mockRejectedValueOnce(lost)
      .mockResolvedValueOnce({ assignment: {} })

    await expect(group.joinAndSync()).resolves.toEqual({ assignment: {} })
    expect(rebalance).toHaveBeenCalledTimes(2)
  })

  it.each([
    [
      'a lost connection',
      () =>
        new KafkaJSConnectionError('Connection error: connect ECONNREFUSED', {
          code: 'ECONNREFUSED',
        }),
    ],
    ['COORDINATOR_NOT_AVAILABLE (15)', () => createErrorFromCode(15)],
    ['COORDINATOR_LOAD_IN_PROGRESS (14)', () => createErrorFromCode(14)],
  ])(
    'ends the join as a stop when a stop arrives during an attempt that fails with %s after the outage bound',
    async (_name, failed) => {
      const group = buildGroup(0)
      group.sessionTimeout = 10
      group.backoff = { initial: () => 0 }
      const rebalance = jest
        .spyOn(group.groupMembership, 'rebalance')
        .mockRejectedValueOnce(failed())
        .mockImplementationOnce(async () => {
          group.rebalanceCoordinator = { runner: { stopRequested: true } }
          await sleep(30)
          throw failed()
        })

      await expect(group.joinAndSync()).rejects.toMatchObject({ type: 'CONSUMER_STOPPING' })
      expect(rebalance).toHaveBeenCalledTimes(2)
    }
  )

  it("leaves a consumer-protocol member's lost connection to the counted retries", async () => {
    const group = buildGroup(0)
    Object.defineProperty(group, 'kind', { value: 'consumer' })
    const lost = new KafkaJSConnectionError('Connection error: connect ECONNREFUSED', {
      code: 'ECONNREFUSED',
    })
    jest.spyOn(group.groupMembership, 'rebalance').mockRejectedValue(lost)

    await expect(group.joinAndSync()).rejects.toBe(lost)
    expect(group.groupMembership.rebalance).toHaveBeenCalledTimes(1)
  })

  it('bails without retrying on any other error', async () => {
    const group = buildGroup(5)
    jest
      .spyOn(group.groupMembership, 'rebalance')
      .mockRejectedValue(new Error('assigner not found'))

    await expect(group.joinAndSync()).rejects.toThrow('assigner not found')
    expect(group.groupMembership.rebalance).toHaveBeenCalledTimes(1)
  })

  it('is one exchange: it neither applies the result nor reports the round', async () => {
    const group = buildGroup(0)
    const result = { assignment: { 'accessor-topic': [0] }, userData: null }
    jest.spyOn(group.groupMembership, 'rebalance').mockResolvedValue(result)
    // `InstrumentationEventEmitter` exposes only `emit` and `addListener` - there is no `on`,
    // and calling one is a TypeError, not a failing assertion.
    const groupJoin = jest.fn()
    group.instrumentationEmitter.addListener(GROUP_JOIN, groupJoin)

    await expect(group.joinAndSync()).resolves.toEqual(result)

    expect(group.applyRebalanceResult).not.toHaveBeenCalled()
    expect(groupJoin).not.toHaveBeenCalled()
  })
})

describe('Consumer > groupProtocol > ClassicGroupProtocol > the seam members the two protocols share', () => {
  it('identifies itself as the classic protocol', () => {
    expect(buildProtocol().kind).toEqual('classic')
  })

  it('reports the configured heartbeat interval, and nothing at all when it has none', () => {
    expect(buildProtocol({ heartbeatInterval: 5000 }).heartbeatInterval()).toEqual(5000)
    // Not a default of its own: an unconfigured protocol leaves the cadence to whoever schedules
    // the heartbeats, which is the interval that consumer was already built with.
    expect(buildProtocol().heartbeatInterval()).toBeNull()
  })

  it('answers the new seam members without doing anything', async () => {
    const protocol = buildProtocol()

    expect(protocol.needsReconciliation()).toBe(false)
    expect(protocol.onMembershipError(new Error('boom'))).toBeUndefined()
    expect(protocol.onStopRequested()).toBeUndefined()
    await expect(protocol.leaveForPollExpiry()).resolves.toBeUndefined()
  })
})

describe('Consumer > classic group protocol > the leader round guards', () => {
  const refusal = new Error('round retired')

  /** @param {number} [refuseFrom] 1-based guard call from which the round refuses */
  const fakeRound = (refuseFrom = Infinity, overrides = {}) => {
    let calls = 0
    return {
      guard: jest.fn(() => (++calls >= refuseFrom ? refusal : null)),
      wait: (operation) => operation(),
      roundDeadlineAt: performance.now() + 60000,
      allowAutoTopicCreation: true,
      ...overrides,
    }
  }

  const leader = (overrides = {}) => {
    const protocol = buildProtocol({
      cluster: {
        findTopicPartitionMetadata: () => [],
        refreshMetadata: async () => {},
        groupMetadata: async () => ({ topicMetadata: [], brokers: [] }),
      },
      logger: newLogger(),
      ...overrides,
    })
    Object.assign(protocol, {
      leaderId: 'm1',
      memberId: 'm1',
      groupProtocol: 'custom',
      selectedAssigner: protocol.assigners[0] ?? null,
      members: [],
    })
    return protocol
  }

  it('refuses before reading any metadata when the round already retired', async () => {
    const protocol = leader()
    const findTopicPartitionMetadata = jest.spyOn(protocol.cluster, 'findTopicPartitionMetadata')
    await expect(protocol.leaderMetadata([], ['a'], fakeRound(1))).rejects.toBe(refusal)
    expect(findTopicPartitionMetadata).not.toHaveBeenCalled()
  })

  it('throws the guard error when the round deadline passed before the group topics load', async () => {
    const protocol = leader()
    const groupMetadata = jest.spyOn(protocol.cluster, 'groupMetadata')
    const members = [
      { memberId: 'm2', memberMetadata: MemberMetadata.encode({ version: 0, topics: ['b'] }) },
    ]
    const round = fakeRound(2, { roundDeadlineAt: performance.now() - 1 })
    await expect(protocol.leaderMetadata(members, ['a'], round)).rejects.toBe(refusal)
    expect(groupMetadata).not.toHaveBeenCalled()
  })

  it('keeps only requested, healthy group topics from the snapshot', async () => {
    const protocol = leader()
    const rows = [{ partitionId: 0, leader: 1, replicas: [1] }]
    const groupMetadata = jest.spyOn(protocol.cluster, 'groupMetadata').mockResolvedValue({
      brokers: [],
      topicMetadata: [
        { topic: 'b', topicErrorCode: 0, partitionMetadata: rows },
        { topic: 'broken', topicErrorCode: 3, partitionMetadata: [] },
        { topic: 'unrequested', topicErrorCode: 0, partitionMetadata: rows },
      ],
    })
    const warn = jest.spyOn(protocol.logger, 'warn')
    const members = [
      {
        memberId: 'm2',
        memberMetadata: MemberMetadata.encode({ version: 0, topics: ['b', 'broken'] }),
      },
    ]
    const { groupTopics, groupTopicMetadata } = await protocol.leaderMetadata(
      members,
      ['a'],
      fakeRound()
    )
    expect(groupTopics).toEqual(['a', 'b', 'broken'])
    expect([...groupTopicMetadata.keys()].sort()).toEqual(['a', 'b'])
    expect(groupMetadata).toHaveBeenCalledWith(expect.objectContaining({ topics: ['b', 'broken'] }))
    expect(warn).toHaveBeenCalledWith('Skipping unavailable group topic', {
      topic: 'broken',
      topicErrorCode: 3,
    })
  })

  it('refuses at the end of the snapshot when the round retired meanwhile', async () => {
    await expect(leader().leaderMetadata([], ['a'], fakeRound(2))).rejects.toBe(refusal)
  })

  it('fails the leader sync when the coordinator picked an assignor this member lacks', async () => {
    const protocol = leader()
    await expect(
      protocol.sync({
        topics: ['a'],
        topicsSubscribed: ['a'],
        beforeSync: jest.fn(),
        round: fakeRound(),
      })
    ).rejects.toThrow('Unsupported partition assigner "custom"')
  })

  it('drops the selected assigner of the joined membership with its leader and selected protocol when the generation is reset', () => {
    const protocol = leader({ assigners: [{ name: 'custom', assign: async () => [] }] })
    expect(protocol.selectedAssigner).toBe(protocol.assigners[0])

    protocol.resetGeneration()

    expect(protocol.leaderId).toBeNull()
    expect(protocol.groupProtocol).toBeNull()
    expect(protocol.selectedAssigner).toBeNull()
  })

  it('does not upload an assignment once the round retired during the assign window', async () => {
    const syncGroup = jest.fn()
    const beforeSync = jest.fn()
    const protocol = leader({ assigners: [{ name: 'custom', assign: async () => [] }] })
    protocol.coordinator = { syncGroup }
    await expect(
      protocol.sync({ topics: ['a'], topicsSubscribed: ['a'], beforeSync, round: fakeRound(3) })
    ).rejects.toBe(refusal)
    expect(beforeSync).not.toHaveBeenCalled()
    expect(syncGroup).not.toHaveBeenCalled()
  })

  it('ignores an undecodable assigned buffer and still names the owners of a real conflict', () => {
    const protocol = leader()
    const owned = MemberMetadata.encode({
      version: 1,
      topics: ['a'],
      userData: Buffer.alloc(0),
      ownedPartitions: [{ topic: 'a', partitions: [0] }],
    })
    const members = [{ memberId: 'm1', memberMetadata: owned }]
    const taken = MemberAssignment.encode({
      version: 1,
      assignment: { a: [0] },
      userData: Buffer.alloc(0),
    })
    const assignment = [
      { memberId: 'm3', memberAssignment: Buffer.alloc(0) },
      { memberId: 'm2', memberAssignment: taken },
    ]
    expect(() => protocol.ownershipOf(members).assertPlan(assignment)).toThrow(
      'a-0 to m2 while m1 still owns it'
    )
  })
})

describe('Consumer > classic group protocol > coordinator publication', () => {
  /** @returns {object} a real membership and lookup held at the cluster boundary */
  const heldLookup = () => {
    let resolve
    const answer = new Promise((done) => (resolve = done))
    const ownership = new GroupOwnership()
    const protocol = buildProtocol({
      ownership,
      cluster: { findGroupCoordinator: ({ fence }) => fence.wait(answer) },
    })
    protocol.coordinatorUnknown = true
    return { protocol, ownership, resolve }
  }

  it.each(['caller fence', 'lookup retirement'])(
    'publishes no coordinator after %s refuses a pending lookup',
    async (reason) => {
      const { protocol, resolve } = heldLookup()
      const fence = new Generation()
      const refusal = new Error('lookup refused')
      const pending = protocol.connect({ fence }).catch((error) => error)
      if (reason === 'caller fence') fence.close(refusal)
      else protocol.coordinatorLookup.retire(refusal)
      resolve({ nodeId: 'obsolete' })

      expect(await pending).toBe(refusal)
      expect(protocol.coordinator).toBeNull()
      expect(protocol.coordinatorUnknown).toBe(true)
    }
  )

  it('checks the caller fence again after the lookup resolves before publishing', async () => {
    const { protocol, resolve } = heldLookup()
    const fence = new Generation()
    const refusal = new Error('caller retired before publication')
    const find = protocol.coordinatorLookup.find.bind(protocol.coordinatorLookup)
    jest.spyOn(protocol.coordinatorLookup, 'find').mockImplementation(async (caller) => {
      const coordinator = await find(caller)
      fence.close(refusal)
      return coordinator
    })
    const pending = protocol.connect({ fence }).catch((error) => error)
    resolve({ nodeId: 'obsolete' })

    expect(await pending).toBe(refusal)
    expect(protocol.coordinator).toBeNull()
    expect(protocol.coordinatorUnknown).toBe(true)
  })

  it.each(['membership retirement', 'ownership replacement'])(
    'keeps the replacement coordinator when %s overtakes a lookup',
    async (reason) => {
      const { protocol, ownership, resolve } = heldLookup()
      const pending = protocol.connect().catch((error) => error)
      if (reason === 'membership retirement') protocol.forgetMemberId()
      else ownership.renew()
      const current = { nodeId: 'current' }
      protocol.coordinator = current
      resolve({ nodeId: 'obsolete' })

      expect(await pending).toMatchObject({ type: 'JOIN_DEADLINE_EXCEEDED' })
      expect(protocol.coordinator).toBe(current)
      expect(protocol.coordinatorUnknown).toBe(true)
    }
  )
})

describe('Consumer > classic group protocol > a heartbeat that loses its coordinator', () => {
  const {
    KafkaJSConnectionError,
    KafkaJSConnectionClosedError,
    KafkaJSRequestTimeoutError,
  } = require('../../errors')
  const { createErrorFromCode } = require('../../protocol/error')

  /**
   * A protocol whose first coordinator fails every heartbeat with `failure` and whose next lookup
   * finds a healthy one.
   * @param {() => Error} failure
   */
  const movedCoordinator = async (failure) => {
    const old = {
      heartbeat: jest.fn(async () => Promise.reject(failure())),
      isConnected: () => true,
    }
    const current = { heartbeat: jest.fn(async () => {}), isConnected: () => true }
    const found = [old, current]
    const cluster = { findGroupCoordinator: jest.fn(async () => found.shift() ?? current) }
    const protocol = buildProtocol({ cluster })
    await protocol.connect()
    Object.assign(protocol, { memberId: 'seam-member', generationId: 1 })
    return { protocol, cluster, old, current }
  }

  it.each([
    [
      'its connection closed',
      () => new KafkaJSConnectionClosedError('Closed connection', { host: 'old', port: 9092 }),
    ],
    [
      'it could not be reached',
      () =>
        new KafkaJSConnectionError('Connection error: connect ECONNREFUSED', {
          broker: 'old:9092',
          code: 'ECONNREFUSED',
        }),
    ],
    [
      'it timed out',
      () =>
        new KafkaJSRequestTimeoutError('Request Heartbeat(key: 12, version: 4) timed out', {
          broker: 'old:9092',
        }),
    ],
    ['it answered COORDINATOR_NOT_AVAILABLE', () => createErrorFromCode(15)],
    ['it answered NOT_COORDINATOR', () => createErrorFromCode(16)],
  ])('looks the coordinator up again before the next heartbeat when %s', async (_name, failure) => {
    const { protocol, cluster, old, current } = await movedCoordinator(failure)

    await expect(protocol.heartbeat({ interval: 0 })).rejects.toThrow()
    await protocol.heartbeat({ interval: 0 })
    await protocol.heartbeat({ interval: 0 })

    expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(2)
    expect(old.heartbeat).toHaveBeenCalledTimes(1)
    expect(current.heartbeat).toHaveBeenCalledTimes(2)
  })

  it('looks the coordinator up again before a heartbeat when its connection is no longer open', async () => {
    let connected = true
    const old = { heartbeat: jest.fn(async () => {}), isConnected: () => connected }
    const current = { heartbeat: jest.fn(async () => {}), isConnected: () => true }
    const found = [old, current]
    const cluster = { findGroupCoordinator: jest.fn(async () => found.shift() ?? current) }
    const protocol = buildProtocol({ cluster })
    await protocol.connect()
    Object.assign(protocol, { memberId: 'seam-member', generationId: 1 })

    await protocol.heartbeat({ interval: 0 })
    connected = false
    await protocol.heartbeat({ interval: 0 })

    expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(2)
    expect(old.heartbeat).toHaveBeenCalledTimes(1)
    expect(current.heartbeat).toHaveBeenCalledTimes(1)
  })

  it('sends no leave, and looks for no coordinator, while its last heartbeat left the coordinator unknown', async () => {
    const { protocol, cluster, old } = await movedCoordinator(() => createErrorFromCode(15))
    old.leaveGroup = jest.fn(async () => {})
    const debug = jest.spyOn(protocol.logger, 'debug')
    await expect(protocol.heartbeat({ interval: 0 })).rejects.toThrow()

    await protocol.leave()

    expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
    expect(old.leaveGroup).not.toHaveBeenCalled()
    expect(debug).toHaveBeenCalledWith(
      'Leaving without a LeaveGroup: no coordinator is known',
      expect.any(Object)
    )
    expect(protocol.memberId).toBeNull()
  })

  it('sends its leave to a coordinator it looks up again when the cached one was retired', async () => {
    const old = { retired: true, leaveGroup: jest.fn(async () => {}), isConnected: () => true }
    const current = { leaveGroup: jest.fn(async () => {}), isConnected: () => true }
    const found = [old, current]
    const cluster = { findGroupCoordinator: jest.fn(async () => found.shift()) }
    const protocol = buildProtocol({ cluster })
    await protocol.connect()
    protocol.memberId = 'seam-member'

    await protocol.leave()

    expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(2)
    expect(old.leaveGroup).not.toHaveBeenCalled()
    expect(current.leaveGroup).toHaveBeenCalledTimes(1)
  })

  it('keeps its coordinator after a heartbeat the coordinator answered as a member error', async () => {
    const { protocol, cluster, old } = await movedCoordinator(() => createErrorFromCode(27))

    await expect(protocol.heartbeat({ interval: 0 })).rejects.toThrow()
    await expect(protocol.heartbeat({ interval: 0 })).rejects.toThrow()

    expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
    expect(old.heartbeat).toHaveBeenCalledTimes(2)
  })
})

describe('Consumer > classic group protocol > a heartbeat answered after its caller refuses', () => {
  const {
    events: { HEARTBEAT },
  } = require('../instrumentationEvents')

  /**
   * A joined member whose heartbeat is written and held until the test answers it; `refuse` makes
   * the caller's guard refuse from then on.
   */
  const heldHeartbeat = async () => {
    const held = {}
    const coordinator = {
      heartbeat: jest.fn(
        () =>
          new Promise((resolve, reject) => {
            Object.assign(held, { resolve, reject })
          })
      ),
      isConnected: () => true,
    }
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const events = jest.fn()
    instrumentationEmitter.addListener(HEARTBEAT, events)
    const protocol = buildProtocol({
      cluster: { findGroupCoordinator: jest.fn(async () => coordinator) },
      instrumentationEmitter,
    })
    await protocol.connect()
    Object.assign(protocol, { memberId: 'seam-member', generationId: 1, lastRequest: 0 })
    let refusal = null
    const call = protocol.heartbeat({ interval: 0, guard: () => refusal })
    call.catch(() => {})
    for (let i = 0; i < 20 && coordinator.heartbeat.mock.calls.length === 0; i++) {
      await Promise.resolve()
    }
    expect(coordinator.heartbeat).toHaveBeenCalledTimes(1)
    return {
      protocol,
      call,
      events,
      held,
      refuse: (error) => {
        refusal = error
      },
    }
  }

  it('a success answered after the refusal emits no HEARTBEAT and keeps the last request time', async () => {
    const { protocol, call, events, held, refuse } = await heldHeartbeat()
    const stopped = new Error('stopped')

    refuse(stopped)
    held.resolve()

    await expect(call).rejects.toBe(stopped)
    expect(events).not.toHaveBeenCalled()
    expect(protocol.lastRequest).toBe(0)
  })

  it('a coordinator error answered after the refusal leaves the coordinator known', async () => {
    const { protocol, call, events, held, refuse } = await heldHeartbeat()
    const stopped = new Error('stopped')

    refuse(stopped)
    held.reject(createErrorFromCode(15))

    await expect(call).rejects.toBe(stopped)
    expect(protocol.coordinatorUnknown).toBe(false)
    expect(events).not.toHaveBeenCalled()
  })
})

describe('Consumer > classic group protocol > the idle heartbeat probe', () => {
  const ConsumerProtocol = require('../groupProtocol/consumer')
  const INTERVAL = 3000
  let now

  /** A connected protocol whose coordinator answers every heartbeat with `answer()`. */
  const joinedProtocol = (answer = async () => {}) => {
    const coordinator = { heartbeat: jest.fn(answer), isConnected: () => true }
    const protocol = buildProtocol()
    protocol.coordinator = coordinator
    Object.assign(protocol, { memberId: 'seam-member', generationId: 1 })
    protocol.lastRequest = Date.now()
    return { protocol, coordinator }
  }

  beforeEach(() => {
    now = 1_000_000
    jest.spyOn(Date, 'now').mockImplementation(() => now)
  })

  afterEach(() => jest.restoreAllMocks())

  it('turns false exactly when heartbeat() sends, and true again once the heartbeat is answered', async () => {
    const { protocol, coordinator } = joinedProtocol()

    now += INTERVAL - 1
    expect(protocol.heartbeatIdle(INTERVAL)).toBe(true)
    await protocol.heartbeat({ interval: INTERVAL })
    expect(coordinator.heartbeat).not.toHaveBeenCalled()

    now += 1
    expect(protocol.heartbeatIdle(INTERVAL)).toBe(false)
    await protocol.heartbeat({ interval: INTERVAL })
    expect(coordinator.heartbeat).toHaveBeenCalledTimes(1)
    expect(protocol.lastRequest).toBe(now)
    expect(protocol.heartbeatIdle(INTERVAL)).toBe(true)
  })

  it('is idle without a joined member, however long since the last request, as heartbeat() then sends nothing', async () => {
    const { protocol, coordinator } = joinedProtocol()
    protocol.memberId = null

    now += 10 * INTERVAL
    expect(protocol.heartbeatIdle(INTERVAL)).toBe(true)
    await protocol.heartbeat({ interval: INTERVAL })
    expect(coordinator.heartbeat).not.toHaveBeenCalled()
  })

  it('is not idle while a heartbeat is in flight, and a not-due call joins it and rejects with its error', async () => {
    let fail, onWire
    const written = new Promise((resolve) => (onWire = resolve))
    const { protocol, coordinator } = joinedProtocol(
      () =>
        new Promise((_resolve, reject) => {
          fail = reject
          onWire()
        })
    )
    const failure = createErrorFromCode(27)

    now += INTERVAL
    const sent = protocol.heartbeat({ interval: INTERVAL })
    expect(protocol.heartbeatIdle(Infinity)).toBe(false)
    const joined = protocol.heartbeat({ interval: Infinity })
    await written
    expect(coordinator.heartbeat).toHaveBeenCalledTimes(1)
    const sentOutcome = sent.then(
      () => 'resolved',
      (e) => e
    )
    const joinedOutcome = joined.then(
      () => 'resolved',
      (e) => e
    )
    fail(failure)

    expect(await sentOutcome).toBe(failure)
    expect(await joinedOutcome).toBe(failure)
    expect(coordinator.heartbeat).toHaveBeenCalledTimes(1)
    expect(protocol.heartbeatIdle(Infinity)).toBe(true)
  })

  it('leaves the consumer-protocol member out: its group is never idle, so every call reaches it', () => {
    expect('heartbeatIdle' in ConsumerProtocol.prototype).toBe(false)
    const forward = (groupMembership) =>
      ConsumerGroup.prototype.heartbeatIdle.call({ groupMembership }, INTERVAL)

    expect(forward(Object.create(ConsumerProtocol.prototype))).toBe(false)
    expect(forward({ heartbeatIdle: (interval) => interval === INTERVAL })).toBe(true)
  })
})

describe('Consumer > classic group protocol > the protocols a join offers', () => {
  /** @returns {jest.Mock} an `assign` that gives the one member its golden assignment */
  const assigns = () =>
    jest.fn(async () => [{ memberId: MEMBER_ID, memberAssignment: MEMBER_ASSIGNMENT }])

  /**
   * @param {string} declared the assigner's own `name`
   * @param {string} offered the name its `protocol()` returns
   * @param {jest.Mock} [assign]
   * @returns {object} an assigner whose declared and offered names can differ
   */
  const assigner = (declared, offered, assign = assigns()) => ({
    name: declared,
    protocol: () => ({ name: offered, metadata: PROTOCOL_METADATA }),
    assign,
  })

  /**
   * One classic round, over a real broker and socket, against a coordinator that selects `selected`.
   * @param {object[]} assigners
   * @param {string} selected the protocol name the JoinGroup answer carries
   * @param {object} [options]
   * @param {string} [options.mode] the rebalance protocol of the member
   * @param {(joinNumber: number) => Buffer} [options.joinAnswers] the answer to the nth JoinGroup,
   *   counted from 1; the plain selection of `selected` when absent
   * @param {(broker: object) => void} [options.prepare] called with the real broker before the round
   * @param {number} [options.partitions=0] how many partitions the cluster knows for each topic
   * @returns {Promise<{ outcome: Promise<object>, bodyOf: Function, protocol: ClassicGroupProtocol }>}
   *   the round's result, the reader of the frames written and the member
   */
  const roundSelecting = async (
    assigners,
    selected,
    {
      mode,
      joinAnswers = () => joinGroupResponse(selected),
      prepare = () => {},
      partitions = 0,
    } = {}
  ) => {
    let joins = 0
    const { broker, bodyOf } = await createFakeCoordinator((header) =>
      header.apiKey === apiKeys.JoinGroup ? joinAnswers((joins += 1)) : answer(header)
    )
    prepare(broker)
    const protocol = new ClassicGroupProtocol({
      cluster: {
        findGroupCoordinator: async () => broker,
        refreshMetadata: async () => {},
        findTopicPartitionMetadata: () =>
          Array.from({ length: partitions }, (_, partitionId) => ({
            partitionId,
            leader: 0,
            replicas: [0],
          })),
      },
      groupId: GROUP_ID,
      groupInstanceId: null,
      sessionTimeout: SESSION_TIMEOUT,
      rebalanceTimeout: REBALANCE_TIMEOUT,
      assigners,
      mode,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: newLogger(),
    })
    const outcome = protocol.rebalance({
      topics: TOPICS,
      topicsSubscribed: TOPICS,
      beforeSync: () => {},
      round: realRound(),
    })
    outcome.catch(() => {})
    return { outcome, bodyOf, protocol }
  }

  it('keeps only the selected assigner once the join is installed, not the offers and the metadata they copied', async () => {
    const chosen = assigner('declared-name', 'offered-name')

    const { outcome, protocol } = await roundSelecting([chosen], 'offered-name')

    await outcome
    expect(protocol.selectedAssigner).toBe(chosen)
    expect(Object.values(protocol).filter((value) => value instanceof AssignerOffers)).toEqual([])
  })

  it('refuses two assigners whose offered names, not their declared ones, are written as the same bytes, and writes no JoinGroup', async () => {
    const first = assigner('first', 'planner-\uD800')
    const second = assigner('second', 'planner-\uDBFF')

    const { outcome, bodyOf } = await roundSelecting([first, second], 'planner-�')

    await expect(outcome).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
    await expect(outcome).rejects.toThrow(
      'The partition assigners "planner-\uD800" and "planner-\uDBFF" are both sent as "planner-�", so the group cannot tell them apart'
    )
    expect(() => bodyOf(apiKeys.JoinGroup)).toThrow('no request written')
    expect(first.assign).not.toHaveBeenCalled()
    expect(second.assign).not.toHaveBeenCalled()
  })

  it('hands the round to the assigner that offered the name the coordinator selected, whatever it declares', async () => {
    const chosen = assigner('declared-name', 'offered-name')
    const other = assigner('other-declared', 'other-offered')

    const { outcome } = await roundSelecting([other, chosen], 'offered-name')

    await expect(outcome).resolves.toMatchObject({ assignment: expect.any(Object) })
    expect(chosen.assign).toHaveBeenCalledTimes(1)
    expect(other.assign).not.toHaveBeenCalled()
  })

  it('finds the one assigner whose offered name holds a lone surrogate when the coordinator selects the name as it was written', async () => {
    const lone = assigner('planner-\uD800', 'planner-\uD800')

    const { outcome } = await roundSelecting([lone], 'planner-�')

    await expect(outcome).resolves.toMatchObject({ assignment: expect.any(Object) })
    expect(lone.assign).toHaveBeenCalledTimes(1)
  })

  it('keeps the first of two assigners that offer the same name', async () => {
    const first = assigner('twin', 'twin')
    const second = assigner('twin', 'twin')

    const { outcome } = await roundSelecting([first, second], 'twin')

    await outcome
    expect(first.assign).toHaveBeenCalledTimes(1)
    expect(second.assign).not.toHaveBeenCalled()
  })

  it('refuses a selected name no assigner offered, as the assigner list did before', async () => {
    const { outcome } = await roundSelecting([assigner('known', 'known')], 'unknown')

    await expect(outcome).rejects.toThrow(
      'Unsupported partition assigner "unknown", the assigner wasn\'t found in the assigners list'
    )
  })

  it('sends and resolves the offers as the assigners made them when one changes its reused offer during the member-id handshake', async () => {
    const metadata = MemberMetadata.encode({ version: 0, topics: [TOPICS[0]] })
    const offered = Buffer.from(metadata)
    const reused = { name: 'offered-name', metadata }
    const chosen = { name: 'declared-name', protocol: () => reused, assign: assigns() }
    const other = assigner('other-declared', 'other-offered')

    const { outcome, bodyOf } = await roundSelecting([other, chosen], 'offered-name', {
      joinAnswers: (joinNumber) => {
        if (joinNumber > 1) return joinGroupResponse('offered-name')
        reused.name = 'changed-name'
        metadata.fill(0)
        return joinResponse({ errorCode: 79, leaderId: '', memberId: 'assigned-id', members: [] })
      },
    })

    await expect(outcome).resolves.toMatchObject({ assignment: expect.any(Object) })
    const { body } = bodyOf(apiKeys.JoinGroup)
    expect(body.includes('assigned-id')).toBe(true)
    expect(body.includes('offered-name')).toBe(true)
    expect(body.includes(offered)).toBe(true)
    expect(body.includes('changed-name')).toBe(false)
    expect(chosen.assign).toHaveBeenCalledTimes(1)
    expect(other.assign).not.toHaveBeenCalled()
  })

  describe('the leader checks every cooperative plan against the partitions members report owning', () => {
    const A = MEMBER_ID
    const B = 'golden-member-2'
    const C = 'golden-member-3'
    const TOPIC = TOPICS[0]
    const ORDERS = ['as listed', 'reversed']
    const STICKY = 'cooperative-sticky'

    /**
     * One member's subscription to `TOPIC`.
     * @param {{ owned?: number[], version?: number, generation?: number, userData?: Buffer }} [claim]
     *   `generation` is written from version 2 on
     * @returns {Buffer}
     */
    const subscription = ({
      owned = [],
      version = 3,
      generation = -1,
      userData = Buffer.alloc(0),
    } = {}) =>
      MemberMetadata.encode({
        version,
        topics: [TOPIC],
        userData,
        ownedPartitions: [{ topic: TOPIC, partitions: owned }],
        generationId: generation,
      })

    /** @param {number} generation @returns {Buffer} the built-in assigner's four-byte userData */
    const generationData = (generation) => new Encoder().writeInt32(generation).buffer

    /**
     * @param {{[memberId: string]: number[]|null}} byMember null for no topic row at all
     * @param {number} [version=1]
     * @returns {{ memberId: string, memberAssignment: Buffer }[]} plan rows, in this order
     */
    const planOf = (byMember, version = 1) =>
      Object.entries(byMember).map(([memberId, partitions]) => ({
        memberId,
        memberAssignment: MemberAssignment.encode({
          version,
          assignment: partitions === null ? {} : { [TOPIC]: partitions },
        }),
      }))

    /** @returns {Buffer} the bytes the SyncGroup request writes for one plan row */
    const written = ({ memberId, memberAssignment }) =>
      new Encoder().writeString(memberId).writeBytes(memberAssignment).buffer

    /** @returns {object} a cooperative assigner offering `name` whose plan is `rows` */
    const returning = (name, rows) => ({
      name,
      protocol: () => ({ name, metadata: PROTOCOL_METADATA }),
      supportedProtocols: () => ['cooperative'],
      assign: jest.fn(async () => rows),
    })

    /**
     * One leader round whose JoinGroup answer lists the dynamic members of `claims` in that order,
     * or reversed.
     * @returns {Promise<{ outcome: Promise<object>, bodyOf: Function }>}
     */
    const leaderRound = ({
      assigner,
      name,
      claims,
      order,
      mode = COOPERATIVE,
      partitions = 2,
      prepare,
    }) => {
      const members = Object.entries(claims).map(([memberId, claim]) => ({
        memberId,
        groupInstanceId: null,
        metadata: subscription(claim),
      }))
      if (order === 'reversed') members.reverse()
      return roundSelecting([assigner], name, {
        mode,
        partitions,
        prepare,
        joinAnswers: () => joinResponse({ protocolName: name, members }),
      })
    }

    const expectWritten = async (round, rows) => {
      const { outcome, bodyOf } = await round
      await outcome
      const { body } = bodyOf(apiKeys.SyncGroup)
      for (const row of rows) expect(body.includes(written(row))).toBe(true)
    }

    const expectRefused = async (round, message) => {
      const { outcome, bodyOf } = await round
      await expect(outcome).rejects.toThrow(message)
      await expect(outcome).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
      expect(() => bodyOf(apiKeys.SyncGroup)).toThrow('no request written')
    }

    /** @returns {Array<Array<*>>} each case once per broker member order, the order second */
    const inBothOrders = (cases) =>
      cases.flatMap(([title, ...rest]) => ORDERS.map((order) => [title, order, ...rest]))

    describe('any one claimant may keep a partition several members claim', () => {
      const claims = {
        [A]: { owned: [0], generation: -1, userData: generationData(9) },
        [B]: { owned: [0], generation: -1, userData: Buffer.from('not a generation') },
      }

      test.each(
        inBothOrders([
          ['the leader', A],
          ['the other member', B],
        ])
      )(
        'writes the plan that keeps it with %s, members %s, reading neither GenerationId -1 nor userData',
        async (_label, order, keeper) => {
          const rows = planOf({ [keeper]: [0] })
          await expectWritten(
            leaderRound({ assigner: returning(STICKY, rows), name: STICKY, claims, order }),
            rows
          )
        }
      )
    })

    describe('a custom assigner may keep a partition with a member whose generation only its userData holds', () => {
      test.each(
        inBothOrders([
          ['a version-2 member at GenerationId 5', { owned: [0], version: 2, generation: 5 }],
          ['another version-1 member', { owned: [0], version: 1, userData: generationData(5) }],
        ])
      )('writes the plan beside %s, members %s', async (_label, order, other) => {
        const rows = planOf({ [A]: [0], [B]: [] })
        const claims = { [A]: { owned: [0], version: 1, userData: generationData(9) }, [B]: other }
        await expectWritten(
          leaderRound({ assigner: returning('custom', rows), name: 'custom', claims, order }),
          rows
        )
      })
    })

    describe('a partition its owner claims never goes to a member that does not claim it', () => {
      test.each(
        inBothOrders([
          ['with an empty row for the owner', { [A]: [], [C]: [0] }],
          ['with no row for the owner', { [C]: [0] }],
        ])
      )('refuses the plan %s, members %s, and writes no SyncGroup', async (_label, order, plan) => {
        const claims = { [A]: { owned: [0] }, [C]: {} }
        await expectRefused(
          leaderRound({
            assigner: returning('custom', planOf(plan)),
            name: 'custom',
            claims,
            order,
          }),
          `${TOPIC}-0 to ${C} while ${A} still owns it`
        )
      })
    })

    describe('a partition is in at most one assignment', () => {
      const twice = planOf({ [A]: [0], [B]: [0] })
      const inTwoRows = new Encoder()
        .writeInt16(1)
        .writeArray([
          new Encoder().writeString(TOPIC).writeArray([1]),
          new Encoder().writeString(TOPIC).writeArray([0]),
        ])
        .writeBytes(Buffer.alloc(0)).buffer
      const second = `${TOPIC}-0 to ${B} while ${A} is assigned it too`

      test.each(
        inBothOrders([
          ['nobody claims it', {}, twice, second],
          ['one member claims it', { [A]: [0] }, twice, second],
          ['both members claim it', { [A]: [0], [B]: [0] }, twice, second],
          [
            'the second copy is in a repeated topic row',
            {},
            [...planOf({ [B]: [0] }), { memberId: A, memberAssignment: inTwoRows }],
            `${TOPIC}-0 to ${A} while ${B} is assigned it too`,
          ],
        ])
      )(
        'refuses the plan when %s, members %s, and writes no SyncGroup',
        async (_label, order, owned, rows, message) => {
          const claims = { [A]: { owned: owned[A] ?? [] }, [B]: { owned: owned[B] ?? [] } }
          await expectRefused(
            leaderRound({ assigner: returning('custom', rows), name: 'custom', claims, order }),
            message
          )
        }
      )
    })

    describe('withholding, an unclaimed partition and a sole claimant keeping its partition all pass', () => {
      test.each(
        inBothOrders([
          ['an unclaimed partition goes to one member', {}, { [A]: [0], [B]: [1] }],
          ['the sole claimant keeps its partition', { [A]: [0] }, { [A]: [0], [B]: [1] }],
          [
            'a claimed partition is withheld from everyone',
            { [A]: [0], [B]: [0] },
            { [A]: [1], [B]: [] },
          ],
        ])
      )('writes the plan when %s, members %s', async (_label, order, owned, plan) => {
        const rows = planOf(plan)
        const claims = { [A]: { owned: owned[A] ?? [] }, [B]: { owned: owned[B] ?? [] } }
        await expectWritten(
          leaderRound({ assigner: returning('custom', rows), name: 'custom', claims, order }),
          rows
        )
      })
    })

    describe('the built-in cooperative-sticky assigner passes the same check by construction', () => {
      const sent = (byMember) => planOf(byMember, 3)
      const builtInRound = (claims, order, partitions) =>
        leaderRound({
          assigner: cooperativeSticky({ cluster: {} }),
          name: STICKY,
          claims,
          order,
          partitions,
        })

      test.each(
        inBothOrders([
          [
            'a stale claimant is not handed a partition the newer owner still claims',
            { [A]: { owned: [0, 1], generation: 9 }, [B]: { owned: [1], generation: 2 } },
            2,
            sent({ [A]: [0], [B]: null }),
          ],
          [
            'a stale claimant does not keep a partition the newer owner claims',
            { [A]: { owned: [0], generation: 9 }, [B]: { owned: [0], generation: 2 } },
            1,
            sent({ [A]: [0], [B]: null }),
          ],
          [
            'a tie at the highest generation is withheld from both claimants',
            { [A]: { owned: [0], generation: 5 }, [B]: { owned: [0], generation: 5 } },
            1,
            sent({ [A]: null, [B]: null }),
          ],
          [
            'the newest claimant keeps the partition past an older tie',
            {
              [A]: { owned: [0], generation: 2 },
              [B]: { owned: [0], generation: 2 },
              [C]: { owned: [0], generation: 9 },
            },
            1,
            sent({ [A]: null, [B]: null, [C]: [0] }),
          ],
        ])
      )('writes its plan when %s, members %s', async (_label, order, claims, partitions, rows) => {
        await expectWritten(builtInRound(claims, order, partitions), rows)
      })

      test.each(ORDERS)(
        'writes the tied partition to exactly one member the next round, once both revoked it, members %s',
        async (order) => {
          const claims = { [A]: { generation: 6 }, [B]: { generation: 6 } }
          const { outcome, bodyOf } = await builtInRound(claims, order, 1)
          await outcome
          const { body } = bodyOf(apiKeys.SyncGroup)
          const holders = [A, B].filter((id) => body.includes(written(sent({ [id]: [0] })[0])))
          expect(holders).toHaveLength(1)
        }
      )
    })

    describe('every assigner goes through the same check, whatever its name or object', () => {
      const claims = { [A]: { owned: [0, 1], generation: 4 }, [B]: { generation: 4 } }
      const MOVES = {
        rows: planOf({ [A]: [0], [B]: [1] }),
        message: `${TOPIC}-1 to ${B} while ${A} still owns it`,
      }
      const DUPLICATES = {
        rows: planOf({ [A]: [0, 1], [B]: [1] }),
        message: `${TOPIC}-1 to ${B} while ${A} is assigned it too`,
      }

      /** @returns {object} an assigner under its own name that changes the built-in's plan */
      const wrapping = (rows) => {
        const inner = cooperativeSticky({ cluster: {} })
        return {
          name: 'wrapped-sticky',
          supportedProtocols: () => inner.supportedProtocols(),
          protocol: (offer) => ({ ...inner.protocol(offer), name: 'wrapped-sticky' }),
          assign: jest.fn(async (group) => {
            await inner.assign(group)
            return rows
          }),
          onAssignment: (assignment, metadata) => inner.onAssignment(assignment, metadata),
        }
      }

      const replacedAssign = (rows) =>
        Object.assign(cooperativeSticky({ cluster: {} }), { assign: jest.fn(async () => rows) })

      test.each(
        inBothOrders([
          [
            'the built-in with a replaced assign moves an owned partition',
            () => replacedAssign(MOVES.rows),
            STICKY,
            MOVES.message,
          ],
          [
            'the built-in with a replaced assign duplicates a partition',
            () => replacedAssign(DUPLICATES.rows),
            STICKY,
            DUPLICATES.message,
          ],
          [
            'the built-in with its withhold step replaced moves an owned partition',
            () =>
              Object.assign(cooperativeSticky({ cluster: {} }), {
                withholdTransferringPartitions: () => {},
              }),
            STICKY,
            MOVES.message,
          ],
          [
            'a wrapper of the built-in under another name moves an owned partition',
            () => wrapping(MOVES.rows),
            'wrapped-sticky',
            MOVES.message,
          ],
          [
            'a wrapper of the built-in under another name duplicates a partition',
            () => wrapping(DUPLICATES.rows),
            'wrapped-sticky',
            DUPLICATES.message,
          ],
          [
            'a custom assigner offering the name cooperative-sticky moves an owned partition',
            () => returning(STICKY, MOVES.rows),
            STICKY,
            MOVES.message,
          ],
          [
            'a custom assigner offering the name cooperative-sticky duplicates a partition',
            () => returning(STICKY, DUPLICATES.rows),
            STICKY,
            DUPLICATES.message,
          ],
        ])
      )(
        'refuses the plan when %s, members %s, and writes no SyncGroup',
        async (_label, order, make, name, message) => {
          await expectRefused(leaderRound({ assigner: make(), name, claims, order }), message)
        }
      )
    })

    describe('what is checked and written cannot change after the claims are read', () => {
      test.each(ORDERS)(
        'writes a plan that moves an owned partition unchecked under eager, members %s',
        async (order) => {
          const rows = planOf({ [A]: [0], [B]: [1] })
          const assigner = { ...returning('custom', rows), supportedProtocols: () => ['eager'] }
          const claims = { [A]: { owned: [0, 1] }, [B]: {} }
          await expectWritten(
            leaderRound({ assigner, name: 'custom', claims, order, mode: EAGER }),
            rows
          )
        }
      )

      test.each(ORDERS)(
        'checks the claims the members sent when the assigner edits a subscription in its input, members %s, and writes no SyncGroup',
        async (order) => {
          const assigner = returning('custom', planOf({ [A]: [0, 1], [B]: [] }))
          const plan = assigner.assign.getMockImplementation()
          assigner.assign.mockImplementation(async (group) => {
            group.members.find(({ memberId }) => memberId === B).memberMetadata = subscription()
            return plan(group)
          })
          const claims = { [A]: { owned: [0] }, [B]: { owned: [1] } }

          await expectRefused(
            leaderRound({ assigner, name: 'custom', claims, order }),
            `${TOPIC}-1 to ${A} while ${B} still owns it`
          )
          expect(assigner.assign).toHaveBeenCalledTimes(1)
        }
      )

      test.each(ORDERS)(
        'writes the plan it checked when the assigner changes the rows it returned while the send is held, members %s',
        async (order) => {
          const rows = planOf({ [A]: [0], [B]: [1] })
          const checked = rows.map((row) => ({
            ...row,
            memberAssignment: Buffer.from(row.memberAssignment),
          }))
          const takeover = planOf({ [B]: [0, 1] })[0].memberAssignment

          let release
          const held = new Promise((resolve) => (release = resolve))
          let reached
          const reachedSync = new Promise((resolve) => (reached = resolve))
          const holdSync = (broker) => {
            const pool = broker.connectionPool
            const getConnectionByRequest = pool.getConnectionByRequest
            pool.getConnectionByRequest = async (protocolRequest, ...rest) => {
              if (protocolRequest.request.apiKey === apiKeys.SyncGroup) {
                reached()
                await held
              }
              return getConnectionByRequest.call(pool, protocolRequest, ...rest)
            }
          }
          const claims = { [A]: { owned: [0] }, [B]: { owned: [1] } }

          const { outcome, bodyOf } = await leaderRound({
            assigner: returning('custom', rows),
            name: 'custom',
            claims,
            order,
            prepare: holdSync,
          })

          await reachedSync
          expect(() => bodyOf(apiKeys.SyncGroup)).toThrow('no request written')
          rows[0].memberAssignment.fill(0)
          rows[1].memberAssignment = takeover
          rows.push({ memberId: C, memberAssignment: takeover })
          release()

          await expect(outcome).resolves.toMatchObject({ assignment: expect.any(Object) })
          const { body } = bodyOf(apiKeys.SyncGroup)
          expect(body.includes(written(checked[0]))).toBe(true)
          expect(body.includes(written(checked[1]))).toBe(true)
          expect(body.includes(takeover)).toBe(false)
          expect(body.includes(C)).toBe(false)
        }
      )
    })
  })
})
