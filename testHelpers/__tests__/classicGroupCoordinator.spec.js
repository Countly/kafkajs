const { ClassicGroupCoordinator, STATES } = require('../classicGroupCoordinator')

const GROUP = 'classic-group'
const PROTOCOL = 'RoundRobinAssigner'
const METADATA = Buffer.from('metadata-1')
const PLAN_A = Buffer.from('plan-a')
const PLAN_B = Buffer.from('plan-b')

/**
 * @param {Promise<*>} promise
 * @returns {{ settled: () => boolean, promise: Promise<*> }} the promise, marked handled, and
 *   whether it has settled
 */
const watch = (promise) => {
  let settled = false
  const watched = promise.finally(() => (settled = true))
  watched.catch(() => {})
  return { settled: () => settled, promise: watched }
}

describe('ClassicGroupCoordinator', () => {
  let coordinator

  beforeEach(() => {
    coordinator = new ClassicGroupCoordinator({ groupId: GROUP })
  })

  afterEach(async () => {
    await coordinator.close()
    expect(coordinator.failures).toEqual([])
  })

  /** @returns {import('../../src/broker')} a production Broker of a new client of the group */
  const client = () => coordinator.connect().broker

  /**
   * @param {object} broker
   * @param {object} [fields] over a static-free first JoinGroup
   * @param {object} [options] the Broker's private join options
   * @returns {Promise<object>} the JoinGroup answer
   */
  const join = (broker, fields = {}, options) =>
    broker.joinGroup(
      {
        groupId: GROUP,
        sessionTimeout: 30000,
        rebalanceTimeout: 60000,
        memberId: '',
        groupInstanceId: null,
        protocolType: 'consumer',
        groupProtocols: [{ name: PROTOCOL, metadata: METADATA }],
        ...fields,
      },
      options
    )

  /**
   * @param {object} broker
   * @param {{ memberId: string, generationId: number, groupInstanceId?: string|null,
   *   groupAssignment?: object[] }} fields
   * @returns {Promise<object>} the SyncGroup answer
   */
  const sync = (broker, { memberId, generationId, groupInstanceId = null, groupAssignment = [] }) =>
    broker.syncGroup({
      groupId: GROUP,
      generationId,
      memberId,
      groupInstanceId,
      groupAssignment,
      protocolType: 'consumer',
      protocolName: PROTOCOL,
    })

  /**
   * @param {object} broker
   * @param {{ memberId: string, generationId: number, groupInstanceId?: string|null }} fields
   * @returns {Promise<object>}
   */
  const heartbeat = (broker, { memberId, generationId, groupInstanceId = null }) =>
    broker.heartbeat({ groupId: GROUP, groupGenerationId: generationId, memberId, groupInstanceId })

  /**
   * Member `b` of `completingPair` sends its SyncGroup of generation 2, which the coordinator
   * accepts before this resolves.
   * @param {{ broker: object, memberId: string }} b
   * @returns {Promise<{ settled: () => boolean, promise: Promise<object> }>} the watched SyncGroup
   */
  const followerSync = async (b) => {
    const watched = watch(
      sync(b.broker, { memberId: b.memberId, generationId: 2, groupInstanceId: 'b' })
    )
    await coordinator.whenAccepted('SyncGroup', ({ memberId }) => memberId === b.memberId)
    return watched
  }

  /** @returns {Promise<object>} once the coordinator has accepted a JoinGroup of instance `b` */
  const instanceBJoined = () =>
    coordinator.whenAccepted('JoinGroup', ({ groupInstanceId }) => groupInstanceId === 'b')

  /**
   * A stable group of one static member, instance `a`, holding PLAN_A in generation 1.
   * @returns {Promise<{ broker: object, memberId: string }>}
   */
  const stableLeader = async () => {
    const broker = client()
    const { memberId, generationId } = await join(broker, { groupInstanceId: 'a' })
    await sync(broker, {
      memberId,
      generationId,
      groupInstanceId: 'a',
      groupAssignment: [{ memberId, memberAssignment: PLAN_A }],
    })
    return { broker, memberId }
  }

  /**
   * Static member `b` joins the stable group of `stableLeader`; the leader learns of the rebalance
   * from its heartbeat and rejoins, which completes generation 2. Neither has synced in it yet.
   * @returns {Promise<{ a: object, b: object }>} each with `broker`, `memberId` and the `answer`
   *   of its JoinGroup in generation 2
   */
  const completingPair = async () => {
    const a = await stableLeader()
    const b = { broker: client() }
    const bJoin = join(b.broker, { groupInstanceId: 'b' })
    await instanceBJoined()
    expect(coordinator.describe().state).toBe(STATES.PREPARING)
    await expect(
      heartbeat(a.broker, { memberId: a.memberId, generationId: 1, groupInstanceId: 'a' })
    ).rejects.toMatchObject({ type: 'REBALANCE_IN_PROGRESS' })
    a.answer = await join(a.broker, { memberId: a.memberId, groupInstanceId: 'a' })
    b.answer = await bJoin
    b.memberId = b.answer.memberId
    return { a, b }
  }

  describe('JoinGroup, as GroupMetadataManager 4.4.0 lines 7036-7449 and 7480-7572', () => {
    test('a new dynamic member is asked for a member id with 79, joins with it and leads generation 1 alone', async () => {
      const ids = []
      const answer = await join(client(), {}, { onMemberIdRequired: (id) => ids.push(id) })

      expect(ids).toHaveLength(1)
      expect(answer).toMatchObject({
        memberId: ids[0],
        generationId: 1,
        leaderId: ids[0],
        protocolType: 'consumer',
        groupProtocol: PROTOCOL,
        members: [{ memberId: ids[0], groupInstanceId: null, memberMetadata: METADATA }],
      })
      expect(coordinator.requests.map(({ api, request }) => [api, request.memberId])).toEqual([
        ['JoinGroup', ''],
        ['JoinGroup', ids[0]],
      ])
      expect(coordinator.describe()).toMatchObject({
        state: STATES.COMPLETING,
        generationId: 1,
        pending: [],
      })
    })

    test('a member offering no protocol every member supports is refused with 23 and changes nothing', async () => {
      const a = await stableLeader()

      await expect(
        join(client(), {
          groupInstanceId: 'b',
          groupProtocols: [{ name: 'OtherAssigner', metadata: METADATA }],
        })
      ).rejects.toMatchObject({ type: 'INCONSISTENT_GROUP_PROTOCOL' })
      expect(coordinator.describe()).toMatchObject({
        state: STATES.STABLE,
        generationId: 1,
        members: [{ memberId: a.memberId, groupInstanceId: 'a' }],
      })
    })

    test('a member joining a stable group waits until every member has rejoined, and the leader gets the metadata each member sent last', async () => {
      const a = await stableLeader()
      const b = client()
      const bJoin = watch(
        join(b, {
          groupInstanceId: 'b',
          groupProtocols: [{ name: PROTOCOL, metadata: Buffer.from('metadata-b') }],
        })
      )
      await instanceBJoined()
      const bId = coordinator.describe().members[1].memberId
      expect(coordinator.describe()).toMatchObject({
        state: STATES.PREPARING,
        awaitingJoin: [bId],
      })
      expect(bJoin.settled()).toBe(false)

      const changed = Buffer.from('metadata-a-2')
      const aAnswer = await join(a.broker, {
        memberId: a.memberId,
        groupInstanceId: 'a',
        groupProtocols: [{ name: PROTOCOL, metadata: changed }],
      })
      const bAnswer = await bJoin.promise

      expect(aAnswer).toMatchObject({
        generationId: 2,
        leaderId: a.memberId,
        members: [
          { memberId: a.memberId, groupInstanceId: 'a', memberMetadata: changed },
          {
            memberId: bAnswer.memberId,
            groupInstanceId: 'b',
            memberMetadata: Buffer.from('metadata-b'),
          },
        ],
      })
      expect(bAnswer).toMatchObject({ generationId: 2, leaderId: a.memberId, members: [] })
    })

    test('the rebalance timeout removes a dynamic member that did not rejoin and moves on without it', async () => {
      jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] })
      try {
        const a = client()
        const aAnswer = await join(a, { rebalanceTimeout: 5000 })
        await sync(a, { memberId: aAnswer.memberId, generationId: 1 })
        const bJoin = watch(join(client(), { rebalanceTimeout: 5000 }))
        const { memberId: bId } = await coordinator.whenAccepted(
          'JoinGroup',
          ({ memberId }) => memberId !== '' && memberId !== aAnswer.memberId
        )
        expect(coordinator.describe().state).toBe(STATES.PREPARING)

        jest.advanceTimersByTime(4999)
        expect(coordinator.describe()).toMatchObject({
          state: STATES.PREPARING,
          generationId: 1,
          awaitingJoin: [bId],
        })
        jest.advanceTimersByTime(1)
        const bAnswer = await bJoin.promise

        expect(bAnswer).toMatchObject({
          generationId: 2,
          leaderId: bAnswer.memberId,
          members: [{ memberId: bAnswer.memberId }],
        })
        await expect(
          heartbeat(a, { memberId: aAnswer.memberId, generationId: 1 })
        ).rejects.toMatchObject({ type: 'UNKNOWN_MEMBER_ID' })
      } finally {
        jest.useRealTimers()
      }
    })

    test('a static member rejoining a stable group with an empty member id and the same protocols takes a new id without a rebalance; the answer names the leader before the replacement', async () => {
      const a = await stableLeader()

      const successor = await join(client(), { groupInstanceId: 'a' })

      expect(successor.memberId).not.toBe(a.memberId)
      expect(successor).toMatchObject({ generationId: 1, leaderId: a.memberId, members: [] })
      expect(coordinator.describe()).toMatchObject({
        state: STATES.STABLE,
        generationId: 1,
        leaderId: successor.memberId,
        members: [{ memberId: successor.memberId, groupInstanceId: 'a', assignment: PLAN_A }],
      })
      await expect(
        heartbeat(a.broker, { memberId: a.memberId, generationId: 1, groupInstanceId: 'a' })
      ).rejects.toMatchObject({ type: 'FENCED_INSTANCE_ID' })
    })

    test("a successor client joining with the instance of a completing member fences that member's parked SyncGroup with 82 and starts a rebalance", async () => {
      const { a, b } = await completingPair()
      const bSync = await followerSync(b)

      const successorJoin = watch(join(client(), { groupInstanceId: 'b' }))

      await expect(bSync.promise).rejects.toMatchObject({ type: 'FENCED_INSTANCE_ID' })
      expect(coordinator.describe().state).toBe(STATES.PREPARING)
      await join(a.broker, { memberId: a.memberId, groupInstanceId: 'a' })
      const successor = await successorJoin.promise
      expect(successor).toMatchObject({ generationId: 3, leaderId: a.memberId })
      expect(successor.memberId).not.toBe(b.memberId)
    })

    test('the initial rebalance is extended only by what is left of the rebalance timeout after the first delay: members arriving in later windows are answered when it runs out', async () => {
      jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] })
      try {
        coordinator = new ClassicGroupCoordinator({ groupId: GROUP, initialRebalanceDelay: 1000 })
        const joinInstance = async (groupInstanceId) => {
          const watched = watch(join(client(), { groupInstanceId, rebalanceTimeout: 1500 }))
          await coordinator.whenAccepted(
            'JoinGroup',
            (request) => request.groupInstanceId === groupInstanceId
          )
          return watched
        }

        const a = await joinInstance('a')
        jest.advanceTimersByTime(500)
        const b = await joinInstance('b')
        jest.advanceTimersByTime(700)
        const c = await joinInstance('c')
        jest.advanceTimersByTime(299)
        expect(coordinator.describe().state).toBe(STATES.PREPARING)
        expect(c.settled()).toBe(false)

        jest.advanceTimersByTime(1)
        expect(coordinator.describe()).toMatchObject({ state: STATES.COMPLETING, generationId: 1 })
        const answers = await Promise.all([a.promise, b.promise, c.promise])
        expect(answers.map(({ generationId }) => generationId)).toEqual([1, 1, 1])
      } finally {
        jest.useRealTimers()
      }
    })

    test('a pending dynamic member id presented with an instance id is answered -1 and changes nothing: the pending member still joins', async () => {
      const first = coordinator.hold('JoinGroup', 'acceptance')
      const second = coordinator.hold('JoinGroup', 'acceptance')
      const dynamicJoin = watch(join(client()))
      await first.reached
      first.release()
      const { memberId: pendingId } = await second.reached
      expect(coordinator.describe().pending).toEqual([pendingId])

      await expect(
        join(client(), { memberId: pendingId, groupInstanceId: 'x' })
      ).rejects.toMatchObject({ code: -1 })

      expect(coordinator.describe()).toMatchObject({ members: [], pending: [pendingId] })
      second.release()
      await expect(dynamicJoin.promise).resolves.toMatchObject({
        memberId: pendingId,
        generationId: 1,
      })
      expect(coordinator.describe()).toMatchObject({
        pending: [],
        members: [{ memberId: pendingId, groupInstanceId: null }],
      })
    })
  })

  describe('SyncGroup, as GroupMetadataManager 4.4.0 lines 8273-8359 and 8436-8466', () => {
    test('a SyncGroup naming another generation is answered 22, and the stored assignment is unchanged', async () => {
      const a = await stableLeader()

      for (const generationId of [0, 2]) {
        await expect(
          sync(a.broker, {
            memberId: a.memberId,
            generationId,
            groupInstanceId: 'a',
            groupAssignment: [{ memberId: a.memberId, memberAssignment: PLAN_B }],
          })
        ).rejects.toMatchObject({ type: 'ILLEGAL_GENERATION' })
      }
      await expect(
        sync(a.broker, { memberId: a.memberId, generationId: 1, groupInstanceId: 'a' })
      ).resolves.toMatchObject({ memberAssignment: PLAN_A })
    })

    test('a SyncGroup of a member the group does not know is answered 25, and one naming an instance another id holds 82', async () => {
      const a = await stableLeader()

      await expect(sync(a.broker, { memberId: 'stranger', generationId: 1 })).rejects.toMatchObject(
        { type: 'UNKNOWN_MEMBER_ID' }
      )
      await expect(
        sync(a.broker, { memberId: 'stranger', generationId: 1, groupInstanceId: 'a' })
      ).rejects.toMatchObject({ type: 'FENCED_INSTANCE_ID' })
    })

    test('a SyncGroup while the group prepares a rebalance is answered 27', async () => {
      const a = await stableLeader()
      watch(join(client(), { groupInstanceId: 'b' }))
      await instanceBJoined()
      expect(coordinator.describe().state).toBe(STATES.PREPARING)

      await expect(
        sync(a.broker, { memberId: a.memberId, generationId: 1, groupInstanceId: 'a' })
      ).rejects.toMatchObject({ type: 'REBALANCE_IN_PROGRESS' })
    })

    test("a follower's SyncGroup waits for the leader's, which stores its plan once: a later leader SyncGroup gets the stored bytes", async () => {
      const { a, b } = await completingPair()
      const bSync = await followerSync(b)
      expect(coordinator.describe()).toMatchObject({
        state: STATES.COMPLETING,
        awaitingSync: [b.memberId],
      })
      expect(bSync.settled()).toBe(false)

      const leaderSync = await sync(a.broker, {
        memberId: a.memberId,
        generationId: 2,
        groupInstanceId: 'a',
        groupAssignment: [
          { memberId: a.memberId, memberAssignment: PLAN_A },
          { memberId: b.memberId, memberAssignment: PLAN_B },
        ],
      })
      await expect(bSync.promise).resolves.toMatchObject({
        memberAssignment: PLAN_B,
        protocolType: 'consumer',
        protocolName: PROTOCOL,
      })
      expect(leaderSync.memberAssignment).toEqual(PLAN_A)

      const again = await sync(a.broker, {
        memberId: a.memberId,
        generationId: 2,
        groupInstanceId: 'a',
        groupAssignment: [{ memberId: a.memberId, memberAssignment: Buffer.from('other') }],
      })
      expect(again.memberAssignment).toEqual(PLAN_A)
      await expect(
        sync(b.broker, { memberId: b.memberId, generationId: 2, groupInstanceId: 'b' })
      ).resolves.toMatchObject({ memberAssignment: PLAN_B })
    })

    test("a member the leader's plan leaves out is given zero bytes", async () => {
      const { a, b } = await completingPair()
      const bSync = await followerSync(b)

      await sync(a.broker, {
        memberId: a.memberId,
        generationId: 2,
        groupInstanceId: 'a',
        groupAssignment: [{ memberId: a.memberId, memberAssignment: PLAN_A }],
      })

      await expect(bSync.promise).resolves.toMatchObject({ memberAssignment: Buffer.alloc(0) })
    })

    test('a rebalance answers a parked SyncGroup with 27', async () => {
      const { a, b } = await completingPair()
      const bSync = await followerSync(b)

      // The leader rejoins with changed metadata, which starts a rebalance of the completing group.
      watch(
        join(a.broker, {
          memberId: a.memberId,
          groupInstanceId: 'a',
          groupProtocols: [{ name: PROTOCOL, metadata: Buffer.from('metadata-a-2') }],
        })
      )

      await expect(bSync.promise).rejects.toMatchObject({ type: 'REBALANCE_IN_PROGRESS' })
      expect(coordinator.describe().state).toBe(STATES.PREPARING)
    })
  })

  describe('Heartbeat, as GroupMetadataManager 4.4.0 lines 8532-8597', () => {
    test('a heartbeat is answered 25 for an unknown member, 82 for an instance another id holds, 22 for another generation, 27 while a rebalance prepares, and success otherwise', async () => {
      const a = await stableLeader()
      const beat = (fields) =>
        heartbeat(a.broker, {
          memberId: a.memberId,
          generationId: 1,
          groupInstanceId: 'a',
          ...fields,
        })

      await expect(beat({ memberId: 'stranger', groupInstanceId: null })).rejects.toMatchObject({
        type: 'UNKNOWN_MEMBER_ID',
      })
      await expect(beat({ memberId: 'stranger' })).rejects.toMatchObject({
        type: 'FENCED_INSTANCE_ID',
      })
      await expect(beat({ generationId: 2 })).rejects.toMatchObject({ type: 'ILLEGAL_GENERATION' })
      await expect(beat({})).resolves.toMatchObject({ errorCode: 0 })

      watch(join(client(), { groupInstanceId: 'b' }))
      await instanceBJoined()
      expect(coordinator.describe().state).toBe(STATES.PREPARING)
      await expect(beat({})).rejects.toMatchObject({ type: 'REBALANCE_IN_PROGRESS' })
    })
  })

  describe('LeaveGroup, as GroupMetadataManager 4.4.0 lines 9049-9181', () => {
    test("an empty member id with an instance id removes whoever holds the instance, whoever sends it, and the holder's next heartbeat is answered 25", async () => {
      const a = await stableLeader()

      const answer = await client().leaveGroup({
        groupId: GROUP,
        members: [{ memberId: '', groupInstanceId: 'a', reason: null }],
      })

      expect(answer.members).toEqual([{ memberId: '', groupInstanceId: 'a', errorCode: 0 }])
      expect(coordinator.describe().members).toEqual([])
      await expect(
        heartbeat(a.broker, { memberId: a.memberId, generationId: 1, groupInstanceId: 'a' })
      ).rejects.toMatchObject({ type: 'UNKNOWN_MEMBER_ID' })
    })

    test('a leave naming a replaced member id with its instance is answered 82 and removes nobody', async () => {
      const a = await stableLeader()
      const successor = await join(client(), { groupInstanceId: 'a' })

      await expect(
        a.broker.leaveGroup({ groupId: GROUP, memberId: a.memberId, groupInstanceId: 'a' })
      ).rejects.toMatchObject({ type: 'FENCED_INSTANCE_ID' })
      expect(coordinator.describe().members).toEqual([
        expect.objectContaining({ memberId: successor.memberId, groupInstanceId: 'a' }),
      ])
    })

    test('a pending member id leaves without having joined, and the same id is unknown afterwards', async () => {
      const broker = client()
      let pending = null
      const options = {
        onMemberIdRequired: (id) => (pending = id),
        guard: () => (pending ? new Error('stopped after the member id') : null),
      }
      await expect(join(broker, {}, options)).rejects.toThrow('stopped after the member id')
      expect(coordinator.describe().pending).toEqual([pending])

      await expect(
        broker.leaveGroup({ groupId: GROUP, memberId: pending, groupInstanceId: null })
      ).resolves.toMatchObject({ errorCode: 0 })
      expect(coordinator.describe().pending).toEqual([])
      await expect(
        broker.leaveGroup({ groupId: GROUP, memberId: pending, groupInstanceId: null })
      ).rejects.toMatchObject({ type: 'UNKNOWN_MEMBER_ID' })
    })

    test('a leave of a member whose JoinGroup is parked answers that JoinGroup 25', async () => {
      await stableLeader()
      const b = client()
      const bJoin = watch(join(b, { groupInstanceId: 'b' }))
      await instanceBJoined()
      const bId = coordinator.describe().members[1].memberId
      expect(coordinator.describe().awaitingJoin).toEqual([bId])

      await client().leaveGroup({
        groupId: GROUP,
        members: [{ memberId: bId, groupInstanceId: 'b', reason: null }],
      })

      await expect(bJoin.promise).rejects.toMatchObject({ type: 'UNKNOWN_MEMBER_ID' })
    })
  })

  describe('session expiry, as GroupMetadataManager 4.4.0 lines 7597-7668', () => {
    test('expireSession keeps a member that awaits its SyncGroup, and removes it once it does not, starting a rebalance', async () => {
      const { a, b } = await completingPair()
      const bSync = await followerSync(b)

      expect(coordinator.expireSession(b.memberId)).toBe(false)
      await sync(a.broker, {
        memberId: a.memberId,
        generationId: 2,
        groupInstanceId: 'a',
        groupAssignment: [{ memberId: b.memberId, memberAssignment: PLAN_B }],
      })
      await expect(bSync.promise).resolves.toMatchObject({ memberAssignment: PLAN_B })

      expect(coordinator.expireSession(b.memberId)).toBe(true)
      expect(coordinator.describe()).toMatchObject({
        state: STATES.PREPARING,
        members: [{ memberId: a.memberId }],
      })
      await expect(
        heartbeat(a.broker, { memberId: a.memberId, generationId: 2, groupInstanceId: 'a' })
      ).rejects.toMatchObject({ type: 'REBALANCE_IN_PROGRESS' })
    })
  })

  describe('the three steps of a request', () => {
    test('a JoinGroup held before acceptance changes nothing until it is released', async () => {
      const a = await stableLeader()
      const held = coordinator.hold('JoinGroup', 'acceptance')
      const bJoin = watch(join(client(), { groupInstanceId: 'b' }))

      await expect(held.reached).resolves.toMatchObject({ groupInstanceId: 'b' })
      expect(coordinator.describe()).toMatchObject({ state: STATES.STABLE, generationId: 1 })
      await expect(
        heartbeat(a.broker, { memberId: a.memberId, generationId: 1, groupInstanceId: 'a' })
      ).resolves.toMatchObject({ errorCode: 0 })

      held.release()
      await instanceBJoined()
      const bId = coordinator.describe().members[1].memberId
      expect(coordinator.describe()).toMatchObject({
        state: STATES.PREPARING,
        awaitingJoin: [bId],
      })
      expect(bJoin.settled()).toBe(false)
    })

    test('a LeaveGroup held after acceptance has already removed the member: the client gives up and the removal stands', async () => {
      const a = await stableLeader()
      const held = coordinator.hold('LeaveGroup', 'reply')
      const leaver = coordinator.connect()
      const leave = watch(
        leaver.broker.leaveGroup({ groupId: GROUP, memberId: a.memberId, groupInstanceId: 'a' })
      )

      await held.reached
      expect(coordinator.describe().members).toEqual([])
      await leaver.close()
      held.release()

      await expect(leave.promise).rejects.toThrow()
      expect(coordinator.describe()).toMatchObject({ state: STATES.EMPTY, members: [] })
    })
  })

  describe('reply faults', () => {
    test('a faulted SyncGroup reply leaves the stored plan alone: the next SyncGroup of the member gets the stored bytes', async () => {
      const a = await stableLeader()
      coordinator.faultNextReply(
        'SyncGroup',
        (answer) => ({ ...answer, memberAssignment: Buffer.from([0]) }),
        { memberId: a.memberId }
      )

      const fields = { memberId: a.memberId, generationId: 1, groupInstanceId: 'a' }
      await expect(sync(a.broker, fields)).resolves.toMatchObject({
        memberAssignment: Buffer.from([0]),
      })
      await expect(sync(a.broker, fields)).resolves.toMatchObject({ memberAssignment: PLAN_A })
    })

    test('a SyncGroup reply edited in place leaves the stored plan alone: the next SyncGroup of the member gets the stored bytes', async () => {
      const a = await stableLeader()
      coordinator.faultNextReply(
        'SyncGroup',
        (answer) => {
          answer.memberAssignment[0] ^= 0xff
          return answer
        },
        { memberId: a.memberId }
      )

      const fields = { memberId: a.memberId, generationId: 1, groupInstanceId: 'a' }
      const faulted = await sync(a.broker, fields)

      expect(faulted.memberAssignment).not.toEqual(PLAN_A)
      await expect(sync(a.broker, fields)).resolves.toMatchObject({ memberAssignment: PLAN_A })
      expect(coordinator.describe().members[0].assignment).toEqual(PLAN_A)
    })

    test("a JoinGroup reply edited in place leaves the member's stored metadata alone: the leader's next answer still carries the bytes it offered", async () => {
      const broker = client()
      coordinator.faultNextReply('JoinGroup', (answer) => {
        answer.members[0].memberMetadata.fill(0)
        return answer
      })

      const faulted = await join(broker, { groupInstanceId: 'a' })
      expect(faulted.members[0].memberMetadata).not.toEqual(METADATA)

      const again = await join(broker, { memberId: faulted.memberId, groupInstanceId: 'a' })
      expect(again.members[0].memberMetadata).toEqual(METADATA)
      expect(again.generationId).toBe(faulted.generationId)
    })
  })

  describe('what the coordinator hands out is a copy', () => {
    test('a describe() snapshot edited in place leaves the stored plan alone', async () => {
      const a = await stableLeader()

      coordinator.describe().members[0].assignment.fill(0)

      await expect(
        sync(a.broker, { memberId: a.memberId, generationId: 1, groupInstanceId: 'a' })
      ).resolves.toMatchObject({ memberAssignment: PLAN_A })
      expect(coordinator.describe().members[0].assignment).toEqual(PLAN_A)
    })

    test('a recorded request edited in place leaves the stored plan alone', async () => {
      const a = await stableLeader()

      for (const { request } of coordinator.requests) {
        for (const { memberAssignment } of request.groupAssignment ?? []) memberAssignment.fill(0)
      }

      await expect(
        sync(a.broker, { memberId: a.memberId, generationId: 1, groupInstanceId: 'a' })
      ).resolves.toMatchObject({ memberAssignment: PLAN_A })
    })

    test("a SyncGroup request edited in place while its acceptance is held leaves the leader's plan and identity as the client sent them", async () => {
      const broker = client()
      const { memberId, generationId } = await join(broker, { groupInstanceId: 'a' })
      const held = coordinator.hold('SyncGroup', 'acceptance')
      const leaderSync = sync(broker, {
        memberId,
        generationId,
        groupInstanceId: 'a',
        groupAssignment: [{ memberId, memberAssignment: PLAN_A }],
      })

      const request = await held.reached
      request.groupAssignment[0].memberAssignment.fill(0)
      request.groupAssignment[0].memberId = 'nobody'
      request.memberId = 'nobody'
      request.generationId = 99
      held.release()

      await expect(leaderSync).resolves.toMatchObject({ memberAssignment: PLAN_A })
      expect(coordinator.describe().members[0].assignment).toEqual(PLAN_A)
      await expect(
        sync(broker, { memberId, generationId, groupInstanceId: 'a' })
      ).resolves.toMatchObject({ memberAssignment: PLAN_A })
    })

    test("a JoinGroup request edited in place while its acceptance is held leaves the member's protocol metadata and instance as the client sent them", async () => {
      const broker = client()
      const held = coordinator.hold('JoinGroup', 'acceptance')
      const joining = join(broker, { groupInstanceId: 'a' })

      const request = await held.reached
      request.groupProtocols[0].metadata.fill(0)
      request.groupProtocols[0].name = 'Other'
      request.groupInstanceId = 'z'
      held.release()

      const answer = await joining
      expect(answer.groupProtocol).toBe(PROTOCOL)
      expect(answer.members[0]).toMatchObject({ groupInstanceId: 'a', memberMetadata: METADATA })
      expect(coordinator.describe().members[0].groupInstanceId).toBe('a')
    })

    test('a request handed to whenAccepted, or to its match function, edited in place leaves what the next observer sees alone', async () => {
      const broker = client()
      const joining = join(broker, { groupInstanceId: 'a' })
      const seen = await coordinator.whenAccepted('JoinGroup', (request) => {
        request.groupProtocols[0].metadata.fill(0)
        return true
      })
      seen.groupProtocols[0].name = 'Other'
      await joining

      const again = await coordinator.whenAccepted('JoinGroup')
      expect(again.groupProtocols[0]).toEqual({ name: PROTOCOL, metadata: METADATA })
    })
  })

  describe('unload, as GroupMetadataManager 4.4.0 onUnloaded', () => {
    test('unloading answers a parked JoinGroup with 16, and every later request too', async () => {
      const a = await stableLeader()
      const bJoin = watch(join(client(), { groupInstanceId: 'b' }))
      await instanceBJoined()
      expect(coordinator.describe().awaitingJoin).toHaveLength(1)

      coordinator.unload()

      // kafkajs names code 16 NOT_COORDINATOR_FOR_GROUP.
      const notCoordinator = { type: 'NOT_COORDINATOR_FOR_GROUP', code: 16 }
      await expect(bJoin.promise).rejects.toMatchObject(notCoordinator)
      await expect(
        heartbeat(a.broker, { memberId: a.memberId, generationId: 1, groupInstanceId: 'a' })
      ).rejects.toMatchObject(notCoordinator)
    })
  })
})
