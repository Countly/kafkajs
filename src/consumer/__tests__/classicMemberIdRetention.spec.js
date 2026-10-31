const ClassicGroupProtocol = require('../groupProtocol/classic')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { MemberMetadata } = require('../assignerProtocol')
const { KafkaJSConnectionError, KafkaJSProtocolError } = require('../../errors')
const { sentMembership } = require('../rebalanceErrorContract')
const { newLogger } = require('testHelpers')

const TOPICS = ['topic-a']
const METADATA = MemberMetadata.encode({ version: 0, topics: TOPICS })

/**
 * A classic protocol over a coordinator that answers a join with an empty member id as the Broker
 * does after MEMBER_ID_REQUIRED: it reports the assigned id through `onMemberIdRequired`, then
 * loses the follow-up request. A join that names an id succeeds.
 * @param {{ groupInstanceId?: string|null, retainedMemberId?: string|null }} [options] the static
 *   instance id the protocol is configured with, and the id a crashed membership handed it
 * @returns {{ protocol: ClassicGroupProtocol, coordinator: object, hooks: { beforeNotify?: () => void },
 *   joinIds: () => string[], join: () => Promise<void>, assign: (...ids: string[]) => void }}
 *   `joinIds` is the member id of every JoinGroup the protocol sent; `assign` queues the ids the
 *   next handshakes assign
 */
const build = ({ groupInstanceId = null, retainedMemberId = null } = {}) => {
  const queued = []
  const sent = []
  const hooks = {}
  const coordinator = {
    isConnected: () => true,
    negotiatedVersion: () => 5,
    leaveGroup: jest.fn(async () => {}),
    heartbeat: jest.fn(async () => {}),
    joinGroup: jest.fn(async ({ memberId }, options) => {
      sent.push(memberId)
      if (memberId === '' && queued.length) {
        const assigned = queued.shift()
        hooks.beforeNotify?.()
        options?.onMemberIdRequired?.(assigned)
        throw new KafkaJSConnectionError('The follow-up JoinGroup was lost')
      }
      return {
        generationId: 1,
        leaderId: memberId,
        memberId: memberId || 'granted',
        members: [],
        groupProtocol: 'RoundRobinAssigner',
        protocolType: 'consumer',
      }
    }),
  }
  const protocol = new ClassicGroupProtocol({
    cluster: { findGroupCoordinator: async () => coordinator },
    groupId: 'retention-group',
    groupInstanceId,
    retainedMemberId,
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
    assigners: [
      {
        name: 'RoundRobinAssigner',
        protocol: () => ({ name: 'RoundRobinAssigner', metadata: METADATA }),
      },
    ],
    instrumentationEmitter: new InstrumentationEventEmitter(),
    logger: newLogger(),
  })
  return {
    protocol,
    coordinator,
    hooks,
    joinIds: () => sent,
    join: () => protocol.join({ topicsSubscribed: TOPICS }),
    assign: (...ids) => queued.push(...ids),
  }
}

/**
 * @param {() => boolean} done
 * @returns {Promise<boolean>} whether `done` held within 200 turns of the event loop
 */
const settles = async (done) => {
  for (let turn = 0; turn < 200 && !done(); turn += 1) {
    await new Promise((resolve) => setImmediate(resolve))
  }
  return done()
}

describe('Consumer > classic member id retention', () => {
  it('sends the id a lost handshake was assigned in the next join of the same membership', async () => {
    const { protocol, join, joinIds, assign } = build()
    assign('A')

    await expect(join()).rejects.toThrow('follow-up JoinGroup was lost')
    await join()

    expect(joinIds()).toEqual(['', 'A'])
    expect(protocol.memberId).toBe('A')
  })

  it('does not treat the pending id as a joined member', async () => {
    const { protocol, coordinator, join, assign } = build()
    assign('A')
    await expect(join()).rejects.toThrow()

    await protocol.heartbeat({ interval: 0 })

    expect(protocol.memberId).toBeNull()
    expect(coordinator.heartbeat).not.toHaveBeenCalled()
  })

  it('forgets the pending id when the membership is reset', async () => {
    const { protocol, join, joinIds, assign } = build()
    assign('A')
    await expect(join()).rejects.toThrow()

    protocol.memberId = null
    await join()

    expect(joinIds()).toEqual(['', ''])
  })

  it('drops an id assigned to a membership that was retired while its join was in flight', async () => {
    const { protocol, hooks, join, joinIds, assign } = build()
    hooks.beforeNotify = () => {
      protocol.memberId = null
    }
    assign('A')
    await expect(join()).rejects.toThrow()

    await join()

    expect(joinIds()).toEqual(['', ''])
  })

  it('names the pending id in its LeaveGroup and forgets it when that request fails', async () => {
    const { protocol, coordinator, join, joinIds, assign } = build()
    assign('A')
    await expect(join()).rejects.toThrow()
    coordinator.leaveGroup.mockRejectedValueOnce(new Error('LeaveGroup was lost'))

    await expect(protocol.leave()).rejects.toThrow('LeaveGroup was lost')
    await join()

    expect(coordinator.leaveGroup).toHaveBeenCalledWith(expect.objectContaining({ memberId: 'A' }))
    expect(joinIds()).toEqual(['', ''])
  })

  it('forgets the pending id of a leave that sends nothing', async () => {
    const { protocol, coordinator, join, joinIds, assign } = build()
    assign('A')
    await expect(join()).rejects.toThrow()

    await protocol.leave({ leaveGroup: false })
    await join()

    expect(coordinator.leaveGroup).not.toHaveBeenCalled()
    expect(joinIds()).toEqual(['', ''])
  })

  it('lets a leave that settles late leave a newer membership alone', async () => {
    const { protocol, coordinator, join, joinIds, assign } = build()
    let release
    coordinator.leaveGroup.mockImplementation(
      () =>
        new Promise((resolve) => {
          release = resolve
        })
    )
    assign('A', 'B')
    await expect(join()).rejects.toThrow()
    const leaving = protocol.leave()
    const sent = await settles(() => coordinator.leaveGroup.mock.calls.length === 1)
    protocol.memberId = null
    await expect(join()).rejects.toThrow()

    release?.()
    await leaving
    await join()

    expect(sent).toBe(true)
    expect(joinIds()).toEqual(['', '', 'B'])
  })

  it('does not reinstall a join accepted before poll retirement', async () => {
    const ConsumerGroup = require('../consumerGroup')
    const RebalanceCoordinator = require('../rebalanceCoordinator')
    const IdleWait = require('../idleWait')
    expect(typeof ConsumerGroup.prototype.beginRebalanceRound).toBe('function')
    const { protocol, coordinator } = build()
    protocol.coordinator = coordinator
    protocol.memberId = 'A'
    protocol.generationId = 1
    let release
    coordinator.joinGroup.mockImplementation(
      () =>
        new Promise((resolve) => {
          release = () =>
            resolve({
              generationId: 2,
              leaderId: 'A',
              memberId: 'A',
              members: [],
              groupProtocol: 'RoundRobinAssigner',
              protocolType: 'consumer',
            })
        })
    )
    const group = Object.create(ConsumerGroup.prototype)
    Object.assign(group, {
      groupMembership: protocol,
      idleWait: new IdleWait(),
      rebalanceTimeout: 60000,
      cluster: { allowAutoTopicCreation: true },
      isStopping: () => false,
      assignmentEpoch: 0,
    })
    let releaseLeave
    coordinator.leaveGroup.mockImplementation(
      () =>
        new Promise((resolve) => {
          releaseLeave = resolve
        })
    )
    const owner = Object.create(RebalanceCoordinator.prototype)
    Object.assign(owner, {
      consumerGroup: group,
      pollTimer: { deadline: () => 1000 },
      runner: { stopRequested: false },
      logger: newLogger(),
      instrumentationEmitter: { emit() {} },
      heartbeatManager: { stop() {} },
      contain: async (_, work) => work(),
      isRebalancing: () => true,
    })
    const retiring = owner.onPollExpired({ elapsedMs: 1001, isStatic: false })
    expect(await settles(() => !!releaseLeave)).toBe(true)
    const round = group.beginRebalanceRound()
    const joining = protocol.join({ topicsSubscribed: TOPICS, round: round.attempt() })
    expect(await settles(() => !!release)).toBe(true)
    releaseLeave()
    await retiring
    expect(protocol.memberId).toBeNull()
    expect(round.guard()).toBeNull()
    release()
    await expect(joining).rejects.toMatchObject({ type: 'JOIN_DEADLINE_EXCEEDED' })
    round.retire()
    expect(protocol.memberId).toBeNull()
  })

  it('forgets the pending id of a leave that finds no coordinator to ask', async () => {
    const { protocol, coordinator, join, joinIds, assign } = build()
    assign('A')
    await expect(join()).rejects.toThrow()
    protocol.coordinatorUnknown = true

    await protocol.leave()
    await join()

    expect(coordinator.leaveGroup).not.toHaveBeenCalled()
    expect(joinIds()).toEqual(['', ''])
  })

  it('names the instance id of a static member on every join, and a leave forgets only the member id', async () => {
    const { protocol, coordinator, join, assign } = build({ groupInstanceId: 'instance-1' })
    assign('A')
    await expect(join()).rejects.toThrow()
    await join()

    protocol.memberId = null
    await join()
    await protocol.leave()

    const instances = coordinator.joinGroup.mock.calls.map(([payload]) => payload.groupInstanceId)
    expect(instances).toEqual(['instance-1', 'instance-1', 'instance-1'])
    expect(coordinator.leaveGroup).toHaveBeenCalledWith(
      expect.objectContaining({ memberId: 'granted', groupInstanceId: 'instance-1' })
    )
    expect(protocol.groupInstanceId).toBe('instance-1')
    expect(protocol.memberId).toBeNull()
  })

  it("starts a second protocol with the same configuration without the first one's id", async () => {
    const first = build({ groupInstanceId: 'instance-1' })
    first.assign('A')
    await expect(first.join()).rejects.toThrow()
    const second = build({ groupInstanceId: 'instance-1' })

    await second.join()

    expect(second.joinIds()).toEqual([''])
    expect(first.protocol.pendingMemberId).toBe('A')
  })

  it('sends a retained id in its first join without treating it as joined', async () => {
    const retained = build({ retainedMemberId: 'kept' })

    expect(retained.protocol.memberId).toBeNull()
    expect(retained.protocol.retainableMemberId()).toBe('kept')
    await retained.join()

    expect(retained.joinIds()).toEqual(['kept'])
    expect(retained.protocol.memberId).toBe('kept')
  })

  it('offers the joined id for a restart, else the pending one, and nothing once reset', async () => {
    const lost = build()
    expect(lost.protocol.retainableMemberId()).toBeNull()
    lost.assign('A')
    await expect(lost.join()).rejects.toThrow()
    expect(lost.protocol.retainableMemberId()).toBe('A')

    await lost.join()
    expect(lost.protocol.retainableMemberId()).toBe('A')

    lost.protocol.forgetMemberId()
    expect(lost.protocol.retainableMemberId()).toBeNull()
  })

  describe('over the production Broker and Connection', () => {
    /**
     * A real round and handshake whose only substitute is the socket: the first JoinGroup is
     * answered MEMBER_ID_REQUIRED, the second succeeds.
     * @param {boolean} retire whether the membership is retired just before the first answer
     * @returns {Promise<{ result: Error|null, frames: number }>} the join's outcome and the number
     *   of JoinGroup frames written
     */
    const handshake = async (retire) => {
      const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
      const Encoder = require('../../protocol/encoder')
      const ConsumerGroup = require('../consumerGroup')
      const IdleWait = require('../idleWait')
      expect(typeof ConsumerGroup.prototype.beginRebalanceRound).toBe('function')
      const { protocol } = build()
      const group = Object.create(ConsumerGroup.prototype)
      Object.assign(group, {
        idleWait: new IdleWait(),
        rebalanceTimeout: 60000,
        cluster: {},
        isStopping: () => false,
      })
      const round = group.beginRebalanceRound()
      let frames = 0
      const harness = createGroupApiBroker({
        versions: { 11: { minVersion: 0, maxVersion: 8 } },
        respond: () => {
          frames += 1
          if (frames === 1 && retire) {
            protocol.leave({ leaveGroup: false })
            expect(round.guard()).toBeNull()
          }
          return new Encoder()
            .writeInt32(0)
            .writeInt16(frames === 1 ? 79 : 0)
            .writeInt32(5)
            .writeUVarIntString('consumer')
            .writeUVarIntString('RoundRobinAssigner')
            .writeUVarIntString('A')
            .writeUVarIntString('A')
            .writeUVarIntArray([])
            .writeTaggedFields().buffer
        },
      })
      protocol.cluster.findGroupCoordinator = async () => harness.broker
      try {
        const result = await protocol
          .join({ topicsSubscribed: TOPICS, round: round.attempt() })
          .then(
            () => null,
            (error) => error
          )
        return { result, frames: harness.requests.length }
      } finally {
        round.retire()
        await harness.pool.destroy()
      }
    }

    it('sends the follow-up of an unchanged membership', async () => {
      const { result, frames } = await handshake(false)

      expect(result).toBeNull()
      expect(frames).toBe(2)
    })

    it('writes no follow-up for a membership retired before the answer arrived', async () => {
      const { result, frames } = await handshake(true)

      expect(result).toMatchObject({ type: 'JOIN_DEADLINE_EXCEEDED' })
      expect(frames).toBe(1)
    })
  })
  describe('the identity of a retired membership', () => {
    beforeEach(() => {
      expect(typeof ClassicGroupProtocol.prototype.changeIdentity).toBe('function')
      expect(typeof ClassicGroupProtocol.prototype.forgetMemberId).toBe('function')
    })

    it.each([
      ['a pending id', { pendingMemberId: 'late' }],
      ['a joined id and generation', { memberId: 'late', pendingMemberId: null, generationId: 9 }],
      ['a forgotten id', { memberId: null }],
      ['a generation reset', { generationId: null }],
    ])('refuses %s from a request that began before the retirement', (_, change) => {
      const { protocol } = build()
      protocol.memberId = 'A'
      protocol.generationId = 3
      const began = protocol.membershipRevision
      protocol.forgetMemberId()
      protocol.memberId = 'B'
      protocol.generationId = 4
      protocol.keepPendingMemberId('P', protocol.membershipRevision)

      expect(protocol.changeIdentity(began, change)).toBe(false)
      expect(protocol.forgetMemberId(began)).toBe(false)
      expect(protocol.keepPendingMemberId('late', began)).toBe(false)
      expect([protocol.memberId, protocol.pendingMemberId, protocol.generationId]).toEqual([
        'B',
        'P',
        4,
      ])
    })

    it('applies a change from the current membership', () => {
      const { protocol } = build()
      const began = protocol.membershipRevision

      expect(protocol.keepPendingMemberId('P', began)).toBe(true)
      expect(
        protocol.changeIdentity(began, { memberId: 'A', pendingMemberId: null, generationId: 1 })
      ).toBe(true)
      expect([protocol.memberId, protocol.pendingMemberId, protocol.generationId]).toEqual([
        'A',
        null,
        1,
      ])
      expect(protocol.forgetMemberId(began)).toBe(true)
      expect(protocol.membershipRevision).toBe(began + 1)
    })
  })

  describe('an UNKNOWN_MEMBER_ID answer that outlived its membership', () => {
    beforeEach(() => {
      expect(typeof ClassicGroupProtocol.prototype.changeIdentity).toBe('function')
      expect(typeof require('../consumerGroup').prototype.forgetMemberId).toBe('function')
      expect(typeof sentMembership).toBe('function')
    })

    const unknownMember = () => new KafkaJSProtocolError({ type: 'UNKNOWN_MEMBER_ID', code: 25 })

    /**
     * A ConsumerGroup over a real protocol, with a current round. `retire` retires the member
     * `A` the way a poll overrun does while the round stays valid, then installs the replacement.
     * @param {'pending'|'accepted'} replacement the identity the replacement membership holds
     * @returns {{ group: object, protocol: ClassicGroupProtocol, coordinator: object, round: object,
     *   retire: () => void }}
     */
    const withReplacement = (replacement) => {
      const ConsumerGroup = require('../consumerGroup')
      const IdleWait = require('../idleWait')
      expect(typeof ConsumerGroup.prototype.beginRebalanceRound).toBe('function')
      const { protocol, coordinator } = build()
      const group = Object.create(ConsumerGroup.prototype)
      Object.assign(group, {
        groupMembership: protocol,
        idleWait: new IdleWait(),
        rebalanceTimeout: 60000,
        cluster: {},
        isStopping: () => false,
        retrier: (work) =>
          work((error) => {
            throw error
          }),
      })
      protocol.memberId = 'A'
      const round = group.beginRebalanceRound()
      const retire = () => {
        protocol.leave({ leaveGroup: false })
        if (replacement === 'pending')
          protocol.keepPendingMemberId('B', protocol.membershipRevision)
        else protocol.memberId = 'B'
        expect(round.guard()).toBeNull()
      }
      return { group, protocol, coordinator, round, retire }
    }

    const survivor = (protocol, replacement) =>
      replacement === 'pending' ? protocol.pendingMemberId : protocol.memberId

    describe.each(['pending', 'accepted'])('with a %s replacement', (replacement) => {
      it('leaves the replacement alone when the JoinGroup it answered is obsolete', async () => {
        const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
        const Encoder = require('../../protocol/encoder')
        const { group, protocol, round, retire } = withReplacement(replacement)
        const harness = createGroupApiBroker({
          versions: { 11: { minVersion: 0, maxVersion: 8 } },
          respond: () => {
            retire()
            return new Encoder()
              .writeInt32(0)
              .writeInt16(25)
              .writeInt32(5)
              .writeUVarIntString('consumer')
              .writeUVarIntString('RoundRobinAssigner')
              .writeUVarIntString('A')
              .writeUVarIntString('A')
              .writeUVarIntArray([])
              .writeTaggedFields().buffer
          },
        })
        protocol.cluster.findGroupCoordinator = async () => harness.broker
        protocol.rebalance = ({ round: attempt }) =>
          protocol.join({ topicsSubscribed: TOPICS, round: attempt })
        try {
          await expect(group.joinAndSyncAttempt(round)).rejects.toMatchObject({
            type: 'UNKNOWN_MEMBER_ID',
          })
          expect(harness.requests).toHaveLength(1)
          expect(survivor(protocol, replacement)).toBe('B')
        } finally {
          round.retire()
          await harness.pool.destroy()
        }
      })

      it('leaves the replacement alone when the SyncGroup it answered is obsolete', async () => {
        const { group, protocol, round, retire } = withReplacement(replacement)
        protocol.rebalance = async () => {
          retire()
          throw unknownMember()
        }
        try {
          await expect(group.joinAndSyncAttempt(round)).rejects.toMatchObject({
            type: 'UNKNOWN_MEMBER_ID',
          })
          expect(survivor(protocol, replacement)).toBe('B')
        } finally {
          round.retire()
        }
      })

      it('leaves the replacement alone when the heartbeat it answered is obsolete', async () => {
        const { protocol, coordinator, retire } = withReplacement(replacement)
        protocol.coordinator = coordinator
        protocol.generationId = 1
        protocol.lastRequest = 0
        coordinator.heartbeat.mockImplementation(async () => {
          retire()
          throw unknownMember()
        })
        const answered = await protocol.sharedHeartbeat({ interval: 0 }).catch((error) => error)

        expect(answered).toMatchObject({ type: 'UNKNOWN_MEMBER_ID' })
        expect(protocol.forgetMemberId(sentMembership(answered))).toBe(false)
        expect(survivor(protocol, replacement)).toBe('B')
      })
    })

    it('leaves the replacement alone when the answer belongs to a round that was replaced', async () => {
      const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
      const Encoder = require('../../protocol/encoder')
      const { group, protocol, round } = withReplacement('accepted')
      let replacing
      const harness = createGroupApiBroker({
        versions: { 11: { minVersion: 0, maxVersion: 8 } },
        respond: () => {
          protocol.leave({ leaveGroup: false })
          replacing = group.beginRebalanceRound()
          protocol.memberId = 'B'
          return new Encoder()
            .writeInt32(0)
            .writeInt16(25)
            .writeInt32(5)
            .writeUVarIntString('consumer')
            .writeUVarIntString('RoundRobinAssigner')
            .writeUVarIntString('A')
            .writeUVarIntString('A')
            .writeUVarIntArray([])
            .writeTaggedFields().buffer
        },
      })
      protocol.cluster.findGroupCoordinator = async () => harness.broker
      protocol.rebalance = ({ round: attempt }) =>
        protocol.join({ topicsSubscribed: TOPICS, round: attempt })
      try {
        await expect(group.joinAndSyncAttempt(round)).rejects.toBeDefined()
        expect(round.guard()).not.toBeNull()
        expect(harness.requests).toHaveLength(1)
        expect(protocol.memberId).toBe('B')
      } finally {
        replacing?.retire()
        await harness.pool.destroy()
      }
    })

    it('stamps the membership an OffsetCommit began under on its answer', async () => {
      const OffsetManager = require('../offsetManager')
      const manager = Object.create(OffsetManager.prototype)
      Object.assign(manager, {
        ownership: null,
        membershipProtocol: 'classic',
        membership: () => ({ revision: 4 }),
        withTopicIds: (topics) => topics,
        getCoordinator: async () => ({
          offsetCommit: async () => {
            throw unknownMember()
          },
        }),
      })

      const answered = await manager
        .sendOffsetCommit({ groupGenerationId: 1, topics: [] })
        .catch((error) => error)

      expect(answered).toMatchObject({ type: 'UNKNOWN_MEMBER_ID' })
      expect(sentMembership(answered)).toBe(4)
    })

    it('forgets the identity when the answer belongs to the current membership', async () => {
      const { group, protocol, round } = withReplacement('pending')
      protocol.rebalance = async () => {
        throw unknownMember()
      }
      protocol.keepPendingMemberId('C', protocol.membershipRevision)
      try {
        await expect(group.joinAndSyncAttempt(round)).rejects.toMatchObject({
          type: 'UNKNOWN_MEMBER_ID',
        })
        expect(protocol.memberId).toBeNull()
        expect(protocol.pendingMemberId).toBeNull()
      } finally {
        round.retire()
      }
    })
  })
})
