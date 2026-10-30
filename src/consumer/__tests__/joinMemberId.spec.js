const { Kafka } = require('../../../index')
const apiKeys = require('../../protocol/requests/apiKeys')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { createChaosSocketFactory } = require('../../../testHelpers/chaosSocket')
const { waitForCondition } = require('./fixtures/liveScenario')

const NODES = [{ id: 0, host: 'node0', port: 9092 }]
const REBALANCE_IN_PROGRESS = 27
const UNKNOWN_MEMBER_ID = 25
const MEMBER_ID_REQUIRED = 79
const KILLED = new Error('Connection killed')
const RETRY = { retries: 3, initialRetryTime: 10, maxRetryTime: 20 }

/**
 * A coordinator that makes every new member join again with an id it hands out (`assigned-1`,
 * `assigned-2`, ...), accepts a join that names an id, and starts a rebalance on demand: once
 * `rebalance()` is called it answers heartbeats with REBALANCE_IN_PROGRESS until the next join
 * that names an id is answered.
 * A member that names a static instance id needs no such handshake: the coordinator answers its
 * empty-id join with the member id `static-1`.
 * @returns {{ cluster: ReturnType<typeof createScriptedCluster>, rebalance: () => void,
 *   joinIds: () => string[], instanceIds: () => Array<string|null>,
 *   frames: (api: string) => object[], allocations: () => number, lose: (memberId: string) => void,
 *   forget: (memberId: string) => void }} `joinIds` is the member id of every JoinGroup written
 *   so far, `instanceIds` its instance id, `allocations` the number of ids handed out; `lose` kills the connection of the next JoinGroup that names `memberId`, as a
 *   connection killed while the join is parked does; `forget` makes the next JoinGroup that names
 *   `memberId` answer UNKNOWN_MEMBER_ID, as a coordinator that dropped the member does
 */
const coordinator = () => {
  const cluster = groupScript(createScriptedCluster({ nodes: NODES }), { batches: 0 })
  let assigned = 0
  let rebalancing = false
  const forgotten = new Set()
  cluster
    .answer(
      'JoinGroup',
      ({ memberId, groupInstanceId, protocolType, groupProtocols: [protocol] }) => {
        if (forgotten.delete(memberId)) return { errorCode: UNKNOWN_MEMBER_ID }
        if (memberId === '' && groupInstanceId === null) {
          return { errorCode: MEMBER_ID_REQUIRED, memberId: `assigned-${(assigned += 1)}` }
        }
        rebalancing = false
        return {
          protocolType,
          memberId: memberId || 'static-1',
          generationId: 1,
          leaderId: memberId || 'static-1',
          groupProtocol: protocol.name,
          members: [
            {
              memberId: memberId || 'static-1',
              groupInstanceId,
              memberMetadata: protocol.metadata,
            },
          ],
        }
      }
    )
    .answer('Heartbeat', () => (rebalancing ? { errorCode: REBALANCE_IN_PROGRESS } : {}))
  return {
    cluster,
    rebalance: () => {
      rebalancing = true
    },
    joinIds: () =>
      cluster
        .requests()
        .filter(({ api }) => api === 'JoinGroup')
        .map(({ fields }) => fields.memberId),
    instanceIds: () =>
      cluster
        .requests()
        .filter(({ api }) => api === 'JoinGroup')
        .map(({ fields }) => fields.groupInstanceId),
    frames: (api) => cluster.requests().filter((request) => request.api === api),
    allocations: () => assigned,
    lose: (memberId) =>
      cluster.schedule.once(
        { api: 'JoinGroup', match: ({ fields }) => fields.memberId === memberId },
        KILLED
      ),
    forget: (memberId) => forgotten.add(memberId),
  }
}

describe('Consumer > a JoinGroup lost while parked', () => {
  let consumer

  /**
   * Runs a consumer against the cluster until it has joined.
   * @param {ReturnType<typeof createScriptedCluster>} cluster
   * @param {object} [overrides] consumer options laid over the defaults
   * @returns {Promise<object[]>} the crash events seen, which stay empty: the join is tried again
   *   on the same consumer, not restarted
   */
  const join = async (cluster, overrides = {}) => {
    const crashes = start(cluster, overrides)
    await consumer.connect()
    await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })
    await consumer.run({ eachMessage: async () => {} })
    return crashes
  }

  /**
   * Builds the consumer without connecting it.
   * @param {ReturnType<typeof createScriptedCluster>} cluster
   * @param {object} [overrides] consumer options laid over the defaults
   * @param {object} [kafkaOverrides] client options laid over the cluster's
   * @returns {object[]} the crash events, filled as they happen
   */
  const start = (cluster, overrides = {}, kafkaOverrides = {}) => {
    const crashes = []
    consumer = new Kafka(cluster.kafkaOptions(kafkaOverrides)).consumer({
      groupId: 'join-member-id',
      heartbeatInterval: 20,
      maxWaitTimeInMs: 20,
      retry: RETRY,
      ...overrides,
    })
    consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload))
    return crashes
  }

  /**
   * Connects, subscribes and runs the consumer without waiting for the first join, which the
   * test may hold.
   * @returns {Promise<void>} resolves once `run` was called, not once it settled
   */
  const begin = async () => {
    await consumer.connect()
    await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })
    consumer.run({ eachMessage: async () => {} }).catch(() => {})
  }

  afterEach(async () => {
    await consumer?.disconnect()
    consumer = undefined
  })

  it('reuses the MEMBER_ID_REQUIRED id after a parked initial JoinGroup is lost', async () => {
    const { cluster, joinIds, lose } = coordinator()
    lose('assigned-1')

    const crashes = await join(cluster)

    expect(joinIds()).toEqual(['', 'assigned-1', 'assigned-1'])
    expect(crashes).toEqual([])
  })

  it('keeps the one assigned id through repeated losses of the initial JoinGroup', async () => {
    const { cluster, joinIds, lose } = coordinator()
    lose('assigned-1')
    lose('assigned-1')
    lose('assigned-1')

    const crashes = await join(cluster)

    expect(joinIds()).toEqual(['', 'assigned-1', 'assigned-1', 'assigned-1', 'assigned-1'])
    expect(crashes).toEqual([])
  })

  it('keeps the one assigned id when the follow-up JoinGroup answers are dropped and the connection ends', async () => {
    const { cluster, joinIds, allocations } = coordinator()
    const chaos = createChaosSocketFactory(cluster.socketFactory)
    const losses = 3
    let lost = 0
    let disarm
    const arm = () => {
      if (disarm) return
      disarm = chaos.arm({}, (socket) => {
        socket.dropResponses(1, { apiKey: apiKeys.JoinGroup, thenClose: true })
        socket.once('end', () => {
          lost += 1
          if (lost === losses) disarm()
        })
      })
    }

    const crashes = start(cluster, {}, { socketFactory: chaos })
    consumer.on(consumer.events.REQUEST, ({ payload }) => {
      if (payload.apiName === 'JoinGroup') arm()
    })
    await consumer.connect()
    await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })
    await consumer.run({ eachMessage: async () => {} })

    expect(lost).toBe(losses)
    expect(joinIds()).toEqual(['', ...Array(losses + 1).fill('assigned-1')])
    expect(allocations()).toBe(1)
    expect(crashes).toEqual([])
  })

  it('asks for a new id when the coordinator no longer knows the one it assigned', async () => {
    const { cluster, joinIds, forget } = coordinator()
    forget('assigned-1')

    const crashes = await join(cluster)

    expect(joinIds()).toEqual(['', 'assigned-1', '', 'assigned-2'])
    expect(crashes).toEqual([])
  })

  it('keeps the id of a member that has one, so a rejoin lost while parked is sent again under the same id', async () => {
    const { cluster, joinIds, lose, rebalance } = coordinator()
    await join(cluster)
    expect(joinIds()).toEqual(['', 'assigned-1'])

    lose('assigned-1')
    rebalance()
    const rejoined = await waitForCondition(() => joinIds().length === 4, {
      attempts: 400,
      delay: 5,
    })

    expect(rejoined).toBe(true)
    expect(joinIds()).toEqual(['', 'assigned-1', 'assigned-1', 'assigned-1'])
  })

  describe('an established member id and the coordinator verdicts', () => {
    const WAIT = { attempts: 400, delay: 5 }

    it('asks for a new id when a rejoin finds the established one unknown', async () => {
      const { cluster, joinIds, forget, rebalance } = coordinator()
      await join(cluster)

      forget('assigned-1')
      rebalance()
      const rejoined = await waitForCondition(() => joinIds().length === 5, WAIT)

      expect(rejoined).toBe(true)
      expect(joinIds()).toEqual(['', 'assigned-1', 'assigned-1', '', 'assigned-2'])
    })

    it('asks for a new id when SyncGroup finds the member unknown', async () => {
      const { cluster, joinIds } = coordinator()
      cluster.schedule.once({ api: 'SyncGroup' }, { errorCode: UNKNOWN_MEMBER_ID })

      const crashes = await join(cluster)

      expect(joinIds()).toEqual(['', 'assigned-1', '', 'assigned-2'])
      expect(crashes).toEqual([])
    })

    it('asks for a new id when a heartbeat finds the member unknown', async () => {
      const { cluster, joinIds } = coordinator()
      await join(cluster)

      cluster.schedule.once({ api: 'Heartbeat' }, { errorCode: UNKNOWN_MEMBER_ID })
      const rejoined = await waitForCondition(() => joinIds().length === 4, WAIT)

      expect(rejoined).toBe(true)
      expect(joinIds()).toEqual(['', 'assigned-1', '', 'assigned-2'])
    })

    it('asks for a new id when an OffsetCommit finds the member unknown', async () => {
      const { cluster, joinIds, frames } = coordinator()
      await join(cluster)
      cluster.schedule.once(
        { api: 'OffsetCommit' },
        {
          topics: [
            { topic: 'test-topic', partitions: [{ partition: 0, errorCode: UNKNOWN_MEMBER_ID }] },
          ],
        }
      )

      await consumer
        .commitOffsets([{ topic: 'test-topic', partition: 0, offset: '1' }])
        .catch(() => {})
      const rejoined = await waitForCondition(() => joinIds().length === 4, WAIT)

      expect(rejoined).toBe(true)
      expect(frames('OffsetCommit').length).toBeGreaterThanOrEqual(1)
      expect(joinIds()).toEqual(['', 'assigned-1', '', 'assigned-2'])
    })

    it.each([
      ['SyncGroup', { errorCode: 22 }, 'ILLEGAL_GENERATION'],
      ['Heartbeat', { errorCode: 22 }, 'ILLEGAL_GENERATION'],
      ['Heartbeat', { errorCode: 14 }, 'COORDINATOR_LOAD_IN_PROGRESS'],
      ['Heartbeat', { errorCode: 15 }, 'COORDINATOR_NOT_AVAILABLE'],
      ['Heartbeat', { errorCode: 16 }, 'NOT_COORDINATOR'],
      ['Heartbeat', KILLED, 'a connection lost on the heartbeat'],
    ])('keeps the id through a %s failure: %s', async (api, fault) => {
      const { cluster, joinIds, frames, allocations } = coordinator()
      if (api === 'SyncGroup') cluster.schedule.once({ api }, fault)
      await join(cluster)
      const before = frames('Heartbeat').length
      if (api === 'Heartbeat') cluster.schedule.once({ api }, fault)

      const settled = await waitForCondition(
        () => cluster.schedule.rules.length === 0 && frames('Heartbeat').length >= before + 3,
        WAIT
      )

      expect(settled).toBe(true)
      expect(joinIds()[0]).toBe('')
      expect(
        joinIds()
          .slice(1)
          .every((id) => id === 'assigned-1')
      ).toBe(true)
      expect(allocations()).toBe(1)
    })
  })

  describe('a stop or disconnect while the follow-up JoinGroup is parked', () => {
    it.each([
      ['stop', false],
      ['stop', true],
      ['disconnect', false],
      ['disconnect', true],
    ])(
      'writes nothing after %s, leave failing %s, and starts the next run without an id',
      async (how, leaveFails) => {
        const { cluster, joinIds, frames } = coordinator()
        const parked = cluster.schedule.hold({
          api: 'JoinGroup',
          match: ({ fields }) => fields.memberId === 'assigned-1',
        })
        if (leaveFails) cluster.schedule.once({ api: 'LeaveGroup' }, KILLED)
        start(cluster)
        await begin()
        await parked.arrived()
        const written = cluster.requests().length

        const closing = how === 'stop' ? consumer.stop() : consumer.disconnect()
        parked.release()
        await closing
        await waitForCondition(() => false, { attempts: 10, delay: 5 })

        const after = cluster
          .requests()
          .slice(written)
          .map(({ api }) => api)
        expect(after.filter((api) => api !== 'LeaveGroup')).toEqual([])
        expect(joinIds()).toEqual(['', 'assigned-1'])
        if (how === 'disconnect') {
          await consumer.connect()
          await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })
        }
        await consumer.run({ eachMessage: async () => {} })
        expect(joinIds()).toEqual(['', 'assigned-1', '', 'assigned-2'])
        expect(frames('Heartbeat').every(({ fields }) => fields.memberId !== 'assigned-1')).toBe(
          true
        )
      }
    )
  })

  describe('a member that has not joined', () => {
    it('sends no heartbeat or commit under the pending id and reports no join', async () => {
      const { cluster, joinIds, frames } = coordinator()
      const parked = cluster.schedule.hold({
        api: 'JoinGroup',
        match: ({ fields }) => fields.memberId === 'assigned-1',
      })
      const joins = []
      start(cluster)
      consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => joins.push(payload))
      await begin()
      await parked.arrived()

      const commit = consumer
        .commitOffsets([{ topic: 'test-topic', partition: 0, offset: '1' }])
        .then(
          () => null,
          (error) => error
        )
      await Promise.race([commit, waitForCondition(() => false, { attempts: 20, delay: 5 })])

      const underPending = ['Heartbeat', 'OffsetCommit', 'SyncGroup'].flatMap(frames)
      expect(underPending.filter(({ fields }) => fields.memberId === 'assigned-1')).toEqual([])
      expect(joins).toEqual([])
      parked.release()
      const joined = await waitForCondition(() => joins.length === 1, { attempts: 400, delay: 5 })
      expect(joined).toBe(true)
      expect(joinIds()).toEqual(['', 'assigned-1'])
      expect(joins[0].memberId).toBe('assigned-1')
    })
  })

  describe('a fenced static member', () => {
    it('ends the run without restarting or joining again after a scripted 79 and 82', async () => {
      const { cluster, joinIds, instanceIds } = coordinator()
      cluster.schedule.once(
        { api: 'JoinGroup', match: ({ fields }) => fields.memberId === 'assigned-1' },
        { errorCode: 82 }
      )
      cluster.answer('JoinGroup', ({ memberId, groupInstanceId }) =>
        memberId === ''
          ? { errorCode: MEMBER_ID_REQUIRED, memberId: 'assigned-1' }
          : { groupInstanceId }
      )
      const crashes = start(cluster, {
        groupInstanceId: 'instance-1',
        restartOnFailure: async () => true,
      })

      await begin()
      const crashed = await waitForCondition(() => crashes.length === 1, {
        attempts: 400,
        delay: 5,
      })
      await waitForCondition(() => false, { attempts: 20, delay: 5 })

      expect(crashed).toBe(true)
      expect(crashes[0]).toMatchObject({ restart: false, error: { type: 'FENCED_INSTANCE_ID' } })
      expect(joinIds()).toEqual(['', 'assigned-1'])
      expect(instanceIds()).toEqual(['instance-1', 'instance-1'])
    })
  })

  describe('a static member', () => {
    it('names its instance id on every join and keeps it when the member id is cleared', async () => {
      const { cluster, joinIds, instanceIds, forget, lose, rebalance } = coordinator()
      await join(cluster, { groupInstanceId: 'instance-1' })

      lose('static-1')
      rebalance()
      await waitForCondition(() => joinIds().length === 3, { attempts: 400, delay: 5 })
      forget('static-1')
      rebalance()
      const cleared = await waitForCondition(() => joinIds().length === 5, {
        attempts: 400,
        delay: 5,
      })

      expect(cleared).toBe(true)
      expect(joinIds()).toEqual(['', 'static-1', 'static-1', 'static-1', ''])
      expect(new Set(instanceIds())).toEqual(new Set(['instance-1']))
    })
  })

  describe('a second consumer with the same configuration', () => {
    it('starts without the id the first one was assigned', async () => {
      const { cluster, joinIds } = coordinator()
      await join(cluster)
      const first = consumer

      await join(cluster)

      expect(joinIds().slice(0, 4)).toEqual(['', 'assigned-1', '', 'assigned-2'])
      await first.disconnect()
    })
  })
})
