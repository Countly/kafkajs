const { Kafka } = require('../../../index')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { waitForCondition } = require('./fixtures/liveScenario')

const NODES = [{ id: 0, host: 'node0', port: 9092 }]
const REBALANCE_IN_PROGRESS = 27
const MEMBER_ID_REQUIRED = 79
const KILLED = new Error('Connection killed')
const RETRY = { retries: 3, initialRetryTime: 10, maxRetryTime: 20 }

/**
 * A coordinator that makes every new member join again with an id it hands out (`assigned-1`,
 * `assigned-2`, ...), accepts a join that names an id, and starts a rebalance on demand: once
 * `rebalance()` is called it answers heartbeats with REBALANCE_IN_PROGRESS until the next join
 * that names an id is answered.
 * @returns {{ cluster: ReturnType<typeof createScriptedCluster>, rebalance: () => void,
 *   joinIds: () => string[], lose: (memberId: string) => void }} `joinIds` is the member id of
 *   every JoinGroup written so far; `lose` kills the connection of the next JoinGroup that names
 *   `memberId`, as a connection killed while the join is parked does
 */
const coordinator = () => {
  const cluster = groupScript(createScriptedCluster({ nodes: NODES }), { batches: 0 })
  let assigned = 0
  let rebalancing = false
  cluster
    .answer('JoinGroup', ({ memberId, protocolType, groupProtocols: [protocol] }) => {
      if (memberId === '') {
        return { errorCode: MEMBER_ID_REQUIRED, memberId: `assigned-${(assigned += 1)}` }
      }
      rebalancing = false
      return {
        protocolType,
        memberId,
        generationId: 1,
        leaderId: memberId,
        groupProtocol: protocol.name,
        members: [{ memberId, groupInstanceId: null, memberMetadata: protocol.metadata }],
      }
    })
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
    lose: (memberId) =>
      cluster.schedule.once(
        { api: 'JoinGroup', match: ({ fields }) => fields.memberId === memberId },
        KILLED
      ),
  }
}

describe('Consumer > a JoinGroup lost while parked', () => {
  let consumer

  /**
   * Runs a consumer against the cluster until it has joined.
   * @param {ReturnType<typeof createScriptedCluster>} cluster
   * @returns {Promise<object[]>} the crash events seen, which stay empty: the join is tried again
   *   on the same consumer, not restarted
   */
  const join = async (cluster) => {
    const crashes = []
    consumer = new Kafka(cluster.kafkaOptions()).consumer({
      groupId: 'join-member-id',
      heartbeatInterval: 20,
      maxWaitTimeInMs: 20,
      retry: RETRY,
    })
    consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload))
    await consumer.connect()
    await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })
    await consumer.run({ eachMessage: async () => {} })
    return crashes
  }

  afterEach(async () => {
    await consumer?.disconnect()
    consumer = undefined
  })

  it('starts a new member over without the id the coordinator assigned, as kafkajs 2.2.4 does, leaving the first member in the group', async () => {
    const { cluster, joinIds, lose } = coordinator()
    lose('assigned-1')

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
})
