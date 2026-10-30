const Cluster = require('../../cluster')
const Connection = require('../../network/connection')
const ClassicGroupProtocol = require('../groupProtocol/classic')
const ConsumerGroup = require('../consumerGroup')
const IdleWait = require('../idleWait')
const RoundRobinAssigner = require('../assigners/roundRobinAssigner')
const { MemberMetadata, MemberAssignment } = require('../assignerProtocol')
const apiKeys = require('../../protocol/requests/apiKeys')
const { newLogger } = require('testHelpers')
const { Schedule } = require('../../../testHelpers/schedule')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { watch, flushAll } = require('../../producer/__tests__/fixtures/bufferHarness')

const NODES = [
  { id: 0, host: 'node0', port: 9092 },
  { id: 1, host: 'node1', port: 9093 },
]
const SASL = { mechanism: 'plain', username: 'u', password: 'p' }
const EXPIRED = { type: 'JOIN_DEADLINE_EXCEEDED' }
const STAGES = ['connect', 'negotiation', 'auth', 'encoding', 'queue', 'response']
const REDIAL = ['connect', 'negotiation', 'auth']

/** One stage the test holds: `arrived()` settles when the stage is reached, `release()` lets it go. */
class Hold {
  constructor() {
    this.reached = new Promise((resolve) => {
      this.reach = resolve
    })
    this.opened = new Promise((resolve) => {
      this.open = resolve
    })
  }

  /** @returns {Promise<void>} resolves once the stage is reached */
  arrived() {
    return this.reached
  }

  /** @returns {void} lets the held stage continue */
  release() {
    this.open()
  }

  /** @returns {Promise<void>} what the stage awaits: reached now, resolved at `release` */
  pass() {
    this.reach()
    return this.opened
  }
}

/** @param {...string} topics @returns {Buffer} a classic subscription */
const sub = (...topics) => MemberMetadata.encode({ version: 0, topics })

/** @param {object} fields @returns {string|undefined} the topic names a Metadata request asked for */
const asked = (fields) =>
  fields.topics
    ?.map(({ name }) => name)
    .sort()
    .join()

/**
 * @param {Array<{ id: number, leader?: number, error?: number }>} partitions
 * @param {number} [errorCode] the topic's own error
 * @returns {{ errorCode: number, partitions: object[] }} one topic of the scripted metadata
 */
const topic = (partitions, errorCode = 0) => ({ errorCode, partitions })

/**
 * Answers Metadata from `table`: a name it has not got is UNKNOWN_TOPIC_OR_PARTITION, as with
 * automatic creation off.
 * @param {Map<string, ReturnType<typeof topic>>} table
 * @returns {(fields: object) => object} the answerer
 */
const metadataFrom = (table) => (fields) => ({
  brokers: NODES.map(({ id, host, port }) => ({ nodeId: id, host, port, rack: null })),
  controllerId: 0,
  topics: (fields.topics ?? [...table.keys()].map((name) => ({ name }))).map(({ name }) => {
    const { errorCode, partitions } = table.get(name) ?? topic([], 3)
    return {
      errorCode,
      name,
      topicId: `00000000-0000-0000-0000-${Buffer.from(name).toString('hex').padStart(12, '0')}`,
      isInternal: false,
      topicAuthorizedOperations: -2147483648,
      partitions: partitions.map(({ id, leader = 0, error = 0 }) => ({
        errorCode: error,
        partitionIndex: id,
        leaderId: leader,
        leaderEpoch: 1,
        replicas: [0],
        isr: [0],
        offlineReplicas: [],
      })),
    }
  }),
})

let now = 0
let open = []
let encoding = null

/**
 * A real `Cluster`, with its brokers, connections and request queues, over the scripted wire: the
 * only substitutes are the sockets and the clock.
 * @param {object} [options]
 * @param {boolean} [options.sasl] authenticates every connection
 * @param {boolean} [options.enforceRequestTimeout]
 * @param {number|null} [options.maxInFlightRequests]
 * @param {boolean} [options.allowAutoTopicCreation]
 * @param {number} [options.metadataMaxAge] how long the cluster trusts its metadata
 * @param {number} [options.coordinator] the id of the node that coordinates the group
 * @param {boolean} [options.redial] leaves every broker disconnected, so the next request dials
 * @param {object} [options.peer] another rig whose wire, schedule and topics this one shares
 * @returns {Promise<object>} the rig: `cluster`, `wire`, `schedule`, `table`, `frames`, `hold`,
 *   `finish` and the other handles the tests use
 */
const rig = async ({
  sasl = false,
  enforceRequestTimeout = false,
  maxInFlightRequests = null,
  allowAutoTopicCreation = true,
  metadataMaxAge,
  coordinator = 0,
  redial = false,
  peer = null,
} = {}) => {
  const schedule = peer?.schedule ?? new Schedule()
  const wire =
    peer?.wire ??
    createScriptedCluster({ nodes: NODES, schedule, profile: sasl ? 'sasl' : 'consumer' })
  const table = peer?.table ?? new Map([['A', topic([{ id: 0 }])]])
  const armed = { connect: null }
  const [{ host, port }] = NODES.filter(({ id }) => id === coordinator)
  if (!peer) {
    wire.answer('Metadata', metadataFrom(table))
    wire
      .answer('FindCoordinator', ({ coordinatorKeys }) => ({
        coordinators: coordinatorKeys.map((key) => ({
          key,
          nodeId: coordinator,
          host,
          port,
          errorCode: 0,
          errorMessage: null,
        })),
      }))
      .answer('SyncGroup', () => ({
        throttleTime: 0,
        memberAssignment: MemberAssignment.encode({ version: 1, assignment: { A: [0] } }).toString(
          'hex'
        ),
      }))
  }
  const { clientId, brokers, socketFactory } = wire.kafkaOptions()
  const cluster = new Cluster({
    logger: newLogger(),
    clientId,
    brokers,
    socketFactory: (options) =>
      socketFactory(
        armed.connect
          ? {
              ...options,
              onConnect: () => {
                const held = armed.connect
                armed.connect = null
                held.pass().then(options.onConnect)
              },
            }
          : options
      ),
    connectionTimeout: 1000,
    requestTimeout: 30000,
    enforceRequestTimeout,
    maxInFlightRequests,
    allowAutoTopicCreation,
    metadataMaxAge,
    sasl: sasl ? SASL : undefined,
    retry: { retries: 0 },
  })
  await cluster.connect()
  await cluster.addTargetTopic('A')
  if (redial) {
    const everyBroker = [
      ...Object.values(cluster.brokerPool.brokers),
      cluster.brokerPool.seedBroker,
    ]
    await Promise.all(everyBroker.map((broker) => broker.disconnect()))
  }

  const connected = () => Object.values(cluster.brokerPool.brokers).find((b) => b.isConnected())
  const env = {
    cluster,
    wire,
    schedule,
    table,
    /** @returns {object[]} the requests of `api` written so far, those naming `topics` when given */
    frames: (api, topics) =>
      wire.requests().filter((r) => r.api === api && (!topics || asked(r.fields) === topics)),
    /** @returns {object[]} what the snapshot of topic B wrote */
    snapshots: () => env.frames('Metadata', 'B'),
    /** @returns {{ state: string, value: unknown }} a watched snapshot of topic B */
    snapshot: (budget = 100) =>
      watch(
        cluster.groupMetadata({
          topics: ['B'],
          allowAutoTopicCreation: false,
          deadlineAt: performance.now() + budget,
          guard: () => null,
        })
      ),
    /** @returns {{ arrived: Function, release: Function }} a hold on the leader's own refresh */
    holdRefresh: () =>
      schedule.hold({ api: 'Metadata', match: ({ fields }) => asked(fields) === 'A' }),
    /** @param {string} stage one of `STAGES` @returns {{ arrived: Function, release: Function }} */
    hold: (stage) => {
      if (stage === 'connect') return (armed.connect = new Hold())
      if (stage === 'encoding') return (encoding = new Hold())
      if (stage === 'negotiation') return schedule.hold({ api: 'ApiVersions' })
      if (stage === 'auth') return schedule.hold({ api: 'SaslAuthenticate' })
      if (stage === 'queue') {
        const gate = schedule.hold({
          api: 'Metadata',
          match: ({ fields }) => asked(fields) !== 'B',
        })
        connected()
          .metadata(['A'])
          .catch(() => {})
        return gate
      }

      return schedule.hold({ api: 'Metadata', match: ({ fields }) => asked(fields) === 'B' })
    },
    /** @returns {Promise<void>} disconnects, then checks the wire and the timers are clean */
    finish: async () => {
      await flushAll()
      await cluster.disconnect({ drainTimeout: 0 })
      await flushAll()
      wire.assertClean()
      expect(jest.getTimerCount()).toBe(0)
      open = open.filter((candidate) => candidate !== cluster)
    },
  }
  open.push(cluster)
  return env
}

/** @returns {Promise<void>} lets queued work and the throttle waits of the scripted wire run out */
const drain = async () => {
  for (let turn = 0; turn < 10; turn += 1) await flushAll()
}

/**
 * @param {{ state: string }} watched a watched promise
 * @returns {Promise<void>} resolves once it settled, after at most 50 turns of the event loop
 */
const settled = async (watched) => {
  for (let turn = 0; turn < 50 && watched.state === 'pending'; turn += 1) await flushAll()
}

/**
 * @param {{ state: string }} watched a watched promise that waits on retry backoff
 * @returns {Promise<void>} resolves once it settled, after at most 50 steps of 100 ms
 */
const driven = async (watched) => {
  for (let step = 0; step < 50 && watched.state === 'pending'; step += 1) await elapse(100)
}

/**
 * Moves the actual clock and, unless withheld, the timers with it.
 * @param {number} ms
 * @param {{ timers?: boolean }} [options] `timers: false` withholds every timer callback
 * @returns {Promise<void>}
 */
const elapse = async (ms, { timers = true } = {}) => {
  now += ms
  if (timers) await jest.advanceTimersByTimeAsync(ms)
  await flushAll()
}

/**
 * A round owner with the lifecycle boundaries the stop path needs.
 * @param {object} cluster
 * @param {object} [extra] fields laid over it
 * @returns {object} real `ConsumerGroup` round methods
 */
const roundGroup = (cluster, extra = {}) => {
  const group = Object.create(ConsumerGroup.prototype)
  return Object.assign(group, {
    idleWait: new IdleWait(),
    rebalanceTimeout: 1000,
    cluster,
    rebalanceCoordinator: { runner: { stopRequested: false } },
    subscriptionState: { clearPauses() {} },
    seekOffset: new Map(),
    groupMembership: { onStopRequested() {} },
    ...extra,
  })
}

/** @param {object} group @returns {void} a terminal stop through the real lifecycle method */
const stop = (group) => {
  group.rebalanceCoordinator.runner.stopRequested = true
  group.onStopRequested()
}

/** @param {Function} assign @returns {object} a custom assigner */
const custom = (assign) => ({
  name: 'custom',
  assign,
  protocol: ({ topics }) => ({ name: 'custom', metadata: sub(...topics) }),
})

/**
 * A classic protocol over the real cluster and its real coordinator, joined as the leader.
 * @param {object} env the rig
 * @param {object} assigner
 * @param {Array<[string, Buffer]>} members `[memberId, subscription]` of every member
 * @returns {Promise<ClassicGroupProtocol>}
 */
const leader = async (env, assigner, members) => {
  const protocol = new ClassicGroupProtocol({
    cluster: env.cluster,
    groupId: 'group',
    groupInstanceId: null,
    sessionTimeout: 30000,
    rebalanceTimeout: 1000,
    assigners: [assigner],
    instrumentationEmitter: { emit() {} },
    logger: newLogger(),
  })
  return Object.assign(protocol, {
    memberId: 'a',
    leaderId: 'a',
    generationId: 1,
    groupProtocol: assigner.name,
    protocolType: 'consumer',
    members: members.map(([memberId, memberMetadata]) => ({ memberId, memberMetadata })),
    coordinator: await env.cluster.findGroupCoordinator({ groupId: 'group' }),
  })
}

/**
 * @param {ClassicGroupProtocol} protocol
 * @param {object} round the round, or an attempt of it
 * @param {() => void} [beforeSync]
 * @returns {{ state: string, value: unknown }} the watched leader sync
 */
const syncing = (protocol, round, beforeSync = () => {}) =>
  watch(
    protocol.sync({ topics: ['A'], topicsSubscribed: ['A'], beforeSync, round: round.attempt() })
  )

/** @param {object} frame a SyncGroup request @returns {object} member id to decoded assignment */
const assignments = (frame) =>
  Object.fromEntries(
    frame.fields.groupAssignment.map(({ memberId, memberAssignment }) => [
      memberId,
      MemberAssignment.decode(Buffer.from(memberAssignment, 'hex')).assignment,
    ])
  )

/**
 * A real `ConsumerGroup` over the rig's cluster, joined as the leader of a classic group whose
 * members are `members`. The sync and the apply are the real ones; only the offset and position
 * work of a fetch is left out.
 * @param {object} env the rig, made with `metadataMaxAge`
 * @param {Array<[string, Buffer]>} members `[memberId, subscription]`, the leader first
 * @returns {Promise<{ group: ConsumerGroup, protocol: ClassicGroupProtocol, round: object }>}
 *   the group after its first assignment was applied
 */
const assigned = async (env, members) => {
  const group = new ConsumerGroup({
    cluster: env.cluster,
    groupId: 'group',
    topics: ['A'],
    topicConfigurations: { A: {} },
    logger: newLogger(),
    instrumentationEmitter: { emit() {} },
    assigners: [RoundRobinAssigner({ cluster: env.cluster })],
    sessionTimeout: 30000,
    rebalanceTimeout: 1000,
    maxWaitTimeInMs: 100,
    metadataMaxAge: 5000,
    retry: { retries: 0 },
  })
  group.seekOffsets = async () => {}
  group.validatePositions = async () => {}
  const protocol = group.groupMembership
  Object.assign(protocol, {
    memberId: 'a',
    leaderId: 'a',
    generationId: 1,
    groupProtocol: protocol.assigners[0].name,
    protocolType: 'consumer',
    members: members.map(([memberId, memberMetadata]) => ({ memberId, memberMetadata })),
    coordinator: await env.cluster.findGroupCoordinator({ groupId: 'group' }),
  })
  const round = group.beginRebalanceRound()
  const result = await protocol.sync({
    topics: group.topics,
    topicsSubscribed: group.topicsSubscribed,
    beforeSync: () => {
      group.partitionsPerSubscribedTopic = group.generatePartitionsPerSubscribedTopic()
    },
    round: round.attempt(),
  })
  await group.applyRebalanceResult(result, { round })
  round.retire()
  return { group, protocol, round }
}

/** @param {ConsumerGroup} group @returns {Promise<void>} one fetch preparation on the current run */
const prepare = (group) => group.prepareFetch(0, group.idleWait, group.ownership.token())

describe('leader union over the real transport', () => {
  beforeEach(() => {
    expect(typeof Cluster.prototype.groupMetadata).toBe('function')
    expect(typeof ClassicGroupProtocol.prototype.leaderMetadata).toBe('function')
    expect(typeof ConsumerGroup.prototype.beginRebalanceRound).toBe('function')
    now = 0
    encoding = null
    const original = Connection.prototype.send
    jest.spyOn(Connection.prototype, 'send').mockImplementation(function send(protocol) {
      const held = encoding
      if (!held || protocol.request.apiKey !== apiKeys.Metadata) {
        return original.call(this, protocol)
      }

      encoding = null
      const encode = async () => {
        await held.pass()
        return protocol.request.encode()
      }
      return original.call(this, { ...protocol, request: { ...protocol.request, encode } })
    })
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
    jest.spyOn(performance, 'now').mockImplementation(() => now)
  })
  afterEach(async () => {
    jest.restoreAllMocks()
    await Promise.all(open.map((cluster) => cluster.disconnect({ drainTimeout: 0 })))
    open = []
    jest.clearAllTimers()
    jest.useRealTimers()
  })

  describe('the union', () => {
    it.each([
      ['members name the other topics', [sub('A'), sub('B'), sub('C')]],
      ['the leader entry is empty', [Buffer.alloc(0), sub('B'), sub('C')]],
    ])('adds its own subscription without trusting the member echo: %s', async (_, subs) => {
      const env = await rig()
      env.table.set('B', topic([{ id: 0 }])).set('C', topic([{ id: 0 }]))
      const assign = jest.fn(async () => [])
      const protocol = await leader(env, custom(assign), [
        ['a', subs[0]],
        ['b', subs[1]],
        ['c', subs[2]],
      ])
      const round = roundGroup(env.cluster).beginRebalanceRound()
      const topics = ['A']

      const result = await protocol.sync({
        topics,
        topicsSubscribed: topics,
        beforeSync() {},
        round: round.attempt(),
      })

      expect(result).toMatchObject({ assignment: { A: [0] } })
      expect(assign.mock.calls[0][0].topics).toBe(topics)
      expect(assign.mock.calls[0][0].groupTopics).toEqual(['A', 'B', 'C'])
      expect([...assign.mock.calls[0][0].groupTopicMetadata.keys()].sort()).toEqual(['A', 'B', 'C'])
      expect(env.frames('Metadata', 'B,C')).toHaveLength(1)
      round.retire()
      await env.finish()
    })

    it('needs no extra Metadata when every member shares the leader subscription', async () => {
      const env = await rig()
      const assign = jest.fn(async () => [])
      const protocol = await leader(env, custom(assign), [
        ['a', sub('A')],
        ['b', sub('A')],
      ])
      const round = roundGroup(env.cluster).beginRebalanceRound()
      const topics = ['A']
      const before = env.wire.requests().length

      const result = await protocol.sync({
        topics,
        topicsSubscribed: topics,
        beforeSync() {},
        round: round.attempt(),
      })

      expect(result.unionSnapshot).toBeNull()
      expect(assign.mock.calls[0][0].topics).toBe(topics)
      expect(assign.mock.calls[0][0].groupTopics).toEqual(['A'])
      const metadata = env.wire
        .requests()
        .slice(before)
        .filter((r) => r.api === 'Metadata')
      expect(metadata.map((r) => asked(r.fields))).toEqual(['A'])
      round.retire()
      await env.finish()
    })

    it('excludes a topic error over a stale cached row and keeps a leaderless partition assignable', async () => {
      const env = await rig()
      env.table.set('B', topic([{ id: 2, leader: -1, error: 5 }])).set('C', topic([], 29))
      const targets = [...env.cluster.targetTopics]
      const real = RoundRobinAssigner({ cluster: env.cluster })
      const stale = {
        ...real,
        assign: async (args) => {
          env.table.set('C', topic([{ id: 99 }]))
          await env.cluster.brokerPool.refreshMetadata(['C'])
          expect(env.cluster.findTopicPartitionMetadata('C')).toMatchObject([{ partitionId: 99 }])
          return real.assign(args)
        },
      }
      const protocol = await leader(env, stale, [
        ['a', sub('A')],
        ['b', sub('B')],
        ['c', sub('C')],
      ])
      const round = roundGroup(env.cluster).beginRebalanceRound()

      await settled(syncing(protocol, round))

      const [frame] = env.frames('SyncGroup')
      expect(assignments(frame)).toEqual({ a: { A: [0] }, b: { B: [2] } })
      expect([...env.cluster.targetTopics]).toEqual(targets)
      expect(env.cluster.findTopicPartitionMetadata('B')).toEqual([])
      round.retire()
      await env.finish()
    })

    it.each([
      ['a literal subscription with creation allowed', true, {}, true],
      ['a literal subscription with creation refused', false, {}, false],
      [
        'a client pattern beside a literal, creation allowed',
        true,
        { regexSubscription: {} },
        false,
      ],
      [
        'a broker pattern beside a literal, creation allowed',
        true,
        { subscribedTopicRegex: '^A' },
        false,
      ],
    ])('sends the effective creation policy for %s', async (_, configured, pattern, expected) => {
      const env = await rig({ allowAutoTopicCreation: configured })
      env.table.set('B', topic([{ id: 0 }]))
      const protocol = await leader(
        env,
        custom(async () => []),
        [
          ['a', sub('A')],
          ['b', sub('B')],
        ]
      )
      const round = roundGroup(env.cluster, pattern).beginRebalanceRound()

      await settled(syncing(protocol, round))

      expect(env.frames('Metadata', 'B')[0].fields.allowAutoTopicCreation).toBe(expected)
      expect(env.cluster.allowAutoTopicCreation).toBe(configured)
      round.retire()
      await env.finish()
    })
  })

  describe('one snapshot failure', () => {
    it('keeps a transport failure, asks one broker once and fails over to none', async () => {
      const env = await rig()
      const { brokerPool } = env.cluster
      const [metadata, cache, targets] = [
        brokerPool.metadata,
        brokerPool.metadataCache,
        [...env.cluster.targetTopics],
      ]
      const sockets = env.wire.sockets().length
      env.schedule.once(
        { api: 'Metadata', match: ({ fields }) => asked(fields) === 'B' },
        new Error('reset by peer')
      )

      const result = env.snapshot()
      await drain()

      expect(result.state).toBe('rejected')
      expect(result.value).toMatchObject({ name: 'KafkaJSConnectionError' })
      expect(env.snapshots()).toHaveLength(1)
      expect(env.wire.requests().filter((r) => r.node === 1)).toEqual([])
      expect(env.wire.sockets()).toHaveLength(sockets)
      expect([brokerPool.metadata, brokerPool.metadataCache]).toEqual([metadata, cache])
      expect([...env.cluster.targetTopics]).toEqual(targets)
      await env.finish()
    })

    it('keeps an authentication failure and tries no other broker', async () => {
      const env = await rig({ sasl: true, redial: true })
      env.schedule.once(
        { api: 'SaslAuthenticate' },
        { errorCode: 58, errorMessage: 'bad credentials', authBytes: null }
      )

      const result = env.snapshot()
      await drain()

      expect(result.state).toBe('rejected')
      expect(result.value).toMatchObject({ name: 'KafkaJSSASLAuthenticationError' })
      expect(env.snapshots()).toEqual([])
      expect(env.wire.requests().filter((r) => r.node === 1)).toEqual([])
      await env.finish()
    })
  })

  describe('the round budget', () => {
    it('gives the snapshot half of what the join left, never a fresh budget', async () => {
      const env = await rig()
      const join = env.schedule.hold({ api: 'JoinGroup' })
      const snapshot = env.hold('response')
      env.wire.answer('JoinGroup', ({ groupProtocols: [offered] }) => ({
        throttleTime: 0,
        protocolType: 'consumer',
        memberId: 'a',
        generationId: 1,
        leaderId: 'a',
        groupProtocol: 'custom',
        members: [
          { memberId: 'a', groupInstanceId: null, memberMetadata: offered.metadata },
          { memberId: 'b', groupInstanceId: null, memberMetadata: sub('B').toString('hex') },
        ],
      }))
      env.table.set('B', topic([{ id: 0 }]))
      const protocol = await leader(
        env,
        custom(async () => []),
        []
      )
      Object.assign(protocol, { memberId: null, leaderId: null, generationId: null })
      const round = roundGroup(env.cluster).beginRebalanceRound()

      const rebalance = watch(
        protocol.rebalance({
          topics: ['A'],
          topicsSubscribed: ['A'],
          beforeSync() {},
          round: round.attempt(),
        })
      )
      await join.arrived()
      await elapse(800)
      join.release()
      await snapshot.arrived()
      await elapse(99)
      expect(rebalance.state).toBe('pending')
      await elapse(1)

      expect(rebalance.state).toBe('rejected')
      expect(rebalance.value).toMatchObject(EXPIRED)
      expect(round.roundDeadlineAt).toBe(1000)
      snapshot.release()
      await drain()
      expect(env.frames('SyncGroup')).toEqual([])
      round.retire()
      await env.finish()
    })

    it('gives the snapshot half of what the leader refresh left', async () => {
      const env = await rig()
      env.table.set('B', topic([{ id: 0 }]))
      const refresh = env.holdRefresh()
      const snapshot = env.hold('response')
      const protocol = await leader(
        env,
        custom(async () => []),
        [
          ['a', sub('A')],
          ['b', sub('B')],
        ]
      )
      const round = roundGroup(env.cluster).beginRebalanceRound()
      const syncing1 = syncing(protocol, round)
      await refresh.arrived()

      await elapse(800)
      refresh.release()
      await snapshot.arrived()
      await elapse(99)
      expect(syncing1.state).toBe('pending')
      await elapse(1)

      expect(syncing1.value).toMatchObject(EXPIRED)
      snapshot.release()
      await drain()
      expect(env.frames('SyncGroup')).toEqual([])
      round.retire()
      await env.finish()
    })

    it('installs nothing from an own refresh that answers after the round expired', async () => {
      const env = await rig()
      env.table.set('A', topic([{ id: 0 }, { id: 1 }]))
      const refresh = env.schedule.hold({
        api: 'Metadata',
        match: ({ fields }) => asked(fields) === 'A',
      })
      const protocol = await leader(
        env,
        custom(async () => []),
        [['a', sub('A')]]
      )
      const round = roundGroup(env.cluster).beginRebalanceRound()
      const sync = syncing(protocol, round)
      await refresh.arrived()

      await elapse(1000)
      refresh.release()
      await drain()

      expect(sync.value).toMatchObject(EXPIRED)
      expect(env.cluster.findTopicPartitionMetadata('A')).toHaveLength(1)
      expect(env.frames('SyncGroup')).toEqual([])
      round.retire()
      await env.finish()
    })

    it('makes its own refresh call beside a held shared one', async () => {
      const env = await rig()
      const shared = env.schedule.hold({
        api: 'Metadata',
        match: ({ fields }) => asked(fields) === 'A',
      })
      const before = env.frames('Metadata', 'A').length
      const unguarded = watch(env.cluster.refreshMetadata())
      await shared.arrived()

      const guarded = watch(env.cluster.refreshMetadata({ guard: () => null }))
      await drain()

      expect(env.frames('Metadata', 'A')).toHaveLength(before + 2)
      expect([unguarded.state, guarded.state]).toEqual(['pending', 'fulfilled'])
      shared.release()
      await drain()
      expect(unguarded.state).toBe('fulfilled')
      await env.finish()
    })
  })

  describe.each([true, false])('the snapshot with request-timeout enforcement %s', (enforce) => {
    it.each(STAGES)('settles at its own deadline while the %s stage is held', async (stage) => {
      const env = await rig({
        enforceRequestTimeout: enforce,
        maxInFlightRequests: stage === 'queue' ? 1 : null,
        sasl: stage === 'auth',
        redial: REDIAL.includes(stage),
      })
      const held = env.hold(stage)
      const result = env.snapshot()
      await held.arrived()
      await drain()

      await elapse(99)
      expect(result.state).toBe('pending')
      await elapse(1)
      expect(result.state).toBe('rejected')
      expect(result.value).toMatchObject(EXPIRED)
      held.release()
      await drain()

      expect(env.snapshots()).toHaveLength(stage === 'response' ? 1 : 0)
      expect(env.wire.requests().filter((r) => r.node === 1)).toEqual([])
      await env.finish()
    })

    it.each(STAGES)(
      'writes no late frame once the actual clock passed the deadline before the %s stage was released',
      async (stage) => {
        const env = await rig({
          enforceRequestTimeout: enforce,
          maxInFlightRequests: stage === 'queue' ? 1 : null,
          sasl: stage === 'auth',
          redial: REDIAL.includes(stage),
        })
        const held = env.hold(stage)
        const result = env.snapshot()
        await held.arrived()
        await drain()

        await elapse(101, { timers: false })
        expect(result.state).toBe('pending')
        held.release()
        await drain()

        expect(result.state).toBe('rejected')
        expect(result.value).toMatchObject(EXPIRED)
        expect(env.snapshots()).toHaveLength(stage === 'response' ? 1 : 0)
        await env.finish()
      }
    )
  })

  describe('SyncGroup and the assigner', () => {
    it.each([true, false])(
      'refuses a queued SyncGroup after the original expiry, enforcement %s',
      async (enforce) => {
        const env = await rig({
          coordinator: 1,
          maxInFlightRequests: 1,
          enforceRequestTimeout: enforce,
        })
        const assign = jest.fn(async () => [])
        const protocol = await leader(env, custom(assign), [['a', sub('A')]])
        const heartbeat = env.schedule.hold({ api: 'Heartbeat' })
        let ahead
        const round = roundGroup(env.cluster).beginRebalanceRound()
        const sync = syncing(protocol, round, () => {
          ahead = watch(
            protocol.coordinator.heartbeat({
              groupId: 'group',
              groupGenerationId: 1,
              memberId: 'a',
            })
          )
        })
        await heartbeat.arrived()
        await drain()
        expect(env.frames('SyncGroup')).toEqual([])

        await elapse(1001, { timers: false })
        heartbeat.release()
        await drain()

        expect(sync.value).toMatchObject(EXPIRED)
        expect(env.frames('SyncGroup')).toEqual([])
        expect(ahead.state).toBe('fulfilled')
        round.retire()
        await env.finish()
      }
    )

    it('writes the queued SyncGroup while the round is still current', async () => {
      const env = await rig({ coordinator: 1, maxInFlightRequests: 1 })
      const protocol = await leader(
        env,
        custom(async () => []),
        [['a', sub('A')]]
      )
      const heartbeat = env.schedule.hold({ api: 'Heartbeat' })
      const round = roundGroup(env.cluster).beginRebalanceRound()
      const sync = syncing(protocol, round, () => {
        protocol.coordinator
          .heartbeat({ groupId: 'group', groupGenerationId: 1, memberId: 'a' })
          .catch(() => {})
      })
      await heartbeat.arrived()
      await drain()
      expect(env.frames('SyncGroup')).toEqual([])

      heartbeat.release()
      await settled(sync)

      expect(sync.state).toBe('fulfilled')
      expect(env.frames('SyncGroup')).toHaveLength(1)
      round.retire()
      await env.finish()
    })

    it.each([
      ['reaches the deadline', async () => elapse(1000)],
      [
        'is released after the clock passed it, before any timer ran',
        async () => elapse(1001, { timers: false }),
      ],
    ])('does not give a held assigner fresh time when it %s', async (_, passTime) => {
      const env = await rig()
      const hold = new Hold()
      const assign = jest.fn(async () => hold.pass().then(() => []))
      const protocol = await leader(env, custom(assign), [['a', sub('A')]])
      const round = roundGroup(env.cluster).beginRebalanceRound()
      const beforeSync = jest.fn()
      const sync = syncing(protocol, round, beforeSync)
      await hold.arrived()

      await passTime()
      if (sync.state === 'pending') hold.release()
      await drain()
      hold.release()
      await drain()

      expect(sync.value).toMatchObject(EXPIRED)
      expect(beforeSync).not.toHaveBeenCalled()
      expect(env.frames('SyncGroup')).toEqual([])
      round.retire()
      await env.finish()
    })
  })

  describe.each(['stop', 'replacement'])('a %s while old leader work is held', (how) => {
    it.each(['own refresh', 'snapshot response', 'assigner'])(
      'sends nothing and changes nothing when the %s completes late',
      async (stage) => {
        const env = await rig()
        env.table.set('A', topic([{ id: 0 }, { id: 1 }])).set('B', topic([{ id: 0 }]))
        const hold = new Hold()
        let calls = 0
        const assign = jest.fn(async () =>
          ++calls === 1 && stage === 'assigner' ? hold.pass().then(() => []) : []
        )
        const protocol = await leader(env, custom(assign), [
          ['a', sub('A')],
          ['b', sub('B')],
        ])
        const group = roundGroup(env.cluster)
        const old = group.beginRebalanceRound()
        const gate =
          stage === 'assigner'
            ? hold
            : env.schedule.hold({
                api: 'Metadata',
                match: ({ fields }) => asked(fields) === (stage === 'own refresh' ? 'A' : 'B'),
              })
        const sync = syncing(protocol, old)
        await gate.arrived()

        let next
        if (how === 'stop') stop(group)
        else next = group.beginRebalanceRound()
        await drain()
        gate.release()
        await drain()

        expect(sync.state).toBe('rejected')
        expect(sync.value.type).toBe(
          how === 'stop' ? 'CONSUMER_STOPPING' : 'JOIN_DEADLINE_EXCEEDED'
        )
        expect(env.frames('SyncGroup')).toEqual([])
        expect(env.snapshots()).toHaveLength(stage === 'own refresh' ? 0 : 1)
        expect(env.cluster.findTopicPartitionMetadata('A')).toHaveLength(
          stage === 'own refresh' ? 1 : 2
        )
        expect(env.cluster.findTopicPartitionMetadata('B')).toEqual([])
        expect(assign).toHaveBeenCalledTimes(stage === 'assigner' ? 1 : 0)
        if (next) {
          const again = syncing(protocol, next)
          await settled(again)
          expect(again.state).toBe('fulfilled')
          expect(env.frames('SyncGroup')).toHaveLength(1)
          next.retire()
        }
        old.retire()
        await env.finish()
      }
    )
  })

  describe('ordinary metadata', () => {
    it('throws the last broker error after trying every broker, and keeps the cache', async () => {
      const env = await rig()
      const cache = env.cluster.brokerPool.metadataCache
      env.schedule.once({ api: 'Metadata', node: 0 }, new Error('reset by peer'))
      env.schedule.once({ api: 'Metadata', node: 1 }, new Error('reset by peer'))
      const before = env.wire.requests().length

      const result = watch(env.cluster.metadata({ topics: ['A'] }))
      await driven(result)

      expect(result.state).toBe('rejected')
      expect(result.value).toMatchObject({ name: 'KafkaJSConnectionError' })
      const written = env.wire
        .requests()
        .slice(before)
        .filter((r) => r.api === 'Metadata')
      expect(new Set(written.map((r) => r.node))).toEqual(new Set([0, 1]))
      expect(env.cluster.brokerPool.metadataCache).toBe(cache)
      await env.finish()
    })

    it('answers from the second broker when only the first one fails', async () => {
      const env = await rig()
      const failed = env.schedule.hold({
        api: 'Metadata',
        match: ({ fields }) => asked(fields) === 'A',
      })
      const before = env.wire.requests().length

      const result = watch(env.cluster.metadata({ topics: ['A'] }))
      await failed.arrived()
      failed.fail(new Error('reset by peer'))
      await driven(result)

      expect(result.state).toBe('fulfilled')
      const frames = env.wire
        .requests()
        .slice(before)
        .filter((r) => r.api === 'Metadata')
      expect(frames.map((r) => asked(r.fields))).toEqual(['A', 'A'])
      expect(new Set(frames.map((r) => r.node)).size).toBe(2)
      await env.finish()
    })
  })

  describe('a topic created while the first round is held', () => {
    it('is assigned only in the next normal round, to its subscriber', async () => {
      const env = await rig()
      const subscriber = await rig({ peer: env })
      env.table.set('B', topic([{ id: 0 }]))
      const protocol = await leader(env, RoundRobinAssigner({ cluster: env.cluster }), [
        ['a', sub('A')],
        ['b', sub('C')],
      ])
      const group = roundGroup(env.cluster)
      const first = group.beginRebalanceRound()
      const syncGroup = env.schedule.hold({ api: 'SyncGroup' })
      const targets = [...env.cluster.targetTopics]

      const firstSync = syncing(protocol, first)
      await syncGroup.arrived()
      env.table.set('C', topic([{ id: 0 }, { id: 1 }]))
      env.wire.mark('topic created')
      syncGroup.release()
      await settled(firstSync)
      expect(firstSync.state).toBe('fulfilled')
      env.wire.mark('first round complete')

      const seen = await subscriber.cluster.metadata({ topics: ['C'] })
      first.retire()
      const second = group.beginRebalanceRound()
      const secondSync = syncing(protocol, second)
      await settled(secondSync)
      expect(secondSync.state).toBe('fulfilled')
      env.wire.mark('second round complete')

      const [snapshot, detection, laterSnapshot] = env.frames('Metadata', 'C')
      const [firstSyncFrame, laterSyncFrame] = env.frames('SyncGroup')
      const markAt = (label) => env.wire.timeline().find((event) => event.label === label).order
      const order = [
        snapshot.answeredOrder,
        markAt('topic created'),
        firstSyncFrame.answeredOrder,
        markAt('first round complete'),
        detection.answeredOrder,
        laterSnapshot.answeredOrder,
        laterSyncFrame.answeredOrder,
        markAt('second round complete'),
      ]
      expect(order.every((position, index) => index === 0 || position > order[index - 1])).toBe(
        true
      )
      expect(seen.topicMetadata[0].partitionMetadata).toHaveLength(2)
      expect(assignments(firstSyncFrame)).toEqual({ a: { A: [0] } })
      expect(assignments(laterSyncFrame).b).toEqual({ C: [0, 1] })
      expect([...env.cluster.targetTopics]).toEqual(targets)
      second.retire()
      await subscriber.cluster.disconnect({ drainTimeout: 0 })
      await env.finish()
    })
  })
  describe('the leader union watch', () => {
    const STALE = { name: 'KafkaJSStaleTopicMetadataAssignment', topic: 'C' }
    /** @param {object} env @returns {object[]} the Metadata requests that asked for C alone */
    const readsOfC = (env) => env.frames('Metadata', 'C')

    it.each([
      ['a topic that did not exist at the assignment', null, [0, 1], [0, 1]],
      ['a topic that gained partitions', [0], [0, 1, 2], [1, 2]],
    ])('rejoins when %s and only a follower subscribes to it', async (_, before, after, gained) => {
      const env = await rig({ metadataMaxAge: 5000 })
      if (before) env.table.set('C', topic(before.map((id) => ({ id }))))
      const targets = [...env.cluster.targetTopics]
      const { group } = await assigned(env, [
        ['a', sub('A')],
        ['b', sub('C')],
      ])
      env.table.set('C', topic(after.map((id) => ({ id }))))
      const asks = readsOfC(env).length

      await elapse(4000)
      await prepare(group)
      expect(readsOfC(env)).toHaveLength(asks)

      await elapse(1001)
      await expect(prepare(group)).rejects.toMatchObject({
        ...STALE,
        unknownPartitions: gained,
      })

      expect(readsOfC(env)).toHaveLength(asks + 1)
      expect(readsOfC(env)[asks].fields.allowAutoTopicCreation).toBe(false)
      expect([...env.cluster.targetTopics]).toEqual(targets)
      expect(env.cluster.findTopicPartitionMetadata('C')).toEqual([])
      await elapse(10000)
      await prepare(group)
      expect(readsOfC(env)).toHaveLength(asks + 1)
      group.onStopRequested()
      await env.finish()
    })

    it('keeps watching after a failed read and reads again after the next interval', async () => {
      const env = await rig({ metadataMaxAge: 5000 })
      const { group } = await assigned(env, [
        ['a', sub('A')],
        ['b', sub('C')],
      ])
      const asks = readsOfC(env).length
      env.schedule.once(
        { api: 'Metadata', match: ({ fields }) => asked(fields) === 'C' },
        new Error('reset by peer')
      )
      await elapse(5001)
      const failing = watch(prepare(group))
      await driven(failing)
      expect(failing.state).toBe('fulfilled')
      expect(readsOfC(env)).toHaveLength(asks + 1)

      env.table.set('C', topic([{ id: 0 }]))
      await elapse(4000)
      await prepare(group)
      expect(readsOfC(env)).toHaveLength(asks + 1)
      await elapse(1001)
      await expect(prepare(group)).rejects.toMatchObject(STALE)
      group.onStopRequested()
      await env.finish()
    })

    it('sends no Metadata for the union when every member shares the leader subscription', async () => {
      const env = await rig({ metadataMaxAge: 5000 })
      const { group } = await assigned(env, [
        ['a', sub('A')],
        ['b', sub('A')],
      ])
      const before = env.wire.requests().length

      await elapse(60000)
      await prepare(group)

      const written = env.wire
        .requests()
        .slice(before)
        .filter((r) => r.api === 'Metadata')
      expect(written.map((r) => asked(r.fields))).toEqual(['A'])
      group.onStopRequested()
      await env.finish()
    })

    it('stops watching when leadership is lost', async () => {
      const env = await rig({ metadataMaxAge: 5000 })
      const { group, protocol } = await assigned(env, [
        ['a', sub('A')],
        ['b', sub('C')],
      ])
      const asks = readsOfC(env).length
      env.table.set('C', topic([{ id: 0 }]))
      protocol.leaderId = 'b'

      await elapse(60000)
      await prepare(group)
      await elapse(60000)
      await prepare(group)

      expect(readsOfC(env)).toHaveLength(asks)
      group.onStopRequested()
      await env.finish()
    })

    it('stops watching when the next round starts', async () => {
      const env = await rig({ metadataMaxAge: 5000 })
      const { group } = await assigned(env, [
        ['a', sub('A')],
        ['b', sub('C')],
      ])
      const asks = readsOfC(env).length
      env.table.set('C', topic([{ id: 0 }]))

      const next = group.beginRebalanceRound()
      await elapse(60000)
      await prepare(group)

      expect(readsOfC(env)).toHaveLength(asks)
      next.retire()
      group.onStopRequested()
      await env.finish()
    })

    it('drops a read still in flight when the next round starts', async () => {
      const env = await rig({ metadataMaxAge: 5000 })
      const { group } = await assigned(env, [
        ['a', sub('A')],
        ['b', sub('C')],
      ])
      const held = env.schedule.hold({
        api: 'Metadata',
        match: ({ fields }) => asked(fields) === 'C',
      })
      env.table.set('C', topic([{ id: 0 }]))
      await elapse(5001)
      const preparing = watch(prepare(group))
      const reached = watch(held.arrived())
      await drain()
      expect(reached.state).toBe('fulfilled')

      const next = group.beginRebalanceRound()
      held.release()
      await drain()

      expect(preparing.state).toBe('fulfilled')
      next.retire()
      group.onStopRequested()
      await env.finish()
    })

    it('stops watching when the run stops, and drops a read still in flight', async () => {
      const env = await rig({ metadataMaxAge: 5000 })
      const { group } = await assigned(env, [
        ['a', sub('A')],
        ['b', sub('C')],
      ])
      const asks = readsOfC(env).length
      const held = env.schedule.hold({
        api: 'Metadata',
        match: ({ fields }) => asked(fields) === 'C',
      })
      env.table.set('C', topic([{ id: 0 }]))
      await elapse(5001)
      const preparing = watch(prepare(group))
      const reached = watch(held.arrived())
      await drain()
      expect(reached.state).toBe('fulfilled')

      group.onStopRequested()
      await drain()
      expect(preparing.state).toBe('rejected')
      expect(preparing.value).toMatchObject({ type: 'CONSUMER_STOPPING' })
      held.release()
      await drain()

      group.startIdleWait()
      await elapse(60000)
      await prepare(group)
      expect(readsOfC(env)).toHaveLength(asks + 1)
      group.onStopRequested()
      await env.finish()
    })

    describe('retirement of a read before it is transmitted', () => {
      it.each(['round', 'leadership'])(
        'writes no old read after %s retirement and settles without the held work',
        async (how) => {
          const env = await rig({ metadataMaxAge: 5000 })
          const { group, protocol } = await assigned(env, [
            ['a', sub('A')],
            ['b', sub('C')],
          ])
          await elapse(5001)
          await env.cluster.refreshMetadata()
          const asks = readsOfC(env).length
          expect(asks).toBe(1)
          const held = env.hold('encoding')
          const preparing = watch(prepare(group))
          const reached = watch(held.arrived())
          await drain()
          expect(reached.state).toBe('fulfilled')

          let next
          if (how === 'round') next = group.beginRebalanceRound()
          else protocol.leaderId = 'b'
          await drain()
          // Only a round start is an event the local wait can follow; a bare leadership change is
          // seen at the next guard check.
          if (how === 'round') expect(preparing.state).toBe('fulfilled')
          held.release()
          await drain()

          expect(readsOfC(env)).toHaveLength(asks)
          expect(preparing.state).toBe('fulfilled')
          next?.retire()
          group.onStopRequested()
          await env.finish()
        }
      )

      it('publishes no drift from an answer that arrives after leadership was lost', async () => {
        const env = await rig({ metadataMaxAge: 5000 })
        const { group, protocol } = await assigned(env, [
          ['a', sub('A')],
          ['b', sub('C')],
        ])
        const held = env.schedule.hold({
          api: 'Metadata',
          match: ({ fields }) => asked(fields) === 'C',
        })
        env.table.set('C', topic([{ id: 0 }]))
        await elapse(5001)
        const preparing = watch(prepare(group))
        const reached = watch(held.arrived())
        await drain()
        expect(reached.state).toBe('fulfilled')

        protocol.leaderId = 'b'
        held.release()
        await drain()

        expect(preparing.state).toBe('fulfilled')
        expect(group.leaderUnionWatch.drift).toBeNull()
        group.onStopRequested()
        await env.finish()
      })
    })
  })
})
