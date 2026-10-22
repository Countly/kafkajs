const { Kafka, PartitionAssigners } = require('../../../index')
const { KafkaJSConnectionError } = require('../../errors')
const createConsumer = require('../index')
const { createCluster, newLogger } = require('testHelpers')
const {
  createScriptedCluster,
  groupScript,
  consumerProtocolScript,
} = require('../../../testHelpers/scriptedCluster')
const { prepareRecordBatches, TOPIC, TOPIC_ID } = require('./fixtures/fetchHarness')
const { RecordBatch } = require('../../protocol/recordBatch/v0')
const RecordEncoder = require('../../protocol/recordBatch/record/v0')
const { waitForCondition } = require('./fixtures/liveScenario')
const sleep = require('../../utils/sleep')

const LEADER = 0
const COORDINATOR = 1
const NODES = [
  { id: LEADER, host: 'node0', port: 9092 },
  { id: COORDINATOR, host: 'node1', port: 9092 },
]
const NOT_COORDINATOR = 16
const COORDINATOR_LOAD_IN_PROGRESS = 14
const OUTAGE_MS = 600
const SESSION_TIMEOUT = 4000
// One retry and 10 to 20 ms between attempts: without riding the outage out, the consumer would
// spend this budget in the first tens of milliseconds of the outage and crash.
const RETRY = { retries: 1, initialRetryTime: 10, maxRetryTime: 20 }

/**
 * @param {Error} error a crash's error
 * @returns {boolean} whether it, or an error in its chain of causes, is a lost connection
 */
const lostConnection = (error) => {
  for (let link = error; link; link = link.cause) {
    if (/^KafkaJS(Connection|RequestTimeout)/.test(link.name)) return true
  }
  return false
}

/**
 * Makes every Fetch return one new record at the offset it asks for, so records keep flowing and
 * every batch commits.
 * @param {ReturnType<typeof createScriptedCluster>} cluster
 * @returns {void}
 */
const endlessRecords = (cluster) => {
  cluster.answer('Fetch', async ({ topics }) => {
    await sleep(10)
    const partitions = []
    for (const { partition, fetchOffset } of topics[0].partitions) {
      const batch = await RecordBatch({
        firstOffset: fetchOffset,
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        records: [RecordEncoder({ key: Buffer.from('key'), value: Buffer.from('value') })],
      })
      partitions.push({ partition, errorCode: 0, records: Buffer.from(batch.buffer) })
    }
    return { partitions }
  })
}

/** Polls until `predicate` holds, failing with `what` when it never does. */
const until = async (predicate, what, attempts = 1000) => {
  if (!(await waitForCondition(predicate, { attempts, delay: 5 }))) {
    throw new Error(`${what} did not happen`)
  }
}

/**
 * Lets a test take a node out the way a failure of `kind` does, until it is restored: `refused`
 * and `reset` reset its open connections and fail every new dial with that code, `closed` ends its
 * open connections and refuses new dials, and `timeout` keeps its connections open but answers
 * nothing, so every request on it times out.
 * @param {ReturnType<typeof createScriptedCluster>} cluster
 * @returns {{ take: (id: number, kind: string) => void, restore: (id: number) => void }}
 */
const outages = (cluster) => {
  const broken = new Map()
  const dial = cluster.socketFactory
  const nodeAt = ({ host, port }) =>
    cluster.nodes.find((node) => node.host === host && node.port === port)
  const failedDial = (options, code) => {
    const socket = cluster.fake.factory({ ...options, onConnect: () => {} })
    setImmediate(() => socket.destroy(Object.assign(new Error(`connect ${code}`), { code })))
    return socket
  }

  cluster.socketFactory = (options) => {
    const node = nodeAt(options)
    if (broken.get(node?.id) === 'reset') return failedDial(options, 'ECONNRESET')
    const socket = dial(options)
    const write = socket.write
    socket.write = (...args) => (broken.get(node?.id) === 'timeout' ? true : write(...args))
    return socket
  }

  return {
    take(id, kind) {
      broken.set(id, kind)
      if (kind === 'refused' || kind === 'closed') cluster.down(id)
      const { host, port } = cluster.nodes.find((node) => node.id === id)
      for (const socket of cluster.sockets()) {
        if (socket.destroyed || socket.endpoint.host !== host || socket.endpoint.port !== port) {
          continue
        }
        if (kind === 'closed') {
          socket.emit('end')
          socket.destroy()
        } else if (kind !== 'timeout') {
          socket.destroy(Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' }))
        }
      }
    },
    restore(id) {
      broken.delete(id)
      cluster.up(id)
    },
  }
}

/**
 * A lost connection, or a coordinator that moved, is ridden out while it lasts less than the session
 * timeout: the consumer neither crashes nor rejoins, and carries on once the broker is back. A
 * longer outage is handled as before. The cases run the real consumer over the scripted cluster:
 * the partition is led by one node and the group is coordinated by the other.
 */
describe('Consumer > broker outages', () => {
  let consumer

  beforeAll(prepareRecordBatches)

  afterEach(async () => {
    await consumer?.disconnect()
    consumer = undefined
  })

  /**
   * @param {object} [options] consumer options over the scenario's
   * @param {object} [kafkaOptions]
   * @param {{ batches?: number }} [script] how many Fetches carry the record batch
   * @returns {{ cluster: object, breaker: ReturnType<typeof outages>, events: object }}
   */
  const scenario = (options = {}, kafkaOptions = {}, { batches = 1 } = {}) => {
    const cluster = groupScript(createScriptedCluster({ nodes: NODES }), {
      coordinator: COORDINATOR,
      batches,
    })
    const breaker = outages(cluster)
    const kafka = new Kafka(cluster.kafkaOptions({ retry: RETRY, ...kafkaOptions }))
    consumer = kafka.consumer({
      groupId: 'outage-group',
      sessionTimeout: SESSION_TIMEOUT,
      heartbeatInterval: 100,
      maxWaitTimeInMs: 20,
      retry: RETRY,
      ...options,
    })
    const events = { crashes: [], joins: [], rebalancing: [], records: [] }
    consumer.on(consumer.events.CRASH, ({ payload }) => events.crashes.push(payload))
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => events.joins.push(payload))
    consumer.on(consumer.events.REBALANCING, ({ payload }) => events.rebalancing.push(payload))
    return { cluster, breaker, events }
  }

  const start = async (events, run = {}) => {
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    return consumer.run({
      eachMessage: async ({ message }) => events.records.push(message.offset),
      ...run,
    })
  }

  const wire = (cluster, api, from = 0) =>
    cluster
      .requests()
      .slice(from)
      .filter((request) => request.api === api)

  const answeredFetchOnLeader = (cluster, from) =>
    wire(cluster, 'Fetch', from).some(
      ({ node, answeredOrder }) => node === LEADER && answeredOrder !== undefined
    )

  it.each([['refused'], ['reset'], ['closed'], ['timeout']])(
    'a partition leader %s for longer than the retry budget costs no crash and no rejoin',
    async (kind) => {
      const { cluster, breaker, events } = scenario({}, { requestTimeout: 300 })
      await start(events)
      await until(() => events.records.length === 1, 'the first record')

      breaker.take(LEADER, kind)
      await sleep(OUTAGE_MS)
      const restoredAt = cluster.requests().length
      breaker.restore(LEADER)
      await until(
        () => answeredFetchOnLeader(cluster, restoredAt),
        'a Fetch answered by the leader after it is back'
      )

      expect(events.crashes).toEqual([])
      expect(wire(cluster, 'JoinGroup')).toHaveLength(1)
      expect(events.rebalancing).toEqual([])
    }
  )

  it.each([['refused'], ['reset']])(
    'a coordinator %s during a cooperative rejoin costs no crash: the round joins once it is back',
    async (kind) => {
      const { cluster, breaker, events } = scenario({
        partitionAssigners: [PartitionAssigners.cooperativeSticky],
      })
      await start(events)
      await until(() => events.joins.length === 1, 'the first join')

      const join = cluster.schedule.hold({ api: 'JoinGroup' })
      cluster.schedule.once({ api: 'Heartbeat' }, { errorCode: 27 })
      await join.arrived()
      breaker.take(COORDINATOR, kind)
      await sleep(OUTAGE_MS)
      breaker.restore(COORDINATOR)
      join.release()
      await until(() => events.joins.length === 2, 'the second join')

      expect(events.crashes).toEqual([])
      expect(events.rebalancing.map(({ reason }) => reason)).toEqual(['rebalance-in-progress'])
    }
  )

  it.each([['refused'], ['reset']])(
    'a coordinator %s at the first join costs no crash: the consumer joins once it is back',
    async (kind) => {
      const { breaker, events } = scenario()
      breaker.take(COORDINATOR, kind)
      const running = start(events)
      await sleep(OUTAGE_MS)
      breaker.restore(COORDINATOR)
      await running
      await until(() => events.records.length === 1, 'the first record')

      expect(events.crashes).toEqual([])
      expect(events.joins).toHaveLength(1)
    }
  )

  it('a coordinator that moves while the old one stays up is looked up again, without a rejoin', async () => {
    const { cluster, events } = scenario()
    await start(events)
    await until(() => events.records.length === 1, 'the first record')

    const movedAt = cluster.requests().length
    const [newCoordinator] = NODES
    cluster.answer('FindCoordinator', ({ coordinatorKeys }) => ({
      coordinators: coordinatorKeys.map((key) => ({
        key,
        nodeId: newCoordinator.id,
        host: newCoordinator.host,
        port: newCoordinator.port,
        errorCode: 0,
        errorMessage: null,
      })),
    }))
    cluster.answer('Heartbeat', (_fields, { node }) =>
      node === COORDINATOR ? { errorCode: NOT_COORDINATOR } : {}
    )
    await until(
      () => wire(cluster, 'Heartbeat', movedAt).some(({ node }) => node === newCoordinator.id),
      'a heartbeat to the new coordinator'
    )

    expect(wire(cluster, 'Heartbeat', movedAt).some(({ node }) => node === COORDINATOR)).toBe(true)
    expect(wire(cluster, 'JoinGroup')).toHaveLength(1)
    expect(events.rebalancing).toEqual([])
    expect(events.crashes).toEqual([])
  })

  it.each([
    ['an eager', {}],
    ['a cooperative', { partitionAssigners: [PartitionAssigners.cooperativeSticky] }],
  ])(
    '%s member retries the commits a coordinator still loading answers COORDINATOR_LOAD_IN_PROGRESS, without a rejoin',
    async (_mode, assigners) => {
      const { cluster, events } = scenario(assigners)
      endlessRecords(cluster)
      let loading = false
      cluster.answer('OffsetCommit', ({ topics }) => ({
        topics: topics.map((entry) => ({
          topic: entry.topic,
          partitions: entry.partitions.map(({ partition }) => ({
            partition,
            errorCode: loading ? COORDINATOR_LOAD_IN_PROGRESS : 0,
          })),
        })),
      }))
      await start(events, { autoCommitInterval: 20 })
      await until(() => events.records.length > 0, 'the first record')

      const loadingAt = cluster.requests().length
      loading = true
      await until(
        () => wire(cluster, 'OffsetCommit', loadingAt).length > 1,
        'two commits sent while the coordinator loads'
      )
      await sleep(OUTAGE_MS)
      loading = false
      const loadedAt = cluster.requests().length
      await until(
        () =>
          wire(cluster, 'OffsetCommit', loadedAt).some(
            ({ answeredOrder }) => answeredOrder !== undefined
          ),
        'a commit answered once the coordinator has loaded'
      )

      expect(events.crashes).toEqual([])
      expect(events.rebalancing).toEqual([])
      expect(wire(cluster, 'JoinGroup')).toHaveLength(1)
    }
  )

  describe('a commit helper that eachBatch calls while the coordinator is still loading', () => {
    const SESSION = 1000

    /**
     * Runs a classic member whose handler commits through `commitOffsetsIfNecessary`, over a
     * coordinator that answers its first `loadingAnswers` commits COORDINATOR_LOAD_IN_PROGRESS.
     * @param {number} loadingAnswers
     * @returns {Promise<{ events: object, cluster: object, helper: { loading: number, completed: number, firstLoadingAt?: number, crashedAt?: number } }>}
     */
    const startHelper = async (loadingAnswers) => {
      const { cluster, events } = scenario({
        sessionTimeout: SESSION,
        retry: { ...RETRY, restartOnFailure: async () => false },
      })
      endlessRecords(cluster)
      const helper = { loading: 0, completed: 0 }
      consumer.on(consumer.events.CRASH, () => {
        helper.crashedAt ??= performance.now()
      })
      cluster.answer('OffsetCommit', ({ topics }) => {
        const loading = helper.loading < loadingAnswers
        if (loading) {
          helper.loading++
          helper.firstLoadingAt ??= performance.now()
        }
        return {
          topics: topics.map((entry) => ({
            topic: entry.topic,
            partitions: entry.partitions.map(({ partition }) => ({
              partition,
              errorCode: loading ? COORDINATOR_LOAD_IN_PROGRESS : 0,
            })),
          })),
        }
      })
      await start(events, {
        autoCommit: false,
        autoCommitThreshold: 1,
        eachMessage: undefined,
        eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary }) => {
          resolveOffset(batch.lastOffset())
          await commitOffsetsIfNecessary()
          helper.completed++
        },
      })
      return { events, cluster, helper }
    }

    it('retries a loading answer that is short, with no crash and no rejoin', async () => {
      const { events, cluster, helper } = await startHelper(6)
      await until(
        () => helper.completed > 0 || events.crashes.length > 0,
        'the helper to commit or the consumer to crash'
      )

      expect(events.crashes).toEqual([])
      expect(helper.loading).toBe(6)
      expect(events.rebalancing).toEqual([])
      expect(wire(cluster, 'JoinGroup')).toHaveLength(1)
    })

    it('crashes with the loading error once the answer outlasts the session timeout, with no rejoin', async () => {
      const { events, cluster, helper } = await startHelper(Infinity)
      await until(() => events.crashes.length > 0, 'a crash', 2000)

      expect(events.crashes).toHaveLength(1)
      const [{ error, restart }] = events.crashes
      expect(error.name).toBe('KafkaJSNumberOfRetriesExceeded')
      expect(error.cause.code).toBe(COORDINATOR_LOAD_IN_PROGRESS)
      expect(restart).toBe(false)
      expect(helper.crashedAt - helper.firstLoadingAt).toBeGreaterThanOrEqual(SESSION)
      expect(events.rebalancing).toEqual([])
      expect(wire(cluster, 'JoinGroup')).toHaveLength(1)
    })
  })

  it('a disconnect while the fetch loop rides out a leader outage reports no crash', async () => {
    const { breaker, events } = scenario()
    await start(events)
    await until(() => events.records.length === 1, 'the first record')
    breaker.take(LEADER, 'refused')
    await sleep(OUTAGE_MS / 3)

    await consumer.disconnect()

    expect(events.crashes).toEqual([])
  })

  it('a handler that throws a lost connection of its own counts against the retries, as before', async () => {
    const { events } = scenario({ retry: { ...RETRY, retries: 0 } }, {}, { batches: 2 })
    const thrown = new KafkaJSConnectionError('the handler lost its own connection')
    await start(events, {
      eachMessage: async () => {
        throw thrown
      },
    })
    await until(() => events.crashes.length > 0, 'a crash')

    const [{ error, restart }] = events.crashes
    expect(error.name).toBe('KafkaJSNumberOfRetriesExceeded')
    expect(error.cause).toBe(thrown)
    expect(restart).toBe(true)
  })

  it('a handler that throws a value that is not an error counts against the retries, as before', async () => {
    const { events } = scenario({ retry: { ...RETRY, retries: 0 } }, {}, { batches: 2 })
    const reason = 'the handler gave up'
    await start(events, {
      eachMessage: async () => {
        throw reason
      },
    })
    await until(() => events.crashes.length > 0, 'a crash')

    const [{ error, restart }] = events.crashes
    expect(error.name).toBe('KafkaJSNumberOfRetriesExceeded')
    expect(error.cause).toBe(reason)
    expect(restart).toBe(true)
  })

  it("a handler that rethrows its own commit's COORDINATOR_NOT_AVAILABLE counts against the retries", async () => {
    const { cluster, events } = scenario({ retry: { ...RETRY, retries: 0 } }, {}, { batches: 2 })
    let moved = false
    cluster.answer('OffsetCommit', ({ topics }) => ({
      topics: topics.map((entry) => ({
        topic: entry.topic,
        partitions: entry.partitions.map(({ partition }) => ({
          partition,
          errorCode: moved ? 15 : 0,
        })),
      })),
    }))
    let calls = 0
    let callsAtCrash
    consumer.on(consumer.events.CRASH, () => {
      callsAtCrash ??= calls
    })
    await start(events, {
      eachMessage: undefined,
      eachBatch: async ({ batch, commitOffsetsIfNecessary }) => {
        calls++
        moved = true
        await commitOffsetsIfNecessary({
          topics: [
            { topic: batch.topic, partitions: [{ partition: batch.partition, offset: '1' }] },
          ],
        })
      },
    })
    await until(() => events.crashes.length > 0, 'a crash')

    const [{ error, restart }] = events.crashes
    expect(error.name).toBe('KafkaJSNumberOfRetriesExceeded')
    expect(error.cause.type).toBe('GROUP_COORDINATOR_NOT_AVAILABLE')
    expect(restart).toBe(true)
    expect(callsAtCrash).toBe(1)
  })

  it('a handler that keeps throwing while another leader cannot be reached counts against the retries, as before', async () => {
    const nodes = [...NODES, { id: 2, host: 'node2', port: 9092 }]
    const cluster = groupScript(createScriptedCluster({ nodes }), {
      coordinator: COORDINATOR,
      partitions: 2,
      committed: { 0: '0', 1: '0' },
    })
    endlessRecords(cluster)
    cluster.answer('Metadata', () => ({
      brokers: nodes.map(({ id, host, port }) => ({ nodeId: id, host, port, rack: null })),
      controllerId: LEADER,
      topics: [
        {
          errorCode: 0,
          name: TOPIC,
          topicId: TOPIC_ID,
          isInternal: false,
          topicAuthorizedOperations: -2147483648,
          partitions: [LEADER, 2].map((leaderId, partitionIndex) => ({
            errorCode: 0,
            partitionIndex,
            leaderId,
            leaderEpoch: 5,
            replicas: [leaderId],
            isr: [leaderId],
            offlineReplicas: [],
          })),
        },
      ],
    }))
    const breaker = outages(cluster)
    consumer = new Kafka(cluster.kafkaOptions({ retry: RETRY })).consumer({
      groupId: 'outage-group',
      sessionTimeout: SESSION_TIMEOUT,
      heartbeatInterval: 100,
      maxWaitTimeInMs: 20,
      retry: RETRY,
    })
    const events = { crashes: [], records: [] }
    consumer.on(consumer.events.CRASH, ({ payload }) => events.crashes.push(payload))
    breaker.take(2, 'refused')
    const poison = new Error('the handler cannot process this record')
    await start(events, {
      eachMessage: async () => {
        throw poison
      },
    })
    await until(() => events.crashes.length > 0, 'a crash', 2000)

    const [{ error, restart }] = events.crashes
    expect(error.name).toBe('KafkaJSNumberOfRetriesExceeded')
    expect(error.cause).toBe(poison)
    expect(restart).toBe(true)
  })

  it('a stop while the first join rides out an outage ends the join without a crash', async () => {
    const { breaker, events } = scenario()
    breaker.take(COORDINATOR, 'refused')
    const running = start(events)
    await sleep(OUTAGE_MS / 3)

    await consumer.disconnect()
    await running

    expect(events.crashes).toEqual([])
    expect(events.joins).toEqual([])
  })

  it('a stop during a join attempt that outlasts the session timeout ends the join without a crash', async () => {
    const { cluster, events } = scenario({
      sessionTimeout: 1000,
      retry: { ...RETRY, restartOnFailure: async () => false },
    })
    cluster.schedule.once({ api: 'JoinGroup' }, { errorCode: 15 })
    const held = cluster.schedule.hold({ api: 'JoinGroup' })
    const running = start(events)
    await held.arrived()

    const disconnecting = consumer.disconnect()
    await sleep(1200)
    held.release({ errorCode: 15 })
    await disconnecting
    await running

    expect(events.crashes).toEqual([])
    expect(events.joins).toEqual([])
    expect(cluster.sockets().filter((socket) => !socket.destroyed)).toEqual([])
  })

  describe('an outage that outlasts the session timeout', () => {
    const SHORT = { sessionTimeout: 1000, heartbeatInterval: 100 }

    it('is not two shorter outages with a leader answering between them', async () => {
      const { cluster, breaker, events } = scenario(SHORT)
      await start(events)
      await until(() => events.records.length === 1, 'the first record')

      for (const outage of [1, 2]) {
        breaker.take(LEADER, 'refused')
        await sleep(OUTAGE_MS)
        const restoredAt = cluster.requests().length
        breaker.restore(LEADER)
        await until(
          () => answeredFetchOnLeader(cluster, restoredAt),
          `a Fetch answered by the leader after outage ${outage}`
        )
      }

      expect(events.crashes).toEqual([])
    })

    it('is not two shorter coordinator outages with a heartbeat and a commit answered between them', async () => {
      const { cluster, breaker, events } = scenario({ ...SHORT, autoCommitInterval: 20 })
      endlessRecords(cluster)
      await start(events)
      await until(() => events.records.length > 0, 'the first record')
      const answeredOnCoordinator = (api, from) =>
        wire(cluster, api, from).some(
          ({ node, answeredOrder }) => node === COORDINATOR && answeredOrder !== undefined
        )

      for (const outage of [1, 2]) {
        breaker.take(COORDINATOR, 'refused')
        await sleep(OUTAGE_MS)
        const restoredAt = cluster.requests().length
        breaker.restore(COORDINATOR)
        await until(
          () =>
            answeredOnCoordinator('Heartbeat', restoredAt) &&
            answeredOnCoordinator('OffsetCommit', restoredAt),
          `a heartbeat and a commit answered after coordinator outage ${outage}`
        )
      }

      expect(events.crashes).toEqual([])
    })

    it('is not two shorter commit outages with a commit answered between them', async () => {
      const { cluster, events } = scenario({ ...SHORT, autoCommitInterval: 20 })
      endlessRecords(cluster)
      let moved = false
      cluster.answer('OffsetCommit', ({ topics }) => ({
        topics: topics.map((entry) => ({
          topic: entry.topic,
          partitions: entry.partitions.map(({ partition }) => ({
            partition,
            errorCode: moved ? 15 : 0,
          })),
        })),
      }))
      await start(events)
      await until(() => events.records.length > 0, 'the first record')

      for (const outage of [1, 2]) {
        const movedAt = cluster.requests().length
        moved = true
        await sleep(OUTAGE_MS)
        moved = false
        const restoredAt = cluster.requests().length
        expect(wire(cluster, 'OffsetCommit', movedAt).length).toBeGreaterThan(0)
        await until(
          () =>
            wire(cluster, 'OffsetCommit', restoredAt).some(({ answeredOrder }) => answeredOrder),
          `a commit answered after commit outage ${outage}`
        )
      }

      expect(events.crashes).toEqual([])
      expect(events.rebalancing).toEqual([])
    })

    it('crashes the fetch loop when the automatic commits keep failing, with the default auto-commit settings', async () => {
      const { cluster, events } = scenario(SHORT)
      endlessRecords(cluster)
      let moved = false
      cluster.answer('OffsetCommit', ({ topics }) => ({
        topics: topics.map((entry) => ({
          topic: entry.topic,
          partitions: entry.partitions.map(({ partition }) => ({
            partition,
            errorCode: moved ? 15 : 0,
          })),
        })),
      }))
      await start(events)
      await until(() => events.records.length > 0, 'the first record')
      moved = true
      await until(() => events.crashes.length > 0, 'a crash', 2000)

      const [{ error, restart }] = events.crashes
      expect(error.name).toBe('KafkaJSNumberOfRetriesExceeded')
      expect(error.cause.type).toBe('GROUP_COORDINATOR_NOT_AVAILABLE')
      expect(restart).toBe(true)
    })

    it("crashes the fetch loop when a cooperative member's coordinator keeps answering NOT_COORDINATOR", async () => {
      const { cluster, events } = scenario({
        ...SHORT,
        partitionAssigners: [PartitionAssigners.cooperativeSticky],
      })
      await start(events)
      await until(() => events.records.length === 1, 'the first record')
      cluster.answer('Heartbeat', () => ({ errorCode: NOT_COORDINATOR }))
      cluster.answer('JoinGroup', () => ({ errorCode: NOT_COORDINATOR }))
      await until(() => events.crashes.length > 0, 'a crash', 2000)

      const [{ error, restart }] = events.crashes
      expect(error.name).toBe('KafkaJSNumberOfRetriesExceeded')
      expect(error.cause.type).toBe('NOT_COORDINATOR_FOR_GROUP')
      expect(restart).toBe(true)
    })

    it('crashes the first join as before, with the lost connection', async () => {
      const { breaker, events } = scenario(SHORT)
      breaker.take(COORDINATOR, 'refused')
      await start(events)
      await until(() => events.crashes.length > 0, 'a crash', 2000)

      const [{ error, restart }] = events.crashes
      expect(restart).toBe(true)
      expect(lostConnection(error)).toBe(true)
      expect(events.joins).toEqual([])
    })

    it.each([
      ['a leader', LEADER],
      ['a coordinator', COORDINATOR],
    ])(
      'crashes the fetch loop when %s stops answering, however long each attempt takes',
      async (_name, node) => {
        const { breaker, events } = scenario(SHORT, { requestTimeout: 1000 })
        await start(events)
        await until(() => events.records.length === 1, 'the first record')
        breaker.take(node, 'timeout')
        await until(() => events.crashes.length > 0, 'a crash', 2000)

        const [{ error, restart }] = events.crashes
        expect(lostConnection(error)).toBe(true)
        expect(restart).toBe(true)
      }
    )

    it('crashes the fetch loop as before, once the retries are spent', async () => {
      const { breaker, events } = scenario(SHORT)
      await start(events)
      await until(() => events.records.length === 1, 'the first record')
      breaker.take(LEADER, 'refused')
      await until(() => events.crashes.length > 0, 'a crash', 2000)

      const [{ error, restart }] = events.crashes
      expect(error.name).toBe('KafkaJSNumberOfRetriesExceeded')
      expect(lostConnection(error)).toBe(true)
      expect(restart).toBe(true)
    })
  })

  it('a consumer-protocol member keeps counting a lost connection against its retries', async () => {
    const cluster = consumerProtocolScript(
      createScriptedCluster({ nodes: NODES, profile: 'consumer' }),
      { coordinator: COORDINATOR, batches: 1 }
    )
    const breaker = outages(cluster)
    const kafka = new Kafka(cluster.kafkaOptions({ retry: RETRY }))
    consumer = kafka.consumer({
      groupId: 'outage-group',
      groupProtocol: 'consumer',
      maxWaitTimeInMs: 20,
      retry: RETRY,
    })
    const events = { crashes: [], records: [] }
    consumer.on(consumer.events.CRASH, ({ payload }) => events.crashes.push(payload))
    await start(events)
    await until(() => events.records.length === 1, 'the first record')
    breaker.take(LEADER, 'refused')
    await until(() => events.crashes.length > 0, 'a crash', 2000)

    const [{ error }] = events.crashes
    expect(error.name).toBe('KafkaJSNumberOfRetriesExceeded')
    expect(lostConnection(error)).toBe(true)
  })

  it('a cluster another consumer disconnected under a running one leaves no connection behind', async () => {
    const scripted = groupScript(createScriptedCluster({ nodes: NODES }), {
      coordinator: COORDINATOR,
      batches: 1,
    })
    const shared = createCluster(
      { socketFactory: scripted.socketFactory, connectionTimeout: 1000, requestTimeout: 5000 },
      NODES.map(({ host, port }) => `${host}:${port}`)
    )
    const unused = createConsumer({ cluster: shared, groupId: 'outage-group', logger: newLogger() })
    consumer = createConsumer({
      cluster: shared,
      groupId: 'outage-group',
      logger: newLogger(),
      heartbeatInterval: 100,
      maxWaitTimeInMs: 20,
    })
    const events = { records: [] }
    await start(events)
    await until(() => events.records.length === 1, 'the first record')

    await unused.disconnect()
    await sleep(OUTAGE_MS / 2)
    await consumer.disconnect()
    consumer = undefined
    const disconnectedAt = scripted.requests().length
    await sleep(OUTAGE_MS)

    expect(scripted.sockets().filter((socket) => !socket.destroyed)).toEqual([])
    expect(scripted.requests().slice(disconnectedAt)).toEqual([])
  })
})
