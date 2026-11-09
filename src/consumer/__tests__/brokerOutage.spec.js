const { Kafka, PartitionAssigners } = require('../../../index')
const { KafkaJSConnectionError } = require('../../errors')
const createConsumer = require('../index')
const Runner = require('../runner')
const ClassicMembership = require('../groupProtocol/classic')
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
    jest.restoreAllMocks()
  })

  /**
   * @param {object} [options] consumer options over the scenario's
   * @param {object} [kafkaOptions]
   * @param {{ batches?: number, nodes?: object[], partitions?: number }} [script] how many Fetches carry the record batch
   * @returns {{ cluster: object, breaker: ReturnType<typeof outages>, events: object }}
   */
  const scenario = (
    options = {},
    kafkaOptions = {},
    { batches = 1, nodes = NODES, partitions = 1 } = {}
  ) => {
    const cluster = groupScript(createScriptedCluster({ nodes }), {
      coordinator: COORDINATOR,
      partitions,
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
    const events = { crashes: [], crashedAt: [], joins: [], rebalancing: [], records: [] }
    consumer.on(consumer.events.CRASH, ({ payload }) => {
      events.crashes.push(payload)
      events.crashedAt.push(performance.now())
    })
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

  it('recovers after a lost heartbeat when the skipped coordinator is refused and the surviving broker reports it unavailable', async () => {
    jest.spyOn(Math, 'random').mockReturnValue(0)
    const { cluster, breaker, events } = scenario()
    await start(events)
    await until(() => events.records.length === 1, 'the first record')

    const heartbeat = cluster.schedule.hold({ api: 'Heartbeat', node: COORDINATOR })
    await heartbeat.arrived()
    let unavailable = true
    cluster.answer('FindCoordinator', ({ coordinatorKeys }) => ({
      coordinators: coordinatorKeys.map((key) => ({
        key,
        nodeId: COORDINATOR,
        host: NODES[COORDINATOR].host,
        port: NODES[COORDINATOR].port,
        errorCode: unavailable ? 15 : 0,
        errorMessage: null,
      })),
    }))
    const outageAt = cluster.requests().length
    breaker.take(COORDINATOR, 'refused')
    heartbeat.release()
    await until(
      () =>
        wire(cluster, 'FindCoordinator', outageAt).some(
          ({ node, answeredOrder }) => node === LEADER && answeredOrder !== undefined
        ),
      'an unavailable coordinator answer from the surviving broker'
    )
    await sleep(OUTAGE_MS)
    const unavailableLookups = wire(cluster, 'FindCoordinator', outageAt).filter(
      ({ node, answeredOrder }) => node === LEADER && answeredOrder !== undefined
    )
    const restoredAt = cluster.requests().length
    unavailable = false
    breaker.restore(COORDINATOR)
    await until(
      () =>
        events.crashes.length > 0 ||
        wire(cluster, 'Heartbeat', restoredAt).some(
          ({ node, answeredOrder }) => node === COORDINATOR && answeredOrder !== undefined
        ),
      'a heartbeat answered after coordinator restoration or a crash'
    )

    expect(events.crashes).toEqual([])
    expect(unavailableLookups.length).toBeGreaterThan(0)
    expect(
      wire(cluster, 'Heartbeat', restoredAt).some(
        ({ node, answeredOrder }) => node === COORDINATOR && answeredOrder !== undefined
      )
    ).toBe(true)
    expect(wire(cluster, 'JoinGroup')).toHaveLength(1)
    expect(events.rebalancing).toEqual([])
  })

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

  const HELPER_SESSION = 1000

  /**
   * Runs a classic member whose handler commits through `commitOffsetsIfNecessary`, over a
   * coordinator that answers its first `failedAnswers` commits with `code`.
   * @param {number} failedAnswers
   * @param {{ code?: number, assigners?: object }} [options] the answer's error code (loading by
   *   default) and the member's assigner option
   * @returns {Promise<{ events: object, cluster: object, helper: { failed: number, completed: number, firstFailedAt?: number, crashedAt?: number } }>}
   */
  const startHelper = async (
    failedAnswers,
    { code = COORDINATOR_LOAD_IN_PROGRESS, assigners = {} } = {}
  ) => {
    const { cluster, events } = scenario({
      ...assigners,
      sessionTimeout: HELPER_SESSION,
      retry: { ...RETRY, restartOnFailure: async () => false },
    })
    endlessRecords(cluster)
    const helper = { failed: 0, completed: 0 }
    consumer.on(consumer.events.CRASH, () => {
      helper.crashedAt ??= performance.now()
    })
    cluster.answer('OffsetCommit', ({ topics }) => {
      const failing = helper.failed < failedAnswers
      if (failing) {
        helper.failed++
        helper.firstFailedAt ??= performance.now()
      }
      return {
        topics: topics.map((entry) => ({
          topic: entry.topic,
          partitions: entry.partitions.map(({ partition }) => ({
            partition,
            errorCode: failing ? code : 0,
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

  describe('a commit helper that eachBatch calls while the coordinator is still loading', () => {
    it('retries a loading answer that is short, with no crash and no rejoin', async () => {
      const { events, cluster, helper } = await startHelper(6)
      await until(
        () => helper.completed > 0 || events.crashes.length > 0,
        'the helper to commit or the consumer to crash'
      )

      expect(events.crashes).toEqual([])
      expect(helper.failed).toBe(6)
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
      expect(helper.crashedAt - helper.firstFailedAt).toBeGreaterThanOrEqual(HELPER_SESSION)
      expect(events.rebalancing).toEqual([])
      expect(wire(cluster, 'JoinGroup')).toHaveLength(1)
    })
  })

  describe('a handler that propagates a NOT_COORDINATOR from its own client call', () => {
    const ASSIGNERS = [
      ['an eager', {}],
      ['a cooperative', { partitionAssigners: [PartitionAssigners.cooperativeSticky] }],
    ]

    it('rides out the heartbeat() answers of an eager member, with no crash and no rejoin', async () => {
      const { cluster, events } = scenario({
        retry: { ...RETRY, restartOnFailure: async () => false },
      })
      endlessRecords(cluster)
      let rethrown = 0
      await start(events, {
        eachMessage: async ({ message, heartbeat }) => {
          events.records.push(message.offset)
          try {
            if (rethrown < 2) {
              const held = cluster.schedule.hold({ api: 'Heartbeat' })
              await held.arrived()
              // Share the pending wire request before releasing its answer: a fresh call can
              // otherwise be throttled after an automatic heartbeat renews the interval.
              const heartbeating = heartbeat()
              held.release({ errorCode: NOT_COORDINATOR })
              await heartbeating
            } else {
              await heartbeat()
            }
          } catch (e) {
            expect(e.code).toBe(NOT_COORDINATOR)
            rethrown++
            throw e
          }
        },
      })
      await until(() => rethrown === 2 || events.crashes.length > 0, 'two rethrown answers', 2000)
      const recordsAfter = events.records.length
      await until(
        () => events.records.length > recordsAfter + 3 || events.crashes.length > 0,
        'more records or a crash',
        2000
      )

      expect(events.crashes).toEqual([])
      expect(rethrown).toBe(2)
      expect(events.rebalancing).toEqual([])
      expect(wire(cluster, 'JoinGroup')).toHaveLength(1)
    })

    it.each(ASSIGNERS)(
      'rides out the commit answers %s member propagates from eachBatch, with no crash and no rejoin',
      async (_mode, assigners) => {
        const { events, cluster, helper } = await startHelper(2, {
          code: NOT_COORDINATOR,
          assigners,
        })
        await until(
          () => helper.completed > 0 || events.crashes.length > 0,
          'the helper to commit or the consumer to crash'
        )

        expect(events.crashes).toEqual([])
        expect(helper.failed).toBe(2)
        expect(events.rebalancing).toEqual([])
        expect(wire(cluster, 'JoinGroup')).toHaveLength(1)
      }
    )

    it.each(ASSIGNERS)(
      'crashes %s member with the commit error once the answer outlasts the session timeout, with no rejoin',
      async (_mode, assigners) => {
        const { events, cluster, helper } = await startHelper(Infinity, {
          code: NOT_COORDINATOR,
          assigners,
        })
        await until(() => events.crashes.length > 0, 'a crash', 2000)

        expect(events.crashes).toHaveLength(1)
        const [{ error, restart }] = events.crashes
        expect(error.name).toBe('KafkaJSNumberOfRetriesExceeded')
        expect(error.cause.code).toBe(NOT_COORDINATOR)
        expect(restart).toBe(false)
        expect(helper.crashedAt - helper.firstFailedAt).toBeGreaterThanOrEqual(HELPER_SESSION)
        expect(events.rebalancing).toEqual([])
        expect(wire(cluster, 'JoinGroup')).toHaveLength(1)
      }
    )
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

  it.each([
    ['coordinator first', 0],
    ['leader first', 0.99],
  ])(
    'a disconnect after a heartbeat lost the coordinator sends no leave and looks for no coordinator (%s)',
    async (_order, random) => {
      jest.spyOn(Math, 'random').mockReturnValue(random)
      const retry = { retries: 5, initialRetryTime: 300, maxRetryTime: 2000 }
      let heartbeatFailure
      const heartbeat = ClassicMembership.prototype.heartbeat
      jest
        .spyOn(ClassicMembership.prototype, 'heartbeat')
        .mockImplementation(async function (options) {
          try {
            return await heartbeat.call(this, options)
          } catch (error) {
            heartbeatFailure ??= error
            throw error
          }
        })
      const { cluster, breaker, events } = scenario(
        { heartbeatInterval: 1000, sessionTimeout: 6000, retry },
        { retry },
        { batches: 0 }
      )
      const held = cluster.schedule.hold({ api: 'Heartbeat' })
      await start(events)
      // Make a Fetch straddle the stop: its late empty result must not start a heartbeat
      // (and a fresh coordinator lookup) after stop released the existing lookup waiters.
      const fetch = cluster.schedule.hold({ api: 'Fetch', node: LEADER })
      await fetch.arrived()
      await held.arrived()
      breaker.take(COORDINATOR, 'refused')
      // Observe the real rejection before fetch-manager draining can delay its log behind a lookup.
      await until(() => heartbeatFailure !== undefined, 'the heartbeat that lost the coordinator')
      held.release()

      const lostAt = cluster.requests().length
      const disconnectedAt = performance.now()
      const disconnecting = consumer.disconnect()
      fetch.release()
      await disconnecting

      expect(lostConnection(heartbeatFailure)).toBe(true)
      expect(wire(cluster, 'FindCoordinator', lostAt)).toEqual([])
      expect(wire(cluster, 'LeaveGroup', lostAt)).toEqual([])
      expect(performance.now() - disconnectedAt).toBeLessThan(1000)
      expect(events.crashes).toEqual([])
    },
    20000
  )

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

  it.each([[15], [COORDINATOR_LOAD_IN_PROGRESS]])(
    'a stop during a join attempt answered %i that outlasts the session timeout ends the join without a crash',
    async (errorCode) => {
      // No retries: the first answer must reach the outage bound, not the join retrier.
      const { cluster, events } = scenario({
        sessionTimeout: 1000,
        retry: { ...RETRY, retries: 0, restartOnFailure: async () => false },
      })
      cluster.schedule.once({ api: 'JoinGroup' }, { errorCode })
      const held = cluster.schedule.hold({ api: 'JoinGroup' })
      const running = start(events)
      await held.arrived()

      const disconnecting = consumer.disconnect()
      await sleep(1200)
      held.release({ errorCode })
      await disconnecting
      await running

      expect(events.crashes).toEqual([])
      expect(events.joins).toEqual([])
      expect(cluster.sockets().filter((socket) => !socket.destroyed)).toEqual([])
    }
  )

  describe('independent broker fetch outages', () => {
    /**
     * Runs two partition leaders beside a separate coordinator, capturing the real runner while
     * keeping its Fetch and retry decisions intact. Both leaders continuously produce records.
     * Outage clocks are keyed by the pool's string node ids.
     * Reconnect backoff is capped at 100 ms so the crash timing measures the outage deadline,
     * not reconnect pacing.
     * @returns {object} the scenario, leaders, and access to the active runner
     */
    const leaders = () => {
      const nodes = [...NODES, { id: 2, host: 'node2', port: 9092 }]
      const rig = scenario(
        {
          sessionTimeout: 1000,
          heartbeatInterval: 100,
          retry: { ...RETRY, restartOnFailure: async () => false },
        },
        { reconnectBackoffMax: 100 },
        { nodes, partitions: 2 }
      )
      rig.cluster.answer('Metadata', () => ({
        brokers: nodes.map(({ id, host, port }) => ({ nodeId: id, host, port, rack: null })),
        controllerId: COORDINATOR,
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
      endlessRecords(rig.cluster)
      let runner
      const fetch = Runner.prototype.fetch
      jest.spyOn(Runner.prototype, 'fetch').mockImplementation(function (nodeId) {
        runner = this
        return fetch.call(this, nodeId)
      })
      return { ...rig, runner: () => runner }
    }

    it('expires a failed broker on its original deadline while its sibling keeps producing', async () => {
      const rig = leaders()
      await start(rig.events)
      await until(() => rig.events.records.length >= 4, 'both leaders producing')
      rig.breaker.take(2, 'reset')
      const runner = rig.runner()
      await until(() => runner.outages.fetch.nodes.has('2'), 'the failed broker clock')
      const first = runner.outages.fetch.nodes.get('2').since
      const records = rig.events.records.length
      await sleep(500)
      expect(rig.events.records.length).toBeGreaterThan(records)
      expect(runner.outages.fetch.nodes.get('2').since).toBe(first)
      expect(rig.events.crashes).toHaveLength(0)
      await until(() => rig.events.crashes.length > 0, 'the exhausted broker crash', 600)
      expect(rig.events.crashedAt[0] - first).toBeGreaterThanOrEqual(1000)
      expect(rig.events.crashedAt[0] - first).toBeLessThan(2500)
      expect(rig.events.crashes).toHaveLength(1)
      expect(rig.events.crashes[0].error.name).toBe('KafkaJSNumberOfRetriesExceeded')
      expect(lostConnection(rig.events.crashes[0].error)).toBe(true)
      expect(rig.events.crashes[0].restart).toBe(false)
    })

    it('clears only the recovered broker and keeps the other broker first failure time', async () => {
      const rig = leaders()
      await start(rig.events)
      await until(() => rig.events.records.length >= 4, 'both leaders producing')
      rig.breaker.take(LEADER, 'reset')
      rig.breaker.take(2, 'reset')
      const runner = rig.runner()
      await until(() => runner.outages.fetch.nodes.size === 2, 'both broker clocks')
      const first = runner.outages.fetch.nodes.get('2').since
      await sleep(200)
      rig.breaker.restore(LEADER)
      await until(
        () => !runner.outages.fetch.nodes.has(String(LEADER)),
        'the recovered broker clock cleared'
      )
      expect(runner.outages.fetch.nodes.get('2').since).toBe(first)
      expect(rig.events.crashes).toHaveLength(0)
      await until(() => rig.events.crashes.length > 0, 'the remaining broker crash', 600)
      expect(rig.events.crashes).toHaveLength(1)
      expect(lostConnection(rig.events.crashes[0].error)).toBe(true)
    })

    it('retires a removed broker clock before it returns and clears clocks on stop', async () => {
      const rig = leaders()
      await start(rig.events)
      await until(() => rig.events.records.length >= 4, 'both leaders producing')
      const runner = rig.runner()
      rig.breaker.take(2, 'reset')
      await until(() => runner.outages.fetch.nodes.has('2'), 'the failed broker clock')
      const first = runner.outages.fetch.nodes.get('2').since
      const getNodeIds = runner.consumerGroup.getNodeIds.bind(runner.consumerGroup)
      let removed = true
      jest
        .spyOn(runner.consumerGroup, 'getNodeIds')
        .mockImplementation(() => getNodeIds().filter((nodeId) => !removed || nodeId !== '2'))
      await until(() => !runner.outages.fetch.nodes.has('2'), 'the removed broker retired')
      removed = false
      await until(() => runner.outages.fetch.nodes.has('2'), 'the returning broker clock')
      expect(runner.outages.fetch.nodes.get('2').since).toBeGreaterThan(first)
      await consumer.stop()
      expect(runner.outages.fetch.nodes.size).toBe(0)
      expect(rig.events.crashes).toHaveLength(0)
    })

    it('keeps exhaustion through a healthy fetch completing during manager teardown and surfaces once', async () => {
      const rig = leaders()
      await start(rig.events)
      await until(() => rig.events.records.length >= 4, 'both leaders producing')
      const runner = rig.runner()
      const retry = runner.fetchRetryDelay.bind(runner)
      let exhausted = false
      jest.spyOn(runner, 'fetchRetryDelay').mockImplementation((error, nodeId) => {
        const delay = retry(error, nodeId)
        if (nodeId === '2' && delay === undefined) exhausted = true
        return delay
      })
      let release
      const held = new Promise((resolve) => {
        release = resolve
      })
      const answer = rig.cluster.answerers.get('Fetch')
      let holding = false
      rig.cluster.answer('Fetch', async (fields, record) => {
        if (record.node === LEADER) {
          holding = true
          await held
        }
        return answer(fields, record)
      })
      try {
        await until(() => holding, 'a healthy fetch held open')
        rig.breaker.take(2, 'reset')
        await until(() => runner.outages.fetch.nodes.has('2'), 'the failed broker clock')
        const first = runner.outages.fetch.nodes.get('2').since
        const outerChecks = []
        const ride = runner.rideOutage.bind(runner)
        jest.spyOn(runner, 'rideOutage').mockImplementation((error, nodeId) => {
          const delay = ride(error, nodeId)
          if (nodeId === undefined && runner.fetchNodes.get(error) === '2') {
            outerChecks.push({ since: runner.outages.fetch.nodes.get('2')?.since, delay })
          }
          return delay
        })
        await until(() => exhausted, 'budget exhaustion', 600)
        await sleep(30)
        expect(rig.events.crashes).toHaveLength(0)
        expect(runner.outages.fetch.nodes.get('2').since).toBe(first)
        release()
        await until(() => rig.events.crashes.length > 0, 'the single crash', 600)
        expect(rig.events.crashes).toHaveLength(1)
        expect(lostConnection(rig.events.crashes[0].error)).toBe(true)
        expect(outerChecks).toEqual([
          { since: first, delay: undefined },
          { since: first, delay: undefined },
        ])
        expect(rig.events.crashes[0].restart).toBe(false)
      } finally {
        release()
      }
    })
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

  it('a consumer-protocol member rides a lost connection without spending retries', async () => {
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
    const events = { crashes: [], records: [], joins: [], rebalancing: [] }
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => events.joins.push(payload))
    consumer.on(consumer.events.REBALANCING, ({ payload }) => events.rebalancing.push(payload))
    consumer.on(consumer.events.CRASH, ({ payload }) => events.crashes.push(payload))
    await start(events)
    await until(() => events.records.length === 1, 'the first record')
    const joins = events.joins.length
    const rebalances = events.rebalancing.length
    breaker.take(LEADER, 'refused')
    await sleep(OUTAGE_MS)
    const before = cluster.requests().filter(({ api }) => api === 'Fetch').length
    breaker.restore(LEADER)
    await until(
      () => cluster.requests().filter(({ api }) => api === 'Fetch').length > before,
      'a restored Fetch'
    )
    expect(events.crashes).toEqual([])
    expect(events.joins).toHaveLength(joins)
    expect(events.rebalancing).toHaveLength(rebalances)
    await consumer.disconnect()
    consumer = undefined
    cluster.assertClean()
  })

  describe('a consumer-protocol member and its fencing', () => {
    // The client's default retry shape, so the heartbeat backoff is the one a user gets; enough
    // retries that the empty fetch rounds' heartbeat errors cannot crash the loop in the window.
    const SLOW_RETRY = { retries: 10, initialRetryTime: 300, maxRetryTime: 30000 }

    /**
     * @param {(request: { memberId: string, memberEpoch: number }) => object|null} [override]
     *   answers a heartbeat itself when it returns an answer
     * @returns {{ cluster: object, breaker: ReturnType<typeof outages>, heartbeats: number[] }}
     */
    const fencingScenario = (override = () => null) => {
      const cluster = consumerProtocolScript(
        createScriptedCluster({ nodes: NODES, profile: 'consumer' }),
        { coordinator: COORDINATOR, batches: 1 }
      )
      const heartbeats = []
      let epoch = 0
      cluster.answer('ConsumerGroupHeartbeat', (request) => {
        heartbeats.push(Date.now())
        const { memberId, memberEpoch } = request
        return (
          override(request) || {
            memberId,
            memberEpoch: memberEpoch === 0 ? ++epoch : memberEpoch,
            heartbeatIntervalMs: 100,
            assignment:
              memberEpoch === 0
                ? { topicPartitions: [{ topicId: TOPIC_ID, partitions: [0] }] }
                : null,
          }
        )
      })
      return { cluster, breaker: outages(cluster), heartbeats }
    }

    it("reaches its coordinator again within about a second of its return, as Java's retry.backoff.max.ms caps the heartbeat backoff", async () => {
      const { cluster, breaker, heartbeats } = fencingScenario()
      consumer = new Kafka(cluster.kafkaOptions({ retry: SLOW_RETRY })).consumer({
        groupId: 'outage-group',
        groupProtocol: 'consumer',
        maxWaitTimeInMs: 20,
        retry: SLOW_RETRY,
      })
      const events = { crashes: [], records: [] }
      consumer.on(consumer.events.CRASH, ({ payload }) => events.crashes.push(payload))
      await start(events, { autoCommit: false })
      await until(() => events.records.length === 1, 'the first record')

      breaker.take(COORDINATOR, 'reset')
      await sleep(5000)
      const before = heartbeats.length
      const restoredAt = Date.now()
      breaker.restore(COORDINATOR)
      await until(() => heartbeats.length > before, 'a heartbeat after the restore', 2000)

      // Uncapped, the attempts are 0.3, 0.6, 1.2, 2.4 and 4.8 s apart, so the first one after a
      // 5 s outage comes 2.4 to 6.2 s after the coordinator is back.
      expect(heartbeats[before] - restoredAt).toBeLessThan(1500)
      expect(events.crashes).toEqual([])
    })

    it('delivers nothing more from a batch once a background heartbeat answers that it is fenced', async () => {
      const entered = []
      let inHandler = false
      let enteredAtFence = null
      // Only a heartbeat that arrives while a record is in the handler is fenced: that one is the
      // 100 ms timer's, never the runner's own per-message call, which runs after the handler.
      const { cluster } = fencingScenario(({ memberId, memberEpoch }) => {
        if (memberEpoch <= 0 || enteredAtFence !== null || !inHandler) return null
        enteredAtFence = entered.length
        return {
          errorCode: 110,
          memberId,
          memberEpoch,
          heartbeatIntervalMs: 100,
          assignment: null,
        }
      })
      cluster.answer('Fetch', async ({ maxWaitMs, topics }) => {
        const [{ partition, fetchOffset }] = topics[0].partitions
        if (fetchOffset !== '0' || entered.length > 0) {
          await sleep(maxWaitMs)
          return { partitions: [{ partition, errorCode: 0, records: false }] }
        }
        const batch = await RecordBatch({
          firstOffset: fetchOffset,
          firstTimestamp: 1700000000000,
          maxTimestamp: 1700000000000,
          records: [0, 1, 2].map((offsetDelta) =>
            RecordEncoder({ offsetDelta, key: Buffer.from('key'), value: Buffer.from('value') })
          ),
        })
        return { partitions: [{ partition, errorCode: 0, records: Buffer.from(batch.buffer) }] }
      })
      consumer = new Kafka(cluster.kafkaOptions({ retry: RETRY })).consumer({
        groupId: 'outage-group',
        groupProtocol: 'consumer',
        maxWaitTimeInMs: 20,
        retry: RETRY,
      })
      let enteredAtLoss = null
      consumer.on(consumer.events.PARTITIONS_LOST, () => {
        enteredAtLoss ??= entered.length
      })
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      await consumer.run({
        autoCommit: false,
        eachMessage: async ({ message }) => {
          entered.push(message.offset)
          inHandler = true
          await sleep(300)
          inHandler = false
        },
      })
      await until(() => enteredAtLoss !== null, 'the lost partitions', 2000)

      expect(enteredAtFence).toBe(1)
      expect(entered.slice(0, enteredAtLoss)).toEqual(['0'])
    })
  })

  it('a classic member delivers nothing more once a background heartbeat answers that it is unknown, and gets every record again after its rejoin', async () => {
    const records = ['0', '1', '2', '3', '4', '5']
    const cluster = groupScript(createScriptedCluster({ nodes: NODES }), {
      coordinator: COORDINATOR,
    })
    const entered = []
    let inHandler = false
    let enteredAtFence = null
    cluster.answer('Heartbeat', () => {
      if (!inHandler || enteredAtFence !== null) return { errorCode: 0 }
      enteredAtFence = entered.length
      return { errorCode: 25 }
    })
    // A long poll keeps the fetch loop from settling for seconds after the loss, the window in
    // which the member used to go on delivering.
    cluster.answer('Fetch', async ({ maxWaitMs, topics }) => {
      const [{ partition, fetchOffset }] = topics[0].partitions
      const first = Number(fetchOffset)
      if (first >= records.length) {
        await sleep(maxWaitMs)
        return { partitions: [{ partition, errorCode: 0, records: false }] }
      }
      const batch = await RecordBatch({
        firstOffset: fetchOffset,
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        records: records
          .slice(first)
          .map((_, offsetDelta) =>
            RecordEncoder({ offsetDelta, key: Buffer.from('key'), value: Buffer.from('value') })
          ),
      })
      return { partitions: [{ partition, errorCode: 0, records: Buffer.from(batch.buffer) }] }
    })
    consumer = new Kafka(cluster.kafkaOptions({ retry: RETRY })).consumer({
      groupId: 'outage-group',
      heartbeatInterval: 100,
      maxWaitTimeInMs: 3000,
      retry: RETRY,
    })
    let enteredAtLoss = null
    consumer.on(consumer.events.PARTITIONS_LOST, () => {
      enteredAtLoss ??= entered.length
    })
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await consumer.run({
      eachMessage: async ({ message }) => {
        entered.push(message.offset)
        inHandler = true
        await sleep(200)
        inHandler = false
      },
    })
    await until(() => enteredAtLoss !== null, 'the lost partitions', 2000)
    await until(() => entered.length >= enteredAtLoss + records.length, 'the redelivery', 2000)

    expect(entered.slice(0, enteredAtLoss)).toEqual(records.slice(0, enteredAtFence))
    expect(entered.slice(enteredAtLoss)).toEqual(records)
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
