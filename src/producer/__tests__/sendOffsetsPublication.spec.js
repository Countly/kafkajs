const { Kafka } = require('../../../index')
const Cluster = require('../../cluster')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { WireOrder, WireOrderViolation } = require('../../../testHelpers/wireOrder')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')

const NODES = [{ id: 0, host: 'node0', port: 9092 }]
const GROUP = 'publication-group'

/**
 * Runs one transaction that sends a record and the offsets of a consumer group, and commits it, on
 * a scripted cluster of the given transaction profile. Every write of the shared committed-offset
 * cache is marked on the cluster's timeline as `cache write`.
 * @param {'tv1'|'tv2'} profile
 * @returns {Promise<{ cluster: object, writes: object[] }>} the cluster, and the arguments of each
 *   cache write
 */
const commitOneTransaction = async (profile) => {
  const cluster = producerScript(createScriptedCluster({ nodes: NODES, profile }))
  const writes = []
  const write = Cluster.prototype.markOffsetAsCommitted
  jest.spyOn(Cluster.prototype, 'markOffsetAsCommitted').mockImplementation(function (offsets) {
    cluster.mark('cache write')
    writes.push(offsets)
    return write.call(this, offsets)
  })

  const producer = new Kafka(cluster.kafkaOptions()).producer({
    transactionalId: `publication-${profile}`,
    maxInFlightRequests: 1,
  })
  try {
    await producer.connect()
    const transaction = await producer.transaction()
    await transaction.send({ topic: TOPIC, messages: [{ value: 'a' }] })
    await transaction.sendOffsets({
      consumerGroupId: GROUP,
      topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '5' }] }],
    })
    await transaction.commit()
    cluster.assertClean()
  } finally {
    await producer.disconnect()
  }

  return { cluster, writes }
}

describe.each(['tv1', 'tv2'])('Producer > transactional offsets on the wire (%s)', (profile) => {
  let run
  /** What `assertOrder` found: null when the publication order holds, else its violation. */
  let publicationOrder

  beforeEach(async () => {
    run = await commitOneTransaction(profile)
    try {
      WireOrder.of(run.cluster).assertOrder([
        'TxnOffsetCommit response',
        'EndTxn request',
        'EndTxn response',
        'cache write',
      ])
      publicationOrder = null
    } catch (error) {
      if (!(error instanceof WireOrderViolation)) {
        throw error
      }
      publicationOrder = error
    }
  })

  afterEach(() => jest.restoreAllMocks())

  it('commits the transaction and marks the offsets of the call committed once', () => {
    expect(run.writes).toEqual([{ groupId: GROUP, topic: TOPIC, partition: 0, offset: '5' }])
    expect(run.cluster.requests().map(({ api }) => api)).toContain('EndTxn')
    run.cluster.assertClean()
  })

  /**
   * The cache is the shared per-instance record of what a group has committed, so it may be
   * written only once the commit that carries the offsets is confirmed: after the EndTxn response,
   * never when the TxnOffsetCommit is answered. The run happens in `beforeEach`, which records any
   * ordering violation for this case to report.
   */
  it('writes the cache only after the EndTxn response', () => {
    expect(publicationOrder).toBeNull()
  })
})

{
  const createProducer = require('../index')
  const { createFakeCluster } = require('./fixtures/fakeCluster')
  const OffsetOperations = require('../../consumer/offsetManager/offsetOperations')
  const { KafkaJSProtocolError, KafkaJSConnectionClosedError } = require('../../errors')
  const { createErrorFromCode } = require('../../protocol/error')
  const { newLogger } = require('testHelpers')

  const topic = 'publication-topic'
  const groupId = 'publication-group'
  const topics = (offset, partition = 0) => [
    { topic, partitions: [{ partition, offset, leaderEpoch: 3, metadata: 'original' }] },
  ]
  const tick = () => new Promise((resolve) => setImmediate(resolve))
  const deferred = () => {
    let resolve
    const promise = new Promise((done) => {
      resolve = done
    })
    return { promise, resolve }
  }

  /** A real public producer wrapper and state machine over an explicitly object-level broker. */
  const open = async (level = 2) => {
    const cluster = createFakeCluster()
    const broker = cluster.broker
    const ledger = new OffsetOperations()
    const cache = new Map()
    const order = []
    cluster.offsetOperations = () => ledger
    cluster.markOffsetAsCommitted.mockImplementation(({ topic, partition, offset }) => {
      order.push('cache')
      cache.set(`${topic}:${partition}`, offset)
    })
    Object.assign(broker, {
      finalizedFeatures: { 'transaction.version': { minVersionLevel: 0, maxVersionLevel: level } },
      finalizedFeaturesEpoch: 1,
      negotiatedVersion: () => 5,
      addOffsetsToTxn: jest.fn(async () => ({})),
      txnOffsetCommit: jest.fn(async () => {
        order.push('TxnOffsetCommit response')
      }),
    })
    broker.initProducerId.mockImplementation(async ({ producerEpoch = -1 }) => ({
      producerId: '1000',
      producerEpoch: producerEpoch + 1,
    }))
    broker.endTxn.mockImplementation(async () => {
      order.push('EndTxn request')
      order.push('EndTxn response')
      return { producerId: '-1', producerEpoch: -1 }
    })
    const producer = createProducer({
      cluster,
      logger: newLogger(),
      transactionalId: 'publication-transaction',
      retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 2 },
    })
    await producer.connect()
    const transaction = await producer.transaction()
    return {
      cluster,
      broker,
      ledger,
      cache,
      order,
      producer,
      transaction,
      send: (value) => transaction.sendOffsets({ consumerGroupId: groupId, topics: value }),
    }
  }

  describe('Transactional offset publication object controls', () => {
    let fixture
    afterEach(async () => {
      if (fixture) {
        await fixture.producer.disconnect()
        fixture = undefined
      }
    })

    test.each([1, 2])('version %s publishes only after confirmed commit, once', async (level) => {
      fixture = await open(level)
      await fixture.send(topics('9'))
      expect(fixture.cluster.markOffsetAsCommitted).not.toHaveBeenCalled()
      expect(fixture.ledger.size()).toBe(1)
      await fixture.transaction.commit()
      expect(fixture.order).toEqual([
        'TxnOffsetCommit response',
        'EndTxn request',
        'EndTxn response',
        'cache',
      ])
      expect(fixture.cache.get(`${topic}:0`)).toBe('9')
      expect(fixture.ledger.size()).toBe(0)
      await fixture.producer.disconnect()
      expect(fixture.cluster.markOffsetAsCommitted).toHaveBeenCalledTimes(1)
    })

    test.each([1, 2])(
      'version %s confirmed abort releases and publishes nothing',
      async (level) => {
        fixture = await open(level)
        await fixture.send(topics('9'))
        await fixture.transaction.abort()
        expect(fixture.cluster.markOffsetAsCommitted).not.toHaveBeenCalled()
        expect(fixture.ledger.size()).toBe(0)
      }
    )

    test.each(['before transmission', 'response lost'])(
      'unknown commit %s keeps staging until a confirmed retry',
      async (variant) => {
        fixture = await open()
        let brokerOffset = '0'
        fixture.broker.endTxn
          .mockImplementationOnce(async () => {
            if (variant === 'response lost') brokerOffset = '9'
            throw new KafkaJSConnectionClosedError('unconfirmed')
          })
          .mockImplementationOnce(async () => {
            if (variant === 'response lost') brokerOffset = '9'
            throw new KafkaJSConnectionClosedError('unconfirmed')
          })
          .mockImplementationOnce(async () => {
            brokerOffset = '9'
            return {}
          })
        await fixture.send(topics('9'))
        await expect(fixture.transaction.commit()).rejects.toMatchObject({ retriable: true })
        expect(fixture.cluster.markOffsetAsCommitted).not.toHaveBeenCalled()
        expect(fixture.ledger.size()).toBe(1)
        expect(brokerOffset).toBe(variant === 'response lost' ? '9' : '0')
        await fixture.transaction.commit()
        expect(fixture.cache.get(`${topic}:0`)).toBe('9')
        expect(fixture.ledger.size()).toBe(0)
      }
    )

    test('disconnect confirms a pending commit before releasing its fence', async () => {
      fixture = await open()
      await fixture.send(topics('9'))
      fixture.broker.endTxn
        .mockRejectedValueOnce(new KafkaJSConnectionClosedError('unknown'))
        .mockRejectedValueOnce(new KafkaJSConnectionClosedError('unknown'))
      await expect(fixture.transaction.commit()).rejects.toThrow('unknown')
      await fixture.producer.disconnect()
      expect(fixture.cache.get(`${topic}:0`)).toBe('9')
      expect(fixture.order.slice(-3)).toEqual(['EndTxn request', 'EndTxn response', 'cache'])
      expect(fixture.ledger.size()).toBe(0)
    })

    test.each([false, true])(
      'unknown disconnect forfeits permanently, intervening mutation=%s',
      async (mutate) => {
        fixture = await open()
        await fixture.send(topics('9'))
        fixture.cache.set(`${topic}:0`, '4')
        fixture.broker.endTxn.mockRejectedValue(new KafkaJSConnectionClosedError('unknown'))
        await expect(fixture.transaction.commit()).rejects.toThrow('unknown')
        await fixture.producer.disconnect()
        expect(fixture.ledger.size()).toBe(0)
        if (mutate) {
          const ticket = fixture.ledger.begin([{ topic, partition: 0 }], { mutation: true })
          fixture.ledger.apply(ticket, topic, 0)
          fixture.cache.set(`${topic}:0`, '2')
          fixture.ledger.end(ticket)
        }
        fixture.broker.endTxn.mockResolvedValue({})
        await fixture.producer.connect()
        await fixture.transaction.commit()
        expect(fixture.cache.get(`${topic}:0`)).toBe(mutate ? '2' : '4')
        expect(fixture.cluster.markOffsetAsCommitted).not.toHaveBeenCalled()
      }
    )

    test.each([1, 2])(
      'version %s forfeits the offsets an open transaction sent before disconnect, which aborts it, and its commit after reconnect is refused',
      async (level) => {
        fixture = await open(level)
        await fixture.send(topics('9'))
        await fixture.producer.disconnect()
        expect(fixture.ledger.size()).toBe(0)
        expect(fixture.broker.endTxn).toHaveBeenCalledTimes(1)
        expect(fixture.broker.endTxn).toHaveBeenCalledWith(
          expect.objectContaining({ transactionResult: false })
        )
        await fixture.producer.connect()
        await expect(fixture.transaction.commit()).rejects.toThrow(
          'Cannot continue to use transaction once ended'
        )
        expect(fixture.broker.endTxn).toHaveBeenCalledTimes(1)
        expect(fixture.cluster.markOffsetAsCommitted).not.toHaveBeenCalled()
        expect(fixture.ledger.size()).toBe(0)
      }
    )

    test.each([
      ['9', '2'],
      ['2', '9'],
    ])('concurrent %s then %s is serialized through the answer', async (a, b) => {
      fixture = await open()
      const held = deferred()
      fixture.broker.txnOffsetCommit.mockImplementationOnce(async () => held.promise)
      const first = fixture.send(topics(a))
      const second = fixture.send(topics(b))
      await tick()
      expect(fixture.broker.txnOffsetCommit).toHaveBeenCalledTimes(1)
      held.resolve()
      await Promise.all([first, second])
      await fixture.transaction.commit()
      expect(
        fixture.broker.txnOffsetCommit.mock.calls.map(
          ([request]) => request.topics[0].partitions[0].offset
        )
      ).toEqual([a, b])
      expect(fixture.cache.get(`${topic}:0`)).toBe(b)
    })

    test('call-time snapshot feeds the wire and fence survives a held coordinator lookup', async () => {
      fixture = await open()
      const held = deferred()
      fixture.cluster.findGroupCoordinator.mockImplementationOnce(async () => held.promise)
      const input = topics('9')
      const sending = fixture.send(input)
      input[0].partitions[0].offset = '88'
      input[0].partitions[0].leaderEpoch = 99
      input[0].partitions[0].metadata = 'mutated'
      fixture.ledger.touchTopic(topic)
      held.resolve(fixture.broker)
      await sending
      expect(fixture.broker.txnOffsetCommit.mock.calls[0][0].topics).toEqual(topics('9'))
      await fixture.transaction.commit()
      expect(fixture.cluster.markOffsetAsCommitted).not.toHaveBeenCalled()
      expect(fixture.ledger.size()).toBe(0)
    })

    test.each([1, 2])(
      'version %s retries an earlier call before transmitting the later call',
      async (level) => {
        fixture = await open(level)
        const held = deferred()
        const error = new KafkaJSProtocolError(createErrorFromCode(14))
        // A fresh manager with one explicit retry budget; the outer fixture remains cleanup-safe.
        await fixture.producer.disconnect()
        fixture.producer = createProducer({
          cluster: fixture.cluster,
          logger: newLogger(),
          transactionalId: 'retry-transaction',
          retry: { retries: 1, initialRetryTime: 1, factor: 0 },
        })
        await fixture.producer.connect()
        fixture.transaction = await fixture.producer.transaction()
        fixture.broker.txnOffsetCommit.mockImplementationOnce(async () => {
          await held.promise
          throw error
        })
        const send = (value) =>
          fixture.transaction.sendOffsets({ consumerGroupId: groupId, topics: topics(value) })
        const first = send('9')
        const second = send('2')
        await tick()
        expect(fixture.broker.txnOffsetCommit).toHaveBeenCalledTimes(1)
        held.resolve()
        await Promise.all([first, second])
        expect(
          fixture.broker.txnOffsetCommit.mock.calls.map(
            ([request]) => request.topics[0].partitions[0].offset
          )
        ).toEqual(['9', '9', '2'])
        await fixture.transaction.commit()
        expect(fixture.cache.get(`${topic}:0`)).toBe('2')
      }
    )

    test.each([1, 2])(
      'version %s empty topics keep current wire behavior and no ticket',
      async (level) => {
        fixture = await open(level)
        await fixture.send([])
        await fixture.send([{ topic, partitions: [] }])
        expect(fixture.broker.addOffsetsToTxn).toHaveBeenCalledTimes(level === 1 ? 2 : 0)
        expect(fixture.broker.txnOffsetCommit).toHaveBeenCalledTimes(level === 1 ? 2 : 0)
        expect(fixture.ledger.size()).toBe(0)
        await fixture.transaction.commit()
        expect(fixture.cluster.markOffsetAsCommitted).not.toHaveBeenCalled()
        expect(fixture.ledger.size()).toBe(0)
      }
    )

    test.each([90, 30])(
      'failed offset call code %s frees its own fence and publishes nothing',
      async (code) => {
        fixture = await open()
        fixture.broker.txnOffsetCommit.mockRejectedValue(
          new KafkaJSProtocolError(createErrorFromCode(code))
        )
        await expect(fixture.send(topics('9'))).rejects.toThrow()
        expect(fixture.ledger.size()).toBe(0)
        expect(fixture.cluster.markOffsetAsCommitted).not.toHaveBeenCalled()
      }
    )
    test('fatal offset failure releases fences staged by earlier accepted calls', async () => {
      fixture = await open()
      await fixture.send(topics('9'))
      fixture.broker.txnOffsetCommit.mockRejectedValue(
        new KafkaJSProtocolError(createErrorFromCode(90))
      )
      await expect(fixture.send(topics('2', 1))).rejects.toThrow()
      expect(fixture.ledger.size()).toBe(0)
      expect(fixture.cluster.markOffsetAsCommitted).not.toHaveBeenCalled()
    })

    test.each([1, 2])(
      'version %s a queued call is refused, untransmitted, once an earlier call has made the producer unusable',
      async (level) => {
        fixture = await open(level)
        const held = deferred()
        fixture.broker.txnOffsetCommit.mockImplementationOnce(async () => {
          await held.promise
          throw new KafkaJSProtocolError(createErrorFromCode(90))
        })
        const settled = Promise.allSettled([fixture.send(topics('9')), fixture.send(topics('2'))])
        await tick()
        held.resolve()
        expect((await settled).map(({ status }) => status)).toEqual(['rejected', 'rejected'])
        expect(fixture.broker.txnOffsetCommit).toHaveBeenCalledTimes(1)
        expect(fixture.broker.addOffsetsToTxn).toHaveBeenCalledTimes(level === 1 ? 1 : 0)
        expect(fixture.ledger.size()).toBe(0)
        expect(fixture.cluster.markOffsetAsCommitted).not.toHaveBeenCalled()
      }
    )
  })
}

{
  const groupId = 'publication-group'
  const tick = () => new Promise((resolve) => setImmediate(resolve))
  const { KafkaJSConnectionClosedError } = require('../../errors')
  const Connection = require('../../network/connection')
  const apiKeys = require('../../protocol/requests/apiKeys')

  const { Kafka } = require('../../../index')
  const Cluster = require('../../cluster')
  const Runner = require('../../consumer/runner')
  const {
    createScriptedCluster,
    groupScript,
    producerScript,
  } = require('../../../testHelpers/scriptedCluster')
  const { prepareRecordBatches, TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')
  const { WireOrder } = require('../../../testHelpers/wireOrder')

  /** Waits for a public group join without relying on an arbitrary sleep. */
  const joined = (consumer) =>
    new Promise((resolve) => {
      const remove = consumer.on(consumer.events.GROUP_JOIN, () => {
        remove()
        resolve()
      })
    })

  /**
   * A transaction and two consumers created only through public Kafka factories. Request records
   * and response completion records come from the seam; cache writes use a call-through spy.
   */
  const openWire = async (profile, separate = false) => {
    const scripted = groupScript(
      producerScript(
        createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }], profile }),
        { partitions: 2 }
      ),
      { batches: 0, partitions: 2 }
    )
    scripted.answer('InitProducerId', ({ producerEpoch }) => ({
      producerId: '1000',
      producerEpoch: producerEpoch < 0 ? 0 : producerEpoch + 1,
    }))
    const gates = []
    const hold = scripted.schedule.hold.bind(scripted.schedule)
    scripted.schedule.hold = (selector) => {
      const gate = hold(selector)
      gates.push(gate)
      return gate
    }
    const kafka = new Kafka(
      scripted.kafkaOptions({
        requestTimeout: 20000,
        retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 2 },
      })
    )
    const peerKafka = separate ? new Kafka(scripted.kafkaOptions({ requestTimeout: 20000 })) : kafka
    const producer = kafka.producer({
      transactionalId: 'wire-publication',
      retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 2 },
    })
    const consumers = [0, 1].map(() =>
      peerKafka.consumer({ groupId, heartbeatInterval: 100, maxWaitTimeInMs: 20 })
    )
    const runners = []
    const start = Runner.prototype.start
    const capture = jest.spyOn(Runner.prototype, 'start').mockImplementation(function (...args) {
      runners.push(this)
      return start.apply(this, args)
    })
    const writes = []
    const mark = Cluster.prototype.markOffsetAsCommitted
    const publication = jest
      .spyOn(Cluster.prototype, 'markOffsetAsCommitted')
      .mockImplementation(function (value) {
        scripted.mark('cache write')
        WireOrder.of(scripted).assertEndTxnAfterOffsets()
        writes.push({ cluster: this, value, trace: scripted.timeline() })
        return mark.call(this, value)
      })
    /** Attempts every owned close and restoration before exposing its first failure. */
    const close = async () => {
      const failures = []
      const attempt = async (work) => {
        try {
          await work()
        } catch (error) {
          failures.push(error)
        }
      }
      for (const gate of gates)
        await attempt(() => {
          if (gate.settle) gate.release()
        })
      await Promise.all(consumers.map((consumer) => attempt(() => consumer.disconnect())))
      await attempt(() => producer.disconnect())
      await attempt(() => scripted.assertClean())
      await attempt(() => capture.mockRestore())
      await attempt(() => publication.mockRestore())
      if (failures.length) throw failures[0]
    }
    try {
      await producer.connect()
      for (const consumer of consumers) {
        await consumer.connect()
        await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
        const ready = joined(consumer)
        await consumer.run({ autoCommit: false, eachMessage: async () => {} })
        await ready
      }
      const transaction = await producer.transaction()
      const ledger = runners[0].consumerGroup.operations
      for (let turn = 0; turn < 4000; turn++) {
        const cache = runners[0].consumerGroup.cluster.committedOffsets({ groupId })
        if (
          ledger.size() === 0 &&
          cache[TOPIC]?.[0]?.toString() === '0' &&
          cache[TOPIC]?.[1]?.toString() === '0'
        )
          break
        await new Promise((resolve) => setTimeout(resolve, 1))
      }
      expect(ledger.size()).toBe(0)
      for (const runner of runners) {
        expect(runner.consumerGroup.operations).toBe(ledger)
        expect(runner.consumerGroup.offsetManager.operations).toBe(ledger)
        expect(runner.consumerGroup.offsetManager.obligations).toBe(
          runner.consumerGroup.obligations
        )
        expect(runner.consumerGroup.cluster.offsetOperations({ groupId })).toBe(ledger)
      }
      expect(scripted.requests().some((request) => request.api === 'ApiVersions')).toBe(true)
      expect(scripted.requests().some((request) => request.api === 'JoinGroup')).toBe(true)
      expect(scripted.requests().some((request) => request.api === 'SyncGroup')).toBe(true)
      return {
        scripted,
        producer,
        consumers,
        runners,
        transaction,
        writes,
        ledger,
        close,
      }
    } catch (error) {
      await close().catch(() => {})
      throw error
    }
  }

  /** An independent broker read on a new factory, so a shared cache cannot answer it. */
  const freshBrokerOffset = async (scripted) => {
    const admin = new Kafka(scripted.kafkaOptions()).admin()
    try {
      await admin.connect()
      const result = await admin.fetchOffsets({ groupId, topics: [TOPIC] })
      return result[0].partitions[0].offset
    } finally {
      await admin.disconnect()
    }
  }

  // Allowed replay: no produced log records; broker commits are the independently scripted EndTxn
  // result; handler delivery can repeat only after an abort or lost outcome.
  describe.each(['tv1', 'tv2'])('Transactional offset publication on %s frames', (profile) => {
    let wire
    beforeAll(() => prepareRecordBatches())
    afterEach(async () => {
      if (wire) {
        const current = wire
        wire = undefined
        try {
          await current.close()
        } finally {
          jest.restoreAllMocks()
        }
      }
    })

    test.each([false, true])(
      'public factory cache ordering; separate Kafka=%s',
      async (separate) => {
        wire = await openWire(profile, separate)
        const committed = '9'
        wire.scripted.answer('OffsetFetch', () => ({
          groupId,
          topics: [
            {
              topic: TOPIC,
              partitions: [
                {
                  partition: 0,
                  offset: committed,
                  leaderEpoch: 3,
                  metadata: 'original',
                  errorCode: 0,
                },
              ],
            },
          ],
        }))
        await wire.transaction.sendOffsets({
          consumerGroupId: groupId,
          topics: [
            {
              topic: TOPIC,
              partitions: [
                { partition: 0, offset: committed, leaderEpoch: 3, metadata: 'original' },
              ],
            },
          ],
        })
        const held = wire.scripted.schedule.hold({ api: 'EndTxn' })
        const committing = wire.transaction.commit()
        await held.arrived()
        await wire.consumers[1].commitOffsets([{ topic: TOPIC, partition: 0, offset: '2' }])
        const consumerCluster = wire.runners[0].consumerGroup.cluster
        expect(consumerCluster.committedOffsets({ groupId })[TOPIC][0].toString()).toBe('2')
        held.release()
        await committing
        const producerWrites = wire.writes.filter(({ value }) => value.offset === committed)
        expect(producerWrites).toHaveLength(separate ? 1 : 0)
        expect(consumerCluster.committedOffsets({ groupId })[TOPIC][0].toString()).toBe('2')
        expect(await freshBrokerOffset(wire.scripted)).toBe(committed)
      }
    )

    test.each([
      'abort',
      'unknown before write',
      'unknown response lost',
      'commit',
      'disconnect commit',
    ])('%s has its own cache and broker oracle', async (outcome) => {
      wire = await openWire(profile)
      let brokerOffset = '0'
      wire.scripted.answer('OffsetFetch', () => ({
        groupId,
        topics: [
          {
            topic: TOPIC,
            partitions: [
              { partition: 0, offset: brokerOffset, leaderEpoch: -1, metadata: null, errorCode: 0 },
            ],
          },
        ],
      }))
      const wanted = [{ topic: TOPIC, partitions: [{ partition: 0, offset: '9' }] }]
      await wire.transaction.sendOffsets({ consumerGroupId: groupId, topics: wanted })
      expect(wire.writes).toHaveLength(0)
      if (outcome === 'abort') {
        await wire.transaction.abort()
      } else if (outcome.startsWith('unknown')) {
        let blocked
        if (outcome === 'unknown before write') {
          const send = Connection.prototype.send
          blocked = jest
            .spyOn(Connection.prototype, 'send')
            .mockImplementation(function (protocol) {
              if (protocol.request.apiKey === apiKeys.EndTxn)
                return Promise.reject(new KafkaJSConnectionClosedError('not transmitted'))
              return send.call(this, protocol)
            })
        } else
          wire.scripted.answer('EndTxn', () => {
            brokerOffset = '9'
            throw new KafkaJSConnectionClosedError('lost answer')
          })
        await expect(wire.transaction.commit()).rejects.toMatchObject({ retriable: true })
        expect(wire.writes).toHaveLength(0)
        expect(wire.ledger.size()).toBe(1)
        expect(await freshBrokerOffset(wire.scripted)).toBe(
          outcome === 'unknown response lost' ? '9' : '0'
        )
        blocked?.mockRestore()
        wire.scripted.answer('EndTxn', () => {
          brokerOffset = '9'
          return {}
        })
        await wire.transaction.commit()
      } else if (outcome === 'disconnect commit') {
        wire.scripted.answer('EndTxn', () => {
          throw new KafkaJSConnectionClosedError('unknown')
        })
        await expect(wire.transaction.commit()).rejects.toMatchObject({ retriable: true })
        wire.scripted.answer('EndTxn', () => {
          brokerOffset = '9'
          return {}
        })
        await wire.producer.disconnect()
      } else {
        wire.scripted.answer('EndTxn', () => {
          brokerOffset = '9'
          return {}
        })
        await wire.transaction.commit()
      }
      expect(wire.writes.filter(({ value }) => value.offset === '9')).toHaveLength(
        outcome === 'abort' ? 0 : 1
      )
      expect(wire.ledger.size()).toBe(0)
      expect(await freshBrokerOffset(wire.scripted)).toBe(outcome === 'abort' ? '0' : '9')
      if (outcome !== 'abort') {
        const trace = wire.writes.find(({ value }) => value.offset === '9').trace
        const response = trace.findLast(({ api, kind }) => api === 'EndTxn' && kind === 'response')
        const request = trace.find(
          ({ api, kind, correlationId, generation }) =>
            api === 'EndTxn' &&
            kind === 'request' &&
            correlationId === response.correlationId &&
            generation === response.generation
        )
        const offsets = trace.find(
          ({ api, kind }) => api === 'TxnOffsetCommit' && kind === 'response'
        )
        const publication = trace.findLast(({ label }) => label === 'cache write')
        expect(offsets.order).toBeLessThan(request.order)
        expect(request.order).toBeLessThan(response.order)
        expect(response.order).toBeLessThan(publication.order)
        expect(
          trace.filter(({ api, kind }) => api === 'EndTxn' && kind === 'request')
        ).toHaveLength(['unknown response lost', 'disconnect commit'].includes(outcome) ? 3 : 1)
        WireOrder.of(wire.scripted).assertOrder([
          'TxnOffsetCommit response',
          'EndTxn request',
          'EndTxn response',
          'cache write',
        ])
      }
    })

    test.each([
      ['9', '2'],
      ['2', '9'],
    ])('only call order selects %s then %s on the wire', async (a, b) => {
      wire = await openWire(profile)
      const held = wire.scripted.schedule.hold({ api: 'TxnOffsetCommit' })
      const first = wire.transaction.sendOffsets({
        consumerGroupId: groupId,
        topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: a }] }],
      })
      const second = wire.transaction.sendOffsets({
        consumerGroupId: groupId,
        topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: b }] }],
      })
      await held.arrived()
      await tick()
      expect(
        wire.scripted.requests().filter((request) => request.api === 'TxnOffsetCommit')
      ).toHaveLength(1)
      held.release()
      await Promise.all([first, second])
      await wire.transaction.commit()
      expect(
        wire.scripted
          .requests()
          .filter((request) => request.api === 'TxnOffsetCommit')
          .map((request) => request.fields.topics[0].partitions[0].offset)
      ).toEqual([a, b])
      expect(wire.writes.map(({ value }) => value.offset)).toEqual([b])
    })

    test.each(['seek', 'recreation'])(
      '%s during a held offset answer fences publication',
      async (mutation) => {
        wire = await openWire(profile)
        const held = wire.scripted.schedule.hold({ api: 'TxnOffsetCommit' })
        const input = [
          {
            topic: TOPIC,
            partitions: [{ partition: 0, offset: '9', leaderEpoch: 3, metadata: 'original' }],
          },
        ]
        const sending = wire.transaction.sendOffsets({ consumerGroupId: groupId, topics: input })
        await held.arrived()
        input[0].partitions[0].offset = '99'
        if (mutation === 'seek') {
          wire.consumers[1].seek({ topic: TOPIC, partition: 0, offset: '2' })
          await wire.runners[1].consumerGroup.seekOffsets([{ topic: TOPIC, partitions: [0] }])
        } else {
          const previous = wire.scripted.answerers.get('Metadata')
          wire.scripted.answer('Metadata', (...args) => ({
            ...previous(...args),
            topics: previous(...args).topics.map((entry) => ({
              ...entry,
              topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
            })),
          }))
          await wire.runners[1].consumerGroup.cluster.refreshMetadata()
        }
        held.release()
        await sending
        await wire.transaction.commit()
        expect(
          wire.scripted.requests().find((request) => request.api === 'TxnOffsetCommit').fields
            .topics
        ).toEqual([
          {
            topic: TOPIC,
            partitions: [{ partition: 0, offset: '9', leaderEpoch: 3, metadata: 'original' }],
          },
        ])
        expect(wire.writes.filter(({ value }) => value.offset === '9')).toHaveLength(0)
        for (let turn = 0; turn < 4000 && wire.ledger.size() !== 0; turn++)
          await new Promise((resolve) => setTimeout(resolve, 1))
        expect(wire.ledger.size()).toBe(0)
      }
    )
    test.each([false, true])(
      'unknown close forfeits publication across reconnect, consumer mutation=%s',
      async (mutate) => {
        wire = await openWire(profile)
        let brokerOffset = '0'
        wire.scripted.answer('OffsetFetch', () => ({
          groupId,
          topics: [
            {
              topic: TOPIC,
              partitions: [
                {
                  partition: 0,
                  offset: brokerOffset,
                  leaderEpoch: -1,
                  metadata: null,
                  errorCode: 0,
                },
              ],
            },
          ],
        }))
        await wire.transaction.sendOffsets({
          consumerGroupId: groupId,
          topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '9' }] }],
        })
        wire.scripted.answer('EndTxn', () => {
          brokerOffset = '9'
          throw new KafkaJSConnectionClosedError('lost answer')
        })
        await expect(wire.transaction.commit()).rejects.toMatchObject({ retriable: true })
        await wire.producer.disconnect()
        expect(wire.ledger.size()).toBe(0)
        if (mutate)
          await wire.consumers[1].commitOffsets([{ topic: TOPIC, partition: 0, offset: '2' }])
        const cache = wire.runners[0].consumerGroup.cluster.committedOffsets({ groupId })
        const retained = cache[TOPIC]?.[0]
        wire.scripted.answer('EndTxn', () => ({}))
        await wire.producer.connect()
        await wire.transaction.commit()
        expect(cache[TOPIC]?.[0]).toBe(retained)
        expect(wire.writes.filter(({ value }) => value.offset === '9')).toHaveLength(0)
        expect(await freshBrokerOffset(wire.scripted)).toBe('9')
      }
    )

    test('overlapping cells coalesce without dropping the earlier disjoint partition', async () => {
      wire = await openWire(profile)
      await wire.transaction.sendOffsets({
        consumerGroupId: groupId,
        topics: [
          {
            topic: TOPIC,
            partitions: [
              { partition: 0, offset: '9' },
              { partition: 1, offset: '7' },
            ],
          },
        ],
      })
      await wire.transaction.sendOffsets({
        consumerGroupId: groupId,
        topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '2' }] }],
      })
      await wire.transaction.commit()
      expect(
        wire.writes.map(({ value: { partition, offset } }) => ({ partition, offset }))
      ).toEqual([
        { partition: 0, offset: '2' },
        { partition: 1, offset: '7' },
      ])
    })

    test('an immutable snapshot and its fence precede the group coordinator lookup', async () => {
      wire = await openWire(profile)
      const lookup = wire.scripted.schedule.hold({
        api: 'FindCoordinator',
        match: (record) => record.fields.coordinatorKeys.includes(groupId),
      })
      const input = [
        {
          topic: TOPIC,
          partitions: [{ partition: 0, offset: '9', leaderEpoch: 3, metadata: 'original' }],
        },
      ]
      const sending = wire.transaction.sendOffsets({ consumerGroupId: groupId, topics: input })
      input[0].partitions[0].offset = '99'
      input[0].partitions[0].leaderEpoch = 77
      input[0].partitions[0].metadata = 'changed'
      await lookup.arrived()
      await wire.consumers[1].commitOffsets([{ topic: TOPIC, partition: 0, offset: '2' }])
      lookup.release()
      await sending
      await wire.transaction.commit()
      expect(
        wire.scripted.requests().find((record) => record.api === 'TxnOffsetCommit').fields.topics
      ).toEqual([
        {
          topic: TOPIC,
          partitions: [{ partition: 0, offset: '9', leaderEpoch: 3, metadata: 'original' }],
        },
      ])
      expect(wire.writes.filter(({ value }) => value.offset === '9')).toHaveLength(0)
    })

    test('an earlier retried call owns the chain before the later call transmits', async () => {
      wire = await openWire(profile)
      const held = wire.scripted.schedule.hold({ api: 'TxnOffsetCommit' })
      const send = (offset) =>
        wire.transaction.sendOffsets({
          consumerGroupId: groupId,
          topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset }] }],
        })
      const first = send('9')
      const second = send('2')
      await held.arrived()
      expect(
        wire.scripted.requests().filter((record) => record.api === 'TxnOffsetCommit')
      ).toHaveLength(1)
      held.release({ topics: [{ topic: TOPIC, partitions: [{ partition: 0, errorCode: 14 }] }] })
      await Promise.all([first, second])
      expect(
        wire.scripted
          .requests()
          .filter((record) => record.api === 'TxnOffsetCommit')
          .map((record) => record.fields.topics[0].partitions[0].offset)
      ).toEqual(['9', '9', '2'])
      await wire.transaction.commit()
      expect(wire.writes.map(({ value }) => value.offset)).toEqual(['2'])
    })

    test.each([[[]], [[{ topic: 'test-topic', partitions: [] }]]])(
      'empty input retains version-specific wire behavior without staging',
      async (empty) => {
        wire = await openWire(profile)
        const before = wire.scripted.requests().length
        await wire.transaction.sendOffsets({ consumerGroupId: groupId, topics: empty })
        expect(wire.ledger.size()).toBe(0)
        const requests = wire.scripted.requests().slice(before)
        expect(requests.filter((record) => record.api === 'AddOffsetsToTxn')).toHaveLength(
          profile === 'tv1' ? 1 : 0
        )
        expect(requests.filter((record) => record.api === 'TxnOffsetCommit')).toHaveLength(
          profile === 'tv1' ? 1 : 0
        )
        await wire.transaction.commit()
        expect(wire.ledger.size()).toBe(0)
        expect(wire.writes).toHaveLength(0)
      }
    )

    test.each([90, 30])(
      'failed offsets code %s retain or release prior fences according to state',
      async (code) => {
        wire = await openWire(profile)
        await wire.transaction.sendOffsets({
          consumerGroupId: groupId,
          topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '9' }] }],
        })
        wire.scripted.answer('TxnOffsetCommit', () => ({
          topics: [{ topic: TOPIC, partitions: [{ partition: 1, errorCode: code }] }],
        }))
        await expect(
          wire.transaction.sendOffsets({
            consumerGroupId: groupId,
            topics: [{ topic: TOPIC, partitions: [{ partition: 1, offset: '2' }] }],
          })
        ).rejects.toThrow()
        expect(wire.ledger.size()).toBe(code === 90 ? 0 : 1)
        if (code === 30) await wire.transaction.abort()
        expect(wire.ledger.size()).toBe(0)
        expect(wire.writes).toHaveLength(0)
      }
    )
  })
}
