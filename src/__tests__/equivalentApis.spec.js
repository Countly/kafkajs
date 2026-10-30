const { Kafka, CompressionTypes } = require('../../index')
const createProducer = require('../producer')
const { createFakeCluster } = require('../producer/__tests__/fixtures/fakeCluster')
const { createLogger, LEVELS } = require('../loggers')
const {
  createScriptedCluster,
  groupScript,
  producerScript,
} = require('../../testHelpers/scriptedCluster')
const { prepareRecordBatches, TOPIC } = require('../consumer/__tests__/fixtures/fetchHarness')
const RecordBatch = require('../protocol/recordBatch/v0')
const Record = require('../protocol/recordBatch/record/v0')
const Decoder = require('../protocol/decoder')
const { readRequestHeader } = require('../../testHelpers/fakeSocket')
const nodes = [{ id: 0, host: 'node0', port: 9092 }]
const retry = { retries: 0 }
/** Creates a silent test logger. @returns {object} logger; @throws Never. */
const logger = () => createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} })
/** Advances one event-loop turn. @returns {Promise<void>} completion; @throws Never. */
const tick = () => new Promise((resolve) => setImmediate(resolve))
/** Waits for a predicate with a finite bound. @param {Function} predicate @returns {Promise<void>} @throws {Error} when the predicate never holds. */
const until = async (predicate) => {
  for (let i = 0; i < 1000 && !predicate(); i++)
    await new Promise((resolve) => setTimeout(resolve, 5))
  expect(predicate()).toBe(true)
}
let resources
let gates
beforeAll(prepareRecordBatches)
beforeEach(() => {
  resources = []
  gates = []
})
afterEach(async () => {
  const failures = []
  for (const gate of gates) {
    try {
      if (gate.settle)
        gate.release({ partitions: [{ partition: 0, errorCode: 0, records: false }] })
    } catch (error) {
      failures.push(error)
    }
  }
  for (const { client, cluster } of resources.reverse()) {
    try {
      await client.disconnect()
    } catch (error) {
      failures.push(error)
    } finally {
      try {
        if (cluster.assertClean) cluster.assertClean()
      } catch (error) {
        failures.push(error)
      }
    }
  }
  if (failures.length) throw new AggregateError(failures, 'client teardown failed')
})

/** Registers client teardown. @param {object} client @param {object} cluster @returns {object} client @throws Never. */
const use = (client, cluster) => {
  resources.push({ client, cluster })
  return client
}
/** Tracks a held seam call for cleanup. @param {object} cluster @param {object} selector @returns {object} gate @throws {Error} for an invalid selector. */
const hold = (cluster, selector) => {
  const gate = cluster.schedule.hold(selector)
  gates.push(gate)
  return gate
}
/** Builds a public consumer over the seam. @param {object} cluster @returns {object} consumer @throws {Error} for invalid client options. */
const makeConsumer = (cluster) =>
  use(
    new Kafka(cluster.kafkaOptions({ retry })).consumer({
      groupId: 'equivalent',
      retry,
      maxWaitTimeInMs: 20,
    }),
    cluster
  )
/** Starts one subscribed consumer. @param {object} consumer @param {Function} eachMessage @returns {Promise<void>} @throws {Error} on a client setup failure. */
const start = async (consumer, eachMessage) => {
  await consumer.connect()
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
  await consumer.run({ autoCommit: false, eachMessage })
}
it('R1§A1:send', async () => {
  const cluster = createFakeCluster({ partitions: 2 })
  const producer = use(
    createProducer({ cluster, logger: logger(), idempotent: false, linger: 0, retry }),
    cluster
  )
  await producer.connect()
  const metadata = await producer.sendBatch({
    topicMessages: [
      {
        topic: 'z',
        messages: [
          { partition: 1, value: 'z1' },
          { partition: 0, value: 'z0' },
        ],
      },
      {
        topic: 'a',
        messages: [
          { partition: 1, value: 'a1' },
          { partition: 0, value: 'a0' },
        ],
      },
    ],
  })
  expect(metadata.map(({ topicName, partition }) => [topicName, partition])).toEqual([
    ['a', 0],
    ['a', 1],
    ['z', 0],
    ['z', 1],
  ])
})
it('R1§A1:initTransactions', async () => {
  const cluster = createFakeCluster()
  const producer = use(
    createProducer({
      cluster,
      logger: logger(),
      transactionalId: 'implicit',
      retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 1 },
    }),
    cluster
  )
  await producer.connect()
  const transaction = await producer.transaction()
  expect(
    cluster.broker.initProducerId.mock.calls.filter(
      ([request]) => request.transactionalId === 'implicit'
    )
  ).toHaveLength(1)
  expect(transaction.isActive()).toBe(true)
  await transaction.send({ topic: 't', messages: [{ value: 'v' }] })
  expect(
    cluster.broker.initProducerId.mock.calls.filter(
      ([request]) => request.transactionalId === 'implicit'
    )
  ).toHaveLength(1)
  expect(
    cluster.broker.initProducerId.mock.invocationCallOrder[
      cluster.broker.initProducerId.mock.calls.findIndex(
        ([request]) => request.transactionalId === 'implicit'
      )
    ]
  ).toBeLessThan(cluster.broker.produce.mock.invocationCallOrder[0])
  await transaction.abort()
})
it('R1§B1:poll', async () => {
  const cluster = groupScript(createScriptedCluster({ nodes }))
  const batches = []
  for (let i = 0; i < 3; i++)
    batches.push(
      (
        await RecordBatch.RecordBatch({
          firstOffset: String(i),
          firstTimestamp: 1700000000000,
          maxTimestamp: 1700000000000,
          records: [Record({ value: Buffer.from(`v${i}`) })],
        })
      ).buffer
    )
  let answered = false
  cluster.answer('Fetch', async ({ maxWaitMs }) => {
    if (answered) await new Promise((resolve) => setTimeout(resolve, maxWaitMs))
    const records = answered ? false : Buffer.concat(batches)
    answered = true
    return { partitions: [{ partition: 0, errorCode: 0, highWatermark: '3', records }] }
  })
  const consumer = makeConsumer(cluster)
  const seen = []
  await start(consumer, async ({ partition, message }) =>
    seen.push([partition, message.offset, message.value.toString()])
  )
  await until(() => seen.length === 3)
  await consumer.stop()
  expect(seen).toEqual([
    [0, '0', 'v0'],
    [0, '1', 'v1'],
    [0, '2', 'v2'],
  ])
  expect(consumer.poll).toBeUndefined()
})
it('R1§B1:wakeup', async () => {
  const cluster = groupScript(createScriptedCluster({ nodes }))
  const consumer = makeConsumer(cluster)
  const seen = []
  const next = hold(cluster, {
    api: 'Fetch',
    match: () => cluster.requests().filter((r) => r.api === 'Fetch').length === 2,
  })
  await start(consumer, async ({ message }) => seen.push(message.offset))
  await next.arrived()
  expect(seen).toEqual(['0'])
  const nextRecord = (
    await RecordBatch.RecordBatch({
      firstOffset: '1',
      firstTimestamp: 1700000000000,
      maxTimestamp: 1700000000000,
      records: [Record({ value: Buffer.from('after-stop') })],
    })
  ).buffer
  const stop = consumer.stop()
  next.release({
    partitions: [{ partition: 0, errorCode: 0, highWatermark: '2', records: nextRecord }],
  })
  await stop
  const fetched = cluster.requests().filter((r) => r.api === 'Fetch').length
  await tick()
  await tick()
  expect(seen).toEqual(['0'])
  expect(cluster.requests().filter((r) => r.api === 'Fetch')).toHaveLength(fetched)
})
it('R1§B1:seekToBeginning', async () => {
  for (const autoCommit of [false, true]) {
    for (const fromBeginning of [false, true]) {
      const cluster = groupScript(createScriptedCluster({ nodes }), {
        batches: 0,
        committed: { 0: '99' },
      })
      cluster.answer('ListOffsets', ({ topics }) => ({
        topics: topics.map(({ name, partitions }) => ({
          name,
          partitions: partitions.map(({ partitionIndex, timestamp }) => ({
            partitionIndex,
            errorCode: 0,
            timestamp: '-1',
            offset: timestamp === '-2' ? '11' : '83',
            leaderEpoch: 5,
          })),
        })),
      }))
      let held = hold(cluster, { api: 'Fetch' })
      const consumer = makeConsumer(cluster)
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC, fromBeginning })
      await consumer.run({ autoCommit, eachMessage: async () => {} })
      await held.arrived()
      for (const sentinel of ['-2', '-1']) {
        const next = hold(cluster, { api: 'Fetch' })
        const before = cluster.requests().filter((r) => r.api === 'ListOffsets').length
        consumer.seek({ topic: TOPIC, partition: 0, offset: sentinel })
        held.release({ partitions: [{ partition: 0, errorCode: 0, records: false }] })
        await next.arrived()
        const requests = cluster
          .requests()
          .filter((r) => r.api === 'ListOffsets')
          .slice(before)
        expect(requests).toHaveLength(1)
        expect(requests[0].fields.topics).toEqual([
          {
            name: TOPIC,
            partitions: [expect.objectContaining({ partitionIndex: 0, timestamp: sentinel })],
          },
        ])
        expect(
          cluster
            .requests()
            .filter((r) => r.api === 'Fetch')
            .at(-1).fields.topics[0].partitions[0].fetchOffset
        ).toBe(sentinel === '-2' ? '11' : '83')
        held = next
      }
      const stop = consumer.stop()
      held.release({ partitions: [{ partition: 0, errorCode: 0, records: false }] })
      await stop
    }
  }

  const cluster = groupScript(createScriptedCluster({ nodes }), {
    batches: 0,
    partitions: 2,
    committed: { 0: '99', 1: '99' },
  })
  cluster.answer('ListOffsets', ({ topics }) => ({
    topics: topics.map(({ name, partitions }) => ({
      name,
      partitions: partitions.map(({ partitionIndex, timestamp }) => ({
        partitionIndex,
        errorCode: 0,
        timestamp: '-1',
        offset: timestamp === '-2' ? '11' : '83',
        leaderEpoch: 5,
      })),
    })),
  }))
  const first = hold(cluster, { api: 'Fetch' })
  const consumer = makeConsumer(cluster)
  await start(consumer, async () => {})
  await first.arrived()
  const next = hold(cluster, { api: 'Fetch' })
  const before = cluster.requests().filter((r) => r.api === 'ListOffsets').length
  consumer.seek({ topic: TOPIC, partition: 0, offset: '-2' })
  consumer.seek({ topic: TOPIC, partition: 1, offset: '-1' })
  first.release({
    partitions: [0, 1].map((partition) => ({ partition, errorCode: 0, records: false })),
  })
  await next.arrived()
  const requests = cluster
    .requests()
    .filter((r) => r.api === 'ListOffsets')
    .slice(before)
  expect(requests).toHaveLength(1)
  expect(
    requests[0].fields.topics[0].partitions.map(({ partitionIndex, timestamp }) => [
      partitionIndex,
      timestamp,
    ])
  ).toEqual([
    [0, '-2'],
    [1, '-1'],
  ])
  expect(
    cluster
      .requests()
      .filter((r) => r.api === 'Fetch')
      .at(-1)
      .fields.topics[0].partitions.map(({ partition, fetchOffset }) => [partition, fetchOffset])
      .sort(([a], [b]) => a - b)
  ).toEqual([
    [0, '11'],
    [1, '83'],
  ])
  const stop = consumer.stop()
  next.release({
    partitions: [0, 1].map((partition) => ({ partition, errorCode: 0, records: false })),
  })
  await stop
})
it('R1§B1:listTopics', async () => {
  const cluster = groupScript(createScriptedCluster({ nodes }), { partitions: 2 })
  const admin = use(new Kafka(cluster.kafkaOptions({ retry })).admin({ retry }), cluster)
  await admin.connect()
  expect(await admin.listTopics()).toEqual([TOPIC])
  const { topics } = await admin.fetchTopicMetadata({ topics: [TOPIC] })
  expect(topics.map((t) => [t.name, t.partitions.map((p) => p.partitionId)])).toEqual([
    [TOPIC, [0, 1]],
  ])
})
/**
 * Builds one broker batch that carries several records.
 * @param {number} count how many records the batch holds, at offsets 0 to count - 1
 * @returns {Promise<Buffer>} the encoded batch; the key of each record is `k<offset>`
 * @throws Never.
 */
const multiRecordBatch = async (count) =>
  Buffer.from(
    (
      await RecordBatch.RecordBatch({
        firstOffset: '0',
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        records: Array.from({ length: count }, (_, i) =>
          Record({ offsetDelta: i, key: Buffer.from(`k${i}`), value: Buffer.from(`v${i}`) })
        ),
      })
    ).buffer
  )
it('R1§B1:currentLag', async () => {
  const highWatermark = 100 // the fetch harness's fixed high watermark
  let confirmed = '-1'
  const records = await multiRecordBatch(3)
  let answered = false
  const cluster = groupScript(createScriptedCluster({ nodes }))
    .answer('Fetch', async ({ maxWaitMs }) => {
      if (answered) await new Promise((resolve) => setTimeout(resolve, maxWaitMs))
      const batch = answered ? false : records
      answered = true
      return {
        partitions: [
          { partition: 0, errorCode: 0, highWatermark: String(highWatermark), records: batch },
        ],
      }
    })
    .answer('OffsetCommit', ({ topics }) => {
      confirmed = topics[0].partitions[0].offset
      return {
        topics: topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({ partition, errorCode: 0 })),
        })),
      }
    })
    .answer('OffsetFetch', ({ groupId, topics }) => ({
      groupId,
      topics: topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({
          partition,
          offset: confirmed,
          leaderEpoch: -1,
          metadata: null,
          errorCode: 0,
        })),
      })),
    }))
    .answer('ListOffsets', ({ topics }) => ({
      topics: topics.map(({ name, partitions }) => ({
        name,
        partitions: partitions.map(({ partitionIndex, timestamp }) => ({
          partitionIndex,
          errorCode: 0,
          timestamp: '-1',
          offset: timestamp === '-2' ? '0' : String(highWatermark),
          leaderEpoch: 5,
        })),
      })),
    }))
  const consumer = makeConsumer(cluster)
  const admin = use(new Kafka(cluster.kafkaOptions({ retry })).admin({ retry }), cluster)
  const seen = []
  await consumer.connect()
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
  await consumer.run({
    autoCommit: false,
    eachBatch: async ({ batch, resolveOffset }) => {
      seen.push({ lag: batch.offsetLag(), last: batch.lastOffset() })
      for (const message of batch.messages) resolveOffset(message.offset)
    },
  })
  await until(() => seen.length === 1)
  const { lag, last } = seen[0]
  const next = String(Number(last) + 1)
  await consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: next }])
  await admin.connect()
  const [{ partitions }] = await admin.fetchOffsets({ groupId: 'equivalent', topics: [TOPIC] })
  const [{ offset: committed }] = partitions
  const [{ high }] = await admin.fetchTopicOffsets(TOPIC)
  expect(committed).toBe(next)
  expect(Number(high) - Number(committed)).toBe(Number(lag))
  expect(Number(high) - Number(last)).not.toBe(Number(lag))
})
it('R1§B2:max.poll.records', async () => {
  const records = await multiRecordBatch(4)
  let answered = false
  const cluster = groupScript(createScriptedCluster({ nodes })).answer(
    'Fetch',
    async ({ maxWaitMs }) => {
      if (answered) await new Promise((resolve) => setTimeout(resolve, maxWaitMs))
      const batch = answered ? false : records
      answered = true
      return { partitions: [{ partition: 0, errorCode: 0, highWatermark: '4', records: batch }] }
    }
  )
  const consumer = makeConsumer(cluster)
  const invocations = []
  await start(consumer, async ({ message }) => {
    invocations.push([message.key.toString(), message.offset])
  })
  await until(() => invocations.length === 4)
  await tick()
  expect(invocations).toEqual([
    ['k0', '0'],
    ['k1', '1'],
    ['k2', '2'],
    ['k3', '3'],
  ])
  expect(Object.keys(consumer)).not.toContain('maxPollRecords')
})
it('R1§A2:compression.type', async () => {
  const cluster = producerScript(createScriptedCluster({ nodes }))
  const producer = use(
    new Kafka(cluster.kafkaOptions({ retry })).producer({ retry, linger: 0 }),
    cluster
  )
  await producer.connect()
  const send = (compression) =>
    producer.send({
      topic: TOPIC,
      messages: [{ value: 'hello' }],
      ...(compression === undefined ? {} : { compression }),
    })
  await send()
  await send(CompressionTypes.GZIP)
  await send()
  const attributes = cluster
    .sockets()
    .flatMap((socket) => socket.writes)
    .flatMap((frame) => {
      const h = readRequestHeader(frame)
      if (h.apiKey !== 0) return []
      const d = new Decoder(frame.subarray(h.byteLength))
      d.readUVarIntString()
      d.readInt16()
      d.readInt32()
      const attrs = d
        .readUVarIntArray((t) => {
          t.readUuid()
          const parts = t.readUVarIntArray((p) => {
            p.readInt32()
            const bytes = p.readUVarIntBytes()
            p.readTaggedFields()
            return bytes.readInt16BE(21) & 7
          })
          t.readTaggedFields()
          return parts
        })
        .flat()
      d.readTaggedFields()
      expect(d.canReadBytes(1)).toBe(false)
      return attrs
    })
  expect(attributes).toEqual([0, 1, 0])
})

/**
 * Attempts every cleanup action and unconditionally restores global test state.
 * @param {Error|undefined} primary original test failure
 * @param {Function[]} actions independent resource cleanup attempts
 * @param {Function} [restore] final synchronous restoration, even after cleanup failures
 * @returns {Promise<void>} after all actions and restoration were attempted
 * @throws {Error|AggregateError} original with cleanupErrors, or cleanup failures alone
 */
const finish = async (primary, actions, restore = () => {}) => {
  const failures = []
  try {
    for (const action of actions) {
      try {
        await action()
      } catch (error) {
        failures.push(error)
      }
    }
  } finally {
    try {
      restore()
    } catch (error) {
      failures.push(error)
    }
  }
  if (primary) {
    if (failures.length) primary.cleanupErrors = failures
    throw primary
  }
  if (failures.length) throw new AggregateError(failures, 'Resource cleanup failed')
}

it('R1§A1:partitionsFor', async () => {
  const cluster = producerScript(createScriptedCluster({ nodes }), { partitions: 2 })
  const kafka = new Kafka(cluster.kafkaOptions({ retry }))
  const admin = use(kafka.admin(), cluster)
  await admin.connect()
  const metadata = await admin.fetchTopicMetadata({ topics: [TOPIC] })
  expect(metadata.topics).toEqual([
    expect.objectContaining({
      name: TOPIC,
      partitions: [
        expect.objectContaining({
          partitionId: 0,
          leader: 0,
          replicas: [0],
          isr: [0],
          offlineReplicas: [],
        }),
        expect.objectContaining({
          partitionId: 1,
          leader: 0,
          replicas: [0],
          isr: [0],
          offlineReplicas: [],
        }),
      ],
    }),
  ])
  expect(cluster.requests().filter((r) => r.api === 'Produce')).toHaveLength(0)
})

it('R2§2:ManualFlushing', async () => {
  const clients = []
  const work = []
  let primary
  /** Watches pending work before any assertion can interrupt the case.
   * @param {Promise<*>} promise operation
   * @returns {object} settlement record
   * @throws Never.
   */
  const watch = (promise) => {
    const result = { status: 'pending' }
    promise.then(
      () => {
        result.status = 'fulfilled'
      },
      () => {
        result.status = 'rejected'
      }
    )
    work.push({ promise, result })
    return result
  }
  /** Pumps the fake clock without waiting on a held promise.
   * @param {Function} ready settlement predicate
   * @returns {Promise<void>} after a finite number of turns
   * @throws {Error} when work cannot settle
   */
  const pump = async (ready) => {
    for (let i = 0; i < 200 && !ready(); i++) {
      await jest.advanceTimersByTimeAsync(0)
      await tick()
    }
    expect(ready()).toBe(true)
  }
  try {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] })
    for (const batchSize of [1, 1048576]) {
      const cluster = createFakeCluster()
      const producer = createProducer({
        cluster,
        logger: logger(),
        linger: 3600000,
        deliveryTimeout: 3700000,
        batchSize,
        idempotent: true,
      })
      clients.push({ producer, cluster })
      expect(typeof producer.flush).toBe('function')
      await producer.connect()
      const pending = producer.send({
        topic: 'recipe',
        messages: [{ value: 'v', timestamp: 1000 }],
      })
      watch(pending)
      for (let i = 0; i < 20; i++) {
        await jest.advanceTimersByTimeAsync(0)
        await tick()
      }
      expect(cluster.broker.produce.mock.calls).toHaveLength(batchSize === 1 ? 1 : 0)
      const flushed = producer.flush()
      const done = watch(flushed)
      await pump(() => done.status !== 'pending')
      await flushed
      await pending
      expect(cluster.broker.produce.mock.calls).toHaveLength(1)
    }
  } catch (error) {
    primary = error
  } finally {
    await finish(
      primary,
      [
        ...clients.map(({ producer }) => async () => {
          const close = producer.disconnect()
          const closed = watch(close)
          await pump(() => closed.status !== 'pending')
          await close
        }),
        async () => {
          await pump(() => work.every(({ result }) => result.status !== 'pending'))
          await Promise.allSettled(work.map(({ promise }) => promise))
        },
        ...clients.map(({ cluster }) => () => {
          if (cluster.assertClean) cluster.assertClean()
        }),
      ],
      () => jest.useRealTimers()
    )
  }
})
it('R1§B1:seek', async () => {
  const cluster = groupScript(createScriptedCluster({ nodes }), {
    batches: 0,
    committed: { 0: '99' },
  })
  const consumer = makeConsumer(cluster)
  expect(() => consumer.seek({ topic: TOPIC, partition: 0, offset: '5' })).toThrow(
    'Consumer group was not initialized, consumer#run must be called first'
  )
  await consumer.connect()
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
  const firstFetch = hold(cluster, { api: 'Fetch' })
  const hookGate = {}
  hookGate.promise = new Promise((resolve) => {
    hookGate.release = resolve
  })
  let entered = false
  const running = consumer.run({
    autoCommit: false,
    eachMessage: async () => {},
    rebalanceListener: {
      onPartitionsAssigned: async () => {
        consumer.seek({ topic: TOPIC, partition: 0, offset: '42' })
        entered = true
        await hookGate.promise
      },
    },
  })
  await until(() => entered)
  await tick()
  expect(cluster.requests().filter((r) => r.api === 'Fetch')).toHaveLength(0)
  hookGate.release()
  await running
  await firstFetch.arrived()
  expect(
    cluster.requests().filter((r) => r.api === 'Fetch')[0].fields.topics[0].partitions[0]
      .fetchOffset
  ).toBe('42')
  const stop = consumer.stop()
  firstFetch.release({ partitions: [{ partition: 0, errorCode: 0, records: false }] })
  await stop
})
it('R2§3:OnOffsetsFetched', async () => {
  const cluster = groupScript(createScriptedCluster({ nodes }), {
    batches: 0,
    partitions: 2,
    committed: { 0: '99', 1: '77' },
  })
  const consumer = makeConsumer(cluster)
  await consumer.connect()
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
  const firstFetch = hold(cluster, { api: 'Fetch' })
  const hookGate = {}
  hookGate.promise = new Promise((resolve) => {
    hookGate.release = resolve
  })
  let adjusted
  let positions
  let entered = false
  const running = consumer.run({
    autoCommit: false,
    eachMessage: async () => {},
    rebalanceListener: {
      onPartitionsAssigned: async () => {
        const read = await consumer.committed(
          [0, 1].map((partition) => ({ topic: TOPIC, partition }))
        )
        adjusted = read.map(({ partition, offset }) => [partition, offset])
        consumer.seek({ topic: TOPIC, partition: 0, offset: String(Number(read[0].offset) - 10) })
        consumer.seek({ topic: TOPIC, partition: 1, offset: String(Number(read[1].offset) - 20) })
        positions = [
          await consumer.position({ topic: TOPIC, partition: 0 }),
          await consumer.position({ topic: TOPIC, partition: 1 }),
        ]
        entered = true
        await hookGate.promise
      },
    },
  })
  await until(() => entered)
  await tick()
  expect(cluster.requests().filter((r) => r.api === 'Fetch')).toHaveLength(0)
  hookGate.release()
  await running
  await firstFetch.arrived()
  const first = cluster.requests().filter((r) => r.api === 'Fetch')[0].fields.topics[0].partitions
  expect(adjusted).toEqual([
    [0, '99'],
    [1, '77'],
  ])
  expect(positions).toEqual(['89', '57'])
  expect(
    first.map(({ partition, fetchOffset }) => [partition, fetchOffset]).sort(([a], [b]) => a - b)
  ).toEqual([
    [0, '89'],
    [1, '57'],
  ])
  const stop = consumer.stop()
  firstFetch.release({
    partitions: [0, 1].map((partition) => ({ partition, errorCode: 0, records: false })),
  })
  await stop
})
