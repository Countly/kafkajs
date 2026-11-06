const { Kafka, CompressionTypes } = require('../../index')
const createProducer = require('../producer')
const { createFakeCluster } = require('../producer/__tests__/fixtures/fakeCluster')
const { createLogger, LEVELS } = require('../loggers')
const {
  createScriptedCluster,
  groupScript,
  producerScript,
} = require('../../testHelpers/scriptedCluster')
const {
  prepareRecordBatches,
  TOPIC,
  TOPIC_ID,
} = require('../consumer/__tests__/fixtures/fetchHarness')
const RecordBatch = require('../protocol/recordBatch/v0')
const Record = require('../protocol/recordBatch/record/v0')
const Decoder = require('../protocol/decoder')
const { readRequestHeader } = require('../../testHelpers/fakeSocket')
const fs = require('fs')
const path = require('path')
const vm = require('vm')
const Broker = require('../broker')
const ConnectionPool = require('../network/connectionPool')
const adminFactory = require('../admin')
const apiKeys = require('../protocol/requests/apiKeys')
const expectedWire = require('../../testHelpers/wire/expected')
const { createGroupApiBroker } = require('../broker/__tests__/fixtures/groupApiHarness')
const {
  A,
  B,
  acquire,
  ackAnswer,
  acquired,
  deferred,
  fetchAnswer,
  flush,
  part,
  shareClient,
  until: reach,
} = require('../../testHelpers/shareHarness')
const {
  KafkaJSDeleteGroupsError,
  KafkaJSNonRetriableError,
  KafkaJSShareAcknowledgementError,
} = require('../errors')
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
/** @type {Function[]} releases of the answer holds a test made, run before any client closes */
const releases = []
afterEach(async () => {
  const failures = []
  for (const release of releases.splice(0)) release()
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
/**
 * Holds the answer of one scripted API after the broker received the request.
 * @param {object} cluster scripted cluster
 * @param {string} api API name
 * @returns {{ hits: number, release: () => void }} the hold; released at test end
 */
const holdAnswer = (cluster, api) => {
  const original = cluster.answerers.get(api)
  let open = false
  let wake
  const opened = new Promise((resolve) => {
    wake = resolve
  })
  const held = {
    hits: 0,
    release: () => {
      open = true
      wake()
    },
  }
  cluster.answer(api, async (fields, record) => {
    held.hits += 1
    if (!open) await opened
    return original(fields, record)
  })
  releases.push(held.release)
  return held
}
/** @param {number} offset first and only offset @returns {Promise<Buffer>} a one-record batch */
const oneRecordBatch = async (offset) =>
  Buffer.from(
    (
      await RecordBatch.RecordBatch({
        firstOffset: String(offset),
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        records: [Record({ value: Buffer.from(`v${offset}`) })],
      })
    ).buffer
  )
it('R1§B2:default.api.timeout.ms', async () => {
  const bounded = { retries: 0, maxTotalTime: 250 }
  const cell = [{ topic: TOPIC, partition: 0 }]
  const build = (retryOptions) => {
    const cluster = groupScript(createScriptedCluster({ nodes }))
    const consumer = use(
      new Kafka(cluster.kafkaOptions({ retry: retryOptions })).consumer({
        groupId: 'equivalent',
        retry: retryOptions,
        maxWaitTimeInMs: 20,
      }),
      cluster
    )
    return { cluster, consumer }
  }
  const frames = (cluster, api) => cluster.requests().filter((r) => r.api === api).length

  const lookup = build(bounded)
  await lookup.consumer.connect()
  const heldLookup = holdAnswer(lookup.cluster, 'FindCoordinator')
  await expect(lookup.consumer.committed(cell)).rejects.toMatchObject({
    name: 'KafkaJSDeliveryTimeout',
  })
  expect(heldLookup.hits).toBeGreaterThan(0)
  heldLookup.release()
  await tick()
  expect(frames(lookup.cluster, 'OffsetFetch')).toBe(0)

  const response = build(bounded)
  await response.consumer.connect()
  const heldResponse = holdAnswer(response.cluster, 'OffsetFetch')
  await expect(response.consumer.committed(cell)).rejects.toMatchObject({
    name: 'KafkaJSDeliveryTimeout',
  })
  expect(heldResponse.hits).toBe(1)

  const commit = build(bounded)
  await start(commit.consumer, async () => {})
  await until(() => frames(commit.cluster, 'OffsetFetch') > 0)
  const heldCommit = holdAnswer(commit.cluster, 'OffsetCommit')
  await expect(
    commit.consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '1' }])
  ).rejects.toMatchObject({ name: 'KafkaJSDeliveryTimeout' })
  expect(heldCommit.hits).toBe(1)

  const unset = build({ retries: 0 })
  await unset.consumer.connect()
  const heldUnset = holdAnswer(unset.cluster, 'OffsetFetch')
  let settled = false
  const reading = unset.consumer.committed(cell).then((rows) => {
    settled = true
    return rows
  })
  await until(() => heldUnset.hits === 1)
  await new Promise((resolve) => setTimeout(resolve, 400))
  expect(settled).toBe(false)
  heldUnset.release()
  await expect(reading).resolves.toHaveLength(1)
})
it('R1§B2:enable.auto.commit', async () => {
  const first = await oneRecordBatch(0)
  const second = await oneRecordBatch(1)
  const frames = (cluster) => cluster.requests().filter((r) => r.api === 'OffsetCommit')
  /** Runs one consumer over a batch, an idle gap and a second batch. */
  const cadence = async (runOptions) => {
    const cluster = groupScript(createScriptedCluster({ nodes }))
    let fetches = 0
    let release
    const gap = new Promise((resolve) => {
      release = resolve
    })
    cluster.answer('Fetch', async ({ maxWaitMs }) => {
      fetches += 1
      if (fetches === 1)
        return { partitions: [{ partition: 0, errorCode: 0, highWatermark: '1', records: first }] }
      if (fetches === 2) {
        await gap
        return { partitions: [{ partition: 0, errorCode: 0, highWatermark: '2', records: second }] }
      }
      await new Promise((resolve) => setTimeout(resolve, maxWaitMs))
      return { partitions: [{ partition: 0, errorCode: 0, records: false }] }
    })
    const consumer = use(
      new Kafka(cluster.kafkaOptions({ retry })).consumer({
        groupId: 'equivalent',
        retry,
        maxWaitTimeInMs: 20,
      }),
      cluster
    )
    const seen = []
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await consumer.run({
      eachMessage: async ({ message }) => seen.push(message.offset),
      ...runOptions,
    })
    await until(() => seen.length === 1)
    const afterBatch = frames(cluster).length
    await new Promise((resolve) => setTimeout(resolve, 450))
    const afterIdle = frames(cluster).length
    release()
    await until(() => seen.length === 2)
    await tick()
    return { afterBatch, afterIdle, final: frames(cluster).length, cluster }
  }

  const interval = await cadence({ autoCommit: true, autoCommitInterval: 200 })
  expect(interval.afterBatch).toBe(0)
  expect(interval.afterIdle).toBe(0)
  expect(interval.final).toBe(1)

  const threshold = await cadence({ autoCommit: true, autoCommitThreshold: 1 })
  expect(threshold.afterBatch).toBe(1)

  const both = await cadence({
    autoCommit: true,
    autoCommitInterval: 60000,
    autoCommitThreshold: 2,
  })
  expect(both.afterBatch).toBe(0)
  expect(both.final).toBe(1)

  const neither = await cadence({ autoCommit: true })
  expect(neither.afterBatch).toBe(1)
  expect(neither.final).toBe(2)

  const off = await cadence({ autoCommit: false })
  expect(off.final).toBe(0)
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
it('R2§3:MaxConcurrentFetches', async () => {
  const threeNodes = [0, 1, 2].map((id) => ({ id, host: `node${id}`, port: 9092 + id }))
  const cluster = groupScript(createScriptedCluster({ nodes: threeNodes }), {
    partitions: 3,
    batches: 0,
    committed: { 0: '0', 1: '0', 2: '0' },
  })
  cluster.answer('Metadata', () => ({
    brokers: threeNodes.map(({ id, host, port }) => ({ nodeId: id, host, port, rack: null })),
    controllerId: 0,
    topics: [
      {
        errorCode: 0,
        name: TOPIC,
        topicId: TOPIC_ID,
        isInternal: false,
        topicAuthorizedOperations: -2147483648,
        partitions: [0, 1, 2].map((partitionIndex) => ({
          errorCode: 0,
          partitionIndex,
          leaderId: partitionIndex,
          leaderEpoch: 5,
          replicas: [partitionIndex],
          isr: [partitionIndex],
          offlineReplicas: [],
        })),
      },
    ],
  }))
  const fetchGates = threeNodes.map(({ id }) => hold(cluster, { api: 'Fetch', node: id }))
  for (const [node, gate] of fetchGates.entries()) {
    releases.push(() => {
      if (gate.settle)
        gate.release({ partitions: [{ partition: node, errorCode: 0, records: false }] })
    })
  }
  const handler = {}
  handler.promise = new Promise((resolve) => {
    handler.release = resolve
  })
  releases.push(() => handler.release())
  let handled = 0
  const consumer = makeConsumer(cluster)
  await start(consumer, async () => {
    handled += 1
    await handler.promise
  })
  await Promise.all(fetchGates.map((gate) => gate.arrived()))

  /** @returns {{count: number[], peak: number[], peakTotal: number}} the Fetches written per node, and the most in flight at once */
  const load = () => {
    const open = [0, 0, 0]
    const peak = [0, 0, 0]
    let total = 0
    let peakTotal = 0
    for (const { api, kind, node } of cluster.timeline()) {
      if (api !== 'Fetch') continue
      const delta = kind === 'request' ? 1 : -1
      open[node] += delta
      total += delta
      peak[node] = Math.max(peak[node], open[node])
      peakTotal = Math.max(peakTotal, total)
    }
    const fetches = cluster.requests().filter(({ api }) => api === 'Fetch')
    return {
      count: threeNodes.map(({ id }) => fetches.filter(({ node }) => node === id).length),
      peak,
      peakTotal,
    }
  }

  await tick()
  expect(load()).toEqual({ count: [1, 1, 1], peak: [1, 1, 1], peakTotal: 3 })

  fetchGates[1].release({ partitions: [{ partition: 1, errorCode: 0, records: true }] })
  await until(() => handled === 1)
  for (let turn = 0; turn < 20; turn++) await tick()
  await new Promise((resolve) => setTimeout(resolve, 100))
  expect(load().count).toEqual([1, 1, 1])

  handler.release()
  await until(() => load().count[1] === 2)
  expect(load()).toMatchObject({ count: [1, 2, 1], peak: [1, 1, 1] })
  expect(load().peakTotal).toBeLessThanOrEqual(3)
  const [, second] = cluster.requests().filter(({ api, node }) => api === 'Fetch' && node === 1)
  expect(second.fields.topics[0].partitions[0]).toMatchObject({ partition: 1, fetchOffset: '1' })
})

/** Computes committed lag from fresh broker observations, using lossless decimal arithmetic.
 * @param {object} admin connected admin client
 * @param {string} groupId group to observe
 * @param {string} topic topic to observe
 * @returns {Promise<object[]>} partition, hasCommit and decimal lag
 * @throws {Error} broker refusal or an omitted requested partition
 */
async function committedLagFor(admin, groupId, topic) {
  const fetched = await admin.fetchOffsets({ groupId, topics: [topic], resolveOffsets: false })
  const bounds = await admin.fetchTopicOffsets(topic, { isolationLevel: 'read_uncommitted' })
  const offsets = fetched.find((row) => row.topic === topic)?.partitions ?? []
  return bounds.map(({ partition, high, low }) => {
    const raw = offsets.find((row) => row.partition === partition)
    if (!raw) throw new Error('The broker omitted a requested partition')
    const hasCommit = raw.offset !== '-1'
    return {
      partition,
      hasCommit,
      lag: (BigInt(high) - BigInt(hasCommit ? raw.offset : low)).toString(),
    }
  })
}

describe('admin detail and lag equivalents', () => {
  it('R1§C:listTopics', async () => {
    const scripted = createScriptedCluster({ nodes })
    const topicId = '01020304-0506-0708-090a-0b0c0d0e0f10'
    scripted.answer('Metadata', () => ({
      brokers: nodes.map(({ id, host, port }) => ({ nodeId: id, host, port, rack: null })),
      topics: [
        {
          name: 'detail',
          topicId,
          errorCode: 0,
          isInternal: false,
          partitions: [],
          topicAuthorizedOperations: -2147483648,
        },
      ],
    }))
    const admin = new Kafka(scripted.kafkaOptions({ retry })).admin()
    try {
      await admin.connect()
      const start = scripted.requests().length
      expect(await admin.listTopics()).toEqual(['detail'])
      const middle = scripted.requests().length
      const { topics } = await admin.fetchTopicMetadata()
      expect(topics).toEqual([
        expect.objectContaining({ name: 'detail', isInternal: false, topicId }),
      ])
      const first = scripted
        .requests()
        .slice(start, middle)
        .filter((r) => r.api === 'Metadata')
      const second = scripted
        .requests()
        .slice(middle)
        .filter((r) => r.api === 'Metadata')
      expect(first).toHaveLength(1)
      expect(second).toHaveLength(1)
      expect(first[0].fields.topics).toEqual(second[0].fields.topics)
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })
  it('R2§5:kadm.Lag', async () => {
    const createAdmin = require('../admin')
    let committed = '0'
    const coordinator = {
      offsetFetch: jest.fn(async () => ({
        responses: [{ topic: 't', partitions: [{ partition: 0, offset: committed }] }],
      })),
    }
    const cluster = {
      findGroupCoordinator: async () => coordinator,
      addTargetTopic: async () => {},
      refreshMetadataIfNecessary: async () => {},
      findTopicPartitionMetadata: () => [{ partitionId: 0 }],
      fetchTopicsOffset: jest.fn(async (topics, options) => {
        expect(options.isolationLevel).toBe(0)
        return [
          {
            topic: 't',
            partitions: [{ partition: 0, offset: topics[0].fromBeginning ? '7' : '100' }],
          },
        ]
      }),
    }
    const admin = createAdmin({ cluster, logger: logger(), retry })
    const observed = []
    for (committed of ['0', '20', '50']) {
      const [observedLag] = await committedLagFor(admin, 'g', 't')
      observed.push(observedLag.lag)
    }
    expect(observed).toEqual(['100', '80', '50'])
    expect((100n - (19n + 1n)).toString()).toBe('80')
    committed = '-1'
    expect(await committedLagFor(admin, 'g', 't')).toEqual([
      { partition: 0, hasCommit: false, lag: '93' },
    ])
    const huge = {
      fetchOffsets: jest.fn(async () => [
        { topic: 't', partitions: [{ partition: 0, offset: '9007199254740993' }] },
      ]),
      fetchTopicOffsets: async () => [{ partition: 0, high: '9007199254741093', low: '7' }],
    }
    expect((await committedLagFor(huge, 'g', 't'))[0].lag).toBe('100')
    expect(huge.fetchOffsets).toHaveBeenCalledWith({
      groupId: 'g',
      topics: ['t'],
      resolveOffsets: false,
    })
    expect(coordinator.offsetFetch).toHaveBeenCalledTimes(4)
  })
})

it('R1§C:forceTerminateTransaction', async () => {
  const createAdmin = require('../admin')
  const request = require('../protocol/requests/initProducerId/v5/request')
  const bodies = []
  const broker = {
    initProducerId: async (options) => {
      bodies.push((await request(options).encode()).buffer.toString('hex'))
      return { producerId: '9007199254740993', producerEpoch: 8 }
    },
  }
  const cluster = {
    findGroupCoordinator: async ({ coordinatorType }) => {
      expect(coordinatorType).toBe(1)
      return broker
    },
  }
  const admin = createAdmin({ cluster, logger: logger(), retry })
  const forceTerminateEquivalent = (id) => admin.fenceProducers({ transactionalIds: [id] })
  const equivalent = await forceTerminateEquivalent('tx-a')
  const direct = await admin.fenceProducers({ transactionalIds: ['tx-a'] })
  expect(equivalent).toEqual(direct)
  expect(bodies).toHaveLength(2)
  expect(bodies[0]).toBe(bodies[1])
  expect(admin.forceTerminateTransaction).toBeUndefined()
})

const LEADER = { leader: 0, epoch: 1 }
const C = '20112233-4455-6677-8899-aabbccddeeff'
const ORDERS = { orders: { topicId: A, partitions: [LEADER] } }
const TOPIC_PAIR = {
  a: { topicId: A, partitions: [LEADER] },
  b: { topicId: B, partitions: [LEADER] },
}
const TOPIC_TRIPLE = { ...TOPIC_PAIR, c: { topicId: C, partitions: [LEADER] } }
const GROUP_UUID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const RESET_OFFSETS = {
  low: { 0: '7', 1: '9007199254740993' },
  high: { 0: '83', 1: '9223372036854775807' },
  at: { 0: '11', 1: '9007199254740994' },
}
const SHARE_GUIDE = fs.readFileSync(path.join(__dirname, '../../docs/ShareConsumer.md'), 'utf8')
const MIGRATION_GUIDE = fs.readFileSync(
  path.join(__dirname, '../../docs/MigrationGuide-3-0-0.md'),
  'utf8'
)

/**
 * Starts a public share consumer over the scripted share cluster, subscribed, with every
 * acknowledgement event collected; the client closes after the test.
 * @param {object} [options] Case options.
 * @param {object} [options.topics] Metadata the cluster serves, by topic name.
 * @param {string[]|null} [options.subscribe] Topic names to subscribe to, every served topic by default; null subscribes to none.
 * @param {object} [options.share] Overrides of the shareConsumer options.
 * @returns {Promise<{h: object, acks: object[]}>} The harness client and the collected payloads.
 * @throws {Error} When the subscription fails.
 */
const startShare = async ({ topics = ORDERS, subscribe = Object.keys(topics), share } = {}) => {
  const h = shareClient({ topics, ...(share && { share }) })
  use({ disconnect: () => h.close() }, {})
  const acks = []
  h.consumer.on(h.consumer.events.ACKNOWLEDGE, ({ payload }) => acks.push(payload))
  if (subscribe) await h.consumer.subscribe({ topics: subscribe })
  return { h, acks }
}
/** Holds the fetch of one session epoch. @param {object} h Share client. @param {number} epoch Session epoch. @returns {object} The gate. @throws Never. */
const holdFetch = (h, epoch) =>
  h.hold({ api: 'ShareFetch', match: ({ fields }) => fields.shareSessionEpoch === epoch })
/** A handler gate released at teardown. @returns {{promise: Promise<void>, resolve: Function}} The gate. @throws Never. */
const handlerGate = () => {
  const gate = deferred()
  releases.push(gate.resolve)
  return gate
}
/** @param {object} frame A recorded share frame. @returns {object[]} Its acknowledgement batches, in wire order. @throws Never. */
const batchesOf = ({ fields }) =>
  fields.topics.flatMap((t) => t.partitions.flatMap((p) => p.acknowledgementBatches))
/** @param {object[]} acks Acknowledge payloads. @returns {Array<Array>} Range, type and error reason of each, in event order. @throws Never. */
const outcomes = (acks) =>
  acks.map(({ firstOffset, lastOffset, type, error }) => [
    firstOffset,
    lastOffset,
    type,
    error ? error.reason : null,
  ])
/** @param {object[]} acks Payloads of small ranges. @returns {string[]} Every offset they cover, in event order. @throws Never. */
const coveredOffsets = (acks) =>
  acks.flatMap(({ firstOffset, lastOffset }) => {
    const offsets = []
    for (let offset = BigInt(firstOffset); offset <= BigInt(lastOffset); offset++)
      offsets.push(String(offset))
    return offsets
  })
/** @param {string} text Text. @returns {string} Its COMPACT_STRING bytes as hex. @throws Never. */
const compactHex = (text) =>
  (Buffer.byteLength(text) + 1).toString(16).padStart(2, '0') + Buffer.from(text).toString('hex')

/**
 * Compiles the offset-reset recipe printed in the share guide, in a context that shares this
 * realm's intrinsics, so the tested function is the documented one.
 * @returns {Function} The guide's `resetShareOffsets`.
 * @throws {Error} When the guide no longer prints the recipe.
 */
const guideReset = () => {
  const start = SHARE_GUIDE.indexOf('async function resetShareOffsets(')
  expect(start).toBeGreaterThan(-1)
  const source = SHARE_GUIDE.slice(start, SHARE_GUIDE.indexOf('\n}\n', start) + 2)
  return vm.runInContext(`(${source})`, vm.createContext({ Error, Number, BigInt }))
}

/**
 * Builds a real Admin whose share calls reach a real Broker over the scripted share cluster and
 * whose offset lookups answer from `offsets`; every share frame is decoded by the independent wire pairs.
 * @param {object} [options] Case options.
 * @param {object[]} [options.members] Members the described group reports.
 * @param {number} [options.describeCode] Group-level code of the description.
 * @param {number} [options.alterCode] Group-level code of the offset alteration.
 * @param {Record<number, number>} [options.partitionCodes] Error code of the alteration by partition.
 * @param {number[]} [options.partitions] Partitions the topic has.
 * @param {{low: object, high: object, at: object}} [options.offsets] Lookup answers by partition.
 * @returns {Promise<{admin: object, lookups: object[], frames: Function}>} The admin, the offset lookups it made and a frame reader.
 * @throws {Error} When the broker cannot connect.
 */
const resetRig = async ({
  members = [],
  describeCode = 0,
  alterCode = 0,
  partitionCodes = {},
  partitions = [0, 1],
  offsets = RESET_OFFSETS,
} = {}) => {
  const scripted = createScriptedCluster({ nodes, profile: 'share' })
  scripted.answer('ShareGroupDescribe', ({ groupIds }) => ({
    groups: groupIds.map((groupId) => ({
      ...expectedWire.shareGroupDescribeV1.response.groups[0],
      groupId,
      groupState: 'Empty',
      errorCode: describeCode,
      errorMessage: describeCode ? 'group error' : null,
      members,
    })),
  }))
  scripted.answer('AlterShareGroupOffsets', ({ topics }) => ({
    errorCode: alterCode,
    errorMessage: alterCode ? 'group error' : null,
    responses: topics.map(({ topicName, partitions: rows }) => ({
      topicName,
      topicId: GROUP_UUID,
      partitions: rows.map(({ partitionIndex }) => ({
        partitionIndex,
        errorCode: partitionCodes[partitionIndex] ?? 0,
        errorMessage: null,
      })),
    })),
  }))
  const pool = new ConnectionPool({
    host: 'node0',
    port: 9092,
    clientId: 'reset-admin',
    socketFactory: scripted.socketFactory,
    logger: logger(),
    connectionTimeout: 1000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
    connectionsMaxIdle: 0,
  })
  use({ disconnect: () => pool.destroy({ drainTimeout: 0 }) }, scripted)
  const broker = new Broker({ connectionPool: pool, logger: logger() })
  broker.nodeId = 0
  await broker.connect()
  const lookups = []
  const cluster = {
    findGroupCoordinator: async () => broker,
    refreshMetadata: async () => {},
    addTargetTopic: async () => {},
    refreshMetadataIfNecessary: async () => {},
    findTopicPartitionMetadata: () => partitions.map((partitionId) => ({ partitionId })),
    fetchTopicsOffset: async ([request]) => {
      lookups.push(request)
      const source =
        request.fromTimestamp !== undefined
          ? offsets.at
          : request.fromBeginning
            ? offsets.low
            : offsets.high
      return [
        {
          topic: request.topic,
          partitions: request.partitions.map(({ partition }) => ({
            partition,
            offset: source[partition],
          })),
        },
      ]
    },
  }
  return {
    admin: adminFactory({ cluster, retry, logger: logger() }),
    lookups,
    frames: (api) => scripted.requests().filter((request) => request.api === api),
  }
}

it('R1§E:poll', async () => {
  for (const kind of ['eachMessage', 'eachBatch']) {
    const { h } = await startShare()
    const { consumer } = h
    expect(consumer.poll).toBeUndefined()
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a', 'b', 'c'])])])
    const join = h.hold({ api: 'ShareGroupHeartbeat' })
    const piggyback = holdFetch(h, 1)
    const seen = []
    let callbackEntries = 0
    const handler = async ({ topic, partition, message, batch }) => {
      callbackEntries++
      for (const delivered of batch ? batch.messages : [message]) {
        seen.push([
          batch ? batch.topic : topic,
          batch ? batch.partition : partition,
          delivered.offset,
          delivered.value.toString(),
          delivered.deliveryCount,
        ])
      }
    }
    let state = 'pending'
    const running = consumer.run({ [kind]: handler }).then(
      () => {
        state = 'joined'
      },
      (error) => {
        state = error
      }
    )
    await join.arrived()
    await flush()
    expect(state).toBe('pending')
    expect(callbackEntries).toBe(0)
    expect(h.frames(78)).toHaveLength(0)
    join.release()
    await running
    expect(state).toBe('joined')
    await piggyback.arrived()
    expect(seen).toEqual([
      ['orders', 0, '0', 'a', 1],
      ['orders', 0, '1', 'b', 1],
      ['orders', 0, '2', 'c', 1],
    ])
    expect(callbackEntries).toBe(kind === 'eachBatch' ? 1 : 3)
    expect(consumer.assignment()).toEqual([{ topic: 'orders', partitions: [0] }])
    const entriesBeforeStop = callbackEntries
    await consumer.stop()
    piggyback.release(fetchAnswer([acquire(0, 3, ['late'])]))
    await flush()
    const postStopEntries = callbackEntries - entriesBeforeStop
    expect(postStopEntries).toBe(0)
    expect(consumer.assignment()).toEqual([])
    expect(consumer.subscription()).toEqual(['orders'])
  }
})

it('R1§E:commitAsync', async () => {
  const delivered = ['0', '1', '2']
  for (const acknowledgementMode of ['implicit', 'explicit']) {
    const { h, acks } = await startShare({ share: { acknowledgementMode } })
    expect(h.consumer.commitAsync).toBeUndefined()
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a', 'b', 'c'])])])
    const piggyback = holdFetch(h, 1)
    const decided = deferred()
    const finish = handlerGate()
    await h.consumer.run({
      eachBatch: async ({ batch, acknowledge }) => {
        const [first, second, third] = batch.messages
        acknowledge(first, 'release')
        acknowledge(first, 'accept')
        acknowledge(second, 'release')
        acknowledge(third, 'reject')
        decided.resolve()
        await finish.promise
      },
    })
    await decided.promise
    await flush()
    expect(h.frames(79)).toHaveLength(0)
    expect(h.frames(78)).toHaveLength(1)
    expect(acks).toEqual([])
    finish.resolve()
    const sent = await piggyback.arrived()
    expect(batchesOf(sent)).toEqual([
      { firstOffset: '0', lastOffset: '2', acknowledgeTypes: [1, 2, 3] },
    ])
    expect(acks).toEqual([])
    piggyback.release(fetchAnswer([part({ partition: 0 })]))
    await reach(() => acks.length === 3, 'the three settlements')
    expect(outcomes(acks)).toEqual([
      ['0', '0', 'accept', null],
      ['1', '1', 'release', null],
      ['2', '2', 'reject', null],
    ])
    expect(coveredOffsets(acks)).toEqual(delivered)
    expect(new Set(coveredOffsets(acks)).size).toBe(delivered.length)
    expect(h.frames(79)).toHaveLength(0)
  }

  const thrown = await startShare()
  const failures = []
  thrown.h.consumer.on(thrown.h.consumer.events.HANDLER_ERROR, ({ payload }) =>
    failures.push(payload)
  )
  thrown.h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a', 'b', 'c'])])])
  const failure = new Error('handler failed')
  await thrown.h.consumer.run({
    eachBatch: async ({ batch, acknowledge }) => {
      acknowledge(batch.messages[0], 'accept')
      throw failure
    },
  })
  await reach(() => thrown.h.frames(79).length === 1 && thrown.acks.length === 2, 'the flush')
  expect(failures).toHaveLength(1)
  expect(failures[0].error).toBe(failure)
  expect(thrown.h.frames(79).map(batchesOf)).toEqual([
    [{ firstOffset: '0', lastOffset: '2', acknowledgeTypes: [1, 2, 2] }],
  ])
  expect(outcomes(thrown.acks)).toEqual([
    ['0', '0', 'accept', null],
    ['1', '2', 'release', null],
  ])

  const lost = await startShare()
  lost.h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
  const unanswered = lost.h.hold({ api: 'ShareAcknowledge' })
  await lost.h.consumer.run({
    eachMessage: async () => {
      throw new Error('handler failed')
    },
  })
  await unanswered.arrived()
  unanswered.fail(new Error('connection lost'))
  await reach(() => lost.acks.length === 1, 'the lost settlement')
  await flush()
  expect(outcomes(lost.acks)).toEqual([['0', '0', 'release', 'session-lost']])
  expect(lost.acks[0].error).toBeInstanceOf(KafkaJSShareAcknowledgementError)
  expect(lost.h.frames(79)).toHaveLength(1)
  expect(lost.h.frames(78, 79).flatMap(batchesOf)).toEqual([
    { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [2] },
  ])

  const omitted = await startShare({
    topics: { orders: { topicId: A, partitions: [LEADER, LEADER] } },
  })
  omitted.h.script('ShareFetch', [
    fetchAnswer([acquire(0, 0, ['a']), acquire(1, 0, ['b'])]),
    () => fetchAnswer([part({ partition: 0 })]),
    fetchAnswer([acquire(0, 1, ['c'])]),
  ])
  await omitted.h.consumer.run({ eachMessage: async () => {} })
  await reach(() => omitted.acks.length === 3, 'the omission and the healthy acquisition')
  const onZero = omitted.acks.filter(({ partition }) => partition === 0)
  const onOne = omitted.acks.filter(({ partition }) => partition === 1)
  expect(outcomes(onZero)).toEqual([
    ['0', '0', 'accept', null],
    ['1', '1', 'accept', null],
  ])
  expect(onOne).toHaveLength(1)
  expect(onOne[0].error).toBeInstanceOf(KafkaJSShareAcknowledgementError)
  expect(onOne[0].error.ranges).toEqual([{ firstOffset: '0', lastOffset: '0' }])
  const sentForOne = omitted.h
    .frames(78, 79)
    .flatMap(({ fields }) => fields.topics.flatMap((t) => t.partitions))
    .filter(
      ({ partitionIndex, acknowledgementBatches }) =>
        partitionIndex === 1 && acknowledgementBatches.length
    )
  expect(sentForOne).toHaveLength(1)
})

it('R1§E:ackCallback', async () => {
  const FIRST = '9007199254740993'
  const LAST = '9007199255740992'

  const gap = await startShare()
  const crashes = []
  const removed = []
  gap.h.consumer.on(gap.h.consumer.events.CRASH, ({ payload }) => crashes.push(payload))
  gap.h.consumer.on(gap.h.consumer.events.ACKNOWLEDGE, () => {
    throw new Error('synchronous observer failure')
  })
  gap.h.consumer.on(gap.h.consumer.events.ACKNOWLEDGE, async () => {
    throw new Error('asynchronous observer failure')
  })
  const remove = gap.h.consumer.on(gap.h.consumer.events.ACKNOWLEDGE, ({ payload }) =>
    removed.push(payload)
  )
  remove()
  remove()
  expect(gap.h.consumer.setAcknowledgementCommitCallback).toBeUndefined()
  gap.h.script('ShareFetch', [
    fetchAnswer([part({ partition: 0, acquire: [acquired(FIRST, LAST)] })]),
  ])
  await gap.h.consumer.run({ eachMessage: async () => {} })
  await reach(() => gap.acks.length === 1, 'the settled gap')
  const [gapEvent] = gap.acks
  expect(Object.keys(gapEvent).sort()).toEqual([
    'error',
    'firstOffset',
    'lastOffset',
    'partition',
    'topic',
    'type',
  ])
  expect(gapEvent).toEqual({
    topic: 'orders',
    partition: 0,
    firstOffset: FIRST,
    lastOffset: LAST,
    type: 'gap',
    error: null,
  })
  expect(gapEvent.offsets).toBeUndefined()
  expect(gap.h.frames(78).flatMap(batchesOf)).toEqual([
    { firstOffset: FIRST, lastOffset: LAST, acknowledgeTypes: [0] },
  ])
  expect(removed).toEqual([])
  expect(crashes).toEqual([])

  const mixed = await startShare({ share: { acknowledgementMode: 'explicit' } })
  mixed.h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a', 'b', 'c'])])])
  await mixed.h.consumer.run({
    eachBatch: async ({ batch, acknowledge }) => {
      const [first, second, third] = batch.messages
      acknowledge(first, 'accept')
      acknowledge(second, 'release')
      acknowledge(third, 'reject')
    },
  })
  await reach(() => mixed.acks.length === 3, 'the three distinct settlements')
  expect(outcomes(mixed.acks)).toEqual([
    ['0', '0', 'accept', null],
    ['1', '1', 'release', null],
    ['2', '2', 'reject', null],
  ])
  expect(coveredOffsets(mixed.acks)).toEqual(['0', '1', '2'])

  const renewed = await startShare()
  renewed.h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
  renewed.h.script('ShareAcknowledge', [
    ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 2000 }),
    ackAnswer([{ partition: 0, code: 121 }]),
  ])
  const renewals = []
  await renewed.h.consumer.run({
    eachMessage: async ({ renew }) => {
      renewals.push(await renew())
      renewals.push(
        await renew().then(
          () => 'applied',
          (error) => error
        )
      )
    },
  })
  await reach(() => renewed.acks.length === 3, 'two renewals and the terminal outcome')
  expect(renewals[0]).toBe(2000)
  expect(renewals[1]).toBeInstanceOf(KafkaJSShareAcknowledgementError)
  expect(renewals[1]).toMatchObject({
    reason: 'rejected-by-broker',
    ranges: [{ firstOffset: '0', lastOffset: '0' }],
  })
  expect(outcomes(renewed.acks)).toEqual([
    ['0', '0', 'renew', null],
    ['0', '0', 'renew', 'rejected-by-broker'],
    ['0', '0', 'accept', null],
  ])
  const terminalAfterRenew = renewed.acks.filter(({ type }) => type !== 'renew')
  expect(terminalAfterRenew).toHaveLength(1)
  expect(terminalAfterRenew[0].error).toBeNull()
})

it('R1§E:reset-offsets', async () => {
  const resetShareOffsets = guideReset()
  const GROUP = 'orders-workers'
  /** @param {object} rig Reset rig. @returns {object[]} The alterations the broker received, in the guide's shape. @throws Never. */
  const sent = (rig) =>
    rig.frames('AlterShareGroupOffsets').map(({ fields }) => ({
      groupId: fields.groupId,
      topics: fields.topics.map(({ topicName, partitions }) => ({
        topic: topicName,
        partitions: partitions.map(({ partitionIndex, startOffset }) => ({
          partition: partitionIndex,
          offset: startOffset,
        })),
      })),
    }))
  const selected = [
    [
      'earliest',
      [
        { partition: 0, offset: '7' },
        { partition: 1, offset: '9007199254740993' },
      ],
    ],
    [
      'latest',
      [
        { partition: 0, offset: '83' },
        { partition: 1, offset: '9223372036854775807' },
      ],
    ],
    [
      1700000000000,
      [
        { partition: 0, offset: '11' },
        { partition: 1, offset: '9007199254740994' },
      ],
    ],
  ]
  for (const [position, partitions] of selected) {
    const rig = await resetRig()
    const results = await resetShareOffsets(rig.admin, GROUP, 'orders', position)
    expect(sent(rig)).toEqual([{ groupId: GROUP, topics: [{ topic: 'orders', partitions }] }])
    expect(results).toEqual([
      {
        topic: 'orders',
        topicId: GROUP_UUID,
        partitions: [
          { partition: 0, errorCode: 0 },
          { partition: 1, errorCode: 0 },
        ],
      },
    ])
    expect(rig.frames('ShareGroupDescribe').map(({ fields }) => fields.groupIds)).toEqual([[GROUP]])
    expect(
      rig.lookups.map(({ fromTimestamp }) => fromTimestamp).filter((value) => value !== undefined)
    ).toEqual(typeof position === 'number' ? [position] : [])
  }

  const unmatched = await resetRig({ offsets: { ...RESET_OFFSETS, at: { 0: '11', 1: '-1' } } })
  await resetShareOffsets(unmatched.admin, GROUP, 'orders', 1700000000000)
  expect(sent(unmatched)[0].topics[0].partitions).toEqual([
    { partition: 0, offset: '11' },
    { partition: 1, offset: '9223372036854775807' },
  ])
  const zero = await resetRig({ offsets: { ...RESET_OFFSETS, low: { 0: '0', 1: '0' } } })
  await resetShareOffsets(zero.admin, GROUP, 'orders', 'earliest')
  expect(sent(zero)[0].topics[0].partitions).toEqual([
    { partition: 0, offset: '0' },
    { partition: 1, offset: '0' },
  ])

  for (const position of [
    -1,
    1.5,
    Number.MAX_SAFE_INTEGER + 1,
    'middle',
    '1700000000000',
    null,
    undefined,
  ]) {
    const rig = await resetRig()
    await expect(resetShareOffsets(rig.admin, GROUP, 'orders', position)).rejects.toThrow(
      'Use earliest, latest or nonnegative epoch milliseconds'
    )
    expect(rig.frames('ShareGroupDescribe')).toEqual([])
    expect(rig.lookups).toEqual([])
    expect(sent(rig)).toEqual([])
  }
  const memberMessage = 'The share group must exist and have no members'
  const active = await resetRig({
    members: expectedWire.shareGroupDescribeV1.response.groups[0].members,
  })
  await expect(resetShareOffsets(active.admin, GROUP, 'orders', 'earliest')).rejects.toThrow(
    memberMessage
  )
  expect(active.frames('ShareGroupDescribe')).toHaveLength(1)
  expect(active.lookups).toEqual([])
  expect(sent(active)).toEqual([])
  const missing = await resetRig({ describeCode: 69 })
  await expect(resetShareOffsets(missing.admin, GROUP, 'orders', 'earliest')).rejects.toThrow(
    memberMessage
  )
  expect(missing.lookups).toEqual([])
  expect(sent(missing)).toEqual([])
  for (const malformed of ['-5', '9223372036854775808', '007', '1.5', '', 'abc', 83]) {
    const rig = await resetRig({ offsets: { ...RESET_OFFSETS, high: { 0: '83', 1: malformed } } })
    await expect(resetShareOffsets(rig.admin, GROUP, 'orders', 'latest')).rejects.toThrow(
      'The offset lookup must return a nonnegative signed int64 string'
    )
    expect(sent(rig)).toEqual([])
  }
  const empty = await resetRig({ partitions: [] })
  await expect(resetShareOffsets(empty.admin, GROUP, 'orders', 'latest')).rejects.toThrow(
    'The lookup returned no partitions'
  )
  expect(sent(empty)).toEqual([])

  const refused = await resetRig({ partitionCodes: { 1: 29 } })
  await expect(resetShareOffsets(refused.admin, GROUP, 'orders', 'latest')).rejects.toMatchObject({
    code: 29,
    type: 'TOPIC_AUTHORIZATION_FAILED',
  })
  expect(sent(refused)).toHaveLength(1)
  const raced = await resetRig({ alterCode: 68 })
  await expect(resetShareOffsets(raced.admin, GROUP, 'orders', 'latest')).rejects.toMatchObject({
    code: 68,
    type: 'NON_EMPTY_GROUP',
  })
  expect(raced.frames('ShareGroupDescribe')).toHaveLength(1)
  expect(sent(raced)).toHaveLength(1)
})

it('R1§C:deleteShareGroups', async () => {
  const GROUP = 'equivalent-share-empty'
  const notEmpty = 'The group is not empty.'
  /**
   * @param {number} code Group-level error code.
   * @param {string|null} message Broker message.
   * @returns {Buffer} A DeleteGroups v3 answer for the one group.
   * @throws Never.
   */
  const answerFor = (code, message) =>
    Buffer.from(
      `00000000${'02'}${compactHex(GROUP)}${code.toString(16).padStart(4, '0')}${
        message === null ? '00' : compactHex(message)
      }0000`,
      'hex'
    )
  for (const [code, message] of [
    [0, null],
    [68, notEmpty],
  ]) {
    const harness = createGroupApiBroker({
      versions: { [apiKeys.DeleteGroups]: { minVersion: 0, maxVersion: 3 } },
      respond: () => answerFor(code, message),
    })
    use({ disconnect: () => harness.pool.destroy() }, {})
    const lookups = []
    const admin = adminFactory({
      logger: logger(),
      retry,
      cluster: {
        refreshMetadata: async () => {},
        findGroupCoordinator: async (options) => {
          lookups.push(options)
          return harness.broker
        },
        targetTopics: new Set(),
      },
    })
    expect(admin.deleteShareGroups).toBeUndefined()
    if (code === 0) {
      await expect(admin.deleteGroups([GROUP])).resolves.toEqual([
        { groupId: GROUP, errorCode: 0, errorMessage: null },
      ])
    } else {
      const failure = await admin.deleteGroups([GROUP]).catch((error) => error)
      expect(failure).toBeInstanceOf(KafkaJSDeleteGroupsError)
      expect(failure.groups).toEqual([
        {
          groupId: GROUP,
          errorCode: 68,
          error: expect.objectContaining({ code: 68, type: 'NON_EMPTY_GROUP' }),
          errorMessage: notEmpty,
        },
      ])
    }
    expect(lookups).toEqual([{ groupId: GROUP }])
    expect(harness.requests.map(({ apiKey, apiVersion }) => [apiKey, apiVersion])).toEqual([
      [apiKeys.DeleteGroups, 3],
    ])
    expect(harness.requests[0].body.toString('hex')).toBe(`02${compactHex(GROUP)}00`)
  }
})

it('R1§E:commitSync', async () => {
  const { h, acks } = await startShare()
  const { consumer } = h
  for (const method of ['commitSync', 'commit', 'commitOffsets', 'commitAsync'])
    expect(consumer[method]).toBeUndefined()
  h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a', 'b'])])])
  const piggyback = holdFetch(h, 1)
  const decided = deferred()
  const finish = handlerGate()
  await consumer.run({
    eachBatch: async ({ batch, acknowledge }) => {
      for (const message of batch.messages) acknowledge(message, 'accept')
      decided.resolve()
      await finish.promise
    },
  })
  await decided.promise
  await flush()
  const eventsWhileHandlerHeld = [...acks]
  expect(eventsWhileHandlerHeld).toEqual([])
  expect(h.frames(79)).toHaveLength(0)
  expect(h.frames(78)).toHaveLength(1)
  finish.resolve()
  await piggyback.arrived()
  const eventsWhileAnswerHeld = [...acks]
  expect(eventsWhileAnswerHeld).toEqual([])
  piggyback.release(fetchAnswer([part({ partition: 0 })]))
  await reach(() => acks.length === 1, 'the automatic settlement')
  expect(acks[0]).toEqual({
    topic: 'orders',
    partition: 0,
    firstOffset: '0',
    lastOffset: '1',
    type: 'accept',
    error: null,
  })
  expect(h.frames(79)).toHaveLength(0)
  for (const guide of [SHARE_GUIDE, MIGRATION_GUIDE])
    expect(guide).toContain('There is no callable synchronous flush-and-wait')
})

it('R1§E:unsubscribe', async () => {
  const { h } = await startShare({ topics: TOPIC_PAIR, subscribe: null })
  const { consumer } = h
  expect(consumer.unsubscribe).toBeUndefined()
  expect(consumer.subscription()).toEqual([])
  await consumer.subscribe({ topics: ['a', 'b'] })
  const previousSnapshot = consumer.subscription()
  expect(previousSnapshot).toEqual(['a', 'b'])
  previousSnapshot.push('mutated')
  expect(consumer.subscription()).toEqual(['a', 'b'])
  expect(consumer.subscription()).not.toBe(previousSnapshot)
  const eachMessage = async () => {}
  await consumer.run({ eachMessage })
  await consumer.stop()
  expect(consumer.assignment()).toEqual([])
  expect(consumer.subscription()).toEqual(['a', 'b'])
  await expect(consumer.subscribe({ topics: [] })).rejects.toThrow(KafkaJSNonRetriableError)
  expect(consumer.subscription()).toEqual(['a', 'b'])
  await consumer.run({ eachMessage })
  const joins = h.frames(76).filter(({ fields }) => fields.memberEpoch === 0)
  const secondJoinTopics = joins[1].fields.subscribedTopicNames
  expect(joins).toHaveLength(2)
  expect(secondJoinTopics).toEqual(['a', 'b'])
  expect(joins[1].fields.memberId).not.toBe(joins[0].fields.memberId)
  for (const guide of [SHARE_GUIDE, MIGRATION_GUIDE])
    expect(guide).toContain('stop retains the subscribed names')
})

it('R1§E:resubscribe', async () => {
  const { h } = await startShare({ topics: TOPIC_TRIPLE, subscribe: ['a', 'b'] })
  const { consumer } = h
  h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['first'])])])
  const oldFetch = holdFetch(h, 1)
  const handled = []
  const eachMessage = async ({ topic, message }) => {
    handled.push(`${topic}:${message.value.toString()}`)
  }
  await consumer.run({ eachMessage })
  await oldFetch.arrived()
  expect(handled).toEqual(['a:first'])
  const joinsOf = () => h.frames(76).filter(({ fields }) => fields.memberEpoch === 0)
  const oldMemberId = joinsOf()[0].fields.memberId
  const heartbeatsBefore = h.frames(76).length
  await expect(consumer.subscribe({ topics: ['c'] })).rejects.toThrow(KafkaJSNonRetriableError)
  expect(consumer.subscription()).toEqual(['a', 'b'])
  await reach(() => h.frames(76).length > heartbeatsBefore, 'a heartbeat after the rejected call')
  const replacementHeartbeatsBeforeStop = h
    .frames(76)
    .filter(({ fields }) => (fields.subscribedTopicNames ?? []).includes('c'))
  expect(replacementHeartbeatsBeforeStop).toEqual([])
  await consumer.stop()
  await consumer.subscribe({ topics: ['c'] })
  await consumer.run({ eachMessage })
  expect(consumer.subscription()).toEqual(['c'])
  const joins = joinsOf()
  expect(joins.map(({ fields }) => fields.subscribedTopicNames)).toEqual([['a', 'b'], ['c']])
  const newMemberId = joins[1].fields.memberId
  expect(newMemberId).not.toBe(oldMemberId)
  oldFetch.release(fetchAnswer([acquire(0, 5, ['late'])]))
  await flush()
  expect(handled).toEqual(['a:first'])
  expect(consumer.subscription()).toEqual(['c'])
  expect(joinsOf()).toHaveLength(2)
  for (const guide of [SHARE_GUIDE, MIGRATION_GUIDE])
    expect(guide).toContain('This sequence does not preserve running-replacement semantics.')
})
