const v8 = require('v8')
const vm = require('vm')
const { Kafka } = require('../../../index')
const Broker = require('../../broker')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { KafkaJSConnectionClosedError, KafkaJSTimeout } = require('../../errors')

const NODES = [{ id: 0, host: 'node', port: 9092 }]
/** A broker the bootstrap node does not know, one a late Metadata answer can advertise. */
const ADVERTISED = { id: 1, host: 'advertised', port: 9092 }
const TOPIC = 'paged-topic'
const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const RECREATED_ID = 'f1f2f3f4-f5f6-f7f8-f9fa-fbfcfdfeff00'
/** Java's `default.api.timeout.ms` default: the one budget of the read. */
const DEADLINE_MS = 60000

/** @param {number} partitionIndex @returns {object} one partition of a DescribeTopicPartitions row */
const partition = (partitionIndex) => ({
  errorCode: 0,
  partitionIndex,
  leaderId: 0,
  leaderEpoch: 1,
  replicaNodes: [0],
  isrNodes: [0],
  eligibleLeaderReplicas: null,
  lastKnownElr: null,
  offlineReplicas: [],
})

/**
 * @param {number[]} partitions the partition indexes this page holds
 * @param {{ topicId?: string|null, more?: boolean }} [options] `more` names a cursor after them
 * @returns {object} the fields of one page of the topic
 */
const page = (partitions, { topicId = TOPIC_ID, more = false } = {}) => ({
  throttleTime: 0,
  topics: [
    {
      errorCode: 0,
      name: TOPIC,
      topicId,
      isInternal: false,
      partitions: partitions.map(partition),
      topicAuthorizedOperations: -2147483648,
    },
  ],
  nextCursor: more ? { topicName: TOPIC, partitionIndex: partitions.at(-1) + 1 } : null,
})

/** Lets the in-process sockets deliver and the fake clock move, `ms` at a time. */
const turn = async (ms = 0) => {
  await new Promise(setImmediate)
  await jest.advanceTimersByTimeAsync(ms)
  await new Promise(setImmediate)
}

/**
 * @param {Promise<T>} promise
 * @returns {{ promise: Promise<void>, done: boolean, value?: T, error?: Error }} its outcome, read
 *   without awaiting it
 * @template T
 */
const settle = (promise) => {
  const state = { done: false }
  state.promise = promise.then(
    (value) => Object.assign(state, { done: true, value }),
    (error) => Object.assign(state, { done: true, error })
  )
  return state
}

describe('Admin > fetchTopicMetadata through DescribeTopicPartitions > one deadline', () => {
  let cluster, admin, baseline, gates

  /**
   * @param {object} [options] over the Kafka options of the scripted cluster
   * @param {Array<{ id: number, host: string, port: number }>} [nodes] the endpoints the cluster serves
   * @returns {Promise<void>}
   */
  const connect = async (options = {}, nodes = NODES) => {
    cluster = createScriptedCluster({
      nodes,
      advertise: { DescribeTopicPartitions: [0, 0] },
    })
    gates = []
    const hold = cluster.schedule.hold.bind(cluster.schedule)
    cluster.schedule.hold = (selector) => {
      const gate = hold(selector)
      gates.push(gate)
      return gate
    }
    cluster.answer('Metadata', () => ({
      brokers: [{ nodeId: 0, host: 'node', port: 9092, rack: null }],
      controllerId: 0,
      topics: [],
    }))
    // A request timeout longer than the deadline: a held request must stall, not time out.
    admin = new Kafka(
      cluster.kafkaOptions({
        requestTimeout: 2 * DEADLINE_MS,
        retry: { retries: 3, initialRetryTime: 100, maxRetryTime: 100, factor: 0, multiplier: 1 },
        ...options,
      })
    ).admin()
    await admin.connect()
    baseline = jest.getTimerCount()
  }

  /** @returns {object[]} the DescribeTopicPartitions requests written, oldest first */
  const pages = () => cluster.requests().filter(({ api }) => api === 'DescribeTopicPartitions')

  /** @returns {Promise<object>} the read, settled in the background */
  const read = () =>
    settle(admin.fetchTopicMetadata({ topics: [TOPIC], useDescribeTopicPartitions: true }))

  beforeEach(() => {
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] })
  })

  afterEach(async () => {
    try {
      // A case that failed while a request was held must not leave the disconnect waiting on it.
      for (const gate of gates) if (gate.settle) gate.release()
      if (admin) await admin.disconnect()
    } finally {
      admin = null
      jest.useRealTimers()
    }
    cluster.assertClean()
  })

  test('ordinary paging resolves with every page and leaves no timer', async () => {
    await connect()
    cluster.schedule.once({ api: 'DescribeTopicPartitions' }, page([0, 1], { more: true }))
    cluster.schedule.once({ api: 'DescribeTopicPartitions' }, page([2]))

    const result = read()
    while (!result.done) await turn(1)

    expect(result.error).toBeUndefined()
    expect(result.value.topics).toEqual([
      expect.objectContaining({ name: TOPIC, topicId: TOPIC_ID, partitions: expect.any(Array) }),
    ])
    expect(result.value.topics[0].partitions.map(({ partitionId }) => partitionId)).toEqual([
      0, 1, 2,
    ])
    expect(pages().map(({ fields }) => fields.cursor)).toEqual([
      null,
      { topicName: TOPIC, partitionIndex: 2 },
    ])
    expect(jest.getTimerCount()).toBe(baseline)
  })

  test('a held discovery rejects at the deadline, and no page is asked for', async () => {
    await connect({ metadataMaxAge: 0 })
    const discovery = cluster.schedule.hold({ api: 'Metadata' })

    const result = read()
    await discovery.arrived()
    await turn(DEADLINE_MS - 1)
    expect(result.done).toBe(false)
    await turn(1)

    expect(result.error).toBeInstanceOf(KafkaJSTimeout)
    expect(jest.getTimerCount()).toBe(baseline)
    discovery.release()
    for (let step = 0; step < 20; step++) await turn(10)
    expect(pages()).toEqual([])
  })

  test('a held discovery released after the deadline opens no connection and sends no request', async () => {
    await connect({ metadataMaxAge: 0, brokers: ['node:9092'] }, [...NODES, ADVERTISED])
    const discovery = cluster.schedule.hold({ api: 'Metadata' })

    const result = read()
    await discovery.arrived()
    await turn(DEADLINE_MS)
    expect(result.error).toBeInstanceOf(KafkaJSTimeout)
    const written = cluster.requests().length

    discovery.release({
      brokers: [
        { nodeId: ADVERTISED.id, host: ADVERTISED.host, port: ADVERTISED.port, rack: null },
      ],
      controllerId: ADVERTISED.id,
      topics: [],
    })
    for (let step = 0; step < 20; step++) await turn(10)

    expect(cluster.sockets().map(({ endpoint }) => endpoint.host)).toEqual([NODES[0].host])
    expect(cluster.requests()).toHaveLength(written)
    expect(jest.getTimerCount()).toBeLessThanOrEqual(baseline)
  })

  test('a held last page rejects at the deadline, and its late answer changes nothing', async () => {
    await connect()
    cluster.schedule.once({ api: 'DescribeTopicPartitions' }, page([0], { more: true }))
    const last = cluster.schedule.hold({ api: 'DescribeTopicPartitions' })

    const result = read()
    await last.arrived()
    await turn(DEADLINE_MS)

    expect(result.error).toBeInstanceOf(KafkaJSTimeout)
    expect(result.error.message).toBe(
      `fetchTopicMetadata did not complete within ${DEADLINE_MS} ms; outcome is unknown; a mutation may have been applied`
    )
    expect(jest.getTimerCount()).toBe(baseline)
    last.release(page([1], { more: true }))
    for (let step = 0; step < 20; step++) await turn(10)
    expect(pages()).toHaveLength(2)
    expect(result.value).toBeUndefined()
  })

  test('the deadline is the one taken at the start, across an outer retry', async () => {
    await connect()
    const failed = cluster.schedule.hold({ api: 'DescribeTopicPartitions' })
    const started = Date.now()
    const result = read()
    await failed.arrived()
    await turn(DEADLINE_MS - 10000)
    const stalled = cluster.schedule.hold({ api: 'DescribeTopicPartitions' })
    failed.fail(new KafkaJSConnectionClosedError('closed'))
    while (!stalled.call) await turn(10)
    expect(result.done).toBe(false)

    await turn(started + DEADLINE_MS - 1 - Date.now())
    expect(result.done).toBe(false)
    await turn(1)

    expect(result.error).toBeInstanceOf(KafkaJSTimeout)
    expect(pages()).toHaveLength(2)
    expect(jest.getTimerCount()).toBe(baseline)
    stalled.release(page([0]))
    for (let step = 0; step < 20; step++) await turn(10)
    expect(pages()).toHaveLength(2)
  })

  test('no attempt starts after the deadline, even one the retrier had scheduled', async () => {
    await connect()
    const failed = cluster.schedule.hold({ api: 'DescribeTopicPartitions' })
    const started = Date.now()
    const result = read()
    await failed.arrived()
    await turn(started + DEADLINE_MS - 50 - Date.now())
    failed.fail(new KafkaJSConnectionClosedError('closed'))
    await turn(50)
    const written = cluster.requests().length

    expect(result.error).toBeInstanceOf(KafkaJSTimeout)
    for (let step = 0; step < 30; step++) await turn(10)
    expect(pages()).toHaveLength(1)
    // Not even the metadata refresh the next attempt would have begun with.
    expect(cluster.requests().slice(written)).toEqual([])
    // The failed connection took its own request-timeout timer with it.
    expect(jest.getTimerCount()).toBeLessThanOrEqual(baseline)
  })

  test('a recreation between pages rejects the read retriably, and the retrier reads again', async () => {
    await connect()
    cluster.schedule.once({ api: 'DescribeTopicPartitions' }, page([0], { more: true }))
    cluster.schedule.once({ api: 'DescribeTopicPartitions' }, page([1], { topicId: RECREATED_ID }))
    cluster.schedule.once(
      { api: 'DescribeTopicPartitions' },
      page([0], { topicId: RECREATED_ID, more: true })
    )
    cluster.schedule.once({ api: 'DescribeTopicPartitions' }, page([1], { topicId: RECREATED_ID }))

    const result = read()
    while (!result.done) await turn(10)

    expect(result.error).toBeUndefined()
    expect(result.value.topics[0]).toMatchObject({ topicId: RECREATED_ID })
    expect(result.value.topics[0].partitions.map(({ partitionId }) => partitionId)).toEqual([0, 1])
    expect(pages().map(({ fields }) => fields.cursor)).toEqual([
      null,
      { topicName: TOPIC, partitionIndex: 1 },
      null,
      { topicName: TOPIC, partitionIndex: 1 },
    ])
  })

  test('a topic keeps its first known id across a row that carries none', async () => {
    await connect()
    cluster.schedule.once(
      { api: 'DescribeTopicPartitions' },
      page([0], { topicId: null, more: true })
    )
    cluster.schedule.once({ api: 'DescribeTopicPartitions' }, page([1], { more: true }))
    cluster.schedule.once({ api: 'DescribeTopicPartitions' }, page([2], { topicId: null }))

    const result = read()
    while (!result.done) await turn(10)

    expect(result.error).toBeUndefined()
    expect(result.value.topics[0].topicId).toBe(TOPIC_ID)
    expect(result.value.topics[0].partitions).toHaveLength(3)
    expect(pages()).toHaveLength(3)
  })

  test('without request timeouts a stalled page still rejects at the deadline', async () => {
    await connect({ enforceRequestTimeout: false })
    const stalled = cluster.schedule.hold({ api: 'DescribeTopicPartitions' })

    const result = read()
    await stalled.arrived()
    await turn(DEADLINE_MS)

    expect(result.error).toBeInstanceOf(KafkaJSTimeout)
    expect(jest.getTimerCount()).toBe(baseline)
    stalled.release(page([0]))
    await turn(10)
  })

  test('the pages read before the deadline are dropped when it passes', async () => {
    await connect({ enforceRequestTimeout: false })
    v8.setFlagsFromString('--expose-gc')
    const gc = vm.runInNewContext('gc')
    const original = Broker.prototype.describeTopicPartitions
    let firstPage
    Broker.prototype.describeTopicPartitions = async function (...args) {
      const response = await original.apply(this, args)
      firstPage ??= new WeakRef(response.topics[0].partitions[0])
      return response
    }
    try {
      cluster.schedule.once({ api: 'DescribeTopicPartitions' }, page([0], { more: true }))
      const stalled = cluster.schedule.hold({ api: 'DescribeTopicPartitions' })
      const result = read()
      await stalled.arrived()
      await turn(DEADLINE_MS)
      expect(result.error).toBeInstanceOf(KafkaJSTimeout)

      gc()
      await new Promise(setImmediate)
      gc()

      expect(firstPage.deref()).toBeUndefined()
      stalled.release(page([1]))
      await turn(10)
    } finally {
      Broker.prototype.describeTopicPartitions = original
    }
  })
})
