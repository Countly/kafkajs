const { Kafka } = require('../../../index')
const Cluster = require('../../cluster')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { newLogger, connectionOpts } = require('testHelpers')
const { TOPIC, TOPIC_ID } = require('../../consumer/__tests__/fixtures/fetchHarness')

const NODES = [
  { id: 0, host: 'node0', port: 9092 },
  { id: 1, host: 'node1', port: 9092 },
]
const MAX_AGE = 10000
const RETRY = { initialRetryTime: 100, maxRetryTime: 30000, retries: 3, factor: 0 }

/**
 * Jumps added to `Date.now` and `performance.now`, so the metadata age and the backoff are driven
 * by the test.
 */
let skew = 0
/** When set, the real times both clocks stand still at, between jumps. */
let frozenAt = null
let frozenPerfAt = null
const realNow = Date.now.bind(Date)
const realPerfNow = performance.now.bind(performance)
const freeze = () => {
  frozenAt = realNow()
  frozenPerfAt = realPerfNow()
}
const advance = (ms) => {
  skew += ms
}
const ageMetadata = () => advance(MAX_AGE + 1)

/** @returns {Promise<void>} lets answers, which arrive on later macrotasks, settle */
const turns = async (count = 30) => {
  for (let i = 0; i < count; i++) await new Promise(setImmediate)
}

/**
 * @param {Promise<any>} promise
 * @param {string} what
 * @param {number} [ms]
 * @returns {Promise<any>} the promise's outcome, or a rejection naming `what` after `ms` real ms
 */
const within = (promise, what, ms = 2000) => {
  let timer
  const late = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} did not settle within ${ms} ms`)), ms)
  })
  return Promise.race([promise, late]).finally(() => clearTimeout(timer))
}

/** @returns {{ promise: Promise<any>, release: (value?: any) => void }} */
const hold = () => {
  let release
  const promise = new Promise((resolve) => {
    release = resolve
  })
  return { promise, release }
}

/**
 * @param {{ partitions?: number, leader?: number, epoch?: number }} [options]
 * @returns {object} a Metadata answer listing the topic
 */
const metadataAnswer = ({ partitions = 1, leader = 0, epoch = 5 } = {}) => ({
  brokers: NODES.map(({ id, host, port }) => ({ nodeId: id, host, port, rack: null })),
  controllerId: 0,
  topics: [
    {
      errorCode: 0,
      name: TOPIC,
      topicId: TOPIC_ID,
      isInternal: false,
      topicAuthorizedOperations: -2147483648,
      partitions: Array.from({ length: partitions }, (_, partitionIndex) => ({
        errorCode: 0,
        partitionIndex,
        leaderId: leader,
        leaderEpoch: epoch,
        replicas: [leader],
        isr: [leader],
        offlineReplicas: [],
      })),
    },
  ],
})

/**
 * A scripted cluster whose Produce requests are recorded as `{ node, partitions, seqs }`, the
 * `seqs` read from the `{"seq":n}` values in the order they were written.
 * @returns {{ cluster: object, produced: object[] }}
 */
const recordingCluster = () => {
  const cluster = producerScript(createScriptedCluster({ nodes: NODES }))
  const produced = []
  const pending = []
  cluster.watch((event, record) => {
    if (event !== 'request' || record.api !== 'Produce') return
    const entry = {
      node: record.node,
      partitions: record.fields.topics.flatMap((t) => t.partitions.map((p) => p.partition)),
      seqs: [],
    }
    produced.push(entry)
    pending.push(entry)
  })
  const socketFactory = cluster.socketFactory
  cluster.socketFactory = (options) => {
    const socket = socketFactory(options)
    const write = socket.write
    socket.write = (buffer, ...rest) => {
      const frame = Buffer.from(buffer)
      const seqs =
        frame.readInt16BE(4) === 0
          ? [...frame.toString('latin1').matchAll(/\{"seq":(\d+)\}/g)].map(([, n]) => Number(n))
          : null
      const result = write(buffer, ...rest)
      if (seqs) {
        const entry = pending.shift()
        if (entry) entry.seqs = seqs
      }
      return result
    }
    return socket
  }
  return { cluster, produced }
}

/**
 * A connected producer over a scripted cluster, warmed by one send so the topic's metadata is
 * cached.
 * @param {{ idempotent?: boolean, createPartitioner?: Function, requestTimeout?: number, retry?: object }} [options]
 * @returns {Promise<{ cluster: object, produced: object[], producer: object }>}
 */
const warmProducer = async ({
  idempotent = true,
  createPartitioner,
  requestTimeout,
  retry = RETRY,
} = {}) => {
  const { cluster, produced } = recordingCluster()
  cluster.answer('Metadata', () => metadataAnswer())
  const kafka = new Kafka(
    cluster.kafkaOptions({
      metadataRecoveryStrategy: 'none',
      retry,
      ...(requestTimeout && { requestTimeout }),
    })
  )
  const producer = kafka.producer({ idempotent, metadataMaxAge: MAX_AGE, createPartitioner })
  await producer.connect()
  await producer.send({ topic: TOPIC, messages: [{ value: 'warm' }] })
  return { cluster, produced, producer }
}

/**
 * Captures the Cluster the next producer connects with, so a test can read its broker pool.
 * @returns {() => Cluster} the captured cluster
 */
const captureCluster = () => {
  let captured
  const connect = Cluster.prototype.connect
  jest.spyOn(Cluster.prototype, 'connect').mockImplementation(function (...args) {
    captured ??= this
    return connect.apply(this, args)
  })
  return () => captured
}

/** @param {object} cluster @returns {number} Metadata requests not yet answered or cancelled */
const metadataInFlight = (cluster) =>
  cluster.tally('Metadata') -
  cluster.tally('Metadata', 'answered') -
  cluster.tally('Metadata', 'cancelled')

const send = (producer, seq) =>
  producer.send({ topic: TOPIC, messages: [{ value: JSON.stringify({ seq }) }] })

describe('Producer > sends on metadata that is only past metadataMaxAge', () => {
  const unhandled = []
  const onUnhandled = (reason) => unhandled.push(reason)
  let producer

  beforeEach(() => {
    skew = 0
    unhandled.length = 0
    producer = null
    frozenAt = null
    frozenPerfAt = null
    jest.spyOn(Date, 'now').mockImplementation(() => (frozenAt ?? realNow()) + skew)
    jest.spyOn(performance, 'now').mockImplementation(() => (frozenPerfAt ?? realPerfNow()) + skew)
    process.on('unhandledRejection', onUnhandled)
  })

  afterEach(async () => {
    await producer?.disconnect()
    process.off('unhandledRejection', onUnhandled)
    jest.restoreAllMocks()
  })

  describe.each([
    ['an idempotent producer', true],
    ['a non-idempotent producer', false],
  ])('%s', (_name, idempotent) => {
    it('completes sends on the cached leaders while one refresh that never answers is in flight', async () => {
      const setup = await warmProducer({ idempotent })
      producer = setup.producer
      const { cluster } = setup
      const before = cluster.tally('Metadata')
      cluster.answer('Metadata', () => new Promise(() => {}))
      ageMetadata()

      await within(
        Promise.all(Array.from({ length: 50 }, (_, seq) => send(producer, seq))),
        'sends on aged metadata'
      )
      await turns()

      expect(cluster.tally('Metadata')).toBe(before + 1)
      expect(metadataInFlight(cluster)).toBe(1)
      cluster.assertClean()
    })

    it('fails no send when the refresh fails, and waits out a doubling backoff before the next one', async () => {
      const setup = await warmProducer({ idempotent })
      producer = setup.producer
      const { cluster } = setup
      freeze()
      const before = cluster.tally('Metadata')
      cluster.outage('Metadata', { errorCode: 7 })
      ageMetadata()
      const requests = () => cluster.tally('Metadata') - before
      const sendSome = async (count) => {
        for (let i = 0; i < count; i++) await within(send(producer, i), 'a send on aged metadata')
        await turns()
      }

      await sendSome(100)
      expect(requests()).toBe(1)

      // initialRetryTime 100 after the first failure, then 200, 400, 800, then capped at 1000.
      const expected = [
        [99, 1],
        [1, 2],
        [199, 2],
        [1, 3],
        [399, 3],
        [1, 4],
        [799, 4],
        [1, 5],
        [999, 5],
        [1, 6],
        [999, 6],
        [1, 7],
      ]
      for (const [ms, total] of expected) {
        advance(ms)
        await sendSome(20)
        expect(requests()).toBe(total)
      }

      // A success clears the backoff: the next failure waits only initialRetryTime again.
      cluster.recover('Metadata')
      advance(1000)
      await sendSome(5)
      expect(requests()).toBe(8)
      cluster.outage('Metadata', { errorCode: 7 })
      ageMetadata()
      await sendSome(5)
      expect(requests()).toBe(9)
      advance(100)
      await sendSome(5)
      expect(requests()).toBe(10)

      expect(unhandled).toEqual([])
      cluster.assertClean()
    })
  })

  it('waits at least 10 ms between failed background refreshes when initialRetryTime is 0', async () => {
    const setup = await warmProducer({ retry: { ...RETRY, initialRetryTime: 0 } })
    producer = setup.producer
    const { cluster } = setup
    freeze()
    const before = cluster.tally('Metadata')
    cluster.outage('Metadata', { errorCode: 7 })
    ageMetadata()
    const requests = () => cluster.tally('Metadata') - before
    const sendSome = async (count) => {
      for (let i = 0; i < count; i++) await within(send(producer, i), 'a send on aged metadata')
      await turns()
    }

    await sendSome(20)
    expect(requests()).toBe(1)
    // 10 ms after the first failure, then 20, 40, 80, then capped at 100.
    for (const [ms, total] of [
      [9, 1],
      [1, 2],
      [19, 2],
      [1, 3],
      [39, 3],
      [1, 4],
      [79, 4],
      [1, 5],
      [99, 5],
      [1, 6],
      [99, 6],
      [1, 7],
    ]) {
      advance(ms)
      await sendSome(10)
      expect(requests()).toBe(total)
    }
    cluster.assertClean()
  })

  it('still waits for the refresh, and fails with its error, when the topic is not in the metadata', async () => {
    const setup = await warmProducer()
    producer = setup.producer
    const { cluster } = setup
    const other = 'aged-metadata-other-topic'
    const held = hold()
    cluster.answer('Metadata', () => held.promise)
    ageMetadata()

    let settled = false
    const pending = producer
      .send({ topic: other, messages: [{ value: 'x' }] })
      .finally(() => (settled = true))
    await turns()
    expect(settled).toBe(false)

    held.release({ errorCode: 31, ...metadataAnswer() })
    await expect(within(pending, 'the send to an unknown topic')).rejects.toMatchObject({
      type: 'CLUSTER_AUTHORIZATION_FAILED',
    })
  })

  it('waits for the first metadata of a producer and fails with its error', async () => {
    const cluster = producerScript(createScriptedCluster({ nodes: NODES }))
    cluster.outage('Metadata', { errorCode: 31 })
    const kafka = new Kafka(
      cluster.kafkaOptions({ metadataRecoveryStrategy: 'none', retry: RETRY })
    )
    producer = kafka.producer({ metadataMaxAge: MAX_AGE })
    await producer.connect().catch(() => {})

    await expect(within(send(producer, 0), 'the send without metadata')).rejects.toMatchObject({
      type: 'CLUSTER_AUTHORIZATION_FAILED',
    })
  })

  it('awaits a refresh after a not-leader answer and retries on the new leader', async () => {
    const setup = await warmProducer()
    producer = setup.producer
    const { cluster, produced } = setup
    let metadataCalls = 0
    cluster.answer('Metadata', () =>
      metadataCalls++ === 0 ? new Promise(() => {}) : metadataAnswer({ leader: 1, epoch: 6 })
    )
    cluster.answer('Produce', ({ topics }, record) => ({
      responses: topics.map(({ partitions, ...key }) => ({
        ...key,
        partitions: partitions.map(({ partition }) => ({
          partition,
          errorCode: record.node === 0 ? 6 : 0,
          baseOffset: '0',
          logAppendTime: '-1',
          logStartOffset: '0',
          leader: null,
        })),
      })),
      nodeEndpoints: [],
    }))
    ageMetadata()
    const start = produced.length

    await within(send(producer, 1), 'the send moved to the new leader', 4000)

    expect(produced.slice(start).map(({ node }) => node)).toEqual([0, 1])
    cluster.assertClean()
  })

  it('uses the new partitions of a known topic once the background refresh installs them', async () => {
    const setup = await warmProducer({
      createPartitioner:
        () =>
        ({ partitionMetadata }) =>
          partitionMetadata.length - 1,
    })
    producer = setup.producer
    const { cluster, produced } = setup
    const held = hold()
    cluster.answer('Metadata', () => held.promise)
    ageMetadata()
    const partitionsOfOneSend = async () => {
      const start = produced.length
      await within(send(producer, 0), 'a send')
      return produced.slice(start).flatMap(({ partitions }) => partitions)
    }

    expect(await partitionsOfOneSend()).toEqual([0])
    held.release(metadataAnswer({ partitions: 3 }))
    await turns()
    expect(await partitionsOfOneSend()).toEqual([2])
    cluster.assertClean()
  })

  it('lets a disconnect end a background refresh within the request drain and installs nothing after it', async () => {
    const clusterOf = captureCluster()
    const setup = await warmProducer({ requestTimeout: 500 })
    const { cluster } = setup
    const pool = clusterOf().brokerPool
    const held = hold()
    cluster.answer('Metadata', () => held.promise)
    ageMetadata()
    await within(send(setup.producer, 1), 'a send on aged metadata')
    expect(metadataInFlight(cluster)).toBe(1)

    await within(setup.producer.disconnect(), 'the disconnect', 1500)
    held.release(metadataAnswer({ partitions: 3 }))
    await turns()

    expect(cluster.tally('Metadata', 'answered')).toBe(cluster.tally('Metadata') - 1)
    expect(metadataInFlight(cluster)).toBe(0)
    expect(pool.metadata).toBeNull()
    expect(pool.brokers).toEqual({})
    expect(unhandled).toEqual([])
  })

  it('installs nothing from a background refresh answered during the disconnect drain, and the first send after a reconnect fetches metadata again', async () => {
    const clusterOf = captureCluster()
    const setup = await warmProducer({
      requestTimeout: 2000,
      createPartitioner:
        () =>
        ({ partitionMetadata }) =>
          partitionMetadata.length - 1,
    })
    producer = setup.producer
    const { cluster, produced } = setup
    const pool = clusterOf().brokerPool
    const held = hold()
    cluster.answer('Metadata', () => held.promise)
    ageMetadata()
    await within(send(producer, 1), 'a send on aged metadata')
    expect(metadataInFlight(cluster)).toBe(1)

    const closing = producer.disconnect()
    await turns(5)
    held.release(metadataAnswer({ partitions: 3 }))
    await within(closing, 'the disconnect', 4000)
    await turns()

    expect(cluster.tally('Metadata', 'answered')).toBe(cluster.tally('Metadata'))
    expect(pool.metadata).toBeNull()
    expect(pool.brokers).toEqual({})

    cluster.answer('Metadata', () => metadataAnswer({ partitions: 2 }))
    await producer.connect()
    expect(pool.metadata).toBeNull()
    const before = cluster.tally('Metadata')
    const start = produced.length
    await within(send(producer, 2), 'the send after the reconnect')
    expect(cluster.tally('Metadata')).toBe(before + 1)
    expect(produced.slice(start).flatMap(({ partitions }) => partitions)).toEqual([1])
    cluster.assertClean()
  })

  it('keeps the call order of idempotent sends when the refresh installs mid-drain', async () => {
    const setup = await warmProducer()
    producer = setup.producer
    const { cluster, produced } = setup
    const held = hold()
    cluster.answer('Metadata', () => held.promise)
    ageMetadata()
    const start = produced.length

    const first = Array.from({ length: 10 }, (_, i) => send(producer, i))
    await within(Promise.all(first), 'sends before the refresh installs')
    const second = Array.from({ length: 10 }, (_, i) => send(producer, 10 + i))
    held.release(metadataAnswer({ epoch: 6 }))
    const third = Array.from({ length: 10 }, (_, i) => send(producer, 20 + i))
    await within(Promise.all([...second, ...third]), 'sends around the install')

    expect(produced.slice(start).flatMap(({ seqs }) => seqs)).toEqual(
      Array.from({ length: 30 }, (_, i) => i)
    )
    cluster.assertClean()
  })
})

describe('Cluster > a refresh across a disconnect', () => {
  afterEach(() => jest.restoreAllMocks())

  it('does not join a caller after a reconnect to a refresh begun before the disconnect, and installs its own answer', async () => {
    const scripted = producerScript(createScriptedCluster({ nodes: NODES }))
    scripted.answer('Metadata', () => metadataAnswer())
    const clusterOf = captureCluster()
    const admin = new Kafka(
      scripted.kafkaOptions({
        metadataRecoveryStrategy: 'none',
        retry: { initialRetryTime: 3000, retries: 1, factor: 0, multiplier: 1 },
      })
    ).admin()
    await admin.connect()
    const cluster = clusterOf()
    cluster.targetTopics.add(TOPIC)
    const leaderless = metadataAnswer()
    leaderless.topics[0].errorCode = 5
    scripted.answer('Metadata', () => leaderless)
    const old = cluster.refreshMetadata().catch(() => {})
    await turns()
    try {
      await admin.disconnect()
      scripted.answer('Metadata', () => metadataAnswer({ partitions: 2 }))
      await admin.connect()
      await within(cluster.refreshMetadata(), 'the refresh after the reconnect', 1500)

      expect(cluster.findTopicPartitionMetadata(TOPIC)).toHaveLength(2)
    } finally {
      await admin.disconnect()
      await old
    }
  })
})

describe('Cluster > refreshMetadataIfNecessary without the producer option', () => {
  /** @returns {{ cluster: Cluster, held: { promise: Promise<any>, release: Function } }} */
  const agedCluster = () => {
    const cluster = new Cluster({
      ...connectionOpts(),
      logger: newLogger(),
      metadataMaxAge: MAX_AGE,
      retry: { retries: 0 },
    })
    cluster.targetTopics.add(TOPIC)
    const pool = cluster.brokerPool
    pool.metadata = { brokers: [], topicMetadata: [{ topic: TOPIC, partitionMetadata: [] }] }
    pool.metadataExpireAt = Date.now() - 1
    const held = hold()
    pool.refreshMetadata = jest.fn(() => held.promise)
    return { cluster, held }
  }

  afterEach(() => jest.restoreAllMocks())

  it.each([
    ['consumer and admin callers', (cluster) => cluster.refreshMetadataIfNecessary()],
    ['a scoped caller', (cluster) => cluster.refreshMetadataIfNecessary({ isCurrent: () => true })],
  ])('%s still wait for the refresh of aged metadata', async (_name, call) => {
    const { cluster, held } = agedCluster()
    let settled = false
    const pending = call(cluster).finally(() => (settled = true))
    await turns(5)

    expect(cluster.brokerPool.refreshMetadata).toHaveBeenCalledTimes(1)
    expect(settled).toBe(false)
    held.release()
    await pending
    expect(settled).toBe(true)
  })
})
