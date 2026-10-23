const createSendMessages = require('../sendMessages')
const createEosManager = require('../eosManager')
const createProducer = require('../index')
const Idempotence = require('../idempotence')
const Accumulator = require('../state/accumulator')
const createRetry = require('../../retry')
const Generation = require('../generation')
const Broker = require('../../broker')
const Cluster = require('../../cluster')
const Connection = require('../../network/connection')
const { lookup } = require('../../protocol/requests')
const { responseHeaderVersion } = require('../../protocol/headers')
const { CONNECTION_STATUS } = require('../../network/connectionStatus')
const { partitionResults } = require('../../protocol/requests/produce/partitionResults')
const { createErrorFromCode } = require('../../protocol/error')
const apiKeys = require('../../protocol/requests/apiKeys')
const Decoder = require('../../protocol/decoder')
const Encoder = require('../../protocol/encoder')
const { INT_32_MAX_VALUE } = require('../../constants')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const {
  KafkaJSConnectionClosedError,
  KafkaJSDeliveryTimeout,
  KafkaJSError,
  KafkaJSInvariantViolation,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSProtocolError,
} = require('../../errors')
const {
  READY,
  IN_TRANSACTION,
  ABORTING,
  ABORTABLE_ERROR,
} = require('../eosManager/transactionStates')
const {
  METADATA_ORDER_CASE,
  captureMetadataOrder,
} = require('../../../testHelpers/compatBaseline/capture')
const capturedMetadataOrder = require('../../../testHelpers/compatBaseline/nonIdempotentMetadataOrder.json')
const {
  newLogger,
  createFakeSocketFactory,
  respondWithFrame,
  readRequestHeader,
} = require('testHelpers')

const TOPIC = 'topic-a'
const OTHER_TOPIC = 'topic-b'
const RETRY = { retries: 3, initialRetryTime: 10, maxRetryTime: 40, factor: 0, multiplier: 2 }
/**
 * Seeds of the randomized case. With several batches of a partition allowed in flight, each of
 * them reorders a partition.
 */
const FUZZ_SEEDS = [12, 14, 37, 47, 50, 58]

const NOT_LEADER_OR_FOLLOWER = 6
const REQUEST_TIMED_OUT = 7
const OUT_OF_ORDER_SEQUENCE_NUMBER = 45
const DUPLICATE_SEQUENCE_NUMBER = 46
const INVALID_RECORD = 87

/** The longest delay `setTimeout` honours, in ms. */
const MAX_TIMER_DELAY = 2147483647

/** Routes every record to the partition it names. */
const byMessage = ({ message }) => message.partition

/** @returns {{ partition: number, value: string }[]} `count` records for `partition` */
const records = (partition, count = 1) =>
  Array.from({ length: count }, (_, index) => ({ partition, value: `value-${partition}-${index}` }))

/** A promise and the function that resolves it. */
const deferred = () => {
  let resolve
  const promise = new Promise((done) => {
    resolve = done
  })
  return { promise, resolve }
}

/** Lets every pending promise callback and queued microtask run. */
const settle = () => new Promise((done) => setImmediate(done))

/**
 * @param {Promise} promise
 * @returns {() => boolean} whether `promise` has settled
 */
const settledFlag = (promise) => {
  let settled = false
  const flag = () => {
    settled = true
  }
  promise.then(flag, flag)
  return () => settled
}

/**
 * A partition log on the broker for one producer, by the sequence rules of Kafka 4.3's
 * `ProducerAppendInfo` (non-transactional) and its window of the last 5 batches, which answers a
 * re-sent batch as a duplicate: a partition the producer never wrote accepts any sequence, a new
 * epoch must start at 0, and otherwise the next batch must follow the last one.
 * @returns {{ append: (key: string, epoch: number, first: number, count: number, value: string)
 *   => number|'duplicate', written: string[] }} `append` answers 0, 45 or `'duplicate'`
 */
const createPartitionLog = () => {
  const partitions = new Map()
  const written = []
  const append = (key, epoch, first, count, value) => {
    const last = first + count - 1
    const entry = partitions.get(key)
    if (entry !== undefined) {
      if (entry.epoch === epoch && entry.window.some((b) => b.first === first && b.last === last)) {
        return 'duplicate'
      }
      if (epoch !== entry.epoch ? first !== 0 : first !== entry.last + 1) {
        return OUT_OF_ORDER_SEQUENCE_NUMBER
      }
    }

    const window = entry !== undefined && entry.epoch === epoch ? entry.window : []
    window.push({ first, last })
    if (window.length > 5) {
      window.shift()
    }
    partitions.set(key, { epoch, last, window })
    written.push(`${key}:${value}`)
    return 0
  }
  return { append, written }
}

/**
 * The partitions of a Produce v3-v7 request body, with the producer epoch, first sequence and
 * record count of each batch (the record batch header's fixed fields).
 * @see https://kafka.apache.org/43/design/protocol
 * @param {Buffer} body
 * @returns {{ topic: string, partition: number, epoch: number, firstSequence: number,
 *   count: number }[]}
 */
const readProduceRequest = (body) => {
  const decoder = new Decoder(body)
  decoder.readString()
  decoder.readInt16()
  decoder.readInt32()
  return decoder
    .readArray((topics) => {
      const topic = topics.readString()
      return topics.readArray((partitions) => {
        const partition = partitions.readInt32()
        const batch = partitions.readBytes()
        return {
          topic,
          partition,
          epoch: batch.readInt16BE(51),
          firstSequence: batch.readInt32BE(53),
          count: batch.readInt32BE(57),
        }
      })
    })
    .flat()
}

/**
 * A Produce v7 response body answering each partition with its error code.
 * @param {{ topic: string, partition: number, errorCode: number }[]} partitions
 * @returns {Buffer}
 */
const produceResponseV7 = (partitions) =>
  new Encoder()
    .writeArray(
      [...new Set(partitions.map(({ topic }) => topic))].map((topic) =>
        new Encoder().writeString(topic).writeArray(
          partitions
            .filter((answer) => answer.topic === topic)
            .map(({ partition, errorCode }) =>
              new Encoder()
                .writeInt32(partition)
                .writeInt16(errorCode)
                .writeInt64(errorCode === 0 ? 0 : -1)
                .writeInt64(-1)
                .writeInt64(0)
            )
        )
      )
    )
    .writeInt32(0).buffer

/**
 * The per-partition answer to a Produce request, as `Broker#produce({ perPartitionResults: true })`
 * resolves it: success for every partition, except those `errors` names with an error code.
 * @param {{ topicData: { topic: string, partitions: { partition: number }[] }[] }} request
 * @param {number} nodeId
 * @param {{ [topicPartition: string]: number }} [errors] `'topic/partition'` to error code
 * @param {string} [logStartOffset='0']
 * @returns {object[]}
 */
const respond = ({ topicData }, nodeId, errors = {}, logStartOffset = '0') =>
  partitionResults({
    topics: topicData.map(({ topic, partitions }) => ({
      topicName: topic,
      partitions: partitions.map(({ partition }) => {
        const errorCode = errors[`${topic}/${partition}`] ?? 0
        return {
          partition,
          errorCode,
          baseOffset: errorCode === 0 ? String(nodeId * 1000 + partition) : '-1',
          logAppendTime: '-1',
          logStartOffset,
        }
      }),
    })),
  })

/**
 * A seeded pseudo-random generator (mulberry32), so a randomized case replays exactly.
 * @param {number} seed
 * @returns {() => number} a number in [0, 1) per call
 */
const seededRandom = (seed) => () => {
  seed = (seed + 0x6d2b79f5) | 0
  let t = Math.imul(seed ^ (seed >>> 15), 1 | seed)
  t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296
}

/**
 * A real `Broker` and `Connection` over a fake socket whose answers are released by hand, in
 * front of a partition log that checks sequences: what the broker processes is appended to `log`
 * in the order the requests were written.
 * @param {ReturnType<typeof createPartitionLog>} log
 * @returns {{ broker: Broker, answer: () => void, drop: () => void, destroy: () => Promise<void> }}
 *   `answer` processes and answers everything written so far; `drop` ends the socket, so nothing
 *   written so far is processed
 */
const createDroppingBroker = (log) => {
  const { factory, state } = createFakeSocketFactory()
  const connection = new Connection({
    host: '127.0.0.1',
    port: 9092,
    logger: newLogger(),
    socketFactory: factory,
    clientId: 'test-client',
    connectionTimeout: 1000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
  })
  const versions = { [apiKeys.Produce]: { minVersion: 3, maxVersion: 7 } }
  const pool = {
    host: '127.0.0.1',
    port: 9092,
    sasl: null,
    connectionTimeout: 1000,
    generation: 0,
    isConnected: () => connection.isConnected(),
    isAuthenticated: () => true,
    getConnection: async () => {
      if (!connection.isConnected()) {
        await connection.connect()
      }
      return connection
    },
    getConnectionByRequest: async () => pool.getConnection(),
    send: async (protocolRequest) => (await pool.getConnection()).send(protocolRequest),
    setVersions: () => {},
    destroy: async () => connection.disconnect(),
    retire: () => {},
  }
  const broker = new Broker({ connectionPool: pool, logger: newLogger(), versions })
  broker.lookupRequest = lookup(versions)
  broker.versionsGeneration = pool.generation
  let processed = 0

  return {
    broker,
    answer: () => {
      while (processed < state.writes.length) {
        const frame = state.writes[processed]
        processed += 1
        const header = readRequestHeader(frame)
        const partitions = readProduceRequest(frame.subarray(header.byteLength)).map(
          ({ topic, partition, epoch, firstSequence, count }) => {
            const code = log.append(
              `${topic}/${partition}`,
              epoch,
              firstSequence,
              count,
              `${firstSequence}@e${epoch}`
            )
            return {
              topic,
              partition,
              errorCode: code === 'duplicate' ? DUPLICATE_SEQUENCE_NUMBER : code,
            }
          }
        )
        respondWithFrame(
          state,
          header.correlationId,
          responseHeaderVersion(header.apiKey, header.apiVersion),
          produceResponseV7(partitions)
        )
      }
    },
    drop: () => {
      processed = state.writes.length
      state.socket.emit('end')
    },
    destroy: () => connection.disconnect(),
  }
}

/**
 * The answers of an unreliable cluster, for `createHarness({ answer })`. Each node processes a
 * request when it arrives and answers its requests in arrival order, each after a random delay of
 * 0-3 ms, against `log`. It loses some connections: the request a drop hits, and every request
 * that reaches the node before the client has seen that drop, are failed unprocessed - or, for
 * some drops, processed and then failed. A node that stops leading a partition refuses its batches
 * (NOT_LEADER_OR_FOLLOWER) until the client has seen a refusal. Some written batches are answered
 * REQUEST_TIMED_OUT, as when an answer is lost.
 * @param {() => number} random
 * @param {ReturnType<typeof createPartitionLog>} log
 * @returns {{ answer: Function, maxOutstanding: () => number }}
 */
const createUnreliableCluster = (random, log) => {
  const outstanding = new Map()
  const answeredAfter = new Map()
  const dropped = new Set()
  const refusing = new Set()
  let maxOutstanding = 0

  const answer = async (request, nodeId) => {
    outstanding.set(nodeId, (outstanding.get(nodeId) ?? 0) + 1)
    maxOutstanding = Math.max(maxOutstanding, outstanding.get(nodeId))

    const deadOnArrival = dropped.has(nodeId)
    const roll = deadOnArrival ? 0 : random()
    const drops = !deadOnArrival && (roll < 0.03 || roll > 0.985)
    if (drops) {
      dropped.add(nodeId)
    }

    const errors = {}
    const refusals = []
    if (roll >= 0.03) {
      for (const { topic, partitions } of request.topicData) {
        for (const { partition, firstSequence, messages } of partitions) {
          const key = `${topic}/${partition}`
          const fate = random()
          if (fate < 0.05 || refusing.has(key)) {
            if (!refusing.has(key)) {
              refusing.add(key)
              refusals.push(key)
            }
            errors[key] = NOT_LEADER_OR_FOLLOWER
            continue
          }
          const values = messages.map(({ value }) => value).join('+')
          const code = log.append(
            key,
            request.producerEpoch,
            firstSequence,
            messages.length,
            values
          )
          if (code === 'duplicate') {
            errors[key] = DUPLICATE_SEQUENCE_NUMBER
          } else if (code !== 0 || fate < 0.1) {
            errors[key] = code || REQUEST_TIMED_OUT
          }
        }
      }
    }

    const previous = answeredAfter.get(nodeId) ?? Promise.resolve()
    const answered = previous.then(
      () => new Promise((resolve) => setTimeout(resolve, Math.floor(random() * 4)))
    )
    answeredAfter.set(nodeId, answered)
    await answered

    outstanding.set(nodeId, outstanding.get(nodeId) - 1)
    refusals.forEach((key) => refusing.delete(key))
    if (drops) {
      dropped.delete(nodeId)
    }
    if (roll < 0.03 || roll > 0.985) {
      throw new KafkaJSConnectionClosedError('Connection closed', { host: 'h', port: nodeId })
    }
    return respond(request, nodeId, errors)
  }

  return { answer, maxOutstanding: () => maxOutstanding }
}

/**
 * An idempotent send path over a fake cluster whose partitions are led as `layout` says, with the
 * real eosManager and state manager. Every Produce is recorded and answered by `answer`.
 * @param {object} [options]
 * @param {{ [topic: string]: { [partition: number]: number } }} [options.layout] partition to the
 *   node leading it; mutable through the returned `layout`
 * @param {(request: object, nodeId: number) => Promise<object[]>|object[]} [options.answer]
 * @param {boolean} [options.transactional=false]
 * @param {object} [options.retry]
 * @param {number} [options.deliveryTimeout]
 * @param {Function} [options.partitioner]
 * @param {Generation} [options.generation] the producer lifecycle every send runs under
 */
const createHarness = ({
  layout = { [TOPIC]: { 0: 1, 1: 2 } },
  answer = (request, nodeId) => respond(request, nodeId),
  transactional = false,
  retry = RETRY,
  deliveryTimeout,
  partitioner = byMessage,
  generation,
} = {}) => {
  const logger = newLogger()
  const produced = []
  const brokers = new Map()
  const coordinator = {
    nodeId: 9,
    initProducerId: jest.fn(async () => ({ producerId: '1000', producerEpoch: 0 })),
    addPartitionsToTxn: jest.fn(async () => ({})),
    endTxn: jest.fn(async () => ({})),
  }
  const brokerFor = (nodeId) => {
    const id = Number(nodeId)
    if (!brokers.has(id)) {
      brokers.set(id, {
        nodeId: id,
        produce: jest.fn(async (request) => {
          const refusal = request.guard?.(100)
          if (refusal) throw refusal
          produced.push({ nodeId: id, request })
          return answer(request, id)
        }),
      })
    }
    return brokers.get(id)
  }
  const leaderOf = (topic, partition) => (layout[topic] || {})[partition]
  const cluster = {
    targetTopics: new Set(),
    addMultipleTargetTopics: jest.fn(async () => {}),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    refreshMetadata: jest.fn(async () => {}),
    isConnected: jest.fn(() => true),
    connect: jest.fn(async () => {}),
    findTopicPartitionMetadata: (topic) =>
      Object.keys(layout[topic] || {}).map((id) => ({
        partitionId: Number(id),
        leader: leaderOf(topic, id),
      })),
    findLeaderForPartitions: (topic, ids) =>
      ids.reduce((result, id) => {
        const leader = leaderOf(topic, id)
        return leader === undefined
          ? result
          : { ...result, [leader]: [...(result[leader] || []), Number(id)] }
      }, {}),
    findBroker: jest.fn(async ({ nodeId }) => brokerFor(nodeId)),
    findConnectedBroker: jest.fn(async () => coordinator),
    findGroupCoordinator: jest.fn(async () => coordinator),
    topicId: () => undefined,
    removeBroker: jest.fn(),
    applyLeaderHint: jest.fn(async () => true),
  }
  const eosManager = createEosManager({
    logger,
    cluster,
    transactional,
    transactionalId: transactional ? 'transactional-a' : undefined,
    retry,
  })
  let status = CONNECTION_STATUS.CONNECTED
  const sendMessages = createSendMessages({
    logger,
    cluster,
    retrier: createRetry(retry),
    partitioner,
    eosManager,
    idempotence: new Idempotence({ enabled: true, explicit: false, logger }),
    retry,
    deliveryTimeout,
    getConnectionStatus: () => status,
    ...(generation ? { getGeneration: () => generation } : {}),
  })

  /** Initializes the producer id, and begins a transaction for a transactional producer. */
  const start = async () => {
    await eosManager.initProducerId()
    if (transactional) {
      await eosManager.beginTransaction()
    }
  }

  /**
   * One send of `topicMessages`, with the request-level options a send() passes, as the producer
   * makes it: the records assigned to partitions once, then those batches sent, their delivery
   * deadline counted from the send's start.
   */
  const send = async (topicMessages, { acks = -1, timeout = 30000, compression = 0 } = {}) => {
    const createdAt = Date.now()
    const batches = await sendMessages.partition(topicMessages)
    for (const batch of batches) {
      batch.createdAt = createdAt
    }
    return sendMessages.sendPartitioned({ acks, timeout, compression, batches })
  }

  /** The partitions, with their sequences and producer epoch, each recorded request carried. */
  const sent = () =>
    produced.map(({ nodeId, request }) => ({
      nodeId,
      epoch: request.producerEpoch,
      partitions: request.topicData.flatMap(({ topic, partitions }) =>
        partitions.map(({ partition, firstSequence }) => `${topic}/${partition}@${firstSequence}`)
      ),
    }))

  return {
    cluster,
    coordinator,
    eosManager,
    manager: eosManager.stateManager,
    sendMessages,
    layout,
    produced,
    broker: brokerFor,
    start,
    send,
    sent,
    setStatus: (next) => {
      status = next
    },
  }
}

/**
 * Points `cluster` at the real `Cluster` lookups over one topic's metadata, so a partition whose
 * leader is negative makes the lookup throw as it does in production.
 * @param {object} cluster the harness's cluster
 * @param {string} topic
 * @param {{ partitionId: number, leader: number }[]} partitionMetadata
 */
const useClusterLookups = (cluster, topic, partitionMetadata) => {
  const metadataCache = {
    brokerPool: { metadata: { topicMetadata: [{ topic, partitionMetadata }] } },
  }
  metadataCache.findTopicPartitionMetadata =
    Cluster.prototype.findTopicPartitionMetadata.bind(metadataCache)
  cluster.findTopicPartitionMetadata = metadataCache.findTopicPartitionMetadata
  cluster.findLeaderForPartitions = Cluster.prototype.findLeaderForPartitions.bind(metadataCache)
}

describe('Producer > the idempotent drain loop', () => {
  afterEach(() => {
    jest.useRealTimers()
  })

  describe('pipelining and drain rules', () => {
    test('sends to different partitions go out before the first is answered; a partition’s next batch waits for its answer', async () => {
      const answers = []
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1, 1: 1 } },
        answer: (request, nodeId) => {
          const held = deferred()
          answers.push(() => held.resolve(respond(request, nodeId)))
          return held.promise
        },
      })
      await harness.start()

      const first = harness.send([{ topic: TOPIC, messages: records(0, 2) }])
      await settle()
      const other = harness.send([{ topic: TOPIC, messages: records(1) }])
      await settle()
      const next = harness.send([{ topic: TOPIC, messages: records(0, 3) }])
      await settle()

      expect(harness.sent()).toEqual([
        { nodeId: 1, epoch: 0, partitions: ['topic-a/0@0'] },
        { nodeId: 1, epoch: 0, partitions: ['topic-a/1@0'] },
      ])
      expect(harness.manager.sequencer.entry(TOPIC, 0).queued[0].firstSequence).toBeNull()

      answers.shift()()
      await expect(first).resolves.toEqual([expect.objectContaining({ partition: 0 })])
      await settle()
      expect(harness.sent()[2]).toEqual({ nodeId: 1, epoch: 0, partitions: ['topic-a/0@2'] })

      answers.forEach((release) => release())
      await expect(other).resolves.toEqual([expect.objectContaining({ partition: 1 })])
      await expect(next).resolves.toEqual([expect.objectContaining({ partition: 0 })])
    })

    test('a partition not yet in the transaction stays queued while its sibling goes out on the same pass', async () => {
      const harness = createHarness({ transactional: true, layout: { [TOPIC]: { 0: 1, 1: 1 } } })
      await harness.start()
      await harness.send([{ topic: TOPIC, messages: records(0) }])
      const added = deferred()
      harness.coordinator.addPartitionsToTxn.mockImplementationOnce(() => added.promise)

      const both = harness.send([{ topic: TOPIC, messages: [...records(0), ...records(1)] }])
      await settle()

      expect(harness.sent()).toEqual([
        { nodeId: 1, epoch: 0, partitions: ['topic-a/0@0'] },
        { nodeId: 1, epoch: 0, partitions: ['topic-a/0@1'] },
      ])

      added.resolve({})
      await expect(both).resolves.toHaveLength(2)
      expect(harness.sent()[2]).toEqual({ nodeId: 1, epoch: 0, partitions: ['topic-a/1@0'] })
    })

    test('a send resolves with a RecordMetadata of exactly six keys, whatever else the broker answered with', async () => {
      const harness = createHarness()
      await harness.start()

      const [metadata] = await harness.send([{ topic: TOPIC, messages: records(0) }])

      expect(Object.keys(metadata).sort()).toEqual([
        'baseOffset',
        'errorCode',
        'logAppendTime',
        'logStartOffset',
        'partition',
        'topicName',
      ])
    })

    test('two concurrent sends over the same two partitions in opposite order both complete', async () => {
      const harness = createHarness()
      await harness.start()

      const results = await Promise.all([
        harness.send([{ topic: TOPIC, messages: [...records(0), ...records(1)] }]),
        harness.send([{ topic: TOPIC, messages: [...records(1), ...records(0)] }]),
      ])

      expect(results.map((metadata) => metadata.map(({ partition }) => partition))).toEqual([
        [0, 1],
        [0, 1],
      ])
    })

    test('one request per broker and options triple: differing compression is never merged', async () => {
      const harness = createHarness({ layout: { [TOPIC]: { 0: 1, 1: 1 } } })
      await harness.start()

      await Promise.all([
        harness.sendMessages.sendPartitioned({
          acks: -1,
          timeout: 30000,
          compression: 0,
          batches: [{ topic: TOPIC, partition: 0, messages: records(0) }],
        }),
        harness.sendMessages.sendPartitioned({
          acks: -1,
          timeout: 30000,
          compression: 1,
          batches: [{ topic: TOPIC, partition: 1, messages: records(1) }],
        }),
      ])

      expect(harness.produced.map(({ request }) => request.compression)).toEqual([0, 1])
      expect(harness.sent().map(({ partitions }) => partitions)).toEqual([
        ['topic-a/0@0'],
        ['topic-a/1@0'],
      ])
    })
  })

  describe('the retry unit is the partition batch', () => {
    test('a re-send re-uses its batch: the partitioner does not run again, even after the partition count grew', async () => {
      const partitioner = jest.fn(byMessage)
      let calls = 0
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1 } },
        partitioner,
        answer: (request, nodeId) => {
          calls += 1
          if (calls === 1) {
            harness.layout[TOPIC][1] = 1
            return respond(request, nodeId, { 'topic-a/0': NOT_LEADER_OR_FOLLOWER })
          }
          return respond(request, nodeId)
        },
      })
      await harness.start()
      const messages = records(0, 3)

      await expect(harness.send([{ topic: TOPIC, messages }])).resolves.toEqual([
        expect.objectContaining({ topicName: TOPIC, partition: 0 }),
      ])

      expect(partitioner).toHaveBeenCalledTimes(3)
      expect(harness.sent()).toEqual([
        { nodeId: 1, epoch: 0, partitions: ['topic-a/0@0'] },
        { nodeId: 1, epoch: 0, partitions: ['topic-a/0@0'] },
      ])
      const [first, second] = harness.produced.map(({ request }) => request.topicData[0])
      expect(second.partitions[0].messages).toBe(first.partitions[0].messages)
    })

    test('a send settles after every batch: metadata by topic then partition, or the failed batch’s error', async () => {
      const answers = new Map()
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1, 1: 2 }, [OTHER_TOPIC]: { 0: 3 } },
        answer: (request) => {
          const held = deferred()
          answers.set(request.topicData[0].topic + request.topicData[0].partitions[0].partition, {
            request,
            held,
          })
          return held.promise
        },
      })
      await harness.start()
      const answer = (key, errors) => {
        const { request, held } = answers.get(key)
        held.resolve(respond(request, 1, errors))
      }

      let settled = false
      const success = harness
        .send([
          { topic: OTHER_TOPIC, messages: records(0) },
          { topic: TOPIC, messages: [...records(1), ...records(0)] },
        ])
        .finally(() => {
          settled = true
        })
      await settle()
      answer(`${TOPIC}1`)
      answer(`${OTHER_TOPIC}0`)
      await settle()
      expect(settled).toBe(false)
      answer(`${TOPIC}0`)

      expect(
        (await success).map(({ topicName, partition }) => `${topicName}/${partition}`)
      ).toEqual(['topic-a/0', 'topic-a/1', 'topic-b/0'])

      answers.clear()
      settled = false
      const failure = harness
        .send([
          { topic: OTHER_TOPIC, messages: records(0) },
          { topic: TOPIC, messages: [...records(1), ...records(0)] },
        ])
        .catch((error) => error)
        .finally(() => {
          settled = true
        })
      await settle()
      answer(`${TOPIC}1`, { 'topic-a/1': INVALID_RECORD })
      await settle()
      expect(settled).toBe(false)
      answer(`${OTHER_TOPIC}0`, { 'topic-b/0': INVALID_RECORD })
      answer(`${TOPIC}0`)

      const error = await failure
      expect(error).toBeInstanceOf(KafkaJSProtocolError)
      expect(error).toMatchObject({
        code: INVALID_RECORD,
        category: 'invalidConfiguration',
        topic: TOPIC,
        partition: 1,
      })
      expect(harness.manager.sequencer.entry(TOPIC, 0)).toMatchObject({ lastAckedSequence: 1 })
    })

    test('a retriable error that outlasts retry.retries rejects with KafkaJSNumberOfRetriesExceeded', async () => {
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1 } },
        answer: (request, nodeId) => respond(request, nodeId, { 'topic-a/0': REQUEST_TIMED_OUT }),
      })
      await harness.start()

      const error = await harness.send([{ topic: TOPIC, messages: records(0, 2) }]).catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(error).toMatchObject({ retryCount: 3, code: REQUEST_TIMED_OUT })
      expect(error.cause).toMatchObject({ code: REQUEST_TIMED_OUT })
      expect(harness.produced).toHaveLength(4)
      expect(harness.sent().every(({ partitions }) => partitions[0] === 'topic-a/0@0')).toBe(true)
    })

    test('a non-retriable answer on the last retry rejects with its own error', async () => {
      let calls = 0
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1 } },
        retry: { ...RETRY, retries: 1 },
        answer: (request, nodeId) => {
          calls += 1
          const code = calls === 1 ? REQUEST_TIMED_OUT : INVALID_RECORD
          return respond(request, nodeId, { 'topic-a/0': code })
        },
      })
      await harness.start()

      const error = await harness.send([{ topic: TOPIC, messages: records(0) }]).catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSProtocolError)
      expect(error).toMatchObject({ code: INVALID_RECORD })
      expect(harness.produced).toHaveLength(2)
    })

    test('a send still assigning partitions when another send turns idempotence off goes out without the producer id, never with it and no sequence', async () => {
      const cluster = createFakeCluster()
      const producer = createProducer({ cluster, logger: newLogger() })
      await producer.connect()
      await producer.send({ topic: TOPIC, messages: [{ value: 'first' }] })
      const partitioned = deferred()
      cluster.refreshMetadataIfNecessary.mockImplementationOnce(() => partitioned.promise)

      const pending = producer.send({ topic: TOPIC, messages: [{ value: 'second' }] })
      await settle()
      await producer.send({ topic: TOPIC, acks: 1, messages: [{ value: 'third' }] })
      partitioned.resolve()
      await pending

      const second = cluster.broker.produce.mock.calls
        .map(([request]) => request)
        .find(({ topicData }) => topicData[0].partitions[0].messages[0].value === 'second')
      expect(producer.isIdempotent()).toBe(false)
      expect(second).toMatchObject({ acks: -1, producerId: -1, producerEpoch: 0 })
      expect(second.topicData[0].partitions[0].firstSequence).toBeUndefined()
    })

    test("a producer's retry options, not the cluster's, bound a batch's re-sends", async () => {
      const cluster = createFakeCluster({
        options: { retry: { retries: 0 } },
        produce: async ({ topicData }) => ({
          topics: topicData.map(({ topic, partitions }) => ({
            topicName: topic,
            partitions: partitions.map(({ partition }) => ({
              partition,
              errorCode: REQUEST_TIMED_OUT,
              baseOffset: '-1',
              logAppendTime: '-1',
              logStartOffset: '0',
            })),
          })),
        }),
      })
      const producer = createProducer({
        cluster,
        logger: newLogger(),
        retry: { retries: 2, initialRetryTime: 1, maxRetryTime: 1 },
      })
      await producer.connect()

      const error = await producer
        .send({ topic: TOPIC, messages: [{ value: 'value' }] })
        .catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(error).toMatchObject({ retryCount: 2, code: REQUEST_TIMED_OUT })
      expect(cluster.broker.produce).toHaveBeenCalledTimes(3)
    })
  })

  describe('verdicts several at a time, the epoch and the sequence', () => {
    test('two next-sequence OUT_OF_ORDER answers in one response bump twice and lose nothing', async () => {
      let calls = 0
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1, 1: 1 } },
        answer: (request, nodeId) => {
          calls += 1
          return calls === 1
            ? respond(request, nodeId, {
                'topic-a/0': OUT_OF_ORDER_SEQUENCE_NUMBER,
                'topic-a/1': OUT_OF_ORDER_SEQUENCE_NUMBER,
              })
            : respond(request, nodeId)
        },
      })
      await harness.start()
      jest.useFakeTimers({ now: 0 })

      const resent = harness.send([{ topic: TOPIC, messages: [...records(0), ...records(1)] }])
      await jest.advanceTimersByTimeAsync(RETRY.initialRetryTime)
      await resent
      await harness.send([{ topic: TOPIC, messages: [...records(0), ...records(1)] }])

      expect(harness.sent()).toEqual([
        { nodeId: 1, epoch: 0, partitions: ['topic-a/0@0', 'topic-a/1@0'] },
        { nodeId: 1, epoch: 2, partitions: ['topic-a/0@0', 'topic-a/1@0'] },
        { nodeId: 1, epoch: 2, partitions: ['topic-a/0@1', 'topic-a/1@1'] },
      ])
    })

    test('a stale partition re-sends under its old epoch in a request of its own, on the same pass', async () => {
      let calls = 0
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1, 1: 1 } },
        answer: (request, nodeId) => {
          calls += 1
          return calls === 1
            ? respond(request, nodeId, {
                'topic-a/0': OUT_OF_ORDER_SEQUENCE_NUMBER,
                'topic-a/1': REQUEST_TIMED_OUT,
              })
            : respond(request, nodeId)
        },
      })
      await harness.start()
      jest.useFakeTimers({ now: 0 })

      const resent = harness.send([{ topic: TOPIC, messages: [...records(0), ...records(1)] }])
      await jest.advanceTimersByTimeAsync(RETRY.initialRetryTime)
      await resent
      await harness.send([{ topic: TOPIC, messages: records(1) }])

      expect(harness.sent()).toEqual([
        { nodeId: 1, epoch: 0, partitions: ['topic-a/0@0', 'topic-a/1@0'] },
        { nodeId: 1, epoch: 1, partitions: ['topic-a/0@0'] },
        { nodeId: 1, epoch: 0, partitions: ['topic-a/1@0'] },
        { nodeId: 1, epoch: 1, partitions: ['topic-a/1@0'] },
      ])
    })

    test('a batch that straddles INT_32_MAX_VALUE is followed by its wrapped sequence', async () => {
      const harness = createHarness({ layout: { [TOPIC]: { 0: 1 } } })
      await harness.start()
      harness.manager.advance(TOPIC, 0, INT_32_MAX_VALUE - 2)

      await harness.send([{ topic: TOPIC, messages: records(0, 5) }])
      await harness.send([{ topic: TOPIC, messages: records(0, 1) }])

      expect(harness.sent().map(({ partitions }) => partitions[0])).toEqual([
        `topic-a/0@${INT_32_MAX_VALUE - 2}`,
        'topic-a/0@2',
      ])
    })

    test('a DUPLICATE_SEQUENCE_NUMBER answer resolves the send with no offset', async () => {
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1 } },
        answer: (request, nodeId) =>
          respond(request, nodeId, { 'topic-a/0': DUPLICATE_SEQUENCE_NUMBER }),
      })
      await harness.start()

      await expect(harness.send([{ topic: TOPIC, messages: records(0, 2) }])).resolves.toEqual([
        {
          topicName: TOPIC,
          partition: 0,
          errorCode: 0,
          baseOffset: '-1',
          logAppendTime: '-1',
          logStartOffset: '0',
        },
      ])
      expect(harness.manager.sequencer.entry(TOPIC, 0)).toMatchObject({
        lastAckedSequence: 1,
        lastAckedOffset: -1,
      })
    })

    test('a partition missing from the response, or answered under an unknown topic id, is re-sent', async () => {
      let calls = 0
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1, 1: 1 } },
        answer: (request, nodeId) => {
          calls += 1
          const results = respond(request, nodeId)
          if (calls === 1) {
            return [{ ...results[0], topicName: null }]
          }
          return results
        },
      })
      await harness.start()
      jest.useFakeTimers({ now: 0 })

      const resent = harness.send([{ topic: TOPIC, messages: [...records(0), ...records(1)] }])
      await jest.advanceTimersByTimeAsync(RETRY.initialRetryTime)
      await expect(resent).resolves.toHaveLength(2)

      expect(harness.sent().map(({ partitions }) => partitions)).toEqual([
        ['topic-a/0@0', 'topic-a/1@0'],
        ['topic-a/0@0', 'topic-a/1@0'],
      ])
    })

    test('an answer that carries no partition at all fails every batch retriably', async () => {
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1 } },
        retry: { ...RETRY, retries: 1 },
        answer: () => undefined,
      })
      await harness.start()

      const error = await harness.send([{ topic: TOPIC, messages: records(0) }]).catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(error.cause).toBeInstanceOf(KafkaJSError)
      expect(error.cause.message).toEqual('No result for topic-a partition 0')
      expect(harness.produced).toHaveLength(2)
    })

    test('an idempotent send that does not ask for acks -1 is refused before anything is queued', async () => {
      const harness = createHarness()
      await harness.start()

      for (const acks of [0, 1]) {
        await expect(
          harness.send([{ topic: TOPIC, messages: records(0) }], { acks })
        ).rejects.toThrow(KafkaJSInvariantViolation)
      }
      expect(harness.produced).toEqual([])
      expect(harness.manager.sequencer.entry(TOPIC, 0)).toBeNull()
    })
  })

  describe('a transaction', () => {
    test('a transactional request carries the transactional id and the transaction version, and no version cap', async () => {
      const harness = createHarness({ transactional: true })
      await harness.start()

      await harness.send([{ topic: TOPIC, messages: records(0) }])

      const [{ request }] = harness.produced
      expect(request).toMatchObject({
        transactionalId: 'transactional-a',
        transactionVersion: 1,
        perPartitionResults: true,
        producerId: '1000',
        producerEpoch: 0,
      })
      expect(request).not.toHaveProperty('maxVersion')
      expect(harness.coordinator.addPartitionsToTxn).toHaveBeenCalledWith(
        expect.objectContaining({ topics: [{ topic: TOPIC, partitions: [0] }] })
      )
    })

    test('a request outside a transaction carries no transactional id', async () => {
      const harness = createHarness()
      await harness.start()

      await harness.send([{ topic: TOPIC, messages: records(0) }])

      expect(harness.produced[0].request).toMatchObject({
        transactionalId: undefined,
        topicIds: undefined,
      })
    })

    test('an error state armed mid-send drains nothing more, and the send rejects once', async () => {
      const added = deferred()
      const answered = deferred()
      const harness = createHarness({
        transactional: true,
        layout: { [TOPIC]: { 0: 1, 1: 1 } },
        answer: async (request, nodeId) => {
          await answered.promise
          return respond(request, nodeId, { 'topic-a/0': OUT_OF_ORDER_SEQUENCE_NUMBER })
        },
      })
      await harness.start()
      harness.coordinator.addPartitionsToTxn
        .mockImplementationOnce(async () => ({}))
        .mockImplementationOnce(() => added.promise)
      const first = harness.sendMessages.sendPartitioned({
        acks: -1,
        timeout: 30000,
        compression: 0,
        batches: [{ topic: TOPIC, partition: 0, messages: records(0) }],
      })
      await settle()
      const second = harness.sendMessages.sendPartitioned({
        acks: -1,
        timeout: 30000,
        compression: 0,
        batches: [{ topic: TOPIC, partition: 1, messages: records(1) }],
      })
      await settle()

      answered.resolve()
      const [firstError, secondError] = await Promise.all([
        first.catch((e) => e),
        second.catch((e) => e),
      ])
      added.resolve({})
      await settle()

      expect(harness.manager.state()).toEqual(ABORTABLE_ERROR)
      expect(firstError).toMatchObject({ code: OUT_OF_ORDER_SEQUENCE_NUMBER })
      expect(secondError).toBe(harness.manager.transaction.armedError())
      expect(harness.sent()).toEqual([{ nodeId: 1, epoch: 0, partitions: ['topic-a/0@0'] }])
    })

    test('a re-send queued before the same answer armed an error state is rejected at once, not after its backoff', async () => {
      let calls = 0
      const harness = createHarness({
        transactional: true,
        layout: { [TOPIC]: { 0: 1, 1: 1 } },
        answer: (request, nodeId) => {
          calls += 1
          return respond(request, nodeId, {
            'topic-a/0': REQUEST_TIMED_OUT,
            'topic-a/1': OUT_OF_ORDER_SEQUENCE_NUMBER,
          })
        },
      })
      await harness.start()
      jest.useFakeTimers({ now: 0 })

      const sending = harness
        .send([{ topic: TOPIC, messages: [...records(0), ...records(1)] }])
        .catch((e) => e)
      await jest.advanceTimersByTimeAsync(0)

      expect(harness.manager.state()).toEqual(ABORTABLE_ERROR)
      expect(harness.manager.sequencer.hasInFlight()).toBe(false)
      expect(await sending).toBe(harness.manager.transaction.armedError())
      expect(calls).toBe(1)
    })

    test('a send made while an error state is armed is rejected at once and registers nothing', async () => {
      const harness = createHarness({ transactional: true })
      await harness.start()
      harness.manager.arm(createErrorFromCode(INVALID_RECORD))
      harness.coordinator.addPartitionsToTxn.mockClear()

      await expect(harness.send([{ topic: TOPIC, messages: records(0) }])).rejects.toBe(
        harness.manager.transaction.armedError()
      )
      expect(harness.coordinator.addPartitionsToTxn).not.toHaveBeenCalled()
      expect(harness.produced).toEqual([])
    })

    test('a partition registered, its transaction ended and the next one begun in one turn is added again and sent', async () => {
      const harness = createHarness({ transactional: true })
      await harness.start()
      const request = (partition) => ({
        acks: -1,
        timeout: 30000,
        compression: 0,
        batches: [{ topic: TOPIC, partition, messages: records(partition) }],
      })

      const abandoned = harness.sendMessages.sendPartitioned(request(0)).catch((e) => e)
      harness.manager.sequencer.abortQueued(new KafkaJSNonRetriableError('aborted'))
      harness.manager.transitionTo(ABORTING)
      harness.manager.transitionTo(READY)
      harness.manager.transitionTo(IN_TRANSACTION)
      const next = harness.sendMessages.sendPartitioned(request(1))

      await expect(abandoned).resolves.toMatchObject({ message: 'aborted' })
      await expect(next).resolves.toEqual([expect.objectContaining({ partition: 1 })])
      expect(harness.coordinator.addPartitionsToTxn).toHaveBeenCalledTimes(1)
      expect(harness.coordinator.addPartitionsToTxn).toHaveBeenCalledWith(
        expect.objectContaining({ topics: [{ topic: TOPIC, partitions: [1] }] })
      )
      expect(harness.sent()).toEqual([{ nodeId: 2, epoch: 0, partitions: ['topic-a/1@0'] }])
    })
  })

  describe('waiting, the order of the results and the edges of a send', () => {
    test('with every queued batch in backoff the loop drains nothing until the earliest one ends', async () => {
      const answers = []
      const harness = createHarness({
        answer: (request, nodeId) => {
          const held = deferred()
          answers.push({ request, nodeId, held })
          return held.promise
        },
      })
      await harness.start()
      const drain = jest.spyOn(harness.manager, 'drain')
      jest.useFakeTimers({ now: 0 })
      const timedOut = { 'topic-a/0': REQUEST_TIMED_OUT, 'topic-a/1': REQUEST_TIMED_OUT }

      const sending = harness.send([{ topic: TOPIC, messages: [...records(0), ...records(1)] }])
      await jest.advanceTimersByTimeAsync(0)
      const drainsBeforeAnswers = drain.mock.calls.length
      for (const { request, nodeId, held } of answers.splice(0)) {
        held.resolve(respond(request, nodeId, timedOut))
      }
      await jest.advanceTimersByTimeAsync(RETRY.initialRetryTime - 1)

      expect(drain).toHaveBeenCalledTimes(drainsBeforeAnswers)
      expect(harness.produced).toHaveLength(2)

      await jest.advanceTimersByTimeAsync(1)
      expect(drain.mock.calls.length).toBeGreaterThan(drainsBeforeAnswers)
      for (const { request, nodeId, held } of answers.splice(0)) {
        held.resolve(respond(request, nodeId))
      }
      await expect(sending).resolves.toHaveLength(2)
      expect(harness.produced).toHaveLength(4)
    })

    test('the metadata of a two-topic, two-broker send is sorted by topic then partition, as a non-idempotent producer’s is', async () => {
      const { leaders, topicMessages } = METADATA_ORDER_CASE.input
      const harness = createHarness({ layout: leaders })
      await harness.start()

      const metadata = await harness.send(topicMessages)

      expect(metadata.map(({ topicName, partition }) => `${topicName}/${partition}`)).toEqual([
        'baseline-topic-a/0',
        'baseline-topic-a/1',
        'baseline-topic-b/0',
      ])
      expect(await captureMetadataOrder(METADATA_ORDER_CASE)).toEqual(capturedMetadataOrder)
    })

    test('a send with no batches resolves at once with nothing sent, queued or looked up', async () => {
      const harness = createHarness()
      await harness.start()

      await expect(
        harness.sendMessages.sendPartitioned({
          acks: -1,
          timeout: 30000,
          compression: 0,
          batches: [],
        })
      ).resolves.toEqual([])

      expect(harness.cluster.addMultipleTargetTopics).not.toHaveBeenCalled()
      expect(harness.produced).toEqual([])
      expect(harness.manager.sequencer.hasInFlight()).toBe(false)
    })

    test('a topic that does not exist rejects within retry.retries, as before, leaving nothing queued or scheduled', async () => {
      const harness = createHarness({ layout: {} })
      await harness.start()
      harness.cluster.addMultipleTargetTopics.mockImplementation(async () => {
        throw new KafkaJSProtocolError(createErrorFromCode(3))
      })
      jest.useFakeTimers({ now: 0 })

      const sending = harness
        .send([{ topic: 'no-such-topic', messages: records(0) }])
        .catch((e) => e)
      await jest.advanceTimersByTimeAsync(1000)
      const error = await sending

      expect(error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(error.cause).toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })
      expect(harness.cluster.addMultipleTargetTopics).toHaveBeenCalledTimes(RETRY.retries + 1)
      expect(harness.produced).toEqual([])
      expect(harness.manager.sequencer.entry('no-such-topic', 0)).toBeNull()
      expect(jest.getTimerCount()).toBe(0)
    })

    test('a topic whose metadata is not loaded yet is refreshed and retried before its records are partitioned', async () => {
      const harness = createHarness({ layout: {} })
      await harness.start()
      harness.cluster.refreshMetadata
        .mockRejectedValueOnce(new Error('no metadata'))
        .mockImplementationOnce(async () => {
          harness.layout[TOPIC] = { 0: 1 }
        })

      await expect(harness.send([{ topic: TOPIC, messages: records(0) }])).resolves.toHaveLength(1)
      expect(harness.cluster.refreshMetadata).toHaveBeenCalledTimes(2)
    })

    test('an invalid topic name rejects with the broker’s error at once', async () => {
      const harness = createHarness()
      await harness.start()
      const invalid = new KafkaJSProtocolError(createErrorFromCode(17))
      harness.cluster.addMultipleTargetTopics.mockRejectedValue(invalid)

      await expect(harness.send([{ topic: 'bad topic!', messages: records(0) }])).rejects.toBe(
        invalid
      )
      expect(harness.cluster.addMultipleTargetTopics).toHaveBeenCalledTimes(1)
      expect(harness.cluster.refreshMetadata).not.toHaveBeenCalled()
    })

    test.each([
      [CONNECTION_STATUS.DISCONNECTING, KafkaJSNonRetriableError, /disconnecting/],
      [CONNECTION_STATUS.DISCONNECTED, KafkaJSError, /disconnected/],
    ])(
      'a producer %s settles every queued, in-flight and backing-off send with its status error, and sends nothing more',
      async (status, ErrorClass, message) => {
        const held = deferred()
        let calls = 0
        const harness = createHarness({
          transactional: true,
          layout: { [TOPIC]: { 0: 1, 1: 1, 2: 1 } },
          answer: async (request, nodeId) => {
            calls += 1
            if (calls === 1) {
              return respond(request, nodeId, { 'topic-a/0': REQUEST_TIMED_OUT })
            }
            await held.promise
            return respond(request, nodeId, { 'topic-a/1': REQUEST_TIMED_OUT })
          },
        })
        await harness.start()
        const adding = deferred()
        harness.coordinator.addPartitionsToTxn
          .mockImplementationOnce(async () => ({}))
          .mockImplementationOnce(async () => ({}))
          .mockImplementationOnce(() => adding.promise)
        jest.useFakeTimers({ now: 0 })
        const sendTo = (partition) =>
          harness.sendMessages
            .sendPartitioned({
              acks: -1,
              timeout: 30000,
              compression: 0,
              batches: [{ topic: TOPIC, partition, messages: records(partition) }],
            })
            .catch((e) => e)

        const backingOff = sendTo(0)
        await jest.advanceTimersByTimeAsync(0)
        const inFlight = sendTo(1)
        await jest.advanceTimersByTimeAsync(0)
        const queued = sendTo(2)
        await jest.advanceTimersByTimeAsync(0)
        expect(harness.produced).toHaveLength(2)

        harness.setStatus(status)
        held.resolve()
        await jest.advanceTimersByTimeAsync(RETRY.maxRetryTime)
        adding.resolve({})
        await jest.advanceTimersByTimeAsync(RETRY.maxRetryTime)

        for (const error of await Promise.all([backingOff, inFlight, queued])) {
          expect(error).toBeInstanceOf(ErrorClass)
          expect(error.message).toMatch(message)
        }
        expect(harness.produced).toHaveLength(2)
        expect(harness.manager.sequencer.hasInFlight()).toBe(false)
        expect(harness.manager.sequencer.entry(TOPIC, 0)).toMatchObject({
          unresolvedNextSequence: 1,
        })
        expect(harness.manager.sequencer.entry(TOPIC, 1)).toMatchObject({
          unresolvedNextSequence: 1,
        })
        expect(harness.manager.sequencer.entry(TOPIC, 2)).toBeNull()
        expect(jest.getTimerCount()).toBe(0)
      }
    )

    test('a producer that stops being connected waits for the answers on their way without a timer', async () => {
      const held = deferred()
      const harness = createHarness({
        answer: async (request, nodeId) => {
          if (nodeId === 2) {
            await held.promise
            return respond(request, nodeId)
          }
          return respond(request, nodeId, { 'topic-a/0': REQUEST_TIMED_OUT })
        },
      })
      await harness.start()
      jest.useFakeTimers({ now: 0 })

      const sending = harness
        .send([{ topic: TOPIC, messages: [...records(0), ...records(1)] }])
        .catch((e) => e)
      await jest.advanceTimersByTimeAsync(0)
      harness.setStatus(CONNECTION_STATUS.DISCONNECTING)
      await jest.advanceTimersByTimeAsync(RETRY.initialRetryTime)

      expect(jest.getTimerCount()).toBe(0)
      held.resolve()
      expect((await sending).message).toMatch(/disconnecting/)
      expect(harness.produced).toHaveLength(2)
    })
  })

  describe('routing', () => {
    test('a leader hint is applied, and stale metadata refreshed, before the re-send', async () => {
      let calls = 0
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1 } },
        answer: (request, nodeId) => {
          calls += 1
          if (calls > 1) {
            return respond(request, nodeId)
          }
          const results = respond(request, nodeId, { 'topic-a/0': NOT_LEADER_OR_FOLLOWER })
          results[0].error.currentLeader = { leaderId: 2, leaderEpoch: 6 }
          results[0].error.nodeEndpoints = [{ nodeId: 2, host: 'b', port: 9092 }]
          return results
        },
      })
      await harness.start()
      harness.cluster.applyLeaderHint.mockImplementation(async () => {
        harness.layout[TOPIC][0] = 2
        return true
      })

      await harness.send([{ topic: TOPIC, messages: records(0) }])

      expect(harness.cluster.applyLeaderHint).toHaveBeenCalledWith({
        topic: TOPIC,
        partition: 0,
        leaderId: 2,
        leaderEpoch: 6,
        nodeEndpoints: [{ nodeId: 2, host: 'b', port: 9092 }],
      })
      expect(harness.cluster.refreshMetadata).toHaveBeenCalledTimes(1)
      expect(harness.sent().map(({ nodeId }) => nodeId)).toEqual([1, 2])
    })

    test('a leader hint carries the producer lifecycle that began the send', async () => {
      const generation = new Generation()
      let calls = 0
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1 } },
        generation,
        answer: (request, nodeId) => {
          calls += 1
          if (calls > 1) {
            return respond(request, nodeId)
          }
          const results = respond(request, nodeId, { 'topic-a/0': NOT_LEADER_OR_FOLLOWER })
          results[0].error.currentLeader = { leaderId: 2, leaderEpoch: 6 }
          return results
        },
      })
      await harness.start()

      await harness.send([{ topic: TOPIC, messages: records(0) }])

      expect(harness.cluster.applyLeaderHint).toHaveBeenCalledWith(
        expect.objectContaining({ topic: TOPIC, partition: 0, leaderId: 2, leaderEpoch: 6 }),
        { fence: generation }
      )
    })

    test('a hint that fails to apply and a refresh that fails leave the re-send to the retry', async () => {
      let calls = 0
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1 } },
        answer: (request, nodeId) => {
          calls += 1
          if (calls > 1) {
            return respond(request, nodeId)
          }
          const results = respond(request, nodeId, { 'topic-a/0': NOT_LEADER_OR_FOLLOWER })
          results[0].error.currentLeader = { leaderId: 2, leaderEpoch: 6 }
          return results
        },
      })
      await harness.start()
      harness.cluster.applyLeaderHint.mockRejectedValue(new Error('bad endpoint'))
      harness.cluster.refreshMetadata.mockRejectedValue(new Error('no metadata'))

      await expect(harness.send([{ topic: TOPIC, messages: records(0) }])).resolves.toHaveLength(1)
      expect(harness.produced).toHaveLength(2)
    })

    test('a closed connection drops its broker, and a disconnected cluster reconnects before the re-send', async () => {
      let calls = 0
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1 } },
        answer: (request, nodeId) => {
          calls += 1
          if (calls === 1) {
            throw new KafkaJSConnectionClosedError('closed', { host: 'a', port: 9092 })
          }
          return respond(request, nodeId)
        },
      })
      await harness.start()
      harness.cluster.isConnected.mockReturnValueOnce(false)

      await harness.send([{ topic: TOPIC, messages: records(0) }])

      expect(harness.cluster.removeBroker).toHaveBeenCalledWith({ host: 'a', port: 9092 })
      expect(harness.cluster.connect).toHaveBeenCalledTimes(1)
      expect(harness.cluster.refreshMetadata).toHaveBeenCalledTimes(1)
    })

    test('a broker that cannot be found fails its batches retriably', async () => {
      const harness = createHarness({ layout: { [TOPIC]: { 0: 1 } } })
      await harness.start()
      const findBroker = harness.cluster.findBroker.getMockImplementation()
      harness.cluster.findBroker.mockImplementationOnce(async () => {
        throw new KafkaJSError('broker not found')
      })
      harness.cluster.findBroker.mockImplementation(findBroker)

      await expect(harness.send([{ topic: TOPIC, messages: records(0) }])).resolves.toHaveLength(1)
      expect(harness.sent()).toEqual([{ nodeId: 1, epoch: 0, partitions: ['topic-a/0@0'] }])
    })

    test('a queued batch whose partition lost its leader waits with a refresh and gives up after retry.retries waits', async () => {
      const answered = new Set()
      const harness = createHarness({
        layout: { [TOPIC]: { 1: 1, 2: 1 }, [OTHER_TOPIC]: { 0: 1 } },
        answer: (request, nodeId) => {
          const [{ partition }] = request.topicData[0].partitions
          const first = !answered.has(partition)
          answered.add(partition)
          return respond(
            request,
            nodeId,
            first ? { [`topic-a/${partition}`]: REQUEST_TIMED_OUT } : {}
          )
        },
      })
      await harness.start()
      jest.useFakeTimers({ now: 0 })
      const findLeaderForPartitions = harness.cluster.findLeaderForPartitions
      harness.cluster.findLeaderForPartitions = (topic, ids) => {
        if (topic === OTHER_TOPIC) {
          throw new KafkaJSError('Invalid partition metadata')
        }
        return findLeaderForPartitions(topic, ids)
      }
      const request = (batches) =>
        harness.sendMessages.sendPartitioned({ acks: -1, timeout: 30000, compression: 0, batches })

      const sending = request([
        { topic: TOPIC, partition: 0, messages: records(0) },
        { topic: TOPIC, partition: 1, messages: records(1) },
        { topic: OTHER_TOPIC, partition: 0, messages: records(0) },
      ]).catch((e) => e)
      await jest.advanceTimersByTimeAsync(1)
      const sibling = request([{ topic: TOPIC, partition: 2, messages: records(2) }])
      await jest.advanceTimersByTimeAsync(1000)
      const error = await sending

      expect(error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(error.cause.message).toMatch(/^No leader for topic-(a|b) partition 0$/)
      await expect(sibling).resolves.toHaveLength(1)
      expect(harness.sent().map(({ partitions }) => partitions)).toEqual([
        ['topic-a/1@0'],
        ['topic-a/2@0'],
        ['topic-a/1@0'],
        ['topic-a/2@0'],
      ])
      expect(harness.cluster.refreshMetadata).toHaveBeenCalled()
      expect(harness.manager.sequencer.entry(TOPIC, 0)).toBeNull()
      expect(harness.manager.sequencer.entry(OTHER_TOPIC, 0)).toBeNull()
      expect(jest.getTimerCount()).toBe(0)
    })

    describe('a partition without a leader among partitions that have one', () => {
      /**
       * One send of a batch per partition, each settling on its own settler, to a topic whose
       * partition `n` has leader `leaders[n]`.
       * @param {(number|null)[]} leaders `null` is a partition the cache does not hold, which
       *   still gets a batch
       */
      const sendTo = async (leaders) => {
        const harness = createHarness()
        useClusterLookups(
          harness.cluster,
          TOPIC,
          leaders.flatMap((leader, partitionId) =>
            leader === null ? [] : [{ partitionId, leader }]
          )
        )
        await harness.start()
        jest.useFakeTimers({ now: 0 })
        const settlers = leaders.map(() => ({ resolve: jest.fn(), reject: jest.fn() }))

        const sending = harness.sendMessages
          .sendPartitioned({
            acks: -1,
            timeout: 30000,
            compression: 0,
            batches: settlers.map((settler, partition) => ({
              topic: TOPIC,
              partition,
              messages: records(partition),
              settlers: [settler],
            })),
          })
          .catch((e) => e)
        await jest.advanceTimersByTimeAsync(1000)
        return { harness, settlers, error: await sending }
      }

      test('the partitions with a leader are produced and the one without is rejected alone, naming itself', async () => {
        const { harness, settlers, error } = await sendTo([1, -1, 1])

        expect(harness.sent()).toEqual([
          { nodeId: 1, epoch: 0, partitions: ['topic-a/0@0', 'topic-a/2@0'] },
        ])
        for (const partition of [0, 2]) {
          expect(settlers[partition].resolve).toHaveBeenCalledTimes(1)
          expect(settlers[partition].reject).not.toHaveBeenCalled()
        }
        expect(settlers[1].resolve).not.toHaveBeenCalled()
        expect(settlers[1].reject).toHaveBeenCalledTimes(1)
        expect(error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
        expect(error.cause.message).toEqual('No leader for topic-a partition 1')
        expect(error.retryCount).toBe(RETRY.retries)
      })

      test('a partition the cache does not hold is rejected beside the one without a leader, each naming itself', async () => {
        const { harness, settlers } = await sendTo([1, -1, null])

        expect(harness.sent()).toEqual([{ nodeId: 1, epoch: 0, partitions: ['topic-a/0@0'] }])
        expect(settlers[0].resolve).toHaveBeenCalledTimes(1)
        for (const partition of [1, 2]) {
          expect(settlers[partition].resolve).not.toHaveBeenCalled()
          expect(settlers[partition].reject).toHaveBeenCalledTimes(1)
          expect(settlers[partition].reject.mock.calls[0][0].cause.message).toEqual(
            `No leader for topic-a partition ${partition}`
          )
        }
      })

      test('every partition of the topic without a leader is rejected, each naming itself', async () => {
        const { harness, settlers } = await sendTo([-1, -1])

        expect(harness.produced).toEqual([])
        settlers.forEach(({ resolve }) => expect(resolve).not.toHaveBeenCalled())
        settlers.forEach(({ reject }, partition) => {
          expect(reject).toHaveBeenCalledTimes(1)
          expect(reject.mock.calls[0][0].cause.message).toEqual(
            `No leader for topic-a partition ${partition}`
          )
        })
      })
    })
  })

  describe('deadlines and the producer id', () => {
    test('a batch that never got out expires on its deadline with KafkaJSDeliveryTimeout', async () => {
      const harness = createHarness({ transactional: true, deliveryTimeout: 100 })
      await harness.start()
      harness.coordinator.addPartitionsToTxn.mockImplementationOnce(() => new Promise(() => {}))
      jest.useFakeTimers({ now: 0 })

      const sending = harness.send([{ topic: TOPIC, messages: records(0) }]).catch((e) => e)
      await jest.advanceTimersByTimeAsync(100)
      const error = await sending

      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(error.message).toEqual(
        'Gave up on topic-a partition 0 after 1 attempts: its 100 ms delivery deadline passed'
      )
      expect(error.cause).toBeUndefined()
      expect(harness.manager.state()).toEqual(ABORTABLE_ERROR)
      expect(harness.produced).toEqual([])
    })

    test.each([
      ['a retriable answer cannot change the earlier timeout', REQUEST_TIMED_OUT, true],
      ['a terminal answer cannot change the earlier timeout', INVALID_RECORD, false],
    ])('after the deadline, %s', async (_, code, expires) => {
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1 } },
        retry: { ...RETRY, maxTotalTime: 50 },
        answer: async (request, nodeId) => {
          await new Promise((resolve) => setTimeout(resolve, 60))
          return respond(request, nodeId, { 'topic-a/0': code })
        },
      })
      await harness.start()
      jest.useFakeTimers({ now: 0 })

      const sending = harness.send([{ topic: TOPIC, messages: records(0) }]).catch((e) => e)
      await jest.advanceTimersByTimeAsync(60)
      const error = await sending

      expect(expires).toBe(code === REQUEST_TIMED_OUT)
      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(error.outcome).toBe('unknown')
      expect(error.cause).toBeUndefined()
      expect(harness.produced).toHaveLength(1)
    })

    test('a re-send whose backoff ends past its deadline expires at the deadline', async () => {
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1 } },
        deliveryTimeout: 15,
        answer: (request, nodeId) => respond(request, nodeId, { 'topic-a/0': REQUEST_TIMED_OUT }),
      })
      await harness.start()
      jest.useFakeTimers({ now: 0 })

      const sending = harness.send([{ topic: TOPIC, messages: records(0) }]).catch((e) => e)
      await jest.advanceTimersByTimeAsync(15)
      const error = await sending

      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(error.cause).toMatchObject({ code: REQUEST_TIMED_OUT })
      expect(harness.produced).toHaveLength(2)
      expect(harness.manager.sequencer.entry(TOPIC, 0)).toMatchObject({ unresolvedNextSequence: 1 })
    })

    test('a batch never expires before the one queued ahead of it in its partition', async () => {
      const harness = createHarness({ deliveryTimeout: 100 })
      harness.coordinator.initProducerId.mockImplementationOnce(() => new Promise(() => {}))
      jest.useFakeTimers({ now: 1000 })
      const request = (createdAt) =>
        harness.sendMessages
          .sendPartitioned({
            acks: -1,
            timeout: 30000,
            compression: 0,
            batches: [{ topic: TOPIC, partition: 0, messages: records(0), createdAt }],
          })
          .catch((e) => e)

      const ahead = request(1000)
      const behind = request(950)
      await jest.advanceTimersByTimeAsync(60)
      const settledEarly = await Promise.race([
        behind.then(() => 'expired'),
        Promise.resolve('queued'),
      ])
      await jest.advanceTimersByTimeAsync(40)

      expect(settledEarly).toEqual('queued')
      expect(await ahead).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(await behind).toBeInstanceOf(KafkaJSDeliveryTimeout)
    })

    test('without a producer id the loop acquires one, once, then drains', async () => {
      const harness = createHarness({ layout: { [TOPIC]: { 0: 1, 1: 1 } }, retry: {} })
      const acquired = deferred()
      harness.coordinator.initProducerId.mockImplementationOnce(() => acquired.promise)
      const request = (partition) =>
        harness.sendMessages.sendPartitioned({
          acks: -1,
          timeout: 30000,
          compression: 0,
          batches: [{ topic: TOPIC, partition, messages: records(partition) }],
        })

      const first = request(0)
      await settle()
      const second = request(1)
      await settle()
      acquired.resolve({ producerId: '2000', producerEpoch: 0 })

      await expect(Promise.all([first, second])).resolves.toHaveLength(2)
      expect(harness.coordinator.initProducerId).toHaveBeenCalledTimes(1)
      expect(harness.produced.map(({ request }) => request.producerId)).toEqual(['2000'])
    })
  })

  describe('deadlines found without the owner timer', () => {
    /**
     * Starts one send per entry of `partitions`, a turn apart.
     * @param {ReturnType<typeof createHarness>} harness
     * @param {number[]} partitions
     * @returns {Promise<Promise<Error|object[]>[]>} each send's outcome, errors caught
     */
    const sendEach = async (harness, partitions) => {
      const outcomes = []
      for (const partition of partitions) {
        outcomes.push(
          harness.send([{ topic: TOPIC, messages: records(partition) }]).catch((e) => e)
        )
        await jest.advanceTimersByTimeAsync(0)
      }
      return outcomes
    }

    /**
     * Expects `errors` to be distinct, unsent `KafkaJSDeliveryTimeout`s, and the transaction armed.
     * @param {ReturnType<typeof createHarness>} harness
     * @param {Error[]} errors
     */
    const expectOwnTimeouts = (harness, errors) => {
      expect(errors).toEqual([
        expect.any(KafkaJSDeliveryTimeout),
        expect.any(KafkaJSDeliveryTimeout),
      ])
      expect(errors[1]).not.toBe(errors[0])
      expect(errors.map(({ outcome }) => outcome)).toEqual(['unsent', 'unsent'])
      expect(harness.manager.state()).toEqual(ABORTABLE_ERROR)
      expect(jest.getTimerCount()).toBe(0)
    }

    /**
     * A transactional harness with a 2000 ms deadline and three brokers, each leading one
     * partition. Partition 0's request waits for `encoder` before its write guard, partition 1's
     * is written and answered with a retriable code once `retryAnswer` resolves, and partition
     * 2's is written and waits for `response`.
     * @returns {Promise<{ harness: ReturnType<typeof createHarness>, encoder: object,
     *   retryAnswer: object, response: object }>}
     */
    const startMixedDeadlines = async () => {
      const encoder = deferred()
      const retryAnswer = deferred()
      const response = deferred()
      const harness = createHarness({
        transactional: true,
        deliveryTimeout: 2000,
        layout: { [TOPIC]: { 0: 1, 1: 2, 2: 3 } },
      })
      await harness.start()
      harness.broker(1).produce.mockImplementationOnce(async (request) => {
        await encoder.promise
        const refusal = request.guard(100)
        if (refusal) throw refusal
        return respond(request, 1)
      })
      harness.broker(2).produce.mockImplementationOnce(async (request) => {
        const refusal = request.guard(100)
        if (refusal) throw refusal
        await retryAnswer.promise
        return respond(request, 2, { [`${TOPIC}/1`]: NOT_LEADER_OR_FOLLOWER })
      })
      harness.broker(3).produce.mockImplementationOnce(async (request) => {
        const refusal = request.guard(100)
        if (refusal) throw refusal
        await response.promise
        return respond(request, 3)
      })
      return { harness, encoder, retryAnswer, response }
    }

    /**
     * From a send of partitions 0 and 1 created at 0 and partition 2 created at 1000, brought to
     * the write guard of partition 0: partitions 1 and 2 are on the wire, partition 1 is answered
     * retriably and queued for its retry, and the clock reaches the deadline of partitions 0 and 1
     * without the owner timer running. Partition 2's deadline is 1000 ms later.
     * @param {Awaited<ReturnType<typeof startMixedDeadlines>>} held
     * @returns {Promise<void>}
     */
    const expireAtTheGuard = async ({ harness, encoder, retryAnswer }) => {
      await jest.advanceTimersByTimeAsync(0)
      for (const nodeId of [1, 2, 3]) {
        expect(harness.broker(nodeId).produce).toHaveBeenCalledTimes(1)
      }
      jest.setSystemTime(1995)
      retryAnswer.resolve()
      await jest.advanceTimersByTimeAsync(0)
      expect(harness.manager.sequencer.entry(TOPIC, 1).queued).toHaveLength(1)
      jest.setSystemTime(2000)
      encoder.resolve()
      await jest.advanceTimersByTimeAsync(0)
    }

    /**
     * Releases every held continuation and runs the clock past the last deadline.
     * @param {Awaited<ReturnType<typeof startMixedDeadlines>>} held
     * @returns {Promise<void>}
     */
    const releaseMixedDeadlines = async ({ encoder, retryAnswer, response }) => {
      encoder.resolve()
      retryAnswer.resolve()
      response.resolve()
      await jest.advanceTimersByTimeAsync(1000)
    }

    test('the write-boundary guard settles every due caller with its own timeout before arming the transaction', async () => {
      const held = deferred()
      const harness = createHarness({
        transactional: true,
        deliveryTimeout: 1000,
        layout: { [TOPIC]: { 0: 1 } },
      })
      await harness.start()
      harness.broker(1).produce.mockImplementationOnce(async (request) => {
        await held.promise
        const refusal = request.guard(100)
        if (refusal) throw refusal
        return respond(request, 1)
      })
      jest.useFakeTimers({ now: 0 })

      const sending = await sendEach(harness, [0, 0])
      const settled = settledFlag(Promise.all(sending))
      jest.setSystemTime(1000)
      held.resolve()
      await jest.advanceTimersByTimeAsync(0)

      expect(settled()).toBe(true)
      expect(harness.broker(1).produce).toHaveBeenCalledTimes(1)
      expectOwnTimeouts(harness, await Promise.all(sending))
    })

    test('a pass that finds several deliveries due settles each with its own timeout before arming the transaction', async () => {
      const added = deferred()
      const harness = createHarness({
        transactional: true,
        deliveryTimeout: 1000,
        layout: { [TOPIC]: { 0: 1, 1: 1 } },
      })
      await harness.start()
      harness.coordinator.addPartitionsToTxn.mockImplementation(() => added.promise)
      jest.useFakeTimers({ now: 0 })

      const sending = await sendEach(harness, [0, 1])
      const settled = settledFlag(Promise.all(sending))
      jest.setSystemTime(1000)
      added.resolve({})
      await jest.advanceTimersByTimeAsync(0)

      expect(settled()).toBe(true)
      expect(harness.broker(1).produce).not.toHaveBeenCalled()
      expectOwnTimeouts(harness, await Promise.all(sending))
    })

    test('a send that is due on arrival settles the callers already due with their own timeouts before arming the transaction', async () => {
      const added = deferred()
      const harness = createHarness({
        transactional: true,
        deliveryTimeout: 1000,
        layout: { [TOPIC]: { 0: 1, 1: 1 } },
      })
      await harness.start()
      harness.coordinator.addPartitionsToTxn.mockImplementation(() => added.promise)
      jest.useFakeTimers({ now: 0 })

      const [queued] = await sendEach(harness, [0])
      jest.setSystemTime(1000)
      const arriving = harness.sendMessages
        .sendPartitioned({
          acks: -1,
          timeout: 30000,
          compression: 0,
          batches: [{ topic: TOPIC, partition: 1, messages: records(1), createdAt: 0 }],
        })
        .catch((e) => e)
      const settled = settledFlag(Promise.all([queued, arriving]))
      await jest.advanceTimersByTimeAsync(0)
      added.resolve({})
      await jest.advanceTimersByTimeAsync(0)

      expect(settled()).toBe(true)
      expectOwnTimeouts(harness, await Promise.all([queued, arriving]))
    })

    test('the loop never asks a timer to wait longer than it can, and wakes in steps to the deadline', async () => {
      const initialized = deferred()
      const harness = createHarness({ deliveryTimeout: MAX_TIMER_DELAY })
      harness.coordinator.initProducerId.mockImplementationOnce(() => initialized.promise)
      jest.useFakeTimers({ now: 0 })
      const schedule = jest.spyOn(global, 'setTimeout')
      const delays = () => schedule.mock.calls.map(([, delay]) => delay)

      const sending = harness.sendMessages
        .sendPartitioned({
          acks: -1,
          timeout: 30000,
          compression: 0,
          batches: [{ topic: TOPIC, partition: 0, messages: records(0), createdAt: 1000 }],
        })
        .catch((e) => e)
      await jest.advanceTimersByTimeAsync(0)
      expect(Math.max(...delays())).toBeLessThanOrEqual(MAX_TIMER_DELAY)

      schedule.mockClear()
      await jest.advanceTimersByTimeAsync(MAX_TIMER_DELAY)
      expect(delays()).toEqual(expect.arrayContaining([1000]))
      expect(Math.max(...delays())).toBeLessThanOrEqual(MAX_TIMER_DELAY)
      await jest.advanceTimersByTimeAsync(999)
      expect(jest.getTimerCount()).toBeGreaterThan(0)
      await jest.advanceTimersByTimeAsync(1)

      const error = await sending
      initialized.resolve({ producerId: '1000', producerEpoch: 0 })
      await jest.advanceTimersByTimeAsync(0)
      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(error.outcome).toBe('unsent')
      expect(jest.getTimerCount()).toBe(0)
    })

    test('a delivery deadline beyond one timer interval ends at its absolute time, not with a capped retrier', async () => {
      const harness = createHarness({ layout: { [TOPIC]: { 0: 1 } } })
      const plain = createSendMessages({
        logger: newLogger(),
        cluster: harness.cluster,
        retrier: createRetry(RETRY),
        partitioner: jest.fn(),
        eosManager: harness.eosManager,
        idempotence: new Idempotence({ enabled: false, explicit: false, logger: newLogger() }),
        retry: RETRY,
        deliveryTimeout: MAX_TIMER_DELAY,
      })
      const answers = [deferred(), deferred()]
      const answerOf = ({ topicData }, errorCode) =>
        partitionResults({
          topics: topicData.map(({ topic, partitions }) => ({
            topicName: topic,
            partitions: partitions.map(({ partition }) => ({ partition, errorCode })),
          })),
        })
      const produce = harness.broker(1).produce
      for (const answer of answers) {
        produce.mockImplementationOnce(async (request) => {
          const refusal = request.guard(100)
          if (refusal) throw refusal
          return answerOf(request, await answer.promise)
        })
      }
      jest.useFakeTimers({ now: 0 })

      const sending = plain
        .sendPartitioned({
          acks: -1,
          timeout: 30000,
          compression: 0,
          batches: [{ topic: TOPIC, partition: 0, messages: records(0), createdAt: 1000 }],
        })
        .catch((e) => e)
      const isSettled = settledFlag(sending)
      await jest.advanceTimersByTimeAsync(MAX_TIMER_DELAY - 5)
      expect(produce).toHaveBeenCalledTimes(1)
      answers[0].resolve(NOT_LEADER_OR_FOLLOWER)
      await jest.advanceTimersByTimeAsync(0)
      expect(isSettled()).toBe(false)

      await jest.advanceTimersByTimeAsync(1004)
      expect(produce).toHaveBeenCalledTimes(2)
      expect(Date.now()).toBe(MAX_TIMER_DELAY + 999)
      expect(isSettled()).toBe(false)
      await jest.advanceTimersByTimeAsync(1)

      const error = await sending
      answers[1].resolve(0)
      await jest.advanceTimersByTimeAsync(0)
      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(error.outcome).toBe('unknown')
      expect(jest.getTimerCount()).toBe(0)
    })

    test('a delivery already settled by its timeout is not settled again when the transaction arms, so a later one on the wire keeps its deadline', async () => {
      const held = await startMixedDeadlines()
      const { harness } = held
      jest.useFakeTimers({ now: 1000 })
      const settlers = [0, 1, 2].map(() => ({ resolve: jest.fn(), reject: jest.fn() }))
      const sending = harness.sendMessages
        .sendPartitioned({
          acks: -1,
          timeout: 30000,
          compression: 0,
          batches: settlers.map((settler, partition) => ({
            topic: TOPIC,
            partition,
            messages: records(partition),
            createdAt: partition === 2 ? 1000 : 0,
            settlers: [settler],
          })),
        })
        .catch((e) => e)
      const isSettled = settledFlag(sending)

      try {
        await expireAtTheGuard(held)
        expect(isSettled()).toBe(false)
        expect(jest.getTimerCount()).toBeGreaterThan(0)

        await jest.advanceTimersByTimeAsync(1000)
        const rejections = settlers.map(({ reject }) => reject.mock.calls.map(([error]) => error))
        expect(rejections.map((errors) => errors.length)).toEqual([1, 1, 1])
        for (const [error] of rejections) expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
        expect(rejections[0][0]).not.toBe(rejections[1][0])
        expect(rejections[2][0].outcome).toBe('unknown')
        expect(await sending).toBeInstanceOf(KafkaJSDeliveryTimeout)
        expect(jest.getTimerCount()).toBe(0)
      } finally {
        await releaseMixedDeadlines(held)
        await sending
      }
    })

    test('a record already written is not reported unsent because a batch beside it expired', async () => {
      const held = await startMixedDeadlines()
      const { harness } = held
      jest.useFakeTimers({ now: 0 })
      const accumulator = new Accumulator({
        linger: 1000,
        batchSize: 16384,
        partition: harness.sendMessages.partition,
        sendPartitioned: harness.sendMessages.sendPartitioned,
      })
      const append = (partition) =>
        accumulator
          .append({
            acks: -1,
            timeout: 30000,
            compression: 0,
            topicMessages: [{ topic: TOPIC, messages: records(partition) }],
          })
          .catch((e) => e)
      const appends = [append(0), append(1)]
      await jest.advanceTimersByTimeAsync(0)
      jest.setSystemTime(1000)
      appends.push(append(2))
      await jest.advanceTimersByTimeAsync(0)
      const flushing = accumulator.flush()
      const isSettled = settledFlag(appends[2])

      try {
        await expireAtTheGuard(held)
        expect(isSettled()).toBe(false)

        await jest.advanceTimersByTimeAsync(1000)
        const [first, second, third] = await Promise.all(appends)
        expect(first).toBeInstanceOf(KafkaJSDeliveryTimeout)
        expect(second).toBeInstanceOf(KafkaJSDeliveryTimeout)
        expect(third).toBeInstanceOf(KafkaJSDeliveryTimeout)
        expect(third.outcome).toBe('unknown')
        expect(jest.getTimerCount()).toBe(0)
      } finally {
        await releaseMixedDeadlines(held)
        await Promise.all([...appends, flushing])
      }
    })
  })

  describe('pre-partitioned sends', () => {
    test("every batch's own settlers settle with it", async () => {
      const harness = createHarness({ layout: { [TOPIC]: { 0: 1, 1: 1 } } })
      await harness.start()
      const settler = () => ({ resolve: jest.fn(), reject: jest.fn() })
      const [first, second] = [settler(), settler()]

      await harness.sendMessages.sendPartitioned({
        acks: -1,
        timeout: 30000,
        compression: 0,
        batches: [
          { topic: TOPIC, partition: 0, messages: records(0), settlers: [first] },
          { topic: TOPIC, partition: 1, messages: records(1), settlers: [second] },
        ],
      })

      expect(first.resolve).toHaveBeenCalledWith(expect.objectContaining({ partition: 0 }))
      expect(second.resolve).toHaveBeenCalledWith(expect.objectContaining({ partition: 1 }))
    })

    test('a KIP-467 record error names the record of the batch it came from', async () => {
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1 } },
        answer: (request, nodeId) => {
          const results = respond(request, nodeId, { 'topic-a/0': INVALID_RECORD })
          results[0].error.recordErrors = [
            { batchIndex: 1, errorMessage: 'bad' },
            { batchIndex: 9 },
          ]
          return results
        },
      })
      await harness.start()
      const messages = records(0, 2)

      const error = await harness.send([{ topic: TOPIC, messages }]).catch((e) => e)

      expect(error.recordErrors).toEqual([
        { batchIndex: 1, errorMessage: 'bad', record: messages[1] },
        { batchIndex: 9 },
      ])
    })

    test('without idempotence each entry settles with its own partition’s outcome: a request that fails rejects only its own entries', async () => {
      const harness = createHarness({ layout: { [TOPIC]: { 0: 1, 1: 2 } } })
      const plain = createSendMessages({
        logger: newLogger(),
        cluster: harness.cluster,
        retrier: createRetry({ retries: 0 }),
        partitioner: jest.fn(),
        eosManager: harness.eosManager,
        idempotence: new Idempotence({ enabled: false, explicit: false, logger: newLogger() }),
      })
      const decoded = async ({ topicData }) =>
        partitionResults({
          topics: topicData.map(({ topic, partitions }) => ({
            topicName: topic,
            partitions: partitions.map(({ partition }) => ({ partition, errorCode: 0 })),
          })),
        })
      harness.broker(1).produce.mockImplementation(decoded)
      harness.broker(2).produce.mockImplementation(decoded)
      const settlers = [0, 1].map(() => ({ resolve: jest.fn(), reject: jest.fn() }))
      const batches = [0, 1].map((partition) => ({
        topic: TOPIC,
        partition,
        messages: records(partition),
        settlers: [settlers[partition]],
      }))

      harness.broker(2).produce.mockRejectedValueOnce(createErrorFromCode(INVALID_RECORD))
      const error = await plain
        .sendPartitioned({ acks: 1, timeout: 30000, compression: 0, batches })
        .catch((e) => e)
      expect(error).toMatchObject({ code: INVALID_RECORD })
      expect(settlers[0].resolve).toHaveBeenCalledWith(expect.objectContaining({ partition: 0 }))
      expect(settlers[0].reject).not.toHaveBeenCalled()
      expect(settlers[1].reject).toHaveBeenCalledWith(error)
      expect(settlers[1].resolve).not.toHaveBeenCalled()
      settlers.forEach(({ resolve }) => resolve.mockClear())

      const metadata = await plain.sendPartitioned({
        acks: 1,
        timeout: 30000,
        compression: 0,
        batches,
      })
      expect(settlers[0].resolve).toHaveBeenCalledWith(
        metadata.find(({ partition }) => partition === 0)
      )
      expect(settlers[1].resolve).toHaveBeenCalledWith(
        metadata.find(({ partition }) => partition === 1)
      )
      expect(harness.broker(1).produce).toHaveBeenCalledWith(
        expect.objectContaining({ producerId: -1, producerEpoch: 0, acks: 1 })
      )
    })
  })

  describe('order, the in-flight window and faults', () => {
    test('two requests drained together reach a real broker connection in drain order, the larger one first', async () => {
      const log = createPartitionLog()
      const brokerHarness = createGroupApiBroker({
        versions: { [apiKeys.Produce]: { minVersion: 3, maxVersion: 7 } },
        respond: () =>
          produceResponseV7(
            readProduceRequest(brokerHarness.requests.at(-1).body).map(
              ({ topic, partition, epoch, firstSequence, count }) => ({
                topic,
                partition,
                errorCode:
                  log.append(
                    `${topic}/${partition}`,
                    epoch,
                    firstSequence,
                    count,
                    `${firstSequence}@e${epoch}`
                  ) || 0,
              })
            )
          ),
      })
      const harness = createHarness({ layout: { [TOPIC]: { 0: 1, 1: 1, 2: 1, 3: 1, 4: 1 } } })
      harness.cluster.findBroker.mockImplementation(async () => brokerHarness.broker)
      await harness.start()
      const request = (batches, timeout = 30000) =>
        harness.sendMessages.sendPartitioned({ acks: -1, timeout, compression: 0, batches })

      const sends = await Promise.allSettled([
        request(
          [0, 1, 2, 3].map((partition) => ({
            topic: TOPIC,
            partition,
            messages: records(partition),
          }))
        ),
        request([{ topic: TOPIC, partition: 4, messages: records(4) }], 30001),
      ])
      await brokerHarness.pool.destroy()

      expect(sends.map(({ status }) => status)).toEqual(['fulfilled', 'fulfilled'])
      expect(log.written).toEqual([0, 1, 2, 3, 4].map((partition) => `topic-a/${partition}:0@e0`))
      expect(harness.manager.producerEpoch).toBe(0)
      expect(brokerHarness.requests.map(({ body }) => readProduceRequest(body).length)).toEqual([
        4, 1,
      ])
    })

    test('requests are handed to a broker in drain order even when finding the broker takes longer for the first', async () => {
      const harness = createHarness({ layout: { [TOPIC]: { 0: 1, 1: 1 } } })
      await harness.start()
      const found = deferred()
      const findBroker = harness.cluster.findBroker.getMockImplementation()
      harness.cluster.findBroker
        .mockImplementationOnce(async (node) => {
          await found.promise
          return findBroker(node)
        })
        .mockImplementation(findBroker)

      const sending = Promise.all([
        harness.sendMessages.sendPartitioned({
          acks: -1,
          timeout: 30000,
          compression: 0,
          batches: [{ topic: TOPIC, partition: 0, messages: records(0) }],
        }),
        harness.sendMessages.sendPartitioned({
          acks: -1,
          timeout: 30000,
          compression: 1,
          batches: [{ topic: TOPIC, partition: 1, messages: records(1) }],
        }),
      ])
      await settle()
      expect(harness.produced).toEqual([])
      found.resolve()
      await sending

      expect(harness.sent().map(({ partitions }) => partitions)).toEqual([
        ['topic-a/0@0'],
        ['topic-a/1@0'],
      ])
    })

    test('at most 5 requests are outstanding to a broker; the batches beyond them wait unnumbered', async () => {
      const partitions = Array.from({ length: 12 }, (_, partition) => partition)
      const answers = []
      const harness = createHarness({
        layout: { [TOPIC]: Object.fromEntries(partitions.map((partition) => [partition, 1])) },
        answer: (request, nodeId) => {
          const held = deferred()
          answers.push(() => held.resolve(respond(request, nodeId)))
          return held.promise
        },
      })
      await harness.start()

      const sends = []
      for (const partition of partitions) {
        sends.push(
          harness.sendMessages.sendPartitioned({
            acks: -1,
            timeout: 30000,
            compression: 0,
            batches: [{ topic: TOPIC, partition, messages: records(partition) }],
          })
        )
        await settle()
      }

      expect(harness.produced).toHaveLength(5)
      const heads = partitions.map(
        (partition) => harness.manager.sequencer.entry(TOPIC, partition).queued[0]?.firstSequence
      )
      expect(heads).toEqual([...Array(5).fill(undefined), ...Array(7).fill(null)])

      answers.shift()()
      await settle()
      expect(harness.produced).toHaveLength(6)

      while (answers.length > 0) {
        answers.shift()()
        await settle()
      }
      await expect(Promise.all(sends)).resolves.toHaveLength(12)
      expect(harness.sent().map(({ partitions: sent }) => sent)).toEqual([
        ...partitions.slice(0, 5).map((partition) => [`topic-a/${partition}@0`]),
        partitions.slice(5).map((partition) => `topic-a/${partition}@0`),
      ])
    })

    test('a batch the broker wrote but answered retriably is re-sent inside the broker’s 5-batch window and completes as a duplicate', async () => {
      const log = createPartitionLog()
      let calls = 0
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1 } },
        answer: (request, nodeId) => {
          calls += 1
          const call = calls
          const errors = {}
          for (const { topic, partitions } of request.topicData) {
            for (const { partition, firstSequence, messages } of partitions) {
              const key = `${topic}/${partition}`
              const code = log.append(
                key,
                request.producerEpoch,
                firstSequence,
                messages.length,
                messages[0].value
              )
              if (code === OUT_OF_ORDER_SEQUENCE_NUMBER) {
                errors[key] = code
              } else if (code === 'duplicate') {
                errors[key] = DUPLICATE_SEQUENCE_NUMBER
              } else if (call === 3) {
                errors[key] = REQUEST_TIMED_OUT
              }
            }
          }
          return respond(request, nodeId, errors)
        },
      })
      await harness.start()

      const sends = Array.from({ length: 10 }, (_, index) =>
        harness.sendMessages.sendPartitioned({
          acks: -1,
          timeout: 30000,
          compression: 0,
          batches: [{ topic: TOPIC, partition: 0, messages: [{ value: `m${index}` }] }],
        })
      )

      await expect(Promise.all(sends)).resolves.toHaveLength(10)
      expect(log.written).toEqual(Array.from({ length: 10 }, (_, index) => `topic-a/0:m${index}`))
    })

    test('a fault in a pass fails the live sends with it, and the next send starts the loop again', async () => {
      const harness = createHarness({ layout: { [TOPIC]: { 0: 1 } } })
      await harness.start()
      const fault = new Error('a fault in the drain')
      jest.spyOn(harness.manager, 'drain').mockImplementationOnce(() => {
        throw fault
      })

      await expect(harness.send([{ topic: TOPIC, messages: records(0) }])).rejects.toBe(fault)
      await expect(harness.send([{ topic: TOPIC, messages: records(0) }])).resolves.toHaveLength(1)
      expect(harness.sent()).toEqual([{ nodeId: 1, epoch: 0, partitions: ['topic-a/0@0'] }])
    })

    test('a fault while answering a request fails the live sends with it; answers still on their way only update the sequencer and settle nobody again', async () => {
      const held = new Map()
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1, 1: 2, 2: 3, 3: 1 } },
        answer: (request, nodeId) => {
          if (nodeId === 1) {
            return respond(request, nodeId)
          }
          const release = deferred()
          held.set(nodeId, (errors) => release.resolve(respond(request, nodeId, errors)))
          return release.promise
        },
      })
      await harness.start()
      const fault = new Error('a fault in an answer')
      const complete = harness.manager.complete.bind(harness.manager)
      jest
        .spyOn(harness.manager, 'complete')
        .mockImplementationOnce(complete)
        .mockImplementationOnce(() => {
          throw fault
        })
        .mockImplementation(complete)

      const settlers = [0, 1, 2, 3].map(() => ({ resolve: jest.fn(), reject: jest.fn() }))
      const error = await harness.sendMessages
        .sendPartitioned({
          acks: -1,
          timeout: 30000,
          compression: 0,
          batches: settlers.map((settler, partition) => ({
            topic: TOPIC,
            partition,
            messages: records(partition),
            settlers: [settler],
          })),
        })
        .catch((e) => e)
      expect(error).toBe(fault)
      expect(harness.manager.sequencer.entry(TOPIC, 0)).toMatchObject({ lastAckedSequence: 0 })
      expect(harness.manager.sequencer.hasInFlight()).toBe(true)

      held.get(2)()
      held.get(3)({ 'topic-a/2': INVALID_RECORD })
      await settle()

      for (const settler of settlers.slice(1)) {
        expect(settler.reject).toHaveBeenCalledTimes(1)
        expect(settler.reject).toHaveBeenCalledWith(fault)
        expect(settler.resolve).not.toHaveBeenCalled()
      }
      expect(harness.manager.sequencer.hasInFlight()).toBe(false)
      expect(harness.manager.sequencer.entry(TOPIC, 1)).toMatchObject({ lastAckedSequence: 0 })
      await expect(harness.send([{ topic: TOPIC, messages: records(0) }])).resolves.toHaveLength(1)
    })

    test('a retriable answer to a batch a fault already settled takes it out of the sequencer, so its partition keeps sending', async () => {
      const held = []
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1, 1: 1 } },
        answer: (request, nodeId) =>
          held.length > 0
            ? respond(request, nodeId)
            : new Promise((done) =>
                held.push(() =>
                  done(respond(request, nodeId, { [`${TOPIC}/0`]: NOT_LEADER_OR_FOLLOWER }))
                )
              ),
      })
      await harness.start()
      const fault = new Error('a fault in the drain')
      const first = harness.send([{ topic: TOPIC, messages: records(0) }]).catch((e) => e)
      await settle()
      const drain = harness.manager.drain.bind(harness.manager)
      jest
        .spyOn(harness.manager, 'drain')
        .mockImplementationOnce(() => {
          throw fault
        })
        .mockImplementation(drain)
      await harness.send([{ topic: TOPIC, messages: records(1) }]).catch(() => {})
      expect(await first).toBe(fault)

      held[0]()
      await settle()
      expect(harness.manager.sequencer.hasInFlight()).toBe(false)

      await expect(harness.send([{ topic: TOPIC, messages: records(0) }])).resolves.toHaveLength(1)
      expect(harness.sent().map(({ partitions }) => partitions)).toEqual([
        ['topic-a/0@0'],
        ['topic-a/0@1'],
      ])
    })

    test('five requests rejected with a closed connection give their slots back: the next send to that broker completes', async () => {
      let calls = 0
      const holds = []
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1 } },
        answer: (request, nodeId) => {
          calls += 1
          if (calls <= 5) {
            return new Promise((_, reject) => holds.push(reject))
          }
          return respond(request, nodeId)
        },
      })
      await harness.start()
      const sends = []
      for (const partition of [0, 1, 2, 3, 4, 5]) {
        sends.push(harness.send([{ topic: TOPIC, messages: records(partition) }]))
        await settle()
      }
      expect(harness.produced).toHaveLength(5)

      for (const reject of holds) {
        reject(new KafkaJSConnectionClosedError('Connection closed', { host: 'h', port: 1 }))
      }

      await expect(Promise.all(sends)).resolves.toHaveLength(6)
      expect(
        harness
          .sent()
          .slice(5)
          .flatMap(({ partitions }) => partitions)
          .sort()
      ).toEqual([0, 1, 2, 3, 4, 5].map((partition) => `topic-a/${partition}@0`))
    })

    test('a request that fails while the producer disconnects neither reconnects nor refreshes; one still on its way settles on its answer', async () => {
      const held = new Map()
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1, 1: 2 } },
        answer: (request, nodeId) =>
          new Promise((resolve, reject) =>
            held.set(nodeId, { answer: () => resolve(respond(request, nodeId)), reject })
          ),
      })
      await harness.start()
      const pending = harness
        .send([{ topic: TOPIC, messages: [...records(0), ...records(1)] }])
        .catch((e) => e)
      await settle()

      harness.setStatus(CONNECTION_STATUS.DISCONNECTING)
      harness.cluster.isConnected.mockReturnValue(false)
      held
        .get(1)
        .reject(new KafkaJSConnectionClosedError('Connection closed', { host: 'h', port: 1 }))
      await settle()
      held.get(2).answer()

      expect((await pending).message).toMatch(/disconnecting/)
      expect(harness.cluster.connect).not.toHaveBeenCalled()
      expect(harness.cluster.refreshMetadata).not.toHaveBeenCalled()
      expect(harness.manager.sequencer.entry(TOPIC, 1)).toMatchObject({ lastAckedSequence: 0 })
    })

    test('a producer that starts disconnecting while a pass waits on a leader hint neither reconnects, refreshes nor sends again', async () => {
      const hint = deferred()
      const firstAnswer = deferred()
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1, 1: 1 } },
        answer: async (request, nodeId) => {
          const [{ partitions }] = request.topicData
          if (partitions[0].partition === 1) {
            await firstAnswer.promise
            return respond(request, nodeId)
          }
          const results = respond(request, nodeId, { 'topic-a/0': NOT_LEADER_OR_FOLLOWER })
          results[0].error.currentLeader = { leaderId: 1, leaderEpoch: 2 }
          return results
        },
      })
      harness.cluster.applyLeaderHint.mockImplementation(() => hint.promise)
      await harness.start()
      const first = harness.send([{ topic: TOPIC, messages: records(1) }]).catch((e) => e)
      await settle()
      const held = harness.send([{ topic: TOPIC, messages: records(1) }]).catch((e) => e)
      await settle()
      const hinted = harness.send([{ topic: TOPIC, messages: records(0) }]).catch((e) => e)
      await settle()
      const produced = harness.produced.length

      harness.setStatus(CONNECTION_STATUS.DISCONNECTING)
      harness.cluster.isConnected.mockReturnValue(false)
      firstAnswer.resolve()
      await settle()
      hint.resolve(true)
      const [firstOutcome, heldOutcome, hintedOutcome] = await Promise.all([first, held, hinted])

      expect(harness.cluster.connect).not.toHaveBeenCalled()
      expect(harness.cluster.refreshMetadata).not.toHaveBeenCalled()
      expect(harness.produced).toHaveLength(produced)
      expect(firstOutcome).toEqual([expect.objectContaining({ partition: 1 })])
      expect(heldOutcome.message).toMatch(/disconnecting/)
      expect(hintedOutcome.message).toMatch(/disconnecting/)
    })

    test('a producer that starts disconnecting while the cluster reconnects does not refresh its metadata', async () => {
      let calls = 0
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1 } },
        answer: (request, nodeId) => {
          calls += 1
          if (calls === 1) {
            throw new KafkaJSConnectionClosedError('closed', { host: 'a', port: 9092 })
          }
          return respond(request, nodeId)
        },
      })
      await harness.start()
      harness.cluster.isConnected.mockReturnValue(false)
      harness.cluster.connect.mockImplementationOnce(async () => {
        harness.setStatus(CONNECTION_STATUS.DISCONNECTING)
      })

      const error = await harness.send([{ topic: TOPIC, messages: records(0) }]).catch((e) => e)

      expect(harness.cluster.connect).toHaveBeenCalledTimes(1)
      expect(harness.cluster.refreshMetadata).not.toHaveBeenCalled()
      expect(error.message).toMatch(/disconnecting/)
      expect(harness.produced).toHaveLength(1)
    })

    test('a batch on its way to a partition that then lost its leader settles on its answer while the queued ones are given up', async () => {
      const held = []
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1 } },
        answer: (request, nodeId) => {
          const release = deferred()
          held.push(() => release.resolve(respond(request, nodeId)))
          return release.promise
        },
      })
      await harness.start()
      jest.useFakeTimers({ now: 0 })

      const onItsWay = harness.send([{ topic: TOPIC, messages: records(0) }])
      await jest.advanceTimersByTimeAsync(0)
      delete harness.layout[TOPIC][0]
      const queued = harness.sendMessages
        .sendPartitioned({
          acks: -1,
          timeout: 30000,
          compression: 0,
          batches: [{ topic: TOPIC, partition: 0, messages: records(0) }],
        })
        .catch((e) => e)
      await jest.advanceTimersByTimeAsync(1000)

      const error = await queued
      expect(error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(error.cause.message).toEqual('No leader for topic-a partition 0')
      held[0]()
      await expect(onItsWay).resolves.toHaveLength(1)
      expect(harness.produced).toHaveLength(1)
    })

    test('a partition with no leader is refreshed and waited on exactly retry.retries times before its batches are rejected', async () => {
      const harness = createHarness({ layout: { [TOPIC]: {} } })
      await harness.start()
      jest.useFakeTimers({ now: 0 })

      const sending = harness.sendMessages
        .sendPartitioned({
          acks: -1,
          timeout: 30000,
          compression: 0,
          batches: [
            { topic: TOPIC, partition: 0, messages: records(0) },
            { topic: TOPIC, partition: 0, messages: records(0) },
          ],
        })
        .catch((e) => e)
      await jest.advanceTimersByTimeAsync(1000)
      const error = await sending

      expect(error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(error.retryCount).toBe(RETRY.retries)
      expect(harness.cluster.refreshMetadata).toHaveBeenCalledTimes(RETRY.retries)
      expect(harness.produced).toEqual([])
      expect(harness.manager.sequencer.entry(TOPIC, 0)).toBeNull()
    })

    test('a send settled from outside the loop leaves no backoff timer behind', async () => {
      const harness = createHarness({
        layout: { [TOPIC]: { 0: 1 } },
        answer: (request, nodeId) => respond(request, nodeId, { 'topic-a/0': REQUEST_TIMED_OUT }),
      })
      await harness.start()
      jest.useFakeTimers({ now: 0 })

      const sending = harness.send([{ topic: TOPIC, messages: records(0) }]).catch((e) => e)
      await jest.advanceTimersByTimeAsync(0)
      expect(jest.getTimerCount()).toBe(1)

      harness.manager.sequencer.abortQueued(new KafkaJSNonRetriableError('aborted'))
      await jest.advanceTimersByTimeAsync(0)

      expect((await sending).message).toEqual('aborted')
      expect(jest.getTimerCount()).toBe(0)
    })
  })

  describe('the order of a partition when requests fail on their way to the broker', () => {
    test('after a connection drop, a partition’s next batch waits for the re-send of the batch the drop failed', async () => {
      const log = createPartitionLog()
      const dropping = createDroppingBroker(log)
      const harness = createHarness({ layout: { [TOPIC]: { 0: 1, 1: 1 } } })
      harness.cluster.findBroker.mockImplementation(async () => dropping.broker)
      await harness.start()
      const request = (partition) =>
        harness.sendMessages
          .sendPartitioned({
            acks: -1,
            timeout: 30000,
            compression: 0,
            batches: [{ topic: TOPIC, partition, messages: records(partition) }],
          })
          .then(
            () => 'sent',
            (e) => e.message
          )

      const other = request(1)
      await settle()
      const first = request(0)
      await settle()
      dropping.drop()
      const next = request(0)
      const outcomes = Promise.all([other, first, next])
      const settled = settledFlag(outcomes)
      while (!settled()) {
        await new Promise((done) => setTimeout(done, 5))
        dropping.answer()
      }
      await dropping.destroy()

      expect(await outcomes).toEqual(['sent', 'sent', 'sent'])
      expect(log.written.filter((entry) => entry.startsWith('topic-a/0:'))).toEqual([
        'topic-a/0:0@e0',
        'topic-a/0:1@e0',
      ])
    })

    test.each(FUZZ_SEEDS)(
      'with lost connections, refusals and lost answers, every record is written once and in order (seed %i)',
      async (seed) => {
        const random = seededRandom(seed)
        const log = createPartitionLog()
        const unreliable = createUnreliableCluster(random, log)
        const harness = createHarness({
          layout: { [TOPIC]: { 0: 1, 1: 1, 2: 2, 3: 2 } },
          retry: { retries: 1000, initialRetryTime: 1, maxRetryTime: 5, factor: 0, multiplier: 2 },
          answer: unreliable.answer,
        })
        await harness.start()
        jest.useFakeTimers({ now: 0 })

        const expected = [[], [], [], []]
        const outcomes = []
        let at = 0
        for (let index = 0; index < 200; index += 1) {
          const partitions = [0, 1, 2, 3].filter(() => random() < 0.5)
          if (partitions.length === 0) {
            partitions.push(Math.floor(random() * 4))
          }
          const messages = partitions.map((partition) => ({ partition, value: `${index}` }))
          partitions.forEach((partition) => expected[partition].push(`${index}`))
          at += Math.floor(random() * 3)
          setTimeout(() => {
            outcomes.push(
              harness.send([{ topic: TOPIC, messages }]).then(
                () => 'sent',
                (e) => e.message
              )
            )
          }, at)
        }
        await jest.advanceTimersByTimeAsync(at + 1)
        const settled = settledFlag(Promise.all(outcomes))
        for (let step = 0; step < 10000 && !settled(); step += 1) {
          await jest.advanceTimersByTimeAsync(10)
        }

        const written = [[], [], [], []]
        for (const entry of log.written) {
          const [key, values] = entry.split(':')
          written[Number(key.split('/')[1])].push(...values.split('+'))
        }
        expect((await Promise.all(outcomes)).filter((outcome) => outcome !== 'sent')).toEqual([])
        expect(written).toEqual(expected)
        expect(unreliable.maxOutstanding()).toBeLessThanOrEqual(5)
        expect(harness.manager.sequencer.hasInFlight()).toBe(false)
      }
    )
  })
})
