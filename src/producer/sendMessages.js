const { failureSourceOf } = require('../broker/failureSource')
const describeThrown = require('../network/describeThrown')
const createRetry = require('../retry')
const {
  KafkaJSError,
  KafkaJSNonRetriableError,
  KafkaJSMetadataNotLoaded,
  KafkaJSInvariantViolation,
  KafkaJSDeliveryTimeout,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSTransactionAbortableError,
  KafkaJSRecordTooLargeError,
  KafkaJSProducerClosedError,
} = require('../errors')
const { staleMetadata } = require('../protocol/error')
const { RETRIABLE_CATEGORIES } = require('../protocol/errorCategories')
const { CONNECTION_STATUS } = require('../network/connectionStatus')
const { isConnectionFailure } = require('../network/connectionFailure')
const { isArmed } = require('./eosManager/transactionStates')
const { surfaceInTransaction } = require('./eosManager')
const groupMessagesPerPartition = require('./groupMessagesPerPartition')
const Fifo = require('../utils/fifo')
const { Deadline, ExpiryTimer, MAX_DELAY } = require('./deadline')
const { Types: Compression } = require('../protocol/message/compression')
const Accumulator = require('./state/accumulator')
const { RequestBudget, recordsBytes } = require('./state/recordSize')

const { keys } = Object

// The last Produce version that keys topics by name, and the highest a transactional send uses:
// from v12 a produce inside a transaction implicitly adds the partition to the transaction.
// @see clients/src/main/resources/common/message/ProduceRequest.json (Apache Kafka 4.3.1)
const PRODUCE_TOPIC_NAMES_MAX_VERSION = 11

const NO_PARTITION_METADATA = 'Producing to a partition without metadata'

/**
 * The error a send rejects with while the producer is not connected: the one `send()` raises up
 * front for that status.
 * @param {string} status a `CONNECTION_STATUS` value
 * @returns {KafkaJSError|null} null while connected
 */
const connectionStatusError = (status) => {
  switch (status) {
    case CONNECTION_STATUS.DISCONNECTING:
      return new KafkaJSNonRetriableError(
        `The producer is disconnecting; therefore, it can't safely accept messages anymore`
      )
    case CONNECTION_STATUS.DISCONNECTED:
      return new KafkaJSError('The producer is disconnected')
    default:
      return null
  }
}

/**
 * Resolves each `batchIndex` the broker reported (KIP-467, Produce v8+) to the record it refers
 * to: `BatchIndex` indexes the records of that partition's batch.
 * @param {Error} e the partition's produce error
 * @param {object[]} messages the records that partition's batch carried
 * @returns {void}
 */
const resolveFailingRecords = (e, messages) => {
  if (!Array.isArray(e.recordErrors) || e.recordErrors.length === 0) {
    return
  }

  e.recordErrors = e.recordErrors.map((recordError) => {
    const record = messages[recordError.batchIndex]
    return record === undefined ? recordError : { ...recordError, record }
  })
}

/**
 * Resolves the failing records of an error thrown by a whole produce request, through the topic
 * data that request carried.
 * @param {Error} e the error thrown by the produce request
 * @param {{topic: string, partitions: {partition: number, messages: object[]}[]}[]} topicData
 * @returns {void}
 */
const attachFailingRecords = (e, topicData) => {
  const matchedTopic = topicData.find(({ topic }) => topic === e.topic)
  const matchedPartition =
    matchedTopic && matchedTopic.partitions.find(({ partition }) => partition === e.partition)

  if (matchedPartition) {
    resolveFailingRecords(e, matchedPartition.messages)
  }
}

/**
 * Whether a failed produce means the client's metadata may be stale: the next attempt then
 * refreshes it first.
 * @param {Error} e
 * @returns {boolean}
 */
const refreshesMetadata = (e) =>
  staleMetadata({ type: describeThrown(e, 'type') }) ||
  describeThrown(e, 'name') === 'KafkaJSMetadataNotLoaded' ||
  isConnectionFailure(e) ||
  describeThrown(e, 'name') === 'KafkaJSConnectionClosedError' ||
  (describeThrown(e, 'name') === 'KafkaJSProtocolError' &&
    describeThrown(e, 'retriable') === 'true')

/**
 * The topic ids of a produce request, when every topic in it has one: the request can then be
 * negotiated at v13, which keys topics by id (KIP-516).
 * @param {import("../../types").Cluster} cluster
 * @param {Iterable<string>} topics
 * @returns {Map<string, string>|undefined} undefined when any topic's id is unknown
 */
const topicIdsOf = (cluster, topics) => {
  const topicIds = new Map()
  for (const topic of topics) {
    const topicId = cluster.topicId(topic)
    if (topicId === undefined) {
      return undefined
    }
    topicIds.set(topic, topicId)
  }
  return topicIds
}

/**
 * The leaders of a topic's partitions: one cluster lookup answers for them all. That lookup
 * throws for the whole call when any partition has no leader, so after a throw each partition
 * is looked up on its own: the ones with a leader still route, and the others come back with
 * the error of their own lookup. A partition the cache does not hold answers without throwing;
 * it comes back with the error a send gives it when no lookup throws.
 * @param {import("../../types").Cluster} cluster
 * @param {string} topic
 * @param {(string|number)[]} partitions
 * @returns {{ leaders: { [nodeId: string]: number[] },
 *   failed: { partition: string|number, error: Error }[] }}
 */
const leadersOfPartitions = (cluster, topic, partitions) => {
  try {
    return { leaders: cluster.findLeaderForPartitions(topic, partitions), failed: [] }
  } catch {
    const leaders = {}
    const failed = []
    for (const partition of partitions) {
      try {
        const own = cluster.findLeaderForPartitions(topic, [partition])
        const nodeIds = keys(own)
        if (nodeIds.length === 0) {
          failed.push({ partition, error: new KafkaJSMetadataNotLoaded(NO_PARTITION_METADATA) })
        }
        for (const nodeId of nodeIds) {
          leaders[nodeId] = [...(leaders[nodeId] ?? []), ...own[nodeId]]
        }
      } catch (error) {
        failed.push({ partition, error })
      }
    }
    return { leaders, failed }
  }
}

/**
 * The topic data of one Produce request: the drained batches by topic, each partition carrying
 * the sequence its batch was numbered with.
 * @param {import('./state/partitionSequencer').PartitionBatch[]} batches
 * @returns {{ topic: string, partitions: { partition: number, firstSequence: number, messages: object[] }[] }[]}
 */
const topicDataOf = (batches) => {
  const topicData = []
  for (const { topic, partition, firstSequence, messages } of batches) {
    let entry = topicData.find((candidate) => candidate.topic === topic)
    if (entry === undefined) {
      entry = { topic, partitions: [] }
      topicData.push(entry)
    }
    const data = { partition, messages }
    if (firstSequence !== undefined) data.firstSequence = firstSequence
    entry.partitions.push(data)
  }
  return topicData
}

/**
 * Finds a batch's result in a Produce response. The broker answers in request order, so the
 * result at the batch's own position is tried first.
 * @param {import('../protocol/requests/produce/partitionResults').PartitionResult[]} results
 * @param {number} index the batch's position in the request
 * @param {{ topic: string, partition: number }} batch
 * @returns {import('../protocol/requests/produce/partitionResults').PartitionResult|undefined}
 */
const resultFor = (results, index, { topic, partition }) => {
  const matches = (result) => result.topicName === topic && result.partition === partition
  return results[index] !== undefined && matches(results[index])
    ? results[index]
    : results.find(matches)
}

/**
 * The `RecordMetadata` a successful partition result resolves to.
 * @param {import('../protocol/requests/produce/partitionResults').PartitionResult} result
 * @returns {object}
 */
const metadataOf = ({
  topicName,
  partition,
  errorCode,
  baseOffset,
  logAppendTime,
  logStartOffset,
}) => ({
  topicName,
  partition,
  errorCode,
  baseOffset,
  logAppendTime,
  logStartOffset,
})

/**
 * The `RecordMetadata` of a batch the broker already had (DUPLICATE_SEQUENCE_NUMBER): a success
 * with no valid offset or timestamp, as the Java producer reports it.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java
 * @param {{ topic: string, partition: number }} batch
 * @param {{ logStartOffset: string }} result the partition's result
 * @returns {object}
 */
const duplicateMetadataOf = ({ topic, partition }, result) => ({
  topicName: topic,
  partition,
  errorCode: 0,
  baseOffset: '-1',
  logAppendTime: '-1',
  logStartOffset: result.logStartOffset,
})

/** Sorts `RecordMetadata` by topic name, then partition. */
const byTopicThenPartition = (a, b) =>
  a.topicName < b.topicName ? -1 : a.topicName > b.topicName ? 1 : a.partition - b.partition

/**
 * Settles every caller of a batch with the same value, once: after a fault in the loop (see
 * `DrainLoop#fault`) a batch on the wire is rejected before its answer arrives, and the answer
 * then settles nobody.
 * @param {import('./state/partitionSequencer').PartitionBatch} batch
 * @param {object} metadata
 * @returns {void}
 */
const resolveBatch = (batch, metadata) => {
  if (deliveryOf(batch).settled) {
    return
  }
  for (const settler of batch.settlers) {
    settler.resolve(metadata)
  }
}

/**
 * Settles every caller of a batch with the same error, once, as `resolveBatch` does.
 * @param {import('./state/partitionSequencer').PartitionBatch} batch
 * @param {Error} error
 * @returns {void}
 */
const rejectBatch = (batch, error) => {
  if (deliveryOf(batch).settled) {
    return
  }
  for (const settler of batch.settlers) {
    settler.reject(error)
  }
}

/** An internal, uncounted refusal of one request that has never been written. */
class ProduceAttemptRefused extends Error {}

/** The refusal of a request whose encoded frame is above `maxRequestSize`; nothing was written. */
class ProduceAttemptTooLarge extends ProduceAttemptRefused {
  /**
   * @param {number} frameLength the frame's bytes, its length prefix included
   * @param {number} limit `maxRequestSize`
   */
  constructor(frameLength, limit) {
    super(`A ${frameLength}-byte Produce request is above maxRequestSize (${limit} bytes)`)
    this.frameLength = frameLength
    this.limit = limit
  }
}

/**
 * The error a batch fails with when no request under `maxRequestSize` can carry it: one record,
 * or a batch whose earlier attempt was written and so keeps its records together.
 * @param {{ topic: string, partition: number, messages: object[] }} batch
 * @param {ProduceAttemptTooLarge} refusal
 * @returns {KafkaJSRecordTooLargeError}
 */
const tooLarge = ({ topic, partition, messages }, { frameLength, limit }) =>
  new KafkaJSRecordTooLargeError(
    `${topic} partition ${partition} needs a ${frameLength}-byte Produce request, above ` +
      `maxRequestSize (${limit} bytes)`,
    {
      reason: 'maxRequestSize',
      scope: messages.length === 1 ? 'record' : 'send',
      size: frameLength,
      limit,
      topic,
    }
  )

/** One immutable request membership and the right to transmit it once. */
class ProduceAttempt {
  /**
   * @param {Delivery[]} deliveries fixed constituent delivery objects
   * @param {() => string} getConnectionStatus producer connection status
   * @param {(delivery: Delivery) => void} expire settles a due delivery synchronously; the drain
   *   loop's also settles every other due delivery
   * @param {number} [maxRequestSize=Infinity] cap checked at transmission
   * @param {import('./generation')} [generation] captured lifecycle; never recaptured
   */
  constructor(deliveries, getConnectionStatus, expire, maxRequestSize = Infinity, generation) {
    this.deliveries = deliveries.slice()
    this.getConnectionStatus = getConnectionStatus
    this.expire = expire
    this.maxRequestSize = maxRequestSize
    this.generation = generation
    this.written = false
    this.revoked = false
    for (const delivery of this.deliveries) delivery.currentAttempt = this
  }

  /** Revokes only this attempt, even after an older attempt wrote. @returns {void} */
  revoke() {
    if (!this.written) this.revoked = true
  }

  /**
   * At transmission a frame above `maxRequestSize` is refused, unwritten; a written attempt ends
   * the size subdivision (`child`) of the batches it carries.
   * @param {number} [frameLength] supplied only at transmission; includes the prefix and header
   * @returns {Error|null} refusal before any write, or null
   */
  guard = (frameLength) => {
    if (this.revoked) return new ProduceAttemptRefused('The Produce attempt was revoked')
    const closed = this.generation?.guard()
    if (closed) return closed
    const statusError = connectionStatusError(this.getConnectionStatus())
    if (statusError) return statusError
    if (frameLength === undefined) return null
    const now = Date.now()
    for (const delivery of this.deliveries) {
      if (!delivery.settled && now >= delivery.deadline) this.expire(delivery)
    }
    if (this.revoked || this.deliveries.some((delivery) => delivery.settled)) {
      this.revoke()
      return new ProduceAttemptRefused('A delivery in this Produce attempt has settled')
    }
    if (frameLength > this.maxRequestSize) {
      this.revoke()
      return new ProduceAttemptTooLarge(frameLength, this.maxRequestSize)
    }
    this.written = true
    for (const delivery of this.deliveries) {
      delivery.everWritten = true
      delivery.batch.child = null
    }
    return null
  }
}

/**
 * One `sendPartitioned()` call: it settles once every batch it created has settled, resolving
 * with their `RecordMetadata` by topic, then partition, or rejecting with the first error any of
 * them failed with. The metadata of batches that succeeded is then discarded.
 */
class Send {
  #pending
  #error = null
  #metadata = []
  #onSettled
  #resolve
  #reject

  /**
   * @param {number} count the batches this send owns
   * @param {(send: Send) => void} onSettled called once, when the last batch has settled
   */
  constructor(count, onSettled) {
    this.promise = new Promise((resolve, reject) => {
      this.#resolve = resolve
      this.#reject = reject
    })
    this.#pending = count
    this.#onSettled = onSettled
    /** @type {Delivery[]} */
    this.deliveries = []
  }

  /**
   * One batch more, cut from one of its own.
   * @param {Delivery} delivery
   * @returns {void}
   */
  add(delivery) {
    this.#pending += 1
    this.deliveries.push(delivery)
  }

  /**
   * One batch delivered.
   * @param {object} metadata
   * @returns {void}
   */
  delivered(metadata) {
    this.#metadata.push(metadata)
    this.#settleOne()
  }

  /**
   * One batch failed.
   * @param {Error} error
   * @returns {void}
   */
  failed(error) {
    this.#error = this.#error || error
    this.#settleOne()
  }

  /** @returns {void} */
  #settleOne() {
    this.#pending -= 1
    if (this.#pending > 0) {
      return
    }

    this.#onSettled(this)
    if (this.#error !== null) {
      this.#reject(this.#error)
    } else {
      this.#resolve(this.#metadata.sort(byTopicThenPartition))
    }
  }
}

/**
 * The drain loop's own settler of one batch, and what the loop knows about it: whether it is
 * settled or on the wire, its deadline, its last error and its last backoff. A batch is resolved
 * only by its request's answer and then retires (see the accumulator's `retire`); one rejected while
 * no request carries it retires at once, and one rejected on the wire when its request's answer or
 * failure arrives.
 */
class Delivery {
  /**
   * @param {Send} send
   * @param {number} deadline when the batch's delivery timeout runs out; Infinity without one
   * @param {(delivery: Delivery) => void} detachDeadline removes this delivery's deadline ownership
   */
  constructor(send, deadline, detachDeadline) {
    this.send = send
    this.deadline = deadline
    this.detachDeadline = detachDeadline
    this.expiryPrevious = null
    this.expiryNext = null
    this.expiryLinked = false
    /** @type {import('./state/partitionSequencer').PartitionBatch} */
    this.batch = null
    this.settled = false
    this.dispatched = false
    this.everWritten = false
    this.expired = false
    this.currentAttempt = null
    /** @type {Error|undefined} */
    this.lastError = undefined
    /** @type {number|undefined} the last backoff, in ms */
    this.retryTime = undefined
    this.routingFailures = 0
  }

  /**
   * Drops the drain copy's payload and retires its accumulator owner once the attempt ends.
   * @returns {void}
   */
  retire() {
    this.batch.messages = null
    this.batch.retire?.()
  }

  /**
   * Settles the batch for its send, once; a later call does nothing. The batch's other callers are
   * dropped when its deadline settles it, but this delivery stays in `batch.settlers`, so a loop
   * over them - `abortQueued` when a transaction arms - can reach it again.
   * @param {object} metadata
   * @returns {void}
   */
  resolve(metadata) {
    if (this.settled) return
    this.settled = true
    this.detachDeadline(this)
    this.retire()
    this.send.delivered(metadata)
  }

  /**
   * Settles the batch for its send, once; a later call does nothing (see `resolve`). A repeat
   * would count the batch twice against its send and release a live delivery's deadline.
   * @param {Error} error
   * @returns {void}
   */
  reject(error) {
    if (this.settled) return
    this.settled = true
    this.detachDeadline(this)
    if (!this.dispatched) {
      if (this.expired) this.batch.retire?.()
      else this.retire()
    }
    this.send.failed(error)
  }
}

/**
 * The loop's settler of a batch: the last one `sendPartitioned()` adds.
 * @param {import('./state/partitionSequencer').PartitionBatch} batch
 * @returns {Delivery}
 */
const deliveryOf = (batch) => batch.settlers[batch.settlers.length - 1]

/**
 * The most Produce requests the loop keeps outstanding to one broker - Java's
 * `max.in.flight.requests.per.connection` for an idempotent producer. Each request carries at most
 * one batch per partition, and the manager holds a partition's next batch until the one before it
 * was answered.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java
 */
const MAX_REQUESTS_PER_BROKER = 5

const RESOLVED = Promise.resolve()
const ignore = () => {}

/**
 * A producer lifecycle as the trailing argument of a cluster call, or no argument at all: a call
 * with no lifecycle keeps the shape it always had.
 * @param {import('./generation')} [fence]
 * @returns {[{ fence: import('./generation') }]|[]}
 */
const fenceArgs = (fence) => (fence ? [{ fence }] : [])

/**
 * The idempotent send path: per-partition batches queued in the producer's `ProducerStateManager`
 * and drained to their leaders, a Produce request per broker and options triple, until every send
 * has settled.
 *
 * One loop serves every send of an eosManager. A pass drains each broker until the manager has
 * nothing left for it, or until the broker holds `MAX_REQUESTS_PER_BROKER` requests of this loop
 * without an answer - Java's `client.ready(node)` - so the batches of different partitions go out
 * pipelined while the rest wait in their partition's queue, unnumbered. Within one partition the
 * manager drains a new batch only once the one before it was answered, so no batch can reach the
 * broker ahead of an earlier one of its partition, whatever happens to a request on its way.
 * Requests reach each broker in the order they were drained. Each response settles its batches:
 * a success completes the batch, a failure goes to the manager, whose verdict re-queues the batch
 * with the retrier's backoff or rejects it. Between passes the loop sleeps until a response, a new
 * send, a manager event, or the earliest backoff or deadline of a partition's first queued batch.
 *
 * Each batch keeps its partition, records and sequence across re-sends; the partitioner never runs
 * again, and a failure fails only its own partition's batch. `retry.retries` bounds the re-sends
 * of one batch, and `deliveryTimeout` (or `retry.maxTotalTime`) the time from `send()` to the
 * batch's last answer.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/RecordAccumulator.java
 */
class DrainLoop {
  #logger
  #cluster
  #eosManager
  #retries
  #backoff
  #deadlineMs
  #compressionLevels
  #getConnectionStatus
  #budget
  #generation
  /** @type {Set<Send>} */
  #sends = new Set()
  /** @type {Map<string, Map<number, Fifo<Delivery>>>} every partition's live batches, by enqueue order */
  #queues = new Map()
  /** @type {Map<string, number>} Produce requests sent to a broker and not yet answered */
  #outstanding = new Map()
  /** @type {Map<string, Promise<void>>} per broker, settles once the last request was handed on */
  #handoffs = new Map()
  /** @type {Promise<void>[]} leader hints to apply before the next drain */
  #hints = []
  #nextId = 0
  #running = false
  #dirty = false
  #refresh = false
  #wakeAt = Infinity
  #wake = null
  #timer = null
  #watched = null
  #expiry
  #expiryHead = null
  #expiryTail = null
  /** @type {{ batch: object, error: Error, transmitted: boolean }[]} settled, state not yet applied */
  #expired = []

  /**
   * @param {object} options
   * @param {import("../../types").Logger} options.logger
   * @param {import("../../types").Cluster} options.cluster
   * @param {object} options.eosManager the facade whose `stateManager` the loop drains
   * @param {import("../../types").RetryOptions} options.retry the producer's merged retry options
   * @param {number} [options.deliveryTimeout] ms from `send()` to a batch's last answer
   * @param {{ gzip?: number, zstd?: number }} [options.compressionLevels] per-codec levels
   * @param {() => string} options.getConnectionStatus
   * @param {RequestBudget} [options.budget] the request-size plan of each request it drains, and
   *   the `maxRequestSize` its frames are refused above
   * @param {import('./generation')} [options.generation] this loop's captured lifecycle
   */
  constructor({
    logger,
    cluster,
    eosManager,
    retry,
    deliveryTimeout,
    compressionLevels,
    getConnectionStatus,
    budget = new RequestBudget(),
    generation,
  }) {
    this.#logger = logger
    this.#cluster = cluster
    this.#eosManager = eosManager
    this.#retries = retry.retries ?? createRetry.defaults.retries
    this.#backoff = createRetry.backoff(retry)
    this.#compressionLevels = compressionLevels
    this.#deadlineMs = deliveryTimeout ?? retry.maxTotalTime
    this.#getConnectionStatus = getConnectionStatus
    this.#budget = budget
    this.#generation = generation
    this.#expiry = new ExpiryTimer(this.#expireDue)
  }

  /**
   * Settles every retained child before applying any forced-close consequence.
   * Outstanding requests keep their ownership until their terminal callbacks.
   * @param {Error} [surfaced] what the callers reject with instead of the close error, which the
   *   state consequences still see
   * @returns {void}
   */
  forceClose(surfaced) {
    for (const partitions of this.#queues.values()) {
      for (const queue of partitions.values()) {
        for (const delivery of queue) {
          this.#expire(
            delivery,
            new KafkaJSProducerClosedError('The producer was closed', {
              outcome: delivery.everWritten ? 'unknown' : 'unsent',
            }),
            surfaced
          )
        }
      }
    }
    this.#settleDue(-Infinity)
    this.#kick()
  }

  /**
   * Queues one batch per entry and settles once all of them have. Every entry's `settlers` are
   * settled with its batch: resolved with the batch's `RecordMetadata`, or rejected with its error.
   * A transactional producer registers each partition with the transaction here, before anything
   * is drained.
   * @param {{ acks: number, timeout: number, compression: number,
   *   batches: { topic: string, partition: number, messages: object[], settlers?: object[],
   *   createdAt?: number }[] }} request
   * @returns {Promise<object[]>}
   */
  send({ acks, timeout, compression, batches }) {
    const manager = this.#eosManager.stateManager
    const send = new Send(batches.length, this.#settled)
    const now = Date.now()
    const queued = batches.map(
      ({
        topic,
        partition,
        messages,
        settlers = [],
        createdAt = now,
        heldBytes,
        debt,
        retire,
        ownership,
        wire = recordsBytes(messages),
      }) => {
        const queue = this.#queueOf(topic, Number(partition))
        const delivery = new Delivery(
          send,
          new Deadline(createdAt, this.#deadlineMs).dueAt,
          this.#detachDeadline
        )
        delivery.batch = {
          id: this.#nextId++,
          topic,
          partition: Number(partition),
          messages,
          recordCount: messages.length,
          acks,
          timeout,
          compression,
          firstSequence: null,
          producerId: null,
          producerEpoch: null,
          attempt: 0,
          settlers: [...settlers, delivery],
          backoffUntil: 0,
          createdAt,
          heldBytes,
          debt,
          retire,
          ownership,
          wire,
          child: null,
          sequenceReset: false,
        }
        send.deliveries.push(delivery)
        queue.push(delivery)
        return delivery.batch
      }
    )

    if (this.#eosManager.isTransactional()) {
      this.#register(manager, queued)
    }

    this.#sends.add(send)
    manager.enqueue(queued)
    for (const delivery of send.deliveries) {
      if (delivery.settled) continue
      if (now >= delivery.deadline) this.#expire(delivery)
      else this.#attachDeadline(delivery)
    }
    this.#expireDue(now)
    return send.promise
  }

  /**
   * A partition's live batches, by enqueue order - which is the order its queue drains in.
   * @param {string} topic
   * @param {number} partition
   * @returns {Fifo<Delivery>}
   */
  #queueOf(topic, partition) {
    let partitions = this.#queues.get(topic)
    if (partitions === undefined) {
      partitions = new Map()
      this.#queues.set(topic, partitions)
    }

    let queue = partitions.get(partition)
    if (queue === undefined) {
      queue = new Fifo()
      partitions.set(partition, queue)
    }
    return queue
  }

  /**
   * Registers each partition with the transaction; under transaction version 1 the loop wakes
   * when the AddPartitionsToTxn carrying it settles. Nothing is registered once an error state is
   * armed: the enqueue that follows rejects the batches at once.
   * @param {import('./state')} manager
   * @param {import('./state/partitionSequencer').PartitionBatch[]} batches
   * @returns {void}
   */
  #register(manager, batches) {
    if (isArmed(manager.state())) {
      return
    }

    let watched = null
    for (const { topic, partition } of batches) {
      const added = manager.addPartitionToTransaction(topic, partition)
      if (added !== null && added !== watched) {
        watched = added
        added.then(this.#kick, this.#kick)
      }
    }
  }

  /** Forgets a send once its last batch has settled; the last one lets the loop end. */
  #settled = (send) => {
    this.#sends.delete(send)
    if (this.#sends.size === 0) this.#expiry.clear()
    if (this.#sends.size === 0) {
      this.#kick()
    }
  }

  /** Runs a pass soon: now if the loop is asleep, after the current one if it is running. */
  #kick = () => {
    this.#dirty = true
    if (this.#timer !== null) {
      clearTimeout(this.#timer)
      this.#timer = null
    }
    if (this.#wake !== null) {
      const wake = this.#wake
      this.#wake = null
      wake()
    }
    if (!this.#running) {
      this.#running = true
      Promise.resolve().then(this.#run)
    }
  }

  /**
   * Passes until no send is left, sleeping between passes. A pass that throws fails every live
   * send with its error, and the next send starts the loop again.
   * @returns {Promise<void>}
   */
  #run = async () => {
    try {
      while (this.#sends.size > 0) {
        if (!this.#dirty) {
          await this.#sleep()
        }
        this.#dirty = false
        await this.#pass()
      }
    } catch (error) {
      this.#fault(error)
    } finally {
      this.#running = false
    }
  }

  /**
   * Fails every live send with `error`, after a fault the loop's own code raised: a batch still
   * queued leaves its queue as `#abandon` describes; a batch on the wire is settled now and its
   * answer, when it comes, only updates the sequencer. A transactional producer is then unusable:
   * the fault came from the loop's own code - an error that is not this client's own is armed as
   * an invariant violation - and a batch it settled may still be on the wire.
   * @param {Error} error
   * @returns {void}
   */
  #fault = (error) => {
    this.#logger.error(`The idempotent send loop failed: ${error.message}`, { stack: error.stack })
    const manager = this.#eosManager.stateManager
    for (const send of [...this.#sends]) {
      for (const delivery of send.deliveries) {
        if (delivery.settled) {
          continue
        }
        if (delivery.dispatched) {
          rejectBatch(delivery.batch, error)
        } else {
          this.#abandon(manager, delivery, error)
        }
      }
    }
    this.#inTransaction(
      manager,
      error instanceof KafkaJSError ? error : new KafkaJSInvariantViolation(error)
    )
  }

  /**
   * Waits for a kick, or for the earliest backoff or deadline of a queued batch. A wake further
   * away than a timer can hold arrives early, and the next pass sleeps toward it again.
   * @returns {Promise<void>}
   */
  #sleep() {
    if (this.#wakeAt !== Infinity) {
      this.#timer = setTimeout(
        this.#kick,
        Math.min(MAX_DELAY, Math.max(0, this.#wakeAt - Date.now()))
      )
    }
    return new Promise((resolve) => {
      this.#wake = resolve
    })
  }

  /**
   * One pass. While the producer is not connected it rejects every queued batch with the error
   * `send()` raises for that status and does nothing else - checked again after the leader hints
   * and the metadata refresh it awaits, so a `disconnect()` begun meanwhile neither reconnects
   * the cluster nor sends anything new. Otherwise it settles the queued batches
   * whose deadline passed, then drains every broker that leads a partition whose first queued
   * batch is out of its backoff, a request per drain, until the manager has nothing left for it or
   * the broker holds `MAX_REQUESTS_PER_BROKER` requests. Arming an error state rejects the queued
   * batches itself, and the manager drains nothing while one is armed.
   * @returns {Promise<void>}
   */
  async #pass() {
    const manager = this.#eosManager.stateManager
    if (this.#generation?.closed) return
    this.#wakeAt = Infinity
    if (this.#abandonIfNotConnected(manager)) {
      return
    }

    if (this.#hints.length > 0) {
      await Promise.all(this.#hints.splice(0))
    }
    if (this.#refresh) {
      this.#refresh = false
      await this.#refreshMetadata()
    }
    if (this.#abandonIfNotConnected(manager)) {
      return
    }

    if (!manager.isInitialized()) {
      this.#watch(manager.ensureProducerId())
    }

    const now = Date.now()
    this.#settleDue(now)
    /** @type {Map<string, Map<number, Delivery>>} the first queued batch of each ready partition */
    const ready = new Map()
    for (const [topic, partitions] of this.#queues) {
      this.#cluster.touchTargetTopic?.(topic)
      for (const [partition, queue] of partitions) {
        const head = this.#headOf(queue)
        if (queue.size === 0) {
          partitions.delete(partition)
          continue
        }
        if (head === undefined) {
          continue
        }

        this.#wakeAt = Math.min(this.#wakeAt, head.deadline)
        if (head.batch.backoffUntil > now) {
          this.#wakeAt = Math.min(this.#wakeAt, head.batch.backoffUntil)
          continue
        }
        if (!ready.has(topic)) {
          ready.set(topic, new Map())
        }
        ready.get(topic).set(partition, head)
      }
      if (partitions.size === 0) {
        this.#queues.delete(topic)
      }
    }

    const leaders = this.#leadersOf(manager, ready, now)
    const nodes = new Set()
    for (const topic of keys(leaders)) {
      for (const nodeId of keys(leaders[topic])) {
        nodes.add(nodeId)
      }
    }

    for (const nodeId of nodes) {
      const broker = { nodeId }
      while ((this.#outstanding.get(nodeId) ?? 0) < MAX_REQUESTS_PER_BROKER) {
        const drained = manager.drain(broker, leaders, this.#budget)
        if (drained.length === 0) {
          break
        }
        this.#dispatch(nodeId, drained)
      }
    }
  }

  /**
   * A partition's first queued batch: the batches it has settled are dropped from the front of its
   * queue, and the first one neither settled nor on the wire is returned. Expiry has already run.
   * @param {Fifo<Delivery>} queue
   * @returns {Delivery|undefined} undefined when every batch left is on the wire
   */
  #headOf(queue) {
    while (queue.size > 0 && queue.peek().settled) queue.shift()
    for (const delivery of queue) {
      if (!delivery.settled && !delivery.dispatched) return delivery
    }
    return undefined
  }

  /**
   * Links a finite deadline into the owner's sorted delivery list; arrivals normally append.
   * @param {Delivery} delivery
   * @returns {void}
   */
  #attachDeadline(delivery) {
    if (delivery.deadline === Infinity) return
    let previous = this.#expiryTail
    while (previous && previous.deadline > delivery.deadline) previous = previous.expiryPrevious
    delivery.expiryPrevious = previous
    delivery.expiryNext = previous ? previous.expiryNext : this.#expiryHead
    if (delivery.expiryNext) delivery.expiryNext.expiryPrevious = delivery
    else this.#expiryTail = delivery
    if (previous) previous.expiryNext = delivery
    else this.#expiryHead = delivery
    delivery.expiryLinked = true
    this.#expiry.arm(this.#expiryHead.deadline)
  }

  /** Unlinks settlement in constant time and re-arms only when the head changes. @param {Delivery} delivery @returns {void} */
  #detachDeadline = (delivery) => {
    if (!delivery.expiryLinked) return
    const wasHead = this.#expiryHead === delivery
    if (delivery.expiryPrevious) delivery.expiryPrevious.expiryNext = delivery.expiryNext
    else this.#expiryHead = delivery.expiryNext
    if (delivery.expiryNext) delivery.expiryNext.expiryPrevious = delivery.expiryPrevious
    else this.#expiryTail = delivery.expiryPrevious
    delivery.expiryPrevious = delivery.expiryNext = null
    delivery.expiryLinked = false
    if (wasHead) {
      this.#expiry.clear()
      this.#expiry.arm(this.#expiryHead?.deadline ?? Infinity)
    }
  }

  /**
   * Settles all due deliveries even while the drain pass awaits work, and runs a pass. The owner
   * timer's callback, and the entry a send and the write-boundary guard take.
   * @param {number} now epoch milliseconds
   * @returns {number} the next deadline, or Infinity
   */
  #expireDue = (now) => {
    this.#settleDue(now)
    this.#kick()
    return this.#expiryHead?.deadline ?? Infinity
  }

  /**
   * Settles every due delivery with its own `KafkaJSDeliveryTimeout`, then applies the state
   * consequences of all of them: the one path every synchronous expiry takes. Arming a transaction
   * rejects the other queued batches, so no consequence may run while a due delivery still waits
   * for its own timeout.
   * @param {number} now epoch milliseconds
   * @returns {void}
   */
  #settleDue(now) {
    while (this.#expiryHead && now >= this.#expiryHead.deadline) this.#expire(this.#expiryHead)
    const manager = this.#eosManager.stateManager
    for (const { batch, error, transmitted } of this.#expired.splice(0)) {
      manager.fail(batch, error, { transmitted })
      const delivery = deliveryOf(batch)
      if (!delivery.dispatched) delivery.retire()
    }
  }

  /**
   * Rejects every queued batch with `error`, the producer's connection status, as `#abandon`
   * describes. A transactional producer that dropped a batch so is then abort-only: the armed
   * error is a `KafkaJSTransactionAbortableError` with `error` as its cause, never `error` itself,
   * whose category could make the producer unusable for a mere `disconnect()`.
   * @param {import('./state')} manager
   * @param {Error} error
   * @returns {void}
   */
  #abandonQueued(manager, error) {
    let dropped = false
    for (const partitions of this.#queues.values()) {
      for (const queue of partitions.values()) {
        for (const delivery of queue) {
          if (!delivery.settled && !delivery.dispatched) {
            this.#abandon(manager, delivery, error)
            dropped = true
          }
        }
      }
    }

    if (dropped && this.#eosManager.isTransactional()) {
      manager.arm(
        new KafkaJSTransactionAbortableError(
          `A batch of the transaction was dropped (${error.message}); call abort() on it before starting another`,
          { cause: error }
        )
      )
    }
  }

  /**
   * The leader of every ready partition, from the cluster's metadata. A partition with no leader
   * waits out its first batch's backoff with a metadata refresh, and its queued batches are
   * rejected once that batch has waited `retry.retries` times.
   * @param {import('./state')} manager
   * @param {Map<string, Map<number, Delivery>>} ready
   * @param {number} now
   * @returns {import('./state/partitionSequencer').LeaderMap} keyed by topic, without a
   *   prototype: a topic may be named `toString` or `__proto__`
   */
  #leadersOf(manager, ready, now) {
    const leaders = Object.create(null)
    for (const [topic, heads] of ready) {
      const { leaders: routed, failed } = leadersOfPartitions(this.#cluster, topic, [
        ...heads.keys(),
      ])
      for (const { partition, error } of failed) {
        this.#logger.debug(`No leader for ${topic} partition ${partition}: ${error.message}`)
      }

      leaders[topic] = routed
      const found = new Set()
      for (const nodeId of keys(routed)) {
        routed[nodeId].forEach((id) => found.add(Number(id)))
      }
      for (const [partition, head] of heads) {
        if (!found.has(partition)) {
          this.#unrouted(manager, this.#queues.get(topic).get(partition), head, now)
        }
      }
    }

    return leaders
  }

  /**
   * A ready partition with no leader: refresh the metadata and let its first batch wait a backoff;
   * once that batch has waited `retry.retries` times, reject every batch the partition has queued.
   * @param {import('./state')} manager
   * @param {Fifo<Delivery>} queue the partition's live batches
   * @param {Delivery} head its first queued batch
   * @param {number} now
   * @returns {void}
   */
  #unrouted(manager, queue, head, now) {
    const { batch } = head
    const error = new KafkaJSMetadataNotLoaded(
      `No leader for ${batch.topic} partition ${batch.partition}`
    )
    if (head.routingFailures >= this.#retries) {
      const exhausted = this.#inTransaction(
        manager,
        new KafkaJSNumberOfRetriesExceeded(error, {
          retryCount: head.routingFailures,
          retryTime: head.retryTime,
        })
      )
      for (const delivery of queue) {
        if (!delivery.settled && !delivery.dispatched) {
          this.#abandon(manager, delivery, exhausted)
        }
      }
      return
    }

    this.#refresh = true
    head.routingFailures += 1
    head.lastError = error
    batch.backoffUntil = now + this.#nextRetryTime(head)
    this.#wakeAt = Math.min(this.#wakeAt, batch.backoffUntil)
  }

  /**
   * Hands one drained set of batches to its broker - after the requests drained before it, so the
   * broker receives them in drain order - and settles them on the answer.
   * @param {string} nodeId
   * @param {import('./state/partitionSequencer').PartitionBatch[]} batches
   * @returns {void}
   */
  #dispatch(nodeId, batches) {
    const attempt = new ProduceAttempt(
      batches.map(deliveryOf),
      this.#getConnectionStatus,
      () => this.#expireDue(Date.now()),
      this.#budget.maxRequestSize,
      this.#generation
    )
    for (const batch of batches) {
      deliveryOf(batch).dispatched = true
    }
    this.#outstanding.set(nodeId, (this.#outstanding.get(nodeId) ?? 0) + 1)

    const handed = (this.#handoffs.get(nodeId) ?? RESOLVED).then(() =>
      this.#handOff(nodeId, batches, attempt)
    )
    this.#handoffs.set(nodeId, handed.then(ignore, ignore))
    handed
      .then(({ answer }) => answer)
      .then(
        (results) => this.#answered(nodeId, batches, results),
        (error) => this.#rejected(nodeId, batches, error)
      )
      .catch(this.#fault)
  }

  /**
   * Starts one Produce request, under the producer id and epoch the batches were numbered with and
   * the options triple they share.
   * @param {string} nodeId
   * @param {import('./state/partitionSequencer').PartitionBatch[]} batches
   * @param {ProduceAttempt} attempt fixed request membership and transmission right
   * @returns {Promise<{ answer: Promise<import('../protocol/requests/produce/partitionResults').PartitionResult[]|undefined> }>}
   *   once the request is handed to the broker; `answer` settles with its answer, and is wrapped so
   *   that waiting for the hand-off does not wait for the answer
   */
  async #handOff(nodeId, batches, attempt) {
    const refusal = attempt.guard()
    if (refusal) throw refusal
    const broker = await this.#cluster.findBroker({
      nodeId,
      ...(this.#generation ? { fence: this.#generation } : {}),
    })
    const refused = attempt.guard()
    if (refused) throw refused
    const [{ acks, timeout, compression, producerId, producerEpoch }] = batches
    const topicData = topicDataOf(batches)
    const transactional = this.#eosManager.isTransactional()

    const answer = broker.produce({
      transactionalId: transactional ? this.#eosManager.getTransactionalId() : undefined,
      producerId,
      producerEpoch,
      acks,
      timeout,
      compression,
      compressionLevel:
        compression === Compression.GZIP
          ? this.#compressionLevels?.gzip
          : compression === Compression.ZSTD
            ? this.#compressionLevels?.zstd
            : undefined,
      topicData,
      topicIds: transactional
        ? undefined
        : topicIdsOf(
            this.#cluster,
            topicData.map(({ topic }) => topic)
          ),
      transactionVersion: this.#eosManager.stateManager.transactionVersion(),
      perPartitionResults: true,
      guard: attempt.guard,
    })
    return { answer }
  }

  /**
   * A broker's request settled: one fewer outstanding, and the loop drains again.
   * @param {string} nodeId
   * @returns {void}
   */
  #answeredBy(nodeId) {
    const outstanding = this.#outstanding.get(nodeId) - 1
    if (outstanding === 0) {
      this.#outstanding.delete(nodeId)
    } else {
      this.#outstanding.set(nodeId, outstanding)
    }
    this.#kick()
  }

  /**
   * Settles each batch of a request from its partition's result. A partition the response does
   * not carry - or carries under a topic id the request did not ask for - failed retriably.
   * @param {string} nodeId
   * @param {import('./state/partitionSequencer').PartitionBatch[]} batches
   * @param {import('../protocol/requests/produce/partitionResults').PartitionResult[]} answer
   *   anything else - an `acks: 0` request has no answer - carries no partition
   * @returns {void}
   */
  #answered(nodeId, batches, answer) {
    this.#answeredBy(nodeId)
    if (this.#generation?.closed) {
      for (const batch of batches) {
        const delivery = deliveryOf(batch)
        delivery.dispatched = false
        delivery.retire()
      }
      return
    }
    const manager = this.#eosManager.stateManager
    const results = Array.isArray(answer) ? answer : []
    for (const batch of batches) {
      deliveryOf(batch).dispatched = false
    }

    batches.forEach((batch, index) => {
      const result = resultFor(results, index, batch)
      if (deliveryOf(batch).settled) {
        if (result?.error === null) manager.complete(batch, result)
        else if (result?.error?.code === 46) manager.complete(batch, { duplicate: true })
        if (result?.error) this.#sideChannels(result.error)
        if (!deliveryOf(batch).expired) manager.sequencer.fail(batch)
        deliveryOf(batch).retire()
        return
      }
      if (result === undefined) {
        this.#failed(
          manager,
          batch,
          new KafkaJSError(`No result for ${batch.topic} partition ${batch.partition}`)
        )
      } else if (result.error === null) {
        manager.complete(batch, result)
        resolveBatch(batch, metadataOf(result))
      } else {
        resolveFailingRecords(result.error, batch.messages)
        this.#sideChannels(result.error)
        this.#failed(manager, batch, result.error, result)
      }
    })
  }

  /**
   * Settles each batch of a request that failed as a whole.
   * @param {string} nodeId
   * @param {import('./state/partitionSequencer').PartitionBatch[]} batches
   * @param {Error} error
   * @returns {void}
   */
  #rejected(nodeId, batches, error) {
    this.#answeredBy(nodeId)
    if (this.#generation?.closed) {
      for (const batch of batches) {
        const delivery = deliveryOf(batch)
        delivery.dispatched = false
        delivery.retire()
      }
      return
    }
    const manager = this.#eosManager.stateManager
    for (const batch of batches) {
      deliveryOf(batch).dispatched = false
    }

    if (!(error instanceof ProduceAttemptRefused)) this.#sideChannels(error)
    const refused = []
    for (const batch of batches) {
      const delivery = deliveryOf(batch)
      if (delivery.settled) {
        if (!delivery.expired) manager.sequencer.fail(batch)
        delivery.retire()
        continue
      }
      if (error instanceof ProduceAttemptTooLarge) {
        refused.push(batch)
      } else if (error instanceof ProduceAttemptRefused) {
        manager.sequencer.requeue(batch, 0, { counted: false })
      } else this.#failed(manager, batch, error)
    }
    if (refused.length > 0) this.#repartition(manager, refused, error)
  }

  /**
   * The live batches of a request the size guard refused, unwritten. Several batches become two
   * parts, in order, each retained until a request carrying it is written: a batch with a written
   * attempt in its history goes back with its records and sequences, any other unnumbered at the
   * head of its partition's queue. A batch refused alone fails when it has a written attempt in
   * its history, with no further write and its partition's sequence left unresolved (the earlier
   * attempt may have reached the broker), or when it is one record; one never written with several
   * records is cut in two. So N records make at most 2N - 1 size attempts.
   * @param {import('./state')} manager
   * @param {import('./state/partitionSequencer').PartitionBatch[]} batches
   * @param {ProduceAttemptTooLarge} refusal
   * @returns {void}
   */
  #repartition(manager, batches, refusal) {
    if (batches.length > 1) {
      const parts = [{}, {}]
      const half = Math.floor(batches.length / 2)
      batches.forEach((batch, index) => {
        batch.child = parts[index < half ? 0 : 1]
        if (deliveryOf(batch).everWritten) manager.sequencer.requeue(batch, 0, { counted: false })
        else manager.sequencer.restoreUnsent(batch)
      })
    } else if (!deliveryOf(batches[0]).everWritten && batches[0].messages.length > 1) {
      this.#cut(manager, batches[0])
    } else {
      this.#tooLarge(manager, batches[0], refusal)
    }
  }

  /**
   * Cuts a batch never written in two at its middle record: the batch keeps the second half and
   * its delivery, and a new batch with a delivery of its own, in the same send, takes the first
   * and goes ahead of it in its partition's queues. Each half is a part of its own.
   * @param {import('./state')} manager
   * @param {import('./state/partitionSequencer').PartitionBatch} batch
   * @returns {void}
   */
  #cut(manager, batch) {
    manager.sequencer.restoreUnsent(batch)
    const own = deliveryOf(batch)
    const [first, second] = Accumulator.cut(
      { ...batch, settlers: batch.settlers.slice(0, -1) },
      Math.floor(batch.messages.length / 2)
    )
    const delivery = new Delivery(own.send, own.deadline, this.#detachDeadline)
    delivery.lastError = own.lastError
    delivery.retryTime = own.retryTime
    delivery.batch = { ...batch, ...first, id: this.#nextId++, child: {} }
    delivery.batch.settlers.push(delivery)
    Object.assign(batch, second, { child: {} })
    batch.settlers.push(own)
    own.send.add(delivery)

    const queue = this.#queueOf(batch.topic, batch.partition)
    let index = 0
    for (const queued of queue) {
      if (queued === own) break
      index += 1
    }
    queue.insertAt(index, delivery)
    manager.sequencer.restoreUnsent(delivery.batch)
    this.#attachDeadline(delivery)
  }

  /**
   * Fails a batch no request under `maxRequestSize` can carry. A batch never written gives its
   * sequences back; one with a written attempt leaves its partition's sequence unresolved before it
   * leaves, its range never given back. The error carries no `outcome`. Inside a transaction it
   * arms the abortable state, and the batch's sends keep the error.
   * @param {import('./state')} manager
   * @param {import('./state/partitionSequencer').PartitionBatch} batch
   * @param {ProduceAttemptTooLarge} refusal
   * @returns {void}
   */
  #tooLarge(manager, batch, refusal) {
    if (deliveryOf(batch).everWritten) manager.sequencer.markUnresolved(batch)
    else if (batch.firstSequence !== null) manager.sequencer.rebase(batch)
    manager.sequencer.fail(batch)
    const error = tooLarge(batch, refusal)
    rejectBatch(batch, error)
    this.#inTransaction(manager, error)
  }

  /**
   * Routing work a failure asks for before the next drain: a closed connection's broker is
   * dropped, a KIP-951 leader hint is applied, and stale metadata is refreshed. All best effort.
   * @param {Error} e
   * @returns {void}
   */
  #sideChannels(e) {
    if (this.#generation?.closed) return
    if (e.name === 'KafkaJSConnectionClosedError') {
      this.#cluster.removeBroker({ host: e.host, port: e.port, source: failureSourceOf(e) })
    }

    if (e.currentLeader) {
      this.#hints.push(
        Promise.resolve(
          this.#cluster.applyLeaderHint(
            {
              topic: e.topic,
              partition: e.partition,
              leaderId: e.currentLeader.leaderId,
              leaderEpoch: e.currentLeader.leaderEpoch,
              nodeEndpoints: e.nodeEndpoints,
            },
            ...fenceArgs(this.#generation)
          )
        ).catch((hintError) => {
          this.#logger.debug(`Best-effort leader hint application failed: ${hintError.message}`)
        })
      )
    }

    if (refreshesMetadata(e)) {
      if (this.#sends.size === 0) this.#refreshMetadata().catch(ignore)
      else this.#refresh = true
    }
  }

  /**
   * A batch the broker did not accept: the manager decides, and the loop acts on its verdict. A
   * re-send waits out its backoff in its queue, where a pass expires it once its deadline passes:
   * the deadline bounds every re-send, and no other failure. A batch a fault already settled (see
   * `DrainLoop#fault`) is not re-sent: it leaves the sequencer as an abandoned batch does.
   * @param {import('./state')} manager
   * @param {import('./state/partitionSequencer').PartitionBatch} batch
   * @param {Error} error
   * @param {import('../protocol/requests/produce/partitionResults').PartitionResult} [result]
   * @returns {void}
   */
  #failed(manager, batch, error, result) {
    const delivery = deliveryOf(batch)
    const verdict = manager.fail(batch, error, result)

    switch (verdict) {
      case 'COMPLETE_AS_DUPLICATE':
        manager.complete(batch, { duplicate: true })
        resolveBatch(batch, duplicateMetadataOf(batch, result))
        return
      case 'RETRY_IN_ORDER':
      case 'BUMP_EPOCH':
        if (delivery.settled) {
          this.#abandon(manager, delivery, error)
          return
        }
        delivery.lastError = error
        manager.sequencer.requeue(batch, Date.now() + this.#nextRetryTime(delivery))
        return
      default:
        rejectBatch(
          batch,
          this.#inTransaction(manager, this.#exhausted(batch, error, delivery) ?? error)
        )
    }
  }

  /**
   * The error a batch that failed for good rejects with. Inside a transaction it also arms the
   * state its category calls for, as the Java producer does for every failed batch: the
   * transaction is missing that batch's records. A retriable error that ran out of retries arms
   * - and surfaces as - `KafkaJSTransactionAbortableError`.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
   * @param {import('./state')} manager
   * @param {Error} error a classified error
   * @returns {Error}
   */
  #inTransaction(manager, error) {
    if (!this.#eosManager.isTransactional()) {
      return error
    }

    const surfaced = surfaceInTransaction(error)
    manager.arm(surfaced)
    return surfaced
  }

  /**
   * Settles a batch whose deadline passed with `KafkaJSDeliveryTimeout` for each of its callers,
   * queued or on the wire. Its state consequence waits in `#expired` for `#settleDue`, and its
   * other callers are dropped so that arming a transaction cannot reject them a second time; the
   * delivery stays, and ignores that second call (see `Delivery#reject`). An attempt of the batch
   * that has not written is revoked; one on the wire keeps its request until its terminal callback.
   * @param {Delivery} delivery
   * @param {KafkaJSProducerClosedError} [closedError] this child's forced outcome
   * @param {Error} [surfaced] what the callers reject with instead of `closedError`
   * @returns {void}
   */
  #expire(delivery, closedError, surfaced) {
    if (delivery.settled) return
    delivery.currentAttempt?.revoke()
    const error = closedError || this.#timedOut(delivery.batch, delivery.lastError)
    this.#expired.push({ batch: delivery.batch, error, transmitted: delivery.everWritten })
    delivery.expired = true
    rejectBatch(delivery.batch, surfaced ?? error)
    delivery.batch.settlers = [delivery]
  }

  /**
   * Rejects a queued batch with `error`. It leaves its queue; one already sent leaves its
   * partition's sequence unresolved - the broker may hold it - so nothing new is numbered there
   * until the partition's batches in flight have settled and the gap, if any, is resolved.
   * @param {import('./state')} manager
   * @param {Delivery} delivery
   * @param {Error} error
   * @returns {void}
   */
  #abandon(manager, delivery, error) {
    const { batch } = delivery
    manager.sequencer.markUnresolved(batch)
    manager.sequencer.fail(batch)
    rejectBatch(batch, error)
  }

  /**
   * @param {import('./state/partitionSequencer').PartitionBatch} batch
   * @param {Error} [cause]
   * @returns {KafkaJSDeliveryTimeout}
   */
  #timedOut(batch, cause) {
    return new KafkaJSDeliveryTimeout(
      `Gave up on ${batch.topic} partition ${batch.partition} after ${batch.attempt + 1} attempts: ` +
        `its ${this.#deadlineMs} ms delivery deadline passed`,
      { cause, outcome: deliveryOf(batch).everWritten ? 'unknown' : 'unsent' }
    )
  }

  /**
   * The error a batch that ran out of `retry.retries` on a retriable error rejects with, as the
   * retrier rejects a call.
   * @param {import('./state/partitionSequencer').PartitionBatch} batch
   * @param {Error} error
   * @param {Delivery} delivery
   * @returns {KafkaJSNumberOfRetriesExceeded|undefined}
   */
  #exhausted(batch, error, delivery) {
    if (batch.attempt < this.#retries || !RETRIABLE_CATEGORIES.has(error.category)) {
      return undefined
    }

    return new KafkaJSNumberOfRetriesExceeded(error, {
      retryCount: batch.attempt,
      retryTime: delivery.retryTime,
    })
  }

  /**
   * The next backoff of a batch, on the retrier's schedule.
   * @param {Delivery} delivery
   * @returns {number}
   */
  #nextRetryTime(delivery) {
    delivery.retryTime =
      delivery.retryTime === undefined
        ? this.#backoff.initial()
        : this.#backoff.next(delivery.retryTime)
    return delivery.retryTime
  }

  /**
   * Wakes the loop when `promise` settles, once per promise.
   * @param {Promise<void>} promise
   * @returns {void}
   */
  #watch(promise) {
    if (promise !== this.#watched) {
      this.#watched = promise
      promise.then(this.#kick, this.#kick)
    }
  }

  /**
   * Whether the producer is connected; when it is not, rejects every queued batch with the error
   * `send()` raises for its status.
   * @param {import('./state')} manager
   * @returns {boolean} true when the producer is not connected
   */
  #abandonIfNotConnected(manager) {
    if (this.#generation?.closed) return true
    const statusError = connectionStatusError(this.#getConnectionStatus())
    if (statusError !== null) {
      this.#abandonQueued(manager, statusError)
    }
    return statusError !== null
  }

  /**
   * Refreshes the cluster's metadata - reconnecting first if the cluster lost its connection -
   * without failing the pass on an error. Does nothing once the producer is not connected, so a
   * `disconnect()` is never undone.
   * @returns {Promise<void>}
   */
  async #refreshMetadata() {
    const connected = () =>
      !this.#generation?.closed && connectionStatusError(this.#getConnectionStatus()) === null
    try {
      if (!connected()) {
        return
      }
      if (!this.#cluster.isConnected()) {
        await this.#cluster.connect(...fenceArgs(this.#generation))
      }
      if (connected()) {
        await this.#cluster.refreshMetadata(...fenceArgs(this.#generation))
      }
    } catch (e) {
      this.#logger.debug(`Best-effort metadata refresh failed: ${e.message}`)
    }
  }
}

/**
 * The send path: `partition` assigns a send's records to partitions, and `sendPartitioned` sends
 * those batches. An idempotent producer's batches go through the drain loop above; every other
 * batch goes out in the per-broker requests of a non-idempotent producer, retried by `retrier`,
 * each batch settled and retried on its own partition's answer.
 *
 * @param {Object} options
 * @param {import("../../types").Logger} options.logger
 * @param {import("../../types").Cluster} options.cluster
 * @param {ReturnType<import("../../types").ICustomPartitioner>} options.partitioner
 * @param {import("./eosManager").EosManager} options.eosManager
 * @param {import("../retry").Retrier} options.retrier
 * @param {import("./idempotence")} [options.idempotence] read at every call; absent, every send
 *   is non-idempotent
 * @param {import("../../types").RetryOptions} [options.retry={}] the producer's merged retry
 *   options, for the idempotent path's per-batch budget and backoff
 * @param {number} [options.deliveryTimeout] ms from `send()` to each batch's last answer
 * @param {{ gzip?: number, zstd?: number }} [options.compressionLevels] validated producer
 *   snapshot, selected by codec on both send paths and retained for retries
 * @param {() => string} [options.getConnectionStatus] the producer's connection status
 * @param {RequestBudget} [options.budget] the producer's request-size plan: both send paths keep
 *   each request within it, and refuse a frame above its `maxRequestSize` before writing it
 * @param {() => import('./generation')|undefined} [options.getGeneration] current lifecycle
 * @returns {{ sendPartitioned: Function, partition: Function, forceClose: Function, inTransaction: Function }} the
 *   `inTransaction` of a send that settled before it reached the send path arms the transaction
 */
const createSendMessages = ({
  logger,
  cluster,
  partitioner,
  eosManager,
  retrier,
  idempotence,
  retry = {},
  deliveryTimeout,
  compressionLevels,
  getConnectionStatus,
  budget = new RequestBudget(),
  getGeneration = () => undefined,
}) => {
  const isIdempotent = () => idempotence !== undefined && idempotence.isEnabled()
  let loop = null
  let loopGeneration
  const plainForces = new Set()

  /**
   * The error a transactional send whose records could not be assigned to partitions rejects
   * with: a client error arms the state its category calls for, as a failed batch does. Any other
   * error - a custom partitioner's own, say - leaves the transaction as it was, as the Java
   * producer's `doSend` arms only for an `ApiException`.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java
   * @param {Error} error
   * @returns {Error}
   */
  const inTransaction = (error) => {
    if (!eosManager.isTransactional() || !(error instanceof KafkaJSError)) {
      return error
    }

    const { stateManager } = eosManager
    stateManager.classify(error, 'produce')
    const surfaced = surfaceInTransaction(error)
    stateManager.arm(surfaced)
    return surfaced
  }

  /**
   * Gets the cluster ready for the next attempt after a failed produce, as the non-idempotent send
   * always has: a closed connection's broker is dropped, a leader hint is applied, a disconnected
   * cluster reconnects and refreshes its metadata, and stale metadata is refreshed.
   * @param {Error} e the attempt's error
   * @param {(error: Error) => void} bail the retrier's bail
   * @param {number} retryCount
   * @param {number} retryTime
   * @param {import('./generation')} [generation] the producer lifecycle the call began under:
   *   once it closes nothing is dropped, reconnected or refreshed
   * @returns {Promise<never>} rethrows `e` for the retrier, or bails with it when it is not
   *   retriable
   * @throws {Error} the lifecycle's close error
   */
  const recover = async (e, bail, retryCount, retryTime, generation) => {
    generation?.check()
    if (e.name === 'KafkaJSConnectionClosedError') {
      cluster.removeBroker({ host: e.host, port: e.port, source: failureSourceOf(e) })
    }

    // KIP-951: the broker told us which node now leads this partition. Applying it updates
    // the leader-epoch cache and the broker table, so the next attempt - scheduled by the
    // retrier below, on its normal backoff - routes there. Nothing about the retry unit or
    // the backoff changes. Best-effort, same reasoning as the other side channels below: a
    // rejection here (e.g. a bad address in `nodeEndpoints`) must not replace the original
    // produce error's `type`/`code`/`retriable` and skip the `staleMetadata` fall-through
    // below.
    if (e.name === 'KafkaJSProtocolError' && e.currentLeader) {
      await cluster
        .applyLeaderHint(
          {
            topic: e.topic,
            partition: e.partition,
            leaderId: e.currentLeader.leaderId,
            leaderEpoch: e.currentLeader.leaderEpoch,
            nodeEndpoints: e.nodeEndpoints,
          },
          ...fenceArgs(generation)
        )
        .catch((hintError) => {
          logger.debug(
            `Best-effort leader hint application failed, continuing with the original error: ${hintError.message}`
          )
        })
      generation?.check()
    }

    if (!cluster.isConnected()) {
      logger.debug(`Cluster has disconnected, reconnecting: ${e.message}`, {
        retryCount,
        retryTime,
      })
      // Best-effort reconnect/refresh: this is here to put the cluster back in a usable
      // state before the retrier's next attempt, not to replace `e` - a failure of either
      // step (e.g. still unreachable) must not turn `e` into a different error class and
      // strip its `retryCount`/`retryTime` on its way to the `throw` below. `Promise.resolve`
      // tolerates a test double that returns `undefined` instead of a promise.
      await Promise.resolve(cluster.connect(...fenceArgs(generation))).catch((connectError) => {
        logger.debug(
          `Best-effort reconnect failed, continuing with the original error: ${connectError.message}`
        )
      })
      generation?.check()
      await Promise.resolve(cluster.refreshMetadata(...fenceArgs(generation))).catch(
        (refreshError) => {
          logger.debug(
            `Best-effort metadata refresh failed, continuing with the original error: ${refreshError.message}`
          )
        }
      )
      generation?.check()
      throw e
    }

    // This is necessary in case the metadata is stale and the number of partitions
    // for this topic has increased in the meantime
    if (refreshesMetadata(e)) {
      logger.error(`Failed to send messages: ${e.message}`, { retryCount, retryTime })
      // Best-effort, same reasoning as above: a stale-metadata refresh that itself fails
      // must not replace the retriable send error (e.g. NOT_LEADER_FOR_PARTITION) with a
      // non-retriable one and short-circuit the retry a leader election would otherwise let
      // through.
      await Promise.resolve(cluster.refreshMetadata(...fenceArgs(generation))).catch(
        (refreshError) => {
          logger.debug(
            `Best-effort metadata refresh failed, continuing with the original error: ${refreshError.message}`
          )
        }
      )
      generation?.check()
      throw e
    }

    logger.error(`${e.message}`, { retryCount, retryTime })
    if (e.retriable) throw e
    bail(e)
  }

  /**
   * The produce arguments of one non-idempotent request to `broker`, past its topic data.
   * Topic ids are decided per broker request, not per send(): a global rule would downgrade one
   * broker's request because a *different* broker's topic had an unknown id.
   * @param {string[]} topics the request's topics
   * @returns {object}
   */
  const plainRequestOf = (topics) => {
    const isTransactional = eosManager.isTransactional()
    let topicIds
    let maxVersion = PRODUCE_TOPIC_NAMES_MAX_VERSION

    if (!isTransactional) {
      topicIds = topicIdsOf(cluster, topics)
      maxVersion = topicIds === undefined ? PRODUCE_TOPIC_NAMES_MAX_VERSION : undefined
    }

    return {
      transactionalId: isTransactional ? eosManager.getTransactionalId() : undefined,
      producerId: eosManager.getProducerId(),
      producerEpoch: eosManager.getProducerEpoch(),
      topicIds,
      maxVersion,
    }
  }

  /**
   * The retrier for a send's wait for its partitions. A finite remaining budget within one timer
   * interval becomes `maxTotalTime`; above it, `null` leaves the absolute deadline to the holder's
   * stepped expiry timer. A capped segment must not end the send early. With no deadline, the
   * producer's own retrier is used.
   * @param {{ deadline?: import('./deadline').Deadline }} send
   * @param {import('./generation')} [fence] the producer lifecycle the wait began under: its close
   *   ends the retries at once, with its close error
   * @returns {import('../retry').Retrier}
   * @throws {KafkaJSDeliveryTimeout} when no time is left: a retrier is never built for zero
   */
  const retrierOf = ({ deadline }, fence) => {
    if (deadline === undefined || deadline.dueAt === Infinity) {
      return fence ? createRetry({ ...retry, fence }) : retrier
    }

    const remaining = deadline.remaining()
    if (remaining === 0) {
      throw new KafkaJSDeliveryTimeout(
        `Gave up before the send reached a broker: its ${deadline.budgetMs} ms delivery deadline passed`,
        { outcome: 'unsent' }
      )
    }

    return createRetry({
      ...retry,
      maxTotalTime: remaining > MAX_DELAY ? null : remaining,
      ...(fence ? { fence } : {}),
    })
  }

  /**
   * The records of a send that is still waiting.
   * @param {{ records: ?object[] }} send
   * @returns {{ topic: string, messages: object[] }[]}
   * @throws {Error} once the send has settled and dropped its records: whatever waits for it
   *   stops, and nothing is enqueued
   */
  const recordsOf = (send) => {
    if (send.records === null) {
      throw Object.assign(new Error('The send settled before its records were assigned'), {
        retriable: false,
      })
    }
    return send.records
  }

  /**
   * Assigns each record to a partition once, after making sure the topics' metadata is loaded:
   * the partitioner never runs again for these records. Retried while the metadata cannot be
   * loaded, within the send's delivery budget. The records are read from `send` after each await:
   * a send that settled meanwhile has none, and the wait ends without enqueuing anything. A
   * failure that arrives after the send settled is ignored: it refreshes no metadata, is not
   * retried and arms no transaction. Otherwise a transactional producer's failure arms the
   * transaction as `inTransaction` says.
   * @param {{ records: ?object[], deadline?: import('./deadline').Deadline }|object[]} source a
   *   send waiting to be partitioned (see the accumulator's `PendingSend`), or a plain list of
   *   `{ topic, messages }` for a caller with no deadline
   * @param {import('./generation')} [fence] the producer lifecycle the caller captured before its
   *   first await; the current one when absent. Once it closes the retries stop and no metadata
   *   is asked for. A send that is pinned stays pinned until the retry callback itself returns,
   *   whether or not its caller was cancelled.
   * @returns {Promise<{ topic: string, partition: number, messages: object[] }[]>}
   */
  const partition = (source, fence = getGeneration()) => {
    const send = Array.isArray(source) ? { records: source } : source
    const attempt = async (bail) => {
      try {
        fence?.check()
        await cluster.addMultipleTargetTopics(
          recordsOf(send).map(({ topic }) => topic),
          ...fenceArgs(fence)
        )
        fence?.check()
        recordsOf(send)
        await cluster.refreshMetadataIfNecessary(...fenceArgs(fence))
        fence?.check()

        const entries = []
        for (const { topic, messages } of recordsOf(send)) {
          const partitionMetadata = cluster.findTopicPartitionMetadata(topic)
          if (partitionMetadata.length === 0) {
            throw new KafkaJSMetadataNotLoaded('Producing to topic without metadata')
          }

          const grouped = groupMessagesPerPartition({
            topic,
            partitionMetadata,
            messages,
            partitioner,
          })
          for (const id of keys(grouped)) {
            entries.push({ topic, partition: Number(id), messages: grouped[id] })
          }
        }
        return entries
      } catch (e) {
        if (send.settled) return bail(e)
        fence?.check()
        if (refreshesMetadata(e)) {
          await Promise.resolve(cluster.refreshMetadata(...fenceArgs(fence))).catch(
            (refreshError) => {
              logger.debug(`Best-effort metadata refresh failed: ${refreshError.message}`)
            }
          )
        }
        if (send.settled) return bail(e)
        fence?.check()
        if (e.retriable) throw e
        bail(e)
      }
    }
    const pinned =
      fence && send.pin
        ? (bail) => {
            send.pin()
            return attempt(bail).finally(() => send.unpin())
          }
        : attempt
    return retrierOf(
      send,
      fence
    )(pinned).catch((error) => {
      throw send.settled || fence?.closed ? error : inTransaction(error)
    })
  }

  /**
   * Sends records already assigned to partitions, one batch per entry, and settles each entry's
   * `settlers` as its batch settles. Idempotent: through the drain loop. Otherwise: one
   * non-idempotent call over all entries, each settled and retried on its own partition's answer.
   * @param {object} request
   * @param {number} request.acks
   * @param {number} request.timeout
   * @param {number} request.compression
   * @param {{ topic: string, partition: number, messages: object[], settlers?: object[],
   *   createdAt?: number }[]} request.batches `createdAt` starts the delivery deadline; now when
   *   absent
   * @returns {Promise<object[]>} the `RecordMetadata` of every batch
   * @throws {KafkaJSInvariantViolation} an idempotent send whose `acks` is not -1
   */
  const sendPartitioned = async ({ acks, timeout, compression, batches }) =>
    isIdempotent()
      ? sendIdempotent({ acks, timeout, compression, batches })
      : sendPartitionedPlain({ acks, timeout, compression, batches })

  /**
   * The idempotent half of `sendPartitioned`: the batches go through the drain loop, created on
   * the first send.
   * @param {object} request see `sendPartitioned`
   * @returns {Promise<object[]>}
   * @throws {KafkaJSInvariantViolation} an `acks` other than -1
   */
  const sendIdempotent = async ({ acks, timeout, compression, batches }) => {
    if (batches.length === 0) {
      return []
    }

    if (acks !== -1) {
      batches.forEach((batch) => batch.retire?.())
      throw new KafkaJSInvariantViolation(`An idempotent send must use acks -1, not ${acks}`)
    }

    const generation = getGeneration()
    if (loop === null || loopGeneration !== generation) {
      loopGeneration = generation
      loop = new DrainLoop({
        logger,
        cluster,
        eosManager,
        retry,
        deliveryTimeout,
        compressionLevels,
        getConnectionStatus,
        budget,
        generation,
      })
    }
    return loop.send({ acks, timeout, compression, batches })
  }

  /** ms from a send's start to its delivery deadline, the producer's */
  const deadlineMs = deliveryTimeout ?? retry.maxTotalTime

  /**
   * The leaders of a topic's partitions, once its metadata is loaded; see `leadersOfPartitions`.
   * @param {string} topic
   * @param {string[]} partitions
   * @returns {ReturnType<typeof leadersOfPartitions>}
   * @throws {KafkaJSMetadataNotLoaded} while the topic has no metadata
   */
  const leadersOf = (topic, partitions) => {
    if (cluster.findTopicPartitionMetadata(topic).length === 0) {
      logger.debug('Producing to topic without metadata', {
        topic,
        targetTopics: Array.from(cluster.targetTopics),
      })
      throw new KafkaJSMetadataNotLoaded('Producing to topic without metadata')
    }
    return leadersOfPartitions(cluster, topic, partitions)
  }

  /**
   * `sendPartitioned` without idempotence: the batches still unsettled go out in one request per
   * leader, retried by `retrier`, and each settles on its own partition's answer, as the Java
   * producer completes each partition's batch. A batch answered OK resolves at once and is never
   * sent again; one answered with an error that is not retriable rejects alone; one answered with a
   * retriable error, or whose whole request failed with one, goes out again on the next attempt,
   * within the call's retry budget. A request that fails as a whole, or a topic or broker that
   * cannot be looked up, leaves its own batches for the next attempt when the error is retriable
   * and rejects them otherwise; the other batches go out. Each attempt waits for every broker's
   * answer, so the slowest broker paces the retries. A batch whose delivery deadline, counted from
   * its own first send, has already passed rejects without being sent and the others go out; the
   * budget ends at the earliest deadline among those sent. Once the budget runs out, or the call
   * fails before any request, the batches still unsettled reject with the retrier's error.
   *
   * A partition's batches go out in order, one per request: the first one not settled, so a batch
   * that waits for a retry stays ahead of the later ones and one that failed for good lets the next
   * go. A broker's batches go out in as many requests as the plan of `maxRequestSize` needs, each
   * within it; a request refused at its write as too large is divided as the idempotent path
   * divides one (see `DrainLoop#repartition`), and the attempt costs no retry.
   *
   * Every await of the call (the metadata, the broker lookups, the requests, the recovery and the
   * side channels of a late error) can hold the batches it routes. So a batch that has settled
   * while no request carries it, or whose request has ended since, detaches at once (see the
   * accumulator's `detach`): it drops its records and gives back their shares, and whatever still
   * holds the batch holds no record. Its sends' shells and its debit stay charged until the call's
   * work has ended, when every batch of the call retires.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java
   * @param {object} request see `sendPartitioned`
   * @returns {Promise<object[]>} the `RecordMetadata` of every batch, once all have settled;
   *   rejects with the first error a batch failed with
   */
  const sendPartitionedPlain = ({ acks, timeout, compression, batches }) => {
    const generation = getGeneration()
    let callbacks = 0
    let workEnded = false
    const unsettled = new Set(batches)
    /** Every batch of the call in partition order: a cut batch's first half goes before it. */
    const order = [...batches]
    const metadata = []
    const deliveries = new Map()
    let failure = null
    let resolveDone, rejectDone
    const done = new Promise((resolve, reject) => {
      resolveDone = resolve
      rejectDone = reject
    })
    /** Settles the public promise once no delivery remains. @returns {void} */
    const finish = () => {
      if (unsettled.size !== 0) return
      timer.clear()
      if (failure !== null) rejectDone(failure)
      else resolveDone(metadata)
    }
    /** Retires every batch of the call, once nothing of the call holds them. @returns {void} */
    const retireAll = () => {
      if (!workEnded || callbacks !== 0) return
      for (const batch of deliveries.keys()) batch.retire?.()
    }
    /**
     * Settles one live batch exactly once and revokes its current unwritten attempt.
     * @param {object} batch
     * @param {object|undefined} value acknowledgement metadata
     * @param {Error} [error] a terminal failure
     * @returns {void}
     */
    const settle = (batch, value, error) => {
      if (!unsettled.delete(batch)) return
      const delivery = deliveries.get(batch)
      delivery.settled = true
      delivery.currentAttempt?.revoke()
      if (!delivery.carried) batch.detach?.()
      if (error !== undefined) failure = failure || error
      else if (value !== undefined) metadata.push(value)
      batch.settlers?.forEach((settler) =>
        error === undefined ? settler.resolve(value) : settler.reject(error)
      )
      finish()
    }
    /** @param {object} delivery the due batch's state @returns {void} */
    const expire = (delivery) => {
      if (delivery.settled) return
      delivery.currentAttempt?.revoke()
      delivery.expired = true
      settle(
        delivery.batch,
        undefined,
        new KafkaJSDeliveryTimeout(
          delivery.everWritten
            ? `Gave up on ${delivery.batch.topic} partition ${delivery.batch.partition}: its delivery deadline passed`
            : `Gave up on ${delivery.batch.topic} partition ${delivery.batch.partition} before sending it: its ${deadlineMs} ms delivery deadline passed`,
          { cause: delivery.lastError, outcome: delivery.everWritten ? 'unknown' : 'unsent' }
        )
      )
    }
    const timer = new ExpiryTimer((now) => {
      let next = Infinity
      for (const batch of [...unsettled]) {
        const delivery = deliveries.get(batch)
        if (now >= delivery.deadline) expire(delivery)
        else next = Math.min(next, delivery.deadline)
      }
      return next
    })
    const now = Date.now()
    for (const batch of batches) {
      const delivery = {
        batch,
        deadline: new Deadline(batch.createdAt ?? now, deadlineMs).dueAt,
        settled: false,
        everWritten: false,
        expired: false,
        currentAttempt: null,
        carried: false,
        lastError: undefined,
      }
      deliveries.set(batch, delivery)
    }
    for (const delivery of deliveries.values()) {
      if (now >= delivery.deadline) expire(delivery)
      else timer.arm(delivery.deadline)
    }

    /**
     * Settles every live leaf, including children cut after hand-off.
     * @param {Error} [surfaced] what they reject with instead of the close error
     * @returns {void}
     */
    const force = (surfaced) => {
      for (const batch of order) {
        const delivery = deliveries.get(batch)
        settle(
          batch,
          undefined,
          surfaced ??
            new KafkaJSProducerClosedError('The producer was closed', {
              outcome: delivery.everWritten ? 'unknown' : 'unsent',
            })
        )
      }
    }
    plainForces.add(force)
    const forgetForce = () => plainForces.delete(force)
    done.then(forgetForce, forgetForce)

    /** Routing side effects also apply to late errors. @param {Error} error @returns {Promise<void>} */
    const sideChannels = async (error) => {
      if (generation?.closed) return
      if (error.name === 'KafkaJSConnectionClosedError')
        cluster.removeBroker({ host: error.host, port: error.port, source: failureSourceOf(error) })
      if (error.currentLeader) {
        await Promise.resolve(
          cluster.applyLeaderHint(
            {
              topic: error.topic,
              partition: error.partition,
              leaderId: error.currentLeader.leaderId,
              leaderEpoch: error.currentLeader.leaderEpoch,
              nodeEndpoints: error.nodeEndpoints,
            },
            ...fenceArgs(generation)
          )
        ).catch(() => {})
      }
      if (!generation?.closed && refreshesMetadata(error))
        await Promise.resolve(cluster.refreshMetadata(...fenceArgs(generation))).catch(() => {})
    }

    /**
     * The Produce request of `sent`, built in a frame of its own, so that once the request has
     * ended nothing of this call holds its topic data, which lists the batches' records.
     * @param {object[]} sent
     * @param {ProduceAttempt} attempt
     * @returns {object}
     */
    const requestOf = (sent, attempt) => {
      const topicData = topicDataOf(sent)
      return {
        ...plainRequestOf(topicData.map(({ topic }) => topic)),
        acks,
        timeout,
        compression,
        compressionLevel:
          compression === Compression.GZIP
            ? compressionLevels?.gzip
            : compression === Compression.ZSTD
              ? compressionLevels?.zstd
              : undefined,
        topicData,
        perPartitionResults: true,
        guard: attempt.guard,
      }
    }

    /**
     * The first batch of each of a topic's partitions that is not settled yet, in partition order.
     * @param {string} topic
     * @returns {object[]}
     */
    const headsOf = (topic) => {
      const heads = []
      const partitions = new Set()
      for (const batch of order) {
        const partition = Number(batch.partition)
        if (batch.topic === topic && unsettled.has(batch) && !partitions.has(partition)) {
          partitions.add(partition)
          heads.push(batch)
        }
      }
      return heads
    }

    /**
     * The requests one broker's batches go out in, in order: a part of a refused request alone,
     * every other batch together, each request within the plan of `maxRequestSize` and holding at
     * least one batch.
     * @param {object[]} owned
     * @returns {object[][]}
     */
    const requestsOf = (owned) => {
      const parts = new Map()
      for (const batch of owned) {
        const part = batch.child ?? null
        if (parts.has(part)) parts.get(part).push(batch)
        else parts.set(part, [batch])
      }
      const requests = []
      for (const members of parts.values()) {
        let request = []
        let planned = 0
        let topics = new Set()
        for (const batch of members) {
          const bytesOf = () =>
            budget.batchBytes(batch) +
            (topics.has(batch.topic) ? 0 : budget.topicBytes(batch.topic))
          if (request.length > 0 && planned + bytesOf() > budget.maxRequestBytes) {
            requests.push(request)
            request = []
            planned = 0
            topics = new Set()
          }
          planned += bytesOf()
          topics.add(batch.topic)
          request.push(batch)
        }
        requests.push(request)
      }
      return requests
    }

    /**
     * Cuts a batch never written in two at its middle record (see `Accumulator.cut`): the batch
     * keeps the second half, and the first goes before it with a delivery of its own.
     * @param {object} batch
     * @returns {void}
     */
    const cut = (batch) => {
      const [first, second] = Accumulator.cut(batch, Math.floor(batch.messages.length / 2))
      const head = { ...batch, ...first, child: {} }
      Object.assign(batch, second, { child: {} })
      order.splice(order.indexOf(batch), 0, head)
      unsettled.add(head)
      deliveries.set(head, {
        ...deliveries.get(batch),
        batch: head,
        currentAttempt: null,
        carried: false,
      })
    }

    /**
     * The live batches of a request refused at its write as too large, divided as
     * `DrainLoop#repartition` divides them: several become two parts; one refused alone fails when
     * it has a written attempt or is one record, and is cut otherwise.
     * @param {object[]} sent
     * @param {ProduceAttemptTooLarge} refusal
     * @returns {void}
     */
    const repartition = (sent, refusal) => {
      const live = sent.filter((candidate) => unsettled.has(candidate))
      if (live.length > 1) {
        const parts = [{}, {}]
        const half = Math.floor(live.length / 2)
        live.forEach((batch, index) => {
          batch.child = parts[index < half ? 0 : 1]
        })
      } else if (
        live.length === 1 &&
        !deliveries.get(live[0]).everWritten &&
        live[0].messages.length > 1
      ) {
        cut(live[0])
      } else if (live.length === 1) {
        settle(live[0], undefined, tooLarge(live[0], refusal))
      }
    }

    /**
     * Builds one request with fixed membership after filtering all prior awaits.
     * @param {object} broker the broker already looked up
     * @param {object[]} candidates batches proposed for this broker
     * @returns {Promise<Error|null>} a genuine retriable failure, a private refusal, or no failure
     */
    const produceTo = async (broker, candidates) => {
      const sent = candidates.filter((batch) => unsettled.has(batch))
      if (sent.length === 0) return null
      const carried = sent.map((batch) => deliveries.get(batch))
      const attempt = new ProduceAttempt(
        carried,
        getConnectionStatus || (() => CONNECTION_STATUS.CONNECTED),
        expire,
        budget.maxRequestSize,
        generation
      )
      /** The request ended: a batch that settled meanwhile detaches now. @returns {void} */
      const ended = () => {
        for (const delivery of carried) {
          delivery.carried = false
          if (delivery.settled) delivery.batch.detach?.()
        }
      }
      carried.forEach((delivery) => {
        delivery.carried = true
      })
      let results
      try {
        results = await broker.produce(requestOf(sent, attempt))
      } catch (error) {
        const refused = error instanceof ProduceAttemptRefused
        if (!refused) attachFailingRecords(error, topicDataOf(sent))
        ended()
        if (generation?.closed) return null
        if (error instanceof ProduceAttemptTooLarge) repartition(sent, error)
        if (refused) return error
        for (const delivery of carried) delivery.lastError = error
        if (sent.every((batch) => !unsettled.has(batch))) {
          await sideChannels(error)
          return null
        }
        if (error.retriable) return error
        sent.forEach((batch) => settle(batch, undefined, error))
        return null
      }
      ended()
      if (generation?.closed) return null
      if (acks === 0) {
        sent.forEach((batch) => settle(batch, undefined))
        return null
      }
      let retriable = null
      for (let index = 0; index < sent.length; index += 1) {
        const batch = sent[index]
        const result = resultFor(results, index, batch)
        const error =
          result === undefined
            ? new KafkaJSError(`No result for ${batch.topic} partition ${batch.partition}`)
            : result.error
        if (!unsettled.has(batch)) {
          if (error) await sideChannels(error)
          continue
        }
        if (error === null) settle(batch, metadataOf(result))
        else {
          deliveries.get(batch).lastError = error
          resolveFailingRecords(error, batch.messages)
          if (error.retriable) retriable = retriable || error
          else settle(batch, undefined, error)
        }
      }
      return retriable
    }

    /**
     * Runs delivery retries; every continuation rechecks membership after its await.
     * Work may outlive the public settlement until the uncancellable request ends.
     * @returns {Promise<void>}
     */
    const work = async () => {
      if (unsettled.size === 0) return
      let latest = -Infinity
      for (const batch of unsettled) latest = Math.max(latest, deliveries.get(batch).deadline)
      const remaining = latest - Date.now()
      if (remaining <= 0) {
        for (const batch of [...unsettled]) expire(deliveries.get(batch))
        return
      }
      // Past one timer interval no retrier can hold the budget: the owner timer and the write
      // guard enforce the absolute deadline, and a capped budget would end the delivery early.
      const handOffRetrier =
        remaining === Infinity
          ? generation
            ? createRetry({ ...retry, fence: generation })
            : retrier
          : createRetry({
              ...retry,
              maxTotalTime: remaining > MAX_DELAY ? null : Math.max(1, remaining),
              ...(generation ? { fence: generation } : {}),
            })
      try {
        const run = async (bail, retryCount, retryTime) => {
          generation?.check()
          while (unsettled.size > 0) {
            const topics = [...new Set([...unsettled].map(({ topic }) => topic))]
            generation?.check()
            await cluster.addMultipleTargetTopics(topics, ...fenceArgs(generation))
            generation?.check()
            if (unsettled.size === 0) return
            try {
              await cluster.refreshMetadataIfNecessary(...fenceArgs(generation))
              generation?.check()
              if (unsettled.size === 0) return
              const left = []
              const perBroker = new Map()
              const leave = (lost, error) => {
                const live = lost.filter((batch) => unsettled.has(batch))
                if (live.length === 0) return
                live.forEach((batch) => {
                  deliveries.get(batch).lastError = error
                })
                if (error.retriable) left.push(error)
                else live.forEach((batch) => settle(batch, undefined, error))
              }
              for (const topic of topics) {
                const own = headsOf(topic)
                if (own.length === 0) continue
                let lookup
                try {
                  lookup = leadersOf(
                    topic,
                    own.map(({ partition }) => partition)
                  )
                } catch (error) {
                  leave(own, error)
                  continue
                }
                for (const { partition, error } of lookup.failed)
                  leave(
                    own.filter((batch) => Number(batch.partition) === Number(partition)),
                    error
                  )
                for (const nodeId of keys(lookup.leaders)) {
                  const candidates = own.filter((batch) =>
                    lookup.leaders[nodeId].some(
                      (partition) => Number(partition) === Number(batch.partition)
                    )
                  )
                  let broker
                  try {
                    broker = await cluster.findBroker({
                      nodeId,
                      ...(generation ? { fence: generation } : {}),
                    })
                    generation?.check()
                  } catch (error) {
                    leave(candidates, error)
                    continue
                  }
                  const live = candidates.filter((batch) => unsettled.has(batch))
                  if (live.length > 0)
                    perBroker.set(broker, [...(perBroker.get(broker) || []), ...live])
                }
              }
              const waiting = unsettled.size
              const errors = await Promise.all(
                [...perBroker].flatMap(([broker, owned]) =>
                  requestsOf(owned).map((request) => produceTo(broker, request))
                )
              )
              const genuine = [...left, ...errors].find(
                (error) => error !== null && !(error instanceof ProduceAttemptRefused)
              )
              if (genuine !== undefined) throw genuine
              if (errors.some((error) => error instanceof ProduceAttemptRefused)) continue
              if (unsettled.size < waiting) continue
              if (unsettled.size > 0) throw new KafkaJSMetadataNotLoaded(NO_PARTITION_METADATA)
              return
            } catch (error) {
              if (unsettled.size === 0) {
                await sideChannels(error)
                return
              }
              generation?.check()
              await recover(error, bail, retryCount, retryTime, generation)
              generation?.check()
            }
          }
        }
        await handOffRetrier((...args) => {
          callbacks += 1
          return run(...args).finally(() => {
            callbacks -= 1
            retireAll()
          })
        })
      } catch (error) {
        if (generation?.closed) {
          force()
          return
        }
        for (const batch of [...unsettled]) {
          const delivery = deliveries.get(batch)
          settle(
            batch,
            undefined,
            error instanceof KafkaJSDeliveryTimeout
              ? new KafkaJSDeliveryTimeout(error.message, {
                  cause: error.cause,
                  outcome: delivery.everWritten ? 'unknown' : 'unsent',
                })
              : error
          )
        }
      } finally {
        timer.clear()
      }
    }
    if (unsettled.size > 0)
      work()
        .catch((error) => {
          for (const batch of [...unsettled]) settle(batch, undefined, error)
        })
        .finally(() => {
          workEnded = true
          retireAll()
        })
    else {
      finish()
      workEnded = true
      retireAll()
    }
    return done
  }

  /**
   * Forces current deliveries; a later lifecycle gets its own loop.
   * @param {Error} [surfaced] what the callers reject with instead of the close error
   * @returns {void}
   */
  const forceClose = (surfaced) => {
    loop?.forceClose(surfaced)
    for (const force of [...plainForces]) force(surfaced)
  }

  return { sendPartitioned, partition, inTransaction, forceClose }
}

createSendMessages.connectionStatusError = connectionStatusError

module.exports = createSendMessages
