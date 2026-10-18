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
} = require('../errors')
const { staleMetadata } = require('../protocol/error')
const { RETRIABLE_CATEGORIES } = require('../protocol/errorCategories')
const { CONNECTION_STATUS } = require('../network/connectionStatus')
const { isConnectionFailure } = require('../network/connectionFailure')
const { isArmed } = require('./eosManager/transactionStates')
const { surfaceInTransaction } = require('./eosManager')
const groupMessagesPerPartition = require('./groupMessagesPerPartition')
const Fifo = require('../utils/fifo')

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
    entry.partitions.push({ partition, firstSequence, messages })
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
 * settled or on the wire, its deadline, its last error and its last backoff.
 */
class Delivery {
  /**
   * @param {Send} send
   * @param {number} deadline when the batch's delivery timeout runs out; Infinity without one
   */
  constructor(send, deadline) {
    this.send = send
    this.deadline = deadline
    /** @type {import('./state/partitionSequencer').PartitionBatch} */
    this.batch = null
    this.settled = false
    this.onWire = false
    /** @type {Error|undefined} */
    this.lastError = undefined
    /** @type {number|undefined} the last backoff, in ms */
    this.retryTime = undefined
    this.routingFailures = 0
  }

  /**
   * Settles the batch for its send. Called once: `resolveBatch`/`rejectBatch` settle a batch
   * only while it is unsettled, and `abortQueued` only reaches batches still queued.
   * @param {object} metadata
   * @returns {void}
   */
  resolve(metadata) {
    this.settled = true
    this.send.delivered(metadata)
  }

  /**
   * Settles the batch for its send, once (see `resolve`).
   * @param {Error} error
   * @returns {void}
   */
  reject(error) {
    this.settled = true
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
  #getConnectionStatus
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

  /**
   * @param {object} options
   * @param {import("../../types").Logger} options.logger
   * @param {import("../../types").Cluster} options.cluster
   * @param {object} options.eosManager the facade whose `stateManager` the loop drains
   * @param {import("../../types").RetryOptions} options.retry the producer's merged retry options
   * @param {number} [options.deliveryTimeout] ms from `send()` to a batch's last answer
   * @param {() => string} options.getConnectionStatus
   */
  constructor({ logger, cluster, eosManager, retry, deliveryTimeout, getConnectionStatus }) {
    this.#logger = logger
    this.#cluster = cluster
    this.#eosManager = eosManager
    this.#retries = retry.retries ?? createRetry.defaults.retries
    this.#backoff = createRetry.backoff(retry)
    this.#deadlineMs = deliveryTimeout ?? retry.maxTotalTime
    this.#getConnectionStatus = getConnectionStatus
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
    const queued = batches.map(({ topic, partition, messages, settlers = [], createdAt = now }) => {
      const queue = this.#queueOf(topic, Number(partition))
      const delivery = new Delivery(send, createdAt + (this.#deadlineMs ?? Infinity))
      delivery.batch = {
        id: this.#nextId++,
        topic,
        partition: Number(partition),
        messages,
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
        sequenceReset: false,
      }
      send.deliveries.push(delivery)
      queue.push(delivery)
      return delivery.batch
    })

    if (this.#eosManager.isTransactional()) {
      this.#register(manager, queued)
    }

    this.#sends.add(send)
    manager.enqueue(queued)
    this.#kick()
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
        if (delivery.onWire) {
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
   * Waits for a kick, or for the earliest backoff or deadline of a queued batch.
   * @returns {Promise<void>}
   */
  #sleep() {
    if (this.#wakeAt !== Infinity) {
      this.#timer = setTimeout(this.#kick, Math.max(0, this.#wakeAt - Date.now()))
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
    /** @type {Map<string, Map<number, Delivery>>} the first queued batch of each ready partition */
    const ready = new Map()
    for (const [topic, partitions] of this.#queues) {
      for (const [partition, queue] of partitions) {
        const head = this.#headOf(manager, queue, now)
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
        const drained = manager.drain(broker, leaders)
        if (drained.length === 0) {
          break
        }
        this.#dispatch(nodeId, drained)
      }
    }
  }

  /**
   * A partition's first queued batch: the batches it has settled are dropped from the front of its
   * queue, and those whose deadline passed are expired on the way. The scan stops at the first
   * batch still queued in time, as Java's own expiry does, so no batch expires before the one
   * queued ahead of it in its partition.
   * @param {import('./state')} manager
   * @param {Fifo<Delivery>} queue
   * @param {number} now
   * @returns {Delivery|undefined} undefined when every batch left is on the wire
   */
  #headOf(manager, queue, now) {
    while (queue.size > 0 && queue.peek().settled) {
      queue.shift()
    }

    for (const delivery of queue) {
      if (delivery.settled || delivery.onWire) {
        continue
      }
      if (now < delivery.deadline) {
        return delivery
      }
      this.#expire(manager, delivery)
    }
    return undefined
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
          if (!delivery.settled && !delivery.onWire) {
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
        if (!delivery.settled && !delivery.onWire) {
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
    for (const batch of batches) {
      deliveryOf(batch).onWire = true
    }
    this.#outstanding.set(nodeId, (this.#outstanding.get(nodeId) ?? 0) + 1)

    const handed = (this.#handoffs.get(nodeId) ?? RESOLVED).then(() =>
      this.#handOff(nodeId, batches)
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
   * @returns {Promise<{ answer: Promise<import('../protocol/requests/produce/partitionResults').PartitionResult[]|undefined> }>}
   *   once the request is handed to the broker; `answer` settles with its answer, and is wrapped so
   *   that waiting for the hand-off does not wait for the answer
   */
  async #handOff(nodeId, batches) {
    const broker = await this.#cluster.findBroker({ nodeId })
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
      topicData,
      topicIds: transactional
        ? undefined
        : topicIdsOf(
            this.#cluster,
            topicData.map(({ topic }) => topic)
          ),
      transactionVersion: this.#eosManager.stateManager.transactionVersion(),
      perPartitionResults: true,
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
    const manager = this.#eosManager.stateManager
    const results = Array.isArray(answer) ? answer : []
    for (const batch of batches) {
      deliveryOf(batch).onWire = false
    }

    batches.forEach((batch, index) => {
      const result = resultFor(results, index, batch)
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
    const manager = this.#eosManager.stateManager
    for (const batch of batches) {
      deliveryOf(batch).onWire = false
    }

    this.#sideChannels(error)
    for (const batch of batches) {
      this.#failed(manager, batch, error)
    }
  }

  /**
   * Routing work a failure asks for before the next drain: a closed connection's broker is
   * dropped, a KIP-951 leader hint is applied, and stale metadata is refreshed. All best effort.
   * @param {Error} e
   * @returns {void}
   */
  #sideChannels(e) {
    if (e.name === 'KafkaJSConnectionClosedError') {
      this.#cluster.removeBroker({ host: e.host, port: e.port })
    }

    if (e.currentLeader) {
      this.#hints.push(
        Promise.resolve(
          this.#cluster.applyLeaderHint({
            topic: e.topic,
            partition: e.partition,
            leaderId: e.currentLeader.leaderId,
            leaderEpoch: e.currentLeader.leaderEpoch,
            nodeEndpoints: e.nodeEndpoints,
          })
        ).catch((hintError) => {
          this.#logger.debug(`Best-effort leader hint application failed: ${hintError.message}`)
        })
      )
    }

    if (refreshesMetadata(e)) {
      this.#refresh = true
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
   * Settles a queued batch whose deadline passed, through the manager, with `KafkaJSDeliveryTimeout`.
   * @param {import('./state')} manager
   * @param {Delivery} delivery
   * @returns {void}
   */
  #expire(manager, delivery) {
    const error = this.#timedOut(delivery.batch, delivery.lastError)
    manager.fail(delivery.batch, error)
    rejectBatch(delivery.batch, error)
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
      { cause }
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
    const connected = () => connectionStatusError(this.#getConnectionStatus()) === null
    try {
      if (!connected()) {
        return
      }
      if (!this.#cluster.isConnected()) {
        await this.#cluster.connect()
      }
      if (connected()) {
        await this.#cluster.refreshMetadata()
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
 * @param {() => string} [options.getConnectionStatus] the producer's connection status
 * @returns {{ sendPartitioned: Function, partition: Function }}
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
  getConnectionStatus,
}) => {
  const isIdempotent = () => idempotence !== undefined && idempotence.isEnabled()
  let loop = null

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
   * @returns {Promise<never>} rethrows `e` for the retrier, or bails with it when it is not
   *   retriable
   */
  const recover = async (e, bail, retryCount, retryTime) => {
    if (e.name === 'KafkaJSConnectionClosedError') {
      cluster.removeBroker({ host: e.host, port: e.port })
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
        .applyLeaderHint({
          topic: e.topic,
          partition: e.partition,
          leaderId: e.currentLeader.leaderId,
          leaderEpoch: e.currentLeader.leaderEpoch,
          nodeEndpoints: e.nodeEndpoints,
        })
        .catch((hintError) => {
          logger.debug(
            `Best-effort leader hint application failed, continuing with the original error: ${hintError.message}`
          )
        })
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
      await Promise.resolve(cluster.connect()).catch((connectError) => {
        logger.debug(
          `Best-effort reconnect failed, continuing with the original error: ${connectError.message}`
        )
      })
      await Promise.resolve(cluster.refreshMetadata()).catch((refreshError) => {
        logger.debug(
          `Best-effort metadata refresh failed, continuing with the original error: ${refreshError.message}`
        )
      })
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
      await Promise.resolve(cluster.refreshMetadata()).catch((refreshError) => {
        logger.debug(
          `Best-effort metadata refresh failed, continuing with the original error: ${refreshError.message}`
        )
      })
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
   * Assigns each record to a partition once, after making sure the topics' metadata is loaded:
   * the partitioner never runs again for these records. Retried by `retrier` while the metadata
   * cannot be loaded. A transactional producer's failure arms the transaction as `inTransaction`
   * says.
   * @param {{ topic: string, messages: object[] }[]} topicMessages
   * @returns {Promise<{ topic: string, partition: number, messages: object[] }[]>}
   */
  const partition = (topicMessages) =>
    retrier(async (bail) => {
      try {
        await cluster.addMultipleTargetTopics(topicMessages.map(({ topic }) => topic))
        await cluster.refreshMetadataIfNecessary()

        const entries = []
        for (const { topic, messages } of topicMessages) {
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
        if (refreshesMetadata(e)) {
          await Promise.resolve(cluster.refreshMetadata()).catch((refreshError) => {
            logger.debug(`Best-effort metadata refresh failed: ${refreshError.message}`)
          })
        }
        if (e.retriable) throw e
        bail(e)
      }
    }).catch((error) => {
      throw inTransaction(error)
    })

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
      throw new KafkaJSInvariantViolation(`An idempotent send must use acks -1, not ${acks}`)
    }

    loop =
      loop ||
      new DrainLoop({ logger, cluster, eosManager, retry, deliveryTimeout, getConnectionStatus })
    return loop.send({ acks, timeout, compression, batches })
  }

  /** ms from a send's start to its delivery deadline, the producer's */
  const deadlineMs = deliveryTimeout ?? retry.maxTotalTime

  /**
   * Splits one non-idempotent hand-off by delivery deadline, which counts from each batch's own
   * first send: a batch whose deadline has passed is late, and the rest share a retrier that
   * ends at the earliest deadline among them.
   * @param {{ createdAt?: number }[]} batches
   * @returns {{ late: object[], retrier: import("../retry").Retrier|null }} the late batches, and
   *   the retrier of the others; null when every batch is late
   */
  const withinDeadline = (batches) => {
    if (deadlineMs == null) {
      return { late: [], retrier }
    }

    const now = Date.now()
    const late = []
    let budget = deadlineMs
    for (const batch of batches) {
      const remaining = (batch.createdAt ?? now) + deadlineMs - now
      if (remaining < 1) {
        late.push(batch)
      } else {
        budget = Math.min(budget, remaining)
      }
    }
    const noneLeft = late.length > 0 && late.length === batches.length
    return { late, retrier: noneLeft ? null : createRetry({ ...retry, maxTotalTime: budget }) }
  }

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
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java
   * @param {object} request see `sendPartitioned`
   * @returns {Promise<object[]>} the `RecordMetadata` of every batch, once all have settled;
   *   rejects with the first error a batch failed with
   */
  const sendPartitionedPlain = async ({ acks, timeout, compression, batches }) => {
    const unsettled = new Set(batches)
    const metadata = []
    let failure = null

    const resolve = (batch, value) => {
      unsettled.delete(batch)
      if (value !== undefined) {
        metadata.push(value)
      }
      batch.settlers?.forEach((settler) => settler.resolve(value))
    }
    const reject = (batch, error) => {
      unsettled.delete(batch)
      failure = failure || error
      batch.settlers?.forEach((settler) => settler.reject(error))
    }

    /**
     * Sends `broker` the batches it leads and settles those its answer settles.
     * @param {object} broker
     * @param {{ topic: string, partitions: number[], batches: object }[]} owned the batches by
     *   topic, `batches` keyed by partition
     * @returns {Promise<Error|null>} the retriable error that leaves a batch for the next attempt
     */
    const produceTo = async (broker, owned) => {
      const sent = owned.flatMap(({ partitions, batches }) =>
        partitions.map((partition) => batches[partition])
      )
      const topicData = owned.map(({ topic, partitions, batches }) => ({
        topic,
        partitions: partitions.map((partition) => ({
          partition,
          messages: batches[partition].messages,
        })),
      }))

      let results
      try {
        results = await broker.produce({
          ...plainRequestOf(owned.map(({ topic }) => topic)),
          acks,
          timeout,
          compression,
          topicData,
          perPartitionResults: true,
        })
      } catch (e) {
        attachFailingRecords(e, topicData)
        if (e.retriable) {
          return e
        }
        sent.forEach((batch) => reject(batch, e))
        return null
      }

      if (acks === 0) {
        sent.forEach((batch) => resolve(batch, undefined))
        return null
      }

      let retriable = null
      sent.forEach((batch, index) => {
        const result = resultFor(results, index, batch)
        const error =
          result === undefined
            ? new KafkaJSError(`No result for ${batch.topic} partition ${batch.partition}`)
            : result.error
        if (error === null) {
          resolve(batch, metadataOf(result))
        } else if (error.retriable) {
          resolveFailingRecords(error, batch.messages)
          retriable = retriable || error
        } else {
          resolveFailingRecords(error, batch.messages)
          reject(batch, error)
        }
      })
      return retriable
    }

    const { late, retrier: handOffRetrier } = withinDeadline(batches)
    late.forEach((batch) =>
      reject(
        batch,
        new KafkaJSDeliveryTimeout(
          `Gave up on ${batch.topic} partition ${batch.partition} before sending it: its ` +
            `${deadlineMs} ms delivery deadline passed`
        )
      )
    )
    if (handOffRetrier === null) {
      throw failure
    }

    try {
      await handOffRetrier(async (bail, retryCount, retryTime) => {
        const topics = []
        // no prototype: a topic may be named `toString` or `__proto__`
        const byTopic = Object.create(null)
        for (const batch of unsettled) {
          if (byTopic[batch.topic] === undefined) {
            byTopic[batch.topic] = Object.create(null)
            topics.push(batch.topic)
          }
          byTopic[batch.topic][batch.partition] = batch
        }
        await cluster.addMultipleTargetTopics(topics)

        try {
          await cluster.refreshMetadataIfNecessary()

          /** @type {Error[]} the retriable lookup failures that leave batches for the next attempt */
          const left = []
          /**
           * A topic's or a broker's lookup failed: a retriable error leaves its batches for the next
           * attempt, any other rejects them.
           * @param {object[]} lost
           * @param {Error} e
           */
          const leave = (lost, e) => {
            if (e.retriable) {
              left.push(e)
            } else {
              lost.forEach((batch) => reject(batch, e))
            }
          }

          /** @type {Map<object, { topic: string, partitions: number[], batches: object }[]>} */
          const perBroker = new Map()
          for (const topic of topics) {
            let lookup
            try {
              lookup = leadersOf(topic, keys(byTopic[topic]))
            } catch (e) {
              leave(Object.values(byTopic[topic]), e)
              continue
            }

            const { leaders: partitionsPerLeader, failed } = lookup
            for (const { partition, error } of failed) {
              leave([byTopic[topic][partition]], error)
            }
            for (const nodeId of keys(partitionsPerLeader)) {
              const partitions = partitionsPerLeader[nodeId]
              let broker
              try {
                broker = await cluster.findBroker({ nodeId })
              } catch (e) {
                leave(
                  partitions.map((partition) => byTopic[topic][partition]),
                  e
                )
                continue
              }
              const owned = perBroker.get(broker) || []
              owned.push({ topic, partitions, batches: byTopic[topic] })
              perBroker.set(broker, owned)
            }
          }

          const errors = await Promise.all(
            Array.from(perBroker, ([broker, owned]) => produceTo(broker, owned))
          )
          const retriable = [...left, ...errors].find((error) => error !== null)
          if (retriable !== undefined) {
            throw retriable
          }
          if (unsettled.size > 0) {
            throw new KafkaJSMetadataNotLoaded(NO_PARTITION_METADATA)
          }
        } catch (e) {
          await recover(e, bail, retryCount, retryTime)
        }
      })
    } catch (error) {
      unsettled.forEach((batch) => reject(batch, error))
    }

    if (failure !== null) {
      throw failure
    }
    return metadata
  }

  return { sendPartitioned, partition }
}

createSendMessages.connectionStatusError = connectionStatusError

module.exports = createSendMessages
