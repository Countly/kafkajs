const { createErrorFromCode } = require('../protocol/error')
const { KafkaJSError, KafkaJSNonRetriableError, KafkaJSTimeout } = require('../errors')

/**
 * The page size asked for. The broker lowers it to its own `max.request.partition.size.limit`, so
 * the number of partitions a page holds is never counted on.
 */
const PAGE_PARTITION_LIMIT = 2000

/**
 * The one overall budget of a DescribeTopicPartitions read, in milliseconds: Java's
 * `default.api.timeout.ms` default.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/CommonClientConfigs.java
 */
const READ_TIMEOUT_MS = 60000

/**
 * The private state of one read's deadline: `expired` once the deadline passed, and `pages`, the
 * topics read so far, which are dropped when it passes.
 * @typedef {{ expired: boolean, pages: Map<string, PageTopic>|null }} ReadDeadline
 */

/** @returns {KafkaJSTimeout} the error a read rejects with at its deadline */
const readTimeout = () =>
  new KafkaJSTimeout(`DescribeTopicPartitions did not complete within ${READ_TIMEOUT_MS} ms`)

/**
 * Runs `work` under one overall deadline taken now. At the deadline the returned promise rejects
 * with `KafkaJSTimeout` whatever `work` is waiting on, the state is marked expired and the pages
 * read so far are dropped; the timer is cleared however the call ends. `work` sees the state and
 * stops at its next check; its own late outcome is ignored. A request already sent is not cancelled.
 * @template T
 * @param {(deadline: ReadDeadline) => Promise<T>} work
 * @param {number} [timeoutMs=READ_TIMEOUT_MS]
 * @returns {Promise<T>}
 * @throws {KafkaJSTimeout} at the deadline
 */
const withReadDeadline = async (work, timeoutMs = READ_TIMEOUT_MS) => {
  const deadline = { expired: false, pages: null }
  let timer
  const expiry = new Promise((resolve, reject) => {
    timer = setTimeout(() => {
      deadline.expired = true
      deadline.pages = null
      reject(readTimeout())
    }, timeoutMs)
  })
  const running = work(deadline)
  running.catch(() => {})

  try {
    return await Promise.race([running, expiry])
  } finally {
    clearTimeout(timer)
  }
}

/**
 * @param {ReadDeadline} deadline
 * @returns {void}
 * @throws {KafkaJSTimeout} once the deadline has passed
 */
const checkDeadline = (deadline) => {
  if (deadline.expired) {
    throw readTimeout()
  }
}

/**
 * @typedef {object} PageTopic one topic as `Broker#describeTopicPartitions` decodes it, with the
 *   partitions of every page it was read from
 * @property {number} errorCode the topic's own error, 0 when none
 * @property {string|null} name
 * @property {string|null} topicId
 * @property {boolean} isInternal
 * @property {object[]} partitions
 */

/**
 * Reads the topics `names` page by page and gathers each topic's partitions, following Java's
 * `Admin#describeTopics`: a topic the page ends inside is the cursor topic, it stays in the next
 * request and the next cursor is `{ topic, partitions read so far }`; a topic that is complete, or
 * failed, leaves the request. When the broker ends a page exactly at a topic boundary it names the
 * next topic at partition 0 and leaves it out of the page; that topic has no partitions read, so no
 * cursor is sent and it simply heads the next request.
 *
 * Every page must move the read forward: a real broker's pages are contiguous (a topic's partition
 * ids are dense and a page resumes where the last one stopped) and answer every topic asked, the
 * ones it may not describe and the ones that do not exist as rows with an error code. A broker that
 * breaks that, such as one that repeats a page, or answers a cursor with nothing, would keep the
 * loop asking for ever, so the loop fails instead of returning what it has.
 *
 * A topic keeps the first known id its rows carry (a row of an unknown topic carries none); a later
 * row with another known id means the topic was deleted and recreated between pages, so the read
 * fails retriably before any partition of that row is merged, and the caller reads again from the
 * start. The deadline is checked before each page is asked for and after each page arrives, so a
 * page that arrives after it changes nothing and no page starts after it.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java
 * @see https://github.com/apache/kafka/blob/4.3.1/metadata/src/main/java/org/apache/kafka/metadata/KRaftMetadataCache.java
 * @param {{ describeTopicPartitions: Function }} broker
 * @param {string[]} names sorted by UTF-16 code unit and without duplicates
 * @param {ReadDeadline} deadline holds the topics read so far
 * @returns {Promise<Map<string, PageTopic>>} every topic of `names`, by name
 * @throws {KafkaJSNonRetriableError} when a page starts a topic's partitions anywhere but where the
 *   last one stopped, names a cursor and brings nothing new, or the broker ends the read without
 *   having answered for every topic
 * @throws {KafkaJSError} retriable, when a topic's known id changes between pages
 * @throws {KafkaJSTimeout} once the deadline has passed
 */
const readPages = async (broker, names, deadline) => {
  const pending = new Set(names)
  deadline.pages = new Map()
  let next = { cursor: null }

  while (pending.size > 0 && next !== null) {
    checkDeadline(deadline)
    next = await readPage(broker, pending, next.cursor, deadline)
  }

  if (pending.size > 0) {
    throw new KafkaJSNonRetriableError(
      `DescribeTopicPartitions ended without answering for ${[...pending].join(', ')}`
    )
  }

  return deadline.pages
}

/**
 * Asks for one page and merges it into `deadline.pages`. The page is held by this call alone and
 * nothing of it is returned, so once the call ends no part of the page survives in the waiting
 * read: at the deadline, dropping `deadline.pages` releases everything read.
 * @param {{ describeTopicPartitions: Function }} broker
 * @param {Set<string>} pending the topics still to read; the ones this page completes are removed
 * @param {{ topicName: string, partitionIndex: number }|null} cursor where this page starts
 * @param {ReadDeadline} deadline
 * @returns {Promise<{ cursor: { topicName: string, partitionIndex: number }|null }|null>} where
 *   the next page starts, or null when the broker ended the read
 * @throws {KafkaJSNonRetriableError} when the page breaks the rules of {@link readPages}
 * @throws {KafkaJSError} retriable, when a topic's known id changes
 * @throws {KafkaJSTimeout} when the page arrives after the deadline
 */
const readPage = async (broker, pending, cursor, deadline) => {
  const { topics, nextCursor } = await broker.describeTopicPartitions({
    topics: [...pending],
    responsePartitionLimit: PAGE_PARTITION_LIMIT,
    cursor,
  })
  checkDeadline(deadline)
  const read = deadline.pages
  let progressed = false

  for (const row of topics) {
    if (!pending.has(row.name)) {
      continue
    }

    const topic = read.get(row.name) ?? { ...row, partitions: [] }
    const [first] = row.partitions

    if (row.topicId != null && topic.topicId != null && row.topicId !== topic.topicId) {
      throw new KafkaJSError(`topic ${row.name} was recreated while it was being read`, {
        retriable: true,
      })
    }

    topic.topicId = topic.topicId ?? row.topicId

    if (first && first.partitionIndex !== topic.partitions.length) {
      throw new KafkaJSNonRetriableError(
        `DescribeTopicPartitions answered partition ${first.partitionIndex} of ${row.name} where ${topic.partitions.length} was next`
      )
    }

    topic.partitions.push(...row.partitions)
    topic.errorCode = row.errorCode || topic.errorCode
    read.set(row.name, topic)
    progressed = progressed || row.errorCode !== 0 || row.partitions.length > 0

    if (row.errorCode !== 0 || nextCursor?.topicName !== row.name) {
      pending.delete(row.name)
    }
  }

  if (nextCursor === null) {
    return null
  }

  if (!progressed) {
    throw new KafkaJSNonRetriableError(
      `DescribeTopicPartitions named the cursor ${nextCursor.topicName}:${nextCursor.partitionIndex} and sent nothing new`
    )
  }

  const partial = read.get(nextCursor.topicName)
  return {
    cursor:
      partial && pending.has(partial.name)
        ? { topicName: partial.name, partitionIndex: partial.partitions.length }
        : null,
  }
}

/**
 * Reads the named topics through DescribeTopicPartitions.
 * @param {{ describeTopicPartitions: Function }} broker any broker
 * @param {string[]} names the topics to describe; at least one, duplicates are read once
 * @param {ReadDeadline} deadline the deadline of the whole operation, see {@link withReadDeadline}
 * @returns {Promise<PageTopic[]>} one row per topic, in name order; a topic the broker failed has
 *   its `errorCode` and no partitions
 * @throws {KafkaJSServerDoesNotSupportApiKey} when the broker does not offer the API
 * @throws {KafkaJSNonRetriableError} when the broker's pages break the rules of {@link readPages}
 * @throws {KafkaJSError} retriable, when a topic was recreated between pages
 * @throws {KafkaJSTimeout} once the deadline has passed
 */
const readTopicPages = async (broker, names, deadline) => {
  const sortedNames = [...new Set(names)].sort()
  const read = await readPages(broker, sortedNames, deadline)

  return sortedNames.map((name) => read.get(name))
}

/**
 * @param {object} partition a decoded DescribeTopicPartitions partition
 * @returns {object} the `PartitionMetadata` of the Metadata path, plus `leaderEpoch`,
 *   `eligibleLeaderReplicas` (`null` when the cluster does not track them) and `lastKnownElr`
 */
const toPartitionMetadata = (partition) => ({
  partitionErrorCode: partition.errorCode,
  partitionId: partition.partitionIndex,
  leader: partition.leaderId,
  replicas: partition.replicaNodes,
  isr: partition.isrNodes,
  offlineReplicas: partition.offlineReplicas,
  leaderEpoch: partition.leaderEpoch,
  eligibleLeaderReplicas: partition.eligibleLeaderReplicas,
  lastKnownElr: partition.lastKnownElr,
})

/**
 * What `admin.fetchTopicMetadata` returns for topics read by {@link readTopicPages}, with each
 * topic's `topicId` and `isInternal`.
 *
 * A topic the broker failed (unknown, not authorized) did not stop the others from being read; the
 * call rejects with the error of the first failing topic in name order. A partition's own error
 * code is data in `partitionErrorCode`, passed through as the broker sends it. A missing leader
 * shows as `leader: -1`: Kafka 4.1.2 and 4.3 brokers leave the code at 0 for it, so check the
 * leader to find a leaderless partition. Unlike a Metadata request for named topics, this path
 * never waits for leaders. DescribeTopicPartitions never creates a topic.
 * @param {PageTopic[]} rows in name order
 * @returns {{ topics: object[] }}
 * @throws {KafkaJSProtocolError} for the first failing topic
 */
const toTopicsMetadata = (rows) => {
  const failed = rows.find(({ errorCode }) => errorCode !== 0)

  if (failed) {
    throw createErrorFromCode(failed.errorCode)
  }

  return {
    topics: rows.map(({ name, topicId, isInternal, partitions }) => ({
      name,
      topicId,
      isInternal,
      partitions: partitions.map(toPartitionMetadata),
    })),
  }
}

module.exports = { readTopicPages, toTopicsMetadata, withReadDeadline, checkDeadline }
