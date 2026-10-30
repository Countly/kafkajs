const createRetry = require('../retry')
const { KafkaJSInvariantViolation, isStaleCoordinator } = require('../errors')

/**
 * Reads a group's committed offsets from its coordinator as an observation: stable reads
 * (RequireStable) with no member identity, so a manual or detached consumer can read offsets of
 * partitions it does not own. Nothing here touches the offset cache, the operation ledger or any
 * resolved position.
 */
class CommittedOffsetReader {
  /**
   * @param {object} options
   * @param {import('../../types').Cluster} options.cluster coordinator discovery
   * @param {string} options.groupId the group whose commits are read
   * @param {import('../../types').RetryOptions} [options.retry] the consumer's retry options
   */
  constructor({ cluster, groupId, retry }) {
    this.cluster = cluster
    this.groupId = groupId
    this.retryOptions = Object.assign({}, retry)
    this.retrier = createRetry(this.retryOptions)
  }

  /**
   * Reads one offset per requested pair, in input order.
   * @param {{topic: string, partition: number}[]} entries validated, non-empty and unique pairs
   * @param {{ operation?: import('./groupProtocol/interface').ConsumerOperation }} [context] the
   *   public call's one deadline scope: its retries, coordinator lookup and write guard replace the
   *   ordinary retrier's, and nothing is returned once it expired
   * @returns {Promise<{topic: string, partition: number, offset: string|null, metadata: string|null}[]>}
   *   rows in input order; an offset the group never committed is `null`, not `'-1'`, and its
   *   metadata is `null` too (the broker answers `''` for it)
   * @throws {KafkaJSInvariantViolation} when the answer names another group or omits a requested cell
   * @throws {Error} a broker or partition error once the retry budget is spent, or the operation's
   *   own refusal
   */
  async read(entries, { operation } = {}) {
    const topics = []
    const byTopic = new Map()
    for (const { topic, partition } of entries) {
      let partitions = byTopic.get(topic)
      if (!partitions) {
        partitions = []
        byTopic.set(topic, partitions)
        topics.push({ topic, partitions })
      }
      partitions.push({ partition })
    }

    const attempt = async (bail) => {
      try {
        const coordinator = await this.cluster.findGroupCoordinator({
          groupId: this.groupId,
          ...(operation && { fence: operation }),
        })
        operation?.check()
        return await coordinator.offsetFetch({
          groupId: this.groupId,
          topics,
          memberId: null,
          memberEpoch: -1,
          requireStable: true,
          ...(operation && { guard: operation.guard }),
        })
      } catch (error) {
        if (operation?.guard()) throw operation.guard()
        if (isStaleCoordinator(error) || error.retriable !== false) throw error
        return bail(error)
      }
    }
    const { responses } = await (operation
      ? operation.retry(attempt, this.retryOptions)
      : this.retrier(attempt))

    const cells = new Map()
    for (const { topic, partitions } of responses) {
      for (const cell of partitions) cells.set(`${topic}|${cell.partition}`, cell)
    }

    return entries.map(({ topic, partition }) => {
      const cell = cells.get(`${topic}|${partition}`)
      if (!cell) {
        throw new KafkaJSInvariantViolation(
          `OffsetFetch for group "${this.groupId}" returned no answer for ${topic}[${partition}]`
        )
      }
      const uncommitted = cell.offset == null || String(cell.offset).startsWith('-')
      return {
        topic,
        partition,
        offset: uncommitted ? null : String(cell.offset),
        metadata: uncommitted ? null : (cell.metadata ?? null),
      }
    })
  }
}

module.exports = CommittedOffsetReader
