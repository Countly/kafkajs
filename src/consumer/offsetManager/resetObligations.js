/**
 * The resets a consumer owes the topics that were deleted and recreated while it was subscribed.
 *
 * Owned by the `Consumer` and shared by reference with every `ConsumerGroup` and `OffsetManager`
 * it builds, so an obligation survives the rebuilds a rebalance or a crash restart causes. An
 * obligation is not consumed by reading it: a partition settles only when a write, seek, commit or
 * publication is applied to it, so a failed ListOffsets or a dropped answer leaves it owed. Only a
 * user disconnect, through `clear()`, ends the obligations.
 */
class ResetObligations {
  constructor() {
    /** @type {Map<string, {stamp: string, discharged: Set<number>}>} */
    this.entries = new Map()
    this.detach = null
  }

  /**
   * A recreation was observed: the topic owes a reset on every partition for its new incarnation.
   * A newer recreation replaces the entry of an older one.
   * @param {string} topic
   * @param {string} topicId the id of the new incarnation
   * @returns {void}
   */
  owe(topic, topicId) {
    this.entries.set(topic, { stamp: topicId, discharged: new Set() })
  }

  /**
   * @param {string} topic
   * @param {string} [cachedTopicId] the topic id the metadata cache holds now, undefined when unknown
   * @returns {string|undefined} the stamp of the owed reset. A cached id that is known and differs
   *   from the stamp disposes of the entry and returns undefined; an unknown id keeps it, because
   *   a cleared cache is not a recreation.
   */
  current(topic, cachedTopicId) {
    const entry = this.entries.get(topic)
    if (!entry) {
      return undefined
    }

    if (cachedTopicId !== undefined && cachedTopicId !== entry.stamp) {
      this.entries.delete(topic)
      return undefined
    }

    return entry.stamp
  }

  /**
   * @param {string} topic
   * @param {number} partition
   * @returns {boolean} whether this partition still owes the reset
   */
  isOwed(topic, partition) {
    const entry = this.entries.get(topic)
    return entry !== undefined && !entry.discharged.has(partition)
  }

  /**
   * An applied write, seek, commit or publication settled this partition for the new incarnation.
   * Ignored when both the mutation's topic id and the stamp are known and differ.
   * @param {string} topic
   * @param {number} partition
   * @param {string} [topicId] the topic id the mutation was made under, when known
   * @returns {void}
   */
  discharge(topic, partition, topicId) {
    const entry = this.entries.get(topic)
    if (!entry || (topicId !== undefined && topicId !== entry.stamp)) {
      return
    }

    entry.discharged.add(partition)
  }

  /**
   * Deletes the entry once every currently assigned partition has discharged it.
   * @param {string} topic
   * @param {Iterable<number>} assignedPartitions
   * @returns {void}
   */
  settle(topic, assignedPartitions) {
    const entry = this.entries.get(topic)
    if (!entry) {
      return
    }

    for (const partition of assignedPartitions) {
      if (!entry.discharged.has(partition)) {
        return
      }
    }

    this.entries.delete(topic)
  }

  /**
   * Links this table to a group's operation ledger, so a mutation applied by any participant can
   * discharge it. Linking again replaces the previous link.
   * @param {import('./offsetOperations')} operations
   * @returns {void}
   */
  attachTo(operations) {
    this.detach?.()
    this.detach = operations.attach(this)
  }

  /**
   * Unlinks from the ledger and keeps every entry, for a consumer that rebuilds its group after
   * a crash: the next group it builds links the table again and the debt is still owed.
   * @returns {void}
   */
  unlink() {
    this.detach?.()
    this.detach = null
  }

  /**
   * Drops every entry and unlinks from the ledger. Called when the user disconnects the consumer,
   * which ends its obligations; a later run starts with none.
   * @returns {void}
   */
  clear() {
    this.entries.clear()
    this.unlink()
  }

  /**
   * @returns {number} the topics that owe a reset, for the resource probe and the tests
   */
  size() {
    return this.entries.size
  }
}

module.exports = ResetObligations
