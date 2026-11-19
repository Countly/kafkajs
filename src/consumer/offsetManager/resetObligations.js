/**
 * The resets a consumer owes the topics that were deleted and recreated while it was subscribed.
 *
 * Owned by the `Consumer` and shared by reference with every `ConsumerGroup` and `OffsetManager`
 * it builds, so an obligation survives the rebuilds a rebalance or a crash restart causes. An
 * obligation is not consumed by reading it: a partition settles only when a confirmed
 * current-incarnation durable commit (an explicit or automatic commit, or a confirmed
 * transaction's offsets) is applied to it. A local resolution, a seek with auto-commit off, a
 * sentinel boundary, a failed ListOffsets or a dropped answer leave it owed. Only a user
 * disconnect, through `clear()`, ends the obligations.
 *
 * The table is also the consumer's ownership record in the group's ledger: the topics it
 * subscribes to, the topics its live offset manager is assigned and the topics it owes a reset keep
 * the group's committed state; every change that gives a topic up asks the ledger to retire it.
 * The owner sets are snapshots, replaced as a whole.
 */
class ResetObligations {
  constructor() {
    /** @type {Map<string, {stamp: string, discharged: Set<number>}>} */
    this.entries = new Map()
    this.detach = null
    /** @type {import('./offsetOperations')|null} the ledger this table is linked to */
    this.ledger = null
    /** @type {Set<string>} the consumer's subscribed names */
    this.subscribed = new Set()
    /** @type {Set<string>} the topics of the consumer's live assignment */
    this.assigned = new Set()
  }

  /**
   * Whether this consumer owns a topic of its group. Reads only.
   * @param {string} topic
   * @returns {boolean}
   */
  holds(topic) {
    return this.entries.has(topic) || this.subscribed.has(topic) || this.assigned.has(topic)
  }

  /**
   * Replaces the subscribed names and retires the topics that are owned no longer.
   * @param {Iterable<string>} topics copied, never retained
   * @returns {void}
   */
  claimSubscription(topics) {
    this.subscribed = new Set(topics)
    this.retireUnowned()
  }

  /**
   * Replaces the live assignment's topics. The offset manager asks for retirement itself, once it
   * has applied the assignment.
   * @param {Iterable<string>} topics copied, never retained
   * @returns {void}
   */
  claimAssignment(topics) {
    this.assigned = new Set(topics)
  }

  /**
   * Ends assignment ownership when the run stopped and no manager is live, then retires.
   * @returns {void}
   */
  releaseAssignment() {
    this.assigned = new Set()
    this.retireUnowned()
  }

  /**
   * Asks the linked ledger to retire the topics nobody owns; an unlinked table asks nobody.
   * @returns {void}
   */
  retireUnowned() {
    this.ledger?.retireUnowned()
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
      this.retireUnowned()
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
   * A confirmed durable commit settled this partition for the new incarnation. Ignored when both
   * the mutation's topic id and the stamp are known and differ.
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
   * Deletes the entry once every partition of the incarnation has discharged it. An empty or
   * partial assignment never deletes a debt another partition still owes: given the topic's known
   * partitions, only a complete discharge of all of them deletes, and an empty list (metadata that
   * could not be verified) deletes nothing. Retained entries are bounded by the live topics; a
   * topic's retirement or the user's disconnect (`clear`) ends them.
   * @param {string} topic
   * @param {Iterable<number>} assignedPartitions the partitions this member holds; the only ones
   *   checked when `knownPartitions` is absent
   * @param {Iterable<number>} [knownPartitions] every partition the metadata cache holds for the
   *   topic. Absent for a caller with no metadata to offer, which keeps the assigned-only rule.
   * @returns {void}
   */
  settle(topic, assignedPartitions, knownPartitions) {
    const entry = this.entries.get(topic)
    if (!entry) {
      return
    }

    const partitions =
      knownPartitions === undefined ? [...assignedPartitions] : [...knownPartitions]
    if (knownPartitions !== undefined && partitions.length === 0) {
      return
    }

    for (const partition of partitions) {
      if (!entry.discharged.has(partition)) {
        return
      }
    }

    this.entries.delete(topic)
    this.retireUnowned()
  }

  /**
   * Drops the entries of every topic outside `topics`. A consumer whose subscription changes calls
   * it so debts stay bounded by the names it can still read; a name that returns owes anew.
   * @param {Set<string>} topics the names that keep their entries
   * @returns {void}
   */
  retain(topics) {
    let dropped = false
    for (const topic of this.entries.keys()) {
      if (!topics.has(topic)) dropped = this.entries.delete(topic)
    }
    if (dropped) this.retireUnowned()
  }

  /**
   * Links this table to a group's operation ledger, so a mutation applied by any participant can
   * discharge it. Linking to the same ledger again changes nothing; linking to another replaces
   * the link and lets the old ledger retire what this table owned there.
   * @param {import('./offsetOperations')} operations
   * @returns {void}
   */
  attachTo(operations) {
    if (this.ledger === operations) return
    const previous = this.ledger
    this.detach?.()
    this.detach = operations.attach(this)
    this.ledger = operations
    previous?.retireUnowned()
  }

  /**
   * Unlinks from the ledger and keeps every entry and owner set, for a consumer that rebuilds its
   * group after a crash: the next group it builds links the table again and the debt is still
   * owed. The ledger retires what only this table owned.
   * @returns {void}
   */
  unlink() {
    const previous = this.ledger
    this.detach?.()
    this.detach = null
    this.ledger = null
    previous?.retireUnowned()
  }

  /**
   * Drops every entry and owner set and unlinks from the ledger. Called when the user disconnects
   * the consumer, which ends its obligations; a later run starts with none.
   * @returns {void}
   */
  clear() {
    this.entries.clear()
    this.subscribed = new Set()
    this.assigned = new Set()
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
