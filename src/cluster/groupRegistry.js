const OffsetOperations = require('../consumer/offsetManager/offsetOperations')
const CommittedOffsetRevisions = require('./committedOffsetRevisions')
const dictionary = require('../utils/dictionary')

const registries = new WeakMap()

/**
 * Pairs a shared offset cache with its ledger until its last participant and work retire, and
 * retires a topic's cache key and revision stamps once no participant owns the topic, so the pair
 * holds only live topics while the group stays in use.
 */
class GroupRegistry {
  /**
   * Returns the one registry for this pair of stores. Store keys are weakly held.
   * @param {Map<string, object>} offsets shared committed-offset cache
   * @param {Map<string, OffsetOperations>} operations shared operation ledgers
   * @returns {GroupRegistry} the registry for these exact stores
   */
  static forStores(offsets, operations) {
    let byOperations = registries.get(offsets)
    if (!byOperations) {
      byOperations = new WeakMap()
      registries.set(offsets, byOperations)
    }
    let registry = byOperations.get(operations)
    if (!registry) {
      registry = new GroupRegistry(offsets, operations)
      byOperations.set(operations, registry)
    }
    return registry
  }

  /**
   * @param {Map<string, object>} offsets shared cache
   * @param {Map<string, OffsetOperations>} operations shared ledgers
   */
  constructor(offsets, operations) {
    this.offsets = offsets
    this.operations = operations
    this.entries = new Map()
  }

  /**
   * Acquires one holder per Cluster/group lifetime; repeated access is idempotent.
   * @param {string} groupId consumer group
   * @param {object} holder the participant identity
   * @returns {{ledger: OffsetOperations, offsets: object, holders: Set<object>}} paired state
   */
  acquire(groupId, holder) {
    let entry = this.entries.get(groupId)
    if (!entry) {
      const ledger = this.operations.get(groupId) || new OffsetOperations()
      const offsets = ledger.bind(this.offsets.get(groupId) || dictionary())
      entry = { ledger, offsets, holders: new Set() }
      this.entries.set(groupId, entry)
      this.operations.set(groupId, ledger)
      this.offsets.set(groupId, offsets)
      ledger.onIdle = () => this.sweep(groupId, entry)
      ledger.onTopicIdle = (topic) => this.retire(groupId, entry, topic)
      ledger.onRetire = () => this.retireUnowned(groupId, entry)
    }
    entry.holders.add(holder)
    return entry
  }

  /**
   * Releases only this participant. Open tickets and attached tables keep the pair alive.
   * @param {string} groupId consumer group
   * @param {object} holder participant identity
   * @returns {void}
   */
  release(groupId, holder) {
    const entry = this.entries.get(groupId)
    if (!entry) return
    entry.holders.delete(holder)
    this.sweep(groupId, entry)
  }

  /**
   * Drops a topic's cache key and revision stamps when the entry is still current and no open
   * ticket or attached consumer owns the topic. Dropping stamps only makes old private positions
   * stale; the next owner reads the coordinator again.
   * @param {string} groupId consumer group
   * @param {object} entry captured entry, which may already have been replaced
   * @param {string} topic
   * @returns {void}
   */
  retire(groupId, entry, topic) {
    if (this.entries.get(groupId) !== entry) return
    const revisions = CommittedOffsetRevisions.peek(entry.offsets)
    if (!Object.hasOwn(entry.offsets, topic) && !revisions?.cells.has(topic)) return
    if (entry.ledger.isOwned(topic)) return
    delete entry.offsets[topic]
    revisions?.clearTopic(topic)
  }

  /**
   * Retires every topic of a still-current entry that nobody owns.
   * @param {string} groupId consumer group
   * @param {object} entry captured entry, which may already have been replaced
   * @returns {void}
   */
  retireUnowned(groupId, entry) {
    if (this.entries.get(groupId) !== entry) return
    const topics = new Set(Object.keys(entry.offsets))
    for (const topic of CommittedOffsetRevisions.peek(entry.offsets)?.cells.keys() ?? []) {
      topics.add(topic)
    }
    for (const topic of topics) this.retire(groupId, entry, topic)
  }

  /**
   * Retires a still-current, completely idle entry and detaches its callbacks.
   * @param {string} groupId consumer group
   * @param {object} entry captured entry, which may already have been replaced
   * @returns {void}
   */
  sweep(groupId, entry) {
    if (this.entries.get(groupId) !== entry || entry.holders.size || !entry.ledger.isIdle()) return
    this.entries.delete(groupId)
    if (this.operations.get(groupId) === entry.ledger) this.operations.delete(groupId)
    if (this.offsets.get(groupId) === entry.offsets) this.offsets.delete(groupId)
    entry.ledger.onIdle = null
    entry.ledger.onTopicIdle = null
    entry.ledger.onRetire = null
  }
}

module.exports = GroupRegistry
