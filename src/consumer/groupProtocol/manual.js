const { EAGER } = require('./interface')
const { KafkaJSNonRetriableError } = require('../../errors')
const { consumerStoppingError } = require('../rebalanceErrorContract')

/** Local assignment lifecycle; never joins, heartbeats, or starts a membership timer. */
class ManualGroupProtocol {
  /**
   * Copies caller intent for one captured run. Offset coordination is lazy and group-only.
   * @param {object} options
   * @param {object} options.cluster coordinator discovery for offset operations
   * @param {{topic: string, partitions: number[]}[]} options.topicPartitions desired assignment
   * @param {{isCurrent: () => boolean}} options.runIdentity checks only the original run
   * @param {string} [options.groupId] omitted disables every group request
   * @throws {KafkaJSNonRetriableError} for invalid or duplicate entries
   */
  constructor({ cluster, topicPartitions, runIdentity, groupId }) {
    ManualGroupProtocol.validateAssignment(topicPartitions)
    Object.assign(this, { cluster, runIdentity, groupId })
    this.desired = ManualGroupProtocol.snapshot(topicPartitions)
    this.generationId = -1
    this.memberId = ''
    this.groupInstanceId = null
    this.memberEpoch = null
    this.joinCount = 0
    this.membershipRevision = 0
    this.leaderId = null
    this.members = []
    this.groupProtocol = null
    this.coordinator = null
    this.lastRequest = 0
    this.stopped = false
    this.ownedPartitions = []
  }

  /**
   * Validates before requests or mutation.
   * @param {unknown} entry public single-partition argument
   * @returns {void}
   * @throws {KafkaJSNonRetriableError} for invalid topic or partition
   */
  static validateTopicPartition(entry) {
    if (
      !entry ||
      typeof entry !== 'object' ||
      Array.isArray(entry) ||
      typeof entry.topic !== 'string' ||
      entry.topic.length === 0 ||
      !Number.isInteger(entry.partition) ||
      entry.partition < 0 ||
      entry.partition > 2147483647
    ) {
      throw new KafkaJSNonRetriableError(
        'Expected a non-empty topic and an integer partition from 0 to 2147483647'
      )
    }
  }

  /**
   * Checks the complete assignment, including duplicates across repeated topic rows.
   * @param {unknown} entries public assignment argument
   * @returns {void}
   * @throws {KafkaJSNonRetriableError} for malformed, empty or duplicate entries
   */
  static validateAssignment(entries) {
    if (!Array.isArray(entries) || entries.length === 0) {
      throw new KafkaJSNonRetriableError('assign requires a non-empty array of topic partitions')
    }
    const seen = new Map()
    for (const entry of entries) {
      if (
        !entry ||
        typeof entry !== 'object' ||
        Array.isArray(entry) ||
        ![Object.prototype, null].includes(Object.getPrototypeOf(entry)) ||
        !Array.isArray(entry.partitions) ||
        entry.partitions.length === 0
      ) {
        throw new KafkaJSNonRetriableError(
          'assign requires plain entries with non-empty partition arrays'
        )
      }
      for (const partition of entry.partitions) {
        ManualGroupProtocol.validateTopicPartition({ topic: entry.topic, partition })
        const partitions = seen.get(entry.topic) || new Set()
        if (partitions.has(partition)) {
          throw new KafkaJSNonRetriableError('assign requires unique topic-partition pairs')
        }
        partitions.add(partition)
        seen.set(entry.topic, partitions)
      }
    }
  }

  /**
   * Copies validated entries with prototype-safe topic indexing and numeric partition order.
   * @param {{topic: string, partitions: number[]}[]} entries validated assignment
   * @returns {{topic: string, partitions: number[]}[]} merged, sorted defensive copy
   */
  static snapshot(entries) {
    const topics = new Map()
    for (const { topic, partitions } of entries) {
      topics.set(topic, [...(topics.get(topic) || []), ...partitions])
    }
    return [...topics.keys()].sort().map((topic) => ({
      topic,
      partitions: [...topics.get(topic)].sort((a, b) => a - b),
    }))
  }

  /** @param {number} [revision] unused membership revision @returns {boolean} false: no member id exists to forget */
  forgetMemberId(revision) {
    return false
  }

  /** @returns {'manual'} local protocol kind */
  get kind() {
    return 'manual'
  }

  /** @returns {void} @throws {Error} when the captured run has retired */
  assertCurrent() {
    if (this.stopped || !this.runIdentity.isCurrent()) throw consumerStoppingError()
  }

  /** @returns {Promise<void>} local readiness, without coordinator discovery @throws {Error} when retired */
  async connect() {
    this.assertCurrent()
  }

  /**
   * Resolves an offset coordinator, checking the captured run before publication.
   * @returns {Promise<object>} connected coordinator
   * @throws {KafkaJSNonRetriableError} without group ID
   * @throws {Error} on discovery failure or retirement
   */
  async liveCoordinator() {
    this.assertCurrent()
    if (this.groupId === undefined) {
      throw new KafkaJSNonRetriableError('Consumer groupId must be a non-empty string.')
    }
    if (this.coordinator?.isConnected()) return this.coordinator
    const coordinator = await this.cluster.findGroupCoordinator({ groupId: this.groupId })
    this.assertCurrent()
    this.coordinator = coordinator
    return coordinator
  }

  /**
   * Returns a local application result; the caller bypasses membership rounds and events.
   * @returns {Promise<{assignment: object, userData: null}>} copied assignment dictionary
   * @throws {Error} on retirement
   */
  async rebalance() {
    this.assertCurrent()
    const assignment = Object.create(null)
    for (const { topic, partitions } of this.desired) assignment[topic] = [...partitions]
    return { assignment, userData: null }
  }

  /** @param {object} dependencies unused group collaborators @returns {void} */
  attach(dependencies) {}
  /** @returns {Promise<void>} no frame @throws {Error} when obsolete handler work resumes */
  async heartbeat() {
    this.assertCurrent()
  }

  /** @returns {number} no deadline; manual runners never schedule this value */
  heartbeatInterval() {
    return Infinity
  }

  /** @returns {boolean} false: there is no membership target */
  needsReconciliation() {
    return false
  }

  /** @param {Error} error offset refusal, never membership loss @returns {void} */
  onMembershipError(error) {}
  /** @returns {void} retires ownership before teardown awaits */
  onStopRequested() {
    this.stopped = true
    this.ownedPartitions = []
  }

  /** @param {object} [options] unused leave preference @returns {Promise<void>} local retirement */
  async leave(options) {
    this.onStopRequested()
  }

  /** @returns {Promise<void>} local retirement; poll expiry is unreachable for manual runners */
  async leaveForPollExpiry() {
    this.onStopRequested()
  }

  /** @returns {'eager'} whole local replacement, without membership work */
  rebalanceProtocol() {
    return EAGER
  }

  /** @param {object} [options] unused preference @returns {void} permanently retires this instance */
  resetGeneration(options) {
    this.onStopRequested()
  }

  /** @returns {boolean} false: manual consumers never lead groups */
  isLeader() {
    return false
  }

  /** @returns {void} no static membership exists */
  warnIfStaticMembershipInactive() {}
  /** @param {object} assignment unused assigner notification @returns {void} */
  notifyAssignment(assignment) {}
  /** @returns {void} no assigner state exists */
  clearAssignerState() {}
  /** @returns {{topic: string, partitions: number[]}[]} sorted defensive ownership copy */
  owned() {
    return ManualGroupProtocol.snapshot(this.ownedPartitions)
  }

  /** @param {object[]} entries acquired assignment @returns {void} @throws {Error} when retired */
  setOwned(entries) {
    this.assertCurrent()
    this.ownedPartitions = ManualGroupProtocol.snapshot(entries)
  }

  /** @param {object[]} entries retained assignment @returns {void} drops only revoked partitions */
  narrowOwned(entries) {
    const target = new Map(entries.map(({ topic, partitions }) => [topic, new Set(partitions)]))
    this.ownedPartitions = this.ownedPartitions
      .map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.filter((partition) => target.get(topic)?.has(partition)),
      }))
      .filter(({ partitions }) => partitions.length > 0)
  }

  /**
   * Computes local ownership changes without events or wire traffic.
   * @param {object[]} entries next acquired assignment
   * @returns {{added: object[], revoked: object[]}} ownership differences
   */
  diffAssignment(entries) {
    const difference = (left, right) => {
      const index = new Map(right.map(({ topic, partitions }) => [topic, new Set(partitions)]))
      return left
        .map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.filter((partition) => !index.get(topic)?.has(partition)),
        }))
        .filter(({ partitions }) => partitions.length > 0)
    }
    return {
      added: difference(entries, this.ownedPartitions),
      revoked: difference(this.ownedPartitions, entries),
    }
  }
}

module.exports = ManualGroupProtocol
