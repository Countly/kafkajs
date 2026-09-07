const createState = (topic) => ({
  topic,
  paused: new Set(),
  pauseAll: false,
  resumed: new Set(),
})

module.exports = class SubscriptionState {
  constructor() {
    this.assignedPartitionsByTopic = {}
    /**
     * The same assignment as a membership index. Rebuilt in `assign()`, which runs once per
     * apply, so the per-message ownership check is a hash lookup instead of a scan of the whole
     * partition list for that topic.
     * @type {Map<string, Set<number>>}
     */
    this.assignedPartitionSetsByTopic = new Map()
    this.subscriptionStatesByTopic = {}
  }

  /**
   * Replace the current assignment with a new set of assignments
   *
   * @param {Array<TopicPartitions>} topicPartitions Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  assign(topicPartitions = []) {
    this.assignedPartitionsByTopic = topicPartitions.reduce(
      (assigned, { topic, partitions = [] }) => {
        return { ...assigned, [topic]: { topic, partitions } }
      },
      {}
    )

    this.assignedPartitionSetsByTopic = new Map(
      topicPartitions.map(({ topic, partitions = [] }) => [topic, new Set(partitions)])
    )
  }

  /**
   * Replaces the assignment without touching the pause state.
   *
   * `assign()` has never cleared `subscriptionStatesByTopic`, so a paused partition survives a
   * rebalance today; an incremental apply must keep that property. Delegating to `assign()` is
   * deliberate - one body, so the two cannot drift apart - and the name is what documents the
   * intent at the call site.
   *
   * @param {Array<TopicPartitions>} topicPartitions Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   * @returns {void}
   */
  update(topicPartitions = []) {
    this.assign(topicPartitions)
  }

  /**
   * @param {Array<TopicPartitions>} topicPartitions Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  pause(topicPartitions = []) {
    topicPartitions.forEach(({ topic, partitions }) => {
      const state = this.subscriptionStatesByTopic[topic] || createState(topic)

      if (typeof partitions === 'undefined') {
        state.paused.clear()
        state.resumed.clear()
        state.pauseAll = true
      } else if (Array.isArray(partitions)) {
        partitions.forEach((partition) => {
          state.paused.add(partition)
          state.resumed.delete(partition)
        })
        state.pauseAll = false
      }

      this.subscriptionStatesByTopic[topic] = state
    })
  }

  /**
   * @param {Array<TopicPartitions>} topicPartitions Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  resume(topicPartitions = []) {
    topicPartitions.forEach(({ topic, partitions }) => {
      const state = this.subscriptionStatesByTopic[topic] || createState(topic)

      if (typeof partitions === 'undefined') {
        state.paused.clear()
        state.resumed.clear()
        state.pauseAll = false
      } else if (Array.isArray(partitions)) {
        partitions.forEach((partition) => {
          state.paused.delete(partition)

          if (state.pauseAll) {
            state.resumed.add(partition)
          }
        })
      }

      this.subscriptionStatesByTopic[topic] = state
    })
  }

  /**
   * @returns {Array<import("../../types").TopicPartitions>} topicPartitions
   * Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  assigned() {
    return Object.values(this.assignedPartitionsByTopic).map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.sort(),
    }))
  }

  /**
   * @returns {Array<import("../../types").TopicPartitions>} topicPartitions
   * Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  active() {
    return Object.values(this.assignedPartitionsByTopic).map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.filter((partition) => !this.isPaused(topic, partition)).sort(),
    }))
  }

  /**
   * @returns {Array<import("../../types").TopicPartitions>} topicPartitions
   * Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  paused() {
    return Object.values(this.assignedPartitionsByTopic)
      .map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.filter((partition) => this.isPaused(topic, partition)).sort(),
      }))
      .filter(({ partitions }) => partitions.length !== 0)
  }

  /**
   * Whether a topic-partition is part of the current assignment.
   *
   * A batch is fetched under one assignment and handed to a worker under whatever assignment is
   * current when the worker gets to it, so the two can differ across a rebalance.
   *
   * @param {string} topic
   * @param {number} partition
   * @returns {boolean}
   */
  isAssigned(topic, partition) {
    const partitions = this.assignedPartitionSetsByTopic.get(topic)
    return partitions !== undefined && partitions.has(partition)
  }

  isPaused(topic, partition) {
    const state = this.subscriptionStatesByTopic[topic]

    if (!state) {
      return false
    }

    const partitionResumed = state.resumed.has(partition)
    const partitionPaused = state.paused.has(partition)

    return (state.pauseAll && !partitionResumed) || partitionPaused
  }
}
