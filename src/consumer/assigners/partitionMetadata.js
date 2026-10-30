/**
 * Reads the authoritative group snapshot, or the legacy cluster source when absent.
 * @param {object} cluster the legacy metadata owner
 * @param {ReadonlyMap<string, object[]>|undefined} groupTopicMetadata round-local snapshot
 * @param {string} topic exact topic name
 * @returns {object[]} partition rows, including leaderless partition IDs
 */
module.exports = (cluster, groupTopicMetadata, topic) =>
  groupTopicMetadata === undefined
    ? cluster.findTopicPartitionMetadata(topic)
    : (groupTopicMetadata.get(topic) ?? [])
