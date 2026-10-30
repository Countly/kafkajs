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

/**
 * Copies partition rows with replica racks from the same Metadata answer. The internal rack
 * snapshot is non-enumerable so custom assigners keep the public partition-row shape.
 * @param {object[]} rows decoded partition metadata
 * @param {Array<{nodeId:number,rack?:string|null}>} brokers brokers from that answer
 * @returns {object[]} independent round rows with captured replica racks, including non-ISR replicas
 */
module.exports.withReplicaRacks = (rows, brokers) => {
  const racks = new Map(brokers.map(({ nodeId, rack }) => [nodeId, rack]))
  return rows.map((row) =>
    Object.defineProperty({ ...row }, 'replicaRacks', {
      value: (row.replicas || []).map((id) => racks.get(id)).filter((rack) => rack != null),
    })
  )
}
