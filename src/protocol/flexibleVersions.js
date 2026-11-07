/**
 * The first flexible (KIP-482) version of each API, keyed by API key, or `null` when the API
 * has no flexible version at all. A flexible version uses compact strings/bytes/arrays and
 * ends every structure with a tagged-field section; it also selects request header v2 and
 * response header v1.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields
 * @see https://kafka.apache.org/43/design/protocol
 *
 * Generated from the Apache Kafka 4.4.0 message definitions
 * (clients/src/main/resources/common/message/<Name>Request.json).
 * Do not edit by hand — run `npm run protocol:generate`; `npm run protocol:check` fails on drift.
 *
 * @type {{ [apiKey: number]: number | null }}
 */
module.exports = {
  0: 9, // Produce
  1: 12, // Fetch
  2: 6, // ListOffsets
  3: 9, // Metadata
  8: 8, // OffsetCommit
  9: 6, // OffsetFetch
  10: 3, // FindCoordinator
  11: 6, // JoinGroup
  12: 4, // Heartbeat
  13: 4, // LeaveGroup
  14: 4, // SyncGroup
  15: 5, // DescribeGroups
  16: 3, // ListGroups
  17: null, // SaslHandshake
  18: 3, // ApiVersions
  19: 5, // CreateTopics
  20: 4, // DeleteTopics
  21: 2, // DeleteRecords
  22: 2, // InitProducerId
  23: 4, // OffsetForLeaderEpoch
  24: 3, // AddPartitionsToTxn
  25: 3, // AddOffsetsToTxn
  26: 3, // EndTxn
  27: 1, // WriteTxnMarkers
  28: 3, // TxnOffsetCommit
  29: 2, // DescribeAcls
  30: 2, // CreateAcls
  31: 2, // DeleteAcls
  32: 4, // DescribeConfigs
  33: 2, // AlterConfigs
  34: 2, // AlterReplicaLogDirs
  35: 2, // DescribeLogDirs
  36: 2, // SaslAuthenticate
  37: 2, // CreatePartitions
  38: 2, // CreateDelegationToken
  39: 2, // RenewDelegationToken
  40: 2, // ExpireDelegationToken
  41: 2, // DescribeDelegationToken
  42: 2, // DeleteGroups
  43: 2, // ElectLeaders
  44: 1, // IncrementalAlterConfigs
  45: 0, // AlterPartitionReassignments
  46: 0, // ListPartitionReassignments
  47: null, // OffsetDelete
  48: 1, // DescribeClientQuotas
  49: 1, // AlterClientQuotas
  50: 0, // DescribeUserScramCredentials
  51: 0, // AlterUserScramCredentials
  57: 0, // UpdateFeatures
  60: 0, // DescribeCluster
  61: 0, // DescribeProducers
  65: 0, // DescribeTransactions
  66: 0, // ListTransactions
  68: 0, // ConsumerGroupHeartbeat
  69: 0, // ConsumerGroupDescribe
  71: 0, // GetTelemetrySubscriptions
  72: 0, // PushTelemetry
  74: 0, // ListConfigResources
  75: 0, // DescribeTopicPartitions
  76: 0, // ShareGroupHeartbeat
  77: 0, // ShareGroupDescribe
  78: 0, // ShareFetch
  79: 0, // ShareAcknowledge
  90: 0, // DescribeShareGroupOffsets
  91: 0, // AlterShareGroupOffsets
  92: 0, // DeleteShareGroupOffsets
}
