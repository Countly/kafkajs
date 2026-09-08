const requestV8 = require('../v8/request')

/**
 * OffsetCommitRequest (Version: 9) — flexible (KIP-482), request header v2
 *
 * Byte-identical to v8. What changes is the MEANING of `GenerationIdOrMemberEpoch`: under the
 * KIP-848 consumer protocol it carries the member epoch, which the coordinator validates and
 * answers with STALE_MEMBER_EPOCH when it is behind. A classic member keeps sending its
 * generation id in the same field, so the wire is unchanged for it.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
 * @see clients/src/main/resources/common/message/OffsetCommitRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options as v8; `groupGenerationId` is the member epoch under the consumer
 *   protocol and the generation id under the classic one
 * @returns {object} the protocol request
 */
module.exports = (options) => Object.assign(requestV8(options), { apiVersion: 9 })

module.exports.schema = {
  api: 'OffsetCommit',
  version: 9,
  direction: 'request',
  fields: [
    { name: 'GroupId', type: 'string', versions: '0+' },
    { name: 'GenerationIdOrMemberEpoch', type: 'int32', versions: '1+' },
    { name: 'MemberId', type: 'string', versions: '1+' },
    { name: 'GroupInstanceId', type: 'string', versions: '7+', nullable: true },
    { name: 'Topics', type: '[]OffsetCommitRequestTopic', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0-9' },
    { name: 'Topics.Partitions', type: '[]OffsetCommitRequestPartition', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.CommittedOffset', type: 'int64', versions: '0+' },
    { name: 'Topics.Partitions.CommittedLeaderEpoch', type: 'int32', versions: '6+' },
    { name: 'Topics.Partitions.CommittedMetadata', type: 'string', versions: '0+', nullable: true },
  ],
}
