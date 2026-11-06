const Encoder = require('../../../encoder')
const { OffsetFetch: apiKey } = require('../../apiKeys')

/**
 * OffsetFetch Request (Version: 9) — flexible (KIP-482), request header v2
 *
 * Groups => COMPACT_ARRAY
 *   GroupId => COMPACT_STRING
 *   MemberId => COMPACT_NULLABLE_STRING (9+)
 *   MemberEpoch => INT32 (9+)
 *   Topics => COMPACT_ARRAY (nullable)
 *     Name => COMPACT_STRING
 *     PartitionIndexes => COMPACT_ARRAY of INT32
 *     TAG_BUFFER
 *   TAG_BUFFER
 * RequireStable => BOOLEAN
 * TAG_BUFFER
 *
 * v9 is the first version usable with the KIP-848 consumer protocol: the coordinator validates
 * the member id and epoch and answers STALE_MEMBER_EPOCH or UNKNOWN_MEMBER_ID when they are
 * wrong. Their wire defaults — null and -1 — mean "no consumer-protocol membership", which is
 * exactly what a classic member and an admin client mean, so those callers pass nothing and get
 * v8's semantics. `RequireStable` stays request-level and means what it means at v7.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
 * @see clients/src/main/resources/common/message/OffsetFetchRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {object[]} [options.groups] explicit multi-group request; absent retains legacy shape
 * @param {string} options.groupId
 * @param {{topic: string, partitions: {partition: number}[]}[]} options.topics an empty or
 *   absent list asks for every topic in the group, as at every earlier version
 * @param {string|null} [options.memberId=null]
 * @param {number} [options.memberEpoch=-1]
 * @param {boolean} [options.requireStable=false] ask for stable offsets only (KIP-447, see v7)
 * @returns {object} the protocol request
 */
module.exports = ({
  groupId,
  topics,
  groups,
  memberId = null,
  memberEpoch = -1,
  requireStable = false,
}) => ({
  apiKey,
  apiVersion: 9,
  apiName: 'OffsetFetch',
  encode: async () =>
    new Encoder()
      .writeUVarIntArray(
        (groups ?? [{ groupId, topics, memberId, memberEpoch }]).map((group) =>
          new Encoder()
            .writeUVarIntString(group.groupId)
            .writeUVarIntString(group.memberId ?? null)
            .writeInt32(group.memberEpoch ?? -1)
            .writeUVarIntArray(
              group.topics == null || (groups === undefined && group.topics.length === 0)
                ? null
                : group.topics.map(encodeTopic)
            )
            .writeTaggedFields()
        )
      )
      .writeBoolean(requireStable)
      .writeTaggedFields(),
})

const encodeTopic = ({ topic, partitions }) =>
  new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeTaggedFields()

const encodePartition = ({ partition }) => new Encoder().writeInt32(partition)

module.exports.schema = {
  api: 'OffsetFetch',
  version: 9,
  direction: 'request',
  fields: [
    { name: 'Groups', type: '[]OffsetFetchRequestGroup', versions: '8+' },
    { name: 'Groups.GroupId', type: 'string', versions: '8+' },
    { name: 'Groups.MemberId', type: 'string', versions: '9+', nullable: true },
    { name: 'Groups.MemberEpoch', type: 'int32', versions: '9+' },
    { name: 'Groups.Topics', type: '[]OffsetFetchRequestTopics', versions: '8+', nullable: true },
    { name: 'Groups.Topics.Name', type: 'string', versions: '8-9' },
    { name: 'Groups.Topics.PartitionIndexes', type: '[]int32', versions: '8+' },
    { name: 'RequireStable', type: 'bool', versions: '7+' },
  ],
}
