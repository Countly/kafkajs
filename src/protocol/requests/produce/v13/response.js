const Decoder = require('../../../decoder')
const { parse, decodePartition, decodeNodeEndpoints } = require('../v10/response')

/**
 * ProduceResponse (Version: 13) - flexible, response header v1
 *
 * Identical to v10/v11/v12 except that each topic is keyed by `TopicId` (uuid) instead of `Name`.
 * The name is restored **here, during decode**, from the id-to-name map of the request that
 * produced this response: `Connection#send` parses inside the protocol layer, and `parse` is what
 * sets `error.topic` on a produce failure, so a caller-side restoration would leave that field
 * undefined and silently stop per-record errors being attached to the thrown error.
 * @see clients/src/main/resources/common/message/ProduceResponse.json (Apache Kafka 4.3.1)
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-516%3A+Topic+Identifiers
 */
const decodeTopic = (topicNames) => (decoder) => {
  const topicId = decoder.readUuid()
  const partitions = decoder.readUVarIntArray(decodePartition)
  decoder.readTaggedFields()

  // An id with no mapping cannot happen without a broker bug - the client can only ask for ids it
  // was given - so it surfaces as a null name rather than being hidden or thrown from a codec.
  const topicName = topicNames.get(topicId)
  return { topicId, topicName: topicName === undefined ? null : topicName, partitions }
}

/**
 * @param {Buffer} rawData
 * @param {{ topicNames?: Map<string, string> }} [options] topic id to topic name, bound per request
 * @returns {Promise<object>} same keys as v10's decode, plus `topics[].topicId`
 */
const decode = async (rawData, options = {}) => {
  const topicNames = options.topicNames || new Map()
  const decoder = new Decoder(rawData)
  const topics = decoder.readUVarIntArray(decodeTopic(topicNames))
  const throttleTime = decoder.readInt32()
  const tags = decoder.readTaggedFields()

  return {
    topics,
    throttleTime,
    nodeEndpoints: decodeNodeEndpoints(tags),
  }
}

module.exports = { decode, parse }

module.exports.schema = {
  api: 'Produce',
  version: 13,
  direction: 'response',
  fields: [
    { name: 'Responses', type: '[]TopicProduceResponse', versions: '0+' },
    { name: 'Responses.TopicId', type: 'uuid', versions: '13+' },
    { name: 'Responses.PartitionResponses', type: '[]PartitionProduceResponse', versions: '0+' },
    { name: 'Responses.PartitionResponses.Index', type: 'int32', versions: '0+' },
    { name: 'Responses.PartitionResponses.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Responses.PartitionResponses.BaseOffset', type: 'int64', versions: '0+' },
    { name: 'Responses.PartitionResponses.LogAppendTimeMs', type: 'int64', versions: '2+' },
    { name: 'Responses.PartitionResponses.LogStartOffset', type: 'int64', versions: '5+' },
    {
      name: 'Responses.PartitionResponses.RecordErrors',
      type: '[]BatchIndexAndErrorMessage',
      versions: '8+',
    },
    { name: 'Responses.PartitionResponses.RecordErrors.BatchIndex', type: 'int32', versions: '8+' },
    {
      name: 'Responses.PartitionResponses.RecordErrors.BatchIndexErrorMessage',
      type: 'string',
      versions: '8+',
      nullable: true,
    },
    {
      name: 'Responses.PartitionResponses.ErrorMessage',
      type: 'string',
      versions: '8+',
      nullable: true,
    },
    {
      name: 'Responses.PartitionResponses.CurrentLeader',
      type: 'LeaderIdAndEpoch',
      versions: '10+',
      tag: 0,
    },
    {
      name: 'Responses.PartitionResponses.CurrentLeader.LeaderId',
      type: 'int32',
      versions: '10+',
    },
    {
      name: 'Responses.PartitionResponses.CurrentLeader.LeaderEpoch',
      type: 'int32',
      versions: '10+',
    },
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'NodeEndpoints', type: '[]NodeEndpoint', versions: '10+', tag: 0 },
    { name: 'NodeEndpoints.NodeId', type: 'int32', versions: '10+' },
    { name: 'NodeEndpoints.Host', type: 'string', versions: '10+' },
    { name: 'NodeEndpoints.Port', type: 'int32', versions: '10+' },
    { name: 'NodeEndpoints.Rack', type: 'string', versions: '10+', nullable: true },
  ],
}
