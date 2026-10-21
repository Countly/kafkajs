const isInvalidOffset = require('./isInvalidOffset')
const dictionary = require('../../utils/dictionary')
const { keys, assign } = Object

const indexPartitions = (obj, { partition, offset }) => assign(obj, { [partition]: offset })
const indexTopics = (obj, { topic, partitions }) =>
  assign(obj, { [topic]: partitions.reduce(indexPartitions, {}) })

module.exports = (consumerOffsets, topicOffsets) => {
  const indexedConsumerOffsets = consumerOffsets.reduce(indexTopics, dictionary())
  const indexedTopicOffsets = topicOffsets.reduce(indexTopics, dictionary())

  return keys(indexedConsumerOffsets).map((topic) => {
    const partitions = indexedConsumerOffsets[topic]
    return {
      topic,
      partitions: keys(partitions).map((partition) => {
        const offset = partitions[partition]
        const resolvedOffset = isInvalidOffset(offset)
          ? indexedTopicOffsets[topic][partition]
          : offset

        return { partition: Number(partition), offset: resolvedOffset }
      }),
    }
  })
}
