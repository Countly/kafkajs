const isInvalidOffset = require('./isInvalidOffset')
const dictionary = require('../../utils/dictionary')
const { keys, assign } = Object

const indexPartitions = (obj, { partition, offset }) => assign(obj, { [partition]: offset })
const indexTopics = (obj, { topic, partitions }) =>
  assign(obj, { [topic]: partitions.reduce(indexPartitions, {}) })

/**
 * Keeps valid committed offsets and fills invalid offsets from topic boundary results.
 * Returns fresh topic/partition entries; input arrays are not changed, partition keys
 * become numbers, and fields other than partition and offset are omitted.
 * @param {{topic: string, partitions: {partition: number|string, offset: string}[]}[]} consumerOffsets
 * @param {{topic: string, partitions: {partition: number|string, offset: string}[]}[]} topicOffsets
 * @returns {{topic: string, partitions: {partition: number, offset: string|undefined}[]}[]}
 * @throws {TypeError} for malformed arrays or a missing topic needed to fill an invalid offset.
 */
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
