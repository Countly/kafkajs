const RequestV6Protocol = require('./request')
const Decoder = require('../../../decoder')

describe('Protocol > Requests > OffsetFetch > v6', () => {
  test('request', async () => {
    const groupId = 'test-group'
    const topics = [{ topic: 'test-topic', partitions: [{ partition: 0 }, { partition: 1 }] }]

    const { buffer } = await RequestV6Protocol({ groupId, topics }).encode()
    const decoder = new Decoder(buffer)

    expect(decoder.readUVarIntString()).toEqual(groupId)
    const decodedTopics = decoder.readUVarIntArray((topicDecoder) => {
      const topic = {
        topic: topicDecoder.readUVarIntString(),
        partitions: topicDecoder.readUVarIntArray((partitionDecoder) =>
          partitionDecoder.readInt32()
        ),
      }
      topicDecoder.readTaggedFields()
      return topic
    })
    expect(decodedTopics).toEqual([{ topic: 'test-topic', partitions: [0, 1] }])
    expect(decoder.readTaggedFields().size).toEqual(0)
  })

  test('encodes an empty topics list as the null compact array (fetch every topic)', async () => {
    const { buffer } = await RequestV6Protocol({ groupId: 'test-group', topics: [] }).encode()
    const decoder = new Decoder(buffer)
    decoder.readUVarIntString()
    expect(decoder.readUVarIntArray(() => {})).toBeNull()
  })
})
