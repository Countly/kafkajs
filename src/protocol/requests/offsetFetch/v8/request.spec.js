const RequestV8Protocol = require('./request')
const Decoder = require('../../../decoder')

const decodeGroups = (decoder) =>
  decoder.readUVarIntArray((groupDecoder) => {
    const group = {
      groupId: groupDecoder.readUVarIntString(),
      topics: groupDecoder.readUVarIntArray((topicDecoder) => {
        const topic = {
          topic: topicDecoder.readUVarIntString(),
          partitions: topicDecoder.readUVarIntArray((partitionDecoder) =>
            partitionDecoder.readInt32()
          ),
        }
        topicDecoder.readTaggedFields()
        return topic
      }),
    }
    groupDecoder.readTaggedFields()
    return group
  })

describe('Protocol > Requests > OffsetFetch > v8', () => {
  test('sends exactly one group, carrying the caller topics list', async () => {
    const topics = [{ topic: 'test-topic', partitions: [{ partition: 0 }, { partition: 1 }] }]
    const { buffer } = await RequestV8Protocol({ groupId: 'test-group', topics }).encode()
    const decoder = new Decoder(buffer)

    const groups = decodeGroups(decoder)
    expect(groups).toEqual([
      {
        groupId: 'test-group',
        topics: [{ topic: 'test-topic', partitions: [0, 1] }],
      },
    ])
    expect(decoder.readBoolean()).toEqual(false)
    expect(decoder.readTaggedFields().size).toEqual(0)
  })

  test('encodes a null topics list as the null compact array (fetch every topic)', async () => {
    const { buffer } = await RequestV8Protocol({ groupId: 'test-group', topics: null }).encode()
    const decoder = new Decoder(buffer)

    const groups = decoder.readUVarIntArray((groupDecoder) => {
      const group = {
        groupId: groupDecoder.readUVarIntString(),
        topics: groupDecoder.readUVarIntArray(() => {}),
      }
      groupDecoder.readTaggedFields()
      return group
    })
    expect(groups).toEqual([{ groupId: 'test-group', topics: null }])
  })
})
