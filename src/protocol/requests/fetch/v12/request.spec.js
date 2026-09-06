const RequestV12Protocol = require('./request')
const Decoder = require('../../../decoder')

describe('Protocol > Requests > Fetch > v12', () => {
  const build = (overrides = {}) =>
    RequestV12Protocol({
      replicaId: -1,
      maxWaitTime: 5000,
      minBytes: 1,
      maxBytes: 10485760,
      isolationLevel: 1,
      sessionId: 0,
      sessionEpoch: -1,
      forgottenTopics: [],
      rackId: '',
      topics: [
        {
          topic: 'test-topic',
          partitions: [
            {
              partition: 0,
              currentLeaderEpoch: 5,
              fetchOffset: '42',
              logStartOffset: -1,
              maxBytes: 1048576,
            },
          ],
        },
      ],
      ...overrides,
    })

  test('encodes the flexible request', async () => {
    const { buffer } = await build().encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v12_request.json')))
    expect(build().apiVersion).toEqual(12)
  })

  test('sends LastFetchedEpoch at its -1 default and omits the ClusterId tag', async () => {
    const { buffer } = await build().encode()
    const decoder = new Decoder(buffer)

    expect(decoder.readInt32()).toEqual(-1) // replicaId
    decoder.readInt32() // maxWaitMs
    decoder.readInt32() // minBytes
    decoder.readInt32() // maxBytes
    decoder.readInt8() //  isolationLevel
    expect(decoder.readInt32()).toEqual(0) //  sessionId
    expect(decoder.readInt32()).toEqual(-1) // sessionEpoch

    const topics = decoder.readUVarIntArray((d) => {
      const topic = {
        topic: d.readUVarIntString(),
        partitions: d.readUVarIntArray((p) => {
          const partition = {
            partition: p.readInt32(),
            currentLeaderEpoch: p.readInt32(),
            fetchOffset: p.readInt64().toString(),
            lastFetchedEpoch: p.readInt32(),
            logStartOffset: p.readInt64().toString(),
            maxBytes: p.readInt32(),
          }
          expect(p.readTaggedFields().size).toEqual(0)
          return partition
        }),
      }
      expect(d.readTaggedFields().size).toEqual(0)
      return topic
    })

    expect(topics).toEqual([
      {
        topic: 'test-topic',
        partitions: [
          {
            partition: 0,
            currentLeaderEpoch: 5,
            fetchOffset: '42',
            lastFetchedEpoch: -1,
            logStartOffset: '-1',
            maxBytes: 1048576,
          },
        ],
      },
    ])

    // The two neighbouring bytes that are easiest to get wrong: an empty compact array and an
    // empty compact string are both a single 0x01, and the body tag buffer is a single 0x00.
    expect(decoder.buffer[decoder.offset]).toEqual(0x01) // forgottenTopicsData
    expect(decoder.readUVarIntArray(() => {})).toEqual([])
    expect(decoder.buffer[decoder.offset]).toEqual(0x01) // rackId
    expect(decoder.readUVarIntString()).toEqual('')
    expect(decoder.buffer[decoder.offset]).toEqual(0x00) // body tag buffer, no ClusterId
    expect(decoder.readTaggedFields().size).toEqual(0)
    expect(decoder.canReadBytes(1)).toBe(false)
  })
})
