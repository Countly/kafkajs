const RequestV9Protocol = require('./request')
const Decoder = require('../../../decoder')

const vector = (caseId) => require(`../fixtures/java/offsetFetchRequest-v9-${caseId}.json`)

/**
 * Reads a v9 request body back: one group carrying the two new fields, then the topics array and
 * the request's own `RequireStable` flag.
 * @param {Buffer} buffer
 * @returns {{ groups: object[], requireStable: boolean }}
 */
const decodeRequest = (buffer) => {
  const decoder = new Decoder(buffer)
  const groups = decoder.readUVarIntArray((group) => {
    const entry = {
      groupId: group.readUVarIntString(),
      memberId: group.readUVarIntString(),
      memberEpoch: group.readInt32(),
      topics: group.readUVarIntArray((topic) => {
        const read = {
          topic: topic.readUVarIntString(),
          partitions: topic.readUVarIntArray((p) => ({ partition: p.readInt32() })),
        }
        topic.readTaggedFields()
        return read
      }),
    }
    group.readTaggedFields()
    return entry
  })

  const requireStable = decoder.readBoolean()
  decoder.readTaggedFields()
  expect(decoder.canReadBytes(1)).toBe(false)
  return { groups, requireStable }
}

describe('Protocol > Requests > OffsetFetch > v9', () => {
  test.each(['memberFields', 'nullMember'])(
    'encodes the %s vector byte-identically to the Apache 4.3.1 codec',
    async (caseId) => {
      const { hex, fields } = vector(caseId)
      const { buffer } = await RequestV9Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test('announces itself as version 9', () => {
    const request = RequestV9Protocol({ groupId: 'g', topics: [] })

    expect(request.apiVersion).toEqual(9)
    expect(request.apiName).toEqual('OffsetFetch')
  })

  test('defaults to the wire defaults, which is what a classic or admin caller means', async () => {
    // null member id and -1 epoch tell the coordinator "no consumer-protocol membership here",
    // which is exactly what v8 conveyed by not having the fields at all.
    const { buffer } = await RequestV9Protocol({ groupId: 'g', topics: [] }).encode()
    const decoded = decodeRequest(buffer)

    expect(decoded.groups[0].memberId).toBeNull()
    expect(decoded.groups[0].memberEpoch).toEqual(-1)
  })

  test('adds exactly five bytes to the v8 body: a null compact string and an INT32', async () => {
    const v9 = await RequestV9Protocol({ groupId: 'g', topics: [] }).encode()
    const v8 = await require('../v8/request')({ groupId: 'g', topics: [] }).encode()

    expect(v9.buffer.length).toEqual(v8.buffer.length + 5)
  })

  test('sends the member id and epoch when the caller has a membership', async () => {
    const { fields } = vector('memberFields')
    const { buffer } = await RequestV9Protocol(fields).encode()
    const decoded = decodeRequest(buffer)

    expect(decoded.groups[0]).toMatchObject({
      groupId: 'test-group',
      memberId: 'test-member',
      memberEpoch: 5,
    })
    expect(decoded.groups[0].topics).toEqual([
      { topic: 'test-topic', partitions: [{ partition: 0 }] },
    ])
    // This client has never asked the broker to hold back unstable offsets, at any version.
    expect(decoded.requireStable).toBe(false)
  })

  test('asks for every topic when the topic list is empty or absent', async () => {
    // A null Topics array means "all topics in the group" and is what `nullMember` carries; an
    // empty JavaScript list has to mean the same thing, because that is what the v8 codec did.
    const fromEmpty = await RequestV9Protocol({ groupId: 'test-group', topics: [] }).encode()
    const fromNull = await RequestV9Protocol({ groupId: 'test-group', topics: null }).encode()

    expect(fromEmpty.buffer).toEqual(fromNull.buffer)
    expect(decodeRequest(fromEmpty.buffer).groups[0].topics).toBeNull()
  })

  test('sends every partition of every requested topic', async () => {
    const { buffer } = await RequestV9Protocol({
      groupId: 'test-group',
      memberId: 'test-member',
      memberEpoch: 0,
      topics: [
        { topic: 'topic-a', partitions: [{ partition: 0 }, { partition: 3 }] },
        { topic: 'topic-b', partitions: [{ partition: 1 }] },
      ],
    }).encode()
    const decoded = decodeRequest(buffer)

    expect(decoded.groups[0].memberEpoch).toEqual(0)
    expect(decoded.groups[0].topics).toEqual([
      { topic: 'topic-a', partitions: [{ partition: 0 }, { partition: 3 }] },
      { topic: 'topic-b', partitions: [{ partition: 1 }] },
    ])
  })
})
