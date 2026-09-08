const RequestV1Protocol = require('./request')
const Decoder = require('../../../decoder')

const vector = (caseId) =>
  require(`../fixtures/java/consumerGroupHeartbeatRequest-v1-${caseId}.json`)

/**
 * Decodes a v1 request body with a hand-written reader, so the assertion is about the bytes and
 * not about this repo's own encoder agreeing with itself.
 * @param {Buffer} buffer
 * @returns {object} the decoded fields, in the fixture's own shape
 */
const decodeRequest = (buffer) => {
  const decoder = new Decoder(buffer)
  const fields = {
    groupId: decoder.readUVarIntString(),
    memberId: decoder.readUVarIntString(),
    memberEpoch: decoder.readInt32(),
    instanceId: decoder.readUVarIntString(),
    rackId: decoder.readUVarIntString(),
    rebalanceTimeoutMs: decoder.readInt32(),
    subscribedTopicNames: decoder.readUVarIntArray((d) => d.readUVarIntString()),
    subscribedTopicRegex: decoder.readUVarIntString(),
    serverAssignor: decoder.readUVarIntString(),
    topicPartitions: decoder.readUVarIntArray((d) => {
      const entry = { topicId: d.readUuid(), partitions: d.readUVarIntArray((p) => p.readInt32()) }
      d.readTaggedFields()
      return entry
    }),
  }

  decoder.readTaggedFields()
  expect(decoder.canReadBytes(1)).toBe(false)
  return fields
}

describe('Protocol > Requests > ConsumerGroupHeartbeat > v1', () => {
  test.each(['join', 'regexSubscription', 'regexClear'])(
    'encodes the %s vector byte-identically to the Apache 4.3.1 codec',
    async (caseId) => {
      const { hex, fields } = vector(caseId)
      const { buffer } = await RequestV1Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test.each(['join', 'regexSubscription', 'regexClear'])(
    'the %s vector decodes back to the values it was built from',
    (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(decodeRequest(Buffer.from(hex, 'hex'))).toEqual(fields)
    }
  )

  test('sends the regex with an EMPTY name list on a joining heartbeat', async () => {
    const { hex, fields } = vector('regexSubscription')
    const { buffer } = await RequestV1Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
    expect(decodeRequest(buffer).subscribedTopicNames).toEqual([])
    expect(decodeRequest(buffer).subscribedTopicRegex).toEqual('^test-.*')
  })

  test('clears a regex with the empty string, which is not null', async () => {
    const { hex, fields } = vector('regexClear')
    const { buffer } = await RequestV1Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
    expect(decodeRequest(buffer).subscribedTopicRegex).toEqual('')
    expect(
      decodeRequest(
        (await RequestV1Protocol({ ...fields, subscribedTopicRegex: null }).encode()).buffer
      ).subscribedTopicRegex
    ).toBeNull()
  })

  test('differs from v0 by the regex field and nothing else', async () => {
    // v1 adds exactly one field, in the middle of the body. A length comparison alone would pass
    // for any one-byte change anywhere, so this splices the field out of the v1 bytes and asserts
    // the remainder IS the v0 frame - which fails on a reordering, a widened field or a moved tag
    // buffer, while both version-golden suites still pass.
    const fields = vector('join').fields
    const v1 = (await RequestV1Protocol(fields).encode()).buffer
    const v0 = (await require('../v0/request')(fields).encode()).buffer

    let i = 0
    while (i < Math.min(v1.length, v0.length) && v1[i] === v0[i]) i++

    // A null compact string is the single byte 0x00: `subscribedTopicRegex` is null on this join.
    expect(v1[i]).toEqual(0x00)
    expect(Buffer.concat([v1.subarray(0, i), v1.subarray(i + 1)])).toEqual(v0)
  })

  test('encodes owned partitions at v1, the shape an acknowledging heartbeat sends', async () => {
    // No committed v1 vector carries owned partitions, so the expectation is built from the v0
    // golden that does. v1's body is v0's with one field inserted between the topic-name list and
    // `ServerAssignor`, and a null compact string is the single byte 0x00 - so the expected v1
    // frame is the v0 golden with `00` spliced in at that offset. The v0 layout, from the message
    // JSON, puts that offset at byte 34:
    //   0b + 10  GroupId "test-group"        [0,11)
    //   0c + 11  MemberId "test-member"      [11,23)
    //   00000005 MemberEpoch                 [23,27)
    //   00       InstanceId null             [27,28)
    //   00       RackId null                 [28,29)
    //   ffffffff RebalanceTimeoutMs -1       [29,33)
    //   00       SubscribedTopicNames null   [33,34)
    //   <- SubscribedTopicRegex null (00) is inserted here at v1
    //   00       ServerAssignor null
    //   02       TopicPartitions, one entry: 0102..0f10 uuid, 03 = two partitions, 0 and 2
    const SUBSCRIBED_TOPIC_REGEX_OFFSET = 34
    const owned = require('../fixtures/java/consumerGroupHeartbeatRequest-v0-ownedPartitions.json')
    const { fields } = owned
    const v0 = Buffer.from(owned.hex, 'hex')
    const expected = Buffer.concat([
      v0.subarray(0, SUBSCRIBED_TOPIC_REGEX_OFFSET),
      Buffer.from([0x00]),
      v0.subarray(SUBSCRIBED_TOPIC_REGEX_OFFSET),
    ])

    const { buffer } = await RequestV1Protocol(fields).encode()

    expect(buffer).toEqual(expected)
    expect(decodeRequest(buffer)).toEqual({ ...fields, subscribedTopicRegex: null })
    expect(decodeRequest(buffer).topicPartitions).toEqual([
      { topicId: '01020304-0506-0708-090a-0b0c0d0e0f10', partitions: [0, 2] },
    ])
  })

  test('takes every send-if-changed default when the caller omits a field', async () => {
    const { buffer } = await RequestV1Protocol({
      groupId: 'g',
      memberId: 'm',
      memberEpoch: 7,
    }).encode()

    expect(decodeRequest(buffer)).toEqual({
      groupId: 'g',
      memberId: 'm',
      memberEpoch: 7,
      instanceId: null,
      rackId: null,
      rebalanceTimeoutMs: -1,
      subscribedTopicNames: null,
      subscribedTopicRegex: null,
      serverAssignor: null,
      topicPartitions: null,
    })
  })
})
