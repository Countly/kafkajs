const RequestV0Protocol = require('./request')
const Decoder = require('../../../decoder')

const vector = (caseId) =>
  require(`../fixtures/java/consumerGroupHeartbeatRequest-v0-${caseId}.json`)

/**
 * Decodes a v0 request body with a hand-written reader, so the assertion is about the bytes and
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

describe('Protocol > Requests > ConsumerGroupHeartbeat > v0', () => {
  test.each([
    'join',
    'stableMinimal',
    'ownedPartitions',
    'leaveDynamic',
    'leaveStatic',
    'staticJoinWithAssignor',
  ])('encodes the %s vector byte-identically to the Apache 4.3.1 codec', async (caseId) => {
    const { hex, fields } = vector(caseId)
    const { buffer } = await RequestV0Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test.each([
    'join',
    'stableMinimal',
    'ownedPartitions',
    'leaveDynamic',
    'leaveStatic',
    'staticJoinWithAssignor',
  ])('the %s vector decodes back to the values it was built from', (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(decodeRequest(Buffer.from(hex, 'hex'))).toEqual(fields)
  })

  test('keeps an empty subscription list distinct from an absent one', async () => {
    // null means "unchanged since the last heartbeat" (compact-array 0x00); [] means "no names"
    // (0x01). Coalescing the two would make a regex subscription say nothing at all.
    const base = { groupId: 'g', memberId: 'm', memberEpoch: 0 }
    const withEmpty = await RequestV0Protocol({ ...base, subscribedTopicNames: [] }).encode()
    const withNull = await RequestV0Protocol({ ...base, subscribedTopicNames: null }).encode()

    expect(withEmpty.buffer).not.toEqual(withNull.buffer)
    expect(withEmpty.buffer.length).toEqual(withNull.buffer.length)
  })

  test('keeps an empty owned-partition list distinct from an absent one', async () => {
    // The same distinction one field along, and it is the one the join heartbeat turns on: `[]`
    // says "I own nothing", null says "unchanged since the last heartbeat". The two committed
    // vectors carry the pair - `join` has `topicPartitions: []`, `stableMinimal` has null - but
    // they differ in four other fields too, so the isolating twin is built here.
    const base = { groupId: 'g', memberId: 'm', memberEpoch: 0 }
    const withEmpty = await RequestV0Protocol({ ...base, topicPartitions: [] }).encode()
    const withNull = await RequestV0Protocol({ ...base, topicPartitions: null }).encode()

    expect(withEmpty.buffer).not.toEqual(withNull.buffer)
    expect(withEmpty.buffer.length).toEqual(withNull.buffer.length)
    // And the committed pair agrees with the twin, one byte apart at the same offset.
    const join = Buffer.from(vector('join').hex, 'hex')
    const stable = Buffer.from(vector('stableMinimal').hex, 'hex')
    expect(join[join.length - 2]).toEqual(0x01)
    expect(stable[stable.length - 2]).toEqual(0x00)
  })

  // SF-18: `TopicPartitions.Partitions` is declared NON-nullable, and the encoder writes the
  // 0x00 null sentinel for a null array - which Kafka's generated reader rejects outright with
  // "non-nullable field Partitions was serialized as null", failing the whole heartbeat.
  test('never writes the non-nullable Partitions as the null compact array', async () => {
    const { fields } = vector('ownedPartitions')
    const topicId = fields.topicPartitions[0].topicId
    const base = { ...fields, topicPartitions: [{ topicId, partitions: [] }] }

    const { buffer: empty } = await RequestV0Protocol(base).encode()
    const { buffer: nulled } = await RequestV0Protocol({
      ...base,
      topicPartitions: [{ topicId, partitions: null }],
    }).encode()

    expect(nulled).toEqual(empty)
    expect(decodeRequest(nulled).topicPartitions).toEqual([{ topicId, partitions: [] }])
    // The Java vector for the same field with two partitions writes 0x03 (count + 1); the empty
    // array is 0x01, and the null sentinel this must never be is 0x00.
    expect(Buffer.from(vector('ownedPartitions').hex, 'hex')).toContain(0x03)
    expect(nulled).toContain(0x01)
    expect(nulled).not.toEqual(Buffer.from(vector('ownedPartitions').hex, 'hex'))
  })

  test('takes every send-if-changed default when the caller omits a field', async () => {
    const { buffer } = await RequestV0Protocol({
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
      serverAssignor: null,
      topicPartitions: null,
    })
  })
})
