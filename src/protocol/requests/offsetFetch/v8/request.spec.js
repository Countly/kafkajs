const RequestV8Protocol = require('./request')
const Decoder = require('../../../decoder')
const { protocol } = require('../index')

const CASES = ['requireStable', 'notRequireStable']
const vector = (caseId) => require(`../fixtures/java/offsetFetchRequest-v8-${caseId}.json`)

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

/**
 * Reads a v8 request body back into the builder's arguments: the one group's id and topics, then
 * the request's `RequireStable` flag.
 * @param {Buffer} buffer
 * @returns {{ groupId: string, topics: object[]|null, requireStable: boolean }}
 */
const decodeRequest = (buffer) => {
  const decoder = new Decoder(buffer)
  const groups = decodeGroups(decoder)
  const requireStable = decoder.readBoolean()
  expect(decoder.readTaggedFields().size).toEqual(0)
  expect(decoder.canReadBytes(1)).toBe(false)
  expect(groups).toHaveLength(1)

  const [{ groupId, topics }] = groups
  return {
    groupId,
    topics:
      topics &&
      topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map((partition) => ({ partition })),
      })),
    requireStable,
  }
}

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

  test('encodes byte-identically to the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV8Protocol({
      groupId: 'test-group',
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0 }] }],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v8_request_java.json')))
  })

  test.each([
    ['4.3.1', '../fixtures/v8_request_kafka431.json'],
    ['4.1.2', '../fixtures/v8_request_kafka412.json'],
  ])('is the request a real Kafka %s cluster answered', (_version, fixture) => {
    const decoder = new Decoder(Buffer.from(require(fixture)))
    const groups = decodeGroups(decoder)

    expect(groups).toHaveLength(1)
    expect(groups[0].groupId).toEqual('kafkajs-capture-group')
    expect(groups[0].topics[0].topic).toEqual('kafkajs-capture-topic')
    expect(groups[0].topics[0].partitions.length).toBeGreaterThan(0)
    expect(decoder.readBoolean()).toEqual(false) // requireStable
    expect(decoder.readTaggedFields().size).toEqual(0)
    expect(decoder.canReadBytes(1)).toBe(false)
  })

  test.each(CASES)('encodes the Apache 4.3.1 %s vector byte for byte', async (caseId) => {
    const { hex, fields } = vector(caseId)
    const { buffer } = await RequestV8Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test.each(CASES)('the %s vector decodes back to the values it was built from', (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(decodeRequest(Buffer.from(hex, 'hex'))).toEqual(fields)
  })

  test('the version map passes requireStable through', async () => {
    const { hex, fields } = vector('requireStable')
    const { request } = protocol({ version: 8 })(fields)

    expect(request).toMatchObject({ apiName: 'OffsetFetch', apiVersion: 8 })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })

  test('through the version map, a caller that does not name it sends false', async () => {
    const { hex, fields } = vector('notRequireStable')
    const { request } = protocol({ version: 8 })({ groupId: fields.groupId, topics: fields.topics })

    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
