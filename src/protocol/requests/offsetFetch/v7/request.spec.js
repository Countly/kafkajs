const RequestV7Protocol = require('./request')
const Decoder = require('../../../decoder')
const { protocol } = require('../index')

const CASES = ['requireStable', 'notRequireStable']
const vector = (caseId) => require(`../fixtures/java/offsetFetchRequest-v7-${caseId}.json`)

/**
 * Reads a v7 request body back into the builder's arguments: the other direction of the byte check.
 * @param {Buffer} buffer
 * @returns {{ groupId: string, topics: object[]|null, requireStable: boolean }}
 */
const decodeRequest = (buffer) => {
  const decoder = new Decoder(buffer)
  const fields = {
    groupId: decoder.readUVarIntString(),
    topics: decoder.readUVarIntArray((topic) => {
      const entry = {
        topic: topic.readUVarIntString(),
        partitions: topic.readUVarIntArray((partition) => ({ partition: partition.readInt32() })),
      }
      expect(topic.readTaggedFields().size).toEqual(0)
      return entry
    }),
    requireStable: decoder.readBoolean(),
  }
  expect(decoder.readTaggedFields().size).toEqual(0)
  expect(decoder.canReadBytes(1)).toBe(false)
  return fields
}

describe('Protocol > Requests > OffsetFetch > v7', () => {
  test('sends RequireStable as false', async () => {
    const { buffer } = await RequestV7Protocol({ groupId: 'g', topics: [] }).encode()
    const decoder = new Decoder(buffer)

    decoder.readUVarIntString() // groupId
    decoder.readUVarIntArray(() => {}) // topics (null: empty list means "fetch all")
    expect(decoder.readBoolean()).toEqual(false)
    expect(decoder.readTaggedFields().size).toEqual(0)
  })

  test('encodes byte-identically to the Apache kafka-clients 4.3.1 codec', async () => {
    const { buffer } = await RequestV7Protocol({
      groupId: 'test-group',
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0 }] }],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v7_request_java.json')))
  })

  test.each(CASES)('encodes the Apache 4.3.1 %s vector byte for byte', async (caseId) => {
    const { hex, fields } = vector(caseId)
    const { buffer } = await RequestV7Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test.each(CASES)('the %s vector decodes back to the values it was built from', (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(decodeRequest(Buffer.from(hex, 'hex'))).toEqual(fields)
  })

  test('the version map passes requireStable through', async () => {
    const { hex, fields } = vector('requireStable')
    const { request } = protocol({ version: 7 })(fields)

    expect(request).toMatchObject({ apiName: 'OffsetFetch', apiVersion: 7 })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })

  test('through the version map, a caller that does not name it sends false', async () => {
    const { hex, fields } = vector('notRequireStable')
    const { request } = protocol({ version: 7 })({ groupId: fields.groupId, topics: fields.topics })

    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })

  test('below v7 the version map drops it: that body has no field for it', async () => {
    const { fields } = vector('requireStable')
    const asked = protocol({ version: 6 })(fields).request
    const notAsked = protocol({ version: 6 })({
      groupId: fields.groupId,
      topics: fields.topics,
    }).request

    expect((await asked.encode()).buffer).toEqual((await notAsked.encode()).buffer)
  })
})
