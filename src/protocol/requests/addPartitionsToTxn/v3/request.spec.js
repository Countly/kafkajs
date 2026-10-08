const Decoder = require('../../../decoder')
const RequestV3Protocol = require('./request')
const { protocol, versions } = require('../index')

const vector = () => require('../fixtures/java/addPartitionsToTxnRequest-v3-basic.json')
const responseVector = (caseId) =>
  require(`../fixtures/java/addPartitionsToTxnResponse-v3-${caseId}.json`)

/**
 * Reads a v3 request body back into the builder's arguments: compact strings and arrays, and a tag
 * buffer closing each topic and the request.
 * @param {Buffer} buffer
 * @returns {{ transactionalId: string, producerId: string, producerEpoch: number, topics: { topic: string, partitions: number[] }[] }}
 */
const decodeRequest = (buffer) => {
  const decoder = new Decoder(buffer)
  const fields = {
    transactionalId: decoder.readUVarIntString(),
    producerId: decoder.readInt64().toString(),
    producerEpoch: decoder.readInt16(),
    topics: decoder.readUVarIntArray((topic) => {
      const entry = {
        topic: topic.readUVarIntString(),
        partitions: topic.readUVarIntArray((partition) => partition.readInt32()),
      }
      expect(topic.readTaggedFields().size).toEqual(0)
      return entry
    }),
  }
  expect(decoder.readTaggedFields().size).toEqual(0)
  expect(decoder.canReadBytes(1)).toBe(false)
  return fields
}

describe('Protocol > Requests > AddPartitionsToTxn > v3', () => {
  test('encodes the Apache 4.3.1 vector byte for byte', async () => {
    const { hex, fields } = vector()
    const { buffer } = await RequestV3Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('the vector decodes back to the values it was built from', () => {
    const { hex, fields } = vector()

    expect(decodeRequest(Buffer.from(hex, 'hex'))).toEqual(fields)
  })

  test('announces version 3 and is reachable through the version map', async () => {
    const { hex, fields } = vector()
    const { request } = protocol({ version: 3 })(fields)

    expect(request).toMatchObject({ apiName: 'AddPartitionsToTxn', apiVersion: 3 })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })

  test('decodes and parses its response through the version map', async () => {
    const { fields } = vector()
    const { response } = protocol({ version: 3 })(fields)
    const ok = responseVector('ok')
    const failed = responseVector('error')

    expect(await response.decode(Buffer.from(ok.hex, 'hex'))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: ok.fields.throttleTime,
      errors: ok.fields.errors,
    })
    await expect(
      response.parse(await response.decode(Buffer.from(failed.hex, 'hex')))
    ).rejects.toMatchObject({ type: 'CONCURRENT_TRANSACTIONS', code: 51 })
  })

  test('stops at v3: the protocol keeps v4 and above for brokers', () => {
    expect(versions).toEqual(['0', '1', '2', '3'])
  })
})
