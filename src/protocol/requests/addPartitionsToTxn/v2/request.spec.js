const Decoder = require('../../../decoder')
const RequestV2Protocol = require('./request')
const { protocol } = require('../index')

const vector = () => require('../fixtures/java/addPartitionsToTxnRequest-v2-basic.json')
const responseVector = (caseId) =>
  require(`../fixtures/java/addPartitionsToTxnResponse-v2-${caseId}.json`)

/**
 * Reads a v2 request body back into the builder's arguments: the other direction of the byte check.
 * @param {Buffer} buffer
 * @returns {{ transactionalId: string, producerId: string, producerEpoch: number, topics: { topic: string, partitions: number[] }[] }}
 */
const decodeRequest = (buffer) => {
  const decoder = new Decoder(buffer)
  const fields = {
    transactionalId: decoder.readString(),
    producerId: decoder.readInt64().toString(),
    producerEpoch: decoder.readInt16(),
    topics: decoder.readArray((topic) => ({
      topic: topic.readString(),
      partitions: topic.readArray((partition) => partition.readInt32()),
    })),
  }
  expect(decoder.canReadBytes(1)).toBe(false)
  return fields
}

describe('Protocol > Requests > AddPartitionsToTxn > v2', () => {
  test('encodes the Apache 4.3.1 vector byte for byte', async () => {
    const { hex, fields } = vector()
    const { buffer } = await RequestV2Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('the vector decodes back to the values it was built from', () => {
    const { hex, fields } = vector()

    expect(decodeRequest(Buffer.from(hex, 'hex'))).toEqual(fields)
  })

  test('is the v1 body: v2 only lets the broker answer PRODUCER_FENCED', async () => {
    const { hex, fields } = vector()
    const v1 = await require('../v1/request')(fields).encode()

    expect(v1.buffer.toString('hex')).toEqual(hex)
  })

  test('announces version 2 and is reachable through the version map', async () => {
    const { hex, fields } = vector()
    const { request } = protocol({ version: 2 })(fields)

    expect(request).toMatchObject({ apiName: 'AddPartitionsToTxn', apiVersion: 2 })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })

  test('decodes and parses its response through the version map', async () => {
    const { fields } = vector()
    const { response } = protocol({ version: 2 })(fields)
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
})
