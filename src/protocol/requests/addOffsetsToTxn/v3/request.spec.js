const Decoder = require('../../../decoder')
const RequestV3Protocol = require('./request')
const { protocol } = require('../index')

const vector = () => require('../fixtures/java/addOffsetsToTxnRequest-v3-basic.json')
const responseVector = (version, caseId) =>
  require(`../fixtures/java/addOffsetsToTxnResponse-v${version}-${caseId}.json`)

/**
 * Reads a v3 request body back into the builder's arguments: compact strings and a closing tag
 * buffer.
 * @param {Buffer} buffer
 * @returns {{ transactionalId: string, producerId: string, producerEpoch: number, groupId: string }}
 */
const decodeRequest = (buffer) => {
  const decoder = new Decoder(buffer)
  const fields = {
    transactionalId: decoder.readUVarIntString(),
    producerId: decoder.readInt64().toString(),
    producerEpoch: decoder.readInt16(),
    groupId: decoder.readUVarIntString(),
  }
  expect(decoder.readTaggedFields().size).toEqual(0)
  expect(decoder.canReadBytes(1)).toBe(false)
  return fields
}

describe('Protocol > Requests > AddOffsetsToTxn > v3', () => {
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

    expect(request).toMatchObject({ apiName: 'AddOffsetsToTxn', apiVersion: 3 })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })

  test('decodes and parses its response through the version map', async () => {
    const { fields } = vector()
    const { response } = protocol({ version: 3 })(fields)
    const ok = responseVector(3, 'ok')
    const failed = responseVector(3, 'error')

    expect(await response.decode(Buffer.from(ok.hex, 'hex'))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: ok.fields.throttleTime,
      errorCode: 0,
    })
    await expect(
      response.parse(await response.decode(Buffer.from(failed.hex, 'hex')))
    ).rejects.toMatchObject({ type: 'CONCURRENT_TRANSACTIONS', code: 51 })
    // A flexible body ends with a tag buffer, so the classic v2 bytes are a partial message here.
    await expect(
      response.decode(Buffer.from(responseVector(2, 'ok').hex, 'hex'))
    ).rejects.toMatchObject({ name: 'KafkaJSPartialMessageError' })
  })
})
