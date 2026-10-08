const RequestV4Protocol = require('./request')
const { protocol, versions } = require('../index')

const vector = (version) =>
  require(`../fixtures/java/addOffsetsToTxnRequest-v${version}-basic.json`)
const responseVector = (version, caseId) =>
  require(`../fixtures/java/addOffsetsToTxnResponse-v${version}-${caseId}.json`)

describe('Protocol > Requests > AddOffsetsToTxn > v4', () => {
  test('encodes the Apache 4.3.1 vector byte for byte', async () => {
    const { hex, fields } = vector(4)
    const { buffer } = await RequestV4Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('is the v3 body: v4 only lets the broker answer TRANSACTION_ABORTABLE', async () => {
    const { hex, fields } = vector(4)
    const v3 = await require('../v3/request')(fields).encode()

    expect(v3.buffer.toString('hex')).toEqual(hex)
    expect(vector(3).hex).toEqual(hex)
  })

  test('announces version 4 and is reachable through the version map', async () => {
    const { hex, fields } = vector(4)
    const { request } = protocol({ version: 4 })(fields)

    expect(request).toMatchObject({ apiName: 'AddOffsetsToTxn', apiVersion: 4 })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
    expect(versions).toEqual(['0', '1', '2', '3', '4'])
  })

  test('decodes and parses its response through the version map', async () => {
    const { fields } = vector(4)
    const { response } = protocol({ version: 4 })(fields)
    const ok = responseVector(4, 'ok')
    const failed = responseVector(4, 'error')

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
