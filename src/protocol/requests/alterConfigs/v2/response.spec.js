const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/alterConfigsResponse-v2-${caseId}.json`)

describe('Protocol > Requests > AlterConfigs > v2 response', () => {
  test.each(['ok', 'error'])('decodes the Apache 4.3.1 %s vector', async (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      resources: fields.resources,
    })
  })

  test('parse resolves a clean response and throws the first failed resource as v0 does', async () => {
    const clean = await decode(Buffer.from(vector('ok').hex, 'hex'))
    const failed = await decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(parse(clean)).resolves.toBe(clean)
    await expect(parse(failed)).rejects.toMatchObject({ type: 'INVALID_REQUEST', code: 42 })
  })

  test('keeps an empty message and an empty name as empty strings, never as null', async () => {
    const body = new Encoder()
      .writeInt32(25)
      .writeUVarIntArray([
        new Encoder()
          .writeInt16(0)
          .writeUVarIntString('')
          .writeInt8(4)
          .writeUVarIntString('')
          .writeTaggedFields(),
      ])
      .writeTaggedFields().buffer

    expect(await decode(body)).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 25,
      resources: [{ errorCode: 0, errorMessage: '', resourceType: 4, resourceName: '' }],
    })
  })
})
