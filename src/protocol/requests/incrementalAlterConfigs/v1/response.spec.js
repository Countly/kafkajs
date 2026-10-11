const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) =>
  require(`../fixtures/java/incrementalAlterConfigsResponse-v1-${caseId}.json`)

describe('Protocol > Requests > IncrementalAlterConfigs > v1 response', () => {
  test.each(['ok', 'error'])('decodes the Apache 4.3.1 %s vector', async (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      resources: fields.resources,
    })
  })

  test('parse resolves every resource result and never throws for one', async () => {
    const data = await decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(parse(data)).resolves.toBe(data)
    expect(data.resources.map(({ errorCode }) => errorCode)).toEqual([0, 40])
  })

  test('keeps the decoded keys of v0', async () => {
    const v0 = await require('../v0/response').decode(
      Buffer.from(
        require('../fixtures/java/incrementalAlterConfigsResponse-v0-error.json').hex,
        'hex'
      )
    )
    const v1 = await decode(Buffer.from(vector('error').hex, 'hex'))

    expect(Object.keys(v1).sort()).toEqual(Object.keys(v0).sort())
    expect(v1.resources[1]).toEqual(v0.resources[1])
  })

  test('rejects a truncated body with a typed error', async () => {
    const { hex } = vector('ok')

    await expect(decode(Buffer.from(hex.slice(0, -8), 'hex'))).rejects.toMatchObject({
      name: 'KafkaJSPartialMessageError',
    })
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
