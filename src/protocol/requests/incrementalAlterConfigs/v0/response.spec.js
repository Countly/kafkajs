const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) =>
  require(`../fixtures/java/incrementalAlterConfigsResponse-v0-${caseId}.json`)

describe('Protocol > Requests > IncrementalAlterConfigs > v0 response', () => {
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
    expect(data.resources[1].errorMessage).toEqual(vector('error').fields.resources[1].errorMessage)
  })

  test('keeps an empty message and an empty name as empty strings, never as null', async () => {
    const body = new Encoder()
      .writeInt32(25)
      .writeArray([new Encoder().writeInt16(0).writeString('').writeInt8(4).writeString('')]).buffer

    expect(await decode(body)).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 25,
      resources: [{ errorCode: 0, errorMessage: '', resourceType: 4, resourceName: '' }],
    })
  })
})
