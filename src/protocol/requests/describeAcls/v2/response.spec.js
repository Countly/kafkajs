const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/describeAclsResponse-v2-${caseId}.json`)

describe('Protocol > Requests > DescribeAcls > v2 response', () => {
  test.each(['basic'])('decodes the Apache 4.3.1 %s vector', async (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      errorCode: fields.errorCode,
      errorMessage: fields.errorMessage,
      resources: fields.resources,
    })
  })

  test('parse resolves a clean response', async () => {
    const data = await decode(Buffer.from(vector('basic').hex, 'hex'))

    await expect(parse(data)).resolves.toBe(data)
  })

  test('decodes a top-level error with its message, and parse throws it', async () => {
    const body = new Encoder()
      .writeInt32(25)
      .writeInt16(31)
      .writeUVarIntString('Cluster authorization failed.')
      .writeUVarIntArray([])
      .writeTaggedFields().buffer
    const data = await decode(body)

    expect(data).toMatchObject({
      errorCode: 31,
      errorMessage: 'Cluster authorization failed.',
      resources: [],
    })
    await expect(parse(data)).rejects.toMatchObject({ type: 'CLUSTER_AUTHORIZATION_FAILED' })
  })

  test('keeps an empty message, name, principal and host as empty strings, never as null', async () => {
    const body = new Encoder()
      .writeInt32(25)
      .writeInt16(0)
      .writeUVarIntString('')
      .writeUVarIntArray([
        new Encoder()
          .writeInt8(2)
          .writeUVarIntString('')
          .writeInt8(3)
          .writeUVarIntArray([
            new Encoder()
              .writeUVarIntString('')
              .writeUVarIntString('')
              .writeInt8(3)
              .writeInt8(3)
              .writeTaggedFields(),
          ])
          .writeTaggedFields(),
      ])
      .writeTaggedFields().buffer

    expect(await decode(body)).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 25,
      errorCode: 0,
      errorMessage: '',
      resources: [
        {
          resourceType: 2,
          resourceName: '',
          resourcePatternType: 3,
          acls: [{ principal: '', host: '', operation: 3, permissionType: 3 }],
        },
      ],
    })
  })
})
