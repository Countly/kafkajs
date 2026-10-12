const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/describeAclsResponse-v3-${caseId}.json`)

describe('Protocol > Requests > DescribeAcls > v3 response', () => {
  test.each(['basic', 'userResource'])('decodes the Apache 4.3.1 %s vector', async (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      errorCode: fields.errorCode,
      errorMessage: fields.errorMessage,
      resources: fields.resources,
    })
  })

  test('reads the USER resource type, 7, and parse resolves the response', async () => {
    const data = await decode(Buffer.from(vector('userResource').hex, 'hex'))

    expect(data.resources[0]).toMatchObject({ resourceType: 7, resourceName: 'User:bob' })
    await expect(parse(data)).resolves.toBe(data)
  })

  test('parse throws the top-level error of a hand-built v3 body', async () => {
    const body = new Encoder()
      .writeInt32(25)
      .writeInt16(31)
      .writeUVarIntString('Cluster authorization failed.')
      .writeUVarIntArray([])
      .writeTaggedFields().buffer
    const data = await decode(body)

    expect(data).toMatchObject({ errorCode: 31, errorMessage: 'Cluster authorization failed.' })
    await expect(parse(data)).rejects.toMatchObject({
      name: 'KafkaJSProtocolError',
      type: 'CLUSTER_AUTHORIZATION_FAILED',
      code: 31,
    })
  })
})
