const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/deleteAclsResponse-v3-${caseId}.json`)

describe('Protocol > Requests > DeleteAcls > v3 response', () => {
  test.each(['basic', 'userResource'])('decodes the Apache 4.3.1 %s vector', async (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      filterResponses: fields.filterResponses,
    })
  })

  test('reads the USER resource type, 7, and parse resolves the response', async () => {
    const data = await decode(Buffer.from(vector('userResource').hex, 'hex'))

    expect(data.filterResponses[0].matchingAcls[0]).toMatchObject({ resourceType: 7 })
    await expect(parse(data)).resolves.toBe(data)
  })

  test('parse throws the filter error of a hand-built v3 body before a matching ACL error', async () => {
    const matchingAcl = (errorCode) =>
      new Encoder()
        .writeInt16(errorCode)
        .writeUVarIntString(null)
        .writeInt8(7)
        .writeUVarIntString('User:bob')
        .writeInt8(3)
        .writeUVarIntString('User:alice')
        .writeUVarIntString('*')
        .writeInt8(3)
        .writeInt8(3)
        .writeTaggedFields()
    const filterResult = (errorCode, matchingAcls) =>
      new Encoder()
        .writeInt16(errorCode)
        .writeUVarIntString(null)
        .writeUVarIntArray(matchingAcls)
        .writeTaggedFields()
    const body = new Encoder()
      .writeInt32(25)
      .writeUVarIntArray([filterResult(0, [matchingAcl(42)]), filterResult(31, [])])
      .writeTaggedFields().buffer
    const data = await decode(body)

    expect(data.filterResponses.map(({ errorCode }) => errorCode)).toEqual([0, 31])
    await expect(parse(data)).rejects.toMatchObject({
      name: 'KafkaJSProtocolError',
      type: 'CLUSTER_AUTHORIZATION_FAILED',
      code: 31,
    })
  })
})
