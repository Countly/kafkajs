const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/deleteAclsResponse-v2-${caseId}.json`)

/**
 * Writes a response body of one filter result: its own code and message, then the matching ACLs,
 * each with a code of its own and the ACL of a topic.
 * @param {{ filterCode: number, matchCodes: number[] }} result
 * @returns {Buffer}
 */
const bodyOf = ({ filterCode, matchCodes }) =>
  new Encoder()
    .writeInt32(25)
    .writeUVarIntArray([
      new Encoder()
        .writeInt16(filterCode)
        .writeUVarIntString(null)
        .writeUVarIntArray(
          matchCodes.map((errorCode) =>
            new Encoder()
              .writeInt16(errorCode)
              .writeUVarIntString(null)
              .writeInt8(2)
              .writeUVarIntString('test-topic')
              .writeInt8(3)
              .writeUVarIntString('User:alice')
              .writeUVarIntString('*')
              .writeInt8(3)
              .writeInt8(3)
              .writeTaggedFields()
          )
        )
        .writeTaggedFields(),
    ])
    .writeTaggedFields().buffer

describe('Protocol > Requests > DeleteAcls > v2 response', () => {
  test.each(['basic'])('decodes the Apache 4.3.1 %s vector', async (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      filterResponses: fields.filterResponses,
    })
  })

  test('parse resolves a clean response', async () => {
    const data = await decode(Buffer.from(vector('basic').hex, 'hex'))

    await expect(parse(data)).resolves.toBe(data)
  })

  test('parse throws the error of a filter that failed', async () => {
    const data = await decode(bodyOf({ filterCode: 31, matchCodes: [] }))

    expect(data.filterResponses[0]).toMatchObject({ errorCode: 31, matchingAcls: [] })
    await expect(parse(data)).rejects.toMatchObject({ type: 'CLUSTER_AUTHORIZATION_FAILED' })
  })

  test('parse throws the error of a matching ACL that could not be deleted', async () => {
    const data = await decode(bodyOf({ filterCode: 0, matchCodes: [0, 42] }))

    expect(data.filterResponses[0].matchingAcls.map(({ errorCode }) => errorCode)).toEqual([0, 42])
    await expect(parse(data)).rejects.toMatchObject({ type: 'INVALID_REQUEST', code: 42 })
  })

  test('keeps every empty message, name, principal and host as an empty string, never as null', async () => {
    const body = new Encoder()
      .writeInt32(25)
      .writeUVarIntArray([
        new Encoder()
          .writeInt16(0)
          .writeUVarIntString('')
          .writeUVarIntArray([
            new Encoder()
              .writeInt16(0)
              .writeUVarIntString('')
              .writeInt8(2)
              .writeUVarIntString('')
              .writeInt8(3)
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
      filterResponses: [
        {
          errorCode: 0,
          errorMessage: '',
          matchingAcls: [
            {
              errorCode: 0,
              errorMessage: '',
              resourceType: 2,
              resourceName: '',
              resourcePatternType: 3,
              principal: '',
              host: '',
              operation: 3,
              permissionType: 3,
            },
          ],
        },
      ],
    })
  })
})
