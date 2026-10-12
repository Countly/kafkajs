const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/deleteGroupsResponse-v2-${caseId}.json`)

describe('Protocol > Requests > DeleteGroups > v2 response', () => {
  test('decodes the Apache 4.3.1 ok vector, the throttle time as a number', async () => {
    const { hex, fields } = vector('ok')

    expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
      throttleTimeMs: fields.throttleTime,
      throttleTime: 0,
      clientSideThrottleTime: 25,
      results: fields.results,
    })
  })

  test('attaches the error of a failed group to its result, as v0 does', async () => {
    const { hex, fields } = vector('error')
    const data = await decode(Buffer.from(hex, 'hex'))

    expect(data.results).toEqual([
      expect.objectContaining({ ...fields.results[0], error: expect.any(Error) }),
      expect.objectContaining({ ...fields.results[1], error: expect.any(Error) }),
    ])
    expect(data.results.map(({ error }) => error.type)).toEqual([
      'NON_EMPTY_GROUP',
      'GROUP_ID_NOT_FOUND',
    ])
  })

  test('parse returns the decoded response whatever the groups answered', async () => {
    const data = await decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(parse(data)).resolves.toBe(data)
  })

  test('keeps an empty group id as an empty string, never as null', async () => {
    const body = new Encoder()
      .writeInt32(25)
      .writeUVarIntArray([new Encoder().writeUVarIntString('').writeInt16(0).writeTaggedFields()])
      .writeTaggedFields().buffer

    expect(await decode(body)).toEqual({
      throttleTimeMs: 25,
      throttleTime: 0,
      clientSideThrottleTime: 25,
      results: [{ groupId: '', errorCode: 0 }],
    })
  })
})
