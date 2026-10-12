const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/createAclsResponse-v3-${caseId}.json`)

describe('Protocol > Requests > CreateAcls > v3 response', () => {
  test.each(['ok', 'error'])('decodes the Apache 4.3.1 %s vector', async (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      creationResponses: fields.creationResponses,
    })
  })

  test('parse throws the error of the first failed creation', async () => {
    const failed = await decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(parse(failed)).rejects.toMatchObject({ type: 'INVALID_REQUEST', code: 42 })
  })
})
