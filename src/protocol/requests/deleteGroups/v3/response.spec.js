const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/deleteGroupsResponse-v3-${caseId}.json`)
const decoded = (caseId) => decode(Buffer.from(vector(caseId).hex, 'hex'))

describe('Protocol > Requests > DeleteGroups > v3 response', () => {
  test('decodes the Apache 4.4.0 ok vector, the throttle time as a number', async () => {
    const { fields } = vector('ok')

    expect(await decoded('ok')).toEqual({
      throttleTimeMs: fields.throttleTime,
      throttleTime: 0,
      clientSideThrottleTime: 25,
      results: fields.results,
    })
  })

  test('attaches the typed error of a failed group and keeps the broker message beside it', async () => {
    const { results } = await decoded('error')

    expect(results).toEqual([
      {
        groupId: 'test-group',
        errorCode: 68,
        errorMessage: 'The group is not empty.',
        error: expect.any(Error),
      },
      {
        groupId: 'other-group',
        errorCode: 69,
        errorMessage: 'Group other-group not found.',
        error: expect.any(Error),
      },
    ])
    expect(results.map(({ error }) => error.type)).toEqual([
      'NON_EMPTY_GROUP',
      'GROUP_ID_NOT_FOUND',
    ])
    for (const { error, errorMessage } of results) expect(error.message).not.toContain(errorMessage)
  })

  test('keeps non-ASCII ids and messages, and an empty id beside a null message', async () => {
    const { results } = await decoded('nonAscii')

    expect(results).toEqual([
      {
        groupId: 'gruppe-é-日本',
        errorCode: 134,
        errorMessage: 'résumé ✓ failed',
        error: expect.any(Error),
      },
      { groupId: '', errorCode: 0, errorMessage: null },
    ])
  })

  test('decodes an answer with no results', async () => {
    expect(await decoded('noResults')).toEqual({
      throttleTimeMs: 25,
      throttleTime: 0,
      clientSideThrottleTime: 25,
      results: [],
    })
  })

  test('parse returns the decoded response whatever the groups answered', async () => {
    const data = await decoded('error')

    await expect(parse(data)).resolves.toBe(data)
  })
})
