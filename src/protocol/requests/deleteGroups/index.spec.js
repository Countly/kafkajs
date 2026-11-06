const Decoder = require('../../decoder')
const { protocol, versions } = require('./index')
const { decode: decodeV2 } = require('./v2/response')
const { decode: decodeV3 } = require('./v3/response')

const request = (version, caseId) =>
  require(`./fixtures/java/deleteGroupsRequest-v${version}-${caseId}.json`)
const response = (version, caseId) =>
  require(`./fixtures/java/deleteGroupsResponse-v${version}-${caseId}.json`)
const bodyOf = (version, caseId) => Buffer.from(response(version, caseId).hex, 'hex')

describe('Protocol > Requests > DeleteGroups', () => {
  test('offers v0 to v3', () => {
    expect(versions).toEqual(['0', '1', '2', '3'])
  })

  test('v3 preserves null, empty and nonempty per-group messages', async () => {
    const data = await decodeV3(bodyOf(3, 'nullEmptyNonempty'))

    expect(data).toEqual({
      throttleTimeMs: 25,
      throttleTime: 0,
      clientSideThrottleTime: 25,
      results: [
        { groupId: 'test-group', errorCode: 0, errorMessage: null },
        {
          groupId: 'other-group',
          errorCode: 134,
          errorMessage: '',
          error: expect.objectContaining({
            type: 'GROUP_DELETION_FAILED',
            code: 134,
            retriable: false,
          }),
        },
        {
          groupId: 'third-group',
          errorCode: 134,
          errorMessage: 'cleanup failed',
          error: expect.objectContaining({
            type: 'GROUP_DELETION_FAILED',
            code: 134,
            retriable: false,
          }),
        },
      ],
    })
    expect(data.results[0]).not.toHaveProperty('error')
    expect(data.results[1].errorMessage).not.toBeNull()
    expect(data.results[1].error.message).not.toContain('cleanup failed')
  })

  test('unknown response tags are consumed', async () => {
    const read = jest.spyOn(Decoder.prototype, 'readTaggedFields')
    try {
      const data = await decodeV3(bodyOf(3, 'unknownTags'))

      expect(data.results).toEqual([
        { groupId: 'test-group', errorCode: 0, errorMessage: null },
        {
          groupId: 'other-group',
          errorCode: 134,
          errorMessage: 'cleanup failed',
          error: expect.objectContaining({ code: 134 }),
        },
      ])
      expect(read.mock.results.map(({ value }) => [...value.keys()])).toEqual([[7], [7], [7]])
      expect(read.mock.results.map(({ value }) => [...value.values()][0].toString('hex'))).toEqual([
        'aabb',
        'aabb',
        'aabb',
      ])
    } finally {
      read.mockRestore()
    }
  })

  test('v2 stays byte-identical and omits errorMessage', async () => {
    const { hex, fields } = request(2, 'basic')
    const { request: v2 } = protocol({ version: 2 })(fields.groupIds)
    const { request: v3 } = protocol({ version: 3 })(fields.groupIds)

    expect(v2.apiVersion).toEqual(2)
    expect((await v2.encode()).buffer.toString('hex')).toEqual(hex)
    expect((await v3.encode()).buffer.toString('hex')).toEqual(hex)

    const { results } = await decodeV2(bodyOf(2, 'error'))
    expect(results).toHaveLength(2)
    for (const result of results) expect(result).not.toHaveProperty('errorMessage')
  })
})
