const { decode, parse } = require('./response')

const vector = () => require('../fixtures/java/listGroupsResponse-v5-groups.json')

describe('Protocol > Requests > ListGroups > v5', () => {
  test('decodes the groups vector the Apache 4.3.1 codec produced', async () => {
    const { hex, fields } = vector()
    const data = await decode(Buffer.from(hex, 'hex'))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      errorCode: fields.errorCode,
      groups: fields.groups,
    })
  })

  test('decodes groupType at v5, which is what tells a consumer group from a classic one', async () => {
    const data = await decode(Buffer.from(vector().hex, 'hex'))

    expect(data.groups[0]).toEqual({
      groupId: 'test-group',
      protocolType: 'consumer',
      groupState: 'Stable',
      groupType: 'Consumer',
    })
  })

  test('shares v0s parse, which throws on a top-level error code', () => {
    expect(parse).toBe(require('../v0/response').parse)
  })
})
