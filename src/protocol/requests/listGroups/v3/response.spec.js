const { decode, parse } = require('./response')
const { createErrorFromCode } = require('../../../error')

const vector = () => require('../fixtures/java/listGroupsResponse-v3-groups.json')

describe('Protocol > Requests > ListGroups > v3', () => {
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

  // The fields a version does not carry are reported as the empty string, so a caller reads one
  // row shape whatever the broker negotiated.
  test('reports an absent groupState and groupType as empty strings at v3', async () => {
    const data = await decode(Buffer.from(vector().hex, 'hex'))

    expect(data.groups[0]).toEqual({
      groupId: 'test-group',
      protocolType: 'consumer',
      groupState: '',
      groupType: '',
    })
  })

  test('reports the broker throttle as clientSideThrottleTime and pins throttleTime at 0', async () => {
    // No committed vector carries a non-zero ThrottleTimeMs, so the KIP-219 split is pinned from
    // a hand-built frame: 250 ms of throttle, no error, an empty group array and a tag buffer.
    const data = await decode(Buffer.from('000000fa00000100', 'hex'))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 250,
      errorCode: 0,
      groups: [],
    })
  })

  test('throws on a top-level error code, which is what the retrier acts on', async () => {
    // COORDINATOR_LOAD_IN_PROGRESS and COORDINATOR_NOT_AVAILABLE arrive here, and the shipped
    // retrier turns a thrown retriable protocol error into a retry - so `parse` must throw.
    await expect(parse({ errorCode: 14, groups: [] })).rejects.toThrow(createErrorFromCode(14))
  })
})
