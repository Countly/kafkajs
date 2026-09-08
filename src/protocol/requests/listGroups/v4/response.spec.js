const { decode, parse } = require('./response')

const vector = () => require('../fixtures/java/listGroupsResponse-v4-groups.json')

describe('Protocol > Requests > ListGroups > v4', () => {
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

  test('decodes groupState at v4, and still has no groupType', async () => {
    const data = await decode(Buffer.from(vector().hex, 'hex'))

    expect(data.groups[0]).toEqual({
      groupId: 'test-group',
      protocolType: 'consumer',
      groupState: 'Stable',
      // v4 has no GroupType field; the key is present and empty rather than absent.
      groupType: '',
    })
  })

  test('reports a broker-sent empty groupState as the empty string, same as an absent one', async () => {
    // A 4.x broker sends "" for a group whose state it cannot name, so the row shape a caller
    // reads is identical to the one a pre-v4 negotiation produces. Hand-built: no throttle, no
    // error, one group with an empty COMPACT_STRING state, then the two tag buffers.
    const data = await decode(
      Buffer.from(
        '00000000000002' + '0b746573742d67726f7570' + '09636f6e73756d6572' + '010000',
        'hex'
      )
    )

    expect(data.groups[0]).toEqual({
      groupId: 'test-group',
      protocolType: 'consumer',
      groupState: '',
      groupType: '',
    })
  })

  test('shares v0s parse, which throws on a top-level error code', () => {
    expect(parse).toBe(require('../v0/response').parse)
  })
})
