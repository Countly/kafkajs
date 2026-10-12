const { createGroupApiBroker } = require('./fixtures/groupApiHarness')
const apiKeys = require('../../protocol/requests/apiKeys')

const fixture = (api, kind, name) =>
  require(`../../protocol/requests/${api}/fixtures/java/${api}${kind}-${name}.json`)

// What every 4.x broker offers (probed on 4.1.2, 4.2.2 and 4.3.1)
const BROKER_VERSIONS = {
  [apiKeys.JoinGroup]: { minVersion: 0, maxVersion: 9 },
  [apiKeys.SyncGroup]: { minVersion: 0, maxVersion: 5 },
  [apiKeys.Heartbeat]: { minVersion: 0, maxVersion: 4 },
  [apiKeys.LeaveGroup]: { minVersion: 0, maxVersion: 5 },
}

describe('Broker > the classic group APIs on a 4.x broker', () => {
  let harness

  /** One request per case: the harness answers only what is written before its first pump. */
  const exchange = (response) => {
    harness = createGroupApiBroker({
      versions: BROKER_VERSIONS,
      respond: () => Buffer.from(response.hex, 'hex'),
    })
    return harness.broker
  }
  const sent = () =>
    harness.requests.map(({ apiVersion, body }) => [apiVersion, body.toString('hex')])

  afterEach(async () => {
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  test("joins at v8, sending the Apache vector's bytes, and decodes the answer", async () => {
    const request = fixture('joinGroup', 'Request', 'v8-basic')
    const { fields } = request
    const broker = exchange(fixture('joinGroup', 'Response', 'v8-ok'))

    const data = await broker.joinGroup({
      ...fields,
      groupProtocols: fields.groupProtocols.map(({ name, metadata }) => ({
        name,
        metadata: Buffer.from(metadata, 'hex'),
      })),
    })

    expect(sent()).toEqual([[8, request.hex]])
    expect(data).toMatchObject({
      generationId: 5,
      leaderId: 'test-member',
      groupProtocol: 'RoundRobinAssigner',
    })
    expect(data.members).toHaveLength(2)
  })

  describe('SyncGroup', () => {
    /** A vector's builder arguments, its hex-encoded assignments turned into the Buffers a caller passes. */
    const optionsOf = ({ groupAssignment, ...fields }) => ({
      ...fields,
      groupAssignment: groupAssignment.map(({ memberId, memberAssignment }) => ({
        memberId,
        memberAssignment: Buffer.from(memberAssignment, 'hex'),
      })),
    })

    test("syncs at v5 when it names the protocol type and name, sending the Apache vector's bytes", async () => {
      const request = fixture('syncGroup', 'Request', 'v5-withProtocol')
      const broker = exchange(fixture('syncGroup', 'Response', 'v5-protocol'))

      const data = await broker.syncGroup(optionsOf(request.fields))

      expect(sent()).toEqual([[5, request.hex]])
      expect(data).toMatchObject({
        protocolType: 'consumer',
        protocolName: 'RoundRobinAssigner',
        memberAssignment: Buffer.from('000300000001', 'hex'),
      })
    })

    // A 4.x broker answers INCONSISTENT_GROUP_PROTOCOL to a v5 request with either field null
    // (SyncGroupRequest#areMandatoryProtocolTypeAndNamePresent, KafkaApis#handleSyncGroupRequest).
    test.each([
      ['neither', {}],
      ['only the protocol type', { protocolType: 'consumer' }],
      ['only the protocol name', { protocolName: 'RoundRobinAssigner' }],
      ['a protocol type and a null name', { protocolType: 'consumer', protocolName: null }],
      [
        'a null type and a protocol name',
        { protocolType: null, protocolName: 'RoundRobinAssigner' },
      ],
    ])('syncs at v4, with no protocol fields on the wire, when given %s', async (_, protocol) => {
      const request = fixture('syncGroup', 'Request', 'v4-assignment')
      const broker = exchange(fixture('syncGroup', 'Response', 'v4-ok'))

      const data = await broker.syncGroup({ ...optionsOf(request.fields), ...protocol })

      expect(sent()).toEqual([[4, request.hex]])
      expect(data.memberAssignment).toEqual(Buffer.from('000300000001', 'hex'))
    })
  })

  test("heartbeats at v4, sending the Apache vector's bytes", async () => {
    const request = fixture('heartbeat', 'Request', 'v4-basic')
    const broker = exchange(fixture('heartbeat', 'Response', 'v4-ok'))

    await expect(broker.heartbeat(request.fields)).resolves.toMatchObject({ errorCode: 0 })
    expect(sent()).toEqual([[4, request.hex]])
  })

  test("leaves at v5, sending the Apache vector's bytes", async () => {
    const request = fixture('leaveGroup', 'Request', 'v5-oneMember')
    const { groupId, members } = request.fields
    const broker = exchange(fixture('leaveGroup', 'Response', 'v5-ok'))

    await expect(broker.leaveGroup({ groupId, ...members[0] })).resolves.toMatchObject({
      errorCode: 0,
    })
    expect(sent()).toEqual([[5, request.hex]])
  })
})
