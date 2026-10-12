const Encoder = require('../../protocol/encoder')
const { createGroupApiBroker } = require('./fixtures/groupApiHarness')
const ClassicGroupProtocol = require('../../consumer/groupProtocol/classic')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const apiKeys = require('../../protocol/requests/apiKeys')
const { newLogger } = require('testHelpers')

const answer = (name) =>
  require(`../../protocol/requests/leaveGroup/fixtures/java/leaveGroupResponse-${name}.json`)
const REASON = 'the consumer is being closed'
const BATCH = {
  groupId: 'test-group',
  members: [
    { memberId: 'test-member', groupInstanceId: null, reason: REASON },
    {
      memberId: 'other-member',
      groupInstanceId: 'test-instance',
      reason: 'member was removed by an admin',
    },
  ],
}

describe('Broker > leaveGroup', () => {
  let harness

  /** A real Broker over a fake socket offering LeaveGroup up to `maxVersion`, answering with `response`. */
  const brokerAt = (maxVersion, response) => {
    harness = createGroupApiBroker({
      versions: { [apiKeys.LeaveGroup]: { minVersion: 0, maxVersion } },
      respond: () => Buffer.from(response.hex, 'hex'),
    })
    return harness.broker
  }

  afterEach(async () => {
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  test("the batch form resolves every member's own code and throws for none of them", async () => {
    const data = await brokerAt(5, answer('v5-memberError')).leaveGroup(BATCH)

    expect(data).toMatchObject({ errorCode: 0, clientSideThrottleTime: 25 })
    expect(data.members.map(({ memberId, errorCode }) => [memberId, errorCode])).toEqual([
      ['test-member', 0],
      ['other-member', 25],
    ])
  })

  test("the single form still throws the first member's error", async () => {
    const broker = brokerAt(5, answer('v5-memberError'))

    await expect(
      broker.leaveGroup({ groupId: 'test-group', memberId: 'test-member', groupInstanceId: null })
    ).rejects.toMatchObject({ type: 'UNKNOWN_MEMBER_ID' })
  })

  test('both forms throw a top-level error', async () => {
    const hex = new Encoder()
      .writeInt32(0)
      .writeInt16(16)
      .writeUVarIntArray([])
      .writeTaggedFields()
      .buffer.toString('hex')

    await expect(brokerAt(5, { hex }).leaveGroup(BATCH)).rejects.toMatchObject({ code: 16 })
    await harness.pool.destroy()
    await expect(
      brokerAt(5, { hex }).leaveGroup({ groupId: 'test-group', memberId: 'test-member' })
    ).rejects.toMatchObject({ code: 16 })
  })

  test.each([
    [5, true],
    [4, false],
  ])("at v%i the members' reasons are on the wire: %s", async (maxVersion, onTheWire) => {
    await brokerAt(maxVersion, answer(`v${maxVersion}-ok`)).leaveGroup(BATCH)

    const [{ apiVersion, body }] = harness.requests
    expect(apiVersion).toEqual(maxVersion)
    expect(body.includes(Buffer.from(REASON))).toEqual(onTheWire)
  })

  test('the consumer\'s own leave sends the reason "the consumer is being closed" at v5, and none at v4', async () => {
    const leave = async (maxVersion) => {
      const broker = brokerAt(maxVersion, answer(`v${maxVersion}-ok`))
      const protocol = new ClassicGroupProtocol({
        cluster: {},
        groupId: 'test-group',
        sessionTimeout: 30000,
        rebalanceTimeout: 60000,
        assigners: [],
        instrumentationEmitter: new InstrumentationEventEmitter(),
        logger: newLogger(),
      })
      protocol.coordinator = broker
      protocol.memberId = 'test-member'
      await protocol.leave()
      const [{ apiVersion, body }] = harness.requests
      await harness.pool.destroy()
      return [apiVersion, body.includes(Buffer.from(REASON))]
    }

    expect(await leave(5)).toEqual([5, true])
    expect(await leave(4)).toEqual([4, false])
  })
})
