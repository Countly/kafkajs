const Encoder = require('../../protocol/encoder')
const { createGroupApiBroker } = require('./fixtures/groupApiHarness')
const ClassicGroupProtocol = require('../../consumer/groupProtocol/classic')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const apiKeys = require('../../protocol/requests/apiKeys')
const { newLogger } = require('testHelpers')

const syncRequest = require('../../protocol/requests/syncGroup/fixtures/java/syncGroupRequest-v5-withProtocol.json')
const syncAnswer = (name, version = 5) =>
  require(
    `../../protocol/requests/syncGroup/fixtures/java/syncGroupResponse-v${version}-${name}.json`
  )
const INCONSISTENT = { type: 'INCONSISTENT_GROUP_PROTOCOL', code: 23, retriable: false }
const JOIN = {
  groupId: 'test-group',
  sessionTimeout: 30000,
  rebalanceTimeout: 60000,
  memberId: 'm',
  groupProtocols: [{ name: 'p', metadata: Buffer.alloc(0) }],
}
const SYNC = { groupId: 'test-group', generationId: 5, memberId: 'm', groupAssignment: [] }

/** A JoinGroup v8 answer, as the hex the harness frames. */
const joinAnswer = ({
  protocolType = 'consumer',
  protocolName = 'p',
  errorCode = 0,
  leader = 'm',
  memberId = 'm',
} = {}) => ({
  hex: new Encoder()
    .writeInt32(0)
    .writeInt16(errorCode)
    .writeInt32(5)
    .writeUVarIntString(protocolType)
    .writeUVarIntString(protocolName)
    .writeUVarIntString(leader)
    .writeUVarIntString(memberId)
    .writeUVarIntArray([])
    .writeTaggedFields()
    .buffer.toString('hex'),
})

/** A SyncGroup answer with an empty assignment (nothing to decode), reporting no protocol. */
const emptySyncAnswer = (version) => {
  const encoder = new Encoder().writeInt32(0).writeInt16(0)
  if (version >= 5) {
    encoder.writeUVarIntString(null).writeUVarIntString(null)
  }
  return encoder.writeUVarIntBytes(Buffer.alloc(0)).writeTaggedFields().buffer
}

describe('Broker > the protocol the coordinator reports', () => {
  let harness

  const brokerFor = (apiKey, maxVersion, response) => {
    harness = createGroupApiBroker({
      versions: { [apiKey]: { minVersion: 0, maxVersion } },
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

  describe('JoinGroup', () => {
    test.each([
      ['a protocol type that differs from the default', undefined, 'connect', true],
      ['a protocol type that differs from the one passed', 'connect', 'consumer', true],
      ['the protocol type sent', undefined, 'consumer', false],
      ['no protocol type', undefined, null, false],
    ])('answers %s', async (_, sent, reported, raises) => {
      const result = brokerFor(
        apiKeys.JoinGroup,
        8,
        joinAnswer({ protocolType: reported })
      ).joinGroup({
        ...JOIN,
        protocolType: sent,
      })

      if (raises) {
        await expect(result).rejects.toMatchObject(INCONSISTENT)
      } else {
        await expect(result).resolves.toMatchObject({ protocolType: reported })
      }
    })

    test('checks the answer to the retry that a MEMBER_ID_REQUIRED answer asks for, too', async () => {
      const answers = [
        joinAnswer({ errorCode: 79, memberId: 'made-up' }),
        joinAnswer({ protocolType: 'connect' }),
      ]
      harness = createGroupApiBroker({
        versions: { [apiKeys.JoinGroup]: { minVersion: 0, maxVersion: 8 } },
        respond: () => Buffer.from(answers.shift().hex, 'hex'),
      })

      await expect(harness.broker.joinGroup({ ...JOIN, memberId: '' })).rejects.toMatchObject(
        INCONSISTENT
      )
      expect(harness.requests).toHaveLength(2)
      expect(harness.requests.map(({ body }) => body.includes(Buffer.from('made-up')))).toEqual([
        false,
        true,
      ])
    })

    test('rethrows any other error as it was, without a retry', async () => {
      const broker = brokerFor(apiKeys.JoinGroup, 8, joinAnswer({ errorCode: 25 }))

      await expect(broker.joinGroup(JOIN)).rejects.toMatchObject({ type: 'UNKNOWN_MEMBER_ID' })
      expect(harness.requests).toHaveLength(1)
    })

    test('has nothing to compare below v7, whose answer carries no protocol type', async () => {
      const v6 = new Encoder()
        .writeInt32(0)
        .writeInt16(0)
        .writeInt32(5)
        .writeUVarIntString('p')
        .writeUVarIntString('m')
        .writeUVarIntString('m')
        .writeUVarIntArray([])
        .writeTaggedFields()
        .buffer.toString('hex')

      await expect(
        brokerFor(apiKeys.JoinGroup, 6, { hex: v6 }).joinGroup(JOIN)
      ).resolves.toMatchObject({
        generationId: 5,
      })
    })
  })

  describe('SyncGroup v5, which reports a protocol type and a protocol name', () => {
    const ARGS = { ...SYNC, protocolType: 'consumer', protocolName: 'RoundRobinAssigner' }

    test("accepts what was sent, which is the Apache vector's bytes on the wire", async () => {
      const { fields } = syncRequest
      const broker = brokerFor(apiKeys.SyncGroup, 5, syncAnswer('protocol'))

      await expect(
        broker.syncGroup({
          ...fields,
          groupAssignment: fields.groupAssignment.map(({ memberId, memberAssignment }) => ({
            memberId,
            memberAssignment: Buffer.from(memberAssignment, 'hex'),
          })),
        })
      ).resolves.toMatchObject({ protocolType: 'consumer', protocolName: 'RoundRobinAssigner' })
      expect(harness.requests[0].body.toString('hex')).toEqual(syncRequest.hex)
    })

    test.each([
      ['protocol type', { protocolType: 'connect' }],
      ['protocol name', { protocolName: 'RangeAssigner' }],
    ])('raises INCONSISTENT_GROUP_PROTOCOL for another %s', async (_, other) => {
      await expect(
        brokerFor(apiKeys.SyncGroup, 5, syncAnswer('protocol')).syncGroup({ ...ARGS, ...other })
      ).rejects.toMatchObject(INCONSISTENT)
    })

    test('accepts a null report', async () => {
      await expect(
        brokerFor(apiKeys.SyncGroup, 5, syncAnswer('ok')).syncGroup(ARGS)
      ).resolves.toMatchObject({ protocolType: null, protocolName: null })
    })

    test.each([
      ['no protocol keys', {}],
      ['only a protocol name', { protocolName: 'RoundRobinAssigner' }],
    ])('sends v4 and compares nothing when the caller gives %s', async (_, protocol) => {
      const broker = brokerFor(apiKeys.SyncGroup, 5, syncAnswer('ok', 4))

      await expect(broker.syncGroup({ ...SYNC, ...protocol })).resolves.toMatchObject({
        errorCode: 0,
      })
      expect(harness.requests.map(({ apiVersion }) => apiVersion)).toEqual([4])
    })
  })

  describe('the classic consumer protocol', () => {
    const protocolFor = (coordinatorOrCluster) =>
      new ClassicGroupProtocol({
        cluster: coordinatorOrCluster,
        groupId: 'test-group',
        sessionTimeout: 30000,
        rebalanceTimeout: 60000,
        assigners: [
          {
            name: 'RoundRobinAssigner',
            protocol: () => ({ name: 'RoundRobinAssigner', metadata: Buffer.alloc(0) }),
          },
        ],
        instrumentationEmitter: new InstrumentationEventEmitter(),
        logger: newLogger(),
      })
    const rebalance = (protocol) =>
      protocol.rebalance({ topics: [], topicsSubscribed: [], beforeSync: () => {} })

    /** A coordinator whose join reports the consumer protocol type and the one assigner. */
    const joinedCoordinator = () => ({
      joinGroup: jest.fn(async () => ({
        generationId: 5,
        leaderId: 'someone-else',
        memberId: 'm',
        members: [],
        protocolType: 'consumer',
        groupProtocol: 'RoundRobinAssigner',
      })),
      syncGroup: jest.fn(async () => ({ memberAssignment: Buffer.alloc(0) })),
    })

    test('names its protocol type on JoinGroup, and on SyncGroup the type and the protocol the join reported', async () => {
      const coordinator = joinedCoordinator()

      await rebalance(protocolFor({ findGroupCoordinator: async () => coordinator }))

      expect(coordinator.joinGroup).toHaveBeenCalledWith(
        expect.objectContaining({ protocolType: 'consumer' })
      )
      expect(coordinator.syncGroup).toHaveBeenCalledWith(
        expect.objectContaining({ protocolType: 'consumer', protocolName: 'RoundRobinAssigner' })
      )
    })

    test('forgets the protocol type the join reported when the generation is reset', async () => {
      const protocol = protocolFor({ findGroupCoordinator: async () => joinedCoordinator() })
      await rebalance(protocol)
      expect(protocol.protocolType).toBe('consumer')

      protocol.resetGeneration()

      expect(protocol.protocolType).toBeNull()
    })

    describe('over a fake socket', () => {
      /** What a v4 and a v5 SyncGroup request of the joined member look like on the wire. */
      const syncBytes = (withProtocol) => {
        const encoder = new Encoder()
          .writeUVarIntString('test-group')
          .writeInt32(5)
          .writeUVarIntString('m')
          .writeUVarIntString(null)
        if (withProtocol) {
          encoder.writeUVarIntString('consumer').writeUVarIntString('RoundRobinAssigner')
        }
        return encoder.writeUVarIntArray([]).writeTaggedFields().buffer.toString('hex')
      }

      test.each([
        [
          'syncs at v5 with both fields when the join answer names the protocol type and name',
          { protocolType: 'consumer', protocolName: 'RoundRobinAssigner' },
          [5, true],
        ],
        [
          'stays at v4 when the join answer names no protocol name',
          { protocolType: 'consumer', protocolName: null },
          [4, false],
        ],
        [
          'stays at v4 when the join answer names no protocol type',
          { protocolType: null, protocolName: 'RoundRobinAssigner' },
          [4, false],
        ],
      ])('%s', async (_, answered, [version, withProtocol]) => {
        harness = createGroupApiBroker({
          versions: {
            [apiKeys.JoinGroup]: { minVersion: 0, maxVersion: 9 },
            [apiKeys.SyncGroup]: { minVersion: 0, maxVersion: 5 },
          },
          respond: ({ apiKey, apiVersion }) =>
            apiKey === apiKeys.JoinGroup
              ? Buffer.from(joinAnswer({ ...answered, leader: 'someone-else' }).hex, 'hex')
              : emptySyncAnswer(apiVersion),
        })

        await rebalance(protocolFor({ findGroupCoordinator: async () => harness.broker }))

        expect(
          harness.requests.map(({ apiKey, apiVersion, body }) => [
            apiKey,
            apiVersion,
            apiKey === apiKeys.SyncGroup ? body.toString('hex') : null,
          ])
        ).toEqual([
          [apiKeys.JoinGroup, 8, null],
          [apiKeys.SyncGroup, version, syncBytes(withProtocol)],
        ])
      })
    })
  })
})
