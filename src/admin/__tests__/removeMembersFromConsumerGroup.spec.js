const createAdmin = require('../index')
const { createErrorFromCode } = require('../../protocol/error')
const { KafkaJSNonRetriableError } = require('../../errors')
const { newLogger } = require('testHelpers')

const GROUP = 'test-group'
const DEFAULT_REASON = 'member was removed by an admin'
const RETRY = { retries: 2, initialRetryTime: 1, maxRetryTime: 2 }

const consumerRow = (members) => ({
  errorCode: 0,
  errorMessage: null,
  groupId: GROUP,
  groupState: 'Stable',
  groupEpoch: 1,
  assignmentEpoch: 1,
  assignorName: 'uniform',
  members: members.map(({ memberId, instanceId = null }) => ({
    memberId,
    instanceId,
    memberType: 1,
  })),
  authorizedOperations: -2147483648,
})

/** A member answer of a LeaveGroup response. */
const left = (memberId, groupInstanceId, errorCode = 0) => ({
  memberId,
  groupInstanceId,
  errorCode,
})

/**
 * An admin over a stub cluster whose one coordinator answers `leaveGroup` as scripted and describes
 * the group as a consumer-protocol group with `members` (or as unknown, which sends the describe on
 * to the classic API, which also answers "not found").
 */
const build = ({ members, leaveGroup, group, classic, noRows = false } = {}) => {
  const coordinator = {
    leaveGroup: leaveGroup || jest.fn(async () => ({ members: [] })),
    consumerGroupDescribe: jest.fn(async () => ({
      groups: noRows
        ? []
        : [
            group ||
              (members ? consumerRow(members) : { groupId: GROUP, errorCode: 69, members: [] }),
          ],
    })),
    describeGroups: jest.fn(async () => ({
      groups: noRows
        ? []
        : [
            classic || {
              groupId: GROUP,
              errorCode: 69,
              protocolType: '',
              members: [],
              state: 'Dead',
            },
          ],
    })),
  }
  const cluster = { findGroupCoordinator: jest.fn(async () => coordinator) }

  return {
    admin: createAdmin({ logger: newLogger(), retry: RETRY, cluster }),
    coordinator,
    cluster,
  }
}
const sentMembers = (coordinator) => coordinator.leaveGroup.mock.calls[0][0].members

describe('Admin > removeMembersFromConsumerGroup', () => {
  describe('explicit members', () => {
    test('sends each static member once, by instance id alone, to the group coordinator', async () => {
      const { admin, coordinator, cluster } = build({
        leaveGroup: jest.fn(async () => ({ members: [left('', 'a'), left('', 'b')] })),
      })

      const results = await admin.removeMembersFromConsumerGroup({
        groupId: GROUP,
        members: [{ groupInstanceId: 'a' }, { groupInstanceId: 'b' }, { groupInstanceId: 'a' }],
      })

      expect(cluster.findGroupCoordinator).toHaveBeenCalledWith({ groupId: GROUP })
      expect(coordinator.leaveGroup).toHaveBeenCalledTimes(1)
      expect(coordinator.leaveGroup.mock.calls[0][0]).toEqual({
        groupId: GROUP,
        members: [
          { memberId: '', groupInstanceId: 'a', reason: DEFAULT_REASON },
          { memberId: '', groupInstanceId: 'b', reason: DEFAULT_REASON },
        ],
      })
      expect(results).toEqual([
        { memberId: '', groupInstanceId: 'a', errorCode: 0, error: null },
        { memberId: '', groupInstanceId: 'b', errorCode: 0, error: null },
      ])
    })

    test.each([
      ['an empty list', { members: [] }],
      ['a members value that is not a list', { members: { groupInstanceId: 'a' } }],
      ['an entry without an instance id', { members: [{}] }],
      ['an empty instance id', { members: [{ groupInstanceId: '' }] }],
      ['a blank instance id', { members: [{ groupInstanceId: '  ' }] }],
      ['a numeric instance id', { members: [{ groupInstanceId: 5 }] }],
      ['a null entry', { members: [{ groupInstanceId: 'a' }, null] }],
      ['an empty group id', { groupId: '' }],
      ['a reason that is not a string', { members: [{ groupInstanceId: 'a' }], reason: 7 }],
    ])('throws a non-retriable error for %s, before anything is asked', async (_, override) => {
      const { admin, coordinator, cluster } = build()

      await expect(
        admin.removeMembersFromConsumerGroup({ groupId: GROUP, ...override })
      ).rejects.toThrow(KafkaJSNonRetriableError)
      expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
      expect(coordinator.leaveGroup).not.toHaveBeenCalled()
    })

    test('without any options is the same non-retriable error, not a TypeError', async () => {
      await expect(build().admin.removeMembersFromConsumerGroup()).rejects.toThrow(
        KafkaJSNonRetriableError
      )
    })

    test('resolves a result, not an error, for a member that could not be removed', async () => {
      const { admin } = build({
        leaveGroup: jest.fn(async () => ({ members: [left('', 'a'), left('', 'nobody', 25)] })),
      })

      const results = await admin.removeMembersFromConsumerGroup({
        groupId: GROUP,
        members: [{ groupInstanceId: 'a' }, { groupInstanceId: 'nobody' }],
      })

      expect(results.map(({ errorCode, error }) => [errorCode, error && error.type])).toEqual([
        [0, null],
        [25, 'UNKNOWN_MEMBER_ID'],
      ])
    })

    test('reports a member the answer leaves out as failed, never as removed', async () => {
      const { admin } = build({ leaveGroup: jest.fn(async () => ({ members: [left('', 'a')] })) })

      const results = await admin.removeMembersFromConsumerGroup({
        groupId: GROUP,
        members: [{ groupInstanceId: 'a' }, { groupInstanceId: 'b' }],
      })

      expect(results[1]).toMatchObject({ memberId: '', groupInstanceId: 'b', errorCode: -1 })
      expect(results[1].error).toBeInstanceOf(Error)
    })
  })

  describe('every member of the group', () => {
    test('maps static members to their instance id and dynamic ones to their member id', async () => {
      const { admin, coordinator } = build({
        members: [{ memberId: 'dynamic-1' }, { memberId: 'static-1', instanceId: 'i-1' }],
        leaveGroup: jest.fn(async () => ({ members: [left('dynamic-1', null), left('', 'i-1')] })),
      })

      const results = await admin.removeMembersFromConsumerGroup({ groupId: GROUP })

      expect(sentMembers(coordinator)).toEqual([
        { memberId: 'dynamic-1', reason: DEFAULT_REASON },
        { memberId: '', groupInstanceId: 'i-1', reason: DEFAULT_REASON },
      ])
      expect(results.map(({ memberId, groupInstanceId }) => [memberId, groupInstanceId])).toEqual([
        ['dynamic-1', null],
        ['', 'i-1'],
      ])
    })

    test('reports a dynamic member the answer leaves out with a null instance id', async () => {
      const { admin } = build({
        members: [{ memberId: 'dynamic-1' }],
        leaveGroup: jest.fn(async () => ({ members: [] })),
      })

      expect(await admin.removeMembersFromConsumerGroup({ groupId: GROUP })).toMatchObject([
        { memberId: 'dynamic-1', groupInstanceId: null, errorCode: -1 },
      ])
    })

    test.each([
      ['has no member', { members: [] }],
      ['does not exist', {}],
    ])('resolves [] without a request for a group that %s', async (_, scenario) => {
      const { admin, coordinator } = build(scenario)

      await expect(admin.removeMembersFromConsumerGroup({ groupId: GROUP })).resolves.toEqual([])
      expect(coordinator.leaveGroup).not.toHaveBeenCalled()
    })

    test("rejects a group of another protocol type, as Java's describeConsumerGroups does", async () => {
      const connect = {
        groupId: GROUP,
        errorCode: 0,
        protocolType: 'connect',
        state: 'Stable',
        protocol: 'sessioned',
        members: [
          {
            memberId: 'worker-1',
            groupInstanceId: null,
            clientId: 'c',
            clientHost: '/1.2.3.4',
            memberMetadata: Buffer.alloc(0),
            memberAssignment: Buffer.alloc(0),
          },
        ],
        authorizedOperations: -2147483648,
      }
      const { admin, coordinator } = build({ classic: connect })

      await expect(admin.removeMembersFromConsumerGroup({ groupId: GROUP })).rejects.toThrow(
        KafkaJSNonRetriableError
      )
      expect(coordinator.leaveGroup).not.toHaveBeenCalled()
    })

    test('rejects with a typed error when the coordinator describes no group at all', async () => {
      const { admin, coordinator } = build({ noRows: true })

      await expect(admin.removeMembersFromConsumerGroup({ groupId: GROUP })).rejects.toThrow(
        KafkaJSNonRetriableError
      )
      expect(coordinator.leaveGroup).not.toHaveBeenCalled()
    })

    test("rejects with the group's own error rather than reporting an empty group", async () => {
      const { admin, coordinator } = build({ group: { ...consumerRow([]), errorCode: 30 } })

      await expect(admin.removeMembersFromConsumerGroup({ groupId: GROUP })).rejects.toMatchObject({
        code: 30,
      })
      expect(coordinator.leaveGroup).not.toHaveBeenCalled()
    })
  })

  describe('the reason', () => {
    const reasonSent = async (reason) => {
      const { admin, coordinator } = build({
        leaveGroup: jest.fn(async () => ({ members: [left('', 'a')] })),
      })
      await admin.removeMembersFromConsumerGroup({
        groupId: GROUP,
        members: [{ groupInstanceId: 'a' }],
        reason,
      })
      return sentMembers(coordinator)[0].reason
    }

    test('is sent as given, and defaults to the admin text when absent, null or empty (as in Java)', async () => {
      expect(await reasonSent('replaced by a new pod')).toEqual('replaced by a new pod')
      for (const absent of [undefined, null, '']) {
        expect(await reasonSent(absent)).toEqual(DEFAULT_REASON)
      }
    })

    test('is cut to 255 characters, as the Java client cuts it', async () => {
      expect(await reasonSent('x'.repeat(300))).toEqual('x'.repeat(255))
      expect(await reasonSent('y'.repeat(255))).toEqual('y'.repeat(255))
    })
  })

  describe('a top-level error', () => {
    const request = { groupId: GROUP, members: [{ groupInstanceId: 'a' }] }

    test.each([[14], [15], [16]])(
      '%i, a coordinator that cannot serve yet, is asked again',
      async (code) => {
        const leaveGroup = jest
          .fn()
          .mockRejectedValueOnce(createErrorFromCode(code))
          .mockResolvedValue({ members: [left('', 'a')] })
        const { admin, cluster } = build({ leaveGroup })

        await expect(admin.removeMembersFromConsumerGroup(request)).resolves.toEqual([
          { memberId: '', groupInstanceId: 'a', errorCode: 0, error: null },
        ])
        expect(leaveGroup).toHaveBeenCalledTimes(2)
        expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(2)
      }
    )

    test('that never settles rejects with the retry budget, naming the cause', async () => {
      const leaveGroup = jest.fn().mockRejectedValue(createErrorFromCode(16))
      const { admin } = build({ leaveGroup })

      await expect(admin.removeMembersFromConsumerGroup(request)).rejects.toMatchObject({
        name: 'KafkaJSNumberOfRetriesExceeded',
        cause: expect.objectContaining({ code: 16 }),
      })
      expect(leaveGroup).toHaveBeenCalledTimes(RETRY.retries + 1)
    })

    test.each([[30], [41]])(
      '%i, any other, rejects at once with the error itself',
      async (code) => {
        const leaveGroup = jest.fn().mockRejectedValue(createErrorFromCode(code))
        const { admin } = build({ leaveGroup })

        await expect(admin.removeMembersFromConsumerGroup(request)).rejects.toMatchObject({ code })
        expect(leaveGroup).toHaveBeenCalledTimes(1)
      }
    )
  })
})
