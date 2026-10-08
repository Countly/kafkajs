const createAdmin = require('../index')
const { MemberAssignment } = require('../../consumer/assignerProtocol')
const { newLogger } = require('testHelpers')
const {
  parse: parseDescribeGroupsV6,
} = require('../../protocol/requests/describeGroups/v6/response')

const GROUP_ID_NOT_FOUND = 69
const UNSUPPORTED_VERSION = 35
const GROUP_AUTHORIZATION_FAILED = 30
const COORDINATOR_LOAD_IN_PROGRESS = 14
const COORDINATOR_NOT_AVAILABLE = 15
const NOT_COORDINATOR = 16

const consumerGroupRow = (groupId) => ({
  errorCode: 0,
  errorMessage: null,
  groupId,
  groupState: 'Stable',
  groupEpoch: 7,
  assignmentEpoch: 7,
  assignorName: 'uniform',
  members: [
    {
      memberId: 'm-1',
      instanceId: null,
      rackId: null,
      memberEpoch: 7,
      clientId: 'c-1',
      clientHost: '/1.2.3.4',
      subscribedTopicNames: ['t'],
      subscribedTopicRegex: null,
      assignment: { topicPartitions: [{ topicName: 't', partitions: [0, 1] }] },
      targetAssignment: { topicPartitions: [{ topicName: 't', partitions: [0, 1] }] },
      memberType: 1,
    },
  ],
  authorizedOperations: -2147483648,
})

const classicGroupRow = (groupId) => ({
  errorCode: 0,
  errorMessage: null,
  groupId,
  state: 'Stable',
  protocolType: 'consumer',
  protocol: 'RoundRobinAssigner',
  members: [
    {
      memberId: 'm-2',
      groupInstanceId: null,
      clientId: 'c-2',
      clientHost: '/5.6.7.8',
      memberMetadata: Buffer.alloc(0),
      memberAssignment: MemberAssignment.encode({ version: 0, assignment: { t: [2, 3] } }),
    },
  ],
  authorizedOperations: -2147483648,
})

/**
 * A cluster stub reduced to what `describeGroups` touches. By default every group id resolves to
 * the SAME coordinator object, which is what makes the batching assertions meaningful; `nodeIdFor`
 * splits them across coordinators when a case needs two.
 */
const stubCluster = ({
  consumerGroupDescribe,
  describeGroups,
  onFindCoordinator = () => {},
  nodeIdFor = () => 0,
}) => {
  const coordinators = new Map()

  return {
    refreshMetadata: async () => {},
    findGroupCoordinator: async ({ groupId }) => {
      onFindCoordinator(groupId)
      const nodeId = nodeIdFor(groupId)

      if (!coordinators.has(nodeId)) {
        coordinators.set(nodeId, { nodeId, consumerGroupDescribe, describeGroups })
      }

      // The same object for the same node, so `groupBy(coordinator.nodeId)` can batch.
      return coordinators.get(nodeId)
    },
  }
}

describe('Admin > describeGroups > routing', () => {
  const retry = { retries: 3, initialRetryTime: 1, maxRetryTime: 2 }
  /** Small enough that "how many attempts does the budget allow" is exactly assertable. */
  const boundedRetry = { retries: 2, initialRetryTime: 1, maxRetryTime: 5 }

  it('describes a consumer group from ConsumerGroupDescribe alone', async () => {
    const describeGroups = jest.fn()
    const cluster = stubCluster({
      consumerGroupDescribe: jest.fn(async ({ groupIds }) => ({
        groups: groupIds.map(consumerGroupRow),
      })),
      describeGroups,
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups(['g1'])

    expect(describeGroups).not.toHaveBeenCalled()
    expect(groups).toHaveLength(1)
    expect(groups[0]).toMatchObject({
      groupId: 'g1',
      groupType: 'consumer',
      protocolType: 'consumer',
      protocol: 'uniform',
      assignorName: 'uniform',
      state: 'Stable',
      errorCode: 0,
      errorMessage: null,
      groupEpoch: 7,
      assignmentEpoch: 7,
      authorizedOperations: null,
    })
    expect(groups[0].members[0]).toMatchObject({
      memberId: 'm-1',
      clientId: 'c-1',
      clientHost: '/1.2.3.4',
      groupInstanceId: null,
      rackId: null,
      memberEpoch: 7,
      memberType: 'consumer',
      subscribedTopics: ['t'],
      subscribedTopicRegex: null,
      assignment: [{ topic: 't', partitions: [0, 1] }],
      targetAssignment: [{ topic: 't', partitions: [0, 1] }],
    })
  })

  it('synthesises a decodable memberAssignment buffer and an empty memberMetadata', async () => {
    const cluster = stubCluster({
      consumerGroupDescribe: async ({ groupIds }) => ({ groups: groupIds.map(consumerGroupRow) }),
      describeGroups: jest.fn(),
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups(['g1'])
    const member = groups[0].members[0]

    expect(member.memberMetadata).toEqual(Buffer.alloc(0))
    expect(MemberAssignment.decode(member.memberAssignment)).toMatchObject({
      assignment: { t: [0, 1] },
    })
  })

  test.each([
    ['GROUP_ID_NOT_FOUND', GROUP_ID_NOT_FOUND],
    ['UNSUPPORTED_VERSION', UNSUPPORTED_VERSION],
  ])('falls back to DescribeGroups on a 69 %s', async (_name, code) => {
    const describeGroups = jest.fn(async ({ groupIds }) => ({
      groups: groupIds.map(classicGroupRow),
    }))
    const cluster = stubCluster({
      consumerGroupDescribe: async ({ groupIds }) => ({
        groups: groupIds.map((groupId) => ({
          ...consumerGroupRow(groupId),
          errorCode: code,
          errorMessage: 'from the new API',
        })),
      }),
      describeGroups,
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups(['g1'])

    expect(describeGroups).toHaveBeenCalledTimes(1)
    expect(groups[0]).toMatchObject({
      groupId: 'g1',
      groupType: 'classic',
      protocol: 'RoundRobinAssigner',
      errorCode: 0,
      groupEpoch: null,
      assignmentEpoch: null,
      assignorName: null,
    })
    expect(groups[0].members[0]).toMatchObject({
      memberId: 'm-2',
      memberEpoch: null,
      memberType: null,
      subscribedTopics: [],
      subscribedTopicRegex: null,
      targetAssignment: null,
      assignment: [{ topic: 't', partitions: [2, 3] }],
    })
  })

  it('falls back when the broker does not offer key 69 at all', async () => {
    const describeGroups = jest.fn(async ({ groupIds }) => ({
      groups: groupIds.map(classicGroupRow),
    }))
    const cluster = stubCluster({
      consumerGroupDescribe: async () => {
        const error = new Error('The broker does not support CONSUMER_GROUP_DESCRIBE')
        error.type = 'KafkaJSServerDoesNotSupportApiKey'
        error.name = 'KafkaJSServerDoesNotSupportApiKey'
        throw error
      },
      describeGroups,
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups(['g1'])

    expect(describeGroups).toHaveBeenCalledTimes(1)
    expect(groups[0].groupType).toBe('classic')
  })

  it('reports the 69 message when both calls miss', async () => {
    const cluster = stubCluster({
      consumerGroupDescribe: async ({ groupIds }) => ({
        groups: groupIds.map((groupId) => ({
          ...consumerGroupRow(groupId),
          errorCode: GROUP_ID_NOT_FOUND,
          errorMessage: 'Group g1 is not a new consumer group',
        })),
      }),
      describeGroups: async ({ groupIds }) => ({
        groups: groupIds.map((groupId) => ({
          ...classicGroupRow(groupId),
          errorCode: GROUP_ID_NOT_FOUND,
          errorMessage: 'not found',
        })),
      }),
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups(['g1'])

    expect(groups[0].errorCode).toBe(GROUP_ID_NOT_FOUND)
    expect(groups[0].errorMessage).toBe('Group g1 is not a new consumer group')
  })

  it('fails the id with no fallback on a 69 GROUP_AUTHORIZATION_FAILED', async () => {
    const describeGroups = jest.fn()
    const cluster = stubCluster({
      consumerGroupDescribe: async ({ groupIds }) => ({
        groups: groupIds.map((groupId) => ({
          ...consumerGroupRow(groupId),
          errorCode: GROUP_AUTHORIZATION_FAILED,
          errorMessage: 'not authorized',
        })),
      }),
      describeGroups,
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups(['g1'])

    expect(describeGroups).not.toHaveBeenCalled()
    expect(groups[0]).toMatchObject({
      groupId: 'g1',
      errorCode: GROUP_AUTHORIZATION_FAILED,
      errorMessage: 'not authorized',
    })
  })

  // Two ids that shared a coordinator can land on different brokers after a
  // `__consumer_offsets` leader move. Re-pointing the whole batch at the first id's new
  // coordinator burned the budget on NOT_COORDINATOR for the other one.
  it('re-groups a batch that split across coordinators, so both ids still resolve', async () => {
    let moved = false
    const asked = []
    const cluster = stubCluster({
      consumerGroupDescribe: async ({ groupIds }) => {
        asked.push([...groupIds])

        if (!moved) {
          moved = true
          return {
            groups: groupIds.map((groupId) => ({
              ...consumerGroupRow(groupId),
              errorCode: NOT_COORDINATOR,
            })),
          }
        }

        return { groups: groupIds.map((groupId) => consumerGroupRow(groupId)) }
      },
      describeGroups: jest.fn(),
      nodeIdFor: (groupId) => (moved && groupId === 'g2' ? 1 : 0),
    })
    const admin = createAdmin({ logger: newLogger(), retry: boundedRetry, cluster })

    const { groups } = await admin.describeGroups(['g1', 'g2'])

    expect(groups.map(({ groupId, errorCode }) => [groupId, errorCode]).sort()).toEqual([
      ['g1', 0],
      ['g2', 0],
    ])
    expect(asked[0]).toEqual(['g1', 'g2'])
    expect(asked.slice(1).sort()).toEqual([['g1'], ['g2']])
  })

  // A code instead of a group describes nothing, so its type and protocol are unknown.
  it('does not call an errored row a consumer group', async () => {
    const cluster = stubCluster({
      consumerGroupDescribe: async ({ groupIds }) => ({
        groups: groupIds.map((groupId) => ({
          ...consumerGroupRow(groupId),
          errorCode: GROUP_AUTHORIZATION_FAILED,
          errorMessage: 'not authorized',
          groupState: '',
          assignorName: null,
          members: [],
        })),
      }),
      describeGroups: jest.fn(),
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups(['g1'])

    expect(groups[0]).toMatchObject({
      groupId: 'g1',
      errorCode: GROUP_AUTHORIZATION_FAILED,
      groupType: '',
      protocolType: '',
      protocol: '',
      members: [],
    })
  })

  it('retries a COORDINATOR_LOAD_IN_PROGRESS and re-finds the coordinator on a NOT_COORDINATOR', async () => {
    const found = []
    let call = 0
    const cluster = stubCluster({
      consumerGroupDescribe: async ({ groupIds }) => {
        call += 1
        const errorCode =
          call === 1 ? COORDINATOR_LOAD_IN_PROGRESS : call === 2 ? NOT_COORDINATOR : 0
        return {
          groups: groupIds.map((groupId) => ({ ...consumerGroupRow(groupId), errorCode })),
        }
      },
      describeGroups: jest.fn(),
      onFindCoordinator: (groupId) => found.push(groupId),
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups(['g1'])

    expect(groups[0].errorCode).toBe(0)
    expect(call).toBe(3)
    // One initial lookup, plus one re-find for the NOT_COORDINATOR.
    expect(found.filter((id) => id === 'g1').length).toBeGreaterThanOrEqual(2)
  })

  it('asks a loading coordinator again where it is, without looking it up a second time', async () => {
    const found = []
    let call = 0
    const cluster = stubCluster({
      consumerGroupDescribe: async ({ groupIds }) => {
        call += 1
        return {
          groups: groupIds.map((groupId) => ({
            ...consumerGroupRow(groupId),
            errorCode: call < 3 ? COORDINATOR_LOAD_IN_PROGRESS : 0,
          })),
        }
      },
      describeGroups: jest.fn(),
      onFindCoordinator: (groupId) => found.push(groupId),
    })
    const admin = createAdmin({ logger: newLogger(), retry: boundedRetry, cluster })

    const { groups } = await admin.describeGroups(['g1'])

    expect(groups[0].errorCode).toBe(0)
    expect(call).toBe(3)
    // A loading coordinator has not moved, so it is asked again without a second lookup.
    expect(found).toEqual(['g1'])
  })

  it.each([
    ['COORDINATOR_NOT_AVAILABLE', COORDINATOR_NOT_AVAILABLE],
    ['NOT_COORDINATOR', NOT_COORDINATOR],
  ])('looks the coordinator up again after a %s', async (_name, code) => {
    const found = []
    let call = 0
    const cluster = stubCluster({
      consumerGroupDescribe: async ({ groupIds }) => {
        call += 1
        return {
          groups: groupIds.map((groupId) => ({
            ...consumerGroupRow(groupId),
            errorCode: call === 1 ? code : 0,
          })),
        }
      },
      describeGroups: jest.fn(),
      onFindCoordinator: (groupId) => found.push(groupId),
    })
    const admin = createAdmin({ logger: newLogger(), retry: boundedRetry, cluster })

    const { groups } = await admin.describeGroups(['g1'])

    expect(groups[0].errorCode).toBe(0)
    expect(call).toBe(2)
    expect(found).toEqual(['g1', 'g1'])
  })

  it('gives up on the retry budget instead of asking a stuck coordinator for ever', async () => {
    const consumerGroupDescribe = jest.fn(async ({ groupIds }) => ({
      groups: groupIds.map((groupId) => ({
        ...consumerGroupRow(groupId),
        errorCode: COORDINATOR_LOAD_IN_PROGRESS,
      })),
    }))
    const cluster = stubCluster({ consumerGroupDescribe, describeGroups: jest.fn() })
    const admin = createAdmin({ logger: newLogger(), retry: boundedRetry, cluster })

    await expect(admin.describeGroups(['g1'])).rejects.toThrow(/Coordinator|retries/i)
    // The budget bounds the attempts: the initial one plus `retries`.
    expect(consumerGroupDescribe).toHaveBeenCalledTimes(boundedRetry.retries + 1)
  })

  it('describes an id the coordinator left out of its answer through the older API', async () => {
    const describeGroups = jest.fn(async ({ groupIds }) => ({
      groups: groupIds.map(classicGroupRow),
    }))
    const cluster = stubCluster({
      consumerGroupDescribe: async () => ({ groups: [] }),
      describeGroups,
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups(['g1'])

    expect(describeGroups.mock.calls[0][0].groupIds).toEqual(['g1'])
    expect(groups).toHaveLength(1)
    expect(groups[0]).toMatchObject({ groupId: 'g1', groupType: 'classic', errorCode: 0 })
  })

  it('mixes a consumer group and a classic group in one call, every row carrying errorCode', async () => {
    const cluster = stubCluster({
      consumerGroupDescribe: async ({ groupIds }) => ({
        groups: groupIds.map((groupId) =>
          groupId === 'classic'
            ? { ...consumerGroupRow(groupId), errorCode: GROUP_ID_NOT_FOUND, errorMessage: 'nope' }
            : consumerGroupRow(groupId)
        ),
      }),
      describeGroups: async ({ groupIds }) => ({ groups: groupIds.map(classicGroupRow) }),
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups(['modern', 'classic'])

    expect(groups).toHaveLength(2)
    expect(groups.every((group) => 'errorCode' in group)).toBe(true)
    expect(groups.map(({ groupId, groupType }) => [groupId, groupType]).sort()).toEqual([
      ['classic', 'classic'],
      ['modern', 'consumer'],
    ])
  })

  it('sends one request per coordinator, not one per group id', async () => {
    const consumerGroupDescribe = jest.fn(async ({ groupIds }) => ({
      groups: groupIds.map(consumerGroupRow),
    }))
    const cluster = stubCluster({ consumerGroupDescribe, describeGroups: jest.fn() })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups(['g1', 'g2', 'g3'])

    expect(groups).toHaveLength(3)
    // Three ids, one coordinator, ONE request carrying all three - the 2.2.4 shape.
    expect(consumerGroupDescribe).toHaveBeenCalledTimes(1)
    expect(consumerGroupDescribe.mock.calls[0][0].groupIds.sort()).toEqual(['g1', 'g2', 'g3'])
  })

  it('batches the fallback per coordinator too, carrying only the ids that routed', async () => {
    const describeGroups = jest.fn(async ({ groupIds }) => ({
      groups: groupIds.map(classicGroupRow),
    }))
    const consumerGroupDescribe = jest.fn(async ({ groupIds }) => ({
      groups: groupIds.map((groupId) =>
        groupId === 'modern'
          ? consumerGroupRow(groupId)
          : { ...consumerGroupRow(groupId), errorCode: GROUP_ID_NOT_FOUND, errorMessage: 'nope' }
      ),
    }))
    const cluster = stubCluster({ consumerGroupDescribe, describeGroups })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups(['modern', 'old-a', 'old-b'])

    expect(groups).toHaveLength(3)
    expect(consumerGroupDescribe).toHaveBeenCalledTimes(1)
    expect(describeGroups).toHaveBeenCalledTimes(1)
    expect(describeGroups.mock.calls[0][0].groupIds.sort()).toEqual(['old-a', 'old-b'])
  })

  it('sends one request to each coordinator when the ids are split across two', async () => {
    const consumerGroupDescribe = jest.fn(async ({ groupIds }) => ({
      groups: groupIds.map(consumerGroupRow),
    }))
    const cluster = stubCluster({
      consumerGroupDescribe,
      describeGroups: jest.fn(),
      nodeIdFor: (groupId) => (groupId === 'far' ? 1 : 0),
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups(['near-a', 'near-b', 'far'])

    expect(groups).toHaveLength(3)
    expect(consumerGroupDescribe).toHaveBeenCalledTimes(2)
    expect(
      consumerGroupDescribe.mock.calls.map(([{ groupIds }]) => groupIds.sort()).sort()
    ).toEqual([['far'], ['near-a', 'near-b']])
  })

  it.each(['Assigning', 'Reconciling'])(
    'reports the %s state a consumer group can be in',
    async (groupState) => {
      const cluster = stubCluster({
        consumerGroupDescribe: async ({ groupIds }) => ({
          groups: groupIds.map((groupId) => ({ ...consumerGroupRow(groupId), groupState })),
        }),
        describeGroups: jest.fn(),
      })
      const admin = createAdmin({ logger: newLogger(), retry, cluster })

      const { groups } = await admin.describeGroups(['g1'])

      // Wire-cased, and asserted by name rather than as "not Dead": these two strings are new to
      // `ConsumerGroupState` and are the whole reason the union widened.
      expect(groups[0].state).toEqual(groupState)
    }
  )

  it('keeps the 2.2.4 row shape for a group that does not exist, and adds the code', async () => {
    const cluster = stubCluster({
      consumerGroupDescribe: async ({ groupIds }) => ({
        groups: groupIds.map((groupId) => ({
          ...consumerGroupRow(groupId),
          errorCode: GROUP_ID_NOT_FOUND,
          errorMessage: 'Group g1 is not a new consumer group',
        })),
      }),
      describeGroups: async ({ groupIds }) => ({
        groups: groupIds.map((groupId) => ({
          errorCode: GROUP_ID_NOT_FOUND,
          errorMessage: 'not found',
          groupId,
          state: 'Dead',
          protocolType: '',
          protocol: '',
          members: [],
          authorizedOperations: -2147483648,
        })),
      }),
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups(['g1'])

    // Compatibility first: the shape a 2.2.4 caller tested, plus the code it can now read.
    expect(groups[0]).toMatchObject({
      groupId: 'g1',
      state: 'Dead',
      members: [],
      protocol: '',
      protocolType: '',
      errorCode: GROUP_ID_NOT_FOUND,
      errorMessage: 'Group g1 is not a new consumer group',
    })
  })

  it('lets an error that is not a routing signal out of the call', async () => {
    const describeGroups = jest.fn()
    const cluster = stubCluster({
      consumerGroupDescribe: async () => {
        throw new Error('the connection went away')
      },
      describeGroups,
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    await expect(admin.describeGroups(['g1'])).rejects.toThrow('the connection went away')
    expect(describeGroups).not.toHaveBeenCalled()
  })

  it('describes a consumer group the coordinator answered without an assignor or members', async () => {
    const cluster = stubCluster({
      consumerGroupDescribe: async ({ groupIds }) => ({
        groups: groupIds.map((groupId) => ({
          ...consumerGroupRow(groupId),
          assignorName: null,
          members: undefined,
          authorizedOperations: 4,
        })),
      }),
      describeGroups: jest.fn(),
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups(['g1'])

    expect(groups[0]).toMatchObject({
      protocol: '',
      assignorName: null,
      authorizedOperations: 4,
      members: [],
    })
  })

  it('describes consumer-group members whose optional fields the broker did send, and did not', async () => {
    const cluster = stubCluster({
      consumerGroupDescribe: async ({ groupIds }) => ({
        groups: groupIds.map((groupId) => ({
          ...consumerGroupRow(groupId),
          members: [
            {
              memberId: 'static',
              instanceId: 'i-1',
              rackId: 'rack-1',
              memberEpoch: 3,
              clientId: 'c-1',
              clientHost: '/1.2.3.4',
              subscribedTopicNames: undefined,
              subscribedTopicRegex: 'topic-.*',
              assignment: {},
              targetAssignment: {},
              memberType: 0,
            },
            {
              memberId: 'unassigned',
              instanceId: null,
              rackId: null,
              memberEpoch: 0,
              clientId: 'c-2',
              clientHost: '/5.6.7.8',
              subscribedTopicNames: ['t'],
              subscribedTopicRegex: null,
              assignment: undefined,
              targetAssignment: null,
              memberType: 9,
            },
          ],
        })),
      }),
      describeGroups: jest.fn(),
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups(['g1'])

    expect(groups[0].members[0]).toMatchObject({
      groupInstanceId: 'i-1',
      rackId: 'rack-1',
      memberType: 'classic',
      subscribedTopics: [],
      subscribedTopicRegex: 'topic-.*',
      assignment: [],
      targetAssignment: [],
    })
    expect(groups[0].members[1]).toMatchObject({
      memberType: null,
      assignment: [],
      targetAssignment: null,
    })
  })

  it('returns a classic group of another framework as it is, without decoding its buffers', async () => {
    const foreignAssignment = Buffer.from('not-a-consumer-assignment')
    const cluster = stubCluster({
      consumerGroupDescribe: async () => {
        const error = new Error('The broker does not support CONSUMER_GROUP_DESCRIBE')
        error.name = 'KafkaJSServerDoesNotSupportApiKey'
        throw error
      },
      describeGroups: async ({ groupIds }) =>
        parseDescribeGroupsV6({
          groups: groupIds.map((groupId) => ({
            ...classicGroupRow(groupId),
            protocolType: 'connect',
            protocol: 'sessioned',
            members: [
              { ...classicGroupRow(groupId).members[0], memberAssignment: foreignAssignment },
            ],
            authorizedOperations: 4,
          })),
        }),
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups(['g1'])

    expect(groups[0]).toMatchObject({
      protocolType: 'connect',
      groupType: 'classic',
      errorCode: 0,
      errorMessage: null,
      authorizedOperations: 4,
    })
    expect(groups[0].members[0].memberAssignment).toBe(foreignAssignment)
    expect(groups[0].members[0].assignment).toEqual([])
  })

  it('fails the call when DescribeGroups answers a group-level code other than GROUP_ID_NOT_FOUND', async () => {
    // DescribeGroups' own parse throws such a code, so no classic row can carry one: the id that
    // fell back to the older API fails the call through the retrier instead.
    const cluster = stubCluster({
      consumerGroupDescribe: async ({ groupIds }) => ({
        groups: groupIds.map((groupId) => ({
          ...consumerGroupRow(groupId),
          errorCode: GROUP_ID_NOT_FOUND,
          errorMessage: 'Group g1 is not a consumer group',
        })),
      }),
      describeGroups: async ({ groupIds }) =>
        parseDescribeGroupsV6({
          groups: groupIds.map((groupId) => ({
            ...classicGroupRow(groupId),
            errorCode: GROUP_AUTHORIZATION_FAILED,
            errorMessage: 'not authorized',
          })),
        }),
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    await expect(admin.describeGroups(['g1'])).rejects.toMatchObject({
      name: 'KafkaJSNonRetriableError',
      type: 'GROUP_AUTHORIZATION_FAILED',
      code: GROUP_AUTHORIZATION_FAILED,
    })
  })

  it('leaves an assignment buffer it cannot decode, or has none of, as an empty assignment', async () => {
    const cluster = stubCluster({
      consumerGroupDescribe: async ({ groupIds }) => ({
        groups: groupIds.map((groupId) => ({
          ...consumerGroupRow(groupId),
          errorCode: GROUP_ID_NOT_FOUND,
          errorMessage: 'nope',
        })),
      }),
      describeGroups: async ({ groupIds }) => ({
        groups: groupIds.map((groupId) => {
          const row = classicGroupRow(groupId)
          row.members[0].memberAssignment = Buffer.from([0xff, 0xff])
          row.members.push({
            ...row.members[0],
            memberId: 'm-3',
            memberAssignment: Buffer.alloc(0),
          })
          return row
        }),
      }),
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups(['g1'])

    expect(groups[0].members[0].assignment).toEqual([])
    expect(groups[0].members[0].memberAssignment).toEqual(Buffer.from([0xff, 0xff]))
    expect(groups[0].members[1].assignment).toEqual([])
  })
})
