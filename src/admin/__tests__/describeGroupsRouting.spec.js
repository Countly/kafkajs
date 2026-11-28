const { createGroupDescriptions } = require('../groupDescriptions')
const { createErrorFromCode } = require('../../protocol/error')
const createAdmin = require('../index')
const { MemberAssignment } = require('../../consumer/assignerProtocol')
const { newLogger } = require('testHelpers')
const {
  parse: parseDescribeGroupsV6,
} = require('../../protocol/requests/describeGroups/v6/response')
const Encoder = require('../../protocol/encoder')
const Decoder = require('../../protocol/decoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const {
  KafkaJSConnectionError,
  KafkaJSError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSRequestTimeoutError,
  KafkaJSTimeout,
} = require('../../errors')

const GROUP_ID_NOT_FOUND = 69
const UNSUPPORTED_VERSION = 35
const GROUP_AUTHORIZATION_FAILED = 30
const COORDINATOR_LOAD_IN_PROGRESS = 14
const COORDINATOR_NOT_AVAILABLE = 15
const NOT_COORDINATOR = 16

/** The string a broker reads back from the bytes a request carried for `id`. */
const echoOf = (id) => Buffer.from(id, 'utf8').toString('utf8')

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
    withCoordinatorBroker: (nodeId, work) => work(coordinators.get(nodeId)),
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

  test.each([
    ['the DescribeGroups v6 answer', { errorCode: GROUP_ID_NOT_FOUND, errorMessage: 'not found' }],
    ['an older DescribeGroups answer', { errorCode: 0, errorMessage: null }],
  ])(
    'reports %s, not UNSUPPORTED_VERSION, for a missing group on a cluster without the consumer protocol',
    async (_name, answer) => {
      const cluster = stubCluster({
        consumerGroupDescribe: async ({ groupIds }) => ({
          groups: groupIds.map((groupId) => ({ groupId, errorCode: UNSUPPORTED_VERSION })),
        }),
        describeGroups: async ({ groupIds }) => ({
          groups: groupIds.map((groupId) => ({
            groupId,
            state: 'Dead',
            protocolType: '',
            protocol: '',
            members: [],
            authorizedOperations: -2147483648,
            ...answer,
          })),
        }),
      })
      const admin = createAdmin({ logger: newLogger(), retry, cluster })

      const { groups } = await admin.describeGroups(['g1'])

      expect(groups).toEqual([
        expect.objectContaining({ groupId: 'g1', state: 'Dead', groupType: 'classic', ...answer }),
      ])
    }
  )

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

    const { groups } = await admin.describeGroups(['g1'], { includeAuthorizedOperations: true })

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

    const { groups } = await admin.describeGroups(['g1'], { includeAuthorizedOperations: true })

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

  // A lone surrogate is written as U+FFFD, so a broker echoes that and not the string that was asked.
  describe('a group id with a lone surrogate', () => {
    const notConsumerGroup = ({ groupIds }) => ({
      groups: groupIds.map((groupId) => ({
        ...consumerGroupRow(echoOf(groupId)),
        errorCode: GROUP_ID_NOT_FOUND,
        errorMessage: null,
      })),
    })
    const classicEcho = ({ groupIds }) => ({
      groups: groupIds.map((groupId) => classicGroupRow(echoOf(groupId))),
    })

    it('is described from the consumer-protocol row the broker echoes, under the id asked and without a fallback', async () => {
      const describeGroups = jest.fn()
      const consumerGroupDescribe = jest.fn(async ({ groupIds }) => ({
        groups: groupIds.map((groupId) => consumerGroupRow(echoOf(groupId))),
      }))
      const cluster = stubCluster({ consumerGroupDescribe, describeGroups })
      const admin = createAdmin({ logger: newLogger(), retry, cluster })

      const { groups } = await admin.describeGroups(['team-\uD800'])

      expect(
        groups.map(({ groupId, groupType, errorCode }) => ({ groupId, groupType, errorCode }))
      ).toEqual([{ groupId: 'team-\uD800', groupType: 'consumer', errorCode: 0 }])
      expect(describeGroups).not.toHaveBeenCalled()
      expect(consumerGroupDescribe).toHaveBeenCalledTimes(1)
    })

    it('is described once through DescribeGroups when the broker echoes both answers, under the id asked', async () => {
      const describeGroups = jest.fn(classicEcho)
      const cluster = stubCluster({ consumerGroupDescribe: notConsumerGroup, describeGroups })
      const admin = createAdmin({ logger: newLogger(), retry, cluster })

      const { groups } = await admin.describeGroups(['team-\uD800'])

      expect(groups.map(({ groupId, groupType }) => [groupId, groupType])).toEqual([
        ['team-\uD800', 'classic'],
      ])
      expect(describeGroups).toHaveBeenCalledTimes(1)
      expect(describeGroups.mock.calls[0][0].groupIds.map(echoOf)).toEqual(['team-�'])
    })

    it('is not asked again after its coordinator answered while a sibling coordinator moves', async () => {
      let moved = false
      let siblingRefused = false
      const describeGroups = jest.fn(async (request) => {
        if (!moved) {
          moved = true
          throw createErrorFromCode(NOT_COORDINATOR)
        }
        if (request.groupIds.length === 1 && request.groupIds[0] === 'team-b' && !siblingRefused) {
          siblingRefused = true
          throw createErrorFromCode(NOT_COORDINATOR)
        }
        return classicEcho(request)
      })
      const cluster = stubCluster({
        consumerGroupDescribe: notConsumerGroup,
        describeGroups,
        nodeIdFor: (groupId) => (moved && groupId === 'team-b' ? 1 : 0),
      })
      const admin = createAdmin({ logger: newLogger(), retry, cluster })

      const { groups } = await admin.describeGroups(['team-\uD800', 'team-b'])

      expect(groups.map(({ groupId }) => groupId).sort()).toEqual(['team-b', 'team-\uD800'])
      expect(describeGroups.mock.calls.map(([request]) => request.groupIds.map(echoOf))).toEqual([
        ['team-�', 'team-b'],
        ['team-�'],
        ['team-b'],
        ['team-b'],
      ])
    })

    it('is still described through DescribeGroups when the consumer-protocol answer is for another id', async () => {
      const describeGroups = jest.fn(classicEcho)
      const cluster = stubCluster({
        consumerGroupDescribe: async () => ({ groups: [consumerGroupRow('team-��')] }),
        describeGroups,
      })
      const admin = createAdmin({ logger: newLogger(), retry, cluster })

      const { groups } = await admin.describeGroups(['team-\uD800'])

      expect(describeGroups.mock.calls.map(([request]) => request.groupIds.map(echoOf))).toEqual([
        ['team-�'],
      ])
      expect(groups.find(({ groupId }) => groupId === 'team-\uD800')).toMatchObject({
        groupType: 'classic',
      })
      // The row for the id nobody asked is not part of the result.
      expect(groups.map(({ groupId, groupType }) => [groupId, groupType])).toEqual([
        ['team-\uD800', 'classic'],
      ])
    })

    it('is sent once, like an identical id given twice, when another id in the call has the same bytes, and answered under each id', async () => {
      const run = async (ids) => {
        const consumerGroupDescribe = jest.fn(async ({ groupIds }) => ({
          groups: groupIds.map((groupId) => consumerGroupRow(echoOf(groupId))),
        }))
        const cluster = stubCluster({ consumerGroupDescribe, describeGroups: jest.fn() })
        const admin = createAdmin({ logger: newLogger(), retry, cluster })
        const { groups } = await admin.describeGroups(ids)
        return {
          groups,
          sent: consumerGroupDescribe.mock.calls.map(([r]) => r.groupIds.map(echoOf)),
        }
      }

      const identical = await run(['a', 'a'])
      const sameBytes = await run(['a\uD800', 'a\uD801'])

      expect(identical.sent).toEqual([['a']])
      expect(sameBytes.sent).toEqual([['a�']])
      expect(sameBytes.groups).toHaveLength(identical.groups.length)
      expect(sameBytes.groups.map(({ groupId }) => groupId)).toEqual(['a\uD800', 'a\uD801'])
    })

    // The group's name is `team-` and U+FFFD; the caller names it with that character and also with a
    // lone surrogate, which is written as the same bytes.
    describe('next to a literal U+FFFD spelling of the same group', () => {
      const SURROGATE = 'team-\uD800'
      const LITERAL = 'team-�'
      const withMessage = (row, errorCode, errorMessage) => ({ ...row, errorCode, errorMessage })
      const consumerEcho = ({ groupIds }) => ({
        groups: groupIds.map((groupId) => consumerGroupRow(echoOf(groupId))),
      })
      const run = async ({ consumerGroupDescribe, describeGroups = jest.fn() }, ids) => {
        const cluster = stubCluster({ consumerGroupDescribe, describeGroups })
        const admin = createAdmin({ logger: newLogger(), retry, cluster })
        return (await admin.describeGroups(ids)).groups
      }
      const summary = (groups) =>
        groups.map(({ groupId, errorCode, groupType }) => [groupId, errorCode, groupType])

      it('answers a consumer group under each spelling, the lone surrogate given first', async () => {
        const consumerGroupDescribe = jest.fn(consumerEcho)

        const groups = await run({ consumerGroupDescribe }, [SURROGATE, LITERAL])

        expect(summary(groups)).toEqual([
          [SURROGATE, 0, 'consumer'],
          [LITERAL, 0, 'consumer'],
        ])
        expect(consumerGroupDescribe.mock.calls.map(([r]) => r.groupIds.map(echoOf))).toEqual([
          ['team-�'],
        ])
      })

      it('answers a consumer group under each spelling, the literal given first', async () => {
        const groups = await run({ consumerGroupDescribe: consumerEcho }, [LITERAL, SURROGATE])

        expect(summary(groups)).toEqual([
          [LITERAL, 0, 'consumer'],
          [SURROGATE, 0, 'consumer'],
        ])
      })

      it('returns the error the coordinator reported under each spelling, in either order', async () => {
        const denied = ({ groupIds }) => ({
          groups: groupIds.map((groupId) =>
            withMessage(consumerGroupRow(echoOf(groupId)), GROUP_AUTHORIZATION_FAILED, 'no access')
          ),
        })

        const surrogateFirst = await run({ consumerGroupDescribe: denied }, [SURROGATE, LITERAL])
        const literalFirst = await run({ consumerGroupDescribe: denied }, [LITERAL, SURROGATE])

        expect(
          surrogateFirst.map(({ groupId, errorCode, errorMessage }) => [
            groupId,
            errorCode,
            errorMessage,
          ])
        ).toEqual([
          [SURROGATE, GROUP_AUTHORIZATION_FAILED, 'no access'],
          [LITERAL, GROUP_AUTHORIZATION_FAILED, 'no access'],
        ])
        expect(literalFirst.map(({ groupId }) => groupId)).toEqual([LITERAL, SURROGATE])
      })

      it('answers a classic group under each spelling from the one row it was asked once for', async () => {
        const describeGroups = jest.fn(classicEcho)
        const classic = (ids) =>
          run({ consumerGroupDescribe: notConsumerGroup, describeGroups }, ids)

        const surrogateFirst = await classic([SURROGATE, LITERAL])
        const literalFirst = await classic([LITERAL, SURROGATE])

        expect(summary(surrogateFirst)).toEqual([
          [SURROGATE, 0, 'classic'],
          [LITERAL, 0, 'classic'],
        ])
        expect(literalFirst.map(({ groupId }) => groupId)).toEqual([LITERAL, SURROGATE])
        expect(describeGroups.mock.calls.map(([r]) => r.groupIds.map(echoOf))).toEqual([
          ['team-�'],
          ['team-�'],
        ])
      })

      it('returns the unknown-group message of the newer API under each spelling when both APIs miss', async () => {
        const unknown = ({ groupIds }) => ({
          groups: groupIds.map((groupId) =>
            withMessage(consumerGroupRow(echoOf(groupId)), GROUP_ID_NOT_FOUND, 'not consumer')
          ),
        })
        const describeGroups = ({ groupIds }) => ({
          groups: groupIds.map((groupId) => ({
            ...classicGroupRow(echoOf(groupId)),
            errorCode: GROUP_ID_NOT_FOUND,
          })),
        })

        const literalFirst = await run({ consumerGroupDescribe: unknown, describeGroups }, [
          LITERAL,
          SURROGATE,
        ])

        expect(
          literalFirst.map(({ groupId, errorCode, errorMessage }) => [
            groupId,
            errorCode,
            errorMessage,
          ])
        ).toEqual([
          [LITERAL, GROUP_ID_NOT_FOUND, 'not consumer'],
          [SURROGATE, GROUP_ID_NOT_FOUND, 'not consumer'],
        ])
      })

      it('keeps a spelling given twice as two rows from a consumer group and as one from a classic group', async () => {
        const twice = [SURROGATE, LITERAL, SURROGATE]
        const describeGroups = jest.fn(classicEcho)

        const consumer = await run({ consumerGroupDescribe: consumerEcho }, twice)
        const classic = await run(
          { consumerGroupDescribe: notConsumerGroup, describeGroups },
          twice
        )

        expect(consumer.map(({ groupId }) => groupId)).toEqual(twice)
        expect(classic.map(({ groupId }) => groupId)).toEqual([SURROGATE, LITERAL])
      })

      it('sends and looks up a written id once, whatever spellings and repeats the caller gave', async () => {
        const consumerGroupDescribe = jest.fn(consumerEcho)
        const looked = []
        const cluster = stubCluster({
          consumerGroupDescribe,
          describeGroups: jest.fn(),
          onFindCoordinator: (groupId) => looked.push(groupId),
        })
        const admin = createAdmin({ logger: newLogger(), retry, cluster })

        const { groups } = await admin.describeGroups([SURROGATE, LITERAL, SURROGATE, 'other'])

        expect(consumerGroupDescribe.mock.calls.map(([r]) => r.groupIds.map(echoOf))).toEqual([
          ['team-�', 'other'],
        ])
        expect(looked.map(echoOf)).toEqual(['team-�', 'other'])
        expect(groups.map(({ groupId }) => groupId)).toEqual([
          SURROGATE,
          LITERAL,
          SURROGATE,
          'other',
        ])
      })

      // Kafka 4.0.0 hands each offsets shard the whole id list, so a shard that does not own a group
      // answers it as unknown while the owner answers it described.
      describe('when the broker answers one written id in several rows', () => {
        const miss = (groupId) => withMessage(consumerGroupRow(groupId), GROUP_ID_NOT_FOUND, 'no')
        // A row the way a broker sends one that names a code and no group: no members.
        const errorRow = (groupId, errorCode, errorMessage) => ({
          ...consumerGroupRow(groupId),
          errorCode,
          errorMessage,
          groupState: '',
          assignorName: null,
          members: [],
        })
        // Kafka 4.0.0: each shard answers every id it was given; one owns each group.
        const shardRows = (ownerFirst) => (groupIds) => {
          const [team, other] = groupIds
          const teamShard = [consumerGroupRow(team), miss(other)]
          const otherShard = [miss(team), consumerGroupRow(other)]
          return ownerFirst ? [...teamShard, ...otherShard] : [...otherShard, ...teamShard]
        }

        it.each([
          ['the lone surrogate given first, the owner of the group answering first', true, false],
          ['the literal given first, the owner of the group answering first', false, false],
          ['the lone surrogate given first, the other shard answering first', true, true],
          ['the literal given first, the other shard answering first', false, true],
        ])(
          'answers from the described row, not the misses, with %s',
          async (_label, surrogateFirst, otherShardFirst) => {
            const spellings = surrogateFirst ? [SURROGATE, LITERAL] : [LITERAL, SURROGATE]
            const ids = [...spellings, 'other']
            const describeGroups = jest.fn(classicEcho)
            const consumerGroupDescribe = jest.fn(async ({ groupIds }) => ({
              groups: shardRows(!otherShardFirst)(groupIds),
            }))

            const groups = await run({ consumerGroupDescribe, describeGroups }, ids)

            expect(summary(groups).sort()).toEqual(
              ids.map((groupId) => [groupId, 0, 'consumer']).sort()
            )
            expect(describeGroups).not.toHaveBeenCalled()
            expect(consumerGroupDescribe.mock.calls.map(([r]) => r.groupIds.map(echoOf))).toEqual([
              ['team-�', 'other'],
            ])
          }
        )

        // Adversarial: a broker leaves a group it may not describe out of every shard's list, so no
        // release answers one id both described and GROUP_AUTHORIZATION_FAILED.
        it('prefers a described row to an error row for the same written id, in an answer no broker sends', async () => {
          const consumerGroupDescribe = async ({ groupIds }) => ({
            groups: [
              withMessage(consumerGroupRow(groupIds[0]), GROUP_AUTHORIZATION_FAILED, 'no access'),
              consumerGroupRow(groupIds[0]),
            ],
          })

          const groups = await run({ consumerGroupDescribe }, [SURROGATE, LITERAL])

          expect(summary(groups)).toEqual([
            [SURROGATE, 0, 'consumer'],
            [LITERAL, 0, 'consumer'],
          ])
        })

        it('prefers an error row to a miss for the same written id, without asking the classic API', async () => {
          // A group the caller may describe but whose assigned topic it may not, next to a group on
          // another offsets shard of the same broker. Kafka 4.0.0 hands each shard both ids: the
          // owner answers the topic error in place of the group, and the other shard answers it
          // unknown while describing its own group.
          const TOPIC_AUTHORIZATION_FAILED = 29
          const describeGroups = jest.fn(classicEcho)
          const consumerGroupDescribe = async ({ groupIds }) => {
            const [team, other] = groupIds
            return {
              groups: [
                miss(team),
                consumerGroupRow(other),
                errorRow(team, TOPIC_AUTHORIZATION_FAILED, 'no access'),
                miss(other),
              ],
            }
          }

          const groups = await run({ consumerGroupDescribe, describeGroups }, [
            LITERAL,
            SURROGATE,
            'other',
          ])

          expect(summary(groups)).toEqual([
            [LITERAL, TOPIC_AUTHORIZATION_FAILED, ''],
            [SURROGATE, TOPIC_AUTHORIZATION_FAILED, ''],
            ['other', 0, 'consumer'],
          ])
          expect(describeGroups).not.toHaveBeenCalled()
        })

        // Adversarial: one shard owns a group, so no release describes one id twice in one answer.
        it('takes the first of two described rows for the same written id, in an answer no broker sends', async () => {
          const withMember = (groupId, memberId) => {
            const row = consumerGroupRow(groupId)
            return { ...row, members: [{ ...row.members[0], memberId }] }
          }
          const consumerGroupDescribe = async ({ groupIds }) => ({
            groups: [
              withMember('foreign', 'foreign'),
              withMember(groupIds[0], 'first'),
              withMember(groupIds[0], 'second'),
            ],
          })
          const encode = jest.spyOn(MemberAssignment, 'encode')
          try {
            const groups = await run({ consumerGroupDescribe }, [SURROGATE, LITERAL])

            expect(encode).toHaveBeenCalledTimes(1)
            expect(groups.map(({ groupId, members }) => [groupId, members[0].memberId])).toEqual([
              [SURROGATE, 'first'],
              [LITERAL, 'first'],
            ])
          } finally {
            encode.mockRestore()
          }
        })

        it.each([
          ['one row from Kafka 4.4.0', [], (team) => [miss(team)]],
          [
            'a row from each shard of Kafka 4.0.0',
            ['other'],
            (team, other) => [miss(team), miss(other), miss(team), consumerGroupRow(other)],
          ],
        ])(
          'asks the classic API once for a written id every row of which is a miss, with %s',
          async (_label, more, rows) => {
            const describeGroups = jest.fn(classicEcho)
            const consumerGroupDescribe = async ({ groupIds }) => ({ groups: rows(...groupIds) })

            const groups = await run({ consumerGroupDescribe, describeGroups }, [
              SURROGATE,
              LITERAL,
              SURROGATE,
              ...more,
            ])

            expect(describeGroups.mock.calls.map(([r]) => r.groupIds.map(echoOf))).toEqual([
              ['team-�'],
            ])
            expect(summary(groups).filter(([groupId]) => groupId !== 'other')).toEqual([
              [SURROGATE, 0, 'classic'],
              [LITERAL, 0, 'classic'],
            ])
          }
        )
      })

      it('answers every occurrence from one snapshot when its coordinator moves, asking again only for it', async () => {
        let moved = false
        const consumerGroupDescribe = jest.fn(async ({ groupIds }) => {
          if (!moved) {
            moved = true
            return {
              groups: groupIds.map((groupId) =>
                echoOf(groupId) === 'team-�'
                  ? {
                      ...consumerGroupRow(groupId),
                      errorCode: NOT_COORDINATOR,
                      groupState: '',
                      assignorName: null,
                      members: [],
                    }
                  : consumerGroupRow(groupId)
              ),
            }
          }
          return consumerEcho({ groupIds })
        })
        const cluster = stubCluster({
          consumerGroupDescribe,
          describeGroups: jest.fn(),
          nodeIdFor: (groupId) => (moved && echoOf(groupId) === 'team-�' ? 1 : 0),
        })
        const admin = createAdmin({ logger: newLogger(), retry, cluster })

        const { groups } = await admin.describeGroups([SURROGATE, LITERAL, SURROGATE, 'other'])

        expect(consumerGroupDescribe.mock.calls.map(([r]) => r.groupIds.map(echoOf))).toEqual([
          ['team-�', 'other'],
          ['team-�'],
        ])
        expect(summary(groups)).toEqual([
          ['other', 0, 'consumer'],
          [SURROGATE, 0, 'consumer'],
          [LITERAL, 0, 'consumer'],
          [SURROGATE, 0, 'consumer'],
        ])
      })

      it('sends an identical id given twice once and returns two rows from a consumer group and one from a classic group', async () => {
        const consumerGroupDescribe = jest.fn(consumerEcho)
        const describeGroups = jest.fn(classicEcho)

        const consumer = await run({ consumerGroupDescribe }, ['g', 'g'])
        const classic = await run({ consumerGroupDescribe: notConsumerGroup, describeGroups }, [
          'g',
          'g',
        ])

        expect(consumer.map(({ groupId }) => groupId)).toEqual(['g', 'g'])
        expect(classic.map(({ groupId }) => groupId)).toEqual(['g'])
        expect(consumerGroupDescribe.mock.calls.map(([r]) => r.groupIds)).toEqual([['g']])
        expect(describeGroups.mock.calls.map(([r]) => r.groupIds)).toEqual([['g']])
      })

      it('returns the rows of two ordinary ids as the broker named them', async () => {
        const consumerGroupDescribe = jest.fn(consumerEcho)

        const groups = await run({ consumerGroupDescribe }, ['g1', 'g2'])

        expect(summary(groups)).toEqual([
          ['g1', 0, 'consumer'],
          ['g2', 0, 'consumer'],
        ])
      })
    })
  })

  describe('WireNames', () => {
    const SURROGATE = 'team-\uD800'
    const LITERAL = 'team-�'
    const projected = (names, wireId, row, repeats) => {
      const out = []
      names.project(out, wireId, row, repeats)
      return out
    }

    it('lists distinct names as they are, and hands a row back as it is, when no name changes or repeats', () => {
      const { WireNames } = require('../wireNames')
      const names = new WireNames(['a', 'b'])
      const row = { groupId: 'a', state: 'Stable' }

      const out = projected(names, 'a', row, true)

      expect(names.ids).toEqual(['a', 'b'])
      expect(names.wire).toEqual(['a', 'b'])
      expect(out).toHaveLength(1)
      expect(out[0]).toBe(row)
    })

    it('lists each written name once, in the order it first appears', () => {
      const { WireNames } = require('../wireNames')

      const names = new WireNames([SURROGATE, 'x', LITERAL, SURROGATE])

      expect(names.ids).toEqual([LITERAL, 'x'])
      expect(names.wire).toEqual([LITERAL, 'x', LITERAL, LITERAL])
    })

    it('gives a row to every occurrence, or to every distinct spelling, never renaming the literal one', () => {
      const { WireNames } = require('../wireNames')
      const names = new WireNames([SURROGATE, LITERAL, SURROGATE])
      const row = { groupId: LITERAL, state: 'Stable' }

      const occurrences = projected(names, LITERAL, row, true)
      const spellings = projected(names, LITERAL, row, false)

      expect(occurrences.map(({ groupId }) => groupId)).toEqual([SURROGATE, LITERAL, SURROGATE])
      expect(spellings.map(({ groupId }) => groupId)).toEqual([SURROGATE, LITERAL])
      expect(occurrences[1]).toBe(row)
      expect(occurrences[0]).toEqual({ ...row, groupId: SURROGATE })
      expect(row.groupId).toBe(LITERAL)
    })

    it('refuses a row for a name the call did not write, with distinct names or repeated ones', () => {
      const { WireNames } = require('../wireNames')
      const { KafkaJSInvariantViolation } = require('../../errors')
      const row = { groupId: 'other' }

      for (const given of [
        ['a', 'b'],
        [SURROGATE, SURROGATE],
      ]) {
        const names = new WireNames(given)
        const out = []
        expect(() => names.project(out, 'other', row, true)).toThrow(KafkaJSInvariantViolation)
        expect(() => names.project(out, 'other', row, false)).toThrow(KafkaJSInvariantViolation)
        expect(out).toEqual([])
      }
    })

    it('gives each row projected from one outcome its own nested arrays, objects and buffers', () => {
      const { WireNames } = require('../wireNames')
      const names = new WireNames([SURROGATE, LITERAL])
      const row = {
        groupId: LITERAL,
        members: [
          { assignment: [{ topic: 't', partitions: [1] }], memberAssignment: Buffer.from([1, 2]) },
        ],
      }

      const [renamed, original] = projected(names, LITERAL, row, true)

      expect(renamed.members).not.toBe(original.members)
      expect(renamed.members[0]).not.toBe(original.members[0])
      expect(renamed.members[0].assignment[0].partitions).not.toBe(
        original.members[0].assignment[0].partitions
      )
      expect(renamed.members[0].memberAssignment).not.toBe(original.members[0].memberAssignment)
      expect(renamed.members[0].memberAssignment).toEqual(Buffer.from([1, 2]))
    })

    it('keeps the names as they were when the caller changes the array afterwards', () => {
      const { WireNames } = require('../wireNames')
      const given = [SURROGATE, 'x']
      const names = new WireNames(given)

      given.push('late')
      given[1] = 'changed'

      expect(names.ids).toEqual([LITERAL, 'x'])
      expect(names.names).toEqual([SURROGATE, 'x'])
    })
  })

  describe('a group id given more than once', () => {
    const SURROGATE = 'team-\uD800'
    const LITERAL = 'team-�'
    const consumerEcho = ({ groupIds }) => ({
      groups: groupIds.map((groupId) => consumerGroupRow(echoOf(groupId))),
    })
    const unknownToConsumers = ({ groupIds }) => ({
      groups: groupIds.map((groupId) => ({
        ...consumerGroupRow(echoOf(groupId)),
        errorCode: GROUP_ID_NOT_FOUND,
        errorMessage: null,
      })),
    })
    const classicEcho = ({ groupIds }) => ({
      groups: groupIds.map((groupId) => classicGroupRow(echoOf(groupId))),
    })
    const describeWith = async (stubs, ids) => {
      const cluster = stubCluster({ describeGroups: jest.fn(), ...stubs })
      const admin = createAdmin({ logger: newLogger(), retry, cluster })
      return (await admin.describeGroups(ids)).groups
    }
    /**
     * Changes every array, object and Buffer inside `value` at every depth, so a row that shares any
     * nested data with another shows it, whatever fields a description gains.
     */
    const mutateEverything = (value) => {
      if (Buffer.isBuffer(value)) {
        value.fill(0xff)
      } else if (Array.isArray(value)) {
        value.forEach(mutateEverything)
        value.length = 0
      } else if (value !== null && typeof value === 'object') {
        for (const key of Object.keys(value)) {
          if (value[key] !== null && typeof value[key] === 'object') mutateEverything(value[key])
          else value[key] = 'mutated'
        }
        value.added = 'mutated'
      }
    }
    /** Mutates one row of a call and compares each other row with a row of an untouched call. */
    const expectIndependent = async (stubs, ids, untouchedIds) => {
      const [untouched] = await describeWith(stubs, untouchedIds)
      for (let mutated = 0; mutated < ids.length; mutated++) {
        const rows = await describeWith(stubs, ids)
        mutateEverything(rows[mutated])
        expect(rows[mutated]).not.toEqual({ ...untouched, groupId: rows[mutated].groupId })
        rows.forEach((row, index) => {
          if (index !== mutated) expect(row).toEqual({ ...untouched, groupId: row.groupId })
        })
      }
    }

    it('gives each row of a consumer group its own members, assignments and buffers', async () => {
      await expectIndependent({ consumerGroupDescribe: consumerEcho }, ['g', 'g'], ['g'])
    })

    it('gives each row of a classic group its own members, assignments and buffers', async () => {
      await expectIndependent(
        { consumerGroupDescribe: unknownToConsumers, describeGroups: classicEcho },
        [SURROGATE, LITERAL],
        [LITERAL]
      )
    })

    it('gives each occurrence of a spelling given twice next to another spelling its own data', async () => {
      await expectIndependent(
        { consumerGroupDescribe: consumerEcho },
        [SURROGATE, LITERAL, SURROGATE],
        [LITERAL]
      )
    })

    it('returns the one row of a single id as the broker row was shaped, buffers included', async () => {
      const foreign = Buffer.from('not-a-consumer-assignment')
      const describeGroups = async ({ groupIds }) => ({
        groups: groupIds.map((groupId) => ({
          ...classicGroupRow(groupId),
          protocolType: 'connect',
          members: [{ ...classicGroupRow(groupId).members[0], memberAssignment: foreign }],
        })),
      })

      const [row] = await describeWith(
        { consumerGroupDescribe: unknownToConsumers, describeGroups },
        ['g']
      )

      expect(row.members[0].memberAssignment).toBe(foreign)
    })
  })

  describe('when some ids of a batch resolve and others are asked again', () => {
    const described = (groupId, memberId) => {
      const row = consumerGroupRow(groupId)
      return { ...row, members: [{ ...row.members[0], memberId }] }
    }
    // The way a broker sends a row that names a code and no group: no members.
    const codeRow = (groupId, errorCode) => ({
      ...consumerGroupRow(groupId),
      errorCode,
      groupState: '',
      assignorName: null,
      members: [],
    })
    const stubbed = ({ first, found, asked, describeGroups = jest.fn(), moves = [] }) => {
      let answered = 0
      return stubCluster({
        consumerGroupDescribe: async ({ groupIds }) => {
          asked.push([...groupIds])
          answered += 1
          return { groups: answered === 1 ? first : groupIds.map((id) => described(id, 'again')) }
        },
        describeGroups,
        onFindCoordinator: (groupId) => found.push(groupId),
        nodeIdFor: (groupId) => (answered > 0 && moves.includes(groupId) ? 1 : 0),
      })
    }

    it.each([
      ['a 16 row alone', (g1, g2) => [described(g1, 'first'), codeRow(g2, NOT_COORDINATOR)]],
      [
        'a 16 row and, from the other shard of Kafka 4.0.0, a 69 row for the same id',
        (g1, g2) => [
          described(g1, 'first'),
          { ...codeRow(g2, GROUP_ID_NOT_FOUND), errorMessage: 'no' },
          { ...codeRow(g1, GROUP_ID_NOT_FOUND), errorMessage: 'no' },
          codeRow(g2, NOT_COORDINATOR),
        ],
      ],
    ])(
      'keeps the group a coordinator described and asks again only for the one whose coordinator moved, with %s',
      async (_label, rows) => {
        const found = []
        const asked = []
        const describeGroups = jest.fn(async ({ groupIds }) => ({
          groups: groupIds.map(classicGroupRow),
        }))
        const cluster = stubbed({
          first: rows('g1', 'g2'),
          found,
          asked,
          describeGroups,
          moves: ['g2'],
        })
        const admin = createAdmin({ logger: newLogger(), retry, cluster })

        const { groups } = await admin.describeGroups(['g1', 'g2'])

        expect(asked).toEqual([['g1', 'g2'], ['g2']])
        expect(found).toEqual(['g1', 'g2', 'g2'])
        expect(describeGroups).not.toHaveBeenCalled()
        expect(
          groups.map(({ groupId, errorCode, members }) => [groupId, errorCode, members[0].memberId])
        ).toEqual([
          ['g1', 0, 'first'],
          ['g2', 0, 'again'],
        ])
      }
    )

    it('asks again only for the ids still unresolved when one is loading and another has moved', async () => {
      const found = []
      const asked = []
      const cluster = stubbed({
        first: [
          codeRow('g1', COORDINATOR_LOAD_IN_PROGRESS),
          codeRow('g2', NOT_COORDINATOR),
          described('g3', 'first'),
        ],
        found,
        asked,
        moves: ['g2'],
      })
      const refreshMetadata = jest.spyOn(cluster, 'refreshMetadata')
      jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask'] })
      try {
        const admin = createAdmin({
          logger: newLogger(),
          retry: { ...tenMsRetry, retries: 1 },
          cluster,
        })
        const outcome = observe(admin.describeGroups(['g1', 'g2', 'g3']))

        await jest.advanceTimersByTimeAsync(0)
        await new Promise(setImmediate)
        await jest.advanceTimersByTimeAsync(9)
        expect(asked).toEqual([['g1', 'g2', 'g3']])
        expect(found).toEqual(['g1', 'g2', 'g3'])
        expect(refreshMetadata).not.toHaveBeenCalled()

        await jest.advanceTimersByTimeAsync(1)
        await new Promise(setImmediate)
        expect(outcome.settled).toBe(true)
        expect(outcome.error).toBeUndefined()
        expect(asked.slice(1).sort()).toEqual([['g1'], ['g2']])
        expect(found).toEqual(['g1', 'g2', 'g3', 'g2'])
        expect(refreshMetadata).toHaveBeenCalledTimes(1)
        expect(
          outcome.value.groups
            .map(({ groupId, errorCode, members }) => [groupId, errorCode, members[0].memberId])
            .sort()
        ).toEqual([
          ['g1', 0, 'again'],
          ['g2', 0, 'again'],
          ['g3', 0, 'first'],
        ])
      } finally {
        jest.useRealTimers()
      }
    })

    it('does not ask a healthy coordinator again when another coordinator fails', async () => {
      const asked = []
      let failed = false
      const cluster = stubCluster({
        consumerGroupDescribe: async ({ groupIds }) => {
          asked.push([...groupIds])
          if (groupIds.includes('g2') && !failed) {
            failed = true
            throw new Error('the connection went away')
          }
          return { groups: groupIds.map((groupId) => consumerGroupRow(groupId)) }
        },
        describeGroups: jest.fn(),
        nodeIdFor: (groupId) => (groupId === 'g2' ? 1 : 0),
      })
      const admin = createAdmin({ logger: newLogger(), retry, cluster })

      const { groups } = await admin.describeGroups(['g1', 'g2'])

      expect(asked.sort()).toEqual([['g1'], ['g2'], ['g2']])
      expect(groups.map(({ groupId, errorCode }) => [groupId, errorCode])).toEqual([
        ['g1', 0],
        ['g2', 0],
      ])
    })
  })

  it('describes an ordinary Unicode group id with the id sent and returned unchanged', async () => {
    const id = 'über-\u{1F600}'
    const consumerGroupDescribe = jest.fn(async ({ groupIds }) => ({
      groups: groupIds.map((groupId) => consumerGroupRow(echoOf(groupId))),
    }))
    const cluster = stubCluster({ consumerGroupDescribe, describeGroups: jest.fn() })
    const admin = createAdmin({ logger: newLogger(), retry, cluster })

    const { groups } = await admin.describeGroups([id])

    expect(consumerGroupDescribe.mock.calls[0][0].groupIds).toEqual([id])
    expect(groups.map(({ groupId }) => groupId)).toEqual([id])
  })
})

const logger = { namespace: () => logger, debug() {} }
test('does not duplicate an in-flight successful split classic batch', async () => {
  jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask'] })
  const a = {
    nodeId: 1,
    describeGroups: jest
      .fn()
      .mockRejectedValueOnce(createErrorFromCode(16))
      .mockResolvedValue({ groups: [{ groupId: 'a', errorCode: 0, members: [] }] }),
  }
  const b = {
    nodeId: 2,
    describeGroups: jest.fn(
      () =>
        new Promise((resolve) =>
          setTimeout(() => resolve({ groups: [{ groupId: 'b', errorCode: 0, members: [] }] }), 20)
        )
    ),
  }
  const original = {
    nodeId: 0,
    consumerGroupDescribe: async () => ({
      groups: ['a', 'b'].map((groupId) => ({ groupId, errorCode: 69 })),
    }),
    describeGroups: jest.fn().mockRejectedValue(createErrorFromCode(16)),
  }
  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    withCoordinatorBroker: (nodeId, work) => work([original, a, b][nodeId]),
    findGroupCoordinator: jest
      .fn()
      .mockResolvedValueOnce(original)
      .mockResolvedValueOnce(original)
      .mockImplementation(async ({ groupId }) => (groupId === 'a' ? a : b)),
  }
  try {
    const outcome = observe(
      createGroupDescriptions({
        cluster,
        logger,
        retry: { retries: 3, initialRetryTime: 1, factor: 0, multiplier: 1 },
      }).describe(['a', 'b'])
    )
    await driveUntilSettled(outcome)
    expect(outcome.error).toBeUndefined()
    expect(outcome.value.groups.map((row) => row.groupId).sort()).toEqual(['a', 'b'])
    expect(b.describeGroups).toHaveBeenCalledTimes(1)
  } finally {
    jest.useRealTimers()
  }
})

/** Records how `promise` settles, so a test can move fake time on and read it afterwards. */
const observe = (promise) => {
  const outcome = {}
  promise.then(
    (value) => Object.assign(outcome, { settled: true, value }),
    (error) => Object.assign(outcome, { settled: true, error })
  )
  return outcome
}

/** Lets real immediates and fake timers run in turn, 10 ms at a time, until `outcome` settles. */
const driveUntilSettled = async (outcome) => {
  for (let step = 0; step < 100 && !outcome.settled; step++) {
    await new Promise(setImmediate)
    await jest.advanceTimersByTimeAsync(10)
  }
}

/** A fake-time wait. */
const after = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/** Every retry of these tests waits exactly 10 ms. */
const tenMsRetry = { retries: 2, initialRetryTime: 10, maxRetryTime: 10, factor: 0, multiplier: 1 }

/** A described consumer group whose one member is named `memberId`. */
const describedWith = (groupId, memberId) => {
  const row = consumerGroupRow(groupId)
  return { ...row, members: [{ ...row.members[0], memberId }] }
}

describe('Admin > describeGroups > a DescribeGroups answer read row by row', () => {
  let harness

  beforeEach(() => {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask'] })
  })
  afterEach(async () => {
    jest.useRealTimers()
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  /** Reads the ids carried by the requested DescribeGroups version. */
  const askedIn = (body, version) => {
    const decoder = new Decoder(body)
    return version === 6
      ? decoder.readUVarIntArray((entry) => entry.readUVarIntString())
      : decoder.readArray((entry) => entry.readString())
  }

  /**
   * A DescribeGroups v6 response body: a described group with one member named `memberId`, or a
   * row with only `errorCode` set, which is how a coordinator answers the ids of an offsets shard it
   * cannot read (`DescribeGroupsRequest.getErrorDescribedGroupList`).
   */
  const answerOf = (rows) =>
    new Encoder()
      .writeInt32(0)
      .writeUVarIntArray(
        rows.map(({ groupId, errorCode = 0, memberId }) => {
          const described = errorCode === 0
          const members = described
            ? [
                new Encoder()
                  .writeUVarIntString(memberId)
                  .writeUVarIntString(null)
                  .writeUVarIntString('client')
                  .writeUVarIntString('/127.0.0.1')
                  .writeUVarIntBytes(Buffer.alloc(0))
                  .writeUVarIntBytes(Buffer.alloc(0))
                  .writeTaggedFields(),
              ]
            : []
          return new Encoder()
            .writeInt16(errorCode)
            .writeUVarIntString(null)
            .writeUVarIntString(groupId)
            .writeUVarIntString(described ? 'Stable' : '')
            .writeUVarIntString(described ? 'consumer' : '')
            .writeUVarIntString(described ? 'range' : '')
            .writeUVarIntArray(members, 'object')
            .writeInt32(-2147483648)
            .writeTaggedFields()
        }),
        'object'
      )
      .writeTaggedFields().buffer

  /**
   * A v2 description with a stable group or a group-level error and no members.
   * @param {object[]} rows group ids and optional broker error codes
   * @returns {Buffer} DescribeGroups v2 response body
   */
  const answerV2Of = (rows) => {
    const groups = rows.map(({ groupId, errorCode = 0 }) =>
      new Encoder()
        .writeInt16(errorCode)
        .writeString(groupId)
        .writeString(errorCode === 0 ? 'Stable' : '')
        .writeString(errorCode === 0 ? 'consumer' : '')
        .writeString(errorCode === 0 ? 'range' : '')
        .writeArray([])
    )
    return new Encoder().writeInt32(0).writeArray(groups, 'object').buffer
  }

  /**
   * An admin over one real broker on a fake socket that offers the selected DescribeGroups and no
   * ConsumerGroupDescribe, so every id is described through DescribeGroups. `answer(asked, index)`
   * gives the rows of the `index`-th request for the ids it asked.
   */
  const adminOver = (answer, version = 6) => {
    const found = []
    harness = createGroupApiBroker({
      versions: { [apiKeys.DescribeGroups]: { minVersion: 0, maxVersion: version } },
      respond: () => {
        const index = harness.requests.length - 1
        const rows = answer(askedIn(harness.requests[index].body, version), index)
        return version === 6 ? answerOf(rows) : answerV2Of(rows)
      },
    })
    harness.broker.nodeId = 0
    const cluster = {
      refreshMetadata: jest.fn(async () => {}),
      withCoordinatorBroker: (_nodeId, work) => work(harness.broker),
      findGroupCoordinator: jest.fn(async ({ groupId }) => {
        found.push(groupId)
        return harness.broker
      }),
    }
    return {
      found,
      cluster,
      admin: createAdmin({ logger: newLogger(), retry: tenMsRetry, cluster }),
    }
  }

  const sent = (version = 6) =>
    harness.requests.map(({ apiKey, body }) => [apiKey, askedIn(body, version)])

  test.each([
    ['COORDINATOR_LOAD_IN_PROGRESS', COORDINATOR_LOAD_IN_PROGRESS, ['a', 'b'], 0],
    ['COORDINATOR_NOT_AVAILABLE', COORDINATOR_NOT_AVAILABLE, ['a', 'b', 'b'], 1],
    ['NOT_COORDINATOR', NOT_COORDINATOR, ['a', 'b', 'b'], 1],
  ])(
    'keeps the group an answer describes when the same answer gives its sibling %s, and asks again only for the sibling',
    async (_name, code, lookups, refreshes) => {
      const { admin, found, cluster } = adminOver((asked, index) =>
        asked.map((groupId) =>
          index === 0 && groupId === 'b'
            ? { groupId, errorCode: code }
            : { groupId, memberId: index === 0 ? 'first' : 'again' }
        )
      )

      const outcome = observe(admin.describeGroups(['a', 'b']))
      await driveUntilSettled(outcome)

      expect(outcome.error).toBeUndefined()
      expect(sent()).toEqual([
        [apiKeys.DescribeGroups, ['a', 'b']],
        [apiKeys.DescribeGroups, ['b']],
      ])
      expect(found).toEqual(lookups)
      // A move refreshes metadata once before the moved id is looked up again; a load does not.
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(refreshes)
      if (refreshes) {
        expect(cluster.refreshMetadata.mock.invocationCallOrder[0]).toBeLessThan(
          cluster.findGroupCoordinator.mock.invocationCallOrder[2]
        )
      }
      expect(
        outcome.value.groups.map(({ groupId, groupType, errorCode, members }) => [
          groupId,
          groupType,
          errorCode,
          members[0].memberId,
        ])
      ).toEqual([
        ['a', 'classic', 0, 'first'],
        ['b', 'classic', 0, 'again'],
      ])
    }
  )

  test('keeps the settled v2 description while retrying only its loading sibling', async () => {
    const { admin } = adminOver(
      (asked, index) =>
        asked.map((groupId) => ({
          groupId,
          errorCode: index === 0 && groupId === 'b' ? COORDINATOR_LOAD_IN_PROGRESS : 0,
        })),
      2
    )

    const outcome = observe(admin.describeGroups(['a', 'b']))
    await driveUntilSettled(outcome)

    expect(outcome.error).toBeUndefined()
    expect(outcome.value.groups.map(({ groupId, state }) => [groupId, state])).toEqual([
      ['a', 'Stable'],
      ['b', 'Stable'],
    ])
    expect(sent(2)).toEqual([
      [apiKeys.DescribeGroups, ['a', 'b']],
      [apiKeys.DescribeGroups, ['b']],
    ])
  })

  test('fails the call on a GROUP_AUTHORIZATION_FAILED row next to a described one, sending nothing again', async () => {
    const { admin } = adminOver((asked) =>
      asked.map((groupId) =>
        groupId === 'b'
          ? { groupId, errorCode: GROUP_AUTHORIZATION_FAILED }
          : { groupId, memberId: 'first' }
      )
    )

    const outcome = observe(admin.describeGroups(['a', 'b']))
    await driveUntilSettled(outcome)

    expect(outcome.error).toMatchObject({
      name: 'KafkaJSNonRetriableError',
      type: 'GROUP_AUTHORIZATION_FAILED',
      code: GROUP_AUTHORIZATION_FAILED,
    })
    expect(sent()).toEqual([[apiKeys.DescribeGroups, ['a', 'b']]])
  })

  test('gives up on the retry budget when DescribeGroups keeps answering COORDINATOR_LOAD_IN_PROGRESS', async () => {
    const { admin, found, cluster } = adminOver((asked) =>
      asked.map((groupId) => ({ groupId, errorCode: COORDINATOR_LOAD_IN_PROGRESS }))
    )

    const outcome = observe(admin.describeGroups(['b']))
    await driveUntilSettled(outcome)

    expect(outcome.error).toMatchObject({
      name: 'KafkaJSNumberOfRetriesExceeded',
      type: 'GROUP_LOAD_IN_PROGRESS',
      code: COORDINATOR_LOAD_IN_PROGRESS,
    })
    expect(sent()).toEqual([
      [apiKeys.DescribeGroups, ['b']],
      [apiKeys.DescribeGroups, ['b']],
      [apiKeys.DescribeGroups, ['b']],
    ])
    expect(found).toEqual(['b'])
    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
  })
})

describe('Admin > describeGroups > two coordinators', () => {
  beforeEach(() => {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask'] })
  })
  afterEach(() => {
    jest.useRealTimers()
  })

  /** `a` routed to node 0 and every other id to node 1, each node with its own stubs. */
  const twoCoordinators = (forA, forOthers, adminOptions = {}) => {
    const found = []
    const nodes = [
      { nodeId: 0, describeGroups: jest.fn(), ...forA },
      { nodeId: 1, describeGroups: jest.fn(), ...forOthers },
    ]
    const cluster = {
      refreshMetadata: jest.fn(async () => {}),
      withCoordinatorBroker: (nodeId, work) => work(nodes[nodeId]),
      findGroupCoordinator: jest.fn(async ({ groupId }) => {
        found.push(groupId)
        return nodes[groupId === 'a' ? 0 : 1]
      }),
    }
    const admin = createAdmin({ logger: newLogger(), retry: tenMsRetry, cluster, ...adminOptions })
    return { admin, cluster, found, nodes }
  }

  const notConsumerGroups = async ({ groupIds }) => ({
    groups: groupIds.map((groupId) => ({
      ...consumerGroupRow(groupId),
      errorCode: GROUP_ID_NOT_FOUND,
      errorMessage: 'not a consumer group',
    })),
  })

  const orders = [
    ['the coordinator of a answering first', 10, 20],
    ['the other coordinator answering first', 20, 10],
  ]

  // The refusal comes from DescribeGroups' own rule as a request without the row-by-row option
  // gives it; the row form is driven through a real broker above.
  test.each([
    ['the refusal arriving last', 5, 10],
    ['the refusal arriving first', 20, 10],
  ])(
    'ends the call with the GROUP_AUTHORIZATION_FAILED one coordinator answers while another loses its connection, with %s',
    async (_label, connectionAfter, refusalAfter) => {
      const { admin, nodes } = twoCoordinators(
        {
          consumerGroupDescribe: notConsumerGroups,
          describeGroups: jest.fn(async () => {
            await after(connectionAfter)
            throw new KafkaJSConnectionError('Connection error: the connection went away')
          }),
        },
        {
          consumerGroupDescribe: notConsumerGroups,
          describeGroups: jest.fn(async ({ groupIds }) => {
            await after(refusalAfter)
            return parseDescribeGroupsV6({
              groups: groupIds.map((groupId) => ({
                ...classicGroupRow(groupId),
                errorCode: GROUP_AUTHORIZATION_FAILED,
                members: [],
              })),
            })
          }),
        }
      )

      const outcome = observe(admin.describeGroups(['a', 'b']))
      await driveUntilSettled(outcome)

      expect(outcome.error).toMatchObject({
        name: 'KafkaJSNonRetriableError',
        type: 'GROUP_AUTHORIZATION_FAILED',
        code: GROUP_AUTHORIZATION_FAILED,
      })
      expect(nodes[1].describeGroups).toHaveBeenCalledTimes(1)
      expect(nodes[0].describeGroups).toHaveBeenCalledTimes(1)
    }
  )

  test.each(orders)(
    'keeps the description of a from its own coordinator when another coordinator also describes a, with %s',
    async (_label, aAfter, othersAfter) => {
      const { admin, nodes } = twoCoordinators(
        {
          consumerGroupDescribe: jest.fn(async ({ groupIds }) => {
            await after(aAfter)
            return { groups: groupIds.map((groupId) => describedWith(groupId, 'from-a')) }
          }),
        },
        {
          consumerGroupDescribe: jest.fn(async ({ groupIds }) => {
            await after(othersAfter)
            return {
              groups: [
                ...groupIds.map((groupId) => describedWith(groupId, 'from-b')),
                describedWith('a', 'from-b'),
              ],
            }
          }),
        }
      )

      const outcome = observe(admin.describeGroups(['a', 'b']))
      await driveUntilSettled(outcome)

      expect(outcome.error).toBeUndefined()
      expect(
        outcome.value.groups.map(({ groupId, members }) => [groupId, members[0].memberId]).sort()
      ).toEqual([
        ['a', 'from-a'],
        ['b', 'from-b'],
      ])
      expect(nodes[0].consumerGroupDescribe).toHaveBeenCalledTimes(1)
      expect(nodes[1].consumerGroupDescribe).toHaveBeenCalledTimes(1)
    }
  )

  test.each(orders)(
    'does not ask again for a, which its own coordinator described, when another coordinator answers a NOT_COORDINATOR, with %s',
    async (_label, aAfter, othersAfter) => {
      const { admin, cluster, found, nodes } = twoCoordinators(
        {
          consumerGroupDescribe: jest.fn(async ({ groupIds }) => {
            await after(aAfter)
            return { groups: groupIds.map((groupId) => describedWith(groupId, 'from-a')) }
          }),
        },
        {
          consumerGroupDescribe: jest.fn(async ({ groupIds }) => {
            await after(othersAfter)
            return {
              groups: [
                ...groupIds.map((groupId) => describedWith(groupId, 'from-b')),
                { ...consumerGroupRow('a'), errorCode: NOT_COORDINATOR, members: [] },
              ],
            }
          }),
        }
      )

      const outcome = observe(admin.describeGroups(['a', 'b']))
      await driveUntilSettled(outcome)

      expect(outcome.error).toBeUndefined()
      expect(nodes[0].consumerGroupDescribe).toHaveBeenCalledTimes(1)
      expect(nodes[1].consumerGroupDescribe).toHaveBeenCalledTimes(1)
      expect(found).toEqual(['a', 'b'])
      expect(cluster.refreshMetadata).not.toHaveBeenCalled()
      expect(
        outcome.value.groups.map(({ groupId, members }) => [groupId, members[0].memberId]).sort()
      ).toEqual([
        ['a', 'from-a'],
        ['b', 'from-b'],
      ])
    }
  )

  test('neither describes nor asks the older API about ids a coordinator answers without being asked', async () => {
    const { admin, found, nodes } = twoCoordinators(
      {
        consumerGroupDescribe: jest.fn(async () => ({
          groups: [
            describedWith('a', 'from-a'),
            { ...consumerGroupRow('x'), errorCode: GROUP_ID_NOT_FOUND, errorMessage: 'no' },
            describedWith('y', 'from-a'),
          ],
        })),
        describeGroups: jest.fn(async ({ groupIds }) => ({
          groups: groupIds.map(classicGroupRow),
        })),
      },
      {
        describeGroups: jest.fn(async ({ groupIds }) => ({
          groups: groupIds.map(classicGroupRow),
        })),
      }
    )

    const outcome = observe(admin.describeGroups(['a']))
    await driveUntilSettled(outcome)

    expect(outcome.error).toBeUndefined()
    expect(outcome.value.groups.map(({ groupId, groupType }) => [groupId, groupType])).toEqual([
      ['a', 'consumer'],
    ])
    expect(nodes[0].describeGroups).not.toHaveBeenCalled()
    expect(nodes[1].describeGroups).not.toHaveBeenCalled()
    expect(found).toEqual(['a'])
  })

  test('retries a lookup and describes its group while another lookup is pending', async () => {
    const found = []
    const node = {
      nodeId: 0,
      consumerGroupDescribe: jest.fn(async ({ groupIds }) => ({
        groups: groupIds.map(consumerGroupRow),
      })),
    }
    const cluster = {
      withCoordinatorBroker: (_nodeId, work) => work(node),
      findGroupCoordinator: jest.fn(async ({ groupId }) => {
        found.push(groupId)
        if (found.length === 1) throw new KafkaJSError('The coordinator is not available yet')
        if (groupId === 'b') await after(40)
        return node
      }),
    }
    const admin = createAdmin({ logger: newLogger(), retry: tenMsRetry, cluster })

    const outcome = observe(admin.describeGroups(['a', 'b']))
    for (let step = 0; step < 3; step++) {
      await new Promise(setImmediate)
      await jest.advanceTimersByTimeAsync(10)
    }
    const early = node.consumerGroupDescribe.mock.calls.map(([request]) => request.groupIds)
    await driveUntilSettled(outcome)

    expect(early).toEqual([['a']])
    expect(outcome.error).toBeUndefined()
    expect(found).toEqual(['a', 'b', 'a'])
    expect(node.consumerGroupDescribe.mock.calls.map(([request]) => request.groupIds)).toEqual([
      ['a'],
      ['b'],
    ])
  })

  test('preserves retry exhaustion from coordinator discovery', async () => {
    const exhausted = new KafkaJSNumberOfRetriesExceeded(createErrorFromCode(34), {
      retryCount: 0,
      retryTime: 10,
    })
    const cluster = {
      findGroupCoordinator: jest.fn().mockRejectedValue(exhausted),
      withCoordinatorBroker: jest.fn(),
    }
    const admin = createAdmin({ logger: newLogger(), retry: tenMsRetry, cluster })

    await expect(admin.describeGroups(['a'])).rejects.toBe(exhausted)
    expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
    expect(cluster.withCoordinatorBroker).not.toHaveBeenCalled()
  })

  /**
   * A coordinator that describes through the chosen API. Classic rows carry the same versioned
   * error the internal result reader returns in the wire tests.
   */
  const describeVia = (api, answer) =>
    api === 'consumer'
      ? { consumerGroupDescribe: jest.fn(answer) }
      : { consumerGroupDescribe: notConsumerGroups, describeGroups: jest.fn(answer) }

  test.each(['consumer', 'classic'])(
    'keeps the %s retry budget across coordinator moves',
    async (api) => {
      const lookedUp = []
      const sentTo = []
      const row = api === 'consumer' ? consumerGroupRow : classicGroupRow
      const cluster = stubCluster({
        ...describeVia(api, async function ({ groupIds }) {
          sentTo.push(this.nodeId)
          return {
            groups: groupIds.map((id) => ({
              ...row(id),
              errorCode: NOT_COORDINATOR,
              members: [],
              ...(api === 'classic' && { error: createErrorFromCode(NOT_COORDINATOR) }),
            })),
          }
        }),
        onFindCoordinator: (id) => lookedUp.push(id),
        nodeIdFor: () => lookedUp.length,
      })
      const admin = createAdmin({ logger: newLogger(), retry: tenMsRetry, cluster })
      const outcome = observe(admin.describeGroups(['a']))
      await driveUntilSettled(outcome)

      expect(lookedUp).toEqual(['a', 'a', 'a'])
      expect(sentTo).toEqual([1, 2, 3])
      expect(outcome.error).toMatchObject({
        name: 'KafkaJSNumberOfRetriesExceeded',
        retryCount: 2,
        code: NOT_COORDINATOR,
      })
    }
  )

  test.each(['consumer', 'classic'])(
    'retries a loading %s group before a held sibling answers near the deadline',
    async (api) => {
      let releaseSibling
      const held = new Promise((resolve) => {
        releaseSibling = resolve
      })
      const row = api === 'consumer' ? consumerGroupRow : classicGroupRow
      const method = api === 'consumer' ? 'consumerGroupDescribe' : 'describeGroups'
      let first = true
      const retriesWhileHeld = []
      let siblingInFlight = false
      let released = false
      const { admin, nodes } = twoCoordinators(
        describeVia(api, async ({ groupIds }) => {
          const [groupId] = groupIds
          if (!first) {
            retriesWhileHeld.push(siblingInFlight && !released)
            return { groups: [row(groupId)] }
          }
          first = false
          const errorCode = COORDINATOR_LOAD_IN_PROGRESS
          return {
            groups: [
              {
                ...row(groupId),
                errorCode,
                members: [],
                ...(api === 'classic' && { error: createErrorFromCode(errorCode) }),
              },
            ],
          }
        }),
        describeVia(api, () => {
          siblingInFlight = true
          return held
        }),
        { apiTimeout: 60 }
      )

      const outcome = observe(admin.describeGroups(['a', 'b']))
      for (let step = 0; step < 5; step++) {
        await new Promise(setImmediate)
        await jest.advanceTimersByTimeAsync(10)
      }
      await jest.advanceTimersByTimeAsync(5)
      const siblingWasAsked = nodes[1][method].mock.calls.length === 1
      released = true
      releaseSibling({ groups: [row('b')] })
      await driveUntilSettled(outcome)

      expect(siblingWasAsked).toBe(true)
      expect(retriesWhileHeld).toEqual([true])
      expect(outcome.error).toBeUndefined()
      expect(outcome.value.groups.map(({ groupId }) => groupId)).toEqual(['a', 'b'])
      expect(nodes[0][method]).toHaveBeenCalledTimes(2)
      expect(nodes[1][method]).toHaveBeenCalledTimes(1)
    }
  )

  test('starts a ready classic fallback while another consumer description is in flight', async () => {
    let releaseSibling
    const held = new Promise((resolve) => {
      releaseSibling = resolve
    })
    const { admin, nodes } = twoCoordinators(
      describeVia('classic', async ({ groupIds }) => ({ groups: groupIds.map(classicGroupRow) })),
      describeVia('consumer', () => held)
    )
    const outcome = observe(admin.describeGroups(['a', 'b']))
    for (let step = 0; step < 3; step++) {
      await new Promise(setImmediate)
      await jest.advanceTimersByTimeAsync(10)
    }
    const fallbackStarted = nodes[0].describeGroups.mock.calls.length === 1
    releaseSibling({ groups: [consumerGroupRow('b')] })
    await driveUntilSettled(outcome)

    expect(fallbackStarted).toBe(true)
    expect(outcome.error).toBeUndefined()
    expect(outcome.value.groups.map(({ groupId, groupType }) => [groupId, groupType])).toEqual([
      ['b', 'consumer'],
      ['a', 'classic'],
    ])
  })

  test('backs off on a request timeout without delaying another coordinator retry', async () => {
    const events = []
    let failed = false
    let loading = true
    const { admin, nodes } = twoCoordinators(
      {
        consumerGroupDescribe: jest.fn(async ({ groupIds }) => {
          if (!failed) {
            failed = true
            await after(50)
            events.push('a timed out')
            throw new KafkaJSRequestTimeoutError('Request ConsumerGroupDescribe timed out')
          }
          events.push('a retried')
          return { groups: groupIds.map(consumerGroupRow) }
        }),
      },
      {
        consumerGroupDescribe: jest.fn(async ({ groupIds }) => {
          if (loading) {
            loading = false
            return {
              groups: groupIds.map((id) => ({
                ...consumerGroupRow(id),
                errorCode: COORDINATOR_LOAD_IN_PROGRESS,
              })),
            }
          }
          events.push('b retried')
          return { groups: groupIds.map(consumerGroupRow) }
        }),
      }
    )

    const outcome = observe(admin.describeGroups(['a', 'b']))
    await driveUntilSettled(outcome)

    expect(outcome.error).toBeUndefined()
    expect(nodes[0].consumerGroupDescribe).toHaveBeenCalledTimes(2)
    expect(nodes[1].consumerGroupDescribe).toHaveBeenCalledTimes(2)
    expect(events).toEqual(['b retried', 'a timed out', 'a retried'])
    expect(outcome.value.groups.map(({ groupId, errorCode }) => [groupId, errorCode])).toEqual([
      ['b', 0],
      ['a', 0],
    ])
  })

  test('ends the call with the GROUP_AUTHORIZATION_FAILED one coordinator answered before the deadline while the request of another is held past it', async () => {
    let releaseA
    const heldA = new Promise((resolve) => {
      releaseA = resolve
    })
    const { admin, nodes } = twoCoordinators(
      {
        consumerGroupDescribe: notConsumerGroups,
        describeGroups: jest.fn(() => heldA),
      },
      {
        consumerGroupDescribe: notConsumerGroups,
        describeGroups: jest.fn(async ({ groupIds }) => {
          await after(10)
          return parseDescribeGroupsV6({
            groups: groupIds.map((groupId) => ({
              ...classicGroupRow(groupId),
              errorCode: GROUP_AUTHORIZATION_FAILED,
              members: [],
            })),
          })
        }),
      },
      { apiTimeout: 50 }
    )

    const outcome = observe(admin.describeGroups(['a', 'b']))
    await driveUntilSettled(outcome)

    expect(outcome.error).toMatchObject({
      name: 'KafkaJSNonRetriableError',
      type: 'GROUP_AUTHORIZATION_FAILED',
      code: GROUP_AUTHORIZATION_FAILED,
    })

    // The held request answers late: nothing changes and nothing is sent again.
    releaseA({ groups: [classicGroupRow('a')] })
    for (let step = 0; step < 5; step++) {
      await new Promise(setImmediate)
      await jest.advanceTimersByTimeAsync(100)
    }
    expect(outcome.error).toMatchObject({ code: GROUP_AUTHORIZATION_FAILED })
    expect(outcome.value).toBeUndefined()
    expect(nodes[0].describeGroups).toHaveBeenCalledTimes(1)
    expect(nodes[1].describeGroups).toHaveBeenCalledTimes(1)
  })

  test('does not start the classic fallback once the deadline passed during the consumer-protocol request', async () => {
    const { admin, nodes } = twoCoordinators(
      {
        consumerGroupDescribe: jest.fn(async (request) => {
          await after(40)
          return notConsumerGroups(request)
        }),
        describeGroups: jest.fn(async ({ groupIds }) => ({
          groups: groupIds.map(classicGroupRow),
        })),
      },
      {},
      { apiTimeout: 30 }
    )

    const outcome = observe(admin.describeGroups(['a']))
    await driveUntilSettled(outcome)
    for (let step = 0; step < 5; step++) {
      await new Promise(setImmediate)
      await jest.advanceTimersByTimeAsync(100)
    }

    expect(outcome.error).toBeInstanceOf(KafkaJSTimeout)
    expect(outcome.value).toBeUndefined()
    expect(nodes[0].consumerGroupDescribe).toHaveBeenCalledTimes(1)
    expect(nodes[0].describeGroups).not.toHaveBeenCalled()
  })
})

describe('Admin > describeGroups > a coordinator node that moved to another address', () => {
  const { Kafka } = require('../../..')
  const Broker = require('../../broker')
  const Cluster = require('../../cluster')
  const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')

  // The scripted sockets deliver data and closes through queueMicrotask, which must stay real.
  beforeEach(() => {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask'] })
  })
  afterEach(() => {
    jest.restoreAllMocks()
    jest.useRealTimers()
  })

  const A = { id: 0, host: 'node-a', port: 9092 }
  const OLD = { id: 1, host: 'node-b-old', port: 9092 }
  const NEW = { id: 1, host: 'node-b-new', port: 9092 }

  /**
   * A real Kafka admin over a cluster whose node 1 is the coordinator of every group, at `OLD`
   * until `state.moved`, then at `NEW`; both addresses stay up and answer as node 1. Metadata and
   * FindCoordinator name node 1 at its current address. The description is answered by
   * `answer(host, groupId, call, state)` on the real broker the client picked, by that broker's
   * address; `metadataSeen` holds how many Metadata requests had been written at each such call.
   */
  const movingCluster = (answer, { api = 'consumer', mixed = false, failRefresh = false } = {}) => {
    const state = { moved: false }
    const at = () => (state.moved ? NEW : OLD)
    const scripted = createScriptedCluster({ nodes: [A, OLD, NEW] })
      .answer('Metadata', () => ({
        brokers: [A, at()].map(({ id, host, port }) => ({ nodeId: id, host, port, rack: null })),
        controllerId: A.id,
        topics: [],
      }))
      .answer('FindCoordinator', ({ coordinatorKeys }) => ({
        coordinators: coordinatorKeys.map((key) => ({
          key,
          nodeId: at().id,
          host: at().host,
          port: at().port,
          errorCode: 0,
          errorMessage: null,
        })),
      }))
    const asked = []
    const metadataSeen = []
    const metadataRequests = () => scripted.requests().filter(({ api }) => api === 'Metadata')
    const find = Cluster.prototype.findGroupCoordinator
    const initial = []
    let initialReady
    const lookups = jest
      .spyOn(Cluster.prototype, 'findGroupCoordinator')
      .mockImplementation(function (request) {
        const found = find.call(this, request)
        if (!mixed || state.moved) return found
        // Release both initial, real discoveries in request order to exercise one mixed batch.
        const index = initial.push(found) - 1
        initialReady ??= Promise.resolve().then(() => Promise.all(initial))
        return initialReady.then((brokers) => brokers[index])
      })
    const refresh = Cluster.prototype.refreshMetadata
    const refreshes = []
    jest.spyOn(Cluster.prototype, 'refreshMetadata').mockImplementation(async function (...args) {
      if (!state.moved) return refresh.apply(this, args)
      if (failRefresh && refreshes.length === 0) {
        refreshes.push('failed')
        throw new KafkaJSConnectionError('Metadata transport failed')
      }
      await refresh.apply(this, args)
      refreshes.push('completed')
    })
    if (api === 'classic') {
      jest
        .spyOn(Broker.prototype, 'consumerGroupDescribe')
        .mockImplementation(async ({ groupIds }) => ({
          groups: groupIds.map((groupId) => ({ groupId, errorCode: GROUP_ID_NOT_FOUND })),
        }))
    }
    const method = api === 'classic' ? 'describeGroups' : 'consumerGroupDescribe'
    jest.spyOn(Broker.prototype, method).mockImplementation(async function ({ groupIds }) {
      const { host } = this.connectionPool
      if (host === OLD.host) state.oldBroker ??= this
      if (host === NEW.host) state.oldRetired = state.oldBroker.retired
      asked.push([host, ...groupIds])
      metadataSeen.push(metadataRequests().length)
      return {
        groups: groupIds
          .flatMap((groupId) => answer(host, groupId, asked.length, state, groupIds))
          .map((row) =>
            api === 'classic' && row.errorCode !== 0
              ? { ...row, error: createErrorFromCode(row.errorCode) }
              : row
          ),
      }
    })
    const admin = new Kafka(
      scripted.kafkaOptions({
        brokers: [`${A.host}:${A.port}`],
        retry: mixed ? { ...tenMsRetry, retries: 5 } : tenMsRetry,
      })
    ).admin()
    return { scripted, admin, state, asked, metadataSeen, metadataRequests, refreshes, lookups }
  }

  const run = async ({ scripted, admin }, work) => {
    try {
      await admin.connect()
      const outcome = observe(work())
      await driveUntilSettled(outcome)
      return outcome
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  }

  test('sends the next request to the new address after the old address answers NOT_COORDINATOR, with one metadata refresh', async () => {
    const fixture = movingCluster((host, groupId, _call, state) => {
      if (host !== OLD.host) return [consumerGroupRow(groupId)]
      // The coordinator moves while its old address still answers: from this answer on, Metadata
      // and FindCoordinator name node 1 at the new address, which the client has not learned yet.
      state.moved = true
      return [{ ...consumerGroupRow(groupId), errorCode: NOT_COORDINATOR, members: [] }]
    })
    const outcome = await run(fixture, () => fixture.admin.describeGroups(['g']))

    expect(outcome.error).toBeUndefined()
    // The client knew node 1 at the old address, so its first request went there and got 16...
    expect(fixture.asked).toEqual([
      [OLD.host, 'g'],
      [NEW.host, 'g'],
    ])
    expect(fixture.state.moved).toBe(true)
    // ...then exactly one Metadata request, served after the 16, moved node 1 to the new address
    // before the next request, and none followed.
    expect(fixture.metadataSeen[1] - fixture.metadataSeen[0]).toBe(1)
    expect(fixture.metadataRequests()).toHaveLength(fixture.metadataSeen[1])
    expect(outcome.value.groups.map(({ groupId, errorCode }) => [groupId, errorCode])).toEqual([
      ['g', 0],
    ])
  })

  test.each([
    ['consumer', 'loading first', ['load', 'move'], false],
    ['consumer', 'moved first', ['move', 'load'], false],
    ['classic', 'loading first', ['load', 'move'], false],
    ['classic', 'moved first', ['move', 'load'], false],
    ['consumer', 'a failed refresh', ['load', 'move'], true],
    ['classic', 'a failed refresh', ['load', 'move'], true],
  ])(
    'uses the current broker for a pending %s sibling after %s',
    async (api, _order, ids, failRefresh) => {
      const row = api === 'consumer' ? consumerGroupRow : classicGroupRow
      let movedDescribed = false
      const fixture = movingCluster(
        (host, groupId, _call, state, groupIds) => {
          if (host === OLD.host && groupId === 'move') {
            state.moved = true
            return [{ ...row(groupId), errorCode: NOT_COORDINATOR, members: [] }]
          }
          if (groupId === 'load') {
            state.loadAtNew = (state.loadAtNew || 0) + Number(host === NEW.host)
            if (
              host === OLD.host ||
              !movedDescribed ||
              state.loadAtNew === 1 ||
              groupIds.includes('move')
            ) {
              return [{ ...row(groupId), errorCode: COORDINATOR_LOAD_IN_PROGRESS, members: [] }]
            }
          }
          if (groupId === 'move') movedDescribed = true
          return [row(groupId)]
        },
        { api, mixed: true, failRefresh }
      )
      const outcome = await run(fixture, () => fixture.admin.describeGroups(ids))

      expect(outcome.error).toBeUndefined()
      expect(fixture.asked[0]).toEqual([OLD.host, ...ids])
      expect(fixture.asked.filter(([, ...groupIds]) => groupIds.includes('move'))).toEqual([
        [OLD.host, ...ids],
        expect.arrayContaining([NEW.host, 'move']),
      ])
      const last = fixture.asked[fixture.asked.length - 1]
      expect(last).toEqual([NEW.host, 'load'])
      expect(fixture.state.oldRetired).toBe(true)
      expect(fixture.refreshes).toEqual(failRefresh ? ['failed', 'completed'] : ['completed'])
      expect(fixture.lookups.mock.calls.map(([{ groupId }]) => groupId)).toEqual([...ids, 'move'])
      expect(outcome.value.groups.map(({ groupId }) => groupId).sort()).toEqual(['load', 'move'])
    }
  )

  test.each([
    [
      'a COORDINATOR_LOAD_IN_PROGRESS row',
      (host, groupId, call) =>
        call === 1
          ? [{ ...consumerGroupRow(groupId), errorCode: COORDINATOR_LOAD_IN_PROGRESS, members: [] }]
          : [consumerGroupRow(groupId)],
      [
        [OLD.host, 'g'],
        [OLD.host, 'g'],
      ],
    ],
    [
      'a NOT_COORDINATOR row for an id the request did not ask',
      (host, groupId) => [
        consumerGroupRow(groupId),
        { ...consumerGroupRow('other'), errorCode: NOT_COORDINATOR, members: [] },
      ],
      [[OLD.host, 'g']],
    ],
  ])('refreshes no metadata after %s', async (_label, answer, asked) => {
    const fixture = movingCluster(answer)
    let before
    const outcome = await run(fixture, () => {
      before = fixture.metadataRequests().length
      return fixture.admin.describeGroups(['g'])
    })

    expect(outcome.error).toBeUndefined()
    expect(fixture.asked).toEqual(asked)
    expect(fixture.metadataRequests().length - before).toBe(0)
  })
})

describe('Admin > describeGroups > coordinator reconnect', () => {
  const { Kafka } = require('../../..')
  const Broker = require('../../broker')
  const Cluster = require('../../cluster')
  const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')

  beforeEach(() => {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask'] })
  })
  afterEach(() => {
    jest.restoreAllMocks()
    jest.useRealTimers()
  })

  test.each([
    ['ConsumerGroupDescribe', 0],
    ['ConsumerGroupDescribe', 2],
    ['DescribeGroups', 0],
    ['DescribeGroups', 2],
  ])('spends the %s budget of %i retries on failed coordinator setup', async (api, retries) => {
    const coordinator = { id: 1, host: 'coordinator', port: 9092 }
    const seed = { id: 0, host: 'seed', port: 9092 }
    const native = api === 'ConsumerGroupDescribe'
    const scripted = createScriptedCluster({
      nodes: [coordinator, seed],
      profile: 'sasl',
      advertise: { DescribeGroups: [6, 6], ...(native && { ConsumerGroupDescribe: [1, 1] }) },
    })
    const apiTimeout = 200
    const admin = new Kafka(
      scripted.kafkaOptions({
        brokers: [`${seed.host}:${seed.port}`],
        retry: { ...tenMsRetry, retries: 4 },
        sasl: { mechanism: 'plain', username: 'user', password: 'password' },
      })
    ).admin({ apiTimeout, retry: { retries } })
    let broker
    let reconnecting = false
    const failedPools = []
    scripted.answer('SaslHandshake', (_fields, { node }) => {
      if (node !== coordinator.id || !reconnecting) return {}
      failedPools.push(broker.connectionPool)
      return { errorCode: 34, enabledMechanisms: [] }
    })

    const findGroupCoordinator = Cluster.prototype.findGroupCoordinator
    jest.spyOn(Cluster.prototype, 'findGroupCoordinator').mockImplementation(async function (args) {
      broker = await findGroupCoordinator.call(this, args)
      if (native) {
        await broker.disconnect()
        reconnecting = true
      }
      return broker
    })
    const consumerGroupDescribe = Broker.prototype.consumerGroupDescribe
    const askNative = jest.spyOn(Broker.prototype, 'consumerGroupDescribe')
    if (!native) {
      askNative.mockImplementation(async function (args) {
        try {
          return await consumerGroupDescribe.call(this, args)
        } finally {
          // The native API is unavailable. Drop the connection before classic dispatch.
          await this.disconnect()
          reconnecting = true
        }
      })
    }
    const askClassic = jest.spyOn(Broker.prototype, 'describeGroups')

    try {
      await admin.connect()
      const started = Date.now()
      const outcome = observe(admin.describeGroups(['g']))
      await driveUntilSettled(outcome)

      expect(outcome.error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(outcome.error).toMatchObject({ retryCount: retries, cause: { code: 34 } })
      expect(Date.now() - started).toBeLessThan(apiTimeout)
      expect(failedPools).toHaveLength(retries + 1)
      expect(new Set([...failedPools, broker.connectionPool]).size).toBe(retries + 2)
      expect(askNative).toHaveBeenCalledTimes(native ? 0 : 1)
      expect(askClassic).not.toHaveBeenCalled()
      expect(
        scripted
          .requests()
          .filter(({ api }) => /^(ConsumerGroupDescribe|DescribeGroups)$/.test(api))
      ).toEqual([])
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })
})
