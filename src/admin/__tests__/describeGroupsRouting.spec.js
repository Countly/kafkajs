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
const { KafkaJSConnectionError, KafkaJSError } = require('../../errors')

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
            groups: [withMember(groupIds[0], 'first'), withMember(groupIds[0], 'second')],
          })

          const groups = await run({ consumerGroupDescribe }, [SURROGATE, LITERAL])

          expect(groups.map(({ groupId, members }) => [groupId, members[0].memberId])).toEqual([
            [SURROGATE, 'first'],
            [LITERAL, 'first'],
          ])
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
      const admin = createAdmin({ logger: newLogger(), retry, cluster })

      const { groups } = await admin.describeGroups(['g1', 'g2', 'g3'])

      expect(asked[0]).toEqual(['g1', 'g2', 'g3'])
      expect(asked.slice(1).sort()).toEqual([['g1'], ['g2']])
      expect(found).toEqual(['g1', 'g2', 'g3', 'g2'])
      expect(
        groups.map(({ groupId, errorCode, members }) => [groupId, errorCode, members[0].memberId])
      ).toEqual([
        ['g3', 0, 'first'],
        ['g1', 0, 'again'],
        ['g2', 0, 'again'],
      ])
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
  jest.useFakeTimers()
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
    findGroupCoordinator: jest
      .fn()
      .mockResolvedValueOnce(original)
      .mockResolvedValueOnce(original)
      .mockImplementation(async ({ groupId }) => (groupId === 'a' ? a : b)),
  }
  try {
    const result = createGroupDescriptions({
      cluster,
      logger,
      retry: { retries: 3, initialRetryTime: 1, factor: 0, multiplier: 1 },
    })
      .describe(['a', 'b'])
      .then(
        (value) => ({ value }),
        (error) => ({ error })
      )
    await jest.advanceTimersByTimeAsync(100)
    const outcome = await result
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

  /** The ids a DescribeGroups v6 request body asks for. */
  const askedIn = (body) =>
    new Decoder(body).readUVarIntArray((decoder) => decoder.readUVarIntString())

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
   * An admin over one real broker on a fake socket that offers DescribeGroups up to v6 and no
   * ConsumerGroupDescribe, so every id is described through DescribeGroups. `answer(asked, index)`
   * gives the rows of the `index`-th request for the ids it asked.
   */
  const adminOver = (answer) => {
    const found = []
    harness = createGroupApiBroker({
      versions: { [apiKeys.DescribeGroups]: { minVersion: 0, maxVersion: 6 } },
      respond: () => {
        const index = harness.requests.length - 1
        return answerOf(answer(askedIn(harness.requests[index].body), index))
      },
    })
    const cluster = {
      refreshMetadata: jest.fn(async () => {}),
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

  const sent = () => harness.requests.map(({ apiKey, body }) => [apiKey, askedIn(body)])

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
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(refreshes)
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
    const { admin, found } = adminOver((asked) =>
      asked.map((groupId) => ({ groupId, errorCode: COORDINATOR_LOAD_IN_PROGRESS }))
    )

    const outcome = observe(admin.describeGroups(['b']))
    await driveUntilSettled(outcome)

    expect(outcome.error).toMatchObject({
      name: 'KafkaJSNumberOfRetriesExceeded',
      type: 'COORDINATOR_LOAD_IN_PROGRESS',
      code: COORDINATOR_LOAD_IN_PROGRESS,
    })
    expect(sent()).toEqual([
      [apiKeys.DescribeGroups, ['b']],
      [apiKeys.DescribeGroups, ['b']],
      [apiKeys.DescribeGroups, ['b']],
    ])
    expect(found).toEqual(['b'])
  })
})

describe('Admin > describeGroups > two coordinators in one attempt', () => {
  beforeEach(() => {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask'] })
  })
  afterEach(() => {
    jest.useRealTimers()
  })

  /** `a` routed to node 0 and every other id to node 1, each node with its own stubs. */
  const twoCoordinators = (forA, forOthers) => {
    const found = []
    const nodes = [
      { nodeId: 0, describeGroups: jest.fn(), ...forA },
      { nodeId: 1, describeGroups: jest.fn(), ...forOthers },
    ]
    const cluster = {
      refreshMetadata: jest.fn(async () => {}),
      findGroupCoordinator: jest.fn(async ({ groupId }) => {
        found.push(groupId)
        return nodes[groupId === 'a' ? 0 : 1]
      }),
    }
    const admin = createAdmin({ logger: newLogger(), retry: tenMsRetry, cluster })
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
    ['the refusal arriving last', 10, 20],
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
        outcome.value.groups.map(({ groupId, members }) => [groupId, members[0].memberId])
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
        outcome.value.groups.map(({ groupId, members }) => [groupId, members[0].memberId])
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

  test('waits for every coordinator lookup of an attempt, so the next attempt does not look a sibling up again', async () => {
    const found = []
    let failed = false
    const node = {
      nodeId: 0,
      consumerGroupDescribe: jest.fn(async ({ groupIds }) => ({
        groups: groupIds.map((groupId) => consumerGroupRow(groupId)),
      })),
      describeGroups: jest.fn(),
    }
    const cluster = {
      refreshMetadata: jest.fn(async () => {}),
      findGroupCoordinator: jest.fn(async ({ groupId }) => {
        found.push(groupId)
        if (groupId === 'a' && !failed) {
          failed = true
          throw new KafkaJSError('The coordinator is not available yet')
        }
        if (groupId === 'b') await after(30)
        return node
      }),
    }
    const admin = createAdmin({ logger: newLogger(), retry: tenMsRetry, cluster })

    const outcome = observe(admin.describeGroups(['a', 'b']))
    await driveUntilSettled(outcome)

    expect(outcome.error).toBeUndefined()
    expect(found).toEqual(['a', 'b', 'a'])
    expect(node.consumerGroupDescribe.mock.calls.map(([request]) => request.groupIds)).toEqual([
      ['b'],
      ['a'],
    ])
    expect(outcome.value.groups.map(({ groupId, errorCode }) => [groupId, errorCode])).toEqual([
      ['b', 0],
      ['a', 0],
    ])
  })

  test('asks a loading id again only after the slower request of another coordinator in the same attempt has settled', async () => {
    const started = performance.now()
    const askedAt = []
    const { admin, nodes } = twoCoordinators(
      {
        consumerGroupDescribe: jest.fn(async ({ groupIds }) => {
          await after(50)
          return { groups: groupIds.map((groupId) => consumerGroupRow(groupId)) }
        }),
      },
      {
        consumerGroupDescribe: jest.fn(async ({ groupIds }) => {
          askedAt.push(performance.now() - started)
          return {
            groups: groupIds.map((groupId) =>
              askedAt.length === 1
                ? { ...consumerGroupRow(groupId), errorCode: COORDINATOR_LOAD_IN_PROGRESS }
                : consumerGroupRow(groupId)
            ),
          }
        }),
      }
    )

    const outcome = observe(admin.describeGroups(['a', 'b']))
    await driveUntilSettled(outcome)

    expect(outcome.error).toBeUndefined()
    expect(nodes[0].consumerGroupDescribe).toHaveBeenCalledTimes(1)
    expect(nodes[1].consumerGroupDescribe).toHaveBeenCalledTimes(2)
    // The retry waits for the 50 ms request of the other coordinator, then the 10 ms backoff.
    expect(askedAt[1]).toBeGreaterThanOrEqual(60)
  })
})
