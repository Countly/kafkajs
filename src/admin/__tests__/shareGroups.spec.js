const createAdmin = require('../index')
const { createShareGroups } = require('../shareGroups')
const { currentDeadline, runWithDeadline } = require('../deadline')
const Broker = require('../../broker')
const ConnectionPool = require('../../network/connectionPool')
const Encoder = require('../../protocol/encoder')
const { createErrorFromCode } = require('../../protocol/error')
const { Kafka } = require('../../../index')
const { newLogger, readRequestHeader } = require('testHelpers')
const { createScriptedCluster, shareScript } = require('../../../testHelpers/scriptedCluster')
const expected = require('../../../testHelpers/wire/expected')
const {
  KafkaJSProtocolError,
  KafkaJSInvalidConfigurationError,
  KafkaJSTimeout,
  KafkaJSInvariantViolation,
  KafkaJSDeleteGroupsError,
  KafkaJSConnectionError,
  KafkaJSShareGroupsNotSupportedError,
} = require('../../errors')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')

jest.setTimeout(3000)

const retry = { retries: 2, initialRetryTime: 1, maxRetryTime: 1, multiplier: 1, factor: 0 }

/** Broker method, API name, API key and the one released version of every share API. */
const APIS = [
  ['shareGroupHeartbeat', 'ShareGroupHeartbeat', 76, 1],
  ['shareGroupDescribe', 'ShareGroupDescribe', 77, 1],
  ['shareFetch', 'ShareFetch', 78, 2],
  ['shareAcknowledge', 'ShareAcknowledge', 79, 2],
  ['describeShareGroupOffsets', 'DescribeShareGroupOffsets', 90, 1],
  ['alterShareGroupOffsets', 'AlterShareGroupOffsets', 91, 0],
  ['deleteShareGroupOffsets', 'DeleteShareGroupOffsets', 92, 0],
]

const DESCRIBE = ['describeShareGroups', ['share-group'], {}]
const LIST = ['listShareGroupOffsets', { groupId: 'share-group' }]
const ALTER = [
  'alterShareGroupOffsets',
  {
    groupId: 'share-group',
    topics: [{ topic: 'alpha', partitions: [{ partition: 0, offset: '8' }] }],
  },
]
const DELETE = ['deleteShareGroupOffsets', { groupId: 'share-group', topics: ['alpha'] }]
/** Public method, its options (the group ids for describe) and describe's second argument. */
const CALLS = [DESCRIBE, LIST, ALTER, DELETE]
const OFFSET_CALLS = [LIST, ALTER, DELETE]
const WIRE_METHODS = [
  'shareGroupDescribe',
  'describeShareGroupOffsets',
  'alterShareGroupOffsets',
  'deleteShareGroupOffsets',
]

/** @param {string} method public method @returns {string} the Broker method it sends through */
const brokerMethodOf = (method) =>
  ({
    describeShareGroups: 'shareGroupDescribe',
    listShareGroupOffsets: 'describeShareGroupOffsets',
  })[method] || method

/**
 * Calls a public method with `extra` merged into the options argument it reads.
 * @param {object} admin the admin under test
 * @param {Array} call public method name, its first argument and describe's options
 * @param {object} [extra] fields such as `apiTimeout`
 * @returns {Promise<any>} the call's result
 */
const callWith = (admin, [method, options, second], extra = {}) =>
  method === 'describeShareGroups'
    ? admin[method](options, { ...second, ...extra })
    : admin[method]({ ...options, ...extra })

/** @returns {object} a complete group row built from the independent literal */
const group = (groupId, errorCode = 0) => ({
  ...expected.shareGroupDescribeV1.response.groups[0],
  groupId,
  errorCode,
})

/** @returns {object} a stub coordinator advertising the four admin APIs and a finalized share feature */
const coordinator = (nodeId = 0) => ({
  nodeId,
  versions: {
    77: { minVersion: 1, maxVersion: 1 },
    90: { minVersion: 1, maxVersion: 1 },
    91: { minVersion: 0, maxVersion: 0 },
    92: { minVersion: 0, maxVersion: 0 },
  },
  finalizedFeatures: { 'share.version': { maxVersionLevel: 1 } },
  finalizedFeaturesEpoch: 7,
  shareGroupDescribe: jest.fn(async ({ groupIds }) => ({
    groups: groupIds.map((id) => group(id)),
  })),
  describeShareGroupOffsets: jest.fn(async () => ({
    groups: [expected.describeShareGroupOffsetsV1.response.groups[0]],
  })),
  alterShareGroupOffsets: jest.fn(async () => expected.alterShareGroupOffsetsV0.response),
  deleteShareGroupOffsets: jest.fn(async () => expected.deleteShareGroupOffsetsV0.response),
})

/** @returns {object} the real admin factory over a stubbed GROUP discovery that routes with `route` */
const fixture = (brokers = [coordinator()], route = () => brokers[0]) => {
  const cluster = {
    findGroupCoordinator: jest.fn(async ({ groupId }) => route(groupId)),
    refreshMetadata: jest.fn(async () => {}),
  }
  return { admin: createAdmin({ cluster, retry, logger: newLogger() }), cluster, brokers }
}

/** @returns {{promise: Promise<void>, resolve: Function}} a promise that settles only when told to */
const gate = () => {
  let resolve
  const promise = new Promise((done) => {
    resolve = done
  })
  return { promise, resolve }
}

/** @param {Promise<any>} promise @returns {Promise<{value?: any, error?: Error}>} its outcome, never a rejection */
const settle = (promise) =>
  promise.then(
    (value) => ({ value }),
    (error) => ({ error })
  )

/** Waits, a turn at a time, until `condition` holds, then asserts it. */
const until = async (condition) => {
  for (let turn = 0; turn < 200 && !condition(); turn++) await new Promise(setImmediate)
  expect(condition()).toBe(true)
}

/** Lets pending promise continuations run; timers stay under the test's control. */
const flush = async () => {
  for (let turn = 0; turn < 30; turn++) await Promise.resolve()
}

/**
 * Builds the complete request frame a client must write from a literal body.
 * @returns {Buffer} the size-prefixed frame with the flexible request header
 */
const frame = (key, version, correlationId, hex, clientId) => {
  const body = Buffer.from(hex, 'hex')
  const client = Buffer.from(clientId)
  const out = Buffer.alloc(15 + client.length + body.length)
  out.writeInt32BE(out.length - 4, 0)
  out.writeInt16BE(key, 4)
  out.writeInt16BE(version, 6)
  out.writeInt32BE(correlationId, 8)
  out.writeInt16BE(client.length, 12)
  client.copy(out, 14)
  out[14 + client.length] = 0
  body.copy(out, 15 + client.length)
  return out
}

/** Scripts all seven share APIs with their literal answers, then the id-correlated describe and single-group list answers. */
const script = (scripted) => {
  shareScript(
    scripted,
    APIS.map(([method, api, , version]) => ({
      api,
      response: { ...expected[`${method}V${version}`].response, throttleTime: 0 },
    }))
  )
  scripted.answer('ShareGroupDescribe', ({ groupIds }) => ({
    throttleTime: 0,
    groups: groupIds.map((id) => group(id)),
  }))
  scripted.answer('DescribeShareGroupOffsets', () => ({
    throttleTime: 0,
    groups: [expected.describeShareGroupOffsetsV1.response.groups[0]],
  }))
  return scripted
}

/**
 * Connects three real Brokers, each over its own Pool, to one scripted share cluster.
 * @param {object} [poolOptions] ConnectionPool overrides
 * @returns {Promise<object>} the cluster, pools, brokers and the admin fixture over them
 */
const seam = async (poolOptions = {}) => {
  const scripted = script(
    createScriptedCluster({
      nodes: [0, 1, 2].map((id) => ({ id, host: `node${id}`, port: 9092 })),
      profile: 'share',
    })
  )
  const pools = []
  const brokers = []
  try {
    for (let id = 0; id < 3; id++) {
      const pool = new ConnectionPool({
        host: `node${id}`,
        port: 9092,
        clientId: 'share-admin',
        socketFactory: scripted.socketFactory,
        logger: newLogger(),
        connectionTimeout: 1000,
        requestTimeout: 1000,
        enforceRequestTimeout: false,
        connectionsMaxIdle: 0,
        ...poolOptions,
      })
      pools.push(pool)
      const broker = new Broker({ connectionPool: pool, logger: newLogger() })
      broker.nodeId = id
      await broker.connect()
      brokers.push(broker)
    }
  } catch (error) {
    for (const pool of pools) await pool.destroy({ drainTimeout: 0 })
    throw error
  }
  return { scripted, pools, brokers, ...fixture(brokers) }
}

describe('share administration', () => {
  let f
  afterEach(async () => {
    if (f?.pools) {
      for (const pool of f.pools) await pool.destroy({ drainTimeout: 0 })
      f.scripted.assertClean()
    }
    f = null
    jest.restoreAllMocks()
    jest.useRealTimers()
  })

  test('registers four additive methods and no deletion alias', () => {
    f = fixture()
    for (const [method] of CALLS) expect(typeof f.admin[method]).toBe('function')
    expect(f.admin.deleteShareGroups).toBeUndefined()
    expect(typeof f.admin.deleteGroups).toBe('function')
  })

  describe('describeShareGroups', () => {
    test('batches two groups on one GROUP coordinator in one frame', async () => {
      f = await seam()
      const result = await f.admin.describeShareGroups(['a', 'b'])
      expect(result.groups.map((g) => g.groupId)).toEqual(['a', 'b'])
      expect(
        f.scripted
          .requests()
          .filter((r) => r.api === 'ShareGroupDescribe')
          .map((r) => r.fields.groupIds)
      ).toEqual([['a', 'b']])
      expect(f.cluster.findGroupCoordinator.mock.calls.map(([options]) => options)).toEqual([
        { groupId: 'a' },
        { groupId: 'b' },
      ])
    })

    test('batches on two coordinators and preserves a bad group beside its sibling', async () => {
      f = await seam()
      const held = gate()
      f.cluster.findGroupCoordinator.mockImplementation(
        async ({ groupId }) => f.brokers[groupId === 'a' ? 0 : 1]
      )
      f.scripted.answer('ShareGroupDescribe', async ({ groupIds }) => {
        await held.promise
        return { throttleTime: 0, groups: groupIds.map((id) => group(id, id === 'b' ? 69 : 0)) }
      })
      const frames = () => f.scripted.requests().filter((r) => r.api === 'ShareGroupDescribe')
      let finished = false
      const pending = settle(f.admin.describeShareGroups(['a', 'b'])).then((outcome) => {
        finished = true
        return outcome
      })
      try {
        await until(() => frames().length === 2)
        expect(finished).toBe(false)
        expect(
          frames()
            .map((r) => [r.node, r.fields.groupIds])
            .sort(([x], [y]) => x - y)
        ).toEqual([
          [0, ['a']],
          [1, ['b']],
        ])
      } finally {
        held.resolve()
      }
      const { value: result, error } = await pending
      expect(error).toBeUndefined()
      expect(result.groups.map((g) => [g.groupId, g.errorCode])).toEqual([
        ['a', 0],
        ['b', 69],
      ])
    })

    test('relocates only failed ids once and regroups a split coordinator', async () => {
      const first = coordinator(0)
      const second = coordinator(1)
      const third = coordinator(2)
      first.shareGroupDescribe.mockResolvedValueOnce({
        groups: [group('a'), group('b', 16), group('c', 15)],
      })
      const lookups = new Map()
      f = fixture([first, second, third], (id) => {
        const n = lookups.get(id) || 0
        lookups.set(id, n + 1)
        return n ? (id === 'b' ? second : third) : first
      })
      const result = await f.admin.describeShareGroups(['a', 'b', 'c', 'a'])
      expect(result.groups.map((r) => r.groupId)).toEqual(['a', 'b', 'c', 'a'])
      expect(first.shareGroupDescribe).toHaveBeenCalledTimes(1)
      expect(second.shareGroupDescribe.mock.calls[0][0].groupIds).toEqual(['b'])
      expect(third.shareGroupDescribe.mock.calls[0][0].groupIds).toEqual(['c'])
      expect([...lookups]).toEqual([
        ['a', 1],
        ['b', 2],
        ['c', 2],
      ])
      result.groups[0].members[0].assignment[0].partitions.push(99)
      expect(result.groups[3].members[0].assignment[0].partitions).toEqual([0, 2])
    })

    test('returns a second NOT_COORDINATOR as group data without a third discovery', async () => {
      const broker = coordinator()
      broker.shareGroupDescribe.mockImplementation(async ({ groupIds }) => ({
        groups: groupIds.map((id) => group(id, 16)),
      }))
      f = fixture([broker])
      const result = await f.admin.describeShareGroups(['a', 'a'])
      expect(result.groups.map((r) => r.errorCode)).toEqual([16, 16])
      expect(f.cluster.findGroupCoordinator).toHaveBeenCalledTimes(2)
      expect(broker.shareGroupDescribe).toHaveBeenCalledTimes(2)
    })

    test('retries loading in place without resending a settled sibling', async () => {
      const broker = coordinator()
      broker.shareGroupDescribe.mockResolvedValueOnce({ groups: [group('a'), group('b', 14)] })
      f = fixture([broker])
      await f.admin.describeShareGroups(['a', 'b'])
      expect(broker.shareGroupDescribe.mock.calls.map(([request]) => request.groupIds)).toEqual([
        ['a', 'b'],
        ['b'],
      ])
      expect(f.cluster.findGroupCoordinator).toHaveBeenCalledTimes(2)
    })

    test('keeps a coordinator that never resolves as the group own error row after the retry budget', async () => {
      f = fixture()
      f.cluster.findGroupCoordinator.mockRejectedValue(createErrorFromCode(15))
      const result = await f.admin.describeShareGroups(['a'])
      expect(result.groups).toEqual([
        {
          groupId: 'a',
          state: '',
          groupEpoch: 0,
          assignmentEpoch: 0,
          assignorName: '',
          members: [],
          errorCode: -1,
          errorMessage: createErrorFromCode(15).message,
          authorizedOperations: null,
        },
      ])
      expect(f.cluster.findGroupCoordinator).toHaveBeenCalledTimes(retry.retries + 1)
      expect(f.brokers[0].shareGroupDescribe).not.toHaveBeenCalled()
    })

    test('keeps a healthy group beside one whose coordinator never resolves', async () => {
      const broker = coordinator()
      f = fixture([broker])
      f.cluster.findGroupCoordinator.mockImplementation(async ({ groupId }) => {
        if (groupId === 'bad') throw createErrorFromCode(15)
        return broker
      })
      const result = await f.admin.describeShareGroups(['good', 'bad'])
      expect(result.groups.map((g) => [g.groupId, g.errorCode, g.errorMessage])).toEqual([
        ['good', 0, null],
        ['bad', -1, createErrorFromCode(15).message],
      ])
      expect(broker.shareGroupDescribe.mock.calls.map(([request]) => request.groupIds)).toEqual([
        ['good'],
      ])
      expect(f.cluster.findGroupCoordinator.mock.calls.map(([options]) => options.groupId)).toEqual(
        ['good', 'bad', 'bad', 'bad']
      )
    })

    test('resends only the group whose coordinator lookup failed once', async () => {
      const broker = coordinator()
      f = fixture([broker])
      let failures = 1
      f.cluster.findGroupCoordinator.mockImplementation(async ({ groupId }) => {
        if (groupId === 'b' && failures-- > 0) throw createErrorFromCode(16)
        return broker
      })
      const result = await f.admin.describeShareGroups(['a', 'b'])
      expect(result.groups.map((g) => [g.groupId, g.errorCode])).toEqual([
        ['a', 0],
        ['b', 0],
      ])
      expect(broker.shareGroupDescribe.mock.calls.map(([request]) => request.groupIds)).toEqual([
        ['a'],
        ['b'],
      ])
    })

    test('keeps a coordinator batch that answered when another batch throws a retriable error', async () => {
      const first = coordinator(0)
      const second = coordinator(1)
      second.shareGroupDescribe.mockRejectedValueOnce(createErrorFromCode(14))
      f = fixture([first, second], (id) => (id === 'a' ? first : second))
      const result = await f.admin.describeShareGroups(['a', 'b'])
      expect(result.groups.map((g) => [g.groupId, g.errorCode])).toEqual([
        ['a', 0],
        ['b', 0],
      ])
      expect(first.shareGroupDescribe.mock.calls.map(([request]) => request.groupIds)).toEqual([
        ['a'],
      ])
      expect(second.shareGroupDescribe.mock.calls.map(([request]) => request.groupIds)).toEqual([
        ['b'],
        ['b'],
      ])
    })

    test('keeps a batch that keeps throwing as its own error rows beside the settled batch', async () => {
      const first = coordinator(0)
      const second = coordinator(1)
      second.shareGroupDescribe.mockRejectedValue(createErrorFromCode(14))
      f = fixture([first, second], (id) => (id === 'a' ? first : second))
      const result = await f.admin.describeShareGroups(['a', 'b'])
      expect(result.groups.map((g) => [g.groupId, g.errorCode, g.errorMessage])).toEqual([
        ['a', 0, null],
        ['b', -1, createErrorFromCode(14).message],
      ])
      expect(first.shareGroupDescribe).toHaveBeenCalledTimes(1)
      expect(second.shareGroupDescribe).toHaveBeenCalledTimes(retry.retries + 1)
    })

    test.each([[15], [16]])(
      'relocates a group whose batch request throws %s to its new coordinator beside a settled sibling',
      async (code) => {
        const first = coordinator(0)
        const second = coordinator(1)
        const third = coordinator(2)
        first.shareGroupDescribe.mockRejectedValue(createErrorFromCode(code))
        const lookups = new Map()
        f = fixture([first, second, third], (id) => {
          const n = lookups.get(id) || 0
          lookups.set(id, n + 1)
          if (id === 'b') return third
          return n ? second : first
        })
        const result = await f.admin.describeShareGroups(['a', 'b'])
        expect(result.groups.map((g) => [g.groupId, g.errorCode])).toEqual([
          ['a', 0],
          ['b', 0],
        ])
        expect(first.shareGroupDescribe.mock.calls.map(([request]) => request.groupIds)).toEqual([
          ['a'],
        ])
        expect(second.shareGroupDescribe.mock.calls.map(([request]) => request.groupIds)).toEqual([
          ['a'],
        ])
        expect(third.shareGroupDescribe.mock.calls.map(([request]) => request.groupIds)).toEqual([
          ['b'],
        ])
        expect([...lookups]).toEqual([
          ['a', 2],
          ['b', 1],
        ])
      }
    )

    test('relocates a group once for a thrown NOT_COORDINATOR and keeps the new route when it throws again', async () => {
      const first = coordinator(0)
      const second = coordinator(1)
      first.shareGroupDescribe.mockRejectedValue(createErrorFromCode(16))
      second.shareGroupDescribe.mockRejectedValue(createErrorFromCode(16))
      let lookups = 0
      f = fixture([first, second], () => (lookups++ ? second : first))
      const result = await f.admin.describeShareGroups(['a'])
      expect(result.groups.map((g) => [g.groupId, g.errorCode, g.errorMessage])).toEqual([
        ['a', -1, createErrorFromCode(16).message],
      ])
      expect(f.cluster.findGroupCoordinator).toHaveBeenCalledTimes(2)
      expect(first.shareGroupDescribe).toHaveBeenCalledTimes(1)
      expect(second.shareGroupDescribe).toHaveBeenCalledTimes(retry.retries)
    })

    test('keeps the route for a thrown COORDINATOR_LOAD_IN_PROGRESS and still allows the later relocation', async () => {
      const first = coordinator(0)
      const second = coordinator(1)
      first.shareGroupDescribe
        .mockRejectedValueOnce(createErrorFromCode(14))
        .mockRejectedValueOnce(createErrorFromCode(16))
      let lookups = 0
      f = fixture([first, second], () => (lookups++ ? second : first))
      const result = await f.admin.describeShareGroups(['a'])
      expect(result.groups.map((g) => [g.groupId, g.errorCode])).toEqual([['a', 0]])
      expect(first.shareGroupDescribe.mock.calls.map(([request]) => request.groupIds)).toEqual([
        ['a'],
        ['a'],
      ])
      expect(second.shareGroupDescribe.mock.calls.map(([request]) => request.groupIds)).toEqual([
        ['a'],
      ])
      expect(f.cluster.findGroupCoordinator).toHaveBeenCalledTimes(2)
    })

    test.each([['lookup'], ['request']])(
      'rejects the whole call with the identical transport error from a failing %s',
      async (stage) => {
        const transport = new KafkaJSConnectionError('socket closed')
        const first = coordinator(0)
        const second = coordinator(1)
        f = fixture([first, second], (id) => (id === 'a' ? first : second))
        if (stage === 'lookup') {
          f.cluster.findGroupCoordinator.mockImplementation(async ({ groupId }) => {
            if (groupId === 'b') throw transport
            return first
          })
        } else {
          second.shareGroupDescribe.mockRejectedValue(transport)
        }
        await expect(f.admin.describeShareGroups(['a', 'b'])).rejects.toBe(transport)
        expect(f.cluster.findGroupCoordinator).toHaveBeenCalledTimes(2)
        expect(second.shareGroupDescribe).toHaveBeenCalledTimes(stage === 'request' ? 1 : 0)
      }
    )

    test('describes no groups without discovery or a request', async () => {
      f = fixture()
      expect(await f.admin.describeShareGroups([])).toEqual({ groups: [] })
      expect(f.cluster.findGroupCoordinator).not.toHaveBeenCalled()
      expect(f.brokers[0].shareGroupDescribe).not.toHaveBeenCalled()
    })

    test('maps Java setter fields and preserves unknown signed authorization bits', async () => {
      f = fixture()
      const result = (
        await f.admin.describeShareGroups(['share-group'], { includeAuthorizedOperations: true })
      ).groups[0]
      expect(f.brokers[0].shareGroupDescribe.mock.calls[0][0]).toEqual({
        groupIds: ['share-group'],
        includeAuthorizedOperations: true,
      })
      expect(result).toEqual({
        groupId: 'share-group',
        state: 'Stable',
        groupEpoch: 2147483647,
        assignmentEpoch: 9,
        assignorName: 'simple',
        errorCode: 0,
        errorMessage: null,
        authorizedOperations: -2147483647,
        members: [
          {
            memberId: 'share-member',
            rackId: null,
            memberEpoch: 8,
            clientId: 'client-a',
            clientHost: '/127.0.0.1',
            subscribedTopics: ['alpha', 'βeta'],
            assignment: [
              {
                topic: 'alpha',
                topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
                partitions: [0, 2],
              },
            ],
          },
        ],
      })
      expect(
        (await f.admin.describeShareGroups(['share-group'])).groups[0].authorizedOperations
      ).toBeNull()
      f.brokers[0].shareGroupDescribe.mockResolvedValueOnce({
        groups: [{ ...group('share-group'), authorizedOperations: -2147483648 }],
      })
      expect(
        (await f.admin.describeShareGroups(['share-group'], { includeAuthorizedOperations: true }))
          .groups[0].authorizedOperations
      ).toBeNull()
    })

    test.each([
      ['missing', []],
      ['foreign', [group('foreign')]],
      ['duplicate', [group('a'), group('a')]],
    ])('refuses a %s describe row', async (kind, groups) => {
      const broker = coordinator()
      broker.shareGroupDescribe.mockResolvedValueOnce({ groups })
      f = fixture([broker])
      await expect(f.admin.describeShareGroups(['a'])).rejects.toBeInstanceOf(
        KafkaJSInvariantViolation
      )
    })
  })

  describe('offset operations', () => {
    test('sends omitted, empty and named list selections distinctly on the wire', async () => {
      f = await seam()
      await f.admin.listShareGroupOffsets({ groupId: 'share-group' })
      await f.admin.listShareGroupOffsets({ groupId: 'share-group', topics: [] })
      await f.admin.listShareGroupOffsets({
        groupId: 'share-group',
        topics: [{ topic: 'alpha', partitions: [0, 2] }],
      })
      expect(
        f.scripted
          .requests()
          .filter((r) => r.api === 'DescribeShareGroupOffsets')
          .map((r) => r.fields)
      ).toEqual([
        { groups: [{ groupId: 'share-group', topics: null }] },
        { groups: [{ groupId: 'share-group', topics: [] }] },
        {
          groups: [
            { groupId: 'share-group', topics: [{ topicName: 'alpha', partitions: [0, 2] }] },
          ],
        },
      ])
    })

    test('returns exact offsets and known lag with unknown lag null and partition errors as data', async () => {
      f = await seam()
      const [topic] = await f.admin.listShareGroupOffsets({ groupId: 'share-group' })
      expect(topic.topic).toBe('alpha')
      expect(topic.topicId).toBe('01020304-0506-0708-090a-0b0c0d0e0f10')
      expect(topic.partitions[0]).toEqual({
        partition: 0,
        startOffset: '9007199254740993',
        leaderEpoch: 2147483647,
        lag: '9223372036854775807',
        errorCode: 0,
      })
      expect(topic.partitions[1]).toEqual({
        partition: 2,
        startOffset: '9223372036854775807',
        leaderEpoch: -1,
        lag: null,
        errorCode: 29,
        error: expect.any(KafkaJSProtocolError),
      })
    })

    test('alter sends exact decimal offsets including the int64 ceiling', async () => {
      f = await seam()
      const rows = await f.admin.alterShareGroupOffsets({
        groupId: 'share-group',
        topics: [
          {
            topic: 'alpha',
            partitions: [
              { partition: 0, offset: '9007199254740993' },
              { partition: 2, offset: '9223372036854775807' },
            ],
          },
        ],
      })
      expect(f.scripted.requests().find((r) => r.api === 'AlterShareGroupOffsets').fields).toEqual(
        expected.alterShareGroupOffsetsV0.request
      )
      expect(rows).toEqual([
        {
          topic: 'alpha',
          topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
          partitions: [
            { partition: 0, errorCode: 0 },
            { partition: 2, errorCode: 29, error: expect.any(KafkaJSProtocolError) },
          ],
        },
      ])
    })

    test.each([
      '-1',
      'abc',
      '9223372036854775808',
      9007199254740994,
      '1.5',
      '01',
      '+1',
      '',
      null,
      1n,
    ])('refuses invalid offset %p before discovery and any frame', async (offset) => {
      f = fixture()
      await expect(
        f.admin.alterShareGroupOffsets({
          groupId: 'share-group',
          topics: [{ topic: 'alpha', partitions: [{ partition: 0, offset }] }],
        })
      ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(f.cluster.findGroupCoordinator).not.toHaveBeenCalled()
      expect(f.brokers[0].alterShareGroupOffsets).not.toHaveBeenCalled()
    })

    test('delete maps names and preserves a subscribed-topic error as data', async () => {
      f = await seam()
      f.scripted.answer('DeleteShareGroupOffsets', () => ({
        throttleTime: 0,
        responses: [
          { ...expected.deleteShareGroupOffsetsV0.response.responses[0], errorCode: 86 },
          expected.deleteShareGroupOffsetsV0.response.responses[1],
        ],
      }))
      const rows = await f.admin.deleteShareGroupOffsets({
        groupId: 'share-group',
        topics: ['alpha', 'βeta'],
      })
      expect(f.scripted.requests().find((r) => r.api === 'DeleteShareGroupOffsets').fields).toEqual(
        expected.deleteShareGroupOffsetsV0.request
      )
      expect(rows.map((r) => [r.topic, r.errorCode])).toEqual([
        ['alpha', 86],
        ['βeta', 29],
      ])
      expect(rows.every((r) => r.error instanceof KafkaJSProtocolError)).toBe(true)
    })

    test.each(OFFSET_CALLS)(
      '%s throws the original group protocol error',
      async (method, options) => {
        const broker = coordinator()
        const wire = brokerMethodOf(method)
        broker[wire].mockResolvedValueOnce(
          method === 'listShareGroupOffsets'
            ? { groups: [{ groupId: 'share-group', errorCode: 30, topics: [] }] }
            : { errorCode: 68, responses: [] }
        )
        f = fixture([broker])
        const error = await f.admin[method](options).catch((e) => e)
        expect(error).toBeInstanceOf(KafkaJSProtocolError)
        expect(error.code).toBe(method === 'listShareGroupOffsets' ? 30 : 68)
        expect(broker[wire]).toHaveBeenCalledTimes(1)
      }
    )

    test.each(OFFSET_CALLS)(
      '%s rethrows a Broker-rejected protocol error as the same object after one attempt',
      async (method, options) => {
        const sentinel = createErrorFromCode(30)
        const broker = coordinator()
        const wire = brokerMethodOf(method)
        broker[wire].mockRejectedValue(sentinel)
        f = fixture([broker])
        await expect(f.admin[method](options)).rejects.toBe(sentinel)
        expect(broker[wire]).toHaveBeenCalledTimes(1)
      }
    )

    test.each(OFFSET_CALLS)('%s relocates once to the new coordinator', async (method, options) => {
      const first = coordinator(0)
      const second = coordinator(1)
      const wire = brokerMethodOf(method)
      first[wire].mockResolvedValueOnce(
        method === 'listShareGroupOffsets'
          ? { groups: [{ groupId: 'share-group', errorCode: 16, topics: [] }] }
          : { errorCode: 16, responses: [] }
      )
      f = fixture([first, second])
      f.cluster.findGroupCoordinator.mockResolvedValueOnce(first).mockResolvedValueOnce(second)
      await f.admin[method](options)
      expect(first[wire]).toHaveBeenCalledTimes(1)
      expect(second[wire]).toHaveBeenCalledTimes(1)
      expect(f.cluster.findGroupCoordinator).toHaveBeenCalledTimes(2)
    })

    test.each([14, 16])(
      'offset group error %s remains a protocol error after bounded retry',
      async (code) => {
        const broker = coordinator()
        broker.describeShareGroupOffsets.mockResolvedValue({
          groups: [{ groupId: 'share-group', topics: [], errorCode: code }],
        })
        f = fixture([broker])
        const error = await f.admin
          .listShareGroupOffsets({ groupId: 'share-group' })
          .catch((e) => e)
        expect(error).toBeInstanceOf(KafkaJSProtocolError)
        expect(error.code).toBe(code)
        expect(broker.describeShareGroupOffsets).toHaveBeenCalledTimes(code === 14 ? 3 : 2)
        expect(f.cluster.findGroupCoordinator).toHaveBeenCalledTimes(code === 14 ? 1 : 2)
      }
    )

    test.each([
      ['an answer for another group', { groups: [{ groupId: 'other', errorCode: 0, topics: [] }] }],
      ['no group rows', { groups: [] }],
    ])('list refuses %s', async (kind, answer) => {
      const broker = coordinator()
      broker.describeShareGroupOffsets.mockResolvedValueOnce(answer)
      f = fixture([broker])
      await expect(
        f.admin.listShareGroupOffsets({ groupId: 'share-group' })
      ).rejects.toBeInstanceOf(KafkaJSInvariantViolation)
      expect(broker.describeShareGroupOffsets).toHaveBeenCalledTimes(1)
    })
  })

  describe('capability', () => {
    test.each(CALLS)('%s converts runtime 35 to a typed capability refusal', async (...call) => {
      const [method] = call
      const broker = coordinator()
      const wire = brokerMethodOf(method)
      const [, apiName, apiKey] = APIS.find(([name]) => name === wire)
      broker[wire].mockResolvedValueOnce(
        method === 'describeShareGroups' || method === 'listShareGroupOffsets'
          ? { groups: [{ ...group('share-group'), errorCode: 35 }] }
          : { errorCode: 35, responses: [] }
      )
      f = fixture([broker])
      const error = await callWith(f.admin, call).catch((e) => e)
      expect(error).toBeInstanceOf(KafkaJSShareGroupsNotSupportedError)
      expect(error).toMatchObject({
        name: 'KafkaJSShareGroupsNotSupportedError',
        reason: 'refused-by-broker',
        retriable: false,
        apiKey,
        apiName,
      })
      expect(broker[wire]).toHaveBeenCalledTimes(1)
    })

    /** Answers whose group envelope is fine but whose last partition or topic row says UNSUPPORTED_VERSION. */
    const itemRefusals = () => {
      const withLast = (rows, patch) => [...rows.slice(0, -1), { ...rows.at(-1), ...patch }]
      const listed = expected.describeShareGroupOffsetsV1.response.groups[0]
      const altered = expected.alterShareGroupOffsetsV0.response
      const deleted = expected.deleteShareGroupOffsetsV0.response
      return {
        listShareGroupOffsets: {
          groups: [
            {
              ...listed,
              topics: [
                {
                  ...listed.topics[0],
                  partitions: withLast(listed.topics[0].partitions, { errorCode: 35 }),
                },
              ],
            },
          ],
        },
        alterShareGroupOffsets: {
          ...altered,
          responses: [
            {
              ...altered.responses[0],
              partitions: withLast(altered.responses[0].partitions, { errorCode: 35 }),
            },
          ],
        },
        deleteShareGroupOffsets: {
          ...deleted,
          responses: withLast(deleted.responses, { errorCode: 35 }),
        },
      }
    }

    test.each(OFFSET_CALLS)(
      '%s converts an item-level runtime 35 to a typed capability refusal',
      async (method, options) => {
        const broker = coordinator()
        const wire = brokerMethodOf(method)
        const [, apiName, apiKey] = APIS.find(([name]) => name === wire)
        broker[wire].mockResolvedValueOnce(itemRefusals()[method])
        f = fixture([broker])
        const error = await f.admin[method](options).catch((e) => e)
        expect(error).toBeInstanceOf(KafkaJSShareGroupsNotSupportedError)
        expect(error).toMatchObject({
          name: 'KafkaJSShareGroupsNotSupportedError',
          reason: 'refused-by-broker',
          retriable: false,
          shareVersion: null,
          apiKey,
          apiName,
        })
        expect(broker[wire]).toHaveBeenCalledTimes(1)
        expect(f.cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
      }
    )

    test.each([
      ['missing', 'api-not-advertised'],
      ['preview', 'api-version-range'],
      ['feature', 'feature-disabled'],
    ])('refuses %s capability before a share frame', async (mode, reason) => {
      const broker = coordinator()
      if (mode === 'missing') delete broker.versions[90]
      if (mode === 'preview') broker.versions[90] = { minVersion: 0, maxVersion: 0 }
      if (mode === 'feature') broker.finalizedFeatures = {}
      f = fixture([broker])
      await expect(f.admin.listShareGroupOffsets({ groupId: 'share-group' })).rejects.toMatchObject(
        {
          name: 'KafkaJSShareGroupsNotSupportedError',
          reason,
          apiName: 'DescribeShareGroupOffsets',
        }
      )
      expect(broker.describeShareGroupOffsets).not.toHaveBeenCalled()
    })

    test.each([
      ['describeShareGroups', 77, 'shareGroupDescribe'],
      ['listShareGroupOffsets', 90, 'describeShareGroupOffsets'],
      ['alterShareGroupOffsets', 91, 'alterShareGroupOffsets'],
      ['deleteShareGroupOffsets', 92, 'deleteShareGroupOffsets'],
    ])('%s checks only its own API key on the coordinator', async (method, key, wire) => {
      const broker = coordinator()
      for (const other of [77, 90, 91, 92]) if (other !== key) delete broker.versions[other]
      f = fixture([broker])
      await callWith(
        f.admin,
        CALLS.find(([name]) => name === method)
      )
      expect(broker[wire]).toHaveBeenCalledTimes(1)
    })
  })

  describe('validation', () => {
    test('validates the complete selection before any coordinator request', async () => {
      f = fixture()
      const invalid = [
        { groupId: '' },
        { groupId: 'g', topics: null },
        { groupId: 'share-group', topics: Array(1) },
        { groupId: 'share-group', topics: [{ topic: 'alpha', partitions: Array(1) }] },
        { groupId: 'g', topics: [{ topic: 'alpha', partitions: [0, 0] }] },
        { groupId: 'g', topics: [{ topic: 'alpha', partitions: [1.5] }] },
        { groupId: 'g', topics: [{ topic: 'alpha', partitions: [-1] }] },
        { groupId: 'g', topics: [{ topic: 'alpha', partitions: ['0'] }] },
        { groupId: 'g', topics: [{ topic: 'alpha', partitions: [2147483648] }] },
        {
          groupId: 'g',
          topics: [
            { topic: 'alpha', partitions: [] },
            { topic: 'alpha', partitions: [] },
          ],
        },
      ]
      for (const options of invalid)
        await expect(f.admin.listShareGroupOffsets(options)).rejects.toBeInstanceOf(
          KafkaJSInvalidConfigurationError
        )
      await expect(
        f.admin.describeShareGroups(['g'], { includeAuthorizedOperations: 'yes' })
      ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
      await expect(f.admin.describeShareGroups([''])).rejects.toBeInstanceOf(
        KafkaJSInvalidConfigurationError
      )
      await expect(f.admin.describeShareGroups(Array(1))).rejects.toBeInstanceOf(
        KafkaJSInvalidConfigurationError
      )
      expect(f.cluster.findGroupCoordinator).not.toHaveBeenCalled()
    })

    test.each([
      ['a partition number in place of an offset entry', [0]],
      [
        'a duplicate partition',
        [
          { partition: 0, offset: '1' },
          { partition: 0, offset: '2' },
        ],
      ],
      ['a hole in the partition list', Array(1)],
      ['a missing offset', [{ partition: 0 }]],
    ])('alter refuses %s before any coordinator request', async (title, partitions) => {
      f = fixture()
      await expect(
        f.admin.alterShareGroupOffsets({ groupId: 'g', topics: [{ topic: 'alpha', partitions }] })
      ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(f.cluster.findGroupCoordinator).not.toHaveBeenCalled()
    })

    test.each([
      ['an object in place of a name', [{ topic: 'alpha' }]],
      ['an empty name', ['']],
      ['a duplicate name', ['alpha', 'alpha']],
      ['a hole in the topic list', Array(1)],
    ])('delete refuses %s before any coordinator request', async (title, topics) => {
      f = fixture()
      await expect(
        f.admin.deleteShareGroupOffsets({ groupId: 'g', topics })
      ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(f.cluster.findGroupCoordinator).not.toHaveBeenCalled()
    })

    test.each([
      [
        'list refuses a valid partition followed by a negative one',
        'listShareGroupOffsets',
        [{ topic: 'alpha', partitions: [0, -1] }],
      ],
      [
        'list refuses a valid topic followed by one with an invalid partition',
        'listShareGroupOffsets',
        [
          { topic: 'alpha', partitions: [0] },
          { topic: 'beta', partitions: [1.5] },
        ],
      ],
      [
        'list refuses a valid topic followed by an empty topic name',
        'listShareGroupOffsets',
        [
          { topic: 'alpha', partitions: [0] },
          { topic: '', partitions: [0] },
        ],
      ],
      [
        'alter refuses a valid partition followed by an invalid offset',
        'alterShareGroupOffsets',
        [
          {
            topic: 'alpha',
            partitions: [
              { partition: 0, offset: '1' },
              { partition: 1, offset: '-1' },
            ],
          },
        ],
      ],
      [
        'alter refuses a valid topic followed by one with a duplicate partition',
        'alterShareGroupOffsets',
        [
          { topic: 'alpha', partitions: [{ partition: 0, offset: '1' }] },
          {
            topic: 'beta',
            partitions: [
              { partition: 0, offset: '1' },
              { partition: 0, offset: '2' },
            ],
          },
        ],
      ],
      [
        'alter refuses a valid topic followed by an empty topic name',
        'alterShareGroupOffsets',
        [
          { topic: 'alpha', partitions: [{ partition: 0, offset: '1' }] },
          { topic: '', partitions: [{ partition: 0, offset: '1' }] },
        ],
      ],
      [
        'delete refuses a valid topic followed by an empty name',
        'deleteShareGroupOffsets',
        ['alpha', ''],
      ],
      [
        'delete refuses a valid topic followed by a repeated name',
        'deleteShareGroupOffsets',
        ['alpha', 'beta', 'alpha'],
      ],
    ])('%s before discovery or any frame', async (title, method, topics) => {
      f = fixture()
      await expect(f.admin[method]({ groupId: 'share-group', topics })).rejects.toBeInstanceOf(
        KafkaJSInvalidConfigurationError
      )
      expect(f.cluster.findGroupCoordinator).not.toHaveBeenCalled()
      for (const wire of WIRE_METHODS) expect(f.brokers[0][wire]).not.toHaveBeenCalled()
    })

    test('snapshots caller selections before delayed discovery', async () => {
      const broker = coordinator()
      const held = gate()
      const asked = gate()
      f = fixture([broker])
      f.cluster.findGroupCoordinator.mockImplementation(() => {
        asked.resolve()
        return held.promise
      })
      const options = {
        groupId: 'share-group',
        topics: [{ topic: 'alpha', partitions: [{ partition: 0, offset: '8' }] }],
      }
      const pending = settle(f.admin.alterShareGroupOffsets(options))
      await asked.promise
      options.topics[0].partitions[0].offset = '9'
      options.topics[0].topic = 'changed'
      held.resolve(broker)
      expect(await pending).toHaveProperty('value')
      expect(broker.alterShareGroupOffsets.mock.calls[0][0]).toEqual({
        groupId: 'share-group',
        topics: [{ topicName: 'alpha', partitions: [{ partitionIndex: 0, startOffset: '8' }] }],
      })
    })
  })

  describe('deadlines', () => {
    test.each(CALLS)('%s validates apiTimeout before any discovery', async (...call) => {
      f = fixture()
      await expect(callWith(f.admin, call, { apiTimeout: 0 })).rejects.toBeInstanceOf(
        KafkaJSInvalidConfigurationError
      )
      expect(f.cluster.findGroupCoordinator).not.toHaveBeenCalled()
    })

    test.each(CALLS)(
      '%s rejects held discovery without publishing or sending late work',
      async (...call) => {
        jest.useFakeTimers()
        const broker = coordinator()
        const held = gate()
        const entered = gate()
        f = fixture([broker])
        f.cluster.findGroupCoordinator.mockImplementation(() => {
          entered.resolve()
          return held.promise
        })
        let outcome
        const pending = settle(callWith(f.admin, call, { apiTimeout: 5 })).then((result) => {
          outcome = result
        })
        await entered.promise
        await jest.advanceTimersByTimeAsync(4)
        expect(outcome).toBeUndefined()
        await jest.advanceTimersByTimeAsync(1)
        await pending
        expect(outcome.error).toBeInstanceOf(KafkaJSTimeout)
        held.resolve(broker)
        await flush()
        for (const wire of WIRE_METHODS) expect(broker[wire]).not.toHaveBeenCalled()
        expect(f.cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
      }
    )

    test('checks actual time before send with deadline timer delivery withheld', async () => {
      const broker = coordinator()
      f = fixture([broker])
      let now = 0
      jest.spyOn(performance, 'now').mockImplementation(() => now)
      f.cluster.findGroupCoordinator.mockImplementation(async () => {
        now = 1001
        return broker
      })
      await expect(
        f.admin.listShareGroupOffsets({ groupId: 'share-group', apiTimeout: 1000 })
      ).rejects.toBeInstanceOf(KafkaJSTimeout)
      expect(broker.describeShareGroupOffsets).not.toHaveBeenCalled()
    })

    test('a family call made inside an invocation reuses its token and budget', async () => {
      const broker = coordinator()
      f = fixture([broker])
      const family = createShareGroups({ cluster: f.cluster, retry })
      let outer
      const seen = []
      broker.describeShareGroupOffsets.mockImplementation(async () => {
        seen.push([currentDeadline().token, currentDeadline().timeoutMs])
        return { groups: [expected.describeShareGroupOffsetsV1.response.groups[0]] }
      })
      await runWithDeadline('outer', 100, async () => {
        outer = currentDeadline().token
        await family.listShareGroupOffsets({ groupId: 'share-group', apiTimeout: 500 })
      })
      expect(seen).toEqual([[outer, 100]])
    })

    test.each(CALLS)(
      '%s registers its apiTimeout at its own argument position',
      async (...call) => {
        f = fixture()
        const budgets = []
        const wire = brokerMethodOf(call[0])
        const send = f.brokers[0][wire].getMockImplementation()
        f.brokers[0][wire].mockImplementation(async (...args) => {
          budgets.push(currentDeadline().timeoutMs)
          return send(...args)
        })
        await callWith(f.admin, call)
        await callWith(f.admin, call, { apiTimeout: 123 })
        expect(budgets).toEqual([60000, 123])
      }
    )

    test('forwards each captured invocation guard to the Broker method', async () => {
      f = fixture()
      const tokens = []
      for (const call of CALLS) {
        const wire = brokerMethodOf(call[0])
        const send = f.brokers[0][wire].getMockImplementation()
        f.brokers[0][wire].mockImplementation(async (request, sendOptions) => {
          const operation = currentDeadline()
          tokens.push(operation.token)
          expect(sendOptions.guard).toBe(operation.guard)
          expect(sendOptions.guard()).toBeNull()
          return send(request)
        })
        await callWith(f.admin, call)
      }
      expect(new Set(tokens).size).toBe(4)
    })

    test('checks the original deadline after a late successful mutation response', async () => {
      const broker = coordinator()
      f = fixture([broker])
      let now = 0
      jest.spyOn(performance, 'now').mockImplementation(() => now)
      broker.alterShareGroupOffsets.mockImplementation(async () => {
        now = 1001
        return expected.alterShareGroupOffsetsV0.response
      })
      await expect(callWith(f.admin, ALTER, { apiTimeout: 1000 })).rejects.toBeInstanceOf(
        KafkaJSTimeout
      )
      expect(broker.alterShareGroupOffsets).toHaveBeenCalledTimes(1)
    })

    test('refuses actual deadline expiry after encode with transport enforcement disabled', async () => {
      f = await seam()
      let now = 0
      jest.spyOn(performance, 'now').mockImplementation(() => now)
      const broker = f.brokers[0]
      const lookup = broker.lookupRequest.bind(broker)
      jest.spyOn(broker, 'lookupRequest').mockImplementation((key, definition) => {
        const build = lookup(key, definition)
        return (fields) => {
          const built = build(fields)
          const encode = built.request.encode
          built.request.encode = async () => {
            const value = await encode()
            now = 1001
            return value
          }
          return built
        }
      })
      await expect(callWith(f.admin, ALTER, { apiTimeout: 1000 })).rejects.toBeInstanceOf(
        KafkaJSTimeout
      )
      expect(f.scripted.requests().some((r) => r.apiKey === 91)).toBe(false)
    })

    test('refuses original deadline expiry after coordinator socket reconnect', async () => {
      f = await seam()
      const connection = f.pools[0].pool[0]
      await connection.disconnect({ drainTimeout: 0 })
      let now = 0
      jest.spyOn(performance, 'now').mockImplementation(() => now)
      const connect = connection.connect.bind(connection)
      jest.spyOn(connection, 'connect').mockImplementation(async (...args) => {
        await connect(...args)
        now = 1001
      })
      await expect(callWith(f.admin, DELETE, { apiTimeout: 1000 })).rejects.toBeInstanceOf(
        KafkaJSTimeout
      )
      expect(f.scripted.requests().some((r) => r.apiKey === 92)).toBe(false)
    })

    test('rechecks the original deadline when a queued admin frame can transmit', async () => {
      f = await seam({ maxInFlightRequests: 1 })
      const connection = f.pools[0].pool[0]
      const held = gate()
      f.scripted.answer('ShareGroupDescribe', async () => {
        await held.promise
        return { throttleTime: 0, groups: [group('share-group')] }
      })
      const blocker = settle(f.brokers[0].shareGroupDescribe(expected.shareGroupDescribeV1.request))
      await until(() => f.scripted.requests().some((r) => r.apiKey === 77))
      let now = 0
      jest.spyOn(performance, 'now').mockImplementation(() => now)
      const queued = settle(callWith(f.admin, DELETE, { apiTimeout: 1000 }))
      try {
        await until(() => connection.requestQueue.pending.length === 1)
        now = 1001
        held.resolve()
        expect(await blocker).toHaveProperty('value')
        expect((await queued).error).toBeInstanceOf(KafkaJSTimeout)
        expect(f.scripted.requests().some((r) => r.apiKey === 92)).toBe(false)
      } finally {
        held.resolve()
        await blocker
        await queued
      }
    })
  })

  describe('wire', () => {
    test.each(APIS)(
      '%s writes the exact key/version and independent body on its assigned socket',
      async (method, api, key, version) => {
        f = await seam()
        const literal = expected[`${method}V${version}`]
        await f.brokers[0][method](literal.request)
        const records = f.scripted.requests().filter((r) => r.api === api)
        expect(records).toHaveLength(1)
        expect(records[0]).toMatchObject({ node: 0, apiKey: key, version, fields: literal.request })
        const slot = key === 78 || key === 79 ? 1 : 0
        const carrying = (index) =>
          (f.pools[0].pool[index].socket?.writes ?? []).filter(
            (bytes) => readRequestHeader(bytes).apiKey === key
          )
        expect(carrying(1 - slot)).toHaveLength(0)
        expect(carrying(slot)).toHaveLength(1)
        const [written] = carrying(slot)
        expect(written).toEqual(
          frame(
            key,
            version,
            readRequestHeader(written).correlationId,
            literal.requestHex,
            'share-admin'
          )
        )
        expect(f.pools[0].generationFor(key)).toBeGreaterThan(0)
      }
    )

    test('public admin discovers the GROUP coordinator for all four share methods', async () => {
      const scripted = script(
        createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }], profile: 'share' })
      )
      const admin = new Kafka(scripted.kafkaOptions({ retry })).admin()
      try {
        await admin.connect()
        for (const call of CALLS) await callWith(admin, call)
        const lookups = scripted.requests().filter((r) => r.api === 'FindCoordinator')
        expect(lookups).toHaveLength(4)
        expect(lookups.map((r) => r.fields)).toEqual(
          Array(4).fill({ keyType: 0, coordinatorKeys: ['share-group'] })
        )
        expect(
          scripted
            .requests()
            .filter((r) => [77, 90, 91, 92].includes(r.apiKey))
            .map((r) => r.apiKey)
        ).toEqual([77, 90, 91, 92])
      } finally {
        await admin.disconnect()
        scripted.assertClean()
      }
    })
  })

  describe('existing group APIs for a share group', () => {
    let harness
    afterEach(async () => {
      if (harness) await harness.pool.destroy()
      harness = null
    })

    /** An admin over one real Broker answering every request with `body`, which advertises only `versions`. */
    const adminOver = (versions, body) => {
      harness = createGroupApiBroker({ versions, respond: () => body })
      return createAdmin({
        logger: newLogger(),
        retry: { retries: 0 },
        cluster: {
          refreshMetadata: async () => {},
          findGroupCoordinator: async () => harness.broker,
          brokerPool: { brokers: { 0: harness.broker } },
          findBroker: async () => harness.broker,
          targetTopics: new Set(),
        },
      })
    }

    test('listGroups preserves share casing and sends the Share display filter', async () => {
      const response = new Encoder()
        .writeInt32(0)
        .writeInt16(0)
        .writeUVarIntArray([
          new Encoder()
            .writeUVarIntString('share')
            .writeUVarIntString('')
            .writeUVarIntString('Empty')
            .writeUVarIntString('Share')
            .writeTaggedFields(),
        ])
        .writeTaggedFields().buffer
      const admin = adminOver({ 16: { minVersion: 5, maxVersion: 5 } }, response)
      expect(await admin.listGroups({ types: ['share'] })).toEqual({
        groups: [{ groupId: 'share', protocolType: '', groupState: 'Empty', groupType: 'share' }],
      })
      expect(harness.requests[0]).toMatchObject({ apiKey: 16, apiVersion: 5 })
      expect(harness.requests[0].body.toString('hex')).toBe('010206536861726500')
      expect((await admin.listGroups()).groups[0].groupType).toBe('share')
      expect(harness.requests[1].body.toString('hex')).toBe('010100')
    })

    /** @returns {Buffer} a DeleteGroups v3 answer for one group */
    const deleteAnswer = (errorCode, errorMessage) =>
      new Encoder()
        .writeInt32(0)
        .writeUVarIntArray([
          new Encoder()
            .writeUVarIntString('share-group')
            .writeInt16(errorCode)
            .writeUVarIntString(errorMessage)
            .writeTaggedFields(),
        ])
        .writeTaggedFields().buffer

    test('deleteGroups sends a share group id in the generic v3 request and returns its typed result', async () => {
      const admin = adminOver({ 42: { minVersion: 0, maxVersion: 3 } }, deleteAnswer(0, null))
      expect(await admin.deleteGroups(['share-group'])).toEqual([
        { groupId: 'share-group', errorCode: 0, errorMessage: null },
      ])
      expect(harness.requests).toHaveLength(1)
      expect(harness.requests[0]).toMatchObject({ apiKey: 42, apiVersion: 3 })
      expect(harness.requests[0].body.toString('hex')).toBe('020c73686172652d67726f757000')
      expect(admin.deleteShareGroups).toBeUndefined()
    })

    test('deleteGroups reports a non-empty share group as NON_EMPTY_GROUP with the broker message', async () => {
      const admin = adminOver(
        { 42: { minVersion: 0, maxVersion: 3 } },
        deleteAnswer(68, 'The group is not empty.')
      )
      const error = await admin.deleteGroups(['share-group']).catch((e) => e)
      expect(error).toBeInstanceOf(KafkaJSDeleteGroupsError)
      expect(error.groups).toEqual([
        {
          groupId: 'share-group',
          errorCode: 68,
          error: expect.objectContaining({ type: 'NON_EMPTY_GROUP', code: 68 }),
          errorMessage: 'The group is not empty.',
        },
      ])
      expect(harness.requests[0].apiVersion).toBe(3)
    })
  })
})
