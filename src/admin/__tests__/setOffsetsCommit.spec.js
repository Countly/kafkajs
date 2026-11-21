const { Kafka } = require('../../../index')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')
const { describeGroupAs } = require('./fixtures/describeGroupAs')

const NODES = [
  { id: 0, host: 'node0', port: 9092 },
  { id: 1, host: 'node1', port: 9092 },
]
const RETRY = { retries: 2, initialRetryTime: 1, maxRetryTime: 2, factor: 0, multiplier: 1 }
/** The requests a member of the group would send; the admin sends none of them. */
const MEMBERSHIP = [
  'JoinGroup',
  'SyncGroup',
  'Heartbeat',
  'LeaveGroup',
  'ConsumerGroupHeartbeat',
  'Fetch',
  'OffsetFetch',
]

describe('Admin > setOffsets > commits the offsets itself', () => {
  let scripted, admin, described, release

  /**
   * @param {object} [options] Kafka options over the scripted cluster's
   * @returns {Promise<object>} a connected admin
   */
  const connect = async (options = {}) => {
    admin = new Kafka(scripted.kafkaOptions({ retry: RETRY, ...options })).admin()
    await admin.connect()
    return admin
  }

  /**
   * Answers every OffsetCommit partition with a code.
   * @param {(partition: number, attempt: number) => number} codeOf the code of a partition on the
   *   attempt-th OffsetCommit (from 1)
   * @returns {void}
   */
  const answerCommit = (codeOf) => {
    let attempt = 0
    scripted.answer('OffsetCommit', ({ topics }) => {
      attempt += 1
      return {
        topics: topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({
            partition,
            errorCode: codeOf(partition, attempt),
          })),
        })),
      }
    })
  }

  /**
   * Runs a call to its end, then lets the clock run on for `after` milliseconds so a late write
   * would show. Real time: the scripted cluster answers on later event-loop turns, which fake time
   * does not drive.
   * @param {() => Promise<unknown>} call
   * @param {number} [after=0]
   * @returns {Promise<Error|undefined>} what the call rejected with
   */
  const outcomeOf = async (call, after = 0) => {
    const error = await call().then(
      () => undefined,
      (e) => e
    )
    await new Promise((resolve) => setTimeout(resolve, after))
    return error
  }

  const commits = () => scripted.requests().filter(({ api }) => api === 'OffsetCommit')
  const apis = () => scripted.requests().map(({ api }) => api)
  /** @returns {string[]} the APIs sent after request `from` and before request `to` */
  const between = (from, to) =>
    scripted
      .requests()
      .slice(scripted.requests().indexOf(from) + 1, scripted.requests().indexOf(to))
      .map(({ api }) => api)
  /** @returns {Array<Array<[number, string]>>} the partition and offset cells of each commit */
  const cells = () =>
    commits().map(({ fields }) =>
      fields.topics.flatMap(({ partitions }) =>
        partitions.map(({ partition, offset }) => [partition, offset])
      )
    )
  const set = (partitions, groupId = 'reset-group') =>
    admin.setOffsets({ groupId, topic: TOPIC, partitions })
  const twoPartitions = [
    { partition: 0, offset: '3' },
    { partition: 1, offset: '4' },
  ]

  beforeEach(() => {
    release = []
    scripted = groupScript(createScriptedCluster({ nodes: NODES }), {
      partitions: 2,
      committed: { 0: '13', 1: '13' },
    })
    described = describeGroupAs()
  })

  afterEach(async () => {
    for (const settle of release) settle()
    described.restore()
    await admin?.disconnect()
    admin = null
  })

  test('one OffsetCommit with no member and generation -1 carries the last entry of each partition, with no group membership', async () => {
    await connect()

    await expect(
      set([
        { partition: 0, offset: '3' },
        { partition: 1, offset: '-2', metadata: 'kept out', leaderEpoch: 9 },
        { partition: 0, offset: '5' },
      ])
    ).resolves.toBeUndefined()

    expect(commits()).toHaveLength(1)
    expect(commits()[0].fields).toEqual({
      groupId: 'reset-group',
      groupGenerationId: -1,
      memberId: '',
      groupInstanceId: null,
      topics: [
        {
          topic: TOPIC,
          partitions: [
            { partition: 0, offset: '5', leaderEpoch: -1, metadata: null },
            { partition: 1, offset: '-2', leaderEpoch: -1, metadata: null },
          ],
        },
      ],
    })
    expect(apis().filter((api) => MEMBERSHIP.includes(api))).toEqual([])
  })

  test('an OffsetCommit that is never answered rejects with the request timeout once the retries are spent', async () => {
    scripted.answer('OffsetCommit', () => new Promise((resolve) => release.push(resolve)))
    await connect({ requestTimeout: 300, retry: { ...RETRY, retries: 1 } })

    const error = await outcomeOf(() => set(twoPartitions))

    expect(error).toMatchObject({ name: 'KafkaJSRequestTimeoutError' })
    expect(error.errors).toBeUndefined()
    expect(commits()).toHaveLength(2)
    expect(between(commits()[0], commits()[1])).toContain('FindCoordinator')
  })

  test('a retriable code then no answer on the last attempt rejects with the aggregate, leaving the confirmed partition out', async () => {
    let attempt = 0
    scripted.answer('OffsetCommit', ({ topics }) => {
      attempt += 1
      if (attempt > 1) return new Promise((resolve) => release.push(resolve))
      return {
        topics: topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({
            partition,
            errorCode: partition === 1 ? 14 : 0,
          })),
        })),
      }
    })
    await connect({ requestTimeout: 300, retry: { ...RETRY, retries: 1 } })

    const error = await outcomeOf(() => set(twoPartitions))

    expect(error.name).toBe('KafkaJSAggregateError')
    expect(error.errors.map(({ partition, topic }) => [partition, topic])).toEqual([[1, TOPIC]])
    expect(error.errors[0].cause.name).toBe('KafkaJSRequestTimeoutError')
    expect(cells()).toEqual([
      [
        [0, '3'],
        [1, '4'],
      ],
      [[1, '4']],
    ])
  })

  test('a code this client does not know, on one partition, rejects with the aggregate naming that partition, and the confirmed one is not sent again', async () => {
    answerCommit((partition) => (partition === 1 ? 9999 : 0))
    await connect()

    const error = await set(twoPartitions).catch((e) => e)

    expect(error.name).toBe('KafkaJSAggregateError')
    expect(error.message).toBe(
      'Offsets not committed for 1 of 2 partitions: 1 (KAFKAJS_UNKNOWN_ERROR_CODE)'
    )
    expect(error.errors.map(({ partition, topic }) => [partition, topic])).toEqual([[1, TOPIC]])
    expect(cells()).toEqual([
      [
        [0, '3'],
        [1, '4'],
      ],
    ])
  })

  test('every partition answered REQUEST_TIMED_OUT rejects with one child per partition and is not retried', async () => {
    answerCommit(() => 7)
    await connect()

    const error = await set(twoPartitions).catch((e) => e)

    expect(error).toMatchObject({ name: 'KafkaJSAggregateError' })
    expect(error.errors.map(({ partition, type, topic }) => [partition, type, topic])).toEqual([
      [0, 'REQUEST_TIMED_OUT', TOPIC],
      [1, 'REQUEST_TIMED_OUT', TOPIC],
    ])
    expect(commits()).toHaveLength(1)
  })

  test('one failed partition rejects naming only that partition, and the other is not sent again', async () => {
    answerCommit((partition) => (partition === 1 ? 12 : 0))
    await connect()

    const error = await set(twoPartitions).catch((e) => e)

    expect(error.name).toBe('KafkaJSAggregateError')
    expect(error.message).toBe(
      'Offsets not committed for 1 of 2 partitions: 1 (OFFSET_METADATA_TOO_LARGE)'
    )
    expect(error.errors.map(({ partition, code }) => [partition, code])).toEqual([[1, 12]])
    expect(cells()).toEqual([
      [
        [0, '3'],
        [1, '4'],
      ],
    ])
  })

  test.each([
    [14, 'GROUP_LOAD_IN_PROGRESS'],
    [27, 'REBALANCE_IN_PROGRESS'],
  ])('a partition answered %i (%s) is sent again alone, to the same coordinator', async (code) => {
    answerCommit((partition, attempt) => (partition === 1 && attempt === 1 ? code : 0))
    await connect()
    await expect(set(twoPartitions)).resolves.toBeUndefined()

    expect(cells()).toEqual([
      [
        [0, '3'],
        [1, '4'],
      ],
      [[1, '4']],
    ])
    const between = scripted
      .requests()
      .slice(scripted.requests().indexOf(commits()[0]), scripted.requests().indexOf(commits()[1]))
      .map(({ api }) => api)
    expect(between).not.toContain('FindCoordinator')
    expect(commits().map(({ node }) => node)).toEqual([0, 0])
  })

  test.each([
    [15, 'GROUP_COORDINATOR_NOT_AVAILABLE'],
    [16, 'NOT_COORDINATOR_FOR_GROUP'],
  ])(
    'a coordinator that answered %i (%s) is looked up again and the retry goes to the new one',
    async (code) => {
      scripted.answer('FindCoordinator', ({ coordinatorKeys }) => {
        const node = NODES[commits().length > 0 ? 1 : 0]
        return {
          coordinators: coordinatorKeys.map((key) => ({
            key,
            nodeId: node.id,
            host: node.host,
            port: node.port,
            errorCode: 0,
            errorMessage: null,
          })),
        }
      })
      answerCommit((partition, attempt) => (attempt === 1 ? code : 0))
      await connect()

      await expect(set(twoPartitions)).resolves.toBeUndefined()

      expect(commits().map(({ node }) => node)).toEqual([0, 1])
      const between = scripted
        .requests()
        .slice(scripted.requests().indexOf(commits()[0]), scripted.requests().indexOf(commits()[1]))
        .map(({ api }) => api)
      expect(between).toContain('FindCoordinator')
    }
  )

  test('a coordinator that keeps moving rejects with NOT_COORDINATOR_FOR_GROUP (16) children after every attempt', async () => {
    answerCommit(() => 16)
    await connect()

    const error = await set(twoPartitions).catch((e) => e)

    expect(error.name).toBe('KafkaJSAggregateError')
    expect(error.errors.map(({ partition, type }) => [partition, type])).toEqual([
      [0, 'NOT_COORDINATOR_FOR_GROUP'],
      [1, 'NOT_COORDINATOR_FOR_GROUP'],
    ])
    expect(commits()).toHaveLength(RETRY.retries + 1)
  })

  test('a member that joined after the precheck makes the coordinator refuse with UNKNOWN_MEMBER_ID, and the admin never joins', async () => {
    answerCommit(() => 25)
    await connect()

    const error = await set(twoPartitions).catch((e) => e)

    expect(error.errors.map(({ partition, type }) => [partition, type])).toEqual([
      [0, 'UNKNOWN_MEMBER_ID'],
      [1, 'UNKNOWN_MEMBER_ID'],
    ])
    expect(commits()).toHaveLength(1)
    expect(apis().filter((api) => MEMBERSHIP.includes(api))).toEqual([])
  })

  test('GROUP_AUTHORIZATION_FAILED on every partition rejects after one request', async () => {
    answerCommit(() => 30)
    await connect()

    const error = await set(twoPartitions).catch((e) => e)

    expect(error.name).toBe('KafkaJSAggregateError')
    expect(error.errors.map(({ type }) => type)).toEqual([
      'GROUP_AUTHORIZATION_FAILED',
      'GROUP_AUTHORIZATION_FAILED',
    ])
    expect(commits()).toHaveLength(1)
  })

  test('a partition confirmed, one refused and one retried then unanswered: one child per failed or unconfirmed partition', async () => {
    scripted = groupScript(createScriptedCluster({ nodes: NODES }), { partitions: 3 })
    let attempt = 0
    scripted.answer('OffsetCommit', ({ topics }) => {
      attempt += 1
      if (attempt > 1) return new Promise((resolve) => release.push(resolve))
      return {
        topics: topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({
            partition,
            errorCode: [0, 12, 14][partition],
          })),
        })),
      }
    })
    await connect({ requestTimeout: 300, retry: { ...RETRY, retries: 1 } })

    const error = await outcomeOf(() => set([...twoPartitions, { partition: 2, offset: '5' }]))

    expect(error.name).toBe('KafkaJSAggregateError')
    expect(error.errors.map(({ partition, topic }) => [partition, topic])).toEqual([
      [1, TOPIC],
      [2, TOPIC],
    ])
    expect(error.errors[0].type).toBe('OFFSET_METADATA_TOO_LARGE')
    expect(error.errors[1].cause.name).toBe('KafkaJSRequestTimeoutError')
    expect(error.message).toContain('2 (not confirmed: ')
    expect(cells()).toEqual([
      [
        [0, '3'],
        [1, '4'],
        [2, '5'],
      ],
      [[2, '5']],
    ])
  })

  describe('the API deadline', () => {
    const call = { groupId: 'reset-group', topic: TOPIC }

    test('passing while a retry waits rejects with the admin timeout and writes no other commit', async () => {
      answerCommit(() => 14)
      await connect({ retry: { ...RETRY, initialRetryTime: 1200, maxRetryTime: 1200 } })

      const error = await outcomeOf(
        () => admin.setOffsets({ ...call, partitions: twoPartitions, apiTimeout: 1000 }),
        1300
      )

      expect(error.name).toBe('KafkaJSTimeout')
      expect(commits()).toHaveLength(1)
    })

    test('passing while the commit answer is held rejects with the admin timeout and writes no other commit', async () => {
      scripted.answer('OffsetCommit', () => new Promise((resolve) => release.push(resolve)))
      await connect()

      const error = await outcomeOf(() =>
        admin.setOffsets({ ...call, partitions: twoPartitions, apiTimeout: 1000 })
      )

      expect(error.name).toBe('KafkaJSTimeout')
      for (const settle of release.splice(0)) settle()
      await new Promise((resolve) => setImmediate(resolve))
      expect(cells()).toEqual([
        [
          [0, '3'],
          [1, '4'],
        ],
      ])
    })
  })

  test.each([
    ['leaves a partition out', () => [{ partition: 0, errorCode: 0 }]],
    [
      'names a partition twice',
      () => [
        { partition: 0, errorCode: 0 },
        { partition: 0, errorCode: 0 },
        { partition: 1, errorCode: 0 },
      ],
    ],
    [
      'names a partition that was not sent',
      () => [
        { partition: 0, errorCode: 0 },
        { partition: 1, errorCode: 0 },
        { partition: 9, errorCode: 0 },
      ],
    ],
  ])('an answer that %s is rejected as malformed, never taken as success', async (_, rows) => {
    scripted.answer('OffsetCommit', () => ({ topics: [{ topic: TOPIC, partitions: rows() }] }))
    await connect()

    await expect(set(twoPartitions)).rejects.toMatchObject({ name: 'KafkaJSInvariantViolation' })
    expect(commits()).toHaveLength(1)
  })

  test('an answer for another topic is rejected as malformed', async () => {
    scripted.answer('OffsetCommit', () => ({
      topics: [
        {
          topic: 'other-topic',
          partitions: [
            { partition: 0, errorCode: 0 },
            { partition: 1, errorCode: 0 },
          ],
        },
      ],
    }))
    await connect()

    await expect(set(twoPartitions)).rejects.toMatchObject({ name: 'KafkaJSInvariantViolation' })
  })

  describe('the precheck', () => {
    test('a group with running members is refused with the unchanged busy-group message before any commit', async () => {
      described.restore()
      described = describeGroupAs({
        classic: { state: 'Stable', members: [] },
      })
      await connect()

      await expect(set(twoPartitions)).rejects.toThrow(
        'The consumer group must have no running instances, current state: Stable'
      )
      expect(commits()).toHaveLength(0)
    })

    test.each([
      ['Dead with code 69, as DescribeGroups v6 answers', { state: 'Dead', errorCode: 69 }],
      [
        'Dead with code 0, as older DescribeGroups versions answer',
        { state: 'Dead', errorCode: 0 },
      ],
    ])('a group that does not exist (%s) is pre-seeded', async (_, classic) => {
      described.restore()
      described = describeGroupAs({ classic: { ...classic, protocolType: '' } })
      await connect()

      await expect(set(twoPartitions)).resolves.toBeUndefined()
      expect(commits()).toHaveLength(1)
      expect(commits()[0].fields).toMatchObject({ memberId: '', groupGenerationId: -1 })
      expect(described.calls).toEqual(['ConsumerGroupDescribe', 'DescribeGroups'])
    })

    test('a group that does not exist on a cluster without the consumer protocol (ConsumerGroupDescribe answers 35) is pre-seeded', async () => {
      described.restore()
      described = describeGroupAs({
        consumer: { errorCode: 35 },
        classic: { state: 'Dead', errorCode: 69, protocolType: '' },
      })
      await connect()

      await expect(set(twoPartitions)).resolves.toBeUndefined()
      expect(commits()).toHaveLength(1)
      expect(described.calls).toEqual(['ConsumerGroupDescribe', 'DescribeGroups'])
    })

    test('a description refused with another code rejects with that error and commits nothing', async () => {
      described.restore()
      described = describeGroupAs({ consumer: { errorCode: 30, groupState: '' } })
      await connect()

      await expect(set(twoPartitions)).rejects.toMatchObject({
        type: 'GROUP_AUTHORIZATION_FAILED',
      })
      expect(commits()).toHaveLength(0)
    })

    test('an empty consumer-protocol group gets the commit with no join and no heartbeat', async () => {
      described.restore()
      described = describeGroupAs({
        consumer: { errorCode: 0, groupState: 'Empty', groupEpoch: 4, assignmentEpoch: 4 },
      })
      await connect()

      await expect(set(twoPartitions)).resolves.toBeUndefined()
      expect(commits()[0].fields).toMatchObject({ memberId: '', groupGenerationId: -1 })
      expect(described.calls).toEqual(['ConsumerGroupDescribe'])
      expect(apis().filter((api) => MEMBERSHIP.includes(api))).toEqual([])
    })
  })

  describe('refused before any request', () => {
    test.each([
      ['abc', 'Invalid offset, expected a long received abc'],
      ['1.5', 'Invalid offset, expected a long received 1.5'],
      ['-5', 'Offset must not be a negative number'],
      ['9223372036854775808', 'Invalid offset, expected a long received 9223372036854775808'],
      ['NaN', 'Invalid offset, expected a long received NaN'],
      ['Infinity', 'Invalid offset, expected a long received Infinity'],
      ['-Infinity', 'Invalid offset, expected a long received -Infinity'],
      ['   ', 'Invalid offset, expected a long received    '],
      ['', 'Invalid offset, expected a long received '],
      ['0x1F', 'Invalid offset, expected a long received 0x1F'],
      ['0b11', 'Invalid offset, expected a long received 0b11'],
      ['\t7\n', 'Invalid offset, expected a long received \t7\n'],
      [NaN, 'Invalid offset, expected a long received NaN'],
      [Infinity, 'Invalid offset, expected a long received Infinity'],
      [1.5, 'Invalid offset, expected a long received 1.5'],
      [2 ** 53, 'Invalid offset, expected a long received 9007199254740992'],
      [true, 'Invalid offset, expected a long received true'],
      [false, 'Invalid offset, expected a long received false'],
      [[], 'Invalid offset, expected a long received '],
      [null, 'Invalid offset, expected a long received null'],
      [undefined, 'Invalid offset, expected a long received undefined'],
    ])('the offset %p', async (offset, message) => {
      await connect()
      const before = scripted.requests().length

      await expect(
        set([
          { partition: 0, offset: '3' },
          { partition: 1, offset },
        ])
      ).rejects.toMatchObject({ name: 'KafkaJSNonRetriableError', message })

      expect(scripted.requests().slice(before)).toEqual([])
    })
  })

  test('a safe integer number offset is sent in decimal', async () => {
    await connect()

    await set([
      { partition: 0, offset: 5 },
      { partition: 1, offset: -2 },
    ])

    expect(cells()).toEqual([
      [
        [0, '5'],
        [1, '-2'],
      ],
    ])
  })

  test('the largest int64 offset is sent as given', async () => {
    await connect()

    await set([{ partition: 0, offset: '9223372036854775807' }])

    expect(cells()).toEqual([[[0, '9223372036854775807']]])
    expect(commits()[0].fields.groupGenerationId).toBe(-1)
  })

  test('a topic the cluster does not know rejects with UNKNOWN_TOPIC_OR_PARTITION and commits nothing', async () => {
    await connect()
    scripted.answer('Metadata', () => ({
      brokers: NODES.map(({ id, host, port }) => ({ nodeId: id, host, port, rack: null })),
      controllerId: 0,
      topics: [],
    }))

    await expect(set(twoPartitions)).rejects.toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })
    expect(commits()).toHaveLength(0)
  })

  test('a named partition the topic does not have is sent, and its UNKNOWN_TOPIC_OR_PARTITION refusal rejects naming it while the others are committed', async () => {
    answerCommit((partition) => (partition >= 2 ? 3 : 0))
    await connect()

    const error = await set([
      { partition: 0, offset: '3' },
      { partition: 7, offset: '1' },
      { partition: 1, offset: '4' },
    ]).catch((e) => e)

    expect(error.name).toBe('KafkaJSAggregateError')
    expect(error.message).toBe(
      'Offsets not committed for 1 of 3 partitions: 7 (UNKNOWN_TOPIC_OR_PARTITION)'
    )
    expect(error.errors.map(({ partition, code }) => [partition, code])).toEqual([[7, 3]])
    expect(cells()).toEqual([
      [
        [0, '3'],
        [1, '4'],
        [7, '1'],
      ],
    ])
  })

  test('a call naming only partitions the topic does not have rejects with UNKNOWN_TOPIC_OR_PARTITION for each', async () => {
    answerCommit((partition) => (partition >= 2 ? 3 : 0))
    await connect()

    const error = await set([{ partition: 5, offset: '1' }]).catch((e) => e)

    expect(error.name).toBe('KafkaJSAggregateError')
    expect(error.errors.map(({ partition, type }) => [partition, type])).toEqual([
      [5, 'UNKNOWN_TOPIC_OR_PARTITION'],
    ])
    expect(commits()).toHaveLength(1)
    expect(apis().filter((api) => MEMBERSHIP.includes(api))).toEqual([])
  })

  test('partitions are committed and reported in numeric order on a topic of more than ten partitions', async () => {
    scripted = groupScript(createScriptedCluster({ nodes: NODES }), { partitions: 12 })
    answerCommit(() => 25)
    await connect()
    const order = Array.from({ length: 12 }, (_, partition) => partition)

    const error = await admin.resetOffsets({ groupId: 'reset-group', topic: TOPIC }).catch((e) => e)
    const listed = await set([
      { partition: 10, offset: '1' },
      { partition: 2, offset: '1' },
    ]).catch((e) => e)

    expect(cells()[0].map(([partition]) => partition)).toEqual(order)
    expect(error.errors.map(({ partition }) => partition)).toEqual(order)
    expect(cells()[1].map(([partition]) => partition)).toEqual([2, 10])
    expect(listed.errors.map(({ partition }) => partition)).toEqual([2, 10])
  })

  test.each([
    ['resetOffsets', () => admin.resetOffsets({ groupId: 'reset-group', topic: TOPIC })],
    [
      'fetchOffsets with resolveOffsets',
      () => admin.fetchOffsets({ groupId: 'reset-group', topics: [TOPIC], resolveOffsets: true }),
    ],
  ])('%s rejects with the same aggregate', async (_, call) => {
    answerCommit(() => 25)
    await connect()

    const error = await call().catch((e) => e)

    expect(error.name).toBe('KafkaJSAggregateError')
    expect(error.errors.map(({ partition, type }) => [partition, type])).toEqual([
      [0, 'UNKNOWN_MEMBER_ID'],
      [1, 'UNKNOWN_MEMBER_ID'],
    ])
    expect(commits()[0].fields.topics[0].partitions.map(({ metadata }) => metadata)).toEqual([
      null,
      null,
    ])
  })
})

describe('Admin > setOffsets > commits the offsets itself, over a stub coordinator', () => {
  const createAdmin = require('..')
  const { ON_DECODED } = require('../../broker/privateOptions')
  const { createErrorFromCode } = require('../../protocol/error')
  const { createGroupDescriptions } = require('../groupDescriptions')
  const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
  const reply = { responses: [{ topic: 't', partitions: [{ partition: 0, errorCode: 0 }] }] }
  const request = { groupId: 'g', topic: 't', partitions: [{ partition: 0, offset: '1' }] }

  /**
   * @param {Function} offsetCommit the coordinator's OffsetCommit
   * @returns {{ admin: object, coordinator: object }} an admin over one stub coordinator
   */
  const adminOver = (offsetCommit, findGroupCoordinator) => {
    const coordinator = {
      offsetCommit: jest.fn(offsetCommit),
      consumerGroupDescribe: async ({ groupIds }) => ({
        groups: groupIds.map((groupId) => ({ groupId, errorCode: 0, groupState: 'Empty' })),
      }),
    }
    const cluster = {
      addTargetTopic: async () => {},
      refreshMetadataIfNecessary: async () => {},
      findTopicPartitionMetadata: () => [{ partitionId: 0 }, { partitionId: 1 }],
      findGroupCoordinator: findGroupCoordinator
        ? jest.fn(() => findGroupCoordinator(coordinator))
        : async () => coordinator,
    }
    return { admin: createAdmin({ cluster, logger, retry: RETRY }), coordinator }
  }

  afterEach(() => jest.restoreAllMocks())

  test('a reply the request never reported as decoded is rejected as malformed', async () => {
    const { admin } = adminOver(async () => reply)

    await expect(admin.setOffsets(request)).rejects.toMatchObject({
      name: 'KafkaJSInvariantViolation',
    })
  })

  test("a protocol error that is not the decoded reply's first failed partition is sent again", async () => {
    let attempt = 0
    const { admin, coordinator } = adminOver(async (_, options) => {
      options[ON_DECODED](reply)
      attempt += 1
      if (attempt === 1) throw createErrorFromCode(16)
      return reply
    })

    await expect(admin.setOffsets(request)).resolves.toBeUndefined()
    expect(coordinator.offsetCommit).toHaveBeenCalledTimes(2)
  })

  test('a sign-in failure while looking the coordinator up again rejects with the aggregate, leaving the confirmed partition out', async () => {
    const { KafkaJSSASLAuthenticationError } = require('../../errors')
    const moved = {
      responses: [
        {
          topic: 't',
          partitions: [
            { partition: 0, errorCode: 0 },
            { partition: 1, errorCode: 16 },
          ],
        },
      ],
    }
    let lookups = 0
    const { admin, coordinator } = adminOver(
      async (_, options) => {
        options[ON_DECODED](moved)
        throw Object.assign(createErrorFromCode(16), { topic: 't', partition: 1 })
      },
      async (found) => {
        lookups += 1
        if (lookups === 1) return found
        throw new KafkaJSSASLAuthenticationError('sign-in refused')
      }
    )

    const error = await admin
      .setOffsets({
        groupId: 'g',
        topic: 't',
        partitions: [
          { partition: 0, offset: '1' },
          { partition: 1, offset: '1' },
        ],
      })
      .catch((e) => e)

    expect(error.name).toBe('KafkaJSAggregateError')
    expect(error.errors.map(({ partition }) => partition)).toEqual([1])
    expect(error.errors[0].cause.name).toBe('KafkaJSSASLAuthenticationError')
    expect(coordinator.offsetCommit).toHaveBeenCalledTimes(1)
  })

  test('a group the coordinator does not describe is refused before any commit', async () => {
    const describer = Object.getPrototypeOf(createGroupDescriptions({ logger }))
    jest.spyOn(describer, 'describe').mockResolvedValue({ groups: [] })
    const { admin, coordinator } = adminOver(async () => reply)

    await expect(admin.setOffsets(request)).rejects.toThrow(
      'The coordinator described no group for g'
    )
    expect(coordinator.offsetCommit).not.toHaveBeenCalled()
  })
})
