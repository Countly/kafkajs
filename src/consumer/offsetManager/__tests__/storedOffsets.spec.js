const OffsetManager = require('../index')
const OffsetOperations = require('../offsetOperations')
const ResetObligations = require('../resetObligations')
const CommittedOffsetReader = require('../../committedOffsets')
const dictionary = require('../../../utils/dictionary')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { createGroupApiBroker } = require('../../../broker/__tests__/fixtures/groupApiHarness')
const apiKeys = require('../../../protocol/requests/apiKeys')
const offsetFetchV9 = require('../../../../testHelpers/wire/offsetFetchV9')

const TOPIC = 'test-topic'
const TOPIC_ID = 'id-of-the-topic'
const GROUP = 'test-group'
const WARNING =
  "Stored committed offset is not a valid offset and is treated as no committed offset, so the reset policy applies. Set the offset with admin.setOffsets or admin.resetOffsets, or subscribe with offsetReset 'none' to halt the partition instead"

/**
 * @param {number} partition
 * @param {string} offset
 * @returns {object} one partition row of an OffsetFetch answer
 */
const cell = (partition, offset) => ({
  partition,
  offset,
  leaderEpoch: -1,
  metadata: null,
  errorCode: 0,
})

/**
 * @param {number} partition
 * @param {string} offset
 * @returns {object} one partition row of an OffsetFetch answer, as the codec decodes it
 */
const decoded = (partition, offset) => ({
  partition,
  offset,
  committedLeaderEpoch: -1,
  metadata: null,
  errorCode: 0,
})

const offsetFetches = ({ requests }) => requests.filter((r) => r.apiKey === apiKeys.OffsetFetch)

const flush = () => new Promise(setImmediate)

/**
 * Replaces the manager's coordinator with one that holds every OffsetFetch until the case answers.
 * @param {OffsetManager} manager
 * @returns {((rows: object[]) => void)[]} one answer per request, in send order; each resolves its
 *   request with `rows` for `TOPIC`
 */
const holdOffsetFetches = (manager) => {
  const answers = []
  manager.getCoordinator = async () => ({
    offsetFetch: () =>
      new Promise((resolve) => {
        answers.push((rows) => resolve({ responses: [{ topic: TOPIC, partitions: rows }] }))
      }),
  })
  return answers
}

describe('Consumer > stored committed offsets', () => {
  const harnesses = []

  afterEach(async () => {
    await Promise.all(harnesses.splice(0).map(({ pool }) => pool.destroy()))
  })

  /**
   * A real broker over a fake socket that answers every OffsetFetch with `offset` for partition 0
   * of `TOPIC`, encoded at v9.
   * @param {string} offset the stored offset the answer carries
   * @returns {object} the harness
   */
  const coordinatorStoring = (offset) => {
    const harness = createGroupApiBroker({
      versions: { [apiKeys.OffsetFetch]: { minVersion: 0, maxVersion: 9 } },
      respond: () =>
        offsetFetchV9.buildResponse({
          throttleTime: 0,
          groupId: GROUP,
          errorCode: 0,
          topics: [{ topic: TOPIC, partitions: [cell(0, offset)] }],
        }),
    })
    harnesses.push(harness)
    return harness
  }

  /**
   * A manager over `partitions` of `TOPIC` whose coordinator stores `offset` for partition 0.
   * @param {string} offset the stored offset
   * @param {object} [options]
   * @param {number[]} [options.partitions] the assigned partitions
   * @param {string} [options.policy] the topic's reset policy
   * @returns {{ manager: OffsetManager, cache: object, lookup: jest.Mock, harness: object,
   *   logger: object }} `lookup` is called for every ListOffsets lookup the manager starts
   */
  const createMember = (offset, { partitions = [0], policy = 'earliest' } = {}) => {
    const harness = coordinatorStoring(offset)
    const cache = dictionary()
    const operations = new OffsetOperations()
    const obligations = new ResetObligations()
    const lookup = jest.fn(async () => [
      { topic: TOPIC, partitions: [{ partition: 0, offset: '4' }] },
    ])
    const logger = { debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() }
    const manager = new OffsetManager({
      cluster: {
        committedOffsets: () => cache,
        topicId: () => TOPIC_ID,
        fetchTopicsOffset: lookup,
      },
      coordinator: harness.broker,
      memberAssignment: { [TOPIC]: partitions },
      autoCommit: false,
      autoCommitInterval: null,
      autoCommitThreshold: null,
      topicConfigurations: { [TOPIC]: { resetPolicy: { kind: policy } } },
      instrumentationEmitter: new InstrumentationEventEmitter(),
      groupId: GROUP,
      generationId: 1,
      memberId: 'member-1',
      operations,
      obligations,
      logger,
    })
    manager.getCoordinator = async () => harness.broker
    obligations.attachTo(operations)
    return { manager, cache, lookup, harness, logger }
  }

  /**
   * @param {string} offset the stored offset
   * @returns {{ reader: CommittedOffsetReader, harness: object }} the reader `consumer.committed`
   *   builds, over a real broker that stores `offset` for partition 0 of `TOPIC`
   */
  const createReader = (offset) => {
    const harness = coordinatorStoring(offset)
    const cluster = { findGroupCoordinator: async () => harness.broker }
    return { reader: new CommittedOffsetReader({ cluster, groupId: GROUP, retry: {} }), harness }
  }

  test.each([
    ['earliest', true],
    ['latest', false],
  ])(
    'a stored -500 under %s is treated as no committed offset: the reset policy positions the partition and no further OffsetFetch is sent',
    async (policy, fromBeginning) => {
      const { manager, cache, lookup, harness } = createMember('-500', { policy })

      await expect(manager.resolveOffsets()).resolves.toBe(true)

      expect(lookup).toHaveBeenCalledTimes(1)
      expect(lookup).toHaveBeenCalledWith([
        { topic: TOPIC, partitions: [{ partition: 0 }], fromBeginning },
      ])
      expect(manager.heldOffsetAt(TOPIC, 0)).toBe('4')
      expect(cache[TOPIC]?.[0]).toBeUndefined()

      await expect(manager.resolveOffsets()).resolves.toBe(true)
      expect(offsetFetches(harness)).toHaveLength(1)
      expect(lookup).toHaveBeenCalledTimes(1)
    }
  )

  test('a stored -500 under none halts that partition without a lookup while a sibling keeps its stored offset', async () => {
    const { manager, cache, lookup } = createMember('-500', { partitions: [0, 1], policy: 'none' })
    const answers = holdOffsetFetches(manager)
    const resolving = manager.resolveOffsets()
    await flush()

    answers[0]([decoded(0, '-500'), decoded(1, '10')])
    await expect(resolving).resolves.toBe(true)

    expect(lookup).not.toHaveBeenCalled()
    expect(cache[TOPIC]).toEqual({ 1: '10' })
    expect(manager.heldOffsetAt(TOPIC, 0)).toBeUndefined()
    expect(
      manager.takeResetRefusals().map(({ topic, partition, offset, reason }) => ({
        topic,
        partition,
        offset,
        reason,
      }))
    ).toEqual([{ topic: TOPIC, partition: 0, offset: null, reason: 'no-offset' }])

    const again = manager.resolveOffsets()
    await flush()
    expect(answers).toHaveLength(1)
    await expect(again).resolves.toBe(true)
  })

  test('a stored -500 logs one warning naming the group, partition, stored value and policy for one resolution', async () => {
    const { manager, logger } = createMember('-500')

    await manager.resolveOffsets()
    await manager.resolveOffsets()

    expect(logger.warn).toHaveBeenCalledTimes(1)
    expect(logger.warn).toHaveBeenCalledWith(WARNING, {
      groupId: GROUP,
      topic: TOPIC,
      partition: 0,
      storedOffset: '-500',
      resetPolicy: 'earliest',
    })
  })

  test.each(['-1', '-2'])(
    'a stored %s goes through the reset policy as before, with no warning',
    async (offset) => {
      const { manager, lookup, logger } = createMember(offset)

      await manager.resolveOffsets()

      expect(lookup).toHaveBeenCalledTimes(1)
      expect(manager.heldOffsetAt(TOPIC, 0)).toBe('4')
      expect(logger.warn).not.toHaveBeenCalled()
    }
  )

  test.each(['0', '10'])('a stored %s is used as stored', async (offset) => {
    const { manager, cache, lookup } = createMember(offset)

    await manager.resolveOffsets()

    expect(lookup).not.toHaveBeenCalled()
    expect(cache[TOPIC]).toEqual({ 0: offset })
  })

  test.each(['-500', '-1', '-2'])(
    'consumer.committed reports a stored %s as an offset the group never committed',
    async (offset) => {
      const { reader } = createReader(offset)

      await expect(reader.read([{ topic: TOPIC, partition: 0 }])).resolves.toEqual([
        { topic: TOPIC, partition: 0, offset: null, metadata: null },
      ])
    }
  )
})
