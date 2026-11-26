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

const offsetFetches = ({ requests }) => requests.filter((r) => r.apiKey === apiKeys.OffsetFetch)

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
   * A manager over partition 0 of `TOPIC` whose coordinator stores `offset` and whose reset policy
   * is earliest.
   * @param {string} offset the stored offset
   * @returns {{ manager: OffsetManager, cache: object, lookup: jest.Mock, harness: object }}
   *   `lookup` is called for every ListOffsets lookup the manager starts
   */
  const createMember = (offset) => {
    const harness = coordinatorStoring(offset)
    const cache = dictionary()
    const operations = new OffsetOperations()
    const obligations = new ResetObligations()
    const lookup = jest.fn(async () => [
      { topic: TOPIC, partitions: [{ partition: 0, offset: '4' }] },
    ])
    const manager = new OffsetManager({
      cluster: {
        committedOffsets: () => cache,
        topicId: () => TOPIC_ID,
        fetchTopicsOffset: lookup,
      },
      coordinator: harness.broker,
      memberAssignment: { [TOPIC]: [0] },
      autoCommit: false,
      autoCommitInterval: null,
      autoCommitThreshold: null,
      topicConfigurations: { [TOPIC]: { resetPolicy: { kind: 'earliest' } } },
      instrumentationEmitter: new InstrumentationEventEmitter(),
      groupId: GROUP,
      generationId: 1,
      memberId: 'member-1',
      operations,
      obligations,
    })
    manager.getCoordinator = async () => harness.broker
    obligations.attachTo(operations)
    return { manager, cache, lookup, harness }
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

  test('a stored -500 rejects the offset resolution with a non-retriable error naming the partition, and no further OffsetFetch is sent', async () => {
    const { manager, cache, lookup, harness } = createMember('-500')

    await expect(manager.resolveOffsets()).rejects.toMatchObject({
      name: 'KafkaJSNonRetriableError',
      retriable: false,
      message: expect.stringMatching(
        /Group "test-group" stores offset -500 for partition 0 of topic test-topic.*admin\.setOffsets/
      ),
    })

    expect(offsetFetches(harness)).toHaveLength(1)
    expect(lookup).not.toHaveBeenCalled()
    expect(cache[TOPIC]?.[0]).toBeUndefined()
    expect(manager.heldOffsetAt(TOPIC, 0)).toBeUndefined()
  })

  test.each(['-1', '-2'])('a stored %s goes through the reset policy as before', async (offset) => {
    const { manager, lookup } = createMember(offset)

    await manager.resolveOffsets()

    expect(lookup).toHaveBeenCalledTimes(1)
    expect(manager.heldOffsetAt(TOPIC, 0)).toBe('4')
  })

  test.each(['0', '10'])('a stored %s is used as stored', async (offset) => {
    const { manager, cache, lookup } = createMember(offset)

    await manager.resolveOffsets()

    expect(lookup).not.toHaveBeenCalled()
    expect(cache[TOPIC]).toEqual({ 0: offset })
  })

  test('consumer.committed for a partition storing -500 rejects with the same error', async () => {
    const { reader, harness } = createReader('-500')

    await expect(reader.read([{ topic: TOPIC, partition: 0 }])).rejects.toMatchObject({
      name: 'KafkaJSNonRetriableError',
      retriable: false,
      message: expect.stringMatching(
        /Group "test-group" stores offset -500 for partition 0 of topic test-topic/
      ),
    })
    expect(offsetFetches(harness)).toHaveLength(1)
  })

  test.each(['-1', '-2'])(
    'consumer.committed reports a stored %s as an offset the group never committed',
    async (offset) => {
      const { reader } = createReader(offset)

      await expect(reader.read([{ topic: TOPIC, partition: 0 }])).resolves.toEqual([
        { topic: TOPIC, partition: 0, offset: null, metadata: null },
      ])
    }
  )
})
