const OffsetManager = require('../index')
const CommittedOffsetReader = require('../../committedOffsets')
const OffsetOperations = require('../offsetOperations')
const ResetObligations = require('../resetObligations')
const dictionary = require('../../../utils/dictionary')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { createGroupApiBroker } = require('../../../broker/__tests__/fixtures/groupApiHarness')
const apiKeys = require('../../../protocol/requests/apiKeys')
const offsetFetchV9 = require('../../../../testHelpers/wire/offsetFetchV9')

const TOPIC = 'test-topic'
const OTHER = 'other-topic'
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

describe('Consumer > offset reads whose answer does not cover the request', () => {
  const harnesses = []

  afterEach(async () => {
    await Promise.all(harnesses.splice(0).map(({ pool }) => pool.destroy()))
  })

  /**
   * A manager over `partitions` of `TOPIC` whose coordinator is a real broker over a fake socket,
   * answering every OffsetFetch with `answered` encoded at v9.
   * @param {{ topic: string, partitions: object[] }[]} answered the answer's topic entries
   * @param {number[]} [partitions=[0]] the assigned partitions
   * @returns {{ manager: OffsetManager, cache: object, lookup: jest.Mock, harness: object }}
   *   `lookup` is called for every ListOffsets lookup the manager starts
   */
  const createMember = (answered, partitions = [0]) => {
    const harness = createGroupApiBroker({
      versions: { [apiKeys.OffsetFetch]: { minVersion: 0, maxVersion: 9 } },
      respond: () =>
        offsetFetchV9.buildResponse({
          throttleTime: 0,
          groupId: GROUP,
          errorCode: 0,
          topics: answered,
        }),
    })
    harnesses.push(harness)
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
      memberAssignment: { [TOPIC]: partitions },
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

  test('an answer naming a topic the request did not carry rejects retriably and changes nothing', async () => {
    const { manager, cache, lookup } = createMember([{ topic: OTHER, partitions: [cell(0, '-1')] }])

    await expect(manager.resolveOffsets()).rejects.toMatchObject({
      name: 'KafkaJSError',
      retriable: true,
      message: expect.stringMatching(/^OffsetFetch response .*topic other-topic/),
    })

    expect(lookup).not.toHaveBeenCalled()
    expect(cache[OTHER]).toBeUndefined()
    expect(cache[TOPIC]).toBeUndefined()
  })

  test('an answer that repeats a partition with different offsets rejects and sets no position', async () => {
    const { manager, cache } = createMember([
      { topic: TOPIC, partitions: [cell(0, '10'), cell(0, '50')] },
    ])

    await expect(manager.resolveOffsets()).rejects.toMatchObject({
      name: 'KafkaJSError',
      retriable: true,
      message: expect.stringMatching(/partition 0 of topic test-topic twice/),
    })

    expect(cache[TOPIC]?.[0]).toBeUndefined()
    expect(manager.heldOffsetAt(TOPIC, 0)).toBeUndefined()
  })

  test('an answer that leaves out an assigned partition rejects and resolves none of them', async () => {
    const { manager, cache } = createMember([{ topic: TOPIC, partitions: [cell(0, '10')] }], [0, 1])

    await expect(manager.resolveOffsets()).rejects.toMatchObject({
      name: 'KafkaJSError',
      retriable: true,
      message: expect.stringMatching(/does not name partition 1 of topic test-topic/),
    })

    expect(cache[TOPIC]?.[0]).toBeUndefined()
    expect(cache[TOPIC]?.[1]).toBeUndefined()
  })

  test('an answer that splits the assigned topic into two entries rejects before any lookup, position or cache write', async () => {
    const { manager, cache, lookup } = createMember(
      [
        { topic: TOPIC, partitions: [cell(0, '-1')] },
        { topic: TOPIC, partitions: [cell(1, '-1')] },
      ],
      [0, 1]
    )

    await expect(manager.resolveOffsets()).rejects.toMatchObject({
      name: 'KafkaJSError',
      retriable: true,
      message: expect.stringMatching(/^OffsetFetch response .*topic test-topic/),
    })

    expect(lookup).not.toHaveBeenCalled()
    expect(cache[TOPIC]).toBeUndefined()
    expect(manager.heldOffsetAt(TOPIC, 0)).toBeUndefined()
    expect(manager.heldOffsetAt(TOPIC, 1)).toBeUndefined()
  })

  test('an answer that covers every assigned partition resolves them all', async () => {
    const { manager, cache } = createMember(
      [{ topic: TOPIC, partitions: [cell(1, '11'), cell(0, '10')] }],
      [0, 1]
    )

    await manager.resolveOffsets()

    expect(cache[TOPIC]).toEqual({ 0: '10', 1: '11' })
  })
})

describe('Consumer > committed offsets read when an answer does not cover the request', () => {
  const harnesses = []
  const ENTRIES = [
    { topic: TOPIC, partition: 0 },
    { topic: TOPIC, partition: 1 },
  ]

  afterEach(async () => {
    await Promise.all(harnesses.splice(0).map(({ pool }) => pool.destroy()))
  })

  /**
   * A reader whose coordinator is a real broker over a fake socket. Attempt n is answered with
   * `answers[n]` encoded at v9; the last answer repeats.
   * @param {{ topic: string, partitions: object[] }[][]} answers the topic entries of each answer
   * @returns {{ reader: CommittedOffsetReader, harness: object }}
   */
  const createReader = (answers) => {
    let sent = 0
    const harness = createGroupApiBroker({
      versions: { [apiKeys.OffsetFetch]: { minVersion: 0, maxVersion: 9 } },
      respond: () =>
        offsetFetchV9.buildResponse({
          throttleTime: 0,
          groupId: GROUP,
          errorCode: 0,
          topics: answers[Math.min(sent++, answers.length - 1)],
        }),
    })
    harnesses.push(harness)
    const cluster = { findGroupCoordinator: async () => harness.broker }
    const retry = { retries: 2, initialRetryTime: 1, maxRetryTime: 2 }
    return { reader: new CommittedOffsetReader({ cluster, groupId: GROUP, retry }), harness }
  }

  test('an answer that never covers the request is read again, then rejects with a non-retriable error caused by the coverage error', async () => {
    const { reader, harness } = createReader([[{ topic: TOPIC, partitions: [cell(0, '10')] }]])

    await expect(reader.read(ENTRIES)).rejects.toMatchObject({
      name: 'KafkaJSNumberOfRetriesExceeded',
      retriable: false,
      retryCount: 2,
      cause: expect.objectContaining({
        name: 'KafkaJSError',
        retriable: true,
        message: expect.stringMatching(/does not name partition 1 of topic test-topic/),
      }),
    })

    expect(harness.requests).toHaveLength(3)
  })

  test('an answer that covers the request on a later attempt resolves with its offsets', async () => {
    const { reader, harness } = createReader([
      [{ topic: TOPIC, partitions: [cell(0, '10')] }],
      [{ topic: TOPIC, partitions: [cell(1, '11'), cell(0, '10')] }],
    ])

    await expect(reader.read(ENTRIES)).resolves.toEqual([
      { topic: TOPIC, partition: 0, offset: '10', metadata: null },
      { topic: TOPIC, partition: 1, offset: '11', metadata: null },
    ])

    expect(harness.requests).toHaveLength(2)
  })
})
