const OffsetManager = require('../index')
const OffsetOperations = require('../offsetOperations')
const ResetObligations = require('../resetObligations')
const dictionary = require('../../../utils/dictionary')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { createGroupApiBroker } = require('../../../broker/__tests__/fixtures/groupApiHarness')
const apiKeys = require('../../../protocol/requests/apiKeys')
const offsetCommitV9 = require('../../../../testHelpers/wire/offsetCommitV9')

const TOPIC = 'test-topic'
const TOPIC_ID = 'id-of-the-topic'
const DURATION = { kind: 'by_duration', durationMs: 1000 }

/** @returns {{ partition: number, errorCode: number }[]} one answer row per `partition:code` pair */
const rows = (...pairs) => pairs.map(([partition, errorCode]) => ({ partition, errorCode }))

/**
 * @param {...{ partition: number, offset: string }} cells
 * @returns {object} the offsets a caller passes to `commitOffsets`
 */
const offsetsOf = (...cells) => ({ topics: [{ topic: TOPIC, partitions: cells }] })

describe('Consumer > offset commits whose answer does not cover the request', () => {
  const harnesses = []

  afterEach(async () => {
    await Promise.all(harnesses.splice(0).map(({ pool }) => pool.destroy()))
  })

  /**
   * A manager over partitions 0 and 1 of `TOPIC` whose coordinator is a real broker over a fake
   * socket, answering every OffsetCommit with one topic entry holding `answered` (none when
   * `answered` is empty). The topic owes a reset, as after a recreation, so that a commit the
   * broker confirms settles it.
   * @param {{ partition: number, errorCode: number }[]} answered
   * @returns {{ manager: OffsetManager, cache: object, obligations: ResetObligations,
   *   committed: jest.Mock }} `committed` is called for every `COMMIT_OFFSETS` event
   */
  const createMember = (answered) => {
    const harness = createGroupApiBroker({
      versions: { [apiKeys.OffsetCommit]: { minVersion: 0, maxVersion: 9 } },
      respond: () =>
        offsetCommitV9.buildResponse({
          throttleTime: 0,
          topics: answered.length === 0 ? [] : [{ topic: TOPIC, partitions: answered }],
        }),
    })
    harnesses.push(harness)
    const cache = dictionary()
    const operations = new OffsetOperations()
    const obligations = new ResetObligations()
    const emitter = new InstrumentationEventEmitter()
    const committed = jest.fn()
    emitter.addListener('consumer.commit_offsets', committed)
    const manager = new OffsetManager({
      cluster: {
        committedOffsets: () => cache,
        topicId: () => TOPIC_ID,
        fetchTopicsOffset: jest.fn(async () => [
          { topic: TOPIC, partitions: [{ partition: 0, offset: '4' }] },
        ]),
      },
      coordinator: harness.broker,
      memberAssignment: { [TOPIC]: [0, 1] },
      autoCommit: true,
      autoCommitInterval: null,
      autoCommitThreshold: null,
      topicConfigurations: { [TOPIC]: { resetPolicy: DURATION } },
      instrumentationEmitter: emitter,
      groupId: 'test-group',
      generationId: 1,
      memberId: 'member-1',
      operations,
      obligations,
    })
    manager.getCoordinator = async () => harness.broker
    obligations.attachTo(operations)
    obligations.owe(TOPIC, TOPIC_ID)
    return { manager, cache, obligations, committed }
  }

  /** A guard that never refuses: as with the consumer's own commits, the reply is observed. */
  const guard = () => null

  test('a commit of p0 and p1 whose answer omits p1 rejects, emits nothing and records nothing', async () => {
    const { manager, cache, committed } = createMember(rows([0, 0]))

    await expect(
      manager.commitOffsets(
        offsetsOf({ partition: 0, offset: '5' }, { partition: 1, offset: '6' }),
        { guard }
      )
    ).rejects.toMatchObject({
      retriable: true,
      message: expect.stringMatching(/does not name partition 1 of topic test-topic/),
    })

    expect(committed).not.toHaveBeenCalled()
    expect(cache[TOPIC]?.[0]).toBeUndefined()
    expect(cache[TOPIC]?.[1]).toBeUndefined()
  })

  test('a commit naming p0 twice, answered p0 accepted then p0 refused, rejects with the refusal and the cache does not hold the later offset', async () => {
    const { manager, cache, committed } = createMember(rows([0, 0], [0, 12]))

    await expect(
      manager.commitOffsets(
        offsetsOf({ partition: 0, offset: '5' }, { partition: 0, offset: '7' }),
        {
          guard,
        }
      )
    ).rejects.toMatchObject({ type: 'OFFSET_METADATA_TOO_LARGE' })

    expect(committed).not.toHaveBeenCalled()
    expect(cache[TOPIC]?.[0]).toBeUndefined()
  })

  test('a commit naming p0 twice and answered twice with success resolves and holds the later offset', async () => {
    const { manager, cache, committed } = createMember(rows([0, 0], [0, 0]))

    await manager.commitOffsets(
      offsetsOf({ partition: 0, offset: '5' }, { partition: 0, offset: '7' }),
      { guard }
    )

    expect(committed).toHaveBeenCalledTimes(1)
    expect(cache[TOPIC][0]).toBe('7')
  })

  test('a reset by duration whose commit answer omits its partition rejects and leaves the reset owed', async () => {
    const { manager, cache, obligations, committed } = createMember([])

    await expect(manager.resetByDuration({ topic: TOPIC, partition: 0 })).rejects.toMatchObject({
      retriable: true,
      message: expect.stringMatching(/does not name partition 0 of topic test-topic/),
    })

    expect(obligations.isOwed(TOPIC, 0)).toBe(true)
    expect(committed).not.toHaveBeenCalled()
    expect(cache[TOPIC]?.[0]).toBeUndefined()
  })

  test('a reset by duration whose commit answer names its partition settles the reset', async () => {
    const { manager, obligations, committed } = createMember(rows([0, 0]))

    await expect(manager.resetByDuration({ topic: TOPIC, partition: 0 })).resolves.toBe(true)

    expect(obligations.isOwed(TOPIC, 0)).toBe(false)
    expect(committed).toHaveBeenCalledTimes(1)
  })
})
