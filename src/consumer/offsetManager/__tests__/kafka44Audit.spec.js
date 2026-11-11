const OffsetManager = require('../index')
const OffsetOperations = require('../offsetOperations')
const CommittedOffsetRevisions = require('../../../cluster/committedOffsetRevisions')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { KafkaJSConnectionError, KafkaJSProtocolError } = require('../../../errors')
const { createErrorFromCode } = require('../../../protocol/error')
const { newLogger } = require('testHelpers')

const TOPIC = 'topic-a'
const STALE_MEMBER_EPOCH = 113

/**
 * One committed-offset answer for partition 0.
 * @param {string} offset the committed offset, `'-1'` for none
 * @returns {{ responses: object[] }}
 */
const committed = (offset) => ({
  responses: [
    {
      topic: TOPIC,
      partitions: [
        { partition: 0, offset, committedLeaderEpoch: -1, metadata: null, errorCode: 0 },
      ],
    },
  ],
})

/**
 * A real `OffsetManager` for one partition, over scripted OffsetFetch and ListOffsets answers and
 * an operation ledger the test can inspect.
 * @param {object} scripts
 * @param {Function} scripts.offsetFetch the coordinator's OffsetFetch
 * @param {Function} [scripts.fetchTopicsOffset] the cluster's ListOffsets
 * @param {'classic'|'consumer'} [scripts.membershipProtocol]
 * @returns {{ manager: OffsetManager, operations: OffsetOperations, publications: () => number }}
 */
const build = ({ offsetFetch, fetchTopicsOffset = jest.fn(), membershipProtocol = 'classic' }) => {
  const cache = { [TOPIC]: {} }
  const coordinator = { isConnected: () => true, offsetFetch, offsetCommit: jest.fn() }
  const operations = new OffsetOperations()
  const manager = new OffsetManager({
    cluster: {
      committedOffsets: () => cache,
      topicId: () => undefined,
      fetchTopicsOffset,
      findGroupCoordinator: jest.fn(async () => coordinator),
      refreshMetadata: jest.fn(async () => {}),
    },
    coordinator,
    operations,
    memberAssignment: { [TOPIC]: [0] },
    autoCommit: false,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    topicConfigurations: { [TOPIC]: { fromBeginning: true } },
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: 'audit-group',
    generationId: 3,
    memberId: 'member-a',
    membershipProtocol,
    staleEpochTimeout: 5000,
    logger: newLogger(),
  })
  const revisions = CommittedOffsetRevisions.forCache(cache)
  const start = revisions.seq
  return { manager, operations, publications: () => revisions.seq - start, cache, coordinator }
}

describe('Kafka 4.4 client fix audit', () => {
  test('KAFKA-20187 failed ListOffsets releases the lookup for retry', async () => {
    const fetchTopicsOffset = jest
      .fn()
      .mockRejectedValueOnce(new KafkaJSConnectionError('broker unavailable'))
      .mockResolvedValueOnce([{ topic: TOPIC, partitions: [{ partition: 0, offset: '7' }] }])
    const { manager, operations, publications } = build({
      offsetFetch: jest.fn(async () => committed('-1')),
      fetchTopicsOffset,
    })

    const failed = await manager.resolveOffsets().then(
      () => null,
      (error) => error
    )

    expect(failed).toBeInstanceOf(KafkaJSConnectionError)
    expect(operations.openTickets).toBe(0)
    expect(operations.cells).toBe(0)
    expect(manager.heldOffsetAt(TOPIC, 0)).toBeUndefined()
    expect(publications()).toBe(0)

    await expect(manager.resolveOffsets()).resolves.toBe(true)

    expect(fetchTopicsOffset).toHaveBeenCalledTimes(2)
    expect(manager.heldOffsetAt(TOPIC, 0)).toBe('7')
    expect(operations.openTickets).toBe(0)
    expect(operations.cells).toBe(0)
  })

  test('KAFKA-20765 stale member epoch OffsetFetch retries settle the original call', async () => {
    jest.useFakeTimers()
    try {
      let epoch = 3
      const sent = []
      const offsetFetch = jest.fn(async ({ memberEpoch }) => {
        sent.push(memberEpoch)
        if (memberEpoch === 3) {
          throw new KafkaJSProtocolError(createErrorFromCode(STALE_MEMBER_EPOCH))
        }
        return committed('12')
      })
      const { manager, operations, publications, cache, coordinator } = build({
        offsetFetch,
        membershipProtocol: 'consumer',
      })
      manager.membership = () => ({ generationId: epoch, memberId: 'member-a', coordinator })

      const settled = jest.fn()
      const resolving = manager.resolveOffsets().then(settled)
      await jest.advanceTimersByTimeAsync(1)
      expect(sent).toEqual([3])
      expect(settled).not.toHaveBeenCalled()
      expect(operations.openTickets).toBe(1)

      epoch = 4
      await jest.advanceTimersByTimeAsync(1000)
      await resolving

      expect(sent).toEqual([3, 4])
      expect(settled).toHaveBeenCalledWith(true)
      expect(publications()).toBe(1)
      expect(cache[TOPIC]).toEqual({ 0: '12' })
      expect(operations.openTickets).toBe(0)
      expect(operations.cells).toBe(0)
      expect(jest.getTimerCount()).toBe(0)
    } finally {
      jest.useRealTimers()
    }
  })
})
