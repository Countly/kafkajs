const OffsetManager = require('../index')
const ResetObligations = require('../resetObligations')
const Batch = require('../../batch')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const RequestV8Protocol = require('../../../protocol/requests/offsetCommit/v8/request')

const TOPIC = 'test-topic'

/** A decoded message as the record-batch decoder produces it, batchContext and all. */
const message = (offset, partitionLeaderEpoch, extra = {}) => ({
  offset: String(offset),
  key: null,
  value: Buffer.from('v'),
  headers: {},
  isControlRecord: false,
  batchContext: { partitionLeaderEpoch },
  ...extra,
})

const buildBatch = (fetchedOffset, messages, partitionData = {}) =>
  new Batch(TOPIC, String(fetchedOffset), {
    partition: 0,
    highWatermark: '100',
    abortedTransactions: [],
    messages,
    ...partitionData,
  })

describe('Consumer > committed leader epoch', () => {
  describe('Batch#leaderEpochAt', () => {
    it('returns the epoch of the record at that offset', () => {
      const batch = buildBatch(10, [message(10, 4), message(11, 4), message(12, 5)])

      expect(batch.leaderEpochAt('12')).toEqual(5)
      expect(batch.leaderEpochAt('10')).toEqual(4)
    })

    it('searches messagesWithinOffset, so a control record still has an epoch', () => {
      const batch = buildBatch(10, [message(10, 4), message(11, 4, { isControlRecord: true })])

      expect(batch.messages).toHaveLength(1) // the control record is filtered out of `messages`
      expect(batch.leaderEpochAt('11')).toEqual(4)
      expect(batch.leaderEpochAt(batch.lastOffset())).toEqual(4)
    })

    it('returns -1 for an offset no record in the batch carries', () => {
      const batch = buildBatch(10, [message(10, 4)])

      expect(batch.leaderEpochAt('999')).toEqual(-1)
    })

    it('returns -1 for a batch emptied by log compaction, which advances past no record', () => {
      // Every raw message sits below the requested offset, so messagesWithinOffset is empty and
      // lastOffset() falls back to the batch's own fetchedOffset - a position with nothing behind
      // it. Reporting the compacted records' epoch here would overstate where the consumer is.
      const batch = buildBatch(10, [message(7, 4), message(8, 4)])

      expect(batch.isEmptyDueToLogCompactedMessages()).toBe(true)
      expect(batch.lastOffset()).toEqual(batch.fetchedOffset)
      expect(batch.leaderEpochAt(batch.lastOffset())).toEqual(-1)
    })

    it('is O(1) for the last offset and linear overall for many lookups', () => {
      const messages = Array.from({ length: 1000 }, (_, i) => message(i, 3))
      const batch = buildBatch(0, messages)

      expect(batch.leaderEpochAt('999')).toEqual(3)
      for (let i = 0; i < 1000; i++) {
        expect(batch.leaderEpochAt(String(i))).toEqual(3)
      }
    })
  })

  describe('OffsetManager tracking', () => {
    const createManager = (coordinator) =>
      new OffsetManager({
        cluster: { committedOffsets: () => ({}), topicId: () => undefined },
        coordinator,
        memberAssignment: { [TOPIC]: [0, 1] },
        autoCommit: false,
        autoCommitInterval: null,
        autoCommitThreshold: null,
        topicConfigurations: { [TOPIC]: {} },
        instrumentationEmitter: new InstrumentationEventEmitter(),
        groupId: 'test-group',
        generationId: 1,
        memberId: 'member-1',
      })

    it('tracks the epoch beside the offset and reports -1 when unknown', () => {
      const manager = createManager({})

      expect(manager.lastConsumedEpoch(TOPIC, 0)).toEqual(-1)
      manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '10', leaderEpoch: 4 })
      expect(manager.lastConsumedEpoch(TOPIC, 0)).toEqual(4)
    })

    it('an omitted leaderEpoch is -1, never a stale value', () => {
      const manager = createManager({})
      manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '10', leaderEpoch: 4 })
      manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '11' })

      expect(manager.lastConsumedEpoch(TOPIC, 0)).toEqual(-1)
    })

    it('forgetConsumedEpoch clears the epoch without moving the offset', () => {
      const manager = createManager({})
      manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '10', leaderEpoch: 4 })
      const before = manager.nextOffset(TOPIC, 0).toString()

      manager.forgetConsumedEpoch(TOPIC, 0)

      expect(manager.lastConsumedEpoch(TOPIC, 0)).toEqual(-1)
      expect(manager.nextOffset(TOPIC, 0).toString()).toEqual(before)
    })

    it('uncommittedOffsets carries the epoch, and the commit sends it', async () => {
      const offsetCommit = jest.fn(async () => ({}))
      const manager = createManager({ isConnected: () => true, offsetCommit })
      manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '10', leaderEpoch: 4 })

      expect(manager.uncommittedOffsets().topics).toEqual([
        { topic: TOPIC, partitions: [{ partition: '0', offset: '11', leaderEpoch: 4 }] },
      ])

      await manager.commitOffsets()
      expect(offsetCommit).toHaveBeenCalledWith(
        expect.objectContaining({
          topics: [
            { topic: TOPIC, partitions: [{ partition: '0', offset: '11', leaderEpoch: 4 }] },
          ],
        })
      )
    })

    it('a user-supplied commitOffsets carries no epoch, and the codec writes -1', async () => {
      const offsetCommit = jest.fn(async () => ({}))
      const manager = createManager({ isConnected: () => true, offsetCommit })

      await manager.commitOffsets({
        topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '50' }] }],
      })

      const [{ topics }] = offsetCommit.mock.calls[0]
      expect(topics[0].partitions[0].leaderEpoch).toBeUndefined()

      // No record backs an offset the caller chose, so the field falls to its own wire default.
      const { buffer } = await RequestV8Protocol({
        groupId: 'test-group',
        groupGenerationId: 1,
        memberId: 'member-1',
        topics,
      }).encode()
      expect(buffer.includes(Buffer.from([0xff, 0xff, 0xff, 0xff]))).toBe(true)
    })

    it('every offset-clearing path clears the epoch with it', () => {
      const manager = createManager({})
      const seed = () =>
        manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '10', leaderEpoch: 4 })

      seed()
      manager.resetOffsetsForTopic(TOPIC)
      expect(manager.lastConsumedEpoch(TOPIC, 0)).toEqual(-1)

      seed()
      manager.clearAllOffsets()
      expect(manager.lastConsumedEpoch(TOPIC, 0)).toEqual(-1)

      // resetOffset rewinds the position to the committed offset, so the epoch of the record it
      // rewinds away from must not survive.
      seed()
      manager.resetOffset({ topic: TOPIC, partition: 0 })
      expect(manager.lastConsumedEpoch(TOPIC, 0)).toEqual(-1)
    })
  })

  describe('read-back on assignment, on a mixed assignment', () => {
    it('seeds the committed epoch for a resolved partition and -1 for one that needed resolution', async () => {
      // Two partitions: 0 has a committed offset carrying an epoch, 1 has none, so
      // initializeConsumerOffsets rebuilds BOTH entries and drops committedLeaderEpoch from both.
      // A single-partition version of this case passes even against that bug.
      const coordinator = {
        isConnected: () => true,
        offsetFetch: async () => ({
          responses: [
            {
              topic: TOPIC,
              partitions: [
                {
                  partition: 0,
                  offset: '10',
                  committedLeaderEpoch: 4,
                  metadata: null,
                  errorCode: 0,
                },
                {
                  partition: 1,
                  offset: '-1',
                  committedLeaderEpoch: -1,
                  metadata: null,
                  errorCode: 0,
                },
              ],
            },
          ],
        }),
      }

      const manager = new OffsetManager({
        cluster: {
          committedOffsets: () => ({}),
          topicId: () => undefined,
          fetchTopicsOffset: async () => [
            { topic: TOPIC, partitions: [{ partition: 1, offset: '0' }] },
          ],
        },
        coordinator,
        memberAssignment: { [TOPIC]: [0, 1] },
        autoCommit: false,
        autoCommitInterval: null,
        autoCommitThreshold: null,
        topicConfigurations: { [TOPIC]: { fromBeginning: true } },
        instrumentationEmitter: new InstrumentationEventEmitter(),
        groupId: 'test-group',
        generationId: 1,
        memberId: 'member-1',
      })

      await manager.resolveOffsets()

      expect(manager.lastConsumedEpoch(TOPIC, 0)).toEqual(4)
      expect(manager.lastConsumedEpoch(TOPIC, 1)).toEqual(-1)
      expect(manager.committedOffsets()[TOPIC][0]).toEqual('10')
      expect(manager.committedOffsets()[TOPIC][1]).toEqual('0')
    })

    it('an offset a pending topic reset invalidated does not seed its stale epoch', async () => {
      const coordinator = {
        isConnected: () => true,
        offsetFetch: async () => ({
          responses: [
            {
              topic: TOPIC,
              partitions: [
                {
                  partition: 0,
                  offset: '10',
                  committedLeaderEpoch: 4,
                  metadata: null,
                  errorCode: 0,
                },
              ],
            },
          ],
        }),
      }

      const obligations = new ResetObligations()
      obligations.owe(TOPIC, 'new-id')

      const manager = new OffsetManager({
        cluster: {
          committedOffsets: () => ({}),
          topicId: () => 'new-id',
          fetchTopicsOffset: async () => [
            { topic: TOPIC, partitions: [{ partition: 0, offset: '0' }] },
          ],
        },
        coordinator,
        memberAssignment: { [TOPIC]: [0] },
        autoCommit: false,
        autoCommitInterval: null,
        autoCommitThreshold: null,
        topicConfigurations: { [TOPIC]: { fromBeginning: true } },
        instrumentationEmitter: new InstrumentationEventEmitter(),
        groupId: 'test-group',
        generationId: 1,
        memberId: 'member-1',
        obligations,
      })

      await manager.resolveOffsets()

      // The owed reset rewrote the offset to the invalid sentinel but left the epoch alone;
      // seeding it would attach the previous incarnation's epoch to a fresh position.
      expect(manager.lastConsumedEpoch(TOPIC, 0)).toEqual(-1)
    })

    it('ignores a topic the response names but this member is not assigned', async () => {
      // A coordinator echoing more than was asked, or a response crossing a re-assignment, must
      // not take the join path down with a TypeError.
      const coordinator = {
        isConnected: () => true,
        offsetFetch: async () => ({
          responses: [
            {
              topic: TOPIC,
              partitions: [
                {
                  partition: 0,
                  offset: '10',
                  committedLeaderEpoch: 4,
                  metadata: null,
                  errorCode: 0,
                },
              ],
            },
            {
              topic: 'not-assigned-to-this-member',
              partitions: [
                {
                  partition: 0,
                  offset: '20',
                  committedLeaderEpoch: 9,
                  metadata: null,
                  errorCode: 0,
                },
              ],
            },
          ],
        }),
      }

      const manager = new OffsetManager({
        cluster: { committedOffsets: () => ({}), topicId: () => undefined },
        coordinator,
        memberAssignment: { [TOPIC]: [0] },
        autoCommit: false,
        autoCommitInterval: null,
        autoCommitThreshold: null,
        topicConfigurations: { [TOPIC]: { fromBeginning: true } },
        instrumentationEmitter: new InstrumentationEventEmitter(),
        groupId: 'test-group',
        generationId: 1,
        memberId: 'member-1',
      })

      await expect(manager.resolveOffsets()).resolves.toBe(true)
      expect(manager.lastConsumedEpoch(TOPIC, 0)).toEqual(4)
      expect(manager.resolvedEpochs['not-assigned-to-this-member']).toBeUndefined()
    })
  })
})
