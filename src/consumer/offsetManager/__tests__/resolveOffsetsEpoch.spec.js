const OffsetManager = require('../index')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')

const TOPIC = 'test-topic'

/** Lets every pending OffsetFetch reach the coordinator stub before a case answers one. */
const flush = () => new Promise(setImmediate)

/**
 * An OffsetFetch answer for partition 0, in the parsed shape `Broker#offsetFetch` resolves with.
 * @param {string} offset the committed offset
 * @param {number} committedLeaderEpoch -1 when the commit carried no epoch, as an offset committed
 *   through TxnOffsetCommit v0/v1 does
 * @returns {object}
 */
const reply = (offset, committedLeaderEpoch) => ({
  responses: [
    {
      topic: TOPIC,
      partitions: [{ partition: 0, offset, committedLeaderEpoch, metadata: null, errorCode: 0 }],
    },
  ],
})

/**
 * A manager over one partition whose coordinator answers nothing until a case resolves the pending
 * OffsetFetch itself, which is how several fetchers' requests come to be in flight at once.
 * @returns {{ pending: Function[], manager: OffsetManager }}
 */
const setup = () => {
  const pending = []
  const coordinator = {
    isConnected: () => true,
    offsetFetch: () => new Promise((resolve) => pending.push(resolve)),
  }

  const manager = new OffsetManager({
    cluster: { committedOffsets: () => ({}), topicId: () => undefined },
    coordinator,
    memberAssignment: { [TOPIC]: [0] },
    autoCommit: false,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    topicConfigurations: { [TOPIC]: {} },
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: 'test-group',
    generationId: 1,
    memberId: 'member-1',
  })

  return { pending, manager }
}

describe('Consumer > OffsetManager > resolveOffsets > committed-epoch seeding', () => {
  it('does not let a late OffsetFetch reply overwrite the epoch of a consumed record', async () => {
    // One fetcher per broker resolves the whole assignment, so after a rejoin two or three
    // OffsetFetch requests for the same partition are in flight together.
    const { pending, manager } = setup()
    const leaderRound = manager.resolveOffsets()
    const otherRound = manager.resolveOffsets()
    await flush()
    expect(pending).toHaveLength(2)

    pending[0](reply('98', -1))
    await leaderRound
    expect(manager.nextOffset(TOPIC, 0).toString()).toEqual('98')
    manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '99', leaderEpoch: 0 })
    const snapshot = manager.uncommittedOffsets()

    pending[1](reply('98', -1))
    await otherRound

    expect(manager.uncommittedOffsets()).toEqual(snapshot)
    expect(manager.lastConsumedEpoch(TOPIC, 0)).toEqual(0)
  })

  it('keeps the -1 of a seek applied before the first resolution', async () => {
    const { pending, manager } = setup()
    await manager.seek({ topic: TOPIC, partition: 0, offset: '500' })
    const round = manager.resolveOffsets()
    await flush()

    pending[0](reply('100', 4))
    await round

    expect(manager.uncommittedOffsets().topics[0].partitions[0]).toEqual({
      partition: '0',
      offset: '500',
      leaderEpoch: -1,
    })
  })

  it('still seeds the committed epoch while the position is the committed offset', async () => {
    // A position equal to the committed offset has not moved since the commit, so the committed
    // epoch describes it: the same seed a freshly assigned partition takes.
    const { pending, manager } = setup()
    await manager.seek({ topic: TOPIC, partition: 0, offset: '100' })
    const round = manager.resolveOffsets()
    await flush()

    pending[0](reply('100', 4))
    await round

    expect(manager.lastConsumedEpoch(TOPIC, 0)).toEqual(4)
  })

  it('never degrades a known epoch to unknown, even at the resolved position', async () => {
    // A commit at the offset just consumed that carried no epoch (-1): the committed offset equals
    // the position, and must not erase the epoch of the record that moved it there.
    const { pending, manager } = setup()
    const leaderRound = manager.resolveOffsets()
    const otherRound = manager.resolveOffsets()
    await flush()

    pending[0](reply('98', -1))
    await leaderRound
    manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '99', leaderEpoch: 0 })

    pending[1](reply('100', -1))
    await otherRound

    expect(manager.resolvedOffsetAt(TOPIC, 0)).toEqual('100')
    expect(manager.lastConsumedEpoch(TOPIC, 0)).toEqual(0)
  })

  it.each([
    ['a higher epoch', 2, 3],
    ['epoch 0, which is a real epoch', 1, 0],
  ])(
    'takes a committed epoch of 0 or more at the resolved position: %s',
    async (_, knownEpoch, committedEpoch) => {
      // Only -1 is refused: any other committed epoch describes the committed offset the position
      // still equals, whether it is above or below the epoch the consumed record carried.
      const { pending, manager } = setup()
      const leaderRound = manager.resolveOffsets()
      const otherRound = manager.resolveOffsets()
      await flush()

      pending[0](reply('98', -1))
      await leaderRound
      manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '99', leaderEpoch: knownEpoch })
      expect(manager.lastConsumedEpoch(TOPIC, 0)).toEqual(knownEpoch)

      pending[1](reply('100', committedEpoch))
      await otherRound

      expect(manager.resolvedOffsetAt(TOPIC, 0)).toEqual('100')
      expect(manager.lastConsumedEpoch(TOPIC, 0)).toEqual(committedEpoch)
    }
  )
})
