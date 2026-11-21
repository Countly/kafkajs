const Long = require('../../../utils/long')
const dictionary = require('../../../utils/dictionary')
const isInvalidOffset = require('../isInvalidOffset')
const OffsetManager = require('../index')
const OffsetOperations = require('../offsetOperations')

/**
 * The full walk `countResolvedOffsets` ran on every call before it kept its sum, read through the
 * manager's own fields: the oracle the kept sum must equal after every change.
 * @param {OffsetManager} manager
 * @returns {Long}
 */
const walkCount = (manager) => {
  let sum = Long.fromValue(0)
  for (const topic of manager.topics) {
    for (const partition of Object.keys(manager.resolvedOffsets[topic])) {
      const resolved = manager.resolvedOffsets[topic][partition]
      if (resolved == null) continue
      const committed = manager.heldOffsetAt(topic, partition)
      const value = Long.fromValue(resolved)
      sum = sum.add(isInvalidOffset(committed) ? value : value.subtract(Long.fromValue(committed)))
    }
  }
  return sum
}

/** A seeded generator, so a failing run can be replayed from its seed. */
const random = (seed) => () => {
  seed = (seed * 1103515245 + 12345) % 2147483648
  return seed / 2147483648
}

/**
 * A member of one group sharing the group's cache and ledger, with every commit recorded through
 * the ledger as the coordinator's answer would be.
 * @param {{cache: object, operations: OffsetOperations}} shared
 * @param {{[topic: string]: number[]}} memberAssignment
 * @param {number|null} [autoCommitThreshold]
 */
const createManager = ({ cache, operations }, memberAssignment, autoCommitThreshold = null) => {
  const manager = new OffsetManager({
    cluster: { committedOffsets: () => cache },
    coordinator: {},
    memberAssignment,
    operations,
    autoCommit: true,
    autoCommitInterval: null,
    autoCommitThreshold,
    topicConfigurations: {},
    instrumentationEmitter: { hasListeners: () => false, emit() {} },
    groupId: 'group',
    generationId: 1,
    memberId: 'member',
  })
  manager.commitOffsets = jest.fn(async () => {
    for (const { topic, partitions } of manager.uncommittedOffsets().topics) {
      for (const { partition, offset } of partitions) {
        operations.publish(null, topic, partition, offset)
      }
    }
  })
  return manager
}

const assignmentOf = (topics, partitions) => {
  const assignment = {}
  for (let t = 0; t < topics; t++) assignment[`topic-${t}`] = [...Array(partitions).keys()]
  return assignment
}

/**
 * Drives one member through a generated run - messages, seeks, offset resets, sibling commits,
 * a cooperative revocation and a full rebalance - checking the threshold after every message the
 * way `eachMessage` does, and the kept count against the walk after every step.
 * @param {number} seed
 * @returns {{fired: number[], expected: number[], checks: number}}
 */
const generatedRun = async (seed) => {
  const next = random(seed)
  const pick = (list) => list[Math.floor(next() * list.length)]
  const shared = { cache: dictionary(), operations: new OffsetOperations() }
  let assignment = assignmentOf(3, 4)
  let manager = createManager(shared, assignment, 7)
  const positions = new Map()
  const cell = () => {
    const topic = pick(Object.keys(assignment))
    return { topic, partition: pick(assignment[topic].length ? assignment[topic] : [0]) }
  }
  const fired = []
  const expected = []
  let checks = 0

  for (let step = 0; step < 1500; step++) {
    const { topic, partition } = cell()
    const key = `${topic}|${partition}`
    const roll = next()
    if (roll < 0.6) {
      const offset = (positions.get(key) ?? 0) + 1 + (next() < 0.1 ? Math.floor(next() * 50) : 0)
      positions.set(key, offset)
      manager.resolveOffset({ topic, partition, offset: String(offset) })
      const due = walkCount(manager).gte(Long.fromValue(manager.autoCommitThreshold))
      if (due) expected.push(step)
      const calls = manager.commitOffsets.mock.calls.length
      await manager.commitOffsetsIfNecessary()
      if (manager.commitOffsets.mock.calls.length > calls) fired.push(step)
    } else if (roll < 0.65) {
      manager.nextOffset(topic, partition)
    } else if (roll < 0.7) {
      const offset = Math.floor(next() * 40)
      positions.set(key, offset - 1)
      manager.forgetResolved(topic, partition)
      manager.installSeekPosition(topic, partition, String(offset), { local: next() < 0.5 })
    } else if (roll < 0.73) {
      manager.resetOffset({ topic, partition })
    } else if (roll < 0.75) {
      manager.holdResetPosition(topic, partition, String(Math.floor(next() * 30)))
    } else if (roll < 0.77) {
      manager.resetOffsetsForTopic(topic)
    } else if (roll < 0.87) {
      shared.operations.publish(null, topic, partition, String(Math.floor(next() * 60)))
    } else if (roll < 0.9) {
      shared.operations.forget(topic, partition)
    } else if (roll < 0.91) {
      shared.operations.publish(null, topic, partition, '-1')
    } else if (roll < 0.92) {
      shared.operations.invalidateTopic(topic)
    } else if (roll < 0.94) {
      manager.resolveOffset({ topic: 'topic-0', partition: 99, offset: '5' })
    } else if (roll < 0.97) {
      const revokedTopic = pick(Object.keys(assignment))
      const revoked = assignment[revokedTopic].slice(0, 1)
      assignment = { ...assignment, [revokedTopic]: assignment[revokedTopic].slice(1) }
      if (next() < 0.5) assignment[`topic-${3 + step}`] = [0, 1]
      manager.updateAssignment({
        revoked: [{ topic: revokedTopic, partitions: revoked }],
        memberAssignment: assignment,
        generationId: 2,
        memberId: 'member',
        coordinator: {},
      })
    } else {
      assignment = assignmentOf(3, 4)
      manager = createManager(shared, assignment, manager.autoCommitThreshold)
    }
    checks++
    expect(manager.countResolvedOffsets().toString()).toEqual(walkCount(manager).toString())
  }
  return { fired, expected, checks }
}

describe('Consumer > OffsetManager > resolved offset count', () => {
  it.each([[1], [7], [42], [2026], [65537]])(
    'fires the threshold commit at exactly the messages the full walk would, run seeded %i',
    async (seed) => {
      const { fired, expected, checks } = await generatedRun(seed)
      expect(checks).toBe(1500)
      expect(expected.length).toBeGreaterThan(5)
      expect(fired).toEqual(expected)
    }
  )

  describe('follows every change to what it counts', () => {
    let shared, manager

    beforeEach(() => {
      shared = { cache: dictionary(), operations: new OffsetOperations() }
      manager = createManager(shared, { t: [0, 1, 2] }, 100)
      shared.operations.fill('t', 0, '10')
      shared.operations.fill('t', 1, '-1')
      manager.resolveOffset({ topic: 't', partition: 0, offset: '14' })
      manager.resolveOffset({ topic: 't', partition: 1, offset: '2' })
      expect(manager.countResolvedOffsets().toString()).toBe('8')
    })

    const expectWalk = (value) => {
      expect(manager.countResolvedOffsets().toString()).toBe(value)
      expect(walkCount(manager).toString()).toBe(value)
    }

    it('a resolved message adds its distance, also across a gap', () => {
      manager.resolveOffset({ topic: 't', partition: 0, offset: '40' })
      expectWalk('34')
    })

    it('a resolved offset for a partition this member does not own changes nothing', () => {
      manager.resolveOffset({ topic: 't', partition: 7, offset: '40' })
      expectWalk('8')
    })

    it('a commit published to the shared cache by any participant moves the baseline', () => {
      shared.operations.publish(null, 't', 0, '15')
      expectWalk('3')
    })

    it('a cell forgotten from the shared cache counts from zero', () => {
      shared.operations.forget('t', 0)
      expectWalk('18')
    })

    it('an offset reset of the topic counts nothing more', () => {
      manager.resetOffsetsForTopic('t')
      expectWalk('0')
    })

    it('a seek counts from its private fallback while nothing is committed', () => {
      manager.forgetResolved('t', 1)
      manager.installSeekPosition('t', 1, '9')
      expectWalk('5')
    })

    it('a forgotten position counts nothing', () => {
      manager.forgetResolved('t', 0)
      expectWalk('3')
    })

    it('a rewound position counts nothing', () => {
      manager.resetOffset({ topic: 't', partition: 0 })
      expectWalk('3')
    })

    it('a position seeded from the committed offset counts as it is held', () => {
      manager.nextOffset('t', 2, '-1')
      expectWalk('7')
    })

    it('a reset position held by this member becomes the baseline', () => {
      manager.holdResetPosition('t', 1, '1')
      expectWalk('7')
    })

    it('a revoked partition stops counting', () => {
      manager.updateAssignment({
        revoked: [{ topic: 't', partitions: [0] }],
        memberAssignment: { t: [1, 2] },
        generationId: 2,
        memberId: 'member',
        coordinator: {},
      })
      expectWalk('3')
    })

    it('a commit through the threshold path is followed by a fresh count', async () => {
      manager.autoCommitThreshold = 8
      manager.commitOffsets = jest.fn(async () => {
        shared.cache.t[0] = '15'
        shared.cache.t[1] = '3'
      })
      await manager.commitOffsetsIfNecessary()
      expect(manager.commitOffsets).toHaveBeenCalledTimes(1)
      expectWalk('0')
    })
  })

  it('checks the threshold per message at a cost that does not grow with the assignment', async () => {
    const perMessage = async (partitions) => {
      const shared = { cache: dictionary(), operations: new OffsetOperations() }
      const manager = createManager(shared, { t: [...Array(partitions).keys()] }, 1e9)
      for (let p = 0; p < partitions; p++) {
        shared.operations.fill('t', p, '100')
        manager.resolveOffset({ topic: 't', partition: p, offset: '1000' })
      }
      await manager.commitOffsetsIfNecessary()
      const held = jest.spyOn(manager, 'heldOffsetAt')
      for (let offset = 1001; offset <= 1100; offset++) {
        manager.resolveOffset({ topic: 't', partition: 0, offset: String(offset) })
        await manager.commitOffsetsIfNecessary()
      }
      expect(manager.countResolvedOffsets().toString()).toBe(String(901 * partitions + 100))
      return held.mock.calls.length / 100
    }

    expect(await perMessage(10)).toBeLessThanOrEqual(1)
    expect(await perMessage(1000)).toBeLessThanOrEqual(1)
  })
})
