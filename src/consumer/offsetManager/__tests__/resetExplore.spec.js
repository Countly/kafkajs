const { interleavings, mulberry32 } = require('../../../../testHelpers/schedule')
const {
  TOPIC,
  NEW_ID,
  offsetFetchAnswer,
  listOffsetsAnswer,
  createScheduledOffsetManager,
} = require('./fixtures/scheduledOffsetManager')
const recorded = require('./fixtures/schedules.json')

const RESOLVER_ANSWER = '1000'
const EPOCH_OFFSET = '55'
const MUTATIONS = new Set(['commit', 'seekUp', 'seekDown', 'publish'])

/**
 * The shapes named by the design, each a list of operations that all begin before any completes.
 * Every operation has one completion release; the orderings are the permutations of those
 * releases. A recreation and a revoke are instantaneous: their release is the moment they happen.
 */
const NAMED_SHAPES = [
  ['resolver and a commit', ['resolver', 'commit']],
  ['resolver and a forward seek', ['resolver', 'seekUp']],
  ['resolver and a backward seek', ['resolver', 'seekDown']],
  ['two resolvers and a seek', ['resolver', 'resolver', 'seekUp']],
  ['seek and seek', ['seekUp', 'seekUp']],
  ['seek and a recreation', ['seekUp', 'recreate']],
  ['seek and a revoke', ['seekUp', 'revoke']],
  ['resolver, a recreation and a revoke', ['resolver', 'recreate', 'revoke']],
  ['a throwing ListOffsets, a commit and a rejoin', ['failingResolver', 'commit', 'revoke']],
  ['a resolver with a delayed epoch seed and a commit', ['epochResolver', 'commit']],
  ['recreation, recreation and a resolver', ['recreate', 'recreate', 'resolver']],
  ['a transaction publication, a seek and a recreation', ['publish', 'seekUp', 'recreate']],
  ['commit and commit', ['commit', 'commit']],
  ['a read begun during an unresolved seek', ['seekUp', 'resolver']],
  ['five operations', ['resolver', 'resolver', 'commit', 'seekDown', 'failingResolver']],
  ['six operations', ['resolver', 'seekUp', 'seekDown', 'commit', 'recreate', 'revoke']],
]

const RANDOM_KINDS = [
  'resolver',
  'failingResolver',
  'epochResolver',
  'seekUp',
  'seekDown',
  'commit',
  'publish',
  'recreate',
  'revoke',
]
const RANDOM_SHAPES = 500
const RANDOM_RUNS = 151968
const RANDOM_SEED = 20261016

/** The explored set is part of the contract: a change to it must be reviewed. */
const DECLARED_RUNS = 153744

const factorial = (n) => (n <= 1 ? 1 : n * factorial(n - 1))
const PLANNED_RUNS =
  NAMED_SHAPES.reduce((sum, [, shape]) => sum + 2 * factorial(shape.length), 0) + RANDOM_RUNS

const valueOf = (kind, index) =>
  ({
    commit: String(50 + 10 * index),
    seekUp: String(700 + index),
    seekDown: String(5 + index),
    publish: '70',
  })[kind]

const commitOf = (partition, offset) => ({
  topics: [{ topic: TOPIC, partitions: [{ partition, offset }] }],
})

/**
 * Begins one operation and returns the function that completes it.
 * @returns {Promise<() => Promise<void>>}
 */
const start = async (ctx, kind, { index, partition, partitions }) => {
  const value = valueOf(kind, index)
  const hold = async (api, begin, release, expectedError) => {
    const gate = ctx.schedule.hold({ api })
    const running = begin()
    await gate.arrived()
    return async () => {
      release(gate)
      if (expectedError) {
        await expect(running).rejects.toThrow(expectedError)
      } else {
        await running
      }
    }
  }

  switch (kind) {
    case 'resolver':
      return hold(
        'ListOffsets',
        () => ctx.manager.resolveOffsets(),
        (gate) =>
          gate.release(
            listOffsetsAnswer(Object.fromEntries(partitions.map((p) => [p, RESOLVER_ANSWER])))
          )
      )
    case 'failingResolver':
      return hold(
        'ListOffsets',
        () => ctx.manager.resolveOffsets(),
        (gate) => gate.fail(new Error('list offsets unavailable')),
        'list offsets unavailable'
      )
    case 'epochResolver':
      return hold(
        'OffsetFetch',
        () => ctx.manager.resolveOffsets(),
        (gate) =>
          gate.release(
            offsetFetchAnswer(Object.fromEntries(partitions.map((p) => [p, EPOCH_OFFSET])), 7)
          )
      )
    case 'commit':
      return hold(
        'OffsetCommit',
        () => ctx.manager.commitOffsets(commitOf(partition, value)),
        (gate) => gate.release({})
      )
    case 'seekUp':
    case 'seekDown':
      return hold(
        'OffsetCommit',
        () => ctx.manager.seek({ topic: TOPIC, partition, offset: value }),
        (gate) => gate.release({})
      )
    case 'publish': {
      const ticket = ctx.operations.begin([{ topic: TOPIC, partition }], { mutation: true })
      return async () => {
        if (!ctx.operations.isSuperseded(ticket, TOPIC, partition)) {
          ctx.cache[TOPIC][partition] = value
          ctx.operations.apply(ticket, TOPIC, partition)
        }
        ctx.operations.end(ticket)
      }
    }
    case 'recreate':
      return async () => ctx.recreate(`${NEW_ID}-${index}`)
    case 'revoke':
      return async () => {
        const membership = { generationId: 2, memberId: 'member-a', coordinator: ctx.coordinator }
        const others = partitions.filter((p) => p !== partition)
        ctx.manager.updateAssignment({
          revoked: [{ topic: TOPIC, partitions: [partition] }],
          memberAssignment: { [TOPIC]: others },
          ...membership,
        })
        ctx.manager.updateAssignment({
          revoked: [],
          memberAssignment: { [TOPIC]: partitions },
          ...membership,
        })
      }
    default:
      throw new Error(`unknown operation ${kind}`)
  }
}

/**
 * What the ordering must leave, from the precedence rules alone: operations are fenced by what
 * was applied to their cell, or by a barrier, before they completed. A mutation applies unless a
 * newer-begun mutation applied first or a barrier came first; a read writes an unresolved cell
 * only if neither happened; every owed cell is reset by a write, a seek, a commit or a publication.
 * @returns {{ before: object[], after: string[] }} each cell's state before and after quiescence
 */
const expectedState = (shape, partitionCount, order) => {
  const cells = Array.from({ length: partitionCount }, () => ({
    cache: undefined,
    owed: true,
    fenced: false,
    barrier: false,
    newest: 0,
  }))
  const stored = Array(partitionCount).fill('99')
  let intent = 0
  const operations = shape.map((kind, index) => {
    const partition = index % partitionCount
    const value = valueOf(kind, index)
    if (kind === 'commit' || kind.startsWith('seek')) {
      stored[partition] = value
    }
    return { kind, partition, value, intent: MUTATIONS.has(kind) ? ++intent : 0 }
  })

  for (const at of order) {
    const { kind, partition, value, intent: own } = operations[at]
    if (kind === 'resolver' || kind === 'epochResolver') {
      for (const cell of cells) {
        if (!cell.fenced && cell.cache === undefined) {
          const reset = cell.owed || kind === 'resolver'
          cell.cache = reset ? (kind === 'resolver' ? RESOLVER_ANSWER : '0') : EPOCH_OFFSET
          cell.owed = reset ? false : cell.owed
        }
      }
    } else if (MUTATIONS.has(kind)) {
      const cell = cells[partition]
      if (!cell.barrier && cell.newest < own) {
        cell.newest = own
        cell.fenced = true
        cell.owed = false
        cell.cache = kind.startsWith('seek') ? undefined : value
      }
    } else if (kind === 'recreate') {
      for (const cell of cells) {
        Object.assign(cell, { cache: undefined, owed: true, fenced: true, barrier: true })
      }
    } else if (kind === 'revoke') {
      Object.assign(cells[partition], { cache: undefined, fenced: true, barrier: true })
    }
  }

  const before = cells.map(({ cache, owed }) => ({ cache, owed }))
  const after = cells.map(({ cache, owed }, partition) =>
    cache === undefined ? (owed ? '0' : stored[partition]) : cache
  )

  return { before, after }
}

/** Runs one ordering of one shape and returns what the manager holds, before and after quiescence. */
const explore = async (shape, partitionCount, order) => {
  const partitions = Array.from({ length: partitionCount }, (_, p) => p)
  const ctx = createScheduledOffsetManager({ partitions, topicId: NEW_ID, autoCommit: true })
  ctx.recreate(NEW_ID)

  const releases = []
  for (const [index, kind] of shape.entries()) {
    releases.push(await start(ctx, kind, { index, partition: index % partitionCount, partitions }))
  }
  for (const at of order) {
    await releases[at]()
  }

  const before = partitions.map((p) => ({ cache: ctx.cache[TOPIC][p], owed: ctx.owed(p) }))
  for (let pass = 0; pass < 4 && !(await ctx.manager.resolveOffsets()); pass++);

  return {
    before,
    after: partitions.map((p) => ctx.next(p)),
    committed: partitions.map((p) => ctx.committedAt(p)),
    epochs: partitions.map((p) => ctx.manager.lastConsumedEpoch(TOPIC, p)),
    ledger: ctx.operations.size(),
    obligations: ctx.obligations.size(),
  }
}

/** Fails with the schedule written out, ready to be added to the recorded orderings. */
const check = async (name, shape, partitionCount, order, seed) => {
  const expected = expectedState(shape, partitionCount, order)
  const actual = await explore(shape, partitionCount, order)
  try {
    expect(actual.before).toEqual(expected.before)
    expect(actual.after).toEqual(expected.after)
    expect(actual.committed).toEqual(expected.after)
    expect(actual.epochs.every((epoch) => epoch === -1)).toBe(true)
    expect(actual.ledger).toBe(0)
    expect(actual.obligations).toBe(0)
  } catch (error) {
    error.message += `\n\nrecord in fixtures/schedules.json: ${JSON.stringify({ name, shape, partitions: partitionCount, order, seed })}`
    throw error
  }
}

describe('Consumer > OffsetManager > exploring operation orderings', () => {
  describe.each(NAMED_SHAPES)('%s', (name, shape) => {
    test.each([1, 2])(
      'over %i partition(s) every ordering of the releases holds the invariants',
      async (partitionCount) => {
        const orderings = interleavings(shape)
        expect(orderings).toHaveLength(factorial(shape.length))

        for (const order of orderings) {
          await check(name, shape, partitionCount, order)
        }
      }
    )
  })

  test('a seeded sample of random shapes of four to six operations holds the invariants', async () => {
    const random = mulberry32(RANDOM_SEED)
    const pick = (list) => list[Math.floor(random() * list.length)]
    let explored = 0

    for (let at = 0; at < RANDOM_SHAPES; at++) {
      const size = 4 + Math.floor(random() * 3)
      const shape = Array.from({ length: size }, () => pick(RANDOM_KINDS))
      const partitionCount = (at % 2) + 1
      const orderings = interleavings(shape)

      expect(orderings).toHaveLength(factorial(shape.length))
      for (const order of orderings) {
        await check(`random shape ${at}`, shape, partitionCount, order, RANDOM_SEED)
        explored += 1
      }
    }

    expect(explored).toBe(RANDOM_RUNS)
  }, 540000)

  test('the explored set is the declared one', () => {
    expect(PLANNED_RUNS).toBe(DECLARED_RUNS)
  })

  test.each(recorded)(
    'the recorded ordering of $name still holds',
    async ({ name, shape, partitions, order, seed }) => {
      await check(name, shape, partitions, order, seed)
    }
  )
})
