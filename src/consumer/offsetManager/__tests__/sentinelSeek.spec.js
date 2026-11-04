const {
  TOPIC,
  NEW_ID,
  offsetFetchAnswer,
  listOffsetsAnswer,
  createScheduledOffsetManager,
  groupOver,
} = require('./fixtures/scheduledOffsetManager')
const ResetObligations = require('../resetObligations')

let contexts
beforeEach(() => {
  contexts = []
})
afterEach(async () => {
  for (const ctx of contexts) {
    ctx.manager.clearPendingSeeks?.()
    for (const { gate, api } of ctx.gates) {
      if (gate.settle)
        gate.release(
          api === 'OffsetFetch' ? offsetFetchAnswer({ 0: '-1' }) : listOffsetsAnswer({ 0: '0' })
        )
    }
    await Promise.allSettled(ctx.resolutions)
    expect(ctx.schedule.rules).toHaveLength(0)
    expect(ctx.operations.size()).toBe(0)
  }
})
/** Creates a tracked manager fixture. @param {object} [options] @returns {object} fixture @throws {Error} for invalid options. */
const fixture = (options) => {
  const ctx = createScheduledOffsetManager(options)
  ctx.gates = []
  ctx.resolutions = []
  const hold = ctx.schedule.hold.bind(ctx.schedule)
  ctx.schedule.hold = (selector) => {
    const gate = hold(selector)
    ctx.gates.push({ gate, api: selector.api })
    return gate
  }
  const resolve = ctx.manager.resolveOffsets.bind(ctx.manager)
  ctx.manager.resolveOffsets = () => {
    const result = resolve()
    ctx.resolutions.push(result)
    result.catch(() => {})
    return result
  }
  contexts.push(ctx)
  return ctx
}
/** Applies an explicit seek. @param {object} ctx @param {string} offset @param {number} [partition] @returns {Promise<boolean>} applied @throws {Error} on commit failure. */
const seek = (ctx, offset, partition = 0) => ctx.manager.seek({ topic: TOPIC, partition, offset })

for (const autoCommit of [false, true]) {
  for (const fromBeginning of [false, true]) {
    for (const sentinel of ['-2', '-1']) {
      test(`explicit sentinel ${sentinel} overrides committed offset with autoCommit ${autoCommit} and fromBeginning ${fromBeginning}`, async () => {
        const ctx = fixture({ autoCommit, committed: '99', listOffset: '37' })
        ctx.manager.topicConfigurations[TOPIC].fromBeginning = fromBeginning
        await seek(ctx, sentinel)
        await expect(ctx.manager.resolveOffsets()).resolves.toBe(true)
        expect(ctx.cluster.fetchTopicsOffset).toHaveBeenCalledWith([
          {
            topic: TOPIC,
            fromBeginning,
            partitions: [{ partition: 0, timestamp: Number(sentinel) }],
          },
        ])
        expect(ctx.next()).toBe('37')
        expect(ctx.heldAt()).toBe('37')
        expect(ctx.committedAt()).toBeUndefined()
        expect(ctx.manager.lastConsumedEpoch(TOPIC, 0)).toBe(-1)
        expect(ctx.coordinator.offsetCommit).not.toHaveBeenCalled()
        expect(ctx.manager.pendingSeeks.size).toBe(0)
        expect(ctx.operations.size()).toBe(0)
      })
    }
  }
}

test('opposite sentinels share one topic resolution without affecting an untouched partition', async () => {
  const ctx = fixture({ partitions: [0, 1, 2], listOffset: '37' })
  await seek(ctx, '-2', 0)
  await seek(ctx, '-1', 1)
  await ctx.manager.resolveOffsets()
  expect(ctx.cluster.fetchTopicsOffset).toHaveBeenCalledWith([
    {
      topic: TOPIC,
      fromBeginning: true,
      partitions: [
        { partition: 0, timestamp: -2 },
        { partition: 1, timestamp: -1 },
      ],
    },
  ])
  expect([ctx.next(0), ctx.next(1), ctx.next(2)]).toEqual(['37', '37', '99'])
  expect(ctx.manager.pendingSeeks.size).toBe(0)
})

test('a failed ListOffsets leaves the sentinel owed for the next attempt', async () => {
  const ctx = fixture({ listOffset: '37' })
  const error = new Error('list rejected')
  ctx.cluster.fetchTopicsOffset.mockRejectedValueOnce(error)
  await seek(ctx, '-1')
  await expect(ctx.manager.resolveOffsets()).rejects.toBe(error)
  expect(ctx.manager.pendingSeeks.get(TOPIC).get(0).timestamp).toBe(-1)
  expect(ctx.operations.size()).toBe(1)
  await ctx.manager.resolveOffsets()
  expect(ctx.next()).toBe('37')
  expect(ctx.cluster.fetchTopicsOffset.mock.calls[1][0][0].partitions[0].timestamp).toBe(-1)
  expect(ctx.manager.pendingSeeks.size).toBe(0)
})

test('a newer sentinel wins over an older held ListOffsets answer', async () => {
  const ctx = fixture({ listOffset: '83' })
  await seek(ctx, '-2')
  const held = ctx.schedule.hold({ api: 'ListOffsets' })
  const resolving = ctx.manager.resolveOffsets()
  await held.arrived()
  await seek(ctx, '-1')
  held.release(listOffsetsAnswer({ 0: '11' }))
  await expect(resolving).resolves.toBe(false)
  expect(ctx.committedAt()).toBeUndefined()
  expect(ctx.manager.pendingSeeks.get(TOPIC).get(0).timestamp).toBe(-1)
  await ctx.manager.resolveOffsets()
  expect(ctx.next()).toBe('83')
  expect(ctx.cluster.fetchTopicsOffset.mock.calls.at(-1)[0][0].partitions[0].timestamp).toBe(-1)
})

for (const api of ['OffsetFetch', 'ListOffsets']) {
  for (const autoCommit of [false, true]) {
    test(`a concrete seek beats a sentinel answer held at ${api} with autoCommit ${autoCommit}`, async () => {
      const ctx = fixture({ autoCommit })
      await seek(ctx, '-1')
      const held = ctx.schedule.hold({ api })
      const resolving = ctx.manager.resolveOffsets()
      await held.arrived()
      await seek(ctx, '44')
      held.release(
        api === 'ListOffsets' ? listOffsetsAnswer({ 0: '11' }) : offsetFetchAnswer({ 0: '99' })
      )
      await expect(resolving).resolves.toBe(false)
      await ctx.manager.resolveOffsets()
      expect(ctx.next()).toBe('44')
      expect(ctx.manager.pendingSeeks.size).toBe(0)
    })
  }
}

for (const during of [false, true]) {
  test(`a ledger commit cancels the sentinel ${during ? 'during' : 'before'} resolution`, async () => {
    const ctx = fixture()
    await seek(ctx, '-1')
    const held = during ? ctx.schedule.hold({ api: 'ListOffsets' }) : null
    const resolving = during ? ctx.manager.resolveOffsets() : null
    if (held) await held.arrived()
    await ctx.manager.commitOffsets({
      topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '55' }] }],
    })
    if (held) {
      held.release(listOffsetsAnswer({ 0: '11' }))
      await expect(resolving).resolves.toBe(false)
    }
    await ctx.manager.resolveOffsets()
    expect(ctx.next()).toBe('55')
    expect(ctx.manager.pendingSeeks.size).toBe(0)
    expect(ctx.operations.size()).toBe(0)
  })
}

test('a failed concrete seek commit preserves the pending sentinel', async () => {
  const ctx = fixture({ autoCommit: true, listOffset: '37' })
  await seek(ctx, '-1')
  const error = new Error('commit rejected')
  ctx.coordinator.offsetCommit.mockRejectedValueOnce(error)
  await expect(seek(ctx, '44')).rejects.toBe(error)
  await ctx.manager.resolveOffsets()
  expect(ctx.next()).toBe('37')
})

for (const during of [false, true]) {
  test(`revocation clears pending intent ${during ? 'during' : 'before'} resolution`, async () => {
    const ctx = fixture()
    await seek(ctx, '-1')
    const held = during ? ctx.schedule.hold({ api: 'ListOffsets' }) : null
    const resolving = during ? ctx.manager.resolveOffsets() : null
    if (held) await held.arrived()
    ctx.manager.updateAssignment({
      revoked: [{ topic: TOPIC, partitions: [0] }],
      memberAssignment: {},
      generationId: 2,
      memberId: 'next',
      coordinator: ctx.coordinator,
    })
    expect(ctx.manager.pendingSeeks.size).toBe(0)
    if (held) {
      held.release(listOffsetsAnswer({ 0: '11' }))
      await expect(resolving).resolves.toBe(false)
    }
    expect(ctx.cache[TOPIC]).toEqual({})
    expect(ctx.operations.size()).toBe(0)
  })
}

test('stop clears queued and applied sentinel seeks and fences their answers', async () => {
  const ctx = fixture()
  const group = groupOver(ctx)
  group.seek({ topic: TOPIC, partition: 0, offset: '-2' })
  group.seek({ topic: 'other-topic', partition: 0, offset: '44' })
  await seek(ctx, '-1')
  const held = ctx.schedule.hold({ api: 'ListOffsets' })
  const resolving = ctx.manager.resolveOffsets()
  await held.arrived()
  group.onStopRequested()
  expect(group.seekOffset.size).toBe(1)
  expect(group.seekOffset.pop('other-topic', 0)).toEqual({
    topic: 'other-topic',
    partition: 0,
    offset: '44',
  })
  expect(ctx.manager.pendingSeeks.size).toBe(0)
  held.release(listOffsetsAnswer({ 0: '11' }))
  await expect(resolving).resolves.toBe(false)
  expect(ctx.committedAt()).toBeUndefined()
  expect(ctx.operations.size()).toBe(0)
})

test('a stop during the OffsetFetch of a sentinel resolution drops its answer', async () => {
  const ctx = fixture()
  const group = groupOver(ctx)
  await seek(ctx, '-1')
  const held = ctx.schedule.hold({ api: 'OffsetFetch' })
  const resolving = ctx.manager.resolveOffsets()
  await held.arrived()
  group.onStopRequested()
  held.release(offsetFetchAnswer({ 0: '99' }))
  await expect(resolving).resolves.toBe(false)
  expect(ctx.committedAt()).toBeUndefined()
  expect(ctx.cluster.fetchTopicsOffset).not.toHaveBeenCalled()
  expect(ctx.operations.size()).toBe(0)
})

test("a stop leaves another consumer's resolution of the same cell alone", async () => {
  const ctx = fixture()
  const group = groupOver(ctx)
  const sibling = ctx.build({ obligations: new ResetObligations() })
  await seek(ctx, '-1')
  const held = ctx.schedule.hold({ api: 'OffsetFetch' })
  const resolving = sibling.resolveOffsets()
  await held.arrived()
  group.onStopRequested()
  held.release(offsetFetchAnswer({ 0: '99' }))
  await expect(resolving).resolves.toBe(true)
  expect(ctx.committedAt()).toBe('99')
  expect(ctx.operations.size()).toBe(0)
})

test('a stop during a sentinel resolution ends the repeated passes of the seek cycle', async () => {
  const ctx = fixture()
  const group = groupOver(ctx)
  group.seek({ topic: TOPIC, partition: 0, offset: '-2' })
  const held = ctx.schedule.hold({ api: 'ListOffsets' })
  const seeking = group.seekOffsets([{ topic: TOPIC, partitions: [0] }])
  await held.arrived()
  group.rebalanceCoordinator = { runner: { stopRequested: true } }
  group.onStopRequested()
  held.release(listOffsetsAnswer({ 0: '11' }))
  await seeking
  expect(ctx.coordinator.offsetFetch).toHaveBeenCalledTimes(1)
  expect(ctx.cluster.fetchTopicsOffset).toHaveBeenCalledTimes(1)
  expect(ctx.committedAt()).toBeUndefined()
})

test('a full assignment rebuild releases the retired manager sentinel ticket', async () => {
  const ctx = fixture()
  const group = groupOver(ctx)
  await seek(ctx, '-1')
  group.partitionsPerSubscribedTopic = new Map([[TOPIC, [0]]])
  await group.applyRebalanceResult({ assignment: { [TOPIC]: [0] } }, { rebuildOffsets: true })
  expect(group.offsetManager).not.toBe(ctx.manager)
  expect(ctx.manager.pendingSeeks.size).toBe(0)
  expect(ctx.operations.size()).toBe(0)
})

test('topic recreation clears an old sentinel and leaves its reset obligation owed', async () => {
  const ctx = fixture()
  await seek(ctx, '-1')
  ctx.recreate(NEW_ID)
  expect(ctx.manager.pendingSeeks.size).toBe(0)
  expect(ctx.operations.size()).toBe(0)
  expect(ctx.owed()).toBe(true)
})

test('successful sentinel publication leaves the recreation obligation owed', async () => {
  const ctx = fixture({ topicId: NEW_ID })
  ctx.obligations.owe(TOPIC, NEW_ID)
  await seek(ctx, '-1')
  expect(ctx.owed()).toBe(true)
  await ctx.manager.resolveOffsets()
  expect(ctx.owed()).toBe(true)
  expect(ctx.obligations.size()).toBe(1)
  expect(ctx.manager.pendingSeeks.size).toBe(0)
})

test('a sentinel publication leaves the reset of another consumer of the group owed too', async () => {
  const ctx = fixture({ topicId: NEW_ID })
  const other = new ResetObligations()
  other.attachTo(ctx.operations)
  ctx.obligations.owe(TOPIC, NEW_ID)
  other.owe(TOPIC, NEW_ID)
  await seek(ctx, '-1')
  expect(other.isOwed(TOPIC, 0)).toBe(true)
  await ctx.manager.resolveOffsets()
  expect(other.isOwed(TOPIC, 0)).toBe(true)
  other.clear()
})

test('a sentinel seek fences an answer held from before it and settles nothing', async () => {
  const ctx = fixture({ topicId: NEW_ID })
  ctx.recreate(NEW_ID)
  const held = ctx.schedule.hold({ api: 'OffsetFetch' })
  const resolving = ctx.manager.resolveOffsets()
  await held.arrived()
  await seek(ctx, '-1')
  held.release(offsetFetchAnswer({ 0: '99' }))
  await expect(resolving).resolves.toBe(false)
  expect(ctx.committedAt()).toBeUndefined()
  expect(ctx.owed()).toBe(true)
})

/**
 * What `consumer.run()` does after `consumer.stop()`: a manager built afresh resolves from the
 * broker. Returns the three things a recreation reset is judged by, apart: the position the next
 * fetch starts from, the committed cache, and the obligation as the stop left it.
 */
const runAgain = async (ctx) => {
  const owedAtStop = ctx.owed()
  const next = ctx.rebuild()
  await next.resolveOffsets()
  return {
    position: next.nextOffset(TOPIC, 0).toString(),
    cache: ctx.heldAt(0, next),
    shared: ctx.committedAt(),
    owedAtStop,
  }
}

/** The reset ran: ListOffsets by the topic's own default, position and cache at its boundary. */
const expectReset = (ctx, { position, cache, shared, owedAtStop }) => {
  expect(shared).toBeUndefined()
  expect(position).toBe('0')
  expect(cache).toBe('0')
  expect(owedAtStop).toBe(true)
  expect(ctx.owed()).toBe(true)
  expect(ctx.obligations.size()).toBe(1)
  expect(ctx.cluster.fetchTopicsOffset.mock.calls.at(-1)[0]).toEqual([
    { topic: TOPIC, fromBeginning: true, partitions: [{ partition: 0 }] },
  ])
}

for (const boundary of ['held', 'failed']) {
  test(`a stop with the ${boundary} boundary of a sentinel leaves the recreation reset owed, and the next run resets`, async () => {
    const ctx = fixture({ topicId: NEW_ID })
    const group = groupOver(ctx)
    ctx.recreate(NEW_ID)
    await seek(ctx, '-2')

    if (boundary === 'held') {
      const held = ctx.schedule.hold({ api: 'ListOffsets' })
      const resolving = ctx.manager.resolveOffsets()
      await held.arrived()
      group.onStopRequested()
      held.release(listOffsetsAnswer({ 0: '11' }))
      await expect(resolving).resolves.toBe(false)
    } else {
      const error = new Error('list rejected')
      ctx.schedule.once({ api: 'ListOffsets' }, error)
      await expect(ctx.manager.resolveOffsets()).rejects.toBe(error)
      group.onStopRequested()
    }

    expect(ctx.manager.pendingSeeks.size).toBe(0)
    expect(ctx.committedAt()).toBeUndefined()
    expect(ctx.manager.resolvedOffsetAt(TOPIC, 0)).toBeUndefined()
    expectReset(ctx, await runAgain(ctx))
  })
}

test('a stop during the yield between resolution passes leaves the recreation reset owed, and the next run resets', async () => {
  const ctx = fixture({ topicId: NEW_ID })
  const group = groupOver(ctx)
  group.rebalanceCoordinator = { runner: { stopRequested: false } }
  ctx.recreate(NEW_ID)
  group.seek({ topic: TOPIC, partition: 0, offset: '-2' })
  const held = ctx.schedule.hold({ api: 'ListOffsets' })
  const seeking = group.seekOffsets([{ topic: TOPIC, partitions: [0] }])
  await held.arrived()
  // A sibling assignment manager over the shared cache and ledger supersedes the held answer.
  ctx.build({ obligations: new ResetObligations() })
  expect(ctx.owed()).toBe(true)

  // Queued before the answer is released, so it runs inside the loop's own setImmediate yield.
  const stopping = new Promise((resolve) =>
    setImmediate(() => {
      group.rebalanceCoordinator.runner.stopRequested = true
      group.onStopRequested()
      resolve()
    })
  )
  held.release(listOffsetsAnswer({ 0: '11' }))
  await seeking
  await stopping

  expect(ctx.owed()).toBe(true)
  expect(ctx.committedAt()).toBeUndefined()
  expect(ctx.cluster.fetchTopicsOffset).toHaveBeenCalledTimes(1)
  expectReset(ctx, await runAgain(ctx))
  expect(ctx.cluster.fetchTopicsOffset).toHaveBeenCalledTimes(2)
})

test('control: a stop without a sentinel leaves the recreation reset owed, and the next run resets', async () => {
  const ctx = fixture({ topicId: NEW_ID })
  const group = groupOver(ctx)
  ctx.recreate(NEW_ID)
  group.onStopRequested()
  expect(ctx.committedAt()).toBeUndefined()
  expectReset(ctx, await runAgain(ctx))
})

test('a full rebuild with a pending sentinel leaves the recreation reset owed', async () => {
  const ctx = fixture({ topicId: NEW_ID })
  const group = groupOver(ctx)
  ctx.recreate(NEW_ID)
  await seek(ctx, '-1')
  group.partitionsPerSubscribedTopic = new Map([[TOPIC, [0]]])
  await group.applyRebalanceResult({ assignment: { [TOPIC]: [0] } }, { rebuildOffsets: true })
  expect(ctx.manager.pendingSeeks.size).toBe(0)
  expectReset(ctx, await runAgain(ctx))
})

test('a revocation with a pending sentinel leaves the recreation reset owed', async () => {
  const ctx = fixture({ topicId: NEW_ID })
  ctx.recreate(NEW_ID)
  await seek(ctx, '-1')
  ctx.manager.updateAssignment({
    revoked: [{ topic: TOPIC, partitions: [0] }],
    memberAssignment: {},
    generationId: 2,
    memberId: 'next',
    coordinator: ctx.coordinator,
  })
  expect(ctx.manager.pendingSeeks.size).toBe(0)
  expect(ctx.owed()).toBe(true)
})

test('a concrete seek without auto-commit after a sentinel leaves the recreation reset owed', async () => {
  const ctx = fixture({ topicId: NEW_ID })
  ctx.recreate(NEW_ID)
  await seek(ctx, '-1')
  expect(ctx.owed()).toBe(true)
  await seek(ctx, '44')
  expect(ctx.owed()).toBe(true)
  expect(ctx.next()).toBe('44')
  expect(ctx.manager.pendingSeeks.size).toBe(0)
})

test('a stop does not restore a reset that a newer commit already settled', async () => {
  const ctx = fixture({ topicId: NEW_ID })
  const group = groupOver(ctx)
  const sibling = ctx.build({ obligations: new ResetObligations() })
  ctx.recreate(NEW_ID)
  await seek(ctx, '-2')
  expect(ctx.owed()).toBe(true)
  await sibling.commitOffsets({
    topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '70' }] }],
  })
  expect(ctx.owed()).toBe(false)
  group.onStopRequested()
  expect(ctx.owed()).toBe(false)
  const { position, cache } = await runAgain(ctx)
  expect(position).toBe('70')
  expect(cache).toBe('70')
  expect(ctx.cluster.fetchTopicsOffset).not.toHaveBeenCalled()
})

test('many replacements and uses retain no historical sentinel or ledger cells', async () => {
  const ctx = fixture({ partitions: [0, 1] })
  for (let i = 0; i < 100; i++) {
    await seek(ctx, '-2', 0)
    await seek(ctx, '-1', 0)
    await seek(ctx, '-1', 1)
    expect(ctx.manager.pendingSeeks.size).toBe(1)
    expect(ctx.operations.size()).toBe(2)
    await ctx.manager.resolveOffsets()
    expect(ctx.manager.pendingSeeks.size).toBe(0)
    expect(ctx.operations.size()).toBe(0)
  }
  await seek(ctx, '-1')
  ctx.manager.clearAllOffsets()
  expect(ctx.manager.pendingSeeks.size).toBe(0)
  expect(ctx.operations.size()).toBe(0)
})

test('a commit begun before the sentinel cannot cancel its newer intent', async () => {
  const ctx = fixture({ listOffset: '83' })
  const commit = ctx.schedule.hold({ api: 'OffsetCommit' })
  const committing = ctx.manager.commitOffsets({
    topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '55' }] }],
  })
  await commit.arrived()
  await seek(ctx, '-1')
  commit.release({})
  await committing
  await ctx.manager.resolveOffsets()
  expect(ctx.next()).toBe('83')
  expect(ctx.cluster.fetchTopicsOffset.mock.calls.at(-1)[0][0].partitions[0].timestamp).toBe(-1)
})

test('a stop before resolution releases all pending cells without a broker request', async () => {
  const ctx = fixture({ partitions: [0, 1] })
  const group = groupOver(ctx, [{ topic: TOPIC, partitions: [0, 1] }])
  await seek(ctx, '-2', 0)
  await seek(ctx, '-1', 1)
  group.onStopRequested()
  expect(ctx.manager.pendingSeeks.size).toBe(0)
  expect(ctx.operations.size()).toBe(0)
  expect(ctx.cluster.fetchTopicsOffset).not.toHaveBeenCalled()
})

test('a sibling cache fill does not erase a current explicit sentinel', async () => {
  const ctx = fixture({ committed: '99' })
  const sibling = ctx.build()
  await ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '-1' })
  const held = ctx.schedule.hold({ api: 'ListOffsets' })
  const resolving = ctx.manager.resolveOffsets()
  await held.arrived()
  await sibling.resolveOffsets()
  expect(ctx.committedAt()).toBe('99')
  held.release(listOffsetsAnswer({ 0: '83' }))
  await resolving
  expect(ctx.next()).toBe('83')
  expect(ctx.manager.pendingSeeks.size).toBe(0)
  expect(ctx.operations.size()).toBe(0)
})

test('a successful sentinel settles its intent and a second resolution sends no ListOffsets', async () => {
  const ctx = fixture({ listOffset: '37' })
  await seek(ctx, '-1')
  await ctx.manager.resolveOffsets()
  expect(ctx.manager.pendingSeeks.size).toBe(0)
  expect(ctx.operations.size()).toBe(0)
  expect(ctx.heldAt()).toBe('37')
  expect(ctx.committedAt()).toBeUndefined()
  await ctx.manager.resolveOffsets()
  expect(ctx.cluster.fetchTopicsOffset).toHaveBeenCalledTimes(1)
  expect(ctx.manager.pendingSeeks.size).toBe(0)
  expect(ctx.operations.size()).toBe(0)
})
