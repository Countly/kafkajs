const {
  TOPIC,
  NEW_ID,
  offsetFetchAnswer,
  listOffsetsAnswer,
  createScheduledOffsetManager,
  groupOver,
} = require('./fixtures/scheduledOffsetManager')

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
        expect(ctx.committedAt()).toBe('37')
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

test('successful sentinel publication discharges the recreation obligation', async () => {
  const ctx = fixture({ topicId: NEW_ID })
  ctx.obligations.owe(TOPIC, NEW_ID)
  await seek(ctx, '-1')
  await ctx.manager.resolveOffsets()
  expect(ctx.owed()).toBe(false)
  expect(ctx.obligations.size()).toBe(0)
  expect(ctx.manager.pendingSeeks.size).toBe(0)
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
