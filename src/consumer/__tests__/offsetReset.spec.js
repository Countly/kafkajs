const { normalizeResetPolicy, resetTimestamp } = require('../offsetReset')
const { KafkaJSNonRetriableError } = require('../../errors')
const Generation = require('../../producer/generation')
const GroupOwnership = require('../groupOwnership')
const ResetObligations = require('../offsetManager/resetObligations')
const {
  TOPIC,
  NEW_ID,
  OLD_ID,
  createScheduledOffsetManager,
  listOffsetsAnswer,
  offsetFetchAnswer,
} = require('../offsetManager/__tests__/fixtures/scheduledOffsetManager')

describe('offset reset', () => {
  test.each([
    [{}, { kind: 'latest' }],
    [{ fromBeginning: true }, { kind: 'earliest' }],
    [{ fromBeginning: false, offsetReset: 'latest' }, { kind: 'latest' }],
    [{ fromBeginning: true, offsetReset: 'earliest' }, { kind: 'earliest' }],
    [{ offsetReset: 'none' }, { kind: 'none' }],
    [
      { offsetReset: 'by_duration:p1dt2h3m4.123456789s' },
      { kind: 'by_duration', durationMs: 93784123 },
    ],
    [{ offsetReset: 'by_duration:PT0S' }, { kind: 'by_duration', durationMs: 0 }],
    [
      { offsetReset: 'by_duration:PT9007199254740.991S' },
      { kind: 'by_duration', durationMs: Number.MAX_SAFE_INTEGER },
    ],
  ])('normalizes %j without changing legacy defaults', (options, expected) =>
    expect(normalizeResetPolicy(options)).toEqual(expected)
  )
  test.each([
    '',
    3,
    null,
    'by_duration',
    'by_duration:',
    'by_duration:P',
    'by_duration:PT',
    'by_duration:P1DT',
    'by_duration:P-1D',
    'by_duration:P1W',
    'by_duration:P1Y',
    'by_duration:P1M',
    'by_duration:PT1.1234567890S',
    'by_duration:PT9007199254740.992S',
    'by_duration:PT99999999999999999999999S',
    'by_duration:P99999999999999D',
    'unknown',
  ])('rejects invalid form %j', (offsetReset) => {
    expect(() => normalizeResetPolicy({ offsetReset })).toThrow(KafkaJSNonRetriableError)
    expect(() => normalizeResetPolicy({ offsetReset })).toThrow(
      "The value must be either 'earliest', 'latest', 'none'"
    )
  })
  test('normalizes no options to the latest policy', () =>
    expect(normalizeResetPolicy()).toEqual({ kind: 'latest' }))
  test.each([true, false])(
    'explicit boolean %j conflicts with none and duration',
    (fromBeginning) => {
      for (const offsetReset of [
        'none',
        'by_duration:PT1S',
        fromBeginning ? 'latest' : 'earliest',
      ]) {
        expect(() => normalizeResetPolicy({ offsetReset, fromBeginning })).toThrow(
          KafkaJSNonRetriableError
        )
      }
    }
  )
  test.each([null, 0, '', {}, []])('rejects nonboolean fromBeginning %j', (fromBeginning) => {
    expect(() => normalizeResetPolicy({ fromBeginning })).toThrow(KafkaJSNonRetriableError)
  })
  test('computes fresh resolution timestamps and clamps raw negative sentinel boundaries', () => {
    const policy = normalizeResetPolicy({ offsetReset: 'by_duration:PT1S' })
    expect(resetTimestamp(policy, 11000)).toBe(10000)
    expect(resetTimestamp(policy, 21000)).toBe(20000)
    for (const now of [998, 999, 1000]) expect(resetTimestamp(policy, now)).toBe(0)
  })
  test('recreation resolution stays owed until a successful concrete commit', async () => {
    const ctx = createScheduledOffsetManager({
      topicId: NEW_ID,
      committed: '99',
      listOffset: '0',
    })
    ctx.recreate(NEW_ID)
    await ctx.manager.resolveOffsets()
    expect(ctx.next()).toBe('0')
    expect(ctx.stored[0]).toBe('99')
    expect(ctx.owed()).toBe(true)
    const rebuilt = ctx.rebuild()
    await rebuilt.resolveOffsets()
    expect(rebuilt.nextOffset(TOPIC, 0).toString()).toBe('0')
    expect(ctx.cluster.fetchTopicsOffset).toHaveBeenCalledTimes(2)
    await rebuilt.commitOffsets({
      topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '1' }] }],
    })
    expect(ctx.stored[0]).toBe('1')
    expect(ctx.owed()).toBe(false)
    const committed = ctx.rebuild()
    await committed.resolveOffsets()
    expect(committed.nextOffset(TOPIC, 0).toString()).toBe('1')
    expect(ctx.cluster.fetchTopicsOffset).toHaveBeenCalledTimes(2)
  })
  test('a local concrete seek fences old work without settling recreation debt', async () => {
    const ctx = createScheduledOffsetManager({
      topicId: NEW_ID,
      committed: '99',
      autoCommit: false,
    })
    ctx.recreate(NEW_ID)
    await ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '7' })
    expect(ctx.next()).toBe('7')
    expect(ctx.owed()).toBe(true)
    expect(ctx.coordinator.offsetCommit).not.toHaveBeenCalled()
    const rebuilt = ctx.rebuild()
    await rebuilt.resolveOffsets()
    expect(rebuilt.nextOffset(TOPIC, 0).toString()).toBe('0')
  })
  test('a sentinel publication leaves recreation debt owed without a broker commit', async () => {
    const ctx = createScheduledOffsetManager({
      topicId: NEW_ID,
      committed: '99',
      autoCommit: false,
    })
    ctx.recreate(NEW_ID)
    await ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '-2' })
    await ctx.manager.resolveOffsets()
    expect(ctx.next()).toBe('0')
    expect(ctx.owed()).toBe(true)
    expect(ctx.coordinator.offsetCommit).not.toHaveBeenCalled()
  })
})

const assert = require('node:assert/strict')
const createConsumer = require('../index')
const Runner = require('../runner')
const TimingOffsetManager = require('../offsetManager')
const TimingOffsetOperations = require('../offsetManager/offsetOperations')
const InstrumentationEmitter = require('../../instrumentation/emitter')
const logger = {
  debug() {},
  debugEnabled() {
    return false
  },
  error() {},
  info() {},
  warn() {},
  namespace() {
    return this
  },
}
const answer = () => ({
  responses: [{ topic: 'topic', partitions: [{ partition: 0, offset: '99' }] }],
})
const boundary = () => [{ topic: 'topic', partitions: [{ partition: 0, offset: '0' }] }]
/**
 * Replays the inherited public stop/run timing with independent broker truth.
 * Only Runner startup is stubbed; stop/run and offset state remain real.
 * @param {'held'|'offset'|'completed'} mode where the stop is issued
 * @returns {Promise<object>} positions, obligation and boundary observations
 * @throws {Error} semantic or lifecycle failure
 */
async function runRecreationTiming(mode) {
  const cache = Object.create(null)
  const operations = new TimingOffsetOperations()
  let group
  let recreation
  let releaseList
  let releaseOffset
  let listArrived
  let offsetArrived
  let lists = 0
  const listReady = new Promise((resolve) => {
    listArrived = resolve
  })
  const offsetReady = new Promise((resolve) => {
    offsetArrived = resolve
  })
  const cluster = {
    committedOffsets: () => cache,
    offsetOperations: () => operations,
    topicId: () => 'new-id',
    onTopicRecreated: (listener) => {
      recreation = listener
      return () => {
        recreation = null
      }
    },
    addMultipleTargetTopics: async () => {},
    disconnect: async () => {},
    fetchTopicsOffset: async () => {
      lists++
      listArrived()
      if (mode === 'completed' || mode === 'offset') return boundary()
      return new Promise((resolve) => {
        releaseList = () => resolve(boundary())
      })
    },
  }
  const coordinator = {
    isConnected: () => true,
    offsetFetch: async () => {
      if (mode !== 'offset') return answer()
      offsetArrived()
      return new Promise((resolve) => {
        releaseOffset = () => {
          coordinator.offsetFetch = async () => answer()
          resolve(answer())
        }
      })
    },
  }
  const options = {
    cluster,
    coordinator,
    memberAssignment: { topic: [0] },
    autoCommit: false,
    topicConfigurations: { topic: { fromBeginning: true } },
    groupId: 'group',
    generationId: 1,
    memberId: 'member',
    operations,
    instrumentationEmitter: new InstrumentationEmitter(),
  }
  const originalStart = Runner.prototype.start
  Runner.prototype.start = async function () {
    group = this.consumerGroup
    this.running = true
  }
  const consumer = createConsumer({
    cluster,
    logger,
    groupId: 'group',
    metadataMaxAge: 300000,
    retry: { retries: 0 },
  })
  try {
    await consumer.subscribe({ topic: 'topic', fromBeginning: true })
    await consumer.run({ autoCommit: false, eachMessage: async () => {} })
    group.offsetManager = new TimingOffsetManager({
      ...options,
      obligations: group.obligations,
      ownership: group.ownership,
      isStopping: () => group.isStopping(),
    })
    cache.topic[0] = '5'
    recreation({ payload: { topic: 'topic', topicId: 'new-id' } })
    const resolving = group.seekOffsets([{ topic: 'topic', partitions: [0] }])
    if (mode === 'completed') await resolving
    else if (mode === 'offset') await offsetReady
    else await listReady
    const owedBeforeStop = group.obligations.isOwed('topic', 0)
    const stopping = consumer.stop()
    if (releaseList) releaseList()
    if (releaseOffset) releaseOffset()
    await resolving
    await stopping
    const cacheAfterStop = cache.topic[0]
    const owedAfterStop = group.obligations.isOwed('topic', 0)
    const listsAtStop = lists
    await consumer.run({ autoCommit: false, eachMessage: async () => {} })
    const restarted = new TimingOffsetManager({
      ...options,
      obligations: group.obligations,
      ownership: group.ownership,
      isStopping: () => group.isStopping(),
    })
    group.offsetManager = restarted
    cluster.fetchTopicsOffset = async () => {
      lists++
      return boundary()
    }
    await restarted.resolveOffsets()
    const value = restarted.nextOffset('topic', 0).toString()
    const result = {
      mode,
      owedBeforeStop,
      owedAfterStop,
      cacheAfterStop: cacheAfterStop ?? null,
      listsAtStop,
      value,
      cache: cache.topic[0],
      lists,
      cells: operations.size(),
    }
    assert.equal(value, '0')
    assert.equal(owedBeforeStop, true)
    assert.equal(owedAfterStop, true)
    assert.equal(lists, 2)
    assert.equal(restarted.heldOffsetAt('topic', 0), value)
    assert.equal(operations.size(), 0)
    return result
  } finally {
    await consumer.disconnect()
    Runner.prototype.start = originalStart
  }
}

test.each(['held', 'offset', 'completed'])(
  'recreation debt survives public stop at %s',
  async (mode) => {
    await runRecreationTiming(mode)
  }
)

test.each([false, true])(
  'duration default reset uses an actual offset with autoCommit %s',
  async (autoCommit) => {
    const ctx = createScheduledOffsetManager({
      topicId: NEW_ID,
      committed: '99',
      listOffset: '12',
      autoCommit,
    })
    ctx.manager.topicConfigurations[TOPIC].resetPolicy = { kind: 'by_duration', durationMs: 1000 }
    ctx.recreate(NEW_ID)
    await ctx.manager.setDefaultOffset({ topic: TOPIC, partition: 0 })
    expect(ctx.next()).toBe('12')
    expect(ctx.owed()).toBe(!autoCommit)
    if (autoCommit) {
      expect(ctx.stored[0]).toBe('12')
      expect(ctx.coordinator.offsetCommit.mock.calls[0][0].topics[0].partitions).toEqual([
        { partition: 0, offset: '12' },
      ])
    } else expect(ctx.coordinator.offsetCommit).not.toHaveBeenCalled()
    expect(ctx.operations.size()).toBe(0)
  }
)

test.each(['before lookup', 'before acknowledgement'])(
  'duration reset retains recreation debt with unknown identity %s',
  async (stage) => {
    const ctx = createScheduledOffsetManager({
      topicId: NEW_ID,
      committed: '99',
      listOffset: '12',
      autoCommit: true,
    })
    ctx.recreate(NEW_ID)
    ctx.manager.topicConfigurations[TOPIC].resetPolicy = { kind: 'by_duration', durationMs: 1000 }
    if (stage === 'before lookup') ctx.setTopicId(undefined)
    const gate = ctx.schedule.hold({ api: 'OffsetCommit' })
    const resetting = ctx.manager.setDefaultOffset({ topic: TOPIC, partition: 0 })
    await gate.arrived()
    if (stage === 'before acknowledgement') ctx.setTopicId(undefined)
    gate.release({})
    expect(await resetting).toBe(true)
    expect(ctx.stored[0]).toBe('12')
    expect(ctx.next()).toBe('12')
    expect(ctx.owed()).toBe(true)
    expect(ctx.operations.size()).toBe(0)
  }
)

const durationRound = (options) => {
  const ctx = createScheduledOffsetManager({
    topicId: NEW_ID,
    committed: '99',
    listOffset: '12',
    autoCommit: true,
    ...options,
  })
  ctx.recreate(NEW_ID)
  ctx.manager.topicConfigurations[TOPIC].resetPolicy = { kind: 'by_duration', durationMs: 1000 }
  const fence = new Generation()
  return { ctx, fence, preparation: { guard: fence.guard, fence } }
}

test('a duration reset hands its round fence to the lookup and the commit', async () => {
  const { ctx, fence, preparation } = durationRound()
  const send = jest.spyOn(ctx.manager, 'sendOffsetCommit')
  await ctx.manager.setDefaultOffset({ topic: TOPIC, partition: 0 }, preparation)
  expect(ctx.cluster.fetchTopicsOffset).toHaveBeenCalledWith(expect.any(Array), {
    fence,
    guard: fence.guard,
  })
  expect(send).toHaveBeenCalledWith(expect.any(Object), expect.objectContaining({ fence }))
  expect(ctx.next()).toBe('12')
})

test('a duration reset of a round closed before it starts sends nothing', async () => {
  const { ctx, fence, preparation } = durationRound()
  const before = ctx.next()
  fence.close(new Error('round closed'))
  const outcome = await ctx.manager
    .setDefaultOffset({ topic: TOPIC, partition: 0 }, preparation)
    .then(
      () => 'resolved',
      (error) => error.message
    )
  expect(outcome).toBe('round closed')
  expect(ctx.cluster.fetchTopicsOffset).not.toHaveBeenCalled()
  expect(ctx.coordinator.offsetCommit).not.toHaveBeenCalled()
  expect(ctx.next()).toBe(before)
  expect(ctx.operations.size()).toBe(0)
})

test('a duration reset whose round closes during the lookup sends no later lookup or commit', async () => {
  const { ctx, fence, preparation } = durationRound({ listOffset: '-1' })
  const before = ctx.next()
  const gate = ctx.schedule.hold({ api: 'ListOffsets' })
  const resetting = ctx.manager.setDefaultOffset({ topic: TOPIC, partition: 0 }, preparation).then(
    (applied) => applied,
    (error) => error.message
  )
  await gate.arrived()
  fence.close(new Error('round closed'))
  gate.release([{ topic: TOPIC, partitions: [{ partition: 0, offset: '-1' }] }])
  expect(await resetting).toBe(false)
  expect(ctx.cluster.fetchTopicsOffset).toHaveBeenCalledTimes(1)
  expect(ctx.coordinator.offsetCommit).not.toHaveBeenCalled()
  expect(ctx.next()).toBe(before)
  expect(ctx.owed()).toBe(true)
  expect(ctx.operations.size()).toBe(0)
})

const DURATION = { kind: 'by_duration', durationMs: 1000 }
const commitOf = (offset) => ({
  topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset }] }],
})

test('duration timestamps are recomputed on each resolution and fallback only once', async () => {
  const ctx = createScheduledOffsetManager({ committed: '-1', listOffset: '12' })
  ctx.manager.topicConfigurations[TOPIC].resetPolicy = DURATION
  const now = jest.spyOn(Date, 'now')
  try {
    for (const [clock, timestamp] of [
      [11000, 10000],
      [21000, 20000],
      [998, 0],
      [999, 0],
    ]) {
      now.mockReturnValue(clock)
      ctx.manager.resetOffsetsForTopic(TOPIC)
      await ctx.manager.resolveOffsets()
      expect(ctx.cluster.fetchTopicsOffset.mock.calls.at(-1)[0][0].partitions).toEqual([
        { partition: 0, timestamp },
      ])
    }
    const before = ctx.cluster.fetchTopicsOffset.mock.calls.length
    ctx.cluster.fetchTopicsOffset
      .mockImplementationOnce(async () => listOffsetsAnswer({ 0: '-1' }))
      .mockImplementationOnce(async () => listOffsetsAnswer({ 0: '12' }))
    now.mockReturnValue(11000)
    ctx.manager.resetOffsetsForTopic(TOPIC)
    await ctx.manager.resolveOffsets()
    const calls = ctx.cluster.fetchTopicsOffset.mock.calls.slice(before)
    expect(calls.map(([topics]) => topics[0].partitions)).toEqual([
      [{ partition: 0, timestamp: 10000 }],
      [{ partition: 0 }],
    ])
    expect(ctx.next()).toBe('12')
    expect(ctx.coordinator.offsetCommit).not.toHaveBeenCalled()
  } finally {
    now.mockRestore()
  }
})

const heldReset = async (ctx) => {
  ctx.manager.topicConfigurations[TOPIC].resetPolicy = DURATION
  const gate = ctx.schedule.hold({ api: 'ListOffsets' })
  const resetting = ctx.manager.setDefaultOffset({ topic: TOPIC, partition: 0 })
  await gate.arrived()
  return { gate, resetting }
}

test('a newer seek or commit wins a held duration reset in either completion order', async () => {
  const seeking = createScheduledOffsetManager({ committed: '99', listOffset: '12' })
  const seekHeld = await heldReset(seeking)
  await seeking.manager.seek({ topic: TOPIC, partition: 0, offset: '7' })
  seekHeld.gate.release(listOffsetsAnswer({ 0: '12' }))
  expect(await seekHeld.resetting).toBe(false)
  expect(seeking.next()).toBe('7')
  expect(seeking.operations.size()).toBe(0)

  const seekLast = createScheduledOffsetManager({ committed: '99', listOffset: '12' })
  seekLast.manager.topicConfigurations[TOPIC].resetPolicy = DURATION
  expect(await seekLast.manager.setDefaultOffset({ topic: TOPIC, partition: 0 })).toBe(true)
  await seekLast.manager.seek({ topic: TOPIC, partition: 0, offset: '7' })
  expect(seekLast.next()).toBe('7')
  expect(seekLast.operations.size()).toBe(0)

  const committing = createScheduledOffsetManager({ committed: '99', listOffset: '12' })
  const commitHeld = await heldReset(committing)
  await committing.manager.commitOffsets(commitOf('3'))
  commitHeld.gate.release(listOffsetsAnswer({ 0: '12' }))
  expect(await commitHeld.resetting).toBe(false)
  expect(committing.committedAt()).toBe('3')

  const commitLast = createScheduledOffsetManager({ committed: '99', listOffset: '12' })
  commitLast.manager.topicConfigurations[TOPIC].resetPolicy = DURATION
  expect(await commitLast.manager.setDefaultOffset({ topic: TOPIC, partition: 0 })).toBe(true)
  expect(commitLast.heldAt()).toBe('12')
  expect(commitLast.committedAt()).toBeUndefined()
  await commitLast.manager.commitOffsets(commitOf('3'))
  expect(commitLast.committedAt()).toBe('3')
  expect(commitLast.operations.size()).toBe(0)
})

test('duration reset without auto-commit never writes an automatic commit after seek', async () => {
  const ctx = createScheduledOffsetManager({ committed: '-1', listOffset: '12', autoCommit: false })
  ctx.manager.topicConfigurations[TOPIC].resetPolicy = DURATION
  await ctx.manager.resolveOffsets()
  expect(ctx.next()).toBe('12')
  await ctx.manager.setDefaultOffset({ topic: TOPIC, partition: 0 })
  expect(ctx.next()).toBe('12')
  await ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '5' })
  expect(ctx.next()).toBe('5')
  expect(ctx.coordinator.offsetCommit).not.toHaveBeenCalled()
  await ctx.manager.commitOffsets(commitOf('12'))
  expect(ctx.coordinator.offsetCommit).toHaveBeenCalledTimes(1)
  expect(ctx.stored[0]).toBe('12')
})

describe('recreation debt after a confirmed commit', () => {
  const owing = (options) => {
    const ctx = createScheduledOffsetManager({
      topicId: NEW_ID,
      committed: '99',
      listOffset: '0',
      autoCommit: true,
      ...options,
    })
    ctx.recreate(NEW_ID)
    return ctx
  }
  const owedAfter = async (ctx, disturb) => {
    const gate = ctx.schedule.hold({ api: 'OffsetCommit' })
    const committing = ctx.manager.commitOffsets(commitOf('1')).then(
      () => 'committed',
      (error) => error.name
    )
    await gate.arrived()
    await disturb(gate)
    const outcome = await committing
    return { outcome, owed: ctx.owed() }
  }

  test('failed or stale concrete commits leave recreation debt owed', async () => {
    const failed = await owedAfter(owing(), async (gate) => gate.fail(new Error('refused')))
    expect(failed).toEqual({ outcome: 'Error', owed: true })

    const recreated = owing()
    const crossing = await owedAfter(recreated, async (gate) => {
      recreated.recreate('31323334-3536-3738-393a-3b3c3d3e3f40')
      gate.release()
    })
    expect(crossing).toEqual({ outcome: 'committed', owed: true })

    const revoked = owing()
    const left = await owedAfter(revoked, async (gate) => {
      revoked.manager.updateAssignment({
        revoked: [{ topic: TOPIC, partitions: [0] }],
        memberAssignment: { [TOPIC]: [] },
        generationId: 1,
        memberId: 'member-a',
        coordinator: revoked.coordinator,
      })
      gate.release()
    })
    expect(left.owed).toBe(true)

    const ownership = new GroupOwnership()
    const lost = owing()
    const stale = lost.build({ ownership })
    const gate = lost.schedule.hold({ api: 'OffsetCommit' })
    const committing = stale.commitOffsets(commitOf('1')).then(
      () => 'committed',
      (error) => error.name
    )
    await gate.arrived()
    ownership.renew()
    gate.release()
    expect(await committing).not.toBe('committed')
    expect(lost.owed()).toBe(true)

    const unknown = owing()
    unknown.setTopicId(undefined)
    await unknown.manager.commitOffsets(commitOf('1'))
    expect(unknown.owed()).toBe(true)

    const confirmed = owing()
    await confirmed.manager.commitOffsets(commitOf('1'))
    expect(confirmed.owed()).toBe(false)
  })

  test('partial and empty assignments cannot erase another partition debt', async () => {
    const ctx = owing({ partitions: [0, 1] })
    ctx.cluster.findTopicPartitionMetadata = () => [{ partitionId: 0 }, { partitionId: 1 }]
    const holder = ctx.build({ memberAssignment: { [TOPIC]: [0] } })
    await holder.commitOffsets(commitOf('1'))
    await holder.resolveOffsets()
    expect(ctx.obligations.isOwed(TOPIC, 0)).toBe(false)
    expect(ctx.obligations.isOwed(TOPIC, 1)).toBe(true)
    expect(ctx.obligations.size()).toBe(1)

    const table = new ResetObligations()
    table.owe(TOPIC, NEW_ID)
    table.discharge(TOPIC, 0)
    table.settle(TOPIC, [], [0, 1])
    table.settle(TOPIC, [0], [0, 1])
    table.settle(TOPIC, [0], [])
    expect(table.size()).toBe(1)
    table.discharge(TOPIC, 1)
    table.settle(TOPIC, [], [0, 1])
    expect(table.size()).toBe(0)

    table.owe(TOPIC, OLD_ID)
    table.discharge(TOPIC, 0)
    table.owe(TOPIC, NEW_ID)
    table.settle(TOPIC, [0], [0])
    expect(table.size()).toBe(1)
  })
})

test('recreation debt survives assignment rebuild at each inherited gate', async () => {
  for (const stage of ['held', 'offset', 'completed']) {
    const ctx = createScheduledOffsetManager({ topicId: NEW_ID, committed: '99', listOffset: '0' })
    ctx.recreate(NEW_ID)
    const gate =
      stage === 'completed'
        ? null
        : ctx.schedule.hold({ api: stage === 'held' ? 'ListOffsets' : 'OffsetFetch' })
    const resolving = ctx.manager.resolveOffsets()
    if (gate) {
      await gate.arrived()
      ctx.manager.clearPendingSeeks()
      gate.release(
        stage === 'held' ? listOffsetsAnswer({ 0: '0' }) : offsetFetchAnswer({ 0: '99' })
      )
    }
    await resolving
    expect(ctx.owed()).toBe(true)
    const rebuilt = ctx.rebuild()
    await rebuilt.resolveOffsets()
    expect(rebuilt.nextOffset(TOPIC, 0).toString()).toBe('0')
    expect(ctx.stored[0]).toBe('99')
    expect(ctx.coordinator.offsetCommit).not.toHaveBeenCalled()
    expect(ctx.operations.size()).toBe(0)
  }
})
