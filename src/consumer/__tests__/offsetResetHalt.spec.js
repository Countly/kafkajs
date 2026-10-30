const { ResetResolver } = require('../offsetReset')
const { KafkaJSNoOffsetError } = require('../../errors')
const { Kafka, logLevel } = require('../../../index')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { RecordBatch } = require('../../protocol/recordBatch/v0')
const RecordEncoder = require('../../protocol/recordBatch/record/v0')
const GroupOwnership = require('../groupOwnership')
const {
  events: { PARTITION_HALTED, LOG_TRUNCATION },
} = require('../instrumentationEvents')
const {
  TOPIC,
  NEW_ID,
  createScheduledOffsetManager,
  groupOver,
  offsetFetchAnswer,
} = require('../offsetManager/__tests__/fixtures/scheduledOffsetManager')

test('none refuses an unresolved partition without ListOffsets and keeps a committed sibling', async () => {
  const cluster = {
    fetchTopicsOffset: jest
      .fn()
      .mockResolvedValue([{ topic: 't', partitions: [{ partition: 0, offset: '0' }] }]),
  }
  const identity = { assignmentEpoch: 3, requestClock: 7, topicId: 'new' }
  const result = await new ResetResolver(cluster).resolve(
    [
      {
        topic: 't',
        partitions: [
          { partition: 0, offset: '-1' },
          { partition: 1, offset: '4' },
        ],
      },
    ],
    { t: { resetPolicy: { kind: 'none' } } },
    () => true,
    () => identity
  )
  expect(cluster.fetchTopicsOffset).not.toHaveBeenCalled()
  expect(result.offsets).toEqual([{ topic: 't', partitions: [{ partition: 1, offset: '4' }] }])
  expect(result.refusals).toEqual([
    expect.objectContaining({
      topic: 't',
      partition: 0,
      offset: null,
      reason: 'no-offset',
      context: identity,
    }),
  ])
  expect(result.refusals[0].error).toBeInstanceOf(KafkaJSNoOffsetError)
  expect(result.refusals[0].error.retriable).toBe(false)
})
test('a stale none resolution produces no refusal or position', async () => {
  const cluster = { fetchTopicsOffset: jest.fn() }
  const result = await new ResetResolver(cluster).resolve(
    [{ topic: 't', partitions: [{ partition: 0, offset: '-1' }] }],
    { t: { resetPolicy: { kind: 'none' } } },
    () => false,
    () => ({})
  )
  expect(result.refusals).toEqual([])
  expect(result.offsets).toEqual([{ topic: 't', partitions: [] }])
  expect(cluster.fetchTopicsOffset).not.toHaveBeenCalled()
})
test('duration misses use one latest fallback with only the missing partition', async () => {
  const now = jest.spyOn(Date, 'now').mockReturnValue(11000)
  const cluster = {
    fetchTopicsOffset: jest
      .fn()
      .mockResolvedValueOnce([
        {
          topic: 't',
          partitions: [
            { partition: 0, offset: '-1' },
            { partition: 1, offset: '8' },
          ],
        },
      ])
      .mockResolvedValueOnce([{ topic: 't', partitions: [{ partition: 0, offset: '12' }] }]),
  }
  try {
    const result = await new ResetResolver(cluster).resolve(
      [
        {
          topic: 't',
          partitions: [
            { partition: 0, offset: '-1' },
            { partition: 1, offset: '-1' },
          ],
        },
      ],
      { t: { resetPolicy: { kind: 'by_duration', durationMs: 1000 } } },
      () => true,
      () => ({})
    )
    expect(cluster.fetchTopicsOffset.mock.calls).toEqual([
      [
        [
          {
            topic: 't',
            fromBeginning: false,
            partitions: [
              { partition: 0, timestamp: 10000 },
              { partition: 1, timestamp: 10000 },
            ],
          },
        ],
      ],
      [[{ topic: 't', fromBeginning: false, partitions: [{ partition: 0 }] }]],
    ])
    expect(result.offsets[0].partitions.map((p) => p.offset)).toEqual(['12', '8'])
  } finally {
    now.mockRestore()
  }
})
test('a current explicit sentinel seek overrides none without a refusal', async () => {
  const cluster = {
    fetchTopicsOffset: jest
      .fn()
      .mockResolvedValue([{ topic: 't', partitions: [{ partition: 0, offset: '3' }] }]),
  }
  const result = await new ResetResolver(cluster).resolve(
    [{ topic: 't', partitions: [{ partition: 0, offset: '-1' }] }],
    { t: { resetPolicy: { kind: 'none' } } },
    () => true,
    () => ({}),
    () => -2
  )
  expect(result.refusals).toEqual([])
  expect(result.offsets[0].partitions[0].offset).toBe('3')
  expect(cluster.fetchTopicsOffset.mock.calls[0][0][0].partitions).toEqual([
    { partition: 0, timestamp: -2 },
  ])
})

const KEY = `${TOPIC}|0`
const PARTS = [{ topic: TOPIC, partitions: [0, 1] }]

/**
 * A real group over a none-policy manager that captures its identity through the group, with
 * partition 0 uncommitted and partition 1 committed at 4.
 * @param {object} [options] over the scheduled manager fixture
 * @returns {object} the fixture plus `group`, `manager`, `halts` and `errors`
 */
const noneGroup = (options = {}) => {
  const ctx = createScheduledOffsetManager({ partitions: [0, 1], committed: '-1', ...options })
  ctx.stored[1] = '4'
  ctx.manager.topicConfigurations[TOPIC].resetPolicy = { kind: 'none' }
  const group = groupOver(ctx, [{ topic: TOPIC, partitions: [0, 1] }])
  group.topicConfigurations = ctx.manager.topicConfigurations
  const captureResetContext = (topic, partition, owner, preparation) =>
    group.captureResetContext(topic, owner, preparation)
  const manager = ctx.build({ captureResetContext })
  group.offsetManager = manager
  group.assignmentEpoch = 1
  for (const partition of [0, 1]) {
    const stamp = { acquiredAt: partition + 1, changedAt: partition + 1 }
    group.acquired.set(`${TOPIC}|${partition}`, stamp)
  }
  group.acquireClock = 2
  const halts = []
  group.instrumentationEmitter.addListener(PARTITION_HALTED, (event) => halts.push(event.payload))
  const errors = jest.spyOn(group.logger, 'error')
  return { ...ctx, group, manager, halts, errors, captureResetContext }
}

/**
 * Starts one resolution of the group's manager and holds its OffsetFetch; the refusals it raises
 * are installed when it settles, as `seekOffsets` does after each pass.
 * @param {object} ctx a `noneGroup`
 * @returns {Promise<{gate: {release: Function}, resolving: Promise<void>}>}
 */
const heldNone = async (ctx) => {
  const held = ctx.schedule.hold({ api: 'OffsetFetch' })
  const resolving = ctx.manager
    .resolveOffsets()
    .then(() => ctx.group.installResetRefusals(ctx.manager))
  await held.arrived()
  const gate = { release: () => held.release(offsetFetchAnswer({ 0: '-1', 1: '4' })) }
  return { gate, resolving }
}

const expectSilent = (ctx) => {
  expect(ctx.group.haltedPartitions.size).toBe(0)
  expect(ctx.halts).toHaveLength(0)
  expect(ctx.errors).not.toHaveBeenCalled()
  expect(ctx.coordinator.offsetCommit).not.toHaveBeenCalled()
}

test('none halts only the uncommitted partition and derives no position or request for it', async () => {
  const ctx = noneGroup()
  await ctx.group.seekOffsets(PARTS)
  expect(ctx.cluster.fetchTopicsOffset).not.toHaveBeenCalled()
  expect(ctx.group.haltedPartitions.get(KEY)).toMatchObject({ offset: null, reason: 'no-offset' })
  expect(ctx.group.haltedPartitions.has(`${TOPIC}|1`)).toBe(false)
  expect(ctx.halts).toHaveLength(1)
  expect(ctx.halts[0]).toMatchObject({ topic: TOPIC, partition: 0, offset: null })
  expect(ctx.halts[0].error).toBeInstanceOf(KafkaJSNoOffsetError)
  expect(ctx.errors).toHaveBeenCalledTimes(1)
  expect(ctx.errors.mock.calls[0][0]).toBe('Halted a partition; seek it to resume')
  expect(ctx.manager.resolvedOffsetAt(TOPIC, 0)).toBeUndefined()
  expect(ctx.manager.nextOffset(TOPIC, 1).toString()).toBe('4')
  expect(ctx.coordinator.offsetCommit).not.toHaveBeenCalled()
})

test('a second current refusal reports one episode', async () => {
  const ctx = noneGroup()
  await Promise.all([ctx.group.seekOffsets(PARTS), ctx.group.seekOffsets(PARTS)])
  const halt = ctx.group.haltedPartitions.get(KEY)
  await ctx.group.seekOffsets(PARTS)
  expect(ctx.group.haltedPartitions.get(KEY)).toBe(halt)
  expect(ctx.halts).toHaveLength(1)
  expect(ctx.errors).toHaveBeenCalledTimes(1)

  const throwing = noneGroup()
  throwing.group.instrumentationEmitter.addListener(PARTITION_HALTED, () => {
    throw new Error('listener failed')
  })
  throwing.errors.mockImplementation(() => {
    throw new Error('logger failed')
  })
  await throwing.group.seekOffsets(PARTS)
  expect(throwing.group.haltedPartitions.has(KEY)).toBe(true)
  expect(throwing.manager.nextOffset(TOPIC, 1).toString()).toBe('4')
})

test('same-offset seek makes the held refusal stale', async () => {
  const seeking = noneGroup()
  const held = await heldNone(seeking)
  seeking.group.seek({ topic: TOPIC, partition: 0, offset: '0' })
  await seeking.group.seekOffsets([{ topic: TOPIC, partitions: [0] }])
  held.gate.release()
  await held.resolving
  expect(seeking.group.haltedPartitions.size).toBe(0)
  expect(seeking.halts).toHaveLength(0)
  expect(seeking.coordinator.offsetCommit).not.toHaveBeenCalled()

  const marked = noneGroup()
  const markedHeld = await heldNone(marked)
  marked.group.markChanged(TOPIC, 0)
  markedHeld.gate.release()
  await markedHeld.resolving
  expectSilent(marked)
})

test('revoke and reacquisition make the held refusal stale', async () => {
  const ctx = noneGroup()
  const { gate, resolving } = await heldNone(ctx)
  ctx.group.acquired.delete(KEY)
  const acquiredAt = ++ctx.group.acquireClock
  ctx.group.acquired.set(KEY, { acquiredAt, changedAt: acquiredAt })
  gate.release()
  await resolving
  expectSilent(ctx)

  await ctx.group.seekOffsets(PARTS)
  expect(ctx.group.haltedPartitions.get(KEY)).toMatchObject({ offset: null, reason: 'no-offset' })
  expect(ctx.halts).toHaveLength(1)
})

test('topic replacement makes the held refusal stale', async () => {
  const ctx = noneGroup()
  const { gate, resolving } = await heldNone(ctx)
  ctx.setTopicId(NEW_ID)
  ctx.obligations.owe(TOPIC, NEW_ID)
  ctx.group.invalidateTopic(TOPIC)
  gate.release()
  await resolving
  expectSilent(ctx)
  expect(ctx.obligations.isOwed(TOPIC, 0)).toBe(true)
})

test('old run and old membership cannot install a refusal', async () => {
  const replaced = noneGroup()
  const replacedHeld = await heldNone(replaced)
  replaced.group.offsetManager = replaced.build()
  replacedHeld.gate.release()
  await replacedHeld.resolving.catch(() => {})
  expectSilent(replaced)

  const renewed = noneGroup()
  const renewedHeld = await heldNone(renewed)
  renewed.group.ownership = new GroupOwnership()
  renewed.group.ownership.renew()
  renewedHeld.gate.release()
  await renewedHeld.resolving.catch(() => {})
  expectSilent(renewed)
})

test('none retries an out-of-range follower once before halting the leader result', async () => {
  const ctx = noneGroup({ committed: '3' })
  const context = ctx.group.captureResetContext(TOPIC, ctx.manager)
  ctx.group.preferredReadReplicasPerTopicPartition[TOPIC] = {
    0: { nodeId: 1, expireAt: Date.now() + 60000 },
  }
  const asked = { topic: TOPIC, partition: 0, offset: '5' }
  await ctx.group.recoverFromOffsetOutOfRange(asked, undefined, context)
  expect(ctx.group.preferredReadReplicasPerTopicPartition[TOPIC][0]).toBeUndefined()
  expect(ctx.halts).toHaveLength(0)
  await ctx.group.recoverFromOffsetOutOfRange(asked, undefined, context)
  expect(ctx.group.haltedPartitions.get(KEY)).toMatchObject({
    offset: '5',
    reason: 'offset-out-of-range',
  })
  expect(ctx.halts).toHaveLength(1)
  expect(ctx.halts[0].error).toMatchObject({ reason: 'offset-out-of-range', partition: 0 })
  await ctx.group.recoverFromOffsetOutOfRange({ topic: TOPIC, partition: 1 }, undefined, context)
  expect(ctx.group.haltedPartitions.get(`${TOPIC}|1`)).toMatchObject({ offset: null })
  expect(ctx.cluster.fetchTopicsOffset).not.toHaveBeenCalled()
  expect(ctx.coordinator.offsetCommit).not.toHaveBeenCalled()
})

test('none halts unknown divergence without a reset request', async () => {
  const ctx = noneGroup({ committed: '3' })
  const preparation = {
    manager: ctx.manager,
    guard: () => null,
    check() {},
    ownershipToken: ctx.group.ownership.token(),
    assignmentEpoch: ctx.group.assignmentEpoch,
  }
  const setDefault = jest.spyOn(ctx.manager, 'setDefaultOffset')
  const seek = jest.spyOn(ctx.manager, 'seek')
  const truncations = []
  ctx.group.instrumentationEmitter.addListener(LOG_TRUNCATION, (event) => truncations.push(event))
  const unknown = { action: 'reset', divergingOffset: null, leaderEpoch: 3 }
  await ctx.group.applyValidationOutcome(TOPIC, 0, '7', unknown, preparation, 2)
  expect(ctx.group.haltedPartitions.get(KEY)).toMatchObject({
    offset: '7',
    reason: 'offset-out-of-range',
  })
  expect(ctx.halts).toHaveLength(1)
  expect(setDefault).not.toHaveBeenCalled()
  expect(ctx.cluster.fetchTopicsOffset).not.toHaveBeenCalled()
  expect(truncations).toHaveLength(1)

  const known = { action: 'truncated', divergingOffset: '4', leaderEpoch: 3 }
  await ctx.group.applyValidationOutcome(TOPIC, 1, '7', known, preparation, 2)
  expect(seek).toHaveBeenCalledWith({ topic: TOPIC, partition: 1, offset: '4' }, preparation)
  expect(ctx.halts).toHaveLength(1)
})

test('seek recovers a none halt without installing automatic progress', async () => {
  const ctx = noneGroup()
  await ctx.group.seekOffsets(PARTS)
  expect(ctx.group.haltedPartitions.has(KEY)).toBe(true)
  const wake = jest.spyOn(ctx.group.idleWait, 'wake')
  ctx.group.seek({ topic: TOPIC, partition: 0, offset: '5' })
  await ctx.group.seekOffsets(PARTS)
  expect(ctx.group.haltedPartitions.has(KEY)).toBe(false)
  expect(wake).toHaveBeenCalled()
  expect(ctx.manager.nextOffset(TOPIC, 0).toString()).toBe('5')
  expect(ctx.coordinator.offsetCommit).not.toHaveBeenCalled()

  ctx.group.invalidateTopic(TOPIC)
  expect(ctx.group.haltedPartitions.size).toBe(0)
  await ctx.group.seekOffsets(PARTS)
  expect(ctx.group.haltedPartitions.has(KEY)).toBe(true)
  expect(ctx.halts).toHaveLength(2)

  ctx.group.haltedPartitions.clear()
  ctx.group.offsetManager.clearPendingSeeks()
  ctx.group.offsetManager = ctx.build({ captureResetContext: ctx.captureResetContext })
  await ctx.group.seekOffsets(PARTS)
  expect(ctx.group.haltedPartitions.has(KEY)).toBe(true)
  expect(ctx.halts).toHaveLength(3)
})

const WIRE_NODE = { id: 0, host: 'node-0', port: 9092 }
const WIRE_BUDGET_MS = 10000

/**
 * One write of the production encoder, so the decoder reads it as it reads a broker's.
 * @param {number} offset first and only offset of the batch
 * @returns {Promise<Buffer>} a one-record batch
 */
const batchAt = async (offset) =>
  Buffer.from(
    (
      await RecordBatch({
        firstOffset: String(offset),
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        partitionLeaderEpoch: 5,
        records: [
          RecordEncoder({
            key: Buffer.from(`key-${offset}`),
            value: Buffer.from(`value-${offset}`),
          }),
        ],
      })
    ).buffer
  )

/**
 * A public none-policy consumer over a scripted broker: partition 0 has no committed offset,
 * partition 1 is committed at 4, and each partition's log holds one record at the offsets in `logs`.
 * @param {Record<number, number[]>} logs record offsets by partition
 * @returns {Promise<object>} the consumer, the cluster and what the run observed
 */
const startWire = async (logs) => {
  const batches = {}
  for (const [partition, offsets] of Object.entries(logs)) {
    for (const offset of offsets) batches[`${partition}|${offset}`] = await batchAt(offset)
  }
  const cluster = groupScript(createScriptedCluster({ nodes: [WIRE_NODE] }), {
    partitions: 2,
    committed: { 0: '-1', 1: '4' },
  }).answer('Fetch', async ({ maxWaitMs, topics }) => {
    const partitions = topics[0].partitions.map(({ partition, fetchOffset }) => ({
      partition,
      errorCode: 0,
      records: batches[`${partition}|${fetchOffset}`] ?? false,
      abortedTransactions: [],
    }))
    if (partitions.every(({ records }) => records === false)) {
      await new Promise((resolve) => setTimeout(resolve, maxWaitMs))
    }
    return { partitions }
  })
  const state = { cluster, delivered: [], halts: [], errors: [] }
  const kafka = new Kafka(
    cluster.kafkaOptions({
      logLevel: logLevel.ERROR,
      logCreator:
        () =>
        ({ level, log }) => {
          if (level === logLevel.ERROR) state.errors.push(log)
        },
      retry: { retries: 2, initialRetryTime: 10, maxRetryTime: 20 },
    })
  )
  state.consumer = kafka.consumer({
    groupId: 'none-group',
    maxWaitTimeInMs: 20,
    heartbeatInterval: 100,
  })
  state.consumer.on(state.consumer.events.PARTITION_HALTED, (event) =>
    state.halts.push(event.payload)
  )
  await state.consumer.connect()
  await state.consumer.subscribe({ topics: [TOPIC], offsetReset: 'none' })
  await state.consumer.run({
    autoCommit: false,
    eachMessage: async ({ partition, message }) => {
      state.delivered.push({ partition, offset: message.offset })
    },
  })
  state.until = async (predicate, label) => {
    const deadline = Date.now() + WIRE_BUDGET_MS
    while (!predicate()) {
      if (Date.now() > deadline) throw new Error(`Did not happen in time: ${label}`)
      await new Promise((resolve) => setTimeout(resolve, 5))
    }
  }
  state.frames = (api) => cluster.requests().filter((request) => request.api === api)
  return state
}

describe('a none-policy consumer on the wire', () => {
  let wire

  afterEach(async () => {
    const closing = wire
    wire = null
    if (closing) {
      await closing.consumer.disconnect()
      closing.cluster.assertClean()
    }
  })

  test('none writes no ListOffsets frame while a committed sibling delivers', async () => {
    wire = await startWire({ 1: [4] })
    await wire.until(
      () => wire.halts.length === 1 && wire.delivered.length === 1,
      'the halt and the sibling'
    )
    await new Promise((resolve) => setTimeout(resolve, 100))

    const listed = wire
      .frames('ListOffsets')
      .flatMap(({ fields }) => fields.topics.flatMap(({ partitions }) => partitions))
    expect(listed.filter(({ partitionIndex }) => partitionIndex === 0)).toEqual([])
    const fetched = wire
      .frames('Fetch')
      .flatMap(({ fields }) => fields.topics.flatMap(({ partitions }) => partitions))
    expect(fetched.length).toBeGreaterThan(0)
    expect(fetched.every(({ partition }) => partition === 1)).toBe(true)
    expect(wire.delivered).toEqual([{ partition: 1, offset: '4' }])
    expect(wire.halts).toHaveLength(1)
    expect(wire.halts[0]).toMatchObject({ partition: 0, offset: null, reason: 'no-offset' })
    expect(wire.halts[0].error).toBeInstanceOf(KafkaJSNoOffsetError)
    expect(
      wire.errors.filter(({ message }) => message.includes('Halted a partition'))
    ).toHaveLength(1)
    expect(wire.frames('OffsetCommit')).toEqual([])
  })

  test('a seek resumes a none-halted partition at the sought offset without an automatic commit', async () => {
    wire = await startWire({ 0: [1], 1: [4] })
    await wire.until(() => wire.halts.length === 1, 'the halt')
    wire.consumer.seek({ topic: TOPIC, partition: 0, offset: '1' })
    await wire.until(
      () => wire.delivered.some(({ partition }) => partition === 0),
      'the sought record'
    )

    expect(wire.delivered.filter(({ partition }) => partition === 0)).toEqual([
      { partition: 0, offset: '1' },
    ])
    expect(wire.halts).toHaveLength(1)
    expect(wire.frames('OffsetCommit')).toEqual([])
    const listed = wire
      .frames('ListOffsets')
      .flatMap(({ fields }) => fields.topics.flatMap(({ partitions }) => partitions))
    expect(listed).toEqual([])
  })
})

test('a held refusal is taken once, skips its cell until retired and follows the topic incarnation', async () => {
  const ctx = noneGroup({ topicId: NEW_ID })
  await ctx.manager.resolveOffsets()
  const taken = ctx.manager.takeResetRefusals()
  expect(taken.map(({ partition, reason, offset }) => ({ partition, reason, offset }))).toEqual([
    { partition: 0, reason: 'no-offset', offset: null },
  ])
  expect(ctx.manager.takeResetRefusals()).toHaveLength(0)
  expect(ctx.manager.takeResetRefusals()).toBe(ctx.manager.takeResetRefusals())
  expect(ctx.manager.currentResetRefusal(TOPIC, 0)).toBe(taken[0])
  expect(ctx.manager.currentResetRefusal(TOPIC, 1)).toBeNull()

  const asked = ctx.coordinator.offsetFetch.mock.calls.length
  await ctx.manager.resolveOffsets()
  expect(ctx.coordinator.offsetFetch).toHaveBeenCalledTimes(asked)

  ctx.setTopicId('31323334-3536-3738-393a-3b3c3d3e3f40')
  expect(ctx.manager.currentResetRefusal(TOPIC, 0)).toBeNull()
  expect(ctx.manager.currentResetRefusal(TOPIC, 0)).toBeNull()
  await ctx.manager.resolveOffsets()
  expect(ctx.coordinator.offsetFetch.mock.calls.length).toBeGreaterThan(asked)
})
