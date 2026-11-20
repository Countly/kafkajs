const Cluster = require('../../../cluster')
const GroupRegistry = require('../../../cluster/groupRegistry')
const CommittedOffsetRevisions = require('../../../cluster/committedOffsetRevisions')
const OffsetOperations = require('../offsetOperations')
const dictionary = require('../../../utils/dictionary')
const { createScheduledOffsetManager } = require('./fixtures/scheduledOffsetManager')
const { newLogger } = require('testHelpers')

const TOPIC = 'test-topic'
const cell = [{ topic: TOPIC, partition: 0 }]

/** @returns {OffsetOperations} a ledger bound to a fresh cache */
const ledger = () => new OffsetOperations(dictionary())

describe('OffsetOperations as the one committed-cache writer', () => {
  test('publish writes the bound cache, advances the revision and keeps the highest-intent durable winner', () => {
    const operations = ledger()
    const revisions = CommittedOffsetRevisions.forCache(operations.cache)
    const before = revisions.capture()
    const first = operations.begin(cell, { mutation: true })
    const second = operations.begin(cell, { mutation: true })

    operations.publish(second, TOPIC, 0, '7')
    operations.publish(first, TOPIC, 0, '5')

    expect(operations.cache[TOPIC][0]).toBe('5')
    expect(revisions.isCurrent(TOPIC, 0, before)).toBe(false)
    expect(operations.durable(TOPIC, 0)).toEqual({
      intent: second.intent,
      offset: '7',
    })
    operations.end(first)
    operations.end(second)
  })

  test('a read fill writes the cache but records no durable winner', () => {
    const operations = ledger()
    const read = operations.begin(cell)
    const revisions = CommittedOffsetRevisions.forCache(operations.cache)
    const before = revisions.capture()

    operations.fill(TOPIC, 0, '3')

    expect(operations.cache[TOPIC][0]).toBe('3')
    expect(revisions.isCurrent(TOPIC, 0, before)).toBe(false)
    expect(operations.durable(TOPIC, 0)).toBeNull()
    operations.end(read)
  })

  test('an ordinary eviction clears the cell and fences older work but keeps the durable winner', () => {
    const operations = ledger()
    const mutation = operations.begin(cell, { mutation: true })
    const older = operations.begin(cell)
    operations.publish(mutation, TOPIC, 0, '5')

    operations.evict(TOPIC, 0)

    expect(operations.cache[TOPIC][0]).toBeUndefined()
    expect(operations.isSuperseded(older, TOPIC, 0)).toBe(true)
    expect(operations.isHardBarred(mutation, TOPIC, 0)).toBe(false)
    expect(operations.durable(TOPIC, 0)).toEqual({
      intent: mutation.intent,
      offset: '5',
    })
    operations.end(older)
    operations.end(mutation)
  })

  test.each([
    ['a recreation barrier', (operations) => operations.touchTopic(TOPIC)],
    ['a topic invalidation', (operations) => operations.invalidateTopic(TOPIC)],
  ])('%s is hard: it fences like a barrier and drops the durable winner', (label, hard) => {
    const operations = ledger()
    const mutation = operations.begin(cell, { mutation: true })
    operations.publish(mutation, TOPIC, 0, '5')
    const later = operations.begin(cell, { mutation: true })

    hard(operations)

    expect(operations.isSuperseded(mutation, TOPIC, 0)).toBe(true)
    expect(operations.isHardBarred(mutation, TOPIC, 0)).toBe(true)
    expect(operations.durable(TOPIC, 0)).toBeNull()
    expect(operations.isHardBarred(operations.begin(cell), TOPIC, 0)).toBe(false)
    operations.end(mutation)
    operations.end(later)
  })

  test('a topic invalidation empties the cached topic and stamps its revision', () => {
    const operations = ledger()
    const mutation = operations.begin(cell, { mutation: true })
    operations.publish(mutation, TOPIC, 0, '5')
    const revisions = CommittedOffsetRevisions.forCache(operations.cache)
    const before = revisions.capture()

    operations.invalidateTopic(TOPIC)

    expect(operations.cache[TOPIC]).toEqual({})
    expect(revisions.isCurrent(TOPIC, 1, before)).toBe(false)
    operations.end(mutation)
  })

  test('the durable winner lives only while a ticket covers the cell', () => {
    const operations = ledger()
    const mutation = operations.begin(cell, { mutation: true })
    operations.publish(mutation, TOPIC, 0, '5')
    operations.end(mutation)

    expect(operations.durable(TOPIC, 0)).toBeNull()
    expect(operations.size()).toBe(0)
    expect(operations.cache[TOPIC][0]).toBe('5')
  })
})

describe('Cache and ledger binding', () => {
  test('the group registry pairs each ledger with its cache', () => {
    const registry = GroupRegistry.forStores(new Map(), new Map())
    const entry = registry.acquire('group', {})
    expect(entry.ledger.cache).toBe(entry.offsets)
  })

  test("Cluster#markOffsetAsCommitted publishes through the group's ledger with the caller's ticket", () => {
    const cluster = new Cluster({
      logger: newLogger(),
      brokers: ['localhost:9092'],
      retry: { retries: 0 },
    })
    const operations = cluster.offsetOperations({ groupId: 'group' })
    const ticket = operations.begin(cell, { mutation: true })

    cluster.markOffsetAsCommitted(
      { groupId: 'group', topic: TOPIC, partition: 0, offset: '9' },
      ticket
    )

    expect(cluster.committedOffsets({ groupId: 'group' })[TOPIC][0]).toBe('9')
    expect(operations.durable(TOPIC, 0)).toEqual({
      intent: ticket.intent,
      offset: '9',
    })
    operations.end(ticket)
  })

  test('an offset manager writes the cache its ledger is bound to', () => {
    const ctx = createScheduledOffsetManager()
    expect(ctx.manager.committedOffsets()).toBe(ctx.operations.cache)
    expect(ctx.cache).toBe(ctx.operations.cache)
  })
})

describe('Offset manager cache writes go through the ledger', () => {
  test('a recorded commit is the durable winner of its covered cell', async () => {
    const ctx = createScheduledOffsetManager({ autoCommit: true })
    const holder = ctx.operations.begin(cell)
    ctx.manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '4' })

    await ctx.manager.commitOffsets()

    expect(ctx.committedAt()).toBe('5')
    expect(ctx.operations.durable(TOPIC, 0)).toMatchObject({ offset: '5' })
    ctx.operations.end(holder)
  })

  test('a revoke evicts the cell without dropping the durable winner; a topic reset drops it', async () => {
    const ctx = createScheduledOffsetManager({ autoCommit: true, partitions: [0, 1] })
    const holder = ctx.operations.begin(cell)
    ctx.manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '4' })
    await ctx.manager.commitOffsets()

    ctx.manager.updateAssignment({
      revoked: [{ topic: TOPIC, partitions: [0] }],
      memberAssignment: { [TOPIC]: [1] },
      generationId: 2,
      memberId: 'member-a',
      coordinator: ctx.coordinator,
    })
    expect(ctx.cache[TOPIC][0]).toBeUndefined()
    expect(ctx.operations.durable(TOPIC, 0)).toMatchObject({ offset: '5' })

    ctx.manager.resetOffsetsForTopic(TOPIC)
    expect(ctx.operations.durable(TOPIC, 0)).toBeNull()
    ctx.operations.end(holder)
  })
  test('a topic reset is one hard barrier over the cells its open tickets cover', () => {
    const ctx = createScheduledOffsetManager({ autoCommit: true })
    const holder = ctx.operations.begin(cell)
    const stamps = jest.spyOn(ctx.operations, 'touchTopic')
    const before = ctx.operations.clock

    ctx.manager.resetOffsetsForTopic(TOPIC)

    expect(stamps).toHaveBeenCalledTimes(1)
    expect(ctx.operations.clock).toBe(before + 1)
    expect(ctx.operations.isHardBarred(holder, TOPIC, 0)).toBe(true)
    stamps.mockRestore()
    ctx.operations.end(holder)
  })
})
